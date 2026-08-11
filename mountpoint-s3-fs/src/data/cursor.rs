//! One cursor: one sequential read position over an object, served by one or two RTM downloads.
//!
//! A cursor owns its downloads and the position it has consumed to. It can serve reads at that
//! position, ahead of it within its range, or a short way behind it from retained bytes — but
//! nowhere else. Choosing which cursor serves a read, and opening new ones, is
//! [`RtmReader`](crate::data::RtmReader)'s job.

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use crate::data::reader::RtmConfig;
use crate::data::{ObjectSpec, ReadError, Segments, Urgency};
use aws_sdk_s3_transfer_manager::operation::download::DownloadHandle;
use aws_sdk_s3_transfer_manager::types::ReadAhead;
use bytes::Bytes;
use tracing::{debug, trace};

/// Identifies a cursor within a reader: monotonic, and stable for the cursor's lifetime, so it
/// can label one in logs and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CursorId(pub u64);

impl std::fmt::Display for CursorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cursor-{}", self.0)
    }
}

/// Bytes already delivered, retained so a short backward seek can be served without
/// restarting the download.
///
/// Bounded by total bytes rather than chunk count, because bytes are what cost memory.
/// Chunks are dropped oldest-first once the bound is exceeded.
#[derive(Debug)]
struct SeekWindow {
    /// `(object offset of first byte, chunk)`, oldest first.
    chunks: VecDeque<(u64, Bytes)>,
    bytes: usize,
    capacity: usize,
}

impl SeekWindow {
    fn new(capacity: usize) -> Self {
        Self {
            chunks: VecDeque::new(),
            bytes: 0,
            capacity,
        }
    }

    fn push(&mut self, offset: u64, chunk: Bytes) {
        if self.capacity == 0 || chunk.is_empty() {
            return;
        }
        self.bytes += chunk.len();
        self.chunks.push_back((offset, chunk));
        while self.bytes > self.capacity {
            match self.chunks.pop_front() {
                Some((_, dropped)) => self.bytes -= dropped.len(),
                None => break,
            }
        }
    }

    /// Lowest offset still retained.
    fn earliest(&self) -> Option<u64> {
        self.chunks.front().map(|(off, _)| *off)
    }

    /// Whether `[offset, offset+len)` lies entirely within the retained bytes.
    fn can_serve(&self, offset: u64, len: usize) -> bool {
        let Some(first) = self.earliest() else {
            return false;
        };
        let Some((last_off, last)) = self.chunks.back() else {
            return false;
        };
        offset >= first && offset + len as u64 <= last_off + last.len() as u64
    }

    /// Copy out `[offset, offset+len)`. Assumes [`can_serve`](Self::can_serve).
    ///
    /// Slicing `Bytes` is a refcount operation, so this walks chunks without copying
    /// their contents.
    fn read(&self, offset: u64, len: usize) -> Segments {
        let mut out = Segments::new();
        let mut want = offset;
        let end = offset + len as u64;
        for (chunk_off, chunk) in &self.chunks {
            let chunk_end = chunk_off + chunk.len() as u64;
            if chunk_end <= want || *chunk_off >= end {
                continue;
            }
            let from = (want - chunk_off) as usize;
            let to = (end.min(chunk_end) - chunk_off) as usize;
            out.push(chunk.slice(from..to));
            want = chunk_end.min(end);
            if want >= end {
                break;
            }
        }
        out
    }
}

/// A cursor's in-flight download and its position.
#[derive(Debug)]
pub struct Cursor {
    id: CursorId,
    /// Next object offset this cursor will deliver.
    position: u64,
    /// Lowest offset still re-servable from the seek window. Equals `position` when nothing is
    /// retained.
    earliest: u64,
    /// One past the last offset this *cursor* covers — the object's end, not any one request's.
    range_end: u64,
    aborted: bool,
    /// The request being consumed. `None` once its body is exhausted with nothing chained behind
    /// it, or once the cursor has been torn down.
    handle: Option<DownloadHandle>,
    /// Opened ahead of need, covering `handle_end..range_end`, and not yet drawn from.
    /// [`fill`](Self::fill) switches to it when `handle` runs dry.
    next: Option<DownloadHandle>,
    /// One past the last offset `handle` covers, which is where `next` begins. Equals `range_end`
    /// once no more chaining is needed.
    handle_end: u64,
    /// Where this cursor started, for measuring how much of the initial request has been consumed.
    start_offset: u64,
    /// The object this cursor reads, so it can open its own follow-on request.
    obj: ObjectSpec,
    /// The transfer manager the follow-on request is opened on.
    tm: aws_sdk_s3_transfer_manager::Client,
    /// Ranged GETs this cursor has issued, including chained ones. The reader folds this into
    /// `ReaderStats::requests_issued`, which counts requests rather than cursors.
    requests_issued: u64,
    /// Next object offset the download itself will deliver, which runs ahead of
    /// `position` by whatever sits unconsumed in `pending`.
    next_offset: u64,
    /// A chunk pulled from the body but not yet fully consumed.
    pending: Segments,
    seek_window: SeekWindow,
    bytes_fetched: u64,
    /// Current read-ahead depth in parts, mirroring what the handle was last told. Starts at 0,
    /// which is demand paging, and only ever grows.
    read_ahead_parts: usize,
    /// Ceiling for the above: [`RtmConfig::max_read_ahead_bytes`] resolved against the client's
    /// part size by [`RtmDataPlane::new`](crate::data::RtmDataPlane::new). Resolved rather than
    /// read from the config because the conversion needs a part size, which is a property of the
    /// transfer manager and not of this cursor.
    max_read_ahead_parts: usize,
    /// The config this cursor was opened under, for its read-ahead policy.
    ///
    /// `RtmConfig` lives in [`reader`](crate::data::reader), which imports this module — so the two
    /// are mutually dependent. That is fine (Rust forbids cycles between crates, not modules), and
    /// they are already a single unit from outside, both re-exported from `crate::data`.
    config: Arc<RtmConfig>,
}

impl Cursor {
    /// The depth a cursor opens at: `Parts(0)`, meaning the part the consumer is waiting on and
    /// nothing beyond it.
    pub const INITIAL_READ_AHEAD_PARTS: usize = 0;

    /// Open a cursor at `start`, over the rest of `obj`, and issue its first request.
    ///
    /// The first request is short — [`RtmConfig::initial_request_size`] — because the transfer
    /// layer's first GET is `min(start + part_size - 1, range_end)`, so the requested range caps it.
    /// A cursor opened for one small read therefore costs one small GET rather than a whole part.
    /// Reading on past that range earns the remainder, chained by
    /// [`maybe_chain_next`](Self::maybe_chain_next) before the first runs out.
    ///
    /// How many requests that takes and where they are cut is this type's business: a caller
    /// supplies what identifies the object and how to behave, not a handle.
    pub fn open(
        id: CursorId,
        tm: aws_sdk_s3_transfer_manager::Client,
        obj: ObjectSpec,
        start: u64,
        config: Arc<RtmConfig>,
        max_read_ahead_parts: usize,
    ) -> Result<Self, ReadError> {
        let range_end = obj.size;
        let handle_end = range_end.min(start.saturating_add(config.initial_request_size));

        // At demand priority: a read is blocked on this cursor the moment it exists. A chained
        // follow-on opens speculative instead, since nobody is waiting on it yet.
        let handle = Self::open_range(
            &tm,
            &obj,
            &config,
            start..handle_end,
            Self::INITIAL_READ_AHEAD_PARTS,
            Urgency::Demand,
        )?;
        trace!(cursor = %id, start, handle_end, range_end, "opened cursor");

        Ok(Self {
            id,
            position: start,
            earliest: start,
            range_end,
            aborted: false,
            handle: Some(handle),
            next: None,
            handle_end,
            start_offset: start,
            obj,
            tm,
            requests_issued: 1,
            next_offset: start,
            pending: Segments::new(),
            seek_window: SeekWindow::new(config.seek_window_bytes),
            bytes_fetched: 0,
            read_ahead_parts: Self::INITIAL_READ_AHEAD_PARTS,
            max_read_ahead_parts,
            config,
        })
    }

    /// Ranged GETs this cursor has issued, including chained follow-ons.
    pub fn requests_issued(&self) -> u64 {
        self.requests_issued
    }

    /// Current read-ahead depth in parts. Diagnostic and test use.
    pub fn read_ahead_parts(&self) -> usize {
        self.read_ahead_parts
    }

    pub fn id(&self) -> CursorId {
        self.id
    }

    /// Bytes this cursor has pulled from S3 over its lifetime. Compared against bytes
    /// delivered, this is read amplification.
    pub fn bytes_fetched(&self) -> u64 {
        self.bytes_fetched
    }

    fn position(&self) -> u64 {
        self.position
    }

    /// Whether this cursor could serve `[offset, offset+len)` without restarting.
    ///
    /// Three ways yes: the bytes are still retained behind the position (a short backward seek),
    /// they are at the position, or they are ahead of it but within this download's range (a
    /// forward seek, served by discarding). This says nothing about how *expensive* serving would
    /// be; bounding forward-seek waste is the reader's policy.
    pub fn can_serve(&self, offset: u64, len: usize) -> bool {
        if self.aborted || offset >= self.range_end || offset < self.earliest {
            return false;
        }
        // A read starting behind the position must lie wholly within retained bytes, which end at
        // the position.
        offset >= self.position || offset + len as u64 <= self.position
    }

    /// Bytes that would be discarded to serve a read at `offset`: the cost of choosing this
    /// cursor. A backward seek into retained bytes discards nothing.
    pub fn distance_to(&self, offset: u64) -> u64 {
        offset.saturating_sub(self.position)
    }

    /// Serve a read, pulling from the RTM body as needed.
    ///
    /// `len` is expected to be pre-clamped to the object's end by the caller.
    pub async fn read_at(&mut self, offset: u64, len: usize) -> Result<Segments, ReadError> {
        if len == 0 {
            return Ok(Segments::new());
        }

        // Sampled around the read, because `bytes_fetched` moves only in `fill`: a change across
        // this read means it went to the body, and a non-zero value *here* means this is not the
        // cursor's first read. Those are exactly the two conditions for deepening read-ahead, so
        // neither needs a flag to carry it between methods.
        let fetched_before = self.bytes_fetched;

        // Backward seek: re-serve retained bytes in place.
        if offset < self.position() {
            if self.seek_window.can_serve(offset, len) {
                return Ok(self.seek_window.read(offset, len));
            }
            return Err(ReadError::OffsetMismatch {
                expected: self.position(),
                actual: offset,
            });
        }

        // Forward seek: discard up to the target.
        while self.position() < offset {
            if self.pending.is_empty() && !self.fill().await? {
                return Err(ReadError::UnexpectedEof {
                    offset: self.position(),
                    short_by: offset - self.position(),
                });
            }
            let skip = (offset - self.position()) as usize;
            let dropped = self.pending.take_front(skip);
            self.consume(dropped);
        }

        let mut out = Segments::new();
        while out.len() < len {
            if self.pending.is_empty() && !self.fill().await? {
                break;
            }
            let taken = self.pending.take_front(len - out.len());
            if taken.is_empty() {
                break;
            }
            out.push(taken.clone());
            self.consume(taken);
        }

        if out.len() < len {
            return Err(ReadError::UnexpectedEof {
                offset: self.position(),
                short_by: (len - out.len()) as u64,
            });
        }

        // This read had to wait on the body, so the window was too shallow for the rate it is being
        // consumed at — the analogue of the prefetcher's `PartQueueStall`. Deepen it.
        //
        // Not on the cursor's first read, which always waits because nothing is buffered yet and so
        // says nothing about whether speculation was too shallow. Counting it would deepen every
        // cursor one step the moment it opened, including the read-once-and-abandon cursors that
        // adaptive depth exists to keep cheap.
        //
        // Reached only on the success path, and once: the early returns above — zero-length reads,
        // backward seeks served from the window, short reads — all bypass it, correctly, since none
        // of them is evidence about read-ahead depth.
        if fetched_before > 0 && self.bytes_fetched > fetched_before {
            self.deepen_read_ahead();
        }
        self.maybe_chain_next();
        Ok(out)
    }

    /// Open the follow-on request once the caller is far enough into the initial one.
    ///
    /// The initial request is short, so that a cursor opened for one small read costs one small
    /// GET. Reading on past it is what earns the rest of the object — the same shape as the
    /// read-ahead ramp, where speculation is a consequence of how the object is being read.
    ///
    /// Issued *while* the first range is still being consumed, at
    /// `initial_request_size / initial_request_trigger_divisor`, so it is already in flight when
    /// the first runs out. Waiting until then would put a round trip in the middle of a sequential
    /// scan.
    fn maybe_chain_next(&mut self) {
        if self.next.is_some() || self.handle_end >= self.range_end {
            return;
        }
        let divisor = self.config.initial_request_trigger_divisor.max(1) as u64;
        let trigger = (self.config.initial_request_size / divisor).max(1);
        if self.position.saturating_sub(self.start_offset) < trigger {
            return;
        }

        // To the end of the cursor's range: the caller has shown it is reading sequentially, so the
        // follow-on is not speculative in the way the first request's length was. What is actually
        // resident is still bounded by the read-ahead window, which this cursor keeps across the
        // handover.
        match self.open_chained() {
            Ok(handle) => {
                trace!(
                    cursor = %self.id,
                    from = self.handle_end,
                    to = self.range_end,
                    "chaining follow-on request"
                );
                self.next = Some(handle);
                self.requests_issued += 1;
            }
            // Not fatal: the current request is still good, and `fill` will retry the chain on the
            // next read. A failure here surfaces as a read error only if the body actually runs out.
            Err(e) => debug!(cursor = %self.id, error = %e, "could not chain follow-on request"),
        }
    }

    /// Issue a ranged GET over `range`, at `read_ahead_parts` depth and the given urgency.
    ///
    /// An associated function rather than a method because [`open`](Self::open) needs it to build the
    /// handle the cursor is then constructed *from*, so there is no `self` to borrow yet.
    ///
    /// `urgency` is the only thing that differs between a cursor's first request and a chained one:
    /// the first has a read blocked on it, a chained one exists precisely so that nothing will be.
    fn open_range(
        tm: &aws_sdk_s3_transfer_manager::Client,
        obj: &ObjectSpec,
        config: &RtmConfig,
        range: Range<u64>,
        read_ahead_parts: usize,
        urgency: Urgency,
    ) -> Result<DownloadHandle, ReadError> {
        let header = format!("bytes={}-{}", range.start, range.end.saturating_sub(1));
        let handle = tm
            .download()
            .bucket(obj.bucket.to_string())
            .key(obj.id.key())
            .if_match(obj.id.etag().as_str())
            .range(&header)
            .read_ahead(ReadAhead::Parts(read_ahead_parts))
            .initiate()
            .map_err(|e| ReadError::Transfer(Box::new(e)))?;
        handle
            .scheduling()
            .set_priority(config.priorities.priority_for(urgency));
        Ok(handle)
    }

    /// Issue the follow-on request covering `handle_end..range_end`, at this cursor's current depth.
    fn open_chained(&self) -> Result<DownloadHandle, ReadError> {
        Self::open_range(
            &self.tm,
            &self.obj,
            &self.config,
            self.handle_end..self.range_end,
            self.read_ahead_parts,
            Urgency::Speculative,
        )
    }

    /// Double read-ahead depth, up to the resolved [`RtmConfig::max_read_ahead_bytes`] ceiling.
    ///
    /// Growth is monotonic within a cursor's life, and nothing shrinks it. It does not need to: a
    /// read this cursor cannot serve destroys it, and the replacement starts at
    /// [`INITIAL_READ_AHEAD_PARTS`](Self::INITIAL_READ_AHEAD_PARTS) again. Under scattered access
    /// cursors are short-lived, so depth cannot accumulate — the reopen *is* the reset. (The
    /// prefetcher's `scale_down` reacts to a memory-reservation failure from `PagedPool`, a signal
    /// this path does not have: RTM admits its buffers against its own budget.)
    fn deepen_read_ahead(&mut self) {
        let max_parts = self.max_read_ahead_parts;
        if self.read_ahead_parts >= max_parts {
            return;
        }
        // Doubling zero stays zero, so the first step is a floor rather than a multiply. `Parts(0)`
        // is one part with no speculation; `Parts(1)` is the first depth that speculates.
        let next = if self.read_ahead_parts == 0 {
            1
        } else {
            self.read_ahead_parts
                .saturating_mul(self.config.read_ahead_multiplier.max(2))
        }
        .min(max_parts);

        self.read_ahead_parts = next;
        self.set_read_ahead(ReadAhead::Parts(next));
    }

    /// Account for `chunk` having left `pending`: advance the position and retain the
    /// bytes against a possible backward seek.
    fn consume(&mut self, chunk: Bytes) {
        let at = self.position;
        let n = chunk.len() as u64;
        self.seek_window.push(at, chunk);
        self.position = at + n;
        self.earliest = self.seek_window.earliest().unwrap_or(self.position);
    }

    /// Pull one chunk from the RTM body into `pending`. `false` at end of body.
    ///
    /// Verifies each chunk's absolute offset rather than trusting arrival order.
    /// `ChunkMetadata::content_range` is the only place a chunk's position is exposed, so it is
    /// parsed and checked: inferring position from order alone would silently serve wrong data
    /// if the transfer layer ever reordered or dropped a chunk.
    async fn fill(&mut self) -> Result<bool, ReadError> {
        // Loops rather than recursing, because exhausting one request switches to the chained one
        // and tries again — and an `async fn` cannot call itself without boxing the future. At most
        // two iterations: there is only ever one chained request.
        loop {
            let Some(handle) = self.handle.as_mut() else {
                return Ok(false);
            };
            match handle.body_mut().next().await {
                None => {
                    // This request is done. If a follow-on was chained, take it over and keep
                    // going: to everything above `fill` the body simply carried on, because it did
                    // — the two requests are contiguous by construction (`next` covers
                    // `handle_end..range_end`), and every chunk's offset is still checked below, so
                    // a gap would be caught rather than served.
                    // Nothing chained yet, but the cursor's range is not finished: chain it now.
                    // `maybe_chain_next` only runs after a *successful* read, and a read can consume
                    // the whole initial range before ever completing — a forward seek discards bytes
                    // to reach its target, and `can_serve` allows one anywhere within the cursor's
                    // range, which is the object's end and not this request's. Without this the body
                    // would simply run dry mid-read and report `UnexpectedEof`.
                    if self.next.is_none() && self.handle_end < self.range_end {
                        trace!(cursor = %self.id, at = self.handle_end, "chaining on demand mid-read");
                        let handle = self
                            .open_chained()
                            .inspect_err(|e| debug!(cursor = %self.id, error = %e, "could not chain on demand"))?;
                        self.requests_issued += 1;
                        self.next = Some(handle);
                    }
                    match self.next.take() {
                        Some(next) => {
                            trace!(cursor = %self.id, at = self.handle_end, "switching to the chained request");
                            self.handle = Some(next);
                            self.handle_end = self.range_end;
                            continue;
                        }
                        None => {
                            self.handle = None;
                            return Ok(false);
                        }
                    }
                }
                Some(Err(e)) => return Err(ReadError::Transfer(Box::new(e))),
                Some(Ok(chunk)) => {
                    if let Some(start) = chunk
                        .metadata
                        .content_range
                        .as_deref()
                        .and_then(parse_content_range_start)
                        && start != self.next_offset
                    {
                        return Err(ReadError::OffsetMismatch {
                            expected: self.next_offset,
                            actual: start,
                        });
                    }
                    let mut segs: Segments = chunk.data.into_segments().collect();
                    let n = segs.len() as u64;
                    self.bytes_fetched += n;
                    self.next_offset += n;
                    self.pending.extend_from(&mut segs, usize::MAX);
                    return Ok(n > 0);
                }
            }
        }
    }

    /// Lower or raise this cursor's scheduling priority.
    ///
    /// Needs only `&self` on the underlying handle, so unlike teardown it cannot deadlock
    /// against a read that is already in flight. Note it changes scheduling share only —
    /// whether it frees any memory is a separate question.
    ///
    /// Applies to the chained follow-on as well as the request being consumed: both belong to this
    /// cursor, and a caller blocked on the cursor is blocked on whichever is serving it.
    pub fn set_priority(&self, priority: u8) {
        for handle in [self.handle.as_ref(), self.next.as_ref()].into_iter().flatten() {
            handle.scheduling().set_priority(priority);
        }
    }

    /// Adjust how far these downloads prefetch ahead of the consumer.
    ///
    /// Both handles, for the same reason as [`set_priority`](Self::set_priority) — and because the
    /// follow-on is opened at whatever depth had been earned when it was chained, so a later
    /// deepening has to reach it or the handover would silently drop back.
    pub fn set_read_ahead(&self, mode: ReadAhead) {
        for handle in [self.handle.as_ref(), self.next.as_ref()].into_iter().flatten() {
            handle.io_ctl().set_read_ahead(mode.clone());
        }
    }

    /// Cancel this cursor's downloads without waiting for them.
    ///
    /// Use [`abort`](Self::abort) if you want to await for all in-flight work to wind down.
    pub fn abandon(&mut self) {
        self.aborted = true;
        drop(self.next.take());
        drop(self.handle.take());
    }

    /// Cancel this cursor's downloads and wait for their in-flight work to wind down.
    ///
    /// Use [`abandon`](Self::abandon) or [`drop`](Self::drop) instead if you do not need to wait.
    pub async fn abort(&mut self) {
        self.aborted = true;
        if let Some(next) = self.next.take() {
            next.abort().await;
        }
        if let Some(handle) = self.handle.take() {
            handle.abort().await;
        }
    }
}

impl Drop for Cursor {
    fn drop(&mut self) {
        self.abandon();
    }
}

/// First byte offset from an HTTP `Content-Range` value, e.g. `bytes 1024-2047/8192`.
///
/// A chunk carries no numeric offset of its own, so parsing this header is the only way to
/// recover where it belongs in the object.
fn parse_content_range_start(value: &str) -> Option<u64> {
    value
        .trim()
        .strip_prefix("bytes ")?
        .split('-')
        .next()?
        .trim()
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_content_range() {
        assert_eq!(parse_content_range_start("bytes 0-1023/8192"), Some(0));
        assert_eq!(parse_content_range_start("bytes 1024-2047/8192"), Some(1024));
        assert_eq!(parse_content_range_start("  bytes 512-999/1000  "), Some(512));
    }

    #[test]
    fn rejects_malformed_content_range() {
        assert_eq!(parse_content_range_start("0-1023/8192"), None);
        assert_eq!(parse_content_range_start("bytes */8192"), None);
        assert_eq!(parse_content_range_start(""), None);
    }

    fn window_with(capacity: usize, chunks: &[(u64, &[u8])]) -> SeekWindow {
        let mut w = SeekWindow::new(capacity);
        for (off, data) in chunks {
            w.push(*off, Bytes::copy_from_slice(data));
        }
        w
    }

    #[test]
    fn seek_window_serves_retained_range() {
        let w = window_with(1024, &[(0, b"abcd"), (4, b"efgh")]);
        assert!(w.can_serve(0, 8));
        assert!(w.can_serve(2, 4));
        assert!(!w.can_serve(0, 9), "past the end of retained bytes");
        assert_eq!(&w.read(2, 4).to_contiguous()[..], b"cdef");
        assert_eq!(&w.read(4, 4).to_contiguous()[..], b"efgh");
    }

    #[test]
    fn seek_window_evicts_oldest_over_capacity() {
        let w = window_with(8, &[(0, b"abcd"), (4, b"efgh"), (8, b"ijkl")]);
        assert_eq!(w.earliest(), Some(4), "first chunk dropped to stay in bounds");
        assert!(!w.can_serve(0, 4));
        assert!(w.can_serve(4, 8));
        assert_eq!(&w.read(4, 8).to_contiguous()[..], b"efghijkl");
    }

    #[test]
    fn zero_capacity_window_retains_nothing() {
        let w = window_with(0, &[(0, b"abcd")]);
        assert_eq!(w.earliest(), None);
        assert!(!w.can_serve(0, 1));
    }

    #[test]
    fn empty_window_serves_nothing() {
        let w = SeekWindow::new(64);
        assert!(!w.can_serve(0, 1));
        assert!(w.read(0, 4).is_empty());
    }

    /// A transfer manager client that is never called.
    ///
    /// The tests below exercise position arithmetic only — `can_serve` and `distance_to` touch
    /// neither the handles nor the client — but `Cursor` owns a client so it can chain its own
    /// follow-on request, so one has to exist to build the struct. Constructed offline, with
    /// credentials and a region that are never used because no request is ever issued.
    fn unused_client() -> aws_sdk_s3_transfer_manager::Client {
        let s3 = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Builder::new()
                .behavior_version_latest()
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .credentials_provider(aws_sdk_s3::config::Credentials::for_tests())
                .build(),
        );
        aws_sdk_s3_transfer_manager::Client::new(aws_sdk_s3_transfer_manager::Config::builder().client(s3).build())
    }

    /// A cursor with no live download, for testing the position arithmetic in isolation.
    ///
    /// `handle_end == range_end` so nothing would be chained even if a read ran: the chaining path
    /// is covered by the integration tests, which have an endpoint to talk to.
    fn cursor_at(position: u64, earliest: u64, range_end: u64) -> Cursor {
        Cursor {
            id: CursorId(0),
            position,
            earliest,
            range_end,
            aborted: false,
            handle: None,
            next: None,
            handle_end: range_end,
            start_offset: position,
            obj: ObjectSpec::new("bucket", "key", "\"etag\"", range_end),
            tm: unused_client(),
            requests_issued: 0,
            next_offset: position,
            pending: Segments::new(),
            seek_window: SeekWindow::new(0),
            bytes_fetched: 0,
            read_ahead_parts: Cursor::INITIAL_READ_AHEAD_PARTS,
            max_read_ahead_parts: 0,
            config: Arc::new(RtmConfig::default()),
        }
    }

    #[test]
    fn serves_at_and_ahead_of_position_within_range() {
        let c = cursor_at(100, 100, 1000);
        assert!(c.can_serve(100, 10), "at the position");
        assert!(c.can_serve(500, 10), "ahead, within range");
        assert!(!c.can_serve(1000, 1), "at range end");
        assert!(!c.can_serve(99, 1), "behind, nothing retained");
    }

    #[test]
    fn serves_backward_only_within_retained_bytes() {
        let c = cursor_at(500, 400, 1000);
        assert!(c.can_serve(400, 100), "wholly retained");
        assert!(!c.can_serve(400, 200), "runs past the position");
        assert!(!c.can_serve(399, 1), "before what is retained");
    }

    #[test]
    fn aborted_cursor_serves_nothing() {
        let mut c = cursor_at(0, 0, 1000);
        c.aborted = true;
        assert!(!c.can_serve(0, 1));
    }

    #[test]
    fn distance_is_forward_only() {
        let c = cursor_at(500, 500, 1000);
        assert_eq!(c.distance_to(700), 200);
        assert_eq!(c.distance_to(500), 0);
        assert_eq!(c.distance_to(100), 0, "backward seeks discard nothing");
    }
}
