//! Reading one object through a single RTM download.
//!
//! [`RtmReader`] holds one cursor: a live download and the position it has consumed to. A read the
//! cursor cannot serve tears that download down and opens another. One cursor rather than several
//! because the existing prefetcher holds exactly one, and comparing against it is the point.
//!
//! # Read-ahead
//!
//! Depth is adaptive, not configured. Each cursor opens at `Parts(0)` — demand paging — and
//! [`RtmConfig::max_read_ahead_bytes`] is a ceiling it grows towards while reads keep stalling on
//! the body. This mirrors the prefetcher, where `max_read_window_size` bounds a window that
//! `sequential_prefetch_multiplier` scales up on evidence of sequential reading — and defaults to
//! the same 2 GiB, so the two arms speculate up to the same budget.
//!
//! The ceiling is in bytes and the transfer layer's knob counts parts, so it is converted once in
//! [`RtmDataPlane::new`] — the only place the part size is knowable, since it belongs to the
//! transfer manager rather than to any one download.
//!
//! The point is that speculation should be earned. A fixed depth charges a cursor opened for one
//! scattered read the same as one streaming an object end to end, and under random access almost
//! every cursor is the former.
//!
//! # Locking
//!
//! [`Reader::read_at`] takes `&self`, so locking happens here rather than in the caller. One
//! [`AsyncMutex`](crate::sync::AsyncMutex) over the cursor, held across the data await, guarding
//! both the cursor and the decision to replace it — otherwise two reads could each find the
//! cursor unusable and both open a download, leaking one.
//!
//! It has to be an async mutex: the guard is held across pulling from the download body, and a
//! std guard is neither `Send` across a suspension point nor safe to block a runtime worker on.
//!
//! Reads at unrelated offsets therefore serialize. With one cursor that costs nothing they could
//! otherwise have: a cursor is a single sequential stream, so they would contend for it anyway.
//!
//! The stats mutex is separate and std, since its critical sections span no await.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::data::cursor::{Cursor, CursorId};
use crate::data::writer::{RtmWriter, WriterConfig};
use crate::data::{DataPlane, ObjectSpec, ReadError, Reader, ReaderStats, Segments, Urgency, WriteError, WriteSpec};

use crate::sync::AsyncMutex;
use tracing::{debug, trace, warn};

use crate::data::priority::PriorityTable;

/// Tunables for the RTM data plane.
#[derive(Debug, Clone)]
pub struct RtmConfig {
    /// Bytes retained per cursor for backward seeks. Matches the prefetcher's
    /// `max_backward_seek_distance` default.
    pub seek_window_bytes: usize,

    /// How far a read may seek forward within a cursor's existing range before a new cursor
    /// is opened instead. A forward seek is served by discarding bytes, so this bounds the
    /// waste. Matches the prefetcher's `max_forward_seek_wait_distance` default.
    pub max_forward_seek: u64,

    /// Ceiling on read-ahead, in bytes. **A maximum, not a setting.**
    ///
    /// The counterpart of `PrefetcherConfig::max_read_window_size`, and defaulted from the same
    /// place, so both read paths speculate up to the same budget. A cursor opens at `Parts(0)` —
    /// demand paging — and doubles towards this bound each time a read has to wait on the body;
    /// the policy is in [`Cursor::read_at`]. So a sequential reader earns the full window while a
    /// cursor read once and abandoned costs one part.
    ///
    /// In bytes rather than parts because that is the unit a memory budget is denominated in, and
    /// the unit the prefetcher uses. The transfer layer's knob is part-granular, so this is
    /// converted once when the data plane is built — see [`RtmDataPlane::new`], which is also
    /// where the part size becomes knowable.
    ///
    /// Because the transfer layer's unit is the part, a small read at a large part size still
    /// fetches a whole part. That floor cannot be tuned around from here; it needs a smaller part
    /// size.
    pub max_read_ahead_bytes: usize,

    /// Factor read-ahead depth grows by per stall. Matches
    /// `PrefetcherConfig::sequential_prefetch_multiplier`. Values below 2 are raised to 2, since
    /// a multiplier of 1 would never grow.
    pub read_ahead_multiplier: usize,

    /// Bytes a cursor's first request covers, before a follow-on request to the end of the object.
    /// Matches `PrefetcherConfig::initial_request_size`.
    ///
    /// This is what stops a small read costing a whole part. The transfer layer's first GET for a
    /// download is `min(start + part_size - 1, range_end)` (RTM
    /// `operation/download/discovery.rs`), so a short *range* makes a short *request* — a 128 KiB
    /// read at an 8 MiB part size otherwise pulls the whole 8 MiB.
    ///
    /// Every cursor opens with this range, not just a reader's first: under scattered access almost
    /// every cursor is opened once and abandoned, which is the case it exists for. Reading on past
    /// it earns the follow-on request — see
    /// [`Cursor::maybe_chain_next`](crate::data::cursor::Cursor).
    pub initial_request_size: u64,

    /// Open the follow-on request once `initial_request_size / this` has been consumed. 2 is
    /// mid-way.
    ///
    /// Issued while the caller is still reading the first range, so it is already in flight when
    /// the first runs out rather than costing a round trip at the handover. A larger divisor issues
    /// it earlier.
    pub initial_request_trigger_divisor: usize,

    /// Urgency-to-priority mapping.
    pub priorities: PriorityTable,

    /// Tunables for the upload path used by [`DataPlane::open_write`].
    pub writer: WriterConfig,
}

impl Default for RtmConfig {
    fn default() -> Self {
        Self {
            seek_window_bytes: 1024 * 1024,
            max_forward_seek: 16 * 1024 * 1024,
            // The prefetcher's ceiling, from the same function, so the two arms are comparable and
            // stay so under `UNSTABLE_MOUNTPOINT_MAX_PREFETCH_WINDOW_SIZE`. Only a sequential
            // reader that keeps stalling gets there; a one-read cursor stays at one part.
            max_read_ahead_bytes: crate::prefetch::determine_max_read_size(),
            read_ahead_multiplier: 2,
            initial_request_size: crate::prefetch::INITIAL_REQUEST_SIZE as u64,
            initial_request_trigger_divisor: 2,
            priorities: PriorityTable::default(),
            writer: WriterConfig::default(),
        }
    }
}

impl RtmConfig {
    /// Set the urgency-to-priority mapping.
    pub fn with_priorities(mut self, priorities: PriorityTable) -> Self {
        self.priorities = priorities;
        self
    }
}

/// Opens [`RtmReader`]s over objects.
#[derive(Debug, Clone)]
pub struct RtmDataPlane {
    tm: aws_sdk_s3_transfer_manager::Client,
    config: Arc<RtmConfig>,
    /// [`RtmConfig::max_read_ahead_bytes`] resolved against the client's part size. Derived here
    /// rather than per read, since the part size cannot change for a client's lifetime.
    max_read_ahead_parts: usize,
    next_id: Arc<AtomicU64>,
}

/// What the transfer layer uses for downloads when part size is left to `PartSize::Auto`
/// (RTM `client.rs`, `Handle::download_part_size_bytes`).
///
/// Mirrored rather than derived, because RTM does not expose the resolution. Note `Auto` is not one
/// number: it is 8 MiB for uploads and 16 MiB for the multipart threshold, and this is only the
/// download figure.
const AUTO_DOWNLOAD_PART_SIZE: u64 = 5 * 1024 * 1024;

impl RtmDataPlane {
    /// Open a data plane over `tm`.
    ///
    /// Resolves [`RtmConfig::max_read_ahead_bytes`] against the client's part size. That happens
    /// here because part size is a client-level property — the download builder has no per-request
    /// override, and RTM resolves it from the client in `operation/download/discovery.rs` — so this
    /// is the only place the byte ceiling can be turned into the part-granular knob the transfer
    /// layer takes.
    ///
    /// A client left on `PartSize::Auto` is assumed to use [`AUTO_DOWNLOAD_PART_SIZE`], and says so
    /// at `warn`. The assumption can be wrong in a way we cannot see: `Auto` also lets the transfer
    /// layer re-pick part sizes to align with an object's stored parts, in which case the derived
    /// ceiling is off by whatever it picked. Setting an explicit `PartSize::Target` avoids that.
    pub fn new(tm: aws_sdk_s3_transfer_manager::Client, config: RtmConfig) -> Self {
        let part_size = match tm.config().part_size() {
            aws_sdk_s3_transfer_manager::types::PartSize::Target(bytes) => *bytes,
            _ => {
                warn!(
                    assumed_part_size = AUTO_DOWNLOAD_PART_SIZE,
                    "transfer manager has no explicit part size; assuming the transfer layer's \
                     download default to convert the read-ahead ceiling into parts. Set \
                     PartSize::Target to make this exact."
                );
                AUTO_DOWNLOAD_PART_SIZE
            }
        };
        let max_read_ahead_parts = read_ahead_parts_for(config.max_read_ahead_bytes, part_size);
        debug!(
            max_read_ahead_bytes = config.max_read_ahead_bytes,
            part_size, max_read_ahead_parts, "resolved read-ahead ceiling"
        );

        Self {
            tm,
            config: Arc::new(config),
            max_read_ahead_parts,
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn config(&self) -> &RtmConfig {
        &self.config
    }

    /// The resolved read-ahead ceiling in parts. Test and diagnostic use.
    pub fn max_read_ahead_parts(&self) -> usize {
        self.max_read_ahead_parts
    }
}

/// Convert a byte read-ahead ceiling into the transfer layer's `Parts(n)` knob.
///
/// `Parts(n)` is a window of `n + 1` parts — `n` of speculation beyond the part the consumer is
/// waiting on, which is always admitted (RTM `operation/download/read_ahead.rs::window_parts_for`).
/// So the speculation a byte budget buys is one part less than the budget covers, and a budget under
/// one part floors to `Parts(0)`: demand paging, the shallowest the knob expresses.
fn read_ahead_parts_for(max_bytes: usize, part_size: u64) -> usize {
    if part_size == 0 {
        return 0;
    }
    (max_bytes as u64 / part_size).saturating_sub(1) as usize
}

impl DataPlane for RtmDataPlane {
    type Reader = RtmReader;
    type Writer = RtmWriter;

    fn open_write(&self, spec: WriteSpec) -> Result<RtmWriter, WriteError> {
        RtmWriter::open(&self.tm, spec, &self.config.writer)
    }

    fn open_read(&self, obj: ObjectSpec) -> RtmReader {
        RtmReader {
            tm: self.tm.clone(),
            config: self.config.clone(),
            max_read_ahead_parts: self.max_read_ahead_parts,
            next_id: self.next_id.clone(),
            obj,
            cursor: AsyncMutex::new(None),
            stats: Mutex::new(ReaderStats::default()),
        }
    }
}

/// Reads one object through a single RTM download.
///
/// One cursor, not a set: the existing prefetcher holds exactly one, so this is the
/// like-for-like shape to compare against. A read the cursor cannot serve — a backward seek past
/// the retained window, or a forward seek far enough to make discarding wasteful — tears the
/// download down and opens another, which is precisely what the prefetcher does.
pub struct RtmReader {
    tm: aws_sdk_s3_transfer_manager::Client,
    config: Arc<RtmConfig>,
    /// [`RtmConfig::max_read_ahead_bytes`] in parts, resolved once by [`RtmDataPlane::new`].
    max_read_ahead_parts: usize,
    next_id: Arc<AtomicU64>,
    obj: ObjectSpec,
    /// The live cursor, if one is open. Guards both the cursor and the decision to replace it,
    /// so a read that has to reopen cannot race another doing the same.
    ///
    /// `read_at` takes `&self`, so two reads at unrelated offsets do serialize here. With one
    /// cursor that is not a lost opportunity: they would contend for the same download anyway,
    /// and a cursor is a single sequential stream.
    cursor: AsyncMutex<Option<Cursor>>,
    stats: Mutex<ReaderStats>,
}

impl std::fmt::Debug for RtmReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RtmReader")
            .field("id", &self.obj.id)
            .field("size", &self.obj.size)
            .finish_non_exhaustive()
    }
}

impl RtmReader {
    fn priority(&self, urgency: Urgency) -> u8 {
        self.config.priorities.priority_for(urgency)
    }

    /// Whether the open cursor can serve this read without reopening.
    ///
    /// Forward seeks are served by discarding bytes, so a seek further than
    /// [`RtmConfig::max_forward_seek`] is cheaper to satisfy with a fresh download than by
    /// throwing away everything in between.
    fn can_serve(&self, cursor: &Cursor, offset: u64, len: usize) -> bool {
        cursor.can_serve(offset, len) && cursor.distance_to(offset) <= self.config.max_forward_seek
    }

    /// Open a cursor at `start`, covering the rest of the object.
    ///
    /// How that range is requested — a short first GET and a chained follow-on — is
    /// [`Cursor`]'s business, not this reader's. All that happens here is naming the cursor and
    /// counting it.
    fn open(&self, start: u64) -> Result<Cursor, ReadError> {
        let id = CursorId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let cursor = Cursor::open(
            id,
            self.tm.clone(),
            self.obj.clone(),
            start,
            self.config.clone(),
            self.max_read_ahead_parts,
        )?;
        {
            let mut stats = self.stats.lock().unwrap();
            stats.cursors_opened += 1;
            // The cursor's first request. Later ones are chained by the cursor itself and picked up
            // as a delta after each read — see `read_at`.
            stats.requests_issued += 1;
        }
        Ok(cursor)
    }

    /// Read-ahead depth of the open cursor, in parts, or `None` if no cursor is open. Test and
    /// diagnostic use: this is how far the adaptive window has grown.
    pub async fn read_ahead_parts(&self) -> Option<usize> {
        self.cursor.lock().await.as_ref().map(|c| c.read_ahead_parts())
    }

    /// Cancel the open download, if any. Worth calling before drop, since dropping a
    /// `DownloadHandle` cancels without waiting for in-flight work to wind down.
    pub async fn close(&self) {
        if let Some(mut cursor) = self.cursor.lock().await.take() {
            cursor.abort().await;
        }
    }
}

impl Reader for RtmReader {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Segments, ReadError> {
        if offset >= self.obj.size {
            return Err(ReadError::OutOfRange {
                offset,
                size: self.obj.size,
            });
        }
        // Clamp rather than error: a read running past the end returns what exists, which
        // is what a filesystem read does.
        let len = len.min((self.obj.size - offset) as usize);
        if len == 0 {
            return Ok(Segments::new());
        }

        // One lock, held across the read. It guards both the cursor and the decision to replace
        // it, so two reads cannot each conclude the cursor is unusable and open a download.
        let mut guard = self.cursor.lock().await;

        // Reopen if there is no cursor, or the one we have cannot serve this read. The old cursor is
        // dropped without waiting for in-flight work to wind down.
        let usable = guard.as_ref().is_some_and(|c| self.can_serve(c, offset, len));
        if !usable {
            if let Some(spent) = guard.take() {
                trace!(cursor = %spent.id(), offset, "cursor cannot serve this read; reopening");
                drop(spent);
                self.stats.lock().unwrap().requests_aborted += 1;
            }
            *guard = Some(self.open(offset)?);
        } else {
            // Reassert demand priority: a read is blocked on this cursor again, and it was
            // dropped to speculative when the previous read finished.
            guard
                .as_ref()
                .expect("usable implies present")
                .set_priority(self.priority(Urgency::Demand));
        }

        let cursor = guard.as_mut().expect("just opened or already usable");
        let before = cursor.bytes_fetched();
        // A read can chain a follow-on request, so requests are sampled the same way as bytes:
        // `stats()` is sync and cannot take the cursor lock, so anything the cursor counts has to be
        // folded in here rather than read out of it on demand.
        let requests_before = cursor.requests_issued();
        let result = cursor.read_at(offset, len).await;
        let fetched = cursor.bytes_fetched().saturating_sub(before);
        let chained = cursor.requests_issued().saturating_sub(requests_before);

        match result {
            Ok(segs) => {
                {
                    let mut stats = self.stats.lock().unwrap();
                    stats.bytes_delivered += segs.len() as u64;
                    stats.bytes_fetched += fetched;
                    stats.requests_issued += chained;
                    if fetched == 0 {
                        // Served entirely from bytes already resident.
                        stats.cache_hits += 1;
                    }
                }
                // The read is satisfied; anything this cursor fetches from here is
                // speculation until someone blocks on it again.
                cursor.set_priority(self.priority(Urgency::Speculative));
                Ok(segs)
            }
            Err(e) => {
                // The cursor's position can no longer be trusted, so tear it down. The error
                // still reaches the caller, but a retry opens a fresh download.
                debug!(cursor = %cursor.id(), error = %e, "dropping cursor after failed read");
                {
                    let mut stats = self.stats.lock().unwrap();
                    stats.bytes_fetched += fetched;
                    stats.requests_issued += chained;
                }
                if let Some(mut failed) = guard.take() {
                    failed.abandon();
                    self.stats.lock().unwrap().requests_aborted += 1;
                }
                Err(e)
            }
        }
    }

    fn stats(&self) -> ReaderStats {
        *self.stats.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;

    #[test]
    fn byte_ceiling_converts_to_one_part_less_than_it_buys() {
        // `Parts(n)` is a window of `n + 1`, so a 2 GiB budget at 8 MiB parts is `Parts(255)`:
        // 256 parts resident, one of them the part the consumer is waiting on.
        assert_eq!(read_ahead_parts_for(2 * GIB as usize, 8 * MIB), 255);
        assert_eq!(read_ahead_parts_for(2 * GIB as usize, 16 * MIB), 127);
        assert_eq!(read_ahead_parts_for(2 * GIB as usize, 5 * MIB), 408);
    }

    #[test]
    fn budget_under_one_part_floors_to_demand_paging() {
        // Nothing smaller than one part is expressible, and `Parts(0)` is the floor: one part, no
        // speculation. A zero budget must not underflow.
        assert_eq!(read_ahead_parts_for(0, 8 * MIB), 0);
        assert_eq!(read_ahead_parts_for(8 * MIB as usize - 1, 8 * MIB), 0);
        assert_eq!(read_ahead_parts_for(8 * MIB as usize, 8 * MIB), 0);
        // Exactly two parts of budget buys one part of speculation.
        assert_eq!(read_ahead_parts_for(16 * MIB as usize, 8 * MIB), 1);
    }

    #[test]
    fn zero_part_size_does_not_divide_by_zero() {
        assert_eq!(read_ahead_parts_for(2 * GIB as usize, 0), 0);
    }

    #[test]
    fn default_ceiling_matches_the_prefetcher() {
        assert_eq!(
            RtmConfig::default().max_read_ahead_bytes,
            crate::prefetch::determine_max_read_size(),
            "both read paths must speculate up to the same budget"
        );
    }
}
