//! Reading and writing object data, independent of which transfer layer moves the bytes.
//!
//! Two implementations sit behind [`DataPlane`]:
//!
//! - [`prefetch_adapter`] wraps the existing [`Prefetcher`](crate::prefetch::Prefetcher),
//!   so the CRT-backed read path is reachable through these traits. It is read-only; its
//!   [`open_write`](DataPlane::open_write) always errors.
//! - [`reader`] and [`writer`] drive the AWS S3 Transfer Manager for Rust (RTM), for downloads
//!   and uploads respectively.
//!
//! One trait over both lets a single benchmark binary run the same workload against either
//! backend and compare the results.
//!
//! **Experimental, and used only by the `s3io_benchmark` example.** The RTM dependency is
//! optional and gated behind the `rtm_data_plane` feature, which the `mountpoint-s3` binary
//! does not enable, so nothing here reaches a release build. Nothing in `fs.rs` uses this
//! module.

pub mod segments;

#[cfg(feature = "rtm_data_plane")]
pub mod cursor;
#[cfg(feature = "rtm_data_plane")]
pub mod part_channel;
#[cfg(feature = "rtm_data_plane")]
pub mod priority;
#[cfg(feature = "rtm_data_plane")]
pub mod reader;
#[cfg(feature = "rtm_data_plane")]
pub mod writer;

pub mod prefetch_adapter;

use std::future::Future;

pub use segments::Segments;

#[cfg(feature = "rtm_data_plane")]
pub use priority::PriorityTable;
#[cfg(feature = "rtm_data_plane")]
pub use reader::{RtmConfig, RtmDataPlane, RtmReader};
#[cfg(feature = "rtm_data_plane")]
pub use writer::{RtmWriter, WriteResult, WriterConfig};

pub use prefetch_adapter::PrefetchDataPlane;

use crate::{object::ObjectId, s3::Bucket};

/// Identifies the exact bytes a read is against.
///
/// The `etag` is used in `if_match` on every ranged GET, so a concurrent overwrite
/// fails the request rather than silently splicing two versions of an object together.
///
/// `size` is supplied here rather than discovered. A filesystem already knows it from
/// the superblock, and re-deriving it would mean a HEAD per open.
#[derive(Debug, Clone)]
pub struct ObjectSpec {
    pub bucket: Bucket,
    pub id: ObjectId,
    pub size: u64,
}

impl ObjectSpec {
    pub fn new(bucket: impl Into<String>, key: impl Into<String>, etag: impl AsRef<str>, size: u64) -> Self {
        let bucket = Bucket::new(bucket).expect("bucket should be a valid bucket name");
        let etag = etag.as_ref().parse().expect("etag from HeadObject should always parse");
        let id = ObjectId::new(key.into(), etag);

        Self { bucket, id, size }
    }
}

/// Whether a fetch serves a caller that is blocked, or speculates on one that might be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Urgency {
    /// A `read_at` is blocked on these bytes.
    Demand,
    /// Read-ahead beyond what anyone has asked for.
    Speculative,
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// The read starts at or beyond the end of the object.
    #[error("offset {offset} is beyond object size {size}")]
    OutOfRange { offset: u64, size: u64 },

    /// The object changed underneath us — `if_match` failed. Distinct from a transport
    /// error because the correct response differs: re-open at the new version rather than
    /// retry.
    #[error("object was modified during read (etag mismatch)")]
    ObjectModified,

    /// A part arrived at an offset other than the one requested. Cheap to check and
    /// catastrophic to miss, so it is checked.
    #[error("expected data at offset {expected}, got {actual}")]
    OffsetMismatch { expected: u64, actual: u64 },

    /// The transfer layer failed. Boxed to keep this enum substrate-independent while
    /// still preserving the source chain for diagnosability.
    #[error("transfer failed: {0}")]
    Transfer(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The body ended before the requested range did, without an error.
    #[error("stream ended at offset {offset}, {short_by} bytes short")]
    UnexpectedEof { offset: u64, short_by: u64 },
}

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    /// A write arrived somewhere other than the current end of the stream.
    ///
    /// Uploads are append-only: a multipart part cannot be revised once sent, so there is no
    /// way to honour a write behind the current position.
    #[error("out-of-order write at offset {write_offset}, expected {expected_offset}")]
    OutOfOrderWrite { write_offset: u64, expected_offset: u64 },

    /// The transfer layer failed. Boxed for the same reason as [`ReadError::Transfer`].
    ///
    /// An unknown-length upload that overruns the multipart ceiling (`part_size * 10,000`)
    /// surfaces here, mid-stream, since without a declared size it cannot be caught up front.
    #[error("transfer failed: {0}")]
    Transfer(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The upload was already finished — completed or aborted — when this call arrived.
    #[error("upload is no longer in progress")]
    NotInProgress,
}

/// Per-reader counters, for diagnostics and for comparing backends: request and cursor
/// counts are what make read amplification and cursor thrash visible.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReaderStats {
    /// Bytes handed to the caller.
    pub bytes_delivered: u64,
    /// Bytes requested from S3. The gap over `bytes_delivered` is read amplification.
    pub bytes_fetched: u64,
    /// Ranged GETs issued.
    pub requests_issued: u64,
    /// GETs cancelled before completing. Non-zero means work was thrown away.
    pub requests_aborted: u64,
    /// Reads served without waiting on the network.
    pub cache_hits: u64,
    /// Cursors created over this reader's lifetime. Above 1 means the access pattern forced the
    /// download to be torn down and reopened.
    pub cursors_opened: u64,
}

/// Per-writer counters, the upload counterpart of [`ReaderStats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriterStats {
    /// Bytes accepted from the caller by [`Writer::write_at`].
    pub bytes_accepted: u64,
    /// Bytes handed to the transfer layer.
    ///
    /// Trails `bytes_accepted` by whatever is still buffered; equal once
    /// [`complete`](Writer::complete) returns.
    pub bytes_dispatched: u64,
    /// Times `write_at` had to wait for buffer space, i.e. the caller was throttled to the
    /// network's rate rather than buffering without limit.
    pub write_stalls: u64,
}

/// Identifies an object being created.
///
/// Not [`ObjectSpec`]: there is no etag yet — that is the upload's *result* — and the size is
/// not known in advance. A filesystem writer never knows the final object size when it opens
/// the file, so the upload streams to end-of-stream with no declared bound (see
/// [`writer`](crate::data::writer)).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WriteSpec {
    pub bucket: String,
    pub key: String,
}

impl WriteSpec {
    pub fn new(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
        }
    }
}

/// What an upload produced.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WriteOutcome {
    /// The finished object's etag, if the transfer layer reported one.
    pub etag: Option<String>,
    /// Bytes uploaded.
    pub size: u64,
    /// Whether this went out as a multipart upload rather than a single PUT.
    pub multipart: bool,
}

/// Opens readers and writers over objects.
pub trait DataPlane: Send + Sync {
    type Reader: Reader;
    type Writer: Writer;

    /// Open a reader.
    ///
    /// Cheap, infallible, and lazy: no request is issued until the first
    /// [`read_at`](Reader::read_at), so opening a handle that is never read from costs no
    /// round trip. Matches [`Prefetcher::prefetch`].
    ///
    /// [`Prefetcher::prefetch`]: crate::prefetch::Prefetcher::prefetch
    fn open_read(&self, obj: ObjectSpec) -> Self::Reader;

    /// Open a writer, starting the upload.
    ///
    /// Fallible and eager where [`open_read`](Self::open_read) is infallible and lazy: an
    /// upload is a single transfer covering the whole object, so it has to begin here rather
    /// than on first write.
    fn open_write(&self, spec: WriteSpec) -> Result<Self::Writer, WriteError>;
}

/// Reads bytes of one object.
///
/// `read_at` takes `&self` because the operation is positional — a `pread`, with the offset as
/// an argument — so there is no implicit position for a mutable borrow to protect. Taking
/// `&mut self` would serialize reads at unrelated offsets, which defeats the point of an
/// implementation holding more than one cursor.
///
/// Implementations therefore lock internally, at a finer grain than the whole reader.
pub trait Reader: Send + Sync {
    /// Read exactly `len` bytes at `offset`, or fewer only when the object ends first.
    ///
    /// `Err(ReadError::OutOfRange)` if `offset` is at or past the end of the object; a `len`
    /// that runs past the end is clamped rather than an error.
    ///
    /// The contract matches `PrefetchGetObject::read`, so behaviour is comparable across both
    /// implementations.
    fn read_at(&self, offset: u64, len: usize) -> impl Future<Output = Result<Segments, ReadError>> + Send;

    fn stats(&self) -> ReaderStats;
}

/// Writes the bytes of one object.
///
/// `write_at` takes `&mut self` where [`Reader::read_at`] takes `&self`. Uploads are
/// append-only — parts are numbered in order and a sent part cannot be revised — so there is
/// exactly one write position and it is implicit state. The `offset` argument exists only to
/// check the caller against that position, reported as [`WriteError::OutOfOrderWrite`].
///
/// **[`abort`](Self::abort) must be called explicitly to cancel; `Drop` is not a substitute.**
/// Dropping a writer cancels the transfer without issuing `AbortMultipartUpload`, leaving a
/// partial multipart upload on S3 for a lifecycle rule to clean up. `Drop` cannot fix this
/// because issuing that call is async. Both terminal methods take `self`, so a writer cannot
/// be used after either.
pub trait Writer: Send {
    /// Append `data` at `offset`, which must equal the current end of the stream.
    ///
    /// Returns the number of bytes accepted, always `data.len()` on success — a short write is
    /// not a success.
    ///
    /// Waits when the internal buffer is full. That wait is the backpressure that keeps
    /// buffered bytes bounded when a caller writes faster than the network drains.
    fn write_at(&mut self, offset: u64, data: &[u8]) -> impl Future<Output = Result<usize, WriteError>> + Send;

    /// Flush anything buffered, finish the transfer, and report what was uploaded.
    fn complete(self) -> impl Future<Output = Result<WriteOutcome, WriteError>> + Send;

    /// Cancel the upload, issuing `AbortMultipartUpload` if a multipart upload was started.
    fn abort(self) -> impl Future<Output = Result<(), WriteError>> + Send;

    fn stats(&self) -> WriterStats;
}
