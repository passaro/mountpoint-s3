//! The existing CRT-backed [`Prefetcher`] and [`Uploader`](crate::upload::Uploader) behind the
//! [`DataPlane`] traits — reads and writes both.
//!
//! **This path validates checksums on read and the RTM path does not.** `PrefetchGetObject::read`
//! returns [`ChecksummedBytes`], and satisfying [`Reader`] means calling `into_bytes()`, which
//! CRCs every byte. A raw throughput comparison therefore charges this path for work the other
//! skips.
//!
//! **`bytes_fetched` is not observable here.** The prefetcher does not report bytes actually
//! fetched from S3, so [`ReaderStats::bytes_fetched`] is set equal to `bytes_delivered`, making
//! this arm read as exactly 1.0x amplification by construction.
//!
//! **Write counters are limited.** The CRT uploader exposes no stall or multipart signal, so
//! [`CrtWriter`] reports only `bytes_accepted`/`bytes_dispatched`; `write_stalls` is 0 and
//! `WriteOutcome::multipart` is `false`.

use std::sync::Mutex;

use mountpoint_s3_client::ObjectClient;

use crate::sync::AsyncMutex;

use crate::data::{
    DataPlane, ObjectSpec, ReadError, Reader, ReaderStats, Segments, WriteError, WriteOutcome, WriteSpec, Writer,
    WriterStats,
};
use crate::prefetch::{PrefetchGetObject, Prefetcher};
use crate::upload::{AppendUploadRequest, UploadRequest, Uploader};

/// [`DataPlane`] over the existing CRT [`Prefetcher`] (reads) and [`Uploader`] (writes).
pub struct CrtDataPlane<Client>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    prefetcher: Prefetcher<Client>,
    uploader: Uploader<Client>,
}

impl<Client> CrtDataPlane<Client>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    pub fn new(prefetcher: Prefetcher<Client>, uploader: Uploader<Client>) -> Self {
        Self { prefetcher, uploader }
    }
}

impl<Client> DataPlane for CrtDataPlane<Client>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    type Reader = CrtReader<Client>;
    type Writer = CrtWriter<Client>;

    fn open_read(&self, obj: ObjectSpec) -> Self::Reader {
        let request = self
            .prefetcher
            .prefetch(obj.bucket.to_string(), obj.id.clone(), obj.size);
        CrtReader {
            request: AsyncMutex::new(request),
            size: obj.size,
            stats: Mutex::new(ReaderStats::default()),
        }
    }

    fn open_write(&self, spec: WriteSpec) -> Result<Self::Writer, WriteError> {
        if spec.incremental {
            // Append: PutObject-with-offset per buffer, driven by the uploader's incremental queue.
            let request = self.uploader.start_incremental_upload(spec.bucket, spec.key, 0, None);
            Ok(CrtWriter::Incremental { request, bytes: 0 })
        } else {
            let request = self
                .uploader
                .start_atomic_upload(spec.bucket, spec.key)
                .map_err(|e| WriteError::Transfer(Box::new(e)))?;
            Ok(CrtWriter::Atomic { request, bytes: 0 })
        }
    }
}

/// [`Reader`] over a single [`PrefetchGetObject`].
///
/// The mutex is the cost of the trait taking `&self` while `PrefetchGetObject::read` takes
/// `&mut self`. It serializes all reads through this reader — which is exactly what
/// Mountpoint does today one layer up, where `FileHandleState` sits behind an `AsyncMutex`
/// for the same reason. So this is not a handicap the adapter introduces; it is the existing
/// constraint made visible.
///
/// It has to be an async mutex, not a std one: the guard is held across
/// `PrefetchGetObject::read`'s await, and a std guard is neither `Send` across a suspension
/// point nor safe to block a runtime worker on.
pub struct CrtReader<Client>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    request: AsyncMutex<PrefetchGetObject<Client>>,
    size: u64,
    stats: Mutex<ReaderStats>,
}

impl<Client> std::fmt::Debug for CrtReader<Client>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrtReader").field("size", &self.size).finish()
    }
}

impl<Client> Reader for CrtReader<Client>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    async fn read_at(&self, offset: u64, len: usize) -> Result<Segments, ReadError> {
        if offset >= self.size {
            return Err(ReadError::OutOfRange {
                offset,
                size: self.size,
            });
        }
        let len = len.min((self.size - offset) as usize);
        if len == 0 {
            return Ok(Segments::new());
        }

        // `PrefetchGetObject::read` needs `&mut self`, so the lock is held across the await.
        let result = {
            let mut request = self.request.lock().await;
            request.read(offset, len).await
        };

        match result {
            Ok(checksummed) => {
                // Validates CRC32C over every byte. See the module note on fairness.
                let bytes = checksummed.into_bytes().map_err(|e| ReadError::Transfer(Box::new(e)))?;
                let mut stats = self.stats.lock().expect("stats lock poisoned");
                stats.bytes_delivered += bytes.len() as u64;
                // The prefetcher does not expose bytes actually fetched from S3, so
                // amplification is not observable on this path. Left equal to delivered
                // rather than zero, so the two backends' stats are not silently
                // incomparable in opposite directions.
                stats.bytes_fetched += bytes.len() as u64;
                Ok(Segments::from(bytes))
            }
            Err(e) => Err(ReadError::Transfer(Box::new(e))),
        }
    }

    fn stats(&self) -> ReaderStats {
        *self.stats.lock().expect("stats lock poisoned")
    }
}

/// [`Writer`] over the CRT [`Uploader`], for either an atomic (whole-object) or an incremental
/// (append) upload.
///
/// The CRT request types cut and pace parts internally, so there is no caller-side buffer to bound
/// and no stall counter — [`stats`](Writer::stats) reports only bytes. Neither type surfaces whether
/// the upload went out as a multipart upload, so [`WriteOutcome::multipart`] is always `false`.
pub enum CrtWriter<Client: ObjectClient> {
    Atomic {
        request: UploadRequest<Client>,
        bytes: u64,
    },
    Incremental {
        request: AppendUploadRequest<Client>,
        bytes: u64,
    },
}

impl<Client> Writer for CrtWriter<Client>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    async fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<usize, WriteError> {
        let written = match self {
            // Atomic `write` takes an `i64` offset; incremental takes `u64`. Both are strictly
            // sequential and surface an out-of-order write as `UploadError::OutOfOrderWrite`.
            CrtWriter::Atomic { request, bytes } => {
                let n = request
                    .write(offset as i64, data)
                    .await
                    .map_err(|e| WriteError::Transfer(Box::new(e)))?;
                *bytes += n as u64;
                n
            }
            CrtWriter::Incremental { request, bytes } => {
                let n = request
                    .write(offset, data)
                    .await
                    .map_err(|e| WriteError::Transfer(Box::new(e)))?;
                *bytes += n as u64;
                n
            }
        };
        Ok(written)
    }

    async fn complete(self) -> Result<WriteOutcome, WriteError> {
        match self {
            CrtWriter::Atomic { request, bytes } => {
                let result = request
                    .complete()
                    .await
                    .map_err(|e| WriteError::Transfer(Box::new(e)))?;
                Ok(WriteOutcome {
                    etag: Some(result.etag.as_str().to_owned()),
                    size: bytes,
                    multipart: false,
                })
            }
            CrtWriter::Incremental { request, bytes } => {
                // `None` when no PUT was ever issued (an empty upload).
                let result = request
                    .complete()
                    .await
                    .map_err(|e| WriteError::Transfer(Box::new(e)))?;
                Ok(WriteOutcome {
                    etag: result.map(|r| r.etag.as_str().to_owned()),
                    size: bytes,
                    multipart: false,
                })
            }
        }
    }

    async fn abort(self) -> Result<(), WriteError> {
        // Neither CRT request type has an explicit abort; dropping it cancels the in-flight
        // request so the object is never completed. `self` drops at the end of this scope.
        Ok(())
    }

    fn stats(&self) -> WriterStats {
        let bytes = match self {
            CrtWriter::Atomic { bytes, .. } | CrtWriter::Incremental { bytes, .. } => *bytes,
        };
        WriterStats {
            bytes_accepted: bytes,
            bytes_dispatched: bytes,
            write_stalls: 0,
        }
    }
}
