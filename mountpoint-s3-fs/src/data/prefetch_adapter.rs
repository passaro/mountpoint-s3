//! The existing CRT-backed [`Prefetcher`] behind the [`DataPlane`] traits.
//!
//! **This path validates checksums and the RTM path does not.** `PrefetchGetObject::read`
//! returns [`ChecksummedBytes`], and satisfying [`Reader`] means calling `into_bytes()`, which
//! CRCs every byte. A raw throughput comparison therefore charges this path for work the other
//! skips.
//!
//! **`bytes_fetched` is not observable here.** The prefetcher does not report bytes actually
//! fetched from S3, so [`ReaderStats::bytes_fetched`] is set equal to `bytes_delivered`, making
//! this arm read as exactly 1.0x amplification by construction.
//!
//! Request counts and cursor behaviour are unaffected by either, and are the figures that
//! compare directly.

use std::sync::Mutex;

use mountpoint_s3_client::ObjectClient;

use crate::sync::AsyncMutex;

use crate::data::{DataPlane, ObjectSpec, ReadError, Reader, ReaderStats, Segments};
use crate::prefetch::{PrefetchGetObject, Prefetcher};

/// [`DataPlane`] over the existing [`Prefetcher`].
#[derive(Debug)]
pub struct PrefetchDataPlane<Client>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    prefetcher: Prefetcher<Client>,
}

impl<Client> PrefetchDataPlane<Client>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    pub fn new(prefetcher: Prefetcher<Client>) -> Self {
        Self { prefetcher }
    }
}

impl<Client> DataPlane for PrefetchDataPlane<Client>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    type Reader = PrefetchReader<Client>;

    fn open_read(&self, obj: ObjectSpec) -> Self::Reader {
        let request = self
            .prefetcher
            .prefetch(obj.bucket.to_string(), obj.id.clone(), obj.size);
        PrefetchReader {
            request: AsyncMutex::new(request),
            size: obj.size,
            stats: Mutex::new(ReaderStats::default()),
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
pub struct PrefetchReader<Client>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    request: AsyncMutex<PrefetchGetObject<Client>>,
    size: u64,
    stats: Mutex<ReaderStats>,
}

impl<Client> std::fmt::Debug for PrefetchReader<Client>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefetchReader").field("size", &self.size).finish()
    }
}

impl<Client> Reader for PrefetchReader<Client>
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
