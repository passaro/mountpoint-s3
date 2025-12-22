use mountpoint_s3_client::{ObjectClient, types::ETag};

use crate::checksums::ChecksummedBytes;
use crate::object::ObjectId;
use crate::prefetch::{PrefetchGetObject, Prefetcher};

use super::{DataLayer, Download};

pub fn create_data_layer<Client>(prefetcher: Prefetcher<Client>) -> impl DataLayer
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    PrefetchDataLayer { prefetcher }
}

struct PrefetchDataLayer<Client>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    prefetcher: Prefetcher<Client>,
}

impl<Client> DataLayer for PrefetchDataLayer<Client>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    fn download(&self, bucket: String, key: String, etag: ETag, size: usize) -> impl Download {
        let object_id = ObjectId::new(key, etag);
        self.prefetcher.prefetch(bucket, object_id, size as u64)
    }
}

impl<Client> Download for PrefetchGetObject<Client>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    async fn read(&mut self, offset: u64, length: usize) -> Result<ChecksummedBytes, anyhow::Error> {
        Ok(self.read(offset, length).await?)
    }
}
