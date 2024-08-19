use crate::object::ObjectId;

use super::{BlockIndex, ChecksummedBytes, DataCache, DataCacheResult};

use bytes::BytesMut;
use futures::{pin_mut, StreamExt};
use mountpoint_s3_client::types::PutObjectParams;
use mountpoint_s3_client::{ObjectClient, PutObjectRequest};
use sha2::{Digest, Sha256};
use tracing::{warn, Instrument};

const CACHE_VERSION: &str = "V1";

pub struct XozDataCache<Client: ObjectClient> {
    bucket_name: String,
    client: Client,
    block_size: u64,
}
impl<Client> XozDataCache<Client>
where
    Client: ObjectClient + Send + Sync + 'static,
{
    pub fn new(bucket_name: &str, client: Client, block_size: Option<u64>) -> Self {
        Self {
            client,
            bucket_name: bucket_name.to_owned(),
            block_size: block_size.unwrap_or(1024) * 1024,
        }
    }

    async fn get_block_xoz(&self, object_key: String) -> DataCacheResult<Option<ChecksummedBytes>> {
        // TODO: is this the right thing in terms of a range? this is not causing any HEAD request so it might be
        let result = match self.client.get_object(&self.bucket_name, &object_key, None, None).await {
            Ok(ok_result) => ok_result,
            Err(_e) => {
                //TODO: assuming that key does not exists but it could be something else, so we will need to do something smarter here
                return DataCacheResult::Ok(None);
            }
        };

        pin_mut!(result);

        let mut buffer = BytesMut::default();
        loop {
            warn!("get_block_xoz {}", object_key);
            match result.next().await {
                Some(Ok((_offset, body))) => {
                    // TODO: check offset expectation
                    buffer.extend_from_slice(&body);
                }
                Some(Err(_e)) => {
                    // TODO: what should we do here? anything better?
                    return DataCacheResult::Ok(None);
                }
                None => {
                    break;
                }
            }
        }
        let buffer = buffer.freeze();
        DataCacheResult::Ok(Some(buffer.into()))
    }

    async fn put_block_xoz(&self, object_key: String, bytes: ChecksummedBytes) -> DataCacheResult<()> {
        warn!("put_block_xoz {}", object_key);
        // TODO: handle errors in a better way than just expects
        let params = PutObjectParams::new();
        let mut req = self
            .client
            .put_object(&self.bucket_name, &object_key, &params)
            .in_current_span()
            .await
            .expect("could not create req");
        let (data, _crc) = bytes.into_inner().expect("could not unpack checksummed bytes");
        req.write(&data).await.expect("unable to write");
        req.complete().await.expect("unable to complete upload");

        DataCacheResult::Ok(())
    }
}
impl<Client> DataCache for XozDataCache<Client>
where
    Client: ObjectClient + Send + Sync + 'static,
{
    fn get_block(
        &self,
        cache_key: &ObjectId,
        block_idx: BlockIndex,
        _block_offset: u64, // TODO: should we use this?
    ) -> DataCacheResult<Option<ChecksummedBytes>> {
        let object_key = object_key(cache_key, block_idx);

        return futures::executor::block_on(self.get_block_xoz(object_key));
    }

    fn put_block(
        &self,
        cache_key: ObjectId,
        block_idx: BlockIndex,
        _block_offset: u64,
        bytes: ChecksummedBytes,
    ) -> DataCacheResult<()> {
        let object_key = object_key(&cache_key, block_idx);
        return futures::executor::block_on(self.put_block_xoz(object_key, bytes));
    }

    fn block_size(&self) -> u64 {
        self.block_size
    }
}

/// Hash the cache key using its fields as well as the [CACHE_VERSION].
fn hash_cache_key_raw(cache_key: &ObjectId) -> [u8; 32] {
    let s3_key = cache_key.key();
    let etag = cache_key.etag();

    let mut hasher = Sha256::new();
    hasher.update(CACHE_VERSION.as_bytes());
    hasher.update(s3_key);
    hasher.update(etag.as_str());
    hasher.finalize().into()
}

fn object_key(cache_key: &ObjectId, block_idx: BlockIndex) -> String {
    let hashed_cache_key = hex::encode(hash_cache_key_raw(cache_key));
    format!("{}/{:010}", hashed_cache_key, block_idx)
}