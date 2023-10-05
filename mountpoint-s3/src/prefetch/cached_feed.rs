use std::{ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{pin_mut, StreamExt};
use mountpoint_s3_client::{
    error::{GetObjectError, ObjectClientError},
    types::ETag,
    ObjectClient,
};
use tracing::{error, trace, warn};

use crate::checksums::ChecksummedBytes;
use crate::data_cache::{BlockIndex, DataCache};
use crate::prefetch::{
    feed::{ObjectPartFeed, RequestRange},
    part::Part,
    part_queue::PartQueueProducer,
};

type CacheKey = (String, ETag);

/// [ObjectPartFeed] implementation which maintains a [Cache] for the object data retrieved by a [Client].
#[derive(Debug)]
pub struct CachedPartFeed<Client, Cache> {
    client: Arc<Client>,
    cache: Cache,
}

impl<Client, Cache> CachedPartFeed<Client, Cache> {
    pub fn new(client: Arc<Client>, cache: Cache) -> Self {
        Self { client, cache }
    }
}

#[async_trait]
impl<Client, Cache> ObjectPartFeed<Client> for CachedPartFeed<Client, Cache>
where
    Client: ObjectClient + Send + Sync + 'static,
    Cache: DataCache<CacheKey> + Send + Sync,
{
    async fn get_object_parts(
        &self,
        bucket: &str,
        key: &str,
        range: RequestRange,
        if_match: ETag,
        _preferred_part_size: usize,
        part_queue_producer: PartQueueProducer<ObjectClientError<GetObjectError, Client::ClientError>>,
    ) {
        let block_size = self.cache.block_size();
        let block_range = block_indices_for_byte_range(&range, block_size);

        let cache_key = (key.to_owned(), if_match.clone());
        let block_indexes = match self.cache.cached_block_indices(&cache_key, block_range.clone()) {
            Ok(blocks) => blocks,
            Err(error) => {
                trace!(?key, ?range, ?error, "error reading from cache");
                return self
                    .get_from_client(bucket, &cache_key, range, block_range, part_queue_producer)
                    .await;
            }
        };

        // If the cached blocks do not cover the whole range, fall back to the client.
        // TODO: consider allowing partial cache hits.
        let block_range_len = block_range.end.saturating_sub(block_range.start) as usize;
        if block_indexes.len() < block_range_len {
            trace!(
                ?key,
                ?range,
                "cache miss - only found {} blocks out of {}",
                block_indexes.len(),
                block_range_len
            );
            return self
                .get_from_client(bucket, &cache_key, range, block_range, part_queue_producer)
                .await;
        }

        for block_index in block_indexes {
            match self.cache.get_block(&cache_key, block_index) {
                Ok(Some(block)) => {
                    trace!(?key, ?range, block_index, "cache hit");
                    let part = make_part(key, block, block_index, block_size, &range);
                    metrics::counter!("cache.total_bytes", part.len() as u64, "type" => "read");
                    part_queue_producer.push(Ok(part));
                }
                Ok(None) => {
                    let range = range.trim_start(block_index * block_size);
                    let block_range = block_index..block_range.end;
                    trace!(?key, ?range, block_index, "cache miss - no data for block");
                    return self
                        .get_from_client(bucket, &cache_key, range, block_range, part_queue_producer)
                        .await;
                }
                Err(error) => {
                    let range = range.trim_start(block_index * block_size);
                    let block_range = block_index..block_range.end;
                    trace!(?key, ?range, block_index, ?error, "error reading block from cache");
                    return self
                        .get_from_client(bucket, &cache_key, range, block_range, part_queue_producer)
                        .await;
                }
            }
        }
    }

    fn get_aligned_request_range(&self, object_size: usize, offset: u64, preferred_size: usize) -> RequestRange {
        // If the request size is bigger than a block size we will try to align it to block boundaries.
        let block_size = self.cache.block_size();
        let offset_in_part = offset % block_size;
        let size = if offset_in_part != 0 {
            // if the offset is not at the start of the part we will drain all the bytes from that part first
            let remaining_in_part = block_size - offset_in_part;
            preferred_size.min(remaining_in_part as usize)
        } else {
            // if the request size is smaller than the block size, just return the block size
            if preferred_size < block_size as usize {
                block_size as usize
            } else {
                // if it exceeds block boundaries, trim it to the block boundaries
                let request_boundary = offset + preferred_size as u64;
                let remainder = request_boundary % block_size;
                if remainder != 0 {
                    preferred_size + (block_size - remainder) as usize
                } else {
                    preferred_size
                }
            }
        };
        RequestRange::new(object_size, offset, size)
    }
}

impl<Client, Cache> CachedPartFeed<Client, Cache>
where
    Client: ObjectClient + Send + Sync + 'static,
    Cache: DataCache<CacheKey> + Send + Sync,
{
    async fn get_from_client(
        &self,
        bucket: &str,
        cache_key: &CacheKey,
        range: RequestRange,
        block_range: Range<u64>,
        part_queue_producer: PartQueueProducer<ObjectClientError<GetObjectError, Client::ClientError>>,
    ) {
        let key = &cache_key.0;
        let block_size = self.cache.block_size();
        assert!(block_size > 0);

        let block_aligned_byte_range =
            (block_range.start * block_size)..(block_range.end * block_size).min(range.object_size() as u64);

        trace!(
            ?key,
            range =? block_aligned_byte_range,
            original_range =? range,
            "fetching data from client"
        );
        let get_object_result = match self
            .client
            .get_object(
                bucket,
                key,
                Some(block_aligned_byte_range),
                Some(cache_key.1.to_owned()),
            )
            .await
        {
            Ok(get_object_result) => get_object_result,
            Err(e) => {
                error!(key, error=?e, "GetObject request failed");
                part_queue_producer.push(Err(e));
                return;
            }
        };

        pin_mut!(get_object_result);
        let mut block_index = block_range.start;
        let mut buffer = ChecksummedBytes::default();
        loop {
            match get_object_result.next().await {
                Some(Ok((offset, body))) => {
                    trace!(offset, length = body.len(), "received GetObject part");

                    // Split the body into blocks.
                    let mut body: Bytes = body.into();
                    while !body.is_empty() {
                        let remaining = (block_size as usize).saturating_sub(buffer.len()).min(body.len());
                        let chunk = body.split_to(remaining);
                        buffer.extend(chunk.into()).unwrap();
                        if buffer.len() < block_size as usize {
                            break;
                        }
                        self.update_cache(cache_key, block_index, &buffer);
                        part_queue_producer.push(Ok(make_part(key, buffer, block_index, block_size, &range)));
                        block_index += 1;
                        buffer = ChecksummedBytes::default();
                    }
                }
                Some(Err(e)) => {
                    error!(key, error=?e, "GetObject body part failed");
                    part_queue_producer.push(Err(e));
                    break;
                }
                None => {
                    if !buffer.is_empty() {
                        if buffer.len() + (block_index * block_size) as usize == range.object_size() {
                            // Write last block to the cache.
                            self.update_cache(cache_key, block_index, &buffer);
                        }
                        part_queue_producer.push(Ok(make_part(key, buffer, block_index, block_size, &range)));
                    }
                    break;
                }
            }
        }
        trace!("request finished");
    }

    fn update_cache(&self, cache_key: &CacheKey, block_index: u64, block: &ChecksummedBytes) {
        // TODO: consider updating the cache asynchronously
        metrics::counter!("cache.total_bytes", block.len() as u64, "type" => "write");
        match self.cache.put_block(cache_key.clone(), block_index, block.clone()) {
            Ok(()) => {}
            Err(error) => {
                warn!(key = cache_key.0, block_index, ?error, "failed to update cache");
            }
        };
    }
}

fn make_part(key: &str, block: ChecksummedBytes, block_index: u64, block_size: u64, range: &RequestRange) -> Part {
    let block_offset = block_index * block_size;
    let block_size = block.len();
    let part_range = range
        .trim_start(block_offset)
        .trim_end(block_offset + block_size as u64);
    trace!(
        key,
        ?part_range,
        block_index,
        block_offset,
        block_size,
        "feeding part from block"
    );

    let trim_start = (part_range.start().saturating_sub(block_offset)) as usize;
    let trim_end = (part_range.end().saturating_sub(block_offset)) as usize;
    let bytes = block.slice(trim_start..trim_end);
    Part::new(key, part_range.start(), bytes)
}

fn block_indices_for_byte_range(range: &RequestRange, block_size: u64) -> Range<BlockIndex> {
    let start_block = range.start() / block_size;
    let mut end_block = range.end() / block_size;
    if !range.is_empty() && range.end() % block_size != 0 {
        end_block += 1;
    }

    start_block..end_block
}

#[cfg(test)]
mod tests {
    // It's convenient to write test constants like "1 * 1024 * 1024" for symmetry
    #![allow(clippy::identity_op)]

    use futures::executor::{block_on, ThreadPool};
    use mountpoint_s3_client::mock_client::{ramp_bytes, MockClient, MockClientConfig, MockObject, Operation};
    use test_case::test_case;

    use crate::{
        data_cache::in_memory_data_cache::InMemoryDataCache,
        prefetch::part_queue::{unbounded_part_queue, PartQueue},
    };

    use super::*;

    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;

    #[test_case(1 * MB, 8 * MB, 16 * MB, 0, 16 * MB; "whole object")]
    #[test_case(1 * MB, 8 * MB, 16 * MB, 1 * MB, 3 * MB + 512 * KB; "aligned offset")]
    #[test_case(1 * MB, 8 * MB, 16 * MB, 512 * KB, 3 * MB; "non-aligned range")]
    #[test_case(3 * MB, 8 * MB, 14 * MB, 0, 14 * MB; "whole object, size not aligned to parts or blocks")]
    #[test_case(3 * MB, 8 * MB, 14 * MB, 9 * MB, 100 * MB; "aligned offset, size not aligned to parts or blocks")]
    #[test_case(1 * MB, 8 * MB, 100 * KB, 0, 100 * KB; "small object")]
    fn test_read_from_cache(
        block_size: usize,
        client_part_size: usize,
        object_size: usize,
        offset: usize,
        preferred_size: usize,
    ) {
        let key = "object";
        let seed = 0xaa;
        let object = MockObject::ramp(seed, object_size, ETag::for_tests());
        let etag = object.etag();

        let cache = InMemoryDataCache::new(block_size as u64);
        let bucket = "test-bucket";
        let config = MockClientConfig {
            bucket: bucket.to_string(),
            part_size: client_part_size,
        };
        let mock_client = Arc::new(MockClient::new(config));
        mock_client.add_object(key, object.clone());

        let feed = Arc::new(CachedPartFeed::new(mock_client.clone(), cache));
        let runtime = ThreadPool::builder().pool_size(1).create().unwrap();
        let range = feed.get_aligned_request_range(object_size, offset as u64, preferred_size);

        let first_read_count = {
            // First request (from client)
            let get_object_counter = mock_client.new_counter(Operation::GetObject);
            let (part_queue, part_sink) = unbounded_part_queue();
            spawn_request(&runtime, feed.clone(), bucket, key, range, &etag, part_sink);
            compare_read(key, &range, seed, part_queue);
            get_object_counter.count()
        };
        assert!(first_read_count > 0);

        let second_read_count = {
            // Second request (from cache)
            let get_object_counter = mock_client.new_counter(Operation::GetObject);
            let (part_queue, part_sink) = unbounded_part_queue();
            spawn_request(&runtime, feed.clone(), bucket, key, range, &etag, part_sink);
            compare_read(key, &range, seed, part_queue);
            get_object_counter.count()
        };
        assert_eq!(second_read_count, 0);
    }

    #[test_case(1 * MB, 8 * MB)]
    #[test_case(8 * MB, 8 * MB)]
    #[test_case(1 * MB, 5 * MB + 1)]
    #[test_case(1 * MB + 1, 5 * MB)]
    fn test_get_object_parts(block_size: usize, client_part_size: usize) {
        let key = "object";
        let object_size = 16 * MB;
        let seed = 0xaa;
        let object = MockObject::ramp(seed, object_size, ETag::for_tests());
        let etag = object.etag();

        let cache = InMemoryDataCache::new(block_size as u64);
        let bucket = "test-bucket";
        let config = MockClientConfig {
            bucket: bucket.to_string(),
            part_size: client_part_size,
        };
        let mock_client = Arc::new(MockClient::new(config));
        mock_client.add_object(key, object.clone());

        let feed = Arc::new(CachedPartFeed::new(mock_client.clone(), cache));

        let runtime = ThreadPool::builder().pool_size(1).create().unwrap();
        for offset in [0, 512 * KB, 1 * MB, 4 * MB, 9 * MB] {
            for preferred_size in [1 * KB, 512 * KB, 4 * MB, 12 * MB, 16 * MB] {
                let range = feed.get_aligned_request_range(object_size, offset as u64, preferred_size);
                let (part_queue, part_sink) = unbounded_part_queue();
                spawn_request(&runtime, feed.clone(), bucket, key, range, &etag, part_sink);
                compare_read(key, &range, seed, part_queue);
            }
        }
    }

    fn spawn_request<Client: ObjectClient>(
        runtime: &ThreadPool,
        feed: Arc<impl ObjectPartFeed<Client> + Send + Sync + 'static>,
        bucket: &str,
        key: &str,
        range: RequestRange,
        etag: &ETag,
        part_sink: PartQueueProducer<ObjectPartFeedError<Client::ClientError>>,
    ) {
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let etag = etag.to_owned();
        let preferred_part_size = 128 * KB;
        runtime.spawn_ok(async move {
            feed.get_object_parts(&bucket, &key, range, etag, preferred_part_size, part_sink)
                .await
        });
    }

    fn compare_read(
        key: &str,
        range: &RequestRange,
        seed: u8,
        part_queue: PartQueue<ObjectPartFeedError<impl std::error::Error + Send + Sync + 'static>>,
    ) {
        let mut offset = range.start();
        let mut remaining = range.len();
        while remaining > 0 {
            let part = block_on(part_queue.read(remaining)).unwrap();
            let bytes = part.into_bytes(key, offset).unwrap();

            let expected = ramp_bytes(seed as usize + offset as usize, bytes.len());
            let bytes = bytes.into_bytes().unwrap();
            assert_eq!(bytes, expected);

            offset += bytes.len() as u64;
            remaining -= bytes.len();
        }
    }
}
