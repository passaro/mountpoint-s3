use std::{fmt::Debug, ops::Range, sync::Arc};

use bytes::Bytes;
use futures::task::SpawnExt;
use futures::{pin_mut, task::Spawn, StreamExt};
use mountpoint_s3_client::error::{GetObjectError, ObjectClientError};
use mountpoint_s3_client::{types::ETag, ObjectClient};
use mountpoint_s3_crt::checksums::crc32c;
use tracing::{debug_span, error, trace, Instrument};

use crate::checksums::ChecksummedBytes;
use crate::prefetch::part::Part;
use crate::prefetch::part_queue::unbounded_part_queue;
use crate::prefetch::task::{PrefetchReadError, RequestTask};

pub type TaskError<Client> = ObjectClientError<GetObjectError, <Client as ObjectClient>::ClientError>;

/// A generic interface to retrieve data from objects in a S3-like store.
pub trait ObjectPartFeed<Client: ObjectClient, Runtime: Spawn> {
    /// Spawns a request to get the content of an object. The object data will be retrieved in fixed size
    /// parts and can then be consumed using [RequestTask::read]. Callers need to specify a preferred
    /// size for the parts, but implementations are allowed to ignore it.
    fn spawn_get_object_request(
        &self,
        bucket: &str,
        key: &str,
        if_match: ETag,
        range: RequestRange,
        preferred_part_size: usize,
    ) -> RequestTask<TaskError<Client>>;
}

/// The range of a [ObjectPartFeed::get_object_parts] request.
/// Includes the total size of the object.
#[derive(Clone, Copy)]
pub struct RequestRange {
    object_size: usize,
    offset: u64,
    size: usize,
}

impl RequestRange {
    pub fn new(object_size: usize, offset: u64, size: usize) -> Self {
        let size = size.min(object_size.saturating_sub(offset as usize));
        Self {
            object_size,
            offset,
            size,
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn object_size(&self) -> usize {
        self.object_size
    }

    pub fn start(&self) -> u64 {
        self.offset
    }

    pub fn end(&self) -> u64 {
        self.offset + self.size as u64
    }

    pub fn trim_start(&self, start_offset: u64) -> Self {
        let offset = start_offset.max(self.offset);
        let size = self.end().saturating_sub(offset) as usize;
        Self {
            object_size: self.object_size,
            offset,
            size,
        }
    }

    pub fn trim_end(&self, end_offset: u64) -> Self {
        let end = end_offset.min(self.end());
        let size = end.saturating_sub(self.offset) as usize;
        Self {
            object_size: self.object_size,
            offset: self.offset,
            size,
        }
    }
}

impl From<RequestRange> for Range<u64> {
    fn from(val: RequestRange) -> Self {
        val.start()..val.end()
    }
}

impl Debug for RequestRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{} out of {}", self.start(), self.end(), self.object_size())
    }
}

/// [ObjectPartFeed] implementation which delegates retrieving object data to a [Client].
#[derive(Debug)]
pub struct ClientPartFeed<Client, Runtime> {
    client: Arc<Client>,
    runtime: Runtime,
}

impl<Client, Runtime> ClientPartFeed<Client, Runtime> {
    pub fn new(client: Arc<Client>, runtime: Runtime) -> Self {
        Self { client, runtime }
    }
}

impl<Client, Runtime> ObjectPartFeed<Client, Runtime> for ClientPartFeed<Client, Runtime>
where
    Client: ObjectClient + Send + Sync + 'static,
    Runtime: Spawn,
{
    fn spawn_get_object_request(
        &self,
        bucket: &str,
        key: &str,
        if_match: ETag,
        range: RequestRange,
        preferred_part_size: usize,
    ) -> RequestTask<TaskError<Client>> {
        assert!(preferred_part_size > 0);
        let request_range = get_aligned_request_range(range, self.client.part_size().unwrap_or(8 * 1024 * 1024));
        let start = request_range.start();
        let size = request_range.len();

        let (part_queue, part_queue_producer) = unbounded_part_queue();
        trace!(range=?request_range, "spawning request");

        let request_task = {
            let client = self.client.clone();
            let bucket = bucket.to_owned();
            let key = key.to_owned();
            let span = debug_span!("prefetch", range=?request_range);

            async move {
                let get_object_result = match client
                    .get_object(&bucket, &key, Some(request_range.into()), Some(if_match))
                    .await
                {
                    Ok(get_object_result) => get_object_result,
                    Err(e) => {
                        error!(error=?e, "GetObject request failed");
                        part_queue_producer.push(Err(PrefetchReadError::GetRequestFailed(e)));
                        return;
                    }
                };

                pin_mut!(get_object_result);
                loop {
                    match get_object_result.next().await {
                        Some(Ok((offset, body))) => {
                            trace!(offset, length = body.len(), "received GetObject part");
                            // pre-split the body into multiple parts as suggested by preferred part size
                            // in order to avoid validating checksum on large parts at read.
                            let mut body: Bytes = body.into();
                            let mut curr_offset = offset;
                            loop {
                                let chunk_size = preferred_part_size.min(body.len());
                                if chunk_size == 0 {
                                    break;
                                }
                                let chunk = body.split_to(chunk_size);
                                // S3 doesn't provide checksum for us if the request range is not aligned to
                                // object part boundaries, so we're computing our own checksum here.
                                let checksum = crc32c::checksum(&chunk);
                                let checksum_bytes = ChecksummedBytes::new(chunk, checksum);
                                let part = Part::new(&key, curr_offset, checksum_bytes);
                                curr_offset += part.len() as u64;
                                part_queue_producer.push(Ok(part));
                            }
                        }
                        Some(Err(e)) => {
                            error!(error=?e, "GetObject body part failed");
                            part_queue_producer.push(Err(PrefetchReadError::GetRequestFailed(e)));
                            break;
                        }
                        None => break,
                    }
                }
                trace!("request finished");
            }
            .instrument(span)
        };

        let task_handle = self.runtime.spawn_with_handle(request_task).unwrap();

        RequestTask::from_handle(task_handle, size, start, part_queue)
    }
}

fn get_aligned_request_range(range: RequestRange, part_alignment: usize) -> RequestRange {
    let object_size = range.object_size();
    let offset = range.start();
    let preferred_length = range.len();

    // If the request size is bigger than a part size we will try to align it to part boundaries.
    let offset_in_part = (offset % part_alignment as u64) as usize;
    let size = if offset_in_part != 0 {
        // if the offset is not at the start of the part we will drain all the bytes from that part first
        let remaining_in_part = part_alignment - offset_in_part;
        preferred_length.min(remaining_in_part)
    } else {
        // if the request size is smaller than the part size, just return that value
        if preferred_length < part_alignment {
            preferred_length
        } else {
            // if it exceeds part boundaries, trim it to the part boundaries
            let request_boundary = offset + preferred_length as u64;
            let remainder = (request_boundary % part_alignment as u64) as usize;
            preferred_length - remainder
        }
    };
    RequestRange::new(object_size, offset, size)
}
