use std::{collections::VecDeque, ops::Range, sync::Arc};

use aws_sdk_s3_transfer_manager::{Client as TMClient, io::AggregatedBytes, operation::download::DownloadHandle};
use bytes::{Buf, Bytes, BytesMut};
use mountpoint_s3_client::types::ETag;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tracing::{debug, trace};

use crate::{
    mem_limiter::{BufferArea, MemoryLimiter},
    s3::Bucket,
};

use super::{DataLayer, Download};

#[derive(Debug, Clone, Copy)]
pub struct PrefetchConfig {
    pub initial_prefetch_size: usize,
    pub prefetch_multiplier: usize,
    pub max_prefetch_size: usize,
    pub min_prefetch_size: usize,
}

/// Data layer implemented using the Rust Transfer Manager
#[derive(Debug, Clone)]
pub struct TMDataLayer {
    client: TMClient,
    limiter: Arc<MemoryLimiter>,
    config: PrefetchConfig,
}

impl TMDataLayer {
    pub fn new(client: TMClient, config: PrefetchConfig, limiter: Arc<MemoryLimiter>) -> Self {
        Self {
            client,
            limiter,
            config,
        }
    }
}

impl DataLayer for TMDataLayer {
    fn download(&self, bucket: String, key: String, etag: ETag, size: usize) -> impl Download {
        let bucket = Bucket::new(bucket).unwrap();
        let object_info = ObjectInfo::new(bucket, key, etag, size);
        TMDownload::new(object_info, self.clone())
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ObjectInfo {
    bucket: Bucket,
    key: String,
    etag: ETag,
    size: usize,
}

impl std::fmt::Debug for ObjectInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectInfo")
            .field("bucket", &self.bucket.as_str())
            .field("key", &self.key.as_str())
            .field("etag", &self.etag.as_str())
            .field("size", &self.size)
            .finish()
    }
}

impl ObjectInfo {
    pub fn new(bucket: Bucket, key: String, etag: ETag, size: usize) -> Self {
        Self {
            bucket,
            key,
            etag,
            size,
        }
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn etag(&self) -> &ETag {
        &self.etag
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

/// A download handle for a specific object using the Transfer Manager
struct TMDownload {
    object_info: Arc<ObjectInfo>,
    tm: TMDataLayer,
    current_offset: u64,
    current_chunk: Option<TMBuffer>,
    requests: VecDeque<Request>,
    prefetch_size: usize,
}

impl TMDownload {
    fn new(object_info: ObjectInfo, tm: TMDataLayer) -> Self {
        Self {
            object_info: Arc::new(object_info),
            tm,
            current_offset: 0,
            current_chunk: None,
            requests: Default::default(),
            prefetch_size: 0,
        }
    }

    async fn read(&mut self, offset: u64, length: usize) -> Result<Bytes, anyhow::Error> {
        if length == 0 || offset >= self.object_info.size() as u64 {
            return Ok(Bytes::default());
        }

        if length + offset as usize > self.object_info.size() {
            return Err(anyhow::anyhow!("read would exceed object size"));
        }

        self.adjust_requests(offset, length)?;

        trace!(offset, length, object = ?self.object_info, "read request");

        let mut buffer = BytesMut::new();
        while buffer.len() < length {
            let Some(chunk) = self.next_chunk().await? else {
                break;
            };
            trace!(
                chunk_len = chunk.len(),
                required_len = length - buffer.len(),
                "read from chunk"
            );

            if buffer.is_empty() && chunk.len() >= length {
                // Happy case: contiguous data available
                let read_chunk = chunk.copy_to_bytes(length);
                self.current_offset += length as u64;
                self.tm.limiter.release(BufferArea::Prefetch, length as u64);
                return Ok(read_chunk);
            }

            let to_read = (length - buffer.len()).min(chunk.len());
            let read_chunk = chunk.copy_to_bytes(to_read);
            buffer.extend_from_slice(&read_chunk);
            self.current_offset += to_read as u64;
            self.tm.limiter.release(BufferArea::Prefetch, to_read as u64);
        }

        Ok(buffer.freeze())
    }

    async fn next_chunk(&mut self) -> Result<Option<&mut TMBuffer>, anyhow::Error> {
        if self.current_chunk.as_ref().is_none_or(|chunk| chunk.is_empty()) {
            while let Some(request) = self.requests.front_mut() {
                if let Ok(chunk) = request.receiver.try_recv() {
                    self.current_chunk = Some(chunk?);
                    break;
                }
                self.tm.config.scale_up(&mut self.prefetch_size);
                if let Some(chunk) = request.receiver.recv().await.transpose()? {
                    self.current_chunk = Some(chunk);
                    break;
                } else {
                    self.requests.pop_front();
                }
            }
        }

        Ok(self.current_chunk.as_mut())
    }

    fn adjust_requests(&mut self, offset: u64, length: usize) -> Result<(), anyhow::Error> {
        if offset != self.current_offset {
            trace!(
                new_offset = offset,
                previous_offset = self.current_offset,
                requests_to_cancel = self.requests.len(),
                "non-sequential read"
            );
            let requested = self
                .requests
                .back()
                .map(|r| r.range.end)
                .unwrap_or(offset)
                .saturating_sub(offset);
            self.tm.limiter.release(BufferArea::Prefetch, requested);

            self.requests.clear();
            self.current_chunk = None;
            self.prefetch_size = 0;
            self.current_offset = offset;
        }

        let requested_offset = self.requests.back().map(|r| r.range.end).unwrap_or(offset);
        let requested_size = requested_offset.saturating_sub(offset) as usize;
        if requested_size <= length.max(self.prefetch_size / 2) && requested_offset < self.object_info.size() as u64 {
            let range = self.reserve_memory_for_next_request(requested_offset, requested_size, length);

            debug!(?range, request_size = range.end - range.start, prefetch_size = self.prefetch_size, read_offset = offset, total_prefetch = range.end - offset, request_count = self.requests.len() + 1, object_info =? self.object_info, "initiating request");

            let download = self
                .tm
                .client
                .download()
                .bucket(self.object_info.bucket())
                .key(self.object_info.key())
                .if_match(self.object_info.etag().as_str())
                .range(format_as_byte_range(&range))
                .initiate()?;

            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            let handle = tokio::spawn(handle_download(download, sender));
            let request = Request {
                _handle: handle,
                receiver,
                range,
            };
            self.requests.push_back(request);
        }

        assert!(!self.requests.is_empty(), "we have at least one request");

        Ok(())
    }

    fn reserve_memory_for_next_request(
        &mut self,
        requested_offset: u64,
        requested_size: usize,
        length: usize,
    ) -> Range<u64> {
        let mut can_scale_down = true;
        loop {
            // Ensure we cover the required size.
            if self.prefetch_size + requested_size < length {
                self.tm.config.scale_up(&mut self.prefetch_size);
                can_scale_down = false;
                continue;
            }

            let size = self.prefetch_size.clamp(
                length.saturating_sub(requested_size),
                self.tm.config.max_prefetch_size.saturating_sub(requested_size),
            );
            let end = (requested_offset + size as u64).min(self.object_info.size() as u64);
            let request_size = end - requested_offset;

            if self.tm.limiter.try_reserve(BufferArea::Prefetch, request_size) {
                return requested_offset..end;
            }

            if can_scale_down {
                self.tm.config.scale_down(&mut self.prefetch_size);
            } else {
                // Force reservation of the required size.
                self.tm.limiter.reserve(BufferArea::Prefetch, request_size);
                return requested_offset..end;
            }
        }
    }
}

impl PrefetchConfig {
    pub fn new(part_size: usize) -> Self {
        Self {
            initial_prefetch_size: (1024 + 128) * 1024,
            prefetch_multiplier: 2,
            max_prefetch_size: (2 * 1024 * 1024 * 1024).max(part_size),
            min_prefetch_size: part_size,
        }
    }

    fn scale_up(&self, value: &mut usize) {
        *value = if *value == 0 {
            self.initial_prefetch_size
        } else {
            (*value * self.prefetch_multiplier).clamp(self.min_prefetch_size, self.max_prefetch_size)
        };
    }

    fn scale_down(&self, value: &mut usize) {
        *value = if *value <= self.initial_prefetch_size {
            self.initial_prefetch_size
        } else {
            (*value / self.prefetch_multiplier).clamp(self.min_prefetch_size, self.max_prefetch_size)
        };
    }
}

impl Download for TMDownload {
    async fn read(&mut self, offset: u64, length: usize) -> Result<Bytes, anyhow::Error> {
        self.read(offset, length).await
    }
}

#[derive(Debug)]
struct Request {
    _handle: JoinHandle<()>,
    receiver: UnboundedReceiver<Result<TMBuffer, anyhow::Error>>,
    range: Range<u64>,
}

fn format_as_byte_range(range: &Range<u64>) -> String {
    format!("bytes={}-{}", range.start, range.end.saturating_sub(1))
}

async fn handle_download(mut download: DownloadHandle, sender: UnboundedSender<Result<TMBuffer, anyhow::Error>>) {
    while let Some(chunk) = download.body_mut().next().await {
        match chunk {
            Ok(chunk) => {
                let data = TMBuffer::new(chunk.data);
                _ = sender.send(Ok(data));
            }
            Err(e) => {
                _ = sender.send(Err(e.into()));
                break;
            }
        }
    }
}

#[derive(Debug)]
struct TMBuffer {
    inner: AggregatedBytes,
}

impl TMBuffer {
    fn new(chunk_data: AggregatedBytes) -> Self {
        Self { inner: chunk_data }
    }

    fn is_empty(&self) -> bool {
        !self.inner.has_remaining()
    }

    fn len(&self) -> usize {
        self.inner.remaining()
    }

    fn copy_to_bytes(&mut self, length: usize) -> Bytes {
        self.inner.copy_to_bytes(length)
    }
}
