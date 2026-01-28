use futures::Future;
use mountpoint_s3_client::types::ETag;

mod adapter;
mod buffer;
mod tm;

pub use adapter::create_data_layer;
pub use buffer::Buffer;
pub use tm::{PrefetchConfig, TMDataLayer};

pub trait DataLayer {
    fn download(&self, bucket: String, key: String, etag: ETag, size: usize) -> impl Download;
}

pub trait Download: Send {
    fn read(&mut self, offset: u64, length: usize) -> impl Future<Output = Result<Buffer, anyhow::Error>> + Send;
}
