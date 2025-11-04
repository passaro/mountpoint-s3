use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::stream::FusedStream;
use futures::{
    FutureExt, SinkExt, Stream, StreamExt as _,
    channel::mpsc::{UnboundedReceiver, unbounded},
    future::RemoteHandle,
    pin_mut,
    task::SpawnExt,
};
use mountpoint_s3_client::{
    ObjectClient,
    error::{GetObjectError, ObjectClientError},
    error_metadata::ProvideErrorMetadata,
    types::{GetBodyPart, GetObjectParams},
};
use pin_project::pin_project;
use thiserror::Error;
use tracing::{error, trace};

use crate::{
    Runtime,
    fs::error_metadata::{ErrorMetadata, MOUNTPOINT_ERROR_CLIENT},
    object::ObjectId,
};

// #[derive(Debug)]
// pub struct Prefetcher<Client> {
//     client: Client,
// }

// impl<Client> Prefetcher<Client>
// where
//     Client: ObjectClient,
// {
//     pub fn prefetch(&self, bucket: String, object_id: ObjectId, size: u64) -> ObjectReader {
//         ObjectReader {  }
//     }
// }

// pub struct ObjectReader {

// }

#[derive(Debug)]
pub struct Scheduler<Client> {
    client: Client,
    runtime: Runtime,
}

impl<Client> Scheduler<Client>
where
    Client: ObjectClient + Clone + Send + 'static,
{
    pub fn new(client: Client, runtime: Runtime) -> Self {
        Self { client, runtime }
    }

    pub fn download(&self, bucket: String, object_id: ObjectId, size: u64) -> Download<Client> {
        Download::new(self, bucket, object_id, size)
    }
}

async fn get_part<Client>(
    client: Client,
    bucket: String,
    object_id: ObjectId,
    params: &GetObjectParams,
) -> Result<GetBodyPart, DownloadError<Client::ClientError>>
where
    Client: ObjectClient + Clone + Send + 'static,
{
    let response = client
        .get_object(&bucket, object_id.key(), &params)
        .await
        .inspect_err(|e| error!(key=object_id.key(), error=?e, "GetObject failed"))
        .map_err(|err| DownloadError::get_request_failed(err, &bucket, &object_id.key()))?;
    pin_mut!(response);
    let Some(next) = response.next().await else {
        return Err(DownloadError::UnexpectedEmptyGetRequest);
    };

    let part = next
        .inspect_err(|e| error!(key=object_id.key(), error=?e, "GetObject body part failed"))
        .map_err(|err| DownloadError::get_request_failed(err, &bucket, &object_id.key()))?;
    let length = part.data.len() as u64;
    trace!(offset = part.offset, length, "received GetObject part");
    metrics::counter!("s3.client.total_bytes", "type" => "read").increment(length);
    Ok(part)
}

#[pin_project]
pub struct Download<Client>
where
    Client: ObjectClient,
{
    #[pin]
    receiver: UnboundedReceiver<RemoteHandle<Result<GetBodyPart, DownloadError<Client::ClientError>>>>,
    #[pin]
    current: Option<RemoteHandle<Result<GetBodyPart, DownloadError<Client::ClientError>>>>,
    _handle: RemoteHandle<()>,
    _bucket: String,
    _object_id: ObjectId,
}

impl<Client> Download<Client>
where
    Client: ObjectClient + Send + Clone + 'static,
{
    fn new(scheduler: &Scheduler<Client>, bucket: String, object_id: ObjectId, size: u64) -> Self {
        let (mut sender, receiver) = unbounded();

        let handle = {
            let client = scheduler.client.clone();
            let bucket = bucket.clone();
            let object_id = object_id.clone();
            let runtime = scheduler.runtime.clone();
            scheduler.runtime
                .spawn_with_handle(async move {
                    let params = GetObjectParams::new().if_match(Some(object_id.etag().clone()));
                    let part_size = client.read_part_size() as u64;

                    let mut offset = 0;
                    while offset < size {
                        let end = (offset + part_size).min(size);
                        let params = params.clone().range(Some(offset..end));
                        let client = client.clone();
                        let bucket = bucket.clone();
                        let object_id = object_id.clone();
                        let response = runtime
                            .spawn_with_handle(async move { get_part(client, bucket, object_id, &params).await })
                            .unwrap();
                        _ = sender.send(response).await;
                        offset = end;
                    }
                })
                .unwrap()
        };

        Self {
            receiver,
            current: None,
            _handle: handle,
            _bucket: bucket,
            _object_id: object_id,
        }
    }
}

impl<Client> Stream for Download<Client>
where
    Client: ObjectClient + 'static,
{
    type Item = Result<GetBodyPart, DownloadError<Client::ClientError>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.as_mut().current.take() {
            Some(mut handle) => match handle.poll_unpin(cx) {
                Poll::Ready(result) => {
                    return Poll::Ready(Some(result));
                }
                Poll::Pending => {
                    _ = self.current.insert(handle);
                    return Poll::Pending;
                }
            },
            None => {}
        }

        let mut this = self.project();
        if this.receiver.is_terminated() {
            return Poll::Ready(None);
        }

        let handle = match this.receiver.poll_next(cx) {
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(mut handle)) => match handle.poll_unpin(cx) {
                Poll::Ready(result) => return Poll::Ready(Some(result)),
                Poll::Pending => handle,
            },
            Poll::Pending => return Poll::Pending,
        };

        _ = this.current.insert(handle);
        Poll::Pending
    }
}

#[derive(Debug, Error)]
pub enum DownloadError<E> {
    #[error("get object request failed")]
    GetRequestFailed {
        source: ObjectClientError<GetObjectError, E>,
        metadata: Box<ErrorMetadata>,
    },
    #[error("get object request failed")]
    UnexpectedEmptyGetRequest,
}

impl<E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static> DownloadError<E> {
    fn get_request_failed(err: ObjectClientError<GetObjectError, E>, bucket: &str, key: &str) -> Self {
        let metadata = ErrorMetadata {
            client_error_meta: err.meta(),
            error_code: Some(MOUNTPOINT_ERROR_CLIENT.to_string()),
            s3_bucket_name: Some(bucket.to_string()),
            s3_object_key: Some(key.to_string()),
        };
        let metadata = Box::new(metadata);
        Self::GetRequestFailed { source: err, metadata }
    }
}
