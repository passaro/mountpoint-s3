use futures::future::RemoteHandle;
use mountpoint_s3_client::ObjectClient;

use super::PrefetchReadError;
use super::controller::PrefetchController;
use super::part::Part;
use super::part_queue::PartQueue;
use super::part_stream::RequestRange;

/// A single GetObject request submitted to the S3 client
#[derive(Debug)]
pub struct RequestTask<Client: ObjectClient> {
    /// Handle on the task/future. The future is cancelled when handle is dropped. This is None if
    /// the request is fake (created by seeking backwards in the stream)
    _task_handle: RemoteHandle<()>,
    remaining: usize,
    range: RequestRange,
    part_queue: PartQueue<Client>,
    prefetch_controller: PrefetchController,
}

impl<Client: ObjectClient> RequestTask<Client> {
    pub fn from_handle(
        task_handle: RemoteHandle<()>,
        range: RequestRange,
        part_queue: PartQueue<Client>,
        backpressure_controller: PrefetchController,
    ) -> Self {
        Self {
            _task_handle: task_handle,
            remaining: range.len(),
            range,
            part_queue,
            prefetch_controller: backpressure_controller,
        }
    }

    // Push a given list of parts in front of the part queue
    pub async fn push_front(&mut self, parts: Vec<Part>) -> Result<(), PrefetchReadError<Client::ClientError>> {
        // Iterate backwards to push each part to the front of the part queue
        for part in parts.into_iter().rev() {
            self.remaining += part.len();
            self.prefetch_controller.backward_seek(part.len());
            self.part_queue.push_front(part).await?;
        }
        Ok(())
    }

    pub async fn read(&mut self, length: usize) -> Result<Part, PrefetchReadError<Client::ClientError>> {
        self.prefetch_controller.update(length, self.available_offset()).await;

        let part = self.part_queue.read(length).await?;
        debug_assert!(part.len() <= self.remaining);
        self.remaining -= part.len();

        self.prefetch_controller.mark_read(part.len(), part.offset());

        // let next_offset = part.offset() + part.len() as u64;
        // let remaining_in_queue = self.available_offset().saturating_sub(next_offset) as usize;
        // // If the part queue is empty it means we are reading faster than the task could prefetch,
        // // so we should use larger window for the task.
        // if remaining_in_queue == 0 {
        //     self.backpressure_controller.send_feedback(PartQueueStall).await?;
        // }

        Ok(part)
    }

    pub fn start_offset(&self) -> u64 {
        self.range.start()
    }

    pub fn end_offset(&self) -> u64 {
        self.range.end()
    }

    pub fn total_size(&self) -> usize {
        self.range.len()
    }

    pub fn remaining(&self) -> usize {
        self.remaining
    }

    /// Maximum offset which data is known to be already in the `self.part_queue`
    pub fn available_offset(&self) -> u64 {
        self.start_offset() + self.part_queue.bytes_received() as u64
    }

    pub fn requested_offset(&self) -> u64 {
        self.prefetch_controller.requested_offset()
    }
}
