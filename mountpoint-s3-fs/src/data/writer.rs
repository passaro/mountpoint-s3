//! Uploading one object through a single RTM transfer.
//!
//! Bytes take this route:
//!
//! ```text
//! write_at ─▶ PartSink ─(bounded, in bytes)─▶ PartSource ─▶ RTM upload ─▶ S3
//! ```
//!
//! [`RtmWriter`] owns the sink and the upload handle; the [`part_channel`](crate::data::part_channel)
//! between them is a single-part rendezvous — the writer fills one part, then blocks until RTM takes
//! it — so at most one part is ever buffered. That block is the backpressure that keeps buffered
//! bytes bounded when the caller outruns the network.
//!
//! Three constraints shape the API:
//!
//! - **The object size is not declared.** A filesystem writer does not know it when the file is
//!   opened, so the upload streams to end-of-stream with no upper bound (RTM's unknown-length
//!   streaming upload support, PR #164). The object is capped at `part_size * 10,000`; overrunning
//!   it fails the transfer partway through, surfacing as [`WriteError::Transfer`], since without a
//!   declared size it cannot be caught up front.
//! - **Cancellation must be explicit.** Dropping an RTM `UploadHandle` without `join()` or
//!   `abort()` cancels the transfer without issuing `AbortMultipartUpload`, leaving a partial
//!   multipart upload on S3. `Drop` cannot make that call, since it is async, so it logs a
//!   warning instead — callers must use [`Writer::abort`].
//! - **Appending to an existing object is not supported.** RTM's upload builder exposes no
//!   write offset and no `if_match`, so there is no way to express it.

use std::sync::Mutex;

use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::operation::upload::UploadHandle;
use aws_sdk_s3_transfer_manager::types::PartSize;
use tracing::{debug, trace, warn};

use crate::data::part_channel::{self, Closed, PartSink};
use crate::data::priority::PriorityTable;
use crate::data::{Urgency, WriteError, WriteOutcome, WriteSpec, Writer, WriterStats};

/// RTM's default upload part size when the client is left on `PartSize::Auto`.
const AUTO_UPLOAD_PART_SIZE: usize = 8 * 1024 * 1024;

/// Tunables for the write path.
#[derive(Debug, Clone, Default)]
pub struct WriterConfig {
    /// Urgency-to-priority mapping, shared with the read path.
    ///
    /// An upload runs at [`Urgency::Demand`] throughout: nothing is ever uploaded
    /// speculatively, so every buffered byte is one a caller has already written and is
    /// waiting on. The table is configurable so an upload's share against concurrent reads
    /// can be varied.
    pub priorities: PriorityTable,
}

/// The size the sink cuts parts to — RTM's own upload part size for this client, so pushed parts
/// match what RTM expects. `Target` is already clamped to the 5 MiB S3 floor by RTM's config
/// builder; `Auto` uploads use 8 MiB.
fn upload_part_size(tm: &aws_sdk_s3_transfer_manager::Client) -> usize {
    match tm.config().part_size() {
        PartSize::Target(bytes) => *bytes as usize,
        _ => AUTO_UPLOAD_PART_SIZE,
    }
}

/// The result of a completed upload.
pub type WriteResult = WriteOutcome;

/// Writes one object through a single RTM upload.
pub struct RtmWriter {
    /// `None` once [`complete`](Writer::complete) or [`abort`](Writer::abort) has consumed it.
    handle: Option<UploadHandle>,
    /// `None` after the sink has been dropped at completion.
    sink: Option<PartSink>,
    key: String,
    /// Offset the next write must arrive at, i.e. the current end of the stream.
    next_offset: u64,
    stats: Mutex<WriterStats>,
}

impl std::fmt::Debug for RtmWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RtmWriter")
            .field("key", &self.key)
            .field("next_offset", &self.next_offset)
            .field("in_progress", &self.handle.is_some())
            .finish_non_exhaustive()
    }
}

impl RtmWriter {
    /// Initiate an upload.
    ///
    /// The upload streams to end-of-stream with no declared size — see the module docs. Fails
    /// only if the transfer layer rejects the initiate call.
    pub fn open(
        tm: &aws_sdk_s3_transfer_manager::Client,
        spec: WriteSpec,
        config: &WriterConfig,
    ) -> Result<Self, WriteError> {
        let (sink, source) = part_channel::channel(upload_part_size(tm));
        // No size hint is set on the source, so RTM reads it to end-of-stream rather than
        // dispatching a part budget computed from a declared length. The source hands over one
        // part at a time and blocks the writer until RTM takes it — see `part_channel`.

        let handle = tm
            .upload()
            .bucket(&spec.bucket)
            .key(&spec.key)
            .body(InputStream::from_part_stream(source))
            .initiate()
            .map_err(|e| WriteError::Transfer(Box::new(e)))?;

        // Uploads carry no speculative work, so this priority holds for the whole transfer.
        handle
            .scheduling()
            .set_priority(config.priorities.priority_for(Urgency::Demand));

        trace!(key = %spec.key, "upload initiated");

        Ok(Self {
            handle: Some(handle),
            sink: Some(sink),
            key: spec.key,
            next_offset: 0,
            stats: Mutex::new(WriterStats::default()),
        })
    }

    /// Bytes accepted so far, i.e. the current end of the stream.
    pub fn size(&self) -> u64 {
        self.next_offset
    }

    /// Translate a closed channel into the reason the transfer ended.
    ///
    /// A closed channel only says the transfer dropped the stream; the cause lives on the
    /// handle, and `join()` is what recovers it. Consumes the handle, so `handle` is `None`
    /// afterwards.
    async fn diagnose_closed(&mut self) -> WriteError {
        match self.handle.take() {
            Some(handle) => match handle.join().await {
                // Completed while the caller still had bytes to write, so the object is
                // shorter than intended. Reported rather than passed off as success.
                Ok(_) => WriteError::NotInProgress,
                Err(e) => WriteError::Transfer(Box::new(e)),
            },
            None => WriteError::NotInProgress,
        }
    }
}

impl Writer for RtmWriter {
    async fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<usize, WriteError> {
        if offset != self.next_offset {
            // Append-only: a part already sent cannot be revised, so a write anywhere but
            // the current end cannot be honoured.
            return Err(WriteError::OutOfOrderWrite {
                write_offset: offset,
                expected_offset: self.next_offset,
            });
        }
        if data.is_empty() {
            return Ok(0);
        }

        let end = self.next_offset.saturating_add(data.len() as u64);

        let Some(sink) = self.sink.as_mut() else {
            return Err(WriteError::NotInProgress);
        };

        match sink.write(data).await {
            Ok(stalls) => {
                self.next_offset = end;
                let mut stats = self.stats.lock().expect("writer stats lock poisoned");
                stats.bytes_accepted += data.len() as u64;
                stats.write_stalls += stalls;
                Ok(data.len())
            }
            // The transfer ended. Report why, not just that the channel closed.
            Err(Closed) => Err(self.diagnose_closed().await),
        }
    }

    async fn complete(mut self) -> Result<WriteOutcome, WriteError> {
        // Close first, then join: closing is how the stream signals EOF, so joining before it
        // would wait for a part that is never coming. `finish` flushes the final part, which the
        // transfer must take — so a transfer that died first surfaces here as `Closed`.
        let Some(mut sink) = self.sink.take() else {
            return Err(WriteError::NotInProgress);
        };
        let finished = sink.finish().await;
        drop(sink);
        if finished.is_err() {
            return Err(self.diagnose_closed().await);
        }

        let Some(handle) = self.handle.take() else {
            return Err(WriteError::NotInProgress);
        };

        let size = self.next_offset;
        let output = handle.join().await.map_err(|e| WriteError::Transfer(Box::new(e)))?;

        {
            let mut stats = self.stats.lock().expect("writer stats lock poisoned");
            // Everything accepted has now been sent, by definition of a successful join.
            stats.bytes_dispatched = stats.bytes_accepted;
        }

        // A multipart upload reports an upload id; a single PUT does not.
        let multipart = output.upload_id().is_some();
        debug!(key = %self.key, size, multipart, "upload completed");

        Ok(WriteOutcome {
            etag: output.e_tag().map(str::to_owned),
            size,
            multipart,
        })
    }

    async fn abort(mut self) -> Result<(), WriteError> {
        // Drop the sink first so the stream terminates and nothing is left waiting on a part
        // that will not arrive.
        self.sink = None;

        let Some(handle) = self.handle.take() else {
            return Err(WriteError::NotInProgress);
        };

        // Awaited, because this is what issues `AbortMultipartUpload`. Dropping the handle
        // instead would cancel the transfer and leave the partial upload on S3.
        let aborted = handle.abort().await.map_err(|e| WriteError::Transfer(Box::new(e)))?;
        debug!(key = %self.key, upload_id = ?aborted.upload_id(), "upload aborted");
        Ok(())
    }

    fn stats(&self) -> WriterStats {
        *self.stats.lock().expect("writer stats lock poisoned")
    }
}

impl Drop for RtmWriter {
    fn drop(&mut self) {
        if self.handle.is_some() {
            // Nothing can be done about it here: issuing `AbortMultipartUpload` is async and
            // `Drop` is not. The transfer will be cancelled, but any multipart upload it
            // started stays on S3 until a lifecycle rule reaps it. Warned about loudly because
            // the leftover is remote state this process can no longer clean up.
            warn!(
                key = %self.key,
                "RtmWriter dropped without complete() or abort(); any multipart upload is left \
                 incomplete on S3. Call abort() explicitly."
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_spec_carries_bucket_and_key() {
        let spec = WriteSpec::new("bucket", "key");
        assert_eq!(spec.bucket, "bucket");
        assert_eq!(spec.key, "key");
    }

    #[test]
    fn an_upload_runs_at_demand_priority() {
        // Uploads have no speculative half, so this is the only priority they ever carry.
        assert_eq!(
            WriterConfig::default().priorities.priority_for(Urgency::Demand),
            PriorityTable::default().demand
        );
    }
}
