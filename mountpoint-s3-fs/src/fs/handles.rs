use std::str::FromStr as _;

use mountpoint_s3_client::ObjectClient;
use mountpoint_s3_client::types::ETag;
use tracing::{debug, error};

use crate::data::{DataPlane, ObjectSpec, WriteSpec, Writer};
use crate::fs::InodeError;
use crate::memory::WriteHandleSlot;
use crate::metablock::{Lookup, Metablock, PendingUploadHook, ReadWriteMode, S3Location};
use crate::object::ObjectId;
use crate::s3::Bucket;
use crate::sync::{Arc, AsyncMutex};

use super::{Error, InodeNo, OpenFlags, S3Filesystem, ToErrno};

pub struct FileHandle<DP: DataPlane> {
    pub ino: InodeNo,
    pub location: S3Location,
    pub state: AsyncMutex<FileHandleState<DP>>,
    /// Process that created the handle
    pub open_pid: u32,
}

impl<DP: DataPlane> std::fmt::Debug for FileHandle<DP> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileHandle")
            .field("ino", &self.ino)
            .field("location", &self.location)
            .field("open_pid", &self.open_pid)
            .finish_non_exhaustive()
    }
}

impl<DP: DataPlane> FileHandle<DP> {
    pub fn file_name(&self) -> &str {
        self.location.name()
    }
}

pub enum FileHandleState<DP: DataPlane> {
    /// The file handle has been assigned as a read handle
    Read {
        reader: DP::Reader,
        /// Set to true when `flush` called on the handle, and unset on a `read`
        flushed: bool,
    },
    /// The file handle has been assigned as a write handle
    Write {
        state: UploadState<DP>,
        /// Set to true when `flush` called on the handle, and unset on a `write`
        flushed: bool,
        /// Slot reserved on the [`crate::memory::WriteHandleLimiter`] for this
        /// handle. Held purely for its `Drop` side effect, which releases the slot when the file
        /// handle is closed.
        _write_slot: Option<WriteHandleSlot>,
    },
}

impl<DP: DataPlane> std::fmt::Debug for FileHandleState<DP> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileHandleState::Read { flushed, .. } => f.debug_struct("Read").field("flushed", flushed).finish(),
            FileHandleState::Write { flushed, .. } => f.debug_struct("Write").field("flushed", flushed).finish(),
        }
    }
}

impl<DP: DataPlane> FileHandleState<DP> {
    pub async fn new<Client>(
        mode: ReadWriteMode,
        lookup: &Lookup,
        write_slot: Option<WriteHandleSlot>,
        flags: OpenFlags,
        fs: &S3Filesystem<Client, DP>,
    ) -> Result<FileHandleState<DP>, Error>
    where
        Client: ObjectClient + Clone + Send + Sync + 'static,
    {
        let ino = lookup.ino();
        let stat = lookup.stat();
        let location = lookup.s3_location()?;
        let full_key = location.full_key();
        let bucket = location.bucket_name();

        match mode {
            ReadWriteMode::Read => {
                let object_size = stat.size as u64;
                let etag = match &stat.etag {
                    None => return Err(err!(libc::EBADF, "no E-Tag for inode {}", ino)),
                    Some(etag) => ETag::from_str(etag).expect("E-Tag should be set"),
                };
                let object_id = ObjectId::new(full_key.into(), etag);
                let spec = ObjectSpec {
                    bucket: Bucket::new(bucket.to_string()).expect("bucket name from mount should be valid"),
                    id: object_id,
                    size: object_size,
                };
                let reader = fs.data_plane.open_read(spec);
                let handle = FileHandleState::Read { reader, flushed: false };
                metrics::gauge!("fs.current_handles", "type" => "read").increment(1.0);
                Ok(handle)
            }
            ReadWriteMode::Write => {
                let is_truncate = flags.contains(OpenFlags::O_TRUNC);
                let write_mode = fs.config.write_mode();

                let (spec, initial_etag, initial_offset) = if write_mode.incremental_upload {
                    let initial_etag: Option<ETag> = if is_truncate {
                        None
                    } else {
                        stat.etag.as_ref().map(|e| e.into())
                    };
                    let current_offset = if is_truncate { 0 } else { stat.size as u64 };
                    let spec = WriteSpec::incremental_at(
                        bucket.to_string(),
                        full_key.to_string(),
                        current_offset,
                        initial_etag.clone(),
                    );
                    (spec, initial_etag, current_offset)
                } else {
                    (WriteSpec::new(bucket.to_string(), full_key.to_string()), None, 0)
                };

                let writer = fs.data_plane.open_write(spec)?;
                let upload_state = UploadState::InProgress {
                    writer,
                    incremental: write_mode.incremental_upload,
                    offset: initial_offset,
                    written_bytes: 0,
                    initial_etag,
                };
                let handle = FileHandleState::Write {
                    state: upload_state,
                    flushed: false,
                    _write_slot: write_slot,
                };
                metrics::gauge!("fs.current_handles", "type" => "write").increment(1.0);
                Ok(handle)
            }
        }
    }
}

/// The upload backing a write handle.
///
/// The byte transfer lives behind [`DataPlane::open_write`]'s [`Writer`]; this type keeps the
/// metadata orchestration around it — file-size bookkeeping, `finish_writing`, the append
/// commit-and-restart on `fsync`, and the pid/empty-file heuristics that decide when a `flush`
/// should actually complete the object.
pub enum UploadState<DP: DataPlane> {
    InProgress {
        writer: DP::Writer,
        /// Whether this is an incremental (append) upload. Append finalizes-and-restarts on commit
        /// so the handle stays writable; an atomic write finalizes once.
        incremental: bool,
        /// Absolute offset the next write must arrive at (the current end of the stream). Starts at
        /// the existing object size for a non-truncate append, otherwise 0.
        offset: u64,
        /// Total bytes written on this handle, never reset. Drives the "nothing written" checks.
        written_bytes: usize,
        /// The etag guarding an append, carried across commit restarts.
        initial_etag: Option<ETag>,
    },
    Completed,
    // Remember the failure reason to respond to retries
    Failed(libc::c_int),
}

impl<DP: DataPlane> std::fmt::Debug for UploadState<DP> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UploadState::InProgress {
                incremental,
                offset,
                written_bytes,
                ..
            } => f
                .debug_struct("InProgress")
                .field("incremental", incremental)
                .field("offset", offset)
                .field("written_bytes", written_bytes)
                .finish_non_exhaustive(),
            UploadState::Completed => f.write_str("Completed"),
            UploadState::Failed(e) => f.debug_tuple("Failed").field(e).finish(),
        }
    }
}

impl<DP: DataPlane + 'static> UploadState<DP> {
    pub async fn write<Client>(
        &mut self,
        fs: &S3Filesystem<Client, DP>,
        handle: &FileHandle<DP>,
        offset: i64,
        data: &[u8],
        fh: u64,
    ) -> Result<u32, Error>
    where
        Client: ObjectClient + Clone + Send + Sync + 'static,
    {
        // Borrow the writer only for the duration of the transfer; the await yields an owned
        // `Result`, releasing the borrow so the error path below can take the writer out to abort it.
        let result = match self {
            UploadState::InProgress { writer, .. } => writer.write_at(offset as u64, data).await,
            UploadState::Completed => {
                return Err(err!(libc::EIO, "upload already completed for key {}", handle.location));
            }
            UploadState::Failed(e) => {
                return Err(err!(*e, "upload already aborted for key {}", handle.location));
            }
        };

        match result {
            Ok(len) => {
                if let UploadState::InProgress {
                    offset: off,
                    written_bytes,
                    ..
                } = self
                {
                    *off = offset as u64 + len as u64;
                    *written_bytes += len;
                }
                fs.metablock.inc_file_size(handle.ino, len).await?;
                Ok(len as u32)
            }
            Err(e) => {
                let err: Error = e.into();
                // Take the writer out and abort it. On RTM this issues `AbortMultipartUpload`;
                // dropping the writer instead would leave a partial multipart upload on S3.
                if let UploadState::InProgress { writer, .. } =
                    std::mem::replace(self, UploadState::Failed(err.to_errno()))
                    && let Err(abort_err) = writer.abort().await
                {
                    debug!(?abort_err, key=%handle.location, "aborting writer after a failed write also failed");
                }
                Self::finish_on_error(fs.metablock.clone(), handle.ino, &handle.location, fh).await;
                Err(err)
            }
        }
    }

    /// Commit data to S3 and mark the upload as completed. In case it is an append request, finalize
    /// the current data and start a new request at the new offset and etag so the handle stays
    /// writable.
    pub async fn commit<Client>(
        &mut self,
        fs: &S3Filesystem<Client, DP>,
        handle: Arc<FileHandle<DP>>,
        fh: u64,
    ) -> Result<(), Error>
    where
        Client: ObjectClient + Clone + Send + Sync + 'static,
    {
        match self {
            UploadState::Completed => return Ok(()),
            UploadState::Failed(e) => {
                return Err(err!(*e, "upload already aborted for key {}", handle.location));
            }
            UploadState::InProgress { .. } => {}
        };

        let UploadState::InProgress {
            writer,
            incremental,
            offset,
            written_bytes,
            initial_etag,
        } = std::mem::replace(self, UploadState::Completed)
        else {
            unreachable!("checked above");
        };

        if incremental {
            // Finalize the current append, then reopen at the new end so writes can continue.
            let outcome = match writer.complete().await {
                Ok(outcome) => outcome,
                Err(e) => {
                    let err: Error = e.into();
                    *self = UploadState::Failed(err.to_errno());
                    return Err(err);
                }
            };
            let new_etag = outcome
                .etag
                .and_then(|e| ETag::from_str(&e).ok())
                .or(initial_etag);
            debug!(%handle.location, "append committed");

            let writer = match fs.data_plane.open_write(WriteSpec::incremental_at(
                handle.location.bucket_name().to_owned(),
                handle.location.full_key().to_string(),
                offset,
                new_etag.clone(),
            )) {
                Ok(writer) => writer,
                Err(e) => {
                    let err: Error = e.into();
                    *self = UploadState::Failed(err.to_errno());
                    return Err(err);
                }
            };
            *self = UploadState::InProgress {
                writer,
                incremental: true,
                offset,
                written_bytes,
                initial_etag: new_etag,
            };
        } else {
            // Atomic: fsync finalizes the whole object; the handle is now `Completed`.
            Self::finish_upload(fs.metablock.clone(), handle.ino, &handle.location, writer, initial_etag, fh)
                .await
                .inspect_err(|e| *self = UploadState::Failed(e.to_errno()))?;
        }
        Ok(())
    }

    /// Commit any buffered data (if written by the opener-process) to S3, and mark the upload as
    /// completed. In case there is no data written, or if it is written by a different process,
    /// don't complete the upload but mark the handle as flushed.
    pub async fn complete<Client>(
        &mut self,
        fs: &S3Filesystem<Client, DP>,
        handle: Arc<FileHandle<DP>>,
        pid: u32,
        open_pid: u32,
        fh: u64,
    ) -> Result<(), Error>
    where
        Client: ObjectClient + Clone + Send + Sync + 'static,
    {
        let (incremental, written_bytes) = match self {
            UploadState::InProgress {
                incremental,
                written_bytes,
                ..
            } => (*incremental, *written_bytes),
            UploadState::Completed => return Ok(()),
            UploadState::Failed(e) => {
                return Err(err!(
                    *e,
                    "upload already aborted for key {:?}",
                    handle.location.full_key()
                ));
            }
        };

        if incremental {
            if written_bytes == 0 || !are_from_same_process(open_pid, pid) {
                // Commit current changes. But don't close the write handle, only mark it as flushed.
                self.commit(fs, handle.clone(), fh).await?;
                return Self::flush_writer(fs, handle.ino, handle.clone(), fh).await;
            }
        } else {
            if written_bytes == 0 {
                debug!(key=%handle.location, "not completing upload because nothing was written yet");
                return Self::flush_writer(fs, handle.ino, handle.clone(), fh).await;
            }
            if !are_from_same_process(open_pid, pid) {
                debug!(
                    key=%handle.location,
                    pid, open_pid, "not completing upload because current PID differs from PID at open",
                );
                return Self::flush_writer(fs, handle.ino, handle.clone(), fh).await;
            }
        }

        let UploadState::InProgress {
            writer, initial_etag, ..
        } = std::mem::replace(self, UploadState::Completed)
        else {
            unreachable!("checked above");
        };
        Self::finish_upload(fs.metablock.clone(), handle.ino, &handle.location, writer, initial_etag, fh)
            .await
            .inspect_err(|e| *self = UploadState::Failed(e.to_errno()))?;
        Ok(())
    }

    /// Check state of upload, and complete the upload if it's still in-progress (i.e. not completed).
    ///
    /// When successful, returns a [`Lookup`] where the upload was still in-progress and thus
    /// completed by this method call.
    ///
    /// This is only called by the PendingUploadHook
    pub async fn complete_pending_upload(
        &mut self,
        metablock: Arc<dyn Metablock>,
        ino: InodeNo,
        key: &S3Location,
        fh: u64,
    ) -> Result<Option<Lookup>, InodeError> {
        // We do two rounds of `match` here because we want to retain the UploadState in case it is
        // already terminal.
        // State can only be "Failed" if there has been a previous upload attempt, in which case the
        // data has been lost already, but the PendingUploadHook will have invalidated the cache
        // for future requests to force revalidation of inode metadata.
        match self {
            // TODO: good to have - relay the error from the previous attempt here
            UploadState::Completed | UploadState::Failed(_) => return Ok(None),
            UploadState::InProgress { .. } => {}
        }

        let UploadState::InProgress {
            writer, initial_etag, ..
        } = std::mem::replace(self, UploadState::Completed)
        else {
            unreachable!("checked above");
        };
        Ok(Some(Self::finish_upload(metablock, ino, key, writer, initial_etag, fh).await?))
    }

    /// Finalize a writer and record the result on the inode.
    async fn finish_upload(
        metablock: Arc<dyn Metablock>,
        ino: InodeNo,
        key: &S3Location,
        writer: DP::Writer,
        initial_etag: Option<ETag>,
        fh: u64,
    ) -> Result<Lookup, InodeError> {
        match writer.complete().await {
            Ok(outcome) => {
                // `None` when no PUT was issued (an empty append), in which case the object keeps its
                // existing etag.
                let etag = outcome.etag.and_then(|e| ETag::from_str(&e).ok()).or(initial_etag);
                debug!(?etag, %key, size = outcome.size, "put succeeded");
                metablock.finish_writing(ino, etag, fh).await
            }
            Err(e) => {
                Self::finish_on_error(metablock, ino, key, fh).await;
                Err(InodeError::write_error(e, key.clone()))
            }
        }
    }

    async fn finish_on_error(metablock: Arc<dyn Metablock>, ino: InodeNo, s3location: &S3Location, fh: u64) {
        if let Err(err) = metablock.finish_writing(ino, None, fh).await {
            // Log the issue but still return put_result.
            error!(?err, key=?s3location.full_key(), "error updating the inode status");
        }
    }

    /// Mark the write-handle as deactivated in the inode's handle_map entry, and attach a
    /// PendingUploadHook to the inode for a future release/open to complete the delayed upload
    /// and clean up the writer.
    async fn flush_writer<Client>(
        fs: &S3Filesystem<Client, DP>,
        ino: InodeNo,
        handle: Arc<FileHandle<DP>>,
        fh: u64,
    ) -> Result<(), Error>
    where
        Client: ObjectClient + Clone + Send + Sync + 'static,
    {
        let pending_upload_hook = PendingUploadHook::new(fs.metablock.clone(), handle, fh);
        fs.metablock.flush_writer(ino, fh, pending_upload_hook).await?;
        Ok(())
    }
}

/// Get the thread-group id (tgid) from a process id (pid).
/// Despite the names, the process id is actually the thread id
/// and the thread-group id is the parent process id.
/// Returns `None` if unable to find or parse the task status.
/// Not supported on macOS.
fn get_tgid(pid: u32) -> Option<u32> {
    if cfg!(not(target_os = "macos")) {
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let path = format!("/proc/{pid}/task/{pid}/status");
        let file = File::open(path).ok()?;
        for line in BufReader::new(file).lines() {
            let line = line.ok()?;
            if line.starts_with("Tgid:") {
                return line["Tgid: ".len()..].trim().parse::<u32>().ok();
            }
        }
    }

    None
}

/// Check whether two pids correspond to the same process.
fn are_from_same_process(pid1: u32, pid2: u32) -> bool {
    if pid1 == pid2 {
        return true;
    }
    let Some(tgid1) = get_tgid(pid1) else {
        return false;
    };
    let Some(tgid2) = get_tgid(pid2) else {
        return false;
    };
    tgid1 == tgid2
}
