//! A single-part rendezvous between a caller that pushes bytes and a transfer that pulls parts.
//!
//! [`PartSink`] is the push end, written to by
//! [`RtmWriter::write_at`](crate::data::RtmWriter). [`PartSource`] is the pull end, an RTM
//! [`PartStream`] whose `poll_part` is called when the upload wants another part.
//!
//! # At most one part is buffered
//!
//! The sink accumulates bytes into a single part of `part_size`. When that part fills, it is handed
//! to the source and the writer **awaits until RTM takes it** (`poll_part`) before accepting any
//! more bytes. So peak buffered between caller and network is exactly one part — never a queue.
//!
//! This does not serialise the S3 uploads: RTM takes a part (unblocking the writer), then uploads it
//! concurrently on its own runtime while polling for the next. The writer simply never runs more
//! than one part ahead of what RTM has taken.
//!
//! # Parts are cut on the push side
//!
//! Because the object length is unknown, RTM (its unknown-length streaming support, PR #164) reads
//! the stream to end-of-stream and uploads each returned `PartData` exactly as given — it does not
//! re-cut or re-buffer a caller part. So the sink cuts parts at a fixed `part_size` (resolved from
//! the RTM client's configured upload part size, ≥ the 5 MiB S3 minimum) and every part but the last
//! is exactly that size, as S3 requires.
//!
//! # End of stream, and failures
//!
//! [`PartSink::finish`] flushes the final (possibly short, possibly absent) part and marks the stream
//! closed, so `poll_part` yields `None`. Failure travels the other way: when the transfer drops the
//! source, a writer waiting on a handoff wakes with [`Closed`] rather than hanging. Dropping the sink
//! also closes the stream, so an abandoned writer never leaves RTM waiting.

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use aws_sdk_s3_transfer_manager::io::{PartData, PartStream, SizeHint, StreamContext};
use bytes::{Bytes, BytesMut};
use mountpoint_s3_client::checksums::crc64nvme::Crc64nvmeHasher;
use mountpoint_s3_client::checksums::crc64nvme_to_base64;

/// The other end is gone: the upload either finished or failed. Which of the two is the
/// upload handle's to report, not the channel's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

/// The one-slot handoff shared by [`PartSink`] and [`PartSource`].
#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    /// The single full (or final) part awaiting pickup by RTM. `None` between handoffs.
    ready: Option<Bytes>,
    /// The writer signalled end-of-stream (`finish`, or dropped).
    closed: bool,
    /// The transfer dropped the source — it finished or failed. A blocked writer wakes as `Closed`.
    source_dropped: bool,
    /// RTM's waker, stored when `poll_part` finds no part ready; woken when one is published.
    poll_waker: Option<Waker>,
    /// The writer's waker, stored while it waits for the slot; woken when RTM takes the part.
    sink_waker: Option<Waker>,
    /// The digest over the whole object, set by `finish` and read once RTM reaches end-of-stream.
    full_object_checksum: Option<String>,
    /// 1-indexed, as S3 numbers parts. Assigned on the pull side as each part is taken.
    next_part_number: u64,
}

/// Create a connected sink and source that hand off whole parts of `part_size` bytes.
pub fn channel(part_size: usize) -> (PartSink, PartSource) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            ready: None,
            closed: false,
            source_dropped: false,
            poll_waker: None,
            sink_waker: None,
            full_object_checksum: None,
            next_part_number: 1,
        }),
    });
    (
        PartSink {
            shared: shared.clone(),
            current: BytesMut::new(),
            part_size: part_size.max(1),
            hasher: Crc64nvmeHasher::new(),
        },
        PartSource { shared },
    )
}

/// The push end, written to by [`Writer::write_at`](crate::data::Writer::write_at).
pub struct PartSink {
    shared: Arc<Shared>,
    /// Bytes accumulated toward the current part; never grows past `part_size`.
    current: BytesMut,
    part_size: usize,
    /// Running full-object CRC64-NVME, updated as bytes are accepted (writes are append-only, so the
    /// order is the object's order). Finalised in [`finish`](Self::finish).
    hasher: Crc64nvmeHasher,
}

impl std::fmt::Debug for PartSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartSink")
            .field("part_size", &self.part_size)
            .field("buffered", &self.current.len())
            .finish_non_exhaustive()
    }
}

impl PartSink {
    /// Accept `data`, cutting and handing off whole parts as they fill.
    ///
    /// Returns the number of handoffs that had to wait for RTM to take the previous part — the
    /// backpressure signal reported as
    /// [`WriterStats::write_stalls`](crate::data::WriterStats::write_stalls). Since at most one part
    /// is buffered, filling a part always means awaiting RTM before continuing.
    pub async fn write(&mut self, data: &[u8]) -> Result<u64, Closed> {
        let mut stalls = 0;
        let mut rest = data;
        while !rest.is_empty() {
            let space = self.part_size - self.current.len();
            let take = space.min(rest.len());
            self.hasher.update(&rest[..take]);
            self.current.extend_from_slice(&rest[..take]);
            rest = &rest[take..];

            if self.current.len() == self.part_size {
                let part = self.current.split().freeze();
                stalls += Self::handoff(&self.shared, part).await?;
            }
        }
        Ok(stalls)
    }

    /// Hand one full part to the source and wait until RTM has taken it.
    ///
    /// Two phases, both waking/parking against the shared slot: place the part once the slot is free,
    /// then wait until it is taken. Returns 1 if either phase had to wait, else 0.
    async fn handoff(shared: &Arc<Shared>, part: Bytes) -> Result<u64, Closed> {
        let mut waited = false;

        // Phase 1: wait for the slot to be free, then place the part and wake RTM.
        let mut part = Some(part);
        poll_fn(|cx| {
            let mut st = shared.state.lock().expect("part channel lock poisoned");
            if st.source_dropped {
                return Poll::Ready(Err(Closed));
            }
            if st.ready.is_none() {
                st.ready = part.take();
                if let Some(w) = st.poll_waker.take() {
                    w.wake();
                }
                Poll::Ready(Ok(()))
            } else {
                waited = true;
                st.sink_waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await?;

        // Phase 2: wait until RTM takes the part (the slot empties again).
        poll_fn(|cx| {
            let mut st = shared.state.lock().expect("part channel lock poisoned");
            if st.ready.is_none() {
                // Taken. (Even if the source has since dropped, this part made it across.)
                Poll::Ready(Ok::<(), Closed>(()))
            } else if st.source_dropped {
                // The transfer died before taking this part.
                Poll::Ready(Err(Closed))
            } else {
                waited = true;
                st.sink_waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await?;

        Ok(u64::from(waited))
    }

    /// Flush the final part, if any, then signal end of stream.
    ///
    /// An empty object flushes nothing — RTM emits the single zero-length part S3 needs when the
    /// stream yields `None`. Finalises the full-object checksum before closing, so it is available by
    /// the time RTM reaches end-of-stream and asks for it.
    pub async fn finish(&mut self) -> Result<(), Closed> {
        if !self.current.is_empty() {
            let part = self.current.split().freeze();
            Self::handoff(&self.shared, part).await?;
        }
        let checksum = crc64nvme_to_base64(&self.hasher.clone().finalize());
        let mut st = self.shared.state.lock().expect("part channel lock poisoned");
        st.full_object_checksum = Some(checksum);
        st.closed = true;
        if let Some(w) = st.poll_waker.take() {
            w.wake();
        }
        Ok(())
    }
}

impl Drop for PartSink {
    fn drop(&mut self) {
        // A writer dropped mid-stream (e.g. abort) must not leave the transfer waiting for a part
        // that will never come.
        let mut st = self.shared.state.lock().expect("part channel lock poisoned");
        st.closed = true;
        if let Some(w) = st.poll_waker.take() {
            w.wake();
        }
    }
}

/// The pull end: RTM's [`PartStream`] over the one-slot handoff.
///
/// # Checksums
///
/// The sink computes a full-object CRC64-NVME over the bytes as they pass through and this reports it
/// from [`full_object_checksum`](PartStream::full_object_checksum), so S3 validates the assembled
/// object against it. RTM's default checksum strategy asks for a full-object checksum but cannot
/// compute one for a caller-supplied stream, since it only sees individual parts.
#[derive(Debug)]
pub struct PartSource {
    shared: Arc<Shared>,
}

impl PartStream for PartSource {
    fn poll_part(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<std::io::Result<PartData>>> {
        // `part_size` from the StreamContext is ignored: the sink already cut this part to size.
        let this = self.get_mut();
        let mut st = this.shared.state.lock().expect("part channel lock poisoned");

        if let Some(part) = st.ready.take() {
            let part_number = st.next_part_number;
            st.next_part_number += 1;
            if let Some(w) = st.sink_waker.take() {
                w.wake();
            }
            return Poll::Ready(Some(Ok(PartData::new(part_number, part))));
        }
        if st.closed {
            return Poll::Ready(None);
        }
        // Nothing ready yet; wake this task when the sink publishes a part or closes.
        st.poll_waker = Some(cx.waker().clone());
        Poll::Pending
    }

    fn size_hint(&self) -> SizeHint {
        // No upper bound: the object's size is not known in advance. RTM reads to end-of-stream.
        SizeHint::default()
    }

    fn full_object_checksum(&self) -> Option<String> {
        self.shared
            .state
            .lock()
            .expect("part channel lock poisoned")
            .full_object_checksum
            .clone()
    }
}

impl Drop for PartSource {
    fn drop(&mut self) {
        // The transfer dropped the stream (finished or failed). Wake any writer blocked on a handoff
        // so it learns the transfer is gone rather than waiting forever.
        let mut st = self.shared.state.lock().expect("part channel lock poisoned");
        st.source_dropped = true;
        if let Some(w) = st.sink_waker.take() {
            w.wake();
        }
    }
}

#[cfg(test)]
impl PartSource {
    /// Take the ready part, if any, mimicking what `poll_part` does — for tests, which cannot
    /// construct a `StreamContext` to drive `poll_part` directly.
    fn take_ready(&self) -> Option<Bytes> {
        let mut st = self.shared.state.lock().expect("part channel lock poisoned");
        let part = st.ready.take();
        if part.is_some() {
            st.next_part_number += 1;
            if let Some(w) = st.sink_waker.take() {
                w.wake();
            }
        }
        part
    }

    fn is_closed(&self) -> bool {
        self.shared.state.lock().expect("part channel lock poisoned").closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use futures::future::poll_immediate;

    #[test]
    fn a_full_part_blocks_the_writer_until_taken() {
        let (mut sink, source) = channel(4);

        // Two parts' worth in one write. The first part fills and is handed off; the write cannot
        // finish until RTM takes it.
        let mut write = Box::pin(sink.write(&[1u8; 8]));
        assert!(
            block_on(poll_immediate(&mut write)).is_none(),
            "the write must block once a part is buffered, or more than one part is held"
        );

        // The first part is available; take it.
        assert_eq!(source.take_ready().as_deref(), Some(&[1u8; 4][..]));

        // Still blocked: the second part is now buffered and awaiting pickup.
        assert!(block_on(poll_immediate(&mut write)).is_none());
        assert_eq!(source.take_ready().as_deref(), Some(&[1u8; 4][..]));

        // Both parts taken; the write completes, having stalled on each handoff.
        let stalls = block_on(write).expect("write completes");
        assert_eq!(stalls, 2);
    }

    #[test]
    fn finish_flushes_the_final_short_part_and_closes() {
        let (mut sink, source) = channel(4);

        // A sub-part write buffers but hands off nothing yet.
        assert_eq!(block_on(sink.write(&[7u8; 2])).expect("write"), 0);
        assert!(source.take_ready().is_none(), "a partial part is not handed off early");
        assert_eq!(source.full_object_checksum(), None, "checksum is absent until drained");

        // finish flushes the 2-byte final part; it must be taken for finish to complete.
        let mut finish = Box::pin(sink.finish());
        assert!(
            block_on(poll_immediate(&mut finish)).is_none(),
            "finish waits for the final part"
        );
        assert_eq!(source.take_ready().as_deref(), Some(&[7u8; 2][..]));
        block_on(finish).expect("finish completes");

        assert!(source.is_closed());
        assert!(source.full_object_checksum().is_some(), "checksum finalised at finish");
    }

    #[test]
    fn empty_object_finishes_without_a_part() {
        let (mut sink, source) = channel(4);
        block_on(sink.finish()).expect("finish");
        assert!(source.is_closed());
        assert!(source.take_ready().is_none(), "no part for an empty object");
        assert!(source.full_object_checksum().is_some());
    }

    #[test]
    fn dropping_the_source_wakes_a_blocked_writer_as_closed() {
        let (mut sink, source) = channel(4);

        let mut write = Box::pin(sink.write(&[1u8; 8]));
        assert!(block_on(poll_immediate(&mut write)).is_none());

        drop(source);
        assert_eq!(
            block_on(write),
            Err(Closed),
            "a blocked writer must learn the transfer is gone"
        );
    }

    #[test]
    fn dropping_the_sink_closes_the_stream() {
        let (sink, source) = channel(4);
        drop(sink);
        assert!(source.is_closed(), "a dropped writer must not leave RTM waiting");
    }

    #[test]
    fn write_after_the_source_is_gone_reports_closed() {
        let (mut sink, source) = channel(4);
        drop(source);
        assert_eq!(block_on(sink.write(&[1u8; 4])), Err(Closed));
    }
}
