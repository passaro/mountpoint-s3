use std::ops::Range;

use async_watch::{Receiver, Sender};
use tracing::trace;

use crate::mem_limiter::{BufferArea, MemoryLimiter};
use crate::sync::Arc;
use crate::sync::atomic::{AtomicU64, Ordering};

use super::PrefetchReadError;
use super::part_stream::RequestRange;

/// Configuration for the prefetch heuristic.
#[derive(Debug)]
pub struct PrefetchHeuristicConfig {
    /// Maximum size to prefetch.
    max_prefetch_size: usize,
    /// Factor to increase the prefetch size by scaling up.
    prefetch_size_multiplier: usize,

    initial_prefetch_size: usize,
}

impl PrefetchHeuristicConfig {
    pub fn new(max_prefetch_size: usize, prefetch_size_multiplier: usize, initial_prefetch_size: usize) -> Self {
        // Minimum window size multiplier as the scaling up and down won't work if the multiplier is 1.
        const MIN_WINDOW_SIZE_MULTIPLIER: usize = 2;

        Self {
            max_prefetch_size,
            prefetch_size_multiplier: prefetch_size_multiplier.max(MIN_WINDOW_SIZE_MULTIPLIER),
            initial_prefetch_size,
        }
    }
}

/// Prefetch heuristic.
#[derive(Debug)]
pub struct PrefetchHeuristic {
    config: PrefetchHeuristicConfig,
    range: RequestRange,
    next_offset: u64,
    prefetched_offset: u64,
    window_size: usize,
}

impl PrefetchHeuristic {
    pub fn new(config: PrefetchHeuristicConfig, range: RequestRange) -> Self {
        Self {
            config,
            range,
            next_offset: range.start(),
            prefetched_offset: range.start(),
            window_size: 0,
        }
    }

    pub fn on_read(&mut self, length: usize, available_offset: u64) -> PrefetchRequest {
        let offset = self.next_offset;
        let length = length.min(self.range.trim_start(offset).len());
        self.next_offset += length as u64;

        if self.next_offset >= available_offset {
            // Scale up
            if self.window_size < self.config.max_prefetch_size {
                self.window_size = (self.window_size * self.config.prefetch_size_multiplier)
                    .min(self.config.max_prefetch_size)
                    .max(self.config.initial_prefetch_size);
                metrics::histogram!("prefetch.scale_up.window_after_increase_mib")
                    .record((self.window_size / 1024 / 1024) as f64);
            }
        }

        let remaining = self.prefetched_offset.saturating_sub(offset) as usize;
        if remaining < (self.window_size / 2) {
            self.prefetched_offset = offset + self.window_size as u64;
        }

        self.prefetched_offset = self.prefetched_offset.max(self.next_offset);

        metrics::gauge!("prefetch.heuristic.preferred").set((self.prefetched_offset - offset) as f64);

        self.current()
    }

    pub fn current(&self) -> PrefetchRequest {
        PrefetchRequest {
            required_offset: self.next_offset,
            preferred_offset: self.prefetched_offset,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefetchRequest {
    required_offset: u64,
    preferred_offset: u64,
}

impl PrefetchRequest {
    fn merge(&mut self, other: PrefetchRequest) {
        self.required_offset = self.required_offset.max(other.required_offset);
        self.preferred_offset = self.preferred_offset.max(other.preferred_offset);
    }
}

/// A [BackpressureController] should be given to consumers of a byte stream.
/// It is used to send feedback ([Self::send_feedback]) to its corresponding [BackpressureLimiter],
/// the counterpart which should be leveraged by the stream producer.
#[derive(Debug)]
pub struct PrefetchController {
    /// Sender for the [BackpressureLimiter] to receive size increments from the controller.
    prefetch_request_sender: Sender<PrefetchRequest>,

    /// Upper bound of the current read window, relative to the start of the S3 object.
    ///
    /// The request can return data up to this offset *exclusively*.
    /// This value must be advanced to continue fetching new data.
    prefetch_heuristic: PrefetchHeuristic,

    /// Memory limiter is used to guide decisions on how much data to prefetch.
    ///
    /// For example, when memory is low we should scale down [Self::preferred_read_window_size].
    mem_limiter: Arc<MemoryLimiter>,

    /// Next offset of the data to be read, relative to the start of the S3 object.
    read_offset: Arc<AtomicU64>,

    requested_offset: Arc<AtomicU64>,
}

/// The [BackpressureLimiter] is used on producer side of a stream, for example,
/// any [super::part_stream::ObjectPartStream] that supports backpressure.
///
/// The producer can call [Self::wait_for_read_window_increment] to wait for feedback from the consumer.
#[derive(Debug)]
pub struct PrefetchLimiter {
    prefetch_request_receiver: Receiver<PrefetchRequest>,
    /// End offset for the request we want to apply backpressure. The request can return
    /// data up to this offset *exclusively*.
    request_end_offset: u64,
    mem_limiter: Arc<MemoryLimiter>,

    pushed_offset: Arc<AtomicU64>,
    reserved_offset: Arc<AtomicU64>,

    /// Next offset of the data to be read, relative to the start of the S3 object.
    read_offset: Arc<AtomicU64>,
}

/// Creates a [BackpressureController] and its related [BackpressureLimiter].
///
/// This pair allows a consumer to send feedback ([BackpressureFeedbackEvent]) when starved or bytes are consumed,
/// informing a producer (a holder of the [BackpressureLimiter]) when it should provide data more aggressively.
pub fn new_prefetch_controller(
    prefetch_heuristic_config: PrefetchHeuristicConfig,
    request_range: RequestRange,
    mem_limiter: Arc<MemoryLimiter>,
) -> (PrefetchController, PrefetchLimiter) {
    let start_offset = request_range.start();
    let prefetch_heuristic = PrefetchHeuristic::new(prefetch_heuristic_config, request_range);
    let initial_request = prefetch_heuristic.current();
    let (prefetch_request_sender, prefetch_request_receiver) = async_watch::channel(initial_request);

    let read_offset = Arc::new(AtomicU64::new(start_offset));
    let reserved_offset = Arc::new(AtomicU64::new(start_offset));

    let controller = PrefetchController {
        prefetch_request_sender,
        prefetch_heuristic,
        mem_limiter: mem_limiter.clone(),
        read_offset: read_offset.clone(),
        requested_offset: reserved_offset.clone(),
    };

    trace!(?controller, "initialising prefetch controller");

    let limiter = PrefetchLimiter {
        prefetch_request_receiver,
        request_end_offset: request_range.end(),
        mem_limiter,
        pushed_offset: Arc::new(AtomicU64::new(start_offset)),
        reserved_offset,
        read_offset: read_offset.clone(),
    };

    (controller, limiter)
}

impl PrefetchController {
    pub fn requested_offset(&self) -> u64 {
        self.requested_offset.load(Ordering::SeqCst)
    }

    pub async fn update(&mut self, length: usize, available_offset: u64) {
        let prefetch = self.prefetch_heuristic.on_read(length, available_offset);
        trace!(
            next_read_offset = self.read_offset.load(Ordering::SeqCst),
            ?prefetch,
            "incrementing prefetch window",
        );

        let _ = self
            .prefetch_request_sender
            .send(prefetch)
            .inspect_err(|_| trace!("prefetch window incrementing queue is already closed"));
    }

    pub fn mark_read(&self, len: usize, offset: u64) {
        let len = len as u64;
        let previous = self.read_offset.fetch_add(len, Ordering::SeqCst);
        debug_assert_eq!(previous, offset, "unexpected read offset");
        trace!(len, "read releasing memory");
        self.mem_limiter.release(BufferArea::Prefetch, len);
    }

    pub fn backward_seek(&self, len: usize) {
        let len = len as u64;
        self.read_offset.fetch_sub(len, Ordering::SeqCst);
    }
}

impl PrefetchLimiter {
    pub fn reserved_offset(&self) -> u64 {
        self.reserved_offset.load(Ordering::SeqCst)
    }

    /// Ensure some memory in the given range is reserved.
    /// Wait on prefetch requests to reach the given range.
    pub async fn ensure_reserved_memory<E>(
        &mut self,
        range: Range<u64>,
        alignment: usize,
    ) -> Result<(u64, bool), PrefetchReadError<E>> {
        let mut reserved_offset = self.reserved_offset.load(Ordering::SeqCst);
        let mut updated = false;

        if reserved_offset >= range.end {
            return Ok((reserved_offset, false));
        }

        let mut current_prefetch = self.prefetch_request_receiver.borrow().clone();
        loop {
            let required = if current_prefetch.required_offset > range.start {
                current_prefetch
                    .required_offset
                    .min(range.end)
                    .saturating_sub(reserved_offset)
            } else {
                0
            };

            metrics::gauge!("prefetch.required")
                .set(current_prefetch.required_offset.saturating_sub(range.start) as f64);
            metrics::gauge!("prefetch.requested")
                .set(current_prefetch.preferred_offset.saturating_sub(range.start) as f64);

            let requested_mem_size = current_prefetch
                .preferred_offset
                .min(range.end)
                .saturating_sub(reserved_offset);

            trace!(
                ?current_prefetch,
                requested_mem_size,
                required,
                ?range,
                reserved_offset,
                "reserving"
            );

            let reserved =
                self.mem_limiter
                    .reserve_aligned(BufferArea::Prefetch, requested_mem_size, alignment as u64, required);
            if reserved > 0 {
                reserved_offset += reserved;
                updated = true;
                break;
            }
            if required == 0 && reserved_offset > range.start {
                break;
            }

            trace!(reserved_offset, ?current_prefetch, "blocking for prefetch request",);
            match self.prefetch_request_receiver.recv().await {
                Ok(latest) => current_prefetch.merge(latest),
                Err(_) => return Err(PrefetchReadError::ReadWindowIncrement),
            }
        }

        if updated {
            self.reserved_offset.store(reserved_offset, Ordering::SeqCst);

            metrics::gauge!("prefetch.reserved").set((reserved_offset - range.start) as f64);
            let read_offset = self.read_offset.load(Ordering::SeqCst);
            metrics::gauge!("prefetch.in_flight").set((reserved_offset - read_offset) as f64);
        }

        // Nothing else to do.
        Ok((reserved_offset, updated))
    }

    pub(crate) fn pushed_offset_counter(&self) -> Arc<AtomicU64> {
        self.pushed_offset.clone()
    }
}

impl Drop for PrefetchLimiter {
    fn drop(&mut self) {
        let pushed_offset = self.pushed_offset.load(Ordering::SeqCst);
        debug_assert!(
            pushed_offset <= self.request_end_offset,
            "invariant: the pushed offset should never be larger than the request end offset",
        );
        // Free up memory we have reserved for the read window.
        let reserved_offset = self.reserved_offset.load(Ordering::SeqCst);
        let remaining_window = reserved_offset.saturating_sub(pushed_offset);
        trace!(remaining_window, "PrefetchLimiter drop releasing memory");
        self.mem_limiter.release(BufferArea::Prefetch, remaining_window);
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     use std::sync::Arc;

//     use futures::executor::block_on;
//     use mountpoint_s3_client::mock_client::MockClientError;
//     use test_case::test_case;

//     use crate::mem_limiter::MemoryLimiter;
//     use crate::memory::PagedPool;
//     use crate::prefetch::INITIAL_REQUEST_SIZE;

//     #[test_case(INITIAL_REQUEST_SIZE, 2)] // real config
//     #[test_case(3 * 1024 * 1024, 4)]
//     #[test_case(8 * 1024 * 1024, 8)]
//     #[test_case(2 * 1024 * 1024 * 1024, 2)]
//     fn test_read_window_scale_up(initial_read_window_size: usize, read_window_size_multiplier: usize) {
//         let request_range = 0..(5 * 1024 * 1024 * 1024);
//         let backpressure_config = BackpressureConfig {
//             initial_read_window_size,
//             min_read_window_size: 8 * 1024 * 1024,
//             max_read_window_size: 2 * 1024 * 1024 * 1024,
//             read_window_size_multiplier,
//             request_range,
//         };

//         let (mut backpressure_controller, _backpressure_limiter) =
//             new_backpressure_controller_for_test(backpressure_config);
//         while backpressure_controller.preferred_read_window_size < backpressure_controller.max_read_window_size {
//             backpressure_controller.scale_up();
//             assert!(backpressure_controller.preferred_read_window_size >= backpressure_controller.min_read_window_size);
//             assert!(backpressure_controller.preferred_read_window_size <= backpressure_controller.max_read_window_size);
//         }
//         assert_eq!(
//             backpressure_controller.preferred_read_window_size, backpressure_controller.max_read_window_size,
//             "should have scaled up to max read window size"
//         );
//     }

//     #[test_case(2 * 1024 * 1024 * 1024, 2)]
//     #[test_case(15 * 1024 * 1024 * 1024, 2)]
//     #[test_case(2 * 1024 * 1024 * 1024, 8)]
//     #[test_case(8 * 1024 * 1024, 8)]
//     fn test_read_window_scale_down(initial_read_window_size: usize, read_window_size_multiplier: usize) {
//         let request_range = 0..(5 * 1024 * 1024 * 1024);
//         let backpressure_config = BackpressureConfig {
//             initial_read_window_size,
//             min_read_window_size: 8 * 1024 * 1024,
//             max_read_window_size: 2 * 1024 * 1024 * 1024,
//             read_window_size_multiplier,
//             request_range,
//         };

//         let (mut backpressure_controller, _backpressure_limiter) =
//             new_backpressure_controller_for_test(backpressure_config);
//         while backpressure_controller.preferred_read_window_size > backpressure_controller.min_read_window_size {
//             backpressure_controller.scale_down();
//             assert!(backpressure_controller.preferred_read_window_size <= backpressure_controller.max_read_window_size);
//             assert!(backpressure_controller.preferred_read_window_size >= backpressure_controller.min_read_window_size);
//         }
//         assert_eq!(
//             backpressure_controller.preferred_read_window_size, backpressure_controller.min_read_window_size,
//             "should have scaled down to min read window size"
//         );
//     }

//     #[test]
//     fn wait_for_read_window_increment_drains_all_events() {
//         const KIB: usize = 1024;
//         const MIB: usize = 1024 * KIB;
//         const GIB: usize = 1024 * MIB;

//         // OK, back to basics. Just reproduce what happened, verify it passes after the fix.
//         #[allow(clippy::identity_op)]
//         let backpressure_config = BackpressureConfig {
//             initial_read_window_size: 1 * MIB,
//             min_read_window_size: 8 * MIB,
//             max_read_window_size: 2 * GIB,
//             read_window_size_multiplier: 2,
//             request_range: 0..(5 * GIB as u64),
//         };

//         let (mut backpressure_controller, mut backpressure_limiter) =
//             new_backpressure_controller_for_test(backpressure_config);

//         block_on(async {
//             #[allow(clippy::identity_op)]
//             let expected_offset = 1 * MIB as u64;
//             assert_eq!(
//                 backpressure_limiter.read_window_end_offset(),
//                 expected_offset,
//                 "read window end offset should already be {expected_offset} due to initial read window size config",
//             );

//             // Send more than one increment.
//             backpressure_controller.increment_read_window(7 * MIB).await;
//             backpressure_controller.increment_read_window(8 * MIB).await;
//             backpressure_controller.increment_read_window(8 * MIB).await;

//             let curr_offset = backpressure_limiter
//                 .wait_for_read_window_increment::<MockClientError>(0)
//                 .await
//                 .expect("should return OK as we have new values to increment before channels are closed")
//                 .expect("value should change as we sent increments");
//             assert_eq!(
//                 24 * MIB as u64,
//                 curr_offset,
//                 "expected offset did not match offset reported by limiter",
//             );
//         });
//     }

//     #[test_case(500, 1000, 100, 500; "offset before second request start")]
//     #[test_case(1000, 1000, 512, 1000; "offset at second request start")]
//     #[test_case(1500, 1000, 512, 1512; "offset after second request start, needs alignment")]
//     #[test_case(2024, 1000, 512, 2024; "offset after second request start, already aligned")]
//     #[test_case(1001, 1000, 512, 1512; "offset just after second request start, needs alignment")]
//     #[test_case(1512, 1000, 512, 1512; "offset exactly at part boundary")]
//     #[test_case(1513, 1000, 512, 2024; "offset just past part boundary")]
//     fn test_read_window_alignment(offset: u64, from_offset: u64, part_size: u64, expected: u64) {
//         let result = ReadWindowAlignmentConfig::AlignToPartSize { from_offset, part_size }.align(offset);
//         assert_eq!(result, expected);
//     }

//     fn new_backpressure_controller_for_test(
//         backpressure_config: BackpressureConfig,
//     ) -> (BackpressureController, BackpressureLimiter) {
//         let pool = PagedPool::new_with_candidate_sizes([8 * 1024 * 1024]);
//         let mem_limiter = Arc::new(MemoryLimiter::new(
//             pool,
//             backpressure_config.max_read_window_size as u64,
//         ));
//         new_backpressure_controller(backpressure_config, mem_limiter.clone())
//     }
// }
