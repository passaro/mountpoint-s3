//! Read workloads and their measurements.
//!
//! Two shapes, because the priority experiment needs contention between them:
//!
//! - [`Workload::Sequential`] — stream an object end to end. Generates speculation, which
//!   is what a foreground reader competes against.
//! - [`Workload::RandomReads`] — small reads at scattered offsets. Latency-sensitive, and
//!   nothing about it is speculative.
//!
//! Neither is labelled. Priority is derived from what a read *is*, not from what a caller
//! calls it (`mountpoint_s3_fs::data::Urgency`), so the workload is the only independent
//! variable — a foreground reader differs from a background one solely in what it does.

// Shared by three test binaries, each of which uses a different part of this workload driver;
// per-binary dead-code analysis therefore flags items the other binaries use.
#![allow(dead_code)]

use std::time::{Duration, Instant};

use mountpoint_s3_fs::data::{ReadError, Reader};

/// Latency samples, kept in full so percentiles are exact rather than estimated. Read
/// counts here are thousands, so the memory cost is irrelevant and the precision is free.
#[derive(Debug, Default, Clone)]
pub struct Latencies {
    samples: Vec<Duration>,
}

impl Latencies {
    pub fn record(&mut self, d: Duration) {
        self.samples.push(d);
    }

    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Nearest-rank percentile, `q` in 0.0..=1.0.
    pub fn percentile(&self, q: f64) -> Duration {
        if self.samples.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let rank = ((sorted.len() as f64 - 1.0) * q).round() as usize;
        sorted[rank]
    }

    pub fn p50(&self) -> Duration {
        self.percentile(0.50)
    }

    pub fn p99(&self) -> Duration {
        self.percentile(0.99)
    }

    pub fn max(&self) -> Duration {
        self.samples.iter().copied().max().unwrap_or_default()
    }

    pub fn mean(&self) -> Duration {
        if self.samples.is_empty() {
            return Duration::ZERO;
        }
        let total: Duration = self.samples.iter().sum();
        total / self.samples.len() as u32
    }
}

/// What a reader does.
#[derive(Debug, Clone, Copy)]
pub enum Workload {
    /// Read straight through from `start`, `read_size` at a time, up to `max_bytes`.
    Sequential {
        start: u64,
        read_size: usize,
        max_bytes: u64,
    },
    /// `count` reads of `read_size`, at offsets spread deterministically across the object.
    ///
    /// Deterministic rather than random: an experiment comparing configurations must issue
    /// the same reads in every arm, or the arms are not comparable.
    RandomReads {
        count: usize,
        read_size: usize,
        object_size: u64,
    },
}

#[derive(Debug, Clone)]
pub struct WorkloadResult {
    pub bytes_read: u64,
    pub reads: usize,
    pub elapsed: Duration,
    pub latencies: Latencies,
}

impl WorkloadResult {
    /// Delivered throughput in bytes per second.
    pub fn throughput_bps(&self) -> f64 {
        if self.elapsed.is_zero() {
            return 0.0;
        }
        self.bytes_read as f64 / self.elapsed.as_secs_f64()
    }

    pub fn throughput_mib_s(&self) -> f64 {
        self.throughput_bps() / (1024.0 * 1024.0)
    }
}

impl Workload {
    /// Run to completion, timing every read.
    pub async fn run(self, reader: &impl Reader) -> Result<WorkloadResult, ReadError> {
        let started = Instant::now();
        let mut latencies = Latencies::default();
        let mut bytes_read = 0u64;
        let mut reads = 0usize;

        match self {
            Workload::Sequential {
                start,
                read_size,
                max_bytes,
            } => {
                let mut offset = start;
                while bytes_read < max_bytes {
                    let at = Instant::now();
                    let segs = match reader.read_at(offset, read_size).await {
                        Ok(segs) => segs,
                        // End of object: the workload is done, not failed.
                        Err(ReadError::OutOfRange { .. }) => break,
                        Err(e) => return Err(e),
                    };
                    latencies.record(at.elapsed());
                    if segs.is_empty() {
                        break;
                    }
                    bytes_read += segs.len() as u64;
                    offset += segs.len() as u64;
                    reads += 1;
                }
            }
            Workload::RandomReads {
                count,
                read_size,
                object_size,
            } => {
                for i in 0..count {
                    let offset = scatter(i, count, object_size, read_size);
                    let at = Instant::now();
                    let segs = reader.read_at(offset, read_size).await?;
                    latencies.record(at.elapsed());
                    bytes_read += segs.len() as u64;
                    reads += 1;
                }
            }
        }

        Ok(WorkloadResult {
            bytes_read,
            reads,
            elapsed: started.elapsed(),
            latencies,
        })
    }
}

/// Deterministic offset for read `i` of `count`, spread across the object.
///
/// A golden-ratio (low-discrepancy) sequence rather than a PRNG: it spreads offsets evenly
/// without clustering, is identical across arms and across runs, and needs no seed
/// threaded through the harness.
fn scatter(i: usize, count: usize, object_size: u64, read_size: usize) -> u64 {
    debug_assert!(count > 0);
    let span = object_size.saturating_sub(read_size as u64).max(1);
    // Fractional part of i * phi^-1, in fixed point.
    const INV_PHI: u64 = 11_400_714_819_323_198_485; // 2^64 / phi
    let frac = (i as u64).wrapping_mul(INV_PHI);
    let unit = (frac >> 11) as f64 / (1u64 << 53) as f64;
    ((unit * span as f64) as u64).min(span)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_are_ordered() {
        let mut l = Latencies::default();
        for ms in 1..=100 {
            l.record(Duration::from_millis(ms));
        }
        assert!(l.p50() <= l.p99());
        assert!(l.p99() <= l.max());
        // Nearest-rank over 1..=100: rank = round(99 * 0.5) = 50, so sorted[50] = 51ms.
        assert_eq!(l.p50(), Duration::from_millis(51));
        assert_eq!(l.p99(), Duration::from_millis(99));
        assert_eq!(l.max(), Duration::from_millis(100));
    }

    #[test]
    fn percentiles_of_one_sample_are_that_sample() {
        let mut l = Latencies::default();
        l.record(Duration::from_millis(7));
        assert_eq!(l.p50(), Duration::from_millis(7));
        assert_eq!(l.p99(), Duration::from_millis(7));
        assert_eq!(l.max(), Duration::from_millis(7));
    }

    #[test]
    fn empty_latencies_are_zero_not_a_panic() {
        let l = Latencies::default();
        assert_eq!(l.p50(), Duration::ZERO);
        assert_eq!(l.p99(), Duration::ZERO);
        assert_eq!(l.mean(), Duration::ZERO);
        assert!(l.is_empty());
    }

    #[test]
    fn scatter_stays_in_bounds() {
        let object_size = 8 * 1024 * 1024;
        let read_size = 128 * 1024;
        for i in 0..1000 {
            let offset = scatter(i, 1000, object_size, read_size);
            assert!(
                offset + read_size as u64 <= object_size,
                "read {i} at {offset} would run past the object"
            );
        }
    }

    #[test]
    fn scatter_is_deterministic_and_spread() {
        // Same inputs, same outputs: arms must be comparable.
        let a: Vec<_> = (0..50).map(|i| scatter(i, 50, 1 << 20, 4096)).collect();
        let b: Vec<_> = (0..50).map(|i| scatter(i, 50, 1 << 20, 4096)).collect();
        assert_eq!(a, b);

        // And genuinely scattered rather than clustered at one end.
        let distinct: std::collections::HashSet<_> = a.iter().collect();
        assert!(
            distinct.len() > 40,
            "offsets should be well spread, got {} distinct of 50",
            distinct.len()
        );
    }

    #[test]
    fn scatter_handles_object_smaller_than_read() {
        // Degenerate but reachable; must not panic or overflow.
        assert_eq!(scatter(0, 1, 100, 4096), 0);
    }
}
