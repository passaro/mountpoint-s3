use serde::Serialize;
use serde_with::{DurationSecondsWithFrac, serde_as};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AggregationError {
    #[error("JSON serialization failed: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

#[serde_as]
#[derive(Debug, Clone, Serialize)]
pub struct JobResult {
    pub job_name: String,
    pub workload_type: String,
    pub iterations_completed: usize,
    pub total_bytes: u64,
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub elapsed_seconds: Duration,
    pub errors: Vec<ErrorInfo>,
    /// Read-path counters, summed over the job's readers. `None` for write jobs.
    ///
    /// The point of reporting these is that `total_bytes` alone cannot distinguish backends:
    /// two data planes delivering the same bytes may issue very different numbers of GETs,
    /// fetch very different amounts to do it, and hand the result back in very different
    /// numbers of pieces. `bytes_fetched` over `bytes_delivered` is read amplification, and
    /// `chunks_returned` over `reads_completed` is mean fragmentation; both are comparable
    /// between backends in a way throughput is not (see the checksum caveat in
    /// `mountpoint_s3_fs::data::prefetch_adapter`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_stats: Option<ReadStats>,
    /// Write-path counters. `None` for read jobs, and for write jobs on the CRT `Uploader`, which
    /// exposes no equivalent — so this is populated only on the RTM write arm, which drives a
    /// `mountpoint_s3_fs::data::Writer`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_stats: Option<WriteStats>,
}

/// Read-path counters as reported by a `mountpoint_s3_fs::data::Reader`, plus what the
/// benchmark itself observed while consuming the reads.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ReadStats {
    pub bytes_delivered: u64,
    pub bytes_fetched: u64,
    pub requests_issued: u64,
    pub requests_aborted: u64,
    pub cache_hits: u64,
    pub cursors_opened: u64,

    /// `read_at` calls that returned more than one chunk, and so would have to be copied to be
    /// made contiguous.
    ///
    /// Counted by the benchmark, not the reader: fragmentation is a property of what the
    /// reader handed back, and the cost of resolving it falls on the consumer. The benchmark
    /// itself no longer pays that cost — it consumes the `Segments` by length and never
    /// flattens them — so this is the copy a contiguity-requiring consumer (a real FUSE
    /// `reply.data()`) *would* incur, which is what makes it comparable between backends.
    /// Always 0 on the prefetcher arm, whose `Segments` is always a single chunk.
    pub reads_copied: u64,
    /// Bytes those copies would move — the volume `reads_copied` represents, which the count
    /// alone does not give, since a fragmented 1 MiB read is far more expensive than a
    /// fragmented 4 KiB one.
    pub bytes_copied: u64,
    /// Chunks summed over every `read_at`. Over `read_at` count this is the mean
    /// fragmentation; 1.0 means every read arrived in one piece.
    pub chunks_returned: u64,
    /// `read_at` calls, as the denominator for `chunks_returned`.
    pub reads_completed: u64,
}

/// Write-path counters as reported by a `mountpoint_s3_fs::data::Writer`.
///
/// `write_stalls` is the one to watch: it counts how often the writer had to wait for RTM to take a
/// full part, so a non-zero value means the writer was genuinely throttled to the network's rate
/// rather than buffering without limit (the write path holds at most one part).
#[derive(Debug, Clone, Default, Serialize)]
pub struct WriteStats {
    pub bytes_accepted: u64,
    pub write_stalls: u64,
    /// Uploads that went out as MPUs. Always equal to the iteration count on the RTM arm — a
    /// caller-supplied `PartStream` is MPU-only, whatever the object's size.
    pub multipart_uploads: u64,
}

impl ReadStats {
    /// Accumulate one reader's counters.
    pub fn add(&mut self, s: mountpoint_s3_fs::data::ReaderStats) {
        self.bytes_delivered += s.bytes_delivered;
        self.bytes_fetched += s.bytes_fetched;
        self.requests_issued += s.requests_issued;
        self.requests_aborted += s.requests_aborted;
        self.cache_hits += s.cache_hits;
        self.cursors_opened += s.cursors_opened;
    }

    /// Record the fragmentation of one completed `read_at`.
    ///
    /// Separate from [`add`](Self::add) because the source differs: that sums counters the
    /// reader keeps, this records what the benchmark saw on each call. Only a successful read
    /// is counted, so `reads_completed` matches the reads that contributed bytes.
    pub fn add_read(&mut self, len: usize, chunks: usize) {
        self.reads_completed += 1;
        self.chunks_returned += chunks as u64;
        if chunks > 1 {
            self.reads_copied += 1;
            self.bytes_copied += len as u64;
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorInfo {
    pub error_type: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkResults {
    pub jobs: Vec<JobResult>,
    pub summary: SummaryResult,
}

#[serde_as]
#[derive(Debug, Clone, Serialize)]
pub struct SummaryResult {
    pub total_bytes: u64,
    #[serde_as(as = "DurationSecondsWithFrac<f64>")]
    pub total_elapsed_seconds: Duration,
    pub total_errors: usize,
    pub peak_memory_mib: f64,
}

impl BenchmarkResults {
    pub fn aggregate(results: Vec<JobResult>, peak_memory_mib: f64) -> Self {
        let total_bytes: u64 = results.iter().map(|r| r.total_bytes).sum();
        let total_elapsed_seconds = results
            .iter()
            .map(|r| r.elapsed_seconds)
            .max()
            .unwrap_or(Duration::ZERO);
        let total_errors: usize = results.iter().flat_map(|r| &r.errors).count();

        BenchmarkResults {
            jobs: results,
            summary: SummaryResult {
                total_bytes,
                total_elapsed_seconds,
                total_errors,
                peak_memory_mib,
            },
        }
    }

    pub fn write_json(&self, output_file: Option<&str>) -> Result<(), AggregationError> {
        let json = serde_json::to_string_pretty(self)?;

        if let Some(path) = output_file {
            std::fs::write(path, json)?;
        } else {
            println!("{}", json);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_read_charges_only_fragmented_reads() {
        let mut stats = ReadStats::default();
        stats.add_read(4096, 1);
        stats.add_read(1024 * 1024, 3);
        stats.add_read(8192, 2);

        // Every read counts towards fragmentation, but a single-chunk read needs no copy.
        assert_eq!(stats.reads_completed, 3);
        assert_eq!(stats.chunks_returned, 6);
        assert_eq!(stats.reads_copied, 2);
        // The byte volume, not just the count: one fragmented 1 MiB read dwarfs a 8 KiB one.
        assert_eq!(stats.bytes_copied, 1024 * 1024 + 8192);
    }

    #[test]
    fn contiguous_reads_report_no_copies() {
        let mut stats = ReadStats::default();
        for _ in 0..10 {
            stats.add_read(65536, 1);
        }
        // The shape of the prefetcher arm, whose `Segments` is always one chunk.
        assert_eq!(stats.reads_copied, 0);
        assert_eq!(stats.bytes_copied, 0);
        assert_eq!(stats.chunks_returned, stats.reads_completed);
    }

    #[test]
    fn fragmentation_counters_are_serialized() {
        let mut stats = ReadStats::default();
        stats.add_read(1024, 2);
        let json = serde_json::to_value(&stats).expect("ReadStats serializes");

        // The figures are only useful if they reach the JSON a run is judged from.
        assert_eq!(json["reads_copied"], 1);
        assert_eq!(json["bytes_copied"], 1024);
        assert_eq!(json["chunks_returned"], 2);
        assert_eq!(json["reads_completed"], 1);
    }
}
