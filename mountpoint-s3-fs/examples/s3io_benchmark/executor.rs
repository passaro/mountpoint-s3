use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use mountpoint_s3_client::config::{Allocator, EndpointConfig, S3ClientConfig, Uri};
use mountpoint_s3_client::types::HeadObjectParams;
use mountpoint_s3_client::{ObjectClient, S3CrtClient};
use mountpoint_s3_fs::data::{DataPlane, ObjectSpec, PrefetchDataPlane, Reader};
use mountpoint_s3_fs::memory::effective_total_memory;
use mountpoint_s3_fs::memory::{CandidateSize, PagedPool};
use mountpoint_s3_fs::object::ObjectId;
use mountpoint_s3_fs::prefetch::{Prefetcher, PrefetcherConfig};
use mountpoint_s3_fs::upload::{Uploader, UploaderConfig};
use mountpoint_s3_fs::{Runtime, ServerSideEncryption};
use rand::{RngExt, SeedableRng};
use rand_pcg::Pcg64;
use thiserror::Error;

use crate::config::{
    AccessPattern, ChecksumAlgorithm, DataPlaneKind, GlobalConfig, ResolvedJobConfig, SseType, WorkloadType,
};
use crate::results::{ErrorInfo, JobResult, ReadStats};

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("Job execution failed: {0}")]
    ExecutionFailed(String),

    #[error("S3 operation failed: {0}")]
    S3Error(String),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Resource initialization failed: {0}")]
    ResourceInitError(String),
}

/// Runs benchmark jobs, whatever data plane serves the reads.
#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute_read_job(&self, config: &ResolvedJobConfig) -> Result<JobResult, ExecutionError>;
    async fn execute_write_job(&self, config: &ResolvedJobConfig) -> Result<JobResult, ExecutionError>;
}

/// The one implementation, generic over the read backend.
///
/// Reads always use `D`. Writes usually go through the CRT `upload::Uploader`, except the RTM arm,
/// which writes through its own data-plane `Writer` (`read_plane.open_write`) — that is the path
/// being measured against the CRT uploader. `writes_via_plane` selects which; it is `true` only for
/// the RTM arm, since `PrefetchDataPlane::open_write` only errors.
struct ExecutorImpl<D: DataPlane> {
    client: S3CrtClient,
    uploader: Uploader<S3CrtClient>,
    read_plane: D,
    writes_via_plane: bool,
}

/// Build the configured data plane and an executor over it.
///
/// The only place a data plane type is named. Each arm builds its own concrete `ExecutorImpl`
/// and erases it immediately, which is what keeps the type parameter inside this module — a
/// `match` cannot return two types, but each arm can produce its own trait object.
pub fn build_executor(global: &GlobalConfig) -> Result<Arc<dyn Executor>, ExecutionError> {
    let S3Resources {
        client,
        uploader,
        runtime,
        pool,
    } = build_s3(global)?;

    match global.data_plane {
        DataPlaneKind::Prefetcher => {
            let prefetcher =
                Prefetcher::default_builder(client.clone()).build(runtime, pool, PrefetcherConfig::default());
            Ok(Arc::new(ExecutorImpl {
                client,
                uploader,
                read_plane: PrefetchDataPlane::new(prefetcher),
                // The prefetcher arm is read-only; its writes go through the CRT uploader.
                writes_via_plane: false,
            }))
        }
        #[cfg(feature = "rtm_data_plane")]
        DataPlaneKind::Rtm => {
            let read_plane = build_rtm_plane(global)?;
            Ok(Arc::new(ExecutorImpl {
                client,
                uploader,
                read_plane,
                // The RTM arm writes through its own data-plane Writer, the path under measurement.
                writes_via_plane: true,
            }))
        }
        #[cfg(not(feature = "rtm_data_plane"))]
        DataPlaneKind::Rtm => Err(ExecutionError::ResourceInitError(
            crate::config::RTM_ERROR_STRING.to_string(),
        )),
    }
}

/// The client and uploader alone, for callers that need S3 access but no data plane.
///
/// Test-object generation is the one such caller. It shares this rather than building an
/// executor it would use two fields of — which under `data_plane = "rtm"` also meant standing up
/// a second `aws_sdk_s3::Client` and transfer manager it never read a byte through.
pub fn build_client_and_uploader(
    global: &GlobalConfig,
) -> Result<(S3CrtClient, Uploader<S3CrtClient>), ExecutionError> {
    let resources = build_s3(global)?;
    Ok((resources.client, resources.uploader))
}

/// Everything an executor needs that does not depend on which data plane serves reads.
struct S3Resources {
    client: S3CrtClient,
    uploader: Uploader<S3CrtClient>,
    /// Handed back rather than consumed, because `Prefetcher::build` takes both by value and
    /// only the prefetcher arm wants them.
    runtime: Runtime,
    pool: PagedPool,
}

/// The configured read part size, or the default. Shared by [`build_s3`] and the RTM builder,
/// which size different things from it.
fn read_part_size(global: &GlobalConfig) -> usize {
    global.read_part_size.unwrap_or(8 * 1024 * 1024)
}

/// Build the S3 client, buffer pool, runtime, and uploader from config.
fn build_s3(global: &GlobalConfig) -> Result<S3Resources, ExecutionError> {
    let region = global.region.as_deref().unwrap_or("us-east-1");
    let read_part_size = read_part_size(global);
    let write_part_size = global.write_part_size.unwrap_or(8 * 1024 * 1024);

    let memory_target = global
        .memory_target
        .unwrap_or_else(|| ((effective_total_memory() as f64 * 0.95) / (1024.0 * 1024.0)) as usize);

    let bind = global.bind.clone().unwrap_or_default();

    let sse_type = global.sse.map(|sse| match sse {
        SseType::Aes256 => "AES256".to_string(),
        SseType::AwsKms => "aws:kms".to_string(),
    });

    let checksum_algorithm = match global.checksum_algorithm.unwrap_or(ChecksumAlgorithm::Crc32c) {
        ChecksumAlgorithm::Crc64nvme => Some(mountpoint_s3_client::types::ChecksumAlgorithm::Crc64nvme),
        ChecksumAlgorithm::Crc32c => Some(mountpoint_s3_client::types::ChecksumAlgorithm::Crc32c),
        ChecksumAlgorithm::Crc32 => Some(mountpoint_s3_client::types::ChecksumAlgorithm::Crc32),
        ChecksumAlgorithm::Sha1 => Some(mountpoint_s3_client::types::ChecksumAlgorithm::Sha1),
        ChecksumAlgorithm::Sha256 => Some(mountpoint_s3_client::types::ChecksumAlgorithm::Sha256),
        ChecksumAlgorithm::Off => None,
    };

    let memory_target_bytes = memory_target * 1024 * 1024;
    let pool = PagedPool::config()
        .with_candidate_sizes([CandidateSize::new(read_part_size), CandidateSize::new(write_part_size)])
        .with_memory_limit(memory_target_bytes)
        .build();

    let mut endpoint_config = EndpointConfig::new(region);
    if let Some(url) = &global.endpoint_url {
        let endpoint_uri = Uri::new_from_str(&Allocator::default(), url)
            .map_err(|e| ExecutionError::ResourceInitError(format!("Failed to parse endpoint URL: {}", e)))?;
        endpoint_config = endpoint_config.endpoint(endpoint_uri);
    }

    let mut client_config = S3ClientConfig::new()
        .endpoint_config(endpoint_config)
        .read_backpressure(true)
        .initial_read_window(read_part_size)
        .write_part_size(write_part_size)
        .memory_pool(pool.clone());

    if let Some(throughput_gbps) = global.throughput_target_gbps {
        client_config = client_config.throughput_target_gbps(throughput_gbps);
    }

    if !bind.is_empty() {
        client_config = client_config.network_interface_names(bind);
    }

    let client = S3CrtClient::new(client_config)
        .map_err(|e| ExecutionError::ResourceInitError(format!("Failed to create S3 client: {}", e)))?;

    let runtime = Runtime::new(client.event_loop_group());

    let server_side_encryption = ServerSideEncryption::new(sse_type, global.sse_kms_key_id.clone());

    let uploader = Uploader::new(
        client.clone(),
        runtime.clone(),
        pool.clone(),
        UploaderConfig::new(write_part_size)
            .server_side_encryption(server_side_encryption)
            .default_checksum_algorithm(checksum_algorithm),
    );

    Ok(S3Resources {
        client,
        uploader,
        runtime,
        pool,
    })
}

/// Build the RTM-backed data plane.
///
/// Note this constructs a *second* S3 client — an `aws_sdk_s3::Client` alongside the
/// `S3CrtClient` the uploader and HeadObject calls use. Both exist in the process during
/// a run, with separate connection pools and TLS stacks. That is inherent to running the
/// two data planes in one binary, and worth remembering when reading memory figures.
#[cfg(feature = "rtm_data_plane")]
fn build_rtm_plane(global: &GlobalConfig) -> Result<mountpoint_s3_fs::data::RtmDataPlane, ExecutionError> {
    use aws_sdk_s3_transfer_manager::types::{ConcurrencyMode, MemoryBudgetConfig, PartSize, TargetThroughput};
    use mountpoint_s3_fs::data::{RtmConfig, RtmDataPlane};

    let read_part_size = read_part_size(global);
    let region = global.region.clone().unwrap_or_else(|| "us-east-1".to_string());

    // `load_defaults` is async; the executor is built from sync context, so block on it.
    let sdk_config = futures::executor::block_on(async {
        let mut loader =
            aws_config::defaults(aws_config::BehaviorVersion::latest()).region(aws_config::Region::new(region.clone()));
        if let Some(url) = &global.endpoint_url {
            loader = loader.endpoint_url(url);
        }
        loader.load().await
    });
    let s3 = aws_sdk_s3::Client::new(&sdk_config);

    let mut tm_builder = aws_sdk_s3_transfer_manager::Config::builder()
        .client(s3)
        .part_size(PartSize::Target(read_part_size as u64));
    if let Some(gbps) = global.throughput_target_gbps {
        tm_builder = tm_builder.concurrency(ConcurrencyMode::TargetThroughput(
            TargetThroughput::new_gigabits_per_sec(gbps as u64),
        ));
    }
    // Apply `memory_target` to the transfer manager.
    if let Some(mib) = global.memory_target {
        tm_builder = tm_builder.memory_budget(MemoryBudgetConfig::Limit(mib * 1024 * 1024));
    }
    let tm = aws_sdk_s3_transfer_manager::Client::new(tm_builder.build());
    let mut config = RtmConfig::default();
    if let Some(bytes) = global.rtm_read_ahead_bytes {
        config.max_read_ahead_bytes = bytes;
    }
    if let Some(bytes) = global.rtm_initial_request_size {
        config.initial_request_size = bytes;
    }

    // The transfer manager is built with an explicit `PartSize::Target` above, so the data plane can
    // divide the byte read-ahead ceiling by it exactly rather than assuming a default.
    Ok(RtmDataPlane::new(tm, config))
}

#[async_trait]
impl<D: DataPlane + 'static> Executor for ExecutorImpl<D> {
    async fn execute_read_job(&self, config: &ResolvedJobConfig) -> Result<JobResult, ExecutionError> {
        self.execute_read_job(config).await
    }

    async fn execute_write_job(&self, config: &ResolvedJobConfig) -> Result<JobResult, ExecutionError> {
        self.execute_write_job(config).await
    }
}

impl<D: DataPlane> ExecutorImpl<D> {
    async fn execute_read_job(&self, config: &ResolvedJobConfig) -> Result<JobResult, ExecutionError> {
        if config.workload_type != WorkloadType::Read {
            return Err(ExecutionError::ExecutionFailed(
                "execute_read_job can only execute read workloads".to_string(),
            ));
        }

        match config.access_pattern {
            AccessPattern::Sequential => self.execute_sequential_read(config).await,
            AccessPattern::Random => self.execute_random_read(config).await,
        }
    }

    async fn execute_write_job(&self, config: &ResolvedJobConfig) -> Result<JobResult, ExecutionError> {
        if config.workload_type != WorkloadType::Write {
            return Err(ExecutionError::ExecutionFailed(
                "execute_write_job can only execute write workloads".to_string(),
            ));
        }

        if config.incremental_upload {
            // Append semantics, which the RTM write path cannot express: its upload builder exposes
            // no write offset and no `if_match`. Erroring rather than silently falling back to the
            // CRT uploader, which would mislabel the arm being measured.
            if self.writes_via_plane {
                return Err(ExecutionError::ExecutionFailed(
                    "incremental_upload is not supported by the RTM data plane: its upload builder \
                     has no write offset or if_match"
                        .to_string(),
                ));
            }
            Ok(self.execute_incremental_upload(config).await)
        } else if self.writes_via_plane {
            Ok(self.execute_plane_upload(config).await)
        } else {
            Ok(self.execute_multipart_upload(config).await)
        }
    }

    /// One upload per iteration through the data plane's own write path (the RTM arm).
    ///
    /// The writer declares no size — a filesystem does not know it up front — so RTM streams to
    /// end-of-stream. Reads `writer.stats()` before the terminal call (both `complete` and `abort`
    /// consume the writer), and aborts explicitly on a failed write, since dropping would leave a
    /// partial MPU on S3.
    async fn execute_plane_upload(&self, config: &ResolvedJobConfig) -> JobResult {
        use crate::results::WriteStats;
        use mountpoint_s3_fs::data::{WriteSpec, Writer};

        let mut total_bytes = 0u64;
        let mut errors = Vec::new();
        let mut iterations_completed = 0usize;
        let mut write_stats = WriteStats::default();

        let job_start = Instant::now();
        let contents = vec![0xab; config.write_size];
        let target_size = config.object_size;

        for _iteration in 0..config.iterations {
            if let Some(max_dur) = config.max_duration
                && job_start.elapsed() >= max_dur
            {
                break;
            }

            let spec = WriteSpec::new(config.bucket.clone(), config.object_key.clone());
            let mut writer = match self.read_plane.open_write(spec) {
                Ok(writer) => writer,
                Err(e) => {
                    errors.push(ErrorInfo {
                        error_type: "OpenWriteError".to_string(),
                        message: format!("open_write failed: {e}"),
                    });
                    continue;
                }
            };

            let mut offset = 0u64;
            let mut failed = None;
            while offset < target_size {
                let len = (contents.len() as u64).min(target_size - offset) as usize;
                if let Err(e) = writer.write_at(offset, &contents[..len]).await {
                    failed = Some(ErrorInfo {
                        error_type: "WriteError".to_string(),
                        message: format!("write_at {offset} failed: {e}"),
                    });
                    break;
                }
                offset += len as u64;
            }

            // Read the counters before a terminal call consumes the writer.
            let stats = writer.stats();
            write_stats.bytes_accepted += stats.bytes_accepted;
            write_stats.write_stalls += stats.write_stalls;

            if let Some(error) = failed {
                if let Err(e) = writer.abort().await {
                    errors.push(ErrorInfo {
                        error_type: "AbortError".to_string(),
                        message: format!("abort after a failed write also failed: {e}"),
                    });
                }
                errors.push(error);
                continue;
            }

            match writer.complete().await {
                Ok(outcome) => {
                    write_stats.multipart_uploads += u64::from(outcome.multipart);
                    total_bytes += outcome.size;
                    iterations_completed += 1;
                }
                Err(e) => errors.push(ErrorInfo {
                    error_type: "CompleteError".to_string(),
                    message: format!("complete failed: {e}"),
                }),
            }
        }

        JobResult {
            job_name: config.name.clone(),
            workload_type: "write".to_string(),
            iterations_completed,
            total_bytes,
            elapsed_seconds: job_start.elapsed(),
            errors,
            read_stats: None,
            write_stats: Some(write_stats),
        }
    }

    async fn execute_sequential_read(&self, config: &ResolvedJobConfig) -> Result<JobResult, ExecutionError> {
        let client = &self.client;
        let bucket = &config.bucket;
        let object_key = &config.object_key;

        let head_result = client
            .head_object(bucket, object_key, &HeadObjectParams::new())
            .await
            .map_err(|e| ExecutionError::S3Error(format!("HeadObject failed: {}", e)))?;

        let etag = head_result.etag;
        let size = head_result.size;
        let object_spec = ObjectSpec::new(bucket.clone(), object_key.clone(), etag.as_str(), size);

        let mut total_bytes = 0u64;
        let mut errors = Vec::new();
        let mut iterations_completed = 0usize;
        let mut read_stats = ReadStats::default();

        let job_start = Instant::now();
        let max_duration = config.max_duration;

        for _iteration in 0..config.iterations {
            if let Some(max_dur) = max_duration
                && job_start.elapsed() >= max_dur
            {
                break;
            }

            let request = self.read_plane.open_read(object_spec.clone());
            let mut offset = 0;
            while offset < size {
                if let Some(max_dur) = max_duration
                    && job_start.elapsed() >= max_dur
                {
                    break;
                }

                let read_size = std::cmp::min(config.read_size as u64, size - offset);

                match request.read_at(offset, read_size as usize).await {
                    Ok(segments) => {
                        let chunks = segments.chunk_count();
                        let buffer = segments.to_contiguous();
                        read_stats.add_read(buffer.len(), chunks);
                        let bytes_read = buffer.len() as u64;
                        offset += bytes_read;
                        total_bytes += bytes_read;
                    }
                    Err(e) => {
                        errors.push(ErrorInfo {
                            error_type: "ReadError".to_string(),
                            message: format!("Read failed at offset {}: {}", offset, e),
                        });
                        break;
                    }
                }
            }

            read_stats.add(request.stats());

            if offset >= size {
                iterations_completed += 1;
            }
        }

        let duration = job_start.elapsed();

        Ok(JobResult {
            job_name: config.name.clone(),
            workload_type: "read".to_string(),
            iterations_completed,
            total_bytes,
            elapsed_seconds: duration,
            errors,
            read_stats: Some(read_stats),
            write_stats: None,
        })
    }

    async fn execute_random_read(&self, config: &ResolvedJobConfig) -> Result<JobResult, ExecutionError> {
        let client = &self.client;
        let bucket = &config.bucket;
        let object_key = &config.object_key;

        let head_result = client
            .head_object(bucket, object_key, &HeadObjectParams::new())
            .await
            .map_err(|e| ExecutionError::S3Error(format!("HeadObject failed: {}", e)))?;

        let etag = head_result.etag.clone();
        let object_id = ObjectId::new(object_key.to_string(), head_result.etag);
        let size = head_result.size;
        let object_spec = ObjectSpec::new(bucket.clone(), object_key.clone(), etag.as_str(), size);

        let mut total_bytes = 0u64;
        let mut errors = Vec::new();
        let mut iterations_completed = 0usize;
        let mut read_stats = ReadStats::default();

        let job_start = Instant::now();
        let max_duration = config.max_duration;
        let iteration_duration = config.iteration_duration;

        for iteration in 0..config.iterations {
            if let Some(max_dur) = max_duration
                && job_start.elapsed() >= max_dur
            {
                break;
            }

            let iteration_start = Instant::now();
            let request = self.read_plane.open_read(object_spec.clone());

            // Create a unique, deterministic seed by combining randseed with object_id hash
            // and iteration. This ensures each object/iteration has a different but reproducible
            // random access pattern.
            let randseed = config.randseed;
            let mut hasher = DefaultHasher::new();
            randseed.hash(&mut hasher);
            object_id.hash(&mut hasher);
            iteration.hash(&mut hasher);
            let seed = hasher.finish();
            let mut rng = Pcg64::seed_from_u64(seed);

            let max_offset = size.saturating_sub(1);
            let mut bytes_read_this_iteration = 0u64;

            // Determine exit condition based on iteration_duration
            let should_continue = |bytes_read: u64, iteration_start: &Instant| -> bool {
                if let Some(iter_dur) = iteration_duration {
                    // Time-based: continue until iteration duration elapsed
                    iteration_start.elapsed() < iter_dur
                } else {
                    // Byte-based: read approximately one file's worth of data
                    bytes_read < size
                }
            };

            let mut completed_successfully = true;
            let mut timed_out = false;
            // Note: This intentionally allows overlapping reads, which is acceptable for now.
            while should_continue(bytes_read_this_iteration, &iteration_start) {
                if let Some(max_dur) = max_duration
                    && job_start.elapsed() >= max_dur
                {
                    timed_out = true;
                    break;
                }

                let offset = rng.random_range(0..=max_offset);
                let read_size = std::cmp::min(config.read_size as u64, size - offset);

                match request.read_at(offset, read_size as usize).await {
                    Ok(segments) => {
                        let chunks = segments.chunk_count();
                        let buffer = segments.to_contiguous();
                        read_stats.add_read(buffer.len(), chunks);
                        let bytes_read = buffer.len() as u64;
                        bytes_read_this_iteration += bytes_read;
                        total_bytes += bytes_read;
                    }
                    Err(e) => {
                        errors.push(ErrorInfo {
                            error_type: "ReadError".to_string(),
                            message: format!("Read failed at offset {}: {}", offset, e),
                        });
                        completed_successfully = false;
                        break;
                    }
                }
            }

            read_stats.add(request.stats());

            if completed_successfully && !timed_out {
                iterations_completed += 1;
            }
        }

        let duration = job_start.elapsed();

        Ok(JobResult {
            job_name: config.name.clone(),
            workload_type: "read".to_string(),
            iterations_completed,
            total_bytes,
            elapsed_seconds: duration,
            errors,
            read_stats: Some(read_stats),
            write_stats: None,
        })
    }

    async fn execute_multipart_upload_iteration(
        &self,
        config: &ResolvedJobConfig,
        contents: &[u8],
        max_duration: Option<std::time::Duration>,
        job_start: Instant,
    ) -> Result<u64, ErrorInfo> {
        let uploader = &self.uploader;
        let bucket = &config.bucket;
        let object_key = &config.object_key;

        let mut request = uploader
            .start_atomic_upload(bucket.to_string(), object_key.to_string())
            .map_err(|e| ErrorInfo {
                error_type: "StartUploadError".to_string(),
                message: format!("Failed to start upload: {}", e),
            })?;

        let mut offset = 0;
        let target_size = config.object_size as usize;
        while offset < target_size {
            if let Some(max_dur) = max_duration
                && job_start.elapsed() >= max_dur
            {
                break;
            }

            let bytes_written = request.write(offset as i64, contents).await.map_err(|e| ErrorInfo {
                error_type: "WriteError".to_string(),
                message: format!("Write failed at offset {}: {}", offset, e),
            })?;

            offset += bytes_written;
        }

        if offset < target_size {
            return Err(ErrorInfo {
                error_type: "IncompleteUpload".to_string(),
                message: format!("Upload incomplete: wrote {} of {} bytes", offset, target_size),
            });
        }

        request.complete().await.map_err(|e| ErrorInfo {
            error_type: "CompleteError".to_string(),
            message: format!("Failed to complete upload: {}", e),
        })?;

        Ok(offset as u64)
    }

    async fn execute_multipart_upload(&self, config: &ResolvedJobConfig) -> JobResult {
        let mut total_bytes = 0u64;
        let mut errors = Vec::new();
        let mut iterations_completed = 0usize;

        let job_start = Instant::now();
        let max_duration = config.max_duration;

        // Allocate buffer once and reuse it for all writes
        let contents = vec![0xab; config.write_size];

        for _iteration in 0..config.iterations {
            if let Some(max_dur) = max_duration
                && job_start.elapsed() >= max_dur
            {
                break;
            }

            match self
                .execute_multipart_upload_iteration(config, &contents, max_duration, job_start)
                .await
            {
                Ok(bytes_written) => {
                    total_bytes += bytes_written;
                    iterations_completed += 1;
                }
                Err(error) => {
                    errors.push(error);
                }
            }
        }

        let duration = job_start.elapsed();

        JobResult {
            job_name: config.name.clone(),
            workload_type: "write".to_string(),
            iterations_completed,
            total_bytes,
            elapsed_seconds: duration,
            errors,
            read_stats: None,
            write_stats: None,
        }
    }

    async fn execute_incremental_upload_iteration(
        &self,
        config: &ResolvedJobConfig,
        contents: &[u8],
        max_duration: Option<std::time::Duration>,
        job_start: Instant,
    ) -> Result<u64, ErrorInfo> {
        let uploader = &self.uploader;
        let bucket = &config.bucket;
        let object_key = &config.object_key;

        let mut request = uploader.start_incremental_upload(bucket.to_string(), object_key.to_string(), 0, None);

        let mut offset = 0u64;
        let target_size = config.object_size;
        while offset < target_size {
            if let Some(max_dur) = max_duration
                && job_start.elapsed() >= max_dur
            {
                break;
            }

            request.write(offset, contents).await.map_err(|e| ErrorInfo {
                error_type: "IncrementalWriteError".to_string(),
                message: format!("Incremental write failed at offset {}: {}", offset, e),
            })?;

            offset += contents.len() as u64;
        }

        if offset < target_size {
            return Err(ErrorInfo {
                error_type: "IncompleteUpload".to_string(),
                message: format!("Upload incomplete: wrote {} of {} bytes", offset, target_size),
            });
        }

        request.complete().await.map_err(|e| ErrorInfo {
            error_type: "CompleteError".to_string(),
            message: format!("Failed to complete upload: {}", e),
        })?;

        Ok(offset)
    }

    async fn execute_incremental_upload(&self, config: &ResolvedJobConfig) -> JobResult {
        let mut total_bytes = 0u64;
        let mut errors = Vec::new();
        let mut iterations_completed = 0usize;

        let job_start = Instant::now();
        let max_duration = config.max_duration;

        // Allocate buffer once and reuse it for all writes
        let contents = vec![0xab; config.write_size];

        for _iteration in 0..config.iterations {
            if let Some(max_dur) = max_duration
                && job_start.elapsed() >= max_dur
            {
                break;
            }

            match self
                .execute_incremental_upload_iteration(config, &contents, max_duration, job_start)
                .await
            {
                Ok(bytes_written) => {
                    total_bytes += bytes_written;
                    iterations_completed += 1;
                }
                Err(error) => {
                    errors.push(error);
                }
            }
        }

        let duration = job_start.elapsed();

        JobResult {
            job_name: config.name.clone(),
            workload_type: "write".to_string(),
            iterations_completed,
            total_bytes,
            elapsed_seconds: duration,
            errors,
            read_stats: None,
            write_stats: None,
        }
    }
}
