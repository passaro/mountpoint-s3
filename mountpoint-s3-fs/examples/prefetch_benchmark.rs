use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context;
use aws_config::BehaviorVersion;
use aws_sdk_s3_transfer_manager::types::{ConcurrencyMode, PartSize, TargetThroughput};
use clap::{Parser, value_parser};
use futures::channel::mpsc::UnboundedSender;
use futures::executor::block_on;
use futures::{SinkExt, StreamExt};
use mountpoint_s3_client::config::{EndpointConfig, RustLogAdapter, S3ClientConfig};
use mountpoint_s3_client::types::HeadObjectParams;
use mountpoint_s3_client::{ObjectClient, S3CrtClient};
use mountpoint_s3_fs::Runtime;
use mountpoint_s3_fs::data::{DataLayer, Download, PrefetchConfig, TMDataLayer, create_data_layer};
use mountpoint_s3_fs::mem_limiter::MemoryLimiter;
use mountpoint_s3_fs::memory::PagedPool;
use mountpoint_s3_fs::object::ObjectId;
use mountpoint_s3_fs::prefetch::{Prefetcher, PrefetcherConfig};
use serde_json::{json, to_writer};
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};
use tokio::runtime;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::Subscriber;
use tracing_subscriber::util::SubscriberInitExt;

const SECONDS_PER_DAY: u64 = 86400;

/// Like `tracing_subscriber::fmt::init` but sends logs to stderr
fn init_tracing_subscriber() {
    RustLogAdapter::try_init().expect("should succeed as first and only adapter init call");

    let subscriber = Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(supports_color::on(supports_color::Stream::Stderr).is_some())
        .with_writer(std::io::stderr)
        .finish();

    subscriber
        .try_init()
        .expect("should succeed as first and only subscriber init call");
}

#[derive(Parser, Debug)]
#[clap(
    name = "Mountpoint Prefetcher Benchmark",
    about = "Run workloads against the prefetcher component of Mountpoint. Fetched data is discarded."
)]
pub struct CliArgs {
    #[clap(help = "S3 bucket name containing the S3 objects to fetch")]
    pub bucket: String,

    #[clap(help = "List of S3 object keys to fetch", num_args = 1.., value_delimiter = ',')]
    pub s3_keys: Vec<String>,

    #[clap(
        long,
        help = "AWS region of the bucket",
        default_value = "us-east-1",
        value_name = "AWS_REGION"
    )]
    pub region: String,

    #[clap(
        long,
        help = "Target throughput in gibibits per second",
        value_name = "N",
        value_parser = value_parser!(u64).range(1..),
        alias = "throughput-target-gbps",
    )]
    pub maximum_throughput_gbps: Option<u64>,

    #[clap(
        long,
        help = "Maximum memory usage target for Mountpoint's memory limiter [default: 95% of total system memory]",
        value_name = "MiB",
        value_parser = value_parser!(u64).range(512..),
    )]
    pub max_memory_target: Option<u64>,

    #[clap(
        long,
        help = "Part size for multi-part GET in bytes",
        value_name = "BYTES",
        value_parser = value_parser!(u64).range(1..usize::MAX as u64),
        alias = "read-part-size",
    )]
    pub part_size: Option<u64>,

    #[arg(
        long,
        help = "Size of read requests requests to the prefetcher",
        default_value_t = 128 * 1024,
        value_name = "BYTES",
    )]
    read_size: usize,

    #[arg(long, help = "Number of times to download the S3 object", default_value_t = 1)]
    iterations: usize,

    #[arg(
        long,
        help = "Maximum duration in seconds (overrides iterations if specified)",
        value_name = "SECONDS",
        value_parser = parse_duration,
    )]
    max_duration: Option<Duration>,

    #[arg(
        long,
        help = "One or more network interfaces to use when accessing S3. Requires Linux 5.7+ or running as root.",
        value_name = "NETWORK_INTERFACE",
        value_delimiter = ','
    )]
    bind: Option<Vec<String>>,

    #[clap(long, help = "Output file to write the results to", value_name = "OUTPUT_FILE")]
    output_file: Option<PathBuf>,

    #[clap(long, help = "Use the new experimental implementation")]
    experimental: bool,
}

fn parse_duration(arg: &str) -> Result<Duration, String> {
    arg.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|e| format!("Invalid duration: {e}"))
}

impl CliArgs {
    fn memory_target_in_bytes(&self) -> u64 {
        if let Some(target) = self.max_memory_target {
            target * 1024 * 1024
        } else {
            // Default to 95% of total system memory
            let sys = System::new_with_specifics(RefreshKind::everything());
            (sys.total_memory() as f64 * 0.95) as u64
        }
    }

    fn s3_client_config(&self) -> S3ClientConfig {
        // Set up backpressure with the same initial window used in Mountpoint.
        let mut client_config = S3ClientConfig::new()
            .read_backpressure(true)
            .endpoint_config(EndpointConfig::new(self.region.as_str()));
        if let Some(throughput_target_gbps) = self.maximum_throughput_gbps {
            client_config = client_config.throughput_target_gbps(throughput_target_gbps as f64);
        }
        if let Some(part_size) = self.part_size {
            client_config = client_config.part_size(part_size as usize);
            client_config = client_config.initial_read_window(part_size as usize);
        }
        if let Some(nics) = &self.bind {
            client_config = client_config.network_interface_names(nics.to_vec());
        }
        const ENV_VAR_KEY_CRT_ELG_THREADS: &str = "UNSTABLE_CRT_EVENTLOOP_THREADS";
        if let Some(crt_elg_threads) = std::env::var_os(ENV_VAR_KEY_CRT_ELG_THREADS) {
            let crt_elg_threads = crt_elg_threads.to_string_lossy().parse::<u16>().unwrap_or_else(|_| {
                panic!(
                    "Invalid value for environment variable {ENV_VAR_KEY_CRT_ELG_THREADS}. Must be positive integer."
                )
            });
            client_config = client_config.event_loop_threads(crt_elg_threads);
        }

        client_config
    }
}

fn main() -> anyhow::Result<()> {
    init_tracing_subscriber();
    let _metrics_handle = mountpoint_s3_fs::metrics::install(None);

    let args = CliArgs::parse();

    let bucket = args.bucket.as_str();
    let part_size = args.part_size.unwrap_or(8 * 1024 * 1024) as usize;
    let pool = PagedPool::new_with_candidate_sizes([part_size]);
    let client_config = args.s3_client_config().memory_pool(pool.clone());
    let client = S3CrtClient::new(client_config).context("failed to create S3 CRT client")?;
    let mem_limiter = Arc::new(MemoryLimiter::new(pool.clone(), args.memory_target_in_bytes()));
    let runtime = Runtime::new(client.event_loop_group());

    // Verify if all objects exist and collect metadata
    let object_metadata: Vec<(ObjectId, u64)> = args
        .s3_keys
        .iter()
        .map(|key| {
            let head_result = block_on(client.head_object(bucket, key, &HeadObjectParams::new()))
                .with_context(|| format!("HeadObject failed for {key}"))?;
            Ok((ObjectId::new(key.to_string(), head_result.etag), head_result.size))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let threaded_rt = runtime::Runtime::new()?;
    threaded_rt.block_on(async move {
        let (tx, mut rx) = futures::channel::mpsc::unbounded();
        tokio::spawn(async { poll_memory(Duration::from_millis(500), tx).await });

        if args.experimental {
            let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
            let client = aws_sdk_s3::Client::new(&config);

            let mut config_builder = aws_sdk_s3_transfer_manager::Config::builder()
                .client(client)
                .part_size(PartSize::Target(part_size as u64));
            if let Some(gbps) = args.maximum_throughput_gbps {
                config_builder = config_builder.concurrency(ConcurrencyMode::TargetThroughput(
                    TargetThroughput::new_gigabits_per_sec(gbps),
                ));
            }
            let tm_client = aws_sdk_s3_transfer_manager::Client::new(config_builder.build());
            let manager = TMDataLayer::new(tm_client, PrefetchConfig::new(part_size), mem_limiter.clone());

            run_benchmark(args, object_metadata, manager).await;
        } else {
            let manager = create_data_layer(Prefetcher::default_builder(client.clone()).build(
                runtime.clone(),
                mem_limiter.clone(),
                PrefetcherConfig::default(),
            ));

            run_benchmark(args, object_metadata, manager).await;
        }

        rx.close();
        let peak_mem = rx.fold(0, |acc, m| async move { acc.max(m) }).await;
        println!("Peak memory {peak_mem} bytes");
    });

    Ok(())
}

async fn run_benchmark(
    args: CliArgs,
    object_metadata: Vec<(ObjectId, u64)>,
    manager: impl DataLayer + Send + Sync + 'static,
) {
    let bucket = args.bucket.to_string();
    let max_duration = args.max_duration.unwrap_or(Duration::from_secs(SECONDS_PER_DAY));

    println!(
        "Run benchmark - objects: {}, iterations: {}, max duration: {:.2}s",
        object_metadata.len(),
        args.iterations,
        max_duration.as_secs_f64()
    );

    let total_start = Instant::now();
    let mut iteration = 0;
    let mut total_bytes = 0;
    let mut iter_results = Vec::new();
    let timeout: Instant = total_start.checked_add(max_duration).expect("Duration overflow error");
    let manager = Arc::new(manager);
    while iteration < args.iterations && Instant::now() < timeout {
        let received_bytes = Arc::new(AtomicU64::new(0));
        let start = Instant::now();

        let mut download_tasks = JoinSet::new();

        for (object_id, size) in &object_metadata {
            let size = *size;
            let received_bytes = received_bytes.clone();
            let bucket = bucket.clone();
            let key = object_id.key().to_string();
            let etag = object_id.etag().clone();
            let manager = manager.clone();
            let read_size = args.read_size;

            download_tasks.spawn(async move {
                let request = manager.download(bucket, key, etag, size as usize);
                let result = wait_for_download(request, size, read_size as u64, timeout).await;
                if let Ok(bytes_read) = result {
                    received_bytes.fetch_add(bytes_read, Ordering::SeqCst);
                } else {
                    // As object download failures can produce
                    // misleading results, exit the benchmarks
                    // to avoid confusion.
                    eprintln!("Download failed: {:?}", result.err());
                    eprintln!("Exiting benchmarks due to download failure");
                    std::process::exit(1);
                }
            });
        }

        download_tasks.join_all().await;

        let elapsed = start.elapsed();
        let received_size = received_bytes.load(Ordering::SeqCst);
        total_bytes += received_size;
        println!(
            "{iteration}: received {received_size} bytes in {:.2}s: {:.2} Gib/s",
            elapsed.as_secs_f64(),
            (received_size as f64) / elapsed.as_secs_f64() / (1024 * 1024 * 1024 / 8) as f64
        );
        iter_results.push(json!({
            "iteration": iteration,
            "bytes": received_size,
            "elapsed_seconds": elapsed.as_secs_f64(),
        }));
        iteration += 1;
    }
    let total_elapsed = total_start.elapsed();
    println!(
        "\nTotal: {iteration} iterations, {total_bytes} bytes in {:.2}s: {:.2} Gib/s",
        total_elapsed.as_secs_f64(),
        (total_bytes as f64) / total_elapsed.as_secs_f64() / (1024 * 1024 * 1024 / 8) as f64
    );

    if let Some(output_path) = args.output_file {
        let output_file = std::fs::File::create(output_path).expect("Failed to create output_file: {output_path}");
        let results = json!({
            "summary": {
                "total_bytes": total_bytes,
                "total_elapsed_seconds": total_elapsed.as_secs_f64(),
                "max_duration_seconds": max_duration,
                "iterations": iteration,
            },
            "iterations": iter_results
        });
        to_writer(output_file, &results).expect("Failed to write to output file: {output_path}");
    }
}

async fn wait_for_download(
    mut request: impl Download,
    size: u64,
    read_size: u64,
    timeout: Instant,
) -> Result<u64, Box<dyn Error>> {
    let mut offset = 0;
    let mut total_bytes_read = 0;
    while offset < size && Instant::now() < timeout {
        let bytes = request.read(offset, read_size as usize).await?;
        let bytes_read = bytes.len() as u64;
        offset += bytes_read;
        total_bytes_read += bytes_read;
    }
    Ok(total_bytes_read)
}

async fn poll_memory(interval: Duration, mut sender: UnboundedSender<u64>) {
    let mut sys = System::new();
    if let Ok(pid) = sysinfo::get_current_pid() {
        let mut last_mem = 0;
        loop {
            sys.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                false,
                ProcessRefreshKind::nothing().with_memory(),
            );
            if let Some(process) = sys.process(pid) {
                // update the metrics only when there is some change, otherwise it will be too spammy.
                if last_mem != process.memory() {
                    last_mem = process.memory();
                    if let Err(_e) = sender.send(last_mem).await {
                        break;
                    }
                }
            }
            sleep(interval).await;
        }
    }
}
