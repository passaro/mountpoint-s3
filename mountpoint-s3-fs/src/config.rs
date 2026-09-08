use anyhow::{Context as _, anyhow};
use futures::executor::block_on;
use mountpoint_s3_client::ObjectClient;

use crate::data_cache::{DataCacheConfig, DiskDataCache, ExpressDataCache, MultilevelDataCache};
use crate::fuse::config::FuseSessionConfig;
use crate::fuse::session::FuseSession;
use crate::fuse::{ErrorLogger, S3FuseFilesystem};
use crate::memory::PagedPool;
use crate::metablock::Metablock;
use crate::prefetch::{Prefetcher, PrefetcherBuilder};
use crate::sync::Arc;
use crate::{Runtime, S3Filesystem, S3FilesystemConfig};

/// Which [`DataPlane`](crate::data::DataPlane) the filesystem reads and writes through.
///
/// Defaults to [`Crt`](DataPlaneKind::Crt), the CRT-backed prefetcher/uploader — the production
/// path. [`Rtm`](DataPlaneKind::Rtm) selects the experimental AWS S3 Transfer Manager backend and
/// is only available when the crate is built with the `rtm_data_plane` feature; it does not
/// validate read checksums and cannot express append (incremental) uploads, which fail at open with
/// `EOPNOTSUPP`.
#[derive(Debug, Default)]
pub enum DataPlaneKind {
    #[default]
    Crt,
    #[cfg(feature = "rtm_data_plane")]
    Rtm(RtmMountConfig),
}

/// Settings for standing up the RTM transfer-manager client the [`Rtm`](DataPlaneKind::Rtm) backend
/// drives. The transfer manager is a separate client stack from the CRT [`ObjectClient`] the mount
/// uses for metadata.
#[cfg(feature = "rtm_data_plane")]
#[derive(Debug, Clone, Default)]
pub struct RtmMountConfig {
    /// AWS region; falls back to `us-east-1` when unset.
    pub region: Option<String>,
    /// Override the S3 endpoint (e.g. for a local test server).
    pub endpoint_url: Option<String>,
    /// Target read part size in bytes; `None` uses the transfer manager default.
    pub read_part_size: Option<usize>,
    /// Write part size in bytes; `None` uses the RTM writer default.
    pub write_part_size: Option<usize>,
    /// Target throughput in gigabits/sec; `None` uses the transfer manager default.
    pub throughput_target_gbps: Option<usize>,
    /// Memory budget in MiB, shared by reads and writes; `None` leaves the transfer manager default.
    pub memory_target_mib: Option<usize>,
}

/// Configuration for a Mountpoint session
#[derive(Debug)]
pub struct MountpointConfig {
    fuse_session_config: FuseSessionConfig,
    data_cache_config: DataCacheConfig,
    filesystem_config: S3FilesystemConfig,
    error_logger: Option<Box<dyn ErrorLogger + Send + Sync>>,
    data_plane: DataPlaneKind,
}

impl MountpointConfig {
    pub fn new(
        fuse_session_config: FuseSessionConfig,
        filesystem_config: S3FilesystemConfig,
        data_cache_config: DataCacheConfig,
    ) -> anyhow::Result<Self> {
        if filesystem_config.read_only != fuse_session_config.read_only() {
            return Err(anyhow!(
                "read-only must be set consistently: `FuseOptions::read_only` is {} but \
                `S3FilesystemConfig::read_only` is {}",
                fuse_session_config.read_only(),
                filesystem_config.read_only,
            ));
        }

        Ok(Self {
            fuse_session_config,
            data_cache_config,
            filesystem_config,
            error_logger: None,
            data_plane: DataPlaneKind::default(),
        })
    }

    /// Set the [Self::error_logger] field
    pub fn error_logger(mut self, error_logger: impl ErrorLogger + Send + Sync + 'static) -> Self {
        self.error_logger = Some(Box::new(error_logger));
        self
    }

    /// Select the [`DataPlane`](crate::data::DataPlane) backend. Defaults to
    /// [`DataPlaneKind::Crt`].
    pub fn data_plane(mut self, data_plane: DataPlaneKind) -> Self {
        self.data_plane = data_plane;
        self
    }

    /// Create a new FUSE session
    pub fn create_fuse_session<Client>(
        self,
        metablock: impl Metablock + 'static,
        client: Client,
        runtime: Runtime,
        memory_pool: PagedPool,
    ) -> anyhow::Result<FuseSession>
    where
        Client: ObjectClient + Clone + Send + Sync + 'static,
    {
        tracing::trace!(filesystem_config=?self.filesystem_config, data_plane=?self.data_plane, "creating file system");
        // Each arm builds its own concrete `S3FuseFilesystem<Client, DP>` and erases it into a
        // `FuseSession`, so the two backends unify at that boundary despite having different `DP`.
        let session = match self.data_plane {
            DataPlaneKind::Crt => {
                let prefetcher_builder =
                    create_prefetcher_builder(self.data_cache_config, &client, &runtime, memory_pool.clone())?;
                let fs = S3Filesystem::new(
                    client,
                    prefetcher_builder,
                    memory_pool,
                    runtime,
                    metablock,
                    self.filesystem_config,
                );
                let fuse_fs = S3FuseFilesystem::new(fs, self.error_logger);
                FuseSession::new(fuse_fs, self.fuse_session_config)?
            }
            #[cfg(feature = "rtm_data_plane")]
            DataPlaneKind::Rtm(rtm_config) => {
                let (data_plane, write_part_size) = build_rtm_data_plane(&rtm_config)?;
                // `Client` here is only the phantom type parameter; the RTM plane carries its own
                // transfer-manager client and does not use it.
                let fs = S3Filesystem::<Client, _>::new_with_data_plane(
                    data_plane,
                    write_part_size,
                    memory_pool,
                    metablock,
                    self.filesystem_config,
                );
                let fuse_fs = S3FuseFilesystem::new(fs, self.error_logger);
                FuseSession::new(fuse_fs, self.fuse_session_config)?
            }
        };
        ctrlc::set_handler(session.shutdown_fn()).context("failed to set interrupt handler")?;
        Ok(session)
    }
}

/// Build the RTM transfer-manager data plane and report the write part size it will use (needed to
/// size the write-handle limiter). Mirrors the `s3io_benchmark` example's setup.
#[cfg(feature = "rtm_data_plane")]
fn build_rtm_data_plane(config: &RtmMountConfig) -> anyhow::Result<(crate::data::RtmDataPlane, usize)> {
    use aws_sdk_s3_transfer_manager::memory::{BufferPool, MemoryBudgetConfig, MemoryConfig};
    use aws_sdk_s3_transfer_manager::types::{ConcurrencyMode, PartSize, TargetThroughput};

    use crate::data::{RtmConfig, RtmDataPlane};

    let region = config.region.clone().unwrap_or_else(|| "us-east-1".to_string());

    // `load` is async; `create_fuse_session` is sync, so block on it.
    let sdk_config = block_on(async {
        let mut loader =
            aws_config::defaults(aws_config::BehaviorVersion::latest()).region(aws_config::Region::new(region));
        if let Some(url) = &config.endpoint_url {
            loader = loader.endpoint_url(url);
        }
        loader.load().await
    });
    let s3 = aws_sdk_s3::Client::new(&sdk_config);

    let mut tm_builder = aws_sdk_s3_transfer_manager::Config::builder().client(s3);
    if let Some(read_part_size) = config.read_part_size {
        tm_builder = tm_builder.part_size(PartSize::Target(read_part_size as u64));
    }
    if let Some(gbps) = config.throughput_target_gbps {
        tm_builder = tm_builder.concurrency(ConcurrencyMode::TargetThroughput(
            TargetThroughput::new_gigabits_per_sec(gbps as u64),
        ));
    }
    // One pool, shared by the transfer manager and the data plane's writer.
    let mut pool_builder = BufferPool::builder();
    if let Some(mib) = config.memory_target_mib {
        pool_builder = pool_builder.memory_budget(MemoryBudgetConfig::Limit(mib * 1024 * 1024));
    }
    let pool = pool_builder.build().context("invalid RTM memory target")?;
    tm_builder = tm_builder.memory(MemoryConfig::Explicit(pool));
    let tm = aws_sdk_s3_transfer_manager::Client::new(tm_builder.build());

    let mut rtm_config = RtmConfig::default();
    if let Some(bytes) = config.write_part_size {
        rtm_config.writer.write_part_size = bytes;
    }
    let write_part_size = rtm_config.writer.write_part_size;

    Ok((RtmDataPlane::new(tm, rtm_config), write_part_size))
}

fn create_prefetcher_builder<Client>(
    data_cache_config: DataCacheConfig,
    client: &Client,
    runtime: &Runtime,
    memory_pool: PagedPool,
) -> anyhow::Result<PrefetcherBuilder<Client>>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    let disk_cache = data_cache_config
        .disk_cache_config
        .map(|config| DiskDataCache::new(config, memory_pool));
    let express_cache = match data_cache_config.express_cache_config {
        None => None,
        Some(config) => {
            let cache_bucket_name = config.bucket_name.clone();
            let express_cache = ExpressDataCache::new(client.clone(), config);
            block_on(express_cache.verify_cache_valid())
                .with_context(|| format!("initial PutObject failed for shared cache bucket {cache_bucket_name}"))?;
            Some(express_cache)
        }
    };
    let client = client.clone();
    let builder = match (disk_cache, express_cache) {
        (None, Some(express_cache)) => Prefetcher::caching_builder(express_cache, client),
        (Some(disk_cache), None) => Prefetcher::caching_builder(disk_cache, client),
        (Some(disk_cache), Some(express_cache)) => {
            let cache = MultilevelDataCache::new(Arc::new(disk_cache), express_cache, runtime.clone());
            Prefetcher::caching_builder(cache, client)
        }
        _ => Prefetcher::default_builder(client),
    };
    Ok(builder)
}

#[cfg(test)]
mod tests {
    use fuser::MountOption;
    use test_case::test_case;

    use super::*;
    use crate::fuse::config::MountPoint;

    /// Built directly rather than through [`FuseSessionConfig::new`] + [`MountPoint::new`], which
    /// validate the mount point against the real file system and `/proc`. The tests below only care
    /// about read-only.
    fn fuse_session_config(read_only: bool) -> FuseSessionConfig {
        FuseSessionConfig {
            mount_point: MountPoint::Directory("/mnt/mountpoint-s3-test".into()),
            options: if read_only { vec![MountOption::RO] } else { vec![] },
            max_threads: 1,
            clone_fuse_fd: false,
        }
    }

    fn mountpoint_config(fuse_read_only: bool, fs_read_only: bool) -> anyhow::Result<MountpointConfig> {
        MountpointConfig::new(
            fuse_session_config(fuse_read_only),
            S3FilesystemConfig {
                read_only: fs_read_only,
                ..Default::default()
            },
            DataCacheConfig::default(),
        )
    }

    #[test_case(false, false)]
    #[test_case(true, true)]
    fn test_read_only_consistent_is_accepted(fuse_read_only: bool, fs_read_only: bool) {
        mountpoint_config(fuse_read_only, fs_read_only).expect("consistent read-only configuration should be accepted");
    }

    #[test_case(true, false)]
    #[test_case(false, true)]
    fn test_read_only_inconsistent_is_rejected(fuse_read_only: bool, fs_read_only: bool) {
        let err = mountpoint_config(fuse_read_only, fs_read_only)
            .expect_err("inconsistent read-only configuration should be rejected");
        assert!(
            err.to_string().contains("read-only must be set consistently"),
            "unexpected error: {err}"
        );
    }
}
