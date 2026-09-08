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
/// path. [`Rtm`](DataPlaneKind::Rtm) carries an already-built AWS S3 Transfer Manager data plane and
/// is only available when the crate is built with the `rtm_data_plane` feature; it does not validate
/// read checksums and cannot express append (incremental) uploads, which fail at open with
/// `EOPNOTSUPP`.
///
/// The caller constructs the [`RtmDataPlane`](crate::data::RtmDataPlane) from the same S3
/// configuration (region, endpoint, part sizes, throughput/memory targets) it used to build the CRT
/// [`ObjectClient`] this session is given, so the two cannot diverge. There is deliberately no
/// separate RTM config struct here to fill in a second time.
#[derive(Debug, Default)]
pub enum DataPlaneKind {
    #[default]
    Crt,
    #[cfg(feature = "rtm_data_plane")]
    Rtm(crate::data::RtmDataPlane),
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
            DataPlaneKind::Rtm(data_plane) => {
                // The write-handle limiter is sized from the mount's client, the single source of
                // truth for part sizing — the caller configures the RTM writer's part size from the
                // same place, so the two agree.
                let write_part_size = client.write_part_size();
                let fs = S3Filesystem::new_with_data_plane(
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
