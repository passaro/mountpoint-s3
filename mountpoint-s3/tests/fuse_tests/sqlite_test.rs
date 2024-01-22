use std::{fs, io, time::Duration};

use fuser::BackgroundSession;
use mountpoint_s3::{fs::CacheConfig, S3FilesystemConfig};
use mountpoint_s3_client::mock_client::Operation;
use tempfile::{NamedTempFile, TempDir};

use test_case::test_case;

use crate::common::fuse::{self, mock_session, TestClient, TestClientBox, TestSessionConfig};

fn sqlite_read_test<F>(creator_fn: F, cache_config: CacheConfig)
where
    F: FnOnce(&str, TestSessionConfig) -> (TempDir, BackgroundSession, TestClientBox),
{
    const KEY: &str = "db.sqlite";
    let config = TestSessionConfig {
        filesystem_config: S3FilesystemConfig {
            cache_config,
            ..Default::default()
        },
        ..Default::default()
    };
    let (mount_point, _session, mut test_client) = creator_fn("sqlite_read_test", config);

    let items: Vec<_> = (0..100).map(|i| format!("item{i}")).collect();

    let db_data = get_test_db_data(&items).unwrap();
    test_client.put_object(KEY, &db_data).unwrap();

    let path = mount_point.path().join(KEY);
    let connection = sqlite::open(path).unwrap();
    let query = "SELECT * FROM test";

    for _ in 0..10 {
        let mut read_items = Vec::new();
        connection
            .iterate(query, |pairs| {
                for &(_, value) in pairs {
                    if let Some(value) = value {
                        read_items.push(value.to_owned());
                    }
                }
                true
            })
            .unwrap();

        assert_eq!(read_items, items);
    }
}

const METADATA_CACHE_DISABLED_CONFIG: CacheConfig = CacheConfig {
    serve_lookup_from_cache: false,
    file_ttl: Duration::from_millis(100),
    dir_ttl: Duration::from_millis(1000),
    negative_cache_size: 100000,
};
const METADATA_CACHE_ENABLED_CONFIG: CacheConfig = CacheConfig {
    serve_lookup_from_cache: true,
    file_ttl: Duration::from_secs(60),
    dir_ttl: Duration::from_secs(60),
    negative_cache_size: 100000,
};

#[cfg(feature = "s3_tests")]
#[test_case(METADATA_CACHE_DISABLED_CONFIG)]
#[test_case(METADATA_CACHE_ENABLED_CONFIG)]
fn sqlite_read_test_s3(cache_config: CacheConfig) {
    sqlite_read_test(fuse::s3_session::new, cache_config);
}

#[test_case(METADATA_CACHE_DISABLED_CONFIG)]
#[test_case(METADATA_CACHE_ENABLED_CONFIG)]
fn sqlite_read_test_mock(cache_config: CacheConfig) {
    sqlite_read_test(fuse::mock_session::new, cache_config);
}

fn get_test_db_data(items: impl IntoIterator<Item = impl AsRef<str>>) -> Result<Vec<u8>, io::Error> {
    let temp_path = NamedTempFile::new().unwrap();

    {
        let connection = sqlite::open(&temp_path).unwrap();

        let query = "CREATE TABLE test (field TEXT);";
        connection.execute(query).unwrap();

        let mut insert = connection.prepare("INSERT INTO test VALUES (?);").unwrap();
        for item in items.into_iter() {
            insert.bind((1, item.as_ref())).unwrap();
            while insert.next().unwrap() != sqlite::State::Done {}
            insert.reset().unwrap();
        }
    }

    fs::read(&temp_path)
}

#[test_case(METADATA_CACHE_DISABLED_CONFIG)]
#[test_case(METADATA_CACHE_ENABLED_CONFIG)]
fn sqlite_companion_files_lookup_test_mock(cache_config: CacheConfig) {
    let is_negative_cache_enabled = cache_config.serve_lookup_from_cache;
    let config = TestSessionConfig {
        filesystem_config: S3FilesystemConfig {
            cache_config,
            ..Default::default()
        },
        ..Default::default()
    };

    let (mount_point, _session, mut test_client) =
        mock_session::new_mock_session("sqlite_companion_files_lookup_test_mock", config);

    let items: Vec<_> = (0..100).map(|i| format!("item{i}")).collect();

    const KEY: &str = "db.sqlite";

    let db_data = get_test_db_data(&items).unwrap();
    test_client.put_object(KEY, &db_data).unwrap();

    let path = mount_point.path().join(KEY);
    let connection = sqlite::open(path).unwrap();
    let query = "SELECT * FROM test";

    for iteration in 0..10 {
        let head_counter = test_client.client.new_counter(Operation::HeadObject);
        let list_counter = test_client.client.new_counter(Operation::ListObjectsV2);

        let mut read_items = Vec::new();
        connection
            .iterate(query, |pairs| {
                for &(_, value) in pairs {
                    if let Some(value) = value {
                        read_items.push(value.to_owned());
                    }
                }
                true
            })
            .unwrap();

        assert_eq!(read_items, items);

        if is_negative_cache_enabled && iteration != 0 {
            assert_eq!(head_counter.count(), 0);
            assert_eq!(list_counter.count(), 0);
        } else {
            assert_eq!(head_counter.count(), 2);
            assert_eq!(list_counter.count(), 2);
        };
    }
}
