//! Reader conformance: does `read_at` return the right bytes, under every access pattern?
//!
//! Correctness before performance. Each test asserts both the length
//! contract — exactly `len` bytes, or fewer only at EOF — and that the bytes are *right*,
//! by checking every byte against a function of its own offset. A test that only checked
//! lengths would pass while serving data from the wrong offset, which is precisely the bug
//! naive position-tracking risks.
//!
//! The alternating-two-region test is the shape a single-cursor prefetcher turns into
//! worst-case random reads, and the one a sequential-only harness cannot see.

mod data_plane;

use data_plane::{Fixture, expected_bytes};
use mountpoint_s3_client::types::ETag;
use mountpoint_s3_fs::{
    data::{DataPlane, ReadError, Reader, RtmConfig},
    object::ObjectId,
};

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

/// Assert a read returned exactly the object's bytes for that range.
async fn assert_reads(reader: &impl Reader, offset: u64, len: usize) {
    let segs = reader
        .read_at(offset, len)
        .await
        .unwrap_or_else(|e| panic!("read at {offset} len {len} failed: {e}"));
    assert_eq!(segs.len(), len, "short read at offset {offset}");
    assert_eq!(
        &segs.to_contiguous()[..],
        &expected_bytes(offset, len)[..],
        "wrong bytes at offset {offset} len {len}"
    );
}

#[tokio::test]
async fn sequential_reads_cover_the_object() {
    let fx = Fixture::new(4 * MIB, RtmConfig::default()).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    let read_size = 128 * KIB;
    let mut offset = 0;
    while offset < fx.spec.size {
        assert_reads(&reader, offset, read_size as usize).await;
        offset += read_size;
    }

    let stats = reader.stats();
    assert_eq!(stats.bytes_delivered, fx.spec.size);
    assert_eq!(stats.cursors_opened, 1, "a purely sequential read needs one cursor");
    reader.close().await;
}

#[tokio::test]
async fn read_past_end_is_clamped_not_an_error() {
    let size = 100 * KIB;
    let fx = Fixture::new(size, RtmConfig::default()).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    // Ask for more than remains: a filesystem read returns what exists.
    let segs = reader.read_at(size - 1000, 8192).await.expect("clamped read succeeds");
    assert_eq!(segs.len(), 1000);
    assert_eq!(&segs.to_contiguous()[..], &expected_bytes(size - 1000, 1000)[..]);
    reader.close().await;
}

#[tokio::test]
async fn read_at_or_past_end_is_out_of_range() {
    let size = 64 * KIB;
    let fx = Fixture::new(size, RtmConfig::default()).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    for offset in [size, size + 1, size * 2] {
        match reader.read_at(offset, 512).await {
            Err(ReadError::OutOfRange { offset: o, size: s }) => {
                assert_eq!(o, offset);
                assert_eq!(s, size);
            }
            other => panic!("expected OutOfRange at {offset}, got {other:?}"),
        }
    }
    reader.close().await;
}

#[tokio::test]
async fn zero_length_read_is_empty_and_harmless() {
    let fx = Fixture::new(64 * KIB, RtmConfig::default()).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    let segs = reader.read_at(0, 0).await.expect("zero-length read");
    assert!(segs.is_empty());
    assert_eq!(
        reader.stats().cursors_opened,
        0,
        "a zero-length read should not open a download"
    );
    reader.close().await;
}

#[tokio::test]
async fn strided_reads_return_correct_bytes() {
    let fx = Fixture::new(4 * MIB, RtmConfig::default()).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    // Read 64 KiB every 256 KiB: forward seeks within one cursor's range.
    let read_size = 64 * KIB;
    let stride = 256 * KIB;
    let mut offset = 0;
    while offset + read_size <= fx.spec.size {
        assert_reads(&reader, offset, read_size as usize).await;
        offset += stride;
    }
    reader.close().await;
}

#[tokio::test]
async fn backward_seek_within_window_is_served_in_place() {
    let config = RtmConfig {
        seek_window_bytes: 1024 * 1024,
        ..Default::default()
    };
    let fx = Fixture::new(4 * MIB, config).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    // Read forward, then step back inside the retained window.
    assert_reads(&reader, 0, 256 * KIB as usize).await;
    assert_reads(&reader, 128 * KIB, 64 * KIB as usize).await;
    assert_reads(&reader, 0, 4096).await;

    let stats = reader.stats();
    assert_eq!(
        stats.cursors_opened, 1,
        "a backward seek inside the window must not restart the download"
    );
    reader.close().await;
}

#[tokio::test]
async fn backward_seek_beyond_window_opens_a_new_cursor() {
    let config = RtmConfig {
        // Retain almost nothing, so the backward seek cannot be served in place.
        seek_window_bytes: 4096,
        ..Default::default()
    };
    let fx = Fixture::new(4 * MIB, config).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    assert_reads(&reader, 2 * MIB, 128 * KIB as usize).await;
    // Far behind the retained bytes: needs a fresh download, and must still be correct.
    assert_reads(&reader, 0, 128 * KIB as usize).await;

    assert!(
        reader.stats().cursors_opened >= 2,
        "an unservable backward seek should reopen the download"
    );
    reader.close().await;
}

#[tokio::test]
async fn alternating_regions_reopen_each_time() {
    // A reader alternating between two regions further apart than the seek window. With one
    // cursor this is worst-case: each alternation tears the download down and opens another.
    //
    // Asserted rather than merely tolerated, because it is the cost of the single-cursor shape
    // and the thing a multi-cursor design would exist to avoid. Correctness must hold anyway.
    let config = RtmConfig {
        seek_window_bytes: 64 * 1024,
        max_forward_seek: 1024, // small, so the far region is never served by a seek
        ..Default::default()
    };
    let fx = Fixture::new(8 * MIB, config).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    let read_size = 32 * KIB as usize;
    let alternations = 4u64;
    for i in 0..alternations {
        assert_reads(&reader, i * read_size as u64, read_size).await;
        assert_reads(&reader, 6 * MIB + i * read_size as u64, read_size).await;
    }

    let stats = reader.stats();
    assert!(
        stats.cursors_opened > alternations,
        "one cursor must reopen on each alternation (got {stats:?})"
    );
    // Every reopen aborts the download it replaced, so nothing is left running.
    assert_eq!(
        stats.requests_aborted,
        stats.cursors_opened - 1,
        "every cursor but the live one should have been aborted (got {stats:?})"
    );
    reader.close().await;
}

#[tokio::test]
async fn concurrent_reads_on_disjoint_regions_are_correct() {
    // `read_at(&self)` exists so that reads at unrelated offsets need not serialize. This
    // asserts the correctness half of that: whatever the interleaving, every read returns
    // its own bytes.
    use std::sync::Arc;

    let config = RtmConfig {
        seek_window_bytes: 64 * 1024,
        max_forward_seek: 1024,
        ..Default::default()
    };
    let fx = Fixture::new(8 * MIB, config).await;
    let reader = Arc::new(fx.plane.open_read(fx.spec.clone()));

    let mut tasks = Vec::new();
    for region in 0..4u64 {
        let reader = reader.clone();
        tasks.push(tokio::spawn(async move {
            let base = region * 2 * MIB;
            for i in 0..8u64 {
                let offset = base + i * 32 * KIB;
                let segs = reader.read_at(offset, 16 * KIB as usize).await.expect("read");
                assert_eq!(
                    &segs.to_contiguous()[..],
                    &expected_bytes(offset, 16 * KIB as usize)[..],
                    "wrong bytes at {offset} under concurrency"
                );
            }
        }));
    }
    for task in tasks {
        task.await.expect("task panicked");
    }
    reader.close().await;
}

#[tokio::test]
async fn etag_mismatch_is_rejected() {
    // Reading an object that changed underneath must fail rather than splice two versions
    // together. The endpoint returns 412 on an `If-Match` mismatch, as a real ranged GET does.
    let fx = Fixture::new(MIB, RtmConfig::default()).await;
    let mut spec = fx.spec.clone();
    spec.id = ObjectId::new(
        spec.id.key().to_string(),
        ETag::from("\"0000000000000000000000000000dead\""),
    );
    let reader = fx.plane.open_read(spec);

    let result = reader.read_at(0, 4096).await;
    assert!(
        result.is_err(),
        "a stale etag must not read successfully, got {result:?}"
    );
    reader.close().await;
}

#[tokio::test]
async fn small_object_smaller_than_a_part() {
    let size = 5 * KIB;
    let fx = Fixture::with_part_size(size, RtmConfig::default(), 8 * MIB).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    assert_reads(&reader, 0, size as usize).await;
    assert_reads(&reader, 1000, 1000).await;
    reader.close().await;
}

#[tokio::test]
async fn reads_spanning_part_boundaries_are_contiguous() {
    // A read straddling two RTM parts arrives as multiple segments. The bytes must still
    // join up in order — the segmented representation must not reorder or drop.
    let part_size = MIB;
    let fx = Fixture::with_part_size(4 * MIB, RtmConfig::default(), part_size).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    // Straddle the first boundary.
    assert_reads(&reader, part_size - 4096, 8192).await;
    // Span an entire part plus both edges.
    assert_reads(&reader, part_size + 512, (part_size + 1024) as usize).await;
    reader.close().await;
}

#[tokio::test]
async fn forward_seek_past_the_initial_range_is_served() {
    // A cursor opens over a short initial range and chains the rest when a read runs on past it.
    // But a *forward seek* consumes that range without completing a read: `can_serve` allows a seek
    // anywhere within the cursor's range — the object's end — while the cursor only holds bytes to
    // the end of its first request. The chain therefore has to be openable from inside the fill
    // loop, not only after a read succeeds.
    //
    // This regressed exactly that: scattered reads failed with `UnexpectedEof` once a seek target
    // sat beyond the initial range but within `max_forward_seek` of the position.
    let config = RtmConfig {
        // Large enough that a distant read is served by seeking forward rather than reopening.
        max_forward_seek: 16 * MIB,
        ..Default::default()
    };
    let fx = Fixture::new(32 * MIB, config).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    // Land in the initial range, then seek several megabytes past its end — still a forward seek, so
    // the same cursor must serve it by discarding through the handover.
    assert_reads(&reader, 0, 128 * KIB as usize).await;
    assert_reads(&reader, 8 * MIB, 128 * KIB as usize).await;

    let stats = reader.stats();
    assert_eq!(
        stats.cursors_opened, 1,
        "a forward seek within range should not reopen the cursor (got {stats:?})"
    );
    assert!(
        stats.requests_issued >= 2,
        "it should have chained a follow-on request to reach the target (got {stats:?})"
    );
    reader.close().await;
}
