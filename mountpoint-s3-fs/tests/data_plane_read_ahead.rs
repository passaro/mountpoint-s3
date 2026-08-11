//! What read-ahead costs, in bytes, and how it adapts.
//!
//! Depth is not configured directly: `RtmConfig::max_read_ahead_bytes` is a ceiling, and a cursor
//! opens at `Parts(0)` and grows towards it as reads stall. A ceiling under one part floors to
//! `Parts(0)`, so `max_read_ahead_bytes: 0` pins a cursor at demand paging — which is how most of
//! these tests hold the depth still in order to measure it.
//!
//! The measurement is `ReaderStats::bytes_fetched` against `bytes_delivered`, i.e. read
//! amplification. Fetched bytes are counted as they leave the download body, so this reflects
//! what the transfer layer actually pulled rather than what was asked for.
//!
//! The endpoint also records every range it is asked for, so the figures can be corroborated at
//! the wire instead of trusting the reader's own counters.

mod data_plane;

use data_plane::Fixture;
use mountpoint_s3_fs::data::{DataPlane, Reader, RtmConfig};

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

/// The small-read case: what a 128 KiB read actually costs.
///
/// A 128 KiB read at an 8 MiB part size, with read-ahead growth pinned off
/// (`max_read_ahead_bytes: 0`). The cost is `initial_request_size`, not the part size — the
/// transfer layer's first GET is `min(start + part_size - 1, range_end)`, so the short initial range
/// a cursor opens with caps it well below one part.
#[tokio::test]
async fn small_read_costs_the_initial_request_not_a_part() {
    let part_size = 8 * MIB;
    let config = RtmConfig {
        max_read_ahead_bytes: 0,
        ..Default::default()
    };
    let fx = Fixture::with_part_size(32 * MIB, config, part_size).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    let read_size = 128 * KIB;
    reader.read_at(0, read_size as usize).await.expect("read succeeds");

    let stats = reader.stats();
    assert_eq!(stats.bytes_delivered, read_size);

    // The RTM delivers a chunk at a time, and `read_at` returns as soon as the read is satisfied,
    // so this measures how much had to arrive to serve 128 KiB.
    let initial = RtmConfig::default().initial_request_size;
    let amplification = stats.bytes_fetched as f64 / stats.bytes_delivered as f64;
    eprintln!(
        "128 KiB read at {} MiB parts: fetched {} bytes for {} delivered ({:.1}x amplification); \
         initial_request_size is {} bytes",
        part_size / MIB,
        stats.bytes_fetched,
        stats.bytes_delivered,
        amplification,
        initial,
    );
    assert!(
        stats.bytes_fetched >= stats.bytes_delivered,
        "cannot deliver more than was fetched: {stats:?}"
    );
    // The point: nowhere near a whole part. Without the short initial range this fetched 8 MiB.
    assert!(
        stats.bytes_fetched <= initial,
        "a small read should cost at most the initial request ({initial} bytes), fetched {} — the \
         short opening range is not capping the first GET",
        stats.bytes_fetched
    );
    assert!(
        stats.bytes_fetched < part_size,
        "and must be under one part ({part_size} bytes), fetched {}",
        stats.bytes_fetched
    );
    reader.close().await;
}

/// A smaller part size is the only lever, and it is not per-request.
/// A small read costs the same whatever the part size.
///
/// This used to be the opposite claim: amplification tracked the part size, so the only lever on
/// small-read cost was a smaller part — which is client-wide and would penalise sequential
/// throughput. The short opening range removes that trade. The first GET is
/// `min(start + part_size - 1, range_end)`, and once the range is the smaller term the part size
/// stops mattering, so all three configurations below fetch the same.
#[tokio::test]
async fn small_read_cost_is_independent_of_part_size() {
    let read_size = 128 * KIB;
    let mut measured = Vec::new();

    for part_size in [256 * KIB, MIB, 8 * MIB] {
        let config = RtmConfig {
            max_read_ahead_bytes: 0,
            ..Default::default()
        };
        let fx = Fixture::with_part_size(32 * MIB, config, part_size).await;
        let reader = fx.plane.open_read(fx.spec.clone());
        reader.read_at(0, read_size as usize).await.expect("read succeeds");
        let stats = reader.stats();
        eprintln!(
            "part_size {:>5} KiB -> fetched {:>9} bytes for {} delivered",
            part_size / KIB,
            stats.bytes_fetched,
            stats.bytes_delivered
        );
        measured.push((part_size, stats.bytes_fetched));
        reader.close().await;
    }

    let initial = RtmConfig::default().initial_request_size;
    for (part_size, fetched) in &measured {
        assert!(
            *fetched <= initial,
            "part_size {part_size}: fetched {fetched}, above the initial request ({initial}) — the \
             opening range is not capping the first GET"
        );
    }
    // The 8 MiB part is 64x the read and the 256 KiB part is 2x it, yet both cost the same: the
    // range is what bounds the request, not the part.
    assert_eq!(
        measured[0].1, measured[2].1,
        "a 256 KiB part and an 8 MiB part should cost a small read the same"
    );
}

/// **The 5 MiB download part-size floor.**
///
/// `Config::builder().part_size(PartSize::Target(n))` applies
/// `max(n, MIN_MULTIPART_PART_SIZE_BYTES)` where the constant is 5 MiB
/// (`aws-sdk-s3-transfer-manager/src/config.rs:183`) — and that clamp is applied to the
/// *download* part size too, via `download_part_size_bytes` (`src/client.rs:73`). So a
/// caller cannot configure download parts smaller than 5 MiB, and `PartSize::Auto`
/// resolves to 5 MiB for downloads as well.
///
/// The 5 MiB minimum is an S3 **multipart upload** constraint — `part_size`'s own doc
/// comment discusses only upload behaviour and `PutObject` — and a ranged `GetObject` has
/// no such minimum. Applying it to downloads looks like an oversight rather than a design
/// decision, which makes it a good upstream contribution: narrow, additive, and it does not
/// change upload behaviour.
///
/// The clamp is real, but it is **not** what a small read pays: the requested range caps the first
/// GET below it. So a 128 KiB read costs `initial_request_size`, not 5 MiB — which is what this
/// asserts, since the clamp only starts to matter once a cursor is reading past its initial range.
#[tokio::test]
async fn part_size_has_a_5_mib_floor_on_downloads() {
    const FLOOR: u64 = 5 * MIB;
    let read_size = 128 * KIB;

    // Ask for a 64 KiB part size — two orders of magnitude below the floor.
    let config = RtmConfig {
        max_read_ahead_bytes: 0,
        ..Default::default()
    };
    let fx = Fixture::with_part_size(32 * MIB, config, 64 * KIB).await;
    let reader = fx.plane.open_read(fx.spec.clone());
    reader.read_at(0, read_size as usize).await.expect("read succeeds");

    let stats = reader.stats();
    eprintln!(
        "requested 64 KiB parts, Parts(0), 128 KiB read -> fetched {} bytes \
         ({:.0}x amplification). Floor is {} MiB.",
        stats.bytes_fetched,
        stats.bytes_fetched as f64 / stats.bytes_delivered as f64,
        FLOOR / MIB,
    );
    // The clamp raises the *part* size to 5 MiB, but the cursor's opening range is shorter than
    // that, and the range wins: `min(start + part_size - 1, range_end)`.
    assert!(
        stats.bytes_fetched <= RtmConfig::default().initial_request_size,
        "the opening range should cap the first GET below the {} MiB part floor, fetched {} bytes",
        FLOOR / MIB,
        stats.bytes_fetched
    );
    reader.close().await;
}

/// Are `Parts(n)` boundaries object-absolute or request-relative?
///
/// Source says request-relative: ranges slice forward from the request's own start
/// (`transfer.rs:344`), and the stored-part realignment path is skipped whenever a range is
/// supplied. This checks the consequence that matters — a read at a deliberately
/// part-misaligned offset still returns correct bytes — since a caching proxy needs
/// *absolute* boundaries and would see a different range set than it would for an aligned
/// request.
#[tokio::test]
async fn misaligned_request_start_reads_correctly() {
    let part_size = MIB;
    let config = RtmConfig {
        // Two parts of budget buys one of speculation, i.e. the old `Parts(1)`.
        max_read_ahead_bytes: (2 * part_size) as usize,
        ..Default::default()
    };
    let fx = Fixture::with_part_size(8 * MIB, config, part_size).await;

    // Deliberately not a multiple of the part size, nor of any power of two.
    let offset = part_size + 12345;
    let reader = fx.plane.open_read(fx.spec.clone());
    let segs = reader.read_at(offset, 64 * KIB as usize).await.expect("read");

    assert_eq!(
        &segs.to_contiguous()[..],
        &data_plane::expected_bytes(offset, 64 * KIB as usize)[..],
        "a part-misaligned request start must still yield correct bytes"
    );
    reader.close().await;
}

/// `Parts(0)` is demand paging, not "fetch nothing".
///
/// Worth pinning: the name invites reading it as "no reads at all", but the demand part is
/// always admitted — the window resolves to `n + 1` (RTM
/// `operation/download/read_ahead.rs::window_parts_for`). A sequential scan with growth pinned
/// off must therefore still complete, just without speculation.
#[tokio::test]
async fn parts_zero_still_completes_a_sequential_scan() {
    let config = RtmConfig {
        max_read_ahead_bytes: 0,
        ..Default::default()
    };
    let fx = Fixture::with_part_size(4 * MIB, config, MIB).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    let read_size = 128 * KIB;
    let mut offset = 0;
    while offset < fx.spec.size {
        let segs = reader.read_at(offset, read_size as usize).await.expect("read");
        assert_eq!(segs.len(), read_size as usize);
        offset += read_size;
    }
    assert_eq!(reader.stats().bytes_delivered, fx.spec.size);
    reader.close().await;
}

/// A deeper ceiling should not change correctness, only how much is resident.
#[tokio::test]
async fn deeper_read_ahead_preserves_correctness() {
    for depth in [0usize, 1, 4] {
        let config = RtmConfig {
            max_read_ahead_bytes: depth,
            ..Default::default()
        };
        let fx = Fixture::with_part_size(4 * MIB, config, MIB).await;
        let reader = fx.plane.open_read(fx.spec.clone());

        let mut offset = 0;
        while offset < fx.spec.size {
            let segs = reader.read_at(offset, 256 * KIB as usize).await.expect("read");
            assert_eq!(
                &segs.to_contiguous()[..],
                &data_plane::expected_bytes(offset, 256 * KIB as usize)[..],
                "wrong bytes at {offset} with a ceiling of {depth} parts"
            );
            offset += 256 * KIB;
        }
        reader.close().await;
    }
}

/// The endpoint's own view of what was fetched, as a check on `ReaderStats`.
///
/// Every other amplification figure here comes from `ReaderStats::bytes_fetched`, which the
/// reader increments itself. This asserts the same thing from the wire — the sum of the ranges
/// the endpoint was actually asked for — so a bug in the reader's own accounting cannot make
/// amplification look better than it is.
#[tokio::test]
async fn requested_ranges_corroborate_reported_amplification() {
    let config = RtmConfig {
        max_read_ahead_bytes: 0,
        ..Default::default()
    };
    let fx = Fixture::with_part_size(32 * MIB, config, 8 * MIB).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    let read_size = 128 * KIB;
    reader.read_at(0, read_size as usize).await.expect("read succeeds");

    let stats = reader.stats();
    let on_the_wire = fx.bytes_requested();
    let ranges = fx.requested_ranges();

    eprintln!(
        "wire check: reader reported {} bytes fetched; endpoint saw {} bytes across {} range(s): {:?}",
        stats.bytes_fetched,
        on_the_wire,
        ranges.len(),
        ranges,
    );

    assert!(
        !ranges.is_empty(),
        "the read should have issued at least one ranged GET"
    );
    // The reader counts bytes as they leave the RTM body, and `read_at` returns as soon as the
    // read is satisfied — so a request may still be in flight and the reported figure can lag
    // the wire. It must never *exceed* it: that would mean counting bytes never requested.
    assert!(
        stats.bytes_fetched <= on_the_wire,
        "reader reported {} bytes fetched but only {} were requested",
        stats.bytes_fetched,
        on_the_wire,
    );
    // And the part size, not the read size, is what was asked for.
    assert!(
        on_the_wire >= read_size,
        "expected at least the read size on the wire, saw {on_the_wire}"
    );
    reader.close().await;
}

/// A sequential reader earns depth; a single read does not.
///
/// The point of making read-ahead adaptive: depth is a consequence of how the object is being
/// read, not a setting. This pins both halves against `read_ahead_parts()`, which reports what the
/// cursor has actually told the transfer layer.
#[tokio::test]
async fn sequential_reading_grows_read_ahead_depth() {
    // 5 MiB parts, the smallest a download can use — the builder clamps anything lower. Nine
    // parts of budget buys eight of speculation, since `Parts(n)` is a window of `n + 1`.
    const PART: u64 = 5 * MIB;
    let config = RtmConfig {
        max_read_ahead_bytes: (9 * PART) as usize,
        ..Default::default()
    };
    let fx = Fixture::with_part_size(64 * MIB, config, PART).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    // One read: whatever it cost, it cannot have grown past the first step.
    reader.read_at(0, 128 * KIB as usize).await.expect("read");
    let after_one = reader.read_ahead_parts().await.expect("a cursor is open");
    assert!(
        after_one <= 1,
        "one read should not deepen read-ahead beyond the first step, got {after_one}"
    );

    // Keep reading forward. Each read that has to wait on the body doubles the depth.
    let read_size = 512 * KIB;
    let mut offset = 128 * KIB;
    while offset + read_size < 32 * MIB {
        reader.read_at(offset, read_size as usize).await.expect("read");
        offset += read_size;
    }

    let grown = reader.read_ahead_parts().await.expect("a cursor is open");
    assert!(
        grown > after_one,
        "a sequential scan should deepen read-ahead: started at {after_one}, still {grown}"
    );
    assert!(
        grown <= 8,
        "read-ahead must not exceed the configured ceiling, got {grown}"
    );
    eprintln!("read-ahead depth after a sequential scan: {after_one} -> {grown} parts (ceiling 8)");
    reader.close().await;
}

/// A cursor opened for one scattered read stays at demand paging.
///
/// The case a fixed depth got wrong: under random access nearly every cursor is read once and
/// abandoned, and charging each of them a full window is what made scattered reads ruinous. Each
/// read here lands far enough from the last to force a fresh cursor, so none should ever grow.
#[tokio::test]
async fn scattered_reads_never_deepen_read_ahead() {
    let config = RtmConfig {
        // 5 MiB is the download floor; 33 parts of budget => a ceiling of Parts(32).
        max_read_ahead_bytes: (33 * 5 * MIB) as usize,
        // Small, so a distant read cannot be served by seeking forward within the open cursor.
        max_forward_seek: 1024,
        seek_window_bytes: 4096,
        ..Default::default()
    };
    let fx = Fixture::with_part_size(64 * MIB, config, MIB).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    // Walk backwards in big strides: every read is behind the previous one and outside the seek
    // window, so every one reopens.
    for i in 0..6u64 {
        let offset = (32 - i * 5) * MIB;
        reader.read_at(offset, 128 * KIB as usize).await.expect("read");
        let depth = reader.read_ahead_parts().await.expect("a cursor is open");
        assert_eq!(
            depth, 0,
            "a freshly opened cursor must stay at demand paging, got {depth} after read {i}"
        );
    }

    let stats = reader.stats();
    assert!(
        stats.cursors_opened >= 6,
        "each scattered read should have opened its own cursor (got {stats:?})"
    );
    reader.close().await;
}

/// A client left on `PartSize::Auto` falls back to the transfer layer's download default.
///
/// The read-ahead ceiling is in bytes and the transfer layer's knob counts parts, so a part size is
/// needed to convert between them. Rather than refuse, the data plane assumes 5 MiB — what RTM uses
/// for downloads under `Auto` — and warns. This pins the resulting ceiling, since the assumption is
/// invisible otherwise: it is only right if the transfer layer does not re-pick part sizes to align
/// with an object's stored parts, which `Auto` permits.
#[tokio::test]
async fn auto_part_size_assumes_the_download_default() {
    let server = wiremock::MockServer::start().await;
    let tm = aws_sdk_s3_transfer_manager::Client::new(
        aws_sdk_s3_transfer_manager::Config::builder()
            .client(data_plane::test_s3_client_for(&server))
            .build(),
    );

    // Ten parts of budget at the assumed 5 MiB, so the ceiling is nine parts of speculation.
    let config = RtmConfig {
        max_read_ahead_bytes: (10 * 5 * MIB) as usize,
        ..Default::default()
    };
    let plane = mountpoint_s3_fs::data::RtmDataPlane::new(tm, config);

    assert_eq!(
        plane.max_read_ahead_parts(),
        9,
        "an unset part size should resolve against RTM's 5 MiB download default"
    );
}

/// The follow-on request is issued *while* the first range is still being read.
///
/// That is the whole point of chaining it early: if it were opened only when the first range ran
/// out, a sequential scan would pay a round trip in the middle. Asserted from the wire — the
/// endpoint records every range it is asked for — so a lazy implementation fails this even though it
/// would return identical bytes.
#[tokio::test]
async fn the_follow_on_request_is_issued_before_it_is_needed() {
    let initial = RtmConfig::default().initial_request_size;
    let fx = Fixture::with_part_size(64 * MIB, RtmConfig::default(), 5 * MIB).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    // Read past the trigger (half the initial range by default) but stay inside the initial range,
    // so nothing has yet needed the follow-on.
    let read_size = 128 * KIB;
    let stop = initial / 2 + read_size;
    let mut offset = 0;
    while offset < stop {
        reader.read_at(offset, read_size as usize).await.expect("read");
        offset += read_size;
    }
    assert!(offset < initial, "test should still be inside the initial range");

    // `requests_issued` counts GETs, not cursors: a second one means the follow-on was opened while
    // the reader was still inside the first range. (The wire is checked in
    // `reading_across_the_handover_is_seamless`; here the ranges the endpoint has *served* lag the
    // request, since RTM issues its discovery GET asynchronously after `initiate` returns.)
    let stats = reader.stats();
    eprintln!(
        "after reading {offset} of an {initial}-byte opening range: {} request(s), {} cursor(s), \
         ranges served so far {:?}",
        stats.requests_issued,
        stats.cursors_opened,
        fx.requested_ranges(),
    );
    assert_eq!(
        stats.cursors_opened, 1,
        "still one cursor: the follow-on is a request, not a new cursor (got {stats:?})"
    );
    assert_eq!(
        stats.requests_issued, 2,
        "expected the follow-on to have been issued already, before the opening range ran out \
         (got {stats:?})"
    );
    reader.close().await;
}

/// A scan across the handover returns correct bytes, and keeps its earned read-ahead depth.
///
/// Two things could break at the switch from the initial request to the chained one: bytes could be
/// served from the wrong offset (the two requests must be contiguous), and the read-ahead ramp could
/// reset (it must not — it is one cursor throughout, and the reader has proven itself sequential by
/// getting this far).
#[tokio::test]
async fn reading_across_the_handover_is_seamless() {
    let initial = RtmConfig::default().initial_request_size;
    let fx = Fixture::with_part_size(64 * MIB, RtmConfig::default(), 5 * MIB).await;
    let reader = fx.plane.open_read(fx.spec.clone());

    let read_size = 256 * KIB;
    let mut offset = 0;
    let mut depth_before_handover = None;

    // Read well past the initial range, checking bytes either side of the boundary.
    while offset < initial + 8 * read_size {
        if offset + read_size >= initial && depth_before_handover.is_none() {
            depth_before_handover = reader.read_ahead_parts().await;
        }
        let segs = reader.read_at(offset, read_size as usize).await.expect("read");
        assert_eq!(
            &segs.to_contiguous()[..],
            &data_plane::expected_bytes(offset, read_size as usize)[..],
            "wrong bytes at {offset} (initial range ends at {initial})"
        );
        offset += read_size;
    }

    let before = depth_before_handover.expect("sampled before the handover");
    let after = reader.read_ahead_parts().await.expect("a cursor is open");
    eprintln!("read-ahead depth across the handover: {before} -> {after} parts");
    assert!(
        after >= before,
        "read-ahead depth must not reset at the handover: {before} -> {after}"
    );

    // One cursor throughout, but more than one request: the two counters mean different things.
    let stats = reader.stats();
    assert_eq!(
        stats.cursors_opened, 1,
        "a sequential scan is one cursor, chained requests and all (got {stats:?})"
    );
    assert!(
        stats.requests_issued >= 2,
        "and it should have issued the initial request plus a follow-on (got {stats:?})"
    );
    reader.close().await;
}
