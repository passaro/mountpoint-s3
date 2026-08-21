//! Writer conformance: does an upload put the right bytes on the wire, and does it fail where it
//! should?
//!
//! Assertions read the endpoint's record rather than the writer's own counters. A writer can
//! report the right byte count while assembling the object wrongly, and only the wire tells the
//! two apart. Object content is a function of offset (`byte[i] = i % 251`), so a part landing at
//! the wrong position shows up in the bytes.
//!
//! Every upload here is of **unknown length**: the writer declares no size, so RTM reads the
//! stream to end-of-stream (its unknown-length streaming support, PR #164). This is the shape a
//! filesystem writer needs — the file's size is not known when it is opened.

mod data_plane;

use std::time::Duration;

use data_plane::{UploadFixture, expected_bytes};
use mountpoint_s3_fs::data::{DataPlane, RtmConfig, WriteError, WriteSpec, Writer};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

/// The smallest upload part size the transfer manager will actually use.
///
/// Requesting less is silently raised to this, as
/// [`a_sub_5_mib_part_size_is_silently_raised_to_the_floor`] shows, so every part-boundary test
/// below works in multiples of it.
const PART: u64 = 5 * 1024 * 1024;

/// Write `total` bytes through `writer` in `chunk`-sized pieces of offset-derived content.
async fn write_all(writer: &mut impl Writer, total: usize, chunk: usize) {
    let mut offset = 0;
    while offset < total {
        let len = chunk.min(total - offset);
        let data = expected_bytes(offset as u64, len);
        let n = writer
            .write_at(offset as u64, &data)
            .await
            .unwrap_or_else(|e| panic!("write at {offset} len {len} failed: {e}"));
        assert_eq!(n, len, "a short write is not a success");
        offset += len;
    }
}

/// An unknown-length upload stores exactly the bytes written, across the sizes that stress the
/// part boundaries: a sub-part object, an object of exactly one part, exact multiples, and a
/// short tail. This is the headline behaviour — the writer declares no size and RTM drains the
/// stream — checked byte for byte and against S3's equal-part-size rule.
#[tokio::test]
async fn unknown_length_uploads_store_exactly_what_was_written() {
    let cases = [
        ("sub-part", 1000usize),
        ("exactly one part", PART as usize),
        ("two whole parts", 2 * PART as usize),
        ("several parts plus a tail", 3 * PART as usize + 1000),
    ];
    for (label, size) in cases {
        let fx = UploadFixture::with_part_size(RtmConfig::default(), PART).await;
        let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");
        write_all(&mut writer, size, 128 * KIB).await;
        let outcome = writer.complete().await.expect("complete");

        assert_eq!(outcome.size, size as u64, "{label}: reported size");
        let log = fx.log();
        assert_eq!(log.assembled(), expected_bytes(0, size), "{label}: stored bytes");

        // Every part but the last must equal the part size (S3's rule).
        let sizes = log.part_sizes();
        if let Some((last, rest)) = sizes.split_last() {
            assert!(
                rest.iter().all(|s| *s == PART as usize),
                "{label}: non-final parts must be full, got {sizes:?}"
            );
            assert!(
                *last <= PART as usize,
                "{label}: last part over the size, got {sizes:?}"
            );
        }
    }
}

/// Drain-driven, not budget-driven: RTM puts exactly `ceil(size / part_size)` parts on the wire,
/// no phantom dispatches. The count distinguishes reading a stream to its end from spending a
/// part budget computed from a declared length.
#[tokio::test]
async fn no_phantom_part_dispatches() {
    let fx = UploadFixture::with_part_size(RtmConfig::default(), PART).await;
    let size = 2 * PART as usize + 1000;

    let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");
    write_all(&mut writer, size, 128 * KIB).await;
    writer.complete().await.expect("complete");

    let log = fx.log();
    let expected_parts = size.div_ceil(PART as usize);
    assert_eq!(
        log.parts.len(),
        expected_parts,
        "exactly ceil(bytes / part_size) parts should reach the wire, got {}",
        log.parts.len()
    );
}

/// A streamed upload is always multipart, whatever the object's size.
///
/// A 100-byte object still costs `CreateMultipartUpload` + `UploadPart` +
/// `CompleteMultipartUpload`, three round trips where a `PutObject` would be one, and no
/// configuration changes it: a caller-supplied part stream cannot be rewound, so the multipart
/// path is taken before the size or the multipart threshold is consulted.
///
/// Worth pinning down because small objects are the common case for a filesystem, and the CRT
/// uploader issues a single `PutObject` for them. Avoiding it would mean buffering the whole
/// object in memory instead of streaming it, giving up the bounded
/// memory the channel exists to provide.
#[tokio::test]
async fn every_streamed_upload_is_multipart_even_a_tiny_one() {
    let fx = UploadFixture::new(RtmConfig::default()).await;
    let size = 100;

    let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");
    write_all(&mut writer, size, size).await;
    let outcome = writer.complete().await.expect("complete");

    assert_eq!(outcome.size, size as u64);
    assert!(
        outcome.multipart,
        "a PartStream is MPU-only, so even 100 bytes goes out as a multipart upload"
    );

    let log = fx.log();
    assert_eq!(log.creates, 1, "CreateMultipartUpload, for 100 bytes");
    assert_eq!(log.puts.len(), 0, "PutObject is unreachable for a streamed body");
    assert_eq!(log.part_sizes(), vec![size], "one part, carrying everything");
    assert_eq!(
        log.assembled(),
        expected_bytes(0, size),
        "stored bytes must match written"
    );
}

/// A `write_part_size` below 5 MiB is raised to it, silently — no error, no warning.
///
/// The sink cuts parts at `WriterConfig::write_part_size`, but S3 requires every part but the last
/// to be at least 5 MiB, so the writer clamps a smaller request to that floor. Pinned down because
/// it means a caller cannot shrink upload parts to trade write amplification for latency.
#[tokio::test]
async fn a_sub_5_mib_part_size_is_silently_raised_to_the_floor() {
    let requested = 64 * KIB as u64;
    let fx = UploadFixture::with_part_size(RtmConfig::default(), requested).await;
    let size = 12 * MIB;

    let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");
    write_all(&mut writer, size, 128 * KIB).await;
    writer.complete().await.expect("complete");

    let sizes = fx.log().part_sizes();
    assert_eq!(
        sizes,
        vec![PART as usize, PART as usize, 2 * MIB],
        "requested {requested} byte parts; the writer used its 5 MiB floor"
    );
    println!("upload part-size floor: requested {requested} bytes, got {sizes:?}");
}

#[tokio::test]
async fn multipart_upload_assembles_in_order() {
    let fx = UploadFixture::with_part_size(RtmConfig::default(), PART).await;
    // Three whole parts plus a short tail, so both the equal-size rule and the
    // short-last-part exception are exercised.
    let size = 3 * PART as usize + 1000;

    let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");
    write_all(&mut writer, size, 128 * KIB).await;
    let outcome = writer.complete().await.expect("complete");

    assert_eq!(outcome.size, size as u64);
    assert!(outcome.multipart);

    let log = fx.log();
    assert!(log.is_multipart());
    assert_eq!(
        log.assembled(),
        expected_bytes(0, size),
        "the assembled object must match what was written, byte for byte"
    );

    // S3's rule: every part but the last is the same size.
    let sizes = log.part_sizes();
    let (last, rest) = sizes.split_last().expect("at least one part");
    assert!(
        rest.iter().all(|s| *s == PART as usize),
        "every part but the last must equal the part size, got {sizes:?}"
    );
    assert_eq!(*last, 1000, "the last part carries the remainder");
    assert_eq!(sizes.len(), 4);
}

/// A write smaller than the part size, repeated: the pull side must coalesce these into whole
/// parts rather than emitting one undersized part per write, which S3 would reject.
#[tokio::test]
async fn many_small_writes_coalesce_into_whole_parts() {
    let fx = UploadFixture::with_part_size(RtmConfig::default(), PART).await;
    let size = 2 * PART as usize;

    let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");
    // 4 KiB writes: 1,280 of them per part.
    write_all(&mut writer, size, 4 * KIB).await;
    writer.complete().await.expect("complete");

    let log = fx.log();
    assert_eq!(
        log.part_sizes(),
        vec![PART as usize, PART as usize],
        "small writes must be coalesced, not sent as undersized parts"
    );
    assert_eq!(log.assembled(), expected_bytes(0, size));
}

/// A write larger than a part must be split across parts, not sent oversized.
#[tokio::test]
async fn a_write_larger_than_a_part_is_split() {
    let fx = UploadFixture::with_part_size(RtmConfig::default(), PART).await;
    let size = 3 * PART as usize;

    let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");
    // One write covering everything, three times the part size.
    write_all(&mut writer, size, size).await;
    writer.complete().await.expect("complete");

    let log = fx.log();
    assert_eq!(log.part_sizes(), vec![PART as usize; 3]);
    assert_eq!(log.assembled(), expected_bytes(0, size));
}

#[tokio::test]
async fn out_of_order_write_is_rejected_and_leaves_the_stream_usable() {
    let fx = UploadFixture::with_part_size(RtmConfig::default(), PART).await;

    let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");
    let data = expected_bytes(0, 8 * KIB);
    writer.write_at(0, &data).await.expect("first write");

    // Backwards.
    let err = writer.write_at(0, &data).await.expect_err("rewriting is not allowed");
    assert!(matches!(
        err,
        WriteError::OutOfOrderWrite {
            write_offset: 0,
            expected_offset: 8192
        }
    ));

    // Forwards, leaving a hole.
    let err = writer
        .write_at(16 * KIB as u64, &data)
        .await
        .expect_err("skipping ahead is not allowed");
    assert!(matches!(err, WriteError::OutOfOrderWrite { .. }));

    // A rejected write must not have consumed the position, so the correct next write lands.
    let next = expected_bytes(8 * KIB as u64, 8 * KIB);
    writer
        .write_at(8 * KIB as u64, &next)
        .await
        .expect("resumes at the position");

    let outcome = writer.complete().await.expect("complete");
    assert_eq!(outcome.size, 16 * KIB as u64);
    assert_eq!(fx.log().assembled(), expected_bytes(0, 16 * KIB));
}

/// The RTM writer streams a single multipart upload with no write offset or `if_match`, so it
/// cannot express an append. An incremental `WriteSpec` is rejected up front, before anything is
/// sent — the CRT arm covers append instead.
#[tokio::test]
async fn rtm_rejects_incremental_upload() {
    let fx = UploadFixture::new(RtmConfig::default()).await;

    let err = fx
        .plane
        .open_write(WriteSpec::incremental("test-bucket", "object"))
        .err()
        .expect("RTM must reject an incremental upload");
    assert!(matches!(err, WriteError::IncrementalUnsupported), "got {err:?}");

    let log = fx.log();
    assert_eq!(log.creates, 0, "nothing should reach the wire");
    assert_eq!(log.puts.len(), 0);
}

#[tokio::test]
async fn empty_upload_completes() {
    let fx = UploadFixture::with_part_size(RtmConfig::default(), PART).await;

    let writer = fx.plane.open_write(fx.spec()).expect("open_write");
    let outcome = writer.complete().await.expect("an object with no bytes is legal");

    assert_eq!(outcome.size, 0);
    assert!(fx.log().assembled().is_empty());
}

/// `abort()` issues `AbortMultipartUpload`, so a cancelled upload leaves nothing behind on S3.
#[tokio::test]
async fn abort_issues_abort_multipart_upload() {
    let fx = UploadFixture::with_part_size(RtmConfig::default(), PART).await;

    let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");
    write_all(&mut writer, 2 * PART as usize, 16 * KIB).await;
    writer.abort().await.expect("abort");

    let log = fx.log();
    assert!(log.creates > 0, "an MPU was started, so there is something to abort");
    assert_eq!(log.aborts, 1, "abort() must issue AbortMultipartUpload");
    assert!(log.completes.is_empty(), "an aborted upload must not be completed");
}

/// A dropped writer issues no `AbortMultipartUpload`, leaving a partial upload on S3.
///
/// The counterpart to the test above, and why `abort` cannot be left to `Drop`. Asserted rather
/// than only documented, because it is a hazard a caller has to know about.
#[tokio::test]
async fn dropping_a_writer_does_not_abort_the_multipart_upload() {
    let fx = UploadFixture::with_part_size(RtmConfig::default(), PART).await;

    {
        let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");
        write_all(&mut writer, 2 * PART as usize, 16 * KIB).await;
        // Dropped without complete() or abort(). `Drop` warns, but cannot issue the abort,
        // since that call is async.
    }

    // Give any cancellation time to land, so this is not just a race that was won.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let log = fx.log();
    assert!(log.creates > 0, "the MPU was created");
    assert_eq!(
        log.aborts, 0,
        "a dropped handle leaves the MPU incomplete on S3 — this is why abort() is explicit"
    );
}

/// Backpressure: a writer pushing faster than the network must block rather than buffer without
/// limit. With the single-part rendezvous, the writer fills one part and then awaits RTM's take,
/// so once RTM's (capped) concurrency is saturated by slow uploads the next part cannot be handed
/// off and the write stalls. Peak buffered between caller and network is exactly one part.
#[tokio::test]
async fn a_writer_outrunning_the_network_is_throttled() {
    // `slow` caps RTM concurrency and delays each part, so parts back up behind the network.
    let fx = UploadFixture::slow(RtmConfig::default(), PART, Duration::from_millis(50)).await;

    let size = 4 * PART as usize;
    let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");
    write_all(&mut writer, size, 16 * KIB).await;

    let stats = writer.stats();
    assert_eq!(stats.bytes_accepted, size as u64);
    assert!(
        stats.write_stalls > 0,
        "writing {size} bytes at 50ms per part with capped concurrency must stall; \
         if it does not, the one-part rendezvous is not throttling the writer"
    );
    println!(
        "backpressure: {} stalls writing {size} bytes one part at a time",
        stats.write_stalls
    );

    let outcome = writer.complete().await.expect("complete");
    assert_eq!(outcome.size, size as u64);
    assert_eq!(fx.log().assembled(), expected_bytes(0, size));
}

/// The hazard the rendezvous's close-on-drop exists to prevent: when the transfer dies, a writer
/// must learn about it — whether it is blocked on a handoff or reaches completion — rather than
/// waiting forever for a part nobody will take.
///
/// Wrapped in a timeout, so a regression fails the test instead of hanging CI. The failure may
/// legitimately surface at either `write_at` or `complete`, depending on how much was buffered
/// when the transfer died; the contract is that it surfaces, promptly, as the transfer's own
/// error.
#[tokio::test]
async fn a_failed_transfer_surfaces_to_the_writer_rather_than_hanging() {
    // 403: not retryable, so the transfer fails promptly rather than exhausting a retry budget.
    // `failing_parts` also caps concurrency so the writer blocks on a handoff before the failure.
    let fx = UploadFixture::failing_parts(RtmConfig::default(), PART, 403).await;

    // Several parts' worth, so parts are actually dispatched (and so fail) rather than the whole
    // object sitting in the single buffered part.
    let size = 4 * PART as usize;
    let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");

    let err = tokio::time::timeout(Duration::from_secs(60), async {
        let mut offset = 0usize;
        while offset < size {
            let len = (16 * KIB).min(size - offset);
            let data = expected_bytes(offset as u64, len);
            if let Err(e) = writer.write_at(offset as u64, &data).await {
                return e;
            }
            offset += len;
        }
        // Everything was accepted before the failure propagated back; completion must report it.
        writer
            .complete()
            .await
            .expect_err("completing a failed transfer must not succeed")
    })
    .await
    .expect("a failed transfer must not hang the writer");

    assert!(
        matches!(err, WriteError::Transfer(_)),
        "the writer should report the transfer's own error, got {err:?}"
    );
    println!("failed transfer surfaced as: {err}");
}

/// A full-object checksum must reach `CompleteMultipartUpload`.
///
/// The default checksum strategy asks for a full-object CRC64-NVME but cannot compute one for a
/// caller-supplied stream, since it only sees individual parts. Without the value `PartSource`
/// provides, the upload would be completed declaring a full-object checksum type with nothing to
/// validate against — leaving the write path doing less integrity checking than the read path's
/// comparison arm.
#[tokio::test]
async fn a_full_object_checksum_reaches_complete_multipart_upload() {
    let fx = UploadFixture::with_part_size(RtmConfig::default(), PART).await;
    let size = 2 * PART as usize;

    let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");
    write_all(&mut writer, size, 16 * KIB).await;
    writer.complete().await.expect("complete");

    let log = fx.log();
    assert_eq!(
        log.complete_checksums.len(),
        1,
        "CompleteMultipartUpload should carry exactly one full-object checksum, got {:?}",
        log.complete_checksums
    );
    // Check the value, not merely its presence: the CRC must cover the whole object.
    assert_eq!(
        log.complete_checksums[0],
        expected_crc64(size),
        "the checksum must be over the assembled object"
    );
}

/// The CRC64-NVME an object of `size` synthetic bytes should have, base64-encoded as S3 wants
/// it. Computed independently of the writer, so a wrong checksum is caught rather than mirrored.
fn expected_crc64(size: usize) -> String {
    use mountpoint_s3_client::checksums::crc64nvme::Crc64nvmeHasher;
    let mut hasher = Crc64nvmeHasher::new();
    hasher.update(&expected_bytes(0, size));
    mountpoint_s3_client::checksums::crc64nvme_to_base64(&hasher.finalize())
}

/// A checksum is reported whether or not the object divides evenly into parts.
///
/// Regression test. A part yielded with nothing left staged may be the last one; if RTM happens
/// not to poll again before completing, the end-of-stream branch that finalizes the checksum
/// never runs. `PartSource` guards against that by also recording the running checksum whenever a
/// part empties the accumulator, so an exactly-aligned object still carries its checksum. Both
/// alignments are asserted so the asymmetry cannot regress unnoticed.
#[tokio::test]
async fn a_part_size_aligned_object_still_reports_its_checksum() {
    for (label, size) in [("aligned", 2 * PART as usize), ("unaligned", 2 * PART as usize + 1)] {
        let fx = UploadFixture::with_part_size(RtmConfig::default(), PART).await;
        let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");
        write_all(&mut writer, size, 128 * KIB).await;
        writer.complete().await.expect("complete");

        let log = fx.log();
        assert_eq!(
            log.complete_checksums.len(),
            1,
            "{label} ({size} bytes): CompleteMultipartUpload carried no full-object checksum"
        );
        assert_eq!(
            log.complete_checksums[0],
            expected_crc64(size),
            "{label} ({size} bytes): checksum must cover the whole object"
        );
        assert_eq!(log.assembled(), expected_bytes(0, size), "{label}: wrong bytes stored");
    }
}

/// Both terminal calls consume the writer, so double-completion is a compile error rather than
/// a runtime one. What remains testable is the *stats* contract: everything accepted is
/// dispatched by the time `complete` returns.
#[tokio::test]
async fn completion_accounts_for_every_accepted_byte() {
    let fx = UploadFixture::with_part_size(RtmConfig::default(), PART).await;
    let size = 3 * PART as usize + 77;

    let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");
    write_all(&mut writer, size, 4 * KIB).await;

    let before = writer.stats();
    assert_eq!(before.bytes_accepted, size as u64);

    let outcome = writer.complete().await.expect("complete");
    assert_eq!(outcome.size, size as u64);
    assert_eq!(
        fx.log().assembled().len(),
        size,
        "the endpoint must have received exactly what was accepted"
    );
}

/// An upload larger than a single part, written in FUSE-sized pieces, against a delayed
/// endpoint — the shape closest to a real write workload, checked end to end.
#[tokio::test]
async fn a_realistic_sequential_write_is_correct() {
    let fx = UploadFixture::slow(RtmConfig::default(), 5 * MIB as u64, Duration::from_millis(5)).await;
    let size = 12 * MIB;

    let mut writer = fx.plane.open_write(fx.spec()).expect("open_write");
    // 128 KiB is Mountpoint's typical FUSE write size.
    write_all(&mut writer, size, 128 * KIB).await;

    // Read the counters before `complete` consumes the writer.
    let stats = writer.stats();
    let outcome = writer.complete().await.expect("complete");

    assert_eq!(outcome.size, size as u64);
    assert!(outcome.multipart);
    assert_eq!(stats.bytes_accepted, size as u64);

    let log = fx.log();
    assert_eq!(log.assembled(), expected_bytes(0, size));
    assert_eq!(
        log.part_sizes(),
        vec![5 * MIB, 5 * MIB, 2 * MIB],
        "12 MiB at a 5 MiB part size"
    );
    println!(
        "sequential write: {} MiB in 128 KiB writes -> {} parts, sizes {:?}, {} stalls",
        size / MIB,
        log.part_sizes().len(),
        log.part_sizes(),
        stats.write_stalls
    );
}
