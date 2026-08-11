//! Does demand-over-speculation priority change what the network does?
//!
//! # Design
//!
//! One foreground reader issuing small scattered reads while N background readers stream
//! sequentially. The background readers' speculation is what the foreground contends with.
//! Nothing is labelled: priority is derived from what a read *is*
//! (`mountpoint_s3_fs::data::Urgency`), so the workload is the only independent variable.
//!
//! Three arms, sweeping the urgency→priority table as configuration:
//!
//! | arm | demand | speculative |
//! | --- | --- | --- |
//! | `flat` | 128 | 128 | the null arm
//! | `default` | 192 | 64 |
//! | `extreme` | 255 | 1 |
//!
//! **The `flat` arm is the control**, and it is the only one. Every arm shares the same workload,
//! the same cursor count, and the same concurrency cap; `flat` differs only in that demand and
//! speculative carry the same priority, so any difference between it and the others is
//! attributable to the priority gap and nothing else.
//!
//! Note the FAST condition is *not* a second control. With `ConcurrencyMode::Explicit(2)` and five
//! readers, requests queue whether or not responses are delayed — foreground p50 is ~190 ms in both
//! conditions. FAST and SLOW differ in how long a slot is held, not in whether slots are
//! contended.
//!
//! # What this can and cannot show
//!
//! Contention here is over the RTM's own concurrency, not network bandwidth: the endpoint serves
//! from memory, so what makes slots scarce is [`SharedFixture`] — **one** RTM client for every
//! reader, with `ConcurrencyMode::Explicit(2)` capping in-flight requests across all of them —
//! plus a per-response delay so an admitted request holds its slot.
//!
//! Sharing the client is essential. Priority is a weight applied within a single scheduler's
//! ready set, so a client-per-reader setup has nothing to order and priority cannot have an effect
//! at all — a null result from such a setup would say nothing.
//!
//! The trade-off: each top-level transfer forms its own scheduling group and fan-out buys no extra
//! share. Every reader holds exactly one cursor, so no arm can take more of the client than
//! another by holding more transfers.
//!
//! Two limits remain, and they bound what a null result here can mean:
//!
//! - **It is not real S3.** No shared bandwidth pipe, and per-request rates are not elastic.
//! - **The pacing is coarse.** The endpoint delays whole responses rather than trickling a body,
//!   so a slot is held for the delay and then released all at once, instead of being held while
//!   bytes arrive gradually.
//!
//! Ignored by default: it is a timing measurement, not a pass/fail correctness test, and it takes
//! minutes. Run with `--ignored --nocapture`.

mod data_plane;

use std::sync::Arc;
use std::time::Duration;

use data_plane::SharedFixture;
use data_plane::workload::Workload;
use mountpoint_s3_fs::data::{DataPlane, PriorityTable, RtmConfig};

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

const OBJECT_SIZE: u64 = 24 * MIB;
/// Background streams. More than the concurrency limit, so they queue against each other and
/// against the foreground.
const BACKGROUND_READERS: usize = 4;
/// Per-response delay standing in for a slow link. An admitted request holds its concurrency slot
/// for at least this long, which is what makes slots contended.
const SLOW_DELAY: Duration = Duration::from_millis(40);
/// In-flight requests allowed across *all* readers. Deliberately far below the number of readers,
/// so there is a queue for priority to order.
const CONCURRENCY: usize = 2;
struct Arm {
    name: &'static str,
    foreground_p50: Duration,
    foreground_p99: Duration,
    foreground_reads: usize,
    background_mib_s: f64,
}

/// Run one arm: every reader on one shared RTM client.
///
/// Every reader holds one cursor, so the only thing that varies between arms is the priority
/// table.
async fn run_arm(name: &'static str, priorities: PriorityTable, slow: bool) -> Arm {
    let config = RtmConfig {
        priorities,
        // Three parts of budget at the fixture's 8 MiB part size => a ceiling of Parts(2).
        max_read_ahead_bytes: 3 * 8 * 1024 * 1024,
        ..Default::default()
    };
    let delay = slow.then_some(SLOW_DELAY);
    let fx = Arc::new(SharedFixture::new(OBJECT_SIZE, config, CONCURRENCY, delay).await);

    // Background: sequential streams, generating the speculation the foreground competes with.
    // Distinct keys, so readers cannot serve each other from resident data.
    let mut background = Vec::new();
    for i in 0..BACKGROUND_READERS {
        let fx = fx.clone();
        background.push(tokio::spawn(async move {
            let reader = Arc::new(fx.plane().open_read(fx.spec(&format!("background-{i}"))));
            let result = Workload::Sequential {
                start: 0,
                read_size: MIB as usize,
                max_bytes: OBJECT_SIZE,
            }
            .run(&*reader)
            .await;
            reader.close().await;
            result
        }));
    }

    // Let the background streams get their downloads in flight, so the foreground contends for
    // slots rather than arriving first.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let foreground = fx.plane().open_read(fx.spec("foreground"));
    let fg = Workload::RandomReads {
        count: 40,
        read_size: 128 * KIB as usize,
        object_size: OBJECT_SIZE,
    }
    .run(&foreground)
    .await
    .expect("foreground workload");
    foreground.close().await;

    let mut background_bytes = 0u64;
    let mut background_secs = 0f64;
    for task in background {
        let result = task.await.expect("background task").expect("background read");
        background_bytes += result.bytes_read;
        background_secs = background_secs.max(result.elapsed.as_secs_f64());
    }

    Arm {
        name,
        foreground_p50: fg.latencies.p50(),
        foreground_p99: fg.latencies.p99(),
        foreground_reads: fg.reads,
        background_mib_s: if background_secs > 0.0 {
            background_bytes as f64 / background_secs / (1024.0 * 1024.0)
        } else {
            0.0
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "timing measurement, not a correctness test: run with --ignored --nocapture"]
async fn priority_sweep() {
    let tables = [
        ("flat (null arm)", PriorityTable::flat()),
        ("default 192/64", PriorityTable::default()),
        ("extreme 255/1", PriorityTable::extreme()),
    ];

    // Both conditions are contended — the concurrency cap, not the delay, is what creates the
    // queue. They differ in how long an admitted request holds its slot.
    let mut fast = Vec::new();
    for (name, table) in tables {
        fast.push(run_arm(name, table, false).await);
    }
    let mut slow = Vec::new();
    for (name, table) in tables {
        slow.push(run_arm(name, table, true).await);
    }

    eprintln!(
        "\nPriority sweep: {BACKGROUND_READERS} background sequential streams vs 1 foreground \
         random-read workload"
    );
    eprintln!(
        "  one shared RTM client, ConcurrencyMode::Explicit({CONCURRENCY}), one cursor per \
         reader,\n  {} MiB objects, 128 KiB foreground reads, read-ahead ceiling 2 parts",
        OBJECT_SIZE / MIB
    );

    for (condition, arms) in [("FAST (no bottleneck)", &fast), ("SLOW responses", &slow)] {
        eprintln!("\n  {condition}");
        if condition.starts_with("SLOW") {
            eprintln!("  every response delayed by {SLOW_DELAY:?}");
        }
        eprintln!(
            "  {:<18} {:>12} {:>12} {:>8} {:>14}",
            "arm", "fg p50", "fg p99", "fg reads", "bg throughput"
        );
        for arm in arms.iter() {
            eprintln!(
                "  {:<18} {:>12} {:>12} {:>8} {:>10.1} MiB/s",
                arm.name,
                format!("{:.2?}", arm.foreground_p50),
                format!("{:.2?}", arm.foreground_p99),
                arm.foreground_reads,
                arm.background_mib_s,
            );
        }
        let null = &arms[0];
        let extreme = &arms[2];
        eprintln!(
            "  extreme vs null: fg p50 {:.2}x, fg p99 {:.2}x, bg throughput {:.2}x",
            extreme.foreground_p50.as_secs_f64() / null.foreground_p50.as_secs_f64().max(1e-9),
            extreme.foreground_p99.as_secs_f64() / null.foreground_p99.as_secs_f64().max(1e-9),
            extreme.background_mib_s / null.background_mib_s.max(1e-9),
        );
    }

    eprintln!(
        "\n  Reading this: a ratio below 1.0 means priority helped the foreground, against the \
         flat\n  arm as control. Both conditions are contended (the concurrency cap, not the \
         delay, makes\n  the queue), so an effect is expected in both.\n\n  Compare `default` \
         against `extreme`: a graded fair-share scheduler should separate them,\n  since their \
         demand:speculative ratios differ by ~85x. If they land together, whatever is\n  \
         happening is not proportional weighting.\n"
    );

    // Asserted weakly on purpose. The output is the table; the point is to learn the answer, not
    // to enforce one. A threshold would either be arbitrary or turn an unexpected result into a
    // red build. What is asserted is that every arm ran and produced data, so a silent zero
    // cannot masquerade as a finding.
    for arm in fast.iter().chain(slow.iter()) {
        assert_eq!(
            arm.foreground_reads, 40,
            "arm {} did not complete its foreground workload",
            arm.name
        );
        assert!(
            arm.background_mib_s > 0.0,
            "arm {} recorded no background throughput",
            arm.name
        );
    }
}
