//! The crash matrix of plan §9 (2), the retention overrun of §4.6, and the two
//! instances of §6.6 — the three places where "exactly once" is a claim rather
//! than a hope.
//!
//! The matrix is the load-bearing one. For each of the five points a process
//! can die at, a driver is killed there, a FRESH driver is started over the same
//! Queen and the same bucket, and the result is compared against a control run
//! that never crashed:
//!
//! * the data objects are **byte-identical** — same keys, same sha256, which is
//!   what makes a retried upload an overwrite of the same content (plan §4.2);
//! * every record appears **exactly once** across every object;
//! * the windows **tile** with no gap and no overlap.
//!
//! The crash is in-process ([`CrashMode::Return`]) so that one test binary can
//! run the whole matrix; the e2e suite runs the same five points against a real
//! process that really dies.

use std::sync::Arc;

use queen_s3::config::CrashAt;
use queen_s3::driver::Stop;
use queen_s3::lease::{Acquired, Lease};
use queen_s3::obs::{M_PRECONDITION_LOST, M_RECORDS_LOST, M_WINDOWS_COMMITTED};
use queen_s3::types::{Format, Layout, Micros};
use queen_s3::writer::WriterConfig;

#[path = "driver_support.rs"]
mod support;
use support::*;

/// The control: what the bucket holds when nothing goes wrong.
async fn control(parts: &[&str], layout: Layout, wcfg: WriterConfig) -> (Rig, Vec<(String, i64)>) {
    let mut cfg = test_cfg();
    cfg.layout = layout;
    let rig = Rig::with_writer(cfg, wcfg);
    let expected = seed_two_hours(&rig.queen, "orders", parts);
    let mut d = rig.driver("orders").await;
    run_until(&mut d, 600, |d| d.engine().committed_k() >= 2).await;
    run_until_quiet(&mut d, 400, 8).await;
    assert_eq!(d.engine().committed_k(), 2, "the control must ship it all");
    (rig, expected)
}

/// Kill a driver at `at`, restart it, and hold the result against the control.
async fn matrix_case(at: CrashAt, layout: Layout, wcfg: WriterConfig) {
    let parts = ["a", "b"];
    let (reference, expected) = control(&parts, layout, wcfg.clone()).await;
    let want = fingerprint(&reference.store);
    let want_manifests: Vec<u64> = manifests(&reference.store).iter().map(|m| m.k).collect();

    let mut cfg = test_cfg();
    cfg.layout = layout;
    let rig = Rig::with_writer(cfg.clone(), wcfg);
    seed_two_hours(&rig.queen, "orders", &parts);
    let lease = rig.own("orders", "inst-a").await;

    let mut crashing = cfg.clone();
    crashing.crash_at = at;
    let mut first = rig.restart(crashing, "orders", lease.clone());
    let stop = run_until(&mut first, 600, |_| false).await;
    assert_eq!(
        stop,
        Some(Stop::Crashed(at)),
        "the crash point must actually fire"
    );
    // A crashed process runs no destructor and answers no engine: everything
    // the restart needs is already in the KV store and the bucket.
    drop(first);

    let mut second = rig.restart(cfg, "orders", lease);
    run_until(&mut second, 600, |d| d.engine().committed_k() >= 2).await;
    run_until_quiet(&mut second, 400, 8).await;

    assert_eq!(
        fingerprint(&rig.store),
        want,
        "after a crash at {}, the objects must be byte-identical to a clean run",
        at.as_str()
    );
    assert_eq!(
        manifests(&rig.store)
            .iter()
            .map(|m| m.k)
            .collect::<Vec<_>>(),
        want_manifests,
        "and the same windows must exist"
    );
    assert_exactly_once(&rig.store, &expected);
    assert_no_gaps(&rig.store);
    assert_windows_tile(&manifests(&rig.store));
    assert_manifests_match_objects(&rig.store);
}

#[tokio::test(start_paused = true)]
async fn crash_after_intent_redoes_the_identical_window() {
    matrix_case(
        CrashAt::AfterIntent,
        Layout::Merged,
        WriterConfig::default(),
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn crash_mid_upload_redoes_the_identical_window() {
    matrix_case(CrashAt::MidUpload, Layout::Merged, WriterConfig::default()).await;
}

#[tokio::test(start_paused = true)]
async fn crash_after_upload_redoes_the_identical_window() {
    matrix_case(
        CrashAt::AfterUpload,
        Layout::Merged,
        WriterConfig::default(),
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn crash_before_commit_redoes_the_identical_window() {
    matrix_case(
        CrashAt::BeforeCommit,
        Layout::Merged,
        WriterConfig::default(),
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn crash_after_commit_moves_on_without_repeating_a_record() {
    matrix_case(
        CrashAt::AfterCommit,
        Layout::Merged,
        WriterConfig::default(),
    )
    .await;
}

/// The awkward corner of `mid_upload`: with one object per partition the crash
/// lands between two objects of the SAME window, so the restart has to rewrite
/// the ones that landed and write the ones that did not.
#[tokio::test(start_paused = true)]
async fn crash_mid_upload_with_one_object_per_partition() {
    matrix_case(
        CrashAt::MidUpload,
        Layout::PerPartition,
        WriterConfig::default(),
    )
    .await;
}

/// And the same for Parquet, whose bytes are a footer, a schema and a set of
/// row groups rather than a stream of lines: determinism there is a property of
/// the pinned writer properties, and this is where it is exercised through a
/// real redo.
#[tokio::test(start_paused = true)]
async fn crash_before_commit_with_parquet_objects() {
    matrix_case(
        CrashAt::BeforeCommit,
        Layout::Merged,
        WriterConfig {
            format: Format::Parquet,
            parquet_row_group_records: 8,
            ..WriterConfig::default()
        },
    )
    .await;
}

/// Retention passed the sink by (plan §4.6): the gap is counted, named in the
/// window's manifest, and the sink keeps committing — a stalled sink loses
/// more, not less.
#[tokio::test(start_paused = true)]
async fn a_retention_overrun_is_counted_named_and_survived() {
    let rig = Rig::new(test_cfg());

    // Window 1: three records per partition, committed at safeTime.
    for p in ["a", "b"] {
        rig.queen.push(
            "orders",
            p,
            t("2026-09-04T10:05:00.000000Z"),
            &["{\"n\":1}"; 3],
        );
    }
    rig.queen.set_safe_time(t("2026-09-04T10:10:00.000000Z"));
    let mut d = rig.driver("orders").await;
    run_until(&mut d, 400, |d| d.engine().committed_k() >= 1).await;
    assert_eq!(all_rows(&rig.store).len(), 6);

    // More arrives, and retention eats the middle of it before the sink reads
    // it: offsets 3..5 of partition `a` are gone for ever.
    for p in ["a", "b"] {
        rig.queen.push(
            "orders",
            p,
            t("2026-09-04T10:20:00.000000Z"),
            &["{\"n\":2}"; 3],
        );
        rig.queen.push(
            "orders",
            p,
            t("2026-09-04T10:30:00.000000Z"),
            &["{\"n\":3}"; 3],
        );
    }
    rig.queen.retention_delete_below("orders", "a", 6);
    rig.queen.set_safe_time(t("2026-09-04T10:40:00.000000Z"));

    run_until(&mut d, 400, |d| d.engine().committed_k() >= 2).await;
    run_until_quiet(&mut d, 200, 8).await;

    assert_eq!(
        rig.metrics.counter(M_RECORDS_LOST, &[("queue", "orders")]),
        3,
        "the gap is counted by the offset range, not guessed"
    );
    let ms = manifests(&rig.store);
    let lost: Vec<_> = ms.iter().flat_map(|m| m.lost.iter()).collect();
    assert_eq!(lost.len(), 1, "one gap, named once: {lost:?}");
    assert_eq!(lost[0].partition, "a");
    assert_eq!((lost[0].from, lost[0].to), (3, 5));

    // …and it kept going: everything retention left is in the lake.
    let mut expected: Vec<(String, i64)> = Vec::new();
    for o in 0..3 {
        expected.push(("a".to_string(), o));
    }
    for o in 6..9 {
        expected.push(("a".to_string(), o));
    }
    for o in 0..9 {
        expected.push(("b".to_string(), o));
    }
    assert_exactly_once(&rig.store, &expected);
    assert_windows_tile(&ms);
    assert_manifests_match_objects(&rig.store);
    assert!(
        rig.metrics
            .counter(M_WINDOWS_COMMITTED, &[("queue", "orders")])
            >= 2,
        "a sink that stalls on a gap loses more than one that carries on"
    );
}

/// Two instances, one queue (plan §6.6): exactly one owns it, and the other's
/// FIRST durable write — the intent — is rolled back whole by the fence at
/// index 0. It therefore never writes an object, and the windows still tile.
#[tokio::test(start_paused = true)]
async fn two_instances_one_queue_and_only_one_of_them_commits() {
    let rig = Rig::new(test_cfg());
    let expected = seed_two_hours(&rig.queen, "orders", &["a"]);

    let a = rig.own("orders", "inst-a").await;
    let b = Arc::new(Lease::new(
        rig.queen.clone(),
        "default",
        "orders",
        "inst-b",
        30_000,
    ));
    assert_eq!(
        b.acquire().await.expect("the claim is answered"),
        Acquired::HeldBy("inst-a".to_string()),
        "the second instance is told who owns the queue"
    );

    // The owner ships one window.
    let mut da = rig.restart(rig.cfg.clone(), "orders", a);
    run_until(&mut da, 400, |d| d.engine().committed_k() >= 1).await;
    let after_a = data_keys(&rig.store);
    assert_eq!(after_a.len(), 1);

    // The second instance runs anyway — an operator started a second copy — and
    // gets exactly as far as its first conditional write.
    let mut db = rig.restart(rig.cfg.clone(), "orders", b);
    let stop = run_until(&mut db, 400, |_| false).await;
    match stop {
        Some(Stop::Fenced(why)) => assert!(
            why.contains("precondition") || why.contains("intent"),
            "{why}"
        ),
        other => panic!("the second instance must be fenced, got {other:?}"),
    }
    assert_eq!(
        data_keys(&rig.store),
        after_a,
        "a fenced instance writes no object: the intent is the first durable step"
    );
    assert!(
        rig.metrics.counter(M_PRECONDITION_LOST, &[]) >= 1,
        "and the loss is counted where an operator can see it"
    );

    // The owner is undisturbed.
    run_until(&mut da, 400, |d| d.engine().committed_k() >= 2).await;
    run_until_quiet(&mut da, 200, 8).await;
    assert_exactly_once(&rig.store, &expected);
    assert_no_gaps(&rig.store);
    assert_windows_tile(&manifests(&rig.store));
    assert_manifests_match_objects(&rig.store);
}

/// The window an instance loses mid-flight is never half-committed: the commit
/// batch of a fenced instance rolls back WITH its pointer write, so the pointer
/// still names the last window it legitimately committed.
#[tokio::test(start_paused = true)]
async fn a_lease_lost_during_a_window_leaves_the_pointer_where_it_was() {
    let rig = Rig::new(test_cfg());
    seed_two_hours(&rig.queen, "orders", &["a"]);
    let lease = rig.own("orders", "inst-a").await;
    let mut d = rig.restart(rig.cfg.clone(), "orders", lease.clone());
    run_until(&mut d, 400, |d| d.engine().committed_k() >= 1).await;
    let pointer = rig
        .queen
        .kv_get("s3:default:orders:committed")
        .expect("the pointer is written");

    // Somebody else takes the lease over between two windows.
    rig.queen
        .kv_seed(lease.key(), serde_json::json!({"instance":"inst-b"}));
    let stop = run_until(&mut d, 400, |_| false).await;
    assert!(
        matches!(stop, Some(Stop::Fenced(_))),
        "expected a fence, got {stop:?}"
    );
    assert_eq!(
        rig.queen.kv_get("s3:default:orders:committed"),
        Some(pointer),
        "the fenced instance's commit rolled back whole, pointer included"
    );
    assert_eq!(
        manifests(&rig.store).len(),
        1,
        "and window 2 was never committed by the loser"
    );
}

/// A corrupt intent document is not a crash and not a replay: its VERSION is
/// still what the next conditional write must expect, and the window is simply
/// chosen afresh (there is no object for it yet).
#[tokio::test(start_paused = true)]
async fn an_unreadable_pointer_is_ignored_but_its_version_is_not() {
    let rig = Rig::new(test_cfg());
    let expected = seed_two_hours(&rig.queen, "orders", &["a"]);
    rig.queen.kv_seed(
        "s3:default:orders:intent",
        serde_json::json!({"nonsense": 1}),
    );

    let mut d = rig.driver("orders").await;
    run_until(&mut d, 600, |d| d.engine().committed_k() >= 2).await;
    run_until_quiet(&mut d, 400, 8).await;
    assert_exactly_once(&rig.store, &expected);
    assert_windows_tile(&manifests(&rig.store));
}

/// `checkpoint_every` and the window numbering: a checkpoint is written for the
/// windows it names and for no others, and it is never ahead of the commit.
#[tokio::test(start_paused = true)]
async fn checkpoints_land_on_their_windows_and_never_ahead_of_the_commit() {
    let mut cfg = test_cfg();
    cfg.engine.checkpoint_every = 1;
    let rig = Rig::new(cfg);
    seed_two_hours(&rig.queen, "orders", &["a"]);
    let mut d = rig.driver("orders").await;
    run_until(&mut d, 600, |d| d.engine().committed_k() >= 2).await;
    run_until_quiet(&mut d, 400, 8).await;

    let cps = checkpoint_keys(&rig.store);
    assert_eq!(
        cps.len(),
        2,
        "one per window at checkpoint_every=1: {cps:?}"
    );
    for (i, key) in cps.iter().enumerate() {
        let cp = queen_s3::checkpoint::decode(&bytes_of(&rig.store, key)).expect("decodes");
        assert_eq!(cp.k, i as u64 + 1);
        assert!(
            cp.k <= d.engine().committed_k(),
            "a checkpoint ahead of the commit would skip records on restart"
        );
        assert!(cp.t_end > Micros::MIN);
    }
}

/// The same overrun, but across a RESTART: the position comes back from a
/// checkpoint, retention has passed it while the process was down, and the gap
/// must still be reported. Clamping the restored position up to `logStart`
/// would be the one path where this connector loses records quietly — the
/// fetch path reports the identical gap through `OFFSET_OUT_OF_RANGE`.
#[tokio::test(start_paused = true)]
async fn a_retention_overrun_seen_only_at_restart_is_still_reported() {
    let mut cfg = test_cfg();
    cfg.engine.checkpoint_every = 1;
    let rig = Rig::new(cfg);
    for p in ["a", "b"] {
        rig.queen.push(
            "orders",
            p,
            t("2026-09-04T10:05:00.000000Z"),
            &["{\"n\":1}"; 3],
        );
    }
    rig.queen.set_safe_time(t("2026-09-04T10:10:00.000000Z"));
    let lease = rig.own("orders", "inst-a").await;
    let mut first = rig.restart(rig.cfg.clone(), "orders", lease.clone());
    run_until(&mut first, 400, |d| d.engine().committed_k() >= 1).await;
    // …and one more round, so the checkpoint the commit queued is written: it is
    // the only thing that carries a position across the restart.
    run_until_quiet(&mut first, 200, 4).await;
    assert_eq!(checkpoint_keys(&rig.store).len(), 1);
    drop(first);

    // The process is DOWN. More arrives and retention eats all of it.
    for p in ["a", "b"] {
        rig.queen.push(
            "orders",
            p,
            t("2026-09-04T10:20:00.000000Z"),
            &["{\"n\":2}"; 3],
        );
    }
    rig.queen.retention_delete_below("orders", "a", 6);
    for p in ["a", "b"] {
        rig.queen.push(
            "orders",
            p,
            t("2026-09-04T10:30:00.000000Z"),
            &["{\"n\":3}"; 3],
        );
    }
    rig.queen.set_safe_time(t("2026-09-04T10:40:00.000000Z"));

    let mut second = rig.restart(rig.cfg.clone(), "orders", lease);
    run_until(&mut second, 600, |d| d.engine().committed_k() >= 2).await;
    run_until_quiet(&mut second, 400, 8).await;

    assert_eq!(
        rig.metrics.counter(M_RECORDS_LOST, &[("queue", "orders")]),
        3,
        "the gap a restart discovers is counted like any other"
    );
    let lost: Vec<_> = manifests(&rig.store)
        .into_iter()
        .flat_map(|m| m.lost)
        .collect();
    assert_eq!(lost.len(), 1, "{lost:?}");
    assert_eq!((lost[0].from, lost[0].to), (3, 5));
    assert_manifests_match_objects(&rig.store);
}
