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
//! run the whole matrix; an end-to-end suite runs the same five points against
//! a real broker that really dies, and a restart that may be another node.

use std::sync::Arc;

use queen_s3::config::CrashAt;
use queen_s3::driver::Stop;
use queen_s3::lease::{spawn_refresh, Acquired, Lease};
use queen_s3::obs::{M_PRECONDITION_LOST, M_RECORDS_LOST, M_WINDOWS_COMMITTED};
use queen_s3::types::{Format, Layout, Micros, ParquetCodec};
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

/// Two nodes, one queue (plan §6.6): exactly one owns it, and the other's
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

    // The second node runs anyway — its lease handle believes it holds the
    // queue — and gets exactly as far as its first conditional write.
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

/// The stamps `seed_two_hours` pushes, in its order: what a node that has
/// applied only part of the log holds is a prefix of them.
const STAMPS: [&str; 5] = [
    "2026-09-04T10:05:00.000000Z",
    "2026-09-04T10:15:00.000000Z",
    "2026-09-04T10:45:00.000000Z",
    "2026-09-04T11:05:00.000000Z",
    "2026-09-04T11:20:00.000000Z",
];

/// Push stamps `which` of `seed_two_hours`, byte for byte: same payloads, same
/// order, so the same offsets and transaction ids.
fn push_stamps(queen: &queen_s3::queen::FakeQueen, parts: &[&str], which: std::ops::Range<usize>) {
    for s in which {
        for p in parts {
            let payloads: Vec<String> = (0..3)
                .map(|i| format!("{{\"seg\":{s},\"i\":{i},\"p\":\"{p}\"}}"))
                .collect();
            let refs: Vec<&str> = payloads.iter().map(String::as_str).collect();
            queen.push("orders", p, t(STAMPS[s]), &refs);
        }
    }
}

/// A queue changes hands while a window is in flight, and the redo runs on a
/// node whose applied log does not yet hold the window: its `safeTime` is below
/// the window's end. Through the broker's adapter this does not arise — the KV
/// read that brings the intent waits for the read index, which is past the
/// window's records — but the engine must not depend on how the intent reached
/// it: the redo waits for this node to apply the window rather than rebuild it
/// from what the node happens to hold. Before the fix, with no partition of the
/// queue applied yet, it uploaded an empty object over the window's key and
/// committed it.
#[tokio::test(start_paused = true)]
async fn a_redo_on_a_node_that_is_behind_waits_and_then_rebuilds_the_whole_window() {
    let parts = ["a", "b"];
    let (reference, _) = control(&parts, Layout::Merged, WriterConfig::default()).await;
    let want: Vec<(String, String)> = fingerprint(&reference.store)
        .into_iter()
        .filter(|(k, _)| k.contains("/w-0000000001-"))
        .collect();
    assert_eq!(want.len(), 1, "the control's window 1");

    // Node 1 writes the intent of window 1 and dies.
    let node1 = Rig::new(test_cfg());
    seed_two_hours(&node1.queen, "orders", &parts);
    let lease = node1.own("orders", "node-1").await;
    let mut crashing = node1.cfg.clone();
    crashing.crash_at = CrashAt::AfterIntent;
    let mut first = node1.restart(crashing, "orders", lease);
    assert_eq!(
        run_until(&mut first, 600, |_| false).await,
        Some(Stop::Crashed(CrashAt::AfterIntent))
    );
    let intent = node1
        .queen
        .kv_get("s3:default:orders:intent")
        .expect("the intent is durable");
    let t_end = Micros(intent["tEnd"].as_i64().expect("tEnd"));
    assert_eq!(t_end, t("2026-09-04T11:00:00.000000Z"));

    // Node 2 owns the queue next. It has applied the intent (a KV entry) but
    // none of the window's records: the queue exists, nothing in it yet, and
    // its safeTime is below the window's end.
    let node2 = Rig::new(test_cfg());
    node2.queen.create_queue("orders");
    node2
        .queen
        .kv_seed("s3:default:orders:intent", intent.clone());
    node2.queen.set_safe_time(t("2026-09-04T10:00:00.000000Z"));
    let mut second = node2.driver("orders").await;
    for _ in 0..40 {
        assert_eq!(second.tick().await, None);
    }
    assert!(second.engine().redoing());
    assert!(
        data_keys(&node2.store).is_empty(),
        "nothing may be uploaded over window 1 by a node that has not applied it"
    );

    // Part of the window arrives; the clock is still below the end.
    push_stamps(&node2.queen, &parts, 0..2);
    node2.queen.set_safe_time(t("2026-09-04T10:20:00.000000Z"));
    for _ in 0..40 {
        assert_eq!(second.tick().await, None);
    }
    assert!(data_keys(&node2.store).is_empty());
    assert_eq!(second.engine().committed_k(), 0);

    // The rest is applied, and this node's safeTime passes the end: the
    // rebuilt window is the whole window, byte for byte.
    push_stamps(&node2.queen, &parts, 2..5);
    node2.queen.clear_safe_time();
    run_until(&mut second, 600, |d| d.engine().committed_k() >= 1).await;
    let got: Vec<(String, String)> = fingerprint(&node2.store)
        .into_iter()
        .filter(|(k, _)| k.contains("/w-0000000001-"))
        .collect();
    assert_eq!(
        got, want,
        "the redo on a node that was behind is the window the old owner would have written"
    );
    assert_manifests_match_objects(&node2.store);
}

/// The broker read `a`'s bounds, then retention trimmed its head before the
/// broker read the log: the fetch answer starts above the offset the sink asked
/// for, with the old `logStart`. The offsets it stepped over are gone, and they
/// are counted and named exactly like an `OFFSET_OUT_OF_RANGE` gap.
#[tokio::test(start_paused = true)]
async fn an_offset_jump_in_a_fetch_answer_is_counted_and_named() {
    let rig = Rig::new(test_cfg());
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

    for p in ["a", "b"] {
        rig.queen.push(
            "orders",
            p,
            t("2026-09-04T10:20:00.000000Z"),
            &["{\"n\":2}"; 3],
        );
    }
    // The sink's next read of `a` starts at 3; retention takes 3..=4 between
    // the broker's two reads.
    rig.queen.trim_during_next_fetch("orders", "a", 5);
    rig.queen.set_safe_time(t("2026-09-04T10:40:00.000000Z"));
    run_until(&mut d, 400, |d| d.engine().committed_k() >= 2).await;
    run_until_quiet(&mut d, 200, 8).await;

    assert_eq!(
        rig.metrics.counter(M_RECORDS_LOST, &[("queue", "orders")]),
        2,
        "the offsets the broker stepped over"
    );
    let ms = manifests(&rig.store);
    let lost: Vec<_> = ms.iter().flat_map(|m| m.lost.iter()).collect();
    assert_eq!(lost.len(), 1, "{lost:?}");
    assert_eq!(lost[0].partition, "a");
    assert_eq!((lost[0].from, lost[0].to), (3, 4));

    let mut expected: Vec<(String, i64)> = Vec::new();
    for o in [0, 1, 2, 5] {
        expected.push(("a".to_string(), o));
    }
    for o in 0..6 {
        expected.push(("b".to_string(), o));
    }
    assert_exactly_once(&rig.store, &expected);
    assert_windows_tile(&ms);
    assert_manifests_match_objects(&rig.store);
}

/// Kill a driver at `at` over the two-hour log, and hand back the rig, the
/// lease and the records the lake must end up with: the first half of a
/// matrix case, for the tests that look at the restart more closely.
async fn crashed_at(at: CrashAt) -> (Rig, Arc<Lease>, Vec<(String, i64)>) {
    crashed_at_with(at, WriterConfig::default()).await
}

/// [`crashed_at`] with a writer of the test's choosing.
async fn crashed_at_with(at: CrashAt, wcfg: WriterConfig) -> (Rig, Arc<Lease>, Vec<(String, i64)>) {
    let cfg = test_cfg();
    let rig = Rig::with_writer(cfg.clone(), wcfg);
    let expected = seed_two_hours(&rig.queen, "orders", &["a", "b"]);
    let lease = rig.own("orders", "inst-a").await;
    let mut crashing = cfg;
    crashing.crash_at = at;
    let mut first = rig.restart(crashing, "orders", lease.clone());
    assert_eq!(
        run_until(&mut first, 600, |_| false).await,
        Some(Stop::Crashed(at))
    );
    drop(first);
    (rig, lease, expected)
}

const MANIFEST_1: &str =
    "queen/_queen/tenant=00000000-0000-0000-0000-000000000001/queue=orders/windows/0000000001.json";

/// The previous attempt uploaded window 1 whole — objects, then the manifest —
/// and died before its commit. The restart must not rebuild the window, and
/// must not upload a byte: the manifest names exactly the intent, so the
/// commit is all that is left, and the pointer takes the manifest's counts.
#[tokio::test(start_paused = true)]
async fn a_redo_whose_upload_finished_commits_from_its_manifest_and_writes_nothing() {
    let (reference, _) = control(&["a", "b"], Layout::Merged, WriterConfig::default()).await;
    for at in [CrashAt::AfterUpload, CrashAt::BeforeCommit] {
        let (rig, lease, expected) = crashed_at(at).await;
        let manifest: queen_s3::types::Manifest =
            serde_json::from_slice(&bytes_of(&rig.store, MANIFEST_1)).unwrap();
        assert!(rig.queen.kv_get("s3:default:orders:committed").is_none());
        let puts = rig.store.puts().len();
        let fetches = rig.queen.fetch_calls();

        let mut second = rig.restart(rig.cfg.clone(), "orders", lease);
        assert_eq!(second.tick().await, None);
        assert_eq!(
            second.engine().committed_k(),
            1,
            "{}: committed in the first round",
            at.as_str()
        );
        assert_eq!(
            rig.store.puts().len(),
            puts,
            "{}: nothing uploaded again",
            at.as_str()
        );
        assert_eq!(
            rig.queen.fetch_calls(),
            fetches,
            "{}: and nothing read again — the check comes before the rebuild",
            at.as_str()
        );
        let pointer = rig
            .queen
            .kv_get("s3:default:orders:committed")
            .expect("the commit pointer moved");
        assert_eq!(pointer["k"], 1);
        assert_eq!(pointer["manifest"], MANIFEST_1);
        assert_eq!(pointer["records"], manifest.records);
        assert_eq!(pointer["bytes"], manifest.bytes);
        assert_eq!(pointer["tEnd"], manifest.t_end.to_iso());
        assert_eq!(
            rig.metrics
                .counter(queen_s3::obs::M_RECORDS_WRITTEN, &[("queue", "orders")]),
            manifest.records
        );

        // ...and the queue carries on to the same lake a clean run writes.
        run_until(&mut second, 600, |d| d.engine().committed_k() >= 2).await;
        run_until_quiet(&mut second, 400, 8).await;
        assert_eq!(
            fingerprint(&rig.store),
            fingerprint(&reference.store),
            "{}",
            at.as_str()
        );
        assert_exactly_once(&rig.store, &expected);
        assert_windows_tile(&manifests(&rig.store));
        assert_manifests_match_objects(&rig.store);
    }
}

/// No manifest: the previous attempt died before its upload finished (here,
/// between its data object and its manifest). The window is rebuilt and
/// uploaded, as it always was.
#[tokio::test(start_paused = true)]
async fn a_redo_with_no_manifest_is_rebuilt_and_uploaded() {
    let (reference, _) = control(&["a", "b"], Layout::Merged, WriterConfig::default()).await;
    let (rig, lease, expected) = crashed_at(CrashAt::MidUpload).await;
    assert!(rig.store.bytes_of(MANIFEST_1).is_none());
    let puts = rig.store.puts().len();

    let mut second = rig.restart(rig.cfg.clone(), "orders", lease);
    run_until(&mut second, 600, |d| d.engine().committed_k() >= 1).await;
    let again: Vec<String> = rig.store.puts()[puts..]
        .iter()
        .map(|p| p.key.clone())
        .collect();
    assert!(
        again.iter().any(|k| k.contains("/w-0000000001-")),
        "the data object is uploaded again: {again:?}"
    );
    assert!(
        again.iter().any(|k| k == MANIFEST_1),
        "and the manifest: {again:?}"
    );
    run_until_quiet(&mut second, 400, 8).await;
    assert_eq!(fingerprint(&rig.store), fingerprint(&reference.store));
    assert_exactly_once(&rig.store, &expected);
}

/// A manifest under window 1's key that does not name this intent — other
/// bounds, or another writer — is not this window's upload. The window is
/// rebuilt and uploaded, and the manifest replaced by the one that matches.
#[tokio::test(start_paused = true)]
async fn a_manifest_of_another_window_or_writer_is_not_taken_for_the_redo() {
    let (reference, _) = control(&["a", "b"], Layout::Merged, WriterConfig::default()).await;
    type Tamper = fn(&mut queen_s3::types::Manifest);
    let tampers: [(&str, Tamper); 2] = [
        ("other bounds", |m| m.t_end = Micros(m.t_end.0 - 1)),
        ("another writer", |m| {
            m.writer = "queen-s3/0.0.0 jsonl+zstd".into()
        }),
    ];
    for (what, tamper) in tampers {
        let (rig, lease, expected) = crashed_at(CrashAt::BeforeCommit).await;
        let mut foreign: queen_s3::types::Manifest =
            serde_json::from_slice(&bytes_of(&rig.store, MANIFEST_1)).unwrap();
        tamper(&mut foreign);
        queen_s3::s3::ObjectStore::put(
            &*rig.store,
            MANIFEST_1,
            bytes::Bytes::from(serde_json::to_vec(&foreign).unwrap()),
            "application/json",
        )
        .await
        .unwrap();
        let puts = rig.store.puts().len();

        let mut second = rig.restart(rig.cfg.clone(), "orders", lease);
        run_until(&mut second, 600, |d| d.engine().committed_k() >= 1).await;
        let again: Vec<String> = rig.store.puts()[puts..]
            .iter()
            .map(|p| p.key.clone())
            .collect();
        assert!(
            again.iter().any(|k| k.contains("/w-0000000001-")),
            "{what}: the window is rebuilt and uploaded: {again:?}"
        );
        let now: queen_s3::types::Manifest =
            serde_json::from_slice(&bytes_of(&rig.store, MANIFEST_1)).unwrap();
        assert_ne!(now, foreign, "{what}: the manifest is the redo's now");
        run_until_quiet(&mut second, 400, 8).await;
        assert_eq!(
            fingerprint(&rig.store),
            fingerprint(&reference.store),
            "{what}"
        );
        assert_exactly_once(&rig.store, &expected);
        assert_manifests_match_objects(&rig.store);
    }
}

/// No manifest when the redo starts, but one lands while it is reading — the
/// previous owner finishing its upload late. The redo looks again right
/// before its first PUT, and commits from that manifest instead of writing
/// its rebuild over the finished upload.
#[tokio::test(start_paused = true)]
async fn a_manifest_that_lands_while_the_redo_reads_is_taken_before_any_put() {
    let (reference, _) = control(&["a", "b"], Layout::Merged, WriterConfig::default()).await;
    let (rig, lease, expected) = crashed_at(CrashAt::MidUpload).await;
    assert!(rig.store.bytes_of(MANIFEST_1).is_none());

    let mut second = rig.restart(rig.cfg.clone(), "orders", lease);
    assert_eq!(second.tick().await, None);
    assert!(
        second.engine().redoing(),
        "nothing in the bucket yet: a rebuild"
    );

    // The late upload: window 1's object, then its manifest, exactly as the
    // clean run wrote them.
    let window_1: Vec<String> = data_keys(&reference.store)
        .into_iter()
        .filter(|k| k.contains("/w-0000000001-"))
        .chain(std::iter::once(MANIFEST_1.to_string()))
        .collect();
    for key in &window_1 {
        queen_s3::s3::ObjectStore::put(
            &*rig.store,
            key,
            bytes_of(&reference.store, key),
            "application/octet-stream",
        )
        .await
        .unwrap();
    }
    let puts = rig.store.puts().len();

    run_until(&mut second, 600, |d| d.engine().committed_k() >= 1).await;
    let again: Vec<String> = rig.store.puts()[puts..]
        .iter()
        .map(|p| p.key.clone())
        .collect();
    assert!(
        !again.iter().any(|k| window_1.contains(k)),
        "the finished upload is not overwritten: {again:?}"
    );
    let manifest: queen_s3::types::Manifest =
        serde_json::from_slice(&bytes_of(&rig.store, MANIFEST_1)).unwrap();
    let pointer = rig.queen.kv_get("s3:default:orders:committed").unwrap();
    assert_eq!(pointer["records"], manifest.records);

    run_until_quiet(&mut second, 400, 8).await;
    assert_eq!(fingerprint(&rig.store), fingerprint(&reference.store));
    assert_exactly_once(&rig.store, &expected);
}

/// A Parquet+Snappy window records its effective codec: the intent and the
/// manifest both say `snappy` (before 2.0.0 they reported the JSONL knob,
/// `none`), and both take it from the same writer, so the redo's manifest
/// match compares like with like — a finished Snappy upload is committed from
/// its manifest, not uploaded again.
#[tokio::test(start_paused = true)]
async fn a_snappy_parquet_window_records_its_codec_and_its_redo_still_matches() {
    let wcfg = WriterConfig {
        format: Format::Parquet,
        parquet_codec: ParquetCodec::Snappy,
        parquet_row_group_records: 8,
        ..WriterConfig::default()
    };
    let (rig, lease, expected) = crashed_at_with(CrashAt::BeforeCommit, wcfg).await;
    let manifest: serde_json::Value =
        serde_json::from_slice(&bytes_of(&rig.store, MANIFEST_1)).unwrap();
    assert_eq!(manifest["format"], "parquet");
    assert_eq!(manifest["compression"], "snappy");
    let intent = rig
        .queen
        .kv_get("s3:default:orders:intent")
        .expect("the intent was written before the upload");
    assert_eq!(intent["k"], 1);
    assert_eq!(intent["compression"], "snappy");
    assert_eq!(intent["writer"], manifest["writer"]);
    assert!(
        manifest["writer"].as_str().unwrap().ends_with(" snappy"),
        "{}",
        manifest["writer"]
    );
    let puts = rig.store.puts().len();

    let mut second = rig.restart(rig.cfg.clone(), "orders", lease);
    assert_eq!(second.tick().await, None);
    assert_eq!(
        second.engine().committed_k(),
        1,
        "the manifest matched its intent: committed in the first round"
    );
    assert_eq!(rig.store.puts().len(), puts, "nothing uploaded again");

    run_until(&mut second, 600, |d| d.engine().committed_k() >= 2).await;
    run_until_quiet(&mut second, 400, 8).await;
    for m in manifests(&rig.store) {
        assert_eq!(
            m.compression,
            queen_s3::types::Compression::Snappy,
            "window {}",
            m.k
        );
    }
    assert_exactly_once(&rig.store, &expected);
    assert_windows_tile(&manifests(&rig.store));
    assert_manifests_match_objects(&rig.store);
}

/// The refresh task and the intent and commit batches all write the lease
/// row, each expecting the version this node last saw. Serialised per lease
/// handle they never fence one another: a one-second TTL refreshes every
/// 333 ms, every KV batch takes 40 ms to apply — so refreshes land while
/// batches are in flight — and twenty windows commit with no fence and no lost
/// precondition. A REAL other owner still fences the next batch.
#[tokio::test(start_paused = true)]
async fn the_refresh_never_fences_its_own_node_and_another_owner_still_does() {
    let rig = Rig::new(test_cfg());
    // The TTL clock stands still: the row must survive on refreshes, not
    // expire between them.
    rig.queen.set_now_ms(1_000_000);
    for h in 0..20 {
        rig.queen.push(
            "orders",
            "a",
            t(&format!("2026-09-04T{h:02}:05:00.000000Z")),
            &["{\"h\":1}"],
        );
    }
    rig.queen
        .set_kv_latency(std::time::Duration::from_millis(40));
    let lease = Arc::new(Lease::new(
        rig.queen.clone(),
        "default",
        "orders",
        "node-1",
        1_000,
    ));
    assert_eq!(lease.acquire().await.unwrap(), Acquired::Taken);
    let refresher = spawn_refresh(lease.clone());
    let mut d = rig.restart(rig.cfg.clone(), "orders", lease.clone());

    let stop = run_until(&mut d, 2_000, |d| d.engine().committed_k() >= 20).await;
    assert_eq!(
        stop, None,
        "a node that owns its queue is never fenced by itself"
    );
    assert_eq!(rig.metrics.counter(M_PRECONDITION_LOST, &[]), 0);
    assert!(!lease.lost());
    let refreshes = rig
        .queen
        .kv_batches()
        .iter()
        .filter(|b| {
            b.len() == 1
                && b[0].key() == Some(lease.key())
                && matches!(b[0], queen_s3::queen::KvOp::Put { .. })
        })
        .count();
    assert!(
        refreshes >= 3,
        "the refresh ran alongside the windows: {refreshes}"
    );

    // Another owner takes the row. With the refresher gone, it is the next
    // fenced batch — the intent of the next window — that finds out.
    drop(refresher);
    rig.queen.kv_seed(
        lease.key(),
        serde_json::json!({"instance": "node-2", "incarnation": "x", "sinceMs": 0}),
    );
    rig.queen.push(
        "orders",
        "a",
        t("2026-09-04T21:05:00.000000Z"),
        &["{\"h\":2}"],
    );
    let stop = run_until(&mut d, 400, |_| false).await;
    assert!(matches!(stop, Some(Stop::Fenced(_))), "{stop:?}");
    assert!(lease.lost());
    assert_eq!(rig.metrics.counter(M_PRECONDITION_LOST, &[]), 1);
}
