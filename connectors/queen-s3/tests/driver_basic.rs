//! The driver end to end, with a Queen and a bucket that are not on a network:
//! windows, objects, keys, manifests, checkpoints, the two `start` modes, both
//! layouts and both formats.
//!
//! Every assertion here is one a READER could make. The point of the connector
//! is that a reader who never heard of Queen can glob a prefix and get the log
//! back, so the tests decode the objects the way DuckDB would — zstd, then
//! JSONL; or the Parquet row iterator — rather than inspecting the writer's
//! internals, which have their own tests.

use std::sync::Arc;

use queen_s3::obs::{M_RECORDS_WRITTEN, M_WINDOWS_COMMITTED};
use queen_s3::types::{Align, Format, Layout, Micros, ParquetCodec, Start};
use queen_s3::writer::WriterConfig;

#[path = "driver_support.rs"]
mod support;
use support::*;

/// The shape of the whole protocol in one test: two aligned hours in, two
/// windows out, at the keys plan §6.3 names, with the manifests that describe
/// them and the records a reader gets back.
#[tokio::test(start_paused = true)]
async fn two_aligned_hours_become_two_windows_a_reader_can_glob() {
    let rig = Rig::new(test_cfg());
    let expected = seed_two_hours(&rig.queen, "orders", &["cust-1", "cust-2", "cust-3"]);
    let mut d = rig.driver("orders").await;

    let stop = run_until(&mut d, 400, |d| d.engine().committed_k() >= 2).await;
    assert_eq!(stop, None, "nothing may stop the queue here");
    run_until_quiet(&mut d, 200, 8).await;

    // --- the objects, at the keys the plan names ---------------------------
    let t_start_1 = t("2026-09-04T10:00:00.000000Z");
    let t_end_1 = t("2026-09-04T11:00:00.000000Z");
    // The second window closes at safeTime, which the double puts one
    // microsecond past the newest segment.
    let t_end_2 = t("2026-09-04T11:20:00.000001Z");
    let keys = data_keys(&rig.store);
    assert_eq!(
        keys,
        vec![
            format!(
                "queen/queue=orders/dt=2026-09-04/hour=10/w-{:010}-{:016}-{:016}.jsonl.zst",
                1, t_start_1.0, t_end_1.0
            ),
            format!(
                "queen/queue=orders/dt=2026-09-04/hour=11/w-{:010}-{:016}-{:016}.jsonl.zst",
                2, t_end_1.0, t_end_2.0
            ),
        ],
        "one object per window, in a Hive bucket the window never straddles"
    );

    // --- the records ------------------------------------------------------
    assert_exactly_once(&rig.store, &expected);
    assert_no_gaps(&rig.store);
    let ms = manifests(&rig.store);
    assert_eq!(ms.len(), 2);
    assert_windows_tile(&ms);
    assert_manifests_match_objects(&rig.store);

    assert_eq!(ms[0].k, 1);
    assert_eq!(ms[0].t_start, t_start_1);
    assert_eq!(ms[0].t_end, t_end_1);
    assert_eq!(ms[0].records, 27, "3 partitions x 3 segments x 3 records");
    assert_eq!(ms[0].partitions, 3);
    assert_eq!(ms[0].min_ts, Some(t("2026-09-04T10:05:00.000000Z")));
    assert_eq!(ms[0].max_ts, Some(t("2026-09-04T10:45:00.000000Z")));
    assert!(ms[0].lost.is_empty());
    assert_eq!(ms[0].format, Format::Jsonl);
    assert_eq!(ms[0].layout, Layout::Merged);
    assert_eq!(ms[0].queue, "orders");
    assert_eq!(ms[0].sink, "default");
    assert!(
        ms[0].writer.starts_with("queen-s3/"),
        "the manifest says which code wrote the object: {}",
        ms[0].writer
    );
    assert_eq!(ms[1].records, 18, "3 partitions x 2 segments x 3 records");

    // --- what a reader actually sees ---------------------------------------
    let rows = rows_of(&rig.store, &keys[0]);
    assert_eq!(rows.len(), 27);
    assert_eq!(rows[0].queue, "orders");
    assert_eq!(rows[0].partition, "cust-1");
    assert_eq!(rows[0].offset, 0);
    assert_eq!(
        rows[0].transaction_id, "txn-cust-1-0",
        "the transaction id is the message's own, not a fabrication"
    );
    assert_eq!(rows[0].ts, "2026-09-04T10:05:00.000000Z");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(rows[0].payload.as_ref().unwrap()).unwrap(),
        serde_json::json!({"seg": 0, "i": 0, "p": "cust-1"}),
        "the payload is the producer's, spliced"
    );
    // Sorted by (partition, offset) — the order per-entity history is read in.
    let order: Vec<(String, i64)> = rows
        .iter()
        .map(|r| (r.partition.clone(), r.offset))
        .collect();
    let mut sorted = order.clone();
    sorted.sort();
    assert_eq!(order, sorted);

    // --- the checkpoint, every N windows ------------------------------------
    let cps = checkpoint_keys(&rig.store);
    assert_eq!(
        cps,
        vec![format!("queen/_queen/orders/checkpoint/{:010}.json.zst", 2)],
        "checkpoint_every=2 writes at window 2 and not at window 1"
    );
    let cp = queen_s3::checkpoint::decode(&bytes_of(&rig.store, &cps[0]))
        .expect("the checkpoint must decode");
    assert_eq!(cp.k, 2);
    assert_eq!(cp.t_end, t_end_2);
    assert_eq!(
        cp.positions,
        vec![
            ("cust-1".to_string(), 15),
            ("cust-2".to_string(), 15),
            ("cust-3".to_string(), 15)
        ],
        "the position map is where the NEXT window starts, sorted by name"
    );

    // --- the metrics an operator reads --------------------------------------
    let m = &rig.metrics;
    assert_eq!(m.counter(M_WINDOWS_COMMITTED, &[("queue", "orders")]), 2);
    assert_eq!(m.counter(M_RECORDS_WRITTEN, &[("queue", "orders")]), 45);
    assert!(
        m.gauge(queen_s3::obs::M_LAG_SECONDS, &[("queue", "orders")])
            .is_some(),
        "the SLO gauge is published per queue"
    );
    assert!(rig
        .metrics
        .render()
        .contains("queen_s3_windows_committed_total"));
}

/// A restart with nothing to redo picks the log up at the commit pointer and
/// ships the rest — the boring half of the crash matrix, and the one that
/// proves `restore` reads what `commit` wrote.
#[tokio::test(start_paused = true)]
async fn a_clean_restart_resumes_at_the_commit_pointer() {
    let rig = Rig::new(test_cfg());
    let expected = seed_two_hours(&rig.queen, "orders", &["a", "b"]);
    let lease = rig.own("orders", "test-a").await;

    let mut first = rig.restart(rig.cfg.clone(), "orders", lease.clone());
    run_until(&mut first, 400, |d| d.engine().committed_k() >= 1).await;
    assert_eq!(first.engine().committed_k(), 1);
    drop(first);

    let mut second = rig.restart(rig.cfg.clone(), "orders", lease);
    run_until_quiet(&mut second, 400, 8).await;
    assert_eq!(
        second.engine().committed_k(),
        2,
        "the second process continues the numbering rather than starting over"
    );
    assert_exactly_once(&rig.store, &expected);
    assert_windows_tile(&manifests(&rig.store));
    assert_manifests_match_objects(&rig.store);
}

/// `start=latest`: a queue with a backlog and no committed pointer ships
/// NOTHING from before `T_0`, and everything after it (plan §4.1, D6).
#[tokio::test(start_paused = true)]
async fn start_latest_skips_the_backlog_and_earliest_takes_it() {
    // --- latest ------------------------------------------------------------
    let mut cfg = test_cfg();
    cfg.engine.start = Start::Latest;
    let rig = Rig::new(cfg);
    for stamp in [
        "2026-09-04T10:05:00.000000Z",
        "2026-09-04T10:25:00.000000Z",
        "2026-09-04T10:45:00.000000Z",
    ] {
        for p in ["a", "b"] {
            rig.queen
                .push("orders", p, t(stamp), &["{\"old\":true}"; 3]);
        }
    }
    // T_0 is the first safeTime the sink sees.
    rig.queen.set_safe_time(t("2026-09-04T11:00:00.000000Z"));
    let mut d = rig.driver("orders").await;
    run_until(&mut d, 50, |d| d.engine().t_prev().is_some()).await;
    assert_eq!(
        d.engine().t_prev(),
        Some(t("2026-09-04T11:00:00.000000Z")),
        "T_0 is the first safeTime, not the sink's clock"
    );

    let mut expected = Vec::new();
    for p in ["a", "b"] {
        let base = rig.queen.push(
            "orders",
            p,
            t("2026-09-04T11:10:00.000000Z"),
            &["{\"new\":true}"; 3],
        );
        for i in 0..3 {
            expected.push((p.to_string(), base + i));
        }
    }
    rig.queen.set_safe_time(t("2026-09-04T11:30:00.000000Z"));
    run_until(&mut d, 400, |d| d.engine().committed_k() >= 1).await;
    run_until_quiet(&mut d, 200, 8).await;

    assert_exactly_once(&rig.store, &expected);
    let rows = all_rows(&rig.store);
    assert_eq!(rows.len(), 6, "only what was written after T_0");
    assert!(
        rows.iter().all(|r| r.ts == "2026-09-04T11:10:00.000000Z"),
        "nothing from before T_0 may appear: {rows:?}"
    );

    // --- earliest ----------------------------------------------------------
    let rig = Rig::new(test_cfg());
    let mut all = Vec::new();
    for stamp in [
        "2026-09-04T10:05:00.000000Z",
        "2026-09-04T10:25:00.000000Z",
        "2026-09-04T10:45:00.000000Z",
    ] {
        for p in ["a", "b"] {
            let base = rig
                .queen
                .push("orders", p, t(stamp), &["{\"old\":true}"; 3]);
            for i in 0..3 {
                all.push((p.to_string(), base + i));
            }
        }
    }
    let mut d = rig.driver("orders").await;
    run_until(&mut d, 400, |d| d.engine().committed_k() >= 1).await;
    run_until_quiet(&mut d, 200, 8).await;
    assert_exactly_once(&rig.store, &all);
    assert_eq!(all_rows(&rig.store).len(), 18, "everything retention holds");
}

/// `layout=per-partition` (plan §2): the Connect-shaped key, one object per
/// (window, partition), with the offset range in the name.
#[tokio::test(start_paused = true)]
async fn per_partition_layout_writes_one_object_per_partition() {
    let mut cfg = test_cfg();
    cfg.layout = Layout::PerPartition;
    let rig = Rig::new(cfg);

    let names: Vec<String> = (0..16).map(|i| i.to_string()).collect();
    let mut expected = Vec::new();
    for p in &names {
        for (s, stamp) in ["2026-09-04T10:05:00.000000Z", "2026-09-04T10:25:00.000000Z"]
            .iter()
            .enumerate()
        {
            let payloads: Vec<String> =
                (0..2).map(|i| format!("{{\"s\":{s},\"i\":{i}}}")).collect();
            let refs: Vec<&str> = payloads.iter().map(String::as_str).collect();
            let base = rig.queen.push("orders", p, t(stamp), &refs);
            for i in 0..refs.len() as i64 {
                expected.push((p.clone(), base + i));
            }
        }
    }

    let mut d = rig.driver("orders").await;
    run_until(&mut d, 400, |d| d.engine().committed_k() >= 1).await;
    run_until_quiet(&mut d, 200, 8).await;

    let keys = data_keys(&rig.store);
    assert_eq!(keys.len(), 16, "one object per partition per window");
    for p in &names {
        let wanted = format!("-p-{p}-{:012}-{:012}.jsonl.zst", 0, 3);
        assert!(
            keys.iter().any(|k| k.ends_with(&wanted)),
            "no key ends with {wanted}: {keys:?}"
        );
    }
    assert_exactly_once(&rig.store, &expected);
    assert_manifests_match_objects(&rig.store);
    let ms = manifests(&rig.store);
    assert_eq!(ms.len(), 1, "one manifest per WINDOW, not per object");
    assert_eq!(ms[0].objects.len(), 16);
    for obj in &ms[0].objects {
        assert!(
            obj.partition.is_some(),
            "a per-partition object names its lane"
        );
        assert_eq!(obj.first_offset, Some(0));
        assert_eq!(obj.last_offset, Some(3));
        assert_eq!(obj.records, 4);
    }
}

/// Parquet, end to end and read back with the crate's own row reader — the
/// same envelope plan §6.4 pins, arriving through the whole driver rather than
/// through the writer's unit test.
#[tokio::test(start_paused = true)]
async fn parquet_objects_decode_to_the_records_that_were_pushed() {
    let rig = Rig::with_writer(
        test_cfg(),
        WriterConfig {
            format: Format::Parquet,
            parquet_codec: ParquetCodec::Zstd,
            parquet_row_group_records: 8,
            ..WriterConfig::default()
        },
    );
    let expected = seed_two_hours(&rig.queen, "events", &["p-1", "p-2"]);
    let mut d = rig.driver("events").await;
    run_until(&mut d, 400, |d| d.engine().committed_k() >= 2).await;
    run_until_quiet(&mut d, 200, 8).await;

    let keys = data_keys(&rig.store);
    assert_eq!(keys.len(), 2);
    assert!(
        keys.iter().all(|k| k.ends_with(".parquet")),
        "the extension is the reader's contract: {keys:?}"
    );
    assert_exactly_once(&rig.store, &expected);
    assert_manifests_match_objects(&rig.store);

    let rows = rows_of(&rig.store, &keys[0]);
    assert_eq!(rows[0].queue, "events");
    assert_eq!(rows[0].partition, "p-1");
    assert_eq!(rows[0].offset, 0);
    assert_eq!(rows[0].transaction_id, "txn-p-1-0");
    assert_eq!(
        rows[0].ts, "2026-09-04T10:05:00.000000Z",
        "TIMESTAMP(MICROS, UTC) survives the round trip"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(rows[0].payload.as_ref().unwrap()).unwrap(),
        serde_json::json!({"seg": 0, "i": 0, "p": "p-1"})
    );
    let ms = manifests(&rig.store);
    assert_eq!(ms[0].format, Format::Parquet);
    assert!(
        ms[0].writer.contains("parquet/"),
        "the manifest records the writer version: {}",
        ms[0].writer
    );
}

/// `align=day` puts the whole day in one bucket and gives the window a `dt=`
/// with no `hour=`; the boundary a window may not straddle moves with it.
#[tokio::test(start_paused = true)]
async fn day_alignment_moves_the_bucket_and_the_boundary() {
    let rig = Rig::new(with_align(test_cfg(), Align::Day));
    let expected = seed_two_hours(&rig.queen, "orders", &["a"]);
    let mut d = rig.driver("orders").await;
    run_until(&mut d, 400, |d| d.engine().committed_k() >= 1).await;
    run_until_quiet(&mut d, 200, 8).await;

    let keys = data_keys(&rig.store);
    assert_eq!(
        keys.len(),
        1,
        "one day, one window: the hours do not split it"
    );
    assert!(
        keys[0].starts_with("queen/queue=orders/dt=2026-09-04/w-"),
        "no hour= component under align=day: {}",
        keys[0]
    );
    assert_exactly_once(&rig.store, &expected);
    let ms = manifests(&rig.store);
    assert_eq!(ms[0].t_start, t("2026-09-04T00:00:00.000000Z"));
}

/// The queue does not exist: terminal, named, and not retried
/// (032_log_fetch.sql:50-57).
#[tokio::test(start_paused = true)]
async fn an_unknown_queue_stops_the_task_with_a_reason() {
    let rig = Rig::new(test_cfg());
    let mut d = rig.driver("nope").await;
    let stop = run_until(&mut d, 50, |_| false).await;
    match stop {
        Some(queen_s3::driver::Stop::Failed(why)) => {
            assert!(why.contains("UNKNOWN_TOPIC_OR_PARTITION"), "{why}")
        }
        other => panic!("expected a terminal failure, got {other:?}"),
    }
    assert!(data_keys(&rig.store).is_empty(), "and it wrote nothing");
}

/// A transport failure is a lag, never a loss (plan §6.7): the same fetch and
/// the same KV batch are retried, and the window still lands complete.
#[tokio::test(start_paused = true)]
async fn transport_failures_only_cost_time() {
    let rig = Rig::new(test_cfg());
    let expected = seed_two_hours(&rig.queen, "orders", &["a", "b"]);
    let mut d = rig.driver("orders").await;

    rig.queen.fail_next(3);
    rig.queen.fail_kv_next(2);
    rig.store.fail_next(2);

    run_until(&mut d, 600, |d| d.engine().committed_k() >= 2).await;
    run_until_quiet(&mut d, 200, 8).await;
    assert_exactly_once(&rig.store, &expected);
    assert_no_gaps(&rig.store);
    assert_manifests_match_objects(&rig.store);
    assert_windows_tile(&manifests(&rig.store));
}

/// The global memory budget at cardinality: a thousand partitions, a budget a
/// fraction of what they hold, and a configuration in which NOTHING ELSE can
/// close a window — no alignment, a day-long `max_window`, a 128 MiB size
/// trigger. The second window exists only because the budget forced it
/// (plan §4.4, §6.2), which the control run at the end makes explicit.
#[tokio::test(start_paused = true)]
async fn the_global_memory_budget_forces_early_closes_at_scale() {
    fn cfg() -> queen_s3::driver::DriverConfig {
        let mut cfg = with_align(test_cfg(), Align::None);
        cfg.engine.max_window = Micros::DAY;
        cfg
    }
    fn seed(rig: &Rig) -> Vec<(String, i64)> {
        let base = t("2026-09-04T10:00:00.000000Z");
        let mut expected = Vec::new();
        for p in 0..1_000i64 {
            let name = format!("cust-{p:04}");
            // A distinct timestamp per partition, so a forced close has
            // somewhere legal to land: a window that could only cut inside one
            // timestamp could not shrink at all.
            let ts = Micros(base.0 + p * 1_000_000);
            let offset = rig
                .queen
                .push("orders", &name, ts, &["{\"x\":1}", "{\"x\":2}"]);
            expected.push((name.clone(), offset));
            expected.push((name, offset + 1));
        }
        expected
    }
    let half = t("2026-09-04T10:08:20.000000Z"); // base + 500 s
    let past_the_end = t("2026-09-04T10:16:41.000000Z"); // base + 1001 s

    // --- with the budget ----------------------------------------------------
    let rig = Rig::new(cfg()).with_budget(16 * 1024);
    let expected = seed(&rig);
    rig.queen.set_safe_time(half);
    let mut d = rig.driver("orders").await;
    run_until(&mut d, 4_000, |d| d.engine().committed_k() >= 1).await;
    rig.queen.set_safe_time(past_the_end);
    run_until(&mut d, 4_000, |d| d.engine().committed_k() >= 2).await;
    run_until_quiet(&mut d, 4_000, 8).await;

    let ms = manifests(&rig.store);
    assert!(
        ms.len() >= 2,
        "a 16 KiB budget over ~200 KiB of buffered records must close early, got {} window(s)",
        ms.len()
    );
    assert_windows_tile(&ms);
    assert_exactly_once(&rig.store, &expected);
    assert_no_gaps(&rig.store);
    assert_manifests_match_objects(&rig.store);
    assert_eq!(
        rig.metrics
            .counter(M_RECORDS_WRITTEN, &[("queue", "orders")]),
        2_000
    );

    // --- the control: same log, same clock, a budget nobody hits ------------
    let control = Rig::new(cfg()).with_budget(1024 * 1024 * 1024);
    seed(&control);
    control.queen.set_safe_time(half);
    let mut c = control.driver("orders").await;
    run_until(&mut c, 4_000, |d| d.engine().committed_k() >= 1).await;
    control.queen.set_safe_time(past_the_end);
    run_until_quiet(&mut c, 4_000, 20).await;
    assert_eq!(
        c.engine().committed_k(),
        1,
        "without the budget nothing closes the second window: no boundary, no age, no size"
    );
    assert!(
        all_rows(&control.store).len() < 2_000,
        "and the rest is still buffered, which is exactly what the budget is for"
    );
}

/// The budget is a process-wide sum, and the queue holding the most is the one
/// that closes — asserted directly, because at the driver level it is only
/// visible as "some window closed early".
#[test]
fn the_budget_picks_the_largest_queue() {
    let b = queen_s3::driver::MemoryBudget::new(100);
    assert!(!b.report("small", 10));
    assert!(!b.report("large", 80));
    assert!(b.report("large", 95), "over budget: the largest closes");
    assert!(!b.report("small", 10), "and nobody else does");
}

/// The lease is the fence and it is also the ownership: a driver whose lease is
/// gone stops before it can write anything (plan §6.6).
#[tokio::test(start_paused = true)]
async fn a_driver_whose_lease_was_taken_stops() {
    let rig = Rig::new(test_cfg());
    seed_two_hours(&rig.queen, "orders", &["a"]);
    let lease = rig.own("orders", "test-a").await;
    let mut d = rig.restart(rig.cfg.clone(), "orders", lease.clone());
    // One tick of honest work first, so the stop is not simply "it never ran".
    run_until(&mut d, 20, |d| d.engine().safe_time().is_some()).await;
    lease.mark_lost();
    let stop = run_until(&mut d, 20, |_| false).await;
    assert!(
        matches!(stop, Some(queen_s3::driver::Stop::Fenced(_))),
        "expected a fenced stop, got {stop:?}"
    );
    assert!(data_keys(&rig.store).is_empty());
    // The health probe must forget a queue this process no longer owns.
    let _ = Arc::strong_count(&rig.health);
}
