//! Streams end to end.
//!
//! Ports the JS `stream/` and `streams-unit/` suites plus the Go and Python
//! `streams_integration` ones: windows of every kind, event time and
//! watermarks, gates, sinks, state isolation and recovery.
//!
//! These are wall-clock tests. Windows are kept to a couple of seconds and the
//! assertions are on *totals* rather than on how many windows a batch happened
//! to split into, so they do not become flaky when a cycle lands either side of
//! a boundary.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use queen_mq::streams::{Every, LatePolicy, RunOptions, Stream};
use queen_mq::QueueOptions;

use common::*;

/// Push `n` messages one per `gap`, so events actually span several windows.
async fn push_spread(queen: &queen_mq::Queen, queue: &str, partition: &str, n: usize, gap: Duration) {
    for i in 0..n {
        queen
            .queue(queue)
            .partition(partition)
            .push(serde_json::json!({ "amount": i + 1, "i": i }))
            .await
            .unwrap();
        tokio::time::sleep(gap).await;
    }
}

// ============================================================================
// Registration
// ============================================================================

#[tokio::test]
async fn a_stream_registers_and_reports_its_query_id() {
    let q = broker!();
    let src = unique("st-register");
    create_queue(&q, &src, QueueOptions::default()).await;

    let handle = Stream::from(q.queue(&src))
        .map(|r| r.data.clone())
        .to(q.queue(&unique("st-register-sink")))
        .run(&q, RunOptions::new(unique("st-register-q")).reset(true))
        .await
        .unwrap();

    assert!(!handle.query_id().is_empty(), "no query id came back");
    handle.stop().await.unwrap();

    drop_queue(&q, &src).await;
}

#[tokio::test]
async fn redeploying_a_changed_chain_is_refused_until_reset() {
    let q = broker!();
    let src = unique("st-hash");
    let sink = unique("st-hash-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;
    let query = unique("st-hash-q");

    let first = Stream::from(q.queue(&src))
        .window_tumbling(2)
        .aggregate_count("count")
        .to(q.queue(&sink))
        .run(&q, RunOptions::new(&query))
        .await
        .unwrap();
    first.stop().await.unwrap();

    // A different window size means the stored state was computed under
    // different semantics. Silently reusing it is exactly what this guard
    // exists to prevent.
    let err = Stream::from(q.queue(&src))
        .window_tumbling(10)
        .aggregate_count("count")
        .to(q.queue(&sink))
        .run(&q, RunOptions::new(&query))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("different operator chain"),
        "expected a config-hash refusal, got: {err}"
    );

    // reset(true) accepts the new shape and wipes the old state.
    let second = Stream::from(q.queue(&src))
        .window_tumbling(10)
        .aggregate_count("count")
        .to(q.queue(&sink))
        .run(&q, RunOptions::new(&query).reset(true))
        .await
        .unwrap();
    second.stop().await.unwrap();

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

// ============================================================================
// Stateless pipeline
// ============================================================================

#[tokio::test]
async fn a_stateless_pipeline_maps_filters_and_sinks() {
    let q = broker!();
    let src = unique("st-stateless");
    let sink = unique("st-stateless-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    for i in 0..10 {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "n": i }))
            .await
            .unwrap();
    }

    let handle = Stream::from(q.queue(&src))
        .filter(|r| r.number("n").unwrap_or(0.0) % 2.0 == 0.0)
        .map(|r| serde_json::json!({ "doubled": r.number("n").unwrap_or(0.0) * 2.0 }))
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-stateless-q"))
                .reset(true)
                .batch_size(50),
        )
        .await
        .unwrap();

    // 0,2,4,6,8 survive the filter and double to 0,4,8,12,16 = 40.
    let out = drain_until(&q, &sink, Duration::from_secs(20), |m| m.len() >= 5).await;
    handle.stop().await.unwrap();

    assert_eq!(out.len(), 5, "expected the five even numbers");
    assert_eq!(sum_field(&out, "doubled"), 40.0);

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn flat_map_expands_one_record_into_several() {
    let q = broker!();
    let src = unique("st-flatmap");
    let sink = unique("st-flatmap-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "items": [1, 2, 3] }))
        .await
        .unwrap();

    let handle = Stream::from(q.queue(&src))
        .flat_map(|r| {
            r.field("items")
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|v| serde_json::json!({ "v": v }))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        })
        .to(q.queue(&sink))
        .run(&q, RunOptions::new(unique("st-flatmap-q")).reset(true))
        .await
        .unwrap();

    let out = drain_until(&q, &sink, Duration::from_secs(20), |m| m.len() >= 3).await;
    handle.stop().await.unwrap();

    assert_eq!(out.len(), 3);
    assert_eq!(sum_field(&out, "v"), 6.0);

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

// ============================================================================
// Tumbling windows
// ============================================================================

#[tokio::test]
async fn a_tumbling_window_sums_and_closes_every_window() {
    let q = broker!();
    let src = unique("st-tumbling");
    let sink = unique("st-tumbling-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    // docs:start(rust-stream)
    let handle = Stream::from(q.queue(&src))
        .window_tumbling(2)
        .idle_flush_ms(500)
        .aggregate_count("count")
        .aggregate_sum("sum", |r| r.number("amount"))
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-tumbling-q"))
                .reset(true)
                .batch_size(50)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();
    // docs:end

    // 8 messages, one every ~1.1s, so they span several 2s windows.
    push_spread(&q, &src, "one", 8, Duration::from_millis(1100)).await;

    // The idle flush has to close the trailing window too, or the last
    // messages would sit in state forever.
    let out = drain_until(&q, &sink, Duration::from_secs(30), |m| {
        sum_field(m, "count") >= 8.0
    })
    .await;
    handle.stop().await.unwrap();

    assert!(out.len() >= 2, "expected several windows, got {}", out.len());
    assert_eq!(sum_field(&out, "count"), 8.0, "every message counted once");
    assert_eq!(sum_field(&out, "sum"), 36.0, "1+2+...+8");

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn window_state_is_isolated_per_partition() {
    let q = broker!();
    let src = unique("st-isolation");
    let sink = unique("st-isolation-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    let handle = Stream::from(q.queue(&src))
        .window_tumbling(2)
        .idle_flush_ms(400)
        .aggregate_sum("sum", |r| r.number("amount"))
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-isolation-q"))
                .reset(true)
                .max_partitions(8)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    // Three customers, distinct amounts. Cross-contamination would show up as
    // a sink partition whose total is not its own.
    for round in 0..4 {
        for (lane, amount) in [("cust-a", 1.0), ("cust-b", 10.0), ("cust-c", 100.0)] {
            q.queue(&src)
                .partition(lane)
                .push(serde_json::json!({ "amount": amount, "round": round }))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(700)).await;
    }

    let out = drain_until(&q, &sink, Duration::from_secs(30), |m| {
        sum_field(m, "sum") >= 444.0
    })
    .await;
    handle.stop().await.unwrap();

    assert_eq!(
        sum_field(&out, "sum"),
        444.0,
        "4 rounds x (1 + 10 + 100) — a wrong total means partitions bled into each other"
    );

    // Every emit's total must be a multiple of exactly one customer's amount.
    for m in &out {
        let s = m.data["sum"].as_f64().unwrap();
        let lane = m.partition.as_str();
        let unit = match lane {
            "cust-a" => 1.0,
            "cust-b" => 10.0,
            "cust-c" => 100.0,
            other => panic!("unexpected sink partition {other}"),
        };
        assert_eq!(
            s % unit,
            0.0,
            "partition {lane} emitted {s}, which is not a multiple of {unit}"
        );
        assert!(s > 0.0);
    }

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn every_aggregate_field_computes() {
    let q = broker!();
    let src = unique("st-aggfields");
    let sink = unique("st-aggfields-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    let handle = Stream::from(q.queue(&src))
        .window_tumbling(2)
        .idle_flush_ms(400)
        .aggregate_count("count")
        .aggregate_sum("sum", |r| r.number("v"))
        .aggregate_min("min", |r| r.number("v"))
        .aggregate_max("max", |r| r.number("v"))
        .aggregate_avg("avg", |r| r.number("v"))
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-aggfields-q"))
                .reset(true)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    // Pushed together so they land in one window.
    for v in [5.0, 1.0, 9.0, 5.0] {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "v": v }))
            .await
            .unwrap();
    }

    let out = drain_until(&q, &sink, Duration::from_secs(30), |m| {
        sum_field(m, "count") >= 4.0
    })
    .await;
    handle.stop().await.unwrap();

    assert_eq!(sum_field(&out, "count"), 4.0);
    assert_eq!(sum_field(&out, "sum"), 20.0);
    let min = out
        .iter()
        .filter_map(|m| m.data["min"].as_f64())
        .fold(f64::INFINITY, f64::min);
    let max = out
        .iter()
        .filter_map(|m| m.data["max"].as_f64())
        .fold(f64::NEG_INFINITY, f64::max);
    assert_eq!(min, 1.0);
    assert_eq!(max, 9.0);
    // avg is present and inside the range.
    for m in &out {
        let avg = m.data["avg"].as_f64().unwrap();
        assert!((1.0..=9.0).contains(&avg), "avg {avg} outside the data range");
    }

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn the_idle_flush_closes_a_window_on_a_partition_that_went_quiet() {
    let q = broker!();
    let src = unique("st-idleflush");
    let sink = unique("st-idleflush-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    let handle = Stream::from(q.queue(&src))
        .window_tumbling(1)
        .idle_flush_ms(300)
        .aggregate_count("count")
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-idleflush-q"))
                .reset(true)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    // One message, then silence. Without the idle sweep this window would
    // never close: nothing further arrives to move the clock past it.
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();

    let out = drain_until(&q, &sink, Duration::from_secs(25), |m| !m.is_empty()).await;
    let metrics = handle.metrics();
    handle.stop().await.unwrap();

    assert!(
        !out.is_empty(),
        "the trailing window never closed — the idle flush did not run"
    );
    assert_eq!(sum_field(&out, "count"), 1.0);
    assert!(
        metrics.flush_cycles > 0,
        "no flush cycle was recorded: {metrics:?}"
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

// ============================================================================
// Other window kinds
// ============================================================================

#[tokio::test]
async fn a_sliding_window_counts_each_event_in_several_windows() {
    let q = broker!();
    let src = unique("st-sliding");
    let sink = unique("st-sliding-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    let handle = Stream::from(q.queue(&src))
        .window_sliding(4, 2)
        .idle_flush_ms(400)
        .aggregate_count("count")
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-sliding-q"))
                .reset(true)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    push_spread(&q, &src, "one", 4, Duration::from_millis(600)).await;

    // size/slide = 2 windows per event, so 4 events contribute 8 counts in
    // total across the closed windows.
    let out = drain_until(&q, &sink, Duration::from_secs(30), |m| {
        sum_field(m, "count") >= 8.0
    })
    .await;
    handle.stop().await.unwrap();

    assert!(
        sum_field(&out, "count") >= 8.0,
        "each event should land in 2 windows; got {} across {} emits",
        sum_field(&out, "count"),
        out.len()
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn a_session_window_closes_after_silence() {
    let q = broker!();
    let src = unique("st-session");
    let sink = unique("st-session-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    let handle = Stream::from(q.queue(&src))
        .window_session(2)
        .idle_flush_ms(300)
        .aggregate_count("count")
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-session-q"))
                .reset(true)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    // A burst of three, then a gap longer than the session gap.
    for _ in 0..3 {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "n": 1 }))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let out = drain_until(&q, &sink, Duration::from_secs(25), |m| !m.is_empty()).await;
    handle.stop().await.unwrap();

    assert!(!out.is_empty(), "the session never closed");
    assert_eq!(
        sum_field(&out, "count"),
        3.0,
        "the burst should have closed as one session of three"
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn a_cron_window_aligns_to_the_wall_clock() {
    let q = broker!();
    let src = unique("st-cron");
    let sink = unique("st-cron-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    let handle = Stream::from(q.queue(&src))
        .window_cron(Every::Second)
        .idle_flush_ms(300)
        .aggregate_count("count")
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-cron-q"))
                .reset(true)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    push_spread(&q, &src, "one", 4, Duration::from_millis(500)).await;

    let out = drain_until(&q, &sink, Duration::from_secs(25), |m| {
        sum_field(m, "count") >= 4.0
    })
    .await;
    handle.stop().await.unwrap();

    assert_eq!(sum_field(&out, "count"), 4.0);

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

// ============================================================================
// Keying and event time
// ============================================================================

#[tokio::test]
async fn key_by_splits_state_within_a_partition() {
    let q = broker!();
    let src = unique("st-keyby");
    let sink = unique("st-keyby-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    let handle = Stream::from(q.queue(&src))
        .key_by(|r| r.text("kind").unwrap_or("other").to_string())
        .window_tumbling(2)
        .idle_flush_ms(400)
        .aggregate_sum("sum", |r| r.number("v"))
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-keyby-q"))
                .reset(true)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    // One partition, two logical keys. Each must accumulate on its own.
    for _ in 0..3 {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "kind": "a", "v": 1 }))
            .await
            .unwrap();
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "kind": "b", "v": 10 }))
            .await
            .unwrap();
    }

    let out = drain_until(&q, &sink, Duration::from_secs(30), |m| {
        sum_field(m, "sum") >= 33.0
    })
    .await;
    handle.stop().await.unwrap();

    assert_eq!(sum_field(&out, "sum"), 33.0, "3x1 + 3x10");
    // No emit may mix the two keys: every total is a multiple of 1 or of 10,
    // and a mixed one would be neither a pure 3 nor a pure 30.
    for m in &out {
        let s = m.data["sum"].as_f64().unwrap();
        assert!(
            s % 10.0 == 0.0 || s < 10.0,
            "emit {s} looks like two keys merged"
        );
    }

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn event_time_windows_by_the_payloads_timestamp_and_drops_late_events() {
    let q = broker!();
    let src = unique("st-eventtime");
    let sink = unique("st-eventtime-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    let handle = Stream::from(q.queue(&src))
        .window_tumbling(10)
        .event_time(|m| m.data.get("ts").and_then(|v| v.as_i64()))
        .idle_flush_ms(0) // event time: no wall-clock sweeping
        .aggregate_count("count")
        .aggregate_sum("sum", |r| r.number("v"))
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-eventtime-q"))
                .reset(true)
                .batch_size(50)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    // Timestamps in the payload, not arrival order. Windows are 10s of event
    // time; these all belong to the window starting at 0.
    for (ts, v) in [(1_000i64, 1.0), (2_000, 2.0), (3_000, 3.0)] {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "ts": ts, "v": v }))
            .await
            .unwrap();
    }
    // Advance event time well past the first window so it closes.
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "ts": 60_000, "v": 100.0 }))
        .await
        .unwrap();

    let out = drain_until(&q, &sink, Duration::from_secs(30), |m| {
        sum_field(m, "sum") >= 6.0
    })
    .await;

    let before_late = handle.metrics().late_events;

    // Now a very late event: its timestamp is far below the watermark, so with
    // the default policy it is dropped rather than reopening a closed window.
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "ts": 500, "v": 999.0 }))
        .await
        .unwrap();
    sleep_ms(2500).await;

    let metrics = handle.metrics();
    handle.stop().await.unwrap();

    assert_eq!(
        sum_field(&out, "sum"),
        6.0,
        "the first event-time window should total 1+2+3"
    );
    assert!(
        metrics.late_events > before_late,
        "the late event was not counted as late: {metrics:?}"
    );

    // And it never reached the sink.
    let extra = drain_until(&q, &sink, Duration::from_secs(3), |_| false).await;
    assert!(
        !extra.iter().any(|m| m.data["sum"].as_f64() == Some(999.0)),
        "a dropped late event still produced an emit"
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn on_late_include_accumulates_a_late_event_instead_of_dropping_it() {
    let q = broker!();
    let src = unique("st-lateinclude");
    let sink = unique("st-lateinclude-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    let handle = Stream::from(q.queue(&src))
        .window_tumbling(10)
        .event_time(|m| m.data.get("ts").and_then(|v| v.as_i64()))
        .on_late(LatePolicy::Include)
        .idle_flush_ms(0)
        .aggregate_sum("sum", |r| r.number("v"))
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-lateinclude-q"))
                .reset(true)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    // Fill and close the window [0, 10s).
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "ts": 1_000, "v": 1.0 }))
        .await
        .unwrap();
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "ts": 90_000, "v": 0.0 }))
        .await
        .unwrap();
    let first = drain_until(&q, &sink, Duration::from_secs(25), |m| !m.is_empty()).await;
    assert!(!first.is_empty(), "the window never closed");

    // A straggler for the closed window. Under `Include` it is accumulated
    // rather than discarded, which recreates the state row and produces a
    // second emit — best-effort, and the reason `Drop` is the default.
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "ts": 500, "v": 7.0 }))
        .await
        .unwrap();
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "ts": 95_000, "v": 0.0 }))
        .await
        .unwrap();

    let second = drain_until(&q, &sink, Duration::from_secs(25), |m| {
        m.iter().any(|x| x.data["sum"].as_f64() == Some(7.0))
    })
    .await;
    let metrics = handle.metrics();
    handle.stop().await.unwrap();

    assert!(
        second.iter().any(|m| m.data["sum"].as_f64() == Some(7.0)),
        "the late event was dropped even though on_late is Include"
    );
    assert_eq!(
        metrics.late_events, 0,
        "Include must not count events as dropped-late: {metrics:?}"
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

// ============================================================================
// Reduce, foreach, post-stages
// ============================================================================

#[tokio::test]
async fn a_custom_fold_accumulates_across_a_window() {
    let q = broker!();
    let src = unique("st-reduce");
    let sink = unique("st-reduce-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    let handle = Stream::from(q.queue(&src))
        .window_tumbling(2)
        .idle_flush_ms(400)
        .reduce(serde_json::json!({ "product": 1.0 }), |acc, r| {
            let cur = acc.get("product").and_then(|v| v.as_f64()).unwrap_or(1.0);
            serde_json::json!({ "product": cur * r.number("v").unwrap_or(1.0) })
        })
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-reduce-q"))
                .reset(true)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    for v in [2.0, 3.0, 5.0] {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "v": v }))
            .await
            .unwrap();
    }

    let out = drain_until(&q, &sink, Duration::from_secs(30), |m| !m.is_empty()).await;
    handle.stop().await.unwrap();

    let product: f64 = out
        .iter()
        .filter_map(|m| m.data["product"].as_f64())
        .fold(1.0, |a, b| a * b);
    assert_eq!(product, 30.0, "2 * 3 * 5");

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn foreach_runs_a_side_effect_with_window_context() {
    let q = broker!();
    let src = unique("st-foreach");
    create_queue(&q, &src, QueueOptions::default()).await;

    let seen = Arc::new(Mutex::new(Vec::<(String, Option<i64>)>::new()));
    let sink = Arc::clone(&seen);

    let handle = Stream::from(q.queue(&src))
        .window_tumbling(1)
        .idle_flush_ms(300)
        .aggregate_count("count")
        .foreach(move |value, ctx| {
            let sink = Arc::clone(&sink);
            async move {
                sink.lock()
                    .unwrap()
                    .push((ctx.key.clone(), ctx.window_start));
                assert!(value.get("count").is_some());
                Ok(())
            }
        })
        .run(
            &q,
            RunOptions::new(unique("st-foreach-q"))
                .reset(true)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(25);
    while seen.lock().unwrap().is_empty() && std::time::Instant::now() < deadline {
        sleep_ms(200).await;
    }
    handle.stop().await.unwrap();

    let seen = seen.lock().unwrap();
    assert!(!seen.is_empty(), "foreach never ran");
    let (key, window_start) = &seen[0];
    assert!(!key.is_empty(), "the emit context carried no key");
    assert!(
        window_start.is_some(),
        "a windowed emit should carry its window start"
    );

    drop_queue(&q, &src).await;
}

#[tokio::test]
async fn a_post_reducer_map_reshapes_the_emitted_value() {
    let q = broker!();
    let src = unique("st-post");
    let sink = unique("st-post-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    let handle = Stream::from(q.queue(&src))
        .window_tumbling(1)
        .idle_flush_ms(300)
        .aggregate_count("count")
        // Runs on the aggregate, not on the source records.
        .map(|r| {
            serde_json::json!({
                "total": r.number("count").unwrap_or(0.0),
                "labelled": true,
                "has_window": r.ctx.as_ref().and_then(|c| c.window_start).is_some(),
            })
        })
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-post-q"))
                .reset(true)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    for _ in 0..3 {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "n": 1 }))
            .await
            .unwrap();
    }

    let out = drain_until(&q, &sink, Duration::from_secs(25), |m| {
        sum_field(m, "total") >= 3.0
    })
    .await;
    handle.stop().await.unwrap();

    assert_eq!(sum_field(&out, "total"), 3.0);
    assert!(out.iter().all(|m| m.data["labelled"] == true));
    assert!(
        out.iter().all(|m| m.data["has_window"] == true),
        "the post stage did not receive the window context"
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

// ============================================================================
// Gate
// ============================================================================

#[tokio::test]
async fn a_gate_lets_a_prefix_through_and_holds_the_rest_in_order() {
    let q = broker!();
    let src = unique("st-gate");
    let sink = unique("st-gate-sink");
    // A short lease so the held tail comes back quickly.
    create_queue(
        &q,
        &src,
        QueueOptions {
            lease_time: Some(2),
            ..Default::default()
        },
    )
    .await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    // A token bucket with a fixed budget per cycle: the first three of any
    // batch pass, the rest are held.
    let handle = Stream::from(q.queue(&src))
        .gate(|_rec, ctx| {
            let used = ctx.num("used", 0.0);
            if used >= 3.0 {
                return false;
            }
            ctx.set_num("used", used + 1.0);
            true
        })
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-gate-q"))
                .reset(true)
                .batch_size(10)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    for i in 0..6 {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "i": i }))
            .await
            .unwrap();
    }

    // Only the budget passes; the tail is denied and keeps its lease.
    let out = drain_until(&q, &sink, Duration::from_secs(12), |m| m.len() >= 3).await;
    let metrics = handle.metrics();
    handle.stop().await.unwrap();

    assert!(out.len() >= 3, "the allowed prefix never arrived");
    assert!(metrics.gate_allowed >= 3, "{metrics:?}");
    assert!(
        metrics.gate_denied > 0,
        "nothing was denied, so the budget was not enforced: {metrics:?}"
    );

    // What did arrive is in source order — the gate must not reorder a lane.
    let order: Vec<i64> = out.iter().filter_map(|m| m.data["i"].as_i64()).collect();
    let mut sorted = order.clone();
    sorted.sort();
    assert_eq!(order, sorted, "the gate reordered the partition: {order:?}");

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn a_gate_that_denies_everything_commits_nothing() {
    let q = broker!();
    let src = unique("st-gate-deny");
    let sink = unique("st-gate-deny-sink");
    create_queue(
        &q,
        &src,
        QueueOptions {
            lease_time: Some(2),
            ..Default::default()
        },
    )
    .await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    let handle = Stream::from(q.queue(&src))
        .gate(|_, _| false)
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-gate-deny-q"))
                .reset(true)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "i": 0 }))
        .await
        .unwrap();

    sleep_ms(4000).await;
    let metrics = handle.metrics();
    handle.stop().await.unwrap();

    let out = drain_until(&q, &sink, Duration::from_secs(3), |_| false).await;
    assert!(out.is_empty(), "a fully denied batch still emitted");
    assert!(metrics.gate_denied > 0, "{metrics:?}");
    assert_eq!(metrics.gate_allowed, 0);

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

// ============================================================================
// Durability
// ============================================================================

#[tokio::test]
async fn state_survives_a_restart_of_the_runner() {
    let q = broker!();
    let src = unique("st-restart");
    let sink = unique("st-restart-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;
    let query = unique("st-restart-q");

    // Event time, so the test controls the clock instead of racing it: every
    // value below lands in the window [0, 10s), which only closes when a much
    // later timestamp arrives at the very end.
    let build = |queen: &queen_mq::Queen| {
        Stream::from(queen.queue(&src))
            .window_tumbling(10)
            .event_time(|m| m.data.get("ts").and_then(|v| v.as_i64()))
            .idle_flush_ms(0)
            .aggregate_sum("sum", |r| r.number("v"))
            .to(queen.queue(&sink))
    };

    let first = build(&q)
        .run(
            &q,
            RunOptions::new(&query)
                .reset(true)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();
    for (ts, v) in [(1_000i64, 1.0), (2_000, 2.0)] {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "ts": ts, "v": v }))
            .await
            .unwrap();
    }
    sleep_ms(2500).await;
    let first_metrics = first.metrics();
    first.stop().await.unwrap();
    assert!(first_metrics.cycles > 0, "the first runner never cycled");

    // Nothing emitted yet: the window is still open.
    let early = drain_until(&q, &sink, Duration::from_secs(2), |_| false).await;
    assert!(early.is_empty(), "the window closed before it should have");

    // A second runner under the same query id must resume the accumulator
    // rather than starting from zero.
    let second = build(&q)
        .run(
            &q,
            RunOptions::new(&query).max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();
    for (ts, v) in [(3_000i64, 4.0), (4_000, 8.0)] {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "ts": ts, "v": v }))
            .await
            .unwrap();
    }
    // Push event time past the window so it closes.
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "ts": 90_000, "v": 0.0 }))
        .await
        .unwrap();

    let out = drain_until(&q, &sink, Duration::from_secs(30), |m| !m.is_empty()).await;
    second.stop().await.unwrap();

    assert!(!out.is_empty(), "the window never closed after the restart");
    assert_eq!(
        sum_field(&out, "sum"),
        15.0,
        "the accumulator did not survive the restart (expected 1+2+4+8)"
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn two_queries_over_one_queue_keep_separate_state() {
    let q = broker!();
    let src = unique("st-twoqueries");
    let sink_a = unique("st-twoqueries-a");
    let sink_b = unique("st-twoqueries-b");
    for name in [&src, &sink_a, &sink_b] {
        create_queue(&q, name, QueueOptions::default()).await;
    }

    let a = Stream::from(q.queue(&src))
        .window_tumbling(1)
        .idle_flush_ms(300)
        .aggregate_count("count")
        .to(q.queue(&sink_a))
        .run(
            &q,
            RunOptions::new(unique("st-twoqueries-qa"))
                .reset(true)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    let b = Stream::from(q.queue(&src))
        .window_tumbling(1)
        .idle_flush_ms(300)
        .aggregate_sum("sum", |r| r.number("v"))
        .to(q.queue(&sink_b))
        .run(
            &q,
            RunOptions::new(unique("st-twoqueries-qb"))
                .reset(true)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    for v in [3.0, 4.0] {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "v": v }))
            .await
            .unwrap();
    }

    // Each query has its own consumer group, so both see every message.
    let out_a = drain_until(&q, &sink_a, Duration::from_secs(25), |m| {
        sum_field(m, "count") >= 2.0
    })
    .await;
    let out_b = drain_until(&q, &sink_b, Duration::from_secs(25), |m| {
        sum_field(m, "sum") >= 7.0
    })
    .await;

    a.stop().await.unwrap();
    b.stop().await.unwrap();

    assert_eq!(sum_field(&out_a, "count"), 2.0);
    assert_eq!(sum_field(&out_b, "sum"), 7.0);

    for name in [&src, &sink_a, &sink_b] {
        drop_queue(&q, name).await;
    }
}

#[tokio::test]
async fn a_stream_reports_its_work_and_stops_cleanly() {
    let q = broker!();
    let src = unique("st-metrics");
    let sink = unique("st-metrics-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    let handle = Stream::from(q.queue(&src))
        .map(|r| r.data.clone())
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-metrics-q"))
                .reset(true)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    let pushed = 5;
    for i in 0..pushed {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "i": i }))
            .await
            .unwrap();
    }

    let _ = drain_until(&q, &sink, Duration::from_secs(20), |m| m.len() >= pushed).await;
    let metrics = handle.metrics();

    // stop() must return promptly and leave nothing running.
    let stopped = tokio::time::timeout(Duration::from_secs(10), handle.stop()).await;
    assert!(stopped.is_ok(), "stop() hung");
    stopped.unwrap().unwrap();

    assert!(metrics.cycles > 0, "{metrics:?}");
    assert_eq!(metrics.messages, pushed as u64, "{metrics:?}");
    assert_eq!(metrics.push_items, pushed as u64, "{metrics:?}");
    assert_eq!(metrics.errors, 0, "{metrics:?}");

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn a_stream_over_many_partitions_keeps_every_message() {
    let q = broker!();
    let src = unique("st-manyparts");
    let sink = unique("st-manyparts-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    let handle = Stream::from(q.queue(&src))
        .map(|r| r.data.clone())
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-manyparts-q"))
                .reset(true)
                .max_partitions(16)
                .batch_size(200)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    const LANES: usize = 12;
    const PER_LANE: usize = 10;
    for lane in 0..LANES {
        for i in 0..PER_LANE {
            q.queue(&src)
                .partition(format!("lane-{lane}"))
                .push(serde_json::json!({ "lane": lane, "i": i }))
                .await
                .unwrap();
        }
    }

    let want = LANES * PER_LANE;
    let out = drain_until(&q, &sink, Duration::from_secs(45), |m| m.len() >= want).await;
    handle.stop().await.unwrap();

    assert_eq!(out.len(), want, "lost {} of {want} messages", want - out.len());
    // Each lane's messages stay in order within their own lane.
    for lane in 0..LANES {
        let seq: Vec<i64> = out
            .iter()
            .filter(|m| m.data["lane"].as_u64() == Some(lane as u64))
            .filter_map(|m| m.data["i"].as_i64())
            .collect();
        let mut sorted = seq.clone();
        sorted.sort();
        assert_eq!(seq, sorted, "lane {lane} arrived out of order: {seq:?}");
    }

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}
