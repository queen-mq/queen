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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use queen_mq::streams::{Every, LatePolicy, RunOptions, Stream};
use queen_mq::{Cancel, QueueOptions, SubscriptionMode};

use common::*;

/// Push `n` messages one per `gap`, so events actually span several windows.
async fn push_spread(
    queen: &queen_mq::Queen,
    queue: &str,
    partition: &str,
    n: usize,
    gap: Duration,
) {
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
        .to(q.queue(unique("st-register-sink")))
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

    assert!(
        out.len() >= 2,
        "expected several windows, got {}",
        out.len()
    );
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
        assert!(
            (1.0..=9.0).contains(&avg),
            "avg {avg} outside the data range"
        );
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

    {
        let seen = seen.lock().unwrap();
        assert!(!seen.is_empty(), "foreach never ran");
        let (key, window_start) = &seen[0];
        assert!(!key.is_empty(), "the emit context carried no key");
        assert!(
            window_start.is_some(),
            "a windowed emit should carry its window start"
        );
    }

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
// Gate behind a stateless stage
// ============================================================================
//
// A pre-stage breaks the one-to-one correspondence between claimed messages and
// records: `filter` leaves a message with no record at all, `flat_map` leaves it
// with several. The gate's ack is an offset commit — `count` messages ending at
// a transaction id — so every one of these tests is really asking the same
// question: is the ack counted in messages, or in records?

/// A source whose lease lapses in a couple of seconds, so a held or unsettled
/// tail comes back while the test is still watching, and whose retry limit is
/// high enough that a redelivery loop shows up as a duplicate emit instead of
/// quietly draining into the DLQ.
fn replayable_source() -> QueueOptions {
    QueueOptions {
        lease_time: Some(2),
        retry_limit: Some(50),
        ..Default::default()
    }
}

/// The consumer group a stream claims under when `RunOptions` names none.
fn stream_group(query_id: &str) -> String {
    format!("streams.{query_id}")
}

#[tokio::test]
async fn a_gate_behind_a_filter_settles_every_claimed_message() {
    let q = broker!();
    let src = unique("st-gate-filter");
    let sink = unique("st-gate-filter-sink");
    create_queue(&q, &src, replayable_source()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;
    let query = unique("st-gate-filter-q");

    // Six claimed messages, two of which survive the filter, and a gate that
    // allows everything: all six have to be settled. Acking the two *records*
    // instead settles the first two messages, leaves four claimed for ever, and
    // re-emits the two that did pass on every redelivery.
    let handle = Stream::from(q.queue(&src))
        .filter(|r| r.number("i").unwrap_or(0.0) >= 4.0)
        .gate(|_, _| true)
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(&query)
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

    let out = drain_until(&q, &sink, Duration::from_secs(25), |m| m.len() >= 2).await;
    // Keep watching for longer than the lease: a misplaced ack shows up as the
    // same records arriving a second time.
    let extra = drain_until(&q, &sink, Duration::from_secs(6), |_| false).await;
    let metrics = handle.metrics();
    handle.stop().await.unwrap();

    let mut seen: Vec<i64> = out.iter().filter_map(|m| m.data["i"].as_i64()).collect();
    seen.sort();
    assert_eq!(
        seen,
        vec![4, 5],
        "the wrong records reached the sink: {seen:?}"
    );
    assert!(
        extra.is_empty(),
        "the batch was redelivered and re-emitted: {} extra sink messages",
        extra.len()
    );
    assert_eq!(
        metrics.gate_allowed, 6,
        "the gate settles messages, not records — four were filtered out but all \
         six were claimed: {metrics:?}"
    );
    assert_eq!(metrics.errors, 0, "{metrics:?}");

    // Nothing is left claimable: the filtered-out messages were acked too.
    let leftover = pop_retry(&q, &src, Some(&stream_group(&query)), 10, 10).await;
    assert!(
        leftover.is_empty(),
        "{} messages were never settled",
        leftover.len()
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn a_gate_behind_a_flat_map_acks_the_message_not_its_records() {
    let q = broker!();
    let src = unique("st-gate-fanout");
    let sink = unique("st-gate-fanout-sink");
    create_queue(&q, &src, replayable_source()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;
    let query = unique("st-gate-fanout-q");

    // One claimed message, three records. Counting records indexes past the end
    // of the claimed batch — a panic inside the spawned loop task, which
    // `stop()` then reports as a clean shutdown while the sink stays empty.
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
        .gate(|_, _| true)
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(&query)
                .reset(true)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "items": [1, 2, 3] }))
        .await
        .unwrap();

    let out = drain_until(&q, &sink, Duration::from_secs(25), |m| m.len() >= 3).await;
    let extra = drain_until(&q, &sink, Duration::from_secs(6), |_| false).await;
    let metrics = handle.metrics();
    handle.stop().await.unwrap();

    assert_eq!(out.len(), 3, "the expanded records never reached the sink");
    assert_eq!(sum_field(&out, "v"), 6.0, "1 + 2 + 3");
    assert!(
        extra.is_empty(),
        "the message was redelivered and expanded again: {} extra sink messages",
        extra.len()
    );
    assert_eq!(
        metrics.gate_allowed, 1,
        "one message was settled, however many records it produced: {metrics:?}"
    );
    assert_eq!(metrics.errors, 0, "{metrics:?}");

    let leftover = pop_retry(&q, &src, Some(&stream_group(&query)), 10, 10).await;
    assert!(leftover.is_empty(), "the message was never settled");

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn a_gate_that_denies_half_an_expanded_message_commits_none_of_it() {
    let q = broker!();
    let src = unique("st-gate-partial");
    let sink = unique("st-gate-partial-sink");
    create_queue(&q, &src, replayable_source()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;
    let query = unique("st-gate-partial-q");

    // The gate allows two of the message's three records. A redelivery replays
    // the whole message, so committing the allowed prefix would duplicate those
    // two records on every retry: a message is settled only when all of it is.
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
        .gate(|rec, _| rec.number("v").unwrap_or(0.0) < 3.0)
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(&query)
                .reset(true)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "items": [1, 2, 3] }))
        .await
        .unwrap();

    // Long enough for the lease to lapse and the message to be replayed twice.
    let out = drain_until(&q, &sink, Duration::from_secs(9), |_| false).await;
    let metrics = handle.metrics();
    handle.stop().await.unwrap();

    assert!(
        out.is_empty(),
        "{} records of a part-denied message were committed",
        out.len()
    );
    assert_eq!(
        metrics.gate_allowed, 0,
        "no message was fully allowed: {metrics:?}"
    );
    assert!(metrics.gate_denied > 0, "{metrics:?}");

    // Un-acked, so it is claimable again once the lease lapses.
    let back = pop_retry(&q, &src, Some(&stream_group(&query)), 10, 30).await;
    assert_eq!(
        back.len(),
        1,
        "the part-denied message was settled instead of being held"
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn a_gate_ack_counts_messages_even_when_a_filter_thinned_them() {
    let q = broker!();
    let src = unique("st-gate-prefix");
    let sink = unique("st-gate-prefix-sink");
    create_queue(&q, &src, replayable_source()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;
    let query = unique("st-gate-prefix-q");

    // Five messages; the filter drops #1, the gate denies from #3 on. The
    // allowed prefix is three MESSAGES (#0, the empty #1, #2) but only two
    // RECORDS. Those two counts must not be confused: acking two would leave
    // message #2 claimed after its record was already emitted, so the redelivery
    // would emit it twice.
    let handle = Stream::from(q.queue(&src))
        .filter(|r| r.number("i").unwrap_or(0.0) != 1.0)
        .gate(|rec, _| rec.number("i").unwrap_or(0.0) < 3.0)
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(&query)
                .reset(true)
                .batch_size(10)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    for i in 0..5 {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "i": i }))
            .await
            .unwrap();
    }

    let out = drain_until(&q, &sink, Duration::from_secs(25), |m| m.len() >= 2).await;
    let extra = drain_until(&q, &sink, Duration::from_secs(6), |_| false).await;
    let metrics = handle.metrics();
    handle.stop().await.unwrap();

    let mut seen: Vec<i64> = out.iter().filter_map(|m| m.data["i"].as_i64()).collect();
    seen.sort();
    assert_eq!(seen, vec![0, 2], "the wrong prefix was emitted: {seen:?}");
    assert!(
        extra.is_empty(),
        "an already-emitted message was redelivered: {} extra sink messages",
        extra.len()
    );
    assert_eq!(
        metrics.gate_allowed, 3,
        "three messages were settled to emit two records: {metrics:?}"
    );

    // The tail comes back exactly where the ack stopped.
    let back = pop_retry(&q, &src, Some(&stream_group(&query)), 10, 30).await;
    assert!(
        !back.is_empty(),
        "the denied tail was settled instead of being held"
    );
    let first = back[0].data["i"].as_i64();
    assert_eq!(
        first,
        Some(3),
        "the ack settled the wrong prefix: the tail resumes at {first:?}, not at 3"
    );
    let replayed: Vec<i64> = back.iter().filter_map(|m| m.data["i"].as_i64()).collect();
    assert!(
        replayed.iter().all(|i| *i >= 3),
        "an allowed message came back: {replayed:?}"
    );

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

    assert_eq!(
        out.len(),
        want,
        "lost {} of {want} messages",
        want - out.len()
    );
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

// ============================================================================
// Event time: the watermark drives the sweep
// ============================================================================

#[tokio::test]
async fn an_event_time_idle_flush_waits_for_the_watermark_not_the_wall_clock() {
    let q = broker!();
    let src = unique("st-etflush");
    let sink = unique("st-etflush-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    // The other event-time tests all pass idle_flush_ms(0), so the watermark
    // branch of the sweep never runs — and a sweep is on by default in
    // production (5s for tumbling). These timestamps are in 1970, so a sweep
    // that reached for the wall clock instead of the watermark would find every
    // window ripe by half a century and close them immediately.
    let handle = Stream::from(q.queue(&src))
        .window_tumbling(10)
        .event_time(|m| m.data.get("ts").and_then(|v| v.as_i64()))
        .idle_flush_ms(300)
        .aggregate_sum("sum", |r| r.number("v"))
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-etflush-q"))
                .reset(true)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    // Two events inside the window [0s, 10s), then silence. Event time has only
    // reached 2s, so the window is not ripe and nothing may be emitted however
    // many times the sweep ticks.
    for (ts, v) in [(1_000i64, 1.0), (2_000, 2.0)] {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "ts": ts, "v": v }))
            .await
            .unwrap();
    }

    let premature = drain_until(&q, &sink, Duration::from_secs(5), |_| false).await;
    let idle_metrics = handle.metrics();
    // Without this the silence below would prove nothing: a runner that never
    // cycled has no watermark to sweep against either.
    assert!(
        idle_metrics.cycles > 0,
        "the two events were never consumed: {idle_metrics:?}"
    );
    assert!(
        premature.is_empty(),
        "the sweep closed a window the data had not reached yet: {} emits",
        premature.len()
    );
    assert_eq!(
        idle_metrics.flush_cycles, 0,
        "a flush committed on a partition whose watermark had not moved: {idle_metrics:?}"
    );

    // Now move event time past the window end. Exactly one window closes: the
    // one this event lands in stays open, because the watermark it just set
    // sits at its start, not past its end.
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "ts": 60_000, "v": 100.0 }))
        .await
        .unwrap();

    let out = drain_until(&q, &sink, Duration::from_secs(25), |m| !m.is_empty()).await;
    let tail = drain_until(&q, &sink, Duration::from_secs(4), |_| false).await;
    handle.stop().await.unwrap();

    assert_eq!(
        out.len(),
        1,
        "expected one closed window, got {}",
        out.len()
    );
    assert_eq!(
        sum_field(&out, "sum"),
        3.0,
        "the closed window should total 1+2 and nothing else"
    );
    assert!(
        tail.is_empty(),
        "the window holding the newest event closed too: {} extra emits",
        tail.len()
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn allowed_lateness_admits_a_straggler_inside_the_tolerance_and_drops_the_rest() {
    let q = broker!();
    let src = unique("st-lateness");
    let sink = unique("st-lateness-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    // allowed_lateness holds the watermark back: it is max(event time) minus
    // the tolerance, so an event that old is still on time. Nothing else
    // exercises it, and the default of 0 makes the subtraction invisible.
    let handle = Stream::from(q.queue(&src))
        .window_tumbling(10)
        .event_time(|m| m.data.get("ts").and_then(|v| v.as_i64()))
        .allowed_lateness(30)
        .idle_flush_ms(0)
        .aggregate_sum("sum", |r| r.number("v"))
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-lateness-q"))
                .reset(true)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    // Watermark after this cycle: 100s - 30s = 70s. It has to be established
    // before the stragglers arrive, since a batch is judged against the
    // watermark it started with.
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "ts": 100_000, "v": 1.0 }))
        .await
        .unwrap();
    sleep_ms(2500).await;

    // 75s: 25s late, inside the 30s tolerance, so it accumulates into [70s, 80s).
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "ts": 75_000, "v": 5.0 }))
        .await
        .unwrap();
    // 60s: past the tolerance, so it is dropped rather than reopening anything.
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "ts": 60_000, "v": 999.0 }))
        .await
        .unwrap();
    sleep_ms(2500).await;

    // Push event time far enough ahead to close both windows.
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "ts": 200_000, "v": 0.0 }))
        .await
        .unwrap();

    let out = drain_until(&q, &sink, Duration::from_secs(30), |m| {
        sum_field(m, "sum") >= 6.0
    })
    .await;
    let metrics = handle.metrics();
    handle.stop().await.unwrap();

    assert_eq!(
        sum_field(&out, "sum"),
        6.0,
        "expected 1 from the on-time window and 5 from the tolerated straggler, \
         with the 999 dropped: {out:?}"
    );
    assert!(
        out.iter().any(|m| m.data["sum"].as_f64() == Some(5.0)),
        "the straggler inside the tolerance was dropped: {out:?}"
    );
    assert!(
        metrics.late_events >= 1,
        "the event past the tolerance was not counted as late: {metrics:?}"
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

// ============================================================================
// Windows without a reducer, post stages, sinks
// ============================================================================

#[tokio::test]
async fn a_window_without_a_reducer_annotates_and_passes_records_straight_through() {
    let q = broker!();
    let src = unique("st-annotate");
    let sink = unique("st-annotate-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    // `window_tumbling(..).to(..)` with no reducer is allowed by the chain
    // rules but never exercised: it is the one branch that emits records
    // without folding them. A minute-long window makes the point — nothing is
    // buffered waiting for it to close, and no state row is written.
    let handle = Stream::from(q.queue(&src))
        .window_tumbling(60)
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-annotate-q"))
                .reset(true)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    for n in 1..=3 {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "n": n }))
            .await
            .unwrap();
    }

    let out = drain_until(&q, &sink, Duration::from_secs(20), |m| m.len() >= 3).await;
    let metrics = handle.metrics();
    handle.stop().await.unwrap();

    assert_eq!(
        out.len(),
        3,
        "the records were held back waiting for a window that had no reducer to close"
    );
    assert_eq!(sum_field(&out, "n"), 6.0, "the payloads were reshaped");
    assert_eq!(
        metrics.state_ops, 0,
        "an annotating window wrote state it will never read: {metrics:?}"
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn a_post_reducer_filter_drops_whole_emits() {
    let q = broker!();
    let src = unique("st-postfilter");
    let sink = unique("st-postfilter-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    // Only Map was covered on the post side. A filter there runs on the closed
    // aggregate, so it suppresses an entire window's emit — the state row is
    // still settled, it just never reaches the sink.
    let handle = Stream::from(q.queue(&src))
        .window_tumbling(1)
        .idle_flush_ms(300)
        .aggregate_sum("sum", |r| r.number("v"))
        .filter(|r| r.number("sum").unwrap_or(0.0) >= 10.0)
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-postfilter-q"))
                .reset(true)
                .max_partitions(4)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    // One message per lane, so each lane closes exactly one window.
    for (lane, v) in [("loud", 10.0), ("quiet", 1.0)] {
        q.queue(&src)
            .partition(lane)
            .push(serde_json::json!({ "v": v }))
            .await
            .unwrap();
    }

    let out = drain_until(&q, &sink, Duration::from_secs(25), |m| !m.is_empty()).await;
    let tail = drain_until(&q, &sink, Duration::from_secs(5), |_| false).await;
    handle.stop().await.unwrap();

    assert_eq!(out.len(), 1, "expected only the loud lane's window");
    assert_eq!(sum_field(&out, "sum"), 10.0);
    assert_eq!(out[0].partition.as_str(), "loud");
    assert!(
        tail.is_empty(),
        "the quiet lane's emit reached the sink after all: {tail:?}"
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn a_post_reducer_flat_map_fans_one_emit_into_several_sink_messages() {
    let q = broker!();
    let src = unique("st-postfanout");
    let sink = unique("st-postfanout-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    // The FlatMap arm of the post stage has to carry the emit's key and window
    // key onto every copy, or the sink loses the window each row came from.
    let handle = Stream::from(q.queue(&src))
        .window_tumbling(1)
        .idle_flush_ms(300)
        .aggregate_sum("sum", |r| r.number("v"))
        .flat_map(|r| {
            let half = r.number("sum").unwrap_or(0.0) / 2.0;
            vec![
                serde_json::json!({ "half": half, "side": "left" }),
                serde_json::json!({ "half": half, "side": "right" }),
            ]
        })
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-postfanout-q"))
                .reset(true)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "v": 8.0 }))
        .await
        .unwrap();

    let out = drain_until(&q, &sink, Duration::from_secs(25), |m| m.len() >= 2).await;
    handle.stop().await.unwrap();

    assert_eq!(
        out.len(),
        2,
        "one emit should have become two sink messages"
    );
    assert_eq!(sum_field(&out, "half"), 8.0, "the halves lost their total");
    let mut sides: Vec<&str> = out.iter().filter_map(|m| m.data["side"].as_str()).collect();
    sides.sort();
    assert_eq!(
        sides,
        vec!["left", "right"],
        "both copies carry the same side, so the fan-out duplicated one value"
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn to_partitioned_routes_by_the_emitted_value_not_the_source_lane() {
    let q = broker!();
    let src = unique("st-topart");
    let sink = unique("st-topart-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    // `to_partitioned` has no call site anywhere in the crate. Everything is
    // pushed down one source lane, so a sink that reused the source partition
    // would put every message in "mixed".
    let handle = Stream::from(q.queue(&src))
        .to_partitioned(q.queue(&sink), |v| {
            v.get("tenant")
                .and_then(|t| t.as_str())
                .unwrap_or("none")
                .to_string()
        })
        .run(
            &q,
            RunOptions::new(unique("st-topart-q"))
                .reset(true)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    for tenant in ["acme", "acme", "globex", "globex"] {
        q.queue(&src)
            .partition("mixed")
            .push(serde_json::json!({ "tenant": tenant }))
            .await
            .unwrap();
    }

    let out = drain_until(&q, &sink, Duration::from_secs(25), |m| m.len() >= 4).await;
    handle.stop().await.unwrap();

    assert_eq!(out.len(), 4, "not every message was routed");
    for m in &out {
        let tenant = m.data["tenant"].as_str().unwrap_or("");
        assert_eq!(
            m.partition.as_str(),
            tenant,
            "a message for {tenant} landed in partition {}",
            m.partition
        );
    }

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

// ============================================================================
// Terminals that fail
// ============================================================================

#[tokio::test]
async fn a_failing_foreach_aborts_the_cycle_before_the_ack() {
    let q = broker!();
    let src = unique("st-foreach-err");
    create_queue(&q, &src, replayable_source()).await;
    let query = unique("st-foreach-err-q");

    // The effect runs before the commit, so a user error has to abort the whole
    // cycle: the message stays claimed and comes back. The existing foreach test
    // always returns Ok, which never touches this path — and a swallowed error
    // would look identical from the outside except that the work is silently
    // lost.
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);

    let handle = Stream::from(q.queue(&src))
        .foreach(move |_value, _ctx| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err::<(), String>("the effect failed".to_string())
            }
        })
        .run(
            &q,
            RunOptions::new(&query)
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

    // Two calls means the message was redelivered, which only happens if the
    // first cycle committed nothing.
    let deadline = std::time::Instant::now() + Duration::from_secs(25);
    while calls.load(Ordering::SeqCst) < 2 && std::time::Instant::now() < deadline {
        sleep_ms(200).await;
    }
    let metrics = handle.metrics();
    handle.stop().await.unwrap();

    assert!(
        calls.load(Ordering::SeqCst) >= 2,
        "the effect ran {} time(s): a failed effect was acked instead of retried",
        calls.load(Ordering::SeqCst)
    );
    assert!(
        metrics.errors > 0,
        "a failing effect was not counted as a cycle error: {metrics:?}"
    );
    assert_eq!(metrics.push_items, 0, "{metrics:?}");

    let back = pop_retry(&q, &src, Some(&stream_group(&query)), 10, 30).await;
    assert!(
        !back.is_empty(),
        "the message was settled even though the effect never succeeded"
    );

    drop_queue(&q, &src).await;
}

// ============================================================================
// RunOptions
// ============================================================================

#[tokio::test]
async fn a_named_consumer_group_replaces_the_default_streams_group() {
    let q = broker!();
    let src = unique("st-cgroup");
    let sink = unique("st-cgroup-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;
    let query = unique("st-cgroup-q");
    let group = unique("st-cgroup-g");

    let handle = Stream::from(q.queue(&src))
        .map(|r| r.data.clone())
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(&query)
                .reset(true)
                .consumer_group(&group)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    for i in 0..3 {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "i": i }))
            .await
            .unwrap();
    }

    let out = drain_until(&q, &sink, Duration::from_secs(25), |m| m.len() >= 3).await;
    handle.stop().await.unwrap();
    assert_eq!(out.len(), 3, "the stream did not consume the source");

    // The named group's cursor is past the batch...
    let under_named = pop_retry(&q, &src, Some(&group), 10, 12).await;
    assert!(
        under_named.is_empty(),
        "{} messages are still pending for the named group, so the stream acked \
         under a different one",
        under_named.len()
    );
    // ...while the default name never saw them, which is what makes the check
    // above meaningful: the messages are still there, they are simply settled
    // for the group the stream was told to use.
    let under_default = pop_retry(&q, &src, Some(&stream_group(&query)), 10, 12).await;
    assert_eq!(
        under_default.len(),
        3,
        "the stream claimed under the default group after being given a name"
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn a_stream_can_start_at_the_tail_instead_of_replaying_the_backlog() {
    let q = broker!();
    let src = unique("st-subseed");
    let sink_new = unique("st-subseed-new");
    let sink_now = unique("st-subseed-now");
    for name in [&src, &sink_new, &sink_now] {
        create_queue(&q, name, QueueOptions::default()).await;
    }

    // A backlog that predates both queries. Neither of these two RunOptions
    // setters had a call site, and the default ("all") would replay all of it.
    for i in 0..3 {
        q.queue(&src)
            .partition("one")
            .push(serde_json::json!({ "era": "old", "i": i }))
            .await
            .unwrap();
    }
    sleep_ms(1500).await;

    let first = Stream::from(q.queue(&src))
        .map(|r| r.data.clone())
        .to(q.queue(&sink_new))
        .run(
            &q,
            RunOptions::new(unique("st-subseed-qa"))
                .reset(true)
                .subscription_mode(SubscriptionMode::New)
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();
    // The cursor is seeded the first time the group meets the partition, so the
    // marker has to be pushed after that first poll.
    sleep_ms(2000).await;
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "era": "new" }))
        .await
        .unwrap();

    let out = drain_until(&q, &sink_new, Duration::from_secs(25), |m| {
        m.iter().any(|x| x.data["era"] == "new")
    })
    .await;
    first.stop().await.unwrap();

    assert!(
        out.iter().any(|m| m.data["era"] == "new"),
        "the message pushed after the subscription never arrived: {out:?}"
    );
    assert!(
        !out.iter().any(|m| m.data["era"] == "old"),
        "subscription_mode(New) replayed the backlog: {out:?}"
    );

    // subscription_from("now") seeds the same way for a group that has never
    // met the partition — and by now the backlog includes the "new" marker too.
    let second = Stream::from(q.queue(&src))
        .map(|r| r.data.clone())
        .to(q.queue(&sink_now))
        .run(
            &q,
            RunOptions::new(unique("st-subseed-qb"))
                .reset(true)
                .subscription_from("now")
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();
    sleep_ms(2000).await;
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "era": "newest" }))
        .await
        .unwrap();

    let out = drain_until(&q, &sink_now, Duration::from_secs(25), |m| {
        m.iter().any(|x| x.data["era"] == "newest")
    })
    .await;
    second.stop().await.unwrap();

    assert!(
        out.iter().any(|m| m.data["era"] == "newest"),
        "the message pushed after the subscription never arrived: {out:?}"
    );
    assert!(
        out.iter().all(|m| m.data["era"] == "newest"),
        "subscription_from(\"now\") replayed what was already in the queue: {out:?}"
    );

    for name in [&src, &sink_new, &sink_now] {
        drop_queue(&q, name).await;
    }
}

#[tokio::test]
async fn a_cancel_token_stops_the_runner_like_stop_does() {
    let q = broker!();
    let src = unique("st-cancel");
    let sink = unique("st-cancel-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    // `RunOptions::cancel` is the shutdown path for a process that already has
    // a token wired through it; nothing exercised it, so a runner that only
    // ever looked at its own stop flag would pass every other test.
    let cancel = Cancel::new();
    let handle = Stream::from(q.queue(&src))
        .map(|r| r.data.clone())
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-cancel-q"))
                .reset(true)
                .cancel(cancel.clone())
                .max_wait(Duration::from_millis(200)),
        )
        .await
        .unwrap();

    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "when": "before" }))
        .await
        .unwrap();
    let before = drain_until(&q, &sink, Duration::from_secs(25), |m| !m.is_empty()).await;
    assert!(!before.is_empty(), "the stream never processed anything");

    cancel.cancel();
    // One poll is at most max_wait long, so the loop is out well inside this.
    sleep_ms(1500).await;
    q.queue(&src)
        .partition("one")
        .push(serde_json::json!({ "when": "after" }))
        .await
        .unwrap();

    let after = drain_until(&q, &sink, Duration::from_secs(8), |_| false).await;
    let metrics = handle.metrics();
    // stop() on an already-cancelled runner still has to return cleanly.
    handle.stop().await.unwrap();

    assert!(
        after.is_empty(),
        "the runner kept consuming after its token was cancelled: {after:?}"
    );
    assert_eq!(
        metrics.messages, 1,
        "only the message pushed before the cancel should have been consumed: {metrics:?}"
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

// ============================================================================
// Scale-out and volume
// ============================================================================

#[tokio::test]
async fn two_runners_under_one_query_id_share_the_cursor() {
    let q = broker!();
    let src = unique("st-scaleout");
    let sink = unique("st-scaleout-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;
    let query = unique("st-scaleout-q");

    // This is how a stream scales: same query id, same consumer group, same
    // state. Covered so far only as a sequential restart and as two *different*
    // queries, neither of which would notice a runner that claimed its own
    // cursor and processed every message twice.
    let build = |queen: &queen_mq::Queen| {
        Stream::from(queen.queue(&src))
            .map(|r| r.data.clone())
            .to(queen.queue(&sink))
    };
    let opts = || {
        RunOptions::new(&query)
            .max_partitions(4)
            .batch_size(50)
            .max_wait(Duration::from_millis(300))
    };

    let a = build(&q).run(&q, opts().reset(true)).await.unwrap();
    let b = build(&q).run(&q, opts()).await.unwrap();
    assert_eq!(
        a.query_id(),
        b.query_id(),
        "one query id must register as one query"
    );

    const LANES: usize = 4;
    const PER_LANE: usize = 10;
    for lane in 0..LANES {
        q.queue(&src)
            .partition(format!("lane-{lane}"))
            .push_many((0..PER_LANE).map(|i| serde_json::json!({ "n": lane * PER_LANE + i })))
            .await
            .unwrap();
    }

    let want = LANES * PER_LANE;
    let out = drain_until(&q, &sink, Duration::from_secs(45), |m| m.len() >= want).await;
    let combined = a.metrics().messages + b.metrics().messages;
    a.stop().await.unwrap();
    b.stop().await.unwrap();

    let seen: std::collections::HashSet<i64> =
        out.iter().filter_map(|m| m.data["n"].as_i64()).collect();
    assert_eq!(
        seen.len(),
        want,
        "expected {want} distinct messages, got {} across {} sink messages — \
         a shared cursor means neither loss nor duplication",
        seen.len(),
        out.len()
    );
    assert_eq!(
        combined, want as u64,
        "the two runners consumed {combined} messages between them instead of {want}"
    );

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn a_windowed_stream_totals_hundreds_of_messages_across_lanes_exactly() {
    let q = broker!();
    let src = unique("st-volume");
    let sink = unique("st-volume-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    const LANES: usize = 6;
    const PER_LANE: usize = 50;

    // The window tests with exact totals all run on a handful of messages, so a
    // window that double-counts once per hundred cycles, or a flush that races
    // the loop on a busy partition, would stay invisible. Here the arithmetic is
    // only right if every message lands in exactly one window emit.
    let handle = Stream::from(q.queue(&src))
        .window_tumbling(1)
        .idle_flush_ms(400)
        .aggregate_count("count")
        .aggregate_sum("sum", |r| r.number("amount"))
        .to(q.queue(&sink))
        .run(
            &q,
            RunOptions::new(unique("st-volume-q"))
                .reset(true)
                .max_partitions(8)
                .batch_size(200)
                .max_wait(Duration::from_millis(300)),
        )
        .await
        .unwrap();

    for lane in 0..LANES {
        q.queue(&src)
            .partition(format!("lane-{lane}"))
            .push_many((1..=PER_LANE).map(|i| serde_json::json!({ "amount": i })))
            .await
            .unwrap();
    }

    let want_count = (LANES * PER_LANE) as f64;
    // 1 + 2 + ... + PER_LANE, once per lane.
    let want_sum = (PER_LANE * (PER_LANE + 1) / 2 * LANES) as f64;

    let out = drain_until(&q, &sink, Duration::from_secs(60), |m| {
        sum_field(m, "count") >= want_count
    })
    .await;
    let metrics = handle.metrics();
    handle.stop().await.unwrap();

    assert_eq!(
        sum_field(&out, "count"),
        want_count,
        "counted {} of {want_count} messages across {} window emits",
        sum_field(&out, "count"),
        out.len()
    );
    assert_eq!(
        sum_field(&out, "sum"),
        want_sum,
        "the amounts do not add up: a window was folded twice or missed"
    );
    assert_eq!(
        metrics.messages, want_count as u64,
        "a batch was redelivered, so some messages were folded more than once: {metrics:?}"
    );
    for m in &out {
        assert!(
            m.partition.starts_with("lane-"),
            "an emit landed in partition {}, losing its source lane",
            m.partition
        );
    }

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}
