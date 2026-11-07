//! Feed streams that keep state, against the conformance kit's fake broker:
//! two positions and a replay, snapshots in layers, crashes anywhere, a long
//! close that commits in pieces, tables, shards, live reads, and what a
//! stream leaves in KV for a reader outside it (the records). The behaviours
//! of the JS feed-unit suite (feedSteps, feedSnapshots, feedEventTime,
//! feedRecords), on this SDK.

mod feed_support;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use feed_support::*;
use queen_mq::feed::FeedFrom;
use queen_mq::streams::feed::{
    shard_of, FeedAggregate, FeedSink, FeedSource, FeedStream, FeedWindow, Gather, Join, Pattern,
    Poison, Reset, ShardBy, TableJoin, ViewOptions,
};
use queen_mq::streams::Stream;
use queen_mq::Queen;
use serde_json::{json, Value};

const MIN0: f64 = T0; // T0 is on a minute

fn per_minute(q: &Queen) -> FeedStream {
    Stream::from_feed(
        q,
        FeedSource::new()
            .from(FeedFrom::Earliest)
            .queues(["orders"]),
    )
    .window_tumbling(60.0)
    .aggregate(FeedAggregate::new().count("count"))
    .name("perMinute")
    .to("counts")
}

fn record_pos(r: &Option<Value>) -> u64 {
    r.as_ref().and_then(|v| v["position"].as_u64()).unwrap_or(0)
}

// ------------------------------------------------------ two positions, replay

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_writes_the_position_only_while_windows_stay_open() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    for i in 0..3 {
        b.push("orders", "c1", json!({ "n": i })).await;
    }
    let r = p
        .run(per_minute(&p.queen), fast("pos-only").now(b.clock()))
        .await
        .unwrap();
    until(3000, "the position past the messages", || async {
        record_pos(&b.record("pos-only").await) > 3
    })
    .await;
    let stored = b.record("pos-only").await.unwrap();
    assert_eq!(
        stored["snapshot"]["chunks"], 0,
        "no chunks: the state is not written"
    );
    assert_eq!(
        stored["snapshot"]["position"], 1,
        "the snapshot is the stream's start, where the state was empty"
    );
    let keys: Vec<String> = b
        .kv_keys(NS, "pos-only#")
        .await
        .into_iter()
        .filter(|k| k != "pos-only#about")
        .collect();
    assert!(
        keys.is_empty(),
        "nothing but the position record (and the description): {keys:?}"
    );
    assert_eq!(m(&r, "stateKeys"), 1.0, "the open window is in memory");
    r.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_a_crash_the_next_runner_reads_again_from_the_snapshot_nothing_lost_nothing_twice() {
    let b = Fake::start();
    let mut pa = b.proc();
    b.at(1000.0).await;
    b.push("orders", "c1", json!({})).await;
    b.push("orders", "c1", json!({})).await;
    b.push_at(61_000.0, "orders", "c1", json!({})).await;
    let opts = |id: &str| {
        fast(id)
            .now(b.clock())
            .lock_ttl("1s")
            .snapshot_every_millis(3_600_000.0)
            .on_error(|_| {})
    };
    let a = pa.run(per_minute(&pa.queen), opts("crash")).await.unwrap();
    until(3000, "minute 0 closed and committed", || async {
        !b.messages("counts").await.is_empty() && record_pos(&b.record("crash").await) > 3
    })
    .await;
    assert_eq!(b.payloads("counts").await, [json!({ "count": 2 })]);
    let commits = m(&a, "commits");
    pa.crash();
    b.push_at(62_000.0, "orders", "c1", json!({})).await;
    let pb = b.proc();
    let second = pb.run(per_minute(&pb.queen), opts("crash")).await.unwrap();
    until(6000, "the takeover", || async { role(&second) == "active" }).await;
    assert!(m(&second, "replays") >= 1.0, "it read the feed again");
    assert!(m(&second, "replayEntries") >= 3.0);
    b.push_at(121_000.0, "orders", "other", json!({})).await;
    until(3000, "minute 1", || async {
        b.messages("counts").await.len() >= 2
    })
    .await;
    second.stop().await;
    assert_eq!(
        b.payloads("counts").await,
        [json!({ "count": 2 }), json!({ "count": 2 })],
        "minute 0 once, minute 1 whole"
    );
    assert_eq!(
        m(&a, "commits"),
        commits,
        "the crashed process committed nothing more"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_is_a_full_layer_then_deltas_and_a_restart_loads_them_and_the_tail() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    for i in 0..20 {
        b.push("orders", &format!("c{i}"), json!({})).await;
    }
    let r = p
        .run(
            per_minute(&p.queen),
            fast("snapdue").now(b.clock()).snapshot_max_entries(2),
        )
        .await
        .unwrap();
    until(3000, "a snapshot", || async {
        m(&r, "snapshots") >= 1.0 && m(&r, "entriesRead") >= 20.0
    })
    .await;
    let first = b.record("snapdue").await.unwrap()["snapshot"].clone();
    assert_eq!(first["layers"].as_array().unwrap().len(), 1);
    assert_eq!(
        first["layers"][0]["full"], true,
        "the first snapshot is a full layer"
    );
    for _ in 0..2 {
        b.push("orders", "c0", json!({})).await;
    }
    until(3000, "a delta", || async {
        b.record("snapdue").await.unwrap()["snapshot"]["layers"]
            .as_array()
            .map_or(0, |l| l.len())
            >= 2
    })
    .await;
    let second = b.record("snapdue").await.unwrap()["snapshot"].clone();
    assert_eq!(
        second["layers"][0]["sid"], first["layers"][0]["sid"],
        "the full layer is still the one under it"
    );
    assert!(second["layers"][1].get("full").is_none(), "a delta");
    assert!(
        second["layers"][1]["bytes"].as_f64().unwrap()
            < second["layers"][0]["bytes"].as_f64().unwrap() / 5.0,
        "the delta holds the changed key only"
    );
    r.stop().await;
    let stored = b.record("snapdue").await.unwrap();
    let named: HashSet<String> = stored["snapshot"]["layers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["sid"].as_str().unwrap().to_string())
        .collect();
    let keys = b.kv_keys(NS, "snapdue#snap#").await;
    assert!(!keys.is_empty());
    for k in &keys {
        let rest = &k["snapdue#snap#".len()..];
        assert!(
            named.contains(&rest[..rest.rfind('#').unwrap()]),
            "only the chunks of named layers: {k}"
        );
    }
    let p2 = b.proc();
    let again = p2
        .run(per_minute(&p2.queen), fast("snapdue").now(b.clock()))
        .await
        .unwrap();
    assert_eq!(
        m(&again, "stateKeys"),
        20.0,
        "the twenty open windows came back"
    );
    b.push_at(61_000.0, "orders", "late", json!({})).await;
    until(3000, "twenty counts", || async {
        b.messages("counts").await.len() >= 20
    })
    .await;
    again.stop().await;
    let counts: HashMap<String, i64> = b
        .messages("counts")
        .await
        .iter()
        .map(|e| {
            (
                e["partition"].as_str().unwrap().to_string(),
                e["payload"]["count"].as_i64().unwrap(),
            )
        })
        .collect();
    for i in 0..20 {
        assert_eq!(counts[&format!("c{i}")], if i == 0 { 3 } else { 1 });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_state_that_did_not_change_moves_the_snapshot_and_an_empty_state_drops_the_chunks() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    b.push("orders", "c1", json!({})).await;
    let r = p
        .run(
            per_minute(&p.queen),
            fast("moved")
                .now(b.clock())
                .snapshot_max_entries(1)
                .position_commit_millis(0.0),
        )
        .await
        .unwrap();
    until(3000, "a snapshot", || async { m(&r, "snapshots") >= 1.0 }).await;
    let sid = b.record("moved").await.unwrap()["snapshot"]["id"].clone();
    let snapshots = m(&r, "snapshots");
    for _ in 0..3 {
        b.push("other-queue", "x", json!({})).await;
    }
    let end = b.dump().await["applied"].as_u64().unwrap() + 1;
    until(3000, "the position at the end", || async {
        record_pos(&b.record("moved").await) >= end
    })
    .await;
    let stored = b.record("moved").await.unwrap();
    assert_eq!(stored["snapshot"]["id"], sid, "the same chunks");
    assert_eq!(
        stored["snapshot"]["position"], stored["position"],
        "named at the new position"
    );
    assert_eq!(m(&r, "snapshots"), snapshots, "nothing written");
    // The window closes: an empty state is a snapshot of no chunks, and the old chunks go.
    b.at(61_000.0).await;
    until(3000, "the window to close", || async {
        !b.messages("counts").await.is_empty()
    })
    .await;
    until(3000, "no chunk left", || async {
        b.record("moved").await.unwrap()["snapshot"]["chunks"] == 0
            && b.kv_keys(NS, "moved#snap#").await.is_empty()
    })
    .await;
    assert_eq!(m(&r, "stateKeys"), 0.0);
    r.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_state_of_thousands_of_keys_in_many_chunks_comes_back_whole_after_a_crash() {
    let b = Fake::start();
    let mut pa = b.proc();
    b.at(1000.0).await;
    let n = 1500;
    for i in (0..n).step_by(50) {
        let items: Vec<(String, Value)> = (0..50)
            .map(|j| (format!("k{}", i + j), json!({ "pad": "x".repeat(40) })))
            .collect();
        let refs: Vec<(&str, &str, Value)> = items
            .iter()
            .map(|(p, d)| ("orders", p.as_str(), d.clone()))
            .collect();
        b.tx(&refs).await;
    }
    let a = pa
        .run(
            per_minute(&pa.queen),
            fast("big")
                .now(b.clock())
                .lock_ttl("1s")
                .snapshot_max_entries(100)
                .snapshot_chunk_chars(2000)
                .on_error(|_| {}),
        )
        .await
        .unwrap();
    until(5000, "a snapshot of everything read", || async {
        m(&a, "entriesRead") >= n as f64 && m(&a, "snapshots") >= 1.0
    })
    .await;
    until(3000, "a snapshot of several chunks", || async {
        b.record("big").await.unwrap()["snapshot"]["chunks"]
            .as_u64()
            .unwrap_or(0)
            > 3
    })
    .await;
    pa.crash();
    let pb = b.proc();
    let second = pb
        .run(
            per_minute(&pb.queen),
            fast("big").now(b.clock()).lock_ttl("1s").on_error(|_| {}),
        )
        .await
        .unwrap();
    until(6000, "the takeover", || async { role(&second) == "active" }).await;
    until(6000, "every key back", || async {
        m(&second, "stateKeys") == n as f64
    })
    .await;
    b.push_at(61_000.0, "orders", "flush", json!({})).await;
    until(8000, "every count", || async {
        b.messages("counts").await.len() >= n
    })
    .await;
    second.stop().await;
    let counts = b.messages("counts").await;
    assert_eq!(counts.len(), n, "one result per key, none twice");
    let parts: HashSet<String> = counts
        .iter()
        .map(|e| e["partition"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(parts.len(), n);
    assert!(counts.iter().all(|e| e["payload"]["count"] == 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_of_the_bucket_format_is_read_once_and_carried_on_as_snapshots() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    let pos = b.push("orders", "c1", json!({})).await;
    let hash = per_minute(&p.queen).chain_hash().unwrap();
    let sk = format!("w\u{1f}{}\u{1f}c1", MIN0 as i64);
    b.kv_put(NS, "legacy", json!({ "position": pos + 1, "queryId": "legacy", "epoch": 0, "buckets": 2, "hash": hash, "clock": T0 + 1000.0 })).await;
    b.kv_put(NS, "legacy#0", json!({ "e": { sk: { "key": "c1", "start": MIN0, "end": MIN0 + 60000.0, "due": MIN0 + 60000.0, "acc": { "count": 5 } } } })).await;
    b.kv_put(NS, "legacy#1", json!({ "e": {} })).await;
    let r = p
        .run(per_minute(&p.queen), fast("legacy").now(b.clock()))
        .await
        .unwrap();
    b.push_at(2000.0, "orders", "c1", json!({})).await;
    until(3000, "a snapshot with chunks", || async {
        b.record("legacy").await.unwrap()["snapshot"]["chunks"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    })
    .await;
    until(2000, "the buckets to go", || async {
        b.kv_keys(NS, "legacy#")
            .await
            .iter()
            .all(|k| k.starts_with("legacy#snap#") || k == "legacy#about")
    })
    .await;
    b.push_at(61_000.0, "orders", "c2", json!({})).await;
    until(3000, "the count", || async {
        !b.messages("counts").await.is_empty()
    })
    .await;
    r.stop().await;
    assert_eq!(
        b.payloads("counts").await[0],
        json!({ "count": 6 }),
        "the 5 from the buckets and the new one"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_cancels_a_long_poll_in_flight_and_still_commits_and_snapshots() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    for i in 0..3 {
        b.push("orders", &format!("c{i}"), json!({})).await;
    }
    let r = p
        .run(
            per_minute(&p.queen),
            fast("prompt-stop")
                .now(b.clock())
                .wait_millis(20_000)
                .commit_every_millis(60_000.0),
        )
        .await
        .unwrap();
    until(3000, "three read", || async { m(&r, "entriesRead") >= 3.0 }).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let t0 = std::time::Instant::now();
    r.stop().await;
    assert!(
        t0.elapsed() < Duration::from_secs(1),
        "stopped in {:?}, not after the 20 s poll",
        t0.elapsed()
    );
    let stored = b.record("prompt-stop").await.unwrap();
    assert!(
        stored["snapshot"]["chunks"].as_u64().unwrap() >= 1,
        "the open windows went into a snapshot"
    );
    assert!(stored["position"].as_u64().unwrap() > 3);
    assert_eq!(stored["stopped"]["reason"], "stop");
}

// ------------------------------------------------------ a long close in pieces

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn twelve_thousand_windows_that_end_at_one_time_commit_in_pieces_each_result_once() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    let n = 12_000;
    for i in (0..n).step_by(500) {
        let items: Vec<String> = (i..i + 500).map(|k| format!("c{k}")).collect();
        let refs: Vec<(&str, &str, Value)> = items
            .iter()
            .map(|q| ("orders", q.as_str(), json!({})))
            .collect();
        b.tx(&refs).await;
    }
    let r = p
        .run(
            per_minute(&p.queen),
            fast("burst")
                .now(b.clock())
                .max_pending_outputs(1000)
                .snapshot_every_millis(3_600_000.0),
        )
        .await
        .unwrap();
    until(15_000, "everything read", || async {
        m(&r, "entriesRead") >= n as f64
    })
    .await;
    b.push_at(61_000.0, "orders", "late", json!({})).await;
    until(30_000, "every window closed", || async {
        b.messages("counts").await.len() >= n
    })
    .await;
    r.stop().await;
    let keys: Vec<String> = b
        .messages("counts")
        .await
        .iter()
        .map(|e| e["partition"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(keys.len(), n);
    assert_eq!(
        keys.iter().collect::<HashSet<_>>().len(),
        n,
        "no result twice"
    );
    assert!(
        m(&r, "closeCommits") >= 5.0,
        "commits in the middle of the close ({})",
        m(&r, "closeCommits")
    );
    assert_eq!(
        b.record("burst").await.unwrap()["units"],
        n,
        "the record counts the closes"
    );
}

async fn burst_crash(snapshot_every: f64) {
    let b = Fake::start();
    let mut pa = b.proc();
    b.at(1000.0).await;
    let n = 20_000;
    for i in (0..n).step_by(500) {
        let items: Vec<String> = (i..i + 500).map(|k| format!("c{k}")).collect();
        let refs: Vec<(&str, &str, Value)> = items
            .iter()
            .map(|q| ("orders", q.as_str(), json!({})))
            .collect();
        b.tx(&refs).await;
    }
    let opts = || {
        fast("burstcrash")
            .now(b.clock())
            .lock_ttl("1s")
            .max_pending_outputs(400)
            .snapshot_every_millis(snapshot_every)
            .on_error(|_| {})
    };
    let a = pa.run(per_minute(&pa.queen), opts()).await.unwrap();
    until(15_000, "everything read", || async {
        m(&a, "entriesRead") >= n as f64
    })
    .await;
    b.push_at(61_000.0, "orders", "late", json!({})).await;
    until(15_000, "commits in the middle of the close", || async {
        m(&a, "closeCommits") >= 3.0
    })
    .await;
    pa.crash();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let stored = b.record("burstcrash").await.unwrap();
    let units = stored["units"].as_u64().unwrap() as usize;
    assert!(
        units > 0 && units < n,
        "the record names how far the close got ({units} of {n})"
    );
    assert_eq!(
        b.messages("counts").await.len(),
        units,
        "what is committed is what the record counts"
    );
    let pb = b.proc();
    let second = pb.run(per_minute(&pb.queen), opts()).await.unwrap();
    until(8000, "the takeover", || async { role(&second) == "active" }).await;
    until(30_000, "the rest of the close", || async {
        b.messages("counts").await.len() >= n
    })
    .await;
    second.stop().await;
    let keys: Vec<String> = b
        .messages("counts")
        .await
        .iter()
        .map(|e| e["partition"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(keys.len(), n, "every window once");
    assert_eq!(
        keys.iter().collect::<HashSet<_>>().len(),
        n,
        "no result twice"
    );
    assert_eq!(b.record("burstcrash").await.unwrap()["units"], n);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_in_the_middle_of_a_long_close_read_again_from_the_start() {
    burst_crash(3_600_000.0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_in_the_middle_of_a_long_close_with_snapshots_in_the_middle() {
    burst_crash(1.0).await;
}

// ------------------------------------------------------------- the layers

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wait_that_matched_after_the_full_layer_is_a_delete_in_the_next_delta() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    for i in 0..20 {
        b.push("orders", &format!("o{i}"), json!({ "start": true }))
            .await;
    }
    let waits = |q: &Queen| {
        Stream::from_feed(
            q,
            FeedSource::new()
                .from(FeedFrom::Earliest)
                .queues(["orders"]),
        )
        .pattern(
            Pattern::new(
                |m| m["data"]["start"] == true,
                |m| m["data"]["paid"] == true,
                "1h",
            )
            .on_match(|s, _| json!({ "paid": s["partition"] })),
        )
        .to("paid")
    };
    let r = p
        .run(
            waits(&p.queen),
            fast("dels").now(b.clock()).snapshot_max_entries(1),
        )
        .await
        .unwrap();
    until(3000, "a snapshot", || async {
        m(&r, "entriesRead") >= 20.0 && m(&r, "snapshots") >= 1.0
    })
    .await;
    let base = b.record("dels").await.unwrap()["snapshot"]["layers"][0]["sid"].clone();
    b.push("orders", "o3", json!({ "paid": true })).await;
    b.push("orders", "o7", json!({ "paid": true })).await;
    until(3000, "two matches and a delta", || async {
        b.messages("paid").await.len() >= 2
            && b.record("dels").await.unwrap()["snapshot"]["layers"]
                .as_array()
                .map_or(0, |l| l.len())
                >= 2
    })
    .await;
    r.stop().await;
    assert_eq!(
        b.record("dels").await.unwrap()["snapshot"]["layers"][0]["sid"],
        base,
        "the full layer stays under the deltas"
    );
    let p2 = b.proc();
    let again = p2
        .run(waits(&p2.queen), fast("dels").now(b.clock()))
        .await
        .unwrap();
    assert_eq!(
        m(&again, "open"),
        18.0,
        "eighteen waits came back, not the two that matched"
    );
    b.push("orders", "o3", json!({ "paid": true })).await;
    b.push("orders", "o4", json!({ "paid": true })).await;
    until(3000, "the third match", || async {
        b.messages("paid").await.len() >= 3
    })
    .await;
    again.stop().await;
    let mut paid: Vec<String> = b
        .payloads("paid")
        .await
        .iter()
        .map(|p| p["paid"].as_str().unwrap().to_string())
        .collect();
    paid.sort();
    assert_eq!(paid, ["o3", "o4", "o7"], "o3 matched once, o4 matched now");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn when_the_deltas_weigh_as_much_as_the_full_layer_a_full_layer_replaces_them() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    for i in 0..4 {
        b.push("orders", &format!("c{i}"), json!({})).await;
    }
    let r = p
        .run(
            per_minute(&p.queen),
            fast("compact").now(b.clock()).snapshot_max_entries(1),
        )
        .await
        .unwrap();
    until(3000, "a snapshot", || async {
        m(&r, "snapshots") >= 1.0 && m(&r, "entriesRead") >= 4.0
    })
    .await;
    let first_base = b.record("compact").await.unwrap()["snapshot"]["layers"][0]["sid"].clone();
    let mut saw_deltas = 0;
    for i in 4..40 {
        b.push("orders", &format!("c{i}"), json!({})).await;
        until(3000, "the message read", || async {
            m(&r, "entriesRead") >= (i + 1) as f64
        })
        .await;
        let n = b.record("compact").await.unwrap()["snapshot"]["layers"]
            .as_array()
            .map_or(0, |l| l.len());
        saw_deltas = saw_deltas.max(n.saturating_sub(1));
    }
    until(3000, "a new full layer", || async {
        let s = b.record("compact").await.unwrap()["snapshot"].clone();
        s["layers"][0]["sid"] != first_base && s["layers"][0]["full"] == true
    })
    .await;
    r.stop().await;
    assert!(saw_deltas >= 1, "deltas came before it");
    assert!(
        m(&r, "snapshotsFull") >= 2.0,
        "the first full layer and at least one more"
    );
    let snap = b.record("compact").await.unwrap()["snapshot"].clone();
    let named: HashSet<String> = snap["layers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["sid"].as_str().unwrap().to_string())
        .collect();
    for k in b.kv_keys(NS, "compact#snap#").await {
        let rest = &k["compact#snap#".len()..];
        let sid = &rest[..rest.rfind('#').unwrap()];
        assert!(
            named.contains(sid),
            "the replaced layers are gone from KV: {k}"
        );
        assert_ne!(json!(sid), first_base, "the first full layer is gone");
    }
    let p2 = b.proc();
    let again = p2
        .run(per_minute(&p2.queen), fast("compact").now(b.clock()))
        .await
        .unwrap();
    assert_eq!(m(&again, "stateKeys"), 40.0, "all forty windows came back");
    again.stop().await;
}

// ----------------------------------------------------------- steps and tables

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_later_step_whose_function_keeps_failing_on_a_result_skips_that_result() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    for part in ["c1", "c2", "c2"] {
        b.push("orders", part, json!({})).await;
    }
    let s = Stream::from_feed(
        &p.queen,
        FeedSource::new()
            .from(FeedFrom::Earliest)
            .queues(["orders"]),
    )
    .window_tumbling(60.0)
    .aggregate(FeedAggregate::new().count("count"))
    .key_by(|v| -> Result<Value, String> {
        if v["count"] == 1 {
            return Err("a single order is refused".into());
        }
        Ok(json!("all"))
    })
    .window_tumbling(120.0)
    .aggregate(FeedAggregate::new().field("sum", |v: &Value| v["count"].clone()))
    .to("out");
    let r = p
        .run(
            s,
            fast("poison-step")
                .now(b.clock())
                .on_poison(Poison::Skip)
                .on_error(|_| {}),
        )
        .await
        .unwrap();
    b.push_at(61_000.0, "orders", "x", json!({})).await;
    b.push_at(121_000.0, "orders", "x", json!({})).await;
    until(3000, "the sum", || async {
        !b.messages("out").await.is_empty()
    })
    .await;
    r.stop().await;
    assert_eq!(
        b.payloads("out").await,
        [json!({ "sum": 2 })],
        "c1 failed and was skipped; c2 counted"
    );
    assert!(m(&r, "skipped") >= 1.0);
}

fn bookings(q: &Queen, from: FeedFrom, table: TableJoin) -> FeedStream {
    Stream::from_feed(q, FeedSource::new().from(from).queues(["bookings"]))
        .join_table(table)
        .to("out")
}

fn settings() -> TableJoin {
    TableJoin::new("settings", |m, _| m["data"]["property"].clone()).select(|v, row, _| {
        json!({ "id": v.unwrap()["id"], "currency": row.map(|r| r["currency"].clone()).unwrap_or(Value::Null) })
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_stream_first_reads_the_table_queue_and_initial_now_starts_empty() {
    let b = Fake::start();
    let p = b.proc();
    b.push("settings", "p1", json!({ "currency": "EUR" })).await;
    b.push("bookings", "x", json!({ "id": 0, "property": "p1" }))
        .await;
    let r = p
        .run(
            bookings(&p.queen, FeedFrom::Now, settings().missing("drop")),
            fast("prelude"),
        )
        .await
        .unwrap();
    b.push("bookings", "x", json!({ "id": 1, "property": "p1" }))
        .await;
    b.push("bookings", "x", json!({ "id": 2, "property": "nope" }))
        .await;
    b.push("bookings", "x", json!({ "id": 3, "property": "p1" }))
        .await;
    until(3000, "two outputs", || async {
        b.messages("out").await.len() >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    r.stop().await;
    assert_eq!(
        b.payloads("out").await,
        [
            json!({ "id": 1, "currency": "EUR" }),
            json!({ "id": 3, "currency": "EUR" })
        ]
    );
    assert!(m(&r, "tableRows") >= 1.0);

    let b = Fake::start();
    let p = b.proc();
    b.push("settings", "p1", json!({ "currency": "EUR" })).await;
    let r = p
        .run(
            bookings(&p.queen, FeedFrom::Now, settings().initial("now")),
            fast("table-now"),
        )
        .await
        .unwrap();
    b.push("bookings", "x", json!({ "id": 1, "property": "p1" }))
        .await;
    b.push("settings", "p1", json!({ "currency": "CHF" })).await;
    b.push("bookings", "x", json!({ "id": 2, "property": "p1" }))
        .await;
    until(3000, "two outputs", || async {
        b.messages("out").await.len() >= 2
    })
    .await;
    r.stop().await;
    assert_eq!(
        b.payloads("out").await,
        [
            json!({ "id": 1, "currency": null }),
            json!({ "id": 2, "currency": "CHF" })
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_where_of_the_stream_does_not_filter_the_tables_rows_and_the_table_survives_a_restart() {
    let b = Fake::start();
    let (p1, p2) = (b.proc(), b.proc());
    b.push("settings", "p1", json!({ "currency": "EUR" })).await;
    b.push(
        "bookings",
        "x",
        json!({ "id": 1, "property": "p1", "vip": true }),
    )
    .await;
    b.push(
        "bookings",
        "x",
        json!({ "id": 2, "property": "p1", "vip": false }),
    )
    .await;
    let chain = |q: &Queen| {
        Stream::from_feed(
            q,
            FeedSource::new()
                .from(FeedFrom::Earliest)
                .queues(["bookings"]),
        )
        .where_("data.vip", "=", json!(true))
        .join_table(settings())
        .to("out")
    };
    let first = p1.run(chain(&p1.queen), fast("table-where")).await.unwrap();
    until(3000, "one output", || async {
        !b.messages("out").await.is_empty()
    })
    .await;
    first.stop().await;
    b.push(
        "bookings",
        "x",
        json!({ "id": 3, "property": "p1", "vip": true }),
    )
    .await;
    let second = p2.run(chain(&p2.queen), fast("table-where")).await.unwrap();
    until(3000, "two outputs", || async {
        b.messages("out").await.len() >= 2
    })
    .await;
    second.stop().await;
    assert_eq!(
        b.payloads("out").await,
        [
            json!({ "id": 1, "currency": "EUR" }),
            json!({ "id": 3, "currency": "EUR" })
        ]
    );
}

// ------------------------------------------------------------------ shards

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shards_read_every_partition_once_each_with_its_own_position_and_layout() {
    let b = Fake::start();
    let p = b.proc();
    for i in 0..30 {
        for j in 0..2 {
            b.push("orders", &format!("c{i}"), json!({ "i": i, "j": j }))
                .await;
        }
    }
    let s = Stream::from_feed(
        &p.queen,
        FeedSource::new()
            .from(FeedFrom::Earliest)
            .queues(["orders"]),
    )
    .map(|m, _| m["data"].clone())
    .to("copy");
    let g = p.run(s, fast("sharded").shards(3)).await.unwrap();
    until(5000, "60 copies", || async {
        b.messages("copy").await.len() >= 60
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    g.stop().await;
    let copy = b.messages("copy").await;
    assert_eq!(copy.len(), 60, "each message once");
    assert_eq!(
        copy.iter()
            .map(|e| e["transactionId"].as_str().unwrap())
            .collect::<HashSet<_>>()
            .len(),
        60
    );
    for s in 0..3 {
        assert!(
            record_pos(&b.record(&format!("sharded#shard{s}of3")).await) > 1,
            "shard {s} has a position"
        );
    }
    assert_eq!(b.record("sharded#layout").await.unwrap()["shards"], 3);
    let mm = g.metrics();
    assert_eq!(mm["shards"], 3);
    assert_eq!(mm["outputs"], 60);
    for e in &copy {
        assert_eq!(
            shard_of(e["partition"].as_str().unwrap(), 3),
            shard_of(&format!("c{}", e["payload"]["i"]), 3)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_by_a_key_gives_windows_per_tenant_across_shards() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    for i in 0..40 {
        b.push(
            "clicks",
            &format!("p{}", i % 7),
            json!({ "tenant": format!("t{}", i % 5) }),
        )
        .await;
    }
    let s = Stream::from_feed(
        &p.queen,
        FeedSource::new()
            .from(FeedFrom::Earliest)
            .queues(["clicks"]),
    )
    .key_by(|m| m["data"]["tenant"].clone())
    .window_tumbling(60.0)
    .aggregate(FeedAggregate::new().count("count"))
    .map(|v, ctx| json!({ "tenant": ctx.unwrap()["key"], "count": v["count"] }))
    .to("per-tenant");
    let g = p
        .run(
            s,
            fast("by-tenant")
                .shards(4)
                .shard_by(ShardBy::key(|m| m["data"]["tenant"].clone()))
                .now(b.clock()),
        )
        .await
        .unwrap();
    b.push_at(61_000.0, "clicks", "p0", json!({ "tenant": "flush" }))
        .await;
    until(5000, "five tenants", || async {
        b.messages("per-tenant").await.len() >= 5
    })
    .await;
    g.stop().await;
    let mut out = b.payloads("per-tenant").await;
    out.sort_by_key(|v| v["tenant"].as_str().unwrap().to_string());
    assert_eq!(
        out,
        (0..5)
            .map(|t| json!({ "tenant": format!("t{t}"), "count": 8 }))
            .collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_different_shard_count_is_refused_and_a_reset_starts_the_shards_over() {
    let b = Fake::start();
    let p = b.proc();
    b.push("orders", "c1", json!({})).await;
    let chain = |q: &Queen| {
        Stream::from_feed(
            q,
            FeedSource::new()
                .from(FeedFrom::Earliest)
                .queues(["orders"]),
        )
        .to("copy")
    };
    let g = p
        .run(chain(&p.queen), fast("layout").shards(3))
        .await
        .unwrap();
    until(3000, "the copy", || async {
        !b.messages("copy").await.is_empty()
    })
    .await;
    g.stop().await;
    let e = p
        .run(chain(&p.queen), fast("layout").shards(2))
        .await
        .unwrap_err();
    assert_eq!(e.code().map(|c| c.as_str()), Some("feed_layout_changed"));
    let e = p.run(chain(&p.queen), fast("layout")).await.unwrap_err();
    assert_eq!(e.code().map(|c| c.as_str()), Some("feed_layout_changed"));
    let again = p
        .run(
            chain(&p.queen),
            fast("layout").shards(2).reset(Reset::Token("two".into())),
        )
        .await
        .unwrap();
    again.stop().await;
    assert_eq!(b.record("layout#layout").await.unwrap()["shards"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_process_waits_on_every_shard_and_takes_them_over_and_every_shard_sees_the_table()
{
    let b = Fake::start();
    let (pa, pb) = (b.proc(), b.proc());
    for i in 0..12 {
        b.push("orders", &format!("c{i}"), json!({ "n": i })).await;
    }
    let chain = |q: &Queen| {
        Stream::from_feed(
            q,
            FeedSource::new()
                .from(FeedFrom::Earliest)
                .queues(["orders"]),
        )
        .to("copy")
    };
    let a = pa
        .run(chain(&pa.queen), fast("standby-shards").shards(3))
        .await
        .unwrap();
    let bb = pb
        .run(chain(&pb.queen), fast("standby-shards").shards(3))
        .await
        .unwrap();
    until(5000, "12 copies", || async {
        b.messages("copy").await.len() >= 12
    })
    .await;
    assert_eq!(bb.metrics()["running"], 0, "all standby");
    a.stop().await;
    for i in 12..24 {
        b.push("orders", &format!("c{i}"), json!({ "n": i })).await;
    }
    until(6000, "24 copies", || async {
        b.messages("copy").await.len() >= 24
    })
    .await;
    bb.stop().await;
    let mut ns: Vec<i64> = b
        .payloads("copy")
        .await
        .iter()
        .map(|p| p["n"].as_i64().unwrap())
        .collect();
    ns.sort();
    assert_eq!(ns, (0..24).collect::<Vec<_>>());

    let b = Fake::start();
    let p = b.proc();
    b.push("settings", "p1", json!({ "currency": "EUR" })).await;
    for i in 0..20 {
        b.push(
            "bookings",
            &format!("b{i}"),
            json!({ "id": i, "property": "p1" }),
        )
        .await;
    }
    let g = p
        .run(
            bookings(&p.queen, FeedFrom::Earliest, settings()),
            fast("shard-table").shards(3),
        )
        .await
        .unwrap();
    until(5000, "20 outputs", || async {
        b.messages("out").await.len() >= 20
    })
    .await;
    g.stop().await;
    assert!(b
        .payloads("out")
        .await
        .iter()
        .all(|p| p["currency"] == "EUR"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shards_never_split_a_table_queue_the_streams_queues_also_name() {
    for queues in [vec!["acme.*"], vec!["acme.bookings", "acme.settings"]] {
        let mut bk = 0;
        while shard_of(&format!("b{bk}"), 2) != 0 {
            bk += 1;
        }
        let mut pr = 0;
        while shard_of(&format!("p{pr}"), 2) != 1 {
            pr += 1;
        }
        let b = Fake::start();
        let p = b.proc();
        b.push(
            "acme.settings",
            &format!("p{pr}"),
            json!({ "currency": "EUR" }),
        )
        .await;
        b.push(
            "acme.bookings",
            &format!("b{bk}"),
            json!({ "id": 1, "property": format!("p{pr}") }),
        )
        .await;
        let s = Stream::from_feed(&p.queen, FeedSource::new().from(FeedFrom::Earliest).queues(queues.clone()))
            .join_table(TableJoin::new("acme.settings", |m, _| m["data"]["property"].clone()).select(|v, row, _| {
                json!({ "id": v.unwrap()["id"], "currency": row.map(|r| r["currency"].clone()).unwrap_or(Value::Null) })
            }))
            .to("out");
        let g = p
            .run(s, fast(&format!("t-{}", queues.len())).shards(2))
            .await
            .unwrap();
        until(3000, "the output", || async {
            !b.messages("out").await.is_empty()
        })
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        g.stop().await;
        assert_eq!(
            b.payloads("out").await,
            [json!({ "id": 1, "currency": "EUR" })],
            "{queues:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_process_still_on_the_old_layout_commits_nothing_once_a_reset_changed_it_and_stops() {
    let b = Fake::start();
    let (pold, pnew) = (b.proc(), b.proc());
    for i in 0..6 {
        b.push("orders", &format!("c{i}"), json!({ "n": i })).await;
    }
    let chain = |q: &Queen| {
        Stream::from_feed(
            q,
            FeedSource::new()
                .from(FeedFrom::Earliest)
                .queues(["orders"]),
        )
        .to("copy")
    };
    let old = pold
        .run(
            chain(&pold.queen),
            fast("relayout")
                .shards(2)
                .position_commit_millis(50.0)
                .on_error(|_| {}),
        )
        .await
        .unwrap();
    until(3000, "six copies", || async {
        b.messages("copy").await.len() >= 6
    })
    .await;
    let fresh = pnew
        .run(
            chain(&pnew.queen),
            fast("relayout")
                .shards(3)
                .reset(Reset::Token("three".into()))
                .on_error(|_| {}),
        )
        .await
        .unwrap();
    b.push("orders", "c9", json!({ "n": 9 })).await;
    until(4000, "the old shards to stop", || async {
        old.metrics()["perShard"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["role"] == "stopped")
    })
    .await;
    until(3000, "the nine", || async {
        b.payloads("copy").await.iter().any(|p| p["n"] == 9)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    fresh.stop().await;
    let nines = b
        .payloads("copy")
        .await
        .iter()
        .filter(|p| p["n"] == 9)
        .count();
    assert_eq!(
        nines, 1,
        "the new message once: only the new layout committed it"
    );
    assert!(old.metrics()["perShard"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| m["stopped"]["error"]
            .as_str()
            .is_some_and(|e| e.contains("layout"))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shard_by_that_throws_meets_the_poison_policy_in_shard_0_only() {
    let b = Fake::start();
    let p = b.proc();
    for i in 0..6 {
        b.push("orders", &format!("c{i}"), json!({ "n": i })).await;
    }
    let skipped_in = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let s2 = std::sync::Arc::clone(&skipped_in);
    let g = p
        .run(
            Stream::from_feed(
                &p.queen,
                FeedSource::new()
                    .from(FeedFrom::Earliest)
                    .queues(["orders"]),
            )
            .to("copy"),
            fast("bad-shard-key")
                .shards(3)
                .shard_by(ShardBy::key(|m| -> Result<Value, String> {
                    if m["data"]["n"] == 4 {
                        return Err("no key".into());
                    }
                    Ok(json!(format!("k{}", m["data"]["n"])))
                }))
                .on_poison(Poison::Skip)
                .on_error(move |info| {
                    if info.ctx["action"] == "skipped" {
                        s2.lock().unwrap().push(info.ctx["shard"]["index"].clone());
                    }
                }),
        )
        .await
        .unwrap();
    until(5000, "five copies", || async {
        b.messages("copy").await.len() >= 5
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    g.stop().await;
    let mut ns: Vec<i64> = b
        .payloads("copy")
        .await
        .iter()
        .map(|p| p["n"].as_i64().unwrap())
        .collect();
    ns.sort();
    assert_eq!(ns, [0, 1, 2, 3, 5]);
    assert_eq!(
        *skipped_in.lock().unwrap(),
        [json!(0)],
        "one skip, in shard 0"
    );
}

// ------------------------------------------------------------- live reads

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_reads_in_the_process_and_from_outside_on_one_runner_and_on_shards() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    for i in 0..9 {
        b.push("orders", &format!("c{}", i % 3), json!({})).await;
    }
    let r = p
        .run(
            per_minute(&p.queen),
            fast("view-one").now(b.clock()).serve_wait_millis(100),
        )
        .await
        .unwrap();
    until(3000, "nine read", || async { m(&r, "entriesRead") >= 9.0 }).await;
    let one = r.state("perMinute").get("c1");
    assert_eq!(one["found"], true);
    assert_eq!(one["value"]["windows"][0]["value"], json!({ "count": 3 }));
    assert_eq!(
        r.state("perMinute").list(100)["items"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(r.state("perMinute").get("nope")["found"], false);
    assert!(r.state("nope").get("c1")["error"]
        .as_str()
        .unwrap()
        .contains("no step named"));

    let asker = b.proc();
    let view = Stream::view(
        &asker.queen,
        "view-one",
        ViewOptions {
            shards: None,
            timeout_millis: 2000,
        },
    )
    .unwrap();
    let got = view.get("perMinute", "c1").await.unwrap();
    assert_eq!(got["found"], true);
    assert_eq!(got["value"]["windows"][0]["value"]["count"], 3);
    assert_eq!(
        view.list("perMinute", 100).await.unwrap()["items"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    let status = view.status().await.unwrap();
    assert_eq!(status[0]["role"], "active");
    assert!(status[0]["metrics"]["entriesRead"].as_u64().unwrap() >= 9);

    let g = p
        .run(
            Stream::from_feed(
                &p.queen,
                FeedSource::new()
                    .from(FeedFrom::Earliest)
                    .queues(["orders"]),
            )
            .window_tumbling(60.0)
            .aggregate(FeedAggregate::new().count("count"))
            .name("perMinute")
            .to("counts2"),
            fast("view-shards")
                .shards(3)
                .now(b.clock())
                .serve_wait_millis(100),
        )
        .await
        .unwrap();
    until(3000, "nine read", || async { m(&g, "entriesRead") >= 9.0 }).await;
    let sharded = Stream::view(
        &asker.queen,
        "view-shards",
        ViewOptions {
            shards: None,
            timeout_millis: 2000,
        },
    )
    .unwrap();
    for k in ["c0", "c1", "c2"] {
        let r = sharded.get("perMinute", k).await.unwrap();
        assert_eq!(r["found"], true, "{k}");
        assert_eq!(r["shard"], shard_of(k, 3), "{k} answered by its shard");
    }
    assert_eq!(
        sharded.list("perMinute", 100).await.unwrap()["items"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(sharded.status().await.unwrap().len(), 3);
    r.stop().await;
    g.stop().await;

    // A view of a stream nobody runs times out with the reason.
    let nobody = Stream::view(
        &asker.queen,
        "nobody",
        ViewOptions {
            shards: None,
            timeout_millis: 150,
        },
    )
    .unwrap();
    let e = nobody.get("s", "k").await.unwrap_err();
    assert_eq!(e.code().map(|c| c.as_str()), Some("feed_view_timeout"));
}

// ------------------------------------------------------- crashes anywhere

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_result_reaches_the_next_step_at_the_time_its_window_closed_however_late_a_replay_sees_it(
) {
    let chain = |q: &Queen| {
        Stream::from_feed(
            q,
            FeedSource::new()
                .from(FeedFrom::Earliest)
                .queues(["orders"]),
        )
        .window_tumbling(60.0)
        .aggregate(FeedAggregate::new().count("count"))
        .key_by(|_| json!("all"))
        .window_tumbling(60.0)
        .aggregate(FeedAggregate::new().count("windows"))
        .map(|v, ctx| {
            let mut o = v.clone();
            o["minute"] = ctx.unwrap()["windowKey"].clone();
            o
        })
        .to("out")
    };
    let b = Fake::start();
    let mut pa = b.proc();
    b.push_at(1000.0, "orders", "c1", json!({})).await;
    let opts = || {
        fast("late-close")
            .now(b.clock())
            .lock_ttl("1s")
            .snapshot_every_millis(3_600_000.0)
            .on_error(|_| {})
    };
    let a = pa.run(chain(&pa.queen), opts()).await.unwrap();
    until(3000, "one read", || async { m(&a, "entriesRead") >= 1.0 }).await;
    b.at(61_000.0).await;
    until(3000, "the first result in the second step", || async {
        a.metrics()["steps"][1]["open"] == 1
    })
    .await;
    let x = b.push_at(130_000.0, "orders", "x", json!({})).await;
    until(3000, "the first output", || async {
        !b.messages("out").await.is_empty()
    })
    .await;
    until(3000, "the position past x", || async {
        record_pos(&b.record("late-close").await) > x
    })
    .await;
    pa.crash();
    let pb = b.proc();
    let second = pb.run(chain(&pb.queen), opts()).await.unwrap();
    until(6000, "the takeover", || async { role(&second) == "active" }).await;
    b.push_at(250_000.0, "orders", "y", json!({})).await;
    until(4000, "two outputs", || async {
        b.messages("out").await.len() >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    second.stop().await;
    let minutes: Vec<Value> = b
        .payloads("out")
        .await
        .iter()
        .map(|p| p["minute"].clone())
        .collect();
    assert_eq!(
        minutes,
        [json!(iso(MIN0 + 60_000.0)), json!(iso(MIN0 + 180_000.0))],
        "each result once, in the minute its window closed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_windows_chained_crashed_at_random_points_send_what_one_uninterrupted_run_sends() {
    let mut seed: u64 = 7;
    let mut rnd = move || {
        seed = (seed * 1_103_515_245 + 12_345) % 2_147_483_648;
        seed as f64 / 2_147_483_648.0
    };
    let mut msgs = Vec::new();
    let mut t = 0.0;
    for _ in 0..160 {
        t += (rnd() * 4000.0).floor();
        let part = format!("p{}", (rnd() * 9.0).floor());
        let v = (rnd() * 10.0).floor() as i64;
        msgs.push((t, part, v));
    }
    let chain = |q: &Queen| {
        Stream::from_feed(
            q,
            FeedSource::new()
                .from(FeedFrom::Earliest)
                .queues(["events"]),
        )
        .window_tumbling(60.0)
        .aggregate(
            FeedAggregate::new()
                .count("count")
                .field("sum", |v: &Value| v["v"].clone()),
        )
        .key_by(|_| json!("all"))
        .window_tumbling(180.0)
        .aggregate(
            FeedAggregate::new()
                .count("windows")
                .field("total", |v: &Value| v["sum"].clone()),
        )
        .to("out")
    };
    async fn load(b: &Fake, msgs: &[(f64, String, i64)]) {
        for (t, p, v) in msgs {
            b.push_at(*t, "events", p, json!({ "v": v })).await;
        }
    }
    async fn flush(b: &Fake, t: f64) -> u64 {
        b.push_at(t + 400_000.0, "events", "flush", json!({ "v": 0 }))
            .await;
        b.push_at(t + 800_000.0, "events", "flush", json!({ "v": 0 }))
            .await
    }
    let reference = Fake::start();
    load(&reference, &msgs).await;
    let rp = reference.proc();
    let r = rp
        .run(chain(&rp.queen), fast("rand").now(reference.clock()))
        .await
        .unwrap();
    let ref_end = flush(&reference, t).await;
    until(6000, "the reference run", || async {
        record_pos(&reference.record("rand").await) > ref_end
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    r.stop().await;
    let expected = reference.payloads("out").await;
    assert!(expected.len() >= 3);

    let b = Fake::start();
    load(&b, &msgs).await;
    let opts = || {
        fast("rand")
            .now(b.clock())
            .lock_ttl("1s")
            .max_records(7)
            .snapshot_max_entries(25)
            .on_error(|_| {})
    };
    let mut p = b.proc();
    let mut runner = p.run(chain(&p.queen), opts()).await.unwrap();
    for k in 1..=3 {
        let target = (msgs.len() * k / 4) as u64;
        until(6000, "the position", || async {
            record_pos(&b.record("rand").await) >= target
        })
        .await;
        p.crash();
        p = b.proc();
        runner = p.run(chain(&p.queen), opts()).await.unwrap();
        until(6000, "the takeover", || async { role(&runner) == "active" }).await;
    }
    let end = flush(&b, t).await;
    until(8000, "the end", || async {
        record_pos(&b.record("rand").await) > end && b.messages("out").await.len() >= expected.len()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    runner.stop().await;
    assert_eq!(b.payloads("out").await, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replay_leaves_out_the_messages_the_run_before_skipped() {
    let b = Fake::start();
    let mut pa = b.proc();
    b.at(1000.0).await;
    for i in 0..4 {
        b.push("orders", "c1", json!({ "n": i })).await;
    }
    let failing = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let chain = |q: &Queen, f: std::sync::Arc<std::sync::atomic::AtomicBool>| {
        Stream::from_feed(
            q,
            FeedSource::new()
                .from(FeedFrom::Earliest)
                .queues(["orders"]),
        )
        .map(move |m, _| -> Result<Value, String> {
            if f.load(std::sync::atomic::Ordering::SeqCst) && m["data"]["n"] == 2 {
                return Err("a flaky service".into());
            }
            Ok(m["data"].clone())
        })
        .window_tumbling(60.0)
        .aggregate(FeedAggregate::new().count("count"))
        .to("counts")
    };
    let opts = || {
        fast("skips")
            .now(b.clock())
            .lock_ttl("1s")
            .on_poison(Poison::Skip)
            .snapshot_every_millis(3_600_000.0)
            .on_error(|_| {})
    };
    let a = pa
        .run(chain(&pa.queen, failing.clone()), opts())
        .await
        .unwrap();
    until(3000, "the skip committed", || async {
        m(&a, "skipped") == 1.0 && record_pos(&b.record("skips").await) > 4
    })
    .await;
    assert_eq!(
        b.record("skips").await.unwrap()["skips"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "the skip is kept with the position"
    );
    pa.crash();
    failing.store(false, std::sync::atomic::Ordering::SeqCst);
    let pb = b.proc();
    let second = pb
        .run(chain(&pb.queen, failing.clone()), opts())
        .await
        .unwrap();
    until(6000, "the takeover", || async { role(&second) == "active" }).await;
    assert!(m(&second, "replaySkipped") >= 1.0);
    b.push_at(61_000.0, "orders", "x", json!({ "n": 9 })).await;
    until(3000, "the count", || async {
        !b.messages("counts").await.is_empty()
    })
    .await;
    second.stop().await;
    assert_eq!(
        b.payloads("counts").await[0],
        json!({ "count": 3 }),
        "as the first run counted: the skipped message stays out"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn since_without_from_starts_at_the_first_message_since_then() {
    let b = Fake::start();
    let p = b.proc();
    b.push_at(1000.0, "orders", "c1", json!({ "n": 1 })).await;
    b.push_at(5000.0, "orders", "c1", json!({ "n": 2 })).await;
    let since = std::time::UNIX_EPOCH + Duration::from_millis((T0 + 3000.0) as u64);
    let r = p
        .run(
            Stream::from_feed(&p.queen, FeedSource::new().queues(["orders"]).since(since))
                .to("copy"),
            fast("since"),
        )
        .await
        .unwrap();
    until(3000, "the copy", || async {
        !b.messages("copy").await.is_empty()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    r.stop().await;
    assert_eq!(
        b.payloads("copy")
            .await
            .iter()
            .map(|p| p["n"].clone())
            .collect::<Vec<_>>(),
        [json!(2)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gather_by_count_of_results_from_a_window_counts_each_window_once() {
    let b = Fake::start();
    let p = b.proc();
    let s = Stream::from_feed(&p.queen, FeedSource::new().from(FeedFrom::Earliest).queues(["orders"]))
        .window_tumbling(60.0)
        .aggregate(FeedAggregate::new().count("count"))
        .gather(
            Gather::count("10m", |_, _| json!(2))
                .key(|m, _| m["partition"].clone())
                .on_complete(|got, key| json!({ "key": key, "minutes": got.as_array().unwrap().iter().map(|g| g["data"]["count"].clone()).collect::<Vec<_>>() })),
        )
        .to("out");
    let r = p
        .run(s, fast("gather-windows").now(b.clock()))
        .await
        .unwrap();
    b.push_at(1000.0, "orders", "c1", json!({})).await;
    b.push_at(61_000.0, "orders", "c1", json!({})).await;
    b.push("orders", "c1", json!({})).await;
    b.push_at(121_000.0, "orders", "flush", json!({})).await;
    until(3000, "the gathered run", || async {
        !b.messages("out").await.is_empty()
    })
    .await;
    r.stop().await;
    assert_eq!(
        b.payloads("out").await,
        [json!({ "key": "c1", "minutes": [1, 2] })]
    );
}

// ----------------------------------------------------- event time restarts

fn unanswered(q: &Queen, extra: impl Fn(Pattern) -> Pattern) -> FeedStream {
    unanswered_within(q, "1h", extra)
}

fn unanswered_within(q: &Queen, within: &str, extra: impl Fn(Pattern) -> Pattern) -> FeedStream {
    let p = Pattern::new(|m| m["queue"] == "issues", |m| m["queue"] == "comments", within)
        .key(|m, _| json!(format!("{}#{}", m["partition"].as_str().unwrap(), m["data"]["n"])))
        .event_time(|m| m["data"]["at"].clone())
        .on_match(|s, t| json!({ "issue": format!("{}#{}", s["partition"].as_str().unwrap(), s["data"]["n"]), "answered": &t["data"]["at"].as_str().unwrap()[11..16] }))
        .on_timeout(|s| json!({ "issue": format!("{}#{}", s["partition"].as_str().unwrap(), s["data"]["n"]), "unanswered": true }));
    Stream::from_feed(
        q,
        FeedSource::new()
            .from(FeedFrom::Earliest)
            .queues(["issues", "comments"]),
    )
    .pattern(extra(p))
    .name("unanswered")
    .to("out")
}

const H0: f64 = 1_759_312_800_000.0; // 2025-10-01T10:00:00.000Z

fn ev(min: f64, sec: f64) -> String {
    iso(H0 + min * 60_000.0 + sec * 1000.0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_and_a_start_keep_the_held_messages_and_the_watermark() {
    let b = Fake::start();
    let (pa, pb) = (b.proc(), b.proc());
    b.push("issues", "r1", json!({ "n": 1, "at": ev(0.0, 0.0) }))
        .await;
    b.push("issues", "r2", json!({ "n": 1, "at": ev(10.0, 0.0) }))
        .await;
    let first = pa
        .run(
            unanswered(&pa.queen, |p| p.allowed_lateness("30m")),
            fast("restart"),
        )
        .await
        .unwrap();
    until(3000, "two read", || async {
        m(&first, "entriesRead") >= 2.0
    })
    .await;
    assert_eq!(first.metrics()["steps"][0]["held"], 2);
    first.stop().await;
    b.push("comments", "r1", json!({ "n": 1, "at": ev(50.0, 0.0) }))
        .await;
    b.push("issues", "r3", json!({ "n": 1, "at": ev(200.0, 0.0) }))
        .await;
    let second = pb
        .run(
            unanswered(&pb.queen, |p| p.allowed_lateness("30m")),
            fast("restart"),
        )
        .await
        .unwrap();
    until(3000, "two outputs", || async {
        b.messages("out").await.len() >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        b.payloads("out").await,
        [
            json!({ "issue": "r1#1", "answered": "10:50" }),
            json!({ "issue": "r2#1", "unanswered": true })
        ]
    );
    assert_eq!(
        m(&second, "replays"),
        0.0,
        "the stop wrote a snapshot: nothing to read again"
    );
    second.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_message_whose_step_keeps_failing_meets_the_poison_policy_as_the_message_it_is() {
    let b = Fake::start();
    let p = b.proc();
    b.push("issues", "bad", json!({ "n": 1, "at": ev(0.0, 0.0) }))
        .await;
    b.push("issues", "good", json!({ "n": 1, "at": ev(0.0, 0.0) }))
        .await;
    b.push("comments", "bad", json!({ "n": 1, "at": ev(5.0, 0.0) }))
        .await;
    b.push("comments", "good", json!({ "n": 1, "at": ev(6.0, 0.0) }))
        .await;
    b.push("issues", "later", json!({ "n": 1, "at": ev(30.0, 0.0) }))
        .await;
    let s = unanswered(&p.queen, |p| {
        p.allowed_lateness("10m").on_match(|s, _| -> Result<Value, String> {
            if s["partition"] == "bad" {
                return Err("boom".into());
            }
            Ok(json!({ "issue": format!("{}#{}", s["partition"].as_str().unwrap(), s["data"]["n"]), "answered": true }))
        })
    });
    let r = p
        .run(
            s,
            fast("poison-held")
                .on_poison(Poison::DeadLetter("dlq".into()))
                .on_error(|_| {}),
        )
        .await
        .unwrap();
    until(3000, "an output and a dead letter", || async {
        !b.messages("out").await.is_empty() && !b.messages("dlq").await.is_empty()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    r.stop().await;
    assert_eq!(
        b.payloads("out").await,
        [json!({ "issue": "good#1", "answered": true })]
    );
    let record = &b.payloads("dlq").await[0];
    assert_eq!(
        record["entry"]["queue"], "comments",
        "the comment that made the failing match"
    );
    assert_eq!(record["entry"]["partition"], "bad");
    assert_eq!(record["error"]["message"], "boom");
    assert_eq!(m(&r, "deadLettered"), 1.0);
}

// ----------------------------------------------------------------- records

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_description_says_what_the_stream_reads_its_steps_where_results_go_and_its_poison_policy(
) {
    let b = Fake::start();
    let p = b.proc();
    b.push("orders", "c1", json!({ "country": "IT", "total": 1 }))
        .await;
    let chain = |q: &Queen, seconds: f64| {
        Stream::from_feed(
            q,
            FeedSource::new()
                .from(FeedFrom::Earliest)
                .queues(["orders", "pay*"]),
        )
        .where_("data.country", "=", json!("IT"))
        .map(|m, _| m["data"].clone())
        .window_tumbling(seconds)
        .aggregate(
            FeedAggregate::new()
                .count("count")
                .field("sum", |v: &Value| v["total"].clone()),
        )
        .name("perCustomer")
        .to("totals")
    };
    let r = p
        .run(
            chain(&p.queen, 60.0),
            fast("about").on_poison(Poison::DeadLetter("feed-dlq".into())),
        )
        .await
        .unwrap();
    until(3000, "the first commit", || async {
        b.record("about#about").await.is_some()
    })
    .await;
    let about = b.record("about#about").await.unwrap();
    assert_eq!(about["v"], 1);
    assert_eq!(about["queryId"], "about");
    assert!(about["client"]
        .as_str()
        .unwrap()
        .starts_with("client-rust "));
    assert_eq!(about["hash"].as_str().unwrap().len(), 16);
    assert_eq!(
        about["reads"],
        json!({ "queues": ["orders", "pay*"], "partitions": null, "where": [{ "field": "data.country", "op": "=", "value": "IT" }] })
    );
    assert_eq!(about["steps"][0], json!({ "kind": "ops", "ops": ["map"] }));
    let w = &about["steps"][1];
    assert_eq!(w["name"], "perCustomer");
    assert_eq!(w["clock"], "feed");
    assert_eq!(w["window"]["kind"], "window-tumbling");
    assert_eq!(w["window"]["seconds"], 60);
    assert_eq!(
        w["reducer"],
        json!({ "kind": "aggregate", "fields": ["count", "sum"] })
    );
    assert_eq!(
        about["sink"],
        json!({ "kind": "queues", "queues": ["totals"] })
    );
    assert_eq!(about["onPoison"], json!({ "queue": "feed-dlq" }));
    assert_eq!(about["shards"], Value::Null);
    assert_eq!(about["exclusive"], true);
    assert_eq!(about["lockTtl"], "15s");
    // Once per activation; the next deploy's chain replaces it.
    let v1 = b.kv(NS, "about#about").await.unwrap().1;
    b.push("orders", "c2", json!({ "country": "IT", "total": 1 }))
        .await;
    let second_pos = b.messages("orders").await[1]["position"].as_u64().unwrap();
    until(3000, "the next commit", || async {
        record_pos(&b.record("about").await) > second_pos
    })
    .await;
    assert_eq!(
        b.kv(NS, "about#about").await.unwrap().1,
        v1,
        "not at every commit"
    );
    r.stop().await;
    let p2 = b.proc();
    let again = p2
        .run(
            chain(&p2.queen, 10.0),
            fast("about").reset(Reset::Token("v2".into())),
        )
        .await
        .unwrap();
    until(3000, "the new deploy's first commit", || async {
        b.kv(NS, "about#about").await.unwrap().1 > v1
    })
    .await;
    assert_eq!(
        b.record("about#about").await.unwrap()["steps"][1]["window"]["seconds"],
        10
    );
    again.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_description_names_a_table_a_pattern_their_clocks_a_route_and_one_record_for_the_shards(
) {
    let b = Fake::start();
    let p = b.proc();
    b.push("orders", "c1", json!({ "at": iso(T0) })).await;
    let s = Stream::from_feed(
        &p.queen,
        FeedSource::new()
            .from(FeedFrom::Earliest)
            .queues(["orders", "payments"]),
    )
    .join_table(
        TableJoin::new("customers", |m, _| m["partition"].clone()).select(|v, row, _| {
            let mut o = v.cloned().unwrap_or(json!({}));
            o["tier"] = row.map(|r| r["tier"].clone()).unwrap_or(Value::Null);
            o
        }),
    )
    .name("customers")
    .pattern(
        Pattern::new(
            |m| m["queue"] == "orders",
            |m| m["queue"] == "payments",
            "15m",
        )
        .event_time(|m| m["data"]["at"].clone())
        .on_timeout(|s| json!({ "unpaid": s["partition"] })),
    )
    .name("unpaid")
    .gather(
        Gather::steps("1h")
            .step("unpaid", |m, _| m["data"].get("unpaid").is_some())
            .key(|m, _| m["data"]["unpaid"].clone())
            .feed_clock(),
    )
    .name("alerts")
    .to(FeedSink::route(|v: Option<Value>, _m: Value| async move {
        if v.as_ref().is_some_and(|v| v.get("unpaid").is_some()) {
            json!("alerts")
        } else {
            Value::Null
        }
    }));
    let g = p
        .run(s, fast("kinds").shards(2).on_poison(Poison::Skip))
        .await
        .unwrap();
    until(3000, "a shard's first commit", || async {
        b.record("kinds#about").await.is_some()
    })
    .await;
    let about = b.record("kinds#about").await.unwrap();
    let kinds: Vec<(Value, Value)> = about["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| (s["kind"].clone(), s["name"].clone()))
        .collect();
    assert_eq!(
        kinds,
        [
            (json!("table"), json!("customers")),
            (json!("pattern"), json!("unpaid")),
            (json!("gather"), json!("alerts"))
        ]
    );
    assert_eq!(about["steps"][0]["config"]["queues"], json!(["customers"]));
    assert!(
        about["steps"][0].get("clock").is_none(),
        "a table runs on no clock"
    );
    assert_eq!(
        about["steps"][2]["clock"], "feed",
        "feed_clock(), after a step on event time"
    );
    assert_eq!(about["steps"][2]["config"]["parts"], json!(["unpaid"]));
    assert_eq!(about["steps"][1]["clock"], "event");
    assert_eq!(about["steps"][1]["config"]["within"], 15 * 60000);
    assert_eq!(about["reads"]["tables"], json!(["customers"]));
    assert_eq!(about["sink"], json!({ "kind": "route" }));
    assert_eq!(about["onPoison"], "skip");
    assert_eq!(about["shards"], json!({ "count": 2, "by": "partition" }));
    assert_eq!(
        b.kv_keys(NS, "kinds#about").await,
        ["kinds#about"],
        "one description for every shard"
    );
    g.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_record_says_the_feed_time_it_read_up_to_that_it_was_caught_up_and_what_each_step_holds(
) {
    let b = Fake::start();
    let p = b.proc();
    b.push_at(1000.0, "orders", "c1", json!({ "n": 1 })).await;
    b.push_at(2000.0, "orders", "c2", json!({ "n": 2 })).await;
    let r = p
        .run(
            Stream::from_feed(&p.queen, FeedSource::new().from(FeedFrom::Earliest))
                .map(|m, _| m["data"].clone())
                .to("out"),
            fast("read-to").now(b.clock()),
        )
        .await
        .unwrap();
    until(3000, "caught up", || async {
        b.messages("out").await.len() >= 2
            && b.record("read-to")
                .await
                .is_some_and(|r| r["caughtUp"] == true)
    })
    .await;
    let rec = b.record("read-to").await.unwrap();
    assert!(
        rec["readTo"].as_f64().unwrap() >= T0 + 2000.0,
        "at or past the last message read"
    );
    assert!(
        rec.get("open").is_none(),
        "a chain with no state counts nothing"
    );
    assert!(rec.get("stopped").is_none());
    r.stop().await;

    let b = Fake::start();
    let p = b.proc();
    b.push_at(
        1000.0,
        "issues",
        "r1",
        json!({ "n": 1, "at": ev(0.0, 0.0) }),
    )
    .await;
    b.push("issues", "r1", json!({ "n": 2, "at": ev(30.0, 0.0) }))
        .await;
    let r = p
        .run(
            unanswered(&p.queen, |p| p.allowed_lateness("20m")),
            fast("held").now(b.clock()),
        )
        .await
        .unwrap();
    until(3000, "one held", || async {
        r.metrics()["steps"][0]["held"] == 1
    })
    .await;
    until(3000, "the commit after it", || async {
        b.record("held")
            .await
            .is_some_and(|r| r["held"]["unanswered"] == 1)
    })
    .await;
    let rec = b.record("held").await.unwrap();
    assert_eq!(
        rec["open"]["unanswered"], 1,
        "issue 1 waits for its answer; issue 2 waits for the watermark"
    );
    r.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn why_it_stopped_poison_first_long_error_asked_standby_and_before_the_first_read() {
    let b = Fake::start();
    let p = b.proc();
    for i in 1..=2 {
        b.push("orders", &format!("c{i}"), json!({ "n": i })).await;
    }
    let r = p
        .run(
            Stream::from_feed(&p.queen, FeedSource::new().from(FeedFrom::Earliest))
                .map(|_, _| Err::<Value, String>("x".repeat(5000)))
                .to("out"),
            fast("first").on_error(|_| {}),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), r.done())
        .await
        .unwrap();
    let rec = b.record("first").await.unwrap();
    assert_eq!(
        rec["position"],
        b.messages("orders").await[0]["position"],
        "the record still says so, with nothing else to commit"
    );
    assert_eq!(rec["stopped"]["reason"], "poison");
    assert!(
        rec["stopped"]["error"]
            .as_str()
            .unwrap()
            .encode_utf16()
            .count()
            <= 501,
        "a long error is cut"
    );

    // A stop asked by its process says so; the next run's commit drops it.
    let b = Fake::start();
    let (pa, pb) = (b.proc(), b.proc());
    for i in 1..=2 {
        b.push("orders", &format!("c{i}"), json!({ "n": i })).await;
    }
    let first = pa
        .run(
            Stream::from_feed(&pa.queen, FeedSource::new().from(FeedFrom::Earliest)).to("out"),
            fast("asked"),
        )
        .await
        .unwrap();
    until(3000, "two outputs", || async {
        b.messages("out").await.len() >= 2
    })
    .await;
    first.stop().await;
    assert_eq!(
        b.record("asked").await.unwrap()["stopped"]["reason"],
        "stop"
    );
    let second = pb
        .run(
            Stream::from_feed(&pb.queen, FeedSource::new().from(FeedFrom::Earliest)).to("out"),
            fast("asked"),
        )
        .await
        .unwrap();
    b.push("orders", "c3", json!({ "n": 3 })).await;
    until(3000, "three outputs", || async {
        b.messages("out").await.len() >= 3
    })
    .await;
    until(3000, "the new run's commit", || async {
        b.record("asked").await.unwrap().get("stopped").is_none()
    })
    .await;
    second.stop().await;

    // A standby that stops writes nothing.
    let b = Fake::start();
    let (pa, pb) = (b.proc(), b.proc());
    b.push("orders", "c1", json!({ "n": 1 })).await;
    let active = pa
        .run(
            Stream::from_feed(&pa.queen, FeedSource::new().from(FeedFrom::Earliest)).to("out"),
            fast("pair"),
        )
        .await
        .unwrap();
    until(3000, "caught up", || async {
        !b.messages("out").await.is_empty()
            && b.record("pair")
                .await
                .is_some_and(|r| r["caughtUp"] == true)
    })
    .await;
    let standby = pb
        .run(
            Stream::from_feed(&pb.queen, FeedSource::new().from(FeedFrom::Earliest)).to("out"),
            fast("pair"),
        )
        .await
        .unwrap();
    assert_eq!(role(&standby), "standby");
    let v = b.kv(NS, "pair").await.unwrap().1;
    standby.stop().await;
    assert_eq!(b.kv(NS, "pair").await.unwrap().1, v);
    assert!(b.record("pair").await.unwrap().get("stopped").is_none());
    active.stop().await;

    // A stream stopped before its first read writes no record (or one with a position).
    let b = Fake::start();
    let p = b.proc();
    let r = p
        .run(
            Stream::from_feed(&p.queen, FeedSource::new().from(FeedFrom::Earliest)).to("out"),
            fast("early"),
        )
        .await
        .unwrap();
    r.stop().await;
    match b.record("early").await {
        None => {}
        Some(rec) => assert!(rec["position"].is_u64(), "a position or nothing, got {rec}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quiet_caught_up_stream_keeps_its_record_fresh() {
    let b = Fake::start();
    let p = b.proc();
    b.push_at(1000.0, "orders", "c1", json!({})).await;
    let r = p
        .run(
            Stream::from_feed(&p.queen, FeedSource::new().from(FeedFrom::Earliest)).to("out"),
            fast("fresh").now(b.clock()).position_commit_millis(5000.0),
        )
        .await
        .unwrap();
    until(3000, "caught up", || async {
        !b.messages("out").await.is_empty()
            && b.record("fresh")
                .await
                .is_some_and(|r| r["caughtUp"] == true)
    })
    .await;
    let v = b.kv(NS, "fresh").await.unwrap().1;
    b.at(3000.0).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        b.kv(NS, "fresh").await.unwrap().1,
        v,
        "not before positionCommitMillis"
    );
    b.at(7000.0).await;
    // The broker's clock moved; the next commit of the position comes once the log moves.
    b.push("noise", "x", json!({})).await;
    until(3000, "the next position commit", || async {
        b.kv(NS, "fresh").await.unwrap().1 > v
    })
    .await;
    let rec = b.record("fresh").await.unwrap();
    assert_eq!(rec["caughtUp"], true);
    assert_eq!(rec["updatedAt"], json!(iso(T0 + 7000.0)));
    assert_eq!(
        b.record("fresh#about").await.unwrap()["positionCommitMillis"],
        5000
    );
    r.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_window_of_event_time_with_idle_closes_on_a_quiet_feed() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    b.push("readings", "s1", json!({ "at": ev(0.0, 10.0), "v": 1 }))
        .await;
    b.push("readings", "s1", json!({ "at": ev(0.0, 50.0), "v": 2 }))
        .await;
    let s = Stream::from_feed(
        &p.queen,
        FeedSource::new()
            .from(FeedFrom::Earliest)
            .queues(["readings"]),
    )
    .window(
        FeedWindow::tumbling(60.0)
            .event_time(|m| m["data"]["at"].clone())
            .idle("30s"),
    )
    .aggregate(FeedAggregate::new().field("sum", |v: &Value| v["v"].clone()))
    .to("sums");
    let r = p.run(s, fast("idle").now(b.clock())).await.unwrap();
    until(3000, "two read", || async { m(&r, "entriesRead") >= 2.0 }).await;
    b.at(36_000.0).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(b.messages("sums").await.is_empty());
    b.at(42_000.0).await;
    until(3000, "the minute", || async {
        !b.messages("sums").await.is_empty()
    })
    .await;
    assert_eq!(b.payloads("sums").await, [json!({ "sum": 3 })]);
    r.stop().await;
}

// ------------------------------------------------- chains, crashes, takeovers

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chain_of_a_table_join_a_three_sided_join_and_a_window_replays_after_a_crash_to_the_same_results(
) {
    fn chain(q: &Queen) -> FeedStream {
        Stream::from_feed(
            q,
            FeedSource::new()
                .from(FeedFrom::Earliest)
                .queues(["orders", "payments", "shipments"]),
        )
        .join_table(
            TableJoin::new("carriers", |m, _| m["data"]["carrier"].clone()).select(|v, row, _| {
                let mut o = v.cloned().unwrap_or_else(|| json!({}));
                o["speed"] = row
                    .map(|r| r["speed"].clone())
                    .unwrap_or_else(|| json!("unknown"));
                o
            }),
        )
        .join(
            Join::sides("10m")
                .side_queue("order", "orders")
                .side_queue("paid", "payments")
                .side_queue("sent", "shipments")
                // A join keeps the message's payload; keep the table-joined value instead.
                .keep(|_, v| v.cloned().unwrap_or(Value::Null))
                .select_all(
                    |got, key| json!({ "order": key, "speed": got["sent"]["data"]["speed"] }),
                ),
        )
        .key_by(|_| json!("all"))
        .window_tumbling(60.0)
        .aggregate(
            FeedAggregate::new()
                .count("done")
                .field("fast", |v: &Value| {
                    json!(if v["speed"] == "fast" { 1 } else { 0 })
                }),
        )
        .to("done-per-minute")
    }
    async fn load(b: &Fake) {
        b.push_at(500.0, "carriers", "dhl", json!({ "speed": "fast" }))
            .await;
        for i in 0..6 {
            b.at(1000.0 + f64::from(i) * 100.0).await;
            b.push(
                "shipments",
                &format!("o{i}"),
                json!({ "carrier": if i % 2 == 1 { "dhl" } else { "ups" } }),
            )
            .await;
            b.push("orders", &format!("o{i}"), json!({})).await;
            if i != 5 {
                b.push("payments", &format!("o{i}"), json!({})).await;
            }
        }
    }
    // Once without a crash, for the expected results.
    let reference = Fake::start();
    load(&reference).await;
    let rp = reference.proc();
    let r = rp
        .run(chain(&rp.queen), fast("steps").now(reference.clock()))
        .await
        .unwrap();
    reference
        .push_at(61_000.0, "orders", "flush", json!({}))
        .await;
    until(3000, "the reference result", || async {
        !reference.messages("done-per-minute").await.is_empty()
    })
    .await;
    r.stop().await;
    let expected = reference.payloads("done-per-minute").await;
    assert_eq!(expected, [json!({ "done": 5, "fast": 2 })]);
    // Again, with a crash in the middle.
    let b = Fake::start();
    load(&b).await;
    let mut pa = b.proc();
    let a = pa
        .run(
            chain(&pa.queen),
            fast("steps")
                .now(b.clock())
                .lock_ttl("1s")
                .max_records(4)
                .on_error(|_| {}),
        )
        .await
        .unwrap();
    until(3000, "eight read", || async { m(&a, "entriesRead") >= 8.0 }).await;
    pa.crash();
    let pb = b.proc();
    let second = pb
        .run(
            chain(&pb.queen),
            fast("steps").now(b.clock()).lock_ttl("1s").on_error(|_| {}),
        )
        .await
        .unwrap();
    until(6000, "the takeover", || async { role(&second) == "active" }).await;
    b.push_at(61_000.0, "orders", "flush", json!({})).await;
    until(3000, "the result", || async {
        !b.messages("done-per-minute").await.is_empty()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    second.stop().await;
    assert_eq!(b.payloads("done-per-minute").await, expected);
}

fn xstep_chain(q: &Queen) -> FeedStream {
    Stream::from_feed(
        q,
        FeedSource::new()
            .from(FeedFrom::Earliest)
            .queues(["events"]),
    )
    .window_tumbling(10.0)
    .aggregate(FeedAggregate::new().count("count"))
    .key_by(|_| json!("all"))
    .join(
        Join::new(
            |m| m["ctx"]["key"] == "k1",
            |m| m["ctx"]["key"] == "k2",
            "5s",
        )
        .on_timeout(|m, side| json!({ "side": side, "key": m["ctx"]["key"] })),
    )
    .to("out")
}

async fn xstep_scenario(crash_it: bool) -> Vec<Value> {
    let b = Fake::start();
    let mut p = b.proc();
    let opts = || {
        fast("xstep")
            .now(b.clock())
            .lock_ttl("1s")
            .snapshot_every_millis(3_600_000.0)
            .on_error(|_| {})
    };
    b.push_at(1000.0, "events", "k1", json!({})).await;
    b.push_at(11_000.0, "events", "k2", json!({})).await;
    let mut r = p.run(xstep_chain(&p.queen), opts()).await.unwrap();
    until(3000, "k1's result in step 2", || async {
        m(&r, "entriesRead") >= 2.0 && r.metrics()["steps"][1]["open"] == 1
    })
    .await;
    // Quiet: a tick, and step 2's deadline (15 s) passes alone.
    b.at(16_000.0).await;
    until(3000, "the left timeout", || async {
        !b.messages("out").await.is_empty()
    })
    .await;
    // A tick: k2's window closes at 20 s, its result waits in step 2.
    b.at(21_000.0).await;
    until(3000, "k2's result in step 2", || async {
        let mm = r.metrics();
        mm["steps"][1]["open"] == 1 && mm["steps"][0]["open"] == 0
    })
    .await;
    until(3000, "the clock committed", || async {
        b.record("xstep")
            .await
            .and_then(|r| r["clock"].as_f64())
            .unwrap_or(0.0)
            >= T0 + 21_000.0
    })
    .await;
    if crash_it {
        p.crash();
        p = b.proc();
        r = p.run(xstep_chain(&p.queen), opts()).await.unwrap();
        until(6000, "the takeover", || async { role(&r) == "active" }).await;
        assert!(m(&r, "replays") >= 1.0);
    }
    b.push_at(26_000.0, "events", "z", json!({})).await;
    until(3000, "the right timeout", || async {
        b.messages("out").await.len() >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    r.stop().await;
    b.payloads("out").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_later_steps_deadline_that_passed_on_a_tick_closes_before_an_earlier_steps_result_reaches_it_on_a_replay_too(
) {
    let live = xstep_scenario(false).await;
    assert_eq!(
        live,
        [
            json!({ "side": "left", "key": "k1" }),
            json!({ "side": "right", "key": "k2" })
        ]
    );
    assert_eq!(xstep_scenario(true).await, live);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_takeover_after_a_stand_down_keeps_the_state_it_loaded() {
    let b = Fake::start();
    b.at(1000.0).await;
    let chain = |q: &Queen| {
        Stream::from_feed(
            q,
            FeedSource::new()
                .from(FeedFrom::Earliest)
                .queues(["orders"]),
        )
        .window_tumbling(60.0)
        .aggregate(FeedAggregate::new().count("count"))
        .to("counts")
    };
    let opts = || {
        fast("arm")
            .now(b.clock())
            .lock_ttl("1s")
            .retry_delay_millis(20.0)
            .on_error(|_| {})
    };
    // a's process is cut off at its second feed call: its first read, once it holds the lock.
    let proxy = Proxy::start(&b.url).await;
    proxy.cut_at_feed_call(2);
    let pa = proxy.proc();
    let a = pa.run(chain(&pa.queen), opts()).await.unwrap();
    until(15_000, "a stands down", || async { role(&a) == "standby" }).await;
    for i in 0..3 {
        b.push("orders", &format!("c{i}"), json!({})).await;
    }
    let pb = b.proc();
    let other = pb.run(chain(&pb.queen), opts()).await.unwrap();
    until(15_000, "b active with three windows", || async {
        role(&other) == "active" && m(&other, "stateKeys") == 3.0
    })
    .await;
    other.stop().await;
    proxy.cut_at_feed_call(0);
    proxy.cut(false);
    until(20_000, "a takes over", || async { role(&a) == "active" }).await;
    // The log moves (on this fake only pushes take positions): a commits its position,
    // and the snapshot it names is the one a loaded.
    b.push("noise", "x", json!({})).await;
    until(5000, "a commits", || async { m(&a, "commits") >= 1.0 }).await;
    a.stop().await;
    let pc = b.proc();
    let c = pc.run(chain(&pc.queen), opts()).await.unwrap();
    assert_eq!(
        m(&c, "stateKeys"),
        3.0,
        "a wrote the three windows it loaded, not its empty start"
    );
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reset_string_applies_to_a_process_that_took_over_as_a_standby() {
    let b = Fake::start();
    let (p1, p2) = (b.proc(), b.proc());
    b.at(1000.0).await;
    b.push("orders", "c1", json!({})).await;
    let first = p1
        .run(
            per_minute(&p1.queen),
            fast("reset-takeover")
                .now(b.clock())
                .reset(Reset::Token("v1".into())),
        )
        .await
        .unwrap();
    until(3000, "the window", || async {
        m(&first, "stateKeys") == 1.0
    })
    .await;
    let second = p2
        .run(
            per_minute(&p2.queen),
            fast("reset-takeover")
                .now(b.clock())
                .reset(Reset::Token("v2".into())),
        )
        .await
        .unwrap();
    assert_eq!(role(&second), "standby");
    first.stop().await;
    until(6000, "the takeover", || async { role(&second) == "active" }).await;
    until(3000, "the new token", || async {
        b.record("reset-takeover")
            .await
            .is_some_and(|r| r["resetToken"] == "v2")
    })
    .await;
    assert_eq!(
        b.record("reset-takeover").await.unwrap()["epoch"],
        1,
        "started over once"
    );
    second.stop().await;
}

// ------------------------------------------------- event time, end to end

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replay_closes_again_what_idle_closed_at_the_same_feed_time_and_sends_it_once() {
    let b = Fake::start();
    let chain = |q: &Queen| unanswered_within(q, "1m", |p| p.idle("30s"));
    let opts = || {
        fast("idle-replay")
            .now(b.clock())
            .lock_ttl("1s")
            .snapshot_every_millis(3_600_000.0)
            .on_error(|_| {})
    };
    b.at(1000.0).await;
    b.push("issues", "r1", json!({ "n": 1, "at": ev(0.0, 0.0) }))
        .await;
    b.push("issues", "r2", json!({ "n": 1, "at": ev(0.0, 30.0) }))
        .await;
    let mut pa = b.proc();
    let a = pa.run(chain(&pa.queen), opts()).await.unwrap();
    until(3000, "two read", || async { m(&a, "entriesRead") >= 2.0 }).await;
    // Quiet: r1#1 (due 10:01:00.001) passes 61.001 s after the last message.
    b.at(1000.0 + 62_000.0).await;
    until(3000, "the idle timeout", || async {
        !b.messages("out").await.is_empty()
    })
    .await;
    // r4#1 keeps the state from going empty (an empty state would move the snapshot here).
    b.push("issues", "r4", json!({ "n": 1, "at": ev(1.0, 15.0) }))
        .await;
    let end = b
        .push("comments", "r2", json!({ "n": 1, "at": ev(1.0, 20.0) }))
        .await;
    until(3000, "the match", || async {
        b.messages("out").await.len() >= 2
    })
    .await;
    until(3000, "the position past it", || async {
        record_pos(&b.record("idle-replay").await) > end
    })
    .await;
    assert_eq!(
        b.record("idle-replay").await.unwrap()["snapshot"]["position"],
        1,
        "no snapshot since the start"
    );
    pa.crash();
    // The next runner reads it all again: the timeout (idle) and the match
    // come back in the state, and are not sent again.
    let pb = b.proc();
    let second = pb.run(chain(&pb.queen), opts()).await.unwrap();
    until(6000, "the takeover", || async { role(&second) == "active" }).await;
    assert!(
        m(&second, "replayEntries") >= 4.0,
        "replayed {}",
        m(&second, "replayEntries")
    );
    b.push_at(
        1000.0 + 70_000.0,
        "issues",
        "r3",
        json!({ "n": 1, "at": ev(2.0, 0.0) }),
    )
    .await;
    b.at(1000.0 + 200_000.0).await;
    until(4000, "four results", || async {
        b.messages("out").await.len() >= 4
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    second.stop().await;
    assert_eq!(
        b.payloads("out").await,
        [
            json!({ "issue": "r1#1", "unanswered": true }),
            json!({ "issue": "r2#1", "answered": "10:01" }),
            json!({ "issue": "r4#1", "unanswered": true }),
            json!({ "issue": "r3#1", "unanswered": true }),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn out_of_order_history_crashed_at_random_points_sends_what_one_uninterrupted_run_sends() {
    // The JS test's numbers: the same sequence.
    // The LCG in 32-bit integers, as `(Math.imul(seed, 1103515245) + 12345) & 0x7fffffff`.
    let mut seed: u32 = 11;
    let mut rnd = move || {
        seed = seed.wrapping_mul(1_103_515_245).wrapping_add(12_345) & 0x7fff_ffff;
        f64::from(seed) / 2_147_483_648.0
    };
    let mut msgs: Vec<(u64, String, bool, f64)> = Vec::new();
    let mut e = 0.0;
    for i in 0..150u64 {
        e += (rnd() * 90.0).floor();
        let jitter = ((rnd() - 0.5) * 240.0).floor(); // up to 2 minutes either way
        let repo = format!("r{}", (rnd() * 6.0).floor());
        let open = rnd() < 0.5;
        msgs.push((i, repo, open, (e + jitter).max(0.0)));
    }
    fn chain(q: &Queen) -> FeedStream {
        Stream::from_feed(q, FeedSource::new().from(FeedFrom::Earliest).queues(["issues", "comments"]))
            .pattern(
                Pattern::new(|m| m["queue"] == "issues", |m| m["queue"] == "comments", "10m")
                    .event_time(|m| m["data"]["at"].clone())
                    .allowed_lateness("3m")
                    .idle("30s")
                    .on_match(|s, t| json!({ "repo": s["partition"], "s": s["data"]["i"], "t": t["data"]["i"] }))
                    .on_timeout(|s| json!({ "repo": s["partition"], "s": s["data"]["i"], "timeout": true })),
            )
            .key_by(|v| v["repo"].clone())
            .window_tumbling(1800.0)
            .aggregate(FeedAggregate::new().count("results"))
            .map(|v, ctx| {
                let c = ctx.unwrap();
                json!({ "repo": c["key"], "half": &c["windowKey"].as_str().unwrap()[11..16], "results": v["results"] })
            })
            .to("out")
    }
    // Per repo (the sink's partition), in order: one commit groups its pushes
    // by partition, and where the commits fall differs with the crashes.
    async fn by_repo(b: &Fake) -> std::collections::BTreeMap<String, Vec<Value>> {
        let mut out: std::collections::BTreeMap<String, Vec<Value>> = Default::default();
        for e in b.messages("out").await {
            out.entry(e["partition"].as_str().unwrap().to_string())
                .or_default()
                .push(e["payload"].clone());
        }
        out
    }
    async fn load(b: &Fake, msgs: &[(u64, String, bool, f64)]) -> u64 {
        let mut last = 0;
        for (i, repo, open, sec) in msgs {
            b.at(*i as f64 * 1000.0).await;
            let q = if *open { "issues" } else { "comments" };
            last = b
                .push(q, repo, json!({ "i": i, "at": iso(H0 + sec * 1000.0) }))
                .await;
        }
        last
    }
    // The import ends; the feed's clock goes on for 80 minutes, which closes
    // every wait and window (idle), the same way on both runs.
    async fn tail(b: &Fake, runner: &queen_mq::streams::feed::FeedHandle, n: usize) {
        for k in 1..=8 {
            b.at(n as f64 * 1000.0 + f64::from(k) * 600_000.0).await;
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        until(8000, "every wait and window to close", || async {
            let mm = runner.metrics();
            mm["open"] == 0
                && mm["steps"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|s| s["held"].as_u64().unwrap_or(0) == 0)
                && mm["pendingOutputs"] == 0
        })
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let reference = Fake::start();
    let ref_end = load(&reference, &msgs).await;
    let rp = reference.proc();
    let r = rp
        .run(chain(&rp.queen), fast("rand-et").now(reference.clock()))
        .await
        .unwrap();
    until(6000, "the reference run", || async {
        record_pos(&reference.record("rand-et").await) > ref_end
    })
    .await;
    tail(&reference, &r, msgs.len()).await;
    r.stop().await;
    let expected = by_repo(&reference).await;
    let total: usize = expected.values().map(|v| v.len()).sum();
    assert!(expected.len() == 6 && total >= 12, "results: {total}");

    let b = Fake::start();
    let end = load(&b, &msgs).await;
    let opts = || {
        fast("rand-et")
            .now(b.clock())
            .lock_ttl("1s")
            .max_records(7)
            .snapshot_max_entries(20)
            .on_error(|_| {})
    };
    let mut p = b.proc();
    let mut runner = p.run(chain(&p.queen), opts()).await.unwrap();
    for k in 1..=3 {
        let target = (msgs.len() * k / 4) as u64;
        until(6000, "the position", || async {
            record_pos(&b.record("rand-et").await) >= target
        })
        .await;
        p.crash();
        p = b.proc();
        runner = p.run(chain(&p.queen), opts()).await.unwrap();
        until(6000, "the takeover", || async { role(&runner) == "active" }).await;
    }
    until(6000, "the end", || async {
        record_pos(&b.record("rand-et").await) > end
    })
    .await;
    tail(&b, &runner, msgs.len()).await;
    runner.stop().await;
    assert_eq!(by_repo(&b).await, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_step_after_a_step_on_event_time_follows_its_watermark_on_a_quiet_feed() {
    // The pattern's timeout of issue 1 and the match of issue 2 both count in
    // the hour, though idle moved the pattern on in between.
    let b = Fake::start();
    let p = b.proc();
    for (ms, queue, n) in [
        (0.0, "issues", 1),
        (300_000.0, "issues", 2),
        (400_000.0, "comments", 2),
    ] {
        b.push_at(ms, queue, "r1", json!({ "n": n, "at": iso(T0 + ms) }))
            .await;
    }
    let s = Stream::from_feed(
        &p.queen,
        FeedSource::new()
            .from(FeedFrom::Earliest)
            .queues(["issues", "comments"]),
    )
    .pattern(
        Pattern::new(
            |m| m["queue"] == "issues",
            |m| m["queue"] == "comments",
            "10m",
        )
        .key(|m, _| {
            json!(format!(
                "{}#{}",
                m["partition"].as_str().unwrap(),
                m["data"]["n"]
            ))
        })
        .event_time(|m| m["data"]["at"].clone())
        .idle("1m")
        .on_match(|s, _| json!({ "issue": s["data"]["n"], "answered": true }))
        .on_timeout(|s| json!({ "issue": s["data"]["n"], "answered": false })),
    )
    .key_by(|_| json!("r1"))
    .window_tumbling(3600.0)
    .aggregate(FeedAggregate::new().count("results"))
    .to("hourly");
    let r = p.run(s, fast("review-h1").now(b.clock())).await.unwrap();
    until(3000, "three read", || async { m(&r, "entriesRead") >= 3.0 }).await;
    for ms in [
        500_000.0,
        700_000.0,
        1_200_000.0,
        3_000_000.0,
        4_000_000.0,
        5_000_000.0,
        6_000_000.0,
    ] {
        b.at(ms).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    until(3000, "the hour", || async {
        !b.messages("hourly").await.is_empty()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(m(&r, "late"), 0.0);
    assert_eq!(b.payloads("hourly").await, [json!({ "results": 2 })]);
    r.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_window_after_a_window_counts_every_reading_once_with_idle_on_the_first_only() {
    let b = Fake::start();
    let p = b.proc();
    // A message every 20 s of the feed's clock; event time at a third of that pace.
    for k in 0..60u32 {
        b.push_at(
            f64::from(k) * 20_000.0,
            "r",
            "s",
            json!({ "at": iso(H0 + (f64::from(k) * 20_000.0 / 3.0).floor()) }),
        )
        .await;
    }
    let s = Stream::from_feed(
        &p.queen,
        FeedSource::new().from(FeedFrom::Earliest).queues(["r"]),
    )
    .window(
        FeedWindow::tumbling(60.0)
            .event_time(|m| m["data"]["at"].clone())
            .idle("30s"),
    )
    .aggregate(FeedAggregate::new().count("n"))
    .window_tumbling(3600.0)
    .aggregate(FeedAggregate::new().field("n", |v: &Value| v["n"].clone()))
    .to("out");
    let r = p.run(s, fast("review-h1b").now(b.clock())).await.unwrap();
    until(3000, "sixty read", || async {
        m(&r, "entriesRead") >= 60.0
    })
    .await;
    for k in 1..=8 {
        b.at(60.0 * 20_000.0 + f64::from(k) * 900_000.0).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    until(3000, "the hour", || async {
        !b.messages("out").await.is_empty()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(m(&r, "late"), 0.0);
    assert_eq!(b.payloads("out").await, [json!({ "n": 60 })]);
    r.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_held_items_of_one_message_that_both_fail_go_to_the_dead_letter_queue_each_once() {
    let b = Fake::start();
    let p = b.proc();
    b.push(
        "issues",
        "r1",
        json!({ "ids": ["a", "b"], "bad": true, "at": ev(0.0, 0.0) }),
    )
    .await;
    b.push(
        "comments",
        "r1",
        json!({ "ids": ["a", "b"], "at": ev(5.0, 0.0) }),
    )
    .await;
    b.push("issues", "r1", json!({ "ids": ["c"], "at": ev(30.0, 0.0) }))
        .await;
    let s = Stream::from_feed(
        &p.queen,
        FeedSource::new()
            .from(FeedFrom::Earliest)
            .queues(["issues", "comments"]),
    )
    .flat_map(|m, _| {
        m["data"]["ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|id| json!({ "id": id }))
            .collect::<Vec<_>>()
    })
    .pattern(
        Pattern::new(
            |m| m["queue"] == "issues",
            |m| m["queue"] == "comments",
            "1h",
        )
        .key(|_, v| v.unwrap()["id"].clone())
        .event_time(|m| m["data"]["at"].clone())
        .on_match(|s, _| -> Result<Value, String> {
            if s["data"]["bad"] == true {
                return Err("boom".into());
            }
            Ok(json!({ "ok": true }))
        }),
    )
    .to("out");
    let r = p
        .run(
            s,
            fast("review-h3")
                .on_poison(Poison::DeadLetter("dlq".into()))
                .on_error(|_| {}),
        )
        .await
        .unwrap();
    until(3000, "two dead letters", || async {
        b.messages("dlq").await.len() >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    let dlq = b.messages("dlq").await;
    assert_eq!(dlq.len(), 2);
    assert_eq!(
        dlq.iter()
            .map(|e| e["transactionId"].as_str().unwrap())
            .collect::<HashSet<_>>()
            .len(),
        2
    );
    assert!(
        m(&r, "conflicts") <= 1.0,
        "conflicts: {}",
        m(&r, "conflicts")
    );
    r.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_without_a_time_meets_the_poison_policy_and_a_window_takes_allowed_lateness_as_a_duration(
) {
    let b = Fake::start();
    let p = b.proc();
    b.push("issues", "r1", json!({ "n": 1, "at": ev(0.0, 0.0) }))
        .await;
    b.push("issues", "r1", json!({ "n": 2 })).await;
    b.push("comments", "r1", json!({ "n": 1, "at": ev(5.0, 0.0) }))
        .await;
    for k in 3..10 {
        b.push(
            "issues",
            "r1",
            json!({ "n": k, "at": ev(60.0 * f64::from(k), 0.0) }),
        )
        .await;
    }
    let r = p
        .run(
            unanswered(&p.queen, |p| p),
            fast("review-m1").on_poison(Poison::Skip).on_error(|_| {}),
        )
        .await
        .unwrap();
    until(3000, "ten read", || async { m(&r, "entriesRead") >= 10.0 }).await;
    until(3000, "three results", || async {
        b.messages("out").await.len() >= 3
    })
    .await;
    assert_eq!(m(&r, "skipped"), 1.0);
    assert_eq!(
        b.payloads("out").await[..2],
        [
            json!({ "issue": "r1#1", "answered": "10:05" }),
            json!({ "issue": "r1#3", "unanswered": true })
        ]
    );
    r.stop().await;

    let w = Fake::start();
    let wp = w.proc();
    for min in [0.0, 1.0, 2.0, 30.0, 31.0, 90.0] {
        w.push("r", "s", json!({ "at": ev(min, 0.0) })).await;
    }
    let s = Stream::from_feed(
        &wp.queen,
        FeedSource::new().from(FeedFrom::Earliest).queues(["r"]),
    )
    .window(
        FeedWindow::tumbling(600.0)
            .event_time(|m| m["data"]["at"].clone())
            .allowed_lateness("5m"),
    )
    .aggregate(FeedAggregate::new().count("n"))
    .to("out");
    let r = wp.run(s, fast("review-m1b")).await.unwrap();
    until(3000, "two windows", || async {
        w.messages("out").await.len() >= 2
    })
    .await;
    assert_eq!(
        w.payloads("out").await,
        [json!({ "n": 3 }), json!({ "n": 2 })]
    );
    r.stop().await;
}

// ------------------------------------------------------ records, counts

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_record_counts_open_windows_and_nothing_held_before_a_result_reaches_the_event_time_step(
) {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    for c in ["c1", "c2", "c3"] {
        b.push("orders", c, json!({ "total": 1, "at": ev(0.0, 0.0) }))
            .await;
    }
    b.push("orders", "c1", json!({ "total": 1, "at": ev(30.0, 0.0) }))
        .await;
    let s = Stream::from_feed(&p.queen, FeedSource::new().from(FeedFrom::Earliest))
        .window_tumbling(60.0)
        .aggregate(FeedAggregate::new().count("count"))
        .name("perMinute")
        .pattern(
            Pattern::new(|_| true, |_| false, "1h")
                .event_time(|_| json!(ev(0.0, 0.0)))
                .allowed_lateness("1h")
                .on_timeout(|s| s.clone()),
        )
        .name("later")
        .to("out");
    let r = p.run(s, fast("counts").now(b.clock())).await.unwrap();
    until(3000, "four read", || async { m(&r, "entriesRead") >= 4.0 }).await;
    until(3000, "three open windows in the record", || async {
        b.record("counts")
            .await
            .is_some_and(|r| r["open"]["perMinute"] == 3)
    })
    .await;
    let rec = b.record("counts").await.unwrap();
    // Every stateful step (JSON key order is not kept: serde_json's map is sorted).
    let mut steps: Vec<&String> = rec["open"].as_object().unwrap().keys().collect();
    steps.sort();
    assert_eq!(steps, ["later", "perMinute"]);
    assert!(
        rec.get("held").is_none(),
        "nothing reached the event-time step yet: no window closed"
    );
    r.stop().await;
}

// ---------------------------------------------------- snapshot bytes in KV

/// Window state keys as the record names them: every layer's pieces applied in order.
async fn layered_state(b: &Fake, id: &str) -> (HashMap<String, Value>, usize) {
    let rec = b.record(id).await.unwrap();
    let mut state = HashMap::new();
    let mut pieces = 0;
    for l in rec["snapshot"]["layers"].as_array().unwrap() {
        for pc in b.layer_pieces(id, l["sid"].as_str().unwrap()).await {
            assert_eq!(pc["v"], 2);
            pieces += 1;
            for kv in pc["set"].as_array().unwrap() {
                state.insert(kv[0].as_str().unwrap().to_string(), kv[1].clone());
            }
            for k in pc["del"].as_array().unwrap() {
                state.remove(k.as_str().unwrap());
            }
        }
    }
    (state, pieces)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_big_state_is_written_in_pieces_and_reads_back_whole() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    let n = 20_000;
    for i in (0..n).step_by(500) {
        let items: Vec<String> = (i..i + 500).map(|k| format!("k{k}")).collect();
        let refs: Vec<(&str, &str, Value)> = items
            .iter()
            .map(|q| ("orders", q.as_str(), json!({})))
            .collect();
        b.tx(&refs).await;
    }
    let r = p
        .run(
            per_minute(&p.queen),
            fast("big-layer")
                .now(b.clock())
                .snapshot_piece_bytes(64 * 1024)
                .snapshot_every_millis(3_600_000.0),
        )
        .await
        .unwrap();
    until(15_000, "everything read", || async {
        m(&r, "entriesRead") >= n as f64
    })
    .await;
    r.stop().await;
    let rec = b.record("big-layer").await.unwrap();
    assert!(
        rec["snapshot"]["chunks"].as_u64().unwrap() > 10,
        "many chunks ({})",
        rec["snapshot"]["chunks"]
    );
    let (state, pieces) = layered_state(&b, "big-layer").await;
    assert!(pieces > 10, "written piece by piece ({pieces})");
    assert_eq!(state.len(), n);
    let one = &state[&format!("w\u{1f}{}\u{1f}k12345", MIN0 as i64)];
    assert_eq!(one["key"], "k12345");
    assert_eq!(one["acc"], json!({ "count": 1 }));
    assert_eq!(one["due"].as_f64(), Some(MIN0 + 60_000.0));
    let p2 = b.proc();
    let again = p2
        .run(per_minute(&p2.queen), fast("big-layer").now(b.clock()))
        .await
        .unwrap();
    assert_eq!(m(&again, "stateKeys"), n as f64, "read back whole");
    again.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_piece_bigger_than_one_kv_value_goes_on_in_the_next_and_deletes_are_read_back() {
    let b = Fake::start();
    let p = b.proc();
    // Hex of hashes: deflate saves about half of it, so the one piece spans several chunks.
    let noise: String = (0..64)
        .map(|i| {
            use sha2::Digest;
            sha2::Sha256::digest(i.to_string().as_bytes())
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        })
        .collect();
    b.push("issues", "a", json!({ "big": noise, "n": 1 })).await;
    b.push("issues", "b", json!({ "n": 2 })).await;
    b.push("issues", "c", json!({ "n": 3 })).await;
    let s = Stream::from_feed(
        &p.queen,
        FeedSource::new()
            .from(FeedFrom::Earliest)
            .queues(["issues", "comments"]),
    )
    .pattern(
        Pattern::new(
            |m| m["queue"] == "issues",
            |m| m["queue"] == "comments",
            "1h",
        )
        .on_match(|s, _| json!({ "answered": s["partition"] })),
    )
    .to("out");
    let r = p
        .run(
            s,
            fast("pieces")
                .snapshot_chunk_chars(1024)
                .snapshot_max_entries(1),
        )
        .await
        .unwrap();
    until(3000, "a snapshot of the three", || async {
        m(&r, "entriesRead") >= 3.0 && m(&r, "snapshots") >= 1.0
    })
    .await;
    let full = b.record("pieces").await.unwrap()["snapshot"]["layers"][0].clone();
    let sid = full["sid"].as_str().unwrap().to_string();
    let count = full["chunks"].as_u64().unwrap() as usize;
    assert!(count >= 2, "the piece spans several chunks ({count})");
    for n in 0..count {
        let v = b
            .record(&format!("pieces#snap#{sid}#{n}"))
            .await
            .expect("the chunk");
        assert!(v["d"].as_str().unwrap().len() <= 1024);
        if n < count - 1 {
            assert_eq!(v["m"], 1, "chunk {n} says more follows");
        } else {
            assert!(v.get("m").is_none(), "the last chunk ends the piece");
        }
    }
    let pieces = b.layer_pieces("pieces", &sid).await;
    assert_eq!(pieces.len(), 1);
    assert_eq!(pieces[0]["k"], "pattern");
    let set = pieces[0]["set"].as_array().unwrap();
    assert_eq!(set.len(), 3);
    let a_key = set
        .iter()
        .find(|kv| kv[1]["start"]["data"]["n"] == 1 || kv[1].to_string().contains(&noise[..64]))
        .expect("a's wait")[0]
        .clone();
    assert!(
        set.iter().any(|kv| kv[1].to_string().contains(&noise)),
        "the big value whole"
    );
    // a is answered: its wait goes, and the next layer (a delta: the other two
    // stay) says so.
    b.push("comments", "a", json!({})).await;
    until(3000, "the match and a delta", || async {
        !b.messages("out").await.is_empty()
            && b.record("pieces").await.unwrap()["snapshot"]["layers"]
                .as_array()
                .map_or(0, |l| l.len())
                >= 2
    })
    .await;
    r.stop().await;
    let layers = b.record("pieces").await.unwrap()["snapshot"]["layers"].clone();
    let delta = layers
        .as_array()
        .unwrap()
        .iter()
        .find(|l| l.get("full").is_none())
        .expect("a delta")
        .clone();
    let pieces = b
        .layer_pieces("pieces", delta["sid"].as_str().unwrap())
        .await;
    let dels: Vec<Value> = pieces
        .iter()
        .flat_map(|pc| pc["del"].as_array().unwrap().clone())
        .collect();
    assert_eq!(dels, [a_key], "the delete of a's wait");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_of_format_1_still_loads_and_the_next_one_is_a_full_layer_of_format_2() {
    let b = Fake::start();
    let p = b.proc();
    b.at(1000.0).await;
    for i in 0..3 {
        b.push("orders", &format!("c{i}"), json!({})).await;
    }
    let first = p
        .run(per_minute(&p.queen), fast("old").now(b.clock()))
        .await
        .unwrap();
    until(3000, "three read", || async {
        m(&first, "entriesRead") >= 3.0
    })
    .await;
    first.stop().await;
    // Rewrite the stored snapshot the way a runner of format 1 wrote it.
    let rec = b.record("old").await.unwrap();
    let (state, _) = layered_state(&b, "old").await;
    for k in b.kv_keys(NS, "old#snap#").await {
        b.kv_delete(NS, &k).await;
    }
    let mut entries: Vec<Value> = state.into_iter().map(|(k, v)| json!([k, v])).collect();
    entries.sort_by_key(|e| e[0].as_str().unwrap().to_string());
    let text =
        json!({ "v": 1, "steps": [{ "name": "perMinute", "kind": "window", "entries": entries }] })
            .to_string();
    let b64 = b64_encode(&deflate_raw(text.as_bytes()));
    let parts: Vec<&str> = b64
        .as_bytes()
        .chunks(48_000)
        .map(|c| std::str::from_utf8(c).unwrap())
        .collect();
    let sid = "f1old0000000000";
    for (n, d) in parts.iter().enumerate() {
        b.kv_put(NS, &format!("old#snap#{sid}#{n}"), json!({ "d": d }))
            .await;
    }
    let mut old = rec.clone();
    old["snapshot"] = json!({
        "id": sid, "chunks": parts.len(), "bytes": text.len(),
        "position": rec["snapshot"]["position"], "clock": rec["snapshot"]["clock"], "metas": rec["snapshot"]["metas"]
    });
    b.kv_put(NS, "old", old).await;
    let p2 = b.proc();
    let again = p2
        .run(
            per_minute(&p2.queen),
            fast("old").now(b.clock()).snapshot_max_entries(1),
        )
        .await
        .unwrap();
    assert_eq!(
        m(&again, "stateKeys"),
        3.0,
        "the three windows of the old snapshot came back"
    );
    b.push("orders", "c9", json!({})).await;
    until(3000, "the next snapshot", || async {
        b.record("old").await.unwrap()["snapshot"]["layers"]
            .as_array()
            .map_or(0, |l| l.len())
            == 1
    })
    .await;
    again.stop().await;
    let snap = b.record("old").await.unwrap()["snapshot"].clone();
    assert_eq!(snap["layers"][0]["full"], true, "a full layer of format 2");
    assert!(
        b.kv_keys(NS, "old#snap#")
            .await
            .iter()
            .all(|k| !k.starts_with(&format!("old#snap#{sid}#"))),
        "the old chunks are gone"
    );
    let p3 = b.proc();
    let last = p3
        .run(per_minute(&p3.queen), fast("old").now(b.clock()))
        .await
        .unwrap();
    assert_eq!(m(&last, "stateKeys"), 4.0);
    last.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_piece_limit_counts_utf8_bytes_not_characters() {
    let b = Fake::start();
    let p = b.proc();
    // 200 characters, 600 bytes in UTF-8: counted in characters, a piece would hold three times the limit.
    let text = "漢字".repeat(100);
    for i in 0..12 {
        b.push("issues", &format!("k{i}"), json!({ "text": text }))
            .await;
    }
    let s = Stream::from_feed(
        &p.queen,
        FeedSource::new()
            .from(FeedFrom::Earliest)
            .queues(["issues", "comments"]),
    )
    .pattern(Pattern::new(
        |m| m["queue"] == "issues",
        |m| m["queue"] == "comments",
        "1h",
    ))
    .to("out");
    let room = 2048;
    let r = p
        .run(
            s,
            fast("utf8")
                .snapshot_piece_bytes(room)
                .snapshot_every_millis(3_600_000.0),
        )
        .await
        .unwrap();
    until(3000, "twelve read", || async {
        m(&r, "entriesRead") >= 12.0
    })
    .await;
    r.stop().await;
    let layers = b.record("utf8").await.unwrap()["snapshot"]["layers"].clone();
    let mut pieces = Vec::new();
    for l in layers.as_array().unwrap() {
        pieces.extend(b.layer_pieces("utf8", l["sid"].as_str().unwrap()).await);
    }
    let set: Vec<Value> = pieces
        .iter()
        .flat_map(|pc| pc["set"].as_array().unwrap().clone())
        .collect();
    assert_eq!(set.len(), 12);
    assert!(
        set.iter().all(|kv| kv[1].to_string().contains(&text)),
        "the text read back whole"
    );
    assert!(pieces.len() >= 3, "{} pieces", pieces.len());
    for pc in &pieces {
        // A piece ends at the entry that takes it to the limit: the entries before that one stay under it.
        let entries = pc["set"].as_array().unwrap();
        let before: usize = entries[..entries.len() - 1]
            .iter()
            .map(|e| e.to_string().len())
            .sum();
        assert!(
            before < room,
            "{before} bytes before the last entry of a piece (limit {room})"
        );
    }
}
