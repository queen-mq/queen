//! Feed streams on a real broker (one built with the tenant feed): what the
//! fake broker cannot show. A window on a quiet feed closes because the runner
//! moves the broker's clock with a write of its own (a tick); every node of a
//! cluster serves a read (a leader by naming its followers); a process killed
//! mid-stream is taken over from its stored position.
//!
//! `QUEEN_FEED_TEST_URL` names the broker (a single node or any node of a
//! cluster, e.g. `http://127.0.0.1:17872`); `QUEEN_FEED_TEST_NODES` (comma
//! separated) the nodes of a cluster for the read test. Unset: the tests skip.
//! Queue names start with `fc-rust-live-`, new on every run.

mod feed_support;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use feed_support::{fast, m, role, until, Proc};
use queen_mq::feed::{FeedFrom, FeedOptions};
use queen_mq::streams::feed::{FeedAggregate, FeedSource};
use queen_mq::streams::Stream;
use queen_mq::{Config, Queen};
use serde_json::{json, Value};

fn broker() -> Option<String> {
    match std::env::var("QUEEN_FEED_TEST_URL") {
        Ok(u) if !u.trim().is_empty() => Some(u.trim().trim_end_matches('/').to_string()),
        _ => {
            eprintln!("QUEEN_FEED_TEST_URL is not set: skipping the live feed test");
            None
        }
    }
}

/// `fc-rust-live-<millis>-<pid>-<n>-<what>`: a name no earlier run used, and no other
/// test of this run either (the tests run in parallel, often in the same millisecond).
fn name(what: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("fc-rust-live-{ms}-{}-{n}-{what}", std::process::id())
}

async fn push(q: &Queen, queue: &str, partition: &str, data: Value) {
    let r = q
        .queue(queue)
        .partition(partition)
        .push(data)
        .await
        .expect("push");
    assert!(
        r.iter().all(|x| x.status == queen_mq::PushStatus::Queued),
        "{r:?}"
    );
}

async fn payloads(q: &Queen, queue: &str, expect: usize) -> Vec<Value> {
    // Read the outputs back from the feed itself: every push to the queue, in log order.
    let mut reader = q
        .feed(
            FeedOptions::new()
                .queues([queue])
                .from(FeedFrom::Earliest)
                .follow(false),
        )
        .expect("a reader");
    let mut out = Vec::new();
    while let Some(e) = reader.next_entry().await.expect("a read") {
        out.push(e.to_value()["data"].clone());
    }
    assert!(out.len() >= expect, "{} of {expect} outputs", out.len());
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_window_on_a_quiet_feed_closes_because_the_runner_moves_the_brokers_clock() {
    let Some(url) = broker() else { return };
    let (orders, counts) = (name("orders"), name("counts"));
    let p = Proc::new(&url);
    let s = Stream::from_feed(&p.queen, FeedSource::new().queues([orders.as_str()]))
        .window_tumbling(2.0)
        .aggregate(FeedAggregate::new().count("count"))
        .to(counts.as_str());
    let r = p
        .run(s, fast(&name("tick")).wait_millis(500))
        .await
        .unwrap();
    push(&p.queen, &orders, "c1", json!({ "n": 1 })).await;
    push(&p.queen, &orders, "c1", json!({ "n": 2 })).await;
    // This test pushes nothing else: on a quiet broker only the runner's own
    // write (a tick) moves the clock past the window; on a shared one other
    // writes may do it first.
    until(15_000, "the window to close on a quiet feed", || async {
        m(&r, "outputs") >= 1.0
    })
    .await;
    eprintln!(
        "ticks: {} (0: other writes moved the broker's clock first)",
        m(&r, "ticks")
    );
    r.stop().await;
    assert_eq!(
        payloads(&p.queen, &counts, 1).await,
        [json!({ "count": 2 })]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_node_of_a_cluster_serves_a_read() {
    let Some(url) = broker() else { return };
    let nodes: Vec<String> = std::env::var("QUEEN_FEED_TEST_NODES")
        .ok()
        .map(|s| {
            s.split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect()
        })
        .unwrap_or_else(|| vec![url.clone()]);
    let queue = name("reads");
    let q = Queen::connect(Config::new(&url)).unwrap();
    push(&q, &queue, "p1", json!({ "n": 1 })).await;
    for node in &nodes {
        let q = Queen::connect(Config::new(node)).unwrap();
        // A leader answers 421 with its followers: the reader asks them.
        let mut reader = q
            .feed(
                FeedOptions::new()
                    .queues([queue.as_str()])
                    .from(FeedFrom::Earliest)
                    .follow(false),
            )
            .unwrap();
        let mut seen = Vec::new();
        let end = std::time::Instant::now() + Duration::from_secs(10);
        while seen.is_empty() && std::time::Instant::now() < end {
            reader.seek(FeedFrom::Earliest);
            let page = reader
                .read(Some(1000))
                .await
                .unwrap_or_else(|e| panic!("{node}: {e}"));
            seen.extend(
                page.entries
                    .into_iter()
                    .map(|e| e.to_value()["data"].clone()),
            );
        }
        assert_eq!(seen, [json!({ "n": 1 })], "{node}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_killed_process_is_taken_over_from_its_stored_position_each_message_once() {
    let Some(url) = broker() else { return };
    let (orders, copy, id) = (name("orders"), name("copy"), name("kill"));
    let chain = |q: &Queen| {
        Stream::from_feed(q, FeedSource::new().queues([orders.as_str()])).to(copy.as_str())
    };
    let opts = || fast(&id).lock_ttl("2s").wait_millis(300).on_error(|_| {});
    let mut pa = Proc::new(&url);
    let pb = Proc::new(&url);
    let a = pa.run(chain(&pa.queen), opts()).await.unwrap();
    let b = pb.run(chain(&pb.queen), opts()).await.unwrap();
    assert_eq!(role(&a), "active");
    assert_eq!(role(&b), "standby");
    for n in 1..=3 {
        push(&pa.queen, &orders, "c1", json!({ "n": n })).await;
    }
    until(10_000, "three copies", || async { m(&a, "outputs") >= 3.0 }).await;
    pa.crash();
    for n in 4..=6 {
        push(&pb.queen, &orders, "c1", json!({ "n": n })).await;
    }
    until(15_000, "the takeover", || async {
        role(&b) == "active" && m(&b, "outputs") >= 3.0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    b.stop().await;
    let ns: Vec<Value> = payloads(&pb.queen, &copy, 6)
        .await
        .iter()
        .map(|p| p["n"].clone())
        .collect();
    assert_eq!(
        ns,
        (1..=6).map(|n| json!(n)).collect::<Vec<_>>(),
        "each message once, in order"
    );
}
