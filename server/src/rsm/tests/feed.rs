//! `POST /api/v1/feed` on one node: the tenant's pushes in log order, one
//! position per log entry (a transaction's pushes share theirs), the filters,
//! the caps cut between positions, the long poll, and the shards that read
//! their own records only, through the sealed files' indexes.

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{ApiReq, Deadline, PushReq, ReqCtx, Rsm, RsmBuildCtx, TxnReq};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-feed-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn build_ctx(dir: &Path) -> RsmBuildCtx {
    RsmBuildCtx {
        data_dir: dir.display().to_string(),
        notifier: crate::notify::Notifier::new(false),
        disk_high_pct: 99.0,
        disk_low_pct: 98.0,
    }
}

fn ctx() -> ReqCtx {
    ReqCtx::new(
        crate::config::DEFAULT_TENANT,
        Deadline::after(Duration::from_secs(30)),
    )
}

async fn push(f: &RaftFacade, items: Value) {
    let out = f
        .push(
            ctx(),
            PushReq {
                raw: json!({ "items": items }).to_string().into_bytes(),
            },
        )
        .await
        .expect("push");
    let v: Value = serde_json::from_str(&out.body).expect("push body");
    for it in v.as_array().expect("push array") {
        assert_eq!(it["status"], "queued", "{it}");
    }
}

async fn transaction(f: &RaftFacade, items: Value) {
    let out = f
        .transaction(
            ctx(),
            TxnReq {
                raw: json!({ "operations": [{ "type": "push", "items": items }] })
                    .to_string()
                    .into_bytes(),
            },
        )
        .await
        .expect("transaction");
    let v: Value = serde_json::from_str(&out.body).expect("transaction body");
    assert_eq!(v["success"], true, "{v}");
}

async fn feed(f: &RaftFacade, body: Value) -> (u16, Value) {
    let out = f
        .api(
            ctx(),
            ApiReq {
                method: "POST".into(),
                path: "/api/v1/feed".into(),
                query: None,
                body: body.to_string().into_bytes(),
            },
        )
        .await
        .expect("feed call");
    let v = serde_json::from_str(&out.body)
        .unwrap_or_else(|e| panic!("bad JSON body: {e}\n{}", out.body));
    (out.status, v)
}

fn entries(v: &Value) -> &Vec<Value> {
    v["entries"].as_array().expect("entries")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn the_feed_reads_every_queue_in_log_order_and_a_transaction_shares_one_position() {
    let dir = scratch("order");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    push(
        &f,
        json!([{"queue":"orders","partition":"c1","payload":{"n":1},"transactionId":"o1"}]),
    )
    .await;
    push(
        &f,
        json!([{"queue":"payments","partition":"c1","payload":{"n":2},"transactionId":"p1"}]),
    )
    .await;
    push(
        &f,
        json!([{"queue":"orders","partition":"c2","payload":{"n":3},"transactionId":"o2"}]),
    )
    .await;
    transaction(
        &f,
        json!([
            {"queue":"orders","partition":"c1","payload":{"n":4},"transactionId":"t-o"},
            {"queue":"payments","partition":"c1","payload":{"n":5},"transactionId":"t-p"}
        ]),
    )
    .await;

    let (status, v) = feed(&f, json!({"from":"earliest"})).await;
    assert_eq!(status, 200, "{v}");
    let es = entries(&v);
    let ns: Vec<i64> = es
        .iter()
        .map(|e| e["payload"]["n"].as_i64().unwrap())
        .collect();
    assert_eq!(ns, vec![1, 2, 3, 4, 5], "{v}");
    let pos: Vec<u64> = es.iter().map(|e| e["position"].as_u64().unwrap()).collect();
    assert!(
        pos[0] < pos[1] && pos[1] < pos[2] && pos[2] < pos[3],
        "{pos:?}"
    );
    assert_eq!(
        pos[3], pos[4],
        "a transaction's pushes share one position: {pos:?}"
    );
    assert_eq!(es[0]["queue"], "orders");
    assert_eq!(es[0]["partition"], "c1");
    assert_eq!(es[0]["transactionId"], "o1");
    assert_eq!(es[0]["offset"], 0);
    assert_eq!(es[0]["kind"], "push");
    assert_eq!(es[3]["offset"], 1, "the second message of orders/c1");
    assert!(es[0]["ts"].as_str().is_some_and(|t| t.ends_with('Z')));
    assert_eq!(v["caughtUp"], true);
    assert_eq!(
        v["next"].as_u64().unwrap(),
        v["applied"].as_u64().unwrap() + 1
    );
    assert!(v["resolvedTs"].as_str().is_some());

    // From the answer's `next` there is nothing more, and from "now" the
    // same: both start past everything applied.
    let (_, again) = feed(&f, json!({"from": v["next"]})).await;
    assert!(entries(&again).is_empty(), "{again}");
    let (_, now) = feed(&f, json!({})).await;
    assert!(entries(&now).is_empty(), "{now}");
    assert_eq!(now["next"], v["next"]);

    // From a position inside the log: that position on, included.
    let (_, tail) = feed(&f, json!({"from": pos[2]})).await;
    let ns: Vec<i64> = entries(&tail)
        .iter()
        .map(|e| e["payload"]["n"].as_i64().unwrap())
        .collect();
    assert_eq!(ns, vec![3, 4, 5]);
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn filters_pick_queues_partitions_and_payload_fields() {
    let dir = scratch("filters");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    push(
        &f,
        json!([
            {"queue":"orders","partition":"c1","payload":{"country":"IT","total":40}},
            {"queue":"orders","partition":"c2","payload":{"country":"FR","total":10}},
            {"queue":"orders-archive","partition":"c1","payload":{"country":"IT","total":99}},
            {"queue":"payments","partition":"c1","payload":{"country":"IT","total":5}}
        ]),
    )
    .await;

    let count = |v: &Value| entries(v).len();
    let (_, v) = feed(&f, json!({"from":"earliest","queues":["orders"]})).await;
    assert_eq!(count(&v), 2, "an exact name: {v}");
    let (_, v) = feed(&f, json!({"from":"earliest","queues":["orders*"]})).await;
    assert_eq!(count(&v), 3, "a pattern: {v}");
    let (_, v) = feed(&f, json!({"from":"earliest","partitions":["c1"]})).await;
    assert_eq!(count(&v), 3, "a partition in every queue: {v}");
    let (_, v) = feed(
        &f,
        json!({"from":"earliest","where":[{"field":"data.country","op":"=","value":"IT"},
                                          {"field":"data.total","op":">=","value":40}]}),
    )
    .await;
    let totals: Vec<i64> = entries(&v)
        .iter()
        .map(|e| e["payload"]["total"].as_i64().unwrap())
        .collect();
    assert_eq!(totals, vec![40, 99], "{v}");
    let (_, v) = feed(
        &f,
        json!({"from":"earliest","where":[{"field":"queue","op":"!=","value":"orders"}]}),
    )
    .await;
    assert_eq!(count(&v), 2, "{v}");
    // A filter that drops everything still moves `next` past what it read.
    let (_, v) = feed(
        &f,
        json!({"from":"earliest","where":[{"field":"data.country","op":"=","value":"DE"}]}),
    )
    .await;
    assert_eq!(count(&v), 0);
    assert_eq!(v["caughtUp"], true);
    // A bad body is a 400 that says what is wrong.
    let (status, v) = feed(&f, json!({"queue":"orders"})).await;
    assert_eq!(status, 400, "{v}");
    let (status, v) = feed(&f, json!({"kinds":["ack"]})).await;
    assert_eq!(status, 400, "{v}");
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn caps_cut_between_positions_and_reading_on_misses_nothing() {
    let dir = scratch("caps");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    for n in 0..10 {
        push(
            &f,
            json!([{"queue":"q","partition":format!("p{}", n % 3),"payload":{"n":n}}]),
        )
        .await;
    }
    // A transaction of four pushes is one position: a cap of one message
    // still answers all four.
    transaction(
        &f,
        json!((10..14)
            .map(|n| json!({"queue":"q","partition":"p0","payload":{"n":n}}))
            .collect::<Vec<_>>()),
    )
    .await;

    let mut seen: Vec<i64> = Vec::new();
    let mut from = json!("earliest");
    let mut calls = 0;
    loop {
        let (status, v) = feed(&f, json!({"from": from, "maxRecords": 3})).await;
        assert_eq!(status, 200, "{v}");
        calls += 1;
        seen.extend(
            entries(&v)
                .iter()
                .map(|e| e["payload"]["n"].as_i64().unwrap()),
        );
        let positions: Vec<u64> = entries(&v)
            .iter()
            .map(|e| e["position"].as_u64().unwrap())
            .collect();
        // Never a position split across two answers.
        if let Some(last) = positions.last() {
            assert!(v["next"].as_u64().unwrap() > *last);
        }
        if v["caughtUp"] == true {
            break;
        }
        from = v["next"].clone();
        assert!(calls < 20, "the read does not end");
    }
    assert_eq!(
        seen,
        (0..14).collect::<Vec<_>>(),
        "every message once, in order"
    );
    assert!(calls >= 4, "the cap was applied: {calls} calls");
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_read_from_now_waits_for_the_next_push() {
    let dir = scratch("wait");
    let f = Arc::new(RaftFacade::open(&build_ctx(&dir)).expect("open facade"));
    push(&f, json!([{"queue":"q","partition":"p","payload":{"n":0}}])).await;
    let (_, head) = feed(&f, json!({})).await;
    let next = head["next"].clone();

    let reader = {
        let f = f.clone();
        tokio::spawn(async move {
            let began = Instant::now();
            let out = feed(&f, json!({"from": next, "maxWaitMs": 10_000})).await;
            (out, began.elapsed())
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    push(&f, json!([{"queue":"q","partition":"p","payload":{"n":1}}])).await;
    let ((status, v), took) = reader.await.expect("reader");
    assert_eq!(status, 200, "{v}");
    let ns: Vec<i64> = entries(&v)
        .iter()
        .map(|e| e["payload"]["n"].as_i64().unwrap())
        .collect();
    assert_eq!(ns, vec![1], "{v}");
    assert!(
        took < Duration::from_secs(5),
        "woken by the push, not by the deadline: {took:?}"
    );

    // Nothing pushed: the wait ends at maxWaitMs with an empty answer.
    let began = Instant::now();
    let (_, v) = feed(&f, json!({"from": v["next"], "maxWaitMs": 400})).await;
    assert!(entries(&v).is_empty());
    assert!(
        began.elapsed() >= Duration::from_millis(350),
        "{:?}",
        began.elapsed()
    );
    let f = Arc::try_unwrap(f).ok().expect("one owner");
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A caught-up answer read while entries apply: meta is RAM, read live, and
/// apply writes an entry's clock before its applied index, so a clock read
/// after the index could be the clock of the entry above it (Jepsen W13,
/// 2026-11-07: a resolvedTs equal to the stamp at `next`). Writers push while
/// a reader asks for the head; then no message at or after an answer's
/// `next` may be stamped at or before its `resolvedTs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_caught_up_answer_claims_no_stamp_past_its_next_while_entries_apply() {
    let dir = scratch("resolved-race");
    let f = Arc::new(RaftFacade::open(&build_ctx(&dir)).expect("open facade"));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let (f, stop) = (f.clone(), stop.clone());
            tokio::spawn(async move {
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    n += 1;
                    push(
                        &f,
                        json!([{"queue":"race","partition":format!("w{w}"),
                                "payload":{"n":n},"transactionId":format!("w{w}-{n}")}]),
                    )
                    .await;
                }
            })
        })
        .collect();
    let mut answers: Vec<(u64, String)> = Vec::new();
    let until = Instant::now() + Duration::from_secs(3);
    while Instant::now() < until {
        let (status, v) = feed(&f, json!({"from": "now"})).await;
        assert_eq!(status, 200, "{v}");
        answers.push((
            v["next"].as_u64().unwrap(),
            v["resolvedTs"].as_str().unwrap().to_string(),
        ));
    }
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.await.expect("writer");
    }

    // Every message's position and stamp, and the lowest stamp at or after
    // each position.
    let mut stamped: Vec<(u64, String)> = read_all(&f, json!({}), LARGE)
        .await
        .iter()
        .map(|e| {
            (
                e["position"].as_u64().unwrap(),
                e["ts"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    stamped.sort();
    assert!(stamped.len() > 100, "{} messages", stamped.len());
    let mut lowest = vec![String::new(); stamped.len()];
    for i in (0..stamped.len()).rev() {
        lowest[i] = match lowest.get(i + 1) {
            Some(next) if !next.is_empty() && *next < stamped[i].1 => next.clone(),
            _ => stamped[i].1.clone(),
        };
    }
    for (next, resolved) in &answers {
        let i = stamped.partition_point(|(p, _)| p < next);
        if i < stamped.len() {
            assert!(
                lowest[i] > *resolved,
                "next {next}, resolvedTs {resolved}: a message at or after position {next} \
                 is stamped {} ({} answers)",
                lowest[i],
                answers.len()
            );
        }
    }
    let f = Arc::try_unwrap(f).ok().expect("one owner");
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A no-op entry (a new leader's first, a membership change) moves the
/// applied index and not the clock. An answer cut at the newest push, then a
/// caught-up answer past the no-op: the second claims the clock less one
/// microsecond, so the first may claim no more (Jepsen W13, 2026-11-07, kill
/// and partition runs: resolvedTs went back by one microsecond on one node).
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn resolved_ts_does_not_go_back_over_a_no_op_entry() {
    use crate::rsm::replicator::Replicator;
    let dir = scratch("resolved-noop");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    push(
        &f,
        json!([{ "queue": "noop-q", "partition": "p", "payload": { "n": 1 } }]),
    )
    .await;
    // The first push to a new queue sets off an entry of its own (with a
    // clock): let it apply, so the no-op is the entry after the next push.
    let mut applied = 0;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let (_, head) = feed(&f, json!({ "from": "now" })).await;
        let now = head["applied"].as_u64().expect("applied");
        if now == applied {
            break;
        }
        applied = now;
    }
    push(
        &f,
        json!([{ "queue": "noop-q", "partition": "p", "payload": { "n": 2 } }]),
    )
    .await;
    let noop = crate::rsm::entry::encode_entry(&crate::rsm::entry::Entry::noop()).expect("encode");
    f.repl_for_test()
        .propose(
            bytes::Bytes::from(noop),
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .expect("the no-op applies");
    let (status, first) = feed(&f, json!({ "from": applied + 1, "maxRecords": 1 })).await;
    assert_eq!(status, 200, "{first}");
    assert_eq!(entries(&first).len(), 1, "{first}");
    assert_eq!(entries(&first)[0]["payload"]["n"], 2, "{first}");
    assert_eq!(
        first["caughtUp"], false,
        "the no-op is past the cut: {first}"
    );
    let (status, second) = feed(&f, json!({ "from": first["next"] })).await;
    assert_eq!(status, 200, "{second}");
    assert_eq!(second["caughtUp"], true, "{second}");
    let (a, b) = (
        first["resolvedTs"].as_str().expect("resolvedTs"),
        second["resolvedTs"].as_str().expect("resolvedTs"),
    );
    assert!(
        b >= a,
        "resolvedTs went back from {a} to {b}\n{first}\n{second}"
    );
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A read of a queue that does not exist yet waits on its name, and the push
/// that creates the queue wakes it: the answer comes with the push, not at
/// the one-second re-read that used to be the only way to find such a queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_read_of_a_queue_that_does_not_exist_yet_is_woken_by_its_first_push() {
    let dir = scratch("fresh");
    let f = Arc::new(RaftFacade::open(&build_ctx(&dir)).expect("open facade"));
    push(
        &f,
        json!([{"queue":"other","partition":"p","payload":{"n":0}}]),
    )
    .await;
    let reader = {
        let f = f.clone();
        tokio::spawn(async move {
            let began = Instant::now();
            let out = feed(&f, json!({"queues": ["fresh"], "maxWaitMs": 10_000})).await;
            (out, began.elapsed())
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    push(
        &f,
        json!([{"queue":"fresh","partition":"p","payload":{"n":1}}]),
    )
    .await;
    let ((status, v), took) = reader.await.expect("reader");
    assert_eq!(status, 200, "{v}");
    let ns: Vec<i64> = entries(&v)
        .iter()
        .map(|e| e["payload"]["n"].as_i64().unwrap())
        .collect();
    assert_eq!(ns, vec![1], "{v}");
    // The re-read comes a second after the read began to wait; the push lands
    // about 50 ms in, and the wake with it.
    assert!(
        took < Duration::from_millis(800),
        "woken by the re-read, not by the queue's first push: {took:?}"
    );
    let f = Arc::try_unwrap(f).ok().expect("one owner");
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ------------------------------------------------- shards through the indexes

/// FNV-1a, 32 bit, over the name's UTF-8 bytes: the shard of `count` a
/// partition is in, computed as a client computes it.
fn shard_of(partition: &str, count: u32) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for b in partition.as_bytes() {
        h ^= u32::from(*b);
        h = h.wrapping_mul(0x0100_0193);
    }
    h % count
}

/// A facade whose queue logs roll every few kilobytes: its pushes seal
/// files, each with its index.
fn open_small(dir: &Path) -> RaftFacade {
    use crate::rsm::facade::real::TEST_SEGMENT_BYTES;
    TEST_SEGMENT_BYTES.with(|c| c.set(Some(4 << 10)));
    let f = RaftFacade::open(&build_ctx(dir));
    TEST_SEGMENT_BYTES.with(|c| c.set(None));
    f.expect("open facade")
}

/// The index files of the sealed queue-log files under `dir`.
fn index_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for log in std::fs::read_dir(dir.join("qlog"))
        .expect("qlog dir")
        .flatten()
    {
        let Ok(files) = std::fs::read_dir(log.path()) else {
            continue;
        };
        out.extend(
            files
                .flatten()
                .map(|f| f.path())
                .filter(|p| p.extension().is_some_and(|e| e == "qidx")),
        );
    }
    out.sort();
    out
}

/// Pushes to `orders` (40 partitions), `payments` (12) and `settings` (3).
/// Each round: one push of twelve messages of about `pad` bytes (one
/// position, a record in each of twelve partitions), one push of a single
/// message, and a transaction over two queues (one position in two logs); and
/// now and then a setting.
async fn fill(f: &RaftFacade, rounds: u64, pad: usize) {
    let mut n = 0u64;
    for r in 0..rounds {
        let mut items = Vec::new();
        for i in 0..12 {
            n += 1;
            items.push(json!({
                "queue": "orders",
                "partition": format!("c{}", (r * 7 + i) % 40),
                "payload": {"n": n, "pad": "x".repeat(pad)},
            }));
        }
        push(f, json!(items)).await;
        n += 1;
        push(
            f,
            json!([{"queue":"payments","partition":format!("p{}", r % 12),"payload":{"n":n}}]),
        )
        .await;
        transaction(
            f,
            json!([
                {"queue":"orders","partition":format!("c{}", (r * 3) % 40),"payload":{"n":n + 1}},
                {"queue":"payments","partition":format!("p{}", (r * 5) % 12),"payload":{"n":n + 2}}
            ]),
        )
        .await;
        n += 2;
        if r % 4 == 0 {
            n += 1;
            push(
                f,
                json!([{"queue":"settings","partition":format!("s{}", r % 3),"payload":{"n":n}}]),
            )
            .await;
        }
    }
}

/// Small pages: the caps of the call cut nearly every answer.
const SMALL: (u64, u64) = (9, 900);

/// Large pages: the caps of the call cut none, and the walks of the logs stop
/// on their own budgets (a third of the bytes each, over three logs).
const LARGE: (u64, u64) = (100_000, 300 << 10);

/// Every entry a read gives from the start, in pages of at most `page`
/// (`maxRecords`, `maxBytes`), with what each page owes checked on the way: a
/// position in one page only (whole), `next` past every position of its page
/// and moving on, and a `resolvedTs` that never goes back.
async fn read_all(f: &RaftFacade, ask: Value, page: (u64, u64)) -> Vec<Value> {
    let mut from = json!("earliest");
    let mut out: Vec<Value> = Vec::new();
    let mut seen: HashSet<u64> = HashSet::new();
    let (mut last_next, mut last_resolved) = (0u64, String::new());
    for _ in 0..20_000 {
        let mut body = ask.clone();
        body["from"] = from.clone();
        body["maxRecords"] = json!(page.0);
        body["maxBytes"] = json!(page.1);
        let (status, v) = feed(f, body).await;
        assert_eq!(status, 200, "{v}");
        let page: BTreeSet<u64> = entries(&v)
            .iter()
            .map(|e| e["position"].as_u64().unwrap())
            .collect();
        for p in &page {
            assert!(seen.insert(*p), "{ask}: position {p} split over two pages");
        }
        let next = v["next"].as_u64().unwrap();
        assert!(page.iter().all(|p| *p < next), "{ask}: next {next} {v}");
        assert!(
            next > last_next,
            "{ask}: next went from {last_next} to {next}"
        );
        let resolved = v["resolvedTs"].as_str().unwrap().to_string();
        assert!(
            resolved >= last_resolved,
            "{ask}: resolvedTs went back from {last_resolved} to {resolved}"
        );
        (last_next, last_resolved) = (next, resolved);
        out.extend(entries(&v).iter().cloned());
        if v["caughtUp"] == true {
            return out;
        }
        from = json!(next);
    }
    panic!("{ask}: the read does not end");
}

/// `(position, queue, partition, offset)` of each entry.
fn keys(es: &[Value]) -> Vec<(u64, String, String, u64)> {
    es.iter()
        .map(|e| {
            (
                e["position"].as_u64().unwrap(),
                e["queue"].as_str().unwrap().to_string(),
                e["partition"].as_str().unwrap().to_string(),
                e["offset"].as_u64().unwrap(),
            )
        })
        .collect()
}

/// `got` and `want` are the same entries, payloads and all; on a difference,
/// the first one is named.
fn assert_same(got: &[Value], want: &[Value], what: &str) {
    if got == want {
        return;
    }
    let at = got.iter().zip(want).position(|(g, w)| g != w);
    panic!(
        "{what}: {} entries, want {}; first difference at {at:?}: {:?} / {:?}",
        got.len(),
        want.len(),
        at.and_then(|i| got.get(i)),
        at.and_then(|i| want.get(i)),
    );
}

/// The entries of `full` a shard of `count` reads: of the partitions in shard
/// `index`, and the whole of the queues `whole` names.
fn of_shard(full: &[Value], count: u32, index: u32, whole: &[&str]) -> Vec<Value> {
    full.iter()
        .filter(|e| {
            whole.contains(&e["queue"].as_str().unwrap())
                || shard_of(e["partition"].as_str().unwrap(), count) == index
        })
        .cloned()
        .collect()
}

/// A shard reads its partitions' records through the indexes of the sealed
/// files (and scans the open tail of each log), and answers exactly what the
/// full read gives for its partitions: for 1, 2, 3 and 16 shards, page by
/// page, whole positions, no entry twice, and every entry in one shard. In
/// small pages (the call's caps cut them) and in large ones (each log's walk
/// stops on its own budget, and the other logs' walks stop with it).
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn each_shard_reads_its_own_records_through_the_indexes() {
    let dir = scratch("shards");
    let f = open_small(&dir);
    fill(&f, 24, 640).await;
    assert!(
        index_files(&dir).len() >= 10,
        "few sealed files: {:?}",
        index_files(&dir)
    );
    let indexed = RaftFacade::feed_indexed_files_for_test();
    for page in [SMALL, LARGE] {
        let full = read_all(&f, json!({}), page).await;
        assert!(full.len() > 300, "{} entries", full.len());
        for count in [1u32, 2, 3, 16] {
            let mut union: Vec<Value> = Vec::new();
            for index in 0..count {
                let ask = json!({"shard": {"count": count, "index": index}});
                let got = read_all(&f, ask, page).await;
                assert_same(
                    &got,
                    &of_shard(&full, count, index, &[]),
                    &format!("{page:?}: shard {index} of {count}"),
                );
                union.extend(got);
            }
            let mut union = keys(&union);
            union.sort();
            let distinct: BTreeSet<_> = union.iter().cloned().collect();
            assert_eq!(
                distinct.len(),
                union.len(),
                "{count} shards: an entry twice"
            );
            assert_eq!(
                union,
                keys(&full),
                "{count} shards: the union is not the read"
            );
        }
    }
    assert!(
        RaftFacade::feed_indexed_files_for_test() > indexed,
        "no shard read a sealed file through its index"
    );

    // `since` on a shard: the index's stamps pass over what is older, unread.
    let full = read_all(&f, json!({}), SMALL).await;
    let since = full[full.len() / 2]["ts"].clone();
    let all_since = read_all(&f, json!({"since": since}), SMALL).await;
    assert!(!all_since.is_empty() && all_since.len() < full.len());
    for index in 0..3 {
        let ask = json!({"since": since, "shard": {"count": 3, "index": index}});
        let got = read_all(&f, ask, SMALL).await;
        assert_same(
            &got,
            &of_shard(&all_since, 3, index, &[]),
            &format!("since, shard {index} of 3"),
        );
    }
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// With `shard.queues`, the queues it names are split and read through their
/// indexes, and the log of a queue it leaves whole is scanned: every shard
/// reads all of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_queue_the_shard_leaves_whole_is_read_whole_by_every_shard() {
    let dir = scratch("shards-whole");
    let f = open_small(&dir);
    fill(&f, 16, 48).await;
    for page in [SMALL, LARGE] {
        let full = read_all(&f, json!({}), page).await;
        for index in 0..4 {
            let ask = json!({"shard": {"count": 4, "index": index, "queues": ["orders", "pay*"]}});
            let got = read_all(&f, ask, page).await;
            assert_same(
                &got,
                &of_shard(&full, 4, index, &["settings"]),
                &format!("{page:?}: shard {index} of 4"),
            );
        }
    }
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A shard whose index entries are mostly of partitions the read does not
/// want (`partitions` names a few) passes over them unread; past its cap of
/// entries per answer it stops at the entry of the next record it reads,
/// wanted or not, and the next page goes on from there. With a cap of a few
/// entries nearly every page ends so, and the answers are still exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_shard_that_passes_over_most_of_its_index_stops_at_whole_entries() {
    let dir = scratch("shards-cap");
    let f = open_small(&dir);
    fill(&f, 16, 48).await;
    let names = ["c1", "c4", "c9", "c16", "c25", "p3", "p7"];
    let full = read_all(&f, json!({"partitions": names}), LARGE).await;
    assert!(full.len() > 20, "{} entries", full.len());
    RaftFacade::set_feed_index_slots_for_test(3);
    for page in [SMALL, LARGE] {
        for index in 0..2 {
            let ask = json!({"partitions": names, "shard": {"count": 2, "index": index}});
            let got = read_all(&f, ask, page).await;
            assert_same(
                &got,
                &of_shard(&full, 2, index, &[]),
                &format!("{page:?}: shard {index} of 2"),
            );
        }
    }
    RaftFacade::set_feed_index_slots_for_test(0);
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A sealed file without a usable index is scanned and gives the same
/// answers: after a restart (the boot maps the indexes and checks them in the
/// background; an unchecked one is not read through), with an index the
/// restart rebuilt from its file, and once every index is checked.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_file_without_a_usable_index_gives_the_same_answers() {
    let dir = scratch("shards-reopen");
    let f = open_small(&dir);
    fill(&f, 16, 48).await;
    let ask = |index: usize| json!({"shard": {"count": 3, "index": index}});
    let mut before = Vec::new();
    for index in 0..3 {
        before.push(read_all(&f, ask(index), SMALL).await);
    }
    f.shutdown().await;

    let qidx = index_files(&dir);
    assert!(qidx.len() >= 4, "{qidx:?}");
    std::fs::remove_file(&qidx[qidx.len() / 2]).expect("remove an index");
    let f = open_small(&dir);
    for (index, want) in before.iter().enumerate() {
        let got = read_all(&f, ask(index), SMALL).await;
        assert_same(&got, want, &format!("reopened, shard {index} of 3"));
    }
    {
        let repl = f.repl_for_test();
        let reader = repl.qlog_reader().expect("qlog reader");
        reader.check_indexes().expect("check the indexes");
    }
    for (index, want) in before.iter().enumerate() {
        let got = read_all(&f, ask(index), SMALL).await;
        assert_same(&got, want, &format!("checked, shard {index} of 3"));
    }
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
