//! A transaction whose cursor somebody else holds (`reserved`: another
//! transaction reserved it, or a checkpoint still carries its last change) is
//! prepared again on the leader until its deadline
//! ([`crate::rsm::facade::intake`]), not refused at once. End to end through
//! the real [`RaftFacade`] (receiver, intake, engine, batcher, planner,
//! apply), like `positions.rs`; the follower case on a running cluster of
//! three whole nodes, like `leader_lease.rs`.
//!
//! The hold is a transaction in flight, made by hand: `Engine::txn_prepare`
//! reserves the cursor as the route's own call does, and `Engine::txn_resolve`
//! lets it go as the batcher does for an entry it did not log.
//!
//! - [`a_transaction_on_a_held_cursor_commits_once_the_hold_ends`]: it waits
//!   while the cursor is held and commits after, its position read back.
//! - [`a_cursor_held_to_the_deadline_is_refused_reserved_after_the_wait`]: it
//!   is refused `reserved` only near its deadline, with nothing written, and
//!   commits once the hold is gone.
//! - [`a_transaction_sent_to_a_follower_waits_on_the_leader`]: forwarded, it
//!   waits on the leader the same way.
//!
//! Without the retry, each was refused `reserved` at once, its cursor still
//! held. Jepsen's Kafka workload met it (2026-11-19): ten transactional
//! producers commit one group's offsets on the same partitions, and the
//! facade answered the refusal with a fatal INVALID_TXN_STATE.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::leader_lease::{close, cluster_config, free_ports, leading, open, ONE_AT_A_TIME};
use crate::rsm::consume::Engine;
use crate::rsm::entry::RequestId;
use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{ApiReq, Deadline, PushReq, ReqCtx, Rsm, RsmBuildCtx, TxnReq};
use crate::rsm::planner::txn::TxnCommand;

static SEQ: AtomicU64 = AtomicU64::new(0);

const TENANT: &str = crate::config::DEFAULT_TENANT;

/// How long a test holds the cursor before it looks: far past the first
/// tries of a transaction that waits for it.
const HOLD: Duration = Duration::from_millis(300);

fn scratch(tag: &str) -> PathBuf {
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-txn-reserved-{tag}-{}-{}",
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
        disk_high_pct: 85.0,
        disk_low_pct: 80.0,
    }
}

fn ctx(budget: Duration) -> ReqCtx {
    ReqCtx::new(TENANT, Deadline::after(budget))
}

fn parse(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("bad JSON body: {e}\n{body}"))
}

async fn push(f: &RaftFacade, queue: &str, partition: &str, n: usize) {
    let items: Vec<Value> = (0..n)
        .map(|i| json!({"queue": queue, "partition": partition, "payload": {"n": i}}))
        .collect();
    let out = f
        .push(
            ctx(Duration::from_secs(10)),
            PushReq {
                raw: json!({ "items": items }).to_string().into_bytes(),
            },
        )
        .await
        .expect("push");
    for it in parse(&out.body).as_array().expect("push array") {
        assert_eq!(it["status"], "queued", "{it}");
    }
}

/// One `positions` entry of a transaction body.
fn set(queue: &str, partition: &str, group: &str, offset: u64) -> Value {
    json!({
        "queue": queue,
        "partition": partition,
        "consumerGroup": group,
        "offset": offset,
        "metadata": "",
    })
}

/// A transaction that sets `group`'s position on `queue`/`partition`.
fn position_body(queue: &str, partition: &str, group: &str, offset: u64) -> Value {
    json!({ "positions": [set(queue, partition, group, offset)] })
}

async fn txn(f: &RaftFacade, body: Value, budget: Duration) -> Value {
    let out = f
        .transaction(
            ctx(budget),
            TxnReq {
                raw: body.to_string().into_bytes(),
            },
        )
        .await
        .expect("transaction");
    parse(&out.body)
}

/// `group`'s position on `queue`/`partition` (`null`: none).
async fn position(f: &RaftFacade, group: &str, queue: &str, partition: &str) -> Value {
    let out = f
        .api(
            ctx(Duration::from_secs(10)),
            ApiReq {
                method: "POST".into(),
                path: "/api/v1/consumer-groups/positions".into(),
                query: None,
                body: json!({
                    "consumerGroup": group,
                    "entries": [{"queue": queue, "partition": partition}],
                })
                .to_string()
                .into_bytes(),
            },
        )
        .await
        .expect("positions");
    assert_eq!(out.status, 200, "{}", out.body);
    parse(&out.body)["entries"][0]["offset"].clone()
}

/// [`position`] on another node than the one that answered the transaction:
/// read again for up to 5 s, as that node may apply the entry a moment later.
async fn position_on(
    f: &RaftFacade,
    group: &str,
    queue: &str,
    partition: &str,
    want: u64,
) -> Value {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        let v = position(f, group, queue, partition).await;
        if v == want || Instant::now() >= end {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Hold `group`'s cursor on `queue`/`partition` as a transaction in flight
/// does: its consumption half prepared, the cursor reserved, its entry not
/// landed yet. Its request id lets it go ([`Engine::txn_resolve`]).
async fn hold(engine: &Engine, queue: &str, partition: &str, group: &str) -> RequestId {
    let positions = crate::rsm::planner::positions::parse_position_ops(
        &[set(queue, partition, group, 2)],
        TENANT,
    )
    .expect("a valid position");
    let t = TxnCommand {
        request_id: crate::util::uuidv7_bytes(),
        tenant: TENANT.to_string(),
        pushes: Vec::new(),
        acks: Vec::new(),
        positional_acks: Vec::new(),
        kv: Vec::new(),
        timers: Vec::new(),
        extra_effects: Vec::new(),
        allow_duplicate: false,
        positions,
        engine_acks: Default::default(),
    };
    // Until the engine reserves the cursor. A node elected a moment ago: its
    // engine may not lead yet. And a cluster's leader answers a push once its
    // entry commits, a moment before it applies it: until then its engine
    // finds no such queue, or the queue without its partition, and then
    // leaves the position to the planner and reserves nothing (Linux: 3 runs
    // in 40).
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        let not_yet = match engine.txn_prepare(&t, engine.now_us()) {
            Ok(Some(part)) if part.planner_positions.is_empty() => return t.request_id,
            Ok(Some(_)) => "the partition is not applied yet".to_string(),
            Ok(None) => panic!("a position is a consumption half"),
            Err(r) if r.retryable || r.code == "queue_not_found" => {
                format!("{} {}", r.code, r.message)
            }
            Err(r) => panic!("the holder does not prepare: {} {}", r.code, r.message),
        };
        assert!(
            Instant::now() < end,
            "the holder holds no cursor after 10 s: {not_yet}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Answered while its cursor was still held: the retry is gone.
async fn assert_still_waiting(
    waiting: tokio::task::JoinHandle<Value>,
) -> tokio::task::JoinHandle<Value> {
    tokio::time::sleep(HOLD).await;
    if waiting.is_finished() {
        let out = waiting.await.expect("join");
        panic!("a transaction answered while its cursor was held: {out}");
    }
    waiting
}

async fn shut(f: Arc<RaftFacade>, dir: &Path) {
    let end = Instant::now() + Duration::from_secs(10);
    let mut f = f;
    loop {
        match Arc::try_unwrap(f) {
            Ok(f) => break f.shutdown().await,
            Err(still) => {
                assert!(Instant::now() < end, "the facade is still shared at close");
                f = still;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_transaction_on_a_held_cursor_commits_once_the_hold_ends() {
    let dir = scratch("waits");
    let f = Arc::new(RaftFacade::open(&build_ctx(&dir)).expect("open facade"));
    push(&f, "orders", "0", 5).await;
    let engine = f.engine_for_test();
    let holder = hold(&engine, "orders", "0", "billing").await;

    let waiting = {
        let f = Arc::clone(&f);
        tokio::spawn(async move {
            txn(
                &f,
                position_body("orders", "0", "billing", 4),
                Duration::from_secs(10),
            )
            .await
        })
    };
    let waiting = assert_still_waiting(waiting).await;
    engine.txn_resolve(holder, false);
    let out = waiting.await.expect("join");
    assert_eq!(out["success"], true, "{out}");
    assert_eq!(position(&f, "billing", "orders", "0").await, 4);

    drop(engine);
    shut(f, &dir).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_cursor_held_to_the_deadline_is_refused_reserved_after_the_wait() {
    let dir = scratch("deadline");
    let f = Arc::new(RaftFacade::open(&build_ctx(&dir)).expect("open facade"));
    push(&f, "orders", "0", 5).await;
    let engine = f.engine_for_test();
    let holder = hold(&engine, "orders", "0", "billing").await;

    let body = position_body("orders", "0", "billing", 4);
    let budget = Duration::from_millis(900);
    let started = Instant::now();
    let out = txn(&f, body.clone(), budget).await;
    let waited = started.elapsed();
    assert_eq!(out["success"], false, "{out}");
    assert_eq!(out["reason"], "reserved", "{out}");
    // Tried until the deadline less the answer margin (100 ms) and the last
    // wait (50 ms at most): answered at once, the retry is gone.
    assert!(
        waited >= Duration::from_millis(600),
        "refused after {waited:?} of its {budget:?}: it did not wait for the hold"
    );
    // A definite refusal: nothing of it was written.
    assert_eq!(position(&f, "billing", "orders", "0").await, Value::Null);

    // The hold gone, the same transaction commits.
    engine.txn_resolve(holder, false);
    let out = txn(&f, body, Duration::from_secs(10)).await;
    assert_eq!(out["success"], true, "{out}");
    assert_eq!(position(&f, "billing", "orders", "0").await, 4);

    drop(engine);
    shut(f, &dir).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_transaction_sent_to_a_follower_waits_on_the_leader() {
    let _one = ONE_AT_A_TIME.lock().await;
    let dirs: Vec<PathBuf> = (1..=3).map(|i| scratch(&format!("cluster-n{i}"))).collect();
    for d in &dirs {
        std::fs::create_dir_all(d).expect("scratch dir");
    }
    let ports = free_ports(3);
    // Together: a fresh node waits for its peers before it answers anything.
    let opening: Vec<_> = (0..3)
        .map(|i| {
            let d = dirs[i].clone();
            let c = cluster_config(&ports, i as u64 + 1);
            tokio::spawn(async move { open(&d, c).await })
        })
        .collect();
    let mut nodes: Vec<Option<Arc<RaftFacade>>> = Vec::new();
    for o in opening {
        nodes.push(Some(o.await.expect("open")));
    }

    let end = Instant::now() + Duration::from_secs(30);
    let l = loop {
        if let [l] = leading(&nodes)[..] {
            break l;
        }
        assert!(Instant::now() < end, "no leader within 30 s");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let leader = Arc::clone(nodes[l].as_ref().expect("open"));
    let follower = Arc::clone(nodes[(l + 1) % 3].as_ref().expect("open"));
    push(&leader, "orders", "0", 5).await;
    let engine = leader.engine_for_test();
    let holder = hold(&engine, "orders", "0", "billing").await;

    let waiting = {
        let follower = Arc::clone(&follower);
        tokio::spawn(async move {
            txn(
                &follower,
                position_body("orders", "0", "billing", 4),
                Duration::from_secs(10),
            )
            .await
        })
    };
    let waiting = assert_still_waiting(waiting).await;
    engine.txn_resolve(holder, false);
    let out = waiting.await.expect("join");
    assert_eq!(out["success"], true, "{out}");
    assert_eq!(position_on(&leader, "billing", "orders", "0", 4).await, 4);

    drop(engine);
    drop(leader);
    drop(follower);
    for n in nodes.iter_mut() {
        if let Some(f) = n.take() {
            close(f).await;
        }
    }
    for d in &dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}
