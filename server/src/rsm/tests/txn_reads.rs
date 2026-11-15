//! A transaction's KV reads, end to end through the real [`RaftFacade`]
//! (receiver, batcher, planner, apply), over a throwaway data directory per
//! test: answered at the transaction's own position in the log, as a KV
//! call's are ([`crate::rsm::kv_reads`]).
//!
//! Jepsen W32 (2026-11-15, Elle over `/api/v1/transaction`): 141 G1c cycles in
//! a 60 s run with no faults. Two transactions sent together, each reading
//! the key the other writes, both saw the other's write in 243 of 300 pairs:
//! the receiver read the state once the entry had applied, and by then the
//! later transaction's write was there.
//!
//! What they prove:
//! - two crossing transactions read as some serial order: exactly one of the
//!   two sees the other's write, never both (G1c), never neither (G2);
//! - a transaction that only reads answers what is committed, at once (it
//!   logs a no-op as its position: without one it would have none, and its
//!   receiver would wait for a rendering that never comes).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{json, Value};

use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{Deadline, ReqCtx, Rsm, RsmBuildCtx, TxnReq};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-txn-reads-{tag}-{}-{}",
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
        // A laptop's disk may be fuller than the default gate.
        disk_high_pct: 99.9,
        disk_low_pct: 99.0,
    }
}

fn ctx() -> ReqCtx {
    ReqCtx::new(
        crate::config::DEFAULT_TENANT,
        Deadline::after(Duration::from_secs(10)),
    )
}

async fn txn(f: &RaftFacade, body: Value) -> Value {
    let out = f
        .transaction(
            ctx(),
            TxnReq {
                raw: body.to_string().into_bytes(),
            },
        )
        .await
        .expect("transaction");
    serde_json::from_str(&out.body).unwrap_or_else(|e| panic!("bad JSON body: {e}\n{}", out.body))
}

/// The `found` of a committed transaction's first KV result (its get).
fn found(answer: &Value) -> bool {
    assert_eq!(answer["success"], true, "{answer}");
    let get = answer["results"]
        .as_array()
        .expect("results")
        .iter()
        .find(|r| r["type"] == "kv" && r["op"] == "get")
        .unwrap_or_else(|| panic!("no get result: {answer}"));
    get["found"]
        .as_bool()
        .unwrap_or_else(|| panic!("a get without found: {answer}"))
}

fn crossing(read: &str, write: &str) -> Value {
    json!({"kv": [
        {"op": "get", "ns": "w32", "key": read},
        {"op": "put", "ns": "w32", "key": write, "value": 1, "forever": true},
    ]})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crossing_transactions_read_as_some_serial_order() {
    let dir = scratch("crossing");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let mut both = Vec::new();
    let mut neither = Vec::new();
    for i in 0..60 {
        let (x, y) = (format!("x{i}"), format!("y{i}"));
        let (a, b) = tokio::join!(txn(&f, crossing(&x, &y)), txn(&f, crossing(&y, &x)));
        match (found(&a), found(&b)) {
            (true, true) => both.push((i, a, b)),
            (false, false) => neither.push((i, a, b)),
            _ => {}
        }
    }
    assert!(
        both.is_empty(),
        "{} of 60 pairs each read the other's write (G1c), first: {:?}",
        both.len(),
        both.first()
    );
    assert!(
        neither.is_empty(),
        "{} of 60 pairs each read before the other's write (G2), first: {:?}",
        neither.len(),
        neither.first()
    );
    drop(f);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_that_only_reads_answers_what_is_committed() {
    let dir = scratch("read-only");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let w = txn(
        &f,
        json!({"kv": [{"op": "put", "ns": "w32", "key": "r", "value": "v", "forever": true}]}),
    )
    .await;
    assert_eq!(w["success"], true, "{w}");
    let t0 = std::time::Instant::now();
    let r = txn(
        &f,
        json!({"kv": [
            {"op": "get", "ns": "w32", "key": "r"},
            {"op": "getMany", "ns": "w32", "keys": ["r", "none"]},
        ]}),
    )
    .await;
    assert!(
        t0.elapsed() < Duration::from_secs(5),
        "a read-only transaction waited {:?}",
        t0.elapsed()
    );
    assert_eq!(r["success"], true, "{r}");
    let results = r["results"].as_array().expect("results");
    assert_eq!(results[0]["found"], true, "{r}");
    assert_eq!(results[0]["value"], "v", "{r}");
    assert_eq!(results[1]["missing"], json!(["none"]), "{r}");
    drop(f);
    let _ = std::fs::remove_dir_all(&dir);
}
