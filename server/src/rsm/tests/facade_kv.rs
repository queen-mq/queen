//! WP-2.2 — KV through the real [`RaftFacade`], end to end: the receiver's pass
//! 1, the batcher and the planner, the local replicator and apply, the reads
//! off this node's applied state, and the leader's expiry sweep. Each test runs
//! over its own throwaway data directory, like `facade.rs`.
//!
//! What they prove, on the wire shapes the HTTP layer returns verbatim:
//! - put / get / delete, a CAS conflict answered with the CURRENT value and
//!   version, a mixed batch, namespaces and the console list;
//! - a shape refusal is 024's (`kv_expiry_not_specified`, 400) and a lost
//!   `required` precondition aborts the call with 024's DETAIL;
//! - N concurrent `putIfAbsent`s through the real pipeline have ONE winner;
//! - an expired key reads absent at once, and the leader sweep then removes the
//!   row and its index entry;
//! - a clean restart (shutdown + reopen) recovers the KV state and the version
//!   counter.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{Deadline, KvFailure, KvListReq, KvReq, ReqCtx, Rsm, RsmBuildCtx};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    // A small store map keeps the sparse file tiny on a laptop (§0.3 smoke).
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-facade-kv-{tag}-{}-{}",
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

fn ctx() -> ReqCtx {
    ReqCtx::new("default", Deadline::after(Duration::from_secs(10)))
}

async fn kv(f: &RaftFacade, ops: Value) -> Result<Vec<Value>, KvFailure> {
    f.kv(
        ctx(),
        KvReq {
            ops: ops.as_array().expect("an op array").clone(),
        },
    )
    .await
    .map(|o| o.results)
}

async fn ok(f: &RaftFacade, ops: Value) -> Vec<Value> {
    kv(f, ops)
        .await
        .unwrap_or_else(|e| panic!("the call failed: {e:?}"))
}

async fn one(f: &RaftFacade, op: Value) -> Value {
    ok(f, json!([op])).await.remove(0)
}

fn applied(r: &Value) -> bool {
    r["applied"] == Value::Bool(true)
}

fn version(r: &Value) -> u64 {
    r["version"].as_u64().unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn kv_calls_round_trip_through_the_raft_facade() {
    let dir = scratch("e2e");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    // ---- put / get (a key may contain slashes) ------------------------------
    let p = one(
        &f,
        json!({"op":"put","ns":"orders","key":"order/9f1/items","value":{"n":1},"ttlSeconds":600}),
    )
    .await;
    assert!(applied(&p), "{p}");
    assert_eq!(p["index"], 0);
    assert_eq!(p["value"], json!({"n":1}));
    let v1 = version(&p);
    let g = one(
        &f,
        json!({"op":"get","ns":"orders","key":"order/9f1/items"}),
    )
    .await;
    assert_eq!(g["found"], true, "{g}");
    assert_eq!(g["value"], json!({"n":1}));
    assert_eq!(version(&g), v1);
    assert!(
        g["expiresAt"].as_str().is_some(),
        "a TTL'd key carries its expiry"
    );

    // ---- CAS: the loser gets the CURRENT value and version -------------------
    let lost = one(
        &f,
        json!({"op":"put","ns":"orders","key":"order/9f1/items","value":{"n":2},
               "ttlSeconds":600,"expect":v1 + 1000}),
    )
    .await;
    assert!(!applied(&lost), "{lost}");
    assert_eq!(lost["reason"], "version");
    assert_eq!(lost["value"], json!({"n":1}));
    assert_eq!(version(&lost), v1);
    let won = one(
        &f,
        json!({"op":"put","ns":"orders","key":"order/9f1/items","value":{"n":2},
               "ttlSeconds":600,"expect":v1}),
    )
    .await;
    assert!(applied(&won), "{won}");
    let v2 = version(&won);
    assert!(v2 > v1);
    let pia = one(
        &f,
        json!({"op":"putIfAbsent","ns":"orders","key":"order/9f1/items","value":0,"forever":true}),
    )
    .await;
    assert!(!applied(&pia) && pia["reason"] == "exists", "{pia}");
    assert_eq!(pia["value"], json!({"n":2}));
    assert_eq!(version(&pia), v2);

    // ---- a mixed batch: index-aligned, reads see the call's writes ----------
    let batch = ok(
        &f,
        json!([
            {"op":"put","ns":"cart","key":"a","value":1,"forever":true},
            {"op":"incr","ns":"cart","key":"n","delta":5,"max":10,"ttlSeconds":60},
            {"op":"getMany","ns":"cart","keys":["a","n","zz"]},
            {"op":"get","ns":"orders","key":"order/9f1/items"},
            {"op":"getPrefix","ns":"cart","prefix":"a"},
        ]),
    )
    .await;
    assert_eq!(batch.len(), 5);
    for (i, r) in batch.iter().enumerate() {
        assert_eq!(r["index"], i, "{r}");
    }
    assert!(applied(&batch[0]) && applied(&batch[1]));
    assert_eq!(batch[1]["value"], 5);
    let many: Vec<&str> = batch[2]["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["key"].as_str().unwrap())
        .collect();
    assert_eq!(many, vec!["a", "n"], "the call's own writes are visible");
    assert_eq!(batch[2]["missing"], json!(["zz"]));
    assert_eq!(batch[3]["value"], json!({"n":2}));
    assert_eq!(batch[4]["rows"][0]["key"], "a");

    // ---- delete ---------------------------------------------------------------
    let d = one(&f, json!({"op":"delete","ns":"cart","key":"a"})).await;
    assert!(applied(&d) && d["value"] == 1, "{d}");
    let g = one(&f, json!({"op":"get","ns":"cart","key":"a"})).await;
    assert_eq!(g["found"], false);

    // ---- 024's refusals --------------------------------------------------------
    match kv(&f, json!([{"op":"put","ns":"cart","key":"x","value":1}])).await {
        Err(KvFailure::Invalid { status, reason, .. }) => {
            assert_eq!((status, reason.as_str()), (400, "kv_expiry_not_specified"))
        }
        other => panic!("expected a 400: {other:?}"),
    }
    match kv(
        &f,
        json!([
            {"op":"put","ns":"cart","key":"b","value":1,"forever":true},
            {"op":"putIfAbsent","ns":"cart","key":"n","value":0,"ttlSeconds":5,"required":true},
        ]),
    )
    .await
    {
        Err(KvFailure::Precondition { detail }) => {
            let d: Value = serde_json::from_str(&detail).expect("detail JSON");
            assert_eq!(d["index"], 1);
            assert_eq!(d["reason"], "exists");
            assert_eq!(d["value"], 5, "the winner's value");
        }
        other => panic!("expected a lost precondition: {other:?}"),
    }
    let b = one(&f, json!({"op":"get","ns":"cart","key":"b"})).await;
    assert_eq!(b["found"], false, "the aborted call wrote nothing");

    // ---- the console -----------------------------------------------------------
    let ns: Value = serde_json::from_str(&f.kv_namespaces(ctx()).await.expect("namespaces"))
        .expect("namespaces JSON");
    assert_eq!(
        ns,
        json!([{"namespace":"cart","keys":1},{"namespace":"orders","keys":1}])
    );
    let list = |after: Option<&str>| KvListReq {
        namespace: "orders".into(),
        prefix: "order/".into(),
        after: after.map(str::to_string),
        limit: Some(50),
        keys_only: false,
        include_expired: true,
    };
    let page: Value =
        serde_json::from_str(&f.kv_list(ctx(), list(None)).await.expect("list")).unwrap();
    assert_eq!(page["rows"][0]["key"], "order/9f1/items", "{page}");
    assert_eq!(page["rows"][0]["expired"], false);
    assert_eq!(page["truncated"], false);
    assert!(page["bytes"].as_i64().unwrap() > 0);
    match f
        .kv_list(
            ctx(),
            KvListReq {
                namespace: "Bad Ns".into(),
                ..list(None)
            },
        )
        .await
    {
        Err(KvFailure::Invalid { status, reason, .. }) => {
            assert_eq!((status, reason.as_str()), (400, "kv_bad_namespace"))
        }
        other => panic!("expected kv_bad_namespace: {other:?}"),
    }

    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_put_if_absent_has_exactly_one_winner() {
    let dir = scratch("race");
    let f = Arc::new(RaftFacade::open(&build_ctx(&dir)).expect("open facade"));
    const N: usize = 16;
    let mut tasks = Vec::new();
    for i in 0..N {
        let f = f.clone();
        tasks.push(tokio::spawn(async move {
            one(
                &f,
                json!({"op":"putIfAbsent","ns":"kvrace","key":"order-9f1",
                       "value":{"holder":i},"ttlSeconds":60}),
            )
            .await
        }));
    }
    let mut out = Vec::new();
    for t in tasks {
        out.push(t.await.expect("join"));
    }
    let winners: Vec<&Value> = out.iter().filter(|r| applied(r)).collect();
    assert_eq!(winners.len(), 1, "exactly one winner out of {N}: {out:?}");
    let w = winners[0];
    for l in out.iter().filter(|r| !applied(r)) {
        assert_eq!(l["reason"], "exists", "{l}");
        assert_eq!(
            l["value"], w["value"],
            "the loser carries the winner's value"
        );
        assert_eq!(version(l), version(w), "and its version");
    }
    let (rows, _) = f.kv_rows_physical();
    assert_eq!(rows, 1);
    match Arc::try_unwrap(f) {
        Ok(f) => f.shutdown().await,
        Err(_) => panic!("a task still holds the facade"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn an_expired_key_reads_absent_at_once_and_the_leader_sweep_prunes_it() {
    let dir = scratch("ttl");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let r = ok(
        &f,
        json!([
            {"op":"put","ns":"marks","key":"idem:1","value":true,"ttlSeconds":1},
            {"op":"put","ns":"marks","key":"keep","value":true,"forever":true},
        ]),
    )
    .await;
    assert!(r.iter().all(applied));
    assert_eq!(
        f.kv_rows_physical(),
        (2, 1),
        "two rows, one indexed for expiry"
    );

    tokio::time::sleep(Duration::from_millis(1_150)).await;
    let g = one(&f, json!({"op":"get","ns":"marks","key":"idem:1"})).await;
    assert_eq!(
        g["found"], false,
        "§5.7: absent at once, before any sweep: {g}"
    );
    // The marker's slot is free for a new lineage.
    let again = one(
        &f,
        json!({"op":"putIfAbsent","ns":"marks","key":"idem:2","value":1,"ttlSeconds":1}),
    )
    .await;
    assert!(applied(&again));

    // The leader sweep (every second by default) removes both dead rows and
    // their index entries; the forever row stays.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if f.kv_rows_physical() == (1, 0) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the sweep never pruned: {:?}",
            f.kv_rows_physical()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let k = one(&f, json!({"op":"get","ns":"marks","key":"keep"})).await;
    assert_eq!(k["found"], true);

    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_restart_recovers_the_kv_state() {
    let dir = scratch("restart");
    let (va, vn) = {
        let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
        let a = one(
            &f,
            json!({"op":"put","ns":"s","key":"a","value":{"x":1},"forever":true}),
        )
        .await;
        let mut last = Value::Null;
        for _ in 0..3 {
            last = one(
                &f,
                json!({"op":"incr","ns":"s","key":"n","delta":1,"forever":true}),
            )
            .await;
        }
        assert_eq!(last["value"], 3);
        f.shutdown().await;
        (version(&a), version(&last))
    };
    let f = RaftFacade::open(&build_ctx(&dir)).expect("reopen facade");
    let g = one(&f, json!({"op":"get","ns":"s","key":"a"})).await;
    assert_eq!(g["value"], json!({"x":1}));
    assert_eq!(version(&g), va, "the same version, not a re-issued one");
    let n = one(
        &f,
        json!({"op":"incr","ns":"s","key":"n","delta":1,"forever":true}),
    )
    .await;
    assert_eq!(n["value"], 4, "the counter continues");
    assert!(version(&n) > vn, "and the version counter too (I18)");
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Jepsen W4 leader-deaf (G1c-realtime): a call that writes and reads was
/// answered after a request-id hit (`at: None` — a forwarding retry of an
/// entry an earlier attempt committed) from the state as it was LATER, and
/// read a write that began after its own had applied. Its reads come from its
/// own position or not at all: a hit whose rendering is gone is an unknown
/// outcome (a timeout), never later state.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_request_id_hit_never_reads_later_state() {
    let dir = scratch("rid-hit");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let mixed = || KvReq {
        ops: json!([
            {"op":"getMany","ns":"w4","keys":["y"]},
            {"op":"put","ns":"w4","key":"x","value":1,"forever":true},
        ])
        .as_array()
        .expect("ops")
        .clone(),
    };
    let first = ctx();
    let id = first.request_id;
    let got = f.kv(first, mixed()).await.expect("the first call").results;
    assert_eq!(got[0]["rows"], json!([]), "{got:?}");
    let later = one(
        &f,
        json!({"op":"put","ns":"w4","key":"y","value":2,"forever":true}),
    )
    .await;
    assert!(applied(&later), "{later}");

    // The same command again, as a forwarding retry sends it: a request-id hit.
    let mut again = ReqCtx::new("default", Deadline::after(Duration::from_millis(300)));
    again.request_id = id;
    match f.kv(again, mixed()).await {
        Ok(o) => assert_eq!(
            o.results[0]["rows"],
            json!([]),
            "a hit read later state: {:?}",
            o.results
        ),
        Err(KvFailure::Rsm(crate::rsm::facade::RsmError::Timeout)) => {}
        Err(e) => panic!("unexpected failure: {e:?}"),
    }
}
