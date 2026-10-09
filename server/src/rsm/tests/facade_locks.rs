//! Locks through the real [`RaftFacade`], end to end: `crate::locks` turning
//! each operation into KV calls, the batcher and the planner, apply, and the
//! reads off this node's applied state. Each test runs over its own throwaway
//! data directory, like `facade_kv.rs`.
//!
//! What they prove, on the answers `POST /api/v1/locks` returns verbatim:
//! - a lock has one holder; the owner that holds it is answered its own
//!   permit again; a renew hands out a new token and retires the old one; a
//!   release needs the current one;
//! - the guard commits a transaction while the caller holds the permit and
//!   rolls it back once the permit is somebody else's;
//! - a lost answer is recovered through the owner (acquire, renew), and never
//!   by somebody else's;
//! - an expired lock goes to the next caller with a higher token;
//! - a semaphore never grants more than its limit, under a crowd too;
//! - one call carries many locks, and its rows travel in one KV write.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::locks;
use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{Deadline, KvReq, ReqCtx, Rsm, RsmBuildCtx, TxnReq};

static SEQ: AtomicU64 = AtomicU64::new(0);
const TENANT: &str = "default";

fn scratch(tag: &str) -> PathBuf {
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-facade-locks-{tag}-{}-{}",
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

/// One call of `tenant`, as the route makes it.
async fn call_as(f: &RaftFacade, tenant: &str, ops: Value) -> Vec<Value> {
    let ops = locks::parse_ops(ops.as_array().expect("an array")).expect("a valid call");
    locks::apply(f, tenant, Deadline::after(Duration::from_secs(10)), &ops)
        .await
        .unwrap_or_else(|e| panic!("the call failed: {e:?}"))
}

async fn call(f: &RaftFacade, ops: Value) -> Vec<Value> {
    call_as(f, TENANT, ops).await
}

async fn one(f: &RaftFacade, op: Value) -> Value {
    call(f, json!([op])).await.remove(0)
}

fn token(r: &Value) -> u64 {
    r["token"].as_u64().unwrap_or(0)
}

/// A transaction whose only KV op is `guard` beside a push to `queue`.
async fn guarded_push(f: &RaftFacade, guard: &Value, queue: &str, id: &str) -> Value {
    let body = json!({
        "operations": [{"type": "push", "items": [
            {"queue": queue, "payload": {"id": id}, "transactionId": id}]}],
        "kv": [guard],
    });
    let out = f
        .transaction(
            ReqCtx::new(TENANT, Deadline::after(Duration::from_secs(10))),
            TxnReq {
                raw: body.to_string().into_bytes(),
            },
        )
        .await
        .expect("transaction");
    serde_json::from_str(&out.body).expect("json")
}

/// A node alone raises the cluster version to its own on its first tick; the
/// guard is a `check`, which needs it at 5.
async fn until_checks_are_served(f: &RaftFacade) {
    let end = Instant::now() + Duration::from_secs(10);
    while !f.cluster_allows(crate::rsm::effect::VERSION_5) {
        assert!(Instant::now() < end, "the cluster version never reached 5");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_lock_is_held_by_one_renewed_guarded_and_released() {
    let dir = scratch("lock");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    until_checks_are_served(&f).await;

    // ---- acquire: one holder ------------------------------------------------
    let a = one(
        &f,
        json!({"op":"acquire","name":"daily-report","ttlSeconds":30,"owner":"a"}),
    )
    .await;
    assert_eq!(a["acquired"], true, "{a}");
    assert_eq!(
        (a["index"].as_u64(), a["op"].as_str()),
        (Some(0), Some("acquire"))
    );
    assert_eq!(a["name"], "daily-report");
    assert_eq!(a["slot"], 0);
    assert_eq!(a["owner"], "a");
    assert!(a.get("already").is_none(), "a first acquire: {a}");
    let t0 = token(&a);
    assert!(t0 > 0);
    assert_eq!(
        a["guard"],
        json!({"op":"check","ns":"queen-locks","key":"daily-report#0","expect":t0,
               "required":true})
    );

    let b = one(
        &f,
        json!({"op":"acquire","name":"daily-report","ttlSeconds":30,"owner":"b"}),
    )
    .await;
    assert_eq!(b["acquired"], false, "{b}");
    assert_eq!(b["reason"], "held");
    assert_eq!(b["holders"], json!([{"slot":0,"owner":"a"}]));
    assert!(b.get("token").is_none() && b.get("guard").is_none(), "{b}");
    // Without an owner a caller is nobody in particular: held, like b.
    let anon = one(
        &f,
        json!({"op":"acquire","name":"daily-report","ttlSeconds":30}),
    )
    .await;
    assert_eq!(anon["acquired"], false, "{anon}");

    // The owner that holds it is answered its own permit, unchanged: what a
    // retry of an acquire whose answer was lost gets.
    let again = one(
        &f,
        json!({"op":"acquire","name":"daily-report","ttlSeconds":30,"owner":"a"}),
    )
    .await;
    assert_eq!(again["acquired"], true, "{again}");
    assert_eq!(again["already"], true);
    assert_eq!(token(&again), t0, "the same permit, not a new one");

    // ---- get ------------------------------------------------------------------
    let g = one(&f, json!({"op":"get","name":"daily-report"})).await;
    assert_eq!(g["held"], true, "{g}");
    let h = &g["holders"][0];
    assert_eq!(
        (h["slot"].as_u64(), h["owner"].as_str()),
        (Some(0), Some("a"))
    );
    assert_eq!(h["token"].as_u64(), Some(t0));
    assert!(
        h["expiresAt"].is_string() && h["renewedAt"].is_string(),
        "{g}"
    );
    // Never renewed: it was taken when it was written.
    let since = h["since"].clone();
    assert!(since.is_string() && since == h["renewedAt"], "{g}");

    // ---- the guard --------------------------------------------------------------
    let ok = guarded_push(&f, &a["guard"], "reports", "r1").await;
    assert_eq!(ok["success"], true, "{ok}");

    // ---- renew: a new token, and the old one is retired ---------------------------
    let r1 = one(
        &f,
        json!({"op":"renew","name":"daily-report","token":t0,"ttlSeconds":30,"owner":"a"}),
    )
    .await;
    assert_eq!(r1["renewed"], true, "{r1}");
    let t1 = token(&r1);
    assert!(t1 > t0, "a later write on the row: {t1} after {t0}");
    assert_eq!(r1["guard"]["expect"].as_u64(), Some(t1));
    let old_guard = guarded_push(&f, &a["guard"], "reports", "r2").await;
    assert_eq!(old_guard["success"], false, "{old_guard}");
    assert_eq!(old_guard["reason"], "kv_precondition");
    assert_eq!(old_guard["version"].as_u64(), Some(t1));
    let new_guard = guarded_push(&f, &r1["guard"], "reports", "r3").await;
    assert_eq!(new_guard["success"], true, "{new_guard}");

    // A renew sent again with the token it had before (its answer was lost):
    // the row is this owner's, so it is carried through at the current token.
    let r2 = one(
        &f,
        json!({"op":"renew","name":"daily-report","token":t0,"ttlSeconds":30,"owner":"a"}),
    )
    .await;
    assert_eq!(r2["renewed"], true, "{r2}");
    let t2 = token(&r2);
    assert!(t2 > t1);
    // Somebody else's stale token is not: lost, and it is told who holds it.
    for stale in [
        json!({"op":"renew","name":"daily-report","token":t0,"ttlSeconds":30,"owner":"b"}),
        json!({"op":"renew","name":"daily-report","token":t0,"ttlSeconds":30}),
    ] {
        let r = one(&f, stale.clone()).await;
        assert_eq!(r["renewed"], false, "{stale} -> {r}");
        assert_eq!(r["reason"], "lost");
        assert_eq!(r["holders"], json!([{"slot":0,"owner":"a"}]));
        assert!(r.get("token").is_none(), "{r}");
    }
    // An owner-less renew with the current token keeps the row's owner.
    let r3 = one(
        &f,
        json!({"op":"renew","name":"daily-report","token":t2,"ttlSeconds":30}),
    )
    .await;
    assert_eq!(r3["renewed"], true, "{r3}");
    let t3 = token(&r3);
    let g = one(&f, json!({"op":"get","name":"daily-report"})).await;
    assert_eq!(g["holders"][0]["owner"], "a", "{g}");
    assert_eq!(g["holders"][0]["token"].as_u64(), Some(t3));
    // Three renewals later, by token, by a stale token and without an owner:
    // `since` is still the acquire's, and the row carries it.
    let h = &g["holders"][0];
    assert_eq!(h["since"], since, "a renewal moved since: {g}");
    let ms = |v: &Value| crate::util::parse_iso_ms(v.as_str().unwrap_or("")).expect("a time");
    assert!(ms(&h["renewedAt"]) >= ms(&since), "{g}");
    assert!(ms(&h["expiresAt"]) > ms(&h["renewedAt"]), "{g}");
    let row = f
        .kv(
            ReqCtx::new(TENANT, Deadline::after(Duration::from_secs(5))),
            KvReq {
                ops: vec![json!({"op":"get","ns":locks::NAMESPACE,"key":"daily-report#0"})],
            },
        )
        .await
        .expect("kv get")
        .results;
    assert_eq!(
        row[0]["value"],
        json!({"owner":"a","since":since}),
        "{row:?}"
    );

    // ---- release: the current token, once -----------------------------------------
    let stale = one(&f, json!({"op":"release","name":"daily-report","token":t1})).await;
    assert_eq!(stale["released"], false, "{stale}");
    assert_eq!(stale["reason"], "lost");
    assert_eq!(stale["holders"], json!([{"slot":0,"owner":"a"}]));
    let done = one(&f, json!({"op":"release","name":"daily-report","token":t3})).await;
    assert_eq!(done["released"], true, "{done}");
    assert_eq!(done["slot"], 0);
    let g = one(&f, json!({"op":"get","name":"daily-report"})).await;
    assert_eq!(g["held"], false, "{g}");
    assert_eq!(g["holders"], json!([]));
    // Released twice, renewed after: there is nothing to hold.
    let twice = one(&f, json!({"op":"release","name":"daily-report","token":t3})).await;
    assert_eq!(twice["released"], false, "{twice}");
    assert_eq!(twice["holders"], json!([]));
    let late = one(
        &f,
        json!({"op":"renew","name":"daily-report","token":t3,"ttlSeconds":30,"owner":"a"}),
    )
    .await;
    assert_eq!(late["renewed"], false, "{late}");
    assert_eq!(late["reason"], "lost");
    let gone = guarded_push(&f, &r3["guard"], "reports", "r4").await;
    assert_eq!(gone["success"], false, "{gone}");
    assert_eq!(gone["kvReason"], "absent");

    // And b takes it, with a token above every one a ever held.
    let b = one(
        &f,
        json!({"op":"acquire","name":"daily-report","ttlSeconds":30,"owner":"b"}),
    )
    .await;
    assert_eq!(b["acquired"], true, "{b}");
    assert!(token(&b) > t3);

    // The permits are KV rows and nothing else: one row, in the lock namespace.
    let (rows, _) = f.kv_rows_physical();
    assert_eq!(rows, 1);
    let kv = f
        .kv(
            ReqCtx::new(TENANT, Deadline::after(Duration::from_secs(5))),
            KvReq {
                ops: vec![json!({"op":"get","ns":locks::NAMESPACE,"key":"daily-report#0"})],
            },
        )
        .await
        .expect("kv get")
        .results;
    assert_eq!(kv[0]["value"], json!({"owner":"b"}), "{kv:?}");
    assert_eq!(kv[0]["version"].as_u64(), Some(token(&b)));

    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn an_expired_lock_goes_to_the_next_and_the_old_holder_is_fenced() {
    let dir = scratch("expiry");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    until_checks_are_served(&f).await;

    let a = one(
        &f,
        json!({"op":"acquire","name":"job","ttlSeconds":1,"owner":"a"}),
    )
    .await;
    assert_eq!(a["acquired"], true, "{a}");
    // a stops renewing (paused, partitioned, dead). Nobody tells it.
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    let g = one(&f, json!({"op":"get","name":"job"})).await;
    assert_eq!(g["held"], false, "an expired permit is not held: {g}");

    let b = one(
        &f,
        json!({"op":"acquire","name":"job","ttlSeconds":60,"owner":"b"}),
    )
    .await;
    assert_eq!(b["acquired"], true, "{b}");
    assert!(
        token(&b) > token(&a),
        "the next holder's token is above the last one's"
    );

    // a comes back believing it holds the lock. Everything it does is refused.
    let step = guarded_push(&f, &a["guard"], "jobs", "late-step").await;
    assert_eq!(step["success"], false, "{step}");
    assert_eq!(step["reason"], "kv_precondition");
    assert_eq!(step["value"], json!({"owner":"b"}));
    let renew = one(
        &f,
        json!({"op":"renew","name":"job","token":token(&a),"ttlSeconds":60,"owner":"a"}),
    )
    .await;
    assert_eq!(renew["renewed"], false, "{renew}");
    assert_eq!(renew["holders"], json!([{"slot":0,"owner":"b"}]));
    let release = one(&f, json!({"op":"release","name":"job","token":token(&a)})).await;
    assert_eq!(release["released"], false, "{release}");
    // And b is untouched by all of it.
    let ok = guarded_push(&f, &b["guard"], "jobs", "b-step").await;
    assert_eq!(ok["success"], true, "{ok}");

    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_semaphore_never_grants_more_than_its_limit() {
    let dir = scratch("semaphore");
    let f = Arc::new(RaftFacade::open(&build_ctx(&dir)).expect("open facade"));
    const LIMIT: usize = 3;
    const CROWD: usize = 24;

    // A crowd at once: each reads the same free slots and tries one.
    let mut tasks = Vec::new();
    for i in 0..CROWD {
        let f = f.clone();
        tasks.push(tokio::spawn(async move {
            one(
                &f,
                json!({"op":"acquire","name":"gpu","ttlSeconds":60,"limit":LIMIT,
                       "owner":format!("w-{i}")}),
            )
            .await
        }));
    }
    let mut out = Vec::new();
    for t in tasks {
        out.push(t.await.expect("join"));
    }
    let winners: Vec<&Value> = out.iter().filter(|r| r["acquired"] == true).collect();
    assert!(
        (1..=LIMIT).contains(&winners.len()),
        "{} permits granted of {LIMIT}: {out:?}",
        winners.len()
    );
    let mut slots: Vec<u64> = winners
        .iter()
        .map(|w| w["slot"].as_u64().unwrap())
        .collect();
    slots.sort_unstable();
    slots.dedup();
    assert_eq!(slots.len(), winners.len(), "a slot has one holder: {out:?}");
    assert!(slots.iter().all(|s| (*s as usize) < LIMIT));
    for l in out.iter().filter(|r| r["acquired"] != true) {
        let why = l["reason"].as_str().unwrap_or("");
        assert!(why == "held" || why == "contended", "{l}");
    }
    let (rows, _) = f.kv_rows_physical();
    assert_eq!(rows as usize, winners.len(), "one row per permit granted");

    // One at a time, the semaphore fills to its limit and not past it.
    let mut held: Vec<Value> = winners.into_iter().cloned().collect();
    for i in 0..LIMIT {
        if held.len() == LIMIT {
            break;
        }
        let r = one(
            &f,
            json!({"op":"acquire","name":"gpu","ttlSeconds":60,"limit":LIMIT,
                   "owner":format!("late-{i}")}),
        )
        .await;
        assert_eq!(
            r["acquired"], true,
            "a free permit goes to a lone caller: {r}"
        );
        held.push(r);
    }
    let full = one(
        &f,
        json!({"op":"acquire","name":"gpu","ttlSeconds":60,"limit":LIMIT,"owner":"one-more"}),
    )
    .await;
    assert_eq!(full["acquired"], false, "{full}");
    assert_eq!(full["reason"], "held");
    assert_eq!(full["holders"].as_array().map(Vec::len), Some(LIMIT));

    // A holder that asks again is answered the permit it has.
    let mine = &held[0];
    let again = one(
        &f,
        json!({"op":"acquire","name":"gpu","ttlSeconds":60,"limit":LIMIT,"owner":mine["owner"]}),
    )
    .await;
    assert_eq!(again["acquired"], true, "{again}");
    assert_eq!(again["already"], true);
    assert_eq!(again["slot"], mine["slot"]);
    assert_eq!(token(&again), token(mine));

    // A release frees exactly that slot, and the next caller gets it.
    let freed = mine["slot"].as_u64().unwrap();
    let rel = one(
        &f,
        json!({"op":"release","name":"gpu","slot":freed,"token":token(mine)}),
    )
    .await;
    assert_eq!(rel["released"], true, "{rel}");
    let next = one(
        &f,
        json!({"op":"acquire","name":"gpu","ttlSeconds":60,"limit":LIMIT,"owner":"next"}),
    )
    .await;
    assert_eq!(next["acquired"], true, "{next}");
    assert_eq!(next["slot"].as_u64(), Some(freed));
    assert!(token(&next) > token(mine));

    // `get` lists the holders by slot, whatever limit they came with.
    let g = one(&f, json!({"op":"get","name":"gpu"})).await;
    let listed: Vec<u64> = g["holders"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["slot"].as_u64().unwrap())
        .collect();
    assert_eq!(listed, vec![0, 1, 2], "{g}");

    match Arc::try_unwrap(f) {
        Ok(f) => f.shutdown().await,
        Err(_) => panic!("a task still holds the facade"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The limit is the caller's and is stored nowhere. A lock is the semaphore
/// of one, so callers that disagree about it share slot 0, and while a limit
/// is being changed (a rolling deploy) the larger one rules.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn callers_that_disagree_about_the_limit_share_the_low_slots() {
    let dir = scratch("resize");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let lock = one(
        &f,
        json!({"op":"acquire","name":"pool","ttlSeconds":60,"owner":"old"}),
    )
    .await;
    assert_eq!(
        (lock["acquired"].as_bool(), lock["slot"].as_u64()),
        (Some(true), Some(0))
    );
    // A caller already on the new limit of 2 takes the slot the old ones do
    // not look at.
    let wide = one(
        &f,
        json!({"op":"acquire","name":"pool","ttlSeconds":60,"limit":2,"owner":"new-1"}),
    )
    .await;
    assert_eq!(
        (wide["acquired"].as_bool(), wide["slot"].as_u64()),
        (Some(true), Some(1))
    );
    let wide_full = one(
        &f,
        json!({"op":"acquire","name":"pool","ttlSeconds":60,"limit":2,"owner":"new-2"}),
    )
    .await;
    assert_eq!(wide_full["acquired"], false, "{wide_full}");
    assert_eq!(wide_full["holders"].as_array().map(Vec::len), Some(2));
    // An old caller sees its one slot, and it is taken.
    let narrow = one(
        &f,
        json!({"op":"acquire","name":"pool","ttlSeconds":60,"owner":"old-2"}),
    )
    .await;
    assert_eq!(narrow["acquired"], false, "{narrow}");
    assert_eq!(narrow["holders"], json!([{"slot":0,"owner":"old"}]));
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// The wire is the protocol crate's: what its types write is what this route
/// reads, field for field, and what this route answers is what its types
/// read. A field renamed on one side fails here, not at a client.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn the_wire_conforms_to_the_protocol_crate() {
    use queen_protocol::locks::{
        LockOperation, LockReason, LockRequest, LockResponse, LOCK_MAX_LIMIT, LOCK_NAMESPACE,
    };

    assert_eq!(LOCK_NAMESPACE, locks::NAMESPACE);
    assert_eq!(LOCK_MAX_LIMIT, locks::MAX_LIMIT);

    let dir = scratch("conformance");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    // A call written by the protocol types, answered by the broker, read back
    // by the protocol types.
    let exchange = |ops: Vec<LockOperation>| {
        let f = &f;
        async move {
            let body = serde_json::to_value(LockRequest::new(ops)).expect("request");
            let answers = call(f, body["operations"].clone()).await;
            let parsed: LockResponse =
                serde_json::from_value(json!({ "results": answers })).expect("the answer parses");
            parsed.results
        }
    };

    let a = exchange(vec![LockOperation::acquire("job", 30).owner("a")])
        .await
        .remove(0);
    assert!(a.acquired(), "{a:?}");
    assert_eq!((a.op.as_str(), a.name.as_str()), ("acquire", "job"));
    let token = a.token.expect("a token");
    let guard = a.guard.clone().expect("a guard");
    // The guard the types read is the op the KV route takes.
    until_checks_are_served(&f).await;
    let held = f
        .kv(
            ReqCtx::new(TENANT, Deadline::after(Duration::from_secs(5))),
            KvReq {
                ops: vec![serde_json::to_value(&guard).expect("guard")],
            },
        )
        .await
        .expect("the guard, as a kv call")
        .results;
    assert_eq!(held[0]["applied"], true, "{held:?}");

    let b = exchange(vec![LockOperation::acquire("job", 30).owner("b")])
        .await
        .remove(0);
    assert!(!b.acquired());
    assert_eq!(b.reason, Some(LockReason::Held));
    assert_eq!(b.holders[0].owner.as_deref(), Some("a"));

    let many = exchange(vec![
        LockOperation::renew("job", token, 30).owner("a"),
        LockOperation::acquire("gpu", 60).limit(4).owner("a"),
        LockOperation::get("nobody"),
    ])
    .await;
    assert!(many[0].renewed(), "{many:?}");
    let renewed_token = many[0].token.expect("a new token");
    assert!(renewed_token > token);
    assert!(many[1].acquired());
    let slot = many[1].slot.expect("a slot");
    assert!(!many[2].held() && many[2].holders.is_empty());

    let last = exchange(vec![
        LockOperation::release("job", token),
        LockOperation::release("gpu", many[1].token.unwrap()).slot(slot),
    ])
    .await;
    assert!(!last[0].released(), "a stale token: {last:?}");
    assert_eq!(last[0].reason, Some(LockReason::Lost));
    assert!(last[1].released());
    let got = exchange(vec![LockOperation::get("job")]).await.remove(0);
    assert!(got.held());
    let h = &got.holders[0];
    assert_eq!(h.token, Some(renewed_token));
    assert!(h.expires_at.is_some() && h.renewed_at.is_some());

    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn one_call_carries_many_locks_and_tenants_never_meet() {
    let dir = scratch("batch");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    let first = call(
        &f,
        json!([
            {"op":"acquire","name":"a","ttlSeconds":60,"owner":"w"},
            {"op":"get","name":"nobody"},
            {"op":"acquire","name":"b","ttlSeconds":60,"owner":"w","limit":2},
            {"op":"acquire","name":"c","ttlSeconds":60,"owner":"w"},
        ]),
    )
    .await;
    assert_eq!(first.len(), 4);
    for (i, r) in first.iter().enumerate() {
        assert_eq!(r["index"].as_u64(), Some(i as u64), "index-aligned: {r}");
    }
    assert_eq!(first[0]["acquired"], true, "{first:?}");
    assert_eq!(first[1]["held"], false);
    assert_eq!(first[2]["acquired"], true);
    assert_eq!(first[3]["acquired"], true);
    // The three rows travelled in ONE KV write: consecutive versions, in the
    // planner's apply order (by key).
    let (ta, tb, tc) = (token(&first[0]), token(&first[2]), token(&first[3]));
    let mut versions = vec![ta, tb, tc];
    versions.sort_unstable();
    assert_eq!(versions, vec![ta, ta + 1, ta + 2], "one entry: {first:?}");

    // Renew one, release one, take a new one, look at a fourth: one call.
    let second = call(
        &f,
        json!([
            {"op":"renew","name":"a","token":ta,"ttlSeconds":60,"owner":"w"},
            {"op":"release","name":"b","slot":first[2]["slot"],"token":tb},
            {"op":"acquire","name":"d","ttlSeconds":60,"owner":"w"},
            {"op":"get","name":"c"},
        ]),
    )
    .await;
    assert_eq!(second[0]["renewed"], true, "{second:?}");
    assert_eq!(second[1]["released"], true);
    assert_eq!(second[2]["acquired"], true);
    assert_eq!(second[3]["holders"][0]["token"].as_u64(), Some(tc));

    // Another tenant's lock of the same name is another lock.
    let other = call_as(
        &f,
        "tenant-b",
        json!([{"op":"acquire","name":"a","ttlSeconds":60,"owner":"x"}]),
    )
    .await;
    assert_eq!(other[0]["acquired"], true, "{other:?}");
    let mine = one(&f, json!({"op":"get","name":"a"})).await;
    assert_eq!(mine["holders"][0]["owner"], "w", "{mine}");

    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
