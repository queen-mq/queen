//! What the lock surface, `check`, and a guarded transaction put on the
//! socket, and what the handle does around them.
//!
//! Against [`support::FakeBroker`], no stack needed, for the reason
//! `kv_timers_wire.rs` gives: the body is the contract, and a wrong field
//! name is a 400 nobody can diagnose from the client side.
//!
//! What lives in the CLIENT and nowhere else, and is pinned here:
//! * the handle always sends its owner, the same one on every call;
//! * a renew's NEW token replaces the old one for the guard and the release;
//! * a guard that lost to the handle's own renewal is sent again with the new
//!   token, and one that lost to another holder is the verdict and marks the
//!   lock lost;
//! * a transaction that asked for a guard never goes out without one.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use queen_mq::{Config, LockOperation, LockOptions, LockReason, Queen};
use serde_json::{json, Value};
use support::{FakeBroker, Hit, Reply};

fn client(broker: &FakeBroker) -> Queen {
    Queen::connect(Config::new(broker.url())).expect("fake broker url")
}

fn results(element: Value) -> Reply {
    Reply::ok(json!({ "results": [element] }).to_string())
}

fn guard_of(name: &str, slot: u32, token: u64) -> Value {
    json!({"op":"check","ns":"queen-locks","key":format!("{name}#{slot}"),"expect":token,"required":true})
}

fn granted(name: &str, slot: u32, token: u64) -> Reply {
    results(
        json!({"index":0,"op":"acquire","name":name,"acquired":true,"slot":slot,
        "token":token,"owner":"o","guard":guard_of(name, slot, token)}),
    )
}

fn renewed(name: &str, slot: u32, token: u64) -> Reply {
    results(
        json!({"index":0,"op":"renew","name":name,"renewed":true,"slot":slot,
        "token":token,"guard":guard_of(name, slot, token)}),
    )
}

fn released(name: &str) -> Reply {
    results(json!({"index":0,"op":"release","name":name,"released":true,"slot":0}))
}

fn op_of(hit: &Hit) -> Value {
    hit.json()["operations"][0].clone()
}

fn manual(ttl_secs: u64) -> LockOptions {
    LockOptions::new(Duration::from_secs(ttl_secs)).manual_renew()
}

#[tokio::test]
async fn the_four_operations_send_exactly_their_fields() {
    let broker = FakeBroker::start(vec![
        granted("daily-report", 0, 100),
        renewed("gpu", 2, 101),
        released("daily-report"),
        results(json!({"index":0,"op":"get","name":"daily-report","held":false,"holders":[]})),
    ])
    .await;
    let locks = client(&broker).locks();

    let a = locks
        .send(LockOperation::acquire("daily-report", 30).owner("o"))
        .await
        .expect("an acquire must not fail");
    assert!(a.acquired());
    assert_eq!(a.token, Some(100));
    let r = locks
        .send(LockOperation::renew("gpu", 100, 60).slot(2).owner("o"))
        .await
        .unwrap();
    assert!(r.renewed());
    assert_eq!(r.token, Some(101), "a renew answers a NEW token");
    assert!(locks
        .send(LockOperation::release("daily-report", 101))
        .await
        .unwrap()
        .released());
    assert!(!locks.get("daily-report").await.unwrap().held());

    let hits = broker.hits();
    assert!(hits
        .iter()
        .all(|h| h.method == "POST" && h.route() == "/api/v1/locks"));
    assert_eq!(
        hits[0].json(),
        json!({"operations":[{"op":"acquire","name":"daily-report","ttlSeconds":30,"owner":"o"}]})
    );
    assert_eq!(
        op_of(&hits[1]),
        json!({"op":"renew","name":"gpu","ttlSeconds":60,"owner":"o","slot":2,"token":100})
    );
    assert_eq!(
        op_of(&hits[2]),
        json!({"op":"release","name":"daily-report","token":101})
    );
    assert_eq!(op_of(&hits[3]), json!({"op":"get","name":"daily-report"}));
}

#[tokio::test]
async fn a_held_lock_is_ok_false_never_an_error() {
    let broker = FakeBroker::start(vec![results(json!({
        "index":0,"op":"acquire","name":"job","acquired":false,"reason":"held",
        "holders":[{"slot":0,"owner":"other"}]
    }))])
    .await;
    let queen = client(&broker);
    let r = queen
        .locks()
        .send(LockOperation::acquire("job", 30))
        .await
        .expect("held is a verdict");
    assert!(!r.acquired());
    assert_eq!(r.reason, Some(LockReason::Held));
    assert_eq!(r.holders[0].owner.as_deref(), Some("other"));

    let lock = queen.lock_with("job", manual(30));
    assert!(!lock.acquire().await.expect("held is a verdict"));
    assert!(!lock.held() && lock.token().is_none() && lock.guard().is_none());
}

#[tokio::test]
async fn the_handle_names_its_owner_and_follows_a_renew() {
    let broker = FakeBroker::start(vec![
        granted("job", 0, 100),
        renewed("job", 0, 101),
        released("job"),
    ])
    .await;
    let queen = client(&broker);
    let lock = queen.lock_with("job", manual(30));
    let other = queen.lock_with("job", manual(30));
    assert_ne!(lock.owner(), other.owner(), "an owner per handle");

    assert!(lock.acquire().await.unwrap());
    assert!(lock.held());
    assert_eq!((lock.token(), lock.slot()), (Some(100), Some(0)));
    assert!(lock.acquire().await.unwrap(), "already held: no call");

    assert!(lock.renew().await.unwrap());
    assert_eq!(lock.token(), Some(101));
    assert_eq!(
        serde_json::to_value(lock.guard().unwrap()).unwrap(),
        guard_of("job", 0, 101)
    );

    assert!(lock.release().await.unwrap());
    assert!(!lock.held() && !lock.is_lost(), "a release is not a loss");
    assert!(!lock.release().await.unwrap(), "nothing left to give back");

    let hits = broker.hits();
    assert_eq!(hits.len(), 3);
    assert_eq!(
        op_of(&hits[0]),
        json!({"op":"acquire","name":"job","ttlSeconds":30,"owner":lock.owner()})
    );
    assert_eq!(
        op_of(&hits[1]),
        json!({"op":"renew","name":"job","ttlSeconds":30,"owner":lock.owner(),"slot":0,"token":100})
    );
    assert_eq!(
        op_of(&hits[2]),
        json!({"op":"release","name":"job","slot":0,"token":101})
    );
}

#[tokio::test]
async fn a_semaphore_handle_sends_its_limit_and_keeps_its_slot() {
    let broker = FakeBroker::start(vec![granted("gpu", 3, 9), released("gpu")]).await;
    let queen = client(&broker);
    let permit = queen.lock_with("gpu", manual(60).limit(4));
    assert!(permit.acquire().await.unwrap());
    assert_eq!(permit.slot(), Some(3));
    permit.release().await.unwrap();
    let hits = broker.hits();
    assert_eq!(op_of(&hits[0])["limit"], 4);
    assert_eq!(
        op_of(&hits[1]),
        json!({"op":"release","name":"gpu","slot":3,"token":9})
    );
    // A lock sends no limit at all.
    let broker = FakeBroker::start(vec![granted("job", 0, 1)]).await;
    let lock = client(&broker).lock("job", Duration::from_secs(30));
    lock.acquire().await.unwrap();
    assert!(op_of(&broker.hits()[0]).get("limit").is_none());
}

#[tokio::test]
async fn a_waiting_acquire_comes_back_until_the_permit_is_free() {
    let held = || {
        results(
            json!({"index":0,"op":"acquire","name":"job","acquired":false,
            "reason":"held","holders":[{"slot":0,"owner":"other"}]}),
        )
    };
    let broker = FakeBroker::start(vec![held(), held(), granted("job", 0, 5)]).await;
    let lock = client(&broker).lock_with(
        "job",
        manual(30).retry(Duration::from_millis(5), Duration::from_millis(10)),
    );
    assert!(lock.acquire_within(Duration::from_secs(5)).await.unwrap());
    assert_eq!(broker.hit_count(), 3);

    let broker = FakeBroker::start(vec![held()]).await;
    let lock = client(&broker).lock_with(
        "job",
        manual(30).retry(Duration::from_millis(5), Duration::from_millis(10)),
    );
    assert!(!lock
        .acquire_within(Duration::from_millis(60))
        .await
        .unwrap());
    assert!(broker.hit_count() >= 2, "it came back while it waited");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_renews_in_the_background_and_a_refused_renew_is_a_loss() {
    // Renewed twice, then the broker says the permit is somebody else's.
    let broker = FakeBroker::start_with(|n, hit| match (n, op_of(hit)["op"].as_str()) {
        (0, _) => granted("job", 0, 1),
        (1, Some("renew")) => renewed("job", 0, 2),
        (2, Some("renew")) => renewed("job", 0, 3),
        _ => results(json!({"index":0,"op":"renew","name":"job","renewed":false,
            "reason":"lost","slot":0,"holders":[{"slot":0,"owner":"other"}]})),
    })
    .await;
    let lock = client(&broker).lock_with(
        "job",
        LockOptions::new(Duration::from_secs(5)).renew_every(Duration::from_millis(40)),
    );
    assert!(lock.acquire().await.unwrap());
    tokio::time::timeout(Duration::from_secs(5), lock.lost())
        .await
        .expect("the handle reported the loss");
    assert!(lock.is_lost() && !lock.held());
    let renews: Vec<Value> = broker.hits().iter().skip(1).map(op_of).collect();
    assert_eq!(renews[0]["token"], 1);
    assert_eq!(
        renews[1]["token"], 2,
        "each renew carries the token of the one before"
    );
    assert_eq!(renews[2]["token"], 3);
    let seen = broker.hit_count();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(broker.hit_count(), seen, "a lost lock renews no more");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lifetime_that_runs_out_here_is_a_loss() {
    let broker = FakeBroker::start(vec![granted("job", 0, 1)]).await;
    let lock = client(&broker).lock_with("job", manual(1));
    assert!(lock.acquire().await.unwrap());
    assert!(lock.held());
    tokio::time::timeout(Duration::from_secs(3), lock.lost())
        .await
        .expect("past its deadline the handle reports the loss");
    assert!(!lock.held());
    assert_eq!(broker.hit_count(), 1, "manual renew: nothing was sent");
}

// ---------------------------------------------------------------------------
// check, and the guard on a transaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_check_sends_its_expect_and_answers_a_verdict() {
    let broker = FakeBroker::start(vec![
        Reply::ok(r#"{"results":[{"index":0,"op":"check","applied":true,"key":"k","version":7}]}"#),
        Reply::ok(
            r#"{"results":[{"index":0,"op":"check","applied":false,"reason":"version","key":"k","value":{"n":2},"version":9}]}"#,
        ),
    ])
    .await;
    let kv = client(&broker).kv();
    let held = kv.check("orders", "k", 7).await.unwrap();
    assert!(held.applied());
    assert_eq!(held.value, None, "a held check hands back no value");
    let stale = kv.check("orders", "k", 7).await.unwrap();
    assert!(!stale.applied());
    assert_eq!(stale.version, Some(9));
    let hits = broker.hits();
    assert_eq!(hits[0].route(), "/api/v1/kv");
    assert_eq!(
        hits[0].json(),
        json!({"operations":[{"op":"check","ns":"orders","key":"k","expect":7}]})
    );
}

fn committed() -> Reply {
    Reply::ok(r#"{"transactionId":"t","success":true,"results":[]}"#)
}

fn lost_to(failed_index: usize, kv_reason: &str, value: Value, version: u64) -> Reply {
    Reply::ok(
        json!({"transactionId":"t","success":false,"reason":"kv_precondition","error":"QKV",
            "results":[],"ok":false,"failedIndex":failed_index,"kvReason":kv_reason,
            "value":value,"version":version})
        .to_string(),
    )
}

#[tokio::test]
async fn the_guard_is_the_first_kv_op_at_the_token_held_when_commit_sends() {
    let broker = FakeBroker::start(vec![
        granted("job", 0, 100),
        renewed("job", 0, 101),
        committed(),
    ])
    .await;
    let queen = client(&broker);
    let lock = queen.lock_with("job", manual(30));
    lock.acquire().await.unwrap();
    let txn = queen
        .transaction()
        .guard(&lock)
        .push("reports", json!({"n":1}))
        .unwrap()
        .kv_check("orders", "state", 0);
    lock.renew().await.unwrap(); // after the guard was asked for, before commit
    let resp = txn.commit().await.unwrap();
    assert!(resp.success);
    let body = broker.hits()[2].json();
    assert_eq!(broker.hits()[2].route(), "/api/v1/transaction");
    assert_eq!(
        body["kv"],
        json!([
            guard_of("job", 0, 101),
            {"op":"check","ns":"orders","key":"state","expect":0,"required":true}
        ])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_guard_that_lost_to_its_own_renewal_is_sent_again_with_the_new_token() {
    // The race, in the order that makes it: the commit goes out with token
    // 100; the lock's renewal is applied while the commit is on its way; the
    // broker judges the commit against token 101 and names this owner's row.
    let owner: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    let seen = Arc::clone(&owner);
    let commits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let broker = FakeBroker::start_with(move |_, hit| {
        if hit.route() == "/api/v1/transaction" {
            if commits.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                // Held while the renew overtakes it.
                std::thread::sleep(Duration::from_millis(200));
                // One pushed item comes first in the flat space: the guard is 1.
                return lost_to(1, "version", json!({"owner": *seen.lock().unwrap()}), 101);
            }
            return committed();
        }
        match op_of(hit)["op"].as_str() {
            Some("acquire") => {
                *seen.lock().unwrap() = op_of(hit)["owner"].as_str().unwrap().to_string();
                granted("job", 0, 100)
            }
            Some("renew") => renewed("job", 0, 101),
            _ => released("job"),
        }
    })
    .await;
    let queen = client(&broker);
    let lock = queen.lock_with("job", manual(30));
    lock.acquire().await.unwrap();
    let commit = tokio::spawn(
        queen
            .transaction()
            .guard(&lock)
            .push("reports", json!({"n":1}))
            .unwrap()
            .commit(),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(lock.renew().await.unwrap());
    let resp = commit.await.unwrap().expect("the step commits");
    assert!(resp.success, "committed on the second send");
    let sends: Vec<Value> = broker
        .hits()
        .iter()
        .filter(|h| h.route() == "/api/v1/transaction")
        .map(|h| h.json())
        .collect();
    assert_eq!(sends.len(), 2);
    assert_eq!(sends[0]["kv"][0]["expect"], 100);
    assert_eq!(sends[1]["kv"][0]["expect"], 101);
    assert_eq!(
        sends[1]["operations"], sends[0]["operations"],
        "the same step"
    );
    assert!(lock.held() && !lock.is_lost());
}

#[tokio::test]
async fn a_guard_that_lost_to_another_holder_is_the_verdict_and_the_lock_is_lost() {
    let broker = FakeBroker::start(vec![
        granted("job", 0, 100),
        lost_to(0, "version", json!({"owner":"somebody-else"}), 250),
    ])
    .await;
    let queen = client(&broker);
    let lock = queen.lock_with("job", manual(30));
    lock.acquire().await.unwrap();
    let resp = queen
        .transaction()
        .guard(&lock)
        .kv_check("orders", "state", 0)
        .commit()
        .await
        .expect("a lost precondition is returned, not raised");
    assert!(!resp.success);
    assert!(resp.lost_precondition().is_some());
    assert!(lock.is_lost() && !lock.held());
    assert_eq!(broker.hit_count(), 2, "not sent again");
}

#[tokio::test]
async fn a_precondition_that_is_not_the_guards_leaves_the_lock_alone() {
    // kv = [guard, check]: flat index 1 is the bundle's own check.
    let broker = FakeBroker::start(vec![
        granted("job", 0, 100),
        lost_to(1, "exists", json!(true), 77),
    ])
    .await;
    let queen = client(&broker);
    let lock = queen.lock_with("job", manual(30));
    lock.acquire().await.unwrap();
    let resp = queen
        .transaction()
        .guard(&lock)
        .kv_check("idem", "order-1", 0)
        .commit()
        .await
        .unwrap();
    assert!(!resp.success);
    assert!(lock.held(), "the marker lost, not the lock");
}

#[tokio::test]
async fn a_step_that_asked_for_a_guard_never_goes_out_without_one() {
    let broker = FakeBroker::start(vec![committed()]).await;
    let queen = client(&broker);
    let lock = queen.lock("job", Duration::from_secs(30));
    let err = queen
        .transaction()
        .guard(&lock)
        .kv_check("orders", "state", 0)
        .commit()
        .await
        .expect_err("an unheld lock cannot guard");
    assert!(err.to_string().contains("is not held"), "{err}");
    assert_eq!(broker.hit_count(), 0);
}
