//! Locks, semaphores, `check` and guarded transactions against a live broker.
//!
//! Skips without `QUEEN_TEST_URL` (see `common`). Every lock here is named
//! under `t-` plus a per-run suffix and carries a lifetime, so a run that goes
//! wrong leaves nothing that does not expire by itself, and a second run
//! against the same broker does not meet the first one's permits.
//!
//! What only a real broker can show: one holder; a retry by the same owner is
//! the same permit; an expired lock goes to the next handle with a higher
//! token and the old handle's guarded step pushes nothing; a semaphore never
//! grants more than its limit.

mod common;

use std::time::Duration;

use queen_mq::{KvOperation, LockOperation, LockOptions, Queen};
use serde_json::json;

fn unique(base: &str) -> String {
    format!(
        "t-{base}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// A broker alone raises its cluster version to 5 on its first tick; `check`
/// needs it.
async fn checks_are_served(queen: &Queen) {
    for _ in 0..100 {
        match queen.kv().check("t-probe", "k", 0).await {
            Ok(_) => return,
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    panic!("the broker never served a check (cluster version below 5?)");
}

async fn drain(queen: &Queen, queue: &str) -> Vec<serde_json::Value> {
    let mut seen = Vec::new();
    loop {
        let msgs = queen
            .queue(queue)
            .batch(50)
            .wait(false)
            .pop()
            .await
            .expect("pop");
        if msgs.is_empty() {
            return seen;
        }
        for m in &msgs {
            seen.push(m.data.clone());
            queen.ack(m).await.expect("ack");
        }
    }
}

#[tokio::test]
async fn a_lock_has_one_holder_and_its_guarded_step_commits() {
    let queen = broker!();
    checks_are_served(&queen).await;
    let name = unique("one-holder");
    let a = queen.lock(&name, Duration::from_secs(30));
    let b = queen.lock(&name, Duration::from_secs(30));

    assert!(a.acquire().await.unwrap());
    assert!(
        !b.acquire().await.unwrap(),
        "a second handle acquired a held lock"
    );
    let token = a.token().expect("a token");

    let who = queen.locks().get(&name).await.unwrap();
    assert!(who.held());
    assert_eq!(who.holders[0].owner.as_deref(), Some(a.owner()));
    assert_eq!(who.holders[0].token, Some(token));

    // The permit is a KV row and nothing else.
    let row = queen
        .kv()
        .get("queen-locks", &format!("{name}#0"))
        .await
        .unwrap();
    assert!(row.found());
    assert_eq!(row.version, Some(token as i64));

    let queue = unique("one-holder-q");
    let resp = queen
        .transaction()
        .guard(&a)
        .push(&queue, json!({"step": 1}))
        .unwrap()
        .commit()
        .await
        .unwrap();
    assert!(resp.success, "{resp:?}");
    assert_eq!(drain(&queen, &queue).await, vec![json!({"step": 1})]);

    assert!(a.release().await.unwrap());
    assert!(b.acquire().await.unwrap(), "free after its release");
    assert!(b.token().unwrap() > token, "a later holder, a higher token");
    b.release().await.unwrap();
}

#[tokio::test]
async fn a_call_sent_again_by_its_owner_is_the_same_permit() {
    let queen = broker!();
    let name = unique("retry-owner");
    let locks = queen.locks();
    let first = locks
        .send(LockOperation::acquire(&name, 30).owner("me"))
        .await
        .unwrap();
    let again = locks
        .send(LockOperation::acquire(&name, 30).owner("me"))
        .await
        .unwrap();
    assert!(first.acquired() && again.acquired());
    assert_eq!(again.already, Some(true));
    assert_eq!(again.token, first.token, "the same permit, not a new one");

    let token = first.token.unwrap();
    let renewed = locks
        .send(LockOperation::renew(&name, token, 30).owner("me"))
        .await
        .unwrap();
    // The renew's answer is lost; the old token is sent again.
    let resent = locks
        .send(LockOperation::renew(&name, token, 30).owner("me"))
        .await
        .unwrap();
    assert!(renewed.renewed() && resent.renewed());
    assert!(resent.token > renewed.token);
    let stranger = locks
        .send(LockOperation::renew(&name, token, 30).owner("somebody-else"))
        .await
        .unwrap();
    assert!(!stranger.renewed(), "a stranger's stale token is lost");
    assert_eq!(stranger.holders[0].owner.as_deref(), Some("me"));
    assert!(locks
        .send(LockOperation::release(&name, resent.token.unwrap()))
        .await
        .unwrap()
        .released());
}

#[tokio::test]
async fn an_expired_lock_is_taken_over_and_the_old_holder_commits_nothing() {
    let queen = broker!();
    checks_are_served(&queen).await;
    let name = unique("expiry");
    let queue = unique("expiry-q");
    // The old holder does not renew: it is "paused" for longer than its lease.
    let old = queen.lock_with(
        &name,
        LockOptions::new(Duration::from_secs(1)).manual_renew(),
    );
    let next = queen.lock(&name, Duration::from_secs(30));

    assert!(old.acquire().await.unwrap());
    let old_token = old.token().unwrap();
    let stale_guard: KvOperation = old.guard().unwrap();
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert!(
        !old.held(),
        "past its lifetime a handle does not claim to hold"
    );

    assert!(next.acquire().await.unwrap(), "an expired lock is free");
    assert!(next.token().unwrap() > old_token);

    // The old holder wakes up and sends the step it was about to send.
    let stale = queen
        .transaction()
        .kv(stale_guard)
        .push(&queue, json!({"from": "old"}))
        .unwrap()
        .commit()
        .await
        .unwrap();
    assert!(!stale.success);
    assert!(stale.lost_precondition().is_some(), "{stale:?}");

    let ok = queen
        .transaction()
        .guard(&next)
        .push(&queue, json!({"from": "next"}))
        .unwrap()
        .commit()
        .await
        .unwrap();
    assert!(ok.success);
    assert_eq!(
        drain(&queen, &queue).await,
        vec![json!({"from": "next"})],
        "only the new holder's message exists"
    );
    next.release().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_guarded_step_survives_the_locks_own_renewal() {
    let queen = broker!();
    checks_are_served(&queen).await;
    let name = unique("renewal");
    let queue = unique("renewal-q");
    let lock = queen.lock_with(
        &name,
        LockOptions::new(Duration::from_secs(2)).renew_every(Duration::from_millis(100)),
    );
    assert!(lock.acquire().await.unwrap());
    let first = lock.token().unwrap();
    let mut committed = 0usize;
    let end = std::time::Instant::now() + Duration::from_millis(1500);
    while std::time::Instant::now() < end {
        let resp = queen
            .transaction()
            .guard(&lock)
            .push(&queue, json!({"n": committed}))
            .unwrap()
            .commit()
            .await
            .unwrap();
        assert!(
            resp.success,
            "step {committed} while the lock was held: {resp:?}"
        );
        committed += 1;
    }
    assert!(
        lock.token().unwrap() > first,
        "the lock renewed during the run"
    );
    assert!(lock.held() && !lock.is_lost());
    assert_eq!(drain(&queen, &queue).await.len(), committed);
    lock.release().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_semaphore_never_grants_more_than_its_limit() {
    let queen = broker!();
    let name = unique("crowd");
    const LIMIT: u32 = 3;
    let permits: Vec<_> = (0..12)
        .map(|_| queen.semaphore(&name, LIMIT, Duration::from_secs(30)))
        .collect();
    let mut tasks = Vec::new();
    for p in &permits {
        let p = p.clone();
        tasks.push(tokio::spawn(async move { p.acquire().await.unwrap() }));
    }
    let mut holders = Vec::new();
    for (p, t) in permits.iter().zip(tasks) {
        if t.await.unwrap() {
            holders.push(p.clone());
        }
    }
    assert!(
        (1..=LIMIT as usize).contains(&holders.len()),
        "{} permits granted of {LIMIT}",
        holders.len()
    );
    // A crowd can leave a permit free; one at a time, the semaphore fills.
    for p in &permits {
        if holders.len() == LIMIT as usize {
            break;
        }
        if !p.held() && p.acquire().await.unwrap() {
            holders.push(p.clone());
        }
    }
    assert_eq!(holders.len(), LIMIT as usize);
    let mut slots: Vec<u32> = holders.iter().map(|p| p.slot().unwrap()).collect();
    slots.sort_unstable();
    assert_eq!(slots, vec![0, 1, 2], "one holder per slot");

    let extra = queen.semaphore(&name, LIMIT, Duration::from_secs(30));
    assert!(!extra.acquire().await.unwrap(), "a permit past the limit");
    assert_eq!(queen.locks().get(&name).await.unwrap().holders.len(), 3);

    // One leaves; the waiter gets exactly that slot.
    let freed = holders[0].slot().unwrap();
    let waiting = {
        let extra = extra.clone();
        tokio::spawn(async move { extra.acquire_within(Duration::from_secs(5)).await.unwrap() })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    holders[0].release().await.unwrap();
    assert!(waiting.await.unwrap());
    assert_eq!(extra.slot(), Some(freed));
    extra.release().await.unwrap();
    for p in &holders[1..] {
        p.release().await.unwrap();
    }
}
