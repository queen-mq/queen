//! KV and timers against a live broker.
//!
//! These need a stack, and nothing else. KV and timers are not a feature a
//! broker may have been built without: every cell that runs this binary serves
//! them, the same way it serves push and pop, so these tests take `broker!()`
//! like the rest of the suite and run whenever `QUEEN_TEST_URL` points at
//! something. A kv call that fails here is a failure, not a cell to skip.
//!
//! # Every test purges before and after, and it is not hygiene
//!
//! A `putIfAbsent` test is green on its first run and red for ever after if the
//! marker it wrote survives; an `incr` test accumulates across runs until it
//! crosses the very ceiling it asserts. Both failures arrive a day later, on
//! somebody else's change. And `Expiry::Forever` appears nowhere below on
//! purpose: a test that goes wrong must not be able to leave immortal state in a
//! shared database.

mod common;

use std::time::Duration;

use common::*;
use queen_mq::{Expiry, KvOperation, KvReason, SubscriptionMode, TimerStatus};

/// Namespaces are `^[a-z0-9][a-z0-9._-]{0,63}$`, which `unique()` satisfies.
fn ns(prefix: &str) -> String {
    unique(prefix)
}

fn key(name: &str) -> String {
    format!("{TEST_KEY_PREFIX}{name}")
}

// ===========================================================================
// KV
// ===========================================================================

#[tokio::test]
async fn a_key_round_trips_and_a_delete_removes_it() {
    let q = broker!();
    let ns = ns("kv-rt");
    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;

    let miss = q.kv().get(&ns, &key("absent")).await.unwrap();
    assert!(!miss.found(), "a fresh namespace must be empty");

    let put = q
        .kv()
        .put(
            &ns,
            &key("order"),
            serde_json::json!({"step": 2}),
            Expiry::seconds(300),
        )
        .send()
        .await
        .unwrap();
    assert!(put.applied());

    let got = q.kv().get(&ns, &key("order")).await.unwrap();
    assert!(got.found());
    assert_eq!(got.value.unwrap()["step"], 2);
    assert_eq!(got.version, put.version);
    assert!(
        got.expires_at.is_some(),
        "every write declares an expiry, so every key has one"
    );

    let deleted = q.kv().delete(&ns, &key("order")).send().await.unwrap();
    assert!(deleted.applied());
    // Deleting nothing is a verdict, not an error, and not an HTTP failure.
    let again = q.kv().delete(&ns, &key("order")).send().await.unwrap();
    assert!(!again.applied());
    assert_eq!(again.reason, Some(KvReason::Absent));

    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;
}

#[tokio::test]
async fn exactly_one_put_if_absent_wins_and_the_loser_learns_the_winners_value() {
    let q = broker!();
    let ns = ns("kv-pia");
    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;

    let first = q
        .kv()
        .put_if_absent(
            &ns,
            &key("idem"),
            serde_json::json!({"by": "worker-1"}),
            Expiry::seconds(300),
        )
        .send()
        .await
        .unwrap();
    assert!(first.applied(), "the first claim must win");

    let second = q
        .kv()
        .put_if_absent(
            &ns,
            &key("idem"),
            serde_json::json!({"by": "worker-2"}),
            Expiry::seconds(300),
        )
        .send()
        .await
        .expect("losing is not an error");
    assert!(!second.applied());
    assert_eq!(second.reason, Some(KvReason::Exists));
    assert_eq!(
        second.value.unwrap()["by"],
        "worker-1",
        "the loser reads the winner's value without a second round trip — that \
         is the whole point of the marker"
    );
    assert_eq!(second.version, first.version);

    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;
}

#[tokio::test]
async fn a_version_fence_never_creates_the_key_it_was_guarding() {
    // The bug this rule exists for: in the naive implementation `expect: N > 0`
    // on an absent key falls into the INSERT arm and CREATES the row — which,
    // in a saga, fires the compensation the fence was there to prevent.
    let q = broker!();
    let ns = ns("kv-fence");
    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;

    let fenced = q
        .kv()
        .put(
            &ns,
            &key("saga"),
            serde_json::json!(1),
            Expiry::seconds(300),
        )
        .expect(7)
        .send()
        .await
        .unwrap();
    assert!(!fenced.applied());
    assert_eq!(fenced.reason, Some(KvReason::Absent));
    assert!(
        !q.kv().get(&ns, &key("saga")).await.unwrap().found(),
        "an expect that matched zero rows must never create anything"
    );

    // ...and against a real version it is an ordinary optimistic lock.
    let created = q
        .kv()
        .put(
            &ns,
            &key("saga"),
            serde_json::json!(1),
            Expiry::seconds(300),
        )
        .send()
        .await
        .unwrap();
    let version = created.version.unwrap();

    let stale = q
        .kv()
        .put(
            &ns,
            &key("saga"),
            serde_json::json!(2),
            Expiry::seconds(300),
        )
        .expect(version + 1)
        .send()
        .await
        .unwrap();
    assert!(!stale.applied());
    assert_eq!(stale.reason, Some(KvReason::Version));

    let ok = q
        .kv()
        .put(
            &ns,
            &key("saga"),
            serde_json::json!(3),
            Expiry::seconds(300),
        )
        .expect(version)
        .send()
        .await
        .unwrap();
    assert!(ok.applied());
    // CHANGED, not GREATER. The version comes from one sequence shared by every
    // key, declared `CACHE 1000`, so each pooled connection hands out its own
    // block: four consecutive writes to one key through a pool of four
    // connections were measured returning 6, 3005, 1007, 2003. It is an opaque
    // token that makes a fence falsifiable, and comparing two of them with `<`
    // is a bug waiting for a busy pool.
    assert_ne!(
        ok.version.unwrap(),
        version,
        "a write must move the version, which is what makes the fence work"
    );
    let stale_again = q
        .kv()
        .put(
            &ns,
            &key("saga"),
            serde_json::json!(4),
            Expiry::seconds(300),
        )
        .expect(version)
        .send()
        .await
        .unwrap();
    assert!(
        !stale_again.applied(),
        "the version that just won must not win twice"
    );

    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;
}

#[tokio::test]
async fn a_counter_admits_and_then_refuses_at_its_ceiling() {
    // With `max`, `applied` IS the admission decision: the request that would
    // have crossed the budget must not have spent it first.
    let q = broker!();
    let ns = ns("kv-incr");
    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;

    for expected in 1..=3 {
        let r = q
            .kv()
            .incr(&ns, &key("quota"), 1, Expiry::seconds(300))
            .max(3)
            .send()
            .await
            .unwrap();
        assert!(
            r.applied(),
            "increment {expected} should have been admitted"
        );
        assert_eq!(r.counter().unwrap(), expected);
    }

    let refused = q
        .kv()
        .incr(&ns, &key("quota"), 1, Expiry::seconds(300))
        .max(3)
        .send()
        .await
        .unwrap();
    assert!(!refused.applied());
    assert_eq!(refused.reason, Some(KvReason::Limit));
    assert_eq!(
        refused.counter().unwrap(),
        3,
        "the refusal reports the CURRENT value, never the would-be one"
    );

    // A delta that is itself outside the bounds is refused before anything is
    // written — the first call of a window must not be able to overshoot.
    let fresh = ns.clone();
    let over = q
        .kv()
        .incr(&fresh, &key("burst"), 10, Expiry::seconds(300))
        .max(5)
        .send()
        .await
        .unwrap();
    assert!(
        !over.applied(),
        "10 against max 5 must not apply, even on a new key"
    );
    assert!(!q.kv().get(&fresh, &key("burst")).await.unwrap().found());

    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;
}

#[tokio::test]
async fn a_multi_key_read_reports_absence_as_a_list() {
    let q = broker!();
    let ns = ns("kv-many");
    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;

    q.kv()
        .put(&ns, &key("a"), serde_json::json!(1), Expiry::seconds(300))
        .send()
        .await
        .unwrap();

    let many = q
        .kv()
        .get_many(&ns, vec![key("a"), key("b")])
        .await
        .unwrap();
    assert_eq!(many.rows().len(), 1);
    assert_eq!(many.rows()[0].key, key("a"));
    assert_eq!(
        many.missing(),
        [key("b")],
        "absence is a datum, not a gap the caller computes by subtraction"
    );

    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;
}

#[tokio::test]
async fn a_prefix_scan_pages_by_cursor() {
    let q = broker!();
    let ns = ns("kv-prefix");
    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;

    for i in 0..5 {
        q.kv()
            .put(
                &ns,
                &key(&format!("page:{i}")),
                serde_json::json!(i),
                Expiry::seconds(300),
            )
            .send()
            .await
            .unwrap();
    }

    let first = q
        .kv()
        .get_prefix(&ns, &key("page:"))
        .limit(2)
        .send()
        .await
        .unwrap();
    assert_eq!(first.rows().len(), 2);
    assert!(first.truncated(), "five keys do not fit a page of two");
    let cursor = first
        .next_after
        .clone()
        .expect("a truncated page has a cursor");

    let second = q
        .kv()
        .get_prefix(&ns, &key("page:"))
        .after(&cursor)
        .limit(10)
        .keys_only()
        .send()
        .await
        .unwrap();
    assert_eq!(second.rows().len(), 3);
    assert!(!second.truncated());
    assert!(
        second.rows().iter().all(|r| r.value.is_none()),
        "keysOnly returns no values"
    );
    assert!(
        second.rows().iter().all(|r| r.key > cursor),
        "the cursor is exclusive"
    );

    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;
}

#[tokio::test]
async fn a_write_with_no_expiry_is_refused_by_the_broker() {
    // The rule lives in the stored procedure so that all seven clients inherit
    // it. This asserts the client cannot route around it — and that it arrives
    // as a 400, not as a silently defaulted TTL.
    let q = broker!();
    let ns = ns("kv-noexp");

    let mut op =
        KvOperation::put(&ns, key("x"), serde_json::json!(1), Expiry::seconds(300)).unwrap();
    op.ttl_seconds = None;

    let err = q.kv().batch(vec![op]).await.unwrap_err();
    assert_eq!(err.status(), Some(400), "{err}");
}

#[tokio::test]
async fn get_prefix_is_refused_inside_a_transaction() {
    // The boundary is COST, not the kind of operation: this transaction holds
    // the outermost lock space of the product, and a read whose size the caller
    // does not bound has no business inside it.
    let q = broker!();
    let ns = ns("kv-txnpfx");

    let err = q
        .transaction()
        .kv(KvOperation::get_prefix(&ns, key("p")))
        .commit()
        .await
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("prefix"),
        "the refusal should name the reason: {err}"
    );
}

#[tokio::test]
async fn the_purge_is_what_makes_this_suite_rerunnable() {
    // Every other test here names its namespace with `unique()`, which keeps
    // parallel runs apart but also means their purges are never actually load
    // bearing. This one uses a FIXED name on purpose, so the cleanup is the
    // thing under test: without the purge below, the first run is green and
    // every run after it is red for ever — the marker it claims is already
    // there, and the counter it increments never goes back to zero.
    //
    // Verified by deleting the two purge calls and running the suite twice: the
    // second run fails on the `applied` assertion.
    let q = broker!();
    let ns = "test-rs-kvt-rerun";
    let queue = "test-rs-kvt-rerun";
    purge_kv(&q, ns, TEST_KEY_PREFIX).await;
    purge_timers(&q, queue).await;

    let claim = q
        .kv()
        .put_if_absent(
            ns,
            &key("marker"),
            serde_json::json!(1),
            Expiry::seconds(300),
        )
        .send()
        .await
        .unwrap();
    assert!(
        claim.applied(),
        "a leftover marker from the previous run was not purged"
    );

    let counter = q
        .kv()
        .incr(ns, &key("runs"), 1, Expiry::seconds(300))
        .send()
        .await
        .unwrap();
    assert_eq!(
        counter.counter().unwrap(),
        1,
        "the counter accumulated across runs, which is what an unpurged \
         namespace does"
    );

    let timer = q
        .timers()
        .schedule(queue, &key("rerun"), Duration::from_secs(3600))
        .payload_json(&serde_json::json!({"run": 1}))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(
        timer.status,
        TimerStatus::Scheduled,
        "a pending timer survived the previous run: this would be `rescheduled`"
    );

    purge_kv(&q, ns, TEST_KEY_PREFIX).await;
    purge_timers(&q, queue).await;
    drop_queue(&q, queue).await;
}

// ===========================================================================
// The transaction riders — the reason the KV surface exists
// ===========================================================================

#[tokio::test]
async fn the_gate_rolls_the_whole_bundle_back_on_a_redelivery() {
    let q = broker!();
    let source = unique("kvt-src");
    let sink = unique("kvt-sink");
    let ns = ns("kvt-saga");
    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;

    q.queue(&source)
        .partition("customer-42")
        .push(serde_json::json!({"order": 9137}))
        .await
        .unwrap();

    let msgs = pop_retry(&q, &source, Some("worker"), 1, 40).await;
    assert_eq!(msgs.len(), 1, "the seeded order was never delivered");

    // First pass: the marker is free, so ack + push + marker all commit.
    let first = q
        .transaction()
        .ack(&msgs[0])
        .push(&sink, serde_json::json!({"order": 9137, "invoiced": true}))
        .unwrap()
        .kv_put_if_absent(
            &ns,
            &key("idem:9137"),
            serde_json::json!({"by": "worker-1"}),
            Expiry::seconds(300),
        )
        .unwrap()
        .commit()
        .await
        .unwrap();
    assert!(first.success);
    assert!(first.lost_precondition().is_none());
    let kv = first.kv_results();
    assert_eq!(kv.len(), 1, "the rider must produce one result");
    assert!(kv[0].applied());

    let invoiced = pop_retry(&q, &sink, Some("check"), 10, 40).await;
    assert_eq!(invoiced.len(), 1);
    q.ack_all(&invoiced).await.unwrap();

    // Second pass: the same work, as a redelivery would present it. The marker
    // is taken, so NOTHING happens — including the push.
    q.queue(&source)
        .partition("customer-42")
        .push(serde_json::json!({"order": 9137}))
        .await
        .unwrap();
    let again = pop_retry(&q, &source, Some("worker"), 1, 40).await;
    assert_eq!(again.len(), 1);

    let second = q
        .transaction()
        .ack(&again[0])
        .push(&sink, serde_json::json!({"order": 9137, "invoiced": true}))
        .unwrap()
        .kv_put_if_absent(
            &ns,
            &key("idem:9137"),
            serde_json::json!({"by": "worker-2"}),
            Expiry::seconds(300),
        )
        .unwrap()
        .commit()
        .await
        .expect("a lost precondition RETURNS: it is the expected outcome here");

    assert!(!second.success, "the bundle must not have committed");
    let lost = second
        .lost_precondition()
        .expect("this is the verdict the gate exists to produce");
    assert_eq!(lost.reason, Some(KvReason::Exists));
    assert_eq!(
        lost.value.unwrap()["by"],
        "worker-1",
        "the verdict carries the winner's value"
    );

    // The proof that the rollback was real: no second invoice.
    let duplicates = q
        .queue(&sink)
        .group("check")
        .subscription_mode(SubscriptionMode::All)
        .batch(10)
        .wait(false)
        .pop()
        .await
        .unwrap();
    assert!(
        duplicates.is_empty(),
        "the gate lost but the push happened anyway: {} extra message(s)",
        duplicates.len()
    );

    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;
    drop_queue(&q, &source).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn a_riders_only_transaction_commits_both_arrays() {
    let q = broker!();
    let ns = ns("kvt-only");
    let queue = unique("kvt-tonly");
    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;
    purge_timers(&q, &queue).await;

    let resp = q
        .transaction()
        .kv_put(
            &ns,
            &key("state"),
            serde_json::json!({"phase": "open"}),
            Expiry::seconds(300),
        )
        .unwrap()
        .schedule(
            &queue,
            &key("timeout"),
            Duration::from_secs(600),
            b"{\"phase\":\"expired\"}".to_vec(),
        )
        .commit()
        .await
        .unwrap();
    assert!(resp.success);

    let kv = resp.kv_results();
    assert_eq!(kv.len(), 1);
    assert!(kv[0].applied());

    let timers = resp.timer_results();
    assert_eq!(timers.len(), 1);
    assert!(timers[0].is_pending());
    assert!(
        timers[0].message_id.is_some(),
        "the message id is promised at schedule time"
    );

    // Both are really there.
    assert!(q.kv().get(&ns, &key("state")).await.unwrap().found());
    assert!(
        q.timers()
            .peek(&queue, &key("timeout"))
            .await
            .unwrap()
            .found
    );

    purge_kv(&q, &ns, TEST_KEY_PREFIX).await;
    purge_timers(&q, &queue).await;
    drop_queue(&q, &queue).await;
}

// ===========================================================================
// Timers
// ===========================================================================

#[tokio::test]
async fn a_timer_becomes_a_message_carrying_the_ids_it_was_promised() {
    let q = broker!();
    let queue = unique("tmr-fire");
    purge_timers(&q, &queue).await;

    let scheduled = q
        .timers()
        .schedule(&queue, &key("fire"), Duration::from_millis(300))
        .partition("customer-42")
        .payload_json(&serde_json::json!({"sagaId": 9137}))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(scheduled.status, TimerStatus::Scheduled);
    let promised_txn = scheduled.txn.clone().expect("a schedule carries its txn");
    let promised_id = scheduled.message_id.clone().expect("and its message id");

    // deliverAt is "no earlier than", so this waits rather than asserting an
    // instant — and on an IDLE broker the wait is long. There is no local wake
    // in v1 (the schedule does not nudge the sweeper), so the loop is asleep on
    // its idle sleep, `QUEEN_SWEEPER_IDLE_MAX_SLEEP_MS`, which defaults to 30
    // SECONDS. A broker with any timer traffic answers in milliseconds; a test
    // stack that has been quiet does not. Measured here: a 300 ms timer
    // delivered 9 s after it was scheduled, on the loop's next wake.
    //
    // The budget below is therefore the idle sleep plus room, not a guess about
    // timer latency.
    let delivered = pop_retry(&q, &queue, Some("timers"), 10, 300).await;
    assert_eq!(
        delivered.len(),
        1,
        "the timer never fired: with no local wake, delivery waits for the \
         sweeper's idle sleep (QUEEN_SWEEPER_IDLE_MAX_SLEEP_MS, default 30s)"
    );
    assert_eq!(delivered[0].data["sagaId"], 9137);
    assert_eq!(delivered[0].partition, "customer-42");
    assert_eq!(
        delivered[0].transaction_id, promised_txn,
        "the txn promised at schedule time is the delivered message's"
    );
    assert_eq!(delivered[0].id, promised_id);
    q.ack_all(&delivered).await.unwrap();

    // No tombstone: the fired timer leaves no row, so a cancel now is `absent`
    // and MAY MEAN ALREADY DELIVERED — which it does, here.
    let late = q
        .timers()
        .cancel_expecting(&queue, &key("fire"), &promised_txn)
        .await
        .unwrap();
    assert_eq!(late.status, TimerStatus::Absent);
    assert!(!late.ok, "absent carries ok:false");
    assert_eq!(
        late.txn.as_deref(),
        Some(promised_txn.as_str()),
        "the echoed txn is what makes the `was it delivered?` check possible"
    );

    purge_timers(&q, &queue).await;
    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_cancelled_timer_never_fires() {
    let q = broker!();
    let queue = unique("tmr-cancel");
    purge_timers(&q, &queue).await;

    q.timers()
        .schedule(&queue, &key("nope"), Duration::from_secs(2))
        .payload_json(&serde_json::json!({"n": 1}))
        .unwrap()
        .send()
        .await
        .unwrap();

    let cancelled = q.timers().cancel(&queue, &key("nope")).await.unwrap();
    assert!(cancelled.ok);
    assert_eq!(cancelled.status, TimerStatus::Cancelled);

    // Long enough to cover the sweeper's idle wake, or this asserts nothing:
    // an empty queue proves the cancel worked only once the timer would have
    // fired without it.
    sleep_ms(35_000).await;
    let nothing = q
        .queue(&queue)
        .group("timers")
        .subscription_mode(SubscriptionMode::All)
        .batch(10)
        .wait(false)
        .pop()
        .await
        .unwrap();
    assert!(nothing.is_empty(), "a cancelled timer fired anyway");

    purge_timers(&q, &queue).await;
    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_reschedule_overwrites_the_pending_timer_under_the_same_key() {
    let q = broker!();
    let queue = unique("tmr-resched");
    purge_timers(&q, &queue).await;

    let first = q
        .timers()
        .schedule(&queue, &key("once"), Duration::from_secs(3600))
        .payload_json(&serde_json::json!({"v": 1}))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(first.status, TimerStatus::Scheduled);

    let second = q
        .timers()
        .schedule(&queue, &key("once"), Duration::from_secs(7200))
        .payload_json(&serde_json::json!({"v": 2}))
        .unwrap()
        .reschedule()
        .send()
        .await
        .unwrap();
    assert_eq!(
        second.status,
        TimerStatus::Rescheduled,
        "the same key twice is one timer, which is what makes a retry after a \
         crash safe rather than a second delivery"
    );
    assert_ne!(second.deliver_at, first.deliver_at);

    let peek = q.timers().peek(&queue, &key("once")).await.unwrap();
    assert!(peek.found);
    let payload = peek.payload_bytes().unwrap().unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&payload).unwrap()["v"],
        2,
        "the reschedule replaced the payload"
    );
    assert_eq!(peek.attempts, 0);
    assert!(!peek.claimed);

    // One row, not two.
    let page = q.timers().list(&queue).send().await.unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].timer_key, key("once"));

    purge_timers(&q, &queue).await;
    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn cancelling_a_timer_that_was_never_scheduled_is_absent_and_not_an_error() {
    let q = broker!();
    let queue = unique("tmr-absent");

    let res = q.timers().cancel(&queue, &key("ghost")).await.unwrap();
    assert_eq!(res.status, TimerStatus::Absent);
    assert!(
        !res.ok,
        "absent must not read as success — the in-house lesson is queue \
         delete's deleted:false with a 200"
    );

    let peek = q.timers().peek(&queue, &key("ghost")).await.unwrap();
    assert!(!peek.found);
}

#[tokio::test]
async fn a_timer_batch_is_index_aligned_with_what_was_sent() {
    let q = broker!();
    let queue = unique("tmr-batch");
    purge_timers(&q, &queue).await;

    let ops = vec![
        queen_mq::TimerOperation::schedule(&queue, key("b1"), 3_600_000, "batch-1", b"{\"n\":1}"),
        queen_mq::TimerOperation::schedule(&queue, key("b2"), 3_600_000, "batch-2", b"{\"n\":2}"),
        // A cancel of something absent, in the same call: the result array must
        // still line up with the request.
        queen_mq::TimerOperation::cancel(&queue, key("b3")),
    ];
    let results = q.timers().batch(ops).await.unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].timer_key, key("b1"));
    assert_eq!(results[1].timer_key, key("b2"));
    assert_eq!(results[2].status, TimerStatus::Absent);

    purge_timers(&q, &queue).await;
    drop_queue(&q, &queue).await;
}
