//! Message-semantics parity.
//!
//! Ports `semantics.js` plus the `ack_commit` / `ack_window` areas of the Go
//! and Python suites. These are the contracts where a subtle regression is
//! invisible until it corrupts somebody's ordering: ack-as-commit, what charges
//! the retry budget, and what a late ack does.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use queen_mq::{AckResult, AckStatus, Message, QueueOptions, SubscriptionMode};

use common::*;

/// Push without configuring: the queue is created on first push and inherits
/// the server defaults (`retryLimit=3`, `deadLetterQueue=true`).
async fn push_only(q: &queen_mq::Queen, queue: &str, payload: serde_json::Value, txn: &str) {
    q.queue(queue)
        .push_items(vec![
            queen_mq::PushItem::new(queue, payload).transaction_id(txn)
        ])
        .await
        .unwrap();
}

/// Push to a named partition, so a test can watch one lane appear after
/// another.
async fn push_to_partition(
    q: &queen_mq::Queen,
    queue: &str,
    partition: &str,
    payload: serde_json::Value,
    txn: &str,
) {
    q.queue(queue)
        .push_items(vec![queen_mq::PushItem::new(queue, payload)
            .partition(partition)
            .transaction_id(txn)])
        .await
        .unwrap();
}

/// A raw POST, for the contracts the typed client cannot express.
///
/// Same shape as the helper in `admin.rs`, and for the same reason: the
/// client's HTTP crate is a private implementation detail, and the suite adds
/// no dependencies of its own.
async fn raw_post(path: &str, body: &serde_json::Value) -> String {
    let base =
        std::env::var("QUEEN_TEST_URL").expect("QUEEN_TEST_URL must be set to reach the broker");
    let out = std::process::Command::new("curl")
        .args([
            "-s",
            "-X",
            "POST",
            &format!("{}{path}", base.trim_end_matches('/')),
            "-H",
            "Content-Type: application/json",
            "-d",
            &body.to_string(),
        ])
        .output()
        .expect("curl is required for the raw-wire tests");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// `POST /api/v1/ack/batch` with one status *per item*.
///
/// The typed API carries a single status for the whole call by design
/// (`ack_all` / `nack_all`), so a mixed batch is unreachable through it — and
/// the per-item cursor clamp only exists *inside one call*, which is exactly
/// what makes it worth pinning. The Go suite reaches for the raw route here too
/// (`rawAckBatch` in `ack_commit_test.go`).
async fn raw_ack_batch(messages: &[Message], statuses: &[&str]) -> Vec<AckResult> {
    assert_eq!(
        messages.len(),
        statuses.len(),
        "the test handed {} statuses for {} messages",
        statuses.len(),
        messages.len()
    );
    let acknowledgments: Vec<serde_json::Value> = messages
        .iter()
        .zip(statuses)
        .map(|(m, status)| {
            let mut item = serde_json::json!({
                "transactionId": m.transaction_id,
                "partitionId": m.partition_id,
                "status": status,
            });
            if !m.lease_id.is_empty() {
                item["leaseId"] = serde_json::Value::String(m.lease_id.clone());
            }
            item
        })
        .collect();
    let body = serde_json::json!({
        "acknowledgments": acknowledgments,
        "consumerGroup": messages[0].consumer_group,
    });

    let raw = raw_post("/api/v1/ack/batch", &body).await;
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("/api/v1/ack/batch did not answer an ack array ({e}): {raw}"))
}

/// Wait until the broker reports at least `want` stored messages for a queue,
/// returning what it reported last.
///
/// A push is not instantly visible — fusion holds frames briefly to batch the
/// write — and the usual answer, `pop_retry`, is unavailable to a test whose
/// subject *is* the first pop. `/resources/queues/{name}` counts what the
/// segments hold without consuming anything.
async fn wait_for_stored(q: &queen_mq::Queen, queue: &str, want: u64) -> u64 {
    let mut stored = 0;
    for _ in 0..40 {
        stored = q
            .admin()
            .queue(queue)
            .await
            .ok()
            .and_then(|v| v.get("segments")?.get("messages")?.as_u64())
            .unwrap_or(0);
        if stored >= want {
            break;
        }
        sleep_ms(150).await;
    }
    stored
}

/// Pop as a group that has never touched this queue and must therefore see
/// everything already stored.
///
/// `all` is stated rather than assumed: the tests that use this are about
/// cursor isolation, not about which seeding mode the broker happens to default
/// to, and they must not turn amber the day that default changes.
async fn pop_from_scratch(q: &queen_mq::Queen, queue: &str, group: &str) -> Vec<Message> {
    for _ in 0..25 {
        let msgs = q
            .queue(queue)
            .group(group)
            .batch(10)
            .wait(false)
            .subscription_mode(SubscriptionMode::All)
            .pop()
            .await
            .expect("pop failed");
        if !msgs.is_empty() {
            return msgs;
        }
        sleep_ms(150).await;
    }
    Vec::new()
}

/// The `n` field of every message, sorted — redelivery order is not part of the
/// contract, the *set* is.
fn sorted_ns(messages: &[Message]) -> Vec<i64> {
    let mut ns: Vec<i64> = messages
        .iter()
        .filter_map(|m| m.data.get("n").and_then(|v| v.as_i64()))
        .collect();
    ns.sort_unstable();
    ns
}

// ============================================================================
// Ack-as-commit
// ============================================================================

#[tokio::test]
async fn acking_the_last_message_implicitly_completes_the_batch() {
    let q = broker!();
    let queue = unique("implicit-ack");
    create_queue(&q, &queue, short_lease(1)).await;

    q.queue(&queue)
        .push_many((1..=5).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();

    let msgs = pop_retry(&q, &queue, None, 5, 25).await;
    assert_eq!(msgs.len(), 5, "expected the whole batch");

    // Ack ONLY the last one.
    let res = q.ack(&msgs[4]).await.unwrap();
    assert!(
        res.success,
        "ack of the last message failed: {:?}",
        res.error
    );

    // Past lease expiry. A contiguous-prefix engine would redeliver 1-4 here.
    sleep_ms(2500).await;

    let again = q.queue(&queue).batch(10).wait(false).pop().await.unwrap();
    assert!(
        again.is_empty(),
        "implicit-ack regression: {} message(s) redelivered after acking only the last",
        again.len()
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn an_out_of_order_ack_completes_earlier_but_not_later_messages() {
    let q = broker!();
    let queue = unique("ooo-ack");
    create_queue(&q, &queue, short_lease(1)).await;

    for n in 1..=3 {
        push_only(
            &q,
            &queue,
            serde_json::json!({ "n": n }),
            &format!("{queue}-tx-{n}"),
        )
        .await;
    }

    let msgs = pop_retry(&q, &queue, None, 3, 25).await;
    assert_eq!(msgs.len(), 3);

    // Ack the MIDDLE one: #1 is implicitly completed, #3 stays outstanding.
    let res = q.ack(&msgs[1]).await.unwrap();
    assert!(res.success);

    sleep_ms(2500).await;

    let again = q.queue(&queue).batch(10).wait(false).pop().await.unwrap();
    let txns: Vec<&str> = again.iter().map(|m| m.transaction_id.as_str()).collect();
    assert_eq!(
        txns,
        vec![format!("{queue}-tx-3")],
        "only the message after the acked one should redeliver"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_completed_ack_below_the_cursor_succeeds_but_is_flagged_noop() {
    let q = broker!();
    let queue = unique("late-ack");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(30),
            ..Default::default()
        },
    )
    .await;

    for n in 1..=3 {
        push_only(
            &q,
            &queue,
            serde_json::json!({ "n": n }),
            &format!("{queue}-tx-{n}"),
        )
        .await;
    }
    let msgs = pop_retry(&q, &queue, None, 3, 25).await;
    assert_eq!(msgs.len(), 3);

    // Commit past #1 and #2; the lease stays live (batch end not reached).
    assert!(q.ack(&msgs[1]).await.unwrap().success);

    // Re-acking #1, now below the cursor: harmless, but it must say so.
    let late = q.ack(&msgs[0]).await.unwrap();
    assert!(late.success, "a duplicate commit should not fail");
    assert!(
        late.noop,
        "an ack below the cursor must be flagged noop, so a caller can tell a \
         real commit from a duplicate"
    );

    // An in-range ack must not carry the flag.
    let fresh = q.ack(&msgs[2]).await.unwrap();
    assert!(fresh.success);
    assert!(!fresh.noop, "an in-range ack was wrongly flagged noop");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_nack_below_the_cursor_is_rejected_rather_than_silently_dropped() {
    let q = broker!();
    let queue = unique("late-nack");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(30),
            ..Default::default()
        },
    )
    .await;

    for n in 1..=3 {
        push_only(
            &q,
            &queue,
            serde_json::json!({ "n": n }),
            &format!("{queue}-tx-{n}"),
        )
        .await;
    }
    let msgs = pop_retry(&q, &queue, None, 3, 25).await;
    assert_eq!(msgs.len(), 3);

    assert!(q.ack(&msgs[1]).await.unwrap().success);

    // The broker can no longer honour a redelivery request for #1 — it has to
    // say so, not answer ok.
    let late = q.nack(&msgs[0], "too late").await.unwrap();
    assert!(
        !late.success,
        "a nack below the committed cursor was silently accepted"
    );
    let err = late.error.unwrap_or_default();
    assert!(
        err.to_lowercase().contains("committed"),
        "rejection should explain itself; got '{err}'"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_nack_is_not_skipped_by_a_later_ack_in_the_same_call() {
    let q = broker!();
    let queue = unique("nack-then-ack");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(2),
            retry_limit: Some(5),
            ..Default::default()
        },
    )
    .await;

    for n in 1..=3 {
        push_only(
            &q,
            &queue,
            serde_json::json!({ "n": n }),
            &format!("{queue}-tx-{n}"),
        )
        .await;
    }
    let msgs = pop_retry(&q, &queue, None, 3, 25).await;
    assert_eq!(msgs.len(), 3);

    // One batch: nack #1, complete #2 and #3. The explicit failure must clamp
    // the cursor — a later completion in the same call cannot skip past it.
    q.nack(&msgs[0], "explicit failure").await.unwrap();
    q.ack_all(&msgs[1..]).await.unwrap();

    sleep_ms(3000).await;

    let again = q.queue(&queue).batch(10).wait(false).pop().await.unwrap();
    let txns: Vec<&str> = again.iter().map(|m| m.transaction_id.as_str()).collect();
    assert!(
        txns.contains(&format!("{queue}-tx-1").as_str()),
        "the nacked message was skipped by a later ack; redelivered: {txns:?}"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_failed_item_clamps_the_cursor_for_the_rest_of_its_own_batch() {
    let q = broker!();
    let queue = unique("mixed-failed");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(30),
            retry_limit: Some(5),
            ..Default::default()
        },
    )
    .await;

    for n in 1..=5 {
        push_only(
            &q,
            &queue,
            serde_json::json!({ "n": n }),
            &format!("{queue}-tx-{n}"),
        )
        .await;
    }
    let msgs = pop_retry(&q, &queue, None, 5, 25).await;
    assert_eq!(msgs.len(), 5, "expected the whole batch");

    // ONE call, five verdicts. The clamp has to happen while the call is being
    // applied: an engine that folded the batch into a single "highest acked
    // offset" would commit past #5 and lose the rejection of #2 entirely.
    // Splitting this into five calls would not test the same thing.
    let results = raw_ack_batch(
        &msgs,
        &["completed", "failed", "completed", "completed", "completed"],
    )
    .await;
    assert_eq!(
        results.len(),
        5,
        "one result per acknowledgment: {results:?}"
    );

    let again = pop_retry(&q, &queue, None, 10, 25).await;
    assert_eq!(
        sorted_ns(&again),
        vec![2, 3, 4, 5],
        "a 'failed' item must clamp the cursor before itself: the nacked message and \
         everything after it redelivers, however many completions follow it in the call"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_retry_item_clamps_its_batch_without_charging_the_budget() {
    let q = broker!();
    let queue = unique("mixed-retry");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(30),
            ..Default::default()
        },
    )
    .await;

    for n in 1..=3 {
        push_only(
            &q,
            &queue,
            serde_json::json!({ "n": n }),
            &format!("{queue}-tx-{n}"),
        )
        .await;
    }
    let msgs = pop_retry(&q, &queue, None, 3, 25).await;
    assert_eq!(msgs.len(), 3);

    // 'retry' is the same clamp as 'failed' but budget-free. A broker that
    // collapsed the status to a boolean would pass the clamp assertion and
    // fail the DLQ one.
    raw_ack_batch(&msgs, &["completed", "retry", "completed"]).await;

    let again = pop_retry(&q, &queue, None, 10, 25).await;
    assert_eq!(
        sorted_ns(&again),
        vec![2, 3],
        "a 'retry' item was skipped by a later completion in the same call"
    );
    assert_eq!(
        dlq_count(&q, &queue).await,
        0,
        "a 'retry' clamp leaked into the DLQ"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn acking_a_transaction_id_that_was_never_pushed_is_refused() {
    let q = broker!();
    let queue = unique("ghost-ack");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(30),
            ..Default::default()
        },
    )
    .await;

    for n in 1..=3 {
        push_only(
            &q,
            &queue,
            serde_json::json!({ "n": n }),
            &format!("{queue}-tx-{n}"),
        )
        .await;
    }
    let msgs = pop_retry(&q, &queue, None, 3, 25).await;
    assert_eq!(msgs.len(), 3);

    // Real partition, real lease, invented transaction id — the shape of a
    // client that outlived the ack window, or of one acking somebody else's
    // work. The broker cannot resolve it, so the cursor does not move; if it
    // answered success anyway the caller would believe a commit happened and
    // loop on the redelivery forever. Worse for the nack: a poison message
    // whose failure vanishes can never reach the DLQ.
    let mut ghost = msgs[0].clone();
    ghost.transaction_id = format!("{queue}-never-pushed");

    // This client has no per-call wrapper around the results array, so the
    // per-item verdict IS the overall one.
    let acked = q.ack(&ghost).await.unwrap();
    assert!(
        !acked.success,
        "a completed ack of a never-pushed transaction reported success"
    );
    let err = acked.error.unwrap_or_default();
    assert!(
        err.to_lowercase().contains("unresolv"),
        "the rejection should name the cause; got '{err}'"
    );

    let nacked = q.nack(&ghost, "semantics-test ghost").await.unwrap();
    assert!(
        !nacked.success,
        "a failed nack of a never-pushed transaction was swallowed as success"
    );
    let err = nacked.error.unwrap_or_default();
    assert!(
        err.to_lowercase().contains("unresolv"),
        "the nack rejection should name the cause; got '{err}'"
    );

    // The lease must have survived both rejections: refusing an unresolvable
    // ack cannot cost the consumer the batch it legitimately holds.
    let results = q.ack_all(&msgs).await.unwrap();
    assert!(
        results.iter().all(|r| r.success),
        "the real batch stopped being ackable after the ghost rejections: {results:?}"
    );

    drop_queue(&q, &queue).await;
}

// ============================================================================
// Cursors are per consumer group
// ============================================================================

#[tokio::test]
async fn an_ack_moves_only_the_cursor_of_its_own_group() {
    let q = broker!();
    let queue = unique("cursor-scope");
    let worker = format!("{queue}-worker");
    let auditor = format!("{queue}-auditor");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(30),
            ..Default::default()
        },
    )
    .await;

    for n in 1..=3 {
        push_only(
            &q,
            &queue,
            serde_json::json!({ "n": n }),
            &format!("{queue}-tx-{n}"),
        )
        .await;
    }

    let claimed = pop_retry(&q, &queue, Some(&worker), 3, 25).await;
    assert_eq!(claimed.len(), 3, "the worker group saw nothing");
    let results = q.ack_all(&claimed).await.unwrap();
    assert!(results.iter().all(|r| r.success), "ack failed: {results:?}");

    // The acking group is done...
    for round in 0..3 {
        let more = q
            .queue(&queue)
            .group(&worker)
            .batch(10)
            .wait(false)
            .pop()
            .await
            .unwrap();
        assert!(
            more.is_empty(),
            "round {round}: the acked messages came back to the group that acked them"
        );
        sleep_ms(100).await;
    }

    // ...and every other group is untouched.
    let audited = pop_from_scratch(&q, &queue, &auditor).await;
    assert_eq!(
        sorted_ns(&audited),
        vec![1, 2, 3],
        "a second group lost messages to the first group's ack — the cursor is \
         supposed to be per (partition, group)"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_transactional_ack_moves_only_the_cursor_of_its_own_group() {
    let q = broker!();
    let queue = unique("cursor-scope-txn");
    let worker = format!("{queue}-worker");
    let auditor = format!("{queue}-auditor");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(30),
            ..Default::default()
        },
    )
    .await;

    for n in 1..=3 {
        push_only(
            &q,
            &queue,
            serde_json::json!({ "n": n }),
            &format!("{queue}-tx-{n}"),
        )
        .await;
    }

    let claimed = pop_retry(&q, &queue, Some(&worker), 3, 25).await;
    assert_eq!(claimed.len(), 3, "the worker group saw nothing");

    // Deliberately the same scenario as the plain-ack test above: the
    // transaction endpoint commits cursors through its own code path, so a
    // group-scoping bug can live in one and not the other.
    q.transaction()
        .ack_all(&claimed)
        .commit()
        .await
        .expect("the transactional ack was rolled back");

    for round in 0..3 {
        let more = q
            .queue(&queue)
            .group(&worker)
            .batch(10)
            .wait(false)
            .pop()
            .await
            .unwrap();
        assert!(
            more.is_empty(),
            "round {round}: the transactionally acked messages came back"
        );
        sleep_ms(100).await;
    }

    let audited = pop_from_scratch(&q, &queue, &auditor).await;
    assert_eq!(
        sorted_ns(&audited),
        vec![1, 2, 3],
        "a transactional ack committed another group's cursor too"
    );

    drop_queue(&q, &queue).await;
}

// ============================================================================
// Retry budget
// ============================================================================

#[tokio::test]
async fn retry_acks_never_charge_the_budget_and_failed_acks_eventually_dlq() {
    let q = broker!();
    // Deliberately unconfigured: push-only, so this also proves DLQ is on by
    // default.
    let queue = unique("retry-free");
    push_only(
        &q,
        &queue,
        serde_json::json!({ "poison": true }),
        &format!("{queue}-tx"),
    )
    .await;

    // Six cycles — twice the default budget of three. A budget-charging
    // regression would lose the message around cycle four.
    for cycle in 0..6 {
        let msgs = pop_retry(&q, &queue, None, 1, 25).await;
        assert_eq!(
            msgs.len(),
            1,
            "cycle {cycle}: not redelivered after a 'retry' ack"
        );
        let res = q.ack_with(&msgs[0], AckStatus::Retry, None).await.unwrap();
        assert!(
            res.success,
            "cycle {cycle}: retry ack failed: {:?}",
            res.error
        );
    }

    assert_eq!(
        dlq_count(&q, &queue).await,
        0,
        "'retry' acks leaked into the DLQ"
    );

    // Now spend the real budget. retryLimit defaults to 3, so it dead-letters.
    for _ in 0..5 {
        let msgs = pop_retry(&q, &queue, None, 1, 10).await;
        if msgs.is_empty() {
            break;
        }
        q.nack(&msgs[0], "semantics-test failure").await.unwrap();
        sleep_ms(100).await;
    }

    assert_eq!(
        dlq_count(&q, &queue).await,
        1,
        "the message never dead-lettered — is deadLetterQueue still on by default?"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_dlq_ack_dead_letters_immediately_bypassing_retries() {
    let q = broker!();
    let queue = unique("force-dlq");
    push_only(
        &q,
        &queue,
        serde_json::json!({ "poison": true }),
        &format!("{queue}-tx"),
    )
    .await;

    let msgs = pop_retry(&q, &queue, None, 1, 25).await;
    assert_eq!(msgs.len(), 1);

    let res = q
        .ack_with(&msgs[0], AckStatus::Dlq, Some("forced by test".into()))
        .await
        .unwrap();
    assert!(res.success, "dlq ack failed: {:?}", res.error);
    assert!(res.dlq, "the result should report the dead-lettering");

    sleep_ms(300).await;
    assert_eq!(
        dlq_count(&q, &queue).await,
        1,
        "not dead-lettered on the spot"
    );

    let again = q.queue(&queue).batch(5).wait(false).pop().await.unwrap();
    assert!(again.is_empty(), "still deliverable after a forced DLQ");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_lapsed_lease_costs_no_retry_budget() {
    let q = broker!();
    let queue = unique("expiry-budget");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(1),
            retry_limit: Some(2),
            ..Default::default()
        },
    )
    .await;
    push_only(
        &q,
        &queue,
        serde_json::json!({ "poison": true }),
        &format!("{queue}-tx"),
    )
    .await;

    // Four expiry cycles, double the budget: claim and walk away.
    for cycle in 0..4 {
        let msgs = pop_retry(&q, &queue, None, 1, 25).await;
        assert_eq!(
            msgs.len(),
            1,
            "cycle {cycle}: not redelivered after lease expiry — was the budget charged?"
        );
        sleep_ms(1600).await;
    }
    assert_eq!(
        dlq_count(&q, &queue).await,
        0,
        "lease expiries dead-lettered the message"
    );

    // The explicit budget must still be intact.
    for _ in 0..4 {
        let msgs = pop_retry(&q, &queue, None, 1, 10).await;
        if msgs.is_empty() {
            break;
        }
        q.nack(&msgs[0], "semantics-test failure").await.unwrap();
        sleep_ms(100).await;
    }
    assert_eq!(
        dlq_count(&q, &queue).await,
        1,
        "explicit failures did not have the full budget after the expiry cycles"
    );

    drop_queue(&q, &queue).await;
}

// ============================================================================
// Leases
// ============================================================================

#[tokio::test]
async fn an_ack_without_a_lease_still_works_after_the_lease_lapsed() {
    let q = broker!();
    let queue = unique("leaseless-ack");
    create_queue(&q, &queue, short_lease(1)).await;
    push_only(
        &q,
        &queue,
        serde_json::json!({ "n": 1 }),
        &format!("{queue}-tx-1"),
    )
    .await;

    let msgs = pop_retry(&q, &queue, None, 1, 25).await;
    assert_eq!(msgs.len(), 1);

    sleep_ms(2000).await; // the lease is now stale

    // Strip the lease: without one the broker skips lease validation, so a
    // handler that outran its claim can still commit its work.
    let mut leaseless = msgs[0].clone();
    leaseless.lease_id = String::new();
    let res = q.ack(&leaseless).await.unwrap();
    assert!(
        res.success,
        "a lease-less ack after expiry should succeed: {:?}",
        res.error
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn an_ack_carrying_a_lapsed_lease_is_refused() {
    let q = broker!();
    let queue = unique("stale-lease-ack");
    create_queue(&q, &queue, short_lease(1)).await;
    push_only(
        &q,
        &queue,
        serde_json::json!({ "n": 1 }),
        &format!("{queue}-tx-1"),
    )
    .await;

    let msgs = pop_retry(&q, &queue, None, 1, 25).await;
    assert_eq!(msgs.len(), 1);

    sleep_ms(2000).await; // the lease is now stale

    // The other half of `an_ack_without_a_lease_still_works_after_the_lease_lapsed`.
    // Dropping the lease is a deliberate "I know I lost it, commit anyway";
    // *presenting* an expired one is a consumer that thinks it still owns the
    // partition. Honouring it would let a zombie handler commit over whoever
    // re-claimed the lane in the meantime, so the ack has to be rejected —
    // which is only meaningful if the leased and lease-less paths differ.
    let res = q.ack(&msgs[0]).await.unwrap();
    assert!(
        !res.success,
        "an ack redeeming an expired lease was accepted; the lease check is not \
         running when a leaseId is present"
    );
    let err = res.error.unwrap_or_default();
    assert!(
        err.to_lowercase().contains("lease"),
        "the rejection should say the lease is the problem; got '{err}'"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_per_request_lease_override_beats_the_queue_setting() {
    let q = broker!();
    let queue = unique("lease-override");
    let group = format!("{queue}-cg");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(60),
            ..Default::default()
        },
    )
    .await;
    push_only(
        &q,
        &queue,
        serde_json::json!({ "n": 1 }),
        &format!("{queue}-tx"),
    )
    .await;

    let mut got = Vec::new();
    for _ in 0..25 {
        got = q
            .queue(&queue)
            .group(&group)
            .wait(false)
            .lease_seconds(1)
            .subscription_mode(SubscriptionMode::All)
            .pop()
            .await
            .unwrap();
        if !got.is_empty() {
            break;
        }
        sleep_ms(150).await;
    }
    assert_eq!(got.len(), 1, "nothing delivered on the override pop");

    // With the queue's own 60s lease this redelivery would not happen.
    sleep_ms(2500).await;
    let again = pop_retry(&q, &queue, Some(&group), 1, 15).await;
    assert_eq!(
        again.len(),
        1,
        "leaseSeconds=1 was ignored — the 60s queue lease was used instead"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn renewing_a_lease_keeps_a_slow_handler_in_possession() {
    let q = broker!();
    let queue = unique("lease-renew");
    create_queue(&q, &queue, short_lease(2)).await;
    push_only(
        &q,
        &queue,
        serde_json::json!({ "n": 1 }),
        &format!("{queue}-tx"),
    )
    .await;

    let msgs = pop_retry(&q, &queue, Some("g-renew"), 1, 25).await;
    assert_eq!(msgs.len(), 1);

    // Renew twice across a span longer than the 2s lease.
    for _ in 0..2 {
        sleep_ms(1200).await;
        assert!(
            q.renew(&msgs[0], Some(2)).await.unwrap(),
            "lease renewal was refused"
        );
    }

    // Still ours: nobody else could have claimed it.
    let stolen = q
        .queue(&queue)
        .group("g-renew")
        .wait(false)
        .pop()
        .await
        .unwrap();
    assert!(
        stolen.is_empty(),
        "the message was re-claimed despite an active renewal"
    );

    assert!(q.ack(&msgs[0]).await.unwrap().success);

    drop_queue(&q, &queue).await;
}

// ============================================================================
// Consumer behaviour on a dead lease
// ============================================================================

#[tokio::test]
async fn a_consumer_abandons_the_rest_of_the_batch_after_a_nack() {
    let q = broker!();
    let queue = unique("each-stop");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(30),
            retry_limit: Some(1),
            ..Default::default()
        },
    )
    .await;

    for n in 1..=5 {
        push_only(
            &q,
            &queue,
            serde_json::json!({ "n": n }),
            &format!("{queue}-tx-{n}"),
        )
        .await;
    }

    let seen: Arc<Mutex<HashMap<String, u32>>> = Arc::new(Mutex::new(HashMap::new()));
    let tally = Arc::clone(&seen);

    q.queue(&queue)
        .batch(5)
        .wait(false)
        .idle(Duration::from_secs(3))
        .consume(move |msg| {
            let tally = Arc::clone(&tally);
            async move {
                *tally
                    .lock()
                    .unwrap()
                    .entry(msg.transaction_id.clone())
                    .or_insert(0) += 1;
                if msg.data["n"] == 2 {
                    return Err("semantics-test poison");
                }
                Ok(())
            }
        })
        .await
        .unwrap();

    let counts = seen.lock().unwrap().clone();
    // #2 is poison. Every other message must be handled exactly once: after the
    // nack the lease is dead, so continuing would produce guaranteed duplicates.
    for n in [1, 3, 4, 5] {
        let txn = format!("{queue}-tx-{n}");
        let c = counts.get(&txn).copied().unwrap_or(0);
        assert!(c > 0, "{txn} was never processed");
        assert_eq!(c, 1, "{txn} was processed {c} times after a mid-batch nack");
    }

    assert_eq!(
        dlq_count(&q, &queue).await,
        1,
        "the poison message did not reach the DLQ"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_queue_mode_pop_never_skips_backlog() {
    let q = broker!();
    let queue = unique("queue-mode-backlog");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push_many((1..=3).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();

    // Queue mode carrying a subscription hint must still drain what is already
    // there — the hint only seeds a *group* cursor, and queue mode has none.
    let mut drained = 0;
    for _ in 0..25 {
        let msgs = q
            .queue(&queue)
            .batch(10)
            .wait(false)
            .subscription_mode(queen_mq::SubscriptionMode::New)
            .pop()
            .await
            .unwrap();
        if msgs.is_empty() {
            sleep_ms(150).await;
            continue;
        }
        drained += msgs.len();
        q.ack_all(&msgs).await.unwrap();
        if drained >= 3 {
            break;
        }
    }
    assert_eq!(drained, 3, "queue-mode pop skipped backlog");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_push_only_queue_is_visible_and_discoverable() {
    let q = broker!();
    // A dotted name is how a namespace and a task are declared *without* ever
    // calling /configure: the push path derives them from the name. Both halves
    // of that contract are checked here — the derived row, and the discovery
    // pop that is the only reason the row's namespace matters.
    let namespace = unique("push-only-ns");
    let queue = format!("{namespace}.taskA.q");
    push_only(
        &q,
        &queue,
        serde_json::json!({ "n": 1 }),
        &format!("{queue}-tx"),
    )
    .await;

    let mut entry = None;
    for _ in 0..20 {
        let listed = q
            .admin()
            .list_queues(&[("limit", "500".to_string())])
            .await
            .unwrap();
        entry = queue_entry(&listed, &queue);
        if entry.is_some() {
            break;
        }
        sleep_ms(200).await;
    }
    let entry = entry.unwrap_or_else(|| {
        panic!("the push-only queue '{queue}' never appeared in /resources/queues")
    });

    assert_eq!(
        entry.get("namespace").and_then(|v| v.as_str()),
        Some(namespace.as_str()),
        "the namespace was not derived from the queue name: {entry}"
    );
    assert_eq!(
        entry.get("task").and_then(|v| v.as_str()),
        Some("taskA"),
        "the task was not derived from the queue name: {entry}"
    );

    // Discovery by namespace, in queue mode: the point is that the broker can
    // find a queue nobody ever configured, so no consumer group is involved.
    let mut found = 0;
    for _ in 0..25 {
        let msgs = q
            .queue_opt(None)
            .namespace(&namespace)
            .batch(5)
            .wait(false)
            .pop()
            .await
            .unwrap();
        found += msgs.len();
        if found > 0 {
            break;
        }
        sleep_ms(200).await;
    }
    assert_eq!(
        found, 1,
        "namespace discovery never reached the push-only queue — a derived \
         namespace nothing can pop by is only half the feature"
    );

    drop_queue(&q, &queue).await;
}

/// One entry of `/api/v1/resources/queues`, which renders as `{"queues":[..]}`
/// but is tolerated as a bare array by every other SDK's test.
fn queue_entry(listed: &serde_json::Value, name: &str) -> Option<serde_json::Value> {
    let items = listed
        .get("queues")
        .and_then(|q| q.as_array())
        .or_else(|| listed.as_array())?;
    items
        .iter()
        .find(|e| e.get("name").and_then(|v| v.as_str()) == Some(name))
        .cloned()
}

#[tokio::test]
async fn message_detail_keeps_its_full_shape() {
    let q = broker!();
    let queue = unique("msg-detail");
    create_queue(&q, &queue, QueueOptions::default()).await;
    push_only(
        &q,
        &queue,
        serde_json::json!({ "n": 1 }),
        &format!("{queue}-tx"),
    )
    .await;

    // Popped and left claimed, so there is a consumer row for consumerGroups[]
    // and a live lease for the status derivation.
    let msgs = pop_retry(&q, &queue, Some("g-detail"), 1, 25).await;
    assert_eq!(msgs.len(), 1);

    let detail = q
        .admin()
        .message(&msgs[0].partition_id, &msgs[0].transaction_id)
        .await
        .unwrap();

    // The webapp reads every one of these keys. They are asserted flatly and
    // unconditionally: an earlier version of this test nested its only real
    // check inside two `if let Some(..)`, so a response that dropped
    // `consumerGroups` altogether passed it. Absence has to be the failure.
    for key in [
        "id",
        "transactionId",
        "partitionId",
        "partition",
        "queue",
        "queuePath",
        "namespace",
        "task",
        "status",
        "data",
        "payload",
        "createdAt",
        "retryCount",
        "queueConfig",
        "consumerGroups",
    ] {
        assert!(
            detail.get(key).is_some(),
            "message detail lost the `{key}` key: {detail}"
        );
    }
    // Keys the detail SP fills in only when it found the partition. Null here
    // means the join silently degraded, which the presence check above cannot
    // see.
    for key in [
        "id",
        "transactionId",
        "partitionId",
        "partition",
        "queue",
        "queuePath",
        "status",
        "createdAt",
        "queueConfig",
        "consumerGroups",
    ] {
        assert!(
            !detail[key].is_null(),
            "message detail degraded `{key}` to null: {detail}"
        );
    }

    assert_eq!(
        detail["transactionId"].as_str(),
        Some(format!("{queue}-tx").as_str()),
        "the detail is for another message: {detail}"
    );
    // `data` is what the SDKs read and `payload` is what the webapp reads. They
    // are the same value, and dropping either one breaks a caller.
    assert_eq!(
        detail["data"],
        serde_json::json!({ "n": 1 }),
        "the payload did not survive the round trip: {detail}"
    );
    assert_eq!(
        detail["payload"], detail["data"],
        "`payload` and `data` diverged: {detail}"
    );
    assert_eq!(
        detail["status"].as_str(),
        Some("processing"),
        "a claimed, unacked message with a live lease must read as processing: {detail}"
    );
    assert!(
        detail["queueConfig"].get("leaseTime").is_some(),
        "queueConfig lost leaseTime: {}",
        detail["queueConfig"]
    );

    let groups = detail["consumerGroups"]
        .as_array()
        .unwrap_or_else(|| panic!("consumerGroups is not an array: {detail}"));
    assert!(
        !groups.is_empty(),
        "consumerGroups is empty despite an active consumer: {detail}"
    );
    // consumerGroups[].name — not .group. The dashboard reads `name`, and the
    // rename would break it silently.
    assert!(
        groups[0].get("name").is_some(),
        "consumerGroups entries lost their `name` key: {}",
        groups[0]
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn subscription_new_delivers_a_partition_born_after_the_group_registered() {
    let q = broker!();
    let queue = unique("late-part");
    let group = format!("{queue}-cg");
    create_queue(&q, &queue, QueueOptions::default()).await;

    // Backlog on a lane that exists before the group does.
    push_to_partition(
        &q,
        &queue,
        "p-early",
        serde_json::json!({ "phase": "backlog" }),
        &format!("{queue}-backlog"),
    )
    .await;
    assert!(
        wait_for_stored(&q, &queue, 1).await >= 1,
        "setup: the backlog never became visible, so registering the group here \
         would prove nothing"
    );

    // The first pop is what registers the group. In `new` mode it is empty by
    // definition.
    let first = q
        .queue(&queue)
        .group(&group)
        .batch(5)
        .partitions(8)
        .wait(false)
        .subscription_mode(SubscriptionMode::New)
        .pop()
        .await
        .unwrap();
    assert!(
        first.is_empty(),
        "subscriptionMode=new delivered {} message(s) of pre-subscription backlog",
        first.len()
    );

    // A lane that did not exist at registration time. Its cursor has to be
    // seeded from the *group's* subscription timestamp, not from the moment the
    // partition is first met — otherwise everything written to a new partition
    // before the group next polls is skipped, silently, forever. That is the
    // regression this test exists for; `subscription_mode_new_skips_the_existing_backlog`
    // in core.rs only covers a partition the group already knew.
    push_to_partition(
        &q,
        &queue,
        "p-late",
        serde_json::json!({ "phase": "late" }),
        &format!("{queue}-late"),
    )
    .await;

    let late = format!("{queue}-late");
    let backlog = format!("{queue}-backlog");
    let mut seen: Vec<String> = Vec::new();
    for _ in 0..30 {
        // No subscriptionMode on the follow-up polls, exactly as a real
        // consumer loop would: the seeding decision was made at registration.
        let msgs = q
            .queue(&queue)
            .group(&group)
            .batch(5)
            .partitions(8)
            .wait(false)
            .pop()
            .await
            .unwrap();
        seen.extend(msgs.iter().map(|m| m.transaction_id.clone()));
        if seen.contains(&late) {
            break;
        }
        sleep_ms(150).await;
    }

    assert!(
        seen.contains(&late),
        "the late-created partition never delivered its post-subscription message; \
         saw {seen:?}"
    );
    assert!(
        !seen.contains(&backlog),
        "pre-subscription backlog reached a mode-new group; saw {seen:?}"
    );

    drop_queue(&q, &queue).await;
}
