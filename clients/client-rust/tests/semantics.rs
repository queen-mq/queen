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

use queen_mq::{AckStatus, QueueOptions};

use common::*;

/// Push without configuring: the queue is created on first push and inherits
/// the server defaults (`retryLimit=3`, `deadLetterQueue=true`).
async fn push_only(q: &queen_mq::Queen, queue: &str, payload: serde_json::Value, txn: &str) {
    q.queue(queue)
        .push_items(vec![queen_mq::PushItem::new(queue, payload).transaction_id(txn)])
        .await
        .unwrap();
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
    assert!(res.success, "ack of the last message failed: {:?}", res.error);

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
        let res = q
            .ack_with(&msgs[0], AckStatus::Retry, None)
            .await
            .unwrap();
        assert!(res.success, "cycle {cycle}: retry ack failed: {:?}", res.error);
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
    assert_eq!(dlq_count(&q, &queue).await, 1, "not dead-lettered on the spot");

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
    let queue = unique("push-only");
    // Never configured — the row must still appear.
    push_only(
        &q,
        &queue,
        serde_json::json!({ "n": 1 }),
        &format!("{queue}-tx"),
    )
    .await;

    let mut found = false;
    for _ in 0..20 {
        let listed = q
            .admin()
            .list_queues(&[("limit", "500".to_string())])
            .await
            .unwrap();
        let text = listed.to_string();
        if text.contains(&queue) {
            found = true;
            break;
        }
        sleep_ms(200).await;
    }
    assert!(found, "a push-only queue never appeared in the resource list");

    drop_queue(&q, &queue).await;
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

    let msgs = pop_retry(&q, &queue, Some("g-detail"), 1, 25).await;
    assert_eq!(msgs.len(), 1);

    let detail = q
        .admin()
        .message(&msgs[0].partition_id, &msgs[0].transaction_id)
        .await
        .unwrap();

    // consumerGroups[].name — not .group. The dashboard reads `name`, and the
    // rename would break it silently.
    if let Some(groups) = detail.get("consumerGroups").and_then(|g| g.as_array()) {
        if let Some(first) = groups.first() {
            assert!(
                first.get("name").is_some(),
                "consumerGroups entries lost their `name` key: {first}"
            );
        }
    }
    assert!(
        detail.get("transactionId").is_some() || detail.get("transaction_id").is_some(),
        "message detail carries no transaction id: {detail}"
    );

    drop_queue(&q, &queue).await;
}
