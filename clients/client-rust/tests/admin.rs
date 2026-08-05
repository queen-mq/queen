//! Transactions, DLQ, consumer groups, retention, producer identity, and the
//! observability endpoints.
//!
//! Ports the `transaction`, `dlq`, `retention`, `auth`, `bootstrap` and
//! `watermark` areas of the JS, Go and Python suites.

mod common;

use std::time::Duration;

use queen_mq::{
    AckStatus, DlqParams, DlqResponse, Message, Queen, QueueOptions, SeekRequest, SubscriptionMode,
    TraceRequest,
};

use common::*;

/// The reason a nack carries in the dead-letter tests. Distinctive on purpose:
/// an assertion that it came back is only worth anything if no other code path
/// could have produced the same string.
const DLQ_REASON: &str = "handler gave up: downstream 503";

// ------------------------------------------------------------------ helpers

/// Claim one *named* lane, riding out the fusion delay the way `pop_retry`
/// does.
///
/// `pop_retry` always addresses the queue as a whole and a multi-partition pop
/// is allowed to come back partial, so a test that needs a claim on two
/// specific partitions has to ask for them one at a time.
async fn pop_lane(
    queen: &Queen,
    queue: &str,
    partition: &str,
    group: &str,
    tries: usize,
) -> Vec<Message> {
    for _ in 0..tries {
        let msgs = queen
            .queue(queue)
            .partition(partition)
            .group(group)
            .batch(10)
            .wait(false)
            .subscription_mode(SubscriptionMode::All)
            .pop()
            .await
            .expect("lane pop failed");
        if !msgs.is_empty() {
            return msgs;
        }
        sleep_ms(150).await;
    }
    Vec::new()
}

/// Read the DLQ until it holds `want` rows, or give up and return what there
/// is so the assertion can say what actually arrived.
///
/// The dead-letter row is filed after the ack commits; a fixed sleep is either
/// flaky or slow.
async fn dlq_until(queen: &Queen, queue: &str, want: usize, tries: usize) -> DlqResponse {
    let mut last = DlqResponse::default();
    for _ in 0..tries {
        last = queen
            .queue(queue)
            .dlq(Some(50), None)
            .await
            .expect("dlq read failed");
        if last.messages.len() >= want {
            return last;
        }
        sleep_ms(200).await;
    }
    last
}

/// Push one payload and nack it until the queue's retry budget dead-letters it.
///
/// Returns the message as it was last delivered, so the caller can address the
/// DLQ row by `(partitionId, transactionId)`. The queue must already be
/// configured with a small `retry_limit`.
async fn dead_letter_one(
    queen: &Queen,
    queue: &str,
    group: &str,
    payload: serde_json::Value,
    reason: &str,
) -> Message {
    queen
        .queue(queue)
        .push(payload)
        .await
        .expect("push failed before the message could be dead-lettered");

    let mut last = None;
    for _ in 0..6 {
        let msgs = pop_retry(queen, queue, Some(group), 1, 12).await;
        if msgs.is_empty() {
            break;
        }
        queen.nack(&msgs[0], reason).await.expect("nack failed");
        last = Some(msgs[0].clone());
        sleep_ms(150).await;
    }
    last.expect("the message was never delivered, so it could not be dead-lettered")
}

// ------------------------------------------------------------ transactions

#[tokio::test]
async fn a_transaction_acks_and_pushes_atomically() {
    let q = broker!();
    let src = unique("txn-src");
    let dst = unique("txn-dst");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &dst, QueueOptions::default()).await;

    q.queue(&src)
        .push(serde_json::json!({ "stage": 1 }))
        .await
        .unwrap();
    let msgs = pop_retry(&q, &src, Some("g-txn"), 1, 25).await;
    assert_eq!(msgs.len(), 1);

    // docs:start(rust-transaction)
    let resp = q
        .transaction()
        .ack(&msgs[0])
        .push(dst.clone(), serde_json::json!({ "stage": 2 }))
        .unwrap()
        .commit()
        .await
        .unwrap();
    // docs:end

    assert!(resp.success);
    assert_eq!(resp.results.len(), 2, "one result per operation");
    assert!(!resp.transaction_id.is_empty());

    // The ack committed...
    let again = q
        .queue(&src)
        .group("g-txn")
        .batch(5)
        .wait(false)
        .pop()
        .await
        .unwrap();
    assert!(again.is_empty(), "the transactional ack did not commit");

    // ...and the push landed.
    let next = pop_retry(&q, &dst, None, 1, 25).await;
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].data["stage"], 2);

    drop_queue(&q, &src).await;
    drop_queue(&q, &dst).await;
}

#[tokio::test]
async fn a_transaction_rolls_back_when_an_ack_is_bogus() {
    let q = broker!();
    let dst = unique("txn-rollback-dst");
    create_queue(&q, &dst, QueueOptions::default()).await;

    // Ack a message that does not exist, alongside a push. Both must vanish.
    let phantom = queen_mq::Message {
        id: "x".into(),
        transaction_id: format!("{dst}-does-not-exist"),
        trace_id: None,
        data: serde_json::json!(null),
        producer_sub: None,
        created_at: "2026-08-04T10:00:00Z".into(),
        partition_id: "00000000-0000-7000-8000-000000000000".into(),
        partition: "Default".into(),
        lease_id: String::new(),
        consumer_group: "g-rollback".into(),
    };

    let out = q
        .transaction()
        .ack(&phantom)
        .push(dst.clone(), serde_json::json!({ "should": "not exist" }))
        .unwrap()
        .commit()
        .await;

    assert!(out.is_err(), "a bogus ack should roll the transaction back");

    // The push must NOT have landed.
    sleep_ms(500).await;
    let leaked = q.queue(&dst).batch(5).wait(false).pop().await.unwrap();
    assert!(
        leaked.is_empty(),
        "the push survived a rolled-back transaction — atomicity is broken"
    );

    drop_queue(&q, &dst).await;
}

#[tokio::test]
async fn a_transaction_honours_the_dlq_ack_status() {
    let q = broker!();
    let queue = unique("txn-dlq");
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
    let sink = unique("txn-dlq-sink");
    create_queue(&q, &sink, QueueOptions::default()).await;

    q.queue(&queue)
        .push(serde_json::json!({ "poison": true }))
        .await
        .unwrap();
    let msgs = pop_retry(&q, &queue, Some("g-txndlq"), 1, 25).await;
    assert_eq!(msgs.len(), 1);

    // `dlq` must survive to SQL rather than collapsing to a boolean nack.
    q.transaction()
        .ack_with(&msgs[0], AckStatus::Dlq)
        .push(sink.clone(), serde_json::json!({ "noted": true }))
        .unwrap()
        .commit()
        .await
        .unwrap();

    sleep_ms(400).await;
    assert_eq!(
        dlq_count(&q, &queue).await,
        1,
        "a transactional `dlq` ack did not dead-letter (collapsed to a plain nack?)"
    );

    drop_queue(&q, &queue).await;
    drop_queue(&q, &sink).await;
}

// A handoff routinely settles work claimed from several lanes at once, and each
// ack op carries its own `partitionId` and lease. A transaction that quietly
// applied them all to the first partition would still answer `success: true`,
// leave the other lane claimed, and only show up as a redelivery much later.
#[tokio::test]
async fn one_transaction_acks_claims_taken_from_two_partitions() {
    let q = broker!();
    let src = unique("txn-multipart");
    let sink = unique("txn-multipart-sink");
    let group = format!("{src}-cg");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    for lane in ["a", "b"] {
        q.queue(&src)
            .partition(lane)
            .push(serde_json::json!({ "lane": lane }))
            .await
            .unwrap();
    }

    let a = pop_lane(&q, &src, "a", &group, 25).await;
    let b = pop_lane(&q, &src, "b", &group, 25).await;
    assert_eq!(a.len(), 1, "lane a never delivered");
    assert_eq!(b.len(), 1, "lane b never delivered");
    assert_ne!(
        a[0].partition_id, b[0].partition_id,
        "the two lanes resolved to the same partition, so this proves nothing"
    );

    let resp = q
        .transaction()
        .ack(&a[0])
        .ack(&b[0])
        .push(sink.clone(), serde_json::json!({ "lanes": 2 }))
        .unwrap()
        .commit()
        .await
        .expect("the cross-partition transaction was rejected");
    assert!(resp.success);
    assert_eq!(resp.results.len(), 3, "two acks and one push: {resp:?}");

    for lane in ["a", "b"] {
        let left = q
            .queue(&src)
            .partition(lane)
            .group(&group)
            .batch(10)
            .wait(false)
            .pop()
            .await
            .unwrap();
        assert!(
            left.is_empty(),
            "lane {lane} is still claimable, so its ack did not commit"
        );
    }

    let out = pop_retry(&q, &sink, None, 1, 25).await;
    assert_eq!(out.len(), 1, "the handoff push did not land");
    assert_eq!(out[0].data["lanes"], 2);

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

// An ack moves ONE group's cursor. If a transactional ack were applied without
// its `consumerGroup` — the field is optional on the wire — the broker would
// settle the queue-mode cursor instead and the second group would silently lose
// the message. Bus fan-out is the headline feature; this is the test that says
// a transaction does not break it.
#[tokio::test]
async fn a_transactional_ack_leaves_another_groups_copy_alone() {
    let q = broker!();
    let queue = unique("txn-groups");
    let first = format!("{queue}-one");
    let second = format!("{queue}-two");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();

    let mine = pop_retry(&q, &queue, Some(&first), 1, 25).await;
    assert_eq!(mine.len(), 1);
    q.transaction()
        .ack(&mine[0])
        .commit()
        .await
        .expect("an ack-only transaction should commit");

    let drained = q
        .queue(&queue)
        .group(&first)
        .batch(5)
        .wait(false)
        .pop()
        .await
        .unwrap();
    assert!(drained.is_empty(), "the acking group can still see it");

    let theirs = pop_retry(&q, &queue, Some(&second), 1, 25).await;
    assert_eq!(
        theirs.len(),
        1,
        "the second group lost its copy to another group's transactional ack"
    );
    assert_eq!(theirs[0].data["n"], 1);

    drop_queue(&q, &queue).await;
}

// The documented way to make a handoff safe is to settle inside the handler
// rather than letting the loop ack afterwards. That needs `auto_ack(false)`, and
// it needs the loop to keep its hands off the message when the handler returns
// Ok — otherwise every consumer-driven handoff double-acks.
#[tokio::test]
async fn a_consumer_can_settle_its_message_inside_a_transaction() {
    let q = broker!();
    let src = unique("txn-consume-src");
    let sink = unique("txn-consume-sink");
    let group = format!("{src}-cg");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    q.queue(&src)
        .push(serde_json::json!({ "stage": 1 }))
        .await
        .unwrap();

    let handoff = q.clone();
    let sink_name = sink.clone();
    let summary = q
        .queue(&src)
        .group(&group)
        .auto_ack(false)
        .limit(1)
        .idle(Duration::from_secs(15))
        .wait(false)
        .subscription_mode(SubscriptionMode::All)
        .consume(move |msg| {
            let queen = handoff.clone();
            let sink = sink_name.clone();
            async move {
                queen
                    .transaction()
                    .ack(&msg)
                    .push(sink, serde_json::json!({ "stage": 2 }))?
                    .commit()
                    .await?;
                Ok::<(), queen_mq::Error>(())
            }
        })
        .await
        .expect("consume returned an error");

    assert_eq!(summary.processed, 1, "the handler never ran: {summary:?}");
    assert_eq!(
        summary.acked, 0,
        "auto_ack was off, so the loop must not have acked as well: {summary:?}"
    );

    let left = q
        .queue(&src)
        .group(&group)
        .batch(5)
        .wait(false)
        .pop()
        .await
        .unwrap();
    assert!(
        left.is_empty(),
        "the transactional ack inside the handler did not commit"
    );

    let out = pop_retry(&q, &sink, None, 1, 25).await;
    assert_eq!(out.len(), 1, "the handoff push did not land");
    assert_eq!(out[0].data["stage"], 2);

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

// `TxnAckOperation` grew an `error` field precisely because a transactional
// failure used to dead-letter with no reason at all while the plain nack route
// carried one. Only an end-to-end read of the DLQ row proves the field reaches
// SQL: the request serializes cleanly either way.
#[tokio::test]
async fn a_transactional_nack_records_its_reason_on_the_dlq_row() {
    let q = broker!();
    let queue = unique("txn-nack-reason");
    let group = format!("{queue}-cg");
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

    q.queue(&queue)
        .push(serde_json::json!({ "poison": true }))
        .await
        .unwrap();

    for _ in 0..6 {
        let msgs = pop_retry(&q, &queue, Some(&group), 1, 12).await;
        if msgs.is_empty() {
            break;
        }
        q.transaction()
            .nack(&msgs[0], DLQ_REASON)
            .commit()
            .await
            .expect("a transactional nack was rejected");
        sleep_ms(150).await;
    }

    let dlq = dlq_until(&q, &queue, 1, 20).await;
    assert_eq!(dlq.messages.len(), 1, "the message never dead-lettered");
    assert_eq!(
        dlq.messages[0].error.as_deref(),
        Some(DLQ_REASON),
        "the transactional nack's reason never reached the DLQ row: {:?}",
        dlq.messages[0]
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_trace_id_survives_a_transactional_push() {
    let q = broker!();
    let queue = unique("txn-trace");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let trace = queen_mq::uuid::uuidv7();
    q.transaction()
        .push_item(
            queen_mq::TxnPushItem::new(&queue, serde_json::json!({ "n": 1 }))
                .trace_id(trace.clone()),
        )
        .unwrap()
        .commit()
        .await
        .unwrap();

    let msgs = pop_retry(&q, &queue, None, 1, 25).await;
    assert_eq!(msgs.len(), 1);
    assert_eq!(
        msgs[0].trace_id.as_deref(),
        Some(trace.as_str()),
        "the transaction path is supposed to carry a trace id, unlike a plain push"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_plain_push_cannot_carry_a_trace_id() {
    let q = broker!();
    let queue = unique("push-trace-drop");
    create_queue(&q, &queue, QueueOptions::default()).await;

    // Send the key the other SDKs send, by hand, and confirm the broker drops
    // it. This is the regression guard for the asymmetry documented on
    // queen_protocol::PushItem: if the push path ever learns to store a trace
    // id, this test fails and the client gains the field deliberately.
    let raw = serde_json::json!({
        "items": [{
            "queue": queue,
            "partition": "Default",
            "payload": { "n": 1 },
            "transactionId": format!("{queue}-tx"),
            "traceId": queen_mq::uuid::uuidv7(),
        }]
    });
    let url = std::env::var("QUEEN_TEST_URL").unwrap();
    let resp = reqwest_post(&format!("{url}/api/v1/push"), &raw).await;
    assert!(resp.contains("queued"), "raw push failed: {resp}");

    let msgs = pop_retry(&q, &queue, None, 1, 25).await;
    assert_eq!(msgs.len(), 1);
    assert!(
        msgs[0].trace_id.is_none(),
        "the push path now stores a trace id — queen_protocol::PushItem should expose it"
    );

    drop_queue(&q, &queue).await;
}

/// Minimal raw POST, for the handful of cases that must bypass the client to
/// prove what the *broker* does.
async fn reqwest_post(url: &str, body: &serde_json::Value) -> String {
    let out = std::process::Command::new("curl")
        .args([
            "-s",
            "-X",
            "POST",
            url,
            "-H",
            "Content-Type: application/json",
            "-d",
            &body.to_string(),
        ])
        .output()
        .expect("curl is required for the raw-wire tests");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

// -------------------------------------------------------------------- dlq

#[tokio::test]
async fn exhausted_messages_land_in_the_dlq_with_their_reason() {
    let q = broker!();
    let queue = unique("dlq-basic");
    let group = format!("{queue}-cg");
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

    let payload = serde_json::json!({ "poison": true, "n": 7 });
    let dead = dead_letter_one(&q, &queue, &group, payload.clone(), DLQ_REASON).await;

    let dlq = dlq_until(&q, &queue, 1, 20).await;
    assert_eq!(dlq.messages.len(), 1, "the message never dead-lettered");
    assert_eq!(
        dlq.total, 1,
        "`total` is the length of the returned page, not the size of the DLQ"
    );

    // The row's *content* is the point. Until `DlqMessage.error` was pointed at
    // the key the SP actually projects (`errorMessage`), every one of these
    // assertions but the count passed while the reason sat unread in `rest`.
    let row = &dlq.messages[0];
    assert_eq!(
        row.error.as_deref(),
        Some(DLQ_REASON),
        "the nack reason did not come back on the DLQ row: {row:?}"
    );
    assert_eq!(
        row.data, payload,
        "the DLQ snapshot is not the payload that was pushed"
    );
    assert_eq!(row.queue.as_deref(), Some(queue.as_str()));
    assert_eq!(row.partition.as_deref(), Some("Default"));
    assert_eq!(
        row.transaction_id.as_deref(),
        Some(dead.transaction_id.as_str()),
        "the DLQ row is addressed by a different transaction id than the delivery"
    );
    assert_eq!(
        row.partition_id.as_deref(),
        Some(dead.partition_id.as_str())
    );
    // Everything the struct does not name stays reachable rather than being
    // dropped on the floor.
    assert_eq!(
        row.rest.get("consumerGroup"),
        Some(&serde_json::json!(group)),
        "the DLQ row lost the group that failed it: {row:?}"
    );
    assert!(
        row.rest
            .get("retryCount")
            .and_then(|v| v.as_i64())
            .is_some(),
        "the DLQ row lost its retry count: {row:?}"
    );

    drop_queue(&q, &queue).await;
}

// `Admin::dlq` is the only DLQ reader that can filter by consumer group, and the
// type deliberately exposes just the four query keys the broker reads. A filter
// that reached the broker but not the stored procedure would return an
// unfiltered page and no error — which reads as success.
#[tokio::test]
async fn the_admin_dlq_reader_filters_by_queue_and_group() {
    let q = broker!();
    let queue = unique("dlq-filter");
    let group = format!("{queue}-cg");
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

    dead_letter_one(
        &q,
        &queue,
        &group,
        serde_json::json!({ "poison": true }),
        DLQ_REASON,
    )
    .await;
    assert_eq!(dlq_until(&q, &queue, 1, 20).await.messages.len(), 1);

    let params = |group: Option<String>, queue: Option<String>| DlqParams {
        queue,
        consumer_group: group,
        limit: Some(50),
        offset: None,
    };

    let mine = q
        .admin()
        .dlq(params(Some(group.clone()), Some(queue.clone())))
        .await
        .unwrap();
    assert_eq!(
        mine.messages.len(),
        1,
        "the row is not readable through Admin::dlq: {mine:?}"
    );
    assert_eq!(mine.total, mine.messages.len());

    let other_group = q
        .admin()
        .dlq(params(Some(format!("{group}-nobody")), Some(queue.clone())))
        .await
        .unwrap();
    assert!(
        other_group.messages.is_empty(),
        "consumerGroup did not filter — a foreign group saw the row: {other_group:?}"
    );

    let other_queue = q
        .admin()
        .dlq(params(None, Some(format!("{queue}-nobody"))))
        .await
        .unwrap();
    assert!(
        other_queue.messages.is_empty(),
        "queue did not filter: {other_queue:?}"
    );

    drop_queue(&q, &queue).await;
}

// `DELETE /messages/{pid}/{txn}` deletes a *dead-letter row* and nothing else —
// live messages live in immutable segments. A client that treated it as a
// general "delete this message" would report success on a claim somebody is
// still holding.
#[tokio::test]
async fn a_dead_letter_row_can_be_deleted_and_a_live_message_cannot() {
    let q = broker!();
    let queue = unique("dlq-delete");
    let group = format!("{queue}-cg");
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

    q.queue(&queue)
        .push(serde_json::json!({ "live": true }))
        .await
        .unwrap();
    let live = pop_retry(&q, &queue, Some(&group), 1, 25).await;
    assert_eq!(live.len(), 1);
    let refused = q
        .admin()
        .delete_message(&live[0].partition_id, &live[0].transaction_id)
        .await
        .expect_err("deleting a live message reported success");
    assert_eq!(
        refused.status(),
        Some(404),
        "a live message should be 'not found' for the delete route: {refused}"
    );
    q.ack_all(&live).await.unwrap();

    let dead = dead_letter_one(
        &q,
        &queue,
        &group,
        serde_json::json!({ "poison": true }),
        DLQ_REASON,
    )
    .await;
    assert_eq!(dlq_until(&q, &queue, 1, 20).await.messages.len(), 1);

    let out = q
        .admin()
        .delete_message(&dead.partition_id, &dead.transaction_id)
        .await
        .expect("deleting the dead-letter row failed");
    assert_eq!(
        out.get("success").and_then(|v| v.as_bool()),
        Some(true),
        "the delete answered 200 without confirming: {out}"
    );
    assert_eq!(
        out.get("transactionId").and_then(|v| v.as_str()),
        Some(dead.transaction_id.as_str()),
        "the delete echoed a different address: {out}"
    );

    let after = q.queue(&queue).dlq(Some(50), None).await.unwrap();
    assert!(
        after.messages.is_empty(),
        "the row survived its own delete: {after:?}"
    );

    let twice = q
        .admin()
        .delete_message(&dead.partition_id, &dead.transaction_id)
        .await
        .expect_err("deleting the same row twice reported success");
    assert_eq!(twice.status(), Some(404));

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn the_dlq_honours_limit_and_offset() {
    let q = broker!();
    let queue = unique("dlq-page");
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

    q.queue(&queue)
        .push_many((0..4).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();

    // Dead-letter them all.
    for _ in 0..20 {
        let msgs = pop_retry(&q, &queue, None, 4, 8).await;
        if msgs.is_empty() {
            break;
        }
        for m in &msgs {
            q.nack(m, "paging test").await.unwrap();
        }
        sleep_ms(120).await;
    }

    let all = q.queue(&queue).dlq(Some(50), None).await.unwrap();
    assert!(!all.messages.is_empty(), "nothing reached the DLQ");

    let first = q.queue(&queue).dlq(Some(1), Some(0)).await.unwrap();
    assert_eq!(first.messages.len(), 1, "limit=1 was ignored");

    if all.messages.len() > 1 {
        let second = q.queue(&queue).dlq(Some(1), Some(1)).await.unwrap();
        assert_eq!(second.messages.len(), 1);
    }

    drop_queue(&q, &queue).await;
}

// --------------------------------------------------------- consumer groups

#[tokio::test]
async fn deleting_a_consumer_group_replays_its_queue_from_the_start() {
    let q = broker!();
    let queue = unique("cg-delete");
    let group = format!("{queue}-cg");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push_many((0..3).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();

    let first = pop_retry(&q, &queue, Some(&group), 10, 25).await;
    assert_eq!(first.len(), 3);
    q.ack_all(&first).await.unwrap();

    // Drained.
    let empty = q
        .queue(&queue)
        .group(&group)
        .batch(10)
        .wait(false)
        .pop()
        .await
        .unwrap();
    assert!(empty.is_empty());

    // Deleting the group drops its cursor, so the backlog is deliverable again.
    q.admin()
        .delete_consumer_group_for_queue(&group, &queue, true)
        .await
        .unwrap();

    let replayed = pop_retry(&q, &queue, Some(&group), 10, 25).await;
    assert_eq!(
        replayed.len(),
        3,
        "deleting the group did not reset its cursor"
    );

    drop_queue(&q, &queue).await;
}

// The per-queue delete had a test; the cross-queue one had none. They are
// different routes over different SQL, and the difference — every queue, not
// just the one named — is the whole reason the method exists. A method wired to
// the per-queue path would still pass a single-queue test.
#[tokio::test]
async fn deleting_a_consumer_group_resets_it_on_every_queue_at_once() {
    let q = broker!();
    let first = unique("cg-delete-all-a");
    let second = unique("cg-delete-all-b");
    let group = format!("{first}-cg");
    create_queue(&q, &first, QueueOptions::default()).await;
    create_queue(&q, &second, QueueOptions::default()).await;

    for name in [&first, &second] {
        q.queue(name)
            .push_many((0..2).map(|n| serde_json::json!({ "n": n })))
            .await
            .unwrap();
        let msgs = pop_retry(&q, name, Some(&group), 10, 25).await;
        assert_eq!(msgs.len(), 2, "{name} did not deliver its backlog");
        q.ack_all(&msgs).await.unwrap();
    }

    let out = q
        .admin()
        .delete_consumer_group(&group, true)
        .await
        .expect("the cross-queue delete failed");
    assert_eq!(
        out.get("success").and_then(|v| v.as_bool()),
        Some(true),
        "delete reported no success: {out}"
    );
    assert_eq!(
        out.get("consumerGroup").and_then(|v| v.as_str()),
        Some(group.as_str())
    );
    assert!(
        out.get("deletedPartitions")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            >= 2,
        "one cursor per queue should have gone: {out}"
    );
    assert_eq!(
        out.get("metadataDeleted").and_then(|v| v.as_bool()),
        Some(true)
    );

    for name in [&first, &second] {
        let replayed = pop_retry(&q, name, Some(&group), 10, 30).await;
        assert_eq!(
            replayed.len(),
            2,
            "{name} did not replay after the group was deleted everywhere"
        );
    }

    drop_queue(&q, &first).await;
    drop_queue(&q, &second).await;
}

#[tokio::test]
async fn seeking_a_consumer_group_moves_its_cursor() {
    let q = broker!();
    let queue = unique("cg-seek");
    let group = format!("{queue}-cg");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push_many((0..3).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();

    let first = pop_retry(&q, &queue, Some(&group), 10, 25).await;
    assert_eq!(first.len(), 3);
    q.ack_all(&first).await.unwrap();

    // Seek back to the epoch. This is the supported way to replay —
    // subscription_from only ever seeds a cursor that does not exist yet — and
    // an instant is how you name the destination: the broker's seek body is
    // `{toEnd}` or `{timestamp}` (see the position test below).
    let reply = q
        .admin()
        .seek_consumer_group(
            &group,
            &queue,
            &SeekRequest {
                timestamp: Some("1970-01-01T00:00:00Z".into()),
                position: None,
            },
        )
        .await
        .expect("the broker refused the seek");

    assert_eq!(
        reply.get("success").and_then(|v| v.as_bool()),
        Some(true),
        "the seek answered 200 but reported failure: {reply}"
    );
    assert_eq!(
        reply.get("consumerGroup").and_then(|v| v.as_str()),
        Some(group.as_str()),
        "the seek was applied to a different group: {reply}"
    );
    assert_eq!(
        reply.get("queueName").and_then(|v| v.as_str()),
        Some(queue.as_str())
    );
    assert_eq!(
        reply.get("partitionsUpdated").and_then(|v| v.as_i64()),
        Some(1),
        "the seek moved no cursor at all: {reply}"
    );

    // And the replay has to actually happen. A cursor move nobody can observe
    // is indistinguishable from a no-op.
    let replayed = pop_retry(&q, &queue, Some(&group), 10, 30).await;
    assert_eq!(
        replayed.len(),
        3,
        "seeking to the epoch re-exposed {} of 3 messages",
        replayed.len()
    );
    let mut ns: Vec<i64> = replayed
        .iter()
        .filter_map(|m| m.data["n"].as_i64())
        .collect();
    ns.sort_unstable();
    assert_eq!(ns, vec![0, 1, 2], "the replay came back incomplete: {ns:?}");

    drop_queue(&q, &queue).await;
}

// `SeekRequest::position` is not a wire key: the broker reads `toEnd` or
// `timestamp` and 400s on anything else. The seek test above used to send
// `position: "earliest"`, catch that 400, print a SKIP and return green — which
// left `seek_consumer_group` with no binding coverage anywhere. Pinning the
// refusal keeps the mismatch visible: the day `position` means something, this
// fails instead of a skip hiding it.
#[tokio::test]
async fn a_seek_that_names_only_a_position_is_refused() {
    let q = broker!();
    let queue = unique("cg-seek-position");
    let group = format!("{queue}-cg");
    create_queue(&q, &queue, QueueOptions::default()).await;

    // Give the queue a real partition and the group a real cursor, so a refusal
    // can only be about the body.
    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();
    let msgs = pop_retry(&q, &queue, Some(&group), 1, 25).await;
    assert_eq!(msgs.len(), 1);
    q.ack_all(&msgs).await.unwrap();

    let err = q
        .admin()
        .seek_consumer_group(
            &group,
            &queue,
            &SeekRequest {
                timestamp: None,
                position: Some("earliest".into()),
            },
        )
        .await
        .expect_err("the broker accepted a seek with no destination it can read");
    assert_eq!(
        err.status(),
        Some(400),
        "expected a rejected body, got: {err}"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn consumer_groups_are_listed() {
    let q = broker!();
    let queue = unique("cg-list");
    let group = format!("{queue}-cg");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();
    let msgs = pop_retry(&q, &queue, Some(&group), 1, 25).await;
    assert_eq!(msgs.len(), 1);
    q.ack_all(&msgs).await.unwrap();

    let _ = q.admin().refresh_consumer_stats().await;

    let mut found = false;
    for _ in 0..15 {
        let listed = q.admin().list_consumer_groups().await.unwrap();
        if listed.to_string().contains(&group) {
            found = true;
            break;
        }
        sleep_ms(300).await;
    }
    assert!(found, "the consumer group never showed up in the listing");

    drop_queue(&q, &queue).await;
}

// The per-group detail endpoint backs the dashboard's group view. It is keyed by
// queue *name* with a `partitions` array underneath, and the entries are
// camelCase where the sibling `lagging` endpoint is snake_case — an asymmetry
// nothing else in this suite would notice if it flipped.
#[tokio::test]
async fn a_consumer_groups_detail_reports_its_partitions() {
    let q = broker!();
    let queue = unique("cg-detail");
    let group = format!("{queue}-cg");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push_many((0..2).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();
    let msgs = pop_retry(&q, &queue, Some(&group), 10, 25).await;
    assert_eq!(msgs.len(), 2);
    q.ack_all(&msgs).await.unwrap();

    let detail = q
        .admin()
        .consumer_group(&group)
        .await
        .expect("group detail failed");
    let per_queue = detail
        .get(queue.as_str())
        .unwrap_or_else(|| panic!("the detail does not mention {queue}: {detail}"));
    let partitions = per_queue
        .get("partitions")
        .and_then(|p| p.as_array())
        .unwrap_or_else(|| panic!("no partitions array under {queue}: {per_queue}"));
    assert_eq!(
        partitions.len(),
        1,
        "one lane was consumed, {} reported: {per_queue}",
        partitions.len()
    );

    let lane = &partitions[0];
    assert_eq!(
        lane.get("partition").and_then(|v| v.as_str()),
        Some("Default"),
        "the lane lost its name: {lane}"
    );
    assert!(
        lane.get("totalConsumed")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            >= 2,
        "the group consumed two messages but the detail disagrees: {lane}"
    );
    for key in ["workerId", "offsetLag", "timeLagSeconds", "leaseActive"] {
        assert!(
            lane.get(key).is_some(),
            "the partition entry lost `{key}`: {lane}"
        );
    }

    drop_queue(&q, &queue).await;
}

// `lagging_consumers` answers a BARE JSON array, unlike every neighbouring
// endpoint, and its keys are snake_case. Both are easy to "tidy up" server-side
// without anyone noticing; a caller that then does `.get("consumers")` sees an
// empty dashboard rather than an error.
#[tokio::test]
async fn a_group_that_falls_behind_is_reported_as_lagging() {
    let q = broker!();
    let queue = unique("cg-lagging");
    let group = format!("{queue}-cg");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push_many((0..2).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();

    // Consume exactly one: the group now has a cursor (without one it is not
    // reportable at all) and a backlog whose age is the lag.
    let first = pop_retry(&q, &queue, Some(&group), 1, 25).await;
    assert_eq!(first.len(), 1);
    q.ack_all(&first).await.unwrap();

    let mut mine = None;
    for _ in 0..25 {
        let list = q
            .admin()
            .lagging_consumers(0)
            .await
            .expect("lagging consumers failed");
        let rows = list
            .as_array()
            .unwrap_or_else(|| panic!("lagging consumers is a bare array, got: {list}"));
        mine = rows
            .iter()
            .find(|r| r.get("queue_name").and_then(|v| v.as_str()) == Some(queue.as_str()))
            .cloned();
        if mine.is_some() {
            break;
        }
        sleep_ms(400).await;
    }

    let row = mine.unwrap_or_else(|| {
        panic!("a group one message behind never showed up in the lagging list")
    });
    assert_eq!(
        row.get("consumer_group").and_then(|v| v.as_str()),
        Some(group.as_str()),
        "the lagging row names another group: {row}"
    );
    assert!(
        row.get("offset_lag").and_then(|v| v.as_i64()).unwrap_or(0) >= 1,
        "the backlog is one message, the row says otherwise: {row}"
    );
    for key in [
        "partition_name",
        "partition_id",
        "time_lag_seconds",
        "lag_hours",
    ] {
        assert!(row.get(key).is_some(), "the row lost `{key}`: {row}");
    }

    drop_queue(&q, &queue).await;
}

// The request key is `subscriptionTimestamp`, and the broker 400s when it is
// missing — so this pins the one thing the client controls. `rowsUpdated` is 0
// for a group with no metadata row: the route is an UPDATE, not an upsert, and
// asserting otherwise would be asserting a bug.
#[tokio::test]
async fn a_groups_subscription_timestamp_can_be_moved() {
    let q = broker!();
    let queue = unique("cg-subts");
    let group = format!("{queue}-cg");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();
    let msgs = pop_retry(&q, &queue, Some(&group), 1, 25).await;
    assert_eq!(msgs.len(), 1);
    q.ack_all(&msgs).await.unwrap();

    let when = iso_now();
    let out = q
        .admin()
        .set_subscription_timestamp(&group, when.clone())
        .await
        .expect("the broker rejected the subscription update");

    assert_eq!(
        out.get("success").and_then(|v| v.as_bool()),
        Some(true),
        "subscription update reported failure: {out}"
    );
    assert_eq!(
        out.get("consumerGroup").and_then(|v| v.as_str()),
        Some(group.as_str())
    );
    assert_eq!(
        out.get("newTimestamp").and_then(|v| v.as_str()),
        Some(when.as_str()),
        "the broker stored a different instant than it was sent: {out}"
    );
    assert!(
        out.get("rowsUpdated").and_then(|v| v.as_i64()).is_some(),
        "the reply lost its row count: {out}"
    );

    drop_queue(&q, &queue).await;
}

// ------------------------------------------------------------------ leases

// `Admin::renew_lease` is only ever reached through `Queen::renew`, which throws
// away everything but `success`. Nothing has ever read the count back, or the
// expiry — which the broker writes under three different key names for three
// different SDKs, so `expires_at()` picking the wrong one is invisible until an
// operator's dashboard shows a blank.
#[tokio::test]
async fn renewing_a_lease_reports_what_it_extended() {
    let q = broker!();
    let queue = unique("lease-renew");
    create_queue(&q, &queue, short_lease(30)).await;

    for lane in ["a", "b"] {
        q.queue(&queue)
            .partition(lane)
            .push(serde_json::json!({ "lane": lane }))
            .await
            .unwrap();
    }

    // One pop over both lanes shares one lease, so the renewal has to touch
    // every claim it took — a multi-partition pop is allowed to come back
    // partial, hence the count is compared against what actually arrived.
    let mut msgs = Vec::new();
    for _ in 0..25 {
        msgs = q
            .queue(&queue)
            .group("g-renew")
            .batch(10)
            .partitions(2)
            .wait(false)
            .subscription_mode(SubscriptionMode::All)
            .pop()
            .await
            .unwrap();
        if msgs.len() >= 2 {
            break;
        }
        sleep_ms(150).await;
    }
    assert!(!msgs.is_empty(), "nothing was ever claimed");
    let claimed: std::collections::HashSet<&str> =
        msgs.iter().map(|m| m.partition_id.as_str()).collect();

    let resp = q
        .admin()
        .renew_lease(&msgs[0].lease_id, Some(120))
        .await
        .expect("lease extension failed");
    assert!(resp.success, "the live lease was not renewed: {resp:?}");
    assert_eq!(
        resp.lease_id, msgs[0].lease_id,
        "the reply is about another lease: {resp:?}"
    );
    assert!(
        resp.renewed >= claimed.len() as i64,
        "{} partition claim(s) were held, {} renewed: {resp:?}",
        claimed.len(),
        resp.renewed
    );
    assert!(
        resp.expires_at().is_some(),
        "the new expiry did not decode under any of its three keys: {resp:?}"
    );

    // Renewal is best-effort: a lease that never existed answers 200 with
    // `success: false`, and a client that treated the HTTP status as the signal
    // would keep a handler running on a claim it no longer holds.
    let unknown = q
        .admin()
        .renew_lease(&queen_mq::uuid::uuidv7(), None)
        .await
        .expect("an unknown lease should still answer, not error");
    assert!(
        !unknown.success,
        "a lease that never existed was renewed: {unknown:?}"
    );
    assert_eq!(unknown.renewed, 0);
    assert!(unknown.expires_at().is_none());

    drop_queue(&q, &queue).await;
}

// --------------------------------------------------------------- resources

// `Admin::queue` had no call site anywhere. It is the endpoint the console's
// queue page is built on, and the shape it returns — per-partition `stats` plus
// a rolled-up `totals` — is the part that breaks quietly.
#[tokio::test]
async fn a_queue_detail_reports_its_partitions_and_totals() {
    let q = broker!();
    let queue = unique("res-queue");
    create_queue(&q, &queue, QueueOptions::default()).await;

    for lane in ["a", "b"] {
        q.queue(&queue)
            .partition(lane)
            .push(serde_json::json!({ "lane": lane }))
            .await
            .unwrap();
    }

    let mut detail = serde_json::Value::Null;
    for _ in 0..25 {
        detail = q.admin().queue(&queue).await.expect("queue detail failed");
        let lanes = detail
            .get("partitions")
            .and_then(|p| p.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        if lanes >= 2 {
            break;
        }
        sleep_ms(200).await;
    }

    assert_eq!(
        detail.get("name").and_then(|v| v.as_str()),
        Some(queue.as_str()),
        "queue detail is for another queue: {detail}"
    );
    for key in ["id", "createdAt", "totals", "partitions"] {
        assert!(detail.get(key).is_some(), "detail lost `{key}`: {detail}");
    }

    let partitions = detail
        .get("partitions")
        .and_then(|p| p.as_array())
        .unwrap_or_else(|| panic!("partitions is not an array: {detail}"));
    let names: Vec<&str> = partitions
        .iter()
        .filter_map(|p| p.get("name").and_then(|v| v.as_str()))
        .collect();
    assert!(
        names.contains(&"a") && names.contains(&"b"),
        "both lanes should be reported, got {names:?}"
    );

    let stats = partitions[0]
        .get("stats")
        .unwrap_or_else(|| panic!("a partition entry has no stats: {}", partitions[0]));
    for key in ["total", "pending", "processing", "completed", "deadLetter"] {
        assert!(stats.get(key).is_some(), "stats lost `{key}`: {stats}");
    }
    assert!(
        detail
            .get("totals")
            .and_then(|t| t.get("total"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            >= 2,
        "two messages were pushed, totals disagree: {detail}"
    );

    // A queue that does not exist is a 404, not an empty object the caller
    // would render as "queue with no partitions".
    let missing = q
        .admin()
        .queue(&format!("{queue}-nope"))
        .await
        .expect_err("an unknown queue answered successfully");
    assert_eq!(missing.status(), Some(404), "unexpected error: {missing}");

    drop_queue(&q, &queue).await;
}

// `Admin::partitions` posts to `/api/v1/resources/partitions`, which this broker
// does not route — the same trap already documented on `clear_queue` and
// `move_message_to_dlq`, but for a method that is still exposed. Pinning the 404
// means the day the route lands (or the method goes) somebody has to come here.
#[tokio::test]
async fn the_partitions_resource_is_not_a_route_on_this_broker() {
    let q = broker!();
    let err = q
        .admin()
        .partitions(&[])
        .await
        .expect_err("/api/v1/resources/partitions answered — the method is no longer dead");
    assert_eq!(err.status(), Some(404), "unexpected error: {err}");
    assert_eq!(
        err.code().map(|c| c.as_str()),
        Some("no_such_route"),
        "the broker's route-miss code did not survive decoding: {err}"
    );
}

// `Admin::list_messages` is the console's message browser. On this engine the
// rows carry `txnHash` and a `segment` locator instead of a transaction id, and
// the envelope carries a `mode` block — none of which the typed surface models,
// so nothing else would notice the shape changing.
#[tokio::test]
async fn messages_can_be_listed_for_one_queue() {
    let q = broker!();
    let queue = unique("res-messages");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push_many((0..2).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();

    let params = [("queue", queue.clone()), ("limit", "50".to_string())];
    let mut listed = serde_json::Value::Null;
    for _ in 0..25 {
        listed = q
            .admin()
            .list_messages(&params)
            .await
            .expect("list messages failed");
        let n = listed
            .get("messages")
            .and_then(|m| m.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        if n >= 2 {
            break;
        }
        sleep_ms(200).await;
    }

    let messages = listed
        .get("messages")
        .and_then(|m| m.as_array())
        .unwrap_or_else(|| panic!("no messages array: {listed}"));
    assert_eq!(
        messages.len(),
        2,
        "both pushes should be listed, got {}: {listed}",
        messages.len()
    );
    assert_eq!(
        listed.get("total").and_then(|v| v.as_i64()),
        Some(messages.len() as i64),
        "`total` is the page length, added by the route: {listed}"
    );
    assert!(
        listed
            .get("mode")
            .and_then(|m| m.get("type"))
            .and_then(|v| v.as_str())
            .is_some(),
        "the listing lost its queue/bus mode block: {listed}"
    );

    for m in messages {
        assert_eq!(
            m.get("queue").and_then(|v| v.as_str()),
            Some(queue.as_str()),
            "the queue filter let another queue's message through: {m}"
        );
        assert_eq!(m.get("partition").and_then(|v| v.as_str()), Some("Default"));
        for key in ["status", "txnHash", "partitionId", "segment"] {
            assert!(m.get(key).is_some(), "message row lost `{key}`: {m}");
        }
    }

    drop_queue(&q, &queue).await;
}

// -------------------------------------------------------------- retention

#[tokio::test]
async fn retention_settings_round_trip() {
    let q = broker!();
    let queue = unique("retention");

    let res = q
        .queue(&queue)
        .configure(QueueOptions {
            retention_enabled: Some(true),
            retention_seconds: Some(60),
            completed_retention_seconds: Some(30),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(res.get("configured").and_then(|v| v.as_bool()), Some(true));
    for (key, want) in [("retentionSeconds", 60), ("completedRetentionSeconds", 30)] {
        if let Some(got) = res.get(key).and_then(|v| v.as_i64()) {
            assert_eq!(got, want, "{key} did not round-trip");
        }
    }

    drop_queue(&q, &queue).await;
}

// ------------------------------------------------------- producer identity

#[tokio::test]
async fn a_producer_sub_in_the_body_is_ignored_without_auth() {
    let q = broker!();
    let queue = unique("auth-spoof");
    create_queue(&q, &queue, QueueOptions::default()).await;

    // producerSub is stamped only from a validated JWT. A caller putting it in
    // the body must not be able to forge an identity.
    let raw = serde_json::json!({
        "items": [{
            "queue": queue,
            "partition": "Default",
            "payload": { "n": 1 },
            "transactionId": format!("{queue}-tx"),
            "producerSub": "impersonated@example.com",
        }]
    });
    let url = std::env::var("QUEEN_TEST_URL").unwrap();
    let resp = reqwest_post(&format!("{url}/api/v1/push"), &raw).await;
    assert!(resp.contains("queued"), "raw push failed: {resp}");

    let msgs = pop_retry(&q, &queue, None, 1, 25).await;
    assert_eq!(msgs.len(), 1);
    assert!(
        msgs[0].producer_sub.is_none(),
        "a body-supplied producerSub was stamped onto the message: {:?}",
        msgs[0].producer_sub
    );

    drop_queue(&q, &queue).await;
}

// NOTE: the positive half of this pair — a JWT-authenticated push stamping
// `producerSub` from the token's `sub`, as Go's TestProducerSubStampedFromJwt
// and Python's test_auth.py cover — has no home here. It needs a broker running
// with JWT_ENABLED and a token signed with its secret; this harness runs with
// auth off, and minting an HS256 token would mean an HMAC dependency the client
// deliberately does not have. A test gated on an env var nothing sets would be
// another silent skip, which is exactly what this suite is removing.

#[tokio::test]
async fn a_bearer_token_is_accepted_when_auth_is_off() {
    // With authentication disabled the broker ignores the header; the point is
    // that sending one does not break anything.
    let Some(q) = client_with_token("a-token-that-is-not-a-jwt") else {
        skipped("a_bearer_token_is_accepted_when_auth_is_off");
        return;
    };
    let queue = unique("auth-token");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let res = q
        .queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();
    assert_eq!(res.len(), 1);

    drop_queue(&q, &queue).await;
}

// ------------------------------------------------------------------ traces

#[tokio::test]
async fn a_trace_event_can_be_recorded_and_read_back() {
    let q = broker!();
    let queue = unique("traces");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();
    let msgs = pop_retry(&q, &queue, Some("g-trace"), 1, 25).await;
    assert_eq!(msgs.len(), 1);

    let name = format!("{queue}-label");
    let resp = q
        .admin()
        .record_trace(&TraceRequest {
            transaction_id: msgs[0].transaction_id.clone(),
            partition_id: msgs[0].partition_id.clone(),
            consumer_group: msgs[0].consumer_group.clone(),
            trace_names: Some(vec![name.clone()]),
            event_type: "info".into(),
            data: serde_json::json!({ "step": "started" }),
        })
        .await
        .unwrap();
    assert!(
        resp.success || resp.error.is_none(),
        "trace failed: {resp:?}"
    );

    let found = q
        .admin()
        .traces_for_message(&msgs[0].partition_id, &msgs[0].transaction_id)
        .await
        .unwrap();
    assert!(
        found.to_string().contains("started"),
        "the recorded trace did not come back: {found}"
    );

    drop_queue(&q, &queue).await;
}

// The by-name lookup is the reason `traceNames` exists on a trace at all, and
// neither of the two endpoints that serve it had a call site. Their rows are
// snake_case (`transaction_id`, `event_type`) while the trace was *written* in
// camelCase — a round trip is the only thing that catches a rename on either
// side.
#[tokio::test]
async fn a_recorded_trace_is_reachable_by_its_name() {
    let q = broker!();
    let queue = unique("traces-byname");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();
    let msgs = pop_retry(&q, &queue, Some("g-tracename"), 1, 25).await;
    assert_eq!(msgs.len(), 1);

    let name = format!("{queue}-label");
    q.admin()
        .record_trace(&TraceRequest {
            transaction_id: msgs[0].transaction_id.clone(),
            partition_id: msgs[0].partition_id.clone(),
            consumer_group: msgs[0].consumer_group.clone(),
            trace_names: Some(vec![name.clone()]),
            event_type: "step".into(),
            data: serde_json::json!({ "step": "named" }),
        })
        .await
        .expect("recording the trace failed");

    let listing_params = [("limit", "200".to_string())];
    let mut names = serde_json::Value::Null;
    let mut mine = None;
    for _ in 0..20 {
        names = q
            .admin()
            .trace_names(&listing_params)
            .await
            .expect("trace names failed");
        mine = names
            .get("trace_names")
            .and_then(|v| v.as_array())
            .and_then(|rows| {
                rows.iter()
                    .find(|r| r.get("trace_name").and_then(|v| v.as_str()) == Some(name.as_str()))
                    .cloned()
            });
        if mine.is_some() {
            break;
        }
        sleep_ms(250).await;
    }
    for key in ["trace_names", "total", "pagination"] {
        assert!(names.get(key).is_some(), "the name listing lost `{key}`");
    }
    let entry = mine.unwrap_or_else(|| panic!("the recorded name never appeared: {names}"));
    for key in ["trace_count", "message_count", "last_seen"] {
        assert!(entry.get(key).is_some(), "name entry lost `{key}`: {entry}");
    }

    let by_name = q
        .admin()
        .traces_by_name(&name, &[("limit", "50".to_string())])
        .await
        .expect("traces by name failed");
    for key in ["traces", "total", "pagination"] {
        assert!(by_name.get(key).is_some(), "the by-name reply lost `{key}`");
    }
    let traces = by_name
        .get("traces")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("traces is not an array: {by_name}"));
    let trace = traces
        .iter()
        .find(|t| {
            t.get("transaction_id").and_then(|v| v.as_str())
                == Some(msgs[0].transaction_id.as_str())
        })
        .unwrap_or_else(|| panic!("the trace is not listed under its own name: {by_name}"));
    assert_eq!(
        trace.get("event_type").and_then(|v| v.as_str()),
        Some("step"),
        "the event type did not survive the round trip: {trace}"
    );
    assert_eq!(
        trace.get("data").and_then(|d| d.get("step")),
        Some(&serde_json::json!("named")),
        "the trace payload did not survive the round trip: {trace}"
    );
    assert_eq!(
        trace.get("partition_id").and_then(|v| v.as_str()),
        Some(msgs[0].partition_id.as_str())
    );

    drop_queue(&q, &queue).await;
}

// ------------------------------------------------------- observability

// These are the endpoints an operator's dashboard is built on, and every one of
// them returns free-form JSON the client does not type. `is_ok()` alone would
// stay green through a renamed key, an empty object, or a body that lost its
// series — so each one is checked for the keys it actually emits.
#[tokio::test]
async fn the_observability_endpoints_answer() {
    let q = broker!();
    let admin = q.admin();

    let health = admin.health().await.unwrap();
    assert_eq!(
        health.get("status").and_then(|v| v.as_str()),
        Some("healthy")
    );

    let overview = admin.overview().await.expect("overview failed");
    for key in ["queues", "partitions", "namespaces", "tasks"] {
        assert!(
            overview.get(key).and_then(|v| v.as_i64()).is_some(),
            "overview lost the `{key}` count: {overview}"
        );
    }
    let messages = overview
        .get("messages")
        .unwrap_or_else(|| panic!("overview lost its messages block: {overview}"));
    for key in ["total", "pending", "processing", "completed", "deadLetter"] {
        assert!(
            messages.get(key).is_some(),
            "overview messages lost `{key}`: {messages}"
        );
    }
    assert!(
        overview
            .get("lag")
            .and_then(|l| l.get("time"))
            .and_then(|t| t.get("avg"))
            .is_some(),
        "overview lost its lag block: {overview}"
    );

    let queues = admin.list_queues(&[]).await.expect("queue list failed");
    assert!(
        queues.get("queues").and_then(|v| v.as_array()).is_some(),
        "the queue list is not an array under `queues`: {queues}"
    );
    // No `pagination` block here: the SPs that paginate are the DLQ
    // (010_log_admin.sql:630), traces (017_traces.sql) and stats
    // (018_stats.sql). The queue list returns the whole set, so what is worth
    // pinning is the shape of a row.
    let row = queues["queues"]
        .as_array()
        .and_then(|a| a.first())
        .cloned()
        .expect("the queue list came back empty in a suite that just created queues");
    for key in [
        "id",
        "name",
        "namespace",
        "task",
        "partitions",
        "messages",
        "segments",
        "retainedBytes",
        "createdAt",
    ] {
        assert!(
            row.get(key).is_some(),
            "a queue row lost the key `{key}`: {row}"
        );
    }
    assert!(
        ["pending", "processing", "total"]
            .iter()
            .all(|k| row["messages"].get(k).is_some()),
        "a queue row's message counters lost a field: {row}"
    );

    let namespaces = admin.namespaces().await.expect("namespaces failed");
    assert!(
        namespaces
            .get("namespaces")
            .and_then(|v| v.as_array())
            .is_some(),
        "namespaces is not an array under `namespaces`: {namespaces}"
    );

    let tasks = admin.tasks().await.expect("tasks failed");
    assert!(
        tasks.get("tasks").and_then(|v| v.as_array()).is_some(),
        "tasks is not an array under `tasks`: {tasks}"
    );

    let status = admin.status(&[]).await.expect("status failed");
    assert!(
        status
            .get("timeRange")
            .and_then(|r| r.get("from"))
            .and_then(|v| v.as_str())
            .is_some(),
        "status lost its time range: {status}"
    );
    for key in ["bucketMinutes", "pointCount", "statsAge"] {
        assert!(
            status.get(key).and_then(|v| v.as_i64()).is_some(),
            "status lost `{key}`: {status}"
        );
    }
    for key in ["throughput", "workers"] {
        assert!(
            status.get(key).and_then(|v| v.as_array()).is_some(),
            "status `{key}` is not a series: {status}"
        );
    }
    // These five are built from optional aggregates, so they can be null on an
    // idle broker — but the KEY has to be there, or the dashboard reads a
    // missing panel as a zeroed one.
    for key in ["queues", "messages", "leases", "deadLetterQueue", "errors"] {
        assert!(status.get(key).is_some(), "status lost `{key}`: {status}");
    }

    // `Admin::metrics()` returns the body of `/metrics` as text, and that route
    // is JSON — the Prometheus exposition lives on `/metrics/prometheus`
    // (server/src/main.rs:780), which this client has no method for at all.
    // Asserting the exposition here would be asserting the wrong route.
    let metrics = admin.metrics().await.unwrap();
    assert!(!metrics.is_empty(), "the metrics endpoint returned nothing");
    let parsed: serde_json::Value = serde_json::from_str(&metrics)
        .unwrap_or_else(|e| panic!("/metrics stopped being JSON ({e}): {metrics}"));
    for key in [
        "cpu", "memory", "database", "messages", "requests", "uptime",
    ] {
        assert!(
            parsed.get(key).is_some(),
            "the metrics body lost `{key}`: {parsed}"
        );
    }
    assert!(
        parsed["database"].get("poolSize").is_some(),
        "the metrics body lost the pool block, which is what says the broker is \
         connected at all: {parsed}"
    );
}

// The analytics quartet had no call site at all. They are pure read models over
// pg_stat_* and the metrics tables, so the only way they break is a shape change
// — and each one feeds a chart that would silently flatline.
#[tokio::test]
async fn the_analytics_endpoints_answer_with_their_series() {
    let q = broker!();
    let admin = q.admin();

    let analytics = admin.analytics(&[]).await.expect("analytics failed");
    assert!(
        analytics
            .get("dataPoints")
            .and_then(|v| v.as_array())
            .is_some(),
        "analytics lost its series: {analytics}"
    );
    for key in ["interval", "from", "to"] {
        assert!(
            analytics.get(key).and_then(|v| v.as_str()).is_some(),
            "analytics lost `{key}`: {analytics}"
        );
    }

    let system = admin
        .system_metrics(&[])
        .await
        .expect("system metrics failed");
    for key in ["timeRange", "replicas", "bucketMinutes", "pointCount"] {
        assert!(
            system.get(key).is_some(),
            "system metrics lost `{key}`: {system}"
        );
    }
    assert!(
        system.get("replicas").and_then(|v| v.as_array()).is_some(),
        "replicas is not an array: {system}"
    );

    let workers = admin
        .worker_metrics(&[])
        .await
        .expect("worker metrics failed");
    for key in ["timeRange", "bucketMinutes", "pointCount", "summary"] {
        assert!(
            workers.get(key).is_some(),
            "worker metrics lost `{key}`: {workers}"
        );
    }
    for key in ["timeSeries", "workers", "queues"] {
        assert!(
            workers.get(key).and_then(|v| v.as_array()).is_some(),
            "worker metrics `{key}` is not a series: {workers}"
        );
    }

    let pg = admin.postgres_stats().await.expect("postgres stats failed");
    for key in ["timestamp", "database", "databaseCache", "cacheSummary"] {
        assert!(pg.get(key).is_some(), "postgres stats lost `{key}`: {pg}");
    }
    for key in ["tableCache", "indexCache", "activeQueries", "tableSizes"] {
        assert!(
            pg.get(key).and_then(|v| v.as_array()).is_some(),
            "postgres stats `{key}` is not an array: {pg}"
        );
    }
}

// --------------------------------------------------------------- watermark

#[tokio::test]
async fn a_leased_backlog_is_not_stranded_by_another_workers_empty_polls() {
    let q = broker!();
    let queue = unique("watermark");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(3),
            ..Default::default()
        },
    )
    .await;
    let group = format!("{queue}-cg");

    q.queue(&queue)
        .push_many((0..3).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();

    // Worker A claims the lane and holds it.
    let held = pop_retry(&q, &queue, Some(&group), 3, 25).await;
    assert_eq!(held.len(), 3);

    // Worker B polls the same group repeatedly and gets nothing — correct,
    // A holds the claim. What must NOT happen is B's empty polls advancing a
    // watermark that then strands the backlog after A's lease lapses.
    for _ in 0..6 {
        let empty = q
            .queue(&queue)
            .group(&group)
            .batch(10)
            .wait(false)
            .pop()
            .await
            .unwrap();
        assert!(empty.is_empty(), "two workers claimed the same lane");
        sleep_ms(200).await;
    }

    // A walks away; the lease lapses and the backlog must come back.
    sleep_ms(4000).await;
    let recovered = pop_retry(&q, &queue, Some(&group), 10, 30).await;
    assert_eq!(
        recovered.len(),
        3,
        "backlog stranded after the lease lapsed — empty polls moved the watermark"
    );

    drop_queue(&q, &queue).await;
}

// ------------------------------------------------------------- bootstrap

#[tokio::test]
async fn a_new_group_seeded_from_a_timestamp_sees_only_later_messages() {
    let q = broker!();
    let queue = unique("bootstrap-ts");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push(serde_json::json!({ "when": "before" }))
        .await
        .unwrap();
    // Make sure it is durable and visible before we take the cut-off.
    let warm = pop_retry(&q, &queue, Some("g-warm-ts"), 10, 25).await;
    assert_eq!(warm.len(), 1);

    sleep_ms(1100).await;
    let cutoff = iso_now();
    sleep_ms(1100).await;

    q.queue(&queue)
        .push(serde_json::json!({ "when": "after" }))
        .await
        .unwrap();

    let mut got = Vec::new();
    for _ in 0..25 {
        got = q
            .queue(&queue)
            .group("g-from-ts")
            .batch(10)
            .wait(false)
            .subscription_mode(queen_mq::SubscriptionMode::New)
            .subscription_from(cutoff.clone())
            .pop()
            .await
            .unwrap();
        if !got.is_empty() {
            break;
        }
        sleep_ms(150).await;
    }

    assert_eq!(got.len(), 1, "expected only the post-cutoff message");
    assert_eq!(got[0].data["when"], "after");

    drop_queue(&q, &queue).await;
}

/// An RFC3339 timestamp for "now", without pulling in a date library.
fn iso_now() -> String {
    let out = std::process::Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .expect("date is required for the timestamp tests");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

// ------------------------------------------------------------- resilience

#[tokio::test]
async fn an_unreachable_backend_fails_over_to_a_healthy_one() {
    let Ok(url) = std::env::var("QUEEN_TEST_URL") else {
        skipped("an_unreachable_backend_fails_over_to_a_healthy_one");
        return;
    };

    // A dead backend first, the real one second. Failover must find the live
    // one rather than surfacing the connection refusal.
    let q = queen_mq::Queen::connect(queen_mq::Config::urls([
        "http://127.0.0.1:1".to_string(),
        url,
    ]))
    .unwrap();

    let queue = unique("failover");
    create_queue(&q, &queue, QueueOptions::default()).await;
    let res = q
        .queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();
    assert_eq!(res.len(), 1);

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_completely_unreachable_broker_reports_an_error() {
    // The counterpart to the failover test: with nowhere to go, the client must
    // surface the failure instead of hanging or pretending success.
    let q = queen_mq::Queen::connect(
        queen_mq::Config::new("http://127.0.0.1:1").timeout(Duration::from_millis(500)),
    )
    .unwrap();
    let err = q
        .queue("nope")
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap_err();
    assert!(
        err.is_retryable(),
        "a refused connection should read as retryable: {err}"
    );
}
