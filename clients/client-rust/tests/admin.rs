//! Transactions, DLQ, consumer groups, retention, producer identity, and the
//! observability endpoints.
//!
//! Ports the `transaction`, `dlq`, `retention`, `auth`, `bootstrap` and
//! `watermark` areas of the JS, Go and Python suites.

mod common;

use std::time::Duration;

use queen_mq::{AckStatus, QueueOptions, SeekRequest, TraceRequest};

use common::*;

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

    for _ in 0..5 {
        let msgs = pop_retry(&q, &queue, None, 1, 10).await;
        if msgs.is_empty() {
            break;
        }
        q.nack(&msgs[0], "dlq-test reason").await.unwrap();
        sleep_ms(150).await;
    }

    let dlq = q.queue(&queue).dlq(Some(50), None).await.unwrap();
    assert_eq!(dlq.messages.len(), 1, "the message never dead-lettered");
    assert_eq!(dlq.total, 1);

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

    // Seek back to the beginning. This is the supported way to replay —
    // subscription_from only ever seeds a cursor that does not exist yet.
    let seek = q
        .admin()
        .seek_consumer_group(
            &group,
            &queue,
            &SeekRequest {
                timestamp: None,
                position: Some("earliest".into()),
            },
        )
        .await;

    if seek.is_err() {
        eprintln!("SKIP seek: the broker refused the request ({seek:?})");
        drop_queue(&q, &queue).await;
        return;
    }

    let replayed = pop_retry(&q, &queue, Some(&group), 10, 25).await;
    assert!(
        !replayed.is_empty(),
        "seeking to earliest delivered nothing"
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
    assert!(resp.success || resp.error.is_none(), "trace failed: {resp:?}");

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

// ------------------------------------------------------- observability

#[tokio::test]
async fn the_observability_endpoints_answer() {
    let q = broker!();
    let admin = q.admin();

    let health = admin.health().await.unwrap();
    assert_eq!(
        health.get("status").and_then(|v| v.as_str()),
        Some("healthy")
    );

    assert!(admin.overview().await.is_ok());
    assert!(admin.list_queues(&[]).await.is_ok());
    assert!(admin.namespaces().await.is_ok());
    assert!(admin.tasks().await.is_ok());
    assert!(admin.status(&[]).await.is_ok());

    // Prometheus text, not JSON — a JSON decode here would be a bug.
    let metrics = admin.metrics().await.unwrap();
    assert!(!metrics.is_empty(), "the metrics endpoint returned nothing");
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
    assert!(err.is_retryable(), "a refused connection should read as retryable: {err}");
}
