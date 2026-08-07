//! End-to-end smoke test for the embedded library facade (`queen::Broker`).
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`:
//!
//! ```bash
//! docker run --rm -d --name queen-embedded-pg -e POSTGRES_PASSWORD=postgres -p 5464:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5464 cargo test --test embedded_smoke -- --ignored --nocapture
//! ```
//!
//! One test function on purpose: the admission arbiter is process-global, so
//! the whole flow drives a single Broker instance.

use queen::protocol as qp;
use queen::{Broker, BrokerConfig, Error, StartError};

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

fn ack_for(m: &qp::Message, group: &str) -> qp::AckRequest {
    qp::AckRequest {
        transaction_id: m.transaction_id.clone(),
        partition_id: m.partition_id.clone(),
        status: qp::AckStatus::Completed,
        consumer_group: Some(group.to_string()),
        lease_id: Some(m.lease_id.clone()),
        error: None,
    }
}

fn group_params(group: &str) -> qp::PopParams {
    qp::PopParams {
        batch: Some(10),
        consumer_group: Some(group.to_string()),
        subscription_mode: Some(qp::SubscriptionMode::All),
        ..Default::default()
    }
}

/// Pop until `want` messages arrive or ~3s elapse — nack redelivery is
/// visibility-timed, not instantaneous.
async fn pop_until(
    broker: &Broker,
    queue: &str,
    params: &qp::PopParams,
    want: usize,
) -> Vec<qp::Message> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    let mut got: Vec<qp::Message> = Vec::new();
    while got.len() < want && std::time::Instant::now() < deadline {
        got.extend(broker.pop(queue, params).await.expect("pop_until").messages);
        if got.len() < want {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
    got
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn embedded_end_to_end() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port) for the embedded smoke test");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // Inflate the parked pop's re-poll floor above the long-poll assertion
    // bound, so the wake assertion below can only pass via the in-process
    // notifier (a broken wake would sit until the first 6s re-poll). Safe:
    // this binary runs one test, and every other pop here is wait=false.
    std::env::set_var("POP_WAIT_INITIAL_INTERVAL_MS", "6000");
    std::env::set_var("POP_WAIT_MAX_INTERVAL_MS", "6000");

    // A malformed boolean env var makes the BINARY exit; the library must
    // refuse to start instead. Checked first, serially, because env is
    // process-global.
    std::env::set_var("QUEEN_HOTLIST", "not-a-bool");
    let bad_start = Broker::start(BrokerConfig::new()).await;
    assert!(
        matches!(&bad_start, Err(StartError::Config(_))),
        "malformed boolean env must be StartError::Config, got {:?}",
        bad_start.err().map(|e| e.to_string())
    );
    std::env::remove_var("QUEEN_HOTLIST");

    let broker = Broker::start(
        BrokerConfig::new()
            .pg(host, port, "postgres", "postgres", "postgres")
            .pool_size(12),
    )
    .await
    .expect("broker start");

    let q = unique("emb-q");
    let q2 = unique("emb-q2");
    let group = "emb-workers";

    // -------------------------------------------------------- configure
    let conf = broker
        .configure(
            &qp::ConfigureRequest::new(q.clone()).options(qp::QueueOptions {
                dedup_window_seconds: Some(300),
                ..Default::default()
            }),
        )
        .await
        .expect("configure q");
    assert!(conf.configured, "configure must report configured: {conf:?}");
    broker
        .configure(&qp::ConfigureRequest::new(q2.clone()))
        .await
        .expect("configure q2");

    // ------------------------------------------------------------- push
    let txn_id = unique("emb-txn");
    let first = broker
        .push(vec![
            qp::PushItem::new(q.clone(), serde_json::json!({"n": 1}))
                .transaction_id(txn_id.clone()),
            qp::PushItem::new(q.clone(), serde_json::json!({"n": 2})),
            qp::PushItem::new(q.clone(), serde_json::json!({"n": 3})),
        ])
        .await
        .expect("push");
    assert_eq!(first.len(), 3);
    assert!(
        first.iter().all(|r| r.status == qp::PushStatus::Queued),
        "all first pushes queued: {first:?}"
    );

    // Same explicit transactionId again -> dedup, same pre-existing message id.
    let dup = broker
        .push(vec![qp::PushItem::new(q.clone(), serde_json::json!({"n": 1}))
            .transaction_id(txn_id.clone())])
        .await
        .expect("dup push");
    assert_eq!(dup[0].status, qp::PushStatus::Duplicate, "{dup:?}");
    assert_eq!(dup[0].message_id, first[0].message_id, "{dup:?}");

    // -------------------------------------------------------------- pop
    let params = group_params(group);
    let popped = broker.pop(&q, &params).await.expect("pop");
    assert_eq!(popped.messages.len(), 3, "{popped:?}");
    assert!(popped.messages.iter().all(|m| m.is_leased()), "{popped:?}");
    assert_eq!(popped.consumer_group, group);

    // ---------------------------------------------------- lease renewal
    let lease = popped.messages[0].lease_id.clone();
    let renewed = broker
        .renew_lease(&lease, Some(120))
        .await
        .expect("renew lease");
    assert!(renewed.success && renewed.renewed >= 1, "{renewed:?}");
    assert!(renewed.expires_at().is_some(), "{renewed:?}");
    let bogus = broker
        .renew_lease("00000000-0000-0000-0000-000000000000", Some(30))
        .await
        .expect("bogus renew call");
    assert!(
        !(bogus.success && bogus.renewed > 0),
        "bogus lease must not renew: {bogus:?}"
    );

    // ------------------------------------------------------ transaction
    // Handoff: ack message[0] and push its successor to q2, guarded by the lease.
    let m0 = &popped.messages[0];
    let txn = broker
        .transaction(
            &qp::TransactionRequest::new(vec![
                qp::TxnOperation::Ack(qp::TxnAckOperation {
                    transaction_id: m0.transaction_id.clone(),
                    partition_id: m0.partition_id.clone(),
                    status: qp::AckStatus::Completed,
                    consumer_group: Some(group.to_string()),
                    lease_id: Some(m0.lease_id.clone()),
                    error: None,
                }),
                qp::TxnOperation::Push {
                    items: vec![qp::TxnPushItem::new(
                        q2.clone(),
                        serde_json::json!({"stage": 2}),
                    )],
                },
            ])
            .with_required_leases([m0.lease_id.clone()]),
        )
        .await
        .expect("transaction");
    assert!(txn.success, "transaction must commit: {txn:?}");
    assert_eq!(txn.results.len(), 2, "{txn:?}");
    assert!(txn.results.iter().all(|r| r.success), "{txn:?}");

    // The transactional push must be poppable from q2.
    let staged = broker.pop(&q2, &group_params(group)).await.expect("pop q2");
    assert_eq!(staged.messages.len(), 1, "{staged:?}");
    assert_eq!(staged.messages[0].data, serde_json::json!({"stage": 2}));
    let a = broker
        .ack(&ack_for(&staged.messages[0], group))
        .await
        .expect("ack staged");
    assert!(a.success, "{a:?}");

    // ------------------------------------- batch ack with a bogus item mixed in
    let remaining: Vec<&qp::Message> = popped.messages[1..].iter().collect();
    let mut acknowledgments: Vec<qp::AckBatchItem> = remaining
        .iter()
        .map(|m| qp::AckBatchItem {
            transaction_id: m.transaction_id.clone(),
            partition_id: m.partition_id.clone(),
            status: qp::AckStatus::Completed,
            lease_id: Some(m.lease_id.clone()),
            error: None,
        })
        .collect();
    acknowledgments.push(qp::AckBatchItem {
        transaction_id: unique("emb-bogus"),
        partition_id: remaining[0].partition_id.clone(),
        status: qp::AckStatus::Completed,
        lease_id: Some(remaining[0].lease_id.clone()),
        error: None,
    });
    let acks = broker
        .ack_batch(&qp::AckBatchRequest {
            acknowledgments,
            consumer_group: Some(group.to_string()),
        })
        .await
        .expect("ack batch");
    assert_eq!(acks.len(), 3, "{acks:?}");
    assert!(acks[0].success && acks[1].success, "{acks:?}");
    assert!(!acks[2].success, "bogus txn must not ack: {acks:?}");

    // Everything acked -> queue drained for this group.
    let drained = broker.pop(&q, &params).await.expect("pop drained");
    assert!(drained.messages.is_empty(), "{drained:?}");

    // ------------------------------------------------ nack -> redelivery
    broker
        .push(vec![qp::PushItem::new(q.clone(), serde_json::json!({"retry": true}))])
        .await
        .expect("push retryable");
    let claimed = pop_until(&broker, &q, &params, 1).await;
    assert_eq!(claimed.len(), 1);
    let nack = broker
        .ack(&qp::AckRequest {
            status: qp::AckStatus::Retry,
            error: Some("forced retry".into()),
            ..ack_for(&claimed[0], group)
        })
        .await
        .expect("nack");
    assert!(nack.success, "{nack:?}");
    let redelivered = pop_until(&broker, &q, &params, 1).await;
    assert_eq!(redelivered.len(), 1, "nacked message must be redelivered");
    assert_eq!(redelivered[0].transaction_id, claimed[0].transaction_id);
    broker
        .ack(&ack_for(&redelivered[0], group))
        .await
        .expect("ack redelivered");

    // ----------------------------------------------- pop_partition + autoAck
    broker
        .push(vec![qp::PushItem::new(q.clone(), serde_json::json!({"aa": 1}))])
        .await
        .expect("push autoack");
    let auto = broker
        .pop_partition(
            &q,
            "Default",
            &qp::PopParams {
                auto_ack: Some(true),
                ..group_params(group)
            },
        )
        .await
        .expect("pop_partition autoAck");
    assert_eq!(auto.messages.len(), 1, "{auto:?}");
    assert!(
        !auto.messages[0].is_leased(),
        "autoAck must take no lease: {auto:?}"
    );
    let after_auto = broker.pop(&q, &params).await.expect("pop after autoAck");
    assert!(
        after_auto.messages.is_empty(),
        "autoAck already advanced the cursor: {after_auto:?}"
    );

    // ------------------------------------------------------ discovery pop
    let ns = unique("emb-ns");
    let qd = unique("emb-qd");
    broker
        .configure(&qp::ConfigureRequest {
            queue: qd.clone(),
            namespace: Some(ns.clone()),
            task: None,
            options: qp::QueueOptions::default(),
        })
        .await
        .expect("configure namespaced");
    broker
        .push(vec![qp::PushItem::new(qd.clone(), serde_json::json!({"d": 1}))])
        .await
        .expect("push namespaced");
    let discovered = broker
        .pop_discover(&qp::PopParams {
            namespace: Some(ns.clone()),
            ..group_params(group)
        })
        .await
        .expect("pop_discover");
    assert_eq!(discovered.messages.len(), 1, "{discovered:?}");
    broker
        .ack(&ack_for(&discovered.messages[0], group))
        .await
        .expect("ack discovered");

    // Discovery without namespace/task is a 400 -> InvalidRequest.
    let bad = broker.pop_discover(&group_params(group)).await;
    assert!(
        matches!(bad, Err(Error::InvalidRequest(_))),
        "discovery without namespace/task must be InvalidRequest, got {bad:?}"
    );

    // ------------------------------------------------------- DLQ round-trip
    // AckStatus::Dlq dead-letters immediately on a DLQ-enabled queue; the
    // entry must be listable, replayable, and deletable through the facade.
    let q4 = unique("emb-q4");
    broker
        .configure(
            &qp::ConfigureRequest::new(q4.clone()).options(qp::QueueOptions {
                dead_letter_queue: Some(true),
                ..Default::default()
            }),
        )
        .await
        .expect("configure dlq queue");
    let dlq_filter = qp::DlqParams {
        queue: Some(q4.clone()),
        ..Default::default()
    };
    let poison_one = |tag: i64| {
        let broker = broker.clone();
        let q4 = q4.clone();
        let params = params.clone();
        async move {
            broker
                .push(vec![qp::PushItem::new(q4.clone(), serde_json::json!({"poison": tag}))])
                .await
                .expect("push poison");
            let claimed = pop_until(&broker, &q4, &params, 1).await;
            assert_eq!(claimed.len(), 1);
            let dlq_ack = broker
                .ack(&qp::AckRequest {
                    status: qp::AckStatus::Dlq,
                    error: Some("poison message".into()),
                    ..ack_for(&claimed[0], group)
                })
                .await
                .expect("dlq ack");
            assert!(dlq_ack.success && dlq_ack.dlq, "{dlq_ack:?}");
        }
    };

    // Entry 1: dead-letter, list, replay, consume the replay.
    poison_one(1).await;
    let dlq_list = broker.dlq(&dlq_filter).await.expect("dlq list");
    assert_eq!(dlq_list.messages.len(), 1, "{dlq_list:?}");
    let entry = &dlq_list.messages[0];
    assert_eq!(entry.error.as_deref(), Some("poison message"), "{entry:?}");
    let replay = broker
        .retry_message(
            entry.partition_id.as_deref().expect("dlq partitionId"),
            entry.transaction_id.as_deref().expect("dlq transactionId"),
        )
        .await
        .expect("retry");
    assert_eq!(replay.get("success"), Some(&serde_json::json!(true)), "{replay}");
    let replayed = pop_until(&broker, &q4, &params, 1).await;
    assert_eq!(replayed.len(), 1, "replayed message must be poppable");
    assert_eq!(replayed[0].data, serde_json::json!({"poison": 1}));
    broker
        .ack(&ack_for(&replayed[0], group))
        .await
        .expect("ack replayed");

    // Entry 2: dead-letter, then drop it from the DLQ via delete_message.
    poison_one(2).await;
    let dlq_list = broker.dlq(&dlq_filter).await.expect("dlq list 2");
    assert_eq!(dlq_list.messages.len(), 1, "{dlq_list:?}");
    let entry = &dlq_list.messages[0];
    let deleted_msg = broker
        .delete_message(
            entry.partition_id.as_deref().expect("dlq partitionId"),
            entry.transaction_id.as_deref().expect("dlq transactionId"),
        )
        .await
        .expect("delete message");
    assert_eq!(
        deleted_msg.get("success"),
        Some(&serde_json::json!(true)),
        "{deleted_msg}"
    );
    let empty_dlq = broker.dlq(&dlq_filter).await.expect("dlq after cleanup");
    assert!(empty_dlq.messages.is_empty(), "{empty_dlq:?}");

    // -------------------------------------------- long-poll in-process wake
    // A wait=true pop on an empty queue must be woken by a concurrent push —
    // with the re-poll floor inflated to 6s (env above), only the notifier
    // wake can beat the 2s bound.
    let q3 = unique("emb-q3");
    broker
        .configure(&qp::ConfigureRequest::new(q3.clone()))
        .await
        .expect("configure q3");
    let waiter = {
        let broker = broker.clone();
        let q3 = q3.clone();
        tokio::spawn(async move {
            let params = qp::PopParams {
                batch: Some(1),
                consumer_group: Some("emb-lp".to_string()),
                wait: Some(true),
                timeout_millis: Some(8_000),
                ..Default::default()
            };
            let started = std::time::Instant::now();
            let got = broker.pop(&q3, &params).await;
            (started.elapsed(), got)
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    broker
        .push(vec![qp::PushItem::new(q3.clone(), serde_json::json!({"wake": true}))])
        .await
        .expect("wake push");
    let (elapsed, got) = waiter.await.expect("waiter join");
    let got = got.expect("long-poll pop");
    assert_eq!(got.messages.len(), 1, "{got:?}");
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "long-poll must be woken by the push (re-poll floor is 6s, timeout 8s), took {elapsed:?}"
    );

    // ---------------------------------------------------------- metrics
    let metrics = broker.metrics().await.expect("metrics");
    assert!(metrics.is_object(), "{metrics}");
    let prom = broker.prometheus().await.expect("prometheus");
    assert!(prom.contains("queen"), "prometheus text: {prom:.0?}");
    let health = broker.health().await.expect("health");
    assert_eq!(
        health.get("status").and_then(|s| s.as_str()),
        Some("healthy"),
        "{health}"
    );

    // ----------------------------------------------------------- delete
    let del = broker.delete_queue(&q).await.expect("delete q");
    assert!(del.existed && del.deleted, "{del:?}");
    let del_again = broker.delete_queue(&q).await.expect("delete q again");
    assert!(!del_again.existed, "{del_again:?}");
    for queue in [&q2, &q3, &q4, &qd] {
        broker.delete_queue(queue).await.expect("cleanup delete");
    }

    assert_eq!(broker.shutdown().await, 0, "no spooled events expected");
}
