//! The embedded facade on the single-node state machine, end to end. The three
//! `docs:start` regions are the snippets the docs site publishes
//! (webdoc/scripts/gen-snippets.mjs).

use queen::protocol as qp;
use queen::{Broker, BrokerConfig};

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn queue_mode_params() -> qp::PopParams {
    qp::PopParams {
        batch: Some(10),
        ..Default::default()
    }
}

fn group_all_params(group: &str) -> qp::PopParams {
    qp::PopParams {
        batch: Some(10),
        consumer_group: Some(group.to_string()),
        subscription_mode: Some(qp::SubscriptionMode::All),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedded_broker_runs_the_message_path_admin_dlq_and_observability() {
    let dir = std::env::temp_dir().join(unique("queen-embedded-raft"));
    let queue = unique("queue");
    // The host's disk usage is not under test: the gate closes only on a full
    // disk (the default 85% refused the first push on a 91%-full dev machine).
    std::env::set_var("QUEEN_RAFT_DISK_HIGH_PCT", "100");
    std::env::set_var("QUEEN_RAFT_DISK_LOW_PCT", "100");
    // docs:start(embedded-start)
    let broker = Broker::start(BrokerConfig::new().raft(&dir))
        .await
        .expect("broker start");
    // docs:end

    // ------------------------------------ push, dedup, pop, transaction
    let q = unique("emb-q");
    let q2 = unique("emb-q2");
    let group = "emb-workers";
    broker
        .configure(
            &qp::ConfigureRequest::new(q.clone()).options(qp::QueueOptions {
                dedup_window_seconds: Some(300),
                ..Default::default()
            }),
        )
        .await
        .expect("configure q");
    let txn_id = unique("emb-txn");
    // docs:start(embedded-push)
    let first = broker
        .push(vec![
            qp::PushItem::new(q.clone(), serde_json::json!({"n": 1}))
                .transaction_id(txn_id.clone()),
            qp::PushItem::new(q.clone(), serde_json::json!({"n": 2})),
            qp::PushItem::new(q.clone(), serde_json::json!({"n": 3})),
        ])
        .await
        .expect("push");
    // docs:end
    assert_eq!(first.len(), 3);
    assert!(
        first.iter().all(|r| r.status == qp::PushStatus::Queued),
        "all first pushes queued: {first:?}"
    );
    // Same explicit transactionId again -> dedup.
    let dup = broker
        .push(vec![qp::PushItem::new(
            q.clone(),
            serde_json::json!({"n": 1}),
        )
        .transaction_id(txn_id.clone())])
        .await
        .expect("dup push");
    assert_eq!(dup[0].status, qp::PushStatus::Duplicate, "{dup:?}");

    let popped = broker.pop(&q, &group_all_params(group)).await.expect("pop");
    assert_eq!(popped.messages.len(), 3, "{popped:?}");
    // Handoff: ack message[0] and push its successor to q2, guarded by the lease.
    let m0 = &popped.messages[0];
    // docs:start(embedded-transaction)
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
    // docs:end
    assert_eq!(txn.results.len(), 2, "{txn:?}");
    let handed = broker
        .pop(&q2, &group_all_params(group))
        .await
        .expect("pop q2");
    assert_eq!(handed.messages.len(), 1, "{handed:?}");
    assert_eq!(handed.messages[0].data, serde_json::json!({"stage": 2}));

    let configured = broker
        .configure(&qp::ConfigureRequest::new(queue.clone()))
        .await
        .expect("configure");
    assert!(configured.configured, "{configured:?}");

    let pushed = broker
        .push(vec![qp::PushItem::new(
            queue.clone(),
            serde_json::json!({"phase": 2}),
        )])
        .await
        .expect("push");
    assert_eq!(pushed[0].status, qp::PushStatus::Queued, "{pushed:?}");

    let popped = broker.pop(&queue, &queue_mode_params()).await.expect("pop");
    assert_eq!(popped.messages.len(), 1, "{popped:?}");
    let message = &popped.messages[0];
    let dlq_ack = broker
        .ack(&qp::AckRequest {
            transaction_id: message.transaction_id.clone(),
            partition_id: message.partition_id.clone(),
            status: qp::AckStatus::Dlq,
            consumer_group: Some(message.consumer_group.clone()),
            lease_id: Some(message.lease_id.clone()),
            error: Some("embedded poison".into()),
        })
        .await
        .expect("dlq ack");
    assert!(dlq_ack.success && dlq_ack.dlq, "{dlq_ack:?}");

    let listed = broker
        .dlq(&qp::DlqParams {
            queue: Some(queue.clone()),
            ..Default::default()
        })
        .await
        .expect("dlq list");
    assert_eq!(listed.messages.len(), 1, "{listed:?}");
    let dead = &listed.messages[0];
    let replay = broker
        .retry_message(
            dead.partition_id.as_deref().expect("partition id"),
            dead.transaction_id.as_deref().expect("transaction id"),
        )
        .await
        .expect("dlq replay");
    assert_eq!(replay["success"], true, "{replay}");

    let replayed = broker
        .pop(&queue, &queue_mode_params())
        .await
        .expect("pop replay");
    assert_eq!(replayed.messages.len(), 1, "{replayed:?}");
    assert_eq!(replayed.messages[0].data, serde_json::json!({"phase": 2}));

    // A named group's explicit seed policy must cross the embedded handler
    // seam. Phase 1 used to force every named group to `new`, silently hiding
    // backlog even when the client asked for `all`.
    let grouped_queue = unique("grouped");
    broker
        .push(vec![qp::PushItem::new(
            grouped_queue.clone(),
            serde_json::json!({"backlog": true}),
        )])
        .await
        .expect("grouped push");
    let grouped = broker
        .pop(&grouped_queue, &group_all_params("all-readers"))
        .await
        .expect("grouped all pop");
    assert_eq!(grouped.messages.len(), 1, "{grouped:?}");
    assert_eq!(
        grouped.messages[0].data,
        serde_json::json!({"backlog": true})
    );

    let metrics = broker.metrics().await.expect("metrics");
    assert_eq!(metrics["engine"], "raft", "{metrics}");
    assert!(metrics.get("database").is_none(), "{metrics}");
    let health = broker.health().await.expect("health");
    assert_eq!(health["status"], "healthy", "{health}");
    assert!(broker
        .prometheus()
        .await
        .expect("prometheus")
        .contains("queen_raft_"));

    let deleted = broker.delete_queue(&queue).await.expect("delete queue");
    assert!(deleted.existed && deleted.deleted, "{deleted:?}");
    let absent = broker.delete_queue(&queue).await.expect("delete again");
    assert!(!absent.existed && !absent.deleted, "{absent:?}");
    let grouped_deleted = broker
        .delete_queue(&grouped_queue)
        .await
        .expect("delete grouped queue");
    assert!(grouped_deleted.deleted, "{grouped_deleted:?}");

    broker.shutdown().await;
    drop(broker);
    let _ = std::fs::remove_dir_all(dir);
}
