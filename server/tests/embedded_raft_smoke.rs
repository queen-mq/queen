//! WP-2.10: the embedded facade on the single-node RSM, with no Postgres.

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
async fn embedded_raft_runs_admin_message_dlq_and_observability_without_postgres() {
    let dir = std::env::temp_dir().join(unique("queen-embedded-raft"));
    let queue = unique("queue");
    // The host's disk usage is not under test: the gate closes only on a full
    // disk (the default 85% refused the first push on a 91%-full dev machine).
    let broker = Broker::start(BrokerConfig::new().raft(&dir).raft_disk_pct(100.0, 100.0))
        .await
        .expect("embedded raft boot");

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
    // The legacy pool block stays for dashboard compatibility, with no SQL
    // connections behind it.
    assert_eq!(metrics["database"]["poolSize"], 0, "{metrics}");
    assert_eq!(metrics["database"]["idleConnections"], 0, "{metrics}");
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

    assert_eq!(broker.shutdown().await, 0);
    drop(broker);
    let _ = std::fs::remove_dir_all(dir);
}
