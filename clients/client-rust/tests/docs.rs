//! The tests behind the published examples.
//!
//! Every marked region in this file is rendered on queenmq.com through
//! webdoc/scripts/gen-snippets.mjs: real queue names, a partition key that
//! means something, and no harness noise inside a marked region. Assertions
//! stay outside the markers. After editing a region, regenerate the partials
//! with `pnpm --dir webdoc gen` or the docs CI check fails on drift.
//!
//! One test on purpose: the stories share the `orders` queue and cargo runs
//! tests concurrently, so a single sequential test keeps the lifecycle sane.
//! The queues are dropped at the end (and defensively at the start) instead
//! of using `unique()` names, because these names are published.

mod common;

use common::*;
use queen_mq::{PushStatus, StopReason, SubscriptionMode};

const DOCS_QUEUES: [&str; 3] = ["orders", "payments", "invoices"];

#[tokio::test]
async fn docs_examples() {
    let q = broker!();

    // Normally a no-op: the previous run dropped these at the end. After a
    // crashed run it actually deletes, and the recreate below then rides the
    // known 30 s stale-partition window, which only the dedup story can feel.
    for queue in DOCS_QUEUES {
        drop_queue(&q, queue).await;
    }

    // docs:start(rust-push)
    let res = q
        .queue("orders")
        .partition("customer-42")
        .push(serde_json::json!({ "orderId": 9137, "amount": 99.5 }))
        .await
        .unwrap();
    // docs:end
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].status, PushStatus::Queued);

    // docs:start(rust-consume)
    let summary = q
        .queue("orders")
        .group("billing")
        .subscription_mode(SubscriptionMode::All)
        .limit(1)
        .consume(|message| async move {
            println!("{}", message.data);
            Ok::<_, std::convert::Infallible>(())
        })
        .await
        .unwrap();
    // docs:end
    assert_eq!(summary.processed, 1);
    assert_eq!(summary.acked, 1);
    assert_eq!(summary.stopped_by, StopReason::Limit);

    // The consume loop acked, so billing's cursor moved past the order.
    let drained = q
        .queue("orders")
        .group("billing")
        .batch(1)
        .wait(false)
        .pop()
        .await
        .unwrap();
    assert!(drained.is_empty(), "billing cursor did not advance");

    // A raw pop on another cursor still sees the message: groups are fan-out.
    // docs:start(rust-pop)
    let messages = q.queue("orders").batch(10).wait(true).pop().await.unwrap();
    // docs:end
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].data["orderId"], 9137);

    // docs:start(rust-push-dedup)
    let paid = queen_mq::PushItem::new(
        "payments",
        serde_json::json!({ "orderId": 9137, "amount": 99.5 }),
    )
    .partition("customer-42")
    .transaction_id("order-9137-paid");

    let first = q
        .queue("payments")
        .push_items(vec![paid.clone()])
        .await
        .unwrap();
    let retry = q.queue("payments").push_items(vec![paid]).await.unwrap();
    // retry[0].status is Duplicate: the second push wrote nothing
    // and answers with the first message's id.
    // docs:end
    assert_eq!(first[0].status, PushStatus::Queued);
    assert_eq!(retry[0].status, PushStatus::Duplicate);
    assert_eq!(retry[0].message_id, first[0].message_id);

    // Transactional handoff: ack the order and push its invoice in one commit.
    let seeded = q
        .queue("orders")
        .partition("customer-77")
        .push(serde_json::json!({ "orderId": 4102, "amount": 18 }))
        .await
        .unwrap();
    assert_eq!(seeded[0].status, PushStatus::Queued);

    // docs:start(rust-transaction)
    let messages = q
        .queue("orders")
        .group("invoicing")
        .subscription_mode(SubscriptionMode::All)
        .batch(1)
        .wait(true)
        .pop()
        .await
        .unwrap();

    let resp = q
        .transaction()
        .ack(&messages[0])
        .push(
            "invoices".to_string(),
            serde_json::json!({ "orderId": messages[0].data["orderId"], "invoiced": true }),
        )
        .unwrap()
        .commit()
        .await
        .unwrap();
    // docs:end
    assert!(resp.success);

    let invoiced = q.queue("invoices").batch(1).wait(true).pop().await.unwrap();
    assert_eq!(invoiced.len(), 1);
    assert_eq!(invoiced[0].data["orderId"], messages[0].data["orderId"]);
    assert_eq!(invoiced[0].data["invoiced"], true);

    for queue in DOCS_QUEUES {
        drop_queue(&q, queue).await;
    }
}
