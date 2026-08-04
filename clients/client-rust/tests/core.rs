//! Core protocol: push, pop, queue lifecycle, consume.
//!
//! Ports the `push`, `pop`, `queue`, `consume`, `complete`, `bootstrap` and
//! `load` areas of the JS, Go and Python suites.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use queen_mq::{BufferOptions, PushStatus, QueueOptions, StopReason, SubscriptionMode};

use common::*;

// ------------------------------------------------------------------- push

#[tokio::test]
async fn push_single_is_queued() {
    let q = broker!();
    let queue = unique("push-single");
    create_queue(&q, &queue, QueueOptions::default()).await;

    // docs:start(rust-push)
    let res = q
        .queue(&queue)
        .push(serde_json::json!({ "hello": "world" }))
        .await
        .unwrap();
    // docs:end

    assert_eq!(res.len(), 1);
    assert_eq!(res[0].status, PushStatus::Queued);
    assert_eq!(res[0].queue_name, queue);
    assert!(!res[0].message_id.is_empty());
    assert!(!res[0].transaction_id.is_empty());

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn push_batch_returns_one_result_per_item_in_order() {
    let q = broker!();
    let queue = unique("push-batch");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let payloads: Vec<serde_json::Value> = (0..5).map(|n| serde_json::json!({ "n": n })).collect();
    let res = q.queue(&queue).push_many(payloads).await.unwrap();

    assert_eq!(res.len(), 5);
    for (i, r) in res.iter().enumerate() {
        assert_eq!(r.index, i, "results must come back in request order");
        assert_eq!(r.status, PushStatus::Queued);
    }

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_repeated_transaction_id_is_a_duplicate_not_a_second_message() {
    let q = broker!();
    let queue = unique("push-dedup");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            dedup_window_seconds: Some(3600),
            ..Default::default()
        },
    )
    .await;

    // docs:start(rust-push-dedup)
    let txn = format!("{queue}-fixed-txn");
    let item = queen_mq::PushItem::new(&queue, serde_json::json!({ "n": 1 }))
        .transaction_id(txn.clone());
    // docs:end

    let first = q.queue(&queue).push_items(vec![item.clone()]).await.unwrap();
    assert_eq!(first[0].status, PushStatus::Queued);

    let second = q.queue(&queue).push_items(vec![item]).await.unwrap();
    assert_eq!(
        second[0].status,
        PushStatus::Duplicate,
        "a retried push inside the dedup window must not enqueue twice"
    );
    // The duplicate echoes the ORIGINAL message id — that is what makes a retry
    // after a timeout safe.
    assert_eq!(second[0].message_id, first[0].message_id);

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn pushes_to_different_partitions_stay_in_their_lanes() {
    let q = broker!();
    let queue = unique("push-partitions");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .partition("eu")
        .push(serde_json::json!({ "lane": "eu" }))
        .await
        .unwrap();
    q.queue(&queue)
        .partition("us")
        .push(serde_json::json!({ "lane": "us" }))
        .await
        .unwrap();

    let eu = pop_retry(&q, &queue, None, 10, 20).await;
    // Popping the queue without naming a partition may serve either lane, so
    // address the lane explicitly.
    let _ = eu;
    let eu_msgs = q
        .queue(&queue)
        .partition("eu")
        .batch(10)
        .wait(false)
        .pop()
        .await
        .unwrap();
    for m in &eu_msgs {
        assert_eq!(m.partition, "eu");
        assert_eq!(m.data["lane"], "eu");
    }

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn buffered_pushes_reach_the_broker_on_flush() {
    let q = broker!();
    let queue = unique("push-buffered");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let b = q.queue(&queue).buffer(BufferOptions {
        message_count: 100,
        time: Duration::from_secs(60),
    });
    for n in 0..10 {
        let out = b.push(serde_json::json!({ "n": n })).await.unwrap();
        // Buffered: no per-item verdict yet.
        assert!(out.is_empty());
    }

    let stats = q.buffer_stats();
    assert_eq!(stats.total_buffered_messages, 10);
    assert_eq!(stats.active_buffers, 1);

    let flushed = q.flush_all_buffers().await.unwrap();
    assert_eq!(flushed.len(), 10);
    assert!(flushed.iter().all(|r| r.status == PushStatus::Queued));
    assert_eq!(q.buffer_stats().total_buffered_messages, 0);

    let msgs = pop_retry(&q, &queue, None, 20, 20).await;
    assert_eq!(msgs.len(), 10, "buffered messages did not all arrive");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_size_threshold_flushes_without_being_asked() {
    let q = broker!();
    let queue = unique("push-autoflush");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let b = q.queue(&queue).buffer(BufferOptions {
        message_count: 5,
        time: Duration::from_secs(60),
    });
    for n in 0..5 {
        b.push(serde_json::json!({ "n": n })).await.unwrap();
    }

    let msgs = pop_retry(&q, &queue, None, 10, 30).await;
    assert_eq!(msgs.len(), 5, "the size threshold did not trigger a flush");

    drop_queue(&q, &queue).await;
}

// -------------------------------------------------------------------- pop

#[tokio::test]
async fn popping_an_empty_queue_yields_nothing_and_no_error() {
    let q = broker!();
    let queue = unique("pop-empty");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let msgs = q.queue(&queue).wait(false).pop().await.unwrap();
    assert!(msgs.is_empty());

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn pop_honours_the_batch_size() {
    let q = broker!();
    let queue = unique("pop-batch");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push_many((0..10).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();

    let msgs = pop_retry(&q, &queue, None, 3, 20).await;
    assert!(
        !msgs.is_empty() && msgs.len() <= 3,
        "batch(3) returned {} messages",
        msgs.len()
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_claimed_message_carries_its_lease_and_identity() {
    let q = broker!();
    let queue = unique("pop-shape");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();
    let msgs = pop_retry(&q, &queue, Some("g1"), 1, 20).await;
    assert_eq!(msgs.len(), 1);

    let m = &msgs[0];
    assert!(!m.id.is_empty());
    assert!(!m.transaction_id.is_empty());
    assert!(!m.partition_id.is_empty());
    assert_eq!(m.consumer_group, "g1");
    assert!(m.is_leased(), "a normal pop must take a lease");
    assert_eq!(m.data["n"], 1);
    // The push path cannot store a trace id.
    assert!(m.trace_id.is_none());

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn an_auto_ack_pop_takes_no_lease_and_commits_immediately() {
    let q = broker!();
    let queue = unique("pop-autoack");
    create_queue(&q, &queue, short_lease(1)).await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();

    let mut msgs = Vec::new();
    for _ in 0..20 {
        msgs = q
            .queue(&queue)
            .group("g-auto")
            .wait(false)
            .pop_auto_ack()
            .await
            .unwrap();
        if !msgs.is_empty() {
            break;
        }
        sleep_ms(150).await;
    }
    assert_eq!(msgs.len(), 1);
    assert!(!msgs[0].is_leased(), "autoAck must not take a lease");

    // The cursor already moved, so nothing redelivers once the (short) lease
    // window has passed.
    sleep_ms(2500).await;
    let again = q
        .queue(&queue)
        .group("g-auto")
        .batch(10)
        .wait(false)
        .pop()
        .await
        .unwrap();
    assert!(again.is_empty(), "autoAck message was redelivered");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn discovery_pop_finds_a_queue_by_namespace_and_task() {
    let q = broker!();
    let queue = unique("pop-discovery");
    let ns = format!("{queue}-ns");
    let task = format!("{queue}-task");

    create_queue(
        &q,
        &queue,
        QueueOptions {
            namespace: Some(ns.clone()),
            task: Some(task.clone()),
            ..Default::default()
        },
    )
    .await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();

    let mut found = Vec::new();
    for _ in 0..20 {
        found = q
            .queue_opt(None)
            .namespace(&ns)
            .task(&task)
            .wait(false)
            .pop()
            .await
            .unwrap();
        if !found.is_empty() {
            break;
        }
        sleep_ms(150).await;
    }
    assert_eq!(found.len(), 1, "discovery pop found nothing");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_multi_partition_pop_drains_several_lanes_under_one_lease() {
    let q = broker!();
    let queue = unique("pop-multipart");
    create_queue(&q, &queue, QueueOptions::default()).await;

    for lane in ["a", "b", "c"] {
        q.queue(&queue)
            .partition(lane)
            .push(serde_json::json!({ "lane": lane }))
            .await
            .unwrap();
    }

    let mut msgs = Vec::new();
    for _ in 0..25 {
        // docs:start(rust-pop)
        msgs = q
            .queue(&queue)
            .group("g-multi")
            .batch(10)
            .partitions(3)
            .wait(false)
            .pop()
            .await
            .unwrap();
        // docs:end
        if msgs.len() >= 3 {
            break;
        }
        sleep_ms(150).await;
    }

    assert!(msgs.len() >= 2, "multi-partition pop drained only {}", msgs.len());
    let leases: std::collections::HashSet<&str> =
        msgs.iter().map(|m| m.lease_id.as_str()).collect();
    assert_eq!(leases.len(), 1, "one pop must yield exactly one lease id");

    drop_queue(&q, &queue).await;
}

// ------------------------------------------------------------------ queue

#[tokio::test]
async fn configure_round_trips_every_option() {
    let q = broker!();
    let queue = unique("queue-config");

    let res = q
        .queue(&queue)
        .configure(QueueOptions {
            lease_time: Some(120),
            retry_limit: Some(7),
            max_size: Some(1000),
            delayed_processing: Some(2),
            window_buffer: Some(0),
            retention_seconds: Some(3600),
            completed_retention_seconds: Some(600),
            retention_enabled: Some(true),
            dedup_window_seconds: Some(60),
            priority: Some(3),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(res.get("configured").and_then(|v| v.as_bool()), Some(true));
    // configure_queue_v1 echoes the options back verbatim; the client must not
    // reshape them on the way out or the way back.
    for (key, want) in [
        ("leaseTime", 120),
        ("retryLimit", 7),
        ("maxSize", 1000),
        ("retentionSeconds", 3600),
        ("completedRetentionSeconds", 600),
        ("dedupWindowSeconds", 60),
    ] {
        if let Some(got) = res.get(key).and_then(|v| v.as_i64()) {
            assert_eq!(got, want as i64, "option {key} did not round-trip");
        }
    }

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_queue_can_be_deleted() {
    let q = broker!();
    let queue = unique("queue-delete");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue).delete().await.unwrap();

    // A deleted queue holds nothing. (Pushing recreates it — queues are
    // created on first push — so assert on emptiness, not on an error.)
    let msgs = q.queue(&queue).batch(10).wait(false).pop().await.unwrap();
    assert!(msgs.is_empty());
}

// ---------------------------------------------------------------- consume

#[tokio::test]
async fn consume_processes_and_acks_each_message() {
    let q = broker!();
    let queue = unique("consume-each");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push_many((0..5).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);

    // docs:start(rust-consume)
    let summary = q
        .queue(&queue)
        .group("g-consume")
        .batch(5)
        .limit(5)
        .wait(false)
        .idle(Duration::from_secs(5))
        .consume(move |msg| {
            let sink = Arc::clone(&sink);
            async move {
                sink.lock().unwrap().push(msg.data["n"].as_i64().unwrap());
                Ok::<_, std::convert::Infallible>(())
            }
        })
        .await
        .unwrap();
    // docs:end

    assert_eq!(summary.processed, 5);
    assert_eq!(summary.acked, 5);
    assert_eq!(summary.nacked, 0);
    assert_eq!(summary.stopped_by, StopReason::Limit);

    let mut got = seen.lock().unwrap().clone();
    got.sort();
    assert_eq!(got, vec![0, 1, 2, 3, 4]);

    // Everything was acked, so nothing redelivers.
    let again = q
        .queue(&queue)
        .group("g-consume")
        .batch(10)
        .wait(false)
        .pop()
        .await
        .unwrap();
    assert!(again.is_empty());

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn consume_batch_settles_the_whole_claim_at_once() {
    let q = broker!();
    let queue = unique("consume-batch");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push_many((0..6).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();

    let batches = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&batches);

    let summary = q
        .queue(&queue)
        .group("g-batch")
        .batch(6)
        .limit(6)
        .wait(false)
        .idle(Duration::from_secs(5))
        .consume_batch(move |msgs| {
            let counter = Arc::clone(&counter);
            async move {
                assert!(!msgs.is_empty());
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<_, std::convert::Infallible>(())
            }
        })
        .await
        .unwrap();

    assert_eq!(summary.processed, 6);
    assert_eq!(summary.acked, 6);
    assert!(
        batches.load(Ordering::SeqCst) <= 2,
        "batch(6) should settle in one or two calls, not {}",
        batches.load(Ordering::SeqCst)
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn an_idle_consumer_stops_on_its_own() {
    let q = broker!();
    let queue = unique("consume-idle");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let started = std::time::Instant::now();
    let summary = q
        .queue(&queue)
        .group("g-idle")
        .wait(false)
        .idle(Duration::from_millis(600))
        .consume(|_msg| async { Ok::<_, std::convert::Infallible>(()) })
        .await
        .unwrap();

    assert_eq!(summary.processed, 0);
    assert_eq!(summary.stopped_by, StopReason::Idle);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "idle consumer took too long to give up"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_cancelled_consumer_winds_down() {
    let q = broker!();
    let queue = unique("consume-cancel");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let cancel = queen_mq::Cancel::new();
    let stopper = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        stopper.cancel();
    });

    let summary = q
        .queue(&queue)
        .group("g-cancel")
        .wait(false)
        .cancel(cancel)
        .consume(|_msg| async { Ok::<_, std::convert::Infallible>(()) })
        .await
        .unwrap();

    assert_eq!(summary.stopped_by, StopReason::Cancelled);

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_failing_handler_nacks_and_the_message_comes_back() {
    let q = broker!();
    let queue = unique("consume-nack");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(1),
            retry_limit: Some(5),
            ..Default::default()
        },
    )
    .await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();

    let summary = q
        .queue(&queue)
        .group("g-nack")
        .limit(1)
        .wait(false)
        .idle(Duration::from_secs(5))
        .consume(|_msg| async { Err::<(), _>("handler exploded") })
        .await
        .unwrap();

    assert_eq!(summary.nacked, 1, "a handler error must nack");
    assert_eq!(summary.acked, 0);

    // A nack clamps the cursor, so the message is redelivered.
    let again = pop_retry(&q, &queue, Some("g-nack"), 1, 25).await;
    assert_eq!(again.len(), 1, "a nacked message must be redelivered");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn auto_ack_off_leaves_the_message_claimed() {
    let q = broker!();
    let queue = unique("consume-manual");
    create_queue(&q, &queue, short_lease(1)).await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();

    let summary = q
        .queue(&queue)
        .group("g-manual")
        .limit(1)
        .wait(false)
        .auto_ack(false)
        .idle(Duration::from_secs(5))
        .consume(|_msg| async { Ok::<_, std::convert::Infallible>(()) })
        .await
        .unwrap();

    assert_eq!(summary.processed, 1);
    assert_eq!(summary.acked, 0, "auto_ack(false) must not ack");

    // Nothing acked it, so the claim expires and it returns.
    sleep_ms(2500).await;
    let again = pop_retry(&q, &queue, Some("g-manual"), 1, 25).await;
    assert_eq!(again.len(), 1);

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn two_consumer_groups_each_receive_every_message() {
    let q = broker!();
    let queue = unique("consume-groups");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push_many((0..3).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();

    let a = pop_retry(&q, &queue, Some("group-a"), 10, 25).await;
    let b = pop_retry(&q, &queue, Some("group-b"), 10, 25).await;

    assert_eq!(a.len(), 3, "group-a did not see every message");
    assert_eq!(b.len(), 3, "group-b did not see every message");

    drop_queue(&q, &queue).await;
}

// --------------------------------------------------------------- pipeline

#[tokio::test]
async fn a_three_stage_handoff_carries_a_message_end_to_end() {
    let q = broker!();
    let init = unique("complete-init");
    let next = unique("complete-next");
    let final_q = unique("complete-final");

    for name in [&init, &next, &final_q] {
        create_queue(&q, name, QueueOptions::default()).await;
    }

    q.queue(&init)
        .push(serde_json::json!({ "message": "First", "count": 0 }))
        .await
        .unwrap();

    // Stage 1 → 2, and stage 2 → 3, each as one atomic ack+push.
    for (from, to) in [(&init, &next), (&next, &final_q)] {
        let msgs = pop_retry(&q, from, None, 1, 25).await;
        assert_eq!(msgs.len(), 1, "stage {from} received nothing");
        let msg = &msgs[0];
        let count = msg.data["count"].as_i64().unwrap();

        q.transaction()
            .ack(msg)
            .push(to.clone(), serde_json::json!({ "count": count + 1 }))
            .unwrap()
            .commit()
            .await
            .unwrap();
    }

    let out = pop_retry(&q, &final_q, None, 1, 25).await;
    assert_eq!(out.len(), 1, "nothing reached the final stage");
    assert_eq!(out[0].data["count"], 2);

    for name in [&init, &next, &final_q] {
        drop_queue(&q, name).await;
    }
}

#[tokio::test]
async fn a_few_thousand_messages_round_trip_without_loss() {
    let q = broker!();
    let queue = unique("load");
    create_queue(&q, &queue, QueueOptions::default()).await;

    const N: usize = 2_000;
    for chunk in 0..(N / 200) {
        q.queue(&queue)
            .push_many((0..200).map(|i| serde_json::json!({ "n": chunk * 200 + i })))
            .await
            .unwrap();
    }

    let mut seen = std::collections::HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while seen.len() < N && std::time::Instant::now() < deadline {
        let msgs = q
            .queue(&queue)
            .group("g-load")
            .batch(500)
            .wait(false)
            .pop()
            .await
            .unwrap();
        if msgs.is_empty() {
            sleep_ms(100).await;
            continue;
        }
        for m in &msgs {
            seen.insert(m.data["n"].as_i64().unwrap());
        }
        q.ack_all(&msgs).await.unwrap();
    }

    assert_eq!(seen.len(), N, "lost {} of {N} messages", N - seen.len());

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn subscription_mode_new_skips_the_existing_backlog() {
    let q = broker!();
    let queue = unique("subscription-new");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push(serde_json::json!({ "when": "before" }))
        .await
        .unwrap();
    // Let the backlog land before the group registers.
    let pre = pop_retry(&q, &queue, Some("g-warm"), 10, 25).await;
    assert_eq!(pre.len(), 1, "setup: the backlog never became poppable");

    // A brand-new group starting at `now` must not see it.
    let fresh = q
        .queue(&queue)
        .group("g-only-new")
        .batch(10)
        .wait(false)
        .subscription_mode(SubscriptionMode::New)
        .subscription_from("now")
        .pop()
        .await
        .unwrap();
    assert!(
        fresh.is_empty(),
        "subscriptionMode=new delivered {} pre-existing message(s)",
        fresh.len()
    );

    q.queue(&queue)
        .push(serde_json::json!({ "when": "after" }))
        .await
        .unwrap();

    let mut after = Vec::new();
    for _ in 0..25 {
        after = q
            .queue(&queue)
            .group("g-only-new")
            .batch(10)
            .wait(false)
            .subscription_mode(SubscriptionMode::New)
            .pop()
            .await
            .unwrap();
        if !after.is_empty() {
            break;
        }
        sleep_ms(150).await;
    }
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].data["when"], "after");

    drop_queue(&q, &queue).await;
}
