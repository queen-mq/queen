//! Core protocol: push, pop, queue lifecycle, consume.
//!
//! Ports the `push`, `pop`, `queue`, `consume`, `complete`, `bootstrap` and
//! `load` areas of the JS, Go and Python suites.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use queen_mq::{BufferOptions, PushStatus, Queen, QueueOptions, StopReason, SubscriptionMode};

use common::*;

// ------------------------------------------------------------------- push

#[tokio::test]
async fn push_single_is_queued() {
    let q = broker!();
    let queue = unique("push-single");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let res = q
        .queue(&queue)
        .push(serde_json::json!({ "hello": "world" }))
        .await
        .unwrap();

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

    let txn = format!("{queue}-fixed-txn");
    let item =
        queen_mq::PushItem::new(&queue, serde_json::json!({ "n": 1 })).transaction_id(txn.clone());

    let first = q
        .queue(&queue)
        .push_items(vec![item.clone()])
        .await
        .unwrap();
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
        ..Default::default()
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
        ..Default::default()
    });
    for n in 0..5 {
        b.push(serde_json::json!({ "n": n })).await.unwrap();
    }

    let msgs = pop_retry(&q, &queue, None, 10, 30).await;
    assert_eq!(msgs.len(), 5, "the size threshold did not trigger a flush");

    drop_queue(&q, &queue).await;
}

/// Wait for the client-side push buffers to empty.
///
/// A time-triggered flush runs on a task the manager spawns, so there is no
/// future to await: the buffer count going to zero is the only observable.
async fn wait_for_buffer_drain(q: &Queen, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if q.buffer_stats().total_buffered_messages == 0 {
            return true;
        }
        sleep_ms(50).await;
    }
    false
}

// Every `BufferOptions` in this repository is built with `time: 60s`, so the
// timer half of the buffer had never actually fired — while the public default
// is one second, which is what every caller who takes the default gets. A timer
// that never armed, or that armed once and never again, would look exactly like
// a working buffer to the size-threshold tests above.
#[tokio::test]
async fn a_time_threshold_flushes_the_buffer_and_then_re_arms() {
    let q = broker!();
    let queue = unique("push-timeflush");
    create_queue(&q, &queue, QueueOptions::default()).await;

    // A count threshold far above what this test pushes, so only the timer can
    // possibly send anything.
    let b = q.queue(&queue).buffer(BufferOptions {
        message_count: 1_000,
        time: Duration::from_millis(400),
        ..Default::default()
    });

    b.push(serde_json::json!({ "n": 1 }))
        .await
        .expect("buffered push failed");
    assert_eq!(
        q.buffer_stats().total_buffered_messages,
        1,
        "the first push should still be waiting, not on the wire"
    );

    assert!(
        wait_for_buffer_drain(&q, Duration::from_secs(10)).await,
        "the time threshold never fired: {} message(s) still buffered",
        q.buffer_stats().total_buffered_messages
    );

    let first = pop_retry(&q, &queue, Some("g-timeflush"), 10, 30).await;
    assert_eq!(
        first.len(),
        1,
        "the timed flush emptied the buffer locally \
         but sent nothing to the broker"
    );
    q.ack_all(&first).await.expect("ack failed");

    // The second push is the point of the test: the manager clears `timer_armed`
    // when a timer fires, so a buffer that has flushed once has to arm a fresh
    // timer. If it does not, every message after the first flush waits forever.
    b.push(serde_json::json!({ "n": 2 }))
        .await
        .expect("buffered push failed");
    assert!(
        wait_for_buffer_drain(&q, Duration::from_secs(10)).await,
        "the buffer timer did not re-arm after firing once"
    );

    let second = pop_retry(&q, &queue, Some("g-timeflush"), 10, 30).await;
    assert_eq!(second.len(), 1, "the re-armed timer sent nothing");
    assert_eq!(second[0].data["n"], 2);

    drop_queue(&q, &queue).await;
}

// `flush_buffer()` has no call site anywhere in the crate. It is the targeted
// counterpart of `flush_all_buffers`, and the way it can go wrong is by
// flushing too much: buffers are keyed by (queue, partition), so an address
// built from the queue alone would ship another lane's messages early.
#[tokio::test]
async fn flush_buffer_sends_only_the_partition_it_was_asked_for() {
    let q = broker!();
    let queue = unique("push-flush-one");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let opts = BufferOptions {
        message_count: 1_000,
        time: Duration::from_secs(60),
        ..Default::default()
    };
    let eu = q.queue(&queue).partition("eu").buffer(opts);
    let us = q.queue(&queue).partition("us").buffer(opts);
    eu.push(serde_json::json!({ "lane": "eu" }))
        .await
        .expect("buffered push failed");
    us.push(serde_json::json!({ "lane": "us" }))
        .await
        .expect("buffered push failed");
    assert_eq!(
        q.buffer_stats().active_buffers,
        2,
        "two partitions of one queue must buffer separately"
    );

    let flushed = eu.flush_buffer().await.expect("flush_buffer failed");
    assert_eq!(
        flushed.len(),
        1,
        "flush_buffer returned {} results for one buffered message",
        flushed.len()
    );
    assert_eq!(flushed[0].status, PushStatus::Queued);

    let stats = q.buffer_stats();
    assert_eq!(
        stats.total_buffered_messages, 1,
        "flush_buffer drained the other partition as well"
    );
    assert_eq!(stats.active_buffers, 1);

    let mut eu_msgs = Vec::new();
    for _ in 0..30 {
        eu_msgs = q
            .queue(&queue)
            .partition("eu")
            .batch(10)
            .wait(false)
            .pop()
            .await
            .expect("pop failed");
        if !eu_msgs.is_empty() {
            break;
        }
        sleep_ms(150).await;
    }
    assert_eq!(eu_msgs.len(), 1, "the flushed lane delivered nothing");
    assert_eq!(eu_msgs[0].data["lane"], "eu");

    let us_msgs = q
        .queue(&queue)
        .partition("us")
        .batch(10)
        .wait(false)
        .pop()
        .await
        .expect("pop failed");
    assert!(
        us_msgs.is_empty(),
        "the lane nobody flushed reached the broker anyway: {} message(s)",
        us_msgs.len()
    );

    // Do not leave a buffer (and its 60-second timer) armed behind this test.
    let _ = us.flush_buffer().await;
    drop_queue(&q, &queue).await;
}

// `close()` has no call site either, and it is the documented way to not lose
// buffered messages at shutdown. A close that emptied the buffer without
// awaiting the send would look identical from the stats and drop everything.
#[tokio::test]
async fn close_flushes_whatever_the_buffers_still_hold() {
    let q = broker!();
    let queue = unique("push-close");
    create_queue(&q, &queue, QueueOptions::default()).await;

    // Neither threshold can fire on its own within this test.
    let b = q.queue(&queue).buffer(BufferOptions {
        message_count: 1_000,
        time: Duration::from_secs(60),
        ..Default::default()
    });
    for n in 0..3 {
        b.push(serde_json::json!({ "n": n }))
            .await
            .expect("buffered push failed");
    }
    assert_eq!(q.buffer_stats().total_buffered_messages, 3);

    q.close().await.expect("close failed");
    assert_eq!(
        q.buffer_stats().total_buffered_messages,
        0,
        "close() returned with messages still buffered"
    );

    let msgs = pop_retry(&q, &queue, Some("g-close"), 10, 30).await;
    assert_eq!(
        msgs.len(),
        3,
        "close() emptied the buffer without sending it: {} of 3 arrived",
        msgs.len()
    );

    drop_queue(&q, &queue).await;
}

// One request carrying the same key three times is the *intra-batch* dedup
// path, which is not the same code as "push it again later": winner and losers
// are decided inside a single transaction. A batch that deduplicated only
// against already-stored rows would enqueue all three.
#[tokio::test]
async fn duplicates_within_one_push_collapse_onto_the_first_item() {
    let q = broker!();
    let queue = unique("push-dedup-batch");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            dedup_window_seconds: Some(3600),
            ..Default::default()
        },
    )
    .await;

    let txn = format!("{queue}-one-key");
    let items: Vec<queen_mq::PushItem> = (0..3)
        .map(|n| {
            queen_mq::PushItem::new(&queue, serde_json::json!({ "n": n })).transaction_id(&txn)
        })
        .collect();

    let res = q
        .queue(&queue)
        .push_items(items)
        .await
        .expect("push failed");
    assert_eq!(res.len(), 3, "one result per item, losers included");

    let winner = res
        .iter()
        .find(|r| r.status == PushStatus::Queued)
        .expect("no item in the batch was queued");
    let duplicates: Vec<_> = res
        .iter()
        .filter(|r| r.status == PushStatus::Duplicate)
        .collect();
    assert_eq!(
        duplicates.len(),
        2,
        "three items sharing one key produced {} duplicate(s)",
        duplicates.len()
    );
    for d in &duplicates {
        assert_eq!(
            d.message_id, winner.message_id,
            "a duplicate must carry the winner's message id, or a producer cannot \
             tell what its retry resolved to"
        );
    }

    // A later request with the same key resolves to the same message, which is
    // what proves the in-batch winner is the row the dedup window remembers.
    let again = q
        .queue(&queue)
        .push_items(vec![
            queen_mq::PushItem::new(&queue, serde_json::json!({ "n": 9 })).transaction_id(&txn),
            queen_mq::PushItem::new(&queue, serde_json::json!({ "n": 10 })).transaction_id(&txn),
        ])
        .await
        .expect("push failed");
    assert!(
        again.iter().all(|r| r.status == PushStatus::Duplicate),
        "a key already stored must not enqueue again: {again:?}"
    );
    assert!(
        again.iter().all(|r| r.message_id == winner.message_id),
        "a late duplicate must resolve to the pre-existing message"
    );

    let msgs = pop_retry(&q, &queue, Some("g-dedup-batch"), 10, 30).await;
    assert_eq!(
        msgs.len(),
        1,
        "five pushes of one key left {} message(s) in the queue",
        msgs.len()
    );

    drop_queue(&q, &queue).await;
}

// The dedup contract under a race, which the sequential retry tests cannot
// reach: eight requests carrying one key, none of them ordered against the
// others. A winner decided anywhere other than the single row the key maps to
// enqueues the message twice, and the loser gets an id nobody can look up.
#[tokio::test]
async fn racing_pushes_of_one_key_still_enqueue_exactly_once() {
    let q = broker!();
    let queue = unique("push-dedup-race");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            dedup_window_seconds: Some(3600),
            ..Default::default()
        },
    )
    .await;

    let txn = format!("{queue}-contended-key");
    let mut tasks = Vec::new();
    for n in 0..8 {
        let q = q.clone();
        let queue = queue.clone();
        let txn = txn.clone();
        tasks.push(tokio::spawn(async move {
            q.queue(&queue)
                .push_items(vec![queen_mq::PushItem::new(
                    &queue,
                    serde_json::json!({ "n": n }),
                )
                .transaction_id(txn)])
                .await
        }));
    }

    let mut results = Vec::new();
    for t in tasks {
        let out = t
            .await
            .expect("a push task panicked")
            .expect("a racing push failed");
        results.extend(out);
    }
    assert_eq!(results.len(), 8, "one result per racing push");

    let queued = results
        .iter()
        .filter(|r| r.status == PushStatus::Queued)
        .count();
    assert_eq!(
        queued, 1,
        "eight racing pushes of one key produced {queued} queued message(s)"
    );

    let ids: std::collections::HashSet<&str> =
        results.iter().map(|r| r.message_id.as_str()).collect();
    assert_eq!(
        ids.len(),
        1,
        "every racer must resolve to one message id, got {ids:?}"
    );

    let msgs = pop_retry(&q, &queue, Some("g-dedup-race"), 10, 30).await;
    assert_eq!(
        msgs.len(),
        1,
        "the queue ended up holding {} message(s)",
        msgs.len()
    );

    drop_queue(&q, &queue).await;
}

// `PushItem::partition` is never used anywhere in the crate, and it is the only
// way to address two lanes in one request. It also documents a trap:
// `push_items` reads the address from each ITEM, so the builder's
// `.partition()` is an affinity and buffering key here, not a default the way
// `push_many` treats it.
#[tokio::test]
async fn push_items_takes_each_items_own_partition() {
    let q = broker!();
    let queue = unique("push-item-lane");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let res = q
        .queue(&queue)
        .partition("eu")
        .push_items(vec![
            queen_mq::PushItem::new(&queue, serde_json::json!({ "lane": "own" })).partition("us"),
            queen_mq::PushItem::new(&queue, serde_json::json!({ "lane": "none" })),
        ])
        .await
        .expect("push failed");
    assert_eq!(res.len(), 2, "one result per item");
    assert!(
        res.iter().all(|r| r.status == PushStatus::Queued),
        "both items should have been stored: {res:?}"
    );

    // A multi-lane drain, because a pop that names no partition may be served
    // any single one of them.
    let all = drain_until(&q, &queue, Duration::from_secs(30), |m| m.len() >= 2).await;
    assert_eq!(all.len(), 2, "only {} of 2 messages arrived", all.len());

    let own = all
        .iter()
        .find(|m| m.data["lane"] == "own")
        .expect("the item addressed to 'us' never arrived");
    assert_eq!(
        own.partition, "us",
        "PushItem::partition must win over the builder's partition"
    );

    let none = all
        .iter()
        .find(|m| m.data["lane"] == "none")
        .expect("the item with no partition never arrived");
    assert_eq!(
        none.partition, "Default",
        "an item with no partition falls back to Default; push_items does not \
         inherit the builder's 'eu'"
    );

    assert!(
        !all.iter().any(|m| m.partition == "eu"),
        "nothing should have landed in the builder's partition"
    );

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
            .subscription_mode(SubscriptionMode::All)
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

    // Register the group BEFORE pushing. The broker's default subscription mode is
    // "new", so a group first seen after the pushes would seed at the tail and read
    // nothing. Seeding it while the queue is empty keeps the documented snippet
    // below free of a subscription_mode call it has no business teaching.
    q.queue(&queue)
        .group("g-multi")
        .batch(1)
        .wait(false)
        .pop()
        .await
        .unwrap();

    for lane in ["a", "b", "c"] {
        q.queue(&queue)
            .partition(lane)
            .push(serde_json::json!({ "lane": lane }))
            .await
            .unwrap();
    }

    let mut msgs = Vec::new();
    for _ in 0..25 {
        msgs = q
            .queue(&queue)
            .group("g-multi")
            .batch(10)
            .partitions(3)
            .wait(false)
            .pop()
            .await
            .unwrap();
        if msgs.len() >= 3 {
            break;
        }
        sleep_ms(150).await;
    }

    assert!(
        msgs.len() >= 2,
        "multi-partition pop drained only {}",
        msgs.len()
    );
    let leases: std::collections::HashSet<&str> =
        msgs.iter().map(|m| m.lease_id.as_str()).collect();
    assert_eq!(leases.len(), 1, "one pop must yield exactly one lease id");

    drop_queue(&q, &queue).await;
}

// `wait(true)` is the builder's default and what the README examples use, yet
// no test in this suite had ever taken it: every one of them opts out with
// `.wait(false)`. A long poll that is never woken, or woken only at its
// deadline, reads as a slow broker rather than as a client that is not
// long-polling at all.
#[tokio::test]
async fn a_blocking_pop_wakes_up_when_a_message_lands() {
    let q = broker!();
    let queue = unique("pop-longpoll");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let producer = q.clone();
    let target = queue.clone();
    let writer = tokio::spawn(async move {
        sleep_ms(1_500).await;
        producer
            .queue(&target)
            .push(serde_json::json!({ "late": true }))
            .await
            .expect("the delayed push failed");
    });

    let started = Instant::now();
    let msgs = q
        .queue(&queue)
        .group("g-longpoll")
        .batch(10)
        .wait(true)
        .poll_timeout(Duration::from_secs(5))
        .pop()
        .await
        .expect("a long poll must not error");
    let waited = started.elapsed();

    writer.await.expect("the producer task panicked");

    assert_eq!(
        msgs.len(),
        1,
        "the long poll returned {} message(s) after {waited:?}",
        msgs.len()
    );
    assert_eq!(msgs[0].data["late"], true);
    assert!(
        waited < Duration::from_secs(5),
        "the long poll only delivered at its deadline ({waited:?}); the push \
         landed after 1.5s and should have woken it"
    );

    drop_queue(&q, &queue).await;
}

// The other half of `wait(true)`: a queue that stays quiet. The client asks for
// `poll_timeout + 5s` on the socket precisely so the broker's timer fires
// first and this reads as an empty poll. Lose that slack and every long-polling
// consumer starts reporting transport failures instead of an idle queue.
#[tokio::test]
async fn a_blocking_pop_on_a_quiet_queue_returns_empty_not_a_timeout() {
    let q = broker!();
    let queue = unique("pop-longpoll-quiet");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let started = Instant::now();
    let msgs = q
        .queue(&queue)
        .group("g-quiet")
        .wait(true)
        .poll_timeout(Duration::from_secs(2))
        .pop()
        .await
        .expect("an exhausted long poll must be Ok(empty), never Err");
    let waited = started.elapsed();

    assert!(
        msgs.is_empty(),
        "a quiet queue served {} message(s)",
        msgs.len()
    );
    assert!(
        waited >= Duration::from_millis(1_500),
        "the poll came back after {waited:?}: wait(true) did not hold the request \
         for its timeout"
    );
    assert!(
        waited < Duration::from_secs(6),
        "the poll took {waited:?}, which is the client's own timeout \
         (poll_timeout + 5s) firing rather than the broker's"
    );

    drop_queue(&q, &queue).await;
}

// `batch` is a budget for the whole claim, not a per-lane allowance. Read the
// other way round — ten *per partition* — a five-lane pop hands back fifty
// messages, a silent fivefold overshoot of whatever the caller sized its
// handler and its lease for.
#[tokio::test]
async fn batch_caps_the_whole_claim_not_each_partition() {
    let q = broker!();
    let queue = unique("pop-batch-cap");
    create_queue(&q, &queue, QueueOptions::default()).await;

    const LANES: usize = 5;
    const PER_LANE: usize = 100;
    for lane in 0..LANES {
        q.queue(&queue)
            .partition(format!("lane-{lane}"))
            .push_many((0..PER_LANE).map(|n| serde_json::json!({ "lane": lane, "n": n })))
            .await
            .expect("push failed");
    }

    // The cap only means something once every lane is poppable: a pop racing the
    // fusion window returns fewer for reasons that have nothing to do with
    // `batch`. The warm-up runs on its own consumer group, so the groups below
    // still see the whole backlog.
    let want = LANES * PER_LANE;
    let warm = drain_until(&q, &queue, Duration::from_secs(60), |m| m.len() >= want).await;
    assert_eq!(
        warm.len(),
        want,
        "setup: only {} of {want} messages became visible",
        warm.len()
    );

    let capped = q
        .queue(&queue)
        .group("g-cap")
        .batch(10)
        .partitions(LANES as i32)
        .wait(false)
        .subscription_mode(SubscriptionMode::All)
        .pop()
        .await
        .expect("pop failed");
    assert_eq!(
        capped.len(),
        10,
        "batch(10) with partitions({LANES}) returned {} messages; the batch caps \
         the claim, not each lane",
        capped.len()
    );

    // And without `partitions()` a claim is a single lane, however large the
    // batch. This asserts the lane count rather than the message count: how much
    // one checkout takes is the broker's business, how many lanes it locks is
    // the contract.
    let single = q
        .queue(&queue)
        .group("g-single-lane")
        .batch(want as i32)
        .wait(false)
        .subscription_mode(SubscriptionMode::All)
        .pop()
        .await
        .expect("pop failed");
    assert!(!single.is_empty(), "a default pop claimed nothing");
    let lanes: std::collections::HashSet<&str> =
        single.iter().map(|m| m.partition.as_str()).collect();
    assert_eq!(
        lanes.len(),
        1,
        "a pop without partitions() drained {} lanes: {lanes:?}",
        lanes.len()
    );

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

    // Seed the group before the pushes: see the note in the multi-partition pop
    // test. The broker's default subscription mode is "new".
    q.queue(&queue)
        .group("g-consume")
        .batch(1)
        .wait(false)
        .pop()
        .await
        .unwrap();

    q.queue(&queue)
        .push_many((0..5).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);

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
        .subscription_mode(SubscriptionMode::All)
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
        .subscription_mode(SubscriptionMode::All)
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
        .subscription_mode(SubscriptionMode::All)
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

// Every consume test above opts out of long polling, which left the loop's
// `popped.is_empty() && builder.wait` branch — the default one — unexecuted.
// That branch must neither sleep nor turn an empty poll into an error, and a
// consumer that did either would still finish this test's work, just late or
// not at all.
#[tokio::test]
async fn a_long_polling_consumer_stops_on_its_limit() {
    let q = broker!();
    let queue = unique("consume-longpoll");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push_many((0..4).map(|n| serde_json::json!({ "n": n })))
        .await
        .expect("push failed");

    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);

    let summary = q
        .queue(&queue)
        .group("g-consume-longpoll")
        .batch(4)
        .limit(4)
        .poll_timeout(Duration::from_secs(3))
        .idle(Duration::from_secs(20))
        .subscription_mode(SubscriptionMode::All)
        .consume(move |msg| {
            let sink = Arc::clone(&sink);
            async move {
                sink.lock()
                    .expect("handler sink poisoned")
                    .push(msg.data["n"].as_i64().unwrap_or(-1));
                Ok::<_, std::convert::Infallible>(())
            }
        })
        .await
        .expect("a long-polling consumer must not fail on an empty poll");

    assert_eq!(summary.processed, 4);
    assert_eq!(summary.acked, 4);
    assert_eq!(
        summary.stopped_by,
        StopReason::Limit,
        "a long-polling consumer that reached its limit stopped by {:?}",
        summary.stopped_by
    );

    let mut got = seen.lock().expect("handler sink poisoned").clone();
    got.sort();
    assert_eq!(got, vec![0, 1, 2, 3], "the consumer saw {got:?}");

    drop_queue(&q, &queue).await;
}

// The idle deadline is only re-checked between polls, so with long polling on
// it competes with the poll window. A consumer that sat on the socket instead
// of winding down would eventually stop for the wrong reason, or hang until
// something arrived.
#[tokio::test]
async fn a_long_polling_consumer_goes_idle_on_a_quiet_queue() {
    let q = broker!();
    let queue = unique("consume-longpoll-idle");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let started = Instant::now();
    let summary = q
        .queue(&queue)
        .group("g-longpoll-idle")
        .poll_timeout(Duration::from_secs(1))
        .idle(Duration::from_secs(2))
        .consume(|_msg| async { Ok::<_, std::convert::Infallible>(()) })
        .await
        .expect("an empty long poll must not fail the consumer");

    assert_eq!(summary.processed, 0);
    assert_eq!(
        summary.stopped_by,
        StopReason::Idle,
        "a quiet long-polling consumer stopped by {:?}",
        summary.stopped_by
    );
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "the consumer took {:?} to give up on an empty queue",
        started.elapsed()
    );

    drop_queue(&q, &queue).await;
}

// No consumer in the suite had ever claimed more than one partition, which left
// the per-message settle against a *shared* lease untested: an ack that
// released the whole claim on the first message would strand the rest of the
// batch and have it redelivered.
#[tokio::test]
async fn a_consumer_can_settle_a_multi_partition_claim_message_by_message() {
    let q = broker!();
    let queue = unique("consume-multipart");
    create_queue(&q, &queue, QueueOptions::default()).await;

    const LANES: usize = 4;
    const PER_LANE: usize = 5;
    for lane in 0..LANES {
        q.queue(&queue)
            .partition(format!("lane-{lane}"))
            .push_many((0..PER_LANE).map(|n| serde_json::json!({ "lane": lane, "n": n })))
            .await
            .expect("push failed");
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);

    let want = (LANES * PER_LANE) as u64;
    let summary = q
        .queue(&queue)
        .group("g-multipart")
        .batch(20)
        .partitions(LANES as i32)
        .limit(want)
        .wait(false)
        .idle(Duration::from_secs(20))
        .subscription_mode(SubscriptionMode::All)
        .consume(move |msg| {
            let sink = Arc::clone(&sink);
            async move {
                sink.lock()
                    .expect("handler sink poisoned")
                    .push(msg.partition.clone());
                Ok::<_, std::convert::Infallible>(())
            }
        })
        .await
        .expect("consume failed");

    assert_eq!(summary.processed, want);
    assert_eq!(
        summary.acked, want,
        "every message under a shared lease must be acked, not just the first"
    );
    assert_eq!(summary.nacked, 0);
    assert_eq!(summary.stopped_by, StopReason::Limit);

    let lanes: std::collections::HashSet<String> = seen
        .lock()
        .expect("handler sink poisoned")
        .iter()
        .cloned()
        .collect();
    assert_eq!(
        lanes.len(),
        LANES,
        "the consumer only reached {} of {LANES} lanes: {lanes:?}",
        lanes.len()
    );

    // A settle that released the lease early would leave the tail of each claim
    // to come back here.
    let again = q
        .queue(&queue)
        .group("g-multipart")
        .batch(50)
        .partitions(LANES as i32)
        .wait(false)
        .pop()
        .await
        .expect("pop failed");
    assert!(
        again.is_empty(),
        "{} message(s) came back after a fully acked multi-partition consume",
        again.len()
    );

    drop_queue(&q, &queue).await;
}

// The suite's only `consume_batch` test has a handler that always succeeds, so
// `settle_batch`'s error arm — one nack covering the entire claim — had never
// run. A batch that half-acked on failure would look fine in the summary and
// lose the messages it silently committed.
#[tokio::test]
async fn a_failing_batch_handler_nacks_the_whole_claim() {
    let q = broker!();
    let queue = unique("consume-batch-fail");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(1),
            // High enough that the redelivery check below cannot race the DLQ.
            retry_limit: Some(20),
            ..Default::default()
        },
    )
    .await;

    q.queue(&queue)
        .push_many((0..4).map(|n| serde_json::json!({ "n": n })))
        .await
        .expect("push failed");

    let handled = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&handled);

    let summary = q
        .queue(&queue)
        .group("g-batchfail")
        .batch(4)
        .limit(4)
        .wait(false)
        .idle(Duration::from_secs(5))
        .subscription_mode(SubscriptionMode::All)
        .consume_batch(move |msgs| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(msgs.len() as u64, Ordering::SeqCst);
                Err::<(), _>("batch handler exploded")
            }
        })
        .await
        .expect("consume_batch failed");

    assert!(
        handled.load(Ordering::SeqCst) > 0,
        "the handler never ran, so nothing was settled either way"
    );
    assert_eq!(
        summary.acked, 0,
        "a failing batch handler must not ack anything"
    );
    assert_eq!(
        summary.nacked, summary.processed,
        "settle_batch must nack every message it handed to the handler"
    );

    let again = pop_retry(&q, &queue, Some("g-batchfail"), 10, 40).await;
    assert!(!again.is_empty(), "a nacked batch was never redelivered");

    drop_queue(&q, &queue).await;
}

// ----------------------------------------------------------------- acking

// `nack_all` has no call site in the crate: the batch reject path was reachable
// only through a `consume_batch` handler error. Rejecting a claim by hand is
// what a manual consumer does, and it has to come back with one verdict per
// message rather than a single collapsed result.
#[tokio::test]
async fn nack_all_rejects_a_whole_claim_and_hands_it_back() {
    let q = broker!();
    let queue = unique("nack-all");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(1),
            retry_limit: Some(20),
            ..Default::default()
        },
    )
    .await;

    q.queue(&queue)
        .push_many((0..4).map(|n| serde_json::json!({ "n": n })))
        .await
        .expect("push failed");

    let msgs = pop_retry(&q, &queue, Some("g-nackall"), 10, 30).await;
    assert!(!msgs.is_empty(), "setup: nothing became poppable");

    let acks = q
        .nack_all(&msgs, "the batch could not be processed")
        .await
        .expect("nack_all failed");
    assert_eq!(
        acks.len(),
        msgs.len(),
        "nack_all returned {} verdicts for {} messages",
        acks.len(),
        msgs.len()
    );
    assert!(
        acks.iter().all(|a| a.success),
        "the broker refused part of the batch nack: {acks:?}"
    );
    assert!(
        acks.iter().all(|a| !a.dlq),
        "a first rejection under a retry limit of 20 must not dead-letter"
    );

    // A nack clamps the group's cursor at the failure, so the claim comes back.
    let again = pop_retry(&q, &queue, Some("g-nackall"), 10, 40).await;
    assert!(
        again.len() >= msgs.len(),
        "a rejected claim of {} came back as {}",
        msgs.len(),
        again.len()
    );

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
            .subscription_mode(SubscriptionMode::All)
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
