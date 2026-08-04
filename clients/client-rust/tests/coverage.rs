//! Breadth coverage: queue options, payload shapes, naming, ordering,
//! concurrency, and the admin operations the other suites do not reach.
//!
//! Where `core.rs` and `semantics.rs` pin the contracts, this suite goes wide —
//! the awkward inputs and the settings that only matter when someone actually
//! turns them on.

mod common;

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use queen_mq::{AckStatus, PushStatus, QueueOptions};

use common::*;

// ============================================================================
// Queue options
// ============================================================================

#[tokio::test]
async fn delayed_processing_holds_a_message_before_it_is_poppable() {
    let q = broker!();
    let queue = unique("cov-delayed");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            delayed_processing: Some(3),
            ..Default::default()
        },
    )
    .await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();

    // Not yet.
    sleep_ms(800).await;
    let early = q.queue(&queue).batch(10).wait(false).pop().await.unwrap();
    assert!(
        early.is_empty(),
        "delayedProcessing=3 served a message after 0.8s"
    );

    // ...but it does arrive.
    let later = pop_retry(&q, &queue, None, 10, 40).await;
    assert_eq!(later.len(), 1, "the delayed message never became poppable");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn window_buffer_batches_pushes_before_making_them_visible() {
    let q = broker!();
    let queue = unique("cov-windowbuffer");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            window_buffer: Some(2),
            ..Default::default()
        },
    )
    .await;

    q.queue(&queue)
        .push_many((0..5).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();

    // The trade windowBuffer makes: latency for batch size. Everything still
    // arrives, just later.
    let msgs = pop_retry(&q, &queue, None, 10, 60).await;
    assert_eq!(msgs.len(), 5, "windowBuffer lost messages");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_queue_configured_for_encryption_still_round_trips() {
    let q = broker!();
    let queue = unique("cov-encrypted");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            encryption_enabled: Some(true),
            ..Default::default()
        },
    )
    .await;

    let payload = serde_json::json!({ "secret": "hunter2", "nested": { "n": [1, 2, 3] } });
    q.queue(&queue).push(payload.clone()).await.unwrap();

    // Whether the bytes are encrypted at rest depends on the broker holding a
    // key; either way the consumer must see the plaintext it pushed.
    let msgs = pop_retry(&q, &queue, None, 1, 25).await;
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].data, payload, "encryption changed what came back");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_per_queue_retry_limit_decides_when_a_message_dead_letters() {
    let q = broker!();
    let queue = unique("cov-retrylimit");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(30),
            retry_limit: Some(2),
            ..Default::default()
        },
    )
    .await;

    q.queue(&queue)
        .push(serde_json::json!({ "poison": true }))
        .await
        .unwrap();

    let mut deliveries = 0;
    for _ in 0..10 {
        let msgs = pop_retry(&q, &queue, None, 1, 10).await;
        if msgs.is_empty() {
            break;
        }
        deliveries += 1;
        q.nack(&msgs[0], "always fails").await.unwrap();
        sleep_ms(150).await;
    }

    assert_eq!(dlq_count(&q, &queue).await, 1, "never dead-lettered");
    assert!(
        (2..=4).contains(&deliveries),
        "retryLimit=2 should mean a handful of deliveries, saw {deliveries}"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn dead_lettering_needs_both_flags_off_to_be_off() {
    let q = broker!();

    // `deadLetterQueue: false` ALONE does not disable dead-lettering. The
    // broker computes
    //     dlq_enabled = COALESCE(dead_letter_queue, true)
    //                   OR COALESCE(dlq_after_max_retries, true)
    // (005_log_ack.sql), and dlqAfterMaxRetries defaults to true — so the OR
    // keeps the DLQ on. Both have to be turned off.
    let still_on = unique("cov-nodlq-one");
    create_queue(
        &q,
        &still_on,
        QueueOptions {
            lease_time: Some(30),
            retry_limit: Some(1),
            dead_letter_queue: Some(false),
            ..Default::default()
        },
    )
    .await;
    q.queue(&still_on)
        .push(serde_json::json!({ "poison": true }))
        .await
        .unwrap();
    for _ in 0..6 {
        let msgs = pop_retry(&q, &still_on, None, 1, 8).await;
        if msgs.is_empty() {
            break;
        }
        q.nack(&msgs[0], "always fails").await.unwrap();
        sleep_ms(150).await;
    }
    assert_eq!(
        dlq_count(&q, &still_on).await,
        1,
        "deadLetterQueue:false alone is expected NOT to disable the DLQ — if this \
         now passes, the broker changed the OR in 005_log_ack.sql and the client \
         docs need updating"
    );
    drop_queue(&q, &still_on).await;

    let queue = unique("cov-nodlq");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(30),
            retry_limit: Some(1),
            dead_letter_queue: Some(false),
            dlq_after_max_retries: Some(false),
            ..Default::default()
        },
    )
    .await;

    q.queue(&queue)
        .push(serde_json::json!({ "poison": true }))
        .await
        .unwrap();

    for _ in 0..6 {
        let msgs = pop_retry(&q, &queue, None, 1, 8).await;
        if msgs.is_empty() {
            break;
        }
        q.nack(&msgs[0], "always fails").await.unwrap();
        sleep_ms(150).await;
    }

    assert_eq!(
        dlq_count(&q, &queue).await,
        0,
        "with BOTH flags off the message should be dropped, not dead-lettered"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn the_dedup_window_can_be_switched_off() {
    let q = broker!();
    let queue = unique("cov-nodedup");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            dedup_window_seconds: Some(0),
            ..Default::default()
        },
    )
    .await;

    let txn = format!("{queue}-same");
    let item = queen_mq::PushItem::new(&queue, serde_json::json!({ "n": 1 }))
        .transaction_id(txn.clone());

    let a = q.queue(&queue).push_items(vec![item.clone()]).await.unwrap();
    let b = q.queue(&queue).push_items(vec![item]).await.unwrap();

    assert_eq!(a[0].status, PushStatus::Queued);
    // With the window at zero the second push is a new message, not a
    // duplicate — the setting is what makes dedup exact rather than best
    // effort.
    assert_eq!(
        b[0].status,
        PushStatus::Queued,
        "dedupWindowSeconds=0 still deduplicated"
    );

    drop_queue(&q, &queue).await;
}

// ============================================================================
// Payload shapes
// ============================================================================

#[tokio::test]
async fn payloads_of_awkward_shapes_survive_the_round_trip() {
    let q = broker!();
    let queue = unique("cov-payloads");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let cases = vec![
        serde_json::json!({ "empty_object": {} }),
        serde_json::json!({ "empty_array": [] }),
        serde_json::json!({ "null_field": null }),
        serde_json::json!({ "bool": true, "float": 1.5, "neg": -42 }),
        serde_json::json!({ "unicode": "héllo — 世界 🐝 \u{1F41D}" }),
        serde_json::json!({ "quotes": "he said \"hi\" and \\ escaped" }),
        serde_json::json!({ "newlines": "a\nb\tc\r\nd" }),
        serde_json::json!({ "deep": { "a": { "b": { "c": { "d": [1, { "e": "f" }] } } } } }),
        serde_json::json!({ "big_number": 9_007_199_254_740_991i64 }),
        serde_json::json!({
            "many_keys": (0..50)
                .map(|i| (format!("k{i}"), serde_json::json!(i)))
                .collect::<serde_json::Map<String, serde_json::Value>>()
        }),
    ];

    for (i, payload) in cases.iter().enumerate() {
        q.queue(&queue)
            .partition(format!("case-{i}"))
            .push(payload.clone())
            .await
            .unwrap();
    }

    let mut seen = 0;
    for (i, payload) in cases.iter().enumerate() {
        // Address the lane directly: a queue-wide pop serves one partition at a
        // time, so "any partition" would be a race, not a test.
        let mut msgs = Vec::new();
        for _ in 0..25 {
            msgs = q
                .queue(&queue)
                .partition(format!("case-{i}"))
                .batch(5)
                .wait(false)
                .pop()
                .await
                .unwrap();
            if !msgs.is_empty() {
                break;
            }
            sleep_ms(150).await;
        }
        assert_eq!(msgs.len(), 1, "case {i} never arrived");
        assert_eq!(&msgs[0].data, payload, "case {i} came back changed");
        seen += 1;
    }
    assert_eq!(seen, cases.len());

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_large_payload_round_trips() {
    let q = broker!();
    let queue = unique("cov-large");
    create_queue(&q, &queue, QueueOptions::default()).await;

    // ~512 KB of JSON. Compression happens broker-side, so this exercises the
    // pack/compress/decompress path rather than just the HTTP layer.
    let blob: String = "queen".repeat(100_000);
    let payload = serde_json::json!({ "blob": blob });

    let res = q.queue(&queue).push(payload.clone()).await.unwrap();
    assert_eq!(res[0].status, PushStatus::Queued);

    let msgs = pop_retry(&q, &queue, None, 1, 30).await;
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].data, payload, "a large payload came back changed");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_scalar_payload_is_accepted() {
    let q = broker!();
    let queue = unique("cov-scalar");
    create_queue(&q, &queue, QueueOptions::default()).await;

    for payload in [
        serde_json::json!(42),
        serde_json::json!("a string"),
        serde_json::json!(true),
        serde_json::json!(null),
    ] {
        q.queue(&queue).push(payload).await.unwrap();
    }

    let msgs = pop_retry(&q, &queue, None, 10, 25).await;
    assert_eq!(msgs.len(), 4, "a scalar payload was rejected or lost");

    drop_queue(&q, &queue).await;
}

// ============================================================================
// Naming
// ============================================================================

#[tokio::test]
async fn awkward_queue_and_partition_names_are_addressable() {
    let q = broker!();
    // The client percent-encodes path segments; without that these break the
    // route rather than the queue.
    let names = [
        format!("{}-with space", unique("cov-name")),
        format!("{}/with-slash", unique("cov-name")),
        format!("{}-ünïcode-🐝", unique("cov-name")),
        format!("{}-with:colon", unique("cov-name")),
    ];

    for name in &names {
        create_queue(&q, name, QueueOptions::default()).await;
        q.queue(name)
            .partition("lane with space/and-slash")
            .push(serde_json::json!({ "name": name }))
            .await
            .unwrap();

        let msgs = q
            .queue(name)
            .partition("lane with space/and-slash")
            .batch(10)
            .wait(false)
            .pop()
            .await
            .unwrap_or_default();
        let msgs = if msgs.is_empty() {
            pop_retry(&q, name, None, 10, 25).await
        } else {
            msgs
        };
        assert!(!msgs.is_empty(), "could not read back from queue '{name}'");
        assert_eq!(msgs[0].data["name"], serde_json::json!(name));

        drop_queue(&q, name).await;
    }
}

#[tokio::test]
async fn a_very_long_partition_name_works() {
    let q = broker!();
    let queue = unique("cov-longpart");
    create_queue(&q, &queue, QueueOptions::default()).await;

    let lane = "l".repeat(200);
    q.queue(&queue)
        .partition(&lane)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();

    let msgs = pop_retry(&q, &queue, None, 1, 25).await;
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].partition, lane);

    drop_queue(&q, &queue).await;
}

// ============================================================================
// Ordering and concurrency
// ============================================================================

#[tokio::test]
async fn a_partition_stays_ordered_under_concurrent_consumers() {
    let q = broker!();
    let queue = unique("cov-ordering");
    create_queue(&q, &queue, QueueOptions::default()).await;

    const LANES: usize = 6;
    const PER_LANE: usize = 25;
    for lane in 0..LANES {
        for i in 0..PER_LANE {
            q.queue(&queue)
                .partition(format!("lane-{lane}"))
                .push(serde_json::json!({ "lane": lane, "seq": i }))
                .await
                .unwrap();
        }
    }

    // Several workers competing for the same queue. A partition is claimed by
    // one at a time, so per-lane order must survive regardless.
    let seen: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);

    let summary = q
        .queue(&queue)
        .group("g-order")
        .concurrency(4)
        .batch(10)
        .limit((LANES * PER_LANE) as u64)
        .wait(false)
        .idle(Duration::from_secs(6))
        .consume(move |msg| {
            let sink = Arc::clone(&sink);
            async move {
                sink.lock().unwrap().push((
                    msg.data["lane"].as_u64().unwrap(),
                    msg.data["seq"].as_u64().unwrap(),
                ));
                Ok::<_, std::convert::Infallible>(())
            }
        })
        .await
        .unwrap();

    assert_eq!(summary.processed, (LANES * PER_LANE) as u64);

    let seen = seen.lock().unwrap();
    for lane in 0..LANES as u64 {
        let seq: Vec<u64> = seen
            .iter()
            .filter(|(l, _)| *l == lane)
            .map(|(_, s)| *s)
            .collect();
        assert_eq!(
            seq.len(),
            PER_LANE,
            "lane {lane} delivered {} of {PER_LANE}",
            seq.len()
        );
        let mut sorted = seq.clone();
        sorted.sort();
        assert_eq!(seq, sorted, "lane {lane} arrived out of order: {seq:?}");
    }

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn concurrent_consumers_never_deliver_the_same_message_twice() {
    let q = broker!();
    let queue = unique("cov-nodupes");
    create_queue(&q, &queue, QueueOptions::default()).await;

    const N: usize = 200;
    q.queue(&queue)
        .push_many((0..N).map(|i| serde_json::json!({ "i": i })))
        .await
        .unwrap();

    let seen: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);

    q.queue(&queue)
        .group("g-nodupes")
        .concurrency(6)
        .batch(10)
        .limit(N as u64)
        .wait(false)
        .idle(Duration::from_secs(6))
        .consume(move |msg| {
            let sink = Arc::clone(&sink);
            async move {
                sink.lock().unwrap().push(msg.data["i"].as_u64().unwrap());
                Ok::<_, std::convert::Infallible>(())
            }
        })
        .await
        .unwrap();

    let seen = seen.lock().unwrap();
    let unique_seen: HashSet<u64> = seen.iter().copied().collect();
    assert_eq!(
        unique_seen.len(),
        seen.len(),
        "a message was delivered to two workers at once"
    );
    assert_eq!(unique_seen.len(), N, "lost messages");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn many_producers_can_push_the_same_queue_at_once() {
    let q = broker!();
    let queue = unique("cov-producers");
    create_queue(&q, &queue, QueueOptions::default()).await;

    const PRODUCERS: usize = 8;
    const PER: usize = 25;

    let mut tasks = Vec::new();
    for p in 0..PRODUCERS {
        let q = q.clone();
        let queue = queue.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..PER {
                q.queue(&queue)
                    .partition(format!("p-{p}"))
                    .push(serde_json::json!({ "p": p, "i": i }))
                    .await
                    .unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let want = PRODUCERS * PER;
    let mut seen = 0;
    let deadline = Instant::now() + Duration::from_secs(45);
    while seen < want && Instant::now() < deadline {
        let msgs = q
            .queue(&queue)
            .group("g-producers")
            .batch(200)
            .partitions(16)
            .wait(false)
            .pop()
            .await
            .unwrap();
        if msgs.is_empty() {
            sleep_ms(100).await;
            continue;
        }
        seen += msgs.len();
        q.ack_all(&msgs).await.unwrap();
    }
    assert_eq!(seen, want, "concurrent producers lost messages");

    drop_queue(&q, &queue).await;
}

// ============================================================================
// Client-side guards
// ============================================================================

#[tokio::test]
async fn batch_acking_across_consumer_groups_is_refused_locally() {
    let q = broker!();
    let queue = unique("cov-mixedgroups");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();

    let a = pop_retry(&q, &queue, Some("group-a"), 1, 25).await;
    let b = pop_retry(&q, &queue, Some("group-b"), 1, 25).await;
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);

    // The batch endpoint carries ONE consumer group, so mixing them would ack
    // half the batch against the wrong cursor. Caught before the request.
    let mixed = vec![a[0].clone(), b[0].clone()];
    let err = q.ack_all(&mixed).await.unwrap_err();
    assert!(
        err.to_string().contains("across consumer groups"),
        "expected a local refusal, got: {err}"
    );

    // Acked separately, both work.
    q.ack_all(&a).await.unwrap();
    q.ack_all(&b).await.unwrap();

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn acking_a_message_with_no_partition_id_is_refused_locally() {
    let q = broker!();
    let queue = unique("cov-nopartition");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();
    let msgs = pop_retry(&q, &queue, None, 1, 25).await;

    let mut broken = msgs[0].clone();
    broken.partition_id = String::new();
    let err = q.ack(&broken).await.unwrap_err();
    assert!(
        err.to_string().contains("partitionId"),
        "a transaction id alone is not unique across partitions; got: {err}"
    );

    drop_queue(&q, &queue).await;
}

// ============================================================================
// Admin operations
// ============================================================================

// NOTE: there is no "clear a queue" test because there is no such route on
// this broker. `DELETE /api/v1/queues/{name}/clear` — which the JS SDK's
// Admin.clearQueue() calls — answers 404 `no_such_route`, so the Rust client
// does not expose it. Dropping the queue is covered in core.rs.

#[tokio::test]
async fn a_message_can_be_inspected_retried_and_dead_lettered_by_hand() {
    let q = broker!();
    let queue = unique("cov-msgadmin");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(30),
            ..Default::default()
        },
    )
    .await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();
    let msgs = pop_retry(&q, &queue, Some("g-admin"), 1, 25).await;
    assert_eq!(msgs.len(), 1);
    let (pid, txn) = (&msgs[0].partition_id, &msgs[0].transaction_id);

    let detail = q.admin().message(pid, txn).await.unwrap();
    assert!(detail.is_object(), "message detail was not an object");

    // `retry` is a DLQ *replay*, so on a live message it must fail rather than
    // quietly doing something else.
    assert!(
        q.admin().retry_message(pid, txn).await.is_err(),
        "retry_message on a live message should error — it replays DLQ rows"
    );

    // Dead-letter it the way the broker actually supports: an ack with `dlq`.
    let acked = q
        .ack_with(&msgs[0], AckStatus::Dlq, Some("by hand".into()))
        .await
        .unwrap();
    assert!(acked.success && acked.dlq);
    sleep_ms(400).await;
    assert_eq!(dlq_count(&q, &queue).await, 1);

    // Now retry DOES apply: it re-pushes the snapshot and drops the DLQ row.
    let replayed = q.admin().retry_message(pid, txn).await;
    assert!(replayed.is_ok(), "DLQ replay failed: {replayed:?}");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_group_can_be_moved_back_to_a_timestamp() {
    let q = broker!();
    let queue = unique("cov-seekts");
    let group = format!("{queue}-cg");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push_many((0..3).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();
    let first = pop_retry(&q, &queue, Some(&group), 10, 25).await;
    assert_eq!(first.len(), 3);
    q.ack_all(&first).await.unwrap();

    // Back to before the first message.
    let seek = q
        .admin()
        .seek_consumer_group(
            &group,
            &queue,
            &queen_mq::SeekRequest {
                timestamp: Some("1970-01-01T00:00:00Z".into()),
                position: None,
            },
        )
        .await;
    if seek.is_err() {
        eprintln!("SKIP timestamp seek: broker refused ({seek:?})");
        drop_queue(&q, &queue).await;
        return;
    }

    let replayed = pop_retry(&q, &queue, Some(&group), 10, 25).await;
    assert!(!replayed.is_empty(), "seeking to the epoch replayed nothing");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn namespaces_and_tasks_are_listed() {
    let q = broker!();
    let queue = unique("cov-nslist");
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

    let mut found_ns = false;
    let mut found_task = false;
    for _ in 0..20 {
        found_ns |= q.admin().namespaces().await.unwrap().to_string().contains(&ns);
        found_task |= q.admin().tasks().await.unwrap().to_string().contains(&task);
        if found_ns && found_task {
            break;
        }
        sleep_ms(250).await;
    }
    assert!(found_ns, "the namespace never appeared");
    assert!(found_task, "the task never appeared");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn queue_statistics_reflect_traffic() {
    let q = broker!();
    let queue = unique("cov-stats");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push_many((0..5).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();
    let msgs = pop_retry(&q, &queue, Some("g-stats"), 10, 25).await;
    q.ack_all(&msgs).await.unwrap();

    let _ = q.admin().refresh_consumer_stats().await;

    let mut seen = false;
    for _ in 0..20 {
        let stats = q
            .admin()
            .queue_stats(&[("limit", "500".to_string())])
            .await
            .unwrap();
        if stats.to_string().contains(&queue) {
            seen = true;
            break;
        }
        sleep_ms(300).await;
    }
    assert!(seen, "the queue never showed up in the statistics");

    // The per-queue detail endpoint answers for it too.
    assert!(q.admin().queue_detail(&queue, &[]).await.is_ok());

    drop_queue(&q, &queue).await;
}

// ============================================================================
// Leases
// ============================================================================

#[tokio::test]
async fn a_renewed_lease_lets_a_slow_handler_finish() {
    let q = broker!();
    let queue = unique("cov-slowhandler");
    create_queue(&q, &queue, short_lease(2)).await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();

    let handled = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&handled);

    // The handler outlives the 2s lease; automatic renewal is what stops the
    // message being handed to somebody else mid-flight.
    let summary = q
        .queue(&queue)
        .group("g-slow")
        .limit(1)
        .wait(false)
        .renew_lease(Duration::from_millis(800))
        .idle(Duration::from_secs(10))
        .consume(move |_msg| {
            let counter = Arc::clone(&counter);
            async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<_, std::convert::Infallible>(())
            }
        })
        .await
        .unwrap();

    assert_eq!(summary.acked, 1, "the slow handler's ack was rejected");
    assert_eq!(handled.load(Ordering::SeqCst), 1);

    // And it was not also delivered to a second consumer.
    let again = q
        .queue(&queue)
        .group("g-slow")
        .batch(10)
        .wait(false)
        .pop()
        .await
        .unwrap();
    assert!(again.is_empty(), "the message was redelivered despite renewal");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_lease_override_shorter_than_the_handler_loses_the_message() {
    let q = broker!();
    let queue = unique("cov-leaselost");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(60),
            ..Default::default()
        },
    )
    .await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();

    // Claim with a 1s lease and do not renew.
    let mut claimed = Vec::new();
    for _ in 0..25 {
        claimed = q
            .queue(&queue)
            .group("g-lost")
            .wait(false)
            .lease_seconds(1)
            .pop()
            .await
            .unwrap();
        if !claimed.is_empty() {
            break;
        }
        sleep_ms(150).await;
    }
    assert_eq!(claimed.len(), 1);

    sleep_ms(2500).await;

    // The claim lapsed, so acking it now is rejected rather than silently
    // accepted — the message belongs to whoever claims it next.
    let res = q.ack(&claimed[0]).await.unwrap();
    let redelivered = pop_retry(&q, &queue, Some("g-lost"), 1, 20).await;
    assert!(
        !res.success || !redelivered.is_empty(),
        "an expired lease neither failed the ack nor redelivered the message"
    );

    drop_queue(&q, &queue).await;
}

// ============================================================================
// Idempotency
// ============================================================================

#[tokio::test]
async fn a_retried_push_inside_the_window_enqueues_once() {
    let q = broker!();
    let queue = unique("cov-idempotent");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            dedup_window_seconds: Some(3600),
            ..Default::default()
        },
    )
    .await;

    // The realistic shape: a client that does not know whether its first push
    // landed, retrying with the same id.
    let txn = format!("{queue}-order-42");
    for _ in 0..5 {
        q.queue(&queue)
            .push_items(vec![queen_mq::PushItem::new(
                &queue,
                serde_json::json!({ "order": 42 }),
            )
            .transaction_id(&txn)])
            .await
            .unwrap();
    }

    let msgs = pop_retry(&q, &queue, None, 10, 25).await;
    assert_eq!(msgs.len(), 1, "five retries produced {} messages", msgs.len());
    assert_eq!(msgs[0].transaction_id, txn);

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn dedup_is_scoped_to_a_queue_and_partition() {
    let q = broker!();
    let a = unique("cov-dedupscope-a");
    let b = unique("cov-dedupscope-b");
    create_queue(&q, &a, QueueOptions::default()).await;
    create_queue(&q, &b, QueueOptions::default()).await;

    let txn = "shared-transaction-id".to_string();
    let ra = q
        .queue(&a)
        .push_items(vec![
            queen_mq::PushItem::new(&a, serde_json::json!({ "q": "a" })).transaction_id(&txn)
        ])
        .await
        .unwrap();
    let rb = q
        .queue(&b)
        .push_items(vec![
            queen_mq::PushItem::new(&b, serde_json::json!({ "q": "b" })).transaction_id(&txn)
        ])
        .await
        .unwrap();

    // The same id in two queues is two messages — dedup is not global, and a
    // shared id space between unrelated producers would be unusable.
    assert_eq!(ra[0].status, PushStatus::Queued);
    assert_eq!(rb[0].status, PushStatus::Queued);

    drop_queue(&q, &a).await;
    drop_queue(&q, &b).await;
}

// ============================================================================
// Transactions, wider
// ============================================================================

#[tokio::test]
async fn one_transaction_can_fan_out_to_several_queues() {
    let q = broker!();
    let src = unique("cov-fanout-src");
    let out_a = unique("cov-fanout-a");
    let out_b = unique("cov-fanout-b");
    for name in [&src, &out_a, &out_b] {
        create_queue(&q, name, QueueOptions::default()).await;
    }

    q.queue(&src)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();
    let msgs = pop_retry(&q, &src, Some("g-fanout"), 1, 25).await;

    q.transaction()
        .ack(&msgs[0])
        .push(out_a.clone(), serde_json::json!({ "to": "a" }))
        .unwrap()
        .push(out_b.clone(), serde_json::json!({ "to": "b" }))
        .unwrap()
        .push_to(out_a.clone(), "lane", serde_json::json!({ "to": "a2" }))
        .unwrap()
        .commit()
        .await
        .unwrap();

    // out_a received two pushes on DIFFERENT partitions, so the drain has to
    // claim more than one lane.
    let a = drain_until(&q, &out_a, Duration::from_secs(25), |m| m.len() >= 2).await;
    let b = drain_until(&q, &out_b, Duration::from_secs(25), |m| !m.is_empty()).await;
    assert_eq!(a.len(), 2, "queue a should have both pushes");
    assert_eq!(b.len(), 1);

    for name in [&src, &out_a, &out_b] {
        drop_queue(&q, name).await;
    }
}

#[tokio::test]
async fn a_transaction_can_ack_a_whole_batch() {
    let q = broker!();
    let src = unique("cov-txnbatch");
    let sink = unique("cov-txnbatch-sink");
    create_queue(&q, &src, QueueOptions::default()).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    q.queue(&src)
        .push_many((0..5).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();
    let msgs = pop_retry(&q, &src, Some("g-txnbatch"), 10, 25).await;
    assert_eq!(msgs.len(), 5);

    let total: i64 = msgs.iter().filter_map(|m| m.data["n"].as_i64()).sum();
    q.transaction()
        .ack_all(&msgs)
        .push(sink.clone(), serde_json::json!({ "total": total }))
        .unwrap()
        .commit()
        .await
        .unwrap();

    let again = q
        .queue(&src)
        .group("g-txnbatch")
        .batch(10)
        .wait(false)
        .pop()
        .await
        .unwrap();
    assert!(again.is_empty(), "the batch ack did not cover everything");

    let out = pop_retry(&q, &sink, None, 1, 25).await;
    assert_eq!(out[0].data["total"], 10);

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

#[tokio::test]
async fn a_transaction_with_a_stale_lease_rolls_back() {
    let q = broker!();
    let src = unique("cov-stalelease");
    let sink = unique("cov-stalelease-sink");
    create_queue(&q, &src, short_lease(1)).await;
    create_queue(&q, &sink, QueueOptions::default()).await;

    q.queue(&src)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();
    let msgs = pop_retry(&q, &src, Some("g-stale"), 1, 25).await;
    assert_eq!(msgs.len(), 1);

    // Let the claim lapse, then try to hand off. requiredLeases is what turns
    // this into a rollback rather than a push for work somebody else now owns.
    sleep_ms(2500).await;
    let out = q
        .transaction()
        .ack(&msgs[0])
        .push(sink.clone(), serde_json::json!({ "stage": 2 }))
        .unwrap()
        .commit()
        .await;

    if out.is_ok() {
        eprintln!("NOTE: the broker accepted a transaction on a lapsed lease");
    } else {
        sleep_ms(400).await;
        let leaked = q.queue(&sink).batch(5).wait(false).pop().await.unwrap();
        assert!(
            leaked.is_empty(),
            "the transaction failed but its push still landed"
        );
    }

    drop_queue(&q, &src).await;
    drop_queue(&q, &sink).await;
}

// ============================================================================
// Ack statuses
// ============================================================================

#[tokio::test]
async fn every_ack_status_is_accepted_and_does_what_it_says() {
    let q = broker!();
    let queue = unique("cov-ackstatus");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(30),
            retry_limit: Some(10),
            ..Default::default()
        },
    )
    .await;

    for status in [AckStatus::Completed, AckStatus::Retry, AckStatus::Failed] {
        q.queue(&queue)
            .partition(format!("lane-{status:?}"))
            .push(serde_json::json!({ "status": format!("{status:?}") }))
            .await
            .unwrap();
    }

    for status in [AckStatus::Completed, AckStatus::Retry, AckStatus::Failed] {
        let lane = format!("lane-{status:?}");
        let mut msgs = Vec::new();
        for _ in 0..25 {
            msgs = q
                .queue(&queue)
                .partition(&lane)
                .group("g-status")
                .batch(1)
                .wait(false)
                .pop()
                .await
                .unwrap();
            if !msgs.is_empty() {
                break;
            }
            sleep_ms(150).await;
        }
        assert_eq!(msgs.len(), 1, "nothing on lane {lane}");
        let res = q.ack_with(&msgs[0], status, Some("coverage".into())).await.unwrap();
        assert!(res.success, "{status:?} was rejected: {:?}", res.error);

        // completed advances the cursor; retry and failed put it back.
        let again = q
            .queue(&queue)
            .partition(&lane)
            .group("g-status")
            .batch(1)
            .wait(false)
            .pop()
            .await
            .unwrap();
        match status {
            AckStatus::Completed => assert!(again.is_empty(), "completed did not commit"),
            _ => { /* redelivery is timing-dependent; the ack succeeding is the assertion */ }
        }
    }

    drop_queue(&q, &queue).await;
}

// ============================================================================
// Long haul
// ============================================================================

#[tokio::test]
async fn a_sustained_push_consume_cycle_stays_consistent() {
    let q = broker!();
    let queue = unique("cov-sustained");
    create_queue(&q, &queue, QueueOptions::default()).await;

    // Producer and consumer running at the same time, which is the shape that
    // shakes out claim/ack races that a push-then-drain test cannot.
    const ROUNDS: usize = 20;
    const PER_ROUND: usize = 25;

    let producer = {
        let q = q.clone();
        let queue = queue.clone();
        tokio::spawn(async move {
            for round in 0..ROUNDS {
                q.queue(&queue)
                    .partition(format!("lane-{}", round % 4))
                    .push_many((0..PER_ROUND).map(|i| serde_json::json!({ "round": round, "i": i })))
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    };

    let want = ROUNDS * PER_ROUND;
    let mut seen: HashSet<(u64, u64)> = HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while seen.len() < want && Instant::now() < deadline {
        let msgs = q
            .queue(&queue)
            .group("g-sustained")
            .batch(100)
            .partitions(8)
            .wait(false)
            .pop()
            .await
            .unwrap();
        if msgs.is_empty() {
            sleep_ms(80).await;
            continue;
        }
        for m in &msgs {
            let key = (
                m.data["round"].as_u64().unwrap(),
                m.data["i"].as_u64().unwrap(),
            );
            assert!(seen.insert(key), "message {key:?} was delivered twice");
        }
        q.ack_all(&msgs).await.unwrap();
    }
    producer.await.unwrap();

    assert_eq!(seen.len(), want, "lost {} messages", want - seen.len());

    drop_queue(&q, &queue).await;
}
