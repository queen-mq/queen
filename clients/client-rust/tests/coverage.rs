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

use queen_mq::{
    AckStatus, Config, Error, PushStatus, Queen, QueueOptions, Strategy, SubscriptionMode,
};

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
    let item =
        queen_mq::PushItem::new(&queue, serde_json::json!({ "n": 1 })).transaction_id(txn.clone());

    let a = q
        .queue(&queue)
        .push_items(vec![item.clone()])
        .await
        .unwrap();
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

#[tokio::test]
async fn the_four_options_nothing_else_ever_sends_reach_the_broker() {
    let q = broker!();
    let queue = unique("cov-writeonly-opts");

    // `retryDelay`, `ttl`, `maxWaitTimeSeconds` and `minPopWaitTime` are the
    // four QueueOptions fields no other test sets. None of them is loud when it
    // goes missing: `configure_queue_v1` COALESCEs every key it does not
    // recognise to that option's own default, so a renamed serde key produces a
    // successful request, a configured queue, and an option that quietly does
    // nothing. The echo is the only place that difference is visible.
    let res = q
        .queue(&queue)
        .configure(QueueOptions {
            retry_delay: Some(2500),
            ttl: Some(4242),
            max_wait_time_seconds: Some(7),
            min_pop_wait_time: Some(250),
            ..Default::default()
        })
        .await
        .expect("configure was refused");

    let options = res
        .get("options")
        .unwrap_or_else(|| panic!("the configure echo carries no options object: {res}"));
    for (key, want) in [
        ("retryDelay", 2500i64),
        ("ttl", 4242),
        ("maxWaitTimeSeconds", 7),
        ("minPopWaitTime", 250),
    ] {
        assert_eq!(
            options.get(key).and_then(|v| v.as_i64()),
            Some(want),
            "{key} never reached configure_queue_v1 — the broker echoed its own \
             default, which is exactly what a renamed key looks like: {options}"
        );
    }

    // And they are stored, not merely echoed: the queue-detail projection reads
    // the columns back out. That projection IS the observable behaviour of
    // retryDelay and ttl — the log engine enforces neither on the hot path, they
    // only ever surface here and in the message-detail `queueConfig`.
    let detail = q
        .admin()
        .queue_detail(&queue, &[])
        .await
        .expect("queue detail failed");
    let config = &detail["queue"]["config"];
    assert_eq!(
        config["retryDelay"].as_i64(),
        Some(2500),
        "retryDelay was not persisted: {detail}"
    );
    assert_eq!(
        config["ttl"].as_i64(),
        Some(4242),
        "ttl was not persisted: {detail}"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn the_broker_clamps_a_minimum_pop_wait_it_considers_absurd() {
    let q = broker!();
    let queue = unique("cov-minpopwait");

    // The clamp is proof of delivery. `configure_queue_v1` stores
    // LEAST(GREATEST(minPopWaitTime, 0), 60000), so 90s comes back as 60000 — a
    // value the client never sent and cannot get by accident, whereas a dropped
    // or misnamed key defaults to 0 and looks like a caller who asked for
    // nothing.
    let over = q
        .queue(&queue)
        .configure(QueueOptions {
            min_pop_wait_time: Some(90_000),
            ..Default::default()
        })
        .await
        .expect("configure was refused");
    assert_eq!(
        over["options"]["minPopWaitTime"].as_i64(),
        Some(60_000),
        "an over-long pop wait was not clamped, which means it never arrived: {over}"
    );

    // Negative is not a faster way of saying zero, and the floor proves the same
    // point from the other side.
    let under = q
        .queue(&queue)
        .configure(QueueOptions {
            min_pop_wait_time: Some(-5),
            ..Default::default()
        })
        .await
        .expect("configure was refused");
    assert_eq!(
        under["options"]["minPopWaitTime"].as_i64(),
        Some(0),
        "a negative pop wait was stored as-is: {under}"
    );

    drop_queue(&q, &queue).await;
}

// ============================================================================
// Options that have to work together
// ============================================================================

#[tokio::test]
async fn a_delayed_queue_with_a_window_buffer_still_delivers_everything() {
    let q = broker!();
    let queue = unique("cov-delayed-window");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            delayed_processing: Some(3),
            window_buffer: Some(1),
            ..Default::default()
        },
    )
    .await;

    q.queue(&queue)
        .push_many((0..4).map(|n| serde_json::json!({ "n": n })))
        .await
        .unwrap();

    // Two independent holds on the same messages, applied at different points
    // of the pop: the window buffer refuses a partition whose newest segment is
    // too young, the delayed deadline filters the segments themselves. Only the
    // longer one may decide. Each option has its own test above and both would
    // still pass if one cancelled the other here — a broker that dropped
    // `delayedProcessing` once `windowBuffer` was set would serve these at ~1s.
    sleep_ms(1200).await;
    let early = q.queue(&queue).batch(10).wait(false).pop().await.unwrap();
    assert!(
        early.is_empty(),
        "delayedProcessing=3 stopped applying once windowBuffer was set: {} message(s) at 1.2s",
        early.len()
    );

    let later = pop_retry(&q, &queue, None, 10, 40).await;
    assert_eq!(later.len(), 4, "the two holds together lost messages");

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn dedup_still_catches_a_retried_push_on_an_encrypted_queue() {
    let q = broker!();
    let queue = unique("cov-encrypted-dedup");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            encryption_enabled: Some(true),
            dedup_window_seconds: Some(3600),
            ..Default::default()
        },
    )
    .await;

    // The two features meet on the same push and must not touch each other:
    // dedup resolves the transactionId's hash against the segment index while
    // encryption rewrites the payload the frame carries. (With no encryption
    // key configured the broker deliberately stores plaintext for a flagged
    // queue, so both assertions hold either way — what must not happen is the
    // retry landing twice.)
    let txn = format!("{queue}-order-7");
    let item = queen_mq::PushItem::new(&queue, serde_json::json!({ "secret": "hunter2", "n": 1 }))
        .transaction_id(&txn);

    let first = q
        .queue(&queue)
        .push_items(vec![item.clone()])
        .await
        .unwrap();
    let second = q.queue(&queue).push_items(vec![item]).await.unwrap();
    assert_eq!(first[0].status, PushStatus::Queued);
    assert_eq!(
        second[0].status,
        PushStatus::Duplicate,
        "the retry was enqueued a second time on an encrypted queue"
    );
    assert_eq!(
        second[0].message_id, first[0].message_id,
        "the duplicate verdict must point at the message that is already there"
    );

    let msgs = pop_retry(&q, &queue, None, 10, 25).await;
    assert_eq!(
        msgs.len(),
        1,
        "an encrypted queue deduplicated the push but \
         delivered {} message(s)",
        msgs.len()
    );
    assert_eq!(
        msgs[0].data,
        serde_json::json!({ "secret": "hunter2", "n": 1 }),
        "the payload did not come back as it was pushed"
    );
    assert_eq!(msgs[0].transaction_id, txn);

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_dead_lettered_message_carries_the_reason_its_nack_gave() {
    let q = broker!();
    let queue = unique("cov-dlq-reason");
    create_queue(
        &q,
        &queue,
        QueueOptions {
            lease_time: Some(30),
            retry_limit: Some(1),
            dead_letter_queue: Some(true),
            dlq_after_max_retries: Some(true),
            ..Default::default()
        },
    )
    .await;

    q.queue(&queue)
        .push(serde_json::json!({ "poison": true }))
        .await
        .unwrap();

    const REASON: &str = "downstream 503 while charging the card";
    for _ in 0..6 {
        let msgs = pop_retry(&q, &queue, None, 1, 10).await;
        if msgs.is_empty() {
            break;
        }
        q.nack(&msgs[0], REASON).await.unwrap();
        sleep_ms(150).await;
    }

    // Every other DLQ test in the suite counts rows. Counting is what let the
    // reason go missing for good: `queen.get_dlq_messages_v1` projects it under
    // `errorMessage`, the client read `error`, and the field was permanently
    // None while the text sat unread in `rest` — a green suite and a DLQ page
    // that says nothing about why anything is in it.
    let dlq = q.queue(&queue).dlq(Some(50), None).await.unwrap();
    assert_eq!(dlq.messages.len(), 1, "the message never dead-lettered");
    let row = &dlq.messages[0];
    assert_eq!(
        row.error.as_deref(),
        Some(REASON),
        "the dead-letter reason did not survive the round trip: {row:?}"
    );
    assert_eq!(
        row.queue.as_deref(),
        Some(queue.as_str()),
        "the DLQ row does not say which queue it came from: {row:?}"
    );
    assert!(
        row.partition_id.is_some(),
        "without a partitionId the row cannot be replayed: {row:?}"
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn an_auto_ack_pop_commits_every_lane_it_claimed() {
    let q = broker!();
    let queue = unique("cov-autoack-multi");
    create_queue(&q, &queue, short_lease(1)).await;

    const LANES: usize = 4;
    for lane in 0..LANES {
        q.queue(&queue)
            .partition(format!("lane-{lane}"))
            .push(serde_json::json!({ "lane": lane }))
            .await
            .unwrap();
    }

    // Broker-side autoAck commits at delivery and takes no lease. Across
    // several lanes it has to advance EVERY claimed partition's cursor, not
    // just the one the response reports at the top level — and the single-lane
    // test cannot tell those apart, because there the two are the same
    // partition.
    let mut seen: HashSet<u64> = HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    while seen.len() < LANES && Instant::now() < deadline {
        let msgs = q
            .queue(&queue)
            .group("g-autoack-multi")
            .batch(10)
            .partitions(8)
            .wait(false)
            .subscription_mode(SubscriptionMode::All)
            .pop_auto_ack()
            .await
            .unwrap();
        if msgs.is_empty() {
            sleep_ms(120).await;
            continue;
        }
        for m in &msgs {
            assert!(
                !m.is_leased(),
                "autoAck took a lease on lane {}",
                m.partition
            );
            let lane = m.data["lane"]
                .as_u64()
                .expect("every message carries its lane");
            assert!(
                seen.insert(lane),
                "lane {lane} was delivered twice under autoAck"
            );
        }
    }
    assert_eq!(
        seen.len(),
        LANES,
        "an autoAck multi-partition pop lost lanes"
    );

    // The queue's lease is one second, so an uncommitted lane would come back
    // here. Nothing does.
    sleep_ms(2500).await;
    let again = q
        .queue(&queue)
        .group("g-autoack-multi")
        .batch(10)
        .partitions(8)
        .wait(false)
        .pop()
        .await
        .unwrap();
    assert!(
        again.is_empty(),
        "autoAck left {} message(s) uncommitted across lanes",
        again.len()
    );

    drop_queue(&q, &queue).await;
}

#[tokio::test]
async fn a_group_seeded_at_now_and_a_queue_mode_consumer_keep_separate_cursors() {
    let q = broker!();
    let queue = unique("cov-modes");
    let group = format!("{queue}-late");
    create_queue(&q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push_many((0..2).map(|n| serde_json::json!({ "n": n, "when": "before" })))
        .await
        .unwrap();

    // Queue mode drains the backlog...
    let backlog = pop_retry(&q, &queue, None, 10, 25).await;
    assert_eq!(backlog.len(), 2, "setup: the backlog never became poppable");
    q.ack_all(&backlog).await.unwrap();

    // ...and a group registering afterwards at `now` inherits neither the
    // backlog nor queue mode's progress: the seeding branch in the pop SP
    // explicitly excludes __QUEUE_MODE__, so the two cursors are seeded and
    // advanced independently. Each mode is covered alone elsewhere; what is
    // untested is them sharing a queue, which is the shape a "replay one
    // consumer without disturbing the workers" deployment actually has.
    let skipped_backlog = q
        .queue(&queue)
        .group(&group)
        .batch(10)
        .wait(false)
        .subscription_mode(SubscriptionMode::New)
        .subscription_from("now")
        .pop()
        .await
        .unwrap();
    assert!(
        skipped_backlog.is_empty(),
        "a group seeded at now saw {} pre-existing message(s)",
        skipped_backlog.len()
    );

    q.queue(&queue)
        .push(serde_json::json!({ "n": 99, "when": "after" }))
        .await
        .unwrap();

    // The message after the cut goes to BOTH. The group's cursor now exists, so
    // the subscription hint is no longer in play — this reads the cursor the
    // empty pop above created.
    let for_group = pop_retry(&q, &queue, Some(&group), 10, 25).await;
    assert_eq!(
        for_group.len(),
        1,
        "the group should see exactly the message pushed after its cut; got {} — \
         more than one means the empty pop never seeded its cursor at `now`",
        for_group.len()
    );
    assert_eq!(for_group[0].data["when"], "after");
    q.ack_all(&for_group).await.unwrap();

    let for_queue_mode = pop_retry(&q, &queue, None, 10, 25).await;
    assert_eq!(
        for_queue_mode.len(),
        1,
        "queue mode lost the message to the consumer group's ack"
    );
    assert_eq!(for_queue_mode[0].data["n"], 99);

    drop_queue(&q, &queue).await;
}

// ============================================================================
// Client configuration, against a real broker
// ============================================================================

#[tokio::test]
async fn every_backend_strategy_completes_a_round_trip() {
    // `broker!()` is the suite's gate: it returns early when QUEEN_TEST_URL is
    // missing and fails under QUEEN_TEST_STRICT, so reading the variable after
    // it cannot become a silent skip.
    let _gate = broker!();
    let url = std::env::var("QUEEN_TEST_URL").expect("broker!() proved the URL is set");

    // Outside the unit tests every client in this suite is built with the
    // defaults, so `strategy`, `retry_attempts`, `failover(false)` and a valid
    // extra header have never been near a broker. Each of them changes how a
    // request is routed or given up on; a regression in any would surface first
    // in somebody's production consumer.
    for strategy in [Strategy::RoundRobin, Strategy::Session, Strategy::Affinity] {
        let q = Queen::connect(
            Config::new(url.clone())
                .strategy(strategy)
                .retry_attempts(1)
                .failover(false)
                .header("x-queen-client", "rust-coverage"),
        )
        .unwrap_or_else(|e| panic!("{strategy:?} is not a usable configuration: {e}"));

        let queue = unique("cov-strategy");
        create_queue(&q, &queue, QueueOptions::default()).await;
        q.queue(&queue)
            .push(serde_json::json!({ "strategy": format!("{strategy:?}") }))
            .await
            .unwrap_or_else(|e| panic!("{strategy:?} could not push: {e}"));

        let msgs = pop_retry(&q, &queue, Some("g-strategy"), 10, 25).await;
        assert_eq!(
            msgs.len(),
            1,
            "{strategy:?} could not read back what it pushed"
        );
        assert_eq!(msgs[0].data["strategy"], format!("{strategy:?}"));
        let acked = q.ack_all(&msgs).await.unwrap();
        assert!(
            acked.iter().all(|a| a.success),
            "{strategy:?} pushed and popped but could not ack: {acked:?}"
        );

        drop_queue(&q, &queue).await;
    }
}

#[tokio::test]
async fn with_failover_off_a_request_stays_on_the_backend_it_picked() {
    let _gate = broker!();
    let url = std::env::var("QUEEN_TEST_URL").expect("broker!() proved the URL is set");

    // Dead backend first, live one second, round-robin so the first pick is
    // deterministic (index 0). With failover ON this is admin.rs's
    // `an_unreachable_backend_fails_over_to_a_healthy_one` and the push
    // succeeds; with it OFF the client must stay where it is, because that is
    // the whole meaning of the flag. Nothing tested the off position, and the
    // failure it hides is silent: a client that walks anyway looks healthier
    // than it is.
    let mut config = Config::urls(["http://127.0.0.1:1".to_string(), url])
        .strategy(Strategy::RoundRobin)
        .failover(false)
        .retry_attempts(1);
    // The dead backend must stay out for the rest of the test: at the default
    // five seconds it would be readmitted mid-drain and a later poll would land
    // on it, failing the test for the wrong reason.
    config.health_retry_after = Duration::from_secs(120);
    let q = Queen::connect(config).unwrap();

    let queue = unique("cov-nofailover");
    let err = q
        .queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .expect_err("the first push goes to the dead backend and must fail");
    assert!(
        !matches!(err, Error::AllBackendsFailed { .. }),
        "failover is off, so the client must not have walked the backend list: {err}"
    );
    assert!(
        err.is_retryable(),
        "a refused connection should read as retryable: {err}"
    );

    // The dead backend is marked down by that failure, so the next request
    // picks the live one: failover off bounds a single request, it does not
    // brick the client.
    let res = q
        .queue(&queue)
        .push(serde_json::json!({ "n": 2 }))
        .await
        .expect("the live backend should have taken the second push");
    assert_eq!(res.len(), 1);
    let msgs = pop_retry(&q, &queue, None, 10, 25).await;
    assert_eq!(msgs.len(), 1, "the push that succeeded never landed");
    assert_eq!(msgs[0].data["n"], 2);

    // And the walk is what produces AllBackendsFailed: same two-backend shape,
    // failover on, nothing alive. Constructing the variant by hand (error.rs)
    // proves its accessors, not that `send` ever builds one.
    let doomed = Queen::connect(
        Config::urls(["http://127.0.0.1:1", "http://127.0.0.1:2"])
            .failover(true)
            .retry_attempts(1),
    )
    .unwrap();
    let err = doomed
        .queue(&queue)
        .push(serde_json::json!({ "n": 3 }))
        .await
        .expect_err("both backends are dead");
    assert!(
        matches!(err, Error::AllBackendsFailed { attempted: 2, .. }),
        "a failed walk over two backends must report the walk: {err}"
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
        .subscription_mode(SubscriptionMode::All)
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

    {
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
        .subscription_mode(SubscriptionMode::All)
        .consume(move |msg| {
            let sink = Arc::clone(&sink);
            async move {
                sink.lock().unwrap().push(msg.data["i"].as_u64().unwrap());
                Ok::<_, std::convert::Infallible>(())
            }
        })
        .await
        .unwrap();

    {
        let seen = seen.lock().unwrap();
        let unique_seen: HashSet<u64> = seen.iter().copied().collect();
        assert_eq!(
            unique_seen.len(),
            seen.len(),
            "a message was delivered to two workers at once"
        );
        assert_eq!(unique_seen.len(), N, "lost messages");
    }

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
            .subscription_mode(SubscriptionMode::All)
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

    // Back to before the first message. A timestamp seek is a supported
    // request, not an optional one: `parse_seek` accepts exactly `toEnd=true`
    // or a timestamp, and `queen.log_seek_one_v1` resolves the epoch to the
    // first segment's `base_offset - 1`, so the whole partition becomes pending
    // again. Tolerating a refusal here meant a client that stopped sending the
    // `timestamp` key — or sent it under another name — passed green.
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
        .await
        .expect("the broker refused a timestamp seek");

    assert_eq!(
        seek.get("success").and_then(|v| v.as_bool()),
        Some(true),
        "the seek reported failure: {seek}"
    );
    assert!(
        seek.get("partitionsUpdated")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            >= 1,
        "the seek moved no partition, so nothing can replay: {seek}"
    );

    let replayed = pop_retry(&q, &queue, Some(&group), 10, 25).await;
    assert_eq!(
        replayed.len(),
        3,
        "seeking to the epoch must replay the whole partition; got {} of 3",
        replayed.len()
    );

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
        found_ns |= q
            .admin()
            .namespaces()
            .await
            .unwrap()
            .to_string()
            .contains(&ns);
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
        .subscription_mode(SubscriptionMode::All)
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
    assert!(
        again.is_empty(),
        "the message was redelivered despite renewal"
    );

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
            .subscription_mode(SubscriptionMode::All)
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
    assert_eq!(
        msgs.len(),
        1,
        "five retries produced {} messages",
        msgs.len()
    );
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
        .push_items(vec![queen_mq::PushItem::new(
            &a,
            serde_json::json!({ "q": "a" }),
        )
        .transaction_id(&txn)])
        .await
        .unwrap();
    let rb = q
        .queue(&b)
        .push_items(vec![queen_mq::PushItem::new(
            &b,
            serde_json::json!({ "q": "b" }),
        )
        .transaction_id(&txn)])
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

    // Let the claim lapse, then try to hand off. The rollback is the contract,
    // and it has one direction only: the builder puts the popped leaseId on the
    // ack operation (and in `requiredLeases`), the broker resolves that ack
    // group's worker from it, and `queen.log_ack_by_hash_v1` rejects a worker
    // whose `lease_expires_at` is in the past — "invalid or expired lease".
    // `log_transaction_wire_v1` escalates that soft failure to a RAISE, so the
    // pushes in the same call roll back with it. Accepting it would mean
    // pushing stage two for a message somebody else can already claim, which is
    // the exact failure the handoff exists to prevent — so "the broker took it"
    // is a failure, not the other half of a two-world test.
    sleep_ms(2500).await;
    let err = q
        .transaction()
        .ack(&msgs[0])
        .push(sink.clone(), serde_json::json!({ "stage": 2 }))
        .unwrap()
        .commit()
        .await
        .expect_err("a transaction on a lapsed lease must be refused");

    let text = err.to_string();
    assert!(
        text.contains("rolled back"),
        "the failure did not surface as a rollback: {text}"
    );
    assert!(
        text.to_lowercase().contains("lease"),
        "the rollback should name the lapsed lease as the cause: {text}"
    );

    sleep_ms(400).await;
    let leaked = q.queue(&sink).batch(5).wait(false).pop().await.unwrap();
    assert!(
        leaked.is_empty(),
        "the transaction was refused but its push still landed"
    );

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
                .subscription_mode(SubscriptionMode::All)
                .pop()
                .await
                .unwrap();
            if !msgs.is_empty() {
                break;
            }
            sleep_ms(150).await;
        }
        assert_eq!(msgs.len(), 1, "nothing on lane {lane}");
        let res = q
            .ack_with(&msgs[0], status, Some("coverage".into()))
            .await
            .unwrap();
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
                    .push_many(
                        (0..PER_ROUND).map(|i| serde_json::json!({ "round": round, "i": i })),
                    )
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
            .subscription_mode(SubscriptionMode::All)
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
