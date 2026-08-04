//! Push and pop maintenance mode.
//!
//! Maintenance is **broker-global**: while it is on, every push goes to the
//! spool and every pop answers paused. So this lives in its own test binary
//! (cargo runs test binaries one at a time, keeping it away from the other
//! suites), and the four scenarios are driven from a *single* `#[test]` rather
//! than four — four test functions in one binary run concurrently by default,
//! and they would fight over the same global switch.
//!
//! Ports `maintenance.js`.

mod common;

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use queen_mq::{PushStatus, QueueOptions};

use common::*;

/// Put the broker back in service. Called at both ends of every scenario: if
/// one fails mid-flight, the next must not inherit a paused broker.
async fn restore(q: &queen_mq::Queen) {
    let _ = q.admin().set_maintenance(false).await;
    let _ = q.admin().set_pop_maintenance(false).await;
}

#[tokio::test]
async fn maintenance_modes_behave() {
    let q = broker!();
    restore(&q).await;

    maintenance_state_is_readable(&q).await;
    push_maintenance_spools_and_then_replays(&q).await;
    pop_maintenance_pauses_consumers_without_erroring(&q).await;
    live_traffic_crosses_the_window_without_loss_or_duplication(&q).await;

    restore(&q).await;
}

async fn push_maintenance_spools_and_then_replays(q: &queen_mq::Queen) {
    let queue = unique("maint-push");
    create_queue(q, &queue, QueueOptions::default()).await;

    // Baseline: a normal push is queued.
    let before = q
        .queue(&queue)
        .push(serde_json::json!({ "phase": "before" }))
        .await
        .unwrap();
    assert_eq!(before[0].status, PushStatus::Queued);

    let state = q.admin().set_maintenance(true).await.unwrap();
    assert_eq!(
        state.push_paused(),
        Some(true),
        "the broker did not enter push maintenance"
    );

    // Now every push is diverted to the on-disk spool. It is accepted and
    // durable, but it is not in the queue yet — which is exactly why
    // PushStatus::Buffered counts as accepted but is not Queued.
    let during = q
        .queue(&queue)
        .push(serde_json::json!({ "phase": "during" }))
        .await
        .unwrap();
    assert_eq!(
        during[0].status,
        PushStatus::Buffered,
        "a push during maintenance should be spooled, not queued"
    );
    assert!(during[0].status.accepted());

    let state = q.admin().set_maintenance(false).await.unwrap();
    assert_eq!(state.push_paused(), Some(false));

    // The spool drains on its own; both messages must end up in the queue.
    let mut seen = std::collections::HashSet::new();
    for _ in 0..40 {
        let msgs = q.queue(&queue).batch(10).wait(false).pop().await.unwrap();
        for m in &msgs {
            seen.insert(m.data["phase"].as_str().unwrap_or_default().to_string());
        }
        if !msgs.is_empty() {
            q.ack_all(&msgs).await.unwrap();
        }
        if seen.len() >= 2 {
            break;
        }
        sleep_ms(200).await;
    }

    assert!(
        seen.contains("before") && seen.contains("during"),
        "the spooled message never replayed; saw {seen:?}"
    );

    restore(q).await;
    drop_queue(q, &queue).await;
}

async fn pop_maintenance_pauses_consumers_without_erroring(q: &queen_mq::Queen) {
    let queue = unique("maint-pop");
    create_queue(q, &queue, QueueOptions::default()).await;

    q.queue(&queue)
        .push(serde_json::json!({ "n": 1 }))
        .await
        .unwrap();
    // Make sure it really is poppable before we pause.
    let warm = pop_retry(q, &queue, Some("g-maint"), 1, 25).await;
    assert_eq!(warm.len(), 1, "setup: nothing to pop");
    q.nack(&warm[0], "putting it back").await.unwrap();

    let state = q.admin().set_pop_maintenance(true).await.unwrap();
    assert_eq!(
        state.pop_paused(),
        Some(true),
        "the broker did not enter pop maintenance"
    );

    // A paused pop is a 204 carrying {"messages":[],"paused":true}. The client
    // must read that as "nothing right now", not as an error — a consumer loop
    // that treated it as a failure would spin or die during a maintenance
    // window.
    for _ in 0..3 {
        let msgs = q
            .queue(&queue)
            .group("g-maint")
            .batch(10)
            .wait(false)
            .pop()
            .await
            .expect("a paused pop must not surface as an error");
        assert!(msgs.is_empty(), "a paused broker served messages");
    }

    let state = q.admin().set_pop_maintenance(false).await.unwrap();
    assert_eq!(state.pop_paused(), Some(false));

    // Service resumes and the message is still there.
    let back = pop_retry(q, &queue, Some("g-maint"), 10, 30).await;
    assert!(
        !back.is_empty(),
        "the message did not come back after maintenance ended"
    );

    restore(q).await;
    drop_queue(q, &queue).await;
}

/// A producer and a consumer that never stop, with a maintenance window in the
/// middle.
///
/// `push_maintenance_spools_and_then_replays` proves that *one* spooled message
/// comes back. That is not the property an operator is buying: they are buying
/// "I can take the broker down for maintenance under load and my accounting
/// still balances". So this counts. `produced == received` fails in both
/// directions on purpose — a spool that drops frames and a spool that replays
/// them twice are both bugs, and an assertion written as `received >= produced`
/// would call the second one a pass. Ports the accounting of `maintenance.js`.
async fn live_traffic_crosses_the_window_without_loss_or_duplication(q: &queen_mq::Queen) {
    let queue = unique("maint-flow");
    create_queue(
        q,
        &queue,
        QueueOptions {
            // Long enough that no claim can lapse mid-test: a redelivery would
            // show up as a duplicate and blame the spool for the lease.
            lease_time: Some(120),
            ..Default::default()
        },
    )
    .await;

    let stop_producer = Arc::new(AtomicBool::new(false));
    let stop_consumer = Arc::new(AtomicBool::new(false));
    let produced = Arc::new(AtomicU64::new(0));
    let push_failures = Arc::new(AtomicU64::new(0));
    let pop_failures = Arc::new(AtomicU64::new(0));
    let ack_failures = Arc::new(AtomicU64::new(0));
    let received: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

    // ~10 messages a second. `seq` is minted from its own counter, never from
    // the produced total, so a retried batch cannot reuse a number and make a
    // duplicate look like an original.
    let producer = tokio::spawn({
        let q = q.clone();
        let queue = queue.clone();
        let stop = Arc::clone(&stop_producer);
        let produced = Arc::clone(&produced);
        let failures = Arc::clone(&push_failures);
        async move {
            let mut next_seq: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                let batch: Vec<serde_json::Value> = (0..10)
                    .map(|i| serde_json::json!({ "seq": next_seq + i }))
                    .collect();
                next_seq += 10;
                match q.queue(&queue).push_many(batch).await {
                    // Buffered counts as produced: the broker took
                    // responsibility for it, which is the whole promise of the
                    // spool.
                    Ok(results) if results.iter().all(|r| r.status.accepted()) => {
                        produced.fetch_add(results.len() as u64, Ordering::Relaxed);
                    }
                    _ => {
                        failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
                sleep_ms(1000).await;
            }
        }
    });

    let consumer = tokio::spawn({
        let q = q.clone();
        let queue = queue.clone();
        let stop = Arc::clone(&stop_consumer);
        let received = Arc::clone(&received);
        let pop_failures = Arc::clone(&pop_failures);
        let ack_failures = Arc::clone(&ack_failures);
        async move {
            while !stop.load(Ordering::Relaxed) {
                match q.queue(&queue).batch(50).wait(false).pop().await {
                    Ok(msgs) if msgs.is_empty() => sleep_ms(50).await,
                    Ok(msgs) => {
                        if q.ack_all(&msgs).await.is_err() {
                            ack_failures.fetch_add(1, Ordering::Relaxed);
                        }
                        let mut seen = received.lock().unwrap();
                        seen.extend(
                            msgs.iter()
                                .map(|m| m.data["seq"].as_u64().unwrap_or(u64::MAX)),
                        );
                    }
                    Err(_) => {
                        pop_failures.fetch_add(1, Ordering::Relaxed);
                        sleep_ms(100).await;
                    }
                }
            }
        }
    });

    // Flow normally for a moment, so the test is measuring a window and not a
    // cold start.
    sleep_ms(2000).await;

    let before_window = produced.load(Ordering::Relaxed);
    let state = q.admin().set_maintenance(true).await.unwrap();
    assert_eq!(
        state.push_paused(),
        Some(true),
        "the broker did not enter push maintenance"
    );
    sleep_ms(4000).await;
    let spooled = produced.load(Ordering::Relaxed) - before_window;
    let state = q.admin().set_maintenance(false).await.unwrap();
    assert_eq!(state.push_paused(), Some(false));

    stop_producer.store(true, Ordering::Relaxed);
    let _ = producer.await;
    let total = produced.load(Ordering::Relaxed);

    // The spool drains on its own clock; give the consumer time to catch up.
    for _ in 0..150 {
        if received.lock().unwrap().len() as u64 >= total {
            break;
        }
        sleep_ms(200).await;
    }
    stop_consumer.store(true, Ordering::Relaxed);
    let _ = consumer.await;

    let seen = received.lock().unwrap().clone();
    let unique_seqs: HashSet<u64> = seen.iter().copied().collect();
    let errors = format!(
        "push errors {}, pop errors {}, ack errors {}",
        push_failures.load(Ordering::Relaxed),
        pop_failures.load(Ordering::Relaxed),
        ack_failures.load(Ordering::Relaxed),
    );

    // A window nothing was pushed into would make the rest of this vacuous.
    assert!(
        spooled > 0,
        "no push landed inside the maintenance window, so nothing was spooled ({errors})"
    );
    assert_eq!(
        seen.len(),
        unique_seqs.len(),
        "{} message(s) were delivered more than once across the window ({errors})",
        seen.len() - unique_seqs.len()
    );
    assert_eq!(
        seen.len() as u64,
        total,
        "produced {total} ({spooled} of them into the spool) but received {} ({errors})",
        seen.len()
    );

    restore(q).await;
    drop_queue(q, &queue).await;
}

async fn maintenance_state_is_readable(q: &queen_mq::Queen) {
    assert_eq!(
        q.admin().maintenance().await.unwrap().push_paused(),
        Some(false)
    );
    assert_eq!(
        q.admin().pop_maintenance().await.unwrap().pop_paused(),
        Some(false)
    );

    q.admin().set_maintenance(true).await.unwrap();
    let state = q.admin().maintenance().await.unwrap();
    assert_eq!(
        state.push_paused(),
        Some(true),
        "push maintenance did not read back as on"
    );
    // The GET reports both knobs, and they are independent.
    assert_eq!(
        state.pop_paused(),
        Some(false),
        "enabling push maintenance also paused pops"
    );

    restore(q).await;
    assert_eq!(
        q.admin().maintenance().await.unwrap().push_paused(),
        Some(false)
    );
}
