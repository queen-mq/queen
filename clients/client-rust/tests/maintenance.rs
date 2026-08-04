//! Push and pop maintenance mode.
//!
//! Maintenance is **broker-global**: while it is on, every push goes to the
//! spool and every pop answers paused. So this lives in its own test binary
//! (cargo runs test binaries one at a time, keeping it away from the other
//! suites), and the three scenarios are driven from a *single* `#[test]` rather
//! than three — three test functions in one binary run concurrently by default,
//! and they would fight over the same global switch.
//!
//! Ports `maintenance.js`.

mod common;

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
        let msgs = q
            .queue(&queue)
            .batch(10)
            .wait(false)
            .pop()
            .await
            .unwrap();
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
    let warm = pop_retry(&q, &queue, Some("g-maint"), 1, 25).await;
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
    let back = pop_retry(&q, &queue, Some("g-maint"), 10, 30).await;
    assert!(
        !back.is_empty(),
        "the message did not come back after maintenance ended"
    );

    restore(&q).await;
    drop_queue(&q, &queue).await;
}

async fn maintenance_state_is_readable(q: &queen_mq::Queen) {
    assert_eq!(q.admin().maintenance().await.unwrap().push_paused(), Some(false));
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

    restore(&q).await;
    assert_eq!(q.admin().maintenance().await.unwrap().push_paused(), Some(false));
}
