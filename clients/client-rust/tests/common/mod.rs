//! Shared harness for the integration suite.
//!
//! These tests need a live broker. `QUEEN_TEST_URL` points at it; the Docker
//! runner sets it, and `test/run.sh` brings the stack up.
//!
//! With the variable unset every test *skips* with a notice rather than
//! failing, so `cargo test` on a laptop still exercises the unit tests. With it
//! set, a broker that does not answer is a hard failure — a silent pass would
//! be worse than a red run.

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use queen_mq::{Config, Message, Queen, QueueOptions};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// The broker under test, or `None` when the suite should skip.
pub fn client() -> Option<Queen> {
    let url = std::env::var("QUEEN_TEST_URL").ok()?;
    Some(Queen::connect(Config::new(url)).expect("QUEEN_TEST_URL is not a usable broker URL"))
}

/// A client with a bearer token attached.
pub fn client_with_token(token: &str) -> Option<Queen> {
    let url = std::env::var("QUEEN_TEST_URL").ok()?;
    Some(Queen::connect(Config::new(url).bearer_token(token)).expect("bad test config"))
}

/// Announce a skip once, from the test that wanted a broker.
pub fn skipped(test: &str) {
    eprintln!("SKIP {test}: QUEEN_TEST_URL is not set");
}

/// Grab the client or skip the test.
#[macro_export]
macro_rules! broker {
    () => {
        match $crate::common::client() {
            Some(c) => c,
            None => {
                $crate::common::skipped(concat!(module_path!(), "::", "test"));
                return;
            }
        }
    };
}

/// A queue name no other test (or run) will collide with.
pub fn unique(prefix: &str) -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("test-rs-{prefix}-{ms}-{n}")
}

pub async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

/// Poll until messages appear.
///
/// A push is not instantly poppable: fusion holds frames briefly to batch the
/// write, and a queue with `windowBuffer` holds them deliberately. Every test
/// that pushes then pops has to ride that out or it is timing-flaky.
pub async fn pop_retry(
    queen: &Queen,
    queue: &str,
    group: Option<&str>,
    batch: i32,
    tries: usize,
) -> Vec<Message> {
    for _ in 0..tries {
        let mut b = queen.queue(queue).batch(batch).wait(false);
        if let Some(g) = group {
            b = b.group(g);
        }
        let msgs = b.pop().await.expect("pop failed");
        if !msgs.is_empty() {
            return msgs;
        }
        sleep_ms(150).await;
    }
    Vec::new()
}

/// Create a queue with the given options, asserting the broker confirmed it.
pub async fn create_queue(queen: &Queen, queue: &str, options: QueueOptions) {
    let res = queen
        .queue(queue)
        .configure(options)
        .await
        .unwrap_or_else(|e| panic!("could not create {queue}: {e}"));
    assert_eq!(
        res.get("configured").and_then(|v| v.as_bool()),
        Some(true),
        "broker did not confirm queue creation: {res}"
    );
}

/// A short lease, so a test can watch a claim expire without sleeping for the
/// 300-second default.
pub fn short_lease(seconds: i32) -> QueueOptions {
    QueueOptions {
        lease_time: Some(seconds),
        ..Default::default()
    }
}

/// Best-effort teardown. A failure here must never mask the test's own result.
pub async fn drop_queue(queen: &Queen, queue: &str) {
    let _ = queen.queue(queue).delete().await;
}

/// Drain a sink queue until `done` accepts the accumulated messages, or the
/// deadline passes.
///
/// Returns whatever arrived either way, so a test can assert on a partial
/// result rather than just timing out with no diagnosis.
pub async fn drain_until<F>(
    queen: &Queen,
    queue: &str,
    timeout: Duration,
    mut done: F,
) -> Vec<Message>
where
    F: FnMut(&[Message]) -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    let mut out: Vec<Message> = Vec::new();
    while std::time::Instant::now() < deadline {
        let msgs = queen
            .queue(queue)
            .group("drain")
            .batch(100)
            .partitions(8)
            .wait(false)
            .pop()
            .await
            .expect("drain pop failed");
        if msgs.is_empty() {
            if done(&out) {
                return out;
            }
            sleep_ms(120).await;
            continue;
        }
        queen.ack_all(&msgs).await.expect("drain ack failed");
        out.extend(msgs);
        if done(&out) {
            return out;
        }
    }
    out
}

/// Sum a numeric field across drained messages.
pub fn sum_field(messages: &[Message], field: &str) -> f64 {
    messages
        .iter()
        .filter_map(|m| m.data.get(field).and_then(|v| v.as_f64()))
        .sum()
}

/// How many messages are in a queue's DLQ.
pub async fn dlq_count(queen: &Queen, queue: &str) -> usize {
    queen
        .queue(queue)
        .dlq(Some(50), None)
        .await
        .map(|r| r.messages.len())
        .unwrap_or(0)
}
