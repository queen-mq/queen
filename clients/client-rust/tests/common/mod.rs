//! Shared harness for the integration suite.
//!
//! These tests need a live broker. `QUEEN_TEST_URL` points at it; the Docker
//! runner sets it, and `test/run.sh` brings the stack up.
//!
//! With the variable unset every test *skips* with a notice rather than
//! failing, so `cargo test` on a laptop still exercises the unit tests. With it
//! set, a broker that does not answer is a hard failure — a silent pass would
//! be worse than a red run.
//!
//! `QUEEN_TEST_STRICT=1` closes the loop: it turns the skip itself into a
//! failure, so a runner that loses `QUEEN_TEST_URL` reports a red suite rather
//! than 100-odd tests that never ran. CI and the Docker runner both set it.

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use queen_mq::{Config, Message, Queen, QueueOptions, SubscriptionMode};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// The broker URL, or `None` when the suite should skip.
///
/// An empty value counts as missing: `export QUEEN_TEST_URL="$QUEEN_HTTP_URL"`
/// in a runner whose `QUEEN_HTTP_URL` is empty would otherwise hand every test
/// a URL that cannot connect, one failure at a time, instead of saying so once.
fn broker_url() -> Option<String> {
    match std::env::var("QUEEN_TEST_URL") {
        Ok(u) if !u.trim().is_empty() => Some(u),
        _ => {
            enforce_strict();
            None
        }
    }
}

/// In a real run the absence of a broker is a hard failure.
///
/// `QUEEN_TEST_STRICT` is set by `test/runners/rust-client/entrypoint.sh` and
/// by CI. Without it, `broker!()` returns early and cargo counts the test as
/// passed — which is what we want on a laptop with no stack, and exactly what
/// we must never ship as a green CI cell. Silently skipping the whole
/// integration suite is indistinguishable from passing it.
fn enforce_strict() {
    let strict = std::env::var("QUEEN_TEST_STRICT")
        .map(|v| !v.is_empty() && v != "0")
        .unwrap_or(false);
    assert!(
        !strict,
        "QUEEN_TEST_STRICT is set but QUEEN_TEST_URL is missing or empty. \
         The integration suite would have skipped every test and reported success."
    );
}

/// The broker under test, or `None` when the suite should skip.
pub fn client() -> Option<Queen> {
    let url = broker_url()?;
    Some(Queen::connect(Config::new(url)).expect("QUEEN_TEST_URL is not a usable broker URL"))
}

/// A client with a bearer token attached.
pub fn client_with_token(token: &str) -> Option<Queen> {
    let url = broker_url()?;
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

// There is no second fixture for KV and timers, and there is deliberately no
// `QUEEN_TEST_KVT`.
//
// There used to be both: the surfaces were gated at boot by `QUEEN_KV_ENABLED`
// and `QUEEN_TIMERS_ENABLED`, a cell with them off did not register the routes
// at all, and `kvt_broker!()` probed for a 404 so the suite could skip. Those
// flags are gone (Alice, 2026-08-18): kv and timers are part of the engine, like
// push and pop, and every broker that answers `QUEEN_TEST_URL` has them. So the
// kv/timer tests take `broker!()` like everything else and simply run.
//
// This is not merely one env var fewer. A skipping test asserts nothing while
// reporting green, and that was tolerable only while a 404 was a legitimate
// configuration. It no longer is, so a kv call that fails is now a failure —
// including the runtime kill switch (503 `kv_disabled`), which is an operator
// having paused a live surface on the cell under test, not a shape of broker the
// suite is expected to tiptoe around.

/// The prefix every key written by this suite carries, so [`purge_kv`] can
/// enumerate them with the one read that is allowed to.
pub const TEST_KEY_PREFIX: &str = "t:";

/// Purge every key of a namespace.
///
/// **Mandatory, and not cosmetic.** Without it a `putIfAbsent` test is green on
/// its first run and red for ever after — the marker it was checking is still
/// there — and an `incr` test accumulates across runs until it crosses whatever
/// ceiling it was asserting. Best-effort throughout: the namespace may not
/// exist, the surface may not exist, and neither may mask the test's own result.
///
/// There is deliberately no `deletePrefix` on this product (the TTL is
/// mandatory, so the sweeper does that work), which is why this enumerates.
///
/// `prefix` is not optional and cannot be empty: a namespace is not a table to
/// enumerate, and the broker refuses an empty prefix rather than scanning. Every
/// test in this suite therefore names its keys under [`TEST_KEY_PREFIX`].
pub async fn purge_kv(queen: &Queen, ns: &str, prefix: &str) {
    let mut after: Option<String> = None;
    for _ in 0..50 {
        let mut q = queen.kv().get_prefix(ns, prefix).limit(1000).keys_only();
        if let Some(a) = &after {
            q = q.after(a);
        }
        let page = match q.send().await {
            Ok(p) => p,
            Err(_) => return,
        };
        if page.rows().is_empty() {
            return;
        }
        for row in page.rows() {
            let _ = queen.kv().delete(ns, &row.key).send().await;
        }
        match page.next_after.clone() {
            Some(cursor) if page.truncated() => after = Some(cursor),
            _ => return,
        }
    }
}

/// Cancel every pending timer of a queue, so a rerun does not meet its own.
pub async fn purge_timers(queen: &Queen, queue: &str) {
    for _ in 0..50 {
        let page = match queen.timers().list(queue).limit(1000).send().await {
            Ok(p) => p,
            Err(_) => return,
        };
        if page.rows.is_empty() {
            return;
        }
        for row in &page.rows {
            let _ = queen.timers().cancel(queue, &row.timer_key).await;
        }
        if !page.truncated {
            return;
        }
    }
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
///
/// A named group asks for `all` explicitly. The callers push first and expect
/// that backlog back, which the broker's own default (`new`, seeding the cursor
/// at the tail) does not give a group meeting the queue for the first time. The
/// mode is ignored for a group that already has a cursor, and group-less pops
/// are pinned to `all` by the broker regardless.
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
            b = b.group(g).subscription_mode(SubscriptionMode::All);
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
            // The sink usually holds messages before the first drain, and this
            // group has never met it.
            .subscription_mode(SubscriptionMode::All)
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
