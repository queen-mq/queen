//! Reporting and deadlines.
//!
//! Two rules the rest of the suite is built on:
//!
//! 1. **One line per assertion.** `  ok   …` or `  FAIL …`, section headers as
//!    `=== …`, and a single `RESULT: PASS` / `RESULT: FAIL (n)` at the end.
//!    The same shape every other row of `compat/` prints, so a rig run reads as
//!    one document.
//! 2. **A hang is a result.** Every blocking call in this suite goes through a
//!    deadline. A suite that waits forever for a broker that will never answer
//!    reports nothing, which is strictly worse than reporting a timeout — and
//!    librdkafka is very good at waiting forever (its default
//!    `socket.timeout.ms` only bounds one request, not a metadata refresh loop
//!    that keeps getting a retriable code).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static FAILURES: AtomicUsize = AtomicUsize::new(0);
static CHECKS: AtomicUsize = AtomicUsize::new(0);

pub fn section(title: &str) {
    println!("\n=== {title}");
}

pub fn ok(what: impl AsRef<str>) {
    CHECKS.fetch_add(1, Ordering::Relaxed);
    println!("  ok   {}", what.as_ref());
}

pub fn fail(what: impl AsRef<str>) {
    CHECKS.fetch_add(1, Ordering::Relaxed);
    FAILURES.fetch_add(1, Ordering::Relaxed);
    println!("  FAIL {}", what.as_ref());
}

/// A note: something worth printing that is not a pass/fail judgement. Used for
/// the deliberate deviations (`PLAN_QUEEN_KAFKA.md` STATUS) and for behaviour
/// that belongs to the CLIENT rather than to the facade.
pub fn info(what: impl AsRef<str>) {
    println!("  ..   {}", what.as_ref());
}

pub fn check(cond: bool, what: impl AsRef<str>) -> bool {
    if cond {
        ok(what);
    } else {
        fail(what);
    }
    cond
}

pub fn check_eq<T: PartialEq + std::fmt::Debug>(got: T, want: T, what: &str) -> bool {
    if got == want {
        ok(format!("{what} ({got:?})"));
        true
    } else {
        fail(format!("{what}: got {got:?}, want {want:?}"));
        false
    }
}

pub fn failures() -> usize {
    FAILURES.load(Ordering::Relaxed)
}

pub fn checks() -> usize {
    CHECKS.load(Ordering::Relaxed)
}

/// Run a future under a deadline. A timeout is a FAIL with the label, never a
/// silent stall.
pub async fn deadline<T>(
    secs: u64,
    what: &str,
    fut: impl std::future::Future<Output = T>,
) -> Option<T> {
    match tokio::time::timeout(Duration::from_secs(secs), fut).await {
        Ok(v) => Some(v),
        Err(_) => {
            fail(format!("TIMED OUT after {secs}s: {what}"));
            None
        }
    }
}

/// The blocking half of the same idea: librdkafka's metadata, watermark and
/// committed-offset calls are synchronous C calls with their own timeout
/// argument, so they get the timeout passed in rather than wrapped.
pub const SHORT: Duration = Duration::from_secs(10);
pub const LONG: Duration = Duration::from_secs(30);
