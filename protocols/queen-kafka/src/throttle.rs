//! Queen's back-pressure, said in the one language every Kafka client already
//! obeys: `throttle_time_ms`.
//!
//! ## The mapping, in one table
//!
//! A Cloud tenant that is frozen or over its rate cap gets a `429` from the
//! proxy, with a `Retry-After` naming when to come back (proxy/src/errors.rs
//! `err_429`; the seconds are computed and clamped to 1..=60 in
//! proxy/src/limits.rs). Kafka has a field for exactly that, on the three
//! responses this facade serves in the hot path:
//!
//! | API      | `throttle_time_ms` | the partitions beside it                |
//! | -------- | ------------------ | --------------------------------------- |
//! | Produce  | Retry-After        | `REQUEST_TIMED_OUT` — retriable         |
//! | Fetch    | Retry-After        | empty, error 0, watermarks `-1`         |
//! | Metadata | Retry-After        | `LEADER_NOT_AVAILABLE` — retriable      |
//!
//! Every Kafka client backs off on `throttle_time_ms` natively and without
//! surfacing anything to the application: the Java client mutes the node for
//! that long (`NetworkClient.throttle`), librdkafka delays the next request on
//! the broker handle. That is the whole point of mapping it rather than
//! inventing a refusal — the client already knows how to wait.
//!
//! ## Why the codes beside it are these
//!
//! **Produce** used to answer `THROTTLING_QUOTA_EXCEEDED` (89), which reads
//! right and is wrong. It is retriable in the Java client, and it is NOT on
//! librdkafka's produce-retry list — that list is `REQUEST_TIMED_OUT`,
//! `NOT_ENOUGH_REPLICAS(_AFTER_APPEND)`, `KAFKA_STORAGE_ERROR`,
//! `LEADER_NOT_AVAILABLE`, `NOT_LEADER_FOR_PARTITION`,
//! `UNKNOWN_TOPIC_OR_PARTITION` — so on librdkafka (and therefore on the
//! Confluent Python, C# and JS clients) a rate-capped batch became a permanent
//! delivery failure raised to the application. `REQUEST_TIMED_OUT` is retriable
//! in every client's produce path, and it is the code this handler already uses
//! for a push whose fate it does not know.
//!
//! **Fetch** answers the partition with no records, error code 0 and the
//! watermarks at `-1`. A consumer reads a partition's watermarks only when they
//! are non-negative (`CompletedFetch.initializeCompletedFetch` guards on `>= 0`)
//! so nothing is corrupted by not knowing them, and error 0 keeps the throttle
//! from also costing a metadata refresh — which under a rate cap would be one
//! more call the tenant cannot afford. The consumer sleeps and polls again,
//! which is exactly what a throttle means.
//!
//! **Metadata** already answers `LEADER_NOT_AVAILABLE` per topic when the queue
//! list cannot be read (`handlers::metadata`), which is retriable and is the
//! code a client expects while a leader is being established. Nothing about it
//! changes; the throttle is added beside it so the retry waits.
//!
//! ## What is deliberately NOT mapped
//!
//! The offset APIs. OffsetCommit and OffsetFetch have a `throttle_time_ms` too,
//! and a 429 there already becomes `COORDINATOR_NOT_AVAILABLE`
//! ([`crate::offsets`]) — retriable, and answered by the client re-finding its
//! coordinator on its own backoff. Adding the field would be free; acting on it
//! would not, because a consumer that sleeps its commit is a consumer that
//! sleeps its whole poll loop. The throttle belongs on the calls whose VOLUME is
//! what the cap is about, which is the data path.

use crate::queen;

/// The throttle to answer when Queen said "later" without saying when.
///
/// A second is the proxy's own floor (`retry_after_s` is clamped to `1..=60`,
/// proxy/src/limits.rs), so a facade that cannot read a hint answers the
/// smallest thing the hint could have been — never longer than the tenant was
/// actually asked to wait.
pub const DEFAULT_MS: i32 = 1_000;

/// Ceiling on any throttle this facade reports.
///
/// A client MUTES its connection for this long, and every Kafka client's own
/// request timeout is 30 s by default — a throttle above that is a client that
/// times out its request instead of waiting, which is strictly worse than a
/// shorter throttle it actually honours. The proxy's own ceiling is 60 s and
/// this clamps below it on purpose: two 30-second waits a client obeys beat one
/// 60-second wait it abandons.
pub const MAX_MS: i32 = 30_000;

/// The `throttle_time_ms` this failure asks a client to wait, or `None` when it
/// is not back-pressure at all.
///
/// Only a `429` is a throttle. A `503` from a draining broker is not: it has no
/// budget attached, the tenant is not over anything, and telling a client to
/// sleep through a failover it could have retried past immediately would turn a
/// blip into a stall.
pub fn for_error(e: &queen::Error) -> Option<i32> {
    match e {
        queen::Error::Status { code: 429, .. } => Some(millis(e.retry_after_ms())),
        _ => None,
    }
}

/// The longer of two throttles. One request can touch several failures — a
/// produce spanning two topics, a fetch chunked over several calls — and the
/// answer carries ONE number, so it has to be the one that satisfies all of
/// them.
pub fn longest(a: Option<i32>, b: Option<i32>) -> Option<i32> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

/// A `Retry-After` in milliseconds, clamped into what a client will honour.
fn millis(retry_after_ms: Option<i64>) -> i32 {
    match retry_after_ms {
        // Zero is a hint that says "immediately", and a throttle of 0 is no
        // throttle at all — which would leave the client retrying flat out
        // against a cap it has already hit. The floor is the default.
        Some(ms) => ms.clamp(i64::from(DEFAULT_MS), i64::from(MAX_MS)) as i32,
        None => DEFAULT_MS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_429_is_a_throttle() {
        assert_eq!(
            for_error(&queen::Error::status(429, "rate_limited")),
            Some(DEFAULT_MS),
            "a 429 with no hint still throttles"
        );
        for e in [
            queen::Error::status(503, "draining"),
            queen::Error::status(403, "forbidden"),
            queen::Error::status(500, "boom"),
            queen::Error::Transport("connection refused".into()),
            queen::Error::Body("not json".into()),
        ] {
            assert_eq!(for_error(&e), None, "{e} was read as a throttle");
        }
    }

    #[test]
    fn the_hint_is_honoured_and_clamped() {
        let throttled = |ms: Option<i64>| {
            for_error(&queen::Error::Status {
                code: 429,
                body: "rate_limited".into(),
                retry_after_ms: ms,
            })
        };
        assert_eq!(throttled(Some(5_000)), Some(5_000));
        // The proxy's own floor and ceiling, either side of ours.
        assert_eq!(throttled(Some(1_000)), Some(DEFAULT_MS));
        assert_eq!(throttled(Some(60_000)), Some(MAX_MS));
        // Nothing a client would not honour: a zero, a negative that survived
        // the parse, and an absurd one all land inside the window.
        assert_eq!(throttled(Some(0)), Some(DEFAULT_MS));
        assert_eq!(throttled(Some(-30)), Some(DEFAULT_MS));
        assert_eq!(throttled(Some(i64::MAX)), Some(MAX_MS));
        assert_eq!(throttled(None), Some(DEFAULT_MS));
    }

    #[test]
    fn one_response_carries_the_longest_wait_it_was_told() {
        assert_eq!(longest(Some(1_000), Some(9_000)), Some(9_000));
        assert_eq!(longest(Some(9_000), Some(1_000)), Some(9_000));
        assert_eq!(longest(None, Some(1_000)), Some(1_000));
        assert_eq!(longest(Some(1_000), None), Some(1_000));
        assert_eq!(longest(None, None), None);
    }
}
