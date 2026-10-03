//! The health verdict (plan §6.8): what [`crate::Sink::status`] reports as
//! `health`, for the broker to serve however it serves its own.
//!
//! THE ONE THING THE VERDICT ANSWERS, and it is the plan's own failure policy:
//! **the sink never drops, it only lags** (§6.7). So there is no "degraded" and
//! no per-subsystem tree: the verdict is red when a queue this node runs is
//! further behind than `3 × MAX_WINDOW_MS` (30 s at the least), and green
//! otherwise. A bucket that refuses every PUT, a KV write that keeps failing, a
//! stalled read: all of them arrive here as the same symptom, because all of
//! them have the same consequence.
//!
//! **Behind** is the queue's lag, `safeTime − completeThrough`
//! ([`crate::window::Engine::complete_through`]): how far the stamp below which
//! the lake is complete trails this node's applied log. Not the age of the last
//! commit — a queue nobody writes to commits nothing, and its lake is still
//! complete up to the newest stamp — so an idle or caught-up queue stays green,
//! and a queue with records it cannot ship goes red.
//!
//! The queue task reports its lag after every round of its protocol. Between
//! two reports the lag is taken to grow with this node's monotonic clock: a
//! task stuck inside one step — an upload a bucket keeps refusing — reports
//! nothing, and the verdict must still see it fall behind. A queue that has not
//! reported a lag yet counts as behind since it started here.
//!
//! `3 ×` and not `1 ×`: a window closes at `MAX_WINDOW_MS`, then has to be
//! uploaded and committed, and the guard keeps the close below `safeTime`. A
//! one-window threshold would go red on a healthy sink under load, and a probe
//! that flaps is a probe an operator turns off.
//!
//! A backfill — `QUEEN_S3_START=earliest` over a long log, or a sink that was
//! stopped for a while — is red until it is back inside the budget: its lake
//! IS that far behind, and the lag says by how much.
//!
//! Which queues count: the ones this node runs. The driver registers a queue
//! when it starts it and forgets it when it stops for a reason that ends this
//! node's part — drained, fenced, held by another node, a queue that does not
//! exist. A queue stopped by a failure an operator must fix (a bucket refusing
//! writes) stays, with its last report getting older: this node goes red for it
//! until it runs the queue again or learns that another node holds it.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use tokio::time::Instant;

use crate::obs::{read_lock, write_lock, Metrics};
use crate::types::Micros;

/// How many `MAX_WINDOW_MS` a queue may lag before the verdict turns red. See
/// the module header for why it is three.
pub const STALE_WINDOWS: i64 = 3;

/// The floor on the budget, so a sink configured with a very small
/// `MAX_WINDOW_MS` does not have a verdict that goes red between two rounds.
pub const MIN_STALE_MS: i64 = 30_000;

/// What the verdict decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Every queue this node runs is inside its budget.
    Healthy { queues: usize },
    /// One queue is not. The FIRST one, by name, so the line is stable across
    /// reads rather than naming a different victim each time. `stale_ms` is its
    /// lag as of the verdict: the last reported lag plus the time since.
    Stale {
        queue: String,
        stale_ms: i64,
        limit_ms: i64,
    },
}

impl Verdict {
    pub fn is_healthy(&self) -> bool {
        matches!(self, Verdict::Healthy { .. })
    }

    /// The verdict as one JSON object: `{"ok":true,"queues":N}` or
    /// `{"ok":false,"queue":…,"staleMs":…,"limitMs":…}`.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Verdict::Healthy { queues } => serde_json::json!({"ok": true, "queues": queues}),
            Verdict::Stale {
                queue,
                stale_ms,
                limit_ms,
            } => serde_json::json!({
                "ok": false,
                "queue": queue,
                "staleMs": stale_ms,
                "limitMs": limit_ms,
            }),
        }
    }

    /// The same object as one line of text, `ok` first — the body a probe
    /// route answers.
    pub fn body(&self) -> String {
        match self {
            Verdict::Healthy { queues } => {
                format!("{{\"ok\":true,\"queues\":{queues}}}")
            }
            Verdict::Stale {
                queue,
                stale_ms,
                limit_ms,
            } => format!(
                "{{\"ok\":false,\"queue\":{},\"staleMs\":{stale_ms},\"limitMs\":{limit_ms}}}",
                serde_json::Value::String(queue.clone())
            ),
        }
    }

    /// The HTTP status a probe route answers the verdict with: 200 or 503.
    pub fn http_status(&self) -> u16 {
        match self {
            Verdict::Healthy { .. } => 200,
            Verdict::Stale { .. } => 503,
        }
    }
}

/// One queue this node runs, as the verdict sees it.
#[derive(Clone, Copy, Debug)]
struct Watch {
    /// When this node started running the queue.
    since: Instant,
    /// The lag, in milliseconds, at the queue's last report, and when that was.
    last: Option<(i64, Instant)>,
}

impl Watch {
    /// The lag as of `now`: the last report grown by the time since it, or the
    /// whole time since the start when there has been none.
    fn lag_ms(&self, now: Instant) -> i64 {
        let since = |t: Instant| now.saturating_duration_since(t).as_millis() as i64;
        match self.last {
            Some((lag_ms, at)) => lag_ms.saturating_add(since(at)),
            None => since(self.since),
        }
    }
}

/// Everything the verdict reads. Shared by `Arc`, written by the per-queue
/// tasks and read by whoever asks.
///
/// Durations are measured on [`tokio::time::Instant`]: monotonic, so a wall
/// clock stepped by NTP moves no verdict, and paused in the crate's tests like
/// every other timer of the sink.
pub struct HealthState {
    queues: RwLock<BTreeMap<String, Watch>>,
    max_window_ms: RwLock<u64>,
    metrics: Arc<Metrics>,
}

impl HealthState {
    pub fn new(metrics: Arc<Metrics>, max_window_ms: u64) -> HealthState {
        HealthState {
            queues: RwLock::new(BTreeMap::new()),
            max_window_ms: RwLock::new(max_window_ms),
            metrics,
        }
    }

    /// Start watching a queue this node now runs. Idempotent: a queue started
    /// again here keeps what it had — a lag that kept growing while it was
    /// stopped by a failure is still that lag.
    pub fn register_queue(&self, queue: &str) {
        self.register_queue_at(queue, Instant::now());
    }

    pub fn register_queue_at(&self, queue: &str, at: Instant) {
        write_lock(&self.queues)
            .entry(queue.to_string())
            .or_insert(Watch {
                since: at,
                last: None,
            });
    }

    /// Stop watching a queue — this node no longer runs it, and it must not
    /// make THIS node's verdict red.
    pub fn forget_queue(&self, queue: &str) {
        write_lock(&self.queues).remove(queue);
    }

    /// The queue's lag after a round of its protocol: `safeTime −
    /// completeThrough` on the broker's clock, or `None` while it is unknown.
    /// Registers the queue if it was not.
    pub fn report_lag(&self, queue: &str, lag: Option<Micros>) {
        self.report_lag_at(queue, lag, Instant::now());
    }

    pub fn report_lag_at(&self, queue: &str, lag: Option<Micros>, at: Instant) {
        let mut q = write_lock(&self.queues);
        let w = q.entry(queue.to_string()).or_insert(Watch {
            since: at,
            last: None,
        });
        let Some(lag) = lag else { return };
        // Monotone in time: a report from a task that was descheduled must not
        // replace a newer one.
        if w.last.is_none_or(|(_, prev)| at >= prev) {
            w.last = Some((lag.0.max(0) / 1_000, at));
        }
    }

    pub fn set_max_window_ms(&self, ms: u64) {
        *write_lock(&self.max_window_ms) = ms;
    }

    /// The budget: `3 × MAX_WINDOW_MS`, floored.
    pub fn limit_ms(&self) -> i64 {
        let window = *read_lock(&self.max_window_ms) as i64;
        (window.saturating_mul(STALE_WINDOWS)).max(MIN_STALE_MS)
    }

    /// The verdict now.
    pub fn verdict(&self) -> Verdict {
        self.verdict_at(Instant::now())
    }

    /// The verdict as of `now`.
    pub fn verdict_at(&self, now: Instant) -> Verdict {
        let limit_ms = self.limit_ms();
        let q = read_lock(&self.queues);
        for (queue, w) in q.iter() {
            let stale_ms = w.lag_ms(now);
            if stale_ms > limit_ms {
                return Verdict::Stale {
                    queue: queue.clone(),
                    stale_ms,
                    limit_ms,
                };
            }
        }
        Verdict::Healthy { queues: q.len() }
    }

    /// The queues the verdict currently counts.
    pub fn queues(&self) -> usize {
        read_lock(&self.queues).len()
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn state(max_window_ms: u64) -> Arc<HealthState> {
        Arc::new(HealthState::new(Arc::new(Metrics::new()), max_window_ms))
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn lag(seconds: i64) -> Option<Micros> {
        Some(Micros(seconds * 1_000_000))
    }

    #[test]
    fn no_queues_at_all_is_healthy() {
        assert!(state(300_000).verdict().is_healthy());
    }

    #[test]
    fn the_budget_is_three_windows_with_a_floor() {
        assert_eq!(state(300_000).limit_ms(), 900_000);
        assert_eq!(state(1_000).limit_ms(), MIN_STALE_MS, "the floor holds");
        let s = state(300_000);
        s.set_max_window_ms(60_000);
        assert_eq!(s.limit_ms(), 180_000);
    }

    /// An idle queue reports a small lag every round, for as long as it runs:
    /// green for ever, however long since its last commit.
    #[test]
    fn a_queue_that_keeps_reporting_a_small_lag_stays_green() {
        let s = state(10_000);
        let t0 = Instant::now();
        s.register_queue_at("orders", t0);
        for round in 0..1_000u64 {
            let at = t0 + ms(round * 2_000);
            s.report_lag_at("orders", lag(2), at);
            assert!(s.verdict_at(at + ms(1_000)).is_healthy(), "round {round}");
        }
    }

    /// The lag is the verdict: inside the budget green, past it red, and the
    /// verdict names the queue and says by how much.
    #[test]
    fn a_lag_past_the_budget_is_red() {
        let s = state(300_000);
        let t0 = Instant::now();
        s.report_lag_at("orders", lag(900), t0);
        assert!(s.verdict_at(t0).is_healthy(), "exactly at the limit");
        s.report_lag_at("orders", lag(901), t0 + ms(1));
        assert_eq!(
            s.verdict_at(t0 + ms(1)),
            Verdict::Stale {
                queue: "orders".into(),
                stale_ms: 901_000,
                limit_ms: 900_000
            }
        );
    }

    /// A task stuck inside one step reports nothing: its last lag grows with
    /// the clock, and the queue goes red once that crosses the budget.
    #[test]
    fn a_queue_that_stops_reporting_falls_behind_with_the_clock() {
        let s = state(10_000);
        let t0 = Instant::now();
        s.report_lag_at("orders", lag(5), t0);
        assert!(s.verdict_at(t0 + ms(25_000)).is_healthy(), "5 s + 25 s");
        match s.verdict_at(t0 + ms(25_001)) {
            Verdict::Stale {
                queue, stale_ms, ..
            } => {
                assert_eq!(queue, "orders");
                assert_eq!(stale_ms, 30_001);
            }
            other => panic!("expected stale, got {other:?}"),
        }
        // Reporting again — a small lag — is green again at once.
        s.report_lag_at("orders", lag(1), t0 + ms(40_000));
        assert!(s.verdict_at(t0 + ms(40_000)).is_healthy());
    }

    /// A queue whose lag is not known yet counts as behind since it started
    /// here: a sink that cannot establish where its lake is complete for three
    /// windows is not healthy.
    #[test]
    fn a_queue_with_no_lag_yet_counts_from_its_start() {
        let s = state(10_000);
        let t0 = Instant::now();
        s.register_queue_at("orders", t0);
        s.report_lag_at("orders", None, t0 + ms(10_000));
        assert!(s.verdict_at(t0 + ms(30_000)).is_healthy());
        assert!(!s.verdict_at(t0 + ms(30_001)).is_healthy());
    }

    #[test]
    fn the_verdict_names_the_first_stale_queue_by_name_every_time() {
        let s = state(300_000);
        let t0 = Instant::now();
        for q in ["zeta", "alpha", "mid"] {
            s.report_lag_at(q, lag(10_000), t0);
        }
        for _ in 0..5 {
            match s.verdict_at(t0) {
                Verdict::Stale { queue, .. } => assert_eq!(queue, "alpha"),
                other => panic!("expected stale, got {other:?}"),
            }
        }
    }

    #[test]
    fn reports_are_monotone_and_a_forgotten_queue_stops_counting() {
        let s = state(300_000);
        let t0 = Instant::now();
        s.report_lag_at("orders", lag(1), t0 + ms(5_000));
        // A task that was descheduled and reports an older lag must not
        // replace the newer report.
        s.report_lag_at("orders", lag(5_000), t0);
        assert!(s.verdict_at(t0 + ms(5_000)).is_healthy());

        // A queue whose lease went to another instance stops being this
        // process's problem.
        s.report_lag_at("clicks", lag(5_000), t0);
        assert!(!s.verdict_at(t0 + ms(5_000)).is_healthy());
        s.forget_queue("clicks");
        assert_eq!(s.verdict_at(t0 + ms(5_000)), Verdict::Healthy { queues: 1 });
        assert_eq!(s.queues(), 1);
    }

    #[test]
    fn registering_a_queue_twice_keeps_its_history() {
        let s = state(10_000);
        let t0 = Instant::now();
        s.report_lag_at("orders", lag(29), t0);
        s.register_queue_at("orders", t0 + ms(5_000));
        assert!(
            !s.verdict_at(t0 + ms(5_000)).is_healthy(),
            "the lag it had kept growing while it was stopped"
        );
    }

    #[test]
    fn the_verdict_renders_as_one_line_and_a_status() {
        let s = state(300_000);
        let t0 = Instant::now();
        assert_eq!(s.verdict_at(t0).body(), "{\"ok\":true,\"queues\":0}");
        assert_eq!(s.verdict_at(t0).http_status(), 200);

        s.report_lag_at("orders", lag(3_600), t0);
        let v = s.verdict_at(t0);
        assert_eq!(v.http_status(), 503);
        let body = v.body();
        assert!(
            body.starts_with("{\"ok\":false,\"queue\":\"orders\""),
            "{body}"
        );
        assert!(!body.contains('\n'), "{body}");
        let json = v.to_json();
        assert_eq!(json["queue"], "orders");
        assert_eq!(json["ok"], false);
        assert_eq!(json["staleMs"], 3_600_000);
        assert_eq!(json["limitMs"], 900_000);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&body).unwrap(),
            json,
            "the line and the object are one verdict"
        );
    }
}
