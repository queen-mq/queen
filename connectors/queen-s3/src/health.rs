//! `/healthz` and `/metrics` on `QUEEN_S3_LISTEN` (plan §6.8).
//!
//! THE ONE THING `/healthz` ANSWERS, and it is the plan's own failure policy:
//! **the sink never drops, it only lags** (§6.7). So there is no "degraded" and
//! no per-subsystem tree — the probe is green while every queue this process
//! owns has committed a window inside `3 × MAX_WINDOW_MS`, and red when one has
//! not. A broker that is unreachable, a bucket that refuses every PUT, a lease
//! lost to another instance: all of them arrive here as the same symptom,
//! because all of them have the same consequence.
//!
//! `3 ×` and not `1 ×`: a window closes at `MAX_WINDOW_MS`, then has to be
//! uploaded and committed, and the broker's own `safeTime` lags by its guard. A
//! one-window threshold would go red on a healthy sink under load, and a probe
//! that flaps is a probe an operator turns off.
//!
//! A queue that has NEVER committed is green. It is the honest answer for a
//! queue with no data (a lane nobody has pushed to yet, which at entity
//! cardinality is most of them at any moment) and for the seconds after boot —
//! the alternative is a container that fails its readiness probe because its
//! queues are idle.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;

use crate::obs::Metrics;

/// How many `MAX_WINDOW_MS` a queue may go without committing before the probe
/// turns red. See the module header for why it is three.
pub const STALE_WINDOWS: i64 = 3;

/// The floor on the staleness budget, so a sink configured with a very small
/// `MAX_WINDOW_MS` does not have a probe that goes red between two commits.
pub const MIN_STALE_MS: i64 = 30_000;

/// What the probe decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Every registered queue is inside its budget, or has never had data.
    Healthy { queues: usize },
    /// One queue is not. The FIRST one, by name, so the line is stable across
    /// scrapes rather than naming a different victim each time.
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

    /// The one-line JSON body the probe answers with.
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

    pub fn status(&self) -> StatusCode {
        match self {
            Verdict::Healthy { .. } => StatusCode::OK,
            Verdict::Stale { .. } => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

/// Everything the two routes read. Cheap to clone by `Arc`, written by the
/// per-queue tasks and read by whatever is probing.
pub struct HealthState {
    /// Registered queue → the wall clock of its last commit, `None` for a queue
    /// that has never committed.
    queues: RwLock<BTreeMap<String, Option<i64>>>,
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

    /// Start watching a queue. Idempotent, and it never resets a commit already
    /// recorded — a queue re-registered after a lease handover keeps its history.
    pub fn register_queue(&self, queue: &str) {
        self.queues
            .write()
            .expect("health queues lock")
            .entry(queue.to_string())
            .or_insert(None);
    }

    /// Stop watching a queue — a lease this instance lost. A queue somebody
    /// else owns must not make THIS process's probe red.
    pub fn forget_queue(&self, queue: &str) {
        self.queues
            .write()
            .expect("health queues lock")
            .remove(queue);
    }

    /// Record a committed window. `wall_ms` is the sink's own clock, which is
    /// the only place one is used: the health budget is about how long ago THIS
    /// PROCESS last made progress, not about where the log's clock is.
    pub fn record_commit(&self, queue: &str, wall_ms: i64) {
        let mut q = self.queues.write().expect("health queues lock");
        let slot = q.entry(queue.to_string()).or_insert(None);
        // Monotone: an out-of-order call from a task that was descheduled must
        // not walk the recorded progress backwards.
        if slot.is_none_or(|prev| wall_ms > prev) {
            *slot = Some(wall_ms);
        }
    }

    pub fn set_max_window_ms(&self, ms: u64) {
        *self.max_window_ms.write().expect("health window lock") = ms;
    }

    /// The staleness budget: `3 × MAX_WINDOW_MS`, floored.
    pub fn limit_ms(&self) -> i64 {
        let window = *self.max_window_ms.read().expect("health window lock") as i64;
        (window.saturating_mul(STALE_WINDOWS)).max(MIN_STALE_MS)
    }

    /// The probe's answer as of `now_ms`.
    pub fn verdict(&self, now_ms: i64) -> Verdict {
        let limit_ms = self.limit_ms();
        let q = self.queues.read().expect("health queues lock");
        for (queue, last) in q.iter() {
            let Some(last) = last else { continue };
            let stale_ms = now_ms.saturating_sub(*last);
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

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}

/// The two routes, so a test can serve them on an ephemeral port of its own.
pub fn router(state: Arc<HealthState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

async fn healthz(State(state): State<Arc<HealthState>>) -> Response {
    let verdict = state.verdict(crate::obs::now_epoch_ms());
    (
        verdict.status(),
        [(header::CONTENT_TYPE, "application/json")],
        verdict.body(),
    )
        .into_response()
}

async fn metrics(State(state): State<Arc<HealthState>>) -> Response {
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics().render(),
    )
        .into_response()
}

/// Serve `/healthz` and `/metrics` on `listen` until the process ends.
///
/// Binding is done here rather than by the caller so that a port already in use
/// is reported against `QUEEN_S3_LISTEN` by name, at boot, instead of as an
/// anonymous `AddrInUse` from inside a spawned task.
pub async fn serve(listen: SocketAddr, state: Arc<HealthState>) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await.map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("cannot bind QUEEN_S3_LISTEN={listen}: {e}"),
        )
    })?;
    tracing::info!(target: "queen-s3", listen = %listen, "health and metrics listening");
    axum::serve(listener, router(state)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(max_window_ms: u64) -> Arc<HealthState> {
        Arc::new(HealthState::new(Arc::new(Metrics::new()), max_window_ms))
    }

    #[test]
    fn a_queue_that_has_never_committed_is_healthy() {
        let s = state(300_000);
        s.register_queue("orders");
        assert_eq!(
            s.verdict(1_000_000_000),
            Verdict::Healthy { queues: 1 },
            "an idle queue is not a broken sink"
        );
    }

    #[test]
    fn no_queues_at_all_is_healthy() {
        assert!(state(300_000).verdict(0).is_healthy());
    }

    #[test]
    fn the_budget_is_three_windows_with_a_floor() {
        assert_eq!(state(300_000).limit_ms(), 900_000);
        assert_eq!(state(1_000).limit_ms(), MIN_STALE_MS, "the floor holds");
        let s = state(300_000);
        s.set_max_window_ms(60_000);
        assert_eq!(s.limit_ms(), 180_000);
    }

    #[test]
    fn a_commit_inside_the_budget_is_green_and_one_outside_it_is_red() {
        let s = state(300_000);
        s.register_queue("orders");
        s.record_commit("orders", 1_000_000);
        assert!(
            s.verdict(1_000_000 + 900_000).is_healthy(),
            "exactly at the limit"
        );
        match s.verdict(1_000_000 + 900_001) {
            Verdict::Stale {
                queue,
                stale_ms,
                limit_ms,
            } => {
                assert_eq!(queue, "orders");
                assert_eq!(stale_ms, 900_001);
                assert_eq!(limit_ms, 900_000);
            }
            other => panic!("expected stale, got {other:?}"),
        }
    }

    #[test]
    fn the_verdict_names_the_first_stale_queue_by_name_every_time() {
        let s = state(300_000);
        for q in ["zeta", "alpha", "mid"] {
            s.register_queue(q);
            s.record_commit(q, 0);
        }
        for _ in 0..5 {
            match s.verdict(10_000_000) {
                Verdict::Stale { queue, .. } => assert_eq!(queue, "alpha"),
                other => panic!("expected stale, got {other:?}"),
            }
        }
    }

    #[test]
    fn commits_are_monotone_and_a_forgotten_queue_stops_counting() {
        let s = state(300_000);
        s.register_queue("orders");
        s.record_commit("orders", 5_000_000);
        // A task that was descheduled and reports an older commit must not walk
        // the recorded progress backwards.
        s.record_commit("orders", 1_000);
        assert!(s.verdict(5_400_000).is_healthy());

        // A queue whose lease went to another instance stops being this
        // process's problem.
        s.record_commit("clicks", 0);
        assert!(!s.verdict(5_400_000).is_healthy());
        s.forget_queue("clicks");
        assert_eq!(s.verdict(5_400_000), Verdict::Healthy { queues: 1 });
    }

    #[test]
    fn registering_a_queue_twice_keeps_its_history() {
        let s = state(300_000);
        s.register_queue("orders");
        s.record_commit("orders", 1_000_000);
        s.register_queue("orders");
        assert!(s.verdict(1_100_000).is_healthy());
        assert_eq!(s.verdict(1_100_000), Verdict::Healthy { queues: 1 });
    }

    /// Serve the router on an ephemeral port and answer its base URL.
    ///
    /// A real listener and a real client rather than a in-process service call:
    /// the status code, the content type and the body are the whole contract of
    /// these two routes, and they are only the contract once they have been
    /// through a socket.
    async fn serve_ephemeral(state: Arc<HealthState>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router(state)).await;
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn the_routes_answer_what_the_verdict_says() {
        let s = state(300_000);
        s.metrics().window_committed("orders");
        let base = serve_ephemeral(s).await;
        let http = reqwest::Client::new();

        let resp = http.get(format!("{base}/healthz")).send().await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(resp.text().await.unwrap(), "{\"ok\":true,\"queues\":0}");

        let resp = http.get(format!("{base}/metrics")).send().await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "text/plain; version=0.0.4; charset=utf-8"
        );
        let text = resp.text().await.unwrap();
        assert!(
            text.contains("queen_s3_windows_committed_total{queue=\"orders\"} 1"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_stale_queue_answers_503_with_a_one_line_verdict() {
        let s = state(300_000);
        s.register_queue("orders");
        // A commit at the epoch is, by any real clock, very stale indeed.
        s.record_commit("orders", 1);
        let base = serve_ephemeral(s).await;
        let resp = reqwest::get(format!("{base}/healthz")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 503);
        let text = resp.text().await.unwrap();
        assert!(
            text.starts_with("{\"ok\":false,\"queue\":\"orders\""),
            "{text}"
        );
        assert!(!text.contains('\n'), "{text}");
    }
}
