//! The Kafka wire protocol, IN this broker (`QUEEN_KAFKA_EMBEDDED=true`).
//!
//! The `queen-kafka` library is a translation layer: every Kafka request
//! becomes operations of this broker ([`queen_kafka::core::Broker`]), and the
//! facade keeps nothing that outlives a connection or a leader's term. This
//! module is that door:
//!
//! - **push / fetch** are the state machine's own typed record path
//!   ([`Rsm::push_records`], [`Rsm::fetch_records`]), authorized as the routes;
//! - every other call (queues, configure, KV, positions, the time index) is one
//!   of the broker's JSON routes, handed to its router in-process, so routing,
//!   auth, tenancy and admission are the ones every HTTP client meets;
//! - **cluster** is the raft membership: every voter is a Kafka broker, and the
//!   raft leader leads every partition and coordinates every group.
//!
//! ## Threads
//! The facade's protocol work runs on its own runtime, threads named
//! `queen-kafka` (`QUEEN_KAFKA_THREADS`, default `min(4, cores / 2)`); every
//! call into the broker is spawned onto the broker's runtime. A panic on a
//! facade thread kills one connection; a serve loop that ends is restarted
//! (1 s doubling to 30 s).

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use queen_kafka::core::{
    BoxFuture, Broker, ClusterView, FetchEntry, FetchedFrom, LocalAnswer, LocalRequest,
    LocalResponse, Node, OrderedLocal, Permit, Pushed, RawFetched, RawPush, RawRecord,
};
use tower::ServiceExt;

use crate::config::KafkaFacadeConfig;
use crate::rsm::facade::Rsm;

const THREAD_NAME: &str = "queen-kafka";
const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const HEALTHY_RUN: Duration = Duration::from_secs(3600);
const MAX_BLOCKING_THREADS: usize = 64;

/// How long one of the facade's KV calls may take.
const KV_BUDGET: Duration = Duration::from_secs(10);

/// The deadline of a typed push or fetch (plus the fetch's own wait).
const RECORDS_BUDGET: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// The door.
// ---------------------------------------------------------------------------

struct RouterDispatch {
    router: axum::Router,
    broker: tokio::runtime::Handle,
    rsm: Arc<dyn Rsm>,
    auth: Arc<crate::auth::Authenticator>,
    /// This node's raft id.
    node_id: u64,
    /// Every voter as a Kafka broker: raft id → (host, port).
    nodes: BTreeMap<u64, (String, i32)>,
}

impl RouterDispatch {
    fn tenant() -> String {
        crate::tenant::Tenant::default_tenant().as_str().to_string()
    }
}

/// The facade's KV call without the router: authorized as the route, then
/// straight to the state machine, minus the tenant KV rate ladder.
async fn direct_kv(
    rsm: Arc<dyn Rsm>,
    auth: Arc<crate::auth::Authenticator>,
    req: LocalRequest,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if let Err((status, why)) = crate::auth::authorize_route(
        &auth,
        &axum::http::Method::POST,
        "/api/v1/kv",
        req.bearer.as_deref(),
    )
    .await
    {
        return (status, format!("{{\"error\":\"{why}\"}}")).into_response();
    }
    let ops = match req
        .body
        .as_deref()
        .map(serde_json::from_str::<serde_json::Value>)
    {
        Some(Ok(serde_json::Value::Object(mut o))) => match o.remove("operations") {
            Some(serde_json::Value::Array(ops)) => ops,
            _ => {
                return (
                    axum::http::StatusCode::BAD_REQUEST,
                    "{\"error\":\"kv_bad_body\"}",
                )
                    .into_response()
            }
        },
        _ => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                "{\"error\":\"kv_bad_body\"}",
            )
                .into_response()
        }
    };
    crate::handlers::facade_kv(rsm, RouterDispatch::tenant(), ops, KV_BUDGET).await
}

/// The answer the route would have given for `e`.
async fn refusal(e: crate::rsm::facade::RsmError) -> LocalResponse {
    let response = crate::handlers::raft::err_response(e);
    let status = response.status().as_u16();
    let retry_after = response
        .headers()
        .get(axum::http::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .unwrap_or_default();
    LocalResponse {
        status,
        retry_after,
        body,
    }
}

/// The route's authorization for a typed call: the JWT subject, or the
/// refusal the router would have answered.
async fn authorize(
    auth: &crate::auth::Authenticator,
    path: &str,
    bearer: Option<&str>,
) -> Result<Option<String>, LocalResponse> {
    crate::auth::authorize_route(auth, &axum::http::Method::POST, path, bearer)
        .await
        .map_err(|(status, why)| LocalResponse {
            status: status.as_u16(),
            retry_after: None,
            body: format!("{{\"error\":\"{why}\"}}"),
        })
}

impl Broker for RouterDispatch {
    fn call(&self, req: LocalRequest) -> BoxFuture<'static, Result<LocalResponse, String>> {
        let router = self.router.clone();
        let (rsm, auth) = (Arc::clone(&self.rsm), Arc::clone(&self.auth));
        let task = self.broker.spawn(async move {
            let response = if req.method == "POST"
                && req.path == "/api/v1/kv"
                && rsm.route() == crate::rsm::facade::Route::Local
            {
                direct_kv(rsm, auth, req).await
            } else {
                let request = build_request(req)?;
                match router.oneshot(request).await {
                    Ok(r) => r,
                    Err(never) => match never {},
                }
            };
            let status = response.status().as_u16();
            let retry_after = response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .map_err(|e| format!("in-process response body: {e}"))?;
            Ok(LocalResponse {
                status,
                retry_after,
                body: String::from_utf8_lossy(&bytes).into_owned(),
            })
        });
        Box::pin(async move {
            task.await
                .map_err(|e| format!("the broker task serving an in-process call ended: {e}"))?
        })
    }

    /// The broker's push admission ([`crate::rsm::admit`]), taken by the
    /// facade for a Produce frame before it decodes a record.
    fn admit(&self, bytes: usize) -> BoxFuture<'static, Result<Option<Permit>, u64>> {
        Box::pin(async move {
            match crate::rsm::admit::global() {
                None => Ok(None),
                Some(gate) => gate
                    .admit(bytes)
                    .await
                    .map(|a| Some(Box::new(a) as Permit))
                    .map_err(|o| o.retry_after_s),
            }
        })
    }

    fn push(
        &self,
        bearer: Option<String>,
        items: Vec<RawPush>,
        admitted: Option<Permit>,
    ) -> OrderedLocal {
        let (rsm, auth) = (Arc::clone(&self.rsm), Arc::clone(&self.auth));
        let (ordered, in_log) = crate::rsm::facade::OrderedPush::new();
        let task = self.broker.spawn(async move {
            // Admitted by its frame before the facade decoded it: held until
            // the push answers, and not taken a second time by the push.
            let pre_admitted = admitted.is_some();
            let _admitted = admitted;
            let sub = match authorize(&auth, "/api/v1/push", bearer.as_deref()).await {
                Ok(sub) => sub,
                Err(refused) => return LocalAnswer::Refused(refused),
            };
            let mut ctx = crate::rsm::facade::ReqCtx::new(
                RouterDispatch::tenant(),
                crate::rsm::facade::Deadline::after(RECORDS_BUDGET),
            )
            .with_producer_sub(sub);
            ctx.ordered = Some(ordered);
            let records = items
                .into_iter()
                .map(|i| crate::rsm::facade::RecordPush {
                    queue: i.queue,
                    partition: i.partition,
                    payload: i.payload,
                    txn: i.txn,
                })
                .collect();
            let pushed = if pre_admitted {
                crate::rsm::admit::pre_admitted_scope(rsm.push_records(ctx, records)).await
            } else {
                rsm.push_records(ctx, records).await
            };
            match pushed {
                Ok(out) => LocalAnswer::Done(
                    out.into_iter()
                        .map(|o| Pushed {
                            status: o.status.to_string(),
                            offset: o.offset.map(|v| v as i64),
                        })
                        .collect(),
                ),
                Err(e) => LocalAnswer::Refused(refusal(e).await),
            }
        });
        OrderedLocal {
            in_log: Box::pin(async move { in_log.await.unwrap_or(false) }),
            done: Box::pin(async move {
                task.await
                    .map_err(|e| format!("the broker task serving an in-process push ended: {e}"))
            }),
        }
    }

    fn fetch(
        &self,
        bearer: Option<String>,
        entries: Vec<FetchEntry>,
        max_wait_ms: i64,
        min_bytes: i64,
        max_bytes: Option<i64>,
    ) -> BoxFuture<'static, Result<LocalAnswer<Vec<RawFetched>>, String>> {
        let (rsm, auth) = (Arc::clone(&self.rsm), Arc::clone(&self.auth));
        let task = self.broker.spawn(async move {
            if let Err(refused) = authorize(&auth, "/api/v1/fetch", bearer.as_deref()).await {
                return LocalAnswer::Refused(refused);
            }
            let max_wait_ms = max_wait_ms.clamp(0, 30_000) as u64;
            let mut ctx = crate::rsm::facade::ReqCtx::new(
                RouterDispatch::tenant(),
                crate::rsm::facade::Deadline::after(
                    RECORDS_BUDGET + Duration::from_millis(max_wait_ms),
                ),
            );
            // Never unlimited: absent, zero or negative is the facade's ceiling
            // (QUEEN_KAFKA_FETCH_MAX_BYTES), and nothing asks past it.
            let ceiling = queen_kafka::config::fetch_max_bytes();
            ctx.fetch_max_bytes = Some(
                max_bytes
                    .filter(|b| *b > 0)
                    .map_or(ceiling, |b| b.min(ceiling))
                    .max(1) as usize,
            );
            let asks = entries
                .into_iter()
                .map(|e| crate::rsm::facade::RecordFetch {
                    queue: e.queue,
                    partition: e.partition,
                    offset: e.offset.max(0) as u64,
                    max_bytes: e.max_bytes.clamp(1, 8 << 20) as usize,
                })
                .collect();
            match rsm
                .fetch_records(ctx, asks, max_wait_ms, min_bytes.max(0) as usize)
                .await
            {
                Ok(read) => LocalAnswer::Done(
                    read.into_iter()
                        .map(|f| RawFetched {
                            records: f
                                .records
                                .into_iter()
                                .map(|r| RawRecord {
                                    offset: r.offset as i64,
                                    payload: if r.payload.is_empty() {
                                        bytes::Bytes::from_static(b"null")
                                    } else {
                                        r.payload
                                    },
                                    timestamp_ms: Some(r.created_at_us.div_euclid(1000)),
                                })
                                .collect(),
                            high_watermark: f.high_watermark as i64,
                            log_start_offset: f.log_start_offset as i64,
                            error: f.error.map(str::to_string),
                        })
                        .collect(),
                ),
                Err(e) => LocalAnswer::Refused(refusal(e).await),
            }
        });
        // A fetch nobody waits for any more (its connection closed, or the
        // facade answered from another read of a split fetch) stops parking.
        let task = AbortOnDrop(task);
        Box::pin(async move {
            let mut task = task;
            (&mut task.0)
                .await
                .map_err(|e| format!("the broker task serving an in-process fetch ended: {e}"))
        })
    }

    fn cluster(&self) -> ClusterView {
        let tenant = RouterDispatch::tenant();
        let leader = if self.rsm.leads(&tenant) {
            Some(self.node_id)
        } else {
            self.rsm.leader_node(&tenant)
        };
        ClusterView {
            node_id: self.node_id as i32,
            leader: leader.map(|l| l as i32),
            nodes: self
                .nodes
                .iter()
                .map(|(id, (host, port))| Node {
                    id: *id as i32,
                    host: host.clone(),
                    port: *port,
                })
                .collect(),
        }
    }

    /// The dashboard's count of Kafka reads, per queue, as the answer carried
    /// them: a pop as a native consumer's pop counts (the pop rate, its lag,
    /// the node's pop throughput), and the Kafka fetch series beside it.
    fn note_fetched(&self, _bearer: Option<&str>, from: &[FetchedFrom]) {
        let Some(m) = crate::metrics::global() else {
            return;
        };
        let tenant = crate::tenant::Tenant::default_tenant();
        let total: u64 = from.iter().map(|f| f.records).sum();
        if total > 0 {
            m.pop.record_request(total as usize);
        }
        for f in from {
            m.per_queue.add_fetched(tenant.as_str(), &f.queue, f.records);
            m.per_queue.add_pop(tenant.as_str(), &f.queue, f.records);
            m.per_queue
                .add_pop_lag(tenant.as_str(), &f.queue, f.lag_sum_ms, f.lag_max_ms, f.records);
        }
    }
}

/// A broker task aborted when its caller stops waiting for it.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The HTTP request the facade stands for, as an axum request.
fn build_request(req: LocalRequest) -> Result<axum::http::Request<axum::body::Body>, String> {
    use axum::http::header;
    let mut builder = axum::http::Request::builder()
        .method(req.method)
        .uri(req.path.as_str())
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(host) = &req.host {
        builder = builder.header(header::HOST, host.as_str());
    }
    if let Some(token) = &req.bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let body = match req.body {
        Some(b) => {
            builder = builder.header(header::CONTENT_LENGTH, b.len());
            axum::body::Body::from(b)
        }
        None => axum::body::Body::empty(),
    };
    builder
        .body(body)
        .map_err(|e| format!("in-process request for {}: {e}", req.path))
}

/// Every raft voter as a Kafka broker. This node is advertised at
/// `QUEEN_KAFKA_ADVERTISED_ADDR`; another at its entry in `QUEEN_KAFKA_PEERS`
/// (`1=host:port,2=host:port,…`) when that is set, else at the host of its raft
/// HTTP address with the same Kafka port, which is the shape of a StatefulSet
/// (one headless name per pod, one port everywhere).
fn kafka_nodes(cfg: &queen_kafka::Config) -> (u64, BTreeMap<u64, (String, i32)>) {
    let own = (cfg.advertised_host.clone(), cfg.advertised_port);
    let explicit: BTreeMap<u64, (String, i32)> = std::env::var("QUEEN_KAFKA_PEERS")
        .unwrap_or_default()
        .split(',')
        .filter_map(|part| {
            let (id, addr) = part.trim().split_once('=')?;
            Some((
                id.trim().parse().ok()?,
                queen_kafka::config::split_host_port(addr.trim())?,
            ))
        })
        .collect();
    match crate::rsm::replicator::raft::cluster::ClusterConfig::from_env() {
        Ok(Some(c)) => {
            let nodes = c
                .members
                .iter()
                .map(|(id, n)| {
                    if *id == c.node_id {
                        return (*id, own.clone());
                    }
                    if let Some(addr) = explicit.get(id) {
                        return (*id, addr.clone());
                    }
                    let host = queen_kafka::config::split_host_port(&n.http)
                        .map(|(h, _)| h)
                        .unwrap_or_else(|| n.http.clone());
                    (*id, (host, cfg.advertised_port))
                })
                .collect();
            (c.node_id, nodes)
        }
        _ => {
            let id = crate::rsm::replicator::raft::cluster::node_id_from_env().unwrap_or(1);
            (id, BTreeMap::from([(id, own)]))
        }
    }
}

// ---------------------------------------------------------------------------
// Boot.
// ---------------------------------------------------------------------------

/// The facade's configuration, resolved at BOOT so a missing
/// `QUEEN_KAFKA_ADVERTISED_ADDR` fails the broker's boot with its own sentence.
pub fn preflight() -> Result<queen_kafka::Config, String> {
    queen_kafka::Config::from_env()
}

/// `QUEEN_KAFKA_THREADS`, default `min(4, cores / 2)`, at least 1.
fn worker_threads() -> usize {
    let cores = std::thread::available_parallelism().map_or(2, |n| n.get());
    let default = (cores / 2).clamp(1, 4);
    std::env::var("QUEEN_KAFKA_THREADS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| (1..=256).contains(n))
        .unwrap_or(default)
}

/// A running in-process facade: what `run_raft` holds to stop it.
pub struct InProcess {
    stop: tokio::sync::watch::Sender<bool>,
    done: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    grace: Duration,
}

/// Start the facade on its own runtime. Must be called from the broker
/// runtime: calls into the broker are spawned onto the runtime current here.
pub fn start(
    knobs: &KafkaFacadeConfig,
    cfg: queen_kafka::Config,
    router: axum::Router,
    _own_port: &str,
    _raft_dir: &str,
    rsm: Arc<dyn Rsm>,
    auth: Arc<crate::auth::Authenticator>,
) -> Arc<InProcess> {
    let (node_id, nodes) = kafka_nodes(&cfg);
    let broker: Arc<dyn Broker> = Arc::new(RouterDispatch {
        router,
        broker: tokio::runtime::Handle::current(),
        rsm,
        auth,
        node_id,
        nodes,
    });
    let threads = worker_threads();
    let status = Arc::new(Status::new(&cfg, threads));
    let _ = STATUS.set(Arc::clone(&status));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .max_blocking_threads(MAX_BLOCKING_THREADS)
        .thread_name(THREAD_NAME)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => crate::obs::fatal(format!("cannot build the Kafka facade runtime: {e}")),
    };
    tracing::info!(
        target: "kafka",
        threads,
        node_id,
        listen = %cfg.listen,
        advertised = %cfg.advertised(),
        "starting the Kafka facade in-process"
    );
    let spawned = std::thread::Builder::new()
        .name(format!("{THREAD_NAME}-main"))
        .spawn(move || {
            runtime.block_on(supervise(cfg, broker, stop_rx, status));
            runtime.shutdown_timeout(Duration::from_millis(500));
            let _ = done_tx.send(());
        });
    if let Err(e) = spawned {
        crate::obs::fatal(format!("cannot start the Kafka facade thread: {e}"));
    }
    Arc::new(InProcess {
        stop: stop_tx,
        done: Mutex::new(Some(done_rx)),
        grace: Duration::from_millis(knobs.shutdown_grace_ms),
    })
}

impl InProcess {
    /// Stop accepting and wait for the facade, bounded by
    /// `QUEEN_KAFKA_SHUTDOWN_GRACE_MS`.
    pub async fn shutdown(&self) {
        let _ = self.stop.send(true);
        let done = self.done.lock().ok().and_then(|mut d| d.take());
        if let Some(done) = done {
            if tokio::time::timeout(self.grace, done).await.is_err() {
                tracing::warn!(
                    target: "shutdown",
                    grace_ms = self.grace.as_millis() as u64,
                    "the in-process Kafka facade did not stop inside its grace window"
                );
            }
        }
    }
}

async fn supervise(
    cfg: queen_kafka::Config,
    broker: Arc<dyn Broker>,
    stop: tokio::sync::watch::Receiver<bool>,
    status: Arc<Status>,
) {
    let mut backoff = BACKOFF_INITIAL;
    loop {
        if *stop.borrow() {
            break;
        }
        let started = Instant::now();
        status.running();
        let mut until = stop.clone();
        let stop_signal = async move {
            let _ = until.wait_for(|stopping| *stopping).await;
        };
        let outcome = AssertUnwindSafe(queen_kafka::serve(
            cfg.clone(),
            Arc::clone(&broker),
            stop_signal,
        ))
        .catch_unwind()
        .await;
        if *stop.borrow() {
            break;
        }
        let reason = match outcome {
            Ok(Ok(())) => "the accept loop ended".to_string(),
            Ok(Err(e)) => e,
            Err(payload) => format!("panicked: {}", panic_text(payload.as_ref())),
        };
        if started.elapsed() >= HEALTHY_RUN {
            backoff = BACKOFF_INITIAL;
        }
        status.exited(&reason, backoff);
        tracing::error!(
            target: "kafka",
            reason = %reason,
            backoff_ms = backoff.as_millis() as u64,
            "the in-process Kafka facade stopped; restarting it"
        );
        let mut until = stop.clone();
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = until.wait_for(|stopping| *stopping) => break,
        }
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
    status.stopped();
}

fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string())
}

// ---------------------------------------------------------------------------
// /status
// ---------------------------------------------------------------------------

static STATUS: OnceLock<Arc<Status>> = OnceLock::new();

struct Status {
    threads: usize,
    listen: String,
    advertised: String,
    inner: Mutex<StatusInner>,
}

struct StatusInner {
    phase: &'static str,
    since: Instant,
    restarts: u64,
    last_exit: Option<String>,
    backoff_ms: u64,
}

impl Status {
    fn new(cfg: &queen_kafka::Config, threads: usize) -> Status {
        Status {
            threads,
            listen: cfg.listen.clone(),
            advertised: cfg.advertised(),
            inner: Mutex::new(StatusInner {
                phase: "starting",
                since: Instant::now(),
                restarts: 0,
                last_exit: None,
                backoff_ms: 0,
            }),
        }
    }

    fn with(&self, f: impl FnOnce(&mut StatusInner)) {
        if let Ok(mut g) = self.inner.lock() {
            f(&mut g);
        }
    }

    fn running(&self) {
        self.with(|s| {
            if s.phase == "backoff" {
                s.restarts += 1;
            }
            s.phase = "running";
            s.since = Instant::now();
            s.backoff_ms = 0;
        });
    }

    fn exited(&self, reason: &str, backoff: Duration) {
        self.with(|s| {
            s.phase = "backoff";
            s.since = Instant::now();
            s.last_exit = Some(reason.chars().take(512).collect());
            s.backoff_ms = backoff.as_millis() as u64;
        });
    }

    fn stopped(&self) {
        self.with(|s| {
            s.phase = "stopped";
            s.since = Instant::now();
        });
    }
}

/// The `kafka` block of `GET /status`, or `None` when the facade is off.
pub fn status_value() -> Option<serde_json::Value> {
    let st = STATUS.get()?;
    let g = st.inner.lock().ok()?;
    Some(serde_json::json!({
        "mode": "in-process",
        "transport": "in-process",
        "phase": g.phase,
        "threads": st.threads,
        "listen": st.listen,
        "advertised": st.advertised,
        "restarts": g.restarts,
        "lastExit": g.last_exit,
        "uptimeMs": if g.phase == "running" { g.since.elapsed().as_millis() as u64 } else { 0 },
        "backoffMs": g.backoff_ms,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The facade's threads must not be core threads, or a facade panic would
    /// abort the broker (obs::panic_policy aborts only on the core).
    #[test]
    fn the_facade_threads_are_not_core_threads() {
        use crate::obs::panic_policy::is_core_thread_name;
        assert!(!is_core_thread_name(THREAD_NAME));
        assert!(!is_core_thread_name(&format!("{THREAD_NAME}-main")));
        assert!(is_core_thread_name("queen-rsm-log"));
    }

    /// The request the router sees is the one HTTP would have carried.
    #[test]
    fn a_local_request_becomes_the_http_request_it_stands_for() {
        let req = build_request(LocalRequest {
            method: "POST",
            path: "/api/v1/push".into(),
            host: Some("t1.example".into()),
            bearer: Some("tok".into()),
            body: Some("{\"items\":[]}".into()),
        })
        .unwrap();
        assert_eq!(req.method(), axum::http::Method::POST);
        assert_eq!(req.uri().path(), "/api/v1/push");
        let h = req.headers();
        assert_eq!(h["host"], "t1.example");
        assert_eq!(h["authorization"], "Bearer tok");
        assert_eq!(h["content-type"], "application/json");
        assert_eq!(h["content-length"], "12");
    }
}
