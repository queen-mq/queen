//! IN-PROCESS MODE for the Kafka wire facade (PLAN_SINGLE_BINARY.md W2).
//!
//! In raft mode, `QUEEN_KAFKA_EMBEDDED=true` runs the `queen-kafka` library
//! INSIDE this broker process — one binary, one process, no child to spawn or
//! supervise. The facade reads exactly the environment the child read (`QUEEN_KAFKA_*`,
//! `QUEEN_TOKEN`), through the same [`queen_kafka::boot::Config`], so every
//! knob keeps its documented meaning.
//!
//! ## Threads
//! The facade's own work — accepting, decoding and encoding the Kafka wire, the
//! record envelope, compression — runs on a dedicated multi-thread tokio
//! runtime whose threads are named `queen-kafka` (`QUEEN_KAFKA_THREADS`, default
//! `min(4, cores / 2)`, at least 1). Every call INTO the broker is spawned onto
//! the broker's own runtime ([`RouterDispatch`]), so broker code never runs on a
//! facade thread and a Kafka load spike cannot occupy the broker's workers with
//! protocol work. What the two share is the state machine itself: a Kafka
//! produce is a push command like any other.
//!
//! ## The Queen API, minus the socket
//! The facade still speaks Queen's HTTP API — routes, JSON, status codes — but
//! its transport is the broker's axum `Router`, called as a service. Routing,
//! auth, tenancy, push admission (`admit_edge`, which turns a full budget into a
//! 429 the facade answers as `throttle_time_ms`) and follower forwarding are the
//! ones every HTTP client meets; TCP, HTTP/1 framing and the reqwest client are
//! gone. A Kafka produce is a Queen push and a Kafka fetch is Queen's
//! `POST /api/v1/fetch`: ONE pipeline, the one every Queen client uses. (A
//! typed path that stored Kafka batches verbatim was built and removed on
//! 2026-09-24: a second pipeline that every change to the first one broke.)
//!
//! The facade's own KV calls — offsets, topic records, the node registry — are
//! the one route answered without the router ([`RouterDispatch::kv`]): the
//! same `Rsm::kv` the KV route calls, minus the tenant KV rate ladder, whose
//! 429 a Kafka client sees as COORDINATOR_NOT_AVAILABLE.
//!
//! An EXPLICIT `QUEEN_URL` keeps the facade on HTTP to that URL: that is the
//! Cloud hairpin through the proxy (authentication, tenant scoping, quotas and
//! metering on every Kafka request), and bypassing it in-process would bypass
//! all four. Only the child process goes away in that case.
//!
//! ## Blast radius
//! A panic on a `queen-kafka` thread kills the task it happened in — one Kafka
//! connection — and nothing else ([`crate::obs::install_panic_hook`] aborts the
//! process for a panic on any OTHER thread, as `panic = "abort"` did). A facade
//! whose serve loop ends (a panic in the accept loop, a listener error) is
//! restarted with the child supervisor's ladder: 1s doubling to 30s, reset
//! after an hour of healthy running. Clients reconnect and resume from offsets
//! that live in Queen, exactly as they did across a child restart.

use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use queen_kafka::boot;
use queen_kafka::queen::{
    BoxFuture, HttpQueen, LocalAnswer, LocalDispatch, LocalRequest, LocalResponse, QueenApi,
};
use tower::ServiceExt;

use crate::config::KafkaFacadeConfig;
use crate::rsm::facade::Rsm;

/// The facade runtime's thread name. It must NOT be a core thread name
/// ([`crate::obs::panic_policy::CORE_THREAD_PREFIXES`]): on a non-core thread a
/// panic unwinds and kills only its task (a Kafka connection), not the broker.
const THREAD_NAME: &str = "queen-kafka";

/// Restart ladder, the same as the child supervisor's (kafka_facade.rs).
const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const HEALTHY_RUN: Duration = Duration::from_secs(3600);

/// Blocking threads of the facade runtime. The facade itself blocks nowhere;
/// the cap is a ceiling on what a misbehaving dependency could spawn.
const MAX_BLOCKING_THREADS: usize = 64;

// ---------------------------------------------------------------------------
// The transport: the broker's router, called as a service.
// ---------------------------------------------------------------------------

/// [`LocalDispatch`] over the broker's router. Each call is SPAWNED onto the
/// broker runtime and awaited from the facade runtime, so the handler, the
/// middleware and every blocking read they make run on broker threads.
struct RouterDispatch {
    router: axum::Router,
    broker: tokio::runtime::Handle,
    /// The state machine and the authenticator, for the one route answered
    /// without the router: the facade's own KV ([`crate::handlers::facade_kv`]).
    kv: Option<(Arc<dyn Rsm>, Arc<crate::auth::Authenticator>)>,
}

/// How long one of the facade's KV calls may take, as over HTTP.
const KV_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

/// The facade's KV call without the router: authorized as the route, then
/// straight to the state machine (see [`crate::handlers::facade_kv`]).
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
        Some(Ok(serde_json::Value::Array(ops))) => ops,
        _ => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                "{\"error\":\"kv_bad_body\"}",
            )
                .into_response()
        }
    };
    let tenant = crate::tenant::Tenant::default_tenant().as_str().to_string();
    crate::handlers::facade_kv(rsm, tenant, ops, KV_BUDGET).await
}

/// The deadline of a typed push, the route's default (`DEFAULT_TIMEOUT`,
/// 30 s): the facade's own 10-second budget for a call ends it first anyway.
const RECORDS_BUDGET: Duration = Duration::from_secs(30);

/// The answer the route would have given for `e`: [`crate::handlers::raft::err_response`]
/// read back into the facade's shape, so a typed call fails exactly as the
/// route does (status, `Retry-After`, body).
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

/// The route's authorization for a typed call: `Ok(sub)` with the JWT subject,
/// or the refusal the router would have answered.
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

impl LocalDispatch for RouterDispatch {
    /// The record path is the state machine's own push and fetch
    /// ([`Rsm::push_records`], [`Rsm::fetch_records`]): the commands, the
    /// admission budget and the reads of the two routes, without their JSON.
    fn serves_records(&self) -> bool {
        self.kv.is_some()
    }

    fn push_records(
        &self,
        bearer: Option<String>,
        items: Vec<queen_kafka::queen::RawPush>,
    ) -> BoxFuture<'static, Result<LocalAnswer<Vec<queen_kafka::queen::Pushed>>, String>> {
        let Some((rsm, auth)) = self.kv.clone() else {
            return Box::pin(async { Err("no state machine for the record path".to_string()) });
        };
        let task = self.broker.spawn(async move {
            let sub = match authorize(&auth, "/api/v1/push", bearer.as_deref()).await {
                Ok(sub) => sub,
                Err(refused) => return LocalAnswer::Refused(refused),
            };
            let tenant = crate::tenant::Tenant::default_tenant().as_str().to_string();
            let ctx = crate::rsm::facade::ReqCtx::new(
                tenant,
                crate::rsm::facade::Deadline::after(RECORDS_BUDGET),
            )
            .with_producer_sub(sub);
            let records = items
                .into_iter()
                .map(|i| crate::rsm::facade::RecordPush {
                    queue: i.queue,
                    partition: i.partition,
                    payload: i.payload,
                })
                .collect();
            match rsm.push_records(ctx, records).await {
                Ok(out) => LocalAnswer::Done(
                    out.into_iter()
                        .map(|o| queen_kafka::queen::Pushed {
                            status: o.status.to_string(),
                            offset: o.offset.map(|v| v as i64),
                        })
                        .collect(),
                ),
                Err(e) => LocalAnswer::Refused(refusal(e).await),
            }
        });
        Box::pin(async move {
            task.await
                .map_err(|e| format!("the broker task serving an in-process push ended: {e}"))
        })
    }

    fn fetch_records(
        &self,
        bearer: Option<String>,
        entries: Vec<queen_kafka::queen::FetchEntry>,
        max_wait_ms: i64,
        min_bytes: i64,
    ) -> BoxFuture<'static, Result<LocalAnswer<Vec<queen_kafka::queen::RawFetched>>, String>> {
        let Some((rsm, auth)) = self.kv.clone() else {
            return Box::pin(async { Err("no state machine for the record path".to_string()) });
        };
        let task = self.broker.spawn(async move {
            if let Err(refused) = authorize(&auth, "/api/v1/fetch", bearer.as_deref()).await {
                return LocalAnswer::Refused(refused);
            }
            let max_wait_ms = max_wait_ms.clamp(0, 30_000) as u64;
            let tenant = crate::tenant::Tenant::default_tenant().as_str().to_string();
            let ctx = crate::rsm::facade::ReqCtx::new(
                tenant,
                crate::rsm::facade::Deadline::after(
                    RECORDS_BUDGET + Duration::from_millis(max_wait_ms),
                ),
            );
            // The route's bounds: a negative offset refuses the whole read, as
            // a 400 does there; a budget is clamped to 1 B..8 MiB.
            if entries.iter().any(|e| e.offset < 0) {
                return LocalAnswer::Refused(LocalResponse {
                    status: 400,
                    retry_after: None,
                    body: "{\"error\":\"offset must be non-negative\"}".to_string(),
                });
            }
            let asks = entries
                .into_iter()
                .map(|e| crate::rsm::facade::RecordFetch {
                    queue: e.queue,
                    partition: e.partition,
                    offset: e.offset as u64,
                    max_bytes: e.max_bytes.clamp(1, 8 << 20) as usize,
                })
                .collect();
            match rsm
                .fetch_records(ctx, asks, max_wait_ms, min_bytes.max(0) as usize)
                .await
            {
                Ok(read) => LocalAnswer::Done(
                    read.into_iter()
                        .map(|f| queen_kafka::queen::RawFetched {
                            records: f
                                .records
                                .into_iter()
                                .map(|r| queen_kafka::queen::RawRecord {
                                    offset: r.offset as i64,
                                    // `null` for an empty payload, as the
                                    // route renders one.
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
        Box::pin(async move {
            task.await
                .map_err(|e| format!("the broker task serving an in-process fetch ended: {e}"))
        })
    }

    fn call(&self, req: LocalRequest) -> BoxFuture<'static, Result<LocalResponse, String>> {
        let router = self.router.clone();
        let kv = self.kv.clone();
        let task = self.broker.spawn(async move {
            let response = match kv {
                // Only where the state machine takes writes: a follower's KV
                // goes through the router, which forwards it to the leader.
                Some((rsm, auth))
                    if req.method == "POST"
                        && req.path == "/api/v1/kv"
                        && rsm.route() == crate::rsm::facade::Route::Local =>
                {
                    direct_kv(rsm, auth, req).await
                }
                _ => {
                    let request = build_request(req)?;
                    match router.oneshot(request).await {
                        Ok(r) => r,
                        Err(never) => match never {},
                    }
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
}

/// `QUEEN_KAFKA_OFFSET_STORE`: where the IN-PROCESS facade keeps committed
/// offsets. `positions` (the default) — this raft broker's own consumer-group
/// positions, one raft entry per commit batch whatever its width, the group
/// visible and shared as a Queen consumer group. `kv` — the KV rows
/// (`qk:group:*`) every other broker keeps them in, which is also what an
/// explicit `QUEEN_URL` always gets: the facade then cannot know the broker
/// behind it. Offsets are not moved from one store to the other.
fn offsets_as_positions() -> bool {
    match std::env::var("QUEEN_KAFKA_OFFSET_STORE") {
        Err(_) => true,
        Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
            "" | "positions" => true,
            "kv" => false,
            other => crate::obs::fatal(format!(
                "QUEEN_KAFKA_OFFSET_STORE={other}: expected `positions` or `kv`"
            )),
        },
    }
}

/// The HTTP request the facade would have sent, as an axum request: the same
/// method, path, `Host`, bearer, `Content-Type` and `Content-Length` (the last
/// one is what push admission sizes its permits by).
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

// ---------------------------------------------------------------------------
// Boot.
// ---------------------------------------------------------------------------

/// Resolve the facade's configuration from the environment. Called at BOOT,
/// before the listener binds, so the knob with no default
/// (`QUEEN_KAFKA_ADVERTISED_ADDR`) fails the broker's boot with the facade's
/// own sentence — the same contract as the child mode's preflight.
pub fn preflight() -> Result<boot::Config, String> {
    boot::Config::from_env()
}

/// Whether a normalized `QUEEN_URL` names this broker's own listener: a
/// loopback or wildcard host on `own_port`. That is almost always a leftover
/// from the child mode (whose QUEEN_URL was the broker's loopback), and it
/// quietly trades the in-process transport for HTTP to itself.
fn points_at_self(queen_url: &str, own_port: &str) -> bool {
    let rest = queen_url
        .strip_prefix("http://")
        .or_else(|| queen_url.strip_prefix("https://"))
        .unwrap_or(queen_url);
    let authority = rest.split('/').next().unwrap_or(rest);
    let Some((host, port)) = authority.rsplit_once(':') else {
        return false;
    };
    port == own_port.trim()
        && matches!(
            host.trim_matches(|c| c == '[' || c == ']'),
            "localhost" | "127.0.0.1" | "::1" | "0.0.0.0" | "::"
        )
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

/// Start the facade on its own runtime, with `router` as its Queen transport
/// (unless `QUEEN_URL` is set explicitly — see the module header). Must be
/// called from the broker runtime: calls are spawned onto the runtime current
/// here.
pub fn start(
    knobs: &KafkaFacadeConfig,
    cfg: boot::Config,
    router: axum::Router,
    own_port: &str,
    raft_dir: &str,
    rsm: Arc<dyn Rsm>,
    auth: Arc<crate::auth::Authenticator>,
) -> Arc<InProcess> {
    let explicit_url = std::env::var("QUEEN_URL")
        .ok()
        .filter(|v| !v.trim().is_empty());
    if explicit_url.is_some() && points_at_self(cfg.queen_url(), own_port) {
        tracing::warn!(
            target: "kafka",
            queen_url = %cfg.queen_url(),
            "QUEEN_URL points at this broker itself, so the in-process Kafka facade will \
             call its own HTTP listener instead of the router; unset QUEEN_URL unless a \
             proxy is meant to sit in between"
        );
    }
    let (api, via): (Arc<dyn QueenApi>, &'static str) = match explicit_url {
        Some(_) => match HttpQueen::new(cfg.queen_url()) {
            Ok(http) => (Arc::new(http), "http (explicit QUEEN_URL)"),
            // Unreachable in practice: `Config::from_env` already normalized
            // and validated this URL.
            Err(e) => crate::obs::fatal(format!("QUEEN_KAFKA_EMBEDDED=true: {e}")),
        },
        None => (
            Arc::new(
                HttpQueen::local(Arc::new(RouterDispatch {
                    router,
                    broker: tokio::runtime::Handle::current(),
                    kv: Some((Arc::clone(&rsm), Arc::clone(&auth))),
                }))
                .with_native_positions(offsets_as_positions()),
            ),
            "in-process",
        ),
    };
    tracing::info!(
        target: "kafka",
        offsets = if api.native_positions() { "positions" } else { "kv" },
        "committed offsets are kept as {}",
        if api.native_positions() {
            "the broker's consumer-group positions"
        } else {
            "KV rows"
        }
    );
    // Inside this broker the facade can say what its storage is: every raft
    // voter holds every partition, and an acknowledged write is on a majority.
    // Over an explicit QUEEN_URL it knows nothing of the broker behind it.
    let cfg = if via == "in-process" {
        let voters = crate::rsm::replicator::raft::cluster::ClusterConfig::from_env()
            .ok()
            .flatten()
            .map_or(1, |c| c.members.len() as u32);
        cfg.with_raft(voters, raft_dir)
    } else {
        cfg
    };
    let threads = worker_threads();
    let status = Arc::new(Status::new(&cfg, via, threads));
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
        transport = via,
        threads,
        listen = %cfg.listen_addr(),
        advertised = %cfg.advertised(),
        "starting the Kafka facade in-process"
    );
    let spawned = std::thread::Builder::new()
        // Inside the unwind prefix: the supervisor itself is facade code.
        .name(format!("{THREAD_NAME}-main"))
        .spawn(move || {
            runtime.block_on(supervise(cfg, api, via, stop_rx, status));
            // Connection tasks still open are dropped here: the process is
            // stopping and their clients reconnect elsewhere or later.
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
    /// Stop accepting, let the facade hand back what it holds (a cluster-mode
    /// registry row), and wait for it — bounded by
    /// `QUEEN_KAFKA_SHUTDOWN_GRACE_MS`, the same window the child had.
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

/// Run the facade until the broker stops it, restarting it when its serve loop
/// ends on its own.
async fn supervise(
    cfg: boot::Config,
    api: Arc<dyn QueenApi>,
    via: &'static str,
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
            "broker shutdown"
        };
        let outcome =
            AssertUnwindSafe(boot::serve(cfg.clone(), Arc::clone(&api), via, stop_signal))
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
    transport: &'static str,
    threads: usize,
    listen: String,
    advertised: String,
    clustered: bool,
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
    fn new(cfg: &boot::Config, transport: &'static str, threads: usize) -> Status {
        Status {
            transport,
            threads,
            listen: cfg.listen_addr().to_string(),
            advertised: cfg.advertised(),
            clustered: cfg.clustered(),
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

/// The `kafka` block of `GET /status` when the facade runs in-process, or
/// `None` when it does not. Beside the supervisor's phase it carries the
/// facade's own report ([`queen_kafka::introspect`]): the live set it
/// advertises (`cluster.live`, `cluster.down`, how it judges them), the
/// width ceiling, and the idempotent-producer tracker's size and evictions.
pub fn status_value() -> Option<serde_json::Value> {
    let st = STATUS.get()?;
    let g = st.inner.lock().ok()?;
    let mut v = serde_json::json!({
        "mode": "in-process",
        "transport": st.transport,
        "phase": g.phase,
        "threads": st.threads,
        "listen": st.listen,
        "advertised": st.advertised,
        "clustered": st.clustered,
        "restarts": g.restarts,
        "lastExit": g.last_exit,
        "uptimeMs": if g.phase == "running" { g.since.elapsed().as_millis() as u64 } else { 0 },
        "backoffMs": g.backoff_ms,
    });
    if let (Some(serde_json::Value::Object(facade)), Some(out)) =
        (queen_kafka::introspect::snapshot(), v.as_object_mut())
    {
        out.extend(facade);
    }
    Some(v)
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

    /// The leftover-child-mode QUEEN_URL is recognised; a real proxy is not.
    #[test]
    fn a_queen_url_naming_this_broker_is_recognised() {
        assert!(points_at_self("http://127.0.0.1:6632", "6632"));
        assert!(points_at_self("http://localhost:6632/", "6632"));
        assert!(points_at_self("http://[::1]:6632", "6632"));
        assert!(!points_at_self("http://127.0.0.1:6633", "6632"));
        assert!(!points_at_self("https://proxy.cell-1.example:443", "6632"));
        assert!(!points_at_self("http://10.0.0.7:6632", "6632"));
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

        let get = build_request(LocalRequest {
            method: "GET",
            path: "/api/v1/resources/queues".into(),
            host: None,
            bearer: None,
            body: None,
        })
        .unwrap();
        assert!(get.headers().get("authorization").is_none());
        assert!(get.headers().get("content-length").is_none());
    }

    fn auth_off() -> Arc<crate::auth::Authenticator> {
        crate::auth::Authenticator::new(crate::config::AuthConfig {
            enabled: false,
            algorithm: "HS256".into(),
            secret: String::new(),
            public_key: String::new(),
            jwks_url: String::new(),
            jwks_refresh_interval_seconds: 3600,
            jwks_request_timeout_ms: 5000,
            issuer: String::new(),
            audience: String::new(),
            clock_skew_seconds: 30,
            skip_paths: Vec::new(),
            roles_claim: "role".into(),
            roles_array_claim: "roles".into(),
            role_admin: "admin".into(),
            role_read_write: "read-write".into(),
            role_read_only: "read-only".into(),
            role_write_only: "write-only".into(),
        })
    }

    /// The facade's own KV goes straight to the state machine — no router,
    /// hence no rate ladder — and answers the route's own shapes: the results
    /// on success, the precondition verdict as the 200 the facade reads it as.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_facades_kv_is_answered_by_the_state_machine_directly() {
        std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
        let dir = std::env::temp_dir().join(format!("queen-kinproc-kv-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let facade = crate::rsm::facade::real::RaftFacade::open(&crate::rsm::facade::RsmBuildCtx {
            data_dir: dir.display().to_string(),
            notifier: crate::notify::Notifier::new(false),
            disk_high_pct: 85.0,
            disk_low_pct: 80.0,
        })
        .expect("open facade");
        let rsm: Arc<dyn Rsm> = Arc::new(facade);
        let dispatch = RouterDispatch {
            // Nothing may reach the router: it has no routes at all.
            router: axum::Router::new(),
            broker: tokio::runtime::Handle::current(),
            kv: Some((Arc::clone(&rsm), auth_off())),
        };
        let ops = vec![
            queen_kafka::queen::KvOp::put(
                "queen-kafka",
                "qk:group:g:t:0",
                serde_json::json!({"offset": 5}),
            ),
            queen_kafka::queen::KvOp::GetMany {
                ns: "queen-kafka".into(),
                keys: vec!["qk:group:g:t:0".into()],
            },
        ];
        let body = serde_json::json!({ "operations": ops }).to_string();
        let answer = dispatch
            .call(LocalRequest {
                method: "POST",
                path: "/api/v1/kv".into(),
                host: None,
                bearer: None,
                body: Some(body),
            })
            .await
            .unwrap();
        assert_eq!(answer.status, 200, "{}", answer.body);
        let v: serde_json::Value = serde_json::from_str(&answer.body).unwrap();
        let results = v["results"].as_array().expect("results");
        assert_eq!(results.len(), 2, "{}", answer.body);
        assert!(
            results.iter().any(|r| r["rows"][0]["value"]["offset"] == 5),
            "the put is read back: {}",
            answer.body
        );

        // Through the facade's own client, as the offsets code calls it: a
        // lost precondition is the Precondition error, not a status.
        let client = queen_kafka::queen::HttpQueen::local(Arc::new(RouterDispatch {
            router: axum::Router::new(),
            broker: tokio::runtime::Handle::current(),
            kv: Some((Arc::clone(&rsm), auth_off())),
        }));
        let lost = client
            .kv(
                &[queen_kafka::queen::KvOp::fence(
                    "queen-kafka",
                    "qk:group:g:t:0",
                    serde_json::json!({"offset": 6}),
                    999_999,
                )],
                None,
            )
            .await;
        assert!(
            matches!(lost, Err(queen_kafka::queen::Error::Precondition { .. })),
            "{lost:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// End to end over a real router: the dispatch answers what the router
    /// answers, status, `Retry-After` and body, and runs it on the broker's
    /// runtime even when called from another one.
    #[test]
    fn the_dispatch_answers_what_the_router_answers_from_another_runtime() {
        use axum::routing::{get, post};
        let broker = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let router = axum::Router::new()
            .route(
                "/api/v1/echo",
                post(|headers: axum::http::HeaderMap, body: String| async move {
                    let auth = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    let thread = std::thread::current().name().unwrap_or("").to_string();
                    format!("{auth}|{body}|{thread}")
                }),
            )
            .route(
                "/api/v1/busy",
                get(|| async {
                    axum::response::Response::builder()
                        .status(429)
                        .header("retry-after", "3")
                        .body(axum::body::Body::from("{\"error\":\"busy\"}"))
                        .unwrap()
                }),
            );
        let dispatch = RouterDispatch {
            router,
            broker: broker.handle().clone(),
            kv: None,
        };
        let facade = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name(THREAD_NAME)
            .enable_all()
            .build()
            .unwrap();
        let (echo, busy) = facade.block_on(async {
            let echo = dispatch
                .call(LocalRequest {
                    method: "POST",
                    path: "/api/v1/echo".into(),
                    host: None,
                    bearer: Some("tok".into()),
                    body: Some("hello".into()),
                })
                .await
                .unwrap();
            let busy = dispatch
                .call(LocalRequest {
                    method: "GET",
                    path: "/api/v1/busy".into(),
                    host: None,
                    bearer: None,
                    body: None,
                })
                .await
                .unwrap();
            (echo, busy)
        });
        assert_eq!(echo.status, 200);
        let mut parts = echo.body.split('|');
        assert_eq!(parts.next(), Some("Bearer tok"));
        assert_eq!(parts.next(), Some("hello"));
        let thread = parts.next().unwrap_or_default();
        assert!(
            !thread.starts_with(THREAD_NAME),
            "the handler ran on a facade thread ({thread})"
        );
        assert_eq!(busy.status, 429);
        assert_eq!(busy.retry_after.as_deref(), Some("3"));
        assert_eq!(busy.body, "{\"error\":\"busy\"}");
    }

    /// The facade's committed offsets as the broker's own consumer-group
    /// POSITIONS, end to end over the REAL raft router: the transaction's
    /// `positions` rider, the read route, the fence riding in the same bundle,
    /// OffsetDelete's forget and DeleteGroups' encoded group delete — through
    /// exactly the client and the offsets code the in-process facade runs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn offsets_are_the_brokers_positions_end_to_end() {
        use queen_kafka::offsets::{self, Committed, Loaded};
        use queen_kafka::queen::{PushItem, QueenApi};
        std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
        let dir = std::env::temp_dir().join(format!(
            "queen-kinproc-positions-{}-{}",
            std::process::id(),
            crate::util::uuidv7_bytes()[15]
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::env::set_var("QUEEN_RAFT_DIR", dir.join("cfg").display().to_string());
        let cfg = crate::config::load();
        let facade = crate::rsm::facade::real::RaftFacade::open(&crate::rsm::facade::RsmBuildCtx {
            data_dir: dir.display().to_string(),
            notifier: crate::notify::Notifier::new(false),
            disk_high_pct: 85.0,
            disk_low_pct: 80.0,
        })
        .expect("open facade");
        let rsm: Arc<dyn Rsm> = Arc::new(facade);
        let state = crate::handlers::raft::build_raft_state_with(&cfg, Some(Arc::clone(&rsm)))
            .expect("raft state");
        let router = crate::handlers::raft::build_raft_router(state, auth_off(), false);
        let client = HttpQueen::local(Arc::new(RouterDispatch {
            router,
            broker: tokio::runtime::Handle::current(),
            kv: Some((Arc::clone(&rsm), auth_off())),
        }))
        .with_native_positions(true);
        assert!(client.native_positions());

        let push = |p: i32| PushItem {
            queue: "orders".into(),
            partition: p.to_string(),
            payload: serde_json::json!({"n": p}),
        };
        client
            .push(&[push(0), push(0), push(0)], None)
            .await
            .expect("push");
        let commit = |o: i64, m: &str| Committed {
            offset: o,
            metadata: m.to_string(),
            ts: 1,
        };
        let group = "team a/billing";
        let k = |p: i32| offsets::key(group, "orders", p).unwrap();

        // A commit, on a partition with data and on one without.
        let stored = offsets::store(
            &client,
            &[(k(0), commit(2, "batch-2")), (k(1), commit(0, ""))],
            None,
            None,
        )
        .await;
        assert!(
            stored.results.iter().all(|r| r.is_ok()),
            "{:?}",
            stored.results
        );
        let loaded = offsets::load(&client, &[k(0), k(1), k(2)], None)
            .await
            .unwrap();
        assert!(
            matches!(&loaded[0], Loaded::Found(c) if c.offset == 2 && c.metadata == "batch-2"),
            "{loaded:?}"
        );
        assert!(
            matches!(&loaded[1], Loaded::Found(c) if c.offset == 0),
            "{loaded:?}"
        );
        assert_eq!(loaded[2], Loaded::Missing);
        assert_eq!(
            offsets::load_group(&client, group, None)
                .await
                .unwrap()
                .len(),
            2
        );

        // The fence rides in the same bundle; a stale one moves nothing.
        let fence = |expect: i64| queen_kafka::cluster::fence::FenceOp {
            key: offsets::fence_key(group).unwrap(),
            value: serde_json::json!({"node": 1}),
            expect,
        };
        let won = offsets::store(&client, &[(k(0), commit(3, ""))], None, Some(&fence(0))).await;
        let version = won
            .fence_version
            .expect("the fence landed with the position");
        let lost = offsets::store(
            &client,
            &[(k(0), commit(9, ""))],
            None,
            Some(&fence(version + 1_000)),
        )
        .await;
        assert!(lost.lost.is_some(), "{:?}", lost.results);
        let loaded = offsets::load(&client, &[k(0)], None).await.unwrap();
        assert!(
            matches!(&loaded[0], Loaded::Found(c) if c.offset == 3),
            "{loaded:?}"
        );

        // OffsetDelete forgets one; DeleteGroups takes the rest and the index.
        let forgot = offsets::delete_offsets(&client, group, &[k(1)], None, None).await;
        assert!(forgot.iter().all(|r| r.is_ok()), "{forgot:?}");
        assert_eq!(
            offsets::load(&client, &[k(1)], None).await.unwrap()[0],
            Loaded::Missing
        );
        offsets::index(&client, group, "consumer", None)
            .await
            .expect("index");
        let removed = offsets::delete_group(&client, group, None)
            .await
            .expect("delete group");
        assert_eq!(removed, 1 + 1, "one position and the index row");
        assert!(offsets::load_group(&client, group, None)
            .await
            .unwrap()
            .is_empty());

        drop(client);
        drop(rsm);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The record path ([`LocalDispatch::push_records`] /
    /// [`LocalDispatch::fetch_records`]) is the routes' push and fetch minus the
    /// JSON: what it writes the route reads back byte for byte, what it reads
    /// is what the route answers, its refusals are the route's, and its long
    /// poll wakes on an append.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_record_path_is_the_routes_push_and_fetch() {
        use queen_kafka::queen::{FetchEntry, QueenApi, RawPush};
        std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
        let dir = std::env::temp_dir().join(format!(
            "queen-kinproc-records-{}-{}",
            std::process::id(),
            crate::util::uuidv7_bytes()[15]
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::env::set_var("QUEEN_RAFT_DIR", dir.join("cfg").display().to_string());
        let cfg = crate::config::load();
        let facade = crate::rsm::facade::real::RaftFacade::open(&crate::rsm::facade::RsmBuildCtx {
            data_dir: dir.display().to_string(),
            notifier: crate::notify::Notifier::new(false),
            disk_high_pct: 85.0,
            disk_low_pct: 80.0,
        })
        .expect("open facade");
        let rsm: Arc<dyn Rsm> = Arc::new(facade);
        let state = crate::handlers::raft::build_raft_state_with(&cfg, Some(Arc::clone(&rsm)))
            .expect("raft state");
        let router = crate::handlers::raft::build_raft_router(state, auth_off(), false);
        // Typed, and the same broker over its routes only (no state machine
        // handed to the dispatch: every call is the router's).
        let typed = Arc::new(HttpQueen::local(Arc::new(RouterDispatch {
            router: router.clone(),
            broker: tokio::runtime::Handle::current(),
            kv: Some((Arc::clone(&rsm), auth_off())),
        })));
        let routed = HttpQueen::local(Arc::new(RouterDispatch {
            router,
            broker: tokio::runtime::Handle::current(),
            kv: None,
        }));

        let item = |p: &str, payload: &str| RawPush {
            queue: "orders".into(),
            partition: p.into(),
            payload: payload.as_bytes().to_vec(),
        };
        let envelope = r#"{"h":[{"k":"x","v":null}],"k":"azE=","t":1756000000000,"v":"dmFsdWU="}"#;
        let pushed = typed
            .push_raw(
                vec![
                    item("0", envelope),
                    item("0", r#"{"k":null,"v":"dg=="}"#),
                    item("1", r#"{"native":true}"#),
                    item("0", r#"{"k":null,"t":5,"v":null}"#),
                ],
                None,
            )
            .await
            .expect("typed push");
        let offsets: Vec<_> = pushed
            .iter()
            .map(|p| (p.status.as_str(), p.offset))
            .collect();
        assert_eq!(
            offsets,
            vec![
                ("queued", Some(0)),
                ("queued", Some(1)),
                ("queued", Some(0)),
                ("queued", Some(2))
            ]
        );
        // The route's push of the same shape lands after them.
        let routed_push = routed
            .push_raw(vec![item("0", r#"{"k":null,"v":"cm91dGVk"}"#)], None)
            .await
            .expect("routed push");
        assert_eq!(routed_push[0].offset, Some(3));

        let entries = vec![
            FetchEntry {
                queue: "orders".into(),
                partition: "0".into(),
                offset: 0,
                max_bytes: 1 << 20,
            },
            FetchEntry {
                queue: "orders".into(),
                partition: "1".into(),
                offset: 0,
                max_bytes: 1 << 20,
            },
            FetchEntry {
                queue: "orders".into(),
                partition: "7".into(),
                offset: 0,
                max_bytes: 1 << 20,
            },
            FetchEntry {
                queue: "nope".into(),
                partition: "0".into(),
                offset: 0,
                max_bytes: 1 << 20,
            },
            FetchEntry {
                queue: "orders".into(),
                partition: "0".into(),
                offset: 9,
                max_bytes: 1 << 20,
            },
        ];
        let a = typed.fetch_raw(&entries, 0, 0, None).await.expect("typed");
        let b = routed
            .fetch_raw(&entries, 0, 0, None)
            .await
            .expect("routed");
        assert_eq!(a, b, "the record path answers what the route answers");
        assert_eq!(a[0].records.len(), 4);
        assert_eq!(&a[0].records[0].payload[..], envelope.as_bytes());
        assert_eq!(a[0].high_watermark, 4);
        assert_eq!(a[1].records.len(), 1);
        assert!(a[2].records.is_empty() && a[2].error.is_none());
        assert_eq!(a[3].error.as_deref(), Some("UNKNOWN_TOPIC_OR_PARTITION"));
        assert_eq!(a[4].error.as_deref(), Some("OFFSET_OUT_OF_RANGE"));
        assert!(a[0].records.iter().all(|r| r.timestamp_ms.is_some()));

        // A byte budget stops after the record that crosses it, as the route's.
        let small = vec![FetchEntry {
            max_bytes: 10,
            ..entries[0].clone()
        }];
        let a = typed.fetch_raw(&small, 0, 0, None).await.unwrap();
        let b = routed.fetch_raw(&small, 0, 0, None).await.unwrap();
        assert_eq!(a, b);
        assert_eq!(a[0].records.len(), 1);

        // The route's refusal, read the same way.
        let bad = vec![FetchEntry {
            offset: -1,
            ..entries[0].clone()
        }];
        let refused = typed.fetch_raw(&bad, 0, 0, None).await;
        assert!(
            matches!(
                refused,
                Err(queen_kafka::queen::Error::Status { code: 400, .. })
            ),
            "{refused:?}"
        );

        // The long poll parks at the end of the log and wakes on an append.
        let tail = vec![FetchEntry {
            offset: 4,
            ..entries[0].clone()
        }];
        let waiter = {
            let typed = Arc::clone(&typed);
            tokio::spawn(async move {
                let started = std::time::Instant::now();
                let got = typed.fetch_raw(&tail, 5_000, 1, None).await;
                (got, started.elapsed())
            })
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        typed
            .push_raw(vec![item("0", r#"{"k":null,"v":"d2FrZQ=="}"#)], None)
            .await
            .expect("wake push");
        let (got, took) = waiter.await.unwrap();
        let got = got.expect("long poll");
        assert_eq!(got[0].records.len(), 1, "{got:?}");
        assert_eq!(got[0].records[0].offset, 4);
        assert!(took < Duration::from_millis(4_000), "parked {took:?}");

        drop(typed);
        drop(rsm);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// KIP-79 end to end over the REAL raft router: the facade's
    /// `offsets_for_times` reaches `POST /api/v1/fetch/offsets` in-process and
    /// reads back the first offset appended at or after each time, on the
    /// clock the fetch route reports.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_time_is_looked_up_in_process_through_the_route() {
        use queen_kafka::queen::{FetchEntry, PushItem, QueenApi, TimeLookup};
        std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
        let dir = std::env::temp_dir().join(format!(
            "queen-kinproc-times-{}-{}",
            std::process::id(),
            crate::util::uuidv7_bytes()[15]
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::env::set_var("QUEEN_RAFT_DIR", dir.join("cfg").display().to_string());
        let cfg = crate::config::load();
        let facade = crate::rsm::facade::real::RaftFacade::open(&crate::rsm::facade::RsmBuildCtx {
            data_dir: dir.display().to_string(),
            notifier: crate::notify::Notifier::new(false),
            disk_high_pct: 85.0,
            disk_low_pct: 80.0,
        })
        .expect("open facade");
        let rsm: Arc<dyn Rsm> = Arc::new(facade);
        let state = crate::handlers::raft::build_raft_state_with(&cfg, Some(Arc::clone(&rsm)))
            .expect("raft state");
        let router = crate::handlers::raft::build_raft_router(state, auth_off(), false);
        let client = HttpQueen::local(Arc::new(RouterDispatch {
            router,
            broker: tokio::runtime::Handle::current(),
            kv: Some((Arc::clone(&rsm), auth_off())),
        }));

        let item = || PushItem {
            queue: "orders".into(),
            partition: "0".into(),
            payload: serde_json::json!({"k": null, "v": "YQ=="}),
        };
        client.push(&[item(), item()], None).await.expect("push");
        tokio::time::sleep(Duration::from_millis(5)).await;
        client.push(&[item()], None).await.expect("push");
        let fetched = client
            .fetch(
                &[FetchEntry {
                    queue: "orders".into(),
                    partition: "0".into(),
                    offset: 0,
                    max_bytes: 1 << 20,
                }],
                0,
                0,
                None,
            )
            .await
            .expect("fetch");
        let second = fetched[0].records[2].timestamp_ms().expect("stamped");

        let at = |ms: i64| TimeLookup {
            queue: "orders".into(),
            partition: "0".into(),
            timestamp_ms: ms,
        };
        let found = client
            .offsets_for_times(&[at(0), at(second), at(second + 60_000)], None)
            .await
            .expect("lookup")
            .expect("the route answers");
        let offsets: Vec<Option<i64>> = found.iter().map(|f| f.offset).collect();
        assert_eq!(offsets, [Some(0), Some(2), None], "{found:?}");
        assert_eq!(found[1].timestamp_ms, Some(second));

        drop(client);
        drop(rsm);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
