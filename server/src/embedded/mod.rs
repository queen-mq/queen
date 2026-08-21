//! Embed the QueenMQ broker in a Rust application.
//!
//! ```no_run
//! use queen::{Broker, BrokerConfig};
//! use queen::protocol as qp;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let broker = Broker::start(
//!     BrokerConfig::new().pg("localhost", 5432, "postgres", "postgres", "postgres"),
//! )
//! .await?;
//!
//! broker
//!     .configure(&qp::ConfigureRequest::new("jobs"))
//!     .await?;
//! broker
//!     .push(vec![qp::PushItem::new("jobs", serde_json::json!({"n": 1}))])
//!     .await?;
//!
//! let popped = broker.pop("jobs", &qp::PopParams::default()).await?;
//! for m in &popped.messages {
//!     broker
//!         .ack(&qp::AckRequest {
//!             transaction_id: m.transaction_id.clone(),
//!             partition_id: m.partition_id.clone(),
//!             status: qp::AckStatus::Completed,
//!             consumer_group: Some(m.consumer_group.clone()),
//!             lease_id: Some(m.lease_id.clone()),
//!             error: None,
//!         })
//!         .await?;
//! }
//! broker.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! Every operation goes through the SAME handler functions the HTTP router
//! dispatches to — constructed extractors in, rendered bytes out, parsed back
//! into [`queen_protocol`] types. Behaviour, defaults and edge cases are
//! therefore the HTTP broker's, by construction. The push/ack render paths are
//! pinned to the protocol types by the `protocol_conformance` tests; the
//! remaining response bodies are covered by the embedded end-to-end test
//! (`tests/embedded_smoke.rs`). The serde round-trip this costs is noise next
//! to the Postgres work behind each call.
//!
//! # Scope and caveats (v1)
//!
//! * **One `Broker` per process LIFETIME is the supported shape.** The
//!   admission arbiter is a first-set-wins process global: a second concurrent
//!   instance — or a new instance after a `start → shutdown → start` cycle —
//!   still routes its maintenance writers through the FIRST broker's arbiter,
//!   whose budget was sized for the first configuration. Everything stays
//!   correct, but maintenance metering degrades. For a full teardown, restart
//!   the process.
//! * **`shutdown` is best-effort.** It aborts the loops this module spawned
//!   and closes the connection pool (idle Postgres connections drop at once).
//!   The loops spawned inside the engine — fusion shards, the admission
//!   adapter, retention, stats, syscollect, the spool drain, the metrics
//!   samplers and the log reporter — expose no handles today and keep running
//!   until process exit; with the pool closed they fail their next
//!   `pool.get()` and idle harmlessly.
//! * **Panics.** The broker's own doctrine is panic=abort + supervisor
//!   restart. Embedded in an unwind build, a panicking background loop dies
//!   silently and its subsystem stops; the request paths themselves are
//!   panic-free under normal operation.
//! * **The on-disk push spool** (DB-outage / maintenance durability) defaults
//!   to a per-instance temp dir, removed on a clean empty shutdown:
//!   `status: "buffered"` pushes survive a broker restart only if you
//!   configure a stable [`BrokerConfig::spool_dir`]. Never share a spool dir
//!   between two instances — with `FILE_BUFFER_DIR` set, a second in-process
//!   Broker gets a private subdir of it for the same reason.
//! * **No mesh.** N embedded instances over one Postgres stay correct (leases,
//!   acks, dedup and maintenance coordinate through the database), but every
//!   cross-instance notice a broker fleet sends as a peer frame is a periodic
//!   READ of the database here, so it costs its floor rather than the ~20ms a
//!   frame takes: a parked pop re-polls on its own backoff (≤ 1s), a config
//!   invalidation rides the reconcile loop (`QUEEN_CACHE_REFRESH_INTERVAL_MS`,
//!   60s), and another instance's PUSH enters this instance's wildcard
//!   candidate ring on the WINDOWED reseed — one `QUEEN_HOTLIST_RESEED_MS`
//!   (30s) plus that ring's de-phasing offset — because a push is a recent
//!   write by definition.
//!
//!   Since 1.0.1 that last floor is two numbers rather than one, and the
//!   second is where an embedded fleet is genuinely worse off than a broker
//!   fleet. Anything that makes OLD partitions pending with NO write — a
//!   backward seek, a consumer-group delete — is invisible to the windowed
//!   pass by construction. A broker peer is told over the mesh; embedded has
//!   no frame to receive, so it waits for the durable repair marker
//!   (`queen.hotlist_repairs`) that the operation writes in its own
//!   transaction and this instance's reconcile loop polls: one interval (60s),
//!   after a 5s settle at start. Which side of that this surface sits on
//!   matters: seek and consumer-group delete are NOT exposed here (see below),
//!   so an embedded instance only ever READS those markers — the publisher is
//!   an HTTP broker sharing this database, whether driven by its API or by
//!   `queenctl`. Everything else a
//!   windowed pass cannot see (a ring entry cleared in error, a claim stranded
//!   by a dropped pop, a stale lease park) waits for the FULL walk,
//!   `QUEEN_HOTLIST_RESEED_FULL_MS` (300s) plus its offset, exactly as in the
//!   broker. Lower `QUEEN_CACHE_REFRESH_INTERVAL_MS` to buy the marker latency
//!   back; `QUEEN_HOTLIST_RESEED_FULL_MS=0` makes every pass a full walk at
//!   the 30s cadence instead (and stops the marker poll, which then has
//!   nothing to add), at the database cost the windowing exists to avoid.
//! * **Not exposed in v1**: consumer-group administration (list/seek/delete),
//!   queue listings, traces and the streams surface — the DLQ is covered
//!   ([`Broker::dlq`], [`Broker::retry_message`], [`Broker::delete_message`]).
//! * Env tuning knobs (`QUEEN_*`, `PG_*`, `LOG_*`) are honoured exactly like
//!   the binary; [`BrokerConfig`] fields win where both are set. Exceptions:
//!   `QUEEN_TENANCY_HEADER` and `JWT_ENABLED` are ignored embedded (a warning
//!   is logged) — see [`BrokerConfig`]. A malformed boolean env var makes the
//!   binary exit; [`Broker::start`] returns [`StartError::Config`] instead.
//!   Logging goes through `tracing` — install your own subscriber (the
//!   library never does).

mod boot;

use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;

use queen_protocol as qp;

use crate::handlers::AppState;

// ---------------------------------------------------------------- errors

/// Why [`Broker::start`] failed. The broker binary exits the process on these;
/// the library reports them.
#[derive(Debug)]
pub enum StartError {
    /// A boolean env knob has an unparseable value (e.g. `QUEEN_HOTLIST=si`).
    /// The binary exits on these; the library refuses to start instead —
    /// see the boolean pre-validation in `embedded/boot.rs`.
    Config(String),
    /// The connection pool could not be built (bad TLS/pool configuration).
    Pool(String),
    /// Postgres is unreachable or refused the credentials.
    Connect(String),
    /// Applying schema.sql + procedures failed.
    Schema(String),
}

impl std::fmt::Display for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(e) => write!(f, "configuration: {e}"),
            Self::Pool(e) => write!(f, "pool configuration: {e}"),
            Self::Connect(e) => write!(f, "postgres connect: {e}"),
            Self::Schema(e) => write!(f, "schema apply: {e}"),
        }
    }
}

impl std::error::Error for StartError {}

/// Operation failure. Mirrors what an HTTP client would see, minus the
/// transport: the broker signals outcomes through a status code plus an
/// `{"error": …}` body, and this enum is that mapping.
#[derive(Debug)]
pub enum Error {
    /// The request was malformed (HTTP 400 territory: bad body, missing field).
    /// Terminal — retrying the same request cannot succeed.
    InvalidRequest(String),
    /// The addressed entity does not exist (HTTP 404 territory). Terminal.
    NotFound(String),
    /// The broker refused or failed the operation. `status` is the HTTP code
    /// the handler chose (`None` when the failure was reported inside a 200
    /// body, as pop and configure do). A 5xx here is usually transient (pool
    /// exhaustion, a DB hiccup) and worth a retry with backoff; an in-body
    /// failure is usually semantic and terminal.
    Broker {
        status: Option<u16>,
        message: String,
    },
    /// The broker's response bytes did not parse into the protocol type. This
    /// is a bug, never expected: the push/ack render paths are pinned to the
    /// protocol types by the `protocol_conformance` tests, and the remaining
    /// bodies are covered by the embedded end-to-end test.
    Decode(String),
}

impl Error {
    fn broker(status: Option<u16>, message: impl Into<String>) -> Self {
        Self::Broker {
            status,
            message: message.into(),
        }
    }

    /// The HTTP status code behind this error, when one exists.
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::InvalidRequest(_) => Some(400),
            Self::NotFound(_) => Some(404),
            Self::Broker { status, .. } => *status,
            Self::Decode(_) => None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRequest(e) => write!(f, "invalid request: {e}"),
            Self::NotFound(e) => write!(f, "not found: {e}"),
            Self::Broker {
                status: Some(s),
                message,
            } => write!(f, "broker error (status {s}): {message}"),
            Self::Broker {
                status: None,
                message,
            } => write!(f, "broker error: {message}"),
            Self::Decode(e) => write!(f, "response decode: {e}"),
        }
    }
}

impl std::error::Error for Error {}

// ---------------------------------------------------------------- config

/// Configuration for [`Broker::start`].
///
/// Every field is optional: unset fields fall back to the same environment
/// variables and defaults the broker binary reads (`PG_HOST`, `DB_POOL_SIZE`,
/// `QUEEN_*` tuning knobs, …), so an embedded broker in a container behaves
/// like the image. Two env knobs are deliberately NOT honoured embedded (a
/// warning is logged if set): `QUEEN_TENANCY_HEADER` (embedded is single-tenant
/// by construction) and `JWT_ENABLED` (there is no HTTP surface to
/// authenticate; the in-process caller is trusted). `QUEEN_MAX_BODY_BYTES` has
/// no embedded equivalent either — there is no request body to cap.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub pg_host: Option<String>,
    pub pg_port: Option<u16>,
    pub pg_user: Option<String>,
    pub pg_password: Option<String>,
    pub pg_database: Option<String>,
    /// Connect to Postgres over TLS (env `PG_USE_SSL`, default false).
    pub pg_use_ssl: Option<bool>,
    /// Verify the server certificate chain when TLS is on
    /// (env `PG_SSL_REJECT_UNAUTHORIZED`, default true).
    pub pg_ssl_reject_unauthorized: Option<bool>,
    /// Per-statement timeout in milliseconds (env `QUEEN_STMT_TIMEOUT_MS`,
    /// default 30000).
    pub stmt_timeout_ms: Option<u64>,
    /// Connection pool size (env `DB_POOL_SIZE`, default 160 — size this DOWN
    /// for an application fleet: every instance owns its own pool).
    pub pool_size: Option<usize>,
    /// Apply schema.sql + procedures at start (advisory-locked, idempotent).
    /// Disable when the schema is managed externally; the connecting role then
    /// needs no DDL rights. Default true.
    pub apply_schema: bool,
    /// Stable directory for the on-disk push spool (DB-outage / maintenance
    /// durability). `None` (default) uses a per-instance temp dir: spooled
    /// pushes then do NOT survive a restart. Never share this dir between
    /// instances.
    pub spool_dir: Option<PathBuf>,
    /// Run the background retention/eviction sweep (advisory-locked, one
    /// sweeper per cycle across all instances sharing the DB). Disable only if
    /// an external broker on the same database already runs it. Default true.
    pub retention: bool,
    /// Run the background stats reconciler feeding `queen.stats` (status and
    /// analytics reads). Same advisory-lock coordination. Default true.
    pub stats_refresh: bool,
    /// Write per-instance worker/system metrics rows (dashboard). Default true.
    pub system_metrics: bool,
    /// Emit the periodic `rates`/`sizes` aggregate log blocks via `tracing`.
    /// Default true (inert without a subscriber).
    pub log_reports: bool,
}

/// Same defaults as [`BrokerConfig::new`] — the derive would silently flip
/// every documented `Default true` flag to false, so it is written out.
impl Default for BrokerConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl BrokerConfig {
    pub fn new() -> Self {
        Self {
            pg_host: None,
            pg_port: None,
            pg_user: None,
            pg_password: None,
            pg_database: None,
            pg_use_ssl: None,
            pg_ssl_reject_unauthorized: None,
            stmt_timeout_ms: None,
            pool_size: None,
            apply_schema: true,
            spool_dir: None,
            retention: true,
            stats_refresh: true,
            system_metrics: true,
            log_reports: true,
        }
    }

    pub fn pg_use_ssl(mut self, on: bool) -> Self {
        self.pg_use_ssl = Some(on);
        self
    }

    pub fn pg_ssl_reject_unauthorized(mut self, on: bool) -> Self {
        self.pg_ssl_reject_unauthorized = Some(on);
        self
    }

    pub fn stmt_timeout_ms(mut self, ms: u64) -> Self {
        self.stmt_timeout_ms = Some(ms);
        self
    }

    /// Set the whole Postgres connection in one call.
    pub fn pg(
        mut self,
        host: impl Into<String>,
        port: u16,
        user: impl Into<String>,
        password: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        self.pg_host = Some(host.into());
        self.pg_port = Some(port);
        self.pg_user = Some(user.into());
        self.pg_password = Some(password.into());
        self.pg_database = Some(database.into());
        self
    }

    pub fn pool_size(mut self, n: usize) -> Self {
        self.pool_size = Some(n);
        self
    }

    pub fn apply_schema(mut self, apply: bool) -> Self {
        self.apply_schema = apply;
        self
    }

    pub fn spool_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.spool_dir = Some(dir.into());
        self
    }

    pub fn retention(mut self, on: bool) -> Self {
        self.retention = on;
        self
    }

    pub fn stats_refresh(mut self, on: bool) -> Self {
        self.stats_refresh = on;
        self
    }

    pub fn system_metrics(mut self, on: bool) -> Self {
        self.system_metrics = on;
        self
    }

    pub fn log_reports(mut self, on: bool) -> Self {
        self.log_reports = on;
        self
    }
}

// ---------------------------------------------------------------- results

/// Outcome of [`Broker::delete_queue`]. Deleting a queue that does not exist
/// is not an error (`existed: false`), matching the HTTP API.
///
/// `existed`/`deleted` are deliberately NOT defaulted: if the rendered body
/// ever drops or renames them, the call surfaces [`Error::Decode`] instead of
/// silently reading as "nothing was deleted".
#[derive(Debug, Clone, serde::Deserialize)]
pub struct DeleteQueueResult {
    pub existed: bool,
    pub deleted: bool,
    #[serde(default)]
    pub message: Option<String>,
}

// ---------------------------------------------------------------- broker

/// An in-process QueenMQ broker handle. `Clone` is cheap (one `Arc`) — clone
/// it into every task that produces or consumes; hold at least one for the
/// lifetime of the application and call [`Broker::shutdown`] on the way out.
///
/// Must be created and used inside a Tokio runtime: the engine spawns its
/// background work on the ambient runtime.
#[derive(Clone)]
pub struct Broker {
    inner: Arc<Inner>,
}

struct Inner {
    st: Arc<AppState>,
    tasks: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Set when the spool dir was auto-generated (no explicit `spool_dir`, no
    /// FILE_BUFFER_DIR): shutdown removes it when it holds no pending events.
    auto_spool_dir: Option<PathBuf>,
}

impl Broker {
    /// Boot the broker engine: connect to Postgres, apply the schema (unless
    /// disabled), start the background machinery. Returns once the broker is
    /// ready to serve operations.
    pub async fn start(cfg: BrokerConfig) -> Result<Self, StartError> {
        let booted = boot::boot(&cfg).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                st: booted.st,
                tasks: std::sync::Mutex::new(booted.tasks),
                auto_spool_dir: booted.auto_spool_dir,
            }),
        })
    }

    /// Push one or more messages. Per-item outcomes (queued / duplicate /
    /// buffered / error / failed) are in the returned vector, in request
    /// order — a partial failure is not an `Err`. This includes the
    /// maintenance-mode spool path, which the HTTP layer reports as a 500 when
    /// any item's spool write failed but whose body is still the per-item
    /// array; the array wins here so the caller can see which items were
    /// accepted.
    pub async fn push(&self, items: Vec<qp::PushItem>) -> Result<Vec<qp::PushResult>, Error> {
        let body = serde_json::to_vec(&qp::PushRequest::new(items))
            .map_err(|e| Error::InvalidRequest(e.to_string()))?;
        let resp = crate::handlers::handle_push(
            State(self.inner.st.clone()),
            Extension(crate::auth::AuthedSub(None)),
            Extension(crate::tenant::Tenant::default_tenant()),
            Bytes::from(body),
        )
        .await;
        let (status, bytes) = read_response(resp).await;
        if !status.is_success() {
            // buffer_all renders the normal per-item array under a 500 when
            // some spool writes failed — surface the outcomes, not an Err.
            if let Ok(results) = serde_json::from_slice::<Vec<qp::PushResult>>(&bytes) {
                return Ok(results);
            }
            return Err(error_from(status, &bytes));
        }
        parse(&bytes)
    }

    /// Claim messages from any partition of `queue`. An empty claim returns an
    /// empty [`qp::PopResponse`], not an error. With `params.wait = true` this
    /// long-polls up to `params.timeout_millis` (broker default 30s), parking
    /// on the broker's in-process waker — no polling loop needed.
    pub async fn pop(&self, queue: &str, params: &qp::PopParams) -> Result<qp::PopResponse, Error> {
        let p: crate::handlers::PopParams = pop_params(params, None, None)?;
        let resp = crate::handlers::handle_pop(
            State(self.inner.st.clone()),
            Extension(crate::tenant::Tenant::default_tenant()),
            Path(queue.to_string()),
            Query(p),
        )
        .await;
        pop_response(resp, queue).await
    }

    /// Claim messages from one named partition of `queue`.
    pub async fn pop_partition(
        &self,
        queue: &str,
        partition: &str,
        params: &qp::PopParams,
    ) -> Result<qp::PopResponse, Error> {
        let p: crate::handlers::PopParams = pop_params(params, None, None)?;
        let resp = crate::handlers::handle_pop_partition(
            State(self.inner.st.clone()),
            Extension(crate::tenant::Tenant::default_tenant()),
            Path((queue.to_string(), partition.to_string())),
            Query(p),
        )
        .await;
        pop_response(resp, queue).await
    }

    /// Discovery pop across every queue matching `params.namespace` /
    /// `params.task` (at least one is required).
    pub async fn pop_discover(&self, params: &qp::PopParams) -> Result<qp::PopResponse, Error> {
        let p: crate::handlers::PopDiscoverParams =
            pop_params(params, params.namespace.as_deref(), params.task.as_deref())?;
        let resp = crate::handlers::handle_pop_discover(
            State(self.inner.st.clone()),
            Extension(crate::tenant::Tenant::default_tenant()),
            Query(p),
        )
        .await;
        pop_response(resp, "").await
    }

    /// Ack (or nack) one message. Like the HTTP API, a REJECTED ack (expired
    /// or foreign lease, unknown message, pool exhaustion on the ack path) is
    /// `Ok` with `success: false` and the reason in `error` — inspect the
    /// [`qp::AckResult`]; `Err` here means the request itself was malformed.
    pub async fn ack(&self, req: &qp::AckRequest) -> Result<qp::AckResult, Error> {
        let body = serde_json::to_vec(req).map_err(|e| Error::InvalidRequest(e.to_string()))?;
        let resp = crate::handlers::handle_ack(
            State(self.inner.st.clone()),
            Extension(crate::tenant::Tenant::default_tenant()),
            Bytes::from(body),
        )
        .await;
        let (status, bytes) = read_response(resp).await;
        if !status.is_success() {
            return Err(error_from(status, &bytes));
        }
        let results: Vec<qp::AckResult> = parse(&bytes)?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| Error::Decode("empty ack result array".into()))
    }

    /// Ack a batch under one consumer group. Per-item outcomes in order.
    pub async fn ack_batch(&self, req: &qp::AckBatchRequest) -> Result<Vec<qp::AckResult>, Error> {
        let body = serde_json::to_vec(req).map_err(|e| Error::InvalidRequest(e.to_string()))?;
        let resp = crate::handlers::handle_ack_batch(
            State(self.inner.st.clone()),
            Extension(crate::tenant::Tenant::default_tenant()),
            Bytes::from(body),
        )
        .await;
        let (status, bytes) = read_response(resp).await;
        if !status.is_success() {
            return Err(error_from(status, &bytes));
        }
        parse(&bytes)
    }

    /// Extend a lease. Renewal is best-effort: an unknown/expired lease is
    /// `success: false` in the response, not an `Err`.
    pub async fn renew_lease(
        &self,
        lease_id: &str,
        seconds: Option<i32>,
    ) -> Result<qp::RenewLeaseResponse, Error> {
        let body = serde_json::to_vec(&qp::RenewLeaseRequest { seconds })
            .map_err(|e| Error::InvalidRequest(e.to_string()))?;
        let resp = crate::handlers::handle_lease_extend(
            State(self.inner.st.clone()),
            Extension(crate::tenant::Tenant::default_tenant()),
            Path(lease_id.to_string()),
            Bytes::from(body),
        )
        .await;
        let (status, bytes) = read_response(resp).await;
        if !status.is_success() {
            return Err(error_from(status, &bytes));
        }
        parse(&bytes)
    }

    /// Atomically ack messages and/or push new ones. A rollback (an expired
    /// required lease, a duplicate push, an unknown message) is returned as
    /// `success: false` with the database's reason in `error` — inspect the
    /// response rather than expecting an `Err`, which is reserved for
    /// malformed requests and broker failures.
    pub async fn transaction(
        &self,
        req: &qp::TransactionRequest,
    ) -> Result<qp::TransactionResponse, Error> {
        let body = serde_json::to_vec(req).map_err(|e| Error::InvalidRequest(e.to_string()))?;
        let resp = crate::handlers::handle_transaction(
            State(self.inner.st.clone()),
            Extension(crate::auth::AuthedSub(None)),
            Extension(crate::tenant::Tenant::default_tenant()),
            Bytes::from(body),
        )
        .await;
        let (status, bytes) = read_response(resp).await;
        if !status.is_success() {
            return Err(error_from(status, &bytes));
        }
        parse(&bytes)
    }

    /// Create or reconfigure a queue.
    pub async fn configure(
        &self,
        req: &qp::ConfigureRequest,
    ) -> Result<qp::ConfigureResponse, Error> {
        let body = serde_json::to_vec(req).map_err(|e| Error::InvalidRequest(e.to_string()))?;
        let resp = crate::handlers::handle_configure(
            State(self.inner.st.clone()),
            Extension(crate::tenant::Tenant::default_tenant()),
            Bytes::from(body),
        )
        .await;
        let (status, bytes) = read_response(resp).await;
        if !status.is_success() {
            return Err(error_from(status, &bytes));
        }
        let parsed: qp::ConfigureResponse = parse(&bytes)?;
        if let Some(err) = &parsed.error {
            return Err(Error::broker(None, err.clone()));
        }
        Ok(parsed)
    }

    /// Delete a queue and everything in it. Idempotent: a missing queue is
    /// `existed: false`, not an error.
    pub async fn delete_queue(&self, queue: &str) -> Result<DeleteQueueResult, Error> {
        let resp = crate::handlers::handle_delete_queue(
            State(self.inner.st.clone()),
            Extension(crate::tenant::Tenant::default_tenant()),
            Path(queue.to_string()),
        )
        .await;
        let (status, bytes) = read_response(resp).await;
        if !status.is_success() {
            return Err(error_from(status, &bytes));
        }
        parse(&bytes)
    }

    /// The broker's main metrics snapshot (the `/metrics` document): counters,
    /// latencies, cache and pool gauges, per-queue rates.
    pub async fn metrics(&self) -> Result<serde_json::Value, Error> {
        let resp = crate::handlers::handle_metrics(State(self.inner.st.clone())).await;
        let (status, bytes) = read_response(resp).await;
        if !status.is_success() {
            return Err(error_from(status, &bytes));
        }
        parse(&bytes)
    }

    /// The same metrics in Prometheus text exposition format.
    pub async fn prometheus(&self) -> Result<String, Error> {
        let resp = crate::handlers::handle_prometheus(State(self.inner.st.clone())).await;
        let (status, bytes) = read_response(resp).await;
        if !status.is_success() {
            return Err(error_from(status, &bytes));
        }
        String::from_utf8(bytes.to_vec()).map_err(|e| Error::Decode(e.to_string()))
    }

    /// Health document (`status`, `database`, `engine`, `version`). It makes a
    /// real database round trip, so it is a READINESS signal, not liveness —
    /// same doctrine as the HTTP `/health`: do not wire it to a restart
    /// policy. The handler answers 200 healthy and 503 unhealthy, both with
    /// the same document shape — both parse here, so read
    /// `status == "unhealthy"` from the document rather than expecting an
    /// `Err` during a DB outage.
    pub async fn health(&self) -> Result<serde_json::Value, Error> {
        let resp = crate::handlers::handle_health(State(self.inner.st.clone())).await;
        let (status, bytes) = read_response(resp).await;
        if !status.is_success() && status != StatusCode::SERVICE_UNAVAILABLE {
            return Err(error_from(status, &bytes));
        }
        parse(&bytes)
    }

    /// Dead-lettered messages, newest first, filtered by `params` (queue,
    /// consumer group, limit/offset).
    pub async fn dlq(&self, params: &qp::DlqParams) -> Result<qp::DlqResponse, Error> {
        let mut query: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for (k, v) in params.to_pairs() {
            query.insert(k.to_string(), v);
        }
        let resp = crate::handlers::handle_dlq(
            State(self.inner.st.clone()),
            Extension(crate::tenant::Tenant::default_tenant()),
            Query(query),
        )
        .await;
        let (status, bytes) = read_response(resp).await;
        if !status.is_success() {
            return Err(error_from(status, &bytes));
        }
        parse(&bytes)
    }

    /// Replay a dead-lettered message: re-push its snapshot, then drop the DLQ
    /// row. Returns the handler's document (`success`, the new push outcome).
    pub async fn retry_message(
        &self,
        partition_id: &str,
        transaction_id: &str,
    ) -> Result<serde_json::Value, Error> {
        let resp = crate::handlers::handle_retry_message(
            State(self.inner.st.clone()),
            Extension(crate::auth::AuthedSub(None)),
            Extension(crate::tenant::Tenant::default_tenant()),
            Path((partition_id.to_string(), transaction_id.to_string())),
        )
        .await;
        let (status, bytes) = read_response(resp).await;
        if !status.is_success() {
            return Err(error_from(status, &bytes));
        }
        parse(&bytes)
    }

    /// Delete one message (and its DLQ row, if any) by partition and
    /// transaction id.
    pub async fn delete_message(
        &self,
        partition_id: &str,
        transaction_id: &str,
    ) -> Result<serde_json::Value, Error> {
        let resp = crate::handlers::handle_delete_message(
            State(self.inner.st.clone()),
            Extension(crate::tenant::Tenant::default_tenant()),
            Path((partition_id.to_string(), transaction_id.to_string())),
        )
        .await;
        let (status, bytes) = read_response(resp).await;
        if !status.is_success() {
            return Err(error_from(status, &bytes));
        }
        parse(&bytes)
    }

    // ------------------------------------------------------------ ephemeral
    //
    // EPHEMERAL_QUEUES.md §3.1, in-process. The embedded broker is by definition
    // a single-broker deployment, which is exactly the ownership topology the
    // engine implements, so nothing here is degraded relative to HTTP.
    //
    // WHY THESE TAKE AND RETURN `serde_json::Value` while every method above
    // takes a typed `queen_protocol` struct. The typed ephemeral structs belong
    // in `queen-protocol` (§10 Q7) alongside the ones `client-rust` will use, and
    // that crate is not this phase's to edit — publishing a second, divergent
    // set of owned types here and then having to keep them in step with the
    // protocol crate for ever is a worse outcome than one honest JSON seam. When
    // the protocol structs land, these signatures tighten and the bodies do not
    // move: they already go through the same handler the router dispatches to,
    // which is the property that makes running embedded BE running the broker.
    //
    // The errors are the HTTP ones (`Error::status()` carries the code), so the
    // ladder of §1.6 is observable from here exactly as a client sees it.

    /// `POST /api/v1/ephemeral/push` — `{queue, partition?, messages:[{payload}]}`.
    pub async fn ephemeral_push(&self, body: serde_json::Value) -> Result<serde_json::Value, Error> {
        self.eph(
            crate::handlers::handle_ephemeral_push(
                State(self.inner.st.clone()),
                Extension(crate::auth::AuthedSub(None)),
                Extension(crate::tenant::Tenant::default_tenant()),
                Bytes::from(body.to_string()),
            )
            .await,
        )
        .await
    }

    /// `GET /api/v1/ephemeral/pop` — the query string as key/value pairs.
    pub async fn ephemeral_pop(
        &self,
        params: &[(&str, String)],
    ) -> Result<serde_json::Value, Error> {
        let q: std::collections::HashMap<String, String> = params
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        self.eph(
            crate::handlers::handle_ephemeral_pop(
                State(self.inner.st.clone()),
                Extension(crate::auth::AuthedSub(None)),
                Extension(crate::tenant::Tenant::default_tenant()),
                Query(q),
            )
            .await,
        )
        .await
    }

    /// `POST /api/v1/ephemeral/ack` — `{queue, group?, acks:[{id, status?}]}`.
    pub async fn ephemeral_ack(&self, body: serde_json::Value) -> Result<serde_json::Value, Error> {
        self.eph(
            crate::handlers::handle_ephemeral_ack(
                State(self.inner.st.clone()),
                Extension(crate::auth::AuthedSub(None)),
                Extension(crate::tenant::Tenant::default_tenant()),
                Bytes::from(body.to_string()),
            )
            .await,
        )
        .await
    }

    /// `POST /api/v1/ephemeral/configure` — `{queue, options?}`. The options are
    /// a CLOSED list and an unknown key is a 400, not a silently ignored field.
    pub async fn ephemeral_configure(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        self.eph(
            crate::handlers::handle_ephemeral_configure(
                State(self.inner.st.clone()),
                Extension(crate::auth::AuthedSub(None)),
                Extension(crate::tenant::Tenant::default_tenant()),
                Bytes::from(body.to_string()),
            )
            .await,
        )
        .await
    }

    /// `POST /api/v1/ephemeral/reset` — drop every message, void every lease,
    /// rewind every cursor. Legal only because of the loss contract (§1.2).
    pub async fn ephemeral_reset(&self, queue: &str) -> Result<serde_json::Value, Error> {
        self.eph(
            crate::handlers::handle_ephemeral_reset(
                State(self.inner.st.clone()),
                Extension(crate::auth::AuthedSub(None)),
                Extension(crate::tenant::Tenant::default_tenant()),
                Bytes::from(serde_json::json!({ "queue": queue }).to_string()),
            )
            .await,
        )
        .await
    }

    /// `DELETE /api/v1/ephemeral/queue/:queue` — the rings AND the declaration.
    pub async fn ephemeral_delete(&self, queue: &str) -> Result<serde_json::Value, Error> {
        self.eph(
            crate::handlers::handle_ephemeral_delete_queue(
                State(self.inner.st.clone()),
                Extension(crate::auth::AuthedSub(None)),
                Extension(crate::tenant::Tenant::default_tenant()),
                Path(queue.to_string()),
            )
            .await,
        )
        .await
    }

    /// `GET /api/v1/ephemeral/queues` — declared and live implicit, zero DB.
    pub async fn ephemeral_queues(&self) -> Result<serde_json::Value, Error> {
        self.eph(
            crate::handlers::handle_ephemeral_queues(
                State(self.inner.st.clone()),
                Extension(crate::auth::AuthedSub(None)),
                Extension(crate::tenant::Tenant::default_tenant()),
            )
            .await,
        )
        .await
    }

    /// `GET /api/v1/ephemeral/queues/:queue/depth`. `Error::NotFound` when the
    /// queue is not on this broker — which for an implicit queue means it was
    /// never used or has been idle-collected, and both are honestly "not here".
    pub async fn ephemeral_depth(
        &self,
        queue: &str,
        group: Option<&str>,
    ) -> Result<serde_json::Value, Error> {
        let mut q: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        if let Some(g) = group {
            q.insert("group".into(), g.to_string());
        }
        self.eph(
            crate::handlers::handle_ephemeral_depth(
                State(self.inner.st.clone()),
                Extension(crate::auth::AuthedSub(None)),
                Extension(crate::tenant::Tenant::default_tenant()),
                Path(queue.to_string()),
                Query(q),
            )
            .await,
        )
        .await
    }

    /// One place where an ephemeral handler's rendered bytes become a result, so
    /// the eight methods above cannot disagree about what a refusal is.
    async fn eph(&self, resp: Response) -> Result<serde_json::Value, Error> {
        let (status, bytes) = read_response(resp).await;
        if !status.is_success() {
            return Err(error_from(status, &bytes));
        }
        parse(&bytes)
    }

    /// Stop the loops this handle owns, close the connection pool (idle
    /// Postgres connections drop immediately; any engine loop that survives —
    /// see the module docs — fails its next `pool.get()` and idles harmlessly),
    /// remove an auto-generated spool dir when it holds nothing, and report how
    /// many spooled push events are still on disk (0 in the common case). Safe
    /// to call once from any clone; later calls are no-ops. After shutdown,
    /// every operation on any clone fails with a pool error.
    pub async fn shutdown(&self) -> usize {
        for t in self.inner.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
        self.inner.st.pool.close();
        let pending = self.inner.st.file_buffer.pending_count();
        if pending > 0 {
            tracing::warn!(
                target: "shutdown",
                pending,
                "embedded broker: spool has undrained events at shutdown"
            );
        } else if let Some(dir) = &self.inner.auto_spool_dir {
            // Best-effort: the auto dir is documented non-durable, and leaving
            // empties behind would accumulate one dir per Broker lifetime.
            let _ = std::fs::remove_dir_all(dir);
        }
        tracing::info!(target: "shutdown", "embedded broker shut down");
        pending
    }
}

// ---------------------------------------------------------------- helpers

async fn read_response(resp: Response) -> (StatusCode, Bytes) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap_or_default();
    (status, bytes)
}

fn parse<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(bytes).map_err(|e| {
        Error::Decode(format!(
            "{e} (body: {})",
            String::from_utf8_lossy(&bytes[..bytes.len().min(256)])
        ))
    })
}

/// Map a non-2xx handler response to [`Error`], extracting the `{"error": …}`
/// body text when present.
fn error_from(status: StatusCode, bytes: &[u8]) -> Error {
    let msg = serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
        .unwrap_or_else(|| String::from_utf8_lossy(bytes).into_owned());
    match status {
        StatusCode::BAD_REQUEST => Error::InvalidRequest(msg),
        StatusCode::NOT_FOUND => Error::NotFound(msg),
        _ => Error::broker(
            Some(status.as_u16()),
            if msg.is_empty() {
                format!("status {}", status.as_u16())
            } else {
                msg
            },
        ),
    }
}

/// Build the handler-side params struct from the protocol one, going through
/// serde with the wire field names — the same names `Query` would parse — so
/// the two structs can never drift apart silently.
fn pop_params<T: serde::de::DeserializeOwned>(
    p: &qp::PopParams,
    namespace: Option<&str>,
    task: Option<&str>,
) -> Result<T, Error> {
    let mut m = serde_json::Map::new();
    if let Some(v) = p.batch {
        m.insert("batch".into(), v.into());
    }
    if let Some(v) = p.partitions {
        m.insert("partitions".into(), v.into());
    }
    if let Some(v) = p.auto_ack {
        m.insert("autoAck".into(), v.into());
    }
    if let Some(v) = p.wait {
        m.insert("wait".into(), v.into());
    }
    if let Some(v) = p.timeout_millis {
        m.insert("timeout".into(), v.into());
    }
    if let Some(v) = p.lease_seconds {
        m.insert("leaseSeconds".into(), v.into());
    }
    if let Some(v) = &p.consumer_group {
        m.insert("consumerGroup".into(), v.clone().into());
    }
    if let Some(v) = p.subscription_mode {
        m.insert("subscriptionMode".into(), v.as_str().into());
    }
    if let Some(v) = &p.subscription_from {
        m.insert("subscriptionFrom".into(), v.clone().into());
    }
    if let Some(v) = namespace {
        m.insert("namespace".into(), v.into());
    }
    if let Some(v) = task {
        m.insert("task".into(), v.into());
    }
    serde_json::from_value(serde_json::Value::Object(m)).map_err(|e| Error::Decode(e.to_string()))
}

/// Decode a pop handler response. 200 carries the rendered body; 204 is an
/// empty claim (or pop maintenance) with the body stripped, synthesized here as
/// an empty success. A `success: false` body is a real failure.
async fn pop_response(resp: Response, queue: &str) -> Result<qp::PopResponse, Error> {
    let (status, bytes) = read_response(resp).await;
    if status == StatusCode::NO_CONTENT {
        let mut empty: qp::PopResponse =
            serde_json::from_str("{}").map_err(|e| Error::Decode(e.to_string()))?;
        empty.queue = queue.to_string();
        return Ok(empty);
    }
    if !status.is_success() {
        return Err(error_from(status, &bytes));
    }
    let parsed: qp::PopResponse = parse(&bytes)?;
    if !parsed.success {
        return Err(Error::broker(
            None,
            parsed.error.unwrap_or_else(|| "pop failed".into()),
        ));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin DeleteQueueResult to the two body shapes handle_delete_queue emits:
    /// the SP's success passthrough and the handler's not-found mutation
    /// (handlers/queues.rs). A drift must surface as a parse error, not as
    /// defaulted fields.
    #[test]
    fn delete_queue_result_pins_both_bodies() {
        let ok: DeleteQueueResult =
            serde_json::from_str(r#"{"deleted":true,"queue":"q","existed":true}"#).unwrap();
        assert!(ok.existed && ok.deleted && ok.message.is_none());

        let missing: DeleteQueueResult = serde_json::from_str(
            r#"{"deleted":false,"queue":"q","existed":false,"message":"Queue not found, nothing was deleted"}"#,
        )
        .unwrap();
        assert!(!missing.existed && !missing.deleted);
        assert!(missing.message.is_some());

        // A body that lost the load-bearing keys must NOT parse.
        assert!(serde_json::from_str::<DeleteQueueResult>(r#"{"queue":"q"}"#).is_err());
    }

    /// Default::default() and new() must configure the same broker — the
    /// derive would silently flip every documented-true flag to false.
    #[test]
    fn default_matches_new() {
        let d = BrokerConfig::default();
        assert!(
            d.apply_schema && d.retention && d.stats_refresh && d.system_metrics && d.log_reports
        );
    }
}
