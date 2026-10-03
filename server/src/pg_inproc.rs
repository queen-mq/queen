//! IN-PROCESS MODE for the Postgres connectors (connectors/queen-pg,
//! PLAN_PG_CONNECTORS.md §6).
//!
//! The `queen-pg` library runs INSIDE this broker process, on every node: a
//! **source** streams committed changes of PostgreSQL tables into queues
//! (logical replication), a **sink** applies a queue to a table (a consumer
//! group the broker runs). Both give exactly-once effects with one rule: the
//! progress marker commits in the same transaction as the data, on the side
//! where the data lands. Nothing here decides anything about that rule; this
//! file only gives the engines a Queen to talk to, a place to run, and their
//! configuration.
//!
//! ## Configuration: documents in the broker-internal tenant
//! A connector is a JSON document stored by `PUT /api/v1/connectors/:name`
//! (handlers/connectors.rs) in KV namespace `queen-pg` of
//! [`crate::config::SYSTEM_TENANT`], key `conn:<tenant>:<name>`, forever. No
//! client addresses that tenant, so the documents — and the sealed passwords
//! in them — are reachable only through the connectors API. The [`Manager`]
//! reads them all again every `QUEEN_PG_RELOAD_MS` and makes the running units
//! match: a new document starts one, a changed one (its `updatedAt`, a resync
//! or a delete request) is stopped and started again, a removed one is
//! stopped and forgotten. Reading every few seconds rather than being told is
//! deliberate: a document written on any node reaches every node through the
//! replicated KV, with no notification path that could be lost.
//!
//! ## Every node runs every unit; leases decide who works
//! A source claims a TTL'd KV lease (`src:<name>:lease`, in the connector's
//! own tenant) and only the holder streams; the others report `standby` and
//! take over within a TTL when the holder dies, at once when it stops on
//! SIGTERM. A sink's workers pop through a consumer group, whose partition
//! leases already split the work between nodes. So a cluster needs no
//! placement of its own for connectors, and losing a node loses nothing.
//!
//! ## The Queen API, answered by this node's state machine
//! [`LocalQueen`] is the engines' [`QueenApi`], bound to one tenant, over the
//! typed twins of five routes: `Rsm::transaction` (pushes + the KV rider,
//! all-or-nothing — the source's exactly-once commit), the KV route's own
//! answer ([`crate::handlers::facade_kv`], minus the tenant KV rate ladder: a
//! connector's lease refreshes are the connector doing its job, as the Kafka
//! facade's offset commits are), `Rsm::pop_wildcard`, `Rsm::ack` and
//! `Rsm::renew`. Every failure is rendered the way the route renders it
//! ([`crate::handlers::raft::err_response`]) and handed over as its status,
//! body and `Retry-After`, so an engine retries a 429 or a 503 behind the
//! broker's own hint and stops on a 400, exactly as over HTTP. Every call is
//! SPAWNED onto the broker runtime and awaited from the connectors' runtime,
//! so broker code never runs on a connector thread.
//!
//! ## One combination that does not run
//! With `QUEEN_RAFT_CLIENT_OFFLOAD=false` only the leader serves typed calls
//! (`Rsm::route` is not `Local` on a follower). Such a follower runs no unit
//! (the manager stops them and says so under `/status`), and [`LocalQueen`]
//! answers every call there with a retryable 503 should one get through: the
//! leader's manager runs every connector. Offload is on by default, and with it
//! every node serves its own calls.
//!
//! ## Threads and blast radius
//! The engines run on a dedicated tokio runtime whose threads are named
//! `queen-pg` (`QUEEN_PG_THREADS`, default 2). It is not a core thread name
//! ([`crate::obs::panic_policy::CORE_THREAD_PREFIXES`]), so a panic there
//! unwinds and kills only its task; the unit supervisor catches it and starts
//! the connector again with the facades' ladder (1 s doubling to 30 s, reset
//! after an hour of healthy running). What in-process does NOT contain is
//! memory: a source's bundle in flight is this process's memory, bounded by the
//! document's `maxBundleBytes`.
//!
//! ## Stopping
//! On SIGTERM the broker calls [`InProcess::begin_shutdown`] at the signal:
//! every unit is stopped (a source flushes the bundle in flight, confirms it to
//! PostgreSQL and gives its lease back; a sink finishes the batch in flight).
//! Once the listener has drained, [`InProcess::shutdown`] waits for what is
//! left of `QUEEN_PG_SHUTDOWN_GRACE_MS` counted from the signal. Cut short,
//! nothing is lost: an uncommitted bundle or batch is redone by the next owner
//! from the progress marker.

use std::collections::{BTreeMap, HashMap};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use queen_pg::config::{ConnectorDoc, NodeKnobs};
use queen_pg::queen::{
    ack_body, parse_ack_answer, parse_kv_answer, parse_pop_answer, parse_txn_answer, AckAnswer,
    AckRequest, BoxFuture, KvAnswer, KvOp, KvRow, PopAnswer, PopRequest, QueenApi, QueenError,
    TxnAnswer, MAX_KV_PREFIX_LIMIT,
};
use queen_pg::{stop_pair, Connector, Context, Metrics, RunEnd, Stop, StopHandle};
use serde_json::{json, Map, Value};

use crate::config::{DEFAULT_TENANT, SYSTEM_TENANT};
use crate::encryption::Encryption;
use crate::rsm::facade::{
    check_message_key_names, AckReq, Deadline, PopOptions, PopReq, RenewReq, ReqCtx, Route, Rsm,
    RsmError, TxnReq,
};

/// The connectors runtime's thread name. It must NOT be a core thread name
/// ([`crate::obs::panic_policy::CORE_THREAD_PREFIXES`]): on a non-core thread a
/// panic unwinds and kills only its task, not the broker.
const THREAD_NAME: &str = "queen-pg";

/// Restart ladder of one connector, the facades' (kafka_inproc.rs).
const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const HEALTHY_RUN: Duration = Duration::from_secs(3600);

/// Blocking threads of the connectors runtime: a ceiling. The engines block
/// nowhere; the name lookups of their connections are the one user.
const MAX_BLOCKING_THREADS: usize = 16;

/// The budget of one call into the broker, the routes' default
/// (`DEFAULT_TIMEOUT`, 30 s).
const CALL_BUDGET: Duration = Duration::from_secs(30);

/// The longest long-poll a pop may ask for, the route's own ceiling
/// (`deadline_for` clamps a pop's timeout to 60 s).
const MAX_POP_WAIT_MS: u64 = 60_000;

/// The connectors' documents: `conn:<tenant>:<name>` in [`SYSTEM_TENANT`].
pub(crate) const DOC_PREFIX: &str = "conn:";

/// The 503 a node answers when it does not serve typed calls (see the module
/// header): retryable, and with a hint long enough not to spin.
const NOT_SERVED_BODY: &str = "{\"error\":\"this node does not serve connector calls \
                               (QUEEN_RAFT_CLIENT_OFFLOAD=false): the leader runs them\"}";
const NOT_SERVED_RETRY_MS: u64 = 30_000;

/// The document key of connector `name` of `tenant`.
pub(crate) fn doc_key(tenant: &str, name: &str) -> String {
    format!("{DOC_PREFIX}{tenant}:{name}")
}

/// `(tenant, name)` of a document key, `None` for a key that is not one.
pub(crate) fn parse_doc_key(key: &str) -> Option<(&str, &str)> {
    let (tenant, name) = key.strip_prefix(DOC_PREFIX)?.split_once(':')?;
    (!tenant.is_empty() && queen_pg::config::valid_connector_name(name)).then_some((tenant, name))
}

/// The metrics label of `tenant`: none for the default tenant, as on the
/// broker's own families.
fn tenant_label(tenant: &str) -> Option<String> {
    (tenant != DEFAULT_TENANT).then(|| tenant.to_string())
}

/// Now, as the documents and the status blocks write it:
/// `2026-10-02T10:00:00.123456Z`.
pub(crate) fn now_iso() -> String {
    crate::rsm::planner::timers::iso_us(now_us())
}

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// The Queen API, from this node's state machine.
// ---------------------------------------------------------------------------

/// [`QueenApi`] over this broker's state machine, for one tenant (see the
/// module header).
pub(crate) struct LocalQueen {
    rsm: Arc<dyn Rsm>,
    broker: tokio::runtime::Handle,
    tenant: String,
}

impl LocalQueen {
    /// `tenant`'s: its queues, its consumer groups, its KV — where a source's
    /// pointer must live, since it commits in the same transaction as the
    /// pushes and a transaction is single-tenant.
    pub(crate) fn new(
        rsm: Arc<dyn Rsm>,
        broker: tokio::runtime::Handle,
        tenant: impl Into<String>,
    ) -> LocalQueen {
        LocalQueen {
            rsm,
            broker,
            tenant: tenant.into(),
        }
    }

    fn ctx(&self, budget: Duration) -> ReqCtx {
        ReqCtx::new(self.tenant.clone(), Deadline::after(budget))
    }

    /// Run `call` on the broker runtime and await it from here. A node that
    /// does not serve typed calls answers the retryable 503 instead
    /// ([`NOT_SERVED_BODY`]), asked on the broker side like the call itself.
    fn on_broker<T, F, Fut>(
        &self,
        what: &'static str,
        call: F,
    ) -> BoxFuture<'static, Result<T, QueenError>>
    where
        T: Send + 'static,
        F: FnOnce(Arc<dyn Rsm>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, QueenError>> + Send + 'static,
    {
        let rsm = Arc::clone(&self.rsm);
        let task = self.broker.spawn(async move {
            if rsm.route() != Route::Local {
                return Err(not_served());
            }
            call(rsm).await
        });
        Box::pin(async move {
            task.await.map_err(|e| {
                QueenError::Transport(format!("the broker task serving {what} ended: {e}"))
            })?
        })
    }
}

fn not_served() -> QueenError {
    QueenError::Status {
        code: 503,
        body: NOT_SERVED_BODY.to_string(),
        retry_after_ms: Some(NOT_SERVED_RETRY_MS),
    }
}

/// The error the route would have answered for `e`
/// ([`crate::handlers::raft::err_response`]): its status, body and
/// `Retry-After`. So a 429 or a 503 is retried behind the broker's own hint
/// and a 400 stops the caller, exactly as the same answer over HTTP would.
async fn rsm_error(e: RsmError) -> QueenError {
    status_error(crate::handlers::raft::err_response(e)).await
}

async fn status_error(response: axum::response::Response) -> QueenError {
    let code = response.status().as_u16();
    let retry_after_ms = retry_after_ms(&response);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .unwrap_or_default();
    QueenError::Status {
        code,
        body,
        retry_after_ms,
    }
}

/// `Retry-After` in milliseconds: the delta-seconds form, the only one the
/// broker writes.
fn retry_after_ms(response: &axum::response::Response) -> Option<u64> {
    let seconds: u64 = response
        .headers()
        .get(axum::http::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    seconds.checked_mul(1_000)
}

/// The body of a 200 answer, or the error its status says.
async fn ok_body(response: axum::response::Response) -> Result<String, QueenError> {
    if response.status() != axum::http::StatusCode::OK {
        return Err(status_error(response).await);
    }
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .map_err(|e| QueenError::Transport(format!("in-process answer: {e}")))
}

impl QueenApi for LocalQueen {
    fn transaction(&self, body: String) -> BoxFuture<'_, Result<TxnAnswer, QueenError>> {
        let ctx = self.ctx(CALL_BUDGET);
        let answered = self.on_broker("a transaction", move |rsm| async move {
            match rsm
                .transaction(
                    ctx,
                    TxnReq {
                        raw: body.into_bytes(),
                    },
                )
                .await
            {
                // A commit AND a rollback verdict (`success:false`, `reason`
                // `kv_precondition` / `duplicate`) are 200s: the verdict is the
                // caller's to read, never an error.
                Ok(out) if out.status == 200 => Ok(out.body),
                // A body the wire refuses before anything is planned (400/413).
                Ok(out) => Err(QueenError::Status {
                    code: out.status,
                    body: out.body,
                    retry_after_ms: None,
                }),
                Err(e) => Err(rsm_error(e).await),
            }
        });
        // Parsed here, on a connector thread: the answer of a big bundle is
        // the connector's work, not the broker's.
        Box::pin(async move { parse_txn_answer(&answered.await?) })
    }

    fn kv(&self, ops: Vec<KvOp>) -> BoxFuture<'_, Result<KvAnswer, QueenError>> {
        let tenant = self.tenant.clone();
        let wire: Vec<Value> = ops.iter().map(KvOp::to_json).collect();
        let answered = self.on_broker("a KV call", move |rsm| async move {
            // The KV route's own answer, byte for byte: `{"results":[…]}`, the
            // 200 `{"ok":false,"reason":"kv_precondition",…}` of a lost
            // `required` precondition (nothing was written), and every refusal
            // rendered as the route renders it.
            let response = crate::handlers::facade_kv(rsm, tenant, wire, CALL_BUDGET).await;
            ok_body(response).await
        });
        Box::pin(async move { parse_kv_answer(&answered.await?) })
    }

    fn pop(&self, req: PopRequest) -> BoxFuture<'_, Result<PopAnswer, QueenError>> {
        let tenant = self.tenant.clone();
        let wait_ms = req.wait_ms.min(MAX_POP_WAIT_MS);
        // The deadline IS the long-poll: the facade holds an empty pop until
        // the request's deadline (`pop_run`), exactly as the route's deadline
        // is the client's `timeout`. A pop that does not wait gets the routes'
        // ordinary budget and answers at once when there is nothing.
        let ctx = self.ctx(if wait_ms > 0 {
            Duration::from_millis(wait_ms)
        } else {
            CALL_BUDGET
        });
        let answered = self.on_broker("a pop", move |rsm| async move {
            if let Err(e) = check_message_key_names(&tenant, &req.queue, Some(&req.group), None) {
                return Err(rsm_error(e).await);
            }
            let pop = PopReq {
                queue: req.queue,
                group: Some(req.group),
                batch: req.batch,
                auto_ack: false,
                wait: wait_ms > 0,
                timeout_ms: wait_ms,
                options: PopOptions {
                    // `None` delegates the claim width to the broker (the
                    // route's `autopilot=true` with no `partitions`).
                    max_parts: req.max_partitions.unwrap_or(1).clamp(1, 64),
                    auto_parts: req.max_partitions.is_none(),
                    lease_seconds: i32::try_from(req.lease_seconds).unwrap_or(i32::MAX),
                    subscription_mode: crate::config::normalize_subscription_mode(
                        &req.subscription_mode,
                    ),
                    ..PopOptions::default()
                },
            };
            match rsm.pop_wildcard(ctx, pop).await {
                // The route answers an empty claim with a bodiless 204.
                Ok(out) if out.empty => Ok(None),
                Ok(out) => Ok(Some(out.body)),
                Err(e) => Err(rsm_error(e).await),
            }
        });
        Box::pin(async move {
            match answered.await? {
                None => Ok(PopAnswer::default()),
                Some(body) => parse_pop_answer(&body),
            }
        })
    }

    fn ack(&self, req: AckRequest) -> BoxFuture<'_, Result<AckAnswer, QueenError>> {
        if req.items.is_empty() {
            return Box::pin(async { Ok(AckAnswer::default()) });
        }
        let tenant = self.tenant.clone();
        let ctx = self.ctx(CALL_BUDGET);
        let raw = ack_body(&req).to_string().into_bytes();
        let group = req.group;
        let answered = self.on_broker("an ack", move |rsm| async move {
            if let Err(e) = check_message_key_names(&tenant, "", Some(&group), None) {
                return Err(rsm_error(e).await);
            }
            match rsm
                .ack(
                    ctx,
                    AckReq {
                        queue: None,
                        group,
                        raw,
                    },
                )
                .await
            {
                Ok(out) => Ok(out.body),
                Err(e) => Err(rsm_error(e).await),
            }
        });
        Box::pin(async move { parse_ack_answer(&answered.await?) })
    }

    fn extend_lease(
        &self,
        lease_id: String,
        seconds: u32,
    ) -> BoxFuture<'_, Result<(), QueenError>> {
        let ctx = self.ctx(CALL_BUDGET);
        // A lease that is gone answers the route's 200 `success:false`, which
        // the trait has no place for: the batch it covered is redelivered,
        // and the sink's progress table drops what it already applied.
        self.on_broker("a lease extension", move |rsm| async move {
            match rsm
                .renew(
                    ctx,
                    RenewReq {
                        lease_id,
                        seconds: i64::from(seconds),
                    },
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => Err(rsm_error(e).await),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Boot.
// ---------------------------------------------------------------------------

/// The node knobs, resolved at boot by [`preflight`]: the API reads the egress
/// policy from them on a node that does not run the manager too.
static KNOBS: OnceLock<Arc<NodeKnobs>> = OnceLock::new();

/// `QUEEN_PG_*` (PLAN §3.3), read strictly at BOOT, before the state machine
/// opens: a value out of range is an error naming the variable, which the
/// broker turns into a fatal exit — retrying cannot fix it. `node` is the
/// broker's server id (`QUEEN_SERVER_ID` → `HOSTNAME` → random), written into
/// the source leases; `proxy_embedded` decides the egress default.
pub fn preflight(node: &str, proxy_embedded: bool) -> Result<NodeKnobs, String> {
    let knobs = NodeKnobs::from_env(node, proxy_embedded)?;
    let _ = KNOBS.set(Arc::new(knobs.clone()));
    Ok(knobs)
}

/// The egress policy a `PUT` checks a connector's host against: this node's
/// `QUEEN_PG_ALLOW_PRIVATE_NETWORKS`, or no restriction where the knobs were
/// never read (a test).
pub(crate) fn egress_policy() -> queen_pg::pg::connect::EgressPolicy {
    queen_pg::pg::connect::EgressPolicy {
        allow_private: KNOBS.get().is_none_or(|k| k.allow_private_networks),
    }
}

/// The running connectors of this node: what `run_raft` holds to stop them.
pub struct InProcess {
    stop: tokio::sync::watch::Sender<bool>,
    done: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    grace: Duration,
    /// When the drain began (the first [`InProcess::begin_shutdown`]): the
    /// grace is counted from here, not from when the broker gets round to
    /// waiting.
    stopping_since: OnceLock<Instant>,
}

/// Start the manager on its own runtime. Must be called from the broker
/// runtime: every call into the broker is spawned onto the runtime current
/// here.
pub fn start(knobs: NodeKnobs, rsm: Arc<dyn Rsm>) -> Arc<InProcess> {
    start_with(knobs, rsm, Encryption::from_env())
}

/// [`start`] with the key the passwords are unsealed with given rather than
/// read from `QUEEN_ENCRYPTION_KEY` (tests).
pub(crate) fn start_with(
    knobs: NodeKnobs,
    rsm: Arc<dyn Rsm>,
    encryption: Arc<Encryption>,
) -> Arc<InProcess> {
    let knobs = Arc::new(knobs);
    let metrics = Metrics::new();
    let status = Arc::new(Status::new(
        knobs.threads,
        knobs.reload_ms,
        Arc::clone(&metrics),
    ));
    let _ = STATUS.set(Arc::clone(&status));
    let manager = Manager::new(
        Arc::clone(&knobs),
        rsm,
        tokio::runtime::Handle::current(),
        encryption,
        metrics,
        status,
    );
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(knobs.threads.max(1))
        .max_blocking_threads(MAX_BLOCKING_THREADS)
        .thread_name(THREAD_NAME)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => crate::obs::fatal(format!("cannot build the Postgres connectors runtime: {e}")),
    };
    tracing::info!(
        target: "queen-pg",
        threads = knobs.threads,
        reload_ms = knobs.reload_ms,
        node = %knobs.node,
        "starting the Postgres connectors in-process"
    );
    let spawned = std::thread::Builder::new()
        // Inside the unwind prefix: the manager itself is connector code.
        .name(format!("{THREAD_NAME}-main"))
        .spawn(move || {
            runtime.block_on(manager.run(stop_rx));
            runtime.shutdown_timeout(Duration::from_millis(500));
            let _ = done_tx.send(());
        });
    if let Err(e) = spawned {
        crate::obs::fatal(format!("cannot start the Postgres connectors thread: {e}"));
    }
    Arc::new(InProcess {
        stop: stop_tx,
        done: Mutex::new(Some(done_rx)),
        grace: Duration::from_millis(knobs.shutdown_grace_ms),
        stopping_since: OnceLock::new(),
    })
}

impl InProcess {
    /// Stop every connector: a source flushes the bundle in flight, confirms
    /// it and gives its lease back; a sink finishes the batch in flight.
    /// Returns at once; [`InProcess::shutdown`] waits. Called at the signal,
    /// so the drain runs beside the broker's own (the ephemeral rings, the
    /// leadership hand-off, the listener) rather than after them.
    pub fn begin_shutdown(&self) {
        let _ = self.stopping_since.set(Instant::now());
        let _ = self.stop.send(true);
    }

    /// Stop the connectors and wait for them: at most
    /// `QUEEN_PG_SHUTDOWN_GRACE_MS` counted from the first
    /// [`InProcess::begin_shutdown`].
    pub async fn shutdown(&self) {
        self.begin_shutdown();
        let spent = self
            .stopping_since
            .get()
            .map_or(Duration::ZERO, |t| t.elapsed());
        let left = self.grace.saturating_sub(spent);
        let done = self.done.lock().ok().and_then(|mut d| d.take());
        if let Some(done) = done {
            if tokio::time::timeout(left, done).await.is_err() {
                tracing::warn!(
                    target: "shutdown",
                    grace_ms = self.grace.as_millis() as u64,
                    "the Postgres connectors did not stop inside their grace window; the next \
                     owner redoes the uncommitted bundle or batch from its progress marker"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The manager: one unit per document.
// ---------------------------------------------------------------------------

/// `(tenant, name)`.
type UnitKey = (String, String);

/// One unit for the status renderer: its key, what the supervisor knows of it
/// ([`unit_head`]), and its engine when one runs.
type UnitSnapshot = (UnitKey, Map<String, Value>, Option<Arc<Connector>>);

/// One document as the manager read it.
struct Wanted {
    tenant: String,
    name: String,
    /// The row's KV version: a teardown deletes exactly this document, so a
    /// re-`PUT` that landed in between wins.
    version: i64,
    /// `updatedAt`, `resyncRequestedAt` and `deleting`: every write of the
    /// connectors API changes one of them, and a change restarts the unit.
    fingerprint: String,
    /// `kind` as the document spells it, for the status of a document that
    /// does not parse.
    kind: Option<String>,
    doc: Result<ConnectorDoc, String>,
}

impl Wanted {
    fn from_row(row: KvRow) -> Option<Wanted> {
        let Some((tenant, name)) = parse_doc_key(&row.key) else {
            if let Some(suppressed) = BAD_KEY.tick_now() {
                tracing::warn!(
                    target: "queen-pg",
                    key = %row.key,
                    suppressed,
                    "a connector document under a key this broker cannot read is skipped"
                );
            }
            return None;
        };
        let fingerprint = format!(
            "{}|{}|{}",
            row.value["updatedAt"], row.value["resyncRequestedAt"], row.value["deleting"]
        );
        let kind = row.value["kind"].as_str().map(str::to_string);
        Some(Wanted {
            tenant: tenant.to_string(),
            name: name.to_string(),
            version: row.version,
            fingerprint,
            kind,
            doc: ConnectorDoc::from_json(&row.value).map_err(|e| e.to_string()),
        })
    }
}

static BAD_KEY: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
static DOCUMENTS_FAIL: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

struct Unit {
    gen: u64,
    fingerprint: String,
    stop: StopHandle,
    /// Whether the unit ever built an engine (and so a metrics series).
    engine: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

/// A unit whose document went away, while it stops.
struct Draining {
    gen: u64,
    engine: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

struct Manager {
    knobs: Arc<NodeKnobs>,
    rsm: Arc<dyn Rsm>,
    broker: tokio::runtime::Handle,
    encryption: Arc<Encryption>,
    metrics: Arc<Metrics>,
    status: Arc<Status>,
    /// The documents' tenant.
    system: Arc<LocalQueen>,
    units: HashMap<UnitKey, Unit>,
    draining: HashMap<UnitKey, Draining>,
    /// Bumped per unit started, so a stopped unit's last status report can
    /// never overwrite its successor's.
    gen: u64,
}

impl Manager {
    fn new(
        knobs: Arc<NodeKnobs>,
        rsm: Arc<dyn Rsm>,
        broker: tokio::runtime::Handle,
        encryption: Arc<Encryption>,
        metrics: Arc<Metrics>,
        status: Arc<Status>,
    ) -> Manager {
        let system = Arc::new(LocalQueen::new(
            Arc::clone(&rsm),
            broker.clone(),
            SYSTEM_TENANT,
        ));
        Manager {
            knobs,
            rsm,
            broker,
            encryption,
            metrics,
            status,
            system,
            units: HashMap::new(),
            draining: HashMap::new(),
            gen: 0,
        }
    }

    async fn run(mut self, mut stop: tokio::sync::watch::Receiver<bool>) {
        self.status.manager_phase("running");
        let reload = Duration::from_millis(self.knobs.reload_ms.max(1));
        loop {
            if *stop.borrow() {
                break;
            }
            // A panic in one pass (a bug here) must not end the reloads: it is
            // logged, and the next pass starts from what the KV says.
            if let Err(payload) = AssertUnwindSafe(self.reconcile()).catch_unwind().await {
                tracing::error!(
                    target: "queen-pg",
                    reason = %panic_text(payload.as_ref()),
                    "a connectors reload panicked; the next one starts over"
                );
            }
            tokio::select! {
                _ = tokio::time::sleep(reload) => {}
                _ = stop.wait_for(|stopping| *stopping) => break,
            }
        }
        self.status.manager_phase("stopping");
        self.stop_everything().await;
        self.status.manager_phase("stopped");
    }

    /// Stop every unit and wait for each.
    async fn stop_everything(&mut self) {
        for unit in self.units.values() {
            unit.stop.stop();
        }
        for (_, unit) in self.units.drain() {
            let _ = unit.task.await;
        }
        for (_, d) in self.draining.drain() {
            let _ = d.task.await;
        }
    }

    /// Read the documents and make the running units match them: start what
    /// is new, restart what changed, stop what went away. A read that fails
    /// changes nothing (it is said in the status and tried again).
    async fn reconcile(&mut self) {
        self.reap();
        if self.rsm.route() != Route::Local {
            // A follower that does not serve typed calls: the leader's
            // manager runs every connector (see the module header).
            if !self.units.is_empty() {
                tracing::info!(
                    target: "queen-pg",
                    units = self.units.len(),
                    "this node no longer serves connector calls (QUEEN_RAFT_CLIENT_OFFLOAD=false \
                     and not the leader): stopping its connectors"
                );
            }
            let keys: Vec<UnitKey> = self.units.keys().cloned().collect();
            for key in keys {
                self.retire(key);
            }
            self.status.manager_phase("not_serving");
            return;
        }
        self.status.manager_phase("running");
        let wanted = match self.read_documents().await {
            Ok(w) => {
                self.status.documents(None);
                w
            }
            Err(e) => {
                if let Some(suppressed) = DOCUMENTS_FAIL.tick_now() {
                    tracing::warn!(
                        target: "queen-pg",
                        error = %e,
                        suppressed,
                        "cannot read the connector documents; the running connectors are kept"
                    );
                }
                self.status.documents(Some(e));
                return;
            }
        };
        let mut seen: Vec<UnitKey> = Vec::with_capacity(wanted.len());
        for w in wanted {
            let key = (w.tenant.clone(), w.name.clone());
            seen.push(key.clone());
            if self
                .units
                .get(&key)
                .is_some_and(|u| u.fingerprint == w.fingerprint)
            {
                continue;
            }
            // New, or changed: the previous unit of the connector (if any) is
            // told to stop now and the new one starts once it has stopped, so
            // two units of one connector never run side by side on a node.
            let previous = match self.units.remove(&key) {
                Some(old) => {
                    old.stop.stop();
                    Some(old.task)
                }
                None => self.draining.remove(&key).map(|d| d.task),
            };
            let unit = self.spawn(w, previous);
            self.units.insert(key, unit);
        }
        let gone: Vec<UnitKey> = self
            .units
            .keys()
            .filter(|key| !seen.contains(key))
            .cloned()
            .collect();
        for key in gone {
            self.retire(key);
        }
    }

    /// Stop `key`'s unit; it is forgotten once it has stopped ([`Manager::reap`]).
    fn retire(&mut self, key: UnitKey) {
        if let Some(old) = self.units.remove(&key) {
            old.stop.stop();
            self.draining.insert(
                key,
                Draining {
                    gen: old.gen,
                    engine: old.engine,
                    task: old.task,
                },
            );
        }
    }

    /// Forget the retired units that have stopped: their status entry and,
    /// where an engine ran, their metrics series (a connector that comes back
    /// starts at 0, which a scraper reads as a restart).
    fn reap(&mut self) {
        let done: Vec<UnitKey> = self
            .draining
            .iter()
            .filter(|(_, d)| d.task.is_finished())
            .map(|(key, _)| key.clone())
            .collect();
        for key in done {
            let Some(d) = self.draining.remove(&key) else {
                continue;
            };
            if self.units.contains_key(&key) || !self.status.forget(&key, d.gen) {
                continue;
            }
            if d.engine.load(Ordering::SeqCst) {
                self.metrics.forget(tenant_label(&key.0).as_deref(), &key.1);
            }
        }
    }

    /// Every document, paged (`getPrefix` at the broker's own page ceiling).
    async fn read_documents(&self) -> Result<Vec<Wanted>, String> {
        let mut out = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let answer = self
                .system
                .kv(vec![KvOp::get_prefix(
                    DOC_PREFIX,
                    MAX_KV_PREFIX_LIMIT,
                    after.clone(),
                )])
                .await
                .map_err(|e| e.to_string())?;
            let page = answer
                .results
                .into_iter()
                .next()
                .ok_or_else(|| "the document read answered nothing".to_string())?;
            out.extend(page.rows.into_iter().filter_map(Wanted::from_row));
            match page.next_after {
                Some(next) if !next.is_empty() && after.as_deref() != Some(next.as_str()) => {
                    after = Some(next)
                }
                _ => break,
            }
        }
        Ok(out)
    }

    /// Start a unit for `w`: wait for `previous` (the connector's last unit)
    /// to stop, then run it.
    fn spawn(&mut self, w: Wanted, previous: Option<tokio::task::JoinHandle<()>>) -> Unit {
        self.gen += 1;
        let gen = self.gen;
        let key = (w.tenant.clone(), w.name.clone());
        let kind = match &w.doc {
            Ok(doc) => Some(doc.kind.as_str().to_string()),
            Err(_) => w.kind.clone(),
        };
        self.status.begin(&key, gen, kind);
        let (handle, stop) = stop_pair();
        let engine = Arc::new(AtomicBool::new(false));
        let unit = UnitCtx {
            key,
            gen,
            version: w.version,
            knobs: Arc::clone(&self.knobs),
            rsm: Arc::clone(&self.rsm),
            broker: self.broker.clone(),
            encryption: Arc::clone(&self.encryption),
            metrics: Arc::clone(&self.metrics),
            status: Arc::clone(&self.status),
            system: Arc::clone(&self.system),
            engine: Arc::clone(&engine),
        };
        let doc = w.doc;
        let task = tokio::spawn(async move {
            if let Some(previous) = previous {
                let _ = previous.await;
            }
            unit.run(doc, stop).await;
        });
        Unit {
            gen,
            fingerprint: w.fingerprint,
            stop: handle,
            engine,
            task,
        }
    }
}

/// What one unit's task needs.
struct UnitCtx {
    key: UnitKey,
    gen: u64,
    version: i64,
    knobs: Arc<NodeKnobs>,
    rsm: Arc<dyn Rsm>,
    broker: tokio::runtime::Handle,
    encryption: Arc<Encryption>,
    metrics: Arc<Metrics>,
    status: Arc<Status>,
    system: Arc<LocalQueen>,
    engine: Arc<AtomicBool>,
}

/// Why a unit runs no engine.
const UNSEAL_FAILED: &str = "the connector's password cannot be unsealed on this node: \
                             QUEEN_ENCRYPTION_KEY is unset or differs from the key of the node \
                             that sealed it (every node needs the same key)";

impl UnitCtx {
    fn tenant(&self) -> &str {
        &self.key.0
    }

    fn name(&self) -> &str {
        &self.key.1
    }

    /// Run the connector until told to stop: build it, run it, and start it
    /// again after an error or a panic with the facades' ladder.
    async fn run(self, doc: Result<ConnectorDoc, String>, stop: Stop) {
        let mut doc = match doc {
            Ok(doc) => doc,
            Err(e) => {
                // Nothing to run until the document changes, which starts a
                // new unit.
                self.status.failed(
                    &self.key,
                    self.gen,
                    "config",
                    &format!("the stored document does not parse: {e}"),
                );
                stop.wait().await;
                self.status.stopped(&self.key, self.gen);
                return;
            }
        };
        // A disabled connector runs nothing — unless it is being deleted: its
        // slot still pins WAL on the database, and the document waits for the
        // owner to drop it. The engine is handed the document as enabled then
        // (a disabled source only waits for its stop): a source being deleted
        // tears down right after it connects, before it streams anything.
        if !doc.enabled {
            if doc.deleting.is_none() {
                self.status.disabled(&self.key, self.gen);
                stop.wait().await;
                self.status.stopped(&self.key, self.gen);
                return;
            }
            doc.enabled = true;
        }
        let password = match self.unseal(&doc) {
            Ok(p) => p,
            Err(why) => {
                self.status.failed(&self.key, self.gen, "unseal", why);
                stop.wait().await;
                self.status.stopped(&self.key, self.gen);
                return;
            }
        };
        let mut backoff = BACKOFF_INITIAL;
        let mut current: Option<Arc<Connector>> = None;
        loop {
            if stop.is_stopped() {
                break;
            }
            let connector = match current.take() {
                Some(c) => c,
                None => match self.build(doc.clone(), password.clone()) {
                    Ok(c) => c,
                    Err((code, message, retry)) => {
                        self.status.failed(&self.key, self.gen, &code, &message);
                        if !retry {
                            // The document must change: a new unit then.
                            stop.wait().await;
                            break;
                        }
                        if stop.sleep(BACKOFF_MAX).await {
                            break;
                        }
                        continue;
                    }
                },
            };
            self.status
                .running(&self.key, self.gen, Arc::clone(&connector));
            let started = Instant::now();
            let outcome = AssertUnwindSafe(connector.run(stop.clone()))
                .catch_unwind()
                .await;
            if started.elapsed() >= HEALTHY_RUN {
                backoff = BACKOFF_INITIAL;
            }
            let delay = match outcome {
                Ok(Ok(RunEnd::TornDown)) => {
                    // The unit ends here, its status saying so until the
                    // document is gone and the next reload forgets it.
                    self.torn_down(&stop).await;
                    return;
                }
                Ok(Ok(RunEnd::Stopped)) | Ok(Err(queen_pg::Error::Stopped))
                    if stop.is_stopped() =>
                {
                    break;
                }
                Ok(Ok(RunEnd::Stopped)) | Ok(Err(queen_pg::Error::Stopped)) => {
                    self.status.exited(
                        &self.key,
                        self.gen,
                        "stopped",
                        "the connector stopped without being told to",
                        backoff,
                    );
                    current = Some(connector);
                    backoff
                }
                Ok(Err(e)) => {
                    if stop.is_stopped() {
                        break;
                    }
                    if matches!(e, queen_pg::Error::Config(_)) {
                        // Never retried: the document must change, which
                        // starts a new unit.
                        self.status
                            .failed(&self.key, self.gen, e.code(), &e.to_string());
                        stop.wait().await;
                        break;
                    }
                    // An operator's fix (a slot, a privilege, the WAL level)
                    // is tried again slowly; anything else with the ladder.
                    let delay = if e.is_retryable() {
                        backoff
                    } else {
                        BACKOFF_MAX
                    };
                    self.status
                        .exited(&self.key, self.gen, e.code(), &e.to_string(), delay);
                    tracing::warn!(
                        target: "queen-pg",
                        tenant = %self.tenant(),
                        connector = %self.name(),
                        code = e.code(),
                        error = %e,
                        retry_ms = delay.as_millis() as u64,
                        "a connector stopped on an error; starting it again"
                    );
                    current = Some(connector);
                    delay
                }
                Err(payload) => {
                    if stop.is_stopped() {
                        break;
                    }
                    let reason = format!("panicked: {}", panic_text(payload.as_ref()));
                    self.status
                        .exited(&self.key, self.gen, "panic", &reason, backoff);
                    tracing::error!(
                        target: "queen-pg",
                        tenant = %self.tenant(),
                        connector = %self.name(),
                        reason = %reason,
                        retry_ms = backoff.as_millis() as u64,
                        "a connector panicked; starting a new one"
                    );
                    // Whatever the panic left half-done is gone with it: the
                    // next run is a new connector.
                    backoff
                }
            };
            if stop.sleep(delay).await {
                break;
            }
            backoff = (backoff * 2).min(BACKOFF_MAX);
        }
        self.status.stopped(&self.key, self.gen);
    }

    /// The password the document's sealed form opens to, with this node's
    /// `QUEEN_ENCRYPTION_KEY`.
    fn unseal(&self, doc: &ConnectorDoc) -> Result<Option<String>, &'static str> {
        let Some(sealed) = doc.connection.password_sealed.as_deref() else {
            // No password stored (trust or certificate authentication): the
            // API never stores one in clear, so this is the document's own.
            return Ok(doc.connection.password.clone());
        };
        self.encryption
            .decrypt_payload_bytes(sealed.as_bytes())
            .and_then(|b| String::from_utf8(b).ok())
            .map(Some)
            .ok_or(UNSEAL_FAILED)
    }

    /// The engine, over this node's state machine as the connector's tenant.
    /// `Err((code, message, retry))`: why it could not be built, and whether
    /// building it again can help.
    fn build(
        &self,
        doc: ConnectorDoc,
        password: Option<String>,
    ) -> Result<Arc<Connector>, (String, String, bool)> {
        let api: Arc<dyn QueenApi> = Arc::new(LocalQueen::new(
            Arc::clone(&self.rsm),
            self.broker.clone(),
            self.tenant(),
        ));
        let ctx = Context {
            tenant: self.tenant().to_string(),
            tenant_label: tenant_label(self.tenant()),
            name: self.name().to_string(),
            api,
            knobs: Arc::clone(&self.knobs),
            metrics: Arc::clone(&self.metrics),
        };
        match std::panic::catch_unwind(AssertUnwindSafe(|| Connector::new(ctx, doc, password))) {
            Ok(Ok(c)) => {
                self.engine.store(true, Ordering::SeqCst);
                Ok(Arc::new(c))
            }
            Ok(Err(e)) => Err((
                e.code().to_string(),
                e.to_string(),
                !matches!(e, queen_pg::Error::Config(_)),
            )),
            Err(payload) => Err((
                "panic".to_string(),
                format!(
                    "building the connector panicked: {}",
                    panic_text(payload.as_ref())
                ),
                true,
            )),
        }
    }

    /// A source carried out its `deleting` request (slot and managed
    /// publication dropped, runtime KV removed): remove the document — this
    /// version of it only, so a re-`PUT` that landed meanwhile wins and its
    /// changed fingerprint starts a new unit at the next reload.
    async fn torn_down(&self, stop: &Stop) {
        self.status.torn_down(&self.key, self.gen);
        let key = doc_key(self.tenant(), self.name());
        let mut backoff = BACKOFF_INITIAL;
        loop {
            match self
                .system
                .kv(vec![KvOp::delete(key.clone(), Some(self.version))])
                .await
            {
                Ok(answer) => {
                    let applied = answer.results.first().is_some_and(|r| r.did_apply());
                    tracing::info!(
                        target: "queen-pg",
                        tenant = %self.tenant(),
                        connector = %self.name(),
                        removed = applied,
                        "a deleted source has dropped its slot; its document is {}",
                        if applied {
                            "removed"
                        } else {
                            "kept: it was written again meanwhile"
                        }
                    );
                    return;
                }
                Err(e) if e.is_retryable() => {
                    if stop.sleep(backoff).await {
                        return;
                    }
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
                Err(e) => {
                    self.status.failed(
                        &self.key,
                        self.gen,
                        "queen",
                        &format!("the torn-down connector's document cannot be removed: {e}"),
                    );
                    return;
                }
            }
        }
    }
}

fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string())
}

// ---------------------------------------------------------------------------
// /status and /metrics/prometheus
// ---------------------------------------------------------------------------

static STATUS: OnceLock<Arc<Status>> = OnceLock::new();

struct Status {
    threads: usize,
    reload_ms: u64,
    metrics: Arc<Metrics>,
    inner: Mutex<StatusInner>,
}

struct StatusInner {
    phase: &'static str,
    /// The last failed read of the documents, until one succeeds.
    documents_error: Option<String>,
    units: BTreeMap<UnitKey, UnitView>,
}

/// One unit, as the supervisor sees it. While its engine runs, the engine's
/// own status block is the unit's ([`Connector::status`]); otherwise the
/// supervisor says why nothing runs.
struct UnitView {
    gen: u64,
    kind: Option<String>,
    /// `starting`, `running`, `backoff`, `disabled`, `error`, `torn_down`,
    /// `stopped`.
    phase: &'static str,
    since_us: i64,
    /// `(code, message, at µs)`.
    error: Option<(String, String, i64)>,
    restarts: u64,
    retry_in_ms: u64,
    connector: Option<Arc<Connector>>,
}

impl Status {
    fn new(threads: usize, reload_ms: u64, metrics: Arc<Metrics>) -> Status {
        Status {
            threads,
            reload_ms,
            metrics,
            inner: Mutex::new(StatusInner {
                phase: "starting",
                documents_error: None,
                units: BTreeMap::new(),
            }),
        }
    }

    fn with<R>(&self, f: impl FnOnce(&mut StatusInner) -> R) -> R {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        f(&mut g)
    }

    /// The unit `gen` of `key`, if it is still the current one.
    fn unit(&self, key: &UnitKey, gen: u64, f: impl FnOnce(&mut UnitView)) {
        self.with(|s| {
            if let Some(u) = s.units.get_mut(key).filter(|u| u.gen == gen) {
                f(u);
            }
        });
    }

    fn manager_phase(&self, phase: &'static str) {
        self.with(|s| s.phase = phase);
    }

    fn documents(&self, error: Option<String>) {
        self.with(|s| s.documents_error = error);
    }

    fn begin(&self, key: &UnitKey, gen: u64, kind: Option<String>) {
        self.with(|s| {
            s.units.insert(
                key.clone(),
                UnitView {
                    gen,
                    kind,
                    phase: "starting",
                    since_us: now_us(),
                    error: None,
                    restarts: 0,
                    retry_in_ms: 0,
                    connector: None,
                },
            );
        });
    }

    fn set_phase(u: &mut UnitView, phase: &'static str) {
        if u.phase != phase {
            u.phase = phase;
            u.since_us = now_us();
        }
    }

    fn running(&self, key: &UnitKey, gen: u64, connector: Arc<Connector>) {
        self.unit(key, gen, |u| {
            if u.phase == "backoff" {
                u.restarts += 1;
            }
            Status::set_phase(u, "running");
            u.retry_in_ms = 0;
            u.connector = Some(connector);
        });
    }

    fn failed(&self, key: &UnitKey, gen: u64, code: &str, message: &str) {
        self.unit(key, gen, |u| {
            Status::set_phase(u, "error");
            u.error = Some((code.to_string(), clip(message), now_us()));
        });
    }

    fn exited(&self, key: &UnitKey, gen: u64, code: &str, message: &str, retry_in: Duration) {
        self.unit(key, gen, |u| {
            Status::set_phase(u, "backoff");
            u.error = Some((code.to_string(), clip(message), now_us()));
            u.retry_in_ms = retry_in.as_millis() as u64;
        });
    }

    fn disabled(&self, key: &UnitKey, gen: u64) {
        self.unit(key, gen, |u| Status::set_phase(u, "disabled"));
    }

    fn torn_down(&self, key: &UnitKey, gen: u64) {
        self.unit(key, gen, |u| Status::set_phase(u, "torn_down"));
    }

    fn stopped(&self, key: &UnitKey, gen: u64) {
        self.unit(key, gen, |u| Status::set_phase(u, "stopped"));
    }

    /// A unit whose document went away, once it has stopped — unless a newer
    /// unit took its place meanwhile. `true` when it was forgotten.
    fn forget(&self, key: &UnitKey, gen: u64) -> bool {
        self.with(|s| {
            if s.units.get(key).is_some_and(|u| u.gen == gen) {
                s.units.remove(key);
                true
            } else {
                false
            }
        })
    }

    /// The units, as `(key, view, engine)` snapshots taken under the lock;
    /// the engines' own status blocks are read after it is released.
    fn snapshot(&self, only: Option<&UnitKey>) -> Vec<UnitSnapshot> {
        self.with(|s| {
            s.units
                .iter()
                .filter(|(key, _)| only.is_none_or(|k| k == *key))
                .map(|(key, u)| (key.clone(), unit_head(key, u), u.connector.clone()))
                .collect()
        })
    }

    fn render(&self) -> Value {
        let (phase, documents_error) = self.with(|s| (s.phase, s.documents_error.clone()));
        let connectors: Vec<Value> = self
            .snapshot(None)
            .into_iter()
            .map(|(_, head, engine)| merge(head, engine))
            .collect();
        json!({
            "mode": "in-process",
            "phase": phase,
            "threads": self.threads,
            "reloadMs": self.reload_ms,
            "documentsError": documents_error,
            "connectors": connectors,
        })
    }

    fn render_one(&self, key: &UnitKey) -> Option<Value> {
        self.snapshot(Some(key))
            .into_iter()
            .next()
            .map(|(_, head, engine)| merge(head, engine))
    }
}

fn clip(message: &str) -> String {
    message.chars().take(1024).collect()
}

/// What the supervisor knows of a unit. `phase`/`since`/`error` are the
/// engine's own while it runs (see [`merge`]), the supervisor's otherwise.
fn unit_head(key: &UnitKey, u: &UnitView) -> Map<String, Value> {
    let mut out = Map::new();
    out.insert("tenant".into(), Value::String(key.0.clone()));
    out.insert("name".into(), Value::String(key.1.clone()));
    out.insert(
        "kind".into(),
        u.kind.clone().map(Value::String).unwrap_or(Value::Null),
    );
    out.insert("generation".into(), Value::from(u.gen));
    out.insert("restarts".into(), Value::from(u.restarts));
    if u.phase != "running" {
        // Not running: the supervisor's word. A unit waiting to start again
        // is in `error` (PLAN §4.7, §5.5), with when it retries.
        let phase = match u.phase {
            "backoff" => "error",
            other => other,
        };
        out.insert("phase".into(), Value::String(phase.to_string()));
        out.insert(
            "since".into(),
            Value::String(crate::rsm::planner::timers::iso_us(u.since_us)),
        );
        out.insert(
            "error".into(),
            match &u.error {
                Some((code, message, at)) => json!({
                    "code": code,
                    "message": message,
                    "at": crate::rsm::planner::timers::iso_us(*at),
                }),
                None => Value::Null,
            },
        );
        if u.phase == "backoff" {
            out.insert("retryInMs".into(), Value::from(u.retry_in_ms));
        }
    }
    out
}

/// A unit's entry: the engine's status block (its phase, its detail), with
/// the supervisor's head on top — the head's own `phase`/`error` win only
/// while the engine is not running.
fn merge(head: Map<String, Value>, engine: Option<Arc<Connector>>) -> Value {
    let mut out = match engine.map(|c| c.status()) {
        Some(Value::Object(m)) => m,
        _ => Map::new(),
    };
    for (k, v) in head {
        out.insert(k, v);
    }
    if !out.contains_key("phase") {
        out.insert("phase".into(), Value::String("starting".into()));
    }
    Value::Object(out)
}

/// The `pg` block of `GET /status` where the connectors run, or `None` where
/// they do not: the manager's phase, the threads and the reload cadence, and
/// one entry per connector document on this node — its tenant, name, kind,
/// generation (a new one per restart caused by a document change) and the
/// engine's own report ([`Connector::status`]: `standby`/`streaming`/… for a
/// source, `running` for a sink, with the error that names the fix).
pub fn status_value() -> Option<Value> {
    Some(STATUS.get()?.render())
}

/// One connector's entry of [`status_value`] on this node, for the API.
pub fn status_of(tenant: &str, name: &str) -> Option<Value> {
    STATUS
        .get()?
        .render_one(&(tenant.to_string(), name.to_string()))
}

/// Every connector's `queen_pg_*` families in Prometheus text, each family
/// once (labels `tenant` — absent for the default tenant — and `connector`),
/// for the broker's `/metrics/prometheus`; `None` where the connectors do not
/// run.
pub fn prometheus_text() -> Option<String> {
    Some(STATUS.get()?.metrics.prometheus())
}

#[cfg(test)]
mod tests;
