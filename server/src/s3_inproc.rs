//! IN-PROCESS MODE for the S3 / data-lake sink (connectors/queen-s3).
//!
//! `QUEEN_S3_EMBEDDED=true` runs the `queen-s3` library INSIDE this broker
//! process: no child to spawn, no binary to ship beside the broker, no socket
//! between the two.
//!
//! ## One sink per tenant
//! Every tenant mirrors to its own bucket with its own credentials:
//!
//! * the **default tenant**'s sink is configured by the environment
//!   (`QUEEN_S3_QUEUES` turns it on; the rest of `QUEEN_S3_*` as the 1.5.0
//!   binary read it, minus the broker address and token it no longer needs);
//! * **every other tenant**'s — a proxy cluster's broker tenant — by the
//!   control plane (`PUT /api/cp/clusters/:slug/s3`, proxy/src/cp.rs), which
//!   keeps the document in the proxy's own table (`px.s3sinks`) with the S3
//!   secret sealed by this cell's `QUEEN_ENCRYPTION_KEY`
//!   ([`ControlPlaneHooks`]). The [`Manager`] reads those rows again every
//!   [`RELOAD`] and starts, rebuilds or stops a tenant's sink to match, while
//!   its cluster is `active` or `push_blocked`.
//!
//! The node-wide knobs — the one memory budget every tenant's buffers share,
//! the lease TTL, the discovery cadence, the threads — stay environment-only
//! ([`queen_s3::config::NodeKnobs`]), and so does the metrics registry: one
//! exposition, the tenant a label of each series (absent for the default
//! tenant, as on the broker's own families). Every object path carries the
//! tenant (`tenant=<id>/queue=<q>/…`).
//!
//! ## Every node runs it; the lease decides who writes
//! In a cluster EVERY node starts the same sinks. For each queue every node
//! claims the queue's lease (`s3:<sink>:<queue>:lease` in KV namespace
//! `queen-s3` of the queue's own tenant, a TTL'd row taken with
//! `putIfAbsent`); one wins and runs the queue's window protocol, the others
//! retry once a TTL has passed. A node that dies stops refreshing its leases
//! and its queues move to the nodes still alive within a TTL; a node that stops
//! on SIGTERM gives them back at once. Every intent and commit batch carries
//! the lease as a `required` conditional write, so a node that lost its lease
//! while it was busy can never move a queue's commit pointer.
//!
//! ## The Queen API, served from this node's own state
//! [`LocalQueen`] is the sink's [`QueenApi`], answered by the state machine:
//!
//! * **fetch** and **discovery** are the typed twins of `POST /api/v1/fetch`
//!   and `POST /api/v1/partitions/changed` ([`Rsm::fetch_log`],
//!   [`Rsm::partitions_changed`]), read from THIS node's applied state — on a
//!   follower too, whatever the cluster routes. That is not an optimisation, it
//!   is what makes a window close sound: `safeTime` is the greatest stamp this
//!   node has applied, so it bounds what this node's reads can still be
//!   missing, and only this node's. A payload reaches the sink as the bytes it
//!   was stored as, never parsed into a tree on the way (a big integer stays
//!   exactly what the producer sent).
//! * **KV** — the lease, the window intent, the commit pointer — is the route's
//!   own `POST /api/v1/kv`, answered with the route's own body. Where this node
//!   takes writes it is [`crate::handlers::facade_kv`] (no tenant rate ladder:
//!   the sink's lease refreshes and claims are the sink doing its job, as the
//!   Kafka facade's offset commits are); on a follower that does not, it is the
//!   router, which forwards it to the leader.
//! * **the queue list** for `queues: *` is `GET /api/v1/resources/queues`.
//!
//! Each [`LocalQueen`] acts for one tenant: its reads, its queue list and its
//! KV documents are that tenant's, so the retention hold of a tenant's queues
//! reads the commit pointers of that tenant's sink. Every call is SPAWNED onto
//! the broker runtime and awaited from the sink's, so broker code never runs on
//! a sink thread.
//!
//! ## Threads, blast radius, memory
//! The sink's own work — window buffers, Parquet and zstd encoding, S3 uploads —
//! runs on a dedicated tokio runtime whose threads are named `queen-s3`
//! (`QUEEN_S3_THREADS`, default `cores / 4` clamped to 1..=2). A panic on one of
//! them unwinds and kills only its task ([`crate::obs::install_panic_hook`]
//! aborts the process only for a panic on a core thread); the crate restarts a
//! queue task that panicked, and this supervisor restarts the whole sink with
//! the facades' ladder (1 s doubling to 30 s, reset after an hour of healthy
//! running) if [`Sink::run`] itself ends. What in-process does NOT contain is
//! memory: the window buffers are this process's memory now, bounded by
//! `QUEEN_S3_MEMORY_MB` (default 512), and an allocation failure or the
//! kernel's OOM killer takes the broker with it. Size the container for it.
//!
//! ## Stopping
//! On SIGTERM the broker calls [`InProcess::begin_shutdown`] at the signal,
//! before its own ephemeral drain and leadership hand-off: every queue stops
//! reading, finishes the window it is committing, gives its lease back. Once
//! the listener has drained, [`InProcess::shutdown`] waits for what is left of
//! `QUEEN_S3_SHUTDOWN_GRACE_MS` counted from the signal (default 30 s: a
//! stopping sink has an open window and an upload possibly in flight, which a
//! facade closing sockets does not). Cut short, nothing is lost: the next
//! owner of the queue redoes the window from its intent and writes the same
//! bytes under the same keys.
//!
//! ## One combination that does not work
//! With `QUEEN_RAFT_CLIENT_OFFLOAD=false` a follower does not take writes, so
//! its KV batches go through the router to the leader — and with JWT auth on,
//! the leader refuses them (the sink carries no token). Those followers then
//! own no queue and the leader's sinks write them all; for a tenant other than
//! the default one a follower in that mode does not even try (the router could
//! not address the tenant). Offload is on by default, and with it every node
//! writes its own KV.

use std::collections::{BTreeMap, HashMap};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use queen_s3::config::NodeKnobs;
use queen_s3::queen::{BoxFuture, KvOp, KvResult, QueenApi};
use queen_s3::s3::ObjectStore;
use queen_s3::sink::SinkShared;
use queen_s3::types::{
    ChangedEntry, ChangedRequestEntry, ChangedResponse, FetchError, FetchRequestEntry,
    FetchedEntry, Micros, PartitionBounds, Record, SinkError,
};
use queen_s3::{Config, Sink};
use serde_json::value::RawValue;
use tower::ServiceExt;

use crate::config::S3SinkConfig;
use crate::rsm::facade::{ApiReq, ChangedAsk, Deadline, RecordFetch, ReqCtx, Route, Rsm, RsmError};

/// The sink runtime's thread name. It must NOT be a core thread name
/// ([`crate::obs::panic_policy::CORE_THREAD_PREFIXES`]): on a non-core thread a
/// panic unwinds and kills only its task, not the broker.
const THREAD_NAME: &str = "queen-s3";

/// Restart ladder of the whole sink, the facades' (kafka_inproc.rs).
const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const HEALTHY_RUN: Duration = Duration::from_secs(3600);

/// Blocking threads of the sink runtime: a ceiling, the sink blocks nowhere by
/// design.
const MAX_BLOCKING_THREADS: usize = 16;

/// The budget of one read or one queue listing, the routes' default
/// (`DEFAULT_TIMEOUT`, 30 s); a fetch adds the time it may park.
const CALL_BUDGET: Duration = Duration::from_secs(30);

/// The budget of one KV batch, as over HTTP (the Kafka facade's too).
const KV_BUDGET: Duration = Duration::from_secs(10);

/// The route's per-entry byte budget when a fetch entry names none, and its
/// ceiling (reads.rs `api_fetch`).
const FETCH_DEFAULT_MAX_BYTES: i64 = 1 << 20;
const FETCH_MAX_BYTES: i64 = 8 << 20;

// ---------------------------------------------------------------------------
// The Queen API, from this node's state machine.
// ---------------------------------------------------------------------------

/// [`QueenApi`] over this broker's state machine (see the module header).
pub(crate) struct LocalQueen {
    rsm: Arc<dyn Rsm>,
    /// For the one call this node may not answer itself: a KV write on a
    /// follower that does not take writes ([`Route::Leader`]).
    router: axum::Router,
    broker: tokio::runtime::Handle,
    tenant: String,
}

impl LocalQueen {
    /// The default tenant's (tests; the manager names every tenant).
    #[cfg(test)]
    pub(crate) fn new(
        rsm: Arc<dyn Rsm>,
        router: axum::Router,
        broker: tokio::runtime::Handle,
    ) -> LocalQueen {
        LocalQueen::for_tenant(rsm, router, broker, crate::config::DEFAULT_TENANT)
    }

    /// `tenant`'s: every read, every KV document and the queue list are that
    /// tenant's, so its leases and commit pointers live in its own KV, where
    /// the retention hold of its queues reads them.
    pub(crate) fn for_tenant(
        rsm: Arc<dyn Rsm>,
        router: axum::Router,
        broker: tokio::runtime::Handle,
        tenant: &str,
    ) -> LocalQueen {
        LocalQueen {
            rsm,
            router,
            broker,
            tenant: tenant.to_string(),
        }
    }

    fn ctx(&self, budget: Duration) -> ReqCtx {
        ReqCtx::new(self.tenant.clone(), Deadline::after(budget))
    }
}

/// The error the route would have answered for `e` ([`crate::handlers::raft::err_response`]):
/// its status, `Retry-After` and body, as the sink's [`SinkError::Status`]. So a
/// 429 or a 503 is retried behind the broker's own `Retry-After`, a 400 stops
/// the queue — exactly what the 1.5.0 sink did with the same answer over HTTP.
async fn sink_error(e: RsmError) -> SinkError {
    let response = crate::handlers::raft::err_response(e);
    status_error(response).await
}

async fn status_error(response: axum::response::Response) -> SinkError {
    let code = response.status().as_u16();
    let retry_after_ms = retry_after_ms(&response);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .unwrap_or_default();
    SinkError::Status {
        code,
        body,
        retry_after_ms,
    }
}

/// `Retry-After` in milliseconds: the delta-seconds form, the only one the
/// broker writes.
fn retry_after_ms(response: &axum::response::Response) -> Option<i64> {
    let seconds: i64 = response
        .headers()
        .get(axum::http::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    seconds.checked_mul(1_000).filter(|ms| *ms >= 0)
}

/// A stored payload as the record envelope's `payload`: the stored bytes
/// themselves when they are one JSON document (they always are for a push: its
/// `payload` field's raw text), `None` for an empty one (the route renders it
/// `null`), and otherwise — bytes that are not JSON, which only a payload the
/// broker could not decrypt is — a JSON string of their lossy UTF-8, as the
/// route renders them ([`crate::rsm::facade::real`]'s `payload_json`).
fn payload_value(bytes: &[u8]) -> Option<Box<RawValue>> {
    if bytes.is_empty() {
        return None;
    }
    if let Ok(text) = std::str::from_utf8(bytes) {
        if let Ok(raw) = RawValue::from_string(text.to_string()) {
            return Some(raw);
        }
    }
    let lossy = serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned());
    RawValue::from_string(lossy.to_string()).ok()
}

impl QueenApi for LocalQueen {
    fn fetch(
        &self,
        entries: Vec<FetchRequestEntry>,
        max_wait_ms: u64,
        min_bytes: i64,
    ) -> BoxFuture<'_, queen_s3::queen::Result<Vec<FetchedEntry>>> {
        let rsm = Arc::clone(&self.rsm);
        let max_wait_ms = max_wait_ms.min(30_000);
        let ctx = self.ctx(CALL_BUDGET + Duration::from_millis(max_wait_ms));
        // The route's bounds: a negative offset refuses the whole read (its
        // 400), a byte budget is clamped to 1 B..8 MiB.
        let asks: Option<Vec<RecordFetch>> = entries
            .iter()
            .map(|e| {
                (e.offset >= 0).then(|| RecordFetch {
                    queue: e.queue.clone(),
                    partition: e.partition.to_string(),
                    offset: e.offset as u64,
                    max_bytes: e
                        .max_bytes
                        .unwrap_or(FETCH_DEFAULT_MAX_BYTES)
                        .clamp(1, FETCH_MAX_BYTES) as usize,
                })
            })
            .collect();
        let task = self.broker.spawn(async move {
            let Some(asks) = asks else {
                return Err(SinkError::Status {
                    code: 400,
                    body: "{\"error\":\"offset must be non-negative\"}".to_string(),
                    retry_after_ms: None,
                });
            };
            match rsm
                .fetch_log(ctx, asks, max_wait_ms, min_bytes.max(0) as usize)
                .await
            {
                Ok(read) => Ok(read),
                Err(e) => Err(sink_error(e).await),
            }
        });
        Box::pin(async move {
            let read = task.await.map_err(|e| {
                SinkError::Transport(format!("the broker task serving a fetch ended: {e}"))
            })??;
            if read.len() != entries.len() {
                return Err(SinkError::Body(format!(
                    "fetch answered {} entries for {} asked",
                    read.len(),
                    entries.len()
                )));
            }
            Ok(entries
                .into_iter()
                .zip(read)
                .map(|(ask, got)| FetchedEntry {
                    records: got
                        .records
                        .into_iter()
                        .map(|r| Record {
                            partition: Arc::clone(&ask.partition),
                            offset: r.offset as i64,
                            transaction_id: r.txn.unwrap_or_default(),
                            ts: Micros(r.created_at_us),
                            payload: payload_value(&r.payload),
                        })
                        .collect(),
                    queue: ask.queue,
                    partition: ask.partition,
                    high_watermark: got.high_watermark as i64,
                    log_start_offset: got.log_start_offset as i64,
                    error: got.error.map(FetchError::from_wire),
                })
                .collect())
        })
    }

    fn partitions_changed(
        &self,
        entries: Vec<ChangedRequestEntry>,
    ) -> BoxFuture<'_, queen_s3::queen::Result<ChangedResponse>> {
        let rsm = Arc::clone(&self.rsm);
        let ctx = self.ctx(CALL_BUDGET);
        let asks: Vec<ChangedAsk> = entries
            .iter()
            .map(|e| ChangedAsk {
                queue: e.queue.clone(),
                since_us: e.since.map(|m| m.0),
                after: e.after.clone(),
                limit: e.limit as usize,
            })
            .collect();
        let task = self.broker.spawn(async move {
            match rsm.partitions_changed(ctx, asks).await {
                Ok(answer) => Ok(answer),
                Err(e) => Err(sink_error(e).await),
            }
        });
        Box::pin(async move {
            let answer = task.await.map_err(|e| {
                SinkError::Transport(format!("the broker task serving a discovery ended: {e}"))
            })??;
            if answer.entries.len() != entries.len() {
                return Err(SinkError::Body(format!(
                    "discovery answered {} entries for {} asked",
                    answer.entries.len(),
                    entries.len()
                )));
            }
            Ok(ChangedResponse {
                safe_time: Micros(answer.safe_time_us),
                // There is no degraded mode on raft: the value is the node's
                // applied stamp, always.
                safe_time_degraded: false,
                entries: entries
                    .into_iter()
                    .zip(answer.entries)
                    .map(|(ask, got)| ChangedEntry {
                        queue: ask.queue,
                        partitions: got
                            .partitions
                            .into_iter()
                            .map(|p| PartitionBounds {
                                name: Arc::from(p.name),
                                id: Some(p.id),
                                last_offset: p.last_offset,
                                log_start: p.log_start as i64,
                                last_write_at: Some(Micros(p.last_write_at_us)),
                            })
                            .collect(),
                        next: got.next,
                        error: got.error.map(str::to_string),
                    })
                    .collect(),
            })
        })
    }

    fn kv(&self, ops: Vec<KvOp>) -> BoxFuture<'_, queen_s3::queen::Result<Vec<KvResult>>> {
        let count = ops.len();
        if count == 0 {
            return Box::pin(async { Ok(Vec::new()) });
        }
        let rsm = Arc::clone(&self.rsm);
        let router = self.router.clone();
        let tenant = self.tenant.clone();
        let body = queen_s3::queen::kv_body(&ops);
        let task = self.broker.spawn(async move {
            // Only where the state machine takes writes; a follower that does
            // not hands the batch to the router, which forwards it — for the
            // default tenant only: the router names a tenant by a header that a
            // broker without tenancy ignores, and a tenant's documents written
            // into the default tenant would be another tenant's leases. Such a
            // node claims no tenant queue; a node that takes writes does
            // (with QUEEN_RAFT_CLIENT_OFFLOAD=false, the leader).
            if rsm.route() != Route::Local && tenant != crate::config::DEFAULT_TENANT {
                return Err(SinkError::Status {
                    code: 503,
                    body: "{\"error\":\"this node does not take writes and cannot forward a \
                           tenant's KV (QUEEN_RAFT_CLIENT_OFFLOAD=false)\"}"
                        .to_string(),
                    retry_after_ms: Some(30_000),
                });
            }
            let response = if rsm.route() == Route::Local {
                let operations = match body {
                    serde_json::Value::Object(mut o) => match o.remove("operations") {
                        Some(serde_json::Value::Array(ops)) => ops,
                        _ => Vec::new(),
                    },
                    _ => Vec::new(),
                };
                crate::handlers::facade_kv(rsm, tenant, operations, KV_BUDGET).await
            } else {
                let request = axum::http::Request::builder()
                    .method(axum::http::Method::POST)
                    .uri("/api/v1/kv")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .map_err(|e| SinkError::Transport(format!("in-process KV request: {e}")))?;
                match router.oneshot(request).await {
                    Ok(r) => r,
                    Err(never) => match never {},
                }
            };
            if !response.status().is_success() {
                return Err(status_error(response).await);
            }
            let status = response.status().as_u16();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .map_err(|e| SinkError::Transport(format!("in-process KV answer: {e}")))?;
            queen_s3::queen::parse_kv_answer(status, &String::from_utf8_lossy(&bytes), count)
        });
        Box::pin(async move {
            task.await.map_err(|e| {
                SinkError::Transport(format!("the broker task serving a KV batch ended: {e}"))
            })?
        })
    }

    fn list_queues(&self) -> BoxFuture<'_, queen_s3::queen::Result<Vec<String>>> {
        let rsm = Arc::clone(&self.rsm);
        let ctx = self.ctx(CALL_BUDGET);
        let task = self.broker.spawn(async move {
            // `stats=lanes`: names from one walk of the queue index, not the
            // dashboard's statistics (a pass over every partition and KV row
            // of the tenant), since every node re-lists while it runs.
            let req = ApiReq {
                method: "GET".to_string(),
                path: "/api/v1/resources/queues".to_string(),
                query: Some("stats=lanes".to_string()),
                body: Vec::new(),
            };
            match rsm.api(ctx, req).await {
                Ok(out) if (200..300).contains(&out.status) => Ok(out.body),
                Ok(out) => Err(SinkError::Status {
                    code: out.status,
                    body: out.body,
                    retry_after_ms: None,
                }),
                Err(e) => Err(sink_error(e).await),
            }
        });
        Box::pin(async move {
            let body = task.await.map_err(|e| {
                SinkError::Transport(format!("the broker task listing the queues ended: {e}"))
            })??;
            queue_names(&body)
        })
    }
}

/// The names of `GET /api/v1/resources/queues`, in the listing's order. Only
/// `name` is read: the rest of the answer is the dashboard's.
fn queue_names(body: &str) -> queen_s3::queen::Result<Vec<String>> {
    let root: serde_json::Value =
        serde_json::from_str(body).map_err(|e| SinkError::Body(format!("queue list: {e}")))?;
    Ok(root
        .get("queues")
        .and_then(serde_json::Value::as_array)
        .map(|queues| {
            queues
                .iter()
                .filter_map(|q| q.get("name").and_then(serde_json::Value::as_str))
                .filter(|n| !n.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default())
}

// ---------------------------------------------------------------------------
// Boot.
// ---------------------------------------------------------------------------

/// What the sink needs from the environment, resolved at BOOT before the state
/// machine opens: the node-wide knobs (one memory budget, the lease TTL, the
/// discovery cadence … shared by every tenant's sink on this node) and the
/// default tenant's own sink when `QUEEN_S3_QUEUES` asks for one. A value out
/// of range, or a default-tenant sink missing a variable it needs, fails the
/// broker's boot with the sink's own sentence, the contract the child mode had.
pub struct Preflight {
    node: NodeKnobs,
    env: Option<Config>,
}

pub fn preflight() -> Result<Preflight, String> {
    // The broker's two knobs of the sink are refused the same way when they
    // are set to something that is not a value; where they are read
    // (config.rs, [`worker_threads`]) they would fall back to their defaults
    // without a word.
    strictly_in("QUEEN_S3_THREADS", 1, 64)?;
    strictly_in("QUEEN_S3_SHUTDOWN_GRACE_MS", 100, 3_600_000)?;
    let mut node = NodeKnobs::from_env_with(&instance_default())?;
    // A lease row naming this node's own instance is taken back at once (it is
    // this node's earlier life, after a restart). Two nodes sharing a name
    // would take each other's queues back and forth — and an explicit
    // QUEEN_S3_INSTANCE is easily one value for every pod — so an explicit
    // name is made this node's own. The default already names the node.
    let explicit = std::env::var("QUEEN_S3_INSTANCE").is_ok_and(|v| !v.trim().is_empty());
    node.instance = lease_instance(&node.instance, explicit, raft_node_id());
    let env = Config::from_env(&node, crate::config::DEFAULT_TENANT)?;
    Ok(Preflight { node, env })
}

/// The name this node holds leases under: an explicit `QUEEN_S3_INSTANCE` made
/// this node's own, anything else (the default `node-<id>@<host>`, or a
/// generated id) as it is.
fn lease_instance(instance: &str, explicit: bool, node: u64) -> String {
    if explicit {
        unique_instance(instance, node)
    } else {
        instance.to_string()
    }
}

/// `configured`, with this node's raft id appended unless it already ends
/// with it.
fn unique_instance(configured: &str, node: u64) -> String {
    let suffix = format!("/node-{node}");
    if configured.ends_with(&suffix) {
        configured.to_string()
    } else {
        format!("{configured}{suffix}")
    }
}

/// This node's raft id (`QUEEN_RAFT_NODE_ID`), 1 for a single node.
fn raft_node_id() -> u64 {
    crate::rsm::replicator::raft::cluster::ClusterConfig::from_env()
        .ok()
        .flatten()
        .map_or(1, |c| c.node_id)
}

/// `Ok` when `name` is unset or an integer in `min..=max`; the sink's own
/// sentence otherwise.
fn strictly_in(name: &str, min: u64, max: u64) -> Result<(), String> {
    match std::env::var(name) {
        Err(_) => Ok(()),
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(n) if (min..=max).contains(&n) => Ok(()),
            _ => Err(format!("{name}={raw} is not an integer in {min}..={max}")),
        },
    }
}

/// The lease instance of this node when `QUEEN_S3_INSTANCE` is unset: the raft
/// node id (`QUEEN_RAFT_NODE_ID`, 1 for a single node) and the host name, so a
/// lease row says which node of which pod holds a queue.
fn instance_default() -> String {
    let node = raft_node_id();
    let host = std::env::var("HOSTNAME")
        .ok()
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .or_else(os_hostname)
        .unwrap_or_else(|| "localhost".to_string());
    format!("node-{node}@{host}")
}

fn os_hostname() -> Option<String> {
    let mut buf = [0u8; 256];
    // SAFETY: the buffer is valid for its length; gethostname NUL-terminates
    // within it or truncates, and the length is bounded by the slice below.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return None;
    }
    let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
    let name = String::from_utf8_lossy(&buf[..end]).trim().to_string();
    (!name.is_empty()).then_some(name)
}

/// `QUEEN_S3_THREADS`, default `cores / 4` clamped to 1..=2: the sink's work is
/// a window close every few minutes and an upload, not a request stream.
fn worker_threads() -> usize {
    let cores = std::thread::available_parallelism().map_or(2, |n| n.get());
    let default = (cores / 4).clamp(1, 2);
    std::env::var("QUEEN_S3_THREADS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| (1..=64).contains(n))
        .unwrap_or(default)
}

/// Where a sink's objects go: `None` builds the S3 client its configuration
/// names; a test hands each sink a bucket of its own.
pub type StoreFor = Arc<dyn Fn(&Config) -> Option<Arc<dyn ObjectStore>> + Send + Sync>;

/// A running in-process sink: what `run_raft` holds to stop it.
pub struct InProcess {
    stop: tokio::sync::watch::Sender<bool>,
    done: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    grace: Duration,
    /// When the drain began (the first [`InProcess::begin_shutdown`]): the
    /// grace is counted from here, not from when the broker gets round to
    /// waiting.
    stopping_since: OnceLock<Instant>,
}

/// Start the sinks on their own runtime: the default tenant's from the
/// environment when it has one, and one per tenant the control plane
/// configured, read again every [`RELOAD`]. Must be called from the broker
/// runtime: every call into the broker is spawned onto the runtime current
/// here. `stores` replaces the S3 clients (tests only).
pub fn start(
    knobs: &S3SinkConfig,
    pre: Preflight,
    router: axum::Router,
    rsm: Arc<dyn Rsm>,
    stores: Option<StoreFor>,
) -> Arc<InProcess> {
    start_with(
        knobs,
        pre,
        router,
        rsm,
        stores,
        crate::encryption::Encryption::from_env(),
    )
}

/// [`start`] with the key the control plane's secrets are opened with given
/// rather than read from `QUEEN_ENCRYPTION_KEY` (tests).
pub(crate) fn start_with(
    knobs: &S3SinkConfig,
    pre: Preflight,
    router: axum::Router,
    rsm: Arc<dyn Rsm>,
    stores: Option<StoreFor>,
    encryption: Arc<crate::encryption::Encryption>,
) -> Arc<InProcess> {
    let threads = worker_threads();
    let shared = Arc::new(SinkShared::new(&pre.node));
    let status = Arc::new(Status::new(threads, Arc::clone(&shared)));
    let _ = STATUS.set(Arc::clone(&status));
    let manager = Manager {
        node: pre.node,
        rsm,
        router,
        broker: tokio::runtime::Handle::current(),
        shared,
        stores,
        encryption,
        status,
        units: HashMap::new(),
        gen: 0,
    };
    // The default tenant's sink is built here, so a configuration its S3
    // client refuses fails the boot like every other configuration error.
    let env = pre.env.map(|cfg| {
        let tenant = crate::config::DEFAULT_TENANT;
        match manager.build(&cfg, tenant) {
            Ok(sink) => (cfg, sink),
            Err(e) => crate::obs::fatal(format!("QUEEN_S3_EMBEDDED=true: {e}")),
        }
    });
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
        Err(e) => crate::obs::fatal(format!("cannot build the S3 sink runtime: {e}")),
    };
    tracing::info!(
        target: "queen-s3",
        threads,
        env_sink = env.is_some(),
        "starting the S3 sink in-process"
    );
    let spawned = std::thread::Builder::new()
        // Inside the unwind prefix: the manager itself is sink code.
        .name(format!("{THREAD_NAME}-main"))
        .spawn(move || {
            runtime.block_on(manager.run(env, stop_rx));
            runtime.shutdown_timeout(Duration::from_millis(500));
            let _ = done_tx.send(());
        });
    if let Err(e) = spawned {
        crate::obs::fatal(format!("cannot start the S3 sink thread: {e}"));
    }
    Arc::new(InProcess {
        stop: stop_tx,
        done: Mutex::new(Some(done_rx)),
        grace: Duration::from_millis(knobs.shutdown_grace_ms),
        stopping_since: OnceLock::new(),
    })
}

impl InProcess {
    /// Tell every sink to stop: no new reads, the window in flight is
    /// finished and every lease given back. Returns at once;
    /// [`InProcess::shutdown`] waits. Called at the signal, so the drain runs
    /// beside the broker's own (the ephemeral rings, the leadership hand-off,
    /// the listener) rather than after them.
    pub fn begin_shutdown(&self) {
        let _ = self.stopping_since.set(Instant::now());
        let _ = self.stop.send(true);
    }

    /// Stop the sinks and wait for them: at most `QUEEN_S3_SHUTDOWN_GRACE_MS`
    /// counted from the first [`InProcess::begin_shutdown`].
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
                    "the in-process S3 sink did not stop inside its grace window; the next \
                     owner of its queues redoes the open windows from their intents"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The manager: one sink per tenant.
// ---------------------------------------------------------------------------

/// How often the control plane's sink rows are read again: a tenant whose
/// sink was configured, changed or removed is started, rebuilt or stopped
/// within this, on every node.
const RELOAD: Duration = Duration::from_secs(5);

/// Rows per page of the control plane's table.
const ROWS_PER_PAGE: u64 = 1000;

/// Whether a cluster's sink runs: its EFFECTIVE status — the worse of its
/// own and its tenant's, by the data plane's own rule
/// ([`queen_proxy::cache::merge_status`]) — is `Active` or `PushBlocked`
/// (pushes refused, data still there to reach the lake). A `Suspended` or
/// `Deleting` cluster's sink is stopped; an unknown status fails closed there.
fn sink_runs(tenant_status: &str, cluster_status: &str) -> bool {
    use queen_proxy::state::ClusterStatus;
    matches!(
        queen_proxy::cache::merge_status(tenant_status, cluster_status),
        ClusterStatus::Active | ClusterStatus::PushBlocked
    )
}

struct Manager {
    node: NodeKnobs,
    rsm: Arc<dyn Rsm>,
    router: axum::Router,
    broker: tokio::runtime::Handle,
    shared: Arc<SinkShared>,
    stores: Option<StoreFor>,
    encryption: Arc<crate::encryption::Encryption>,
    status: Arc<Status>,
    /// By broker tenant.
    units: HashMap<String, Unit>,
    /// Bumped per unit started, so a stopped unit's last status report can
    /// never overwrite its successor's.
    gen: u64,
}

/// One tenant's sink on this node, and what it was built from.
struct Unit {
    /// `env`, or the control-plane row's stamp with its cluster's status: when
    /// either changes, the sink is rebuilt.
    fingerprint: String,
    stop: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

/// The sink a control-plane row asks for.
struct Wanted {
    tenant: String,
    cluster: String,
    fingerprint: String,
    config: serde_json::Value,
    sealed: String,
}

impl Manager {
    /// A sink for `tenant` over this node's state machine, with the node's
    /// shared budget and metrics.
    fn build(&self, cfg: &Config, tenant: &str) -> Result<Arc<Sink>, String> {
        let queen: Arc<dyn QueenApi> = Arc::new(LocalQueen::for_tenant(
            Arc::clone(&self.rsm),
            self.router.clone(),
            self.broker.clone(),
            tenant,
        ));
        let store = self.stores.as_ref().and_then(|f| f(cfg));
        Sink::new_shared(cfg.clone(), queen, store, &self.shared).map(Arc::new)
    }

    async fn run(
        mut self,
        env: Option<(Config, Arc<Sink>)>,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) {
        self.status.phase("running");
        if let Some((cfg, first)) = env {
            let tenant = crate::config::DEFAULT_TENANT.to_string();
            let unit = self.spawn(
                &tenant,
                "env",
                None,
                "env".into(),
                Ok((cfg, Some(first))),
                None,
            );
            self.units.insert(tenant, unit);
        }
        loop {
            if *stop.borrow() {
                break;
            }
            self.reconcile().await;
            tokio::select! {
                _ = tokio::time::sleep(RELOAD) => {}
                _ = stop.wait_for(|stopping| *stopping) => break,
            }
        }
        for unit in self.units.values() {
            let _ = unit.stop.send(true);
        }
        for (_, unit) in self.units.drain() {
            let _ = unit.task.await;
        }
        self.status.phase("stopped");
    }

    /// Read the control plane's rows and make the running sinks match them:
    /// start what is new, rebuild what changed, stop what went away. A read
    /// that fails changes nothing (it is said in the status and tried again).
    async fn reconcile(&mut self) {
        let wanted = match self.read_rows().await {
            Ok(w) => {
                self.status.control_plane(None);
                w
            }
            Err(e) => {
                if let Some(suppressed) = CONTROL_PLANE_FAIL.tick_now() {
                    tracing::warn!(
                        target: "queen-s3",
                        error = %e,
                        suppressed,
                        "cannot read the control plane's S3 sink rows; the running sinks are kept"
                    );
                }
                self.status.control_plane(Some(e));
                return;
            }
        };
        let mut seen: Vec<String> = Vec::with_capacity(wanted.len());
        for w in wanted {
            seen.push(w.tenant.clone());
            if self
                .units
                .get(&w.tenant)
                .is_some_and(|u| u.fingerprint == w.fingerprint)
            {
                continue;
            }
            // New, or changed: the previous sink of the tenant (if any) is
            // told to stop now, and the new one starts once it has drained, so
            // two sinks of one tenant never run side by side on a node.
            let previous = self.units.remove(&w.tenant).map(|old| {
                let _ = old.stop.send(true);
                old.task
            });
            let built = self.config_of(&w).map(|cfg| (cfg, None));
            if let Err(e) = &built {
                tracing::error!(
                    target: "queen-s3",
                    tenant = %w.tenant,
                    cluster = %w.cluster,
                    error = %e,
                    "the control plane's sink for this tenant cannot be started"
                );
            }
            let unit = self.spawn(
                &w.tenant,
                "cp",
                Some(w.cluster.clone()),
                w.fingerprint.clone(),
                built,
                previous,
            );
            self.units.insert(w.tenant, unit);
        }
        // Gone from the control plane (deleted, disabled, its cluster no
        // longer running): stopped, and forgotten once drained.
        let gone: Vec<String> = self
            .units
            .iter()
            .filter(|(tenant, u)| u.fingerprint != "env" && !seen.contains(tenant))
            .map(|(tenant, _)| tenant.clone())
            .collect();
        for tenant in gone {
            if let Some(old) = self.units.remove(&tenant) {
                let _ = old.stop.send(true);
                let status = Arc::clone(&self.status);
                let shared = Arc::clone(&self.shared);
                tokio::spawn(async move {
                    let _ = old.task.await;
                    // Its counters too: a tenant that has no sink here any
                    // more exports nothing (one that comes back starts at 0,
                    // which a scraper reads as a restart).
                    if status.forget(&tenant) {
                        shared.forget_tenant(&tenant);
                    }
                });
            }
        }
    }

    /// The tenant's configuration from its row: the secret opened with this
    /// node's `QUEEN_ENCRYPTION_KEY`, the document read by the sink's own
    /// rules, the tenant labelled in the metrics.
    fn config_of(&self, w: &Wanted) -> Result<Config, String> {
        let secret = self
            .encryption
            .decrypt_payload_bytes(w.sealed.as_bytes())
            .and_then(|b| String::from_utf8(b).ok())
            .ok_or_else(|| {
                "the S3 secret cannot be opened on this node: QUEEN_ENCRYPTION_KEY is unset or \
                 differs from the key of the node that sealed it (every node needs the same key)"
                    .to_string()
            })?;
        let cfg = Config::from_tenant_doc(&self.node, &w.tenant, &w.config, &secret)?;
        Ok(cfg.with_tenant_label(Some(w.tenant.clone())))
    }

    /// Every row of the control plane's sink table whose sink should run: the
    /// row enabled, and its cluster's effective status ([`sink_runs`], the
    /// cluster's own and its tenant's) one the data plane serves.
    async fn read_rows(&self) -> Result<Vec<Wanted>, String> {
        use queen_proxy::store::schema::{ns, ClusterDoc, S3SinkDoc, TenantDoc, K};
        let mut docs: Vec<S3SinkDoc> = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let mut op = serde_json::json!({
                "op": "getPrefix",
                "ns": ns::S3SINKS,
                "prefix": K,
                "limit": ROWS_PER_PAGE,
            });
            if let Some(a) = &after {
                op["after"] = serde_json::Value::String(a.clone());
            }
            let page = self.proxy_kv(op).await?;
            for row in page["rows"].as_array().map(Vec::as_slice).unwrap_or(&[]) {
                match serde_json::from_value::<S3SinkDoc>(row["value"].clone()) {
                    Ok(doc) => docs.push(doc),
                    Err(e) => tracing::warn!(
                        target: "queen-s3",
                        key = %row["key"],
                        error = %e,
                        "a control-plane sink row this broker cannot read is skipped"
                    ),
                }
            }
            match page["nextAfter"].as_str() {
                Some(next) if !next.is_empty() => after = Some(next.to_string()),
                _ => break,
            }
        }
        if docs.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<String> = docs
            .iter()
            .map(|d| format!("{K}{}", d.cluster_id))
            .collect();
        let clusters = self
            .proxy_kv(serde_json::json!({"op": "getMany", "ns": ns::CLUSTERS, "keys": keys}))
            .await?;
        // cluster id -> (its status, its tenant's id)
        let mut cluster_of: HashMap<String, (String, String)> = HashMap::new();
        for row in clusters["rows"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or(&[])
        {
            if let Ok(c) = serde_json::from_value::<ClusterDoc>(row["value"].clone()) {
                cluster_of.insert(c.id.to_string(), (c.status, c.tenant_id.to_string()));
            }
        }
        let mut tenant_ids: Vec<String> = cluster_of
            .values()
            .map(|(_, t)| format!("{K}{t}"))
            .collect();
        tenant_ids.sort();
        tenant_ids.dedup();
        let tenants = self
            .proxy_kv(serde_json::json!({"op": "getMany", "ns": ns::TENANTS, "keys": tenant_ids}))
            .await?;
        let mut tenant_status: HashMap<String, String> = HashMap::new();
        for row in tenants["rows"].as_array().map(Vec::as_slice).unwrap_or(&[]) {
            if let Ok(t) = serde_json::from_value::<TenantDoc>(row["value"].clone()) {
                tenant_status.insert(t.id.to_string(), t.status);
            }
        }
        Ok(docs
            .into_iter()
            .filter(|d| d.enabled)
            .filter_map(|d| {
                let cluster = d.cluster_id.to_string();
                let (status, owner) = cluster_of.get(&cluster)?;
                // A tenant row that cannot be read is no status at all: the
                // rule fails closed on it.
                let owner_status = tenant_status.get(owner).map_or("", String::as_str);
                let tenant = d.broker_tenant.to_string();
                (sink_runs(owner_status, status) && tenant != crate::config::DEFAULT_TENANT).then(
                    || Wanted {
                        // Only the row's own stamp: a cluster going from
                        // active to push_blocked keeps its sink as it is.
                        fingerprint: d.updated_at_us.to_string(),
                        tenant,
                        cluster,
                        config: d.config,
                        sealed: d.secret_key_sealed,
                    },
                )
            })
            .collect())
    }

    /// One KV operation on the proxy's system tenant, answered as its result.
    async fn proxy_kv(&self, op: serde_json::Value) -> Result<serde_json::Value, String> {
        let rsm = Arc::clone(&self.rsm);
        let ctx = ReqCtx::new(
            queen_proxy::store::schema::PROXY_TENANT,
            Deadline::after(KV_BUDGET),
        );
        let task = self.broker.spawn(async move {
            rsm.kv(ctx, crate::rsm::facade::KvReq { ops: vec![op] })
                .await
                .map(|out| out.results)
        });
        let results = task
            .await
            .map_err(|e| format!("the broker task reading the control plane ended: {e}"))?
            .map_err(|e| format!("{e:?}"))?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| "the control plane read answered nothing".to_string())
    }

    /// Start a unit: wait for `previous` (the tenant's last sink) to drain, then
    /// run `built` — or, when it could not be built, keep its error in the
    /// status until the row changes.
    fn spawn(
        &mut self,
        tenant: &str,
        source: &'static str,
        cluster: Option<String>,
        fingerprint: String,
        built: Result<(Config, Option<Arc<Sink>>), String>,
        previous: Option<tokio::task::JoinHandle<()>>,
    ) -> Unit {
        self.gen += 1;
        let gen = self.gen;
        self.status.begin(tenant, gen, source, cluster);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let context = UnitContext {
            tenant: tenant.to_string(),
            gen,
            rsm: Arc::clone(&self.rsm),
            router: self.router.clone(),
            broker: self.broker.clone(),
            shared: Arc::clone(&self.shared),
            stores: self.stores.clone(),
            status: Arc::clone(&self.status),
        };
        let task = tokio::spawn(async move {
            if let Some(previous) = previous {
                let _ = previous.await;
            }
            context.run(built, stop_rx).await;
        });
        Unit {
            fingerprint,
            stop: stop_tx,
            task,
        }
    }
}

static CONTROL_PLANE_FAIL: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// What one unit's task needs to (re)build its sink.
struct UnitContext {
    tenant: String,
    gen: u64,
    rsm: Arc<dyn Rsm>,
    router: axum::Router,
    broker: tokio::runtime::Handle,
    shared: Arc<SinkShared>,
    stores: Option<StoreFor>,
    status: Arc<Status>,
}

impl UnitContext {
    fn build(&self, cfg: &Config) -> Result<Arc<Sink>, String> {
        let queen: Arc<dyn QueenApi> = Arc::new(LocalQueen::for_tenant(
            Arc::clone(&self.rsm),
            self.router.clone(),
            self.broker.clone(),
            &self.tenant,
        ));
        let store = self.stores.as_ref().and_then(|f| f(cfg));
        Sink::new_shared(cfg.clone(), queen, store, &self.shared).map(Arc::new)
    }

    /// Run the tenant's sink until told to stop, building a new one when a run
    /// ends on its own (a panic in the sink's own supervision, never a
    /// queue's: the crate restarts those itself), with the facades' ladder.
    async fn run(
        self,
        built: Result<(Config, Option<Arc<Sink>>), String>,
        stop: tokio::sync::watch::Receiver<bool>,
    ) {
        let (cfg, first) = match built {
            Ok(b) => b,
            Err(e) => {
                // Nothing to run until the row changes, which starts a new unit.
                self.status.failed(&self.tenant, self.gen, &e);
                let mut until = stop.clone();
                let _ = until.wait_for(|stopping| *stopping).await;
                return;
            }
        };
        let mut next = first.map(Ok);
        let mut backoff = BACKOFF_INITIAL;
        loop {
            if *stop.borrow() {
                break;
            }
            let sink = match next.take().unwrap_or_else(|| self.build(&cfg)) {
                Ok(sink) => sink,
                Err(e) => {
                    self.status.failed(&self.tenant, self.gen, &e);
                    let mut until = stop.clone();
                    let _ = until.wait_for(|stopping| *stopping).await;
                    break;
                }
            };
            let started = Instant::now();
            self.status
                .running(&self.tenant, self.gen, Arc::clone(&sink));
            let mut until = stop.clone();
            let stop_signal = async move {
                let _ = until.wait_for(|stopping| *stopping).await;
            };
            let outcome = AssertUnwindSafe(sink.run(stop_signal)).catch_unwind().await;
            if *stop.borrow() {
                break;
            }
            let reason = match outcome {
                Ok(()) => "the sink returned without being stopped".to_string(),
                Err(payload) => format!("panicked: {}", panic_text(payload.as_ref())),
            };
            if started.elapsed() >= HEALTHY_RUN {
                backoff = BACKOFF_INITIAL;
            }
            self.status.exited(&self.tenant, self.gen, &reason, backoff);
            tracing::error!(
                target: "queen-s3",
                tenant = %self.tenant,
                reason = %reason,
                backoff_ms = backoff.as_millis() as u64,
                "an in-process S3 sink stopped; restarting it"
            );
            let mut until = stop.clone();
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = until.wait_for(|stopping| *stopping) => break,
            }
            backoff = (backoff * 2).min(BACKOFF_MAX);
            // A run that ended uncleanly may have left its own state half-done
            // (its "running" mark among it): the next run is a new sink.
        }
        self.status.stopped(&self.tenant, self.gen);
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
// The control plane's side (proxy/src/cp.rs, `/api/cp/clusters/:slug/s3`).
// ---------------------------------------------------------------------------

/// What the proxy's control plane asks of the sink when a tenant's sink is
/// configured: the document checked by the sink's own rules, and the tenant's
/// S3 secret sealed with this cell's `QUEEN_ENCRYPTION_KEY` (the payload
/// envelope, AES-256-GCM), so the replicated KV, the raft log and every
/// snapshot hold it sealed and only a node with the same key opens it.
pub struct ControlPlaneHooks {
    encryption: Arc<crate::encryption::Encryption>,
}

impl ControlPlaneHooks {
    pub fn new() -> ControlPlaneHooks {
        ControlPlaneHooks::with(crate::encryption::Encryption::from_env())
    }

    pub(crate) fn with(encryption: Arc<crate::encryption::Encryption>) -> ControlPlaneHooks {
        ControlPlaneHooks { encryption }
    }
}

impl queen_proxy::s3::S3Sinks for ControlPlaneHooks {
    fn validate(&self, _broker_tenant: &str, config: &serde_json::Value) -> Result<(), String> {
        queen_s3::config::validate_tenant_doc(config)
    }

    fn seal(&self, secret: &str) -> Result<String, String> {
        if !self.encryption.is_enabled() {
            return Err(
                "this cell has no QUEEN_ENCRYPTION_KEY: a tenant's S3 secret is never stored in \
                 clear; set the same key on every node first"
                    .to_string(),
            );
        }
        self.encryption
            .encrypt(secret.as_bytes())
            .and_then(|b| String::from_utf8(b).ok())
            .ok_or_else(|| "sealing the S3 secret failed".to_string())
    }
}

// ---------------------------------------------------------------------------
// /status and /metrics/prometheus
// ---------------------------------------------------------------------------

static STATUS: OnceLock<Arc<Status>> = OnceLock::new();

struct Status {
    threads: usize,
    shared: Arc<SinkShared>,
    inner: Mutex<StatusInner>,
}

struct StatusInner {
    phase: &'static str,
    since: Instant,
    /// The last failed read of the control plane's rows, until one succeeds.
    control_plane: Option<String>,
    /// By broker tenant.
    units: BTreeMap<String, UnitView>,
}

struct UnitView {
    gen: u64,
    source: &'static str,
    cluster: Option<String>,
    phase: &'static str,
    since: Instant,
    error: Option<String>,
    restarts: u64,
    last_exit: Option<String>,
    backoff_ms: u64,
    sink: Option<Arc<Sink>>,
}

impl Status {
    fn new(threads: usize, shared: Arc<SinkShared>) -> Status {
        Status {
            threads,
            shared,
            inner: Mutex::new(StatusInner {
                phase: "starting",
                since: Instant::now(),
                control_plane: None,
                units: BTreeMap::new(),
            }),
        }
    }

    fn with(&self, f: impl FnOnce(&mut StatusInner)) {
        if let Ok(mut g) = self.inner.lock() {
            f(&mut g);
        }
    }

    /// The unit `gen` of `tenant`, if it is still the current one.
    fn unit(&self, tenant: &str, gen: u64, f: impl FnOnce(&mut UnitView)) {
        self.with(|s| {
            if let Some(u) = s.units.get_mut(tenant).filter(|u| u.gen == gen) {
                f(u);
            }
        });
    }

    fn phase(&self, phase: &'static str) {
        self.with(|s| {
            s.phase = phase;
            s.since = Instant::now();
        });
    }

    fn control_plane(&self, error: Option<String>) {
        self.with(|s| s.control_plane = error);
    }

    fn begin(&self, tenant: &str, gen: u64, source: &'static str, cluster: Option<String>) {
        self.with(|s| {
            s.units.insert(
                tenant.to_string(),
                UnitView {
                    gen,
                    source,
                    cluster,
                    phase: "starting",
                    since: Instant::now(),
                    error: None,
                    restarts: 0,
                    last_exit: None,
                    backoff_ms: 0,
                    sink: None,
                },
            );
        });
    }

    fn running(&self, tenant: &str, gen: u64, sink: Arc<Sink>) {
        self.unit(tenant, gen, |u| {
            if u.phase == "backoff" {
                u.restarts += 1;
            }
            u.phase = "running";
            u.since = Instant::now();
            u.backoff_ms = 0;
            u.sink = Some(sink);
        });
    }

    fn failed(&self, tenant: &str, gen: u64, error: &str) {
        self.unit(tenant, gen, |u| {
            u.phase = "error";
            u.since = Instant::now();
            u.error = Some(error.chars().take(512).collect());
        });
    }

    fn exited(&self, tenant: &str, gen: u64, reason: &str, backoff: Duration) {
        self.unit(tenant, gen, |u| {
            u.phase = "backoff";
            u.since = Instant::now();
            u.last_exit = Some(reason.chars().take(512).collect());
            u.backoff_ms = backoff.as_millis() as u64;
        });
    }

    fn stopped(&self, tenant: &str, gen: u64) {
        self.unit(tenant, gen, |u| {
            u.phase = "stopped";
            u.since = Instant::now();
        });
    }

    /// A tenant whose sink the control plane removed, once it has drained —
    /// unless a newer unit took its place meanwhile. `true` when it was
    /// forgotten.
    fn forget(&self, tenant: &str) -> bool {
        let mut forgotten = false;
        self.with(|s| {
            if s.units
                .get(tenant)
                .is_some_and(|u| matches!(u.phase, "stopped" | "error"))
            {
                s.units.remove(tenant);
                forgotten = true;
            }
        });
        forgotten
    }
}

/// The `s3` block of `GET /status` when the sink runs in-process, or `None`
/// when it does not: the manager's phase, the control plane's last read, and
/// one entry per tenant sink on this node — where it comes from (`env` for
/// the default tenant, `cp` for one the control plane configured), its own
/// phase, and the sink's report ([`Sink::status`]: the bucket, the health
/// verdict, a row per queue).
pub fn status_value() -> Option<serde_json::Value> {
    let st = STATUS.get()?;
    let (head, units) = {
        let g = st.inner.lock().ok()?;
        let head = serde_json::json!({
            "mode": "in-process",
            "phase": g.phase,
            "threads": st.threads,
            "controlPlane": { "error": g.control_plane },
        });
        let units: Vec<(String, serde_json::Value, Option<Arc<Sink>>)> = g
            .units
            .iter()
            .map(|(tenant, u)| {
                let v = serde_json::json!({
                    "tenant": tenant,
                    "source": u.source,
                    "cluster": u.cluster,
                    "phase": u.phase,
                    "error": u.error,
                    "restarts": u.restarts,
                    "lastExit": u.last_exit,
                    "uptimeMs": if u.phase == "running" { u.since.elapsed().as_millis() as u64 } else { 0 },
                    "backoffMs": u.backoff_ms,
                });
                (tenant.clone(), v, u.sink.clone())
            })
            .collect();
        (head, units)
    };
    let sinks: Vec<serde_json::Value> = units
        .into_iter()
        .map(|(_, mut v, sink)| {
            if let (Some(sink), Some(out)) = (sink, v.as_object_mut()) {
                if let serde_json::Value::Object(report) = sink.status() {
                    // The unit's own phase and error stay the unit's.
                    for (k, val) in report {
                        out.entry(k).or_insert(val);
                    }
                }
            }
            v
        })
        .collect();
    let mut v = head;
    if let Some(out) = v.as_object_mut() {
        out.insert("sinks".into(), serde_json::Value::Array(sinks));
    }
    Some(v)
}

/// Every sink's `queen_s3_*` families in Prometheus text, each family once,
/// for the broker's `/metrics/prometheus`; `None` when the sink does not run.
pub fn prometheus_text() -> Option<String> {
    Some(STATUS.get()?.shared.prometheus())
}

#[cfg(test)]
mod tests;
