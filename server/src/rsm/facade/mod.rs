//! `rsm/facade.rs` — the storage seam the HTTP handlers call in raft mode
//! (PLAN_RAFT.md §3.2 "receiver pipeline", §3.4 map row `mod.rs`, WP-1.7).
//!
//! In `QUEEN_STORAGE=postgres` (the default until GA, D1) nothing here is
//! reached: the handlers take their existing Postgres path. In
//! `QUEEN_STORAGE=raft` the receiver does its pre-work (§9.1) and hands a typed
//! command to this facade instead of a pooled connection. The facade is the
//! trait; WP-1.7c swaps the real [`Rsm`] implementation in behind the
//! [`build`] hook, and until then [`NotReady`] answers every mutating command
//! with [`RsmError::Unsupported`] (rendered `503 raft_phase1_unsupported` by
//! the handler layer), so the whole message path can be ROUTED now and wired
//! later.
//!
//! This module is deliberately free of `axum`: it speaks domain types and
//! [`RsmError`], and the HTTP layer (`handlers/raft.rs`) owns the mapping to
//! status codes and bodies. It is compiled in every feature set and both crate
//! roots, like the rest of `rsm/`.
//!
//! Invariants this seam keeps:
//! - **D7 / I4** nothing is answered before its entry is committed and applied:
//!   that is the real [`Rsm`]'s job (WP-1.6/1.7c); the stub answers only errors.
//! - **§9.1** the receiver mints one request id per command ([`ReqCtx::new`])
//!   and every command carries a [`Deadline`] (I15).
//! - **WP-1.2 finding R-108** composite name keys `(tenant, queue, group)` and
//!   `(tenant, queue, partition)` are bounded HERE, at the receiver, so no store
//!   key can exceed LMDB's 511-byte limit; [`check_message_key_names`] answers
//!   [`RsmError::NameTooLong`] (413) instead of letting the store refuse a key.

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::notify::Notifier;
use crate::util::uuidv7_bytes;

/// The real state machine (WP-1.7c): store + `LocalReplicator` + apply thread +
/// batcher + segments, behind the [`Rsm`] trait. Registered through the builder
/// hook ([`set_builder`]) by the boot paths; the [`NotReady`] stub is what a
/// build with no builder installed (the WP-1.7a seam tests) still gets.
pub mod real;

// ---------------------------------------------------------------------------
// Store key bound (WP-1.2 finding R-108).
// ---------------------------------------------------------------------------

/// LMDB's maximum key length (heed 0.22, the default 511-byte `MDB_MAXKEYSIZE`).
/// The store adapter (WP-1.2) refuses a longer key with a typed `KeyTooLong`; a
/// key postgres would accept (its name columns are `TEXT`) is one the store
/// cannot hold, so the receiver must reject it FIRST — with a clear 413 — rather
/// than let a valid-looking request fail deep in apply.
pub const MAX_STORE_KEY_BYTES: usize = 511;

/// Bytes reserved, per composite key, for the keyspace tag byte, the field
/// separators and any fixed key components the store codec prepends — including
/// the derived `pid` in the widest message-path key, `pending: (tenant, queue,
/// group, pid)` (§6.1). Held deliberately generous (the exact codec is WP-1.2's)
/// so the receiver's bound is always at or inside the store's real one — never
/// looser: rejecting a hair early is safe, letting a too-long key reach the
/// store is not.
pub const KEY_OVERHEAD_BYTES: usize = 64;

/// The name-length budget the three user-supplied names of the message path
/// share: `tenant`, `queue`, and the wider of `group`/`partition` (they live in
/// different keyspaces — `(tenant,queue,group)` for cursors/pending and
/// `(tenant,queue,partition)` for `partitions_by_key` — so the binding one is
/// the longer of the two).
pub const NAME_BUDGET_BYTES: usize = MAX_STORE_KEY_BYTES - KEY_OVERHEAD_BYTES;

/// Reject a message-path command whose composite store key would exceed
/// [`MAX_STORE_KEY_BYTES`]. `group` and `partition` are the two mutually
/// exclusive third components; pass whichever the command carries (or both, and
/// the wider one binds). On overflow the returned [`RsmError::NameTooLong`]
/// names the single longest offending user field so the client is told what to
/// shorten.
pub fn check_message_key_names(
    tenant: &str,
    queue: &str,
    group: Option<&str>,
    partition: Option<&str>,
) -> Result<(), RsmError> {
    let g = group.unwrap_or("");
    let p = partition.unwrap_or("");
    let third = g.len().max(p.len());
    if tenant.len() + queue.len() + third <= NAME_BUDGET_BYTES {
        return Ok(());
    }
    // Report the largest user-controlled name (never the tenant, which the proxy
    // sets and a client cannot shorten). Ties resolve to `queue`.
    let (field, len) = if queue.len() >= g.len() && queue.len() >= p.len() {
        ("queue", queue.len())
    } else if g.len() >= p.len() {
        ("group", g.len())
    } else {
        ("partition", p.len())
    };
    Err(RsmError::NameTooLong {
        field,
        len,
        limit: NAME_BUDGET_BYTES,
    })
}

// ---------------------------------------------------------------------------
// Deadlines (I15) and the per-command context (§9.1, D6).
// ---------------------------------------------------------------------------

/// A wall-clock deadline every facade call carries (I15: every I/O, RPC and
/// store call has a deadline). Computed by the receiver from the client
/// request's own budget (§9.1 step 5).
#[derive(Clone, Copy, Debug)]
pub struct Deadline {
    at: Instant,
}

impl Deadline {
    /// A deadline `budget` from now.
    pub fn after(budget: Duration) -> Self {
        Deadline {
            at: Instant::now() + budget,
        }
    }

    /// Remaining budget, or zero if already elapsed.
    pub fn remaining(&self) -> Duration {
        self.at.saturating_duration_since(Instant::now())
    }

    /// The underlying instant, for a blocking-pool call that takes an
    /// `Option<Instant>` budget (segment reads, §7.5).
    pub fn instant(&self) -> Instant {
        self.at
    }

    /// Whether the deadline has passed.
    pub fn expired(&self) -> bool {
        Instant::now() >= self.at
    }
}

/// What every command carries besides its payload: the tenant (from the
/// validated token / proxy header, never the body), the 16-byte request id the
/// receiver minted once for this command and reuses on every forwarding retry
/// (§5.4, D6), and the deadline.
#[derive(Clone, Debug)]
pub struct ReqCtx {
    pub tenant: String,
    pub request_id: [u8; 16],
    pub deadline: Deadline,
}

impl ReqCtx {
    /// Mint a fresh request id (uuidv7 bytes, §5.4) for a new command.
    pub fn new(tenant: impl Into<String>, deadline: Deadline) -> Self {
        ReqCtx {
            tenant: tenant.into(),
            request_id: uuidv7_bytes(),
            deadline,
        }
    }
}

// ---------------------------------------------------------------------------
// Typed errors (task: "typed errors incl. Retry and Unsupported").
// ---------------------------------------------------------------------------

/// The failure a facade call reports. The HTTP layer maps each to a status and
/// a `code`; the codes are the wire contract the SDKs and the proxy read.
#[derive(Clone, Debug)]
pub enum RsmError {
    /// This surface is not wired yet in raft phase 1 → `503 raft_phase1_unsupported`.
    /// The `NotReady` stub returns this for every mutating command.
    Unsupported,
    /// The leader is not known, or a propose was refused before append, or a
    /// step-down dropped the overlay: the command may be retried against the
    /// leader with the same request id (§7.1, D13). Carries the leader hint when
    /// one is known.
    Retry { leader_hint: Option<String> },
    /// No leader is known and the hold elapsed (D13) → `503 no_leader`.
    NoLeader,
    /// A composite store key would exceed LMDB's limit (R-108) → `413`.
    NameTooLong {
        field: &'static str,
        len: usize,
        limit: usize,
    },
    /// The cell is above the disk high-water mark (§11.8) → `507 storage_full`.
    StorageFull,
    /// The command's deadline elapsed before it could be answered (I15).
    Timeout,
    /// A non-retryable, whole-command refusal from the planner (§5.4, I14): a
    /// bad request the client must fix, not retry — rendered `400` with the
    /// planner's own `code`. Distinct from [`RsmError::Internal`] (a broker
    /// fault) and from a per-item refusal (which rides inside a `201` push
    /// body). WP-1.7c added it so the facade can surface `plan_*`'s client
    /// refusals as 4xx rather than 500.
    Rejected { code: String, message: String },
    /// An internal, non-retryable failure → `500`. Never produced by the stub.
    Internal(String),
}

impl RsmError {
    /// The stable `code` string the wire body carries for this error.
    pub fn code(&self) -> &'static str {
        match self {
            RsmError::Unsupported => "raft_phase1_unsupported",
            RsmError::Retry { .. } => "retry",
            RsmError::NoLeader => "no_leader",
            RsmError::NameTooLong { .. } => "name_too_long",
            RsmError::StorageFull => "storage_full",
            RsmError::Timeout => "timeout",
            RsmError::Rejected { .. } => "rejected",
            RsmError::Internal(_) => "internal",
        }
    }
}

impl std::fmt::Display for RsmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RsmError::Unsupported => write!(
                f,
                "raft phase 1: the message path is routed but not yet wired to the state machine"
            ),
            RsmError::Retry { leader_hint } => match leader_hint {
                Some(h) => write!(f, "not the leader; retry against {h}"),
                None => write!(f, "no quorum-ack lease; retry"),
            },
            RsmError::NoLeader => write!(f, "no leader is known; retry after"),
            RsmError::NameTooLong { field, len, limit } => write!(
                f,
                "{field} name is {len} bytes; the composite store key limit leaves {limit} bytes for names"
            ),
            RsmError::StorageFull => write!(f, "storage is full on at least one node"),
            RsmError::Timeout => write!(f, "the request deadline elapsed"),
            RsmError::Rejected { message, .. } => write!(f, "{message}"),
            RsmError::Internal(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for RsmError {}

// ---------------------------------------------------------------------------
// Command inputs and outcomes.
//
// These are the seam's provisional shapes: enough for the receiver to hand a
// typed command over and for the handler to render the answer. WP-1.7c fills
// the real pre-work (fusion packing, encryption, the repack after a duplicate
// verdict) and may widen these; nothing outside this module and `handlers/raft`
// depends on their internals.
// ---------------------------------------------------------------------------

/// A push command: the receiver's already-validated raw body plus the tenant on
/// the context. (WP-1.7c replaces `raw` with packed, hashed, encrypted frames.)
#[derive(Clone, Debug)]
pub struct PushReq {
    pub raw: Vec<u8>,
}

/// A wildcard (queue-scoped) pop — `GET /api/v1/pop/queue/:queue`.
#[derive(Clone, Debug)]
pub struct PopReq {
    pub queue: String,
    pub group: Option<String>,
    pub batch: u32,
    pub auto_ack: bool,
    pub wait: bool,
    pub timeout_ms: u64,
}

/// A pinned (single-partition) pop — `.../partition/:partition`.
#[derive(Clone, Debug)]
pub struct PopPinnedReq {
    pub queue: String,
    pub partition: String,
    pub group: Option<String>,
    pub batch: u32,
    pub auto_ack: bool,
    pub wait: bool,
    pub timeout_ms: u64,
}

/// A discovery pop — `GET /api/v1/pop` (namespace/task, no queue in the path).
#[derive(Clone, Debug)]
pub struct PopDiscoverReq {
    pub namespace: String,
    pub task: String,
    pub group: Option<String>,
    pub batch: u32,
    pub auto_ack: bool,
    pub wait: bool,
    pub timeout_ms: u64,
}

/// An ack / nack (single or batch): the receiver's raw body plus the resolved
/// consumer group. `queue` is present when the receiver already knows it.
#[derive(Clone, Debug)]
pub struct AckReq {
    pub queue: Option<String>,
    pub group: String,
    pub raw: Vec<u8>,
}

/// A lease renew — `POST /api/v1/lease/:leaseId/extend`.
#[derive(Clone, Debug)]
pub struct RenewReq {
    pub lease_id: String,
    pub seconds: i64,
}

/// A DLQ-head read (005 `log_dlq_head_v1`): the head of a group's dead-letter
/// stream for a queue.
#[derive(Clone, Debug)]
pub struct DlqHeadReq {
    pub queue: String,
    pub group: String,
}

/// A cheap pending probe used by the long-poll gate (§9.5): does this
/// (queue, group) have work?
#[derive(Clone, Debug)]
pub struct PendingReq {
    pub queue: String,
    pub group: Option<String>,
}

/// A depth read — `GET /api/v1/resources/queues/:queue/depth`.
#[derive(Clone, Debug)]
pub struct DepthReq {
    pub queue: String,
    pub group: Option<String>,
}

/// A push outcome: the rendered per-item JSON array the handler returns with
/// `201`. (Compact outcomes per §5.4 are recorded in state; the rendering is
/// the receiver's, §7.5.)
#[derive(Clone, Debug)]
pub struct PushOut {
    pub body: String,
}

/// A pop outcome: the rendered response body, and whether the claim came back
/// empty (so the long-poll path knows to park, §9.5).
#[derive(Clone, Debug)]
pub struct PopOut {
    pub body: String,
    pub empty: bool,
}

/// An ack outcome: the rendered response body.
#[derive(Clone, Debug)]
pub struct AckOut {
    pub body: String,
}

/// A transaction bundle — `POST /api/v1/transaction` (Phase B): the raw body.
#[derive(Clone, Debug)]
pub struct TxnReq {
    pub raw: Vec<u8>,
}

/// A transaction outcome: the rendered response body (HTTP 200 either way).
#[derive(Clone, Debug)]
pub struct TxnOut {
    pub body: String,
}

/// A renew outcome: the rendered response body.
#[derive(Clone, Debug)]
pub struct RenewOut {
    pub body: String,
}

/// A DLQ-head outcome: the rendered head row, or `None` when the stream is empty.
#[derive(Clone, Debug)]
pub struct DlqHeadOut {
    pub body: Option<String>,
}

/// A depth outcome: pending count for the addressed scope.
#[derive(Clone, Copy, Debug)]
pub struct DepthOut {
    pub pending: i64,
}

// ---------------------------------------------------------------------------
// KV (024, WP-2.2).
// ---------------------------------------------------------------------------

/// A KV call — `POST /api/v1/kv` and the three path routes — as the HTTP layer
/// hands it over: the op array, exactly as `kv_apply_v1` would have received
/// it. The tenant is on the [`ReqCtx`] and is never read from an op (024 §6.1
/// point 6).
#[derive(Clone, Debug)]
pub struct KvReq {
    pub ops: Vec<serde_json::Value>,
}

/// The answer to a KV call: one element per op, index-aligned, in 024's
/// shapes (§6.4).
#[derive(Clone, Debug)]
pub struct KvOut {
    pub results: Vec<serde_json::Value>,
}

/// The console's page read (`POST /api/v1/resources/kv/list`,
/// `kv_list_v1`): every field already resolved by the HTTP layer except the
/// limit, which the store side clamps (its one home).
#[derive(Clone, Debug)]
pub struct KvListReq {
    pub namespace: String,
    pub prefix: String,
    /// The exclusive cursor; `None` (or empty) for the first page.
    pub after: Option<String>,
    pub limit: Option<i64>,
    pub keys_only: bool,
    pub include_expired: bool,
}

/// Why a KV call did not produce an answer array.
#[derive(Clone, Debug)]
pub enum KvFailure {
    /// 024's shape and size refusals: `status` 400 (SQLSTATE 22023) or 413
    /// (22001), `reason` the SP's stable MESSAGE (`kv_bad_namespace`, …),
    /// `detail` its DETAIL (it names only what the caller sent).
    Invalid {
        status: u16,
        reason: String,
        detail: String,
    },
    /// A write that lost its precondition with `"required": true` (024's
    /// 23514): nothing was written, and the answer is a 200 built from this
    /// DETAIL JSON (cut at 4096 characters like the SP's).
    Precondition { detail: String },
    /// Anything the facade itself answers (retry, no leader, timeout, …).
    Rsm(RsmError),
}

// ---------------------------------------------------------------------------
// Health (§14.1).
// ---------------------------------------------------------------------------

/// The `raft` block `/health` adds in raft mode (§14.1). Node-local; the values
/// come from the replicator once it exists (WP-1.6/3.x).
#[derive(Clone, Debug)]
pub struct RaftHealth {
    pub role: String,
    pub leader_known: bool,
    pub term: u64,
    pub applied: u64,
    pub commit: u64,
    pub lag_ms: u64,
    /// Whether the state machine is wired and serving (false under the phase-1
    /// stub). Distinct from process liveness: the broker is up and answers
    /// `/health`, but the message path is not yet on the RSM.
    pub storage_ready: bool,
}

impl RaftHealth {
    /// Render the `raft` object exactly as `/health` embeds it.
    pub fn to_json(&self) -> String {
        format!(
            "{{\"role\":\"{}\",\"leader\":{},\"term\":{},\"applied\":{},\"commit\":{},\"lag\":{},\"storageReady\":{}}}",
            self.role, self.leader_known, self.term, self.applied, self.commit, self.lag_ms, self.storage_ready
        )
    }

    /// Whether `/health` should answer `200 healthy` (a leader is known and the
    /// apply lag is under the ready threshold, §14.1). In phase 1 there is no
    /// consensus lag to gate on, so the stub reports ready.
    pub fn ready(&self, ready_lag_ms: u64) -> bool {
        self.leader_known && self.lag_ms <= ready_lag_ms
    }
}

// ---------------------------------------------------------------------------
// The facade trait.
// ---------------------------------------------------------------------------

/// The storage seam the receiver calls in raft mode. One method per message-path
/// operation the receiver routes in WP-1.7 (push, the three pop variants, ack,
/// renew, dlq-head, the long-poll pending probe, depth). Every mutating call is
/// async, carries a [`Deadline`] (I15), and answers with a typed [`RsmError`].
///
/// `#[async_trait]` boxes the returned futures so the facade can live behind an
/// `Arc<dyn Rsm>` (the same pattern as the `Replicator` seam), which is what the
/// builder hook returns.
#[async_trait]
pub trait Rsm: Send + Sync {
    /// `POST /api/v1/push`.
    async fn push(&self, ctx: ReqCtx, req: PushReq) -> Result<PushOut, RsmError>;
    /// `GET /api/v1/pop/queue/:queue` (wildcard over the queue's partitions).
    async fn pop_wildcard(&self, ctx: ReqCtx, req: PopReq) -> Result<PopOut, RsmError>;
    /// `GET /api/v1/pop/queue/:queue/partition/:partition` (pinned).
    async fn pop_pinned(&self, ctx: ReqCtx, req: PopPinnedReq) -> Result<PopOut, RsmError>;
    /// `GET /api/v1/pop` (namespace/task discovery).
    async fn pop_discover(&self, ctx: ReqCtx, req: PopDiscoverReq) -> Result<PopOut, RsmError>;
    /// `POST /api/v1/ack` and `POST /api/v1/ack/batch`.
    async fn ack(&self, ctx: ReqCtx, req: AckReq) -> Result<AckOut, RsmError>;
    /// `POST /api/v1/lease/:leaseId/extend`.
    async fn renew(&self, ctx: ReqCtx, req: RenewReq) -> Result<RenewOut, RsmError>;
    /// `POST /api/v1/transaction` (Phase B): push + ack all-or-nothing.
    async fn transaction(&self, ctx: ReqCtx, req: TxnReq) -> Result<TxnOut, RsmError>;
    /// The head of a group's DLQ stream (005), read on the ack path.
    async fn dlq_head(&self, ctx: ReqCtx, req: DlqHeadReq) -> Result<DlqHeadOut, RsmError>;
    /// A cheap indexed pending probe for the long-poll gate (§9.5). A local
    /// stale read on a follower; never parks by itself.
    async fn has_pending(&self, ctx: ReqCtx, req: PendingReq) -> Result<bool, RsmError>;
    /// Queue depth (pending count) for the addressed scope.
    async fn depth(&self, ctx: ReqCtx, req: DepthReq) -> Result<DepthOut, RsmError>;

    /// A KV call (024 `kv_apply_v1`, HTTP surface: `getPrefix` allowed, the
    /// HTTP budgets). The default is the un-ported answer, so a facade that
    /// does not serve KV needs no code for it.
    async fn kv(&self, _ctx: ReqCtx, _req: KvReq) -> Result<KvOut, KvFailure> {
        Err(KvFailure::Rsm(RsmError::Unsupported))
    }

    /// The console's page of one namespace (`kv_list_v1`): the JSON object
    /// `{rows, truncated, nextAfter, bytes}`, rendered.
    async fn kv_list(&self, _ctx: ReqCtx, _req: KvListReq) -> Result<String, KvFailure> {
        Err(KvFailure::Rsm(RsmError::Unsupported))
    }

    /// The console's namespace selector (`kv_namespaces_v1`): the JSON array
    /// `[{namespace, keys}]`, rendered.
    async fn kv_namespaces(&self, _ctx: ReqCtx) -> Result<String, KvFailure> {
        Err(KvFailure::Rsm(RsmError::Unsupported))
    }

    /// The `/health` raft block (§14.1). Cheap and non-async: a node-local read.
    fn health(&self) -> RaftHealth;

    /// The notifier long-poll pops park on (§9.5). The apply thread wakes it
    /// after applying `Append`s and cursor releases; the receiver parks a
    /// waiting pop on it. `None` when the facade has no notifier yet (the stub
    /// never has messages to wake for).
    fn notifier(&self) -> Option<&Arc<Notifier>> {
        None
    }
}

// ---------------------------------------------------------------------------
// The NotReady stub (WP-1.7a): routes exist, the RSM does not.
// ---------------------------------------------------------------------------

/// The phase-1 facade: every mutating command answers [`RsmError::Unsupported`]
/// (rendered `503 raft_phase1_unsupported`), so the handlers can be routed and
/// tested now while WP-1.7c installs the real state machine behind [`build`].
/// `/health` still answers, reporting `storage_ready: false`.
pub struct NotReady {
    notifier: Option<Arc<Notifier>>,
}

impl NotReady {
    /// A stub with no notifier (there are no messages to wake for).
    pub fn new() -> Self {
        NotReady { notifier: None }
    }

    /// A stub that carries the receiver's notifier, so the long-poll wiring can
    /// be exercised even before the RSM lands.
    pub fn with_notifier(n: Arc<Notifier>) -> Self {
        NotReady { notifier: Some(n) }
    }
}

impl Default for NotReady {
    fn default() -> Self {
        NotReady::new()
    }
}

#[async_trait]
impl Rsm for NotReady {
    async fn push(&self, _ctx: ReqCtx, _req: PushReq) -> Result<PushOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn pop_wildcard(&self, _ctx: ReqCtx, _req: PopReq) -> Result<PopOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn pop_pinned(&self, _ctx: ReqCtx, _req: PopPinnedReq) -> Result<PopOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn pop_discover(&self, _ctx: ReqCtx, _req: PopDiscoverReq) -> Result<PopOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn ack(&self, _ctx: ReqCtx, _req: AckReq) -> Result<AckOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn renew(&self, _ctx: ReqCtx, _req: RenewReq) -> Result<RenewOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn transaction(&self, _ctx: ReqCtx, _req: TxnReq) -> Result<TxnOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn dlq_head(&self, _ctx: ReqCtx, _req: DlqHeadReq) -> Result<DlqHeadOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn has_pending(&self, _ctx: ReqCtx, _req: PendingReq) -> Result<bool, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn depth(&self, _ctx: ReqCtx, _req: DepthReq) -> Result<DepthOut, RsmError> {
        Err(RsmError::Unsupported)
    }

    fn health(&self) -> RaftHealth {
        // Phase-1 single node: the process is up and reachable, but the state
        // machine is not wired. Report a `single`-node leader so `/health`
        // answers 200 (there is no consensus lag to gate on yet), with
        // `storage_ready: false` telling the truth about the RSM.
        RaftHealth {
            role: "single".to_string(),
            leader_known: true,
            term: 0,
            applied: 0,
            commit: 0,
            lag_ms: 0,
            storage_ready: false,
        }
    }

    fn notifier(&self) -> Option<&Arc<Notifier>> {
        self.notifier.as_ref()
    }
}

// ---------------------------------------------------------------------------
// The builder hook (task: "a function pointer / trait object the integration
// step fills").
// ---------------------------------------------------------------------------

/// What the builder needs to construct a real facade: the data directory
/// (required in raft mode, §11.1) and the receiver's notifier, so the RSM's
/// apply thread can wake parked long-polls through it (§9.5).
pub struct RsmBuildCtx {
    pub data_dir: String,
    pub notifier: Arc<Notifier>,
}

/// The hook WP-1.7c installs: given the build context, produce the real facade.
pub type RsmBuilder = fn(&RsmBuildCtx) -> Arc<dyn Rsm>;

static BUILDER: OnceLock<RsmBuilder> = OnceLock::new();

/// Install the real facade builder (WP-1.7c). First set wins; a second call is
/// ignored, matching the process-global singletons elsewhere in the broker.
pub fn set_builder(f: RsmBuilder) {
    let _ = BUILDER.set(f);
}

/// Build the facade for this node. Returns the real implementation when a
/// builder has been installed (WP-1.7c), otherwise the [`NotReady`] stub, so
/// WP-1.7a boots and routes with the stub and needs no other change when the
/// real builder is registered.
pub fn build(ctx: &RsmBuildCtx) -> Arc<dyn Rsm> {
    match BUILDER.get() {
        Some(f) => f(ctx),
        None => Arc::new(NotReady::with_notifier(ctx.notifier.clone())),
    }
}
