//! Leader-side planning (PLAN_RAFT.md §7, §8): the pure functions that turn a
//! client command into EFFECTS and an OUTCOME, over the committed view plus an
//! overlay of the entries still in flight.
//!
//! The planner plans what the log carries: pushes, the catalog and its
//! configuration, KV, timers, transactions (their pushes, KV and timers) and
//! the deterministic effect commands — among them the consumption engine's
//! checkpoints. It plans NO consumption: pops, acks, nacks, renews, DLQ heads,
//! positional acks and positions are served on the leader by the consumption
//! engine ([`crate::rsm::consume`]), the one writer of every group's cursors
//! and leases, and one that reaches the planner is refused.
//!
//! # What "pure" means here, and where the line is
//!
//! The planner reads committed state (a store read transaction, [`Committed`])
//! and the [`Overlay`], and produces `(effects, outcome)` or a [`Refusal`]. It
//! NEVER writes committed state — that is I1, and apply ([`crate::rsm::apply`])
//! is the only mutator. Unlike the pgless `native/semantics.rs` this file is
//! ported from, there is no `stage()`: pgless applied its own records at plan
//! time (hazard D-1), so its state after planning WAS the state after replay;
//! here the planner emits effects and apply executes them, and the overlay is
//! how a later command in the same cycle sees an earlier one's effects without
//! anything being applied (§7.2).
//!
//! Unlike apply and the store, the planner is NOT under the I2
//! `disallowed_methods` deny: D5 makes TIME the planner's to stamp, and §5.2
//! allows randomness in the planner (a partition uuid, a group id, a timer's
//! dlq id). Apply carries no clock and no randomness, so every absolute time
//! and every id the planner mints travels in an effect.
//!
//! # The differences from pgless, and why
//!
//! * **No `hw`.** pgless split "allocated" from "visible" because its live path
//!   allocated an offset before the write confirmed. In the RSM a segment
//!   exists only because apply executed a COMMITTED `Append`, so `last_offset`
//!   IS the visible tail ([`crate::rsm::store::rows::PartitionRow`]). Every
//!   `visible`/`hw` guard of the port collapses to "offset ≤ last_offset".
//! * **Segments come from the `txns` keyspace, not from RAM SegRefs.** Positions
//!   are node-local (D8, I7) and never reach the planner. The replicated `txns`
//!   rows carry `(base_offset) → (end, created_at, hashes)` for every `Append`
//!   and outlive retention (D10), so the planner reconstructs a partition's
//!   segment shape — base, end, created_at, and the hashes the dedup probe
//!   needs — from them plus the overlay's own appends. It never reads `seg_loc`.
//! * **No repacking, no decompression (O20).** The receiver pre-packs one frame
//!   per message; a duplicate drops a survivor and the `Append` blob is the
//!   survivors concatenated, never repacked.
//!
//! # The cycle, and who calls what
//!
//! The batcher (WP-1.6b) owns the [`Overlay`] across the bounded pipeline (D4,
//! four entries in flight). Per cycle it: brings an `Overlay` to committed
//! bases plus every in-flight entry — the one its planner thread kept from the
//! last cycle with the entries that landed taken out ([`kept`], KEEP_OVERLAY),
//! or one built from scratch by folding every in-flight entry into it
//! ([`Overlay::ingest_entry`]) — then marks the cycle start
//! ([`Overlay::mark_cycle_start`]) so the entry it is
//! about to build gets the right `pid_base`/`kv_version_base`, then for each
//! drained command looks the request id up ([`Planner::lookup_request_id`],
//! §5.4) and, on a miss, calls the matching `plan_*`. A [`Plan::Logged`] is
//! added to the entry (its effects are already folded into the overlay so the
//! next command sees them) and its outcome recorded under the id; a
//! [`Plan::Empty`] or [`Plan::Refused`] is answered at once and neither logged
//! nor recorded (§5.4).

// The planner MAY read the clock and MAY use randomness (D5, §5.2), so it is
// NOT under the I2 `disallowed_methods` deny that `apply`, `state` and `store`
// carry. Determinism is apply's; the planner's job is to hand apply effects
// that make it deterministic.

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};

use smallvec::SmallVec;

use crate::rsm::fasthash::FxBuild;
use std::rc::Rc;

use crate::rsm::dedup::{self, DedupFront, IndexMode, ProbeVerdict, Seed, SeedHash, TxnsRow};
use crate::rsm::effect::{Effect, GroupMeta, Pid, QueueConfig, SubscriptionMode};
use crate::rsm::entry::{Entry, Outcome, RequestId};
use crate::rsm::qlog::set::{QLogReader, QLogSet};
use crate::rsm::segments::{DedupFrame, Reader};
use crate::rsm::state::Committed;
use crate::rsm::store::rows::GroupRow;
use crate::rsm::store::{keys, Keyspace, Reads, StoreError, TypedReads};

/// KEEP_OVERLAY: the overlay kept between cycles on the planner thread.
pub(crate) mod kept;
/// KV: versions from `kv_version_base + ordinal`, TTL, prefix lists,
/// `required`, and the leader's expiry sweep. WP-2.2.
pub mod kv;
pub mod push;
/// Timers: schedule/cancel, the fire step, backoff and the `__timer__` DLQ.
/// WP-2.3.
pub mod timers;

pub use kv::{KvCommand, KvOp};

// The phase-2 planner surfaces keep their place in the §3.4 module map as empty
// inline stubs, exactly as they stood inside `rsm/mod.rs` before this WP filled
// `planner`. The owning WP flips one to a file.

/// The transaction's `positions` rider, as the receiver validates it (the
/// consumption engine sets and forgets the positions).
pub mod positions;
/// The transaction wire: one command, all-or-nothing (Phase B). Its KV leg
/// is [`Planner::plan_kv_writes`]; a timers leg would be
/// [`Planner::plan_timer_ops`].
pub mod txn;
/// Streams: cycle, register, state. WP-2.4.
pub mod streams {}
/// Admin: configure, deletes, consumer groups, messages, flags, quotas.
/// WP-2.5.
pub mod admin {}
/// Retention as a leader loop: rules 1–3, max-wait eviction, txns purge.
/// WP-2.7.
pub mod retention {}

const SEC_US: i64 = 1_000_000;

/// `QUEEN_RAFT_PLAN_MAX_MS` (O17): the batcher cuts the drain after this much
/// wall time spent planning a cycle. A single command that alone exceeds it is
/// still planned (or it could never progress); the batcher owns the timer, this
/// is only the shared default.
pub const PLAN_BUDGET_MS_DEFAULT: u64 = 5;

/// `QUEEN_RAFT_SLOW_COMMAND_MS` (O18): a command that plans slower than this is
/// logged WARN. The batcher owns the clock and the log.
pub const SLOW_COMMAND_MS_DEFAULT: u64 = 50;

/// Config the planner reads. Nothing here is read from the environment by this
/// module (WP-1.7 fills it); it is passed in so the planner stays testable.
#[derive(Clone, Debug)]
pub struct PlanConfig {
    /// `QUEEN_RAFT_ENTRY_MAX_BYTES` (§5.1): a command whose PLANNED size exceeds
    /// this is refused 413. Default [`crate::rsm::entry::ENTRY_MAX_BYTES_DEFAULT`].
    pub entry_max_bytes: usize,
    /// O17, quoted so the batcher and the planner cannot drift.
    pub plan_budget_ms: u64,
    /// O18.
    pub slow_command_ms: u64,
    /// `QUEEN_RAFT_DEDUP_INDEX` (PERF-E): which keyspace the dedup probe /
    /// hash-ack resolve read as the authority. Default [`IndexMode::Rows`] so the
    /// unit-test harness keeps today's behaviour; `BatcherConfig::from_env`
    /// sets the product default (`txns`) and keeps `record` (apply side) in
    /// step via [`dedup::set_record_index_mode`].
    pub index_mode: IndexMode,
}

impl Default for PlanConfig {
    fn default() -> PlanConfig {
        PlanConfig {
            entry_max_bytes: crate::rsm::entry::ENTRY_MAX_BYTES_DEFAULT,
            plan_budget_ms: PLAN_BUDGET_MS_DEFAULT,
            slow_command_ms: SLOW_COMMAND_MS_DEFAULT,
            index_mode: IndexMode::Rows,
        }
    }
}

// ---------------------------------------------------------------------------
// Refusal and the plan result
// ---------------------------------------------------------------------------

/// A whole-command refusal: no effects, no outcome recorded, an error answer to
/// the client (§5.4, I14). `retryable` is what the receiver turns into a 5xx
/// the SDK retries with the SAME request id (D6) versus a 4xx it does not.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl Refusal {
    pub fn client(code: impl Into<String>, message: impl Into<String>) -> Refusal {
        Refusal {
            code: code.into(),
            message: message.into(),
            retryable: false,
        }
    }

    pub fn retry(code: impl Into<String>, message: impl Into<String>) -> Refusal {
        Refusal {
            code: code.into(),
            message: message.into(),
            retryable: true,
        }
    }

    /// The store could not answer a read the planner needs. I14: refuse with a
    /// retryable error, never guess. A `KeyTooLong` is the one 4xx here (a name
    /// LMDB does not accept, R-108).
    pub fn from_store(e: StoreError) -> Refusal {
        match e {
            StoreError::KeyTooLong { .. } => Refusal::client("name_too_long", e.to_string()),
            other => Refusal::retry("unavailable", other.to_string()),
        }
    }
}

fn store_err(e: StoreError) -> Refusal {
    Refusal::from_store(e)
}

/// What a `plan_*` decided (§5.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Plan {
    /// At least one effect. The batcher adds it to the entry with
    /// [`Entry::add_command`] and records the outcome under the id (D6, I6).
    Logged {
        effects: Vec<Effect>,
        outcome: Outcome,
    },
    /// No effects: answered at once, NOT logged and the id NOT recorded (§5.4) —
    /// an empty pop, an all-duplicate push, a read that found nothing to do.
    /// Logging these would make cost follow the poll rate (G-3).
    Empty(Outcome),
    /// A whole-command refusal.
    Refused(Refusal),
}

impl Plan {
    fn logged(effects: Vec<Effect>, outcome: Outcome) -> Plan {
        debug_assert!(!effects.is_empty(), "a Logged plan must carry effects");
        Plan::Logged { effects, outcome }
    }
}

/// A `plan_*` returns either a [`Plan`] or the [`Refusal`] the batcher answers
/// the whole command with. A refusal that is part of the plan shape (an empty
/// pop, an all-duplicate push) is a [`Plan`], not this `Err`: `Err` is the
/// planner unable to decide (I14).
pub type Planned = Result<Plan, Refusal>;

/// The command kind, for the O18 per-kind planner metrics and the slow-command
/// log the batcher keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CommandKind {
    Push,
    PopPinned,
    PopWildcard,
    PopDiscover,
    Ack,
    AckPositional,
    Nack,
    Renew,
    DlqHead,
    Transaction,
    /// A KV call with at least one write (WP-2.2).
    Kv,
    /// `POST /api/v1/timers` and the cancel route.
    Timers,
    /// Phase-2 commands whose receiver has already produced deterministic,
    /// self-contained effects (flags, traces, grants and control metadata).
    Effects,
}

impl CommandKind {
    pub fn name(self) -> &'static str {
        match self {
            CommandKind::Push => "push",
            CommandKind::PopPinned => "pop_pinned",
            CommandKind::PopWildcard => "pop_wildcard",
            CommandKind::PopDiscover => "pop_discover",
            CommandKind::Ack => "ack",
            CommandKind::AckPositional => "ack_positional",
            CommandKind::Nack => "nack",
            CommandKind::Renew => "renew",
            CommandKind::DlqHead => "dlq_head",
            CommandKind::Transaction => "transaction",
            CommandKind::Kv => "kv",
            CommandKind::Timers => "timers",
            CommandKind::Effects => "effects",
        }
    }
}

/// The result of the request-id lookup (§5.4, D6, I6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lookup {
    /// Not seen: plan the command.
    Miss,
    /// Recorded in committed `request_ids`: return this outcome, plan nothing.
    Committed(Outcome),
    /// In an entry still in flight: wait for that entry's apply, then return
    /// this outcome (the batcher owns the wait).
    InFlight(Outcome),
}

// ---------------------------------------------------------------------------
// Command inputs
// ---------------------------------------------------------------------------
//
// The planner's input contract. WP-1.7 (the `command` module) maps HTTP
// requests, facade frames and forwarded commands onto these; nothing here is
// read from the wire. They live in the planner because the planner is their
// only consumer and WP-1.6b codes the batcher against them.

/// One message of a push, pre-packed by the receiver (O20: the planner never
/// packs). `frame` is the exact bytes this one message contributes to a segment
/// blob; the `Append` blob is the survivors' frames concatenated, so a
/// duplicate is dropped by leaving its frame out, never by repacking.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PushItem {
    /// `xxh3_128` of the transaction id, the dedup key and the `Append` hash.
    pub hash: [u8; 16],
    #[serde(with = "serde_bytes")]
    pub frame: Vec<u8>,
}

/// `POST /api/v1/push` for one partition. The receiver has already
/// resolved the queue's config (for the implicit-creation case) and packed each
/// message.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PushCommand {
    pub request_id: RequestId,
    pub tenant: String,
    pub queue: String,
    pub partition: String,
    pub items: Vec<PushItem>,
    /// The config an implicit queue creation uses (first contact). Ignored
    /// when the queue already exists. `id` is the receiver-minted queue uuid.
    pub create_cfg: QueueConfig,
}

/// The subscription intent a pop carries for a group with no stored policy:
/// the pop-carried `sub_mode`/`sub_from` fall-back.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct SubIntent {
    /// `""`, `"new"`, `"all"`, or `"timestamp"`.
    pub mode: String,
    /// An ISO-8601 instant already parsed to µs by the receiver, or `None`.
    pub from_us: Option<i64>,
    /// The literal `"now"` intent (seed at the tail), distinct from an absent
    /// `from`.
    pub now: bool,
}

/// [`PopCommand::max_parts`] of an autopilot pop that left its width to the
/// broker: the leader's consumption engine picks it from the group's exact
/// state ([`crate::rsm::consume`]), for a pop sent here and one a follower
/// forwarded alike.
pub const MAX_PARTS_AUTO: i32 = -1;

/// A pop of one named partition, the whole queue by wildcard, or a discovery
/// group across a namespace or task. One struct, three entry points.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PopCommand {
    pub request_id: RequestId,
    pub tenant: String,
    /// The queue for pinned and wildcard; empty for a discovery pop.
    pub queue: String,
    /// `Some` for a pinned pop.
    pub partition: Option<String>,
    pub group: String,
    /// The worker id, which IS the lease id (handlers/data.rs ≈1050).
    pub worker: String,
    /// Max frames across the whole pop.
    pub budget: i32,
    /// Wildcard/discovery: max partitions claimed; `0` = unlimited;
    /// [`MAX_PARTS_AUTO`] = the leader's consumption engine picks it (the pop
    /// autopilot).
    pub max_parts: i32,
    pub lease_seconds: i32,
    pub auto_ack: bool,
    pub conflate: bool,
    pub sub: SubIntent,
    /// The hot-list wheel owns the window-buffer hold on the pinned path:
    /// skip the debounce here when it says so.
    pub skip_window_debounce: bool,
    /// Discovery: the namespace and task that pick the queues.
    pub namespace: String,
    pub task: String,
    /// The config a wildcard pop uses if it must create the queue; `None`
    /// disables implicit creation (pinned never creates).
    pub create_cfg: Option<QueueConfig>,
    /// Wall-clock µs after which nobody is waiting for this pop's answer
    /// (`0` = no deadline). The planner refuses to CLAIM for a pop that cannot
    /// be answered in time, so a request that timed out while queued behind
    /// the pipeline never leaves an orphaned lease (PLAN_RAFT_DRAIN_FIX P1.2).
    pub deadline_us: i64,
    /// A long-poll pop: an empty answer parks and re-polls on the next wake,
    /// so with lanes an empty result in one lane is final; a pop that does not
    /// wait is re-planned with every partition in view (`batcher_lanes`).
    #[serde(default)]
    pub wait: bool,
}

/// The status an ack item carries. `Ok` covers
/// completed/success/acked/ok/"".
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckStatus {
    Ok,
    Failed,
    Dlq,
    Retry,
}

/// The receiver's snapshot of a poison frame, carried on a signal item so the
/// planner can file the DLQ in the same entry (O7) without decompressing
/// anything (O20). The receiver resolves the acked hash to its offset in its own
/// committed files and reads the frame out before forwarding.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct DlqSnapshot {
    pub message_id: Option<[u8; 16]>,
    pub txn: String,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

/// One item of a hash-resolved ack.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AckItem {
    pub hash: [u8; 16],
    pub status: AckStatus,
    pub error: Option<String>,
    /// Present on a `Failed`/`Dlq` item when the receiver pre-read the poison
    /// frame (O7/O20).
    pub snapshot: Option<DlqSnapshot>,
}

/// One `(pid, group)` target of an ack. A batch acks several.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AckTarget {
    pub pid: Pid,
    pub tenant: String,
    pub queue: String,
    pub group: String,
    /// The lease id; `""` for a lease-less ack, which still advances
    /// (RUSTFIX item 11).
    pub worker: String,
    pub items: Vec<AckItem>,
}

/// `POST /api/v1/ack`, `/ack/batch`.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AckCommand {
    pub request_id: RequestId,
    pub targets: Vec<AckTarget>,
}

/// A positional ack of ONE leased batch: the receiver advances the cursor to
/// an absolute offset it computed from the delivered batch.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AckPositionalCommand {
    pub request_id: RequestId,
    pub pid: Pid,
    pub tenant: String,
    pub queue: String,
    pub group: String,
    pub worker: String,
    /// `Some(offset)` acks up to and including it. Streams leaves this `None`:
    /// a releasing cycle uses the recorded batch end, while a partial cycle
    /// advances `acked_count` frames from the attempt head. `ok=false` nacks.
    pub upto: Option<i64>,
    pub ok: bool,
    /// Release the recorded batch after the acknowledgement. Streams gate
    /// cycles set this false so only the acknowledged prefix advances and the
    /// denied tail remains on the same lease.
    pub release_lease: bool,
    /// How many frames the handler is acking, for `total_consumed`.
    pub acked_count: i32,
}

/// Release a worker's lease on a `(pid, group)` without moving the cursor — the
/// nack the whole batch redelivers from.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct NackCommand {
    pub request_id: RequestId,
    pub pid: Pid,
    pub tenant: String,
    pub queue: String,
    pub group: String,
    pub worker: String,
}

/// `POST /api/v1/lease/:leaseId/extend`. Renews EVERY
/// live lease of the worker: the planner walks `leases_by_worker` in key order
/// (never a hash map, §8), so the effect list is deterministic.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RenewCommand {
    pub request_id: RequestId,
    /// Only this tenant's leases renew (a lease id is a bearer value that can
    /// leak). `None` from a node that predates the field: every lease of the
    /// worker.
    #[serde(default)]
    pub tenant: Option<String>,
    pub worker: String,
    pub seconds: i32,
}

/// A standalone DLQ-head command: file the poison HEAD frame the receiver
/// snapshotted, advance past it, release the lease. Used when the DLQ
/// handoff was not folded into the ack (the `/dlq` replay-then-die path).
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DlqHeadCommand {
    pub request_id: RequestId,
    pub pid: Pid,
    pub tenant: String,
    pub queue: String,
    pub group: String,
    pub worker: String,
    /// The offset the receiver read the poison frame at.
    pub offset: u64,
    pub error: String,
    pub snapshot: DlqSnapshot,
}

/// A deterministic Phase-2 mutation which requires no semantic read while it
/// is planned. Effects that allocate pids/KV versions or depend on current
/// state must keep their dedicated planner; this lane is for whole-row
/// overwrites, deletes and append-only metadata.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct EffectsCommand {
    pub request_id: RequestId,
    pub tenant: String,
    pub effects: Vec<Effect>,
}

// ---------------------------------------------------------------------------
// The overlay
// ---------------------------------------------------------------------------

/// One overlay value and the TAG of the fold that wrote it last — the entry in
/// flight it came from (KEEP_OVERLAY, [`kept`]). When that entry lands, the
/// value goes only if no later entry has overwritten it (the tag still names
/// the landed entry); the planner reads `v` and never looks at the tag.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Tagged<V> {
    v: V,
    tag: u64,
}

/// One `Append` the overlay remembers: its offset range, its stamp, and the
/// per-frame hashes in frame order (for the delivered set and the dedup probe).
#[derive(Clone, Debug, PartialEq, Eq)]
struct OverlayAppend {
    base: u64,
    end: u64,
    created_at_us: i64,
    hashes: Vec<[u8; 16]>,
    /// The fold that added it (KEEP_OVERLAY): its landing removes it.
    tag: u64,
}

/// A partition as the overlay knows it, relative to committed state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct OverlayPart {
    /// `Some` when a `PartitionCreate` for this pid is in the overlay: committed
    /// state has no row for it, so the merged view is built from this. It MUST
    /// go when the create lands (KEEP_OVERLAY): the flag is not idempotent.
    created: Option<Tagged<CreatedPart>>,
    appends: Vec<OverlayAppend>,
    /// A `Watermark` in the overlay (retention in flight): `(log_start,
    /// txns_start)`.
    watermark: Option<Tagged<(u64, u64)>>,
}

impl OverlayPart {
    /// Nothing in flight names this partition any more.
    fn is_empty(&self) -> bool {
        self.created.is_none() && self.appends.is_empty() && self.watermark.is_none()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CreatedPart {
    uuid: [u8; 16],
    tenant: String,
    queue: String,
    partition: String,
    created_at_us: i64,
}

/// The planner's view of everything not yet in committed state: the entries
/// still in flight (D4, up to four) and the commands planned so far in the
/// current cycle. Built and owned by the batcher.
///
/// It carries the running `next_pid`/`next_kv_version` so a `PartitionCreate` or
/// `KvPut` planned this cycle gets the id apply will assert against its header
/// (I18): the batcher marks the cycle start after folding the in-flight
/// entries, and the entry it then builds carries [`Overlay::cycle_pid_base`] /
/// [`Overlay::cycle_kv_base`].
///
/// KEEP_OVERLAY ([`kept`]): every contribution carries the TAG of the fold
/// that wrote it (one tag per entry), so the planner thread can keep ONE
/// overlay across cycles and take out exactly what an entry put in when it
/// lands, instead of rebuilding it from every in-flight entry each cycle.
#[derive(Clone, Debug)]
pub struct Overlay {
    next_pid: u64,
    next_kv_version: u64,
    cycle_pid_base: u64,
    cycle_kv_base: u64,
    /// The greatest `now_us` any folded entry carried (§7.4 clock floor).
    max_now_us: i64,
    /// The greatest `created_at` any folded `Append` carried.
    max_created_at_us: i64,
    queues: HashMap<(String, String), Tagged<Option<QueueConfig>>, FxBuild>,
    groups: HashMap<(String, String, String), Tagged<Option<GroupRow>>, FxBuild>,
    pids_by_key: HashMap<(String, String, String), Tagged<Pid>, FxBuild>,
    parts: HashMap<Pid, OverlayPart, FxBuild>,
    /// `(pid, hash) → [(offset, created_at)]`: the occurrences an overlay
    /// `Append` added, merged with committed `dedup` on a probe or a resolve.
    /// One key per in-flight message, so the value keeps its (almost always
    /// single) occurrence inline. No tag: an occurrence's offset is unique in
    /// its partition, so a landed `Append` removes exactly its own.
    dedup: HashMap<(Pid, [u8; 16]), SmallVec<[DedupOccurrence; 1]>, FxBuild>,
    /// The outcome of every command in flight, by request id — kept by
    /// reference to its entry (B27: a push's outcome carries a verdict per
    /// message, and was cloned for every command of every entry).
    request_ids: HashMap<RequestId, Tagged<InFlightOutcome>, FxBuild>,
    /// `(tenant, ns, key) → row` for every KV row an entry in flight (or an
    /// earlier command of this cycle) wrote — `None` for a delete — so the
    /// next KV write is judged against the version it left, not against the
    /// committed one (WP-2.2).
    kv: HashMap<(String, String, String), Tagged<Option<crate::rsm::store::rows::KvRow>>, FxBuild>,
    /// `(tenant, queue, timer_key) → what the in-flight entries did to it`
    /// (WP-2.3). The overlay's view wins over committed state per key, which
    /// is what keeps a timer whose fire entry is still in flight from firing
    /// a second time in the next cycle (exactly-once in effect).
    timers: HashMap<(String, String, String), TimerSlot, FxBuild>,
    /// Pids a delete in flight takes away: a queue delete's or a tenant
    /// purge's `GarbageAdd`, retention's `PartitionDelete`. The planner ignores
    /// them from the entry that deletes them, as it ignores committed garbage
    /// (§5.2): once that entry applies, a chunk may already have removed the
    /// partition row, and an `Append` or a `CursorSet` to it is fatal in apply.
    gone: HashMap<Pid, Tagged<()>, FxBuild>,
    /// `(tenant, queue)` of a `QueueDelete` in flight. Its partition names and
    /// its groups are swept when it applies, so the committed ones are dead
    /// already; only what the overlay writes after it counts (a push that
    /// re-creates the queue gets a new partition).
    dropped_queues: HashMap<(String, String), Tagged<()>, FxBuild>,
    /// The tenant of a `TenantPurge` in flight: as `dropped_queues` for every
    /// queue of the tenant, and its KV rows and timers too.
    purged_tenants: HashMap<String, Tagged<()>, FxBuild>,
    /// B27: whether the folds keep partition state — appends, their dedup
    /// occurrences, watermarks. Control's kept overlay runs without it
    /// while it plans nothing that reads it ([`kept::KeptOverlay::partitions`]):
    /// folding and unfolding every message of every entry cost the serial
    /// planner thread 238-595 ns per pushed message for commands it did not
    /// plan.
    partition_state: bool,
    /// B27: whether the folds record the request ids in flight. Only a planner
    /// that looks ids up in its overlay needs them (the single planner; with
    /// lanes the router looks them up, L5).
    record_ids: bool,
    /// The tag every fold stamps now (KEEP_OVERLAY): one per in-flight entry,
    /// and the cycle's own for the commands planned into the entry being built.
    tag: u64,
    /// Every effect ever folded into this overlay, counted (KEEP_OVERLAY): the
    /// batcher checks a cycle folded exactly the effects its entry carries.
    folds: u64,
}

/// One timer key's overlay state and the tags that decide what a landing does
/// to it. A `TimerBackoff` PATCHES whatever an earlier fold left, so unlike the
/// last-writer-wins maps its value can depend on an earlier entry: `base` is
/// the tag of the fold the current value's derivation starts at (a schedule, a
/// delete, or a backoff over nothing), `last` the tag of the last fold. When
/// entry `T` lands: `last == T` → nothing later touched the key, drop it;
/// `base <= T < last` → the value was derived THROUGH `T`, re-fold it from the
/// entries still in flight; `base > T` → it never depended on `T`, keep it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TimerSlot {
    v: TimerOverlay,
    last: u64,
    base: u64,
}

/// One timer as the in-flight entries left it, relative to committed state.
#[derive(Clone, Debug, PartialEq, Eq)]
enum TimerOverlay {
    /// Upserted (schedule, reschedule) — the whole row.
    Row(crate::rsm::effect::TimerRow),
    /// Deleted (cancel, delivered, dead-lettered).
    Deleted,
    /// Backed off on top of whatever committed state holds (the row itself
    /// was never folded): apply patches the stored row the same way.
    Backoff {
        visible_at_us: i64,
        attempts: i32,
        last_error: Option<String>,
        updated_at_us: i64,
    },
}

/// One overlay dedup occurrence: `(offset, created_at_us)`.
type DedupOccurrence = (u64, i64);

/// B27: a command in flight's outcome, by reference to the entry that carries
/// it (compared and printed as the outcome).
#[derive(Clone)]
struct InFlightOutcome {
    entry: std::sync::Arc<Entry>,
    command: usize,
}

impl InFlightOutcome {
    fn outcome(&self) -> &Outcome {
        &self.entry.commands[self.command].outcome
    }
}

impl PartialEq for InFlightOutcome {
    fn eq(&self, other: &InFlightOutcome) -> bool {
        self.outcome() == other.outcome()
    }
}

impl Eq for InFlightOutcome {}

impl std::fmt::Debug for InFlightOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.outcome().fmt(f)
    }
}

/// Whether `eff` is partition state ([`Overlay::partition_state`]): what a
/// catalog-only overlay leaves out, and folds later if it has to.
fn is_partition_state(eff: &Effect) -> bool {
    matches!(eff, Effect::Append { .. } | Effect::Watermark { .. })
}

/// Frame `i`'s hash in an `Append`'s `hashes`, exactly as the fold reads it (a
/// short blob yields a zero hash rather than a panic), so the KEEP_OVERLAY
/// landing takes out exactly the keys the fold put in.
fn frame_hash(hashes: &[u8], i: usize) -> [u8; 16] {
    let mut h = [0u8; 16];
    if let Some(slice) = hashes.get(i * 16..i * 16 + 16) {
        h.copy_from_slice(slice);
    }
    h
}

/// The per-frame hashes of an `Append`, in frame order ([`frame_hash`]).
fn append_hashes(hashes: &[u8], count: usize) -> Vec<[u8; 16]> {
    (0..count).map(|i| frame_hash(hashes, i)).collect()
}

impl Overlay {
    /// A fresh overlay over committed bases (before any in-flight entry).
    pub fn new(committed_next_pid: u64, committed_kv_version_next: u64) -> Overlay {
        Overlay {
            next_pid: committed_next_pid,
            next_kv_version: committed_kv_version_next,
            cycle_pid_base: committed_next_pid,
            cycle_kv_base: committed_kv_version_next,
            max_now_us: 0,
            max_created_at_us: 0,
            queues: HashMap::default(),
            groups: HashMap::default(),
            pids_by_key: HashMap::default(),
            parts: HashMap::default(),
            dedup: HashMap::default(),
            request_ids: HashMap::default(),
            kv: HashMap::default(),
            timers: HashMap::default(),
            gone: HashMap::default(),
            dropped_queues: HashMap::default(),
            purged_tenants: HashMap::default(),
            partition_state: true,
            record_ids: true,
            tag: 0,
            folds: 0,
        }
    }

    /// Fold an entry still in flight into the overlay: its effects (so a later
    /// command sees them) and its request ids (so a retry of one in flight is
    /// found, §5.4). Call once per in-flight entry, in index order, before
    /// [`Overlay::mark_cycle_start`]. Everything it folds carries the current
    /// tag ([`Overlay::set_tag`]).
    pub fn ingest_entry(&mut self, e: &Entry) {
        self.ingest(&std::sync::Arc::new(e.clone()));
    }

    /// [`Overlay::ingest_entry`] for an entry the caller holds shared: the
    /// outcomes are recorded by reference to it.
    pub(crate) fn ingest(&mut self, e: &std::sync::Arc<Entry>) {
        self.max_now_us = self.max_now_us.max(e.now_us);
        for eff in &e.effects {
            self.fold_effect(eff);
        }
        self.record_ids_of(e);
    }

    /// Record the request ids of `e` (first writer wins), each by reference
    /// to its command in `e`, under the current tag. Nothing without
    /// [`Overlay::record_ids`].
    fn record_ids_of(&mut self, e: &std::sync::Arc<Entry>) {
        if !self.record_ids {
            return;
        }
        let tag = self.tag;
        for (i, c) in e.commands.iter().enumerate() {
            self.request_ids
                .entry(c.request_id)
                .or_insert_with(|| Tagged {
                    v: InFlightOutcome {
                        entry: e.clone(),
                        command: i,
                    },
                    tag,
                });
        }
    }

    /// The tag every later fold stamps on what it writes (KEEP_OVERLAY): the
    /// in-flight entry being ingested, or the entry the current cycle builds.
    pub(crate) fn set_tag(&mut self, tag: u64) {
        self.tag = tag;
    }

    /// How many effects this overlay has folded so far (KEEP_OVERLAY): the
    /// batcher's proof that a cycle folded exactly what its entry carries.
    pub(crate) fn folds(&self) -> u64 {
        self.folds
    }

    /// Snapshot the running bases as the bases of the entry the batcher is about
    /// to build this cycle. Apply asserts they equal `meta` (I18).
    pub fn mark_cycle_start(&mut self) {
        self.cycle_pid_base = self.next_pid;
        self.cycle_kv_base = self.next_kv_version;
    }

    pub fn cycle_pid_base(&self) -> u64 {
        self.cycle_pid_base
    }

    pub fn cycle_kv_base(&self) -> u64 {
        self.cycle_kv_base
    }

    /// The planner's stamp for this cycle (D5, §7.4): above the wall clock, the
    /// last committed `now`, the last committed `max_created_at`, and everything
    /// any in-flight entry already carried.
    pub fn plan_now<R: Reads + ?Sized>(
        &self,
        committed: &Committed<'_, R>,
        wall_us: i64,
    ) -> Result<i64, Refusal> {
        let base = committed.plan_now(wall_us).map_err(store_err)?;
        Ok(base
            .max(self.max_now_us.saturating_add(1))
            .max(self.max_created_at_us.saturating_add(1)))
    }

    fn request_id(&self, id: &RequestId) -> Option<&Outcome> {
        self.request_ids.get(id).map(|t| t.v.outcome())
    }

    /// Fold one effect into the overlay indexes. Used for in-flight entries and,
    /// via [`Overlay::apply_effects`], for the current cycle's own effects.
    /// Everything it writes carries the current tag.
    fn fold_effect(&mut self, eff: &Effect) {
        self.folds += 1;
        let tag = self.tag;
        match eff {
            Effect::QueueUpsert { tenant, queue, cfg } => {
                self.queues.insert(
                    (tenant.clone(), queue.clone()),
                    Tagged {
                        v: Some(cfg.clone()),
                        tag,
                    },
                );
            }
            Effect::QueueDelete { tenant, queue } => {
                self.queues
                    .insert((tenant.clone(), queue.clone()), Tagged { v: None, tag });
                self.drop_names(tenant, Some(queue));
                self.dropped_queues
                    .insert((tenant.clone(), queue.clone()), Tagged { v: (), tag });
            }
            Effect::TenantPurge { tenant } => {
                self.queues.retain(|(t, _), _| t != tenant);
                self.drop_names(tenant, None);
                self.kv.retain(|(t, _, _), _| t != tenant);
                self.timers.retain(|(t, _, _), _| t != tenant);
                self.purged_tenants
                    .insert(tenant.clone(), Tagged { v: (), tag });
            }
            // A group delete keeps the partition: only its `(pid, group)` rows
            // go.
            Effect::GarbageAdd { pids, scope, .. }
                if !matches!(scope, crate::rsm::effect::GarbageScope::Group { .. }) =>
            {
                for pid in pids {
                    self.gone.insert(*pid, Tagged { v: (), tag });
                }
            }
            Effect::PartitionDelete { pid } => {
                self.gone.insert(*pid, Tagged { v: (), tag });
            }
            Effect::GroupUpsert {
                tenant,
                queue,
                group,
                meta,
            } => {
                self.groups.insert(
                    (tenant.clone(), queue.clone(), group.clone()),
                    Tagged {
                        v: Some(GroupRow {
                            meta: meta.clone(),
                            reg_index: 0,
                            reg_effect: 0,
                        }),
                        tag,
                    },
                );
            }
            Effect::GroupDelete {
                tenant,
                queue,
                group,
            } => {
                self.groups.insert(
                    (tenant.clone(), queue.clone(), group.clone()),
                    Tagged { v: None, tag },
                );
            }
            Effect::PartitionCreate {
                pid,
                uuid,
                tenant,
                queue,
                partition,
                created_at_us,
            } => {
                self.pids_by_key.insert(
                    (tenant.clone(), queue.clone(), partition.clone()),
                    Tagged { v: *pid, tag },
                );
                self.parts.entry(*pid).or_default().created = Some(Tagged {
                    v: CreatedPart {
                        uuid: *uuid,
                        tenant: tenant.clone(),
                        queue: queue.clone(),
                        partition: partition.clone(),
                        created_at_us: *created_at_us,
                    },
                    tag,
                });
                self.next_pid = self.next_pid.max(pid.saturating_add(1));
                self.max_created_at_us = self.max_created_at_us.max(*created_at_us);
            }
            // B27: a catalog-only overlay keeps no partition state (the
            // running clock floor still moves).
            Effect::Append { created_at_us, .. } if !self.partition_state => {
                self.max_created_at_us = self.max_created_at_us.max(*created_at_us);
            }
            Effect::Watermark { .. } if !self.partition_state => {}
            Effect::Append {
                pid,
                base_offset,
                count,
                created_at_us,
                hashes,
                ..
            } => {
                let count = *count as usize;
                let hs = append_hashes(hashes, count);
                let end = base_offset + count as u64 - 1;
                self.dedup.reserve(count);
                for (i, h) in hs.iter().enumerate() {
                    self.dedup
                        .entry((*pid, *h))
                        .or_default()
                        .push((base_offset + i as u64, *created_at_us));
                }
                self.parts
                    .entry(*pid)
                    .or_default()
                    .appends
                    .push(OverlayAppend {
                        base: *base_offset,
                        end,
                        created_at_us: *created_at_us,
                        hashes: hs,
                        tag,
                    });
                self.max_created_at_us = self.max_created_at_us.max(*created_at_us);
            }
            Effect::Watermark {
                pid,
                log_start,
                txns_start,
            } => {
                self.parts.entry(*pid).or_default().watermark = Some(Tagged {
                    v: (*log_start, *txns_start),
                    tag,
                });
            }
            Effect::KvPut {
                tenant,
                ns,
                key,
                value,
                version,
                expires_at_us,
                created_at_us,
                updated_at_us,
            } => {
                self.next_kv_version = self.next_kv_version.max(version.saturating_add(1));
                self.kv.insert(
                    (tenant.clone(), ns.clone(), key.clone()),
                    Tagged {
                        v: Some(crate::rsm::store::rows::KvRow {
                            value: value.clone(),
                            version: *version,
                            expires_at_us: *expires_at_us,
                            created_at_us: *created_at_us,
                            updated_at_us: *updated_at_us,
                        }),
                        tag,
                    },
                );
            }
            Effect::KvDelete { tenant, ns, key } => {
                self.kv.insert(
                    (tenant.clone(), ns.clone(), key.clone()),
                    Tagged { v: None, tag },
                );
            }
            // Timers (WP-2.3): the planner reads them through the overlay for
            // the schedule/cancel verdicts and the fire step's candidate set.
            Effect::TimerUpsert { .. }
            | Effect::TimerDelete { .. }
            | Effect::TimerBackoff { .. } => {
                self.fold_timer(eff, tag);
            }
            // Everything else the planner neither emits nor needs to see through
            // the overlay (a delete's chunks, cursors — the consumption
            // engine's — streams, traces): a later phase folds what its
            // planner reads.
            _ => {}
        }
    }

    /// A queue delete (`Some(queue)`) or a tenant purge (`None`) sweeps the
    /// partition names and the groups it covers when it applies: whatever an
    /// earlier fold wrote for them is dead with the committed ones, and the
    /// tombstone the caller records hides those. A later fold writes them anew.
    fn drop_names(&mut self, tenant: &str, queue: Option<&str>) {
        let hit = |t: &str, q: &str| t == tenant && queue.is_none_or(|x| x == q);
        self.pids_by_key.retain(|(t, q, _), _| !hit(t, q));
        self.groups.retain(|(t, q, _), _| !hit(t, q));
    }

    /// A pid a delete in flight takes away.
    fn is_gone(&self, pid: Pid) -> bool {
        !self.gone.is_empty() && self.gone.contains_key(&pid)
    }

    /// Whether a delete in flight dropped the queue's names and groups: its
    /// own `QueueDelete`, or a purge of its tenant.
    fn queue_dropped(&self, tenant: &str, queue: &str) -> bool {
        if self.dropped_queues.is_empty() && self.purged_tenants.is_empty() {
            return false;
        }
        self.purged_tenants.contains_key(tenant)
            || self
                .dropped_queues
                .contains_key(&(tenant.to_string(), queue.to_string()))
    }

    /// Whether a purge of the tenant is in flight: its committed KV rows and
    /// timers are dead already.
    pub(crate) fn tenant_purged(&self, tenant: &str) -> bool {
        !self.purged_tenants.is_empty() && self.purged_tenants.contains_key(tenant)
    }

    /// Whether anything in flight (or planned this cycle) names the partition:
    /// an append, a create, a watermark. The leader's retention reads only
    /// committed state, and must not delete a partition a push just wrote.
    pub(crate) fn touches_partition(&self, pid: Pid) -> bool {
        self.parts.contains_key(&pid)
    }

    /// For background retention ([`crate::rsm::retention_scan::judge`]):
    /// `None` when an entry in flight takes the partition away (its delete, or
    /// its queue's or tenant's), else the watermarks an entry in flight moves it
    /// to, if one does.
    pub(crate) fn retention_view(
        &self,
        pid: Pid,
        tenant: &str,
        queue: &str,
    ) -> Option<Option<(u64, u64)>> {
        if self.is_gone(pid) || self.queue_dropped(tenant, queue) {
            return None;
        }
        Some(
            self.parts
                .get(&pid)
                .and_then(|o| o.watermark.as_ref().map(|t| t.v)),
        )
    }

    /// Fold one timer effect under `tag` (see [`TimerSlot`] for what `base` and
    /// `last` record). Shared with the KEEP_OVERLAY re-fold of a key whose
    /// value was derived through an entry that landed.
    fn fold_timer(&mut self, eff: &Effect, tag: u64) {
        match eff {
            Effect::TimerUpsert {
                tenant,
                queue,
                key,
                row,
            } => {
                self.timers.insert(
                    (tenant.clone(), queue.clone(), key.clone()),
                    TimerSlot {
                        v: TimerOverlay::Row(row.clone()),
                        last: tag,
                        base: tag,
                    },
                );
            }
            Effect::TimerDelete { tenant, queue, key } => {
                self.timers.insert(
                    (tenant.clone(), queue.clone(), key.clone()),
                    TimerSlot {
                        v: TimerOverlay::Deleted,
                        last: tag,
                        base: tag,
                    },
                );
            }
            Effect::TimerBackoff {
                tenant,
                queue,
                key,
                visible_at_us,
                attempts,
                last_error,
                updated_at_us,
            } => {
                let k = (tenant.clone(), queue.clone(), key.clone());
                let fresh = || TimerOverlay::Backoff {
                    visible_at_us: *visible_at_us,
                    attempts: *attempts,
                    last_error: last_error.clone(),
                    updated_at_us: *updated_at_us,
                };
                let next = match self.timers.remove(&k) {
                    // Patched: the value still depends on the fold that wrote
                    // the row (or the delete), so the chain keeps its base.
                    Some(TimerSlot {
                        v: TimerOverlay::Row(mut row),
                        base,
                        ..
                    }) => {
                        row.visible_at_us = Some(*visible_at_us);
                        row.attempts = *attempts;
                        row.last_error = last_error.clone();
                        row.updated_at_us = *updated_at_us;
                        TimerSlot {
                            v: TimerOverlay::Row(row),
                            last: tag,
                            base,
                        }
                    }
                    Some(TimerSlot {
                        v: TimerOverlay::Deleted,
                        base,
                        ..
                    }) => TimerSlot {
                        v: TimerOverlay::Deleted,
                        last: tag,
                        base,
                    },
                    // A backoff over a backoff (or over nothing) replaces it
                    // whole: a new chain starts here.
                    Some(TimerSlot {
                        v: TimerOverlay::Backoff { .. },
                        ..
                    })
                    | None => TimerSlot {
                        v: fresh(),
                        last: tag,
                        base: tag,
                    },
                };
                self.timers.insert(k, next);
            }
            _ => {}
        }
    }

    /// Fold the current command's own effects, so the next command in the cycle
    /// sees them (§7.2). The planner calls this once a command is decided
    /// [`Plan::Logged`]; a refused or empty command folds nothing. `pub(crate)`
    /// for the planners that assemble one command from several legs (the
    /// transaction wire folds its [`Planner::plan_kv_writes`] leg with it).
    pub(crate) fn apply_effects(&mut self, effects: &[Effect]) {
        for e in effects {
            self.fold_effect(e);
        }
    }

    /// Reserve the next partition id WITHOUT advancing the counter — the planner
    /// peeks it to build a `PartitionCreate`, and the fold advances the counter
    /// when the command is committed to the overlay.
    fn peek_pid(&self) -> Pid {
        self.next_pid
    }

    /// Run `f` with `pid` as the next partition id, and keep the higher of
    /// the two afterwards. A lane creates a new partition with the id the
    /// router assigned it (lane-local creation, `batcher_lanes.rs`), whatever
    /// ids the other lanes create in the same cycle.
    pub(crate) fn with_next_pid<T>(&mut self, pid: Pid, f: impl FnOnce(&mut Overlay) -> T) -> T {
        let saved = self.next_pid;
        self.next_pid = pid;
        let out = f(self);
        self.next_pid = self.next_pid.max(saved);
        out
    }
}

// ---------------------------------------------------------------------------
// The planner
// ---------------------------------------------------------------------------

/// The leader-side planner for one cycle: the committed view and the cycle's
/// stamp. The overlay is passed to each `plan_*` because the batcher owns it
/// across the pipeline.
pub struct Planner<'a, R: Reads + ?Sized> {
    committed: Committed<'a, R>,
    now_us: i64,
    cfg: PlanConfig,
    /// The persistent dedup front (PERF-B). Borrowed for the cycle; the batcher
    /// owns it across cycles. A [`DedupFront::disabled`] instance makes the
    /// planner probe exactly as the baseline does.
    front: &'a DedupFront,
    /// PERF-E `DEDUP_INDEX=segment`: the segment read side, shared with the pop
    /// payload path. `Some` in production (the facade hands the batcher a
    /// cloned `Reader`); `None` for a planner built without segments — legal
    /// only in the `txns`/`rows` modes, which never touch it.
    reader: Option<Reader>,
    /// Phase A2 (`QUEEN_RAFT_QLOG`): the per-queue-log read side. When `Some` AND
    /// `index_mode == Segment`, the committed dedup authority (the probe, the
    /// resolve, the seed, the delivered-set claim walk) reads the per-append
    /// hashes from the queue log instead of the `.seg` files — SAME committed
    /// bound (`SegCtx::committed_end`), SAME O(claimed) walk. `None` is today's
    /// segment path.
    qlog_reader: Option<QLogReader>,
    /// PERF-E: the per-pid committed dedup rows reconstructed from the segments
    /// (bounded to the committed `last_offset`), materialized once per pid per
    /// cycle and shared across every committed `Txns`-authority read of that pid
    /// this cycle. `TxnsRow::end` is INCLUSIVE, like a stored `txns` row. Empty
    /// / unused outside `segment` mode. The planner runs on one blocking thread
    /// per cycle, so the `RefCell` is never contended.
    seg_cache: RefCell<HashMap<Pid, Rc<CommittedTxnsRows>>>,
    /// PERF-E: the per-pid committed segment context (bucket, committed bound,
    /// sealed-file list), cached so every committed read of a pid this cycle
    /// reuses one `partition_files` scan. `None` = no committed frames.
    seg_ctx_cache: RefCell<HashMap<Pid, Option<Rc<SegCtx>>>>,
}

/// A partition merged across committed state and the overlay — the shape the
/// push serialiser reads. There is no `hw`: `last_offset` is the visible tail.
/// Numbers only: the committed row is read in place
/// ([`TypedReads::partition_with`]) — the view used to copy three strings out
/// of every partition read (task 7).
#[derive(Clone, Copy, Debug)]
struct PartView {
    last_offset: i64,
    last_created_at_us: i64,
}

/// What the committed side holds for a pid, as [`Planner::partition`] needs it.
enum CommittedPart {
    /// No row, or a garbage one.
    None,
    /// A row of a queue a delete in flight drops: going away with it.
    Dropped,
    Head(crate::rsm::store::rows::PartitionHead),
}

/// A partition's committed dedup rows reconstructed from the segment files
/// (PERF-E `DEDUP_INDEX=segment`), base-sorted, with `TxnsRow::end` INCLUSIVE
/// (the stored-`txns`-row shape every reader expects).
type CommittedTxnsRows = Vec<(u64, TxnsRow)>;

/// The committed segment context of a partition (PERF-E): its logical bucket,
/// the committed offset bound (`committed last_offset + 1`, from the RoTxn), and
/// its committed sealed-file list (`partition_files`). Cached per pid per
/// cycle. `None` when the partition is unknown or has no committed
/// frames.
struct SegCtx {
    bucket: u16,
    committed_end: u64,
    sealed: Vec<u32>,
    /// Phase A2: the per-queue-log id (`xxh3(tenant ␟ queue)`), so the qlog dedup
    /// read keys the same partition's records the segment read reaches by
    /// `(bucket, sealed)`. Derived from the COMMITTED partition row, like every
    /// other field here.
    queue_id: u64,
}

impl<'a, R: Reads + ?Sized> Planner<'a, R> {
    pub fn new(
        committed: Committed<'a, R>,
        now_us: i64,
        cfg: PlanConfig,
        front: &'a DedupFront,
        reader: Option<Reader>,
    ) -> Planner<'a, R> {
        Planner {
            committed,
            now_us,
            cfg,
            front,
            reader,
            qlog_reader: None,
            seg_cache: RefCell::new(HashMap::new()),
            seg_ctx_cache: RefCell::new(HashMap::new()),
        }
    }

    /// Point the committed `DEDUP_INDEX=segment` dedup reads at the per-queue log
    /// (Phase A2, `QUEEN_RAFT_QLOG`). `Some` reads the committed hashes from the
    /// qlog (same committed bound, same O(claimed) walk); `None` keeps the
    /// segment read path. The batcher sets it once per cycle from its handle.
    pub fn set_qlog_reader(&mut self, reader: Option<QLogReader>) -> &mut Self {
        self.qlog_reader = reader;
        self
    }

    /// The dedup front the batcher threads through this cycle.
    pub fn front(&self) -> &DedupFront {
        self.front
    }

    pub fn plan_effects(&self, ov: &mut Overlay, c: &EffectsCommand) -> Planned {
        if c.effects.is_empty() {
            return Ok(Plan::Empty(Outcome::Empty));
        }
        self.check_partition_rows(ov, &c.effects)?;
        let mut effects = c.effects.clone();
        self.cover_dropped_partitions(ov, &mut effects)?;
        ov.apply_effects(&effects);
        // B26: the dedup front forgets the partitions this deletes now, not a
        // dedup window later (sound either way: an unknown pid is re-seeded).
        for e in &effects {
            match e {
                Effect::GarbageAdd { pids, scope, .. }
                    if !matches!(scope, crate::rsm::effect::GarbageScope::Group { .. }) =>
                {
                    self.front.forget(pids)
                }
                Effect::PartitionDelete { pid } => self.front.forget(std::slice::from_ref(pid)),
                _ => {}
            }
        }
        Ok(Plan::logged(effects, Outcome::Empty))
    }

    /// The cursor and dead-letter rows `effects` write (a seek's, the
    /// consumption engine's checkpoints and transaction riders) name
    /// partitions read before this was planned. A cursor row of one a delete
    /// in flight takes away, or one being chunk-deleted, would reach apply
    /// after its partition row is gone — fatal there — and a dead letter would
    /// outlive the partition it names. Refused whole and retryable: the retry
    /// reads the partitions again (the engine drops the rows of a partition
    /// that is gone and sends the rest).
    pub(crate) fn check_partition_rows(
        &self,
        ov: &Overlay,
        effects: &[Effect],
    ) -> Result<(), Refusal> {
        for e in effects {
            if let Effect::CursorSet { pid, .. } | Effect::DlqInsert { pid, .. } = e {
                if self.partition(ov, *pid)?.is_none() {
                    return Err(Refusal::retry(
                        "partition_gone",
                        format!("partition {pid} is gone or being deleted"),
                    ));
                }
            }
        }
        Ok(())
    }

    /// A queue delete or a tenant purge takes away every partition of what it
    /// drops, and the receiver named them from committed state BEFORE the
    /// command was planned. A partition created since — still in flight, or
    /// committed after that read — loses its names with the queue but would
    /// never become garbage: its rows and payload would stay for ever, and
    /// nothing could read its messages. Every live partition the command does
    /// not name joins its `GarbageAdd` and its first `DeleteChunk`; the chunks
    /// that follow (the receiver's, or the leader's resume) finish them.
    fn cover_dropped_partitions(
        &self,
        ov: &Overlay,
        effects: &mut Vec<Effect>,
    ) -> Result<(), Refusal> {
        use crate::rsm::effect::GarbageScope;
        let mut drops: Vec<(&str, Option<&str>)> = Vec::new();
        let mut scope = GarbageScope::Queue;
        for e in effects.iter() {
            match e {
                Effect::QueueDelete { tenant, queue } => drops.push((tenant, Some(queue))),
                Effect::TenantPurge { tenant } => {
                    drops.push((tenant, None));
                    scope = GarbageScope::Tenant;
                }
                _ => {}
            }
        }
        if drops.is_empty() {
            return Ok(());
        }
        let named: std::collections::HashSet<Pid> = effects
            .iter()
            .filter_map(|e| match e {
                Effect::GarbageAdd { pids, scope, .. }
                    if !matches!(scope, GarbageScope::Group { .. }) =>
                {
                    Some(pids.iter().copied())
                }
                _ => None,
            })
            .flatten()
            .collect();
        let mut missing: Vec<Pid> = Vec::new();
        for (tenant, queue) in drops {
            for pid in self.live_partitions(ov, tenant, queue)? {
                if !named.contains(&pid) && !missing.contains(&pid) {
                    missing.push(pid);
                }
            }
        }
        if missing.is_empty() {
            return Ok(());
        }
        let wide = |s: &GarbageScope| !matches!(s, GarbageScope::Group { .. });
        match effects
            .iter_mut()
            .find(|e| matches!(e, Effect::GarbageAdd { scope, .. } if wide(scope)))
        {
            Some(Effect::GarbageAdd { pids, .. }) => pids.extend_from_slice(&missing),
            _ => effects.push(Effect::GarbageAdd {
                pids: missing.clone(),
                scope: scope.clone(),
                deleted_at_us: self.now_us,
            }),
        }
        match effects
            .iter_mut()
            .find(|e| matches!(e, Effect::DeleteChunk { scope, .. } if wide(scope)))
        {
            Some(Effect::DeleteChunk { pids, .. }) => pids.extend_from_slice(&missing),
            _ => effects.push(Effect::DeleteChunk {
                pids: missing,
                scope,
                resume: Vec::new(),
                limit: 1_000,
            }),
        }
        Ok(())
    }

    /// Every partition of a queue (`Some`) or of a whole tenant (`None`) that
    /// is live in the merged view: committed and not garbage or going away,
    /// or created in flight. In pid order.
    fn live_partitions(
        &self,
        ov: &Overlay,
        tenant: &str,
        queue: Option<&str>,
    ) -> Result<Vec<Pid>, Refusal> {
        let mut queues: Vec<String> = Vec::new();
        match queue {
            Some(q) => queues.push(q.to_string()),
            None => {
                self.reads()
                    .scan_queues(tenant, usize::MAX, &mut |name, _| {
                        queues.push(name.to_string());
                        true
                    })
                    .map_err(store_err)?;
            }
        }
        let mut pids: BTreeSet<Pid> = BTreeSet::new();
        for q in &queues {
            let mut found: Vec<Pid> = Vec::new();
            self.reads()
                .scan_queue_partitions(tenant, q, None, usize::MAX, &mut |pid| {
                    found.push(pid);
                    true
                })
                .map_err(store_err)?;
            for pid in found {
                if self.partition(ov, pid)?.is_some() {
                    pids.insert(pid);
                }
            }
        }
        for (pid, part) in &ov.parts {
            if let Some(c) = &part.created {
                if c.v.tenant == tenant && queue.is_none_or(|q| c.v.queue == q) && !ov.is_gone(*pid)
                {
                    pids.insert(*pid);
                }
            }
        }
        Ok(pids.into_iter().collect())
    }

    fn reads(&self) -> &'a R {
        self.committed.reads()
    }

    /// The committed cluster version (§12.8, D20): what a row this planner
    /// writes may carry ([`crate::rsm::effect::CursorRow::admit`]).
    pub(crate) fn cluster_version(&self) -> Result<u32, Refusal> {
        use crate::rsm::store::TypedReads;
        self.reads().cluster_version().map_err(store_err)
    }

    /// §5.4/D6/I6: committed `request_ids`, then the entries in flight. A
    /// committed hit answers and plans nothing; an in-flight hit waits on that
    /// entry (the batcher owns the wait).
    pub fn lookup_request_id(&self, ov: &Overlay, id: &RequestId) -> Result<Lookup, Refusal> {
        if let Some(bytes) = self.committed.recorded_outcome(id).map_err(store_err)? {
            let outcome = Outcome::decode(&bytes).map_err(|e| {
                // A recorded outcome that will not decode is these bytes a
                // quorum committed; it is fatal for the node, not a client
                // retry (I16). The planner surfaces it as a non-retryable
                // refusal and the batcher escalates.
                Refusal::client("corrupt_outcome", format!("recorded outcome: {e}"))
            })?;
            return Ok(Lookup::Committed(outcome));
        }
        if let Some(o) = ov.request_id(id) {
            return Ok(Lookup::InFlight(o.clone()));
        }
        Ok(Lookup::Miss)
    }

    // ---- merged reads -----------------------------------------------------

    /// The effective queue config: overlay upsert (or tombstone → gone), else
    /// committed — unless a purge of the tenant is in flight.
    fn queue_cfg(
        &self,
        ov: &Overlay,
        tenant: &str,
        queue: &str,
    ) -> Result<Option<QueueConfig>, Refusal> {
        if let Some(t) = ov.queues.get(&(tenant.to_string(), queue.to_string())) {
            return Ok(t.v.clone());
        }
        if ov.tenant_purged(tenant) {
            return Ok(None);
        }
        self.committed.queue(tenant, queue).map_err(store_err)
    }

    /// Whether `(tenant, queue, group)` is registered: a registration or a
    /// delete in flight (the overlay), else committed state — unless a delete
    /// in flight drops the queue's groups.
    fn group(
        &self,
        ov: &Overlay,
        tenant: &str,
        queue: &str,
        group: &str,
    ) -> Result<Option<GroupRow>, Refusal> {
        if let Some(t) = ov
            .groups
            .get(&(tenant.to_string(), queue.to_string(), group.to_string()))
        {
            return Ok(t.v.clone());
        }
        if ov.queue_dropped(tenant, queue) {
            return Ok(None);
        }
        self.reads().group(tenant, queue, group).map_err(store_err)
    }

    /// The live pid of a partition NAME. A name a delete in flight sweeps
    /// resolves to nothing, so a push re-creates the partition with a new pid
    /// instead of appending to one that is going away.
    fn pid_of(
        &self,
        ov: &Overlay,
        tenant: &str,
        queue: &str,
        partition: &str,
    ) -> Result<Option<Pid>, Refusal> {
        // No partition created in flight (steady state): no key to build.
        let created = if ov.pids_by_key.is_empty() {
            None
        } else {
            ov.pids_by_key
                .get(&(tenant.to_string(), queue.to_string(), partition.to_string()))
        };
        let pid = match created {
            Some(t) => Some(t.v),
            None if ov.queue_dropped(tenant, queue) => None,
            None => self
                .committed
                .pid_of(tenant, queue, partition)
                .map_err(store_err)?,
        };
        Ok(pid.filter(|p| !ov.is_gone(*p)))
    }

    /// The merged partition view, or `None` when the pid is unknown, garbage,
    /// or taken away by a delete in flight. The committed row is read in place
    /// (task 7): its numbers are copied, its names only compared.
    fn partition(&self, ov: &Overlay, pid: Pid) -> Result<Option<PartView>, Refusal> {
        if ov.is_gone(pid) {
            return Ok(None);
        }
        let overlay = ov.parts.get(&pid);
        let (mut last_offset, mut last_created) = match (
            self.committed_part(ov, pid)?,
            overlay.and_then(|o| o.created.as_ref()),
        ) {
            // Every committed partition predates the entries in flight, so
            // one of a queue a delete in flight drops is going away with it
            // — named in the delete's garbage or not.
            (CommittedPart::Dropped, _) => return Ok(None),
            (CommittedPart::Head(h), _) => (h.last_offset, h.last_created_at_us),
            (CommittedPart::None, Some(c)) => (-1, c.v.created_at_us - 1),
            (CommittedPart::None, None) => return Ok(None),
        };
        if let Some(last) = overlay.and_then(|o| o.appends.last()) {
            last_offset = last.end as i64;
            last_created = last.created_at_us;
        }
        Ok(Some(PartView {
            last_offset,
            last_created_at_us: last_created,
        }))
    }

    /// The committed half of [`Planner::partition`]: `Committed::partition`
    /// (no row for a garbage pid) through the in-place reads, and whether a
    /// delete in flight drops the row's queue.
    fn committed_part(&self, ov: &Overlay, pid: Pid) -> Result<CommittedPart, Refusal> {
        let reads = self.reads();
        if reads.is_garbage(pid).map_err(store_err)? {
            return Ok(CommittedPart::None);
        }
        let got = reads
            .partition_with(pid, |p| {
                if ov.queue_dropped(p.tenant, p.queue) {
                    CommittedPart::Dropped
                } else {
                    CommittedPart::Head(p.head)
                }
            })
            .map_err(store_err)?;
        Ok(got.unwrap_or(CommittedPart::None))
    }

    // ---- the committed dedup authority from the SEGMENTS (PERF-E) ----------

    /// The committed dedup rows of `pid` reconstructed from the segment files
    /// (`DEDUP_INDEX=segment`), materialized ONCE per pid per cycle and shared
    /// across every committed `Txns`-authority read of that pid this cycle
    /// (the dedup probe and the front seed). Base-sorted, with
    /// `TxnsRow::end` INCLUSIVE (like a stored `txns` row); bounded to the
    /// committed tail by [`segments::Reader::committed_dedup_rows`].
    ///
    /// THE committed bound is read HERE, from the COMMITTED partition row via
    /// the RoTxn (never the overlay-merged `PartView`, whose `last_offset`
    /// includes in-flight appends): `committed_end = committed last_offset + 1`.
    /// The overlay carries `(committed, in_flight]`, so the committed leg must
    /// stop at the committed tail — this is the exactly-once invariant.
    fn committed_txns_rows(&self, pid: Pid) -> Result<Rc<CommittedTxnsRows>, Refusal> {
        if let Some(hit) = self.seg_cache.borrow().get(&pid) {
            return Ok(hit.clone());
        }
        let rows = self.build_committed_txns_rows(pid)?;
        let rc = Rc::new(rows);
        self.seg_cache.borrow_mut().insert(pid, rc.clone());
        Ok(rc)
    }

    /// The WHOLE committed window: the dedup probe/seed authority, cached per
    /// pid per cycle. It is bloom-gated (a probe reads it only on a "maybe"),
    /// so its O(window) hash reads are acceptable there.
    fn build_committed_txns_rows(&self, pid: Pid) -> Result<CommittedTxnsRows, Refusal> {
        let Some(ctx) = self.seg_ctx(pid)? else {
            return Ok(Vec::new());
        };
        let frames = self.committed_frames_of(pid, &ctx)?;
        {
            use crate::rsm::dbgctr::{inc, C};
            inc(&C.push_dedup_build, 1);
            inc(&C.push_dedup_records, frames.len() as u64);
        }
        Ok(frames
            .into_iter()
            .map(|f| {
                (
                    f.base_offset,
                    TxnsRow {
                        end: f.end - 1,
                        created_at_us: f.created_at_us,
                        hashes: f.hashes,
                    },
                )
            })
            .collect())
    }

    /// Every committed dedup frame of `pid`, with its hashes, bounded to the
    /// committed tail — from the QLOG when `QUEEN_RAFT_QLOG` is on (Phase A2),
    /// else the segments (lever 1). Either source returns the SAME
    /// `(base, end-exclusive, created_at, hashes)` shape and the SAME committed
    /// bound (`ctx.committed_end`, the committed partition row's `last_offset + 1`
    /// — the exactly-once invariant), so off-vs-on is byte-identical.
    fn committed_frames_of(&self, pid: Pid, ctx: &SegCtx) -> Result<Vec<DedupFrame>, Refusal> {
        if let Some(ql) = self.qlog_reader.as_ref() {
            let frames = ql
                .committed_frames(ctx.queue_id, pid, 0, ctx.committed_end, true)
                .map_err(|e| Refusal::retry("unavailable", format!("qlog dedup read: {e}")))?;
            return Ok(frames
                .into_iter()
                .map(|f| DedupFrame {
                    base_offset: f.base_offset,
                    end: f.end,
                    created_at_us: f.created_at_us,
                    hashes: f.hashes,
                })
                .collect());
        }
        let Some(reader) = self.reader.as_ref() else {
            return Err(Refusal::retry(
                "unavailable",
                "DEDUP_INDEX=segment planner has no segment reader",
            ));
        };
        reader
            .committed_dedup_rows(ctx.bucket, pid, 0, ctx.committed_end, &ctx.sealed)
            .map_err(|e| Refusal::retry("unavailable", format!("segment dedup read: {e}")))
    }

    /// The committed segment context for `pid` (bucket, committed offset bound,
    /// sealed-file list), cached per pid per cycle. The committed bound is
    /// derived HERE, ONCE, from the COMMITTED partition row via the RoTxn — NEVER
    /// the overlay, which would include in-flight appends. `None` when the
    /// partition is unknown or has no committed frames.
    fn seg_ctx(&self, pid: Pid) -> Result<Option<Rc<SegCtx>>, Refusal> {
        if let Some(hit) = self.seg_ctx_cache.borrow().get(&pid) {
            return Ok(hit.clone());
        }
        let ctx = self.build_seg_ctx(pid)?;
        self.seg_ctx_cache.borrow_mut().insert(pid, ctx.clone());
        Ok(ctx)
    }

    fn build_seg_ctx(&self, pid: Pid) -> Result<Option<Rc<SegCtx>>, Refusal> {
        // The committed row (`Committed::partition`: none for a garbage pid),
        // read in place: its tail, and the bucket and queue log its names
        // key, without copying the names.
        let reads = self.reads();
        if reads.is_garbage(pid).map_err(store_err)? {
            return Ok(None);
        }
        let Some((committed_end, bucket, queue_id)) = reads
            .partition_with(pid, |p| {
                (
                    (p.head.last_offset + 1).max(0) as u64,
                    bucket_of(p.tenant, p.queue, p.partition),
                    QLogSet::queue_id_of(p.tenant, p.queue),
                )
            })
            .map_err(store_err)?
        else {
            return Ok(None);
        };
        if committed_end == 0 {
            return Ok(None);
        }
        let mut sealed: Vec<u32> = Vec::new();
        self.reads()
            .scan_partition_files(pid, usize::MAX, &mut |f| {
                sealed.push(f);
                true
            })
            .map_err(store_err)?;
        Ok(Some(Rc::new(SegCtx {
            bucket,
            committed_end,
            sealed,
            queue_id,
        })))
    }

    // ---- dedup (committed txns + overlay) ---------------------------------

    /// The duplicate verdict for one hash: the ORIGINAL offset (the min over the
    /// window) or `None`. Merges committed occurrences with the overlay's.
    fn dedup_probe_one(
        &self,
        ov: &Overlay,
        pid: Pid,
        hash: &[u8; 16],
        window_floor_us: i64,
    ) -> Result<Option<u64>, Refusal> {
        // The front decides whether the committed authority must be read: a
        // "skip" is a proof that it holds no in-window occurrence of this hash
        // (see [`DedupFront`]). Under `rows` the read is a single LMDB get on
        // the `(pid, hash)` row; under `txns` a "maybe" names the generation
        // offset bands, and the read is a bounded ordered scan of the txns rows
        // (PERF-E). The overlay merge below is UNCONDITIONAL — the front fronts
        // only the committed read, never the in-flight state, so an in-flight
        // duplicate is still caught even on a skip. `front_prepare` must have
        // run for this pid this command.
        let mut best = match self.cfg.index_mode {
            IndexMode::Rows => {
                if self.front.should_probe(pid, hash, window_floor_us) {
                    dedup::probe_one(self.reads(), pid, hash, window_floor_us).map_err(store_err)?
                } else {
                    None
                }
            }
            IndexMode::Txns => self.dedup_probe_txns(pid, hash, window_floor_us)?,
            IndexMode::Segment => self.dedup_probe_segment(pid, hash, window_floor_us)?,
        };
        if let Some(occ) = ov.dedup.get(&(pid, *hash)) {
            for (off, created) in occ {
                if *created >= window_floor_us {
                    best = Some(best.map_or(*off, |b| b.min(*off)));
                }
            }
        }
        Ok(best)
    }

    /// The committed leg of a push probe under `DEDUP_INDEX=txns`: ask the front
    /// for the offset bands to scan, then scan the txns rows for the hash. A
    /// warm filter bounds the scan to one generation's band; an unseeded /
    /// fallback / disabled front scans the whole txns window (exact).
    fn dedup_probe_txns(
        &self,
        pid: Pid,
        hash: &[u8; 16],
        window_floor_us: i64,
    ) -> Result<Option<u64>, Refusal> {
        let mut ranges: Vec<(u64, u64)> = Vec::new();
        let verdict = self
            .front
            .probe_plan(pid, hash, window_floor_us, &mut ranges);
        self.dedup_committed(pid, hash, window_floor_us, verdict, &ranges)
    }

    /// The committed leg of a probe the front has planned (`verdict`, and for
    /// `Ranges` its `bands`), under `DEDUP_INDEX=txns` or `segment` — see
    /// [`Planner::dedup_probe_txns`] and [`Planner::dedup_probe_segment`].
    fn dedup_committed(
        &self,
        pid: Pid,
        hash: &[u8; 16],
        window_floor_us: i64,
        verdict: ProbeVerdict,
        bands: &[(u64, u64)],
    ) -> Result<Option<u64>, Refusal> {
        match (self.cfg.index_mode, verdict) {
            (_, ProbeVerdict::Skip) => Ok(None),
            (IndexMode::Segment, ProbeVerdict::Ranges) if self.qlog_reader.is_some() => {
                self.dedup_probe_qlog_bands(pid, hash, bands, window_floor_us)
            }
            (IndexMode::Segment, _) => {
                let rows = self.committed_txns_rows(pid)?;
                Ok(dedup::scan_seg_rows_for_hash(&rows, hash, window_floor_us))
            }
            (_, ProbeVerdict::Ranges) => {
                dedup::scan_txns_for_hash(self.reads(), pid, hash, bands, window_floor_us)
                    .map_err(store_err)
            }
            (_, ProbeVerdict::Whole) => {
                dedup::scan_txns_for_hash_whole(self.reads(), pid, hash, window_floor_us)
                    .map_err(store_err)
            }
        }
    }

    /// B31: the duplicate verdict of every item of a push to `pid`, each as
    /// [`Planner::dedup_probe_one`] gives it — but the front consulted for all
    /// of them under one hold of its shard (seeding the partition first when
    /// it is not), where it was locked once per message to probe and once per
    /// survivor to insert.
    pub(crate) fn dedup_probe_many(
        &self,
        ov: &Overlay,
        pid: Pid,
        items: &[PushItem],
        window_floor_us: i64,
    ) -> Result<Vec<Option<u64>>, Refusal> {
        if self.cfg.index_mode == IndexMode::Rows {
            self.front_prepare(ov, pid, window_floor_us)?;
            return items
                .iter()
                .map(|it| self.dedup_probe_one(ov, pid, &it.hash, window_floor_us))
                .collect();
        }
        let window_us = self.now_us.saturating_sub(window_floor_us);
        let mut bands: Vec<(u64, u64)> = Vec::new();
        let mut plans: Vec<dedup::HashPlan> = Vec::with_capacity(items.len());
        let hashes = || items.iter().map(|it| &it.hash);
        let mut planned = self.front.probe_plan_many(
            pid,
            hashes(),
            window_floor_us,
            window_us,
            &mut bands,
            &mut plans,
        );
        if !planned {
            self.front_prepare(ov, pid, window_floor_us)?;
            planned = self.front.probe_plan_many(
                pid,
                hashes(),
                window_floor_us,
                window_us,
                &mut bands,
                &mut plans,
            );
        }
        if !planned {
            // Not seeded after all (a reset under us): the whole window, as an
            // unseeded probe plans it.
            bands.clear();
            plans = items
                .iter()
                .map(|_| dedup::HashPlan {
                    verdict: ProbeVerdict::Whole,
                    from: 0,
                    to: 0,
                })
                .collect();
        }
        let mut out = Vec::with_capacity(items.len());
        for (it, plan) in items.iter().zip(&plans) {
            let mut best = self.dedup_committed(
                pid,
                &it.hash,
                window_floor_us,
                plan.verdict,
                &bands[plan.from..plan.to],
            )?;
            if let Some(occ) = ov.dedup.get(&(pid, it.hash)) {
                for (off, created) in occ {
                    if *created >= window_floor_us {
                        best = Some(best.map_or(*off, |b| b.min(*off)));
                    }
                }
            }
            out.push(best);
        }
        Ok(out)
    }

    /// The committed leg of a push probe under `DEDUP_INDEX=segment`: the front
    /// still decides Skip vs scan (its seed and inserts are mode-agnostic — the
    /// same in-window hashes either way), and a scan returns the exact MIN
    /// in-window offset.
    ///
    /// PLAN_RAFT_DRAIN_FIX P3.1: with the qlog on, a `Ranges` verdict reads ONLY
    /// the committed records overlapping the generation bands the front matched
    /// ([`Planner::dedup_probe_qlog_bands`]) — O(band), not O(partition
    /// history). `Whole` (unseeded / fallback / disabled front) and the segment
    /// source keep the whole committed window (RAM-cached per pid this cycle); a
    /// "maybe" band is a sound over-approximation, so the whole-window minimum
    /// matches the `txns` verdict.
    fn dedup_probe_segment(
        &self,
        pid: Pid,
        hash: &[u8; 16],
        window_floor_us: i64,
    ) -> Result<Option<u64>, Refusal> {
        let mut ranges: Vec<(u64, u64)> = Vec::new();
        let verdict = self
            .front
            .probe_plan(pid, hash, window_floor_us, &mut ranges);
        self.dedup_committed(pid, hash, window_floor_us, verdict, &ranges)
    }

    /// PLAN_RAFT_DRAIN_FIX P3.1: the band-limited committed probe over the qlog.
    /// Reads only the committed records that overlap a band
    /// (`base <= hi && end > lo`), bounded by `ctx.committed_end` exactly as the
    /// whole-window read (an uncommitted record is the overlay's, which
    /// [`Planner::dedup_probe_one`] merges unconditionally), hashes only and
    /// cross-cycle cached (P3.2/P3.3), and returns the MIN in-window occurrence
    /// among them.
    ///
    /// SOUND: every committed in-window occurrence of `hash` was inserted into a
    /// live front generation (seed or plan-time insert; a generation ages out
    /// only once wholly below the floor), whose bloom therefore says "maybe" and
    /// whose `(min_base, max_off)` band holds the occurrence's record — so the
    /// band read sees every occurrence the whole-window scan would, and returns
    /// the same minimum.
    fn dedup_probe_qlog_bands(
        &self,
        pid: Pid,
        hash: &[u8; 16],
        bands: &[(u64, u64)],
        window_floor_us: i64,
    ) -> Result<Option<u64>, Refusal> {
        let Some(ql) = self.qlog_reader.as_ref() else {
            return Err(Refusal::retry(
                "unavailable",
                "qlog band probe without a qlog reader",
            ));
        };
        let Some(ctx) = self.seg_ctx(pid)? else {
            return Ok(None); // no committed frames: nothing committed to probe
        };
        let frames = ql
            .committed_hashes_in_bands(ctx.queue_id, pid, bands, ctx.committed_end)
            .map_err(|e| Refusal::retry("unavailable", format!("qlog dedup band read: {e}")))?;
        {
            use crate::rsm::dbgctr::{inc, C};
            inc(&C.push_dedup_build, 1);
            inc(&C.push_dedup_records, frames.len() as u64);
        }
        Ok(crate::rsm::qlog::min_in_window_offset(
            &frames,
            hash,
            window_floor_us,
        ))
    }

    /// Ensure the dedup front can answer for `pid` before the per-hash probe
    /// loop. A partition it has not seen is seeded from the committed `txns`
    /// window UNION the overlay's in-flight appends (both filtered to the dedup
    /// window), or marked always-probe when that union exceeds the seed cap. A
    /// no-op when the front is disabled or the partition is already seeded,
    /// fallback, or was born seeded by a create this front-lifetime.
    pub fn front_prepare(&self, ov: &Overlay, pid: Pid, floor_us: i64) -> Result<(), Refusal> {
        if !self.front.needs_seed(pid) {
            return Ok(());
        }
        let seed = self.front_collect_seed(ov, pid, floor_us)?;
        self.front.install_seed(pid, seed, floor_us);
        Ok(())
    }

    /// Collect the in-window seed for `pid`: committed `txns` hashes then the
    /// overlay's in-flight append hashes, in created order, stopping at
    /// [`dedup::FRONT_SEED_MAX`]. The union is what makes a skip sound for a
    /// partition whose recent appends are still in flight (not yet in `txns`).
    fn front_collect_seed(&self, ov: &Overlay, pid: Pid, floor_us: i64) -> Result<Seed, Refusal> {
        let mut buf: Vec<SeedHash> = Vec::new();
        // B26: every committed append is stamped at or before the partition
        // row's `last_created_at_us` (apply sets it per append, and stamps are
        // monotone per partition), so a partition whose newest committed
        // append is below the floor has nothing committed in window: no row
        // is read. That is every partition the front forgot after a quiet
        // window, when it is pushed to again.
        let committed_in_window = self
            .reads()
            .partition_with(pid, |p| p.head.last_created_at_us >= floor_us)
            .map_err(store_err)?
            .unwrap_or(false);
        // The committed leg. Under `segment` the in-window hashes come from the
        // committed segment rows (bounded to the committed tail) instead of the
        // `txns` keyspace; the SEED and the front are otherwise identical, so a
        // partition seeded either way answers the same skip verdicts.
        if !committed_in_window {
            // Nothing committed to seed.
        } else if self.cfg.index_mode == IndexMode::Segment {
            let rows = self.committed_txns_rows(pid)?;
            for (base, row) in rows.iter() {
                if row.created_at_us >= floor_us {
                    for (i, h) in row.iter_hashes().enumerate() {
                        buf.push(SeedHash {
                            hash: h,
                            created_us: row.created_at_us,
                            base_off: *base,
                            msg_off: base + i as u64,
                        });
                    }
                    if buf.len() > dedup::FRONT_SEED_MAX {
                        return Ok(Seed::Overflow);
                    }
                }
            }
        } else {
            let prefix = keys::txns_prefix(pid);
            let mut bad: Option<StoreError> = None;
            let mut overflow = false;
            self.reads()
                .scan_raw(
                    Keyspace::Txns,
                    &prefix,
                    &prefix,
                    usize::MAX,
                    &mut |k, v| match (keys::txns_base_of(k), TxnsRow::decode(v)) {
                        (Some(base), Ok(row)) => {
                            if row.created_at_us >= floor_us {
                                for (i, h) in row.iter_hashes().enumerate() {
                                    buf.push(SeedHash {
                                        hash: h,
                                        created_us: row.created_at_us,
                                        base_off: base,
                                        msg_off: base + i as u64,
                                    });
                                }
                                if buf.len() > dedup::FRONT_SEED_MAX {
                                    overflow = true;
                                    return false;
                                }
                            }
                            true
                        }
                        _ => {
                            bad = Some(StoreError::corrupt(Keyspace::Txns, "txns row"));
                            false
                        }
                    },
                )
                .map_err(store_err)?;
            if let Some(e) = bad {
                return Err(store_err(e));
            }
            if overflow {
                return Ok(Seed::Overflow);
            }
        }
        if let Some(o) = ov.parts.get(&pid) {
            for a in &o.appends {
                if a.created_at_us >= floor_us {
                    for (i, h) in a.hashes.iter().enumerate() {
                        buf.push(SeedHash {
                            hash: *h,
                            created_us: a.created_at_us,
                            base_off: a.base,
                            msg_off: a.base + i as u64,
                        });
                    }
                    if buf.len() > dedup::FRONT_SEED_MAX {
                        return Ok(Seed::Overflow);
                    }
                }
            }
        }
        Ok(Seed::Complete(buf))
    }
}

// ---------------------------------------------------------------------------
// bucket_of
// ---------------------------------------------------------------------------

/// `xxh3(tenant ␟ queue ␟ partition) % 256` (§0.4, §5.2): which of the 256
/// segment-file groups an `Append`'s payload lands in. The LEADER computes it
/// once and carries it in the effect; every node files the bytes the same way
/// from the carried value (WP-1.3 `Effect::Append.bucket`), so this is the only
/// definition and it does not need to match any node's local hashing.
///
/// The separator is the ASCII unit separator `0x1F`, which the glossary's "␟"
/// denotes; it cannot occur in a `xxh3` of anything but the three names, so a
/// pair like `("ab","c")` and `("a","bc")` cannot collide on it. WP-1.3 refused
/// to guess between this and U+241F and handed the choice to this WP.
pub fn bucket_of(tenant: &str, queue: &str, partition: &str) -> u16 {
    let mut buf = Vec::with_capacity(tenant.len() + queue.len() + partition.len() + 2);
    buf.extend_from_slice(tenant.as_bytes());
    buf.push(0x1F);
    buf.extend_from_slice(queue.as_bytes());
    buf.push(0x1F);
    buf.extend_from_slice(partition.as_bytes());
    (xxhash_rust::xxh3::xxh3_64(&buf) % 256) as u16
}

/// A group's stored policy built for a first-contact registration from a
/// carried intent (a position's): `now`/`new`/unparsable → `new` at the
/// registration instant; an explicit instant → `timestamp`; otherwise `all`.
pub(crate) fn group_meta_for_registration(
    id: [u8; 16],
    partition_name: &str,
    namespace: &str,
    task: &str,
    intent: &SubIntent,
    conflation: bool,
    now_us: i64,
) -> GroupMeta {
    let (mode, ts) = if let Some(ts) = intent.from_us {
        (SubscriptionMode::Timestamp, ts)
    } else if intent.now || intent.mode == "new" {
        (SubscriptionMode::New, now_us)
    } else {
        (SubscriptionMode::All, i64::MIN)
    };
    GroupMeta {
        id,
        partition_name: partition_name.to_string(),
        namespace: namespace.to_string(),
        task: task.to_string(),
        mode,
        subscription_timestamp_us: ts,
        conflation,
        seeded: false,
        registered_at_us: now_us,
    }
}
