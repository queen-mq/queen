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

/// Homes: the shard of a key's in-flight state, by its name.
pub(crate) mod homes;
/// KEEP_OVERLAY: the overlay kept between cycles on the planner thread.
pub(crate) mod kept;
/// The store keys an effect derives, measured before it is logged.
pub(crate) mod keycheck;
/// KV: versions from `kv_version_base + ordinal`, TTL, prefix lists,
/// `required`, and the leader's expiry sweep. WP-2.2.
pub mod kv;
pub mod push;
/// Routing: links into the readers of a queue (PLAN_ROUTING.md).
pub mod route;
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
    /// The cluster version the cycle read when it began (D20), set by the
    /// batcher on its per-cycle copy. Every gate a planner of the cycle asks
    /// ([`Planner::cluster_version`]) then answers from this one value, the
    /// one the batcher checks the entry against: a rise that apply lands while
    /// the cycle plans cannot put a newer shape in an entry that the check
    /// would refuse. `None` (tests, a planner built outside a cycle) reads the
    /// committed value.
    pub cycle_cluster_version: Option<u32>,
}

impl Default for PlanConfig {
    fn default() -> PlanConfig {
        PlanConfig {
            entry_max_bytes: crate::rsm::entry::ENTRY_MAX_BYTES_DEFAULT,
            plan_budget_ms: PLAN_BUDGET_MS_DEFAULT,
            slow_command_ms: SLOW_COMMAND_MS_DEFAULT,
            index_mode: IndexMode::Rows,
            cycle_cluster_version: None,
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
    /// Shared, not owned: a copy of the command (the retry copy a cluster node
    /// keeps, a multi-push split) shares these bytes instead of copying them.
    #[serde(with = "frame_bytes")]
    pub frame: bytes::Bytes,
}

/// [`PushItem::frame`] on the wire: bytes, exactly as `serde_bytes` wrote the
/// `Vec<u8>` it used to be.
mod frame_bytes {
    pub fn serialize<S: serde::Serializer>(b: &bytes::Bytes, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(b)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<bytes::Bytes, D::Error> {
        <serde_bytes::ByteBuf as serde::Deserialize>::deserialize(d)
            .map(|b| bytes::Bytes::from(b.into_vec()))
    }
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
    /// Whether an item is a link frame, when the command's maker knows: a
    /// push that carries links plans alone, and finding out reads every frame
    /// on the cycle thread. Never on the wire: a decoded command does not know.
    #[serde(skip)]
    pub links: Links,
}

/// [`PushCommand::links`]: known, or not (the default), and then the planner
/// reads the frames. It takes no part in a command's equality.
#[derive(Clone, Copy, Debug, Default)]
pub struct Links(Option<bool>);

impl Links {
    /// Known from the items, by the thread that just built their frames.
    pub fn of(items: &[PushItem]) -> Self {
        Links(Some(items.iter().any(|it| route::is_link_frame(&it.frame))))
    }

    /// Whether an item of `items` is a link frame: read from them only when
    /// not known.
    pub fn any(self, items: &[PushItem]) -> bool {
        self.0
            .unwrap_or_else(|| items.iter().any(|it| route::is_link_frame(&it.frame)))
    }
}

impl PartialEq for Links {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for Links {}

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
    /// The message's headers block (`crate::frames`), empty for none. Not in
    /// the serde shape: an older leader decodes a forwarded ack by position
    /// (postcard), so a forwarded command carries the blocks after itself
    /// (`facade::remote`), as only a cluster at catalogue version 8 has any.
    #[serde(skip)]
    pub headers: Vec<u8>,
}

impl crate::rsm::batcher::Command {
    /// Every DLQ snapshot the command carries, in one fixed order: the order
    /// a forwarded command's headers blocks travel in (`facade::remote`).
    pub fn dlq_snapshots_mut(&mut self) -> Vec<&mut DlqSnapshot> {
        use crate::rsm::batcher::Command;
        fn of(t: &mut [AckTarget]) -> Vec<&mut DlqSnapshot> {
            t.iter_mut()
                .flat_map(|t| t.items.iter_mut())
                .filter_map(|i| i.snapshot.as_mut())
                .collect()
        }
        match self {
            Command::Ack(c) => of(&mut c.targets),
            Command::Transaction(c) => of(&mut c.acks),
            Command::DlqHead(c) => vec![&mut c.snapshot],
            _ => Vec::new(),
        }
    }

    /// [`Self::dlq_snapshots_mut`], to read.
    pub fn dlq_snapshots(&self) -> Vec<&DlqSnapshot> {
        use crate::rsm::batcher::Command;
        fn of(t: &[AckTarget]) -> Vec<&DlqSnapshot> {
            t.iter()
                .flat_map(|t| t.items.iter())
                .filter_map(|i| i.snapshot.as_ref())
                .collect()
        }
        match self {
            Command::Ack(c) => of(&c.targets),
            Command::Transaction(c) => of(&c.acks),
            Command::DlqHead(c) => vec![&c.snapshot],
            _ => Vec::new(),
        }
    }
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
/// per-frame hashes in frame order: the dedup probe scans them for a message
/// still in flight ([`Planner::in_flight_min`]), and a seed of the dedup front
/// reads them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OverlayAppend {
    base: u64,
    end: u64,
    created_at_us: i64,
    hashes: Vec<[u8; 16]>,
    /// The fold that added it (KEEP_OVERLAY): its landing removes it.
    tag: u64,
}

/// A partition as the overlay knows it, relative to committed state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct OverlayPart {
    /// `Some` when a `PartitionCreate` for this pid is in the overlay: committed
    /// state has no row for it, so the merged view is built from this. It MUST
    /// go when the create lands (KEEP_OVERLAY): the flag is not idempotent.
    created: Option<Tagged<CreatedPart>>,
    appends: Vec<OverlayAppend>,
    /// A `Watermark` in the overlay (retention in flight): `(log_start,
    /// txns_start, rows_start)` — the last `None` for a version-1 watermark,
    /// whose rows follow its `txns_start`.
    watermark: Option<Tagged<(u64, u64, Option<u64>)>>,
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
    /// The partitions in flight, with their appends. An append keeps its
    /// frames' hashes, and that is all the dedup state in flight there is: no
    /// row per message. The dedup front answers "new" for a message in flight
    /// too (a plan inserts its hashes, a seed reads these appends), and any
    /// other answer scans the partition's appends here
    /// ([`Planner::dedup_probe_many`]).
    parts: Shards<Pid, OverlayPart>,
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
    /// A group's overlay (the footprint scheduler, `rsm/scheduler.rs`): the
    /// cycle's kept overlay, frozen while the groups plan, read under this
    /// one's own maps. Its own folds stay here; its masks (a delete, a purge)
    /// hide what the base holds for the keys they cover. `None` for every
    /// other overlay.
    base: Option<std::sync::Arc<Overlay>>,
    /// The shards of the partition state this overlay's own folds wrote, a bit
    /// each, [`NEW_SHARD`]'s too: a group's merge moves only those
    /// ([`Overlay::take_written_shards`]).
    shards_written: u128,
    /// The shards a group's overlay may read and fold (the footprint
    /// scheduler), `None` for all of them. A pid of any other shard marks the
    /// overlay [`Overlay::strayed`], and its run is planned again on one thread.
    allowed: Option<ShardMask>,
    strayed: Stray,
    /// It owns the shards `allowed` names for a run ([`Overlay::own`]): they
    /// go back to the kept overlay ([`Overlay::give_back`]).
    owner: bool,
    /// What undoes the folds into the partition state, innermost last: a
    /// transaction's ([`Overlay::save`]) and, under it, a planning thread's
    /// cycle in the shards it owns ([`Overlay::own`]). Empty when nothing may
    /// need undoing.
    frames: Vec<Undo>,
}

/// [`Overlay::strayed`], set through `&self`: the frozen overlay a group's
/// overlay reads through is shared by every planning thread.
#[derive(Debug, Default)]
struct Stray(std::sync::atomic::AtomicBool);

impl Stray {
    fn set(&self) {
        self.0.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    fn get(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Clone for Stray {
    fn clone(&self) -> Self {
        Stray(std::sync::atomic::AtomicBool::new(self.get()))
    }
}

/// What undoes a run of folds into the partition state: each partition as the
/// first fold of the run found it. Its appends since carry their frames'
/// hashes, so their occurrences go with them.
#[derive(Clone, Debug, Default)]
pub(crate) struct Undo {
    seen: std::collections::HashSet<Pid, FxBuild>,
    parts: Vec<PartWas>,
}

/// A partition as a run's first fold found it ([`Undo`]).
#[derive(Clone, Debug)]
struct PartWas {
    pid: Pid,
    appends: usize,
    created: Option<Tagged<CreatedPart>>,
    watermark: Option<Tagged<(u64, u64, Option<u64>)>>,
}

/// What a transaction may change in an overlay besides its partition state,
/// kept by [`Overlay::save`]: the small maps and the counters. The partition
/// state is not copied: the savepoint's undo frame records each partition as
/// the transaction first finds it.
pub(crate) struct Saved {
    next_pid: u64,
    next_kv_version: u64,
    max_now_us: i64,
    max_created_at_us: i64,
    folds: u64,
    shards_written: u128,
    queues: HashMap<(String, String), Tagged<Option<QueueConfig>>, FxBuild>,
    groups: HashMap<(String, String, String), Tagged<Option<GroupRow>>, FxBuild>,
    pids_by_key: HashMap<(String, String, String), Tagged<Pid>, FxBuild>,
    kv: HashMap<(String, String, String), Tagged<Option<crate::rsm::store::rows::KvRow>>, FxBuild>,
    timers: HashMap<(String, String, String), TimerSlot, FxBuild>,
    gone: HashMap<Pid, Tagged<()>, FxBuild>,
    dropped_queues: HashMap<(String, String), Tagged<()>, FxBuild>,
    purged_tenants: HashMap<String, Tagged<()>, FxBuild>,
}

/// A savepoint that was kept ([`Overlay::park`]), with what takes its folds
/// back out later.
pub(crate) struct Parked {
    saved: Saved,
    frame: Undo,
}

/// The top bit of a placeholder: a partition id or a KV version a group
/// planned (the footprint scheduler), numbered in entry order once every
/// group is done. No real id or version reaches it.
pub const PLACEHOLDER: u64 = 1 << 63;

/// Whether `n` is a placeholder ([`PLACEHOLDER`]).
pub fn is_placeholder(n: u64) -> bool {
    n & PLACEHOLDER != 0
}

/// The low 32 bits of a placeholder: its ordinal in its group. The bits
/// between it and [`PLACEHOLDER`] name the group.
pub const PLACEHOLDER_ORDINAL: u64 = (1 << 32) - 1;

/// A placeholder's group and its ordinal there.
pub fn placeholder_parts(n: u64) -> (u32, u64) {
    (((n & !PLACEHOLDER) >> 32) as u32, n & PLACEHOLDER_ORDINAL)
}

/// How many shards an overlay's partition state is split into, by pid. A pid's
/// shard comes from its name and never changes ([`homes::Homes`]); each shard
/// has one planning thread, its home, always the same
/// ([`homes::home_of_shard`]), which plans in it in place and lands its rows at
/// the start of its job. The count is fixed while the thread count is a knob
/// (`QUEEN_PLANNER_THREADS`, at most this many): the layout is the same with
/// one thread or eight, and a set of shards is one [`ShardMask`].
pub(crate) const PART_SHARDS: usize = 64;

/// The map after the shards: the partitions a group created this cycle, under
/// their placeholder, until the entry numbers them. A group's own, and empty in
/// a kept overlay.
pub(crate) const NEW_SHARD: usize = PART_SHARDS;

/// A set of shards ([`PART_SHARDS`]), a bit each.
pub(crate) type ShardMask = u64;

const _: () = assert!(PART_SHARDS == ShardMask::BITS as usize);

/// The shards of planning thread `home`, of `threads`: every shard whose home
/// it is ([`homes::home_of_shard`]).
pub(crate) fn shards_of_home(home: usize, threads: usize) -> ShardMask {
    (0..PART_SHARDS)
        .filter(|s| homes::home_of_shard(*s, threads) == home)
        .fold(0, |m, s| m | (1 << s))
}

/// A key of the partition state: its shard is its pid's.
pub(crate) trait PidKey {
    fn pid(&self) -> Pid;
}

impl PidKey for Pid {
    fn pid(&self) -> Pid {
        *self
    }
}

pub(crate) type PartsMap = HashMap<Pid, OverlayPart, FxBuild>;

/// A map split into [`PART_SHARDS`] maps by pid and [`NEW_SHARD`]'s, inline,
/// so the empty overlay of a group costs no allocation. A key always lives in
/// its pid's shard, so two of them compare shard by shard.
#[derive(Clone, Debug)]
pub(crate) struct Shards<K, V> {
    maps: [HashMap<K, V, FxBuild>; PART_SHARDS + 1],
    /// Where a pid's rows live ([`homes::Homes`]): its name's shard. `None`
    /// (the single planner, a test's overlay): `pid % PART_SHARDS`.
    homes: Option<std::sync::Arc<homes::Homes>>,
}

impl<K, V> Default for Shards<K, V> {
    fn default() -> Self {
        Shards {
            maps: std::array::from_fn(|_| HashMap::default()),
            homes: None,
        }
    }
}

impl<K, V> Shards<K, V> {
    /// The shard of `pid`'s rows: [`NEW_SHARD`] for a placeholder (a partition
    /// a planning thread created this cycle, not numbered yet).
    pub(crate) fn shard_of(&self, pid: Pid) -> usize {
        if is_placeholder(pid) {
            return NEW_SHARD;
        }
        match &self.homes {
            Some(h) => h.shard_of(pid),
            None => (pid % PART_SHARDS as u64) as usize,
        }
    }
}

impl<K: PidKey + Eq + std::hash::Hash, V> Shards<K, V> {
    pub(crate) fn get(&self, k: &K) -> Option<&V> {
        self.maps[self.shard_of(k.pid())].get(k)
    }

    pub(crate) fn get_mut(&mut self, k: &K) -> Option<&mut V> {
        self.maps[self.shard_of(k.pid())].get_mut(k)
    }

    pub(crate) fn contains_key(&self, k: &K) -> bool {
        self.maps[self.shard_of(k.pid())].contains_key(k)
    }

    pub(crate) fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.maps[self.shard_of(k.pid())].insert(k, v)
    }

    pub(crate) fn remove(&mut self, k: &K) -> Option<V> {
        self.maps[self.shard_of(k.pid())].remove(k)
    }

    pub(crate) fn entry(&mut self, k: K) -> std::collections::hash_map::Entry<'_, K, V> {
        self.maps[self.shard_of(k.pid())].entry(k)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.maps.iter().flat_map(|m| m.iter())
    }

    pub(crate) fn len(&self) -> usize {
        self.maps.iter().map(HashMap::len).sum()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.maps.iter().all(HashMap::is_empty)
    }

    pub(crate) fn retain(&mut self, mut f: impl FnMut(&K, &mut V) -> bool) {
        for m in &mut self.maps {
            m.retain(&mut f);
        }
    }

    /// Shard `s` itself.
    pub(crate) fn shard(&self, s: usize) -> &HashMap<K, V, FxBuild> {
        &self.maps[s]
    }

    pub(crate) fn shard_mut(&mut self, s: usize) -> &mut HashMap<K, V, FxBuild> {
        &mut self.maps[s]
    }
}

/// One shard of an overlay's partition state, moved out for a planning thread
/// to work on ([`Overlay::take_shards`]).
#[derive(Default)]
pub(crate) struct PartShard {
    pub(crate) parts: PartsMap,
}

impl PartShard {
    pub(crate) fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }
}

/// What a shard's merge could not keep: the rows of partitions created this
/// cycle, still under a placeholder whose shard is not their real pid's. The
/// cycle thread files them once they are numbered ([`Overlay::file_spill`]).
#[derive(Default)]
pub(crate) struct Spill {
    parts: Vec<(Pid, OverlayPart)>,
}

fn merge_part(mine: &mut OverlayPart, part: OverlayPart) {
    if part.created.is_some() {
        mine.created = part.created;
    }
    if part.watermark.is_some() {
        mine.watermark = part.watermark;
    }
    mine.appends.extend(part.appends);
}

/// A two-level lookup's answer: the key's value in a layer, a mask in a
/// layer that hides it (a delete, a purge), or nothing in any layer.
enum Layered<T> {
    Found(T),
    Masked,
    Absent,
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
            parts: Shards::default(),
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
            base: None,
            shards_written: 0,
            allowed: None,
            strayed: Stray::default(),
            owner: false,
            frames: Vec::new(),
        }
    }

    /// Give this overlay its homes ([`homes::Homes`]): from now on a pid's rows
    /// live in its name's shard. Only for an overlay that holds no partition
    /// state yet: a row already filed under `pid % PART_SHARDS` would be lost.
    pub(crate) fn set_homes(&mut self, homes: Option<std::sync::Arc<homes::Homes>>) {
        debug_assert!(
            self.parts.is_empty(),
            "an overlay takes its homes before it holds partition state"
        );
        self.parts.homes = homes;
    }

    /// The homes of this overlay, if it has any.
    pub(crate) fn homes(&self) -> Option<&std::sync::Arc<homes::Homes>> {
        self.parts.homes.as_ref()
    }

    /// The shard of `pid`'s partition state in this overlay.
    pub(crate) fn shard_of(&self, pid: Pid) -> usize {
        self.parts.shard_of(pid)
    }

    /// `pid`'s shard as a mask: none for a placeholder's.
    pub(crate) fn shard_bit(&self, pid: Pid) -> ShardMask {
        if is_placeholder(pid) {
            0
        } else {
            1 << self.parts.shard_of(pid)
        }
    }

    /// `pid` is `tenant`'s partition `partition`: its shard is its name's from
    /// now on, unless the pid has one already ([`homes::Homes::note_name`]).
    /// Called wherever the planner learns a pid's name, BEFORE it reads or
    /// writes that pid's rows here.
    pub(crate) fn note_home(&self, pid: Pid, tenant: &str, partition: &str) {
        if is_placeholder(pid) {
            return;
        }
        if let Some(h) = &self.parts.homes {
            h.note_name(pid, tenant, partition);
        }
    }

    /// Group `group`'s overlay over `base`, the cycle's kept overlay
    /// (the footprint scheduler): empty, reading through to `base`, its new
    /// partition ids and KV versions placeholders of its own.
    pub(crate) fn layered(base: std::sync::Arc<Overlay>, group: u32) -> Overlay {
        let ph = PLACEHOLDER | (u64::from(group) << 32);
        let mut ov = Overlay::new(ph, ph);
        ov.cycle_pid_base = base.cycle_pid_base;
        ov.cycle_kv_base = base.cycle_kv_base;
        ov.max_now_us = base.max_now_us;
        ov.max_created_at_us = base.max_created_at_us;
        ov.record_ids = false;
        ov.tag = base.tag;
        ov.set_homes(base.homes().cloned());
        ov.base = Some(base);
        ov
    }

    /// How many partition ids and KV versions a group's overlay drew from its
    /// placeholder counters ([`Overlay::layered`]).
    pub(crate) fn drawn(&self) -> (u64, u64) {
        (
            self.next_pid & PLACEHOLDER_ORDINAL,
            self.next_kv_version & PLACEHOLDER_ORDINAL,
        )
    }

    /// Raise the running counters to `pid` and `kv`: what an entry handed out
    /// that no row of this overlay shows (a version a later delete of the same
    /// key replaced still counts, I18).
    pub(crate) fn raise_counters(&mut self, pid: u64, kv: u64) {
        self.next_pid = self.next_pid.max(pid);
        self.next_kv_version = self.next_kv_version.max(kv);
    }

    /// Let go of the base a group's overlay reads through, so the cycle can
    /// take the base back whole once every group is done.
    pub(crate) fn detach(&mut self) {
        self.base = None;
    }

    /// Whether this group's overlay can be [`Overlay::absorb`]ed: it holds no
    /// sweep (a delete or a purge only a fold applies to the base) and no
    /// request id.
    pub(crate) fn absorbable(&self) -> bool {
        self.dropped_queues.is_empty()
            && self.purged_tenants.is_empty()
            && self.gone.is_empty()
            && self.request_ids.is_empty()
    }

    /// Move the catalog of a home's overlay into this one, the overlay it was
    /// layered over (the scheduler's homes): what folding the home's logged
    /// effects here would leave, without folding them again. The homes of a
    /// cycle touch disjoint keys, so a key is the home's alone and it wins, as
    /// the later fold would. The home's partition state is not moved here: it
    /// was written in place, in the shards the home owned
    /// ([`Overlay::own`]), and the partitions it created are filed apart
    /// ([`Overlay::file_new`]). `pid` and `kv` give each placeholder its number
    /// in the entry; `false` when one has none, and then this overlay is not
    /// the fold of its entries.
    pub(crate) fn absorb_catalog(
        &mut self,
        g: &mut Overlay,
        pid: &impl Fn(u64) -> Option<u64>,
        kv: &impl Fn(u64) -> Option<u64>,
    ) -> bool {
        let mut ok = true;
        let mut real = |n: u64, f: &dyn Fn(u64) -> Option<u64>| -> u64 {
            if !is_placeholder(n) {
                return n;
            }
            f(n).unwrap_or_else(|| {
                ok = false;
                n
            })
        };
        g.base = None;
        self.queues.extend(g.queues.drain());
        self.groups.extend(g.groups.drain());
        for (k, mut v) in g.pids_by_key.drain() {
            v.v = real(v.v, pid);
            self.pids_by_key.insert(k, v);
        }
        for (k, mut v) in g.kv.drain() {
            if let Some(row) = &mut v.v {
                row.version = real(row.version, kv);
                self.next_kv_version = self.next_kv_version.max(row.version.saturating_add(1));
            }
            self.kv.insert(k, v);
        }
        self.timers.extend(g.timers.drain());
        self.max_now_us = self.max_now_us.max(g.max_now_us);
        self.max_created_at_us = self.max_created_at_us.max(g.max_created_at_us);
        self.folds += g.folds;
        g.folds = 0;
        ok
    }

    /// File what the shard merges spilled: the rows of partitions created this
    /// cycle, under their numbers in the entry (`pid`). `false` when one has
    /// none.
    pub(crate) fn file_spill(&mut self, spill: Spill, pid: &impl Fn(u64) -> Option<u64>) -> bool {
        let mut ok = true;
        for (p, part) in spill.parts {
            let Some(real) = pid(p) else {
                ok = false;
                continue;
            };
            if let Some(c) = &part.created {
                self.next_pid = self.next_pid.max(real.saturating_add(1));
                // Its name gives its shard, before any row of it is filed.
                self.note_home(real, &c.v.tenant, &c.v.partition);
            }
            merge_part(self.parts.entry(real).or_default(), part);
        }
        ok
    }

    fn take_shard(&mut self, s: usize) -> PartShard {
        PartShard {
            parts: std::mem::take(self.parts.shard_mut(s)),
        }
    }

    /// Put shard `s` back where it was taken from, which holds nothing.
    fn put_shard(&mut self, s: usize, sh: PartShard) {
        debug_assert!(
            self.parts.shard(s).is_empty(),
            "a shard comes back over one that holds state"
        );
        *self.parts.shard_mut(s) = sh.parts;
    }

    /// The shards of `mask`, moved out with their numbers: what a planning
    /// thread owns for a run ([`Overlay::own`]). [`Overlay::put_shards`] puts
    /// them back.
    pub(crate) fn take_shards(&mut self, mask: ShardMask) -> Vec<(usize, PartShard)> {
        let mut out = Vec::with_capacity(mask.count_ones() as usize);
        let mut m = mask;
        while m != 0 {
            let s = m.trailing_zeros() as usize;
            m &= m - 1;
            out.push((s, self.take_shard(s)));
        }
        out
    }

    pub(crate) fn put_shards(&mut self, shards: Vec<(usize, PartShard)>) {
        for (s, sh) in shards {
            self.put_shard(s, sh);
        }
    }

    /// Own `shards` for a run: a planning thread's overlay, layered over the
    /// frozen kept overlay, which holds nothing of them meanwhile. Its folds
    /// write them in place and keep what undoes them, and a pid of any other
    /// shard is outside it.
    pub(crate) fn own(&mut self, shards: Vec<(usize, PartShard)>, mask: ShardMask) {
        self.put_shards(shards);
        self.allowed = Some(mask);
        self.owner = true;
        self.frames.push(Undo::default());
    }

    /// Read and fold only the pids of `mask`'s shards: a group's overlay
    /// planned beside the planning threads that own the others.
    pub(crate) fn allow(&mut self, mask: ShardMask) {
        self.allowed = Some(mask);
    }

    /// Whether it read or folded a pid outside its shards.
    pub(crate) fn strayed(&self) -> bool {
        self.strayed.get()
    }

    /// The shards it owned, moved back out, and what undoes its folds in
    /// them: nothing for an overlay that owned none.
    pub(crate) fn give_back(&mut self) -> (Vec<(usize, PartShard)>, Option<Undo>) {
        let (true, Some(mask)) = (self.owner, self.allowed) else {
            return (Vec::new(), None);
        };
        assert_eq!(self.frames.len(), 1, "a savepoint was left open");
        self.owner = false;
        let undo = self.frames.pop();
        (self.take_shards(mask), undo)
    }

    /// Take back what the folds that `u` recorded did to the partition state:
    /// each partition as the run's first fold found it.
    pub(crate) fn undo(&mut self, u: &Undo) {
        for was in &u.parts {
            let Some(p) = self.parts.get_mut(&was.pid) else {
                continue;
            };
            // The appends since go, and their frames' hashes with them.
            p.appends.truncate(was.appends.min(p.appends.len()));
            p.created = was.created.clone();
            p.watermark = was.watermark.clone();
            if p.is_empty() {
                self.parts.remove(&was.pid);
            }
        }
    }

    /// A transaction's savepoint: its small maps and counters, copied, and an
    /// undo frame for the partition state. [`Overlay::restore`] takes the
    /// transaction back out; [`Overlay::commit`] keeps it.
    pub(crate) fn save(&mut self) -> Saved {
        self.frames.push(Undo::default());
        Saved {
            next_pid: self.next_pid,
            next_kv_version: self.next_kv_version,
            max_now_us: self.max_now_us,
            max_created_at_us: self.max_created_at_us,
            folds: self.folds,
            shards_written: self.shards_written,
            queues: self.queues.clone(),
            groups: self.groups.clone(),
            pids_by_key: self.pids_by_key.clone(),
            kv: self.kv.clone(),
            timers: self.timers.clone(),
            gone: self.gone.clone(),
            dropped_queues: self.dropped_queues.clone(),
            purged_tenants: self.purged_tenants.clone(),
        }
    }

    /// Back to `s`: what the transaction folded since is gone.
    pub(crate) fn restore(&mut self, s: Saved) {
        let frame = self.frames.pop().expect("a savepoint's frame");
        self.undo(&frame);
        self.reset_to(s);
    }

    /// The small maps and the counters as `s` holds them.
    fn reset_to(&mut self, s: Saved) {
        self.next_pid = s.next_pid;
        self.next_kv_version = s.next_kv_version;
        self.max_now_us = s.max_now_us;
        self.max_created_at_us = s.max_created_at_us;
        self.folds = s.folds;
        self.shards_written = s.shards_written;
        self.queues = s.queues;
        self.groups = s.groups;
        self.pids_by_key = s.pids_by_key;
        self.kv = s.kv;
        self.timers = s.timers;
        self.gone = s.gone;
        self.dropped_queues = s.dropped_queues;
        self.purged_tenants = s.purged_tenants;
    }

    /// Keep what the transaction folded since `s`. The frame under it, if
    /// any, takes the partitions it had not seen yet, as the transaction found
    /// them: that is how they were when it began.
    pub(crate) fn commit(&mut self, s: Saved) {
        drop(s);
        let frame = self.frames.pop().expect("a savepoint's frame");
        if let Some(under) = self.frames.last_mut() {
            for was in frame.parts {
                if under.seen.insert(was.pid) {
                    under.parts.push(was);
                }
            }
        }
    }

    /// Keep what was folded since `s`, as [`Overlay::commit`] does, and hand
    /// back what still takes it out: a planning thread's second round, which
    /// stays or goes once every thread has voted (the scheduler, S8). By then
    /// the shards this overlay owned are back in the overlay it was layered
    /// over, so taking the round out is done in two places
    /// ([`Overlay::unpark`]).
    pub(crate) fn park(&mut self, s: Saved) -> Parked {
        let frame = self.frames.pop().expect("a savepoint's frame");
        if let Some(under) = self.frames.last_mut() {
            for was in &frame.parts {
                if under.seen.insert(was.pid) {
                    under.parts.push(was.clone());
                }
            }
        }
        Parked { saved: s, frame }
    }

    /// Back to where `p` was taken ([`Overlay::park`]): this overlay's own maps
    /// and counters, and the partition state it still holds (the partitions it
    /// created). The frame this returns takes the rest back out of the overlay
    /// that holds the shards now ([`Overlay::undo`]).
    pub(crate) fn unpark(&mut self, p: Parked) -> Undo {
        self.undo(&p.frame);
        self.reset_to(p.saved);
        p.frame
    }

    /// Mark the overlay strayed when `pid` is outside the shards it may touch.
    fn check(&self, pid: Pid) {
        if let Some(m) = self.allowed {
            if !is_placeholder(pid) && m & self.shard_bit(pid) == 0 {
                self.strayed.set();
            }
        }
    }

    /// Record `pid`'s partition as the innermost frame first finds it.
    fn note_touch(&mut self, pid: Pid) {
        let Some(u) = self.frames.last_mut() else {
            return;
        };
        if !u.seen.insert(pid) {
            return;
        }
        let p = self.parts.get(&pid);
        u.parts.push(PartWas {
            pid,
            appends: p.map_or(0, |p| p.appends.len()),
            created: p.and_then(|p| p.created.clone()),
            watermark: p.and_then(|p| p.watermark.clone()),
        });
    }

    /// File the partitions a group created ([`NEW_SHARD`]) under their
    /// numbers in the entry (`pid`). `false` when one has none.
    pub(crate) fn file_new(&mut self, sh: PartShard, pid: &impl Fn(u64) -> Option<u64>) -> bool {
        self.file_spill(
            Spill {
                parts: sh.parts.into_iter().collect(),
            },
            pid,
        )
    }

    /// The shards this overlay's own folds wrote, moved out with their
    /// numbers: a push group's overlay holds one. The merge of a cycle's groups
    /// takes these instead of all [`PART_SHARDS`].
    pub(crate) fn take_written_shards(&mut self) -> Vec<(usize, PartShard)> {
        let mut out = Vec::with_capacity(self.shards_written.count_ones() as usize);
        for s in 0..=NEW_SHARD {
            if self.shards_written & (1u128 << s) != 0 {
                out.push((s, self.take_shard(s)));
            }
        }
        self.shards_written = 0;
        out
    }

    fn base(&self) -> Option<&Overlay> {
        self.base.as_deref()
    }

    /// This layer's own mask over the queue's names and groups.
    fn queue_dropped_here(&self, tenant: &str, queue: &str) -> bool {
        if self.dropped_queues.is_empty() && self.purged_tenants.is_empty() {
            return false;
        }
        self.purged_tenants.contains_key(tenant)
            || self
                .dropped_queues
                .contains_key(&(tenant.to_string(), queue.to_string()))
    }

    /// This layer's own mask over the tenant's queues, KV rows and timers.
    fn tenant_purged_here(&self, tenant: &str) -> bool {
        !self.purged_tenants.is_empty() && self.purged_tenants.contains_key(tenant)
    }

    fn queue_lookup(&self, tenant: &str, queue: &str) -> Layered<&Option<QueueConfig>> {
        if let Some(t) = self.queues.get(&(tenant.to_string(), queue.to_string())) {
            return Layered::Found(&t.v);
        }
        if self.tenant_purged_here(tenant) {
            return Layered::Masked;
        }
        match self.base() {
            Some(b) => b.queue_lookup(tenant, queue),
            None => Layered::Absent,
        }
    }

    fn group_lookup(&self, tenant: &str, queue: &str, group: &str) -> Layered<&Option<GroupRow>> {
        if let Some(t) =
            self.groups
                .get(&(tenant.to_string(), queue.to_string(), group.to_string()))
        {
            return Layered::Found(&t.v);
        }
        if self.queue_dropped_here(tenant, queue) {
            return Layered::Masked;
        }
        match self.base() {
            Some(b) => b.group_lookup(tenant, queue, group),
            None => Layered::Absent,
        }
    }

    fn pid_lookup(&self, tenant: &str, queue: &str, partition: &str) -> Layered<Pid> {
        if !self.pids_by_key.is_empty() {
            if let Some(t) = self.pids_by_key.get(&(
                tenant.to_string(),
                queue.to_string(),
                partition.to_string(),
            )) {
                return Layered::Found(t.v);
            }
        }
        if self.queue_dropped_here(tenant, queue) {
            return Layered::Masked;
        }
        match self.base() {
            Some(b) => b.pid_lookup(tenant, queue, partition),
            None => Layered::Absent,
        }
    }

    fn kv_lookup(
        &self,
        tenant: &str,
        ns: &str,
        key: &str,
    ) -> Layered<&Option<crate::rsm::store::rows::KvRow>> {
        if !self.kv.is_empty() {
            if let Some(t) = self
                .kv
                .get(&(tenant.to_string(), ns.to_string(), key.to_string()))
            {
                return Layered::Found(&t.v);
            }
        }
        if self.tenant_purged_here(tenant) {
            return Layered::Masked;
        }
        match self.base() {
            Some(b) => b.kv_lookup(tenant, ns, key),
            None => Layered::Absent,
        }
    }

    fn timer_lookup(&self, tk: &(String, String, String)) -> Layered<&TimerSlot> {
        if let Some(s) = self.timers.get(tk) {
            return Layered::Found(s);
        }
        if self.tenant_purged_here(&tk.0) {
            return Layered::Masked;
        }
        match self.base() {
            Some(b) => b.timer_lookup(tk),
            None => Layered::Absent,
        }
    }

    /// The create in flight of `pid`, in any layer.
    fn created_of(&self, pid: Pid) -> Option<&CreatedPart> {
        self.check(pid);
        self.parts
            .get(&pid)
            .and_then(|o| o.created.as_ref().map(|t| &t.v))
            .or_else(|| self.base()?.created_of(pid))
    }

    /// The last append in flight to `pid`: this layer's own, else the base's.
    fn last_append(&self, pid: Pid) -> Option<&OverlayAppend> {
        self.check(pid);
        self.parts
            .get(&pid)
            .and_then(|o| o.appends.last())
            .or_else(|| self.base()?.last_append(pid))
    }

    /// Every append in flight to `pid`, oldest first: the base's, then this
    /// layer's own.
    fn appends_of(&self, pid: Pid) -> Vec<&OverlayAppend> {
        self.check(pid);
        let mut out = self.base().map(|b| b.appends_of(pid)).unwrap_or_default();
        if let Some(o) = self.parts.get(&pid) {
            out.extend(o.appends.iter());
        }
        out
    }

    /// Every partition created in flight, in every layer, with its names.
    fn created_parts(&self) -> Vec<(Pid, &CreatedPart)> {
        // Every shard: an overlay that may touch only some cannot answer.
        if self.allowed.is_some() {
            self.strayed.set();
        }
        let mut out = self.base().map(|b| b.created_parts()).unwrap_or_default();
        out.extend(
            self.parts
                .iter()
                .filter_map(|(pid, p)| p.created.as_ref().map(|c| (*pid, &c.v))),
        );
        out
    }

    /// Every partition name in flight, in every layer: `(tenant, queue,
    /// partition) → pid`, this layer's own winning, the base's masked by this
    /// layer's deletes.
    fn names_in_flight(&self) -> Vec<((&str, &str, &str), Pid)> {
        let mut out: Vec<((&str, &str, &str), Pid)> = Vec::new();
        if let Some(b) = self.base() {
            for (k, pid) in b.names_in_flight() {
                if !self.queue_dropped_here(k.0, k.1)
                    && !self.pids_by_key.contains_key(&(
                        k.0.to_string(),
                        k.1.to_string(),
                        k.2.to_string(),
                    ))
                {
                    out.push((k, pid));
                }
            }
        }
        out.extend(
            self.pids_by_key
                .iter()
                .map(|((t, q, p), v)| ((t.as_str(), q.as_str(), p.as_str()), v.v)),
        );
        out
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
        self.request_ids
            .get(id)
            .map(|t| t.v.outcome())
            .or_else(|| self.base()?.request_id(id))
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
                // The partition's name gives its shard, before any row of it.
                self.note_home(*pid, tenant, partition);
                self.check(*pid);
                self.note_touch(*pid);
                self.shards_written |= 1u128 << self.parts.shard_of(*pid);
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
                self.check(*pid);
                self.note_touch(*pid);
                self.shards_written |= 1u128 << self.parts.shard_of(*pid);
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
                rows,
            } => {
                self.check(*pid);
                self.note_touch(*pid);
                self.shards_written |= 1u128 << self.parts.shard_of(*pid);
                self.parts.entry(*pid).or_default().watermark = Some(Tagged {
                    v: (*log_start, *txns_start, rows.map(|m| m.rows_start)),
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
        (!self.gone.is_empty() && self.gone.contains_key(&pid))
            || self.base().is_some_and(|b| b.is_gone(pid))
    }

    /// Whether a delete in flight dropped the queue's names and groups: its
    /// own `QueueDelete`, or a purge of its tenant.
    fn queue_dropped(&self, tenant: &str, queue: &str) -> bool {
        self.queue_dropped_here(tenant, queue)
            || self.base().is_some_and(|b| b.queue_dropped(tenant, queue))
    }

    /// Whether a purge of the tenant is in flight: its committed KV rows and
    /// timers are dead already.
    pub(crate) fn tenant_purged(&self, tenant: &str) -> bool {
        self.tenant_purged_here(tenant) || self.base().is_some_and(|b| b.tenant_purged(tenant))
    }

    /// Whether anything in flight (or planned this cycle) names the partition:
    /// an append, a create, a watermark. The leader's retention reads only
    /// committed state, and must not delete a partition a push just wrote.
    pub(crate) fn touches_partition(&self, pid: Pid) -> bool {
        self.check(pid);
        self.parts.contains_key(&pid) || self.base().is_some_and(|b| b.touches_partition(pid))
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
    ) -> Option<Option<(u64, u64, Option<u64>)>> {
        if self.is_gone(pid) || self.queue_dropped(tenant, queue) {
            return None;
        }
        Some(self.watermark_of(pid))
    }

    /// The watermark in flight of `pid`: this layer's own, else the base's.
    fn watermark_of(&self, pid: Pid) -> Option<(u64, u64, Option<u64>)> {
        self.check(pid);
        self.parts
            .get(&pid)
            .and_then(|o| o.watermark.as_ref().map(|t| t.v))
            .or_else(|| self.base()?.watermark_of(pid))
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
                // A group's overlay patches the base's slot as its own copy.
                let prior = match self.timers.remove(&k) {
                    Some(s) => Some(s),
                    None if !self.tenant_purged_here(tenant) => match self.base() {
                        Some(b) => match b.timer_lookup(&k) {
                            Layered::Found(s) => Some(s.clone()),
                            _ => None,
                        },
                        None => None,
                    },
                    None => None,
                };
                let next = match prior {
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
    /// The keys this planner's merged reads touched, when asked
    /// ([`Planner::record_touches`]): the footprint scheduler checks that no
    /// other group of the cycle wrote one of them.
    touches: Option<RefCell<Vec<u64>>>,
    /// The first partition id no committed entry had when the cycle began
    /// ([`Planner::set_base_pid`]): a pid below it was committed before the
    /// cycle, so no entry in flight creates it. `None`: not told.
    base_pid: Option<u64>,
}

/// The keys the footprint scheduler (`rsm/batcher_scheduler.rs`) groups and
/// checks commands on, each hashed to one `u64`, so recording one costs no
/// allocation. Two keys that hash alike only ever make the scheduler more
/// careful: it joins two commands it need not join, or plans a cycle on one
/// thread. A real conflict always hashes alike.
pub(crate) mod footprint {
    use std::hash::Hasher;

    use crate::rsm::effect::Pid;

    /// A queue's row: its settings, its route, its groups.
    pub(crate) fn queue(t: &str, q: &str) -> u64 {
        h(1, &[t.as_bytes(), q.as_bytes()])
    }

    /// A partition by name: which pid it is, so whether a push creates it.
    pub(crate) fn part(t: &str, q: &str, p: &str) -> u64 {
        h(2, &[t.as_bytes(), q.as_bytes(), p.as_bytes()])
    }

    /// Every partition of a queue, declared by a push that a reader re-keys
    /// from the payload: every `part` of the queue reads it.
    pub(crate) fn any_part(t: &str, q: &str) -> u64 {
        h(3, &[t.as_bytes(), q.as_bytes()])
    }

    /// A partition's log by pid: its appends, its dedup window, its watermark.
    pub(crate) fn log(pid: Pid) -> u64 {
        h(4, &[&pid.to_le_bytes()])
    }

    /// A pid's names and that it still exists: what a create or a delete
    /// changes.
    pub(crate) fn names(pid: Pid) -> u64 {
        h(5, &[&pid.to_le_bytes()])
    }

    /// One group's cursor on one partition.
    pub(crate) fn cursor(pid: Pid, group: &str) -> u64 {
        h(6, &[&pid.to_le_bytes(), group.as_bytes()])
    }

    /// A partition's dead letters.
    pub(crate) fn dlq(pid: Pid) -> u64 {
        h(7, &[&pid.to_le_bytes()])
    }

    /// One dead letter, by id (a replay deletes it).
    pub(crate) fn dlq_row(id: &[u8; 16]) -> u64 {
        h(8, &[id])
    }

    pub(crate) fn kv(t: &str, ns: &str, k: &str) -> u64 {
        h(9, &[t.as_bytes(), ns.as_bytes(), k.as_bytes()])
    }

    pub(crate) fn timer(t: &str, q: &str, k: &str) -> u64 {
        h(10, &[t.as_bytes(), q.as_bytes(), k.as_bytes()])
    }

    /// A streams query's row.
    pub(crate) fn streams_query(query: &[u8; 16]) -> u64 {
        h(11, &[query])
    }

    /// One streams state cell.
    pub(crate) fn streams_state(query: &[u8; 16], pid: Pid, key: &str) -> u64 {
        h(12, &[query, &pid.to_le_bytes(), key.as_bytes()])
    }

    fn h(kind: u8, parts: &[&[u8]]) -> u64 {
        let mut s = crate::rsm::fasthash::FxHasher::default();
        s.write_u8(kind);
        for p in parts {
            s.write_usize(p.len());
            s.write(p);
        }
        s.finish()
    }
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
            touches: None,
            base_pid: None,
        }
    }

    /// Tell the planner the committed partition counter the cycle read when it
    /// began, before it read anything else: [`Planner::partition_exists`] then
    /// knows which partitions only an entry in flight can have created.
    pub(crate) fn set_base_pid(&mut self, base_pid: u64) -> &mut Self {
        self.base_pid = Some(base_pid);
        self
    }

    /// Record every key the merged reads touch from now on
    /// ([`Planner::take_touches`]).
    pub(crate) fn record_touches(&mut self) {
        self.touches = Some(RefCell::new(Vec::new()));
    }

    /// The keys touched since the last take.
    pub(crate) fn take_touches(&self) -> Vec<u64> {
        self.touches
            .as_ref()
            .map(|t| std::mem::take(&mut *t.borrow_mut()))
            .unwrap_or_default()
    }

    fn touched(&self, key: impl FnOnce() -> u64) {
        if let Some(t) = &self.touches {
            t.borrow_mut().push(key());
        }
    }

    /// `f` with nothing it reads recorded ([`Planner::record_touches`]): the
    /// caller records the weaker key it really depends on.
    fn quietly<T>(&self, f: impl FnOnce() -> T) -> T {
        let saved = self
            .touches
            .as_ref()
            .map(|t| std::mem::take(&mut *t.borrow_mut()));
        let out = f();
        if let (Some(t), Some(saved)) = (&self.touches, saved) {
            *t.borrow_mut() = saved;
        }
        out
    }

    /// The tenant, queue and partition names of `pid`: the overlay's create in
    /// flight, else the committed row. `None` when the partition is unknown.
    pub(crate) fn partition_key(
        &self,
        ov: &Overlay,
        pid: Pid,
    ) -> Result<Option<(String, String, String)>, Refusal> {
        if let Some(c) = ov.created_of(pid) {
            return Ok(Some((
                c.tenant.clone(),
                c.queue.clone(),
                c.partition.clone(),
            )));
        }
        if is_placeholder(pid) {
            return Ok(None);
        }
        self.reads()
            .partition_with(pid, |p| {
                (
                    p.tenant.to_string(),
                    p.queue.to_string(),
                    p.partition.to_string(),
                )
            })
            .map_err(store_err)
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
        // A key that would pass the store's limit is refused here: in apply it
        // is `KeyTooLong`, which stops every node on the entry. So is a command
        // too large for one entry (admin routes carry one effect per row).
        self.check_effect_keys(ov, &c.effects)?;
        self.check_planned_size(&c.effects)?;
        self.check_partition_rows(ov, &c.effects)?;
        let mut effects = c.effects.clone();
        self.cover_dropped_partitions(ov, &mut effects)?;
        // Routing's other side of a catalog change (route.rs).
        self.complete_routes(ov, &mut effects)?;
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
    /// Every store key `effects` make apply write or delete must fit the
    /// store: apply refuses a longer one DETERMINISTICALLY, and that stops
    /// every node on the entry, again after a restart ([`keycheck`]). Refused
    /// here as the client's error instead, before anything folds.
    ///
    /// A cursor's group counter also holds its partition's tenant and queue.
    /// Every queue's tenant and queue were budgeted when it was created
    /// (`check_message_key_names`, at most `NAME_BUDGET_BYTES` together), so
    /// the names are read only for a group long enough to overflow even then:
    /// an ordinary ack, and a seek of an ordinary group, pay no read.
    pub(crate) fn check_effect_keys(
        &self,
        ov: &Overlay,
        effects: &[Effect],
    ) -> Result<(), Refusal> {
        let max = self.reads().max_key_len();
        // counter_group = 1 + tenant+2 + queue+2 + group+2 + 2.
        let group_safe =
            |g: &str| keycheck::name_len(g) + 3 + crate::rsm::facade::NAME_BUDGET_BYTES + 4 <= max;
        for e in effects {
            match e {
                Effect::TraceAppend { event } | Effect::TraceRecord { event } => {
                    crate::rsm::traces::check_keys(event, max)
                        .map_err(|why| Refusal::client("name_too_long", why))?;
                }
                Effect::CursorSet { pid, group, .. } if !group_safe(group) => {
                    if let Some((tenant, queue)) = self.partition_names(ov, *pid)? {
                        let len = keycheck::counter_group_len(&tenant, &queue, group);
                        if len > max {
                            return Err(Refusal::client(
                                "name_too_long",
                                keycheck::Overlong {
                                    key_len: len,
                                    field: "consumerGroup",
                                    field_len: group.len(),
                                }
                                .message(max),
                            ));
                        }
                    }
                }
                _ => {}
            }
            if let Some(o) = keycheck::overlong(e, max) {
                return Err(Refusal::client("name_too_long", o.message(max)));
            }
        }
        Ok(())
    }

    /// A command must fit one entry. The push, KV, positions and timers
    /// planners measure theirs; an Effects command (an admin delete, a seek, a
    /// streams reset) carries one effect per row and is measured here: an
    /// entry over a follower's frame limit never replicates, and one over the
    /// codec limit stops the leader's log writer. The routes that make many
    /// effects send them in chunks well under this.
    pub(crate) fn check_planned_size(&self, effects: &[Effect]) -> Result<(), Refusal> {
        let planned = keycheck::planned_bytes(effects);
        if planned > self.cfg.entry_max_bytes {
            return Err(Refusal::client(
                "too_large",
                format!(
                    "planned command of {planned} B ({} effects) exceeds \
                     QUEEN_RAFT_ENTRY_MAX_BYTES ({})",
                    effects.len(),
                    self.cfg.entry_max_bytes
                ),
            ));
        }
        Ok(())
    }

    /// The tenant and queue of `pid`, from the overlay when it is created in
    /// flight, else from committed state. `None` when the partition is unknown
    /// (the caller's other checks refuse that case).
    pub(crate) fn partition_names(
        &self,
        ov: &Overlay,
        pid: Pid,
    ) -> Result<Option<(String, String)>, Refusal> {
        self.touched(|| footprint::names(pid));
        if let Some(c) = ov.created_of(pid) {
            return Ok(Some((c.tenant.clone(), c.queue.clone())));
        }
        self.reads()
            .partition_with(pid, |p| (p.tenant.to_string(), p.queue.to_string()))
            .map_err(store_err)
    }

    pub(crate) fn check_partition_rows(
        &self,
        ov: &Overlay,
        effects: &[Effect],
    ) -> Result<(), Refusal> {
        for e in effects {
            if let Effect::CursorSet { pid, .. } | Effect::DlqInsert { pid, .. } = e {
                // A cursor or a dead letter needs its partition to exist, not
                // its head: the footprint scheduler counts this read as one of
                // the partition's names, which only a delete changes.
                if !self.quietly(|| self.partition_exists(ov, *pid))? {
                    self.touched(|| footprint::names(*pid));
                    return Err(Refusal::retry(
                        "partition_gone",
                        format!("partition {pid} is gone or being deleted"),
                    ));
                }
                self.touched(|| footprint::names(*pid));
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
        for (pid, c) in ov.created_parts() {
            if c.tenant == tenant && queue.is_none_or(|q| c.queue == q) && !ov.is_gone(pid) {
                pids.insert(pid);
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
        if let Some(v) = self.cfg.cycle_cluster_version {
            return Ok(v);
        }
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
    pub(crate) fn queue_cfg(
        &self,
        ov: &Overlay,
        tenant: &str,
        queue: &str,
    ) -> Result<Option<QueueConfig>, Refusal> {
        self.touched(|| footprint::queue(tenant, queue));
        match ov.queue_lookup(tenant, queue) {
            Layered::Found(v) => Ok(v.clone()),
            Layered::Masked => Ok(None),
            Layered::Absent => self.committed.queue(tenant, queue).map_err(store_err),
        }
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
        self.touched(|| footprint::queue(tenant, queue));
        match ov.group_lookup(tenant, queue, group) {
            Layered::Found(v) => Ok(v.clone()),
            Layered::Masked => Ok(None),
            Layered::Absent => self.reads().group(tenant, queue, group).map_err(store_err),
        }
    }

    /// The live pid of a partition NAME. A name a delete in flight sweeps
    /// resolves to nothing, so a push re-creates the partition with a new pid
    /// instead of appending to one that is going away.
    pub(crate) fn pid_of(
        &self,
        ov: &Overlay,
        tenant: &str,
        queue: &str,
        partition: &str,
    ) -> Result<Option<Pid>, Refusal> {
        self.touched(|| footprint::part(tenant, queue, partition));
        let pid = match ov.pid_lookup(tenant, queue, partition) {
            Layered::Found(p) => Some(p),
            Layered::Masked => None,
            Layered::Absent => self
                .committed
                .pid_of(tenant, queue, partition)
                .map_err(store_err)?,
        };
        // The name gives the pid its shard (`homes`), before anything reads
        // or writes the pid's rows in the overlay.
        if let Some(p) = pid {
            ov.note_home(p, tenant, partition);
        }
        Ok(pid.filter(|p| !ov.is_gone(*p)))
    }

    /// The merged partition view, or `None` when the pid is unknown, garbage,
    /// or taken away by a delete in flight. The committed row is read in place
    /// (task 7): its numbers are copied, its names only compared.
    fn partition(&self, ov: &Overlay, pid: Pid) -> Result<Option<PartView>, Refusal> {
        self.touched(|| footprint::log(pid));
        if ov.is_gone(pid) {
            return Ok(None);
        }
        // A placeholder is a partition a group created: never committed.
        let committed = if is_placeholder(pid) {
            CommittedPart::None
        } else {
            self.committed_part(ov, pid)?
        };
        let (mut last_offset, mut last_created) = match (committed, ov.created_of(pid)) {
            // Every committed partition predates the entries in flight, so
            // one of a queue a delete in flight drops is going away with it
            // — named in the delete's garbage or not.
            (CommittedPart::Dropped, _) => return Ok(None),
            (CommittedPart::Head(h), _) => (h.last_offset, h.last_created_at_us),
            (CommittedPart::None, Some(c)) => (-1, c.created_at_us - 1),
            (CommittedPart::None, None) => return Ok(None),
        };
        if let Some(last) = ov.last_append(pid) {
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
    /// Whether partition `pid` exists: [`Planner::partition`] is `Some`. The
    /// overlay's partition state is read only for a partition not committed
    /// yet, so a row naming a committed partition plans on whichever planning
    /// thread (the footprint scheduler: the thread need not own its shard).
    fn partition_exists(&self, ov: &Overlay, pid: Pid) -> Result<bool, Refusal> {
        if ov.is_gone(pid) {
            return Ok(false);
        }
        if is_placeholder(pid) {
            return Ok(ov.created_of(pid).is_some());
        }
        Ok(match self.committed_part(ov, pid)? {
            CommittedPart::Dropped => false,
            CommittedPart::Head(_) => true,
            // No committed row. A pid below the counter the cycle began with
            // was committed before it, and its row is gone: nothing in flight
            // creates it, so the overlay is not read (a planning thread plans
            // the row of a partition that is not in its shards). A pid at or
            // above it can only be one an entry in flight created.
            CommittedPart::None => {
                self.base_pid.is_none_or(|base| pid >= base) && ov.created_of(pid).is_some()
            }
        })
    }

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
        // The front decides whether anything must be read: a "skip" is a proof
        // that no in-window occurrence of this hash exists, committed or in
        // flight (see [`DedupFront`]: a plan inserts every survivor's hash
        // before it commits, and a seed reads the overlay's appends beside the
        // committed rows). Under `rows` the committed read is a single LMDB
        // get on the `(pid, hash)` row; under `txns` a "maybe" names the
        // generation offset bands, and the read is a bounded ordered scan of
        // the txns rows (PERF-E). Any answer but "skip" also reads the appends
        // in flight to the partition. A partition created this cycle is not in
        // the front (its id is a placeholder): its appends are always read.
        // `front_prepare` must have run for this pid this command.
        if is_placeholder(pid) {
            return Ok(Self::in_flight_min(
                &ov.appends_of(pid),
                hash,
                window_floor_us,
                None,
            ));
        }
        let (best, absent) = match self.cfg.index_mode {
            IndexMode::Rows => {
                if self.front.should_probe(pid, hash, window_floor_us) {
                    let found = dedup::probe_one(self.reads(), pid, hash, window_floor_us)
                        .map_err(store_err)?;
                    (found, false)
                } else {
                    (None, true)
                }
            }
            IndexMode::Txns | IndexMode::Segment => {
                let mut ranges: Vec<(u64, u64)> = Vec::new();
                let verdict = self
                    .front
                    .probe_plan(pid, hash, window_floor_us, &mut ranges);
                let found = self.dedup_committed(pid, hash, window_floor_us, verdict, &ranges)?;
                (found, verdict == ProbeVerdict::Skip)
            }
        };
        if absent {
            return Ok(None);
        }
        Ok(Self::in_flight_min(
            &ov.appends_of(pid),
            hash,
            window_floor_us,
            best,
        ))
    }

    /// `best` lowered to the earliest in-window occurrence of `hash` among
    /// `appends`, the appends in flight to its partition
    /// ([`Overlay::appends_of`]: every layer, oldest first). A scan: an append
    /// keeps its frames' hashes and nothing else indexes them. It runs for a
    /// hash the front could not rule out (a real duplicate, a false positive,
    /// a partition with no warm filter), never for a hash it called new.
    fn in_flight_min(
        appends: &[&OverlayAppend],
        hash: &[u8; 16],
        window_floor_us: i64,
        mut best: Option<u64>,
    ) -> Option<u64> {
        for a in appends {
            if a.created_at_us < window_floor_us {
                continue;
            }
            for (i, h) in a.hashes.iter().enumerate() {
                if h == hash {
                    let off = a.base + i as u64;
                    best = Some(best.map_or(off, |b| b.min(off)));
                }
            }
        }
        best
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
        // A partition a group created this cycle: nothing committed, and never
        // in the dedup front (whose entries a real pid owns).
        if is_placeholder(pid) {
            let appends = ov.appends_of(pid);
            return Ok(items
                .iter()
                .map(|it| Self::in_flight_min(&appends, &it.hash, window_floor_us, None))
                .collect());
        }
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
        // The appends in flight to this partition: read once, and only when a
        // hash needs them.
        let mut in_flight: Option<Vec<&OverlayAppend>> = None;
        for (it, plan) in items.iter().zip(&plans) {
            let mut best = self.dedup_committed(
                pid,
                &it.hash,
                window_floor_us,
                plan.verdict,
                &bands[plan.from..plan.to],
            )?;
            // A "skip" is the front's proof that the hash has no in-window
            // occurrence, committed or in flight ([`Planner::dedup_probe_one`]).
            // Any other verdict reads the appends in flight.
            if plan.verdict != ProbeVerdict::Skip {
                let appends = in_flight.get_or_insert_with(|| ov.appends_of(pid));
                best = Self::in_flight_min(appends, &it.hash, window_floor_us, best);
            }
            out.push(best);
        }
        Ok(out)
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
        for a in ov.appends_of(pid) {
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
