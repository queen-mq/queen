//! Leader-side planning (PLAN_RAFT.md §7, §8): the pure functions that turn a
//! client command into EFFECTS and an OUTCOME, over the committed view plus an
//! overlay of the entries still in flight.
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
//! allows randomness in the planner (a partition uuid, a dlq id, the wildcard
//! rotation). Apply carries no clock and no randomness, so every absolute time
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
//!   segment shape — base, end, created_at, and the hashes the delivered set
//!   needs — from them plus the overlay's own appends. It never reads `seg_loc`.
//! * **Seeding is by the subscription INSTANT, which is position-exact.** §8
//!   calls RSM seeding position-based; the mechanism that realises it is the
//!   group's stored subscription instant (`registered_at_us` for `new`,
//!   `subscription_timestamp_us` for `timestamp`) compared to each append's
//!   `created_at`. D5's clock (`now = max(wall, last_now+1, max_created_at+1)`)
//!   makes `created_at < registered_at` hold for exactly the appends committed
//!   BEFORE the registration entry, so the comparison is the position
//!   comparison — and it is what the SQL spec itself does (004 ≈305–316,
//!   `s.created_at >= v_from_ts`). The `(reg_index, reg_effect)` positions apply
//!   records on the group row are not needed under this formulation.
//! * **The delivered set is in the cursor (O16).** A claim records the distinct
//!   transaction hashes it delivered on the cursor row, bounded by the batch
//!   size, so the ack fast path is deterministic instead of resting on a RAM map
//!   a failover loses.
//! * **No repacking, no decompression (O20).** The receiver pre-packs one frame
//!   per message; a duplicate drops a survivor and the `Append` blob is the
//!   survivors concatenated, never repacked. A forced-DLQ ack carries the
//!   receiver's snapshot of the poison frame, never decompressed here.
//!
//! # The cycle, and who calls what
//!
//! The batcher (WP-1.6b) owns the [`Overlay`] across the bounded pipeline (D4,
//! four entries in flight). Per cycle it: builds an `Overlay` from committed
//! bases and folds every in-flight entry into it ([`Overlay::ingest_entry`]),
//! marks the cycle start ([`Overlay::mark_cycle_start`]) so the entry it is
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

use std::collections::{BTreeSet, HashMap};

use crate::rsm::dedup::{self, AckRes, TxnsRow};
use crate::rsm::effect::{CursorRow, Effect, GroupMeta, Pid, QueueConfig, SubscriptionMode};
use crate::rsm::entry::{Entry, Outcome, RequestId};
use crate::rsm::state::Committed;
use crate::rsm::store::rows::GroupRow;
use crate::rsm::store::{keys, Keyspace, Reads, StoreError};

pub mod ack;
pub mod pop;
pub mod push;

// The phase-2 planner surfaces keep their place in the §3.4 module map as empty
// inline stubs, exactly as they stood inside `rsm/mod.rs` before this WP filled
// `planner`. The owning WP flips one to a file.

/// The transaction wire: one command, the SQL's lock order (005). WP-2.1.
pub mod txn {}
/// KV: versions from `kv_version_base + ordinal`, TTL, prefix lists,
/// `required` (024). WP-2.2.
pub mod kv {}
/// Timers: apply, fire, fail, DLQ, and the leader wheel (025). WP-2.3.
pub mod timers {}
/// Streams: cycle, register, state (007, 008). WP-2.4.
pub mod streams {}
/// Admin: configure, deletes, consumer groups, messages, flags, quotas
/// (010, 012, 013, 014, 016, 030, 031). WP-2.5.
pub mod admin {}
/// Retention as a leader loop: rules 1–3, max-wait eviction, txns purge
/// (006). WP-2.7.
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
}

impl Default for PlanConfig {
    fn default() -> PlanConfig {
        PlanConfig {
            entry_max_bytes: crate::rsm::entry::ENTRY_MAX_BYTES_DEFAULT,
            plan_budget_ms: PLAN_BUDGET_MS_DEFAULT,
            slow_command_ms: SLOW_COMMAND_MS_DEFAULT,
        }
    }
}

// ---------------------------------------------------------------------------
// Refusal and the plan result
// ---------------------------------------------------------------------------

/// A whole-command refusal: no effects, no outcome recorded, an error answer to
/// the client (§5.4, I14). `retryable` is what the receiver turns into a 5xx
/// the SDK retries with the SAME request id (D6) versus a 4xx it does not.
#[derive(Clone, Debug, PartialEq, Eq)]
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
    /// the postgres schema accepts and LMDB does not, R-108).
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushItem {
    /// `xxh3_128` of the transaction id, the dedup key and the `Append` hash.
    pub hash: [u8; 16],
    pub frame: Vec<u8>,
}

/// `POST /api/v1/push` for one partition (003). The receiver has already
/// resolved the queue's config (for the implicit-creation case) and packed each
/// message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushCommand {
    pub request_id: RequestId,
    pub tenant: String,
    pub queue: String,
    pub partition: String,
    pub items: Vec<PushItem>,
    /// The config an implicit queue creation uses (003 first contact). Ignored
    /// when the queue already exists. `id` is the receiver-minted queue uuid.
    pub create_cfg: QueueConfig,
}

/// The subscription intent a pop carries for a group with no stored policy
/// (004 ≈320): the pop-carried `sub_mode`/`sub_from` fall-back.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct SubIntent {
    /// `""`, `"new"`, `"all"`, or `"timestamp"`.
    pub mode: String,
    /// An ISO-8601 instant already parsed to µs by the receiver, or `None`.
    pub from_us: Option<i64>,
    /// The literal `"now"` intent (seed at the tail), distinct from an absent
    /// `from`.
    pub now: bool,
}

/// A pop of one named partition (`log_pop_specific_v1`), the whole queue by
/// wildcard (`log_pop_wildcard_*_v1`), or a discovery group across a namespace
/// or task (`log_pop_discover_*_v1`). One struct, three entry points.
#[derive(Clone, Debug, PartialEq, Eq)]
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
    /// Wildcard/discovery: max partitions claimed; `0` = unlimited.
    pub max_parts: i32,
    pub lease_seconds: i32,
    pub auto_ack: bool,
    pub conflate: bool,
    pub sub: SubIntent,
    /// The hot-list wheel owns the window-buffer hold on the pinned path
    /// (004): skip the debounce here when it says so.
    pub skip_window_debounce: bool,
    /// Discovery: the namespace and task that pick the queues.
    pub namespace: String,
    pub task: String,
    /// The config a wildcard pop uses if it must create the queue (004 ≈1046);
    /// `None` disables implicit creation (pinned never creates).
    pub create_cfg: Option<QueueConfig>,
}

/// The status an ack item carries (005). `Ok` covers the SQL's
/// completed/success/acked/ok/"".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct DlqSnapshot {
    pub message_id: Option<[u8; 16]>,
    pub txn: String,
    pub payload: Vec<u8>,
}

/// One item of a hash-resolved ack (005).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckItem {
    pub hash: [u8; 16],
    pub status: AckStatus,
    pub error: Option<String>,
    /// Present on a `Failed`/`Dlq` item when the receiver pre-read the poison
    /// frame (O7/O20).
    pub snapshot: Option<DlqSnapshot>,
}

/// One `(pid, group)` target of an ack (005). A batch acks several.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckTarget {
    pub pid: Pid,
    pub tenant: String,
    pub queue: String,
    pub group: String,
    /// The lease id; `""` for a lease-less ack, which still advances (005
    /// RUSTFIX item 11).
    pub worker: String,
    pub items: Vec<AckItem>,
}

/// `POST /api/v1/ack`, `/ack/batch` (005 `log_ack_by_hash_v1` / `log_ack_multi_v1`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckCommand {
    pub request_id: RequestId,
    pub targets: Vec<AckTarget>,
}

/// A positional ack of ONE leased batch (`log_ack_v1` / `log_ack_at_v1`): the
/// receiver advances the cursor to an absolute offset it computed from the
/// delivered batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckPositionalCommand {
    pub request_id: RequestId,
    pub pid: Pid,
    pub tenant: String,
    pub queue: String,
    pub group: String,
    pub worker: String,
    /// `Some(offset)` acks up to and including it; `None` (or `ok=false`) is a
    /// nack that releases the lease and redelivers.
    pub upto: Option<i64>,
    pub ok: bool,
    /// How many frames the handler is acking, for `total_consumed`.
    pub acked_count: i32,
}

/// Release a worker's lease on a `(pid, group)` without moving the cursor — the
/// nack the whole batch redelivers from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NackCommand {
    pub request_id: RequestId,
    pub pid: Pid,
    pub tenant: String,
    pub queue: String,
    pub group: String,
    pub worker: String,
}

/// `POST /api/v1/lease/:leaseId/extend` (`log_renew_lease_v1`). Renews EVERY
/// live lease of the worker: the planner walks `leases_by_worker` in key order
/// (never a hash map, §8), so the effect list is deterministic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenewCommand {
    pub request_id: RequestId,
    pub worker: String,
    pub seconds: i32,
}

/// `log_dlq_head_v1` as a standalone command: file the poison HEAD frame the
/// receiver snapshotted, advance past it, release the lease. Used when the DLQ
/// handoff was not folded into the ack (the `/dlq` replay-then-die path).
#[derive(Clone, Debug, PartialEq, Eq)]
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

// ---------------------------------------------------------------------------
// The overlay
// ---------------------------------------------------------------------------

/// One `Append` the overlay remembers: its offset range, its stamp, and the
/// per-frame hashes in frame order (for the delivered set and the dedup probe).
#[derive(Clone, Debug)]
struct OverlayAppend {
    base: u64,
    end: u64,
    created_at_us: i64,
    hashes: Vec<[u8; 16]>,
}

/// A partition as the overlay knows it, relative to committed state.
#[derive(Clone, Debug, Default)]
struct OverlayPart {
    /// `Some` when a `PartitionCreate` for this pid is in the overlay: committed
    /// state has no row for it, so the merged view is built from this.
    created: Option<CreatedPart>,
    appends: Vec<OverlayAppend>,
    /// A `Watermark` in the overlay (retention in flight): `(log_start,
    /// txns_start)`.
    watermark: Option<(u64, u64)>,
}

#[derive(Clone, Debug)]
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
    queues: HashMap<(String, String), Option<QueueConfig>>,
    groups: HashMap<(String, String, String), Option<GroupRow>>,
    pids_by_key: HashMap<(String, String, String), Pid>,
    parts: HashMap<Pid, OverlayPart>,
    cursors: HashMap<(Pid, String), Option<CursorRow>>,
    /// `(pid, hash) → [(offset, created_at)]`: the occurrences an overlay
    /// `Append` added, merged with committed `dedup` on a probe or a resolve.
    dedup: HashMap<(Pid, [u8; 16]), Vec<DedupOccurrence>>,
    request_ids: HashMap<RequestId, Outcome>,
}

/// One overlay dedup occurrence: `(offset, created_at_us)`.
type DedupOccurrence = (u64, i64);

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
            queues: HashMap::new(),
            groups: HashMap::new(),
            pids_by_key: HashMap::new(),
            parts: HashMap::new(),
            cursors: HashMap::new(),
            dedup: HashMap::new(),
            request_ids: HashMap::new(),
        }
    }

    /// Fold an entry still in flight into the overlay: its effects (so a later
    /// command sees them) and its request ids (so a retry of one in flight is
    /// found, §5.4). Call once per in-flight entry, in index order, before
    /// [`Overlay::mark_cycle_start`].
    pub fn ingest_entry(&mut self, e: &Entry) {
        self.max_now_us = self.max_now_us.max(e.now_us);
        for eff in &e.effects {
            self.fold_effect(eff);
        }
        for c in &e.commands {
            self.request_ids
                .entry(c.request_id)
                .or_insert_with(|| c.outcome.clone());
        }
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
        self.request_ids.get(id)
    }

    /// Fold one effect into the overlay indexes. Used for in-flight entries and,
    /// via [`Overlay::apply_effects`], for the current cycle's own effects.
    fn fold_effect(&mut self, eff: &Effect) {
        match eff {
            Effect::QueueUpsert { tenant, queue, cfg } => {
                self.queues
                    .insert((tenant.clone(), queue.clone()), Some(cfg.clone()));
            }
            Effect::QueueDelete { tenant, queue } => {
                self.queues.insert((tenant.clone(), queue.clone()), None);
            }
            Effect::GroupUpsert {
                tenant,
                queue,
                group,
                meta,
            } => {
                self.groups.insert(
                    (tenant.clone(), queue.clone(), group.clone()),
                    Some(GroupRow {
                        meta: meta.clone(),
                        reg_index: 0,
                        reg_effect: 0,
                    }),
                );
            }
            Effect::GroupDelete {
                tenant,
                queue,
                group,
            } => {
                self.groups
                    .insert((tenant.clone(), queue.clone(), group.clone()), None);
            }
            Effect::PartitionCreate {
                pid,
                uuid,
                tenant,
                queue,
                partition,
                created_at_us,
            } => {
                self.pids_by_key
                    .insert((tenant.clone(), queue.clone(), partition.clone()), *pid);
                self.parts.entry(*pid).or_default().created = Some(CreatedPart {
                    uuid: *uuid,
                    tenant: tenant.clone(),
                    queue: queue.clone(),
                    partition: partition.clone(),
                    created_at_us: *created_at_us,
                });
                self.next_pid = self.next_pid.max(pid.saturating_add(1));
                self.max_created_at_us = self.max_created_at_us.max(*created_at_us);
            }
            Effect::Append {
                pid,
                base_offset,
                count,
                created_at_us,
                hashes,
                ..
            } => {
                let count = *count as usize;
                let mut hs: Vec<[u8; 16]> = Vec::with_capacity(count);
                for i in 0..count {
                    let mut h = [0u8; 16];
                    if let Some(slice) = hashes.get(i * 16..i * 16 + 16) {
                        h.copy_from_slice(slice);
                    }
                    hs.push(h);
                }
                let end = base_offset + count as u64 - 1;
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
                    });
                self.max_created_at_us = self.max_created_at_us.max(*created_at_us);
            }
            Effect::CursorSet { pid, group, row } => {
                self.cursors
                    .insert((*pid, group.clone()), Some(row.clone()));
            }
            Effect::CursorDelete { pid, group } => {
                self.cursors.insert((*pid, group.clone()), None);
            }
            Effect::Watermark {
                pid,
                log_start,
                txns_start,
            } => {
                self.parts.entry(*pid).or_default().watermark = Some((*log_start, *txns_start));
            }
            Effect::KvPut { version, .. } => {
                self.next_kv_version = self.next_kv_version.max(version.saturating_add(1));
            }
            // Everything else the phase-1 planner neither emits nor needs to see
            // through the overlay (admin deletes, timers, streams, traces): a
            // later phase folds what its planner reads.
            _ => {}
        }
    }

    /// Fold the current command's own effects, so the next command in the cycle
    /// sees them (§7.2). The planner calls this once a command is decided
    /// [`Plan::Logged`]; a refused or empty command folds nothing.
    fn apply_effects(&mut self, effects: &[Effect]) {
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
}

/// A partition merged across committed state and the overlay — the shape the
/// pop walk, the push serialiser and seeding read. There is no `hw`:
/// `last_offset` is the visible tail. Some fields are read by later phases
/// (the uuid a trace echoes, the txns watermark retention reads); phase 1 does
/// not touch all of them.
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct PartView {
    pid: Pid,
    uuid: [u8; 16],
    tenant: String,
    queue: String,
    partition: String,
    last_offset: i64,
    log_start: u64,
    txns_start: u64,
    last_created_at_us: i64,
    created_at_us: i64,
}

impl PartView {
    /// The first offset a pop can still be served (`log_start - 1`, the "last
    /// acked" form the cursor arithmetic uses).
    fn floor(&self) -> i64 {
        self.log_start as i64 - 1
    }
}

/// One segment (one `Append`) as the pop walk sees it.
#[derive(Clone, Copy, Debug)]
struct Seg {
    base: u64,
    end: u64,
    created_at_us: i64,
}

impl<'a, R: Reads + ?Sized> Planner<'a, R> {
    pub fn new(committed: Committed<'a, R>, now_us: i64, cfg: PlanConfig) -> Planner<'a, R> {
        Planner {
            committed,
            now_us,
            cfg,
        }
    }

    pub fn now_us(&self) -> i64 {
        self.now_us
    }

    pub fn committed(&self) -> &Committed<'a, R> {
        &self.committed
    }

    fn reads(&self) -> &'a R {
        self.committed.reads()
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
    /// committed.
    fn queue_cfg(
        &self,
        ov: &Overlay,
        tenant: &str,
        queue: &str,
    ) -> Result<Option<QueueConfig>, Refusal> {
        if let Some(v) = ov.queues.get(&(tenant.to_string(), queue.to_string())) {
            return Ok(v.clone());
        }
        self.committed.queue(tenant, queue).map_err(store_err)
    }

    fn group(
        &self,
        ov: &Overlay,
        tenant: &str,
        queue: &str,
        group: &str,
    ) -> Result<Option<GroupRow>, Refusal> {
        if let Some(v) = ov
            .groups
            .get(&(tenant.to_string(), queue.to_string(), group.to_string()))
        {
            return Ok(v.clone());
        }
        self.committed
            .group(tenant, queue, group)
            .map_err(store_err)
    }

    fn pid_of(
        &self,
        ov: &Overlay,
        tenant: &str,
        queue: &str,
        partition: &str,
    ) -> Result<Option<Pid>, Refusal> {
        if let Some(pid) =
            ov.pids_by_key
                .get(&(tenant.to_string(), queue.to_string(), partition.to_string()))
        {
            return Ok(Some(*pid));
        }
        self.committed
            .pid_of(tenant, queue, partition)
            .map_err(store_err)
    }

    /// The merged partition view, or `None` when the pid is unknown or garbage.
    fn partition(&self, ov: &Overlay, pid: Pid) -> Result<Option<PartView>, Refusal> {
        let committed = self.committed.partition(pid).map_err(store_err)?;
        let overlay = ov.parts.get(&pid);
        let (
            uuid,
            tenant,
            queue,
            partition,
            mut last_offset,
            mut log_start,
            mut txns_start,
            mut last_created,
            created_at,
        ) = match (&committed, overlay.and_then(|o| o.created.as_ref())) {
            (Some(p), _) => (
                p.uuid,
                p.tenant.clone(),
                p.queue.clone(),
                p.partition.clone(),
                p.last_offset,
                p.log_start,
                p.txns_start,
                p.last_created_at_us,
                p.created_at_us,
            ),
            (None, Some(c)) => (
                c.uuid,
                c.tenant.clone(),
                c.queue.clone(),
                c.partition.clone(),
                -1,
                0,
                0,
                c.created_at_us - 1,
                c.created_at_us,
            ),
            (None, None) => return Ok(None),
        };
        if let Some(o) = overlay {
            if let Some(last) = o.appends.last() {
                last_offset = last.end as i64;
                last_created = last.created_at_us;
            }
            if let Some((ls, ts)) = o.watermark {
                log_start = ls;
                txns_start = ts;
            }
        }
        Ok(Some(PartView {
            pid,
            uuid,
            tenant,
            queue,
            partition,
            last_offset,
            log_start,
            txns_start,
            last_created_at_us: last_created,
            created_at_us: created_at,
        }))
    }

    fn cursor(&self, ov: &Overlay, pid: Pid, group: &str) -> Result<Option<CursorRow>, Refusal> {
        if let Some(v) = ov.cursors.get(&(pid, group.to_string())) {
            return Ok(v.clone());
        }
        self.committed.cursor(pid, group).map_err(store_err)
    }

    // ---- segments (from the txns keyspace + overlay) ----------------------

    /// The partition's segments with `base >= from_base`, in offset order:
    /// committed `txns` rows merged with the overlay's own appends. Bounded by
    /// nothing in phase 1 (the pop walk of pgless was equally O(segments); the
    /// per-partition segment RAM is the ratified §6.1 cost, R-105). A production
    /// bound (scan near `wanted`) is a follow-up.
    fn segs_from(
        &self,
        ov: &Overlay,
        part: &PartView,
        from_base: u64,
    ) -> Result<Vec<Seg>, Refusal> {
        let mut segs: Vec<Seg> = Vec::new();
        let prefix = keys::txns_prefix(part.pid);
        let from = keys::txns(part.pid, from_base);
        let mut bad: Option<StoreError> = None;
        self.reads()
            .scan_raw(
                Keyspace::Txns,
                &from,
                &prefix,
                usize::MAX,
                &mut |k, v| match (keys::txns_base_of(k), TxnsRow::decode(v)) {
                    (Some(base), Ok(row)) => {
                        segs.push(Seg {
                            base,
                            end: row.end,
                            created_at_us: row.created_at_us,
                        });
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
        if let Some(o) = ov.parts.get(&part.pid) {
            for a in &o.appends {
                if a.base >= from_base {
                    segs.push(Seg {
                        base: a.base,
                        end: a.end,
                        created_at_us: a.created_at_us,
                    });
                }
            }
        }
        segs.sort_by_key(|s| s.base);
        Ok(segs)
    }

    /// The distinct transaction hashes delivered in the inclusive offset range
    /// `[lo, hi]`: the delivered set a claim records on the cursor (O16),
    /// bounded by the batch size. Committed hashes come from the `txns` rows,
    /// overlay hashes from the overlay's appends.
    fn hashes_in_range(
        &self,
        ov: &Overlay,
        pid: Pid,
        lo: u64,
        hi: u64,
    ) -> Result<Vec<[u8; 16]>, Refusal> {
        if hi < lo {
            return Ok(Vec::new());
        }
        let mut seen: BTreeSet<[u8; 16]> = BTreeSet::new();
        let mut out: Vec<[u8; 16]> = Vec::new();
        let mut push = |h: [u8; 16]| {
            if seen.insert(h) {
                out.push(h);
            }
        };
        // committed txns rows overlapping [lo, hi]
        let prefix = keys::txns_prefix(pid);
        // start at the row that may cover `lo`: the greatest base <= lo.
        let start_base = self.seg_base_covering(pid, lo)?.unwrap_or(lo);
        let from = keys::txns(pid, start_base);
        let mut bad: Option<StoreError> = None;
        let mut stop = false;
        self.reads()
            .scan_raw(Keyspace::Txns, &from, &prefix, usize::MAX, &mut |k, v| {
                if stop {
                    return false;
                }
                match (keys::txns_base_of(k), TxnsRow::decode(v)) {
                    (Some(base), Ok(row)) => {
                        if base > hi {
                            stop = true;
                            return false;
                        }
                        for (i, h) in row.iter_hashes().enumerate() {
                            let off = base + i as u64;
                            if off >= lo && off <= hi {
                                push(h);
                            }
                        }
                        true
                    }
                    _ => {
                        bad = Some(StoreError::corrupt(Keyspace::Txns, "txns row"));
                        false
                    }
                }
            })
            .map_err(store_err)?;
        if let Some(e) = bad {
            return Err(store_err(e));
        }
        if let Some(o) = ov.parts.get(&pid) {
            for a in &o.appends {
                for (i, h) in a.hashes.iter().enumerate() {
                    let off = a.base + i as u64;
                    if off >= lo && off <= hi {
                        push(*h);
                    }
                }
            }
        }
        Ok(out)
    }

    /// The base offset of the segment covering `off` (the greatest base ≤ off),
    /// or `None` when no committed segment does. Overlay appends are the tail;
    /// the caller adds them.
    fn seg_base_covering(&self, pid: Pid, off: u64) -> Result<Option<u64>, Refusal> {
        let prefix = keys::txns_prefix(pid);
        let from = keys::txns(pid, off);
        let mut found: Option<u64> = None;
        let mut bad: Option<StoreError> = None;
        self.reads()
            .scan_rev_raw(Keyspace::Txns, &from, &prefix, 1, &mut |k, _v| {
                match keys::txns_base_of(k) {
                    Some(base) => found = Some(base),
                    None => bad = Some(StoreError::corrupt(Keyspace::Txns, "txns key")),
                }
                false
            })
            .map_err(store_err)?;
        if let Some(e) = bad {
            return Err(store_err(e));
        }
        Ok(found)
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
        let mut best =
            dedup::probe_one(self.reads(), pid, hash, window_floor_us).map_err(store_err)?;
        if let Some(occ) = ov.dedup.get(&(pid, *hash)) {
            for (off, created) in occ {
                if *created >= window_floor_us {
                    best = Some(best.map_or(*off, |b| b.min(*off)));
                }
            }
        }
        Ok(best)
    }

    /// Resolve one hash for a hash ack (005): `eff` = MIN over `[lo, hi]`,
    /// `below` = any occurrence at or below `committed`. Merges committed and
    /// overlay occurrences.
    fn dedup_resolve(
        &self,
        ov: &Overlay,
        pid: Pid,
        hash: &[u8; 16],
        lo: u64,
        hi: u64,
        committed: i64,
    ) -> Result<AckRes, Refusal> {
        let mut res =
            dedup::resolve(self.reads(), pid, hash, lo, hi, committed).map_err(store_err)?;
        if let Some(occ) = ov.dedup.get(&(pid, *hash)) {
            for (off, _created) in occ {
                if (*off as i64) <= committed {
                    res.below = true;
                }
                if *off >= lo && *off <= hi {
                    res.eff = Some(res.eff.map_or(*off, |b| b.min(*off)));
                }
            }
        }
        Ok(res)
    }

    // ---- seeding ----------------------------------------------------------

    /// The cursor `committed` a first-contact (partition, group) seeds to, from
    /// the group's stored subscription (§8, 004 ≈305–316). `all` = before
    /// `log_start`; `new` = before the first append at/after the group's
    /// registration instant; `timestamp` = before the first append at/after the
    /// subscription timestamp. All three are one `seed_from_ts` over the
    /// segments, and the instant is position-exact by D5's clock (see the
    /// module header).
    fn seed_committed(
        &self,
        ov: &Overlay,
        part: &PartView,
        group_row: &GroupRow,
    ) -> Result<i64, Refusal> {
        let floor = part.floor();
        let seed = match group_row.meta.mode {
            SubscriptionMode::All => floor,
            SubscriptionMode::New => {
                let segs = self.segs_from(ov, part, part.log_start)?;
                seed_from_ts(
                    &segs,
                    part.log_start,
                    part.last_offset,
                    group_row.meta.registered_at_us,
                )
            }
            SubscriptionMode::Timestamp => {
                let segs = self.segs_from(ov, part, part.log_start)?;
                seed_from_ts(
                    &segs,
                    part.log_start,
                    part.last_offset,
                    group_row.meta.subscription_timestamp_us,
                )
            }
        };
        Ok(seed.max(floor).max(-1))
    }

    /// Seed from a pop-carried intent when the group has no stored policy
    /// (004 ≈320): `now`/`new` → the tail, an explicit instant → `seed_from_ts`,
    /// anything unparsable → ignore (the SQL's EXCEPTION handler).
    fn seed_from_intent(
        &self,
        ov: &Overlay,
        part: &PartView,
        intent: &SubIntent,
    ) -> Result<Option<i64>, Refusal> {
        if let Some(ts) = intent.from_us {
            let segs = self.segs_from(ov, part, part.log_start)?;
            return Ok(Some(seed_from_ts(
                &segs,
                part.log_start,
                part.last_offset,
                ts,
            )));
        }
        if intent.now || intent.mode == "new" {
            return Ok(Some(part.last_offset));
        }
        Ok(None)
    }
}

/// The cursor a subscription seeds: just before the first retained segment
/// stamped at or after `ts_us`; the allocated tail when nothing is that recent.
/// Inclusive (`created_at >= ts`), matching the SQL spec (004 ≈314).
fn seed_from_ts(segs: &[Seg], log_start: u64, last_offset: i64, ts_us: i64) -> i64 {
    for s in segs {
        if s.base >= log_start && s.created_at_us >= ts_us {
            return s.base as i64 - 1;
        }
    }
    last_offset
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
/// pop-carried intent (004 ≈226–246). `now`/`new`/unparsable → `new` at the
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
