//! The apply thread: the only mutator of committed state (PLAN_RAFT.md §3.3,
//! §7, §11.3–§11.5, §11.7; I1, I2, I4, I8, I10, I11, I18).
//!
//! One `std` thread per node consumes committed entries from a bounded channel
//! IN INDEX ORDER and executes their effects against two things and nothing
//! else: the ordered store (`rsm/store`, WP-1.2) and the payload segment files
//! (`rsm/segments`, WP-1.3). It is the only writer of both. The planner
//! produces effects and never touches state (I1); apply executes effects and
//! knows no semantics (D3).
//!
//! # Determinism (I2) — what makes it true, and what enforces it
//!
//! Apply is a pure function of (committed state, entry). It reads no clock, no
//! environment, no randomness, and iterates nothing whose order a hash map
//! decides:
//!
//! - **Time** comes from the entry (`entry.now_us`, D5) and from the effects,
//!   which carry absolute stamps the planner computed. Apply writes
//!   `meta.last_now_us` and advances `meta.max_created_at_us` from `Append`
//!   (§7.4) and does not otherwise know what time it is.
//! - **Iteration** is over the store's key order (LMDB's `memcmp`) and over
//!   `BTreeMap`/`BTreeSet`/`Vec`. The one exception is the effect list itself,
//!   which is a `Vec` in apply order.
//! - **Bounds that change state contents are CONSTANTS, not knobs.**
//!   [`REQUEST_EXPIRE_LIMIT`] bounds how many recorded outcomes one
//!   `RequestIdsExpire` retires; if it were an environment knob, two nodes
//!   with different values would hold different `request_ids` keyspaces and
//!   the §12.9 digest would diverge. Everything in [`ApplyConfig`] is
//!   node-local by construction: it decides WHEN bytes move (commit cadence,
//!   durable-point cadence, GC batch), never WHAT they are.
//! - **Enforcement**: `clippy.toml` lists `SystemTime::now`, `Instant::now`,
//!   `std::env::var*` and `rand::{random,thread_rng}`; `[lints.clippy]` in
//!   Cargo.toml allows the lint for the rest of the package and this file,
//!   `rsm/state/` and `rsm/store/` re-`deny` it. The two `#[allow]`s below are
//!   the whole of the exception and each says why at the allow.
//!
//! # Shards (`QUEEN_RAFT_APPLY_SHARDS`)
//!
//! "One thread" above is the ONE-SHARD path. With more shards, the effects
//! whose every row is keyed by one partition (appends, cursors, watermarks,
//! fresh dead letters, partition creates, stream state) execute on
//! `pid % shards` threads at once, between the global effects, which still
//! run here in entry order with no shard active. The state an entry leaves is
//! the one-shard state byte for byte — replicated and node-local, counters,
//! wakes and refusal reports included; `apply_shard.rs` has the argument and
//! `tests/apply_shards.rs` the differential proof. Apply stays a pure
//! function of (committed state, entry): the shards change which thread
//! writes a row, never which rows are written.
//!
//! # Idempotence, and why a replicator may replay from the durable index
//!
//! `meta.applied_index` is written in the SAME store transaction as everything
//! the entry changed (I11), so the store always reopens at a whole number of
//! entries. [`Applier::apply`] therefore SKIPS an entry whose index is at or
//! below the applied index, and that single guard is the whole of apply's
//! idempotence: replaying from the durable point (§11.5, the single-voter
//! repair path) re-delivers entries that are already in the store, and each of
//! them is a no-op instead of a second `Append`, a second dedup occurrence and
//! a double-counted counter.
//!
//! # The two cadences (§11.3, §11.4)
//!
//! - **Store commit**, non-durable, every `QUEEN_RAFT_STORE_COMMIT_MS` (4) or
//!   `QUEEN_RAFT_STORE_COMMIT_ENTRIES` (256). The write transaction stays open
//!   across entries; the Raft log is the write-ahead log, so nothing is lost
//!   between commits. Every such commit records the applied index AND the
//!   lengths and liveness of every segment file the entries touched, in one
//!   transaction — that is I11, and it is what lets recovery truncate files to
//!   the recorded lengths and trust every `seg_loc` row it finds.
//! - **Durable point**, every `QUEEN_RAFT_DURABLE_EVERY_MS` (1000) or
//!   `QUEEN_RAFT_DURABLE_EVERY_BYTES` (256 MiB): fsync the segment files
//!   written since the last one and their directories, then a DURABLE store
//!   commit carrying `meta.durable_index`, and only then report the durable
//!   index to the replicator. A failed durable commit reports NOTHING
//!   ([`StoreError::lost_durable_point`]): a durable index that never reached
//!   the platter would let a log be truncated behind acknowledged effects
//!   nothing can replay (I4, I11).
//!
//! # File GC (§11.7, I10)
//!
//! Two phases, one durable point apart. A dead file's node-local rows (`files`,
//! `partition_files`) are removed in a commit; the file itself is unlinked only
//! after the NEXT durable point, when no durable commit references it any
//! more. A pin held by a reader (a committed pop claim whose payloads have not
//! been read, I4) blocks both phases: `gc_candidates` skips pinned files and
//! `unlink` re-checks under the same lock.
//!
//! # What this work package does NOT apply
//!
//! Phase 1 is the MESSAGE PATH (§15, WP-1.4). The store opens the twenty
//! keyspaces of §6.1's message path plus, since WP-2.2, `kv` and `kv_expiry`
//! (the `KvPut` / `KvDelete` arms) and, since WP-2.3, `timers` and
//! `timers_due` (the `TimerUpsert` / `TimerDelete` / `TimerBackoff` arms).
//! `streams`, `traces`, `flags`, `quotas` and `eph_config` do not exist yet,
//! so their effect kinds answer [`ApplyError::Unsupported`] — a typed, fatal
//! refusal that stops this node, never a skip (I16). Phase 2 adds the
//! keyspaces and the arms together.

// I2, enforced rather than reviewed: `clippy.toml` lists the clock,
// environment and randomness calls this file may not make, `[lints.clippy]` in
// Cargo.toml switches the lint off for the rest of the package and every
// integration test, and this is where it is switched back on.
#![deny(clippy::disallowed_methods)]

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::AtomicU32;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::rsm::dedup;
use crate::rsm::effect::{Assigns, CodecError, Effect, GarbageScope, Kind, Pid};
use crate::rsm::entry::{CommandRecord, Entry, Outcome, RequestId};
use crate::rsm::qlog::set::QLogSet;
use crate::rsm::segments::{self, FileState, Position, Release, SegError, Segments};
use crate::rsm::store::keys::{self, Counter};
use crate::rsm::store::rows::{self, FileRow, GarbageRow, GroupRow, SegLocRow};
use crate::rsm::store::{
    meta, CheckpointCut, Keyspace, Reads, Store, StoreError, TypedReads, TypedWrites, Writes,
};

// Sharded apply: the pid-keyed effects of a run execute on several shards at
// once (`QUEEN_RAFT_APPLY_SHARDS`). Its header states why that is still a pure
// function of (committed state, entry); I2's `deny` above covers it too.
#[path = "apply_shard.rs"]
mod shard;
use shard::{shard_of, Shard};

// ---------------------------------------------------------------------------
// Constants that are part of the replicated behaviour
// ---------------------------------------------------------------------------

/// How many recorded outcomes one `RequestIdsExpire` effect retires.
///
/// A CONSTANT, never a knob (I2): it decides what the `request_ids` keyspace
/// holds after the effect, so two nodes with different values would diverge.
/// Raising it is a behaviour change and belongs with a kind version bump.
pub const REQUEST_EXPIRE_LIMIT: usize = 4096;

/// Counter keys the shards must hold between them before a flush writes
/// their partition-scope keys on the shard threads (below it, one thread does
/// it sooner than it wakes three). Node-local: WHEN bytes move, not what —
/// which is why the unit tests run it at 1, so every sharded test takes the
/// parallel flush.
#[cfg(not(test))]
const PARALLEL_FLUSH_MIN: usize = 512;
#[cfg(test)]
const PARALLEL_FLUSH_MIN: usize = 1;

/// Commands an entry must log before the shards record their outcomes in
/// parallel (see [`Applier::record_outcomes`]); the unit tests take the
/// parallel path at every size, as with [`PARALLEL_FLUSH_MIN`].
#[cfg(not(test))]
const PARALLEL_OUTCOMES_MIN: usize = 64;
#[cfg(test)]
const PARALLEL_OUTCOMES_MIN: usize = 1;

/// Rows per chunk of the unbounded name-keyed sweeps a queue delete does
/// (§5.2: the name-keyed rows go at once, the pid-keyed ones in
/// `DeleteChunk`s). It bounds the BUFFER of one `delete_range` call, not the
/// work, so every node deletes exactly the same rows whatever it is.
const SWEEP_CHUNK: usize = 4096;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// What apply refuses with. Every variant is FATAL for this node except the
/// store's own retryable ones: apply is the only writer, so there is nothing
/// to retry against and nothing to skip (I16, §0.3 "refuse, never guess").
#[derive(Debug)]
pub enum ApplyError {
    /// The store said no. Keeps its own classification
    /// ([`StoreError::fatal`], [`StoreError::retryable`],
    /// [`StoreError::lost_durable_point`]).
    Store(StoreError),
    /// The segment files said no.
    Segments(SegError),
    /// The entry did not decode or did not pass [`Entry::validate`] — an
    /// unknown kind, an unknown catalogue version, a span that does not cover
    /// the effects, a counter that is not its base plus its ordinal. I16: the
    /// node stops instead of stepping over it.
    Entry(CodecError),
    /// The replicator delivered an index this node cannot apply next. Apply
    /// never guesses across a hole in the log.
    Gap { expected: u64, got: u64 },
    /// I18: the entry's planner-assigned bases are not what `meta` holds, so
    /// the planner and this node disagree about which partition ids and KV
    /// versions the entry means.
    Bases {
        what: &'static str,
        expected: u64,
        got: u64,
    },
    /// I5: `entry.now_us` went backwards. The planner's clock is monotone
    /// across terms by construction; a violation would make lease expiry and
    /// every TTL judged against a time already spent.
    TimeWentBackwards { last: i64, got: i64 },
    /// An effect kind this build cannot apply (phase 2's keyspaces, or a kind
    /// from a newer catalogue). Stops the node; never skipped (I16).
    Unsupported { kind: Kind },
    /// The effect names state that is not there: a committed `Append` for a
    /// partition no row describes, an offset that is not the partition's next
    /// one. Two nodes that disagree about this disagree about the log.
    Inconsistent { what: &'static str, detail: String },
    /// A refusal already happened, so this applier will do nothing else. Every
    /// other variant leaves the entry it failed on HALF EXECUTED in the open
    /// store transaction, in the segment files and in the derived RAM, and
    /// apply is not a transaction: there is no partial undo. Carrying on —
    /// re-delivering the entry, committing what is open — would double the
    /// prefix: a second frame for the same `(pid, base_offset)`, a second set
    /// of dedup occurrences, counters counted twice. The node stops instead,
    /// and recovery replays from the last durable point (§11.5), which is the
    /// only state the prefix never reached.
    Poisoned { after: String },
    /// I11: the store and the segment files disagree — a file shorter than the
    /// length a committed transaction recorded, a missing file, or a frame
    /// below that length that does not verify. §11.5 answers it with a
    /// snapshot install (raft3) or a repair from this node's own newest
    /// snapshot (raft1); neither exists before WP-4.6, so phase 1 refuses to
    /// start rather than serving a partition whose tail is gone.
    Disagreement { detail: String },
    /// The SHADOW per-queue log (`QUEEN_RAFT_QLOG`, Phase A1) could not be
    /// written or fsynced. The shadow is not authoritative in A1, but a failure
    /// on the write path this phase exists to prove must surface LOUDLY rather
    /// than silently diverging what A2 will read — so, like a segment I/O error,
    /// it is fatal for the node (the authoritative store/segments already
    /// committed; the node stops and replays from its durable point).
    Qlog(std::io::Error),
    /// The node-local observability journal could not be opened. Queue state is
    /// untouched, but Phase 2 requires this store for retention/dashboard data.
    LocalMetrics(std::io::Error),
}

impl ApplyError {
    /// This node cannot carry on applying. ALWAYS true.
    ///
    /// It does not depend on [`StoreError::retryable`], and that is the point:
    /// a store error is retryable against a store, not against apply. The
    /// entry that raised it is half executed in the open transaction and in
    /// the segment files ([`ApplyError::Poisoned`]), so "retry" here means
    /// "apply its prefix twice". The applier refuses everything afterwards;
    /// the caller stops the Raft instance (§12.1 `Fatal`) and the node repairs
    /// from its last durable point.
    pub fn fatal(&self) -> bool {
        true
    }

    /// Would the store have taken this one again? Kept for the metrics and
    /// for a caller deciding what to report, never for deciding to carry on.
    pub fn store_retryable(&self) -> bool {
        matches!(self, ApplyError::Store(e) if e.retryable() && !e.fatal())
    }

    /// The durable point of §11.4 did not happen, so the caller reports no
    /// durable index for it (step 3).
    pub fn lost_durable_point(&self) -> bool {
        matches!(self, ApplyError::Store(e) if e.lost_durable_point())
    }

    /// Would every node that applies this entry refuse it the same way? True
    /// for a refusal decided by the entry and the replicated state alone — a
    /// codec or validation refusal, a clock or base mismatch, an unsupported
    /// kind, an effect naming state that is not there, a key over the store's
    /// limit. False for what only THIS node hit: I/O, a full map or disk, its
    /// own files or store bytes, a hole in what its replicator delivered, and
    /// the follow-on refusal of an applier already stopped.
    ///
    /// The operator's skip (`QUEEN_RAFT_APPLY_SKIP`) is for the first kind
    /// only: skipping an entry that other nodes applied diverges them.
    pub fn deterministic(&self) -> bool {
        match self {
            ApplyError::Entry(_)
            | ApplyError::Bases { .. }
            | ApplyError::TimeWentBackwards { .. }
            | ApplyError::Unsupported { .. }
            | ApplyError::Inconsistent { .. } => true,
            ApplyError::Store(e) => matches!(e, StoreError::KeyTooLong { .. }),
            ApplyError::Gap { .. }
            | ApplyError::Segments(_)
            | ApplyError::Poisoned { .. }
            | ApplyError::Disagreement { .. }
            | ApplyError::Qlog(_)
            | ApplyError::LocalMetrics(_) => false,
        }
    }
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApplyError::Store(e) => write!(f, "apply: {e}"),
            ApplyError::Segments(e) => write!(f, "apply: segments: {e}"),
            ApplyError::Entry(e) => write!(f, "apply: entry: {e}"),
            ApplyError::Gap { expected, got } => write!(
                f,
                "apply: the log jumped: expected index {expected}, got {got}"
            ),
            ApplyError::Bases {
                what,
                expected,
                got,
            } => write!(
                f,
                "apply: I18: the entry's {what} base is {got}, meta says {expected}"
            ),
            ApplyError::TimeWentBackwards { last, got } => write!(
                f,
                "apply: I5: entry now_us {got} is below the last applied {last}"
            ),
            ApplyError::Unsupported { kind } => write!(
                f,
                "apply: effect kind {} is not applicable in this build (phase 1 is the message path)",
                kind.name()
            ),
            ApplyError::Inconsistent { what, detail } => {
                write!(f, "apply: {what}: {detail}")
            }
            ApplyError::Poisoned { after } => write!(
                f,
                "apply: this node stopped applying at an earlier refusal ({after}), \
                 and an entry left half executed is never resumed"
            ),
            ApplyError::Disagreement { detail } => write!(
                f,
                "apply: I11: the store and the segment files disagree: {detail}"
            ),
            ApplyError::Qlog(e) => write!(f, "apply: qlog (shadow): {e}"),
            ApplyError::LocalMetrics(e) => write!(f, "apply: local metrics: {e}"),
        }
    }
}

impl std::error::Error for ApplyError {}

impl From<StoreError> for ApplyError {
    fn from(e: StoreError) -> ApplyError {
        ApplyError::Store(e)
    }
}

impl From<SegError> for ApplyError {
    fn from(e: SegError) -> ApplyError {
        if e.is_disagreement() {
            return ApplyError::Disagreement {
                detail: e.to_string(),
            };
        }
        ApplyError::Segments(e)
    }
}

impl From<CodecError> for ApplyError {
    fn from(e: CodecError) -> ApplyError {
        ApplyError::Entry(e)
    }
}

pub type Result<T> = std::result::Result<T, ApplyError>;

// ---------------------------------------------------------------------------
// The clock, injected
// ---------------------------------------------------------------------------

/// The cadence clock, and the only clock anywhere near apply.
///
/// It never decides what state holds — only when the store commits, when a
/// durable point runs and how long the loop parks waiting for the next entry.
/// It is a trait so that the apply loop itself contains no clock call and a
/// test can drive both cadences exactly.
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

/// The wall clock, for the broker.
pub struct SystemClock;

impl Clock for SystemClock {
    // The ONE clock read on this side of the I2 line, and it is node-local by
    // construction: its value decides when bytes are flushed, never what the
    // store or the segment files contain. Every state-bearing time comes from
    // `entry.now_us` (D5).
    #[allow(clippy::disallowed_methods)]
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// A clock a test moves by hand. `base` is supplied by the caller, so this
/// file still reads no clock of its own.
pub struct ManualClock {
    at: std::sync::Mutex<Instant>,
}

impl ManualClock {
    pub fn new(base: Instant) -> ManualClock {
        ManualClock {
            at: std::sync::Mutex::new(base),
        }
    }

    pub fn advance(&self, by: Duration) {
        let mut g = self.at.lock().expect("manual clock");
        *g += by;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        *self.at.lock().expect("manual clock")
    }
}

// ---------------------------------------------------------------------------
// Notifications: waiters and wakes
// ---------------------------------------------------------------------------

/// What apply tells the rest of the node. Every method is called ON THE APPLY
/// THREAD and MUST NOT BLOCK (I15): an implementation sends on an unbounded or
/// try-send channel, or calls `Notify::notify_waiters` on a tokio primitive.
/// A blocking implementation stalls the only writer in the process.
pub trait Notify: Send + Sync {
    /// The entry at `index` is applied on this node. `commands` carries each
    /// logged command's request id and outcome, in plan order (§5.4): the
    /// leader resolves its waiters from it (I4 — nothing is answered before
    /// this call), and a follower ignores it.
    fn applied(&self, index: u64, term: u64, commands: &[CommandRecord]);

    /// New work, or a lease released, for a (tenant, queue, group). Parked
    /// long-polls on this node wake (§9.5). `group` is `None` for new data on
    /// the queue as a whole, emitted only while [`Notify::wants_append_wakes`]
    /// says someone parks outside the registered groups.
    fn wake(&self, tenant: &str, queue: &str, group: Option<&str>);

    /// A registered group's wake for an APPEND: one call per (partition,
    /// group) the append armed, in the same place of the same sorted sequence
    /// as every other wake. Only the append path calls it; a released lease
    /// and the queue-wide `None` wake keep coming through [`Notify::wake`].
    ///
    /// Split out because the two can be answered at different moments: a
    /// leader may wake its parked pops when it PLANS the append (one raft
    /// round earlier) and then skip this apply-time repeat, while a follower
    /// keeps it. The default is exactly [`Notify::wake`].
    fn wake_append(&self, tenant: &str, queue: &str, group: &str) {
        self.wake(tenant, queue, Some(group))
    }

    /// Whether a pop is parked that no registered group's wake reaches (a
    /// pinned pop never registers its group): apply then also wakes
    /// `(tenant, queue, None)` for every append. One atomic load per append.
    fn wants_append_wakes(&self) -> bool {
        false
    }

    /// A durable point covered `index` (§11.4 step 3). It bounds recovery
    /// replay and lets `LocalReplicator` truncate its log behind it. Never
    /// called for a durable point that failed.
    fn durable(&self, index: u64);

    /// Whether anyone listens for [`Notify::appended`]. Read once per entry;
    /// apply collects nothing for an entry when it is `false`.
    fn wants_appended(&self) -> bool {
        false
    }

    /// Records were appended to `(tenant, queue, partition)` on this node:
    /// the per-PARTITION wake a Kafka Fetch parks on. Unlike [`Notify::wake`]
    /// it does not depend on any native consumer group being subscribed.
    fn appended(&self, _tenant: &str, _queue: &str, _partition: &str) {}

    /// Apply refused the entry `failure` describes, and this node stops here
    /// (§12.1 Fatal). Called once, on the apply thread, before it returns the
    /// error: the replicator keeps it for `/health` and for the next boot's
    /// `QUEEN_RAFT_APPLY_SKIP` checks. It must not block (a file write of a
    /// few hundred bytes is the most it may do).
    fn failed(&self, _failure: &ApplyFailure) {}

    /// The consumption engine of the facade this apply feeds
    /// ([`crate::rsm::consume`]): apply hands it every append and catalog
    /// effect. Resolved once when apply starts; `None` when there is no
    /// engine (boot before the seam, most apply tests).
    fn engine(&self) -> Option<Arc<crate::rsm::consume::Engine>> {
        None
    }
}

/// Why apply stopped this node at one entry, as the operator needs it: which
/// entry (index, term, digest), which effect of which command, and whether
/// the same entry stops every node ([`ApplyError::deterministic`]).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyFailure {
    pub index: u64,
    pub term: u64,
    /// [`crate::rsm::entry::entry_digest`] of the entry, 16 hex digits: the
    /// same on every node that holds it, whatever form it is held in.
    pub digest: String,
    /// `deterministic`: a logic or invariant refusal, the same on every node
    /// that applies this entry (the hatch, `QUEEN_RAFT_APPLY_SKIP`, is for
    /// these). `node-local`: I/O, a full disk or map, this node's files — the
    /// other nodes apply the entry, and only this node needs repair.
    pub class: String,
    /// The effect that failed, `#<ordinal> <kind>`; `None` when the entry was
    /// refused before any effect ran (a gate: validation, clock, bases).
    pub effect: Option<String>,
    /// The command that effect belongs to: its request id (hex) and outcome.
    pub command: Option<String>,
    pub error: String,
}

impl ApplyFailure {
    pub fn deterministic(&self) -> bool {
        self.class == "deterministic"
    }

    /// The value of `QUEEN_RAFT_APPLY_SKIP` that steps over this entry (in
    /// raft group 0; group `g` prefixes it with `g<g>/`).
    pub fn skip_value(&self) -> String {
        format!("{}:{}", self.index, self.digest)
    }
}

/// `meta` prefix of `apply_skipped/<index, u64 BE>` -> JSON: an entry this
/// node applied as a skip marker (an operator's `QUEEN_RAFT_APPLY_SKIP`,
/// `replicator/raft/repair.rs`). NODE-LOCAL like the qlog tails: out of the
/// §12.9 digest. What `/health` lists, and what tells the next boot that this
/// node skipped the entry rather than executed it.
pub const APPLY_SKIPPED_PREFIX: &[u8] = b"apply_skipped/";

/// The [`APPLY_SKIPPED_PREFIX`] key of entry `index`.
pub fn apply_skipped_key(index: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(APPLY_SKIPPED_PREFIX.len() + 8);
    k.extend_from_slice(APPLY_SKIPPED_PREFIX);
    k.extend_from_slice(&index.to_be_bytes());
    k
}

/// Every skip marker this store applied: `(index, term)`, ascending.
pub fn skipped_entries<R: Reads + ?Sized>(
    reads: &R,
) -> std::result::Result<Vec<(u64, u64)>, StoreError> {
    let mut out = Vec::new();
    reads.scan_raw(
        Keyspace::Meta,
        APPLY_SKIPPED_PREFIX,
        APPLY_SKIPPED_PREFIX,
        usize::MAX,
        &mut |k, v| {
            if let Ok(b) = <[u8; 8]>::try_from(&k[APPLY_SKIPPED_PREFIX.len()..]) {
                let term = serde_json::from_slice::<serde_json::Value>(v)
                    .ok()
                    .and_then(|j| j.get("term").and_then(serde_json::Value::as_u64))
                    .unwrap_or(0);
                out.push((u64::from_be_bytes(b), term));
            }
            true
        },
    )?;
    Ok(out)
}

/// Is `e` a skip marker: an entry with no command and no effect whose header
/// is not all zeros? The batcher never proposes an entry without a command, and
/// openraft's own entries reach apply as [`Entry::noop`] (all zeros), so the
/// only such entry is the one `QUEEN_RAFT_APPLY_SKIP` put in place of a
/// skipped one: its header carries the skipped entry's clock and the id
/// bases after it (`replicator/raft/repair.rs`).
pub fn is_skip_marker(e: &Entry) -> bool {
    e.commands.is_empty()
        && e.effects.is_empty()
        && (e.now_us != 0 || e.pid_base != 0 || e.kv_version_base != 0)
}

/// The marker that stands in for a skipped entry: no command, no effect, the
/// skipped entry's clock, and the partition-id and KV-version bases AFTER it —
/// the ids it reserved are burned, never reused, so every entry planned after
/// it keeps its bases (I18) and applies.
pub fn skip_marker_of(e: &Entry) -> Entry {
    let (mut pids, mut kvs) = (0u64, 0u64);
    for eff in &e.effects {
        match eff.assigns() {
            Assigns::Pid(_) => pids += 1,
            Assigns::KvVersion(_) => kvs += 1,
            Assigns::Nothing => {}
        }
    }
    Entry::new(
        e.now_us.max(1),
        e.pid_base.saturating_add(pids),
        e.kv_version_base.saturating_add(kvs),
    )
}

/// The notifier of a node with nothing attached yet (phase 1 tests, embedded
/// boot before the seam of WP-1.7).
pub struct NoNotify;

impl Notify for NoNotify {
    fn applied(&self, _index: u64, _term: u64, _commands: &[CommandRecord]) {}
    fn wake(&self, _tenant: &str, _queue: &str, _group: Option<&str>) {}
    fn durable(&self, _index: u64) {}
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// The node-local cadences of §11.3, §11.4 and §11.7.
///
/// Every field decides WHEN this node moves bytes. None of them can change
/// what the state holds, which is why they may be knobs at all (I2): a bound
/// that changes state contents is a constant — see [`REQUEST_EXPIRE_LIMIT`].
#[derive(Clone, Copy, Debug)]
pub struct ApplyConfig {
    /// `QUEEN_RAFT_STORE_COMMIT_MS` (§11.3).
    pub store_commit_ms: u64,
    /// `QUEEN_RAFT_STORE_COMMIT_ENTRIES` (§11.3).
    pub store_commit_entries: u64,
    /// `QUEEN_RAFT_DURABLE_EVERY_MS` (§11.4).
    pub durable_every_ms: u64,
    /// `QUEEN_RAFT_DURABLE_EVERY_BYTES` (§11.4).
    pub durable_every_bytes: u64,
    /// Files unlinked per maintenance pass (§11.7).
    pub gc_per_pass: usize,
    /// How long the loop parks on an empty channel before running the
    /// maintenance step. Never longer than the store-commit cadence, or an
    /// idle node would hold the last entry uncommitted.
    pub idle_tick_ms: u64,
    /// `QUEEN_RAFT_DURABLE_ASYNC` (§11.4). On (default): a helper thread pushes
    /// the segment files' dirty pages to the device between durable points, so
    /// the point on the apply thread has little left to flush and blocks the
    /// pipeline for less. Off: the durable point flushes everything inline, the
    /// pre-WP behaviour. It is a pure pre-flush warm-up — it changes neither what
    /// a durable point records nor recovery (I11) — so toggling it is safe on a
    /// live node and never affects correctness, only the tail.
    pub durable_async: bool,
    /// `QUEEN_RAFT_CHECKPOINT_ASYNC` (§11.4, Phase C). On (default): the store
    /// half of a durable point runs on a checkpoint thread. Apply prepares the
    /// point as always and takes the store's CUT (every RAM row changed since
    /// the last point, one reference count each) and carries on; the thread
    /// writes the cut into LMDB, commits and syncs it, and only THEN is the
    /// point's index reported durable (step 3) and are the files it stopped
    /// naming unlinked (I10). One point in flight at a time. Off: the whole
    /// point runs inline on the apply thread — measured at 46-75 ms once a
    /// second at 300k msg/s, during which nothing is applied. LMDB holds one
    /// consistent checkpoint per point either way, and recovery is unchanged
    /// (it reopens at the last point that completed), so toggling it is safe on
    /// a live node.
    pub checkpoint_async: bool,
    /// `QUEEN_RAFT_SEG_BUFFERED` (PERF-C, §11.3). On (default): the apply thread
    /// buffers an entry's frames per file and writes each file's whole run with
    /// ONE `write` at the end of the entry, instead of one `write` per message.
    /// Off: the pre-PERF-C path, one `write` per append. Positions, bytes and
    /// recorded lengths are byte-identical either way (I2/I11); only the number
    /// of `write` syscalls changes, so toggling it is safe on a live node.
    pub seg_buffered: bool,
    /// `QUEEN_RAFT_APPLY_WRITERS` (PERF-C, §11.3). The size of the segment-write
    /// pool that flushes those per-file runs off the apply thread (each bucket
    /// owned by one writer). 0 = the apply thread writes them itself. Implies
    /// `seg_buffered`. Default `min(4, cores/2)`.
    pub apply_writers: usize,
    /// `QUEEN_RAFT_BATCH_COUNTERS` (PERF-D, §6.4/D16). On (default): every
    /// counter bump and stamp an entry makes is accumulated in a per-transaction
    /// RAM map and written to the store ONCE per key at commit (and before a
    /// durable point, a digest, or any counter read through the store), instead
    /// of a read-modify-write per bump. It is TRANSPARENT (I2): the committed
    /// counter rows after any entry boundary are identical to the per-bump path
    /// — additive counters fold by sum, stamps by max — so the digest is
    /// unchanged and toggling it is safe on a live node. Off: today's per-bump
    /// read-modify-write. Also gates the per-transaction group-list cache that
    /// serves the append path's `scan_groups` (same lifetime, same transparency).
    pub batch_counters: bool,
    /// `QUEEN_RAFT_QLOG` (`ALICE_PGLESS_NEWARCH.md` Phase A1 write / A2 read).
    /// OFF (default): exactly today's behaviour — apply keeps no per-queue log
    /// and touches no `qlog/` directory. ON: apply writes every `Append` into its
    /// queue's log (`rsm/qlog`), fsynced at each store commit and durable point
    /// (A2 flushes it BEFORE the store commit, so a committed offset is always
    /// already in the qlog); AND the pop payload read and the
    /// `DEDUP_INDEX=segment` dedup authority read FROM the qlog instead of the
    /// `.seg` files and the LMDB `txns`/`dedup` keyspaces (`Applier::qlog_reader`,
    /// threaded to the facade + planner). The segments and the raft-log blob are
    /// STILL written (removed in A3), so turning it on changes nothing the
    /// replicated digest can see (it only adds files under `qlog/`) and off-vs-on
    /// is behaviourally identical — the segment/txns read and the qlog read
    /// return byte-identical bytes and verdicts (the read-match test).
    ///
    /// A3b (`ALICE_PGLESS_NEWARCH.md` §5): with the knob on, apply no longer
    /// files the payload into a `.seg` (the double-write is gone); it does the
    /// message METADATA only and reads the payload from the qlog. WHO writes the
    /// qlog then depends on [`ApplyConfig::qlog_writer_external`].
    pub qlog: bool,
    /// A3b: `true` when an EXTERNAL writer (the `LocalReplicator` log writer)
    /// owns the qlog — it writes each payload and fsyncs it BEFORE the referencing
    /// raft-log entry, the durability ordering apply itself cannot provide
    /// (apply runs AFTER the entry is durable). Then `Applier::open` opens no
    /// qlog of its own and apply's `append`/`execute`/commit qlog sites are all
    /// skipped; apply still records `meta::QLOG_DURABLE_INDEX` (from the highest
    /// applied `Append` index) so recovery reconciles against it.
    ///
    /// `false` (the default) keeps the A1/A2/A3a behaviour: apply opens and
    /// writes the qlog itself (fsync at the store-commit cadence). This is what
    /// the unit tests drive — they build entries and apply them without a
    /// replicator writer — and it exercises the SAME record format and read path
    /// the external writer produces. The live single-node path (the
    /// `LocalReplicator`) sets this `true`; the external writer's ordering is
    /// proven by the crash matrix, not the unit apply loop.
    pub qlog_writer_external: bool,
    /// `QUEEN_RAFT_APPLY_SHARDS`: how many shards execute an entry's
    /// PID-KEYED effects (appends, cursors, watermarks, fresh dead letters,
    /// partition creates, stream state) — `pid % shards` picks one, and every
    /// other effect runs on the apply thread between the runs of them
    /// (`apply_shard.rs` has the argument). 1 is the one-thread path, every
    /// effect in entry order on the apply thread. Node-local: the replicated
    /// state is byte-identical at every value (the differential tests), so
    /// nodes of one cluster may differ and a node may change it across a
    /// restart. Default 4 from the environment; 1 in the programmatic default,
    /// so unit tests take the one-thread path unless they ask.
    pub apply_shards: usize,
    /// `QUEEN_RAFT_APPLY_SHARD_MIN` (default 8): an entry with fewer
    /// pid-keyed effects than this is applied on the apply thread alone, in
    /// entry order, as with one shard; so is a run of fewer. A hand-off to a
    /// sleeping shard thread costs a wake-up (~5-50 µs), which a handful of
    /// 15 µs effects does not repay. Node-local, like the shard count.
    pub apply_shard_min: usize,
}

impl Default for ApplyConfig {
    fn default() -> ApplyConfig {
        ApplyConfig {
            store_commit_ms: 4,
            store_commit_entries: 256,
            durable_every_ms: 1000,
            durable_every_bytes: 256 << 20,
            gc_per_pass: 32,
            idle_tick_ms: 2,
            durable_async: true,
            checkpoint_async: true,
            // Buffering is the safe, high-value half of PERF-C and is on by
            // default; the pool is off in the programmatic default so unit tests
            // spawn no writer threads. `from_env` (the shipped binary) turns the
            // pool on.
            seg_buffered: true,
            apply_writers: 0,
            // PERF-D: counter batching is transparent, so it is on everywhere
            // (unit tests included) — a test that reads a counter after a commit
            // sees the same value either way.
            batch_counters: true,
            // Phase C: the per-queue logs ARE the WAL — on by default
            // (`QUEEN_RAFT_QLOG=0` keeps the pre-qlog raft-log path).
            qlog: true,
            // A3b: apply owns the qlog by default (the unit-test path); the
            // `LocalReplicator` sets this true so the WRITER owns it.
            qlog_writer_external: false,
            // One shard: unit tests take the one-thread path unless they ask;
            // `from_env` (the shipped binary) defaults to four.
            apply_shards: 1,
            apply_shard_min: 8,
        }
    }
}

impl ApplyConfig {
    /// Resolve from the environment. Called ONCE at boot by the storage seam
    /// (WP-1.7), never from the apply loop.
    // Boot-only, and node-local: see the struct's doc. The apply loop is
    // handed the resolved value and reads nothing (I2).
    #[allow(clippy::disallowed_methods)]
    pub fn from_env() -> ApplyConfig {
        fn num(name: &str, cur: u64) -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(cur)
        }
        // A boolean knob: only "0" turns it off, so an unset or malformed value
        // keeps the default-on async pre-flush.
        fn flag(name: &str, cur: bool) -> bool {
            std::env::var(name).ok().map(|v| v != "0").unwrap_or(cur)
        }
        // The pool default: min(4, cores/2). A boot-time read of the machine's
        // parallelism (node-local, not apply state — I2).
        fn default_apply_writers() -> usize {
            let cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            (cores / 2).min(4)
        }
        let d = ApplyConfig::default();
        ApplyConfig {
            store_commit_ms: num("QUEEN_RAFT_STORE_COMMIT_MS", d.store_commit_ms),
            store_commit_entries: num("QUEEN_RAFT_STORE_COMMIT_ENTRIES", d.store_commit_entries),
            durable_every_ms: num("QUEEN_RAFT_DURABLE_EVERY_MS", d.durable_every_ms),
            durable_every_bytes: num("QUEEN_RAFT_DURABLE_EVERY_BYTES", d.durable_every_bytes),
            gc_per_pass: d.gc_per_pass,
            idle_tick_ms: d.idle_tick_ms,
            durable_async: flag("QUEEN_RAFT_DURABLE_ASYNC", d.durable_async),
            checkpoint_async: flag("QUEEN_RAFT_CHECKPOINT_ASYNC", d.checkpoint_async),
            seg_buffered: flag("QUEEN_RAFT_SEG_BUFFERED", d.seg_buffered),
            apply_writers: std::env::var("QUEEN_RAFT_APPLY_WRITERS")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or_else(default_apply_writers),
            batch_counters: flag("QUEEN_RAFT_BATCH_COUNTERS", d.batch_counters),
            qlog: flag("QUEEN_RAFT_QLOG", d.qlog),
            // Set by the `LocalReplicator` boot, never from the environment: the
            // storage seam decides who owns the qlog, not a knob.
            qlog_writer_external: d.qlog_writer_external,
            // Four shards unless told otherwise; capped at 64 (a shard is a
            // thread and a counter overlay, and past the cores it only adds
            // hand-offs).
            apply_shards: std::env::var("QUEEN_RAFT_APPLY_SHARDS")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(4)
                .min(64),
            apply_shard_min: num("QUEEN_RAFT_APPLY_SHARD_MIN", d.apply_shard_min as u64) as usize,
        }
    }
}

// ---------------------------------------------------------------------------
// What the caller sends and gets back
// ---------------------------------------------------------------------------

/// One committed entry, as the replicator hands it over (§12.1). The entry is
/// shared with the replicator's log: handing it over copies nothing.
#[derive(Clone, Debug)]
pub struct Committed {
    pub index: u64,
    pub term: u64,
    pub entry: Arc<Entry>,
}

/// The bounded channel between the replicator and the apply thread (§3.3).
pub fn channel(capacity: usize) -> (SyncSender<Committed>, Receiver<Committed>) {
    std::sync::mpsc::sync_channel(capacity.max(1))
}

/// What one [`Applier::apply`] call did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Applied {
    /// The entry was executed.
    Executed { effects: usize },
    /// The entry is at or below the applied index the store reopened with:
    /// already in state, so nothing was done. This is apply's whole
    /// idempotence (see the module header).
    Skipped,
}

/// Counts, for `/metrics` and for tests. No timings: there is no clock here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ApplyStats {
    pub entries: u64,
    pub skipped: u64,
    pub effects: u64,
    pub appends: u64,
    pub messages: u64,
    pub bytes_appended: u64,
    pub commits: u64,
    pub durable_points: u64,
    pub durable_points_failed: u64,
    pub outcomes_recorded: u64,
    pub wakes: u64,
    pub files_unlinked: u64,
    /// Unlinks a pin deferred to a later durable point (§11.7, I4).
    pub gc_deferred: u64,
    pub rows_swept: u64,
    /// Effects that named a row that was not there and were therefore a
    /// no-op: a delete of something already deleted. Counted rather than
    /// refused, because deletes are idempotent.
    pub missing_rows: u64,
    /// Skip markers applied: entries an operator stepped over
    /// (`QUEEN_RAFT_APPLY_SKIP`).
    pub skipped_by_operator: u64,
}

impl ApplyStats {
    /// Field by field, the sum of two counts (the coordinator's and each
    /// shard's make the applier's).
    fn plus(mut self, o: &ApplyStats) -> ApplyStats {
        self.entries += o.entries;
        self.skipped += o.skipped;
        self.effects += o.effects;
        self.appends += o.appends;
        self.messages += o.messages;
        self.bytes_appended += o.bytes_appended;
        self.commits += o.commits;
        self.durable_points += o.durable_points;
        self.durable_points_failed += o.durable_points_failed;
        self.outcomes_recorded += o.outcomes_recorded;
        self.wakes += o.wakes;
        self.files_unlinked += o.files_unlinked;
        self.gc_deferred += o.gc_deferred;
        self.rows_swept += o.rows_swept;
        self.missing_rows += o.missing_rows;
        self.skipped_by_operator += o.skipped_by_operator;
        self
    }
}

/// What recovery found (§11.5).
#[derive(Clone, Debug)]
pub struct Recovered {
    /// `meta.applied_index` the store reopened with.
    pub applied_index: u64,
    pub applied_term: u64,
    /// `meta.durable_index`: the last point that reached the platter.
    pub durable_index: u64,
    /// The index the replicator replays AFTER (§11.5 step 5).
    ///
    /// It is the DURABLE index, not the applied one: apply skips whatever the
    /// store already holds, so replaying from the durable point costs a few
    /// no-ops and is exactly the single-voter repair path of §11.5 — the node
    /// re-applies its own log over the last state known to be on the platter.
    /// A replicator that can only replay after `applied_index` is equally
    /// correct.
    pub replay_after: u64,
    /// What the segment tree did: truncations, deletions, rebuilt indexes.
    pub segments: segments::Recovery,
}

// ---------------------------------------------------------------------------
// PERF-D: the per-transaction counter overlay and group-list cache
// ---------------------------------------------------------------------------

/// The per-transaction counter overlay (§6.4, D16, PERF-D).
///
/// Apply makes many counter bumps per entry — an `Append` alone touches the
/// partition, queue and tenant `pushed` and `retained_bytes`, the per-group
/// `pending`, and the push stamp — and today each one is a read-modify-write of
/// the store. Because the write transaction stays open ACROSS entries (§11.3),
/// those bumps re-read and re-write the same handful of counter rows over and
/// over inside one transaction. This overlay accumulates them in RAM and writes
/// each key ONCE, at [`CounterCache::flush`], which apply calls before every
/// commit and every durable point; a read in between folds the pending delta in
/// ([`CounterCache::fold`]), and a sweep drops it ([`CounterCache::forget_prefix`]).
///
/// I2 holds because the fold is EXACT: additive counters accumulate by sum,
/// stamps ("last time") by max, and a given key is only ever one or the other,
/// so the committed value after any entry boundary equals the per-bump path's,
/// whatever the commit cadence or the (irrelevant) hash-map flush order. With
/// `enabled == false` every call is the per-bump store path, unchanged.
#[derive(Debug, Default)]
struct CounterCache {
    enabled: bool,
    /// Additive deltas since the last flush, by counter key.
    adds: HashMap<Box<[u8]>, i64, crate::rsm::fasthash::FxBuild>,
    /// Stamp maxima since the last flush, by counter key.
    stamps: HashMap<Box<[u8]>, i64, crate::rsm::fasthash::FxBuild>,
}

impl CounterCache {
    fn new(enabled: bool) -> CounterCache {
        CounterCache {
            enabled,
            adds: HashMap::default(),
            stamps: HashMap::default(),
        }
    }

    /// Add `delta` to a counter: RAM when enabled, a store read-modify-write
    /// otherwise. A zero delta is a no-op either way (as `Applier::bump` was).
    fn add<W: Writes + ?Sized>(
        &mut self,
        writes: &mut W,
        key: &[u8],
        delta: i64,
    ) -> crate::rsm::store::Result<()> {
        if delta == 0 {
            return Ok(());
        }
        if self.enabled {
            // Look up by the borrowed key first: a key bumped again in the
            // same window (the common case) costs no allocation.
            match self.adds.get_mut(key) {
                Some(v) => *v += delta,
                None => {
                    self.adds.insert(Box::from(key), delta);
                }
            }
            Ok(())
        } else {
            writes.add_counter(key, delta).map(|_| ())
        }
    }

    /// Move a "latest time" stamp up to `at`: RAM max when enabled, the store
    /// read-compare-set otherwise. Monotone either way (D16, §6.4).
    fn stamp<W: Writes + ?Sized>(
        &mut self,
        writes: &mut W,
        key: &[u8],
        at: i64,
    ) -> crate::rsm::store::Result<()> {
        if self.enabled {
            match self.stamps.get_mut(key) {
                Some(e) => {
                    if at > *e {
                        *e = at;
                    }
                }
                None => {
                    self.stamps.insert(Box::from(key), at);
                }
            }
            Ok(())
        } else {
            if writes.counter_at(key)? < at {
                writes.set_counter(key, at)?;
            }
            Ok(())
        }
    }

    /// `base` (the committed row, or what other overlays already folded into
    /// it) with this overlay's pending delta or stamp. A key is only ever an
    /// additive counter or a stamp, so the fold is unambiguous, and folding
    /// several overlays in any order gives the one value a single overlay
    /// would (sharded apply keeps one per shard).
    fn fold(&self, base: i64, key: &[u8]) -> i64 {
        if !self.enabled {
            return base;
        }
        if let Some(s) = self.stamps.get(key) {
            return base.max(*s);
        }
        base + self.adds.get(key).copied().unwrap_or(0)
    }

    /// Drop every pending delta and stamp whose key starts with `prefix`: the
    /// store rows under it are about to be swept (a queue, group or partition
    /// delete), and a later flush must not recreate them — including a counter
    /// bumped this window that was never committed.
    fn forget_prefix(&mut self, prefix: &[u8]) {
        if !self.enabled {
            return;
        }
        self.adds.retain(|k, _| !k.starts_with(prefix));
        self.stamps.retain(|k, _| !k.starts_with(prefix));
    }

    /// Write every accumulated delta and stamp into the open transaction and
    /// empty the overlay. Called before a commit and a durable point (and so
    /// before the digest, which reads committed state). Per-key order does not
    /// affect the committed value, so the hash-map iteration order is immaterial
    /// (I2).
    fn flush<W: Writes + ?Sized>(&mut self, writes: &mut W) -> crate::rsm::store::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        // Every key the overlay holds got at least one non-zero bump (`add`
        // skips a zero one before it ever inserts), so the per-bump path had
        // WRITTEN it — creating the row on the first bump — even when the bumps
        // since cancel to a net zero. Writing it here too keeps the committed
        // ROWS identical (a `0` row is not an absent one to the §12.9 digest),
        // which is what I2 asks of the batched path.
        for (key, delta) in self.adds.drain() {
            writes.add_counter(&key, delta)?;
        }
        for (key, at) in self.stamps.drain() {
            if writes.counter_at(&key)? < at {
                writes.set_counter(&key, at)?;
            }
        }
        Ok(())
    }

    /// [`CounterCache::flush`] of the PARTITION-scope keys of shard `shard`'s
    /// pids only: the part a shard may write on its own thread while the
    /// other shards write theirs (the store's shard writers allow concurrent
    /// read-modify-writes of DIFFERENT keys only). A shard's overlay can hold
    /// another shard's partition key — a dead letter whose id was in use
    /// settles the OLD pid's gauge from the new pid's shard — and two shards
    /// flushing one key at once lose an update, so such a key, and every
    /// shared one, stays for [`CounterCache::flush`] on the write handle.
    /// Keeps the maps' capacity, as `drain` does, so the next window does not
    /// regrow them.
    fn flush_partition_scope<W: Writes + ?Sized>(
        &mut self,
        writes: &mut W,
        shard: usize,
        shards: usize,
    ) -> crate::rsm::store::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let ours = |k: &[u8]| {
            k.first() == Some(&(keys::CounterScope::Partition as u8))
                && k.get(1..9)
                    .and_then(|b| <[u8; 8]>::try_from(b).ok())
                    .is_some_and(|b| shard_of(u64::from_be_bytes(b), shards) == shard)
        };
        let mut err = None;
        self.adds.retain(|key, delta| {
            if !ours(key) {
                return true;
            }
            if err.is_none() {
                if let Err(e) = writes.add_counter(key, *delta) {
                    err = Some(e);
                }
            }
            false
        });
        self.stamps.retain(|key, at| {
            if !ours(key) {
                return true;
            }
            if err.is_none() {
                let r = writes.counter_at(key).and_then(|cur| {
                    if cur < *at {
                        writes.set_counter(key, *at)
                    } else {
                        Ok(())
                    }
                });
                if let Err(e) = r {
                    err = Some(e);
                }
            }
            false
        });
        err.map_or(Ok(()), Err)
    }

    /// Keys pending in the overlay.
    fn len(&self) -> usize {
        self.adds.len() + self.stamps.len()
    }
}

// ---------------------------------------------------------------------------
// The applier
// ---------------------------------------------------------------------------

/// The state machine. Owns the write transaction and the segment writer; holds
/// the store by reference, because the write handle borrows it.
pub struct Applier<'s, S: Store> {
    store: &'s S,
    writes: S::Write<'s>,
    segments: Segments,
    /// The SHADOW per-queue logs (`QUEEN_RAFT_QLOG`, Phase A1). `Some` only when
    /// the knob is on; `None` is today's path, and every qlog site below is
    /// then a single `is_some` check that does nothing. The applier owns it like
    /// it owns [`Applier::segments`]; it holds no threads, so dropping it (on
    /// shutdown, or when the applier is dropped) just closes the file handles.
    qlog: Option<QLogSet>,
    /// Node-local D17 history, shared with the facade for dashboard reads.
    local_metrics: Arc<crate::rsm::local_metrics::LocalMetrics>,
    cfg: ApplyConfig,
    notify: Arc<dyn Notify>,
    /// The consumption engine this apply feeds ([`Notify::engine`], resolved
    /// once at open): appends and catalog effects are reported to it.
    engine: Option<Arc<crate::rsm::consume::Engine>>,

    /// PERF-D: the counter overlay of the effects the apply thread executes
    /// itself (the global ones), §6.4. Each shard keeps its own
    /// ([`Shard::ctr`]); every read folds all of them, every sweep drops from
    /// all of them, and every commit and durable point flushes all of them.
    counters: CounterCache,
    /// The shards of `QUEEN_RAFT_APPLY_SHARDS` (one or more): each owns the
    /// counter overlay of its pids' effects, its queue cache (B41: groups,
    /// kept across commits and invalidated by every catalogue write) and the
    /// entry's wake counts. Shard `pid % shards` owns pid's.
    shards: Vec<Shard>,
    /// The pid-keyed effects of the entry waiting for the next global effect
    /// (sharded apply only; empty with one shard).
    run: shard::Run,
    /// The shard threads (`shards - 1` of them: the apply thread runs one
    /// shard of each run itself). `None` with one shard, and when the counter
    /// overlay is off (`QUEEN_RAFT_BATCH_COUNTERS=0`): then a shard would
    /// read-modify-write queue and tenant counters another shard shares, which
    /// the store's shard writers do not allow, so the runs execute one shard
    /// after another on the apply thread.
    pool: Option<shard::Pool>,
    /// The segment positions a run's pre-pass gave its `Append`s, by effect
    /// ordinal (reused).
    positions: Vec<Option<Position>>,
    /// Scratch: a counter key for the apply thread's own bumps.
    key: Vec<u8>,
    /// Scratch: one `request_ids` row (B08).
    req_row: Vec<u8>,

    applied_index: u64,
    applied_term: u64,
    durable_index: u64,
    next_pid: u64,
    kv_version_next: u64,
    last_now_us: i64,
    max_created_at_us: i64,
    /// A3b (`ALICE_PGLESS_NEWARCH.md` §5): the highest entry index that carried an
    /// `Append`. Recorded into `meta::QLOG_DURABLE_INDEX` at every commit /
    /// durable point when the qlog knob is on, so recovery reconciles the
    /// reopened qlog's durable tail against it (NA-QLOG-I1). With an external
    /// writer the qlog record of every applied `Append` is fsync'd BEFORE the
    /// entry reached apply, so it is durable by the time this index names it —
    /// which is exactly what the reconciliation needs. Seeded at open from the
    /// stored value so it never regresses over a reopened tail.
    last_append_index: u64,
    /// The writer's fsync'd queue-log tails, recorded per log at every durable
    /// point (`meta::qlog_tail_key`); `None` when apply owns the qlog (tests).
    qlog_tails: Option<Arc<crate::rsm::qlog::set::QlogTails>>,

    /// Entries executed since the last store commit (§11.3's second trigger).
    entries_since_commit: u64,
    /// Set when the open transaction holds anything at all.
    dirty: bool,
    /// Why this applier refuses to do anything else (see
    /// [`Applier::apply`]'s failure contract).
    poisoned: Option<String>,
    /// The effect of the entry being executed that refused, for the failure
    /// report ([`ApplyFailure`]).
    failed_effect: Option<u32>,

    /// Sealed files whose `partition_files` rows are already committed.
    /// Bounded by the files sealed since the last commit, not by the files
    /// this node holds: the pids themselves come from the file's own index
    /// ([`Segments::pids_in`]), never from a map this process built, so a file
    /// that sealed before a restart still loses its rows when it is collected.
    recorded_seals: BTreeSet<(u16, u32)>,
    /// Phase one of GC done (rows removed in a commit); the unlink waits for
    /// the next durable point (I10). Bounded by `cfg.gc_per_pass`.
    gc_staged: BTreeSet<(u16, u32)>,
    /// Staged files whose unlink a PIN refused (I4 beats I10, §11.7): their
    /// rows are gone and must stay gone, and every durable point tries again.
    ///
    /// Kept apart from [`Applier::gc_staged`] because the pass bound is a bound
    /// on WORK, and a deferred file is not work: re-staging it into the same 32
    /// slots at every durable point is how a handful of long-held claim pins
    /// stopped GC for every other file on the node. This list is bounded by the
    /// claims in flight — outstanding work, never retained volume (I8).
    gc_deferred: BTreeSet<(u16, u32)>,
    /// The async durable point (`ApplyConfig::checkpoint_async`): the index
    /// whose cut the checkpoint thread is writing, `None` when none is. At most
    /// one: the next point waits for it.
    ckpt_inflight: Option<u64>,
    /// GC phase two owed to the in-flight cut: the files staged or deferred
    /// when it was taken. Their rows are gone from that cut, so they are
    /// unlinked once it is durable and not before (I10).
    gc_inflight: BTreeSet<(u16, u32)>,

    stats: ApplyStats,
    /// Runs executed on the shard threads, and runs executed here one shard
    /// after another (too small, no pool, or a pre-pass refusal).
    runs: [u64; 2],
}

/// What every effect of the entry being executed shares.
#[derive(Clone, Copy)]
struct EntryCx {
    index: u64,
    now_us: i64,
    /// [`Notify::wants_appended`], read once per entry.
    wants_appended: bool,
    /// [`Notify::wants_append_wakes`], read once per entry.
    wants_append_wakes: bool,
}

/// How [`Applier::begin_checkpoint`] started a durable point.
pub enum PointStart {
    /// The store's cut, for the checkpoint thread, and the index it covers.
    Cut(CheckpointCut, u64),
    /// The store cannot cut: the point ran inline and this index is durable.
    Inline(u64),
}

impl<'s, S: Store> Applier<'s, S> {
    /// Recovery, §11.5 steps 2–5.
    ///
    /// The caller has taken the data directory's LOCK and read IDENTITY (step
    /// 1, WP-1.7) and has opened the store (step 2). This call:
    ///
    /// 2. reads the applied position the REOPENED state reports — never an
    ///    assumption about which commit the engine came back at, because with
    ///    `MDB_NOSYNC` it legitimately reopens PAST the last durable point;
    /// 3. hands the recorded file table to the segment tree, which truncates
    ///    every file to its recorded length, deletes files the state does not
    ///    know, and VERIFIES checksums. Exactly what is verified, because the
    ///    claim is load-bearing: every frame of an active file (it is
    ///    rescanned to rebuild its RAM index anyway); every frame of a sealed
    ///    file whose `.qidx` is missing, stale or damaged (it is rebuilt by
    ///    scanning); and, for a sealed file whose `.qidx` opens, the frames
    ///    between its last DURABLE length and its recorded length — the ones
    ///    §11.5 step 3 names, and the only ones no barrier has covered. Frames
    ///    below a durable point are NOT re-read, which is what keeps the cost
    ///    proportional to the change (I8). A file shorter than the record, a
    ///    missing file or a damaged frame is the I11 disagreement and comes
    ///    back as [`ApplyError::Disagreement`];
    /// 4. rebuilds the RAM derived structures of §6.3;
    /// 5. reports the index the replicator replays after.
    ///
    /// Repeating it after a crash at any step is safe: every step reads state
    /// and writes only what it recomputes.
    pub fn open(
        store: &'s S,
        seg_root: &Path,
        seg_opts: segments::Options,
        cfg: ApplyConfig,
        notify: Arc<dyn Notify>,
    ) -> Result<(Applier<'s, S>, Recovered)> {
        let (
            applied_index,
            applied_term,
            durable_index,
            next_pid,
            kv_version_next,
            last_now_us,
            max_created_at_us,
            qlog_durable_index,
            recorded,
        ) = store.read(|r| {
            let mut recorded: Vec<FileState> = Vec::new();
            r.scan_files(usize::MAX, &mut |bucket, file_id, row| {
                recorded.push(FileState {
                    bucket,
                    file_id,
                    len: row.len,
                    durable_len: row.durable_len,
                    sealed: row.sealed,
                    frames: row.frames,
                    retained_frames: row.retained_frames,
                    retained_bytes: row.retained_bytes,
                    window_frames: row.window_frames,
                    snapshot_refs: row.snapshot_refs,
                });
                true
            })?;
            Ok((
                r.applied_index()?,
                r.applied_term()?,
                r.durable_index()?,
                r.next_pid()?,
                r.kv_version_next()?,
                r.last_now_us()?,
                r.max_created_at_us()?,
                // A3a: the qlog-durable index the last commit recorded (0 when a
                // node never ran with `QUEEN_RAFT_QLOG` on). Node-local.
                r.meta_u64(meta::QLOG_DURABLE_INDEX)?.unwrap_or(0),
                recorded,
            ))
        })?;

        // Phase A1/A2: mirror the segment writer's roll size and fsync mode into
        // the qlog options (built BEFORE `seg_opts` is moved into
        // `Segments::open`), so the shadow rolls and fsyncs on the same terms as
        // the authoritative store. `None` unless `QUEEN_RAFT_QLOG` is on. A2:
        // REOPEN every existing `q<id>/` directory now (torn-tail truncate +
        // `.qidx` rebuild), so a reopened node serves reads from the qlog
        // immediately, before any new append — and so the lazy open on the first
        // flush is only ever a genuinely new queue, never a `create_new`
        // collision with a directory a previous run left.
        // A3b: with an EXTERNAL writer (the `LocalReplicator` log writer) the qlog
        // is opened + reconciled + owned by the BOOT thread, and the writer writes
        // it BEFORE the raft-log entry — so apply opens NO qlog of its own here
        // (a second `QLogSet` on the same directory would double-open it). The
        // A1/A2/A3a path (`qlog_writer_external == false`, the unit tests) still
        // opens + reconciles + writes it below.
        let qlog = if cfg.qlog && !cfg.qlog_writer_external {
            let opts = crate::rsm::qlog::QLogOptions {
                segment_bytes: seg_opts.segment_bytes,
                fsync: match seg_opts.fsync {
                    segments::FsyncMode::Full => crate::rsm::qlog::Fsync::Full,
                    segments::FsyncMode::Data => crate::rsm::qlog::Fsync::Data,
                },
            };
            // `<data_dir>/qlog`, sibling to `seg/`, `log/`, `store/`. `seg_root`
            // is `<data_dir>/seg`, so its parent is the data dir.
            let data_dir = seg_root
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."));
            let mut set = QLogSet::new(data_dir.join("qlog"), opts);
            let qlog_tail = set.reopen_all().map_err(ApplyError::Qlog)?;
            // A3a recovery cross-check (NA-QLOG-I1, `ALICE_PGLESS_NEWARCH.md`
            // §5). The qlog is fsync'd BEFORE each store commit records
            // `QLOG_DURABLE_INDEX` (`commit_inner`/`durable_point_inner`), so
            // every record the store counts as qlog-durable was on the platter
            // before that commit and MUST be present now. The reopened qlog's
            // durable tail is therefore AHEAD of or EQUAL to what the store
            // recorded — never behind. Behind means a committed record is missing
            // from the qlog: silent data loss once A3b makes the qlog the sole
            // payload store, so refuse to start rather than serve a hole. (The
            // qlog may be AHEAD — records for entries the store rolled back and
            // the raft log will replay, or an un-fsync'd tail a SIGKILL left in
            // the page cache; both are benign, see the `qlog/mod.rs` recovery
            // note.) This is a watermark check, not a per-record scan: the exact
            // per-record proof is the coordinator's VM difffuzz.
            if qlog_tail < qlog_durable_index {
                return Err(ApplyError::Disagreement {
                    detail: format!(
                        "qlog durable tail seq {qlog_tail} is BEHIND the store's \
                         recorded qlog-durable index {qlog_durable_index}: a committed \
                         record is missing from the qlog (NA-QLOG-I1)"
                    ),
                });
            }
            tracing::info!(
                target: "rsm",
                qlog_tail,
                qlog_durable_index,
                "rsm qlog recovery reconciled (tail ≥ store's qlog-durable index)",
            );
            Some(set)
        } else {
            None
        };
        let (mut segments, seg_recovery) = Segments::open(seg_root, seg_opts, &recorded)?;
        // PERF-C: turn on write coalescing and the write pool for this node.
        // Boot-only and node-local (I2); the pool's threads live and die with
        // the segment writer, which the apply thread owns.
        segments.configure_writes(cfg.seg_buffered, cfg.apply_writers);
        // STORAGE_V2 Lever 2: when this node serves the committed dedup authority
        // from the segments (`DEDUP_INDEX=segment`), keep each active frame's
        // hash list in the active index's RAM so the planner's committed dedup
        // scan reads it without a disk hit. The mode is pinned by the boot seam
        // (`real_builder`) before this open, so `record_index_mode()` is
        // authoritative here; the default modes leave it off (no RAM cost).
        if dedup::record_index_mode() == dedup::IndexMode::Segment {
            segments.retain_active_hashes(true);
        }
        let data_dir = seg_root
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let local_metrics = crate::rsm::local_metrics::open(data_dir.join("local.db"))
            .map_err(ApplyError::LocalMetrics)?;
        let nshards = cfg.apply_shards.max(1);
        let shards: Vec<Shard> = (0..nshards)
            .map(|_| Shard::new(cfg.batch_counters, max_created_at_us))
            .collect();
        // A store that cannot hand out shard writers (one with a keyspace
        // outside RAM) runs every run on this thread; probed once here, so a
        // refusal later is the store's own failure, not a missing feature.
        let writers_ok = match store.shard_writer() {
            Ok(w) => {
                drop(w);
                true
            }
            Err(e) => {
                if nshards > 1 {
                    tracing::warn!(
                        target: "rsm",
                        error = %e,
                        "apply: the store hands out no shard writers; runs execute on the apply thread",
                    );
                }
                false
            }
        };
        let pool = if nshards > 1 && cfg.batch_counters && writers_ok {
            match shard::Pool::new(nshards - 1) {
                Ok(p) => Some(p),
                Err(e) => {
                    // Node-local and not a correctness matter: the runs then
                    // execute one shard after another on this thread.
                    tracing::warn!(
                        target: "rsm",
                        error = %e,
                        shards = nshards,
                        "apply: the shard threads could not be started; runs execute on the apply thread",
                    );
                    None
                }
            }
        } else {
            None
        };
        let writes = store.write()?;

        let applier = Applier {
            store,
            writes,
            segments,
            qlog,
            local_metrics,
            counters: CounterCache::new(cfg.batch_counters),
            shards,
            run: shard::Run::new(nshards),
            pool,
            positions: Vec::new(),
            key: Vec::with_capacity(64),
            req_row: Vec::with_capacity(256),
            cfg,
            engine: notify.engine(),
            notify,
            applied_index,
            applied_term,
            durable_index,
            next_pid,
            kv_version_next,
            last_now_us,
            max_created_at_us,
            last_append_index: qlog_durable_index,
            qlog_tails: None,
            entries_since_commit: 0,
            dirty: false,
            poisoned: None,
            failed_effect: None,
            recorded_seals: BTreeSet::new(),
            gc_staged: BTreeSet::new(),
            gc_deferred: BTreeSet::new(),
            ckpt_inflight: None,
            gc_inflight: BTreeSet::new(),
            stats: ApplyStats::default(),
            runs: [0; 2],
        };
        let rec = Recovered {
            applied_index,
            applied_term,
            durable_index,
            replay_after: durable_index,
            segments: seg_recovery,
        };
        tracing::info!(
            target: "rsm",
            applied = applied_index,
            term = applied_term,
            durable = durable_index,
            truncated = rec.segments.truncated.len(),
            deleted = rec.segments.deleted.len(),
            rebuilt = rec.segments.rebuilt.len(),
            rescanned = rec.segments.rescanned.len(),
            verified = rec.segments.verified.len(),
            scanned_frames = rec.segments.scanned_frames,
            shards = applier.shards.len(),
            shard_threads = applier.pool.as_ref().map_or(0, |p| p.workers()),
            "rsm apply recovered",
        );
        Ok((applier, rec))
    }

    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }

    pub fn applied_term(&self) -> u64 {
        self.applied_term
    }

    pub fn durable_index(&self) -> u64 {
        self.durable_index
    }

    pub fn stats(&self) -> ApplyStats {
        self.shards
            .iter()
            .fold(self.stats, |acc, s| acc.plus(&s.stats))
    }

    /// How many shards execute the pid-keyed effects (`QUEEN_RAFT_APPLY_SHARDS`).
    pub fn shards(&self) -> usize {
        self.shards.len()
    }

    /// `(runs executed on the shard threads, runs executed on this thread)`.
    pub fn run_counts(&self) -> (u64, u64) {
        (self.runs[0], self.runs[1])
    }

    /// The read side of the segment files, for the blocking pool (§9.4). A
    /// reader is cloneable and holds no write state.
    pub fn reader(&self) -> segments::Reader {
        self.segments.reader()
    }

    /// The read side of the per-queue logs (`QUEEN_RAFT_QLOG`, Phase A2), for the
    /// facade's pop payload read and the planner's `DEDUP_INDEX=segment` dedup
    /// read. `None` when the knob is off (today's path). Cloneable and holds no
    /// write state; it shares the applier's live logs, so it sees every append
    /// the moment [`QLogSet::flush`] lands it.
    pub fn qlog_reader(&self) -> Option<crate::rsm::qlog::set::QLogReader> {
        self.qlog.as_ref().map(|s| s.reader())
    }

    /// The segment writer, for a caller that owns a step apply does not: a
    /// snapshot build sealing every bucket (§11.6), the crash matrix.
    pub fn segments_mut(&mut self) -> &mut Segments {
        &mut self.segments
    }

    /// The segment tree, read-only. The apply loop reads it to drive the async
    /// durable-point pre-flush (§11.4); it mutates nothing.
    pub fn segments(&self) -> &Segments {
        &self.segments
    }

    /// The SHADOW per-queue logs (`QUEEN_RAFT_QLOG`, Phase A1), for the A1 match
    /// test to read a record back. `None` unless the knob is on. Test-only: no
    /// product code reads the shadow set until A2.
    #[cfg(test)]
    pub(crate) fn qlog(&self) -> Option<&QLogSet> {
        self.qlog.as_ref()
    }

    // -- the entry ---------------------------------------------------------

    /// Execute one committed entry, or skip it if the store already holds it.
    ///
    /// The order is fixed and each step exists for a reason:
    /// validate (I16) → skip or refuse a gap → assert the bases (I18) → the
    /// effects in apply order → the outcomes (§5.4) → `meta` → resolve the
    /// waiters (I4) → the wakes.
    ///
    /// # On failure
    ///
    /// There is NO partial undo. An entry that fails half way leaves its
    /// prefix in the open store transaction, in the segment files and in the
    /// derived RAM, so this applier poisons itself: every later `apply`,
    /// `commit`, `durable_point`, `gc_pass` and `flush` answers
    /// [`ApplyError::Poisoned`] and the open transaction is never committed.
    /// Dropping the applier aborts it, and the node repairs by replaying its
    /// log from the last durable point (§11.5) — the only state the prefix
    /// never reached. Every refusal is fatal ([`ApplyError::fatal`]); nothing
    /// here is ever retried in place.
    pub fn apply(&mut self, c: &Committed) -> Result<Applied> {
        self.usable()?;
        // The gates run before a single row is written, so a refusal from one
        // of them rejects the entry rather than half executing it: the node
        // still stops (every [`ApplyError`] is fatal), but this applier is
        // consistent and a test — or a crash matrix step — can carry on with
        // it. Everything after them can leave a prefix, and poisons.
        match self.gates(c) {
            Ok(Some(done)) => return Ok(done),
            Ok(None) => {}
            // A gate refusal (I5/I16/I18) rejects the entry before any write,
            // so — unlike `execute` — it does NOT poison this applier: it stays
            // consistent and an in-process test (or a crash-matrix step) can
            // carry on with it. But it IS fatal for the node (the apply thread
            // returns `Err` and stops), so log the reason here. Without this the
            // only log the operator sees is the replicator's later "apply thread
            // gone" (`local.rs`), never the I5/gap/bases cause (WP-1.11
            // diagnosability finding).
            Err(e) => {
                let e = self.refused(e);
                self.report(c, &e, None);
                return Err(e);
            }
        }
        self.failed_effect = None;
        // A reader answering from several rows sees this entry whole or not at
        // all ([`crate::rsm::store::EntryGate`]).
        let store: &'s S = self.store;
        let _whole = store.entry_gate().entry();
        match self.execute(c) {
            Ok(a) => Ok(a),
            Err(e) => {
                let first = self.poisoned.is_none();
                let e = self.poison(e);
                if first {
                    let at = self.failed_effect.take();
                    self.report(c, &e, at);
                }
                Err(e)
            }
        }
    }

    /// The KV calls of `c` a receiver in this process waits on
    /// ([`crate::rsm::kv_reads`]), keyed by the ordinal of each one's last
    /// effect. Empty (one atomic load) when nobody waits.
    fn kv_render_points(&self, c: &Committed) -> HashMap<u32, Vec<usize>> {
        let reads = crate::rsm::kv_reads::global();
        let mut at: HashMap<u32, Vec<usize>> = HashMap::new();
        if !reads.any_waiting() {
            return at;
        }
        for (i, cmd) in c.entry.commands.iter().enumerate() {
            if matches!(cmd.outcome, Outcome::Kv(_))
                && cmd.effect_count > 0
                && reads.call(&cmd.request_id).is_some()
            {
                at.entry(cmd.first_effect + cmd.effect_count - 1)
                    .or_default()
                    .push(i);
            }
        }
        at
    }

    /// Render the answers of `cmds` (indexes into `c.entry.commands`) against
    /// the state as it stands now — after their own effects and every earlier
    /// command's, before any later one's — at the entry's instant (D5).
    fn render_kv_reads(&self, c: &Committed, cmds: &[usize]) {
        let reads = crate::rsm::kv_reads::global();
        for &i in cmds {
            let cmd = &c.entry.commands[i];
            let Outcome::Kv(o) = &cmd.outcome else {
                continue;
            };
            let Some((tenant, ops)) = reads.call(&cmd.request_id) else {
                continue;
            };
            let answer = crate::rsm::planner::kv::render_call(
                &self.writes,
                &tenant,
                &ops,
                &o.results,
                c.entry.now_us,
            )
            .map_err(|e| e.to_string());
            reads.answer(&cmd.request_id, answer);
        }
    }

    /// Tell the operator exactly what stopped this node: the entry (index,
    /// term, digest), the effect and its command, whether every node stops on
    /// it, and what to set if so. One error line, then [`Notify::failed`].
    fn report(&self, c: &Committed, e: &ApplyError, effect: Option<u32>) {
        let digest = format!("{:016x}", crate::rsm::entry::entry_digest(&c.entry));
        let eff = effect.and_then(|ord| {
            c.entry
                .effects
                .get(ord as usize)
                .map(|x| format!("#{ord} {}", x.kind().name()))
        });
        let cmd = effect.and_then(|ord| {
            c.entry
                .commands
                .iter()
                .find(|k| k.first_effect <= ord && ord < k.first_effect + k.effect_count)
                .map(|k| {
                    let id: String = k.request_id.iter().map(|b| format!("{b:02x}")).collect();
                    format!("request {id} ({})", outcome_name(&k.outcome))
                })
        });
        let failure = ApplyFailure {
            index: c.index,
            term: c.term,
            digest,
            class: if e.deterministic() {
                "deterministic"
            } else {
                "node-local"
            }
            .to_string(),
            effect: eff,
            command: cmd,
            error: e.to_string(),
        };
        if failure.deterministic() {
            tracing::error!(
                target: "rsm",
                index = failure.index,
                term = failure.term,
                digest = %failure.digest,
                effect = failure.effect.as_deref().unwrap_or("-"),
                command = failure.command.as_deref().unwrap_or("-"),
                error = %failure.error,
                "APPLY FAILED at entry {} (term {}), a DETERMINISTIC refusal: every node that \
                 applies this entry stops on it, and a restart replays it and stops again. To \
                 step over it without a code change, set QUEEN_RAFT_APPLY_SKIP={} (prefix `g<N>/` \
                 for raft group N > 0) on EVERY node of the cluster, stopped and new ones \
                 included, and restart them: the entry becomes a no-op everywhere, none of its \
                 effects apply, and its commands were never answered. Entries planned on top of \
                 it may fail next; each is reported the same way",
                failure.index,
                failure.term,
                failure.skip_value(),
            );
        } else {
            tracing::error!(
                target: "rsm",
                index = failure.index,
                term = failure.term,
                digest = %failure.digest,
                effect = failure.effect.as_deref().unwrap_or("-"),
                error = %failure.error,
                "APPLY FAILED at entry {} (term {}), a NODE-LOCAL failure (I/O, space, or this \
                 node's own files): the other nodes apply this entry. Do NOT set \
                 QUEEN_RAFT_APPLY_SKIP for it. Repair this node (disk, space, permissions) and \
                 restart it, or wipe its data directory and let it rejoin from a peer",
                failure.index,
                failure.term,
            );
        }
        self.notify.failed(&failure);
    }

    /// Log a gate refusal that stops this node here (§12.1 Fatal) and hand it
    /// back unchanged. Unlike [`Self::poison`] it does not mark the applier
    /// poisoned, because a gate refuses before any write and leaves the applier
    /// consistent (see [`Self::apply`]).
    fn refused(&self, e: ApplyError) -> ApplyError {
        tracing::error!(
            target: "rsm",
            error = %e,
            applied = self.applied_index,
            durable = self.durable_index,
            "apply refused at the gate: this node stops here (§12.1 Fatal)",
        );
        e
    }

    /// The applier is usable: no earlier refusal.
    fn usable(&self) -> Result<()> {
        match &self.poisoned {
            Some(after) => Err(ApplyError::Poisoned {
                after: after.clone(),
            }),
            None => Ok(()),
        }
    }

    /// Record the refusal that stops this node, and hand it back unchanged.
    fn poison(&mut self, e: ApplyError) -> ApplyError {
        if self.poisoned.is_none() {
            tracing::error!(
                target: "rsm",
                error = %e,
                applied = self.applied_index,
                durable = self.durable_index,
                "apply refused: this node stops here (§12.1 Fatal)",
            );
            self.poisoned = Some(e.to_string());
        }
        e
    }

    /// Everything that is decided before the first write: the skip, the gap,
    /// the entry's own validity (I16), monotone time (I5) and the planner's
    /// bases (I18). `Some` means the entry needs no execution.
    fn gates(&mut self, c: &Committed) -> Result<Option<Applied>> {
        if c.index <= self.applied_index {
            self.stats.skipped += 1;
            return Ok(Some(Applied::Skipped));
        }
        if c.index != self.applied_index + 1 {
            return Err(ApplyError::Gap {
                expected: self.applied_index + 1,
                got: c.index,
            });
        }
        // A consensus-internal entry (an openraft blank or membership entry,
        // `Entry::noop`): nothing to validate, no clock and no bases to check.
        // `execute` advances the applied index and term only.
        if c.entry.is_noop() {
            return Ok(None);
        }
        // I16: an unknown kind, an unknown catalogue version or a span that
        // does not cover the effects stops this node here, before a single row
        // is written.
        c.entry.validate()?;

        if c.entry.now_us < self.last_now_us {
            return Err(ApplyError::TimeWentBackwards {
                last: self.last_now_us,
                got: c.entry.now_us,
            });
        }
        // I18: the planner assigned partition ids and KV versions from these
        // bases. If they are not what this node's `meta` holds, the two
        // disagree about which ids the entry means, and applying it would make
        // two partitions one partition on every node at once.
        if c.entry.pid_base != self.next_pid {
            return Err(ApplyError::Bases {
                what: "partition id",
                expected: self.next_pid,
                got: c.entry.pid_base,
            });
        }
        if c.entry.kv_version_base != self.kv_version_next {
            return Err(ApplyError::Bases {
                what: "kv version",
                expected: self.kv_version_next,
                got: c.entry.kv_version_base,
            });
        }
        Ok(None)
    }

    /// The entry itself: effects in order, outcomes, `meta`, the waiters and
    /// the wakes. Every refusal from here leaves a prefix behind and poisons
    /// the applier (see [`Applier::apply`]).
    fn execute(&mut self, c: &Committed) -> Result<Applied> {
        if c.entry.is_noop() {
            return self.execute_noop(c);
        }
        let ecx = EntryCx {
            index: c.index,
            now_us: c.entry.now_us,
            wants_appended: self.notify.wants_appended(),
            wants_append_wakes: self.notify.wants_append_wakes(),
        };
        let mut pids_assigned = 0u64;
        let mut kv_versions_assigned = 0u64;
        let max_created_before = self.max_created_at_us;
        // A test's injected refusal (`faults::refuse_apply_of`): off unless a
        // test armed one, one relaxed load per entry.
        let refuse_at = crate::rsm::faults::apply_refusal(&c.entry);
        // KV calls whose receiver waits for its reads (`kv_reads`): each is
        // rendered right after its OWN last effect, before any later command's.
        let kv_render = self.kv_render_points(c);
        let effects = &c.entry.effects;
        // Runs only for an entry with enough pid-keyed effects to repay the
        // hand-offs; a smaller one takes the one-thread path, in entry order.
        let min = self.cfg.apply_shard_min.max(1);
        let sharded = self.shards.len() > 1
            && (min == 1
                || effects
                    .iter()
                    .filter(|e| shard::pid_keyed(e).is_some())
                    .take(min)
                    .count()
                    >= min);
        for (ord, e) in effects.iter().enumerate() {
            match e.assigns() {
                Assigns::Pid(_) => pids_assigned += 1,
                Assigns::KvVersion(_) => kv_versions_assigned += 1,
                Assigns::Nothing => {}
            }
            let o = ord as u32;
            if let Some((at, why)) = &refuse_at {
                if *at == ord {
                    // Everything before it lands first, as on one thread: the
                    // refusal leaves the same half-executed entry.
                    self.flush_run(c, &ecx)?;
                    self.failed_effect = Some(o);
                    return Err(ApplyError::Inconsistent {
                        what: "injected apply fault (test)",
                        detail: why.clone(),
                    });
                }
            }
            if sharded {
                // A pid-keyed effect joins the run; anything else ends it, and
                // runs only once every effect before it has landed. An effect
                // a KV read is rendered after stays out of runs, so the render
                // sees its state at once.
                if !kv_render.contains_key(&o) && self.run.admit(&self.writes, o, e)? {
                    continue;
                }
                // (The run's effects hit `apply.mid_entry` where they ran.)
                self.flush_run(c, &ecx)?;
            }
            self.failed_effect = Some(o);
            self.effect(&ecx, o, e)?;
            // The consumption engine drops or refreshes what a catalog effect
            // names (the pid-keyed ones report from their shard).
            if let (Some(en), None) = (&self.engine, shard::pid_keyed(e)) {
                en.on_effect(e, ecx.index);
            }
            if let Some(cmds) = kv_render.get(&o) {
                self.render_kv_reads(c, cmds);
            }
            // §13.5 `apply.mid_entry`: some effects of this entry are written
            // to the OPEN store transaction (and some payload bytes to files),
            // the rest are not, and the applied index has NOT advanced — it is
            // written last, in the same commit as every effect (I11). A crash
            // here (before any store commit) reopens at the previous applied
            // index and replays the whole entry: the atomicity test (I1, I11).
            // Only when at least one effect remains, so it means "mid".
            if ord + 1 < effects.len() {
                crate::rsm::faults::hit("apply.mid_entry");
                #[cfg(test)]
                crate::rsm::faults::mid_entry_hook();
            }
        }
        self.flush_run(c, &ecx)?;
        self.failed_effect = None;
        self.stats.effects += effects.len() as u64;
        for s in &self.shards {
            self.max_created_at_us = self.max_created_at_us.max(s.max_created_at_us);
        }

        // PERF-C: flush this entry's buffered segment writes — one `write` per
        // touched file, on the pool when configured — and publish their index
        // records. It runs BEFORE the leader answers (I4: a pop's payload bytes
        // are in this node's files by the time `notify.applied` fires below) and
        // BEFORE any store commit records the new file lengths (I11): the run
        // loop only commits after `apply` returns, so this is the boundary that
        // keeps every recorded length backed by bytes already on the fd. A
        // no-op when buffering is off.
        self.segments.flush_writes()?;

        // Write this entry's buffered qlog records to the file + RAM index (one
        // `write` per touched queue, NO fsync). `Some` ONLY on the unit-test /
        // A1-A2-A3a path; on the LIVE A3b path the external writer already wrote
        // AND fsync'd them before the entry reached apply, so this is `None`.
        // Where it runs it does so at the boundary the payload becomes readable,
        // before the leader answers — the A2 read invariant.
        if let Some(qlog) = self.qlog.as_mut() {
            qlog.flush().map_err(ApplyError::Qlog)?;
        }
        // A3b: the highest applied `Append` index, recorded into
        // `meta::QLOG_DURABLE_INDEX` at the next commit / durable point so
        // recovery reconciles the reopened qlog's durable tail against it
        // (NA-QLOG-I1). On the live path the external writer fsync'd this entry's
        // qlog record BEFORE the entry reached apply, so it is durable by the time
        // this index names it. Only when the qlog knob is on (else no qlog).
        // Phase C: with the WRITER owning the queue logs, EVERY entry is in them
        // (a `REC_ENTRY` record in each queue log it touches, fsync'd before
        // apply), so the qlog-durable index names every entry, not only appends.
        if self.cfg.qlog
            && (self.cfg.qlog_writer_external
                || c.entry
                    .effects
                    .iter()
                    .any(|e| matches!(e, Effect::Append { .. })))
        {
            self.last_append_index = self.last_append_index.max(c.index);
        }

        // §5.4: every LOGGED command's outcome is recorded, so a retry of a
        // request id inside the window is answered from state and plans
        // nothing (I6). A command that planned no effects was never logged.
        self.record_outcomes(c)?;

        // meta, in the same transaction as everything above (I11).
        self.applied_index = c.index;
        self.applied_term = c.term;
        self.last_now_us = c.entry.now_us;
        self.next_pid += pids_assigned;
        self.kv_version_next += kv_versions_assigned;
        self.writes
            .set_meta_i64(meta::LAST_NOW_US, c.entry.now_us)?;
        // Only when an `Append` moved it: this is a random put, and an entry
        // that carries no payload has no business paying for one.
        if self.max_created_at_us != max_created_before {
            self.writes
                .set_meta_i64(meta::MAX_CREATED_AT_US, self.max_created_at_us)?;
        }
        if pids_assigned > 0 {
            self.writes.set_meta_u64(meta::NEXT_PID, self.next_pid)?;
        }
        if kv_versions_assigned > 0 {
            self.writes
                .set_meta_u64(meta::KV_VERSION_NEXT, self.kv_version_next)?;
        }
        // Phase C: the applied index is the entry's LAST write. The hot
        // keyspaces are RAM and LIVE (no snapshot), and the planner reads
        // `applied_index` FIRST and folds every entry above it: an index that
        // names this entry must imply every other write of it — above all
        // `NEXT_PID` — is already visible, or a planner reading in between would
        // neither fold this entry nor see its pids, and could mint a duplicate.
        self.writes.set_applied(c.index, c.term)?;
        self.dirty = true;
        self.entries_since_commit += 1;
        self.stats.entries += 1;

        // I4: the leader answers from here, and not one step earlier. The
        // bytes of a pop's payload are already in this node's files; a reader
        // that needs the ROWS waits for the next store commit (≤ 4 ms, §11.3).
        self.notify.applied(c.index, c.term, &c.entry.commands);
        // Per-partition append wakes (Kafka fetches), once per partition of
        // the entry, after the answer like every wake.
        if ecx.wants_appended {
            let mut appended: Vec<(String, String, String)> = Vec::new();
            for s in &mut self.shards {
                appended.append(&mut s.appended);
            }
            appended.sort_unstable();
            appended.dedup();
            for (t, q, part) in &appended {
                self.notify.appended(t, q, part);
            }
        }
        self.emit_wakes();
        Ok(Applied::Executed {
            effects: c.entry.effects.len(),
        })
    }

    /// The entry's wakes, from every shard's counts, in the order one thread
    /// emits them.
    ///
    /// One wake per EVENT (a partition made claimable for a group), in key
    /// order: the raft gates release ONE parked pop per wake
    /// (PLAN_RAFT_DRAIN_FIX P2.2), so an entry that armed sixteen partitions
    /// must wake sixteen pops, not one. The order does not depend on where in
    /// the entry they sat, nor on which shard counted them: `(tenant, queue,
    /// group)` ascending with the queue-wide `None` first and emitted once,
    /// and for one group its lease-release wakes ([`Notify::wake`]) before its
    /// append wakes ([`Notify::wake_append`]). The shards count instead of
    /// collecting one `(String, String, Option<String>)` per event (B41).
    fn emit_wakes(&mut self) {
        let mut all: Vec<(&str, &str, Option<&str>, u32, u32)> = Vec::new();
        for s in &self.shards {
            s.queues.wakes(&mut all);
        }
        if all.is_empty() {
            return;
        }
        all.sort_unstable_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)));
        let mut emitted = 0u64;
        let mut i = 0;
        while i < all.len() {
            let (t, q, g) = (all[i].0, all[i].1, all[i].2);
            let (mut appends, mut releases) = (0u32, 0u32);
            while i < all.len() && (all[i].0, all[i].1, all[i].2) == (t, q, g) {
                appends += all[i].3;
                releases += all[i].4;
                i += 1;
            }
            match g {
                None => {
                    self.notify.wake(t, q, None);
                    emitted += 1;
                }
                Some(g) => {
                    for _ in 0..releases {
                        self.notify.wake(t, q, Some(g));
                    }
                    for _ in 0..appends {
                        self.notify.wake_append(t, q, g);
                    }
                    emitted += u64::from(appends) + u64::from(releases);
                }
            }
        }
        drop(all);
        self.stats.wakes += emitted;
        for s in &mut self.shards {
            s.queues.clear_wakes();
        }
    }

    /// Record every logged command's outcome ([`record_outcome`]).
    ///
    /// With a pool and a wide entry, the shards record them: command `i` on
    /// shard `i % shards`, through its own shard writer. Every command of an
    /// entry has its own request id (`Entry::validate`), so the keys are
    /// disjoint, and a row's bytes do not depend on when it is written. It
    /// runs after every effect of the entry, as the one-thread loop does, so
    /// no effect of this entry sees these rows either way.
    fn record_outcomes(&mut self, c: &Committed) -> Result<()> {
        let cmds = &c.entry.commands;
        let now_us = c.entry.now_us;
        self.stats.outcomes_recorded += cmds.len() as u64;
        let n = self.shards.len();
        let pool = match self.pool.as_mut() {
            Some(p) if cmds.len() >= PARALLEL_OUTCOMES_MIN => p,
            _ => {
                for cmd in cmds {
                    record_outcome(
                        &mut self.writes,
                        &mut self.req_row,
                        &cmd.request_id,
                        now_us,
                        &cmd.outcome,
                    )?;
                }
                return Ok(());
            }
        };
        let mut writers: Vec<S::Shard<'s>> = Vec::with_capacity(n - 1);
        for _ in 1..n {
            writers.push(self.store.shard_writer()?);
        }
        let mut errs: Vec<Option<ApplyError>> = (0..n).map(|_| None).collect();
        {
            let (first, rest) = self.shards.split_at_mut(1);
            let (err0, errs_rest) = errs.split_at_mut(1);
            let mut jobs: Vec<_> = rest
                .iter_mut()
                .zip(writers.iter_mut())
                .zip(errs_rest.iter_mut())
                .enumerate()
                .map(|(i, ((s, w), err))| {
                    let row = &mut s.row;
                    move || {
                        for cmd in cmds.iter().skip(i + 1).step_by(n) {
                            if let Err(e) =
                                record_outcome(w, row, &cmd.request_id, now_us, &cmd.outcome)
                            {
                                *err = Some(e);
                                return;
                            }
                        }
                    }
                })
                .collect();
            let mut refs: Vec<&mut (dyn FnMut() + Send)> = jobs
                .iter_mut()
                .map(|j| j as &mut (dyn FnMut() + Send))
                .collect();
            let writes = &mut self.writes;
            let row = &mut first[0].row;
            pool.scope(&mut refs, &mut || {
                for cmd in cmds.iter().step_by(n) {
                    if let Err(e) =
                        record_outcome(writes, row, &cmd.request_id, now_us, &cmd.outcome)
                    {
                        err0[0] = Some(e);
                        return;
                    }
                }
            });
        }
        drop(writers);
        match errs.into_iter().flatten().next() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Execute the pending run: the frames of its `Append`s first, in entry
    /// order, on this thread (the pre-pass); then every shard's effects, each
    /// shard in entry order; then the releases and journal records the shards
    /// kept, in entry order again. A refusal is the lowest failing ordinal —
    /// the effect a single thread would have stopped at.
    fn flush_run(&mut self, c: &Committed, ecx: &EntryCx) -> Result<()> {
        if self.run.is_empty() {
            return Ok(());
        }
        let r = self.execute_run(c, ecx);
        // A refusal poisons the applier, so what a failed run kept is never
        // applied; it is dropped all the same, and the run starts empty.
        if r.is_err() {
            for s in &mut self.shards {
                s.releases.clear();
                s.retention.clear();
                s.churn.clear();
                s.failed = None;
            }
        }
        self.run.clear();
        r
    }

    fn execute_run(&mut self, c: &Committed, ecx: &EntryCx) -> Result<()> {
        let effects = &c.entry.effects;
        let first_fail = AtomicU32::new(u32::MAX);
        // 1. The pre-pass. Node-local bytes whose ORDER is in the files and the
        //    qlog, so they are written here, in entry order, exactly as the
        //    one-thread path writes them. None on the live path.
        let mut prepass_err: Option<(u32, ApplyError)> = None;
        if !(self.cfg.qlog && self.cfg.qlog_writer_external) {
            self.positions.clear();
            self.positions.resize(effects.len(), None);
            for &ord in &self.run.all {
                let Effect::Append {
                    pid,
                    bucket,
                    base_offset,
                    count,
                    created_at_us,
                    hashes,
                    blob,
                } = &effects[ord as usize]
                else {
                    continue;
                };
                let pos = match self.segments.append(
                    *bucket,
                    *pid,
                    *base_offset,
                    *count,
                    *created_at_us,
                    hashes,
                    blob,
                ) {
                    Ok(p) => p,
                    Err(e) => {
                        // The effects before it still run, so a refusal among
                        // them is reported first, as on one thread.
                        first_fail.store(ord, std::sync::atomic::Ordering::Relaxed);
                        prepass_err = Some((ord, e.into()));
                        break;
                    }
                };
                // §13.5 `apply.segment_written`, as on the one-thread path: the
                // frame is in the file, nothing that records it is committed.
                crate::rsm::faults::hit("apply.segment_written");
                self.positions[ord as usize] = Some(pos);
                if let Some(qlog) = self.qlog.as_mut() {
                    // The qlog record names the partition's queue: from its
                    // row, or from a create earlier in this run. Neither means
                    // the shard refuses the append ("no partition row").
                    let names = match self.writes.partition(*pid) {
                        Err(e) => {
                            first_fail.store(ord, std::sync::atomic::Ordering::Relaxed);
                            prepass_err = Some((ord, e.into()));
                            break;
                        }
                        Ok(Some(p)) => Some((p.tenant, p.queue)),
                        Ok(None) => self
                            .run
                            .all
                            .iter()
                            .find_map(|&o| match &effects[o as usize] {
                                Effect::PartitionCreate {
                                    pid: created,
                                    tenant,
                                    queue,
                                    ..
                                } if o < ord && created == pid => {
                                    Some((tenant.clone(), queue.clone()))
                                }
                                _ => None,
                            }),
                    };
                    if let Some((tenant, queue)) = names {
                        qlog.buffer(
                            &tenant,
                            &queue,
                            ecx.index,
                            *pid,
                            *base_offset,
                            *count,
                            *created_at_us,
                            hashes,
                            blob,
                        );
                    }
                }
            }
        }
        // 2. The shards: on their threads when there is a pool, more than one
        //    shard has work and the run is big enough; else one after another
        //    here. Same effects, same state either way (module header of
        //    `apply_shard.rs`).
        {
            let cx = shard::Cx {
                cfg: &self.cfg,
                index: ecx.index,
                now_us: ecx.now_us,
                wants_appended: ecx.wants_appended,
                wants_append_wakes: ecx.wants_append_wakes,
                positions: &self.positions,
                engine: self.engine.as_ref(),
            };
            let ords = &self.run.ords;
            let busy = ords.iter().filter(|o| !o.is_empty()).count();
            let parallel = busy > 1 && self.run.all.len() >= self.cfg.apply_shard_min.max(1);
            match self.pool.as_mut() {
                Some(pool) if parallel && prepass_err.is_none() => {
                    // This thread runs the last busy shard itself, on the write
                    // handle; every other busy shard gets a pool thread and a
                    // shard writer of its own, dropped before this returns (the
                    // store commits only with none alive).
                    let last = (0..ords.len())
                        .rev()
                        .find(|&s| !ords[s].is_empty())
                        .expect("a busy shard");
                    let mut writers: Vec<S::Shard<'s>> = Vec::with_capacity(busy - 1);
                    for _ in 1..busy {
                        writers.push(self.store.shard_writer()?);
                    }
                    let (head, tail) = self.shards.split_at_mut(last);
                    let own = &mut tail[0];
                    let (cx, ff) = (&cx, &first_fail);
                    let mut jobs: Vec<_> = head
                        .iter_mut()
                        .enumerate()
                        .filter(|(s, _)| !ords[*s].is_empty())
                        .zip(writers.iter_mut())
                        .map(|((s, shard), w)| {
                            let mine = &ords[s][..];
                            move || shard.run(w, cx, effects, mine, ff)
                        })
                        .collect();
                    let mut refs: Vec<&mut (dyn FnMut() + Send)> = jobs
                        .iter_mut()
                        .map(|j| j as &mut (dyn FnMut() + Send))
                        .collect();
                    let writes = &mut self.writes;
                    pool.scope(&mut refs, &mut || {
                        own.run(writes, cx, effects, &ords[last], ff)
                    });
                    self.runs[0] += 1;
                }
                _ => {
                    self.runs[1] += 1;
                    for (s, shard) in self.shards.iter_mut().enumerate() {
                        if !ords[s].is_empty() {
                            shard.run(&mut self.writes, &cx, effects, &ords[s], &first_fail);
                        }
                    }
                }
            }
        }
        // 3. The refusal a single thread stops at: the lowest ordinal.
        let mut failed = prepass_err;
        for s in &mut self.shards {
            if let Some((ord, e)) = s.failed.take() {
                if failed.as_ref().is_none_or(|(f, _)| ord < *f) {
                    failed = Some((ord, e));
                }
            }
        }
        if let Some((ord, e)) = failed {
            self.failed_effect = Some(ord);
            return Err(e);
        }
        // 4. What the shards kept, in entry order: segment releases, retention
        //    journal records, partition churn.
        let mut releases: Vec<(u32, Position, Release)> = Vec::new();
        let mut retention: Vec<(u32, shard::RetentionRec)> = Vec::new();
        let mut churn: Vec<u32> = Vec::new();
        for s in &mut self.shards {
            releases.append(&mut s.releases);
            retention.append(&mut s.retention);
            churn.append(&mut s.churn);
        }
        releases.sort_by_key(|r| r.0);
        for (_, pos, what) in releases {
            self.segments.release(pos, what);
        }
        retention.sort_by_key(|r| r.0);
        for (_, r) in retention {
            self.local_metrics.record_retention(
                ecx.now_us,
                &r.tenant,
                &r.queue,
                r.pid,
                r.log_from,
                r.log_to,
                r.txns_from,
                r.txns_to,
            );
        }
        churn.sort_unstable();
        for ord in churn {
            if let Effect::PartitionCreate { tenant, queue, .. } = &effects[ord as usize] {
                self.local_metrics
                    .record_churn(ecx.now_us, tenant, queue, 1, 0);
            }
        }
        Ok(())
    }

    /// A pid-keyed effect executed on this thread: every effect with one
    /// shard, and with more, one that stays out of a run. Its shard's state,
    /// its side effects inline — the one-thread path.
    fn pid_effect_inline(&mut self, ecx: &EntryCx, ord: u32, pid: Pid, e: &Effect) -> Result<()> {
        let s = shard_of(pid, self.shards.len());
        let cx = shard::Cx {
            cfg: &self.cfg,
            index: ecx.index,
            now_us: ecx.now_us,
            wants_appended: ecx.wants_appended,
            wants_append_wakes: ecx.wants_append_wakes,
            positions: &[],
            engine: self.engine.as_ref(),
        };
        let mut side = shard::Side::Inline {
            segments: &mut self.segments,
            qlog: self.qlog.as_mut(),
            metrics: &self.local_metrics,
        };
        self.shards[s].effect(&mut self.writes, &cx, &mut side, ord, e)
    }

    /// [`Entry::noop`]: the applied index and term move, in the same store
    /// transaction discipline as any entry (`set_applied` is its only write),
    /// and the waiter on this index is answered. No clock moves — `last_now_us`
    /// stays what the last real entry set, so the next one is judged against it.
    fn execute_noop(&mut self, c: &Committed) -> Result<Applied> {
        // The writer put this entry's record into the system log before apply
        // saw it, so the qlog-durable index may name it (Phase C).
        if self.cfg.qlog && self.cfg.qlog_writer_external {
            self.last_append_index = self.last_append_index.max(c.index);
        }
        if is_skip_marker(&c.entry) {
            self.execute_skip_marker(c)?;
        }
        self.applied_index = c.index;
        self.applied_term = c.term;
        self.writes.set_applied(c.index, c.term)?;
        self.dirty = true;
        self.entries_since_commit += 1;
        self.stats.entries += 1;
        self.notify.applied(c.index, c.term, &[]);
        Ok(Applied::Executed { effects: 0 })
    }

    /// A skip marker ([`is_skip_marker`]): the entry an operator told this
    /// cluster to step over (`QUEEN_RAFT_APPLY_SKIP`) was replaced by it in the
    /// log, so none of that entry's effects, outcomes or wakes exist here. What
    /// the marker carries is kept: the clock (never backwards, I5) and the
    /// partition-id and KV-version bases after the skipped entry (the ids it
    /// reserved are burned, so the entries planned after it keep their bases,
    /// I18). The skip is recorded in node-local `meta`, and said loudly.
    fn execute_skip_marker(&mut self, c: &Committed) -> Result<()> {
        let m = &c.entry;
        if m.now_us > self.last_now_us {
            self.last_now_us = m.now_us;
            self.writes.set_meta_i64(meta::LAST_NOW_US, m.now_us)?;
        }
        if m.pid_base > self.next_pid {
            self.next_pid = m.pid_base;
            self.writes.set_meta_u64(meta::NEXT_PID, self.next_pid)?;
        }
        if m.kv_version_base > self.kv_version_next {
            self.kv_version_next = m.kv_version_base;
            self.writes
                .set_meta_u64(meta::KV_VERSION_NEXT, self.kv_version_next)?;
        }
        let record = serde_json::json!({
            "term": c.term,
            "nowUs": m.now_us,
            "pidBase": m.pid_base,
            "kvVersionBase": m.kv_version_base,
        })
        .to_string();
        self.writes
            .set_meta_blob(&apply_skipped_key(c.index), record.as_bytes())?;
        self.stats.skipped_by_operator += 1;
        tracing::error!(
            target: "rsm",
            index = c.index,
            term = c.term,
            next_pid = self.next_pid,
            kv_version_next = self.kv_version_next,
            "apply: SKIPPED entry {} (term {}) as an operator asked (QUEEN_RAFT_APPLY_SKIP): none \
             of its effects applied, its commands were not answered, the ids it reserved are \
             burned",
            c.index,
            c.term,
        );
        Ok(())
    }

    // -- one effect --------------------------------------------------------

    /// One effect, on this thread. A pid-keyed one goes to its shard's code
    /// with its side effects inline ([`Applier::pid_effect_inline`]); the rest
    /// are the global effects, which only ever run here, with no shard active.
    fn effect(&mut self, ecx: &EntryCx, ord: u32, e: &Effect) -> Result<()> {
        if let Some(pid) = shard::pid_keyed(e) {
            return self.pid_effect_inline(ecx, ord, pid, e);
        }
        let (index, now_us) = (ecx.index, ecx.now_us);
        match e {
            Effect::Noop => Ok(()),

            Effect::QueueUpsert { tenant, queue, cfg } => {
                self.writes.put_queue(tenant, queue, cfg)?;
                // B41: the append path's cached delay (and group list) of the
                // queue is stale.
                self.invalidate_queue(tenant, queue);
                Ok(())
            }
            Effect::QueueDelete { tenant, queue } => self.queue_delete(tenant, queue),

            Effect::GroupUpsert {
                tenant,
                queue,
                group,
                meta,
            } => {
                // The registration's POSITION is apply's to assign: §8 seeds a
                // subscription from (entry index, effect position), and only
                // apply knows the index. It is set when the row is CREATED and
                // carried forward on every later upsert, so a configuration
                // change (conflation, a timestamp) cannot move the seeding
                // point of a partition this group has not touched yet.
                let existed = self.writes.group(tenant, queue, group)?;
                let (reg_index, reg_effect) = match &existed {
                    Some(old) => (old.reg_index, old.reg_effect),
                    None => (index, ord),
                };
                let row = GroupRow {
                    meta: meta.clone(),
                    reg_index,
                    reg_effect,
                };
                self.writes.put_group(tenant, queue, group, &row)?;
                // The queue's group set changed; the append path's cached
                // list must be read again (every shard's).
                self.invalidate_queue(tenant, queue);
                // A NEW `all` group starts at the retained floor: its `pending`
                // gauge carries the backlog it inherited.
                if existed.is_none() && meta.mode == crate::rsm::effect::SubscriptionMode::All {
                    self.inherit_backlog(tenant, queue, group)?;
                }
                Ok(())
            }
            Effect::GroupDelete {
                tenant,
                queue,
                group,
            } => {
                self.writes.del_group(tenant, queue, group)?;
                // The group's COUNTERS go with its name (§6.4): they are the
                // lag inputs of a group that no longer exists, and the name is
                // reusable at once (§5.2) — a group recreated under it would
                // otherwise inherit a dead group's pending, completed,
                // consumed and failed, and report lag from its history.
                let prefix = counter_one_group_prefix(tenant, queue, group);
                self.ctr_sweep(&prefix)?;
                // The queue's group set changed.
                self.invalidate_queue(tenant, queue);
                Ok(())
            }

            Effect::PartitionDelete { pid } => {
                let owner = self.writes.partition(*pid)?.map(|p| (p.tenant, p.queue));
                self.partition_delete(*pid)?;
                if let Some((tenant, queue)) = owner {
                    self.local_metrics
                        .record_churn(now_us, &tenant, &queue, 0, 1);
                }
                Ok(())
            }

            Effect::DlqDelete {
                dlq_id,
                tenant,
                queue,
            } => {
                let Some(row) = self.writes.dlq(tenant, queue, dlq_id)? else {
                    self.stats.missing_rows += 1;
                    return Ok(());
                };
                let pid = row.pid;
                self.writes.del_dlq(tenant, queue, dlq_id, &row)?;
                // Through the same gate as every other removal: a dead letter
                // of a queue that has been deleted (its `dlq` rows outlive the
                // name, §5.2) must not come out of the gauges of the queue
                // that now holds that name.
                self.settle_dlq(pid, tenant, queue, 1)?;
                Ok(())
            }

            Effect::GarbageAdd {
                pids,
                scope,
                deleted_at_us,
            } => {
                for pid in pids {
                    let row = GarbageRow {
                        deleted_at_us: *deleted_at_us,
                        scope: scope.clone(),
                        // WHOSE gauges these pids may still settle, decided
                        // here — while the answer is still knowable — and
                        // never again by the queue's NAME, which §5.2 makes
                        // reusable the instant this entry lands. A queue or
                        // tenant delete removed the `queues` row earlier in
                        // this same entry, so this reads `None` and the chunks
                        // that follow leave every queue and tenant gauge
                        // alone; a group delete leaves the row in place and
                        // records its id.
                        queue_id: self.queue_id_of(*pid)?,
                        resume: Vec::new(),
                    };
                    self.writes.put_garbage(*pid, &row)?;
                    if !matches!(scope, GarbageScope::Group { .. }) {
                        // Readers and planners ignore a garbage pid from this
                        // point (§5.2 rules), so its share of every surviving
                        // group's `pending` gauge goes now, while the cursors
                        // are still here to say what that share is: the chunks
                        // that follow delete them one stage at a time, and by
                        // the time the partition row goes there is nothing left
                        // to read.
                        self.settle_group_pending(*pid)?;
                    }
                }
                Ok(())
            }
            Effect::DeleteChunk {
                pids,
                scope,
                resume,
                limit,
            } => self.delete_chunk(pids, scope, resume, *limit as usize),

            Effect::RequestIdsExpire { cutoff_us } => {
                let mut victims: Vec<(i64, RequestId)> = Vec::new();
                self.writes
                    .scan_request_expiry(REQUEST_EXPIRE_LIMIT, &mut |at, id| {
                        if at >= *cutoff_us {
                            return false;
                        }
                        victims.push((at, id));
                        true
                    })?;
                for (at, id) in &victims {
                    self.writes.del_request_id(id, *at)?;
                }
                self.stats.rows_swept += victims.len() as u64;
                Ok(())
            }

            // WP-2.2: KV. Apply knows no KV semantics (I2): the planner
            // decided the value, the version (`kv_version_base + ordinal`,
            // asserted by `Entry::validate` and counted by `execute`), the
            // expiry and both stamps from the entry's `now_us`. Apply writes the
            // row and keeps the expiry index exact — deterministic, and exactly
            // once under replay from the durable checkpoint (Phase C reopens the
            // store AT the checkpoint and re-applies every later entry once).
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
                let row = crate::rsm::store::rows::KvRow {
                    value: value.clone(),
                    version: *version,
                    expires_at_us: *expires_at_us,
                    created_at_us: *created_at_us,
                    updated_at_us: *updated_at_us,
                };
                self.writes.put_kv(tenant, ns, key, &row)?;
                Ok(())
            }
            // A physical delete: a client delete, a lost `expect` that still
            // prunes an expired row, or one row of the leader's expiry sweep.
            // A row that is already gone is a no-op, never an error — the
            // planner decided against the same state, so only a replay could
            // meet it, and a replay must converge.
            Effect::KvDelete { tenant, ns, key } => {
                if self.writes.del_kv(tenant, ns, key)?.is_none() {
                    self.stats.missing_rows += 1;
                }
                Ok(())
            }

            Effect::ClusterVersionSet { version } => {
                self.writes.set_meta_u32(meta::CLUSTER_VERSION, *version)?;
                Ok(())
            }
            Effect::MembershipNote {
                node_id,
                generation,
                disk_uuid,
                address,
            } => {
                // D21's note, beside the library's own membership record
                // (WP-4.3 owns that one). One key per node, so a three-voter
                // cluster holds three notes and not the last one written.
                let mut key = Vec::with_capacity(meta::MEMBERSHIP.len() + 9);
                key.extend_from_slice(meta::MEMBERSHIP);
                key.push(b':');
                key.extend_from_slice(&node_id.to_be_bytes());
                let mut w = crate::rsm::effect::Writer::with_capacity(64);
                w.u64(*node_id);
                w.u64(*generation);
                w.bytes16(disk_uuid);
                w.str(address);
                self.writes.set_meta_blob(&key, &w.into_inner())?;
                Ok(())
            }
            Effect::TenantPurge { tenant } => self.tenant_purge(tenant),

            // Timers (WP-2.3). Plain overwrites of one row and its
            // fire-order index entry, from values the planner computed — no
            // clock here (I2): `deliver_at` and every backoff instant travel in
            // the effect. A fire is NOT a timer kind: it is the push's `Append`
            // plus a `TimerDelete` (or `TimerBackoff`) in the same entry, so the
            // message and the timer's removal land together or not at all.
            Effect::TimerUpsert {
                tenant,
                queue,
                key,
                row,
            } => {
                self.writes.put_timer(tenant, queue, key, row)?;
                Ok(())
            }
            Effect::TimerDelete { tenant, queue, key } => {
                // A cancel racing a fire planned in the same pipeline is refused
                // by the planner's overlay, so a missing row here is only a
                // replay-safe no-op, counted like every other one.
                match self.writes.timer(tenant, queue, key)? {
                    Some(old) => {
                        self.writes.del_timer(tenant, queue, key, &old)?;
                    }
                    None => self.stats.missing_rows += 1,
                }
                Ok(())
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
                match self.writes.timer(tenant, queue, key)? {
                    Some(mut row) => {
                        // A new visibility, the attempt count the planner
                        // decided (unchanged on a transient failure), the
                        // error. The row stays cancellable.
                        row.visible_at_us = Some(*visible_at_us);
                        row.attempts = *attempts;
                        row.last_error = last_error.clone();
                        row.updated_at_us = *updated_at_us;
                        self.writes.put_timer(tenant, queue, key, &row)?;
                    }
                    None => self.stats.missing_rows += 1,
                }
                Ok(())
            }

            Effect::StreamsQueryUpsert {
                query_id,
                tenant,
                row,
            } => {
                self.writes.put_streams_query(tenant, query_id, row)?;
                Ok(())
            }
            // Every pid-keyed kind went to its shard above (`pid_keyed`).
            Effect::PartitionCreate { .. }
            | Effect::Append { .. }
            | Effect::CursorSet { .. }
            | Effect::CursorDelete { .. }
            | Effect::DlqInsert { .. }
            | Effect::Watermark { .. }
            | Effect::StreamsStatePut { .. }
            | Effect::StreamsStateDelete { .. } => Err(ApplyError::Inconsistent {
                what: "apply",
                detail: format!("{} reached the global dispatcher", e.kind().name()),
            }),

            Effect::TraceAppend { event } => {
                // Sequence is assigned by apply, not the planner: all voters
                // see the same ordered prefix and therefore choose the same
                // next value. The secondary indexes carry the primary key so
                // expiry can delete the complete event atomically.
                let prefix = keys::trace_prefix(&event.tenant, event.pid, &event.txn);
                let mut seq = 0u64;
                self.writes.scan_raw(
                    Keyspace::Traces,
                    &prefix,
                    &prefix,
                    usize::MAX,
                    &mut |k, _| {
                        if let Some(n) = keys::trace_seq_of(k) {
                            seq = seq.max(n.saturating_add(1));
                        }
                        true
                    },
                )?;
                let primary = keys::trace(&event.tenant, event.pid, &event.txn, seq);
                self.writes
                    .put_raw(Keyspace::Traces, &primary, &rows::trace_encode(event))?;
                for name in &event.names {
                    self.writes.put_raw(
                        Keyspace::TraceNames,
                        &keys::trace_name(
                            &event.tenant,
                            name,
                            event.created_at_us,
                            &event.trace_id,
                        ),
                        &primary,
                    )?;
                }
                self.writes.put_raw(
                    Keyspace::TraceExpiry,
                    &keys::trace_expiry(event.created_at_us, &event.trace_id),
                    &primary,
                )?;
                Ok(())
            }
            Effect::TraceExpire { cutoff_us } => {
                let mut indexed: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                self.writes.scan_raw(
                    Keyspace::TraceExpiry,
                    &[],
                    &[],
                    usize::MAX,
                    &mut |expiry_key, primary| {
                        if keys::trace_expiry_created_of(expiry_key)
                            .is_none_or(|created| created >= *cutoff_us)
                        {
                            return false;
                        }
                        indexed.push((expiry_key.to_vec(), primary.to_vec()));
                        true
                    },
                )?;
                for (expiry_key, primary) in indexed {
                    let Some(raw) = self.writes.get_raw(Keyspace::Traces, &primary)? else {
                        self.writes.del_raw(Keyspace::TraceExpiry, &expiry_key)?;
                        self.stats.missing_rows += 1;
                        continue;
                    };
                    let event = rows::trace_decode(raw)
                        .map_err(|e| StoreError::corrupt(Keyspace::Traces, format!("{e}")))?;
                    for name in &event.names {
                        self.writes.del_raw(
                            Keyspace::TraceNames,
                            &keys::trace_name(
                                &event.tenant,
                                name,
                                event.created_at_us,
                                &event.trace_id,
                            ),
                        )?;
                    }
                    self.writes.del_raw(Keyspace::Traces, &primary)?;
                    self.writes.del_raw(Keyspace::TraceExpiry, &expiry_key)?;
                    self.stats.rows_swept += 1;
                }
                Ok(())
            }

            Effect::FlagSet { key, value } => {
                self.writes.put_flag(key, value)?;
                Ok(())
            }
            Effect::QuotaSet {
                kind,
                tenant,
                grant,
            } => {
                self.writes.put_quota(*kind, tenant, grant)?;
                Ok(())
            }
            Effect::EphemeralConfigSet {
                tenant,
                queue,
                options,
                updated_at_us,
            } => {
                self.writes.put_eph_config(
                    tenant,
                    queue,
                    &rows::EphConfigRow {
                        options: options.clone(),
                        updated_at_us: *updated_at_us,
                    },
                )?;
                Ok(())
            }
            Effect::EphemeralConfigDelete { tenant, queue } => {
                if !self.writes.del_eph_config(tenant, queue)? {
                    self.stats.missing_rows += 1;
                }
                Ok(())
            }
        }
    }

    // -- groups ------------------------------------------------------------

    /// A just-registered `all` group (see the `GroupUpsert` arm) is seeded at
    /// the retained floor of every partition of the queue: its pending counter
    /// carries the backlog it inherited. In pid order (deterministic, I2),
    /// O(partitions), once per group registration.
    fn inherit_backlog(&mut self, tenant: &str, queue: &str, group: &str) -> Result<()> {
        let mut pids: Vec<Pid> = Vec::new();
        self.writes
            .scan_queue_partitions(tenant, queue, None, usize::MAX, &mut |pid| {
                pids.push(pid);
                true
            })?;
        let mut inherited: i64 = 0;
        for pid in pids {
            let Some(p) = self.writes.partition(pid)? else {
                continue;
            };
            if p.last_offset < p.log_start as i64 {
                continue; // nothing retained to find
            }
            inherited += p.last_offset + 1 - p.log_start as i64;
        }
        if inherited > 0 {
            self.ctr_add(
                &keys::counter_group(tenant, queue, group, Counter::Pending),
                inherited,
            )?;
        }
        Ok(())
    }

    /// Retire one `seg_loc` row's claim on its segment file (§11.7).
    ///
    /// A row the qlog path filed (`file_id` 0, offset 0: the payload lives
    /// once, in the qlog) holds no claim. File 0 is nonetheless a real file of
    /// its bucket, so releasing against it would drive that file's counters
    /// below the truth ("a segment claim was released twice").
    fn release_seg_loc(&mut self, row: &SegLocRow, what: Release) {
        if self.cfg.qlog && self.cfg.qlog_writer_external && row.file_id == 0 && row.offset == 0 {
            return;
        }
        self.segments.release(
            Position {
                bucket: row.bucket,
                file_id: row.file_id,
                offset: row.offset,
                len: row.len,
            },
            what,
        );
    }

    // -- deletes -----------------------------------------------------------

    /// Remove every name-keyed resource owned by a tenant in this store
    /// transaction. Partition-owned rows have already been made unreachable
    /// with `GarbageAdd` in the same entry and are reclaimed by bounded
    /// `DeleteChunk` entries. Keeping this as one effect is what makes a crash
    /// incapable of exposing a tenant with only half of its metadata removed.
    fn tenant_purge(&mut self, tenant: &str) -> Result<()> {
        let mut queues = Vec::new();
        self.writes
            .scan_queues(tenant, usize::MAX, &mut |queue, _| {
                queues.push(queue.to_string());
                true
            })?;

        // Collect the rows whose secondary indexes need their old values
        // before deleting any queue metadata.
        let mut kv = Vec::new();
        self.writes
            .scan_kv_tenant(tenant, usize::MAX, &mut |ns, key, _| {
                kv.push((ns.to_string(), key.to_string()));
                true
            })?;
        let timer_prefix = keys::queues_prefix(tenant);
        let mut timers = Vec::new();
        let mut timer_bad = false;
        self.writes.scan_raw(
            Keyspace::Timers,
            &timer_prefix,
            &timer_prefix,
            usize::MAX,
            &mut |key, value| match (keys::timers_parts(key), rows::timer_decode(value)) {
                (Some((t, queue, timer_key)), Ok(row)) if t == tenant => {
                    timers.push((queue, timer_key, row));
                    true
                }
                _ => {
                    timer_bad = true;
                    false
                }
            },
        )?;
        if timer_bad {
            return Err(StoreError::corrupt(Keyspace::Timers, "timer row").into());
        }

        let mut query_ids = Vec::new();
        self.writes
            .scan_streams_queries(tenant, usize::MAX, &mut |id, _| {
                query_ids.push(id);
                true
            })?;

        let trace_prefix = keys::queues_prefix(tenant);
        let mut traces = Vec::new();
        let mut trace_bad = false;
        self.writes.scan_raw(
            Keyspace::Traces,
            &trace_prefix,
            &trace_prefix,
            usize::MAX,
            &mut |primary, value| match rows::trace_decode(value) {
                Ok(event) if event.tenant == tenant => {
                    traces.push((primary.to_vec(), event));
                    true
                }
                _ => {
                    trace_bad = true;
                    false
                }
            },
        )?;
        if trace_bad {
            return Err(StoreError::corrupt(Keyspace::Traces, "trace row").into());
        }

        for queue in &queues {
            self.queue_delete(tenant, queue)?;
        }
        for (ns, key) in &kv {
            self.writes.del_kv(tenant, ns, key)?;
        }
        for (queue, key, row) in &timers {
            self.writes.del_timer(tenant, queue, key, row)?;
        }
        for query_id in &query_ids {
            self.sweep(
                Keyspace::StreamsState,
                &keys::streams_query_state_prefix(query_id),
            )?;
            self.writes.del_raw(
                Keyspace::StreamsQueries,
                &keys::streams_query(tenant, query_id),
            )?;
        }
        self.sweep(Keyspace::EphConfig, &keys::eph_config_prefix(tenant))?;
        for kind in [
            crate::rsm::effect::QuotaKind::Kv,
            crate::rsm::effect::QuotaKind::Ephemeral,
            crate::rsm::effect::QuotaKind::Streams,
        ] {
            self.writes
                .del_raw(Keyspace::Quotas, &keys::quota(kind, tenant))?;
        }
        for (primary, event) in traces {
            for name in &event.names {
                self.writes.del_raw(
                    Keyspace::TraceNames,
                    &keys::trace_name(tenant, name, event.created_at_us, &event.trace_id),
                )?;
            }
            self.writes.del_raw(
                Keyspace::TraceExpiry,
                &keys::trace_expiry(event.created_at_us, &event.trace_id),
            )?;
            self.writes.del_raw(Keyspace::Traces, &primary)?;
        }
        // Safety sweeps cover stale rows left by an interrupted old build.
        self.sweep(Keyspace::TraceNames, &keys::queues_prefix(tenant))?;
        self.ctr_sweep(&keys::counter_queue_tenant_prefix(tenant))?;
        self.ctr_sweep(&keys::counter_group_tenant_prefix(tenant))?;
        self.ctr_sweep(&keys::counter_tenant_prefix(tenant))?;
        Ok(())
    }

    /// A queue delete: the NAME-keyed rows go at once, as one transaction, so
    /// the name is reusable immediately and a push right after the delete
    /// recreates it. The pid-keyed data goes in `DeleteChunk`s behind a
    /// `GarbageAdd` (§5.2), DLQ included, so between this call and the last
    /// `DeleteChunk` the dead queue's dead letters are still in the `dlq`
    /// keyspace under `(tenant, queue, id)` — a name a push may already have
    /// recreated. Their gauges are settled and cannot move again
    /// ([`Applier::queue_gauges_live`]), but a DLQ LISTING that filters only by
    /// name would show them. The reader must skip a dead letter whose pid is in
    /// the `garbage` set, as planners already skip garbage pids.
    fn queue_delete(&mut self, tenant: &str, queue: &str) -> Result<()> {
        self.writes.del_queue(tenant, queue)?;
        // The queue's counters go with its name, so the tenant's GAUGES are
        // settled HERE, from the queue's own — retained bytes, the number the
        // proxy's storage quota reads, and the dead-letter count. The
        // cumulative ones (pushed, completed) are the tenant's history and
        // stay. After this the per-partition sweeps must not touch any of
        // them again, which is what the `queue(...)` check in
        // [`Applier::partition_delete`] is for: `add_counter` would otherwise
        // recreate a swept row holding a negative number.
        for c in [Counter::RetainedBytes, Counter::DlqCount] {
            let held = self.ctr_read(&keys::counter_queue(tenant, queue, c))?;
            if held != 0 {
                self.ctr_add(&keys::counter_tenant(tenant, c), -held)?;
            }
        }
        let prefix = counter_queue_prefix(tenant, queue);
        self.ctr_sweep(&prefix)?;
        let prefix = counter_group_prefix(tenant, queue);
        self.ctr_sweep(&prefix)?;
        let mut groups: Vec<String> = Vec::new();
        self.writes
            .scan_groups(tenant, queue, usize::MAX, &mut |g, _row| {
                groups.push(g.to_string());
                true
            })?;
        for g in &groups {
            self.writes.del_group(tenant, queue, g)?;
        }
        // The queue and its group set are gone (every shard's cache).
        self.invalidate_queue(tenant, queue);
        let prefix = keys::partitions_by_key_prefix(tenant, queue);
        self.sweep(Keyspace::PartitionsByKey, &prefix)?;
        let prefix = keys::queue_partitions_prefix(tenant, queue);
        self.sweep(Keyspace::QueuePartitions, &prefix)?;
        // Phase A1 SHADOW: drop the deleted queue's log handle and any records
        // still buffered for it (§NA-I5). On-disk cleanup of `qlog/q<id>/` is
        // retention, a later phase; the shadow is never read in A1.
        if let Some(qlog) = self.qlog.as_mut() {
            qlog.remove(tenant, queue);
        }
        Ok(())
    }

    /// Drop a partition and everything keyed by it (retention's partition
    /// cleanup, and the tail of a delete whose chunks have finished).
    fn partition_delete(&mut self, pid: Pid) -> Result<()> {
        let Some(p) = self.writes.partition(pid)? else {
            self.stats.missing_rows += 1;
            return Ok(());
        };
        let (tenant, queue) = (p.tenant.clone(), p.queue.clone());
        let retained = self.ctr_read(&keys::counter_partition(pid, Counter::RetainedBytes))?;
        // The chunked path settled this partition's share of every group's
        // `pending` gauge at its `GarbageAdd`, while the cursors were still
        // there to say what it was; a direct `PartitionDelete` (retention's
        // partition cleanup) settles it here, with everything still in place.
        if self.writes.garbage(pid)?.is_none() {
            self.settle_group_pending(pid)?;
        }

        // cursors and their lease index rows
        let mut cursors: Vec<(String, Option<String>)> = Vec::new();
        self.writes.scan_cursors(pid, usize::MAX, &mut |g, c| {
            cursors.push((g.to_string(), c.worker.clone()));
            true
        })?;
        for (g, worker) in &cursors {
            if let Some(w) = worker {
                self.writes.del_lease(w, pid, g)?;
            }
            self.writes.del_cursor(pid, g)?;
        }

        // dead letters filed against it
        let mut dlq_ids: Vec<[u8; 16]> = Vec::new();
        let mut bad = false;
        let prefix = keys::dlq_by_pos_pid_prefix(pid);
        self.writes.scan_raw(
            Keyspace::DlqByPos,
            &prefix,
            &prefix,
            usize::MAX,
            &mut |_k, v| {
                match rows::dlq_ids_decode(v) {
                    Ok(ids) => dlq_ids.extend(ids),
                    Err(_) => bad = true,
                }
                true
            },
        )?;
        if bad {
            return Err(StoreError::corrupt(Keyspace::DlqByPos, "dlq id list").into());
        }
        let mut gone = 0i64;
        for id in &dlq_ids {
            if let Some(row) = self.writes.dlq(&tenant, &queue, id)? {
                self.writes.del_dlq(&tenant, &queue, id, &row)?;
                gone += 1;
            }
        }
        self.settle_dlq(pid, &tenant, &queue, gone)?;
        self.sweep(Keyspace::DlqByPos, &prefix)?;

        // payload positions: the files lose whatever claims these frames still
        // hold (§11.7)
        self.release_all_segments(pid, p.log_start)?;

        self.sweep(Keyspace::Dedup, &keys::dedup_prefix(pid))?;
        self.sweep(Keyspace::Txns, &keys::txns_prefix(pid))?;
        self.sweep(Keyspace::PartitionFiles, &keys::partition_files_prefix(pid))?;
        self.ctr_sweep(&keys::counter_partition_prefix(pid))?;

        // While the QUEUE still exists, its retained bytes and the tenant's
        // have to lose this partition's share: it is the number the proxy's
        // storage quota reads. The cumulative event counters (pushed,
        // completed) are left alone — they count what happened, not what is
        // stored. A queue that has already been deleted settled both in
        // [`Applier::queue_delete`], and adding here would resurrect a swept
        // row holding a negative number.
        if retained != 0 && shard::queue_gauges_live(&self.writes, pid, &tenant, &queue)? {
            self.ctr_add(
                &keys::counter_queue(&tenant, &queue, Counter::RetainedBytes),
                -retained,
            )?;
            self.ctr_add(
                &keys::counter_tenant(&tenant, Counter::RetainedBytes),
                -retained,
            )?;
        }

        self.writes.del_partition(pid, &p)?;
        self.writes.del_garbage(pid)?;
        Ok(())
    }

    /// Release every frame of a partition and forget where they were.
    ///
    /// `log_start` decides WHICH claims each frame still holds: retention
    /// released the payload of everything below it ([`Applier::watermark`]
    /// step 1) while its `seg_loc` row stayed for the txns window (D10).
    /// Releasing those twice is not harmless — [`Segments::release`] saturates,
    /// so the file table's `retained_frames`/`retained_bytes` end up BELOW the
    /// truth and §11.7's compaction would read them.
    fn release_all_segments(&mut self, pid: Pid, log_start: u64) -> Result<()> {
        let mut rows: Vec<(u64, SegLocRow)> = Vec::new();
        self.writes
            .scan_seg_loc(pid, 0, usize::MAX, &mut |base, row| {
                rows.push((base, row));
                true
            })?;
        for (base, row) in &rows {
            self.release_seg_loc(row, release_for(*base, log_start));
            self.writes.del_seg_loc(pid, *base)?;
        }
        Ok(())
    }

    /// One bounded step of the deletion behind a `GarbageAdd` (§5.2).
    ///
    /// The resume point lives in STATE (the garbage row), not in the effect:
    /// the effect's `resume` is the planner's view and is used only to start a
    /// pid whose row has none, so a replayed chunk can never sweep a range
    /// twice or skip one. Every stage deletes in key order, and the stage byte
    /// is the first byte of the resume key.
    fn delete_chunk(
        &mut self,
        pids: &[Pid],
        scope: &GarbageScope,
        resume: &[u8],
        limit: usize,
    ) -> Result<()> {
        let mut budget = limit.max(1);
        for pid in pids {
            if budget == 0 {
                break;
            }
            let Some(mut g) = self.writes.garbage(*pid)? else {
                continue;
            };
            if let GarbageScope::Group { group } = scope {
                // A group delete touches only the `(pid, group)` rows,
                // and there is exactly one cursor row per pair: no resume is
                // needed and none is kept.
                self.group_scope_chunk(*pid, group)?;
                // The marker goes only if it is THIS delete's. A queue or
                // tenant delete of the same pid replaces the row with its own,
                // wider one (`GarbageAdd` overwrites), and a group chunk still
                // in flight from before would otherwise drop THAT marker: the
                // chunks that follow find no garbage row, `continue`, and every
                // pid-keyed row the wider delete had not reached yet — the
                // partition, its `seg_loc`, its dedup, its counters — is
                // stranded for the life of the node.
                if g.scope == *scope {
                    self.writes.del_garbage(*pid)?;
                }
                budget = budget.saturating_sub(1);
                continue;
            }
            let start = if g.resume.is_empty() {
                resume.to_vec()
            } else {
                g.resume.clone()
            };
            let (spent, next) = self.sweep_pid(*pid, &start, budget)?;
            budget = budget.saturating_sub(spent);
            match next {
                Some(k) => {
                    g.resume = k;
                    self.writes.put_garbage(*pid, &g)?;
                }
                None => {
                    // Everything pid-keyed is gone; the partition row and the
                    // garbage marker go with it.
                    self.partition_delete(*pid)?;
                    self.writes.del_garbage(*pid)?;
                }
            }
        }
        Ok(())
    }

    /// The `(pid, group)` rows of a consumer-group delete.
    fn group_scope_chunk(&mut self, pid: Pid, group: &str) -> Result<()> {
        let s = shard_of(pid, self.shards.len());
        self.shards[s].cursor_delete(&mut self.writes, pid, group)?;
        let prefix = keys::dlq_by_pos_prefix(pid, group);
        let mut ids: Vec<[u8; 16]> = Vec::new();
        let mut bad = false;
        let mut names: Option<(String, String)> = None;
        if let Some(p) = self.writes.partition(pid)? {
            names = Some((p.tenant.clone(), p.queue.clone()));
        }
        self.writes.scan_raw(
            Keyspace::DlqByPos,
            &prefix,
            &prefix,
            usize::MAX,
            &mut |_k, v| {
                match rows::dlq_ids_decode(v) {
                    Ok(list) => ids.extend(list),
                    Err(_) => bad = true,
                }
                true
            },
        )?;
        if bad {
            return Err(StoreError::corrupt(Keyspace::DlqByPos, "dlq id list").into());
        }
        if let Some((tenant, queue)) = names {
            let mut gone = 0i64;
            for id in &ids {
                if let Some(row) = self.writes.dlq(&tenant, &queue, id)? {
                    self.writes.del_dlq(&tenant, &queue, id, &row)?;
                    gone += 1;
                }
            }
            // A group delete takes the group's dead letters with it, and
            // the count they were carrying goes too: the queue survives the
            // group, and its `dlq_count` is a gauge of the rows that exist.
            self.settle_dlq(pid, &tenant, &queue, gone)?;
        }
        self.sweep(Keyspace::DlqByPos, &prefix)?;
        Ok(())
    }

    /// The staged sweep of one garbage pid. Returns `(rows deleted, the next
    /// resume key)`; `None` means this pid is finished.
    fn sweep_pid(
        &mut self,
        pid: Pid,
        resume: &[u8],
        budget: usize,
    ) -> Result<(usize, Option<Vec<u8>>)> {
        /// The stages, in the order a chunk walks them. The byte is stored in
        /// the resume key, so it is PERMANENT.
        const CURSORS: u8 = 0;
        const DLQ: u8 = 1;
        const SEGLOC: u8 = 2;
        const DEDUP: u8 = 3;
        const TXNS: u8 = 4;
        const PARTITION_FILES: u8 = 5;
        const COUNTERS: u8 = 6;
        const DONE: u8 = 7;

        let (mut stage, mut from) = match resume.split_first() {
            Some((s, rest)) => (*s, rest.to_vec()),
            None => (CURSORS, Vec::new()),
        };
        let mut spent = 0usize;
        while spent < budget {
            let left = budget - spent;
            let (n, next) = match stage {
                CURSORS => self.sweep_cursors(pid, &from, left)?,
                DLQ => self.sweep_dlq(pid, &from, left)?,
                SEGLOC => self.sweep_seg_loc(pid, &from, left)?,
                DEDUP => {
                    let prefix = keys::dedup_prefix(pid);
                    self.sweep_step(Keyspace::Dedup, &prefix, &from, left)?
                }
                TXNS => {
                    let prefix = keys::txns_prefix(pid);
                    self.sweep_step(Keyspace::Txns, &prefix, &from, left)?
                }
                PARTITION_FILES => {
                    let prefix = keys::partition_files_prefix(pid);
                    self.sweep_step(Keyspace::PartitionFiles, &prefix, &from, left)?
                }
                COUNTERS => {
                    let prefix = keys::counter_partition_prefix(pid);
                    // PERF-D: drop any pending counter delta of this partition
                    // too, from every overlay, so a flush cannot recreate a row
                    // this chunk sweeps (including one bumped this window that
                    // never committed).
                    self.forget_counters(&prefix);
                    self.sweep_step(Keyspace::Counters, &prefix, &from, left)?
                }
                _ => return Ok((spent, None)),
            };
            spent += n;
            self.stats.rows_swept += n as u64;
            match next {
                Some(k) => {
                    from = k;
                    let mut key = Vec::with_capacity(from.len() + 1);
                    key.push(stage);
                    key.extend_from_slice(&from);
                    if spent >= budget {
                        return Ok((spent, Some(key)));
                    }
                }
                None => {
                    stage += 1;
                    from = Vec::new();
                    if stage == DONE {
                        return Ok((spent, None));
                    }
                }
            }
        }
        let mut key = Vec::with_capacity(from.len() + 1);
        key.push(stage);
        key.extend_from_slice(&from);
        Ok((spent, Some(key)))
    }

    /// One bounded step over a plain pid-keyed prefix.
    fn sweep_step(
        &mut self,
        ks: Keyspace,
        prefix: &[u8],
        from: &[u8],
        limit: usize,
    ) -> Result<(usize, Option<Vec<u8>>)> {
        let start = if from.is_empty() { prefix } else { from };
        let (n, next) = self.writes.delete_range(ks, start, prefix, limit)?;
        Ok((n, next))
    }

    /// Cursors, with their `leases_by_worker` twins.
    fn sweep_cursors(
        &mut self,
        pid: Pid,
        from: &[u8],
        limit: usize,
    ) -> Result<(usize, Option<Vec<u8>>)> {
        let prefix = keys::cursors_prefix(pid);
        let start = if from.is_empty() {
            prefix.clone()
        } else {
            from.to_vec()
        };
        let mut victims: Vec<String> = Vec::new();
        self.writes
            .scan_raw(Keyspace::Cursors, &start, &prefix, limit, &mut |k, _v| {
                if let Some(g) = keys::cursors_group_of(k) {
                    victims.push(g);
                }
                true
            })?;
        let s = shard_of(pid, self.shards.len());
        for g in &victims {
            self.shards[s].cursor_delete(&mut self.writes, pid, g)?;
        }
        let next = if victims.len() == limit {
            victims.last().and_then(|g| {
                crate::rsm::store::resume_after(&keys::cursors(pid, g), self.store.max_key_len())
            })
        } else {
            None
        };
        Ok((victims.len(), next))
    }

    /// The `(pid, group, offset)` index and the dead letters it names.
    fn sweep_dlq(
        &mut self,
        pid: Pid,
        from: &[u8],
        limit: usize,
    ) -> Result<(usize, Option<Vec<u8>>)> {
        let prefix = keys::dlq_by_pos_pid_prefix(pid);
        let start = if from.is_empty() {
            prefix.clone()
        } else {
            from.to_vec()
        };
        let names = self
            .writes
            .partition(pid)?
            .map(|p| (p.tenant.clone(), p.queue.clone()));
        let mut ids: Vec<[u8; 16]> = Vec::new();
        let mut bad = false;
        self.writes
            .scan_raw(Keyspace::DlqByPos, &start, &prefix, limit, &mut |_k, v| {
                match rows::dlq_ids_decode(v) {
                    Ok(list) => ids.extend(list),
                    Err(_) => bad = true,
                }
                true
            })?;
        if bad {
            return Err(StoreError::corrupt(Keyspace::DlqByPos, "dlq id list").into());
        }
        if let Some((tenant, queue)) = names {
            let mut gone = 0i64;
            for id in &ids {
                if let Some(row) = self.writes.dlq(&tenant, &queue, id)? {
                    self.writes.del_dlq(&tenant, &queue, id, &row)?;
                    gone += 1;
                }
            }
            self.settle_dlq(pid, &tenant, &queue, gone)?;
        }
        let (n, next) = self
            .writes
            .delete_range(Keyspace::DlqByPos, &start, &prefix, limit)?;
        Ok((n, next))
    }

    /// Payload positions: every row releases its frame's two claims before it
    /// goes, or the file would never die (§11.7).
    fn sweep_seg_loc(
        &mut self,
        pid: Pid,
        from: &[u8],
        limit: usize,
    ) -> Result<(usize, Option<Vec<u8>>)> {
        let prefix = keys::seg_loc_prefix(pid);
        let start = if from.is_empty() {
            prefix.clone()
        } else {
            from.to_vec()
        };
        // The watermark tells each row which claims it still holds; a
        // partition whose row is already gone can only be one whose frames
        // hold both (nothing moved its watermarks after it).
        let log_start = self
            .writes
            .partition(pid)?
            .map(|p| p.log_start)
            .unwrap_or(0);
        let mut rows: Vec<(u64, SegLocRow)> = Vec::new();
        let mut bad = false;
        self.writes.scan_raw(
            Keyspace::SegLoc,
            &start,
            &prefix,
            limit,
            &mut |k, v| match (keys::seg_loc_base_of(k), rows::seg_loc_decode(v)) {
                (Some(base), Ok(row)) => {
                    rows.push((base, row));
                    true
                }
                _ => {
                    bad = true;
                    false
                }
            },
        )?;
        if bad {
            return Err(StoreError::corrupt(Keyspace::SegLoc, "seg_loc row").into());
        }
        for (base, row) in &rows {
            self.release_seg_loc(row, release_for(*base, log_start));
            self.writes.del_seg_loc(pid, *base)?;
        }
        let next = if rows.len() == limit {
            rows.last().and_then(|(base, _)| {
                crate::rsm::store::resume_after(
                    &keys::seg_loc(pid, *base),
                    self.store.max_key_len(),
                )
            })
        } else {
            None
        };
        Ok((rows.len(), next))
    }

    /// Delete every row under a prefix, in bounded chunks. The chunk is a
    /// BUFFER bound, not a work bound: the rows deleted are the same on every
    /// node whatever [`SWEEP_CHUNK`] is.
    fn sweep(&mut self, ks: Keyspace, prefix: &[u8]) -> Result<usize> {
        let mut from = prefix.to_vec();
        let mut total = 0usize;
        loop {
            let (n, next) = self.writes.delete_range(ks, &from, prefix, SWEEP_CHUNK)?;
            total += n;
            match next {
                Some(k) if n == SWEEP_CHUNK => from = k,
                _ => break,
            }
        }
        self.stats.rows_swept += total as u64;
        Ok(total)
    }

    // -- counters ----------------------------------------------------------

    /// Add `delta` to a counter, through this thread's overlay (PERF-D).
    fn ctr_add(&mut self, key: &[u8], delta: i64) -> Result<()> {
        self.counters.add(&mut self.writes, key, delta)?;
        Ok(())
    }

    /// A counter read that sees every overlay's pending deltas (PERF-D): the
    /// value apply itself must observe inside the transaction (a settle that
    /// subtracts a queue's held bytes from the tenant, for instance). The
    /// shards' overlays hold the bumps of their pids' effects, so all of them
    /// are folded in; sum and max make the order immaterial.
    fn ctr_read(&self, key: &[u8]) -> Result<i64> {
        let base = self.writes.counter_at(key)?;
        let v = self.counters.fold(base, key);
        Ok(self.shards.iter().fold(v, |v, s| s.ctr.fold(v, key)))
    }

    /// Drop every overlay's pending deltas under `prefix`: the rows are about
    /// to be swept, and a later flush must not recreate them.
    fn forget_counters(&mut self, prefix: &[u8]) {
        self.counters.forget_prefix(prefix);
        for s in &mut self.shards {
            s.ctr.forget_prefix(prefix);
        }
    }

    /// Sweep a counter prefix AND drop every overlay's pending deltas under
    /// it, so a later flush cannot recreate a row this delete just removed
    /// (PERF-D).
    fn ctr_sweep(&mut self, prefix: &[u8]) -> Result<usize> {
        self.forget_counters(prefix);
        self.sweep(Keyspace::Counters, prefix)
    }

    /// Write every overlay into the open transaction (before a commit, a
    /// durable point, and so before any digest). A key several overlays hold
    /// is written once per overlay; sums and maxima commute, so the rows are
    /// the one-overlay path's (I2).
    fn flush_counters(&mut self) -> Result<()> {
        self.counters.flush(&mut self.writes)?;
        // The partition-scope keys — one per partition an entry touched, the
        // bulk of a flush at a million partitions (it was 32% of the apply
        // thread at 10M) — go on the shards' threads, each shard its own
        // pids' keys through a shard writer of its own. The shared keys
        // (queue, tenant, group scope) several shards may hold stay for the
        // write handle below, one overlay after another.
        let pending: usize = self.shards.iter().map(|s| s.ctr.len()).sum();
        if let Some(pool) = self.pool.as_mut() {
            if pending >= PARALLEL_FLUSH_MIN {
                let mut writers: Vec<S::Shard<'s>> = Vec::with_capacity(self.shards.len() - 1);
                for _ in 1..self.shards.len() {
                    writers.push(self.store.shard_writer()?);
                }
                let n = self.shards.len();
                let mut errs: Vec<Option<StoreError>> = vec![None; n];
                {
                    let (first, rest) = self.shards.split_at_mut(1);
                    let (err0, errs_rest) = errs.split_at_mut(1);
                    let mut jobs: Vec<_> = rest
                        .iter_mut()
                        .zip(writers.iter_mut())
                        .zip(errs_rest.iter_mut())
                        .enumerate()
                        .map(|(i, ((s, w), err))| {
                            move || {
                                if let Err(e) = s.ctr.flush_partition_scope(w, i + 1, n) {
                                    *err = Some(e);
                                }
                            }
                        })
                        .collect();
                    let mut refs: Vec<&mut (dyn FnMut() + Send)> = jobs
                        .iter_mut()
                        .map(|j| j as &mut (dyn FnMut() + Send))
                        .collect();
                    let writes = &mut self.writes;
                    let own = &mut first[0];
                    pool.scope(&mut refs, &mut || {
                        if let Err(e) = own.ctr.flush_partition_scope(writes, 0, n) {
                            err0[0] = Some(e);
                        }
                    });
                }
                drop(writers);
                if let Some(e) = errs.into_iter().flatten().next() {
                    return Err(e.into());
                }
            }
        }
        for s in &mut self.shards {
            s.ctr.flush(&mut self.writes)?;
        }
        Ok(())
    }

    /// Bound the shards' queue caches (between entries, at a commit).
    fn trim_caches(&mut self) {
        for s in &mut self.shards {
            s.queues.trim();
        }
    }

    /// Forget what every shard's queue cache read for `(tenant, queue)`: its
    /// configuration or its group set changed (B41).
    fn invalidate_queue(&mut self, tenant: &str, queue: &str) {
        for s in &mut self.shards {
            s.queues.invalidate(tenant, queue);
        }
    }

    /// The id of the `queues` row a partition belongs to as state stands, or
    /// `None` if either row is gone.
    ///
    /// Recorded on the garbage row when a pid is deleted, and compared to the
    /// live row afterwards: it is what tells a dead queue's leftovers from the
    /// live queue that took its name (see [`GarbageRow::queue_id`]).
    fn queue_id_of(&self, pid: Pid) -> Result<Option<[u8; 16]>> {
        let Some(p) = self.writes.partition(pid)? else {
            return Ok(None);
        };
        Ok(self.writes.queue(&p.tenant, &p.queue)?.map(|c| c.id))
    }

    /// [`shard::settle_dlq_count`] through this thread's overlay.
    fn settle_dlq(&mut self, pid: Pid, tenant: &str, queue: &str, gone: i64) -> Result<()> {
        shard::settle_dlq_count(
            &mut self.writes,
            &mut self.counters,
            &mut self.key,
            pid,
            tenant,
            queue,
            gone,
        )
    }

    /// Take a partition's share out of the `pending` gauge of every group of
    /// its queue (§6.4).
    ///
    /// `pending` is group-scoped and is maintained as "pushed minus
    /// completed": an `Append` adds the frames for every subscribed group, a
    /// `CursorSet` takes back what it acked. A partition that ceases to exist
    /// therefore has to hand back what it was still carrying —
    /// `last_offset − committed` for each group, with `committed = −1` where
    /// the group never claimed from it — or the queue reports lag for a
    /// partition that is gone.
    ///
    /// Only groups that STILL EXIST are touched: a queue or group delete swept
    /// their counter rows already, and adding to a swept row would recreate
    /// it. A group registered after those appends carries a smaller share than
    /// this (nothing seeds its gauge at registration); making the two exact for
    /// every subscription mode is WP-2.6's, against §8's seeding rules.
    fn settle_group_pending(&mut self, pid: Pid) -> Result<()> {
        let Some(p) = self.writes.partition(pid)? else {
            return Ok(());
        };
        if p.last_offset < 0 {
            return Ok(());
        }
        let (tenant, queue) = (p.tenant.clone(), p.queue.clone());
        let mut groups: Vec<String> = Vec::new();
        self.writes
            .scan_groups(&tenant, &queue, usize::MAX, &mut |g, _row| {
                groups.push(g.to_string());
                true
            })?;
        for g in &groups {
            let committed = self
                .writes
                .cursor(pid, g)?
                .map(|c| c.committed)
                .unwrap_or(-1);
            let share = p.last_offset - committed;
            if share != 0 {
                self.ctr_add(
                    &keys::counter_group(&tenant, &queue, g, Counter::Pending),
                    -share,
                )?;
            }
        }
        Ok(())
    }

    // -- cadences ----------------------------------------------------------

    /// Is a store commit due (§11.3)?
    pub fn commit_due(&self, since_last: Duration) -> bool {
        self.dirty
            && (self.entries_since_commit >= self.cfg.store_commit_entries
                || since_last >= Duration::from_millis(self.cfg.store_commit_ms))
    }

    /// Is a durable point due (§11.4)?
    pub fn durable_due(&self, since_last: Duration) -> bool {
        self.applied_index > self.durable_index
            && (self.segments.unsynced_bytes() >= self.cfg.durable_every_bytes
                || since_last >= Duration::from_millis(self.cfg.durable_every_ms))
    }

    /// End the open transaction and start the next one (§11.3). NON-DURABLE:
    /// the Raft log is the write-ahead log, so nothing is lost between
    /// commits.
    ///
    /// I11 lives here: the applied index was written by [`Applier::apply`] and
    /// the lengths and liveness of every file the entries touched are written
    /// NOW, in the same transaction. A commit that recorded one without the
    /// other would let recovery truncate a file below a `seg_loc` row that
    /// survived.
    pub fn commit(&mut self) -> Result<()> {
        self.usable()?;
        match self.commit_inner() {
            Ok(()) => Ok(()),
            Err(e) => Err(self.poison(e)),
        }
    }

    fn commit_inner(&mut self) -> Result<()> {
        self.record_files()?;
        let seals = self.record_seals()?;
        // PERF-D: fold the transaction's accumulated counter deltas — every
        // overlay's — into the store before it commits, so the committed rows
        // (and the digest a reader takes after) are exactly the per-bump
        // path's. The queue caches stay (B41): every catalogue write
        // invalidates what it changed, so they are not the transaction's.
        self.flush_counters()?;
        self.trim_caches();
        // The qlog is a WAL. On the LIVE A3b path the EXTERNAL writer already
        // fsync'd every `Append`'s record BEFORE its raft-log entry was made
        // durable — the record of any applied entry is on the platter, and the
        // `qlog.record_written`/`qlog.record_fsynced` crash points fire THERE (on
        // the writer), not here. So apply does not sync the qlog on this path. On
        // the unit-test / A1-A2-A3a path (`Some`) apply owns the qlog and fsyncs
        // it now, before the store commit records the applied index.
        if let Some(qlog) = self.qlog.as_mut() {
            qlog.sync().map_err(ApplyError::Qlog)?;
        }
        // Record the qlog-durable index from the highest applied `Append` index
        // (NA-QLOG-I1). It is durable on both paths — fsync'd by the external
        // writer before the entry, or by the `sync` just above — so recovery may
        // reconcile the reopened qlog's tail against it. Gated on the knob (else
        // there is no qlog at all).
        if self.cfg.qlog {
            self.writes
                .set_meta_u64(meta::QLOG_DURABLE_INDEX, self.last_append_index)?;
        }
        self.writes.commit()?;
        // §13.5 `apply.store_committed`: the (non-durable) store commit that
        // carries the applied index and the segment file lengths has landed;
        // the files themselves are NOT fsynced. A `kill -9` here keeps the page
        // cache, so the store reopens AT this applied index and recovery
        // truncates the files to the recorded lengths (I11). A power loss
        // returns to the last DURABLE commit instead, and the log replays.
        crate::rsm::faults::hit("apply.store_committed");
        self.stats.commits += 1;
        self.entries_since_commit = 0;
        self.dirty = false;
        // The caller's commit has landed, so the sealed files' RAM indexes are
        // no longer the only place their frames can be found.
        for (b, id) in seals {
            self.segments.forget_sealed(b, id);
        }
        Ok(())
    }

    /// The durable point of §11.4, in the order the plan gives:
    ///
    /// 1. fsync every segment file written since the last point (sealed files
    ///    once) and their directories;
    /// 2. a DURABLE store commit carrying `meta.applied_index/term` and the
    ///    file lengths — the atomic half of I11;
    /// 3. report the durable index.
    ///
    /// Step 3 happens only if step 2 returned: a durable index that never
    /// reached the platter would let a replicator truncate its log behind
    /// acknowledged effects nothing can replay, and on Linux the failing fsync
    /// is the ONLY moment at which that is visible (the kernel drops the
    /// pages and the next sync succeeds).
    pub fn durable_point(&mut self) -> Result<u64> {
        self.usable()?;
        match self.durable_point_inner() {
            Ok(i) => Ok(i),
            Err(e) => Err(self.poison(e)),
        }
    }

    fn durable_point_inner(&mut self) -> Result<u64> {
        if let Some(i) = self.ckpt_inflight {
            return Err(ApplyError::Inconsistent {
                what: "durable point",
                detail: format!("the checkpoint of {i} is still being written"),
            });
        }
        let seals = self.prepare_point()?;
        self.commit_point_inline(seals)
    }

    /// Steps 1 and 2's preparation (§11.4), shared by the inline point and the
    /// async cut: fsync the segment files written since the last point and
    /// write every row the point records — file lengths, seals, counters, the
    /// durable and applied indexes. Returns the sealed files whose rows are now
    /// written.
    fn prepare_point(&mut self) -> Result<Vec<(u16, u32)>> {
        let point = self.segments.durable_point()?;
        // Fsync the qlog alongside the segment files ONLY on the unit-test /
        // A1-A2-A3a path (`Some`). On the LIVE path the external writer owns the
        // queue logs and fsyncs them — but on a cluster an entry applies once it
        // is WRITTEN (`log_store::Written`: a quorum of other nodes may commit it
        // first), possibly before this node's fsync. The point below records the
        // image of every entry through `applied_index`, so it waits for the
        // writer's fsync to pass them: an image ahead of the log on the platter
        // is a node that a power loss leaves unable to boot.
        if let Some(qlog) = self.qlog.as_mut() {
            qlog.sync().map_err(ApplyError::Qlog)?;
        }
        if let Some(tails) = &self.qlog_tails {
            tails
                .wait_synced(self.applied_index)
                .map_err(ApplyError::Qlog)?;
        }
        // §13.5 `durable.files_synced`: every segment file written since the
        // last point is fsynced (step 1, §11.4), the durable store commit that
        // records their lengths (step 2) has NOT landed. A crash here reopens
        // at the last durable store commit (the previous point); the files are
        // safe on disk but longer than the store records, and recovery
        // truncates them back and replays the log tail (I11).
        crate::rsm::faults::hit("durable.files_synced");
        for f in &point.files {
            self.put_file_state(f)?;
        }
        self.record_files()?;
        let seals = self.record_seals()?;
        // PERF-D: the durable point must carry every counter delta on the
        // platter — a reader (and the digest) takes committed state after it —
        // so fold every overlay in before the durable commit.
        self.flush_counters()?;
        self.trim_caches();
        self.writes
            .set_meta_u64(meta::DURABLE_INDEX, self.applied_index)?;
        // NA-QLOG-I1: record the qlog-durable index from the highest applied
        // `Append` index (durable on both paths — see `commit_inner`).
        if self.cfg.qlog {
            self.writes
                .set_meta_u64(meta::QLOG_DURABLE_INDEX, self.last_append_index)?;
        }
        // Each queue log's own fsync'd, applied tail: a reopen refuses a log
        // that comes back shorter (QLogSet::check_tails).
        if let Some(tails) = &self.qlog_tails {
            for (log, seq) in tails.take_committed(self.last_append_index) {
                self.writes.set_meta_u64(&meta::qlog_tail_key(log), seq)?;
            }
        }
        self.writes
            .set_applied(self.applied_index, self.applied_term)?;
        Ok(seals)
    }

    /// Step 2's commit and step 3, inline: the durable store commit, then the
    /// durable index reported and GC phase two.
    fn commit_point_inline(&mut self, seals: Vec<(u16, u32)>) -> Result<u64> {
        match self.writes.durable_commit() {
            Ok(()) => {}
            Err(e) => {
                self.stats.durable_points_failed += 1;
                tracing::error!(
                    target: "rsm",
                    error = %e,
                    lost_durable_point = e.lost_durable_point(),
                    "the durable point did not happen; no durable index is reported",
                );
                return Err(e.into());
            }
        }
        self.entries_since_commit = 0;
        self.dirty = false;
        self.durable_index = self.applied_index;
        self.stats.commits += 1;
        self.stats.durable_points += 1;
        // §13.5 `durable.store_committed`: the durable point is complete (files
        // fsynced AND the durable store commit landed); the entries after this
        // index are not durable yet. A crash here reopens exactly here; nothing
        // owed to this point is lost, and phase two of GC below simply runs
        // again on the next point (I10, I11).
        crate::rsm::faults::hit("durable.store_committed");
        for (b, id) in seals {
            self.segments.forget_sealed(b, id);
        }
        // Phase two of GC: the durable commit above no longer names these
        // files, so their bytes may go (I10).
        self.unlink_staged()?;
        self.notify.durable(self.durable_index);
        Ok(self.durable_index)
    }

    /// Whether an async durable point's cut is being written.
    pub fn checkpoint_in_flight(&self) -> bool {
        self.ckpt_inflight.is_some()
    }

    /// The async durable point (`ApplyConfig::checkpoint_async`), first half:
    /// prepare the point exactly as the inline one does, then take the store's
    /// CUT instead of committing it here. Returns the cut and the index it
    /// covers, for the checkpoint thread — or, when the store cannot cut, runs
    /// the whole point inline and returns its durable index.
    ///
    /// Until [`Applier::checkpoint_done`] reports the cut written: no durable
    /// index is reported (step 3), the files the cut stopped naming stay on
    /// disk (I10), and no other point starts.
    pub fn begin_checkpoint(&mut self) -> Result<PointStart> {
        self.usable()?;
        if !self.writes.can_cut() {
            return self.durable_point().map(PointStart::Inline);
        }
        match self.begin_checkpoint_inner() {
            Ok(v) => Ok(v),
            Err(e) => Err(self.poison(e)),
        }
    }

    fn begin_checkpoint_inner(&mut self) -> Result<PointStart> {
        if let Some(i) = self.ckpt_inflight {
            return Err(ApplyError::Inconsistent {
                what: "durable point",
                detail: format!("the checkpoint of {i} is still being written"),
            });
        }
        let seals = self.prepare_point()?;
        let Some(cut) = self.writes.take_cut()? else {
            // `can_cut` said yes a moment ago; finish this point inline rather
            // than leave it prepared and uncommitted.
            return self.commit_point_inline(seals).map(PointStart::Inline);
        };
        // The cut carries everything a plain commit would: its cadence restarts.
        self.entries_since_commit = 0;
        self.dirty = false;
        self.stats.commits += 1;
        // Every row is RAM and read live, so the sealed files' rows are already
        // where a reader looks: their RAM indexes go now, as after a plain
        // commit.
        for (b, id) in seals {
            self.segments.forget_sealed(b, id);
        }
        // GC phase two waits for THIS cut to be durable: the files staged or
        // deferred so far lost their rows in it (I10).
        let mut owed = std::mem::take(&mut self.gc_staged);
        owed.append(&mut self.gc_deferred);
        self.gc_inflight = owed;
        self.ckpt_inflight = Some(self.applied_index);
        Ok(PointStart::Cut(cut, self.applied_index))
    }

    /// The async durable point, second half: the checkpoint thread wrote and
    /// synced the cut of `index` (or failed to). Success is step 3 of §11.4 —
    /// `index` is reported durable — and GC phase two for the files the cut
    /// stopped naming. A failure is a durable point that did not happen: no
    /// durable index, and the node stops, as on the inline path.
    pub fn checkpoint_done(
        &mut self,
        index: u64,
        result: std::result::Result<(), StoreError>,
    ) -> Result<()> {
        self.usable()?;
        if self.ckpt_inflight != Some(index) {
            let e = ApplyError::Inconsistent {
                what: "checkpoint",
                detail: format!(
                    "the cut of {index} landed while {:?} was in flight",
                    self.ckpt_inflight
                ),
            };
            return Err(self.poison(e));
        }
        self.ckpt_inflight = None;
        if let Err(e) = result {
            self.stats.durable_points_failed += 1;
            tracing::error!(
                target: "rsm",
                error = %e,
                lost_durable_point = e.lost_durable_point(),
                "the durable point did not happen (checkpoint thread); no durable index is reported",
            );
            return Err(self.poison(e.into()));
        }
        self.durable_index = index;
        self.stats.durable_points += 1;
        // §13.5 `durable.store_committed`, as on the inline path.
        crate::rsm::faults::hit("durable.store_committed");
        let owed = std::mem::take(&mut self.gc_inflight);
        if let Err(e) = self.unlink_set(owed) {
            return Err(self.poison(e));
        }
        self.notify.durable(index);
        Ok(())
    }

    /// Everything the maintenance tick does between entries: the durable
    /// point when it is due, then one pass of file GC (§11.7).
    pub fn maintenance(&mut self, since_commit: Duration, since_durable: Duration) -> Result<()> {
        if self.durable_due(since_durable) {
            self.durable_point()?;
        } else if self.commit_due(since_commit) {
            self.commit()?;
        }
        self.gc_pass()?;
        Ok(())
    }

    /// Write the file table rows for every file whose length or liveness
    /// changed since the last commit (I11).
    fn record_files(&mut self) -> Result<()> {
        for f in self.segments.take_touched() {
            self.put_file_state(&f)?;
        }
        Ok(())
    }

    fn put_file_state(&mut self, f: &FileState) -> Result<()> {
        // A file GC has already staged is one the store must stop naming
        // (I10). Its row was deleted in phase one and its bytes go at the next
        // durable point; writing it back here — from a `release` that emptied
        // it, in the very commit that is supposed to free it — is what once
        // left a `files` row pointing at a file this same durable point had
        // just unlinked, and a node that refused to start after an ordinary
        // retention cycle.
        if self.gc_holds(f.bucket, f.file_id) {
            return Ok(());
        }
        self.writes.put_file(
            f.bucket,
            f.file_id,
            &FileRow {
                len: f.len,
                durable_len: f.durable_len,
                sealed: f.sealed,
                frames: f.frames,
                retained_frames: f.retained_frames,
                retained_bytes: f.retained_bytes,
                window_frames: f.window_frames,
                snapshot_refs: f.snapshot_refs,
            },
        )?;
        Ok(())
    }

    /// Write the `partition_files` rows of every file that has sealed since
    /// the last commit: one row per partition inside it, written once (§6.1's
    /// G0 amendment), instead of one row per append.
    ///
    /// Returns the files whose rows are now in the transaction, so the caller
    /// can retire their RAM indexes AFTER the commit lands — not before: until
    /// then the RAM copy is the only place a reader can find those frames.
    fn record_seals(&mut self) -> Result<Vec<(u16, u32)>> {
        let mut done = Vec::new();
        for (b, id) in self.segments.unrecorded_seals() {
            if self.recorded_seals.contains(&(b, id)) {
                done.push((b, id));
                continue;
            }
            // A file can die before the commit that records its seal: one
            // entry rolls it and the next moves both watermarks past
            // everything in it, and GC phase one then stages it. Writing its
            // rows now would put them in the very commit that deleted them,
            // for a file the durable point is about to unlink. It keeps its
            // RAM index (nothing calls `forget_sealed` for it) until the
            // unlink drops it.
            if self.gc_holds(b, id) {
                continue;
            }
            for pid in self.segments.pids_in(b, id)? {
                self.writes.put_partition_file(pid, id)?;
            }
            self.recorded_seals.insert((b, id));
            done.push((b, id));
        }
        Ok(done)
    }

    // -- file GC (§11.7, I10) ----------------------------------------------

    /// GC phase one has taken this file's rows out and the store must not name
    /// it again — whether its unlink is still owed to the next durable point
    /// or a pin has already deferred it once.
    fn gc_holds(&self, bucket: u16, file_id: u32) -> bool {
        self.gc_staged.contains(&(bucket, file_id))
            || self.gc_deferred.contains(&(bucket, file_id))
            || self.gc_inflight.contains(&(bucket, file_id))
    }

    /// Phase one: a dead, unpinned file loses its node-local rows. The file
    /// itself is unlinked at the next durable point, when no durable commit
    /// references it any more.
    ///
    /// Public because §11.7 runs it BETWEEN entries: the loop calls it on
    /// every turn, and a caller driving apply by hand (the crash matrix, a
    /// test) has to be able to do the same.
    pub fn gc_pass(&mut self) -> Result<()> {
        self.usable()?;
        // Only the files waiting for their FIRST unlink count against the pass
        // bound. A file the last durable point could not unlink because a claim
        // pin holds it is in `gc_deferred` instead: it needs no room here, and
        // counting it would let `gc_per_pass` long-held pins stop GC for the
        // whole node.
        let Some(room) = self.cfg.gc_per_pass.checked_sub(self.gc_staged.len()) else {
            return Ok(());
        };
        if room == 0 {
            return Ok(());
        }
        // The walk costs the candidates it returns, not the files this node
        // holds: `Segments::gc_candidates` takes the bound (I8, §11.7).
        let candidates = self.segments.gc_candidates(room);
        for (b, id) in candidates {
            if self.gc_holds(b, id) {
                continue;
            }
            // Read from the file's own index, so a file that sealed before a
            // restart loses its rows too (a RAM map rebuilt empty by
            // [`Applier::open`] left one `partition_files` row per partition
            // behind, for ever, on every file collected after a restart).
            for pid in self.segments.pids_in(b, id)? {
                self.writes.del_partition_file(pid, id)?;
            }
            self.writes.del_file(b, id)?;
            // From here this file owes the store nothing: whatever put it in
            // the writer's touched set, the next commit must not record it
            // again (see [`Segments::untouch`] and [`Applier::put_file_state`]).
            self.segments.untouch(b, id);
            self.gc_staged.insert((b, id));
            self.dirty = true;
        }
        Ok(())
    }

    /// Phase two: unlink what the last durable commit no longer references.
    /// `unlink` re-checks the file's liveness and its pins, so a claim pinned
    /// between the two phases still wins (I4).
    fn unlink_staged(&mut self) -> Result<()> {
        // Both lists: the files staged since the last point and the ones a pin
        // held back at it. A deferred file has no rows left either, so it is
        // tried again here and nowhere else.
        let mut staged = std::mem::take(&mut self.gc_staged);
        staged.append(&mut self.gc_deferred);
        self.unlink_set(staged)
    }

    /// GC phase two over `staged`: files whose rows a DURABLE point no longer
    /// holds (the inline point's, or the async cut's once it is written).
    fn unlink_set(&mut self, staged: BTreeSet<(u16, u32)>) -> Result<()> {
        for (b, id) in staged {
            // §13.5 `gc.before_unlink`: a durable store commit no longer
            // references this file (its rows are gone); the file is still on
            // disk. A crash here leaves an orphan file that recovery sweeps
            // because no row names it (I10) — never a referenced file gone.
            crate::rsm::faults::hit("gc.before_unlink");
            match self.segments.unlink(b, id) {
                Ok(true) => {
                    // §13.5 `gc.after_unlink`: the file is unlinked. Its rows
                    // were already removed in a durable commit, so a crash here
                    // is indistinguishable from a clean unlink: nothing points
                    // at it (I10).
                    crate::rsm::faults::hit("gc.after_unlink");
                    self.recorded_seals.remove(&(b, id));
                    self.stats.files_unlinked += 1;
                }
                Ok(false) => match self.segments.file_meta(b, id) {
                    // Still dead, so something holds it: a claim pin taken
                    // between the two phases (I4 beats I10, §11.7). It stays
                    // STAGED — its rows are gone and must stay gone — and the
                    // next durable point tries again. Dropping it here would
                    // leave a file on disk that no row names until the next
                    // boot deletes it.
                    Some(m) if m.is_dead() => {
                        self.gc_deferred.insert((b, id));
                        self.stats.gc_deferred += 1;
                    }
                    // Alive again (a snapshot manifest took a reference): it
                    // needs its row back, in the next commit, or recovery
                    // would delete a file the manifest hard-links.
                    Some(m) => {
                        let state = FileState::of(b, id, &m);
                        self.put_file_state(&state)?;
                        for pid in self.segments.pids_in(b, id)? {
                            self.writes.put_partition_file(pid, id)?;
                        }
                        self.dirty = true;
                    }
                    // Gone from the table without this call unlinking it:
                    // nothing left to say about it.
                    None => {}
                },
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Commit what is open and take a final durable point. Called when the
    /// channel closes and by a clean shutdown (§12.7).
    pub fn flush(&mut self) -> Result<()> {
        self.usable()?;
        if self.applied_index > self.durable_index {
            self.durable_point()?;
        } else if self.dirty {
            self.commit()?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The async durable-point pre-flush helper (§11.4, QUEEN_RAFT_DURABLE_ASYNC)
// ---------------------------------------------------------------------------

/// A helper thread that pushes the segment files' dirty pages to the device
/// between durable points, so the point on the apply thread finds little left
/// to flush and stalls the pipeline for less (PERF-A, §11.4).
///
/// It is a page-cache writeback warm-up and NOTHING else: it never records a
/// durable index, never advances the segment tree's bookkeeping, and never
/// touches the store. The durable point on the apply thread remains the only
/// recorded flush and re-fsyncs every file itself, so a crash between a
/// pre-flush and a point behaves exactly as if the helper never ran (I11). This
/// is why the helper needs no error channel back to apply: a failed warm-up
/// costs only a slower point, never correctness.
pub(crate) struct Preflusher {
    tx: SyncSender<PreflushMsg>,
    join: Option<std::thread::JoinHandle<()>>,
}

enum PreflushMsg {
    Batch(segments::PreflushBatch),
    Stop,
}

impl Preflusher {
    fn spawn() -> Preflusher {
        // A small bounded queue: submissions are best-effort, so when the
        // helper falls behind the apply thread drops the warm-up (`try_send`)
        // rather than blocking — the durable point still flushes everything.
        let (tx, rx) = std::sync::mpsc::sync_channel::<PreflushMsg>(4);
        let join = std::thread::Builder::new()
            .name("queen-rsm-preflush".into())
            .spawn(move || {
                while let Ok(msg) = rx.recv() {
                    match msg {
                        PreflushMsg::Batch(b) => b.sync(),
                        PreflushMsg::Stop => break,
                    }
                }
            })
            .expect("spawn the durable pre-flush thread");
        Preflusher {
            tx,
            join: Some(join),
        }
    }

    /// Hand the helper a snapshot of the dirty segment handles. Dropped on the
    /// floor if the helper is still busy with the last one (best-effort).
    fn submit(&self, batch: segments::PreflushBatch) {
        if batch.is_empty() {
            return;
        }
        let _ = self.tx.try_send(PreflushMsg::Batch(batch));
    }
}

impl Drop for Preflusher {
    fn drop(&mut self) {
        // A blocking send: the queue is tiny and the helper only ever fsyncs,
        // so Stop lands promptly. Joining keeps the thread from outliving the
        // store whose files it holds dup'd handles to.
        let _ = self.tx.send(PreflushMsg::Stop);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

// ---------------------------------------------------------------------------
// The async durable point's checkpoint thread (§11.4, QUEEN_RAFT_CHECKPOINT_ASYNC)
// ---------------------------------------------------------------------------

/// The thread that writes a durable point's store half — the cut apply took
/// ([`Applier::begin_checkpoint`]) — into the store, commits and syncs it, then
/// reports back ([`Applier::checkpoint_done`]). One cut at a time, in the
/// order they were taken: apply starts the next point only after the last one
/// reported.
///
/// The cut's values are shared with the live tables, so it is freed here, off
/// the apply thread; a cut that failed goes back to the dirty sets
/// ([`Store::restore_cut`]) before the failure is reported.
pub(crate) struct Checkpointer {
    tx: Option<SyncSender<CkptJob>>,
    done: Receiver<CkptDone>,
    join: Option<std::thread::JoinHandle<()>>,
}

struct CkptJob {
    cut: CheckpointCut,
    index: u64,
}

pub(crate) struct CkptDone {
    index: u64,
    result: std::result::Result<(), StoreError>,
    write: Duration,
    rows: usize,
}

impl Checkpointer {
    fn spawn<S: Store + 'static>(store: Arc<S>, clock: Arc<dyn Clock>) -> Checkpointer {
        let (tx, rx) = std::sync::mpsc::sync_channel::<CkptJob>(1);
        let (dtx, done) = std::sync::mpsc::channel::<CkptDone>();
        let join = std::thread::Builder::new()
            .name("queen-rsm-ckpt".into())
            .spawn(move || {
                while let Ok(mut job) = rx.recv() {
                    let t0 = clock.now();
                    let rows = job.cut.keys();
                    let result = store.write_cut(&mut job.cut);
                    let write = clock.now().duration_since(t0);
                    if result.is_err() {
                        store.restore_cut(job.cut);
                    } else {
                        drop(job.cut);
                    }
                    let d = CkptDone {
                        index: job.index,
                        result,
                        write,
                        rows,
                    };
                    if dtx.send(d).is_err() {
                        return;
                    }
                }
            })
            .expect("spawn the checkpoint thread");
        Checkpointer {
            tx: Some(tx),
            done,
            join: Some(join),
        }
    }

    fn submit(&self, cut: CheckpointCut, index: u64) -> Result<()> {
        let sent = self
            .tx
            .as_ref()
            .map(|tx| tx.send(CkptJob { cut, index }).is_ok())
            .unwrap_or(false);
        if sent {
            Ok(())
        } else {
            Err(ApplyError::Inconsistent {
                what: "checkpoint thread",
                detail: "the checkpoint thread is gone".into(),
            })
        }
    }

    /// Hand a finished cut's report to apply: the durable index, GC phase two,
    /// the timings.
    fn settle<S: Store>(applier: &mut Applier<'_, S>, d: CkptDone) -> Result<()> {
        if crate::rsm::timing::enabled() {
            let tm = crate::rsm::timing::metrics();
            tm.checkpoint_write.record_dur(d.write);
            tm.checkpoint_rows.record(d.rows as u64);
        }
        applier.checkpoint_done(d.index, d.result)
    }

    /// Settle every report that has arrived, without waiting.
    fn poll<S: Store>(&self, applier: &mut Applier<'_, S>) -> Result<()> {
        while let Ok(d) = self.done.try_recv() {
            Self::settle(applier, d)?;
        }
        Ok(())
    }

    /// Wait for the cut in flight, if any, and settle it.
    fn drain<S: Store>(&self, applier: &mut Applier<'_, S>) -> Result<()> {
        while applier.checkpoint_in_flight() {
            match self.done.recv() {
                Ok(d) => Self::settle(applier, d)?,
                Err(_) => {
                    return Err(ApplyError::Inconsistent {
                        what: "checkpoint thread",
                        detail: "the checkpoint thread stopped with a cut in flight".into(),
                    })
                }
            }
        }
        Ok(())
    }
}

impl Drop for Checkpointer {
    fn drop(&mut self) {
        // Closing the channel ends the thread after the cut it is writing (a
        // cut is never abandoned half-written: the transaction commits or
        // aborts as a whole). Joining keeps it from outliving the store.
        drop(self.tx.take());
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

// ---------------------------------------------------------------------------
// The thread
// ---------------------------------------------------------------------------

/// Run the apply loop until the channel closes.
///
/// It blocks — on the channel, on the store, on fsync — and that is correct:
/// this is a `std` thread of its own, never a tokio worker (I15). The park is
/// bounded by `idle_tick_ms` so the cadences run on an idle node too.
///
/// `preflush` is the async durable-point helper (§11.4); `None` runs every
/// durable point fully inline (`QUEEN_RAFT_DURABLE_ASYNC=0`, and the crash
/// matrix, which drives points by hand).
pub(crate) fn run<S: Store>(
    applier: &mut Applier<'_, S>,
    rx: &Receiver<Committed>,
    clock: &dyn Clock,
    preflush: Option<&Preflusher>,
    ckpt: Option<&Checkpointer>,
) -> Result<()> {
    let mut last_commit = clock.now();
    let mut last_durable = last_commit;
    let mut last_timing = last_commit;
    let mut last_preflush = last_commit;
    let timing_interval = crate::rsm::timing::timing_log_interval();
    let tick = Duration::from_millis(applier.cfg.idle_tick_ms.max(1));
    // The async warm-up (§11.4) fires on whichever comes first: every
    // `preflush_step` bytes (so a point due by BYTES — a fat push run — still
    // meets flushed files) or every `preflush_interval` (so a point due by TIME
    // does too, at any throughput). Both are a fraction of the durable triggers,
    // so each point finds most of its bytes already on the device. It is only a
    // warm-up: the point still fsyncs everything, so the cadence is a tuning
    // knob, never a correctness one.
    let preflush_step = (applier.cfg.durable_every_bytes / 16).clamp(1 << 20, 8 << 20);
    let preflush_interval = Duration::from_millis((applier.cfg.durable_every_ms / 8).max(20));
    let mut next_preflush = preflush_step;
    loop {
        match rx.recv_timeout(tick) {
            Ok(c) => {
                // PERF-1: the apply-channel depth at receive, then the per-entry
                // apply duration split into its segment `write_all` portion and
                // the rest — all timed through the INJECTED clock, because
                // apply.rs holds no clock of its own (I2). The knob gates the
                // clock reads THEMSELVES (`probe` is `None` when metrics are off),
                // not just the histogram write, so `QUEEN_RAFT_METRICS=0` pays
                // for neither `clock.now()` here nor the depth/segment gauges.
                let probe = crate::rsm::timing::enabled().then(|| {
                    crate::rsm::timing::apply_channel_recv();
                    (crate::rsm::timing::segment_write_ns_total(), clock.now())
                });
                applier.apply(&c)?;
                if let Some((seg_before, t0)) = probe {
                    let total = clock.now().duration_since(t0);
                    let total_ns = total.as_nanos().min(u64::MAX as u128) as u64;
                    let seg =
                        crate::rsm::timing::segment_write_ns_total().saturating_sub(seg_before);
                    let tm = crate::rsm::timing::metrics();
                    tm.apply_entry.record_dur(total);
                    tm.apply_other.record(total_ns.saturating_sub(seg));
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                // The final point is inline, after the cut in flight landed:
                // two checkpoints are never written at once.
                if let Some(c) = ckpt {
                    c.drain(applier)?;
                }
                applier.flush()?;
                return Ok(());
            }
        }
        // A cut the checkpoint thread finished since the last turn: its point
        // is durable now (step 3), and its GC phase two runs.
        if let Some(c) = ckpt {
            c.poll(applier)?;
        }
        // `now` drives the commit/durable/timing CADENCE (load-bearing); the
        // extra `clock.now()` around each stage is metrics-only, so it is gated
        // on the knob (`t0` is `None` when metrics are off).
        let now = clock.now();
        // Between points, push what has been written so far to the device on the
        // helper thread, so the point below has little left to flush. Checked
        // AFTER apply and BEFORE the point, so the last chunk of an interval is
        // warmed too; `unsynced_bytes` resets to 0 at the point, and both
        // thresholds follow it back down there.
        if let Some(pf) = preflush {
            let unsynced = applier.segments().unsynced_bytes();
            let by_bytes = unsynced >= next_preflush;
            let by_time = unsynced > 0 && now.duration_since(last_preflush) >= preflush_interval;
            if by_bytes || by_time {
                pf.submit(applier.segments().preflush_batch());
                next_preflush = unsynced.saturating_add(preflush_step);
                last_preflush = now;
            }
        }
        if applier.durable_due(now.duration_since(last_durable)) && !applier.checkpoint_in_flight()
        {
            let t0 = crate::rsm::timing::enabled().then(|| clock.now());
            match ckpt {
                Some(c) => match applier.begin_checkpoint()? {
                    PointStart::Cut(cut, index) => c.submit(cut, index)?,
                    PointStart::Inline(_) => {}
                },
                None => {
                    applier.durable_point()?;
                }
            }
            if let Some(t0) = t0 {
                crate::rsm::timing::metrics()
                    .durable_point
                    .record_dur(clock.now().duration_since(t0));
            }
            // The point cleared `unsynced_bytes`; the warm-up threshold follows.
            next_preflush = preflush_step;
            last_durable = now;
            last_commit = now;
        } else if applier.commit_due(now.duration_since(last_commit)) {
            let t0 = crate::rsm::timing::enabled().then(|| clock.now());
            applier.commit()?;
            if let Some(t0) = t0 {
                crate::rsm::timing::metrics()
                    .store_commit
                    .record_dur(clock.now().duration_since(t0));
            }
            last_commit = now;
        }
        applier.gc_pass()?;
        if !timing_interval.is_zero() && now.duration_since(last_timing) >= timing_interval {
            crate::rsm::timing::emit_timing_log(&applier.stats());
            last_timing = now;
        }
    }
}

/// Start the apply thread on its own `std` thread, owning the store.
///
/// The handle resolves when the channel closes (a clean shutdown) or when
/// apply refuses: a refusal is fatal for this node (§12.1 `Fatal`), and the
/// caller stops the Raft instance rather than carrying on.
pub fn spawn<S: Store + 'static>(
    store: Arc<S>,
    seg_root: std::path::PathBuf,
    seg_opts: segments::Options,
    cfg: ApplyConfig,
    notify: Arc<dyn Notify>,
    clock: Arc<dyn Clock>,
    rx: Receiver<Committed>,
) -> std::thread::JoinHandle<Result<ApplyStats>> {
    spawn_with_reader(
        store, seg_root, seg_opts, cfg, notify, clock, rx, None, None,
    )
}

/// [`spawn`], plus a one-shot sink the thread publishes the segment
/// [`segments::Reader`] into once it has opened the applier (WP-1.7c).
///
/// The apply thread is the only owner of the [`Segments`] writer, and the
/// facade (`rsm/facade`) must read pop payloads from the very same file set the
/// writer keeps appending to — its RAM active index and sealed `.qidx` files —
/// so a second, independently opened reader (which would rescan its own copy
/// and never see live appends) will not do (§7.5, D7). The reader shares the
/// writer's `Arc<Shared>`, so it is published HERE, right after `Applier::open`
/// (before any entry replays), and the caller ([`LocalReplicator::open`]) takes
/// it out of the sink and hands it to the facade. `None` is the pre-WP-1.7c
/// path and every test that does not read payloads back.
#[allow(clippy::too_many_arguments)]
pub fn spawn_with_reader<S: Store + 'static>(
    store: Arc<S>,
    seg_root: std::path::PathBuf,
    seg_opts: segments::Options,
    cfg: ApplyConfig,
    notify: Arc<dyn Notify>,
    clock: Arc<dyn Clock>,
    rx: Receiver<Committed>,
    reader_sink: Option<Arc<std::sync::OnceLock<segments::Reader>>>,
    qlog_tails: Option<Arc<crate::rsm::qlog::set::QlogTails>>,
) -> std::thread::JoinHandle<Result<ApplyStats>> {
    std::thread::Builder::new()
        .name("queen-rsm-apply".into())
        .spawn(move || {
            let (mut applier, rec) = Applier::open(&*store, &seg_root, seg_opts, cfg, notify)?;
            applier.qlog_tails = qlog_tails;
            if let Some(sink) = &reader_sink {
                // First-write-wins; there is only ever one apply thread per
                // replicator, so this sets exactly once.
                let _ = sink.set(applier.reader());
            }
            // A3b: the qlog reader is published by the BOOT thread from the
            // writer-owned qlog, not here — the applier no longer owns the qlog
            // on the live path.
            // The async durable-point helper (§11.4). Spawned per apply thread,
            // dropped (Stop + join) when `run` returns, so it never outlives the
            // segment handles it holds dup'd copies of.
            let preflush = cfg.durable_async.then(Preflusher::spawn);
            // The async durable point's store half (§11.4). Declared after the
            // applier, so it is dropped (Stop + join) first when `run` returns:
            // no cut outlives the thread, and the store is released with it.
            let ckpt = cfg
                .checkpoint_async
                .then(|| Checkpointer::spawn(store.clone(), clock.clone()));
            tracing::info!(
                target: "rsm",
                replay_after = rec.replay_after,
                applied = rec.applied_index,
                durable_async = cfg.durable_async,
                checkpoint_async = cfg.checkpoint_async,
                "rsm apply thread started",
            );
            run(
                &mut applier,
                &rx,
                clock.as_ref(),
                preflush.as_ref(),
                ckpt.as_ref(),
            )?;
            Ok(applier.stats())
        })
        .expect("spawn the apply thread")
}

/// A command's outcome kind, for the failure report.
fn outcome_name(o: &crate::rsm::entry::Outcome) -> &'static str {
    use crate::rsm::entry::Outcome;
    match o {
        Outcome::Empty => "empty",
        Outcome::Push(_) => "push",
        Outcome::Pop(_) => "pop",
        Outcome::Ack(_) => "ack",
        Outcome::Renew(_) => "renew",
        Outcome::DlqHead(_) => "dlq head",
        Outcome::Kv(_) => "kv",
        Outcome::Placeholder(_) => "admin",
    }
}

/// The most dead letters one watermark retires ([`Applier::trim_dlq`]).
const DLQ_TRIM_PER_WATERMARK: usize = 2048;

/// What a frame at `base` still holds when its partition is deleted.
///
/// Retention releases the payload of everything below `log_start` and leaves
/// the `seg_loc` row for the txns window (D10, §11.7), so only the hash-list
/// claim is left on those; everything at or above it still holds both.
fn release_for(base: u64, log_start: u64) -> Release {
    if base < log_start {
        Release::Window
    } else {
        Release::Both
    }
}

/// Every counter of one queue (the queue-scope rows a queue delete takes).
/// Whether a counter is kept at PARTITION scope too. Only the two a reader
/// asks a partition for: its retained bytes (the retention/size reads) and its
/// dead letters. The partition's pushed, completed, consumed and failed counts
/// and its last push/pop stamps had no reader (2026-09-30: only
/// `RetainedBytes` and `DlqCount` are read outside tests), and at one to ten
/// million partitions each was a counter row (~150 B) and a store
/// read-modify-write per event: ~30% of the apply thread and ~700 B of RAM per
/// partition. The queue, tenant and group scopes still count them.
///
/// A behaviour change of the replicated state (fewer rows), so every node of a
/// cluster must run the same build; rows an older build wrote stay until their
/// partition is deleted, and are never read.
fn partition_scoped(c: Counter) -> bool {
    matches!(c, Counter::RetainedBytes | Counter::DlqCount)
}

/// Record one logged command's outcome under its request id, and its expiry
/// index row (D6, §5.4), through `w` (the write handle, or a shard writer).
///
/// Byte-for-byte what `put_request_outcome(id, now_us, &outcome.encode())`
/// wrote — a test holds the two equal — built in one reused buffer: that path
/// copied the encoded outcome into a row struct, encoded the struct into a
/// second buffer and allocated both keys, per command (B08).
fn record_outcome<W: Writes + ?Sized>(
    w: &mut W,
    row: &mut Vec<u8>,
    id: &RequestId,
    now_us: i64,
    outcome: &Outcome,
) -> Result<()> {
    let body = outcome.encode();
    request_row_into(row, now_us, &body);
    // `keys::request_ids(id)` is the id's own sixteen bytes.
    w.put_raw(Keyspace::RequestIds, id, row)?;
    w.put_raw(
        Keyspace::RequestExpiry,
        &request_expiry_key(now_us, id),
        rows::UNIT,
    )?;
    Ok(())
}

/// A `request_ids` row into `out`: exactly
/// `rows::request_id_encode(&RequestIdRow { now_us, outcome })` — the row
/// version, the stamp, the outcome as a length-prefixed blob — without the
/// struct's copy of the outcome or a second buffer (B08; a test holds the two
/// equal).
fn request_row_into(out: &mut Vec<u8>, now_us: i64, outcome: &[u8]) {
    out.clear();
    out.reserve(1 + 8 + 4 + outcome.len());
    out.push(rows::ROW_V1);
    out.extend_from_slice(&now_us.to_le_bytes());
    out.extend_from_slice(&(outcome.len() as u32).to_le_bytes());
    out.extend_from_slice(outcome);
}

/// `keys::request_expiry(now_us, id)` on the stack: the stamp big-endian with
/// its sign bit flipped (so earlier sorts first), then the id.
fn request_expiry_key(now_us: i64, id: &RequestId) -> [u8; 24] {
    let mut k = [0u8; 24];
    k[..8].copy_from_slice(&((now_us as u64) ^ (1u64 << 63)).to_be_bytes());
    k[8..].copy_from_slice(id);
    k
}

fn counter_queue_prefix(tenant: &str, queue: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(tenant.len() + queue.len() + 5);
    k.push(keys::CounterScope::Queue as u8);
    keys::push_name(&mut k, tenant);
    keys::push_name(&mut k, queue);
    k
}

/// Every counter of ONE consumer group.
fn counter_one_group_prefix(tenant: &str, queue: &str, group: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(tenant.len() + queue.len() + group.len() + 7);
    k.push(keys::CounterScope::Group as u8);
    keys::push_name(&mut k, tenant);
    keys::push_name(&mut k, queue);
    keys::push_name(&mut k, group);
    k
}

/// Every counter of every consumer group of one queue.
fn counter_group_prefix(tenant: &str, queue: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(tenant.len() + queue.len() + 5);
    k.push(keys::CounterScope::Group as u8);
    keys::push_name(&mut k, tenant);
    keys::push_name(&mut k, queue);
    k
}

// ---------------------------------------------------------------------------
// Digest (tests, and the shape §12.9 will use)
// ---------------------------------------------------------------------------

/// A digest of the REPLICATED state, keyspace by keyspace, in key order.
///
/// This is the I2 instrument: two nodes that applied the same entries hold the
/// same bytes under the same keys, whatever their hash seeds, their file
/// boundaries or their durable points were. It is deliberately NOT the digest
/// chain of §12.9 (WP-4.7 owns that, and it is incremental); it is a full
/// scan, for tests and for a divergence report.
///
/// Node-local keyspaces are excluded by [`crate::rsm::store::Scope`], and so
/// is `meta.durable_index`: how far THIS node has flushed is not replicated
/// state, and two correct nodes differ on it constantly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateDigest {
    pub whole: u128,
    /// `(keyspace name, digest, rows)`, in [`Keyspace::ALL`] order.
    pub per_keyspace: Vec<(&'static str, u128, u64)>,
}

impl StateDigest {
    /// The first keyspace that differs, for a test's failure message.
    pub fn first_difference(&self, other: &StateDigest) -> Option<&'static str> {
        for ((n, a, _), (_, b, _)) in self.per_keyspace.iter().zip(other.per_keyspace.iter()) {
            if a != b {
                return Some(n);
            }
        }
        None
    }
}

/// Hash every replicated keyspace in key order.
pub fn state_digest<R: Reads + ?Sized>(reads: &R) -> std::result::Result<StateDigest, StoreError> {
    digest_of(reads, crate::rsm::store::Scope::Replicated)
}

/// The same, over the NODE-LOCAL keyspaces: `seg_loc`, `files`,
/// `partition_files` (§6.2).
///
/// Never comparable BETWEEN nodes — that is what node-local means (D8, I7) —
/// and deliberately not part of [`state_digest`]. Comparable on ONE node
/// across a restart, which is the only instrument that can say whether a
/// repaired node put its files, their recorded lengths and their liveness back
/// exactly where an uninterrupted run had them. Without it a crash test
/// compares the replicated rows and calls the file table proven.
pub fn local_digest<R: Reads + ?Sized>(reads: &R) -> std::result::Result<StateDigest, StoreError> {
    digest_of(reads, crate::rsm::store::Scope::NodeLocal)
}

fn digest_of<R: Reads + ?Sized>(
    reads: &R,
    scope: crate::rsm::store::Scope,
) -> std::result::Result<StateDigest, StoreError> {
    use xxhash_rust::xxh3::Xxh3;
    let mut whole = Xxh3::new();
    let mut per_keyspace = Vec::new();
    for ks in Keyspace::ALL {
        if ks.scope() != scope {
            continue;
        }
        let mut h = Xxh3::new();
        let mut rows = 0u64;
        reads.scan_raw(ks, &[], &[], usize::MAX, &mut |k, v| {
            // `durable_index` and the qlog's `qlog_durable_index` are this
            // node's platter, not the cluster's state.
            if ks == Keyspace::Meta
                && (k == meta::DURABLE_INDEX
                    || k == meta::QLOG_DURABLE_INDEX
                    || k.starts_with(meta::QLOG_TAIL_PREFIX)
                    || k.starts_with(APPLY_SKIPPED_PREFIX))
            {
                return true;
            }
            h.update(&(k.len() as u64).to_le_bytes());
            h.update(k);
            h.update(&(v.len() as u64).to_le_bytes());
            h.update(v);
            rows += 1;
            true
        })?;
        let d = h.digest128();
        whole.update(ks.name().as_bytes());
        whole.update(&d.to_le_bytes());
        per_keyspace.push((ks.name(), d, rows));
    }
    Ok(StateDigest {
        whole: whole.digest128(),
        per_keyspace,
    })
}

#[cfg(test)]
mod counter_flush_tests {
    use super::*;
    use crate::rsm::store::{HeedStore, StoreOpts};

    /// The parallel flush's filter: shard `s` of `n` writes the partition
    /// keys of ITS pids and nothing else, so two shards never flush one key
    /// at once; everything else stays pending for the write handle's flush.
    #[test]
    fn a_shard_flushes_only_its_own_pids_partition_keys() {
        let dir =
            std::env::temp_dir().join(format!("queen-rsm-apply-flush-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = HeedStore::open(
            &dir,
            &StoreOpts {
                map_bytes: Some(64 << 20),
                ..Default::default()
            },
        )
        .expect("store");
        {
            let mut w = store.write().expect("write");
            let mut c = CounterCache::new(true);
            for pid in 1..=8u64 {
                c.add(&mut w, &keys::counter_partition(pid, Counter::DlqCount), 1)
                    .expect("add");
            }
            c.stamp(&mut w, &keys::counter_partition(5, Counter::LastPushUs), 77)
                .expect("stamp");
            c.add(&mut w, &keys::counter_queue("t", "q", Counter::Pushed), 5)
                .expect("add");
            c.flush_partition_scope(&mut w, 1, 4).expect("flush");
            for pid in 1..=8u64 {
                let got = w
                    .counter_at(&keys::counter_partition(pid, Counter::DlqCount))
                    .expect("read");
                assert_eq!(got, i64::from(pid % 4 == 1), "pid {pid}");
            }
            assert_eq!(
                w.counter_at(&keys::counter_partition(5, Counter::LastPushUs))
                    .expect("read"),
                77
            );
            assert_eq!(
                w.counter_at(&keys::counter_queue("t", "q", Counter::Pushed))
                    .expect("read"),
                0,
                "a shared key waits for the write handle"
            );
            // 8 + 1 + 1 keys, 3 of them shard 1's.
            assert_eq!(c.len(), 7);
            c.flush(&mut w).expect("flush");
            assert_eq!(c.len(), 0);
            for pid in 1..=8u64 {
                assert_eq!(
                    w.counter_at(&keys::counter_partition(pid, Counter::DlqCount))
                        .expect("read"),
                    1
                );
            }
            assert_eq!(
                w.counter_at(&keys::counter_queue("t", "q", Counter::Pushed))
                    .expect("read"),
                5
            );
        }
        store.close();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
