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
//!   Cargo.toml allows the lint for the rest of the package (the postgres
//!   class reads clocks and the environment 196 times) and this file,
//!   `rsm/state/` and `rsm/store/` re-`deny` it. The two `#[allow]`s below are
//!   the whole of the exception and each says why at the allow.
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
//! keyspaces of §6.1's message path; `kv`, `timers`, `streams`, `traces`,
//! `flags`, `quotas` and `eph_config` do not exist yet, so their effect kinds
//! answer [`ApplyError::Unsupported`] — a typed, fatal refusal that stops this
//! node, never a skip (I16). Phase 2 adds the keyspaces and the arms together.

// I2, enforced rather than reviewed: `clippy.toml` lists the clock,
// environment and randomness calls this file may not make, `[lints.clippy]` in
// Cargo.toml switches the lint off for the rest of the package (the postgres
// class, and every integration test), and this is where it is switched back
// on.
#![deny(clippy::disallowed_methods)]

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::rsm::dedup;
use crate::rsm::effect::{Assigns, CodecError, Effect, GarbageScope, Kind, Pid};
use crate::rsm::entry::{CommandRecord, Entry, RequestId};
use crate::rsm::segments::{self, FileState, Position, Release, SegError, Segments};
use crate::rsm::state::Derived;
use crate::rsm::store::keys::{self, Counter};
use crate::rsm::store::rows::{
    self, DlqRow, FileRow, GarbageRow, GroupRow, PartitionRow, SegLocRow,
};
use crate::rsm::store::{
    meta, Keyspace, Reads, Store, StoreError, TypedReads, TypedWrites, Writes,
};

// ---------------------------------------------------------------------------
// Constants that are part of the replicated behaviour
// ---------------------------------------------------------------------------

/// How many recorded outcomes one `RequestIdsExpire` effect retires.
///
/// A CONSTANT, never a knob (I2): it decides what the `request_ids` keyspace
/// holds after the effect, so two nodes with different values would diverge.
/// Raising it is a behaviour change and belongs with a kind version bump.
pub const REQUEST_EXPIRE_LIMIT: usize = 4096;

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
    /// long-polls on this node wake (§9.5). `group` is `None` when the wake is
    /// for every group of the queue.
    fn wake(&self, tenant: &str, queue: &str, group: Option<&str>);

    /// A durable point covered `index` (§11.4 step 3). It bounds recovery
    /// replay and lets `LocalReplicator` truncate its log behind it. Never
    /// called for a durable point that failed.
    fn durable(&self, index: u64);
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
        let d = ApplyConfig::default();
        ApplyConfig {
            store_commit_ms: num("QUEEN_RAFT_STORE_COMMIT_MS", d.store_commit_ms),
            store_commit_entries: num("QUEEN_RAFT_STORE_COMMIT_ENTRIES", d.store_commit_entries),
            durable_every_ms: num("QUEEN_RAFT_DURABLE_EVERY_MS", d.durable_every_ms),
            durable_every_bytes: num("QUEEN_RAFT_DURABLE_EVERY_BYTES", d.durable_every_bytes),
            gc_per_pass: d.gc_per_pass,
            idle_tick_ms: d.idle_tick_ms,
        }
    }
}

// ---------------------------------------------------------------------------
// What the caller sends and gets back
// ---------------------------------------------------------------------------

/// One committed entry, as the replicator hands it over (§12.1).
#[derive(Clone, Debug)]
pub struct Committed {
    pub index: u64,
    pub term: u64,
    pub entry: Entry,
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
    /// refused, because the SQL's deletes are idempotent too.
    pub missing_rows: u64,
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
    /// Ready rings and live leases rebuilt from `pending` and
    /// `leases_by_worker` (§6.3, O(work outstanding), never O(retained)).
    pub rings: usize,
    pub leases: usize,
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
    derived: Derived,
    cfg: ApplyConfig,
    notify: Arc<dyn Notify>,

    applied_index: u64,
    applied_term: u64,
    durable_index: u64,
    next_pid: u64,
    kv_version_next: u64,
    last_now_us: i64,
    max_created_at_us: i64,

    /// Entries executed since the last store commit (§11.3's second trigger).
    entries_since_commit: u64,
    /// Set when the open transaction holds anything at all.
    dirty: bool,
    /// Why this applier refuses to do anything else (see
    /// [`Applier::apply`]'s failure contract).
    poisoned: Option<String>,

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

    stats: ApplyStats,
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
                recorded,
            ))
        })?;

        let (segments, seg_recovery) = Segments::open(seg_root, seg_opts, &recorded)?;
        let derived = store.read(|r| Derived::rebuild(r, last_now_us))?;
        let rings = derived.rings_len();
        let leases = derived.lease_count();
        let writes = store.write()?;

        let applier = Applier {
            store,
            writes,
            segments,
            derived,
            cfg,
            notify,
            applied_index,
            applied_term,
            durable_index,
            next_pid,
            kv_version_next,
            last_now_us,
            max_created_at_us,
            entries_since_commit: 0,
            dirty: false,
            poisoned: None,
            recorded_seals: BTreeSet::new(),
            gc_staged: BTreeSet::new(),
            gc_deferred: BTreeSet::new(),
            stats: ApplyStats::default(),
        };
        let rec = Recovered {
            applied_index,
            applied_term,
            durable_index,
            replay_after: durable_index,
            segments: seg_recovery,
            rings,
            leases,
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
            rings,
            leases,
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
        self.stats
    }

    pub fn derived(&self) -> &Derived {
        &self.derived
    }

    /// The read side of the segment files, for the blocking pool (§9.4). A
    /// reader is cloneable and holds no write state.
    pub fn reader(&self) -> segments::Reader {
        self.segments.reader()
    }

    /// The segment writer, for a caller that owns a step apply does not: a
    /// snapshot build sealing every bucket (§11.6), the crash matrix.
    pub fn segments_mut(&mut self) -> &mut Segments {
        &mut self.segments
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
        if let Some(done) = self.gates(c)? {
            return Ok(done);
        }
        match self.execute(c) {
            Ok(a) => Ok(a),
            Err(e) => Err(self.poison(e)),
        }
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
        let mut wakes: Vec<(String, String, Option<String>)> = Vec::new();
        let mut pids_assigned = 0u64;
        let mut kv_versions_assigned = 0u64;
        let max_created_before = self.max_created_at_us;
        for (ord, e) in c.entry.effects.iter().enumerate() {
            match e.assigns() {
                Assigns::Pid(_) => pids_assigned += 1,
                Assigns::KvVersion(_) => kv_versions_assigned += 1,
                Assigns::Nothing => {}
            }
            self.effect(c.index, ord as u32, c.entry.now_us, e, &mut wakes)?;
        }
        self.stats.effects += c.entry.effects.len() as u64;

        // §5.4: every LOGGED command's outcome is recorded, so a retry of a
        // request id inside the window is answered from state and plans
        // nothing (I6). A command that planned no effects was never logged.
        for cmd in &c.entry.commands {
            let bytes = cmd.outcome.encode();
            self.writes
                .put_request_outcome(&cmd.request_id, c.entry.now_us, &bytes)?;
            self.stats.outcomes_recorded += 1;
        }

        // meta, in the same transaction as everything above (I11).
        self.applied_index = c.index;
        self.applied_term = c.term;
        self.last_now_us = c.entry.now_us;
        self.next_pid += pids_assigned;
        self.kv_version_next += kv_versions_assigned;
        self.writes.set_applied(c.index, c.term)?;
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
        self.dirty = true;
        self.entries_since_commit += 1;
        self.stats.entries += 1;

        // I4: the leader answers from here, and not one step earlier. The
        // bytes of a pop's payload are already in this node's files; a reader
        // that needs the ROWS waits for the next store commit (≤ 4 ms, §11.3).
        self.notify.applied(c.index, c.term, &c.entry.commands);
        // One wake per (tenant, queue, group) the entry touched, in key order:
        // a batch of forty pushes to one queue is one wake per group, not
        // forty, and the order does not depend on where in the entry they sat.
        wakes.sort_unstable();
        wakes.dedup();
        for (t, q, g) in wakes {
            self.notify.wake(&t, &q, g.as_deref());
            self.stats.wakes += 1;
        }
        Ok(Applied::Executed {
            effects: c.entry.effects.len(),
        })
    }

    // -- one effect --------------------------------------------------------

    fn effect(
        &mut self,
        index: u64,
        ord: u32,
        now_us: i64,
        e: &Effect,
        wakes: &mut Vec<(String, String, Option<String>)>,
    ) -> Result<()> {
        match e {
            Effect::Noop => Ok(()),

            Effect::QueueUpsert { tenant, queue, cfg } => {
                self.writes.put_queue(tenant, queue, cfg)?;
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
                let (reg_index, reg_effect) = match self.writes.group(tenant, queue, group)? {
                    Some(old) => (old.reg_index, old.reg_effect),
                    None => (index, ord),
                };
                let row = GroupRow {
                    meta: meta.clone(),
                    reg_index,
                    reg_effect,
                };
                self.writes.put_group(tenant, queue, group, &row)?;
                self.derived.ensure_ring(tenant, queue, group);
                Ok(())
            }
            Effect::GroupDelete {
                tenant,
                queue,
                group,
            } => {
                self.writes.del_group(tenant, queue, group)?;
                let prefix = keys::pending_prefix(tenant, queue, group);
                self.sweep(Keyspace::Pending, &prefix)?;
                // The group's COUNTERS go with its name (§6.4): they are the
                // lag inputs of a group that no longer exists, and the name is
                // reusable at once (§5.2) — a group recreated under it would
                // otherwise inherit a dead group's pending, completed,
                // consumed and failed, and report lag from its history.
                let prefix = counter_one_group_prefix(tenant, queue, group);
                self.sweep(Keyspace::Counters, &prefix)?;
                self.derived.drop_ring(tenant, queue, group);
                Ok(())
            }

            Effect::PartitionCreate {
                pid,
                uuid,
                tenant,
                queue,
                partition,
                created_at_us,
            } => {
                if self.writes.partition(*pid)?.is_some() {
                    return Err(ApplyError::Inconsistent {
                        what: "PartitionCreate",
                        detail: format!("pid {pid} already exists"),
                    });
                }
                let row = PartitionRow::new(*uuid, tenant, queue, partition, *created_at_us);
                self.writes.create_partition(*pid, &row)?;
                Ok(())
            }
            Effect::PartitionDelete { pid } => self.partition_delete(*pid),

            Effect::Append {
                pid,
                bucket,
                base_offset,
                count,
                created_at_us,
                hashes,
                blob,
            } => self.append(
                *pid,
                *bucket,
                *base_offset,
                *count,
                *created_at_us,
                hashes,
                blob,
                now_us,
                wakes,
            ),

            Effect::CursorSet { pid, group, row } => {
                self.cursor_set(*pid, group, row, now_us, wakes)
            }
            Effect::CursorDelete { pid, group } => self.cursor_delete(*pid, group),

            Effect::DlqInsert {
                dlq_id,
                tenant,
                queue,
                pid,
                group,
                offset,
                message_id,
                txn,
                payload,
                error,
                retry_count,
                failed_at_us,
            } => {
                let row = DlqRow {
                    pid: *pid,
                    group: group.clone(),
                    offset: *offset,
                    message_id: *message_id,
                    txn: txn.clone(),
                    payload: payload.clone(),
                    error: error.clone(),
                    retry_count: *retry_count,
                    failed_at_us: *failed_at_us,
                };
                // The row is keyed by id and the second index by position, so
                // an id written twice would leave the FIRST position pointing
                // at a row that now names another one. Reusing an id is a
                // planner bug; leaving a dangling index behind would be this
                // node's.
                if let Some(old) = self.writes.dlq(tenant, queue, dlq_id)? {
                    self.writes.del_dlq(tenant, queue, dlq_id, &old)?;
                    self.settle_dlq_count(old.pid, tenant, queue, 1)?;
                }
                self.writes.put_dlq(tenant, queue, dlq_id, &row)?;
                self.bump(*pid, tenant, queue, None, Counter::DlqCount, 1)?;
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
                self.settle_dlq_count(pid, tenant, queue, 1)?;
                Ok(())
            }

            Effect::Watermark {
                pid,
                log_start,
                txns_start,
            } => self.watermark(*pid, *log_start, *txns_start),

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
                        // point (§5.2 rules), so it leaves the rings now and
                        // not when the last chunk lands.
                        self.derived.forget_partition(*pid);
                        // And its share of every surviving group's `pending`
                        // gauge goes now, while the cursors are still here to
                        // say what that share is: the chunks that follow
                        // delete them one stage at a time, and by the time the
                        // partition row goes there is nothing left to read.
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

            // Phase 2's keyspaces do not exist in this build. I16: a kind this
            // node cannot apply stops it; it is never stepped over.
            other => Err(ApplyError::Unsupported { kind: other.kind() }),
        }
    }

    // -- append ------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        pid: Pid,
        bucket: u16,
        base_offset: u64,
        count: u32,
        created_at_us: i64,
        hashes: &[u8],
        blob: &[u8],
        now_us: i64,
        wakes: &mut Vec<(String, String, Option<String>)>,
    ) -> Result<()> {
        let Some(mut p) = self.writes.partition(pid)? else {
            return Err(ApplyError::Inconsistent {
                what: "Append",
                detail: format!("no partition row for pid {pid}"),
            });
        };
        // Offsets are gapless and the planner allocates them from the row this
        // node holds. A mismatch means the planner planned against different
        // state, which is a divergence and not something to paper over.
        let want = (p.last_offset + 1) as u64;
        if base_offset != want {
            return Err(ApplyError::Inconsistent {
                what: "Append",
                detail: format!("pid {pid}: base_offset {base_offset}, partition wants {want}"),
            });
        }
        if count == 0 {
            return Err(ApplyError::Inconsistent {
                what: "Append",
                detail: format!("pid {pid}: an append of no messages"),
            });
        }

        let pos =
            self.segments
                .append(bucket, pid, base_offset, count, created_at_us, hashes, blob)?;
        self.writes.put_seg_loc(
            pid,
            base_offset,
            &SegLocRow {
                bucket: pos.bucket,
                file_id: pos.file_id,
                offset: pos.offset,
                len: pos.len,
            },
        )?;
        // The dedup index and the txns row (D10 option (a), lean). Apply
        // re-reads the row it extends rather than carrying the planner's view
        // forward: the probe ran in another transaction, on another node.
        let end = base_offset + count as u64 - 1;
        let accepted: Vec<([u8; 16], u64)> = hashes
            .chunks_exact(16)
            .enumerate()
            .map(|(i, h)| {
                let mut a = [0u8; 16];
                a.copy_from_slice(h);
                (a, base_offset + i as u64)
            })
            .collect();
        dedup::record(
            &mut self.writes,
            pid,
            base_offset,
            end,
            &accepted,
            created_at_us,
        )?;

        p.last_offset = end as i64;
        p.last_write_at_us = created_at_us;
        p.last_created_at_us = created_at_us;
        if p.oldest_live_at_us.is_none() {
            p.oldest_live_at_us = Some(created_at_us);
        }
        let tenant = p.tenant.clone();
        let queue = p.queue.clone();
        self.writes.put_partition(pid, &p)?;

        // §7.4: `meta.max_created_at_us` is the floor the next planner stamp
        // has to clear, and segment stamps can run ahead of `now` by a
        // microsecond per segment in a cycle.
        if created_at_us > self.max_created_at_us {
            self.max_created_at_us = created_at_us;
        }

        // Counters (§6.4, D16): O(1) per effect at partition, queue and tenant
        // scope, plus O(subscribed groups) below.
        let n = count as i64;
        self.bump(pid, &tenant, &queue, None, Counter::Pushed, n)?;
        self.bump(
            pid,
            &tenant,
            &queue,
            None,
            Counter::RetainedBytes,
            pos.len as i64,
        )?;
        self.stamp(pid, &tenant, &queue, Counter::LastPushUs, created_at_us)?;

        // `pending`, one row per subscribed group (§6.1): the ready rings
        // rebuild from it in O(pending), so nothing here walks partitions.
        let ready_at = self.ready_at(&tenant, &queue, created_at_us)?;
        let mut groups: Vec<String> = Vec::new();
        self.writes
            .scan_groups(&tenant, &queue, usize::MAX, &mut |g, _row| {
                groups.push(g.to_string());
                true
            })?;
        for g in &groups {
            self.writes.put_pending(&tenant, &queue, g, pid, ready_at)?;
            self.derived
                .set_pending(&tenant, &queue, g, pid, ready_at, now_us);
            self.writes.add_counter(
                &keys::counter_group(&tenant, &queue, g, Counter::Pending),
                n,
            )?;
            wakes.push((tenant.clone(), queue.clone(), Some(g.clone())));
        }

        self.stats.appends += 1;
        self.stats.messages += count as u64;
        self.stats.bytes_appended += pos.len as u64;
        Ok(())
    }

    /// When a partition is next worth looking at for a group after an append.
    ///
    /// COARSE BY CONTRACT, exactly like the ring entry it becomes: the claim
    /// re-verifies everything against the cursor row. `delayed_processing` and
    /// `window_buffer` are the two configured reasons a fresh frame is not
    /// claimable yet (003, 004).
    fn ready_at(&self, tenant: &str, queue: &str, created_at_us: i64) -> Result<i64> {
        let Some(cfg) = self.writes.queue(tenant, queue)? else {
            return Ok(created_at_us);
        };
        let delay = cfg.delayed_processing.max(cfg.window_buffer).max(0) as i64;
        Ok(created_at_us.saturating_add(delay.saturating_mul(1_000_000)))
    }

    // -- cursors -----------------------------------------------------------

    fn cursor_set(
        &mut self,
        pid: Pid,
        group: &str,
        row: &crate::rsm::effect::CursorRow,
        now_us: i64,
        wakes: &mut Vec<(String, String, Option<String>)>,
    ) -> Result<()> {
        let Some(p) = self.writes.partition(pid)? else {
            return Err(ApplyError::Inconsistent {
                what: "CursorSet",
                detail: format!("no partition row for pid {pid}"),
            });
        };
        let tenant = p.tenant.clone();
        let queue = p.queue.clone();
        let old = self.writes.cursor(pid, group)?;

        // `leases_by_worker` is the derived index `log_renew_lease_v1` walks.
        // It mirrors the cursor row exactly: one entry while the row names a
        // worker and an expiry, none otherwise.
        let old_worker = old.as_ref().and_then(|c| c.worker.clone());
        let had_lease = old
            .as_ref()
            .map(|c| c.worker.is_some() && c.lease_expires_at_us.is_some())
            .unwrap_or(false);
        if let Some(w) = &old_worker {
            self.writes.del_lease(w, pid, group)?;
        }
        let has_lease = match (&row.worker, row.lease_expires_at_us) {
            (Some(w), Some(exp)) => {
                self.writes.put_lease(w, pid, group, exp)?;
                self.derived.note_lease(w, pid, group, exp);
                true
            }
            _ => {
                self.derived.clear_lease(pid, group);
                false
            }
        };

        self.writes.put_cursor(pid, group, row)?;

        // Counters (§6.4). `committed` is "last acked offset", so the delta is
        // the frames this write completed; a seek backwards (010) gives a
        // negative delta, which is what the postgres oracle does too.
        let old_committed = old.as_ref().map(|c| c.committed).unwrap_or(-1);
        let delta = row.committed - old_committed;
        if delta != 0 {
            self.bump(pid, &tenant, &queue, Some(group), Counter::Completed, delta)?;
            self.writes.add_counter(
                &keys::counter_group(&tenant, &queue, group, Counter::Pending),
                -delta,
            )?;
        }
        let old_consumed = old.as_ref().map(|c| c.total_consumed).unwrap_or(0);
        if row.total_consumed > old_consumed {
            let d = (row.total_consumed - old_consumed) as i64;
            self.bump(pid, &tenant, &queue, Some(group), Counter::Consumed, d)?;
        }
        let old_retries = old.as_ref().map(|c| c.batch_retry_count).unwrap_or(0);
        if row.batch_retry_count > old_retries {
            self.bump(pid, &tenant, &queue, Some(group), Counter::Failed, 1)?;
        }
        if let Some(at) = row.lease_acquired_at_us {
            self.stamp(pid, &tenant, &queue, Counter::LastPopUs, at)?;
        }

        // `pending` mirrors "this partition has work for this group". A live
        // lease is not work for anyone else until it expires, which is what
        // the ring's deferral is for.
        if row.committed >= p.last_offset {
            self.writes.del_pending(&tenant, &queue, group, pid)?;
            self.derived.clear_pending(&tenant, &queue, group, pid);
        } else {
            let ready_at = if has_lease {
                row.lease_expires_at_us.unwrap_or(now_us)
            } else {
                now_us
            };
            self.writes
                .put_pending(&tenant, &queue, group, pid, ready_at)?;
            self.derived
                .set_pending(&tenant, &queue, group, pid, ready_at, now_us);
        }

        // A released lease is the other half of a wake (§9.5): the partition
        // became claimable for whoever is parked on it.
        if had_lease && !has_lease {
            wakes.push((tenant, queue, Some(group.to_string())));
        }
        Ok(())
    }

    fn cursor_delete(&mut self, pid: Pid, group: &str) -> Result<()> {
        let Some(old) = self.writes.cursor(pid, group)? else {
            self.stats.missing_rows += 1;
            return Ok(());
        };
        if let Some(w) = &old.worker {
            self.writes.del_lease(w, pid, group)?;
        }
        self.derived.clear_lease(pid, group);
        self.writes.del_cursor(pid, group)?;
        if let Some(p) = self.writes.partition(pid)? {
            let (tenant, queue) = (p.tenant.clone(), p.queue.clone());
            self.writes.del_pending(&tenant, &queue, group, pid)?;
            self.derived.clear_pending(&tenant, &queue, group, pid);
        }
        Ok(())
    }

    // -- watermarks (retention, §5.2, §11.7, D10) --------------------------

    /// Move a partition's two watermarks.
    ///
    /// `log_start` is where the payload begins, `txns_start` where the hash
    /// lists begin, and `txns_start ≤ log_start` — the hash lists outlive the
    /// segments retention deletes, because the dedup probe (003) and
    /// ack-by-hash below the cursor (005) still read them inside the txns
    /// window (D10). Each frame is therefore released TWICE, once per
    /// watermark, and only the second release lets its file die (§11.7).
    fn watermark(&mut self, pid: Pid, log_start: u64, txns_start: u64) -> Result<()> {
        let Some(mut p) = self.writes.partition(pid)? else {
            self.stats.missing_rows += 1;
            return Ok(());
        };
        if log_start < p.log_start || txns_start < p.txns_start {
            return Err(ApplyError::Inconsistent {
                what: "Watermark",
                detail: format!(
                    "pid {pid}: watermarks never move back \
                     (log {} → {log_start}, txns {} → {txns_start})",
                    p.log_start, p.txns_start
                ),
            });
        }
        if txns_start > log_start {
            return Err(ApplyError::Inconsistent {
                what: "Watermark",
                detail: format!("pid {pid}: txns_start {txns_start} above log_start {log_start}"),
            });
        }
        let tenant = p.tenant.clone();
        let queue = p.queue.clone();

        // 1. The payload: every frame whose base crossed `log_start` this
        //    time. Its bytes stop being retained; its `seg_loc` row stays,
        //    because the hash list still needs to be findable.
        let mut released: Vec<(u64, SegLocRow)> = Vec::new();
        self.writes
            .scan_seg_loc(pid, p.log_start, usize::MAX, &mut |base, row| {
                if base >= log_start {
                    return false;
                }
                released.push((base, row));
                true
            })?;
        let mut bytes_freed: i64 = 0;
        for (_, row) in &released {
            self.segments.release(
                Position {
                    bucket: row.bucket,
                    file_id: row.file_id,
                    offset: row.offset,
                    len: row.len,
                },
                Release::Retained,
            );
            bytes_freed += row.len as i64;
        }
        if bytes_freed != 0 {
            self.bump(
                pid,
                &tenant,
                &queue,
                None,
                Counter::RetainedBytes,
                -bytes_freed,
            )?;
        }

        // 2. The hash lists: every frame whose base crossed `txns_start`. Now
        //    the frame is gone for good, so its `seg_loc` row goes with it.
        let mut expired: Vec<(u64, SegLocRow)> = Vec::new();
        self.writes
            .scan_seg_loc(pid, p.txns_start, usize::MAX, &mut |base, row| {
                if base >= txns_start {
                    return false;
                }
                expired.push((base, row));
                true
            })?;
        for (base, row) in &expired {
            self.segments.release(
                Position {
                    bucket: row.bucket,
                    file_id: row.file_id,
                    offset: row.offset,
                    len: row.len,
                },
                Release::Window,
            );
            self.writes.del_seg_loc(pid, *base)?;
        }
        self.expire_hashes(pid, p.txns_start, txns_start)?;

        p.log_start = log_start;
        p.txns_start = txns_start;
        self.writes.put_partition(pid, &p)?;
        Ok(())
    }

    /// Drop the dedup occurrences and the `txns` rows below `to`.
    ///
    /// The `txns` row of an append that STRADDLES the new watermark is left
    /// alone: it carries the hash list of offsets above it too, and a row is
    /// the unit D10 stores. Keeping a few hashes longer than asked is exact in
    /// the direction that matters — a duplicate is still found — while
    /// deleting them early would answer "new" for a transaction id the
    /// postgres oracle still calls a duplicate.
    fn expire_hashes(&mut self, pid: Pid, from: u64, to: u64) -> Result<()> {
        if to <= from {
            return Ok(());
        }
        let mut victims: Vec<(u64, dedup::TxnsRow)> = Vec::new();
        let prefix = keys::txns_prefix(pid);
        let start = keys::txns(pid, from);
        let mut bad: Option<&'static str> = None;
        self.writes
            .scan_raw(Keyspace::Txns, &start, &prefix, usize::MAX, &mut |k, v| {
                let Some(base) = keys::txns_base_of(k) else {
                    bad = Some("txns key");
                    return false;
                };
                match dedup::TxnsRow::decode(v) {
                    Ok(row) => {
                        if row.end >= to {
                            return false;
                        }
                        victims.push((base, row));
                        true
                    }
                    Err(e) => {
                        bad = Some(e);
                        false
                    }
                }
            })?;
        if let Some(e) = bad {
            return Err(StoreError::corrupt(Keyspace::Txns, e).into());
        }

        for (base, row) in &victims {
            for h in row.iter_hashes() {
                let k = keys::dedup(pid, &h);
                let Some(cur) = self.writes.get_raw(Keyspace::Dedup, &k)? else {
                    continue;
                };
                dedup::check_occurrences(cur)?;
                let mut keep: Vec<u8> = Vec::with_capacity(cur.len());
                for (off, created) in dedup::occurrences(cur) {
                    if off >= to {
                        dedup::push_occurrence(&mut keep, off, created);
                    }
                }
                if keep.is_empty() {
                    self.writes.del_raw(Keyspace::Dedup, &k)?;
                } else if keep.len() != cur.len() {
                    self.writes.put_raw(Keyspace::Dedup, &k, &keep)?;
                }
            }
            self.writes
                .del_raw(Keyspace::Txns, &keys::txns(pid, *base))?;
            self.stats.rows_swept += 1;
        }
        Ok(())
    }

    // -- deletes -----------------------------------------------------------

    /// A queue delete (013): the NAME-keyed rows go at once, as one
    /// transaction, exactly as the SQL does — so the name is reusable
    /// immediately and a push right after the delete recreates it. The
    /// pid-keyed data goes in `DeleteChunk`s behind a `GarbageAdd` (§5.2).
    /// PARITY NOTE for the read paths (WP-2.x): postgres's
    /// `queen.delete_queue_v1` removes the queue's `log_dlq` rows in the same
    /// transaction, and §5.2 puts DLQ in the CHUNKED half here, so between this
    /// call and the last `DeleteChunk` the dead queue's dead letters are still
    /// in the `dlq` keyspace under `(tenant, queue, id)` — a name a push may
    /// already have recreated. Their gauges are settled and cannot move again
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
            let held = self.writes.queue_counter(tenant, queue, c)?;
            if held != 0 {
                self.writes
                    .add_counter(&keys::counter_tenant(tenant, c), -held)?;
            }
        }
        let prefix = counter_queue_prefix(tenant, queue);
        self.sweep(Keyspace::Counters, &prefix)?;
        let prefix = counter_group_prefix(tenant, queue);
        self.sweep(Keyspace::Counters, &prefix)?;
        let mut groups: Vec<String> = Vec::new();
        self.writes
            .scan_groups(tenant, queue, usize::MAX, &mut |g, _row| {
                groups.push(g.to_string());
                true
            })?;
        for g in &groups {
            self.writes.del_group(tenant, queue, g)?;
            let prefix = keys::pending_prefix(tenant, queue, g);
            self.sweep(Keyspace::Pending, &prefix)?;
            self.derived.drop_ring(tenant, queue, g);
        }
        let prefix = keys::partitions_by_key_prefix(tenant, queue);
        self.sweep(Keyspace::PartitionsByKey, &prefix)?;
        let prefix = keys::queue_partitions_prefix(tenant, queue);
        self.sweep(Keyspace::QueuePartitions, &prefix)?;
        Ok(())
    }

    /// Drop a partition and everything keyed by it (006 cleanup, and the tail
    /// of a delete whose chunks have finished).
    fn partition_delete(&mut self, pid: Pid) -> Result<()> {
        let Some(p) = self.writes.partition(pid)? else {
            self.stats.missing_rows += 1;
            return Ok(());
        };
        let (tenant, queue) = (p.tenant.clone(), p.queue.clone());
        let retained = self.writes.partition_counter(pid, Counter::RetainedBytes)?;
        // The chunked path settled this partition's share of every group's
        // `pending` gauge at its `GarbageAdd`, while the cursors were still
        // there to say what it was; a direct `PartitionDelete` (006 cleanup)
        // settles it here, with everything still in place.
        if self.writes.garbage(pid)?.is_none() {
            self.settle_group_pending(pid)?;
        }

        // cursors, their leases and their pending rows
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
            self.writes.del_pending(&tenant, &queue, g, pid)?;
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
        self.settle_dlq_count(pid, &tenant, &queue, gone)?;
        self.sweep(Keyspace::DlqByPos, &prefix)?;

        // payload positions: the files lose whatever claims these frames still
        // hold (§11.7)
        self.release_all_segments(pid, p.log_start)?;

        self.sweep(Keyspace::Dedup, &keys::dedup_prefix(pid))?;
        self.sweep(Keyspace::Txns, &keys::txns_prefix(pid))?;
        self.sweep(Keyspace::PartitionFiles, &keys::partition_files_prefix(pid))?;
        self.sweep(Keyspace::Counters, &keys::counter_partition_prefix(pid))?;

        // While the QUEUE still exists, its retained bytes and the tenant's
        // have to lose this partition's share: it is the number the proxy's
        // storage quota reads. The cumulative event counters (pushed,
        // completed) are left alone — they count what happened, not what is
        // stored. A queue that has already been deleted settled both in
        // [`Applier::queue_delete`], and adding here would resurrect a swept
        // row holding a negative number.
        if retained != 0 && self.queue_gauges_live(pid, &tenant, &queue)? {
            self.writes.add_counter(
                &keys::counter_queue(&tenant, &queue, Counter::RetainedBytes),
                -retained,
            )?;
            self.writes.add_counter(
                &keys::counter_tenant(&tenant, Counter::RetainedBytes),
                -retained,
            )?;
        }

        self.writes.del_partition(pid, &p)?;
        self.writes.del_garbage(pid)?;
        self.derived.forget_partition(pid);
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
            self.segments.release(
                Position {
                    bucket: row.bucket,
                    file_id: row.file_id,
                    offset: row.offset,
                    len: row.len,
                },
                release_for(*base, log_start),
            );
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
                // A group delete (014) touches only the `(pid, group)` rows,
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
        self.cursor_delete(pid, group)?;
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
            // A group delete takes the group's dead letters with it (014), and
            // the count they were carrying goes too: the queue survives the
            // group, and its `dlq_count` is a gauge of the rows that exist.
            self.settle_dlq_count(pid, &tenant, &queue, gone)?;
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

    /// Cursors, with their `leases_by_worker` and `pending` twins.
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
        let names = self
            .writes
            .partition(pid)?
            .map(|p| (p.tenant.clone(), p.queue.clone()));
        for g in &victims {
            self.cursor_delete(pid, g)?;
            if let Some((tenant, queue)) = &names {
                self.writes.del_pending(tenant, queue, g, pid)?;
            }
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
            self.settle_dlq_count(pid, &tenant, &queue, gone)?;
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
            self.segments.release(
                Position {
                    bucket: row.bucket,
                    file_id: row.file_id,
                    offset: row.offset,
                    len: row.len,
                },
                release_for(*base, log_start),
            );
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

    /// One counter at partition, queue and tenant scope, plus the group scope
    /// when the effect names a group (§6.4, D16).
    fn bump(
        &mut self,
        pid: Pid,
        tenant: &str,
        queue: &str,
        group: Option<&str>,
        c: Counter,
        delta: i64,
    ) -> Result<()> {
        if delta == 0 {
            return Ok(());
        }
        self.writes
            .add_counter(&keys::counter_partition(pid, c), delta)?;
        self.writes
            .add_counter(&keys::counter_queue(tenant, queue, c), delta)?;
        self.writes
            .add_counter(&keys::counter_tenant(tenant, c), delta)?;
        if let Some(g) = group {
            self.writes
                .add_counter(&keys::counter_group(tenant, queue, g, c), delta)?;
        }
        Ok(())
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

    /// May rows removed under `pid` still settle the QUEUE's and the TENANT's
    /// gauges?
    ///
    /// Not a test on the name. §5.2 ratifies that the name is reusable the
    /// instant a delete lands, while the pid-keyed rows of the queue that had
    /// it are deleted in chunks for as long as that takes; a name test
    /// therefore subtracts a dead queue's dead letters and retained bytes from
    /// whatever queue holds the name when the chunk runs. D16 says the counters
    /// ARE the answer — there is no aggregation to correct them — so the
    /// recreated queue reports a negative count, for ever.
    ///
    /// A pid in the `garbage` set answers from the id its `GarbageAdd`
    /// recorded: the gauges are still its own only while the live row carries
    /// that same id, and a queue or tenant delete recorded `None`, having
    /// settled and swept them itself. A pid that is not garbage is an ordinary
    /// partition of a live queue.
    fn queue_gauges_live(&self, pid: Pid, tenant: &str, queue: &str) -> Result<bool> {
        let live = self.writes.queue(tenant, queue)?;
        match self.writes.garbage(pid)? {
            Some(g) => Ok(match (g.queue_id, live) {
                (Some(id), Some(cfg)) => cfg.id == id,
                _ => false,
            }),
            None => Ok(live.is_some()),
        }
    }

    /// Retire dead letters from the `dlq_count` GAUGE at every scope that
    /// still has one (§6.4).
    ///
    /// Every path that removes a dead letter passes through here, not only
    /// `DlqDelete`: a consumer-group delete and a partition delete remove the
    /// rows too, and a gauge that counts only some of the removals drifts up
    /// for ever. The queue and tenant rows are touched only while the queue
    /// those rows belong to is still there ([`Applier::queue_gauges_live`]):
    /// [`Applier::queue_delete`] settles the tenant from the queue and sweeps
    /// both, and `add_counter` on a swept row would recreate it holding a
    /// negative number — or take it out of the queue that has since been
    /// created under the same name.
    fn settle_dlq_count(&mut self, pid: Pid, tenant: &str, queue: &str, gone: i64) -> Result<()> {
        if gone == 0 {
            return Ok(());
        }
        let queue_alive = self.queue_gauges_live(pid, tenant, queue)?;
        self.writes
            .add_counter(&keys::counter_partition(pid, Counter::DlqCount), -gone)?;
        if queue_alive {
            self.writes.add_counter(
                &keys::counter_queue(tenant, queue, Counter::DlqCount),
                -gone,
            )?;
            self.writes
                .add_counter(&keys::counter_tenant(tenant, Counter::DlqCount), -gone)?;
        }
        Ok(())
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
                self.writes.add_counter(
                    &keys::counter_group(&tenant, &queue, g, Counter::Pending),
                    -share,
                )?;
            }
        }
        Ok(())
    }

    /// A "latest time" counter: monotone, so an out-of-order effect cannot
    /// make a queue look idle.
    fn stamp(&mut self, pid: Pid, tenant: &str, queue: &str, c: Counter, at_us: i64) -> Result<()> {
        for key in [
            keys::counter_partition(pid, c),
            keys::counter_queue(tenant, queue, c),
        ] {
            if self.writes.counter_at(&key)? < at_us {
                self.writes.set_counter(&key, at_us)?;
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
        self.writes.commit()?;
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
        let point = self.segments.durable_point()?;
        for f in &point.files {
            self.put_file_state(f)?;
        }
        self.record_files()?;
        let seals = self.record_seals()?;
        self.writes
            .set_meta_u64(meta::DURABLE_INDEX, self.applied_index)?;
        self.writes
            .set_applied(self.applied_index, self.applied_term)?;
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
        for (b, id) in seals {
            self.segments.forget_sealed(b, id);
        }
        // Phase two of GC: the durable commit above no longer names these
        // files, so their bytes may go (I10).
        self.unlink_staged()?;
        self.notify.durable(self.durable_index);
        Ok(self.durable_index)
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
        self.gc_staged.contains(&(bucket, file_id)) || self.gc_deferred.contains(&(bucket, file_id))
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
            if self.gc_staged.contains(&(b, id)) || self.gc_deferred.contains(&(b, id)) {
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
        for (b, id) in staged {
            match self.segments.unlink(b, id) {
                Ok(true) => {
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
// The thread
// ---------------------------------------------------------------------------

/// Run the apply loop until the channel closes.
///
/// It blocks — on the channel, on the store, on fsync — and that is correct:
/// this is a `std` thread of its own, never a tokio worker (I15). The park is
/// bounded by `idle_tick_ms` so the cadences run on an idle node too.
pub fn run<S: Store>(
    applier: &mut Applier<'_, S>,
    rx: &Receiver<Committed>,
    clock: &dyn Clock,
) -> Result<()> {
    let mut last_commit = clock.now();
    let mut last_durable = last_commit;
    let tick = Duration::from_millis(applier.cfg.idle_tick_ms.max(1));
    loop {
        match rx.recv_timeout(tick) {
            Ok(c) => {
                applier.apply(&c)?;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                applier.flush()?;
                return Ok(());
            }
        }
        let now = clock.now();
        if applier.durable_due(now.duration_since(last_durable)) {
            applier.durable_point()?;
            last_durable = now;
            last_commit = now;
        } else if applier.commit_due(now.duration_since(last_commit)) {
            applier.commit()?;
            last_commit = now;
        }
        applier.gc_pass()?;
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
    std::thread::Builder::new()
        .name("queen-rsm-apply".into())
        .spawn(move || {
            let (mut applier, rec) = Applier::open(&*store, &seg_root, seg_opts, cfg, notify)?;
            tracing::info!(
                target: "rsm",
                replay_after = rec.replay_after,
                applied = rec.applied_index,
                "rsm apply thread started",
            );
            run(&mut applier, &rx, clock.as_ref())?;
            Ok(applier.stats())
        })
        .expect("spawn the apply thread")
}

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
            // `durable_index` is this node's platter, not the cluster's state.
            if ks == Keyspace::Meta && k == meta::DURABLE_INDEX {
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
