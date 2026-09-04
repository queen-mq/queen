//! window — the window engine of plan §4: **pure, no I/O, no wall clock**.
//!
//! One [`Engine`] owns one queue. It consumes events — a discovery page, a fetch
//! answer, a seek result, a driver confirmation — and emits [`Action`]s that
//! somebody else executes. It never opens a socket and it never reads
//! `SystemTime`: every timestamp it reasons about is a [`Micros`] that came from
//! PostgreSQL through the broker (a record's `ts`, discovery's `safeTime`, a
//! partition's `lastWriteAt`). A sink that compared its own clock to a `ts`
//! would be wrong by construction (plan §12), and the only way to be sure of
//! that is for the clock not to be reachable from here.
//!
//! # The one idea
//!
//! Window `k` is **the set of records whose segment `ts` lies in
//! `[T_{k-1}, T_k)`**, and it is closed only at a `T_k` at or below the broker's
//! `safeTime` minus a guard. Below `safeTime` nothing can still become visible
//! (plan §4.1), and within a partition `ts` and offset are co-monotone
//! (003_log_push.sql:219-223), so the window is a *deterministic set*: rebuilding
//! it from scratch yields the same records in the same order, hence the same
//! bytes, hence a retried upload that is byte-identical without naming a single
//! offset (plan §4.2). That is the whole trick, and everything in this file
//! exists to keep it true.
//!
//! # The frontier, which is also the memory bound
//!
//! For each tracked partition the engine keeps a **frontier**: a lower bound on
//! the `ts` of the first record it has not yet buffered.
//!
//! * The partition has unread records and the last one it did read had `ts = X`
//!   ⇒ frontier `X`, because `ts` never goes down within a partition.
//! * The partition is caught up (`next_offset == highWatermark`) ⇒ frontier is
//!   the `safeTime` that was current **when that observation was made** — later
//!   records cannot be older than that. Using a *newer* `safeTime` with an older
//!   observation would be unsound, which is why [`Engine::inflight_safe`] pins
//!   the value that was current when the outstanding calls were issued.
//! * Nothing read yet ⇒ unknown, represented as [`Micros::MIN`], which blocks
//!   the close until the partition is fetched.
//!
//! `T_k <= min frontier` is then the invariant that makes the window complete,
//! and "do not fetch a partition whose frontier is already beyond this window"
//! is what stops a queue of a million partitions from being read into memory to
//! close one window (plan §4.4).
//!
//! A caught-up partition's frontier would go stale the moment discovery stopped
//! reporting it — and incremental discovery deliberately reports only what moved
//! — so the engine also uses the contrapositive: after a **complete discovery
//! pass**, a partition the pass did not return has `lastWriteAt < since`, so it
//! has no unread records at all, and anything it receives afterwards is at or
//! after that pass's `safeTime`. That is [`Engine::min_frontier`]'s
//! `complete_pass_safe` term, and it is what lets the sink close windows at a
//! million partitions without a per-partition poll.
//!
//! # What the driver owes the engine
//!
//! Every emitted action gets exactly one answer: [`Engine::on_discovery`] or
//! [`Engine::on_discovery_failed`], [`Engine::on_fetch`] or
//! [`Engine::on_fetch_failed`], and so on. An action that is executed and never
//! answered leaves the engine waiting for ever — the engine cannot time it out,
//! because it has no clock.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::checkpoint::{positions_from, should_checkpoint};
use crate::types::{
    Align, ChangedEntry, Checkpoint, Committed, Compression, FetchError, FetchRequestEntry,
    FetchedEntry, Format, Intent, Layout, LostRange, Micros, PartitionBounds, Record, Start,
};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// The engine's knobs. Every field maps to a `QUEEN_S3_*` variable of plan §6.2
/// except where noted; `config.rs` builds this, the engine never reads an
/// environment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineConfig {
    /// Close-by-size, in **uncompressed** buffered bytes ([`Record::weight`]).
    /// `QUEEN_S3_TARGET_MB`.
    pub target_bytes: usize,
    /// Close-by-age: once `safeTime - T_{k-1}` reaches this, the window closes
    /// at whatever it can reach, however small. `QUEEN_S3_MAX_WINDOW_MS`.
    pub max_window: Micros,
    /// Windows never straddle an alignment boundary, so `dt=`/`hour=` are exact
    /// and derived from `t_start`. `QUEEN_S3_ALIGN`.
    pub align: Align,
    /// Subtracted from the broker's `safeTime` before it is used as a ceiling —
    /// added to the broker's own guard, never subtracted (plan §6.2).
    /// `QUEEN_S3_SAFE_GUARD_MS`.
    pub safe_guard: Micros,
    /// Where a queue with no committed pointer starts. `QUEEN_S3_START`.
    pub start: Start,
    /// Windows between position checkpoints. `QUEEN_S3_CHECKPOINT_EVERY`.
    pub checkpoint_every: u64,
    /// Entries per `POST /api/v1/fetch`; the broker clamps at 1024 (plan F1) and
    /// so does [`Engine::new`].
    pub fetch_entries_per_call: usize,
    /// In-flight fetch calls for this queue. `QUEEN_S3_FETCH_CONCURRENCY`.
    pub fetch_concurrency: usize,
    /// Per-entry `maxBytes` over compressed segment bytes; the broker clamps at
    /// 8 MiB (plan F1).
    pub max_bytes_per_entry: i64,
    /// How far below `T_{k-1}` incremental discovery asks. Must exceed the
    /// `lastWriteAt` quantization of one second (001_log_schema.sql:39-45) or a
    /// partition written just below the boundary can be missed; plan §4.3 says
    /// 2 s.
    pub discovery_slack: Micros,
    /// Discoveries between full enumerations. The full sweep is the
    /// reconciliation pass: it re-establishes the frontier of every partition
    /// and re-admits any the incremental `since` window cannot see.
    pub full_sweep_every: u32,
    /// Partitions per discovery page; the broker clamps at 1000 (plan §5.1).
    pub discovery_limit: u32,
    /// Windows a partition may go unseen by discovery before its position is
    /// dropped (plan §12: a partition the cleanup deleted). **Must exceed the
    /// number of windows between full sweeps**, or live-but-idle partitions get
    /// dropped and re-read.
    pub prune_after_windows: u64,
    /// What [`Action::Idle`] suggests waiting. `QUEEN_S3_DISCOVERY_INTERVAL_MS`.
    pub discovery_interval_ms: u64,
}

impl Default for EngineConfig {
    /// The defaults of plan §6.2.
    fn default() -> EngineConfig {
        EngineConfig {
            target_bytes: 128 * 1024 * 1024,
            max_window: Micros::from_millis(300_000),
            align: Align::Hour,
            safe_guard: Micros::from_millis(5_000),
            start: Start::Latest,
            checkpoint_every: 20,
            fetch_entries_per_call: 1024,
            fetch_concurrency: 4,
            max_bytes_per_entry: 1024 * 1024,
            discovery_slack: Micros(2 * Micros::SECOND.0),
            full_sweep_every: 30,
            discovery_limit: 1000,
            prune_after_windows: 40,
            discovery_interval_ms: 2_000,
        }
    }
}

/// The fields of the [`Intent`] that describe the writer rather than the window.
/// Fixed for the life of the engine; a change of any of them is a redeploy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntentMeta {
    /// `WriterFactory::describe()` — which code wrote the object.
    pub writer: String,
    pub format: Format,
    pub compression: Compression,
    pub layout: Layout,
}

// ---------------------------------------------------------------------------
// Actions and plans
// ---------------------------------------------------------------------------

/// One thing the driver should do. The engine emits these and consumes their
/// answers; it performs none of them.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// One page of `POST /api/v1/partitions/changed` for this queue.
    /// `since: None` is a full enumeration (the reconciliation sweep). The
    /// engine paginates by re-emitting with `after` set from the previous
    /// page's `next`; the driver supplies the queue name and the page limit.
    Discover {
        since: Option<Micros>,
        after: Option<String>,
    },
    /// One `POST /api/v1/fetch` call: at most `fetch_entries_per_call` entries,
    /// all for this queue, one entry per partition. Answer it with exactly one
    /// [`Engine::on_fetch`] (or [`Engine::on_fetch_failed`]).
    Fetch(Vec<FetchRequestEntry>),
    /// Recover a position: the first offset of `partition` with `ts >= t`. Run
    /// it with [`crate::seek`] and answer with [`Engine::on_seek_result`].
    Seek {
        partition: Arc<str>,
        t: Micros,
        last_offset: i64,
        log_start: i64,
    },
    /// Step 4 of plan §4.3: fix `T_k` durably **before** the upload, so a crash
    /// mid-upload redoes the identical window.
    WriteIntent(Intent),
    /// Step 5: the deterministic record set of window `k`. Sorted, complete,
    /// and carrying everything the manifest needs.
    Upload(WindowPlan),
    /// Step 6: the commit pointer. `committed_at_ms` is left at 0 — the driver
    /// stamps the wall clock, because the engine has none.
    Commit(Committed),
    /// Step 7: the position cache, best-effort, never blocking a commit.
    Checkpoint(Checkpoint),
    /// Nothing to do until the discovery cadence comes round or the broker's
    /// long poll returns.
    Idle { min_wait_ms: u64 },
}

/// The record set of one window, ready to be written and manifested.
///
/// `records` is sorted by `(partition, offset)` and ties are impossible (an
/// offset is unique within a partition), so the ordering is total and the object
/// is a pure function of the set (plan §6.4).
#[derive(Clone, Debug, PartialEq)]
pub struct WindowPlan {
    pub k: u64,
    pub t_start: Micros,
    pub t_end: Micros,
    pub records: Vec<Record>,
    /// Offset ranges retention deleted before the sink read them (plan §4.6).
    /// Never silent: the manifest carries them and the driver counts them.
    pub lost: Vec<LostRange>,
    /// Distinct partitions contributing at least one record.
    pub partitions: u64,
    pub min_ts: Option<Micros>,
    pub max_ts: Option<Micros>,
    /// Uncompressed bytes by [`Record::weight`] — the number the size close rule
    /// was measured against, not the object's size.
    pub bytes_estimate: usize,
}

/// Where the engine is in the commit sequence of plan §4.3. Exactly one window
/// is in flight at a time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EngineState {
    /// Discovering, fetching, buffering — no window closed.
    Filling,
    /// Window `k` is closed and its intent is being written (step 4).
    Intent(u64),
    /// The intent is durable; the object is being uploaded (step 5).
    Upload(u64),
    /// The object is up; the commit pointer is being written (step 6).
    Commit(u64),
    /// Terminal. The only cause is `UNKNOWN_TOPIC_OR_PARTITION`: the queue does
    /// not exist for this tenant, which no amount of retrying will change
    /// (032_log_fetch.sql:50-57).
    Failed(String),
}

/// Counters for `/metrics` and for tests. Cheap; the engine keeps them always.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EngineStats {
    pub windows_closed: u64,
    pub windows_committed: u64,
    /// Records dropped on arrival because `ts < T_{k-1}` (rule 3): the cost of a
    /// stale position, and nothing else.
    pub records_dropped: u64,
    /// Records retention deleted before the sink read them (plan §4.6).
    pub records_lost: u64,
    /// Per-entry fetch errors other than the two structural ones.
    pub fetch_errors: u64,
    /// Aligned intervals that held no record and were stepped over without an
    /// object (see [`Engine::try_close`]).
    pub boundaries_skipped: u64,
    pub seeks_issued: u64,
    pub discoveries: u64,
    pub full_sweeps: u64,
    pub partitions_pruned: u64,
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// A buffered record plus the running weight of this partition's buffer up to
/// and including it, so "how many bytes below `t`" is a binary search rather
/// than a scan (the size close rule runs on every tick).
struct Buffered {
    rec: Record,
    cum: usize,
}

/// Which ordered index a partition currently sits in.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Idx {
    /// Has records the sink has not read. Key: the frontier lower bound.
    Unread(Micros),
    /// Read to the high watermark. Key: the `safeTime` of that observation.
    Caught(Micros),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Outstanding {
    None,
    Fetch,
    Seek,
}

struct PartitionState {
    name: Arc<str>,
    /// The next offset not yet buffered — the position. A cache, never the
    /// commit truth (plan §4.5).
    next_offset: i64,
    /// The allocator's last offset; the high watermark is `last_offset + 1`.
    last_offset: i64,
    log_start: i64,
    last_write_at: Option<Micros>,
    /// `ts` of the record at `next_offset - 1`: the frontier's lower bound while
    /// the partition has unread records. `None` = nothing read here yet.
    last_ts: Option<Micros>,
    /// The `safeTime` in force when this partition was last seen caught up.
    caught_up_at: Option<Micros>,
    /// The discovery pass that first returned this partition — used to decide
    /// whether a completed pass covers it.
    added_gen: u32,
    /// Waiting for a position from a [`Action::Seek`].
    pending_seek: bool,
    outstanding: Outstanding,
    /// The window index at the last discovery sighting, for pruning.
    last_seen_k: u64,
    buffered: Vec<Buffered>,
    idx: Option<Idx>,
}

impl PartitionState {
    fn caught_up(&self) -> bool {
        !self.pending_seek && self.next_offset > self.last_offset
    }
    fn buffered_weight(&self) -> usize {
        self.buffered.last().map(|b| b.cum).unwrap_or(0)
    }
    /// The position a checkpoint must record: where the **next** window starts.
    /// Records already buffered belong to it, so the position is the first
    /// buffered offset when there is one, not `next_offset`.
    fn checkpoint_offset(&self) -> i64 {
        self.buffered
            .first()
            .map(|b| b.rec.offset)
            .unwrap_or(self.next_offset)
    }
}

/// The state of one discovery pass — one `since`, one or more pages.
#[derive(Default)]
struct PassState {
    gen: u32,
    open: bool,
    /// The minimum `safeTime` over the pass's pages: the conservative choice,
    /// and the value the "not returned by a complete pass" argument uses.
    safe: Option<Micros>,
    since: Option<Micros>,
    /// The `after` cursor for the next page; `None` = the pass has not started
    /// or the current page is outstanding.
    cursor: Option<String>,
    full: bool,
}

/// The window the engine has closed and is committing. `k` and `t_start` live
/// in the plan; only `t_end` is needed outside it, for the commit pointer.
struct InFlight {
    t_end: Micros,
    plan: WindowPlan,
}

// ---------------------------------------------------------------------------
// The engine
// ---------------------------------------------------------------------------

/// The window state machine of one queue.
pub struct Engine {
    queue: String,
    cfg: EngineConfig,
    meta: IntentMeta,

    // --- windows
    committed_k: u64,
    committed_t_end: Option<Micros>,
    /// `T_{k-1}`. `None` only for `Start::Latest` before the first `safeTime`.
    t_prev: Option<Micros>,
    /// Records below this are dropped on arrival. Equals `t_prev` while filling
    /// and `T_k` while window `k` is in flight, so that fetching may continue
    /// during a commit without re-buffering what the closed window already took.
    t_filter: Option<Micros>,
    /// A recovered intent whose window must be rebuilt exactly (plan §4.3).
    redo: Option<Intent>,
    state: EngineState,
    in_flight: Option<InFlight>,
    last_checkpoint_k: u64,

    // --- the broker's view
    safe_time: Option<Micros>,
    degraded: bool,
    /// The `safeTime` that was current when the oldest still-outstanding call
    /// was issued. A caught-up observation from that call may be attributed only
    /// to this value, never to a newer one.
    inflight_safe: Option<Micros>,

    // --- discovery
    pass: PassState,
    next_gen: u32,
    complete_pass_gen: u32,
    complete_pass_safe: Option<Micros>,
    discovery_outstanding: bool,
    discovered_this_window: bool,
    /// See [`Engine::schedule_discovery`]: the engine has no clock, so a
    /// cadence discovery costs one [`Action::Idle`] first.
    cadence_armed: bool,

    // --- partitions
    parts: BTreeMap<Arc<str>, PartitionState>,
    unread: BTreeSet<(Micros, Arc<str>)>,
    caught: BTreeSet<(Micros, Arc<str>)>,
    /// Caught-up partitions the last completed pass does not cover.
    uncovered: BTreeSet<(Micros, Arc<str>)>,
    with_buffer: BTreeSet<Arc<str>>,
    need_seek: BTreeSet<Arc<str>>,
    /// Positions recovered from a checkpoint, consumed the first time discovery
    /// names the partition (plan §4.5(1)). Not tracked partitions: a checkpoint
    /// entry alone says nothing about bounds, and a partition with unknown
    /// bounds would block every close.
    restored: BTreeMap<Arc<str>, i64>,

    outstanding_calls: usize,
    outstanding_seeks: usize,
    buffered_weight: usize,

    lost: Vec<LostRange>,
    forced: bool,
    pending: Vec<Action>,
    stats: EngineStats,
}

impl Engine {
    /// A fresh engine for `queue`. Call [`Engine::restore`] before feeding it
    /// anything else.
    pub fn new(queue: String, cfg: EngineConfig, meta: IntentMeta) -> Engine {
        let mut cfg = cfg;
        cfg.fetch_entries_per_call = cfg.fetch_entries_per_call.clamp(1, 1024);
        cfg.fetch_concurrency = cfg.fetch_concurrency.max(1);
        cfg.discovery_limit = cfg.discovery_limit.clamp(1, 1000);
        cfg.full_sweep_every = cfg.full_sweep_every.max(1);
        // A target of zero would make every close land on a boundary with no
        // record below it, which is a window that cannot be emitted.
        cfg.target_bytes = cfg.target_bytes.max(1);
        let t0 = match cfg.start {
            Start::Earliest => Some(Micros::MIN),
            // T_0 is the first safeTime seen, which has not arrived yet.
            Start::Latest => None,
        };
        Engine {
            queue,
            cfg,
            meta,
            committed_k: 0,
            committed_t_end: None,
            t_prev: t0,
            t_filter: t0,
            redo: None,
            state: EngineState::Filling,
            in_flight: None,
            last_checkpoint_k: 0,
            safe_time: None,
            degraded: false,
            inflight_safe: None,
            pass: PassState::default(),
            next_gen: 1,
            complete_pass_gen: 0,
            complete_pass_safe: None,
            discovery_outstanding: false,
            discovered_this_window: false,
            cadence_armed: false,
            parts: BTreeMap::new(),
            unread: BTreeSet::new(),
            caught: BTreeSet::new(),
            uncovered: BTreeSet::new(),
            with_buffer: BTreeSet::new(),
            need_seek: BTreeSet::new(),
            restored: BTreeMap::new(),
            outstanding_calls: 0,
            outstanding_seeks: 0,
            buffered_weight: 0,
            lost: Vec::new(),
            forced: false,
            pending: Vec::new(),
            stats: EngineStats::default(),
        }
    }

    /// Boot from what the KV store and the bucket hold (plan §4.3's crash
    /// matrix). Call once, on a fresh engine, before any event.
    ///
    /// * `committed` fixes `T_{k-1}` and the window numbering.
    /// * `intent` with `k == committed.k + 1` puts the engine in **redo mode**:
    ///   `T_k` is no longer chosen, it is the intent's `t_end`, so the rebuilt
    ///   window is the same set of records and the upload overwrites either
    ///   nothing or an identical object. An intent with any other `k` is stale
    ///   and ignored.
    /// * `checkpoint` seeds positions, and only if it is at or below
    ///   `committed.k` — a checkpoint ahead of the commit would start the next
    ///   window past records it must ship.
    pub fn restore(
        &mut self,
        committed: Option<Committed>,
        intent: Option<Intent>,
        checkpoint: Option<Checkpoint>,
    ) {
        self.committed_k = committed.as_ref().map(|c| c.k).unwrap_or(0);
        self.committed_t_end = committed.as_ref().map(|c| c.t_end);

        let redo = intent.filter(|i| i.k == self.committed_k + 1);
        self.t_prev = match (&committed, &redo) {
            (Some(c), _) => Some(c.t_end),
            // No commit yet: the first window's start is whatever the Start rule
            // says — except in a redo, where it is whatever the first attempt
            // used, so that the rebuilt set is identical. For `earliest` that is
            // still MIN (the first attempt buffered everything); for `latest` it
            // is the T_0 the first attempt chose, which the intent recorded.
            (None, Some(i)) if self.cfg.start == Start::Latest => Some(i.t_start),
            (None, _) => match self.cfg.start {
                Start::Earliest => Some(Micros::MIN),
                Start::Latest => None,
            },
        };
        self.t_filter = self.t_prev;
        self.redo = redo;

        if let Some(cp) = checkpoint {
            if cp.k <= self.committed_k {
                for (name, offset) in cp.positions {
                    self.restored.insert(Arc::from(name.as_str()), offset);
                }
                self.last_checkpoint_k = cp.k;
            }
        }
    }

    // -- events -------------------------------------------------------------

    /// Merge one page of `partitions/changed`.
    ///
    /// `safe_time` and `degraded` come from the response envelope, which carries
    /// them once for the whole batch of queues.
    pub fn on_discovery(&mut self, page: &ChangedEntry, safe_time: Micros, degraded: bool) {
        if self.failed() {
            return;
        }
        self.discovery_outstanding = false;
        if let Some(err) = &page.error {
            if err == "UNKNOWN_TOPIC_OR_PARTITION" {
                self.fail(format!(
                    "queue {:?}: UNKNOWN_TOPIC_OR_PARTITION",
                    self.queue
                ));
            } else {
                // Anything else is retried: the pass simply restarts.
                self.pass.open = false;
                self.pass.cursor = None;
            }
            return;
        }

        self.degraded = degraded;
        if self.safe_time.map(|s| safe_time > s).unwrap_or(true) {
            self.safe_time = Some(safe_time);
        }
        // `start=latest`: T_0 is the first safeTime seen (plan §4.1).
        if self.t_prev.is_none() {
            self.t_prev = Some(safe_time);
            self.t_filter = Some(safe_time);
        }
        self.pass.safe = Some(match self.pass.safe {
            Some(s) => s.min(safe_time),
            None => safe_time,
        });

        for bounds in &page.partitions {
            self.merge_partition(bounds, safe_time);
        }

        match &page.next {
            Some(cursor) => self.pass.cursor = Some(cursor.clone()),
            None => self.complete_pass(),
        }
    }

    /// The discovery call did not answer. The pass restarts from its first page:
    /// pagination cursors are only meaningful within one pass, and a superset is
    /// harmless (plan §4.3 step 1).
    pub fn on_discovery_failed(&mut self) {
        self.discovery_outstanding = false;
        self.pass.open = false;
        self.pass.cursor = None;
    }

    /// The answer to exactly one [`Action::Fetch`].
    pub fn on_fetch(&mut self, entries: Vec<FetchedEntry>) {
        if self.failed() {
            return;
        }
        self.outstanding_calls = self.outstanding_calls.saturating_sub(1);
        let observed_safe = self.inflight_safe.or(self.safe_time);
        let t_filter = self.t_filter.unwrap_or(Micros::MIN);
        for entry in entries {
            self.merge_fetch(entry, observed_safe, t_filter);
            if self.failed() {
                break;
            }
        }
        self.settle_inflight_safe();
    }

    /// The fetch call itself failed (transport, 5xx, 429). Nothing is known
    /// about the partitions; they become schedulable again.
    pub fn on_fetch_failed(&mut self, partitions: &[Arc<str>]) {
        self.outstanding_calls = self.outstanding_calls.saturating_sub(1);
        for name in partitions {
            if let Some(p) = self.parts.get_mut(name) {
                if p.outstanding == Outstanding::Fetch {
                    p.outstanding = Outstanding::None;
                }
            }
        }
        self.settle_inflight_safe();
    }

    /// The answer to an [`Action::Seek`]: the first offset with `ts >= t`.
    pub fn on_seek_result(&mut self, partition: &str, next_offset: i64) {
        let observed_safe = self.inflight_safe.or(self.safe_time);
        let Some(p) = self.parts.get_mut(partition) else {
            self.outstanding_seeks = self.outstanding_seeks.saturating_sub(1);
            self.settle_inflight_safe();
            return;
        };
        if p.outstanding == Outstanding::Seek {
            self.outstanding_seeks = self.outstanding_seeks.saturating_sub(1);
        }
        p.outstanding = Outstanding::None;
        p.pending_seek = false;
        p.next_offset = next_offset.max(p.log_start);
        // Nothing has been read at the new position, so the frontier is unknown
        // again unless the position is the head.
        p.last_ts = None;
        if p.caught_up() {
            p.caught_up_at = observed_safe;
        }
        let name = p.name.clone();
        self.need_seek.remove(&name);
        self.reindex(&name);
        self.settle_inflight_safe();
    }

    /// The seek could not be completed. The partition falls back to `log_start`
    /// (plan §4.5(3)): always correct, and the records it re-reads are the ones
    /// the `ts < T_{k-1}` filter drops.
    pub fn on_seek_failed(&mut self, partition: &str) {
        let Some(p) = self.parts.get_mut(partition) else {
            self.outstanding_seeks = self.outstanding_seeks.saturating_sub(1);
            self.settle_inflight_safe();
            return;
        };
        if p.outstanding == Outstanding::Seek {
            self.outstanding_seeks = self.outstanding_seeks.saturating_sub(1);
        }
        p.outstanding = Outstanding::None;
        p.pending_seek = false;
        p.next_offset = p.log_start;
        p.last_ts = None;
        let name = p.name.clone();
        self.need_seek.remove(&name);
        self.reindex(&name);
        self.settle_inflight_safe();
    }

    /// Step 4 confirmed: `T_k` is durable, the upload may start.
    pub fn on_intent_written(&mut self, k: u64) {
        if self.state == EngineState::Intent(k) {
            self.state = EngineState::Upload(k);
            if let Some(f) = &self.in_flight {
                self.pending.push(Action::Upload(f.plan.clone()));
            }
        }
    }

    /// Step 5 confirmed: the object and its manifest are up.
    pub fn on_uploaded(&mut self, k: u64, manifest_key: String, records: u64, bytes: u64) {
        if self.state == EngineState::Upload(k) {
            let Some(f) = &self.in_flight else { return };
            self.state = EngineState::Commit(k);
            self.pending.push(Action::Commit(Committed {
                k,
                t_end: f.t_end,
                manifest: manifest_key,
                records,
                bytes,
                // The engine has no wall clock; the driver stamps this.
                committed_at_ms: 0,
            }));
        }
    }

    /// Step 6 confirmed: the window is committed and the next one begins.
    pub fn on_committed(&mut self, k: u64) {
        if self.state != EngineState::Commit(k) {
            return;
        }
        let Some(f) = self.in_flight.take() else {
            return;
        };
        self.committed_k = k;
        self.committed_t_end = Some(f.t_end);
        self.t_prev = Some(f.t_end);
        self.t_filter = Some(f.t_end);
        self.state = EngineState::Filling;
        self.discovered_this_window = false;
        self.stats.windows_committed += 1;
        self.prune();
        if should_checkpoint(k, self.cfg.checkpoint_every) {
            self.pending.push(Action::Checkpoint(Checkpoint {
                k,
                t_end: f.t_end,
                positions: self.positions(),
            }));
        }
    }

    /// The checkpoint of window `k` landed. Informational: it feeds
    /// `queen_s3_checkpoint_age_windows` and nothing else.
    pub fn on_checkpointed(&mut self, k: u64) {
        self.last_checkpoint_k = self.last_checkpoint_k.max(k);
    }

    /// The driver's global memory budget was hit (`QUEEN_S3_MEMORY_MB`,
    /// plan §6.2): close the window at the largest legal `T_k` on the next tick,
    /// whatever its size or age. It cannot force an *illegal* close — the
    /// frontier and `safeTime` ceilings still hold — so a queue that is blocked
    /// on an unread partition stays blocked, and the driver must pick another
    /// queue.
    pub fn force_close(&mut self) {
        self.forced = true;
    }

    /// Everything the driver should do now. An empty vector means "you already
    /// have outstanding work"; [`Action::Idle`] means there is genuinely nothing
    /// to do until the cadence comes round.
    pub fn next_actions(&mut self) -> Vec<Action> {
        let mut out = std::mem::take(&mut self.pending);
        if self.failed() {
            return out;
        }
        if self.state == EngineState::Filling {
            self.try_close();
            out.append(&mut std::mem::take(&mut self.pending));
            if self.state == EngineState::Filling {
                self.schedule(&mut out);
            }
        }
        if out.is_empty() && self.idle() {
            out.push(Action::Idle {
                min_wait_ms: self.cfg.discovery_interval_ms,
            });
        }
        out
    }

    // -- accessors ----------------------------------------------------------

    /// Uncompressed bytes ([`Record::weight`]) held for the window being filled
    /// and the one after it. The number plan §4.4 bounds.
    pub fn buffered_bytes(&self) -> usize {
        self.buffered_weight
    }

    /// The last committed window, `0` when none has been committed.
    pub fn committed_k(&self) -> u64 {
        self.committed_k
    }

    /// The window being filled or committed.
    pub fn next_k(&self) -> u64 {
        self.committed_k + 1
    }

    /// `safeTime − committed.tEnd`: the SLO of plan §6.8, in the broker's clock
    /// rather than the sink's. Saturates for a queue with nothing committed and
    /// `start=earliest`, where `T_{k-1}` is −∞.
    pub fn lag(&self, safe_time: Micros) -> Micros {
        match self.committed_t_end.or(self.t_prev) {
            Some(t) => safe_time.saturating_sub(t),
            None => Micros(0),
        }
    }

    /// The position map: for each tracked partition the offset the **next**
    /// window starts from, sorted by name and ready for a [`Checkpoint`].
    pub fn positions(&self) -> Vec<(String, i64)> {
        positions_from(
            self.parts
                .values()
                .map(|p| (p.name.as_ref(), p.checkpoint_offset())),
        )
    }

    pub fn state(&self) -> EngineState {
        self.state.clone()
    }

    pub fn stats(&self) -> &EngineStats {
        &self.stats
    }

    /// Partitions with a live position. Grows with the queue's cardinality; the
    /// one structure that does (plan §2).
    pub fn tracked_partitions(&self) -> usize {
        self.parts.len()
    }

    /// The highest `safeTime` any discovery has reported.
    pub fn safe_time(&self) -> Option<Micros> {
        self.safe_time
    }

    /// The broker could not see every session and answered its floor instead
    /// (plan §5.2). A latency fact, never a correctness one.
    pub fn safe_time_degraded(&self) -> bool {
        self.degraded
    }

    /// `T_{k-1}`: the lower bound of the window being filled.
    pub fn t_prev(&self) -> Option<Micros> {
        self.t_prev
    }

    /// Whether the engine is rebuilding a window whose intent survived a crash.
    pub fn redoing(&self) -> bool {
        self.redo.is_some()
    }

    // -- discovery merge ----------------------------------------------------

    fn merge_partition(&mut self, bounds: &PartitionBounds, safe_time: Micros) {
        let name = bounds.name.clone();
        let k = self.next_k();
        let gen = self.pass.gen;
        let t_prev = self.t_prev;
        let start = self.cfg.start;

        let mut restored_gap: Option<LostRange> = None;
        let fresh = !self.parts.contains_key(&name);
        if fresh {
            // Rule 6 (plan §4.5): where does a partition we have never tracked
            // start reading?
            let restored = self.restored.remove(&name);
            // A restored position BELOW `logStart` is a retention overrun the
            // sink was down for (plan §4.6). Clamping it up silently would be
            // the one way this connector loses records without saying so: the
            // fetch path reports the same gap through OFFSET_OUT_OF_RANGE, and
            // a partition first seen after a restart must not be the quiet
            // exception. So the gap is recorded here, before the clamp.
            if let Some(o) = restored {
                if o < bounds.log_start {
                    restored_gap = Some(LostRange {
                        partition: name.to_string(),
                        from: o,
                        to: bounds.log_start - 1,
                    });
                }
            }
            let (next_offset, needs_seek) = match restored {
                // 1. A checkpoint entry is the position, verbatim — clamped to
                //    what retention still holds, with the gap recorded above.
                Some(o) => (o.max(bounds.log_start), false),
                None => match start {
                    // 3. `earliest`: everything retention still holds.
                    Start::Earliest => (bounds.log_start, false),
                    Start::Latest => match (t_prev, bounds.last_write_at) {
                        // The cheap case, and at a million partitions the only
                        // one that matters: the partition's last write is
                        // provably below T_{k-1}, so every record it holds is
                        // already outside the window. `lastWriteAt` is quantized
                        // DOWN by up to a second (001_log_schema.sql:39-45), so
                        // the true last write is within a second above it —
                        // hence the `+ 1s` before the comparison.
                        (Some(tp), Some(w)) if w.saturating_add(Micros::SECOND) < tp => {
                            (bounds.last_offset + 1, false)
                        }
                        // 2. Otherwise the position has to be searched for.
                        (Some(_), _) => (bounds.log_start, true),
                        // No T_0 yet: read nothing until there is one.
                        (None, _) => (bounds.last_offset + 1, false),
                    },
                },
            };
            self.parts.insert(
                name.clone(),
                PartitionState {
                    name: name.clone(),
                    next_offset,
                    last_offset: bounds.last_offset,
                    log_start: bounds.log_start,
                    last_write_at: bounds.last_write_at,
                    last_ts: None,
                    caught_up_at: None,
                    added_gen: gen,
                    pending_seek: needs_seek,
                    outstanding: Outstanding::None,
                    last_seen_k: k,
                    buffered: Vec::new(),
                    idx: None,
                },
            );
            if needs_seek {
                self.need_seek.insert(name.clone());
            }
        }

        let mut lost: Option<LostRange> = None;
        {
            let p = self.parts.get_mut(&name).expect("just inserted");
            p.last_seen_k = k;
            p.last_offset = bounds.last_offset;
            p.last_write_at = bounds.last_write_at;
            if bounds.log_start > p.log_start {
                p.log_start = bounds.log_start;
            }
            // Retention overrun seen through discovery (plan §4.6).
            if !p.pending_seek && p.log_start > p.next_offset {
                lost = Some(LostRange {
                    partition: p.name.to_string(),
                    from: p.next_offset,
                    to: p.log_start - 1,
                });
                p.next_offset = p.log_start;
                p.last_ts = None;
            }
            if p.caught_up() {
                p.caught_up_at = Some(safe_time);
            }
        }
        if let Some(l) = restored_gap {
            self.record_lost(l);
        }
        if let Some(l) = lost {
            self.record_lost(l);
        }
        self.reindex(&name);
    }

    fn complete_pass(&mut self) {
        // Every tracked partition is now covered by this pass: either it was
        // returned, or it was not — and "not returned by a complete pass with
        // `since = X`" means `lastWriteAt < X`, which (with the slack) means it
        // has no unread records at all. Either way its frontier is the pass's
        // safeTime. See the module docs.
        self.complete_pass_gen = self.pass.gen;
        self.complete_pass_safe = self.pass.safe;
        self.uncovered.clear();
        self.pass.open = false;
        self.pass.cursor = None;
        // `safe` is per-pass state: a page that arrives without the engine
        // having asked (a driver may discover on its own initiative) must start
        // a new minimum rather than inherit this pass's, and must count as a new
        // generation so the partitions it introduces are treated as uncovered.
        self.pass.safe = None;
        self.pass.gen = self.next_gen;
        self.next_gen += 1;
        self.discovered_this_window = true;
        self.cadence_armed = false;
        self.stats.discoveries += 1;
        if self.pass.full {
            self.stats.full_sweeps += 1;
        }
    }

    // -- fetch merge --------------------------------------------------------

    fn merge_fetch(
        &mut self,
        entry: FetchedEntry,
        observed_safe: Option<Micros>,
        t_filter: Micros,
    ) {
        let name = entry.partition.clone();
        if !self.parts.contains_key(&name) {
            // Pruned while the call was in flight; the answer is simply dropped.
            return;
        }

        let mut lost: Option<LostRange> = None;
        let mut dropped = 0u64;
        let mut unknown = false;
        let mut other_error = false;
        let mut weight_delta = 0usize;
        let mut has_buffer = false;

        {
            let p = self.parts.get_mut(&name).expect("checked above");
            if p.outstanding == Outstanding::Fetch {
                p.outstanding = Outstanding::None;
            }
            // Both bounds come back whether or not the entry carried a record
            // (plan F1), so every answer refreshes them.
            p.last_offset = entry.high_watermark - 1;
            if entry.log_start_offset > p.log_start {
                p.log_start = entry.log_start_offset;
            }

            match &entry.error {
                Some(FetchError::UnknownTopicOrPartition) => unknown = true,
                Some(FetchError::OffsetOutOfRange) => {
                    if entry.log_start_offset > p.next_offset {
                        // Retention passed the sink by (plan §4.6). Never
                        // silent: the gap goes into the window's manifest and
                        // into records_lost_total.
                        lost = Some(LostRange {
                            partition: p.name.to_string(),
                            from: p.next_offset,
                            to: entry.log_start_offset - 1,
                        });
                        p.next_offset = entry.log_start_offset;
                        p.last_ts = None;
                    } else if p.next_offset > entry.high_watermark {
                        // Above the head: our bounds were stale. Resume there.
                        p.next_offset = entry.high_watermark;
                        p.last_ts = None;
                    }
                }
                Some(FetchError::Other(_)) => {
                    // Not structural: the driver retries the same step next
                    // tick, and the partition keeps blocking the close until it
                    // answers — dropping it from the frontier would close a
                    // window over records nobody read.
                    other_error = true;
                }
                None => {
                    let base = p.buffered.last().map(|b| b.cum).unwrap_or(0);
                    let mut cum = base;
                    for rec in entry.records {
                        if rec.offset < p.next_offset {
                            continue;
                        }
                        p.next_offset = rec.offset + 1;
                        // The frontier's lower bound: ts never goes down within
                        // a partition, so the next record is at or after this.
                        p.last_ts = Some(rec.ts);
                        if rec.ts < t_filter {
                            // Rule 3: already committed. The only cost of a
                            // stale position, and never a correctness one.
                            dropped += 1;
                            continue;
                        }
                        cum += rec.weight();
                        p.buffered.push(Buffered { rec, cum });
                    }
                    weight_delta = cum - base;
                    has_buffer = !p.buffered.is_empty();
                }
            }
            if p.caught_up() {
                p.caught_up_at = observed_safe;
            }
        }

        if unknown {
            self.fail(format!(
                "queue {:?}: UNKNOWN_TOPIC_OR_PARTITION",
                self.queue
            ));
            return;
        }
        if other_error {
            self.stats.fetch_errors += 1;
        }
        self.stats.records_dropped += dropped;
        self.buffered_weight += weight_delta;
        if has_buffer {
            self.with_buffer.insert(name.clone());
        }
        if let Some(l) = lost {
            self.record_lost(l);
        }
        self.reindex(&name);
    }

    // -- closing ------------------------------------------------------------

    /// Try to close the window being filled. Emits [`Action::WriteIntent`] (or
    /// [`Action::Upload`] in redo mode) and moves the state machine on.
    ///
    /// The candidate `T_k` is the largest value satisfying, all at once:
    ///
    /// * `T_k <= safeTime − guard` — nothing below it can still appear;
    /// * `T_k <= min frontier` — every partition is complete below it;
    /// * `T_k <=` the next alignment boundary after `T_{k-1}` — no window
    ///   straddles a Hive bucket;
    /// * `T_k` at a record-`ts` boundary when the buffer below the cap already
    ///   holds about `target_bytes`.
    ///
    /// and it closes only when one of the four *triggers* has fired: size, age,
    /// reaching the alignment boundary, or [`Engine::force_close`]. `T_k` is
    /// always a real record timestamp, an alignment boundary, or `safeTime`
    /// minus the guard — never a number the engine made up.
    ///
    /// **Empty windows are not emitted.** An empty *aligned* interval is
    /// stepped over instead: `T_{k-1}` advances to the boundary in memory
    /// without an object and without a commit. Without that, a queue idle for
    /// five hours would have its cap pinned to the first hour boundary after
    /// `T_{k-1}`, find no records below it, and never advance again. The
    /// interval it steps over is provably empty (it lies below every frontier
    /// and below `safeTime`), the step is recomputed identically after a
    /// restart from `committed.tEnd`, and it is counted as
    /// `boundaries_skipped`.
    fn try_close(&mut self) {
        // The ceiling is the `safeTime` of the last COMPLETE discovery pass, not
        // the newest one seen. That is what makes an untracked partition safe:
        // a partition created — or written for the first time — after that pass
        // holds records with `ts >= pass safeTime`, so they land at or above any
        // `T_k` this close can choose, and the window that eventually ships them
        // is a later one. Closing against a newer `safeTime` than the last pass
        // would let a window run past a partition discovery has never named, and
        // its records would then be dropped by the `ts < T_{k-1}` filter as
        // "already committed" — silently.
        let (Some(mut t_prev), Some(safe)) = (self.t_prev, self.complete_pass_safe) else {
            return;
        };
        let c_safe = safe.saturating_sub(self.cfg.safe_guard);

        // Redo mode: T_k is not chosen, it is remembered.
        if let Some(intent) = self.redo.clone() {
            let t_k = intent.t_end;
            if self.min_frontier().map(|f| f < t_k).unwrap_or(false) {
                return; // still reading; the set is not complete yet
            }
            let plan = self.build_plan(intent.k, intent.t_start, t_k);
            self.redo = None;
            self.stats.windows_closed += 1;
            self.forced = false;
            self.t_filter = Some(t_k);
            self.state = EngineState::Upload(intent.k);
            self.in_flight = Some(InFlight {
                t_end: t_k,
                plan: plan.clone(),
            });
            self.pending.push(Action::Upload(plan));
            return;
        }

        // Alignment can step over empty buckets; everything else is one pass.
        let mut guard_iterations = 0u32;
        loop {
            guard_iterations += 1;
            if guard_iterations > 100_000 {
                return;
            }
            let c_front = self.min_frontier();
            let c_align = self.align_cap(t_prev);
            let mut cap = c_safe;
            if let Some(f) = c_front {
                cap = cap.min(f);
            }
            if let Some(a) = c_align {
                cap = cap.min(a);
            }
            if cap <= t_prev {
                return;
            }

            let size_t = self.size_close(cap);
            let (t_k, size_fired) = match size_t {
                Some(t) => (t, true),
                None => (cap, false),
            };
            let aligned = c_align == Some(cap) && cap <= c_safe && c_front.is_none_or(|f| cap <= f);
            let aged = safe.saturating_sub(t_prev) >= self.cfg.max_window;
            if !(size_fired || aligned || aged || self.forced) {
                return;
            }

            let (count, _) = self.below(t_k);
            if count == 0 {
                if aligned && !size_fired {
                    // Step over an empty aligned interval; see the doc comment.
                    t_prev = t_k;
                    self.t_prev = Some(t_k);
                    self.t_filter = Some(t_k);
                    self.stats.boundaries_skipped += 1;
                    continue;
                }
                return;
            }

            let k = self.next_k();
            // The first window of `start=earliest` has T_{k-1} = −∞, which is
            // not a key, not a Hive bucket and not a number a manifest should
            // carry. Report its floor instead: the aligned bucket the first
            // record falls in, or the first record itself when unaligned.
            let t_start = if t_prev == Micros::MIN {
                let min_ts = self.min_buffered_ts().unwrap_or(t_k);
                match self.cfg.align.unit() {
                    Some(u) => min_ts.floor_to(u),
                    None => min_ts,
                }
            } else {
                t_prev
            };
            let plan = self.build_plan(k, t_start, t_k);
            self.stats.windows_closed += 1;
            self.forced = false;
            self.t_filter = Some(t_k);
            self.state = EngineState::Intent(k);
            self.in_flight = Some(InFlight { t_end: t_k, plan });
            self.pending.push(Action::WriteIntent(Intent {
                k,
                t_start,
                t_end: t_k,
                format: self.meta.format,
                compression: self.meta.compression,
                layout: self.meta.layout,
                writer: self.meta.writer.clone(),
            }));
            return;
        }
    }

    /// Drain every buffered record below `t_end` into a plan, in
    /// `(partition, offset)` order.
    fn build_plan(&mut self, k: u64, t_start: Micros, t_end: Micros) -> WindowPlan {
        let mut records: Vec<Record> = Vec::new();
        let mut partitions = 0u64;
        let mut min_ts: Option<Micros> = None;
        let mut max_ts: Option<Micros> = None;
        let mut bytes = 0usize;
        let mut emptied: Vec<Arc<str>> = Vec::new();

        // `with_buffer` is a BTreeSet of names, so this walk is name order; a
        // partition's buffer is offset order. The pair is the total order the
        // writers require, and it does not depend on arrival order — which is
        // what `plan_is_independent_of_fetch_arrival_order` pins.
        let names: Vec<Arc<str>> = self.with_buffer.iter().cloned().collect();
        for name in &names {
            let Some(p) = self.parts.get_mut(name) else {
                continue;
            };
            let cut = p.buffered.partition_point(|b| b.rec.ts < t_end);
            if cut == 0 {
                continue;
            }
            partitions += 1;
            let taken: Vec<Buffered> = p.buffered.drain(..cut).collect();
            let taken_weight = taken.last().map(|b| b.cum).unwrap_or(0);
            bytes += taken_weight;
            for b in taken {
                let ts = b.rec.ts;
                min_ts = Some(min_ts.map_or(ts, |m: Micros| m.min(ts)));
                max_ts = Some(max_ts.map_or(ts, |m: Micros| m.max(ts)));
                records.push(b.rec);
            }
            // Rebase the running weights of what is left.
            for b in p.buffered.iter_mut() {
                b.cum -= taken_weight;
            }
            if p.buffered.is_empty() {
                emptied.push(name.clone());
            }
        }
        for name in emptied {
            self.with_buffer.remove(&name);
        }
        self.buffered_weight = self.buffered_weight.saturating_sub(bytes);

        WindowPlan {
            k,
            t_start,
            t_end,
            records,
            lost: std::mem::take(&mut self.lost),
            partitions,
            min_ts,
            max_ts,
            bytes_estimate: bytes,
        }
    }

    /// The next alignment boundary a window starting at `t_prev` may not cross.
    /// With `t_prev = −∞` the boundary is computed from the first real record,
    /// because −∞ has no successor boundary.
    fn align_cap(&self, t_prev: Micros) -> Option<Micros> {
        let unit = self.cfg.align.unit()?;
        if t_prev == Micros::MIN {
            Some(self.min_buffered_ts()?.next_boundary(unit))
        } else {
            Some(t_prev.next_boundary(unit))
        }
    }

    /// `Some(T_k)` when the size rule fires: the smallest record timestamp such
    /// that the buffer strictly below it already weighs `target_bytes`, or `cap`
    /// itself when the crossing happens at the very last timestamp.
    fn size_close(&self, cap: Micros) -> Option<Micros> {
        // Cheap gate: the weight below the cap can never exceed the total.
        if self.buffered_weight < self.cfg.target_bytes {
            return None;
        }
        let (_, below) = self.below(cap);
        if below < self.cfg.target_bytes {
            return None;
        }
        // Once per window, and the window is `target_bytes` of data: a sort of
        // its timestamps is not the expensive part of closing it.
        let mut items: Vec<(Micros, usize)> = Vec::new();
        for name in &self.with_buffer {
            let Some(p) = self.parts.get(name) else {
                continue;
            };
            let cut = p.buffered.partition_point(|b| b.rec.ts < cap);
            for b in &p.buffered[..cut] {
                items.push((b.rec.ts, b.rec.weight()));
            }
        }
        items.sort_unstable_by_key(|(t, _)| *t);
        let mut acc = 0usize;
        let mut i = 0usize;
        while i < items.len() {
            let t = items[i].0;
            if acc >= self.cfg.target_bytes {
                return Some(t);
            }
            while i < items.len() && items[i].0 == t {
                acc += items[i].1;
                i += 1;
            }
        }
        Some(cap)
    }

    /// `(records, weight)` of the buffer strictly below `t`.
    fn below(&self, t: Micros) -> (usize, usize) {
        let mut count = 0usize;
        let mut weight = 0usize;
        for name in &self.with_buffer {
            let Some(p) = self.parts.get(name) else {
                continue;
            };
            let cut = p.buffered.partition_point(|b| b.rec.ts < t);
            if cut > 0 {
                count += cut;
                weight += p.buffered[cut - 1].cum;
            }
        }
        (count, weight)
    }

    fn min_buffered_ts(&self) -> Option<Micros> {
        let mut m: Option<Micros> = None;
        for name in &self.with_buffer {
            let Some(p) = self.parts.get(name) else {
                continue;
            };
            if let Some(b) = p.buffered.first() {
                m = Some(m.map_or(b.rec.ts, |x: Micros| x.min(b.rec.ts)));
            }
        }
        m
    }

    /// The smallest frontier over every tracked partition, or `None` when the
    /// queue has no tracked partition at all (nothing can be missing).
    ///
    /// The `caught` term is floored by the last complete pass's `safeTime`,
    /// which is sound for every partition that pass covered. Partitions it did
    /// not cover — those first seen during or after it — are in `uncovered` and
    /// contribute their own, unfloored value, so the minimum is never
    /// overstated.
    fn min_frontier(&self) -> Option<Micros> {
        let mut m: Option<Micros> = None;
        if let Some((t, _)) = self.unread.iter().next() {
            m = Some(*t);
        }
        if let Some((t, _)) = self.caught.iter().next() {
            let eff = match self.complete_pass_safe {
                Some(c) => (*t).max(c),
                None => *t,
            };
            m = Some(m.map_or(eff, |x: Micros| x.min(eff)));
        }
        if let Some((t, _)) = self.uncovered.iter().next() {
            m = Some(m.map_or(*t, |x: Micros| x.min(*t)));
        }
        m
    }

    // -- scheduling ---------------------------------------------------------

    fn schedule(&mut self, out: &mut Vec<Action>) {
        let started = out.len();
        // Reading comes first: discovery is what the engine does when there is
        // nothing left to read, plus once per window for a fresh `safeTime`.
        // The other order would put a discovery call on every tick of a
        // backfill.
        self.schedule_seeks(out);
        self.schedule_fetches(out);
        let busy = out.len() > started;
        self.schedule_discovery(out, busy);
    }

    /// Emit at most one discovery page.
    ///
    /// Three reasons to discover, and only the third is paced:
    ///
    /// 1. the pass is half-read — a partial pass proves nothing about the
    ///    partitions its remaining pages hold, so it is always finished;
    /// 2. nothing has been discovered yet, or this window has not had a pass —
    ///    a window cannot close without a `safeTime` and a frontier;
    /// 3. cadence: nothing else to do.
    ///
    /// The engine has no clock, so it cannot pace (3) itself. It instead emits
    /// [`Action::Idle`] once — with the cadence as `min_wait_ms` — and only
    /// discovers on the call after it. That turns the driver's wait into the
    /// discovery interval without the engine ever reading a clock.
    fn schedule_discovery(&mut self, out: &mut Vec<Action>, busy: bool) {
        if self.discovery_outstanding {
            return;
        }
        if self.pass.open {
            if self.pass.cursor.is_some() {
                self.emit_discover(out);
            }
            return;
        }
        if self.stats.discoveries == 0 || !self.discovered_this_window {
            self.start_pass();
            self.emit_discover(out);
            return;
        }
        if busy || self.outstanding_calls > 0 || self.outstanding_seeks > 0 {
            return;
        }
        if !self.cadence_armed {
            self.cadence_armed = true;
            return; // next_actions turns this into an Idle
        }
        self.cadence_armed = false;
        self.start_pass();
        self.emit_discover(out);
    }

    fn start_pass(&mut self) {
        let gen = self.next_gen;
        self.next_gen += 1;
        // The first discovery is always a full enumeration, and every
        // `full_sweep_every` after it: the reconciliation pass of plan §4.3.
        let full = self
            .stats
            .discoveries
            .is_multiple_of(self.cfg.full_sweep_every as u64);
        let since = if full {
            None
        } else {
            // −∞ has no useful `since`, so a queue that has not committed a
            // window yet enumerates in full.
            self.t_prev
                .filter(|t| *t != Micros::MIN)
                .map(|t| t.saturating_sub(self.cfg.discovery_slack))
        };
        self.pass = PassState {
            gen,
            open: true,
            safe: None,
            since,
            cursor: None,
            full: full || since.is_none(),
        };
    }

    fn emit_discover(&mut self, out: &mut Vec<Action>) {
        self.discovery_outstanding = true;
        let after = self.pass.cursor.take();
        out.push(Action::Discover {
            since: self.pass.since,
            after,
        });
    }

    fn schedule_seeks(&mut self, out: &mut Vec<Action>) {
        if self.outstanding_seeks >= self.cfg.fetch_concurrency {
            return;
        }
        let room = self.cfg.fetch_concurrency - self.outstanding_seeks;
        let Some(t) = self.t_prev else { return };
        let names: Vec<Arc<str>> = self
            .need_seek
            .iter()
            .filter(|n| {
                self.parts
                    .get(*n)
                    .map(|p| p.pending_seek && p.outstanding == Outstanding::None)
                    .unwrap_or(false)
            })
            .take(room)
            .cloned()
            .collect();
        for name in names {
            let Some(p) = self.parts.get_mut(&name) else {
                continue;
            };
            p.outstanding = Outstanding::Seek;
            if self.inflight_safe.is_none() {
                self.inflight_safe = self.safe_time;
            }
            let action = Action::Seek {
                partition: name.clone(),
                t,
                last_offset: p.last_offset,
                log_start: p.log_start,
            };
            self.outstanding_seeks += 1;
            self.stats.seeks_issued += 1;
            out.push(action);
        }
    }

    /// Fetch **lowest frontier first**, and never a partition whose next record
    /// cannot land in the window being filled.
    ///
    /// That second half is the memory bound of plan §4.4: the buffer holds one
    /// window's worth plus what the in-flight calls return, whatever the
    /// partition count, because a partition already beyond the horizon is simply
    /// not read. A caught-up partition is not fetched at all until discovery
    /// says it moved.
    fn schedule_fetches(&mut self, out: &mut Vec<Action>) {
        if self.outstanding_calls >= self.cfg.fetch_concurrency {
            return;
        }
        let calls = self.cfg.fetch_concurrency - self.outstanding_calls;
        let max_entries = calls * self.cfg.fetch_entries_per_call;
        let ceiling = self.fetch_ceiling();

        let mut chosen: Vec<Arc<str>> = Vec::new();
        for (frontier, name) in self.unread.iter() {
            if chosen.len() >= max_entries {
                break;
            }
            // The set is ordered by frontier, so the first partition beyond the
            // horizon ends the walk — this is what keeps scheduling O(chosen)
            // instead of O(partitions).
            if *frontier >= ceiling {
                break;
            }
            let Some(p) = self.parts.get(name) else {
                continue;
            };
            if p.outstanding != Outstanding::None || p.pending_seek {
                continue;
            }
            chosen.push(name.clone());
        }

        for batch in chosen.chunks(self.cfg.fetch_entries_per_call) {
            let mut entries = Vec::with_capacity(batch.len());
            for name in batch {
                let Some(p) = self.parts.get_mut(name) else {
                    continue;
                };
                p.outstanding = Outstanding::Fetch;
                entries.push(FetchRequestEntry {
                    queue: self.queue.clone(),
                    partition: name.clone(),
                    offset: p.next_offset,
                    max_bytes: Some(self.cfg.max_bytes_per_entry),
                });
            }
            if !entries.is_empty() {
                if self.inflight_safe.is_none() {
                    self.inflight_safe = self.safe_time;
                }
                self.outstanding_calls += 1;
                out.push(Action::Fetch(entries));
            }
        }
    }

    /// Records at or above this cannot belong to the window being filled, so
    /// their partitions are not read (plan §4.4). The horizon past the cap is
    /// one alignment unit, or one `max_window` when unaligned: enough that the
    /// *next* window's partitions are already warm, not so much that a backfill
    /// reads the whole log into memory.
    fn fetch_ceiling(&self) -> Micros {
        let Some(safe) = self.complete_pass_safe else {
            return Micros(i64::MAX);
        };
        let Some(t_prev) = self.t_prev else {
            return Micros(i64::MAX);
        };
        let horizon = self.cfg.align.unit().unwrap_or(self.cfg.max_window);
        let mut base = safe.saturating_sub(self.cfg.safe_guard);
        if let Some(a) = self.align_cap(t_prev) {
            base = base.min(a);
        }
        base.saturating_add(horizon)
    }

    fn idle(&self) -> bool {
        !self.discovery_outstanding
            && self.outstanding_calls == 0
            && self.outstanding_seeks == 0
            && self.state == EngineState::Filling
    }

    // -- bookkeeping --------------------------------------------------------

    /// Put a partition in the right ordered index, or move it between them.
    fn reindex(&mut self, name: &Arc<str>) {
        let Some(p) = self.parts.get(name) else {
            return;
        };
        let new = if p.caught_up() {
            Idx::Caught(p.caught_up_at.unwrap_or(Micros::MIN))
        } else {
            // Unknown frontier sorts first: it is both the most urgent to fetch
            // and the one that blocks every close.
            Idx::Unread(p.last_ts.unwrap_or(Micros::MIN))
        };
        let old = p.idx;
        if old == Some(new) {
            return;
        }
        let uncovered = p.added_gen > self.complete_pass_gen;
        if let Some(o) = old {
            match o {
                Idx::Unread(t) => {
                    self.unread.remove(&(t, name.clone()));
                }
                Idx::Caught(t) => {
                    self.caught.remove(&(t, name.clone()));
                    self.uncovered.remove(&(t, name.clone()));
                }
            }
        }
        match new {
            Idx::Unread(t) => {
                self.unread.insert((t, name.clone()));
            }
            Idx::Caught(t) => {
                self.caught.insert((t, name.clone()));
                if uncovered {
                    self.uncovered.insert((t, name.clone()));
                }
            }
        }
        if let Some(p) = self.parts.get_mut(name) {
            p.idx = Some(new);
        }
    }

    fn drop_partition(&mut self, name: &Arc<str>) {
        let Some(p) = self.parts.remove(name) else {
            return;
        };
        match p.idx {
            Some(Idx::Unread(t)) => {
                self.unread.remove(&(t, name.clone()));
            }
            Some(Idx::Caught(t)) => {
                self.caught.remove(&(t, name.clone()));
                self.uncovered.remove(&(t, name.clone()));
            }
            None => {}
        }
        self.with_buffer.remove(name);
        self.need_seek.remove(name);
        self.buffered_weight = self.buffered_weight.saturating_sub(p.buffered_weight());
    }

    /// Drop the positions of partitions discovery has not named for
    /// `prune_after_windows` windows (plan §12: the partition cleanup deletes
    /// *empty* partitions, and a position map that remembers every partition
    /// that ever existed is the one thing here that grows without bound).
    ///
    /// Only caught-up partitions with nothing buffered and nothing outstanding
    /// are dropped; a partition that writes again comes back through discovery
    /// with a position from rule 6.
    fn prune(&mut self) {
        if self.cfg.prune_after_windows == 0 {
            return;
        }
        let k = self.committed_k;
        let doomed: Vec<Arc<str>> = self
            .parts
            .values()
            .filter(|p| {
                k >= p.last_seen_k + self.cfg.prune_after_windows
                    && p.caught_up()
                    && p.buffered.is_empty()
                    && p.outstanding == Outstanding::None
            })
            .map(|p| p.name.clone())
            .collect();
        self.stats.partitions_pruned += doomed.len() as u64;
        for name in doomed {
            self.drop_partition(&name);
        }
    }

    fn record_lost(&mut self, l: LostRange) {
        self.stats.records_lost += (l.to - l.from + 1).max(0) as u64;
        self.lost.push(l);
    }

    /// Once no call is outstanding, the next one may be attributed to the
    /// current `safeTime` again.
    fn settle_inflight_safe(&mut self) {
        if self.outstanding_calls == 0 && self.outstanding_seeks == 0 {
            self.inflight_safe = None;
        }
    }

    fn fail(&mut self, why: String) {
        self.state = EngineState::Failed(why);
    }

    fn failed(&self) -> bool {
        matches!(self.state, EngineState::Failed(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> IntentMeta {
        IntentMeta {
            writer: "queen-s3/test jsonl+zstd".into(),
            format: Format::Jsonl,
            compression: Compression::Zstd,
            layout: Layout::Merged,
        }
    }

    #[test]
    fn config_clamps_to_the_broker_ceilings() {
        let cfg = EngineConfig {
            fetch_entries_per_call: 99_999,
            discovery_limit: 99_999,
            fetch_concurrency: 0,
            full_sweep_every: 0,
            ..EngineConfig::default()
        };
        let e = Engine::new("q".into(), cfg, meta());
        assert_eq!(e.cfg.fetch_entries_per_call, 1024, "plan F1");
        assert_eq!(e.cfg.discovery_limit, 1000, "plan §5.1");
        assert_eq!(e.cfg.fetch_concurrency, 1);
        assert_eq!(e.cfg.full_sweep_every, 1);
    }

    #[test]
    fn a_fresh_engine_starts_by_discovering_everything() {
        let mut e = Engine::new("q".into(), EngineConfig::default(), meta());
        let a = e.next_actions();
        assert_eq!(
            a,
            vec![Action::Discover {
                since: None,
                after: None
            }],
            "the first discovery is always a full enumeration"
        );
        assert_eq!(e.state(), EngineState::Filling);
        assert_eq!(e.committed_k(), 0);
        assert_eq!(e.next_k(), 1);
    }

    #[test]
    fn earliest_starts_at_minus_infinity_and_latest_waits_for_a_safe_time() {
        let e = Engine::new(
            "q".into(),
            EngineConfig {
                start: Start::Earliest,
                ..EngineConfig::default()
            },
            meta(),
        );
        assert_eq!(e.t_prev(), Some(Micros::MIN));
        let e = Engine::new("q".into(), EngineConfig::default(), meta());
        assert_eq!(e.t_prev(), None, "start=latest has no T_0 until discovery");
    }

    #[test]
    fn restore_takes_t_prev_from_the_commit_and_enters_redo() {
        let mut e = Engine::new("q".into(), EngineConfig::default(), meta());
        e.restore(
            Some(Committed {
                k: 7,
                t_end: Micros(5_000),
                manifest: "m".into(),
                records: 3,
                bytes: 9,
                committed_at_ms: 1,
            }),
            Some(Intent {
                k: 8,
                t_start: Micros(5_000),
                t_end: Micros(9_000),
                format: Format::Jsonl,
                compression: Compression::Zstd,
                layout: Layout::Merged,
                writer: "w".into(),
            }),
            Some(Checkpoint {
                k: 6,
                t_end: Micros(4_000),
                positions: vec![("p".into(), 12)],
            }),
        );
        assert_eq!(e.committed_k(), 7);
        assert_eq!(e.t_prev(), Some(Micros(5_000)));
        assert!(e.redoing());
        assert_eq!(e.restored.get("p"), Some(&12));
    }

    #[test]
    fn restore_ignores_a_stale_intent_and_a_checkpoint_ahead_of_the_commit() {
        let mut e = Engine::new("q".into(), EngineConfig::default(), meta());
        e.restore(
            Some(Committed {
                k: 7,
                t_end: Micros(5_000),
                manifest: "m".into(),
                records: 3,
                bytes: 9,
                committed_at_ms: 1,
            }),
            Some(Intent {
                k: 7,
                t_start: Micros(1),
                t_end: Micros(5_000),
                format: Format::Jsonl,
                compression: Compression::Zstd,
                layout: Layout::Merged,
                writer: "w".into(),
            }),
            Some(Checkpoint {
                k: 9,
                t_end: Micros(9_000),
                positions: vec![("p".into(), 12)],
            }),
        );
        assert!(!e.redoing(), "intent.k == committed.k is already committed");
        assert!(
            e.restored.is_empty(),
            "a checkpoint above the commit is unusable"
        );
    }
}
