//! driver — the per-queue task that wires the engine to Queen, to S3 and to the
//! writers (plan §4.3, §6.1).
//!
//! [`window::Engine`] decides *what* must happen and this module makes it
//! happen: it is the only place in the connector where a decision meets a
//! socket. The shape is a loop over [`Action`]s, one queue per [`QueueDriver`],
//! and the whole of plan §4.3 is visible in [`QueueDriver::tick`]'s match arms —
//! discover, fetch, seek, intent, upload, commit, checkpoint.
//!
//! # The three things that are not obvious
//!
//! **Every action is answered, exactly once.** The engine has no clock and no
//! timeout: an action executed and never answered leaves it waiting for ever
//! (see the [`crate::window`] header). So every path out of an execution —
//! success, transport failure, a per-entry error, a page that never came — ends
//! in the matching `on_*` call. The one deliberate exception is a shutdown
//! drain, where the engine is about to be dropped.
//!
//! **A retry is a retry of the same step.** Plan §6.7: the sink never drops, it
//! only lags. Anything [`SinkError::is_retriable`] is retried behind an
//! exponential backoff (1 s → 60 s, jittered, `Retry-After` honoured exactly)
//! for as long as it takes; a fetch is the one call that does not loop in place,
//! because the engine re-schedules the same partitions on the next tick and
//! keeps them blocking the close in the meantime (plan §6.7, the P1 wording).
//!
//! **A lost precondition is not an error, it is a verdict.** It means another
//! instance owns this queue (plan §6.6), and the only correct response is to
//! stop touching it — [`Stop::Fenced`]. The lease's `required` write at index 0
//! of every intent and commit batch is what turns "another owner" from a race
//! into a rolled-back batch.
//!
//! # Crash injection
//!
//! `QUEEN_S3_CRASH_AT` ([`CrashAt`]) fires at the five points of the plan §9
//! matrix. In production it is unset; in the e2e suite it aborts a real process
//! ([`CrashMode::Abort`]) so that the restart is a real restart; in the crate's
//! own tests it returns from the task instead ([`CrashMode::Return`]) so that
//! one process can run the whole matrix.
//!
//! # What this module does NOT do
//!
//! It does not own the process: signals, the queue set, the health listener and
//! the lease handover loop are `main.rs`. It does not decide a window boundary,
//! ever — every timestamp it handles came from the broker.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use serde_json::Value;
use tokio::sync::Notify;
use tokio::task::JoinSet;

use crate::checkpoint;
use crate::config::{Config, CrashAt};
use crate::health::HealthState;
use crate::layout::{self, DataKey};
use crate::lease::{committed_key, intent_key, result_at, Lease};
use crate::obs::{now_epoch_ms, Metrics, Sampler};
use crate::queen::{KvOp, KvResult, QueenApi};
use crate::s3::client::MidUploadHook;
use crate::s3::ObjectStore;
use crate::seek::{Seek, SeekStep};
use crate::types::{
    Align, ChangedRequestEntry, Checkpoint, Committed, Format, Intent, Layout, LostRange, Manifest,
    ManifestObject, Micros, SinkError,
};
use crate::window::{Action, Engine, EngineConfig, EngineState, IntentMeta, WindowPlan};
use crate::writer::WriterFactory;

/// The longest a retry waits once the process is stopping. Short enough that a
/// drain is not held up by a backoff it will never finish, long enough that a
/// retry loop against something that is gone is not a spin.
const DRAIN_BACKOFF: Duration = Duration::from_millis(250);

/// One line per five seconds for each repeated failure kind (the facades'
/// idiom): a bucket having a bad ten minutes must not be a log flood.
static PUT_FAIL: Sampler = Sampler::new(5_000);
static KV_FAIL: Sampler = Sampler::new(5_000);
static FETCH_FAIL: Sampler = Sampler::new(5_000);
static DISCOVER_FAIL: Sampler = Sampler::new(5_000);

/// What a crash point does when it fires.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CrashMode {
    /// `std::process::abort()` — no unwinding, no destructor, no flush: the
    /// closest thing to the `SIGKILL` the e2e suite is really testing against.
    Abort,
    /// Return [`Stop::Crashed`] from the queue task. The in-process crash matrix
    /// needs the test process to survive its own fault injection.
    Return,
}

/// Why a queue task ended. Every variant is terminal for THAT task; `main`
/// decides whether the queue is retried, and after how long.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stop {
    /// A `required` precondition lost: another instance owns this queue, or the
    /// commit pointer moved under this one. Retriable only after the lease TTL.
    Fenced(String),
    /// Terminal for the queue itself — `UNKNOWN_TOPIC_OR_PARTITION`, a
    /// non-retriable status, a bucket that refuses a write. A restart of the
    /// queue task will not fix it; an operator will.
    Failed(String),
    /// A shutdown signal was observed and the queue drained (plan §6.7).
    Drained,
    /// `QUEEN_S3_CRASH_AT` fired under [`CrashMode::Return`].
    Crashed(CrashAt),
}

impl Stop {
    /// Whether re-acquiring the queue later is worth trying.
    pub fn is_retriable(&self) -> bool {
        matches!(self, Stop::Fenced(_))
    }
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

/// The process-wide stop signal, shared by every queue task.
///
/// A flag plus a `Notify` rather than a `watch` channel because the semantics
/// wanted here are latching: once set it stays set, a waiter that arrives after
/// the signal returns immediately, and there is no value to carry.
#[derive(Clone, Default)]
pub struct Shutdown {
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl Shutdown {
    pub fn new() -> Shutdown {
        Shutdown::default()
    }

    pub fn trigger(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub fn is_set(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    /// Resolve as soon as the signal is set, now or later.
    pub async fn wait(&self) {
        loop {
            // Register BEFORE the check: a `trigger` between the two would
            // otherwise be missed and the waiter would sleep through it.
            let notified = self.notify.notified();
            if self.is_set() {
                return;
            }
            notified.await;
        }
    }
}

// ---------------------------------------------------------------------------
// The global memory budget
// ---------------------------------------------------------------------------

/// `QUEEN_S3_MEMORY_MB` across every queue of the process (plan §4.4, §6.2).
///
/// Per-queue memory is already bounded by the frontier rule — one window's worth
/// plus what the in-flight fetches return — so this is the cross-queue backstop:
/// forty queues each legitimately holding a window is forty windows. When the
/// sum goes over, the queue with the LARGEST buffer closes early, which is both
/// the biggest win and the only choice that cannot livelock (a fixed victim
/// could be a queue with nothing to close).
pub struct MemoryBudget {
    limit: usize,
    queues: Mutex<BTreeMap<String, usize>>,
    total: AtomicUsize,
}

impl MemoryBudget {
    pub fn new(limit: usize) -> MemoryBudget {
        MemoryBudget {
            limit: limit.max(1),
            queues: Mutex::new(BTreeMap::new()),
            total: AtomicUsize::new(0),
        }
    }

    /// Publish one queue's buffered bytes. `true` means "you are the largest and
    /// the process is over budget: close now".
    pub fn report(&self, queue: &str, bytes: usize) -> bool {
        let mut g = self.queues.lock().expect("memory budget lock");
        g.insert(queue.to_string(), bytes);
        let total: usize = g.values().sum();
        self.total.store(total, Ordering::Relaxed);
        if total <= self.limit || bytes == 0 {
            return false;
        }
        bytes == g.values().copied().max().unwrap_or(0)
    }

    /// Drop a queue this process no longer owns.
    pub fn forget(&self, queue: &str) {
        let mut g = self.queues.lock().expect("memory budget lock");
        g.remove(queue);
        let total: usize = g.values().sum();
        self.total.store(total, Ordering::Relaxed);
    }

    pub fn total(&self) -> usize {
        self.total.load(Ordering::Relaxed)
    }

    pub fn limit(&self) -> usize {
        self.limit
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Everything a queue task needs from the operator's configuration, resolved
/// once at boot. Separate from [`Config`] because the driver is also built
/// directly by the tests, which have no environment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriverConfig {
    pub sink: String,
    /// `QUEEN_S3_PREFIX`, already stripped of its slashes.
    pub prefix: String,
    pub layout: Layout,
    pub align: Align,
    pub engine: EngineConfig,
    pub crash_at: CrashAt,
    pub crash_mode: CrashMode,
    /// Records one probe of a [`Seek`] asks for. Small: a probe is a bisection
    /// step, not a read.
    pub seek_probe_records: usize,
    /// Attempts a single seek probe makes before the seek gives up and the
    /// partition falls back to `logStart` (plan §4.5(3)).
    pub seek_probe_attempts: u32,
    pub backoff_base_ms: u64,
    pub backoff_max_ms: u64,
    /// Attempts for a checkpoint upload, which is best-effort by definition
    /// (plan §4.3 step 7): it never blocks a commit, so it does not retry
    /// forever either.
    pub checkpoint_attempts: u32,
    /// On SIGTERM, a window at least this big is worth finishing (plan §6.7).
    pub drain_min_bytes: usize,
    /// …and so is one that has been open at least this long, on the BROKER's
    /// clock (`safeTime − T_{k-1}`), which is the only clock a window is ever
    /// measured against.
    pub drain_min_age: Micros,
}

impl DriverConfig {
    /// The operator's configuration, resolved.
    ///
    /// One invariant is established here rather than documented: the engine's
    /// `prune_after_windows` MUST exceed the number of windows between two full
    /// discovery sweeps, or a live-but-idle partition is pruned and re-read for
    /// ever (see [`EngineConfig::prune_after_windows`]). A window costs at least
    /// one discovery (the engine forces a pass per window), so windows between
    /// sweeps ≤ `full_sweep_every`; deriving the prune horizon as **twice**
    /// that makes the invariant hold by construction, whatever the sweep cadence
    /// becomes.
    pub fn from_config(cfg: &Config) -> DriverConfig {
        let defaults = EngineConfig::default();
        let full_sweep_every = defaults.full_sweep_every;
        DriverConfig {
            sink: cfg.sink.clone(),
            prefix: cfg.prefix.clone(),
            layout: cfg.layout,
            align: cfg.align,
            engine: EngineConfig {
                target_bytes: cfg.target_bytes(),
                max_window: Micros::from_millis(cfg.max_window_ms as i64),
                align: cfg.align,
                safe_guard: Micros::from_millis(cfg.safe_guard_ms as i64),
                start: cfg.start,
                checkpoint_every: cfg.checkpoint_every as u64,
                fetch_concurrency: cfg.fetch_concurrency as usize,
                full_sweep_every,
                prune_after_windows: 2 * full_sweep_every as u64,
                discovery_interval_ms: cfg.discovery_interval_ms,
                ..defaults
            },
            crash_at: cfg.crash_at,
            crash_mode: CrashMode::Abort,
            seek_probe_records: 1,
            seek_probe_attempts: 3,
            backoff_base_ms: 1_000,
            backoff_max_ms: crate::s3::MAX_BACKOFF_MS,
            checkpoint_attempts: 3,
            drain_min_bytes: 1024 * 1024,
            drain_min_age: Micros::from_millis(10_000),
        }
    }
}

/// `QUEEN_S3_PARTITIONS` is not supported in this release.
///
/// A static partition list is the zero-broker-change mode plan §5.1 sketched for
/// a demo, and it cannot be made correct as written: a window may close only at
/// or below `safeTime` (plan §4.1), `safeTime` is answered by
/// `POST /api/v1/partitions/changed`, and a sink with a static list never calls
/// it. Rather than ship a mode whose windows are chosen against nothing, the
/// boot refuses and says which endpoint it needs.
pub fn reject_static_partitions(cfg: &Config) -> Result<(), String> {
    if cfg.partitions.is_none() {
        return Ok(());
    }
    Err(
        "QUEEN_S3_PARTITIONS is set: the static partition list is not supported in this \
         release. A window may only close at or below the broker's safeTime, and safeTime is \
         answered by POST /api/v1/partitions/changed — the endpoint discovery already uses — \
         so a sink with a static list has no boundary it is allowed to close at. Unset \
         QUEEN_S3_PARTITIONS; the sink discovers the partitions of QUEEN_S3_QUEUES itself, at \
         any cardinality"
            .to_string(),
    )
}

/// The hook that implements `QUEEN_S3_CRASH_AT=mid_upload` for a MULTIPART
/// upload: installed on the [`crate::s3::S3Client`] by `main`, called between
/// parts, and it does not return. A window big enough to go up in parts is the
/// case where a crash leaves a real stranded multipart upload behind, which is
/// the thing the matrix is checking the restart survives.
pub fn mid_upload_abort_hook() -> MidUploadHook {
    Arc::new(|parts: usize| {
        tracing::error!(
            target: "queen-s3",
            parts,
            "QUEEN_S3_CRASH_AT=mid_upload: aborting between multipart parts"
        );
        std::process::abort();
    })
}

// ---------------------------------------------------------------------------
// The shared runtime
// ---------------------------------------------------------------------------

/// The handles every queue task shares. One per process; cloning is a handful
/// of `Arc` bumps.
#[derive(Clone)]
pub struct Runtime {
    pub cfg: Arc<DriverConfig>,
    pub queen: Arc<dyn QueenApi>,
    pub store: Arc<dyn ObjectStore>,
    pub writers: Arc<dyn WriterFactory>,
    pub metrics: Arc<Metrics>,
    pub health: Arc<HealthState>,
    pub budget: Arc<MemoryBudget>,
    pub shutdown: Shutdown,
}

// ---------------------------------------------------------------------------
// The driver
// ---------------------------------------------------------------------------

/// One queue's task.
pub struct QueueDriver {
    rt: Runtime,
    queue: String,
    lease: Arc<Lease>,
    engine: Engine,
    intent_key: String,
    committed_key: String,
    /// The version each pointer key holds, as this instance last saw it. `0` =
    /// absent, which is also what the store answers for an expired row.
    intent_version: i64,
    committed_version: i64,
    /// The lost ranges of the window being uploaded, carried from the plan to
    /// the commit so the counter and the manifest agree.
    pending_lost: Vec<LostRange>,
    last_checkpoint_k: u64,
    started: bool,
    draining: bool,
    /// A drain closes at most one window; this says it already did.
    drain_committed: bool,
    degraded_logged: bool,
}

impl QueueDriver {
    pub fn new(rt: Runtime, queue: String, lease: Arc<Lease>) -> QueueDriver {
        let meta = IntentMeta {
            writer: rt.writers.describe(),
            format: rt.writers.format(),
            compression: rt.writers.compression(),
            layout: rt.cfg.layout,
        };
        let engine = Engine::new(queue.clone(), rt.cfg.engine.clone(), meta);
        QueueDriver {
            intent_key: intent_key(&rt.cfg.sink, &queue),
            committed_key: committed_key(&rt.cfg.sink, &queue),
            rt,
            queue,
            lease,
            engine,
            intent_version: 0,
            committed_version: 0,
            pending_lost: Vec::new(),
            last_checkpoint_k: 0,
            started: false,
            draining: false,
            drain_committed: false,
            degraded_logged: false,
        }
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// The engine, for assertions. Read-only on purpose: everything that moves
    /// it goes through an action.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Run until something stops the queue.
    pub async fn run(mut self) -> Stop {
        loop {
            if let Some(stop) = self.tick().await {
                self.finish(&stop);
                return stop;
            }
        }
    }

    /// One round: ask the engine what to do, do all of it, answer all of it.
    ///
    /// `Some(stop)` ends the queue. Public because the crate's own tests drive
    /// the driver a tick at a time, which is what makes them deterministic — no
    /// spawned task, no wall clock, no polling for a condition.
    pub async fn tick(&mut self) -> Option<Stop> {
        if !self.started {
            self.started = true;
            if let Some(stop) = self.start().await {
                return Some(stop);
            }
        }
        if self.lease.lost() {
            return Some(Stop::Fenced(format!(
                "queue {:?}: the lease is held by another instance",
                self.queue
            )));
        }
        if let EngineState::Failed(why) = self.engine.state() {
            return Some(Stop::Failed(why));
        }
        if self.rt.shutdown.is_set() && !self.draining {
            self.begin_drain();
        }
        if self.draining && self.drain_committed {
            // ONE window per drain (plan §6.7: "close the CURRENT window …
            // else abandon"). The buffer may well hold enough for another, and
            // closing it would be free of any read — but a drain that keeps
            // going while there is anything left to close is a drain with no
            // bound, and the supervisor's grace is a fixed thirty seconds. What
            // is abandoned is re-read, identically, by the next process.
            // The one thing still worth doing is the checkpoint the commit
            // queued: it costs one PUT and it saves that re-read.
            for action in self.engine.next_actions() {
                if let Action::Checkpoint(cp) = action {
                    self.do_checkpoint(cp).await;
                }
            }
            return Some(Stop::Drained);
        }

        let actions = self.engine.next_actions();
        let mut worked = 0usize;
        let mut fetches: Vec<Vec<crate::types::FetchRequestEntry>> = Vec::new();
        let mut seeks: Vec<(Arc<str>, Micros, i64, i64)> = Vec::new();

        for action in actions {
            match action {
                Action::Fetch(entries) => {
                    if self.draining {
                        // Answer it anyway: an unanswered action leaves the
                        // engine believing a call is in flight, and the drain
                        // still wants an honest close decision.
                        let names: Vec<Arc<str>> =
                            entries.iter().map(|e| e.partition.clone()).collect();
                        self.engine.on_fetch_failed(&names);
                    } else {
                        fetches.push(entries);
                    }
                }
                Action::Seek {
                    partition,
                    t,
                    last_offset,
                    log_start,
                } => {
                    // A seek is deliberately NOT answered during a drain: its
                    // partition keeps blocking the close, which is the safe
                    // reading — the alternative, `on_seek_failed`, would rewrite
                    // the position to `logStart` and cost a full re-read on the
                    // next boot.
                    if !self.draining {
                        seeks.push((partition, t, last_offset, log_start));
                    }
                }
                Action::Discover { since, after } => {
                    if !self.draining {
                        worked += 1;
                        self.do_discover(since, after).await;
                    }
                }
                Action::WriteIntent(intent) => {
                    worked += 1;
                    if let Some(stop) = self.do_intent(intent).await {
                        return Some(stop);
                    }
                }
                Action::Upload(plan) => {
                    worked += 1;
                    if let Some(stop) = self.do_upload(plan).await {
                        return Some(stop);
                    }
                }
                Action::Commit(committed) => {
                    worked += 1;
                    if let Some(stop) = self.do_commit(committed).await {
                        return Some(stop);
                    }
                }
                Action::Checkpoint(cp) => {
                    worked += 1;
                    self.do_checkpoint(cp).await;
                }
                Action::Idle { min_wait_ms } => {
                    if !self.draining {
                        self.idle(min_wait_ms).await;
                    }
                }
            }
        }

        if !seeks.is_empty() {
            worked += seeks.len();
            self.do_seeks(seeks).await;
        }
        if !fetches.is_empty() {
            worked += fetches.len();
            self.do_fetches(fetches).await;
        }

        self.after_tick();

        if self.draining && worked == 0 {
            return Some(Stop::Drained);
        }
        None
    }

    // -- boot ---------------------------------------------------------------

    /// Restore the queue from what the KV store and the bucket hold, then hand
    /// it to the engine (plan §4.3's crash matrix, §4.5).
    async fn start(&mut self) -> Option<Stop> {
        self.rt.health.register_queue(&self.queue);
        let ops = vec![KvOp::get_many(vec![
            self.intent_key.clone(),
            self.committed_key.clone(),
        ])];
        let results = match self.kv_batch("restore", ops).await {
            Ok(r) => r,
            Err(stop) => return Some(stop),
        };

        let mut intent: Option<Intent> = None;
        let mut committed: Option<Committed> = None;
        if let Some(r) = result_at(&results, 0) {
            for row in &r.rows {
                // The VERSION is kept even when the document does not parse: it
                // is what the next conditional write must expect, and a pointer
                // this process cannot read is still a pointer another process
                // wrote.
                if row.key == self.intent_key {
                    self.intent_version = row.version;
                    intent = self.parse_doc("intent", &row.value);
                } else if row.key == self.committed_key {
                    self.committed_version = row.version;
                    committed = self.parse_doc("committed", &row.value);
                }
            }
        }

        let committed_k = committed.as_ref().map(|c| c.k).unwrap_or(0);
        let cp = self.load_checkpoint(committed_k).await;
        self.last_checkpoint_k = cp.as_ref().map(|c| c.k).unwrap_or(0);
        tracing::info!(
            target: "queen-s3",
            queue = %self.queue,
            committed_k,
            redo = intent.as_ref().map(|i| i.k == committed_k + 1).unwrap_or(false),
            checkpoint_k = self.last_checkpoint_k,
            positions = cp.as_ref().map(|c| c.positions.len()).unwrap_or(0),
            "queue restored"
        );
        self.engine.restore(committed, intent, cp);
        None
    }

    fn parse_doc<T: serde::de::DeserializeOwned>(&self, what: &str, value: &Value) -> Option<T> {
        match serde_json::from_value::<T>(value.clone()) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!(
                    target: "queen-s3",
                    queue = %self.queue,
                    error = %e,
                    "the {what} pointer does not parse; ignoring the document, keeping its version"
                );
                None
            }
        }
    }

    /// The newest checkpoint at or below `at_most` (plan §4.5(1)). Best effort
    /// in every failure mode: a missing or unreadable position map costs
    /// re-reads that the `ts < T_{k-1}` filter discards, never correctness.
    async fn load_checkpoint(&self, at_most: u64) -> Option<Checkpoint> {
        let prefix = layout::checkpoint_prefix(&self.rt.cfg.prefix, &self.queue);
        let mut keys: Vec<String> = Vec::new();
        let mut after: Option<String> = None;
        let mut pages = 0;
        loop {
            match self.rt.store.list(&prefix, after.clone(), 1_000).await {
                Ok(listing) => {
                    keys.extend(listing.keys);
                    match listing.next {
                        Some(next) => after = Some(next),
                        None => break,
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target: "queen-s3",
                        queue = %self.queue,
                        error = %e,
                        "cannot list checkpoints; starting from the commit pointer alone"
                    );
                    return None;
                }
            }
            pages += 1;
            if pages > 1_000 {
                break;
            }
        }
        let (k, key) = layout::latest_checkpoint(keys.iter().map(String::as_str), at_most)?;
        match self.rt.store.get(key).await {
            Ok(Some(bytes)) => match checkpoint::decode(&bytes) {
                Ok(cp) => Some(cp),
                Err(e) => {
                    tracing::warn!(target: "queen-s3", queue = %self.queue, k, error = %e, "checkpoint unreadable");
                    None
                }
            },
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(target: "queen-s3", queue = %self.queue, k, error = %e, "checkpoint unreadable");
                None
            }
        }
    }

    // -- actions ------------------------------------------------------------

    /// One page of `POST /api/v1/partitions/changed`, one queue per call.
    ///
    /// Batching several queues into one call is allowed (64 per call) and is not
    /// done: the queues of one process run as independent tasks with independent
    /// paging state, and a shared call would couple their cadences to the
    /// slowest of them for a saving of one small request per queue per tick.
    async fn do_discover(&mut self, since: Option<Micros>, after: Option<String>) {
        let entry = ChangedRequestEntry {
            queue: self.queue.clone(),
            since,
            after,
            limit: self.rt.cfg.engine.discovery_limit,
        };
        match self.rt.queen.partitions_changed(vec![entry]).await {
            Ok(resp) => {
                let page = resp
                    .entries
                    .iter()
                    .find(|e| e.queue == self.queue)
                    .or_else(|| resp.entries.first())
                    .cloned();
                let Some(page) = page else {
                    tracing::warn!(target: "queen-s3", queue = %self.queue, "discovery answered no entry for this queue");
                    self.engine.on_discovery_failed();
                    return;
                };
                if resp.safe_time_degraded && !self.degraded_logged {
                    self.degraded_logged = true;
                    // Once per queue, at info, and never a stall: a broker role
                    // without pg_read_all_stats answers its floor instead of a
                    // computed safeTime, which is normal-and-usable (plan §5.2).
                    tracing::info!(
                        target: "queen-s3",
                        queue = %self.queue,
                        safe_time = %resp.safe_time,
                        "safeTime is degraded: the broker answered its floor (grant \
                         pg_read_all_stats to the broker role for a computed one). Windows still \
                         close, one floor behind"
                    );
                }
                self.rt
                    .metrics
                    .set_discovery_partitions(&self.queue, page.partitions.len() as u64);
                self.engine
                    .on_discovery(&page, resp.safe_time, resp.safe_time_degraded);
            }
            Err(e) => {
                if let Some(suppressed) = DISCOVER_FAIL.tick_now() {
                    tracing::warn!(target: "queen-s3", queue = %self.queue, error = %e, suppressed, "discovery failed");
                }
                self.engine.on_discovery_failed();
                self.sleep(self.error_delay(&e)).await;
            }
        }
    }

    /// Up to `fetch_concurrency` fetch calls at once (plan §6.2).
    ///
    /// `maxWaitMs = 0` and `minBytes = 1`: the sink never parks on the broker.
    /// Its cadence is the engine's [`Action::Idle`], which is the discovery
    /// interval — a park would hold a pop-lane admission slot open for nothing
    /// (plan §6.5) and would make the shutdown drain wait for it.
    async fn do_fetches(&mut self, calls: Vec<Vec<crate::types::FetchRequestEntry>>) {
        let mut set: JoinSet<FetchAnswer> = JoinSet::new();
        for entries in calls {
            let queen = self.rt.queen.clone();
            let names: Vec<Arc<str>> = entries.iter().map(|e| e.partition.clone()).collect();
            set.spawn(async move {
                let answer = queen.fetch(entries, 0, 1).await;
                (names, answer)
            });
        }
        let mut failure: Option<SinkError> = None;
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((_, Ok(entries))) => {
                    self.rt.metrics.fetch_call(&self.queue, "ok");
                    self.engine.on_fetch(entries);
                }
                Ok((names, Err(e))) => {
                    self.rt.metrics.fetch_call(&self.queue, "error");
                    if let Some(suppressed) = FETCH_FAIL.tick_now() {
                        tracing::warn!(target: "queen-s3", queue = %self.queue, error = %e, suppressed, "fetch failed; the same partitions are re-scheduled");
                    }
                    self.engine.on_fetch_failed(&names);
                    failure = Some(e);
                }
                Err(join) => {
                    // A panicked fetch task loses the answer the engine is
                    // owed, so the queue cannot continue honestly.
                    tracing::error!(target: "queen-s3", queue = %self.queue, error = %join, "fetch task panicked");
                    self.engine.force_close();
                }
            }
        }
        if let Some(e) = failure {
            self.sleep(self.error_delay(&e)).await;
        }
    }

    /// Recover positions (plan §4.5(2)). Concurrent, because `start=latest` on a
    /// queue that has just been discovered issues one per partition and they are
    /// independent bisections.
    async fn do_seeks(&mut self, seeks: Vec<(Arc<str>, Micros, i64, i64)>) {
        let mut set: JoinSet<(Arc<str>, Option<i64>)> = JoinSet::new();
        for (partition, t, last_offset, log_start) in seeks {
            let queen = self.rt.queen.clone();
            let queue = self.queue.clone();
            let probe_records = self.rt.cfg.seek_probe_records;
            let attempts = self.rt.cfg.seek_probe_attempts;
            set.spawn(async move {
                let answer = run_seek(
                    queen,
                    queue,
                    partition.clone(),
                    t,
                    last_offset,
                    log_start,
                    probe_records,
                    attempts,
                )
                .await;
                (partition, answer)
            });
        }
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((partition, Some(offset))) => self.engine.on_seek_result(&partition, offset),
                Ok((partition, None)) => self.engine.on_seek_failed(&partition),
                Err(join) => {
                    tracing::error!(target: "queen-s3", queue = %self.queue, error = %join, "seek task panicked");
                }
            }
        }
    }

    /// Step 4: fix `T_k` durably before anything is written to the bucket.
    async fn do_intent(&mut self, intent: Intent) -> Option<Stop> {
        let value = match serde_json::to_value(&intent) {
            Ok(v) => v,
            Err(e) => return Some(Stop::Failed(format!("intent does not serialise: {e}"))),
        };
        let ops = vec![
            self.lease.fence_op(),
            KvOp::fence(self.intent_key.clone(), value, self.intent_version),
        ];
        let results = match self.kv_batch("intent", ops).await {
            Ok(r) => r,
            Err(stop) => return Some(stop),
        };
        self.lease.note(&results);
        if let Some(r) = result_at(&results, 1) {
            self.intent_version = r.version;
        }
        if let Some(stop) = self.crash(CrashAt::AfterIntent) {
            return Some(stop);
        }
        self.engine.on_intent_written(intent.k);
        None
    }

    /// Step 5: the objects and the manifest.
    async fn do_upload(&mut self, plan: WindowPlan) -> Option<Stop> {
        let k = plan.k;
        let objects = match self.build_objects(&plan) {
            Ok(o) => o,
            Err(e) => return Some(Stop::Failed(format!("window {k}: {e}"))),
        };

        let mut manifest_objects: Vec<ManifestObject> = Vec::with_capacity(objects.len());
        let mut total_records = 0u64;
        let mut total_bytes = 0u64;
        for (i, obj) in objects.into_iter().enumerate() {
            let bytes = obj.body.len() as u64;
            if let Err(stop) = self
                .s3_put("put", &obj.key, obj.body, self.rt.writers.content_type())
                .await
            {
                return Some(stop);
            }
            if i == 0 {
                // The single-PUT half of `mid_upload`: a window small enough to
                // go up in one request has no "between parts" to crash in, so
                // the fault lands between the first data object and the
                // manifest — the same gap, at the same point of the protocol.
                if let Some(stop) = self.crash(CrashAt::MidUpload) {
                    return Some(stop);
                }
            }
            total_records += obj.records;
            total_bytes += bytes;
            manifest_objects.push(ManifestObject {
                key: obj.key,
                bytes,
                records: obj.records,
                sha256: obj.sha256,
                partition: obj.partition,
                first_offset: obj.first_offset,
                last_offset: obj.last_offset,
            });
        }

        let manifest_key = layout::manifest_key(&self.rt.cfg.prefix, &self.queue, k);
        let manifest = Manifest {
            sink: self.rt.cfg.sink.clone(),
            queue: self.queue.clone(),
            k,
            t_start: plan.t_start,
            t_end: plan.t_end,
            format: self.rt.writers.format(),
            compression: self.rt.writers.compression(),
            layout: self.rt.cfg.layout,
            objects: manifest_objects,
            records: total_records,
            bytes: total_bytes,
            partitions: plan.partitions,
            min_ts: plan.min_ts,
            max_ts: plan.max_ts,
            lost: plan.lost.clone(),
            writer: self.rt.writers.describe(),
            // The ONE wall-clock value the connector writes, and it is here
            // rather than in the object so that a redo is byte-identical
            // (plan §4.2, §6.3).
            committed_at: Micros::from_millis(now_epoch_ms()).to_iso(),
        };
        let body = match serde_json::to_vec(&manifest) {
            Ok(b) => Bytes::from(b),
            Err(e) => {
                return Some(Stop::Failed(format!(
                    "manifest {k} does not serialise: {e}"
                )))
            }
        };
        if let Err(stop) = self
            .s3_put("put", &manifest_key, body, "application/json")
            .await
        {
            return Some(stop);
        }
        if let Some(stop) = self.crash(CrashAt::AfterUpload) {
            return Some(stop);
        }

        self.pending_lost = plan.lost;
        self.engine
            .on_uploaded(k, manifest_key, total_records, total_bytes);
        None
    }

    /// Step 6: move the commit pointer, behind the lease fence.
    async fn do_commit(&mut self, committed: Committed) -> Option<Stop> {
        let mut committed = committed;
        // The engine has no wall clock, so the driver stamps this. It is
        // informational: nothing reads it back as a boundary.
        committed.committed_at_ms = now_epoch_ms();
        let value = match serde_json::to_value(&committed) {
            Ok(v) => v,
            Err(e) => return Some(Stop::Failed(format!("commit does not serialise: {e}"))),
        };
        let ops = vec![
            self.lease.fence_op(),
            KvOp::fence(self.committed_key.clone(), value, self.committed_version),
        ];
        if let Some(stop) = self.crash(CrashAt::BeforeCommit) {
            return Some(stop);
        }
        let results = match self.kv_batch("commit", ops).await {
            Ok(r) => r,
            Err(stop) => return Some(stop),
        };
        self.lease.note(&results);
        if let Some(r) = result_at(&results, 1) {
            self.committed_version = r.version;
        }
        if let Some(stop) = self.crash(CrashAt::AfterCommit) {
            return Some(stop);
        }

        let k = committed.k;
        self.drain_committed = self.draining;
        self.engine.on_committed(k);
        self.rt
            .health
            .record_commit(&self.queue, committed.committed_at_ms);

        let m = &self.rt.metrics;
        m.window_committed(&self.queue);
        m.records_written(&self.queue, committed.records);
        m.bytes_written(
            &self.queue,
            format_label(self.rt.writers.format()),
            committed.bytes,
        );
        m.observe_window(committed.records, committed.bytes);
        let lost_records: u64 = self
            .pending_lost
            .iter()
            .map(|l| (l.to - l.from + 1).max(0) as u64)
            .sum();
        for l in std::mem::take(&mut self.pending_lost) {
            m.records_lost(&self.queue, (l.to - l.from + 1).max(0) as u64);
            // Never silent (plan §4.6): the gap is in the manifest, in the
            // counter, and in one line per window that has one.
            tracing::warn!(
                target: "queen-s3",
                queue = %self.queue,
                k,
                partition = %l.partition,
                from = l.from,
                to = l.to,
                "retention deleted records before the sink read them"
            );
        }
        let lag = self
            .engine
            .safe_time()
            .map(|safe| secs(safe.saturating_sub(committed.t_end)));
        if let Some(lag) = lag {
            m.set_lag_seconds(&self.queue, lag);
        }
        tracing::info!(
            target: "queen-s3",
            queue = %self.queue,
            k,
            records = committed.records,
            bytes = committed.bytes,
            t_end = %committed.t_end,
            lag_seconds = lag.unwrap_or(0.0),
            lost = lost_records,
            "window committed"
        );
        None
    }

    /// Step 7: the position cache. Best effort, and it never blocks a commit —
    /// what it saves is a re-read, and the re-read is always correct.
    async fn do_checkpoint(&mut self, cp: Checkpoint) {
        let key = layout::checkpoint_key(&self.rt.cfg.prefix, &self.queue, cp.k);
        let body = Bytes::from(checkpoint::encode(&cp));
        for attempt in 0..self.rt.cfg.checkpoint_attempts.max(1) {
            match self
                .rt
                .store
                .put(&key, body.clone(), "application/zstd")
                .await
            {
                Ok(_) => {
                    self.last_checkpoint_k = self.last_checkpoint_k.max(cp.k);
                    self.engine.on_checkpointed(cp.k);
                    tracing::debug!(
                        target: "queen-s3",
                        queue = %self.queue,
                        k = cp.k,
                        positions = cp.positions.len(),
                        "checkpoint written"
                    );
                    return;
                }
                Err(e) if e.is_retriable() && attempt + 1 < self.rt.cfg.checkpoint_attempts => {
                    self.sleep(Duration::from_millis(
                        self.rt.cfg.backoff_base_ms << attempt.min(6),
                    ))
                    .await;
                }
                Err(e) => {
                    tracing::warn!(
                        target: "queen-s3",
                        queue = %self.queue,
                        k = cp.k,
                        error = %e,
                        "checkpoint not written; the next restart re-reads more, and nothing else"
                    );
                    return;
                }
            }
        }
    }

    async fn idle(&self, min_wait_ms: u64) {
        self.sleep(Duration::from_millis(min_wait_ms)).await;
    }

    // -- objects ------------------------------------------------------------

    /// Build the window's data objects: one for `merged`, one per partition for
    /// `per-partition`.
    ///
    /// The records arrive sorted by `(partition, offset)` (the engine's
    /// guarantee), so the per-partition split is a walk over runs rather than a
    /// grouping pass, and each object's offsets are its first and last records'.
    fn build_objects(&self, plan: &WindowPlan) -> Result<Vec<BuiltObject>, String> {
        // The Hive bucket comes from `t_start`, which the intent fixed and a
        // window never straddles (plan §4.4). `min_ts` is the fallback for the
        // one value that is not a date: the `-∞` a first `start=earliest`
        // window would open at.
        let bucket_ts = match plan.t_start == Micros::MIN {
            true => plan.min_ts.unwrap_or(plan.t_end),
            false => plan.t_start,
        };
        let ext = self.rt.writers.extension();
        let mut out = Vec::new();
        match self.rt.cfg.layout {
            Layout::Merged => {
                let mut w = self.rt.writers.open();
                for rec in &plan.records {
                    w.push(&self.queue, rec).map_err(|e| e.to_string())?;
                }
                let body = w.finish().map_err(|e| e.to_string())?;
                let key = DataKey {
                    prefix: &self.rt.cfg.prefix,
                    queue: &self.queue,
                    layout: Layout::Merged,
                    align: self.rt.cfg.align,
                    k: plan.k,
                    t_start: plan.t_start,
                    t_end: plan.t_end,
                    bucket_ts,
                    ext,
                    partition: None,
                    offsets: None,
                }
                .render();
                out.push(BuiltObject::new(
                    key,
                    body,
                    plan.records.len() as u64,
                    None,
                    None,
                ));
            }
            Layout::PerPartition => {
                let mut i = 0usize;
                while i < plan.records.len() {
                    let name = plan.records[i].partition.clone();
                    let mut j = i;
                    let mut w = self.rt.writers.open();
                    while j < plan.records.len() && plan.records[j].partition == name {
                        w.push(&self.queue, &plan.records[j])
                            .map_err(|e| e.to_string())?;
                        j += 1;
                    }
                    let body = w.finish().map_err(|e| e.to_string())?;
                    let first = plan.records[i].offset;
                    let last = plan.records[j - 1].offset;
                    let key = DataKey {
                        prefix: &self.rt.cfg.prefix,
                        queue: &self.queue,
                        layout: Layout::PerPartition,
                        align: self.rt.cfg.align,
                        k: plan.k,
                        t_start: plan.t_start,
                        t_end: plan.t_end,
                        bucket_ts,
                        ext,
                        partition: Some(name.as_ref()),
                        offsets: Some((first, last)),
                    }
                    .render();
                    out.push(BuiltObject::new(
                        key,
                        body,
                        (j - i) as u64,
                        Some(name.to_string()),
                        Some((first, last)),
                    ));
                    i = j;
                }
            }
        }
        Ok(out)
    }

    // -- plumbing -----------------------------------------------------------

    /// A KV batch, retried in place. The only two ways out are an answer and a
    /// [`Stop`].
    async fn kv_batch(&self, what: &'static str, ops: Vec<KvOp>) -> Result<Vec<KvResult>, Stop> {
        let mut backoff = Backoff::new(self.rt.cfg.backoff_base_ms, self.rt.cfg.backoff_max_ms);
        loop {
            match self.rt.queen.kv(ops.clone()).await {
                Ok(results) => return Ok(results),
                Err(e @ SinkError::Precondition { .. }) => {
                    self.rt.metrics.commit_precondition_lost();
                    if let SinkError::Precondition { failed_index, .. } = &e {
                        if *failed_index == 0 {
                            self.lease.mark_lost();
                        }
                    }
                    return Err(Stop::Fenced(format!("{what}: {e}")));
                }
                Err(e) if e.is_retriable() => {
                    if let Some(suppressed) = KV_FAIL.tick_now() {
                        tracing::warn!(target: "queen-s3", queue = %self.queue, op = what, error = %e, suppressed, "kv call failed; retrying");
                    }
                    self.sleep(backoff.next_delay(&e)).await;
                }
                Err(e) => return Err(Stop::Failed(format!("{what}: {e}"))),
            }
        }
    }

    /// A PUT, retried in place (plan §6.7: forever, behind the backoff).
    async fn s3_put(
        &self,
        op: &'static str,
        key: &str,
        body: Bytes,
        content_type: &str,
    ) -> Result<(), Stop> {
        let mut backoff = Backoff::new(self.rt.cfg.backoff_base_ms, self.rt.cfg.backoff_max_ms);
        loop {
            match self.rt.store.put(key, body.clone(), content_type).await {
                Ok(_) => return Ok(()),
                Err(e) if e.is_retriable() => {
                    if let Some(suppressed) = PUT_FAIL.tick_now() {
                        tracing::warn!(target: "queen-s3", queue = %self.queue, op, key, error = %e, suppressed, "object PUT failed; retrying");
                    }
                    self.sleep(backoff.next_delay(&e)).await;
                }
                Err(e) => return Err(Stop::Failed(format!("{op} {key}: {e}"))),
            }
        }
    }

    /// Publish the gauges and enforce the global memory budget.
    fn after_tick(&mut self) {
        let buffered = self.engine.buffered_bytes();
        let over = self.rt.budget.report(&self.queue, buffered);
        self.rt
            .metrics
            .set_buffer_bytes(self.rt.budget.total() as u64);
        if over {
            tracing::info!(
                target: "queen-s3",
                queue = %self.queue,
                buffered,
                total = self.rt.budget.total(),
                limit = self.rt.budget.limit(),
                "memory budget reached: closing this queue's window early"
            );
            self.engine.force_close();
        }
        if let Some(safe) = self.engine.safe_time() {
            self.rt
                .metrics
                .set_lag_seconds(&self.queue, secs(self.engine.lag(safe)));
            // The sink's own clock against the broker's, for ONE gauge. It is a
            // diagnostic (plan §5.2, §6.8) and never a boundary: no window is
            // opened, closed or filtered against it.
            self.rt.metrics.set_safe_lag_seconds(secs(
                Micros::from_millis(now_epoch_ms()).saturating_sub(safe),
            ));
        }
        self.rt.metrics.set_checkpoint_age_windows(
            &self.queue,
            self.engine
                .committed_k()
                .saturating_sub(self.last_checkpoint_k),
        );
    }

    /// SIGTERM arrived (plan §6.7): stop reading, and decide once whether the
    /// window being filled is worth finishing.
    fn begin_drain(&mut self) {
        self.draining = true;
        let buffered = self.engine.buffered_bytes();
        let aged = self
            .engine
            .safe_time()
            .map(|safe| self.engine.lag(safe) >= self.rt.cfg.drain_min_age)
            .unwrap_or(false);
        let worth = buffered >= self.rt.cfg.drain_min_bytes || (aged && buffered > 0);
        tracing::info!(
            target: "queen-s3",
            queue = %self.queue,
            buffered,
            aged,
            closing = worth,
            state = ?self.engine.state(),
            "shutting down: stopping the reads"
        );
        if worth {
            self.engine.force_close();
        }
    }

    fn finish(&self, stop: &Stop) {
        self.rt.health.forget_queue(&self.queue);
        self.rt.budget.forget(&self.queue);
        match stop {
            Stop::Drained => {
                tracing::info!(target: "queen-s3", queue = %self.queue, "queue drained")
            }
            Stop::Fenced(why) => {
                tracing::warn!(target: "queen-s3", queue = %self.queue, why, "queue fenced")
            }
            Stop::Failed(why) => {
                tracing::error!(target: "queen-s3", queue = %self.queue, why, "queue stopped")
            }
            Stop::Crashed(at) => {
                tracing::error!(target: "queen-s3", queue = %self.queue, at = at.as_str(), "crash point fired")
            }
        }
    }

    /// Fire `at` when it is the configured crash point. Under
    /// [`CrashMode::Abort`] this never returns.
    fn crash(&self, at: CrashAt) -> Option<Stop> {
        if self.rt.cfg.crash_at != at {
            return None;
        }
        match self.rt.cfg.crash_mode {
            CrashMode::Abort => {
                tracing::error!(
                    target: "queen-s3",
                    queue = %self.queue,
                    at = at.as_str(),
                    "QUEEN_S3_CRASH_AT fired: aborting"
                );
                std::process::abort();
            }
            CrashMode::Return => Some(Stop::Crashed(at)),
        }
    }

    fn error_delay(&self, e: &SinkError) -> Duration {
        Backoff::new(self.rt.cfg.backoff_base_ms, self.rt.cfg.backoff_max_ms).next_delay(e)
    }

    /// Sleep, cut short by a shutdown.
    ///
    /// A stopping process must not wait out a sixty-second backoff — but it must
    /// not skip the wait entirely either: the retry loops above are `loop {}`
    /// over a call, and a zero wait would turn a broker that is gone into a hot
    /// loop for the whole of the shutdown grace. So a shutdown shortens the
    /// wait to [`DRAIN_BACKOFF`] rather than removing it.
    async fn sleep(&self, d: Duration) {
        if self.rt.shutdown.is_set() {
            tokio::time::sleep(d.min(DRAIN_BACKOFF)).await;
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(d) => {}
            _ = self.rt.shutdown.wait() => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// What one concurrent fetch task hands back: the partitions it asked about —
/// so the engine can be told they are free again when the call failed — and the
/// call's answer.
type FetchAnswer = (
    Vec<Arc<str>>,
    crate::queen::Result<Vec<crate::types::FetchedEntry>>,
);

/// One data object, built and not yet uploaded.
struct BuiltObject {
    key: String,
    body: Bytes,
    records: u64,
    sha256: String,
    partition: Option<String>,
    first_offset: Option<i64>,
    last_offset: Option<i64>,
}

impl BuiltObject {
    fn new(
        key: String,
        body: Vec<u8>,
        records: u64,
        partition: Option<String>,
        offsets: Option<(i64, i64)>,
    ) -> BuiltObject {
        BuiltObject {
            sha256: crate::s3::sigv4::sha256_hex(&body),
            key,
            body: Bytes::from(body),
            records,
            partition,
            first_offset: offsets.map(|(f, _)| f),
            last_offset: offsets.map(|(_, l)| l),
        }
    }
}

/// Exponential backoff with jitter, and `Retry-After` when the answer carried
/// one (plan §6.7; the facade honours it the same way).
pub struct Backoff {
    attempt: u32,
    base_ms: u64,
    max_ms: u64,
}

impl Backoff {
    pub fn new(base_ms: u64, max_ms: u64) -> Backoff {
        Backoff {
            attempt: 0,
            base_ms: base_ms.max(1),
            max_ms: max_ms.max(1),
        }
    }

    /// The next wait. `Retry-After` is taken EXACTLY: the proxy means it, and a
    /// client that jittered it would be answering a rate limit with a guess.
    pub fn next_delay(&mut self, e: &SinkError) -> Duration {
        self.attempt = self.attempt.saturating_add(1);
        if let SinkError::Status {
            retry_after_ms: Some(ms),
            ..
        } = e
        {
            if *ms >= 0 {
                return Duration::from_millis(*ms as u64);
            }
        }
        let shift = (self.attempt - 1).min(20);
        let step = self.base_ms.saturating_mul(1u64 << shift).min(self.max_ms);
        let jitter = 0.75 + rand::random::<f64>() * 0.5;
        Duration::from_millis(((step as f64) * jitter) as u64)
    }

    pub fn attempts(&self) -> u32 {
        self.attempt
    }
}

/// Drive one [`Seek`] to an answer. `None` means the caller should fall back to
/// `logStart` (plan §4.5(3)) — always correct, and it only costs re-reads.
#[allow(clippy::too_many_arguments)]
async fn run_seek(
    queen: Arc<dyn QueenApi>,
    queue: String,
    partition: Arc<str>,
    t: Micros,
    last_offset: i64,
    log_start: i64,
    probe_records: usize,
    attempts: u32,
) -> Option<i64> {
    let mut seek = Seek::new(queue, partition, t, last_offset, log_start, probe_records);
    loop {
        match seek.step() {
            SeekStep::Found(offset) => return Some(offset),
            SeekStep::Failed(_) => return None,
            SeekStep::Continue => {}
        }
        let probe = seek.next_probe()?;
        let mut answer = None;
        for attempt in 0..attempts.max(1) {
            match queen.fetch(vec![probe.clone()], 0, 1).await {
                Ok(mut entries) if !entries.is_empty() => {
                    answer = Some(entries.remove(0));
                    break;
                }
                Ok(_) => return None,
                Err(e) if e.is_retriable() && attempt + 1 < attempts => {
                    tokio::time::sleep(Duration::from_millis(200u64 << attempt.min(5))).await;
                }
                Err(_) => return None,
            }
        }
        seek.on_result(&answer?);
    }
}

/// Microseconds as seconds, for a gauge. Saturates at zero: a negative lag is a
/// clock question, not a number to publish.
fn secs(m: Micros) -> f64 {
    (m.0.max(0) as f64) / 1_000_000.0
}

/// The `format` label of `queen_s3_bytes_written_total`.
fn format_label(format: Format) -> &'static str {
    match format {
        Format::Jsonl => "jsonl",
        Format::Parquet => "parquet",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::types::Start;

    fn env(extra: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut pairs: Vec<(String, String)> = vec![
            ("QUEEN_S3_QUEUES", "orders"),
            ("QUEEN_S3_ENDPOINT", "http://gw:7070"),
            ("QUEEN_S3_REGION", "us-east-1"),
            ("QUEEN_S3_BUCKET", "lake"),
            ("QUEEN_S3_ACCESS_KEY", "ak"),
            ("QUEEN_S3_SECRET_KEY", "sk"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        pairs.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        pairs
    }

    fn config(extra: &[(&str, &str)]) -> Config {
        let owned = env(extra);
        let borrowed: Vec<(&str, &str)> = owned
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        Config::from_pairs(&borrowed).expect("the fixture configuration must parse")
    }

    #[test]
    fn prune_horizon_exceeds_the_windows_between_full_sweeps() {
        let cfg = DriverConfig::from_config(&config(&[]));
        assert!(
            cfg.engine.prune_after_windows > cfg.engine.full_sweep_every as u64,
            "a live-but-idle partition must never be pruned between two sweeps: \
             prune_after_windows={} full_sweep_every={}",
            cfg.engine.prune_after_windows,
            cfg.engine.full_sweep_every
        );
    }

    #[test]
    fn the_engine_configuration_is_the_operators() {
        let cfg = DriverConfig::from_config(&config(&[
            ("QUEEN_S3_TARGET_MB", "8"),
            ("QUEEN_S3_MAX_WINDOW_MS", "60000"),
            ("QUEEN_S3_ALIGN", "day"),
            ("QUEEN_S3_START", "earliest"),
            ("QUEEN_S3_CHECKPOINT_EVERY", "5"),
            ("QUEEN_S3_FETCH_CONCURRENCY", "9"),
            ("QUEEN_S3_SAFE_GUARD_MS", "1500"),
            ("QUEEN_S3_DISCOVERY_INTERVAL_MS", "250"),
        ]));
        assert_eq!(cfg.engine.target_bytes, 8 * 1024 * 1024);
        assert_eq!(cfg.engine.max_window, Micros::from_millis(60_000));
        assert_eq!(cfg.engine.align, Align::Day);
        assert_eq!(cfg.engine.start, Start::Earliest);
        assert_eq!(cfg.engine.checkpoint_every, 5);
        assert_eq!(cfg.engine.fetch_concurrency, 9);
        assert_eq!(cfg.engine.safe_guard, Micros::from_millis(1_500));
        assert_eq!(cfg.engine.discovery_interval_ms, 250);
        assert_eq!(cfg.crash_mode, CrashMode::Abort, "production never returns");
    }

    #[test]
    fn a_static_partition_list_is_refused_by_name() {
        assert!(reject_static_partitions(&config(&[])).is_ok());
        let err = reject_static_partitions(&config(&[("QUEEN_S3_PARTITIONS", "orders:0,1,2")]))
            .expect_err("the static mode has no safeTime and must not boot");
        assert!(err.contains("QUEEN_S3_PARTITIONS"), "{err}");
        assert!(
            err.contains("POST /api/v1/partitions/changed"),
            "the refusal must name the endpoint the sink needs: {err}"
        );
    }

    #[test]
    fn the_budget_elects_the_largest_buffer_and_only_when_over() {
        let b = MemoryBudget::new(1_000);
        assert!(!b.report("a", 400));
        assert!(!b.report("b", 500));
        assert_eq!(b.total(), 900);
        assert!(!b.report("a", 450), "under the limit nobody closes early");
        assert!(b.report("b", 700), "over the limit the largest closes");
        assert!(!b.report("a", 450), "and only the largest");
        b.forget("b");
        assert_eq!(b.total(), 450);
        assert!(!b.report("a", 450));
    }

    #[test]
    fn backoff_grows_and_honours_retry_after_exactly() {
        let mut b = Backoff::new(1_000, 60_000);
        let first = b.next_delay(&SinkError::Transport("x".into()));
        assert!(first >= Duration::from_millis(750) && first <= Duration::from_millis(1_250));
        let second = b.next_delay(&SinkError::Transport("x".into()));
        assert!(second >= Duration::from_millis(1_500) && second <= Duration::from_millis(2_500));
        for _ in 0..20 {
            let d = b.next_delay(&SinkError::Transport("x".into()));
            assert!(
                d <= Duration::from_millis(75_000),
                "the ceiling holds: {d:?}"
            );
        }
        let mut b = Backoff::new(1_000, 60_000);
        assert_eq!(
            b.next_delay(&SinkError::Status {
                code: 429,
                body: String::new(),
                retry_after_ms: Some(2_500),
            }),
            Duration::from_millis(2_500),
            "Retry-After is taken exactly, never jittered"
        );
    }

    #[tokio::test]
    async fn shutdown_wakes_a_waiter_that_arrives_late() {
        let s = Shutdown::new();
        assert!(!s.is_set());
        s.trigger();
        assert!(s.is_set());
        // Already set: the wait resolves without another trigger.
        tokio::time::timeout(Duration::from_secs(1), s.wait())
            .await
            .expect("a latched signal must not make a late waiter hang");
    }
}
