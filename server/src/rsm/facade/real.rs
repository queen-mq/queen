//! `rsm/facade/real.rs` — the real state machine behind the [`Rsm`] seam
//! (PLAN_RAFT.md WP-1.7c).
//!
//! WP-1.7a routed the message path to the [`super::NotReady`] stub through a
//! builder hook ([`super::set_builder`]); this module fills that hook. One
//! [`RaftFacade`] owns the whole single-node RSM through phase 2:
//!
//! - a [`HeedStore`] (D9), opened at `<data_dir>/store`;
//! - a [`LocalReplicator`] (§12.2) over `<data_dir>/log` and `<data_dir>/seg`,
//!   which spawns WP-1.4's apply thread (the only writer, I1) and publishes the
//!   segment [`segments::Reader`] the payload reads use (§7.5, D7);
//! - a [`Batcher`] (§7.1), spawned as one tokio task, that drains commands,
//!   plans, proposes and answers each receiver once its entry is committed AND
//!   applied on this node (I4).
//!
//! # What the facade does, and where the line to the planner is
//!
//! The facade is the RECEIVER of §9.1: it does the pool-free pre-work the
//! Postgres handlers used to do against a connection — parse the wire body, mint
//! message ids, hash transaction ids (`xxh3_128`), pack one frame per message
//! (O20, the survivors' frames are concatenated by the planner, never
//! repacked), build the typed [`Command`], submit it, and render the wire answer
//! from the [`Outcome`] the batcher returns. It never plans and never writes
//! committed state; the planner (`rsm/planner`) and apply (`rsm/apply`) own
//! that. A pop's payload bytes are read from THIS node's own segment files after
//! the claim applied locally (D7).
//!
//! Phase 2 adds transactions, KV, timers, streams, administration, dashboard
//! reads, retention, metrics and compaction to that same committed-state seam.
//! Producer subjects come only from validated auth, queue-configured payloads
//! are encrypted before they reach either payload log and decrypted at reads,
//! and forced-DLQ rows carry the original message id and payload snapshot.
//!
//! One wire representation remains intentionally Raft-native:
//!
//! - **`partitionId` is the numeric pid** (decimal), not a uuid: the RSM has no
//!   uuid→pid index, and the ack must map the wire `partitionId` back to a pid
//!   AND to a queue, both of which the pid gives directly (`partition(pid)`).
//!   Every SDK treats `partitionId` as an opaque string (the C1/C-SQS notes in
//!   `handlers/data.rs`), so this is behaviourally transparent within a raft
//!   deployment. Read/admin endpoints accept both this decimal form and the
//!   UUID exposed by resource views.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::frames::{
    pack_frames, unpack_frames_ref, uuid_bytes_to_string, uuid_string_to_bytes, FrameIn,
};
use crate::notify::Notifier;
use crate::rsm::apply::SystemClock;
use crate::rsm::batcher::{Batcher, BatcherConfig, Command, CommandTx, Reply, Submission};
use crate::rsm::effect::{Pid, QueueConfig};
use crate::rsm::entry::{Outcome, PopClaim, PushVerdict, RequestId};
use crate::rsm::planner::timers::{
    list_limit, list_row_json, parse_timer_ops, peek_json, timers_results, TimerOp, TimersCommand,
};
use crate::rsm::planner::{
    bucket_of, AckCommand, AckItem, AckStatus, AckTarget, DlqSnapshot, PopCommand, PushCommand,
    PushItem, RenewCommand, SubIntent,
};
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::replicator::local::{LocalReplicator, OpenConfig, Waker};
use crate::rsm::replicator::Replicator;
use crate::rsm::segments;
use crate::rsm::store::{rows, HeedStore, Reads, Store, StoreOpts, TypedReads};
use crate::util::{txn_hash128, uuidv7_bytes};

use super::{
    AckOut, AckReq, ApiOut, ApiReq, DepthOut, DepthReq, DlqHeadOut, DlqHeadReq, KvFailure,
    KvListReq, KvOut, KvReq, PendingReq, PopDiscoverReq, PopOptions, PopOut, PopPinnedReq, PopReq,
    PushOut, PushReq, RaftHealth, RenewOut, RenewReq, ReqCtx, Rsm, RsmBuildCtx, RsmError,
    TimerPeekReq, TimerReadOut, TimersCountReq, TimersListReq, TimersOut, TimersReq,
};

/// The KV receiver (WP-2.2): 024's pass 1 here, the writes through the planner,
/// the reads off this node's applied state. A child module so it shares the
/// facade's private plumbing (`submit`, the store handle).
mod kv;
mod phase2;

/// The single-node node id of raft1 / embedded (D2). Membership and identity
/// are WP-4.3's; phase 1 is one voter.
const NODE_ID: u64 = 1;

/// The default consumer group of "queue mode" (`handlers/data.rs`): a pop with
/// no `consumerGroup`. The SQL hard-pins it to seed `all`.
const QUEUE_MODE_GROUP: &str = "__QUEUE_MODE__";

// ---------------------------------------------------------------------------
// The waker: apply-thread wakes → the receiver's long-poll notifier (§9.5)
// ---------------------------------------------------------------------------

/// Bridges [`Waker`] (called on the apply thread after an `Append` or a lease
/// release) to the process [`Notifier`] parked pops wait on (§9.5). It replaces
/// the mesh `MESSAGE_AVAILABLE` frame: a wake for `(tenant, queue, group)` wakes
/// every pop parked on that queue's gate on this node.
struct NotifierWaker {
    notifier: Arc<Notifier>,
    gates: Arc<WaitGates>,
}

impl Waker for NotifierWaker {
    fn wake(&self, tenant: &str, queue: &str, group: Option<&str>) {
        // Raft pops park on the per-group gates (P2.2): one wake = one partition
        // became claimable = one parked pop. The queue-wide notifier still fires
        // for any other waiter on this node (it is a no-op when nobody parked).
        self.gates.wake_one(tenant, queue, group);
        let qkey = crate::handlers::tenant_queue_key(tenant, queue);
        self.notifier.wake_local_hint(&qkey, "");
    }
}

/// PLAN_RAFT_DRAIN_FIX P2.2: long-poll gates per `(tenant, queue, group)`.
/// Apply knows exactly which group's partition became claimable, so each wake
/// releases ONE parked pop of that group (`notify_one`) instead of every pop of
/// the queue — the O(parked) re-plan storm (18.6 re-plans per wake, measured)
/// dies. `notify_one` banks a single permit when nobody is parked, which only
/// makes the next parker re-poll once.
#[derive(Default)]
struct WaitGates {
    map: std::sync::RwLock<
        std::collections::HashMap<(String, String, String), Arc<tokio::sync::Notify>>,
    >,
}

impl WaitGates {
    fn gate(&self, key: &(String, String, String)) -> Arc<tokio::sync::Notify> {
        if let Some(g) = self.map.read().expect("gates poisoned").get(key) {
            return g.clone();
        }
        self.map
            .write()
            .expect("gates poisoned")
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
            .clone()
    }

    /// Park until woken or `dur` elapses; true on a wake.
    async fn wait(&self, key: &(String, String, String), dur: Duration) -> bool {
        let gate = self.gate(key);
        let notified = gate.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        tokio::time::timeout(dur, notified).await.is_ok()
    }

    fn wake_one(&self, tenant: &str, queue: &str, group: Option<&str>) {
        let map = self.map.read().expect("gates poisoned");
        match group {
            Some(g) => {
                let key = (tenant.to_string(), queue.to_string(), g.to_string());
                if let Some(gate) = map.get(&key) {
                    gate.notify_one();
                }
            }
            // No group named: one pop of every group of the queue.
            None => {
                for ((t, q, _), gate) in map.iter() {
                    if t == tenant && q == queue {
                        gate.notify_one();
                    }
                }
            }
        }
    }
}

/// P1.1: the least deadline a long-poll RE-poll must still have to be submitted
/// (`QUEEN_RAFT_POP_SUBMIT_MIN_MS`, default 100 ms). Below it the pop answers
/// empty: submitting would claim for a waiter that is about to time out.
static POP_SUBMIT_MIN: std::sync::LazyLock<Duration> = std::sync::LazyLock::new(|| {
    Duration::from_millis(
        std::env::var("QUEEN_RAFT_POP_SUBMIT_MIN_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(100),
    )
});

// ---------------------------------------------------------------------------
// The facade
// ---------------------------------------------------------------------------

/// The real single-node [`Rsm`] through phase 2.
pub struct RaftFacade {
    /// The ordered store (D9), shared with the batcher's planning reads and the
    /// apply thread's writes.
    store: Arc<HeedStore>,
    /// The single-node consensus + apply thread (§12.2). Held for `role`,
    /// `metrics` and `applied_index`; also the sole owner of the apply/writer
    /// threads, joined when the facade drops.
    repl: Arc<LocalReplicator<HeedStore>>,
    /// The segment reader for pop payloads (§7.5), off the live file set.
    reader: segments::Reader,
    /// The per-queue-log reader for pop payloads (Phase A2, `QUEEN_RAFT_QLOG`),
    /// `Some` only when the knob is on. When present the pop render reads the
    /// payload from the queue log instead of the segments; the segments are still
    /// written (removed in A3), so off-vs-on is byte-identical.
    qlog_reader: Option<QLogReader>,
    /// Persistent node-local metrics/history (`<data_dir>/local.db`, D17).
    local_metrics: Arc<crate::rsm::local_metrics::LocalMetrics>,
    /// At-rest payload cipher shared by push, transaction, timers, pop, and
    /// management reads. Queue policy remains replicated in `QueueConfig`;
    /// only key material is node-local configuration.
    encryption: Arc<crate::encryption::Encryption>,
    /// The command channel the batcher drains (§7.1). Bounded; back-pressure
    /// reaches the receiver.
    cmd_tx: CommandTx,
    /// The long-poll notifier (§9.5), shared with the receiver and driven by
    /// [`NotifierWaker`].
    notifier: Arc<Notifier>,
    /// P2.2: the per-group long-poll gates raft pops park on.
    gates: Arc<WaitGates>,
    /// The batcher task handle, kept so [`RaftFacade::shutdown`] can join it.
    batcher_join: tokio::task::JoinHandle<()>,
    /// `QUEEN_RAFT_POP_FASTPATH_EMPTY` (PERF-J, default on): answer a wildcard
    /// pop that is provably empty from committed state WITHOUT submitting a
    /// `PopWildcard` command onto the single serial batcher pipeline. Resolved
    /// once at open.
    pop_fastpath_empty: bool,
    /// Push admission budget ([`crate::rsm::admit`]); `None` when disabled.
    admit: Option<crate::rsm::admit::AdmitGate>,
    data_dir: PathBuf,
    storage_full: std::sync::atomic::AtomicBool,
    storage_pressure_enabled: bool,
    storage_checked_at_us: std::sync::atomic::AtomicI64,
    storage_check_interval_us: i64,
    disk_high_pct: f64,
    disk_low_pct: f64,
}

/// Wall micros for the PERF-J fastpath's `ready_at` comparison. A coarse hint —
/// the pending ring's `ready_at` is in the RSM clock base (monotone wall micros),
/// so a few microseconds of skew only ever makes a borderline deferred partition
/// look not-yet-ready, which self-heals on the next re-poll (§9.5). The
/// drained-partition case this optimises has no pending row at all, so it does
/// not depend on this clock.
fn wall_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// A boolean env knob: only `0`/`false`/`off`/`no` turns it off; unset or any
/// other value keeps the default (matches [`BatcherConfig::from_env`]).
fn env_flag(name: &str, default_on: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            )
        })
        .unwrap_or(default_on)
}

fn env_pct(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| (1.0..=100.0).contains(v))
        .unwrap_or(default)
}

#[cfg(unix)]
fn filesystem_used_pct(path: &std::path::Path) -> Option<f64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `statvfs` initializes `stat` on 0.
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: the successful call above initialized every field.
    let stat = unsafe { stat.assume_init() };
    let total = stat.f_blocks as f64 * stat.f_frsize as f64;
    let available = stat.f_bavail as f64 * stat.f_frsize as f64;
    (total > 0.0).then_some((total - available) * 100.0 / total)
}

#[cfg(not(unix))]
fn filesystem_used_pct(_path: &std::path::Path) -> Option<f64> {
    None
}

impl RaftFacade {
    #[cfg(test)]
    pub(crate) fn set_encryption_for_test(
        &mut self,
        encryption: Arc<crate::encryption::Encryption>,
    ) {
        self.encryption = encryption;
    }

    fn storage_pressure(&self) -> bool {
        use std::sync::atomic::Ordering;
        if !self.storage_pressure_enabled {
            return false;
        }
        let now = wall_micros();
        let checked = self.storage_checked_at_us.load(Ordering::Acquire);
        if now.saturating_sub(checked) < self.storage_check_interval_us {
            return self.storage_full.load(Ordering::Relaxed);
        }
        // One caller refreshes the cached probe; concurrent submissions use
        // the previous safe result instead of issuing a statvfs storm.
        if self
            .storage_checked_at_us
            .compare_exchange(checked, now, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return self.storage_full.load(Ordering::Relaxed);
        }
        let map_pct = self.store.map_usage().pct();
        let disk_pct = filesystem_used_pct(&self.data_dir).unwrap_or(0.0);
        let was_full = self.storage_full.load(Ordering::Relaxed);
        let full = if was_full {
            map_pct >= crate::rsm::store::MAP_LOW_PCT || disk_pct >= self.disk_low_pct
        } else {
            map_pct >= crate::rsm::store::MAP_HIGH_PCT || disk_pct >= self.disk_high_pct
        };
        if full != was_full {
            self.storage_full.store(full, Ordering::Relaxed);
            tracing::warn!(target:"rsm", full, map_pct, disk_pct, "raft storage pressure changed");
        }
        full
    }

    /// Open the whole RSM at `ctx.data_dir` (§11.1). Blocking boot I/O; called
    /// once, from the storage seam (`build_raft_state`) inside the runtime.
    pub fn open(ctx: &RsmBuildCtx) -> Result<RaftFacade, String> {
        let mut batcher = BatcherConfig::from_env();
        if cfg!(test) {
            // Unit tests create many independent facades in parallel. Their
            // subject is command semantics, not the real-node background loop;
            // keep host disk fullness and concurrent local GC out of them.
            batcher.maintenance_every_ms = 0;
        }
        RaftFacade::open_inner(ctx, batcher, !cfg!(test))
    }

    /// [`RaftFacade::open`] with an explicit batcher configuration instead of
    /// the environment's — the tests' seam (a fast timer tick, the injected
    /// fire failure), never the boot path's.
    pub fn open_with(ctx: &RsmBuildCtx, batcher_cfg: BatcherConfig) -> Result<RaftFacade, String> {
        RaftFacade::open_inner(ctx, batcher_cfg, false)
    }

    fn open_inner(
        ctx: &RsmBuildCtx,
        batcher_cfg: BatcherConfig,
        storage_pressure_enabled: bool,
    ) -> Result<RaftFacade, String> {
        let dir = PathBuf::from(&ctx.data_dir);
        if ctx.data_dir.trim().is_empty() {
            return Err("QUEEN_RAFT_DIR is required in raft mode (§11.1)".into());
        }
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("create raft data dir {}: {e}", dir.display()))?;

        let store = Arc::new(
            HeedStore::open(&dir.join("store"), &store_opts_from_env())
                .map_err(|e| format!("open store at {}/store: {e}", dir.display()))?,
        );

        let gates = Arc::new(WaitGates::default());
        let waker: Arc<dyn Waker> = Arc::new(NotifierWaker {
            notifier: ctx.notifier.clone(),
            gates: gates.clone(),
        });
        let repl = Arc::new(
            LocalReplicator::open(
                store.clone(),
                OpenConfig::new(NODE_ID, dir.clone()),
                waker,
                Arc::new(SystemClock),
            )
            .map_err(|e| format!("open the local replicator at {}: {e}", dir.display()))?,
        );
        let reader = repl.reader();
        // Phase A2: the per-queue-log reader (or `None` when `QUEEN_RAFT_QLOG` is
        // off), published by the apply thread alongside the segment reader.
        let qlog_reader = repl.qlog_reader();
        let local_metrics = crate::rsm::local_metrics::open(dir.join("local.db"))
            .map_err(|e| format!("open local metrics at {}/local.db: {e}", dir.display()))?;

        // PERF-E `DEDUP_INDEX=segment`: the planner serves the committed dedup
        // authority from the segments, so hand the batcher a cloned segment
        // reader (the default modes never read it). Phase A2: also hand it the
        // qlog reader, so with the knob on the planner reads that authority from
        // the queue log instead.
        let batcher = Batcher::new(store.clone(), repl.clone(), batcher_cfg)
            .with_reader(reader.clone())
            .with_qlog_reader(qlog_reader.clone());
        let (cmd_tx, batcher_join) = batcher.spawn();

        tracing::info!(
            target: "rsm",
            dir = %dir.display(),
            applied = repl.applied_index(),
            "raft facade open (WP-1.7c)",
        );

        Ok(RaftFacade {
            store,
            repl,
            reader,
            qlog_reader,
            local_metrics,
            encryption: crate::encryption::Encryption::from_env(),
            cmd_tx,
            notifier: ctx.notifier.clone(),
            gates,
            batcher_join,
            pop_fastpath_empty: env_flag("QUEEN_RAFT_POP_FASTPATH_EMPTY", true),
            admit: crate::rsm::admit::AdmitGate::from_env(),
            data_dir: dir,
            storage_full: std::sync::atomic::AtomicBool::new(false),
            storage_pressure_enabled,
            storage_checked_at_us: std::sync::atomic::AtomicI64::new(0),
            storage_check_interval_us: std::env::var("QUEEN_RAFT_DISK_CHECK_MS")
                .ok()
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(100)
                .clamp(10, 60_000)
                .saturating_mul(1_000),
            disk_high_pct: env_pct("QUEEN_RAFT_DISK_HIGH_PCT", 85.0),
            disk_low_pct: env_pct("QUEEN_RAFT_DISK_LOW_PCT", 80.0),
        })
    }

    /// Clean teardown (tests, embedded restart): drop the command channel so the
    /// batcher drains and exits, join it, then join the apply and writer threads
    /// and close the store env, so the SAME data directory can be reopened in
    /// this process (heed refuses two opens of one path). The server never calls
    /// this — its facade lives for the process and the OS reclaims on exit.
    pub async fn shutdown(self) {
        let RaftFacade {
            store,
            repl,
            reader,
            qlog_reader,
            local_metrics: _,
            encryption: _,
            cmd_tx,
            notifier: _,
            gates: _,
            batcher_join,
            pop_fastpath_empty: _,
            admit: _,
            data_dir: _,
            storage_full: _,
            storage_pressure_enabled: _,
            storage_checked_at_us: _,
            storage_check_interval_us: _,
            disk_high_pct: _,
            disk_low_pct: _,
        } = self;
        drop(cmd_tx); // the batcher sees a closed channel, drains, and exits
        let _ = batcher_join.await; // its Arc<repl>/Arc<store> drop here
        drop(reader); // the segment Shared reference the facade held
        drop(qlog_reader); // the qlog Shared reference the facade held

        // A stray reference can outlive the batcher by a moment: a propose
        // forwarding task still owns its `repl` clone after the driver already
        // answered its entry off the applied notify (PERF-G), and a blocking
        // read may still hold a `store` clone. Give them a BOUNDED moment to
        // finish, so a clean shutdown really closes the environment and the
        // SAME directory reopens in this process — under a loaded test run the
        // immediate `try_unwrap` lost that race and the reopen found the store
        // "already open".
        let grace = std::time::Instant::now() + Duration::from_secs(5);
        let mut repl = repl;
        let r = loop {
            match Arc::try_unwrap(repl) {
                Ok(r) => break Some(r),
                Err(still) if std::time::Instant::now() < grace => {
                    repl = still;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Err(still) => {
                    // Still shared: drop what we can and let it wind down on
                    // its own.
                    drop(still);
                    break None;
                }
            }
        };
        let Some(r) = r else {
            drop(store);
            return;
        };
        // Joins the apply and writer threads; returns the sole store Arc.
        match r.shutdown() {
            Ok((_stats, store2)) => {
                drop(store); // the facade's own clone
                let mut store2 = store2;
                loop {
                    match Arc::try_unwrap(store2) {
                        Ok(s) => {
                            s.close();
                            break;
                        }
                        Err(still) if std::time::Instant::now() < grace => {
                            store2 = still;
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(e) => tracing::warn!(target: "rsm", error = %e, "raft facade shutdown"),
        }
    }

    /// Submit one command and await its [`Reply`] under the context deadline
    /// (I15). A closed channel or an elapsed deadline is a retryable failure.
    async fn submit(&self, ctx: &ReqCtx, command: Command) -> Result<Reply, RsmError> {
        if command.grows_storage() && self.storage_pressure() {
            return Err(RsmError::StorageFull);
        }
        // Held until the reply arrives (or the deadline passes): the command's
        // bytes count against the admission budget while it is in the pipeline.
        let _admitted =
            match (&self.admit, command.grows_storage()) {
                (Some(gate), true) => Some(gate.admit(command.size_hint()).await.map_err(|o| {
                    RsmError::Overloaded {
                        retry_after_s: o.retry_after_s,
                    }
                })?),
                _ => None,
            };
        let (sub, rx) = Submission::new(command);
        // The bounded channel absorbs back-pressure; a full channel waits, up to
        // the deadline.
        let send = tokio::time::timeout(ctx.deadline.remaining(), self.cmd_tx.send(sub)).await;
        match send {
            Ok(Ok(())) => {}
            Ok(Err(_closed)) => return Err(RsmError::Internal("planner channel closed".into())),
            Err(_elapsed) => return Err(RsmError::Timeout),
        }
        match tokio::time::timeout(ctx.deadline.remaining(), rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_dropped)) => Err(RsmError::Internal("planner dropped the reply".into())),
            Err(_elapsed) => Err(RsmError::Timeout),
        }
    }

    /// PERF-J: whether a wildcard pop of `(tenant, queue, group)` is provably
    /// empty from committed state, so it need not enter the serial batcher
    /// pipeline (`QUEEN_RAFT_POP_FASTPATH_EMPTY`). The committed read runs on the
    /// blocking pool (I15), like every other store read on the facade's hot
    /// paths. Any error (join or store) is treated as "not provably empty", so
    /// the caller submits and the planner decides — correctness over the
    /// optimisation.
    async fn wildcard_would_be_empty(&self, tenant: &str, queue: &str, group: &str) -> bool {
        let store = self.store.clone();
        let tenant = tenant.to_string();
        let queue = queue.to_string();
        let group = group.to_string();
        let now_us = wall_micros();
        let res = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                crate::rsm::planner::pop::wildcard_pop_provably_empty(
                    r,
                    &tenant,
                    &queue,
                    &group,
                    now_us,
                    crate::rsm::planner::pop::POP_FASTPATH_SCAN_CAP,
                )
            })
        })
        .await;
        matches!(res, Ok(Ok(true)))
    }

    /// Every dead letter this node has filed, decoded off the committed store.
    /// Test-only: the facade exposes no DLQ read endpoint in phase 1 (§9.6), so
    /// the ack-path DLQ tests read the rows directly here.
    #[cfg(test)]
    pub(crate) fn dlq_rows(&self) -> Vec<crate::rsm::store::rows::DlqRow> {
        use crate::rsm::store::{Keyspace, Reads};
        self.store
            .read(|r| {
                let mut out = Vec::new();
                r.scan_raw(Keyspace::Dlq, &[], &[], usize::MAX, &mut |_k, v| {
                    if let Ok(row) = crate::rsm::store::rows::dlq_decode(v) {
                        out.push(row);
                    }
                    true
                })?;
                Ok(out)
            })
            .expect("read dlq rows")
    }
}

// ---------------------------------------------------------------------------
// Reply → RsmError, and the derived per-command request id
// ---------------------------------------------------------------------------

/// Map a non-`Done` [`Reply`] to the typed facade error.
fn reply_error(reply: Reply) -> RsmError {
    match reply {
        Reply::Retry { hint } => RsmError::Retry {
            leader_hint: hint.map(|n| n.to_string()),
        },
        Reply::Refused(r) => {
            if r.retryable {
                RsmError::Retry { leader_hint: None }
            } else {
                RsmError::Rejected {
                    code: r.code,
                    message: r.message,
                }
            }
        }
        Reply::Done { .. } => RsmError::Internal("unexpected Done in reply_error".into()),
    }
}

/// Derive a distinct request id per split command from the receiver's minted
/// one (D6): the base is unique per HTTP request (uuidv7), and XOR-ing the
/// ordinal keeps it unique per group AND reproducible on a forwarding retry of
/// the same request (so the retry hits the same dedup outcome, I6).
fn derived_request_id(base: RequestId, ordinal: u32) -> RequestId {
    let mut id = base;
    let o = ordinal.to_be_bytes();
    for i in 0..4 {
        id[8 + i] ^= o[i];
    }
    id
}

// ---------------------------------------------------------------------------
// The default queue config for implicit creation (003 first contact)
// ---------------------------------------------------------------------------

/// The config an implicitly-created queue gets, from the `queen.queues` DDL
/// defaults (`server/sql/schema.sql`). The planner stamps `created_at_us`; the
/// receiver mints the id. ONE definition, shared with the timer fire's own
/// implicit creation (`planner::timers::implicit_queue_config`), so a queue
/// born from a fired timer is configured exactly like one born from a push.
fn default_queue_config(queue: &str) -> QueueConfig {
    crate::rsm::planner::timers::implicit_queue_config_for(queue)
}

/// The store options, honouring `QUEEN_RAFT_MAP_BYTES` (the boot path is exempt
/// from the I2 clock/env ban that `rsm/store` itself carries).
fn store_opts_from_env() -> StoreOpts {
    let mut o = StoreOpts::default();
    if let Some(v) = std::env::var("QUEEN_RAFT_MAP_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
    {
        o.map_bytes = Some(v);
    }
    o
}

// ---------------------------------------------------------------------------
// Transaction (Phase B)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct TxnBodyIn<'a> {
    #[serde(borrow, default)]
    operations: Option<Vec<TxnOpIn<'a>>>,
    #[serde(default, rename = "requiredLeases")]
    required_leases: Option<Vec<String>>,
    #[serde(borrow, default)]
    kv: Option<Vec<&'a RawValue>>,
    #[serde(borrow, default)]
    timers: Option<Vec<&'a RawValue>>,
}

/// One `operations` element: a push (with `items`, or one item inline) or an ack.
#[derive(Deserialize)]
struct TxnOpIn<'a> {
    #[serde(default, rename = "type")]
    ty: String,
    #[serde(borrow, default)]
    items: Option<Vec<PushItemIn<'a>>>,
    #[serde(borrow, default)]
    queue: Option<std::borrow::Cow<'a, str>>,
    #[serde(borrow, default)]
    partition: Option<std::borrow::Cow<'a, str>>,
    #[serde(borrow, default)]
    payload: Option<&'a RawValue>,
    #[serde(default, rename = "transactionId")]
    transaction_id: Option<String>,
    #[serde(borrow, default, rename = "traceId")]
    trace_id: Option<std::borrow::Cow<'a, str>>,
    #[serde(default, rename = "partitionId")]
    partition_id: Option<String>,
    #[serde(default, rename = "consumerGroup")]
    consumer_group: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default, rename = "leaseId")]
    lease_id: Option<String>,
}

impl RaftFacade {
    /// `POST /api/v1/transaction`: every push and ack of the bundle in ONE
    /// `Transaction` command → ONE entry, all-or-nothing (planner/txn.rs), crash-
    /// atomic across queue logs by Phase C's present-in-all replay. The wire
    /// contract is the SQL wire transaction's: HTTP 200, `success` + a flat
    /// `results` array on commit; `success:false` + `reason` on a rollback.
    async fn txn_impl(&self, ctx: ReqCtx, req: super::TxnReq) -> Result<super::TxnOut, RsmError> {
        let txn_id = uuid_bytes_to_string(&uuidv7_bytes());
        let fail = |reason: &str, err: &str| super::TxnOut {
            body: serde_json::json!({
                "transactionId": txn_id,
                "success": false,
                "reason": reason,
                "error": err,
                "results": [],
            })
            .to_string(),
        };
        let body: TxnBodyIn = match serde_json::from_slice(&req.raw) {
            Ok(b) => b,
            Err(e) => return Ok(fail("bad_request", &format!("bad body: {e}"))),
        };
        // The timers rider: schedules and cancels in one array, as on the
        // POST /timers route (025), validated by the timers port.
        let timer_values: Vec<serde_json::Value> = body
            .timers
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|r| serde_json::from_str(r.get()).unwrap_or(serde_json::Value::Null))
            .collect();
        let mut timer_ops = match crate::rsm::planner::timers::parse_timer_ops(
            &timer_values,
            ctx.producer_sub.as_deref(),
        ) {
            Ok(o) => o,
            Err(e) => return Ok(fail("bad_request", &e.message)),
        };
        // The KV rider, validated with the WIRE's limits (024: fewer ops, no
        // getPrefix inside a transaction).
        let kv_values: Vec<serde_json::Value> = body
            .kv
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|r| serde_json::from_str(r.get()).unwrap_or(serde_json::Value::Null))
            .collect();
        let kv_ops = match crate::rsm::planner::kv::parse_ops(
            &kv_values,
            &ctx.tenant,
            true,
            self.store.max_key_len(),
        ) {
            Ok(o) => o,
            Err(e) => return Ok(fail(e.reason, &e.detail)),
        };
        let ops = body.operations.unwrap_or_default();
        if ops.is_empty() && kv_ops.is_empty() && timer_ops.is_empty() {
            return Ok(fail(
                "bad_request",
                "transaction requires an operations array (or a top-level kv/timers array)",
            ));
        }
        let mut queue_names = std::collections::BTreeSet::new();
        for op in &ops {
            if let Some(queue) = op.queue.as_deref() {
                queue_names.insert(queue.to_string());
            }
            for item in op.items.as_deref().unwrap_or_default() {
                queue_names.insert(item.queue.as_ref().to_string());
            }
        }
        for op in &timer_ops {
            queue_names.insert(op.queue().to_string());
        }
        let encrypted_queues = self.encrypted_queues(&ctx.tenant, queue_names).await?;
        if let Err(error) = self.encrypt_timer_ops(&mut timer_ops, &encrypted_queues) {
            return Ok(fail("bad_request", &error));
        }
        let mut hints: Vec<String> = body
            .required_leases
            .unwrap_or_default()
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect();

        // 1. Flatten. Pushes resolve exactly like `push_impl` (minted id, txn,
        //    default partition, intra-bundle same-txn collapse); acks are kept
        //    per consumer group for target resolution.
        struct TxnPush {
            flat: usize,
            message_id: String,
            txn: String,
            queue: String,
            follower_of: Option<usize>, // index into `pushes`
            hash: [u8; 16],
            frame: Vec<u8>,
        }
        let mut pushes: Vec<TxnPush> = Vec::new();
        let mut seen: std::collections::HashMap<(String, String, String), usize> =
            std::collections::HashMap::new();
        let mut groups: indexed_groups::Groups = indexed_groups::Groups::new();
        // consumer group → its ack flats
        let mut acks_by_group: std::collections::BTreeMap<String, Vec<AckFlat>> =
            std::collections::BTreeMap::new();
        let mut ack_txn: Vec<(usize, String, AckStatus)> = Vec::new();
        let mut flat = 0usize;

        let mut add_push = |queue: &str,
                            partition: Option<&str>,
                            payload: &RawValue,
                            txn_in: Option<&str>,
                            trace_id: Option<&str>,
                            flat: usize,
                            pushes: &mut Vec<TxnPush>|
         -> Result<(), RsmError> {
            let mid = uuidv7_bytes();
            let mid_str = uuid_bytes_to_string(&mid);
            let txn = txn_in
                .map(str::to_string)
                .unwrap_or_else(|| mid_str.clone());
            let partition = partition
                .filter(|p| !p.is_empty())
                .unwrap_or("Default")
                .to_string();
            super::check_message_key_names(&ctx.tenant, queue, None, Some(&partition))?;
            let key = (queue.to_string(), partition.clone(), txn.clone());
            let follower_of = seen.get(&key).copied();
            let idx = pushes.len();
            let frame = if follower_of.is_none() {
                seen.insert(key, idx);
                groups.push(queue, &partition, idx);
                let (payload, encrypted) = self.encode_payload(
                    encrypted_queues.contains(queue),
                    payload.get().as_bytes(),
                    queue,
                );
                pack_frames(&[FrameIn {
                    message_id: mid,
                    txn: &txn,
                    // Invalid trace ids are deliberately ignored, matching the
                    // permissive transaction wire contract.
                    trace_id: trace_id.and_then(uuid_string_to_bytes),
                    producer_sub: ctx.producer_sub.as_deref(),
                    payload: &payload,
                    encrypted,
                }])
            } else {
                Vec::new()
            };
            pushes.push(TxnPush {
                flat,
                message_id: mid_str,
                hash: txn_hash128(&txn),
                txn,
                queue: queue.to_string(),
                follower_of,
                frame,
            });
            Ok(())
        };

        for op in &ops {
            match op.ty.as_str() {
                "push" => {
                    if let Some(items) = &op.items {
                        for it in items {
                            add_push(
                                it.queue.as_ref(),
                                it.partition.as_deref(),
                                it.payload,
                                it.transaction_id.as_deref(),
                                it.trace_id.as_deref(),
                                flat,
                                &mut pushes,
                            )?;
                            flat += 1;
                        }
                    } else {
                        let (Some(q), Some(pl)) = (op.queue.as_deref(), op.payload) else {
                            return Ok(fail(
                                "bad_request",
                                "a push operation needs queue and payload",
                            ));
                        };
                        add_push(
                            q,
                            op.partition.as_deref(),
                            pl,
                            op.transaction_id.as_deref(),
                            op.trace_id.as_deref(),
                            flat,
                            &mut pushes,
                        )?;
                        flat += 1;
                    }
                }
                "ack" => {
                    let txn = op.transaction_id.clone().unwrap_or_default();
                    let group = op
                        .consumer_group
                        .clone()
                        .filter(|g| !g.is_empty())
                        .unwrap_or_else(|| QUEUE_MODE_GROUP.to_string());
                    let status = ack_status_of(op.status.as_deref());
                    let lease = op.lease_id.clone().filter(|l| !l.is_empty());
                    if let Some(l) = &lease {
                        hints.push(l.clone());
                    }
                    let pid = match op.partition_id.as_deref().unwrap_or("").parse::<u64>() {
                        Ok(p) => p,
                        Err(_) => {
                            return Ok(fail("bad_request", "partitionId is not a partition id"))
                        }
                    };
                    acks_by_group.entry(group).or_default().push(AckFlat {
                        index: flat,
                        txn: txn.clone(),
                        pid,
                        worker: lease.unwrap_or_default(),
                        status,
                        error: op.error.clone(),
                    });
                    ack_txn.push((flat, txn, status));
                    flat += 1;
                }
                "kv" | "timer" | "timers" => {
                    return Ok(fail(
                        "bad_request",
                        "kv and timer operations are TOP-LEVEL arrays of the request \
                         (\"kv\":[...], \"timers\":[...]), never elements of `operations`",
                    ))
                }
                "" => {
                    return Ok(fail(
                        "bad_request",
                        "every transaction operation needs a `type` of push or ack",
                    ))
                }
                other => {
                    return Ok(fail(
                        "bad_request",
                        &format!(
                            "transaction supports only push and ack operations, got `{other}`"
                        ),
                    ))
                }
            }
        }

        // The riders take the flat ordinals after the operations (the SQL wire's
        // layout: operations keep their indices, the riders append).
        let kv_base = flat;
        flat += kv_ops.len();
        let timers_base = flat;
        flat += timer_ops.len();

        // The single unambiguous lease hint is every lease-less ack's worker
        // (the JS/Go builders put the pop's leaseId in `requiredLeases`).
        let unique_hint: Option<String> = {
            let mut it = hints.iter();
            match it.next() {
                Some(first) if it.all(|h| h == first) => Some(first.clone()),
                _ => None,
            }
        };

        // 2. The command: one PushCommand per (queue, partition), one ack
        //    target per (pid, group, worker).
        let mut push_cmds: Vec<PushCommand> = Vec::with_capacity(groups.len());
        let mut push_members: Vec<Vec<usize>> = Vec::with_capacity(groups.len());
        for (ordinal, g) in groups.iter().enumerate() {
            push_cmds.push(PushCommand {
                request_id: derived_request_id(ctx.request_id, ordinal as u32),
                tenant: ctx.tenant.clone(),
                queue: g.queue.clone(),
                partition: g.partition.clone(),
                items: g
                    .members
                    .iter()
                    .map(|&i| PushItem {
                        hash: pushes[i].hash,
                        frame: pushes[i].frame.clone(),
                    })
                    .collect(),
                create_cfg: default_queue_config(&g.queue),
            });
            push_members.push(g.members.clone());
        }
        let mut targets: Vec<AckTarget> = Vec::new();
        for (group, mut flats) in acks_by_group {
            if let Some(h) = &unique_hint {
                for f in flats.iter_mut().filter(|f| f.worker.is_empty()) {
                    f.worker = h.clone();
                }
            }
            let store = self.store.clone();
            let reader = self.reader.clone();
            let qlog_reader = self.qlog_reader.clone();
            let encryption = self.encryption.clone();
            let tenant = ctx.tenant.clone();
            let resolved = tokio::task::spawn_blocking(move || {
                resolve_ack_targets(
                    &store,
                    &reader,
                    qlog_reader.as_ref(),
                    &encryption,
                    &tenant,
                    &group,
                    flats,
                )
            })
            .await
            .map_err(|e| RsmError::Internal(format!("txn ack resolve: {e}")))?;
            let (t, _per_item, bad) = resolved.map_err(RsmError::Internal)?;
            if let Some((_, why)) = bad.first() {
                return Ok(fail(
                    "rejected_ack",
                    &format!("QTXN {why}; the transaction rolled back"),
                ));
            }
            targets.extend(t);
        }

        let cmd = Command::Transaction(crate::rsm::batcher::TxnCommand {
            request_id: ctx.request_id,
            tenant: ctx.tenant.clone(),
            pushes: push_cmds,
            acks: targets,
            positional_acks: Vec::new(),
            kv: kv_ops.clone(),
            timers: timer_ops,
            extra_effects: Vec::new(),
            allow_duplicate: false,
        });
        let out = match self.submit(&ctx, cmd).await? {
            Reply::Done { outcome, .. } => crate::rsm::batcher::TxnOutcome::from_outcome(&outcome)
                .ok_or_else(|| {
                    RsmError::Internal("transaction got a non-transaction outcome".into())
                })?,
            Reply::Refused(r) => return Ok(fail(&r.code, &r.message)),
            other => return Err(reply_error(other)),
        };

        // A lost `required` KV precondition rolled the whole bundle back: 024's
        // precondition body (failedIndex in the FLAT space, kvReason, version,
        // value), HTTP 200.
        if let Some(f) = &out.kv.failed {
            let detail = crate::rsm::planner::kv::precondition_detail(&kv_ops, f);
            let mut body = serde_json::json!({
                "transactionId": txn_id,
                "success": false,
                "reason": "kv_precondition",
                "error": "QKV a required KV precondition failed; the transaction rolled back",
                "results": [],
                "ok": false,
            });
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&detail) {
                body["failedIndex"] = v
                    .get("index")
                    .and_then(|x| x.as_u64())
                    .map(|n| serde_json::Value::from(kv_base + n as usize))
                    .unwrap_or(serde_json::Value::Null);
                body["kvReason"] = v.get("reason").cloned().unwrap_or(serde_json::Value::Null);
                body["version"] = v.get("version").cloned().unwrap_or(serde_json::Value::Null);
                body["value"] = v.get("value").cloned().unwrap_or(serde_json::Value::Null);
            }
            return Ok(super::TxnOut {
                body: body.to_string(),
            });
        }

        // 3. Render: one result per flat ordinal, the SQL wire's shapes.
        let mut results: Vec<serde_json::Value> = vec![serde_json::Value::Null; flat];
        if !kv_ops.is_empty() {
            // Deferred reads are evaluated NOW, after the entry applied (024 §6.4).
            let store = self.store.clone();
            let tenant = ctx.tenant.clone();
            let ops2 = kv_ops.clone();
            let pre = out.kv.results.clone();
            let now = wall_micros();
            let vals = tokio::task::spawn_blocking(move || {
                store.read(|r| crate::rsm::planner::kv::render_call(r, &tenant, &ops2, &pre, now))
            })
            .await
            .map_err(|e| RsmError::Internal(format!("txn kv render: {e}")))?
            .map_err(|e| RsmError::Internal(format!("txn kv render: {e}")))?;
            for (i, v) in vals.into_iter().enumerate() {
                let mut obj = match v {
                    serde_json::Value::Object(m) => m,
                    other => {
                        let mut m = serde_json::Map::new();
                        m.insert("result".to_string(), other);
                        m
                    }
                };
                obj.insert("opIndex".to_string(), serde_json::Value::from(i));
                obj.insert("index".to_string(), serde_json::Value::from(kv_base + i));
                obj.insert("type".to_string(), serde_json::Value::String("kv".into()));
                results[kv_base + i] = serde_json::Value::Object(obj);
            }
        }
        let mut verdict_mid: Vec<Option<String>> = vec![None; pushes.len()];
        for (g, members) in push_members.iter().enumerate() {
            for (k, &i) in members.iter().enumerate() {
                let created = matches!(
                    out.pushes.get(g).and_then(|o| o.items.get(k)),
                    Some(PushVerdict::Created { .. })
                );
                verdict_mid[i] = Some(pushes[i].message_id.clone());
                results[pushes[i].flat] = serde_json::json!({
                    "index": pushes[i].flat,
                    "type": "push",
                    "success": created,
                    "transactionId": pushes[i].txn,
                    "messageId": pushes[i].message_id,
                    "queueName": pushes[i].queue,
                });
            }
        }
        for p in &pushes {
            if let Some(leader) = p.follower_of {
                results[p.flat] = serde_json::json!({
                    "index": p.flat,
                    "type": "push",
                    "success": true,
                    "transactionId": p.txn,
                    "messageId": verdict_mid[leader].clone().unwrap_or_default(),
                    "queueName": p.queue,
                    "duplicate": true,
                });
            }
        }
        for (i, v) in out.timers.iter().enumerate() {
            let mut obj = match v.clone() {
                serde_json::Value::Object(m) => m,
                other => {
                    let mut m = serde_json::Map::new();
                    m.insert("result".to_string(), other);
                    m
                }
            };
            obj.insert("opIndex".to_string(), serde_json::Value::from(i));
            obj.insert(
                "index".to_string(),
                serde_json::Value::from(timers_base + i),
            );
            obj.insert(
                "type".to_string(),
                serde_json::Value::String("timer".into()),
            );
            if timers_base + i < results.len() {
                results[timers_base + i] = serde_json::Value::Object(obj);
            }
        }
        for (i, txn, status) in ack_txn {
            results[i] = serde_json::json!({
                "index": i,
                "type": "ack",
                "success": true,
                "transactionId": txn,
                "error": serde_json::Value::Null,
                "dlq": matches!(status, AckStatus::Dlq),
            });
        }
        Ok(super::TxnOut {
            body: serde_json::json!({
                "transactionId": txn_id,
                "success": true,
                "results": results,
            })
            .to_string(),
        })
    }
}

// ---------------------------------------------------------------------------
// Push
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct PushBodyIn<'a> {
    #[serde(borrow)]
    items: Vec<PushItemIn<'a>>,
}

#[derive(Deserialize)]
struct PushItemIn<'a> {
    #[serde(borrow)]
    queue: std::borrow::Cow<'a, str>,
    #[serde(borrow, default)]
    partition: Option<std::borrow::Cow<'a, str>>,
    #[serde(borrow)]
    payload: &'a RawValue,
    #[serde(borrow, default, rename = "transactionId")]
    transaction_id: Option<std::borrow::Cow<'a, str>>,
    #[serde(borrow, default, rename = "traceId")]
    trace_id: Option<std::borrow::Cow<'a, str>>,
}

/// One input item, receiver-resolved: its original index, minted id, txn, queue,
/// partition, and the packed frame + dedup hash. A follower (an intra-request
/// same-txn duplicate) carries no frame and points at its leader.
struct PushResolved {
    message_id: String,
    txn: String,
    queue: String,
    partition: String,
    /// `None` for a survivor; `Some(leader index into the flat results)` for an
    /// intra-request follower (postgres `resolve_push_followers`).
    follower_of: Option<usize>,
    hash: [u8; 16],
    frame: Vec<u8>,
}

/// A per-item rendered verdict, in input order.
#[derive(Clone)]
struct PushItemOut {
    message_id: String,
    txn: String,
    queue: String,
    status: &'static str,
    offset: Option<u64>,
}

impl RaftFacade {
    /// Resolve the replicated queue encryption policy without touching the
    /// legacy Postgres pool. The read is kept off the async runtime because an
    /// LMDB page fault is blocking I/O (I15).
    async fn encrypted_queues(
        &self,
        tenant: &str,
        queues: std::collections::BTreeSet<String>,
    ) -> Result<std::collections::BTreeSet<String>, RsmError> {
        if queues.is_empty() || !self.encryption.is_enabled() {
            return Ok(std::collections::BTreeSet::new());
        }
        let store = self.store.clone();
        let tenant = tenant.to_string();
        tokio::task::spawn_blocking(move || {
            store.read(|r| {
                let mut enabled = std::collections::BTreeSet::new();
                for queue in queues {
                    if r.queue(&tenant, &queue)?
                        .is_some_and(|cfg| cfg.encryption_enabled)
                    {
                        enabled.insert(queue);
                    }
                }
                Ok(enabled)
            })
        })
        .await
        .map_err(|e| RsmError::Internal(format!("queue encryption read task: {e}")))?
        .map_err(|e| {
            if e.retryable() {
                RsmError::Retry { leader_hint: None }
            } else {
                RsmError::Internal(format!("queue encryption read: {e}"))
            }
        })
    }

    /// Encrypt one JSON payload when the replicated queue policy requires it.
    /// Crypto failure follows the established Queen contract: log and store
    /// plaintext rather than acknowledging a message that was not stored.
    fn encode_payload(&self, encrypt: bool, raw: &[u8], queue: &str) -> (Vec<u8>, bool) {
        if encrypt {
            if let Some(payload) = self.encryption.encrypt(raw) {
                return (payload, true);
            }
            tracing::warn!(target: "push", queue, "encryption failed; stored plaintext");
        }
        (raw.to_vec(), false)
    }

    /// Apply the same queue-at-rest policy to scheduled messages. The timer
    /// row stores a fully packed frame, so encryption happens before planning
    /// and the eventual fire remains deterministic.
    fn encrypt_timer_ops(
        &self,
        ops: &mut [TimerOp],
        encrypted_queues: &std::collections::BTreeSet<String>,
    ) -> Result<(), String> {
        for op in ops {
            let TimerOp::Schedule(schedule) = op else {
                continue;
            };
            if !encrypted_queues.contains(&schedule.queue) {
                continue;
            }
            if schedule.encrypted {
                return Err(format!(
                    "queue `{}` encrypts at rest, so `encrypted` is set by the broker and must not be supplied",
                    schedule.queue
                ));
            }
            let packed = {
                let frames = unpack_frames_ref(&schedule.frame)
                    .ok_or_else(|| "the prepared timer frame is malformed".to_string())?;
                let frame = frames
                    .first()
                    .ok_or_else(|| "the prepared timer frame is empty".to_string())?;
                let (payload, encrypted) =
                    self.encode_payload(true, frame.payload, &schedule.queue);
                schedule.encrypted = encrypted;
                pack_frames(&[FrameIn {
                    message_id: frame.message_id,
                    txn: frame.txn,
                    trace_id: frame.trace_id,
                    producer_sub: frame.producer_sub,
                    payload: &payload,
                    encrypted,
                }])
            };
            schedule.frame = packed;
        }
        Ok(())
    }

    async fn push_impl(&self, ctx: ReqCtx, req: PushReq) -> Result<PushOut, RsmError> {
        // PERF-J: the push-only HTTP-boundary split. `_t_prep` covers the
        // pre-submit work (parse + resolve + pack); `submit_ns` accumulates the
        // `cmd_tx.send` await (channel back-pressure, which `arrival_to_proposed`
        // cannot see because it stamps just before the send).
        let _t_prep = crate::rsm::timing::stamp();
        let mut _submit_ns: u64 = 0;
        let body: PushBodyIn =
            serde_json::from_slice(&req.raw).map_err(|e| RsmError::Rejected {
                code: "bad_body".into(),
                message: format!("bad push body: {e}"),
            })?;
        if body.items.is_empty() {
            return Ok(PushOut { body: "[]".into() });
        }
        if self.storage_pressure() {
            return Err(RsmError::StorageFull);
        }
        let encrypted_queues = self
            .encrypted_queues(
                &ctx.tenant,
                body.items
                    .iter()
                    .map(|item| item.queue.as_ref().to_string())
                    .collect(),
            )
            .await?;

        // 1. Resolve every input item and collapse intra-request duplicates by
        //    (queue, partition, txn), so the planner is never handed two frames
        //    with the same hash in one command (its probe folds only what came
        //    before, so it would let both survive). The leader is the first
        //    occurrence; a follower inherits the leader's verdict at render.
        let mut resolved: Vec<PushResolved> = Vec::with_capacity(body.items.len());
        // (queue, partition, txn) → the flat index of its leader.
        let mut seen: std::collections::HashMap<(String, String, String), usize> =
            std::collections::HashMap::new();
        // (queue, partition) → the survivors' flat indices, in order.
        let mut groups: indexed_groups::Groups = indexed_groups::Groups::new();

        for it in &body.items {
            let mid = uuidv7_bytes();
            let mid_str = uuid_bytes_to_string(&mid);
            let txn = it
                .transaction_id
                .as_deref()
                .map(str::to_string)
                .unwrap_or_else(|| mid_str.clone());
            let queue = it.queue.as_ref().to_string();
            let partition = it
                .partition
                .as_deref()
                .filter(|p| !p.is_empty())
                .unwrap_or("Default")
                .to_string();

            super::check_message_key_names(&ctx.tenant, &queue, None, Some(&partition))?;

            let flat = resolved.len();
            let key = (queue.clone(), partition.clone(), txn.clone());
            let follower_of = match seen.get(&key) {
                Some(&leader) => Some(leader),
                None => {
                    seen.insert(key, flat);
                    None
                }
            };
            let hash = txn_hash128(&txn);
            let frame = if follower_of.is_none() {
                let (payload, encrypted) = self.encode_payload(
                    encrypted_queues.contains(&queue),
                    it.payload.get().as_bytes(),
                    &queue,
                );
                pack_frames(&[FrameIn {
                    message_id: mid,
                    txn: &txn,
                    trace_id: None,
                    producer_sub: ctx.producer_sub.as_deref(),
                    payload: &payload,
                    encrypted,
                }])
            } else {
                Vec::new()
            };
            if follower_of.is_none() {
                groups.push(&queue, &partition, flat);
            }
            resolved.push(PushResolved {
                message_id: mid_str,
                txn,
                queue,
                partition,
                follower_of,
                hash,
                frame,
            });
        }

        // 2. One PushCommand per (queue, partition) group, its items the group's
        //    survivors in order. Submit them all, then await every reply.
        // PERF-J: record the pre-submit leg now (parse + resolve + pack done).
        if let Some(t) = _t_prep {
            crate::rsm::timing::metrics()
                .push_h_prep
                .record_dur(t.elapsed());
        }
        let mut rxs = Vec::with_capacity(groups.len());
        for (ordinal, g) in groups.iter().enumerate() {
            let items: Vec<PushItem> = g
                .members
                .iter()
                .map(|&flat| PushItem {
                    hash: resolved[flat].hash,
                    frame: resolved[flat].frame.clone(),
                })
                .collect();
            let cmd = Command::Push(PushCommand {
                request_id: derived_request_id(ctx.request_id, ordinal as u32),
                tenant: ctx.tenant.clone(),
                queue: g.queue.clone(),
                partition: g.partition.clone(),
                items,
                create_cfg: default_queue_config(&g.queue),
            });
            let (sub, rx) = Submission::new(cmd);
            let _t_send = crate::rsm::timing::stamp();
            let sent = tokio::time::timeout(ctx.deadline.remaining(), self.cmd_tx.send(sub)).await;
            if let Some(t) = _t_send {
                _submit_ns = _submit_ns.saturating_add(t.elapsed().as_nanos() as u64);
            }
            match sent {
                Ok(Ok(())) => rxs.push((g.members.clone(), rx)),
                Ok(Err(_)) => return Err(RsmError::Internal("planner channel closed".into())),
                Err(_) => return Err(RsmError::Timeout),
            }
        }
        // PERF-J: the accumulated channel-enqueue wait (all groups), and open
        // the reply-wait leg (propose+commit+apply+answer), push-only.
        let _t_await = crate::rsm::timing::stamp();
        if _t_await.is_some() {
            crate::rsm::timing::metrics()
                .push_h_submit
                .record(_submit_ns);
        }

        // 3. Collect. A whole-group Retry fails the whole push (the SDK retries
        //    with the same derived ids, deduped by request id); a non-retryable
        //    Rejected marks that group's items "error" (a push answers 201 with
        //    per-item statuses).
        let mut out: Vec<Option<PushItemOut>> = vec![None; resolved.len()];
        let mut duplicate_ids: Vec<(usize, Pid, u64)> = Vec::new();
        for (members, rx) in rxs {
            let reply = match tokio::time::timeout(ctx.deadline.remaining(), rx).await {
                Ok(Ok(r)) => r,
                Ok(Err(_)) => return Err(RsmError::Internal("planner dropped the reply".into())),
                Err(_) => return Err(RsmError::Timeout),
            };
            match reply {
                Reply::Done { outcome, .. } => {
                    let verdicts = match outcome {
                        Outcome::Push(p) => p.items,
                        other => {
                            return Err(RsmError::Internal(format!(
                                "push got a non-push outcome: {other:?}"
                            )))
                        }
                    };
                    for (k, &flat) in members.iter().enumerate() {
                        let r = &resolved[flat];
                        let (status, offset) = match verdicts.get(k) {
                            Some(PushVerdict::Created { offset, .. }) => ("queued", Some(*offset)),
                            Some(PushVerdict::Duplicate { pid, offset }) => {
                                duplicate_ids.push((flat, *pid, *offset));
                                ("duplicate", Some(*offset))
                            }
                            Some(PushVerdict::Refused { .. }) | None => ("error", None),
                        };
                        out[flat] = Some(PushItemOut {
                            message_id: r.message_id.clone(),
                            txn: r.txn.clone(),
                            queue: r.queue.clone(),
                            status,
                            offset,
                        });
                    }
                }
                Reply::Retry { hint } => {
                    return Err(RsmError::Retry {
                        leader_hint: hint.map(|n| n.to_string()),
                    })
                }
                Reply::Refused(refusal) if refusal.retryable => {
                    return Err(RsmError::Retry { leader_hint: None })
                }
                Reply::Refused(_) => {
                    for &flat in &members {
                        let r = &resolved[flat];
                        out[flat] = Some(PushItemOut {
                            message_id: r.message_id.clone(),
                            txn: r.txn.clone(),
                            queue: r.queue.clone(),
                            status: "error",
                            offset: None,
                        });
                    }
                }
            }
        }

        // PERF-J: every group's reply is in — close the reply-wait leg.
        if let Some(t) = _t_await {
            crate::rsm::timing::metrics()
                .push_h_await
                .record_dur(t.elapsed());
        }

        // A duplicate returns the ORIGINAL message id, not the id minted for
        // this rejected attempt. The replicated verdict carries its original
        // offset; resolve the immutable frame locally after apply (D7).
        if !duplicate_ids.is_empty() {
            let store = self.store.clone();
            let reader = self.reader.clone();
            let qlog = self.qlog_reader.clone();
            let tenant = ctx.tenant.clone();
            let lookup: Vec<_> = duplicate_ids
                .iter()
                .map(|(flat, pid, offset)| {
                    (
                        *flat,
                        *pid,
                        *offset,
                        resolved[*flat].queue.clone(),
                        resolved[*flat].partition.clone(),
                    )
                })
                .collect();
            let ids = tokio::task::spawn_blocking(move || {
                resolve_duplicate_message_ids(&store, &reader, qlog.as_ref(), &tenant, &lookup)
            })
            .await
            .map_err(|e| RsmError::Internal(format!("duplicate id task: {e}")))??;
            for (flat, id) in ids {
                if let Some(item) = &mut out[flat] {
                    item.message_id = id;
                }
            }
        }

        // 4. Followers inherit the leader's id, status ("duplicate") and offset
        //    (C1). A follower whose leader errored is an error too.
        for i in 0..resolved.len() {
            if let Some(leader) = resolved[i].follower_of {
                let lead = out[leader].clone();
                let r = &resolved[i];
                out[i] = Some(match lead {
                    Some(l) if l.status == "error" => PushItemOut {
                        message_id: l.message_id,
                        txn: r.txn.clone(),
                        queue: r.queue.clone(),
                        status: "error",
                        offset: None,
                    },
                    Some(l) => PushItemOut {
                        message_id: l.message_id,
                        txn: r.txn.clone(),
                        queue: r.queue.clone(),
                        status: "duplicate",
                        offset: l.offset,
                    },
                    None => PushItemOut {
                        message_id: r.message_id.clone(),
                        txn: r.txn.clone(),
                        queue: r.queue.clone(),
                        status: "error",
                        offset: None,
                    },
                });
            }
        }

        Ok(PushOut {
            body: render_push(&out),
        })
    }
}

type DuplicateIdLookup = (usize, Pid, u64, String, String);

fn resolve_duplicate_message_ids(
    store: &HeedStore,
    reader: &segments::Reader,
    qlog: Option<&QLogReader>,
    tenant: &str,
    lookups: &[DuplicateIdLookup],
) -> Result<Vec<(usize, String)>, RsmError> {
    let mut out = Vec::with_capacity(lookups.len());
    for (flat, pid, offset, queue, partition) in lookups {
        let blob = match qlog {
            Some(qlog) => qlog
                .read_owned(QLogReader::queue_id_of(tenant, queue), *pid, *offset)
                .map_err(|e| RsmError::Internal(format!("duplicate qlog read: {e}")))?
                .map(|r| (r.base_offset, r.payload)),
            None => {
                let sealed = store
                    .read(|r| {
                        let mut files = Vec::new();
                        r.scan_partition_files(*pid, usize::MAX, &mut |file| {
                            files.push(file);
                            true
                        })?;
                        Ok(files)
                    })
                    .map_err(|e| RsmError::Internal(format!("duplicate file read: {e}")))?;
                reader
                    .read_at(bucket_of(tenant, queue, partition), *pid, *offset, &sealed)
                    .map_err(|e| RsmError::Internal(format!("duplicate segment read: {e}")))?
                    .map(|r| (r.base_offset, r.blob))
            }
        };
        let Some((base, blob)) = blob else {
            return Err(RsmError::Internal(format!(
                "duplicate payload is missing at pid {pid} offset {offset}"
            )));
        };
        let frames = unpack_frames_ref(&blob).ok_or_else(|| {
            RsmError::Internal(format!(
                "duplicate payload is corrupt at pid {pid} offset {offset}"
            ))
        })?;
        let frame = frames
            .get(offset.saturating_sub(base) as usize)
            .ok_or_else(|| {
                RsmError::Internal(format!(
                    "duplicate offset {offset} is outside its record at pid {pid}"
                ))
            })?;
        out.push((*flat, uuid_bytes_to_string(&frame.message_id)));
    }
    Ok(out)
}

/// `[{index, message_id, transaction_id, queueName, status, offset?}]`, input
/// order (`handlers/data.rs::render_push_results`).
fn render_push(items: &[Option<PushItemOut>]) -> String {
    let mut out = String::with_capacity(items.len() * 176 + 2);
    out.push('[');
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let it = item.as_ref();
        out.push_str("{\"index\":");
        out.push_str(&i.to_string());
        out.push_str(",\"message_id\":\"");
        if let Some(it) = it {
            out.push_str(&it.message_id);
        }
        out.push_str("\",\"transaction_id\":\"");
        if let Some(it) = it {
            crate::fusion::json_escape_into(&mut out, &it.txn);
        }
        out.push_str("\",\"queueName\":\"");
        if let Some(it) = it {
            crate::fusion::json_escape_into(&mut out, &it.queue);
        }
        out.push_str("\",\"status\":\"");
        out.push_str(it.map(|i| i.status).unwrap_or("error"));
        out.push('"');
        if let Some(off) = it.and_then(|i| i.offset) {
            out.push_str(",\"offset\":");
            out.push_str(&off.to_string());
        }
        out.push('}');
    }
    out.push(']');
    out
}

/// Ordered grouping of push survivors by (queue, partition), first-seen order.
#[allow(clippy::new_without_default, clippy::len_without_is_empty)]
mod indexed_groups {
    pub struct Group {
        pub queue: String,
        pub partition: String,
        pub members: Vec<usize>,
    }
    pub struct Groups {
        groups: Vec<Group>,
        index: std::collections::HashMap<(String, String), usize>,
    }
    impl Groups {
        pub fn new() -> Groups {
            Groups {
                groups: Vec::new(),
                index: std::collections::HashMap::new(),
            }
        }
        pub fn push(&mut self, queue: &str, partition: &str, flat: usize) {
            let key = (queue.to_string(), partition.to_string());
            let idx = *self.index.entry(key).or_insert_with(|| {
                self.groups.push(Group {
                    queue: queue.to_string(),
                    partition: partition.to_string(),
                    members: Vec::new(),
                });
                self.groups.len() - 1
            });
            self.groups[idx].members.push(flat);
        }
        pub fn len(&self) -> usize {
            self.groups.len()
        }
        pub fn iter(&self) -> std::slice::Iter<'_, Group> {
            self.groups.iter()
        }
    }
}

// ---------------------------------------------------------------------------
// Pop
// ---------------------------------------------------------------------------

impl RaftFacade {
    /// The shared pop driver: build the [`PopCommand`], run it (with a bounded
    /// long-poll re-poll on an empty claim when `wait`, §9.5), and render.
    #[allow(clippy::too_many_arguments)]
    async fn pop_run(
        &self,
        ctx: &ReqCtx,
        queue: String,
        partition: Option<String>,
        namespace: String,
        task: String,
        group_opt: Option<String>,
        batch: u32,
        auto_ack: bool,
        wait: bool,
        wildcard_create: bool,
        options: PopOptions,
    ) -> Result<PopOut, RsmError> {
        let group = group_opt.unwrap_or_else(|| QUEUE_MODE_GROUP.to_string());
        // Queue mode always seeds `all`. Named groups carry the receiver's
        // normalized subscription intent and persist it on first contact.
        let sub = if group == QUEUE_MODE_GROUP {
            SubIntent::default() // mode "" → seed at the floor (all)
        } else {
            SubIntent {
                mode: options.subscription_mode.clone(),
                from_us: options.subscription_from_us,
                now: options.subscription_from_now,
            }
        };
        let lease_seconds = if options.lease_seconds > 0 {
            options.lease_seconds
        } else if queue.is_empty() {
            // Discovery can span queues. Its Postgres implementation resolves
            // each queue independently; the current command has one field, so
            // use the implicit-queue default unless the caller overrides it.
            60
        } else {
            let store = self.store.clone();
            let tenant = ctx.tenant.clone();
            let queue = queue.clone();
            tokio::task::spawn_blocking(move || {
                store.read(|r| Ok(r.queue(&tenant, &queue)?.map(|q| q.lease_time)))
            })
            .await
            .map_err(|e| RsmError::Internal(format!("pop queue config task: {e}")))?
            .map_err(|e| RsmError::Internal(format!("pop queue config: {e}")))?
            .unwrap_or(60)
            .max(1)
        };
        let worker = uuid_bytes_to_string(&uuidv7_bytes());
        let budget = batch.min(i32::MAX as u32) as i32;
        let gate_key = (ctx.tenant.clone(), queue.clone(), group.clone());
        let mut attempt: u32 = 0;
        // P2.2: set when the last park ended on a WAKE. Wakes fire right after
        // apply, before the store commit, so the committed-state fast path could
        // still read the partition as unclaimable and swallow the one wake meant
        // for it; a woken pop therefore goes straight to the planner, whose
        // overlay folds every applied-but-uncommitted entry.
        let mut woke = false;

        loop {
            // PLAN_RAFT_DRAIN_FIX P1.1: a re-poll that cannot come back before the
            // deadline is not started — a pop that times out while queued still
            // CLAIMS, and nobody would ever ack that lease.
            if attempt > 0 && ctx.deadline.remaining() < *POP_SUBMIT_MIN {
                return self
                    .render_claims(ctx, &queue, &group, &worker, auto_ack, Vec::new())
                    .await;
            }
            attempt += 1;
            // P1.2: the planner refuses to claim once nobody can receive the answer.
            let deadline_us = wall_micros()
                .saturating_add(ctx.deadline.remaining().as_micros().min(i64::MAX as u128) as i64);
            let cmd = PopCommand {
                request_id: uuidv7_bytes(), // a fresh command per attempt (§5.4)
                tenant: ctx.tenant.clone(),
                queue: queue.clone(),
                partition: partition.clone(),
                group: group.clone(),
                worker: worker.clone(),
                budget,
                // A pinned pop is one partition; a wildcard/discovery pop may
                // sweep up to the batch (clamped to the 64-wide checkout ceiling).
                max_parts: if partition.is_some() {
                    1
                } else {
                    options.max_parts.clamp(1, 64) as i32
                },
                lease_seconds,
                auto_ack,
                conflate: group != QUEUE_MODE_GROUP && options.conflate,
                sub: sub.clone(),
                skip_window_debounce: false,
                namespace: namespace.clone(),
                task: task.clone(),
                create_cfg: if wildcard_create {
                    Some(default_queue_config(&queue))
                } else {
                    None
                },
                deadline_us,
            };
            let command = match &partition {
                Some(_) => Command::PopPinned(cmd),
                None if !namespace.is_empty() || !task.is_empty() => Command::PopDiscover(cmd),
                None => Command::PopWildcard(cmd),
            };

            // PERF-J: a wildcard pop that is provably empty from committed state
            // (the group is registered and no partition is ready) never enters
            // the single serial batcher pipeline, where its ~0.5 ms plan would
            // queue behind — and delay — the pushes. It flows into exactly the
            // same empty handling below (long-poll park or empty render); a push
            // that lands meanwhile re-arms the ring and wakes the park, so no
            // claim is stranded (§9.5).
            let claims = if self.pop_fastpath_empty
                && !woke
                && matches!(command, Command::PopWildcard(_))
                && self
                    .wildcard_would_be_empty(&ctx.tenant, &queue, &group)
                    .await
            {
                Vec::new()
            } else {
                let reply = self.submit(ctx, command).await?;
                match reply {
                    Reply::Done { outcome, .. } => match outcome {
                        Outcome::Pop(o) => o.claims,
                        other => {
                            return Err(RsmError::Internal(format!(
                                "pop got a non-pop outcome: {other:?}"
                            )))
                        }
                    },
                    other => return Err(reply_error(other)),
                }
            };

            if !claims.is_empty() {
                return self
                    .render_claims(ctx, &queue, &group, &worker, auto_ack, claims)
                    .await;
            }

            // Empty. Long-poll only for a queue-scoped pop (§9.5); discovery has
            // no single gate here.
            if !wait || partition.is_none() && (!namespace.is_empty() || !task.is_empty()) {
                return self
                    .render_claims(ctx, &queue, &group, &worker, auto_ack, Vec::new())
                    .await;
            }
            let remaining = ctx.deadline.remaining();
            if remaining.is_zero() {
                return self
                    .render_claims(ctx, &queue, &group, &worker, auto_ack, Vec::new())
                    .await;
            }
            let park = remaining.min(Duration::from_millis(500));
            woke = self.gates.wait(&gate_key, park).await;
            if ctx.deadline.expired() {
                return self
                    .render_claims(ctx, &queue, &group, &worker, auto_ack, Vec::new())
                    .await;
            }
            // Loop and re-poll.
        }
    }

    /// Read every claim's payload off this node's files (§7.5) and render the
    /// pop wire body. The reads run on the blocking pool (I15).
    async fn render_claims(
        &self,
        ctx: &ReqCtx,
        queue: &str,
        group: &str,
        worker: &str,
        auto_ack: bool,
        claims: Vec<PopClaim>,
    ) -> Result<PopOut, RsmError> {
        let store = self.store.clone();
        let reader = self.reader.clone();
        let qlog_reader = self.qlog_reader.clone();
        let encryption = self.encryption.clone();
        let tenant = ctx.tenant.clone();
        let queue = queue.to_string();
        let group = group.to_string();
        let worker = worker.to_string();
        let deadline = ctx.deadline;

        let rendered = tokio::task::spawn_blocking(move || {
            // PERF-1: pop payload read latency — the blocking segment render.
            // The clock read is gated on the knob (`stamp` is `None` when
            // metrics are off) so the ablation prices it, not just the record.
            let r0 = crate::rsm::timing::stamp();
            let out = render_pop_blocking(
                &store,
                &reader,
                qlog_reader.as_ref(),
                &encryption,
                &tenant,
                &queue,
                &group,
                &worker,
                auto_ack,
                &claims,
                deadline,
            );
            if let Some(r0) = r0 {
                crate::rsm::timing::metrics()
                    .pop_read
                    .record_dur(r0.elapsed());
            }
            out
        })
        .await
        .map_err(|e| RsmError::Internal(format!("pop render task: {e}")))?;

        rendered.map_err(RsmError::Internal)
    }
}

/// Per-partition read context for the render.
struct PartInfo {
    name: String,
    queue: String,
    bucket: u16,
    sealed: Vec<u32>,
}

/// The blocking render: one store read for every claimed partition's name and
/// sealed-file list, then a segment read per claimed segment.
#[allow(clippy::too_many_arguments)]
fn render_pop_blocking(
    store: &HeedStore,
    reader: &segments::Reader,
    qlog_reader: Option<&QLogReader>,
    encryption: &crate::encryption::Encryption,
    tenant: &str,
    top_queue: &str,
    group: &str,
    worker: &str,
    auto_ack: bool,
    claims: &[PopClaim],
    deadline: super::Deadline,
) -> Result<PopOut, String> {
    // Resolve every claimed pid's partition row + sealed files in one read txn.
    //
    // PLAN_RAFT_DRAIN_FIX P1.4: the pop is answered right after APPLY, which can
    // precede the store commit by a few ms (I4: the payload is in the files, the
    // ROWS wait for the next commit). A partition CREATED in that window has no
    // committed row yet; skipping its claim sent the client a batch without
    // those messages, so they were never acked and the lease froze the
    // partition for its whole length (measured: ~48 partitions at startup). Wait
    // for the commit instead (bounded), and count what still misses.
    let mut infos: std::collections::HashMap<Pid, PartInfo> = std::collections::HashMap::new();
    for attempt in 0..50u32 {
        if attempt > 0 {
            crate::rsm::dbgctr::inc(&crate::rsm::dbgctr::C.render_part_retry, 1);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        render_part_infos(store, tenant, claims, &mut infos)?;
        if claims.iter().all(|c| infos.contains_key(&c.pid)) {
            break;
        }
    }
    let missing = claims
        .iter()
        .filter(|c| !infos.contains_key(&c.pid))
        .count();
    if missing > 0 {
        crate::rsm::dbgctr::inc(&crate::rsm::dbgctr::C.render_part_missing, missing as u64);
    }
    render_pop_body(
        qlog_reader,
        reader,
        encryption,
        tenant,
        top_queue,
        group,
        worker,
        auto_ack,
        claims,
        deadline,
        &infos,
    )
}

/// Fill `infos` with the committed partition row + sealed files of every claim
/// not resolved yet (one read txn).
fn render_part_infos(
    store: &HeedStore,
    tenant: &str,
    claims: &[PopClaim],
    infos: &mut std::collections::HashMap<Pid, PartInfo>,
) -> Result<(), String> {
    store
        .read(|r| {
            for c in claims {
                if infos.contains_key(&c.pid) {
                    continue;
                }
                let Some(part) = r.partition(c.pid)? else {
                    continue;
                };
                let bucket = bucket_of(tenant, &part.queue, &part.partition);
                let mut sealed = Vec::new();
                r.scan_partition_files(c.pid, usize::MAX, &mut |f| {
                    sealed.push(f);
                    true
                })?;
                infos.insert(
                    c.pid,
                    PartInfo {
                        name: part.partition,
                        queue: part.queue,
                        bucket,
                        sealed,
                    },
                );
            }
            Ok(())
        })
        .map_err(|e| format!("pop render read: {e}"))
}

/// Render the pop wire body from the resolved partition infos.
#[allow(clippy::too_many_arguments)]
fn render_pop_body(
    qlog_reader: Option<&QLogReader>,
    reader: &segments::Reader,
    encryption: &crate::encryption::Encryption,
    tenant: &str,
    top_queue: &str,
    group: &str,
    worker: &str,
    auto_ack: bool,
    claims: &[PopClaim],
    deadline: super::Deadline,
    infos: &std::collections::HashMap<Pid, PartInfo>,
) -> Result<PopOut, String> {
    let lease_id = if auto_ack || claims.is_empty() {
        ""
    } else {
        worker
    };
    let (first_name, first_pid) = claims
        .first()
        .map(|c| {
            (
                infos.get(&c.pid).map(|i| i.name.as_str()).unwrap_or(""),
                c.pid.to_string(),
            )
        })
        .unwrap_or(("", String::new()));

    let mut out = String::with_capacity(256 + top_queue.len());
    out.push_str("{\"success\":true,\"queue\":\"");
    crate::fusion::json_escape_into(&mut out, top_queue);
    out.push_str("\",\"partition\":\"");
    crate::fusion::json_escape_into(&mut out, first_name);
    out.push_str("\",\"partitionId\":\"");
    crate::fusion::json_escape_into(&mut out, &first_pid);
    out.push_str("\",\"leaseId\":\"");
    crate::fusion::json_escape_into(&mut out, lease_id);
    out.push_str("\",\"consumerGroup\":\"");
    crate::fusion::json_escape_into(&mut out, group);
    out.push_str("\",\"messages\":[");

    let dl = Some(deadline.instant());
    let mut count = 0usize;
    for claim in claims {
        let Some(info) = infos.get(&claim.pid) else {
            continue;
        };
        let attempt = claim.delivery_attempt.max(1);
        let partition_id = claim.pid.to_string();
        // Phase A2: the queue-log id for this partition's queue, when the qlog
        // read path is on. The claimed offsets are all committed (the claim came
        // from a committed plan), and the qlog is flushed BEFORE the store commit
        // that made them committed, so the record is always present — a `None`
        // here is a genuine gap, exactly as a segment miss.
        let qlog_qid = qlog_reader.map(|_| QLogReader::queue_id_of(tenant, &info.queue));
        let mut off = claim.start_offset;
        while off <= claim.end_offset {
            // The payload bytes: from the QUEUE LOG when the knob is on (Phase
            // A2), else the segment files. Both return the same `(base_offset,
            // created_at, count, blob)` for a committed offset, so the rendered
            // wire body is byte-identical.
            let popped: Option<(u64, i64, u32, Vec<u8>)> = match (qlog_reader, qlog_qid) {
                (Some(ql), Some(qid)) => match ql.read_owned(qid, claim.pid, off) {
                    Ok(Some(r)) => Some((r.base_offset, r.created_at_us, r.count, r.payload)),
                    Ok(None) => None,
                    Err(e) => {
                        return Err(format!(
                            "read pop payload (qlog) at pid {} off {off}: {e}",
                            claim.pid
                        ))
                    }
                },
                _ => match reader.read_at_within(info.bucket, claim.pid, off, &info.sealed, dl) {
                    Ok(Some(f)) => Some((f.base_offset, f.created_at_us, f.count, f.blob)),
                    Ok(None) => None,
                    Err(e) => {
                        return Err(format!(
                            "read pop payload at pid {} off {off}: {e}",
                            claim.pid
                        ))
                    }
                },
            };
            let (base, created_at_us, frame_count, blob) = match popped {
                Some(t) => t,
                None => {
                    // A gap (retention passed it, or not yet visible): skip one.
                    off += 1;
                    continue;
                }
            };
            let seg_created = iso_from_us(created_at_us);
            let frames = unpack_frames_ref(&blob);
            if let Some(frames) = frames {
                for (i, fr) in frames.iter().enumerate() {
                    let msg_off = base + i as u64;
                    if msg_off < off || msg_off < claim.start_offset || msg_off > claim.end_offset {
                        continue;
                    }
                    if count > 0 {
                        out.push(',');
                    }
                    out.push_str("{\"id\":\"");
                    crate::frames::uuid_hex_into(&mut out, &fr.message_id);
                    out.push_str("\",\"transactionId\":\"");
                    crate::fusion::json_escape_into(&mut out, fr.txn);
                    out.push_str("\",\"traceId\":");
                    match &fr.trace_id {
                        Some(t) => {
                            out.push('"');
                            crate::frames::uuid_hex_into(&mut out, t);
                            out.push('"');
                        }
                        None => out.push_str("null"),
                    }
                    out.push_str(",\"data\":");
                    let decrypted = fr
                        .encrypted
                        .then(|| encryption.decrypt_payload_bytes(fr.payload))
                        .flatten();
                    let payload = decrypted.as_deref().unwrap_or(fr.payload);
                    if payload.is_empty() {
                        out.push_str("null");
                    } else {
                        push_utf8(&mut out, payload);
                    }
                    out.push_str(",\"producerSub\":");
                    match &fr.producer_sub {
                        Some(ps) => {
                            out.push('"');
                            crate::fusion::json_escape_into(&mut out, ps);
                            out.push('"');
                        }
                        None => out.push_str("null"),
                    }
                    out.push_str(",\"createdAt\":\"");
                    out.push_str(&seg_created);
                    out.push_str("\",\"partitionId\":\"");
                    crate::fusion::json_escape_into(&mut out, &partition_id);
                    out.push_str("\",\"partition\":\"");
                    crate::fusion::json_escape_into(&mut out, &info.name);
                    out.push_str("\",\"leaseId\":\"");
                    crate::fusion::json_escape_into(&mut out, lease_id);
                    out.push_str("\",\"consumerGroup\":\"");
                    crate::fusion::json_escape_into(&mut out, group);
                    out.push_str("\",\"deliveryAttempt\":");
                    out.push_str(&attempt.to_string());
                    out.push_str(",\"offset\":");
                    out.push_str(&msg_off.to_string());
                    out.push('}');
                    count += 1;
                }
            }
            // Advance past this whole segment; a claim range is a run of segments.
            off = base + frame_count as u64;
        }
        // silence the unused warning on info.queue (kept for a future
        // per-partition top-level queue on discovery).
        let _ = &info.queue;
    }

    out.push_str("],\"partitionsClaimed\":");
    out.push_str(&claims.len().to_string());
    out.push('}');
    Ok(PopOut {
        body: out,
        empty: count == 0,
    })
}

// ---------------------------------------------------------------------------
// Ack
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct AckBodyIn {
    // single
    #[serde(rename = "transactionId")]
    transaction_id: Option<String>,
    #[serde(rename = "partitionId")]
    partition_id: Option<String>,
    status: Option<String>,
    #[serde(rename = "leaseId")]
    lease_id: Option<String>,
    error: Option<String>,
    // batch
    #[serde(default)]
    acknowledgments: Vec<AckBodyItem>,
}

#[derive(Deserialize)]
struct AckBodyItem {
    #[serde(rename = "transactionId")]
    transaction_id: Option<String>,
    #[serde(rename = "partitionId")]
    partition_id: Option<String>,
    status: Option<String>,
    #[serde(rename = "leaseId")]
    lease_id: Option<String>,
    error: Option<String>,
}

/// One flat ack the receiver resolved: original index, txn, pid, worker, status,
/// error.
struct AckFlat {
    index: usize,
    txn: String,
    pid: Pid,
    worker: String,
    status: AckStatus,
    error: Option<String>,
}

fn ack_status_of(s: Option<&str>) -> AckStatus {
    match s.map(|x| x.to_ascii_lowercase()).as_deref() {
        Some("failed") => AckStatus::Failed,
        Some("dlq") => AckStatus::Dlq,
        Some("retry") => AckStatus::Retry,
        _ => AckStatus::Ok,
    }
}

impl RaftFacade {
    async fn ack_impl(&self, ctx: ReqCtx, req: AckReq) -> Result<AckOut, RsmError> {
        let body: AckBodyIn = serde_json::from_slice(&req.raw).map_err(|e| RsmError::Rejected {
            code: "bad_body".into(),
            message: format!("bad ack body: {e}"),
        })?;

        // Flatten single vs batch into (index, txn, partitionId, status, lease).
        let mut raw_items: Vec<(String, String, AckStatus, String, Option<String>)> = Vec::new();
        if !body.acknowledgments.is_empty() {
            for it in &body.acknowledgments {
                raw_items.push((
                    it.transaction_id.clone().unwrap_or_default(),
                    it.partition_id.clone().unwrap_or_default(),
                    ack_status_of(it.status.as_deref()),
                    it.lease_id.clone().unwrap_or_default(),
                    it.error.clone(),
                ));
            }
        } else {
            raw_items.push((
                body.transaction_id.clone().unwrap_or_default(),
                body.partition_id.clone().unwrap_or_default(),
                ack_status_of(body.status.as_deref()),
                body.lease_id.clone().unwrap_or_default(),
                body.error.clone(),
            ));
        }
        if raw_items.is_empty() {
            return Ok(AckOut { body: "[]".into() });
        }

        // Resolve each `partitionId` (the numeric pid, see the module header)
        // to a pid; an unparsable one is a per-item error the render carries.
        let mut flats: Vec<AckFlat> = Vec::with_capacity(raw_items.len());
        let mut bad: Vec<(usize, String)> = Vec::new();
        for (i, (txn, pid_str, status, worker, error)) in raw_items.iter().enumerate() {
            match pid_str.parse::<u64>() {
                Ok(pid) => flats.push(AckFlat {
                    index: i,
                    txn: txn.clone(),
                    pid,
                    worker: worker.clone(),
                    status: *status,
                    error: error.clone(),
                }),
                Err(_) => bad.push((i, "partitionId is not a partition id".to_string())),
            }
        }

        // Group by (pid, worker) into AckTargets. Read each pid's (tenant,
        // queue) once. A pid with no row is a per-item error.
        let store = self.store.clone();
        let reader = self.reader.clone();
        let qlog_reader = self.qlog_reader.clone();
        let encryption = self.encryption.clone();
        let tenant = ctx.tenant.clone();
        let group = req.group.clone();
        let resolved = tokio::task::spawn_blocking(move || {
            resolve_ack_targets(
                &store,
                &reader,
                qlog_reader.as_ref(),
                &encryption,
                &tenant,
                &group,
                flats,
            )
        })
        .await
        .map_err(|e| RsmError::Internal(format!("ack resolve task: {e}")))?;
        let (targets, per_item, more_bad) = resolved.map_err(RsmError::Internal)?;
        bad.extend(more_bad);

        let txns: Vec<String> = raw_items.iter().map(|r| r.0.clone()).collect();

        if targets.is_empty() {
            // Nothing resolvable: render all as errors/failures.
            return Ok(AckOut {
                body: render_ack(&txns, &[], &per_item, &bad),
            });
        }

        let cmd = Command::Ack(AckCommand {
            request_id: ctx.request_id,
            targets,
        });
        let reply = self.submit(&ctx, cmd).await?;
        let results = match reply {
            Reply::Done { outcome, .. } => match outcome {
                Outcome::Ack(o) => o.results,
                other => {
                    return Err(RsmError::Internal(format!(
                        "ack got a non-ack outcome: {other:?}"
                    )))
                }
            },
            other => return Err(reply_error(other)),
        };

        Ok(AckOut {
            body: render_ack(&txns, &results, &per_item, &bad),
        })
    }
}

/// The per-input-item resolution result: which target it went to (by index into
/// the returned targets), so the render can attribute the target's lease/dlq.
/// The item's own `status` is kept because the target's `AckResult` reports the
/// DLQ as a COUNT, not a per-item set (WP-1.1's shape, R-101): the render needs
/// this item's status to tell a filed dead letter from a sibling ack on the same
/// target (see [`render_ack`]).
struct AckPerItem {
    index: usize,
    target: usize,
    hash: [u8; 16],
    status: AckStatus,
    lease_invalid: bool,
}

struct AckSnapshotWork {
    input: usize,
    target: usize,
    item: usize,
    pid: Pid,
    queue: String,
    partition: String,
    from: u64,
    to: u64,
    sealed: Vec<u32>,
    hash: [u8; 16],
    txn: String,
}

/// Group resolved acks by (pid, worker), reading each pid's queue once.
fn resolve_ack_targets(
    store: &HeedStore,
    reader: &segments::Reader,
    qlog_reader: Option<&QLogReader>,
    encryption: &crate::encryption::Encryption,
    tenant: &str,
    group: &str,
    flats: Vec<AckFlat>,
) -> Result<(Vec<AckTarget>, Vec<AckPerItem>, Vec<(usize, String)>), String> {
    let mut targets: Vec<AckTarget> = Vec::new();
    let mut per_item: Vec<AckPerItem> = Vec::new();
    let mut bad: Vec<(usize, String)> = Vec::new();
    let mut snapshot_work: Vec<AckSnapshotWork> = Vec::new();
    // (pid, worker) → target index.
    let mut index: std::collections::HashMap<(Pid, String), usize> =
        std::collections::HashMap::new();

    store
        .read(|r| {
            for f in &flats {
                let Some(part) = r.partition(f.pid)? else {
                    bad.push((f.index, format!("no partition {}", f.pid)));
                    continue;
                };
                let key = (f.pid, f.worker.clone());
                let ti = match index.get(&key) {
                    Some(&ti) => ti,
                    None => {
                        let ti = targets.len();
                        targets.push(AckTarget {
                            pid: f.pid,
                            tenant: tenant.to_string(),
                            queue: part.queue.clone(),
                            group: group.to_string(),
                            worker: f.worker.clone(),
                            items: Vec::new(),
                        });
                        index.insert(key, ti);
                        ti
                    }
                };
                let hash = txn_hash128(&f.txn);
                // Keep the reason for a planner-side stale result.  AckResult's
                // replicated shape intentionally carries hashes rather than
                // receiver error strings, so the receiver snapshots whether a
                // presented lease was already invalid while resolving it.
                let lease_invalid = if f.worker.is_empty() {
                    false
                } else {
                    match r.cursor(f.pid, group)? {
                        Some(cur) => {
                            cur.worker.as_deref() != Some(f.worker.as_str())
                                || !rows::lease_live(&cur, wall_micros())
                        }
                        None => true,
                    }
                };
                // O20: a signal carries the original frame snapshot so the DLQ
                // write and cursor advance remain one replicated entry. Populate
                // it after this short store read from the node-local payload log.
                let signal = matches!(f.status, AckStatus::Dlq | AckStatus::Failed);
                let snapshot = signal.then(|| DlqSnapshot {
                    message_id: None,
                    txn: f.txn.clone(),
                    payload: Vec::new(),
                });
                let item = targets[ti].items.len();
                targets[ti].items.push(AckItem {
                    hash,
                    status: f.status,
                    error: f.error.clone(),
                    snapshot,
                });
                if signal {
                    if let Some(cur) = r.cursor(f.pid, group)? {
                        if let Some(to) = cur.batch_end {
                            let from = (cur.committed + 1).max(part.log_start as i64).max(0) as u64;
                            let mut sealed = Vec::new();
                            r.scan_partition_files(f.pid, usize::MAX, &mut |file| {
                                sealed.push(file);
                                true
                            })?;
                            snapshot_work.push(AckSnapshotWork {
                                input: f.index,
                                target: ti,
                                item,
                                pid: f.pid,
                                queue: part.queue.clone(),
                                partition: part.partition.clone(),
                                from,
                                to,
                                sealed,
                                hash,
                                txn: f.txn.clone(),
                            });
                        }
                    }
                }
                per_item.push(AckPerItem {
                    index: f.index,
                    target: ti,
                    hash,
                    status: f.status,
                    lease_invalid,
                });
            }
            Ok(())
        })
        .map_err(|e| format!("ack resolve read: {e}"))?;

    for work in snapshot_work {
        match read_dlq_snapshot(reader, qlog_reader, encryption, tenant, &work) {
            Ok(Some(snapshot)) => targets[work.target].items[work.item].snapshot = Some(snapshot),
            Ok(None) => {
                // The planner will classify an unknown/stale hash without ever
                // consuming the placeholder. A live signal whose frame really
                // disappeared is reported by the payload readers as an error.
            }
            Err(e) => {
                return Err(format!("ack snapshot for item {}: {e}", work.input));
            }
        }
    }
    Ok((targets, per_item, bad))
}

fn read_dlq_snapshot(
    reader: &segments::Reader,
    qlog_reader: Option<&QLogReader>,
    encryption: &crate::encryption::Encryption,
    tenant: &str,
    work: &AckSnapshotWork,
) -> Result<Option<DlqSnapshot>, String> {
    let mut off = work.from;
    while off <= work.to {
        let record = match qlog_reader {
            Some(qlog) => {
                let qid = QLogReader::queue_id_of(tenant, &work.queue);
                qlog.read_owned(qid, work.pid, off)
                    .map_err(|e| format!("qlog read pid {} offset {off}: {e}", work.pid))?
                    .map(|r| (r.base_offset, r.count, r.payload))
            }
            None => reader
                .read_at(
                    bucket_of(tenant, &work.queue, &work.partition),
                    work.pid,
                    off,
                    &work.sealed,
                )
                .map_err(|e| format!("segment read pid {} offset {off}: {e}", work.pid))?
                .map(|r| (r.base_offset, r.count, r.blob)),
        };
        let Some((base, count, blob)) = record else {
            return Ok(None);
        };
        let frames = unpack_frames_ref(&blob)
            .ok_or_else(|| format!("invalid packed frames at pid {} offset {base}", work.pid))?;
        for (i, frame) in frames.iter().enumerate() {
            let message_offset = base + i as u64;
            if message_offset < work.from || message_offset > work.to {
                continue;
            }
            if txn_hash128(frame.txn) == work.hash && frame.txn == work.txn {
                let payload = if frame.encrypted {
                    encryption
                        .decrypt_payload_bytes(frame.payload)
                        .unwrap_or_else(|| frame.payload.to_vec())
                } else {
                    frame.payload.to_vec()
                };
                return Ok(Some(DlqSnapshot {
                    message_id: Some(frame.message_id),
                    txn: frame.txn.to_string(),
                    payload,
                }));
            }
        }
        off = base.saturating_add(count as u64);
        if count == 0 {
            return Err(format!(
                "zero-frame payload record at pid {} offset {base}",
                work.pid
            ));
        }
    }
    Ok(None)
}

/// `[{index, transactionId, success, error, leaseReleased, dlq, noop}]`, input order
/// (`handlers/data.rs` ack wire). The group is applied by the caller.
fn render_ack(
    txns: &[String],
    results: &[crate::rsm::entry::AckResult],
    per_item: &[AckPerItem],
    bad: &[(usize, String)],
) -> String {
    let mut out = String::with_capacity(txns.len() * 96 + 2);
    out.push('[');
    for (i, txn) in txns.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"index\":");
        out.push_str(&i.to_string());
        // Attribute this input item to its target result.
        let item = per_item.iter().find(|p| p.index == i);
        let bad_msg = bad.iter().find(|(bi, _)| *bi == i).map(|(_, m)| m.as_str());
        out.push_str(",\"transactionId\":\"");
        crate::fusion::json_escape_into(&mut out, txn);
        out.push('"');
        if let Some(msg) = bad_msg {
            out.push_str(",\"success\":false,\"error\":\"");
            crate::fusion::json_escape_into(&mut out, msg);
            out.push_str("\",\"leaseReleased\":false,\"dlq\":false,\"noop\":false}");
            continue;
        }
        let (success, lease_released, dlq, noop, error) = match item
            .and_then(|p| Some((p, results.get(p.target)?)))
        {
            Some((p, res)) => {
                let stale = res.stale_hashes.contains(&p.hash);
                let noop = res.noop_hashes.contains(&p.hash);
                // Per-item DLQ, NOT `res.dlq > 0` broadcast to the whole target
                // (the batch-ack mis-attribution R-101 leaves us to guard here).
                // `res.dlq` is a COUNT — the outcome shape carries no per-item
                // DLQ set — so a completed ack that shares a (partition, lease)
                // target with a sibling that DID dead-letter must not inherit
                // its flag. An item reads `dlq:true` only when its target filed
                // a dead letter AND this item itself carried a DLQ-eligible
                // signal; `res.dlq == 0` (e.g. a `failed` whose retry budget
                // remained, so it was released to redeliver) reads false for
                // every item. RESIDUAL, owed to R-101's shape refinement: two+
                // signal items on ONE target with `res.dlq == 1` see the head
                // (lowest-offset) one filed, but the receiver holds no offsets
                // in this AckResult shape and marks each signal item — the
                // per-item DLQ set the outcome must carry to disambiguate.
                let dlq = res.dlq > 0 && matches!(p.status, AckStatus::Dlq | AckStatus::Failed);
                let error = if stale && p.lease_invalid {
                    Some("invalid or expired lease")
                } else if stale && !matches!(p.status, AckStatus::Ok) {
                    Some(
                            "transaction is unresolvable, already committed, or acknowledgment is stale",
                        )
                } else if stale {
                    Some(
                            "transaction is unresolvable, already committed, or acknowledgment is stale",
                        )
                } else {
                    None
                };
                (!stale, res.lease_released, dlq, noop, error)
            }
            None => (true, false, false, false, None),
        };
        out.push_str(",\"success\":");
        out.push_str(if success { "true" } else { "false" });
        out.push_str(",\"error\":");
        if let Some(error) = error {
            out.push('"');
            crate::fusion::json_escape_into(&mut out, error);
            out.push('"');
        } else {
            out.push_str("null");
        }
        out.push_str(",\"leaseReleased\":");
        out.push_str(if lease_released { "true" } else { "false" });
        out.push_str(",\"dlq\":");
        out.push_str(if dlq { "true" } else { "false" });
        out.push_str(",\"noop\":");
        out.push_str(if noop { "true" } else { "false" });
        out.push('}');
    }
    out.push(']');
    out
}

// ---------------------------------------------------------------------------
// Renew, DLQ head, pending, depth
// ---------------------------------------------------------------------------

impl RaftFacade {
    async fn renew_impl(&self, ctx: ReqCtx, req: RenewReq) -> Result<RenewOut, RsmError> {
        let cmd = Command::Renew(RenewCommand {
            request_id: ctx.request_id,
            worker: req.lease_id.clone(),
            seconds: req.seconds.clamp(1, i32::MAX as i64) as i32,
        });
        let reply = self.submit(&ctx, cmd).await?;
        let outcome = match reply {
            Reply::Done { outcome, .. } => outcome,
            other => return Err(reply_error(other)),
        };
        let (renewed, expires) = match outcome {
            Outcome::Renew(r) => (r.renewed, r.min_expires_at_us),
            other => {
                return Err(RsmError::Internal(format!(
                    "renew got a non-renew outcome: {other:?}"
                )))
            }
        };
        let expires_iso = expires.map(iso_from_us);
        let mut out = String::from("{\"leaseId\":\"");
        crate::fusion::json_escape_into(&mut out, &req.lease_id);
        out.push_str("\",\"success\":");
        out.push_str(if renewed > 0 { "true" } else { "false" });
        out.push_str(",\"renewed\":");
        out.push_str(&renewed.to_string());
        for key in ["newExpiresAt", "expiresAt", "lease_expires_at"] {
            out.push_str(",\"");
            out.push_str(key);
            out.push_str("\":");
            match &expires_iso {
                Some(e) => {
                    out.push('"');
                    out.push_str(e);
                    out.push('"');
                }
                None => out.push_str("null"),
            }
        }
        out.push('}');
        Ok(RenewOut { body: out })
    }
}

// ---------------------------------------------------------------------------
// Timers (WP-2.3, 025)
// ---------------------------------------------------------------------------

/// A store error on a timer read: retryable ones are a 503 the client retries,
/// anything else a broker fault.
fn read_error(e: crate::rsm::store::StoreError) -> RsmError {
    if e.retryable() {
        RsmError::Retry { leader_hint: None }
    } else {
        RsmError::Internal(format!("timer read: {e}"))
    }
}

fn timers_bad_request(message: impl Into<String>) -> RsmError {
    RsmError::Rejected {
        code: "timers_bad_request".into(),
        message: message.into(),
    }
}

impl RaftFacade {
    /// The receiver of a timers call: 025's validation and the frame packing
    /// (O20 — the planner never packs), the R-108 key bound, then ONE
    /// [`Command::Timers`] under the request's id, answered once its entry is
    /// committed and applied (I4). A retry of the same request id is answered
    /// from the recorded outcome (D6, I6).
    async fn timers_apply_impl(&self, ctx: ReqCtx, req: TimersReq) -> Result<TimersOut, RsmError> {
        let mut ops = parse_timer_ops(&req.ops, req.producer_sub.as_deref())
            .map_err(|e| timers_bad_request(e.message))?;
        if ops.is_empty() {
            return Ok(TimersOut {
                results: Vec::new(),
            });
        }
        let encrypted_queues = self
            .encrypted_queues(
                &ctx.tenant,
                ops.iter().map(|op| op.queue().to_string()).collect(),
            )
            .await?;
        self.encrypt_timer_ops(&mut ops, &encrypted_queues)
            .map_err(timers_bad_request)?;
        for op in &ops {
            // (tenant, queue, timer_key) is the `timers` key; the schedule's
            // (tenant, queue, partition) becomes `partitions_by_key` at the
            // fire, and must be storable NOW rather than fail at the fire.
            super::check_message_key_names(&ctx.tenant, op.queue(), None, Some(op.key()))?;
            if let TimerOp::Schedule(s) = op {
                super::check_message_key_names(&ctx.tenant, &s.queue, None, Some(&s.partition))?;
            }
        }
        let cmd = Command::Timers(TimersCommand {
            request_id: ctx.request_id,
            tenant: ctx.tenant.clone(),
            ops,
        });
        match self.submit(&ctx, cmd).await? {
            Reply::Done { outcome, .. } => match timers_results(&outcome) {
                Some(results) => Ok(TimersOut { results }),
                None => Err(RsmError::Internal(format!(
                    "timers got a non-timers outcome: {outcome:?}"
                ))),
            },
            other => Err(reply_error(other)),
        }
    }

    /// Run one committed-state read on the blocking pool (I15), under the
    /// request deadline. Timer reads are LOCAL reads: the RAM keyspaces are
    /// live, so a read after an answered schedule sees it (read-your-writes on
    /// this node).
    async fn timer_read<F>(&self, ctx: &ReqCtx, f: F) -> Result<TimerReadOut, RsmError>
    where
        F: FnOnce(&HeedStore) -> Result<String, RsmError> + Send + 'static,
    {
        let store = self.store.clone();
        let task = tokio::task::spawn_blocking(move || f(&store));
        match tokio::time::timeout(ctx.deadline.remaining(), task).await {
            Ok(Ok(Ok(body))) => Ok(TimerReadOut { body }),
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(join)) => Err(RsmError::Internal(format!("timer read task: {join}"))),
            Err(_) => Err(RsmError::Timeout),
        }
    }

    async fn timer_peek_impl(
        &self,
        ctx: ReqCtx,
        req: TimerPeekReq,
    ) -> Result<TimerReadOut, RsmError> {
        if req.queue.is_empty() || req.timer_key.is_empty() {
            return Err(timers_bad_request(
                "QTIMER peek needs a queue and a timerKey",
            ));
        }
        super::check_message_key_names(&ctx.tenant, &req.queue, None, Some(&req.timer_key))?;
        let tenant = ctx.tenant.clone();
        self.timer_read(&ctx, move |store| {
            store
                .read(|r| {
                    let row = r.timer(&tenant, &req.queue, &req.timer_key)?;
                    Ok(peek_json(&req.queue, &req.timer_key, row.as_ref()).to_string())
                })
                .map_err(read_error)
        })
        .await
    }

    async fn timers_list_impl(
        &self,
        ctx: ReqCtx,
        req: TimersListReq,
    ) -> Result<TimerReadOut, RsmError> {
        if req.queue.is_empty() {
            return Err(timers_bad_request("QTIMER list needs a queue"));
        }
        super::check_message_key_names(&ctx.tenant, &req.queue, None, req.after.as_deref())?;
        let tenant = ctx.tenant.clone();
        let limit = list_limit(req.limit);
        self.timer_read(&ctx, move |store| {
            store
                .read(|r| {
                    // LIMIT n + 1: the probe row decides `truncated` without a
                    // second read (025 `list_v1`).
                    let mut rows: Vec<serde_json::Value> = Vec::with_capacity(limit + 1);
                    r.scan_timers(
                        &tenant,
                        &req.queue,
                        req.after.as_deref(),
                        limit + 1,
                        &mut |key, row| {
                            rows.push(list_row_json(&req.queue, key, &row));
                            true
                        },
                    )?;
                    let truncated = rows.len() > limit;
                    rows.truncate(limit);
                    let next_after = if truncated {
                        rows.last()
                            .and_then(|r| r.get("timerKey").cloned())
                            .unwrap_or(serde_json::Value::Null)
                    } else {
                        serde_json::Value::Null
                    };
                    let mut m = serde_json::Map::new();
                    m.insert("rows".into(), serde_json::Value::Array(rows));
                    m.insert("truncated".into(), serde_json::Value::Bool(truncated));
                    m.insert("nextAfter".into(), next_after);
                    Ok(serde_json::Value::Object(m).to_string())
                })
                .map_err(read_error)
        })
        .await
    }

    async fn timers_count_impl(
        &self,
        ctx: ReqCtx,
        req: TimersCountReq,
    ) -> Result<TimerReadOut, RsmError> {
        if req.queue.is_empty() {
            return Err(timers_bad_request("QTIMER count needs a queue"));
        }
        if req.prefix.is_empty() {
            return Err(timers_bad_request("QTIMER count needs a non-empty prefix"));
        }
        if req.prefix.len() > 128 {
            return Err(timers_bad_request("QTIMER count prefix exceeds 128 bytes"));
        }
        let tenant = ctx.tenant.clone();
        self.timer_read(&ctx, move |store| {
            store
                .read(|r| {
                    let n = r.count_timers_with_prefix(&tenant, &req.queue, &req.prefix)?;
                    Ok(format!("{{\"count\":{n}}}"))
                })
                .map_err(read_error)
        })
        .await
    }
}

// ---------------------------------------------------------------------------
// The Rsm impl
// ---------------------------------------------------------------------------

#[async_trait]
impl Rsm for RaftFacade {
    fn bootstrap(&self) -> super::RsmBootstrap {
        match self.store.read(|r| {
            let flag = |key: &str, default: bool| -> crate::rsm::store::Result<bool> {
                Ok(r.flag(key)?
                    .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
                    .and_then(|v| v.get("enabled").and_then(serde_json::Value::as_bool))
                    .unwrap_or(default))
            };
            let mut ephemeral_configs = Vec::new();
            r.scan_eph_configs(usize::MAX, &mut |tenant, queue, row| {
                if let Ok(options) = serde_json::from_slice(&row.options) {
                    ephemeral_configs.push((tenant.to_string(), queue.to_string(), options));
                }
                true
            })?;
            let now_us = wall_micros();
            let mut grants = Vec::new();
            let mut bad_quota = false;
            r.scan_raw(
                crate::rsm::store::Keyspace::Quotas,
                &[],
                &[],
                usize::MAX,
                &mut |key, value| {
                    if let (Some((kind, tenant)), Ok(grant)) = (
                        crate::rsm::store::keys::quota_parts(key),
                        crate::rsm::store::rows::quota_decode(value),
                    ) {
                        grants.push((kind, tenant, grant));
                    } else {
                        bad_quota = true;
                        return false;
                    }
                    true
                },
            )?;
            if bad_quota {
                return Err(crate::rsm::store::StoreError::corrupt(
                    crate::rsm::store::Keyspace::Quotas,
                    "quota row",
                ));
            }
            let mut kv_grants = Vec::new();
            let mut ephemeral_grants = Vec::new();
            for (kind, tenant, grant) in grants {
                match kind {
                    crate::rsm::effect::QuotaKind::Kv => {
                        let mut kv_rows = 0i64;
                        let mut kv_bytes = 0i64;
                        r.scan_kv_tenant(&tenant, usize::MAX, &mut |_ns, _key, row| {
                            if row.live(now_us) {
                                kv_rows += 1;
                                kv_bytes = kv_bytes.saturating_add(row.value.len() as i64);
                            }
                            true
                        })?;
                        let mut timer_rows = 0i64;
                        r.scan_raw(
                            crate::rsm::store::Keyspace::Timers,
                            &crate::rsm::store::keys::queues_prefix(&tenant),
                            &crate::rsm::store::keys::queues_prefix(&tenant),
                            usize::MAX,
                            &mut |_key, _value| {
                                timer_rows += 1;
                                true
                            },
                        )?;
                        kv_grants.push((tenant, grant, kv_rows, kv_bytes, timer_rows));
                    }
                    crate::rsm::effect::QuotaKind::Ephemeral => {
                        ephemeral_grants.push((tenant, grant));
                    }
                    crate::rsm::effect::QuotaKind::Streams => {}
                }
            }
            Ok(super::RsmBootstrap {
                startup_error: None,
                maintenance: flag("maintenance_mode", false)?,
                pop_maintenance: flag("pop_maintenance_mode", false)?,
                kv_enabled: flag(crate::switches::Switches::KEY_KV, true)?,
                timers_schedule_enabled: flag(
                    crate::switches::Switches::KEY_TIMERS_SCHEDULE,
                    true,
                )?,
                timers_fire_enabled: flag(crate::switches::Switches::KEY_TIMERS_FIRE, true)?,
                ephemeral_enabled: flag(crate::switches::Switches::KEY_EPHEMERAL, true)?,
                ephemeral_configs,
                kv_grants,
                ephemeral_grants,
            })
        }) {
            Ok(bootstrap) => bootstrap,
            Err(error) => super::RsmBootstrap {
                startup_error: Some(error.to_string()),
                ..super::RsmBootstrap::default()
            },
        }
    }

    fn prometheus(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP queen_raft_store_operations_total Embedded-store operations by kind\n# TYPE queen_raft_store_operations_total counter\n");
        for (kind, value) in self.store.metrics().snapshot() {
            out.push_str(&format!(
                "queen_raft_store_operations_total{{kind=\"{kind}\"}} {value}\n"
            ));
        }
        let map = self.store.map_usage();
        out.push_str("# HELP queen_raft_store_map_bytes LMDB map capacity and use\n# TYPE queen_raft_store_map_bytes gauge\n");
        out.push_str(&format!(
            "queen_raft_store_map_bytes{{kind=\"capacity\"}} {}\nqueen_raft_store_map_bytes{{kind=\"used\"}} {}\n",
            map.map_bytes, map.used_bytes
        ));
        out.push_str("# HELP queen_raft_store_readers LMDB reader slots\n# TYPE queen_raft_store_readers gauge\n");
        out.push_str(&format!(
            "queen_raft_store_readers{{kind=\"used\"}} {}\nqueen_raft_store_readers{{kind=\"limit\"}} {}\n",
            map.readers_in_use, map.max_readers
        ));

        let repl = self.repl.metrics();
        out.push_str("# HELP queen_raft_index Raft/RSM indexes\n# TYPE queen_raft_index gauge\n");
        for (kind, value) in [
            ("log", repl.last_log_index),
            ("committed", repl.committed_index),
            ("applied", repl.applied_index),
            ("durable", repl.durable_index),
        ] {
            out.push_str(&format!("queen_raft_index{{kind=\"{kind}\"}} {value}\n"));
        }
        out.push_str("# HELP queen_raft_inflight Entries committed or proposed but not applied\n# TYPE queen_raft_inflight gauge\n");
        out.push_str(&format!("queen_raft_inflight {}\n", repl.inflight));
        out.push_str("# HELP queen_raft_proposals_total Proposals accepted since open\n# TYPE queen_raft_proposals_total counter\n");
        out.push_str(&format!("queen_raft_proposals_total {}\n", repl.proposals));
        out.push_str("# HELP queen_raft_log_storage Queue-log files and bytes\n# TYPE queen_raft_log_storage gauge\n");
        out.push_str(&format!(
            "queen_raft_log_storage{{kind=\"files\"}} {}\nqueen_raft_log_storage{{kind=\"bytes\"}} {}\n",
            repl.log_files, repl.log_bytes
        ));
        out.push_str("# HELP queen_raft_storage_full Disk/map admission gate\n# TYPE queen_raft_storage_full gauge\n");
        out.push_str(&format!(
            "queen_raft_storage_full {}\n",
            u8::from(self.storage_full.load(std::sync::atomic::Ordering::Relaxed))
        ));
        out
    }

    async fn push(&self, ctx: ReqCtx, req: PushReq) -> Result<PushOut, RsmError> {
        self.push_impl(ctx, req).await
    }

    async fn pop_wildcard(&self, ctx: ReqCtx, req: PopReq) -> Result<PopOut, RsmError> {
        self.pop_run(
            &ctx,
            req.queue,
            None,
            String::new(),
            String::new(),
            req.group,
            req.batch,
            req.auto_ack,
            req.wait,
            true,
            req.options,
        )
        .await
    }

    async fn pop_pinned(&self, ctx: ReqCtx, req: PopPinnedReq) -> Result<PopOut, RsmError> {
        self.pop_run(
            &ctx,
            req.queue,
            Some(req.partition),
            String::new(),
            String::new(),
            req.group,
            req.batch,
            req.auto_ack,
            req.wait,
            false,
            req.options,
        )
        .await
    }

    async fn pop_discover(&self, ctx: ReqCtx, req: PopDiscoverReq) -> Result<PopOut, RsmError> {
        self.pop_run(
            &ctx,
            String::new(),
            None,
            req.namespace,
            req.task,
            req.group,
            req.batch,
            req.auto_ack,
            req.wait,
            false,
            req.options,
        )
        .await
    }

    async fn ack(&self, ctx: ReqCtx, req: AckReq) -> Result<AckOut, RsmError> {
        self.ack_impl(ctx, req).await
    }

    async fn transaction(
        &self,
        ctx: ReqCtx,
        req: super::TxnReq,
    ) -> Result<super::TxnOut, RsmError> {
        self.txn_impl(ctx, req).await
    }

    async fn renew(&self, ctx: ReqCtx, req: RenewReq) -> Result<RenewOut, RsmError> {
        self.renew_impl(ctx, req).await
    }

    async fn dlq_head(&self, _ctx: ReqCtx, _req: DlqHeadReq) -> Result<DlqHeadOut, RsmError> {
        // The standalone DLQ-head command is not routed in phase 1 (§9.6); a
        // forced DLQ rides the ack path. Answered empty rather than erroring.
        Ok(DlqHeadOut { body: None })
    }

    async fn has_pending(&self, _ctx: ReqCtx, _req: PendingReq) -> Result<bool, RsmError> {
        // The indexed pending probe is a later WP's local stale read (§9.5). The
        // long-poll re-poll drives the pop directly meanwhile.
        Ok(true)
    }

    async fn depth(&self, ctx: ReqCtx, req: DepthReq) -> Result<DepthOut, RsmError> {
        let store = self.store.clone();
        let tenant = ctx.tenant;
        let queue = req.queue.clone();
        let now = wall_micros();
        let result = tokio::task::spawn_blocking(move || {
            store.read(|r| phase2::depth_json(r, &tenant, &req.queue, req.group.as_deref(), now))
        })
        .await
        .map_err(|e| RsmError::Internal(format!("depth read: {e}")))?
        .map_err(read_error)?;
        let Some(value) = result else {
            return Err(RsmError::Rejected {
                code: "queue_not_found".to_string(),
                message: format!("queue '{}' not found", queue),
            });
        };
        Ok(DepthOut {
            pending: value
                .get("pending")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0),
        })
    }

    async fn kv(&self, ctx: ReqCtx, req: KvReq) -> Result<KvOut, KvFailure> {
        self.kv_impl(ctx, req).await
    }

    async fn kv_list(&self, ctx: ReqCtx, req: KvListReq) -> Result<String, KvFailure> {
        self.kv_list_impl(ctx, req).await
    }

    async fn kv_namespaces(&self, ctx: ReqCtx) -> Result<String, KvFailure> {
        self.kv_namespaces_impl(ctx).await
    }

    async fn timers_apply(&self, ctx: ReqCtx, req: TimersReq) -> Result<TimersOut, RsmError> {
        self.timers_apply_impl(ctx, req).await
    }

    async fn timer_peek(&self, ctx: ReqCtx, req: TimerPeekReq) -> Result<TimerReadOut, RsmError> {
        self.timer_peek_impl(ctx, req).await
    }

    async fn timers_list(&self, ctx: ReqCtx, req: TimersListReq) -> Result<TimerReadOut, RsmError> {
        self.timers_list_impl(ctx, req).await
    }

    async fn timers_count(
        &self,
        ctx: ReqCtx,
        req: TimersCountReq,
    ) -> Result<TimerReadOut, RsmError> {
        self.timers_count_impl(ctx, req).await
    }

    async fn api(&self, ctx: ReqCtx, req: ApiReq) -> Result<ApiOut, RsmError> {
        self.api_impl(ctx, req).await
    }

    fn health(&self) -> RaftHealth {
        let m = self.repl.metrics();
        let role = self.repl.role();
        RaftHealth {
            role: match role {
                crate::rsm::replicator::Role::Leader { .. } => "leader".into(),
                crate::rsm::replicator::Role::Follower { .. } => "follower".into(),
                crate::rsm::replicator::Role::Learner => "learner".into(),
                crate::rsm::replicator::Role::Candidate => "candidate".into(),
                crate::rsm::replicator::Role::Stopped => "stopped".into(),
            },
            leader_known: m.leader.is_some()
                && !matches!(role, crate::rsm::replicator::Role::Stopped),
            term: m.term,
            applied: m.applied_index,
            commit: m.committed_index,
            lag_ms: 0,
            storage_ready: !matches!(role, crate::rsm::replicator::Role::Stopped),
        }
    }

    fn notifier(&self) -> Option<&Arc<Notifier>> {
        Some(&self.notifier)
    }
}

// ---------------------------------------------------------------------------
// The builder hook (WP-1.7a left it; the boot paths register this)
// ---------------------------------------------------------------------------

/// The [`super::RsmBuilder`] the binary and embedded boot register through
/// [`super::set_builder`]. A store or replicator that cannot open at boot is
/// fatal (the node cannot serve without its state); it is NOT registered in the
/// unit-test binary, so the WP-1.7a seam tests keep the `NotReady` stub and the
/// integration tests build [`RaftFacade`] directly.
pub fn real_builder(ctx: &RsmBuildCtx) -> Arc<dyn Rsm> {
    // PERF-E: pin the apply-side `record` dedup authority ONCE, at the
    // production boot seam only (unit tests build `RaftFacade` directly and
    // never reach here, so they keep the `rows` default that the existing
    // apply/store tests assert against). The planner side is resolved
    // independently in `BatcherConfig::from_env`; both read the same
    // `QUEEN_RAFT_DEDUP_INDEX`, so a real node's write and read paths agree.
    crate::rsm::dedup::set_record_index_mode(crate::rsm::dedup::IndexMode::from_env());
    match RaftFacade::open(ctx) {
        Ok(f) => Arc::new(f),
        Err(e) => crate::obs::fatal(format!("raft storage failed to open: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// Splice bytes expected to be valid UTF-8 (payload JSON), lossy on the rare
/// invalid tail — the same policy as `handlers/data.rs::push_utf8`.
fn push_utf8(out: &mut String, bytes: &[u8]) {
    match std::str::from_utf8(bytes) {
        Ok(s) => out.push_str(s),
        Err(_) => out.push_str(&String::from_utf8_lossy(bytes)),
    }
}

/// Format epoch microseconds as the SP's UTC ISO-8601 shape
/// (`YYYY-MM-DDTHH:MM:SS.mmmZ`), the inverse of `util::parse_iso_ms`'s
/// `days_from_civil`. No date-time dependency (matching `util`).
fn iso_from_us(us: i64) -> String {
    const US_PER_DAY: i64 = 86_400_000_000;
    let days = us.div_euclid(US_PER_DAY);
    let rem = us.rem_euclid(US_PER_DAY); // µs into the day
    let (y, m, d) = civil_from_days(days);
    let secs = rem / 1_000_000;
    let ms = (rem / 1000) % 1000;
    let hh = secs / 3600;
    let mm = (secs / 60) % 60;
    let ss = secs % 60;
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{ms:03}Z")
}

/// Howard Hinnant's `civil_from_days`: days since 1970-01-01 → (year, month,
/// day) in the proleptic Gregorian calendar.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m: i64 = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m as u32, d)
}
