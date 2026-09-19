//! `LocalReplicator` (PLAN_RAFT.md §12.2): a single node, its own write-ahead
//! log, no network and no elections. The rest of the RSM cannot tell it from
//! the Raft adapter (§12.1); it is what phases 1 and 2 run on, and what
//! embedded mode keeps.
//!
//! # How a propose flows
//!
//! ```text
//!   propose(bytes) ───── Pending ─────▶ writer thread (std)
//!                                          │  group commit + ONE fsync (log.rs)
//!                                          │  register waiter[index]
//!                                          ▼
//!                                    apply channel ──▶ apply thread (WP-1.4)
//!                                          │  execute entry, page-cache write
//!                                          ▼
//!                                    Notify::applied(index) ── resolve waiter
//!   propose ◀───────────────── AppliedAt{index, term} ────────────────┘
//! ```
//!
//! The log fsync happens BEFORE the entry reaches apply, so an acknowledged
//! entry survives a crash through the log even though the store commit between
//! durable points is not durable (§11.3, I4). The waiter is registered before
//! the entry is handed to apply, so `Notify::applied` never races ahead of it.
//!
//! # Threads (I15)
//!
//! - the **writer** thread owns the [`LogStore`], batches proposals into one
//!   group, fsyncs once, and drops log files behind a durable point. Blocking
//!   I/O on a `std` thread, never a tokio worker.
//! - the **apply** thread is WP-1.4's, spawned here with a [`ReplNotify`] that
//!   resolves waiters, advances the applied index, wakes long-polls and asks
//!   the writer to truncate the log.
//! - `propose` runs on a tokio task: it sends on an UNBOUNDED std channel to
//!   the writer (never blocks a worker) and awaits a `oneshot` under the
//!   deadline. No `std::sync::Mutex` is ever held across the `.await`.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{
    Receiver as StdReceiver, RecvTimeoutError, Sender as StdSender, SyncSender, TryRecvError,
};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{oneshot, watch};
// `apply::Notify` (the apply-side trait) is in scope below, so the tokio
// primitive is aliased to keep the two names apart (PERF-G).
use tokio::sync::Notify as ApplyWake;

use crate::rsm::apply::{self, ApplyConfig, ApplyStats, Committed, Notify};
use crate::rsm::entry::decode_entry;
use crate::rsm::segments;
use crate::rsm::store::{Store, TypedReads};

use super::log::{LogOptions, LogStore, SyncHandle, LOG_TERM};
use super::{
    AppliedAt, Membership, MembershipChange, NodeId, ProposeError, ReplError, ReplMetrics,
    Replicator, Role,
};

/// One group's cap: at most this many entries per fsync.
const GROUP_COMMIT_MAX: usize = 4096;
/// …or this many bytes, whichever comes first.
const GROUP_COMMIT_BYTES: usize = 4 * 1024 * 1024;
/// The bounded channel between the writer and the apply thread (§3.3).
const DEFAULT_APPLY_CHANNEL: usize = 256;
/// The writer→syncer channel is a rendezvous (capacity 0): the writer hands one
/// group across only when the syncer is ready to take it, i.e. once the syncer
/// has finished the PREVIOUS group's fsync (PERF-G, `QUEEN_RAFT_WRITER_PIPELINE`).
/// This preserves group-commit batching — while the syncer fsyncs group N the
/// writer BLOCKS on the hand-off of N+1, so the batcher's next in-flight entries
/// pile in the command channel and the writer drains them all into ONE group N+2
/// when the syncer frees, instead of racing ahead forming one-entry groups (a
/// buffered channel let the writer outrun the batcher and lost the batching).
const WRITER_PIPELINE_DEPTH: usize = 0;

/// Long-poll wakes (§9.5): the apply thread reports work for a
/// `(tenant, queue, group)`, and the node's parked long-polls on that key
/// wake. `LocalReplicator` forwards `apply::Notify::wake` here; WP-1.7 wires
/// the real one. Phase-1 tests use [`NoWaker`].
pub trait Waker: Send + Sync {
    fn wake(&self, tenant: &str, queue: &str, group: Option<&str>);
}

/// A waker that drops every wake. Boot before the seam, and every test that
/// does not assert on wakes.
pub struct NoWaker;

impl Waker for NoWaker {
    fn wake(&self, _tenant: &str, _queue: &str, _group: Option<&str>) {}
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

struct Shared {
    node_id: NodeId,
    applied_index: AtomicU64,
    applied_term: AtomicU64,
    durable_index: AtomicU64,
    last_log_index: AtomicU64,
    proposals: AtomicU64,
    log_files: AtomicU64,
    log_bytes: AtomicU64,
    /// Set once the writer hits a fatal log error; every later propose is
    /// `Fatal` and the role is `Stopped`.
    poisoned: Mutex<Option<String>>,
    /// index → the propose waiting on that entry's local apply.
    waiters: Mutex<HashMap<u64, oneshot::Sender<AppliedAt>>>,
    /// PERF-G (`QUEEN_RAFT_DRIVER_NOTIFY`): pulsed on the apply thread each time
    /// `applied_index` advances, so the batcher can wake and resolve the freed
    /// pipeline slots directly. Node-local, never state.
    applied_notify: Arc<ApplyWake>,
}

impl Shared {
    fn poison(&self, why: &str) {
        let mut g = self.poisoned.lock().expect("poison lock");
        if g.is_none() {
            tracing::error!(target: "rsm", why, "local replicator poisoned; node stops");
            *g = Some(why.to_string());
        }
    }

    fn is_poisoned(&self) -> Option<String> {
        self.poisoned.lock().expect("poison lock").clone()
    }
}

// ---------------------------------------------------------------------------
// The notifier the apply thread calls
// ---------------------------------------------------------------------------

/// The `apply::Notify` a `LocalReplicator` installs on the apply thread. Every
/// method runs ON THE APPLY THREAD and does no blocking work (I15): a brief
/// mutex and a non-blocking `oneshot` send.
///
/// It deliberately holds NO handle to the writer. `durable` only bumps an
/// atomic the writer polls between groups; if it held a clone of the writer's
/// command sender, that sender would keep the writer alive after
/// [`LocalReplicator::shutdown`] dropped its own, and the two threads would
/// wait on each other forever (the writer for the apply thread to drop the
/// sender, the apply thread for the writer to drop `apply_tx`).
struct ReplNotify {
    shared: Arc<Shared>,
    waker: Arc<dyn Waker>,
}

impl Notify for ReplNotify {
    fn applied(&self, index: u64, term: u64, _commands: &[crate::rsm::entry::CommandRecord]) {
        // Apply is strictly in index order, so this only ever moves forward;
        // `fetch_max` is defence, not need.
        self.shared.applied_index.fetch_max(index, Ordering::AcqRel);
        self.shared.applied_term.store(term, Ordering::Release);
        if let Some(tx) = self.shared.waiters.lock().expect("waiters").remove(&index) {
            let _ = tx.send(AppliedAt { index, term });
        }
        // PERF-G: the applied index advanced; wake the driver so it reuses the
        // freed pipeline slot at once (`QUEEN_RAFT_DRIVER_NOTIFY`). One permit
        // is stored if the driver is momentarily busy, so no wake is lost, and
        // the driver reads the atomic — never a per-entry event — so coalesced
        // advances resolve every passed entry in one pass.
        self.shared.applied_notify.notify_one();
    }

    fn wake(&self, tenant: &str, queue: &str, group: Option<&str>) {
        self.waker.wake(tenant, queue, group);
    }

    fn durable(&self, index: u64) {
        // The writer drops log files behind this between groups. No channel,
        // so this notifier holds nothing that could outlive shutdown.
        self.shared.durable_index.fetch_max(index, Ordering::AcqRel);
    }
}

// ---------------------------------------------------------------------------
// The writer thread
// ---------------------------------------------------------------------------

/// One proposal on its way to the log.
struct Pending {
    bytes: Bytes,
    entry: crate::rsm::entry::Entry,
    done: oneshot::Sender<AppliedAt>,
    /// When `propose` submitted this (PERF-1): the `proposed_to_committed`
    /// histogram measures from here to the group's fsync. `None` when the
    /// instrumentation is off (`QUEEN_RAFT_METRICS=0`), so the knob prices the
    /// clock read at propose too, not just the histogram write.
    proposed_at: Option<std::time::Instant>,
}

/// How often the writer wakes when idle, to drop log files behind a durable
/// point and to notice a disconnected command channel promptly.
const WRITER_TICK: Duration = Duration::from_millis(100);

/// Where the writer sends a written group. `Direct` fsyncs inline and hands the
/// entries to apply on the writer thread (the pre-PERF-G path). `Pipelined`
/// hands the still-unfsynced group to the syncer thread, which fsyncs it and
/// only then acknowledges it (`QUEEN_RAFT_WRITER_PIPELINE`).
enum WriterSink {
    Direct(SyncSender<Committed>),
    Pipelined(SyncSender<SyncJob>),
}

/// One group written but not yet fsynced, handed from the writer to the syncer
/// (PERF-G). It carries the `Pending`s (their waiters and entries) so the
/// syncer can acknowledge them after the fsync, and the log gauges the syncer
/// republishes (the writer owns the [`LogStore`], so it reads them off it).
struct SyncJob {
    pending: Vec<Pending>,
    first_index: u64,
    handle: SyncHandle,
    log_files: u64,
    log_bytes: u64,
}

struct Writer {
    log: LogStore,
    sink: WriterSink,
    shared: Arc<Shared>,
    role_tx: watch::Sender<Role>,
    /// The highest durable index this writer has already truncated behind.
    last_truncated: u64,
}

impl Writer {
    fn run(mut self, rx: StdReceiver<Pending>) {
        loop {
            let mut pending: Vec<Pending> = Vec::new();
            let mut closed = false;
            match rx.recv_timeout(WRITER_TICK) {
                Ok(p) => pending.push(p),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            let mut bytes: usize = pending
                .iter()
                .map(|p| p.bytes.len() + super::log::FRAME_OVERHEAD)
                .sum();
            while pending.len() < GROUP_COMMIT_MAX && bytes < GROUP_COMMIT_BYTES {
                match rx.try_recv() {
                    Ok(p) => {
                        bytes += p.bytes.len() + super::log::FRAME_OVERHEAD;
                        pending.push(p);
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        closed = true;
                        break;
                    }
                }
            }

            if !pending.is_empty() && !self.commit(pending) {
                return; // poisoned: the node stops here
            }
            self.maybe_truncate();
            if closed {
                break;
            }
        }
    }

    /// Drop log files behind the durable point the apply thread reported, if it
    /// advanced since the last pass.
    fn maybe_truncate(&mut self) {
        let durable = self.shared.durable_index.load(Ordering::Acquire);
        if durable <= self.last_truncated {
            return;
        }
        match self.log.truncate_through(durable) {
            Ok(n) if n > 0 => self.refresh_log_metrics(),
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(target: "rsm", error = %e, "local log truncation failed");
            }
        }
        self.last_truncated = durable;
    }

    /// Write one group and, in `Direct` mode, fsync it and hand its entries to
    /// apply; in `Pipelined` mode write it and hand the unfsynced group to the
    /// syncer, which flushes and acknowledges it. Returns false when the log,
    /// the apply thread or the syncer failed and the node must stop.
    fn commit(&mut self, pending: Vec<Pending>) -> bool {
        // PERF-1/PERF-G: group size and the writer-pickup leg, before any write.
        if crate::rsm::timing::enabled() {
            let tm = crate::rsm::timing::metrics();
            let group_bytes: u64 = pending.iter().map(|p| p.bytes.len() as u64).sum();
            tm.group_entries.record(pending.len() as u64);
            tm.group_bytes.record(group_bytes);
            // proposed → the writer dequeued this group and is about to write it
            // (the wait behind the previous group). ≈ proposed → fsync start.
            for p in &pending {
                if let Some(at) = p.proposed_at {
                    tm.writer_pickup.record_dur(at.elapsed());
                }
            }
        }

        match &self.sink {
            WriterSink::Direct(_) => {
                let slices: Vec<&[u8]> = pending.iter().map(|p| p.bytes.as_ref()).collect();
                let first_index = match self.log.append_group(&slices) {
                    Ok(i) => i,
                    Err(e) => {
                        drop(slices);
                        self.fail(&format!("local log append failed: {e}"), pending);
                        return false;
                    }
                };
                drop(slices);
                self.refresh_log_metrics();
                let (files, bytes) = (self.log.file_count() as u64, self.log.bytes());
                let WriterSink::Direct(apply_tx) = &self.sink else {
                    unreachable!()
                };
                if !handoff_group(&self.shared, apply_tx, pending, first_index, files, bytes) {
                    self.poison("apply thread gone");
                    return false;
                }
                true
            }
            WriterSink::Pipelined(_) => {
                let slices: Vec<&[u8]> = pending.iter().map(|p| p.bytes.as_ref()).collect();
                let (first_index, handle) = match self.log.append_group_deferred(&slices) {
                    Ok(v) => v,
                    Err(e) => {
                        drop(slices);
                        self.fail(&format!("local log append failed: {e}"), pending);
                        return false;
                    }
                };
                drop(slices);
                self.refresh_log_metrics();
                let job = SyncJob {
                    pending,
                    first_index,
                    handle,
                    log_files: self.log.file_count() as u64,
                    log_bytes: self.log.bytes(),
                };
                let WriterSink::Pipelined(job_tx) = &self.sink else {
                    unreachable!()
                };
                if job_tx.send(job).is_err() {
                    // The syncer stopped (a fsync or apply failure it already
                    // poisoned on). Stop the writer too.
                    self.poison("log syncer gone");
                    return false;
                }
                true
            }
        }
    }

    /// The log write itself failed: drop the pending waiters (their proposes
    /// see a closed channel → `Fatal`) and stop.
    fn fail(&self, why: &str, pending: Vec<Pending>) {
        drop(pending); // dropping each `done` closes its oneshot
        self.poison(why);
    }

    fn poison(&self, why: &str) {
        self.shared.poison(why);
        let _ = self.role_tx.send(Role::Stopped);
    }

    fn refresh_log_metrics(&self) {
        self.shared
            .log_files
            .store(self.log.file_count() as u64, Ordering::Release);
        self.shared
            .log_bytes
            .store(self.log.bytes(), Ordering::Release);
    }
}

/// The post-fsync handoff, shared by the direct writer and the pipeline syncer.
/// The group is now durable in the log (its fsync returned); price the commit
/// leg, publish the log tip, then register each waiter and hand its entry to
/// apply IN INDEX ORDER (I4). Returns false when the apply thread is gone, so
/// the caller poisons and stops.
fn handoff_group(
    shared: &Arc<Shared>,
    apply_tx: &SyncSender<Committed>,
    pending: Vec<Pending>,
    first_index: u64,
    log_files: u64,
    log_bytes: u64,
) -> bool {
    // The group is fsynced (committed); price the proposed → committed leg
    // (gated: `proposed_at` is `None` and the clock read skipped when off).
    if crate::rsm::timing::enabled() {
        let tm = crate::rsm::timing::metrics();
        for p in &pending {
            if let Some(at) = p.proposed_at {
                tm.proposed_to_committed.record_dur(at.elapsed());
            }
        }
    }
    let last_index = first_index + pending.len() as u64 - 1;
    shared.last_log_index.store(last_index, Ordering::Release);
    shared.log_files.store(log_files, Ordering::Release);
    shared.log_bytes.store(log_bytes, Ordering::Release);

    // §13.5 `commit.before_apply`: the group is committed by quorum (a single
    // voter here) and durable in the log, and not one entry of it has been
    // applied. A crash here loses nothing: apply replays the whole group from
    // the log on restart, exactly once (I4, I13).
    crate::rsm::faults::hit("commit.before_apply");

    let mut index = first_index;
    for p in pending {
        // Register BEFORE the send, so `Notify::applied(index)` — which can
        // only fire after apply consumes this `Committed` — always finds the
        // waiter.
        shared
            .waiters
            .lock()
            .expect("waiters")
            .insert(index, p.done);
        let committed = Committed {
            index,
            term: LOG_TERM,
            entry: p.entry,
        };
        // PERF-1: mark the channel depth so the apply thread can read it.
        crate::rsm::timing::apply_channel_send();
        if apply_tx.send(committed).is_err() {
            // The apply thread is gone (it refused an entry and stopped, §12.1
            // Fatal). The waiter we just registered will never resolve; drop it
            // so the propose sees a closed channel.
            shared.waiters.lock().expect("waiters").remove(&index);
            return false;
        }
        index += 1;
    }
    true
}

/// The fsync half of the write/fsync pipeline (PERF-G,
/// `QUEEN_RAFT_WRITER_PIPELINE`). It owns the apply-channel sender: the writer
/// hands it a written-but-unfsynced group per index-ordered [`SyncJob`], and it
/// flushes each group and only then acknowledges its entries — so an entry is
/// answered only after a fsync that covered its bytes (I4), while the writer
/// forms and writes the NEXT group during this fsync.
struct Syncer {
    apply_tx: SyncSender<Committed>,
    shared: Arc<Shared>,
    role_tx: watch::Sender<Role>,
}

impl Syncer {
    fn run(self, rx: StdReceiver<SyncJob>) {
        // Iterates until the writer drops the job sender (a clean shutdown) or
        // this thread poisons.
        for job in rx {
            let SyncJob {
                pending,
                first_index,
                handle,
                log_files,
                log_bytes,
            } = job;
            if let Err(e) = handle.sync() {
                // The barrier failed: the group is NOT durable. Drop its
                // waiters (their proposes see a closed channel → Fatal) and
                // stop the node — the same disposition the inline writer's
                // `fail` takes on a write error.
                drop(pending);
                self.poison(&format!("local log fsync failed: {e}"));
                return;
            }
            // §13.5 `log.flushed`: the group is durable in the raft log.
            crate::rsm::faults::hit("log.flushed");
            if !handoff_group(
                &self.shared,
                &self.apply_tx,
                pending,
                first_index,
                log_files,
                log_bytes,
            ) {
                self.poison("apply thread gone");
                return;
            }
        }
    }

    fn poison(&self, why: &str) {
        self.shared.poison(why);
        let _ = self.role_tx.send(Role::Stopped);
    }
}

// ---------------------------------------------------------------------------
// The replicator
// ---------------------------------------------------------------------------

/// How to open a [`LocalReplicator`]: the log and segment directories, the
/// engine options, and the apply-thread cadences. Resolved at boot (WP-1.7).
pub struct OpenConfig {
    pub node_id: NodeId,
    pub log_dir: PathBuf,
    pub log_opts: LogOptions,
    pub seg_root: PathBuf,
    pub seg_opts: segments::Options,
    pub apply_cfg: ApplyConfig,
    /// Capacity of the bounded channel to the apply thread.
    pub apply_channel_capacity: usize,
    /// How long `open` waits for apply to replay the log after the store's
    /// durable index (§11.5). Boot only.
    pub replay_deadline: Duration,
    /// `QUEEN_RAFT_WRITER_PIPELINE` (PERF-G, **default OFF** — the round-3 A/B
    /// showed it regresses push p50, see [`writer_pipeline_from_env`]): run the
    /// log write and its group fsync on two threads, so the writer forms and
    /// writes group N+1 while the syncer fsyncs group N. Entries are still
    /// acknowledged only after their own group's fsync (I4), and the syncer
    /// processes groups in index order (so apply sees them in order). Off (the
    /// default): the single-thread write-then-fsync writer.
    pub writer_pipeline: bool,
}

impl OpenConfig {
    pub fn new(node_id: NodeId, dir: PathBuf) -> OpenConfig {
        OpenConfig {
            node_id,
            log_dir: dir.join("log"),
            log_opts: LogOptions::from_env(),
            seg_root: dir.join("seg"),
            seg_opts: segments::Options::from_env(),
            apply_cfg: ApplyConfig::from_env(),
            apply_channel_capacity: DEFAULT_APPLY_CHANNEL,
            replay_deadline: Duration::from_secs(120),
            writer_pipeline: writer_pipeline_from_env(),
        }
    }
}

/// Resolve `QUEEN_RAFT_WRITER_PIPELINE`. **Default OFF (PERF-G round 3):** the
/// laptop A/B showed the write/fsync pipeline REGRESSES push p50 (10.3 → 14.1 ms
/// at A20k, both channel shapes) because the two-thread split cannot beat the
/// inline writer's natural group-commit batching, and the finer `writer_pickup`
/// stage shows there is nothing for it to overlap where it would matter: on the
/// VM the writer picks a group up in ≈0 ms (PERF-2: proposed→committed ≈
/// log_fsync), so the whole `proposed→committed` leg IS the fsync, which the
/// pipeline cannot shorten. It only helps a box whose fsync is expensive enough
/// to make the writer queue behind it AND whose extra fsyncs are cheap — neither
/// holds here. Kept as a knob (`=1` turns it on) and proven I2-transparent, for
/// a box where the trade-off flips. Only "1"/"true"/"on"/"yes" turns it on.
/// `pub(crate)` so the replicator tests build an `OpenConfig` honouring the knob.
pub(crate) fn writer_pipeline_from_env() -> bool {
    match std::env::var("QUEEN_RAFT_WRITER_PIPELINE") {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "on" | "yes"
        ),
        Err(_) => false,
    }
}

/// A single-node replicator over a local log and the WP-1.4 apply thread.
pub struct LocalReplicator<S: Store> {
    shared: Arc<Shared>,
    role_rx: watch::Receiver<Role>,
    /// Proposals and durable-point notices to the writer. Unbounded, so
    /// `propose` never blocks a tokio worker.
    cmd_tx: StdSender<Pending>,
    /// Kept `Some` until [`LocalReplicator::shutdown`] or `Drop`.
    writer_join: Option<JoinHandle<()>>,
    /// The write/fsync-pipeline syncer thread (PERF-G), `None` when
    /// `QUEEN_RAFT_WRITER_PIPELINE` is off. Joined between the writer and the
    /// apply thread at shutdown.
    syncer_join: Option<JoinHandle<()>>,
    apply_join: Option<JoinHandle<apply::Result<ApplyStats>>>,
    store: Arc<S>,
    /// The segment reader the apply thread published at open (WP-1.7c). The
    /// facade reads pop payloads through it, off the SAME file set the writer
    /// keeps appending to (§7.5). Always set once `open` returns.
    reader: segments::Reader,
}

impl<S: Store + 'static> LocalReplicator<S> {
    /// Open the replicator over `store` (recovered by the apply thread) and the
    /// local log under `cfg.log_dir`. Replays every log entry after the store's
    /// durable index into apply (§11.5), waits for apply to catch up, then
    /// begins accepting proposals. A boot call: it does blocking I/O and must
    /// not run on a tokio worker.
    pub fn open(
        store: Arc<S>,
        cfg: OpenConfig,
        waker: Arc<dyn Waker>,
        clock: Arc<dyn apply::Clock>,
    ) -> io::Result<LocalReplicator<S>> {
        // 1. The log: recover it, truncate any torn tail.
        let (log, log_rec) = LogStore::open(&cfg.log_dir, cfg.log_opts)?;
        let last_log = log.last_index();

        // 2. Where the store reopened (§11.5 step 2). The apply thread reads
        //    the same values inside its own recovery; reading them here too is
        //    a read transaction, no conflict.
        let (store_applied, store_term, store_durable) = store
            .read(|r| Ok((r.applied_index()?, r.applied_term()?, r.durable_index()?)))
            .map_err(|e| io::Error::other(format!("read store recovery point: {e}")))?;

        if store_applied > last_log {
            // The store is ahead of the log: impossible if the log is the WAL,
            // and a sign the log directory was truncated or swapped. Refuse
            // rather than silently lose the tail (§0.3 "refuse, never guess").
            return Err(io::Error::other(format!(
                "store applied index {store_applied} is ahead of the log's last index {last_log}"
            )));
        }

        let shared = Arc::new(Shared {
            node_id: cfg.node_id,
            applied_index: AtomicU64::new(store_applied),
            applied_term: AtomicU64::new(store_term),
            durable_index: AtomicU64::new(store_durable),
            last_log_index: AtomicU64::new(last_log),
            proposals: AtomicU64::new(0),
            log_files: AtomicU64::new(log.file_count() as u64),
            log_bytes: AtomicU64::new(log.bytes()),
            poisoned: Mutex::new(None),
            waiters: Mutex::new(HashMap::new()),
            applied_notify: Arc::new(ApplyWake::new()),
        });

        // 3. Always leader, term 1, until it stops.
        let (role_tx, role_rx) = watch::channel(Role::Leader { term: LOG_TERM });

        // 4. The apply thread, with our notifier.
        let (apply_tx, apply_rx) = apply::channel(cfg.apply_channel_capacity);
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel::<Pending>();
        let notify: Arc<dyn Notify> = Arc::new(ReplNotify {
            shared: shared.clone(),
            waker,
        });
        // The apply thread owns the segment writer; publish its reader here so
        // the facade can read pop payloads off the same live file set (§7.5,
        // WP-1.7c). Set once, before the first entry replays.
        let reader_sink: Arc<std::sync::OnceLock<segments::Reader>> =
            Arc::new(std::sync::OnceLock::new());
        let apply_join = apply::spawn_with_reader(
            store.clone(),
            cfg.seg_root.clone(),
            cfg.seg_opts,
            cfg.apply_cfg,
            notify,
            clock,
            apply_rx,
            Some(reader_sink.clone()),
        );

        // 5. Replay: every entry after the store's durable index, in order.
        //    Apply skips whatever the store already holds (idempotence,
        //    §11.5). Sends block only if apply is momentarily behind, which is
        //    fine on this boot thread.
        let mut replayed = 0u64;
        let replay = log.scan_from(store_durable + 1, &mut |index, term, body| {
            let entry = decode_entry(body).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("replay: entry {index} does not decode: {e:?}"),
                )
            })?;
            apply_tx
                .send(Committed { index, term, entry })
                .map_err(|_| io::Error::other("replay: apply thread exited"))?;
            replayed += 1;
            Ok(())
        });
        let replay = match replay {
            Ok(n) => n,
            Err(e) => {
                // Tear the apply thread down before returning, so the store's
                // env can be reopened.
                drop(apply_tx);
                let _ = apply_join.join();
                return Err(e);
            }
        };

        // 6. Wait for apply to reach the log's end. Past this the node may
        //    serve local stale reads (§11.5 step 6).
        if let Err(e) = wait_until_applied(&shared, last_log, cfg.replay_deadline, &apply_join) {
            drop(apply_tx);
            let _ = apply_join.join();
            return Err(e);
        }

        // The reader the apply thread published at `Applier::open` (WP-1.7c). It
        // is set before any entry replays, so it is available by now on the
        // replay>0 path; on the replay==0 path the thread may still be inside
        // `Applier::open`, so spin briefly (the apply thread has no reason to be
        // slow here, and a dead thread short-circuits).
        let reader = {
            let end = Instant::now() + cfg.replay_deadline;
            loop {
                if let Some(r) = reader_sink.get() {
                    break r.clone();
                }
                if apply_join.is_finished() {
                    drop(apply_tx);
                    let _ = apply_join.join();
                    return Err(io::Error::other(
                        "apply thread exited before it published the segment reader",
                    ));
                }
                if Instant::now() >= end {
                    drop(apply_tx);
                    let _ = apply_join.join();
                    return Err(io::Error::other(
                        "apply thread did not publish the segment reader in time",
                    ));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        };

        tracing::info!(
            target: "rsm",
            node = cfg.node_id,
            last_log,
            store_applied,
            store_durable,
            replayed = replay,
            log_files = log_rec.files,
            truncated_tail = log_rec.truncated_tail,
            "local replicator open",
        );

        // 7. Start the writer, which owns the log and accepts proposals from
        //    here on. In the direct path `apply_tx` moves into the writer: it is
        //    the only sender, so dropping `cmd_tx` at shutdown drains the
        //    writer, which drops `apply_tx`, closing the apply thread. In the
        //    pipeline path the syncer owns `apply_tx` instead, and the shutdown
        //    chain is `cmd_tx` → writer drops the job sender → syncer drains and
        //    drops `apply_tx` → apply thread closes (WP-1.11 F-2 order kept).
        let (sink, syncer_join) = if cfg.writer_pipeline {
            let (job_tx, job_rx) = std::sync::mpsc::sync_channel::<SyncJob>(WRITER_PIPELINE_DEPTH);
            let syncer = Syncer {
                apply_tx,
                shared: shared.clone(),
                role_tx: role_tx.clone(),
            };
            let syncer_join = std::thread::Builder::new()
                .name("queen-rsm-sync".into())
                .spawn(move || syncer.run(job_rx))
                .map_err(|e| io::Error::other(format!("spawn the log syncer: {e}")))?;
            (WriterSink::Pipelined(job_tx), Some(syncer_join))
        } else {
            (WriterSink::Direct(apply_tx), None)
        };
        let writer = Writer {
            log,
            sink,
            shared: shared.clone(),
            role_tx,
            last_truncated: store_durable,
        };
        let writer_join = std::thread::Builder::new()
            .name("queen-rsm-log".into())
            .spawn(move || writer.run(cmd_rx))
            .map_err(|e| io::Error::other(format!("spawn the log writer: {e}")))?;

        Ok(LocalReplicator {
            shared,
            role_rx,
            cmd_tx,
            writer_join: Some(writer_join),
            syncer_join,
            apply_join: Some(apply_join),
            store,
            reader,
        })
    }

    /// The node id.
    pub fn node_id(&self) -> NodeId {
        self.shared.node_id
    }

    /// The segment reader for pop payloads (§7.5, WP-1.7c). Cloneable and
    /// `Send + Sync`; the facade clones one per blocking-pool read.
    pub fn reader(&self) -> segments::Reader {
        self.reader.clone()
    }

    /// The last index the log holds (fsynced).
    pub fn last_log_index(&self) -> u64 {
        self.shared.last_log_index.load(Ordering::Acquire)
    }

    /// The last index a durable point covered.
    pub fn durable_index(&self) -> u64 {
        self.shared.durable_index.load(Ordering::Acquire)
    }

    /// Stop the node: drain and join the writer, then the apply thread. Returns
    /// the apply thread's stats and the store handle, whose only remaining
    /// reference this is — the caller may `Arc::try_unwrap` it to close the
    /// engine (heed needs an explicit close to reopen in one process).
    pub fn shutdown(mut self) -> io::Result<(ApplyStats, Arc<S>)> {
        let stats = self.stop()?;
        Ok((stats, self.store.clone()))
    }
}

impl<S: Store> LocalReplicator<S> {
    /// The join half of both shutdown and `Drop`. Independent of `S: 'static`,
    /// so `Drop` (which cannot add that bound) can call it.
    fn stop(&mut self) -> io::Result<ApplyStats> {
        // Dropping the command sender closes the writer's input; it finishes
        // its current group and returns, dropping `apply_tx`, which closes the
        // apply channel.
        //
        // Replace the live sender with a fresh, disconnected one so a second
        // call (Drop after shutdown) is a no-op.
        let (dead_tx, _dead_rx) = std::sync::mpsc::channel::<Pending>();
        let cmd_tx = std::mem::replace(&mut self.cmd_tx, dead_tx);
        drop(cmd_tx);
        if let Some(j) = self.writer_join.take() {
            j.join()
                .map_err(|_| io::Error::other("log writer thread panicked"))?;
        }
        // The writer has returned and dropped the job sender; the syncer now
        // drains its remaining groups and drops `apply_tx`, closing the apply
        // channel. Join it before the apply thread (PERF-G).
        if let Some(j) = self.syncer_join.take() {
            j.join()
                .map_err(|_| io::Error::other("log syncer thread panicked"))?;
        }
        match self.apply_join.take() {
            Some(j) => match j.join() {
                Ok(Ok(stats)) => Ok(stats),
                Ok(Err(e)) => Err(io::Error::other(format!("apply thread: {e}"))),
                Err(_) => Err(io::Error::other("apply thread panicked")),
            },
            None => Ok(ApplyStats::default()),
        }
    }
}

impl<S: Store> Drop for LocalReplicator<S> {
    fn drop(&mut self) {
        if self.writer_join.is_some() || self.syncer_join.is_some() || self.apply_join.is_some() {
            let _ = self.stop();
        }
    }
}

/// Poll the applied index until it reaches `target`, or the deadline passes, or
/// the apply thread dies. A boot-time spin with a short park; there is no
/// tokio context here.
fn wait_until_applied(
    shared: &Arc<Shared>,
    target: u64,
    deadline: Duration,
    apply_join: &JoinHandle<apply::Result<ApplyStats>>,
) -> io::Result<()> {
    let end = Instant::now() + deadline;
    loop {
        if shared.applied_index.load(Ordering::Acquire) >= target {
            return Ok(());
        }
        if let Some(why) = shared.is_poisoned() {
            return Err(io::Error::other(format!(
                "apply refused during replay: {why}"
            )));
        }
        if apply_join.is_finished() {
            return Err(io::Error::other(
                "apply thread exited before the log was replayed",
            ));
        }
        if Instant::now() >= end {
            return Err(io::Error::other(format!(
                "replay did not reach index {target} within {deadline:?} (at {})",
                shared.applied_index.load(Ordering::Acquire)
            )));
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[async_trait]
impl<S: Store + 'static> Replicator for LocalReplicator<S> {
    async fn propose(&self, entry: Bytes, deadline: Instant) -> Result<AppliedAt, ProposeError> {
        if let Some(why) = self.shared.is_poisoned() {
            return Err(ProposeError::Fatal(why));
        }
        // Decode once, for the `Committed` the writer hands to apply. The bytes
        // themselves go to the log unchanged. A proposal a valid batcher built
        // always decodes; a failure is a caller bug, refused (not fatal to the
        // node).
        let decoded = decode_entry(&entry)
            .map_err(|e| ProposeError::Refused(format!("proposal does not decode: {e:?}")))?;

        let (done_tx, done_rx) = oneshot::channel();
        let pending = Pending {
            bytes: entry,
            entry: decoded,
            done: done_tx,
            proposed_at: crate::rsm::timing::stamp(),
        };
        // Enqueue BEFORE awaiting the deadline, so even a past deadline still
        // puts the entry in flight (I3: a timed-out entry may still commit).
        if self.cmd_tx.send(pending).is_err() {
            return Err(ProposeError::Fatal("log writer gone".into()));
        }
        self.shared.proposals.fetch_add(1, Ordering::Relaxed);

        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), done_rx).await {
            Ok(Ok(at)) => Ok(at),
            // The waiter was dropped without resolving: the writer or the apply
            // thread stopped (§12.1 Fatal).
            Ok(Err(_)) => Err(ProposeError::Fatal(
                self.shared
                    .is_poisoned()
                    .unwrap_or_else(|| "apply resolver dropped".into()),
            )),
            // I3: the entry STAYS IN FLIGHT. It is already in the log pipeline;
            // it will apply, and `applied_index` will advance. The planner
            // holds it and does not plan the next cycle until then.
            Err(_elapsed) => Err(ProposeError::Timeout),
        }
    }

    fn role(&self) -> Role {
        *self.role_rx.borrow()
    }

    fn watch_role(&self) -> watch::Receiver<Role> {
        self.role_rx.clone()
    }

    fn applied_notify(&self) -> Option<Arc<ApplyWake>> {
        Some(self.shared.applied_notify.clone())
    }

    async fn read_barrier(&self, _deadline: Instant) -> Result<u64, ProposeError> {
        if let Some(why) = self.shared.is_poisoned() {
            return Err(ProposeError::Fatal(why));
        }
        // A single node is its own quorum: everything acknowledged is applied,
        // so the current applied index is a valid linearizable read index
        // (§9.4). No round trip.
        Ok(self.shared.applied_index.load(Ordering::Acquire))
    }

    fn applied_index(&self) -> u64 {
        self.shared.applied_index.load(Ordering::Acquire)
    }

    async fn transfer_leadership(
        &self,
        to: Option<NodeId>,
        _deadline: Instant,
    ) -> Result<(), ReplError> {
        match to {
            None => Ok(()),
            Some(n) if n == self.shared.node_id => Ok(()),
            Some(_) => Err(ReplError::Unsupported(
                "a single-node LocalReplicator has no peer to transfer to".into(),
            )),
        }
    }

    async fn membership(&self) -> Membership {
        Membership::single(self.shared.node_id)
    }

    async fn change_membership(
        &self,
        _change: MembershipChange,
        _deadline: Instant,
    ) -> Result<(), ReplError> {
        Err(ReplError::Unsupported(
            "membership changes need the Raft adapter (phase 3+)".into(),
        ))
    }

    fn metrics(&self) -> ReplMetrics {
        let applied = self.shared.applied_index.load(Ordering::Acquire);
        let last_log = self.shared.last_log_index.load(Ordering::Acquire);
        let stopped = self.shared.is_poisoned().is_some();
        ReplMetrics {
            term: LOG_TERM,
            leader: Some(self.shared.node_id),
            is_leader: !stopped,
            last_log_index: last_log,
            committed_index: last_log,
            applied_index: applied,
            durable_index: self.shared.durable_index.load(Ordering::Acquire),
            inflight: last_log.saturating_sub(applied),
            proposals: self.shared.proposals.load(Ordering::Relaxed),
            log_files: self.shared.log_files.load(Ordering::Acquire),
            log_bytes: self.shared.log_bytes.load(Ordering::Acquire),
        }
    }
}
