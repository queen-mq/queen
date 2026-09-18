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

use crate::rsm::apply::{self, ApplyConfig, ApplyStats, Committed, Notify};
use crate::rsm::entry::decode_entry;
use crate::rsm::segments;
use crate::rsm::store::{Store, TypedReads};

use super::log::{LogOptions, LogStore, LOG_TERM};
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
}

/// How often the writer wakes when idle, to drop log files behind a durable
/// point and to notice a disconnected command channel promptly.
const WRITER_TICK: Duration = Duration::from_millis(100);

struct Writer {
    log: LogStore,
    apply_tx: SyncSender<Committed>,
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

    /// Write one group, fsync once, register waiters, hand entries to apply in
    /// index order. Returns false when the log or the apply thread failed and
    /// the node must stop.
    fn commit(&mut self, pending: Vec<Pending>) -> bool {
        let slices: Vec<&[u8]> = pending.iter().map(|p| p.bytes.as_ref()).collect();
        let first_index = match self.log.append_group(&slices) {
            Ok(i) => i,
            Err(e) => {
                self.fail(&format!("local log append failed: {e}"), pending);
                return false;
            }
        };
        let last_index = first_index + pending.len() as u64 - 1;
        self.shared
            .last_log_index
            .store(last_index, Ordering::Release);
        self.refresh_log_metrics();

        let mut index = first_index;
        for p in pending {
            // Register BEFORE the send, so `Notify::applied(index)` — which can
            // only fire after apply consumes this `Committed` — always finds
            // the waiter.
            self.shared
                .waiters
                .lock()
                .expect("waiters")
                .insert(index, p.done);
            let committed = Committed {
                index,
                term: LOG_TERM,
                entry: p.entry,
            };
            if self.apply_tx.send(committed).is_err() {
                // The apply thread is gone (it refused an entry and stopped,
                // §12.1 Fatal). The waiter we just registered will never
                // resolve; drop it so the propose sees a closed channel.
                self.shared.waiters.lock().expect("waiters").remove(&index);
                self.poison("apply thread gone");
                return false;
            }
            index += 1;
        }
        true
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
        }
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
        //    here on. `apply_tx` moves into it: it is now the only sender, so
        //    dropping `cmd_tx` at shutdown drains the writer, which drops
        //    `apply_tx`, which closes the apply thread.
        let writer = Writer {
            log,
            apply_tx,
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
        if self.writer_join.is_some() || self.apply_join.is_some() {
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
