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
//! # Phase C: the queue logs are the WAL (`QUEEN_RAFT_QLOG` on)
//!
//! With the qlog knob on the writer does not append to the raft log at all: it
//! ASSIGNS the indexes itself, writes every entry (payload records + one
//! payload-free entry record per touched queue log, or the system log) into
//! the per-queue logs, fsyncs every touched log once, and only then hands the
//! group to apply ([`Writer::write_qlog_group`]). `open` replays from the queue
//! logs (`QLogSet::scan_entries`: merged, deduped, gapless, complete entries
//! only), truncates the unacknowledged tail, and continues the indexes right
//! after the replayed prefix. `Notify::durable` raises the queue logs' recovery
//! floor so retention never drops a file recovery still needs. With the knob
//! off, everything below is the raft-log path exactly as before.
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
use crate::rsm::entry::{decode_entry, encode_entry_payload_free, Entry};
use crate::rsm::qlog::codec::StoredPayload;
use crate::rsm::qlog::set::{QLogSet, SYSTEM_QUEUE_ID};
use crate::rsm::qlog::{EntryInput, RecordInput, WriteRecord};
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
    /// Phase C: the queue logs' recovery floor (`QUEEN_RAFT_QLOG` on), raised
    /// to each durable index so retention never unlinks a file recovery still
    /// replays from. `None` with the knob off. An atomic, like `durable_index`:
    /// it holds nothing that could outlive shutdown.
    qlog_floor: Option<Arc<AtomicU64>>,
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
        // Phase C: the store now durably holds everything through `index`, so
        // recovery replays from `index + 1` and the queue logs may give up
        // files wholly at or below it (never above). Called on the apply thread
        // only after the durable store commit landed (`durable_point_inner`).
        if let Some(floor) = &self.qlog_floor {
            floor.fetch_max(index, Ordering::AcqRel);
        }
    }
}

// ---------------------------------------------------------------------------
// The writer thread
// ---------------------------------------------------------------------------

/// One proposal on its way to the log.
struct Pending {
    bytes: Bytes,
    /// The entry's size for the writer's group cap and metrics: `bytes.len()`,
    /// or an estimate of the payload when `propose_entry` sent no bytes.
    size: usize,
    /// Shared with the batcher's in-flight list on the `propose_entry` path (no
    /// decode, no payload copy); replaced by the payload-free form after the
    /// queue-log write.
    entry: Arc<crate::rsm::entry::Entry>,
    done: oneshot::Sender<AppliedAt>,
    /// When `propose` submitted this (PERF-1): the `proposed_to_committed`
    /// histogram measures from here to the group's fsync. `None` when the
    /// instrumentation is off (`QUEEN_RAFT_METRICS=0`), so the knob prices the
    /// clock read at propose too, not just the histogram write.
    proposed_at: Option<std::time::Instant>,
    /// The qlog codec's work on this entry's `Append` blobs, in effect order,
    /// started at propose so it overlaps the writer's previous fsync. Empty
    /// off the qlog path.
    pre: Vec<crate::rsm::qlog::codec::Pre>,
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
    /// The raft-log barrier still owed for this group; `None` on the Phase C
    /// queue-log path.
    handle: Option<SyncHandle>,
    /// The queue-log barrier still owed for this group (Phase C): the logs its
    /// write touched, fsynced by the syncer while the writer writes the next
    /// group.
    qlog: Option<(crate::rsm::qlog::set::QLogSyncer, crate::rsm::qlog::set::SyncTicket)>,
    log_files: u64,
    log_bytes: u64,
}

/// Resolve a `pid` to its `queue_id` from the committed partition catalog (a
/// store read), for the writer's `pid -> queue_id` map miss (A3b).
pub(crate) type PartitionLookup = Arc<dyn Fn(u64) -> io::Result<Option<u64>> + Send + Sync>;

/// The log-native WRITE side the writer owns when `QUEEN_RAFT_QLOG` is on
/// (`ALICE_PGLESS_NEWARCH.md` A3b, the double-write kill; Phase C, the queue
/// logs as the ONLY write-ahead log). For every entry of a group the writer
/// writes each `Append`'s payload record AND the entry's payload-free entry
/// record into every queue log the entry touches (or the system log), fsyncs
/// every touched log ONCE, and only then hands the group to apply. The raft log
/// is not written on this path: the queue logs alone recover every
/// acknowledged entry.
pub(crate) struct QlogWrite {
    /// The per-queue logs (opened + reconciled at boot, moved here). The single
    /// WRITER of them; the facade + planner read the SAME logs through a shared
    /// `QLogReader` the boot published.
    pub(crate) set: QLogSet,
    /// `pid -> queue_id`, so the writer routes an `Append` to its queue's log
    /// without a name lookup. Seeded at boot from the raft-log replay window
    /// (partitions created between the store's durable point and the log tip),
    /// maintained here from `PartitionCreate`/`PartitionDelete`, and filled on a
    /// miss from the committed partition catalog via [`QlogWrite::lookup`].
    pub(crate) pid_qid: HashMap<u64, u64>,
    /// Resolve `pid -> queue_id` from the committed partition catalog (a store
    /// read), for a partition created before the last restart — its create was
    /// truncated from the replay window, but its row is committed, so a map miss
    /// can only be such a partition and this read always finds it.
    pub(crate) lookup: PartitionLookup,
}

impl QlogWrite {
    /// Write one group into the queue logs and fsync it — THE write-ahead
    /// barrier on both replicators: [`QlogWrite::write_group_nosync`], then one
    /// fsync of every log it wrote. Only then may the caller acknowledge the
    /// group. On return every [`GroupBody::Entry`] holds its PAYLOAD-FREE
    /// decode.
    pub(crate) fn write_group(&mut self, items: &mut [GroupItem]) -> io::Result<()> {
        let t = self.write_group_nosync(items)?;
        // THE BARRIER: every touched queue log, once. Timed into `log_fsync`
        // (PERF-1), which keeps meaning "the WAL barrier" on this path.
        let s0 = crate::rsm::timing::stamp();
        self.set.syncer().sync(&t)?;
        if let Some(s0) = s0 {
            crate::rsm::timing::metrics()
                .log_fsync
                .record_dur(s0.elapsed());
        }
        self.set.note_durable(t.seq);
        // `qlog.record_fsynced` (and `log.flushed`): the group is durable in
        // the queue logs and not yet applied; it WILL replay on restart.
        crate::rsm::faults::hit("qlog.record_fsynced");
        crate::rsm::faults::hit("log.flushed");
        Ok(())
    }

    /// Write one group into the queue logs WITHOUT the fsync, and return what
    /// the fsync must cover. For each item, in order: its `Append` payload
    /// records into each payload's log, then one entry record (with `copies`,
    /// and the item's `seq` and `term`) into every log its effects touch, or
    /// into the system log ([`SYSTEM_QUEUE_ID`]) when it touches none (and
    /// always for a [`GroupBody::Raw`] record). One `write` per touched log;
    /// the logs of different LANES are written on parallel threads (a log is
    /// one lane of one queue, [`QLogSet::log_id_for`]). Nothing may be
    /// acknowledged before the returned ticket is fsynced; recovery keeps the
    /// complete, gapless prefix (every copy of an entry present), so a crash
    /// anywhere before that leaves nothing half-acknowledged. The caller may
    /// write the NEXT group while another thread fsyncs this one: each log still
    /// receives its records in `seq` order. On return every
    /// [`GroupBody::Entry`] holds its PAYLOAD-FREE decode.
    pub(crate) fn write_group_nosync(
        &mut self,
        items: &mut [GroupItem],
    ) -> io::Result<crate::rsm::qlog::set::SyncTicket> {
        use crate::rsm::effect::Effect;
        use crate::rsm::qlog::set::lane_log_id;
        // The payload-free bytes of every entry — the entry records carry
        // these: encoded from the ORIGINAL entry (its payloads are still in
        // it), or as received (a follower's stored-form entry).
        let mut pf: Vec<Bytes> = Vec::with_capacity(items.len());
        for it in items.iter() {
            match &it.body {
                GroupBody::Entry { entry, .. } => {
                    pf.push(Bytes::from(encode_entry_payload_free(entry).map_err(|e| {
                        io::Error::other(format!("payload-free entry encode: {e:?}"))
                    })?))
                }
                GroupBody::Stored { pf: b, .. } => pf.push(b.clone()),
                GroupBody::Raw { bytes, .. } => pf.push(Bytes::copy_from_slice(bytes)),
            }
        }
        // The node-local payload codec (qlog::codec): every `Append` blob of the
        // group's full entries, in effect order — already compressed (a raft
        // leader awaited it before proposing), started at propose, or
        // compressed now. `None` = store raw. A stored-form entry needs none.
        let mut zblobs: Vec<Option<Bytes>> = Vec::new();
        for it in items.iter_mut() {
            let GroupBody::Entry { entry, pre, z } = &mut it.body else {
                continue;
            };
            let n = entry
                .effects
                .iter()
                .filter(|eff| matches!(eff, Effect::Append { .. }))
                .count();
            if let Some(z) = z.take().filter(|z| z.len() == n) {
                zblobs.extend(z.iter().cloned());
                continue;
            }
            let pre = std::mem::take(pre);
            if pre.len() == n {
                zblobs.extend(
                    pre.into_iter()
                        .map(|p| crate::rsm::qlog::codec::Pre::finish(p).map(Bytes::from)),
                );
            } else {
                let raws: Vec<&[u8]> = entry
                    .effects
                    .iter()
                    .filter_map(|eff| match eff {
                        Effect::Append { blob, .. } => Some(blob.as_slice()),
                        _ => None,
                    })
                    .collect();
                zblobs.extend(
                    crate::rsm::qlog::codec::compress_all(&raws)
                        .into_iter()
                        .map(|o| o.map(Bytes::from)),
                );
            }
        }
        let lanes = self.set.lanes();
        let mut zi = 0usize;
        let mut max_seq = 0u64;
        // Every log written, with its lane.
        let mut lane_of_log: HashMap<u64, u64> = HashMap::new();
        // The records borrow `items` (hashes, blobs, stored payloads), `pf`
        // (entry bytes) and `zblobs`, so the writes are scoped to end BEFORE the
        // entries are rewritten below.
        {
            let q = &mut *self;
            let lookup = q.lookup.clone();
            let mut by_log: std::collections::BTreeMap<u64, Vec<WriteRecord<'_>>> =
                std::collections::BTreeMap::new();
            let mut touched: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
            let mut msgs: Vec<(u64, WriteRecord<'_>)> = Vec::new();
            for (i, it) in items.iter().enumerate() {
                let seq = it.seq;
                max_seq = max_seq.max(seq);
                touched.clear();
                msgs.clear();
                let (effects, now_us, stored): (&[Effect], i64, Option<&[StoredPayload]>) =
                    match &it.body {
                        GroupBody::Entry { entry, .. } => {
                            (entry.effects.as_slice(), entry.now_us, None)
                        }
                        GroupBody::Stored {
                            entry, payloads, ..
                        } => (
                            entry.effects.as_slice(),
                            entry.now_us,
                            Some(payloads.as_slice()),
                        ),
                        GroupBody::Raw { now_us, .. } => (&[], *now_us, None),
                    };
                let mut unresolved = matches!(it.body, GroupBody::Raw { .. });
                let mut si = 0usize;
                for eff in effects {
                    match eff {
                        Effect::QueueUpsert { tenant, queue, .. }
                        | Effect::QueueDelete { tenant, queue }
                        | Effect::GroupUpsert { tenant, queue, .. }
                        | Effect::GroupDelete { tenant, queue, .. }
                        | Effect::DlqInsert { tenant, queue, .. }
                        | Effect::DlqDelete { tenant, queue, .. } => {
                            let log = QLogSet::queue_id_of(tenant, queue);
                            lane_of_log.insert(log, 0);
                            touched.insert(log);
                        }
                        Effect::PartitionCreate {
                            pid, tenant, queue, ..
                        } => {
                            let qid = QLogSet::queue_id_of(tenant, queue);
                            q.pid_qid.insert(*pid, qid);
                            let lane = pid % lanes;
                            let log = lane_log_id(qid, lane);
                            lane_of_log.insert(log, lane);
                            touched.insert(log);
                        }
                        Effect::PartitionDelete { pid } => {
                            match resolve_qid(&mut q.pid_qid, &lookup, *pid)? {
                                Some(qid) => {
                                    let lane = pid % lanes;
                                    let log = lane_log_id(qid, lane);
                                    lane_of_log.insert(log, lane);
                                    touched.insert(log);
                                }
                                None => unresolved = true,
                            }
                            q.pid_qid.remove(pid);
                        }
                        Effect::CursorSet { pid, .. }
                        | Effect::CursorDelete { pid, .. }
                        | Effect::Watermark { pid, .. } => {
                            match resolve_qid(&mut q.pid_qid, &lookup, *pid)? {
                                Some(qid) => {
                                    let lane = pid % lanes;
                                    let log = lane_log_id(qid, lane);
                                    lane_of_log.insert(log, lane);
                                    touched.insert(log);
                                }
                                None => unresolved = true,
                            }
                        }
                        Effect::Append {
                            pid,
                            base_offset,
                            count,
                            created_at_us,
                            hashes,
                            blob,
                            ..
                        } => {
                            let qid =
                                resolve_qid(&mut q.pid_qid, &lookup, *pid)?.ok_or_else(|| {
                                    io::Error::other(format!(
                                        "qlog route: no partition row for pid {pid}"
                                    ))
                                })?;
                            let lane = pid % lanes;
                            let log = lane_log_id(qid, lane);
                            lane_of_log.insert(log, lane);
                            touched.insert(log);
                            let (zstd, payload): (bool, &[u8]) = match stored {
                                Some(sp) => {
                                    let p = sp.get(si).ok_or_else(|| {
                                        io::Error::other(format!(
                                            "entry {seq}: fewer stored payloads than appends"
                                        ))
                                    })?;
                                    si += 1;
                                    (p.zstd, p.bytes.as_ref())
                                }
                                None => {
                                    let z = zblobs[zi].as_deref();
                                    zi += 1;
                                    match z {
                                        Some(z) => (true, z),
                                        None => (false, blob.as_slice()),
                                    }
                                }
                            };
                            let r = RecordInput {
                                seq,
                                pid: *pid,
                                base_offset: *base_offset,
                                count: *count,
                                created_at_us: *created_at_us,
                                txn: None,
                                hashes,
                                payload,
                            };
                            msgs.push((
                                log,
                                if zstd {
                                    WriteRecord::Zstd(r)
                                } else {
                                    WriteRecord::Msg(r)
                                },
                            ));
                        }
                        _ => {}
                    }
                }
                if let Some(sp) = stored {
                    if si != sp.len() {
                        return Err(io::Error::other(format!(
                            "entry {seq}: {} stored payloads for {si} appends",
                            sp.len()
                        )));
                    }
                }
                if touched.is_empty() || unresolved {
                    lane_of_log.insert(SYSTEM_QUEUE_ID, 0);
                    touched.insert(SYSTEM_QUEUE_ID);
                }
                let copies = touched.len() as u32;
                // Each log: this entry's payload records (effect order), then its
                // entry record — so an entry record found on disk implies every
                // payload record of that entry in the same log precedes it.
                for (log, r) in msgs.drain(..) {
                    by_log.entry(log).or_default().push(r);
                }
                for log in &touched {
                    by_log
                        .entry(*log)
                        .or_default()
                        .push(WriteRecord::Entry(EntryInput {
                            seq,
                            now_us,
                            copies,
                            term: it.term,
                            entry: &pf[i],
                        }));
                }
            }
            // One write per touched log (page cache, no fsync yet), the lanes
            // in parallel. Every log written is marked dirty, so the fsync
            // covers a group of pops and acks exactly as it covers pushes.
            let mut per_lane: std::collections::BTreeMap<u64, Vec<(u64, &Vec<WriteRecord<'_>>)>> =
                std::collections::BTreeMap::new();
            for (log, records) in &by_log {
                per_lane
                    .entry(lane_of_log.get(log).copied().unwrap_or(0))
                    .or_default()
                    .push((*log, records));
            }
            let set = &q.set;
            let write_lane = |logs: &[(u64, &Vec<WriteRecord<'_>>)]| -> io::Result<()> {
                for (log, records) in logs {
                    set.write_mixed_shared(*log, records)?;
                }
                Ok(())
            };
            let mut lanes_iter = per_lane.values();
            match per_lane.len() {
                0 => {}
                1 => write_lane(lanes_iter.next().expect("one lane"))?,
                _ => std::thread::scope(|s| -> io::Result<()> {
                    let first = lanes_iter.next().expect("a lane");
                    let joins: Vec<std::thread::ScopedJoinHandle<'_, io::Result<()>>> = lanes_iter
                        .map(|logs| s.spawn(move || write_lane(logs)))
                        .collect();
                    let mut res = write_lane(first);
                    for j in joins {
                        let r = j.join().unwrap_or_else(|_| {
                            Err(io::Error::other("qlog lane writer panicked"))
                        });
                        if res.is_ok() {
                            res = r;
                        }
                    }
                    res
                })?,
            }
            let written: Vec<u64> = by_log.keys().copied().collect();
            q.set.note_written(written, max_seq);
            // `qlog.record_written` (and its raft-era twin `log.appended`): the
            // group is in the page cache, not fsynced, nothing answered. A kill
            // here keeps the page cache and the group replays; a power loss may
            // keep any subset — recovery keeps the complete gapless prefix.
            crate::rsm::faults::hit("qlog.record_written");
            crate::rsm::faults::hit("log.appended");
        }
        // Hand apply the PAYLOAD-FREE form — each `Append`'s `blob` is the
        // payload's 4-byte frame length — byte-identical to what a replay from the
        // queue logs hands it, so `RetainedBytes` (a REPLICATED counter) is
        // computed from the length on BOTH paths and the digest is replay-stable
        // (I2). Cheap: the entry carries no payload. A stored-form entry already
        // is that form.
        for (it, bytes) in items.iter_mut().zip(pf.iter()) {
            if let GroupBody::Entry { entry, .. } = &mut it.body {
                *entry = Arc::new(decode_entry(bytes).map_err(|e| {
                    io::Error::other(format!("payload-free entry re-decode: {e:?}"))
                })?);
            }
        }
        Ok(self.set.take_ticket())
    }
}

/// One entry of a queue-log write group — what the local writer and the
/// openraft log storage both hand [`QlogWrite::write_group`].
pub(crate) struct GroupItem {
    /// The record `seq`: the entry's index in the log.
    pub(crate) seq: u64,
    /// The Raft term (`0` on the local replicator), stored in each entry record.
    pub(crate) term: u64,
    pub(crate) body: GroupBody,
}

/// What a [`GroupItem`] carries.
pub(crate) enum GroupBody {
    /// An application entry: every `Append` payload goes to its queue's log
    /// and one payload-free entry record to every log its effects touch. After
    /// the write, `entry` is REPLACED by its payload-free decode — the exact
    /// form a replay hands apply (the I2 `RetainedBytes` rule).
    Entry {
        entry: Arc<crate::rsm::entry::Entry>,
        pre: Vec<crate::rsm::qlog::codec::Pre>,
        /// The stored form of every `Append` blob, in effect order, when it is
        /// already known (a raft leader awaited it before proposing, and sent
        /// the same bytes to its followers). `None` = use `pre`, or compress.
        z: Option<Arc<Vec<Option<Bytes>>>>,
    },
    /// An application entry as a follower received it: the PAYLOAD-FREE entry
    /// and its encoding (the entry record), plus every `Append` payload
    /// exactly as the leader stored it. Written as is: no encode, no codec.
    Stored {
        entry: Arc<crate::rsm::entry::Entry>,
        pf: Bytes,
        payloads: Arc<Vec<StoredPayload>>,
    },
    /// A consensus-internal record (an openraft blank or membership entry):
    /// its bytes are written verbatim as one entry record in the system log.
    Raw { now_us: i64, bytes: Vec<u8> },
}

struct Writer {
    /// The raft log. The WAL with the knob off; with `QUEEN_RAFT_QLOG` on it is
    /// no longer written (Phase C) — only a legacy tail from before Phase C is
    /// replayed from it at open, and its sealed files are still dropped behind
    /// durable points.
    log: LogStore,
    sink: WriterSink,
    shared: Arc<Shared>,
    role_tx: watch::Sender<Role>,
    /// The highest durable index this writer has already truncated behind.
    last_truncated: u64,
    /// PERF-K trace: when the previous group finished, for the inter-group gap.
    last_group_at: Option<Instant>,
    /// The log-native write side (`QUEEN_RAFT_QLOG`), `None` when the knob is
    /// off (then the writer writes the raft log EXACTLY as today: the entry
    /// carries its payload, apply files it into a segment).
    qlog: Option<QlogWrite>,
    /// Phase C (qlog path only): the index the next entry gets. The writer
    /// ASSIGNS indexes itself now that no raft log is appended: monotone,
    /// gapless, starting right after the recovered prefix (`open`).
    next_index: u64,
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
                .map(|p| p.size + super::log::FRAME_OVERHEAD)
                .sum();
            while pending.len() < GROUP_COMMIT_MAX && bytes < GROUP_COMMIT_BYTES {
                match rx.try_recv() {
                    Ok(p) => {
                        bytes += p.size + super::log::FRAME_OVERHEAD;
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
            let group_bytes: u64 = pending.iter().map(|p| p.size as u64).sum();
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

        // PERF-K: one GROUPTRACE line per fsync group (Direct path times the
        // whole append_group = write + fsync barrier; the write is a memcpy, so
        // this is fsync-dominated). Gated on QUEEN_RAFT_CYCLE_TRACE.
        let trace = crate::rsm::timing::cycle_trace_enabled();
        let group_len = pending.len();
        let group_bytes_t: u64 = if trace {
            pending.iter().map(|p| p.size as u64).sum()
        } else {
            0
        };

        // Phase C (`QUEEN_RAFT_QLOG` on): the queue logs are the only WAL — the
        // writer assigns the indexes, writes every entry (and every `Append`'s
        // payload) into the queue logs, fsyncs them once, and hands the group
        // to apply. The raft log is not touched.
        if self.qlog.is_some() {
            return self.commit_qlog(pending, trace, group_len, group_bytes_t);
        }

        // Knob OFF: the raft log carries the entry (payload included) exactly
        // as before the qlog existed, byte-for-byte.
        match &self.sink {
            WriterSink::Direct(_) => {
                let slices: Vec<&[u8]> = pending.iter().map(|p| p.bytes.as_ref()).collect();
                let fsync_started = trace.then(Instant::now);
                let first_index = match self.log.append_group(&slices) {
                    Ok(i) => i,
                    Err(e) => {
                        drop(slices);
                        self.fail(&format!("local log append failed: {e}"), pending);
                        return false;
                    }
                };
                if let Some(started) = fsync_started {
                    let now = Instant::now();
                    let gap = self
                        .last_group_at
                        .map(|t| now.saturating_duration_since(t).as_micros() as u64)
                        .unwrap_or(0);
                    self.last_group_at = Some(now);
                    crate::rsm::timing::cycle_trace_line(format!(
                        "GROUPTRACE t={} entries={} bytes={} write_fsync_us={} gap_us={}",
                        crate::rsm::timing::trace_now_us(),
                        group_len,
                        group_bytes_t,
                        started.elapsed().as_micros() as u64,
                        gap,
                    ));
                }
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
                    handle: Some(handle),
                    qlog: None,
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

    /// Phase C, one group on the queue-log path: assign the indexes, write and
    /// fsync every entry into the queue logs ([`Writer::write_qlog_group`]), then
    /// hand the group to apply (directly, or through the syncer, which then only
    /// preserves the order — the barrier already ran). Returns false when the
    /// node must stop.
    fn commit_qlog(
        &mut self,
        mut pending: Vec<Pending>,
        trace: bool,
        group_len: usize,
        group_bytes_t: u64,
    ) -> bool {
        let first_index = self.next_index;
        let started = trace.then(Instant::now);
        // Pipelined: write only; the syncer fsyncs this group while the next
        // one is written. Direct: write and fsync here.
        let pipelined = matches!(self.sink, WriterSink::Pipelined(_));
        let ticket = match self.write_qlog_group(&mut pending, first_index, !pipelined) {
            Ok(t) => t,
            Err(e) => {
                self.fail(&format!("local qlog write failed: {e}"), pending);
                return false;
            }
        };
        // Indexes are consumed only once the group is durable: a failed write
        // poisoned the node above, so no index is ever handed out twice.
        self.next_index = first_index + pending.len() as u64;
        if let Some(started) = started {
            let now = Instant::now();
            let gap = self
                .last_group_at
                .map(|t| now.saturating_duration_since(t).as_micros() as u64)
                .unwrap_or(0);
            self.last_group_at = Some(now);
            crate::rsm::timing::cycle_trace_line(format!(
                "GROUPTRACE t={} entries={} bytes={} write_fsync_us={} gap_us={} qlog=1",
                crate::rsm::timing::trace_now_us(),
                group_len,
                group_bytes_t,
                started.elapsed().as_micros() as u64,
                gap,
            ));
        }
        let (files, bytes) = self.qlog.as_ref().expect("qlog on").set.totals();
        match &self.sink {
            WriterSink::Direct(apply_tx) => {
                if !handoff_group(&self.shared, apply_tx, pending, first_index, files, bytes) {
                    self.poison("apply thread gone");
                    return false;
                }
                true
            }
            WriterSink::Pipelined(job_tx) => {
                let syncer = self.qlog.as_ref().expect("qlog on").set.syncer();
                let job = SyncJob {
                    pending,
                    first_index,
                    handle: None,
                    qlog: ticket.map(|t| (syncer, t)),
                    log_files: files,
                    log_bytes: bytes,
                };
                if job_tx.send(job).is_err() {
                    self.poison("log syncer gone");
                    return false;
                }
                true
            }
        }
    }

    /// Phase C: write EVERY entry of this group into the queue logs and fsync
    /// them — the queue logs are the only write-ahead log (`QUEEN_RAFT_QLOG`).
    ///
    /// # Layout
    ///
    /// For each entry, in index order, its `seq` is its ENTRY INDEX
    /// (`first_index + position`). Every queue log the entry's effects touch
    /// gets, in one `write` per log for the whole group: the entry's `Append`
    /// payload records for that queue (message records, indexed for pop/dedup,
    /// as in A3b), then ONE payload-free ENTRY record (`REC_ENTRY`) carrying the
    /// exact `encode_entry_payload_free` bytes and `copies` = the number of logs
    /// that get this same record. An entry that touches no queue — or one of
    /// whose pid-scoped effects names a partition that cannot be resolved (a
    /// deleted one) — also (or only) goes to the system log
    /// ([`SYSTEM_QUEUE_ID`]). Queue of an effect:
    ///
    /// - by name: `QueueUpsert`, `QueueDelete`, `GroupUpsert`, `GroupDelete`,
    ///   `PartitionCreate`, `DlqInsert`, `DlqDelete`;
    /// - by pid (the `pid -> queue_id` map, filled on a miss from the committed
    ///   catalog): `Append` (a miss is fatal: the payload must land in its
    ///   queue), `CursorSet`, `CursorDelete`, `Watermark`, `PartitionDelete`;
    /// - none (system log): everything else — `Noop`, `RequestIdsExpire`, KV,
    ///   timers, streams, traces, flags, quotas, ephemeral config, garbage
    ///   bookkeeping, cluster version, membership notes.
    ///
    /// # Ordering (the invariant)
    ///
    /// Every record of the group is written first, then ONE `sync` fsyncs every
    /// touched log (THE barrier), and only then does the caller hand the group
    /// to apply and answer it. So every answered entry — push, pop lease, ack,
    /// create — is durable in the queue logs, the raft log plays no part, and:
    ///
    /// - a crash before the sync (`qlog.record_written` / `log.appended`) or
    ///   during it leaves the group partially on disk: some logs may hold an
    ///   entry's record and others not, and a later entry may survive an
    ///   earlier one. Nothing of the group was answered. Recovery
    ///   ([`QLogSet::scan_entries`]) keeps only the gapless prefix of COMPLETE
    ///   entries (all `copies` found) and truncates the rest
    ///   ([`QLogSet::truncate_from`]) before the seqs are reused — and an entry
    ///   whose record is present in every touched log has all its payloads too
    ///   (a log's records before its entry record precede it in the file, and a
    ///   torn tail is only ever cut as a suffix);
    /// - a crash after the sync (`qlog.record_fsynced` / `log.flushed`) leaves
    ///   the whole group durable: it replays on restart, exactly once (I4).
    ///
    /// Each `p.entry` is then replaced by its payload-free decode, so apply sees
    /// LIVE exactly the form a replay hands it (the I2 `RetainedBytes` rule).
    fn write_qlog_group(
        &mut self,
        pending: &mut [Pending],
        first_index: u64,
        sync: bool,
    ) -> io::Result<Option<crate::rsm::qlog::set::SyncTicket>> {
        // The shared group write (`QlogWrite::write_group`), with the index the
        // writer assigns and term 0 (the local replicator has no terms).
        let mut items: Vec<GroupItem> = pending
            .iter_mut()
            .enumerate()
            .map(|(i, p)| GroupItem {
                seq: first_index + i as u64,
                term: 0,
                body: GroupBody::Entry {
                    entry: p.entry.clone(),
                    pre: std::mem::take(&mut p.pre),
                    z: None,
                },
            })
            .collect();
        let q = self.qlog.as_mut().expect("qlog on");
        let ticket = if sync {
            q.write_group(&mut items)?;
            None
        } else {
            Some(q.write_group_nosync(&mut items)?)
        };
        for (p, it) in pending.iter_mut().zip(items) {
            if let GroupBody::Entry { entry, .. } = it.body {
                p.entry = entry;
            }
        }
        Ok(ticket)
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

    /// Publish the WAL gauges: the queue logs' totals on the Phase C path, the
    /// raft log's otherwise.
    fn refresh_log_metrics(&self) {
        let (files, bytes) = match &self.qlog {
            Some(q) => q.set.totals(),
            None => (self.log.file_count() as u64, self.log.bytes()),
        };
        self.shared.log_files.store(files, Ordering::Release);
        self.shared.log_bytes.store(bytes, Ordering::Release);
    }
}

/// `pid -> queue_id` for the writer's routing: the map, else the committed
/// partition catalog (a partition created before the last restart), memoized.
/// `None` when neither knows the pid (a partition already deleted).
pub(crate) fn resolve_qid(
    map: &mut HashMap<u64, u64>,
    lookup: &PartitionLookup,
    pid: u64,
) -> io::Result<Option<u64>> {
    if let Some(qid) = map.get(&pid) {
        return Ok(Some(*qid));
    }
    match lookup(pid)? {
        Some(qid) => {
            map.insert(pid, qid);
            Ok(Some(qid))
        }
        None => Ok(None),
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
            // Unique by now (the payload-free replacement, or a decoded entry).
            entry: Arc::try_unwrap(p.entry).unwrap_or_else(|a| (*a).clone()),
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
        let trace = crate::rsm::timing::cycle_trace_enabled();
        let mut last_group_at: Option<Instant> = None;
        // Iterates until the writer drops the job sender (a clean shutdown) or
        // this thread poisons.
        for job in rx {
            let SyncJob {
                pending,
                first_index,
                handle,
                qlog,
                log_files,
                log_bytes,
            } = job;
            let (group_len, group_bytes_t) = if trace {
                (
                    pending.len(),
                    pending.iter().map(|p| p.size as u64).sum::<u64>(),
                )
            } else {
                (0, 0)
            };
            let fsync_started = trace.then(Instant::now);
            // Phase C: the queue-log barrier of this group — every log its write
            // touched — while the writer writes the next group.
            if let Some((syncer, ticket)) = qlog {
                let s0 = crate::rsm::timing::stamp();
                if let Err(e) = syncer.sync(&ticket) {
                    drop(pending);
                    self.poison(&format!("local qlog fsync failed: {e}"));
                    return;
                }
                if let Some(s0) = s0 {
                    crate::rsm::timing::metrics()
                        .log_fsync
                        .record_dur(s0.elapsed());
                }
                crate::rsm::faults::hit("qlog.record_fsynced");
                crate::rsm::faults::hit("log.flushed");
            }
            if let Some(handle) = handle {
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
            }
            if let Some(started) = fsync_started {
                let now = Instant::now();
                let gap = last_group_at
                    .map(|t| now.saturating_duration_since(t).as_micros() as u64)
                    .unwrap_or(0);
                last_group_at = Some(now);
                crate::rsm::timing::cycle_trace_line(format!(
                    "GROUPTRACE t={} entries={} bytes={} write_fsync_us={} gap_us={} pipelined=1",
                    crate::rsm::timing::trace_now_us(),
                    group_len,
                    group_bytes_t,
                    started.elapsed().as_micros() as u64,
                    gap,
                ));
            }
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
    /// `QUEEN_RAFT_WRITER_PIPELINE` (default ON, see
    /// [`writer_pipeline_from_env`]): run the log write and its group fsync on
    /// two threads, so the writer forms and writes group N+1 while the syncer
    /// fsyncs group N. Entries are still acknowledged only after their own
    /// group's fsync (I4), and the syncer processes groups in index order (so
    /// apply sees them in order). Off: the single-thread write-then-fsync
    /// writer.
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

/// Resolve `QUEEN_RAFT_WRITER_PIPELINE`. **Default ON since the queue logs
/// became the WAL**: on that path the writer used to fsync inside the group
/// write, so the pipeline overlapped nothing; now the syncer fsyncs group N
/// while the writer writes group N+1 (and a raft follower's writer does the
/// same). `0`/`false`/`off`/`no` turns it off. History, measured on the raft-log
/// path before Phase C (PERF-G round 3): the
/// laptop A/B showed the write/fsync pipeline REGRESSES push p50 (10.3 → 14.1 ms
/// at A20k, both channel shapes) because the two-thread split cannot beat the
/// inline writer's natural group-commit batching, and the finer `writer_pickup`
/// stage shows there is nothing for it to overlap where it would matter: on the
/// VM the writer picks a group up in ≈0 ms (PERF-2: proposed→committed ≈
/// log_fsync), so the whole `proposed→committed` leg IS the fsync, which the
/// pipeline cannot shorten. It only helps a box whose fsync is expensive enough
/// to make the writer queue behind it AND whose extra fsyncs are cheap — neither
/// holds here. Kept as a knob (`=1` turns it on) and proven I2-transparent, for
/// a box where the trade-off flips.
/// `pub(crate)` so the replicator tests build an `OpenConfig` honouring the knob.
pub(crate) fn writer_pipeline_from_env() -> bool {
    match std::env::var("QUEEN_RAFT_WRITER_PIPELINE") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        Err(_) => true,
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
    /// The per-queue-log reader the apply thread published at open (Phase A2),
    /// `Some` only when `QUEEN_RAFT_QLOG` is on. The facade reads pop payloads
    /// and the planner reads the `DEDUP_INDEX=segment` dedup authority through it
    /// instead of the segments/LMDB, off the SAME live logs the applier appends
    /// to. Set once `open` returns.
    qlog_reader: Option<crate::rsm::qlog::set::QLogReader>,
    /// The queue logs are the WAL, so `propose` starts the payload codec.
    qlog_codec: bool,
}

impl<S: Store + 'static> LocalReplicator<S> {
    /// Open the replicator over `store` (recovered by the apply thread) and the
    /// local log under `cfg.log_dir`. Replays every log entry after the store's
    /// durable index into apply (§11.5), waits for apply to catch up, then
    /// begins accepting proposals. A boot call: it does blocking I/O and must
    /// not run on a tokio worker.
    ///
    /// Phase C (`QUEEN_RAFT_QLOG` on): the queue logs are the WAL. The replay is
    /// (a) any LEGACY raft-log tail above the store's durable index (a data
    /// directory last run before Phase C, whose un-fsynced payload-free raft log
    /// may still hold entries), then (b) the queue logs' entry records from
    /// there on ([`QLogSet::scan_entries`]: merged across every log, deduped,
    /// gapless, complete entries only). Everything at or above where (b) stops
    /// is the unacknowledged tail of a group whose fsyncs did not all land and
    /// is truncated ([`QLogSet::truncate_from`]); the writer then assigns
    /// indexes from right after the replayed prefix.
    pub fn open(
        store: Arc<S>,
        cfg: OpenConfig,
        waker: Arc<dyn Waker>,
        clock: Arc<dyn apply::Clock>,
    ) -> io::Result<LocalReplicator<S>> {
        // 1. The log: recover it, truncate any torn tail. With the qlog knob on
        //    it is only a legacy source (nothing appends to it any more).
        let (log, log_rec) = LogStore::open(&cfg.log_dir, cfg.log_opts)?;
        let last_log = log.last_index();

        // 2. Where the store reopened (§11.5 step 2). The apply thread reads
        //    the same values inside its own recovery; reading them here too is
        //    a read transaction, no conflict.
        let (store_applied, store_term, store_durable) = store
            .read(|r| Ok((r.applied_index()?, r.applied_term()?, r.durable_index()?)))
            .map_err(|e| io::Error::other(format!("read store recovery point: {e}")))?;

        // The raft log is the WAL only with the qlog knob off; with it on the
        // queue logs are, and the same check runs against them after replay.
        if !cfg.apply_cfg.qlog && store_applied > last_log {
            // The store is ahead of the log: impossible if the log is the WAL,
            // and a sign the log directory was truncated or swapped. Refuse
            // rather than silently lose the tail (§0.3 "refuse, never guess").
            return Err(io::Error::other(format!(
                "store applied index {store_applied} is ahead of the log's last index {last_log}"
            )));
        }

        // A3b (`ALICE_PGLESS_NEWARCH.md` §5): with `QUEEN_RAFT_QLOG` on, the
        // WRITER owns the per-queue logs — it writes each `Append`'s payload and
        // fsyncs it BEFORE the referencing raft-log entry, so the entry is
        // payload-free (the double-write dies) and no committed entry ever points
        // at a missing payload. We open + reconcile the qlog HERE, on the boot
        // thread, hand the write side to the writer below, and tell the applier
        // NOT to open or write it (`qlog_writer_external`). The facade reads
        // through the reader we publish. Knob off: none of this runs, and the
        // writer + applier are byte-for-byte today.
        let mut apply_cfg = cfg.apply_cfg;
        let qlog_on = apply_cfg.qlog;
        let (qlog_set, qlog_lookup, qlog_reader): (
            Option<QLogSet>,
            Option<PartitionLookup>,
            Option<crate::rsm::qlog::set::QLogReader>,
        ) = if qlog_on {
            // Options mirror the segment writer's (roll size + fsync mode), so the
            // qlog rolls and fsyncs on the same terms the store did.
            let qopts = crate::rsm::qlog::QLogOptions {
                segment_bytes: cfg.seg_opts.segment_bytes,
                fsync: match cfg.seg_opts.fsync {
                    segments::FsyncMode::Full => crate::rsm::qlog::Fsync::Full,
                    segments::FsyncMode::Data => crate::rsm::qlog::Fsync::Data,
                },
            };
            // `<data_dir>/qlog`, sibling to `seg/`, `log/`, `store/` — the SAME
            // path A2/A3a used, so an existing qlog is reopened, not re-created.
            let data_dir = cfg
                .seg_root
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."));
            let mut set = QLogSet::new(data_dir.join("qlog"), qopts);
            let qlog_tail = set.reopen_all()?;
            // NA-QLOG-I1 reconciliation, moved here from `Applier::open` now that
            // the qlog is boot/writer-owned: the reopened durable tail must be
            // AHEAD of or EQUAL to what the store recorded as qlog-durable, never
            // behind. Behind means a committed record is missing from the SOLE
            // payload store — silent data loss — so refuse to start. (Ahead is
            // benign: an un-fsync'd SIGKILL tail, or records for rolled-back
            // entries the raft log will replay.)
            let qlog_durable_index = store
                .read(|r| {
                    Ok(r.meta_u64(crate::rsm::store::meta::QLOG_DURABLE_INDEX)?
                        .unwrap_or(0))
                })
                .map_err(|e| io::Error::other(format!("read qlog durable index: {e}")))?;
            if qlog_tail < qlog_durable_index {
                return Err(io::Error::other(format!(
                    "qlog durable tail seq {qlog_tail} is BEHIND the store's recorded \
                     qlog-durable index {qlog_durable_index}: a committed record is missing \
                     from the qlog (NA-QLOG-I1)"
                )));
            }
            tracing::info!(
                target: "rsm",
                qlog_tail,
                qlog_durable_index,
                "rsm qlog recovery reconciled (boot, A3b — tail ≥ store's qlog-durable index)",
            );
            let reader = set.reader();
            // On-demand `pid -> queue_id` for a map miss: only a partition created
            // before the last restart (its create is gone from the replay window
            // but its row is committed) can miss, so this committed-catalog read
            // always finds it.
            let store_for_lookup = store.clone();
            let lookup: PartitionLookup = Arc::new(move |pid| {
                store_for_lookup
                    .read(|r| {
                        Ok(r.partition(pid)?
                            .map(|p| QLogSet::queue_id_of(&p.tenant, &p.queue)))
                    })
                    .map_err(|e| {
                        io::Error::other(format!(
                            "A3b qlog route: partition read for pid {pid}: {e}"
                        ))
                    })
            });
            // The applier must not open or write the qlog now: the writer does.
            apply_cfg.qlog_writer_external = true;
            // Phase C recovery floor: the store durably holds everything
            // through its durable index, so no queue-log file wholly at or
            // below it is needed for replay. Raised at each durable point by
            // `ReplNotify::durable`.
            set.set_recovery_floor(store_durable);
            (Some(set), Some(lookup), Some(reader))
        } else {
            (None, None, None)
        };
        let mut qlog_set = qlog_set;
        // Seeded during replay (below) from the replay window's Create/Delete.
        let mut seed_pid_qid: HashMap<u64, u64> = HashMap::new();

        let (log_files0, log_bytes0) = match &qlog_set {
            Some(set) => set.totals(),
            None => (log.file_count() as u64, log.bytes()),
        };
        let shared = Arc::new(Shared {
            node_id: cfg.node_id,
            applied_index: AtomicU64::new(store_applied),
            applied_term: AtomicU64::new(store_term),
            durable_index: AtomicU64::new(store_durable),
            // The qlog path learns its tip only after the replay below.
            last_log_index: AtomicU64::new(if qlog_on {
                store_applied.max(store_durable)
            } else {
                last_log
            }),
            proposals: AtomicU64::new(0),
            log_files: AtomicU64::new(log_files0),
            log_bytes: AtomicU64::new(log_bytes0),
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
            qlog_floor: qlog_set.as_ref().map(|s| s.recovery_floor_handle()),
        });
        // The apply thread owns the segment writer; publish its reader here so
        // the facade can read pop payloads off the same live file set (§7.5,
        // WP-1.7c). Set once, before the first entry replays.
        let reader_sink: Arc<std::sync::OnceLock<segments::Reader>> =
            Arc::new(std::sync::OnceLock::new());
        // A3b: the qlog reader is published by the BOOT thread (from the
        // writer-owned qlog opened above), not by the applier — the applier no
        // longer owns the qlog when the knob is on. `apply_cfg` carries
        // `qlog_writer_external` so `Applier::open` opens no qlog of its own.
        let apply_join = apply::spawn_with_reader(
            store.clone(),
            cfg.seg_root.clone(),
            cfg.seg_opts,
            apply_cfg,
            notify,
            clock,
            apply_rx,
            Some(reader_sink.clone()),
        );

        // 5. Replay: every entry after the store's durable index, in order.
        //    Apply skips whatever the store already holds (idempotence,
        //    §11.5). Sends block only if apply is momentarily behind, which is
        //    fine on this boot thread. Returns the last index replayed (the
        //    log's tip), which the writer continues from.
        let mut replayed = 0u64;
        let replay: io::Result<u64> = (|| {
            if !qlog_on {
                // Knob OFF: the raft log is the WAL, exactly as before.
                log.scan_from(store_durable + 1, &mut |index, term, body| {
                    let entry = decode_replayed(index, body)?;
                    apply_tx
                        .send(Committed { index, term, entry })
                        .map_err(|_| io::Error::other("replay: apply thread exited"))?;
                    replayed += 1;
                    Ok(())
                })?;
                return Ok(last_log);
            }
            // (a) A LEGACY raft-log tail: entries above the durable point that a
            //     pre-Phase-C writer put only in the raft log (payload-free, their
            //     payloads fsync'd in the queue logs). Empty on a Phase C node.
            let mut next = store_durable + 1;
            log.scan_from(next, &mut |index, term, body| {
                if index != next {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("replay: legacy raft log jumps from {next} to {index}"),
                    ));
                }
                let entry = decode_replayed(index, body)?;
                seed_pid_qid_from(&mut seed_pid_qid, &entry);
                apply_tx
                    .send(Committed { index, term, entry })
                    .map_err(|_| io::Error::other("replay: apply thread exited"))?;
                replayed += 1;
                next += 1;
                Ok(())
            })?;
            // (b) The queue logs: every entry record from `next` on, merged
            //     across the logs, deduped, gapless and complete.
            let set = qlog_set.as_mut().expect("qlog on");
            let scan = set.scan_entries(next, &mut |rec| {
                let index = rec.seq;
                let entry = decode_replayed(index, &rec.entry)?;
                // Seed the writer's `pid -> queue_id` map from the replay window:
                // these creates are above the committed catalog the writer's
                // on-demand lookup reads.
                seed_pid_qid_from(&mut seed_pid_qid, &entry);
                apply_tx
                    .send(Committed {
                        index,
                        term: LOG_TERM,
                        entry,
                    })
                    .map_err(|_| io::Error::other("replay: apply thread exited"))?;
                replayed += 1;
                Ok(())
            })?;
            // (c) Everything at or above the cut is an unacknowledged tail (a
            //     group whose fsyncs did not all land): drop it, durably, before
            //     the writer hands those seqs to new entries. The tail may sit in
            //     a SEALED file: while the syncer fsynced a group, the writer's
            //     next group can roll a log (the roll fsyncs and seals the file
            //     holding that group's records), and another lane's log may not
            //     have been fsynced yet — so the cut goes across sealed files.
            let cut = scan.next_seq;
            let dropped = set.truncate_from_across(cut)?;
            if scan.stopped.is_some() || dropped > 0 {
                tracing::warn!(
                    target: "rsm",
                    cut,
                    discarded_entries = scan.discarded,
                    max_seq_found = scan.max_seq_found,
                    dropped_bytes = dropped,
                    why = scan.stopped.as_deref().unwrap_or("stale records above the cut"),
                    "rsm qlog recovery: dropped an unacknowledged tail",
                );
            }
            let tip = cut - 1;
            if store_applied > tip {
                // The store holds an entry the WAL cannot replay: every applied
                // entry was fsync'd in the queue logs before apply saw it, so
                // this is a swapped or damaged directory. Refuse (§0.3).
                return Err(io::Error::other(format!(
                    "store applied index {store_applied} is ahead of the queue logs' \
                     recoverable tip {tip}"
                )));
            }
            Ok(tip)
        })();
        let last_index = match replay {
            Ok(n) => n,
            Err(e) => {
                // Tear the apply thread down before returning, so the store's
                // env can be reopened.
                drop(apply_tx);
                let _ = apply_join.join();
                return Err(e);
            }
        };
        shared.last_log_index.store(last_index, Ordering::Release);

        // 6. Wait for apply to reach the log's end. Past this the node may
        //    serve local stale reads (§11.5 step 6).
        if let Err(e) = wait_until_applied(&shared, last_index, cfg.replay_deadline, &apply_join) {
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
        // A3b: assemble the writer's qlog write side — the set opened + reconciled
        // above, plus the map seeded from the replay window just now, plus the
        // on-demand catalog lookup. `qlog_reader` was taken from the same set at
        // open, so the facade reads the SAME live logs the writer appends to.
        let qlog_write = match (qlog_set, qlog_lookup) {
            (Some(set), Some(lookup)) => Some(QlogWrite {
                set,
                pid_qid: seed_pid_qid,
                lookup,
            }),
            _ => None,
        };

        tracing::info!(
            target: "rsm",
            node = cfg.node_id,
            last_log,
            last_index,
            store_applied,
            store_durable,
            replayed,
            qlog_wal = qlog_on,
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
            last_group_at: None,
            qlog: qlog_write,
            next_index: last_index + 1,
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
            qlog_reader,
            qlog_codec: qlog_on,
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

    /// The per-queue-log reader for pop payloads and the `DEDUP_INDEX=segment`
    /// dedup authority (Phase A2), `Some` only when `QUEEN_RAFT_QLOG` is on.
    /// Cloneable and `Send + Sync`; the facade clones one per blocking-pop read
    /// and hands one to the batcher for the planner.
    pub fn qlog_reader(&self) -> Option<crate::rsm::qlog::set::QLogReader> {
        self.qlog_reader.clone()
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

/// Decode one replayed entry (raft log or queue-log entry record).
pub(crate) fn decode_replayed(index: u64, body: &[u8]) -> io::Result<Entry> {
    decode_entry(body).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("replay: entry {index} does not decode: {e:?}"),
        )
    })
}

/// Seed the writer's `pid -> queue_id` map from a replayed entry's
/// `PartitionCreate`/`PartitionDelete` (qlog path): partitions created above
/// the store's durable point are not in the committed catalog the writer's
/// on-demand lookup reads, so without this the first live append to one after
/// boot could not be routed.
pub(crate) fn seed_pid_qid_from(map: &mut HashMap<u64, u64>, entry: &Entry) {
    for eff in &entry.effects {
        match eff {
            crate::rsm::effect::Effect::PartitionCreate {
                pid, tenant, queue, ..
            } => {
                map.insert(*pid, QLogSet::queue_id_of(tenant, queue));
            }
            crate::rsm::effect::Effect::PartitionDelete { pid } => {
                map.remove(pid);
            }
            _ => {}
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

/// `QUEEN_RAFT_PROPOSE_ENTRY` (default on): on the queue-log path the batcher
/// hands the planned entry over instead of encoding it for a decode here.
/// `0` restores the encode + decode path (an A/B switch).
fn propose_entry_on() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("QUEEN_RAFT_PROPOSE_ENTRY").as_deref(),
            Ok("0") | Ok("false") | Ok("off")
        )
    })
}

impl<S: Store + 'static> LocalReplicator<S> {
    /// Hand one entry to the log writer and wait for it to apply. Runs its
    /// synchronous prefix (up to the channel send) in the caller's first poll,
    /// which is what makes the log index follow the submission order.
    async fn submit(
        &self,
        entry: Bytes,
        decoded: Arc<crate::rsm::entry::Entry>,
        deadline: Instant,
    ) -> Result<AppliedAt, ProposeError> {
        // Start the qlog codec on the payloads now (on the codec pool, sharing
        // the entry): it runs while the writer fsyncs the group ahead of this one.
        let pre = if self.qlog_codec {
            decoded
                .effects
                .iter()
                .enumerate()
                .filter_map(|(i, eff)| match eff {
                    crate::rsm::effect::Effect::Append { .. } => {
                        Some(crate::rsm::qlog::codec::Pre::start_append(&decoded, i))
                    }
                    _ => None,
                })
                .collect()
        } else {
            Vec::new()
        };
        let (done_tx, done_rx) = oneshot::channel();
        let size = if entry.is_empty() {
            decoded
                .effects
                .iter()
                .map(|eff| match eff {
                    crate::rsm::effect::Effect::Append { blob, hashes, .. } => {
                        blob.len() + hashes.len() + 64
                    }
                    _ => 64,
                })
                .sum()
        } else {
            entry.len()
        };
        let pending = Pending {
            size,
            bytes: entry,
            entry: decoded,
            done: done_tx,
            proposed_at: crate::rsm::timing::stamp(),
            pre,
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

        // Start the qlog codec on the payloads now (a copy each, on the codec
        // pool): it runs while the writer fsyncs the group ahead of this one.
        self.submit(entry, Arc::new(decoded), deadline).await
    }

    /// On the queue-log path the writer stores the planned entry itself and
    /// never writes the encoded form: the batcher may skip the encode.
    fn wants_bytes(&self) -> bool {
        !self.qlog_codec || !propose_entry_on()
    }

    async fn propose_entry(
        &self,
        entry: Bytes,
        planned: Arc<crate::rsm::entry::Entry>,
        deadline: Instant,
    ) -> Result<AppliedAt, ProposeError> {
        if let Some(why) = self.shared.is_poisoned() {
            return Err(ProposeError::Fatal(why));
        }
        if !self.qlog_codec || !propose_entry_on() {
            // The raft-log path writes the encoded bytes: keep `propose`.
            return self.propose(entry, deadline).await;
        }
        // The planned entry IS the entry (no decode, no payload copy); it is
        // submitted in this first poll, so the index order is the plan order.
        self.submit(entry, planned, deadline).await
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
