//! openraft's log storage, on the queue logs.
//!
//! There is no second write-ahead log. An entry openraft appends goes through
//! the same group write the local replicator uses ([`QlogWrite::write_group`]):
//! its payloads into their queues' logs, one payload-free entry record (now
//! carrying the entry's TERM) into every queue log it touches, ONE fsync of
//! every touched log, and only then openraft's flush callback. openraft's own
//! entries (a leader's blank entry, a membership change) are entry records in
//! the system log with a body [`encode_internal`] marks.
//!
//! # What lives where
//!
//! - **Entries**: the queue logs. The entries openraft may still read — those
//!   above the store's applied index — are also kept in memory
//!   ([`Mem::cache`]), so apply never reads a file. An entry evicted once
//!   applied is read back from the queue logs if a caller ever asks
//!   ([`QLogReader::entry_records_range`]).
//! - **Vote and purge point**: `raft/state.json`, replaced atomically
//!   (temporary file, fsync, rename, directory fsync) before the call returns.
//!   Written by the writer thread, so a vote and an append are ordered, as
//!   openraft requires.
//! - **Commit point**: `raft/committed.json`, rewritten in place and never
//!   fsynced. openraft calls it optional; a lost or torn copy only means the
//!   node re-applies less at restart and catches up once it commits again.
//!
//! # What stays readable, and for how long
//!
//! openraft may read any entry above the last one it PURGED — to apply it, or
//! to send it to a follower that is behind. So queue-log retention reclaims
//! only files wholly at or below BOTH the store's durable index (recovery
//! replays above it) and the purge point ([`FloorGate`]). openraft purges only
//! entries a snapshot covers, and the replicator decides when
//! ([`super`]'s purge driver): after every live follower has them.
//!
//! An entry in memory has its payloads. One read back from the queue logs has
//! only its payload-free record; [`LogStore::read_range`] restores its payloads
//! from the message records (rehydration), so a follower always receives the
//! full entry.
//!
//! # The store's checkpoint is the snapshot
//!
//! The store reopens exactly at its last durable point, and recovery replays
//! the entries above it from the queue logs; the state machine's snapshot is
//! that checkpoint ([`super::state_machine`], [`super::snapshot`]).

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::ops::{Bound, RangeBounds};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver as StdReceiver, Sender as StdSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use openraft::storage::{IOFlushed, LogState, RaftLogStorage};
use openraft::{EntryPayload, OptionalSend, RaftLogReader};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use super::types::{
    decode_internal, encode_internal, log_id, rsm_index, term_of, AppEntry, LogId, Membership,
    REntry, TypeConfig, Vote, INTERNAL_BLANK, INTERNAL_MEMBERSHIP,
};
use crate::rsm::entry::Entry;
use crate::rsm::qlog::set::{QLogReader, QLogSet};
use crate::rsm::qlog::{EntryRecord, QLogOptions};
use crate::rsm::replicator::local::{
    decode_replayed, seed_pid_qid_from, GroupBody, GroupItem, PartitionLookup, QlogWrite,
};

const STATE_FILE: &str = "state.json";
const COMMITTED_FILE: &str = "committed.json";

/// One write group's caps, as the local writer's.
const GROUP_MAX_ENTRIES: usize = 4096;
const GROUP_MAX_BYTES: usize = 4 * 1024 * 1024;

/// What `state.json` holds.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Persisted {
    vote: Option<Vote>,
    purged: Option<LogId>,
    /// `1`: retention has never reclaimed an entry above `purged`
    /// ([`FloorGate`]). `0` (a directory from the single-voter replicator
    /// before the cluster step): entries at or below the store's applied index
    /// may be gone, so they are treated as purged.
    #[serde(default)]
    floor_v: u8,
}

const FLOOR_V: u8 = 1;

/// Which queue-log files retention may reclaim: those wholly at or below both
/// the store's durable index and the last entry openraft purged. Shared by
/// the apply thread's notifier (durable points) and the log store (purges).
pub(crate) struct FloorGate {
    durable: AtomicU64,
    /// RSM index of the last purged entry, `0` for none.
    purged: AtomicU64,
    /// The queue logs' recovery floor.
    floor: Arc<AtomicU64>,
}

impl FloorGate {
    fn new(floor: Arc<AtomicU64>, durable: u64, purged: u64) -> Arc<FloorGate> {
        let g = Arc::new(FloorGate {
            durable: AtomicU64::new(durable),
            purged: AtomicU64::new(purged),
            floor,
        });
        g.raise();
        g
    }

    /// The store made `index` durable.
    pub(crate) fn durable(&self, index: u64) {
        self.durable.fetch_max(index, Ordering::AcqRel);
        self.raise();
    }

    /// openraft purged every entry up to RSM index `index`.
    pub(crate) fn purged(&self, index: u64) {
        self.purged.fetch_max(index, Ordering::AcqRel);
        self.raise();
    }

    /// The store's durable index as last reported.
    pub(crate) fn durable_index(&self) -> u64 {
        self.durable.load(Ordering::Acquire)
    }

    fn raise(&self) {
        let f = self
            .durable
            .load(Ordering::Acquire)
            .min(self.purged.load(Ordering::Acquire));
        self.floor.fetch_max(f, Ordering::AcqRel);
    }
}

/// The poison flag shared with the replicator: set once, by the first fatal
/// log error; every later write fails with it.
pub(crate) type Poison = Arc<Mutex<Option<String>>>;

/// The in-memory side of the log.
struct Mem {
    /// openraft index → entry, for every entry that has not been evicted
    /// since it was appended (or recovered above the store's applied index).
    cache: BTreeMap<u64, REntry>,
    /// What the cached entries hold in memory (payloads and wire bytes).
    cache_bytes: usize,
    /// The first index the log holds when it holds any: entries below it were
    /// purged or never existed (an empty log may begin anywhere).
    start: u64,
    last_log_id: Option<LogId>,
    purged: Option<LogId>,
    vote: Option<Vote>,
    committed: Option<LogId>,
}

impl Mem {
    /// Nothing between the purge point and the end.
    fn is_empty(&self) -> bool {
        self.last_log_id == self.purged
    }
}

struct Inner {
    mem: Mutex<Mem>,
    /// `None` once [`LogStore::close`] ran: the writer drains and exits.
    writer_tx: Mutex<Option<StdSender<Cmd>>>,
    reader: QLogReader,
    poison: Poison,
    gate: Arc<FloorGate>,
    /// Above this many cached bytes, applied entries are evicted even if a
    /// follower still needs them (it then reads them back from disk).
    cache_cap: usize,
    /// See [`Written`].
    written: Written,
}

/// The highest entry (RSM numbering) this node's queue logs hold: written and
/// indexed — readable by the pop render and the planner — though not yet
/// fsynced. Set by the writer thread after each group's write, lowered by a
/// truncation.
///
/// openraft may APPLY an entry before this node's own write of it: `append`
/// makes it readable from memory at once, and a quorum of OTHER nodes can
/// commit it (a follower applies what the leader committed; the leader commits
/// on two followers). Apply is then ahead of the queue logs, and a pop answered
/// right after apply renders offsets whose payload records are not there yet:
/// the client got a short or empty batch, never acked the rest, and the lease
/// froze the partition for its whole length (measured on a 3-node cluster at
/// 300k msg/s: ~1 lease/s, e2e p99 28 s; the slowest disk had 92% of them). So
/// the state machine waits for this before it applies ([`LogStore::wait_written`]).
type Written = Arc<tokio::sync::watch::Sender<u64>>;

/// `QUEEN_RAFT_LOG_CACHE_MB` (default 512): see [`Inner::cache_cap`].
pub(crate) fn cache_cap_from_env() -> usize {
    std::env::var("QUEEN_RAFT_LOG_CACHE_MB")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(512)
        .max(1)
        * 1024
        * 1024
}

/// openraft's [`RaftLogStorage`] and [`RaftLogReader`] over the queue logs.
/// Cloning shares the same log.
#[derive(Clone)]
pub struct LogStore {
    inner: Arc<Inner>,
}

/// What [`LogStore::open`] needs.
pub(crate) struct OpenCfg {
    /// `<data_dir>/qlog`.
    pub qlog_root: PathBuf,
    pub qopts: QLogOptions,
    /// `<data_dir>/raft`: `state.json`, `committed.json`.
    pub state_dir: PathBuf,
    /// `pid -> queue_id` from the committed partition catalog.
    pub lookup: PartitionLookup,
    /// The store's durable index (RSM numbering): recovery replays after it.
    pub durable_index: u64,
    /// The store's applied entry as an openraft log id.
    pub applied: Option<LogId>,
    /// `meta::QLOG_DURABLE_INDEX`: the reopened queue logs must not be behind it.
    pub qlog_durable_index: u64,
    pub poison: Poison,
    /// See [`Inner::cache_cap`].
    pub cache_cap: usize,
}

/// What [`LogStore::open`] recovered.
pub(crate) struct Opened {
    pub store: LogStore,
    pub writer: JoinHandle<()>,
    /// The reader over the same live logs (pop payloads, the planner's dedup).
    pub reader: QLogReader,
    /// Entries replayed from the queue logs above the store's durable index.
    pub recovered: u64,
    /// Nothing was ever written: no vote, no entry, nothing applied.
    pub fresh: bool,
    /// The retention floor's two inputs; the apply thread's notifier reports
    /// durable points to it.
    pub gate: Arc<FloorGate>,
}

impl LogStore {
    /// Reopen the queue logs, recover the consensus log from them and start the
    /// writer thread. A boot call (blocking I/O).
    pub(crate) fn open(cfg: OpenCfg) -> io::Result<Opened> {
        fs::create_dir_all(&cfg.state_dir)?;
        let persisted = read_state(&cfg.state_dir)?;
        let committed = read_committed(&cfg.state_dir);

        let mut set = QLogSet::new(cfg.qlog_root.clone(), cfg.qopts);
        let tail = set.reopen_all()?;
        if tail < cfg.qlog_durable_index {
            return Err(io::Error::other(format!(
                "queue logs' durable tail {tail} is BEHIND the store's qlog-durable index {}: \
                 a committed record is missing (NA-QLOG-I1)",
                cfg.qlog_durable_index
            )));
        }
        // What openraft may still read starts right after the purge point. A
        // directory from before the retention gate (`floor_v` 0) may have lost
        // entries at or below the store's applied index: those count as purged.
        let legacy = cfg.state_dir.join(STATE_FILE).exists() && persisted.floor_v < FLOOR_V;
        let purged = if legacy {
            max_log_id(persisted.purged, cfg.applied)
        } else {
            persisted.purged
        };
        let gate = FloorGate::new(
            set.recovery_floor_handle(),
            cfg.durable_index,
            purged.map_or(0, |p| rsm_index(p.index)),
        );
        if legacy || !cfg.state_dir.join(STATE_FILE).exists() {
            let state = Persisted {
                vote: persisted.vote,
                purged,
                floor_v: FLOOR_V,
            };
            write_atomic(
                &cfg.state_dir,
                STATE_FILE,
                &serde_json::to_vec(&state).map_err(io::Error::other)?,
            )?;
        }

        // Replay every complete entry above the store's durable point. The ones
        // above the store's applied index are openraft's unapplied log and are
        // kept in memory; those between the purge point and the applied index
        // stay on disk and are read back on demand.
        let applied_index = cfg.applied.as_ref().map(|l| l.index);
        let mut pid_qid = std::collections::HashMap::new();
        let mut cache: BTreeMap<u64, REntry> = BTreeMap::new();
        let mut last: Option<LogId> = None;
        let mut recovered = 0u64;
        let scan = set.scan_entries(cfg.durable_index + 1, &mut |rec| {
            let (entry, rsm) = entry_of_record(&rec)?;
            if let Some(e) = &rsm {
                seed_pid_qid_from(&mut pid_qid, e);
            }
            recovered += 1;
            if applied_index.is_none_or(|a| entry.log_id.index > a) {
                last = Some(entry.log_id);
                cache.insert(entry.log_id.index, entry);
            }
            Ok(())
        })?;
        // Everything at or above the cut belongs to a group whose fsyncs did
        // not all land: never acknowledged, so dropped before its indexes are
        // reused. Across sealed files: a roll during the next group's write
        // may have sealed a file holding part of an unacknowledged group.
        let dropped = set.truncate_from_across(scan.next_seq)?;
        if scan.stopped.is_some() || dropped > 0 {
            tracing::warn!(
                target: "rsm",
                cut = scan.next_seq,
                discarded = scan.discarded,
                dropped_bytes = dropped,
                why = scan.stopped.as_deref().unwrap_or("stale records above the cut"),
                "raft log recovery: dropped an unacknowledged tail",
            );
        }

        let last_log_id = max_log_id(max_log_id(last, cfg.applied), purged);
        let fresh = persisted.vote.is_none() && last_log_id.is_none();
        let reader = set.reader();
        let start = purged.map_or(0, |p| p.index + 1);
        let cache_bytes = cache.values().map(entry_mem).sum();
        let mem = Mem {
            cache,
            cache_bytes,
            start,
            last_log_id,
            purged,
            vote: persisted.vote,
            committed,
        };

        let committed_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(cfg.state_dir.join(COMMITTED_FILE))?;
        // Everything recovered is on disk.
        let written: Written = Arc::new(tokio::sync::watch::Sender::new(
            last_log_id.map_or(0, |l| rsm_index(l.index)),
        ));
        let (tx, rx) = std::sync::mpsc::channel::<Cmd>();
        let (sync_tx, syncer, committed_file) =
            if crate::rsm::replicator::local::writer_pipeline_from_env() {
                let (stx, srx) = std::sync::mpsc::sync_channel::<SyncMsg>(sync_ahead_from_env());
                let syncer = Syncer {
                    syncer: set.syncer(),
                    committed_file,
                    poison: cfg.poison.clone(),
                };
                let join = std::thread::Builder::new()
                    .name("queen-raft-sync".into())
                    .spawn(move || syncer.run(srx))
                    .map_err(|e| io::Error::other(format!("spawn the raft log syncer: {e}")))?;
                (Some(stx), Some(join), None)
            } else {
                (None, None, Some(committed_file))
            };
        let writer = Writer {
            q: QlogWrite {
                set,
                pid_qid,
                lookup: cfg.lookup,
            },
            state_dir: cfg.state_dir,
            committed_file,
            poison: cfg.poison.clone(),
            sync_tx,
            syncer,
            written: written.clone(),
        };
        let join = std::thread::Builder::new()
            .name("queen-raft-log".into())
            .spawn(move || writer.run(rx))
            .map_err(|e| io::Error::other(format!("spawn the raft log writer: {e}")))?;
        Ok(Opened {
            store: LogStore {
                inner: Arc::new(Inner {
                    mem: Mutex::new(mem),
                    writer_tx: Mutex::new(Some(tx)),
                    reader: reader.clone(),
                    poison: cfg.poison,
                    gate: gate.clone(),
                    cache_cap: cfg.cache_cap,
                    written,
                }),
            },
            writer: join,
            reader,
            recovered,
            fresh,
            gate,
        })
    }

    /// Stop accepting writes: the writer drains what it has and exits once
    /// every clone of this store has also been dropped by openraft.
    pub(crate) fn close(&self) {
        self.inner.writer_tx.lock().expect("writer_tx").take();
    }

    /// Wait until this node's queue logs hold the entry at `raft_index`
    /// (openraft numbering) and everything before it — see [`Written`]. Fails
    /// once the log is poisoned (nothing more will be written).
    pub(crate) async fn wait_written(&self, raft_index: u64) -> io::Result<()> {
        let want = rsm_index(raft_index);
        if *self.inner.written.borrow() >= want {
            return Ok(());
        }
        crate::rsm::dbgctr::inc(&crate::rsm::dbgctr::C.apply_waited_write, 1);
        let t0 = std::time::Instant::now();
        let mut rx = self.inner.written.subscribe();
        loop {
            match tokio::time::timeout(std::time::Duration::from_millis(100), rx.wait_for(|w| *w >= want))
                .await
            {
                Ok(Ok(_)) => {
                    let us = t0.elapsed().as_micros() as u64;
                    crate::rsm::dbgctr::max(&crate::rsm::dbgctr::C.apply_wait_max_us, us);
                    if us > 50_000 {
                        crate::rsm::dbgctr::inc(&crate::rsm::dbgctr::C.apply_wait_over_50ms, 1);
                    }
                    return Ok(());
                }
                Ok(Err(_)) => return Err(io::Error::other("the raft log writer stopped")),
                Err(_) => {
                    if let Some(why) = self.inner.poison.lock().expect("poison").clone() {
                        return Err(io::Error::other(why));
                    }
                }
            }
        }
    }

    /// The last entry in the log (openraft numbering).
    pub(crate) fn last_log_id(&self) -> Option<LogId> {
        self.inner.mem.lock().expect("log mem").last_log_id
    }

    /// Entries in memory and their bytes (gauges).
    pub(crate) fn cached(&self) -> (usize, usize) {
        let m = self.inner.mem.lock().expect("log mem");
        (m.cache.len(), m.cache_bytes)
    }

    /// Drop the in-memory copy of every entry at or below `applied` (openraft
    /// indexes) that every live follower also has (`replicated`): they stay in
    /// the queue logs and are read back from there if anyone asks. Over the
    /// cache cap, everything applied goes regardless of the followers.
    pub(crate) fn evict(&self, applied: u64, replicated: u64) {
        let mut m = self.inner.mem.lock().expect("log mem");
        let upto = if m.cache_bytes > self.inner.cache_cap {
            applied
        } else {
            applied.min(replicated)
        };
        if m.cache.first_key_value().is_none_or(|(k, _)| *k > upto) {
            return;
        }
        let keep = m.cache.split_off(&(upto.saturating_add(1)));
        let gone = std::mem::replace(&mut m.cache, keep);
        let freed: usize = gone.values().map(entry_mem).sum();
        m.cache_bytes = m.cache_bytes.saturating_sub(freed);
    }

    /// The retention floor's inputs.
    pub(crate) fn gate(&self) -> &Arc<FloorGate> {
        &self.inner.gate
    }

    fn send(&self, cmd: Cmd) -> io::Result<()> {
        if let Some(why) = self.inner.poison.lock().expect("poison").clone() {
            return Err(io::Error::other(why));
        }
        match self.inner.writer_tx.lock().expect("writer_tx").as_ref() {
            Some(tx) => tx
                .send(cmd)
                .map_err(|_| io::Error::other("the raft log writer has stopped")),
            None => Err(io::Error::other("the raft log is closed")),
        }
    }

    /// Every entry with an openraft index in `[start, end)` the log holds, in
    /// order: the queue logs below the in-memory window (payloads restored),
    /// then the window.
    fn read_range(&self, start: u64, end: u64) -> io::Result<Vec<REntry>> {
        let (disk_end, from_cache, start) = {
            let m = self.inner.mem.lock().expect("log mem");
            let first = m.purged.map_or(0, |p| p.index + 1).max(m.start);
            let last_excl = m.last_log_id.map_or(0, |l| l.index + 1);
            let start = start.max(first);
            let end = end.min(last_excl);
            if start >= end {
                return Ok(Vec::new());
            }
            let cache_start = m.cache.keys().next().copied().unwrap_or(end).min(end);
            let from_cache: Vec<REntry> = m
                .cache
                .range(start.max(cache_start)..end)
                .map(|(_, e)| e.clone())
                .collect();
            (cache_start, from_cache, start)
        };
        if start >= disk_end {
            return Ok(from_cache);
        }
        // Below the window: committed history, immutable, in the queue logs.
        let recs = self
            .inner
            .reader
            .entry_records_range(rsm_index(start), rsm_index(disk_end))?;
        if recs.len() as u64 != disk_end - start {
            return Err(io::Error::other(format!(
                "raft log entries {start}..{disk_end} are not all in the queue logs \
                 ({} found)",
                recs.len()
            )));
        }
        let mut out = Vec::with_capacity(recs.len() + from_cache.len());
        for (rec, qids) in &recs {
            out.push(rehydrate(&self.inner.reader, rec, qids)?);
        }
        out.extend(from_cache);
        Ok(out)
    }
}

/// What a cached entry holds in memory.
fn entry_mem(e: &REntry) -> usize {
    match &e.payload {
        EntryPayload::Normal(app) => app.mem_bytes(),
        _ => 64,
    }
}

/// An entry read back from the queue logs, whole: its payload-free record plus
/// every `Append` payload AS STORED (still compressed) from the message record
/// that holds it — in one of the logs holding a copy of the entry (`logs`),
/// since an `Append` always touches its own lane's log. The result is a
/// stored-form entry: sent to a follower as it is, never recompressed.
fn rehydrate(reader: &QLogReader, rec: &EntryRecord, logs: &[u64]) -> io::Result<REntry> {
    use crate::rsm::effect::Effect;
    use crate::rsm::qlog::codec::StoredPayload;
    let (mut entry, pf) = entry_of_record(rec)?;
    let Some(pf) = pf else {
        return Ok(entry);
    };
    let mut payloads = Vec::new();
    for eff in pf.effects.iter() {
        let Effect::Append {
            pid,
            base_offset,
            count,
            ..
        } = eff
        else {
            continue;
        };
        let mut found = None;
        for log in logs {
            if let Some(r) = reader.read_stored_in(*log, *pid, *base_offset)? {
                if r.seq == rec.seq && r.base_offset == *base_offset && r.count == *count {
                    found = Some(r);
                    break;
                }
            }
        }
        let r = found.ok_or_else(|| {
            io::Error::other(format!(
                "entry {}: the payload of pid {pid} offset {base_offset} is not in its queue log",
                rec.seq
            ))
        })?;
        payloads.push(StoredPayload {
            zstd: r.zstd,
            bytes: bytes::Bytes::from(r.payload),
        });
    }
    entry.payload = EntryPayload::Normal(AppEntry::stored(
        pf,
        bytes::Bytes::from(rec.entry.clone()),
        payloads,
        None,
    ));
    Ok(entry)
}

/// The later of two optional log ids.
fn max_log_id(a: Option<LogId>, b: Option<LogId>) -> Option<LogId> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if a >= b { a } else { b }),
        (a, None) => a,
        (None, b) => b,
    }
}

/// An openraft entry from a queue-log entry record, plus the RSM entry when it
/// is an application entry.
fn entry_of_record(rec: &EntryRecord) -> io::Result<(REntry, Option<Arc<Entry>>)> {
    if rec.seq == 0 {
        return Err(io::Error::other("entry record with seq 0"));
    }
    let id = log_id(rec.term, rec.seq - 1);
    if let Some((kind, payload)) = decode_internal(&rec.entry) {
        let payload = match kind {
            INTERNAL_BLANK => EntryPayload::Blank,
            INTERNAL_MEMBERSHIP => {
                let m: Membership = serde_json::from_slice(payload).map_err(|e| {
                    io::Error::other(format!("membership entry {} does not decode: {e}", rec.seq))
                })?;
                EntryPayload::Membership(m)
            }
            k => {
                return Err(io::Error::other(format!(
                    "entry record {} carries an unknown consensus kind {k}",
                    rec.seq
                )))
            }
        };
        return Ok((
            REntry {
                log_id: id,
                payload,
            },
            None,
        ));
    }
    let e = Arc::new(decode_replayed(rec.seq, &rec.entry)?);
    Ok((
        REntry {
            log_id: id,
            payload: EntryPayload::Normal(AppEntry::recovered(e.clone())),
        },
        Some(e),
    ))
}

/// After a snapshot replaced the data directory (at boot, before the log
/// opens): every entry up to the snapshot's `purged` is gone from this node's
/// log, the vote stays, and the commit point is unknown again.
pub(crate) fn state_after_snapshot(state_dir: &Path, purged: Option<LogId>) -> io::Result<()> {
    fs::create_dir_all(state_dir)?;
    let old = read_state(state_dir)?;
    let state = Persisted {
        vote: old.vote,
        purged: max_log_id(old.purged, purged),
        floor_v: FLOOR_V,
    };
    write_atomic(
        state_dir,
        STATE_FILE,
        &serde_json::to_vec(&state).map_err(io::Error::other)?,
    )?;
    match fs::remove_file(state_dir.join(COMMITTED_FILE)) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
        _ => {}
    }
    Ok(())
}

fn read_state(dir: &Path) -> io::Result<Persisted> {
    match fs::read(dir.join(STATE_FILE)) {
        Ok(b) => serde_json::from_slice(&b)
            .map_err(|e| io::Error::other(format!("raft/{STATE_FILE} does not parse: {e}"))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Persisted::default()),
        Err(e) => Err(e),
    }
}

/// The commit point written in place without fsync: a torn or missing file
/// reads as "unknown", which is always safe.
fn read_committed(dir: &Path) -> Option<LogId> {
    let b = fs::read(dir.join(COMMITTED_FILE)).ok()?;
    let end = b.iter().position(|c| *c == b'\n').unwrap_or(b.len());
    serde_json::from_slice::<Option<LogId>>(&b[..end])
        .ok()
        .flatten()
}

/// Replace `dir/name` durably: a temporary file, fsync, rename, directory fsync.
pub(crate) fn write_atomic(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, dir.join(name))?;
    File::open(dir)?.sync_all()
}

fn range_bounds<RB: RangeBounds<u64>>(range: RB) -> (u64, u64) {
    let start = match range.start_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n + 1,
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(&n) => n + 1,
        Bound::Excluded(&n) => n,
        Bound::Unbounded => u64::MAX,
    };
    (start, end)
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<REntry>, io::Error> {
        let (start, end) = range_bounds(range);
        self.read_range(start, end)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        Ok(self.inner.mem.lock().expect("log mem").vote)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogStore;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let m = self.inner.mem.lock().expect("log mem");
        Ok(LogState {
            last_purged_log_id: m.purged,
            last_log_id: max_log_id(m.last_log_id, m.purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        let state = {
            let mut m = self.inner.mem.lock().expect("log mem");
            m.vote = Some(*vote);
            Persisted {
                vote: m.vote,
                purged: m.purged,
                floor_v: FLOOR_V,
            }
        };
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Persist { state, done: tx })?;
        rx.await.map_err(|_| {
            io::Error::other("the raft log writer stopped before the vote was saved")
        })?
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        self.inner.mem.lock().expect("log mem").committed = committed;
        self.send(Cmd::Committed { committed })
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        Ok(self.inner.mem.lock().expect("log mem").committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = REntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<REntry> = entries.into_iter().collect();
        if entries.is_empty() {
            callback.io_completed(Ok(()));
            return Ok(());
        }
        {
            // Readable the moment this returns (openraft's contract), before
            // the fsync.
            let mut m = self.inner.mem.lock().expect("log mem");
            // An empty log (nothing appended, nothing purged) may start at any
            // index; after that every append continues the log exactly.
            let mut next = m
                .last_log_id
                .map_or(entries[0].log_id.index, |l| l.index + 1);
            if m.is_empty() {
                m.start = next;
            }
            for e in &entries {
                if e.log_id.index != next {
                    return Err(io::Error::other(format!(
                        "raft append would leave a hole: expected index {next}, got {}",
                        e.log_id.index
                    )));
                }
                next += 1;
                m.cache_bytes += entry_mem(e);
                if let Some(old) = m.cache.insert(e.log_id.index, e.clone()) {
                    m.cache_bytes = m.cache_bytes.saturating_sub(entry_mem(&old));
                }
                m.last_log_id = Some(e.log_id);
            }
        }
        self.send(Cmd::Append { entries, callback })
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        let cut = last_log_id.map_or(0, |l| l.index + 1);
        let forget_pids = {
            let mut m = self.inner.mem.lock().expect("log mem");
            if m.last_log_id.is_none_or(|l| l.index < cut) {
                return Ok(());
            }
            let removed = m.cache.split_off(&cut);
            let freed: usize = removed.values().map(entry_mem).sum();
            m.cache_bytes = m.cache_bytes.saturating_sub(freed);
            m.last_log_id = max_log_id(last_log_id, m.purged);
            // A partition created by an entry that is being thrown away must not
            // keep routing its pid (a new leader may give that pid to another
            // queue).
            let mut pids = Vec::new();
            for e in removed.values() {
                if let EntryPayload::Normal(app) = &e.payload {
                    if let Ok(pf) = app.payload_free() {
                        for eff in &pf.effects {
                            if let crate::rsm::effect::Effect::PartitionCreate { pid, .. } = eff {
                                pids.push(*pid);
                            }
                        }
                    }
                }
            }
            pids
        };
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Truncate {
            from_seq: rsm_index(cut),
            forget_pids,
            done: tx,
        })?;
        rx.await
            .map_err(|_| io::Error::other("the raft log writer stopped before the truncation"))?
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        let state = {
            let mut m = self.inner.mem.lock().expect("log mem");
            if m.purged.is_some_and(|p| p >= log_id) {
                return Ok(());
            }
            m.purged = Some(log_id);
            m.start = m.start.max(log_id.index + 1);
            let keep = m.cache.split_off(&(log_id.index + 1));
            let gone = std::mem::replace(&mut m.cache, keep);
            let freed: usize = gone.values().map(entry_mem).sum();
            m.cache_bytes = m.cache_bytes.saturating_sub(freed);
            if m.last_log_id.is_none_or(|l| l < log_id) {
                m.last_log_id = Some(log_id);
            }
            Persisted {
                vote: m.vote,
                purged: m.purged,
                floor_v: FLOOR_V,
            }
        };
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Persist { state, done: tx })?;
        rx.await
            .map_err(|_| io::Error::other("the raft log writer stopped before the purge"))??;
        // Durable: retention may now reclaim the files wholly at or below it
        // (and the store's durable index).
        self.inner.gate.purged(rsm_index(log_id.index));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The writer thread
// ---------------------------------------------------------------------------

/// What the writer does, strictly in arrival order (openraft requires every
/// write — votes included — to be serialized).
enum Cmd {
    Append {
        entries: Vec<REntry>,
        callback: IOFlushed<TypeConfig>,
    },
    Truncate {
        from_seq: u64,
        forget_pids: Vec<u64>,
        done: oneshot::Sender<io::Result<()>>,
    },
    Persist {
        state: Persisted,
        done: oneshot::Sender<io::Result<()>>,
    },
    Committed {
        committed: Option<LogId>,
    },
}

impl Cmd {
    fn entries(&self) -> usize {
        match self {
            Cmd::Append { entries, .. } => entries.len(),
            _ => 0,
        }
    }

    fn bytes(&self) -> usize {
        match self {
            Cmd::Append { entries, .. } => entries.iter().map(entry_size).sum(),
            _ => 0,
        }
    }
}

/// The payload bytes an entry brings to a group (the cap's estimate).
fn entry_size(e: &REntry) -> usize {
    match &e.payload {
        EntryPayload::Normal(app) => {
            if let Some(f) = app.full() {
                f.effects
                    .iter()
                    .map(|eff| match eff {
                        crate::rsm::effect::Effect::Append { blob, hashes, .. } => {
                            blob.len() + hashes.len() + 64
                        }
                        _ => 64,
                    })
                    .sum()
            } else if let Some((pf, pfb, payloads)) = app.stored_parts() {
                pfb.len()
                    + pf.effects.len() * 16
                    + payloads.iter().map(|p| p.bytes.len()).sum::<usize>()
            } else {
                64
            }
        }
        _ => 64,
    }
}

struct Writer {
    q: QlogWrite,
    state_dir: PathBuf,
    /// Written here only when there is no syncer; otherwise the syncer owns it.
    committed_file: Option<File>,
    poison: Poison,
    /// The fsync half (`QUEEN_RAFT_WRITER_PIPELINE`, default on): this thread
    /// keeps writing groups while the syncer fsyncs the earlier ones and
    /// answers openraft. Bounded to `QUEEN_RAFT_SYNC_AHEAD` groups: a slow
    /// fsync must not stop the WRITES, because apply waits for them
    /// ([`Written`]) — with a rendezvous here one 100 ms fsync on the leader
    /// held every apply behind it (measured: push p99 684 ms for 10 s at 450k
    /// msg/s). The syncer fsyncs every group waiting for it at once.
    sync_tx: Option<std::sync::mpsc::SyncSender<SyncMsg>>,
    syncer: Option<JoinHandle<()>>,
    /// See [`Written`]: raised after each group's write.
    written: Written,
}

/// What the writer hands its syncer, in order.
enum SyncMsg {
    /// A written group: fsync its logs, then answer openraft.
    Group {
        ticket: crate::rsm::qlog::set::SyncTicket,
        items: Vec<GroupItem>,
        apps: Vec<Option<AppEntry>>,
        callbacks: Vec<IOFlushed<TypeConfig>>,
    },
    /// The commit point, written after every earlier group is durable.
    Committed(Option<LogId>),
    /// Answered once every earlier message is done (before a truncation or a
    /// vote write, which must follow every earlier append).
    Barrier(std::sync::mpsc::SyncSender<()>),
}

struct Syncer {
    syncer: crate::rsm::qlog::set::QLogSyncer,
    committed_file: File,
    poison: Poison,
}

impl Syncer {
    /// Take every message waiting (up to [`SYNC_BATCH_MAX`]), fsync the logs of
    /// all their groups ONCE, then handle them in arrival order: each group
    /// answered, each commit point written, each barrier released — every one
    /// after the fsync that covers all the groups before it.
    fn run(mut self, rx: StdReceiver<SyncMsg>) {
        while let Ok(first) = rx.recv() {
            let mut batch = vec![first];
            while batch.len() < SYNC_BATCH_MAX {
                match rx.try_recv() {
                    Ok(m) => batch.push(m),
                    Err(_) => break,
                }
            }
            let mut qids: Vec<u64> = Vec::new();
            let mut seq = 0u64;
            let mut groups = 0usize;
            for m in &batch {
                if let SyncMsg::Group { ticket, .. } = m {
                    qids.extend_from_slice(&ticket.qids);
                    seq = seq.max(ticket.seq);
                    groups += 1;
                }
            }
            let failed: Option<String> = if groups == 0 {
                None
            } else {
                qids.sort_unstable();
                qids.dedup();
                let poisoned = self.poison.lock().expect("poison").clone();
                match poisoned {
                    Some(why) => Some(why),
                    None => {
                        let s0 = crate::rsm::timing::stamp();
                        let r = self
                            .syncer
                            .sync(&crate::rsm::qlog::set::SyncTicket { qids, seq });
                        if let Some(s0) = s0 {
                            crate::rsm::timing::metrics()
                                .log_fsync
                                .record_dur(s0.elapsed());
                        }
                        match r {
                            Ok(()) => None,
                            Err(e) => {
                                let why = format!("raft log fsync failed: {e}");
                                set_poison(&self.poison, why.clone());
                                Some(why)
                            }
                        }
                    }
                }
            };
            for m in batch {
                match m {
                    SyncMsg::Group {
                        items,
                        apps,
                        callbacks,
                        ..
                    } => match &failed {
                        None => {
                            crate::rsm::faults::hit("qlog.record_fsynced");
                            crate::rsm::faults::hit("log.flushed");
                            finish_group(&items, apps, callbacks);
                        }
                        Some(why) => {
                            for cb in callbacks {
                                cb.io_completed(Err(io::Error::other(why.clone())));
                            }
                        }
                    },
                    SyncMsg::Committed(c) => write_committed_to(&mut self.committed_file, c),
                    SyncMsg::Barrier(done) => {
                        let _ = done.send(());
                    }
                }
            }
        }
    }
}

/// At most this many queued messages per syncer pass.
const SYNC_BATCH_MAX: usize = 256;

/// `QUEEN_RAFT_SYNC_AHEAD` (default 16): how many written groups may wait for
/// the syncer before the writer blocks (see [`Writer::sync_tx`]).
fn sync_ahead_from_env() -> usize {
    std::env::var("QUEEN_RAFT_SYNC_AHEAD")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(16)
}

/// A group is durable: record each entry's payload-free form and answer
/// openraft, in order.
fn finish_group(
    items: &[GroupItem],
    apps: Vec<Option<AppEntry>>,
    callbacks: Vec<IOFlushed<TypeConfig>>,
) {
    for (it, app) in items.iter().zip(apps) {
        if let (GroupBody::Entry { entry, .. }, Some(app)) = (&it.body, app) {
            app.set_payload_free(entry.clone());
        }
    }
    for cb in callbacks {
        cb.io_completed(Ok(()));
    }
}

fn set_poison(poison: &Poison, why: String) {
    let mut g = poison.lock().expect("poison");
    if g.is_none() {
        tracing::error!(target: "rsm", why = %why, "raft log writer poisoned; node stops");
        *g = Some(why);
    }
}

/// The commit point, in place and without fsync (see the module header).
fn write_committed_to(file: &mut File, committed: Option<LogId>) {
    if let Ok(mut b) = serde_json::to_vec(&committed) {
        b.push(b'\n');
        let _ = file
            .write_all_at(&b, 0)
            .and_then(|()| file.set_len(b.len() as u64));
    }
}

impl Writer {
    fn run(mut self, rx: StdReceiver<Cmd>) {
        let mut group: Vec<(Vec<REntry>, IOFlushed<TypeConfig>)> = Vec::new();
        loop {
            let first = match rx.recv() {
                Ok(c) => c,
                Err(_) => break,
            };
            let mut batch = vec![first];
            let mut entries = batch[0].entries();
            let mut bytes = batch[0].bytes();
            let mut closed = false;
            while entries < GROUP_MAX_ENTRIES && bytes < GROUP_MAX_BYTES {
                match rx.try_recv() {
                    Ok(c) => {
                        entries += c.entries();
                        bytes += c.bytes();
                        batch.push(c);
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        closed = true;
                        break;
                    }
                }
            }
            // A commit point never splits a group (it would cost an fsync):
            // the latest one is written after the group, whose entries it
            // cannot be ahead of.
            let mut committed: Option<Option<LogId>> = None;
            for cmd in batch {
                match cmd {
                    Cmd::Append { entries, callback } => group.push((entries, callback)),
                    Cmd::Committed { committed: c } => committed = Some(c),
                    other => {
                        self.flush(&mut group);
                        self.exec(other);
                    }
                }
            }
            self.flush(&mut group);
            if let Some(c) = committed {
                self.write_committed(c);
            }
            if closed {
                break;
            }
        }
        // Let the syncer drain what it holds, then stop it.
        self.sync_tx.take();
        if let Some(j) = self.syncer.take() {
            let _ = j.join();
        }
    }

    /// Wait until the syncer has finished every group handed to it.
    fn drain_syncer(&self) {
        if let Some(tx) = &self.sync_tx {
            let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
            if tx.send(SyncMsg::Barrier(done_tx)).is_ok() {
                let _ = done_rx.recv();
            }
        }
    }

    fn poisoned(&self) -> Option<String> {
        self.poison.lock().expect("poison").clone()
    }

    fn set_poison(&self, why: String) {
        set_poison(&self.poison, why);
    }

    /// Write and fsync the pending appends as ONE group, then answer openraft.
    fn flush(&mut self, group: &mut Vec<(Vec<REntry>, IOFlushed<TypeConfig>)>) {
        if group.is_empty() {
            return;
        }
        let appends = std::mem::take(group);
        if let Some(why) = self.poisoned() {
            for (_, cb) in appends {
                cb.io_completed(Err(io::Error::other(why.clone())));
            }
            return;
        }
        let mut items: Vec<GroupItem> = Vec::new();
        let mut apps: Vec<Option<AppEntry>> = Vec::new();
        let mut callbacks = Vec::with_capacity(appends.len());
        let mut failed: Option<String> = None;
        let mut max_seq = 0u64;
        for (entries, cb) in appends {
            for e in entries {
                let seq = rsm_index(e.log_id.index);
                max_seq = max_seq.max(seq);
                let term = term_of(&e.log_id);
                match e.payload {
                    EntryPayload::Normal(app) => {
                        if let Some(full) = app.full() {
                            items.push(GroupItem {
                                seq,
                                term,
                                body: GroupBody::Entry {
                                    entry: full.clone(),
                                    pre: app.take_pre(),
                                    z: app.z(),
                                },
                            });
                            apps.push(Some(app));
                        } else if let Some((entry, pf, payloads)) = app.stored_parts() {
                            // Received (or read back) in stored form: written as
                            // is, and its payload-free form is already set.
                            items.push(GroupItem {
                                seq,
                                term,
                                body: GroupBody::Stored {
                                    entry,
                                    pf,
                                    payloads,
                                },
                            });
                            apps.push(None);
                        } else {
                            failed.get_or_insert_with(|| {
                                format!(
                                    "entry {seq} reached the raft log writer without its payloads"
                                )
                            });
                        }
                    }
                    EntryPayload::Blank => {
                        items.push(GroupItem {
                            seq,
                            term,
                            body: GroupBody::Raw {
                                now_us: 0,
                                bytes: encode_internal(INTERNAL_BLANK, &[]),
                            },
                        });
                        apps.push(None);
                    }
                    EntryPayload::Membership(m) => match serde_json::to_vec(&m) {
                        Ok(json) => {
                            items.push(GroupItem {
                                seq,
                                term,
                                body: GroupBody::Raw {
                                    now_us: 0,
                                    bytes: encode_internal(INTERNAL_MEMBERSHIP, &json),
                                },
                            });
                            apps.push(None);
                        }
                        Err(err) => {
                            failed.get_or_insert_with(|| {
                                format!("membership entry {seq} does not encode: {err}")
                            });
                        }
                    },
                }
            }
            callbacks.push(cb);
        }
        if crate::rsm::timing::enabled() {
            let tm = crate::rsm::timing::metrics();
            tm.group_entries.record(items.len() as u64);
            tm.group_bytes.record(
                items
                    .iter()
                    .map(|it| match &it.body {
                        GroupBody::Entry { entry, .. } => entry
                            .effects
                            .iter()
                            .map(|eff| match eff {
                                crate::rsm::effect::Effect::Append { blob, .. } => {
                                    blob.len() as u64
                                }
                                _ => 0,
                            })
                            .sum::<u64>(),
                        GroupBody::Stored { payloads, .. } => {
                            payloads.iter().map(|p| p.bytes.len() as u64).sum()
                        }
                        GroupBody::Raw { bytes, .. } => bytes.len() as u64,
                    })
                    .sum(),
            );
        }
        if let Some(why) = failed {
            return self.fail_group(why, callbacks);
        }
        // Pipelined: write, then hand the fsync and the answers to the syncer
        // (blocking until it is done with the previous group). Otherwise write
        // and fsync here.
        if let Some(tx) = &self.sync_tx {
            let tw = std::time::Instant::now();
            match self.q.write_group_nosync(&mut items) {
                Ok(ticket) => {
                    crate::rsm::dbgctr::max(
                        &crate::rsm::dbgctr::C.writer_write_max_us,
                        tw.elapsed().as_micros() as u64,
                    );
                    self.note_written(max_seq);
                    let msg = SyncMsg::Group {
                        ticket,
                        items,
                        apps,
                        callbacks,
                    };
                    let th = std::time::Instant::now();
                    let sent = tx.send(msg);
                    crate::rsm::dbgctr::max(
                        &crate::rsm::dbgctr::C.writer_handoff_max_us,
                        th.elapsed().as_micros() as u64,
                    );
                    if let Err(std::sync::mpsc::SendError(msg)) = sent {
                        let SyncMsg::Group { callbacks, .. } = msg else {
                            unreachable!()
                        };
                        self.fail_group("the raft log syncer stopped".into(), callbacks);
                    }
                }
                Err(e) => self.fail_group(format!("raft log write failed: {e}"), callbacks),
            }
            return;
        }
        match self.q.write_group(&mut items) {
            Ok(()) => {
                self.note_written(max_seq);
                finish_group(&items, apps, callbacks)
            }
            Err(e) => self.fail_group(format!("raft log write failed: {e}"), callbacks),
        }
    }

    /// The group through `max_seq` is written and indexed: apply may pass it.
    fn note_written(&self, max_seq: u64) {
        self.written.send_if_modified(|w| {
            let raise = max_seq > *w;
            if raise {
                *w = max_seq;
            }
            raise
        });
    }

    fn fail_group(&self, why: String, callbacks: Vec<IOFlushed<TypeConfig>>) {
        self.set_poison(why.clone());
        for cb in callbacks {
            cb.io_completed(Err(io::Error::other(why.clone())));
        }
    }

    fn exec(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Append { .. } => unreachable!("appends are grouped by run"),
            Cmd::Truncate {
                from_seq,
                forget_pids,
                done,
            } => {
                // Every earlier append is fsynced and answered first.
                self.drain_syncer();
                // What is cut is no longer written; its indexes are rewritten.
                self.written.send_if_modified(|w| {
                    let lower = *w >= from_seq;
                    if lower {
                        *w = from_seq.saturating_sub(1);
                    }
                    lower
                });
                let res = match self.poisoned() {
                    Some(why) => Err(io::Error::other(why)),
                    None => self.q.set.truncate_from_across(from_seq).map(|dropped| {
                        for pid in forget_pids {
                            self.q.pid_qid.remove(&pid);
                        }
                        tracing::info!(
                            target: "rsm",
                            from_seq,
                            dropped_bytes = dropped,
                            "raft log: truncated a conflicting tail",
                        );
                    }),
                };
                if let Err(e) = &res {
                    self.set_poison(format!("raft log truncation failed: {e}"));
                }
                let _ = done.send(res);
            }
            Cmd::Persist { state, done } => {
                let tp = std::time::Instant::now();
                let _tp = scopeguard_max(tp);
                self.drain_syncer();
                let res = match self.poisoned() {
                    Some(why) => Err(io::Error::other(why)),
                    None => serde_json::to_vec(&state)
                        .map_err(io::Error::other)
                        .and_then(|b| write_atomic(&self.state_dir, STATE_FILE, &b)),
                };
                if let Err(e) = &res {
                    self.set_poison(format!("raft state write failed: {e}"));
                }
                let _ = done.send(res);
            }
            Cmd::Committed { committed } => self.write_committed(committed),
        }
    }

    /// The commit point, in place and without fsync (see the module header),
    /// after every group already written is durable.
    fn write_committed(&mut self, committed: Option<LogId>) {
        match (&self.sync_tx, self.committed_file.as_mut()) {
            (Some(tx), _) => {
                let _ = tx.send(SyncMsg::Committed(committed));
            }
            (None, Some(f)) => write_committed_to(f, committed),
            (None, None) => {}
        }
    }
}

/// DIAG: records the persist duration when dropped.
struct PersistTimer(std::time::Instant);
impl Drop for PersistTimer {
    fn drop(&mut self) {
        crate::rsm::dbgctr::max(
            &crate::rsm::dbgctr::C.writer_persist_max_us,
            self.0.elapsed().as_micros() as u64,
        );
    }
}
fn scopeguard_max(t: std::time::Instant) -> PersistTimer {
    PersistTimer(t)
}
