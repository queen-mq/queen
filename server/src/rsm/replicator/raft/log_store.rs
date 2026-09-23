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
//! # The store's checkpoint is the snapshot
//!
//! The store reopens exactly at its last durable point, and recovery replays
//! the entries above it from the queue logs. So at open the entries at or below
//! the store's applied index are reported PURGED, and the state machine's
//! snapshot is that checkpoint ([`super::state_machine`]).

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::ops::{Bound, RangeBounds};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
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
}

/// The poison flag shared with the replicator: set once, by the first fatal
/// log error; every later write fails with it.
pub(crate) type Poison = Arc<Mutex<Option<String>>>;

/// The in-memory side of the log.
struct Mem {
    /// openraft index → entry, for every entry above the store's applied
    /// index that has not been evicted since.
    cache: BTreeMap<u64, REntry>,
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
        set.set_recovery_floor(cfg.durable_index);

        // Replay every complete entry above the store's durable point. Entries
        // at or below the store's applied index are reported purged (the store
        // IS the snapshot); the ones above are openraft's live log.
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
        // reused.
        let dropped = set.truncate_from(scan.next_seq)?;
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

        let purged = max_log_id(persisted.purged, cfg.applied);
        let last_log_id = max_log_id(last, purged);
        let fresh = persisted.vote.is_none() && last_log_id.is_none();
        let reader = set.reader();
        let start = cache
            .keys()
            .next()
            .copied()
            .unwrap_or_else(|| purged.map_or(0, |p| p.index + 1));
        let mem = Mem {
            cache,
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
        let (tx, rx) = std::sync::mpsc::channel::<Cmd>();
        let writer = Writer {
            q: QlogWrite {
                set,
                pid_qid,
                lookup: cfg.lookup,
            },
            state_dir: cfg.state_dir,
            committed_file,
            poison: cfg.poison.clone(),
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
                }),
            },
            writer: join,
            reader,
            recovered,
            fresh,
        })
    }

    /// Stop accepting writes: the writer drains what it has and exits once
    /// every clone of this store has also been dropped by openraft.
    pub(crate) fn close(&self) {
        self.inner.writer_tx.lock().expect("writer_tx").take();
    }

    /// The last entry in the log (openraft numbering).
    pub(crate) fn last_log_id(&self) -> Option<LogId> {
        self.inner.mem.lock().expect("log mem").last_log_id
    }

    /// Entries in memory (a gauge).
    pub(crate) fn cached(&self) -> usize {
        self.inner.mem.lock().expect("log mem").cache.len()
    }

    /// Drop the in-memory copy of every entry at or below `raft_index`: apply
    /// has consumed them. They stay in the queue logs.
    pub(crate) fn evict_applied(&self, raft_index: u64) {
        let mut m = self.inner.mem.lock().expect("log mem");
        let keep = m.cache.split_off(&(raft_index + 1));
        m.cache = keep;
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
    /// order: the in-memory window, and the queue logs below it.
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
        for rec in &recs {
            out.push(entry_of_record(rec)?.0);
        }
        out.extend(from_cache);
        Ok(out)
    }
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
fn write_atomic(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
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
                m.cache.insert(e.log_id.index, e.clone());
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
            m.cache = keep;
            if m.last_log_id.is_none_or(|l| l < log_id) {
                m.last_log_id = Some(log_id);
            }
            Persisted {
                vote: m.vote,
                purged: m.purged,
            }
        };
        // The queue logs keep the records: their own retention reclaims files.
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Persist { state, done: tx })?;
        rx.await
            .map_err(|_| io::Error::other("the raft log writer stopped before the purge"))?
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
        EntryPayload::Normal(app) => app
            .full()
            .map(|f| {
                f.effects
                    .iter()
                    .map(|eff| match eff {
                        crate::rsm::effect::Effect::Append { blob, hashes, .. } => {
                            blob.len() + hashes.len() + 64
                        }
                        _ => 64,
                    })
                    .sum()
            })
            .unwrap_or(64),
        _ => 64,
    }
}

struct Writer {
    q: QlogWrite,
    state_dir: PathBuf,
    committed_file: File,
    poison: Poison,
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
    }

    fn poisoned(&self) -> Option<String> {
        self.poison.lock().expect("poison").clone()
    }

    fn set_poison(&self, why: String) {
        let mut g = self.poison.lock().expect("poison");
        if g.is_none() {
            tracing::error!(target: "rsm", why = %why, "raft log writer poisoned; node stops");
            *g = Some(why);
        }
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
        for (entries, cb) in appends {
            for e in entries {
                let seq = rsm_index(e.log_id.index);
                let term = term_of(&e.log_id);
                match e.payload {
                    EntryPayload::Normal(app) => {
                        match app.full() {
                            Some(full) => {
                                items.push(GroupItem {
                                    seq,
                                    term,
                                    body: GroupBody::Entry {
                                        entry: full.clone(),
                                        pre: app.take_pre(),
                                    },
                                });
                                apps.push(Some(app));
                            }
                            None => {
                                failed.get_or_insert_with(|| {
                                format!("entry {seq} reached the raft log writer without its payloads")
                            });
                            }
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
                        GroupBody::Raw { bytes, .. } => bytes.len() as u64,
                    })
                    .sum(),
            );
        }
        let result = match failed {
            Some(why) => Err(io::Error::other(why)),
            None => self.q.write_group(&mut items),
        };
        match result {
            Ok(()) => {
                for (it, app) in items.iter().zip(apps) {
                    if let (GroupBody::Entry { entry, .. }, Some(app)) = (&it.body, app) {
                        app.set_payload_free(entry.clone());
                    }
                }
                for cb in callbacks {
                    cb.io_completed(Ok(()));
                }
            }
            Err(e) => {
                let why = format!("raft log write failed: {e}");
                self.set_poison(why.clone());
                for cb in callbacks {
                    cb.io_completed(Err(io::Error::other(why.clone())));
                }
            }
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
                let res = match self.poisoned() {
                    Some(why) => Err(io::Error::other(why)),
                    None => self.q.set.truncate_from(from_seq).map(|dropped| {
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

    /// The commit point, in place and without fsync (see the module header).
    fn write_committed(&mut self, committed: Option<LogId>) {
        if let Ok(mut b) = serde_json::to_vec(&committed) {
            b.push(b'\n');
            let _ = self
                .committed_file
                .write_all_at(&b, 0)
                .and_then(|()| self.committed_file.set_len(b.len() as u64));
        }
    }
}
