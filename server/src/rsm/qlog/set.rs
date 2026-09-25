//! `rsm/qlog/set.rs` — the applier-owned per-queue [`QLog`] registry and the
//! cloneable [`QLogReader`] the facade + planner read through
//! (`ALICE_PGLESS_NEWARCH.md` §1–§5, Phase A1 write / Phase A2 read).
//!
//! # A1 write, A2 read
//!
//! A0 built the per-queue store ([`QLog`]); A1 made this registry SHADOW-write
//! every `Append` a second time (behind `QUEEN_RAFT_QLOG`, default off) into its
//! queue's log, alongside the still-authoritative segment write. A2 switches the
//! READS over: with the knob on, the pop payload read and the
//! `DEDUP_INDEX=segment` dedup authority read from the queue log instead of the
//! `.seg` files and the LMDB keyspaces. The segments and the raft-log blob are
//! still written (removed in A3), so off-vs-on stays behaviourally identical.
//!
//! # The single writer, and the concurrent readers
//!
//! One [`QLog`] per queue, opened under `<data_dir>/qlog/q<queue_id>/` (sibling
//! to `seg/`, `log/`, `store/`). The applier is the SINGLE writer (`buffer` +
//! `flush`, on the apply thread), exactly as it is the single writer of
//! [`crate::rsm::segments::Segments`]. But the READS run on OTHER threads — the
//! pop render on the blocking pool, the dedup probe on the batcher — so each
//! queue's [`QLog`] lives behind an `RwLock`, and the map of them behind another,
//! shared with a cloneable [`QLogReader`] the way `Segments::reader()` shares the
//! segment file set. A write takes the queue's write lock only for its one
//! `append_group` (one `write` + one fsync); a read takes the read lock.
//!
//! # Reopen on restart (A2)
//!
//! [`QLogSet::reopen_all`] discovers every existing `q<id>/` directory at
//! `Applier::open` and reopens it ([`QLog::open`] — torn-tail truncate + `.qidx`
//! rebuild), so a reopened node serves reads from the qlog immediately, before
//! any new append. A genuinely new queue is still opened lazily on its first
//! flush; A0's `QLog::open` reopens an existing directory rather than
//! `create_new`-colliding with it, so the lazy path and the reopen path share one
//! constructor.
//!
//! # An `Append` is buffered, written per entry, fsync'd per durable point
//!
//! An `Append` is [`QLogSet::buffer`]ed — copied into an owned record keyed by
//! its queue — during `apply`, because the effect's `hashes`/`blob` borrow the
//! entry and are freed when `apply` returns. [`QLogSet::flush`] then drains each
//! queue's buffer into ONE [`QLog::write_group`] (a `write`, NO fsync) at the END
//! of that entry's apply — right after `segments.flush_writes`, before the leader
//! answers. That per-entry write is the A2 read invariant: the leader answers a
//! pop right after apply, and a pop can claim (through the overlay) an offset an
//! in-flight push appended in the SAME uncommitted commit window, so the record
//! MUST be readable from the qlog the moment the entry is applied — not one store
//! commit later. The segment path does exactly this (`flush_writes` per entry).
//! [`QLogSet::sync`] fsyncs the queues written since the last durable point, so
//! the barrier stays batched, the qlog twin of the segment durable point.
//!
//! # Determinism (I2)
//!
//! Nothing here reads a clock, the environment or randomness. `seq` and
//! `created_at_us` are the leader's inputs (from the entry/effect); `queue_id`
//! is a stable [`xxhash_rust::xxh3`] of `(tenant, queue)` (see
//! [`QLogSet::queue_id_of`]) — the same shape `planner::bucket_of` uses. The
//! queue id is node-local anyway (it only names a directory, never replicated
//! state), but keeping it a pure function of the two names keeps the shadow
//! path free of any environment the I2 deny-gate forbids.

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::os::unix::fs::FileExt;
use std::sync::{Arc, RwLock};

use crate::rsm::qlog::{
    record, BandFrame, CommittedFrame, EntryPart, EntryRecord, OwnedRecord, QLog, QLogOptions,
    ReclaimProgress, RecordInput, WriteRecord,
};

/// How the log writer lays an entry's payload-free record across the logs its
/// effects touch (`QUEEN_QLOG_ENTRY_LAYOUT`). Node-local and read-compatible
/// both ways: the readers accept either layout, mixed in one directory, so the
/// knob may flip on a live data directory (a binary WITHOUT stub support must
/// not open a directory written with `stub`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EntryLayout {
    /// Every touched log gets the whole record (Phase C as built): an entry
    /// costs `touched logs × its bytes` — bytes grow like queues² per cycle
    /// (PLAN_QLOG_ENTRY_BYTES.md §1).
    Copies,
    /// The lowest touched log id gets the whole record, every other touched
    /// log a 61-byte stub ([`record::REC_ENTRY_STUB`]): an entry costs its bytes
    /// once plus 61 bytes per extra log. Same logs, same fsyncs, same rule.
    #[default]
    Stub,
}

impl EntryLayout {
    /// `QUEEN_QLOG_ENTRY_LAYOUT`: `copies`, anything else (or unset) `stub`.
    pub fn from_env() -> EntryLayout {
        match std::env::var("QUEEN_QLOG_ENTRY_LAYOUT") {
            Ok(v) if matches!(v.trim().to_ascii_lowercase().as_str(), "copies" | "copy") => {
                EntryLayout::Copies
            }
            _ => EntryLayout::Stub,
        }
    }
}

/// One seq's parts across the logs, as [`EntryMerge`] has seen them.
struct Parts {
    /// `(copies, term, now_us)` of the first part seen: every part must agree.
    head: (u32, u64, i64),
    full: Option<EntryRecord>,
    /// `(digest, len)` of every stub seen, checked against the whole record.
    stubs: Vec<(u64, u32)>,
    /// The logs holding a part (whole or stub), in the order seen.
    logs: Vec<u64>,
}

/// What [`EntryMerge`] makes of one seq.
enum Verdict {
    /// Every part is present and they agree: the whole record, and the logs
    /// holding a part (where the entry's payload records are).
    Complete(EntryRecord, Vec<u64>),
    /// Fewer parts than `copies`: the group did not land everywhere (why).
    Incomplete(String),
}

fn disagree(seq: u64, what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("the copies of entry {seq} disagree across the queue logs ({what})"),
    )
}

impl Parts {
    /// Complete, incomplete, or refused. More parts than `copies`, a stub that
    /// does not name the whole record, or every part a stub is refused: never
    /// guessed.
    fn verdict(self, seq: u64) -> io::Result<Verdict> {
        let copies = self.head.0;
        let found = self.logs.len() as u32;
        if found > copies {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("entry {seq} is in {found} queue logs but was written to {copies}"),
            ));
        }
        if let Some(full) = &self.full {
            let named = (record::entry_digest(&full.entry), full.entry.len() as u32);
            if self.stubs.iter().any(|s| *s != named) {
                return Err(disagree(seq, "a stub does not name the whole record"));
            }
        }
        if found < copies {
            return Ok(Verdict::Incomplete(format!(
                "entry {seq} is incomplete: {found} of its {copies} copies survived"
            )));
        }
        match self.full {
            Some(full) => Ok(Verdict::Complete(full, self.logs)),
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("entry {seq}: all {copies} of its parts are stubs; no log holds the whole record"),
            )),
        }
    }
}

/// The merge [`QLogSet::scan_entries`] and [`QLogReader::entry_records_range`]
/// share: every log's entry parts, by seq, checked for agreement as they come.
#[derive(Default)]
struct EntryMerge {
    by_seq: BTreeMap<u64, Parts>,
}

impl EntryMerge {
    fn add(&mut self, log: u64, part: EntryPart) -> io::Result<()> {
        let seq = part.seq();
        let head = part.head();
        let p = self.by_seq.entry(seq).or_insert_with(|| Parts {
            head,
            full: None,
            stubs: Vec::new(),
            logs: Vec::new(),
        });
        if p.head != head {
            return Err(disagree(seq, "copies, term or clock"));
        }
        p.logs.push(log);
        match part {
            EntryPart::Full(r) => match &p.full {
                Some(f) if f.entry != r.entry => return Err(disagree(seq, "two whole records")),
                Some(_) => {}
                None => p.full = Some(r),
            },
            EntryPart::Stub(s) => p.stubs.push((s.digest, s.len)),
        }
        Ok(())
    }
}

/// Phase C: the reserved queue id of the SYSTEM log (`<root>/q0/`). It holds the
/// entry record of every entry whose effects touch no queue — a `Noop`, a
/// `RequestIdsExpire`, trace / flag / quota / KV / timer / streams / ephemeral
/// config writes, garbage bookkeeping — and of an entry one of whose pid-scoped
/// effects names a partition the writer cannot resolve (a deleted one). It is
/// never read by pop or dedup (no queue name maps to it:
/// [`QLogSet::queue_id_of`] never returns 0 for a real queue); recovery merges
/// it with every queue log ([`QLogSet::scan_entries`]).
pub const SYSTEM_QUEUE_ID: u64 = 0;

/// What [`QLogSet::scan_entries`] found and delivered.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EntryScan {
    /// Entries handed to the callback: exactly `from_seq .. next_seq`, gapless,
    /// each one complete (all of its copies found, all identical).
    pub delivered: u64,
    /// The first seq NOT delivered: where the durable, acknowledgeable prefix
    /// ends. Every record at or above it belongs to a group whose fsyncs did not
    /// all land (nothing of which was acknowledged) — [`QLogSet::truncate_from`]
    /// drops it before the writer reuses those seqs.
    pub next_seq: u64,
    /// The highest entry-record seq found in any log (`0` when none).
    pub max_seq_found: u64,
    /// Distinct entries found at or above `next_seq` (the discarded tail).
    pub discarded: u64,
    /// Why the walk stopped before `max_seq_found`, if it did.
    pub stopped: Option<String>,
}

/// The shared map of per-queue logs. The applier owns the [`QLogSet`] that
/// writes them; a [`QLogReader`] clones this `Arc` and reads them. The outer
/// `RwLock` guards the MAP (opens/removes, rare, apply-thread only); each inner
/// `RwLock` guards ONE queue's [`QLog`] (its writes are the applier's, its reads
/// the pool's / the batcher's).
type SharedLogs = Arc<RwLock<BTreeMap<u64, Arc<RwLock<QLog>>>>>;

#[derive(Default)]
struct SharedTotals {
    files: AtomicU64,
    bytes: AtomicU64,
}

fn replace_total(total: &AtomicU64, before: u64, after: u64) {
    if after >= before {
        total.fetch_add(after - before, Ordering::AcqRel);
    } else {
        total.fetch_sub(before - after, Ordering::AcqRel);
    }
}

/// One `Append` buffered until the next flush, OWNING its bytes.
///
/// The effect's `hashes`/`blob` are borrowed from the entry and go away when
/// `apply` returns; the flush that writes them runs later (at a commit or a
/// durable point), so the buffer keeps its own copies. Bounded by one
/// store-commit window's worth of appends (the flush drains it every commit).
struct Buffered {
    seq: u64,
    pid: u64,
    base_offset: u64,
    count: u32,
    created_at_us: i64,
    hashes: Vec<u8>,
    payload: Vec<u8>,
}

/// The per-queue [`QLog`] registry (Phase A1 shadow write / A2 read). One
/// instance per applier; the write methods take `&mut self` (the applier is the
/// single writer). Reads go through a [`QLogReader`] clone, off other threads.
pub struct QLogSet {
    /// `<data_dir>/qlog`. Each queue's files live under `q<queue_id>/` below it.
    root: PathBuf,
    /// Roll size + fsync mode, mirrored from the segment writer's options so the
    /// shadow rolls and fsyncs on the same terms the authoritative store does.
    opts: QLogOptions,
    /// One open log per queue id, shared with every [`QLogReader`].
    logs: SharedLogs,
    /// Records buffered within ONE entry, per queue id. Drained (written to the
    /// file + index) at the end of that entry's apply by [`QLogSet::flush`], so a
    /// record is readable before the leader answers. Apply-thread only.
    pending: BTreeMap<u64, Vec<Buffered>>,
    /// Queue ids written since the last [`QLogSet::sync`]: their active file has
    /// an un-fsync'd tail, fsync'd together at the durable point. Apply-thread
    /// only.
    dirty: std::collections::BTreeSet<u64>,
    /// A3a durable index (`ALICE_PGLESS_NEWARCH.md` §5). The highest record `seq`
    /// (the leader's order stamp = the entry index) that [`QLogSet::flush`] has
    /// written to a file (page cache), across every queue. Apply-thread only.
    written_seq: u64,
    /// The highest `seq` that [`QLogSet::sync`] has made DURABLE (fsync'd). Every
    /// queue [`QLogSet::flush`] wrote is marked `dirty` and [`QLogSet::sync`]
    /// fsyncs all of them, so once a sync returns every written record is on the
    /// platter and this equals `written_seq`. The applier records it into the
    /// store (`meta::QLOG_DURABLE_INDEX`) in the SAME commit that follows the
    /// sync, so recovery can reconcile the qlog's durable tail against it.
    durable_seq: u64,
    /// Phase C recovery floor, shared with every log this set opens and with
    /// every [`QLogReader`]: no file holding a record with `seq >` it is ever
    /// unlinked ([`QLog::unlink_dead_files`]). `0` until someone raises it (so a
    /// fresh set deletes nothing); the `LocalReplicator` seeds it with the
    /// store's durable index at open and raises it at every durable point.
    floor: Arc<AtomicU64>,
    /// Files / valid bytes across every open log (the replicator's `log_files` /
    /// `log_bytes` metrics once the queue logs are the WAL). Shared with the
    /// maintenance reader because retention may compact a log off the writer
    /// thread.
    totals: Arc<SharedTotals>,
    /// Fair starting point for the globally bounded local-GC step.
    reclaim_queue_cursor: Arc<AtomicU64>,
    /// While non-zero, retention unlinks and rewrites nothing
    /// ([`QLogReader::pause_reclaim`]): a snapshot is linking the files.
    reclaim_paused: Arc<AtomicU64>,
    /// How many LANES each queue's records are split into: a partition's
    /// records live in its lane's log ([`QLogSet::log_id_for`]), and the lanes
    /// of one group are written in parallel. Fixed when the directory is
    /// created (`qlog/LANES`); shared with every reader.
    lanes: Arc<AtomicU64>,
    /// How many SHARED logs the queues are spread over (`0` = one log per
    /// queue): a queue's records live in shared log `queue_id % shards`
    /// ([`route_log`]). Fixed when the directory is created (`qlog/SHARDS`);
    /// shared with every reader.
    shards: Arc<AtomicU64>,
    /// When the writer last ran [`QLogSet::idle_pass`].
    idle_at: Option<std::time::Instant>,
    /// Where the next [`QLogSet::idle_pass`] resumes (a log id).
    idle_cursor: u64,
    /// The fsync'd tail of every log a sync covered, for the applier to record
    /// ([`QLogSet::track_tails`]); `None` when nobody records them.
    tails: Option<Arc<QlogTails>>,
}

/// Per queue log, the `seq`s its fsyncs made durable, waiting for their
/// entries to be applied: the applier records each log's tail at its durable
/// points (`meta::qlog_tail_key`), and a reopen refuses a log that comes back
/// shorter ([`QLogSet::check_tails`]) — a log truncated inside acknowledged
/// records, which the set-wide tail check cannot see while another log is
/// complete (Jepsen P6, `repro/qlog-bitflip-hole.sh cut`). Only applied `seq`s
/// are recorded: a follower's unapplied suffix may still be cut by Raft.
#[derive(Debug, Default)]
pub struct QlogTails {
    synced: std::sync::Mutex<BTreeMap<u64, std::collections::VecDeque<u64>>>,
}

impl QlogTails {
    /// A sync made `(log, seq)` durable: the log held records up to `seq`.
    fn note_synced(&self, pairs: &[(u64, u64)]) {
        let mut g = self.synced.lock().expect("qlog tails");
        for (log, seq) in pairs {
            let q = g.entry(*log).or_default();
            if q.back().is_none_or(|b| b < seq) {
                q.push_back(*seq);
            }
        }
    }

    /// The tails the applied index `upto` makes permanent: per log, the
    /// highest synced `seq` at or below it.
    pub fn take_committed(&self, upto: u64) -> Vec<(u64, u64)> {
        let mut g = self.synced.lock().expect("qlog tails");
        let mut out = Vec::new();
        g.retain(|log, q| {
            let mut last = None;
            while q.front().is_some_and(|s| *s <= upto) {
                last = q.pop_front();
            }
            if let Some(s) = last {
                out.push((*log, s));
            }
            !q.is_empty()
        });
        out
    }

    /// Raft cut every log at `cut`: synced `seq`s at or above it are gone.
    fn truncate_from(&self, cut: u64) {
        let mut g = self.synced.lock().expect("qlog tails");
        g.retain(|_, q| {
            while q.back().is_some_and(|s| *s >= cut) {
                q.pop_back();
            }
            !q.is_empty()
        });
    }
}

/// The file under the queue-log root that fixes the directory's lane count.
pub const LANES_FILE: &str = "LANES";

/// The most lanes a directory may have.
pub const MAX_LANES: u64 = 64;

/// `QUEEN_QLOG_LANES`: how many queue-log lanes a NEW data directory gets
/// (default 1). An existing directory keeps the count it was created with.
/// Separate from the planner's lanes (`QUEEN_LANES`): every lane log a group
/// touches is one more fsync per group, which on a single disk costs more than
/// the parallel write gains unless the device has the IOPS to spare.
pub fn lanes_from_env() -> u64 {
    std::env::var("QUEEN_QLOG_LANES")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(1)
        .clamp(1, MAX_LANES)
}

/// The file under the queue-log root that fixes the directory's SHARED-log
/// count ([`QLogSet::shards`]).
pub const SHARDS_FILE: &str = "SHARDS";

/// The most shared logs a directory may have.
pub const MAX_SHARDS: u64 = 4096;

/// `QUEEN_QLOG_SHARDS`: how many SHARED logs a NEW data directory gets
/// (default 0 = one log per queue, the layout before shared logs). With `K > 0`
/// every queue's records go to shared log `queue_id % K`, so a group fsyncs at
/// most `K` logs however many queues it touches — the per-queue fsync fan-out
/// is what collapsed 10,000 single-partition queues on one node while one
/// queue with 10,000 partitions (one log) stayed at ~10 ms. An existing
/// directory keeps the layout it was created with.
pub fn shards_from_env() -> u64 {
    std::env::var("QUEEN_QLOG_SHARDS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0)
        .min(MAX_SHARDS)
}

/// The log id of shared log `shard`. `0` is the system log and `1` only ever a
/// remapped queue id in a per-queue directory, so shared logs start at 2.
pub fn shard_log_id(shard: u64) -> u64 {
    2 + shard
}

/// The log holding partition `pid` of queue `queue_id`: the queue's shared log
/// when the directory has shards, else the partition's lane log of the queue.
pub fn route_log(queue_id: u64, pid: u64, lanes: u64, shards: u64) -> u64 {
    if shards > 0 {
        shard_log_id(queue_id % shards)
    } else {
        lane_log_id(queue_id, pid % lanes.max(1))
    }
}

/// The log holding a queue's partition-less records (catalog, groups, DLQ
/// entry parts): the queue's shared log, or its lane-0 log (the queue id).
pub fn route_queue_log(queue_id: u64, shards: u64) -> u64 {
    if shards > 0 {
        shard_log_id(queue_id % shards)
    } else {
        queue_id
    }
}

/// The rewrite threshold shared logs get unless `QUEEN_QLOG_COMPACT_MIN_DEAD_PCT`
/// says otherwise: a shared file mixes queues whose messages die at different
/// times, so it is copied forward only once half of it is dead.
pub const SHARED_COMPACT_MIN_DEAD_PCT: u8 = 50;

/// `QUEEN_QLOG_SEAL_AGE_S` (default 600, 0 = never): an active file holding
/// data this old is sealed by the writer's idle pass so retention can reclaim
/// it ([`QLogSet::idle_pass`]).
pub fn seal_age_from_env() -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var("QUEEN_QLOG_SEAL_AGE_S")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(600),
    )
}

/// How often the writer runs [`QLogSet::idle_pass`].
const IDLE_PASS_EVERY: std::time::Duration = std::time::Duration::from_secs(5);

/// The most seals + removals one [`QLogSet::idle_pass`] does.
const IDLE_PASS_BUDGET: usize = 64;

/// A fixed pool of fsync threads ([`QLogSyncer::sync`]). A group that touched
/// many logs used to spawn one scoped thread per extra log per group: at ~1,000
/// touched logs that alone cost 22-38 ms per group even with fsync off.
struct FsyncPool {
    tx: std::sync::Mutex<std::sync::mpsc::Sender<FsyncJob>>,
}

struct FsyncJob {
    file: std::fs::File,
    mode: crate::rsm::qlog::Fsync,
    done: std::sync::mpsc::Sender<io::Result<()>>,
}

/// `QUEEN_QLOG_FSYNC_THREADS` (default 32): the pool's size.
fn fsync_pool() -> &'static FsyncPool {
    static POOL: std::sync::OnceLock<FsyncPool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let n = std::env::var("QUEEN_QLOG_FSYNC_THREADS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(32)
            .clamp(1, 1024);
        let (tx, rx) = std::sync::mpsc::channel::<FsyncJob>();
        let rx = Arc::new(std::sync::Mutex::new(rx));
        for i in 0..n {
            let rx = rx.clone();
            std::thread::Builder::new()
                .name(format!("queen-qlog-fsync-{i}"))
                .spawn(move || {
                    // W1: part of the core log sync.
                    crate::obs::panic_policy::mark_current_thread_core();
                    loop {
                        let job = {
                            let guard = rx.lock().unwrap_or_else(|e| e.into_inner());
                            guard.recv()
                        };
                        let Ok(job) = job else { return };
                        let res = crate::rsm::qlog::fsync_file(&job.file, job.mode);
                        let _ = job.done.send(res);
                    }
                })
                .expect("spawn a qlog fsync thread");
        }
        FsyncPool {
            tx: std::sync::Mutex::new(tx),
        }
    })
}

/// What one group's writes left to fsync: every log written, and the highest
/// record `seq` among them. The writer takes it ([`QLogSet::take_ticket`]); a
/// [`QLogSyncer`] fsyncs it, on another thread if the caller wants the next
/// group's writes to overlap this fsync.
#[derive(Clone, Debug, Default)]
pub struct SyncTicket {
    pub qids: Vec<u64>,
    pub seq: u64,
}

/// A cloneable handle that fsyncs the logs a [`SyncTicket`] names.
#[derive(Clone)]
pub struct QLogSyncer {
    logs: SharedLogs,
    mode: crate::rsm::qlog::Fsync,
    tails: Option<Arc<QlogTails>>,
}

impl QLogSyncer {
    /// Fsync every log in `t`, concurrently: the slowest fsync, not the sum.
    /// Each log's active file is cloned under a brief READ lock and fsynced
    /// outside any lock, so pop reads proceed during the fsync. A writer may
    /// roll a log between the clone and the fsync: the roll itself fsyncs the
    /// file it seals before it switches, so whichever file was cloned, every
    /// byte written before this call is durable when it returns.
    pub fn sync(&self, t: &SyncTicket) -> io::Result<()> {
        let mut handles: Vec<std::fs::File> = Vec::with_capacity(t.qids.len());
        let mut tails: Vec<(u64, u64)> = Vec::new();
        for qid in &t.qids {
            let arc = self
                .logs
                .read()
                .expect("qlog set poisoned")
                .get(qid)
                .cloned();
            if let Some(log) = arc {
                let g = log.read().expect("qlog poisoned");
                // Under the same lock as the clone: every record up to this
                // tail is in the cloned file or in a file its roll fsync'd.
                let tail = g.durable_tail();
                if let Some(f) = g.active_clone()? {
                    handles.push(f);
                    tails.push((*qid, tail));
                }
            }
        }
        self.fsync_all(handles)?;
        if let Some(t) = &self.tails {
            t.note_synced(&tails);
        }
        Ok(())
    }

    fn fsync_all(&self, mut handles: Vec<std::fs::File>) -> io::Result<()> {
        let mode = self.mode;
        if handles.len() <= 1 {
            // The one-busy-queue case: inline, no hand-off.
            return match handles.pop() {
                Some(one) => crate::rsm::qlog::fsync_file(&one, mode),
                None => Ok(()),
            };
        }
        // Every log but the first goes to the fixed pool; this thread fsyncs
        // the first meanwhile. Still the slowest fsync, not the sum.
        let first = handles.remove(0);
        let (done_tx, done_rx) = std::sync::mpsc::channel::<io::Result<()>>();
        let mut sent = 0usize;
        let mut res: io::Result<()> = Ok(());
        {
            let pool = fsync_pool();
            let tx = pool.tx.lock().unwrap_or_else(|e| e.into_inner());
            for file in handles {
                let job = FsyncJob {
                    file,
                    mode,
                    done: done_tx.clone(),
                };
                match tx.send(job) {
                    Ok(()) => sent += 1,
                    // A dead pool (every thread gone) fsyncs here instead.
                    Err(e) => {
                        let job = e.0;
                        let r = crate::rsm::qlog::fsync_file(&job.file, job.mode);
                        if res.is_ok() {
                            res = r;
                        }
                    }
                }
            }
        }
        drop(done_tx);
        let r = crate::rsm::qlog::fsync_file(&first, mode);
        if res.is_ok() {
            res = r;
        }
        for _ in 0..sent {
            let r = done_rx
                .recv()
                .unwrap_or_else(|_| Err(io::Error::other("qlog fsync thread gone")));
            if res.is_ok() {
                res = r;
            }
        }
        res
    }
}

/// The log id of `lane` of the queue `queue_id`. Lane 0 IS the queue's id, so a
/// one-lane directory is laid out exactly as before lanes; any other lane gets
/// its own stable id (never the system log's 0).
pub fn lane_log_id(queue_id: u64, lane: u64) -> u64 {
    if lane == 0 {
        return queue_id;
    }
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&queue_id.to_le_bytes());
    b[8..].copy_from_slice(&lane.to_le_bytes());
    match xxhash_rust::xxh3::xxh3_64(&b) {
        SYSTEM_QUEUE_ID => 1,
        h => h,
    }
}

fn read_lanes_file(root: &std::path::Path) -> io::Result<Option<u64>> {
    match std::fs::read_to_string(root.join(LANES_FILE)) {
        Ok(s) => s
            .trim()
            .parse::<u64>()
            .ok()
            .filter(|n| (1..=MAX_LANES).contains(n))
            .map(Some)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: not a lane count: {s:?}", root.join(LANES_FILE).display()),
                )
            }),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

impl QLogSet {
    /// A fresh registry rooted at `root` (`<data_dir>/qlog`). Opens no file yet;
    /// call [`QLogSet::reopen_all`] to pick up an existing directory's queues,
    /// and each genuinely new queue's log is created on the first flush that
    /// carries a record for it.
    pub fn new(root: PathBuf, opts: QLogOptions) -> QLogSet {
        QLogSet {
            root,
            opts,
            logs: Arc::new(RwLock::new(BTreeMap::new())),
            pending: BTreeMap::new(),
            dirty: std::collections::BTreeSet::new(),
            written_seq: 0,
            durable_seq: 0,
            floor: Arc::new(AtomicU64::new(0)),
            totals: Arc::new(SharedTotals::default()),
            reclaim_queue_cursor: Arc::new(AtomicU64::new(0)),
            reclaim_paused: Arc::new(AtomicU64::new(0)),
            lanes: Arc::new(AtomicU64::new(1)),
            shards: Arc::new(AtomicU64::new(0)),
            idle_at: None,
            idle_cursor: 0,
            tails: None,
        }
    }

    /// Record every log's fsync'd tail from now on, for the applier to
    /// persist ([`QlogTails`]). Call before taking any [`QLogSet::syncer`].
    pub fn track_tails(&mut self) -> Arc<QlogTails> {
        self.tails.get_or_insert_with(Default::default).clone()
    }

    /// Every open log against the tail the store recorded for it at a durable
    /// point (`recorded`: log id -> `meta::qlog_tail_key` value). A log that
    /// reopened SHORTER lost records this node had fsync'd and applied: a
    /// truncation inside acknowledged records, which [`QLog::open_guarded`]
    /// cannot see when nothing verifies after the cut.
    pub fn check_tails(&self, recorded: impl Fn(u64) -> io::Result<Option<u64>>) -> io::Result<()> {
        let logs: Vec<(u64, u64)> = self
            .logs
            .read()
            .expect("qlog set poisoned")
            .iter()
            .map(|(id, l)| (*id, l.read().expect("qlog poisoned").durable_tail()))
            .collect();
        for (id, tail) in logs {
            if let Some(want) = recorded(id)? {
                if tail < want {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "rsm qlog corrupt (queue log q{id}): it ends at seq {tail}, below \
                             seq {want}, which this node fsync'd and applied: acknowledged \
                             records are missing (a truncated log); refusing to start (restore \
                             this node: a cluster member rejoins from a peer once its data \
                             directory is wiped)"
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// This directory's lane count (1 until [`QLogSet::reopen_all`] reads it).
    pub fn lanes(&self) -> u64 {
        self.lanes.load(Ordering::Acquire)
    }

    /// Fix the lane count of a set that has not been reopened (tests, and a
    /// caller that creates a fresh directory itself).
    pub fn set_lanes(&self, n: u64) {
        self.lanes.store(n.clamp(1, MAX_LANES), Ordering::Release);
    }

    /// This directory's shared-log count (0 = one log per queue; 0 until
    /// [`QLogSet::reopen_all`] reads it).
    pub fn shards(&self) -> u64 {
        self.shards.load(Ordering::Acquire)
    }

    /// Fix the shared-log count of a set that has not been reopened (tests, and
    /// a caller that creates a fresh directory itself).
    pub fn set_shards(&self, n: u64) {
        self.shards.store(n.min(MAX_SHARDS), Ordering::Release);
    }

    /// The log that holds partition `pid`'s records in queue `queue_id`.
    pub fn log_id_for(&self, queue_id: u64, pid: u64) -> u64 {
        route_log(queue_id, pid, self.lanes(), self.shards())
    }

    /// The log that holds queue `queue_id`'s partition-less records.
    pub fn log_id_for_queue(&self, queue_id: u64) -> u64 {
        route_queue_log(queue_id, self.shards())
    }

    /// The rewrite threshold this set's logs get
    /// ([`QLog::set_compact_min_dead_pct`]): shared logs mix queues, so they
    /// wait for half a file to be dead; per-queue logs keep the old rule.
    fn compact_pct(&self) -> u8 {
        if self.shards() > 0 {
            SHARED_COMPACT_MIN_DEAD_PCT
        } else {
            0
        }
    }

    /// Read `qlog/SHARDS`, or create it: a new directory takes
    /// `QUEEN_QLOG_SHARDS`, an existing one without the file keeps one log per
    /// queue. Call after [`QLogSet::load_lanes`] (which creates the root).
    fn load_shards(&self) -> io::Result<()> {
        let path = self.root.join(SHARDS_FILE);
        let n = match std::fs::read_to_string(&path) {
            Ok(s) => s
                .trim()
                .parse::<u64>()
                .ok()
                .filter(|n| *n <= MAX_SHARDS)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{}: not a shard count: {s:?}", path.display()),
                    )
                })?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let existing = std::fs::read_dir(&self.root)?
                    .filter_map(|e| e.ok())
                    .any(|e| {
                        e.file_name()
                            .to_str()
                            .and_then(|n| n.strip_prefix('q'))
                            .is_some_and(|r| r.parse::<u64>().is_ok())
                    });
                let n = if existing { 0 } else { shards_from_env() };
                // No file means one log per queue, so only a shared layout is
                // written down. Two openers of one new root (boot + snapshot
                // tail) may race here: each writes its own temp file, and the
                // loser of the rename reads what the winner fixed.
                if n > 0 {
                    static TMP: AtomicU64 = AtomicU64::new(0);
                    let tmp = self.root.join(format!(
                        "{SHARDS_FILE}.tmp.{}.{}",
                        std::process::id(),
                        TMP.fetch_add(1, Ordering::Relaxed)
                    ));
                    {
                        use std::io::Write;
                        let mut f = std::fs::File::create(&tmp)?;
                        f.write_all(format!("{n}\n").as_bytes())?;
                        f.sync_all()?;
                    }
                    std::fs::rename(&tmp, &path)?;
                    std::fs::File::open(&self.root)?.sync_all()?;
                    std::fs::read_to_string(&path)?
                        .trim()
                        .parse::<u64>()
                        .ok()
                        .filter(|v| *v <= MAX_SHARDS)
                        .unwrap_or(n)
                } else {
                    0
                }
            }
            Err(e) => return Err(e),
        };
        let env = shards_from_env();
        if std::env::var("QUEEN_QLOG_SHARDS").is_ok() && env != n {
            tracing::warn!(
                target: "rsm",
                dir_shards = n,
                env_shards = env,
                "QUEEN_QLOG_SHARDS differs from the queue-log directory's shard count; the directory's wins",
            );
        }
        if n > 0 && self.lanes() > 1 {
            tracing::warn!(
                target: "rsm",
                shards = n,
                lanes = self.lanes(),
                "queue-log lanes are ignored in a directory with shared logs",
            );
        }
        self.shards.store(n, Ordering::Release);
        Ok(())
    }

    /// The lane of partition `pid` (its log is `lane_log_id(queue, lane)`).
    pub fn lane_of(&self, pid: u64) -> u64 {
        pid % self.lanes()
    }

    /// Read `qlog/LANES`, or create it: a new directory takes `QUEEN_QLOG_LANES`, an
    /// existing one without the file was written with one lane.
    fn load_lanes(&self) -> io::Result<()> {
        let n = match read_lanes_file(&self.root)? {
            Some(n) => n,
            None => {
                let existing = match std::fs::read_dir(&self.root) {
                    Ok(rd) => rd.filter_map(|e| e.ok()).any(|e| {
                        e.file_name()
                            .to_str()
                            .and_then(|n| n.strip_prefix('q'))
                            .is_some_and(|r| r.parse::<u64>().is_ok())
                    }),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => false,
                    Err(e) => return Err(e),
                };
                let n = if existing { 1 } else { lanes_from_env() };
                std::fs::create_dir_all(&self.root)?;
                let tmp = self.root.join(format!("{LANES_FILE}.tmp"));
                {
                    use std::io::Write;
                    let mut f = std::fs::File::create(&tmp)?;
                    f.write_all(format!("{n}\n").as_bytes())?;
                    f.sync_all()?;
                }
                std::fs::rename(&tmp, self.root.join(LANES_FILE))?;
                std::fs::File::open(&self.root)?.sync_all()?;
                n
            }
        };
        let env = lanes_from_env();
        if std::env::var("QUEEN_QLOG_LANES").is_ok() && env != n {
            tracing::warn!(
                target: "rsm",
                dir_lanes = n,
                env_lanes = env,
                "QUEEN_QLOG_LANES differs from the queue-log directory's lane count; the directory's wins",
            );
        }
        self.lanes.store(n, Ordering::Release);
        Ok(())
    }

    /// The shared recovery-floor handle (Phase C). Whoever learns a new durable
    /// index raises it with [`QLogSet::set_recovery_floor`] (or through a
    /// [`QLogReader`], which shares the same handle).
    pub fn recovery_floor_handle(&self) -> Arc<AtomicU64> {
        self.floor.clone()
    }

    /// The current recovery floor.
    pub fn recovery_floor(&self) -> u64 {
        self.floor.load(Ordering::Acquire)
    }

    /// Raise the recovery floor to `durable_index` (monotone: a lower value is
    /// ignored, so two callers reporting the same durable points never fight).
    pub fn set_recovery_floor(&self, durable_index: u64) {
        self.floor.fetch_max(durable_index, Ordering::AcqRel);
    }

    /// `(files, valid bytes)` across every open log.
    pub fn totals(&self) -> (u64, u64) {
        (
            self.totals.files.load(Ordering::Acquire),
            self.totals.bytes.load(Ordering::Acquire),
        )
    }

    /// The A3a durable index: the highest record `seq` fsync'd by
    /// [`QLogSet::sync`] (`ALICE_PGLESS_NEWARCH.md` §5). The applier writes it to
    /// `meta::QLOG_DURABLE_INDEX` in the commit that follows the sync; recovery
    /// reconciles the reopened qlog's durable tail against it.
    pub fn durable_seq(&self) -> u64 {
        self.durable_seq
    }

    /// The stable per-queue id: `xxh3_64(tenant ␟ queue)`, the two-name twin of
    /// `planner::bucket_of`'s `xxh3(tenant ␟ queue ␟ partition)`. Deterministic
    /// and collision-free on the `0x1F` separator (it cannot occur inside a name
    /// segment). A1 has no catalog-assigned queue id — queues are keyed by their
    /// `(tenant, queue)` names throughout — so this hash IS the id; a later
    /// phase may replace it with a catalog id without changing any record's
    /// bytes.
    ///
    /// Phase C: `0` is [`SYSTEM_QUEUE_ID`], reserved for the system log, so a
    /// name whose hash is 0 (probability 2^-64) is remapped to 1.
    pub fn queue_id_of(tenant: &str, queue: &str) -> u64 {
        let mut buf = Vec::with_capacity(tenant.len() + queue.len() + 1);
        buf.extend_from_slice(tenant.as_bytes());
        buf.push(0x1F);
        buf.extend_from_slice(queue.as_bytes());
        match xxhash_rust::xxh3::xxh3_64(&buf) {
            SYSTEM_QUEUE_ID => 1,
            h => h,
        }
    }

    /// A cloneable reader over this set's per-queue logs, for the pop payload
    /// read (facade) and the `DEDUP_INDEX=segment` dedup read (planner). It
    /// shares the applier's live logs, so it sees every append the moment
    /// [`QLogSet::flush`] lands it — the qlog twin of `Segments::reader()`. It
    /// also carries the set's recovery-floor handle (Phase C), so whoever holds a
    /// reader can raise the floor after a durable point.
    pub fn reader(&self) -> QLogReader {
        QLogReader {
            logs: self.logs.clone(),
            floor: self.floor.clone(),
            totals: self.totals.clone(),
            reclaim_queue_cursor: self.reclaim_queue_cursor.clone(),
            reclaim_paused: self.reclaim_paused.clone(),
            root: self.root.clone(),
            lanes: self.lanes.clone(),
            shards: self.shards.clone(),
        }
    }

    /// A handle that fsyncs this set's logs from another thread.
    pub fn syncer(&self) -> QLogSyncer {
        QLogSyncer {
            logs: self.logs.clone(),
            mode: self.opts.fsync,
            tails: self.tails.clone(),
        }
    }

    /// Reopen every existing `q<id>/` directory under the root (A2 recovery), so
    /// a reopened applier serves reads from the qlog before it writes anything.
    /// Each is [`QLog::open`] — torn-tail truncate on the active file, `.qidx`
    /// rebuild on the sealed files. A missing root (a node that never turned the
    /// knob on) is not an error: there is simply nothing to reopen.
    ///
    /// Returns the qlog's DURABLE TAIL (A3a, `ALICE_PGLESS_NEWARCH.md` §5): the
    /// highest record `seq` that survived recovery across every queue. The
    /// applier reconciles it against the store's `meta::QLOG_DURABLE_INDEX`, and
    /// this seeds `written_seq`/`durable_seq` so the next commit's recorded index
    /// never goes backwards over the reopened tail.
    pub fn reopen_all(&mut self) -> io::Result<u64> {
        self.reopen_all_guarded(0)
    }

    /// [`QLogSet::reopen_all`], refusing a log whose damage sits inside what
    /// the store recorded as qlog-durable ([`QLog::open_guarded`]).
    pub fn reopen_all_guarded(&mut self, durable: u64) -> io::Result<u64> {
        self.load_lanes()?;
        self.load_shards()?;
        let rd = match std::fs::read_dir(&self.root) {
            Ok(rd) => rd,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };
        let mut ids: Vec<u64> = Vec::new();
        for entry in rd {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(rest) = name.strip_prefix('q') {
                if let Ok(id) = rest.parse::<u64>() {
                    ids.push(id);
                }
            }
        }
        ids.sort_unstable();
        let mut tail = 0u64;
        for id in ids {
            // Only the apply thread mutates the map (this call is at open, before
            // any reader thread exists), so a check-then-open-then-insert is
            // race-free; open OUTSIDE the map lock so a reader never blocks on
            // the recovery scan of a queue it is not asking about.
            if self
                .logs
                .read()
                .expect("qlog set poisoned")
                .contains_key(&id)
            {
                continue;
            }
            let (mut log, rec) = QLog::open_guarded(&self.root, id, self.opts, durable)?;
            log.set_recovery_floor(self.floor.clone());
            log.set_compact_min_dead_pct(self.compact_pct());
            tail = tail.max(rec.max_seq);
            self.logs
                .write()
                .expect("qlog set poisoned")
                .insert(id, Arc::new(RwLock::new(log)));
        }
        // The reopened tail is on the platter (it was fsync'd at or before the
        // last commit), so the next commit's recorded durable index starts here,
        // never below it.
        self.written_seq = self.written_seq.max(tail);
        self.durable_seq = self.durable_seq.max(tail);
        self.recompute_totals();
        Ok(tail)
    }

    /// Phase C recovery: every [`crate::rsm::qlog::record::REC_ENTRY`] record of
    /// EVERY open log (the system log included) with `seq >= from_seq`, MERGED by
    /// `seq`, DE-DUPLICATED (an entry is written into every queue log it touches),
    /// and handed to `cb` in strictly increasing `seq` order — the replay the
    /// raft log's `scan_from` used to be. Call it after [`QLogSet::reopen_all`],
    /// which already truncated every torn tail (the payload reopen's rule); a
    /// damaged record in a sealed file is surfaced as corruption.
    ///
    /// It delivers only the DURABLE PREFIX: starting at `from_seq`, it stops at
    /// the first seq that is missing (a gap) or incomplete (fewer than `copies`
    /// distinct logs hold a part of it — its whole record or, in the stub layout
    /// ([`EntryLayout::Stub`], `copies` counting both, exactly one whole record
    /// and every stub naming it by digest). An entry is acknowledged only after the fsync of
    /// EVERY log its group touched, and groups are written one after another, so
    /// a missing or incomplete entry means its group's fsyncs did not all land:
    /// nothing of that group was acknowledged and no later group was written.
    /// Stopping there is exactly the raft log's torn-tail truncation; the caller
    /// then drops everything at or above [`EntryScan::next_seq`] with
    /// [`QLogSet::truncate_from`] before any seq is reused. Two copies of one
    /// seq that disagree, a seq appearing twice in one log, or more copies than
    /// the record claims are refused (never guessed).
    ///
    /// Boot-time only: it reads every log's candidate files and holds the
    /// entries since `from_seq` in memory (one durable interval's worth).
    pub fn scan_entries(
        &self,
        from_seq: u64,
        cb: &mut dyn FnMut(EntryRecord) -> io::Result<()>,
    ) -> io::Result<EntryScan> {
        // Entry indexes start at 1.
        let from_seq = from_seq.max(1);
        let logs: Vec<(u64, Arc<RwLock<QLog>>)> = self
            .logs
            .read()
            .expect("qlog set poisoned")
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        let mut merge = EntryMerge::default();
        for (qid, log) in logs {
            let parts = log
                .read()
                .expect("qlog poisoned")
                .entry_parts_between(from_seq, u64::MAX)?;
            let mut prev: Option<u64> = None;
            for part in parts {
                let seq = part.seq();
                if prev.is_some_and(|p| seq <= p) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "qlog q{qid}: entry record seq {seq} follows seq {} (a queue log's \
                             entry seqs must strictly ascend)",
                            prev.unwrap_or(0)
                        ),
                    ));
                }
                prev = Some(seq);
                merge.add(qid, part)?;
            }
        }
        let mut out = EntryScan {
            next_seq: from_seq,
            max_seq_found: merge.by_seq.keys().next_back().copied().unwrap_or(0),
            ..EntryScan::default()
        };
        let total = merge.by_seq.len() as u64;
        for (seq, parts) in merge.by_seq {
            if seq != out.next_seq {
                out.stopped = Some(format!(
                    "entry {} is missing from every queue log (next found: {seq})",
                    out.next_seq
                ));
                break;
            }
            match parts.verdict(seq)? {
                Verdict::Incomplete(why) => {
                    out.stopped = Some(why);
                    break;
                }
                Verdict::Complete(rec, _logs) => {
                    cb(rec)?;
                    out.delivered += 1;
                    out.next_seq += 1;
                }
            }
        }
        out.discarded = total - out.delivered;
        Ok(out)
    }

    /// Phase C recovery: drop every record with `seq >= cut` from every open
    /// log ([`QLog::truncate_seq_from`]) — the unacknowledged tail
    /// [`QLogSet::scan_entries`] stopped before — durably, before the writer
    /// reuses those seqs. Returns the bytes dropped.
    pub fn truncate_from(&mut self, cut: u64) -> io::Result<u64> {
        if let Some(t) = &self.tails {
            t.truncate_from(cut);
        }
        let logs: Vec<Arc<RwLock<QLog>>> = self
            .logs
            .read()
            .expect("qlog set poisoned")
            .values()
            .cloned()
            .collect();
        let mut dropped = 0u64;
        for log in logs {
            dropped += log.write().expect("qlog poisoned").truncate_seq_from(cut)?;
        }
        self.recompute_totals();
        Ok(dropped)
    }

    /// A Raft truncation of every log at `cut`: [`QLog::truncate_seq_from_across`]
    /// (a conflicting suffix or the tail above a snapshot may sit in a sealed
    /// file). Returns the bytes dropped.
    pub fn truncate_from_across(&mut self, cut: u64) -> io::Result<u64> {
        if let Some(t) = &self.tails {
            t.truncate_from(cut);
        }
        let logs: Vec<Arc<RwLock<QLog>>> = self
            .logs
            .read()
            .expect("qlog set poisoned")
            .values()
            .cloned()
            .collect();
        let mut dropped = 0u64;
        for log in logs {
            dropped += log
                .write()
                .expect("qlog poisoned")
                .truncate_seq_from_across(cut)?;
        }
        self.recompute_totals();
        Ok(dropped)
    }

    /// Recount `(files, bytes)` over every open log (boot / truncation only).
    fn recompute_totals(&mut self) {
        let (mut files, mut bytes) = (0u64, 0u64);
        for log in self.logs.read().expect("qlog set poisoned").values() {
            let g = log.read().expect("qlog poisoned");
            files += g.file_count() as u64;
            bytes += g.bytes();
        }
        self.totals.files.store(files, Ordering::Release);
        self.totals.bytes.store(bytes, Ordering::Release);
    }

    /// Run one write against `qid`'s log (opened lazily), keeping the running
    /// `(files, bytes)` totals current and marking the queue dirty for the next
    /// [`QLogSet::sync`].
    fn write_tracked(
        &mut self,
        qid: u64,
        write: impl FnOnce(&mut QLog) -> io::Result<()>,
    ) -> io::Result<()> {
        let log = self.get_or_open(qid)?;
        let mut g = log.write().expect("qlog poisoned");
        let (f0, b0) = (g.file_count() as u64, g.bytes());
        let res = write(&mut g);
        let (f1, b1) = (g.file_count() as u64, g.bytes());
        drop(g);
        replace_total(&self.totals.files, f0, f1);
        replace_total(&self.totals.bytes, b0, b1);
        res?;
        self.dirty.insert(qid);
        Ok(())
    }

    /// Buffer one `Append` for its queue (a shadow of the `segments.append`
    /// that just ran). Copies the bytes so the record can outlive the entry.
    /// `seq` is the entry index (the leader's global order stamp).
    #[allow(clippy::too_many_arguments)]
    pub fn buffer(
        &mut self,
        tenant: &str,
        queue: &str,
        seq: u64,
        pid: u64,
        base_offset: u64,
        count: u32,
        created_at_us: i64,
        hashes: &[u8],
        payload: &[u8],
    ) {
        // The partition's lane log of its queue, as the writer routes it.
        let qid = self.log_id_for(Self::queue_id_of(tenant, queue), pid);
        self.pending.entry(qid).or_default().push(Buffered {
            seq,
            pid,
            base_offset,
            count,
            created_at_us,
            hashes: hashes.to_vec(),
            payload: payload.to_vec(),
        });
    }

    /// Drain every queue's buffer into ONE [`QLog::write_group`] per queue — one
    /// `write` each, NO fsync — opening a genuinely new queue's log lazily. Apply
    /// calls it at the END of every entry (right after `segments.flush_writes`,
    /// before the leader answers), so a record is page-cache readable the moment
    /// the entry is applied — which is what a pop rendered right after apply, and
    /// a planner claim over committed state, both need (the A2 read invariant).
    /// The fsync is deferred to [`QLogSet::sync`] at the durable point, exactly as
    /// the segment path fsyncs per durable point, not per entry.
    ///
    /// A group is taken out of `pending` BEFORE the (fallible) open/write, so a
    /// mid-flush I/O error does not leave a half-written group buffered for a
    /// retry: the applier poisons and is dropped, and the shadow log is reopened
    /// (torn tail truncated) on restart.
    pub fn flush(&mut self) -> io::Result<()> {
        let qids: Vec<u64> = self
            .pending
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .map(|(k, _)| *k)
            .collect();
        for qid in qids {
            let bufs = std::mem::take(self.pending.get_mut(&qid).expect("pending queue present"));
            // The highest `seq` in this group advances the A3a written watermark
            // (the buffer is filled in apply order, so the last is the highest).
            let group_max_seq = bufs.iter().map(|b| b.seq).max().unwrap_or(0);
            let inputs: Vec<RecordInput<'_>> = bufs
                .iter()
                .map(|b| RecordInput {
                    seq: b.seq,
                    pid: b.pid,
                    base_offset: b.base_offset,
                    count: b.count,
                    created_at_us: b.created_at_us,
                    txn: None,
                    hashes: &b.hashes,
                    payload: &b.payload,
                })
                .collect();
            self.write_tracked(qid, |log| log.write_group(&inputs).map(|_| ()))?;
            self.written_seq = self.written_seq.max(group_max_seq);
        }
        Ok(())
    }

    /// Write one queue's group of records directly (one `write`, NO fsync),
    /// opening the queue's log lazily — the log-native WRITER path
    /// (`ALICE_PGLESS_NEWARCH.md` A3b). The writer thread keys by `queue_id` (it
    /// holds a `pid -> queue_id` map, not the queue names the [`QLogSet::buffer`]
    /// applier path carries), groups a written raft-log group's `Append`s by
    /// queue, and calls this once per touched queue BEFORE it fsyncs — then
    /// [`QLogSet::sync`] makes them durable, and only THEN is the referencing
    /// raft-log entry written and fsynced (the payload-before-entry ordering that
    /// keeps a committed entry from ever pointing at a missing payload). `records`
    /// carry `seq` = the entry index (the leader's order stamp), exactly as the
    /// applier path's buffer did. Marks the queue dirty for the next `sync` and
    /// advances the written watermark.
    pub fn write_group_for_qid(&mut self, qid: u64, records: &[RecordInput<'_>]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let group_max_seq = records.iter().map(|r| r.seq).max().unwrap_or(0);
        self.write_tracked(qid, |log| log.write_group(records).map(|_| ()))?;
        self.written_seq = self.written_seq.max(group_max_seq);
        Ok(())
    }

    /// Phase C: [`QLogSet::write_group_for_qid`] for a MIXED group — each
    /// entry's payload records followed by its payload-free entry record
    /// ([`QLog::write_mixed`]), one `write`, NO fsync. `qid` may be
    /// [`SYSTEM_QUEUE_ID`]. Marks the queue dirty, so the next
    /// [`QLogSet::sync`] fsyncs it even when the group carried no message (a
    /// pop / ack / create entry is acknowledged only after that fsync too).
    pub fn write_mixed_for_qid(&mut self, qid: u64, records: &[WriteRecord<'_>]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let group_max_seq = records.iter().map(|r| r.seq()).max().unwrap_or(0);
        self.write_tracked(qid, |log| log.write_mixed(records).map(|_| ()))?;
        self.written_seq = self.written_seq.max(group_max_seq);
        Ok(())
    }

    /// [`QLogSet::write_mixed_for_qid`] through a shared reference, for the
    /// writer's per-lane threads: one log is only ever written by one of them
    /// (its lane's), and its own lock serializes it anyway. The caller records
    /// the written logs afterwards ([`QLogSet::note_written`]).
    pub fn write_mixed_shared(&self, qid: u64, records: &[WriteRecord<'_>]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let log = self.get_or_open(qid)?;
        // Phase one: where the group goes. With room in the active file (the
        // common case) the READ lock is enough — shared with the planner lanes
        // and the pop readers; creating or rolling a file takes the write lock.
        let (f0, b0, file_id, base, fd) = {
            let g = log.read().expect("qlog poisoned");
            let (f0, b0) = (g.file_count() as u64, g.bytes());
            if g.write_ready() {
                let (id, base) = g.write_target();
                (f0, b0, id, base, g.active_handle()?)
            } else {
                drop(g);
                let mut w = log.write().expect("qlog poisoned");
                let (id, base) = w.prepare_write(records[0].seq())?;
                (f0, b0, id, base, w.active_handle()?)
            }
        };
        // Phase two, with NO lock: encode the group and write its bytes at the
        // logical end. This thread is the log's only writer, so nothing moves
        // that end meanwhile, and no reader can reach the bytes before phase
        // three indexes them. It used to run under the write lock — ~1 ms per
        // multi-MB group during which every claim walk, dedup probe and pop
        // render of the queue waited (the lanes' lock waits at 850k msg/s).
        let group = QLog::encode_group(records, file_id, base)?;
        fd.write_all_at(&group.buf, base)?;
        drop(fd);
        // Phase three: publish, under a short write lock.
        let mut g = log.write().expect("qlog poisoned");
        let res = g.publish_write(base, group).map(|_| ());
        let (f1, b1) = (g.file_count() as u64, g.bytes());
        drop(g);
        replace_total(&self.totals.files, f0, f1);
        replace_total(&self.totals.bytes, b0, b1);
        res
    }

    /// Record logs written by [`QLogSet::write_mixed_shared`]: they are dirty
    /// until the next sync, and `max_seq` advances the written watermark.
    pub fn note_written(&mut self, qids: impl IntoIterator<Item = u64>, max_seq: u64) {
        self.dirty.extend(qids);
        self.written_seq = self.written_seq.max(max_seq);
    }

    /// Everything written since the last ticket, for a [`QLogSyncer`]. The logs
    /// stop being dirty here: whoever holds the ticket owns their fsync.
    pub fn take_ticket(&mut self) -> SyncTicket {
        SyncTicket {
            qids: std::mem::take(&mut self.dirty).into_iter().collect(),
            seq: self.written_seq,
        }
    }

    /// A ticket's fsync returned: its records are durable.
    pub fn note_durable(&mut self, seq: u64) {
        self.durable_seq = self.durable_seq.max(seq);
    }

    /// Fsync every queue written since the last sync (§5: a durable point leaves
    /// every buffered record fsync'd). One fsync per touched queue, batched across
    /// the entries since the last durable point — the qlog twin of the segment
    /// durable point, not a per-entry barrier. Apply calls it at the durable point
    /// only.
    pub fn sync(&mut self) -> io::Result<()> {
        let t = self.take_ticket();
        self.syncer().sync(&t)?;
        self.note_durable(t.seq);
        Ok(())
    }

    /// The open log for `qid`, opening it (and inserting it into the shared map)
    /// if it is new. The `QLog::open` runs OUTSIDE the map lock — only the apply
    /// thread writes the map, and a genuinely new queue has no committed offset a
    /// reader could be asking for yet, so no reader misses a claimable record
    /// during the open.
    fn get_or_open(&self, qid: u64) -> io::Result<Arc<RwLock<QLog>>> {
        if let Some(l) = self.logs.read().expect("qlog set poisoned").get(&qid) {
            return Ok(l.clone());
        }
        let (mut log, _rec) = QLog::open(&self.root, qid, self.opts)?;
        log.set_recovery_floor(self.floor.clone());
        log.set_compact_min_dead_pct(self.compact_pct());
        let arc = Arc::new(RwLock::new(log));
        self.logs
            .write()
            .expect("qlog set poisoned")
            .insert(qid, arc.clone());
        Ok(arc)
    }

    /// A queue was deleted: drop any records still buffered for it (the applier
    /// path). Its logs stay OPEN, so retention sees them: its partitions are
    /// gone, so every message record in them is dead and whole files are
    /// unlinked; the writer's [`QLogSet::idle_pass`] seals the last file once
    /// it is old and removes the directory once nothing is left
    /// (§NA-I5: a log for a dropped queue is GC'd). A shared log is never
    /// removed: the queue's records die there the same way.
    pub fn remove(&mut self, tenant: &str, queue: &str) {
        if self.shards() > 0 {
            return;
        }
        let queue_id = Self::queue_id_of(tenant, queue);
        for lane in 0..self.lanes() {
            self.pending.remove(&lane_log_id(queue_id, lane));
        }
    }

    /// [`QLogSet::idle_pass`] at most every few seconds (writer thread).
    pub fn maybe_idle_pass(&mut self) -> io::Result<()> {
        let now = std::time::Instant::now();
        if self
            .idle_at
            .is_some_and(|at| now.duration_since(at) < IDLE_PASS_EVERY)
        {
            return Ok(());
        }
        self.idle_at = Some(now);
        let seal_age = seal_age_from_env();
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        self.idle_pass(now_us, seal_age)
    }

    /// The writer's housekeeping over every open log — the writer is the single
    /// writer of every log, so rolling one here races no append:
    ///
    /// - SEAL an active file whose data is older than `seal_age` (0 = never).
    ///   A log rolls only on size, and retention never touches the active
    ///   file, so a queue that went quiet kept its last file (up to the roll
    ///   size) forever, however long ago its messages expired.
    /// - REMOVE a per-queue log with nothing left (no sealed file, an empty
    ///   active file): close it and delete its directory. A deleted queue ends
    ///   here once retention has unlinked its files; a later write re-creates
    ///   the log. The system log and shared logs are never removed.
    ///
    /// Logs written since the last sync are skipped (they are busy anyway).
    pub fn idle_pass(&mut self, now_us: i64, seal_age: std::time::Duration) -> io::Result<()> {
        let seal_age_us = i64::try_from(seal_age.as_micros()).unwrap_or(i64::MAX);
        let next_seq = self.written_seq.saturating_add(1);
        let per_queue = self.shards() == 0;
        // Resume after the last log this pass acted on: a roll costs a few
        // fsyncs on the writer thread, so a pass does at most
        // `IDLE_PASS_BUDGET` of them and the next one continues from here.
        let mut logs: Vec<(u64, Arc<RwLock<QLog>>)> = self
            .logs
            .read()
            .expect("qlog set poisoned")
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        let split = logs.partition_point(|(id, _)| *id < self.idle_cursor);
        logs.rotate_left(split);
        let mut sealed = 0usize;
        let mut removed = 0usize;
        // Wrap to the start unless the budget stops this pass part-way.
        self.idle_cursor = 0;
        for (id, log) in logs {
            if sealed + removed >= IDLE_PASS_BUDGET {
                self.idle_cursor = id;
                break;
            }
            if self.dirty.contains(&id) || self.pending.contains_key(&id) {
                continue;
            }
            let (seal, empty) = {
                let g = log.read().expect("qlog poisoned");
                let seal =
                    seal_age_us > 0 && g.active_age_us(now_us).is_some_and(|a| a >= seal_age_us);
                (seal, g.is_removable())
            };
            if seal {
                let mut g = log.write().expect("qlog poisoned");
                let (f0, b0) = (g.file_count() as u64, g.bytes());
                if g.seal_active(next_seq)? {
                    sealed += 1;
                }
                replace_total(&self.totals.files, f0, g.file_count() as u64);
                replace_total(&self.totals.bytes, b0, g.bytes());
                continue;
            }
            if empty && per_queue && id != SYSTEM_QUEUE_ID {
                let dir = log.read().expect("qlog poisoned").dir().to_path_buf();
                let removed_log = self.logs.write().expect("qlog set poisoned").remove(&id);
                if let Some(l) = removed_log {
                    let g = l.read().expect("qlog poisoned");
                    replace_total(&self.totals.files, g.file_count() as u64, 0);
                    replace_total(&self.totals.bytes, g.bytes(), 0);
                }
                match std::fs::remove_dir_all(&dir) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
                removed += 1;
            }
        }
        if removed > 0 {
            std::fs::File::open(&self.root)?.sync_all()?;
        }
        if sealed + removed > 0 {
            tracing::debug!(target: "rsm", sealed, removed, "qlog idle pass");
        }
        Ok(())
    }

    /// The open log for a queue id, for the read-match / reopen tests. Test-only:
    /// the product reads the shadow set through a [`QLogReader`].
    #[cfg(test)]
    pub fn log(&self, queue_id: u64) -> Option<Arc<RwLock<QLog>>> {
        self.logs
            .read()
            .expect("qlog set poisoned")
            .get(&queue_id)
            .cloned()
    }
}

/// A cloneable, `Send + Sync` reader over an applier's per-queue logs (Phase
/// A2). The facade clones one for each blocking pop render; the batcher clones
/// one for the planner's `DEDUP_INDEX=segment` dedup read. Every method keys by
/// `queue_id` ([`QLogSet::queue_id_of`]) and reads under the queue's read lock,
/// off the SAME live logs the applier appends to.
#[derive(Clone)]
pub struct QLogReader {
    logs: SharedLogs,
    /// The set's recovery floor (Phase C), shared: see
    /// [`QLogReader::set_recovery_floor`].
    floor: Arc<AtomicU64>,
    /// Running qlog sizes, shared with the writer so copy-forward retention is
    /// visible to the replicator metrics without waiting for another append.
    totals: Arc<SharedTotals>,
    reclaim_queue_cursor: Arc<AtomicU64>,
    /// Shared with the set: see [`QLogReader::pause_reclaim`].
    reclaim_paused: Arc<AtomicU64>,
    /// `<data_dir>/qlog`, for a snapshot that links the files.
    root: PathBuf,
    /// Shared with the set: see [`QLogSet::lanes`].
    lanes: Arc<AtomicU64>,
    /// Shared with the set: see [`QLogSet::shards`].
    shards: Arc<AtomicU64>,
}

/// Retention stays paused while one of these is alive.
pub struct ReclaimPause(Arc<AtomicU64>);

impl Drop for ReclaimPause {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl QLogReader {
    /// Pause retention (no file is unlinked or rewritten) until the guard is
    /// dropped: a snapshot links every file and needs them to stay put. A
    /// retention step already running finishes its one file first.
    pub fn pause_reclaim(&self) -> ReclaimPause {
        self.reclaim_paused.fetch_add(1, Ordering::AcqRel);
        ReclaimPause(self.reclaim_paused.clone())
    }

    /// `<data_dir>/qlog`.
    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    /// Phase C: raise the queue logs' RECOVERY FLOOR to `durable_index` — the
    /// index the store's last durable point covers. The queue logs are the only
    /// write-ahead log, so recovery replays every entry above the store's
    /// durable index FROM them; retention ([`QLog::unlink_dead_files`]) must never
    /// delete a file that holds any record above it. Call it (on the apply
    /// thread) after each durable point has landed; monotone (`fetch_max`), so a
    /// stale or repeated value is harmless. The `LocalReplicator` already does
    /// this from `Notify::durable` and seeds it at open, so an apply-side call
    /// is belt-and-braces, never a conflict.
    pub fn set_recovery_floor(&self, durable_index: u64) {
        self.floor.fetch_max(durable_index, Ordering::AcqRel);
    }

    /// The current recovery floor (`0` until the first durable point / open).
    pub fn recovery_floor(&self) -> u64 {
        self.floor.load(Ordering::Acquire)
    }

    /// `(files, valid bytes)` across every open log.
    pub fn totals(&self) -> (u64, u64) {
        (
            self.totals.files.load(Ordering::Acquire),
            self.totals.bytes.load(Ordering::Acquire),
        )
    }

    /// The shared floor handle itself, for a caller that wants to hold it.
    pub fn recovery_floor_handle(&self) -> Arc<AtomicU64> {
        self.floor.clone()
    }

    /// Node-local retention for one queue. The committed RSM watermarks are
    /// supplied by the leader maintenance read. Work is bounded by file count;
    /// `try_write` ensures GC never queues in front of an append already using
    /// this queue.
    pub(crate) fn reclaim_below_txns(
        &self,
        queue_id: u64,
        txns_starts: &std::collections::HashMap<u64, u64>,
        max_files: usize,
    ) -> io::Result<ReclaimProgress> {
        self.reclaim_step(queue_id, txns_starts, max_files, true)
    }

    /// [`QLogReader::reclaim_below_txns`], with copy-forward rewrites allowed
    /// or not (`allow_compact`): a pass lets one rewrite through and unlinks
    /// every dead file it looks at.
    pub(crate) fn reclaim_step(
        &self,
        queue_id: u64,
        txns_starts: &std::collections::HashMap<u64, u64>,
        max_files: usize,
        allow_compact: bool,
    ) -> io::Result<ReclaimProgress> {
        if self.reclaim_paused.load(Ordering::Acquire) > 0 {
            return Ok(ReclaimProgress {
                more: true,
                ..ReclaimProgress::default()
            });
        }
        match self.log(queue_id) {
            Some(log) => {
                let Ok(mut log) = log.try_write() else {
                    return Ok(ReclaimProgress {
                        more: true,
                        ..ReclaimProgress::default()
                    });
                };
                let (files_before, bytes_before) = (log.file_count() as u64, log.bytes());
                let result = log.reclaim_step(txns_starts, max_files, allow_compact);
                let (files_after, bytes_after) = (log.file_count() as u64, log.bytes());
                replace_total(&self.totals.files, files_before, files_after);
                replace_total(&self.totals.bytes, bytes_before, bytes_after);
                result
            }
            None => Ok(ReclaimProgress::default()),
        }
    }

    pub(crate) fn log_ids(&self) -> Vec<u64> {
        self.logs
            .read()
            .expect("qlog set poisoned")
            .keys()
            .copied()
            .collect()
    }

    pub(crate) fn reclaim_queue_cursor(&self) -> u64 {
        self.reclaim_queue_cursor.load(Ordering::Acquire)
    }

    pub(crate) fn advance_reclaim_queue_cursor(&self, queue_id: u64) {
        self.reclaim_queue_cursor
            .store(queue_id.wrapping_add(1), Ordering::Release);
    }

    /// The consensus-log read below the openraft storage's in-memory window:
    /// every COMPLETE entry record with `from_seq <= seq < end_seq`, merged
    /// across every queue log (the system log included) in `seq` order — the
    /// live-read twin of [`QLogSet::scan_entries`]. It stops at the first seq
    /// that is missing or incomplete; the caller decides whether a short answer
    /// is an error. Reads every candidate file of every log: a rare path (a
    /// follower catching up from before this node's window), never the hot one.
    pub fn entry_records_range(
        &self,
        from_seq: u64,
        end_seq: u64,
    ) -> io::Result<Vec<(EntryRecord, Vec<u64>)>> {
        let from_seq = from_seq.max(1);
        if from_seq >= end_seq {
            return Ok(Vec::new());
        }
        let logs: Vec<(u64, Arc<RwLock<QLog>>)> = self
            .logs
            .read()
            .expect("qlog set poisoned")
            .iter()
            .map(|(qid, l)| (*qid, l.clone()))
            .collect();
        // Every part (whole record or stub) of every seq in the window; the
        // logs holding a part are where the entry's payload records are.
        let mut merge = EntryMerge::default();
        for (qid, log) in logs {
            let parts = log
                .read()
                .expect("qlog poisoned")
                .entry_parts_between(from_seq, end_seq)?;
            for part in parts {
                merge.add(qid, part)?;
            }
        }
        let mut out = Vec::with_capacity(merge.by_seq.len());
        let mut next = from_seq;
        for (seq, parts) in merge.by_seq {
            if seq != next {
                break;
            }
            match parts.verdict(seq)? {
                Verdict::Incomplete(_) => break,
                Verdict::Complete(rec, qids) => out.push((rec, qids)),
            }
            next += 1;
        }
        Ok(out)
    }

    /// This directory's lane count.
    pub fn lanes(&self) -> u64 {
        self.lanes.load(Ordering::Acquire)
    }

    /// This directory's shared-log count (0 = one log per queue).
    pub fn shards(&self) -> u64 {
        self.shards.load(Ordering::Acquire)
    }

    /// The log that holds partition `pid`'s records in queue `queue_id`.
    pub fn log_id_for(&self, queue_id: u64, pid: u64) -> u64 {
        route_log(queue_id, pid, self.lanes(), self.shards())
    }

    /// The open log holding `pid`'s records of queue `queue_id`.
    fn log_for(&self, queue_id: u64, pid: u64) -> Option<Arc<RwLock<QLog>>> {
        self.log(self.log_id_for(queue_id, pid))
    }

    /// The record holding `(pid, offset)` in the log `log_id` exactly as it is
    /// stored (a zstd payload stays compressed): what a leader re-sends to a
    /// follower that is behind.
    pub fn read_stored_in(
        &self,
        log_id: u64,
        pid: u64,
        offset: u64,
    ) -> io::Result<Option<crate::rsm::qlog::StoredRecord>> {
        match self.log(log_id) {
            Some(l) => l.read().expect("qlog poisoned").read_stored(pid, offset),
            None => Ok(None),
        }
    }

    /// The open log for `queue_id`, or `None` when the applier has never written
    /// (or has removed) that queue. Clones the inner `Arc` so the caller reads
    /// without holding the map lock across the read.
    fn log(&self, queue_id: u64) -> Option<Arc<RwLock<QLog>>> {
        self.logs
            .read()
            .expect("qlog set poisoned")
            .get(&queue_id)
            .cloned()
    }

    /// TEMPORARY diagnostic: [`QLog::describe`] of the log holding `pid`.
    pub fn describe(&self, queue_id: u64, pid: u64, offset: u64) -> String {
        let id = self.log_id_for(queue_id, pid);
        match self.log(id) {
            Some(l) => format!("log {id}: {}", l.read().expect("qlog poisoned").describe(pid, offset)),
            None => format!("log {id}: not open"),
        }
    }

    /// The whole record holding `(pid, offset)` — the pop payload read. `None`
    /// when the queue is unknown or no live file holds the offset (a gap the pop
    /// render skips, exactly as a segment `read_at_within` miss).
    pub fn read_owned(
        &self,
        queue_id: u64,
        pid: u64,
        offset: u64,
    ) -> io::Result<Option<OwnedRecord>> {
        match self.log_for(queue_id, pid) {
            Some(l) => l.read().expect("qlog poisoned").read_owned(pid, offset),
            None => Ok(None),
        }
    }

    /// The O(claimed) forward claim walk of `pid` from `from_offset`, bounded to
    /// `committed_end` (the exactly-once invariant), for the dedup delivered set
    /// / resolve. The qlog twin of `segments::Reader::claim_frames`. A no-op when
    /// the queue is unknown.
    #[allow(clippy::too_many_arguments)]
    pub fn claim_frames(
        &self,
        queue_id: u64,
        pid: u64,
        from_offset: u64,
        committed_end: u64,
        want_hashes: bool,
        cb: &mut dyn FnMut(u64, u64, i64, Option<Vec<u8>>) -> bool,
    ) -> io::Result<()> {
        match self.log_for(queue_id, pid) {
            Some(l) => l.read().expect("qlog poisoned").claim_walk(
                pid,
                from_offset,
                committed_end,
                want_hashes,
                cb,
            ),
            None => Ok(()),
        }
    }

    /// The committed dedup rows (or shape, when `!with_hashes`) of `pid` from
    /// `from_base`, bounded to `committed_end` — the dedup probe / resolve / seed
    /// authority. The qlog twin of `segments::Reader::committed_dedup_rows` /
    /// `committed_dedup_shape`. Empty when the queue is unknown.
    ///
    /// PLAN_RAFT_DRAIN_FIX P3: the index walk + cache probes run under the
    /// queue's read lock; the misses' hashes-only `pread`s run AFTER it is
    /// released (committed records are immutable, each miss holds its fd), so a
    /// whole-window read never holds the applier's append behind its I/O.
    pub fn committed_frames(
        &self,
        queue_id: u64,
        pid: u64,
        from_base: u64,
        committed_end: u64,
        with_hashes: bool,
    ) -> io::Result<Vec<CommittedFrame>> {
        match self.log_for(queue_id, pid) {
            Some(l) => {
                let plan = l.read().expect("qlog poisoned").committed_frames_plan(
                    pid,
                    from_base,
                    committed_end,
                    with_hashes,
                )?;
                Ok(super::committed_of(plan.finish()?))
            }
            None => Ok(Vec::new()),
        }
    }

    /// PLAN_RAFT_DRAIN_FIX P3.1: the committed records of `pid` overlapping any
    /// of the dedup front's `(min_base, max_off)` bands, bounded to
    /// `committed_end`, with their hash blocks — see
    /// [`QLog::committed_hashes_in_bands`]. Same lock split as
    /// [`QLogReader::committed_frames`]. Empty when the queue is unknown.
    pub fn committed_hashes_in_bands(
        &self,
        queue_id: u64,
        pid: u64,
        bands: &[(u64, u64)],
        committed_end: u64,
    ) -> io::Result<Vec<BandFrame>> {
        match self.log_for(queue_id, pid) {
            Some(l) => {
                let plan = l
                    .read()
                    .expect("qlog poisoned")
                    .band_plan(pid, bands, committed_end)?;
                plan.finish()
            }
            None => Ok(Vec::new()),
        }
    }

    /// `queue_id` for a `(tenant, queue)`, so a caller that holds the names can
    /// key the reads without reaching for [`QLogSet`].
    pub fn queue_id_of(tenant: &str, queue: &str) -> u64 {
        QLogSet::queue_id_of(tenant, queue)
    }

    /// Whether the reader has an open log for `queue_id` — for the reopen test,
    /// which asserts a restart rebuilt the map.
    #[cfg(test)]
    pub fn has_queue(&self, queue_id: u64) -> bool {
        self.logs
            .read()
            .expect("qlog set poisoned")
            .contains_key(&queue_id)
    }
}
