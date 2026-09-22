//! `rsm/qlog/` — the per-queue append-only log store of
//! `ALICE_PGLESS_NEWARCH.md` §3 (Phase A).
//!
//! A queue's messages live ONCE, appended in the leader's order, in that
//! queue's own rolling log files (`<root>/q<queue_id>/rNNNNNNNN.qlog`). The log
//! IS the write-ahead log and the byte store: there is no separate consensus
//! log holding payloads and no `.seg` re-materialization, so the payload
//! double-write of the raft class is gone (§0). This module is that store,
//! built from the three pieces the raft class already got right:
//!
//! - the record ([`record`]) extends `segments/frame.rs` — `len` and the
//!   checksum each cover every byte after themselves, so the file is
//!   self-describing and a torn tail is caught by the checksum;
//! - the sparse index ([`index`]) is the `.qidx` of `segments/index.rs` —
//!   active file in RAM, sealed files' index immutable beside them and
//!   rebuildable by scanning;
//! - the writer ([`QLog`]) is the rolling-file, group-commit, one-fsync-per-
//!   group, torn-tail-truncating writer of `replicator/log.rs`, MINUS the
//!   truncate-below-the-durable-point (§5: the log is the store; it is
//!   reclaimed only by retention).
//!
//! # Read-authoritative when `QUEEN_RAFT_QLOG` is on (Phase A2)
//!
//! A0 built this store; A1 wired the SHADOW write ([`set`]); A2 switches the
//! READS over: with the knob on, pop reads the payload from the queue log
//! ([`QLog::read_owned`]) and the `DEDUP_INDEX=segment` dedup authority reads the
//! per-append hashes from it ([`QLog::committed_frames`] / [`QLog::claim_walk`]),
//! instead of the `.seg` files and the `txns`/`dedup` keyspaces. The knob is
//! still OFF by default (segments/LMDB authoritative), and the segments and the
//! raft-log blob are still written (removed in A3), so off-vs-on is behaviourally
//! identical. The transaction fields are in the record format ([`record`]) but
//! transactions are NOT implemented (Phase B): a `txn_kind == 1` record
//! round-trips, and nothing acts on it.
//!
//! The reads run on OTHER threads than the applier's single writer (the pop
//! render on the blocking pool, the dedup probe on the batcher), so the applier
//! owns each queue's [`QLog`] behind an `RwLock` and hands the facade + planner a
//! cloneable reader ([`set::QLogReader`]); see [`set`].
//!
//! ## Recovery note — the A2 shadow's replay duplicates are benign for reads
//!
//! The qlog is fsynced at the store-commit cadence but is NOT truncated below the
//! store's durable point on reopen (it is a shadow; the segments/store are still
//! authoritative in A2). So after a crash the qlog can hold records for entries
//! the store rolled back and the replayed log re-appends, leaving a SECOND record
//! for a `(pid, base_offset)` already present. This does not corrupt a read: the
//! index is keyed by `(pid, base_offset)` and the newer append overwrites the
//! active-index entry (a sealed twin is shadowed because [`QLog::locate_record`]
//! checks the active file first), and the replayed bytes are byte-identical to
//! the originals, so `locate`/`read` resolve to a correct record and the dead
//! copy is only wasted space (reclaimed by A3's compaction). The residue is
//! flagged for the coordinator's crash matrix.
//!
//! # Determinism (I2)
//!
//! The writer reads NO clock and NO randomness. `seq` and `created_at_us` are
//! INPUTS the caller (the leader) supplies per record; recovery and the index
//! derive everything else from the bytes on disk. The only environment this
//! module touches is the file system.
//!
//! # Durability contract (§5)
//!
//! A single-queue message is durable ⇔ its record is fsync'd, untorn, in its
//! queue log. [`QLog::append_group`] writes a whole group with ONE fsync and
//! returns each record's `(file_id, byte_offset)`; a crash mid-group leaves an
//! un-fsync'd tail that [`QLog::open`] truncates. Sealed files are fully
//! durable (each group was fsync'd before the roll), so a torn or damaged
//! record in a SEALED file is corruption and is surfaced, never truncated —
//! only the ACTIVE (last) file has a torn tail.
//!
//! # Phase C: the queue logs are the ONLY write-ahead log
//!
//! With `QUEEN_RAFT_QLOG` on, the `LocalReplicator`'s writer no longer writes
//! the raft log. For every entry of a group it writes, into EVERY queue log the
//! entry's effects touch, the entry's `Append` payload records (indexed, as
//! before) followed by ONE payload-free ENTRY record ([`record::REC_ENTRY`]:
//! `seq` = the entry index, `created_at` = the entry's `now_us`, the `pid` slot
//! = `copies`, the number of logs that got the same record, and the exact
//! `encode_entry_payload_free` bytes as its payload). An entry that touches no
//! queue goes to the SYSTEM log ([`set::SYSTEM_QUEUE_ID`], `q0/`). Then ONE
//! fsync of every touched log, and only then apply and the answer. Entry
//! records are never indexed ([`index`] holds messages only), so no pop, dedup
//! or claim read can see them.
//!
//! Recovery ([`set::QLogSet::scan_entries`]) merges every log's entry records by
//! `seq`, dedups the copies, and delivers the gapless prefix of COMPLETE entries
//! (all `copies` found) from the store's durable index + 1; everything at or
//! above the first missing/incomplete seq is the unacknowledged tail of a group
//! whose fsyncs did not all land, and [`set::QLogSet::truncate_from`] drops it
//! ([`QLog::truncate_seq_from`]) before the writer reuses those seqs. Retention
//! ([`QLog::unlink_dead_files`]) never drops a file holding a record above the
//! RECOVERY FLOOR (the store's durable index, raised at each durable point).
//!
//! # Not this phase
//!
//! - transactions / present-in-all (Phase B) — the fields exist, the machinery
//!   does not;
//! - removing the segments / the raft-log blob (A3) — both still written and the
//!   fallback when the knob is off;
//! - parallel per-queue writers (Phase E) — one writer per queue is fine;
//! - read deadlines — I15 deadlines are a later concern. (Read fds ARE cached
//!   per log, and committed hash blocks too — PLAN_RAFT_DRAIN_FIX P3, see
//!   [`ReadCache`].)

pub mod codec;
pub mod index;
pub mod record;
pub mod set;

#[cfg(test)]
mod band_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod wal_tests;

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// The `.qlog` file header magic (§3): `QNQLOG1\0`. Distinct from the raft
/// log's `QNRLOG1\0` and the segment index's `QQIDX1`.
const FILE_MAGIC: [u8; 8] = *b"QNQLOG1\0";

/// The fixed file header: `magic(8) | file_id:u64 | first_seq:u64 |
/// reserved:u64`. `first_seq` is the `seq` of the file's first record (the
/// leader's order stamp), written when the file is created; it is advisory in
/// Phase A (every record carries its own `seq`) and reserved for the
/// offsets-log interleaving of §3.1. An empty file that never took a record
/// still carries the `first_seq` it was created for.
const FILE_HEADER_LEN: u64 = 32;

/// The first file id a fresh queue creates.
const FIRST_FILE_ID: u64 = 1;

/// The default roll size for a `.qlog` file. The log is the store, so a file is
/// reclaimed only by retention (§5); this bounds how much one file mixes, which
/// is the granularity whole-file unlink can drop (§3.3).
pub const DEFAULT_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Below this a "segment" is not a segment; a typo must not turn every record
/// into its own file.
const MIN_SEGMENT_BYTES: u64 = FILE_HEADER_LEN + 1;

/// Zero-filled preallocation ahead of the active file's logical end. On
/// ext4/xfs an `fdatasync` that grows the file (new size, newly allocated
/// blocks) commits the filesystem journal: ~24 KB of device writes and a second
/// cache flush per call, measured at 62% of all device bytes on a 1000-partition
/// queue. An `fdatasync` that overwrites already-written blocks inside the
/// file's size commits nothing. `fallocate` does not help: the first write into
/// an unwritten extent converts it, which is itself a journaled change, so the
/// run is written with real zeros. Zeros cost one extra write of every log byte,
/// so preallocation pays only while the log's bytes-per-sync is under about one
/// journal commit — it is adaptive, per log, and off under [`Fsync::Off`].
const PREALLOC_CHUNK: u64 = 1024 * 1024;

/// The zero run's size: [`PREALLOC_CHUNK`], or `QUEEN_RAFT_QLOG_PREALLOC_KB`
/// (0 = never preallocate). The run is refilled when less than a quarter of it
/// is left ahead of the logical end.
fn prealloc_chunk() -> u64 {
    static CHUNK: OnceLock<u64> = OnceLock::new();
    *CHUNK.get_or_init(|| {
        std::env::var("QUEEN_RAFT_QLOG_PREALLOC_KB")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map_or(PREALLOC_CHUNK, |kb| kb * 1024)
    })
}
/// Preallocate only while the log's bytes-per-sync estimate is below this.
const PREALLOC_MAX_BYTES_PER_SYNC: u64 = 32 * 1024;

/// How the group fsync reaches the platter. Mirrors
/// [`crate::rsm::replicator::log::Fsync`] and
/// [`crate::rsm::segments::FsyncMode`], with a test-only `Off`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fsync {
    /// `F_FULLFSYNC` on macOS, `fdatasync` elsewhere: the barrier a real
    /// deployment uses.
    Full,
    /// `fdatasync` on Linux, a plain `fsync` on macOS (does NOT guarantee the
    /// platter on macOS): for tests and the laptop smoke.
    Data,
    /// No fsync at all. Tests only: a `kill -9` keeps the page cache, so the
    /// bookkeeping is falsified without paying for a barrier.
    Off,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QLogOptions {
    /// Roll to a new file once the active one is at or over this size. Checked
    /// before a group is written, so a file never overshoots by more than one
    /// group; a group larger than the limit still goes to a file of its own.
    pub segment_bytes: u64,
    pub fsync: Fsync,
}

impl Default for QLogOptions {
    fn default() -> QLogOptions {
        QLogOptions {
            segment_bytes: DEFAULT_SEGMENT_BYTES,
            fsync: Fsync::Full,
        }
    }
}

impl QLogOptions {
    /// Small files and a cheap barrier: a handful of appends rolls a file and
    /// exercises the seal, the drop and the recovery scan. No env, no clock.
    pub fn testing(segment_bytes: u64) -> QLogOptions {
        QLogOptions {
            segment_bytes: segment_bytes.max(MIN_SEGMENT_BYTES),
            fsync: Fsync::Off,
        }
    }

    /// The same, but with a real barrier for a durability run.
    pub fn testing_durable(segment_bytes: u64, fsync: Fsync) -> QLogOptions {
        QLogOptions {
            segment_bytes: segment_bytes.max(MIN_SEGMENT_BYTES),
            fsync,
        }
    }
}

// ---------------------------------------------------------------------------
// The record a caller appends, and the record it reads back
// ---------------------------------------------------------------------------

/// The transaction envelope of an appended record (Phase B). Phase A stores it
/// verbatim and never acts on it.
#[derive(Clone, Copy, Debug)]
pub struct TxnInput<'a> {
    pub gtid: u128,
    pub participants: &'a [u64],
}

/// One record to append: what a caller hands [`QLog::append_group`]. `seq` and
/// `created_at_us` are the leader's inputs (I2 — never read from a clock here).
/// `hashes` is `16 * count` bytes (the per-message txn-id hashes); `payload` is
/// the packed message bytes.
#[derive(Clone, Copy, Debug)]
pub struct RecordInput<'a> {
    pub seq: u64,
    pub pid: u64,
    pub base_offset: u64,
    pub count: u32,
    pub created_at_us: i64,
    pub txn: Option<TxnInput<'a>>,
    pub hashes: &'a [u8],
    pub payload: &'a [u8],
}

/// One payload-free ENTRY record to append (Phase C, [`record::REC_ENTRY`]): the
/// log writer's copy of an entry, written into EVERY queue log the entry's
/// effects touch (or into the system log, [`set::SYSTEM_QUEUE_ID`]). `seq` is the
/// entry index, `now_us` the entry's clock, `copies` how many logs receive this
/// same record, and `entry` the exact `encode_entry_payload_free` bytes the raft
/// log used to hold. It is NOT a message: it is never indexed, so no pop, dedup
/// or claim read can ever see it.
#[derive(Clone, Copy, Debug)]
pub struct EntryInput<'a> {
    pub seq: u64,
    pub now_us: i64,
    pub copies: u32,
    pub entry: &'a [u8],
}

/// One record of a mixed group ([`QLog::write_mixed`]): a message or an entry.
/// Within one group, records must come in non-decreasing `seq` order (the writer
/// emits each entry's payload records, then its entry record, entry by entry).
#[derive(Clone, Copy, Debug)]
pub enum WriteRecord<'a> {
    Msg(RecordInput<'a>),
    /// A message record whose `payload` is ALREADY zstd-compressed
    /// ([`codec::compress_one`]); written with [`record::FLAG_PAYLOAD_ZSTD`].
    Zstd(RecordInput<'a>),
    Entry(EntryInput<'a>),
}

impl WriteRecord<'_> {
    /// The record's `seq` (the entry index it belongs to).
    pub fn seq(&self) -> u64 {
        match self {
            WriteRecord::Msg(r) | WriteRecord::Zstd(r) => r.seq,
            WriteRecord::Entry(e) => e.seq,
        }
    }
}

/// One entry record read back by [`QLog::entry_records_from`] (checksum
/// verified): what Phase C recovery merges across the queue logs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntryRecord {
    pub seq: u64,
    pub now_us: i64,
    /// How many queue logs the writer wrote this entry record to (≥ 1).
    pub copies: u32,
    /// The payload-free entry bytes, exactly as written.
    pub entry: Vec<u8>,
}

/// The transaction envelope of a record read back (Phase B).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedTxn {
    pub gtid: u128,
    pub participants: Vec<u64>,
}

/// A record read back in full: the checksum has passed before any of this is
/// returned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedRecord {
    pub seq: u64,
    pub pid: u64,
    pub base_offset: u64,
    pub count: u32,
    pub created_at_us: i64,
    pub txn: Option<OwnedTxn>,
    pub hashes: Vec<u8>,
    pub payload: Vec<u8>,
}

/// Where one appended record landed: what a caller indexes it by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Loc {
    pub file_id: u64,
    pub offset: u64,
}

/// The result of [`QLog::locate`]: the file, byte offset, on-disk length and
/// message count of the record that holds a `(pid, offset)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Located {
    pub file_id: u64,
    pub offset: u64,
    pub len: u32,
    pub count: u32,
}

/// One append's committed dedup facts, served from the queue log for the
/// `DEDUP_INDEX=segment` read path when `QUEEN_RAFT_QLOG` is on (Phase A2). The
/// qlog twin of [`crate::rsm::segments::DedupFrame`]: `(base_offset) -> (end,
/// created_at, hashes)`, with `end` EXCLUSIVE (`base_offset + count`), exactly
/// the stored-`txns`-row / `.qidx` shape every dedup reader expects. Produced by
/// [`QLog::committed_frames`] / [`QLog::claim_walk`], already bounded to the
/// committed tail (`end <= committed_end`) so an applied-but-uncommitted record
/// never appears — the exactly-once invariant (lever 1, `ALICE_PGLESS_NEWARCH.md`
/// §9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedFrame {
    pub base_offset: u64,
    /// Exclusive end (`base_offset + count`).
    pub end: u64,
    pub created_at_us: i64,
    /// `16 * count` bytes in frame order, or empty when the caller asked for the
    /// shape only.
    pub hashes: Vec<u8>,
}

/// PLAN_RAFT_DRAIN_FIX P3.1: one committed record's dedup facts as the BAND read
/// ([`QLog::committed_hashes_in_bands`]) returns them — the [`CommittedFrame`]
/// shape, with the hash block SHARED with the read cache (P3.3) instead of
/// copied. `end` is EXCLUSIVE (`base_offset + count`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BandFrame {
    pub base_offset: u64,
    pub end: u64,
    pub created_at_us: i64,
    /// `16 * count` bytes in frame order (empty for a shape-only read).
    pub hashes: Arc<[u8]>,
}

/// PLAN_RAFT_DRAIN_FIX P3.1: the push dedup verdict over band frames — the MIN
/// in-window (`created >= floor_us`) offset `hash` occurs at, or `None`.
/// `frames` ascend by base (as [`QLog::committed_hashes_in_bands`] returns them),
/// so the first in-window hit is the minimum: exactly
/// [`crate::rsm::dedup::scan_seg_rows_for_hash`] over the same records.
pub fn min_in_window_offset(frames: &[BandFrame], hash: &[u8; 16], floor_us: i64) -> Option<u64> {
    for f in frames {
        if f.created_at_us < floor_us {
            continue; // whole append out of window; keep walking
        }
        for (i, h) in f.hashes.chunks_exact(record::HASH_LEN).enumerate() {
            if h == &hash[..] {
                return Some(f.base_offset + i as u64);
            }
        }
    }
    None
}

/// One record's metadata as [`QLog::scan_from`] walks a partition. The index
/// entry plus the file it is in; the callback reads the bytes itself (via
/// [`QLog::read_record`]) only if it wants them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanEntry {
    pub pid: u64,
    pub base_offset: u64,
    pub end: u64,
    pub count: u32,
    pub created_at_us: i64,
    pub file_id: u64,
    pub offset: u64,
    pub len: u32,
}

/// One `.qlog` file as the store knows it — what [`QLog::unlink_dead_files`]
/// hands the retention predicate. `min`/`max_created_at_us` are `i64::MAX` /
/// `i64::MIN` for a file that holds no record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileMeta {
    pub id: u64,
    /// The `seq` of the first record the file was created for (advisory).
    pub first_seq: u64,
    /// Valid bytes on disk, the 32-byte header included.
    pub bytes: u64,
    /// MESSAGE records in the file (entry records, Phase C, are not counted:
    /// they are not indexed).
    pub records: u64,
    /// A sealed file is immutable and has a `.qidx`; the active (last) file
    /// does not and is never sealed.
    pub sealed: bool,
    /// Over message records only.
    pub min_created_at_us: i64,
    pub max_created_at_us: i64,
    /// Phase C: an UPPER BOUND on the `seq` of every record (message OR entry)
    /// in the file — what the recovery floor guards ([`QLog::unlink_dead_files`]).
    /// Exact for the active file and for a file this process sealed; for a
    /// sealed file found at [`QLog::open`] it is the next file's `first_seq - 1`
    /// (a queue log's seqs never decrease, and a group is written whole into
    /// one file after any roll, so every record of a file is below the first
    /// record of the next). `0` for a file that holds no record.
    pub max_seq: u64,
}

/// What [`QLog::open`] found and fixed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QLogRecovery {
    pub files: usize,
    pub bytes: u64,
    pub records: u64,
    /// A torn tail on the active file (or a header-short newest file) was
    /// truncated.
    pub truncated_tail: bool,
    /// Sealed files whose `.qidx` was missing, stale or damaged and was rebuilt
    /// by scanning the `.qlog`.
    pub rebuilt_indexes: usize,
    /// Records scanned during recovery (torn-tail scan + index rebuilds).
    pub scanned_records: u64,
    /// The highest record `seq` (the leader's order stamp = the entry index)
    /// that survived recovery — the qlog's DURABLE TAIL (A3a,
    /// `ALICE_PGLESS_NEWARCH.md` §5). It is the max over `(active file's
    /// `first_seq`, the highest verified `seq` in the active file)`: the active
    /// file is always the newest, so its records (or, for an empty active file
    /// left by a crash between a roll and its first write, its `first_seq`,
    /// which is strictly above every sealed record's `seq`) bound the whole
    /// queue's tail with no sealed-file read. `0` for a queue that never took a
    /// durable record. The applier reconciles this against the store's recorded
    /// qlog-durable index (`meta::QLOG_DURABLE_INDEX`): the qlog must be AHEAD of
    /// or EQUAL to it, never behind, or a committed record was lost (NA-QLOG-I1).
    pub max_seq: u64,
}

// ---------------------------------------------------------------------------
// The store
// ---------------------------------------------------------------------------

/// The per-queue append-only log store. One instance per queue; every mutating
/// method takes `&mut self` (one writer, by construction — §6/Phase E defers
/// parallel writers).
pub struct QLog {
    dir: PathBuf,
    queue_id: u64,
    opts: QLogOptions,
    /// Every file, ascending by id. When non-empty the last is the ACTIVE file
    /// (unsealed); every other is sealed and has a [`index::View`] in `sealed`.
    files: Vec<FileMeta>,
    /// The write handle for the active file, positioned at its end. `None` for
    /// a fresh queue that has taken no append yet (the first file is created
    /// lazily, so its header's `first_seq` is the real first record's `seq`).
    active: Option<File>,
    /// The active file's index, in RAM.
    active_index: index::ActiveIndex,
    /// Sealed files' immutable indexes, mmap'd, keyed by file id.
    sealed: BTreeMap<u64, index::View>,
    /// PLAN_RAFT_DRAIN_FIX P3.2/P3.3: read fds + committed hash blocks, shared
    /// with in-flight reads that finish after the read guard is dropped.
    cache: Arc<ReadCache>,
    /// Phase C recovery floor: no file holding a record with `seq >` this may
    /// be unlinked ([`QLog::unlink_dead_files`]). A standalone log has no floor
    /// (`u64::MAX`); a log opened by a [`set::QLogSet`] shares the set's handle,
    /// which the apply thread raises to the durable index at each durable point.
    floor: Arc<AtomicU64>,
    /// The active file's physical end when a zero-filled preallocation runs past
    /// its logical end (`files.last().bytes`); at or below the logical end when
    /// there is none. Reset whenever the active file is created, cut or reopened.
    prealloc_end: u64,
    /// Syncs of this log: bumped by [`QLog::sync`] and by
    /// [`QLog::active_clone`] (the set's out-of-lock fsync), hence atomic.
    syncs: AtomicU64,
    /// `syncs` and `written_total` when the bytes-per-sync estimate last moved.
    seen_syncs: u64,
    written_at_seen: u64,
    /// Bytes appended to this log since open, across rolls.
    written_total: u64,
    /// EWMA of bytes appended per sync; 0 until the first sync is seen.
    bytes_per_sync: u64,
    /// Local retention progress. A sealed immutable file only needs another
    /// index walk when either its contents are new or a partition's txn
    /// watermark changed. Without this cache the 5 s maintenance tick walked
    /// the whole retained log forever, making request latency grow with queue
    /// history.
    reclaim_generation: u64,
    reclaim_txns_starts: HashMap<u64, u64>,
    reclaim_checked: HashMap<u64, u64>,
    reclaim_cursor: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReclaimProgress {
    pub changed: usize,
    pub examined: usize,
    pub more: bool,
}

impl QLog {
    /// Open (creating the directory if needed) queue `queue_id`'s logs under
    /// `root`, validate and truncate the torn tail of the active file, rebuild
    /// the active index by scanning it, and map (or rebuild) every sealed
    /// file's `.qidx`.
    pub fn open(root: &Path, queue_id: u64, opts: QLogOptions) -> io::Result<(QLog, QLogRecovery)> {
        let dir = queue_dir(root, queue_id);
        std::fs::create_dir_all(&dir)?;
        let mut ids = scan_ids(&dir)?;
        ids.sort_unstable();
        // A crash before the final compaction rename can leave only the private
        // rewrite file. It was never published, so reopening discards it.
        for id in &ids {
            remove_if_present(&compact_path(&dir, *id))?;
        }

        let mut rec = QLogRecovery::default();

        // A roll interrupted between creating the next file and writing its
        // 32-byte header leaves the NEWEST file shorter than the header. It
        // carries no record (the header precedes any record), so drop it and
        // fall back to the previous file as the active one. Only the newest
        // file can be in this state.
        while let Some(&last) = ids.last() {
            let len = std::fs::metadata(file_path(&dir, last))?.len();
            if len < FILE_HEADER_LEN {
                std::fs::remove_file(file_path(&dir, last))?;
                let _ = std::fs::remove_file(qidx_path(&dir, last));
                let _ = std::fs::remove_file(qidx_tmp_path(&dir, last));
                sync_dir(&dir)?;
                ids.pop();
                rec.truncated_tail = true;
            } else {
                break;
            }
        }

        let mut files: Vec<FileMeta> = Vec::new();
        let mut sealed: BTreeMap<u64, index::View> = BTreeMap::new();
        let mut active_index = index::ActiveIndex::new();
        let mut active: Option<File> = None;

        for (pos, id) in ids.iter().copied().enumerate() {
            let is_last = pos + 1 == ids.len();
            let path = file_path(&dir, id);
            let first_seq = read_file_header(&path, id)?;
            let on_disk = std::fs::metadata(&path)?.len();

            if is_last {
                // The ACTIVE file. A `.qidx` beside it is stale by construction
                // (written at a seal a completed roll would have followed with
                // a new file): drop it. Scan for the torn tail, truncate it,
                // and rebuild the RAM index from what verified.
                let _ = std::fs::remove_file(qidx_path(&dir, id));
                let _ = std::fs::remove_file(qidx_tmp_path(&dir, id));
                let scan = scan_file(&path, on_disk)?;
                if scan.torn.is_some() {
                    let f = OpenOptions::new().write(true).open(&path)?;
                    f.set_len(scan.valid_bytes)?;
                    fsync_file(&f, opts.fsync)?;
                    // A zero length word where the next record would start is
                    // the preallocated run (or a record that never reached the
                    // disk): cut it all the same, but it is not a torn record.
                    let zero_tail =
                        matches!(scan.torn, Some((_, record::RecordError::BadLength(0))));
                    rec.truncated_tail |= !zero_tail;
                }
                let mut fh = OpenOptions::new().read(true).write(true).open(&path)?;
                fh.seek(SeekFrom::Start(scan.valid_bytes))?;
                active = Some(fh);
                active_index.open(id);
                let mut meta = FileMeta::empty(id, first_seq);
                meta.bytes = scan.valid_bytes;
                for r in &scan.records {
                    active_index.insert(*r);
                    meta.absorb(r.created_at_us);
                }
                meta.records = scan.records.len() as u64;
                // Exact: the scan read every verified record (entries too).
                meta.max_seq = scan.max_seq;
                rec.scanned_records += scan.records.len() as u64;
                // The durable tail (A3a): the active file is the newest, so its
                // records dominate every sealed file's `seq`. When it holds
                // records, its highest verified `seq` is the whole queue's tail;
                // when a crash between a roll and its first write left it EMPTY,
                // its `first_seq` (the seq of the record it was created for) is
                // strictly above every sealed record's `seq` and is a sound tail
                // bound — the never-written record was never committed to the
                // store, so the store's qlog-durable index is at most one below
                // it. Either way no sealed file is re-read for this.
                rec.max_seq = first_seq.max(scan.max_seq);
                files.push(meta);
            } else {
                // A SEALED file. Use its `.qidx` if it is present, matches this
                // file and indexes exactly `on_disk` bytes; otherwise rebuild
                // it by scanning the `.qlog` (§5). A torn/damaged record in a
                // sealed file is corruption, not a torn tail.
                let view = match index::View::open(&qidx_path(&dir, id), Some(on_disk)) {
                    Ok(v) if v.check_identity(id).is_ok() => v,
                    _ => {
                        let scan = scan_file(&path, on_disk)?;
                        if let Some((at, why)) = scan.torn {
                            return Err(corrupt(
                                &path,
                                &format!("damaged record at byte {at} of a sealed file: {why}"),
                            ));
                        }
                        let mut records = scan.records;
                        index::sort_records(&mut records);
                        write_qidx(&dir, id, on_disk, &records)?;
                        rec.rebuilt_indexes += 1;
                        rec.scanned_records += records.len() as u64;
                        index::View::open(&qidx_path(&dir, id), Some(on_disk))?
                    }
                };
                let mut meta = FileMeta::empty(id, first_seq);
                meta.bytes = on_disk;
                meta.sealed = true;
                meta.records = view.len() as u64;
                for r in view.records() {
                    meta.absorb(r.created_at_us);
                }
                files.push(meta);
                sealed.insert(id, view);
            }
        }

        // Phase C: a sealed file's `max_seq` bound. The `.qidx` does not carry
        // `seq`, and rescanning every sealed file at boot is what the index
        // exists to avoid; a queue log's seqs never decrease and a group lands
        // whole in one file (the roll precedes the write), so every record of
        // file k is below the first record of file k+1 — `first_seq(k+1) - 1`
        // bounds file k. Only the recovery floor and the recovery scan read it.
        for i in 0..files.len().saturating_sub(1) {
            if files[i].sealed {
                files[i].max_seq = files[i + 1].first_seq.saturating_sub(1);
            }
        }

        rec.files = files.len();
        rec.bytes = files.iter().map(|f| f.bytes).sum();
        rec.records = files.iter().map(|f| f.records).sum();

        let cache = Arc::new(ReadCache::new(dir.clone()));
        // The active file was just cut to its last verified record.
        let prealloc_end = files.last().map_or(0, |m| m.bytes);
        let qlog = QLog {
            dir,
            queue_id,
            opts,
            files,
            active,
            active_index,
            sealed,
            cache,
            floor: Arc::new(AtomicU64::new(u64::MAX)),
            prealloc_end,
            syncs: AtomicU64::new(0),
            seen_syncs: 0,
            written_at_seen: 0,
            written_total: 0,
            bytes_per_sync: 0,
            reclaim_generation: 1,
            reclaim_txns_starts: HashMap::new(),
            reclaim_checked: HashMap::new(),
            reclaim_cursor: FIRST_FILE_ID,
        };
        tracing::info!(
            target: "rsm",
            queue = queue_id,
            files = rec.files,
            records = rec.records,
            truncated_tail = rec.truncated_tail,
            rebuilt = rec.rebuilt_indexes,
            "rsm qlog open",
        );
        Ok((qlog, rec))
    }

    pub fn queue_id(&self) -> u64 {
        self.queue_id
    }

    /// Attach a shared recovery floor (Phase C): from now on no file holding a
    /// record with `seq >` the floor's value is ever unlinked. [`set::QLogSet`]
    /// attaches its own handle to every log it opens.
    pub fn set_recovery_floor(&mut self, floor: Arc<AtomicU64>) {
        self.floor = floor;
    }

    /// The current recovery floor (`u64::MAX` = none attached).
    pub fn recovery_floor(&self) -> u64 {
        self.floor.load(Ordering::Acquire)
    }

    /// The active file id, or `None` for a queue that has taken no append.
    pub fn active_file_id(&self) -> Option<u64> {
        self.active_index.file_id()
    }

    /// Total valid bytes across every file (headers included).
    pub fn bytes(&self) -> u64 {
        self.files.iter().map(|f| f.bytes).sum()
    }

    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Every file's metadata, ascending by id (the last is the active one).
    pub fn files(&self) -> &[FileMeta] {
        &self.files
    }

    /// The current byte length of the active file (its next append offset), or
    /// the header length when a file exists but is empty, or 0 when none does.
    fn active_len(&self) -> u64 {
        self.files.last().map_or(0, |m| m.bytes)
    }

    /// Append one group of records (one `write_all`) and fsync ONCE. Returns each
    /// record's `(file_id, byte_offset)` in order. This is the durable
    /// convenience wrapper — [`QLog::write_group`] then [`QLog::sync`] — kept for
    /// direct callers and the A0 tests. The A2 apply path instead calls
    /// `write_group` PER ENTRY (so the record is page-cache readable before the
    /// leader answers, exactly as `segments.flush_writes`) and `sync` at the
    /// durable point (so the fsync stays batched).
    pub fn append_group(&mut self, records: &[RecordInput<'_>]) -> io::Result<Vec<Loc>> {
        let locs = self.write_group(records)?;
        self.sync()?;
        Ok(locs)
    }

    /// Append one group of records with ONE `write_all` and NO fsync: the bytes
    /// reach the page cache and the RAM index at once, so a reader sees the
    /// record immediately, but it is not durable until [`QLog::sync`]. This is
    /// the A2 write path — the leader answers a pop right after apply, before the
    /// commit that fsyncs, so the payload MUST be readable without waiting for the
    /// barrier (the segment path does the same: `flush_writes` per entry, fsync
    /// per durable point). A crash before the sync leaves an un-fsync'd tail that
    /// [`QLog::open`] truncates; the record was never acknowledged durable.
    pub fn write_group(&mut self, records: &[RecordInput<'_>]) -> io::Result<Vec<Loc>> {
        let mixed: Vec<WriteRecord<'_>> = records.iter().map(|r| WriteRecord::Msg(*r)).collect();
        self.write_mixed(&mixed)
    }

    /// [`QLog::write_group`] for a MIXED group of message and entry records
    /// (Phase C: the log writer appends each entry's payload records and then
    /// its payload-free entry record, entry by entry, in one `write_all`, NO
    /// fsync). Entry records are written but never indexed. Returns every
    /// record's position, in order. `records` must be in non-decreasing `seq`
    /// order (the first one's `seq` names a freshly rolled file).
    pub fn write_mixed(&mut self, records: &[WriteRecord<'_>]) -> io::Result<Vec<Loc>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let first_seq = records[0].seq();
        // Ensure an active file with room. A group larger than the limit still
        // goes whole into the (freshly rolled or created) file.
        if self.active.is_none() {
            self.create_active(FIRST_FILE_ID, first_seq)?;
        } else {
            let sz = self.active_len();
            if sz > FILE_HEADER_LEN && sz >= self.opts.segment_bytes {
                self.roll(first_seq)?;
            }
        }

        let file_id = self.files.last().expect("active meta").id;
        let base = self.active_len();
        let cap = records
            .iter()
            .map(|w| match w {
                WriteRecord::Msg(r) | WriteRecord::Zstd(r) => record::encoded_len(
                    r.count,
                    r.txn.map_or(0, |t| t.participants.len()),
                    r.payload.len(),
                ),
                WriteRecord::Entry(e) => record::FIXED_PREFIX + e.entry.len(),
            })
            .sum();
        let mut buf: Vec<u8> = Vec::with_capacity(cap);
        let mut locs = Vec::with_capacity(records.len());
        let mut new_recs: Vec<index::Record> = Vec::with_capacity(records.len());
        let mut max_seq = 0u64;
        for w in records {
            let start = buf.len();
            let offset = base + start as u64;
            max_seq = max_seq.max(w.seq());
            match w {
                WriteRecord::Msg(r) | WriteRecord::Zstd(r) => {
                    let txn = r.txn.map(|t| (t.gtid, t.participants));
                    let n = record::encode_msg_into(
                        &mut buf,
                        r.seq,
                        r.pid,
                        r.base_offset,
                        r.count,
                        r.created_at_us,
                        txn,
                        r.hashes,
                        r.payload,
                        matches!(w, WriteRecord::Zstd(_)),
                    )?;
                    new_recs.push(index::Record {
                        pid: r.pid,
                        base_offset: r.base_offset,
                        end: r.base_offset.saturating_add(r.count as u64),
                        created_at_us: r.created_at_us,
                        offset,
                        count: r.count,
                        len: n as u32,
                    });
                }
                WriteRecord::Entry(e) => {
                    record::encode_entry_copies_into(
                        &mut buf,
                        e.seq,
                        e.now_us,
                        u64::from(e.copies),
                        e.entry,
                    );
                }
            }
            locs.push(Loc { file_id, offset });
        }

        let f = self.active.as_mut().expect("active file");
        f.write_all(&buf)?;
        self.prealloc_after_write(base + buf.len() as u64, buf.len() as u64)?;

        // Bookkeeping advances only after the write returns (the sync is
        // separate — [`QLog::sync`]). Recovery rebuilds the real valid length
        // from the bytes on disk, so if a crash lands before the sync, the
        // un-fsync'd tail is truncated and the records are simply unanswered
        // (their propose never returned, or the authoritative segment/store the
        // A2 shadow rides did not commit them).
        let added = buf.len() as u64;
        {
            let meta = self.files.last_mut().expect("active meta");
            meta.bytes += added;
            meta.records += new_recs.len() as u64;
            meta.max_seq = meta.max_seq.max(max_seq);
            for r in &new_recs {
                meta.absorb(r.created_at_us);
            }
        }
        for r in new_recs {
            self.active_index.insert(r);
        }
        Ok(locs)
    }

    /// Fsync the active file, making every record [`QLog::write_group`] wrote
    /// since the last sync durable (§5). A no-op when the queue has no active
    /// file (nothing was written). Sealed files were fsync'd at their roll, so
    /// only the active file's tail is ever un-synced.
    pub fn sync(&mut self) -> io::Result<()> {
        if let Some(f) = self.active.as_ref() {
            self.syncs.fetch_add(1, Ordering::Relaxed);
            fsync_file(f, self.opts.fsync)?;
        }
        Ok(())
    }

    /// After a write that moved the logical end to `end` (`added` bytes):
    /// refresh the bytes-per-sync estimate, then, while it is small, keep at
    /// a quarter of [`prealloc_chunk`] zero-filled past `end` so the next
    /// syncs overwrite blocks the file already has (see [`PREALLOC_CHUNK`]).
    /// The zeros are written with `pwrite`, so the append cursor stays at `end`.
    fn prealloc_after_write(&mut self, end: u64, added: u64) -> io::Result<()> {
        let syncs = self.syncs.load(Ordering::Relaxed);
        if syncs > self.seen_syncs {
            // Every byte counted here was written before those syncs ran.
            let per =
                ((self.written_total - self.written_at_seen) / (syncs - self.seen_syncs)).max(1);
            self.bytes_per_sync = if self.bytes_per_sync == 0 {
                per
            } else {
                ((3 * self.bytes_per_sync + per) / 4).max(1)
            };
            self.seen_syncs = syncs;
            self.written_at_seen = self.written_total;
        }
        self.written_total += added;
        let chunk = prealloc_chunk();
        if chunk == 0
            || self.opts.fsync == Fsync::Off
            || self.bytes_per_sync == 0
            || self.bytes_per_sync >= PREALLOC_MAX_BYTES_PER_SYNC
            || end + chunk / 4 <= self.prealloc_end
        {
            return Ok(());
        }
        // No run past the roll size: the next write rolls there anyway.
        let target = (end + chunk).min(self.opts.segment_bytes.max(end));
        let from = self.prealloc_end.max(end);
        if target > from {
            let f = self.active.as_ref().expect("active file");
            zero_fill(f, from, target)?;
            self.prealloc_end = target;
        }
        Ok(())
    }

    /// A dup'd handle to the active file, so the caller can [`fsync_file`] it
    /// OUTSIDE the per-queue lock — the read-parallelism fix (a single hot
    /// queue's 64 consumers must not block for the writer's fsync). `fsync`
    /// flushes the inode's dirty pages regardless of which fd is used, and the
    /// SINGLE writer never rolls between taking this clone and fsyncing it, so
    /// the clone is always the current active file. `None` when nothing was
    /// written yet.
    pub(crate) fn active_clone(&self) -> io::Result<Option<File>> {
        match self.active.as_ref() {
            Some(f) => {
                // The caller takes this handle to fsync it: count the sync.
                self.syncs.fetch_add(1, Ordering::Relaxed);
                Ok(Some(f.try_clone()?))
            }
            None => Ok(None),
        }
    }

    /// Create a fresh active file id `id` whose header records `first_seq`.
    fn create_active(&mut self, id: u64, first_seq: u64) -> io::Result<()> {
        let path = file_path(&self.dir, id);
        {
            // `create_new`: a collision means the disk already holds a file id
            // the store thinks is new — never a thing to paper over.
            let mut f = OpenOptions::new()
                .create_new(true)
                .write(true)
                .read(true)
                .open(&path)?;
            let mut h = [0u8; FILE_HEADER_LEN as usize];
            h[0..8].copy_from_slice(&FILE_MAGIC);
            h[8..16].copy_from_slice(&id.to_le_bytes());
            h[16..24].copy_from_slice(&first_seq.to_le_bytes());
            // [24..32] reserved, zero.
            f.write_all(&h)?;
            f.sync_all()?;
        }
        sync_dir(&self.dir)?;
        let mut f = OpenOptions::new().read(true).write(true).open(&path)?;
        f.seek(SeekFrom::Start(FILE_HEADER_LEN))?;
        self.active = Some(f);
        self.prealloc_end = FILE_HEADER_LEN;
        self.files.push(FileMeta::empty(id, first_seq));
        self.active_index.open(id);
        Ok(())
    }

    /// Seal the active file — write its `.qidx`, map it, mark it sealed — and
    /// create the next active file whose first record will carry `first_seq`.
    fn roll(&mut self, first_seq: u64) -> io::Result<()> {
        // Fsync the outgoing active file's DATA before sealing it. A sealed file
        // must be FULLY durable: [`QLog::open`] treats a torn or damaged record
        // in a sealed file as corruption (it is surfaced, never truncated),
        // because only the active file can carry a torn tail. Since
        // [`QLog::write_group`] defers the per-record fsync to the durable point
        // ([`QLog::sync`]), the roll is where a file about to become sealed must
        // reach the platter — otherwise its unsynced tail would seal, and a power
        // loss would corrupt it. A no-op under `Fsync::Off`.
        if let Some(f) = self.active.as_ref() {
            // A sealed file ends at its last record: cut the zero run first (the
            // `.qidx` indexes exactly the logical length, and a sealed file's
            // scan treats anything past its last record as corruption). The
            // size change rides the same fsync.
            let logical = self.files.last().expect("active meta").bytes;
            if self.prealloc_end > logical {
                f.set_len(logical)?;
            }
            fsync_file(f, self.opts.fsync)?;
        }
        let (old_id, old_bytes) = {
            let m = self.files.last().expect("active meta");
            (m.id, m.bytes)
        };
        // `take` returns the records in (pid, base_offset) order (the map's
        // order), which is exactly the order `index::encode` needs.
        let recs = self.active_index.take();
        write_qidx(&self.dir, old_id, old_bytes, &recs)?;
        self.files.last_mut().expect("active meta").sealed = true;
        let view = index::View::open(&qidx_path(&self.dir, old_id), Some(old_bytes))?;
        self.sealed.insert(old_id, view);
        self.active = None;
        self.create_active(old_id + 1, first_seq)?;
        Ok(())
    }

    /// The file, byte offset, on-disk length and message count of the record
    /// holding `(pid, offset)`, or `None` if no live file holds it (retention
    /// deleted it, or it was never written).
    pub fn locate(&self, pid: u64, offset: u64) -> Option<Located> {
        if let Some((file_id, r)) = self.active_index.probe(pid, offset) {
            return Some(Located {
                file_id,
                offset: r.offset,
                len: r.len,
                count: r.count,
            });
        }
        for (file_id, view) in &self.sealed {
            match view.probe(pid, offset) {
                index::Probe::Hit(r) => {
                    return Some(Located {
                        file_id: *file_id,
                        offset: r.offset,
                        len: r.len,
                        count: r.count,
                    })
                }
                // The offset falls in a gap this file's span covers: no other
                // file can hold it.
                index::Probe::Hole => return None,
                _ => {}
            }
        }
        None
    }

    /// Read one record by its position, checksum-verified. `file_id`/`offset`
    /// come from a [`Loc`] returned by [`QLog::append_group`], or from a
    /// [`ScanEntry`]. The record's own `len` bounds the read.
    pub fn read_record(&self, file_id: u64, offset: u64) -> io::Result<OwnedRecord> {
        // P3.2: a cached fd, no open() per read.
        let f = self.cache.file(file_id)?;
        // Read the fixed prefix to learn the record length, then the whole
        // record. `record_len` is bounded by `parse_header` (≤ 4 + the body
        // cap), so a lie allocates nothing unbounded.
        let mut prefix = [0u8; record::FIXED_PREFIX];
        f.read_exact_at(&mut prefix, offset)?;
        let header = record::parse_header(&prefix).map_err(io::Error::from)?;
        let total = header.record_len();
        let mut buf = vec![0u8; total];
        f.read_exact_at(&mut buf, offset)?;
        let rr = record::decode(&buf).map_err(io::Error::from)?;
        owned_from(&rr)
    }

    /// Locate `(pid, offset)` and read the record there, checking that the
    /// index's `len` is exactly the record's own length (a position one byte
    /// too long would checksum-verify and leak the head of the next record —
    /// the raft class's `LenMismatch` guard). `None` if no live file holds it.
    pub fn read_payload(&self, pid: u64, offset: u64) -> io::Result<Option<Vec<u8>>> {
        match self.locate(pid, offset) {
            Some(loc) => Ok(Some(self.read_located(&loc)?.payload)),
            None => Ok(None),
        }
    }

    /// The `16 * count`-byte hash list of the record holding `(pid, offset)` —
    /// what dedup reads (a later phase). `None` if no live file holds it.
    pub fn read_hashes(&self, pid: u64, offset: u64) -> io::Result<Option<Vec<u8>>> {
        match self.locate(pid, offset) {
            Some(loc) => Ok(Some(self.read_located(&loc)?.hashes)),
            None => Ok(None),
        }
    }

    /// The whole record holding `(pid, offset)`, checksum-verified with the
    /// `len`-match guard — what the pop payload read reads (Phase A2). `None` if
    /// no live file holds it. Unlike [`QLog::read_payload`] it returns the
    /// header too (`base_offset`, `count`, `created_at_us`), which the pop render
    /// needs to unpack and stamp the frame exactly as the segment read did.
    pub fn read_owned(&self, pid: u64, offset: u64) -> io::Result<Option<OwnedRecord>> {
        match self.locate(pid, offset) {
            Some(loc) => Ok(Some(self.read_located(&loc)?)),
            None => Ok(None),
        }
    }

    /// Locate `(pid, offset)` returning the FULL index record (base offset, end,
    /// created_at and the node-local position) plus its file id — what the
    /// O(claimed) claim walk needs and [`QLog::locate`] drops. Active file first
    /// (a roll can only add a newer record for a base that is not yet claimable),
    /// then a binary search of the sealed files. A `Hole` in a file's span means
    /// no other file can hold it (retention deleted it).
    fn locate_record(&self, pid: u64, offset: u64) -> Option<(u64, index::Record)> {
        if let Some((file_id, r)) = self.active_index.probe(pid, offset) {
            return Some((file_id, r));
        }
        for (file_id, view) in &self.sealed {
            match view.probe(pid, offset) {
                index::Probe::Hit(r) => return Some((*file_id, r)),
                index::Probe::Hole => return None,
                _ => {}
            }
        }
        None
    }

    /// Read one located COMMITTED index record's hashes (the `16 * count`
    /// bytes). PLAN_RAFT_DRAIN_FIX P3: from the hash cache, else a hashes-only
    /// `pread` through the cached fd ([`pread_hashes`], header cross-checked
    /// against the index), cached while there is room. Callers pass only records
    /// bounded by the committed tail (the cache holds committed blocks only).
    fn read_record_hashes(&self, file_id: u64, rec: &index::Record) -> io::Result<Vec<u8>> {
        if rec.count == 0 {
            return Ok(Vec::new());
        }
        if let Some(h) = self.cache.hash_get(rec.pid, rec.base_offset) {
            return Ok(h.to_vec());
        }
        let f = self.cache.file(file_id)?;
        let h = pread_hashes(&f, &self.dir, file_id, rec, &mut Vec::new())?;
        self.cache
            .hash_put_many(&[(rec.pid, rec.base_offset, h.clone())], CachePut::IfRoom);
        Ok(h.to_vec())
    }

    /// Walk partition `pid`'s committed records FORWARD from `from_offset`, in
    /// offset order, invoking `cb(base, end_inclusive, created_at, hashes)` for
    /// each until it returns `false` (the pop budget) or the committed tail is
    /// reached — the O(claimed) delivered-set / claim walk, the qlog twin of
    /// [`crate::rsm::segments::Reader::claim_frames`] for `DEDUP_INDEX=segment`.
    ///
    /// It reads (and, when `want_hashes`, `pread`s) only the records the claim
    /// actually consumes (≤ budget), NEVER the whole cursor→tail span: each step
    /// is one [`QLog::locate_record`] (a binary search of the index), advancing
    /// `cur` to the record's exclusive `end`. The range MUST be contiguous, which
    /// it is on the pop path (retention is a prefix delete, no interior holes, and
    /// the caller gates `wanted >= log_start`), so a `locate` miss means the
    /// committed tail, not a gap.
    ///
    /// THE ONE CORRECTNESS INVARIANT (exactly-once, lever 1): the walk stops at
    /// the committed tail. A record whose exclusive `end` reaches PAST
    /// `committed_end` is applied-but-not-committed (or in flight); it is NOT part
    /// of the planner's committed snapshot and IS covered by its overlay, so
    /// reading it here would double-count. `committed_end` is the committed
    /// partition row's `last_offset + 1`, read by the planner via the RoTxn.
    pub fn claim_walk(
        &self,
        pid: u64,
        from_offset: u64,
        committed_end: u64,
        want_hashes: bool,
        cb: &mut dyn FnMut(u64, u64, i64, Option<Vec<u8>>) -> bool,
    ) -> io::Result<()> {
        let mut cur = from_offset;
        while cur < committed_end {
            let Some((file_id, rec)) = self.locate_record(pid, cur) else {
                break; // past the committed tail (the range is contiguous)
            };
            if rec.end > committed_end {
                break; // uncommitted record: the overlay covers it (the invariant)
            }
            let hashes = if want_hashes {
                Some(self.read_record_hashes(file_id, &rec)?)
            } else {
                None
            };
            let keep = cb(rec.base_offset, rec.end - 1, rec.created_at_us, hashes);
            if !keep {
                break;
            }
            cur = rec.end; // contiguous: the next record starts here
        }
        Ok(())
    }

    /// Every committed record of `pid` whose span reaches above `from_base`, in
    /// base-offset order, with its hash list (when `with_hashes`) — the qlog twin
    /// of [`crate::rsm::segments::Reader::committed_dedup_rows`] /
    /// `committed_dedup_shape` for the dedup probe / resolve / seed authority.
    /// Bounded to the committed tail (`end <= committed_end`), the exactly-once
    /// invariant. `from_base = 0` is the whole committed window (bloom-gated, so
    /// its O(window) hash reads are acceptable there); a positive `from_base`
    /// drops records wholly below it before their hash `pread`.
    ///
    /// Unlike [`QLog::claim_walk`] this gathers the candidate records from the
    /// index directly (active file, then the sealed files) rather than a
    /// contiguous forward walk, so it is robust to a future interior hole exactly
    /// as the segment `committed_dedup_frames` is. A `(pid, base_offset)` present
    /// in both the active and a sealed file (only after a crash-and-replay, which
    /// re-appends an identical record — see the module recovery note) is taken
    /// from the ACTIVE file, first-source-wins, matching the segment path.
    pub fn committed_frames(
        &self,
        pid: u64,
        from_base: u64,
        committed_end: u64,
        with_hashes: bool,
    ) -> io::Result<Vec<CommittedFrame>> {
        let plan = self.committed_frames_plan(pid, from_base, committed_end, with_hashes)?;
        Ok(committed_of(plan.finish()?))
    }

    /// The candidate walk of [`QLog::committed_frames`], as a [`HashPlan`]: the
    /// index work + cache probes under the caller's read guard, the misses'
    /// `pread`s in [`HashPlan::finish`] (possibly after the guard is dropped —
    /// [`set::QLogReader::committed_frames`]). The whole-window read must not
    /// flush the band working set, so its misses are cached only while there is
    /// room (P3.3).
    fn committed_frames_plan(
        &self,
        pid: u64,
        from_base: u64,
        committed_end: u64,
        with_hashes: bool,
    ) -> io::Result<HashPlan> {
        // (base_offset) -> (file_id, record), first source wins; the active file
        // is inserted first so a replayed duplicate resolves to it.
        let mut cand: BTreeMap<u64, (u64, index::Record)> = BTreeMap::new();
        if let Some(active_id) = self.active_index.file_id() {
            for r in self.active_index.records_of_from(pid, from_base) {
                if r.end <= committed_end {
                    cand.entry(r.base_offset).or_insert((active_id, r));
                }
            }
        }
        for (file_id, view) in &self.sealed {
            for r in view.records_of(pid) {
                if r.end > from_base && r.end <= committed_end {
                    cand.entry(r.base_offset).or_insert((*file_id, r));
                }
            }
        }
        self.hash_plan(cand.into_values(), with_hashes, CachePut::IfRoom)
    }

    /// PLAN_RAFT_DRAIN_FIX P3.1: the committed records of `pid` that overlap ANY
    /// of `bands` — inclusive offset bands `(lo, hi)`, the `(min_base, max_off)`
    /// of each dedup-front generation whose bloom said "maybe" — with their hash
    /// blocks, ascending by base offset. A record `[base, end)` overlaps `(lo, hi)`
    /// iff `base <= hi && end > lo`.
    ///
    /// Bounded to the committed tail (`end <= committed_end`) exactly as
    /// [`QLog::committed_frames`] — the exactly-once invariant: an uncommitted
    /// record is the overlay's. Duplicate `(pid, base_offset)` resolve active
    /// first, first source wins, as there. Hashes come from the cross-cycle cache
    /// (P3.3) or a hashes-only `pread` through a cached fd (P3.2), never the
    /// payload; misses are cached with clear-when-full eviction (this is the hot
    /// path the cache exists for).
    pub fn committed_hashes_in_bands(
        &self,
        pid: u64,
        bands: &[(u64, u64)],
        committed_end: u64,
    ) -> io::Result<Vec<BandFrame>> {
        self.band_plan(pid, bands, committed_end)?.finish()
    }

    /// The candidate walk of [`QLog::committed_hashes_in_bands`] as a
    /// [`HashPlan`]: O(log n + band) per file via the index, never a partition's
    /// whole run.
    fn band_plan(
        &self,
        pid: u64,
        bands: &[(u64, u64)],
        committed_end: u64,
    ) -> io::Result<HashPlan> {
        let mut cand: BTreeMap<u64, (u64, index::Record)> = BTreeMap::new();
        if committed_end > 0 {
            let active_id = self.active_index.file_id();
            for (lo, hi) in merge_bands(bands) {
                // A committed record ends at or below `committed_end`, so it
                // starts below it: nothing above `committed_end - 1` can qualify.
                if lo >= committed_end {
                    continue;
                }
                let hi = hi.min(committed_end - 1);
                if let Some(active_id) = active_id {
                    for r in self.active_index.records_overlapping(pid, lo, hi) {
                        if r.end <= committed_end {
                            cand.entry(r.base_offset).or_insert((active_id, r));
                        }
                    }
                }
                for (file_id, view) in &self.sealed {
                    for r in view.records_overlapping(pid, lo, hi) {
                        if r.end <= committed_end {
                            cand.entry(r.base_offset).or_insert((*file_id, r));
                        }
                    }
                }
            }
        }
        self.hash_plan(cand.into_values(), true, CachePut::Evict)
    }

    /// Resolve the hash block of each candidate: a cache hit (one lock for the
    /// whole batch), or a miss carrying the fd its `pread` will use (resolved
    /// HERE, under the caller's read guard, so a file retention unlinks later is
    /// still readable). `with_hashes == false` is a shape-only plan.
    fn hash_plan(
        &self,
        cand: impl IntoIterator<Item = (u64, index::Record)>,
        with_hashes: bool,
        put: CachePut,
    ) -> io::Result<HashPlan> {
        let cand: Vec<(u64, index::Record)> = cand.into_iter().collect();
        let mut items: Vec<(index::Record, Slot)> = Vec::with_capacity(cand.len());
        if !with_hashes {
            items.extend(cand.into_iter().map(|(_, r)| (r, Slot::Shape)));
            return Ok(HashPlan {
                cache: self.cache.clone(),
                put,
                items,
            });
        }
        let mut misses: Vec<usize> = Vec::new();
        {
            let hc = self.cache.hashes.lock().expect("qlog hash cache poisoned");
            for (file_id, r) in &cand {
                let slot = if r.count == 0 {
                    Slot::Shape
                } else if let Some(h) = hc.map.get(&(r.pid, r.base_offset)) {
                    Slot::Hit(h.clone())
                } else {
                    misses.push(items.len());
                    Slot::Miss(*file_id, None)
                };
                items.push((*r, slot));
            }
        }
        // One fd per distinct file, from the fd cache (or opened for this read
        // alone when the process-wide fd cap is reached).
        let mut fds: HashMap<u64, Arc<File>> = HashMap::new();
        for i in misses {
            if let Slot::Miss(file_id, fd) = &mut items[i].1 {
                let f = match fds.get(file_id) {
                    Some(f) => f.clone(),
                    None => {
                        let f = self.cache.file(*file_id)?;
                        fds.insert(*file_id, f.clone());
                        f
                    }
                };
                *fd = Some(f);
            }
        }
        Ok(HashPlan {
            cache: self.cache.clone(),
            put,
            items,
        })
    }

    /// The whole record at a located position, with the `len`-match guard.
    fn read_located(&self, loc: &Located) -> io::Result<OwnedRecord> {
        let f = self.cache.file(loc.file_id)?; // P3.2: cached fd, no open() per read
        read_located_in(&f, &self.dir, loc)
    }

    /// Walk partition `pid`'s records in offset order from `from_offset`,
    /// handing each to `cb` (metadata only — the callback reads bytes itself if
    /// it wants them). `cb` returns `true` to continue, `false` to stop. What
    /// recovery, the dedup-window rebuild and the pop gather use (later phases).
    pub fn scan_from(
        &self,
        pid: u64,
        from_offset: u64,
        mut cb: impl FnMut(&ScanEntry) -> bool,
    ) -> io::Result<()> {
        let mut entries: Vec<ScanEntry> = Vec::new();
        for (file_id, view) in &self.sealed {
            for r in view.records_of(pid) {
                if r.end > from_offset {
                    entries.push(scan_entry(*file_id, &r));
                }
            }
        }
        if let Some(active_id) = self.active_index.file_id() {
            for r in self.active_index.records_of_from(pid, from_offset) {
                entries.push(scan_entry(active_id, &r));
            }
        }
        // Across files a partition's base offsets already ascend with file id;
        // the sort makes the order explicit and is cheap (one partition's runs).
        entries.sort_by_key(|e| e.base_offset);
        for e in &entries {
            if !cb(e) {
                break;
            }
        }
        Ok(())
    }

    /// Phase C recovery read: every [`record::REC_ENTRY`] record of this log
    /// with `seq >= from_seq`, in file order (= non-decreasing `seq`),
    /// checksum-verified, with its bytes. A file whose `max_seq` is below
    /// `from_seq` is skipped unread (a bound for a sealed file, exact for the
    /// active one — so an idle queue costs nothing at boot); every other file is
    /// scanned. A damaged record in a SEALED file is corruption and is surfaced;
    /// in the active file ([`QLog::open`] already truncated its torn tail) a
    /// record that no longer verifies ends the walk — the reopen's torn-tail
    /// rule.
    pub fn entry_records_from(&self, from_seq: u64) -> io::Result<Vec<EntryRecord>> {
        let mut out: Vec<EntryRecord> = Vec::new();
        for m in &self.files {
            if m.max_seq < from_seq {
                continue;
            }
            let path = file_path(&self.dir, m.id);
            let (_valid, torn) = scan_records(&path, m.bytes, |h, _pos, bytes| {
                if h.kind() == record::REC_ENTRY && h.seq >= from_seq {
                    let rr = record::decode(bytes).map_err(io::Error::from)?;
                    out.push(EntryRecord {
                        seq: h.seq,
                        now_us: h.created_at_us,
                        // `pid` carries `copies`; 0 (pre-Phase-C) reads as one.
                        copies: u32::try_from(h.pid).unwrap_or(u32::MAX).max(1),
                        entry: rr.payload.to_vec(),
                    });
                }
                Ok(())
            })?;
            if let Some((at, why)) = torn {
                if m.sealed {
                    return Err(corrupt(
                        &path,
                        &format!("damaged record at byte {at} of a sealed file: {why}"),
                    ));
                }
                break; // the active file is the last one
            }
        }
        Ok(out)
    }

    /// Phase C recovery: drop every record (message or entry) with `seq >= cut`
    /// — the unacknowledged tail of a group whose fsyncs did not all land, so
    /// its entries are incomplete across the queue logs (or leave a gap) and
    /// recovery stopped before them. Their seqs are reassigned to NEW entries
    /// from here on, so a stale record left behind would collide with them on
    /// the next replay: it must go, durably, before the writer starts.
    ///
    /// Only the ACTIVE file can hold such a tail: the tail's group was written
    /// into the then-active file (a roll precedes a group's write) and nothing
    /// after it rolled. A sealed file holding a record at or above `cut` means an
    /// incomplete group was sealed behind a later write — impossible unless an
    /// fsync lied or a log was tampered with — and is refused, never guessed.
    /// Seqs in a queue log never decrease, so the cut is a byte offset: the
    /// first record at or above `cut`; every record after it must be at or above
    /// `cut` too (refused otherwise). Returns the bytes dropped.
    pub fn truncate_seq_from(&mut self, cut: u64) -> io::Result<u64> {
        if self.files.iter().all(|m| m.max_seq < cut) {
            return Ok(0);
        }
        // Sealed files: `max_seq` may be the loose `next.first_seq - 1` bound, so
        // a file at or above the cut is checked record by record (rare: only
        // after a crash whose incomplete group rolled this queue's log).
        for i in 0..self.files.len() {
            let m = self.files[i];
            if !m.sealed || m.max_seq < cut {
                continue;
            }
            let path = file_path(&self.dir, m.id);
            let mut actual = 0u64;
            let (_valid, torn) = scan_records(&path, m.bytes, |h, _pos, _bytes| {
                actual = actual.max(h.seq);
                Ok(())
            })?;
            if let Some((at, why)) = torn {
                return Err(corrupt(
                    &path,
                    &format!("damaged record at byte {at} of a sealed file: {why}"),
                ));
            }
            if actual >= cut {
                return Err(corrupt(
                    &path,
                    &format!(
                        "a SEALED file holds a record at seq {actual} >= the recovery cut {cut}: \
                         an incomplete group was sealed behind a later write"
                    ),
                ));
            }
            self.files[i].max_seq = actual;
        }
        let Some(last) = self.files.last().copied() else {
            return Ok(0);
        };
        if last.sealed || last.max_seq < cut {
            return Ok(0);
        }
        let path = file_path(&self.dir, last.id);
        let mut keep: Vec<index::Record> = Vec::new();
        let mut kept_max = 0u64;
        let mut cut_at: Option<u64> = None;
        let mut out_of_order: Option<(u64, u64)> = None;
        scan_records(&path, last.bytes, |h, pos, _bytes| {
            if h.seq >= cut {
                cut_at.get_or_insert(pos);
            } else if cut_at.is_some() {
                out_of_order.get_or_insert((pos, h.seq));
            } else {
                kept_max = kept_max.max(h.seq);
                if h.kind() != record::REC_ENTRY {
                    keep.push(index::Record::of_header(h, pos));
                }
            }
            Ok(())
        })?;
        if let Some((pos, seq)) = out_of_order {
            return Err(corrupt(
                &path,
                &format!(
                    "record at byte {pos} (seq {seq}) follows a record at or above the \
                     recovery cut {cut}: the log's seqs are not monotone"
                ),
            ));
        }
        let Some(at) = cut_at else {
            self.files.last_mut().expect("active meta").max_seq = kept_max;
            return Ok(0);
        };
        let dropped = last.bytes - at;
        {
            let f = OpenOptions::new().write(true).open(&path)?;
            f.set_len(at)?;
            // Durable BEFORE any new record reuses these seqs.
            fsync_file(&f, self.opts.fsync)?;
        }
        self.prealloc_end = at;
        if let Some(f) = self.active.as_mut() {
            f.seek(SeekFrom::Start(at))?;
        }
        self.active_index.open(last.id);
        {
            let meta = self.files.last_mut().expect("active meta");
            meta.bytes = at;
            meta.records = keep.len() as u64;
            meta.min_created_at_us = i64::MAX;
            meta.max_created_at_us = i64::MIN;
            meta.max_seq = kept_max;
            for r in &keep {
                meta.absorb(r.created_at_us);
            }
        }
        for r in keep {
            self.active_index.insert(r);
        }
        // Nothing dropped was ever read as committed in this process, but the
        // cache is keyed by position-free `(pid, base)`: drop it wholesale.
        self.cache.clear_hashes();
        tracing::warn!(
            target: "rsm",
            queue = self.queue_id,
            cut,
            dropped_bytes = dropped,
            "rsm qlog: dropped an unacknowledged tail at the recovery cut",
        );
        Ok(dropped)
    }

    /// Retention: unlink WHOLE files the predicate marks dead. Only
    /// SEALED files are candidates — the active file is never unlinked. Returns
    /// how many were dropped.
    ///
    /// Phase C: the queue logs are the ONLY write-ahead log, so a file that
    /// holds any record (message or entry) with `seq` above the recovery floor
    /// ([`QLog::set_recovery_floor`], the store's durable index) is never a
    /// candidate, whatever the predicate says — recovery replays from there.
    ///
    pub fn unlink_dead_files(&mut self, is_dead: impl Fn(&FileMeta) -> bool) -> io::Result<usize> {
        let active_id = self.active_index.file_id();
        let floor = self.floor.load(Ordering::Acquire);
        let victims: Vec<u64> = self
            .files
            .iter()
            .filter(|m| m.sealed && Some(m.id) != active_id && m.max_seq <= floor && is_dead(m))
            .map(|m| m.id)
            .collect();
        if victims.is_empty() {
            return Ok(0);
        }
        for id in &victims {
            self.sealed.remove(id);
            // P3.2: drop the cached read fd (an in-flight read keeps its own
            // `Arc<File>`, so the unlinked inode stays readable until it ends).
            self.cache.forget_file(*id);
            remove_if_present(&file_path(&self.dir, *id))?;
            remove_if_present(&qidx_path(&self.dir, *id))?;
            remove_if_present(&qidx_tmp_path(&self.dir, *id))?;
        }
        // P3.3: the dropped records' hash blocks are unreachable (no index entry
        // names them); retention is rare, so free them all rather than track
        // which file each block came from.
        self.cache.clear_hashes();
        self.files.retain(|m| !victims.contains(&m.id));
        sync_dir(&self.dir)?;
        tracing::debug!(
            target: "rsm",
            queue = self.queue_id,
            dropped = victims.len(),
            "rsm qlog dead files unlinked",
        );
        Ok(victims.len())
    }

    /// Reclaim sealed files below their partitions' `txns_start` watermarks.
    /// Fully-dead files are unlinked; partially-live files are rewritten
    /// atomically with only their live message records. Entry records are safe
    /// to omit once the whole file is at or below the recovery floor: the
    /// durable store already covers those entries.
    ///
    /// The txns watermark, rather than
    /// `log_start`, is the safe boundary for a queue log: each record carries
    /// both the payload and the hash list used by dedup/ack-by-hash, and the
    /// latter deliberately outlives payload retention (D10).
    ///
    /// A pid absent from `txns_starts` has already been removed from committed
    /// state, so its records are dead. The recovery-floor guard excludes every
    /// file recovery may still need, and the active file is never rewritten.
    /// Thus a stale maintenance read can only retain bytes longer; it cannot
    /// remove bytes that committed state still names.
    pub fn unlink_below_txns(
        &mut self,
        txns_starts: &std::collections::HashMap<u64, u64>,
    ) -> io::Result<usize> {
        Ok(self
            .unlink_below_txns_bounded(txns_starts, usize::MAX)?
            .changed)
    }

    /// One bounded local-GC step. At most `max_files` immutable indexes are
    /// examined, and an unchanged file/watermark pair is never examined twice.
    /// The cursor rotates so a hot mixed file cannot starve newer files when
    /// watermarks advance on every retention tick.
    pub(crate) fn unlink_below_txns_bounded(
        &mut self,
        txns_starts: &std::collections::HashMap<u64, u64>,
        max_files: usize,
    ) -> io::Result<ReclaimProgress> {
        if self.reclaim_txns_starts != *txns_starts {
            self.reclaim_txns_starts = txns_starts.clone();
            self.reclaim_generation = self.reclaim_generation.wrapping_add(1).max(1);
        }
        let generation = self.reclaim_generation;
        let floor = self.floor.load(Ordering::Acquire);
        let active = self.active_index.file_id();
        let mut candidates: Vec<u64> = self
            .files
            .iter()
            .filter(|meta| meta.sealed && Some(meta.id) != active && meta.max_seq <= floor)
            .filter(|meta| self.reclaim_checked.get(&meta.id).copied() != Some(generation))
            .map(|meta| meta.id)
            .collect();
        let split = candidates.partition_point(|id| *id < self.reclaim_cursor);
        candidates.rotate_left(split);
        let limit = max_files.max(1).min(candidates.len());
        let more = candidates.len() > limit;
        let selected: Vec<u64> = candidates.into_iter().take(limit).collect();
        let mut out = ReclaimProgress {
            more,
            ..ReclaimProgress::default()
        };

        for id in selected {
            out.examined += 1;
            self.reclaim_cursor = id.wrapping_add(1).max(FIRST_FILE_ID);
            let (live, dead_messages) = {
                let Some(view) = self.sealed.get(&id) else {
                    continue;
                };
                let mut live = 0usize;
                let mut dead_messages = 0usize;
                for record in view.records() {
                    // Entry records have count=0. They are cheap recovery
                    // metadata, not a reason to rewrite a 64 MiB file whose
                    // messages are all live. They disappear when a genuinely
                    // dead message causes compaction or the whole file dies.
                    if record.count == 0 {
                        continue;
                    }
                    if record_is_dead(&record, txns_starts) {
                        dead_messages += 1;
                    } else {
                        live += 1;
                    }
                }
                (live, dead_messages)
            };

            if live == 0 {
                out.changed += self.unlink_dead_files(|meta| meta.id == id)?;
            } else if dead_messages > 0 {
                self.compact_file_below_txns(id, txns_starts)?;
                out.changed += 1;
            }
            if self.files.iter().any(|meta| meta.id == id) {
                self.reclaim_checked.insert(id, generation);
            } else {
                self.reclaim_checked.remove(&id);
            }
        }
        let live_ids: std::collections::BTreeSet<u64> =
            self.files.iter().map(|meta| meta.id).collect();
        self.reclaim_checked.retain(|id, _| live_ids.contains(id));
        Ok(out)
    }

    /// Crash-safe copy-forward compaction of one sealed, durable file. The
    /// qlog replacement and its index are atomic renames. If a crash lands
    /// between them, open detects the length mismatch and rebuilds the index by
    /// scanning the checksum-protected qlog.
    fn compact_file_below_txns(
        &mut self,
        id: u64,
        txns_starts: &std::collections::HashMap<u64, u64>,
    ) -> io::Result<()> {
        let src = file_path(&self.dir, id);
        let tmp = compact_path(&self.dir, id);
        remove_if_present(&tmp)?;

        let mut header = [0u8; FILE_HEADER_LEN as usize];
        File::open(&src)?.read_exact(&mut header)?;
        let mut out = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&tmp)?;
        out.write_all(&header)?;
        let mut at = FILE_HEADER_LEN;
        let mut records = Vec::new();
        let upto = std::fs::metadata(&src)?.len();
        let (_valid, torn) = scan_records(&src, upto, |header, _old_at, bytes| {
            if header.kind() != record::REC_ENTRY {
                let idx = index::Record::of_header(header, at);
                if !record_is_dead(&idx, txns_starts) {
                    out.write_all(bytes)?;
                    records.push(idx);
                    at = at.saturating_add(bytes.len() as u64);
                }
            }
            Ok(())
        })?;
        if let Some((pos, why)) = torn {
            return Err(corrupt(
                &src,
                &format!("damaged record at byte {pos} of compacted sealed file: {why}"),
            ));
        }
        fsync_file(&out, self.opts.fsync)?;
        drop(out);

        index::sort_records(&mut records);
        // Publish the new index first. While this method owns the queue's write
        // lock, readers still use the old mmap. A crash here leaves a length
        // mismatch and open rebuilds the old file's index.
        write_qidx(&self.dir, id, at, &records)?;
        let view = index::View::open(&qidx_path(&self.dir, id), Some(at))?;
        crate::rsm::faults::hit("compaction.copied");
        std::fs::rename(&tmp, &src)?;

        self.cache.forget_file(id);
        self.cache.clear_hashes();
        self.sealed.insert(id, view);
        if let Some(meta) = self.files.iter_mut().find(|meta| meta.id == id) {
            meta.bytes = at;
            meta.records = records.len() as u64;
            meta.min_created_at_us = i64::MAX;
            meta.max_created_at_us = i64::MIN;
            for record in &records {
                meta.absorb(record.created_at_us);
            }
        }
        crate::rsm::faults::hit("compaction.loc_committed");
        sync_dir(&self.dir)?;
        tracing::debug!(
            target: "rsm",
            queue = self.queue_id,
            file = id,
            bytes = at,
            records = records.len(),
            "rsm qlog sealed file compacted",
        );
        Ok(())
    }
}

fn record_is_dead(
    record: &index::Record,
    txns_starts: &std::collections::HashMap<u64, u64>,
) -> bool {
    txns_starts
        .get(&record.pid)
        .map_or(true, |start| record.end <= *start)
}

impl FileMeta {
    fn empty(id: u64, first_seq: u64) -> FileMeta {
        FileMeta {
            id,
            first_seq,
            bytes: FILE_HEADER_LEN,
            records: 0,
            sealed: false,
            min_created_at_us: i64::MAX,
            max_created_at_us: i64::MIN,
            max_seq: 0,
        }
    }

    fn absorb(&mut self, created_at_us: i64) {
        self.min_created_at_us = self.min_created_at_us.min(created_at_us);
        self.max_created_at_us = self.max_created_at_us.max(created_at_us);
    }
}

fn scan_entry(file_id: u64, r: &index::Record) -> ScanEntry {
    ScanEntry {
        pid: r.pid,
        base_offset: r.base_offset,
        end: r.end,
        count: r.count,
        created_at_us: r.created_at_us,
        file_id,
        offset: r.offset,
        len: r.len,
    }
}

/// The owned form of a verified record. THE decode point for message payloads:
/// a [`record::FLAG_PAYLOAD_ZSTD`] payload is decompressed here, so every
/// reader sees the raw frames.
fn owned_from(rr: &record::RecordRef<'_>) -> io::Result<OwnedRecord> {
    let payload = if rr.header.payload_zstd() {
        codec::decompress(rr.payload)?
    } else {
        rr.payload.to_vec()
    };
    Ok(OwnedRecord {
        seq: rr.header.seq,
        pid: rr.header.pid,
        base_offset: rr.header.base_offset,
        count: rr.header.count,
        created_at_us: rr.header.created_at_us,
        txn: rr.txn.map(|t| OwnedTxn {
            gtid: t.gtid,
            participants: t.participants(),
        }),
        hashes: rr.hashes.to_vec(),
        payload,
    })
}

/// Band frames in the [`CommittedFrame`] shape (hash blocks copied out of the
/// cache's `Arc`; a shape-only frame's block is empty and copies nothing).
fn committed_of(frames: Vec<BandFrame>) -> Vec<CommittedFrame> {
    frames
        .into_iter()
        .map(|f| CommittedFrame {
            base_offset: f.base_offset,
            end: f.end,
            created_at_us: f.created_at_us,
            hashes: f.hashes.to_vec(),
        })
        .collect()
}

/// Sort and coalesce overlapping / adjacent inclusive bands (the front's
/// generation bands overlap where a generation boundary fell mid-append), so the
/// overlap walk visits each record once per coalesced band. Empty bands
/// (`lo > hi`, a generation with no insert) are dropped.
fn merge_bands(bands: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let mut v: Vec<(u64, u64)> = bands.iter().copied().filter(|(lo, hi)| lo <= hi).collect();
    v.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(v.len());
    for (lo, hi) in v {
        match out.last_mut() {
            Some(last) if lo <= last.1.saturating_add(1) => last.1 = last.1.max(hi),
            _ => out.push((lo, hi)),
        }
    }
    out
}

/// The whole record at a located position in an open file, checksum-verified,
/// with the `len`-match guard (a position one byte too long would
/// checksum-verify and leak the head of the next record — the raft class's
/// `LenMismatch` guard).
fn read_located_in(f: &File, dir: &Path, loc: &Located) -> io::Result<OwnedRecord> {
    if (loc.len as usize) < record::FIXED_PREFIX {
        return Err(corrupt(
            &file_path(dir, loc.file_id),
            "located len is shorter than a record header",
        ));
    }
    let mut buf = vec![0u8; loc.len as usize];
    f.read_exact_at(&mut buf, loc.offset)?;
    let rr = record::decode(&buf).map_err(io::Error::from)?;
    if rr.header.record_len() != loc.len as usize {
        return Err(corrupt(
            &file_path(dir, loc.file_id),
            &format!(
                "located len {} != record len {}",
                loc.len,
                rr.header.record_len()
            ),
        ));
    }
    owned_from(&rr)
}

/// PLAN_RAFT_DRAIN_FIX P3.2: the hash block of one located record from ONLY its
/// header + hash block — one `pread` of [`record::hashes_prefix_len`] bytes, the
/// payload is never read. The record checksum covers the payload and so cannot
/// be checked here; instead the header is cross-checked against the index entry
/// that located it (`len`, `pid`, `base_offset`, `count`, `created_at`) — the
/// position guard the full read's `len`-match gives. The records this serves
/// are committed: fsync'd, and either scanned + verified at open or written by
/// this process. A txn-envelope record (Phase B) falls back to the full,
/// checksum-verified read.
fn pread_hashes(
    f: &File,
    dir: &Path,
    file_id: u64,
    rec: &index::Record,
    scratch: &mut Vec<u8>,
) -> io::Result<Arc<[u8]>> {
    let want = record::hashes_prefix_len(rec.count);
    if (rec.len as usize) < want {
        return Err(corrupt(
            &file_path(dir, file_id),
            "located len is shorter than its header + hashes",
        ));
    }
    scratch.clear();
    scratch.resize(want, 0);
    f.read_exact_at(&mut scratch[..], rec.offset)?;
    match record::hashes_from_prefix(&scratch[..]).map_err(io::Error::from)? {
        Some((h, hashes)) => {
            if h.record_len() != rec.len as usize
                || h.pid != rec.pid
                || h.base_offset != rec.base_offset
                || h.count != rec.count
                || h.created_at_us != rec.created_at_us
            {
                return Err(corrupt(
                    &file_path(dir, file_id),
                    &format!(
                        "record header at byte {} disagrees with its index entry",
                        rec.offset
                    ),
                ));
            }
            Ok(Arc::from(hashes))
        }
        None => {
            let loc = Located {
                file_id,
                offset: rec.offset,
                len: rec.len,
                count: rec.count,
            };
            Ok(Arc::from(read_located_in(f, dir, &loc)?.hashes))
        }
    }
}

// ---------------------------------------------------------------------------
// Read caches (PLAN_RAFT_DRAIN_FIX P3.2 / P3.3)
// ---------------------------------------------------------------------------

/// Default process-wide cap on cached read fds (`QUEEN_RAFT_QLOG_FD_CACHE`; 0
/// disables the fd cache).
const FD_CACHE_DEFAULT: usize = 256;

/// Default process-wide budget for cached hash blocks, in MiB
/// (`QUEEN_RAFT_QLOG_HASH_CACHE_MB`; 0 disables the hash cache).
const HASH_CACHE_DEFAULT_MB: usize = 64;

/// What one cached hash block is charged beyond its bytes (key, fat pointer,
/// table slot, `Arc` header) — rough, so the budget tracks RAM.
const HASH_CACHE_ENTRY_OVERHEAD: usize = 64;

/// Read fds cached across every queue log.
static FDS_CACHED: AtomicUsize = AtomicUsize::new(0);

/// Hash-block bytes (with overhead) cached across every queue log.
static HASH_BYTES_CACHED: AtomicUsize = AtomicUsize::new(0);

/// Read-side knobs: they bound fds and RAM, never a byte on disk (I2 is the
/// writer's, and the writer still reads no environment). Read once.
#[allow(clippy::disallowed_methods)]
fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
}

fn fd_cache_cap() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| env_usize("QUEEN_RAFT_QLOG_FD_CACHE").unwrap_or(FD_CACHE_DEFAULT))
}

fn hash_cache_cap() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| {
        env_usize("QUEEN_RAFT_QLOG_HASH_CACHE_MB")
            .unwrap_or(HASH_CACHE_DEFAULT_MB)
            .saturating_mul(1 << 20)
    })
}

/// How a read may spend the hash-cache budget (P3.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CachePut {
    /// Insert; when the budget is full, clear THIS log's blocks first (crude
    /// clear-when-full). The band read — the hot path the cache is for.
    Evict,
    /// Insert only while there is room: the whole-window and claim reads, which
    /// must not flush the band working set.
    IfRoom,
}

/// PLAN_RAFT_DRAIN_FIX P3.2/P3.3: the read-side state one queue log shares with
/// its readers — open read fds by file id, and the hash blocks of COMMITTED
/// records by `(pid, base_offset)`. A committed record is immutable (a crash
/// replay re-appends identical bytes), so a cached block never goes stale.
/// Behind an `Arc` so a read can finish its `pread`s after the queue's `RwLock`
/// read guard is dropped. Both caches are bounded PROCESS-WIDE (fds by count,
/// hash blocks by bytes), so N queues never multiply the budget; a log that
/// finds the hash budget full clears its own blocks (crude: a cold log's residue
/// is reclaimed only on its own next insert, a retention unlink, or its drop).
struct ReadCache {
    dir: PathBuf,
    fds: Mutex<HashMap<u64, Arc<File>>>,
    hashes: Mutex<HashCache>,
}

#[derive(Default)]
struct HashCache {
    map: HashMap<(u64, u64), Arc<[u8]>>,
    /// Charged bytes (blocks + per-entry overhead), mirrored in
    /// [`HASH_BYTES_CACHED`].
    bytes: usize,
}

impl ReadCache {
    fn new(dir: PathBuf) -> ReadCache {
        ReadCache {
            dir,
            fds: Mutex::new(HashMap::new()),
            hashes: Mutex::new(HashCache::default()),
        }
    }

    /// A read fd for `file_id`: cached, or opened and cached while the
    /// process-wide cap allows. At the cap, this log's oldest cached file yields
    /// its slot to a newer one; otherwise the fd serves the caller alone. The
    /// open runs outside the lock (a concurrent reader may win the insert).
    fn file(&self, file_id: u64) -> io::Result<Arc<File>> {
        if let Some(f) = self
            .fds
            .lock()
            .expect("qlog fd cache poisoned")
            .get(&file_id)
        {
            return Ok(f.clone());
        }
        let f = Arc::new(File::open(file_path(&self.dir, file_id))?);
        let mut fds = self.fds.lock().expect("qlog fd cache poisoned");
        if let Some(won) = fds.get(&file_id) {
            return Ok(won.clone());
        }
        if FDS_CACHED.load(Ordering::Relaxed) >= fd_cache_cap() {
            match fds.keys().min().copied() {
                // The slot passes to `file_id`: the process-wide count is unchanged.
                Some(oldest) if oldest < file_id => {
                    fds.remove(&oldest);
                }
                _ => return Ok(f),
            }
        } else {
            FDS_CACHED.fetch_add(1, Ordering::Relaxed);
        }
        fds.insert(file_id, f.clone());
        Ok(f)
    }

    /// Forget the fd of a file retention unlinked.
    fn forget_file(&self, file_id: u64) {
        if self
            .fds
            .lock()
            .expect("qlog fd cache poisoned")
            .remove(&file_id)
            .is_some()
        {
            FDS_CACHED.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn hash_get(&self, pid: u64, base_offset: u64) -> Option<Arc<[u8]>> {
        self.hashes
            .lock()
            .expect("qlog hash cache poisoned")
            .map
            .get(&(pid, base_offset))
            .cloned()
    }

    /// Cache freshly read COMMITTED hash blocks `(pid, base_offset, block)`, one
    /// lock for the batch, spending the budget as `put` allows.
    fn hash_put_many(&self, blocks: &[(u64, u64, Arc<[u8]>)], put: CachePut) {
        if blocks.is_empty() {
            return;
        }
        let cap = hash_cache_cap();
        // Declared before the guard so a cleared cache is freed after the unlock.
        let mut evicted: Vec<HashCache> = Vec::new();
        let mut hc = self.hashes.lock().expect("qlog hash cache poisoned");
        for (pid, base, h) in blocks {
            let add = h.len() + HASH_CACHE_ENTRY_OVERHEAD;
            if add > cap || hc.map.contains_key(&(*pid, *base)) {
                continue;
            }
            if HASH_BYTES_CACHED.load(Ordering::Relaxed) + add > cap {
                if put == CachePut::IfRoom || hc.bytes == 0 {
                    continue;
                }
                // Clear-when-full: a committed block re-reads cheaply (hashes only).
                let old = std::mem::take(&mut *hc);
                HASH_BYTES_CACHED.fetch_sub(old.bytes, Ordering::Relaxed);
                evicted.push(old);
                if HASH_BYTES_CACHED.load(Ordering::Relaxed) + add > cap {
                    continue; // the budget is held by other logs
                }
            }
            hc.map.insert((*pid, *base), h.clone());
            hc.bytes += add;
            HASH_BYTES_CACHED.fetch_add(add, Ordering::Relaxed);
        }
        drop(hc);
        drop(evicted);
    }

    /// Free every cached hash block of this log (retention).
    fn clear_hashes(&self) {
        let old = std::mem::take(&mut *self.hashes.lock().expect("qlog hash cache poisoned"));
        HASH_BYTES_CACHED.fetch_sub(old.bytes, Ordering::Relaxed);
    }

    /// Hash blocks this log holds (tests).
    #[cfg(test)]
    fn cached_blocks(&self) -> usize {
        self.hashes
            .lock()
            .expect("qlog hash cache poisoned")
            .map
            .len()
    }
}

impl Drop for ReadCache {
    fn drop(&mut self) {
        let fds = self.fds.get_mut().unwrap_or_else(|p| p.into_inner()).len();
        FDS_CACHED.fetch_sub(fds, Ordering::Relaxed);
        let bytes = self
            .hashes
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .bytes;
        HASH_BYTES_CACHED.fetch_sub(bytes, Ordering::Relaxed);
    }
}

/// One candidate's hash block, as [`QLog::hash_plan`] resolved it.
enum Slot {
    /// No block wanted (a shape-only read) or none exists (`count == 0`).
    Shape,
    /// Served from the cache (P3.3).
    Hit(Arc<[u8]>),
    /// To `pread` from file `.0` through the fd resolved under the read guard.
    Miss(u64, Option<Arc<File>>),
}

/// PLAN_RAFT_DRAIN_FIX P3: a committed-hashes read split in two. The PLAN (the
/// index walk, the cache probes, an fd per miss) is built under the queue's read
/// guard; [`HashPlan::finish`] runs the misses' hashes-only `pread`s, which may
/// happen AFTER the guard is dropped — the records are committed (immutable) and
/// each miss holds its own fd — so a cold read never holds the applier's append
/// behind its I/O.
struct HashPlan {
    cache: Arc<ReadCache>,
    put: CachePut,
    /// Ascending by base offset (the candidate map's order).
    items: Vec<(index::Record, Slot)>,
}

impl HashPlan {
    fn finish(self) -> io::Result<Vec<BandFrame>> {
        let empty: Arc<[u8]> = Arc::from(&[][..]);
        let mut scratch: Vec<u8> = Vec::new();
        let mut fresh: Vec<(u64, u64, Arc<[u8]>)> = Vec::new();
        let mut out: Vec<BandFrame> = Vec::with_capacity(self.items.len());
        for (rec, slot) in self.items {
            let hashes = match slot {
                Slot::Shape => empty.clone(),
                Slot::Hit(h) => h,
                Slot::Miss(file_id, fd) => {
                    let f = fd.expect("hash_plan resolves every miss's fd");
                    let h = pread_hashes(&f, &self.cache.dir, file_id, &rec, &mut scratch)?;
                    fresh.push((rec.pid, rec.base_offset, h.clone()));
                    h
                }
            };
            out.push(BandFrame {
                base_offset: rec.base_offset,
                end: rec.end,
                created_at_us: rec.created_at_us,
                hashes,
            });
        }
        self.cache.hash_put_many(&fresh, self.put);
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Scanning a file (torn-tail detection, index rebuild)
// ---------------------------------------------------------------------------

/// The outcome of scanning one `.qlog` file's records.
struct ScanOut {
    /// The verified MESSAGE records, in file order. Entry records (Phase C,
    /// [`record::REC_ENTRY`]) are verified and counted in `max_seq` but never
    /// indexed: they hold no partition's messages.
    records: Vec<index::Record>,
    /// The absolute byte offset just past the last verified record — what a
    /// torn active file is truncated to.
    valid_bytes: u64,
    /// The first record that did not verify, if any: `(byte offset, why)`. For
    /// the active file this is the torn tail; for a sealed file it is
    /// corruption.
    torn: Option<(u64, record::RecordError)>,
    /// The highest `seq` (the leader's order stamp) among the verified records,
    /// or `0` when none verified. The index (`.qidx`) is keyed by `(pid,
    /// base_offset)` and does not carry `seq`, so it is captured here from the
    /// record headers this scan already parses — it is what recovery uses to
    /// report the queue's durable tail (A3a).
    max_seq: u64,
}

/// Scan a file's records from its header to `upto`, verifying each. The engine
/// behind both "rebuild a `.qidx`" and "is this tail torn". Stops at the first
/// record that is short, has an implausible header, would overrun `upto`, or
/// fails its checksum — everything after the last verified record is the torn
/// tail (or corruption).
fn scan_file(path: &Path, upto: u64) -> io::Result<ScanOut> {
    let mut records: Vec<index::Record> = Vec::new();
    let mut max_seq = 0u64;
    let (valid_bytes, torn) = scan_records(path, upto, |header, pos, _bytes| {
        max_seq = max_seq.max(header.seq);
        if header.kind() != record::REC_ENTRY {
            records.push(index::Record::of_header(header, pos));
        }
        Ok(())
    })?;
    Ok(ScanOut {
        records,
        valid_bytes,
        torn,
        max_seq,
    })
}

/// The scan engine: walk a file's records from its header to `upto`, verify
/// each, and hand every VERIFIED record to `visit(header, byte_offset,
/// record_bytes)`. Stops at the first record that is short, has an implausible
/// header, would overrun `upto`, or fails its checksum, and returns `(the byte
/// offset just past the last verified record, that first failure if any)`.
/// Everything after the last verified record is the torn tail (or, in a sealed
/// file, corruption). An error from `visit` aborts the scan.
fn scan_records(
    path: &Path,
    upto: u64,
    mut visit: impl FnMut(&record::Header, u64, &[u8]) -> io::Result<()>,
) -> io::Result<(u64, Option<(u64, record::RecordError)>)> {
    let f = File::open(path)?;
    let mut r = BufReader::with_capacity(1 << 16, f);
    r.seek(SeekFrom::Start(FILE_HEADER_LEN))?;
    let mut valid_bytes = FILE_HEADER_LEN;
    let mut torn: Option<(u64, record::RecordError)> = None;
    let mut prefix = [0u8; record::FIXED_PREFIX];
    let mut body: Vec<u8> = Vec::new();
    let mut pos = FILE_HEADER_LEN;
    while pos < upto {
        match read_exact_or_less(&mut r, &mut prefix)? {
            0 => break, // clean EOF at a record boundary
            n if n < record::FIXED_PREFIX => {
                torn = Some((
                    pos,
                    record::RecordError::Truncated {
                        need: record::FIXED_PREFIX,
                        have: n,
                    },
                ));
                break;
            }
            _ => {}
        }
        let header = match record::parse_header(&prefix) {
            Ok(h) => h,
            Err(e) => {
                torn = Some((pos, e));
                break;
            }
        };
        let total = header.record_len();
        if pos + total as u64 > upto {
            torn = Some((
                pos,
                record::RecordError::Truncated {
                    need: total,
                    have: (upto - pos) as usize,
                },
            ));
            break;
        }
        // Grow the buffer, never re-zero it: `resize` on a cleared Vec would
        // write `total` zero bytes per record, a second full pass over the data.
        if body.len() < total {
            body.resize(total, 0);
        }
        let body = &mut body[..total];
        body[..record::FIXED_PREFIX].copy_from_slice(&prefix);
        let rest = total - record::FIXED_PREFIX;
        if read_exact_or_less(&mut r, &mut body[record::FIXED_PREFIX..])? < rest {
            torn = Some((
                pos,
                record::RecordError::Truncated {
                    need: total,
                    have: record::FIXED_PREFIX,
                },
            ));
            break;
        }
        if let Err(e) = record::verify(body, &header) {
            torn = Some((pos, e));
            break;
        }
        visit(&header, pos, body)?;
        pos += total as u64;
        valid_bytes = pos;
    }
    Ok((valid_bytes, torn))
}

// ---------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------

fn queue_dir(root: &Path, queue_id: u64) -> PathBuf {
    root.join(format!("q{queue_id}"))
}

fn file_name(id: u64) -> String {
    format!("r{id:08}.qlog")
}

fn file_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(file_name(id))
}

fn qidx_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("r{id:08}.qidx"))
}

fn qidx_tmp_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("r{id:08}.qidx.tmp"))
}

fn compact_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("r{id:08}.qlog.compact.tmp"))
}

/// Every `rNNNNNNNN.qlog` id in `dir`.
fn scan_ids(dir: &Path) -> io::Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(rest) = name.strip_prefix('r') {
            if let Some(num) = rest.strip_suffix(".qlog") {
                if let Ok(id) = num.parse::<u64>() {
                    ids.push(id);
                }
            }
        }
    }
    Ok(ids)
}

/// Read and validate a file's 32-byte header, returning its `first_seq`.
fn read_file_header(path: &Path, id: u64) -> io::Result<u64> {
    let mut f = File::open(path)?;
    let mut h = [0u8; FILE_HEADER_LEN as usize];
    f.read_exact(&mut h)?;
    if h[0..8] != FILE_MAGIC {
        return Err(corrupt(path, "bad magic"));
    }
    let hdr_id = u64::from_le_bytes(h[8..16].try_into().expect("8 bytes"));
    if hdr_id != id {
        return Err(corrupt(path, "file id in header does not match its name"));
    }
    Ok(u64::from_le_bytes(h[16..24].try_into().expect("8 bytes")))
}

/// Write a sealed file's `.qidx` atomically (temp + rename + directory fsync).
/// `records` must be in [`index::sort_records`] order.
fn write_qidx(dir: &Path, id: u64, file_bytes: u64, records: &[index::Record]) -> io::Result<()> {
    let bytes = index::encode(id, file_bytes, records);
    let tmp = qidx_tmp_path(dir, id);
    let dst = qidx_path(dir, id);
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &dst)?;
    sync_dir(dir)?;
    Ok(())
}

fn remove_if_present(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Fill `buf`, or report EOF/short read as the number of bytes read (a torn
/// tail) rather than an error.
fn read_exact_or_less<R: Read>(rdr: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    let mut read = 0;
    while read < buf.len() {
        match rdr.read(&mut buf[read..]) {
            Ok(0) => break,
            Ok(n) => read += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(read)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    // A directory fsync makes a create/unlink/rename durable. Best-effort on
    // platforms that refuse to open a directory for this.
    match File::open(dir) {
        Ok(f) => f.sync_all(),
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => Ok(()),
        Err(e) => Err(e),
    }
}

fn corrupt(path: &Path, what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("rsm qlog corrupt ({}): {what}", path.display()),
    )
}

// ---------------------------------------------------------------------------
// Fsync: F_FULLFSYNC on macOS, fdatasync elsewhere
// ---------------------------------------------------------------------------

pub(crate) fn fsync_file(f: &File, mode: Fsync) -> io::Result<()> {
    match mode {
        Fsync::Off => Ok(()),
        Fsync::Data => f.sync_data(),
        Fsync::Full => full_fsync(f),
    }
}

#[cfg(target_os = "macos")]
fn full_fsync(f: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    // F_FULLFSYNC = 51. sync_all() on macOS is fsync(2), which does NOT flush
    // the drive's own cache; the durable barrier is F_FULLFSYNC.
    let rc = unsafe { libc_fcntl(f.as_raw_fd(), 51) };
    if rc == -1 {
        return f.sync_all();
    }
    Ok(())
}

#[cfg(target_os = "macos")]
extern "C" {
    #[link_name = "fcntl"]
    fn libc_fcntl(fd: i32, cmd: i32, ...) -> i32;
}

#[cfg(not(target_os = "macos"))]
fn full_fsync(f: &File) -> io::Result<()> {
    // fdatasync: still commits what reading the data back needs (size, block
    // allocation) but not mtime/ctime — with a preallocated run, a sync that
    // only overwrites existing blocks commits no journal transaction.
    f.sync_data()
}

/// Write zeros over `[from, to)` with positional writes (the file cursor is
/// untouched).
fn zero_fill(f: &File, from: u64, to: u64) -> io::Result<()> {
    static ZEROS: [u8; 64 * 1024] = [0u8; 64 * 1024];
    let mut at = from;
    while at < to {
        let n = ((to - at) as usize).min(ZEROS.len());
        f.write_all_at(&ZEROS[..n], at)?;
        at += n as u64;
    }
    Ok(())
}
