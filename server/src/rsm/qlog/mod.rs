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
//! # ISOLATED — dead code until a later phase
//!
//! Nothing here is wired into apply, pop, the replicator or the facade. It
//! changes no live behaviour. A later phase (§10 Phase A) swaps the byte store
//! over to it; until then it exists only to be reviewed and crash-tested. The
//! transaction fields are in the record format ([`record`]) but transactions
//! are NOT implemented (Phase B): a `txn_kind == 1` record round-trips, and
//! nothing acts on it.
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
//! # Not this phase
//!
//! - transactions / present-in-all (Phase B) — the fields exist, the machinery
//!   does not;
//! - wiring into apply/pop/replicator/facade — dead code;
//! - compaction of partially-live files (Phase C) — [`QLog::unlink_dead_files`]
//!   drops WHOLE dead files only; see the `TODO(phase-C-compaction)` there;
//! - parallel per-queue writers (Phase E) — one writer per queue is fine;
//! - handle caching / read deadlines — a read opens the file fresh; the raft
//!   class's `Reader` cache and I15 deadlines are a later concern.

pub mod index;
pub mod record;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

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
    /// Records in the file.
    pub records: u64,
    /// A sealed file is immutable and has a `.qidx`; the active (last) file
    /// does not and is never sealed.
    pub sealed: bool,
    pub min_created_at_us: i64,
    pub max_created_at_us: i64,
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
                    rec.truncated_tail = true;
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
                rec.scanned_records += scan.records.len() as u64;
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

        rec.files = files.len();
        rec.bytes = files.iter().map(|f| f.bytes).sum();
        rec.records = files.iter().map(|f| f.records).sum();

        let qlog = QLog {
            dir,
            queue_id,
            opts,
            files,
            active,
            active_index,
            sealed,
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

    /// Append one group of records and fsync ONCE. Returns each record's
    /// `(file_id, byte_offset)` in order. The whole group is one `write_all`
    /// and one barrier — what amortizes the flush.
    pub fn append_group(&mut self, records: &[RecordInput<'_>]) -> io::Result<Vec<Loc>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        // Ensure an active file with room. A group larger than the limit still
        // goes whole into the (freshly rolled or created) file.
        if self.active.is_none() {
            self.create_active(FIRST_FILE_ID, records[0].seq)?;
        } else {
            let sz = self.active_len();
            if sz > FILE_HEADER_LEN && sz >= self.opts.segment_bytes {
                self.roll(records[0].seq)?;
            }
        }

        let file_id = self.files.last().expect("active meta").id;
        let base = self.active_len();
        let cap = records
            .iter()
            .map(|r| {
                record::encoded_len(
                    r.count,
                    r.txn.map_or(0, |t| t.participants.len()),
                    r.payload.len(),
                )
            })
            .sum();
        let mut buf: Vec<u8> = Vec::with_capacity(cap);
        let mut locs = Vec::with_capacity(records.len());
        let mut new_recs: Vec<index::Record> = Vec::with_capacity(records.len());
        for r in records {
            let start = buf.len();
            let txn = r.txn.map(|t| (t.gtid, t.participants));
            let n = record::encode_into(
                &mut buf,
                r.seq,
                r.pid,
                r.base_offset,
                r.count,
                r.created_at_us,
                txn,
                r.hashes,
                r.payload,
            )?;
            let offset = base + start as u64;
            locs.push(Loc { file_id, offset });
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

        let f = self.active.as_mut().expect("active file");
        f.write_all(&buf)?;
        fsync_file(f, self.opts.fsync)?;

        // Bookkeeping advances only after the fsync returns. Recovery rebuilds
        // the real valid length from the bytes on disk, so if a crash lands
        // before this, the un-fsync'd tail is truncated and the records are
        // simply unanswered (their propose never returned).
        let added = buf.len() as u64;
        {
            let meta = self.files.last_mut().expect("active meta");
            meta.bytes += added;
            meta.records += records.len() as u64;
            for r in &new_recs {
                meta.absorb(r.created_at_us);
            }
        }
        for r in new_recs {
            self.active_index.insert(r);
        }
        Ok(locs)
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
        self.files.push(FileMeta::empty(id, first_seq));
        self.active_index.open(id);
        Ok(())
    }

    /// Seal the active file — write its `.qidx`, map it, mark it sealed — and
    /// create the next active file whose first record will carry `first_seq`.
    fn roll(&mut self, first_seq: u64) -> io::Result<()> {
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
        let path = file_path(&self.dir, file_id);
        let f = File::open(&path)?;
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
        Ok(owned_from(&rr))
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

    /// The whole record at a located position, with the `len`-match guard.
    fn read_located(&self, loc: &Located) -> io::Result<OwnedRecord> {
        if (loc.len as usize) < record::FIXED_PREFIX {
            return Err(corrupt(
                &file_path(&self.dir, loc.file_id),
                "located len is shorter than a record header",
            ));
        }
        let path = file_path(&self.dir, loc.file_id);
        let f = File::open(&path)?;
        let mut buf = vec![0u8; loc.len as usize];
        f.read_exact_at(&mut buf, loc.offset)?;
        let rr = record::decode(&buf).map_err(io::Error::from)?;
        if rr.header.record_len() != loc.len as usize {
            return Err(corrupt(
                &path,
                &format!(
                    "located len {} != record len {}",
                    loc.len,
                    rr.header.record_len()
                ),
            ));
        }
        Ok(owned_from(&rr))
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

    /// Retention, this phase: unlink WHOLE files the predicate marks dead. Only
    /// SEALED files are candidates — the active file is never unlinked. Returns
    /// how many were dropped.
    ///
    /// TODO(phase-C-compaction): a partially-live file (a lagging backlog, a
    /// long-retention survivor) is NOT handled here. Copying its live residue
    /// forward and dropping the old file — or `fallocate` hole-punching a
    /// block-aligned dead run — is Phase C (§3.3). This phase drops only files
    /// whose every record the caller has judged dead.
    pub fn unlink_dead_files(&mut self, is_dead: impl Fn(&FileMeta) -> bool) -> io::Result<usize> {
        let active_id = self.active_index.file_id();
        let victims: Vec<u64> = self
            .files
            .iter()
            .filter(|m| m.sealed && Some(m.id) != active_id && is_dead(m))
            .map(|m| m.id)
            .collect();
        if victims.is_empty() {
            return Ok(0);
        }
        for id in &victims {
            self.sealed.remove(id);
            remove_if_present(&file_path(&self.dir, *id))?;
            remove_if_present(&qidx_path(&self.dir, *id))?;
            remove_if_present(&qidx_tmp_path(&self.dir, *id))?;
        }
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

fn owned_from(rr: &record::RecordRef<'_>) -> OwnedRecord {
    OwnedRecord {
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
        payload: rr.payload.to_vec(),
    }
}

// ---------------------------------------------------------------------------
// Scanning a file (torn-tail detection, index rebuild)
// ---------------------------------------------------------------------------

/// The outcome of scanning one `.qlog` file's records.
struct ScanOut {
    /// The verified records, in file order.
    records: Vec<index::Record>,
    /// The absolute byte offset just past the last verified record — what a
    /// torn active file is truncated to.
    valid_bytes: u64,
    /// The first record that did not verify, if any: `(byte offset, why)`. For
    /// the active file this is the torn tail; for a sealed file it is
    /// corruption.
    torn: Option<(u64, record::RecordError)>,
}

/// Scan a file's records from its header to `upto`, verifying each. The engine
/// behind both "rebuild a `.qidx`" and "is this tail torn". Stops at the first
/// record that is short, has an implausible header, would overrun `upto`, or
/// fails its checksum — everything after the last verified record is the torn
/// tail (or corruption).
fn scan_file(path: &Path, upto: u64) -> io::Result<ScanOut> {
    let f = File::open(path)?;
    let mut r = BufReader::with_capacity(1 << 16, f);
    r.seek(SeekFrom::Start(FILE_HEADER_LEN))?;
    let mut out = ScanOut {
        records: Vec::new(),
        valid_bytes: FILE_HEADER_LEN,
        torn: None,
    };
    let mut prefix = [0u8; record::FIXED_PREFIX];
    let mut body: Vec<u8> = Vec::new();
    let mut pos = FILE_HEADER_LEN;
    while pos < upto {
        match read_exact_or_less(&mut r, &mut prefix)? {
            0 => break, // clean EOF at a record boundary
            n if n < record::FIXED_PREFIX => {
                out.torn = Some((
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
                out.torn = Some((pos, e));
                break;
            }
        };
        let total = header.record_len();
        if pos + total as u64 > upto {
            out.torn = Some((
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
            out.torn = Some((
                pos,
                record::RecordError::Truncated {
                    need: total,
                    have: record::FIXED_PREFIX,
                },
            ));
            break;
        }
        if let Err(e) = record::verify(body, &header) {
            out.torn = Some((pos, e));
            break;
        }
        out.records.push(index::Record::of_header(&header, pos));
        pos += total as u64;
        out.valid_bytes = pos;
    }
    Ok(out)
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
// Fsync: F_FULLFSYNC on macOS, plain fsync elsewhere
// ---------------------------------------------------------------------------

fn fsync_file(f: &File, mode: Fsync) -> io::Result<()> {
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
    f.sync_all()
}
