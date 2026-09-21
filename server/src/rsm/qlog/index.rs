//! The per-queue-log sparse index (`ALICE_PGLESS_NEWARCH.md` §3.2) — one
//! `.qidx` file beside each SEALED `.qlog` file, and the ACTIVE file's index in
//! RAM. Modelled on `segments/index.rs`, which does the same for the raft-class
//! segment files.
//!
//! A queue log holds many partitions interleaved in append order; the index
//! turns "where is `(pid, offset)`?" into a binary search instead of a scan. It
//! maps
//!
//! ```text
//! (pid, base_offset) -> (byte_offset, len, end = base + count, created_at, count)
//! ```
//!
//! Within one partition, offsets are gapless and strictly increasing (a
//! rolled-back cross-queue txn will leave a gap — Phase B — but no record ever
//! overlaps another), so `(pid, base_offset)` is unique in a file and sorts the
//! same way the file is written. The active file's map lives in RAM
//! ([`ActiveIndex`]); a sealed file's map is an immutable `.qidx`
//! ([`View`]), written once at the roll and never touched again, and
//! rebuildable by scanning the `.qlog` because each record's header carries its
//! own `(pid, base_offset, count, created_at)` (§5).
//!
//! # The file
//!
//! ```text
//! magic[8] = "QQIDX1\0\0"
//! file_id:u64
//! count:u32 | record_len:u32
//! file_bytes:u64        the sealed length of the .qlog this indexes
//! records_xxh3:u64      over the record array
//! header_xxh3:u64       over the 40 bytes before it
//! then `count` records of `record_len` bytes, sorted by (pid, base_offset):
//! pid:u64 | base_offset:u64 | end:u64 | created_at:i64 | offset:u64 | count:u32 | len:u32
//! ```
//!
//! `record_len` is in the header so a later format can grow the record and an
//! older binary can refuse it honestly instead of reading garbage.
//! `file_bytes` is the tie to the log file: a `.qidx` whose `file_bytes`
//! disagrees with the length the store recorded for that `.qlog` is stale and
//! is rebuilt by scanning (§5).
//!
//! `offset` and `len` are NODE-LOCAL positions: they live only here, in a file
//! that is never replicated, exactly as in the raft-class `.qidx`.

use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::path::Path;

use memmap2::Mmap;
use xxhash_rust::xxh3::xxh3_64;

use super::record;

/// `QQIDX1\0\0`. The version lives in the magic: a format change gets `QQIDX2`
/// and an explicit rebuild, never a silent reinterpretation of old bytes.
pub const MAGIC: [u8; 8] = *b"QQIDX1\0\0";

/// Bytes before the first record: `magic(8) | file_id(8) | count(4) |
/// record_len(4) | file_bytes(8) | records_xxh3(8) | header_xxh3(8)`.
pub const HEADER_LEN: usize = 8 + 8 + 4 + 4 + 8 + 8 + 8; // 48

/// Bytes of one record.
pub const RECORD_LEN: usize = 8 + 8 + 8 + 8 + 8 + 4 + 4; // 48

/// Bytes of the header the header checksum covers (everything before it).
const HEADER_CHECKED: usize = HEADER_LEN - 8;

/// One record, as the index remembers it.
///
/// `end` is `base_offset + count`, exclusive, stored rather than recomputed so
/// the binary search compares two numbers it read and never one it derived.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Record {
    pub pid: u64,
    pub base_offset: u64,
    pub end: u64,
    pub created_at_us: i64,
    /// Byte offset of the record inside its `.qlog` file.
    pub offset: u64,
    pub count: u32,
    /// Bytes of the record on disk, the `len` field included.
    pub len: u32,
}

impl Record {
    /// The record for a `.qlog` record whose header is `header`, at `offset`.
    pub fn of_header(header: &record::Header, offset: u64) -> Record {
        Record {
            pid: header.pid,
            base_offset: header.base_offset,
            end: header.end_offset(),
            created_at_us: header.created_at_us,
            offset,
            count: header.count,
            len: header.record_len() as u32,
        }
    }

    /// Does this record hold `offset` of partition `pid`?
    pub fn holds(&self, pid: u64, offset: u64) -> bool {
        self.pid == pid && self.base_offset <= offset && offset < self.end
    }

    /// The sort key: `(pid, base_offset)`. Unique in a file.
    pub fn key(&self) -> (u64, u64) {
        (self.pid, self.base_offset)
    }

    fn write_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.pid.to_le_bytes());
        out.extend_from_slice(&self.base_offset.to_le_bytes());
        out.extend_from_slice(&self.end.to_le_bytes());
        out.extend_from_slice(&self.created_at_us.to_le_bytes());
        out.extend_from_slice(&self.offset.to_le_bytes());
        out.extend_from_slice(&self.count.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
    }

    fn read_from(b: &[u8]) -> Record {
        debug_assert_eq!(b.len(), RECORD_LEN);
        Record {
            pid: u64::from_le_bytes(b[0..8].try_into().expect("8 bytes")),
            base_offset: u64::from_le_bytes(b[8..16].try_into().expect("8 bytes")),
            end: u64::from_le_bytes(b[16..24].try_into().expect("8 bytes")),
            created_at_us: i64::from_le_bytes(b[24..32].try_into().expect("8 bytes")),
            offset: u64::from_le_bytes(b[32..40].try_into().expect("8 bytes")),
            count: u32::from_le_bytes(b[40..44].try_into().expect("4 bytes")),
            len: u32::from_le_bytes(b[44..48].try_into().expect("4 bytes")),
        }
    }
}

/// What a `.qidx` can be wrong about.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexError {
    /// Not a `.qidx` at all, or one this build does not know.
    Magic,
    /// The header checksum does not match: the file is damaged.
    HeaderChecksum,
    /// The record array's checksum does not match.
    RecordChecksum,
    /// A record width this build does not know (a newer `.qidx`).
    RecordLen(u32),
    /// The file is shorter than its own header says.
    Short { need: usize, have: usize },
    /// The header names another file: a wrong directory, or a `.qidx` copied
    /// rather than rebuilt.
    Mismatch { file_id: u64 },
    /// The index was written for a `.qlog` of a different length, so the store
    /// and this index disagree about what the file holds (§5).
    StaleLength { indexed: u64, recorded: u64 },
}

impl std::fmt::Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexError::Magic => write!(f, "not a qlog qidx file"),
            IndexError::HeaderChecksum => write!(f, "qidx header checksum mismatch"),
            IndexError::RecordChecksum => write!(f, "qidx record checksum mismatch"),
            IndexError::RecordLen(n) => write!(f, "qidx record width {n} is not known"),
            IndexError::Short { need, have } => {
                write!(f, "qidx needs {need} bytes, file holds {have}")
            }
            IndexError::Mismatch { file_id } => write!(f, "qidx names file {file_id}"),
            IndexError::StaleLength { indexed, recorded } => write!(
                f,
                "qidx indexes {indexed} bytes, the store recorded {recorded}"
            ),
        }
    }
}

impl std::error::Error for IndexError {}

impl From<IndexError> for io::Error {
    fn from(e: IndexError) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, e.to_string())
    }
}

/// Sort records into the order [`encode`] writes them: `(pid, base_offset)`.
/// A roll gets that order for free — [`ActiveIndex`] is keyed by it — so only a
/// rebuild-from-scan, which collects records in FILE order, calls this.
pub fn sort_records(records: &mut [Record]) {
    records.sort_unstable_by_key(|r| r.key());
}

/// Serialize an index. `records` must already be in [`sort_records`] order.
pub fn encode(file_id: u64, file_bytes: u64, records: &[Record]) -> Vec<u8> {
    debug_assert!(
        records.is_sorted_by_key(|r| r.key()),
        "qidx records must be sorted by (pid, base_offset) before they are encoded",
    );
    let mut body = Vec::with_capacity(records.len() * RECORD_LEN);
    for r in records {
        r.write_into(&mut body);
    }
    let mut out = Vec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&file_id.to_le_bytes());
    out.extend_from_slice(&(records.len() as u32).to_le_bytes());
    out.extend_from_slice(&(RECORD_LEN as u32).to_le_bytes());
    out.extend_from_slice(&file_bytes.to_le_bytes());
    out.extend_from_slice(&xxh3_64(&body).to_le_bytes());
    let sum = xxh3_64(&out[..HEADER_CHECKED]);
    out.extend_from_slice(&sum.to_le_bytes());
    debug_assert_eq!(out.len(), HEADER_LEN);
    out.extend_from_slice(&body);
    out
}

/// Where a `(pid, offset)` sits relative to one file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Probe {
    /// The record is here.
    Hit(Record),
    /// This file holds the partition, and every record of it starts ABOVE the
    /// offset asked for: look in an older file.
    Before,
    /// This file holds the partition, and every record of it ends at or below
    /// the offset asked for: look in a newer file.
    After,
    /// The offset falls inside this file's span for the partition but in a hole
    /// between two records — retention deleted what was there. No other file
    /// can hold it.
    Hole,
    /// This file holds no record of that partition at all.
    Missing,
}

/// A `.qidx` mapped into memory and binary-searched in place.
///
/// The map is read-only and the file is immutable, so a view may be shared by
/// any number of readers without a lock. Opening verifies both checksums.
pub struct View {
    map: Mmap,
    file_id: u64,
    count: usize,
    file_bytes: u64,
}

impl std::fmt::Debug for View {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "qlog-qidx(f{}, {} records, {} bytes indexed)",
            self.file_id, self.count, self.file_bytes
        )
    }
}

impl View {
    /// Map `path` and verify it. `recorded_len`, when given, is the length the
    /// store recorded for the `.qlog`: an index written for a different length
    /// is stale and must be rebuilt.
    pub fn open(path: &Path, recorded_len: Option<u64>) -> io::Result<View> {
        let f = File::open(path)?;
        let len = f.metadata()?.len() as usize;
        if len < HEADER_LEN {
            return Err(IndexError::Short {
                need: HEADER_LEN,
                have: len,
            }
            .into());
        }
        // SAFETY: a sealed file's index is created by a temp-file rename and
        // never modified; unlink keeps its pages alive for this mapping on unix.
        let map = unsafe { Mmap::map(&f)? };
        let head = &map[..HEADER_LEN];
        if head[..8] != MAGIC {
            return Err(IndexError::Magic.into());
        }
        let want = u64::from_le_bytes(head[40..48].try_into().expect("8 bytes"));
        if xxh3_64(&head[..HEADER_CHECKED]) != want {
            return Err(IndexError::HeaderChecksum.into());
        }
        let file_id = u64::from_le_bytes(head[8..16].try_into().expect("8 bytes"));
        let count = u32::from_le_bytes(head[16..20].try_into().expect("4 bytes")) as usize;
        let record_len = u32::from_le_bytes(head[20..24].try_into().expect("4 bytes"));
        if record_len as usize != RECORD_LEN {
            return Err(IndexError::RecordLen(record_len).into());
        }
        let file_bytes = u64::from_le_bytes(head[24..32].try_into().expect("8 bytes"));
        let need = HEADER_LEN + count * RECORD_LEN;
        if len < need {
            return Err(IndexError::Short { need, have: len }.into());
        }
        let recs_sum = u64::from_le_bytes(head[32..40].try_into().expect("8 bytes"));
        if xxh3_64(&map[HEADER_LEN..need]) != recs_sum {
            return Err(IndexError::RecordChecksum.into());
        }
        if let Some(recorded) = recorded_len {
            if recorded != file_bytes {
                return Err(IndexError::StaleLength {
                    indexed: file_bytes,
                    recorded,
                }
                .into());
            }
        }
        Ok(View {
            map,
            file_id,
            count,
            file_bytes,
        })
    }

    /// Refuse a view whose header names another file id.
    pub fn check_identity(&self, file_id: u64) -> Result<(), IndexError> {
        if self.file_id != file_id {
            return Err(IndexError::Mismatch {
                file_id: self.file_id,
            });
        }
        Ok(())
    }

    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The sealed length of the `.qlog` this indexes.
    pub fn file_bytes(&self) -> u64 {
        self.file_bytes
    }

    pub fn record(&self, i: usize) -> Record {
        let at = HEADER_LEN + i * RECORD_LEN;
        Record::read_from(&self.map[at..at + RECORD_LEN])
    }

    pub fn records(&self) -> impl Iterator<Item = Record> + '_ {
        (0..self.count).map(move |i| self.record(i))
    }

    /// Every record of `pid`, ascending by base offset. Binary-searched span,
    /// then a copy of each record — for [`super::QLog::scan_from`].
    pub fn records_of(&self, pid: u64) -> Vec<Record> {
        let (lo, hi) = self.span_of(pid);
        (lo..hi).map(|i| self.record(i)).collect()
    }

    /// The half-open range of records belonging to `pid`.
    fn span_of(&self, pid: u64) -> (usize, usize) {
        let lo = self.partition_point(|r| r.pid < pid);
        let hi = self.partition_point(|r| r.pid <= pid);
        (lo, hi)
    }

    /// `records.partition_point(pred)` over the mapped array. The array is
    /// sorted by `(pid, base_offset)`, so every predicate used here is monotone.
    fn partition_point(&self, pred: impl Fn(&Record) -> bool) -> usize {
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if pred(&self.record(mid)) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Where `(pid, offset)` sits relative to this file. O(log n) reads of the
    /// mapping, no allocation.
    pub fn probe(&self, pid: u64, offset: u64) -> Probe {
        let (lo, hi) = self.span_of(pid);
        if lo == hi {
            return Probe::Missing;
        }
        let first = self.record(lo);
        if offset < first.base_offset {
            return Probe::Before;
        }
        let last = self.record(hi - 1);
        if offset >= last.end {
            return Probe::After;
        }
        // Somewhere in this file's span: the greatest record whose base_offset
        // is at or below the offset.
        let at = self.partition_point(|r| (r.pid, r.base_offset) <= (pid, offset));
        debug_assert!(at > lo);
        let cand = self.record(at - 1);
        if cand.holds(pid, offset) {
            Probe::Hit(cand)
        } else {
            Probe::Hole
        }
    }
}

// ---------------------------------------------------------------------------
// The active file's index, in RAM
// ---------------------------------------------------------------------------

/// The index of the ACTIVE `.qlog` file, with the id of the file it indexes.
///
/// One queue = one active file, so this is a single map (not the per-bucket
/// array the raft-class segments keep). The file id lives HERE, beside the
/// records, so [`ActiveIndex::probe`] returns the two together: a reader that
/// took a record and then asked elsewhere which file is active could be
/// answered by a file a roll had just created and read new bytes at an old
/// offset. The two facts move as one.
#[derive(Default)]
pub struct ActiveIndex {
    file_id: Option<u64>,
    recs: BTreeMap<(u64, u64), Record>,
}

impl ActiveIndex {
    pub fn new() -> ActiveIndex {
        ActiveIndex::default()
    }

    /// Point the index at a new (empty) active file.
    pub fn open(&mut self, file_id: u64) {
        self.file_id = Some(file_id);
        self.recs.clear();
    }

    /// Publish one active record's index entry.
    pub fn insert(&mut self, rec: Record) {
        self.recs.insert(rec.key(), rec);
    }

    /// Take the whole index of the active file and forget the file: what a roll
    /// does, right before writing it out as the sealed file's `.qidx`. The
    /// returned records are in `(pid, base_offset)` order (the map's order), so
    /// no [`sort_records`] is needed before [`encode`].
    pub fn take(&mut self) -> Vec<Record> {
        self.file_id = None;
        std::mem::take(&mut self.recs).into_values().collect()
    }

    /// Drop the index without producing it (recovery, before a rescan).
    pub fn clear(&mut self) {
        self.file_id = None;
        self.recs.clear();
    }

    /// The active file id, as the index knows it.
    pub fn file_id(&self) -> Option<u64> {
        self.file_id
    }

    /// The record holding `(pid, offset)` in the active file, with that file's
    /// id.
    pub fn probe(&self, pid: u64, offset: u64) -> Option<(u64, Record)> {
        let file_id = self.file_id?;
        self.recs
            .range(..=(pid, offset))
            .next_back()
            .map(|(_, r)| *r)
            .filter(|r| r.holds(pid, offset))
            .map(|r| (file_id, r))
    }

    /// The active file's records of `pid`, ascending by base offset, whose
    /// `end` is above `from_offset` — the records a forward scan of a partition
    /// from `from_offset` needs.
    pub fn records_of_from(&self, pid: u64, from_offset: u64) -> Vec<Record> {
        self.recs
            .range((pid, 0)..=(pid, u64::MAX))
            .map(|(_, r)| *r)
            .filter(|r| r.end > from_offset)
            .collect()
    }

    /// Records held by the active file.
    pub fn len(&self) -> usize {
        self.recs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.recs.is_empty()
    }
}
