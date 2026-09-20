//! The segment index: `.qidx` files beside the sealed segment files, and the
//! active file's index in RAM (PLAN_RAFT.md §6.1 amendment, §11.2).
//!
//! # Why it is not in the store
//!
//! `segments` was the highest-rate keyspace of §6.1 and the G0 amendment took
//! it OUT of the ordered store, "the way Kafka keeps `.index` files beside
//! `.log` files". One immutable `.qidx` per SEALED segment file, written once
//! at the seal and never touched again; the ACTIVE file's entries live in RAM,
//! bounded by one file's worth per bucket.
//!
//! That removes one store write per push batch — the write that S1 measured as
//! the store's dominant cost — and it costs one mmap'd binary search per read
//! of a sealed file.
//!
//! # RAM arithmetic (the bound I8 cares about)
//!
//! The RAM half is bounded by `QUEEN_RAFT_SEGMENT_BYTES` (64 MiB) times the
//! number of buckets that have an open active file, divided by the average
//! frame size, times [`RECORD_LEN`] plus the map's own overhead. A node whose
//! traffic is spread over all 256 buckets and whose frames are one small
//! message each (~300 B) would hold ~218k records per bucket; with fat batches
//! (the shape push traffic actually has) it is three orders of magnitude less.
//! The number to watch is therefore FRAMES PER ACTIVE FILE, not messages, and
//! it is reported by [`ActiveIndexes::len`] so WP-1.11's flatness run can
//! quote it.
//!
//! # The file
//!
//! ```text
//! magic[8] = "QIDX1\0\0\0"
//! bucket:u16 | reserved:u16 | file_id:u32
//! count:u32  | record_len:u32
//! file_bytes:u64        the sealed length of the .seg this indexes
//! records_xxh3:u64      over the record array
//! header_xxh3:u64       over the 40 bytes before it
//! then `count` records of `record_len` bytes, sorted by (pid, base_offset):
//! pid:u64 | base_offset:u64 | end:u64 | created_at:i64 | offset:u64 | count:u32 | len:u32
//! ```
//!
//! `record_len` is in the header so a later format can grow the record and an
//! older binary can still refuse it honestly instead of reading garbage.
//! `file_bytes` is the tie to the segment file: a `.qidx` whose `file_bytes`
//! disagrees with the length the store recorded for that file is stale and is
//! rebuilt by scanning (§11.5).
//!
//! `offset` and `len` are NODE-LOCAL positions (D8, I7). They live here, in a
//! file that is never replicated and never digested, and they are shipped only
//! with the segment file they index (§11.6 step 5).

use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::path::Path;

use memmap2::Mmap;
use xxhash_rust::xxh3::xxh3_64;

use super::frame;

/// `QIDX1\0\0\0`. The version lives in the magic: a format change gets `QIDX2`
/// and an explicit rebuild, never a silent reinterpretation of old bytes.
pub const MAGIC: [u8; 8] = *b"QIDX1\0\0\0";

/// Bytes before the first record.
pub const HEADER_LEN: usize = 48;

/// Bytes of one record.
pub const RECORD_LEN: usize = 48;

/// Bytes of the header the header checksum covers.
const HEADER_CHECKED: usize = HEADER_LEN - 8;

/// One frame, as the index remembers it.
///
/// `end` is `base_offset + count`, exclusive, stored rather than recomputed so
/// the binary search compares two numbers it read and never one it derived.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Record {
    pub pid: u64,
    pub base_offset: u64,
    pub end: u64,
    pub created_at_us: i64,
    /// Byte offset of the frame inside its segment file.
    pub offset: u64,
    pub count: u32,
    /// Bytes of the frame, `len` field included.
    pub len: u32,
}

impl Record {
    /// The record for a frame at `offset` in a file.
    pub fn of_frame(header: &frame::Header, offset: u64) -> Record {
        Record {
            pid: header.pid,
            base_offset: header.base_offset,
            end: header.end_offset(),
            created_at_us: header.created_at_us,
            offset,
            count: header.count,
            len: header.frame_len() as u32,
        }
    }

    /// Does this frame hold `offset` of partition `pid`?
    pub fn holds(&self, pid: u64, offset: u64) -> bool {
        self.pid == pid && self.base_offset <= offset && offset < self.end
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

    /// The sort key: `(pid, base_offset)`. Within one partition offsets are
    /// gapless and strictly increasing, so this key is unique in a file.
    fn key(&self) -> (u64, u64) {
        (self.pid, self.base_offset)
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
    /// The header names another bucket or file: a wrong data directory, or a
    /// `.qidx` that was copied rather than rebuilt.
    Mismatch { bucket: u16, file_id: u32 },
    /// The index was written for a segment file of a different length, so the
    /// store and this index disagree about what the file holds (§11.5).
    StaleLength { indexed: u64, recorded: u64 },
}

impl std::fmt::Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexError::Magic => write!(f, "not a qidx file"),
            IndexError::HeaderChecksum => write!(f, "qidx header checksum mismatch"),
            IndexError::RecordChecksum => write!(f, "qidx record checksum mismatch"),
            IndexError::RecordLen(n) => write!(f, "qidx record width {n} is not known"),
            IndexError::Short { need, have } => {
                write!(f, "qidx needs {need} bytes, file holds {have}")
            }
            IndexError::Mismatch { bucket, file_id } => {
                write!(f, "qidx names bucket {bucket} file {file_id}")
            }
            IndexError::StaleLength { indexed, recorded } => {
                write!(
                    f,
                    "qidx indexes {indexed} bytes, the store recorded {recorded}"
                )
            }
        }
    }
}

impl std::error::Error for IndexError {}

impl From<IndexError> for io::Error {
    fn from(e: IndexError) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, e.to_string())
    }
}

/// Sort records into the order [`encode`] writes them in: `(pid,
/// base_offset)`, what the binary search needs. A roll gets that order for
/// free — [`ActiveIndexes`] is keyed by it — so only the paths that collect
/// records in FILE order (a rebuild from a scan) call this.
pub fn sort_records(records: &mut [Record]) {
    records.sort_unstable_by_key(|r| r.key());
}
/// Serialize an index. `records` must already be in [`sort_records`] order.
///
/// It takes a SHARED slice on purpose. The roll hands the very same records to
/// the readers of `Shared::sealed_recent` before this call starts — that is
/// what keeps a just-sealed frame findable while the `.qidx` is being written
/// — so nothing here may reorder them under a reader's binary search.
pub fn encode(bucket: u16, file_id: u32, file_bytes: u64, records: &[Record]) -> Vec<u8> {
    debug_assert!(
        records.is_sorted_by_key(|r| r.key()),
        "qidx records must be sorted by (pid, base_offset) before they are encoded",
    );
    let mut body = Vec::with_capacity(records.len() * RECORD_LEN);
    for r in records.iter() {
        r.write_into(&mut body);
    }
    let mut out = Vec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&bucket.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // reserved
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

/// A `.qidx` mapped into memory and binary-searched in place.
///
/// The map is read-only and the file is immutable, so a view may be shared by
/// any number of readers without a lock. Opening verifies both checksums: the
/// header's (48 bytes) and the record array's (about 0.5% of the segment file
/// it indexes). That is the one O(index) cost, paid once per file per process,
/// never per read.
pub struct View {
    map: Mmap,
    bucket: u16,
    file_id: u32,
    count: usize,
    file_bytes: u64,
}

/// Where a `(pid, offset)` sits relative to one file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Probe {
    /// The frame is here.
    Hit(Record),
    /// This file holds the partition, and every frame of it starts ABOVE the
    /// offset asked for: look in an older file.
    Before,
    /// This file holds the partition, and every frame of it ends at or below
    /// the offset asked for: look in a newer file.
    After,
    /// The offset falls inside this file's span for the partition but in a
    /// hole between two frames — retention deleted what was there. No other
    /// file can hold it.
    Hole,
    /// This file holds no frame of that partition at all.
    Missing,
}

impl std::fmt::Debug for View {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "qidx(b{:03}/f{}, {} records, {} bytes indexed)",
            self.bucket, self.file_id, self.count, self.file_bytes
        )
    }
}

impl View {
    /// Map `path` and verify it. `recorded_len`, when given, is the length the
    /// store recorded for the segment file: an index written for a different
    /// length is stale and must be rebuilt.
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
        // SAFETY: the file is immutable once written — a sealed segment's
        // index is created by a temp-file rename and never modified, and GC
        // unlinks it only after evicting every cached view (§11.7). An
        // unlinked file keeps its pages alive for this mapping on unix.
        let map = unsafe { Mmap::map(&f)? };
        let head = &map[..HEADER_LEN];
        if head[..8] != MAGIC {
            return Err(IndexError::Magic.into());
        }
        let want = u64::from_le_bytes(head[40..48].try_into().expect("8 bytes"));
        if xxh3_64(&head[..HEADER_CHECKED]) != want {
            return Err(IndexError::HeaderChecksum.into());
        }
        let bucket = u16::from_le_bytes(head[8..10].try_into().expect("2 bytes"));
        let file_id = u32::from_le_bytes(head[12..16].try_into().expect("4 bytes"));
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
            bucket,
            file_id,
            count,
            file_bytes,
        })
    }

    /// Refuse a view whose header names another file — a wrong data directory,
    /// or a `.qidx` that travelled without its segment.
    pub fn check_identity(&self, bucket: u16, file_id: u32) -> Result<(), IndexError> {
        if self.bucket != bucket || self.file_id != file_id {
            return Err(IndexError::Mismatch {
                bucket: self.bucket,
                file_id: self.file_id,
            });
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The sealed length of the segment file this indexes.
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

    /// The half-open range of records belonging to `pid`.
    fn span_of(&self, pid: u64) -> (usize, usize) {
        let lo = self.partition_point(|r| r.pid < pid);
        let hi = self.partition_point(|r| r.pid <= pid);
        (lo, hi)
    }

    /// Every record of `pid`, ascending by base offset. For the committed
    /// segment dedup scan ([`super::Reader::committed_dedup_rows`]).
    pub fn records_of(&self, pid: u64) -> Vec<Record> {
        let (lo, hi) = self.span_of(pid);
        (lo..hi).map(|i| self.record(i)).collect()
    }

    /// `records.partition_point(pred)` over the mapped array: the number of
    /// leading records for which `pred` holds. The array is sorted by
    /// `(pid, base_offset)`, so every predicate used here is monotone.
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
        // Somewhere in this file's span: the greatest record whose
        // base_offset is at or below the offset.
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

/// The index of every bucket's ACTIVE segment file, WITH the id of the file it
/// indexes.
///
/// One structure, one lock — not the 256 per-bucket locks of pgless
/// (`native/bucket.rs`), which is one of the shapes this plan deliberately
/// does not carry over. The only writer is the apply thread (I1); readers take
/// a read lock for the length of one `probe`, never across an `.await` (I15).
///
/// The file id lives HERE, under the same lock as the records, and
/// [`ActiveIndexes::probe`] returns the two together. That is not tidiness: a
/// reader that took the record from the index and then asked elsewhere which
/// file is active could be answered by the file the roll had just created, and
/// would read the new file at the old file's offset. The two facts have to
/// move as one.
pub struct ActiveIndexes {
    /// One map per bucket, keyed by `(pid, base_offset)` so the same
    /// "greatest key at or below" search the `.qidx` does in binary works here
    /// as a range query.
    buckets: Vec<Bucket>,
}

#[derive(Default)]
struct Bucket {
    file_id: Option<u32>,
    recs: BTreeMap<(u64, u64), Record>,
    /// PERF-E `DEDUP_INDEX=segment`: the per-frame hash list (`16 * count`
    /// bytes, frame order), keyed like `recs`. Retained ONLY when the node
    /// serves dedup from the segments ([`ActiveIndexes::insert`] is handed the
    /// hashes then, `&[]` otherwise), so the default `txns`/`rows` modes pay no
    /// RAM. Bounded exactly as `recs` is — one active file's frames per bucket
    /// — and dropped with the records when the file seals ([`take`]).
    hashes: BTreeMap<(u64, u64), Vec<u8>>,
}

impl ActiveIndexes {
    pub fn new(nbuckets: usize) -> ActiveIndexes {
        ActiveIndexes {
            buckets: (0..nbuckets).map(|_| Bucket::default()).collect(),
        }
    }

    /// Point a bucket at a new (empty) active file.
    pub fn open(&mut self, bucket: u16, file_id: u32) {
        let b = &mut self.buckets[bucket as usize];
        b.file_id = Some(file_id);
        b.recs.clear();
        b.hashes.clear();
    }

    /// Publish one active frame's index record. `hashes` is the frame's
    /// `16 * count`-byte hash list when the node serves dedup from the segments
    /// (`DEDUP_INDEX=segment`), or `&[]` otherwise; a non-empty list is retained
    /// in RAM keyed like the record so [`dedup_frames_of`] can serve it without
    /// a disk read.
    pub fn insert(&mut self, bucket: u16, rec: Record, hashes: &[u8]) {
        let b = &mut self.buckets[bucket as usize];
        b.recs.insert(rec.key(), rec);
        if !hashes.is_empty() {
            b.hashes.insert(rec.key(), hashes.to_vec());
        }
    }

    /// Take the whole index of a bucket's active file and forget the file: what
    /// a roll does, right before writing it out as the sealed file's `.qidx`.
    /// The retained hashes go with it — a sealed frame's hashes are read from
    /// the `.seg` on demand, never kept in RAM.
    pub fn take(&mut self, bucket: u16) -> Vec<Record> {
        let b = &mut self.buckets[bucket as usize];
        b.file_id = None;
        b.hashes.clear();
        std::mem::take(&mut b.recs).into_values().collect()
    }

    /// Drop a bucket's index without producing it (recovery, before a rescan).
    pub fn clear(&mut self, bucket: u16) {
        let b = &mut self.buckets[bucket as usize];
        b.file_id = None;
        b.recs.clear();
        b.hashes.clear();
    }

    /// The retained hash list of one active frame `(pid, base_offset)`, if the
    /// active file holds it and its hashes were retained (`DEDUP_INDEX=segment`).
    /// `None` for a sealed frame (read from the `.seg` instead) or when hashes
    /// were not retained. For the O(claimed) delivered-set walk
    /// ([`super::Reader::claim_frames`]).
    pub fn hashes_of(&self, bucket: u16, pid: u64, base: u64) -> Option<Vec<u8>> {
        self.buckets[bucket as usize]
            .hashes
            .get(&(pid, base))
            .cloned()
    }

    /// The active file's frames of `pid`, ascending by base offset, each paired
    /// with the hash list retained at append time (`DEDUP_INDEX=segment`). An
    /// empty hash list means it was not retained (the default modes), and the
    /// caller reads the hashes from the `.seg` instead. For the committed
    /// segment dedup scan ([`super::Reader::committed_dedup_rows`]).
    pub fn dedup_frames_of(&self, bucket: u16, pid: u64, out: &mut Vec<(Record, Vec<u8>)>) {
        let b = &self.buckets[bucket as usize];
        for (key, rec) in b.recs.range((pid, 0)..=(pid, u64::MAX)) {
            let hashes = b.hashes.get(key).cloned().unwrap_or_default();
            out.push((*rec, hashes));
        }
    }

    /// The active file id of a bucket, as the index knows it.
    pub fn file_id(&self, bucket: u16) -> Option<u32> {
        self.buckets[bucket as usize].file_id
    }

    /// The frame holding `(pid, offset)` in the active file, with that file's
    /// id, read under one lock.
    pub fn probe(&self, bucket: u16, pid: u64, offset: u64) -> Option<(u32, Record)> {
        let b = &self.buckets[bucket as usize];
        let file_id = b.file_id?;
        b.recs
            .range(..=(pid, offset))
            .next_back()
            .map(|(_, r)| *r)
            .filter(|r| r.holds(pid, offset))
            .map(|r| (file_id, r))
    }

    /// Records held, over every bucket. The number I8 watches (see the module
    /// header's arithmetic).
    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.recs.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.iter().all(|b| b.recs.is_empty())
    }

    /// Records held by one bucket's active file.
    pub fn bucket_len(&self, bucket: u16) -> usize {
        self.buckets[bucket as usize].recs.len()
    }
}
