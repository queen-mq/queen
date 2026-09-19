//! The `LocalReplicator`'s own write-ahead log (PLAN_RAFT.md §12.2), based on
//! the pgless node journal (`6e96e228:server/src/native/journal.rs`): one
//! durability stream, group commit, and ONE fsync per group.
//!
//! It is the write-ahead log of the whole node. A `propose` acknowledges a
//! command only after the command's entry has been fsynced HERE and then
//! applied to the store's page cache (§7.1, I4); the store commit itself is
//! non-durable between durable points (§11.3), so what makes an acknowledged
//! entry survive a crash is this log's fsync, not the store's. Recovery
//! (§11.5) re-applies every entry after the store's durable index from here.
//!
//! # Frame (§12.2)
//!
//! ```text
//! len:u32 | xxh3:u64 | index:u64 | term:u64 | bytes[len]
//! ```
//!
//! `len` is the length of `bytes` (the encoded [`Entry`](crate::rsm::entry));
//! `xxh3` is `xxh3_64(index_le ‖ term_le ‖ bytes)`, so the frame carries its
//! OWN integrity independent of the entry codec, and a torn tail — the last
//! group that was writing when the process died — is caught here and
//! truncated. `term` is always [`LOG_TERM`] (1): the `LocalReplicator` is a
//! single node with no elections.
//!
//! # Files, and truncation behind durable points
//!
//! Frames go into rolling files `log/rNNNNNNNN.qlog`, each with a 32-byte
//! header (magic, file id, the index its first frame will carry). The active
//! file rolls at [`LogOptions::segment_bytes`]; sealed files never change. When
//! the apply thread reports a durable point (§11.4 step 3, `Notify::durable`),
//! [`LogStore::truncate_through`] drops every SEALED file whose last index is
//! at or below the durable index: those entries are durably in the store and
//! nothing will ever replay them, so the log holds about one durable interval
//! (§12.2 "the log is truncated behind durable points").
//!
//! # One writer
//!
//! `LogStore` is single-threaded by construction (append and truncate take
//! `&mut self`). The [`super::local::LocalReplicator`] owns it on a dedicated
//! `std` thread — never a tokio worker (I15) — that batches proposals into a
//! group and fsyncs once, exactly as the pgless journal writer did.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use xxhash_rust::xxh3::Xxh3;

/// §12.2: the log's term is fixed at 1 — a single node, no elections.
pub const LOG_TERM: u64 = 1;

const MAGIC: [u8; 8] = *b"QNRLOG1\0";
const FILE_HEADER_LEN: u64 = 32;
/// `len:u32 | xxh3:u64 | index:u64 | term:u64`.
const FRAME_PREFIX: usize = 4 + 8 + 8 + 8;
/// The per-frame overhead, for a caller sizing a group by bytes.
pub const FRAME_OVERHEAD: usize = FRAME_PREFIX;
/// A frame body is one encoded entry (≤ `QUEEN_RAFT_ENTRY_MAX_BYTES`, 96 MiB)
/// plus slack. A `len` prefix above this is refused at scan time before a
/// single byte is reserved, so a lying prefix in a torn tail allocates
/// nothing (the same anti-OOM rule the entry codec keeps).
const MAX_FRAME_BODY: u32 = 128 * 1024 * 1024;

/// Default roll size for a log file. Sealed files below the durable point are
/// dropped whole, so this bounds how much dead log one durable interval holds.
pub const DEFAULT_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// How the group fsync reaches the platter.
///
/// Mirrors [`crate::rsm::segments::FsyncMode`] with a test-only `Off`: on
/// macOS a real barrier is `F_FULLFSYNC` and serializes (the numbers to quote
/// come from the Linux VM anyway, §0.3), so the fast unit tests skip it. The
/// throughput smoke and any durability run use `Full` or `Data`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fsync {
    /// `F_FULLFSYNC` on macOS, `fdatasync` elsewhere: the barrier a real
    /// deployment uses.
    Full,
    /// `fdatasync` on Linux, a plain `fsync` on macOS (does NOT guarantee the
    /// platter on macOS — cheap, for tests and the laptop smoke).
    Data,
    /// No fsync at all. Tests only: a `kill -9` keeps the page cache, so the
    /// bookkeeping is falsified without paying for a barrier (R-02 shape).
    Off,
}

#[derive(Clone, Copy, Debug)]
pub struct LogOptions {
    pub segment_bytes: u64,
    pub fsync: Fsync,
}

impl Default for LogOptions {
    fn default() -> LogOptions {
        LogOptions {
            segment_bytes: DEFAULT_SEGMENT_BYTES,
            fsync: Fsync::Full,
        }
    }
}

impl LogOptions {
    /// Resolve from the environment. Boot-only.
    pub fn from_env() -> LogOptions {
        let mut o = LogOptions::default();
        if let Some(v) = std::env::var("QUEEN_RAFT_LOG_SEGMENT_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v >= FILE_HEADER_LEN)
        {
            o.segment_bytes = v;
        }
        o
    }

    /// Small files and a cheap barrier: a handful of appends rolls a file and
    /// exercises the seal, the drop and the recovery scan.
    pub fn testing(segment_bytes: u64) -> LogOptions {
        LogOptions {
            segment_bytes: segment_bytes.max(FILE_HEADER_LEN + 1),
            fsync: Fsync::Off,
        }
    }
}

/// What [`LogStore::open`] found and fixed (§11.5 step 1 for the log).
#[derive(Clone, Debug, Default)]
pub struct LogRecovery {
    /// The index of the last valid frame (0 when the log is empty).
    pub last_index: u64,
    pub last_term: u64,
    /// Files present after recovery.
    pub files: usize,
    /// Total valid bytes across every file (headers included).
    pub bytes: u64,
    /// A torn tail — a partial or checksum-failing frame at the end of the
    /// last file — was truncated. The acknowledged entries (fsynced groups)
    /// are all below it, so none is ever lost this way.
    pub truncated_tail: bool,
    /// Frames scanned during recovery.
    pub scanned: u64,
}

#[derive(Clone, Debug)]
struct FileMeta {
    id: u64,
    /// The index the first frame in this file carries (from its header).
    base_index: u64,
    /// The index of the last valid frame in this file; `base_index - 1` when
    /// the file holds no frame yet.
    last_index: u64,
    /// Valid bytes in the file, header included (what recovery truncated to).
    bytes: u64,
}

impl FileMeta {
    fn is_empty(&self) -> bool {
        self.last_index + 1 == self.base_index
    }
}

/// A group written to the log but not yet fsynced (PERF-G,
/// `QUEEN_RAFT_WRITER_PIPELINE`). The writer thread hands one to the syncer
/// thread, which flushes it and only then acknowledges the group's entries
/// (I4). It holds a dup of the file the frames landed in, so `sync` flushes the
/// same inode the writer keeps appending to; fsyncing a dup fd covers every
/// byte written before the handle was taken.
pub struct SyncHandle {
    file: File,
    fsync: Fsync,
}

impl SyncHandle {
    /// Flush the group to the platter, per the log's fsync mode. Times the
    /// barrier into `log_fsync` (PERF-1), gated on the metrics knob.
    pub fn sync(&self) -> io::Result<()> {
        let s0 = crate::rsm::timing::stamp();
        let r = match self.fsync {
            Fsync::Off => Ok(()),
            Fsync::Data => self.file.sync_data(),
            Fsync::Full => full_fsync(&self.file),
        };
        if let Some(s0) = s0 {
            crate::rsm::timing::metrics()
                .log_fsync
                .record_dur(s0.elapsed());
        }
        r
    }
}

/// The append-only local log. One writer (`&mut self`), owned by the
/// `LocalReplicator`'s writer thread.
pub struct LogStore {
    dir: PathBuf,
    opts: LogOptions,
    /// Every file, ascending by id; the last is the active one.
    files: Vec<FileMeta>,
    active: File,
    /// Byte size of the active file (== `files.last().bytes`).
    active_size: u64,
    /// The index the next appended frame carries.
    next_index: u64,
    last_term: u64,
}

impl LogStore {
    /// Open (creating if needed) the log under `dir`, validate every frame,
    /// truncate a torn tail in the last file, and leave the active file
    /// positioned at its end. A frame that fails validation in a SEALED file
    /// is real corruption of an acknowledged entry and is refused; in the last
    /// file it is a torn tail and is truncated.
    pub fn open(dir: &Path, opts: LogOptions) -> io::Result<(LogStore, LogRecovery)> {
        std::fs::create_dir_all(dir)?;
        let mut ids = scan_ids(dir)?;
        ids.sort_unstable();

        let mut rec = LogRecovery::default();

        // A `roll` that was interrupted between creating the next file and
        // writing its header (a `kill -9` in that window) leaves the newest
        // file shorter than its 32-byte header. It carries no frame — the
        // header is written before any frame — so drop it and fall back to the
        // previous file as the active one. Only the newest file can be in this
        // state; a sealed file always has a complete header.
        while let Some(&last) = ids.last() {
            let len = std::fs::metadata(file_path(dir, last))?.len();
            if len < FILE_HEADER_LEN {
                std::fs::remove_file(file_path(dir, last))?;
                sync_dir(dir)?;
                ids.pop();
                rec.truncated_tail = true;
            } else {
                break;
            }
        }

        let mut files: Vec<FileMeta> = Vec::new();
        let mut expected_index: Option<u64> = None; // the next index a frame must carry

        for (pos, id) in ids.iter().copied().enumerate() {
            let is_last = pos + 1 == ids.len();
            let path = file_path(dir, id);
            let (meta, torn, scanned) = validate_file(&path, id, expected_index, is_last)?;
            rec.scanned += scanned;
            if torn {
                rec.truncated_tail = true;
            }
            expected_index = Some(meta.last_index + 1);
            files.push(meta);
        }

        // No files at all: create the first one, its frames starting at 1.
        if files.is_empty() {
            let base = 1;
            create_file(dir, 1, base)?;
            files.push(FileMeta {
                id: 1,
                base_index: base,
                last_index: base - 1,
                bytes: FILE_HEADER_LEN,
            });
        }

        let last = files.last().expect("at least one file");
        let active_id = last.id;
        let active_size = last.bytes;
        let next_index = last.last_index + 1;
        let last_term = if next_index > 1 { LOG_TERM } else { 0 };

        // Reopen the active file for append, truncated to its valid length and
        // positioned at the end (a fresh handle sits at offset 0 and would
        // overwrite the header, exactly the pgless journal bug the reopen
        // guards against).
        let mut active = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path(dir, active_id))?;
        active.set_len(active_size)?;
        active.seek(SeekFrom::Start(active_size))?;

        rec.last_index = next_index - 1;
        rec.last_term = last_term;
        rec.files = files.len();
        rec.bytes = files.iter().map(|f| f.bytes).sum();

        let store = LogStore {
            dir: dir.to_path_buf(),
            opts,
            files,
            active,
            active_size,
            next_index,
            last_term,
        };
        tracing::info!(
            target: "rsm",
            files = rec.files,
            last_index = rec.last_index,
            truncated_tail = rec.truncated_tail,
            "rsm local log open",
        );
        Ok((store, rec))
    }

    pub fn last_index(&self) -> u64 {
        self.next_index - 1
    }

    pub fn last_term(&self) -> u64 {
        self.last_term
    }

    pub fn next_index(&self) -> u64 {
        self.next_index
    }

    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    pub fn bytes(&self) -> u64 {
        self.files.iter().map(|f| f.bytes).sum()
    }

    /// Append one group of entries and fsync ONCE. Returns the index the first
    /// entry was assigned; the rest follow contiguously. The whole group is
    /// one `write_all` and one barrier, which is what amortizes the flush the
    /// way a single WAL did for Postgres (pgless journal §10).
    pub fn append_group(&mut self, entries: &[&[u8]]) -> io::Result<u64> {
        if entries.is_empty() {
            return Ok(self.next_index);
        }
        // Roll before the group if the active file is already over its size,
        // so a file never overshoots by more than one group; a group larger
        // than the limit still goes to a file of its own (it was just rolled
        // into an empty one).
        if self.active_size > FILE_HEADER_LEN && self.active_size >= self.opts.segment_bytes {
            self.roll()?;
        }

        let first_index = self.next_index;
        let mut buf: Vec<u8> = Vec::with_capacity(
            entries
                .iter()
                .map(|e| e.len() + FRAME_PREFIX)
                .sum::<usize>(),
        );
        let mut index = self.next_index;
        for e in entries {
            encode_frame(&mut buf, index, LOG_TERM, e);
            index += 1;
        }

        self.active.write_all(&buf)?;
        // §13.5 `log.appended`: the group is in the local raft log, not yet
        // flushed. A crash here (page cache kept) may or may not leave the
        // bytes on disk; recovery's torn-tail truncation drops an unfsynced
        // frame, so the entry is unanswered (its propose never returned) and
        // at-most-once (I4).
        crate::rsm::faults::hit("log.appended");
        // PERF-1: the one group fsync per commit — the write barrier that
        // stands behind the p99 tail. The clock read is gated on the knob
        // (`stamp` is `None` when metrics are off) so the ablation prices it.
        let s0 = crate::rsm::timing::stamp();
        self.fsync_active()?;
        if let Some(s0) = s0 {
            crate::rsm::timing::metrics()
                .log_fsync
                .record_dur(s0.elapsed());
        }
        // §13.5 `log.flushed`: the group is durable in the raft log. It is
        // committed on a single voter (quorum of one); a crash here means the
        // entry WILL replay on restart, so a write whose propose had not yet
        // returned still becomes exactly-once through the log (I4).
        crate::rsm::faults::hit("log.flushed");

        self.advance_after_write(&buf, index);
        Ok(first_index)
    }

    /// Write one group's frames but do NOT fsync; return the first index and a
    /// [`SyncHandle`] the caller flushes (possibly on another thread) before it
    /// acknowledges the group's entries (PERF-G, `QUEEN_RAFT_WRITER_PIPELINE`).
    ///
    /// The handle dups the file the frames landed in, so its `sync` flushes the
    /// SAME inode this writer keeps appending to — covering every byte written
    /// up to now, a superset of this group, which is exactly what I4 needs (an
    /// entry is acked only after a fsync that covered its bytes). The in-memory
    /// index/size bookkeeping advances here (so the next group is framed at the
    /// right index); durability is the handle's job, and recovery rebuilds the
    /// real valid length from the frames on disk, so bookkeeping running ahead
    /// of the fsync is safe.
    pub fn append_group_deferred(&mut self, entries: &[&[u8]]) -> io::Result<(u64, SyncHandle)> {
        if entries.is_empty() {
            return Ok((
                self.next_index,
                SyncHandle {
                    file: self.active.try_clone()?,
                    fsync: self.opts.fsync,
                },
            ));
        }
        if self.active_size > FILE_HEADER_LEN && self.active_size >= self.opts.segment_bytes {
            self.roll()?;
        }
        let first_index = self.next_index;
        let mut buf: Vec<u8> = Vec::with_capacity(
            entries
                .iter()
                .map(|e| e.len() + FRAME_PREFIX)
                .sum::<usize>(),
        );
        let mut index = self.next_index;
        for e in entries {
            encode_frame(&mut buf, index, LOG_TERM, e);
            index += 1;
        }
        self.active.write_all(&buf)?;
        crate::rsm::faults::hit("log.appended");
        // Dup the file the group landed in BEFORE any later roll swaps
        // `self.active`, so the handle always fsyncs the right inode.
        let handle = SyncHandle {
            file: self.active.try_clone()?,
            fsync: self.opts.fsync,
        };
        self.advance_after_write(&buf, index);
        Ok((first_index, handle))
    }

    /// Advance the in-memory bookkeeping after a group's frames are written
    /// (shared by the fsync-inline and deferred-fsync append paths).
    fn advance_after_write(&mut self, buf: &[u8], next_index: u64) {
        let added = buf.len() as u64;
        self.active_size += added;
        self.next_index = next_index;
        self.last_term = LOG_TERM;
        let last = self.files.last_mut().expect("active file meta");
        last.bytes = self.active_size;
        last.last_index = self.next_index - 1;
    }

    /// Roll to a fresh active file whose first frame will carry `next_index`.
    fn roll(&mut self) -> io::Result<()> {
        // The active file is already fsynced per group; seal it and its
        // metadata is final.
        let next_id = self.files.last().expect("active").id + 1;
        create_file(&self.dir, next_id, self.next_index)?;
        self.active = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path(&self.dir, next_id))?;
        self.active.seek(SeekFrom::Start(FILE_HEADER_LEN))?;
        self.active_size = FILE_HEADER_LEN;
        self.files.push(FileMeta {
            id: next_id,
            base_index: self.next_index,
            last_index: self.next_index - 1,
            bytes: FILE_HEADER_LEN,
        });
        Ok(())
    }

    fn fsync_active(&mut self) -> io::Result<()> {
        match self.opts.fsync {
            Fsync::Off => Ok(()),
            Fsync::Data => self.active.sync_data(),
            Fsync::Full => full_fsync(&self.active),
        }
    }

    /// Drop every SEALED file whose last index is at or below `durable_index`:
    /// those entries are durably in the store and nothing replays them. Never
    /// the active file. Returns the number of files unlinked.
    pub fn truncate_through(&mut self, durable_index: u64) -> io::Result<usize> {
        let active_id = self.files.last().expect("active").id;
        let victims: Vec<u64> = self
            .files
            .iter()
            .filter(|f| f.id != active_id && !f.is_empty() && f.last_index <= durable_index)
            .map(|f| f.id)
            .collect();
        if victims.is_empty() {
            return Ok(0);
        }
        for id in &victims {
            match std::fs::remove_file(file_path(&self.dir, *id)) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        self.files.retain(|f| !victims.contains(&f.id));
        sync_dir(&self.dir)?;
        tracing::debug!(
            target: "rsm",
            dropped = victims.len(),
            durable = durable_index,
            "rsm local log files dropped behind the durable point",
        );
        Ok(victims.len())
    }

    /// Every frame with `index >= from`, in index order, handed to `cb` as
    /// borrowed bytes. What recovery replays after the store's durable index
    /// (§11.5 step 5). `cb` returning `Err` stops the scan.
    pub fn scan_from(&self, from: u64, cb: &mut ScanCb<'_>) -> io::Result<u64> {
        let mut count = 0u64;
        for meta in &self.files {
            if meta.is_empty() || meta.last_index < from {
                continue;
            }
            let path = file_path(&self.dir, meta.id);
            let mut rdr = BufReader::new(File::open(&path)?);
            skip_header(&mut rdr)?;
            let mut pos = FILE_HEADER_LEN;
            while pos < meta.bytes {
                let Some(frame) = read_frame(&mut rdr, meta.bytes - pos)? else {
                    break;
                };
                pos += frame.len;
                if frame.index >= from {
                    cb(frame.index, frame.term, &frame.body)?;
                    count += 1;
                }
            }
        }
        Ok(count)
    }
}

// ---------------------------------------------------------------------------
// Frame codec
// ---------------------------------------------------------------------------

/// A callback over `(index, term, body)` for one frame (`scan_from`).
type ScanCb<'a> = dyn FnMut(u64, u64, &[u8]) -> io::Result<()> + 'a;

/// One decoded frame and its total on-disk length.
struct Frame {
    index: u64,
    term: u64,
    body: Vec<u8>,
    /// Bytes this frame occupies on disk (`FRAME_PREFIX + body.len()`).
    len: u64,
}

fn encode_frame(buf: &mut Vec<u8>, index: u64, term: u64, body: &[u8]) {
    let mut h = Xxh3::new();
    h.update(&index.to_le_bytes());
    h.update(&term.to_le_bytes());
    h.update(body);
    let sum = h.digest();
    buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
    buf.extend_from_slice(&sum.to_le_bytes());
    buf.extend_from_slice(&index.to_le_bytes());
    buf.extend_from_slice(&term.to_le_bytes());
    buf.extend_from_slice(body);
}

/// Read one frame, returning `(index, term, body, bytes_consumed)` or `None`
/// for a torn tail (short read, an oversized or over-long `len`, or a checksum
/// mismatch). `remaining` bounds a `len` prefix so a lie allocates nothing.
fn read_frame<R: Read>(rdr: &mut R, remaining: u64) -> io::Result<Option<Frame>> {
    let mut prefix = [0u8; FRAME_PREFIX];
    if !read_exact_or_eof(rdr, &mut prefix)? {
        return Ok(None);
    }
    let len = u32::from_le_bytes(prefix[0..4].try_into().unwrap());
    let sum = u64::from_le_bytes(prefix[4..12].try_into().unwrap());
    let index = u64::from_le_bytes(prefix[12..20].try_into().unwrap());
    let term = u64::from_le_bytes(prefix[20..28].try_into().unwrap());
    if len > MAX_FRAME_BODY {
        return Ok(None);
    }
    let frame_len = FRAME_PREFIX as u64 + len as u64;
    if frame_len > remaining {
        return Ok(None);
    }
    let mut body = vec![0u8; len as usize];
    if !read_exact_or_eof(rdr, &mut body)? {
        return Ok(None);
    }
    let mut h = Xxh3::new();
    h.update(&index.to_le_bytes());
    h.update(&term.to_le_bytes());
    h.update(&body);
    if h.digest() != sum {
        return Ok(None);
    }
    Ok(Some(Frame {
        index,
        term,
        body,
        len: frame_len,
    }))
}

/// Fill `buf`, or report EOF/short read as `false` (a torn tail) rather than
/// an error.
fn read_exact_or_eof<R: Read>(rdr: &mut R, buf: &mut [u8]) -> io::Result<bool> {
    let mut read = 0;
    while read < buf.len() {
        match rdr.read(&mut buf[read..]) {
            Ok(0) => return Ok(false),
            Ok(n) => read += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

/// Validate every frame of one file. In the LAST file a torn tail is truncated
/// (the file is set to the valid length) and reported; in a sealed file any
/// break is corruption of an acknowledged entry and refused. `expected_index`
/// is the index the first frame must carry (`None` for the first file, which
/// takes its base from its header).
fn validate_file(
    path: &Path,
    id: u64,
    expected_index: Option<u64>,
    is_last: bool,
) -> io::Result<(FileMeta, bool, u64)> {
    let mut f = OpenOptions::new().read(true).write(true).open(path)?;
    let file_len = f.metadata()?.len();
    if file_len < FILE_HEADER_LEN {
        return Err(corrupt(path, "file shorter than its header"));
    }
    let mut header = [0u8; FILE_HEADER_LEN as usize];
    f.read_exact(&mut header)?;
    if header[0..8] != MAGIC {
        return Err(corrupt(path, "bad magic"));
    }
    let hdr_id = u64::from_le_bytes(header[8..16].try_into().unwrap());
    if hdr_id != id {
        return Err(corrupt(path, "file id in header does not match its name"));
    }
    let base_index = u64::from_le_bytes(header[16..24].try_into().unwrap());
    if let Some(exp) = expected_index {
        if base_index != exp {
            return Err(corrupt(
                path,
                "first index does not follow the previous file",
            ));
        }
    }

    let mut rdr = BufReader::new(f.try_clone()?);
    rdr.seek(SeekFrom::Start(FILE_HEADER_LEN))?;
    let mut pos = FILE_HEADER_LEN;
    let mut next = base_index;
    let mut scanned = 0u64;
    let mut torn = false;
    loop {
        if pos >= file_len {
            break;
        }
        match read_frame(&mut rdr, file_len - pos)? {
            Some(frame) => {
                if frame.index != next {
                    // A frame whose index does not follow: a torn tail in the
                    // last file (a stale frame left by a previous, shorter
                    // life of this file id), corruption otherwise.
                    if is_last {
                        torn = true;
                        break;
                    }
                    return Err(corrupt(path, "non-contiguous index in a sealed file"));
                }
                next += 1;
                pos += frame.len;
                scanned += 1;
            }
            None => {
                if is_last {
                    torn = true;
                    break;
                }
                return Err(corrupt(path, "torn or corrupt frame in a sealed file"));
            }
        }
    }

    if torn && pos < file_len {
        // Truncate the torn tail so the reopen for append starts clean.
        f.set_len(pos)?;
        f.sync_all()?;
    }

    Ok((
        FileMeta {
            id,
            base_index,
            last_index: next - 1,
            bytes: pos,
        },
        torn,
        scanned,
    ))
}

// ---------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------

fn file_name(id: u64) -> String {
    format!("r{id:08}.qlog")
}

fn file_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(file_name(id))
}

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

fn create_file(dir: &Path, id: u64, base_index: u64) -> io::Result<()> {
    let path = file_path(dir, id);
    let mut f = OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .open(&path)?;
    let mut h = [0u8; FILE_HEADER_LEN as usize];
    h[0..8].copy_from_slice(&MAGIC);
    h[8..16].copy_from_slice(&id.to_le_bytes());
    h[16..24].copy_from_slice(&base_index.to_le_bytes());
    f.write_all(&h)?;
    f.sync_all()?;
    sync_dir(dir)?;
    Ok(())
}

fn skip_header<R: Read>(rdr: &mut R) -> io::Result<()> {
    let mut h = [0u8; FILE_HEADER_LEN as usize];
    rdr.read_exact(&mut h)?;
    Ok(())
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    // A directory fsync makes a create/unlink durable. Best-effort on
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
        format!("rsm local log corrupt ({}): {what}", path.display()),
    )
}

// ---------------------------------------------------------------------------
// F_FULLFSYNC on macOS, plain fsync elsewhere
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
fn full_fsync(f: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    // F_FULLFSYNC = 51. sync_all() on macOS is fsync(2), which does NOT flush
    // the drive's own cache; the durable barrier is F_FULLFSYNC.
    let rc = unsafe { libc_fcntl(f.as_raw_fd(), 51) };
    if rc == -1 {
        // Fall back to fsync if the filesystem does not support F_FULLFSYNC.
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
