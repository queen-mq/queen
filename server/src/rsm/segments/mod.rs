//! Payload segment files (PLAN_RAFT.md §11.2, §11.4, §11.7, §6.1, §6.2).
//!
//! ```text
//! $QUEEN_RAFT_DIR/sm-<index>-<term>/seg/
//!   b000/ f0000000000.seg   append-only frames (§11.2)
//!         f0000000000.qidx  the sealed file's index (§6.1 amendment)
//!         f0000000001.seg   the ACTIVE file: its index is in RAM
//!   b001/ …
//!   b255/
//! ```
//!
//! # What this module is
//!
//! The bytes of every message live here; everything else lives in the ordered
//! store. One directory per bucket — `xxh3(tenant ␟ queue ␟ partition) % 256`,
//! computed by the planner and carried in
//! [`crate::rsm::effect::Effect::Append`] so every node files the bytes the
//! same way. Files are append-only, roll at `QUEEN_RAFT_SEGMENT_BYTES`, and a
//! sealed file never changes again.
//!
//! The apply thread is the ONLY writer (I1) and it writes without fsync: the
//! Raft log is the write-ahead log, and durability comes from the durable
//! point (§11.4), which fsyncs what changed and hands the caller the file
//! lengths to record in the same store commit (I11).
//!
//! # What it is NOT
//!
//! - It is not pgless's per-bucket group-committing log. `native/log.rs` gave
//!   each bucket a thread, a channel and a lock, and paid an fsync per group
//!   so a push could be answered before commit. Here a push is answered after
//!   commit and apply (D7), so the fsync belongs to the durable point and one
//!   thread owns all 256 buckets. The framing, the rolling, the torn-tail
//!   scan and the file-drop discipline are lifted from it; the journal
//!   coupling, the `Wait` modes and the per-bucket locks are not (§3.5).
//! - It holds no replicated state. Positions — `(bucket, file id, offset,
//!   len)` — are node-local, never appear in an entry or a digest, and travel
//!   only with the files they index (D8, I7). Nothing in this module is
//!   readable by another node.
//! - It decides nothing about retention. It counts what is live and refuses to
//!   unlink what is not dead ([`Segments::gc_candidates`]); WHEN a segment
//!   stops being live is the retention loop's call (§8, 006).
//!
//! # The five things it guarantees
//!
//! 1. **Every read verifies.** [`Segments::read`] checks the frame checksum
//!    before the caller sees a byte, and checks that the position's length is
//!    the frame's own — the checksum covers the frame, so a position one byte
//!    too long would otherwise verify and leak the next frame's head into a
//!    payload. pgless's `read_blob` verified nothing at all (§11.2).
//! 2. **A sealed file has an index, or one is rebuilt.** The `.qidx` is
//!    written at the seal; recovery rebuilds any that is missing, stale or
//!    damaged by scanning the file (§11.5). A seal never takes the frames of a
//!    file out of reach: the sealed index is installed for readers before the
//!    `.qidx` write begins, and a write the disk refuses is OWED — the records
//!    keep answering from RAM and every durable point tries again
//!    ([`Segments::owed_indexes`]). A roll also always leaves the bucket a
//!    successor to write into, or the file it owes it.
//! 3. **Recovery trusts the store, not the disk.** Files are truncated to the
//!    lengths the reopened state recorded; a file SHORTER than its recorded
//!    length, or a frame below that length that does not verify, is the
//!    store/files disagreement of I11 and is reported, never papered over.
//! 4. **A file is unlinked only when nothing can want it.** Retention, the
//!    txns window (D10), snapshot manifests and claim pins each hold a file
//!    alive; GC is two-phase so the caller can put the durable commit that
//!    stops referencing a file BEFORE the unlink (I10, §11.7). Those holds are
//!    part of [`FileState`], so they survive a restart: a file table that
//!    persisted only lengths would call every sealed file dead at the next
//!    boot and let GC unlink payloads nobody has acked. A pin is granted only
//!    for a file this node still holds, under the lock [`Segments::unlink`]
//!    keeps from its pin check to the row's removal — so a pin and an unlink
//!    can never both believe they won.
//! 5. **Cost follows the write rate.** A durable point fsyncs the files
//!    touched since the last one, not the files that exist; a lookup is one
//!    binary search in a mapping, not a scan (I8). The directories are the one
//!    exception and they are paid once, at open: the tree's 256 bucket
//!    directories are created and their names made durable there, never on the
//!    write path.
//!
//! # Threads
//!
//! [`Segments`] is the writer's handle and every mutating method takes
//! `&mut self`: the apply thread owns it. [`Reader`] is a cheap clone of the
//! shared read side for the blocking pool.
//!
//! Every call here BLOCKS, so none of them may run on a tokio worker; no lock
//! taken inside is held across a syscall, let alone across an `.await`. That
//! is half of I15. The other half — "every I/O call has a deadline" — is met
//! as far as a blocking file syscall allows: the read path takes the caller's
//! deadline ([`Reader::read_at_within`] and friends) and refuses BEFORE each
//! syscall it would start, so a budget that is gone costs no I/O and a slow
//! disk cannot turn one lookup into an unbounded walk. It cannot abort a
//! syscall the kernel has already taken; for a disk that never answers, the
//! bound is the caller's timeout on the blocking task's join, and this module
//! makes that safe by holding no lock the apply thread needs while it waits.
//! The writer's own calls (append, roll, durable point, recovery) take no
//! deadline on purpose: they run on the apply thread, where a clock read would
//! put nondeterminism on the apply path (I2) and where refusing half way
//! through an entry is not an answer — §11.8 handles the disk that is full,
//! and a disk that has stopped answering stops this node.

pub mod frame;
pub mod index;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use crate::rsm::effect::Pid;

pub use frame::FrameError;
pub use index::{IndexError, Record};

/// Buckets, fixed by the format: `xxh3(names) % 256` (§0.4, D9).
pub const NBUCKETS: usize = 256;

/// `QUEEN_RAFT_SEGMENT_BYTES` (Appendix H).
pub const DEFAULT_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Below this a "segment" is not a segment. A typo in the environment must not
/// turn every frame into its own file.
const MIN_SEGMENT_BYTES: u64 = 1024;

/// How many `.qidx` mappings and read handles one node keeps open. Both caches
/// are node-local conveniences: a miss costs one `open` (and, for an index,
/// one checksum pass), never a wrong answer.
const CACHE_MAX: usize = 1024;

/// The most the encode buffer keeps between appends. Above it the buffer is
/// dropped rather than carried.
const BUF_KEEP_BYTES: usize = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// What can go wrong here.
///
/// The distinction that matters is [`SegError::is_disagreement`]: an I/O error
/// is an I/O error, but a file that is shorter than the length the store
/// recorded, a file the store names and the disk does not have, or a frame
/// below the recorded length whose checksum fails, are all the same thing —
/// the store and the files disagree (I11). §11.5 answers that one case by
/// discarding the state directory and installing a snapshot (raft3) or by
/// repairing from this node's own newest snapshot plus the Raft log (raft1).
/// It must never be confused with a torn TAIL, which is ordinary and is
/// truncated.
#[derive(Debug)]
pub enum SegError {
    /// The store recorded a length the file cannot honour (I11).
    ShortFile {
        bucket: u16,
        file_id: u32,
        on_disk: u64,
        recorded: u64,
    },
    /// The store names a file that is not on disk (I11).
    MissingFile {
        bucket: u16,
        file_id: u32,
    },
    /// A frame BELOW the recorded length did not decode or did not verify
    /// (I11). Past the recorded length the same damage is a torn tail and is
    /// simply truncated.
    Damaged {
        bucket: u16,
        file_id: u32,
        offset: u64,
        why: FrameError,
    },
    /// A position that no file can answer: the file was unlinked, or the
    /// offset is past its end.
    NoSuchPosition(Position),
    /// The index survived its own checksum and still pointed at the wrong
    /// frame. The end-to-end check of [`Reader::read_at`]: `.qidx` bytes and
    /// `.seg` bytes are checksummed separately, so only comparing them catches
    /// an index that is internally consistent and wrong.
    IndexMismatch {
        position: Position,
        want: (Pid, u64),
        got: (Pid, u64),
    },
    /// The position's `len` is not the length the frame at that offset
    /// declares. The frame's checksum covers only the frame, so a position
    /// that is too LONG verifies and would hand the caller the head of the
    /// next frame as if it were payload; one that is too short cannot be the
    /// frame the index promised either. Both are refused rather than served.
    LenMismatch {
        position: Position,
        frame_len: u32,
    },
    /// The caller's deadline passed before this call could issue the next
    /// syscall (I15). Retryable, and never a statement about the data.
    DeadlineExceeded {
        what: &'static str,
    },
    /// The caller asked for something the layout forbids.
    Refused(&'static str),
    Io(io::Error),
}

impl SegError {
    /// Is this the store/files disagreement of I11?
    pub fn is_disagreement(&self) -> bool {
        matches!(
            self,
            SegError::ShortFile { .. } | SegError::MissingFile { .. } | SegError::Damaged { .. }
        )
    }
}

impl std::fmt::Display for SegError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SegError::ShortFile {
                bucket,
                file_id,
                on_disk,
                recorded,
            } => write!(
                f,
                "segment b{bucket:03}/f{file_id} is {on_disk} bytes, the store recorded {recorded}"
            ),
            SegError::MissingFile { bucket, file_id } => {
                write!(f, "segment b{bucket:03}/f{file_id} is missing")
            }
            SegError::Damaged {
                bucket,
                file_id,
                offset,
                why,
            } => write!(
                f,
                "segment b{bucket:03}/f{file_id} is damaged at {offset}: {why}"
            ),
            SegError::NoSuchPosition(p) => write!(f, "no such position {p:?}"),
            SegError::IndexMismatch {
                position,
                want,
                got,
            } => write!(
                f,
                "index at {position:?} promised pid {} offset {}, the frame is pid {} offset {}",
                want.0, want.1, got.0, got.1
            ),
            SegError::LenMismatch {
                position,
                frame_len,
            } => write!(
                f,
                "position {position:?} claims {} bytes, the frame there is {frame_len}",
                position.len
            ),
            SegError::DeadlineExceeded { what } => {
                write!(f, "deadline passed before {what}")
            }
            SegError::Refused(why) => write!(f, "refused: {why}"),
            SegError::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for SegError {}

impl From<io::Error> for SegError {
    fn from(e: io::Error) -> SegError {
        SegError::Io(e)
    }
}

impl From<FrameError> for SegError {
    fn from(e: FrameError) -> SegError {
        SegError::Io(e.into())
    }
}

impl From<IndexError> for SegError {
    fn from(e: IndexError) -> SegError {
        SegError::Io(e.into())
    }
}

impl From<SegError> for io::Error {
    fn from(e: SegError) -> io::Error {
        match e {
            SegError::Io(e) => e,
            other => io::Error::new(io::ErrorKind::InvalidData, other.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, SegError>;

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/// How hard a durable point pushes on the disk.
///
/// `Full` is what a durable point must do: on macOS `F_FULLFSYNC`, the only
/// call that flushes the drive's own cache (it is serialized by the drive, so
/// a laptop durable point costs milliseconds per file); on Linux `fsync`.
/// `Data` is `fdatasync` on Linux and a plain `fsync` on macOS, which does NOT
/// flush the drive cache: it exists to separate "the file system work" from
/// "the barrier" when reading laptop numbers, and to keep the unit tests off
/// the drive's serialized path. It is never a durability mode to ship.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsyncMode {
    Full,
    Data,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Options {
    /// Roll to a new file once the active one would pass this size. Checked
    /// before a frame is written, so a file never overshoots; a frame larger
    /// than the limit goes into a file of its own.
    pub segment_bytes: u64,
    pub fsync: FsyncMode,
    /// How many threads issue the durable point's fsyncs. §11.4's cost is
    /// proportional to the number of BUCKETS touched, not to bytes, so the
    /// fan-out is the knob; WP-1.4 tunes it against the cadence.
    pub fsync_threads: usize,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            segment_bytes: DEFAULT_SEGMENT_BYTES,
            fsync: FsyncMode::Full,
            fsync_threads: 4,
        }
    }
}

impl Options {
    /// Resolve from the environment. Called ONCE at boot, never from apply:
    /// apply reads no environment (I2).
    pub fn from_env() -> Options {
        let mut o = Options::default();
        if let Some(v) = std::env::var("QUEEN_RAFT_SEGMENT_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
        {
            o.segment_bytes = v.max(MIN_SEGMENT_BYTES);
        }
        o
    }

    /// Everything a test wants: small files and no drive barrier.
    #[cfg(test)]
    pub fn testing(segment_bytes: u64) -> Options {
        Options {
            segment_bytes: segment_bytes.max(MIN_SEGMENT_BYTES),
            fsync: FsyncMode::Data,
            fsync_threads: 1,
        }
    }
}

// ---------------------------------------------------------------------------
// Positions, files, frames
// ---------------------------------------------------------------------------

/// Where one frame's bytes sit on THIS node (§0.4, D8).
///
/// Never in an entry, never in a digest, never sent to another node except
/// alongside the file it indexes (I7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position {
    pub bucket: u16,
    pub file_id: u32,
    pub offset: u64,
    pub len: u32,
}

/// One segment file as the store recorded it at a durable point (§6.2 `files`,
/// §11.4 step 2). The input to recovery and the output of a durable point.
///
/// It carries the file's LIVENESS as well as its length, and it has no
/// `Default`: a caller that persists this row persists everything GC decides
/// on, and adding a field breaks every construction site instead of silently
/// zeroing one. The first cut of this WP recorded only `len` and `sealed`, so
/// every sealed file came back from a restart with `retained_frames =
/// window_frames = snapshot_refs = 0` — [`FileMeta::is_dead`] for all of them,
/// and a two-phase GC that unlinked unacked payloads, hash lists still inside
/// the txns window and files hard-linked into a live snapshot. The net I10 and
/// §11.7 depend on was vacuous after every boot; it is these fields that make
/// it hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileState {
    pub bucket: u16,
    pub file_id: u32,
    /// The length apply had made durable.
    pub len: u64,
    /// The length at the last DURABLE point (§11.4), which is at or below
    /// `len`: `len` is what the store's last commit recorded, and a commit is
    /// not a barrier. The frames between the two are the ones §11.5 step 3
    /// verifies at recovery — see [`Segments::recover`].
    pub durable_len: u64,
    /// Sealed files never change again.
    pub sealed: bool,
    /// Frames written into it, ever. Never decreases.
    pub frames: u64,
    /// Frames whose payload retention still keeps, and their bytes (§11.7).
    pub retained_frames: u64,
    pub retained_bytes: u64,
    /// Frames whose hash list is still inside their partition's txns window.
    pub window_frames: u64,
    /// Snapshot manifests that name this file (§11.6).
    pub snapshot_refs: u32,
}

impl FileState {
    /// The row to record for a file the writer holds.
    pub fn of(bucket: u16, file_id: u32, m: &FileMeta) -> FileState {
        FileState {
            bucket,
            file_id,
            len: m.bytes,
            durable_len: m.durable_bytes.min(m.bytes),
            sealed: m.sealed,
            frames: m.frames,
            retained_frames: m.retained_frames,
            retained_bytes: m.retained_bytes,
            window_frames: m.window_frames,
            snapshot_refs: m.snapshot_refs,
        }
    }

    /// The file table entry this row reopens as.
    fn meta(&self) -> FileMeta {
        FileMeta {
            bytes: self.len,
            durable_bytes: self.durable_len.min(self.len),
            sealed: self.sealed,
            frames: self.frames,
            retained_frames: self.retained_frames,
            retained_bytes: self.retained_bytes,
            window_frames: self.window_frames,
            snapshot_refs: self.snapshot_refs,
        }
    }
}

/// One frame, read and verified.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub pid: Pid,
    pub base_offset: u64,
    pub count: u32,
    pub created_at_us: i64,
    /// `16 * count` bytes, frame order (D10, 005).
    pub hashes: Vec<u8>,
    /// The packed, zstd'd frames: the exact bytes `log_segments.blob` holds.
    pub blob: Vec<u8>,
}

/// A frame found by [`Reader::locate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Located {
    pub position: Position,
    pub record: Record,
}

/// What a release retires. A file lives while EITHER of them is above zero
/// (§11.7): the hash lists outlive the segments retention deletes, because the
/// dedup probe (003) and ack-by-hash below the cursor (005) still read them
/// inside the txns window (D10).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Release {
    /// Retention deleted the segment: its payload is no longer readable.
    Retained,
    /// The txns purge passed it: its hash list is no longer needed.
    Window,
    /// Both at once — a partition delete, which takes payload and hashes
    /// together (§5.2 `PartitionDelete`).
    Both,
}

/// The local file table (§6.2 `files`), as the writer keeps it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FileMeta {
    /// Bytes written so far.
    pub bytes: u64,
    /// Bytes at the last durable point.
    pub durable_bytes: u64,
    pub sealed: bool,
    /// Frames written into it, ever.
    pub frames: u64,
    /// Frames whose payload retention still keeps, and their bytes: the "live
    /// bytes per file" of §11.7.
    pub retained_frames: u64,
    pub retained_bytes: u64,
    /// Frames whose hash list is still inside their partition's txns window.
    pub window_frames: u64,
    /// Snapshot manifests that name this file (§11.6 retention).
    pub snapshot_refs: u32,
}

impl FileMeta {
    /// Nothing wants this file's bytes any more (pins are counted separately,
    /// because a reader can hold one without owning any state).
    pub fn is_dead(&self) -> bool {
        self.sealed
            && self.retained_frames == 0
            && self.window_frames == 0
            && self.snapshot_refs == 0
    }
}

/// What recovery did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Recovery {
    /// `(bucket, file, from, to)` — a tail the crash left behind.
    pub truncated: Vec<(u16, u32, u64, u64)>,
    /// Files on disk the reopened state did not know (§11.5 step 3).
    pub deleted: Vec<(u16, u32)>,
    /// Sealed files whose `.qidx` was missing, stale or damaged and was
    /// rebuilt by scanning.
    pub rebuilt: Vec<(u16, u32)>,
    /// `(bucket, file, from, to)` — sealed files whose frames above the last
    /// durable point were checksum-verified without rebuilding the index
    /// (§11.5 step 3).
    pub verified: Vec<(u16, u32, u64, u64)>,
    /// Files this open fsynced because the state recorded bytes above their
    /// durable length: the barrier those verified frames had never had.
    pub synced: u64,
    /// Active files whose RAM index was rebuilt by scanning.
    pub rescanned: Vec<(u16, u32)>,
    pub scanned_frames: u64,
    pub scanned_bytes: u64,
}

/// What a durable point did (§11.4).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DurablePoint {
    /// Every file whose length changed since the last store commit, with the
    /// length to record in this one (I11 step 2).
    pub files: Vec<FileState>,
    pub files_synced: u64,
    pub dirs_synced: u64,
}

/// What one file's scan found.
#[derive(Clone, Debug, Default)]
pub struct Scan {
    pub records: Vec<Record>,
    /// Where the last whole, verified frame ends.
    pub valid_bytes: u64,
    /// The first frame that did not decode or verify, if any.
    pub torn: Option<(u64, FrameError)>,
}

// ---------------------------------------------------------------------------
// Paths and syscalls
// ---------------------------------------------------------------------------

fn bucket_dir(root: &Path, b: u16) -> PathBuf {
    root.join(format!("b{b:03}"))
}

/// Ten digits so the names sort the way the ids do, for the whole u32 range.
fn seg_path(root: &Path, b: u16, id: u32) -> PathBuf {
    bucket_dir(root, b).join(format!("f{id:010}.seg"))
}

fn qidx_path(root: &Path, b: u16, id: u32) -> PathBuf {
    bucket_dir(root, b).join(format!("f{id:010}.qidx"))
}

fn qidx_tmp_path(root: &Path, b: u16, id: u32) -> PathBuf {
    bucket_dir(root, b).join(format!("f{id:010}.qidx.tmp"))
}

/// `f0000000123.seg` → 123. Anything else is not ours.
fn parse_seg_name(name: &str) -> Option<u32> {
    let rest = name.strip_prefix('f')?.strip_suffix(".seg")?;
    rest.parse::<u32>().ok()
}

fn parse_qidx_name(name: &str) -> Option<u32> {
    let rest = name.strip_prefix('f')?.strip_suffix(".qidx")?;
    rest.parse::<u32>().ok()
}

/// The next file id of a bucket. Ids never wrap: at the 64 MiB default a
/// bucket would have to hold 256 EiB to get here, and silently reusing id 0
/// would put new bytes at the offsets old positions still name (I7).
fn next_file_id(current: u32) -> Result<u32> {
    current.checked_add(1).ok_or(SegError::Refused(
        "segment file ids exhausted in this bucket",
    ))
}

fn fsync_fd(fd: RawFd, mode: FsyncMode) -> io::Result<()> {
    let rc = unsafe {
        #[cfg(target_os = "macos")]
        {
            if mode == FsyncMode::Full {
                let r = libc::fcntl(fd, libc::F_FULLFSYNC);
                if r != -1 {
                    return Ok(());
                }
                // F_FULLFSYNC is not supported on every file system; fall
                // through to fsync rather than failing the durable point.
            }
            libc::fsync(fd)
        }
        #[cfg(not(target_os = "macos"))]
        {
            if mode == FsyncMode::Full {
                libc::fsync(fd)
            } else {
                libc::fdatasync(fd)
            }
        }
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn fsync_dir(path: &Path, mode: FsyncMode) -> io::Result<()> {
    let d = File::open(path)?;
    fsync_fd(d.as_raw_fd(), mode)
}

/// Create one directory, saying whether it had to. `create_dir_all` cannot
/// tell the difference, and the difference is what decides whether the parent
/// still owes an fsync (§11.4 step 1: "and their directories").
fn create_dir_reporting(path: &Path) -> io::Result<bool> {
    match std::fs::create_dir(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// The shared read side
// ---------------------------------------------------------------------------

/// Sealed files whose index readers are still served out of RAM; see
/// `Shared::sealed_recent`.
type SealedRecent = BTreeMap<(u16, u32), Arc<Vec<Record>>>;

/// A sealed file's index with the segment length it indexes: what
/// `Segments::write_qidx` needs, and what an owed index keeps until it lands.
type OwedIndex = (u64, Arc<Vec<Record>>);

struct Caches {
    readers: HashMap<(u16, u32), Arc<File>>,
    reader_order: VecDeque<(u16, u32)>,
    indexes: HashMap<(u16, u32), Arc<index::View>>,
    index_order: VecDeque<(u16, u32)>,
}

struct Shared {
    root: PathBuf,
    /// The index of every bucket's ACTIVE file. One lock, not 256 (§3.5).
    active: RwLock<index::ActiveIndexes>,
    /// The index of a file that has JUST been sealed and that the caller may
    /// not have recorded yet.
    ///
    /// A roll moves a bucket's frames out of `active` the moment it seals the
    /// file. The store's `partition_files` row for that file is written by the
    /// caller in ITS next commit, which a reader sees up to
    /// `QUEEN_RAFT_STORE_COMMIT_MS` (4) later (§11.3). Between the two a
    /// reader would find the frame in neither place and answer "not here" for
    /// a message the cluster has committed — a pop payload read that fails for
    /// no reason, timing-dependent, on the hot path. Keeping the sealed index
    /// here until the caller says it has recorded the file
    /// ([`Segments::forget_sealed`], or the SECOND durable point after the
    /// seal as a backstop) closes the window. Not the first: that point's own
    /// store commit has not happened when [`Segments::durable_point`] returns.
    sealed_recent: RwLock<SealedRecent>,
    /// The local file table (§6.2). Readers consult it before opening a file
    /// by name: a dropped file must fail as "gone", never be silently
    /// reopened (pgless's reader-cache lesson).
    files: RwLock<BTreeMap<(u16, u32), FileMeta>>,
    /// Claim pins (§11.7): a committed pop pins the files holding its claimed
    /// segments until the payloads have been read.
    pins: Mutex<HashMap<(u16, u32), u32>>,
    caches: Mutex<Caches>,
    /// Counters, for §14.1's metrics and for the tests.
    reads: AtomicU64,
    read_bytes: AtomicU64,
    index_opens: AtomicU64,
}

/// The read side, cloneable and `Send + Sync`: what a blocking-pool task uses
/// to serve a pop payload while the apply thread keeps writing.
#[derive(Clone)]
pub struct Reader(Arc<Shared>);

/// A file held open against GC (§11.7). Dropping it releases the hold.
pub struct Pin {
    shared: Arc<Shared>,
    key: (u16, u32),
}

impl Drop for Pin {
    fn drop(&mut self) {
        let mut g = self.shared.pins.lock().expect("segment pins poisoned");
        if let Some(n) = g.get_mut(&self.key) {
            *n -= 1;
            if *n == 0 {
                g.remove(&self.key);
            }
        }
    }
}

impl std::fmt::Debug for Pin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pin(b{:03}/f{})", self.key.0, self.key.1)
    }
}

impl Shared {
    fn known(&self, key: (u16, u32)) -> bool {
        self.files
            .read()
            .expect("segment files poisoned")
            .contains_key(&key)
    }

    fn reader_for(&self, key: (u16, u32)) -> Result<Arc<File>> {
        {
            let g = self.caches.lock().expect("segment caches poisoned");
            if let Some(f) = g.readers.get(&key) {
                return Ok(f.clone());
            }
        }
        if !self.known(key) {
            return Err(SegError::MissingFile {
                bucket: key.0,
                file_id: key.1,
            });
        }
        let f = Arc::new(File::open(seg_path(&self.root, key.0, key.1))?);
        let mut g = self.caches.lock().expect("segment caches poisoned");
        if g.readers.len() >= CACHE_MAX {
            if let Some(old) = g.reader_order.pop_front() {
                g.readers.remove(&old);
            }
        }
        g.readers.insert(key, f.clone());
        g.reader_order.push_back(key);
        Ok(f)
    }

    fn index_for(&self, key: (u16, u32)) -> Result<Option<Arc<index::View>>> {
        {
            let g = self.caches.lock().expect("segment caches poisoned");
            if let Some(v) = g.indexes.get(&key) {
                return Ok(Some(v.clone()));
            }
        }
        let path = qidx_path(&self.root, key.0, key.1);
        let view = match index::View::open(&path, None) {
            Ok(v) => v,
            // A file GC unlinked between the caller reading `partition_files`
            // and this probe: not an error, just an answer of "not here".
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(SegError::Io(e)),
        };
        view.check_identity(key.0, key.1)?;
        let view = Arc::new(view);
        self.index_opens.fetch_add(1, Ordering::Relaxed);
        let mut g = self.caches.lock().expect("segment caches poisoned");
        if g.indexes.len() >= CACHE_MAX {
            if let Some(old) = g.index_order.pop_front() {
                g.indexes.remove(&old);
            }
        }
        g.indexes.insert(key, view.clone());
        g.index_order.push_back(key);
        Ok(Some(view))
    }

    fn evict(&self, key: (u16, u32)) {
        let mut g = self.caches.lock().expect("segment caches poisoned");
        g.readers.remove(&key);
        g.reader_order.retain(|k| *k != key);
        g.indexes.remove(&key);
        g.index_order.retain(|k| *k != key);
    }

    /// Read the bytes at a position and VERIFY them (§11.2). Every read of a
    /// segment file in this module goes through here.
    ///
    /// The position's `len` must be exactly the frame's own length. The
    /// checksum covers the frame and nothing else, so a position that is too
    /// long verifies happily and leaves bytes of the NEXT frame in the buffer
    /// — which [`Shared::read_blob`] would have handed to a client as payload.
    /// `len` is therefore checked against what the frame declares, and the
    /// answer is [`SegError::LenMismatch`], not a silently longer blob.
    fn read_verified(&self, pos: Position, dl: Option<Instant>) -> Result<Vec<u8>> {
        let key = (pos.bucket, pos.file_id);
        check_deadline(dl, "the segment file could be opened")?;
        let f = self.reader_for(key)?;
        if (pos.len as usize) < frame::HEADER_LEN {
            return Err(SegError::NoSuchPosition(pos));
        }
        check_deadline(dl, "the frame could be read")?;
        let mut buf = vec![0u8; pos.len as usize];
        f.read_exact_at(&mut buf, pos.offset)
            .map_err(|e| match e.kind() {
                io::ErrorKind::UnexpectedEof => SegError::NoSuchPosition(pos),
                _ => SegError::Io(e),
            })?;
        // `decode` parses, bounds-checks and checksums. Its error is the
        // caller's evidence, so it is reported with the position, not swallowed.
        let fr = frame::decode(&buf).map_err(|why| SegError::Damaged {
            bucket: pos.bucket,
            file_id: pos.file_id,
            offset: pos.offset,
            why,
        })?;
        let frame_len = fr.header.frame_len();
        if frame_len != pos.len as usize {
            return Err(SegError::LenMismatch {
                position: pos,
                frame_len: frame_len as u32,
            });
        }
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.read_bytes.fetch_add(pos.len as u64, Ordering::Relaxed);
        Ok(buf)
    }

    fn read(&self, pos: Position, dl: Option<Instant>) -> Result<Frame> {
        let buf = self.read_verified(pos, dl)?;
        let fr = frame::decode(&buf).expect("verified above");
        Ok(Frame {
            pid: fr.header.pid,
            base_offset: fr.header.base_offset,
            count: fr.header.count,
            created_at_us: fr.header.created_at_us,
            hashes: fr.hashes.to_vec(),
            blob: fr.blob.to_vec(),
        })
    }

    /// The payload only: what a pop answer needs, with the hash list left on
    /// disk instead of copied into RAM.
    fn read_blob(&self, pos: Position, dl: Option<Instant>) -> Result<Vec<u8>> {
        let mut buf = self.read_verified(pos, dl)?;
        let fr = frame::decode(&buf).expect("verified above");
        // Bounded at BOTH ends by the frame itself, never by `pos.len`:
        // `read_verified` has already refused a position whose length is not
        // the frame's, and this keeps the blob right even if it ever stops.
        let total = fr.header.frame_len();
        let from = frame::HEADER_LEN + fr.header.hashes_len();
        buf.truncate(total);
        buf.drain(..from);
        Ok(buf)
    }

    /// `(pid, offset)` → position (§6.1).
    ///
    /// `sealed_files` is the partition's file list, ascending, as the caller
    /// read it from the store's `partition_files` keyspace: this module never
    /// touches the store. The active file's RAM index is consulted first
    /// because that is where a just-applied frame is, and because it costs no
    /// syscall; the sealed files are then BINARY-SEARCHED, each probe one
    /// mapped binary search of that file's `.qidx`.
    fn locate(
        &self,
        bucket: u16,
        pid: Pid,
        offset: u64,
        sealed_files: &[u32],
        dl: Option<Instant>,
    ) -> Result<Option<Located>> {
        if let Some((file_id, rec)) = self
            .active
            .read()
            .expect("segment active index poisoned")
            .probe(bucket, pid, offset)
        {
            return Ok(Some(located(bucket, file_id, rec)));
        }
        {
            // Newest first: a file sealed and not yet recorded by the caller.
            let g = self
                .sealed_recent
                .read()
                .expect("segment sealed index poisoned");
            for ((_, file_id), recs) in g.range((bucket, 0)..=(bucket, u32::MAX)).rev() {
                if let Some(rec) = probe_records(recs, pid, offset) {
                    return Ok(Some(located(bucket, *file_id, rec)));
                }
            }
        }
        let mut lo = 0usize;
        let mut hi = sealed_files.len();
        let mut fell_back = false;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let id = sealed_files[mid];
            // One check per probe: a probe can open and checksum a `.qidx`, so
            // on a slow disk the O(log n) walk is where a caller's budget goes.
            check_deadline(dl, "the partition's files could be probed")?;
            match self.probe_file(bucket, id, pid, offset)? {
                index::Probe::Hit(rec) => return Ok(Some(located(bucket, id, rec))),
                index::Probe::Hole => return Ok(None),
                index::Probe::Before => hi = mid,
                index::Probe::After => lo = mid + 1,
                index::Probe::Missing => {
                    // The list should hold only files with data of this
                    // partition. It does not, so the ordering the binary
                    // search assumes does not hold: answer by scanning.
                    fell_back = true;
                    break;
                }
            }
        }
        if !fell_back {
            return Ok(None);
        }
        for id in sealed_files.iter().rev() {
            check_deadline(dl, "the partition's files could be scanned")?;
            if let index::Probe::Hit(rec) = self.probe_file(bucket, *id, pid, offset)? {
                return Ok(Some(located(bucket, *id, rec)));
            }
        }
        Ok(None)
    }

    fn probe_file(&self, bucket: u16, file_id: u32, pid: Pid, offset: u64) -> Result<index::Probe> {
        match self.index_for((bucket, file_id))? {
            Some(v) => Ok(v.probe(pid, offset)),
            None => Ok(index::Probe::Missing),
        }
    }
}

/// Is the caller's budget already spent (I15)?
///
/// What a deadline can and cannot do here, exactly. It bounds the syscalls a
/// call STARTS: a lookup that would probe five files on a disk serving reads
/// in 200 ms refuses instead of spending a second, and a request whose budget
/// is already gone costs no I/O at all. It cannot interrupt a syscall already
/// issued — a read on a wedged device returns when the kernel says so, and no
/// portable API changes that. The second half of I15 is therefore structural
/// and belongs to the caller: these calls run on the blocking pool, never on a
/// tokio worker, and the caller puts its own timeout on the JOIN, so a stalled
/// disk costs a blocking thread and a retryable answer, not a stalled runtime.
///
/// `Instant::now()` is read only when a deadline was given, so the paths that
/// pass `None` — everything apply touches — read no clock at all (I2).
fn check_deadline(dl: Option<Instant>, what: &'static str) -> Result<()> {
    match dl {
        Some(d) if Instant::now() >= d => Err(SegError::DeadlineExceeded { what }),
        _ => Ok(()),
    }
}

/// The `.qidx` binary search over a sorted slice still in RAM.
fn probe_records(recs: &[Record], pid: Pid, offset: u64) -> Option<Record> {
    let at = recs.partition_point(|r| (r.pid, r.base_offset) <= (pid, offset));
    recs.get(at.checked_sub(1)?)
        .copied()
        .filter(|r| r.holds(pid, offset))
}

fn located(bucket: u16, file_id: u32, rec: Record) -> Located {
    Located {
        position: Position {
            bucket,
            file_id,
            offset: rec.offset,
            len: rec.len,
        },
        record: rec,
    }
}

impl Reader {
    /// Read and verify one frame.
    pub fn read(&self, pos: Position) -> Result<Frame> {
        self.0.read(pos, None)
    }

    /// Read a frame's payload only, without copying its hash list.
    pub fn read_blob(&self, pos: Position) -> Result<Vec<u8>> {
        self.0.read_blob(pos, None)
    }

    /// `(pid, offset)` → position. See [`Shared::locate`].
    pub fn locate(
        &self,
        bucket: u16,
        pid: Pid,
        offset: u64,
        sealed_files: &[u32],
    ) -> Result<Option<Located>> {
        self.0.locate(bucket, pid, offset, sealed_files, None)
    }

    /// Locate and read in one call, cross-checking that the frame on disk is
    /// the one the index promised. A `.qidx` that survived its own checksum
    /// and still points at the wrong frame would otherwise be invisible.
    pub fn read_at(
        &self,
        bucket: u16,
        pid: Pid,
        offset: u64,
        sealed_files: &[u32],
    ) -> Result<Option<Frame>> {
        self.read_at_within(bucket, pid, offset, sealed_files, None)
    }

    /// [`Reader::read`] with the caller's deadline (I15). See
    /// [`check_deadline`] for what a deadline bounds and what it does not.
    pub fn read_within(&self, pos: Position, deadline: Option<Instant>) -> Result<Frame> {
        self.0.read(pos, deadline)
    }

    /// [`Reader::read_blob`] with the caller's deadline (I15).
    pub fn read_blob_within(&self, pos: Position, deadline: Option<Instant>) -> Result<Vec<u8>> {
        self.0.read_blob(pos, deadline)
    }

    /// [`Reader::locate`] with the caller's deadline (I15).
    pub fn locate_within(
        &self,
        bucket: u16,
        pid: Pid,
        offset: u64,
        sealed_files: &[u32],
        deadline: Option<Instant>,
    ) -> Result<Option<Located>> {
        self.0.locate(bucket, pid, offset, sealed_files, deadline)
    }

    /// [`Reader::read_at`] with the caller's deadline (I15): the call a pop
    /// payload read makes on the blocking pool, budget and all.
    pub fn read_at_within(
        &self,
        bucket: u16,
        pid: Pid,
        offset: u64,
        sealed_files: &[u32],
        deadline: Option<Instant>,
    ) -> Result<Option<Frame>> {
        let Some(l) = self.0.locate(bucket, pid, offset, sealed_files, deadline)? else {
            return Ok(None);
        };
        let f = self.0.read(l.position, deadline)?;
        if f.pid != l.record.pid || f.base_offset != l.record.base_offset {
            return Err(SegError::IndexMismatch {
                position: l.position,
                want: (l.record.pid, l.record.base_offset),
                got: (f.pid, f.base_offset),
            });
        }
        Ok(Some(f))
    }

    /// Hold a file against GC until the returned [`Pin`] is dropped (§11.7): a
    /// committed pop claim pins the files holding its claimed segments so
    /// retention cannot unlink the bytes between the claim's apply and the
    /// payload read (I4).
    ///
    /// `None` means the file is not this node's to hold — it was already
    /// unlinked, or never existed — and the caller must fall back (§7.5's
    /// `PayloadRead` from another node), never read.
    ///
    /// The answer is decided under the SAME lock [`Segments::unlink`] holds
    /// from its pin check to the removal of the file's row, so the two cannot
    /// both win. Inserting unconditionally, as the first cut did, granted pins
    /// for files an unlink had already passed the check on: the pin said the
    /// bytes were held, and the read that followed answered `MissingFile` —
    /// exactly the guarantee §11.7 asks the pin for.
    pub fn pin(&self, bucket: u16, file_id: u32) -> Option<Pin> {
        let key = (bucket, file_id);
        let mut pins = self.0.pins.lock().expect("segment pins poisoned");
        if !self
            .0
            .files
            .read()
            .expect("segment files poisoned")
            .contains_key(&key)
        {
            return None;
        }
        *pins.entry(key).or_insert(0) += 1;
        Some(Pin {
            shared: self.0.clone(),
            key,
        })
    }

    /// Does this node still hold the file?
    pub fn has_file(&self, bucket: u16, file_id: u32) -> bool {
        self.0.known((bucket, file_id))
    }

    /// `(frames read, bytes read, indexes opened)`.
    pub fn counters(&self) -> (u64, u64, u64) {
        (
            self.0.reads.load(Ordering::Relaxed),
            self.0.read_bytes.load(Ordering::Relaxed),
            self.0.index_opens.load(Ordering::Relaxed),
        )
    }
}

// ---------------------------------------------------------------------------
// The writer
// ---------------------------------------------------------------------------

struct Active {
    file_id: u32,
    file: File,
    len: u64,
    /// Written since the last durable point.
    dirty: bool,
}

/// The apply thread's handle: the only writer of the segment files (I1).
pub struct Segments {
    shared: Arc<Shared>,
    opts: Options,
    /// One active file per bucket. `None` between a seal and the create that
    /// follows it — and, when that create FAILED (ENOSPC, EMFILE, a name the
    /// disk already holds), until a later call manages to make the file. See
    /// `pending_next`.
    active: Vec<Option<Active>>,
    /// The id of the active file a bucket still owes its create, set before
    /// [`Segments::create_active`] tries and cleared when it succeeds.
    ///
    /// A failed create inside [`Segments::roll`] leaves the bucket with no
    /// active file; the roll reports the error, but the NEXT append must not
    /// find a hole. Without this the next append read `cur_len = 0`, skipped
    /// the roll guard and unwrapped `None`, so a transient ENOSPC during a
    /// roll (§11.8's 507 `storage_full` case) aborted the apply thread
    /// mid-entry instead of returning an error the caller can answer.
    pending_next: Vec<Option<u32>>,
    /// Sealed since the last durable point, kept open so the sync needs no
    /// reopen: `(bucket, file_id, .seg if it still has unsynced bytes, .qidx)`.
    /// A sealed file whose bytes were already fsynced is synced ONCE, as
    /// §11.4 step 1 says, but its brand-new `.qidx` still needs the barrier.
    sealed_dirty: Vec<(u16, u32, Option<File>, Option<File>)>,
    /// Seals handed to the caller by the PREVIOUS durable point, retired by
    /// the next one. See [`Segments::durable_point`] and `Shared::sealed_recent`.
    sealed_staged: Vec<(u16, u32)>,
    /// Sealed files whose `.qidx` write failed, with the length it indexes and
    /// the records to write: `(bucket, file id) → (file bytes, records)`.
    ///
    /// The records are the same `Arc` the readers of `Shared::sealed_recent`
    /// hold, so owing an index costs no second copy. While a file is here its
    /// RAM index is never retired — it is the ONLY way to find a frame in that
    /// file — and every durable point tries the write again.
    qidx_owed: BTreeMap<(u16, u32), OwedIndex>,
    /// Bucket directories that gained or lost a file since the last durable
    /// point.
    dirty_dirs: BTreeSet<u16>,
    /// Files whose length changed since the caller last drained them: what
    /// every apply store commit records (I11).
    touched: BTreeSet<(u16, u32)>,
    /// Files that have GROWN since the last durable point — the ones whose
    /// `durable_bytes` that point advances.
    ///
    /// Kept apart from `touched`, which a plain store commit drains every 4 ms
    /// (§11.3), and maintained for the same reason as `dead`: the first cut
    /// walked the WHOLE file table at every durable point looking for lengths
    /// to advance, which is periodic work proportional to stored data (I8) —
    /// one pass per second over a table that grows with retained bytes /
    /// `QUEEN_RAFT_SEGMENT_BYTES`.
    written_since_durable: BTreeSet<(u16, u32)>,
    /// Bytes appended since the last durable point — the
    /// `QUEEN_RAFT_DURABLE_EVERY_BYTES` side of §11.4, counted here so the
    /// caller does not have to.
    unsynced_bytes: u64,
    /// Directory barriers [`Segments::recover`] issued for the directories it
    /// created, for the test that proves it did.
    dir_syncs_at_open: u64,
    /// Segment files [`Segments::recover`] fsynced because the reopened state
    /// recorded bytes above their durable length (step 7), for the test that
    /// proves the barrier was issued.
    files_synced_at_open: u64,
    /// File table entries the durable points have examined to advance durable
    /// lengths, ever. The same instrument as [`Segments::gc_examined`], for
    /// the other loop that used to walk the whole table (I8).
    durable_examined: u64,
    /// Releases that hit zero and had nothing left to subtract: a claim
    /// retired twice, which leaves the file table's retained figures BELOW the
    /// truth. Counted rather than asserted so a test can insist on zero
    /// (§11.7 feeds compaction from those figures).
    saturated_releases: u64,
    /// The files [`FileMeta::is_dead`] is true of, maintained INCREMENTALLY by
    /// [`Segments::track_liveness`] at every mutation of the file table.
    ///
    /// I8: no periodic work proportional to stored data. [`Segments::gc_pass`]'s
    /// caller runs after every applied entry and on every idle tick, and the
    /// first cut answered it by walking the whole file table and allocating a
    /// vector of every dead file — a per-entry cost that grows with retained
    /// bytes / `QUEEN_RAFT_SEGMENT_BYTES`, which is exactly what the flatness
    /// test of §13.6 measures. A file crosses into this set when a release, a
    /// seal or a dropped snapshot reference empties it, and out of it when an
    /// append or a snapshot reference revives it, so the cost of finding
    /// candidates follows the change and not the store.
    dead: BTreeSet<(u16, u32)>,
    /// File table entries [`Segments::gc_candidates`] has looked at, ever. The
    /// instrument behind the I8 claim above: it must follow the number of
    /// candidates asked for, never the number of files this node holds.
    gc_examined: u64,
    buf: Vec<u8>,
}

impl std::fmt::Debug for Segments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let files = self.shared.files.read().map(|g| g.len()).unwrap_or(0);
        write!(
            f,
            "Segments({}, {files} files, {} active index records)",
            self.shared.root.display(),
            self.active_index_len()
        )
    }
}

impl Segments {
    /// Open the segment tree under `root` (`<state dir>/seg`), reconciling it
    /// with the file table the store reopened with (§11.5 steps 1–3).
    ///
    /// `recorded` is the authority. Files it does not name are deleted; files
    /// longer than it says are truncated; a file SHORTER than it says, or a
    /// frame below that length that does not verify, is the I11 disagreement
    /// and comes back as [`SegError::is_disagreement`].
    ///
    /// A fresh node passes an empty slice against an empty directory. An empty
    /// slice against a NON-empty directory means exactly what it says — the
    /// state knows no files — and the leftovers are deleted.
    ///
    /// The LIVENESS in `recorded` is restored as it stands: it is what decides
    /// whether a file may ever be unlinked (I10), so a caller that does not
    /// persist it hands this node a tree in which every sealed file is dead.
    /// Two shapes of that mistake are refused here rather than acted on: a
    /// non-empty sealed file that claims to have held no frame at all, and a
    /// file that claims more live frames than it ever held.
    pub fn open(
        root: &Path,
        opts: Options,
        recorded: &[FileState],
    ) -> Result<(Segments, Recovery)> {
        let root_is_new = !root.exists();
        std::fs::create_dir_all(root)?;
        let mut files = BTreeMap::new();
        for r in recorded {
            if r.len > 0 && r.sealed && r.frames == 0 {
                return Err(SegError::Refused(
                    "a sealed segment file recorded with no frames: the caller did not persist the file table's liveness (§6.2, I10)",
                ));
            }
            if r.retained_frames > r.frames || r.window_frames > r.frames {
                return Err(SegError::Refused(
                    "a recorded segment file claims more live frames than it ever held",
                ));
            }
            files.insert((r.bucket, r.file_id), r.meta());
        }
        let shared = Arc::new(Shared {
            root: root.to_path_buf(),
            active: RwLock::new(index::ActiveIndexes::new(NBUCKETS)),
            sealed_recent: RwLock::new(BTreeMap::new()),
            files: RwLock::new(files),
            pins: Mutex::new(HashMap::new()),
            caches: Mutex::new(Caches {
                readers: HashMap::new(),
                reader_order: VecDeque::new(),
                indexes: HashMap::new(),
                index_order: VecDeque::new(),
            }),
            reads: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
            index_opens: AtomicU64::new(0),
        });
        let mut segs = Segments {
            shared,
            opts,
            active: (0..NBUCKETS).map(|_| None).collect(),
            pending_next: (0..NBUCKETS).map(|_| None).collect(),
            sealed_dirty: Vec::new(),
            sealed_staged: Vec::new(),
            qidx_owed: BTreeMap::new(),
            dirty_dirs: BTreeSet::new(),
            touched: BTreeSet::new(),
            written_since_durable: BTreeSet::new(),
            unsynced_bytes: 0,
            dir_syncs_at_open: 0,
            files_synced_at_open: 0,
            durable_examined: 0,
            saturated_releases: 0,
            dead: BTreeSet::new(),
            gc_examined: 0,
            buf: Vec::with_capacity(1 << 16),
        };
        let rec = segs.recover(root_is_new)?;
        // The dead set is seeded ONCE, here, from the table the store reopened
        // with and whatever recovery made of it: after this it is maintained
        // by the mutations themselves, so no later call walks the table.
        segs.dead = segs
            .shared
            .files
            .read()
            .expect("segment files poisoned")
            .iter()
            .filter(|(_, m)| m.is_dead())
            .map(|(k, _)| *k)
            .collect();
        Ok((segs, rec))
    }

    /// Directory barriers this open issued (§11.4 step 1 for the directories
    /// it created). Zero on an open that created nothing.
    pub fn dir_syncs_at_open(&self) -> u64 {
        self.dir_syncs_at_open
    }

    /// Segment files this open fsynced because the state recorded bytes above
    /// their durable length (§11.5 step 7). Zero on a clean start.
    pub fn files_synced_at_open(&self) -> u64 {
        self.files_synced_at_open
    }

    /// File table entries the durable points have examined, ever (§11.4).
    ///
    /// It must follow the files that GREW since each point, never the files
    /// this node holds: a durable point that walks the table is periodic work
    /// proportional to stored data, which is what I8 forbids.
    pub fn durable_examined(&self) -> u64 {
        self.durable_examined
    }

    /// The read side, for the blocking pool.
    pub fn reader(&self) -> Reader {
        Reader(self.shared.clone())
    }

    pub fn options(&self) -> Options {
        self.opts
    }

    pub fn root(&self) -> &Path {
        &self.shared.root
    }

    // -- recovery ---------------------------------------------------------

    fn recover(&mut self, root_is_new: bool) -> Result<Recovery> {
        let mut rep = Recovery::default();
        let root = self.shared.root.clone();
        let recorded: Vec<((u16, u32), FileMeta)> = self
            .shared
            .files
            .read()
            .expect("segment files poisoned")
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect();

        // 1. Every bucket directory exists — and its NAME is durable before a
        //    single frame goes into it.
        //
        //    A durable point fsyncs `seg/b0NN`, which makes the entries INSIDE
        //    it durable, and nothing ever fsynced `seg/` itself. So a fresh
        //    node could create its 256 directories, append, take a durable
        //    point, have the store commit durably — and, after a power cut
        //    before the file system committed the directory creations, come
        //    back without `seg/b009`: the store names a file that is not
        //    reachable, recovery step 4 answers `MissingFile`,
        //    `is_disagreement()` is true, and §11.5 discards a live state
        //    directory over an ordinary crash. ext4's journal usually orders
        //    this away; XFS and btrfs promise nothing. Directories are created
        //    only here, so one barrier per created level, at boot, closes it
        //    for good.
        let mut made_bucket_dir = false;
        for b in 0..NBUCKETS as u16 {
            made_bucket_dir |= create_dir_reporting(&bucket_dir(&root, b))?;
        }
        if made_bucket_dir || root_is_new {
            fsync_dir(&root, self.opts.fsync)?;
            self.dir_syncs_at_open += 1;
        }
        if root_is_new {
            // The tree's own name, for the same reason: `seg/` is created by
            // `open`, inside the state directory.
            if let Some(parent) = root.parent() {
                if parent.is_dir() {
                    fsync_dir(parent, self.opts.fsync)?;
                    self.dir_syncs_at_open += 1;
                }
            }
        }

        // 2. What is on disk. Index files are enumerated too: a crash between
        //    unlinking a `.seg` and its `.qidx` (§11.7) leaves an orphan
        //    index, which nothing would ever remove otherwise.
        let mut on_disk: BTreeSet<(u16, u32)> = BTreeSet::new();
        for b in 0..NBUCKETS as u16 {
            for ent in std::fs::read_dir(bucket_dir(&root, b))?.flatten() {
                let name = ent.file_name();
                let Some(name) = name.to_str() else { continue };
                if let Some(id) = parse_seg_name(name).or_else(|| parse_qidx_name(name)) {
                    on_disk.insert((b, id));
                }
            }
        }

        // 3. Files the state does not know: leftovers of a crash between a
        //    roll and the durable point that would have recorded it (§11.5).
        let known: BTreeSet<(u16, u32)> = recorded.iter().map(|(k, _)| *k).collect();
        for key in on_disk.difference(&known) {
            tracing::warn!(
                target: "rsm",
                bucket = key.0, file = key.1,
                "segment file the reopened state does not know: deleting",
            );
            let _ = std::fs::remove_file(seg_path(&root, key.0, key.1));
            let _ = std::fs::remove_file(qidx_path(&root, key.0, key.1));
            let _ = std::fs::remove_file(qidx_tmp_path(&root, key.0, key.1));
            self.dirty_dirs.insert(key.0);
            rep.deleted.push(*key);
        }

        // 4. Every recorded file: present, and exactly as long as recorded.
        for (key, meta) in &recorded {
            let (b, id) = *key;
            let path = seg_path(&root, b, id);
            let md = match std::fs::metadata(&path) {
                Ok(m) => m,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    return Err(SegError::MissingFile {
                        bucket: b,
                        file_id: id,
                    })
                }
                Err(e) => return Err(SegError::Io(e)),
            };
            let on = md.len();
            if on < meta.bytes {
                return Err(SegError::ShortFile {
                    bucket: b,
                    file_id: id,
                    on_disk: on,
                    recorded: meta.bytes,
                });
            }
            if on > meta.bytes {
                tracing::warn!(
                    target: "rsm",
                    bucket = b, file = id, from = on, to = meta.bytes,
                    "truncating a segment file to the length the store recorded",
                );
                let f = OpenOptions::new().write(true).open(&path)?;
                f.set_len(meta.bytes)?;
                fsync_fd(f.as_raw_fd(), self.opts.fsync)?;
                rep.truncated.push((b, id, on, meta.bytes));
            }
        }

        // 5. Sealed files need a valid index; active files need their RAM
        //    index, and must not keep a `.qidx` written before the crash.
        for (key, meta) in &recorded {
            let (b, id) = *key;
            if meta.sealed {
                let ok = match index::View::open(&qidx_path(&root, b, id), Some(meta.bytes)) {
                    Ok(v) => v.check_identity(b, id).is_ok(),
                    Err(_) => false,
                };
                if ok && meta.durable_bytes < meta.bytes {
                    // §11.5 step 3: the frames this file gained since its last
                    // DURABLE point are the ones no barrier has ever covered,
                    // so they are the ones to verify — a valid `.qidx` says
                    // nothing about them (it is written at the seal and
                    // fsynced at the point that follows it, so it can be whole
                    // while the bytes it indexes are not).
                    //
                    // Below `durable_bytes` nothing is scanned, which is what
                    // makes recovery cost proportional to what changed (I8).
                    // The two crash modes are complementary, and that is why
                    // this bound is sound: a PROCESS crash can leave the store
                    // reopening past its last durable commit (MDB_NOSYNC),
                    // but the page cache still holds the file bytes; a POWER
                    // loss can lose unsynced bytes, and then the store comes
                    // back AT its durable commit, so `durable_bytes` is the
                    // real barrier. Neither leaves an unverified frame above
                    // the recorded `durable_bytes`.
                    let scan = self.scan_range(b, id, meta.durable_bytes, meta.bytes)?;
                    if let Some((at, why)) = scan.torn {
                        return Err(SegError::Damaged {
                            bucket: b,
                            file_id: id,
                            offset: at,
                            why,
                        });
                    }
                    rep.scanned_frames += scan.records.len() as u64;
                    rep.scanned_bytes += scan.valid_bytes - meta.durable_bytes;
                    rep.verified.push((b, id, meta.durable_bytes, meta.bytes));
                }
                if !ok {
                    let mut scan = self.scan(b, id, meta.bytes)?;
                    if let Some((at, why)) = scan.torn {
                        return Err(SegError::Damaged {
                            bucket: b,
                            file_id: id,
                            offset: at,
                            why,
                        });
                    }
                    rep.scanned_frames += scan.records.len() as u64;
                    rep.scanned_bytes += scan.valid_bytes;
                    // The handle is dropped: a rebuilt index is not pushed
                    // into `sealed_dirty`, so it is not fsynced here. It does
                    // not need to be — a crash before the next durable point
                    // simply rebuilds it again, from bytes that have not
                    // changed. Nothing reads it in between.
                    //
                    // A scan collects records in FILE order; the file is
                    // written in `(pid, base_offset)` order.
                    index::sort_records(&mut scan.records);
                    self.write_qidx(b, id, meta.bytes, &scan.records)?;
                    rep.rebuilt.push((b, id));
                }
            } else {
                // A `.qidx` beside an ACTIVE file is stale by construction: it
                // was written at a seal the durable point never recorded.
                let _ = std::fs::remove_file(qidx_path(&root, b, id));
                let _ = std::fs::remove_file(qidx_tmp_path(&root, b, id));
                let scan = self.scan(b, id, meta.bytes)?;
                if let Some((at, why)) = scan.torn {
                    return Err(SegError::Damaged {
                        bucket: b,
                        file_id: id,
                        offset: at,
                        why,
                    });
                }
                rep.scanned_frames += scan.records.len() as u64;
                rep.scanned_bytes += scan.valid_bytes;
                if meta.bytes > 0 {
                    rep.rescanned.push((b, id));
                }
                {
                    let mut ai = self
                        .shared
                        .active
                        .write()
                        .expect("segment active index poisoned");
                    ai.open(b, id);
                    for r in &scan.records {
                        ai.insert(b, *r);
                    }
                }
            }
        }

        // 6. Open (or create) each bucket's active file. `recorded` is sorted
        //    by (bucket, file id), so one cursor walks it — not 256 scans,
        //    which on a node with many files would make the boot quadratic.
        let mut at = 0usize;
        for b in 0..NBUCKETS as u16 {
            let from = at;
            while at < recorded.len() && recorded[at].0 .0 == b {
                at += 1;
            }
            let existing = &recorded[from..at];
            let mut unsealed: Option<(u32, FileMeta)> = None;
            let mut highest: Option<u32> = None;
            for ((_, id), m) in existing {
                highest = Some(highest.map_or(*id, |h: u32| h.max(*id)));
                if !m.sealed {
                    if unsealed.is_some() {
                        return Err(SegError::Refused(
                            "the store records more than one unsealed file in a bucket",
                        ));
                    }
                    unsealed = Some((*id, *m));
                }
            }
            if let Some((id, meta)) = unsealed {
                let file = OpenOptions::new()
                    .read(true)
                    .append(true)
                    .open(seg_path(&root, b, id))?;
                self.active[b as usize] = Some(Active {
                    file_id: id,
                    file,
                    len: meta.bytes,
                    dirty: false,
                });
            } else {
                let next = match highest {
                    Some(h) => next_file_id(h)?,
                    None => 0,
                };
                self.create_active(b, next)?;
            }
        }

        // 7. The frames between a file's recorded DURABLE length and its
        //    recorded length have just been verified (step 5) — and nothing
        //    has ever fsynced them: they are bytes a plain store commit
        //    recorded, which §11.3 deliberately does not make durable.
        //
        //    Issuing the barrier HERE, once, is what lets the next durable
        //    point advance their `durable_bytes` honestly. Advancing it
        //    without the barrier — the first cut did — closes the window
        //    §11.5 step 3 exists for while the bytes are still only in the
        //    page cache: a process crash followed by a power loss inside the
        //    writeback window would lose them, and the boot after it would
        //    not re-verify, because the row would say they were durable.
        //
        //    The cost is the files that were mid-write when the node stopped,
        //    never the files it holds (I8).
        let owed: Vec<(u16, u32)> = recorded
            .iter()
            .filter(|(_, m)| m.durable_bytes < m.bytes)
            .map(|(k, _)| *k)
            .collect();
        if !owed.is_empty() {
            let mut handles: Vec<File> = Vec::with_capacity(owed.len());
            for (b, id) in &owed {
                handles.push(File::open(seg_path(&root, *b, *id))?);
            }
            let fds: Vec<RawFd> = handles.iter().map(|f| f.as_raw_fd()).collect();
            self.fsync_all(&fds)?;
            self.files_synced_at_open = fds.len() as u64;
            let mut g = self.shared.files.write().expect("segment files poisoned");
            for key in &owed {
                if let Some(m) = g.get_mut(key) {
                    m.durable_bytes = m.bytes;
                }
            }
            drop(g);
            // They are durable now, so the rows must say so at the next store
            // commit: a row left claiming a stale durable length would make
            // every later boot re-verify frames a barrier has covered.
            self.touched.extend(owed);
            rep.synced = self.files_synced_at_open;
        }
        Ok(rep)
    }

    fn create_active(&mut self, bucket: u16, file_id: u32) -> Result<()> {
        let path = seg_path(&self.shared.root, bucket, file_id);
        // Recorded BEFORE the syscall: if it fails, the bucket owes this file
        // and `ensure_active` will try the same id again. Trying a different
        // one would put new bytes under a name the state may already know.
        self.pending_next[bucket as usize] = Some(file_id);
        // `create_new`: a collision means the state and the disk disagree
        // about which file is next, which is never a thing to paper over.
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(&path)?;
        self.active[bucket as usize] = Some(Active {
            file_id,
            file,
            len: 0,
            dirty: false,
        });
        self.shared
            .files
            .write()
            .expect("segment files poisoned")
            .insert((bucket, file_id), FileMeta::default());
        // Not sealed, so not dead — and a file id is reused by nobody, but the
        // set is kept honest at every mutation and this is one.
        self.track_liveness((bucket, file_id), false);
        self.shared
            .active
            .write()
            .expect("segment active index poisoned")
            .open(bucket, file_id);
        self.dirty_dirs.insert(bucket);
        self.touched.insert((bucket, file_id));
        self.pending_next[bucket as usize] = None;
        Ok(())
    }

    /// Make sure a bucket has an active file, retrying a create an earlier
    /// call could not finish. Returns the error, never a panic: the apply
    /// thread must be able to answer "no" (§11.8) and carry on.
    fn ensure_active(&mut self, bucket: u16) -> Result<()> {
        if self.active[bucket as usize].is_some() {
            return Ok(());
        }
        match self.pending_next[bucket as usize] {
            Some(id) => self.create_active(bucket, id),
            // Nothing sealed this bucket and nothing owes it a file: the
            // writer's own bookkeeping is wrong, which is not a thing to
            // repair by inventing a file id.
            None => Err(SegError::Refused(
                "the bucket has no active file and none is owed",
            )),
        }
    }

    // -- the write path ---------------------------------------------------

    /// Append one `Append` effect's payload and return where it landed.
    ///
    /// Rolls first when the frame would take the active file past
    /// `segment_bytes`, so a file never overshoots; a frame larger than the
    /// limit goes into a file of its own. No fsync: that is the durable
    /// point's (§11.4).
    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        bucket: u16,
        pid: Pid,
        base_offset: u64,
        count: u32,
        created_at_us: i64,
        hashes: &[u8],
        blob: &[u8],
    ) -> Result<Position> {
        if bucket as usize >= NBUCKETS {
            return Err(SegError::Refused("bucket out of range"));
        }
        self.buf.clear();
        let flen = frame::encode_into(
            &mut self.buf,
            pid,
            base_offset,
            count,
            created_at_us,
            hashes,
            blob,
        )? as u64;

        // A bucket whose create failed in an earlier roll owes a file; make
        // it before deciding anything about lengths, so `cur_len` is the
        // length of a file that exists.
        self.ensure_active(bucket)?;
        let cur_len = self.active[bucket as usize]
            .as_ref()
            .map(|a| a.len)
            .unwrap_or(0);
        if cur_len > 0 && cur_len + flen > self.opts.segment_bytes {
            self.roll(bucket)?;
            // `roll` seals, then creates. If the create failed it returned the
            // error above; this is only belt.
            self.ensure_active(bucket)?;
        }

        let Some(a) = self.active[bucket as usize].as_mut() else {
            return Err(SegError::Refused("the bucket has no active file"));
        };
        let file_id = a.file_id;
        let offset = a.len;
        a.file.write_all(&self.buf)?;
        a.len += flen;
        a.dirty = true;

        let pos = Position {
            bucket,
            file_id,
            offset,
            len: flen as u32,
        };
        let rec = Record {
            pid,
            base_offset,
            end: base_offset.saturating_add(count as u64),
            created_at_us,
            offset,
            count,
            len: flen as u32,
        };
        self.shared
            .active
            .write()
            .expect("segment active index poisoned")
            .insert(bucket, rec);
        let dead;
        {
            let mut g = self.shared.files.write().expect("segment files poisoned");
            let m = g.entry((bucket, file_id)).or_default();
            m.bytes = offset + flen;
            m.frames += 1;
            m.retained_frames += 1;
            m.retained_bytes += flen;
            m.window_frames += 1;
            dead = m.is_dead();
        }
        // A frame revives the file it lands in — it never can, in fact: this is
        // the ACTIVE file, and a file enters the dead set only once sealed.
        self.track_liveness((bucket, file_id), dead);
        self.touched.insert((bucket, file_id));
        // The only place a file's length grows, and therefore the only place
        // the next durable point's work comes from.
        self.written_since_durable.insert((bucket, file_id));
        self.unsynced_bytes += flen;
        // One fat frame must not leave the encode buffer fat for the life of
        // the process: `QUEEN_RAFT_ENTRY_MAX_BYTES` is 96 MiB, and holding
        // that per node for one outlier push is the kind of RAM that does not
        // follow the write rate (I8).
        if self.buf.capacity() > BUF_KEEP_BYTES {
            self.buf = Vec::with_capacity(1 << 16);
        }
        Ok(pos)
    }

    /// Seal a bucket's active file and start a new one.
    ///
    /// Sealing writes the `.qidx` from the RAM index — checksummed, temp file
    /// and rename — and does NOT fsync: the next durable point syncs the
    /// `.seg`, the `.qidx` and the directory together. A crash in between
    /// leaves a `.qidx` for a file the store still calls active, which
    /// recovery deletes and rebuilds (step 5 above).
    ///
    /// Three orderings here are load-bearing, and each of them was a defect
    /// first:
    ///
    /// 1. The sealed copy is installed in `Shared::sealed_recent` while the
    ///    ACTIVE index's write lock is still held. The first cut took the
    ///    records out of `active` and installed them only after `write_qidx`
    ///    had sorted, allocated, written, renamed and reopened a file that is
    ///    ~0.5% of a 64 MiB segment — and for the whole of that a reader found
    ///    the frame in neither index, nor in the caller's last-committed
    ///    `partition_files`, and answered "not here" for a message the cluster
    ///    had committed. The window the `sealed_recent` doc describes was
    ///    reopened by the very code that closes it. Nothing between the take
    ///    and the insert may fail or touch the disk.
    /// 2. A `.qidx` the disk refuses is OWED, not lost. The RAM copy stays in
    ///    `sealed_recent` (it is never retired while the index is owed) and
    ///    the next durable point re-writes it. The first cut returned the
    ///    error with the records already dropped: the bucket's index was gone
    ///    from RAM, absent from disk, and every lookup into that file answered
    ///    `Ok(None)` until a restart rescanned it.
    /// 3. The bucket gets its next active file whatever happened above.
    ///    Returning early left `active[b] = None` AND `pending_next[b] = None`,
    ///    so [`Segments::ensure_active`] refused every later append with "no
    ///    active file and none is owed" — for the life of the process, long
    ///    after the operator had freed the disk.
    pub fn roll(&mut self, bucket: u16) -> Result<()> {
        if bucket as usize >= NBUCKETS {
            return Err(SegError::Refused("bucket out of range"));
        }
        let Some(cur_id) = self.active[bucket as usize].as_ref().map(|a| a.file_id) else {
            return Ok(());
        };
        // Before anything is taken apart: at the end of the id space this is
        // the answer, and the bucket keeps its active file.
        let next_id = next_file_id(cur_id)?;
        let old = self.active[bucket as usize]
            .take()
            .expect("the active file checked just above");

        // (1) The index moves from `active` to `sealed_recent` with no gap a
        //     reader can fall into. `ActiveIndexes` is keyed by
        //     `(pid, base_offset)`, so what comes out is already in the order
        //     `probe_records` binary-searches and `index::encode` writes.
        let records = {
            let mut ai = self
                .shared
                .active
                .write()
                .expect("segment active index poisoned");
            let records = Arc::new(ai.take(bucket));
            self.shared
                .sealed_recent
                .write()
                .expect("segment sealed index poisoned")
                .insert((bucket, old.file_id), records.clone());
            // `ai` is still held here: a reader that missed the active index
            // is looking at a `sealed_recent` that already has the answer.
            records
        };
        let dead;
        {
            let mut g = self.shared.files.write().expect("segment files poisoned");
            let m = g.entry((bucket, old.file_id)).or_default();
            m.bytes = old.len;
            m.sealed = true;
            dead = m.is_dead();
        }
        // Sealing is one of the two ways a file becomes dead: a file whose
        // every frame was released before the roll is collectable the moment
        // it stops being the active one.
        self.track_liveness((bucket, old.file_id), dead);
        self.touched.insert((bucket, old.file_id));
        let seg = if old.dirty { Some(old.file) } else { None };

        // (2) The index write. Its failure is owed, and reported.
        let mut first_err = None;
        let qidx = match self.write_qidx(bucket, old.file_id, old.len, &records) {
            Ok(f) => {
                // §13.5 (R-107) `seg.qidx_written`: the sealed file's `.qidx`
                // is written (temp + rename), not yet fsynced. A crash here
                // leaves a `.qidx` for a file the store still calls active,
                // which recovery deletes and rebuilds (step 5). Not part of the
                // HTTP crash matrix; the segment roll tests arm it.
                crate::rsm::faults::hit("seg.qidx_written");
                Some(f)
            }
            Err(e) => {
                tracing::warn!(
                    target: "rsm",
                    bucket, file = old.file_id, error = %e,
                    "the sealed file's index could not be written: owed to the next durable point",
                );
                self.qidx_owed
                    .insert((bucket, old.file_id), (old.len, records));
                first_err = Some(e);
                None
            }
        };
        self.sealed_dirty.push((bucket, old.file_id, seg, qidx));

        // (3) The successor, owed from before its own syscall (`create_active`).
        if let Err(e) = self.create_active(bucket, next_id) {
            first_err = first_err.or(Some(e));
        }
        // §13.5 (R-107) `seg.rolled`: the old file is sealed and the bucket has
        // a fresh active file. A crash here is ordinary — the seal is recorded
        // at the next store commit, and recovery truncates/rebuilds as needed
        // (I11). Not part of the HTTP crash matrix; the segment roll tests arm
        // it so a kill around a roll is deterministic, not timing-driven.
        crate::rsm::faults::hit("seg.rolled");
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Sealed files whose `.qidx` the disk refused, still owed to it.
    ///
    /// They are re-written at every durable point; until one succeeds the
    /// file's records are served from RAM and are never retired from
    /// `Shared::sealed_recent`. A crash while one is owed is ordinary: the
    /// file is sealed in the store, its index is missing, and recovery step 5
    /// rebuilds it by scanning.
    pub fn owed_indexes(&self) -> Vec<(u16, u32)> {
        self.qidx_owed.keys().copied().collect()
    }

    /// Re-write the `.qidx` files an earlier roll could not, newest attempt
    /// first in the durable point that follows it.
    fn retry_owed_indexes(&mut self) {
        if self.qidx_owed.is_empty() {
            return;
        }
        let owed: Vec<((u16, u32), OwedIndex)> = self
            .qidx_owed
            .iter()
            .map(|(k, (bytes, recs))| (*k, (*bytes, recs.clone())))
            .collect();
        for ((b, id), (bytes, recs)) in owed {
            match self.write_qidx(b, id, bytes, &recs) {
                Ok(f) => {
                    self.qidx_owed.remove(&(b, id));
                    // The `.seg` was synced at the point that followed its
                    // seal; this brand-new index still needs the barrier.
                    self.sealed_dirty.push((b, id, None, Some(f)));
                    tracing::info!(
                        target: "rsm",
                        bucket = b, file = id,
                        "the sealed file's index was written on retry",
                    );
                }
                // Still owed. Not an error for the durable point: the point
                // makes BYTES durable, and a sealed file whose index is
                // missing is rebuilt by scanning at the next boot.
                Err(e) => tracing::warn!(
                    target: "rsm",
                    bucket = b, file = id, error = %e,
                    "the sealed file's index is still owed",
                ),
            }
        }
    }

    /// The caller has recorded this sealed file (its `partition_files` rows
    /// and its length are in a committed store transaction), so readers no
    /// longer need the copy `Shared::sealed_recent` was holding for them.
    ///
    /// Calling it is an optimisation, not a duty: the SECOND durable point
    /// after the seal retires the entry anyway, because by then the caller's
    /// commit for the point that reported the seal has landed (§11.4 step 2).
    /// One point of lag is the price of not trusting a commit that has not
    /// happened yet; a caller that calls this as soon as its own commit
    /// returns pays none of it.
    ///
    /// A file whose `.qidx` is still owed keeps its RAM index whatever the
    /// caller says: the store row the caller committed points at a file whose
    /// index is not on disk, so RAM is the only place the frame can be found
    /// until [`Segments::retry_owed_indexes`] succeeds.
    pub fn forget_sealed(&mut self, bucket: u16, file_id: u32) {
        let key = (bucket, file_id);
        if self.retire_sealed(key) {
            self.sealed_staged.retain(|k| *k != key);
        }
    }

    /// Drop a sealed file's RAM index, unless its `.qidx` is still owed.
    /// Returns whether it went.
    fn retire_sealed(&mut self, key: (u16, u32)) -> bool {
        if self.qidx_owed.contains_key(&key) {
            return false;
        }
        self.shared
            .sealed_recent
            .write()
            .expect("segment sealed index poisoned")
            .remove(&key);
        true
    }

    /// Sealed files whose index readers are still being served out of RAM.
    pub fn unrecorded_seals(&self) -> Vec<(u16, u32)> {
        self.shared
            .sealed_recent
            .read()
            .expect("segment sealed index poisoned")
            .keys()
            .copied()
            .collect()
    }

    /// Seal every bucket's active file: step 1 of a snapshot build (§11.6).
    /// Buckets whose active file is empty are left alone — an empty file is
    /// not worth a seal and would only make a `.qidx` with no records.
    pub fn seal_all(&mut self) -> Result<usize> {
        let mut n = 0;
        for b in 0..NBUCKETS as u16 {
            if self.active[b as usize].as_ref().is_some_and(|a| a.len > 0) {
                self.roll(b)?;
                n += 1;
            }
        }
        Ok(n)
    }

    /// Write `<file>.qidx` from `records`, atomically (temp + rename), and
    /// return the handle so the durable point can fsync it without reopening.
    fn write_qidx(
        &mut self,
        bucket: u16,
        file_id: u32,
        file_bytes: u64,
        records: &[Record],
    ) -> Result<File> {
        let bytes = index::encode(bucket, file_id, file_bytes, records);
        let tmp = qidx_tmp_path(&self.shared.root, bucket, file_id);
        let dst = qidx_path(&self.shared.root, bucket, file_id);
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            f.write_all(&bytes)?;
        }
        std::fs::rename(&tmp, &dst)?;
        self.dirty_dirs.insert(bucket);
        // A view cached from an earlier life of this file id would now be
        // wrong; drop it with the read handle.
        self.shared.evict((bucket, file_id));
        Ok(File::open(&dst)?)
    }

    // -- durable points ---------------------------------------------------

    /// Bytes appended since the last durable point — the
    /// `QUEEN_RAFT_DURABLE_EVERY_BYTES` trigger of §11.4.
    pub fn unsynced_bytes(&self) -> u64 {
        self.unsynced_bytes
    }

    /// Every file whose length OR liveness changed since the last drain, with
    /// the row to record. Called at each apply store commit (I11): "every
    /// apply commit already records the lengths of the files it touched".
    ///
    /// Liveness counts as a change: [`Segments::release`] and
    /// [`Segments::set_snapshot_ref`] mark the file too, because a file table
    /// that persists only lengths comes back from a restart believing every
    /// sealed file is dead (see [`FileState`]).
    pub fn take_touched(&mut self) -> Vec<FileState> {
        let keys = std::mem::take(&mut self.touched);
        let g = self.shared.files.read().expect("segment files poisoned");
        keys.into_iter()
            .filter_map(|(b, id)| g.get(&(b, id)).map(|m| FileState::of(b, id, m)))
            .collect()
    }

    /// The whole file table as rows to record: what a caller writes when it
    /// wants the store to hold this node's complete `files` keyspace (a
    /// snapshot build, a first commit after an import) rather than the delta
    /// [`Segments::take_touched`] gives it.
    pub fn file_states(&self) -> Vec<FileState> {
        self.shared
            .files
            .read()
            .expect("segment files poisoned")
            .iter()
            .map(|((b, id), m)| FileState::of(*b, *id, m))
            .collect()
    }

    /// Step 1 of §11.4: fsync every segment file written since the last
    /// durable point (sealed files once) and their directories, then hand the
    /// caller the lengths to put in the durable store commit.
    ///
    /// The caller writes those lengths and its applied index in ONE store
    /// commit; only then is the point durable (I11). This call does not touch
    /// the store — which is why it does NOT retire the seals it is handing
    /// over (see the comment at the end of this function).
    pub fn durable_point(&mut self) -> Result<DurablePoint> {
        // NOTHING is marked clean before every barrier has returned. A durable
        // point that fails half way must leave the writer believing it still
        // owes those fsyncs, so a retry re-issues them; clearing the dirty
        // sets up front would turn one EIO into bytes that are never synced
        // again and a recovery that finds a file shorter than its recorded
        // length (I11's ShortFile) with nothing to explain it.
        // A `.qidx` an earlier roll could not write is written now, so its new
        // handle joins this point's barriers instead of waiting for another.
        self.retry_owed_indexes();
        let mut fds: Vec<RawFd> = Vec::new();
        for (_, _, seg, qidx) in &self.sealed_dirty {
            if let Some(s) = seg {
                fds.push(s.as_raw_fd());
            }
            if let Some(q) = qidx {
                fds.push(q.as_raw_fd());
            }
        }
        for a in self.active.iter_mut().flatten() {
            if a.dirty {
                a.file.flush()?;
                fds.push(a.file.as_raw_fd());
            }
        }
        let files_synced = fds.len() as u64;
        self.fsync_all(&fds)?;

        let dirs_synced = self.dirty_dirs.len() as u64;
        for b in &self.dirty_dirs {
            fsync_dir(&bucket_dir(&self.shared.root, *b), self.opts.fsync)?;
        }

        // Past here every barrier has returned.
        self.sealed_dirty.clear();
        self.dirty_dirs.clear();
        for a in self.active.iter_mut().flatten() {
            a.dirty = false;
        }

        // The durable length of every file this point SYNCED advances to its
        // length, and each of them is TOUCHED again, so the commit this point
        // is about to take records the new one. That length is the floor
        // recovery verifies from (§11.5 step 3): a stale one makes the next
        // boot re-verify bytes a barrier already covered, and the rows went
        // stale because a plain store commit in between drains the touched
        // set.
        //
        // The set is the files that GREW since the last point — the ones the
        // barriers above covered — and not a walk of the file table, so a
        // point costs what changed and not what this node stores (I8).
        {
            let grown = std::mem::take(&mut self.written_since_durable);
            self.durable_examined += grown.len() as u64;
            {
                let mut g = self.shared.files.write().expect("segment files poisoned");
                for key in &grown {
                    if let Some(m) = g.get_mut(key) {
                        m.durable_bytes = m.bytes;
                    }
                }
            }
            self.touched.extend(grown);
        }
        // Seals are retired ONE durable point late, never at this one.
        //
        // This call is not a store commit and the caller's commit for THIS
        // point has not happened yet: it takes the lengths below, writes them
        // with its applied index, and only then is the point durable (I11).
        // Clearing here would stop serving a just-sealed file's index while
        // its `partition_files` rows were still in an uncommitted transaction
        // — the very window `sealed_recent` exists to close, widened from the
        // 4 ms store-commit cadence to the length of a DURABLE commit (S3
        // measured up to 1227 ms). The seals handed over at the previous point
        // are the ones whose commit has certainly landed, because the caller
        // runs points serially on the apply thread: they are retired now, and
        // this point's seals take their place.
        {
            let staged = std::mem::take(&mut self.sealed_staged);
            let mut g = self
                .shared
                .sealed_recent
                .write()
                .expect("segment sealed index poisoned");
            for key in staged {
                // Not one whose `.qidx` is still owed: RAM is the only copy.
                if !self.qidx_owed.contains_key(&key) {
                    g.remove(&key);
                }
            }
            self.sealed_staged = g.keys().copied().collect();
        }
        self.unsynced_bytes = 0;
        Ok(DurablePoint {
            files: self.take_touched(),
            files_synced,
            dirs_synced,
        })
    }

    fn fsync_all(&self, fds: &[RawFd]) -> Result<()> {
        let mode = self.opts.fsync;
        if self.opts.fsync_threads <= 1 || fds.len() < 8 {
            for fd in fds {
                fsync_fd(*fd, mode)?;
            }
            return Ok(());
        }
        let chunk = fds.len().div_ceil(self.opts.fsync_threads);
        std::thread::scope(|sc| {
            let mut hs = Vec::new();
            for part in fds.chunks(chunk) {
                hs.push(sc.spawn(move || {
                    for fd in part {
                        fsync_fd(*fd, mode)?;
                    }
                    Ok::<(), io::Error>(())
                }));
            }
            for h in hs {
                h.join().expect("segment fsync thread")?;
            }
            Ok::<(), io::Error>(())
        })?;
        Ok(())
    }

    // -- reads (forwarded, so the apply thread need not build a Reader) ----

    pub fn read(&self, pos: Position) -> Result<Frame> {
        self.shared.read(pos, None)
    }

    pub fn read_blob(&self, pos: Position) -> Result<Vec<u8>> {
        self.shared.read_blob(pos, None)
    }

    pub fn locate(
        &self,
        bucket: u16,
        pid: Pid,
        offset: u64,
        sealed_files: &[u32],
    ) -> Result<Option<Located>> {
        self.shared.locate(bucket, pid, offset, sealed_files, None)
    }

    /// Verify the checksums of frames the reopened state references (§11.5
    /// step 3). Returns the positions that did NOT verify; an empty answer is
    /// the pass.
    pub fn verify_positions(&self, positions: &[Position]) -> Vec<(Position, String)> {
        let mut bad = Vec::new();
        for p in positions {
            if let Err(e) = self.shared.read(*p, None) {
                bad.push((*p, e.to_string()));
            }
        }
        bad
    }

    /// Scan a file's frames up to `upto`, verifying each. The engine behind
    /// both "rebuild a `.qidx`" and "is this tail torn".
    pub fn scan(&self, bucket: u16, file_id: u32, upto: u64) -> Result<Scan> {
        self.scan_range(bucket, file_id, 0, upto)
    }

    /// The same, from a frame boundary: §11.5 step 3 verifies the frames a
    /// file gained since its last DURABLE point, and nothing below it.
    ///
    /// `from` must be a boundary — every recorded length is one, because a
    /// frame is appended whole — and `Scan::valid_bytes` is absolute, so a
    /// caller compares it with `upto` exactly as it does for a full scan.
    pub fn scan_range(&self, bucket: u16, file_id: u32, from: u64, upto: u64) -> Result<Scan> {
        let path = seg_path(&self.shared.root, bucket, file_id);
        let f = File::open(&path)?;
        let mut r = BufReader::with_capacity(1 << 16, f);
        if from > 0 {
            r.seek(SeekFrom::Start(from))?;
        }
        let mut out = Scan {
            valid_bytes: from,
            ..Default::default()
        };
        let mut head = [0u8; frame::HEADER_LEN];
        let mut body: Vec<u8> = Vec::new();
        let mut pos = from;
        while pos < upto {
            match read_exact_or_less(&mut r, &mut head)? {
                0 => break,
                n if n < frame::HEADER_LEN => {
                    out.torn = Some((
                        pos,
                        FrameError::Truncated {
                            need: frame::HEADER_LEN,
                            have: n,
                        },
                    ));
                    break;
                }
                _ => {}
            }
            let header = match frame::parse_header(&head) {
                Ok(h) => h,
                Err(e) => {
                    out.torn = Some((pos, e));
                    break;
                }
            };
            let total = header.frame_len();
            if pos + total as u64 > upto {
                out.torn = Some((
                    pos,
                    FrameError::Truncated {
                        need: total,
                        have: (upto - pos) as usize,
                    },
                ));
                break;
            }
            // Grow the buffer, never re-zero it: `resize` on a cleared Vec
            // would write `total` zero bytes for every frame, which on a
            // 64 MiB file is a second full pass over the data and was half
            // the measured scan time.
            if body.len() < total {
                body.resize(total, 0);
            }
            let body = &mut body[..total];
            body[..frame::HEADER_LEN].copy_from_slice(&head);
            let rest = total - frame::HEADER_LEN;
            if read_exact_or_less(&mut r, &mut body[frame::HEADER_LEN..])? < rest {
                out.torn = Some((
                    pos,
                    FrameError::Truncated {
                        need: total,
                        have: frame::HEADER_LEN,
                    },
                ));
                break;
            }
            if let Err(e) = frame::verify(body, &header) {
                out.torn = Some((pos, e));
                break;
            }
            out.records.push(Record::of_frame(&header, pos));
            pos += total as u64;
            out.valid_bytes = pos;
        }
        Ok(out)
    }

    // -- the local file table, GC and pins (§11.7) ------------------------

    pub fn file_meta(&self, bucket: u16, file_id: u32) -> Option<FileMeta> {
        self.shared
            .files
            .read()
            .expect("segment files poisoned")
            .get(&(bucket, file_id))
            .copied()
    }

    /// Every file this node holds, with its meta.
    pub fn files(&self) -> Vec<(u16, u32, FileMeta)> {
        self.shared
            .files
            .read()
            .expect("segment files poisoned")
            .iter()
            .map(|((b, id), m)| (*b, *id, *m))
            .collect()
    }

    /// Retire one frame's claim on its file (§11.7). Retention retires the
    /// payload, the txns purge retires the hash list, and a partition delete
    /// retires both; the file dies only when neither is left.
    ///
    /// The file joins the touched set: this is a change to what the store must
    /// record about it, exactly like a new length, and a restart that did not
    /// see it would bring the retired frames back to life.
    pub fn release(&mut self, pos: Position, what: Release) {
        let mut saturated = false;
        let dead;
        {
            let mut g = self.shared.files.write().expect("segment files poisoned");
            let Some(m) = g.get_mut(&(pos.bucket, pos.file_id)) else {
                return;
            };
            if matches!(what, Release::Retained | Release::Both) {
                saturated |= m.retained_frames == 0;
                m.retained_frames = m.retained_frames.saturating_sub(1);
                m.retained_bytes = m.retained_bytes.saturating_sub(pos.len as u64);
            }
            if matches!(what, Release::Window | Release::Both) {
                saturated |= m.window_frames == 0;
                m.window_frames = m.window_frames.saturating_sub(1);
            }
            dead = m.is_dead();
        }
        if saturated {
            // The caller retired a claim this frame no longer held. The
            // saturation keeps the counters sane, but the figures are now
            // below the truth for every frame of this file that is still
            // live, so it is worth a line and a counter, not silence.
            self.saturated_releases += 1;
            tracing::warn!(
                target: "rsm",
                bucket = pos.bucket, file = pos.file_id, offset = pos.offset,
                what = ?what,
                "a segment claim was released twice",
            );
        }
        // The other way a file becomes dead, and the common one: the last
        // retained frame or the last hash list of a sealed file goes.
        self.track_liveness((pos.bucket, pos.file_id), dead);
        self.touched.insert((pos.bucket, pos.file_id));
    }

    /// How many releases had nothing left to retire (see
    /// [`Segments::release`]).
    ///
    /// Zero on a correct caller, but only a FLOOR on the mistake: a claim
    /// retired twice is invisible here while the counter is still above zero,
    /// and shows up as a file table whose retained figures are below the
    /// truth. The test that falsifies it compares the figures themselves
    /// against two partitions sharing one file.
    pub fn saturated_releases(&self) -> u64 {
        self.saturated_releases
    }

    /// Forget that a file owes the store a row.
    ///
    /// GC phase one (§11.7) deletes a dead file's `files` and
    /// `partition_files` rows and then unlinks it at the next durable point.
    /// Between the two, the file is still in this writer's touched set — every
    /// [`Segments::release`] that emptied it put it there — and the durable
    /// point would hand it back to the caller as a row to RECORD, in the very
    /// commit whose job is to stop naming it (I10). The caller therefore tells
    /// this writer, at phase one, that the file is no longer its business.
    ///
    /// Nothing else is dropped: the file keeps its `FileMeta` and its pins
    /// until [`Segments::unlink`] succeeds, so a pin taken in between still
    /// wins and the caller can put the row back.
    pub fn untouch(&mut self, bucket: u16, file_id: u32) {
        self.touched.remove(&(bucket, file_id));
    }

    /// The partitions whose frames a SEALED file holds, sorted and deduped.
    ///
    /// This is the source of the `partition_files` rows (§6.1's G0 amendment):
    /// the file's own index, never a map this process happens to have built.
    /// A RAM map cannot answer for a file that sealed before a restart, which
    /// is how those rows came to leak — one per partition, per file, for ever
    /// (I8 says the cost of a change is proportional to the change).
    ///
    /// Three sources, in this order: the index of a just-sealed file, still in
    /// RAM; the `.qidx` on disk; and, if neither is there, a scan of the file.
    /// One of them always is: a file whose `.qidx` could not be written keeps
    /// its RAM index until it can ([`Segments::retire_sealed`]), and recovery
    /// rebuilds a missing one at boot.
    pub fn pids_in(&self, bucket: u16, file_id: u32) -> Result<Vec<Pid>> {
        let key = (bucket, file_id);
        let mut pids: Vec<Pid> = Vec::new();
        let recent = self
            .shared
            .sealed_recent
            .read()
            .expect("segment sealed index poisoned")
            .get(&key)
            .cloned();
        if let Some(records) = recent {
            pids.extend(records.iter().map(|r| r.pid));
        } else if let Some(view) = self.shared.index_for(key)? {
            pids.extend(view.records().map(|r| r.pid));
        } else {
            let bytes = self.file_meta(bucket, file_id).map(|m| m.bytes);
            let Some(bytes) = bytes else {
                return Err(SegError::MissingFile { bucket, file_id });
            };
            tracing::warn!(
                target: "rsm",
                bucket, file = file_id,
                "no index for a sealed segment file: scanning it for its partitions",
            );
            pids.extend(
                self.scan(bucket, file_id, bytes)?
                    .records
                    .iter()
                    .map(|r| r.pid),
            );
        }
        pids.sort_unstable();
        pids.dedup();
        Ok(pids)
    }

    /// A snapshot manifest started or stopped naming this file (§11.6).
    ///
    /// Recorded like a length (see [`Segments::release`]): a hard link a
    /// manifest holds must still hold after a restart, or the first GC of the
    /// new process unlinks the file out from under a snapshot that is being
    /// sent.
    pub fn set_snapshot_ref(&mut self, bucket: u16, file_id: u32, held: bool) {
        let dead;
        {
            let mut g = self.shared.files.write().expect("segment files poisoned");
            let Some(m) = g.get_mut(&(bucket, file_id)) else {
                return;
            };
            if held {
                m.snapshot_refs += 1;
            } else {
                m.snapshot_refs = m.snapshot_refs.saturating_sub(1);
            }
            dead = m.is_dead();
        }
        // A manifest revives a dead file and, when it lets go, hands it back
        // to GC.
        self.track_liveness((bucket, file_id), dead);
        self.touched.insert((bucket, file_id));
    }

    /// Keep [`Segments::dead`] in step with one file's liveness.
    ///
    /// Called after every mutation of a `FileMeta`, with the flag computed
    /// under the write lock the mutation already held: no second lock, one set
    /// operation. It is what lets `gc_candidates` cost the candidates it
    /// returns instead of the files this node holds (I8).
    fn track_liveness(&mut self, key: (u16, u32), dead: bool) {
        if dead {
            self.dead.insert(key);
        } else {
            self.dead.remove(&key);
        }
    }

    /// At most `limit` files with no live bytes, no hash list inside a txns
    /// window, no snapshot reference and no pin (§11.7).
    ///
    /// This is PHASE ONE of GC. I10 says a file is unlinked only after a
    /// durable store commit that no longer references it, so the caller
    /// records the removal, takes a durable point, and only then calls
    /// [`Segments::unlink`]. The candidate list is re-checked there, so a pin
    /// taken in between still wins.
    ///
    /// `limit` is a WORK bound, not a buffer bound (I8): the caller runs this
    /// after every applied entry, and it must not pay for files it is not
    /// going to collect. The walk is over the incrementally maintained dead
    /// set, so it examines the candidates it returns plus the pinned files
    /// ahead of them — pins are outstanding claims (§11.7, I4), bounded by
    /// work in flight and never by retained volume.
    pub fn gc_candidates(&mut self, limit: usize) -> Vec<(u16, u32)> {
        if limit == 0 || self.dead.is_empty() {
            // The common case, on every turn of the apply loop: nothing to
            // collect, and not even the pins lock is taken for it — readers
            // take pins under that lock (§11.7, I4).
            return Vec::new();
        }
        let mut out: Vec<(u16, u32)> = Vec::new();
        let mut examined = 0u64;
        {
            let pins = self.shared.pins.lock().expect("segment pins poisoned");
            for key in &self.dead {
                examined += 1;
                if pins.contains_key(key) {
                    continue;
                }
                out.push(*key);
                if out.len() >= limit {
                    break;
                }
            }
        }
        self.gc_examined += examined;
        out
    }

    /// File table entries [`Segments::gc_candidates`] has examined, ever.
    ///
    /// The instrument for I8 on the GC path: a test asserts it follows the
    /// candidates asked for, not the number of files. Without it, "the cost of
    /// a change is proportional to the change" is a claim about a loop nobody
    /// measures.
    pub fn gc_examined(&self) -> u64 {
        self.gc_examined
    }

    /// Phase two: unlink a dead file and forget it.
    ///
    /// Refuses the active file, a file that has come back to life and a
    /// pinned one. Evicts the read handle and the mapped index BEFORE
    /// unlinking: on unix the inode would survive an open handle, and serving
    /// bytes out of an unlinked file is exactly the bug that makes a retention
    /// window look like it never applied (pgless `drop_file`).
    ///
    /// The pins lock is held from the check to the removal of the file's row,
    /// because [`Reader::pin`] grants a pin under that same lock and only for
    /// a file the row table still names. Dropping it in between — the first
    /// cut did — let a pin be granted for a file this call was already
    /// unlinking.
    pub fn unlink(&mut self, bucket: u16, file_id: u32) -> Result<bool> {
        if bucket as usize >= NBUCKETS {
            return Err(SegError::Refused("bucket out of range"));
        }
        if self.active[bucket as usize]
            .as_ref()
            .is_some_and(|a| a.file_id == file_id)
        {
            return Err(SegError::Refused("the active file is never unlinked"));
        }
        // A handle of its own, so the guard borrows nothing of `self`.
        let shared = self.shared.clone();
        let pins = shared.pins.lock().expect("segment pins poisoned");
        if pins.contains_key(&(bucket, file_id)) {
            return Ok(false);
        }
        {
            let g = shared.files.read().expect("segment files poisoned");
            match g.get(&(bucket, file_id)) {
                None => return Ok(false),
                Some(m) if !m.is_dead() => return Ok(false),
                Some(_) => {}
            }
        }
        self.shared.evict((bucket, file_id));
        self.qidx_owed.remove(&(bucket, file_id));
        self.forget_sealed(bucket, file_id);
        shared
            .files
            .write()
            .expect("segment files poisoned")
            .remove(&(bucket, file_id));
        self.dead.remove(&(bucket, file_id));
        self.written_since_durable.remove(&(bucket, file_id));
        drop(pins);
        self.sealed_dirty
            .retain(|(b, id, _, _)| !(*b == bucket && *id == file_id));
        self.touched.remove(&(bucket, file_id));
        match std::fs::remove_file(seg_path(&self.shared.root, bucket, file_id)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(SegError::Io(e)),
        }
        let _ = std::fs::remove_file(qidx_path(&self.shared.root, bucket, file_id));
        self.dirty_dirs.insert(bucket);
        tracing::debug!(target: "rsm", bucket, file = file_id, "segment file unlinked");
        Ok(true)
    }

    /// The id of a bucket's active file.
    pub fn active_file(&self, bucket: u16) -> Option<u32> {
        self.active[bucket as usize].as_ref().map(|a| a.file_id)
    }

    /// Bytes in a bucket's active file.
    pub fn active_len(&self, bucket: u16) -> u64 {
        self.active[bucket as usize]
            .as_ref()
            .map(|a| a.len)
            .unwrap_or(0)
    }

    /// Records held by the RAM index of every active file (the I8 number of
    /// [`index::ActiveIndexes`]).
    pub fn active_index_len(&self) -> usize {
        self.shared
            .active
            .read()
            .expect("segment active index poisoned")
            .len()
    }
}

/// `read_exact`, but a short read at the end of a file is an answer, not an
/// error: a torn tail is ordinary (§11.5).
fn read_exact_or_less(r: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut got = 0;
    while got < buf.len() {
        match r.read(&mut buf[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(got)
}
