//! The cross-file directory of one queue log (2026-10-09): which FILE holds a
//! partition's offset.
//!
//! A queue log is many files, each with its own index (`.qidx`, sorted by
//! `(pid, base_offset)`, [`super::index`]). Nothing said which file holds a
//! given `(pid, offset)`: a lookup asked the sealed files one after the other.
//! That is two or three probes for a consumer at the tail and a probe of
//! EVERY file for anything older — 16,000 of them for a queue that retains
//! 1 TB in 64 MiB files — and it is the lookup every pop's payload read makes.
//!
//! A RUN is an immutable, sorted file of EXTENTS for a contiguous range of
//! sealed files:
//!
//! ```text
//! extent: pid:u64 | first:u64 | end:u64 | first_created:i64 | file_id:u64
//!         "file `file_id` holds records of `pid` for offsets [first, end),
//!          the first of them stamped `first_created`"
//! ```
//!
//! sorted by `(pid, first)`. One extent per partition per file it appears in,
//! so a run is at most as large as the indexes it covers and usually far
//! smaller. A lookup binary-searches a run for the extent holding the offset
//! and then asks that ONE file's own index for the record: the run only ever
//! names a candidate, and the file's index stays the authority (a file
//! retention rewrote or removed since simply does not answer).
//!
//! Runs are derived data: built from the `.qidx` files of sealed logs, with
//! sequential I/O only, by the node's maintenance thread — never on the log
//! writer — merged into larger runs as they accumulate, and rebuilt from the
//! `.qidx` files when one is missing or damaged. A sealed file no run covers
//! yet is probed directly, as before.
//!
//! ```text
//! magic[8] = "QQDIRX1\0"
//! lo_file:u64 | hi_file:u64     the sealed files this run covers (inclusive)
//! count:u64                     extents
//! extents_xxh3:u64 | header_xxh3:u64
//! then `count` extents of 40 bytes
//! ```

use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use xxhash_rust::xxh3::xxh3_64;

use super::index;

pub const MAGIC: [u8; 8] = *b"QQDIRX1\0";

/// Bytes before the first extent.
pub const HEADER_LEN: usize = 8 + 8 + 8 + 8 + 8 + 8; // 48

/// Bytes of one extent.
pub const EXTENT_LEN: usize = 8 + 8 + 8 + 8 + 8; // 40

const HEADER_CHECKED: usize = HEADER_LEN - 8;

/// "File `file_id` holds records of `pid` for offsets `[first, end)`."
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Extent {
    pub pid: u64,
    pub first: u64,
    /// Exclusive.
    pub end: u64,
    /// The stamp of the extent's first record (stamps rise with offsets
    /// inside a partition, so a search by time can bisect on it).
    pub first_created_us: i64,
    pub file_id: u64,
}

impl Extent {
    fn write_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.pid.to_le_bytes());
        out.extend_from_slice(&self.first.to_le_bytes());
        out.extend_from_slice(&self.end.to_le_bytes());
        out.extend_from_slice(&self.first_created_us.to_le_bytes());
        out.extend_from_slice(&self.file_id.to_le_bytes());
    }

    fn read_from(b: &[u8]) -> Extent {
        let u = |at: usize| u64::from_le_bytes(b[at..at + 8].try_into().expect("8 bytes"));
        Extent {
            pid: u(0),
            first: u(8),
            end: u(16),
            first_created_us: u(24) as i64,
            file_id: u(32),
        }
    }
}

/// The extents of ONE sealed file, from its index records (which are sorted
/// by `(pid, base_offset)`): one per partition, in that order.
pub fn extents_of(file_id: u64, records: impl Iterator<Item = index::Record>) -> Vec<Extent> {
    let mut out: Vec<Extent> = Vec::new();
    for r in records {
        match out.last_mut() {
            Some(e) if e.pid == r.pid => {
                e.end = e.end.max(r.end);
            }
            _ => out.push(Extent {
                pid: r.pid,
                first: r.base_offset,
                end: r.end,
                first_created_us: r.created_at_us,
                file_id,
            }),
        }
    }
    out
}

/// Where `(pid, offset)` falls among one run's extents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Find {
    /// The run holds nothing of `pid`.
    Missing,
    /// Below the run's first extent of `pid`: an older run may hold it.
    Before,
    /// At or past the end of the run's last extent of `pid`: a newer file may
    /// hold it, no older one can.
    After,
    /// This extent's file is the one to ask.
    In(Extent),
    /// Between two extents of `pid`: no file of this run's range holds it
    /// (retention took it).
    Gap,
}

/// One run, memory-mapped.
pub struct Run {
    map: Mmap,
    count: usize,
    lo_file: u64,
    hi_file: u64,
    path: PathBuf,
}

impl std::fmt::Debug for Run {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "qlog-dirx(f{}..=f{}, {} extents)",
            self.lo_file, self.hi_file, self.count
        )
    }
}

fn corrupt(path: &Path, what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{}: {what}", path.display()),
    )
}

/// The file name of the run covering sealed files `lo..=hi`.
pub fn file_name(lo: u64, hi: u64) -> String {
    format!("dirx-{lo:020}-{hi:020}.qdx")
}

/// `(lo, hi)` of a run's file name, `None` for any other name.
pub fn parse_name(name: &str) -> Option<(u64, u64)> {
    let rest = name.strip_prefix("dirx-")?.strip_suffix(".qdx")?;
    let (lo, hi) = rest.split_once('-')?;
    let (lo, hi) = (lo.parse::<u64>().ok()?, hi.parse::<u64>().ok()?);
    (lo <= hi).then_some((lo, hi))
}

impl Run {
    /// Write the run for sealed files `lo..=hi` (extents sorted by `(pid,
    /// first)`) into `dir`, durably, and return its path. Written to a
    /// temporary name first: a crash leaves either no run or a whole one.
    pub fn write(dir: &Path, lo: u64, hi: u64, extents: &[Extent]) -> io::Result<PathBuf> {
        debug_assert!(extents
            .windows(2)
            .all(|w| (w[0].pid, w[0].first) <= (w[1].pid, w[1].first)));
        let mut body = Vec::with_capacity(extents.len() * EXTENT_LEN);
        for e in extents {
            e.write_into(&mut body);
        }
        let mut head = Vec::with_capacity(HEADER_LEN);
        head.extend_from_slice(&MAGIC);
        head.extend_from_slice(&lo.to_le_bytes());
        head.extend_from_slice(&hi.to_le_bytes());
        head.extend_from_slice(&(extents.len() as u64).to_le_bytes());
        head.extend_from_slice(&xxh3_64(&body).to_le_bytes());
        let hsum = xxh3_64(&head[..HEADER_CHECKED]);
        head.extend_from_slice(&hsum.to_le_bytes());
        let path = dir.join(file_name(lo, hi));
        let tmp = dir.join(format!("{}.tmp", file_name(lo, hi)));
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&head)?;
            f.write_all(&body)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        if let Ok(d) = File::open(dir) {
            let _ = d.sync_all();
        }
        Ok(path)
    }

    /// Map a run, checking its header and every extent's bytes.
    pub fn open(path: &Path) -> io::Result<Run> {
        let f = File::open(path)?;
        // SAFETY: a run is written once under a temporary name, renamed into
        // place and never modified; it is only ever removed, which on the
        // platforms this builds for leaves an existing mapping valid.
        let map = unsafe { Mmap::map(&f)? };
        if map.len() < HEADER_LEN || map[..8] != MAGIC {
            return Err(corrupt(path, "not a queue-log directory run"));
        }
        let u = |at: usize| u64::from_le_bytes(map[at..at + 8].try_into().expect("8 bytes"));
        if xxh3_64(&map[..HEADER_CHECKED]) != u(HEADER_CHECKED) {
            return Err(corrupt(path, "directory run header checksum"));
        }
        let (lo_file, hi_file, count) = (u(8), u(16), u(24) as usize);
        let want = count
            .checked_mul(EXTENT_LEN)
            .and_then(|n| n.checked_add(HEADER_LEN));
        if want != Some(map.len()) || lo_file > hi_file {
            return Err(corrupt(path, "directory run length"));
        }
        if xxh3_64(&map[HEADER_LEN..]) != u(32) {
            return Err(corrupt(path, "directory run extents checksum"));
        }
        Ok(Run {
            map,
            count,
            lo_file,
            hi_file,
            path: path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The sealed files this run covers, inclusive.
    pub fn files(&self) -> (u64, u64) {
        (self.lo_file, self.hi_file)
    }

    pub fn covers(&self, file_id: u64) -> bool {
        self.lo_file <= file_id && file_id <= self.hi_file
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn extent(&self, i: usize) -> Extent {
        let at = HEADER_LEN + i * EXTENT_LEN;
        Extent::read_from(&self.map[at..at + EXTENT_LEN])
    }

    pub fn extents(&self) -> impl Iterator<Item = Extent> + '_ {
        (0..self.count).map(move |i| self.extent(i))
    }

    fn partition_point(&self, pred: impl Fn(&Extent) -> bool) -> usize {
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if pred(&self.extent(mid)) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// The half-open range of this run's extents of `pid`.
    fn span_of(&self, pid: u64) -> (usize, usize) {
        let lo = self.partition_point(|e| e.pid < pid);
        let hi = self.partition_point(|e| e.pid <= pid);
        (lo, hi)
    }

    /// Where `(pid, offset)` falls among this run's extents. O(log n).
    pub fn find(&self, pid: u64, offset: u64) -> Find {
        let (lo, hi) = self.span_of(pid);
        if lo == hi {
            return Find::Missing;
        }
        if offset < self.extent(lo).first {
            return Find::Before;
        }
        // The last extent of `pid` that begins at or below the offset. A
        // rewritten file can leave two extents overlapping an offset only if
        // they name the same records, so the last one is as good as any.
        let at = self.partition_point(|e| (e.pid, e.first) <= (pid, offset));
        debug_assert!(at > lo);
        let e = self.extent(at - 1);
        if offset < e.end {
            return Find::In(e);
        }
        if at == hi {
            Find::After
        } else {
            Find::Gap
        }
    }

    /// The last extent of `pid` whose first record lies below `high` and was
    /// stamped before `before_us`: the file holding the newest record below
    /// both bounds, when this run holds one.
    pub fn extent_before(&self, pid: u64, high: u64, before_us: i64) -> Option<Extent> {
        let (lo, _) = self.span_of(pid);
        let at = self.partition_point(|e| {
            e.pid < pid || (e.pid == pid && e.first < high && e.first_created_us < before_us)
        });
        (at > lo).then(|| self.extent(at - 1))
    }
}

// ---------------------------------------------------------------------------
// Building and merging (the node's maintenance thread, no log lock held)
// ---------------------------------------------------------------------------

/// How many sealed files a log may hold outside any run before a run is
/// built for them: a lookup probes these one by one, newest first.
pub const BATCH: usize = 16;

/// The most sealed files one build reads (their extents are sorted in
/// memory: at most one extent per record of each file).
pub const BUILD_MAX: usize = 64;

/// How many adjacent runs of one tier are merged into one of the next.
pub const FANOUT: usize = 8;

/// A run below this many extents is of tier 0; each tier above holds
/// [`FANOUT`] times more.
const TIER0: usize = 65_536;

/// The size tier of a run of `len` extents.
pub fn tier(len: usize) -> u32 {
    let mut t = 0u32;
    let mut cap = TIER0;
    while len >= cap && t < 16 {
        t += 1;
        cap = cap.saturating_mul(FANOUT);
    }
    t
}

/// One step of directory maintenance, decided under the log's READ lock and
/// run with no lock held ([`Plan::run`]).
pub enum Plan {
    /// Build the run of these sealed files, ascending by id: `(id, its
    /// `.qidx` path, its sealed length)`.
    Build {
        dir: PathBuf,
        files: Vec<(u64, PathBuf, u64)>,
    },
    /// Merge these adjacent runs (ascending) into one covering `lo..=hi`;
    /// `live` is the sealed files that still exist (ascending): an extent of
    /// any other file is dropped.
    Merge {
        dir: PathBuf,
        runs: Vec<PathBuf>,
        lo: u64,
        hi: u64,
        live: Vec<u64>,
    },
}

/// A run written by [`Plan::run`], to install under the log's write lock.
pub struct Built {
    pub lo: u64,
    pub hi: u64,
    pub path: PathBuf,
    pub extents: usize,
}

/// Writes a run extent by extent (they must arrive sorted): a temporary file
/// with a placeholder header, the header written last, then the rename.
struct RunWriter {
    dir: PathBuf,
    lo: u64,
    hi: u64,
    tmp: PathBuf,
    out: io::BufWriter<File>,
    sum: xxhash_rust::xxh3::Xxh3,
    count: u64,
    last: Option<(u64, u64)>,
    buf: Vec<u8>,
}

impl RunWriter {
    fn new(dir: &Path, lo: u64, hi: u64) -> io::Result<RunWriter> {
        let tmp = dir.join(format!("{}.tmp", file_name(lo, hi)));
        let f = File::create(&tmp)?;
        let mut out = io::BufWriter::with_capacity(1 << 20, f);
        out.write_all(&[0u8; HEADER_LEN])?;
        Ok(RunWriter {
            dir: dir.to_path_buf(),
            lo,
            hi,
            tmp,
            out,
            sum: xxhash_rust::xxh3::Xxh3::new(),
            count: 0,
            last: None,
            buf: Vec::with_capacity(EXTENT_LEN),
        })
    }

    fn push(&mut self, e: &Extent) -> io::Result<()> {
        if self.last.is_some_and(|l| l > (e.pid, e.first)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "directory run extents out of order",
            ));
        }
        self.last = Some((e.pid, e.first));
        self.buf.clear();
        e.write_into(&mut self.buf);
        self.sum.update(&self.buf);
        self.out.write_all(&self.buf)?;
        self.count += 1;
        Ok(())
    }

    fn finish(self) -> io::Result<Built> {
        use std::io::{Seek, SeekFrom};
        let RunWriter {
            dir,
            lo,
            hi,
            tmp,
            out,
            sum,
            count,
            ..
        } = self;
        let mut f = out.into_inner().map_err(|e| e.into_error())?;
        let mut head = Vec::with_capacity(HEADER_LEN);
        head.extend_from_slice(&MAGIC);
        head.extend_from_slice(&lo.to_le_bytes());
        head.extend_from_slice(&hi.to_le_bytes());
        head.extend_from_slice(&count.to_le_bytes());
        head.extend_from_slice(&sum.digest().to_le_bytes());
        let hsum = xxh3_64(&head[..HEADER_CHECKED]);
        head.extend_from_slice(&hsum.to_le_bytes());
        f.seek(SeekFrom::Start(0))?;
        f.write_all(&head)?;
        f.sync_all()?;
        drop(f);
        let path = dir.join(file_name(lo, hi));
        std::fs::rename(&tmp, &path)?;
        if let Ok(d) = File::open(&dir) {
            let _ = d.sync_all();
        }
        Ok(Built {
            lo,
            hi,
            path,
            extents: count as usize,
        })
    }
}

impl Plan {
    /// Write the run this plan asks for. Reads only immutable files (sealed
    /// `.qidx` files, runs); a file that went away meanwhile fails the step,
    /// which the next maintenance pass plans again.
    pub fn run(self) -> io::Result<Built> {
        match self {
            Plan::Build { dir, files } => {
                let (lo, hi) = match (files.first(), files.last()) {
                    (Some(a), Some(b)) => (a.0, b.0),
                    _ => return Err(io::Error::other("an empty directory build")),
                };
                let mut all: Vec<Extent> = Vec::new();
                for (id, qidx, bytes) in &files {
                    let view = index::View::open(qidx, Some(*bytes)).map_err(io::Error::from)?;
                    if view.check_identity(*id).is_err() {
                        return Err(corrupt(qidx, "the index of another file"));
                    }
                    all.extend(extents_of(*id, view.records()));
                }
                all.sort_unstable_by_key(|e| (e.pid, e.first, e.file_id));
                let mut w = RunWriter::new(&dir, lo, hi)?;
                for e in &all {
                    w.push(e)?;
                }
                w.finish()
            }
            Plan::Merge {
                dir,
                runs,
                lo,
                hi,
                live,
            } => {
                let opened: Vec<Run> = runs
                    .iter()
                    .map(|p| Run::open(p))
                    .collect::<io::Result<_>>()?;
                // A k-way merge of sorted runs: the smallest head each time.
                let mut heads: Vec<usize> = vec![0; opened.len()];
                let mut w = RunWriter::new(&dir, lo, hi)?;
                loop {
                    let mut best: Option<(usize, Extent)> = None;
                    for (i, run) in opened.iter().enumerate() {
                        if heads[i] >= run.len() {
                            continue;
                        }
                        let e = run.extent(heads[i]);
                        if best.is_none_or(|(_, b)| {
                            (e.pid, e.first, e.file_id) < (b.pid, b.first, b.file_id)
                        }) {
                            best = Some((i, e));
                        }
                    }
                    let Some((i, e)) = best else { break };
                    heads[i] += 1;
                    if live.binary_search(&e.file_id).is_ok() {
                        w.push(&e)?;
                    }
                }
                w.finish()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(pid: u64, base: u64, end: u64, created: i64) -> index::Record {
        index::Record {
            pid,
            base_offset: base,
            end,
            created_at_us: created,
            offset: 0,
            count: (end - base) as u32,
            len: 64,
        }
    }

    fn dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("queen-qlog-dirx-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn a_files_extents_are_one_per_partition() {
        let recs = vec![
            rec(1, 0, 3, 10),
            rec(1, 3, 4, 11),
            rec(2, 7, 9, 12),
            rec(5, 0, 1, 13),
            rec(5, 1, 6, 14),
        ];
        let e = extents_of(9, recs.into_iter());
        assert_eq!(
            e,
            vec![
                Extent {
                    pid: 1,
                    first: 0,
                    end: 4,
                    first_created_us: 10,
                    file_id: 9
                },
                Extent {
                    pid: 2,
                    first: 7,
                    end: 9,
                    first_created_us: 12,
                    file_id: 9
                },
                Extent {
                    pid: 5,
                    first: 0,
                    end: 6,
                    first_created_us: 13,
                    file_id: 9
                },
            ]
        );
    }

    #[test]
    fn a_run_finds_the_file_of_an_offset_and_survives_a_reopen() {
        let d = dir("find");
        // Partition 1 in files 3, 4 and 6 (retention left a hole at 20..30);
        // partition 2 in file 5 only.
        let mut extents = vec![
            Extent {
                pid: 1,
                first: 0,
                end: 10,
                first_created_us: 100,
                file_id: 3,
            },
            Extent {
                pid: 1,
                first: 10,
                end: 20,
                first_created_us: 200,
                file_id: 4,
            },
            Extent {
                pid: 1,
                first: 30,
                end: 40,
                first_created_us: 400,
                file_id: 6,
            },
            Extent {
                pid: 2,
                first: 5,
                end: 8,
                first_created_us: 300,
                file_id: 5,
            },
        ];
        extents.sort_by_key(|e| (e.pid, e.first));
        let path = Run::write(&d, 3, 6, &extents).unwrap();
        assert_eq!(
            parse_name(path.file_name().unwrap().to_str().unwrap()),
            Some((3, 6))
        );
        let run = Run::open(&path).unwrap();
        assert_eq!(run.files(), (3, 6));
        assert_eq!(run.len(), 4);
        assert!(run.covers(3) && run.covers(6) && !run.covers(7) && !run.covers(2));
        assert_eq!(run.extents().collect::<Vec<_>>(), extents);

        assert_eq!(run.find(9, 0), Find::Missing);
        assert_eq!(run.find(2, 4), Find::Before);
        assert_eq!(run.find(2, 8), Find::After);
        assert!(matches!(run.find(2, 7), Find::In(e) if e.file_id == 5));
        assert!(matches!(run.find(1, 0), Find::In(e) if e.file_id == 3));
        assert!(matches!(run.find(1, 9), Find::In(e) if e.file_id == 3));
        assert!(matches!(run.find(1, 10), Find::In(e) if e.file_id == 4));
        assert!(matches!(run.find(1, 39), Find::In(e) if e.file_id == 6));
        assert_eq!(run.find(1, 25), Find::Gap);
        assert_eq!(run.find(1, 40), Find::After);

        // By time: the newest extent that begins below both bounds.
        assert_eq!(
            run.extent_before(1, u64::MAX, i64::MAX).map(|e| e.file_id),
            Some(6)
        );
        assert_eq!(
            run.extent_before(1, 30, i64::MAX).map(|e| e.file_id),
            Some(4)
        );
        assert_eq!(
            run.extent_before(1, u64::MAX, 250).map(|e| e.file_id),
            Some(4)
        );
        assert_eq!(run.extent_before(1, u64::MAX, 100), None);
        assert_eq!(run.extent_before(7, u64::MAX, i64::MAX), None);

        // A damaged run is refused, never half-believed.
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        let bad = d.join(file_name(7, 8));
        std::fs::write(&bad, &bytes).unwrap();
        assert!(Run::open(&bad).is_err());
        std::fs::write(&bad, &bytes[..bytes.len() - 3]).unwrap();
        assert!(Run::open(&bad).is_err());
        assert_eq!(parse_name("dirx-5-3.qdx"), None);
        assert_eq!(parse_name("00000000000000000001.qidx"), None);
        let _ = std::fs::remove_dir_all(&d);
    }
}
