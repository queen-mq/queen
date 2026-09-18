//! Payload segment files: 256 append-only buckets (D9, §11.2).
//!
//! Frame: `len u32 | xxh3 u64 | pid u64 | base_offset u64 | count u32 |
//! created_at i64 | hashes (count * u64) | blob`. `len` covers everything
//! after itself, and the xxh3 covers everything after the checksum field, so a
//! file can be rebuilt by scanning and every read can be verified (pgless's
//! `read_blob` did not verify).
//!
//! The apply thread appends without fsync; files are fsynced only at the
//! durable point (§11.4) and roll at `segment_bytes`.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use xxhash_rust::xxh3::xxh3_64;

pub const NBUCKETS: usize = 256;
#[allow(dead_code)]
pub const HDR: usize = 4 + 8 + 8 + 8 + 4 + 8; // len, hash, pid, base, count, created_at

#[derive(Clone, Copy, Debug)]
pub struct Pos {
    pub bucket: u16,
    pub file_id: u32,
    pub offset: u64,
    pub len: u32,
    pub hash: u64,
}

struct Bucket {
    file_id: u32,
    f: File,
    len: u64,
    dirty: bool,
    /// segments in this file that are still referenced by the store
    live: u64,
}

/// How the durable point flushes a file.
///
/// `Full` is what a durable point must do: on macOS `F_FULLFSYNC` (the only
/// call that flushes the drive's own cache; it is serialized by the drive, so
/// a laptop durable point costs milliseconds per file), on Linux `fsync`.
/// `Data` is `fdatasync` on Linux and a plain `fsync` on macOS, which does
/// NOT flush the drive cache: it is there to separate "the file system work"
/// from "the barrier" when reading laptop numbers, never as a durability mode
/// to ship.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FsyncMode {
    Full,
    Data,
}

impl FsyncMode {
    pub fn parse(s: &str) -> FsyncMode {
        match s {
            "data" | "fdatasync" => FsyncMode::Data,
            _ => FsyncMode::Full,
        }
    }
}

fn fsync_fd(fd: RawFd, mode: FsyncMode) -> std::io::Result<()> {
    let rc = unsafe {
        #[cfg(target_os = "macos")]
        {
            if mode == FsyncMode::Full {
                let r = libc::fcntl(fd, libc::F_FULLFSYNC);
                if r != -1 {
                    return Ok(());
                }
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
        Err(std::io::Error::last_os_error())
    }
}

pub struct Segments {
    dir: PathBuf,
    buckets: Vec<Bucket>,
    segment_bytes: u64,
    /// sealed files not yet fsynced (kept open so the sync needs no reopen)
    sealed_dirty: Vec<(u16, File)>,
    pub appended_bytes: u64,
    pub appended_frames: u64,
    pub fsync_calls: u64,
    buf: Vec<u8>,
}

fn bucket_dir(dir: &Path, b: u16) -> PathBuf {
    dir.join(format!("b{b:03}"))
}

fn file_path(dir: &Path, b: u16, id: u32) -> PathBuf {
    bucket_dir(dir, b).join(format!("f{id:06}.seg"))
}

fn open_append(path: &Path) -> std::io::Result<(File, u64)> {
    let f = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    let len = f.metadata()?.len();
    Ok((f, len))
}

impl Segments {
    /// Opens (or re-opens) the 256 buckets under `dir/seg`.
    pub fn open(root: &Path, segment_bytes: u64) -> std::io::Result<Self> {
        let dir = root.join("seg");
        let mut buckets = Vec::with_capacity(NBUCKETS);
        for b in 0..NBUCKETS as u16 {
            std::fs::create_dir_all(bucket_dir(&dir, b))?;
            // highest existing file id, else 0
            let mut id = 0u32;
            for ent in std::fs::read_dir(bucket_dir(&dir, b))?.flatten() {
                let name = ent.file_name();
                let name = name.to_string_lossy();
                if let Some(rest) = name.strip_prefix('f') {
                    if let Some(num) = rest.strip_suffix(".seg") {
                        if let Ok(v) = num.parse::<u32>() {
                            id = id.max(v);
                        }
                    }
                }
            }
            let (f, len) = open_append(&file_path(&dir, b, id))?;
            buckets.push(Bucket {
                file_id: id,
                f,
                len,
                dirty: false,
                live: 0,
            });
        }
        Ok(Self {
            dir,
            buckets,
            segment_bytes,
            sealed_dirty: Vec::new(),
            appended_bytes: 0,
            appended_frames: 0,
            fsync_calls: 0,
            buf: Vec::with_capacity(1 << 16),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Appends one segment frame. Returns its node-local position (D8).
    pub fn append(
        &mut self,
        bucket: u16,
        pid: u64,
        base_offset: u64,
        count: u32,
        created_at: i64,
        hashes: &[u64],
        blob: &[u8],
    ) -> std::io::Result<Pos> {
        let body_len = 8 + 8 + 8 + 4 + 8 + hashes.len() * 8 + blob.len(); // after `len`: hash, pid, base, count, created_at, hashes, blob
        self.buf.clear();
        self.buf.extend_from_slice(&(body_len as u32).to_le_bytes());
        self.buf.extend_from_slice(&0u64.to_le_bytes()); // checksum placeholder
        self.buf.extend_from_slice(&pid.to_le_bytes());
        self.buf.extend_from_slice(&base_offset.to_le_bytes());
        self.buf.extend_from_slice(&count.to_le_bytes());
        self.buf.extend_from_slice(&created_at.to_le_bytes());
        for h in hashes {
            self.buf.extend_from_slice(&h.to_le_bytes());
        }
        self.buf.extend_from_slice(blob);
        let h = xxh3_64(&self.buf[12..]);
        self.buf[4..12].copy_from_slice(&h.to_le_bytes());

        let flen = self.buf.len() as u64;
        let b = &mut self.buckets[bucket as usize];
        if b.len > 0 && b.len + flen > self.segment_bytes {
            // seal and roll
            let nid = b.file_id + 1;
            let (f, len) = open_append(&file_path(&self.dir, bucket, nid))?;
            let old = std::mem::replace(&mut b.f, f);
            let was_dirty = b.dirty;
            b.file_id = nid;
            b.len = len;
            b.dirty = false;
            b.live = 0;
            if was_dirty {
                self.sealed_dirty.push((bucket, old));
            }
        }
        let b = &mut self.buckets[bucket as usize];
        let offset = b.len;
        b.f.write_all(&self.buf)?;
        b.len += flen;
        b.dirty = true;
        b.live += 1;
        self.appended_bytes += flen;
        self.appended_frames += 1;
        Ok(Pos {
            bucket,
            file_id: b.file_id,
            offset,
            len: flen as u32,
            hash: h,
        })
    }

    /// fsync every file written since the last durable point, plus the bucket
    /// directories that gained a file (§11.4 step 1). `threads` > 1 issues the
    /// fsyncs from a scoped thread pool, which is what an apply thread would
    /// have to do to keep a durable point under a budget: the cost of this
    /// step is proportional to the number of BUCKETS touched, not to bytes.
    /// Returns (files synced, directories synced).
    pub fn sync_dirty(&mut self, threads: usize, mode: FsyncMode) -> std::io::Result<(u64, u64)> {
        let mut fds: Vec<RawFd> = Vec::new();
        let mut dirs: Vec<u16> = Vec::new();
        let sealed = std::mem::take(&mut self.sealed_dirty);
        for (b, f) in &sealed {
            fds.push(f.as_raw_fd());
            dirs.push(*b);
        }
        for (i, b) in self.buckets.iter_mut().enumerate() {
            if b.dirty {
                fds.push(b.f.as_raw_fd());
                b.dirty = false;
                if b.len <= 1 << 16 {
                    dirs.push(i as u16);
                }
            }
        }
        let files = fds.len() as u64;
        if threads <= 1 || fds.len() < 8 {
            for fd in &fds {
                fsync_fd(*fd, mode)?;
            }
        } else {
            let chunk = fds.len().div_ceil(threads);
            std::thread::scope(|sc| {
                let mut hs = Vec::new();
                for part in fds.chunks(chunk) {
                    hs.push(sc.spawn(move || {
                        for fd in part {
                            fsync_fd(*fd, mode)?;
                        }
                        Ok::<(), std::io::Error>(())
                    }));
                }
                for h in hs {
                    h.join().expect("fsync thread")?;
                }
                Ok::<(), std::io::Error>(())
            })?;
        }
        drop(sealed);
        dirs.sort_unstable();
        dirs.dedup();
        let ndirs = dirs.len() as u64;
        for b in dirs {
            if let Ok(d) = File::open(bucket_dir(&self.dir, b)) {
                let _ = fsync_fd(d.as_raw_fd(), mode);
            }
        }
        self.fsync_calls += files;
        Ok((files, ndirs))
    }

    /// (bucket, file_id, length) of every active file: the lengths recorded in
    /// the durable store commit (I11).
    pub fn file_lengths(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(NBUCKETS * 14);
        for (i, b) in self.buckets.iter().enumerate() {
            v.extend_from_slice(&(i as u16).to_le_bytes());
            v.extend_from_slice(&b.file_id.to_le_bytes());
            v.extend_from_slice(&b.len.to_le_bytes());
        }
        v
    }

    #[allow(dead_code)]
    pub fn total_len(&self) -> u64 {
        self.buckets.iter().map(|b| b.len).sum()
    }

    /// Drops the reference of one segment; returns true when the file it was
    /// in has no live segments left and is not the active file of its bucket.
    #[allow(dead_code)]
    pub fn release(&mut self, bucket: u16, file_id: u32) -> bool {
        let b = &mut self.buckets[bucket as usize];
        if b.file_id == file_id {
            b.live = b.live.saturating_sub(1);
            return false;
        }
        true
    }

    /// Unlinks every sealed file of every bucket below `keep_below[bucket]`
    /// (used by the reclaim phase, standing in for retention + file GC, I10).
    pub fn unlink_sealed_below(&mut self, keep_below: &[u32]) -> std::io::Result<(u64, u64)> {
        let mut files = 0u64;
        let mut bytes = 0u64;
        for b in 0..NBUCKETS as u16 {
            let active = self.buckets[b as usize].file_id;
            let limit = keep_below[b as usize].min(active);
            for id in 0..limit {
                let p = file_path(&self.dir, b, id);
                if let Ok(md) = std::fs::metadata(&p) {
                    bytes += md.len();
                    std::fs::remove_file(&p)?;
                    files += 1;
                }
            }
        }
        Ok((files, bytes))
    }

    #[allow(dead_code)]
    pub fn active_file_ids(&self) -> Vec<u32> {
        self.buckets.iter().map(|b| b.file_id).collect()
    }
}

/// Reads one frame and verifies its checksum. Used by `verify` after kill -9.
pub fn read_frame(
    root: &Path,
    bucket: u16,
    file_id: u32,
    offset: u64,
    len: u32,
) -> Result<(u64, u64, u64, u32), String> {
    let dir = root.join("seg");
    let path = file_path(&dir, bucket, file_id);
    let mut f = File::open(&path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let flen = f.metadata().map_err(|e| e.to_string())?.len();
    if offset + len as u64 > flen {
        return Err(format!(
            "short file {}: need {}..{}, have {}",
            path.display(),
            offset,
            offset + len as u64,
            flen
        ));
    }
    f.seek(SeekFrom::Start(offset)).map_err(|e| e.to_string())?;
    let mut buf = vec![0u8; len as usize];
    f.read_exact(&mut buf).map_err(|e| e.to_string())?;
    let body_len = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if body_len as usize + 4 != buf.len() {
        return Err(format!(
            "frame len {} != read {}",
            body_len as usize + 4,
            buf.len()
        ));
    }
    let want = u64::from_le_bytes(buf[4..12].try_into().unwrap());
    let got = xxh3_64(&buf[12..]);
    if want != got {
        return Err(format!("checksum {want:#x} != {got:#x}"));
    }
    let pid = u64::from_le_bytes(buf[12..20].try_into().unwrap());
    let base = u64::from_le_bytes(buf[20..28].try_into().unwrap());
    let count = u32::from_le_bytes(buf[28..32].try_into().unwrap());
    Ok((want, pid, base, count))
}

/// File length of one segment file, or None when it does not exist.
#[allow(dead_code)]
pub fn file_len(root: &Path, bucket: u16, file_id: u32) -> Option<u64> {
    std::fs::metadata(file_path(&root.join("seg"), bucket, file_id))
        .ok()
        .map(|m| m.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("s1-seg-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn frames_round_trip_and_checksums_catch_corruption() {
        let root = tmpdir("rt");
        let mut s = Segments::open(&root, 1 << 20).unwrap();
        let blob = vec![7u8; 1024];
        let hashes = [1u64, 2, 3];
        let a = s.append(3, 42, 0, 3, 111, &hashes, &blob).unwrap();
        let b = s.append(3, 42, 3, 3, 222, &hashes, &blob).unwrap();
        s.sync_dirty(1, FsyncMode::Data).unwrap();
        assert_eq!(a.offset, 0);
        assert_eq!(b.offset, a.len as u64);

        let (h, pid, base, count) =
            read_frame(&root, a.bucket, a.file_id, a.offset, a.len).unwrap();
        assert_eq!((h, pid, base, count), (a.hash, 42, 0, 3));
        let (h, _, base, _) = read_frame(&root, b.bucket, b.file_id, b.offset, b.len).unwrap();
        assert_eq!((h, base), (b.hash, 3));

        // flip one byte inside the blob of the first frame
        let p = root.join("seg").join("b003").join("f000000.seg");
        let mut bytes = std::fs::read(&p).unwrap();
        let mid = (a.len / 2) as usize;
        bytes[mid] ^= 0xff;
        std::fs::write(&p, &bytes).unwrap();
        let err = read_frame(&root, a.bucket, a.file_id, a.offset, a.len).unwrap_err();
        assert!(err.contains("checksum"), "{err}");

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn files_roll_and_lengths_are_recorded() {
        let root = tmpdir("roll");
        let mut s = Segments::open(&root, 4096).unwrap();
        let blob = vec![0u8; 1024];
        let mut last = None;
        for i in 0..8u64 {
            last = Some(s.append(0, 1, i * 10, 1, i as i64, &[i], &blob).unwrap());
        }
        let last = last.unwrap();
        assert!(
            last.file_id >= 2,
            "files must roll at 4 KiB, got id {}",
            last.file_id
        );
        let lens = s.file_lengths();
        assert_eq!(lens.len(), NBUCKETS * 14);
        let active = &lens[0..14];
        assert_eq!(u16::from_le_bytes(active[0..2].try_into().unwrap()), 0);
        assert_eq!(
            u32::from_le_bytes(active[2..6].try_into().unwrap()),
            last.file_id
        );
        assert_eq!(
            u64::from_le_bytes(active[6..14].try_into().unwrap()),
            last.offset + last.len as u64
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
