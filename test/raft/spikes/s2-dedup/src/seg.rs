//! Payload segment files with 128-bit hash lists (D9, D10, §11.2, §11.7).
//!
//! Derived from `s1-store/src/seg.rs` (spike S1) — same 256 buckets, same
//! append-only rolling files, same "fsync only at the durable point" rule —
//! with the three changes option (b) needs:
//!
//!   1. hashes are 16 bytes (xxh3_128), as §5.1 specifies, not S1's 8: at
//!      50k msg/s a 1 h window holds 1.8e8 hashes, where 64-bit hashes would
//!      collide (birthday: ~0.9 expected collisions) and a collision here is a
//!      message silently dropped as a duplicate;
//!   2. `read_hashes` reads one frame's hash list WITHOUT its payload, which is
//!      what a dedup probe or an ack-by-hash resolution needs;
//!   3. `rewrite_hash_only` rebuilds a file keeping only the frames' hash
//!      lists (no blobs) — the compaction §11.7 allows so that hash lists can
//!      outlive the payloads retention deleted, without pinning whole files.
//!
//! Frame: `len u32 | xxh3 u64 | flags u32 | pid u64 | base u64 | count u32 |
//!         created_at i64 | hashes (count*16) | blob`.
//! `len` covers everything after itself; the xxh3 covers everything after the
//! checksum field. flags bit 0 = hash-only frame (blob omitted).

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use xxhash_rust::xxh3::xxh3_64;

pub const NBUCKETS: usize = 256;
/// len, hash, flags, pid, base, count, created_at
pub const HDR: usize = 4 + 8 + 4 + 8 + 8 + 4 + 8;
pub const FLAG_HASH_ONLY: u32 = 1;

#[derive(Clone, Copy, Debug)]
pub struct Pos {
    pub bucket: u16,
    pub file_id: u32,
    pub offset: u64,
    pub len: u32,
}

struct Bucket {
    file_id: u32,
    f: File,
    len: u64,
    dirty: bool,
}

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

pub fn bucket_dir(dir: &Path, b: u16) -> PathBuf {
    dir.join(format!("b{b:03}"))
}

pub fn file_path(dir: &Path, b: u16, id: u32) -> PathBuf {
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

pub struct Segments {
    dir: PathBuf,
    buckets: Vec<Bucket>,
    segment_bytes: u64,
    sealed_dirty: Vec<(u16, File)>,
    pub appended_bytes: u64,
    pub appended_frames: u64,
    pub fsync_calls: u64,
    pub frame_reads: u64,
    pub frame_read_bytes: u64,
    buf: Vec<u8>,
}

impl Segments {
    pub fn open(root: &Path, segment_bytes: u64) -> std::io::Result<Self> {
        let dir = root.join("seg");
        let mut buckets = Vec::with_capacity(NBUCKETS);
        for b in 0..NBUCKETS as u16 {
            std::fs::create_dir_all(bucket_dir(&dir, b))?;
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
            frame_reads: 0,
            frame_read_bytes: 0,
            buf: Vec::with_capacity(1 << 16),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn active_file(&self, bucket: u16) -> u32 {
        self.buckets[bucket as usize].file_id
    }

    /// Appends one frame. Returns its node-local position (D8) and, when the
    /// append rolled the bucket, the id of the file it sealed.
    pub fn append(
        &mut self,
        bucket: u16,
        pid: u64,
        base_offset: u64,
        created_at: i64,
        hashes: &[u128],
        blob: &[u8],
    ) -> std::io::Result<(Pos, Option<u32>)> {
        let count = hashes.len() as u32;
        self.buf.clear();
        let body_len = (HDR - 4 - 8) + hashes.len() * 16 + blob.len();
        self.buf
            .extend_from_slice(&((body_len + 8) as u32).to_le_bytes());
        self.buf.extend_from_slice(&0u64.to_le_bytes()); // checksum placeholder
        self.buf.extend_from_slice(&0u32.to_le_bytes()); // flags
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
        let mut sealed = None;
        let b = &mut self.buckets[bucket as usize];
        if b.len > 0 && b.len + flen > self.segment_bytes {
            let old_id = b.file_id;
            let nid = old_id + 1;
            let (f, len) = open_append(&file_path(&self.dir, bucket, nid))?;
            let old = std::mem::replace(&mut b.f, f);
            let was_dirty = b.dirty;
            b.file_id = nid;
            b.len = len;
            b.dirty = false;
            if was_dirty {
                self.sealed_dirty.push((bucket, old));
            }
            sealed = Some(old_id);
        }
        let b = &mut self.buckets[bucket as usize];
        let offset = b.len;
        b.f.write_all(&self.buf)?;
        b.len += flen;
        b.dirty = true;
        self.appended_bytes += flen;
        self.appended_frames += 1;
        Ok((
            Pos {
                bucket,
                file_id: b.file_id,
                offset,
                len: flen as u32,
            },
            sealed,
        ))
    }

    /// fsyncs every file written since the last durable point (§11.4 step 1).
    pub fn sync_dirty(&mut self, threads: usize, mode: FsyncMode) -> std::io::Result<u64> {
        let mut fds: Vec<RawFd> = Vec::new();
        let sealed = std::mem::take(&mut self.sealed_dirty);
        for (_, f) in &sealed {
            fds.push(f.as_raw_fd());
        }
        for b in self.buckets.iter_mut() {
            if b.dirty {
                fds.push(b.f.as_raw_fd());
                b.dirty = false;
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
        self.fsync_calls += files;
        Ok(files)
    }

    /// (bucket, file_id, length) of every active file: the lengths a durable
    /// store commit records (I11).
    pub fn file_lengths(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(NBUCKETS * 14);
        for (i, b) in self.buckets.iter().enumerate() {
            v.extend_from_slice(&(i as u16).to_le_bytes());
            v.extend_from_slice(&b.file_id.to_le_bytes());
            v.extend_from_slice(&b.len.to_le_bytes());
        }
        v
    }

    /// Reads one frame's hash list. `verify` also reads the payload and checks
    /// the frame checksum (what a paranoid read path would do); without it the
    /// read touches only the header and the hash region, which is the whole
    /// point of keeping the list in front of the blob.
    pub fn read_hashes(
        &mut self,
        pos: &Pos,
        verify: bool,
        out: &mut Vec<u128>,
    ) -> Result<(u64, u64, i64), String> {
        let path = file_path(&self.dir, pos.bucket, pos.file_id);
        let mut f = File::open(&path).map_err(|e| format!("open {}: {e}", path.display()))?;
        f.seek(SeekFrom::Start(pos.offset))
            .map_err(|e| e.to_string())?;
        let want = if verify { pos.len as usize } else { HDR };
        let mut buf = vec![0u8; want];
        f.read_exact(&mut buf)
            .map_err(|e| format!("read {}: {e}", path.display()))?;
        let pid = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let base = u64::from_le_bytes(buf[24..32].try_into().unwrap());
        let count = u32::from_le_bytes(buf[32..36].try_into().unwrap()) as usize;
        let created = i64::from_le_bytes(buf[36..44].try_into().unwrap());
        if !verify {
            let mut hb = vec![0u8; count * 16];
            f.read_exact(&mut hb)
                .map_err(|e| format!("read hashes: {e}"))?;
            out.clear();
            for c in hb.chunks_exact(16) {
                out.push(u128::from_le_bytes(c.try_into().unwrap()));
            }
            self.frame_reads += 1;
            self.frame_read_bytes += (HDR + count * 16) as u64;
        } else {
            let wsum = u64::from_le_bytes(buf[4..12].try_into().unwrap());
            let got = xxh3_64(&buf[12..]);
            if wsum != got {
                return Err(format!(
                    "frame checksum {wsum:#x} != {got:#x} at {path:?}+{}",
                    pos.offset
                ));
            }
            out.clear();
            for c in buf[HDR..HDR + count * 16].chunks_exact(16) {
                out.push(u128::from_le_bytes(c.try_into().unwrap()));
            }
            self.frame_reads += 1;
            self.frame_read_bytes += pos.len as u64;
        }
        Ok((pid, base, created))
    }

    /// Walks every frame of one file, calling `cb(pid, base, created_at,
    /// offset, len, hashes)`. Used to rebuild blooms and the recent cache
    /// after a restart, and by `rewrite_hash_only`.
    pub fn walk_file(
        &self,
        bucket: u16,
        file_id: u32,
        cb: &mut dyn FnMut(u64, u64, i64, u64, u32, &[u128]),
    ) -> Result<u64, String> {
        let path = file_path(&self.dir, bucket, file_id);
        let mut f = match File::open(&path) {
            Ok(f) => f,
            Err(_) => return Ok(0),
        };
        let mut bytes = Vec::new();
        f.read_to_end(&mut bytes).map_err(|e| e.to_string())?;
        let mut off = 0usize;
        let mut hashes: Vec<u128> = Vec::new();
        let mut frames = 0u64;
        while off + HDR <= bytes.len() {
            let flen = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize + 4;
            if flen < HDR || off + flen > bytes.len() {
                break; // torn tail
            }
            let h = &bytes[off..off + flen];
            let pid = u64::from_le_bytes(h[16..24].try_into().unwrap());
            let base = u64::from_le_bytes(h[24..32].try_into().unwrap());
            let count = u32::from_le_bytes(h[32..36].try_into().unwrap()) as usize;
            let created = i64::from_le_bytes(h[36..44].try_into().unwrap());
            if HDR + count * 16 > flen {
                break;
            }
            hashes.clear();
            for c in h[HDR..HDR + count * 16].chunks_exact(16) {
                hashes.push(u128::from_le_bytes(c.try_into().unwrap()));
            }
            cb(pid, base, created, off as u64, flen as u32, &hashes);
            frames += 1;
            off += flen;
        }
        Ok(frames)
    }

    /// Rewrites one sealed file keeping ONLY the hash lists of the frames
    /// `keep` accepts (§11.7: "compaction may copy only the hash lists of
    /// retention-deleted segments"). Returns the new positions and the bytes
    /// before/after. The file is replaced atomically (write temp, fsync,
    /// rename, fsync the directory), so a crash leaves the old file.
    pub fn rewrite_hash_only(
        &mut self,
        bucket: u16,
        file_id: u32,
        keep: &mut dyn FnMut(u64, u64, i64) -> bool,
        mode: FsyncMode,
    ) -> Result<(Vec<(u64, u64, Pos)>, u64, u64), String> {
        let path = file_path(&self.dir, bucket, file_id);
        let before = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let mut frames: Vec<(u64, u64, i64, Vec<u128>)> = Vec::new();
        self.walk_file(bucket, file_id, &mut |pid, base, created, _o, _l, hs| {
            if keep(pid, base, created) {
                frames.push((pid, base, created, hs.to_vec()));
            }
        })?;
        let tmp = path.with_extension("hsnew");
        let mut out = Vec::with_capacity(4096);
        let mut newpos = Vec::with_capacity(frames.len());
        let mut off = 0u64;
        for (pid, base, created, hs) in &frames {
            let start = out.len();
            let body_len = (HDR - 4 - 8) + hs.len() * 16;
            out.extend_from_slice(&((body_len + 8) as u32).to_le_bytes());
            out.extend_from_slice(&0u64.to_le_bytes());
            out.extend_from_slice(&FLAG_HASH_ONLY.to_le_bytes());
            out.extend_from_slice(&pid.to_le_bytes());
            out.extend_from_slice(&base.to_le_bytes());
            out.extend_from_slice(&(hs.len() as u32).to_le_bytes());
            out.extend_from_slice(&created.to_le_bytes());
            for h in hs {
                out.extend_from_slice(&h.to_le_bytes());
            }
            let h = xxh3_64(&out[start + 12..]);
            out[start + 4..start + 12].copy_from_slice(&h.to_le_bytes());
            let flen = (out.len() - start) as u32;
            newpos.push((
                *pid,
                *base,
                Pos {
                    bucket,
                    file_id,
                    offset: off,
                    len: flen,
                },
            ));
            off += flen as u64;
        }
        {
            let mut f = File::create(&tmp).map_err(|e| e.to_string())?;
            f.write_all(&out).map_err(|e| e.to_string())?;
            fsync_fd(f.as_raw_fd(), mode).map_err(|e| e.to_string())?;
        }
        std::fs::rename(&tmp, &path).map_err(|e| e.to_string())?;
        if let Ok(d) = File::open(bucket_dir(&self.dir, bucket)) {
            let _ = fsync_fd(d.as_raw_fd(), mode);
        }
        Ok((newpos, before, out.len() as u64))
    }

    pub fn unlink(&self, bucket: u16, file_id: u32) -> u64 {
        let p = file_path(&self.dir, bucket, file_id);
        let n = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
        let _ = std::fs::remove_file(&p);
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("s2-seg-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn hash_lists_round_trip_without_reading_payloads() {
        let root = tmpdir("rt");
        let mut s = Segments::open(&root, 1 << 20).unwrap();
        let blob = vec![7u8; 4096];
        let hs: Vec<u128> = (0..10u128).map(|i| i * 0x9e37_79b9 + 1).collect();
        let (p, _) = s
            .append(3, 42, 100, 1_700_000_000_000_000, &hs, &blob)
            .unwrap();
        let mut out = Vec::new();
        let (pid, base, created) = s.read_hashes(&p, false, &mut out).unwrap();
        assert_eq!((pid, base, created), (42, 100, 1_700_000_000_000_000));
        assert_eq!(out, hs);
        // the no-verify read must not have touched the blob
        assert!(
            s.frame_read_bytes < 1000,
            "read {} bytes",
            s.frame_read_bytes
        );
        let (_, _, _) = s.read_hashes(&p, true, &mut out).unwrap();
        assert_eq!(out, hs);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn hash_only_rewrite_keeps_hashes_and_drops_payloads() {
        let root = tmpdir("ho");
        let mut s = Segments::open(&root, 1 << 30).unwrap();
        let blob = vec![7u8; 4096];
        let hs: Vec<u128> = (0..10u128).map(|i| i + 1).collect();
        let (_p0, _) = s.append(1, 7, 0, 10, &hs, &blob).unwrap();
        let hs2: Vec<u128> = (100..110u128).collect();
        let (_p1, _) = s.append(1, 8, 0, 11, &hs2, &blob).unwrap();
        s.sync_dirty(1, FsyncMode::Data).unwrap();
        let (newpos, before, after) = s
            .rewrite_hash_only(1, 0, &mut |pid, _base, _c| pid == 8, FsyncMode::Data)
            .unwrap();
        assert_eq!(newpos.len(), 1);
        assert!(after < before / 4, "{after} vs {before}");
        let mut out = Vec::new();
        let (pid, _, created) = s.read_hashes(&newpos[0].2, true, &mut out).unwrap();
        assert_eq!((pid, created), (8, 11));
        assert_eq!(out, hs2);
        let _ = std::fs::remove_dir_all(&root);
    }
}
