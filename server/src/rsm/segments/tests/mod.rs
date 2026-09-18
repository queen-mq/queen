//! The tests of `rsm/segments/` (WP-1.3).
//!
//! - [`frames`] the frame of §11.2 on its own: what it encodes, what it
//!   refuses, and that a damaged byte is always caught.
//! - [`indexes`] the `.qidx`: its header and record checksums, the binary
//!   search, and the four answers a probe can give.
//! - [`files`] append, roll, seal, read: the write path and the lookup across
//!   many files, with checksum failures detected on the way out.
//! - [`recovery`] §11.5 against the store's recorded lengths: truncate a tail,
//!   refuse a short file, rebuild a missing `.qidx` by scanning and prove the
//!   rebuild equals the one that was written.
//! - [`gc`] §11.7: live bytes, the txns window, snapshot references and claim
//!   pins, and that none of them can be unlinked past.
//! - [`crash`] a real `kill -9` of a child process around a roll.
//! - [`measure`] the two numbers WP-1.11 has to budget for, printed on demand
//!   (`--ignored --nocapture`), never asserted.
//!
//! Everything writes into a temporary directory of its own and removes it,
//! and every file the tests make is a few kilobytes: this laptop has ~10 GiB
//! free and the standing rule is no large fixtures.

mod crash;
mod files;
mod frames;
mod gc;
mod indexes;
mod measure;
mod recovery;

use std::path::{Path, PathBuf};

use super::*;

/// A directory that removes itself. One per test, named after it, so a failure
/// leaves a findable path in the message and a passing run leaves nothing.
pub struct TmpDir(PathBuf);

impl TmpDir {
    pub fn new(tag: &str) -> TmpDir {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "queen-rsm-seg-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).expect("temp dir");
        TmpDir(p)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn seg(&self) -> PathBuf {
        self.0.join("seg")
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `count` hashes, distinct and reproducible, in the 16-byte stride of D10.
pub fn hashes(seed: u64, count: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(count as usize * 16);
    for i in 0..count as u64 {
        v.extend_from_slice(&(seed ^ (i << 32)).to_le_bytes());
        v.extend_from_slice(&(seed.wrapping_mul(0x9e37_79b9).wrapping_add(i)).to_le_bytes());
    }
    v
}

/// A payload whose bytes depend on the seed, so a read that returns the wrong
/// frame is visible rather than plausible.
pub fn blob(seed: u64, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (seed as u8).wrapping_add(i as u8))
        .collect()
}

/// Open a fresh tree (no recorded state).
pub fn fresh(dir: &TmpDir, segment_bytes: u64) -> Segments {
    let (s, rec) = Segments::open(&dir.seg(), Options::testing(segment_bytes), &[])
        .expect("open a fresh segment tree");
    assert_eq!(rec, Recovery::default(), "a fresh tree recovers nothing");
    s
}

/// Append one frame of `n` messages with a payload of `blob_len` bytes.
pub fn push(
    s: &mut Segments,
    bucket: u16,
    pid: Pid,
    base: u64,
    n: u32,
    blob_len: usize,
) -> Position {
    s.append(
        bucket,
        pid,
        base,
        n,
        1_700_000_000_000_000 + base as i64,
        &hashes(pid ^ base, n),
        &blob(pid ^ base, blob_len),
    )
    .expect("append")
}

/// Flip one byte of a file at `at`.
pub fn flip(path: &Path, at: u64) {
    let mut bytes = std::fs::read(path).expect("read to corrupt");
    bytes[at as usize] ^= 0xff;
    std::fs::write(path, &bytes).expect("write back");
}

pub fn seg_file(dir: &TmpDir, bucket: u16, id: u32) -> PathBuf {
    dir.seg()
        .join(format!("b{bucket:03}"))
        .join(format!("f{id:010}.seg"))
}

pub fn qidx_file(dir: &TmpDir, bucket: u16, id: u32) -> PathBuf {
    dir.seg()
        .join(format!("b{bucket:03}"))
        .join(format!("f{id:010}.qidx"))
}
