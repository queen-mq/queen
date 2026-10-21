//! A sealed queue-log file's hash lists on their own (`r<id>.qhsh`): what the
//! boot's refill of the `txns` rows ([`crate::rsm::txns_refill`]) reads
//! instead of the file.
//!
//! The refill needs each message record's hash list, 16 bytes a message. In
//! the `.qlog` a list sits right after its record's header, in front of the
//! payload, and the refill reads each record's header and list; records less
//! than [`super::REFILL_READ_GAP`] apart share one read through the payloads
//! between them. With large records that reads the whole file: right after a
//! 420 MB/s fill, a restart's refill read 310 GB in 300 s, nine tenths of the
//! boot, where the lists themselves were under a gigabyte (three GCP nodes,
//! 2026-10-20).
//!
//! A sidecar holds, for every message record of one sealed file, in file
//! order: `offset u64 | pid u64 | base_offset u64 | count u32 |
//! created_at_us i64 | hashes [16 * count]` (little-endian), then a footer:
//! `magic u64 | file_id u64 | file_bytes u64 | entries u64 | body xxh3 u64 |
//! footer xxh3 u64`, the last over the five words before it.
//!
//! It is written after its file sealed, off the log writer (the maintenance
//! pass, [`super::set::QLogReader::hashside_maintain`]), from the file's own
//! reads, each checked against the record's index entry exactly as the
//! refill's read checks it, and handed to the device in steps it waits for
//! (never left to pile up as dirty pages in front of the queue logs' fsyncs).
//! It is never fsynced and never trusted unchecked:
//! a missing, torn or stale sidecar (another file length, a checksum that
//! fails, a record the refill wants that it does not name or names with
//! another header) is not used, and the refill reads the file as it always
//! did. Node-local: no replicated byte depends on one.

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use xxhash_rust::xxh3::xxh3_64;

use super::{index, CommittedFrame};

/// `QHSIDE01`, little-endian.
const MAGIC: u64 = u64::from_le_bytes(*b"QHSIDE01");

/// `offset | pid | base_offset | count | created_at_us`.
const ENTRY_FIXED: usize = 8 + 8 + 8 + 4 + 8;

/// Six words.
const FOOTER: usize = 6 * 8;

/// `QUEEN_QLOG_HASH_SIDECARS` (default on). Off: no sidecar is built, and the
/// refill reads every file itself. Read by the maintenance pass and the
/// refill only, never by the log writer.
#[allow(clippy::disallowed_methods)]
pub(crate) fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("QUEEN_QLOG_HASH_SIDECARS")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "false" | "off" | "no"
        )
    })
}

/// The sidecar of file `id` in log directory `dir`.
pub(crate) fn path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("r{id:08}.qhsh"))
}

fn tmp_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("r{id:08}.qhsh.tmp"))
}

/// The sidecar of `file_id`, `file_bytes` long, for `entries`: every message
/// record of the file in file order with its hash list (`16 * count` bytes).
pub(crate) fn encode(file_id: u64, file_bytes: u64, entries: &[(index::Record, &[u8])]) -> Vec<u8> {
    let body: usize = entries.iter().map(|(_, h)| ENTRY_FIXED + h.len()).sum();
    let mut out = Vec::with_capacity(body + FOOTER);
    for (r, h) in entries {
        out.extend_from_slice(&r.offset.to_le_bytes());
        out.extend_from_slice(&r.pid.to_le_bytes());
        out.extend_from_slice(&r.base_offset.to_le_bytes());
        out.extend_from_slice(&r.count.to_le_bytes());
        out.extend_from_slice(&r.created_at_us.to_le_bytes());
        out.extend_from_slice(h);
    }
    let body_sum = xxh3_64(&out);
    let footer = out.len();
    for w in [MAGIC, file_id, file_bytes, entries.len() as u64, body_sum] {
        out.extend_from_slice(&w.to_le_bytes());
    }
    let footer_sum = xxh3_64(&out[footer..]);
    out.extend_from_slice(&footer_sum.to_le_bytes());
    out
}

/// Write the sidecar of `file_id` (temporary + rename, no fsync: see the
/// module header). Returns its length. Every list must be `16 * count` bytes.
pub(crate) fn write(
    dir: &Path,
    file_id: u64,
    file_bytes: u64,
    entries: &[(index::Record, &[u8])],
) -> io::Result<u64> {
    if let Some((r, h)) = entries
        .iter()
        .find(|(r, h)| h.len() != 16 * r.count as usize)
    {
        return Err(io::Error::other(format!(
            "record at byte {} of file {file_id}: {} hash bytes for {} messages",
            r.offset,
            h.len(),
            r.count
        )));
    }
    let bytes = encode(file_id, file_bytes, entries);
    let tmp = tmp_path(dir, file_id);
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        // Handed to the device a step at a time, each waited for here, as a
        // directory run is (`dirx::sync_range`): left to the kernel's
        // writeback, the sidecars of a busy log piled up as dirty pages (1.7 GB
        // on the leader at 3M msg/s of 256-byte messages, 48 MB/s of hash
        // lists) and reached the device in bursts the queue logs' fsyncs
        // waited behind: their longest went from 6 to 206 ms and the end to
        // end p99 from 17 to 145 ms (three GCP nodes, 2026-10-21). Nothing is
        // made durable: a sidecar never needs to be.
        let step = super::dirx::SYNC_STEP as usize;
        let mut at = 0usize;
        while at < bytes.len() {
            let n = step.min(bytes.len() - at);
            f.write_all(&bytes[at..at + n])?;
            super::dirx::sync_range(&f, at as u64, n as u64)?;
            at += n;
        }
    }
    std::fs::rename(&tmp, path(dir, file_id))?;
    Ok(bytes.len() as u64)
}

/// Remove the sidecar of file `id` and any temporary of it, if present.
pub(crate) fn remove(dir: &Path, id: u64) -> io::Result<()> {
    for p in [path(dir, id), tmp_path(dir, id)] {
        match std::fs::remove_file(&p) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Remove every sidecar (and temporary) in `dir` whose file `keep` does not
/// name: a reclaimed file's, or the active file's (a sidecar names a sealed
/// file only). The boot's sweep, from [`super::QLog::open`].
pub(crate) fn sweep(dir: &Path, keep: impl Fn(u64) -> bool) -> io::Result<usize> {
    let mut removed = 0;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(rest) = name.strip_prefix('r') else {
            continue;
        };
        let num = rest
            .strip_suffix(".qhsh")
            .or_else(|| rest.strip_suffix(".qhsh.tmp"));
        let Some(id) = num.and_then(|n| n.parse::<u64>().ok()) else {
            continue;
        };
        if !keep(id) || name.ends_with(".tmp") {
            match std::fs::remove_file(entry.path()) {
                Ok(()) => removed += 1,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
    }
    Ok(removed)
}

/// One record a sidecar names.
#[derive(Clone, Copy, Debug)]
struct Entry {
    offset: u64,
    pid: u64,
    base_offset: u64,
    count: u32,
    created_at_us: i64,
    /// Where its hash list starts in the sidecar's bytes.
    at: usize,
}

/// A sidecar read whole and checked ([`load`]).
pub(crate) struct Side {
    buf: Vec<u8>,
    entries: Vec<Entry>,
}

/// The sidecar of `file_id` when it is whole and names a file `file_bytes`
/// long: footer, both checksums, every entry inside the body and in strictly
/// ascending file order. `None` otherwise (missing included).
pub(crate) fn load(dir: &Path, file_id: u64, file_bytes: u64) -> Option<Side> {
    parse(std::fs::read(path(dir, file_id)).ok()?, file_id, file_bytes)
}

fn parse(buf: Vec<u8>, file_id: u64, file_bytes: u64) -> Option<Side> {
    let footer = buf.len().checked_sub(FOOTER)?;
    let word = |i: usize| -> u64 {
        let at = footer + 8 * i;
        u64::from_le_bytes(buf[at..at + 8].try_into().expect("8 bytes"))
    };
    if word(0) != MAGIC || xxh3_64(&buf[footer..footer + 40]) != word(5) {
        return None;
    }
    if word(1) != file_id || word(2) != file_bytes || xxh3_64(&buf[..footer]) != word(4) {
        return None;
    }
    let mut entries = Vec::with_capacity(word(3).min(1 << 20) as usize);
    let mut at = 0usize;
    while at < footer {
        if footer - at < ENTRY_FIXED {
            return None;
        }
        let u = |o: usize| u64::from_le_bytes(buf[at + o..at + o + 8].try_into().expect("8 bytes"));
        let count = u32::from_le_bytes(buf[at + 24..at + 28].try_into().expect("4 bytes"));
        let e = Entry {
            offset: u(0),
            pid: u(8),
            base_offset: u(16),
            count,
            created_at_us: u(28) as i64,
            at: at + ENTRY_FIXED,
        };
        let end = e.at.checked_add(16usize.checked_mul(count as usize)?)?;
        if end > footer || entries.last().is_some_and(|p: &Entry| p.offset >= e.offset) {
            return None;
        }
        entries.push(e);
        at = end;
    }
    (entries.len() as u64 == word(3)).then_some(Side { buf, entries })
}

impl Side {
    /// Its length on disk: what the refill read for this file.
    pub(crate) fn len(&self) -> u64 {
        self.buf.len() as u64
    }

    /// The refill's frames for `records` (ascending by offset, as
    /// [`super::FileReads`] holds them), each from the entry at its offset,
    /// whose header must be the index entry's. `None` when one is missing or
    /// disagrees: the caller reads the file instead.
    pub(crate) fn frames_for(
        &self,
        records: &[index::Record],
    ) -> Option<Vec<(u64, CommittedFrame)>> {
        let mut out = Vec::with_capacity(records.len());
        let mut j = 0usize;
        for r in records {
            while self.entries.get(j).is_some_and(|e| e.offset < r.offset) {
                j += 1;
            }
            let e = self.entries.get(j)?;
            if e.offset != r.offset
                || e.pid != r.pid
                || e.base_offset != r.base_offset
                || e.count != r.count
                || e.created_at_us != r.created_at_us
            {
                return None;
            }
            out.push((
                r.pid,
                CommittedFrame {
                    base_offset: r.base_offset,
                    end: r.end,
                    created_at_us: r.created_at_us,
                    hashes: self.buf[e.at..e.at + 16 * e.count as usize].to_vec(),
                },
            ));
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(offset: u64, pid: u64, base: u64, count: u32, created: i64) -> index::Record {
        index::Record {
            pid,
            base_offset: base,
            end: base + count as u64,
            created_at_us: created,
            offset,
            count,
            len: 100,
        }
    }

    fn lists(n: usize) -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| (0..16 * (i % 3 + 1)).map(|b| (i * 7 + b) as u8).collect())
            .collect()
    }

    fn sample() -> (Vec<index::Record>, Vec<Vec<u8>>) {
        let recs: Vec<_> = (0..5u64)
            .map(|i| {
                rec(
                    24 + 200 * i,
                    10 + i % 2,
                    3 * i,
                    (i % 3 + 1) as u32,
                    1_000 + i as i64,
                )
            })
            .collect();
        (recs, lists(5))
    }

    #[test]
    fn a_sidecar_answers_what_was_written_and_nothing_else() {
        let (recs, hs) = sample();
        let entries: Vec<_> = recs
            .iter()
            .copied()
            .zip(hs.iter().map(Vec::as_slice))
            .collect();
        let bytes = encode(7, 4096, &entries);
        let side = parse(bytes.clone(), 7, 4096).expect("whole");
        assert_eq!(side.len(), bytes.len() as u64);
        // Every record, and any subset in order, comes back with its list.
        let frames = side.frames_for(&recs).expect("all");
        for ((pid, f), (r, h)) in frames.iter().zip(&entries) {
            assert_eq!(
                (*pid, f.base_offset, f.end, f.created_at_us),
                (r.pid, r.base_offset, r.end, r.created_at_us)
            );
            assert_eq!(&f.hashes[..], *h);
        }
        let some = [recs[1], recs[4]];
        assert_eq!(side.frames_for(&some).expect("subset").len(), 2);
        // Another file, another length: not this sidecar.
        assert!(parse(bytes.clone(), 8, 4096).is_none());
        assert!(parse(bytes.clone(), 7, 4097).is_none());
        // A record it does not name, or names with another header: no answer.
        let mut other = recs[2];
        other.offset += 1;
        assert!(side.frames_for(&[other]).is_none());
        let mut other = recs[2];
        other.created_at_us += 1;
        assert!(side.frames_for(&[other]).is_none());
        let mut other = recs[2];
        other.count += 1;
        assert!(side.frames_for(&[other]).is_none());
    }

    #[test]
    fn a_damaged_or_torn_sidecar_is_refused() {
        let (recs, hs) = sample();
        let entries: Vec<_> = recs
            .iter()
            .copied()
            .zip(hs.iter().map(Vec::as_slice))
            .collect();
        let bytes = encode(3, 999, &entries);
        for i in [
            0,
            30,
            bytes.len() / 2,
            bytes.len() - FOOTER,
            bytes.len() - 1,
        ] {
            let mut b = bytes.clone();
            b[i] ^= 0x41;
            assert!(
                parse(b, 3, 999).is_none(),
                "a flipped byte at {i} was accepted"
            );
        }
        for cut in [0, 1, FOOTER, bytes.len() - 1] {
            assert!(
                parse(bytes[..cut].to_vec(), 3, 999).is_none(),
                "torn at {cut}"
            );
        }
        // An empty file of no message record is whole.
        assert!(parse(encode(3, 999, &[]), 3, 999).is_some());
    }

    #[test]
    fn a_list_of_the_wrong_length_is_never_written() {
        let dir = std::env::temp_dir().join(format!("queen-hashside-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let r = rec(24, 1, 0, 2, 5);
        assert!(write(&dir, 1, 100, &[(r, &[0u8; 16][..])]).is_err());
        assert!(!path(&dir, 1).exists());
        assert!(write(&dir, 1, 100, &[(r, &[0u8; 32][..])]).is_ok());
        assert!(load(&dir, 1, 100).is_some());
        assert!(load(&dir, 1, 101).is_none());
        // The sweep keeps the named, drops the others and every temporary.
        std::fs::write(tmp_path(&dir, 1), b"x").unwrap();
        std::fs::write(path(&dir, 2), b"x").unwrap();
        assert_eq!(sweep(&dir, |id| id == 1).unwrap(), 2);
        assert!(path(&dir, 1).exists() && !path(&dir, 2).exists() && !tmp_path(&dir, 1).exists());
        remove(&dir, 1).unwrap();
        assert!(!path(&dir, 1).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
