//! A node's recovery, from the queue logs' side (2.6):
//!
//! - a reopened log MAPS its sealed files' indexes without reading them;
//!   retention judges none of those files until [`QLog::check_index`] has
//!   read its index, and a damaged index the check finds is rebuilt from the
//!   file;
//! - the runs of a delta snapshot ([`seq_run`], [`QLog::append_raw`]): a log
//!   rebuilt from another log's runs answers exactly what that log answers —
//!   plain, zstd and txn-envelope message records and entry records, across
//!   many files — whether it starts empty or from its own copy cut at the
//!   basis.

use std::path::{Path, PathBuf};

use super::{
    codec, qidx_path, seq_run, CommittedFrame, EntryInput, QLog, QLogOptions, RecordInput,
    TxnInput, WriteRecord,
};

/// A directory that removes itself, named after its test.
struct TmpDir(PathBuf);

impl TmpDir {
    fn new(tag: &str) -> TmpDir {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "queen-rsm-qlog-recovery-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).expect("temp dir");
        TmpDir(p)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

const PIDS: [u64; 3] = [1, 2, 3];

fn hashes(pid: u64, base: u64, count: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(count as usize * 16);
    for off in base..base + count {
        let mut k = [0u8; 16];
        k[..8].copy_from_slice(&pid.to_le_bytes());
        k[8..].copy_from_slice(&off.to_le_bytes());
        out.extend_from_slice(&xxhash_rust::xxh3::xxh3_128(&k).to_le_bytes());
    }
    out
}

/// Entries `from..=to` into `q`, one write each: a message record of
/// partition `PIDS[seq % 3]` (1 to 4 messages; every 5th payload stored zstd,
/// every 7th record with a txn envelope), then the entry's record.
fn write_entries(q: &mut QLog, from: u64, to: u64, next: &mut [u64; 3]) {
    let parts = [11u64, 12];
    for seq in from..=to {
        let k = (seq % 3) as usize;
        let pid = PIDS[k];
        let count = seq % 4 + 1;
        let base = next[k];
        next[k] += count;
        let h = hashes(pid, base, count);
        let raw: Vec<u8> = (0..200 + seq % 300).map(|i| (seq + i) as u8 % 7).collect();
        let txn = (seq % 7 == 3).then_some(TxnInput {
            gtid: 0xABCD + u128::from(seq),
            participants: &parts,
        });
        let zstd = (seq % 5 == 1).then(|| codec::compress_one(&raw)).flatten();
        let msg = RecordInput {
            seq,
            pid,
            base_offset: base,
            count: count as u32,
            created_at_us: 1_000_000 + seq as i64,
            txn,
            hashes: &h,
            payload: zstd.as_deref().unwrap_or(&raw),
        };
        let entry = format!("entry {seq}").into_bytes();
        let records = [
            if zstd.is_some() && txn.is_none() {
                WriteRecord::Zstd(msg)
            } else {
                WriteRecord::Msg(msg)
            },
            WriteRecord::Entry(EntryInput {
                seq,
                now_us: 2_000_000 + seq as i64,
                copies: 1,
                term: 3,
                entry: &entry,
            }),
        ];
        q.write_mixed(&records).expect("write");
    }
    q.sync().expect("sync");
}

/// Per partition: its committed frames and every payload.
type Answers = Vec<(u64, Vec<CommittedFrame>, Vec<Option<Vec<u8>>>)>;

/// What a log answers for every partition: its committed frames (hashes
/// included) and every payload.
fn answers(q: &QLog) -> Answers {
    PIDS.iter()
        .map(|pid| {
            let frames = q.committed_frames(*pid, 0, u64::MAX, true).expect("frames");
            let end = frames.last().map_or(0, |f| f.end);
            let payloads = (0..end)
                .map(|off| q.read_payload(*pid, off).expect("read"))
                .collect();
            (*pid, frames, payloads)
        })
        .collect()
}

/// The bytes of `q`'s runs of records with `seq` in `(after, upto]`, in file
/// order: what a delta snapshot carries for this log.
fn runs_of(q: &QLog, after: u64, upto: u64) -> Vec<Vec<u8>> {
    use std::os::unix::fs::FileExt;
    q.seq_candidates(after, upto)
        .into_iter()
        .filter_map(|(_, path, bytes, first, max)| {
            let (start, end) = seq_run(&path, bytes, first, max, after, upto).expect("run")?;
            let mut buf = vec![0u8; (end - start) as usize];
            std::fs::File::open(&path)
                .unwrap()
                .read_exact_at(&mut buf, start)
                .unwrap();
            Some(buf)
        })
        .collect()
}

#[test]
fn a_reopened_log_checks_its_indexes_before_retention_judges_them() {
    let td = TmpDir::new("checked");
    let opts = QLogOptions::testing(4 << 10);
    let mut next = [0u64; 3];
    {
        let (mut q, _) = QLog::open(td.path(), 5, opts).unwrap();
        write_entries(&mut q, 1, 200, &mut next);
        assert!(q.file_count() > 5);
    }
    let (mut q, rec) = QLog::open(td.path(), 5, opts).unwrap();
    assert_eq!(rec.rebuilt_indexes, 0);
    let sealed = q.files().iter().filter(|m| m.sealed).count();
    assert!(q.files().iter().filter(|m| m.sealed).all(|m| !m.checked));
    // Mapped, not read: reads work, retention waits.
    let before = answers(&q);
    assert_eq!(
        q.unlink_dead_files(|_| true).unwrap(),
        0,
        "an unchecked file died"
    );
    assert_eq!(q.check_all_indexes().unwrap(), sealed);
    assert!(q.files().iter().all(|m| m.checked));
    let first = q.files()[0];
    assert!(
        first.min_created_at_us <= first.max_created_at_us,
        "the time range is taken"
    );
    assert_eq!(answers(&q), before);
    assert_eq!(q.unlink_dead_files(|_| true).unwrap(), sealed);
}

#[test]
fn a_damaged_index_is_rebuilt_by_the_check() {
    let td = TmpDir::new("damaged");
    let opts = QLogOptions::testing(4 << 10);
    let mut next = [0u64; 3];
    let (dir, victim) = {
        let (mut q, _) = QLog::open(td.path(), 6, opts).unwrap();
        write_entries(&mut q, 1, 120, &mut next);
        let victim = q
            .files()
            .iter()
            .find(|m| m.sealed && m.records > 2)
            .unwrap()
            .id;
        (q.dir().to_path_buf(), victim)
    };
    let truth = {
        let (q, _) = QLog::open(td.path(), 6, opts).unwrap();
        let mut q = q;
        q.check_all_indexes().unwrap();
        answers(&q)
    };
    // A flipped byte in the record array: the header still verifies, so the
    // open maps it; the check finds it.
    let path = qidx_path(&dir, victim);
    let mut bytes = std::fs::read(&path).unwrap();
    let last = bytes.len() - 3;
    bytes[last] ^= 0x5A;
    std::fs::write(&path, &bytes).unwrap();
    let (mut q, rec) = QLog::open(td.path(), 6, opts).unwrap();
    assert_eq!(rec.rebuilt_indexes, 0, "the open does not read the records");
    q.check_all_indexes().unwrap();
    assert_eq!(answers(&q), truth, "answers from the rebuilt index");
    super::index::View::open(&path, None).expect("the index on disk is whole again");
}

#[test]
fn a_log_rebuilt_from_another_logs_runs_answers_the_same() {
    let src_dir = TmpDir::new("runs-src");
    let opts = QLogOptions::testing(4 << 10);
    let mut next = [0u64; 3];
    let (mut src, _) = QLog::open(src_dir.path(), 9, opts).unwrap();
    write_entries(&mut src, 1, 300, &mut next);
    assert!(src.file_count() > 8, "runs across many files");
    let (basis, upto) = (137u64, 300u64);

    // From nothing: everything up to the basis, then the delta.
    let dst_dir = TmpDir::new("runs-dst");
    let (mut dst, _) = QLog::open(dst_dir.path(), 9, opts).unwrap();
    for run in runs_of(&src, 0, basis) {
        dst.append_raw(&run).unwrap();
    }
    assert_eq!(dst.durable_tail(), basis);
    for run in runs_of(&src, basis, upto) {
        dst.append_raw(&run).unwrap();
    }
    assert_eq!(dst.durable_tail(), upto);
    assert_eq!(answers(&dst), answers(&src));
    drop(dst);
    let (dst, _) = QLog::open(dst_dir.path(), 9, opts).unwrap();
    assert_eq!(answers(&dst), answers(&src), "after a reopen");

    // From its own copy past the basis: cut there, then the delta — what a
    // member that applied more than the basis does at the swap.
    let own_dir = TmpDir::new("runs-own");
    let own_log = {
        let (mut own, _) = QLog::open(own_dir.path(), 9, opts).unwrap();
        let mut own_next = [0u64; 3];
        write_entries(&mut own, 1, 210, &mut own_next);
        own.dir().to_path_buf()
    };
    assert!(super::cut_log_above(&own_log, basis).unwrap() > 0);
    let (mut own, _) = QLog::open(own_dir.path(), 9, opts).unwrap();
    assert_eq!(own.durable_tail(), basis);
    for run in runs_of(&src, basis, upto) {
        own.append_raw(&run).unwrap();
    }
    assert_eq!(answers(&own), answers(&src));

    // A run that would go below the tail is refused.
    let again = runs_of(&src, basis, upto);
    assert!(own.append_raw(&again[0]).is_err());
}
