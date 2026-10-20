//! The boot refill's file-by-file read ([`QLog::refill_reads`] +
//! [`super::FileReads::read`]) against the read it replaces,
//! [`QLog::committed_frames`] per partition:
//!
//! - the same records with the same hash blocks, for whole and partial spans,
//!   a committed bound below the tail, active and sealed files, before and
//!   after a reopen, and a txn-envelope record (read whole);
//! - records far apart are read alone (a few prefixes, not the file), records
//!   close together share reads, and a run longer than one read is split.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::{CommittedFrame, QLog, QLogOptions, RecordInput, TxnInput};

/// A directory that removes itself, named after its test.
struct TmpDir(PathBuf);

impl TmpDir {
    fn new(tag: &str) -> TmpDir {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "queen-rsm-qlog-refill-{tag}-{}-{}",
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

/// One record per call: `count` messages of partition `pid` at `base`, with a
/// payload of `payload` bytes, a txn envelope when `txn`.
fn append(q: &mut QLog, seq: u64, pid: u64, base: u64, count: u64, payload: usize, txn: bool) {
    let h = hashes(pid, base, count);
    let p = vec![(seq % 251) as u8; payload];
    let parts = [pid, pid + 1000];
    q.append_group(&[RecordInput {
        seq,
        pid,
        base_offset: base,
        count: count as u32,
        created_at_us: 1_000_000 + seq as i64,
        txn: txn.then_some(TxnInput {
            gtid: 0xABCD + seq as u128,
            participants: &parts,
        }),
        hashes: &h,
        payload: &p,
    }])
    .expect("append");
}

/// What the refill reads for `spans`, per partition, ascending by base; and
/// the bytes it read.
fn refill(q: &QLog, spans: &[(u64, u64, u64)]) -> (BTreeMap<u64, Vec<CommittedFrame>>, u64) {
    let mut out: BTreeMap<u64, Vec<CommittedFrame>> = BTreeMap::new();
    let mut read = 0;
    let mut buf = Vec::new();
    for file in q.refill_reads(spans) {
        let (frames, n) = file.read(&mut buf).expect("refill read");
        read += n;
        for (pid, f) in frames {
            out.entry(pid).or_default().push(f);
        }
    }
    for v in out.values_mut() {
        v.sort_by_key(|f| f.base_offset);
    }
    (out, read)
}

/// What `committed_frames` answers for each span (records of no message left
/// out: they have no hash block and no row).
fn expected(q: &QLog, spans: &[(u64, u64, u64)]) -> BTreeMap<u64, Vec<CommittedFrame>> {
    let mut out = BTreeMap::new();
    for &(pid, from, end) in spans {
        let frames: Vec<CommittedFrame> = q
            .committed_frames(pid, from, end, true)
            .expect("committed frames")
            .into_iter()
            .filter(|f| f.end > f.base_offset)
            .collect();
        if !frames.is_empty() {
            out.insert(pid, frames);
        }
    }
    out
}

/// The bytes of every `.qlog` file under `dir` (a log lives in `q<id>/`).
fn log_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .expect("log dir")
        .filter_map(|e| e.ok())
        .map(|e| {
            let p = e.path();
            if p.is_dir() {
                log_bytes(&p)
            } else if p.extension().is_some_and(|x| x == "qlog") {
                e.metadata().map(|m| m.len()).unwrap_or(0)
            } else {
                0
            }
        })
        .sum()
}

#[test]
fn the_refill_reads_answer_what_committed_frames_answers() {
    let td = TmpDir::new("same");
    let opts = QLogOptions::testing(16 << 10);
    let (mut q, _) = QLog::open(td.path(), 7, opts).unwrap();
    // Three partitions interleaved over many small files: 5 with 4-message
    // records, 6 with 1-message records, 9 with a txn envelope every 7th.
    let (mut b5, mut b6, mut b9) = (0u64, 0u64, 0u64);
    for k in 0..300u64 {
        append(&mut q, 3 * k + 1, 5, b5, 4, 120, false);
        b5 += 4;
        append(&mut q, 3 * k + 2, 6, b6, 1, 40, false);
        b6 += 1;
        append(&mut q, 3 * k + 3, 9, b9, 2, 300, k % 7 == 3);
        b9 += 2;
    }
    assert!(q.file_count() > 10, "files: {}", q.file_count());
    let spans_sets: [&[(u64, u64, u64)]; 3] = [
        // Whole windows.
        &[(5, 0, b5), (6, 0, b6), (9, 0, b9)],
        // Partial: a span starting inside a record, a committed bound below
        // the tail, and a partition this log does not hold.
        &[(5, 401, b5 - 8), (6, 250, b6), (9, 100, 400), (77, 0, 10)],
        // One partition alone.
        &[(6, 0, 120)],
    ];
    for spans in spans_sets {
        let (got, _) = refill(&q, spans);
        assert_eq!(got, expected(&q, spans), "spans {spans:?}");
    }
    // Reopened: every sealed file answers from its .qidx.
    drop(q);
    let (q, rep) = QLog::open(td.path(), 7, opts).unwrap();
    assert_eq!(rep.rebuilt_indexes, 0);
    for spans in spans_sets {
        let (got, _) = refill(&q, spans);
        assert_eq!(got, expected(&q, spans), "after reopen, spans {spans:?}");
    }
}

#[test]
fn records_far_apart_are_read_alone_and_close_ones_together() {
    let td = TmpDir::new("gaps");
    // One big file: 40 records of partition 1 with 1 MiB payloads, each
    // followed by a small record of partition 2; then 100 records of
    // partition 3 with 120 KiB payloads (12 MiB, more than one read).
    let opts = QLogOptions::testing(256 << 20);
    let (mut q, _) = QLog::open(td.path(), 8, opts).unwrap();
    for k in 0..40u64 {
        append(&mut q, 2 * k + 1, 1, k, 1, 1 << 20, false);
        append(&mut q, 2 * k + 2, 2, 3 * k, 3, 64, false);
    }
    for k in 0..100u64 {
        append(&mut q, 100 + k, 3, 2 * k, 2, 120 << 10, false);
    }
    assert_eq!(q.file_count(), 1);
    let file = log_bytes(td.path());

    // Partition 2 alone: its records are 1 MiB apart, so only their own
    // header + hashes are read, not the payloads between them.
    let spans = [(2, 0, 120)];
    let (got, read) = refill(&q, &spans);
    assert_eq!(got, expected(&q, &spans));
    assert!(read < 64 << 10, "read {read} bytes for 40 small records");

    // Partition 3: 120 KiB apart, so the reads run through the payloads
    // (about 11.6 MiB from its first record to its last), in two reads.
    let spans = [(3, 0, 200)];
    let (got, read) = refill(&q, &spans);
    assert_eq!(got, expected(&q, &spans));
    assert!(
        (11 << 20..13 << 20).contains(&read),
        "read {read} bytes for 100 records 120 KiB apart"
    );

    // All three at once.
    let spans = [(1, 0, 40), (2, 0, 120), (3, 0, 200)];
    let (got, read) = refill(&q, &spans);
    assert_eq!(got, expected(&q, &spans));
    assert!(read <= file, "read {read} of a {file}-byte file");
}
