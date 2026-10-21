//! The boot refill's file-by-file read ([`QLog::refill_reads`] +
//! [`super::FileReads::read`]) against the read it replaces,
//! [`QLog::committed_frames`] per partition:
//!
//! - the same records with the same hash blocks, for whole and partial spans,
//!   a committed bound below the tail, active and sealed files, before and
//!   after a reopen, and a txn-envelope record (read whole);
//! - records far apart are read alone (a few prefixes, not the file), records
//!   close together share reads, and a run longer than one read is split;
//! - a sealed file's hash sidecar ([`super::hashside`]) answers in the file's
//!   place with the same frames, reading a fraction of the bytes; one that
//!   does not fit is dropped and the file read; retention, a cut and a reopen
//!   leave no sidecar beside a file it does not name.

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
        let got = file.read(&mut buf).expect("refill read");
        read += got.bytes;
        for (pid, f) in got.frames {
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

/// Build every hash sidecar `q` owes, as the maintenance pass does.
fn build_sidecars(q: &QLog) -> usize {
    let mut n = 0;
    while let Some(plan) = q.hashside_plan() {
        plan.run().expect("sidecar");
        n += 1;
    }
    n
}

/// [`refill`], with the number of files a sidecar answered.
fn refill_counted(
    q: &QLog,
    spans: &[(u64, u64, u64)],
) -> (BTreeMap<u64, Vec<CommittedFrame>>, u64, usize) {
    let mut out: BTreeMap<u64, Vec<CommittedFrame>> = BTreeMap::new();
    let (mut read, mut sides) = (0, 0);
    let mut buf = Vec::new();
    for file in q.refill_reads(spans) {
        let got = file.read(&mut buf).expect("refill read");
        read += got.bytes;
        sides += usize::from(got.sidecar);
        for (pid, f) in got.frames {
            out.entry(pid).or_default().push(f);
        }
    }
    for v in out.values_mut() {
        v.sort_by_key(|f| f.base_offset);
    }
    (out, read, sides)
}

fn sidecars_in(dir: &Path) -> Vec<u64> {
    let mut ids: Vec<u64> = std::fs::read_dir(dir)
        .expect("log dir")
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let n = e.file_name().to_string_lossy().into_owned();
            n.strip_prefix('r')?.strip_suffix(".qhsh")?.parse().ok()
        })
        .collect();
    ids.sort_unstable();
    ids
}

#[test]
fn a_sealed_files_hash_sidecar_answers_the_refill_in_its_place() {
    let td = TmpDir::new("side");
    let opts = QLogOptions::testing(16 << 10);
    let (mut q, _) = QLog::open(td.path(), 7, opts).unwrap();
    let (mut b5, mut b6, mut b9) = (0u64, 0u64, 0u64);
    for k in 0..300u64 {
        append(&mut q, 3 * k + 1, 5, b5, 4, 120, false);
        b5 += 4;
        append(&mut q, 3 * k + 2, 6, b6, 1, 40, false);
        b6 += 1;
        append(&mut q, 3 * k + 3, 9, b9, 2, 300, k % 7 == 3);
        b9 += 2;
    }
    let sealed = q.files().iter().filter(|m| m.sealed).count();
    assert!(sealed > 10);
    let spans: &[(u64, u64, u64)] = &[(5, 0, b5), (6, 401, b6), (9, 0, b9 - 6)];
    let (plain, plain_read, sides) = refill_counted(&q, spans);
    assert_eq!(plain, expected(&q, spans));
    assert_eq!(sides, 0, "no sidecar before the pass");

    // Every file the roll sealed is owed one; the pass builds them all.
    assert_eq!(q.hashside_pending(), sealed);
    assert_eq!(build_sidecars(&q), sealed);
    assert_eq!(sidecars_in(q.dir()).len(), sealed);
    let (got, read, sides) = refill_counted(&q, spans);
    assert_eq!(got, plain, "the sidecars answer what the files answer");
    assert_eq!(sides, sealed, "every sealed file answered by its sidecar");
    assert!(
        read < plain_read,
        "read {read} with sidecars, {plain_read} without"
    );

    // Reopened (indexes mapped, not yet checked): they still answer.
    drop(q);
    let (q, rep) = QLog::open(td.path(), 7, opts).unwrap();
    assert_eq!(rep.rebuilt_indexes, 0);
    assert_eq!(sidecars_in(q.dir()).len(), sealed, "kept across a reopen");
    let (got, _, sides) = refill_counted(&q, spans);
    assert_eq!(got, plain);
    assert_eq!(sides, sealed);
}

#[test]
fn large_records_are_refilled_from_their_sidecar_not_read_through() {
    // The shape of 2026-10-20's slow restart: records of 120 KiB payloads,
    // close enough that the refill reads through every payload.
    let td = TmpDir::new("big");
    let opts = QLogOptions::testing(4 << 20);
    let (mut q, _) = QLog::open(td.path(), 3, opts).unwrap();
    for k in 0..120u64 {
        append(&mut q, k + 1, 3, 10 * k, 10, 120 << 10, false);
    }
    assert!(q.files().iter().filter(|m| m.sealed).count() >= 2);
    let spans = [(3, 0, 1200)];
    let (plain, plain_read, _) = refill_counted(&q, &spans);
    build_sidecars(&q);
    let (got, read, sides) = refill_counted(&q, &spans);
    assert_eq!(got, plain);
    assert!(sides >= 2);
    // Only the active file is still read; each sealed one costs its sidecar
    // (36 bytes and the list a record), not 4 MiB of payloads.
    let active = q.files().last().unwrap().bytes;
    assert!(
        read <= active + (64 << 10) && plain_read > read + (10 << 20),
        "read {read} (active file {active}) against {plain_read}"
    );
}

#[test]
fn a_sidecar_that_does_not_fit_is_dropped_and_the_file_read() {
    let td = TmpDir::new("damaged");
    let opts = QLogOptions::testing(8 << 10);
    let (mut q, _) = QLog::open(td.path(), 4, opts).unwrap();
    for k in 0..200u64 {
        append(&mut q, k + 1, 2, 3 * k, 3, 200, false);
    }
    build_sidecars(&q);
    let spans = [(2, 0, 600)];
    let (want, _, sides) = refill_counted(&q, &spans);
    let ids = sidecars_in(q.dir());
    assert_eq!(sides, ids.len());
    // One flipped byte, one sidecar of another length (a rewrite's leftover).
    let flip = super::hashside::path(q.dir(), ids[1]);
    let mut b = std::fs::read(&flip).unwrap();
    b[40] ^= 0x10;
    std::fs::write(&flip, &b).unwrap();
    let stale = super::hashside::path(q.dir(), ids[2]);
    let other = super::hashside::encode(ids[2], 12345, &[]);
    std::fs::write(&stale, other).unwrap();
    let (got, _, sides) = refill_counted(&q, &spans);
    assert_eq!(got, want, "the files answer for the two that do not fit");
    assert_eq!(sides, ids.len() - 2);
    assert!(!flip.exists() && !stale.exists(), "both dropped");
    assert_eq!(q.hashside_pending(), 2, "both owed again");
    assert_eq!(build_sidecars(&q), 2);
    let (got, _, sides) = refill_counted(&q, &spans);
    assert_eq!((got, sides), (want, ids.len()));
}

#[test]
fn reclaim_a_cut_and_a_reopen_leave_no_sidecar_beside_a_file_it_does_not_name() {
    let td = TmpDir::new("gone");
    let opts = QLogOptions::testing(8 << 10);
    let (mut q, _) = QLog::open(td.path(), 5, opts).unwrap();
    for k in 0..300u64 {
        append(&mut q, k + 1, 1, 2 * k, 2, 150, false);
    }
    build_sidecars(&q);
    let dir = q.dir().to_path_buf();
    let sealed: Vec<u64> = q
        .files()
        .iter()
        .filter(|m| m.sealed)
        .map(|m| m.id)
        .collect();
    let active = q.files().last().unwrap().id;
    assert_eq!(sidecars_in(&dir), sealed);

    // A reopen drops one naming no file and one beside the active file.
    std::fs::write(super::hashside::path(&dir, 99_999), b"stray").unwrap();
    std::fs::write(super::hashside::path(&dir, active), b"stray").unwrap();
    drop(q);
    let (q, _) = QLog::open(td.path(), 5, opts).unwrap();
    assert_eq!(sidecars_in(&dir), sealed, "the stray two swept");

    // A cut above a seq: the files above go with their sidecars, the file
    // cut loses its own (it is active again).
    let cut_at = q.files()[sealed.len() / 2].first_seq + 1;
    drop(q);
    super::cut_log_above(&dir, cut_at).unwrap();
    let left: Vec<u64> = sealed
        .iter()
        .copied()
        .filter(|id| *id < sealed[sealed.len() / 2])
        .collect();
    assert_eq!(sidecars_in(&dir), left);

    // Retention: a dead file's sidecar goes with it.
    let (mut q, _) = QLog::open(td.path(), 5, opts).unwrap();
    q.check_all_indexes().unwrap();
    let dead = q.unlink_dead_files(|_| true).unwrap();
    assert!(dead > 0);
    let still: Vec<u64> = q
        .files()
        .iter()
        .filter(|m| m.sealed)
        .map(|m| m.id)
        .collect();
    for id in sidecars_in(&dir) {
        assert!(still.contains(&id), "sidecar {id} outlived its file");
    }
}
