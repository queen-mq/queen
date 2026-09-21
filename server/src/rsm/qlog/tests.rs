//! The tests of `rsm/qlog/` (Phase A, `ALICE_PGLESS_NEWARCH.md` §3/§5).
//!
//! - the record codec on its own: what it encodes, what it refuses, and that a
//!   damaged byte is always caught — with and without the txn envelope;
//! - the `.qidx`: its two checksums, the staleness tie, the binary search and
//!   the five answers a probe can give;
//! - the writer: append → locate → read, `scan_from` ordering, roll → seal →
//!   reopen, `.qidx` rebuild-by-scan, unlink-dead;
//! - the crash contract: N records across ≥2 files, a torn tail dropped, the
//!   fsync'd records all readable, the index matching — for several tail shapes.
//!
//! Every test writes into a temporary directory of its own and removes it, and
//! every fixture is a few kilobytes.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use super::{record, Fsync, Loc, QLog, QLogOptions, RecordInput, ScanEntry, TxnInput};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A directory that removes itself, named after its test.
struct TmpDir(PathBuf);

impl TmpDir {
    fn new(tag: &str) -> TmpDir {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "queen-rsm-qlog-{tag}-{}-{}",
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

/// `count * 16` deterministic hash bytes.
fn hashes(seed: u64, count: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(count as usize * record::HASH_LEN);
    for i in 0..count as u64 {
        out.extend_from_slice(&(seed ^ (i.wrapping_mul(0x9E37_79B9_7F4A_7C15))).to_le_bytes());
        out.extend_from_slice(&(seed.wrapping_add(i)).to_le_bytes());
    }
    out
}

/// `len` deterministic payload bytes.
fn payload(seed: u64, len: usize) -> Vec<u8> {
    (0..len).map(|i| (seed as u8) ^ (i as u8)).collect()
}

/// Append a single one-message record and return where it landed.
fn append_one(q: &mut QLog, seq: u64, pid: u64, base: u64, created: i64) -> Loc {
    let h = hashes(seq, 1);
    let p = payload(seq, 96);
    let locs = q
        .append_group(&[RecordInput {
            seq,
            pid,
            base_offset: base,
            count: 1,
            created_at_us: created,
            txn: None,
            hashes: &h,
            payload: &p,
        }])
        .expect("append");
    locs[0]
}

// ---------------------------------------------------------------------------
// Record codec
// ---------------------------------------------------------------------------

#[test]
fn codec_roundtrip_no_txn() {
    let h = hashes(7, 3);
    let p = payload(7, 200);
    let mut buf = Vec::new();
    let n = record::encode_into(&mut buf, 42, 9, 1000, 3, 1_700_000_000_000, None, &h, &p)
        .expect("encode");
    assert_eq!(n, buf.len());
    assert_eq!(n, record::encoded_len(3, 0, p.len()));

    let rr = record::decode(&buf).expect("decode");
    assert_eq!(rr.header.seq, 42);
    assert_eq!(rr.header.pid, 9);
    assert_eq!(rr.header.base_offset, 1000);
    assert_eq!(rr.header.count, 3);
    assert_eq!(rr.header.created_at_us, 1_700_000_000_000);
    assert_eq!(rr.header.end_offset(), 1003);
    assert_eq!(rr.header.txn_kind, record::TXN_NONE);
    assert!(rr.txn.is_none());
    assert_eq!(rr.hashes, &h[..]);
    assert_eq!(rr.payload, &p[..]);
}

#[test]
fn codec_roundtrip_with_txn() {
    let h = hashes(3, 2);
    let p = payload(3, 50);
    let parts: [u64; 3] = [11, 22, 33];
    let gtid: u128 = 0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210;
    let mut buf = Vec::new();
    let n = record::encode_into(&mut buf, 5, 4, 8, 2, -12345, Some((gtid, &parts)), &h, &p)
        .expect("encode");
    assert_eq!(n, buf.len());
    assert_eq!(n, record::encoded_len(2, parts.len(), p.len()));

    let rr = record::decode(&buf).expect("decode");
    assert_eq!(rr.header.txn_kind, record::TXN_CROSS_QUEUE);
    let txn = rr.txn.expect("txn present");
    assert_eq!(txn.gtid, gtid);
    assert_eq!(txn.n(), 3);
    assert_eq!(txn.participants(), vec![11, 22, 33]);
    assert_eq!(rr.header.created_at_us, -12345);
    assert_eq!(rr.hashes, &h[..]);
    assert_eq!(rr.payload, &p[..]);
}

#[test]
fn codec_roundtrip_zero_count_empty_payload() {
    let mut buf = Vec::new();
    let n = record::encode_into(&mut buf, 1, 1, 0, 0, 0, None, &[], &[]).expect("encode");
    assert_eq!(n, record::FIXED_PREFIX); // minimal record
    let rr = record::decode(&buf).expect("decode");
    assert_eq!(rr.header.count, 0);
    assert!(rr.hashes.is_empty());
    assert!(rr.payload.is_empty());
    assert_eq!(rr.header.end_offset(), 0);
}

#[test]
fn codec_rejects_hash_stride_on_write() {
    let mut buf = Vec::new();
    // count says 2 (32 hash bytes) but only 16 given.
    let err = record::encode_into(&mut buf, 1, 1, 0, 2, 0, None, &hashes(1, 1), &[]).unwrap_err();
    assert!(matches!(
        err,
        record::RecordError::HashStride {
            count: 2,
            hashes: 16
        }
    ));
    assert!(buf.is_empty(), "nothing is written on a rejected encode");
}

#[test]
fn codec_parse_header_bounds_are_anti_oom() {
    // A believable prefix, then tamper the len field. parse_header must refuse
    // an impossible or oversized length BEFORE anything is allocated.
    let h = hashes(1, 1);
    let p = payload(1, 32);
    let mut buf = Vec::new();
    record::encode_into(&mut buf, 1, 1, 0, 1, 0, None, &h, &p).expect("encode");

    // len below the fixed body.
    let mut low = buf.clone();
    low[0..4].copy_from_slice(&1u32.to_le_bytes());
    assert!(matches!(
        record::parse_header(&low),
        Err(record::RecordError::BadLength(1))
    ));

    // len above the cap.
    let mut high = buf.clone();
    high[0..4].copy_from_slice(&(record::MAX_RECORD_BODY + 1).to_le_bytes());
    assert!(matches!(
        record::parse_header(&high),
        Err(record::RecordError::BadLength(_))
    ));

    // A buffer shorter than the fixed prefix is Truncated, not a panic.
    assert!(matches!(
        record::parse_header(&buf[..10]),
        Err(record::RecordError::Truncated { .. })
    ));
}

#[test]
fn codec_checksum_catches_every_flip() {
    let h = hashes(9, 2);
    let p = payload(9, 64);
    let parts: [u64; 1] = [77];
    let mut base = Vec::new();
    record::encode_into(&mut base, 100, 3, 500, 2, 999, Some((5, &parts)), &h, &p).expect("encode");

    // Flip one byte at every checksum-covered position (everything after the
    // checksum field, offset 12): each must be caught as a checksum failure,
    // because the checksum covers the whole rest of the record.
    for at in record::UNCHECKED_PREFIX..base.len() {
        let mut bad = base.clone();
        bad[at] ^= 0x01;
        match record::decode(&bad) {
            Err(record::RecordError::Checksum { .. }) => {}
            other => panic!("flip at {at} not caught: {other:?}"),
        }
    }
}

#[test]
fn codec_rejects_unknown_txn_kind() {
    // A record with a valid checksum but a txn_kind this build does not know is
    // refused, never interpreted.
    let mut buf = Vec::new();
    record::encode_into(&mut buf, 1, 1, 0, 1, 0, None, &hashes(1, 1), &payload(1, 8))
        .expect("encode");
    buf[48] = 2; // an unknown txn_kind
    let sum = xxhash_rust::xxh3::xxh3_64(&buf[record::UNCHECKED_PREFIX..]);
    buf[4..12].copy_from_slice(&sum.to_le_bytes());
    assert!(matches!(
        record::decode(&buf),
        Err(record::RecordError::BadTxnKind(2))
    ));
}

#[test]
fn codec_decode_split_is_bounded_even_if_checksum_passed() {
    // Hand-build a record whose header claims a count whose hashes cannot fit,
    // with a CORRECT checksum, to prove the split refuses it (Stride) rather
    // than indexing out of the frame.
    let mut buf = vec![0u8; record::FIXED_PREFIX];
    let body_len = (record::FIXED_PREFIX - 4) as u32; // minimal legal body
    buf[0..4].copy_from_slice(&body_len.to_le_bytes());
    // seq/pid/base at their offsets do not matter; set a large count.
    buf[36..40].copy_from_slice(&1_000u32.to_le_bytes()); // count
    buf[48] = record::TXN_NONE;
    let sum = xxhash_rust::xxh3::xxh3_64(&buf[record::UNCHECKED_PREFIX..]);
    buf[4..12].copy_from_slice(&sum.to_le_bytes());
    match record::decode(&buf) {
        Err(record::RecordError::Stride { count: 1000, .. }) => {}
        other => panic!("expected Stride, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The .qidx index
// ---------------------------------------------------------------------------

fn rec(pid: u64, base: u64, count: u32, offset: u64) -> super::index::Record {
    super::index::Record {
        pid,
        base_offset: base,
        end: base + count as u64,
        created_at_us: 1_000 + offset as i64,
        offset,
        count,
        len: 128,
    }
}

#[test]
fn qidx_encode_open_probe() {
    use super::index::{self, Probe};
    let td = TmpDir::new("qidx");
    // pid 1: [0,2),[2,5) ; a hole [5,8) ; [8,9). pid 3: [0,1).
    let mut records = vec![
        rec(1, 0, 2, 100),
        rec(1, 2, 3, 300),
        rec(1, 8, 1, 900),
        rec(3, 0, 1, 40),
    ];
    index::sort_records(&mut records);
    let bytes = index::encode(77, 4096, &records);
    let path = td.path().join("x.qidx");
    std::fs::write(&path, &bytes).unwrap();

    let v = index::View::open(&path, Some(4096)).expect("open");
    assert_eq!(v.file_id(), 77);
    assert_eq!(v.len(), 4);
    v.check_identity(77).expect("identity");
    assert!(v.check_identity(78).is_err());

    assert!(matches!(v.probe(1, 0), Probe::Hit(_)));
    assert!(matches!(v.probe(1, 1), Probe::Hit(_)));
    assert!(matches!(v.probe(1, 4), Probe::Hit(_)));
    assert!(matches!(v.probe(1, 6), Probe::Hole)); // in [5,8) hole
    assert!(matches!(v.probe(1, 9), Probe::After)); // past the last
    assert!(matches!(v.probe(3, 0), Probe::Hit(_)));
    assert!(matches!(v.probe(3, 5), Probe::After));
    assert!(matches!(v.probe(9, 0), Probe::Missing)); // no such partition
                                                      // An offset below a partition's first record -> Before (older file).
    let below = index::encode(1, 4096, &[rec(1, 100, 1, 0)]);
    let p2 = td.path().join("b.qidx");
    std::fs::write(&p2, &below).unwrap();
    let v2 = index::View::open(&p2, Some(4096)).unwrap();
    assert!(matches!(v2.probe(1, 50), Probe::Before));
}

#[test]
fn qidx_staleness_and_checksums() {
    use super::index::{self, IndexError};
    let td = TmpDir::new("qidx-stale");
    let records = vec![rec(1, 0, 1, 100)];
    let bytes = index::encode(3, 4096, &records);
    let path = td.path().join("s.qidx");
    std::fs::write(&path, &bytes).unwrap();

    // A recorded length that disagrees with file_bytes is stale.
    let err = index::View::open(&path, Some(9999)).unwrap_err();
    assert!(err.to_string().contains("indexes"), "{err}");

    // Flip a header byte -> HeaderChecksum.
    let mut bad_head = bytes.clone();
    bad_head[10] ^= 0xFF;
    std::fs::write(&path, &bad_head).unwrap();
    let e = index::View::open(&path, None).unwrap_err().to_string();
    assert!(e.contains("header checksum"), "{e}");

    // Flip a record byte -> RecordChecksum.
    let mut bad_rec = bytes.clone();
    let at = index::HEADER_LEN + 4;
    bad_rec[at] ^= 0xFF;
    std::fs::write(&path, &bad_rec).unwrap();
    let e2 = index::View::open(&path, None).unwrap_err().to_string();
    assert!(e2.contains("record checksum"), "{e2}");

    let _ = IndexError::Magic; // keep the variant referenced
}

// ---------------------------------------------------------------------------
// The writer: append / locate / read / scan
// ---------------------------------------------------------------------------

#[test]
fn write_locate_read() {
    let td = TmpDir::new("wlr");
    let (mut q, rep) = QLog::open(td.path(), 1, QLogOptions::testing(1 << 20)).unwrap();
    assert_eq!(rep.files, 0);
    assert_eq!(q.active_file_id(), None);

    // A group with three records across two partitions.
    let h0 = hashes(1, 1);
    let p0 = payload(1, 100);
    let h1 = hashes(2, 2);
    let p1 = payload(2, 40);
    let h2 = hashes(3, 1);
    let p2 = payload(3, 10);
    let locs = q
        .append_group(&[
            RecordInput {
                seq: 1,
                pid: 10,
                base_offset: 0,
                count: 1,
                created_at_us: 1000,
                txn: None,
                hashes: &h0,
                payload: &p0,
            },
            RecordInput {
                seq: 2,
                pid: 20,
                base_offset: 0,
                count: 2,
                created_at_us: 1001,
                txn: None,
                hashes: &h1,
                payload: &p1,
            },
            RecordInput {
                seq: 3,
                pid: 10,
                base_offset: 1,
                count: 1,
                created_at_us: 1002,
                txn: None,
                hashes: &h2,
                payload: &p2,
            },
        ])
        .unwrap();
    assert_eq!(locs.len(), 3);
    assert_eq!(q.active_file_id(), Some(1));
    assert!(locs.iter().all(|l| l.file_id == 1));
    assert!(locs[0].offset < locs[1].offset && locs[1].offset < locs[2].offset);

    // read_record by position.
    let r0 = q.read_record(locs[0].file_id, locs[0].offset).unwrap();
    assert_eq!(r0.seq, 1);
    assert_eq!(r0.pid, 10);
    assert_eq!(r0.payload, p0);
    assert_eq!(r0.hashes, h0);

    // locate + read_payload / read_hashes.
    let l = q.locate(20, 1).expect("pid 20 offset 1 (within [0,2))");
    assert_eq!(
        (l.file_id, l.offset, l.count),
        (locs[1].file_id, locs[1].offset, 2)
    );
    assert_eq!(q.read_payload(20, 0).unwrap(), Some(p1.clone()));
    assert_eq!(q.read_hashes(20, 1).unwrap(), Some(h1.clone()));
    assert_eq!(q.read_payload(10, 1).unwrap(), Some(p2.clone()));

    // A partition/offset nobody wrote locates to nothing.
    assert_eq!(q.locate(10, 5), None);
    assert_eq!(q.read_payload(999, 0).unwrap(), None);
}

#[test]
fn writer_txn_record_roundtrips() {
    let td = TmpDir::new("wtxn");
    let (mut q, _) = QLog::open(td.path(), 2, QLogOptions::testing(1 << 20)).unwrap();
    let h = hashes(1, 1);
    let p = payload(1, 32);
    let parts = [7u64, 8, 9];
    let locs = q
        .append_group(&[RecordInput {
            seq: 1,
            pid: 1,
            base_offset: 0,
            count: 1,
            created_at_us: 5,
            txn: Some(TxnInput {
                gtid: 0xDEAD_BEEF,
                participants: &parts,
            }),
            hashes: &h,
            payload: &p,
        }])
        .unwrap();
    let r = q.read_record(locs[0].file_id, locs[0].offset).unwrap();
    let txn = r.txn.expect("txn round-trips through the writer");
    assert_eq!(txn.gtid, 0xDEAD_BEEF);
    assert_eq!(txn.participants, vec![7, 8, 9]);
    assert_eq!(r.payload, p);
}

#[test]
fn scan_from_ordering() {
    let td = TmpDir::new("scan");
    // Small files so the partition's records spread across several files.
    let (mut q, _) = QLog::open(td.path(), 1, QLogOptions::testing(300)).unwrap();
    // pid 5, offsets 0..8 (one message each); interleave a second partition.
    for i in 0..8u64 {
        append_one(&mut q, 100 + i, 5, i, 2000 + i as i64);
        append_one(&mut q, 200 + i, 6, i, 3000 + i as i64);
    }
    assert!(q.file_count() >= 2, "the fixture did not roll");

    let mut seen: Vec<u64> = Vec::new();
    q.scan_from(5, 0, |e: &ScanEntry| {
        assert_eq!(e.pid, 5);
        seen.push(e.base_offset);
        true
    })
    .unwrap();
    assert_eq!(seen, (0..8).collect::<Vec<_>>());

    // From a later offset skips the earlier records.
    let mut seen2: Vec<u64> = Vec::new();
    q.scan_from(5, 4, |e| {
        seen2.push(e.base_offset);
        true
    })
    .unwrap();
    assert_eq!(seen2, vec![4, 5, 6, 7]);

    // The callback can stop early.
    let mut n = 0;
    q.scan_from(5, 0, |_| {
        n += 1;
        n < 3
    })
    .unwrap();
    assert_eq!(n, 3);
}

// ---------------------------------------------------------------------------
// Roll, seal, reopen; .qidx rebuild
// ---------------------------------------------------------------------------

/// Append `n` one-message records for one partition and return their locs.
fn fill(q: &mut QLog, pid: u64, n: u64) -> Vec<Loc> {
    (0..n)
        .map(|i| append_one(q, i, pid, i, 1_000 + i as i64))
        .collect()
}

#[test]
fn roll_seal_reopen_reads_everything() {
    let td = TmpDir::new("roll");
    let opts = QLogOptions::testing(300);
    let locs;
    {
        let (mut q, _) = QLog::open(td.path(), 42, opts).unwrap();
        locs = fill(&mut q, 7, 12);
        assert!(
            q.file_count() >= 3,
            "expected several files, got {}",
            q.file_count()
        );
        // At least one sealed file has a .qidx.
        let sealed = q.files().iter().filter(|f| f.sealed).count();
        assert!(sealed >= 2);
    }

    let (q, rep) = QLog::open(td.path(), 42, opts).unwrap();
    assert!(!rep.truncated_tail);
    assert_eq!(
        rep.rebuilt_indexes, 0,
        "every .qidx was written at the seal"
    );
    assert_eq!(rep.records, 12);

    for (i, l) in locs.iter().enumerate() {
        let want = payload(i as u64, 96);
        assert_eq!(
            q.read_payload(7, i as u64).unwrap(),
            Some(want),
            "offset {i} did not read back after reopen"
        );
        // read_record by the original position agrees.
        let r = q.read_record(l.file_id, l.offset).unwrap();
        assert_eq!(r.base_offset, i as u64);
    }
}

#[test]
fn qidx_rebuild_by_scan_equals_the_written_one() {
    let td = TmpDir::new("rebuild");
    let opts = QLogOptions::testing(300);
    let sealed_id;
    let before: Vec<Option<Vec<u8>>>;
    {
        let (mut q, _) = QLog::open(td.path(), 1, opts).unwrap();
        fill(&mut q, 3, 12);
        sealed_id = q
            .files()
            .iter()
            .find(|f| f.sealed)
            .expect("a sealed file")
            .id;
        before = (0..12u64).map(|o| q.read_payload(3, o).unwrap()).collect();
    }

    // Delete a sealed .qidx: recovery must rebuild it by scanning the .qlog.
    let qidx = super::qidx_path(&super::queue_dir(td.path(), 1), sealed_id);
    assert!(qidx.exists());
    std::fs::remove_file(&qidx).unwrap();

    let (q, rep) = QLog::open(td.path(), 1, opts).unwrap();
    assert_eq!(rep.rebuilt_indexes, 1, "the deleted .qidx was rebuilt");
    assert!(qidx.exists(), "the rebuilt .qidx is on disk");

    // Every read is identical to before the .qidx was deleted.
    let after: Vec<Option<Vec<u8>>> = (0..12u64).map(|o| q.read_payload(3, o).unwrap()).collect();
    assert_eq!(before, after);

    // A stale .qidx (wrong file_bytes) is also rebuilt.
    drop(q);
    let good = std::fs::read(&qidx).unwrap();
    let mut stale = good.clone();
    // Corrupt the file_bytes field (offset 24..32 of the qidx header) and fix
    // the header checksum so it passes its OWN checks but disagrees with the
    // .qlog length.
    stale[24..32].copy_from_slice(&(u64::MAX - 1).to_le_bytes());
    let sum = xxhash_rust::xxh3::xxh3_64(&stale[..super::index::HEADER_LEN - 8]);
    let hc = super::index::HEADER_LEN - 8;
    stale[hc..hc + 8].copy_from_slice(&sum.to_le_bytes());
    std::fs::write(&qidx, &stale).unwrap();
    let (_q2, rep2) = QLog::open(td.path(), 1, opts).unwrap();
    assert_eq!(rep2.rebuilt_indexes, 1, "a stale .qidx is rebuilt too");
}

// ---------------------------------------------------------------------------
// The crash contract: torn-tail truncation
// ---------------------------------------------------------------------------

/// The active file's path for queue `qid`, file `id`.
fn qlog_file(root: &Path, qid: u64, id: u64) -> PathBuf {
    super::file_path(&super::queue_dir(root, qid), id)
}

/// Encode one standalone record the way the writer would.
fn encoded_record(seq: u64, pid: u64, base: u64, count: u32) -> Vec<u8> {
    let mut b = Vec::new();
    record::encode_into(
        &mut b,
        seq,
        pid,
        base,
        count,
        7,
        None,
        &hashes(seq, count),
        &payload(seq, 96),
    )
    .expect("encode");
    b
}

/// Append raw bytes to a file, as an un-fsync'd write the OS flushed would.
fn append_raw(path: &Path, bytes: &[u8]) {
    let mut f = OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open active");
    f.write_all(bytes).expect("write raw tail");
    f.sync_all().expect("sync raw tail"); // make the torn bytes really present
}

/// The spec's crash test: N records across ≥2 files, a torn tail dropped on
/// reopen, every fsync'd record still readable, the index matching. Runs the
/// same body for several shapes of torn tail.
fn crash_body(tag: &str, make_tail: impl Fn() -> Vec<u8>) {
    let td = TmpDir::new(tag);
    let opts = QLogOptions::testing(300);
    let locs;
    let active_id;
    {
        let (mut q, _) = QLog::open(td.path(), 1, opts).unwrap();
        locs = fill(&mut q, 9, 12);
        assert!(q.file_count() >= 2, "{tag}: fixture did not roll");
        active_id = q.active_file_id().expect("an active file");
    }
    let active_path = qlog_file(td.path(), 1, active_id);
    // The clean, fsync'd length of the active file: what recovery must return.
    let clean_len = std::fs::metadata(&active_path).unwrap().len();

    // The un-fsync'd tail of a group the crash cut off, made really present on
    // disk (as the OS's writeback of a kill -9 would leave it).
    append_raw(&active_path, &make_tail());
    let torn_len = std::fs::metadata(&active_path).unwrap().len();
    assert!(torn_len > clean_len, "{tag}: the torn tail is on disk");

    let (q, rep) = QLog::open(td.path(), 1, opts).unwrap();
    assert!(
        rep.truncated_tail,
        "{tag}: the torn tail should be reported"
    );

    // The torn tail is PHYSICALLY gone: the file is truncated back to exactly
    // its last good record boundary.
    let recovered_len = std::fs::metadata(&active_path).unwrap().len();
    assert_eq!(
        recovered_len, clean_len,
        "{tag}: torn tail not truncated ({recovered_len} != {clean_len})"
    );

    // Every fsync'd record is still readable and unchanged.
    for i in 0..locs.len() as u64 {
        assert_eq!(
            q.read_payload(9, i).unwrap(),
            Some(payload(i, 96)),
            "{tag}: offset {i} lost after the crash"
        );
    }
    // The index matches: a forward scan yields exactly the 12 offsets, no
    // phantom record from the truncated bytes.
    let mut seen = Vec::new();
    q.scan_from(9, 0, |e| {
        seen.push(e.base_offset);
        true
    })
    .unwrap();
    assert_eq!(
        seen,
        (0..12).collect::<Vec<_>>(),
        "{tag}: index does not match"
    );

    // The queue keeps working: a fresh append lands cleanly at the next offset
    // and reads back, and the scan then yields exactly 0..13.
    let mut q = q;
    let l = append_one(&mut q, 999, 9, 12, 5000);
    assert_eq!(
        q.read_record(l.file_id, l.offset).unwrap().base_offset,
        12,
        "{tag}"
    );
    assert_eq!(
        q.read_payload(9, 12).unwrap(),
        Some(payload(999, 96)),
        "{tag}"
    );
    let mut seen2 = Vec::new();
    q.scan_from(9, 0, |e| {
        seen2.push(e.base_offset);
        true
    })
    .unwrap();
    assert_eq!(
        seen2,
        (0..13).collect::<Vec<_>>(),
        "{tag}: phantom or missing record after recovery+append"
    );
}

#[test]
fn crash_short_partial_record_is_truncated() {
    // A record cut off partway through: the classic torn write.
    crash_body("crash-short", || {
        let mut r = encoded_record(500, 9, 12, 1);
        r.truncate(r.len() - 7); // drop the last 7 bytes
        r
    });
}

#[test]
fn crash_checksum_flipped_record_is_truncated() {
    // A whole record reached the platter but one byte is wrong: checksum fails.
    crash_body("crash-flip", || {
        let mut r = encoded_record(500, 9, 12, 1);
        let last = r.len() - 1;
        r[last] ^= 0xFF;
        r
    });
}

#[test]
fn crash_bad_length_prefix_is_truncated() {
    // Only a header reached disk, with a plausible length but no body.
    crash_body("crash-lenprefix", || {
        let r = encoded_record(500, 9, 12, 1);
        r[..record::FIXED_PREFIX].to_vec() // header only, body missing
    });
}

#[test]
fn crash_valid_record_after_a_torn_one_is_also_dropped() {
    // A torn record followed by a WHOLE valid one: recovery must stop at the
    // torn record and drop everything after it, never accept data that sits
    // beyond a gap. Both go.
    crash_body("crash-torn-then-valid", || {
        let mut torn = encoded_record(500, 9, 12, 1);
        torn.truncate(torn.len() - 5); // torn record
        torn.extend_from_slice(&encoded_record(501, 9, 13, 1)); // a valid one after it
        torn
    });
}

#[test]
fn crash_fully_present_valid_record_is_kept() {
    // A record that reached the platter WHOLE and valid, even though its group
    // was never acked, is kept: recovery accepts present-and-untorn bytes (the
    // propose simply never returned; a retry is swallowed by dedup later). It
    // is readable, and nothing is truncated.
    let td = TmpDir::new("crash-keep");
    let opts = QLogOptions::testing(300);
    let active_id;
    {
        let (mut q, _) = QLog::open(td.path(), 1, opts).unwrap();
        fill(&mut q, 9, 12);
        active_id = q.active_file_id().unwrap();
    }
    // Append a well-formed record for the next offset directly.
    append_raw(
        &qlog_file(td.path(), 1, active_id),
        &encoded_record(500, 9, 12, 1),
    );

    let (q, rep) = QLog::open(td.path(), 1, opts).unwrap();
    assert!(
        !rep.truncated_tail,
        "a valid tail record is not a torn tail"
    );
    assert_eq!(rep.records, 13, "the extra valid record is kept");
    assert_eq!(q.read_payload(9, 12).unwrap(), Some(payload(500, 96)));
}

#[test]
fn crash_header_short_newest_file_is_dropped() {
    // A roll interrupted after create but before the 32-byte header: the newest
    // file is shorter than a header and is dropped, falling back to the
    // previous file as active.
    let td = TmpDir::new("crash-hdr");
    let opts = QLogOptions::testing(300);
    let last_good;
    {
        let (mut q, _) = QLog::open(td.path(), 1, opts).unwrap();
        fill(&mut q, 9, 8);
        last_good = q.active_file_id().unwrap();
    }
    // Simulate the half-created next file.
    let next = last_good + 1;
    std::fs::write(&qlog_file(td.path(), 1, next), b"QNQ").unwrap(); // < 32 bytes

    let (mut q, rep) = QLog::open(td.path(), 1, opts).unwrap();
    assert!(rep.truncated_tail);
    assert!(
        !qlog_file(td.path(), 1, next).exists(),
        "the stub file was dropped"
    );
    // The queue keeps working from the previous active file.
    let l = append_one(&mut q, 100, 9, 8, 1);
    assert_eq!(q.read_record(l.file_id, l.offset).unwrap().base_offset, 8);
}

// ---------------------------------------------------------------------------
// Retention: unlink dead files
// ---------------------------------------------------------------------------

#[test]
fn unlink_dead_drops_whole_files_never_the_active() {
    let td = TmpDir::new("unlink");
    let opts = QLogOptions::testing(300);
    let (mut q, _) = QLog::open(td.path(), 1, opts).unwrap();
    fill(&mut q, 4, 16);
    let files_before = q.file_count();
    assert!(files_before >= 3);
    let active_id = q.active_file_id().unwrap();

    // Mark the two lowest-id sealed files dead.
    let mut sealed_ids: Vec<u64> = q
        .files()
        .iter()
        .filter(|f| f.sealed)
        .map(|f| f.id)
        .collect();
    sealed_ids.sort_unstable();
    let dead: Vec<u64> = sealed_ids.iter().copied().take(2).collect();

    let dropped = q
        .unlink_dead_files(|m| dead.contains(&m.id))
        .expect("unlink");
    assert_eq!(dropped, 2);
    assert_eq!(q.file_count(), files_before - 2);
    for id in &dead {
        assert!(
            !qlog_file(td.path(), 1, *id).exists(),
            "file {id} still on disk"
        );
        assert!(!super::qidx_path(&super::queue_dir(td.path(), 1), *id).exists());
    }

    // The offsets that lived in the dropped files now locate to nothing; the
    // survivors still read.
    let mut any_gone = false;
    let mut any_live = false;
    for o in 0..16u64 {
        match q.read_payload(4, o).unwrap() {
            None => any_gone = true,
            Some(p) => {
                assert_eq!(p, payload(o, 96));
                any_live = true;
            }
        }
    }
    assert!(any_gone, "some offsets were dropped");
    assert!(any_live, "some offsets survive");

    // The active file is never a candidate, even if the predicate says dead.
    let dropped2 = q.unlink_dead_files(|_| true).expect("unlink all");
    assert!(!q.files().is_empty(), "the active file survives unlink-all");
    assert_eq!(q.active_file_id(), Some(active_id));
    // Only sealed files were dropped by the unlink-all.
    assert!(dropped2 >= 1);
    assert_eq!(q.file_count(), 1, "only the active file remains");
}

#[test]
fn unlink_dead_by_created_at_window() {
    let td = TmpDir::new("unlink-time");
    let opts = QLogOptions::testing(300);
    let (mut q, _) = QLog::open(td.path(), 1, opts).unwrap();
    // created_at climbs with the offset.
    for i in 0..16u64 {
        append_one(&mut q, i, 4, i, 10_000 + i as i64);
    }
    // Drop every sealed file whose newest record is older than a cutoff.
    let cutoff = 10_006;
    let dropped = q
        .unlink_dead_files(|m| m.sealed && m.max_created_at_us < cutoff)
        .expect("unlink");
    assert!(dropped >= 1);
    // Nothing above the cutoff was dropped.
    assert!(q.files().iter().all(|f| f.max_created_at_us >= 10_000));
}

// ---------------------------------------------------------------------------
// Empty / degenerate
// ---------------------------------------------------------------------------

#[test]
fn empty_open_then_use() {
    let td = TmpDir::new("empty");
    let opts = QLogOptions::testing(1 << 20);
    let (mut q, rep) = QLog::open(td.path(), 77, opts).unwrap();
    assert_eq!(rep.files, 0);
    assert_eq!(q.bytes(), 0);
    assert_eq!(q.locate(1, 0), None);
    assert_eq!(q.read_payload(1, 0).unwrap(), None);
    let mut scanned = 0;
    q.scan_from(1, 0, |_| {
        scanned += 1;
        true
    })
    .unwrap();
    assert_eq!(scanned, 0);

    // First append creates the first file with the real first seq in its header.
    let l = append_one(&mut q, 123, 1, 0, 1);
    assert_eq!(l.file_id, 1);
    assert_eq!(q.files()[0].first_seq, 123);

    // Reopen an empty-but-created queue: no torn tail, one file, one record.
    drop(q);
    let (q, rep) = QLog::open(td.path(), 77, opts).unwrap();
    assert!(!rep.truncated_tail);
    assert_eq!(rep.files, 1);
    assert_eq!(rep.records, 1);
    assert_eq!(q.read_payload(1, 0).unwrap(), Some(payload(123, 96)));
}
