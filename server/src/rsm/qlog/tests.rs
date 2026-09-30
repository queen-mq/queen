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
    buf[48] = 0x7F; // an unknown txn_kind (2 is REC_ENTRY since 8c9e0c4e)
    let sum = xxhash_rust::xxh3::xxh3_64(&buf[record::UNCHECKED_PREFIX..]);
    buf[4..12].copy_from_slice(&sum.to_le_bytes());
    assert!(matches!(
        record::decode(&buf),
        Err(record::RecordError::BadTxnKind(0x7F))
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
fn prealloc_zero_run_is_cut_at_roll_and_reopen() {
    // Small synced appends switch the zero-filled run on (past the
    // sustained-traffic gate's syncs); it must never leak into a sealed file,
    // a reopen, or a read.
    let td = TmpDir::new("prealloc");
    let opts = QLogOptions::testing_durable(64 * 1024, Fsync::Data);
    let qid = 5;
    let n = 600u64;
    let warm = super::PREALLOC_MIN_SYNCS + 6;
    {
        let (mut q, _) = QLog::open(td.path(), qid, opts).unwrap();
        fill(&mut q, 3, warm);
        let active = *q.files().last().unwrap();
        let phys = std::fs::metadata(qlog_file(td.path(), qid, active.id))
            .unwrap()
            .len();
        assert!(
            phys > active.bytes,
            "no zero run ahead ({phys} <= {})",
            active.bytes
        );
        for i in warm..n {
            append_one(&mut q, i, 3, i, 1_000 + i as i64);
        }
        assert!(q.file_count() >= 2, "fixture did not roll");
        for f in q.files().iter().filter(|f| f.sealed) {
            let len = std::fs::metadata(qlog_file(td.path(), qid, f.id))
                .unwrap()
                .len();
            assert_eq!(len, f.bytes, "sealed file {} keeps a zero tail", f.id);
        }
    }
    let (mut q, rep) = QLog::open(td.path(), qid, opts).unwrap();
    assert!(!rep.truncated_tail, "a zero run is not a torn tail");
    assert_eq!(rep.records, n);
    assert_eq!(rep.rebuilt_indexes, 0);
    let active = *q.files().last().unwrap();
    let phys = std::fs::metadata(qlog_file(td.path(), qid, active.id))
        .unwrap()
        .len();
    assert_eq!(phys, active.bytes, "reopen must cut the zero run");
    for i in n..n + 50 {
        append_one(&mut q, i, 3, i, 1_000 + i as i64);
    }
    for i in 0..n + 50 {
        assert_eq!(
            q.read_payload(3, i).unwrap(),
            Some(payload(i, 96)),
            "offset {i}"
        );
    }
}

#[test]
fn zstd_records_read_back_raw_and_survive_reopen() {
    // A codec-compressed record (WriteRecord::Zstd) is flagged on disk and
    // decompressed at the one decode point: every read sees the raw frames.
    use super::{codec, WriteRecord};
    let td = TmpDir::new("zstd");
    let opts = QLogOptions::testing(1 << 20);
    let raws: Vec<Vec<u8>> = (0..6u64)
        .map(|i| {
            let mut v = Vec::new();
            for j in 0..40 {
                v.extend_from_slice(
                    format!("{{\"n\":{i},\"j\":{j},\"kind\":\"order.created\"}}").as_bytes(),
                );
            }
            v
        })
        .collect();
    let hs: Vec<Vec<u8>> = (0..6u64).map(|i| hashes(i, 40)).collect();
    {
        let (mut q, _) = QLog::open(td.path(), 9, opts).unwrap();
        for i in 0..6u64 {
            let z = codec::compress_one(&raws[i as usize]);
            let stored = z.as_deref().unwrap_or(&raws[i as usize]);
            let r = RecordInput {
                seq: i + 1,
                pid: 4,
                base_offset: i * 40,
                count: 40,
                created_at_us: 1_000 + i as i64,
                txn: None,
                hashes: &hs[i as usize],
                payload: stored,
            };
            // Alternate compressed and raw records in one file.
            let w = if i % 2 == 0 && z.is_some() {
                WriteRecord::Zstd(r)
            } else {
                WriteRecord::Msg(RecordInput {
                    payload: &raws[i as usize],
                    ..r
                })
            };
            q.write_mixed(&[w]).unwrap();
        }
        q.sync().unwrap();
        for i in 0..6u64 {
            let got = q.read_owned(4, i * 40 + 3).unwrap().expect("record");
            assert_eq!(got.payload, raws[i as usize], "record {i}");
            assert_eq!(q.read_hashes(4, i * 40).unwrap().unwrap(), hs[i as usize]);
        }
    }
    let (q, rep) = QLog::open(td.path(), 9, opts).unwrap();
    assert!(!rep.truncated_tail);
    assert_eq!(rep.records, 6);
    for i in 0..6u64 {
        assert_eq!(
            q.read_payload(4, i * 40).unwrap().unwrap(),
            raws[i as usize]
        );
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
    std::fs::write(qlog_file(td.path(), 1, next), b"QNQ").unwrap(); // < 32 bytes

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

#[test]
fn reclaim_below_txns_compacts_mixed_sealed_files() {
    let td = TmpDir::new("unlink-watermarks");
    let opts = QLogOptions::testing(300);
    let (mut q, _) = QLog::open(td.path(), 1, opts).unwrap();
    for i in 0..20u64 {
        append_one(&mut q, i + 1, 10 + (i % 2), i / 2, 10_000 + i as i64);
    }
    let before = q.file_count();
    assert!(before >= 3);

    let mut starts = std::collections::HashMap::new();
    starts.insert(10, u64::MAX);
    starts.insert(11, 0);
    let bytes_before = q.bytes();
    let mut changed = 0;
    loop {
        let step = q.unlink_below_txns_bounded(&starts, 1).unwrap();
        assert!(step.examined <= 1, "the local-GC file budget was exceeded");
        changed += step.changed;
        if !step.more {
            break;
        }
    }
    assert!(changed > 0, "mixed files are compacted");
    assert_eq!(q.file_count(), before, "partial compaction keeps file ids");
    assert!(
        q.bytes() < bytes_before,
        "expired records release disk bytes"
    );
    let mut expired_gone = 0;
    for offset in 0..10 {
        if q.read_payload(10, offset).unwrap().is_none() {
            expired_gone += 1;
        }
        assert!(
            q.read_payload(11, offset).unwrap().is_some(),
            "live pid 11 offset {offset} survives"
        );
    }
    assert!(expired_gone > 0, "sealed expired records are gone");
    let cached = q.unlink_below_txns_bounded(&starts, 1).unwrap();
    assert_eq!(cached.examined, 0, "unchanged files were scanned again");

    drop(q);
    let (mut q, recovery) = QLog::open(td.path(), 1, opts).unwrap();
    assert!(!recovery.truncated_tail, "compacted files reopen cleanly");
    for offset in 0..10 {
        assert!(q.read_payload(11, offset).unwrap().is_some());
    }

    starts.insert(11, u64::MAX);
    let mut dropped = 0;
    loop {
        let step = q.unlink_below_txns_bounded(&starts, 1).unwrap();
        assert!(step.examined <= 1);
        dropped += step.changed;
        if !step.more {
            break;
        }
    }
    assert!(dropped > 0);
    assert_eq!(q.file_count(), 1, "the active file is never reclaimed");
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

#[test]
fn prealloc_waits_for_sustained_traffic_and_sizes_the_run_to_it() {
    use super::prealloc_run as run;
    const MIB: u64 = 1 << 20;
    // No gate (today): the whole chunk once bytes-per-sync is known and small.
    assert_eq!(run(MIB, 0, 1, 2048), Some(MIB));
    assert_eq!(run(MIB, 0, 1, 0), None, "no estimate yet");
    assert_eq!(run(MIB, 0, 1, 40 << 10), None, "big syncs need no run");
    assert_eq!(
        run(0, 64, 1000, 2048),
        None,
        "QUEEN_RAFT_QLOG_PREALLOC_KB=0"
    );
    // Gated: nothing before the 64th sync (a queue's create + first contact)…
    assert_eq!(run(MIB, 64, 63, 2048), None);
    // …then about 64 syncs' worth: 64 × 2 KiB.
    assert_eq!(run(MIB, 64, 64, 2048), Some(128 << 10));
    // Clamped to [64 KiB, chunk].
    assert_eq!(run(MIB, 64, 64, 100), Some(64 << 10));
    assert_eq!(run(MIB, 64, 1000, 30 << 10), Some(MIB));
    assert_eq!(
        run(32 << 10, 64, 1000, 100),
        Some(32 << 10),
        "a chunk under the floor wins"
    );
}

/// Jepsen P5 (`repro/qlog-bitflip-hole.sh`): one damaged byte in the middle of
/// the active file. With verified records at or below the store's durable
/// index after it, the open refuses and leaves the file as it was; with only
/// records above it after the damage (a crash's torn, un-fsync'd group) it
/// cuts the tail as before. Both the record's body and its length word.
#[test]
fn damage_inside_durable_records_refuses_the_open() {
    let td = TmpDir::new("bitflip");
    let opts = QLogOptions::testing_durable(64 << 20, Fsync::Data);
    let qid = 7;
    let locs = {
        let (mut q, _) = QLog::open(td.path(), qid, opts).unwrap();
        fill(&mut q, 3, 100)
    };
    let path = qlog_file(td.path(), qid, locs[50].file_id);
    let clean = std::fs::read(&path).unwrap();
    // A byte of record 50's body, then a byte of its length word.
    for at in [locs[50].offset + 60, locs[50].offset + 2] {
        let mut bytes = clean.clone();
        bytes[at as usize] ^= 0x5A;
        std::fs::write(&path, &bytes).unwrap();

        let err = QLog::open_guarded(td.path(), qid, opts, 99)
            .err()
            .expect("a damaged durable record must refuse the open");
        assert!(
            err.to_string()
                .contains("corruption inside acknowledged data"),
            "{err}"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            bytes,
            "the refusal cut nothing"
        );

        // Only the records after the damage are above the durable index: the
        // torn-tail rule, as before.
        let (_q, rep) = QLog::open_guarded(td.path(), qid, opts, 40).unwrap();
        assert!(rep.truncated_tail);
        assert_eq!(rep.records, 50);
        std::fs::write(&path, &clean).unwrap();
    }
    let (_q, rep) = QLog::open_guarded(td.path(), qid, opts, 99).unwrap();
    assert!(!rep.truncated_tail);
    assert_eq!(rep.records, 100);
}

// ---------------------------------------------------------------------------
// Shared logs, the rewrite threshold, sealing quiet files (2026-09-25)
// ---------------------------------------------------------------------------

/// Twenty one-message records, pid 10 and pid 11 alternating, over several
/// small files: half of every sealed file's bytes belong to each pid.
fn mixed_log(tag: &str) -> (TmpDir, QLog, QLogOptions) {
    let td = TmpDir::new(tag);
    let opts = QLogOptions::testing(300);
    let (mut q, _) = QLog::open(td.path(), 1, opts).unwrap();
    for i in 0..20u64 {
        append_one(&mut q, i + 1, 10 + (i % 2), i / 2, 10_000 + i as i64);
    }
    assert!(q.file_count() >= 3);
    (td, q, opts)
}

fn reclaim_all(q: &mut QLog, starts: &std::collections::HashMap<u64, u64>) -> usize {
    let mut changed = 0;
    loop {
        let step = q.unlink_below_txns_bounded(starts, 1).unwrap();
        changed += step.changed;
        if !step.more {
            return changed;
        }
    }
}

#[test]
fn compact_threshold_waits_for_enough_dead_bytes() {
    let (_td, mut q, _) = mixed_log("compact-threshold");
    let mut starts = std::collections::HashMap::new();
    starts.insert(10, u64::MAX); // pid 10 expired: half of each mixed file
    starts.insert(11, 0);
    // 90% required, ~50% dead: nothing is rewritten.
    q.set_compact_min_dead_pct(90);
    let bytes_before = q.bytes();
    assert_eq!(
        reclaim_all(&mut q, &starts),
        0,
        "below the threshold nothing is rewritten"
    );
    assert_eq!(q.bytes(), bytes_before);
    // A lower threshold rewrites them, once the watermarks move again.
    q.set_compact_min_dead_pct(40);
    starts.insert(12, 0); // a new generation: the files are examined again
    assert!(
        reclaim_all(&mut q, &starts) > 0,
        "above the threshold mixed files are rewritten"
    );
    assert!(q.bytes() < bytes_before);
    for offset in 0..10 {
        assert!(
            q.read_payload(11, offset).unwrap().is_some(),
            "live pid 11 survives"
        );
    }
}

#[test]
fn compact_threshold_zero_keeps_the_first_dead_message_rule() {
    let (_td, mut q, _) = mixed_log("compact-zero");
    q.set_compact_min_dead_pct(0);
    let mut starts = std::collections::HashMap::new();
    starts.insert(10, u64::MAX);
    starts.insert(11, 0);
    assert!(reclaim_all(&mut q, &starts) > 0);
}

#[test]
fn seal_active_lets_retention_drop_a_quiet_file() {
    let td = TmpDir::new("seal-quiet");
    let opts = QLogOptions::testing(1 << 20); // never rolls on size here
    let (mut q, _) = QLog::open(td.path(), 7, opts).unwrap();
    for i in 0..5u64 {
        append_one(&mut q, i + 1, 10, i, 1_000);
    }
    assert_eq!(q.file_count(), 1);
    let age = q
        .active_age_us(1_000 + 60_000_000)
        .expect("an active file with data");
    assert!(age >= 60_000_000, "age runs from the oldest message");
    // Without a seal, retention cannot touch the active file.
    let mut starts = std::collections::HashMap::new();
    starts.insert(10, u64::MAX);
    assert_eq!(
        reclaim_all(&mut q, &starts),
        0,
        "the active file is never a candidate"
    );
    assert!(q.seal_active(6).unwrap(), "a file with data rolls");
    assert_eq!(q.file_count(), 2, "sealed file + a new empty active file");
    assert!(
        q.active_age_us(i64::MAX).is_none(),
        "the new active file holds nothing"
    );
    assert!(
        !q.seal_active(7).unwrap(),
        "an empty active file does not roll"
    );
    starts.insert(11, 0); // new generation
    assert!(
        reclaim_all(&mut q, &starts) > 0,
        "the sealed quiet file is reclaimed"
    );
    assert!(q.is_empty_log(), "nothing left but an empty active file");
    // Reopen: the empty active file carries first_seq 6, the tail bound.
    drop(q);
    let (q, rec) = QLog::open(td.path(), 7, opts).unwrap();
    assert!(!rec.truncated_tail);
    assert!(q.is_empty_log());
    assert!(rec.max_seq >= 5);
}

#[test]
fn shared_logs_route_every_queue_to_one_of_k_logs() {
    use super::set::{shard_log_id, QLogSet, SYSTEM_QUEUE_ID};
    let td = TmpDir::new("shard-route");
    let set = QLogSet::new(td.path().join("qlog"), QLogOptions::testing(1 << 20));
    let qa = QLogSet::queue_id_of("t", "a");
    let qb = QLogSet::queue_id_of("t", "b");
    // Default: one log per queue (lane 0 = the queue id).
    assert_eq!(set.log_id_for(qa, 5), qa);
    assert_eq!(set.log_id_for_queue(qa), qa);
    set.set_shards(4);
    let reader = set.reader();
    for pid in [1u64, 2, 3, 99] {
        assert_eq!(
            set.log_id_for(qa, pid),
            shard_log_id(qa % 4),
            "one queue, one shared log"
        );
        assert_eq!(
            reader.log_id_for(qa, pid),
            set.log_id_for(qa, pid),
            "the reader routes alike"
        );
    }
    assert_eq!(set.log_id_for_queue(qb), shard_log_id(qb % 4));
    for q in [qa, qb] {
        let id = set.log_id_for(q, 1);
        assert!((2..6).contains(&id), "shared log ids are 2..2+K");
        assert_ne!(id, SYSTEM_QUEUE_ID);
    }
}

#[test]
fn shards_file_fixes_the_layout_of_a_directory() {
    use super::set::{QLogSet, SHARDS_FILE};
    // A new directory whose SHARDS file says 8 keeps 8.
    let td = TmpDir::new("shards-file");
    let root = td.path().join("qlog");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join(SHARDS_FILE), "8\n").unwrap();
    let mut set = QLogSet::new(root.clone(), QLogOptions::testing(1 << 20));
    set.reopen_all().unwrap();
    assert_eq!(set.shards(), 8);
    // An existing per-queue directory without the file keeps per-queue logs.
    let td2 = TmpDir::new("shards-legacy");
    let root2 = td2.path().join("qlog");
    std::fs::create_dir_all(root2.join("q12345")).unwrap();
    let mut set2 = QLogSet::new(root2.clone(), QLogOptions::testing(1 << 20));
    set2.reopen_all().unwrap();
    assert_eq!(set2.shards(), 0);
    assert!(
        !root2.join(SHARDS_FILE).exists(),
        "no file = one log per queue"
    );
}

/// Write one one-message record into `log` through the set (no fsync).
fn write_one(set: &mut super::set::QLogSet, log: u64, seq: u64, pid: u64, base: u64, created: i64) {
    let h = hashes(seq, 1);
    let p = payload(seq, 64);
    set.write_group_for_qid(
        log,
        &[RecordInput {
            seq,
            pid,
            base_offset: base,
            count: 1,
            created_at_us: created,
            txn: None,
            hashes: &h,
            payload: &p,
        }],
    )
    .unwrap();
}

#[test]
fn idle_pass_seals_quiet_logs_and_removes_empty_ones() {
    use super::set::QLogSet;
    let td = TmpDir::new("idle-pass");
    let root = td.path().join("qlog");
    let mut set = QLogSet::new(root.clone(), QLogOptions::testing(1 << 20));
    set.reopen_all().unwrap(); // per-queue layout
    set.set_recovery_floor(u64::MAX);
    let q = QLogSet::queue_id_of("t", "quiet");
    let log = set.log_id_for(q, 10);
    for i in 0..3u64 {
        write_one(&mut set, log, i + 1, 10, i, 1_000);
    }
    set.sync().unwrap();
    assert!(root.join(format!("q{log}")).is_dir());
    // The data is an hour old: the pass seals the active file.
    let now = 1_000 + 3_600_000_000;
    set.idle_pass(now, std::time::Duration::from_secs(600))
        .unwrap();
    // Retention: the queue's partition is gone (no watermark) -> dead.
    let reader = set.reader();
    let starts = std::collections::HashMap::new();
    let mut changed = 0;
    loop {
        let p = reader.reclaim_below_txns(log, &starts, 1).unwrap();
        changed += p.changed;
        if !p.more {
            break;
        }
    }
    assert!(changed > 0, "the sealed file was reclaimed");
    // Nothing left: the next pass closes the log and removes its directory.
    set.idle_pass(now, std::time::Duration::from_secs(600))
        .unwrap();
    assert!(
        !root.join(format!("q{log}")).exists(),
        "the empty queue log is removed"
    );
    assert!(!reader.log_ids().contains(&log));
    // A later write re-creates it.
    write_one(&mut set, log, 9, 10, 3, now);
    assert!(root.join(format!("q{log}")).is_dir());
}

#[test]
fn idle_pass_never_removes_shared_logs() {
    use super::set::QLogSet;
    let td = TmpDir::new("idle-shared");
    let root = td.path().join("qlog");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join(super::set::SHARDS_FILE), "2\n").unwrap();
    let mut set = QLogSet::new(root.clone(), QLogOptions::testing(1 << 20));
    set.reopen_all().unwrap();
    set.set_recovery_floor(u64::MAX);
    let q = QLogSet::queue_id_of("t", "x");
    let log = set.log_id_for(q, 1);
    write_one(&mut set, log, 1, 1, 0, 1_000);
    set.sync().unwrap();
    set.idle_pass(1_000 + 3_600_000_000, std::time::Duration::from_secs(1))
        .unwrap();
    let reader = set.reader();
    loop {
        let p = reader
            .reclaim_below_txns(log, &std::collections::HashMap::new(), 1)
            .unwrap();
        if !p.more {
            break;
        }
    }
    set.idle_pass(1_000 + 3_600_000_000, std::time::Duration::from_secs(1))
        .unwrap();
    assert!(root.join(format!("q{log}")).is_dir(), "a shared log stays");
    // remove() of a queue in a shared directory leaves the log alone.
    set.remove("t", "x");
    assert!(reader.log_ids().contains(&log));
}

#[test]
fn pooled_sync_covers_many_logs() {
    use super::set::QLogSet;
    let td = TmpDir::new("pooled-sync");
    let root = td.path().join("qlog");
    let mut set = QLogSet::new(root, QLogOptions::testing_durable(1 << 20, Fsync::Data));
    // Eight logs: one fsynced inline, seven through the pool (kept small, the
    // parallel test run shares one fd limit).
    for q in 0..8u64 {
        let qid = QLogSet::queue_id_of("t", &format!("q{q}"));
        let log = set.log_id_for(qid, 1);
        write_one(&mut set, log, q + 1, 1, 0, 1_000);
    }
    set.sync().unwrap();
    assert_eq!(set.durable_seq(), 8);
}

#[test]
fn reclaim_step_unlinks_every_dead_file_and_caps_rewrites() {
    // Many sealed files: pid 10 fills the first half, pid 11 the second, and
    // a few files mix both.
    let td = TmpDir::new("reclaim-step");
    let opts = QLogOptions::testing(300);
    let (mut q, _) = QLog::open(td.path(), 1, opts).unwrap();
    for i in 0..40u64 {
        let pid = if i < 20 { 10 } else { 11 };
        append_one(&mut q, i + 1, pid, i % 20, 10_000 + i as i64);
    }
    let before = q.file_count();
    assert!(before >= 6);
    // pid 10 expired, pid 11 live: the pid-10 files are wholly dead.
    let mut starts = std::collections::HashMap::new();
    starts.insert(10, u64::MAX);
    starts.insert(11, 0);
    // One step, no rewrites allowed: every wholly dead file goes at once.
    let step = q.reclaim_step(&starts, usize::MAX, false).unwrap();
    assert_eq!(step.compacted, 0, "no rewrite without the budget");
    assert!(step.changed >= 2, "several dead files unlinked in one step");
    assert!(q.file_count() < before);
    for offset in 0..20 {
        assert!(
            q.read_payload(11, offset).unwrap().is_some(),
            "live pid 11 survives"
        );
    }
}

// ---------------------------------------------------------------------------
// Per-file retention (2026-09-28): a file is judged by the partitions in it
// ---------------------------------------------------------------------------

/// A per-queue set of small files: `n` one-message records into one log, the
/// pids cycling through `pids`, each pid's offsets counting up from 0.
fn per_file_set(
    tag: &str,
    file_bytes: u64,
    pids: &[u64],
    n: u64,
) -> (TmpDir, super::set::QLogSet, u64) {
    use super::set::QLogSet;
    let td = TmpDir::new(tag);
    let mut set = QLogSet::new(td.path().join("qlog"), QLogOptions::testing(file_bytes));
    set.reopen_all().unwrap();
    set.set_recovery_floor(u64::MAX);
    let log = set.log_id_for(QLogSet::queue_id_of("t", "per-file"), pids[0]);
    let mut next = std::collections::HashMap::new();
    for i in 0..n {
        let pid = pids[i as usize % pids.len()];
        let base = next.entry(pid).or_insert(0u64);
        write_one(&mut set, log, i + 1, pid, *base, 1_000 + i as i64);
        *base += 1;
    }
    set.sync().unwrap();
    (td, set, log)
}

fn unlimited_budget() -> super::set::ReclaimBudget {
    super::set::ReclaimBudget {
        files: usize::MAX,
        lookups: usize::MAX,
        deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
        rewrite: true,
    }
}

/// One pass over `log`, the watermarks from `starts` (a pid absent is gone);
/// every pid the pass asks about is appended to `asked`. `rewrite`: whether the
/// pass may rewrite a mixed file (and so sample live files to find one).
fn per_file_pass(
    reader: &super::set::QLogReader,
    log: u64,
    starts: &std::collections::HashMap<u64, u64>,
    asked: &mut Vec<u64>,
    rewrite: bool,
) -> super::ReclaimProgress {
    let mut live_start = |pid: u64| -> std::io::Result<Option<u64>> {
        asked.push(pid);
        Ok(starts.get(&pid).copied())
    };
    let mut budget = super::set::ReclaimBudget {
        rewrite,
        ..unlimited_budget()
    };
    reader
        .reclaim_log(log, &mut live_start, &mut budget)
        .unwrap()
}

fn sealed_files(set: &super::set::QLogSet, log: u64) -> usize {
    set.log(log).unwrap().read().unwrap().file_count() - 1
}

#[test]
fn per_file_retention_asks_only_for_the_partitions_in_its_files() {
    use std::collections::HashMap;
    let (_td, set, log) = per_file_set("per-file-ask", 300, &[1, 2, 3], 24);
    let reader = set.reader();
    let before = sealed_files(&set, log);
    assert!(before >= 4, "several sealed files, got {before}");

    // All live: each file stops at its first partition run, one lookup each.
    let live: HashMap<u64, u64> = [(1, 0), (2, 0), (3, 0)].into();
    let mut asked = Vec::new();
    let p = per_file_pass(&reader, log, &live, &mut asked, false);
    assert_eq!((p.changed, p.examined), (0, before));
    assert_eq!(asked.len(), before, "one lookup per live file");
    assert!(
        asked.iter().all(|pid| [1, 2, 3].contains(pid)),
        "only partitions with records in the files are looked up: {asked:?}"
    );
    // A pass that may rewrite also samples each live file for dead bytes: a
    // few more lookups per file (at most its distinct partitions here), still
    // only partitions that are in the files.
    let mut asked = Vec::new();
    let p = per_file_pass(&reader, log, &live, &mut asked, true);
    assert_eq!(p.changed, 0);
    assert!(
        asked.len() <= before * 4,
        "{} lookups for {before} files",
        asked.len()
    );
    assert!(asked.iter().all(|pid| [1, 2, 3].contains(pid)));

    // pid 1 expired: files holding only pid 1 go, the others move past it.
    let mut one_gone = live.clone();
    one_gone.insert(1, u64::MAX);
    let mut asked = Vec::new();
    let p = per_file_pass(&reader, log, &one_gone, &mut asked, false);
    assert!(asked.iter().filter(|pid| **pid == 1).count() <= before);
    let left = sealed_files(&set, log);
    assert_eq!(left, before - p.changed);

    // Nothing changed since: every file resumes at its first live record, so
    // the pass asks one partition per file and never pid 1 again.
    let mut asked = Vec::new();
    let p = per_file_pass(&reader, log, &one_gone, &mut asked, false);
    assert_eq!((p.changed, p.examined), (0, left));
    assert_eq!(asked.len(), left, "one lookup per file: {asked:?}");
    assert!(!asked.contains(&1), "a dead run is never looked up again");

    // Every partition gone: every sealed file goes, the active file stays.
    let mut asked = Vec::new();
    let mut changed = 0;
    loop {
        let p = per_file_pass(&reader, log, &HashMap::new(), &mut asked, true);
        changed += p.changed;
        if !p.more {
            break;
        }
    }
    assert_eq!(changed, left);
    assert_eq!(sealed_files(&set, log), 0);
}

#[test]
fn per_file_retention_rewrites_mixed_files_and_keeps_their_live_records() {
    use std::collections::HashMap;
    // Bigger files, so every sealed file mixes pid 10 and pid 11.
    let (_td, set, log) = per_file_set("per-file-rewrite", 1200, &[10, 11], 40);
    let reader = set.reader();
    assert!(sealed_files(&set, log) >= 2);
    let bytes_before = set.log(log).unwrap().read().unwrap().bytes();
    let starts: HashMap<u64, u64> = [(10, u64::MAX), (11, 0)].into();
    let mut rewrites = 0;
    let mut passes = 0;
    loop {
        let p = per_file_pass(&reader, log, &starts, &mut Vec::new(), true);
        assert!(p.compacted <= 1, "one rewrite per pass");
        rewrites += p.compacted;
        passes += 1;
        if p.changed == 0 && !p.more {
            break;
        }
        assert!(passes < 64, "retention never settled");
    }
    assert!(
        rewrites >= 2,
        "the mixed files were rewritten, got {rewrites}"
    );
    let q = set.log(log).unwrap();
    let q = q.read().unwrap();
    assert!(
        q.bytes() < bytes_before,
        "the expired records released bytes"
    );
    for offset in 0..20 {
        assert!(
            q.read_payload(11, offset).unwrap().is_some(),
            "live pid 11 offset {offset} survives"
        );
    }
    let active = q.files().last().unwrap().id;
    let sealed_pid10 = (0..20)
        .filter_map(|offset| q.locate(10, offset))
        .filter(|loc| loc.file_id != active)
        .count();
    assert_eq!(
        sealed_pid10, 0,
        "no expired pid 10 record is left in a sealed file"
    );
}

#[test]
fn per_file_retention_stops_at_its_lookup_budget_and_resumes() {
    let (_td, set, log) = per_file_set("per-file-budget", 300, &[1, 2, 3], 24);
    let reader = set.reader();
    let before = sealed_files(&set, log);
    let mut passes = 0;
    let mut changed = 0;
    loop {
        let mut budget = super::set::ReclaimBudget {
            lookups: 2,
            ..unlimited_budget()
        };
        let mut asked = 0;
        let mut gone = |_pid: u64| -> std::io::Result<Option<u64>> {
            asked += 1;
            Ok(None)
        };
        let p = reader.reclaim_log(log, &mut gone, &mut budget).unwrap();
        assert!(asked <= 2, "the lookup budget was exceeded: {asked}");
        changed += p.changed;
        passes += 1;
        if !p.more {
            break;
        }
        assert!(passes < 200, "retention never finished");
    }
    assert!(passes > 1, "the budget split the work over passes");
    assert_eq!(changed, before);
    assert_eq!(sealed_files(&set, log), 0);
}

// ---------------------------------------------------------------------------
// Drop-behind: the page cache keeps the hot tail
// ---------------------------------------------------------------------------

/// Every byte of `[0, end)` of every file, as `(file id, from, to)` runs
/// merged per file.
fn merged(ranges: &[(u64, u64, u64)]) -> std::collections::BTreeMap<u64, Vec<(u64, u64)>> {
    let mut out: std::collections::BTreeMap<u64, Vec<(u64, u64)>> = Default::default();
    for (id, from, to) in ranges {
        let runs = out.entry(*id).or_default();
        match runs.last_mut() {
            Some(last) if last.1 == *from => last.1 = *to,
            _ => runs.push((*from, *to)),
        }
    }
    out
}

#[test]
fn drop_behind_hands_out_each_cold_byte_once_and_keeps_the_hot_tail() {
    let td = TmpDir::new("drop-behind");
    let (mut q, _) = QLog::open(td.path(), 9, QLogOptions::testing(1024)).unwrap();
    let hot = 1500u64;

    // A log shorter than the hot tail: nothing is cold.
    fill(&mut q, 1, 4);
    let total: u64 = q.files().iter().map(|f| f.bytes).sum();
    assert!(total <= hot, "{total}");
    assert!(q.take_cold_ranges(hot).is_empty());

    // Grow it across several files, taking the cold ranges as a syncer
    // would after each group.
    let mut taken: Vec<(u64, u64, u64)> = Vec::new();
    for i in 4..60u64 {
        append_one(&mut q, i, 1, i, 1_000 + i as i64);
        taken.extend(q.take_cold_ranges(hot));
        // Asked again with nothing new: nothing.
        assert!(q.take_cold_ranges(hot).is_empty());
    }
    assert!(q.file_count() >= 4, "{} files", q.file_count());

    // What was handed out is exactly the log minus its last `hot` bytes:
    // contiguous from the first byte of the first file, never overlapping.
    let files = q.files().to_vec();
    let mut left = hot;
    let mut boundary = (0u64, 0u64);
    for m in files.iter().rev() {
        if m.bytes <= left {
            left -= m.bytes;
            continue;
        }
        boundary = (m.id, m.bytes - left);
        break;
    }
    let runs = merged(&taken);
    for m in &files {
        let want: Vec<(u64, u64)> = if m.id < boundary.0 {
            vec![(0, m.bytes)]
        } else if m.id == boundary.0 {
            vec![(0, boundary.1)]
        } else {
            Vec::new()
        };
        assert_eq!(
            runs.get(&m.id).cloned().unwrap_or_default(),
            want,
            "file {} ({} bytes), boundary {boundary:?}",
            m.id,
            m.bytes
        );
    }

    // Dropping them (a no-op off Linux) never changes what reads return.
    for (id, from, to) in &taken {
        let fd = q.read_fd(*id).unwrap();
        super::fadvise_dontneed(&fd, *from, *to).unwrap();
    }
    for i in 0..60u64 {
        assert_eq!(q.read_payload(1, i).unwrap(), Some(payload(i, 96)));
    }
}

// ---------------------------------------------------------------------------
// Seq points: an entry read starts near its first seq
// ---------------------------------------------------------------------------

fn entry_bytes(seq: u64) -> Vec<u8> {
    payload(seq ^ 0xAB, 150 + (seq % 50) as usize)
}

/// Every window's entry records are exactly the entries in it, whole.
fn check_windows(q: &QLog, last: u64, windows: &[(u64, u64)]) {
    for &(a, b) in windows {
        let got = q.entry_records_between(a, b).unwrap();
        let want: Vec<u64> = (a..b.min(last + 1)).collect();
        assert_eq!(
            got.iter().map(|r| r.seq).collect::<Vec<_>>(),
            want,
            "window {a}..{b}"
        );
        for r in &got {
            assert_eq!(r.entry, entry_bytes(r.seq), "entry {}", r.seq);
            assert_eq!(r.term, 3);
        }
    }
}

#[test]
fn entry_reads_start_at_seq_points_and_agree_with_a_full_scan() {
    use super::{EntryInput, WriteRecord};
    let td = TmpDir::new("seq-points");
    let opts = QLogOptions::testing(512 * 1024);
    const LAST: u64 = 6000;
    {
        let (mut q, _) = QLog::open(td.path(), 5, opts).unwrap();
        for seq in 1..=LAST {
            let h = hashes(seq, 1);
            let p = payload(seq, 100);
            let e = entry_bytes(seq);
            q.write_mixed(&[
                WriteRecord::Msg(RecordInput {
                    seq,
                    pid: 1,
                    base_offset: seq,
                    count: 1,
                    created_at_us: 1,
                    txn: None,
                    hashes: &h,
                    payload: &p,
                }),
                WriteRecord::Entry(EntryInput {
                    seq,
                    now_us: seq as i64,
                    copies: 1,
                    term: 3,
                    entry: &e,
                }),
            ])
            .unwrap();
        }
        q.sync().unwrap();
        assert!(q.file_count() >= 3, "{} files", q.file_count());
        let points: usize = q
            .seq_points
            .lock()
            .unwrap()
            .files
            .values()
            .map(Vec::len)
            .sum();
        assert!(
            points > q.file_count(),
            "the writes noted points ({points})"
        );
    }
    let windows = [
        (1, 65),
        (700, 764),
        (2500, 2501),
        (4096, 4160),
        (5990, 6100),
        (1, LAST + 1),
    ];
    // Reopened: no point yet; the first reads fill them in.
    let (q, _) = QLog::open(td.path(), 5, opts).unwrap();
    assert!(q.seq_points.lock().unwrap().files.is_empty());
    check_windows(&q, LAST, &windows);
    assert!(
        !q.seq_points.lock().unwrap().files.is_empty(),
        "reads filled points"
    );
    check_windows(&q, LAST, &windows);

    // Points that no longer hold (a rewrite this log missed) are caught and
    // the files read from their start.
    {
        let mut p = q.seq_points.lock().unwrap();
        for v in p.files.values_mut() {
            for pt in v.iter_mut() {
                pt.1 += 7;
            }
        }
    }
    check_windows(&q, LAST, &windows);
    {
        let mut p = q.seq_points.lock().unwrap();
        for v in p.files.values_mut() {
            for pt in v.iter_mut() {
                pt.0 += 1;
            }
        }
    }
    check_windows(&q, LAST, &windows);
}

// ---------------------------------------------------------------------------
// The next file, created ahead of its roll
// ---------------------------------------------------------------------------

/// The `first_seq` a file's header records.
fn header_first_seq(root: &Path, qid: u64, id: u64) -> u64 {
    let b = std::fs::read(qlog_file(root, qid, id)).unwrap();
    u64::from_le_bytes(b[16..24].try_into().unwrap())
}

#[test]
fn a_roll_takes_the_file_created_ahead_and_writes_its_first_seq() {
    let td = TmpDir::new("precreate");
    let opts = QLogOptions::testing(4096);
    let (mut q, _) = QLog::open(td.path(), 11, opts).unwrap();
    // Past half of the first file: its successor is created in the background.
    let mut seq = 0u64;
    while q.files().last().map_or(0, |f| f.bytes) < 2100 {
        append_one(&mut q, seq, 1, seq, 1_000);
        seq += 1;
    }
    let active = q.active_file_id().unwrap();
    if let Some(j) = q.next_job.take() {
        j.join().unwrap();
    }
    assert!(
        qlog_file(td.path(), 11, active + 1).exists(),
        "created ahead"
    );
    assert_eq!(
        header_first_seq(td.path(), 11, active + 1),
        super::PRECREATED
    );
    // Fill it: the roll takes that file and records the real first seq.
    while q.active_file_id() == Some(active) {
        append_one(&mut q, seq, 1, seq, 1_000);
        seq += 1;
    }
    assert_eq!(q.active_file_id(), Some(active + 1));
    assert_eq!(
        header_first_seq(td.path(), 11, active + 1),
        seq - 1,
        "the roll wrote the seq of the record it was made for"
    );
    for i in 0..seq {
        assert_eq!(q.read_payload(1, i).unwrap(), Some(payload(i, 96)));
    }
    // Several rolls later, a reopen reads everything and keeps no file ahead.
    for _ in 0..200 {
        append_one(&mut q, seq, 1, seq, 1_000);
        seq += 1;
    }
    drop(q);
    let (q, rep) = QLog::open(td.path(), 11, opts).unwrap();
    assert!(!rep.truncated_tail);
    assert_eq!(rep.records, seq);
    assert!(q.files().iter().all(|f| f.first_seq != super::PRECREATED));
    for i in 0..seq {
        assert_eq!(q.read_payload(1, i).unwrap(), Some(payload(i, 96)));
    }
}

#[test]
fn open_drops_a_newest_file_that_was_never_taken() {
    for zeros in [false, true] {
        let td = TmpDir::new("precreated-left");
        let opts = QLogOptions::testing(1 << 20);
        let locs;
        {
            let (mut q, _) = QLog::open(td.path(), 12, opts).unwrap();
            locs = fill(&mut q, 3, 5);
        }
        // A crash left the file created ahead (or one whose header never
        // reached the disk) behind the active one.
        let next = locs.last().unwrap().file_id + 1;
        let mut h = [0u8; 32];
        if !zeros {
            h[0..8].copy_from_slice(b"QNQLOG1\0");
            h[8..16].copy_from_slice(&next.to_le_bytes());
            h[16..24].copy_from_slice(&super::PRECREATED.to_le_bytes());
        }
        std::fs::write(qlog_file(td.path(), 12, next), h).unwrap();
        let (mut q, rep) = QLog::open(td.path(), 12, opts).unwrap();
        assert!(!qlog_file(td.path(), 12, next).exists(), "dropped");
        assert_eq!(q.active_file_id(), Some(next - 1));
        assert_eq!(rep.records, 5);
        assert_eq!(rep.max_seq, 4, "the durable tail is the real one");
        // The log goes on where it was.
        append_one(&mut q, 5, 3, 5, 1_005);
        for i in 0..6u64 {
            assert_eq!(q.read_payload(3, i).unwrap(), Some(payload(i, 96)));
        }
    }
}
