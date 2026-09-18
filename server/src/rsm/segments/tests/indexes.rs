//! The `.qidx`: what it promises and what it refuses (§6.1 amendment).

use std::fs;

use super::super::index::*;
use super::TmpDir;

fn rec(pid: u64, base: u64, count: u32, offset: u64) -> Record {
    Record {
        pid,
        base_offset: base,
        end: base + count as u64,
        created_at_us: 1_700_000_000_000_000 + base as i64,
        offset,
        count,
        len: 256,
    }
}

/// Write an index into a temp dir and map it back.
fn round_trip(dir: &TmpDir, bucket: u16, file_id: u32, mut recs: Vec<Record>) -> View {
    // The order the caller collected them in is not the order the file is in:
    // `sort_records` is the step every writer of a `.qidx` takes first.
    sort_records(&mut recs);
    let bytes = encode(bucket, file_id, 4096, &recs);
    let path = dir.path().join("x.qidx");
    fs::write(&path, &bytes).expect("write");
    View::open(&path, None).expect("open")
}

#[test]
fn records_come_back_sorted_whatever_order_they_were_collected_in() {
    let d = TmpDir::new("idx-sort");
    // Written in the order the apply thread produced them: two partitions
    // interleaved in one bucket, which is the normal shape.
    let v = round_trip(
        &d,
        3,
        7,
        vec![
            rec(20, 0, 5, 0),
            rec(10, 0, 5, 300),
            rec(20, 5, 5, 600),
            rec(10, 5, 5, 900),
        ],
    );
    let got: Vec<(u64, u64)> = v.records().map(|r| (r.pid, r.base_offset)).collect();
    assert_eq!(got, vec![(10, 0), (10, 5), (20, 0), (20, 5)]);
    assert_eq!(v.len(), 4);
    assert_eq!(v.file_bytes(), 4096);
    v.check_identity(3, 7).expect("identity");
    assert!(v.check_identity(3, 8).is_err());
    assert!(v.check_identity(4, 7).is_err());
}

#[test]
fn a_probe_answers_hit_before_after_hole_or_missing() {
    let d = TmpDir::new("idx-probe");
    let v = round_trip(
        &d,
        0,
        0,
        vec![
            rec(10, 100, 5, 0),   // [100,105)
            rec(10, 105, 5, 300), // [105,110)
            rec(10, 200, 5, 600), // [200,205) — a hole at [110,200)
        ],
    );
    assert!(matches!(v.probe(10, 100), Probe::Hit(r) if r.offset == 0));
    assert!(matches!(v.probe(10, 104), Probe::Hit(r) if r.offset == 0));
    assert!(matches!(v.probe(10, 105), Probe::Hit(r) if r.offset == 300));
    assert!(matches!(v.probe(10, 204), Probe::Hit(r) if r.offset == 600));
    assert_eq!(
        v.probe(10, 99),
        Probe::Before,
        "below the file's first frame"
    );
    assert_eq!(v.probe(10, 205), Probe::After, "past the file's last frame");
    assert_eq!(
        v.probe(10, 150),
        Probe::Hole,
        "retention took what was there"
    );
    assert_eq!(v.probe(11, 100), Probe::Missing, "another partition");
}

#[test]
fn the_binary_search_is_exact_over_many_partitions_and_frames() {
    let d = TmpDir::new("idx-many");
    let mut recs = Vec::new();
    let mut off = 0u64;
    for pid in 0..40u64 {
        for i in 0..50u64 {
            recs.push(rec(pid * 3, i * 7, 7, off));
            off += 128;
        }
    }
    let v = round_trip(&d, 1, 1, recs);
    assert_eq!(v.len(), 2000);
    // Every offset of every partition resolves to the frame that holds it,
    // and every offset of a pid that does not exist is Missing.
    for pid in 0..40u64 {
        for i in 0..50u64 {
            for k in 0..7u64 {
                match v.probe(pid * 3, i * 7 + k) {
                    Probe::Hit(r) => {
                        assert_eq!(r.pid, pid * 3);
                        assert_eq!(r.base_offset, i * 7);
                    }
                    other => panic!("pid {} offset {} gave {other:?}", pid * 3, i * 7 + k),
                }
            }
        }
        assert_eq!(v.probe(pid * 3 + 1, 0), Probe::Missing);
        assert_eq!(v.probe(pid * 3, 350), Probe::After);
    }
}

#[test]
fn an_empty_index_answers_missing_and_nothing_else() {
    let d = TmpDir::new("idx-empty");
    let v = round_trip(&d, 0, 0, vec![]);
    assert!(v.is_empty());
    assert_eq!(v.probe(1, 0), Probe::Missing);
}

#[test]
fn a_damaged_index_is_refused_rather_than_read() {
    let d = TmpDir::new("idx-bad");
    let path = d.path().join("y.qidx");
    let recs = vec![rec(1, 0, 4, 0), rec(1, 4, 4, 300)];
    let good = encode(2, 5, 1024, &recs);

    // A flip in the header.
    let mut b = good.clone();
    b[10] ^= 0xff;
    fs::write(&path, &b).expect("write");
    assert!(
        View::open(&path, None).is_err(),
        "a damaged header must be refused"
    );

    // A flip in the records.
    let mut b = good.clone();
    b[HEADER_LEN + 3] ^= 0xff;
    fs::write(&path, &b).expect("write");
    assert!(
        View::open(&path, None).is_err(),
        "a damaged record array must be refused"
    );

    // A wrong magic.
    let mut b = good.clone();
    b[0] = b'Z';
    fs::write(&path, &b).expect("write");
    assert!(View::open(&path, None).is_err());

    // Cut short: the header promises records that are not there.
    fs::write(&path, &good[..HEADER_LEN + RECORD_LEN]).expect("write");
    assert!(View::open(&path, None).is_err());

    // Whole and sound.
    fs::write(&path, &good).expect("write");
    let v = View::open(&path, None).expect("the untouched index opens");
    assert_eq!(v.len(), 2);
}

#[test]
fn an_index_written_for_another_length_is_stale() {
    let d = TmpDir::new("idx-stale");
    let path = d.path().join("z.qidx");
    let recs = vec![rec(1, 0, 4, 0)];
    fs::write(&path, encode(0, 0, 4096, &recs)).expect("write");
    assert!(View::open(&path, Some(4096)).is_ok());
    let e = View::open(&path, Some(8192)).expect_err("a different length is stale");
    assert!(e.to_string().contains("4096"), "{e}");
}

#[test]
fn the_active_index_answers_the_file_and_the_record_together() {
    let mut a = ActiveIndexes::new(4);
    assert_eq!(a.probe(0, 1, 0), None, "no active file yet");
    a.open(0, 12);
    a.insert(0, rec(1, 0, 5, 0));
    a.insert(0, rec(1, 5, 5, 300));
    a.insert(0, rec(2, 0, 5, 600));
    assert_eq!(a.len(), 3);
    assert_eq!(a.bucket_len(0), 3);
    assert_eq!(a.bucket_len(1), 0);
    assert_eq!(a.file_id(0), Some(12));
    let (file, r) = a.probe(0, 1, 7).expect("hit");
    assert_eq!((file, r.offset), (12, 300));
    assert_eq!(a.probe(0, 1, 10), None, "past the last frame");
    assert_eq!(a.probe(0, 3, 0), None, "another partition");

    // A roll takes the records and forgets the file, so nothing can be
    // answered against the file that is about to be created.
    let taken = a.take(0);
    assert_eq!(taken.len(), 3);
    assert_eq!(a.file_id(0), None);
    assert_eq!(a.probe(0, 1, 0), None);
    assert!(a.is_empty());
}
