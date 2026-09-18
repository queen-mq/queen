//! Append, roll, seal, read — the write path of §11.2 and the lookup of §6.1.

use super::super::*;
use super::*;

#[test]
fn a_fresh_tree_has_two_hundred_and_fifty_six_buckets_each_with_an_empty_file() {
    let d = TmpDir::new("fresh");
    let s = fresh(&d, 4096);
    assert_eq!(s.files().len(), NBUCKETS, "one active file per bucket");
    for b in 0..NBUCKETS as u16 {
        assert_eq!(s.active_file(b), Some(0));
        assert_eq!(s.active_len(b), 0);
        assert!(seg_file(&d, b, 0).exists());
    }
    assert_eq!(s.active_index_len(), 0);
}

#[test]
fn frames_land_back_to_back_and_read_back_verified() {
    let d = TmpDir::new("append");
    let mut s = fresh(&d, 1 << 20);
    let a = push(&mut s, 3, 42, 0, 4, 100);
    let b = push(&mut s, 3, 42, 4, 4, 200);
    let c = push(&mut s, 3, 99, 0, 1, 50);
    assert_eq!(a.offset, 0);
    assert_eq!(b.offset, a.len as u64);
    assert_eq!(c.offset, (a.len + b.len) as u64);
    assert_eq!((a.bucket, a.file_id), (3, 0));

    let f = s.read(b).expect("read");
    assert_eq!((f.pid, f.base_offset, f.count), (42, 4, 4));
    assert_eq!(f.hashes, hashes(42 ^ 4, 4));
    assert_eq!(f.blob, blob(42 ^ 4, 200));
    assert_eq!(f.created_at_us, 1_700_000_000_000_000 + 4);

    let f = s.read(c).expect("read");
    assert_eq!((f.pid, f.base_offset), (99, 0));
    assert_eq!(s.active_len(3), (a.len + b.len + c.len) as u64);
    assert_eq!(s.active_index_len(), 3);
}

#[test]
fn a_flipped_byte_is_caught_on_the_way_out() {
    // pgless's `read_blob` verified nothing; every read here verifies (§11.2).
    let d = TmpDir::new("read-checksum");
    let mut s = fresh(&d, 1 << 20);
    let a = push(&mut s, 0, 1, 0, 2, 128);
    let b = push(&mut s, 0, 1, 2, 2, 128);
    s.durable_point().expect("durable point");
    assert!(s.read(a).is_ok() && s.read(b).is_ok());

    // Inside the first frame's blob: the second frame is untouched, so the
    // damage is reported for exactly the read that hits it. Reads go through
    // `pread` on the live file, so no reopen is needed to see the damage —
    // and a reopen would not even get this far, because recovery refuses a
    // frame below the recorded length that does not verify (I11).
    flip(
        &seg_file(&d, 0, 0),
        a.offset + frame::HEADER_LEN as u64 + 40,
    );
    let e = s.read(a).expect_err("a damaged frame must not be served");
    assert!(matches!(e, SegError::Damaged { .. }), "{e}");
    assert!(s.read(b).is_ok(), "the neighbouring frame is fine");
    assert!(
        s.reader().read(a).is_err(),
        "and the blocking-pool reader sees the same damage"
    );
}

#[test]
fn a_position_outside_the_file_is_refused_not_read() {
    let d = TmpDir::new("read-bounds");
    let mut s = fresh(&d, 1 << 20);
    let a = push(&mut s, 0, 1, 0, 1, 64);
    let past = Position {
        offset: a.offset + a.len as u64,
        ..a
    };
    assert!(matches!(s.read(past), Err(SegError::NoSuchPosition(_))));
    let tiny = Position { len: 4, ..a };
    assert!(matches!(s.read(tiny), Err(SegError::NoSuchPosition(_))));
    let nowhere = Position { file_id: 77, ..a };
    assert!(matches!(s.read(nowhere), Err(SegError::MissingFile { .. })));
}

#[test]
fn files_roll_at_the_limit_and_a_sealed_file_gets_its_index() {
    let d = TmpDir::new("roll");
    let mut s = fresh(&d, 4096);
    let mut wrote = Vec::new();
    for i in 0..24u64 {
        wrote.push(push(&mut s, 5, 7, i * 2, 2, 900));
    }
    let last = wrote.last().expect("frames");
    assert!(last.file_id >= 4, "24 frames of ~1 KiB must roll at 4 KiB");

    // No file ever passed the limit, and every sealed file has an index that
    // holds exactly the frames written into it.
    for id in 0..last.file_id {
        let meta = s.file_meta(5, id).expect("meta");
        assert!(meta.sealed, "f{id} should be sealed");
        assert!(meta.bytes <= 4096, "f{id} is {} bytes", meta.bytes);
        let v = index::View::open(&qidx_file(&d, 5, id), Some(meta.bytes)).expect("qidx");
        v.check_identity(5, id).expect("identity");
        let n = wrote.iter().filter(|p| p.file_id == id).count();
        assert_eq!(v.len(), n, "f{id} index holds {} of {n}", v.len());
    }
    assert!(!s.file_meta(5, last.file_id).expect("meta").sealed);
    assert!(
        !qidx_file(&d, 5, last.file_id).exists(),
        "the active file has no .qidx"
    );

    // Every frame still reads back, wherever it landed.
    for (i, p) in wrote.iter().enumerate() {
        let f = s.read(*p).expect("read");
        assert_eq!(f.base_offset, i as u64 * 2);
    }
}

#[test]
fn a_frame_larger_than_the_limit_goes_into_a_file_of_its_own() {
    let d = TmpDir::new("fat");
    let mut s = fresh(&d, 4096);
    let small = push(&mut s, 1, 1, 0, 1, 100);
    let fat = push(&mut s, 1, 1, 1, 1, 40_000);
    let after = push(&mut s, 1, 1, 2, 1, 100);
    assert_eq!(small.file_id, 0);
    assert_eq!(fat.file_id, 1, "the fat frame rolls first");
    assert_eq!(after.file_id, 2, "and the next frame rolls again");
    assert_eq!(s.read(fat).expect("read").blob.len(), 40_000);
}

#[test]
fn seal_all_seals_only_the_buckets_that_hold_something() {
    let d = TmpDir::new("seal-all");
    let mut s = fresh(&d, 1 << 20);
    push(&mut s, 1, 1, 0, 1, 64);
    push(&mut s, 200, 2, 0, 1, 64);
    assert_eq!(s.seal_all().expect("seal"), 2);
    assert_eq!(s.active_file(1), Some(1));
    assert_eq!(s.active_file(200), Some(1));
    assert_eq!(s.active_file(7), Some(0), "an empty bucket is left alone");
    assert!(s.file_meta(1, 0).expect("meta").sealed);
    assert_eq!(s.seal_all().expect("seal again"), 0);
}

#[test]
fn lookup_finds_a_frame_in_the_active_file_and_in_every_sealed_one() {
    let d = TmpDir::new("locate");
    let mut s = fresh(&d, 4096);
    let mut wrote = Vec::new();
    // Two partitions sharing a bucket, so the files interleave and the sort
    // the `.qidx` does is load-bearing.
    for i in 0..30u64 {
        wrote.push((11u64, i * 3, push(&mut s, 2, 11, i * 3, 3, 400)));
        wrote.push((22u64, i * 3, push(&mut s, 2, 22, i * 3, 3, 400)));
    }
    let files_11: Vec<u32> = sealed_files_of(&s, 2, 11);
    let files_22: Vec<u32> = sealed_files_of(&s, 2, 22);
    assert!(files_11.len() > 3, "the run must span several files");

    for (pid, base, pos) in &wrote {
        let list = if *pid == 11 { &files_11 } else { &files_22 };
        for k in 0..3u64 {
            let l = s
                .locate(2, *pid, base + k, list)
                .expect("locate")
                .unwrap_or_else(|| panic!("pid {pid} offset {} not found", base + k));
            assert_eq!(l.position, *pos, "pid {pid} offset {}", base + k);
            assert_eq!(l.record.pid, *pid);
            assert_eq!(l.record.base_offset, *base);
        }
    }

    // Offsets that do not exist: below the first, past the last, and a pid
    // that never wrote into this bucket.
    assert!(s.locate(2, 11, 90, &files_11).expect("locate").is_none());
    assert!(s.locate(2, 33, 0, &files_11).expect("locate").is_none());

    // And the same through the reader handle another thread would hold.
    let r = s.reader();
    let f = r
        .read_at(2, 11, 4, &files_11)
        .expect("read_at")
        .expect("found");
    assert_eq!((f.pid, f.base_offset), (11, 3));
    let (reads, bytes, _) = r.counters();
    assert!(reads >= 1 && bytes > 0);
}

/// The `partition_files` list of §6.1, which in the product comes from the
/// store. Here it is derived from what the tests know they wrote.
fn sealed_files_of(s: &Segments, bucket: u16, pid: Pid) -> Vec<u32> {
    let mut out = Vec::new();
    for (b, id, m) in s.files() {
        if b != bucket || !m.sealed {
            continue;
        }
        if let Ok(scan) = s.scan(b, id, m.bytes) {
            if scan.records.iter().any(|r| r.pid == pid) {
                out.push(id);
            }
        }
    }
    out.sort_unstable();
    out
}

#[test]
fn a_lookup_survives_a_partition_file_list_that_is_wrong() {
    // The binary search assumes the list holds only files with data of the
    // partition. If the caller passes a list that breaks that assumption, the
    // answer must still be right — the search falls back to a scan rather
    // than silently missing the frame.
    let d = TmpDir::new("locate-fallback");
    let mut s = fresh(&d, 4096);
    for i in 0..20u64 {
        push(&mut s, 0, 1, i * 2, 2, 900);
    }
    push(&mut s, 0, 2, 0, 1, 900);
    s.roll(0).expect("roll");
    let all: Vec<u32> = s
        .files()
        .into_iter()
        .filter(|(b, _, m)| *b == 0 && m.sealed)
        .map(|(_, id, _)| id)
        .collect();
    // `all` holds files with no frame of pid 2 at all.
    let l = s.locate(0, 2, 0, &all).expect("locate").expect("found");
    assert_eq!(l.record.pid, 2);
    // A file id in the list that no longer exists must not be an error.
    let mut with_ghost = all.clone();
    with_ghost.push(9999);
    with_ghost.sort_unstable();
    assert!(s.locate(0, 1, 0, &with_ghost).expect("locate").is_some());
}

#[test]
fn a_durable_point_reports_the_lengths_of_what_changed_and_nothing_else() {
    let d = TmpDir::new("durable");
    let mut s = fresh(&d, 4096);
    // The very first point carries the 256 files the open created.
    let p0 = s.durable_point().expect("durable point");
    assert_eq!(p0.files.len(), NBUCKETS);
    assert!(p0.files.iter().all(|f| f.len == 0 && !f.sealed));

    let a = push(&mut s, 9, 1, 0, 1, 500);
    let b = push(&mut s, 200, 2, 0, 1, 500);
    let p1 = s.durable_point().expect("durable point");
    let mut touched: Vec<(u16, u32, u64)> = p1
        .files
        .iter()
        .map(|f| (f.bucket, f.file_id, f.len))
        .collect();
    touched.sort_unstable();
    assert_eq!(
        touched,
        vec![(9, 0, a.len as u64), (200, 0, b.len as u64)],
        "a durable point costs what changed, not what exists (I8)"
    );
    assert_eq!(p1.files_synced, 2);

    // Nothing changed since: nothing to record.
    let p2 = s.durable_point().expect("durable point");
    assert!(p2.files.is_empty() && p2.files_synced == 0);

    // A roll makes the sealed file and its successor both reportable.
    for i in 0..10u64 {
        push(&mut s, 9, 1, 1 + i, 1, 900);
    }
    let p3 = s.durable_point().expect("durable point");
    let sealed: Vec<&FileState> = p3.files.iter().filter(|f| f.sealed).collect();
    assert!(
        !sealed.is_empty(),
        "the sealed file's final length is recorded"
    );
    assert!(p3.files.iter().any(|f| !f.sealed && f.bucket == 9));
    assert_eq!(s.unsynced_bytes(), 0);
}

#[test]
fn the_touched_set_drains_at_every_store_commit() {
    // I11: "every apply commit already records the lengths of the files it
    // touched". The set drains, so a commit's cost follows its own writes.
    let d = TmpDir::new("touched");
    let mut s = fresh(&d, 1 << 20);
    assert_eq!(s.take_touched().len(), NBUCKETS);
    assert!(s.take_touched().is_empty());
    push(&mut s, 4, 1, 0, 1, 64);
    push(&mut s, 4, 1, 1, 1, 64);
    let t = s.take_touched();
    assert_eq!(t.len(), 1, "two frames in one file are one recorded length");
    assert_eq!((t[0].bucket, t[0].file_id), (4, 0));
    assert!(s.take_touched().is_empty());
}

#[test]
fn an_out_of_range_bucket_is_refused() {
    let d = TmpDir::new("bucket-range");
    let mut s = fresh(&d, 1 << 20);
    let e = s
        .append(NBUCKETS as u16, 1, 0, 1, 0, &[], &[])
        .expect_err("bucket 256 does not exist");
    assert!(matches!(e, SegError::Refused(_)), "{e}");
}

#[test]
fn a_frame_in_a_just_sealed_file_is_found_before_the_caller_has_recorded_it() {
    // The window a roll opens: the frame has left the active file's RAM index
    // and the store row that would put its file in `partition_files` is in the
    // caller's NEXT commit, up to QUEEN_RAFT_STORE_COMMIT_MS later (§11.3). A
    // reader in between must still find it, or a pop payload read fails for a
    // message the cluster committed.
    let d = TmpDir::new("just-sealed");
    let mut s = fresh(&d, 4096);
    let a = push(&mut s, 2, 77, 0, 2, 900);
    let b = push(&mut s, 2, 77, 2, 2, 900);
    s.roll(2).expect("roll");
    assert_eq!(s.active_file(2), Some(1));
    assert_eq!(s.unrecorded_seals(), vec![(2, 0)]);

    // The caller's list is still empty: the seal is not recorded yet.
    let l = s
        .locate(2, 77, 1, &[])
        .expect("locate")
        .expect("a just-sealed frame is still findable");
    assert_eq!(l.position, a);
    assert_eq!(
        s.locate(2, 77, 3, &[])
            .expect("locate")
            .expect("hit")
            .position,
        b
    );
    assert!(s.locate(2, 77, 9, &[]).expect("locate").is_none());
    assert!(s.locate(2, 78, 0, &[]).expect("locate").is_none());

    // Once the caller says it has recorded the file, the list is the only
    // path — and it still answers.
    s.forget_sealed(2, 0);
    assert!(s.unrecorded_seals().is_empty());
    assert!(s.locate(2, 77, 1, &[]).expect("locate").is_none());
    assert_eq!(
        s.locate(2, 77, 1, &[0])
            .expect("locate")
            .expect("hit")
            .position,
        a
    );

    // A durable point does NOT retire the copy: it is not a store commit, and
    // the caller's commit for it has not happened when it returns. The SECOND
    // point does, because by then that commit has certainly landed.
    let c = push(&mut s, 2, 77, 4, 2, 900);
    s.roll(2).expect("roll");
    assert_eq!(s.unrecorded_seals(), vec![(2, 1)]);
    assert_eq!(
        s.locate(2, 77, 5, &[])
            .expect("locate")
            .expect("hit")
            .position,
        c
    );
    s.durable_point().expect("durable point");
    assert_eq!(
        s.unrecorded_seals(),
        vec![(2, 1)],
        "the caller's commit for this point has not happened yet"
    );
    assert_eq!(
        s.locate(2, 77, 5, &[])
            .expect("locate")
            .expect("still findable")
            .position,
        c
    );
    s.durable_point().expect("durable point");
    assert!(s.unrecorded_seals().is_empty());
    assert_eq!(
        s.locate(2, 77, 5, &[0, 1])
            .expect("locate")
            .expect("hit")
            .position,
        c
    );
}

#[test]
fn a_durable_point_does_not_stop_serving_a_seal_the_caller_has_not_committed() {
    // The window the first cut of this WP reopened. `durable_point` cleared
    // `sealed_recent` with the reasoning "this point is itself a store
    // commit". It is not: it fsyncs the files and hands the caller the lengths
    // to write, and the caller's DURABLE commit — an LMDB durable commit, up
    // to 1227 ms in S3 — happens after it returns. Between the two, a pop
    // payload read for a message in the just-sealed file found it in neither
    // the active RAM index, nor `sealed_recent`, nor the last-committed
    // `partition_files`: Ok(None) for a message the cluster had committed.
    let d = TmpDir::new("durable-seal-window");
    let mut s = fresh(&d, 4096);
    let a = push(&mut s, 6, 51, 0, 2, 900);
    s.roll(6).expect("roll");

    // The apply thread takes its durable point. The caller has NOT committed.
    let p = s.durable_point().expect("durable point");
    assert!(
        p.files
            .iter()
            .any(|f| f.bucket == 6 && f.file_id == 0 && f.sealed),
        "the point hands the sealed file's length to the caller to record"
    );
    let r = s.reader();
    assert_eq!(
        r.locate(6, 51, 1, &[])
            .expect("locate")
            .expect("the frame is still findable while the commit is in flight")
            .position,
        a
    );
    assert_eq!(r.read(a).expect("read").base_offset, 0);

    // Now the caller's commit lands and it says so. Only then does the copy go.
    s.forget_sealed(6, 0);
    assert!(s.unrecorded_seals().is_empty());
    assert!(r.locate(6, 51, 1, &[]).expect("locate").is_none());
    assert_eq!(
        r.locate(6, 51, 1, &[0])
            .expect("locate")
            .expect("and the recorded list answers")
            .position,
        a
    );
}

#[test]
fn a_position_longer_than_its_frame_is_refused_not_served() {
    // The checksum covers the frame and nothing else, so a position whose len
    // runs past the frame VERIFIES — and `read_blob` used to hand back the
    // payload plus the head of the next frame as if it were payload.
    let d = TmpDir::new("read-len");
    let mut s = fresh(&d, 1 << 20);
    let a = push(&mut s, 0, 1, 0, 2, 100);
    let b = push(&mut s, 0, 1, 2, 2, 100);
    let good = s.read_blob(a).expect("read_blob");
    assert_eq!(good, s.read(a).expect("read").blob);

    let long = Position {
        len: a.len + 16,
        ..a
    };
    let e = s
        .read(long)
        .expect_err("a fabricated length must not be served");
    assert!(
        matches!(e, SegError::LenMismatch { frame_len, .. } if frame_len == a.len),
        "{e}"
    );
    let e = s
        .read_blob(long)
        .expect_err("and not through read_blob either");
    assert!(matches!(e, SegError::LenMismatch { .. }), "{e}");
    let e = s
        .reader()
        .read_blob(long)
        .expect_err("nor through the blocking-pool reader");
    assert!(matches!(e, SegError::LenMismatch { .. }), "{e}");

    // Short by one is refused for the same reason: it is not that frame.
    let short = Position {
        len: a.len - 1,
        ..a
    };
    assert!(matches!(
        s.read(short),
        Err(SegError::Damaged { .. }) | Err(SegError::LenMismatch { .. })
    ));
    // The neighbour is untouched, which is what makes the answer about `len`
    // and not about the file.
    assert_eq!(s.read(b).expect("read").base_offset, 2);
}

#[test]
fn a_deadline_that_has_passed_refuses_before_any_io() {
    // I15 as this module can keep it: a budget that is gone costs no syscall.
    // It cannot abort a syscall already issued — that bound is the caller's
    // timeout on the blocking task.
    use std::time::{Duration, Instant};
    let d = TmpDir::new("deadline");
    let mut s = fresh(&d, 4096);
    let mut wrote = Vec::new();
    for i in 0..20u64 {
        wrote.push(push(&mut s, 0, 1, i, 1, 900));
    }
    s.roll(0).expect("roll");
    s.durable_point().expect("durable point");
    s.forget_sealed(0, 0);
    let ids: Vec<u32> = s
        .files()
        .into_iter()
        .filter(|(b, _, m)| *b == 0 && m.sealed)
        .map(|(_, id, _)| id)
        .collect();

    let r = s.reader();
    let past = Instant::now() - Duration::from_millis(1);
    let (reads_before, _, _) = r.counters();
    let e = r
        .read_within(wrote[0], Some(past))
        .expect_err("past deadline");
    assert!(matches!(e, SegError::DeadlineExceeded { .. }), "{e}");
    let e = r
        .read_blob_within(wrote[0], Some(past))
        .expect_err("past deadline");
    assert!(matches!(e, SegError::DeadlineExceeded { .. }), "{e}");
    let e = r
        .locate_within(0, 1, 3, &ids, Some(past))
        .expect_err("past deadline");
    assert!(matches!(e, SegError::DeadlineExceeded { .. }), "{e}");
    let e = r
        .read_at_within(0, 1, 3, &ids, Some(past))
        .expect_err("past deadline");
    assert!(matches!(e, SegError::DeadlineExceeded { .. }), "{e}");
    let (reads_after, _, _) = r.counters();
    assert_eq!(reads_after, reads_before, "no frame was read");

    // A budget that is there reads normally, and None is no budget at all.
    let soon = Instant::now() + Duration::from_secs(30);
    assert_eq!(
        r.read_at_within(0, 1, 3, &ids, Some(soon))
            .expect("read_at")
            .expect("found")
            .base_offset,
        3
    );
    assert!(r.read_within(wrote[0], None).is_ok());
}

#[test]
fn read_blob_returns_exactly_what_read_returns_without_the_hashes() {
    let d = TmpDir::new("read-blob");
    let mut s = fresh(&d, 1 << 20);
    for (n, len) in [(1u32, 0usize), (1, 64), (7, 1000), (0, 32)] {
        let p = s
            .append(0, 1, n as u64, n, 5, &hashes(9, n), &blob(9, len))
            .expect("append");
        let f = s.read(p).expect("read");
        assert_eq!(f.blob.len(), len);
        assert_eq!(f.hashes.len(), n as usize * 16);
        assert_eq!(s.read_blob(p).expect("read_blob"), f.blob);
        assert_eq!(s.reader().read_blob(p).expect("read_blob"), f.blob);
    }
}

#[test]
fn a_fat_frame_does_not_leave_the_encode_buffer_fat() {
    // I8: RAM bounded independently of what one outlier push happened to be.
    let d = TmpDir::new("buf");
    let mut s = fresh(&d, 1 << 30);
    let fat = push(&mut s, 0, 1, 0, 1, 8 * 1024 * 1024);
    assert_eq!(s.read(fat).expect("read").blob.len(), 8 * 1024 * 1024);
    let small = push(&mut s, 0, 1, 1, 1, 16);
    assert_eq!(s.read(small).expect("read").blob.len(), 16);
    assert_eq!(small.offset, fat.len as u64);
}

#[test]
fn a_roll_whose_create_fails_leaves_an_error_behind_it_not_a_panic() {
    // §11.8's case: the disk crosses ENOSPC (or EMFILE, or the deliberate
    // `create_new` collision) at the END of a roll, after the old file is
    // sealed. The bucket then has no active file, and the first cut of this WP
    // read `cur_len = 0` on the next append, skipped the roll guard and
    // unwrapped `None` — a panic on the apply thread, mid-entry, instead of an
    // error the caller can answer with a 507.
    let d = TmpDir::new("roll-create-fails");
    let mut s = fresh(&d, 4096);
    let a = push(&mut s, 0, 1, 0, 1, 900);
    assert_eq!(a.file_id, 0);

    // Stand in for ENOSPC with a name the roll cannot create.
    std::fs::write(seg_file(&d, 0, 1), b"in the way").expect("obstruct f1");

    let mut failed = None;
    for i in 1..10u64 {
        match s.append(0, 1, i, 1, 5, &hashes(1, 1), &blob(1, 900)) {
            Ok(_) => {}
            Err(e) => {
                failed = Some(e);
                break;
            }
        }
    }
    let e = failed.expect("the roll must fail on the obstructed name");
    assert!(
        matches!(e, SegError::Io(_)),
        "the create's own error is reported: {e}"
    );

    // The next append must NOT panic, and must not write into nowhere.
    for i in 20..25u64 {
        let e = s
            .append(0, 1, i, 1, 5, &hashes(1, 1), &blob(1, 900))
            .expect_err("still no active file");
        assert!(matches!(e, SegError::Io(_)), "{e}");
    }
    // Other buckets keep working: one bucket's full disk is not a dead writer.
    assert!(s.append(1, 2, 0, 1, 5, &hashes(2, 1), &blob(2, 64)).is_ok());
    // And what was written before the failure is still readable.
    assert_eq!(s.read(a).expect("read").base_offset, 0);

    // The operator frees the name; the bucket picks up where it left off.
    std::fs::remove_file(seg_file(&d, 0, 1)).expect("free the name");
    let after = s
        .append(0, 1, 30, 1, 5, &hashes(1, 1), &blob(1, 900))
        .expect("the append that follows the repair");
    assert_eq!(
        (after.file_id, after.offset),
        (1, 0),
        "it lands in the file the roll owed, at its start"
    );
    assert_eq!(s.read(after).expect("read").base_offset, 30);
    assert_eq!(s.active_file(0), Some(1));

    // The sealed file the failed roll left behind is intact and indexed.
    let m = s.file_meta(0, 0).expect("meta");
    assert!(m.sealed && m.frames > 0);
    assert!(qidx_file(&d, 0, 0).exists());
    let l = s.locate(0, 1, 0, &[0]).expect("locate").expect("found");
    assert_eq!(l.position, a);
}

#[test]
fn a_seal_never_takes_a_frame_out_of_reach_not_even_while_the_index_is_written() {
    // The window `Shared::sealed_recent` exists to close was REOPENED by the
    // roll itself. It took the bucket's records out of the active index under
    // the write lock and installed the readers' copy only after `write_qidx`
    // had sorted, allocated, written, renamed and reopened an index that is
    // ~0.5% of the segment file (a ~10 MiB write per 64 MiB file, by this
    // module's own `measure` test). For all of that a `locate` found the frame
    // in neither index, and — with the file not yet in the caller's
    // `partition_files` — answered Ok(None) for a message the cluster had
    // committed: a pop payload read failing for no reason, on the hot path.
    //
    // The two single-threaded tests above observe the state only AFTER `roll`
    // returns, so neither can see it. This one reads while the seal happens.
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    let d = TmpDir::new("roll-window");
    // One big file, so the roll happens where the test says it does, and
    // enough frames that the index write is not instantaneous.
    let mut s = fresh(&d, 1 << 30);
    const FRAMES: u64 = 20_000;
    for i in 0..FRAMES {
        push(&mut s, 3, 55, i, 1, 8);
    }
    let reader = s.reader();
    let stop = AtomicBool::new(false);
    let misses = AtomicU64::new(0);
    let rounds = AtomicU64::new(0);
    let during = std::thread::scope(|sc| {
        sc.spawn(|| {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                // The caller has not recorded any seal: the empty list is the
                // whole point.
                if reader
                    .locate(3, 55, i % FRAMES, &[])
                    .expect("locate")
                    .is_none()
                {
                    misses.fetch_add(1, Ordering::Relaxed);
                }
                rounds.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
        });
        // Wait for the reader to be going, but never forever: if it died on
        // its `expect`, the scope's join turns that into this test's failure.
        let give_up = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while rounds.load(Ordering::Relaxed) < 1_000 && std::time::Instant::now() < give_up {
            std::hint::spin_loop();
        }
        let before = rounds.load(Ordering::Relaxed);
        s.roll(3).expect("roll");
        let during = rounds.load(Ordering::Relaxed) - before;
        stop.store(true, Ordering::Relaxed);
        during
    });
    assert_eq!(
        misses.load(Ordering::Relaxed),
        0,
        "a committed frame answered \"not here\" {} times out of {} lookups",
        misses.load(Ordering::Relaxed),
        rounds.load(Ordering::Relaxed),
    );
    assert!(
        during > 0,
        "the reader has to be running DURING the seal for this to test anything",
    );
    // And the seal really happened, and every frame still reads back.
    assert_eq!(s.active_file(3), Some(1));
    assert_eq!(s.unrecorded_seals(), vec![(3, 0)]);
    for off in [0u64, FRAMES / 2, FRAMES - 1] {
        let l = s.locate(3, 55, off, &[]).expect("locate").expect("hit");
        assert_eq!(s.read(l.position).expect("read").base_offset, off);
    }
}

#[test]
fn a_roll_whose_index_write_fails_owes_the_index_and_keeps_serving() {
    // §11.8's case one line earlier than the create: the `.qidx` write is the
    // ~10 MiB write of a roll, so ENOSPC (or EMFILE, or EIO) lands there
    // first. The first cut took the records out of the active index, lost them
    // with the error, and left `active[b] = None` with `pending_next[b] =
    // None`: every later append to that bucket answered "the bucket has no
    // active file and none is owed" for the life of the process — even after
    // the operator freed the disk — and every lookup into the sealed file
    // answered Ok(None) until a restart rescanned it.
    let d = TmpDir::new("roll-qidx-fails");
    let mut s = fresh(&d, 4096);
    let a = push(&mut s, 0, 1, 0, 1, 900);
    let b = push(&mut s, 0, 1, 1, 1, 900);
    // The obstruction: the temp file the index write must create is a
    // directory, so the write fails the way a full disk would.
    let tmp = d.seg().join("b000").join("f0000000000.qidx.tmp");
    std::fs::create_dir(&tmp).expect("obstruct the index write");

    let e = s.roll(0).expect_err("the index write fails");
    assert!(matches!(e, SegError::Io(_)), "the write's own error: {e}");
    assert_eq!(
        s.owed_indexes(),
        vec![(0, 0)],
        "the index is owed, not lost"
    );
    assert!(!qidx_file(&d, 0, 0).exists());

    // The bucket is not wedged: it got its successor, and the next append
    // lands at the start of it.
    assert_eq!(s.active_file(0), Some(1));
    let c = s
        .append(0, 1, 2, 1, 5, &hashes(1, 1), &blob(1, 900))
        .expect("the append after a failed roll");
    assert_eq!((c.file_id, c.offset), (1, 0));
    assert_eq!(s.read(c).expect("read").base_offset, 2);

    // The sealed file's frames are still findable with nothing on disk and
    // nothing in the caller's list — RAM is the only copy, so nothing may
    // retire it: not the caller saying it committed, not two durable points.
    s.forget_sealed(0, 0);
    s.durable_point().expect("durable point");
    s.durable_point().expect("durable point");
    assert_eq!(s.owed_indexes(), vec![(0, 0)], "still obstructed");
    for (want, off) in [(a, 0u64), (b, 1)] {
        let l = s
            .locate(0, 1, off, &[])
            .expect("locate")
            .expect("a frame of the sealed file is still findable");
        assert_eq!(l.position, want);
        assert_eq!(s.read(want).expect("read").base_offset, off);
    }

    // The operator frees the disk. The next durable point pays the debt, the
    // index lands, and the file is served from disk like any other.
    std::fs::remove_dir(&tmp).expect("free the name");
    s.durable_point().expect("durable point");
    assert!(s.owed_indexes().is_empty(), "the debt is paid");
    assert!(qidx_file(&d, 0, 0).exists());
    assert!(s.unrecorded_seals().is_empty(), "and the RAM copy goes");
    assert!(s.locate(0, 1, 0, &[]).expect("locate").is_none());
    assert_eq!(
        s.locate(0, 1, 0, &[0])
            .expect("locate")
            .expect("the index on disk answers")
            .position,
        a
    );
    // The index that was owed is a real one: it holds both frames and reads
    // back through the mapped view.
    let v = index::View::open(
        &qidx_file(&d, 0, 0),
        Some(s.file_meta(0, 0).expect("meta").bytes),
    )
    .expect("the retried index opens");
    assert_eq!(v.len(), 2);
    v.check_identity(0, 0).expect("identity");
}

#[test]
fn the_directories_an_open_creates_are_made_durable() {
    // §11.4 step 1 owes "their directories". A durable point fsyncs
    // `seg/b0NN`, which makes the entries INSIDE it durable — never `seg/`
    // itself, so the NAME `b009` was not durable. A fresh node that created
    // its tree, appended, took a durable point and lost power before the file
    // system committed the directory creations came back without `seg/b009`:
    // the store names a file that is not reachable, recovery answers
    // MissingFile, `is_disagreement()` is true, and §11.5 discards a live
    // state directory over an ordinary crash.
    let d = TmpDir::new("dir-durability");
    let s = fresh(&d, 4096);
    assert_eq!(
        s.dir_syncs_at_open(),
        2,
        "the seg root, for the 256 names it just gained, and its parent, for the root's own",
    );
    let state = s.file_states();
    drop(s);

    let (s, _) = Segments::open(&d.seg(), Options::testing(4096), &state).expect("reopen");
    assert_eq!(
        s.dir_syncs_at_open(),
        0,
        "an open that creates nothing owes no barrier",
    );
    drop(s);

    // A bucket directory that is not there is created again — and the root's
    // entry for it is made durable again, before anything is written into it.
    for ent in std::fs::read_dir(d.seg().join("b009"))
        .expect("read b009")
        .flatten()
    {
        std::fs::remove_file(ent.path()).expect("empty b009");
    }
    std::fs::remove_dir(d.seg().join("b009")).expect("remove b009");
    let (s, _) = Segments::open(&d.seg(), Options::testing(4096), &[]).expect("reopen");
    assert_eq!(s.dir_syncs_at_open(), 1, "the root, not its parent");
    assert!(d.seg().join("b009").is_dir());
}
