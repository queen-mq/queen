//! Recovery against the store's recorded lengths (§11.5 step 3, I11).

use std::fs::OpenOptions;

use super::super::*;
use super::*;

/// The `files` keyspace as the store would hand it back: the whole row,
/// liveness included (§6.2), which is what a caller must persist.
fn recorded(s: &Segments) -> Vec<FileState> {
    s.file_states()
}

fn open(d: &TmpDir, rec: &[FileState]) -> Result<(Segments, Recovery)> {
    Segments::open(&d.seg(), Options::testing(4096), rec)
}

#[test]
fn a_tail_the_crash_left_behind_is_truncated_to_the_recorded_length() {
    let d = TmpDir::new("rec-tail");
    let mut s = fresh(&d, 4096);
    let a = push(&mut s, 1, 5, 0, 2, 500);
    let b = push(&mut s, 1, 5, 2, 2, 500);
    s.durable_point().expect("durable point");
    let state = recorded(&s);
    drop(s);

    // Everything the durable point recorded is readable; what a crash wrote
    // after it is not part of the state. Simulate the crash by appending
    // bytes the store never heard of.
    let path = seg_file(&d, 1, 0);
    let before = std::fs::metadata(&path).expect("meta").len();
    {
        let mut f = OpenOptions::new().append(true).open(&path).expect("open");
        use std::io::Write;
        f.write_all(&[0x5a; 777]).expect("garbage");
    }

    let (s, rep) = open(&d, &state).expect("recover");
    assert_eq!(rep.truncated, vec![(1, 0, before + 777, before)]);
    assert_eq!(
        std::fs::metadata(&path).expect("meta").len(),
        before,
        "the file is cut back to what the store recorded"
    );
    assert_eq!(s.read(a).expect("read").base_offset, 0);
    assert_eq!(s.read(b).expect("read").base_offset, 2);
    assert_eq!(s.active_len(1), before);
}

#[test]
fn a_torn_frame_below_the_recorded_length_is_the_i11_disagreement() {
    // Truncating a file mid-frame is what a lost tail looks like when the
    // store reopened PAST its last durable commit: the state says the frame
    // is there and the bytes are not. §11.5 must say so, not paper over it.
    let d = TmpDir::new("rec-torn");
    let mut s = fresh(&d, 1 << 20);
    push(&mut s, 2, 5, 0, 2, 500);
    let b = push(&mut s, 2, 5, 2, 2, 500);
    s.durable_point().expect("durable point");
    let state = recorded(&s);
    drop(s);

    let path = seg_file(&d, 2, 0);
    let full = std::fs::metadata(&path).expect("meta").len();
    let f = OpenOptions::new().write(true).open(&path).expect("open");
    f.set_len(full - 100).expect("cut mid-frame");
    drop(f);

    let e = open(&d, &state).expect_err("the store and the files disagree");
    assert!(e.is_disagreement(), "{e}");
    match e {
        SegError::ShortFile {
            bucket,
            file_id,
            on_disk,
            recorded,
        } => {
            assert_eq!((bucket, file_id), (2, 0));
            assert_eq!((on_disk, recorded), (full - 100, full));
        }
        other => panic!("{other}"),
    }
    let _ = b;
}

#[test]
fn a_frame_damaged_below_the_recorded_length_is_the_i11_disagreement_too() {
    // Same disagreement, different shape: the bytes are there and they are
    // not the bytes the state believes in. The scan that rebuilds the active
    // file's RAM index is what finds it.
    let d = TmpDir::new("rec-damaged");
    let mut s = fresh(&d, 1 << 20);
    let a = push(&mut s, 3, 5, 0, 2, 500);
    push(&mut s, 3, 5, 2, 2, 500);
    s.durable_point().expect("durable point");
    let state = recorded(&s);
    drop(s);

    flip(&seg_file(&d, 3, 0), a.offset + frame::HEADER_LEN as u64 + 7);
    let e = open(&d, &state).expect_err("a damaged frame below the recorded length");
    assert!(e.is_disagreement(), "{e}");
    assert!(matches!(e, SegError::Damaged { bucket: 3, .. }), "{e}");
}

#[test]
fn a_file_the_state_does_not_know_is_deleted() {
    // The crash landed between a roll and the durable point that would have
    // recorded the new file (§11.5 step 3).
    let d = TmpDir::new("rec-unknown");
    let mut s = fresh(&d, 4096);
    for i in 0..12u64 {
        push(&mut s, 6, 1, i, 1, 900);
    }
    let all = recorded(&s);
    let newest = all
        .iter()
        .filter(|f| f.bucket == 6)
        .map(|f| f.file_id)
        .max()
        .expect("files");
    assert!(newest >= 2, "the run must have rolled");
    drop(s);

    // The store forgets the newest file of bucket 6 — and with it the roll.
    let mut older: Vec<FileState> = all
        .into_iter()
        .filter(|f| f.file_id != newest || f.bucket != 6)
        .collect();
    // The file before it is what the store still calls active.
    for f in older.iter_mut() {
        if f.bucket == 6 && f.file_id == newest - 1 {
            f.sealed = false;
        }
    }
    assert!(seg_file(&d, 6, newest).exists());

    let (s, rep) = open(&d, &older).expect("recover");
    assert_eq!(rep.deleted, vec![(6, newest)]);
    assert!(!seg_file(&d, 6, newest).exists());
    assert_eq!(
        s.active_file(6),
        Some(newest - 1),
        "the previous file is active again"
    );
    assert!(
        !qidx_file(&d, 6, newest - 1).exists(),
        "a .qidx beside an active file is stale and goes"
    );
    assert!(!rep.rescanned.is_empty());
}

#[test]
fn a_missing_file_the_state_names_is_the_i11_disagreement() {
    let d = TmpDir::new("rec-missing");
    let mut s = fresh(&d, 1 << 20);
    push(&mut s, 8, 1, 0, 1, 64);
    let state = recorded(&s);
    drop(s);
    std::fs::remove_file(seg_file(&d, 8, 0)).expect("remove");
    let e = open(&d, &state).expect_err("the state names a file that is gone");
    assert!(e.is_disagreement(), "{e}");
    assert!(
        matches!(
            e,
            SegError::MissingFile {
                bucket: 8,
                file_id: 0
            }
        ),
        "{e}"
    );
}

#[test]
fn a_rebuilt_index_equals_the_one_that_was_written() {
    // The claim the `.qidx` rests on: the file is self-describing, so the
    // index is derivable from it. A rebuild must be byte-equal to the seal's.
    let d = TmpDir::new("rec-rebuild");
    let mut s = fresh(&d, 4096);
    for i in 0..30u64 {
        push(&mut s, 4, 10 + (i % 3), i, 1, 400);
    }
    s.durable_point().expect("durable point");
    let state = recorded(&s);
    let sealed: Vec<u32> = state
        .iter()
        .filter(|f| f.bucket == 4 && f.sealed)
        .map(|f| f.file_id)
        .collect();
    assert!(!sealed.is_empty(), "the run must have rolled");
    let originals: Vec<Vec<u8>> = sealed
        .iter()
        .map(|id| std::fs::read(qidx_file(&d, 4, *id)).expect("read qidx"))
        .collect();
    drop(s);

    // Case 1: the index is simply gone.
    std::fs::remove_file(qidx_file(&d, 4, sealed[0])).expect("remove");
    // Case 2: the index is there and damaged.
    if sealed.len() > 1 {
        flip(&qidx_file(&d, 4, sealed[1]), index::HEADER_LEN as u64 + 5);
    }

    let (s, rep) = open(&d, &state).expect("recover");
    let want: Vec<(u16, u32)> = sealed.iter().take(2).map(|id| (4u16, *id)).collect();
    assert_eq!(rep.rebuilt, want);
    assert!(rep.scanned_frames > 0);
    for (i, id) in sealed.iter().enumerate() {
        let got = std::fs::read(qidx_file(&d, 4, *id)).expect("read qidx");
        assert_eq!(got, originals[i], "the rebuilt index of f{id} differs");
    }

    // And the rebuilt indexes answer the same lookups.
    for i in 0..30u64 {
        let pid = 10 + (i % 3);
        let files: Vec<u32> = sealed.clone();
        if let Some(l) = s.locate(4, pid, i, &files).expect("locate") {
            assert_eq!(l.record.base_offset, i);
        }
    }
    let _ = rep.rescanned;
}

#[test]
fn a_scan_stops_at_the_first_frame_it_cannot_believe() {
    let d = TmpDir::new("rec-scan");
    let mut s = fresh(&d, 1 << 20);
    let a = push(&mut s, 7, 1, 0, 1, 200);
    let b = push(&mut s, 7, 1, 1, 1, 200);
    let c = push(&mut s, 7, 1, 2, 1, 200);
    let total = s.active_len(7);

    let clean = s.scan(7, 0, total).expect("scan");
    assert!(clean.torn.is_none());
    assert_eq!(clean.valid_bytes, total);
    assert_eq!(clean.records.len(), 3);
    assert_eq!(clean.records[1].offset, b.offset);
    assert_eq!(clean.records[2].offset, c.offset);

    // A scan bounded below the last frame reports the frame it cannot finish
    // — the torn tail of a crash, seen through the recorded length.
    let short = s.scan(7, 0, total - 10).expect("scan");
    assert!(short.torn.is_some());
    assert_eq!(short.valid_bytes, (a.len + b.len) as u64);

    // Damage the middle frame: the scan keeps what came before it and says
    // where it stopped.
    flip(&seg_file(&d, 7, 0), b.offset + frame::HEADER_LEN as u64 + 3);
    let torn = s.scan(7, 0, total).expect("scan");
    assert_eq!(torn.records.len(), 1);
    assert_eq!(torn.valid_bytes, a.len as u64);
    let (at, _) = torn.torn.expect("torn");
    assert_eq!(at, b.offset);
    let _ = c;
}

#[test]
fn recovery_repeats_safely() {
    // "A crash at any step must be safe to repeat" (§11.5).
    let d = TmpDir::new("rec-idempotent");
    let mut s = fresh(&d, 4096);
    for i in 0..20u64 {
        push(&mut s, 1, 3, i, 1, 400);
    }
    s.durable_point().expect("durable point");
    let state = recorded(&s);
    drop(s);
    let mut first: Option<Recovery> = None;
    for _ in 0..3 {
        let (s, rep) = open(&d, &state).expect("recover");
        assert_eq!(recorded(&s), state, "recovery does not move the file table");
        match &first {
            None => first = Some(rep),
            Some(f) => assert_eq!(&rep, f, "a second recovery must find nothing new"),
        }
    }
}

#[test]
fn writing_continues_where_the_recorded_length_left_off() {
    let d = TmpDir::new("rec-continue");
    let mut s = fresh(&d, 1 << 20);
    let a = push(&mut s, 1, 3, 0, 1, 400);
    s.durable_point().expect("durable point");
    let state = recorded(&s);
    drop(s);

    let (mut s, _) = open(&d, &state).expect("recover");
    let b = push(&mut s, 1, 3, 1, 1, 400);
    assert_eq!(
        b.offset, a.len as u64,
        "the next frame lands after the last"
    );
    assert_eq!(b.file_id, a.file_id);
    assert_eq!(s.read(a).expect("read").base_offset, 0);
    assert_eq!(s.read(b).expect("read").base_offset, 1);
    // The RAM index came back from the scan, so a lookup finds both.
    assert_eq!(
        s.locate(1, 3, 0, &[])
            .expect("locate")
            .expect("hit")
            .position,
        a
    );
}

#[test]
fn two_unsealed_files_in_one_bucket_are_refused() {
    // The store can only ever record one active file per bucket. If it
    // records two, something upstream is wrong and guessing which one to
    // append to would corrupt the other.
    let d = TmpDir::new("rec-two-active");
    let s = fresh(&d, 4096);
    let mut state = recorded(&s);
    drop(s);
    std::fs::copy(seg_file(&d, 0, 0), seg_file(&d, 0, 1)).expect("copy");
    state.push(FileState {
        bucket: 0,
        file_id: 1,
        len: 0,
        durable_len: 0,
        sealed: false,
        frames: 0,
        retained_frames: 0,
        retained_bytes: 0,
        window_frames: 0,
        snapshot_refs: 0,
    });
    let e = open(&d, &state).expect_err("two active files");
    assert!(matches!(e, SegError::Refused(_)), "{e}");
}

#[test]
fn an_orphan_index_left_by_a_half_done_unlink_is_swept() {
    // GC unlinks the `.seg` and then the `.qidx` (§11.7). A crash between the
    // two leaves an index with no file, which nothing else would ever remove:
    // the file table does not know it, and no lookup names it.
    let d = TmpDir::new("rec-orphan");
    let mut s = fresh(&d, 4096);
    for i in 0..12u64 {
        push(&mut s, 3, 1, i, 1, 900);
    }
    let state = recorded(&s);
    let sealed = state
        .iter()
        .find(|f| f.bucket == 3 && f.sealed)
        .expect("a sealed file")
        .file_id;
    drop(s);

    // The half-done unlink: the segment is gone, its index is not, and the
    // state no longer names either.
    std::fs::remove_file(seg_file(&d, 3, sealed)).expect("remove seg");
    assert!(qidx_file(&d, 3, sealed).exists());
    let without: Vec<FileState> = state
        .into_iter()
        .filter(|f| !(f.bucket == 3 && f.file_id == sealed))
        .collect();

    let (_s, rep) = open(&d, &without).expect("recover");
    assert!(rep.deleted.contains(&(3, sealed)));
    assert!(
        !qidx_file(&d, 3, sealed).exists(),
        "the orphan index is swept with the file the state forgot"
    );
}
