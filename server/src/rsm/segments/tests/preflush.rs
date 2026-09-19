//! The async durable-point pre-flush (§11.4, `QUEEN_RAFT_DURABLE_ASYNC`).
//!
//! [`Segments::preflush_batch`] hands the helper thread the dirty segment
//! handles so it can push their pages to the device between durable points. It
//! is a page-cache warm-up and NOTHING recovery trusts: these tests pin the two
//! properties the correctness of the lever rests on.
//!
//! 1. A pre-flush records nothing — not `unsynced_bytes`, not the file table —
//!    so a durable point that follows it lands byte-for-byte the same state as
//!    one with no pre-flush at all.
//! 2. A pre-flush is not a barrier: bytes a pre-flush pushed to disk but that no
//!    durable point recorded are truncated at recovery exactly as un-synced
//!    bytes are. This is the "kill -9 between the helper's fsync and the store
//!    commit" window — the helper synced, the point did not, and I11 discards
//!    the tail rather than trusting it.

use std::fs::OpenOptions;
use std::io::Write;

use super::super::*;
use super::*;

fn open(d: &TmpDir, rec: &[FileState]) -> Result<(Segments, Recovery)> {
    Segments::open(&d.seg(), Options::testing(4096), rec)
}

#[test]
fn a_preflush_covers_every_dirty_file_and_records_nothing() {
    let d = TmpDir::new("preflush-covers");
    let mut s = fresh(&d, 1 << 20);
    push(&mut s, 1, 5, 0, 4, 300);
    push(&mut s, 1, 5, 4, 4, 300);
    push(&mut s, 7, 9, 0, 2, 300);

    let unsynced_before = s.unsynced_bytes();
    let states_before = s.file_states();
    assert!(unsynced_before > 0, "there are unflushed bytes to warm");

    let batch = s.preflush_batch();
    // One handle per bucket that has dirty bytes: two active files here.
    assert_eq!(batch.len(), 2, "a handle for each dirty active file");
    assert!(!batch.is_empty());
    batch.sync();

    // The warm-up moved not one byte of bookkeeping: the caller still owes the
    // very same durable point it did before.
    assert_eq!(
        s.unsynced_bytes(),
        unsynced_before,
        "unsynced_bytes untouched"
    );
    assert_eq!(
        s.file_states(),
        states_before,
        "the file table is untouched"
    );
}

#[test]
fn an_empty_tree_yields_an_empty_batch() {
    let d = TmpDir::new("preflush-empty");
    let s = fresh(&d, 4096);
    let batch = s.preflush_batch();
    assert!(batch.is_empty(), "nothing dirty, nothing to warm");
    assert_eq!(batch.len(), 0);
    batch.sync(); // a no-op that must not panic
}

#[test]
fn a_durable_point_after_a_preflush_records_the_same_state() {
    // Two trees fed the identical writes; one is warmed before the point, the
    // other is not. The recorded state and the bytes read back must match: the
    // pre-flush is invisible to what a durable point does.
    let warmed = {
        let d = TmpDir::new("preflush-warm");
        let mut s = fresh(&d, 4096);
        push(&mut s, 2, 5, 0, 2, 500);
        push(&mut s, 2, 5, 2, 2, 500);
        push(&mut s, 3, 8, 0, 3, 200);
        s.preflush_batch().sync();
        s.durable_point().expect("durable point");
        (s.file_states(), s.active_len(2), s.active_len(3))
    };
    let plain = {
        let d = TmpDir::new("preflush-plain");
        let mut s = fresh(&d, 4096);
        push(&mut s, 2, 5, 0, 2, 500);
        push(&mut s, 2, 5, 2, 2, 500);
        push(&mut s, 3, 8, 0, 3, 200);
        s.durable_point().expect("durable point");
        (s.file_states(), s.active_len(2), s.active_len(3))
    };
    assert_eq!(
        warmed, plain,
        "the pre-flush changed nothing the point recorded"
    );
}

#[test]
fn a_preflush_is_not_a_barrier_its_bytes_are_truncated_without_a_point() {
    // The crash the async lever must survive: the helper fsynced the file
    // (pre-flush), then the process died BEFORE the store commit that records
    // the new length. Recovery reopens with the state the last DURABLE point
    // recorded and must cut the pre-flushed tail back — never trust it (I11).
    let d = TmpDir::new("preflush-not-barrier");
    let mut s = fresh(&d, 1 << 20);
    let a = push(&mut s, 4, 6, 0, 2, 400);
    s.durable_point().expect("first point records only A");
    let state_at_a = s.file_states();
    let len_at_a = s.active_len(4);

    // B is written and PRE-FLUSHED to the device, but no durable point records
    // it — exactly the helper-synced-but-not-committed window.
    let b = push(&mut s, 4, 6, 2, 2, 400);
    let batch = s.preflush_batch();
    assert!(!batch.is_empty());
    batch.sync();
    let len_with_b = s.active_len(4);
    assert!(len_with_b > len_at_a, "B grew the file on disk");
    drop(s);

    // The bytes are physically on the platter (the pre-flush put them there),
    // yet the store never recorded them, so recovery truncates to A's length.
    let on_disk = std::fs::metadata(seg_file(&d, 4, 0)).expect("meta").len();
    assert_eq!(on_disk, len_with_b, "the pre-flushed bytes are on disk");

    let (s, rep) = open(&d, &state_at_a).expect("recover to the last point");
    assert_eq!(
        rep.truncated,
        vec![(4, 0, len_with_b, len_at_a)],
        "the pre-flushed-but-uncommitted tail is cut back (I11)"
    );
    assert_eq!(s.active_len(4), len_at_a);
    assert_eq!(s.read(a).expect("A survives").base_offset, 0);
    assert!(s.read(b).is_err(), "B was never durable and is gone");
}

#[test]
fn a_preflush_after_a_seal_warms_the_sealed_file_too() {
    // A small segment size makes the second push roll and seal the first file;
    // its handle must appear in the batch so the helper flushes it ahead of the
    // point, which is the "fsync a file as soon as it is sealed" case.
    let d = TmpDir::new("preflush-sealed");
    let mut s = fresh(&d, 1024);
    push(&mut s, 1, 5, 0, 1, 700); // ~756 B, fills most of the 1024-byte file
    push(&mut s, 1, 5, 1, 1, 700); // rolls: file 0 seals, file 1 is active
    assert_eq!(s.active_file(1), Some(1), "the bucket rolled to file 1");

    let batch = s.preflush_batch();
    // The sealed file 0 (.seg + .qidx) and the active file 1: at least three
    // handles, and never fewer than the one active file.
    assert!(batch.len() >= 2, "the sealed file's handles are warmed too");
    batch.sync();

    // And the point still records the truth, sealed file included.
    let unsynced = s.unsynced_bytes();
    assert!(unsynced > 0);
    s.durable_point().expect("durable point");
    assert_eq!(s.unsynced_bytes(), 0, "the point cleared what was owed");
}
