//! File GC and pins (§11.7, I10).

use super::super::*;
use super::*;

/// Fill bucket 0 until it has rolled, and return the sealed file ids and the
/// positions written into the first of them.
fn rolled(s: &mut Segments) -> (Vec<u32>, Vec<Position>) {
    let mut wrote = Vec::new();
    for i in 0..20u64 {
        wrote.push(push(s, 0, 1, i, 1, 900));
    }
    let sealed: Vec<u32> = s
        .files()
        .into_iter()
        .filter(|(b, _, m)| *b == 0 && m.sealed)
        .map(|(_, id, _)| id)
        .collect();
    assert!(!sealed.is_empty(), "the fill must roll");
    let first = sealed[0];
    let in_first: Vec<Position> = wrote.into_iter().filter(|p| p.file_id == first).collect();
    (sealed, in_first)
}

#[test]
fn a_file_lives_while_its_payload_is_retained_or_its_hashes_are_in_the_window() {
    // §11.7: "A file stays live while any segment in it is retained OR any
    // Append in it lies inside its partition's txns window" — the hash lists
    // outlive retention (D10), for the dedup probe and ack-by-hash.
    let d = TmpDir::new("gc-live");
    let mut s = fresh(&d, 4096);
    let (sealed, frames) = rolled(&mut s);
    let file = sealed[0];
    let m = s.file_meta(0, file).expect("meta");
    assert_eq!(m.retained_frames, frames.len() as u64);
    assert_eq!(m.window_frames, frames.len() as u64);
    assert!(!m.is_dead());
    assert!(s.gc_candidates(usize::MAX).is_empty());

    // Retention takes the payloads: the hashes keep the file alive.
    for p in &frames {
        s.release(*p, Release::Retained);
    }
    let m = s.file_meta(0, file).expect("meta");
    assert_eq!(m.retained_frames, 0);
    assert_eq!(m.retained_bytes, 0);
    assert!(
        !m.is_dead(),
        "the hash lists are still inside the txns window"
    );
    assert!(!s.gc_candidates(usize::MAX).contains(&(0, file)));

    // The txns purge passes them: now nothing wants the file.
    for p in &frames {
        s.release(*p, Release::Window);
    }
    assert!(s.file_meta(0, file).expect("meta").is_dead());
    assert!(s.gc_candidates(usize::MAX).contains(&(0, file)));
}

#[test]
fn a_partition_delete_retires_payload_and_hashes_together() {
    let d = TmpDir::new("gc-both");
    let mut s = fresh(&d, 4096);
    let (sealed, frames) = rolled(&mut s);
    for p in &frames {
        s.release(*p, Release::Both);
    }
    assert!(s.file_meta(0, sealed[0]).expect("meta").is_dead());
    // Releasing twice must not wrap a counter round to a live file.
    for p in &frames {
        s.release(*p, Release::Both);
    }
    let m = s.file_meta(0, sealed[0]).expect("meta");
    assert_eq!(
        (m.retained_frames, m.window_frames, m.retained_bytes),
        (0, 0, 0)
    );
}

#[test]
fn the_active_file_is_never_a_candidate_and_never_unlinked() {
    let d = TmpDir::new("gc-active");
    let mut s = fresh(&d, 1 << 20);
    let p = push(&mut s, 0, 1, 0, 1, 64);
    s.release(p, Release::Both);
    assert!(
        s.gc_candidates(usize::MAX).is_empty(),
        "an unsealed file is never dead, whatever its counters say"
    );
    let e = s.unlink(0, 0).expect_err("the active file");
    assert!(matches!(e, SegError::Refused(_)), "{e}");
    assert!(seg_file(&d, 0, 0).exists());
}

#[test]
fn a_pin_holds_a_dead_file_until_it_is_dropped() {
    // The claim pin of §11.7: retention or a delete must not unlink bytes
    // between a claim's apply and its payload read (I4).
    let d = TmpDir::new("gc-pin");
    let mut s = fresh(&d, 4096);
    let (sealed, frames) = rolled(&mut s);
    let file = sealed[0];
    let reader = s.reader();
    let pin = reader.pin(0, file).expect("a file this node holds pins");

    for p in &frames {
        s.release(*p, Release::Both);
    }
    assert!(s.file_meta(0, file).expect("meta").is_dead());
    assert!(
        !s.gc_candidates(usize::MAX).contains(&(0, file)),
        "a pinned file is not a candidate"
    );
    assert!(
        !s.unlink(0, file).expect("unlink"),
        "and cannot be unlinked"
    );
    assert!(seg_file(&d, 0, file).exists());
    // The payload is still readable through the pin, which is the point.
    assert!(reader.read(frames[0]).is_ok());

    drop(pin);
    assert!(s.gc_candidates(usize::MAX).contains(&(0, file)));
    assert!(s.unlink(0, file).expect("unlink"));
    assert!(!seg_file(&d, 0, file).exists());
    assert!(
        !qidx_file(&d, 0, file).exists(),
        "the index goes with the file"
    );
    assert!(!reader.has_file(0, file));
    // And a read of what is gone fails as "gone", never by reopening a name.
    assert!(matches!(
        reader.read(frames[0]),
        Err(SegError::MissingFile { .. })
    ));
}

#[test]
fn nested_pins_are_counted() {
    let d = TmpDir::new("gc-pin-count");
    let mut s = fresh(&d, 4096);
    let (sealed, frames) = rolled(&mut s);
    let file = sealed[0];
    for p in &frames {
        s.release(*p, Release::Both);
    }
    let r = s.reader();
    let a = r.pin(0, file).expect("pin");
    let b = r.pin(0, file).expect("pin");
    assert!(!s.unlink(0, file).expect("unlink"));
    drop(a);
    assert!(!s.unlink(0, file).expect("unlink"), "one pin is still held");
    drop(b);
    assert!(s.unlink(0, file).expect("unlink"));
}

#[test]
fn a_snapshot_reference_holds_a_dead_file() {
    let d = TmpDir::new("gc-snapshot");
    let mut s = fresh(&d, 4096);
    let (sealed, frames) = rolled(&mut s);
    let file = sealed[0];
    s.set_snapshot_ref(0, file, true);
    for p in &frames {
        s.release(*p, Release::Both);
    }
    assert!(!s.file_meta(0, file).expect("meta").is_dead());
    assert!(!s.unlink(0, file).expect("unlink"));
    s.set_snapshot_ref(0, file, false);
    assert!(s.unlink(0, file).expect("unlink"));
}

#[test]
fn unlinking_something_that_is_not_dead_or_not_there_is_a_no_op() {
    let d = TmpDir::new("gc-noop");
    let mut s = fresh(&d, 4096);
    let (sealed, _) = rolled(&mut s);
    assert!(
        !s.unlink(0, sealed[0]).expect("unlink"),
        "a live file is refused quietly"
    );
    assert!(seg_file(&d, 0, sealed[0]).exists());
    assert!(
        !s.unlink(0, 4242).expect("unlink"),
        "a file that never existed"
    );
}

#[test]
fn an_unlinked_file_is_gone_from_the_touched_set_too() {
    // Otherwise the next store commit would record a length for a file that
    // is no longer there, and the recovery after it would refuse to start
    // (I11's MissingFile).
    let d = TmpDir::new("gc-touched");
    let mut s = fresh(&d, 4096);
    let (sealed, frames) = rolled(&mut s);
    let file = sealed[0];
    for p in &frames {
        s.release(*p, Release::Both);
    }
    assert!(s.unlink(0, file).expect("unlink"));
    let t = s.take_touched();
    assert!(
        !t.iter().any(|f| f.bucket == 0 && f.file_id == file),
        "the unlinked file is not offered to the store"
    );
    // And a durable point right after does not trip over its missing handle.
    s.durable_point().expect("durable point");
}

#[test]
fn liveness_survives_a_restart_and_gc_does_not_eat_the_unacked() {
    // The refutation's scenario, exactly. Push until bucket 0 rolls, ack
    // NOTHING, restart the node against the file table the store recorded, and
    // run the documented two-phase GC. The first cut of this WP recorded only
    // `(len, sealed)`, so every sealed file came back with
    // retained/window/snapshot counters at zero: `is_dead()` for all of them,
    // `gc_candidates()` offered them, `unlink()`'s own re-check agreed, and
    // every payload was gone — for a process that had merely restarted.
    let d = TmpDir::new("gc-reopen");
    let mut s = fresh(&d, 4096);
    let (sealed, frames) = rolled(&mut s);
    assert!(sealed.len() >= 2, "the fill must make several sealed files");
    s.durable_point().expect("durable point");
    let state = s.file_states();
    drop(s);

    let (mut s, rep) = Segments::open(&d.seg(), Options::testing(4096), &state).expect("reopen");
    assert!(rep.deleted.is_empty(), "nothing is a leftover here");
    for id in &sealed {
        let m = s.file_meta(0, *id).expect("meta after the reopen");
        assert!(m.sealed);
        assert!(
            m.retained_frames > 0 && m.window_frames > 0,
            "b000/f{id} came back with its liveness: {m:?}"
        );
        assert!(!m.is_dead(), "b000/f{id} is not dead after a restart");
    }
    assert!(
        s.gc_candidates(usize::MAX).is_empty(),
        "GC has nothing to take: {:?}",
        s.gc_candidates(usize::MAX)
    );
    for id in &sealed {
        assert!(!s.unlink(0, *id).expect("unlink"), "and refuses to be told");
        assert!(seg_file(&d, 0, *id).exists());
    }
    // The payloads the restart was supposed to destroy still read back.
    for p in &frames {
        assert!(s.read(*p).is_ok(), "{p:?} survived the restart");
    }

    // And when retention and the txns purge DO retire them, the file dies —
    // across another restart, because the release reached the store.
    for p in &frames {
        s.release(*p, Release::Both);
    }
    s.durable_point().expect("durable point");
    let state = s.file_states();
    drop(s);
    let (mut s, _) = Segments::open(&d.seg(), Options::testing(4096), &state).expect("reopen");
    let first = sealed[0];
    assert!(
        s.file_meta(0, first).expect("meta").is_dead(),
        "a release recorded before the restart stays recorded"
    );
    assert!(s.gc_candidates(usize::MAX).contains(&(0, first)));
    assert!(s.unlink(0, first).expect("unlink"));
    assert!(!seg_file(&d, 0, first).exists());
}

#[test]
fn a_snapshot_reference_survives_a_restart() {
    // §11.6 retention: "hard links keep file data alive for readers". A
    // manifest that holds a file must still hold it after a boot, or the first
    // GC of the new process unlinks the file out from under a snapshot that is
    // being sent (I10).
    let d = TmpDir::new("gc-snapshot-reopen");
    let mut s = fresh(&d, 4096);
    let (sealed, frames) = rolled(&mut s);
    let file = sealed[0];
    s.set_snapshot_ref(0, file, true);
    for p in &frames {
        s.release(*p, Release::Both);
    }
    assert!(!s.file_meta(0, file).expect("meta").is_dead());
    s.durable_point().expect("durable point");
    let state = s.file_states();
    drop(s);

    let (mut s, _) = Segments::open(&d.seg(), Options::testing(4096), &state).expect("reopen");
    assert_eq!(s.file_meta(0, file).expect("meta").snapshot_refs, 1);
    assert!(!s.file_meta(0, file).expect("meta").is_dead());
    assert!(!s.gc_candidates(usize::MAX).contains(&(0, file)));
    assert!(!s.unlink(0, file).expect("unlink"));
    assert!(seg_file(&d, 0, file).exists());

    // The manifest goes away, and only then may the file.
    s.set_snapshot_ref(0, file, false);
    assert!(s.unlink(0, file).expect("unlink"));
}

#[test]
fn a_release_and_a_snapshot_reference_reach_the_store() {
    // Liveness is only durable if the writer offers it: `release` and
    // `set_snapshot_ref` mark the file touched, exactly as an append does, so
    // the caller's next store commit records the change (I11, §6.2).
    let d = TmpDir::new("gc-touched-liveness");
    let mut s = fresh(&d, 4096);
    let (sealed, frames) = rolled(&mut s);
    let file = sealed[0];
    s.take_touched();
    assert!(s.take_touched().is_empty());

    s.release(frames[0], Release::Retained);
    let t = s.take_touched();
    let row = t
        .iter()
        .find(|f| f.bucket == 0 && f.file_id == file)
        .expect("the released file is offered to the store");
    assert_eq!(row.retained_frames, frames.len() as u64 - 1);
    assert_eq!(row.window_frames, frames.len() as u64);
    assert_eq!(row.frames, frames.len() as u64);

    s.set_snapshot_ref(0, file, true);
    let t = s.take_touched();
    let row = t
        .iter()
        .find(|f| f.bucket == 0 && f.file_id == file)
        .expect("the referenced file is offered to the store");
    assert_eq!(row.snapshot_refs, 1);
}

#[test]
fn a_file_table_that_dropped_its_liveness_is_refused_at_open() {
    // The belt for the mistake above: a caller that persists only lengths
    // hands back sealed files that claim to have held no frame. That is not a
    // state to act on — acting on it is the data loss — so the open refuses
    // and says why, instead of booting into a GC that eats the node.
    let d = TmpDir::new("gc-lossy-state");
    let mut s = fresh(&d, 4096);
    let (sealed, _) = rolled(&mut s);
    s.durable_point().expect("durable point");
    let lossy: Vec<FileState> = s
        .file_states()
        .into_iter()
        .map(|f| FileState {
            frames: 0,
            retained_frames: 0,
            retained_bytes: 0,
            window_frames: 0,
            snapshot_refs: 0,
            ..f
        })
        .collect();
    drop(s);
    assert!(!sealed.is_empty());
    let e = Segments::open(&d.seg(), Options::testing(4096), &lossy)
        .expect_err("a lossy file table is refused");
    assert!(matches!(e, SegError::Refused(_)), "{e}");

    // A file claiming more live frames than it ever held is refused too.
    let (mut s, _) = Segments::open(&d.seg(), Options::testing(4096), &[]).expect("start over");
    let (sealed, _) = rolled(&mut s);
    let mut state = s.file_states();
    for f in state.iter_mut() {
        if f.bucket == 0 && f.file_id == sealed[0] {
            f.window_frames = f.frames + 1;
        }
    }
    drop(s);
    let e = Segments::open(&d.seg(), Options::testing(4096), &state)
        .expect_err("an impossible count is refused");
    assert!(matches!(e, SegError::Refused(_)), "{e}");
}

#[test]
fn a_pin_is_never_granted_for_a_file_this_node_no_longer_holds() {
    // §11.7 asks the pin for one thing: while it is held, the bytes are
    // there. `pin` inserted into the pin table unconditionally, without ever
    // looking at the file table, so it answered "held" for a file that was
    // gone — and for a file that never existed. The caller then read, and got
    // MissingFile from under a pin that had just told it the opposite.
    let d = TmpDir::new("gc-pin-gone");
    let mut s = fresh(&d, 4096);
    let (sealed, frames) = rolled(&mut s);
    assert!(sealed.len() >= 2, "the fill must make several sealed files");
    let r = s.reader();
    for p in &frames {
        s.release(*p, Release::Both);
    }
    let gone = sealed[0];
    assert!(s.unlink(0, gone).expect("unlink"));
    assert!(
        r.pin(0, gone).is_none(),
        "a file this node has unlinked cannot be pinned"
    );
    assert!(
        r.pin(0, 4242).is_none(),
        "nor one that never existed on this node"
    );
    // The file it DOES hold pins, and that pin holds it.
    let live = sealed[1];
    let pin = r.pin(0, live).expect("a file this node holds pins");
    assert!(!s.unlink(0, live).expect("unlink"), "the pin wins");
    drop(pin);
}

#[test]
fn a_pin_and_an_unlink_cannot_both_win() {
    // The interleaving behind the finding: `unlink` passed its pin check,
    // dropped the pins lock, and was inside `evict`/`forget_sealed` when a
    // blocking-pool task took a pin for the same file for a committed pop
    // claim. Both believed they had won: the file was unlinked and the pin's
    // read answered MissingFile, which is exactly what §11.7 and I4 promise
    // cannot happen between a claim's apply and its payload read.
    //
    // Every round starts the two at one barrier. Whichever wins is fine; the
    // assertion is that they never both do.
    let d = TmpDir::new("gc-pin-race");
    let mut s = fresh(&d, 4096);
    let mut wrote = Vec::new();
    for i in 0..60u64 {
        wrote.push(push(&mut s, 0, 1, i, 1, 900));
    }
    s.roll(0).expect("roll");
    for p in &wrote {
        s.release(*p, Release::Both);
    }
    let sealed: Vec<u32> = s
        .files()
        .into_iter()
        .filter(|(b, _, m)| *b == 0 && m.sealed)
        .map(|(_, id, _)| id)
        .collect();
    assert!(sealed.len() >= 8, "the fill must make many sealed files");

    let r = s.reader();
    let mut pinned = 0;
    let mut unlinked = 0;
    for id in sealed {
        let first = wrote
            .iter()
            .find(|p| p.file_id == id)
            .copied()
            .expect("a frame of that file");
        let barrier = std::sync::Barrier::new(2);
        let (gone, pin) = std::thread::scope(|sc| {
            let h = sc.spawn(|| {
                barrier.wait();
                r.pin(0, id)
            });
            barrier.wait();
            let gone = s.unlink(0, id).expect("unlink");
            (gone, h.join().expect("the pinning thread"))
        });
        if gone {
            unlinked += 1;
            assert!(
                pin.is_none(),
                "b000/f{id} was unlinked AND pinned: the pin guarantees nothing"
            );
            assert!(!seg_file(&d, 0, id).exists());
            assert!(matches!(r.read(first), Err(SegError::MissingFile { .. })));
        } else {
            pinned += 1;
            let pin = pin.expect("the unlink refused, so the pin is what refused it");
            assert!(seg_file(&d, 0, id).exists());
            assert!(r.read(first).is_ok(), "the pinned bytes are readable");
            drop(pin);
            assert!(
                s.unlink(0, id).expect("unlink"),
                "and go when it is dropped"
            );
        }
    }
    assert!(unlinked + pinned >= 8);
}
