//! Phase C: the RAM keyspaces over their LMDB checkpoint (see
//! [`super::heed_store`]'s module header).
//!
//! What is checked: EVERY keyspace is a RAM table (one visibility horizon, one
//! durability horizon); a write is visible LIVE to a concurrent reader; only a
//! durable cycle checkpoints it (a plain commit carries NO keyspace, which a
//! reopen shows); a delete is a dirty key that leaves the checkpoint at the
//! next durable cycle; a failed durable cycle keeps its keys dirty; abort and
//! drop undo nothing; a RAM scan answers EXACTLY what the documented scan
//! contract answers for every bound (a reference walk over a plain ordered
//! map); the `get_raw` arena of the write handle is released by every mutating
//! call; and readers that re-enter the same keyspace from a scan callback never
//! deadlock against the writer.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;

use super::{
    HeedStore, Keyspace, Reads, Store, StoreError, StoreMetrics, StoreOpts, TypedReads,
    TypedWrites, Writes,
};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// A store in a unique temp directory, removed on drop.
struct Tmp {
    store: Option<HeedStore>,
    dir: PathBuf,
}

fn opts() -> StoreOpts {
    StoreOpts {
        map_bytes: Some(64 << 20),
        ..Default::default()
    }
}

impl Tmp {
    fn new() -> Tmp {
        let dir = std::env::temp_dir().join(format!(
            "queen-rsm-ram-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = HeedStore::open(&dir, &opts()).expect("open");
        Tmp {
            store: Some(store),
            dir,
        }
    }

    fn s(&self) -> &HeedStore {
        self.store.as_ref().expect("open")
    }

    /// Close (dirty RAM rows are NOT written — a crash, for them) and reopen.
    fn reopen(&mut self) {
        if let Some(s) = self.store.take() {
            s.close();
        }
        self.store = Some(HeedStore::open(&self.dir, &opts()).expect("reopen"));
    }
}

impl Drop for Tmp {
    fn drop(&mut self) {
        if let Some(s) = self.store.take() {
            s.close();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The checkpoint's copy of a row, read from a thread that holds no other
/// transaction (LMDB: one transaction per thread).
fn checkpoint(s: &HeedStore, ks: Keyspace, key: &[u8]) -> Option<Vec<u8>> {
    std::thread::scope(|sc| {
        sc.spawn(|| s.checkpoint_get(ks, key).expect("checkpoint read"))
            .join()
            .expect("checkpoint thread")
    })
}

#[test]
fn the_ram_split_is_the_phase_c_one() {
    // The final Phase C split is no split: EVERY keyspace is a RAM table over
    // its LMDB checkpoint — one visibility horizon and one durability horizon
    // (`Keyspace::is_ram`). The node-local ones too: they reach LMDB at the
    // same durable checkpoint as the `applied_index` they must agree with.
    for ks in Keyspace::ALL {
        assert!(ks.is_ram(), "{} is not a RAM keyspace", ks.name());
    }
    // And the store really serves each one from its table: a put is a dirty
    // RAM key in every keyspace (an LMDB-direct keyspace has no dirty set),
    // which a plain commit leaves out of LMDB and the durable cycle writes.
    let t = Tmp::new();
    let s = t.s();
    let mut w = s.write().unwrap();
    for ks in Keyspace::ALL {
        w.put_raw(ks, b"split", b"v").unwrap();
        assert_eq!(s.dirty_len(ks), 1, "{} is not served from RAM", ks.name());
    }
    w.commit().unwrap();
    for ks in Keyspace::ALL {
        assert_eq!(
            s.dirty_len(ks),
            1,
            "{}: a plain commit took the key",
            ks.name()
        );
        assert_eq!(
            checkpoint(s, ks, b"split"),
            None,
            "{} reached LMDB before a durable cycle",
            ks.name()
        );
    }
    w.durable_commit().unwrap();
    for ks in Keyspace::ALL {
        assert_eq!(s.dirty_len(ks), 0, "{} still dirty", ks.name());
        assert_eq!(
            checkpoint(s, ks, b"split").as_deref(),
            Some(&b"v"[..]),
            "{} missed the checkpoint",
            ks.name()
        );
    }
}

// ---------------------------------------------------------------------------
// (a) live visibility
// ---------------------------------------------------------------------------

#[test]
fn a_ram_put_is_visible_to_a_concurrent_reader_before_any_commit() {
    let t = Tmp::new();
    let s = t.s();
    let mut w = s.write().unwrap();
    w.put_raw(Keyspace::Queues, b"k", b"v1").unwrap();
    // `dlq` was LMDB-direct (snapshot semantics) before the final Phase C
    // split; it is a RAM table like every keyspace now, so it is live too.
    w.put_raw(Keyspace::Dlq, b"k", b"d1").unwrap();

    let (tx_got, rx_got) = mpsc::channel::<()>();
    let (tx_go, rx_go) = mpsc::channel::<()>();
    std::thread::scope(|sc| {
        let reader = sc.spawn(move || {
            s.read(|r| {
                let v1 = r
                    .get_raw(Keyspace::Queues, b"k")?
                    .expect("a RAM row is live");
                assert_eq!(v1, b"v1");
                assert_eq!(
                    r.get_raw(Keyspace::Dlq, b"k")?,
                    Some(&b"d1"[..]),
                    "every keyspace is live: there is no snapshot to wait for"
                );
                let mut seen = Vec::new();
                r.scan_raw(Keyspace::Queues, &[], &[], usize::MAX, &mut |k, v| {
                    seen.push((k.to_vec(), v.to_vec()));
                    true
                })?;
                assert_eq!(seen, vec![(b"k".to_vec(), b"v1".to_vec())]);

                // The writer replaces the value while this reader still holds
                // a slice of the old one: the arena keeps those bytes alive,
                // and a fresh read sees the new value at once.
                tx_got.send(()).unwrap();
                rx_go.recv().unwrap();
                assert_eq!(v1, b"v1", "the returned slice outlives the replace");
                assert_eq!(r.get_raw(Keyspace::Queues, b"k")?, Some(&b"v2"[..]));
                Ok(())
            })
        });
        rx_got.recv().unwrap();
        w.put_raw(Keyspace::Queues, b"k", b"v2").unwrap();
        w.del_raw(Keyspace::Queues, b"never-there").unwrap();
        tx_go.send(()).unwrap();
        reader.join().unwrap().unwrap();
    });

    // The writer reads its own writes.
    assert_eq!(w.get_raw(Keyspace::Queues, b"k").unwrap(), Some(&b"v2"[..]));
    assert_eq!(w.get_raw(Keyspace::Dlq, b"k").unwrap(), Some(&b"d1"[..]));
    // Nothing reached the checkpoint: no commit of any kind has happened.
    assert_eq!(s.dirty_len(Keyspace::Queues), 1);
    assert_eq!(s.dirty_len(Keyspace::Dlq), 1);
    assert_eq!(checkpoint(s, Keyspace::Dlq, b"k"), None);
}

// ---------------------------------------------------------------------------
// (b) + (c) what a reopen finds
// ---------------------------------------------------------------------------

#[test]
fn a_durable_cycle_checkpoints_the_ram_rows_across_a_reopen() {
    let mut t = Tmp::new();
    {
        let s = t.s();
        let mut w = s.write().unwrap();
        w.put_raw(Keyspace::Queues, b"q", b"cfg").unwrap();
        w.put_raw(Keyspace::Counters, b"c", &7i64.to_le_bytes())
            .unwrap();
        w.set_applied(11, 2).unwrap();
        w.set_meta_u64(super::meta::DURABLE_INDEX, 11).unwrap();
        assert!(s.dirty_len(Keyspace::Meta) >= 2);
        assert_eq!(checkpoint(s, Keyspace::Queues, b"q"), None);

        w.durable_commit().unwrap();
        assert_eq!(StoreMetrics::get(&s.metrics().durable_commits), 1);
        for ks in Keyspace::ALL {
            assert_eq!(s.dirty_len(ks), 0, "{} still dirty", ks.name());
        }
        assert_eq!(
            checkpoint(s, Keyspace::Queues, b"q").as_deref(),
            Some(&b"cfg"[..])
        );
    }
    t.reopen();
    let s = t.s();
    for ks in Keyspace::ALL {
        assert_eq!(s.dirty_len(ks), 0, "a load leaves nothing dirty");
    }
    s.read(|r| {
        assert_eq!(r.get_raw(Keyspace::Queues, b"q")?, Some(&b"cfg"[..]));
        assert_eq!(
            r.get_raw(Keyspace::Counters, b"c")?,
            Some(&7i64.to_le_bytes()[..])
        );
        assert_eq!(r.applied_index()?, 11);
        assert_eq!(r.durable_index()?, 11);
        Ok(())
    })
    .unwrap();
}

#[test]
fn a_plain_commit_checkpoints_no_keyspace() {
    let mut t = Tmp::new();
    {
        let s = t.s();
        let mut w = s.write().unwrap();
        // A durable point at applied index 5 first: the checkpoint to reopen at.
        w.put_raw(Keyspace::Queues, b"old", b"o").unwrap();
        w.put_raw(Keyspace::Dlq, b"old", b"o").unwrap();
        w.set_applied(5, 1).unwrap();
        w.durable_commit().unwrap();

        // Then work covered only by plain commits — in the hot keyspaces and
        // in the ones that were LMDB-direct before the final Phase C split
        // (`dlq`, and the node-local `files`, which used to ride the plain
        // commit with the file lengths). One durability horizon for all.
        w.put_raw(Keyspace::Queues, b"new", b"n").unwrap();
        w.put_raw(Keyspace::Queues, b"old", b"o2").unwrap();
        w.set_applied(9, 1).unwrap();
        w.put_raw(Keyspace::Dlq, b"direct", b"d").unwrap();
        w.put_raw(Keyspace::Dlq, b"old", b"o2").unwrap();
        w.put_raw(Keyspace::Files, b"f", b"len").unwrap();
        w.commit().unwrap();
        w.put_raw(Keyspace::Pending, b"p", b"1").unwrap();
        w.commit().unwrap();
        assert_eq!(StoreMetrics::get(&s.metrics().commits), 2);
        assert_eq!(s.dirty_len(Keyspace::Queues), 2);
        assert_eq!(s.dirty_len(Keyspace::Dlq), 2);
        assert_eq!(s.dirty_len(Keyspace::Files), 1);
        assert_eq!(s.dirty_len(Keyspace::Pending), 1);
        assert!(s.dirty_len(Keyspace::Meta) >= 1);
        assert_eq!(
            checkpoint(s, Keyspace::Dlq, b"direct"),
            None,
            "a plain commit wrote a row into LMDB"
        );
        // Live readers see all of it.
        std::thread::scope(|sc| {
            sc.spawn(|| {
                s.read(|r| {
                    assert_eq!(r.get_raw(Keyspace::Queues, b"new")?, Some(&b"n"[..]));
                    assert_eq!(r.get_raw(Keyspace::Dlq, b"direct")?, Some(&b"d"[..]));
                    assert_eq!(r.get_raw(Keyspace::Files, b"f")?, Some(&b"len"[..]));
                    assert_eq!(r.applied_index()?, 9);
                    Ok(())
                })
                .unwrap()
            });
        });
    }
    // Close without a durable cycle: for every keyspace this is a crash.
    t.reopen();
    t.s()
        .read(|r| {
            assert!(
                r.get_raw(Keyspace::Queues, b"new")?.is_none(),
                "a row covered only by plain commits is not in the checkpoint"
            );
            assert_eq!(
                r.get_raw(Keyspace::Queues, b"old")?,
                Some(&b"o"[..]),
                "the checkpoint's value, not the later one"
            );
            assert!(r.get_raw(Keyspace::Pending, b"p")?.is_none());
            assert_eq!(r.applied_index()?, 5, "meta reopens at the checkpoint");
            // No keyspace reopens AHEAD of the checkpoint any more, so replay
            // from `durable_index + 1` cannot apply an effect twice.
            assert!(
                r.get_raw(Keyspace::Dlq, b"direct")?.is_none(),
                "a formerly LMDB-direct row rode the plain commit"
            );
            assert_eq!(r.get_raw(Keyspace::Dlq, b"old")?, Some(&b"o"[..]));
            assert!(r.get_raw(Keyspace::Files, b"f")?.is_none());
            Ok(())
        })
        .unwrap();
}

// ---------------------------------------------------------------------------
// (d) deletes, and a durable cycle that did not happen
// ---------------------------------------------------------------------------

#[test]
fn a_delete_is_dirty_and_leaves_the_checkpoint_at_the_next_durable_cycle() {
    let mut t = Tmp::new();
    {
        let s = t.s();
        let mut w = s.write().unwrap();
        w.put_raw(Keyspace::Cursors, b"c1", b"x").unwrap();
        w.put_raw(Keyspace::Cursors, b"c2", b"y").unwrap();
        w.durable_commit().unwrap();
        assert_eq!(s.dirty_len(Keyspace::Cursors), 0);
        assert_eq!(
            checkpoint(s, Keyspace::Cursors, b"c1").as_deref(),
            Some(&b"x"[..])
        );

        let deleted_before = StoreMetrics::get(&s.metrics().rows_deleted);
        assert!(w.del_raw(Keyspace::Cursors, b"c1").unwrap());
        assert!(
            !w.del_raw(Keyspace::Cursors, b"c1").unwrap(),
            "already gone"
        );
        assert_eq!(
            StoreMetrics::get(&s.metrics().rows_deleted),
            deleted_before + 1
        );
        assert_eq!(s.dirty_len(Keyspace::Cursors), 1, "the delete is dirty");
        assert!(w.get_raw(Keyspace::Cursors, b"c1").unwrap().is_none());

        // A plain commit does not carry it to the checkpoint.
        w.commit().unwrap();
        assert_eq!(s.dirty_len(Keyspace::Cursors), 1);
        assert_eq!(
            checkpoint(s, Keyspace::Cursors, b"c1").as_deref(),
            Some(&b"x"[..])
        );

        // The durable cycle does.
        w.durable_commit().unwrap();
        assert_eq!(s.dirty_len(Keyspace::Cursors), 0);
        assert_eq!(checkpoint(s, Keyspace::Cursors, b"c1"), None);
        assert_eq!(
            checkpoint(s, Keyspace::Cursors, b"c2").as_deref(),
            Some(&b"y"[..])
        );

        // Delete then re-put inside one interval: the checkpoint gets the
        // final value.
        w.del_raw(Keyspace::Cursors, b"c2").unwrap();
        w.put_raw(Keyspace::Cursors, b"c2", b"z").unwrap();
        assert_eq!(s.dirty_len(Keyspace::Cursors), 1);
        w.durable_commit().unwrap();
        assert_eq!(
            checkpoint(s, Keyspace::Cursors, b"c2").as_deref(),
            Some(&b"z"[..])
        );

        // Put then delete of a key the checkpoint never had: the durable
        // cycle's delete of an absent row is a no-op, not an error.
        w.put_raw(Keyspace::Cursors, b"c3", b"t").unwrap();
        w.del_raw(Keyspace::Cursors, b"c3").unwrap();
        w.durable_commit().unwrap();
        assert_eq!(checkpoint(s, Keyspace::Cursors, b"c3"), None);
    }
    t.reopen();
    t.s()
        .read(|r| {
            assert!(r.get_raw(Keyspace::Cursors, b"c1")?.is_none());
            assert_eq!(r.get_raw(Keyspace::Cursors, b"c2")?, Some(&b"z"[..]));
            assert!(r.get_raw(Keyspace::Cursors, b"c3")?.is_none());
            assert_eq!(r.count(Keyspace::Cursors)?, 1);
            Ok(())
        })
        .unwrap();
}

#[test]
fn a_ram_delete_range_is_bounded_resumes_and_marks_every_victim_dirty() {
    let t = Tmp::new();
    let s = t.s();
    let mut w = s.write().unwrap();
    for i in 0u8..10 {
        w.put_raw(Keyspace::Pending, &[b'p', i], b"r").unwrap();
    }
    w.put_raw(Keyspace::Pending, b"q", b"other").unwrap();
    w.durable_commit().unwrap();

    let mut from = Vec::new();
    let mut total = 0;
    loop {
        let (n, next) = w.delete_range(Keyspace::Pending, &from, b"p", 4).unwrap();
        total += n;
        match next {
            Some(k) if n == 4 => from = k,
            _ => break,
        }
    }
    assert_eq!(total, 10);
    assert_eq!(s.dirty_len(Keyspace::Pending), 10);
    assert_eq!(
        w.count(Keyspace::Pending).unwrap(),
        1,
        "`q` is outside the prefix"
    );
    w.durable_commit().unwrap();
    assert_eq!(checkpoint(s, Keyspace::Pending, &[b'p', 3]), None);
    assert_eq!(
        checkpoint(s, Keyspace::Pending, b"q").as_deref(),
        Some(&b"other"[..])
    );
}

#[test]
fn a_durable_cycle_whose_sync_fails_keeps_its_keys_dirty() {
    let t = Tmp::new();
    let s = t.s();
    {
        let mut w = s.write().unwrap();
        w.put_raw(Keyspace::Groups, b"g", b"1").unwrap();
        w.set_applied(3, 1).unwrap();
        s.fail_next_sync();
        let e = w.durable_commit().unwrap_err();
        assert!(e.lost_durable_point(), "{e}");
        assert!(e.fatal());
        // The keys the cycle had taken are dirty again, so no later
        // checkpoint can silently miss them.
        assert_eq!(s.dirty_len(Keyspace::Groups), 1);
        assert!(s.dirty_len(Keyspace::Meta) >= 1);

        // The handle is dead for the RAM keyspaces too, and a refused write
        // leaves the table untouched.
        assert_eq!(w.put_raw(Keyspace::Groups, b"h", b"2").unwrap_err(), e);
        assert_eq!(w.del_raw(Keyspace::Groups, b"g").unwrap_err(), e);
        assert_eq!(w.get_raw(Keyspace::Groups, b"g").unwrap_err(), e);
        let scanned = w.scan_raw(Keyspace::Groups, &[], &[], 10, &mut |_, _| true);
        assert_eq!(scanned.unwrap_err(), e);
        assert!(matches!(e, StoreError::CommitFailed { durable: true, .. }));
    }
    s.read(|r| {
        assert!(r.get_raw(Keyspace::Groups, b"h")?.is_none());
        assert_eq!(r.get_raw(Keyspace::Groups, b"g")?, Some(&b"1"[..]));
        Ok(())
    })
    .unwrap();
    // A later handle's durable cycle checkpoints what the failed one could not.
    let mut w = s.write().unwrap();
    w.durable_commit().unwrap();
    drop(w);
    assert_eq!(s.dirty_len(Keyspace::Groups), 0);
    assert_eq!(
        checkpoint(s, Keyspace::Groups, b"g").as_deref(),
        Some(&b"1"[..])
    );
}

#[test]
fn abort_and_drop_do_not_undo_ram_writes() {
    let t = Tmp::new();
    let s = t.s();
    {
        let mut w = s.write().unwrap();
        w.put_raw(Keyspace::Queues, b"q", b"v").unwrap();
        // `dedup` was LMDB-direct (and rolled back by an abort) before the
        // final Phase C split; it is a RAM table now like every keyspace.
        w.put_raw(Keyspace::Dedup, b"d", b"v").unwrap();
        w.abort().unwrap();
        assert_eq!(
            w.get_raw(Keyspace::Queues, b"q").unwrap(),
            Some(&b"v"[..]),
            "RAM is not rolled back (module header)"
        );
        assert_eq!(
            w.get_raw(Keyspace::Dedup, b"d").unwrap(),
            Some(&b"v"[..]),
            "no keyspace is rolled back any more"
        );
        w.put_raw(Keyspace::Queues, b"q2", b"v").unwrap();
    }
    // Dropped without a commit: still there, still dirty.
    assert_eq!(s.dirty_len(Keyspace::Queues), 2);
    assert_eq!(s.dirty_len(Keyspace::Dedup), 1);
    let mut w = s.write().unwrap();
    w.durable_commit().unwrap();
    assert_eq!(
        checkpoint(s, Keyspace::Queues, b"q2").as_deref(),
        Some(&b"v"[..])
    );
    assert_eq!(
        checkpoint(s, Keyspace::Dedup, b"d").as_deref(),
        Some(&b"v"[..])
    );
}

// ---------------------------------------------------------------------------
// (e) scan semantics: the RAM walk against the documented contract
// ---------------------------------------------------------------------------

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

/// The scan contract of [`Reads::scan_raw`] / [`Reads::scan_rev_raw`]
/// (`store/mod.rs`), walked over a plain ordered map and written from the
/// CONTRACT, not from the adapter's bounds: the prefix is a RANGE (every key
/// that starts with it, contiguous in memcmp order) and an empty prefix is the
/// whole keyspace; `from` clamps into it — forward from the first key ≥ `from`,
/// reverse from the last key ≤ `from`, an empty `from` starting at the matching
/// end of the range; a `limit` of 0 still hands over one row; and the row the
/// callback says stop on is counted.
fn reference(
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    from: &[u8],
    prefix: &[u8],
    limit: usize,
    rev: bool,
    stop_after: Option<usize>,
) -> (usize, Rows) {
    let in_range = |k: &[u8]| {
        k.starts_with(prefix) && (from.is_empty() || if rev { k <= from } else { k >= from })
    };
    let walk: Box<dyn Iterator<Item = (&Vec<u8>, &Vec<u8>)>> = if rev {
        Box::new(model.iter().rev())
    } else {
        Box::new(model.iter())
    };
    let want = limit.max(1);
    let mut out: Rows = Vec::new();
    for (k, v) in walk.filter(|(k, _)| in_range(k)) {
        out.push((k.clone(), v.clone()));
        if out.len() >= want || stop_after.is_some_and(|s| out.len() >= s) {
            break;
        }
    }
    (out.len(), out)
}

fn run<R: Reads>(
    r: &R,
    ks: Keyspace,
    from: &[u8],
    prefix: &[u8],
    limit: usize,
    rev: bool,
    stop_after: Option<usize>,
) -> (usize, Rows) {
    let mut out: Rows = Vec::new();
    let mut cb = |k: &[u8], v: &[u8]| {
        out.push((k.to_vec(), v.to_vec()));
        stop_after.is_none_or(|s| out.len() < s)
    };
    let n = if rev {
        r.scan_rev_raw(ks, from, prefix, limit, &mut cb)
    } else {
        r.scan_raw(ks, from, prefix, limit, &mut cb)
    }
    .expect("scan");
    (n, out)
}

/// Keys that exercise every clamp: four tenants — one of them all `0xFF`, so
/// its prefix has no end — each with its bare prefix as a key too, and
/// enough rows (644) to cross the RAM walk's chunk boundary twice.
fn scan_fixture() -> Rows {
    let mut rows = Vec::new();
    for t in [&b"a"[..], b"b", b"b\xff", b"\xff\xff"] {
        rows.push((t.to_vec(), [t, b"=bare"].concat()));
        for i in 0u16..160 {
            let k = [t, &i.to_be_bytes()[..]].concat();
            let v = [&b"v"[..], &k].concat();
            rows.push((k, v));
        }
    }
    rows
}

/// Every scan shape against [`reference`]. Disagreements are COLLECTED, grouped
/// by shape — (reverse, empty prefix, empty `from`) — and reported together, so
/// one run names every class of defect rather than the first case of one.
fn compare_all<R: Reads>(r: &R, model: &BTreeMap<Vec<u8>, Vec<u8>>, what: &str) {
    let prefixes: [&[u8]; 9] = [
        b"",
        b"a",
        b"b",
        b"b\xff",
        b"\xff",
        b"\xff\xff",
        b"c",
        b"b\x00",
        b"a\x00\x05",
    ];
    let froms: [&[u8]; 11] = [
        b"",
        b"a",
        b"a\x00\x10",
        b"b",
        b"b\x00\x50",
        b"b\xff\x00\x01",
        b"c",
        b"\xff\xff\x00\x9f",
        b"\xff\xff\xff",
        b"0",
        &[0xff; 5],
    ];
    let limits = [0usize, 1, 2, 255, 256, 257, 300, 1000, usize::MAX];
    let mut compared = 0u64;
    let mut nonempty = 0u64;
    let mut shapes: BTreeMap<(bool, bool, bool), u64> = BTreeMap::new();
    let mut first: Vec<String> = Vec::new();
    let head =
        |rows: &Rows| -> Vec<Vec<u8>> { rows.iter().take(4).map(|(k, _)| k.clone()).collect() };
    for prefix in prefixes {
        for from in froms {
            for limit in limits {
                for stop in [None, Some(3)] {
                    for rev in [false, true] {
                        let got = run(r, Keyspace::Pending, from, prefix, limit, rev, stop);
                        let want = reference(model, from, prefix, limit, rev, stop);
                        if got != want {
                            *shapes
                                .entry((rev, prefix.is_empty(), from.is_empty()))
                                .or_default() += 1;
                            if first.len() < 3 {
                                first.push(format!(
                                    "from={from:?} prefix={prefix:?} limit={limit} rev={rev} \
                                     stop={stop:?}: got {} rows {:?}…, the contract says {} \
                                     rows {:?}…",
                                    got.0,
                                    head(&got.1),
                                    want.0,
                                    head(&want.1)
                                ));
                            }
                        }
                        compared += 1;
                        if !want.1.is_empty() {
                            nonempty += 1;
                        }
                    }
                }
            }
        }
    }
    assert!(
        shapes.is_empty(),
        "{what}: {} of {compared} scans break the scan contract; by (rev, empty prefix, \
         empty from): {shapes:?}; first cases: {first:#?}",
        shapes.values().sum::<u64>()
    );
    assert!(
        compared > 3_000 && nonempty > 1_000,
        "{compared} {nonempty}"
    );
}

#[test]
fn ram_scans_match_the_reference_walk_on_every_bound_limit_and_direction() {
    // There is no LMDB-direct keyspace left to compare the RAM walk with, so it
    // is compared with the CONTRACT itself ([`reference`]).
    let mut t = Tmp::new();
    let rows = scan_fixture();
    let model: BTreeMap<Vec<u8>, Vec<u8>> = rows.iter().cloned().collect();
    {
        let s = t.s();
        let mut w = s.write().unwrap();
        for (k, v) in &rows {
            w.put_raw(Keyspace::Pending, k, v).unwrap();
        }
        // The apply thread's own view: the live table, nothing committed.
        compare_all(&w, &model, "write handle, uncommitted");
        // A full walk really is the whole fixture, in memcmp order.
        let (n, all) = run(&w, Keyspace::Pending, &[], &[], usize::MAX, false, None);
        let mut sorted = rows.clone();
        sorted.sort();
        assert_eq!((n, all), (sorted.len(), sorted));

        w.durable_commit().unwrap();
        drop(w);
        s.read(|r| {
            compare_all(r, &model, "read handle, live");
            Ok(())
        })
        .unwrap();
    }
    // The table a reopen loads from the checkpoint walks the same.
    t.reopen();
    let s = t.s();
    s.read(|r| {
        compare_all(r, &model, "read handle, reloaded from the checkpoint");
        Ok(())
    })
    .unwrap();

    // An over-long `from` is refused in both directions.
    let long = vec![b'x'; s.max_key_len() + 1];
    s.read(|r| {
        let a = r.scan_raw(Keyspace::Pending, &long, &[], 1, &mut |_, _| true);
        let b = r.scan_rev_raw(Keyspace::Pending, &long, &[], 1, &mut |_, _| true);
        assert!(matches!(a, Err(StoreError::KeyTooLong { .. })));
        assert!(matches!(b, Err(StoreError::KeyTooLong { .. })));
        Ok(())
    })
    .unwrap();
}

// ---------------------------------------------------------------------------
// (f) the arena
// ---------------------------------------------------------------------------

#[test]
fn the_write_handle_s_get_arena_is_released_by_every_mutating_call() {
    let t = Tmp::new();
    let s = t.s();
    let mut w = s.write().unwrap();
    w.put_raw(Keyspace::Counters, b"k", &[7u8; 64]).unwrap();
    assert_eq!(w.arena_len(), 0);
    for _ in 0..10_000 {
        assert_eq!(
            w.get_raw(Keyspace::Counters, b"k").unwrap().unwrap().len(),
            64
        );
    }
    // A miss holds nothing, in any keyspace.
    assert!(w.get_raw(Keyspace::Counters, b"missing").unwrap().is_none());
    assert!(w.get_raw(Keyspace::Dedup, b"k").unwrap().is_none());
    assert_eq!(w.arena_len(), 10_000);
    w.put_raw(Keyspace::Counters, b"k", &[8u8; 64]).unwrap();
    assert_eq!(w.arena_len(), 0, "a put releases every value it can");

    // The apply loop's shape — reads, then a write — never accumulates.
    let mut high = 0;
    for round in 0..200u32 {
        for _ in 0..500 {
            w.get_raw(Keyspace::Counters, b"k").unwrap();
            high = high.max(w.arena_len());
        }
        match round % 5 {
            0 => w
                .put_raw(Keyspace::Counters, b"k", &round.to_le_bytes())
                .unwrap(),
            1 => {
                w.del_raw(Keyspace::Counters, b"gone").unwrap();
            }
            2 => w.commit().unwrap(),
            3 => w.abort().unwrap(),
            _ => {
                w.delete_range(Keyspace::Pending, &[], b"none", 8).unwrap();
            }
        }
        assert_eq!(w.arena_len(), 0, "round {round}");
    }
    assert_eq!(high, 500);
    w.durable_commit().unwrap();
    assert_eq!(w.arena_len(), 0);
}

// ---------------------------------------------------------------------------
// Concurrency: live readers against the writer
// ---------------------------------------------------------------------------

/// Readers that scan a RAM keyspace and, from inside the callback, read the
/// SAME keyspace again — the shape a recursive read lock deadlocks on behind a
/// waiting writer — while the apply thread writes it continuously. Every key a
/// reader sees is a key some writer state held, each at most once and in
/// order, and nobody hangs.
#[test]
fn live_readers_reentering_a_scan_never_block_the_writer() {
    let t = Tmp::new();
    let s = t.s();
    let stop = AtomicBool::new(false);
    std::thread::scope(|sc| {
        let mut readers = Vec::new();
        for rev in [false, true] {
            let stop = &stop;
            readers.push(sc.spawn(move || {
                let mut scans = 0u64;
                loop {
                    s.read(|r| {
                        let mut prev: Option<Vec<u8>> = None;
                        let mut cb = |k: &[u8], v: &[u8]| {
                            if let Some(p) = &prev {
                                if rev {
                                    assert!(k < p.as_slice(), "reverse walk out of order");
                                } else {
                                    assert!(k > p.as_slice(), "walk out of order");
                                }
                            }
                            prev = Some(k.to_vec());
                            assert_eq!(v.len(), 8);
                            // Re-enter the same keyspace mid-walk.
                            let again = r.get_raw(Keyspace::Pending, k).unwrap();
                            if let Some(a) = again {
                                assert_eq!(a.len(), 8);
                            }
                            true
                        };
                        if rev {
                            r.scan_rev_raw(Keyspace::Pending, &[], b"p", usize::MAX, &mut cb)?;
                        } else {
                            r.scan_raw(Keyspace::Pending, &[], b"p", usize::MAX, &mut cb)?;
                        }
                        Ok(())
                    })
                    .unwrap();
                    scans += 1;
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                }
                scans
            }));
        }

        let mut w = s.write().unwrap();
        for i in 0u64..30_000 {
            let k = [&b"p"[..], &(i % 1_500).to_be_bytes()[..]].concat();
            if i % 3 == 2 {
                w.del_raw(Keyspace::Pending, &k).unwrap();
            } else {
                w.put_raw(Keyspace::Pending, &k, &i.to_le_bytes()).unwrap();
            }
            if i % 4_096 == 0 {
                w.commit().unwrap();
            }
        }
        w.durable_commit().unwrap();
        drop(w);
        stop.store(true, Ordering::Relaxed);
        for h in readers {
            assert!(h.join().unwrap() > 0, "a reader never completed a scan");
        }
    });
}

// ---------------------------------------------------------------------------
// The async durable point: a cut taken by the writer, written on another
// thread (QUEEN_RAFT_CHECKPOINT_ASYNC)
// ---------------------------------------------------------------------------

#[test]
fn a_cut_is_written_on_another_thread_while_the_writer_carries_on() {
    let mut t = Tmp::new();
    {
        let s = t.s();
        let mut w = s.write().unwrap();
        assert!(w.can_cut(), "an all-RAM store cuts");
        w.put_raw(Keyspace::Queues, b"a", b"1").unwrap();
        w.put_raw(Keyspace::Queues, b"b", b"1").unwrap();
        w.put_raw(Keyspace::Cursors, b"c", b"1").unwrap();
        w.set_applied(7, 1).unwrap();
        let mut cut = w.take_cut().unwrap().expect("a cut");
        assert!(cut.keys() >= 4, "{} rows", cut.keys());
        for ks in Keyspace::ALL {
            assert_eq!(s.dirty_len(ks), 0, "{}: the cut took the dirty set", ks.name());
        }

        // The writer carries on after the cut: an overwrite, a delete and a new
        // row. None of them is in the cut, all of them are dirty for the next.
        w.put_raw(Keyspace::Queues, b"a", b"2").unwrap();
        w.del_raw(Keyspace::Queues, b"b").unwrap();
        w.put_raw(Keyspace::Queues, b"z", b"new").unwrap();
        w.set_applied(9, 1).unwrap();
        assert_eq!(s.dirty_len(Keyspace::Queues), 3);

        // Written on another thread WHILE the write handle is out: the handle
        // holds no LMDB transaction, so the checkpoint never waits for it.
        let (tx, rx) = mpsc::channel();
        std::thread::scope(|sc| {
            sc.spawn(|| {
                tx.send(s.write_cut(&mut cut)).unwrap();
            });
            let r = rx
                .recv_timeout(std::time::Duration::from_secs(20))
                .expect("the cut was written while the writer held its handle");
            r.expect("write the cut");
        });
        assert_eq!(StoreMetrics::get(&s.metrics().durable_commits), 1);
        assert_eq!(checkpoint(s, Keyspace::Queues, b"a").as_deref(), Some(&b"1"[..]));
        assert_eq!(checkpoint(s, Keyspace::Queues, b"b").as_deref(), Some(&b"1"[..]));
        assert_eq!(checkpoint(s, Keyspace::Queues, b"z"), None);
        // The live tables have the writer's newer values.
        w.put_raw(Keyspace::Groups, b"g", b"1").unwrap();
        assert_eq!(w.get_raw(Keyspace::Queues, b"a").unwrap(), Some(&b"2"[..]));
        assert_eq!(w.get_raw(Keyspace::Queues, b"b").unwrap(), None);
    }
    // A crash after the cut: the store reopens exactly at it.
    t.reopen();
    t.s()
        .read(|r| {
            assert_eq!(r.get_raw(Keyspace::Queues, b"a")?, Some(&b"1"[..]));
            assert_eq!(r.get_raw(Keyspace::Queues, b"b")?, Some(&b"1"[..]));
            assert_eq!(r.get_raw(Keyspace::Queues, b"z")?, None);
            assert_eq!(r.get_raw(Keyspace::Groups, b"g")?, None);
            assert_eq!(r.applied_index()?, 7);
            Ok(())
        })
        .unwrap();
}

#[test]
fn a_cut_that_is_not_written_goes_back_to_the_dirty_sets() {
    let t = Tmp::new();
    let s = t.s();
    let mut w = s.write().unwrap();
    w.put_raw(Keyspace::Partitions, b"p", b"1").unwrap();
    w.set_applied(3, 1).unwrap();
    let mut cut = w.take_cut().unwrap().expect("a cut");
    s.fail_next_sync();
    let e = s.write_cut(&mut cut).unwrap_err();
    assert!(e.lost_durable_point(), "{e}");
    assert!(matches!(e, StoreError::CommitFailed { durable: true, .. }));
    s.restore_cut(cut);
    assert_eq!(s.dirty_len(Keyspace::Partitions), 1);
    assert!(s.dirty_len(Keyspace::Meta) >= 1);
    // The handle is unaffected (the failure is the checkpoint thread's to
    // report), and the next point writes what the failed one could not.
    w.durable_commit().unwrap();
    drop(w);
    assert_eq!(s.dirty_len(Keyspace::Partitions), 0);
    assert_eq!(
        checkpoint(s, Keyspace::Partitions, b"p").as_deref(),
        Some(&b"1"[..])
    );
}

#[test]
fn a_store_that_syncs_every_commit_does_not_cut() {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-ram-sync-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let s = HeedStore::open(
        &dir,
        &StoreOpts {
            sync_every_commit: true,
            ..opts()
        },
    )
    .expect("open");
    {
        let mut w = s.write().unwrap();
        assert!(!w.can_cut());
        w.put_raw(Keyspace::Queues, b"q", b"1").unwrap();
        assert!(w.take_cut().unwrap().is_none());
        // Every commit is the durable one on this store.
        w.commit().unwrap();
        assert_eq!(s.dirty_len(Keyspace::Queues), 0);
    }
    s.close();
    let _ = std::fs::remove_dir_all(&dir);
}
