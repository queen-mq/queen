//! Format 1: value integrity ([`super::integrity`]).
//!
//! What is checked: a new store is format 1 and every stored value — in RAM, in
//! the checkpoint — is `value ‖ checksum` while every caller sees the logical
//! bytes; the checksum is pinned (a change is a FORMAT change); a byte flipped
//! inside `data.mdb` is refused at the reopen and found by the scrub, naming the
//! keyspace and the key; a value under the wrong key or keyspace fails like a
//! flipped bit; a corrupt value found at runtime poisons THIS store only (every
//! later call refused, the hook called once, no checkpoint written past it); the
//! format row is invisible to and refused by the keyspace API; a format-0 store
//! opens unverified with migration off, is migrated once with it on, and a store
//! that lost its format row or carries a newer format is refused; the digest is
//! the same over both formats; a snapshot copy carries its format, reopens, and
//! is verified before it leaves; the incremental scrub walks a whole pass in
//! bounded steps.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use heed::types::Bytes;
use heed::{Database, EnvFlags, EnvOpenOptions};

use super::integrity::{self, CorruptHook, ScrubCursor, StoreFormat, CHECKSUM_LEN, FORMAT_KEY};
use super::keys::{self, Counter};
use super::{
    heed_store, HeedStore, Keyspace, Reads, Store, StoreError, StoreMetrics, StoreOpts, TypedReads,
    TypedWrites, Writes,
};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn tmp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-integrity-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn opts() -> StoreOpts {
    StoreOpts {
        map_bytes: Some(64 << 20),
        ..Default::default()
    }
}

fn legacy_opts() -> StoreOpts {
    StoreOpts {
        create_legacy: true,
        migrate_legacy: false,
        ..opts()
    }
}

/// A store directory, removed on drop; the store itself is opened and closed
/// by the test (a reopen is the point of most of them).
struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Dir {
        Dir(tmp_dir(tag))
    }

    fn open(&self, o: &StoreOpts) -> HeedStore {
        HeedStore::open(&self.0, o).expect("open")
    }

    fn try_open(&self, o: &StoreOpts) -> Result<HeedStore, StoreError> {
        HeedStore::open(&self.0, o)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A value nobody else writes, so it can be found inside `data.mdb`.
fn canary(n: u8) -> Vec<u8> {
    let mut v = b"QUEEN-INTEGRITY-CANARY-".to_vec();
    v.push(b'A' + n);
    v.extend_from_slice(b"-0123456789");
    v
}

/// Flip one bit of the byte `at` bytes into EVERY copy of `pattern` in the
/// store file (an older copy may sit in a freed page), as a disk would.
/// Returns how many copies it hit.
fn flip_in_file(dir: &Path, pattern: &[u8], at: usize) -> usize {
    let path = dir.join("data.mdb");
    let mut bytes = std::fs::read(&path).expect("read data.mdb");
    let mut hits = Vec::new();
    let mut i = 0;
    while i + pattern.len() <= bytes.len() {
        if &bytes[i..i + pattern.len()] == pattern {
            hits.push(i);
            i += pattern.len();
        } else {
            i += 1;
        }
    }
    for &h in &hits {
        bytes[h + at] ^= 0x01;
    }
    // Written in place (not a new file): an environment that has the file
    // mapped sees the change, as it would see a disk's.
    use std::io::{Seek, SeekFrom, Write};
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open data.mdb for writing");
    for &h in &hits {
        f.seek(SeekFrom::Start((h + at) as u64)).unwrap();
        f.write_all(&[bytes[h + at]]).unwrap();
    }
    f.sync_all().unwrap();
    hits.len()
}

/// Raw access to a CLOSED store's file, as another tool (or a build before
/// format 1) would have it.
fn with_raw_meta(dir: &Path, f: impl FnOnce(&mut heed::RwTxn<'_>, Database<Bytes, Bytes>)) {
    let mut o = EnvOpenOptions::new();
    o.map_size(64 << 20);
    o.max_dbs(super::MAX_DBS);
    unsafe { o.flags(EnvFlags::empty()) };
    let env = unsafe { o.open(dir) }.expect("raw open");
    {
        let mut w = env.write_txn().unwrap();
        let meta: Database<Bytes, Bytes> = env
            .open_database(&w, Some(Keyspace::Meta.name()))
            .unwrap()
            .expect("meta exists");
        f(&mut w, meta);
        w.commit().unwrap();
    }
    env.prepare_for_closing().wait();
}

/// A few rows in several keyspaces, meta included, typed and raw.
fn fill(s: &HeedStore) {
    let mut w = s.write().unwrap();
    w.set_applied(42, 3).unwrap();
    w.set_meta_u64(super::meta::NEXT_PID, 9).unwrap();
    w.put_raw(Keyspace::Queues, b"q-canary", &canary(0))
        .unwrap();
    w.put_raw(Keyspace::Cursors, b"c-canary", &canary(1))
        .unwrap();
    w.put_raw(Keyspace::Kv, b"kv-canary", &canary(2)).unwrap();
    w.put_raw(Keyspace::QueuePartitions, b"unit-row", &[])
        .unwrap();
    w.add_counter(&keys::counter_partition(1, Counter::Pushed), 7)
        .unwrap();
    w.put_pending("t", "q", "g", 1, 5_000).unwrap();
    w.durable_commit().unwrap();
}

fn assert_filled(s: &HeedStore) {
    s.read(|r| {
        assert_eq!(r.applied_index()?, 42);
        assert_eq!(r.applied_term()?, 3);
        assert_eq!(r.next_pid()?, 9);
        assert_eq!(
            r.get_raw(Keyspace::Queues, b"q-canary")?,
            Some(&canary(0)[..])
        );
        assert_eq!(
            r.get_raw(Keyspace::Cursors, b"c-canary")?,
            Some(&canary(1)[..])
        );
        assert_eq!(r.get_raw(Keyspace::Kv, b"kv-canary")?, Some(&canary(2)[..]));
        assert_eq!(
            r.get_raw(Keyspace::QueuePartitions, b"unit-row")?,
            Some(&[][..])
        );
        assert_eq!(r.partition_counter(1, Counter::Pushed)?, 7);
        assert_eq!(r.pending_at("t", "q", "g", 1)?, Some(5_000));
        Ok(())
    })
    .unwrap();
}

fn corrupt_value_of(e: &StoreError) -> (&'static str, Vec<u8>) {
    match e {
        StoreError::CorruptValue { keyspace, key, .. } => (keyspace, key.clone()),
        other => panic!("expected CorruptValue, got {other}"),
    }
}

// ---------------------------------------------------------------------------
// The format and the checksum
// ---------------------------------------------------------------------------

#[test]
fn a_new_store_is_format_1_and_every_stored_value_carries_its_checksum() {
    let d = Dir::new("new");
    let s = d.open(&opts());
    assert_eq!(s.format(), StoreFormat::V1);
    // Every keyspace, meta included: the RAM table holds the sealed bytes, the
    // checkpoint receives them unchanged, and every caller reads the logical
    // value.
    {
        let mut w = s.write().unwrap();
        for ks in Keyspace::ALL {
            w.put_raw(ks, b"row", ks.name().as_bytes()).unwrap();
        }
        w.durable_commit().unwrap();
    }
    for ks in Keyspace::ALL {
        let seed = integrity::keyspace_seed(ks);
        let logical = ks.name().as_bytes();
        let sealed = integrity::seal_vec(seed, b"row", logical);
        assert_eq!(sealed.len(), logical.len() + CHECKSUM_LEN);
        assert_eq!(
            s.ram_get_stored(ks, b"row").as_deref(),
            Some(&sealed[..]),
            "{}",
            ks.name()
        );
        let stored = std::thread::scope(|sc| {
            sc.spawn(|| s.checkpoint_get_stored(ks, b"row").unwrap())
                .join()
                .unwrap()
        });
        assert_eq!(
            stored.as_deref(),
            Some(&sealed[..]),
            "{} checkpoint",
            ks.name()
        );
    }
    s.read(|r| {
        for ks in Keyspace::ALL {
            assert_eq!(r.get_raw(ks, b"row")?, Some(ks.name().as_bytes()));
            let mut seen = Vec::new();
            r.scan_raw(ks, &[], &[], usize::MAX, &mut |k, v| {
                seen.push((k.to_vec(), v.to_vec()));
                true
            })?;
            assert_eq!(seen, vec![(b"row".to_vec(), ks.name().as_bytes().to_vec())]);
            let mut rev = Vec::new();
            r.scan_rev_raw(ks, &[], &[], usize::MAX, &mut |_k, v| {
                rev.push(v.to_vec());
                true
            })?;
            assert_eq!(rev, vec![ks.name().as_bytes().to_vec()]);
        }
        Ok(())
    })
    .unwrap();
    // The logical-bytes metric is the logical size, not the stored one.
    let logical: u64 = Keyspace::ALL
        .iter()
        .map(|ks| (3 + ks.name().len()) as u64)
        .sum();
    assert_eq!(StoreMetrics::get(&s.metrics().logical_bytes), logical);
    s.close();
    // It reopens as format 1, verified.
    let s = d.open(&opts());
    assert_eq!(s.format(), StoreFormat::V1);
    s.read(|r| {
        for ks in Keyspace::ALL {
            assert_eq!(r.get_raw(ks, b"row")?, Some(ks.name().as_bytes()));
        }
        Ok(())
    })
    .unwrap();
    s.close();
}

#[test]
fn the_checksum_is_pinned_and_covers_keyspace_key_and_value() {
    // PERMANENT: every format-1 store holds these checksums. A change here is a
    // FORMAT change (a new format number, and a reader for the old one).
    assert_eq!(
        integrity::keyspace_seed(Keyspace::Meta),
        xxhash_rust::xxh3::xxh3_64_with_seed(b"meta", 0x5155_4545_4E53_5631)
    );
    let seed = integrity::keyspace_seed(Keyspace::Cursors);
    let sum = integrity::checksum(seed, b"key", b"value");
    assert_eq!(
        sum,
        xxhash_rust::xxh3::xxh3_64_with_seed(
            b"value",
            xxhash_rust::xxh3::xxh3_64_with_seed(b"key", seed)
        )
    );
    // Cross-checked against the reference C implementation (xxHash 0.8.3, via
    // python-xxhash 4.0.1): the format is plain XXH3, so any tool can check a
    // store's values.
    assert_eq!(
        integrity::keyspace_seed(Keyspace::Meta),
        0x0b5a_59ee_b420_6db6
    );
    assert_eq!(seed, 0x984e_74dc_701c_25f4);
    assert_eq!(
        sum, 0x80af_1f75_452d_c9a4,
        "the format-1 checksum changed: {sum:#018x}"
    );
    // Every input moves it: the keyspace, the key, the value, and where the
    // key ends and the value begins.
    let other_ks = integrity::checksum(
        integrity::keyspace_seed(Keyspace::Pending),
        b"key",
        b"value",
    );
    assert_ne!(sum, other_ks);
    assert_ne!(sum, integrity::checksum(seed, b"kez", b"value"));
    assert_ne!(sum, integrity::checksum(seed, b"key", b"valuf"));
    assert_ne!(sum, integrity::checksum(seed, b"keyv", b"alue"));
    // The seed is the NAME's: every keyspace has its own.
    let mut seeds: Vec<u64> = Keyspace::ALL
        .iter()
        .map(|k| integrity::keyspace_seed(*k))
        .collect();
    seeds.sort_unstable();
    seeds.dedup();
    assert_eq!(seeds.len(), Keyspace::ALL.len());
}

// ---------------------------------------------------------------------------
// A flipped byte in the file
// ---------------------------------------------------------------------------

#[test]
fn a_byte_flipped_inside_data_mdb_is_refused_at_the_reopen_and_found_by_the_scrub() {
    let d = Dir::new("flip");
    let s = d.open(&opts());
    fill(&s);
    s.close();
    // One bit of one byte in the middle of the `cursors` value.
    assert!(
        flip_in_file(&d.0, &canary(1), 7) >= 1,
        "the canary is in the file"
    );

    // The reopen reads (loads) every row: it refuses, naming the row.
    let e = d
        .try_open(&opts())
        .err()
        .expect("a damaged store must not open");
    let (ks, key) = corrupt_value_of(&e);
    assert_eq!(ks, "cursors");
    assert_eq!(key, b"c-canary".to_vec());
    assert!(e.fatal() && !e.retryable() && e.corrupt_store(), "{e}");
    let msg = e.to_string();
    assert!(msg.contains("cursors") && msg.contains("c-canary"), "{msg}");
    assert!(msg.contains("checksum mismatch"), "{msg}");
    assert!(
        msg.contains("Restore this node") && msg.contains("rejoins from a peer"),
        "the operator is told what to do: {msg}"
    );

    // The scrub of the closed directory finds the same row.
    let report = heed_store::scrub_dir(&d.0).unwrap();
    assert_eq!(report.format, StoreFormat::V1);
    assert_eq!(report.corrupt, 1, "{report:?}");
    let first = report.first_corrupt.clone().unwrap();
    assert_eq!(corrupt_value_of(&first), ("cursors", b"c-canary".to_vec()));
    assert!(report.clone().into_result().is_err());

    // QUEEN_STORE_VERIFY=1 refuses the same way, before the load.
    let e = d
        .try_open(&StoreOpts {
            verify_at_open: true,
            ..opts()
        })
        .err()
        .expect("refused by the scrub at open");
    assert_eq!(corrupt_value_of(&e), ("cursors", b"c-canary".to_vec()));
    assert!(e.to_string().contains("by the scrub at open"), "{e}");

    // A second flip, in another keyspace: the scrub counts both and names the
    // first in walk order.
    assert!(flip_in_file(&d.0, &canary(0), 3) >= 1);
    let report = heed_store::scrub_dir(&d.0).unwrap();
    assert_eq!(report.corrupt, 2, "{report:?}");
    assert_eq!(
        corrupt_value_of(report.first_corrupt.as_ref().unwrap()).0,
        "queues",
        "queues comes before cursors in the walk"
    );
}

#[test]
fn a_flipped_key_byte_is_caught_too() {
    // The key is inside the checksum, so damage to the KEY bytes of a row is
    // refused like damage to its value.
    let d = Dir::new("keyflip");
    let s = d.open(&opts());
    {
        let mut w = s.write().unwrap();
        w.put_raw(Keyspace::Groups, b"KEY-CANARY-GROUP-0001", b"v")
            .unwrap();
        w.durable_commit().unwrap();
    }
    s.close();
    assert!(flip_in_file(&d.0, b"KEY-CANARY-GROUP-0001", 12) >= 1);
    let e = d.try_open(&opts()).err().expect("refused");
    let (ks, key) = corrupt_value_of(&e);
    assert_eq!(ks, "groups");
    assert_ne!(
        key,
        b"KEY-CANARY-GROUP-0001".to_vec(),
        "the key as the file now has it"
    );
    assert_eq!(key.len(), b"KEY-CANARY-GROUP-0001".len());
}

#[test]
fn a_flip_while_the_store_is_open_is_found_by_the_scrub_and_poisons_it() {
    // The background scrub's case: the file is damaged under a running node,
    // whose RAM is still right. The scrub reads the image (what the next boot
    // and a snapshot would read) and stops the node on the first bad row.
    let d = Dir::new("liveflip");
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let o = StoreOpts {
        on_corrupt: Some(CorruptHook::new(move |_e| {
            c.fetch_add(1, Ordering::SeqCst);
        })),
        ..opts()
    };
    let s = d.open(&o);
    fill(&s);
    let mut cur = ScrubCursor::default();
    // A clean pass first, in small steps.
    let mut steps = 0;
    while !s.scrub_step(&mut cur, 3).unwrap() {
        steps += 1;
        assert!(steps < 1000);
    }
    assert_eq!(cur.passes, 1);
    assert!(steps > 1, "the pass took several bounded steps");

    assert!(flip_in_file(&d.0, &canary(2), 5) >= 1);
    let mut found = None;
    for _ in 0..1000 {
        match s.scrub_step(&mut cur, 3) {
            Ok(_) => {}
            Err(e) => {
                found = Some(e);
                break;
            }
        }
    }
    let e = found.expect("the scrub found the flipped byte");
    assert_eq!(corrupt_value_of(&e), ("kv", b"kv-canary".to_vec()));
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the hook, once");
    assert!(s.poisoned().is_some());
    // …and the store now refuses to serve, with that first corruption.
    let refused = s.read(|_r| Ok(())).unwrap_err();
    assert_eq!(corrupt_value_of(&refused), ("kv", b"kv-canary".to_vec()));
    assert!(s.write().is_err());
    assert!(s.scrub().is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1, "still once");
    assert_eq!(StoreMetrics::get(&s.metrics().corrupt_values), 1);
    s.close();
}

// ---------------------------------------------------------------------------
// Runtime: a read that meets a corrupt value
// ---------------------------------------------------------------------------

#[test]
fn a_value_under_the_wrong_key_or_in_the_wrong_keyspace_fails_its_check() {
    let d = Dir::new("misplaced");
    let s = d.open(&opts());
    {
        let mut w = s.write().unwrap();
        w.put_raw(Keyspace::Queues, b"a", b"value-a").unwrap();
        w.put_raw(Keyspace::Queues, b"b", b"value-b").unwrap();
        w.put_raw(Keyspace::Groups, b"a", b"value-a").unwrap();
        w.commit().unwrap();
    }
    // `a`'s sealed bytes, moved under `b` (a damaged branch page, a misdirected
    // write): intact bytes, wrong place.
    let a = s.ram_get_stored(Keyspace::Queues, b"a").unwrap();
    s.ram_put_stored(Keyspace::Queues, b"b", &a);
    let e = s
        .read(|r| {
            r.get_raw(Keyspace::Queues, b"b")
                .map(|v| v.map(|v| v.to_vec()))
        })
        .unwrap_err();
    assert_eq!(corrupt_value_of(&e), ("queues", b"b".to_vec()));
    s.close();

    // The same bytes under the same key in ANOTHER keyspace fail as well.
    let d = Dir::new("misplaced-ks");
    let s = d.open(&opts());
    {
        let mut w = s.write().unwrap();
        w.put_raw(Keyspace::Queues, b"a", b"value-a").unwrap();
        w.put_raw(Keyspace::Groups, b"a", b"value-a").unwrap();
        w.commit().unwrap();
    }
    let q = s.ram_get_stored(Keyspace::Queues, b"a").unwrap();
    assert_ne!(q, s.ram_get_stored(Keyspace::Groups, b"a").unwrap());
    s.ram_put_stored(Keyspace::Groups, b"a", &q);
    let e = s
        .read(|r| r.scan_raw(Keyspace::Groups, &[], &[], usize::MAX, &mut |_k, _v| true))
        .unwrap_err();
    assert_eq!(corrupt_value_of(&e), ("groups", b"a".to_vec()));
    s.close();
}

#[test]
fn a_corrupt_value_at_runtime_poisons_this_store_only_and_writes_no_checkpoint_past_it() {
    let a_dir = Dir::new("node-a");
    let b_dir = Dir::new("node-b");
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let hooked = StoreOpts {
        on_corrupt: Some(CorruptHook::new(move |e| {
            assert!(e.corrupt_store());
            c.fetch_add(1, Ordering::SeqCst);
        })),
        ..opts()
    };
    let a = a_dir.open(&hooked);
    let b = b_dir.open(&opts());
    fill(&a);
    fill(&b);

    // Node A's RAM copy of one value is damaged (memory, a wild write): its
    // checksum no longer matches. The file still holds the good checkpoint.
    let mut stored = a.ram_get_stored(Keyspace::Cursors, b"c-canary").unwrap();
    stored[2] ^= 0x40;
    a.ram_put_stored(Keyspace::Cursors, b"c-canary", &stored);

    // Apply's handle meets it: a fatal error, which apply never turns into an
    // outcome (every store error poisons the applier).
    let mut w = a.write().unwrap();
    let e = w.get_raw(Keyspace::Cursors, b"c-canary").unwrap_err();
    assert!(e.fatal() && e.corrupt_store());
    assert_eq!(corrupt_value_of(&e), ("cursors", b"c-canary".to_vec()));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // From here on node A serves nothing: reads, writes, commits, the
    // checkpoint, a snapshot copy, the scrub — all refused with the first
    // corruption, and the hook is not called again.
    assert!(w.put_raw(Keyspace::Queues, b"x", b"y").is_err());
    assert!(w.get_raw(Keyspace::Queues, b"q-canary").is_err());
    assert!(
        w.durable_commit().is_err(),
        "no checkpoint past a known corruption"
    );
    assert!(w.take_cut().is_err());
    drop(w);
    let refused = a.read(|r| r.applied_index()).unwrap_err();
    assert_eq!(
        corrupt_value_of(&refused),
        ("cursors", b"c-canary".to_vec())
    );
    assert!(a.write().is_err());
    let copy = tmp_dir("poisoned-copy");
    assert!(
        a.copy_checkpoint(&copy).is_err(),
        "no snapshot from a poisoned store"
    );
    let _ = std::fs::remove_dir_all(&copy);
    assert!(a.scrub_step(&mut ScrubCursor::default(), 10).is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the hook runs once");
    assert_eq!(StoreMetrics::get(&a.metrics().corrupt_values), 1);

    // Node B, same state, is untouched: corruption is node-local.
    assert!(b.poisoned().is_none());
    assert_filled(&b);
    assert!(b.scrub().is_ok());

    // A restart of node A reloads its LAST GOOD checkpoint: the damage was in
    // RAM only, and nothing checkpointed it.
    a.close();
    let a = a_dir.open(&opts());
    assert!(a.poisoned().is_none());
    assert_filled(&a);
    a.close();
    b.close();
}

#[test]
fn the_scrub_verifies_the_ram_tables_as_well_as_the_image() {
    let d = Dir::new("ramscrub");
    let s = d.open(&opts());
    fill(&s);
    assert!(s.scrub().is_ok());
    let mut stored = s.ram_get_stored(Keyspace::Kv, b"kv-canary").unwrap();
    let last = stored.len() - 1;
    stored[last] ^= 0x01; // the checksum's own bytes
    s.ram_put_stored(Keyspace::Kv, b"kv-canary", &stored);
    let e = s.scrub().unwrap_err();
    assert_eq!(corrupt_value_of(&e), ("kv", b"kv-canary".to_vec()));
    assert!(e.to_string().contains("in RAM by the scrub"), "{e}");
    assert!(s.poisoned().is_some());
    s.close();
}

#[test]
fn a_value_shorter_than_its_checksum_is_corrupt() {
    let d = Dir::new("short");
    let s = d.open(&opts());
    {
        let mut w = s.write().unwrap();
        w.put_raw(Keyspace::Counters, b"k", &7i64.to_le_bytes())
            .unwrap();
        w.commit().unwrap();
    }
    s.ram_put_stored(Keyspace::Counters, b"k", &[1, 2, 3]);
    let e = s.read(|r| r.counter_at(b"k")).unwrap_err();
    assert!(
        e.to_string().contains("shorter than its 8 B checksum"),
        "{e}"
    );
    s.close();
}

// ---------------------------------------------------------------------------
// The format row
// ---------------------------------------------------------------------------

#[test]
fn the_format_row_is_not_a_row_of_the_meta_keyspace() {
    let d = Dir::new("formatrow");
    let s = d.open(&opts());
    fill(&s);
    s.read(|r| {
        assert_eq!(r.get_raw(Keyspace::Meta, FORMAT_KEY)?, None);
        let mut keys_seen = Vec::new();
        r.scan_raw(Keyspace::Meta, &[], &[], usize::MAX, &mut |k, _v| {
            keys_seen.push(k.to_vec());
            true
        })?;
        assert!(!keys_seen.iter().any(|k| k == FORMAT_KEY));
        assert_eq!(r.count(Keyspace::Meta)?, 3, "applied index, term, next pid");
        Ok(())
    })
    .unwrap();
    let mut w = s.write().unwrap();
    assert!(w
        .put_raw(Keyspace::Meta, FORMAT_KEY, &1u32.to_le_bytes())
        .is_err());
    assert!(w.del_raw(Keyspace::Meta, FORMAT_KEY).is_err());
    // A range delete over the whole of meta leaves it where it is.
    let (n, _) = w.delete_range(Keyspace::Meta, &[], &[], 100).unwrap();
    assert_eq!(n, 3);
    w.durable_commit().unwrap();
    drop(w);
    s.close();
    // Still format 1 after all of that.
    let s = d.open(&opts());
    assert_eq!(s.format(), StoreFormat::V1);
    s.close();
}

#[test]
fn a_store_that_lost_its_format_row_is_refused_not_migrated() {
    let d = Dir::new("lostrow");
    let s = d.open(&opts());
    fill(&s);
    s.close();
    let before = std::fs::read(d.0.join("data.mdb")).unwrap();
    with_raw_meta(&d.0, |w, meta| {
        assert!(meta.delete(w, FORMAT_KEY).unwrap());
    });
    let e = d.try_open(&opts()).err().expect("refused");
    assert!(e.corrupt_store(), "{e}");
    assert!(e.to_string().contains("format row was lost"), "{e}");
    // Refused before anything was re-sealed: the rows are the ones written.
    let mut o = opts();
    o.migrate_legacy = false;
    let e = d
        .try_open(&o)
        .err()
        .expect("refused without the migration too");
    assert!(e.to_string().contains("format row was lost"), "{e}");
    let after = std::fs::read(d.0.join("data.mdb")).unwrap();
    for n in 0..3 {
        let c = integrity::seal_vec(
            integrity::keyspace_seed(
                [Keyspace::Queues, Keyspace::Cursors, Keyspace::Kv][n as usize],
            ),
            [&b"q-canary"[..], b"c-canary", b"kv-canary"][n as usize],
            &canary(n),
        );
        let find = |b: &[u8]| b.windows(c.len()).any(|w| w == &c[..]);
        assert!(find(&before) && find(&after), "row {n} still sealed once");
    }
}

#[test]
fn a_newer_or_damaged_format_row_is_refused() {
    let d = Dir::new("newer");
    let s = d.open(&opts());
    fill(&s);
    s.close();
    let seed = integrity::keyspace_seed(Keyspace::Meta);
    with_raw_meta(&d.0, |w, meta| {
        let row = integrity::seal_vec(seed, FORMAT_KEY, &2u32.to_le_bytes());
        meta.put(w, FORMAT_KEY, &row).unwrap();
    });
    let e = d.try_open(&opts()).err().expect("refused");
    assert!(e.to_string().contains("newer build"), "{e}");
    assert!(!e.corrupt_store(), "a newer format is not damage");

    with_raw_meta(&d.0, |w, meta| {
        let mut row = integrity::seal_vec(seed, FORMAT_KEY, &1u32.to_le_bytes());
        row[0] ^= 0x02; // "3", under the checksum of "1"
        meta.put(w, FORMAT_KEY, &row).unwrap();
    });
    let e = d.try_open(&opts()).err().expect("refused");
    assert_eq!(corrupt_value_of(&e), ("meta", FORMAT_KEY.to_vec()));
}

// ---------------------------------------------------------------------------
// Format 0 (legacy)
// ---------------------------------------------------------------------------

/// A format-0 store with the `fill` rows, written as a build before format 1
/// wrote it: plain values, no format row.
fn legacy_store(tag: &str) -> Dir {
    let d = Dir::new(tag);
    let s = d.open(&legacy_opts());
    assert_eq!(s.format(), StoreFormat::Legacy);
    fill(&s);
    // Plain bytes, in RAM and in the file.
    assert_eq!(
        s.ram_get_stored(Keyspace::Cursors, b"c-canary"),
        Some(canary(1))
    );
    s.close();
    let raw = std::fs::read(d.0.join("data.mdb")).unwrap();
    assert!(
        !raw.windows(FORMAT_KEY.len()).any(|w| w == FORMAT_KEY),
        "a format-0 file has no format row"
    );
    d
}

#[test]
fn an_unchecksummed_store_opens_and_works_unverified_with_migration_off() {
    let d = legacy_store("legacy");
    let o = StoreOpts {
        migrate_legacy: false,
        ..opts()
    };
    let s = d.open(&o);
    assert_eq!(s.format(), StoreFormat::Legacy);
    assert_filled(&s);
    // It keeps working: writes stay plain, a reopen keeps the format.
    {
        let mut w = s.write().unwrap();
        w.put_raw(Keyspace::Queues, b"later", b"plain").unwrap();
        w.durable_commit().unwrap();
    }
    assert_eq!(
        s.ram_get_stored(Keyspace::Queues, b"later"),
        Some(b"plain".to_vec())
    );
    // Its scrub walks the order and the counts; there is no checksum to check.
    let rep = s.scrub().unwrap();
    assert_eq!(rep.format, StoreFormat::Legacy);
    assert!(rep.rows >= 8);
    let mut cur = ScrubCursor::default();
    while !s.scrub_step(&mut cur, 4).unwrap() {}
    s.close();
    let s = d.open(&o);
    assert_eq!(s.format(), StoreFormat::Legacy);
    assert_filled(&s);
    s.read(|r| {
        assert_eq!(r.get_raw(Keyspace::Queues, b"later")?, Some(&b"plain"[..]));
        Ok(())
    })
    .unwrap();
    // Unverified means a damaged byte is served as it is: the reason format
    // 0 is migrated by default.
    s.close();
    assert!(flip_in_file(&d.0, &canary(1), 7) >= 1);
    let s = d.open(&o);
    let got = s
        .read(|r| {
            Ok(r.get_raw(Keyspace::Cursors, b"c-canary")?
                .map(|v| v.to_vec()))
        })
        .unwrap()
        .unwrap();
    assert_ne!(got, canary(1));
    s.close();
}

#[test]
fn a_legacy_store_is_migrated_once_atomically_and_then_verified() {
    let d = legacy_store("migrate");
    let s = d.open(&opts());
    assert_eq!(s.format(), StoreFormat::V1, "migrated at open");
    assert_filled(&s);
    let sealed = integrity::seal_vec(
        integrity::keyspace_seed(Keyspace::Cursors),
        b"c-canary",
        &canary(1),
    );
    assert_eq!(
        s.ram_get_stored(Keyspace::Cursors, b"c-canary"),
        Some(sealed.clone())
    );
    let stored = std::thread::scope(|sc| {
        sc.spawn(|| {
            s.checkpoint_get_stored(Keyspace::Cursors, b"c-canary")
                .unwrap()
        })
        .join()
        .unwrap()
    });
    assert_eq!(
        stored,
        Some(sealed.clone()),
        "the file was rewritten sealed"
    );
    s.close();
    // Once: a second open finds format 1 and changes nothing.
    let before = std::fs::read(d.0.join("data.mdb")).unwrap();
    let s = d.open(&opts());
    assert_eq!(s.format(), StoreFormat::V1);
    assert_filled(&s);
    s.close();
    let after = std::fs::read(d.0.join("data.mdb")).unwrap();
    let find = |b: &[u8]| {
        b.windows(sealed.len())
            .filter(|w| *w == &sealed[..])
            .count()
    };
    assert!(find(&before) >= 1 && find(&after) >= 1);
    // And from now on a flipped byte is caught.
    assert!(flip_in_file(&d.0, &canary(1), 7) >= 1);
    assert!(d.try_open(&opts()).err().unwrap().corrupt_store());
}

#[test]
fn a_migration_that_does_not_commit_leaves_a_working_legacy_store() {
    let d = legacy_store("migrate-fail");
    let before = std::fs::read(d.0.join("data.mdb")).unwrap();
    // Every row is rewritten inside the migration's transaction, which then
    // fails before its commit: nothing of it may land, and the store opens as
    // the format-0 store it was — unverified, and working.
    let s = d.open(&StoreOpts {
        fail_migration: true,
        ..opts()
    });
    assert_eq!(s.format(), StoreFormat::Legacy);
    assert_filled(&s);
    assert_eq!(
        s.ram_get_stored(Keyspace::Cursors, b"c-canary"),
        Some(canary(1))
    );
    s.close();
    let after = std::fs::read(d.0.join("data.mdb")).unwrap();
    let plain = canary(1);
    let count = |b: &[u8]| b.windows(plain.len()).filter(|w| *w == &plain[..]).count();
    assert!(count(&after) >= 1);
    assert!(
        !after.windows(FORMAT_KEY.len()).any(|w| w == FORMAT_KEY),
        "no format row landed"
    );
    assert_eq!(count(&before), count(&after));
    // The next open (migration working) migrates it.
    let s = d.open(&opts());
    assert_eq!(s.format(), StoreFormat::V1);
    assert_filled(&s);
    s.close();
}

// ---------------------------------------------------------------------------
// The digest and the snapshot copy
// ---------------------------------------------------------------------------

fn digests(
    s: &HeedStore,
) -> (
    crate::rsm::apply::StateDigest,
    crate::rsm::apply::StateDigest,
) {
    s.read(|r| {
        Ok((
            crate::rsm::apply::state_digest(r)?,
            crate::rsm::apply::local_digest(r)?,
        ))
    })
    .unwrap()
}

#[test]
fn the_digest_is_over_logical_values_and_equal_across_formats() {
    let v1 = Dir::new("digest-v1");
    let v0 = Dir::new("digest-v0");
    let s1 = v1.open(&opts());
    let s0 = v0.open(&legacy_opts());
    for s in [&s1, &s0] {
        fill(s);
        let mut w = s.write().unwrap();
        w.put_raw(Keyspace::SegLoc, b"local", b"file-7").unwrap();
        w.put_raw(Keyspace::Dlq, &[0u8; 40], &[0xEE; 300]).unwrap();
        w.durable_commit().unwrap();
    }
    let (d1, l1) = digests(&s1);
    let (d0, l0) = digests(&s0);
    assert_eq!(d1, d0, "state digest, format 1 vs format 0");
    assert_eq!(l1, l0, "local digest, format 1 vs format 0");
    s1.close();
    s0.close();
    // The migrated store digests as it did before the migration.
    let s0 = v0.open(&opts());
    assert_eq!(s0.format(), StoreFormat::V1);
    assert_eq!(digests(&s0), (d0, l0));
    s0.close();
}

#[test]
fn a_snapshot_copy_carries_its_format_reopens_and_is_verified_before_it_leaves() {
    for (tag, legacy) in [("snap-v1", false), ("snap-v0", true)] {
        let d = if legacy {
            legacy_store(tag)
        } else {
            Dir::new(tag)
        };
        let s = if legacy {
            d.open(&StoreOpts {
                migrate_legacy: false,
                ..opts()
            })
        } else {
            let s = d.open(&opts());
            fill(&s);
            s
        };
        let want = digests(&s);
        let copy = Dir::new(&format!("{tag}-copy"));
        let (applied, term) = s.copy_checkpoint(&copy.0).unwrap();
        assert_eq!((applied, term), (42, 3), "{tag}");
        assert_eq!(heed_store::read_checkpoint_meta(&copy.0).unwrap(), (42, 3));
        let rep = heed_store::scrub_dir(&copy.0).unwrap();
        assert_eq!(
            rep.format,
            s.format(),
            "{tag}: the copy is in the sender's format"
        );
        assert_eq!(rep.corrupt, 0);
        s.close();

        // The receiver boots from it whichever format it carries: format 1 is
        // verified at the load, format 0 is migrated there (or kept, with
        // migration off).
        let r = copy.open(&opts());
        assert_eq!(r.format(), StoreFormat::V1, "{tag}");
        assert_eq!(digests(&r), want, "{tag}");
        r.close();
        if legacy {
            let copy2 = Dir::new(&format!("{tag}-copy2"));
            let s = d.open(&StoreOpts {
                migrate_legacy: false,
                ..opts()
            });
            s.copy_checkpoint(&copy2.0).unwrap();
            s.close();
            let r = copy2.open(&StoreOpts {
                migrate_legacy: false,
                ..opts()
            });
            assert_eq!(r.format(), StoreFormat::Legacy);
            assert_eq!(digests(&r), want);
            r.close();
        }
    }
}

#[test]
fn a_damaged_image_is_not_copied_into_a_snapshot() {
    let d = Dir::new("snap-damaged");
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let s = d.open(&StoreOpts {
        on_corrupt: Some(CorruptHook::new(move |_| {
            c.fetch_add(1, Ordering::SeqCst);
        })),
        ..opts()
    });
    fill(&s);
    // The file is damaged under the running node (its RAM is fine): the copy
    // a follower would boot from is verified before it leaves, and refused.
    assert!(flip_in_file(&d.0, &canary(0), 9) >= 1);
    let copy = Dir::new("snap-damaged-copy");
    let e = s.copy_checkpoint(&copy.0).unwrap_err();
    assert_eq!(corrupt_value_of(&e), ("queues", b"q-canary".to_vec()));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(s.poisoned().is_some());
    s.close();
}

#[test]
fn read_checkpoint_meta_reads_either_format_and_verifies_format_1() {
    let d = Dir::new("ckptmeta");
    let s = d.open(&opts());
    fill(&s);
    s.close();
    assert_eq!(heed_store::read_checkpoint_meta(&d.0).unwrap(), (42, 3));
    let l = legacy_store("ckptmeta-v0");
    assert_eq!(heed_store::read_checkpoint_meta(&l.0).unwrap(), (42, 3));
    // A damaged applied index is refused, never read as a number.
    let seed = integrity::keyspace_seed(Keyspace::Meta);
    with_raw_meta(&d.0, |w, meta| {
        let mut row = integrity::seal_vec(seed, super::meta::APPLIED_INDEX, &42u64.to_le_bytes());
        row[0] ^= 0x80;
        meta.put(w, super::meta::APPLIED_INDEX, &row).unwrap();
    });
    let e = heed_store::read_checkpoint_meta(&d.0).unwrap_err();
    assert_eq!(
        corrupt_value_of(&e),
        ("meta", super::meta::APPLIED_INDEX.to_vec())
    );
}

// ---------------------------------------------------------------------------
// The incremental scrub
// ---------------------------------------------------------------------------

#[test]
fn the_incremental_scrub_walks_a_whole_pass_in_bounded_steps() {
    let d = Dir::new("steps");
    let s = d.open(&opts());
    {
        let mut w = s.write().unwrap();
        for i in 0..500u32 {
            w.put_raw(Keyspace::Pending, &i.to_be_bytes(), &[7u8; 20])
                .unwrap();
            w.put_raw(Keyspace::Cursors, &i.to_be_bytes(), &[9u8; 40])
                .unwrap();
        }
        w.set_applied(1, 1).unwrap();
        w.durable_commit().unwrap();
        // Rows only in RAM (dirty): the RAM half of the pass sees them.
        for i in 500..600u32 {
            w.put_raw(Keyspace::Pending, &i.to_be_bytes(), &[7u8; 20])
                .unwrap();
        }
    }
    let image_rows = 1000 + 2 + 1; // pending, cursors, meta's two, the format row
    let ram_rows = 1100 + 2;
    let mut cur = ScrubCursor::default();
    let mut steps = 0u64;
    let before = StoreMetrics::get(&s.metrics().scrubbed_rows);
    loop {
        let rows_before = cur.rows;
        let done = s.scrub_step(&mut cur, 64).unwrap();
        steps += 1;
        if done {
            break;
        }
        assert!(
            cur.rows - rows_before <= 64,
            "a step is bounded by its budget"
        );
    }
    assert_eq!(cur.passes, 1);
    assert_eq!(
        StoreMetrics::get(&s.metrics().scrubbed_rows) - before,
        (image_rows + ram_rows) as u64
    );
    assert!(steps >= ((image_rows + ram_rows) / 64) as u64);
    // The next pass starts over and finds the same.
    let before = StoreMetrics::get(&s.metrics().scrubbed_rows);
    while !s.scrub_step(&mut cur, 1000).unwrap() {}
    assert_eq!(cur.passes, 2);
    assert_eq!(
        StoreMetrics::get(&s.metrics().scrubbed_rows) - before,
        (image_rows + ram_rows) as u64
    );
    // The full scrub agrees about the image.
    let rep = s.scrub().unwrap();
    assert_eq!(rep.rows, image_rows as u64);
    assert_eq!(rep.corrupt, 0);
    s.close();
}

#[test]
fn a_corrupt_value_error_is_fatal_named_and_carries_the_restore_hint() {
    let e = StoreError::corrupt_value(Keyspace::Pending, b"t\x00\x00q\x00\x00", "x");
    assert!(e.fatal());
    assert!(!e.retryable());
    assert!(!e.lost_durable_point());
    assert!(e.corrupt_store());
    let msg = e.to_string();
    assert!(msg.contains("`pending`"), "{msg}");
    assert!(msg.contains("0x7400007100"), "{msg}");
    assert!(msg.contains(integrity::RESTORE_HINT), "{msg}");
    // The planner refuses on it (retryable 5xx, nothing logged), it does not
    // turn it into anything it proposes.
    let refusal = crate::rsm::planner::Refusal::from_store(e);
    assert!(refusal.retryable);
    // A decode failure stays the other, older error.
    assert!(!StoreError::corrupt(Keyspace::Queues, "x").corrupt_store());
}
