//! The scheduled scrub ([`crate::rsm::scrub`]): it finds a row damaged in the
//! file under a running store (poison, the hook once, then it stops), it runs
//! pass after pass over a healthy store, it ends with the store, and its knobs
//! parse as documented.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::rsm::scrub::{self, Config};
use crate::rsm::store::integrity::CorruptHook;
use crate::rsm::store::{HeedStore, Keyspace, Store, StoreMetrics, StoreOpts, TypedWrites, Writes};

fn opts() -> StoreOpts {
    StoreOpts {
        map_bytes: Some(64 << 20),
        ..Default::default()
    }
}

static SEQ: AtomicU64 = AtomicU64::new(0);

/// A store directory, removed on drop.
struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Dir {
        let d = std::env::temp_dir().join(format!(
            "queen-rsm-scrub-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&d);
        Dir(d)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

const CANARY: &[u8] = b"QUEEN-SCRUB-CANARY-0123456789";

/// `n` rows in `cursors`, the canary among them, in the checkpoint.
fn fill(s: &HeedStore, n: u32) {
    let mut w = s.write().unwrap();
    for i in 0..n {
        w.put_raw(Keyspace::Cursors, &i.to_be_bytes(), &[5u8; 32])
            .unwrap();
    }
    w.put_raw(Keyspace::Cursors, b"c-canary", CANARY).unwrap();
    w.set_applied(1, 1).unwrap();
    w.durable_commit().unwrap();
}

/// Flip one bit of every copy of `pattern` in `data.mdb`, in place, as a disk
/// would under the open environment. Returns the copies hit.
fn flip_in_file(dir: &Path, pattern: &[u8], at: usize) -> usize {
    use std::io::{Seek, SeekFrom, Write};
    let path = dir.join("data.mdb");
    let bytes = std::fs::read(&path).unwrap();
    let hits: Vec<usize> = (0..=bytes.len() - pattern.len())
        .filter(|&i| &bytes[i..i + pattern.len()] == pattern)
        .collect();
    let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    for &h in &hits {
        f.seek(SeekFrom::Start((h + at) as u64)).unwrap();
        f.write_all(&[bytes[h + at] ^ 0x01]).unwrap();
    }
    f.sync_all().unwrap();
    hits.len()
}

fn wait_until(what: &str, secs: u64, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting: {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_ended(h: &JoinHandle<()>, secs: u64) {
    wait_until("the scrub thread ends", secs, || h.is_finished());
}

fn fast(every_ms: u64, rows_per_s: u64) -> Config {
    Config {
        every: Duration::from_millis(every_ms),
        rows_per_s,
    }
}

#[test]
fn the_scheduled_scrub_finds_a_row_damaged_under_a_running_store() {
    let d = Dir::new("flip");
    let hooked = Arc::new(AtomicUsize::new(0));
    let o = StoreOpts {
        on_corrupt: Some(CorruptHook::new({
            let hooked = hooked.clone();
            move |_| {
                hooked.fetch_add(1, Ordering::SeqCst);
            }
        })),
        ..opts()
    };
    let s = Arc::new(HeedStore::open(&d.0, &o).unwrap());
    fill(&s, 300);
    assert!(
        flip_in_file(&d.0, CANARY, 5) >= 1,
        "the canary is in the file"
    );

    let h = scrub::spawn(&s, fast(20, 100_000)).expect("the scrub is on");
    wait_until("the scrub finds the flipped row", 20, || {
        hooked.load(Ordering::SeqCst) > 0
    });
    let e = s.poisoned().expect("the store is poisoned");
    assert!(e.corrupt_store(), "{e}");
    assert!(e.to_string().contains("by the background scrub"), "{e}");
    // It stops by itself: the store serves nothing more.
    wait_ended(&h, 10);
    assert_eq!(hooked.load(Ordering::SeqCst), 1, "the hook runs once");
}

#[test]
fn a_healthy_store_is_scrubbed_pass_after_pass_and_the_scrub_ends_with_it() {
    let d = Dir::new("healthy");
    let s = Arc::new(HeedStore::open(&d.0, &opts()).unwrap());
    fill(&s, 500);
    // The image (500 rows, the canary, meta's two, the format row) and the RAM
    // tables (the same, less the format row): about a thousand rows a pass.
    let pass_rows = 1_000;
    let before = StoreMetrics::get(&s.metrics().scrubbed_rows);

    // 200 rows a step: several steps a pass.
    let h = scrub::spawn(&s, fast(50, 2_000)).expect("the scrub is on");
    wait_until("two passes", 30, || {
        StoreMetrics::get(&s.metrics().scrubbed_rows) - before >= 2 * pass_rows
    });
    assert!(s.poisoned().is_none());

    // Close the store: the scrub holds no reference that keeps it open, and
    // the thread ends after it.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut s = s;
    loop {
        match Arc::try_unwrap(s) {
            Ok(store) => {
                store.close();
                break;
            }
            Err(still) => {
                assert!(Instant::now() < deadline, "the scrub kept the store");
                s = still;
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
    wait_ended(&h, 5);
    // The directory reopens in this process: nothing held it.
    HeedStore::open(&d.0, &opts()).unwrap().close();
}

#[test]
fn zero_turns_the_scrub_off_and_a_bad_value_keeps_the_default() {
    let cfg = |vars: &[(&str, &str)]| {
        Config::from_vars(|k| {
            vars.iter()
                .find(|(name, _)| *name == k)
                .map(|(_, v)| v.to_string())
        })
    };
    assert_eq!(cfg(&[]), Config::default());
    assert_eq!(Config::default().every, Duration::from_secs(21_600));
    assert_eq!(
        cfg(&[
            ("QUEEN_STORE_SCRUB_EVERY_S", "90"),
            ("QUEEN_STORE_SCRUB_ROWS_PER_S", "50"),
        ]),
        Config {
            every: Duration::from_secs(90),
            rows_per_s: 50,
        }
    );
    assert_eq!(
        cfg(&[
            ("QUEEN_STORE_SCRUB_EVERY_S", "soon"),
            ("QUEEN_STORE_SCRUB_ROWS_PER_S", "0"),
        ]),
        Config::default()
    );
    let off = cfg(&[("QUEEN_STORE_SCRUB_EVERY_S", "0")]);
    assert!(off.every.is_zero());

    let d = Dir::new("off");
    let s = Arc::new(HeedStore::open(&d.0, &opts()).unwrap());
    assert!(scrub::spawn(&s, off).is_none(), "no thread when off");
    Arc::try_unwrap(s).ok().unwrap().close();
}
