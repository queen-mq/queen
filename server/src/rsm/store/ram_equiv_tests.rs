//! The RAM containers against a model: the pid-indexed dense table (and the
//! B-tree) must answer EXACTLY what one ordered map of byte keys answers, for
//! every operation of [`super::ram::RamTable`] — point reads, puts, deletes,
//! every range bound in both directions with every limit, the dirty set and
//! its restore, clear, the bulk load — and, through the store, the scan
//! contract of [`super::Reads`] and a reopen from the checkpoint.
//!
//! The keys are chosen to break a dense table: pids on both sides of every
//! block-row and stripe boundary, pids near `u64::MAX`, one pid with enough
//! rows to turn its slot into a tree and back, suffixes empty / short / long
//! (a heap key), and keys a dense table cannot index — shorter than a pid,
//! equal to the lead, another lead byte — which live in its overflow and must
//! interleave with the dense rows in `memcmp` order.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use super::ram::{DirtyMap, Layout, RamKey, RamTable, ScanBuf, StripeBy};
use super::{HeedStore, Keyspace, Reads, Store, StoreOpts, Writes};

/// xorshift64*: deterministic, no dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn chance(&mut self, per_mille: u64) -> bool {
        self.next() % 1000 < per_mille
    }
}

/// The model: one ordered map (key → stored bytes) and its dirty set.
#[derive(Default, Clone)]
struct Model {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
    dirty: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl Model {
    fn put(&mut self, k: &[u8], v: &[u8]) {
        self.map.insert(k.to_vec(), v.to_vec());
        self.dirty.insert(k.to_vec(), Some(v.to_vec()));
    }

    fn remove(&mut self, k: &[u8]) -> Option<Vec<u8>> {
        let v = self.map.remove(k)?;
        self.dirty.insert(k.to_vec(), None);
        Some(v)
    }

    fn clear(&mut self) {
        for k in std::mem::take(&mut self.map).into_keys() {
            self.dirty.insert(k, None);
        }
    }

    fn range(&self, lo: Bound<&[u8]>, hi: Bound<&[u8]>, rev: bool, take: usize) -> Rows {
        let inside = |k: &[u8]| {
            (match lo {
                Bound::Unbounded => true,
                Bound::Included(b) => k >= b,
                Bound::Excluded(b) => k > b,
            }) && (match hi {
                Bound::Unbounded => true,
                Bound::Included(b) => k <= b,
                Bound::Excluded(b) => k < b,
            })
        };
        let it: Box<dyn Iterator<Item = (&Vec<u8>, &Vec<u8>)>> = if rev {
            Box::new(self.map.iter().rev())
        } else {
            Box::new(self.map.iter())
        };
        it.filter(|(k, _)| inside(k))
            .take(take)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

fn table_range(t: &RamTable, lo: Bound<&[u8]>, hi: Bound<&[u8]>, rev: bool, take: usize) -> Rows {
    let mut buf = ScanBuf::new();
    t.copy_range(lo, hi, rev, take, &mut buf);
    (0..buf.len())
        .map(|i| (buf.key(i).to_vec(), buf.stored(i).to_vec()))
        .collect()
}

fn dirty_of(maps: Vec<DirtyMap>) -> HashMap<Vec<u8>, Option<Vec<u8>>> {
    let mut out = HashMap::new();
    for m in maps {
        for (k, v) in m {
            let prev = out.insert(k.to_vec(), v.map(|v| v.to_vec()));
            assert!(prev.is_none(), "a key in two dirty sets: {k:?}");
        }
    }
    out
}

/// The key pool of one lead: dense keys of many shapes and overflow keys.
fn key_pool(lead: &[u8]) -> Vec<Vec<u8>> {
    let pids: Vec<u64> = vec![
        0,
        1,
        2,
        7,
        63,
        64,
        65,
        127,
        1023,
        1024,
        1025,
        1087,
        2047,
        2048,
        3000,
        4095,
        4096,
        70_000,
        0x00FF,
        0x0100,
        0xFF_FFFF,
        0x0100_0000,
        (1 << 40) - 1,
        1 << 63,
        u64::MAX - 64,
        u64::MAX - 1,
        u64::MAX,
    ];
    let suffixes: Vec<Vec<u8>> = vec![
        vec![],
        vec![0x00],
        vec![0x01],
        vec![0xFF],
        vec![0x00, 0x00],
        vec![0xFF, 0xFF],
        b"g1\x00\x00".to_vec(),
        b"g2\x00\x00".to_vec(),
        7u64.to_be_bytes().to_vec(),
        u64::MAX.to_be_bytes().to_vec(),
        [9u8; 16].to_vec(),
        vec![b'x'; 40], // a heap suffix
    ];
    let mut keys = Vec::new();
    for p in &pids {
        for s in &suffixes {
            let mut k = lead.to_vec();
            k.extend_from_slice(&p.to_be_bytes());
            k.extend_from_slice(s);
            keys.push(k);
        }
    }
    // One pid with many rows: its slot goes One → Many → Tree and back.
    for i in 0u16..48 {
        let mut k = lead.to_vec();
        k.extend_from_slice(&5u64.to_be_bytes());
        k.extend_from_slice(&i.to_be_bytes());
        keys.push(k);
    }
    // Keys no dense table can index: shorter than a pid, the bare lead, a
    // partial pid, another lead.
    let mut short = vec![
        vec![0x00],
        vec![0x00, 0x00, 0x00],
        vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04],
        vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00],
        vec![0x01],
        vec![0x01, 0x02, 0x03],
        vec![0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05],
        vec![0x7F],
        vec![0xFF],
        vec![0xFF, 0xFF, 0xFF],
        b"split".to_vec(),
        b"c1".to_vec(),
        b"queue\x00\x00q\x00\x00".to_vec(),
    ];
    if !lead.is_empty() {
        short.push(lead.to_vec());
        let mut partial = lead.to_vec();
        partial.extend_from_slice(&[0, 0, 0]);
        short.push(partial);
    }
    keys.extend(short);
    keys.sort();
    keys.dedup();
    keys
}

/// A bound: a pool key, a prefix end of one, a random short byte string, or
/// none.
fn bound(r: &mut Rng, pool: &[Vec<u8>]) -> (u8, Vec<u8>) {
    let kind = r.below(3) as u8; // 0 unbounded, 1 included, 2 excluded
    let b = match r.below(4) {
        0 | 1 => pool[r.below(pool.len())].clone(),
        2 => {
            let k = &pool[r.below(pool.len())];
            super::prefix_end(&k[..r.below(k.len() + 1)]).unwrap_or_default()
        }
        _ => (0..r.below(11)).map(|_| r.next() as u8 & 0x87).collect(),
    };
    (kind, b)
}

fn as_bound(b: &(u8, Vec<u8>)) -> Bound<&[u8]> {
    match b.0 {
        0 => Bound::Unbounded,
        1 => Bound::Included(&b.1),
        _ => Bound::Excluded(&b.1),
    }
}

fn value(r: &mut Rng, k: &[u8]) -> Vec<u8> {
    // Mostly small (copied out), sometimes past the copy threshold (shared).
    let n = if r.chance(80) {
        300 + r.below(200)
    } else {
        r.below(40)
    };
    let mut v = Vec::with_capacity(n);
    let seed = r.next();
    for i in 0..n {
        v.push((seed as usize + i + k.len()) as u8);
    }
    v
}

/// Keys shaped like `(tenant, queue[, group])`-prefixed keys: names that are
/// prefixes of each other, empty, with an escaped NUL; suffixes empty, a pid,
/// a name; and keys no prefix table can split (too few names, a byte pair no
/// name encoding produces, plain bytes).
fn name_pool() -> Vec<Vec<u8>> {
    use super::keys;
    let names = ["", "a", "ab", "a\0", "b", "tenant-0000-1111-2222", "\u{e9}"];
    let mut keys = Vec::new();
    for t in names {
        for q in ["", "q", "qq", "q\0q"] {
            for pid in [0u64, 1, 255, 256, u64::MAX] {
                keys.push(keys::queue_partitions(t, q, pid));
                for g in ["", "g", "g2"] {
                    keys.push(keys::pending(t, q, g, pid));
                }
            }
            keys.push(keys::queues(t, q));
            keys.push(keys::partitions_by_key(t, q, "p"));
            keys.push(keys::partitions_by_key(t, q, &"x".repeat(40)));
            keys.push(keys::groups(t, q, "g"));
        }
        let mut one = Vec::new();
        keys::push_name(&mut one, t);
        keys.push(one);
    }
    for k in [
        &b"p"[..],
        b"p\x00",
        b"p\x00\x05",
        b"a\x00\x05b\x00\x00q\x00\x00",
        b"\x00",
        b"\x00\x00",
        b"\x00\x01",
        b"\xff\xff",
        b"q",
    ] {
        keys.push(k.to_vec());
    }
    keys.sort();
    keys.dedup();
    keys
}

/// Run `ops` random operations on a table of layout `l` and on the model,
/// comparing every answer.
fn run_against_model(l: Layout, lead: &[u8], seed: u64, ops: usize) {
    run_on_pool(l, key_pool(lead), seed, ops)
}

fn run_on_pool(l: Layout, pool: Vec<Vec<u8>>, seed: u64, ops: usize) {
    let t = RamTable::with_layout(l);
    let mut m = Model::default();
    let mut r = Rng::new(seed);
    let takes = [
        1usize,
        2,
        3,
        15,
        16,
        17,
        63,
        64,
        65,
        255,
        256,
        1000,
        usize::MAX,
    ];
    for op in 0..ops {
        match r.below(100) {
            0..=39 => {
                let k = &pool[r.below(pool.len())];
                let v = value(&mut r, k);
                drop(t.put(k, &v, &[]));
                m.put(k, &v);
            }
            45..=64 => {
                let k = &pool[r.below(pool.len())];
                let got = t.remove(k).map(|v| v.to_vec());
                assert_eq!(got, m.remove(k), "op {op}: remove {k:?}");
            }
            65..=79 => {
                let k = &pool[r.below(pool.len())];
                assert_eq!(
                    t.get(k).map(|v| v.to_vec()),
                    m.map.get(k).cloned(),
                    "op {op}: get {k:?}"
                );
                assert_eq!(
                    t.with(k, |v| v.len()),
                    m.map.get(k).map(|v| v.len()),
                    "op {op}: with {k:?}"
                );
            }
            80..=94 => {
                let lo = bound(&mut r, &pool);
                let hi = bound(&mut r, &pool);
                let rev = r.chance(500);
                let take = takes[r.below(takes.len())];
                let want = m.range(as_bound(&lo), as_bound(&hi), rev, take);
                let got = table_range(&t, as_bound(&lo), as_bound(&hi), rev, take);
                assert!(
                    got == want,
                    "op {op}: range lo={lo:?} hi={hi:?} rev={rev} take={take}: got {} rows {:?}, \
                     want {} rows {:?}",
                    got.len(),
                    got.iter().map(|(k, _)| k).take(6).collect::<Vec<_>>(),
                    want.len(),
                    want.iter().map(|(k, _)| k).take(6).collect::<Vec<_>>()
                );
            }
            95..=96 => {
                // The cut, then (sometimes) its restore.
                let mut maps = Vec::new();
                t.take_dirty(&mut maps);
                let got = dirty_of(maps);
                assert_eq!(got, m.dirty, "op {op}: dirty set");
                let taken: Vec<Vec<u8>> = got.keys().cloned().collect();
                m.dirty.clear();
                assert_eq!(t.dirty_len(), 0);
                if r.chance(500) {
                    // A newer write to some keys first: restore keeps it.
                    for k in taken.iter().take(3) {
                        let v = value(&mut r, k);
                        drop(t.put(k, &v, &[]));
                        m.put(k, &v);
                    }
                    t.restore_dirty(taken.iter().map(|k| RamKey::from(&k[..])));
                    for k in taken {
                        m.dirty
                            .entry(k.clone())
                            .or_insert_with(|| m.map.get(&k).cloned());
                    }
                }
            }
            97 => {
                if r.chance(200) {
                    t.clear();
                    m.clear();
                }
            }
            40..=44 | 98 => {
                // A read-modify-write: create when absent, decline for some
                // values, grow or shrink the rest.
                let k = &pool[r.below(pool.len())];
                let mut scratch = Vec::new();
                let pick = r.next();
                let f = |cur: Option<&[u8]>, out: &mut Vec<u8>| -> bool {
                    match cur {
                        None if pick.is_multiple_of(3) => false,
                        None => {
                            out.extend_from_slice(b"created");
                            true
                        }
                        Some(c) if c.len() % 5 == 1 => false,
                        Some(c) => {
                            out.extend_from_slice(c);
                            if pick.is_multiple_of(2) {
                                out.push(0xAB);
                            } else {
                                out.reverse();
                            }
                            true
                        }
                    }
                };
                let before = m.map.get(k).cloned();
                let mut want = Vec::new();
                let wrote = f(before.as_deref(), &mut want);
                let u = t.upsert(k, &mut scratch, f);
                assert_eq!(u.existed, before.is_some(), "op {op}: upsert existed");
                assert_eq!(u.written, wrote, "op {op}: upsert written");
                drop(u.replaced);
                if wrote {
                    m.put(k, &want);
                }
            }
            _ => {
                assert_eq!(t.len(), m.map.len(), "op {op}: len");
                assert_eq!(t.dirty_len(), m.dirty.len(), "op {op}: dirty len");
            }
        }
    }
    // The whole table, both ways.
    assert_eq!(
        table_range(&t, Bound::Unbounded, Bound::Unbounded, false, usize::MAX),
        m.range(Bound::Unbounded, Bound::Unbounded, false, usize::MAX)
    );
    assert_eq!(
        table_range(&t, Bound::Unbounded, Bound::Unbounded, true, usize::MAX),
        m.range(Bound::Unbounded, Bound::Unbounded, true, usize::MAX)
    );

    // A bulk load of the same rows is the same table, with nothing dirty.
    let rows: Vec<(RamKey, super::ram::RamVal)> = m
        .map
        .iter()
        .map(|(k, v)| (RamKey::from(&k[..]), std::sync::Arc::from(&v[..])))
        .collect();
    let loaded = RamTable::load_with_layout(l, rows);
    assert_eq!(loaded.dirty_len(), 0);
    assert_eq!(loaded.len(), m.map.len());
    for _ in 0..2_000 {
        let lo = bound(&mut r, &pool);
        let hi = bound(&mut r, &pool);
        let rev = r.chance(500);
        let take = takes[r.below(takes.len())];
        assert_eq!(
            table_range(&loaded, as_bound(&lo), as_bound(&hi), rev, take),
            m.range(as_bound(&lo), as_bound(&hi), rev, take),
            "loaded: lo={lo:?} hi={hi:?} rev={rev} take={take}"
        );
    }
}

#[test]
fn the_dense_table_answers_what_an_ordered_map_answers() {
    for seed in 1..=6u64 {
        run_against_model(
            Layout::Dense {
                lead: &[],
                empty_fast_path: seed % 2 == 0,
            },
            &[],
            seed,
            12_000,
        );
    }
}

#[test]
fn the_dense_table_behind_a_lead_byte_answers_what_an_ordered_map_answers() {
    // `counters`: the partition scope is dense behind its scope byte, the
    // named scopes are its overflow.
    for seed in 11..=14u64 {
        run_against_model(
            Layout::Dense {
                lead: &[0],
                empty_fast_path: seed % 2 == 0,
            },
            &[0],
            seed,
            12_000,
        );
    }
}

#[test]
fn the_prefix_table_answers_what_an_ordered_map_answers() {
    let mut seed = 31u64;
    for stripe_by in [StripeBy::Prefix, StripeBy::SuffixPid, StripeBy::SuffixHash] {
        for names in [0usize, 1, 2, 3] {
            run_on_pool(
                Layout::Prefixed { names, stripe_by },
                name_pool(),
                seed,
                6_000,
            );
            seed += 1;
        }
    }
}

/// A prefix whose rows fill every stripe: every scan of it is a merge of 16
/// sub-tables, in rounds, from every position, in both directions, for every
/// size of chunk — against the model.
#[test]
fn a_merged_prefix_scan_is_the_ordered_scan() {
    use super::keys;
    for stripe_by in [StripeBy::SuffixPid, StripeBy::SuffixHash] {
        let t = RamTable::with_layout(Layout::Prefixed {
            names: 2,
            stripe_by,
        });
        let mut m = Model::default();
        let mut r = Rng::new(7);
        for pid in 0u64..5_000 {
            if r.chance(300) {
                continue;
            }
            for (tq, q) in [("t", "q"), ("t", "r")] {
                let k = keys::queue_partitions(tq, q, pid);
                let v = pid.to_le_bytes();
                drop(t.put(&k, &v, &[]));
                m.put(&k, &v);
            }
        }
        let pool: Vec<Vec<u8>> = m.map.keys().cloned().collect();
        let takes = [1usize, 2, 3, 17, 64, 65, 1000, 4000, usize::MAX];
        for _ in 0..600 {
            let k = &pool[r.below(pool.len())];
            let lo = match r.below(3) {
                0 => Bound::Unbounded,
                1 => Bound::Included(&k[..]),
                _ => Bound::Excluded(&k[..]),
            };
            let k2 = &pool[r.below(pool.len())];
            let hi = match r.below(3) {
                0 => Bound::Unbounded,
                1 => Bound::Included(&k2[..]),
                _ => Bound::Excluded(&k2[..]),
            };
            let rev = r.chance(500);
            let take = takes[r.below(takes.len())];
            assert!(
                table_range(&t, lo, hi, rev, take) == m.range(lo, hi, rev, take),
                "{stripe_by:?}: lo={lo:?} hi={hi:?} rev={rev} take={take}"
            );
        }
    }
}

#[test]
fn the_tree_table_answers_what_an_ordered_map_answers() {
    for seed in 21..=22u64 {
        run_against_model(Layout::Tree, &[], seed, 8_000);
    }
}

#[test]
fn a_rewrite_of_the_same_length_reuses_the_value_unless_something_holds_it() {
    let t = RamTable::with_layout(Layout::Dense {
        lead: &[],
        empty_fast_path: false,
    });
    let k = 42u64.to_be_bytes();
    assert!(t.put(&k, b"aaaa", b"1234").is_none());
    let first = t.get(&k).unwrap();
    let p1 = first.as_ptr();
    drop(first);
    // Same length, nothing else holds it (the dirty set's reference is let
    // go of first): rewritten in place.
    assert!(t.put(&k, b"bbbb", b"5678").is_none());
    let now = t.get(&k).unwrap();
    assert_eq!(&*now, b"bbbb5678");
    assert_eq!(now.as_ptr(), p1, "rewritten in its own allocation");
    // A reader holds it: the put allocates, the reader keeps its bytes.
    let replaced = t.put(&k, b"cccc", b"9999");
    assert_eq!(
        &*now, b"bbbb5678",
        "a held value never changes under its holder"
    );
    assert!(replaced.is_some());
    drop(now);
    drop(replaced);
    // A checkpoint cut holds it: same.
    let mut cut = Vec::new();
    t.take_dirty(&mut cut);
    let replaced = t.put(&k, b"dddd", b"0000");
    assert!(replaced.is_some(), "the cut's value is not rewritten");
    let cut = dirty_of(cut);
    assert_eq!(
        cut.get(&k[..]).cloned().flatten().as_deref(),
        Some(&b"cccc9999"[..])
    );
    // A different length: a new allocation.
    assert!(t.put(&k, b"eeeeee", b"1234").is_some());
    assert_eq!(&*t.get(&k).unwrap(), b"eeeeee1234");
}

// ---------------------------------------------------------------------------
// Through the store: the scan contract on dense keyspaces, and a reopen
// ---------------------------------------------------------------------------

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Tmp {
    store: Option<HeedStore>,
    dir: PathBuf,
}

impl Tmp {
    fn new() -> Tmp {
        let dir = std::env::temp_dir().join(format!(
            "queen-rsm-dense-{}-{}",
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

fn opts() -> StoreOpts {
    StoreOpts {
        map_bytes: Some(256 << 20),
        ..Default::default()
    }
}

/// [`Reads::scan_raw`]'s contract written from the CONTRACT (as in
/// `ram_tests`): the prefix is a range, `from` clamps into it, an empty
/// `from` starts at the matching end, a limit of 0 hands over one row, and
/// the row the callback stops on is counted.
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

/// Rows in `ks` shaped like its real keys (plus overflow keys), in enough
/// pids to cross block rows, stripes and the scan chunk.
fn dense_fixture(lead: &[u8]) -> Rows {
    let mut rows = Vec::new();
    for pid in (0u64..2_200).step_by(3).chain([u64::MAX - 1, u64::MAX]) {
        let n = if pid == 30 {
            40
        } else {
            1 + (pid % 3) as usize
        };
        for i in 0..n {
            let mut k = lead.to_vec();
            k.extend_from_slice(&pid.to_be_bytes());
            if !(pid % 5 == 0 && i == 0) {
                k.extend_from_slice(&(i as u16).to_be_bytes());
            }
            let v = [&b"v"[..], &k].concat();
            rows.push((k, v));
        }
    }
    for k in [
        &b"\x00"[..],
        b"\x00\x00\x00\x01",
        b"\x01",
        b"\x01zz",
        b"\xff\xff",
    ] {
        rows.push((k.to_vec(), b"overflow".to_vec()));
    }
    rows.sort();
    rows.dedup_by(|a, b| a.0 == b.0);
    rows
}

fn compare_scans<R: Reads>(r: &R, ks: Keyspace, lead: &[u8], model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let p = |pid: u64| [lead, &pid.to_be_bytes()[..]].concat();
    let prefixes: Vec<Vec<u8>> = vec![
        vec![],
        lead.to_vec(),
        p(0),
        p(30),
        p(1023),
        p(1026),
        p(2199),
        p(u64::MAX),
        [&p(30)[..], &[0x00][..]].concat(),
        [lead, &[0, 0, 0, 0, 0, 0, 4][..]].concat(),
        vec![0x01],
        vec![0xff],
    ];
    let froms: Vec<Vec<u8>> = vec![
        vec![],
        p(0),
        p(1),
        [&p(30)[..], &[0x00, 0x07][..]].concat(),
        p(1024),
        p(1500),
        p(2201),
        p(u64::MAX - 1),
        vec![0x00, 0x00, 0x00, 0x01],
        vec![0x01],
        vec![0xff; 12],
    ];
    let limits = [0usize, 1, 2, 64, 255, 256, 257, 1000, usize::MAX];
    let mut compared = 0u64;
    let mut nonempty = 0u64;
    for prefix in &prefixes {
        for from in &froms {
            for limit in limits {
                for stop in [None, Some(3)] {
                    for rev in [false, true] {
                        let got = run(r, ks, from, prefix, limit, rev, stop);
                        let want = reference(model, from, prefix, limit, rev, stop);
                        assert!(
                            got == want,
                            "{}: from={from:?} prefix={prefix:?} limit={limit} rev={rev} \
                             stop={stop:?}: got {} rows {:?}…, the contract says {} rows {:?}…",
                            ks.name(),
                            got.0,
                            got.1.iter().take(4).map(|(k, _)| k).collect::<Vec<_>>(),
                            want.0,
                            want.1.iter().take(4).map(|(k, _)| k).collect::<Vec<_>>()
                        );
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
        compared > 3_000 && nonempty > 1_000,
        "{compared} {nonempty}"
    );
}

#[test]
fn dense_keyspaces_keep_the_scan_contract_through_the_store_and_a_reopen() {
    let mut t = Tmp::new();
    let cases = [
        (Keyspace::Cursors, &[][..]),
        (Keyspace::Counters, &[0u8][..]),
    ];
    let mut models = Vec::new();
    {
        let s = t.s();
        let mut w = s.write().unwrap();
        for (ks, lead) in cases {
            let rows = dense_fixture(lead);
            for (k, v) in &rows {
                w.put_raw(ks, k, v).unwrap();
            }
            let model: BTreeMap<Vec<u8>, Vec<u8>> = rows.into_iter().collect();
            compare_scans(&w, ks, lead, &model);
            assert_eq!(w.count(ks).unwrap(), model.len() as u64);
            models.push(model);
        }
        w.durable_commit().unwrap();
        drop(w);
        s.read(|r| {
            for ((ks, lead), model) in cases.iter().zip(&models) {
                compare_scans(r, *ks, lead, model);
            }
            Ok(())
        })
        .unwrap();
    }
    t.reopen();
    t.s()
        .read(|r| {
            for ((ks, lead), model) in cases.iter().zip(&models) {
                compare_scans(r, *ks, lead, model);
                assert_eq!(r.count(*ks)?, model.len() as u64);
            }
            Ok(())
        })
        .unwrap();
}

/// A scan of one handle: `(keyspace, from, prefix, limit, rev, stop after)`.
type ScanFn<'a> = dyn Fn(Keyspace, &[u8], &[u8], usize, bool, Option<usize>) -> (usize, Rows) + 'a;

/// The name-keyed index keyspaces keep the scan contract with their real key
/// shapes and the prefixes their readers scan by, through the store and a
/// reopen.
#[test]
fn prefixed_keyspaces_keep_the_scan_contract_through_the_store_and_a_reopen() {
    use super::keys;
    let mut t = Tmp::new();
    let cases = [
        Keyspace::Pending,
        Keyspace::QueuePartitions,
        Keyspace::PartitionsByKey,
    ];
    let tenants = ["t-0000", "t-0001", "t\0z"];
    let queues = ["q", "q2", "qq"];
    let rows_of = |ks: Keyspace| -> Rows {
        let mut rows = Vec::new();
        for t in tenants {
            for q in queues {
                for pid in (0u64..700).step_by(7) {
                    let k = match ks {
                        Keyspace::Pending => {
                            keys::pending(t, q, if pid % 2 == 0 { "g" } else { "h" }, pid)
                        }
                        Keyspace::QueuePartitions => keys::queue_partitions(t, q, pid),
                        _ => keys::partitions_by_key(t, q, &format!("p-{pid}")),
                    };
                    rows.push((k, pid.to_le_bytes().to_vec()));
                }
            }
        }
        // Overflow keys: fewer names than the prefix.
        rows.push((keys::queues_prefix("t-0000"), b"o1".to_vec()));
        rows.push((b"t".to_vec(), b"o2".to_vec()));
        rows.sort();
        rows.dedup_by(|a, b| a.0 == b.0);
        rows
    };
    let prefixes: Vec<Vec<u8>> = vec![
        vec![],
        keys::queues_prefix("t-0000"),
        keys::queues_prefix("t\0z"),
        keys::queue_partitions_prefix("t-0001", "q2"),
        keys::pending_prefix("t-0000", "qq", "g"),
        keys::pending_prefix("t-0000", "qq", "h"),
        keys::groups_prefix("t-0000", "q"),
        b"t".to_vec(),
        b"t-0000".to_vec(),
    ];
    let froms: Vec<Vec<u8>> = vec![
        vec![],
        keys::pending("t-0000", "q", "g", 350),
        keys::queue_partitions("t-0001", "q2", 14),
        keys::partitions_by_key("t-0000", "qq", "p-4"),
        keys::queues("t-0001", "q"),
        keys::queues_prefix("t-0001"),
        b"t-0000\x00\x00q".to_vec(),
        vec![0xff; 4],
    ];
    let limits = [0usize, 1, 2, 255, 256, 257, usize::MAX];
    let compare = |r: &ScanFn<'_>, ks: Keyspace, model: &BTreeMap<Vec<u8>, Vec<u8>>| {
        for prefix in &prefixes {
            for from in &froms {
                for limit in limits {
                    for stop in [None, Some(3)] {
                        for rev in [false, true] {
                            let got = r(ks, from, prefix, limit, rev, stop);
                            let want = reference(model, from, prefix, limit, rev, stop);
                            assert!(
                                got == want,
                                "{}: from={from:?} prefix={prefix:?} limit={limit} rev={rev} \
                                 stop={stop:?}: got {} rows, want {}",
                                ks.name(),
                                got.0,
                                want.0
                            );
                        }
                    }
                }
            }
        }
    };
    let mut models = Vec::new();
    {
        let s = t.s();
        let mut w = s.write().unwrap();
        for ks in cases {
            let rows = rows_of(ks);
            for (k, v) in &rows {
                w.put_raw(ks, k, v).unwrap();
            }
            models.push(rows.into_iter().collect::<BTreeMap<_, _>>());
        }
        for (ks, model) in cases.iter().zip(&models) {
            compare(
                &|ks, from, prefix, limit, rev, stop| run(&w, ks, from, prefix, limit, rev, stop),
                *ks,
                model,
            );
        }
        w.durable_commit().unwrap();
    }
    t.reopen();
    t.s()
        .read(|r| {
            for (ks, model) in cases.iter().zip(&models) {
                compare(
                    &|ks, from, prefix, limit, rev, stop| {
                        run(r, ks, from, prefix, limit, rev, stop)
                    },
                    *ks,
                    model,
                );
                assert_eq!(r.count(*ks)?, model.len() as u64);
            }
            Ok(())
        })
        .unwrap();
}

/// Random writes through the write handle, a checkpoint now and then, and a
/// crash-reopen at the end: every keyspace (dense or not) reopens exactly at
/// its last durable point — the checkpoint wrote every dirty row of every
/// stripe and the load put each back where it belongs.
#[test]
fn every_dense_keyspace_reopens_at_its_last_checkpoint() {
    let mut t = Tmp::new();
    let dense: Vec<(Keyspace, &[u8])> = vec![
        (Keyspace::Partitions, &[]),
        (Keyspace::Garbage, &[]),
        (Keyspace::Cursors, &[]),
        (Keyspace::Txns, &[]),
        (Keyspace::SegLoc, &[]),
        (Keyspace::Dedup, &[]),
        (Keyspace::DlqByPos, &[]),
        (Keyspace::PartitionFiles, &[]),
        (Keyspace::Counters, &[0]),
        (Keyspace::Pending, &[]),
        (Keyspace::QueuePartitions, &[]),
        (Keyspace::PartitionsByKey, &[]),
        (Keyspace::LeasesByWorker, &[]),
        (Keyspace::RequestIds, &[]),
        (Keyspace::RequestExpiry, &[]),
    ];
    let mut r = Rng::new(99);
    let mut live: Vec<BTreeMap<Vec<u8>, Vec<u8>>> = vec![BTreeMap::new(); dense.len()];
    let mut durable = live.clone();
    {
        let s = t.s();
        let mut w = s.write().unwrap();
        for round in 0..4_000 {
            let i = r.below(dense.len());
            let (ks, lead) = dense[i];
            let pool = match ks {
                Keyspace::Pending
                | Keyspace::QueuePartitions
                | Keyspace::PartitionsByKey
                | Keyspace::LeasesByWorker => name_pool(),
                _ => key_pool(lead),
            };
            let k = &pool[r.below(pool.len())];
            if r.chance(300) {
                w.del_raw(ks, k).unwrap();
                live[i].remove(k);
            } else {
                let v = value(&mut r, k);
                w.put_raw(ks, k, &v).unwrap();
                live[i].insert(k.clone(), v);
            }
            if round % 700 == 699 {
                w.durable_commit().unwrap();
                durable = live.clone();
            } else if round % 97 == 0 {
                w.commit().unwrap();
            }
        }
        for (i, (ks, _)) in dense.iter().enumerate() {
            let got = run(&w, *ks, &[], &[], usize::MAX, false, None).1;
            let want: Rows = live[i].clone().into_iter().collect();
            assert!(got == want, "{}: live", ks.name());
        }
    }
    // A crash after the last durable point: the later writes are gone.
    t.reopen();
    t.s()
        .read(|rd| {
            for (i, (ks, _)) in dense.iter().enumerate() {
                let got = run(rd, *ks, &[], &[], usize::MAX, false, None).1;
                let want: Rows = durable[i].clone().into_iter().collect();
                assert!(got == want, "{}: reopened", ks.name());
                for (k, v) in &durable[i] {
                    assert_eq!(rd.get_raw(*ks, k)?, Some(&v[..]));
                }
            }
            Ok(())
        })
        .unwrap();
}

/// The typed fast accessors answer what the owned ones answer, on both
/// handles, and none of them keeps anything in the handle's arena.
#[test]
fn the_fast_typed_reads_answer_what_the_owned_reads_answer() {
    use super::keys;
    use super::rows::{self, GarbageRow, PartitionRow};
    use super::{TypedReads, TypedWrites};
    use crate::rsm::effect::GarbageScope;

    let t = Tmp::new();
    let s = t.s();
    let mut w = s.write().unwrap();
    let row = PartitionRow::new([5u8; 16], "tenant-0000-1111", "queue", "p-7", 1_000);
    w.create_partition(7, &row).unwrap();
    let mut c = rows::cursor_fresh(4, 9);
    c.worker = Some("worker-1".into());
    c.lease_expires_at_us = Some(5_000);
    c.delivered = vec![[1u8; 16], [2u8; 16]];
    c.metadata = "m".into();
    w.put_cursor(7, "g", &c).unwrap();
    w.put_garbage(
        9,
        &GarbageRow {
            deleted_at_us: 1,
            scope: GarbageScope::Queue,
            queue_id: None,
            resume: vec![],
        },
    )
    .unwrap();

    fn check<R: Reads>(r: &R, row: &PartitionRow, c: &crate::rsm::effect::CursorRow) {
        assert_eq!(r.partition(7).unwrap().as_ref(), Some(row));
        assert_eq!(r.partition_head(7).unwrap(), Some(row.head()));
        assert_eq!(
            r.partition_with(7, |p| (p.tenant.to_string(), p.queue.len(), p.to_row()))
                .unwrap(),
            Some((row.tenant.clone(), row.queue.len(), row.clone()))
        );
        assert_eq!(r.partition_head(8).unwrap(), None);
        assert_eq!(r.partition_with(8, |_| ()).unwrap(), None);
        assert_eq!(r.cursor(7, "g").unwrap().as_ref(), Some(c));
        let h = r.cursor_head(7, "g").unwrap().unwrap();
        assert_eq!(h.committed, 4);
        assert!(h.lease_live(4_999) && !h.lease_live(5_000));
        assert_eq!(h.delivered_len, 2);
        assert_eq!(
            r.cursor_with(7, "g", |x| (x.worker.map(str::to_string), x.to_row()))
                .unwrap(),
            Some((c.worker.clone(), c.clone()))
        );
        assert!(r.has_cursor(7, "g").unwrap());
        assert!(!r.has_cursor(7, "h").unwrap());
        assert!(r.is_garbage(9).unwrap());
        assert!(!r.is_garbage(7).unwrap());
        assert_eq!(r.garbage(9).unwrap().is_some(), r.is_garbage(9).unwrap());
        assert_eq!(
            r.pid_of("tenant-0000-1111", "queue", "p-7").unwrap(),
            Some(7)
        );
    }
    check(&w, &row, &c);
    assert_eq!(w.arena_len(), 0, "a typed read keeps nothing in the handle");
    // The raw read still hands out a slice that outlives a later write.
    let raw = w
        .get_raw(Keyspace::Partitions, &keys::pid(7))
        .unwrap()
        .unwrap()
        .to_vec();
    assert_eq!(w.arena_len(), 1);
    assert_eq!(rows::partition_decode(&raw).unwrap(), row);
    w.durable_commit().unwrap();
    drop(w);
    s.read(|r| {
        check(r, &row, &c);
        Ok(())
    })
    .unwrap();
}
