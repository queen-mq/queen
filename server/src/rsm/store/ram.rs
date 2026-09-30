//! The RAM tables every keyspace is served from (Phase C), and the one table
//! that picks each keyspace's container ([`layout`]).
//!
//! Three containers, one contract. Each holds the STORED bytes of every row
//! (`value ‖ checksum` in format 1), is read LIVE by every handle, keeps the
//! rows changed since the last checkpoint in a dirty set whose values are the
//! same `Arc`s the table holds (a checkpoint cut is a swap of those sets), and
//! answers a range in `memcmp` order — LMDB's comparator — for every bound the
//! scans of [`super::Reads`] can ask for. `ram_equiv_tests` holds each of them
//! to one ordered map of byte keys.
//!
//! - [`Layout::Tree`]: one ordered B-tree under one lock. The general case.
//! - [`Layout::Dense`]: keys `lead ‖ pid (8 B big-endian) ‖ suffix`, held in
//!   a table INDEXED by pid. At 10M partitions a B-tree lookup was ~20 byte
//!   comparisons, most of them a cache miss (`RamTable::get` + `memcmp` were
//!   half of a planning lane and of the apply thread, 2026-09-29); here a
//!   lookup is a stripe lock, one hash probe for the pid's block and a search
//!   among that pid's own rows — usually one. The rows are spread over
//!   [`STRIPES`] locks by `pid % STRIPES`, so the planning lanes, the planner,
//!   the apply writer (and the sharded apply's writers) and the readers stop
//!   meeting on one lock word per keyspace. Keys of another shape (shorter,
//!   another lead) live in an ordered overflow, and every range merges the two,
//!   so the byte-level contract does not depend on what a caller puts there.
//! - [`Layout::Prefixed`]: keys that begin with `(tenant, queue[, group])`
//!   names, held as prefix → sub-table by the rest ([`PrefixTable`]): the
//!   36-byte tenant id and the queue name, most of each key's bytes, are
//!   stored once per queue instead of once per row.
//!
//! # Writes
//!
//! A put is ONE lookup of its row (no second walk to share a key with the
//! dirty set), and overwriting a value of the same length reuses its
//! allocation whenever nothing else holds it: the dirty set's reference is
//! dropped first, and a reader never holds a small value (reads copy it out
//! under the lock), so a counter or a partition row rewritten a thousand times
//! between checkpoints is allocated once. A value that a checkpoint cut or a
//! reader still holds is replaced by a new allocation instead — `Arc::get_mut`
//! decides, so the cut always keeps the bytes it took.
//!
//! Every mutation happens under the write lock of the stripe (or table) that
//! holds the row, dirty set included, so several writers — the sharded apply's
//! writers — may write DIFFERENT keys of one table at once.
//!
//! # Reads
//!
//! [`RamTable::with`] runs a closure over the stored bytes under the read
//! lock: the typed accessors decode there, with no copy and no reference
//! count. A scan copies its rows out in chunks ([`ScanBuf`]: small values as
//! bytes, large ones as a shared reference) and hands them to its callback with
//! no lock held.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::keys::CounterScope;
use super::Keyspace;
use crate::rsm::fasthash::FxBuild;

/// Values are `Arc`s so that a checkpoint cut can take a row for the price of
/// a reference count, and a large value can be handed to a reader without a
/// copy.
pub(crate) type RamVal = Arc<[u8]>;

/// The rows changed since the last checkpoint, each with its value as of its
/// last write (`None` = deleted).
pub(crate) type DirtyMap = HashMap<RamKey, Option<RamVal>, FxBuild>;

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

/// How many key bytes a [`RamKey`] keeps in place. 30 makes the key 32 bytes:
/// the counters, cursors and partition rows fit (and save their own heap
/// block), a `pending` key does not. 54 (every hot key inline) was 1.1x on
/// consumption at 10M partitions but +21% resident memory (2026-09-29).
pub(crate) const RAM_KEY_INLINE: usize = 30;

/// A RAM key (or a dense row's suffix). Up to [`RAM_KEY_INLINE`] bytes live in
/// the key itself, so in the tree's nodes: a lookup compares a node's keys
/// where they sit instead of chasing one pointer per comparison. Longer keys
/// stay behind an `Arc`. Ordered, compared and hashed exactly as their bytes
/// (`Borrow<[u8]>`), so every lookup, range and the dirty map read them as the
/// byte keys they are.
#[derive(Clone)]
pub(crate) enum RamKey {
    Inline(u8, [u8; RAM_KEY_INLINE]),
    Heap(Arc<[u8]>),
}

impl RamKey {
    #[inline]
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            RamKey::Inline(n, b) => &b[..*n as usize],
            RamKey::Heap(a) => a,
        }
    }
}

impl From<&[u8]> for RamKey {
    #[inline]
    fn from(k: &[u8]) -> RamKey {
        if k.len() <= RAM_KEY_INLINE {
            let mut b = [0u8; RAM_KEY_INLINE];
            b[..k.len()].copy_from_slice(k);
            RamKey::Inline(k.len() as u8, b)
        } else {
            RamKey::Heap(Arc::from(k))
        }
    }
}

impl std::ops::Deref for RamKey {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.bytes()
    }
}

impl std::borrow::Borrow<[u8]> for RamKey {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.bytes()
    }
}

impl PartialEq for RamKey {
    fn eq(&self, o: &RamKey) -> bool {
        self.bytes() == o.bytes()
    }
}

impl Eq for RamKey {}

impl PartialOrd for RamKey {
    fn partial_cmp(&self, o: &RamKey) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}

impl Ord for RamKey {
    fn cmp(&self, o: &RamKey) -> std::cmp::Ordering {
        self.bytes().cmp(o.bytes())
    }
}

impl std::hash::Hash for RamKey {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        self.bytes().hash(h)
    }
}

impl std::fmt::Debug for RamKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.bytes().fmt(f)
    }
}

// ---------------------------------------------------------------------------
// The layout table
// ---------------------------------------------------------------------------

/// How one RAM keyspace holds its rows. Chosen in ONE place, [`layout`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Layout {
    /// One `memcmp`-ordered B-tree under one lock.
    Tree,
    /// Keys `lead ‖ pid (8 B, big-endian) ‖ suffix` in a pid-indexed table
    /// ([`DenseTable`]); any other key in its ordered overflow.
    ///
    /// `empty_fast_path`: the table keeps a live row count, and a read of an
    /// EMPTY table answers without a lock. Worth it for a keyspace that is
    /// empty most of the time and read on every command (`garbage` is read by
    /// every partition lookup); a waste of one shared atomic per insert and
    /// delete for the ones that are never empty.
    Dense {
        lead: &'static [u8],
        empty_fast_path: bool,
    },
    /// Keys that begin with `names` escaped names ([`super::keys::push_name`]):
    /// prefix → sub-table by the rest ([`PrefixTable`]), so a long common
    /// prefix is stored once per stripe; any other key in its ordered
    /// overflow. The rows are spread over the stripes by `stripe_by`.
    Prefixed { names: usize, stripe_by: StripeBy },
}

/// The partition scope's lead byte in `counters` (`keys::counter_partition`).
const COUNTER_PARTITION_LEAD: &[u8] = &[CounterScope::Partition as u8];

/// The container of every keyspace. A keyspace whose keys begin with a pid is
/// dense; the partition scope of `counters` is dense behind its scope byte
/// (the queue, tenant and group scopes, keyed by names, sit in its overflow);
/// the name-keyed keyspaces are trees.
pub(crate) fn layout(ks: Keyspace) -> Layout {
    match ks {
        // Read by every partition lookup, empty almost always.
        Keyspace::Garbage => Layout::Dense {
            lead: &[],
            empty_fast_path: true,
        },
        // Written per dead letter, per sealed file and (index mode `rows`
        // only) per message: empty or near it on the default path.
        Keyspace::DlqByPos | Keyspace::PartitionFiles | Keyspace::Dedup => Layout::Dense {
            lead: &[],
            empty_fast_path: true,
        },
        Keyspace::Partitions | Keyspace::Cursors | Keyspace::Txns | Keyspace::SegLoc => {
            Layout::Dense {
                lead: &[],
                empty_fast_path: false,
            }
        }
        Keyspace::Counters => Layout::Dense {
            lead: COUNTER_PARTITION_LEAD,
            empty_fast_path: false,
        },
        // The name-keyed tables every apply shard writes: striped by the pid
        // in the key (a shard's pids are its own), or by a hash where the key
        // has no pid. One row per partition behind its `(tenant, queue)` — a
        // 36-byte tenant id and the queue name, most of each key's bytes.
        Keyspace::PartitionsByKey => Layout::Prefixed {
            names: 2,
            stripe_by: StripeBy::SuffixHash,
        },
        Keyspace::QueuePartitions => Layout::Prefixed {
            names: 2,
            stripe_by: StripeBy::SuffixPid,
        },
        // One row per (partition, group) behind `(tenant, queue, group)`.
        Keyspace::Pending => Layout::Prefixed {
            names: 3,
            stripe_by: StripeBy::SuffixPid,
        },
        // `(worker, pid, group)`: behind the worker, by the pid.
        Keyspace::LeasesByWorker => Layout::Prefixed {
            names: 1,
            stripe_by: StripeBy::SuffixPid,
        },
        // One row per logged command, written by every shard: no prefix, by a
        // hash of the id (and of `(now, id)` for the expiry order).
        Keyspace::RequestIds | Keyspace::RequestExpiry => Layout::Prefixed {
            names: 0,
            stripe_by: StripeBy::SuffixHash,
        },
        _ => Layout::Tree,
    }
}

// ---------------------------------------------------------------------------
// Locks
// ---------------------------------------------------------------------------

// A panic under a WRITE lock is an allocation failure inside one map
// operation, which leaves the maps structurally whole, so a poisoned lock is
// read through rather than turned into a second panic.
#[inline]
fn rd<T>(l: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(PoisonError::into_inner)
}

#[inline]
fn wr<T>(l: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(PoisonError::into_inner)
}

/// A lock on its own cache line pair, so two stripes never share one.
#[repr(align(128))]
struct Padded<T>(T);

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

/// `val ‖ trailer` in ONE allocation (the trailer is the checksum in format 1,
/// empty in format 0).
fn make_val(val: &[u8], trailer: &[u8]) -> RamVal {
    let n = val.len() + trailer.len();
    let mut out = Arc::<[u8]>::new_uninit_slice(n);
    let dst = Arc::get_mut(&mut out).expect("a fresh Arc has no other owner");
    // SAFETY: `dst` is exactly `n` bytes long and the two copies below write
    // all of them — `val` at 0, `trailer` at `val.len()` — from sources that
    // cannot overlap a fresh allocation, so every byte is initialized before
    // `assume_init`.
    unsafe {
        let p = dst.as_mut_ptr() as *mut u8;
        std::ptr::copy_nonoverlapping(val.as_ptr(), p, val.len());
        std::ptr::copy_nonoverlapping(trailer.as_ptr(), p.add(val.len()), trailer.len());
        out.assume_init()
    }
}

/// Overwrite `slot` with `val ‖ trailer`: in place when nothing else holds its
/// allocation and the length is unchanged, else with a new one. Returns the
/// replaced value, for the caller to drop outside the lock.
#[inline]
fn overwrite(slot: &mut RamVal, val: &[u8], trailer: &[u8]) -> Option<RamVal> {
    if let Some(buf) = Arc::get_mut(slot) {
        if buf.len() == val.len() + trailer.len() {
            let (a, b) = buf.split_at_mut(val.len());
            a.copy_from_slice(val);
            b.copy_from_slice(trailer);
            return None;
        }
    }
    Some(std::mem::replace(slot, make_val(val, trailer)))
}

/// Whether `BTreeMap::range` accepts these bounds. It PANICS on an inverted
/// range (and on an empty one with both ends excluded), where an LMDB cursor
/// just yields nothing — so such a range is answered as empty, as LMDB does.
pub(crate) fn range_is_walkable(lo: Bound<&[u8]>, hi: Bound<&[u8]>) -> bool {
    match (lo, hi) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
        (Bound::Excluded(a), Bound::Excluded(b)) => a < b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
            a <= b
        }
    }
}

#[inline]
fn above_lo(k: &[u8], lo: Bound<&[u8]>) -> bool {
    match lo {
        Bound::Unbounded => true,
        Bound::Included(b) => k >= b,
        Bound::Excluded(b) => k > b,
    }
}

#[inline]
fn below_hi(k: &[u8], hi: Bound<&[u8]>) -> bool {
    match hi {
        Bound::Unbounded => true,
        Bound::Included(b) => k <= b,
        Bound::Excluded(b) => k < b,
    }
}

// ---------------------------------------------------------------------------
// The scan buffer
// ---------------------------------------------------------------------------

/// Values up to this size are COPIED out of a table (a scan's chunk, a
/// `get_raw`'s arena) instead of shared: a copy of a few hundred bytes costs
/// less than two atomic operations on a reference count other threads touch,
/// and leaves the value unshared, so the writer can rewrite it in place.
pub(crate) const COPY_MAX: usize = 256;

#[derive(Clone, Copy)]
enum Held {
    /// `bytes[a..b]`.
    Bytes(usize, usize),
    /// `pins[i]`.
    Pin(usize),
}

#[derive(Clone, Copy)]
struct BufRow {
    /// The dense pid (0 for a tree row): the sort key within a dense window.
    pid: u64,
    key: (usize, usize),
    val: Held,
}

/// Rows copied out of a table under its lock, to be handed to a callback with
/// no lock held: keys and small values as bytes, large values as a shared
/// reference. The values are STORED bytes (checksum included).
#[derive(Default)]
pub(crate) struct ScanBuf {
    bytes: Vec<u8>,
    rows: Vec<BufRow>,
    pins: Vec<RamVal>,
    /// A dense window's rows per pid position (`(first row, rows)`), and the
    /// rows in pid order: the window's order without a sort.
    spans: Vec<(usize, usize)>,
    order: Vec<BufRow>,
}

impl ScanBuf {
    pub(crate) fn new() -> ScanBuf {
        ScanBuf::default()
    }

    pub(crate) fn clear(&mut self) {
        self.bytes.clear();
        self.rows.clear();
        self.pins.clear();
        self.spans.clear();
        self.order.clear();
    }

    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn key(&self, i: usize) -> &[u8] {
        let (a, b) = self.rows[i].key;
        &self.bytes[a..b]
    }

    pub(crate) fn stored(&self, i: usize) -> &[u8] {
        match self.rows[i].val {
            Held::Bytes(a, b) => &self.bytes[a..b],
            Held::Pin(p) => &self.pins[p],
        }
    }

    #[inline]
    fn push(&mut self, pid: u64, parts: [&[u8]; 3], stored: &RamVal) {
        let k0 = self.bytes.len();
        for p in parts {
            self.bytes.extend_from_slice(p);
        }
        let k1 = self.bytes.len();
        let val = if stored.len() <= COPY_MAX {
            self.bytes.extend_from_slice(stored);
            Held::Bytes(k1, self.bytes.len())
        } else {
            self.pins.push(stored.clone());
            Held::Pin(self.pins.len() - 1)
        };
        self.rows.push(BufRow {
            pid,
            key: (k0, k1),
            val,
        });
    }

    fn truncate(&mut self, n: usize) {
        self.rows.truncate(n);
    }

    /// Append row `j` of another buffer: its key and value bytes (a pinned
    /// value stays pinned).
    fn push_row(&mut self, from: &ScanBuf, j: usize) {
        let r = from.rows[j];
        let k0 = self.bytes.len();
        self.bytes.extend_from_slice(&from.bytes[r.key.0..r.key.1]);
        let k1 = self.bytes.len();
        let val = match r.val {
            Held::Bytes(a, b) => {
                self.bytes.extend_from_slice(&from.bytes[a..b]);
                Held::Bytes(k1, self.bytes.len())
            }
            Held::Pin(i) => {
                self.pins.push(from.pins[i].clone());
                Held::Pin(self.pins.len() - 1)
            }
        };
        self.rows.push(BufRow {
            pid: r.pid,
            key: (k0, k1),
            val,
        });
    }

    /// Put the rows of a dense window (pushed from `from` on, stripe after
    /// stripe) in pid order — ascending, or descending with `rev` — keeping
    /// each pid's rows in the order they were copied (suffix order, or its
    /// reverse): the key order of the window. `spans[pos]` is where the rows
    /// of window position `pos` (`slot offset × STRIPES + stripe`, which is
    /// pid order) were pushed. A permutation of the row descriptors, no sort.
    fn order_window(&mut self, from: usize, rev: bool) {
        let ScanBuf {
            rows, spans, order, ..
        } = self;
        order.clear();
        let mut take = |&(a, n): &(usize, usize)| order.extend_from_slice(&rows[a..a + n]);
        if rev {
            spans.iter().rev().for_each(&mut take);
        } else {
            spans.iter().for_each(&mut take);
        }
        rows.truncate(from);
        rows.extend_from_slice(order);
    }

    /// Order the rows from `from` on by their key bytes (the merge of a dense
    /// table's rows with its overflow's).
    fn sort_by_key(&mut self, from: usize, rev: bool) {
        let ScanBuf { bytes, rows, .. } = self;
        rows[from..].sort_by(|a, b| {
            let o = bytes[a.key.0..a.key.1].cmp(&bytes[b.key.0..b.key.1]);
            if rev {
                o.reverse()
            } else {
                o
            }
        });
    }
}

thread_local! {
    /// Scan buffers this thread has used, kept with their capacity: a scan of
    /// one partition's rows is on the pop path, and two fresh vectors per scan
    /// were two allocations per scan.
    static SCAN_BUFS: std::cell::RefCell<Vec<ScanBuf>> = const {
        std::cell::RefCell::new(Vec::new())
    };
}

/// A pooled buffer keeps at most this much capacity (a scan of a huge chunk
/// gives its memory back).
const POOL_KEEP_BYTES: usize = 1 << 20;

/// A [`ScanBuf`] from this thread's pool — a fresh one when the pool is empty,
/// as for a scan run from inside another scan's callback — given back on drop.
pub(crate) struct PooledBuf(Option<ScanBuf>);

impl PooledBuf {
    pub(crate) fn take() -> PooledBuf {
        let b = SCAN_BUFS
            .try_with(|p| p.borrow_mut().pop())
            .ok()
            .flatten()
            .unwrap_or_default();
        PooledBuf(Some(b))
    }
}

impl std::ops::Deref for PooledBuf {
    type Target = ScanBuf;
    fn deref(&self) -> &ScanBuf {
        self.0.as_ref().expect("taken until drop")
    }
}

impl std::ops::DerefMut for PooledBuf {
    fn deref_mut(&mut self) -> &mut ScanBuf {
        self.0.as_mut().expect("taken until drop")
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        let Some(mut b) = self.0.take() else { return };
        if b.bytes.capacity() > POOL_KEEP_BYTES {
            return;
        }
        b.clear();
        let _ = SCAN_BUFS.try_with(|p| {
            let mut p = p.borrow_mut();
            if p.len() < 4 {
                p.push(b);
            }
        });
    }
}

// ---------------------------------------------------------------------------
// The handle arena
// ---------------------------------------------------------------------------

/// The arena's first block, and the largest it grows a block to (each new
/// block doubles the last): a handle that reads one value allocates a few
/// hundred bytes, one that reads thousands a few large blocks.
const ARENA_FIRST: usize = 512;
const ARENA_MAX_BLOCK: usize = 64 << 10;

/// What a handle's `get_raw` hands out slices of: small values COPIED into
/// blocks the arena owns, large ones pinned by a reference. A slice stays
/// valid for as long as the arena holds its bytes: until [`Arena::clear`]
/// (which only a `&mut` method of the handle calls, when no slice borrowed from
/// `&self` can be alive) or the handle's drop.
///
/// The blocks are raw allocations written through the pointer they were
/// allocated with — never through a reference to a whole block — so writing
/// the next value never aliases a slice already handed out.
pub(crate) struct Arena {
    /// `(start, capacity)` of every block, the last one being filled.
    blocks: Vec<(NonNull<u8>, usize)>,
    /// Bytes used in the last block.
    used: usize,
    pins: Vec<RamVal>,
    /// Values held (copies and pins), for the tests.
    held: usize,
}

// SAFETY: the arena exclusively owns its blocks (plain bytes) and its pins
// (`Arc<[u8]>`, which is `Send`); nothing about them is tied to a thread.
unsafe impl Send for Arena {}

impl Default for Arena {
    fn default() -> Arena {
        Arena::new()
    }
}

impl Arena {
    pub(crate) fn new() -> Arena {
        Arena {
            blocks: Vec::new(),
            used: 0,
            pins: Vec::new(),
            held: 0,
        }
    }

    /// Values held since the last clear.
    pub(crate) fn len(&self) -> usize {
        self.held
    }

    /// Keep `stored` for as long as the arena, and return a pointer to the
    /// bytes kept: a copy for a small value, the value itself for a large one.
    pub(crate) fn hold(&mut self, stored: &RamVal) -> *const [u8] {
        self.held += 1;
        if stored.len() > COPY_MAX {
            let p: *const [u8] = &**stored;
            self.pins.push(stored.clone());
            return p;
        }
        self.copy(stored)
    }

    fn copy(&mut self, b: &[u8]) -> *const [u8] {
        let n = b.len();
        if n == 0 {
            // A zero-length slice needs no bytes, only an aligned pointer.
            return std::ptr::slice_from_raw_parts(NonNull::<u8>::dangling().as_ptr(), 0);
        }
        let fits = self
            .blocks
            .last()
            .is_some_and(|(_, cap)| self.used + n <= *cap);
        if !fits {
            let grown = self
                .blocks
                .last()
                .map_or(ARENA_FIRST, |(_, cap)| (cap * 2).min(ARENA_MAX_BLOCK));
            let cap = grown.max(n);
            self.blocks.push((alloc_block(cap), cap));
            self.used = 0;
        }
        let (base, _) = *self.blocks.last().expect("a block was just made");
        // SAFETY: `base` is the start of a live allocation of `cap` bytes this
        // arena owns, and `used + n <= cap`, so the destination is inside it.
        // The bytes written were never handed out (every slice handed out lies
        // below `used`, and a slice is only ever made over bytes written), so
        // no reference aliases them; the source is a table's value, a
        // different allocation.
        unsafe {
            let dst = base.as_ptr().add(self.used);
            std::ptr::copy_nonoverlapping(b.as_ptr(), dst, n);
            self.used += n;
            std::ptr::slice_from_raw_parts(dst as *const u8, n)
        }
    }

    /// Release every value held. Only callable with `&mut`, i.e. when no slice
    /// handed out through `&self` can still be alive. The last (largest) block
    /// is kept for the next run of reads.
    pub(crate) fn clear(&mut self) {
        self.pins.clear();
        self.held = 0;
        self.used = 0;
        if self.blocks.len() > 1 {
            let keep = self.blocks.pop().expect("len > 1");
            for (p, cap) in self.blocks.drain(..) {
                free_block(p, cap);
            }
            self.blocks.push(keep);
        }
    }
}

/// An arena block: `cap` bytes, UNINITIALIZED — the arena only ever makes a
/// slice over bytes it has written.
fn alloc_block(cap: usize) -> NonNull<u8> {
    let layout = std::alloc::Layout::from_size_align(cap, 1).expect("a block size");
    // SAFETY: `cap > 0` (a zero-length copy never allocates).
    let p = unsafe { std::alloc::alloc(layout) };
    NonNull::new(p).unwrap_or_else(|| std::alloc::handle_alloc_error(layout))
}

fn free_block(p: NonNull<u8>, cap: usize) {
    let layout = std::alloc::Layout::from_size_align(cap, 1).expect("a block size");
    // SAFETY: allocated by `alloc_block` with this very layout, freed once
    // (the caller removed it from the arena).
    unsafe { std::alloc::dealloc(p.as_ptr(), layout) }
}

impl Drop for Arena {
    fn drop(&mut self) {
        for (p, cap) in self.blocks.drain(..) {
            free_block(p, cap);
        }
    }
}

// ---------------------------------------------------------------------------
// The tree container
// ---------------------------------------------------------------------------

/// A B-tree of rows and its dirty set: [`Layout::Tree`]'s table, and a dense
/// table's overflow.
#[derive(Default)]
struct TreeRows {
    /// The live rows, in `memcmp` order.
    map: BTreeMap<RamKey, RamVal>,
    dirty: DirtyMap,
}

/// What one put did to its container.
struct Put {
    /// The row's value now (the dirty set's copy of the reference).
    current: RamVal,
    /// The value it replaced, when the replacement needed a new allocation.
    replaced: Option<RamVal>,
    /// The key was not there before.
    added: bool,
}

/// What one [`RamTable::upsert`] did.
#[derive(Default)]
pub(crate) struct Upserted {
    /// The row was there before.
    pub(crate) existed: bool,
    /// The closure wrote a value (the row is now that value, and dirty).
    pub(crate) written: bool,
    /// The value it replaced when the rewrite needed a new allocation, for
    /// the caller to drop outside the lock.
    pub(crate) replaced: Option<RamVal>,
    /// A new row (the container counts it).
    added: bool,
}

/// An upsert of a row that EXISTS, its value at `v`, under the write lock of
/// its container: `f` gets the stored bytes and writes the new stored bytes
/// into `scratch`, rewritten in place when nothing else holds the value (the
/// dirty set's reference is let go of first, as a put does).
fn upsert_existing(
    v: &mut RamVal,
    dirty: &mut DirtyMap,
    key: &[u8],
    scratch: &mut Vec<u8>,
    f: impl FnOnce(Option<&[u8]>, &mut Vec<u8>) -> bool,
) -> Upserted {
    scratch.clear();
    match dirty.get_mut(key) {
        Some(d) => {
            drop(d.take());
            let written = f(Some(v), scratch);
            let replaced = if written {
                overwrite(v, scratch, &[])
            } else {
                None
            };
            *d = Some(v.clone());
            Upserted {
                existed: true,
                written,
                replaced,
                added: false,
            }
        }
        None => {
            if !f(Some(v), scratch) {
                return Upserted {
                    existed: true,
                    ..Upserted::default()
                };
            }
            let replaced = overwrite(v, scratch, &[]);
            dirty.insert(RamKey::from(key), Some(v.clone()));
            Upserted {
                existed: true,
                written: true,
                replaced,
                added: false,
            }
        }
    }
}

impl TreeRows {
    fn put_in_map(
        map: &mut BTreeMap<RamKey, RamVal>,
        key: &[u8],
        val: &[u8],
        trailer: &[u8],
    ) -> Put {
        if key.len() <= RAM_KEY_INLINE {
            // One walk whether the key is there or not: building an inline
            // key is a copy.
            match map.entry(RamKey::from(key)) {
                std::collections::btree_map::Entry::Occupied(mut e) => {
                    let replaced = overwrite(e.get_mut(), val, trailer);
                    Put {
                        current: e.get().clone(),
                        replaced,
                        added: false,
                    }
                }
                std::collections::btree_map::Entry::Vacant(e) => {
                    let v = make_val(val, trailer);
                    e.insert(v.clone());
                    Put {
                        current: v,
                        replaced: None,
                        added: true,
                    }
                }
            }
        } else {
            // A long key costs an allocation to build: look it up first, and
            // build it only for a row that is new.
            match map.get_mut(key) {
                Some(slot) => {
                    let replaced = overwrite(slot, val, trailer);
                    Put {
                        current: slot.clone(),
                        replaced,
                        added: false,
                    }
                }
                None => {
                    let v = make_val(val, trailer);
                    map.insert(RamKey::from(key), v.clone());
                    Put {
                        current: v,
                        replaced: None,
                        added: true,
                    }
                }
            }
        }
    }

    fn put(&mut self, key: &[u8], val: &[u8], trailer: &[u8]) -> Put {
        let TreeRows { map, dirty } = self;
        match dirty.get_mut(key) {
            Some(d) => {
                // The dirty set holds the same `Arc` as the map: let go of it
                // first, so the map's copy can be rewritten in place.
                drop(d.take());
                let p = TreeRows::put_in_map(map, key, val, trailer);
                *d = Some(p.current.clone());
                p
            }
            None => {
                let p = TreeRows::put_in_map(map, key, val, trailer);
                dirty.insert(RamKey::from(key), Some(p.current.clone()));
                p
            }
        }
    }

    fn upsert(
        &mut self,
        key: &[u8],
        scratch: &mut Vec<u8>,
        f: impl FnOnce(Option<&[u8]>, &mut Vec<u8>) -> bool,
    ) -> Upserted {
        let TreeRows { map, dirty } = self;
        match map.get_mut(key) {
            Some(v) => upsert_existing(v, dirty, key, scratch, f),
            None => {
                scratch.clear();
                if !f(None, scratch) {
                    return Upserted::default();
                }
                let p = self.put(key, scratch, &[]);
                Upserted {
                    existed: false,
                    written: true,
                    replaced: p.replaced,
                    added: p.added,
                }
            }
        }
    }

    fn put_arc(&mut self, key: &[u8], stored: RamVal) -> (Option<RamVal>, bool) {
        let old = self.map.insert(RamKey::from(key), stored.clone());
        let added = old.is_none();
        let old_dirty = self.dirty.insert(RamKey::from(key), Some(stored));
        drop(old_dirty);
        (old, added)
    }

    fn remove(&mut self, key: &[u8]) -> Option<RamVal> {
        let (k, v) = self.map.remove_entry(key)?;
        // Already dirty: `insert` keeps the set's key and replaces the value.
        self.dirty.insert(k, None);
        Some(v)
    }

    /// Every row becomes a dirty delete. Returns the old rows, for the caller
    /// to free outside the lock.
    fn clear(&mut self) -> BTreeMap<RamKey, RamVal> {
        let old = std::mem::take(&mut self.map);
        for k in old.keys() {
            self.dirty.insert(k.clone(), None);
        }
        old
    }

    fn restore_dirty(&mut self, k: RamKey) {
        if !self.dirty.contains_key(&*k) {
            let v = self.map.get(&*k).cloned();
            self.dirty.insert(k, v);
        }
    }

    fn copy_range(
        &self,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        rev: bool,
        take: usize,
        out: &mut ScanBuf,
    ) {
        if take == 0 || !range_is_walkable(lo, hi) {
            return;
        }
        let it = self.map.range::<[u8], _>((lo, hi));
        if rev {
            for (k, v) in it.rev().take(take) {
                out.push(0, [k, &[], &[]], v);
            }
        } else {
            for (k, v) in it.take(take) {
                out.push(0, [k, &[], &[]], v);
            }
        }
    }
}

/// [`Layout::Tree`]: one B-tree under one lock.
pub(crate) struct TreeTable {
    rows: RwLock<TreeRows>,
}

impl TreeTable {
    fn new() -> TreeTable {
        TreeTable {
            rows: RwLock::new(TreeRows::default()),
        }
    }

    fn load(rows: Vec<(RamKey, RamVal)>) -> TreeTable {
        TreeTable {
            rows: RwLock::new(TreeRows {
                // The rows come in key order off an LMDB cursor, so the
                // collect's sort is a single linear pass before the bulk build.
                map: rows.into_iter().collect(),
                dirty: DirtyMap::default(),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// The dense container
// ---------------------------------------------------------------------------

/// Stripes of a dense table: a pid's rows are guarded by the lock of stripe
/// `pid % STRIPES`, so concurrent readers and writers of different partitions
/// rarely share a lock, however few partitions are hot.
pub(crate) const STRIPES: usize = 64;
/// Slots per block per stripe: a block row covers `STRIPES × SLOTS` pids.
const SLOTS: usize = 16;
/// `pid >> ROW_SHIFT` is the pid's block row.
const ROW_SHIFT: u32 = (STRIPES * SLOTS).trailing_zeros();
const STRIPE_SHIFT: u32 = STRIPES.trailing_zeros();
/// A pid's rows are a single row, a small sorted vector up to this many, a
/// B-tree beyond (back to a vector at half of it).
const MANY_MAX: usize = 16;

#[inline]
fn stripe_of(pid: u64) -> usize {
    (pid as usize) & (STRIPES - 1)
}

#[inline]
fn slot_of(pid: u64) -> usize {
    ((pid >> STRIPE_SHIFT) as usize) & (SLOTS - 1)
}

#[inline]
fn pid_at(row: u64, idx: usize, stripe: usize) -> u64 {
    (row << ROW_SHIFT) | ((idx as u64) << STRIPE_SHIFT) | stripe as u64
}

/// The rows of one pid, by suffix.
#[derive(Default)]
enum Slot {
    #[default]
    Empty,
    One(RamKey, RamVal),
    /// Sorted by suffix, 2..=MANY_MAX rows.
    Many(Vec<(RamKey, RamVal)>),
    /// More than MANY_MAX / 2 rows.
    Tree(BTreeMap<RamKey, RamVal>),
}

impl Slot {
    fn is_empty(&self) -> bool {
        matches!(self, Slot::Empty)
    }

    fn len(&self) -> usize {
        match self {
            Slot::Empty => 0,
            Slot::One(..) => 1,
            Slot::Many(r) => r.len(),
            Slot::Tree(m) => m.len(),
        }
    }

    #[inline]
    fn get(&self, sfx: &[u8]) -> Option<&RamVal> {
        match self {
            Slot::Empty => None,
            Slot::One(k, v) => (k.bytes() == sfx).then_some(v),
            Slot::Many(rows) => rows
                .binary_search_by(|(k, _)| k.bytes().cmp(sfx))
                .ok()
                .map(|i| &rows[i].1),
            Slot::Tree(m) => m.get(sfx),
        }
    }

    #[inline]
    fn get_mut(&mut self, sfx: &[u8]) -> Option<&mut RamVal> {
        match self {
            Slot::Empty => None,
            Slot::One(k, v) => (k.bytes() == sfx).then_some(v),
            Slot::Many(rows) => match rows.binary_search_by(|(k, _)| k.bytes().cmp(sfx)) {
                Ok(i) => Some(&mut rows[i].1),
                Err(_) => None,
            },
            Slot::Tree(m) => m.get_mut(sfx),
        }
    }

    /// Put `val ‖ trailer` under `sfx`: ONE search of the pid's rows.
    fn put(&mut self, sfx: &[u8], val: &[u8], trailer: &[u8]) -> Put {
        match self {
            Slot::Empty => {
                let v = make_val(val, trailer);
                *self = Slot::One(RamKey::from(sfx), v.clone());
                Put {
                    current: v,
                    replaced: None,
                    added: true,
                }
            }
            Slot::One(k, cur) if k.bytes() == sfx => {
                let replaced = overwrite(cur, val, trailer);
                Put {
                    current: cur.clone(),
                    replaced,
                    added: false,
                }
            }
            Slot::One(..) => {
                let Slot::One(k0, v0) = std::mem::take(self) else {
                    unreachable!("matched One above")
                };
                let v = make_val(val, trailer);
                let k = RamKey::from(sfx);
                let rows = if k0.bytes() < sfx {
                    vec![(k0, v0), (k, v.clone())]
                } else {
                    vec![(k, v.clone()), (k0, v0)]
                };
                *self = Slot::Many(rows);
                Put {
                    current: v,
                    replaced: None,
                    added: true,
                }
            }
            Slot::Many(rows) => match rows.binary_search_by(|(k, _)| k.bytes().cmp(sfx)) {
                Ok(i) => {
                    let cur = &mut rows[i].1;
                    let replaced = overwrite(cur, val, trailer);
                    Put {
                        current: cur.clone(),
                        replaced,
                        added: false,
                    }
                }
                Err(at) => {
                    let v = make_val(val, trailer);
                    if rows.len() >= MANY_MAX {
                        let mut m: BTreeMap<RamKey, RamVal> =
                            std::mem::take(rows).into_iter().collect();
                        m.insert(RamKey::from(sfx), v.clone());
                        *self = Slot::Tree(m);
                    } else {
                        // A pid's rows are few and rarely added (its counters,
                        // its groups' cursors): exact capacity, not doubling.
                        rows.reserve_exact(1);
                        rows.insert(at, (RamKey::from(sfx), v.clone()));
                    }
                    Put {
                        current: v,
                        replaced: None,
                        added: true,
                    }
                }
            },
            Slot::Tree(m) => TreeRows::put_in_map(m, sfx, val, trailer),
        }
    }

    /// Insert or replace a row with a value as it is (load, tests).
    fn put_arc(&mut self, sfx: &[u8], v: RamVal) -> Option<RamVal> {
        match self {
            Slot::Empty => {
                *self = Slot::One(RamKey::from(sfx), v);
                None
            }
            Slot::One(k, cur) if k.bytes() == sfx => Some(std::mem::replace(cur, v)),
            Slot::One(..) => {
                let Slot::One(k0, v0) = std::mem::take(self) else {
                    unreachable!("matched One above")
                };
                let k = RamKey::from(sfx);
                let rows = if k0.bytes() < sfx {
                    vec![(k0, v0), (k, v)]
                } else {
                    vec![(k, v), (k0, v0)]
                };
                *self = Slot::Many(rows);
                None
            }
            Slot::Many(rows) => match rows.binary_search_by(|(k, _)| k.bytes().cmp(sfx)) {
                Ok(i) => Some(std::mem::replace(&mut rows[i].1, v)),
                Err(at) => {
                    if rows.len() >= MANY_MAX {
                        let mut m: BTreeMap<RamKey, RamVal> =
                            std::mem::take(rows).into_iter().collect();
                        m.insert(RamKey::from(sfx), v);
                        *self = Slot::Tree(m);
                    } else {
                        rows.reserve_exact(1);
                        rows.insert(at, (RamKey::from(sfx), v));
                    }
                    None
                }
            },
            Slot::Tree(m) => m.insert(RamKey::from(sfx), v),
        }
    }

    fn remove(&mut self, sfx: &[u8]) -> Option<RamVal> {
        match self {
            Slot::Empty => None,
            Slot::One(k, _) => {
                if k.bytes() != sfx {
                    return None;
                }
                let Slot::One(_, v) = std::mem::take(self) else {
                    unreachable!("matched One above")
                };
                Some(v)
            }
            Slot::Many(rows) => {
                let i = rows.binary_search_by(|(k, _)| k.bytes().cmp(sfx)).ok()?;
                let (_, v) = rows.remove(i);
                if rows.len() == 1 {
                    let (k, v1) = rows.pop().expect("one row left");
                    *self = Slot::One(k, v1);
                }
                Some(v)
            }
            Slot::Tree(m) => {
                let v = m.remove(sfx)?;
                if m.len() <= MANY_MAX / 2 {
                    let rows: Vec<(RamKey, RamVal)> = std::mem::take(m).into_iter().collect();
                    *self = Slot::Many(rows);
                }
                Some(v)
            }
        }
    }

    /// The rows with a suffix inside `(lo, hi)`, in order (reverse order with
    /// `rev`), at most `limit` of them.
    fn each(
        &self,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        rev: bool,
        limit: usize,
        f: &mut dyn FnMut(&RamKey, &RamVal),
    ) {
        match self {
            Slot::Empty => {}
            Slot::One(k, v) => {
                if limit > 0 && above_lo(k, lo) && below_hi(k, hi) {
                    f(k, v)
                }
            }
            Slot::Many(rows) => {
                let a = match lo {
                    Bound::Unbounded => 0,
                    Bound::Included(b) => rows.partition_point(|(k, _)| k.bytes() < b),
                    Bound::Excluded(b) => rows.partition_point(|(k, _)| k.bytes() <= b),
                };
                let z = match hi {
                    Bound::Unbounded => rows.len(),
                    Bound::Included(b) => rows.partition_point(|(k, _)| k.bytes() <= b),
                    Bound::Excluded(b) => rows.partition_point(|(k, _)| k.bytes() < b),
                };
                if a >= z {
                    return;
                }
                if rev {
                    for (k, v) in rows[a..z].iter().rev().take(limit) {
                        f(k, v)
                    }
                } else {
                    for (k, v) in rows[a..z].iter().take(limit) {
                        f(k, v)
                    }
                }
            }
            Slot::Tree(m) => {
                if !range_is_walkable(lo, hi) {
                    return;
                }
                let it = m.range::<[u8], _>((lo, hi));
                if rev {
                    for (k, v) in it.rev().take(limit) {
                        f(k, v)
                    }
                } else {
                    for (k, v) in it.take(limit) {
                        f(k, v)
                    }
                }
            }
        }
    }

    /// Every row, in order, consuming the slot (clear, the tests).
    fn into_rows(self) -> Vec<(RamKey, RamVal)> {
        match self {
            Slot::Empty => Vec::new(),
            Slot::One(k, v) => vec![(k, v)],
            Slot::Many(rows) => rows,
            Slot::Tree(m) => m.into_iter().collect(),
        }
    }
}

/// `SLOTS` consecutive local pids of one stripe. The slots are one heap
/// block of exactly `SLOTS × 48` = 768 bytes — an allocator size class, which
/// a count stored beside them would push into the next (896: 8 bytes more
/// per pid in every dense keyspace) — and the count lives in the map entry.
struct Block {
    slots: Box<[Slot; SLOTS]>,
    /// Slots that hold a row.
    used: u16,
}

impl Block {
    fn new() -> Block {
        Block {
            slots: Box::new(std::array::from_fn(|_| Slot::Empty)),
            used: 0,
        }
    }
}

/// What one stripe's lock guards: its blocks (by block row), its rows' dirty
/// set, and a row count.
#[derive(Default)]
struct Stripe {
    blocks: HashMap<u64, Block, FxBuild>,
    dirty: DirtyMap,
    rows: usize,
}

/// Where a bound falls among the dense keys: the first (or last) pid it lets
/// in, and the bound on THAT pid's suffixes (every other pid of the range is
/// whole).
type Cut<'b> = Option<(u64, Bound<&'b [u8]>)>;

/// [`Layout::Dense`]. See the module header.
pub(crate) struct DenseTable {
    lead: &'static [u8],
    stripes: Box<[Padded<RwLock<Stripe>>]>,
    /// Block row → the stripes holding a block for it (a bit per stripe): the
    /// order a scan walks the table in, without a lock per pid. Written under
    /// the stripe's write lock (lock order: stripe, then this); a scan never
    /// holds it while it takes a stripe lock.
    rows_index: RwLock<BTreeMap<u64, u64>>,
    /// Keys that are not `lead ‖ pid ‖ suffix`.
    overflow: RwLock<TreeRows>,
    /// `overflow`'s row count, written under its write lock: a scan skips the
    /// overflow's lock while it is empty (it is, for every key a caller of
    /// this build writes), so scans of different partitions share no lock.
    overflow_rows: AtomicUsize,
    empty_fast_path: bool,
    /// Rows in the stripes and the overflow, kept only with
    /// `empty_fast_path`.
    live: AtomicUsize,
}

impl DenseTable {
    fn new(lead: &'static [u8], empty_fast_path: bool) -> DenseTable {
        DenseTable {
            lead,
            stripes: (0..STRIPES)
                .map(|_| Padded(RwLock::new(Stripe::default())))
                .collect(),
            rows_index: RwLock::new(BTreeMap::new()),
            overflow: RwLock::new(TreeRows::default()),
            overflow_rows: AtomicUsize::new(0),
            empty_fast_path,
            live: AtomicUsize::new(0),
        }
    }

    /// Bulk load, rows in key order (off an LMDB cursor).
    fn load(lead: &'static [u8], empty_fast_path: bool, rows: Vec<(RamKey, RamVal)>) -> DenseTable {
        let t = DenseTable::new(lead, empty_fast_path);
        let n = rows.len();
        {
            let mut stripes: Vec<RwLockWriteGuard<'_, Stripe>> =
                t.stripes.iter().map(|s| wr(&s.0)).collect();
            let mut ov = wr(&t.overflow);
            let mut index = wr(&t.rows_index);
            for (k, v) in rows {
                match t.split(&k) {
                    Some((pid, sfx)) => {
                        let s = stripe_of(pid);
                        let st = &mut *stripes[s];
                        let row = pid >> ROW_SHIFT;
                        let block = st.blocks.entry(row).or_insert_with(|| {
                            *index.entry(row).or_insert(0) |= 1u64 << s;
                            Block::new()
                        });
                        let slot = &mut block.slots[slot_of(pid)];
                        if slot.is_empty() {
                            block.used += 1;
                        }
                        let old = slot.put_arc(sfx, v);
                        debug_assert!(old.is_none(), "a key loaded twice");
                        st.rows += 1;
                    }
                    None => {
                        ov.map.insert(k, v);
                    }
                }
            }
        }
        if empty_fast_path {
            t.live.store(n, Ordering::Relaxed);
        }
        let ov = rd(&t.overflow).map.len();
        t.overflow_rows.store(ov, Ordering::Relaxed);
        t
    }

    /// `(pid, suffix)` of a dense key, `None` for an overflow key.
    #[inline]
    fn split<'k>(&self, key: &'k [u8]) -> Option<(u64, &'k [u8])> {
        let l = self.lead.len();
        if key.len() < l + 8 || !key.starts_with(self.lead) {
            return None;
        }
        let pid = u64::from_be_bytes(key[l..l + 8].try_into().expect("8 bytes"));
        Some((pid, &key[l + 8..]))
    }

    #[inline]
    fn stripe(&self, pid: u64) -> &RwLock<Stripe> {
        &self.stripes[stripe_of(pid)].0
    }

    #[inline]
    fn is_empty_fast(&self) -> bool {
        self.empty_fast_path && self.live.load(Ordering::Relaxed) == 0
    }

    #[inline]
    fn live_add(&self) {
        if self.empty_fast_path {
            self.live.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    fn live_sub(&self, n: usize) {
        if self.empty_fast_path && n > 0 {
            self.live.fetch_sub(n, Ordering::Relaxed);
        }
    }

    fn with<T>(&self, key: &[u8], f: impl FnOnce(&RamVal) -> T) -> Option<T> {
        if self.is_empty_fast() {
            return None;
        }
        match self.split(key) {
            None if self.overflow_rows.load(Ordering::Relaxed) == 0 => None,
            None => rd(&self.overflow).map.get(key).map(f),
            Some((pid, sfx)) => {
                let g = rd(self.stripe(pid));
                let b = g.blocks.get(&(pid >> ROW_SHIFT))?;
                b.slots[slot_of(pid)].get(sfx).map(f)
            }
        }
    }

    fn put(&self, key: &[u8], val: &[u8], trailer: &[u8]) -> Option<RamVal> {
        let Some((pid, sfx)) = self.split(key) else {
            let mut ov = wr(&self.overflow);
            let p = ov.put(key, val, trailer);
            self.overflow_rows.store(ov.map.len(), Ordering::Relaxed);
            drop(ov);
            if p.added {
                self.live_add();
            }
            return p.replaced;
        };
        let s = stripe_of(pid);
        let row = pid >> ROW_SHIFT;
        let mut g = wr(&self.stripes[s].0);
        let Stripe {
            blocks,
            dirty,
            rows,
        } = &mut *g;
        let mut new_block = false;
        let block = blocks.entry(row).or_insert_with(|| {
            new_block = true;
            Block::new()
        });
        let slot = &mut block.slots[slot_of(pid)];
        let was_empty = slot.is_empty();
        let p = match dirty.get_mut(key) {
            Some(d) => {
                // The dirty set holds the same `Arc` as the slot: let go of it
                // first, so the slot's copy can be rewritten in place.
                drop(d.take());
                let p = slot.put(sfx, val, trailer);
                *d = Some(p.current.clone());
                p
            }
            None => {
                let p = slot.put(sfx, val, trailer);
                dirty.insert(RamKey::from(key), Some(p.current.clone()));
                p
            }
        };
        if p.added {
            *rows += 1;
            if was_empty {
                block.used += 1;
            }
            self.live_add();
        }
        if new_block {
            *wr(&self.rows_index).entry(row).or_insert(0) |= 1u64 << s;
        }
        drop(g);
        p.replaced
    }

    fn put_arc(&self, key: &[u8], stored: RamVal) -> Option<RamVal> {
        let Some((pid, sfx)) = self.split(key) else {
            let mut ov = wr(&self.overflow);
            let (old, added) = ov.put_arc(key, stored);
            self.overflow_rows.store(ov.map.len(), Ordering::Relaxed);
            drop(ov);
            if added {
                self.live_add();
            }
            return old;
        };
        let s = stripe_of(pid);
        let row = pid >> ROW_SHIFT;
        let mut g = wr(&self.stripes[s].0);
        let st = &mut *g;
        let mut new_block = false;
        let block = st.blocks.entry(row).or_insert_with(|| {
            new_block = true;
            Block::new()
        });
        let slot = &mut block.slots[slot_of(pid)];
        let was_empty = slot.is_empty();
        let old = slot.put_arc(sfx, stored.clone());
        if old.is_none() {
            st.rows += 1;
            if was_empty {
                block.used += 1;
            }
            self.live_add();
        }
        let old_dirty = st.dirty.insert(RamKey::from(key), Some(stored));
        if new_block {
            *wr(&self.rows_index).entry(row).or_insert(0) |= 1u64 << s;
        }
        drop(g);
        drop(old_dirty);
        old
    }

    fn upsert(
        &self,
        key: &[u8],
        scratch: &mut Vec<u8>,
        f: impl FnOnce(Option<&[u8]>, &mut Vec<u8>) -> bool,
    ) -> Upserted {
        let Some((pid, sfx)) = self.split(key) else {
            let mut ov = wr(&self.overflow);
            let u = ov.upsert(key, scratch, f);
            self.overflow_rows.store(ov.map.len(), Ordering::Relaxed);
            drop(ov);
            if u.added {
                self.live_add();
            }
            return u;
        };
        let s = stripe_of(pid);
        let row = pid >> ROW_SHIFT;
        let mut g = wr(&self.stripes[s].0);
        let Stripe {
            blocks,
            dirty,
            rows,
        } = &mut *g;
        if let Some(v) = blocks
            .get_mut(&row)
            .and_then(|b| b.slots[slot_of(pid)].get_mut(sfx))
        {
            return upsert_existing(v, dirty, key, scratch, f);
        }
        scratch.clear();
        if !f(None, scratch) {
            return Upserted::default();
        }
        let mut new_block = false;
        let block = blocks.entry(row).or_insert_with(|| {
            new_block = true;
            Block::new()
        });
        let slot = &mut block.slots[slot_of(pid)];
        let was_empty = slot.is_empty();
        let p = slot.put(sfx, scratch, &[]);
        dirty.insert(RamKey::from(key), Some(p.current.clone()));
        *rows += 1;
        if was_empty {
            block.used += 1;
        }
        self.live_add();
        if new_block {
            *wr(&self.rows_index).entry(row).or_insert(0) |= 1u64 << s;
        }
        drop(g);
        Upserted {
            existed: false,
            written: true,
            replaced: p.replaced,
            added: true,
        }
    }

    fn remove(&self, key: &[u8]) -> Option<RamVal> {
        let Some((pid, sfx)) = self.split(key) else {
            let mut ov = wr(&self.overflow);
            let v = ov.remove(key);
            self.overflow_rows.store(ov.map.len(), Ordering::Relaxed);
            drop(ov);
            if v.is_some() {
                self.live_sub(1);
            }
            return v;
        };
        let s = stripe_of(pid);
        let row = pid >> ROW_SHIFT;
        let mut g = wr(&self.stripes[s].0);
        let st = &mut *g;
        let block = st.blocks.get_mut(&row)?;
        let slot = &mut block.slots[slot_of(pid)];
        let v = slot.remove(sfx)?;
        st.rows -= 1;
        let mut freed: Option<Block> = None;
        if slot.is_empty() {
            block.used -= 1;
            if block.used == 0 {
                freed = st.blocks.remove(&row);
                let mut index = wr(&self.rows_index);
                if let Some(mask) = index.get_mut(&row) {
                    *mask &= !(1u64 << s);
                    if *mask == 0 {
                        index.remove(&row);
                    }
                }
            }
        }
        // Already dirty: `insert` keeps the set's key and replaces the value.
        match st.dirty.get_mut(key) {
            Some(d) => *d = None,
            None => {
                st.dirty.insert(RamKey::from(key), None);
            }
        }
        drop(g);
        drop(freed);
        self.live_sub(1);
        Some(v)
    }

    fn clear(&self) {
        let mut scratch = Vec::with_capacity(self.lead.len() + 8 + RAM_KEY_INLINE);
        let mut removed = 0usize;
        for (s, stripe) in self.stripes.iter().enumerate() {
            let old = {
                let mut g = wr(&stripe.0);
                let st = &mut *g;
                let blocks = std::mem::take(&mut st.blocks);
                let mut rows_out: Vec<(RamKey, RamVal)> = Vec::new();
                for (row, block) in blocks {
                    let slots: [Slot; SLOTS] = *block.slots;
                    for (idx, slot) in slots.into_iter().enumerate() {
                        let pid = pid_at(row, idx, s);
                        for (sfx, v) in slot.into_rows() {
                            scratch.clear();
                            scratch.extend_from_slice(self.lead);
                            scratch.extend_from_slice(&pid.to_be_bytes());
                            scratch.extend_from_slice(&sfx);
                            st.dirty.insert(RamKey::from(&scratch[..]), None);
                            rows_out.push((sfx, v));
                        }
                    }
                }
                // The stripe's blocks are gone: so is their index entry.
                let mut index = wr(&self.rows_index);
                index.retain(|_, mask| {
                    *mask &= !(1u64 << s);
                    *mask != 0
                });
                drop(index);
                removed += st.rows;
                st.rows = 0;
                rows_out
            };
            drop(old);
        }
        let mut ov = wr(&self.overflow);
        let old = ov.clear();
        self.overflow_rows.store(0, Ordering::Relaxed);
        drop(ov);
        removed += old.len();
        drop(old);
        self.live_sub(removed);
    }

    fn take_dirty(&self, out: &mut Vec<DirtyMap>) {
        for stripe in self.stripes.iter() {
            take_one(&stripe.0, |st: &mut Stripe| &mut st.dirty, out);
        }
        take_one(&self.overflow, |t: &mut TreeRows| &mut t.dirty, out);
    }

    fn restore_dirty(&self, k: RamKey) {
        match self.split(&k) {
            None => wr(&self.overflow).restore_dirty(k),
            Some((pid, sfx)) => {
                let mut g = wr(self.stripe(pid));
                let st = &mut *g;
                if st.dirty.contains_key(&*k) {
                    return;
                }
                let v = st
                    .blocks
                    .get(&(pid >> ROW_SHIFT))
                    .and_then(|b| b.slots[slot_of(pid)].get(sfx).cloned());
                st.dirty.insert(k, v);
            }
        }
    }

    fn len(&self) -> usize {
        let dense: usize = self.stripes.iter().map(|s| rd(&s.0).rows).sum();
        dense + self.overflow_rows.load(Ordering::Relaxed)
    }

    fn dirty_len(&self) -> usize {
        let dense: usize = self.stripes.iter().map(|s| rd(&s.0).dirty.len()).sum();
        dense + rd(&self.overflow).dirty.len()
    }

    /// Where a LOWER bound falls among the dense keys (module header).
    fn lo_cut<'b>(&self, lo: Bound<&'b [u8]>) -> Cut<'b> {
        let (b, incl) = match lo {
            Bound::Unbounded => return Some((0, Bound::Unbounded)),
            Bound::Included(b) => (b, true),
            Bound::Excluded(b) => (b, false),
        };
        let l = self.lead.len();
        let n = l.min(b.len());
        match self.lead[..n].cmp(&b[..n]) {
            // Every dense key sorts above the bound.
            std::cmp::Ordering::Greater => return Some((0, Bound::Unbounded)),
            // Every dense key sorts below it.
            std::cmp::Ordering::Less => return None,
            std::cmp::Ordering::Equal => {}
        }
        if b.len() < l {
            // The bound is a proper prefix of the lead: every dense key is
            // longer and starts with it.
            return Some((0, Bound::Unbounded));
        }
        let rest = &b[l..];
        if rest.len() >= 8 {
            let p = u64::from_be_bytes(rest[..8].try_into().expect("8 bytes"));
            let s = &rest[8..];
            Some(match (incl, s.is_empty()) {
                // Every suffix is ≥ the empty one.
                (true, true) => (p, Bound::Unbounded),
                (true, false) => (p, Bound::Included(s)),
                (false, _) => (p, Bound::Excluded(s)),
            })
        } else {
            // `lead ‖ P ‖ S` (longer than the bound) sorts above it exactly
            // when `P ≥ rest ‖ 0…0`.
            let mut a = [0u8; 8];
            a[..rest.len()].copy_from_slice(rest);
            Some((u64::from_be_bytes(a), Bound::Unbounded))
        }
    }

    /// Where an UPPER bound falls among the dense keys (module header).
    fn hi_cut<'b>(&self, hi: Bound<&'b [u8]>) -> Cut<'b> {
        let (b, incl) = match hi {
            Bound::Unbounded => return Some((u64::MAX, Bound::Unbounded)),
            Bound::Included(b) => (b, true),
            Bound::Excluded(b) => (b, false),
        };
        let l = self.lead.len();
        let n = l.min(b.len());
        match self.lead[..n].cmp(&b[..n]) {
            std::cmp::Ordering::Less => return Some((u64::MAX, Bound::Unbounded)),
            std::cmp::Ordering::Greater => return None,
            std::cmp::Ordering::Equal => {}
        }
        if b.len() < l {
            return None;
        }
        let rest = &b[l..];
        if rest.len() >= 8 {
            let p = u64::from_be_bytes(rest[..8].try_into().expect("8 bytes"));
            let s = &rest[8..];
            match (incl, s.is_empty()) {
                // Nothing sorts below the empty suffix: the range ends with
                // the pid before. This is the upper bound of every prefix
                // scan of one pid (`prefix_end(pid)` is the next pid), so it
                // is what keeps such a scan on its one slot.
                (false, true) => p.checked_sub(1).map(|q| (q, Bound::Unbounded)),
                (false, false) => Some((p, Bound::Excluded(s))),
                (true, _) => Some((p, Bound::Included(s))),
            }
        } else {
            // `lead ‖ P ‖ S` sorts below the bound exactly when
            // `P < rest ‖ 0…0`.
            let mut a = [0u8; 8];
            a[..rest.len()].copy_from_slice(rest);
            u64::from_be_bytes(a)
                .checked_sub(1)
                .map(|p| (p, Bound::Unbounded))
        }
    }

    fn copy_range(
        &self,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        rev: bool,
        take: usize,
        out: &mut ScanBuf,
    ) {
        if take == 0 || self.is_empty_fast() {
            return;
        }
        let start = out.len();
        self.copy_dense(lo, hi, rev, take, out);
        if self.overflow_rows.load(Ordering::Relaxed) == 0 {
            return;
        }
        let ov = rd(&self.overflow);
        if ov.map.is_empty() {
            return;
        }
        let dense = out.len() - start;
        ov.copy_range(lo, hi, rev, take, out);
        drop(ov);
        if dense > 0 && out.len() - start > dense {
            // Both sources answered: one order, then the first `take`.
            out.sort_by_key(start, rev);
        }
        out.truncate(start + take);
    }

    fn copy_dense(
        &self,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        rev: bool,
        take: usize,
        out: &mut ScanBuf,
    ) {
        let (Some((pa, sa)), Some((pb, sb))) = (self.lo_cut(lo), self.hi_cut(hi)) else {
            return;
        };
        if pa > pb {
            return;
        }
        let lead = self.lead;
        let start = out.len();
        if pa == pb {
            // One pid (a prefix scan of one partition): its stripe, its slot.
            let g = rd(self.stripe(pa));
            if let Some(b) = g.blocks.get(&(pa >> ROW_SHIFT)) {
                let pid_be = pa.to_be_bytes();
                b.slots[slot_of(pa)].each(sa, sb, rev, take, &mut |sfx, v| {
                    out.push(pa, [lead, &pid_be, sfx], v)
                });
            }
            return;
        }
        // Windows of `k` slot indices (`k × STRIPES` pids) inside one block
        // row: every stripe holding the row is locked once per window, and the
        // window's rows are put in key order by pid.
        let k = take.div_ceil(STRIPES).clamp(1, SLOTS);
        let (ra, rb) = (pa >> ROW_SHIFT, pb >> ROW_SHIFT);
        let mut next: Option<u64> = Some(if rev { rb } else { ra });
        while let Some(from) = next {
            // A batch of the rows present, copied out of the index so no stripe
            // lock is ever taken while it is held.
            let batch: smallvec::SmallVec<[(u64, u64); 32]> = {
                let index = rd(&self.rows_index);
                if rev {
                    index
                        .range(ra..=from)
                        .rev()
                        .take(32)
                        .map(|(r, m)| (*r, *m))
                        .collect()
                } else {
                    index
                        .range(from..=rb)
                        .take(32)
                        .map(|(r, m)| (*r, *m))
                        .collect()
                }
            };
            next = match batch.last() {
                Some((r, _)) if batch.len() == 32 => {
                    if rev {
                        r.checked_sub(1).filter(|r| *r >= ra)
                    } else {
                        r.checked_add(1).filter(|r| *r <= rb)
                    }
                }
                _ => None,
            };
            for (row, mask) in batch {
                let base = row << ROW_SHIFT;
                let plo = pa.max(base);
                let phi = pb.min(base | ((STRIPES * SLOTS) as u64 - 1));
                let (ilo, ihi) = (slot_of(plo), slot_of(phi));
                let mut w = if rev { ihi as isize } else { ilo as isize };
                loop {
                    let (wlo, whi) = if rev {
                        ((w - k as isize + 1).max(ilo as isize) as usize, w as usize)
                    } else {
                        (w as usize, (w as usize + k - 1).min(ihi))
                    };
                    let wstart = out.len();
                    self.copy_window(row, mask, wlo, whi, (pa, sa), (pb, sb), rev, take, out);
                    if out.len() > wstart + 1 {
                        out.order_window(wstart, rev);
                    }
                    if out.len() - start >= take {
                        out.truncate(start + take);
                        return;
                    }
                    if rev {
                        w = wlo as isize - 1;
                        if w < ilo as isize {
                            break;
                        }
                    } else {
                        w = whi as isize + 1;
                        if w > ihi as isize {
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Copy the rows of slot indices `wlo..=whi` of block row `row`, from every
    /// stripe in `mask`, keeping to the pid range `[pa, pb]` and the suffix
    /// bounds at its two ends.
    #[allow(clippy::too_many_arguments)]
    fn copy_window(
        &self,
        row: u64,
        mut mask: u64,
        wlo: usize,
        whi: usize,
        (pa, sa): (u64, Bound<&[u8]>),
        (pb, sb): (u64, Bound<&[u8]>),
        rev: bool,
        take: usize,
        out: &mut ScanBuf,
    ) {
        let lead = self.lead;
        // Where each window position's rows land: positions are
        // `(idx - wlo) × STRIPES + stripe`, which is pid order.
        out.spans.clear();
        out.spans.resize((whi - wlo + 1) * STRIPES, (0, 0));
        while mask != 0 {
            let s = mask.trailing_zeros() as usize;
            mask &= mask - 1;
            let g = rd(&self.stripes[s].0);
            let Some(b) = g.blocks.get(&row) else {
                continue;
            };
            if b.used == 0 {
                continue;
            }
            for idx in wlo..=whi {
                let slot = &b.slots[idx];
                if slot.is_empty() {
                    continue;
                }
                let pid = pid_at(row, idx, s);
                if pid < pa || pid > pb {
                    continue;
                }
                let lo = if pid == pa { sa } else { Bound::Unbounded };
                let hi = if pid == pb { sb } else { Bound::Unbounded };
                let pid_be = pid.to_be_bytes();
                let first = out.rows.len();
                slot.each(lo, hi, rev, take, &mut |sfx, val| {
                    out.push(pid, [lead, &pid_be, sfx], val)
                });
                out.spans[(idx - wlo) * STRIPES + s] = (first, out.rows.len() - first);
            }
        }
    }
}

/// Swap one lock's dirty set for an empty one sized like it (allocated outside
/// the lock), and keep the taken set if it held anything.
fn take_one<T>(lock: &RwLock<T>, field: impl Fn(&mut T) -> &mut DirtyMap, out: &mut Vec<DirtyMap>) {
    let n = field(&mut wr(lock)).len();
    if n == 0 {
        return;
    }
    let fresh = DirtyMap::with_capacity_and_hasher(n, FxBuild::default());
    let taken = std::mem::replace(field(&mut wr(lock)), fresh);
    if !taken.is_empty() {
        out.push(taken);
    }
}

// ---------------------------------------------------------------------------
// The prefix container
// ---------------------------------------------------------------------------

/// Stripes of a prefix table.
const PREFIX_STRIPES: usize = 16;

/// Where the first `names` escaped, terminated names of `key` end
/// ([`super::keys::push_name`]); `None` when `key` holds fewer, or a byte
/// sequence no name encoding produces.
fn names_end(key: &[u8], names: usize) -> Option<usize> {
    let mut at = 0;
    for _ in 0..names {
        loop {
            let z = at + key.get(at..)?.iter().position(|b| *b == 0)?;
            match key.get(z + 1)? {
                0x00 => {
                    at = z + 2;
                    break;
                }
                0xFF => at = z + 2,
                _ => return None,
            }
        }
    }
    Some(at)
}

#[inline]
fn fx(bytes: &[u8]) -> usize {
    use std::hash::{BuildHasher, Hasher};
    let mut h = FxBuild::default().build_hasher();
    h.write(bytes);
    h.finish() as usize
}

/// How a prefix table spreads its rows over its stripes ([`PrefixTable`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StripeBy {
    /// A prefix's rows in one stripe, chosen by the prefix: a scan of one
    /// prefix reads one sub-table.
    Prefix,
    /// By the pid the suffix begins with (its first 8 bytes, big-endian):
    /// `pending`'s and `queue_partitions`' trailing pid, `leases_by_worker`'s
    /// pid. The sharded apply's writers own disjoint pids, so each writes its
    /// own stripes (`pid % 16`: with 4 writers, 4 stripes each) and no two
    /// meet on a lock; a scan of one prefix merges its stripes.
    SuffixPid,
    /// By a hash of the suffix: a prefix's rows spread evenly (the partition
    /// names of one queue; the request ids, which have no prefix at all).
    SuffixHash,
}

/// One stripe of a prefix table: its sub-tables (one per prefix it holds rows
/// of), their rows' dirty set, a row count.
#[derive(Default)]
struct PrefixStripe {
    /// Prefix → its rows in this stripe, by suffix.
    groups: HashMap<RamKey, BTreeMap<RamKey, RamVal>, FxBuild>,
    dirty: DirtyMap,
    rows: usize,
}

/// Where a bound falls among the prefixes: the bound on the prefix itself,
/// and — when the bound names a whole prefix — the bound on THAT prefix's
/// suffixes (every other prefix of the range is whole).
type PrefixCut<'b> = (Bound<&'b [u8]>, Option<(&'b [u8], Bound<&'b [u8]>)>);

/// [`Layout::Prefixed`]: keys that begin with `names` escaped names —
/// `(tenant, queue[, group])`, a worker's name, or nothing at all — held as
/// prefix → sub-table by the rest, so a queue's 36-byte tenant id and its name
/// are stored once per stripe and queue rather than once per row, and a row's
/// own key is its short suffix. The rows are spread over [`PREFIX_STRIPES`]
/// stripes by [`StripeBy`]. Keys with fewer names (or bytes no name encoding
/// produces) live in an ordered overflow.
///
/// The order is the bytes' order: the names are self-delimiting, so two
/// different prefixes are never one a prefix of the other, and `P1 ‖ S1 <
/// P2 ‖ S2` exactly when `P1 < P2`, or `P1 == P2` and `S1 < S2`. A scan walks
/// an ordered index of the prefixes (each with the stripes that hold it) one
/// prefix at a time, merging the prefix's stripes in suffix order, and merges
/// the overflow in.
pub(crate) struct PrefixTable {
    names: usize,
    stripe_by: StripeBy,
    stripes: Box<[Padded<RwLock<PrefixStripe>>]>,
    /// Every prefix holding a row → the stripes holding its rows (a bit
    /// each), in order. Written under the stripe's write lock (lock order:
    /// stripe, then this); a scan never holds it while it takes a stripe lock.
    index: RwLock<BTreeMap<RamKey, u16>>,
    overflow: RwLock<TreeRows>,
    /// As [`DenseTable`]'s: a scan skips an empty overflow's lock.
    overflow_rows: AtomicUsize,
}

/// A prefix's rows arriving in key order at the load, collected per stripe.
fn flush_prefix(
    p: RamKey,
    per: &mut [Vec<(RamKey, RamVal)>],
    stripes: &mut [RwLockWriteGuard<'_, PrefixStripe>],
    index: &mut BTreeMap<RamKey, u16>,
) {
    let mut mask = 0u16;
    for (s, rows) in per.iter_mut().enumerate() {
        if rows.is_empty() {
            continue;
        }
        mask |= 1 << s;
        let st = &mut *stripes[s];
        st.rows += rows.len();
        // In suffix order: the collect is a bulk build.
        st.groups
            .insert(p.clone(), std::mem::take(rows).into_iter().collect());
    }
    if mask != 0 {
        index.insert(p, mask);
    }
}

impl PrefixTable {
    fn new(names: usize, stripe_by: StripeBy) -> PrefixTable {
        PrefixTable {
            names,
            stripe_by,
            stripes: (0..PREFIX_STRIPES)
                .map(|_| Padded(RwLock::new(PrefixStripe::default())))
                .collect(),
            index: RwLock::new(BTreeMap::new()),
            overflow: RwLock::new(TreeRows::default()),
            overflow_rows: AtomicUsize::new(0),
        }
    }

    fn load(names: usize, stripe_by: StripeBy, rows: Vec<(RamKey, RamVal)>) -> PrefixTable {
        let t = PrefixTable::new(names, stripe_by);
        {
            let mut stripes: Vec<RwLockWriteGuard<'_, PrefixStripe>> =
                t.stripes.iter().map(|s| wr(&s.0)).collect();
            let mut index = wr(&t.index);
            let mut ov = wr(&t.overflow);
            // Rows arrive in key order, so a prefix's rows arrive together and
            // in suffix order.
            let mut per: Vec<Vec<(RamKey, RamVal)>> =
                (0..PREFIX_STRIPES).map(|_| Vec::new()).collect();
            let mut cur: Option<RamKey> = None;
            for (k, v) in rows {
                let Some(pl) = names_end(&k, names) else {
                    ov.map.insert(k, v);
                    continue;
                };
                let (p, sfx) = k.split_at(pl);
                if cur.as_ref().is_none_or(|c| c.bytes() != p) {
                    if let Some(c) = cur.take() {
                        flush_prefix(c, &mut per, &mut stripes, &mut index);
                    }
                    cur = Some(RamKey::from(p));
                }
                per[t.stripe_for(p, sfx)].push((RamKey::from(sfx), v));
            }
            if let Some(c) = cur.take() {
                flush_prefix(c, &mut per, &mut stripes, &mut index);
            }
        }
        let ov = rd(&t.overflow).map.len();
        t.overflow_rows.store(ov, Ordering::Relaxed);
        t
    }

    #[inline]
    fn stripe_for(&self, prefix: &[u8], sfx: &[u8]) -> usize {
        let h = match self.stripe_by {
            StripeBy::Prefix => fx(prefix),
            StripeBy::SuffixPid if sfx.len() >= 8 => {
                u64::from_be_bytes(sfx[..8].try_into().expect("8 bytes")) as usize
            }
            StripeBy::SuffixPid | StripeBy::SuffixHash => fx(sfx),
        };
        h & (PREFIX_STRIPES - 1)
    }

    #[inline]
    fn split<'k>(&self, key: &'k [u8]) -> Option<(&'k [u8], &'k [u8])> {
        names_end(key, self.names).map(|pl| key.split_at(pl))
    }

    /// Record that stripe `s` now holds (or no longer holds) rows of `p`.
    fn index_set(&self, p: &[u8], s: usize, holds: bool) {
        let mut index = wr(&self.index);
        if holds {
            match index.get_mut(p) {
                Some(mask) => *mask |= 1 << s,
                None => {
                    index.insert(RamKey::from(p), 1 << s);
                }
            }
        } else if let Some(mask) = index.get_mut(p) {
            *mask &= !(1u16 << s);
            if *mask == 0 {
                index.remove(p);
            }
        }
    }

    fn with<T>(&self, key: &[u8], f: impl FnOnce(&RamVal) -> T) -> Option<T> {
        match self.split(key) {
            None if self.overflow_rows.load(Ordering::Relaxed) == 0 => None,
            None => rd(&self.overflow).map.get(key).map(f),
            Some((p, sfx)) => {
                let g = rd(&self.stripes[self.stripe_for(p, sfx)].0);
                g.groups.get(p)?.get(sfx).map(f)
            }
        }
    }

    fn put(&self, key: &[u8], val: &[u8], trailer: &[u8]) -> Option<RamVal> {
        let Some((p, sfx)) = self.split(key) else {
            let mut ov = wr(&self.overflow);
            let put = ov.put(key, val, trailer);
            self.overflow_rows.store(ov.map.len(), Ordering::Relaxed);
            return put.replaced;
        };
        let s = self.stripe_for(p, sfx);
        let mut g = wr(&self.stripes[s].0);
        let PrefixStripe {
            groups,
            dirty,
            rows,
        } = &mut *g;
        let mut new_group = false;
        let group = match groups.get_mut(p) {
            Some(g) => g,
            None => {
                new_group = true;
                groups.entry(RamKey::from(p)).or_default()
            }
        };
        let put = match dirty.get_mut(key) {
            Some(d) => {
                // As in the other containers: the dirty set's reference goes
                // first, so the row's value can be rewritten in place.
                drop(d.take());
                let put = TreeRows::put_in_map(group, sfx, val, trailer);
                *d = Some(put.current.clone());
                put
            }
            None => {
                let put = TreeRows::put_in_map(group, sfx, val, trailer);
                dirty.insert(RamKey::from(key), Some(put.current.clone()));
                put
            }
        };
        if put.added {
            *rows += 1;
        }
        if new_group {
            self.index_set(p, s, true);
        }
        drop(g);
        put.replaced
    }

    fn put_arc(&self, key: &[u8], stored: RamVal) -> Option<RamVal> {
        let Some((p, sfx)) = self.split(key) else {
            let mut ov = wr(&self.overflow);
            let old = ov.put_arc(key, stored).0;
            self.overflow_rows.store(ov.map.len(), Ordering::Relaxed);
            return old;
        };
        let s = self.stripe_for(p, sfx);
        let mut g = wr(&self.stripes[s].0);
        let st = &mut *g;
        let mut new_group = false;
        let group = match st.groups.get_mut(p) {
            Some(g) => g,
            None => {
                new_group = true;
                st.groups.entry(RamKey::from(p)).or_default()
            }
        };
        let old = group.insert(RamKey::from(sfx), stored.clone());
        if old.is_none() {
            st.rows += 1;
        }
        let old_dirty = st.dirty.insert(RamKey::from(key), Some(stored));
        if new_group {
            self.index_set(p, s, true);
        }
        drop(g);
        drop(old_dirty);
        old
    }

    fn upsert(
        &self,
        key: &[u8],
        scratch: &mut Vec<u8>,
        f: impl FnOnce(Option<&[u8]>, &mut Vec<u8>) -> bool,
    ) -> Upserted {
        let Some((p, sfx)) = self.split(key) else {
            let mut ov = wr(&self.overflow);
            let u = ov.upsert(key, scratch, f);
            self.overflow_rows.store(ov.map.len(), Ordering::Relaxed);
            return u;
        };
        let s = self.stripe_for(p, sfx);
        let mut g = wr(&self.stripes[s].0);
        let PrefixStripe {
            groups,
            dirty,
            rows,
        } = &mut *g;
        if let Some(v) = groups.get_mut(p).and_then(|g| g.get_mut(sfx)) {
            return upsert_existing(v, dirty, key, scratch, f);
        }
        scratch.clear();
        if !f(None, scratch) {
            return Upserted::default();
        }
        let mut new_group = false;
        let group = match groups.get_mut(p) {
            Some(g) => g,
            None => {
                new_group = true;
                groups.entry(RamKey::from(p)).or_default()
            }
        };
        let put = TreeRows::put_in_map(group, sfx, scratch, &[]);
        dirty.insert(RamKey::from(key), Some(put.current.clone()));
        *rows += 1;
        if new_group {
            self.index_set(p, s, true);
        }
        drop(g);
        Upserted {
            existed: false,
            written: true,
            replaced: put.replaced,
            added: true,
        }
    }

    fn remove(&self, key: &[u8]) -> Option<RamVal> {
        let Some((p, sfx)) = self.split(key) else {
            let mut ov = wr(&self.overflow);
            let v = ov.remove(key);
            self.overflow_rows.store(ov.map.len(), Ordering::Relaxed);
            return v;
        };
        let s = self.stripe_for(p, sfx);
        let mut g = wr(&self.stripes[s].0);
        let st = &mut *g;
        let group = st.groups.get_mut(p)?;
        let v = group.remove(sfx)?;
        st.rows -= 1;
        let mut freed = None;
        if group.is_empty() {
            freed = st.groups.remove(p);
            self.index_set(p, s, false);
        }
        match st.dirty.get_mut(key) {
            Some(d) => *d = None,
            None => {
                st.dirty.insert(RamKey::from(key), None);
            }
        }
        drop(g);
        drop(freed);
        Some(v)
    }

    fn clear(&self) {
        let mut scratch: Vec<u8> = Vec::new();
        for (s, stripe) in self.stripes.iter().enumerate() {
            let old = {
                let mut g = wr(&stripe.0);
                let st = &mut *g;
                let groups = std::mem::take(&mut st.groups);
                for (p, rows) in &groups {
                    for sfx in rows.keys() {
                        scratch.clear();
                        scratch.extend_from_slice(p);
                        scratch.extend_from_slice(sfx);
                        st.dirty.insert(RamKey::from(&scratch[..]), None);
                    }
                    self.index_set(p, s, false);
                }
                st.rows = 0;
                groups
            };
            drop(old);
        }
        let mut ov = wr(&self.overflow);
        let old = ov.clear();
        self.overflow_rows.store(0, Ordering::Relaxed);
        drop(ov);
        drop(old);
    }

    fn take_dirty(&self, out: &mut Vec<DirtyMap>) {
        for stripe in self.stripes.iter() {
            take_one(&stripe.0, |st: &mut PrefixStripe| &mut st.dirty, out);
        }
        take_one(&self.overflow, |t: &mut TreeRows| &mut t.dirty, out);
    }

    fn restore_dirty(&self, k: RamKey) {
        match self.split(&k) {
            None => wr(&self.overflow).restore_dirty(k),
            Some((p, sfx)) => {
                let mut g = wr(&self.stripes[self.stripe_for(p, sfx)].0);
                let st = &mut *g;
                if st.dirty.contains_key(&*k) {
                    return;
                }
                let v = st.groups.get(p).and_then(|g| g.get(sfx).cloned());
                st.dirty.insert(k, v);
            }
        }
    }

    fn len(&self) -> usize {
        let rows: usize = self.stripes.iter().map(|s| rd(&s.0).rows).sum();
        rows + self.overflow_rows.load(Ordering::Relaxed)
    }

    fn dirty_len(&self) -> usize {
        let d: usize = self.stripes.iter().map(|s| rd(&s.0).dirty.len()).sum();
        d + rd(&self.overflow).dirty.len()
    }

    /// Where a LOWER bound falls among the prefixes (the type header).
    fn lo_cut<'b>(&self, lo: Bound<&'b [u8]>) -> PrefixCut<'b> {
        let (b, incl) = match lo {
            Bound::Unbounded => return (Bound::Unbounded, None),
            Bound::Included(b) => (b, true),
            Bound::Excluded(b) => (b, false),
        };
        match self.split(b) {
            // Fewer names than a prefix: every prefix above the bound's bytes
            // is whole in the range (one that starts with them is above them).
            None => (Bound::Excluded(b), None),
            // Every suffix is ≥ the empty one.
            Some((p, s)) if incl && s.is_empty() => (Bound::Included(p), None),
            Some((p, s)) => (
                Bound::Included(p),
                Some((
                    p,
                    if incl {
                        Bound::Included(s)
                    } else {
                        Bound::Excluded(s)
                    },
                )),
            ),
        }
    }

    /// Where an UPPER bound falls among the prefixes.
    fn hi_cut<'b>(&self, hi: Bound<&'b [u8]>) -> PrefixCut<'b> {
        let (b, incl) = match hi {
            Bound::Unbounded => return (Bound::Unbounded, None),
            Bound::Included(b) => (b, true),
            Bound::Excluded(b) => (b, false),
        };
        match self.split(b) {
            None => (Bound::Excluded(b), None),
            // Nothing sorts below the empty suffix: the prefix is out.
            Some((p, s)) if !incl && s.is_empty() => (Bound::Excluded(p), None),
            Some((p, s)) => (
                Bound::Included(p),
                Some((
                    p,
                    if incl {
                        Bound::Included(s)
                    } else {
                        Bound::Excluded(s)
                    },
                )),
            ),
        }
    }

    fn copy_range(
        &self,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        rev: bool,
        take: usize,
        out: &mut ScanBuf,
    ) {
        if take == 0 {
            return;
        }
        let start = out.len();
        self.copy_groups(lo, hi, rev, take, out);
        if self.overflow_rows.load(Ordering::Relaxed) == 0 {
            return;
        }
        let ov = rd(&self.overflow);
        if ov.map.is_empty() {
            return;
        }
        let grouped = out.len() - start;
        ov.copy_range(lo, hi, rev, take, out);
        drop(ov);
        if grouped > 0 && out.len() - start > grouped {
            out.sort_by_key(start, rev);
        }
        out.truncate(start + take);
    }

    fn copy_groups(
        &self,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        rev: bool,
        take: usize,
        out: &mut ScanBuf,
    ) {
        let (plo, slo) = self.lo_cut(lo);
        let (phi, shi) = self.hi_cut(hi);
        let start = out.len();
        let mut from: Option<RamKey> = None;
        loop {
            // A batch of the prefixes in range, copied out of the index so no
            // stripe lock is ever taken while it is held.
            let batch: smallvec::SmallVec<[(RamKey, u16); 16]> = {
                let (a, z) = match (&from, rev) {
                    (None, _) => (plo, phi),
                    (Some(f), false) => (Bound::Excluded(f.bytes()), phi),
                    (Some(f), true) => (plo, Bound::Excluded(f.bytes())),
                };
                if !range_is_walkable(a, z) {
                    return;
                }
                let index = rd(&self.index);
                let it = index.range::<[u8], _>((a, z)).map(|(k, m)| (k.clone(), *m));
                if rev {
                    it.rev().take(16).collect()
                } else {
                    it.take(16).collect()
                }
            };
            let more = batch.len() == 16;
            for (p, mask) in &batch {
                let s_lo = match slo {
                    Some((q, b)) if q == p.bytes() => b,
                    _ => Bound::Unbounded,
                };
                let s_hi = match shi {
                    Some((q, b)) if q == p.bytes() => b,
                    _ => Bound::Unbounded,
                };
                if !range_is_walkable(s_lo, s_hi) {
                    continue;
                }
                let left = take - (out.len() - start);
                if mask.count_ones() == 1 {
                    let g = rd(&self.stripes[mask.trailing_zeros() as usize].0);
                    if let Some(rows) = g.groups.get(p.bytes()) {
                        let it = rows.range::<[u8], _>((s_lo, s_hi));
                        if rev {
                            for (sfx, v) in it.rev().take(left) {
                                out.push(0, [p, sfx, &[]], v);
                            }
                        } else {
                            for (sfx, v) in it.take(left) {
                                out.push(0, [p, sfx, &[]], v);
                            }
                        }
                    }
                } else {
                    self.copy_merged(p, *mask, (s_lo, s_hi), rev, left, out);
                }
                if out.len() - start >= take {
                    return;
                }
            }
            if !more {
                return;
            }
            from = batch.into_iter().last().map(|(p, _)| p);
        }
    }

    /// The first `left` rows of prefix `p` inside the suffix bounds, in key
    /// order (reverse with `rev`), merged from the stripes in `mask`.
    ///
    /// In rounds: each stripe gives a batch of its next rows past `resume`
    /// under its own lock; the rows up to the nearest batch end of a stripe
    /// that may hold more (the cutoff) are in their final order and go out,
    /// sorted; the round's rows past it are read again next round. Every round
    /// hands out at least one row, and a batch is sized so that one round
    /// usually covers `left`.
    fn copy_merged(
        &self,
        p: &RamKey,
        mask: u16,
        (s_lo, s_hi): (Bound<&[u8]>, Bound<&[u8]>),
        rev: bool,
        left: usize,
        out: &mut ScanBuf,
    ) {
        let srcs: smallvec::SmallVec<[usize; PREFIX_STRIPES]> = (0..PREFIX_STRIPES)
            .filter(|s| mask & (1 << s) != 0)
            .collect();
        let mut live: smallvec::SmallVec<[bool; PREFIX_STRIPES]> =
            smallvec::smallvec![true; srcs.len()];
        let plen = p.len();
        let mut resume: Vec<u8> = Vec::new();
        let mut resumed = false;
        let mut produced = 0usize;
        let mut round = PooledBuf::take();
        while produced < left {
            let active = live.iter().filter(|l| **l).count();
            if active == 0 {
                return;
            }
            let need = left - produced;
            let b = need.div_ceil(active).saturating_add(8).min(need);
            round.clear();
            // Per source: whether its batch was full (it may hold more), and
            // the byte range of the last key it gave.
            let mut full: smallvec::SmallVec<[bool; PREFIX_STRIPES]> =
                smallvec::smallvec![false; srcs.len()];
            let mut last: smallvec::SmallVec<[Option<(usize, usize)>; PREFIX_STRIPES]> =
                smallvec::smallvec![None; srcs.len()];
            for (i, &s) in srcs.iter().enumerate() {
                if !live[i] {
                    continue;
                }
                let (a, z) = match (resumed, rev) {
                    (false, _) => (s_lo, s_hi),
                    (true, false) => (Bound::Excluded(&resume[..]), s_hi),
                    (true, true) => (s_lo, Bound::Excluded(&resume[..])),
                };
                if !range_is_walkable(a, z) {
                    live[i] = false;
                    continue;
                }
                let g = rd(&self.stripes[s].0);
                let Some(rows) = g.groups.get(p.bytes()) else {
                    live[i] = false;
                    continue;
                };
                let first = round.len();
                let it = rows.range::<[u8], _>((a, z));
                if rev {
                    for (sfx, v) in it.rev().take(b) {
                        round.push(i as u64, [p, sfx, &[]], v);
                    }
                } else {
                    for (sfx, v) in it.take(b) {
                        round.push(i as u64, [p, sfx, &[]], v);
                    }
                }
                drop(g);
                let n = round.len() - first;
                if n == 0 {
                    live[i] = false;
                    continue;
                }
                full[i] = n == b;
                last[i] = Some(round.rows[round.len() - 1].key);
            }
            // The cutoff: the nearest batch end among the sources that may
            // hold more rows past it.
            let mut cutoff: Option<(usize, usize)> = None;
            for i in 0..srcs.len() {
                let (true, Some(k)) = (full[i], last[i]) else {
                    continue;
                };
                cutoff = Some(match cutoff {
                    None => k,
                    Some(c) => {
                        let (kc, kk) = (&round.bytes[c.0..c.1], &round.bytes[k.0..k.1]);
                        if (!rev && kk < kc) || (rev && kk > kc) {
                            k
                        } else {
                            c
                        }
                    }
                });
            }
            round.sort_by_key(0, rev);
            let mut emitted: Option<(usize, usize)> = None;
            for j in 0..round.len() {
                let kj = round.rows[j].key;
                if let Some(c) = cutoff {
                    let (k, c) = (&round.bytes[kj.0..kj.1], &round.bytes[c.0..c.1]);
                    if (!rev && k > c) || (rev && k < c) {
                        break;
                    }
                }
                out.push_row(&round, j);
                produced += 1;
                emitted = Some(kj);
                if produced == left {
                    return;
                }
            }
            let Some(e) = emitted else {
                // A full batch always has its own rows up to the cutoff, so
                // this does not happen; stop rather than loop.
                return;
            };
            resume.clear();
            resume.extend_from_slice(&round.bytes[e.0 + plen..e.1]);
            resumed = true;
            // A source that gave fewer rows than asked has none past its last:
            // it stays in only while that last row was not handed out.
            for i in 0..srcs.len() {
                if live[i] && !full[i] {
                    live[i] = last[i].is_some_and(|k| {
                        let sfx = &round.bytes[k.0 + plen..k.1];
                        if rev {
                            sfx < &resume[..]
                        } else {
                            sfx > &resume[..]
                        }
                    });
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The table
// ---------------------------------------------------------------------------

/// One RAM keyspace: its rows and their dirty set, in the container
/// [`layout`] picked.
pub(crate) enum RamTable {
    Tree(TreeTable),
    Dense(DenseTable),
    Prefixed(PrefixTable),
}

thread_local! {
    /// Debug builds: set while this thread runs a closure under a table's read
    /// lock ([`RamTable::with`]). A closure that re-enters the store could
    /// wait on a lock behind a queued writer; this turns that into a panic
    /// that names it.
    static IN_TABLE: Cell<bool> = const { Cell::new(false) };
}

impl RamTable {
    /// An empty table in `ks`'s container.
    pub(crate) fn new(ks: Keyspace) -> RamTable {
        RamTable::with_layout(layout(ks))
    }

    /// A table loaded with `rows` (in key order), none of them dirty.
    pub(crate) fn load(ks: Keyspace, rows: Vec<(RamKey, RamVal)>) -> RamTable {
        RamTable::load_with_layout(layout(ks), rows)
    }

    /// A table in an explicit container, loaded with `rows` in key order.
    pub(crate) fn load_with_layout(l: Layout, rows: Vec<(RamKey, RamVal)>) -> RamTable {
        match l {
            Layout::Tree => RamTable::Tree(TreeTable::load(rows)),
            Layout::Dense {
                lead,
                empty_fast_path,
            } => RamTable::Dense(DenseTable::load(lead, empty_fast_path, rows)),
            Layout::Prefixed { names, stripe_by } => {
                RamTable::Prefixed(PrefixTable::load(names, stripe_by, rows))
            }
        }
    }

    /// An empty table in an explicit container.
    pub(crate) fn with_layout(l: Layout) -> RamTable {
        match l {
            Layout::Tree => RamTable::Tree(TreeTable::new()),
            Layout::Dense {
                lead,
                empty_fast_path,
            } => RamTable::Dense(DenseTable::new(lead, empty_fast_path)),
            Layout::Prefixed { names, stripe_by } => {
                RamTable::Prefixed(PrefixTable::new(names, stripe_by))
            }
        }
    }

    /// `f` over the STORED bytes at `key`, under the read lock. `f` must not
    /// call back into the store.
    #[inline]
    pub(crate) fn with<T>(&self, key: &[u8], f: impl FnOnce(&RamVal) -> T) -> Option<T> {
        #[cfg(debug_assertions)]
        let _g = InTable::enter();
        match self {
            RamTable::Tree(t) => rd(&t.rows).map.get(key).map(f),
            RamTable::Dense(t) => t.with(key, f),
            RamTable::Prefixed(t) => t.with(key, f),
        }
    }

    /// The stored value at `key`, shared.
    pub(crate) fn get(&self, key: &[u8]) -> Option<RamVal> {
        self.with(key, |v| v.clone())
    }

    /// Store `val ‖ trailer` under `key` and mark it dirty. Returns the value
    /// it replaced when the replacement needed a new allocation, for the
    /// caller to drop outside the lock.
    #[inline]
    pub(crate) fn put(&self, key: &[u8], val: &[u8], trailer: &[u8]) -> Option<RamVal> {
        match self {
            RamTable::Tree(t) => wr(&t.rows).put(key, val, trailer).replaced,
            RamTable::Dense(t) => t.put(key, val, trailer),
            RamTable::Prefixed(t) => t.put(key, val, trailer),
        }
    }

    /// Store `stored` as it is (a test's damage, never a sealed write).
    pub(crate) fn put_arc(&self, key: &[u8], stored: RamVal) -> Option<RamVal> {
        match self {
            RamTable::Tree(t) => wr(&t.rows).put_arc(key, stored).0,
            RamTable::Dense(t) => t.put_arc(key, stored),
            RamTable::Prefixed(t) => t.put_arc(key, stored),
        }
    }

    /// Read-modify-write one row under its write lock, ATOMIC against every
    /// other writer of the table: `f` gets the stored bytes (`None`: no row)
    /// and writes the new stored bytes into `scratch` (cleared first), or
    /// returns false to leave the row as it is. A rewrite of the same length
    /// reuses the value's allocation when nothing else holds it. `f` runs
    /// under the lock: it must not touch the store.
    pub(crate) fn upsert(
        &self,
        key: &[u8],
        scratch: &mut Vec<u8>,
        f: impl FnOnce(Option<&[u8]>, &mut Vec<u8>) -> bool,
    ) -> Upserted {
        match self {
            RamTable::Tree(t) => wr(&t.rows).upsert(key, scratch, f),
            RamTable::Dense(t) => t.upsert(key, scratch, f),
            RamTable::Prefixed(t) => t.upsert(key, scratch, f),
        }
    }

    /// Remove `key`, marking it a dirty delete when it was there. Returns the
    /// removed value (freed by the caller, outside the lock).
    pub(crate) fn remove(&self, key: &[u8]) -> Option<RamVal> {
        match self {
            RamTable::Tree(t) => wr(&t.rows).remove(key),
            RamTable::Dense(t) => t.remove(key),
            RamTable::Prefixed(t) => t.remove(key),
        }
    }

    /// Empty the keyspace: every row it held becomes a dirty delete, so the
    /// checkpoint loses them at the next durable cycle and not before.
    pub(crate) fn clear(&self) {
        match self {
            RamTable::Tree(t) => {
                let old = wr(&t.rows).clear();
                drop(old);
            }
            RamTable::Dense(t) => t.clear(),
            RamTable::Prefixed(t) => t.clear(),
        }
    }

    /// Take the dirty rows, leaving empty sets. O(stripes) under the locks.
    pub(crate) fn take_dirty(&self, out: &mut Vec<DirtyMap>) {
        match self {
            RamTable::Tree(t) => take_one(&t.rows, |r: &mut TreeRows| &mut r.dirty, out),
            RamTable::Dense(t) => t.take_dirty(out),
            RamTable::Prefixed(t) => t.take_dirty(out),
        }
    }

    /// Mark keys dirty again — a durable cycle that did not happen — with
    /// their CURRENT values; a key written again since keeps that newer entry.
    pub(crate) fn restore_dirty(&self, keys: impl IntoIterator<Item = RamKey>) {
        match self {
            RamTable::Tree(t) => {
                let mut g = wr(&t.rows);
                for k in keys {
                    g.restore_dirty(k);
                }
            }
            RamTable::Dense(t) => {
                for k in keys {
                    t.restore_dirty(k);
                }
            }
            RamTable::Prefixed(t) => {
                for k in keys {
                    t.restore_dirty(k);
                }
            }
        }
    }

    /// Live rows.
    pub(crate) fn len(&self) -> usize {
        match self {
            RamTable::Tree(t) => rd(&t.rows).map.len(),
            RamTable::Dense(t) => t.len(),
            RamTable::Prefixed(t) => t.len(),
        }
    }

    /// Keys dirty since the last checkpoint.
    pub(crate) fn dirty_len(&self) -> usize {
        match self {
            RamTable::Tree(t) => rd(&t.rows).dirty.len(),
            RamTable::Dense(t) => t.dirty_len(),
            RamTable::Prefixed(t) => t.dirty_len(),
        }
    }

    /// Copy the first `take` rows of `(lo, hi)` in key order (the LAST `take`,
    /// in reverse order, with `rev`) into `out`: fewer only when the range
    /// holds fewer. Stored bytes, checksum included.
    pub(crate) fn copy_range(
        &self,
        lo: Bound<&[u8]>,
        hi: Bound<&[u8]>,
        rev: bool,
        take: usize,
        out: &mut ScanBuf,
    ) {
        match self {
            RamTable::Tree(t) => rd(&t.rows).copy_range(lo, hi, rev, take, out),
            RamTable::Dense(t) => t.copy_range(lo, hi, rev, take, out),
            RamTable::Prefixed(t) => t.copy_range(lo, hi, rev, take, out),
        }
    }

    /// The container this table is (the tests).
    #[cfg(test)]
    pub(crate) fn is_dense(&self) -> bool {
        matches!(self, RamTable::Dense(_))
    }

    /// The container this table is (the tests).
    #[cfg(test)]
    pub(crate) fn is_prefixed(&self) -> bool {
        matches!(self, RamTable::Prefixed(_))
    }
}

/// The debug-build guard of [`IN_TABLE`].
#[cfg(debug_assertions)]
struct InTable;

#[cfg(debug_assertions)]
impl InTable {
    fn enter() -> InTable {
        IN_TABLE.with(|c| {
            assert!(
                !c.get(),
                "a closure run under a RAM table's lock re-entered the store \
                 (it could wait on a lock behind a queued writer)"
            );
            c.set(true);
        });
        InTable
    }
}

#[cfg(debug_assertions)]
impl Drop for InTable {
    fn drop(&mut self) {
        IN_TABLE.with(|c| c.set(false));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_containers_are_the_size_the_memory_notes_assume() {
        assert_eq!(std::mem::size_of::<RamKey>(), 32);
        assert_eq!(std::mem::size_of::<Slot>(), 48);
        assert_eq!(
            std::mem::size_of::<[Slot; SLOTS]>(),
            768,
            "an allocator size class"
        );
        assert_eq!(std::mem::size_of::<Block>(), 16);
        assert_eq!(std::mem::align_of::<Padded<RwLock<Stripe>>>(), 128);
    }

    #[test]
    fn pid_arithmetic_round_trips() {
        for pid in [0u64, 1, 63, 64, 1023, 1024, 1025, 123_456_789, u64::MAX] {
            let (row, idx, s) = (pid >> ROW_SHIFT, slot_of(pid), stripe_of(pid));
            assert_eq!(pid_at(row, idx, s), pid, "{pid}");
        }
    }

    #[test]
    fn every_key_of_a_pid_keyspace_is_dense() {
        use super::super::keys::{self, Counter};
        let dense = |ks: Keyspace, k: &[u8]| match RamTable::new(ks) {
            RamTable::Dense(t) => t.split(k).is_some(),
            RamTable::Tree(_) | RamTable::Prefixed(_) => false,
        };
        assert!(dense(Keyspace::Partitions, &keys::pid(7)));
        assert!(dense(Keyspace::Garbage, &keys::pid(7)));
        assert!(dense(Keyspace::Cursors, &keys::cursors(7, "g")));
        assert!(dense(Keyspace::Txns, &keys::txns(7, 9)));
        assert!(dense(Keyspace::SegLoc, &keys::seg_loc(7, 9)));
        assert!(dense(Keyspace::Dedup, &keys::dedup(7, &[1; 16])));
        assert!(dense(Keyspace::DlqByPos, &keys::dlq_by_pos(7, "g", -1)));
        assert!(dense(
            Keyspace::PartitionFiles,
            &keys::partition_files(7, 3)
        ));
        assert!(dense(
            Keyspace::Counters,
            &keys::counter_partition(7, Counter::Pushed)
        ));
        // The name-scoped counters are the partition table's overflow.
        assert!(!dense(
            Keyspace::Counters,
            &keys::counter_queue("t", "q", Counter::Pushed)
        ));
        assert!(!dense(
            Keyspace::Counters,
            &keys::counter_tenant("t", Counter::Pushed)
        ));
        // The prefixes the per-partition scans use cut to ONE pid.
        let t = DenseTable::new(&[], false);
        let p = keys::cursors_prefix(7);
        let end = super::super::prefix_end(&p).unwrap();
        let lo = t.lo_cut(Bound::Included(&p)).unwrap();
        let hi = t.hi_cut(Bound::Excluded(&end)).unwrap();
        assert_eq!((lo.0, hi.0), (7, 7));
    }

    #[test]
    fn the_layout_table_is_the_one_documented() {
        for ks in Keyspace::ALL {
            let want = matches!(
                ks,
                Keyspace::Garbage
                    | Keyspace::DlqByPos
                    | Keyspace::PartitionFiles
                    | Keyspace::Dedup
                    | Keyspace::Partitions
                    | Keyspace::Cursors
                    | Keyspace::Txns
                    | Keyspace::SegLoc
                    | Keyspace::Counters
            );
            assert_eq!(RamTable::new(ks).is_dense(), want, "{}", ks.name());
            let prefixed = matches!(
                ks,
                Keyspace::PartitionsByKey
                    | Keyspace::QueuePartitions
                    | Keyspace::Pending
                    | Keyspace::LeasesByWorker
                    | Keyspace::RequestIds
                    | Keyspace::RequestExpiry
            );
            assert_eq!(RamTable::new(ks).is_prefixed(), prefixed, "{}", ks.name());
        }
    }

    #[test]
    fn a_prefix_is_its_names_and_nothing_else() {
        use super::super::keys;
        let k = keys::pending("t\0x", "q", "g", 7);
        let two = names_end(&k, 2).unwrap();
        assert_eq!(&k[..two], &keys::queues("t\0x", "q")[..]);
        let three = names_end(&k, 3).unwrap();
        assert_eq!(&k[..three], &keys::groups("t\0x", "q", "g")[..]);
        assert_eq!(&k[three..], &7u64.to_be_bytes()[..]);
        // A pid is not a name — unless its first bytes happen to read as one
        // (`00 00` is the empty name): the prefix is only ever the first
        // `names` names, so what follows them does not matter.
        let k2 = keys::pending("t", "q", "g", 0x0102_0304_0506_0708);
        assert_eq!(names_end(&k2, 4), None);
        assert_eq!(names_end(b"abc", 1), None);
        assert_eq!(names_end(b"a\x00\x05b\x00\x00", 1), None, "not an escape");
        assert_eq!(names_end(b"", 0), Some(0));
        // A scan's upper bound, `prefix_end` of a prefix, is no prefix: it
        // ends in `0x00 0x01`.
        let end = super::super::prefix_end(&keys::queues("t", "q")).unwrap();
        assert_eq!(names_end(&end, 2), None);
    }
}
