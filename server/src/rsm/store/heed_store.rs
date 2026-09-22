//! The heed (LMDB) adapter, with the four pins of D9 — and, since Phase C, an
//! in-RAM write-back cache over the hot keyspaces.
//!
//! Every pin is implemented here and named at its implementation:
//!
//! | pin | where |
//! |---|---|
//! | 1 `MDB_NOSYNC` | [`HeedStore::open`] |
//! | 2 a read-transaction handle, never free-standing get/scan | [`HeedStore::read`] and [`ReadGuard`] |
//! | 3 `max_readers` ≥ the blocking pool, `MDB_READERS_FULL` a refusal | [`HeedStore::open`], [`map_err`] |
//! | 4 the map-size rule, `MDB_MAP_FULL` typed and counted | [`HeedStore::open`], [`HeedStore::map_usage`], [`err`] |
//!
//! And one rule that is not a pin but an invariant: I1 gives the apply thread
//! the ONLY write handle, and [`HeedStore::write`] enforces it with
//! `writer_out`. LMDB queues writers on a process-shared mutex that has no
//! timeout, so a second caller would block with no deadline, which is what
//! I15 forbids; it is refused with [`StoreError::WriterBusy`] instead.
//!
//! The environment is opened in heed's DEFAULT reader mode — thread-local
//! reader slots (`WithTls`) — which is the mode S1 measured at 9.96 M gets/s
//! on 8 threads against 2.08 M for `MDB_NOTLS` (R-06). The price is that
//! `RoTxn` is not `Send`: a read cannot cross an `.await` and cannot move
//! between tokio workers. That is pin 2's rule turned into a type, and
//! [`HeedStore::read`]'s closure is what keeps it: the handle is borrowed for
//! the length of one blocking call and cannot escape it.
//!
//! # Phase C: RAM keyspaces over an LMDB checkpoint
//!
//! LMDB's per-op copy-on-write churn on the hot keyspaces was the measured
//! write floor (~34 MB/s on small-message shapes). In Phase C the per-queue
//! logs are the write-ahead log — every entry is written, payload-free, into
//! the queue logs it touches and fsynced before the answer — so LMDB no longer
//! has to carry the latest value of every hot row on every commit: for those
//! rows it is a CHECKPOINT store.
//!
//! - **RAM keyspaces** ([`Keyspace::is_ram`]: `meta`, `queues`, `groups`,
//!   `partitions`, `partitions_by_key`, `queue_partitions`, `cursors`,
//!   `leases_by_worker`, `pending`, `counters`, `request_ids`,
//!   `request_expiry`) are served from one ordered map per keyspace
//!   ([`RamTable`]), loaded IN FULL at [`HeedStore::open`] with no dirty key.
//!   A write mutates the map under its write lock and marks the key dirty (a
//!   dirty key whose value is absent is a delete). A read — from the apply
//!   thread's write handle or from any read handle — sees the map LIVE.
//! - **LMDB-direct keyspaces** (`garbage`, `partition_files`, `dlq`,
//!   `dlq_by_pos`, `dedup`, `txns`, `seg_loc`, `files`) are exactly what they
//!   were: rows in the open write transaction, committed on the §11.3 cadence.
//!
//! ## Commit semantics
//!
//! - [`super::Writes::commit`] (§11.3, `cycle(false)`) commits the LMDB
//!   transaction — the LMDB-direct keyspaces ONLY. RAM keys stay dirty.
//! - [`super::Writes::durable_commit`] (§11.4, `cycle(true)`; and every commit
//!   when the store is opened `sync_every_commit`) FIRST writes every dirty RAM
//!   key into the open transaction (its current value, or a delete), THEN
//!   commits and syncs. Since the apply thread is the only writer and calls it
//!   between entries, the LMDB image of the RAM keyspaces is a consistent
//!   checkpoint as of that call.
//!
//! So after a crash the RAM keyspaces — `meta` included, which is where
//! `applied_index` and `durable_index` live — reopen EXACTLY at the last
//! durable checkpoint, and the replicator replays the entries after
//! `durable_index` from the WAL. The LMDB-direct keyspaces can reopen AHEAD of
//! that index (a non-durable commit that reached the page cache survives a
//! `kill -9`), which the replay has to tolerate.
//!
//! ## Isolation
//!
//! RAM keyspaces are READ-UNCOMMITTED and live: a write by the apply thread is
//! visible to every reader at once, before any commit, and a reader can
//! observe an entry half-applied. LMDB-direct keyspaces keep their MVCC
//! snapshot semantics (a read handle sees the last commit before it opened).
//! A reader that mixes both — the planner — is correct because the batcher
//! folds every not-yet-applied entry into its overlay (§7.2).
//!
//! ## Abort does NOT roll RAM back
//!
//! [`super::Writes::abort`] and dropping a [`HeedWrite`] throw the LMDB
//! transaction away; they do not undo RAM writes, which stay dirty and reach
//! the next checkpoint. Nothing relies on the rollback: apply POISONS itself
//! on any failure and the process restarts, and recovery rebuilds from the
//! checkpoint plus the WAL. A durable cycle that fails puts the keys it had
//! taken back into the dirty sets, so nothing is ever silently dropped from
//! the next checkpoint.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use heed::types::Bytes;
use heed::{Database, Env, EnvFlags, EnvOpenOptions, RwTxn, WithTls};

use super::{
    Keyspace, MapUsage, Result, Scope, Store, StoreError, StoreMetrics, StoreOpts, MAX_DBS,
};

/// The unit the map size is rounded up to. A multiple of every page size this
/// broker runs on (4 KiB on x86-64 Linux, 16 KiB on Apple silicon), so the
/// rule never hands LMDB a size it silently rounds down.
pub const MAP_ROUND: usize = 1 << 16;

/// How many rows a RAM scan copies out (two `Arc` clones each) under the read
/// lock at a time. The callback NEVER runs under the lock, so a slow callback
/// cannot stall the apply thread's writes, and a callback that reads — or, on
/// the apply thread, writes — the same keyspace cannot deadlock against it
/// (`std`'s `RwLock` may block a recursive read behind a waiting writer).
const RAM_SCAN_CHUNK: usize = 256;

thread_local! {
    /// Pin 2. True while this thread is inside [`HeedStore::read`]. With
    /// thread-local reader slots a second read transaction on one thread is
    /// `MDB_BAD_RSLOT`; the adapter refuses before LMDB is asked.
    static READ_OPEN: Cell<bool> = const { Cell::new(false) };
}

/// Sets the thread's "a read is open" flag and clears it on the way out,
/// including on a panic — a poisoned flag would refuse every later read on
/// that worker.
struct ReadGuard;

impl ReadGuard {
    fn acquire(metrics: &StoreMetrics) -> Result<ReadGuard> {
        if READ_OPEN.with(|c| c.get()) {
            StoreMetrics::inc(&metrics.nested_read, 1);
            return Err(StoreError::NestedRead);
        }
        READ_OPEN.with(|c| c.set(true));
        Ok(ReadGuard)
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        READ_OPEN.with(|c| c.set(false));
    }
}

/// Map an LMDB error to the store's vocabulary AND count it. Pins 2, 3 and 4
/// live in the three named arms: none is a panic, none is an anonymous string,
/// and each raises the metric §11.8 and `Status` report it by.
///
/// It is a free function taking the store so that every call site can pass it
/// as `|e| err(store, e)`: the map usage it needs is only read when something
/// actually failed, never on the hot path of a `get`.
pub(crate) fn err(store: &HeedStore, e: heed::Error) -> StoreError {
    let m = &store.metrics;
    match e {
        heed::Error::Mdb(heed::MdbError::MapFull) => {
            let u = store.map_usage();
            StoreMetrics::inc(&m.map_full, 1);
            StoreError::MapFull {
                used_bytes: u.used_bytes,
                map_bytes: u.map_bytes,
            }
        }
        heed::Error::Mdb(heed::MdbError::ReadersFull) => {
            StoreMetrics::inc(&m.readers_full, 1);
            StoreError::ReadersFull {
                max_readers: store.max_readers,
            }
        }
        // Pin 2's LMDB-side symptom. It should be unreachable — [`ReadGuard`]
        // refuses first — so if it ever arrives it means a read transaction
        // was opened outside this adapter.
        heed::Error::Mdb(heed::MdbError::BadRslot) => {
            StoreMetrics::inc(&m.nested_read, 1);
            StoreError::NestedRead
        }
        // The one code LMDB raises to say the environment is DEAD ("update of
        // meta page failed or environment had fatal error"). Every later
        // transaction answers it too, so it must never fall into the anonymous
        // arm below, where it would be neither retryable nor fatal and the
        // apply thread would carry on against a store that can no longer
        // commit.
        heed::Error::Mdb(heed::MdbError::Panic) => StoreError::EnvDead {
            detail: heed::MdbError::Panic.to_string(),
        },
        heed::Error::Io(io) => StoreError::Io(io.to_string()),
        other => StoreError::Mdb(other.to_string()),
    }
}

// ---------------------------------------------------------------------------
// RAM keyspaces (Phase C)
// ---------------------------------------------------------------------------

/// Keys and values are `Arc`s so that a reader can take a row out from under
/// the lock for the price of a reference count — a scan's chunk, a `get`'s
/// arena — and so that marking a key dirty never allocates.
type RamKey = Arc<[u8]>;
type RamVal = Arc<[u8]>;

/// What one [`RamTable`]'s lock guards.
struct RamRows {
    /// The live rows, in `memcmp` order — LMDB's comparator, so every range
    /// walk is the walk the LMDB keyspace would do.
    map: BTreeMap<RamKey, RamVal>,
    /// The keys changed since the last checkpoint. A key here whose value is
    /// absent from `map` is a delete. Only the apply thread (the one writer)
    /// touches it; readers never do.
    dirty: HashSet<RamKey, crate::rsm::fasthash::FxBuild>,
}

/// One RAM keyspace: the live rows and their dirty set (module header).
pub(crate) struct RamTable {
    rows: RwLock<RamRows>,
}

impl RamTable {
    fn loaded(rows: Vec<(RamKey, RamVal)>) -> RamTable {
        RamTable {
            rows: RwLock::new(RamRows {
                // The rows come in key order off an LMDB cursor, so the
                // collect's sort is a single linear pass before the bulk build.
                map: rows.into_iter().collect(),
                dirty: HashSet::default(),
            }),
        }
    }

    // A panic under the WRITE lock is an allocation failure inside one
    // `BTreeMap`/`HashSet` operation, which leaves both structurally whole, so
    // a poisoned lock is read through rather than turned into a second panic.
    fn read(&self) -> RwLockReadGuard<'_, RamRows> {
        self.rows.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, RamRows> {
        self.rows.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn get(&self, key: &[u8]) -> Option<RamVal> {
        self.read().map.get(key).cloned()
    }

    fn put(&self, key: &[u8], val: RamVal) {
        let old = {
            let mut g = self.write();
            let rows = &mut *g;
            if let Some(slot) = rows.map.get_mut(key) {
                let old = std::mem::replace(slot, val);
                if !rows.dirty.contains(key) {
                    // First change since the checkpoint: the dirty set shares
                    // the map's key allocation.
                    let k = match rows.map.get_key_value(key) {
                        Some((k, _)) => k.clone(),
                        None => Arc::from(key),
                    };
                    rows.dirty.insert(k);
                }
                Some(old)
            } else {
                let k: RamKey = Arc::from(key);
                if !rows.dirty.contains(key) {
                    rows.dirty.insert(k.clone());
                }
                rows.map.insert(k, val);
                None
            }
        };
        // The replaced value is freed outside the lock.
        drop(old);
    }

    /// Returns the removed value (freed by the caller, outside the lock).
    fn remove(&self, key: &[u8]) -> Option<RamVal> {
        let mut g = self.write();
        let rows = &mut *g;
        let (k, v) = rows.map.remove_entry(key)?;
        // Already dirty: `insert` keeps the set's copy and drops this one,
        // which is never the last reference.
        rows.dirty.insert(k);
        Some(v)
    }

    /// Empty the keyspace: every row it held becomes a dirty delete, so the
    /// checkpoint loses them at the next durable cycle and not before.
    fn clear(&self) {
        let old = {
            let mut g = self.write();
            let rows = &mut *g;
            let old = std::mem::take(&mut rows.map);
            for k in old.keys() {
                if !rows.dirty.contains(&**k) {
                    rows.dirty.insert(k.clone());
                }
            }
            old
        };
        drop(old);
    }

    /// Take the dirty set, leaving an empty one (O(1) under the lock).
    fn take_dirty(&self) -> HashSet<RamKey, crate::rsm::fasthash::FxBuild> {
        std::mem::take(&mut self.write().dirty)
    }

    /// Put keys back into the dirty set: a durable cycle that did not happen.
    fn restore_dirty(&self, keys: Vec<RamKey>) {
        self.write().dirty.extend(keys);
    }
}

/// Keep `v` alive for as long as the handle that owns `arena`, and hand out a
/// slice of it borrowed for that long.
///
/// This is how `get_raw` keeps its `Option<&[u8]>` signature over a RAM row
/// that the apply thread can replace or delete at any moment: the reader owns
/// a reference to the value it was given.
fn pin_in_arena(arena: &RefCell<Vec<RamVal>>, v: RamVal) -> &[u8] {
    let p: *const [u8] = &*v;
    arena.borrow_mut().push(v);
    // SAFETY: `p` points into the heap allocation of the `Arc` just pushed
    // into `arena`, and an `Arc`'s pointee never moves — growing the `Vec`
    // moves the fat pointer, not the bytes. The allocation stays alive while
    // `arena` holds that `Arc`, and `arena` only ever LOSES an element when
    // (a) the handle that owns it is dropped, or (b) a `&mut self` method of
    // [`HeedWrite`] clears it through `RefCell::get_mut`. Both need every
    // `&self` borrow of the handle to have ended, and the returned slice is
    // bounded by exactly such a borrow (the lifetime of `arena` here), so it
    // can never outlive the `Arc` that keeps its bytes.
    unsafe { &*p }
}

/// Whether `BTreeMap::range` accepts these bounds. It PANICS on an inverted
/// range (and on an empty one with both ends excluded), where an LMDB cursor
/// just yields nothing — so such a range is answered as empty, as LMDB does.
fn range_is_walkable(lo: Bound<&[u8]>, hi: Bound<&[u8]>) -> bool {
    match (lo, hi) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
        (Bound::Excluded(a), Bound::Excluded(b)) => a < b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
            a <= b
        }
    }
}

// ---------------------------------------------------------------------------
// The store
// ---------------------------------------------------------------------------

pub struct HeedStore {
    env: Env<WithTls>,
    /// Indexed by [`Keyspace::slot`].
    dbs: Vec<Database<Bytes, Bytes>>,
    /// Indexed by [`Keyspace::slot`]; `Some` exactly for the RAM keyspaces
    /// ([`Keyspace::is_ram`]). See the module header.
    ram: Vec<Option<RamTable>>,
    dir: PathBuf,
    metrics: StoreMetrics,
    max_key: usize,
    max_readers: u32,
    sync_every_commit: bool,
    /// I1/I15. True while a [`HeedWrite`] is out. LMDB serializes writers on a
    /// process-shared mutex with NO timeout and no typed error, so a second
    /// caller must never reach it: [`HeedStore::write`] refuses first, and the
    /// handle clears the flag when it drops.
    writer_out: AtomicBool,
    /// TEST ONLY. Set by [`HeedStore::fail_next_sync`]: the next environment
    /// sync answers as a failing `fsync` does. A durable point whose sync
    /// fails is the one condition §11.4 cannot be checked against on real
    /// hardware without a broken disk, and it is the condition that decides
    /// whether the store's error taxonomy has a slot for "the durable point
    /// did not happen".
    #[cfg(test)]
    fail_sync: AtomicBool,
}

impl HeedStore {
    /// Open (or create) the store under `dir`.
    ///
    /// `dir` is the `store/` directory of the live state directory
    /// (`$QUEEN_RAFT_DIR/sm-<index>-<term>/store/`, §11.1). The map size comes
    /// from the rule of §11.8 ([`StoreOpts::map_size_for`]) applied to what the
    /// data file already holds, so a store that has grown reopens with room to
    /// apply what the Raft log still holds.
    ///
    /// Every RAM keyspace is then read IN FULL into its [`RamTable`] (one read
    /// transaction), which is the checkpoint the WAL replays on top of.
    pub fn open(dir: &Path, opts: &StoreOpts) -> Result<HeedStore> {
        std::fs::create_dir_all(dir)
            .map_err(|e| StoreError::Io(format!("{}: {e}", dir.display())))?;

        // LMDB does not pre-allocate the map: with no `MDB_WRITEMAP` the data
        // file is exactly the pages in use, so its length is what the rule
        // needs and no environment has to be opened to learn it.
        let used = std::fs::metadata(dir.join("data.mdb"))
            .map(|m| m.len())
            .unwrap_or(0);
        let map_bytes = opts.map_size_for(used, MAP_ROUND);

        let mut o = EnvOpenOptions::new();
        o.map_size(map_bytes);
        o.max_dbs(MAX_DBS);
        o.max_readers(opts.max_readers);
        let flags = if opts.sync_every_commit {
            EnvFlags::empty()
        } else {
            // PIN 1. `MDB_NOSYNC`, never `MDB_NOMETASYNC` (measured: 87% of
            // the rate, 1.8× the kernel writes, a 123 ms store-commit p99 —
            // R-05). The Raft log is the write-ahead log; the store reaches
            // the platter at a durable point (§11.4) and nowhere else.
            EnvFlags::NO_SYNC
        };
        // SAFETY: heed marks `flags` unsafe because some LMDB flags change the
        // durability contract. That is exactly the point: §11.4 decides when
        // bytes reach the platter, not every commit.
        unsafe { o.flags(flags) };

        let env: Env<WithTls> = unsafe { o.open(dir) }.map_err(|e| match e {
            heed::Error::EnvAlreadyOpened => StoreError::Io(format!(
                "{} is already open in this process (one store per data dir)",
                dir.display()
            )),
            other => StoreError::Mdb(format!("open {}: {other}", dir.display())),
        })?;

        let mut w = env
            .write_txn()
            .map_err(|e| StoreError::Mdb(format!("open write txn: {e}")))?;
        let mut dbs = Vec::with_capacity(Keyspace::ALL.len());
        for ks in Keyspace::ALL {
            let db = env
                .create_database::<Bytes, Bytes>(&mut w, Some(ks.name()))
                .map_err(|e| StoreError::Mdb(format!("create {}: {e}", ks.name())))?;
            dbs.push(db);
        }
        w.commit()
            .map_err(|e| StoreError::Mdb(format!("commit keyspaces: {e}")))?;
        // The keyspace table itself is durable before anything else runs.
        env.force_sync()
            .map_err(|e| StoreError::Mdb(format!("sync keyspaces: {e}")))?;

        // Phase C: load the checkpoint of every RAM keyspace. No key is dirty
        // afterwards — the RAM image IS the LMDB image.
        let mut ram: Vec<Option<RamTable>> = (0..dbs.len()).map(|_| None).collect();
        {
            let r = env
                .read_txn()
                .map_err(|e| StoreError::Mdb(format!("open load txn: {e}")))?;
            for ks in Keyspace::ALL {
                if !ks.is_ram() {
                    continue;
                }
                let mut rows: Vec<(RamKey, RamVal)> = Vec::new();
                let it = dbs[ks.slot()]
                    .iter(&r)
                    .map_err(|e| StoreError::Mdb(format!("load {}: {e}", ks.name())))?;
                for row in it {
                    let (k, v) =
                        row.map_err(|e| StoreError::Mdb(format!("load {}: {e}", ks.name())))?;
                    rows.push((Arc::from(k), Arc::from(v)));
                }
                ram[ks.slot()] = Some(RamTable::loaded(rows));
            }
        }

        let max_key = env.max_key_size();
        let max_readers = env.max_readers();
        Ok(HeedStore {
            env,
            dbs,
            ram,
            dir: dir.to_path_buf(),
            metrics: StoreMetrics::default(),
            max_key,
            max_readers,
            sync_every_commit: opts.sync_every_commit,
            writer_out: AtomicBool::new(false),
            #[cfg(test)]
            fail_sync: AtomicBool::new(false),
        })
    }

    /// TEST ONLY: make the NEXT environment sync fail, the way a disk
    /// answering `EIO` to the fsync of a durable point does.
    #[cfg(test)]
    pub fn fail_next_sync(&self) {
        self.fail_sync.store(true, Ordering::Relaxed);
    }

    /// TEST ONLY: how many keys of a RAM keyspace are dirty (changed since the
    /// last checkpoint). 0 for an LMDB-direct keyspace.
    #[cfg(test)]
    pub fn dirty_len(&self, ks: Keyspace) -> usize {
        self.ram(ks).map(|t| t.read().dirty.len()).unwrap_or(0)
    }

    /// TEST ONLY: the row as the LMDB image holds it — for a RAM keyspace, the
    /// CHECKPOINT, bypassing the live table. Opens its own read transaction,
    /// so call it from a thread that holds no other one.
    #[cfg(test)]
    pub fn checkpoint_get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let _guard = ReadGuard::acquire(&self.metrics)?;
        let txn = self.env.read_txn().map_err(|e| err(self, e))?;
        let v = self
            .db(ks)
            .get(&txn, key)
            .map_err(|e| err(self, e))?
            .map(|v| v.to_vec());
        Ok(v)
    }

    /// `mdb_env_sync(env, 1)`, with the test hook above in front of it. This is
    /// the leg of the durable point that actually reaches the platter; its
    /// failure is classified by the caller as
    /// [`StoreError::CommitFailed`] `{ durable: true }`.
    fn sync_now(&self) -> Result<()> {
        #[cfg(test)]
        if self.fail_sync.swap(false, Ordering::Relaxed) {
            return Err(err(
                self,
                heed::Error::Io(std::io::Error::other("injected fsync failure (EIO)")),
            ));
        }
        self.env.force_sync().map_err(|e| err(self, e))
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// Close the environment and wait until LMDB has really let go of the
    /// directory.
    ///
    /// heed keeps a process-wide registry of open environments and refuses a
    /// second open of the same path ([`heed::Error::EnvAlreadyOpened`]), so a
    /// REOPEN inside one process — recovery tests, and the snapshot install of
    /// §11.6 which replaces the live state directory — has to go through here
    /// rather than through `drop`, which does not wait.
    ///
    /// RAM rows still dirty are NOT written: closing is a crash as far as the
    /// RAM keyspaces are concerned, and they reopen at the last checkpoint.
    pub fn close(self) {
        let HeedStore { env, dbs, .. } = self;
        drop(dbs);
        env.prepare_for_closing().wait();
    }

    /// The bytes LMDB's data file occupies. The NUMERATOR of write
    /// amplification when a caller measures it across a run.
    pub fn disk_bytes(&self) -> u64 {
        std::fs::metadata(self.dir.join("data.mdb"))
            .map(|m| m.len())
            .unwrap_or(0)
    }

    fn db(&self, ks: Keyspace) -> &Database<Bytes, Bytes> {
        &self.dbs[ks.slot()]
    }

    /// The live table of a RAM keyspace, `None` for an LMDB-direct one.
    fn ram(&self, ks: Keyspace) -> Option<&RamTable> {
        self.ram[ks.slot()].as_ref()
    }

    fn check_key(&self, ks: Keyspace, key: &[u8]) -> Result<()> {
        if key.len() > self.max_key {
            StoreMetrics::inc(&self.metrics.key_too_long, 1);
            return Err(StoreError::KeyTooLong {
                keyspace: ks.name(),
                len: key.len(),
                max: self.max_key,
            });
        }
        Ok(())
    }

    /// The checkpoint half of a durable cycle: write every dirty RAM key into
    /// `txn` — its current value, or a delete when it is gone — in key order.
    ///
    /// The keys taken are appended to `taken` BEFORE they are written, so a
    /// caller whose cycle fails at any later step (a put here, the commit, the
    /// sync) can hand them back with [`HeedStore::restore_dirty`].
    fn drain_dirty(&self, txn: &mut RwTxn<'_>, taken: &mut Vec<(usize, Vec<RamKey>)>) -> Result<()> {
        for ks in Keyspace::ALL {
            let Some(t) = self.ram(ks) else { continue };
            let set = t.take_dirty();
            if set.is_empty() {
                continue;
            }
            let mut keys: Vec<RamKey> = set.into_iter().collect();
            // Key order: LMDB's B-tree is written leaf after leaf, not at
            // random.
            keys.sort_unstable();
            taken.push((ks.slot(), keys));
            let keys = &taken[taken.len() - 1].1;
            let db = *self.db(ks);
            // A read lock is enough: the caller is the only writer. Readers
            // are not held up by it.
            let g = t.read();
            for k in keys {
                match g.map.get(&**k) {
                    Some(v) => db.put(txn, k, v).map_err(|e| err(self, e))?,
                    None => {
                        db.delete(txn, k).map_err(|e| err(self, e))?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Hand the keys a failed durable cycle had taken back to their dirty
    /// sets, so the next checkpoint still carries them.
    fn restore_dirty(&self, taken: Vec<(usize, Vec<RamKey>)>) {
        for (slot, keys) in taken {
            if let Some(t) = self.ram[slot].as_ref() {
                t.restore_dirty(keys);
            }
        }
    }
}

// `HeedStore` is `Send + Sync` by its fields — heed's `Env` and `Database` are
// both, and so is an `RwLock` over maps of `Arc<[u8]>` — so the AUTO impls are
// what `Store: Send + Sync` is satisfied by, and there is deliberately no
// `unsafe impl` here: one would keep compiling (and keep asserting thread
// safety) if a field that is not `Sync` were ever added. The transactions are
// the ones that are not `Send`, which is what keeps a read inside one blocking
// call (pin 2).

impl Store for HeedStore {
    type Read<'s> = HeedRead<'s>;
    type Write<'s> = HeedWrite<'s>;

    fn read<R>(&self, f: impl FnOnce(&HeedRead<'_>) -> Result<R>) -> Result<R> {
        // PIN 2: one read per thread, beginning and ending inside this call.
        let _guard = ReadGuard::acquire(&self.metrics)?;
        let txn = self.env.read_txn().map_err(|e| err(self, e))?;
        StoreMetrics::inc(&self.metrics.read_txns, 1);
        let handle = HeedRead {
            store: self,
            txn,
            arena: RefCell::new(Vec::new()),
        };
        f(&handle)
    }

    fn write(&self) -> Result<HeedWrite<'_>> {
        // I1 and I15, enforced rather than assumed. Without this the second
        // caller would block inside LMDB's writer mutex — no deadline, no
        // typed error, and on a tokio worker no way back.
        if self
            .writer_out
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            StoreMetrics::inc(&self.metrics.writer_busy, 1);
            return Err(StoreError::WriterBusy);
        }
        let txn = match self.env.write_txn() {
            Ok(t) => t,
            Err(e) => {
                self.writer_out.store(false, Ordering::Release);
                return Err(err(self, e));
            }
        };
        StoreMetrics::inc(&self.metrics.write_txns, 1);
        Ok(HeedWrite {
            store: self,
            txn: Some(txn),
            poison: None,
            arena: RefCell::new(Vec::new()),
        })
    }

    fn metrics(&self) -> &StoreMetrics {
        &self.metrics
    }

    fn map_usage(&self) -> MapUsage {
        let info = self.env.info();
        let page = self.env.stat().page_size as u64;
        MapUsage {
            map_bytes: info.map_size as u64,
            used_bytes: (info.last_page_number as u64 + 1) * page,
            readers_in_use: info.number_of_readers,
            max_readers: info.maximum_number_of_readers,
        }
    }

    fn max_key_len(&self) -> usize {
        self.max_key
    }

    fn force_sync(&self) -> Result<()> {
        // A standalone sync is a durable point's other half (§11.4 step 1–2):
        // if it fails, the bytes are not on the platter and the caller must
        // report no durable index. Same classification as
        // [`super::Writes::durable_commit`], for the same reason.
        //
        // It syncs what LMDB has COMMITTED; it does not checkpoint the RAM
        // keyspaces (only a durable cycle of the write handle does).
        self.sync_now().map_err(|e| {
            StoreMetrics::inc(&self.metrics.commit_failed, 1);
            StoreError::CommitFailed {
                durable: true,
                detail: e.to_string(),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Range scans
// ---------------------------------------------------------------------------

/// The two ends of a range scan, shared by the LMDB walk and the RAM walk so a
/// scan behaves identically over either kind of keyspace.
///
/// THE PREFIX IS A RANGE, not just a stop condition. Walking from one end of
/// the whole keyspace and breaking at the first key that does not match is
/// right only when the prefix's rows happen to sit at that end: a reverse scan
/// for tenant `t1`'s newest dead letters would otherwise start on `t2`'s last
/// row and answer an EMPTY list. So both ends of the range are bounded by the
/// prefix, and `from` is CLAMPED into it; the walks keep a `break` as a belt
/// over the bounds.
///
/// `end` is [`super::prefix_end`] of a non-empty prefix (`None` for an empty
/// prefix, or one that is all `0xFF`).
fn scan_bounds<'a>(
    from: &'a [u8],
    prefix: &'a [u8],
    end: Option<&'a [u8]>,
    rev: bool,
) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
    let lo: Bound<&[u8]> = match (from.is_empty(), prefix.is_empty()) {
        (true, true) => Bound::Unbounded,
        (true, false) => Bound::Included(prefix),
        // In reverse, `from` is the HIGH end (see `hi`); the walk runs down to
        // the start of the keyspace. Bounding the low end at `from` too made
        // the range `[from, from]`: a reverse scan returned at most the one row
        // AT `from` (pre-existing; found by the Phase C reference-walk test).
        (false, true) if rev => Bound::Unbounded,
        (false, true) => Bound::Included(from),
        // Forward with a resume point below the prefix range starts at the
        // prefix; in reverse, `from` bounds the other end and the low end is
        // the prefix itself.
        (false, false) => {
            if rev || from < prefix {
                Bound::Included(prefix)
            } else {
                Bound::Included(from)
            }
        }
    };
    let hi: Bound<&[u8]> = if rev && !from.is_empty() {
        match end {
            // A `from` at or past the end of the prefix range is clamped to
            // it, so the walk begins on the prefix's last row.
            Some(e) if from >= e => Bound::Excluded(e),
            _ => Bound::Included(from),
        }
    } else {
        match end {
            Some(e) => Bound::Excluded(e),
            None => Bound::Unbounded,
        }
    };
    (lo, hi)
}

/// One walk over an LMDB-direct keyspace, shared by the read and the write
/// handle so a scan behaves identically whether the apply thread or a reader
/// runs it.
///
/// Eight arguments because that is the range contract of [`super::Reads`] —
/// keyspace, start, prefix, limit, direction and the callback — plus the store
/// and the transaction the two handles hold differently. Splitting it into a
/// struct would only move the same six knobs.
#[allow(clippy::too_many_arguments)]
fn scan_with<'t>(
    store: &HeedStore,
    txn: &'t heed::RoTxn<'t>,
    ks: Keyspace,
    from: &[u8],
    prefix: &[u8],
    limit: usize,
    rev: bool,
    cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
) -> Result<usize> {
    if !from.is_empty() {
        store.check_key(ks, from)?;
    }
    let db = store.db(ks);
    let mut n = 0usize;

    let end = if prefix.is_empty() {
        None
    } else {
        super::prefix_end(prefix)
    };
    let range = scan_bounds(from, prefix, end.as_deref(), rev);

    macro_rules! walk {
        ($it:expr) => {{
            for row in $it {
                let (k, v) = row.map_err(|e| err(store, e))?;
                if !prefix.is_empty() && !k.starts_with(prefix) {
                    break;
                }
                n += 1;
                if !cb(k, v) {
                    break;
                }
                if n >= limit {
                    break;
                }
            }
        }};
    }

    if rev {
        walk!(db.rev_range(txn, &range).map_err(|e| err(store, e))?);
    } else {
        walk!(db.range(txn, &range).map_err(|e| err(store, e))?);
    }
    Ok(n)
}

/// The same walk over a RAM keyspace, with EXACTLY [`scan_with`]'s bounds,
/// prefix belt, `limit` (a limit of 0 still hands over the first row, as the
/// LMDB walk does) and early stop.
///
/// The rows are copied out in chunks of [`RAM_SCAN_CHUNK`] under the read lock
/// and handed to `cb` with the lock released; the next chunk resumes strictly
/// past the last key handed over. Every key is therefore seen at most once and
/// in order, each with its value as of its chunk — the live, read-uncommitted
/// view the module header describes.
#[allow(clippy::too_many_arguments)]
fn ram_scan(
    store: &HeedStore,
    table: &RamTable,
    ks: Keyspace,
    from: &[u8],
    prefix: &[u8],
    limit: usize,
    rev: bool,
    cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
) -> Result<usize> {
    if !from.is_empty() {
        store.check_key(ks, from)?;
    }
    let end = if prefix.is_empty() {
        None
    } else {
        super::prefix_end(prefix)
    };
    let (lo, hi) = scan_bounds(from, prefix, end.as_deref(), rev);
    let want = limit.max(1);
    let mut n = 0usize;
    let mut buf: Vec<(RamKey, RamVal)> = Vec::with_capacity(want.min(RAM_SCAN_CHUNK));
    let mut last: Option<RamKey> = None;
    loop {
        let take = (want - n).min(RAM_SCAN_CHUNK);
        {
            let (clo, chi) = match (&last, rev) {
                (None, _) => (lo, hi),
                (Some(k), false) => (Bound::Excluded(&**k), hi),
                (Some(k), true) => (lo, Bound::Excluded(&**k)),
            };
            if !range_is_walkable(clo, chi) {
                break;
            }
            let g = table.read();
            let it = g.map.range::<[u8], _>((clo, chi));
            if rev {
                buf.extend(it.rev().take(take).map(|(k, v)| (k.clone(), v.clone())));
            } else {
                buf.extend(it.take(take).map(|(k, v)| (k.clone(), v.clone())));
            }
        }
        let exhausted = buf.len() < take;
        for (k, v) in buf.drain(..) {
            if !prefix.is_empty() && !k.starts_with(prefix) {
                return Ok(n);
            }
            n += 1;
            if !cb(&k, &v) || n >= want {
                return Ok(n);
            }
            last = Some(k);
        }
        if exhausted {
            break;
        }
    }
    Ok(n)
}

// ---------------------------------------------------------------------------
// The read handle
// ---------------------------------------------------------------------------

/// A read transaction. Cannot be `Send` (thread-local reader slots) and cannot
/// outlive the [`Store::read`] call that made it.
///
/// LMDB-direct keyspaces are read from the transaction's snapshot; RAM
/// keyspaces from the live tables (module header).
pub struct HeedRead<'s> {
    store: &'s HeedStore,
    txn: heed::RoTxn<'s, WithTls>,
    /// The RAM values this handle has returned from `get_raw`, kept alive for
    /// as long as the handle ([`pin_in_arena`]).
    arena: RefCell<Vec<RamVal>>,
}

impl super::Reads for HeedRead<'_> {
    fn max_key_len(&self) -> usize {
        self.store.max_key
    }

    fn get_raw(&self, ks: Keyspace, key: &[u8]) -> Result<Option<&[u8]>> {
        self.store.check_key(ks, key)?;
        if let Some(t) = self.store.ram(ks) {
            return Ok(t.get(key).map(|v| pin_in_arena(&self.arena, v)));
        }
        self.store
            .db(ks)
            .get(&self.txn, key)
            .map_err(|e| err(self.store, e))
    }

    fn scan_raw(
        &self,
        ks: Keyspace,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
        cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<usize> {
        if let Some(t) = self.store.ram(ks) {
            return ram_scan(self.store, t, ks, from, prefix, limit, false, cb);
        }
        scan_with(self.store, &self.txn, ks, from, prefix, limit, false, cb)
    }

    fn scan_rev_raw(
        &self,
        ks: Keyspace,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
        cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<usize> {
        if let Some(t) = self.store.ram(ks) {
            return ram_scan(self.store, t, ks, from, prefix, limit, true, cb);
        }
        scan_with(self.store, &self.txn, ks, from, prefix, limit, true, cb)
    }
}

// ---------------------------------------------------------------------------
// The write handle
// ---------------------------------------------------------------------------

/// The apply thread's open write transaction (I1: nothing else has one).
///
/// It stays open ACROSS entries and is committed on the cadence of §11.3, so
/// [`super::Writes::commit`] ends one transaction and opens the next rather
/// than consuming the handle.
///
/// RAM keyspaces are written straight into the live tables; see the module
/// header for what a commit, a durable commit and an abort mean for them.
pub struct HeedWrite<'s> {
    store: &'s HeedStore,
    /// `None` only when the handle is poisoned (a commit that did not happen,
    /// or a transaction that could not be opened).
    txn: Option<RwTxn<'s>>,
    /// What ended this handle. Once a commit has failed there is no
    /// transaction to go back to and nothing to retry, so every later call
    /// answers the SAME fatal error instead of a vague "the write transaction
    /// is closed", which `fatal()` would read as continuable.
    poison: Option<StoreError>,
    /// The RAM values `get_raw` has returned since the last `&mut self` call,
    /// kept alive for the `&self` borrows they were returned under
    /// ([`pin_in_arena`]). Every `&mut self` method clears it first — no
    /// borrow can be outstanding then — so it never grows past one run of
    /// reads.
    arena: RefCell<Vec<RamVal>>,
}

impl Drop for HeedWrite<'_> {
    fn drop(&mut self) {
        // Abort FIRST, then give the write right back. `Drop::drop` runs
        // before the fields are dropped, so releasing the right here would
        // open a window in which the next caller passes the guard and then
        // blocks inside LMDB's writer mutex on this very transaction — the
        // deadline-less wait the guard exists to prevent.
        //
        // RAM writes are NOT undone: they stay dirty for the next checkpoint
        // (module header).
        if let Some(txn) = self.txn.take() {
            txn.abort();
        }
        self.store.writer_out.store(false, Ordering::Release);
    }
}

impl<'s> HeedWrite<'s> {
    fn txn(&self) -> Result<&RwTxn<'s>> {
        self.txn.as_ref().ok_or_else(|| self.poisoned())
    }

    fn txn_mut(&mut self) -> Result<&mut RwTxn<'s>> {
        // The borrow checker will not let `poisoned()` run while `txn` is
        // borrowed mutably, so the error is built first.
        let e = self.poisoned();
        self.txn.as_mut().ok_or(e)
    }

    /// The poison check a RAM access makes in place of borrowing the
    /// transaction: a dead handle answers its fatal error for every keyspace.
    fn usable(&self) -> Result<()> {
        if self.txn.is_none() {
            return Err(self.poisoned());
        }
        Ok(())
    }

    /// Drop every value `get_raw` handed out. Only callable with `&mut self`,
    /// i.e. when no slice borrowed from `&self` can still be alive.
    fn release_arena(&mut self) {
        self.arena.get_mut().clear();
    }

    /// TEST ONLY: how many RAM values the arena is holding.
    #[cfg(test)]
    pub fn arena_len(&self) -> usize {
        self.arena.borrow().len()
    }

    /// What a poisoned handle answers. Never `None` in practice: `txn` is only
    /// taken with a poison recorded.
    fn poisoned(&self) -> StoreError {
        self.poison
            .clone()
            .unwrap_or_else(|| StoreError::Io("the write transaction is closed".into()))
    }

    /// Record why this handle ended, and answer it.
    fn poison(&mut self, e: StoreError) -> StoreError {
        if self.poison.is_none() {
            self.poison = Some(e);
        }
        self.poisoned()
    }

    /// A commit that DID NOT HAPPEN (§11.3, or the durable point of §11.4).
    /// Fatal, counted, and it poisons the handle: the transaction is gone and
    /// there is nothing to retry on.
    fn commit_failed(&mut self, durable: bool, inner: StoreError) -> StoreError {
        StoreMetrics::inc(&self.store.metrics.commit_failed, 1);
        self.poison(StoreError::CommitFailed {
            durable,
            detail: inner.to_string(),
        })
    }

    /// End the open transaction and start the next one.
    ///
    /// NON-DURABLE (`durable == false`, and the store not opened
    /// `sync_every_commit`): commit the LMDB transaction, which carries the
    /// LMDB-direct keyspaces only. RAM keys stay dirty.
    ///
    /// DURABLE: first drain every dirty RAM key into the transaction, then
    /// commit, then sync — the checkpoint (module header). Both legs of a
    /// durable point are checked: `mdb_txn_commit` and then
    /// `mdb_env_sync(env, 1)`. A failure in either one (or in the drain)
    /// means the durable point did not happen, and the caller must NOT report
    /// a durable index for it (§11.4 step 3) — reporting one lets the
    /// replicator truncate its log behind a point that is not on the platter,
    /// which a later crash turns into acknowledged effects no replay can bring
    /// back (I4, I11). On Linux the kernel consumes the fsync error and drops
    /// the dirty pages, so the next sync succeeds silently: this return value
    /// is the only notice. The drained keys go back to their dirty sets on any
    /// failure.
    fn cycle(&mut self, durable: bool) -> Result<()> {
        self.release_arena();
        let durable_leg = durable || self.store.sync_every_commit;
        let Some(mut txn) = self.txn.take() else {
            return Err(self.poisoned());
        };
        let mut taken: Vec<(usize, Vec<RamKey>)> = Vec::new();
        if durable_leg {
            if let Err(inner) = self.store.drain_dirty(&mut txn, &mut taken) {
                txn.abort();
                self.store.restore_dirty(taken);
                return Err(self.commit_failed(true, inner));
            }
        }
        if let Err(e) = txn.commit() {
            let inner = err(self.store, e);
            self.store.restore_dirty(taken);
            return Err(self.commit_failed(durable_leg, inner));
        }
        if durable_leg {
            if let Err(inner) = self.store.sync_now() {
                self.store.restore_dirty(taken);
                return Err(self.commit_failed(true, inner));
            }
            StoreMetrics::inc(&self.store.metrics.durable_commits, 1);
        } else {
            StoreMetrics::inc(&self.store.metrics.commits, 1);
        }
        drop(taken);
        let next = match self.store.env.write_txn() {
            Ok(t) => t,
            Err(e) => {
                let inner = err(self.store, e);
                return Err(self.poison(inner));
            }
        };
        StoreMetrics::inc(&self.store.metrics.write_txns, 1);
        self.txn = Some(next);
        Ok(())
    }

    /// Empty one keyspace. LMDB-direct: `mdb_drop` with `del = 0`, O(1). RAM:
    /// the table empties at once and every row it held becomes a dirty delete,
    /// so the checkpoint loses them at the next durable cycle. Used by
    /// [`super::Writes::clear_node_local`].
    fn clear(&mut self, ks: Keyspace) -> Result<()> {
        self.release_arena();
        let store = self.store;
        if let Some(t) = store.ram(ks) {
            self.usable()?;
            t.clear();
            return Ok(());
        }
        let db = *store.db(ks);
        let txn = self.txn_mut()?;
        db.clear(txn).map_err(|e| err(store, e))
    }
}

impl super::Reads for HeedWrite<'_> {
    fn max_key_len(&self) -> usize {
        self.store.max_key
    }

    fn get_raw(&self, ks: Keyspace, key: &[u8]) -> Result<Option<&[u8]>> {
        self.store.check_key(ks, key)?;
        if let Some(t) = self.store.ram(ks) {
            self.usable()?;
            return Ok(t.get(key).map(|v| pin_in_arena(&self.arena, v)));
        }
        self.store
            .db(ks)
            .get(self.txn()?, key)
            .map_err(|e| err(self.store, e))
    }

    fn scan_raw(
        &self,
        ks: Keyspace,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
        cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<usize> {
        let txn = self.txn()?;
        if let Some(t) = self.store.ram(ks) {
            return ram_scan(self.store, t, ks, from, prefix, limit, false, cb);
        }
        scan_with(self.store, txn, ks, from, prefix, limit, false, cb)
    }

    fn scan_rev_raw(
        &self,
        ks: Keyspace,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
        cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<usize> {
        let txn = self.txn()?;
        if let Some(t) = self.store.ram(ks) {
            return ram_scan(self.store, t, ks, from, prefix, limit, true, cb);
        }
        scan_with(self.store, txn, ks, from, prefix, limit, true, cb)
    }
}

impl super::Writes for HeedWrite<'_> {
    fn put_raw(&mut self, ks: Keyspace, key: &[u8], val: &[u8]) -> Result<()> {
        self.release_arena();
        self.store.check_key(ks, key)?;
        let store = self.store;
        let n = (key.len() + val.len()) as u64;
        if let Some(t) = store.ram(ks) {
            self.usable()?;
            t.put(key, Arc::from(val));
        } else {
            let db = *store.db(ks);
            let txn = self.txn_mut()?;
            db.put(txn, key, val).map_err(|e| err(store, e))?;
        }
        StoreMetrics::inc(&store.metrics.rows_put, 1);
        StoreMetrics::inc(&store.metrics.logical_bytes, n);
        Ok(())
    }

    fn del_raw(&mut self, ks: Keyspace, key: &[u8]) -> Result<bool> {
        self.release_arena();
        self.store.check_key(ks, key)?;
        let store = self.store;
        let had = if let Some(t) = store.ram(ks) {
            self.usable()?;
            // The removed value is freed here, outside the table's lock.
            t.remove(key).is_some()
        } else {
            let db = *store.db(ks);
            let txn = self.txn_mut()?;
            db.delete(txn, key).map_err(|e| err(store, e))?
        };
        if had {
            StoreMetrics::inc(&store.metrics.rows_deleted, 1);
        }
        Ok(had)
    }

    fn delete_range(
        &mut self,
        ks: Keyspace,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
    ) -> Result<(usize, Option<Vec<u8>>)> {
        use super::Reads;
        self.release_arena();
        // Collect first, delete second: the iterator borrows the transaction
        // immutably and `delete` needs it mutably. `limit` bounds the buffer,
        // which is what makes a `DeleteChunk` bounded (§5.2 rules).
        let mut victims: Vec<Vec<u8>> = Vec::with_capacity(limit.min(4096));
        self.scan_raw(ks, from, prefix, limit, &mut |k, _v| {
            victims.push(k.to_vec());
            true
        })?;
        let mut resume: Option<Vec<u8>> = None;
        if victims.len() == limit {
            // One past the last deleted key. NOT `last ‖ 0x00`: for a key at
            // the engine's limit that is a key the engine cannot hold, and
            // every later chunk would be refused with `KeyTooLong` — see
            // [`super::resume_after`]. `None` means no storable key is
            // greater, so the range is exhausted.
            if let Some(last) = victims.last() {
                resume = super::resume_after(last, self.store.max_key);
            }
        }
        for k in &victims {
            self.del_raw(ks, k)?;
        }
        Ok((victims.len(), resume))
    }

    fn commit(&mut self) -> Result<()> {
        self.cycle(false)
    }

    fn durable_commit(&mut self) -> Result<()> {
        self.cycle(true)
    }

    fn abort(&mut self) -> Result<()> {
        self.release_arena();
        if self.poison.is_some() {
            // Nothing to throw away, and the next transaction must not make a
            // failed commit look survivable.
            return Err(self.poisoned());
        }
        // Throws away the LMDB-direct writes only: RAM writes are not undone
        // (module header).
        if let Some(txn) = self.txn.take() {
            txn.abort();
        }
        let next = match self.store.env.write_txn() {
            Ok(t) => t,
            Err(e) => {
                let inner = err(self.store, e);
                return Err(self.poison(inner));
            }
        };
        StoreMetrics::inc(&self.store.metrics.write_txns, 1);
        self.txn = Some(next);
        Ok(())
    }

    fn clear_node_local(&mut self) -> Result<()> {
        self.release_arena();
        for ks in Keyspace::ALL {
            if ks.scope() == Scope::NodeLocal {
                self.clear(ks)?;
            }
        }
        Ok(())
    }
}
