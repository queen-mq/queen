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
//!
//! # Format 1: sealed values ([`super::integrity`])
//!
//! A RAM table holds every value in its STORED form, `value ‖ checksum`,
//! sealed once by `put_raw`; the checkpoint copies those bytes to LMDB as they
//! are, and the load at open reads them back into the tables, verifying every
//! one (plus the key order and each B-tree's row count). Every read —
//! `get_raw` and both scans, on either handle, RAM or LMDB — verifies the
//! checksum and hands out the logical bytes only. The first mismatch found at
//! runtime poisons the store ([`HeedStore::poisoned`]): from then on every
//! read, write, commit, checkpoint and copy refuses with it, so nothing more is
//! served from, or checkpointed over, a store known to be damaged.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::io::Write as _;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use heed::types::Bytes;
use heed::{Database, Env, EnvFlags, EnvOpenOptions, RwTxn, WithTls};

use super::integrity::{
    self, CorruptHook, ScrubCursor, ScrubPhase, ScrubReport, StoreFormat, CHECKSUM_LEN, FORMAT_KEY,
};
use super::{
    CheckpointCut, EntryGate, Keyspace, MapUsage, Result, Scope, Store, StoreError, StoreMetrics,
    StoreOpts, MAX_DBS,
};

/// Every keyspace's checksum seed, by slot ([`integrity::seeds`]).
type Seeds = [u64; Keyspace::ALL.len()];

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
    /// The rows changed since the last checkpoint, each with its value as of
    /// its last write (`None` = deleted) — the same `Arc` the map holds, so a
    /// checkpoint cut is a swap of this map, not a lookup per key. Only the
    /// apply thread (the one writer) touches it; readers never do.
    dirty: HashMap<RamKey, Option<RamVal>, crate::rsm::fasthash::FxBuild>,
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
                dirty: HashMap::default(),
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
        let (old, old_dirty) = {
            let mut g = self.write();
            let rows = &mut *g;
            if let Some(slot) = rows.map.get_mut(key) {
                let old = std::mem::replace(slot, val.clone());
                let old_dirty = match rows.dirty.get_mut(key) {
                    Some(d) => d.replace(val),
                    None => {
                        // First change since the checkpoint: the dirty map
                        // shares the map's key allocation.
                        let k = match rows.map.get_key_value(key) {
                            Some((k, _)) => k.clone(),
                            None => Arc::from(key),
                        };
                        rows.dirty.insert(k, Some(val));
                        None
                    }
                };
                (Some(old), old_dirty)
            } else {
                let k: RamKey = Arc::from(key);
                let old_dirty = match rows.dirty.get_mut(key) {
                    Some(d) => d.replace(val.clone()),
                    None => {
                        rows.dirty.insert(k.clone(), Some(val.clone()));
                        None
                    }
                };
                rows.map.insert(k, val);
                (None, old_dirty)
            }
        };
        // The replaced values are freed outside the lock.
        drop(old);
        drop(old_dirty);
    }

    /// Returns the removed value (freed by the caller, outside the lock).
    fn remove(&self, key: &[u8]) -> Option<RamVal> {
        let mut g = self.write();
        let rows = &mut *g;
        let (k, v) = rows.map.remove_entry(key)?;
        // Already dirty: `insert` keeps the map's key and replaces the value.
        rows.dirty.insert(k, None);
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
                rows.dirty.insert(k.clone(), None);
            }
            old
        };
        drop(old);
    }

    /// Take the dirty rows, leaving an empty map (O(1) under the lock).
    fn take_dirty(&self) -> HashMap<RamKey, Option<RamVal>, crate::rsm::fasthash::FxBuild> {
        std::mem::take(&mut self.write().dirty)
    }

    /// Mark keys dirty again — a durable cycle that did not happen — with
    /// their CURRENT values; a key written again since keeps that newer entry.
    fn restore_dirty(&self, keys: Vec<RamKey>) {
        let mut g = self.write();
        let rows = &mut *g;
        for k in keys {
            if !rows.dirty.contains_key(&*k) {
                let v = rows.map.get(&*k).cloned();
                rows.dirty.insert(k, v);
            }
        }
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
    /// Every keyspace is RAM ([`Keyspace::is_ram`], true since Phase C). Then
    /// the write handle holds NO LMDB transaction between durable points —
    /// nothing but a checkpoint writes LMDB — and a checkpoint can be written
    /// on another thread ([`Store::write_cut`]) while apply carries on.
    all_ram: bool,
    /// I1/I15. True while a [`HeedWrite`] is out. LMDB serializes writers on a
    /// process-shared mutex with NO timeout and no typed error, so a second
    /// caller must never reach it: [`HeedStore::write`] refuses first, and the
    /// handle clears the flag when it drops.
    writer_out: AtomicBool,
    /// The entry boundaries of the RAM keyspaces ([`EntryGate`]).
    gate: EntryGate,
    /// The store format ([`integrity`]): format 1 carries a checksum in every
    /// value; a legacy (format 0) store runs unverified.
    format: StoreFormat,
    /// `format.checksummed()`, read on every value access.
    checksummed: bool,
    /// Every keyspace's checksum seed, by slot.
    seeds: Seeds,
    /// Set by the first corrupt value found at RUNTIME ([`HeedStore::poisoned`]):
    /// from then on every read, write, checkpoint and copy answers `poison`,
    /// so a node never serves a store it knows to be damaged.
    poisoned: AtomicBool,
    poison: OnceLock<StoreError>,
    /// Called once, with that first corruption ([`StoreOpts::on_corrupt`]).
    on_corrupt: Option<CorruptHook>,
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
    ///
    /// Format 1 ([`integrity`]): that load VERIFIES every value, the key order
    /// and each B-tree's row count, so a damaged store refuses to open with a
    /// [`StoreError::CorruptValue`] naming the keyspace and the key. A new
    /// store is created in format 1; a legacy one is migrated first
    /// ([`StoreOpts::migrate_legacy`]) or opened unverified with one warning;
    /// [`StoreOpts::verify_at_open`] scrubs the whole image before the load.
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

        // Everything below can refuse (a damaged store refuses to open), and
        // an environment that is merely dropped is not guaranteed to have let
        // go of the path before the caller retries: close it and wait.
        match Self::open_env(env.clone(), dir, opts) {
            Ok(store) => Ok(store),
            Err(e) => {
                env.prepare_for_closing().wait();
                Err(e)
            }
        }
    }

    /// [`HeedStore::open`] from the open environment on: the keyspaces, the
    /// format, the migration, the scrub and the verified load.
    fn open_env(env: Env<WithTls>, dir: &Path, opts: &StoreOpts) -> Result<HeedStore> {
        let seeds = integrity::seeds();
        #[cfg(test)]
        let (create_legacy, fail_migration) = (opts.create_legacy, opts.fail_migration);
        #[cfg(not(test))]
        let (create_legacy, fail_migration) = (false, false);

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
        // Format 1: what this store is — and a NEW store's format row, in the
        // same transaction that creates its keyspaces.
        let mut format = detect_format(&mut w, &dbs, &seeds, create_legacy)?;
        w.commit()
            .map_err(|e| StoreError::Mdb(format!("commit keyspaces: {e}")))?;
        // The keyspace table itself is durable before anything else runs.
        env.force_sync()
            .map_err(|e| StoreError::Mdb(format!("sync keyspaces: {e}")))?;

        if format == StoreFormat::Legacy && opts.migrate_legacy && !create_legacy {
            match migrate_to_v1(&env, &dbs, &seeds, fail_migration) {
                Migration::Done(rows) => {
                    format = StoreFormat::V1;
                    tracing::warn!(
                        target: "rsm",
                        dir = %dir.display(),
                        rows,
                        "store: migrated a format-0 store (no value checksums) to format 1; \
                         every value is verified from now on, and a build before format 1 \
                         can no longer open it",
                    );
                }
                // It lost its format row (never re-sealed), or the migration
                // landed without its sync.
                Migration::Refused(e) => return Err(e),
                Migration::NotDone(e) => tracing::warn!(
                    target: "rsm",
                    dir = %dir.display(),
                    error = %e,
                    "store: the migration to format 1 did not happen; the store stays format 0",
                ),
            }
        }

        if opts.verify_at_open {
            let report = {
                let r = env
                    .read_txn()
                    .map_err(|e| StoreError::Mdb(format!("open scrub txn: {e}")))?;
                scrub_image(&r, &dbs, format, &seeds, "by the scrub at open")
            };
            match &report.first_corrupt {
                None => tracing::info!(
                    target: "rsm",
                    dir = %dir.display(),
                    format = format.version(),
                    keyspaces = report.keyspaces,
                    rows = report.rows,
                    bytes = report.bytes,
                    "store: QUEEN_STORE_VERIFY: every row verified",
                ),
                Some(first) => tracing::error!(
                    target: "rsm",
                    dir = %dir.display(),
                    format = format.version(),
                    rows = report.rows,
                    corrupt = report.corrupt,
                    first = %first,
                    "store: QUEEN_STORE_VERIFY: the store is damaged",
                ),
            }
            report.into_result()?;
        }

        // Phase C: load the checkpoint of every RAM keyspace, VERIFYING it
        // (format 1). No key is dirty afterwards — the RAM image IS the LMDB
        // image, sealed values included.
        let mut ram: Vec<Option<RamTable>> = (0..dbs.len()).map(|_| None).collect();
        {
            let r = env
                .read_txn()
                .map_err(|e| StoreError::Mdb(format!("open load txn: {e}")))?;
            for ks in Keyspace::ALL {
                if !ks.is_ram() {
                    continue;
                }
                let rows = load_keyspace(&r, dbs[ks.slot()], ks, format, seeds[ks.slot()])?;
                ram[ks.slot()] = Some(RamTable::loaded(rows));
            }
        }
        if format == StoreFormat::Legacy {
            // The one warning a format-0 store gets (integrity module header).
            tracing::warn!(
                target: "rsm",
                dir = %dir.display(),
                "store: format 0 (written before value checksums): this store runs UNVERIFIED — \
                 a damaged value is served as it is. It is migrated at the next open with \
                 migration on, or replaced by a snapshot",
            );
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
            all_ram: Keyspace::ALL.iter().all(|k| k.is_ram()),
            writer_out: AtomicBool::new(false),
            gate: EntryGate::new(),
            format,
            checksummed: format.checksummed(),
            seeds,
            poisoned: AtomicBool::new(false),
            poison: OnceLock::new(),
            on_corrupt: opts.on_corrupt.clone(),
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
    /// so call it from a thread that holds no other one. The LOGICAL bytes,
    /// verified in format 1 (a mismatch is the error, without poisoning).
    #[cfg(test)]
    pub fn checkpoint_get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some(stored) = self.checkpoint_get_stored(ks, key)? else {
            return Ok(None);
        };
        if !self.checksummed {
            return Ok(Some(stored));
        }
        integrity::open(self.seeds[ks.slot()], key, &stored)
            .map(|v| Some(v.to_vec()))
            .map_err(|m| integrity::mismatch_error(ks, key, m, "in the checkpoint"))
    }

    /// TEST ONLY: [`HeedStore::checkpoint_get`] without the check: the STORED
    /// bytes (the checksum included in format 1).
    #[cfg(test)]
    pub fn checkpoint_get_stored(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let _guard = ReadGuard::acquire(&self.metrics)?;
        let txn = self.env.read_txn().map_err(|e| err(self, e))?;
        let v = self
            .db(ks)
            .get(&txn, key)
            .map_err(|e| err(self, e))?
            .map(|v| v.to_vec());
        Ok(v)
    }

    /// TEST ONLY: the stored bytes of a row in the LIVE (RAM) table, checksum
    /// included — what a read verifies.
    #[cfg(test)]
    pub fn ram_get_stored(&self, ks: Keyspace, key: &[u8]) -> Option<Vec<u8>> {
        self.ram(ks).and_then(|t| t.get(key)).map(|v| v.to_vec())
    }

    /// TEST ONLY: replace the stored bytes of a row in the LIVE table as they
    /// are, bypassing the seal — memory damage, as a read will meet it.
    #[cfg(test)]
    pub fn ram_put_stored(&self, ks: Keyspace, key: &[u8], stored: &[u8]) {
        if let Some(t) = self.ram(ks) {
            t.put(key, Arc::from(stored));
        }
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

    /// Copy the committed LMDB image into `dir/data.mdb` — one read
    /// transaction, so the copy is consistent — fsync it, and return the
    /// copy's checkpoint `(applied_index, applied_term)`: the state another
    /// node reopens at from this copy (the snapshot of a Raft cluster). The
    /// RAM keyspaces are in the image as of the last durable point, the
    /// LMDB-direct ones possibly ahead of it, exactly as after a crash.
    ///
    /// Format 1: the copy is VERIFIED before it is handed over — every row,
    /// the key order, every B-tree's row count ([`scrub_dir`]). A copy is what
    /// a peer will boot from, so damage stops HERE, on the node it belongs to:
    /// the copy is refused and this store poisoned, instead of a follower
    /// refusing to boot from it and asking again.
    pub fn copy_checkpoint(&self, dir: &Path) -> Result<(u64, u64)> {
        self.check_poison()?;
        std::fs::create_dir_all(dir)
            .map_err(|e| StoreError::Io(format!("{}: {e}", dir.display())))?;
        let path = dir.join("data.mdb");
        let _ = std::fs::remove_file(&path);
        // COMPACTING: LMDB's plain copy takes the writer mutex to read the meta
        // pages, and the apply thread holds a write transaction almost always,
        // so it would starve. The compacting copy walks one read transaction.
        let file = self
            .env
            .copy_to_path(&path, heed::CompactionOption::Enabled)
            .map_err(|e| StoreError::Mdb(format!("copy the store to {}: {e}", path.display())))?;
        file.sync_all()
            .map_err(|e| StoreError::Io(format!("sync {}: {e}", path.display())))?;
        drop(file);
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
        let (applied, term, report) = inspect_image(dir, true).map_err(|e| {
            if e.corrupt_store() {
                self.corrupt_found(e)
            } else {
                e
            }
        })?;
        if let Some(report) = report {
            StoreMetrics::inc(&self.metrics.scrubbed_rows, report.rows);
            if let Some(first) = report.first_corrupt {
                return Err(self.corrupt_found(first));
            }
        }
        Ok((applied, term))
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

    // -----------------------------------------------------------------------
    // Value integrity (format 1, see `integrity`)
    // -----------------------------------------------------------------------

    /// The format this store is in ([`integrity`]).
    pub fn format(&self) -> StoreFormat {
        self.format
    }

    /// The first corrupt value this store found at runtime, once it has
    /// found one: from then on it refuses every call with it.
    pub fn poisoned(&self) -> Option<StoreError> {
        if self.poisoned.load(Ordering::Acquire) {
            Some(self.poison_error())
        } else {
            None
        }
    }

    /// The stored form of `val` under `key`: sealed in format 1, as it is in
    /// format 0. The ONE place a value's checksum is computed.
    #[inline]
    fn seal(&self, ks: Keyspace, key: &[u8], val: &[u8]) -> RamVal {
        if self.checksummed {
            integrity::seal_arc(self.seeds[ks.slot()], key, val)
        } else {
            Arc::from(val)
        }
    }

    /// Verify a stored value and return its logical bytes (format 1), or the
    /// bytes as they are (format 0). A mismatch poisons the store.
    #[inline]
    fn open_value<'v>(&self, ks: Keyspace, key: &[u8], stored: &'v [u8]) -> Result<&'v [u8]> {
        if !self.checksummed {
            return Ok(stored);
        }
        match integrity::open(self.seeds[ks.slot()], key, stored) {
            Ok(v) => Ok(v),
            Err(m) => Err(self.corrupt_found(integrity::mismatch_error(ks, key, m, "by a read"))),
        }
    }

    /// A corrupt value found at RUNTIME: count it, latch the poison (every
    /// later call answers the first corruption), log it and call the hook —
    /// once. Returns `e` for the caller that found it.
    ///
    /// The latch is what keeps a corruption NODE-LOCAL: a caller that swallows
    /// the error cannot read on, plan on or checkpoint on a store known to be
    /// damaged, and the binary's hook ends the process before it can.
    #[cold]
    #[inline(never)]
    fn corrupt_found(&self, e: StoreError) -> StoreError {
        StoreMetrics::inc(&self.metrics.corrupt_values, 1);
        let _ = self.poison.set(e.clone());
        if !self.poisoned.swap(true, Ordering::AcqRel) {
            tracing::error!(
                target: "rsm",
                dir = %self.dir.display(),
                error = %e,
                "store: a corrupt value; this store now refuses every read and write",
            );
            if let Some(hook) = &self.on_corrupt {
                (hook.0)(&e);
            }
        }
        e
    }

    /// Refuse when the store is poisoned. One relaxed load on the hot path.
    #[inline]
    fn check_poison(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(self.poison_error());
        }
        Ok(())
    }

    #[cold]
    fn poison_error(&self) -> StoreError {
        self.poison.get().cloned().unwrap_or_else(|| {
            StoreError::corrupt_value(Keyspace::Meta, &[], "this store found a corrupt value")
        })
    }

    /// A full scrub of the store: every row of the LMDB image in ONE read
    /// transaction (the value checksums, the key order, each B-tree's row
    /// count), then every RAM table's values. Counts every failure and names
    /// the first; a failure poisons the store like a read's does.
    ///
    /// For a test, a tool, or a quiet moment: the one transaction holds pages
    /// for the length of the walk. A live node's background pass runs
    /// [`HeedStore::scrub_step`] instead.
    pub fn scrub(&self) -> Result<ScrubReport> {
        self.check_poison()?;
        let mut report = {
            let _guard = ReadGuard::acquire(&self.metrics)?;
            let txn = self.env.read_txn().map_err(|e| err(self, e))?;
            scrub_image(&txn, &self.dbs, self.format, &self.seeds, "by the scrub")
        };
        if self.checksummed {
            for ks in Keyspace::ALL {
                let Some(t) = self.ram(ks) else { continue };
                let seed = self.seeds[ks.slot()];
                let rows: Vec<(RamKey, RamVal)> = t
                    .read()
                    .map
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                for (k, v) in rows {
                    if let Err(m) = integrity::open(seed, &k, &v) {
                        report.fail(integrity::mismatch_error(ks, &k, m, "in RAM by the scrub"));
                    }
                }
            }
        }
        StoreMetrics::inc(&self.metrics.scrubbed_rows, report.rows);
        if let Some(first) = &report.first_corrupt {
            return Err(self.corrupt_found(first.clone()));
        }
        Ok(report)
    }

    /// One bounded step of the background scrub: at most `budget` rows, the
    /// LMDB image in ONE short read transaction (what the next boot and a
    /// snapshot will read), then the RAM tables (what reads are served from),
    /// resuming where `cur` stopped. Returns whether a whole pass completed
    /// (`cur` then restarts). The first corrupt row is the error — naming its
    /// keyspace and key — and poisons the store, as a read that found it
    /// would.
    ///
    /// No clock here (I2): the caller paces the steps (`QUEEN_STORE_VERIFY`
    /// covers boot; a maintenance loop calls this with a pause between steps,
    /// on every node, since the damage it looks for is node-local).
    pub fn scrub_step(&self, cur: &mut ScrubCursor, budget: usize) -> Result<bool> {
        self.check_poison()?;
        let mut left = budget.max(1);
        let n = Keyspace::ALL.len();
        if cur.phase == ScrubPhase::Image {
            let _guard = ReadGuard::acquire(&self.metrics)?;
            let txn = self.env.read_txn().map_err(|e| err(self, e))?;
            while left > 0 && cur.slot < n {
                let ks = Keyspace::ALL[cur.slot];
                let walked = scrub_image_chunk(
                    &txn,
                    self.dbs[cur.slot],
                    ks,
                    self.format,
                    self.seeds[cur.slot],
                    cur.after.as_deref(),
                    left,
                )
                .map_err(|e| {
                    if e.corrupt_store() {
                        self.corrupt_found(e)
                    } else {
                        e
                    }
                })?;
                left -= walked.rows;
                cur.rows += walked.rows as u64;
                StoreMetrics::inc(&self.metrics.scrubbed_rows, walked.rows as u64);
                match walked.last {
                    Some(last) if !walked.exhausted => cur.after = Some(last),
                    _ => {
                        cur.slot += 1;
                        cur.after = None;
                    }
                }
            }
            if cur.slot < n {
                return Ok(false);
            }
            cur.phase = ScrubPhase::Ram;
            cur.slot = 0;
            cur.after = None;
        }
        // The RAM tables. A format-0 store has no checksum to verify there.
        while self.checksummed && left > 0 && cur.slot < n {
            let ks = Keyspace::ALL[cur.slot];
            let Some(t) = self.ram(ks) else {
                cur.slot += 1;
                continue;
            };
            let take = left.min(RAM_SCAN_CHUNK);
            let rows: Vec<(RamKey, RamVal)> = {
                let g = t.read();
                let lo = match &cur.after {
                    Some(a) => Bound::Excluded(&a[..]),
                    None => Bound::Unbounded,
                };
                g.map
                    .range::<[u8], _>((lo, Bound::Unbounded))
                    .take(take)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            };
            let seed = self.seeds[ks.slot()];
            for (k, v) in &rows {
                if let Err(m) = integrity::open(seed, k, v) {
                    let e = integrity::mismatch_error(ks, k, m, "in RAM by the scrub");
                    return Err(self.corrupt_found(e));
                }
            }
            left -= rows.len();
            cur.rows += rows.len() as u64;
            StoreMetrics::inc(&self.metrics.scrubbed_rows, rows.len() as u64);
            match rows.last() {
                Some((k, _)) if rows.len() == take => cur.after = Some(k.to_vec()),
                _ => {
                    cur.slot += 1;
                    cur.after = None;
                }
            }
        }
        if self.checksummed && cur.slot < n {
            return Ok(false);
        }
        // A whole pass: start over.
        cur.phase = ScrubPhase::Image;
        cur.slot = 0;
        cur.after = None;
        cur.rows = 0;
        cur.passes += 1;
        Ok(true)
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
    fn drain_dirty(
        &self,
        txn: &mut RwTxn<'_>,
        taken: &mut Vec<(usize, Vec<RamKey>)>,
    ) -> Result<()> {
        for ks in Keyspace::ALL {
            let Some(t) = self.ram(ks) else { continue };
            let dirty = t.take_dirty();
            if dirty.is_empty() {
                continue;
            }
            let mut pairs: Vec<(RamKey, Option<RamVal>)> = dirty.into_iter().collect();
            // Key order: LMDB's B-tree is written leaf after leaf, not at
            // random.
            pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            taken.push((ks.slot(), pairs.iter().map(|(k, _)| k.clone()).collect()));
            let db = *self.db(ks);
            for (k, v) in &pairs {
                match v {
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

    /// The checkpoint CUT: every RAM keyspace's dirty rows, each with its
    /// value as of its last write. Only the single writer calls it, between
    /// entries, so those values ARE the store's image at this point; taking
    /// them is a swap of each keyspace's dirty map (O(1) under its lock), and
    /// the sort into key order is left to the checkpoint thread.
    fn cut_dirty(&self) -> CheckpointCut {
        let mut rows = Vec::new();
        let mut keys_total = 0usize;
        for ks in Keyspace::ALL {
            let Some(t) = self.ram(ks) else { continue };
            let dirty = t.take_dirty();
            if dirty.is_empty() {
                continue;
            }
            keys_total += dirty.len();
            rows.push((ks, dirty.into_iter().collect::<Vec<_>>()));
        }
        CheckpointCut {
            rows,
            keys: keys_total,
        }
    }

    /// Write a cut: ONE LMDB write transaction with every row (a put, or a
    /// delete), its commit, then the environment sync. Any error leaves LMDB at
    /// the previous checkpoint (the transaction aborts on drop, or the commit
    /// is not synced and is superseded by the next one).
    fn write_cut_inner(&self, cut: &mut CheckpointCut) -> Result<()> {
        // Key order: LMDB's B-tree is written leaf after leaf, not at random.
        for (_, pairs) in cut.rows.iter_mut() {
            pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        }
        let mut txn = self.env.write_txn().map_err(|e| err(self, e))?;
        for (ks, pairs) in &cut.rows {
            let db = *self.db(*ks);
            for (k, v) in pairs {
                match v {
                    Some(v) => db.put(&mut txn, k, v).map_err(|e| err(self, e))?,
                    None => {
                        db.delete(&mut txn, k).map_err(|e| err(self, e))?;
                    }
                }
            }
        }
        txn.commit().map_err(|e| err(self, e))?;
        self.sync_now()
    }

    /// A cut that was not written: its keys go back to their dirty sets.
    fn restore_cut_inner(&self, cut: CheckpointCut) {
        for (ks, pairs) in cut.rows {
            if let Some(t) = self.ram(ks) {
                t.restore_dirty(pairs.into_iter().map(|(k, _)| k).collect());
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
        // Format 1: a store that found a corrupt value serves nothing more.
        self.check_poison()?;
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
        self.check_poison()?;
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
        if self.all_ram {
            // Every keyspace is RAM: the handle holds no LMDB transaction (the
            // checkpoint opens its own), so the LMDB writer lock stays free for
            // the checkpoint thread.
            return Ok(HeedWrite {
                store: self,
                txn: None,
                poison: None,
                arena: RefCell::new(Vec::new()),
            });
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

    fn entry_gate(&self) -> &EntryGate {
        &self.gate
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

    fn ram_stats(&self) -> Vec<(&'static str, usize, usize)> {
        Keyspace::ALL
            .iter()
            .filter_map(|ks| {
                self.ram(*ks).map(|t| {
                    let g = t.read();
                    (ks.name(), g.map.len(), g.dirty.len())
                })
            })
            .collect()
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

    fn copy_checkpoint(&self, dir: &Path) -> Result<(u64, u64)> {
        HeedStore::copy_checkpoint(self, dir)
    }

    fn write_cut(&self, cut: &mut CheckpointCut) -> Result<()> {
        // A poisoned store writes no checkpoint: its last good one is what a
        // restart must find.
        let written = self.check_poison().and_then(|()| self.write_cut_inner(cut));
        match written {
            Ok(()) => {
                StoreMetrics::inc(&self.metrics.durable_commits, 1);
                Ok(())
            }
            Err(inner) => {
                StoreMetrics::inc(&self.metrics.commit_failed, 1);
                Err(StoreError::CommitFailed {
                    durable: true,
                    detail: inner.to_string(),
                })
            }
        }
    }

    fn restore_cut(&self, cut: CheckpointCut) {
        self.restore_cut_inner(cut)
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
                // The adapter's own format row is not a row of `meta`.
                if ks == Keyspace::Meta && k == FORMAT_KEY {
                    continue;
                }
                let v = store.open_value(ks, k, v)?;
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
            // Verified here, off the table's lock, before the callback sees it.
            let val = store.open_value(ks, &k, &v)?;
            n += 1;
            if !cb(&k, val) || n >= want {
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
        self.store.check_poison()?;
        if let Some(t) = self.store.ram(ks) {
            return match t.get(key) {
                None => Ok(None),
                Some(v) => {
                    let stored = pin_in_arena(&self.arena, v);
                    self.store.open_value(ks, key, stored).map(Some)
                }
            };
        }
        if is_format_row(ks, key) {
            return Ok(None);
        }
        match self
            .store
            .db(ks)
            .get(&self.txn, key)
            .map_err(|e| err(self.store, e))?
        {
            None => Ok(None),
            Some(stored) => self.store.open_value(ks, key, stored).map(Some),
        }
    }

    fn scan_raw(
        &self,
        ks: Keyspace,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
        cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<usize> {
        self.store.check_poison()?;
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
        self.store.check_poison()?;
        if let Some(t) = self.store.ram(ks) {
            return ram_scan(self.store, t, ks, from, prefix, limit, true, cb);
        }
        scan_with(self.store, &self.txn, ks, from, prefix, limit, true, cb)
    }
}

/// The adapter's own format row ([`integrity::FORMAT_KEY`] in `meta`), which
/// the keyspace API neither reads, writes nor deletes.
#[inline]
fn is_format_row(ks: Keyspace, key: &[u8]) -> bool {
    ks == Keyspace::Meta && key == FORMAT_KEY
}

/// The refusal of a write or a delete of the format row through the keyspace
/// API: it describes this node's file, and a caller that rewrote it could make
/// the store unreadable at the next open.
fn format_row_refused() -> StoreError {
    StoreError::Io(format!(
        "`meta/{}` is the store's own format row and is not written through the keyspace API",
        String::from_utf8_lossy(FORMAT_KEY)
    ))
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
    /// The open LMDB transaction. On an all-RAM store (`HeedStore::all_ram`)
    /// it is `None` between durable points: nothing but a checkpoint writes
    /// LMDB. Otherwise `None` only when the handle is poisoned (a commit that
    /// did not happen, or a transaction that could not be opened).
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
        self.usable()?;
        self.txn.as_ref().ok_or_else(|| {
            StoreError::Io("no LMDB write transaction is open (every keyspace is RAM)".into())
        })
    }

    fn txn_mut(&mut self) -> Result<&mut RwTxn<'s>> {
        self.usable()?;
        if self.txn.is_none() {
            // An all-RAM handle opens none up front; an LMDB-direct write (no
            // keyspace is one today) opens it on demand.
            let t = self
                .store
                .env
                .write_txn()
                .map_err(|e| err(self.store, e))?;
            StoreMetrics::inc(&self.store.metrics.write_txns, 1);
            self.txn = Some(t);
        }
        // The borrow checker will not let `poisoned()` run while `txn` is
        // borrowed mutably, so the error is built first.
        let e = self.poisoned();
        self.txn.as_mut().ok_or(e)
    }

    /// The poison check a RAM access makes in place of borrowing the
    /// transaction: a dead handle answers its fatal error for every keyspace,
    /// and so does a handle on a store that found a corrupt value.
    fn usable(&self) -> Result<()> {
        if self.poison.is_some() || (!self.store.all_ram && self.txn.is_none()) {
            return Err(self.poisoned());
        }
        self.store.check_poison()
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
        // A store that found a corrupt value commits nothing more: its last
        // good checkpoint is what a restart must find.
        self.store.check_poison()?;
        if self.store.all_ram {
            return self.cycle_ram(durable);
        }
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

    /// [`HeedWrite::cycle`] on an all-RAM store. A plain commit has nothing to
    /// write — every row is RAM, and LMDB is only ever a checkpoint — so it
    /// only counts. A durable one writes the checkpoint inline: the cut, one
    /// transaction, its commit and the sync (the same work the checkpoint
    /// thread does for [`super::Writes::take_cut`]). A failure hands the cut
    /// back to the dirty sets and poisons the handle, exactly as the drain of
    /// the LMDB-transaction path does.
    fn cycle_ram(&mut self, durable: bool) -> Result<()> {
        self.usable()?;
        // A stray LMDB-direct transaction (none exists while every keyspace is
        // RAM) is committed with the cycle, never left open across it.
        if let Some(txn) = self.txn.take() {
            if let Err(e) = txn.commit() {
                let inner = err(self.store, e);
                return Err(self.commit_failed(durable, inner));
            }
        }
        let durable_leg = durable || self.store.sync_every_commit;
        if !durable_leg {
            StoreMetrics::inc(&self.store.metrics.commits, 1);
            return Ok(());
        }
        let mut cut = self.store.cut_dirty();
        match self.store.write_cut_inner(&mut cut) {
            Ok(()) => {
                StoreMetrics::inc(&self.store.metrics.durable_commits, 1);
                Ok(())
            }
            Err(inner) => {
                self.store.restore_cut_inner(cut);
                Err(self.commit_failed(true, inner))
            }
        }
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
            return match t.get(key) {
                None => Ok(None),
                Some(v) => {
                    let stored = pin_in_arena(&self.arena, v);
                    self.store.open_value(ks, key, stored).map(Some)
                }
            };
        }
        if is_format_row(ks, key) {
            return Ok(None);
        }
        match self
            .store
            .db(ks)
            .get(self.txn()?, key)
            .map_err(|e| err(self.store, e))?
        {
            None => Ok(None),
            Some(stored) => self.store.open_value(ks, key, stored).map(Some),
        }
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
            self.usable()?;
            return ram_scan(self.store, t, ks, from, prefix, limit, false, cb);
        }
        let txn = self.txn()?;
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
        if let Some(t) = self.store.ram(ks) {
            self.usable()?;
            return ram_scan(self.store, t, ks, from, prefix, limit, true, cb);
        }
        let txn = self.txn()?;
        scan_with(self.store, txn, ks, from, prefix, limit, true, cb)
    }
}

impl super::Writes for HeedWrite<'_> {
    fn put_raw(&mut self, ks: Keyspace, key: &[u8], val: &[u8]) -> Result<()> {
        self.release_arena();
        self.store.check_key(ks, key)?;
        if is_format_row(ks, key) {
            return Err(format_row_refused());
        }
        let store = self.store;
        let n = (key.len() + val.len()) as u64;
        if let Some(t) = store.ram(ks) {
            self.usable()?;
            // Format 1: sealed HERE, where the value is born; the checkpoint
            // carries these bytes to the file unchanged.
            t.put(key, store.seal(ks, key, val));
        } else {
            let db = *store.db(ks);
            let (checksummed, seed) = (store.checksummed, store.seeds[ks.slot()]);
            let txn = self.txn_mut()?;
            if checksummed {
                db.put_reserved(txn, key, val.len() + CHECKSUM_LEN, |space| {
                    space.write_all(val)?;
                    space.write_all(&integrity::checksum(seed, key, val).to_le_bytes())
                })
                .map_err(|e| err(store, e))?;
            } else {
                db.put(txn, key, val).map_err(|e| err(store, e))?;
            }
        }
        StoreMetrics::inc(&store.metrics.rows_put, 1);
        StoreMetrics::inc(&store.metrics.logical_bytes, n);
        Ok(())
    }

    fn del_raw(&mut self, ks: Keyspace, key: &[u8]) -> Result<bool> {
        self.release_arena();
        self.store.check_key(ks, key)?;
        if is_format_row(ks, key) {
            return Err(format_row_refused());
        }
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

    fn can_cut(&self) -> bool {
        self.store.all_ram && !self.store.sync_every_commit && self.poison.is_none()
    }

    fn take_cut(&mut self) -> Result<Option<CheckpointCut>> {
        self.release_arena();
        self.usable()?;
        if !self.can_cut() {
            return Ok(None);
        }
        // A stray LMDB-direct transaction (none exists while every keyspace is
        // RAM) must not stay open: the checkpoint thread needs the LMDB writer
        // lock. Committing it is the plain commit it would have had.
        if let Some(txn) = self.txn.take() {
            if let Err(e) = txn.commit() {
                let inner = err(self.store, e);
                return Err(self.commit_failed(false, inner));
            }
        }
        StoreMetrics::inc(&self.store.metrics.commits, 1);
        Ok(Some(self.store.cut_dirty()))
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
        if self.store.all_ram {
            // No transaction is held between durable points (see `txn`).
            return Ok(());
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

// ---------------------------------------------------------------------------
// Format 1: the format row, the migration, the verified load, the scrub
// ---------------------------------------------------------------------------

/// An LMDB error met while WALKING a keyspace: a page LMDB could not read
/// (`MDB_CORRUPTED`, `MDB_PAGE_NOTFOUND`, `MDB_INVALID`) is damage to this
/// node's file and answers [`StoreError::CorruptValue`] at the last key walked;
/// anything else keeps its ordinary meaning.
fn walk_error(ks: Keyspace, after: Option<&[u8]>, e: heed::Error) -> StoreError {
    use heed::MdbError;
    match e {
        heed::Error::Mdb(MdbError::Corrupted | MdbError::PageNotFound | MdbError::Invalid) => {
            let detail = format!("LMDB could not read a page of this keyspace ({e})");
            match after {
                Some(k) => StoreError::corrupt_value(ks, k, format!("{detail}, after this key")),
                None => StoreError::corrupt_value(ks, &[], detail),
            }
        }
        other => StoreError::Mdb(format!("walk {}: {other}", ks.name())),
    }
}

/// A key that does not sort after the one before it: the walk left the
/// B-tree's order, which only a damaged page does.
fn order_error(ks: Keyspace, prev: &[u8], k: &[u8]) -> StoreError {
    StoreError::corrupt_value(
        ks,
        k,
        format!(
            "key out of order: it follows {} in the B-tree walk",
            integrity::render_key(prev)
        ),
    )
}

/// A B-tree whose walk and whose own header disagree about its row count: a
/// damaged branch page skipped or repeated a subtree.
fn count_error(ks: Keyspace, walked: u64, header: u64) -> StoreError {
    StoreError::corrupt_value(
        ks,
        &[],
        format!("the B-tree walk found {walked} rows but its header counts {header}"),
    )
}

/// The format row of `meta`, decoded and VERIFIED; `None` when there is none.
fn read_format_row(
    txn: &heed::RoTxn<'_>,
    meta: Database<Bytes, Bytes>,
    seed: u64,
) -> Result<Option<StoreFormat>> {
    let stored = meta
        .get(txn, FORMAT_KEY)
        .map_err(|e| StoreError::Mdb(format!("read the format row: {e}")))?;
    let Some(stored) = stored else {
        return Ok(None);
    };
    let v = integrity::open(seed, FORMAT_KEY, stored).map_err(|m| {
        integrity::mismatch_error(Keyspace::Meta, FORMAT_KEY, m, "at open (the format row)")
    })?;
    let version = match <[u8; 4]>::try_from(v) {
        Ok(b) => u32::from_le_bytes(b),
        Err(_) => {
            return Err(StoreError::corrupt_value(
                Keyspace::Meta,
                FORMAT_KEY,
                format!("the format row holds {} B, not a u32", v.len()),
            ))
        }
    };
    match version {
        1 => Ok(Some(StoreFormat::V1)),
        n if n > 1 => Err(StoreError::Io(format!(
            "store format {n} was written by a newer build (this one reads formats 0 and 1): \
             refusing to open it"
        ))),
        n => Err(StoreError::corrupt_value(
            Keyspace::Meta,
            FORMAT_KEY,
            format!("the format row says {n}, which no build writes"),
        )),
    }
}

/// Which format the store just opened is in; a NEW store (no row anywhere) is
/// given its format-1 row here, in the transaction that creates its keyspaces.
/// A store with rows and no format row is format 0 (legacy).
fn detect_format(
    w: &mut RwTxn<'_>,
    dbs: &[Database<Bytes, Bytes>],
    seeds: &Seeds,
    create_legacy: bool,
) -> Result<StoreFormat> {
    let meta = dbs[Keyspace::Meta.slot()];
    let seed = seeds[Keyspace::Meta.slot()];
    if let Some(format) = read_format_row(w, meta, seed)? {
        return Ok(format);
    }
    let mut rows = 0u64;
    for (db, ks) in dbs.iter().zip(Keyspace::ALL) {
        rows += db
            .len(w)
            .map_err(|e| StoreError::Mdb(format!("count {}: {e}", ks.name())))?;
    }
    if rows > 0 || create_legacy {
        return Ok(StoreFormat::Legacy);
    }
    let row = integrity::seal_vec(seed, FORMAT_KEY, &StoreFormat::V1.version().to_le_bytes());
    meta.put(w, FORMAT_KEY, &row)
        .map_err(|e| StoreError::Mdb(format!("write the format row: {e}")))?;
    Ok(StoreFormat::V1)
}

/// The refusal of a store that looks like format 0 (no format row) but holds a
/// value that verifies as format 1: it LOST its format row. Serving it as
/// format 0 would hand every caller its checksums as data, and migrating it
/// would seal it twice.
fn lost_format_row(ks: Keyspace, k: &[u8]) -> StoreError {
    StoreError::corrupt_value(
        ks,
        k,
        "this store has no format row, yet this value carries a format-1 checksum: \
         the format row was lost",
    )
}

/// What [`migrate_to_v1`] did.
enum Migration {
    /// Committed and synced: the store is format 1. The rows rewritten.
    Done(u64),
    /// Nothing landed (the transaction aborted before or at its commit): the
    /// store is the format-0 store it was.
    NotDone(StoreError),
    /// The store must not be opened: it lost its format row, or the migration
    /// committed and its sync failed (the file is format 1 in the page cache
    /// and nobody knows what reached the platter).
    Refused(StoreError),
}

/// Format 0 → format 1, once and atomically: ONE write transaction rewrites
/// every row of every keyspace sealed and adds the format row, then commit and
/// sync. A crash anywhere before the commit leaves the store format 0, and the
/// next open migrates again.
fn migrate_to_v1(
    env: &Env<WithTls>,
    dbs: &[Database<Bytes, Bytes>],
    seeds: &Seeds,
    fail_before_commit: bool,
) -> Migration {
    let mut w = match env.write_txn() {
        Ok(w) => w,
        Err(e) => {
            return Migration::NotDone(StoreError::Mdb(format!(
                "open the migration transaction: {e}"
            )))
        }
    };
    let mut total = 0u64;
    for ks in Keyspace::ALL {
        let db = dbs[ks.slot()];
        let seed = seeds[ks.slot()];
        // Collect first: the iterator borrows the transaction the puts need.
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let it = match db.iter(&w) {
            Ok(it) => it,
            Err(e) => return Migration::NotDone(walk_error(ks, None, e)),
        };
        for row in it {
            let (k, v) = match row {
                Ok(r) => r,
                Err(e) => return Migration::NotDone(walk_error(ks, None, e)),
            };
            if integrity::verifies(seed, k, v) {
                return Migration::Refused(lost_format_row(ks, k));
            }
            rows.push((k.to_vec(), v.to_vec()));
        }
        for (k, v) in &rows {
            let put = db.put_reserved(&mut w, k, v.len() + CHECKSUM_LEN, |space| {
                space.write_all(v)?;
                space.write_all(&integrity::checksum(seed, k, v).to_le_bytes())
            });
            if let Err(e) = put {
                return Migration::NotDone(StoreError::Mdb(format!("migrate {}: {e}", ks.name())));
            }
        }
        total += rows.len() as u64;
    }
    let meta_seed = seeds[Keyspace::Meta.slot()];
    let row = integrity::seal_vec(
        meta_seed,
        FORMAT_KEY,
        &StoreFormat::V1.version().to_le_bytes(),
    );
    if let Err(e) = dbs[Keyspace::Meta.slot()].put(&mut w, FORMAT_KEY, &row) {
        return Migration::NotDone(StoreError::Mdb(format!("write the format row: {e}")));
    }
    if fail_before_commit {
        // TEST ONLY: every row was rewritten inside the transaction; dropping
        // it must leave the file exactly as it was.
        return Migration::NotDone(StoreError::Io("injected migration failure".into()));
    }
    if let Err(e) = w.commit() {
        return Migration::NotDone(StoreError::Mdb(format!("commit the migration: {e}")));
    }
    match env.force_sync() {
        Ok(()) => Migration::Done(total),
        Err(e) => Migration::Refused(StoreError::CommitFailed {
            durable: true,
            detail: format!("the migration to format 1 committed but did not sync: {e}"),
        }),
    }
}

/// Load one RAM keyspace from the LMDB image, VERIFYING it on the way: the key
/// order, every value (format 1), and the walk's row count against the
/// B-tree's own header. The format row stays out of the table (it is the
/// adapter's). In a format-0 store every row is checked for a format-1
/// checksum instead, so a store that lost its format row is refused rather
/// than served. The RAM table keeps the stored bytes, checksum included.
fn load_keyspace(
    txn: &heed::RoTxn<'_>,
    db: Database<Bytes, Bytes>,
    ks: Keyspace,
    format: StoreFormat,
    seed: u64,
) -> Result<Vec<(RamKey, RamVal)>> {
    let mut rows: Vec<(RamKey, RamVal)> = Vec::new();
    let mut walked = 0u64;
    let mut prev: Option<&[u8]> = None;
    let it = db.iter(txn).map_err(|e| walk_error(ks, None, e))?;
    for row in it {
        let (k, v) = row.map_err(|e| walk_error(ks, prev, e))?;
        walked += 1;
        if let Some(p) = prev {
            if p >= k {
                return Err(order_error(ks, p, k));
            }
        }
        prev = Some(k);
        if ks == Keyspace::Meta && k == FORMAT_KEY {
            continue;
        }
        if format.checksummed() {
            if let Err(m) = integrity::open(seed, k, v) {
                return Err(integrity::mismatch_error(
                    ks,
                    k,
                    m,
                    "at the load (in the store file)",
                ));
            }
        } else if integrity::verifies(seed, k, v) {
            return Err(lost_format_row(ks, k));
        }
        rows.push((Arc::from(k), Arc::from(v)));
    }
    let header = db
        .len(txn)
        .map_err(|e| StoreError::Mdb(format!("count {}: {e}", ks.name())))?;
    if header != walked {
        return Err(count_error(ks, walked, header));
    }
    Ok(rows)
}

/// Walk EVERY keyspace of an LMDB image inside one read transaction: the key
/// order, every value's checksum (format 1) and each B-tree's row count,
/// counting every failure and naming the first. `dbs` is indexed by slot;
/// `None` is a keyspace the image does not have (an image older than it).
fn scrub_image_opt(
    txn: &heed::RoTxn<'_>,
    dbs: &[Option<Database<Bytes, Bytes>>],
    format: StoreFormat,
    seeds: &Seeds,
    site: &str,
) -> ScrubReport {
    let mut report = ScrubReport::new(format);
    for ks in Keyspace::ALL {
        let Some(db) = dbs[ks.slot()] else { continue };
        let seed = seeds[ks.slot()];
        report.keyspaces += 1;
        let mut walked = 0u64;
        let mut prev: Option<&[u8]> = None;
        let it = match db.iter(txn) {
            Ok(it) => it,
            Err(e) => {
                report.fail(walk_error(ks, None, e));
                continue;
            }
        };
        let mut broke = false;
        for row in it {
            let (k, v) = match row {
                Ok(r) => r,
                Err(e) => {
                    report.fail(walk_error(ks, prev, e));
                    broke = true;
                    break;
                }
            };
            walked += 1;
            report.rows += 1;
            report.bytes += (k.len() + v.len()) as u64;
            if let Some(p) = prev {
                if p >= k {
                    report.fail(order_error(ks, p, k));
                }
            }
            prev = Some(k);
            if format.checksummed() {
                if let Err(m) = integrity::open(seed, k, v) {
                    report.fail(integrity::mismatch_error(ks, k, m, site));
                }
            }
        }
        if !broke {
            match db.len(txn) {
                Ok(header) if header != walked => report.fail(count_error(ks, walked, header)),
                Ok(_) => {}
                Err(e) => report.fail(walk_error(ks, prev, e)),
            }
        }
    }
    report
}

/// [`scrub_image_opt`] over an open store's keyspaces.
fn scrub_image(
    txn: &heed::RoTxn<'_>,
    dbs: &[Database<Bytes, Bytes>],
    format: StoreFormat,
    seeds: &Seeds,
    site: &str,
) -> ScrubReport {
    let dbs: Vec<Option<Database<Bytes, Bytes>>> = dbs.iter().copied().map(Some).collect();
    scrub_image_opt(txn, &dbs, format, seeds, site)
}

/// What one keyspace chunk of the incremental scrub walked.
struct Chunk {
    rows: usize,
    /// The last key verified.
    last: Option<Vec<u8>>,
    /// The keyspace has no row after `last`.
    exhausted: bool,
}

/// At most `limit` rows of one keyspace of the image, after `after`: the key
/// order within the chunk and every value's checksum (format 1). The first
/// failure is the error. A chunk spans one read transaction, so the B-tree's
/// row count is checked only by the whole-image walks (open, [`HeedStore::scrub`]).
fn scrub_image_chunk(
    txn: &heed::RoTxn<'_>,
    db: Database<Bytes, Bytes>,
    ks: Keyspace,
    format: StoreFormat,
    seed: u64,
    after: Option<&[u8]>,
    limit: usize,
) -> Result<Chunk> {
    let range = match after {
        Some(a) => (Bound::Excluded(a), Bound::Unbounded),
        None => (Bound::Unbounded, Bound::Unbounded),
    };
    let it = db
        .range(txn, &range)
        .map_err(|e| walk_error(ks, after, e))?;
    let mut rows = 0usize;
    let mut prev: Option<&[u8]> = None;
    for row in it {
        if rows >= limit {
            return Ok(Chunk {
                rows,
                last: prev.map(|p| p.to_vec()),
                exhausted: false,
            });
        }
        let (k, v) = row.map_err(|e| walk_error(ks, prev.or(after), e))?;
        if let Some(p) = prev.or(after) {
            if p >= k {
                return Err(order_error(ks, p, k));
            }
        }
        if format.checksummed() {
            integrity::open(seed, k, v)
                .map_err(|m| integrity::mismatch_error(ks, k, m, "by the background scrub"))?;
        }
        prev = Some(k);
        rows += 1;
    }
    Ok(Chunk {
        rows,
        last: prev.map(|p| p.to_vec()),
        exhausted: true,
    })
}

/// A store image that is NOT open in this process (a copy about to be sent, a
/// staged snapshot, a live directory before its store opens), opened
/// read-only: its checkpoint `(applied_index, applied_term)` read in its own
/// format, and — with `scrub` — every row verified ([`scrub_image_opt`]).
/// `(0, 0, None)` when there is no image.
fn inspect_image(dir: &Path, scrub: bool) -> Result<(u64, u64, Option<ScrubReport>)> {
    let data = dir.join("data.mdb");
    let used = match std::fs::metadata(&data) {
        Ok(m) => m.len() as usize,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0, None)),
        Err(e) => return Err(StoreError::Io(format!("{}: {e}", data.display()))),
    };
    let map = (used + 2 * MAP_ROUND).div_ceil(MAP_ROUND) * MAP_ROUND;
    let mut o = EnvOpenOptions::new();
    o.map_size(map);
    o.max_dbs(MAX_DBS);
    // SAFETY: a read-only, lock-free open of a copy nobody else writes.
    unsafe { o.flags(EnvFlags::READ_ONLY | EnvFlags::NO_LOCK) };
    let env: Env<WithTls> = unsafe { o.open(dir) }
        .map_err(|e| StoreError::Mdb(format!("open {} read-only: {e}", dir.display())))?;
    let seeds = integrity::seeds();
    let out = (|| {
        let r = env
            .read_txn()
            .map_err(|e| StoreError::Mdb(format!("read {}: {e}", dir.display())))?;
        let db: Option<Database<Bytes, Bytes>> = env
            .open_database(&r, Some(Keyspace::Meta.name()))
            .map_err(|e| StoreError::Mdb(format!("open meta in {}: {e}", dir.display())))?;
        let Some(meta) = db else {
            return Ok((0, 0, None));
        };
        let meta_seed = seeds[Keyspace::Meta.slot()];
        let format = read_format_row(&r, meta, meta_seed)?.unwrap_or(StoreFormat::Legacy);
        let get = |key: &[u8]| -> Result<u64> {
            let stored = meta
                .get(&r, key)
                .map_err(|e| StoreError::Mdb(format!("read meta in {}: {e}", dir.display())))?;
            let Some(stored) = stored else { return Ok(0) };
            let v = if format.checksummed() {
                integrity::open(meta_seed, key, stored).map_err(|m| {
                    integrity::mismatch_error(Keyspace::Meta, key, m, "in a store image")
                })?
            } else {
                stored
            };
            super::rows::u64_decode(v).map_err(|_| StoreError::corrupt(Keyspace::Meta, "u64"))
        };
        let applied = get(super::meta::APPLIED_INDEX)?;
        let term = get(super::meta::APPLIED_TERM)?;
        if !scrub {
            return Ok((applied, term, None));
        }
        let mut dbs: Vec<Option<Database<Bytes, Bytes>>> = Vec::with_capacity(Keyspace::ALL.len());
        for ks in Keyspace::ALL {
            let db = env.open_database(&r, Some(ks.name())).map_err(|e| {
                StoreError::Mdb(format!("open {} in {}: {e}", ks.name(), dir.display()))
            })?;
            dbs.push(db);
        }
        let report = scrub_image_opt(&r, &dbs, format, &seeds, "in a store image");
        Ok((applied, term, Some(report)))
    })();
    env.prepare_for_closing().wait();
    out
}

/// The `(applied_index, applied_term)` of the LMDB image in `dir` (a store
/// directory that is NOT open in this process), read without loading it: a
/// read-only environment and two `meta` keys, in the image's own format
/// (verified in format 1). `(0, 0)` when there is no image.
pub fn read_checkpoint_meta(dir: &Path) -> Result<(u64, u64)> {
    let (applied, term, _) = inspect_image(dir, false)?;
    Ok((applied, term))
}

/// Verify EVERY row of the LMDB image in `dir` (a store directory that is NOT
/// open in this process: a copy, a staged snapshot, a stopped node's store):
/// the whole-image walk of [`HeedStore::scrub`], without opening the store.
/// The report counts every failure; [`ScrubReport::into_result`] turns the
/// first into the error.
pub fn scrub_dir(dir: &Path) -> Result<ScrubReport> {
    let (_, _, report) = inspect_image(dir, true)?;
    Ok(report.unwrap_or_else(|| ScrubReport::new(StoreFormat::Legacy)))
}
