//! The heed (LMDB) adapter, with the four pins of D9.
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

use std::cell::Cell;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use heed::types::Bytes;
use heed::{Database, Env, EnvFlags, EnvOpenOptions, RwTxn, WithTls};

use super::{
    Keyspace, MapUsage, Result, Scope, Store, StoreError, StoreMetrics, StoreOpts, MAX_DBS,
};

/// The unit the map size is rounded up to. A multiple of every page size this
/// broker runs on (4 KiB on x86-64 Linux, 16 KiB on Apple silicon), so the
/// rule never hands LMDB a size it silently rounds down.
pub const MAP_ROUND: usize = 1 << 16;

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
// The store
// ---------------------------------------------------------------------------

pub struct HeedStore {
    env: Env<WithTls>,
    /// Indexed by [`Keyspace::slot`].
    dbs: Vec<Database<Bytes, Bytes>>,
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

        let max_key = env.max_key_size();
        let max_readers = env.max_readers();
        Ok(HeedStore {
            env,
            dbs,
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
}

// `HeedStore` is `Send + Sync` by its fields — heed's `Env` and `Database` are
// both — so the AUTO impls are what `Store: Send + Sync` is satisfied by, and
// there is deliberately no `unsafe impl` here: one would keep compiling (and
// keep asserting thread safety) if a field that is not `Sync` were ever added.
// The transactions are the ones that are not `Send`, which is what keeps a
// read inside one blocking call (pin 2).

impl Store for HeedStore {
    type Read<'s> = HeedRead<'s>;
    type Write<'s> = HeedWrite<'s>;

    fn read<R>(&self, f: impl FnOnce(&HeedRead<'_>) -> Result<R>) -> Result<R> {
        // PIN 2: one read per thread, beginning and ending inside this call.
        let _guard = ReadGuard::acquire(&self.metrics)?;
        let txn = self.env.read_txn().map_err(|e| err(self, e))?;
        StoreMetrics::inc(&self.metrics.read_txns, 1);
        let handle = HeedRead { store: self, txn };
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
// The read handle
// ---------------------------------------------------------------------------

/// A read transaction. Cannot be `Send` (thread-local reader slots) and cannot
/// outlive the [`Store::read`] call that made it.
pub struct HeedRead<'s> {
    store: &'s HeedStore,
    txn: heed::RoTxn<'s, WithTls>,
}

/// One walk over a range, shared by the read and the write handle so a scan
/// behaves identically whether the apply thread or a reader runs it.
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

    // THE PREFIX IS A RANGE, not just a stop condition. Walking from one end
    // of the whole keyspace and breaking at the first key that does not match
    // is right only when the prefix's rows happen to sit at that end: a
    // reverse scan for tenant `t1`'s newest dead letters would otherwise start
    // on `t2`'s last row and answer an EMPTY list. So both ends of the range
    // are bounded by the prefix, and `from` is CLAMPED into it; the `break`
    // below stays as a belt over the bounds.
    let end = if prefix.is_empty() {
        None
    } else {
        super::prefix_end(prefix)
    };
    let lo: Bound<&[u8]> = match (from.is_empty(), prefix.is_empty()) {
        (true, true) => Bound::Unbounded,
        (true, false) => Bound::Included(prefix),
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
        match end.as_deref() {
            // A `from` at or past the end of the prefix range is clamped to
            // it, so the walk begins on the prefix's last row.
            Some(e) if from >= e => Bound::Excluded(e),
            _ => Bound::Included(from),
        }
    } else {
        match end.as_deref() {
            Some(e) => Bound::Excluded(e),
            None => Bound::Unbounded,
        }
    };
    let range = (lo, hi);

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

impl super::Reads for HeedRead<'_> {
    fn max_key_len(&self) -> usize {
        self.store.max_key
    }

    fn get_raw(&self, ks: Keyspace, key: &[u8]) -> Result<Option<&[u8]>> {
        self.store.check_key(ks, key)?;
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
}

impl Drop for HeedWrite<'_> {
    fn drop(&mut self) {
        // Abort FIRST, then give the write right back. `Drop::drop` runs
        // before the fields are dropped, so releasing the right here would
        // open a window in which the next caller passes the guard and then
        // blocks inside LMDB's writer mutex on this very transaction — the
        // deadline-less wait the guard exists to prevent.
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
    /// Both legs of a durable point are checked: `mdb_txn_commit` and then
    /// `mdb_env_sync(env, 1)`. A failure in either one means the durable point
    /// did not happen, and the caller must NOT report a durable index for it
    /// (§11.4 step 3) — reporting one lets the replicator truncate its log
    /// behind a point that is not on the platter, which a later crash turns
    /// into acknowledged effects no replay can bring back (I4, I11). On Linux
    /// the kernel consumes the fsync error and drops the dirty pages, so the
    /// next sync succeeds silently: this return value is the only notice.
    fn cycle(&mut self, durable: bool) -> Result<()> {
        // PHASE-C PROTOTYPE (one-fsync): LMDB is the SECOND fsync domain (its
        // `mdb_env_sync` at the durable point, MDB_NOSYNC otherwise). With
        // QUEEN_RAFT_STORE_NOSYNC=1 we SKIP that sync so the per-queue qlog fsync
        // (writer, per group) is the ONE fsync — like PG's single WAL flush.
        // Metadata durability then drops to at-least-once on power loss (rebuilt
        // from the log = Phase C recovery, prototype-lossy). Default off = today.
        static NOSYNC: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
            matches!(
                std::env::var("QUEEN_RAFT_STORE_NOSYNC").as_deref(),
                Ok("1") | Ok("true") | Ok("on")
            )
        });
        let durable_leg = durable || self.store.sync_every_commit;
        let Some(txn) = self.txn.take() else {
            return Err(self.poisoned());
        };
        if let Err(e) = txn.commit() {
            let inner = err(self.store, e);
            return Err(self.commit_failed(durable_leg, inner));
        }
        if durable_leg {
            if !*NOSYNC {
                if let Err(inner) = self.store.sync_now() {
                    return Err(self.commit_failed(true, inner));
                }
            }
            StoreMetrics::inc(&self.store.metrics.durable_commits, 1);
        } else {
            StoreMetrics::inc(&self.store.metrics.commits, 1);
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

    /// Empty one keyspace in O(1) (`mdb_drop` with `del = 0`). Used by
    /// [`super::Writes::clear_node_local`].
    fn clear(&mut self, ks: Keyspace) -> Result<()> {
        let store = self.store;
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
        scan_with(self.store, self.txn()?, ks, from, prefix, limit, false, cb)
    }

    fn scan_rev_raw(
        &self,
        ks: Keyspace,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
        cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<usize> {
        scan_with(self.store, self.txn()?, ks, from, prefix, limit, true, cb)
    }
}

impl super::Writes for HeedWrite<'_> {
    fn put_raw(&mut self, ks: Keyspace, key: &[u8], val: &[u8]) -> Result<()> {
        self.store.check_key(ks, key)?;
        let store = self.store;
        let db = *store.db(ks);
        let n = (key.len() + val.len()) as u64;
        let txn = self.txn_mut()?;
        db.put(txn, key, val).map_err(|e| err(store, e))?;
        StoreMetrics::inc(&self.store.metrics.rows_put, 1);
        StoreMetrics::inc(&self.store.metrics.logical_bytes, n);
        Ok(())
    }

    fn del_raw(&mut self, ks: Keyspace, key: &[u8]) -> Result<bool> {
        self.store.check_key(ks, key)?;
        let store = self.store;
        let db = *store.db(ks);
        let txn = self.txn_mut()?;
        let had = db.delete(txn, key).map_err(|e| err(store, e))?;
        if had {
            StoreMetrics::inc(&self.store.metrics.rows_deleted, 1);
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
        if self.poison.is_some() {
            // Nothing to throw away, and the next transaction must not make a
            // failed commit look survivable.
            return Err(self.poisoned());
        }
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
        for ks in Keyspace::ALL {
            if ks.scope() == Scope::NodeLocal {
                self.clear(ks)?;
            }
        }
        Ok(())
    }
}
