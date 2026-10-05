//! Traces on disk (`PLAN_TRACES_ON_DISK.md`): the node's TRACE ENVIRONMENT.
//!
//! The store keeps every keyspace in RAM (Phase C, [`Keyspace::is_ram`]), so
//! a trace recorded at catalogue version 1 ([`Effect::TraceAppend`]) costs
//! about 1.5x its bytes of RAM on every node for as long as it is retained.
//! Traces are the one family of rows whose volume a client decides, so from
//! catalogue version 4 ([`Effect::TraceRecord`], [`Effect::TraceTrim`]) they
//! live here instead: a second LMDB environment under `<data_dir>/traces/`,
//! whose pages are file-backed (page cache the kernel can drop), never anon
//! memory per trace.
//!
//! # Why a second environment, and not the store's
//!
//! Since Phase C the store's write handle holds no LMDB transaction between
//! durable points and the checkpoint is written on its own thread; a keyspace
//! written through LMDB directly would bring back an open write transaction
//! on the apply thread, inline durable points and apply waiting on LMDB's
//! writer mutex behind the checkpoint. This environment has its own writer
//! (the apply thread, briefly, once per entry that carries traces) and its own
//! reader table, so it touches none of that.
//!
//! # Exactly once ([`TraceStore::applied_index`])
//!
//! The environment records, in the same transaction as the rows, the index of
//! the last entry whose trace writes it holds. Apply runs an entry's trace
//! writes only when the entry's index is ABOVE it, so a replay from the
//! store's durable point (which this environment may be ahead of after a
//! crash: its commits are `MDB_NOSYNC`) never applies them twice. The keys
//! are deterministic on top of that — `(entry index, effect ordinal)` names a
//! trace on every node and every replay — so even a second application would
//! rewrite the same rows.
//!
//! Durability: [`TraceStore::sync`] runs before the store records a durable
//! index (on the checkpoint thread, or inline), so every trace of an entry at
//! or below a durable index is on the platter before a log can be truncated
//! behind it.
//!
//! # Keys and values
//!
//! The store's encodings ([`keys`]): escaped, terminated names; big-endian
//! integers; a sign-flipped `created_at`. `msg` is `pid? ‖ txn`, the part of
//! the legacy primary key after its tenant, so it sorts as that key did.
//!
//! | db | key | value |
//! |---|---|---|
//! | `meta` | `applied_index`, `format` | u64 LE, u32 LE |
//! | `bodies` | `index u64 ‖ ordinal u32` | the trace row ([`rows::trace_encode`]) |
//! | `by_txn` | `tenant ‖ msg ‖ created_at ‖ index ‖ ordinal` | empty |
//! | `by_name` | `tenant ‖ name ‖ created_at ‖ index ‖ ordinal` | `count u32 ‖ msg` |
//! | `expiry` | `created_at ‖ index ‖ ordinal` | `tenant ‖ msg ‖ n u32 ‖ name*` |
//!
//! Bodies are keyed in apply order, so they are appended at the right edge of
//! their B-tree and expiry, which follows `created_at` (nearly the same
//! order), deletes from the left one. The expiry row carries every key the
//! trace has, so expiry and a tenant purge never read a body.
//!
//! Every value is sealed with the store's format-1 checksum
//! ([`integrity::checksum`], one seed per database) and verified wherever it
//! is read.
//!
//! [`Keyspace::is_ram`]: crate::rsm::store::Keyspace::is_ram
//! [`Effect::TraceAppend`]: crate::rsm::effect::Effect::TraceAppend
//! [`Effect::TraceRecord`]: crate::rsm::effect::Effect::TraceRecord
//! [`Effect::TraceTrim`]: crate::rsm::effect::Effect::TraceTrim

// I2: apply writes this environment, so the clock, the environment and
// randomness stay out of it, as under `rsm/store/` (`clippy.toml`).
#![deny(clippy::disallowed_methods)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError, Weak};

use heed::types::Bytes;
use heed::{Database, Env, EnvFlags, EnvOpenOptions, RoTxn, RwTxn, WithTls};
use xxhash_rust::xxh3::{xxh3_128, xxh3_64_with_seed};

use crate::rsm::effect::{Pid, TraceEvent};
use crate::rsm::store::heed_store::MAP_ROUND;
use crate::rsm::store::{integrity, keys, prefix_end, rows};
use crate::rsm::store::{MapUsage, Result, StoreError, StoreOpts};

/// The environment's directory under a node's data directory.
pub const DIR: &str = "traces";

/// The on-disk format of this environment, in its `meta` database.
const FORMAT: u32 = 1;
const META_FORMAT: &[u8] = b"format";
const META_APPLIED: &[u8] = b"applied_index";

/// Domain separation for the checksum seeds ("QTRACES1"). PERMANENT.
const DOMAIN: u64 = 0x5154_5241_4345_5331;

/// `created_at ‖ index ‖ ordinal`: the fixed tail of every index key.
const TAIL: usize = 8 + 8 + 4;

/// Rows a tenant purge deletes per pass over `by_txn` (a buffer bound, not a
/// work bound: the purge deletes every row of the tenant).
const PURGE_CHUNK: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Db {
    Meta = 0,
    Bodies = 1,
    ByTxn = 2,
    ByName = 3,
    Expiry = 4,
}

impl Db {
    const ALL: [Db; 5] = [Db::Meta, Db::Bodies, Db::ByTxn, Db::ByName, Db::Expiry];

    /// The LMDB database name. PERMANENT.
    fn name(self) -> &'static str {
        match self {
            Db::Meta => "meta",
            Db::Bodies => "bodies",
            Db::ByTxn => "by_txn",
            Db::ByName => "by_name",
            Db::Expiry => "expiry",
        }
    }

    /// The name an error reports.
    fn label(self) -> &'static str {
        match self {
            Db::Meta => "traces.meta",
            Db::Bodies => "traces.bodies",
            Db::ByTxn => "traces.by_txn",
            Db::ByName => "traces.by_name",
            Db::Expiry => "traces.expiry",
        }
    }

    fn seed(self) -> u64 {
        xxh3_64_with_seed(self.name().as_bytes(), DOMAIN)
    }
}

/// One trace's name in the environment: the entry that recorded it and the
/// effect's position in that entry. The same on every node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TraceId {
    pub index: u64,
    pub ordinal: u32,
}

impl TraceId {
    fn key(self) -> [u8; 12] {
        let mut k = [0u8; 12];
        k[..8].copy_from_slice(&self.index.to_be_bytes());
        k[8..].copy_from_slice(&self.ordinal.to_be_bytes());
        k
    }

    fn of(b: &[u8]) -> Option<TraceId> {
        Some(TraceId {
            index: keys::read_u64(b, 0)?,
            ordinal: keys::read_u32(b, 8)?,
        })
    }
}

/// How the environment is opened. Node-local, read at boot.
#[derive(Clone, Copy, Debug)]
pub struct TraceOpts {
    /// `QUEEN_RAFT_TRACE_MAP_BYTES`, or `None` for the store's map-size rule
    /// ([`StoreOpts::map_size_for`]: 64 GiB of address space, grown at a
    /// reopen once half of it is used).
    pub map_bytes: Option<usize>,
    pub max_readers: u32,
}

impl Default for TraceOpts {
    fn default() -> TraceOpts {
        TraceOpts {
            map_bytes: None,
            // The store's pin 3: at least the blocking pool.
            max_readers: 1024,
        }
    }
}

impl TraceOpts {
    /// Boot-only, node-local: how big a map to reserve is not replicated
    /// state.
    #[allow(clippy::disallowed_methods)]
    pub fn from_env() -> TraceOpts {
        TraceOpts {
            map_bytes: std::env::var("QUEEN_RAFT_TRACE_MAP_BYTES")
                .ok()
                .and_then(|v| v.trim().parse::<usize>().ok())
                .filter(|v| *v > 0),
            ..TraceOpts::default()
        }
    }
}

// ---------------------------------------------------------------------------
// One environment per directory per process
// ---------------------------------------------------------------------------

/// The open environments, by canonical directory. Apply, the facade, the
/// leader's maintenance and the snapshot sender share one instance; the last
/// owner to drop it closes it.
static OPEN: LazyLock<Mutex<HashMap<PathBuf, Weak<TraceStore>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn canonical(dir: &Path) -> Result<PathBuf> {
    std::fs::create_dir_all(dir).map_err(|e| StoreError::Io(format!("{}: {e}", dir.display())))?;
    std::fs::canonicalize(dir).map_err(|e| StoreError::Io(format!("{}: {e}", dir.display())))
}

/// The trace environment under `dir` (`<data_dir>/traces`), opened once per
/// process: a second caller gets the instance the first one opened.
pub fn open(dir: &Path, opts: TraceOpts) -> Result<Arc<TraceStore>> {
    let key = canonical(dir)?;
    let mut reg = OPEN.lock().unwrap_or_else(PoisonError::into_inner);
    reg.retain(|_, w| w.strong_count() > 0);
    if let Some(s) = reg.get(&key).and_then(Weak::upgrade) {
        return Ok(s);
    }
    let store = Arc::new(TraceStore::open_at(&key, &opts)?);
    reg.insert(key, Arc::downgrade(&store));
    Ok(store)
}

/// The environment under `dir` if this process has it open.
pub fn lookup(dir: &Path) -> Option<Arc<TraceStore>> {
    let key = std::fs::canonicalize(dir).ok()?;
    OPEN.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&key)
        .and_then(Weak::upgrade)
}

// ---------------------------------------------------------------------------
// Encodings
// ---------------------------------------------------------------------------

fn push_pid(out: &mut Vec<u8>, p: Option<Pid>) {
    match p {
        Some(p) => {
            out.push(1);
            keys::push_u64(out, p);
        }
        None => {
            out.push(0);
            keys::push_u64(out, 0);
        }
    }
}

/// `pid? ‖ txn`: the message a trace is about, encoded as in the legacy
/// primary key after its tenant.
fn push_msg(out: &mut Vec<u8>, pid: Option<Pid>, txn: &str) {
    push_pid(out, pid);
    keys::push_name(out, txn);
}

/// The end of a `msg` that starts at `at`.
fn msg_end(b: &[u8], at: usize) -> Option<usize> {
    let after_pid = at.checked_add(9)?;
    if b.len() < after_pid {
        return None;
    }
    keys::read_name(b, after_pid).map(|(_, end)| end)
}

/// The 128-bit hash of a message: how `/traces/names` counts distinct
/// messages without keeping their names. The legacy rows use it too.
pub fn message_hash(pid: Option<Pid>, txn: &str) -> u128 {
    let mut b = Vec::with_capacity(txn.len() + 11);
    push_msg(&mut b, pid, txn);
    xxh3_128(&b)
}

fn push_tail(out: &mut Vec<u8>, created_at_us: i64, id: TraceId) {
    keys::push_i64(out, created_at_us);
    out.extend_from_slice(&id.key());
}

fn read_tail(k: &[u8]) -> Option<(i64, TraceId)> {
    let at = k.len().checked_sub(TAIL)?;
    Some((keys::read_i64(k, at)?, TraceId::of(&k[at + 8..])?))
}

fn txn_prefix(tenant: &str, pid: Option<Pid>, txn: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(tenant.len() + txn.len() + 13 + TAIL);
    keys::push_name(&mut k, tenant);
    push_msg(&mut k, pid, txn);
    k
}

fn name_prefix(tenant: &str, name: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(tenant.len() + name.len() + 4 + TAIL);
    keys::push_name(&mut k, tenant);
    keys::push_name(&mut k, name);
    k
}

fn expiry_key(created_at_us: i64, id: TraceId) -> [u8; TAIL] {
    let mut k = [0u8; TAIL];
    k[..8].copy_from_slice(&((created_at_us as u64) ^ (1u64 << 63)).to_be_bytes());
    k[8..].copy_from_slice(&id.key());
    k
}

/// Every key a trace event writes, before it writes one: what a caller checks
/// against the engine's key limit ([`check_keys`]).
fn index_keys(ev: &TraceEvent, id: TraceId) -> (Vec<u8>, Vec<(Vec<u8>, u32)>) {
    let mut by_txn = txn_prefix(&ev.tenant, ev.pid, &ev.txn);
    push_tail(&mut by_txn, ev.created_at_us, id);
    let mut counts: BTreeMap<&str, u32> = BTreeMap::new();
    for n in &ev.names {
        *counts.entry(n.as_str()).or_insert(0) += 1;
    }
    let by_name = counts
        .into_iter()
        .map(|(n, c)| {
            let mut k = name_prefix(&ev.tenant, n);
            push_tail(&mut k, ev.created_at_us, id);
            (k, c)
        })
        .collect();
    (by_txn, by_name)
}

/// Whether every key `ev` writes fits the engine — in this environment AND,
/// for a version-1 [`Effect::TraceAppend`], in the store's legacy keyspaces.
/// The facade and the planner refuse an event that does not: in apply a key
/// over the limit is `KeyTooLong`, which stops every node on the same entry.
///
/// [`Effect::TraceAppend`]: crate::rsm::effect::Effect::TraceAppend
pub fn check_keys(ev: &TraceEvent, max_key: usize) -> std::result::Result<(), String> {
    let by_txn = txn_prefix(&ev.tenant, ev.pid, &ev.txn).len() + TAIL;
    let legacy = keys::trace(&ev.tenant, ev.pid, &ev.txn, 0).len();
    if by_txn.max(legacy) > max_key {
        return Err(format!(
            "a transactionId of {} B is too long for a trace (the key limit is {max_key} B)",
            ev.txn.len()
        ));
    }
    for name in &ev.names {
        let by_name = name_prefix(&ev.tenant, name).len() + TAIL;
        let legacy = keys::trace_name(&ev.tenant, name, ev.created_at_us, &ev.trace_id).len();
        if by_name.max(legacy) > max_key {
            return Err(format!(
                "a trace name of {} B is too long (the key limit is {max_key} B)",
                name.len()
            ));
        }
    }
    Ok(())
}

fn corrupt(db: Db, detail: impl Into<String>) -> StoreError {
    StoreError::Corrupt {
        keyspace: db.label(),
        detail: detail.into(),
    }
}

// ---------------------------------------------------------------------------
// The environment
// ---------------------------------------------------------------------------

pub struct TraceStore {
    env: Env<WithTls>,
    dbs: [Database<Bytes, Bytes>; 5],
    seeds: [u64; 5],
    dir: PathBuf,
    max_key: usize,
    /// [`TraceStore::applied_index`].
    applied: AtomicU64,
    /// A commit landed since the last [`TraceStore::sync`].
    unsynced: AtomicBool,
    /// TEST ONLY: the next sync fails as a disk answering `EIO` would.
    #[cfg(test)]
    fail_sync: AtomicBool,
}

impl std::fmt::Debug for TraceStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceStore")
            .field("dir", &self.dir)
            .field("applied", &self.applied_index())
            .finish()
    }
}

/// An LMDB error, as the store's vocabulary has it.
fn mdb(e: heed::Error, usage: impl FnOnce() -> MapUsage) -> StoreError {
    match e {
        heed::Error::Mdb(heed::MdbError::MapFull) => {
            let u = usage();
            StoreError::MapFull {
                used_bytes: u.used_bytes,
                map_bytes: u.map_bytes,
            }
        }
        heed::Error::Mdb(heed::MdbError::ReadersFull) => StoreError::ReadersFull {
            max_readers: TraceOpts::default().max_readers,
        },
        heed::Error::Mdb(heed::MdbError::BadRslot) => StoreError::NestedRead,
        heed::Error::Mdb(heed::MdbError::Panic) => StoreError::EnvDead {
            detail: format!("traces: {}", heed::MdbError::Panic),
        },
        heed::Error::Io(io) => StoreError::Io(format!("traces: {io}")),
        other => StoreError::Mdb(format!("traces: {other}")),
    }
}

impl TraceStore {
    fn open_at(dir: &Path, opts: &TraceOpts) -> Result<TraceStore> {
        // LMDB does not pre-allocate the map (no `MDB_WRITEMAP`): the data
        // file's length is what the store's map-size rule needs.
        let used = std::fs::metadata(dir.join("data.mdb"))
            .map(|m| m.len())
            .unwrap_or(0);
        let rule = StoreOpts {
            map_bytes: opts.map_bytes,
            ..StoreOpts::default()
        };
        let mut o = EnvOpenOptions::new();
        o.map_size(rule.map_size_for(used, MAP_ROUND));
        o.max_dbs(8);
        o.max_readers(opts.max_readers);
        // SAFETY: `MDB_NOSYNC` changes the durability contract, which is the
        // point: the durable point decides when these bytes reach the platter
        // ([`TraceStore::sync`]), as for the store (its pin 1).
        unsafe { o.flags(EnvFlags::NO_SYNC) };
        let env: Env<WithTls> = match unsafe { o.open(dir) } {
            Ok(env) => env,
            Err(heed::Error::EnvAlreadyOpened) => {
                // A previous instance of this process is still closing (its
                // last owner dropped it a moment ago): wait for LMDB to let go
                // of the directory, once.
                if let Some(ev) = heed::env_closing_event(dir) {
                    ev.wait_timeout(std::time::Duration::from_secs(10));
                }
                unsafe { o.open(dir) }.map_err(|e| {
                    StoreError::Io(format!("open the trace store at {}: {e}", dir.display()))
                })?
            }
            Err(e) => {
                return Err(StoreError::Mdb(format!(
                    "open the trace store at {}: {e}",
                    dir.display()
                )))
            }
        };
        let seeds = Db::ALL.map(Db::seed);
        let mut w = env
            .write_txn()
            .map_err(|e| StoreError::Mdb(format!("traces: open write txn: {e}")))?;
        let mut dbs = Vec::with_capacity(Db::ALL.len());
        for db in Db::ALL {
            dbs.push(
                env.create_database::<Bytes, Bytes>(&mut w, Some(db.name()))
                    .map_err(|e| StoreError::Mdb(format!("traces: create {}: {e}", db.name())))?,
            );
        }
        let dbs: [Database<Bytes, Bytes>; 5] = dbs.try_into().expect("five databases");
        let meta = dbs[Db::Meta as usize];
        let seal = |key: &[u8], val: &[u8]| integrity::seal_vec(seeds[0], key, val);
        let open_meta = |key: &[u8], stored: &[u8]| -> Result<Vec<u8>> {
            integrity::open(seeds[0], key, stored)
                .map(<[u8]>::to_vec)
                .map_err(|m| corrupt(Db::Meta, format!("{m:?} at {key:?}")))
        };
        let format = match meta
            .get(&w, META_FORMAT)
            .map_err(|e| StoreError::Mdb(format!("traces: {e}")))?
        {
            Some(stored) => {
                let v = open_meta(META_FORMAT, stored)?;
                u32::from_le_bytes(
                    v.as_slice()
                        .try_into()
                        .map_err(|_| corrupt(Db::Meta, "format row"))?,
                )
            }
            None => {
                meta.put(
                    &mut w,
                    META_FORMAT,
                    &seal(META_FORMAT, &FORMAT.to_le_bytes()),
                )
                .map_err(|e| StoreError::Mdb(format!("traces: {e}")))?;
                FORMAT
            }
        };
        if format != FORMAT {
            return Err(StoreError::Io(format!(
                "the trace store at {} is format {format}; this build reads format {FORMAT}",
                dir.display()
            )));
        }
        let applied = match meta
            .get(&w, META_APPLIED)
            .map_err(|e| StoreError::Mdb(format!("traces: {e}")))?
        {
            Some(stored) => {
                let v = open_meta(META_APPLIED, stored)?;
                u64::from_le_bytes(
                    v.as_slice()
                        .try_into()
                        .map_err(|_| corrupt(Db::Meta, "applied index row"))?,
                )
            }
            None => 0,
        };
        w.commit()
            .map_err(|e| StoreError::Mdb(format!("traces: commit the databases: {e}")))?;
        env.force_sync()
            .map_err(|e| StoreError::Mdb(format!("traces: sync the databases: {e}")))?;
        let max_key = env.max_key_size();
        tracing::info!(
            target: "rsm",
            dir = %dir.display(),
            applied,
            "trace store open",
        );
        Ok(TraceStore {
            env,
            dbs,
            seeds,
            dir: dir.to_path_buf(),
            max_key,
            applied: AtomicU64::new(applied),
            unsynced: AtomicBool::new(false),
            #[cfg(test)]
            fail_sync: AtomicBool::new(false),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The index of the last entry whose trace writes this environment holds
    /// (0: none). Apply runs an entry's trace writes only above it.
    pub fn applied_index(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }

    pub fn max_key_len(&self) -> usize {
        self.max_key
    }

    fn db(&self, db: Db) -> Database<Bytes, Bytes> {
        self.dbs[db as usize]
    }

    fn err(&self, e: heed::Error) -> StoreError {
        mdb(e, || self.map_usage())
    }

    /// A stored value's logical bytes, its checksum verified.
    fn open_value<'v>(&self, db: Db, key: &[u8], stored: &'v [u8]) -> Result<&'v [u8]> {
        integrity::open(self.seeds[db as usize], key, stored).map_err(|m| {
            StoreError::CorruptValue {
                keyspace: db.label(),
                key: key.to_vec(),
                detail: format!("{m:?}"),
            }
        })
    }

    fn check_key(&self, db: Db, key: &[u8]) -> Result<()> {
        if key.len() > self.max_key {
            return Err(StoreError::KeyTooLong {
                keyspace: db.label(),
                len: key.len(),
                max: self.max_key,
            });
        }
        Ok(())
    }

    /// The §11.8 numbers of this environment, for the admission gate.
    pub fn map_usage(&self) -> MapUsage {
        let info = self.env.info();
        let page = self.env.stat().page_size as u64;
        MapUsage {
            map_bytes: info.map_size as u64,
            used_bytes: (info.last_page_number as u64 + 1) * page,
            readers_in_use: info.number_of_readers,
            max_readers: info.maximum_number_of_readers,
        }
    }

    /// The bytes the data file occupies.
    pub fn disk_bytes(&self) -> u64 {
        std::fs::metadata(self.dir.join("data.mdb"))
            .map(|m| m.len())
            .unwrap_or(0)
    }

    /// Run the trace half of entry `index` in ONE write transaction: `f`'s
    /// writes, then `applied_index = index`, then a commit (`MDB_NOSYNC`:
    /// readers see it at once; [`TraceStore::sync`] makes it durable). A
    /// refusal from `f` aborts the transaction, so a failed entry leaves
    /// nothing here. Only the apply thread calls it, and only for an index
    /// above [`TraceStore::applied_index`].
    pub fn write_entry<R>(
        &self,
        index: u64,
        f: impl FnOnce(&mut TraceWrite<'_>) -> Result<R>,
    ) -> Result<R> {
        let applied = self.applied_index();
        if index <= applied {
            return Err(StoreError::Io(format!(
                "traces: entry {index} is at or below the trace store's applied index {applied}"
            )));
        }
        let txn = self.env.write_txn().map_err(|e| self.err(e))?;
        let mut w = TraceWrite {
            store: self,
            txn,
            index,
        };
        let out = f(&mut w)?;
        w.put(Db::Meta, META_APPLIED, index.to_le_bytes().to_vec())?;
        let TraceWrite { txn, .. } = w;
        txn.commit().map_err(|e| match e {
            heed::Error::Mdb(heed::MdbError::MapFull) => self.err(e),
            other => StoreError::CommitFailed {
                durable: false,
                detail: format!("traces: {other}"),
            },
        })?;
        self.applied.store(index, Ordering::Release);
        self.unsynced.store(true, Ordering::Release);
        Ok(out)
    }

    /// Make every committed trace survive a power loss: the trace half of a
    /// durable point, run before the store records the point's index. A no-op
    /// when nothing was committed since the last one. A failure is a durable
    /// point that did not happen (`CommitFailed { durable: true }`).
    pub fn sync(&self) -> Result<()> {
        if !self.unsynced.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        #[cfg(test)]
        if self.fail_sync.swap(false, Ordering::AcqRel) {
            self.unsynced.store(true, Ordering::Release);
            return Err(StoreError::CommitFailed {
                durable: true,
                detail: "traces: injected fsync failure (EIO)".into(),
            });
        }
        self.env.force_sync().map_err(|e| {
            self.unsynced.store(true, Ordering::Release);
            StoreError::CommitFailed {
                durable: true,
                detail: format!("traces: {e}"),
            }
        })
    }

    /// TEST ONLY: the next [`TraceStore::sync`] fails.
    #[cfg(test)]
    pub fn fail_next_sync(&self) {
        self.fail_sync.store(true, Ordering::Release);
    }

    /// Read in ONE read transaction (the store's pin 2: it begins and ends in
    /// this call and never crosses an `.await`).
    pub fn read<R>(&self, f: impl FnOnce(&TraceRead<'_>) -> Result<R>) -> Result<R> {
        let txn = self.env.read_txn().map_err(|e| self.err(e))?;
        f(&TraceRead { store: self, txn })
    }

    /// A consistent, compacted copy of the environment as `dir/data.mdb`
    /// (one read transaction), fsynced: the `traces/` part of a snapshot. Its
    /// own `applied_index` says how far it goes.
    pub fn copy_to(&self, dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir)
            .map_err(|e| StoreError::Io(format!("{}: {e}", dir.display())))?;
        let path = dir.join("data.mdb");
        let _ = std::fs::remove_file(&path);
        let file = self
            .env
            .copy_to_path(&path, heed::CompactionOption::Enabled)
            .map_err(|e| {
                StoreError::Mdb(format!("copy the trace store to {}: {e}", path.display()))
            })?;
        file.sync_all()
            .map_err(|e| StoreError::Io(format!("sync {}: {e}", path.display())))?;
        drop(file);
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Writing (the apply thread)
// ---------------------------------------------------------------------------

/// The write transaction of one entry's trace half ([`TraceStore::write_entry`]).
pub struct TraceWrite<'s> {
    store: &'s TraceStore,
    txn: RwTxn<'s>,
    index: u64,
}

impl TraceWrite<'_> {
    fn put(&mut self, db: Db, key: &[u8], mut val: Vec<u8>) -> Result<()> {
        self.store.check_key(db, key)?;
        let sum = integrity::checksum(self.store.seeds[db as usize], key, &val);
        val.extend_from_slice(&sum.to_le_bytes());
        self.store
            .db(db)
            .put(&mut self.txn, key, &val)
            .map_err(|e| self.store.err(e))
    }

    fn del(&mut self, db: Db, key: &[u8]) -> Result<bool> {
        self.store
            .db(db)
            .delete(&mut self.txn, key)
            .map_err(|e| self.store.err(e))
    }

    /// Record `ev` as the trace at `ordinal` of this entry.
    pub fn record(&mut self, ordinal: u32, ev: &TraceEvent) -> Result<()> {
        let id = TraceId {
            index: self.index,
            ordinal,
        };
        let (by_txn, by_name) = index_keys(ev, id);
        // Every key first: a refusal leaves nothing half written (and the
        // transaction is aborted anyway).
        self.store.check_key(Db::ByTxn, &by_txn)?;
        for (k, _) in &by_name {
            self.store.check_key(Db::ByName, k)?;
        }
        let mut msg = Vec::with_capacity(ev.txn.len() + 11);
        push_msg(&mut msg, ev.pid, &ev.txn);

        self.put(Db::Bodies, &id.key(), rows::trace_encode(ev))?;
        self.put(Db::ByTxn, &by_txn, Vec::new())?;
        for (k, count) in &by_name {
            let mut v = Vec::with_capacity(4 + msg.len());
            v.extend_from_slice(&count.to_be_bytes());
            v.extend_from_slice(&msg);
            self.put(Db::ByName, k, v)?;
        }
        // What expiry needs to find every row of this trace again: the
        // tenant, the message and the distinct names.
        let mut v = Vec::with_capacity(ev.tenant.len() + msg.len() + 8 + 16 * by_name.len());
        keys::push_name(&mut v, &ev.tenant);
        v.extend_from_slice(&msg);
        let mut names: Vec<&str> = ev.names.iter().map(String::as_str).collect();
        names.sort_unstable();
        names.dedup();
        v.extend_from_slice(&(names.len() as u32).to_be_bytes());
        for n in names {
            keys::push_name(&mut v, n);
        }
        self.put(Db::Expiry, &expiry_key(ev.created_at_us, id), v)
    }

    /// Delete one trace's rows, from its expiry row.
    fn drop_trace(&mut self, expiry: &[u8], stored: &[u8]) -> Result<()> {
        let v = self.store.open_value(Db::Expiry, expiry, stored)?.to_vec();
        let bad = || corrupt(Db::Expiry, "expiry row");
        let (created, id) = read_tail(expiry).ok_or_else(bad)?;
        let (tenant, at) = keys::read_name(&v, 0).ok_or_else(bad)?;
        let msg_to = msg_end(&v, at).ok_or_else(bad)?;
        let msg = &v[at..msg_to];
        let n = keys::read_u32(&v, msg_to).ok_or_else(bad)? as usize;
        let mut k = Vec::with_capacity(tenant.len() + msg.len() + 4 + TAIL);
        keys::push_name(&mut k, &tenant);
        k.extend_from_slice(msg);
        push_tail(&mut k, created, id);
        self.del(Db::ByTxn, &k)?;
        let mut at = msg_to + 4;
        for _ in 0..n {
            let (name, end) = keys::read_name(&v, at).ok_or_else(bad)?;
            at = end;
            let mut k = name_prefix(&tenant, &name);
            push_tail(&mut k, created, id);
            self.del(Db::ByName, &k)?;
        }
        self.del(Db::Bodies, &id.key())?;
        self.del(Db::Expiry, expiry)?;
        Ok(())
    }

    /// Delete at most `limit` traces created before `cutoff_us`, oldest
    /// first. Returns how many went.
    pub fn trim(&mut self, cutoff_us: i64, limit: usize) -> Result<usize> {
        let mut due: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        {
            let iter = self
                .store
                .db(Db::Expiry)
                .iter(&self.txn)
                .map_err(|e| self.store.err(e))?;
            for row in iter {
                if due.len() >= limit {
                    break;
                }
                let (k, v) = row.map_err(|e| self.store.err(e))?;
                let created = keys::read_i64(k, 0).ok_or_else(|| corrupt(Db::Expiry, "key"))?;
                if created >= cutoff_us {
                    break;
                }
                due.push((k.to_vec(), v.to_vec()));
            }
        }
        for (k, v) in &due {
            self.drop_trace(k, v)?;
        }
        Ok(due.len())
    }

    /// Delete every trace of `tenant`. Returns how many went.
    pub fn purge_tenant(&mut self, tenant: &str) -> Result<usize> {
        let prefix = keys::queues_prefix(tenant);
        let mut n = 0usize;
        loop {
            let mut chunk: Vec<Vec<u8>> = Vec::new();
            {
                let iter = self
                    .store
                    .db(Db::ByTxn)
                    .prefix_iter(&self.txn, &prefix)
                    .map_err(|e| self.store.err(e))?;
                for row in iter {
                    let (k, _) = row.map_err(|e| self.store.err(e))?;
                    chunk.push(k.to_vec());
                    if chunk.len() >= PURGE_CHUNK {
                        break;
                    }
                }
            }
            if chunk.is_empty() {
                break;
            }
            for k in &chunk {
                let (created, id) = read_tail(k).ok_or_else(|| corrupt(Db::ByTxn, "key"))?;
                self.del(Db::Bodies, &id.key())?;
                self.del(Db::Expiry, &expiry_key(created, id))?;
                self.del(Db::ByTxn, k)?;
            }
            n += chunk.len();
        }
        let end = prefix_end(&prefix);
        let range: (Bound<&[u8]>, Bound<&[u8]>) = (
            Bound::Included(&prefix[..]),
            end.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
        );
        self.store
            .db(Db::ByName)
            .delete_range(&mut self.txn, &range)
            .map_err(|e| self.store.err(e))?;
        Ok(n)
    }
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

/// One read transaction over the environment ([`TraceStore::read`]).
pub struct TraceRead<'s> {
    store: &'s TraceStore,
    txn: RoTxn<'s, WithTls>,
}

/// What `/traces/names` reports for one name.
#[derive(Clone, Debug, Default)]
pub struct NameStat {
    /// Traces carrying the name, a trace that repeats it counted as often.
    pub traces: usize,
    /// Distinct messages (`pid?, txn`), by [`message_hash`].
    pub messages: HashSet<u128>,
    pub last_seen_us: i64,
}

impl NameStat {
    pub fn add(&mut self, traces: usize, message: u128, created_at_us: i64) {
        if self.traces == 0 {
            self.last_seen_us = created_at_us;
        }
        self.traces += traces;
        self.messages.insert(message);
        self.last_seen_us = self.last_seen_us.max(created_at_us);
    }
}

impl TraceRead<'_> {
    fn err(&self, e: heed::Error) -> StoreError {
        self.store.err(e)
    }

    /// The applied index this transaction sees.
    pub fn applied_index(&self) -> Result<u64> {
        let db = self.store.db(Db::Meta);
        match db.get(&self.txn, META_APPLIED).map_err(|e| self.err(e))? {
            None => Ok(0),
            Some(stored) => {
                let v = self.store.open_value(Db::Meta, META_APPLIED, stored)?;
                Ok(u64::from_le_bytes(
                    v.try_into()
                        .map_err(|_| corrupt(Db::Meta, "applied index row"))?,
                ))
            }
        }
    }

    /// The traces of one message, newest first: `cb(created_at_us, tie, id)`
    /// until it returns false. `tie` orders traces of one instant as the
    /// legacy read did (`msg`, then apply order; [`Pager`]).
    pub fn message_rev(
        &self,
        tenant: &str,
        pid: Option<Pid>,
        txn: &str,
        cb: &mut dyn FnMut(i64, &[u8], TraceId) -> bool,
    ) -> Result<()> {
        let prefix = txn_prefix(tenant, pid, txn);
        let msg_at = keys::queues_prefix(tenant).len();
        let mut tie = Vec::with_capacity(prefix.len() + 13);
        let iter = self
            .store
            .db(Db::ByTxn)
            .rev_prefix_iter(&self.txn, &prefix)
            .map_err(|e| self.err(e))?;
        for row in iter {
            let (k, _) = row.map_err(|e| self.err(e))?;
            let (created, id) = read_tail(k).ok_or_else(|| corrupt(Db::ByTxn, "key"))?;
            tie.clear();
            tie.extend_from_slice(&k[msg_at..k.len() - TAIL]);
            tie.push(1);
            tie.extend_from_slice(&id.key());
            if !cb(created, &tie, id) {
                break;
            }
        }
        Ok(())
    }

    /// The traces carrying `name`, newest first, as [`TraceRead::message_rev`].
    pub fn name_rev(
        &self,
        tenant: &str,
        name: &str,
        cb: &mut dyn FnMut(i64, &[u8], TraceId) -> bool,
    ) -> Result<()> {
        let prefix = name_prefix(tenant, name);
        let mut tie = Vec::with_capacity(64);
        let iter = self
            .store
            .db(Db::ByName)
            .rev_prefix_iter(&self.txn, &prefix)
            .map_err(|e| self.err(e))?;
        for row in iter {
            let (k, stored) = row.map_err(|e| self.err(e))?;
            let v = self.store.open_value(Db::ByName, k, stored)?;
            let (created, id) = read_tail(k).ok_or_else(|| corrupt(Db::ByName, "key"))?;
            let msg = v.get(4..).ok_or_else(|| corrupt(Db::ByName, "value"))?;
            tie.clear();
            tie.extend_from_slice(msg);
            tie.push(1);
            tie.extend_from_slice(&id.key());
            if !cb(created, &tie, id) {
                break;
            }
        }
        Ok(())
    }

    /// Fold every trace name of `tenant` into `out` (the `/traces/names`
    /// listing), from the index alone.
    pub fn name_stats(&self, tenant: &str, out: &mut BTreeMap<String, NameStat>) -> Result<()> {
        let prefix = keys::queues_prefix(tenant);
        let iter = self
            .store
            .db(Db::ByName)
            .prefix_iter(&self.txn, &prefix)
            .map_err(|e| self.err(e))?;
        // Rows of one name are adjacent: decode a name once per run.
        let mut cur_raw: Vec<u8> = Vec::new();
        let mut cur: Option<(String, NameStat)> = None;
        for row in iter {
            let (k, stored) = row.map_err(|e| self.err(e))?;
            let v = self.store.open_value(Db::ByName, k, stored)?;
            let bad = || corrupt(Db::ByName, "row");
            let (created, _) = read_tail(k).ok_or_else(bad)?;
            let raw = &k[prefix.len()..k.len() - TAIL];
            if cur.is_none() || raw != cur_raw.as_slice() {
                if let Some((name, stat)) = cur.take() {
                    merge_stat(out, name, stat);
                }
                let (name, _) = keys::read_name(raw, 0).ok_or_else(bad)?;
                cur_raw.clear();
                cur_raw.extend_from_slice(raw);
                cur = Some((name, NameStat::default()));
            }
            let count = keys::read_u32(v, 0).ok_or_else(bad)? as usize;
            let msg = xxh3_128(v.get(4..).ok_or_else(bad)?);
            if let Some((_, stat)) = cur.as_mut() {
                stat.add(count, msg, created);
            }
        }
        if let Some((name, stat)) = cur.take() {
            merge_stat(out, name, stat);
        }
        Ok(())
    }

    /// One trace's event, its checksum verified.
    pub fn body(&self, id: TraceId) -> Result<Option<TraceEvent>> {
        let k = id.key();
        let Some(stored) = self
            .store
            .db(Db::Bodies)
            .get(&self.txn, &k)
            .map_err(|e| self.err(e))?
        else {
            return Ok(None);
        };
        let v = self.store.open_value(Db::Bodies, &k, stored)?;
        rows::trace_decode(v)
            .map(Some)
            .map_err(|e| corrupt(Db::Bodies, format!("{e}")))
    }

    /// Traces created before `cutoff_us`, counted up to `limit`: what the
    /// leader's maintenance asks before it plans a [`TraceWrite::trim`].
    pub fn due(&self, cutoff_us: i64, limit: usize) -> Result<usize> {
        let mut n = 0usize;
        let iter = self
            .store
            .db(Db::Expiry)
            .iter(&self.txn)
            .map_err(|e| self.err(e))?;
        for row in iter {
            if n >= limit {
                break;
            }
            let (k, _) = row.map_err(|e| self.err(e))?;
            let created = keys::read_i64(k, 0).ok_or_else(|| corrupt(Db::Expiry, "key"))?;
            if created >= cutoff_us {
                break;
            }
            n += 1;
        }
        Ok(n)
    }

    /// Rows per database: `(bodies, by_txn, by_name, expiry)`.
    pub fn rows(&self) -> Result<(u64, u64, u64, u64)> {
        let len = |db: Db| self.store.db(db).len(&self.txn).map_err(|e| self.err(e));
        Ok((
            len(Db::Bodies)?,
            len(Db::ByTxn)?,
            len(Db::ByName)?,
            len(Db::Expiry)?,
        ))
    }

    /// Every row but `meta`'s, hashed in key order: equal on two nodes that
    /// applied the same entries (the trace half of the I2 digest; the applied
    /// index is how far this node got, not what it holds).
    pub fn digest(&self) -> Result<u128> {
        let mut h = xxhash_rust::xxh3::Xxh3::new();
        for db in [Db::Bodies, Db::ByTxn, Db::ByName, Db::Expiry] {
            h.update(db.name().as_bytes());
            let iter = self.store.db(db).iter(&self.txn).map_err(|e| self.err(e))?;
            for row in iter {
                let (k, v) = row.map_err(|e| self.err(e))?;
                h.update(&(k.len() as u64).to_le_bytes());
                h.update(k);
                h.update(&(v.len() as u64).to_le_bytes());
                h.update(v);
            }
        }
        Ok(h.digest128())
    }
}

fn merge_stat(out: &mut BTreeMap<String, NameStat>, name: String, stat: NameStat) {
    match out.get_mut(&name) {
        Some(have) => {
            if have.traces == 0 {
                have.last_seen_us = stat.last_seen_us;
            }
            have.traces += stat.traces;
            have.messages.extend(stat.messages);
            have.last_seen_us = have.last_seen_us.max(stat.last_seen_us);
        }
        None => {
            out.insert(name, stat);
        }
    }
}

// ---------------------------------------------------------------------------
// One page of a newest-first listing
// ---------------------------------------------------------------------------

/// One page of traces in the legacy order — `created_at` descending, and
/// traces of one instant by `tie` ascending — from rows fed newest first. It
/// counts every row and keeps only the page, so a listing holds `limit` rows
/// and the rows of one instant, never every match.
pub struct Pager<T> {
    offset: usize,
    limit: usize,
    seen: usize,
    group: Vec<(i64, Vec<u8>, T)>,
    page: Vec<T>,
}

impl<T> Pager<T> {
    pub fn new(offset: usize, limit: usize) -> Pager<T> {
        Pager {
            offset,
            limit,
            seen: 0,
            group: Vec::new(),
            page: Vec::new(),
        }
    }

    /// Feed one row. Rows must come in `created_at` descending order; within
    /// one instant they may come in any order.
    pub fn push(&mut self, created_at_us: i64, tie: &[u8], item: T) {
        if self
            .group
            .first()
            .is_some_and(|(at, _, _)| *at != created_at_us)
        {
            self.flush();
        }
        // The page is full and no instant is open: this row and every later
        // one sort after the page, so they are only counted.
        if self.group.is_empty() && self.seen >= self.offset.saturating_add(self.limit) {
            self.seen += 1;
            return;
        }
        self.group.push((created_at_us, tie.to_vec(), item));
    }

    fn flush(&mut self) {
        self.group.sort_by(|a, b| a.1.cmp(&b.1));
        for (_, _, item) in self.group.drain(..) {
            if self.seen >= self.offset && self.seen - self.offset < self.limit {
                self.page.push(item);
            }
            self.seen += 1;
        }
    }

    /// `(total, page)`.
    pub fn finish(mut self) -> (usize, Vec<T>) {
        self.flush();
        (self.seen, self.page)
    }
}

/// The tie of a legacy row (`keys::trace` primary key): its `msg`, a 0 (legacy
/// rows were all written before any row here), and its sequence.
pub fn legacy_tie(tenant: &str, primary: &[u8]) -> Option<Vec<u8>> {
    let at = keys::queues_prefix(tenant).len();
    let seq_at = primary.len().checked_sub(8)?;
    if seq_at < at {
        return None;
    }
    let mut t = Vec::with_capacity(primary.len() - at + 1);
    t.extend_from_slice(&primary[at..seq_at]);
    t.push(0);
    t.extend_from_slice(&primary[seq_at..]);
    Some(t)
}

#[cfg(test)]
mod tests;
