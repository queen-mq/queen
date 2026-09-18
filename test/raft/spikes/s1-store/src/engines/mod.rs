//! The store seam: one trait, three engines.
//!
//! The shape is PLAN_RAFT.md §11.3. Two shapes are now measurable:
//!   * `legacy`   — the pre-G0 shape: one write transaction per applied entry,
//!                  `segments` rows in the store, no dedup keyspace.
//!   * `ratified` — the G0 amendments: the write transaction is held open and
//!                  committed every `--store-commit-ms` / `--store-commit-entries`
//!                  (4 ms / 256), `segments` is NOT in the store (§6.1 G0
//!                  amendment: one immutable `.qidx` per sealed file), and a
//!                  `dedup` keyspace of uniformly random `(pid, hash)` keys
//!                  carries D10 option (a), one row per message.
//! Both shapes take a durable commit at the durable point (§11.4), use read
//! transactions for local reads, and support ordered iteration for digests,
//! chunked deletes and snapshot export.
//!
//! Every engine here writes the SAME logical rows (the keyspaces of §6.1,
//! plus the node-local `seg_loc` of §6.2), so their numbers are comparable.

use std::path::Path;

pub mod fjall_eng;
pub mod heed_eng;
pub mod redb_eng;

// Table ids. The order is the order of TABLES; ids are used as array indexes.
pub const T_META: u8 = 0;
pub const T_SEGMENTS: u8 = 1;
pub const T_SEG_LOC: u8 = 2; // node-local (§6.2)
pub const T_PARTITIONS: u8 = 3;
pub const T_CURSORS: u8 = 4;
pub const T_PENDING: u8 = 5;
pub const T_KV: u8 = 6;
pub const T_KV_EXPIRY: u8 = 7;
pub const T_TIMERS: u8 = 8;
pub const T_TIMERS_DUE: u8 = 9;
pub const T_REQUEST_IDS: u8 = 10;
pub const T_REQUEST_EXPIRY: u8 = 11;
pub const T_COUNTERS: u8 = 12;
pub const T_DEDUP: u8 = 13; // D10 option (a): (pid, hash) -> (offset, created_at)

pub const TABLES: [&str; 14] = [
    "meta",
    "segments",
    "seg_loc",
    "partitions",
    "cursors",
    "pending",
    "kv",
    "kv_expiry",
    "timers",
    "timers_due",
    "request_ids",
    "request_expiry",
    "counters",
    "dedup",
];
pub const NTABLES: usize = TABLES.len();

/// Fixed meta keys.
pub const M_APPLIED: &[u8] = b"applied_index";
pub const M_DURABLE: &[u8] = b"durable_index";
pub const M_FILELENS: &[u8] = b"file_lengths";

/// One entry's writes, in flat buffers so the steady state allocates nothing.
#[derive(Default)]
pub struct OpBuf {
    keys: Vec<u8>,
    vals: Vec<u8>,
    ops: Vec<OpRec>,
}

#[derive(Clone, Copy)]
pub struct OpRec {
    pub table: u8,
    pub del: bool,
    koff: u32,
    klen: u32,
    voff: u32,
    vlen: u32,
}

impl OpBuf {
    pub fn new() -> Self {
        Self {
            keys: Vec::with_capacity(4096),
            vals: Vec::with_capacity(16384),
            ops: Vec::with_capacity(64),
        }
    }

    pub fn clear(&mut self) {
        self.keys.clear();
        self.vals.clear();
        self.ops.clear();
    }

    pub fn put(&mut self, table: u8, key: &[u8], val: &[u8]) {
        let koff = self.keys.len() as u32;
        self.keys.extend_from_slice(key);
        let voff = self.vals.len() as u32;
        self.vals.extend_from_slice(val);
        self.ops.push(OpRec {
            table,
            del: false,
            koff,
            klen: key.len() as u32,
            voff,
            vlen: val.len() as u32,
        });
    }

    pub fn del(&mut self, table: u8, key: &[u8]) {
        let koff = self.keys.len() as u32;
        self.keys.extend_from_slice(key);
        self.ops.push(OpRec {
            table,
            del: true,
            koff,
            klen: key.len() as u32,
            voff: 0,
            vlen: 0,
        });
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn key(&self, o: &OpRec) -> &[u8] {
        &self.keys[o.koff as usize..(o.koff + o.klen) as usize]
    }

    pub fn val(&self, o: &OpRec) -> &[u8] {
        &self.vals[o.voff as usize..(o.voff + o.vlen) as usize]
    }

    pub fn recs(&self) -> &[OpRec] {
        &self.ops
    }

    /// Key + value bytes of this entry: the denominator of write amplification.
    pub fn logical_bytes(&self) -> u64 {
        (self.keys.len() + self.vals.len()) as u64
    }
}

pub struct ExportPart {
    pub method: &'static str,
    pub bytes: u64,
    pub ms: f64,
}

#[derive(Clone, Copy)]
pub struct Caps {
    /// Can the engine produce a snapshot incrementally (files that can be
    /// hard-linked or shipped as a delta), or is every export O(state)? (I8)
    pub incremental_checkpoint: &'static str,
    pub export_method: &'static str,
    /// Does the export pause the writer?
    pub writer_pause: &'static str,
    /// What the engine is expected to do after kill -9 with non-durable
    /// commits outstanding (checked, not trusted, by `verify`).
    pub crash_model: &'static str,
}

/// Engine open options. `map_size` and `cache` are the two the first campaign
/// had; the `heed_*` fields exist because the refutation of MEMO.md showed the
/// spike had measured exactly one LMDB configuration (`NO_SYNC`, TLS reader
/// slots, default `max_readers`) and none of the others the broker could ship.
#[derive(Clone)]
pub struct EngOpts {
    pub map_size: usize,
    pub cache_bytes: u64,
    /// `nosync` (MDB_NOSYNC, the original), `nometasync` (MDB_NOMETASYNC:
    /// data pages are flushed on every commit, the meta page is not — the
    /// header says this "maintains database integrity, but a system crash may
    /// undo the last committed transaction"), `both`, or `none`.
    pub heed_flags: String,
    /// Open the env with MDB_NOTLS (heed `read_txn_without_tls`), which is the
    /// only mode in which `RoTxn` is `Send` and nested read txns are legal.
    pub heed_no_tls: bool,
    /// LMDB reader-slot table size (default 126). 0 = leave the default.
    pub heed_max_readers: u32,
}

impl Default for EngOpts {
    fn default() -> Self {
        Self {
            map_size: 32usize << 30,
            cache_bytes: 256 << 20,
            heed_flags: "nosync".into(),
            heed_no_tls: false,
            heed_max_readers: 0,
        }
    }
}

pub trait Engine: Send + Sync {
    #[allow(dead_code)]
    fn name(&self) -> &'static str;

    /// One line describing the configuration this instance was opened with
    /// (which LMDB flags, which reader mode, …). Printed in every run header so
    /// a result can never again be read as if it covered every configuration.
    fn config_note(&self) -> String {
        String::new()
    }

    /// One write transaction with every op of an applied entry.
    /// `durable` = the durable point of §11.4 (store commit that must survive
    /// a power loss).
    fn commit(&mut self, ops: &OpBuf, durable: bool) -> Result<(), String>;

    fn get(&self, table: u8, key: &[u8]) -> Result<Option<Vec<u8>>, String>;

    /// Ordered range scan from `prefix`, stopping at the first key that does
    /// not start with it or after `limit` rows. Returns rows seen.
    fn prefix_count(&self, table: u8, prefix: &[u8], limit: usize) -> Result<usize, String>;

    /// Full ordered iteration. Returns (rows, key+value bytes).
    fn scan(&self, table: u8) -> Result<(u64, u64), String>;

    /// Full ordered iteration with a callback; the callback returns false to stop.
    fn scan_cb(&self, table: u8, cb: &mut dyn FnMut(&[u8], &[u8]) -> bool) -> Result<u64, String>;

    /// Consistent export of the whole store into `dest` (§11.6 step 3).
    fn export(&self, dest: &Path) -> Result<Vec<ExportPart>, String>;

    /// Reclaim space after deletes. Returns a note describing what it did.
    fn maintain(&mut self) -> Result<String, String>;

    /// Open `n` read transactions at once and hold them all, to find the
    /// engine's reader-slot ceiling. Returns (how many opened, the error that
    /// stopped it). Engines with no such ceiling open all `n`.
    fn hold_readers(&self, n: usize) -> Result<(usize, String), String> {
        Ok((n, String::new()))
    }

    fn caps(&self) -> Caps;

    /// Sum of the engine's own on-disk files, as the engine reports it (0 if
    /// it does not); the harness always also measures the directory.
    fn reported_disk(&self) -> u64 {
        0
    }
}

pub fn open(engine: &str, dir: &Path, o: &EngOpts) -> Result<Box<dyn Engine>, String> {
    match engine {
        "redb" => Ok(Box::new(redb_eng::Redb::open(dir, o.cache_bytes)?)),
        "fjall" => Ok(Box::new(fjall_eng::Fjall::open(dir, o.cache_bytes)?)),
        "heed" => heed_eng::open(dir, o),
        other => Err(format!("unknown engine {other} (want redb|fjall|heed)")),
    }
}

pub const ALL: [&str; 3] = ["redb", "fjall", "heed"];
