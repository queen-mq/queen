//! The store seam: one trait, three engines.
//!
//! COPIED from `test/raft/spikes/s1-store/src/engines/` (WP-0.3, spike S1) on
//! 2026-09-17, with three deliberate changes and nothing else:
//!   1. the table list is S2's dedup keyspaces instead of S1's apply stream;
//!   2. `Engine::range` was added — an ordered range scan that yields KEYS and
//!      VALUES (S1 only needed `prefix_count`, and S2's probes need the keys:
//!      "the MIN offset of hash H in this span", "the partition's Append
//!      records inside the txns window");
//!   3. nothing was removed: `export`, `maintain` and `caps` are S1's, and S2
//!      uses `maintain` to separate "rows deleted" from "bytes given back".
//! S1's own sources are NOT touched, so a parallel S1 run (part 2, the VM)
//! cannot be disturbed by this spike. `hist.rs` and `sys.rs` are shared with
//! S1 as-is, included with `#[path]` from `main.rs`.
//!
//! The shape is PLAN_RAFT.md §11.3: one write transaction per applied entry,
//! non-durable by default, a durable commit at the durable point (§11.4),
//! read transactions for local reads, ordered iteration, chunked deletes.

use std::path::Path;

pub mod fjall_eng;
pub mod heed_eng;
pub mod redb_eng;

// Table ids. The order is the order of TABLES; ids are used as array indexes.
pub const T_META: u8 = 0;
/// (pid, base_offset) -> segment row: what retention deletes (§6.1 `segments`).
pub const T_SEGMENTS: u8 = 1;
/// (pid, base_offset) -> node-local position of the payload frame (§6.2, D8).
pub const T_SEG_LOC: u8 = 2;
/// Option (b): (pid, base_offset) -> the hash list's locator. Outlives
/// `segments` for the txns window (D10, §11.7).
pub const T_TXNS: u8 = 3;
/// Option (a): (pid, hash16) -> occurrence list [(offset, created_at)].
pub const T_DEDUP: u8 = 4;
/// Option (a): (created_at, pid, hash16) -> (offset): the expiry index.
pub const T_DEDUP_EXPIRY: u8 = 5;
pub const T_PARTITIONS: u8 = 6;
pub const T_CURSORS: u8 = 7;
pub const T_COUNTERS: u8 = 8;

pub const TABLES: [&str; 9] = [
    "meta",
    "segments",
    "seg_loc",
    "txns",
    "dedup",
    "dedup_expiry",
    "partitions",
    "cursors",
    "counters",
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

pub trait Engine {
    #[allow(dead_code)]
    fn name(&self) -> &'static str;

    /// One write transaction with every op of an applied entry.
    /// `durable` = the durable point of §11.4 (store commit that must survive
    /// a power loss).
    fn commit(&mut self, ops: &OpBuf, durable: bool) -> Result<(), String>;

    fn get(&self, table: u8, key: &[u8]) -> Result<Option<Vec<u8>>, String>;

    /// Ordered range scan from `prefix`, stopping at the first key that does
    /// not start with it or after `limit` rows. Returns rows seen.
    #[allow(dead_code)]
    fn prefix_count(&self, table: u8, prefix: &[u8], limit: usize) -> Result<usize, String>;

    /// Ordered range scan from `from` (inclusive), stopping at the first key
    /// that does not start with `prefix`, when the callback returns false, or
    /// after `limit` rows. Returns rows visited. This is the read every dedup
    /// probe needs: the keys carry the offsets.
    fn range(
        &self,
        table: u8,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
        cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<u64, String>;

    /// Full ordered iteration. Returns (rows, key+value bytes).
    fn scan(&self, table: u8) -> Result<(u64, u64), String>;

    /// Full ordered iteration with a callback; the callback returns false to stop.
    fn scan_cb(&self, table: u8, cb: &mut dyn FnMut(&[u8], &[u8]) -> bool) -> Result<u64, String>;

    /// Consistent export of the whole store into `dest` (§11.6 step 3).
    fn export(&self, dest: &Path) -> Result<Vec<ExportPart>, String>;

    /// Reclaim space after deletes. Returns a note describing what it did.
    fn maintain(&mut self) -> Result<String, String>;

    fn caps(&self) -> Caps;

    /// Sum of the engine's own on-disk files, as the engine reports it (0 if
    /// it does not); the harness always also measures the directory.
    fn reported_disk(&self) -> u64 {
        0
    }
}

pub fn open(
    engine: &str,
    dir: &Path,
    map_size: usize,
    cache_bytes: u64,
) -> Result<Box<dyn Engine>, String> {
    match engine {
        "redb" => Ok(Box::new(redb_eng::Redb::open(dir, cache_bytes)?)),
        "fjall" => Ok(Box::new(fjall_eng::Fjall::open(dir, cache_bytes)?)),
        "heed" => Ok(Box::new(heed_eng::Heed::open(dir, map_size)?)),
        other => Err(format!("unknown engine {other} (want redb|fjall|heed)")),
    }
}

pub const ALL: [&str; 3] = ["redb", "fjall", "heed"];
