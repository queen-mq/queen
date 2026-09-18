//! heed 0.22 over lmdb-master-sys 0.2 (LMDB 0.9.x built with `cc`, no cmake).
//!
//! Non-durable commit = a normal write transaction. WHICH FLAGS the env is
//! opened with is a parameter (`--heed-flags`), because the first campaign
//! measured exactly one of them and the memo then reasoned about another:
//!
//!   * `nosync`     MDB_NOSYNC: commit writes dirty pages + meta page through
//!                  the mmap, no fsync at all. lmdb.h: "a system crash can
//!                  corrupt the database or lose the last transactions if
//!                  buffers are not yet flushed to disk ... However, if the
//!                  filesystem preserves write order and the MDB_WRITEMAP flag
//!                  is not used, transactions exhibit ACI ... and only lose D."
//!   * `nometasync` MDB_NOMETASYNC: every commit flushes its own data pages and
//!                  defers only the meta-page flush. lmdb.h: "maintains
//!                  database integrity, but a system crash may undo the last
//!                  committed transaction" — unconditional, no corruption
//!                  clause. That is exactly §11.4's durable-point semantics.
//!   * `both`       MDB_NOSYNC|MDB_NOMETASYNC, `none` = fsync on every commit.
//!
//! Durable point = `Env::force_sync()` (mdb_env_sync with force) in all modes.
//!
//! Reader mode is a parameter too (`--heed-no-tls`, `--heed-max-readers`):
//! heed's default `EnvOpenOptions::new()` is `WithTls`, where `RoTxn` is NOT
//! `Send` (heed 0.22.1 txn.rs:237 implements Send only for `WithoutTls`), so it
//! cannot cross an `.await` or move between tokio workers, and a nested read
//! txn on one thread is illegal. `read_txn_without_tls()` sets MDB_NOTLS
//! (env_open_options.rs:462) and lifts both limits.

use super::{Caps, EngOpts, Engine, ExportPart, OpBuf, NTABLES, TABLES};
use heed::types::Bytes;
use heed::{CompactionOption, Database, Env, EnvFlags, EnvOpenOptions, RwTxn, WithTls, WithoutTls};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};

/// The two reader modes are two DIFFERENT Rust types (`Env<WithTls>` and
/// `Env<WithoutTls>`), and `RoTxn` derefs to the erased `RoTxn<AnyTls>` only
/// for each concrete marker — which is itself the shape of the finding: the
/// reader mode is not a runtime knob in this library, it is part of the type,
/// and the broker has to pick one at compile time.
enum HEnv {
    Tls(Env<WithTls>),
    NoTls(Env<WithoutTls>),
}

/// Runs `$body` with `$r` bound to a read transaction of whichever env this is.
macro_rules! rtxn {
    ($self:expr, $r:ident, $body:block) => {
        match &$self.env {
            HEnv::Tls(en) => {
                let $r = en.read_txn().map_err(e)?;
                $body
            }
            HEnv::NoTls(en) => {
                let $r = en.read_txn().map_err(e)?;
                $body
            }
        }
    };
}

impl HEnv {
    fn write_txn(&self) -> Result<RwTxn<'_>, String> {
        match self {
            HEnv::Tls(e2) => e2.write_txn().map_err(e),
            HEnv::NoTls(e2) => e2.write_txn().map_err(e),
        }
    }
    fn force_sync(&self) -> Result<(), String> {
        match self {
            HEnv::Tls(e2) => e2.force_sync().map_err(e),
            HEnv::NoTls(e2) => e2.force_sync().map_err(e),
        }
    }
    fn max_readers(&self) -> u32 {
        match self {
            HEnv::Tls(e2) => e2.max_readers(),
            HEnv::NoTls(e2) => e2.max_readers(),
        }
    }
    fn copy_to_file(&self, f: &mut File, o: CompactionOption) -> Result<(), String> {
        match self {
            HEnv::Tls(e2) => e2.copy_to_file(f, o).map_err(e),
            HEnv::NoTls(e2) => e2.copy_to_file(f, o).map_err(e),
        }
    }
    fn create_db(&self, w: &mut RwTxn<'_>, name: &str) -> Result<Database<Bytes, Bytes>, String> {
        match self {
            HEnv::Tls(e2) => e2.create_database::<Bytes, Bytes>(w, Some(name)).map_err(e),
            HEnv::NoTls(e2) => e2.create_database::<Bytes, Bytes>(w, Some(name)).map_err(e),
        }
    }
}

pub struct Heed {
    env: HEnv,
    dbs: Vec<Database<Bytes, Bytes>>,
    dir: PathBuf,
    flags_note: &'static str,
    tls_note: &'static str,
    max_readers: u32,
}

fn e<E: std::fmt::Display>(x: E) -> String {
    format!("heed: {x}")
}

fn parse_flags(s: &str) -> Result<(EnvFlags, &'static str), String> {
    Ok(match s {
        "nosync" => (EnvFlags::NO_SYNC, "MDB_NOSYNC"),
        "nometasync" => (EnvFlags::NO_META_SYNC, "MDB_NOMETASYNC"),
        "both" => (
            EnvFlags::NO_SYNC | EnvFlags::NO_META_SYNC,
            "MDB_NOSYNC|MDB_NOMETASYNC",
        ),
        "none" => (EnvFlags::empty(), "(none: fsync per commit)"),
        other => return Err(format!("heed: unknown --heed-flags {other}")),
    })
}

fn finish(
    env: HEnv,
    sdir: PathBuf,
    flags_note: &'static str,
    tls_note: &'static str,
) -> Result<Heed, String> {
    let max_readers = env.max_readers();
    let mut wtxn = env.write_txn()?;
    let mut dbs = Vec::with_capacity(NTABLES);
    for name in TABLES.iter() {
        dbs.push(env.create_db(&mut wtxn, name)?);
    }
    wtxn.commit().map_err(e)?;
    env.force_sync()?;
    Ok(Heed {
        env,
        dbs,
        dir: sdir,
        flags_note,
        tls_note,
        max_readers,
    })
}

/// Opens the env in whichever reader mode `opts` asks for and erases the
/// marker type behind the `Engine` trait object.
pub fn open(dir: &Path, opts: &EngOpts) -> Result<Box<dyn Engine>, String> {
    let sdir = dir.join("store");
    std::fs::create_dir_all(&sdir).map_err(e)?;
    let (flags, flags_note) = parse_flags(&opts.heed_flags)?;
    if opts.heed_no_tls {
        let mut o = EnvOpenOptions::new().read_txn_without_tls();
        o.map_size(opts.map_size);
        o.max_dbs(NTABLES as u32 + 2);
        if opts.heed_max_readers > 0 {
            o.max_readers(opts.heed_max_readers);
        }
        // SAFETY: the flags are what the durable-point protocol (§11.4) needs:
        // the harness, not LMDB, decides when bytes reach the platter.
        unsafe { o.flags(flags) };
        let env: Env<WithoutTls> = unsafe { o.open(&sdir) }.map_err(e)?;
        Ok(Box::new(finish(
            HEnv::NoTls(env),
            sdir,
            flags_note,
            "MDB_NOTLS (RoTxn is Send, nested read txns legal)",
        )?))
    } else {
        let mut o = EnvOpenOptions::new();
        o.map_size(opts.map_size);
        o.max_dbs(NTABLES as u32 + 2);
        if opts.heed_max_readers > 0 {
            o.max_readers(opts.heed_max_readers);
        }
        // SAFETY: as above.
        unsafe { o.flags(flags) };
        let env: Env<WithTls> = unsafe { o.open(&sdir) }.map_err(e)?;
        Ok(Box::new(finish(
            HEnv::Tls(env),
            sdir,
            flags_note,
            "thread-local reader slots (RoTxn is NOT Send, nested read txns illegal)",
        )?))
    }
}

impl Engine for Heed {
    fn name(&self) -> &'static str {
        "heed"
    }

    fn config_note(&self) -> String {
        format!(
            "heed 0.22.1 / LMDB, flags {}, {}, max_readers {}",
            self.flags_note, self.tls_note, self.max_readers
        )
    }

    fn commit(&mut self, ops: &OpBuf, durable: bool) -> Result<(), String> {
        let mut w = self.env.write_txn()?;
        for o in ops.recs() {
            let db = &self.dbs[o.table as usize];
            if o.del {
                db.delete(&mut w, ops.key(o)).map_err(e)?;
            } else {
                db.put(&mut w, ops.key(o), ops.val(o)).map_err(e)?;
            }
        }
        w.commit().map_err(e)?;
        if durable {
            self.env.force_sync()?;
        }
        Ok(())
    }

    fn get(&self, table: u8, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        rtxn!(self, r, {
            Ok(self.dbs[table as usize]
                .get(&r, key)
                .map_err(e)?
                .map(|v| v.to_vec()))
        })
    }

    fn prefix_count(&self, table: u8, prefix: &[u8], limit: usize) -> Result<usize, String> {
        rtxn!(self, r, {
            let range = (Bound::Included(prefix), Bound::Unbounded);
            let mut n = 0usize;
            for row in self.dbs[table as usize].range(&r, &range).map_err(e)? {
                let (k, _v) = row.map_err(e)?;
                if !k.starts_with(prefix) {
                    break;
                }
                n += 1;
                if n >= limit {
                    break;
                }
            }
            Ok(n)
        })
    }

    fn scan(&self, table: u8) -> Result<(u64, u64), String> {
        rtxn!(self, r, {
            let mut rows = 0u64;
            let mut bytes = 0u64;
            for row in self.dbs[table as usize].iter(&r).map_err(e)? {
                let (k, v) = row.map_err(e)?;
                rows += 1;
                bytes += (k.len() + v.len()) as u64;
            }
            Ok((rows, bytes))
        })
    }

    fn scan_cb(&self, table: u8, cb: &mut dyn FnMut(&[u8], &[u8]) -> bool) -> Result<u64, String> {
        rtxn!(self, r, {
            let mut rows = 0u64;
            for row in self.dbs[table as usize].iter(&r).map_err(e)? {
                let (k, v) = row.map_err(e)?;
                rows += 1;
                if !cb(k, v) {
                    break;
                }
            }
            Ok(rows)
        })
    }

    /// Opens `n` read transactions at once and holds them, so the caller can
    /// find the reader-slot ceiling (`MDB_READERS_FULL`) instead of guessing.
    fn hold_readers(&self, n: usize) -> Result<(usize, String), String> {
        match &self.env {
            HEnv::Tls(en) => {
                // With thread-local slots a SECOND read txn on this thread is
                // already illegal, so the probe finds the per-thread limit.
                let mut held = Vec::with_capacity(n);
                for i in 0..n {
                    match en.read_txn() {
                        Ok(t) => held.push(t),
                        Err(err) => return Ok((i, format!("{err}"))),
                    }
                }
                Ok((n, String::new()))
            }
            HEnv::NoTls(en) => {
                let mut held = Vec::with_capacity(n);
                for i in 0..n {
                    match en.read_txn() {
                        Ok(t) => held.push(t),
                        Err(err) => return Ok((i, format!("{err}"))),
                    }
                }
                Ok((n, String::new()))
            }
        }
    }

    fn export(&self, dest: &Path) -> Result<Vec<ExportPart>, String> {
        // LMDB's own consistent copy: no writer pause, one pass over the
        // b-tree, and the result is a usable environment (§11.6 step 3).
        let t0 = std::time::Instant::now();
        let mut f = std::fs::File::create(dest).map_err(e)?;
        self.env.copy_to_file(&mut f, CompactionOption::Disabled)?;
        f.sync_all().map_err(e)?;
        let bytes = f.metadata().map_err(e)?.len();
        let a = ExportPart {
            method: "mdb_env_copy (consistent file copy)",
            bytes,
            ms: t0.elapsed().as_secs_f64() * 1e3,
        };
        let t1 = std::time::Instant::now();
        let mut f2 = std::fs::File::create(dest.with_extension("compacted")).map_err(e)?;
        self.env.copy_to_file(&mut f2, CompactionOption::Enabled)?;
        f2.sync_all().map_err(e)?;
        let bytes2 = f2.metadata().map_err(e)?.len();
        let b = ExportPart {
            method: "mdb_env_copy compacting",
            bytes: bytes2,
            ms: t1.elapsed().as_secs_f64() * 1e3,
        };
        Ok(vec![a, b])
    }

    fn maintain(&mut self) -> Result<String, String> {
        // LMDB never shrinks in place: freed pages go on the free list and are
        // reused. The only way to give space back is a compacting copy.
        let t0 = std::time::Instant::now();
        let tmp = self.dir.join("compact.tmp");
        let mut f = std::fs::File::create(&tmp).map_err(e)?;
        self.env.copy_to_file(&mut f, CompactionOption::Enabled)?;
        let size = f.metadata().map_err(e)?.len();
        drop(f);
        std::fs::remove_file(&tmp).map_err(e)?;
        Ok(format!(
            "no in-place compaction; a compacting copy would be {} MiB, made in {:.0} ms (freed pages are reused in place instead)",
            size / (1 << 20),
            t0.elapsed().as_secs_f64() * 1e3
        ))
    }

    fn caps(&self) -> Caps {
        Caps {
            incremental_checkpoint: "no: mdb_env_copy is always O(state)",
            export_method: "mdb_env_copy, with or without compaction",
            writer_pause: "none (the copy runs in a read transaction; it does hold the free list, so the file grows while it runs)",
            crash_model: if self.flags_note == "MDB_NOSYNC" {
                "MDB_NOSYNC: a committed txn is in the mmap and the OS keeps it after kill -9, so the store reopens PAST the last durable commit (VM: 12/15 kill runs, up to 2456 segments beyond the recorded file lengths); after dropped writes it came back at the durable point in 9 of 10 runs and 1 of 10 would not reopen at all"
            } else {
                "MDB_NOMETASYNC: every commit flushes its own data pages and defers only the meta page, so lmdb.h's guarantee is unconditional integrity with at most the last committed transaction undone"
            },
        }
    }

    fn reported_disk(&self) -> u64 {
        std::fs::metadata(self.dir.join("data.mdb"))
            .map(|m| m.len())
            .unwrap_or(0)
    }
}
