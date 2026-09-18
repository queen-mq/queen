//! heed 0.22 over lmdb-master-sys 0.2 (LMDB 0.9.x built with `cc`, no cmake).
//!
//! Non-durable commit = a normal write transaction with the environment
//! opened `MDB_NOSYNC`: the commit writes the dirty pages and the meta page
//! through the mmap, without fsync. Durable point = `Env::force_sync()`
//! (mdb_env_sync with force).

use super::{Caps, Engine, ExportPart, OpBuf, NTABLES, TABLES};
use heed::types::Bytes;
use heed::{CompactionOption, Database, Env, EnvFlags, EnvOpenOptions};
use std::ops::Bound;
use std::path::{Path, PathBuf};

pub struct Heed {
    env: Env,
    dbs: Vec<Database<Bytes, Bytes>>,
    dir: PathBuf,
}

fn e<E: std::fmt::Display>(x: E) -> String {
    format!("heed: {x}")
}

/// LMDB rejects a zero-length key: positioning a cursor with `MDB_SET_RANGE`
/// on `b""` answers `MDB_BAD_VALSIZE` (mdb.c checks `key->mv_size == 0 ||
/// key->mv_size > maxkeysize`), and the error surfaces before the first row.
/// The trait's `from`/`prefix` use the empty slice for "from the beginning",
/// which is `Bound::Unbounded` here. (Found by WP-0.4 part 2: option (a)'s
/// prune walks its expiry index from `b""` on its first step, so every
/// `--option a --engine heed` run died at once; fjall and redb accept it.)
fn lower(k: &[u8]) -> Bound<&[u8]> {
    if k.is_empty() {
        Bound::Unbounded
    } else {
        Bound::Included(k)
    }
}

impl Heed {
    pub fn open(dir: &Path, map_size: usize) -> Result<Self, String> {
        let sdir = dir.join("store");
        std::fs::create_dir_all(&sdir).map_err(e)?;
        let mut o = EnvOpenOptions::new();
        o.map_size(map_size);
        o.max_dbs(NTABLES as u32 + 2);
        // SAFETY: NO_SYNC is what the durable-point protocol (§11.4) needs:
        // the harness decides when bytes reach the platter.
        unsafe { o.flags(EnvFlags::NO_SYNC) };
        let env = unsafe { o.open(&sdir) }.map_err(e)?;
        let mut wtxn = env.write_txn().map_err(e)?;
        let mut dbs = Vec::with_capacity(NTABLES);
        for name in TABLES.iter() {
            dbs.push(
                env.create_database::<Bytes, Bytes>(&mut wtxn, Some(name))
                    .map_err(e)?,
            );
        }
        wtxn.commit().map_err(e)?;
        env.force_sync().map_err(e)?;
        Ok(Self {
            env,
            dbs,
            dir: sdir,
        })
    }
}

impl Engine for Heed {
    fn name(&self) -> &'static str {
        "heed"
    }

    fn commit(&mut self, ops: &OpBuf, durable: bool) -> Result<(), String> {
        let mut w = self.env.write_txn().map_err(e)?;
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
            self.env.force_sync().map_err(e)?;
        }
        Ok(())
    }

    fn get(&self, table: u8, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let r = self.env.read_txn().map_err(e)?;
        Ok(self.dbs[table as usize]
            .get(&r, key)
            .map_err(e)?
            .map(|v| v.to_vec()))
    }

    fn prefix_count(&self, table: u8, prefix: &[u8], limit: usize) -> Result<usize, String> {
        let r = self.env.read_txn().map_err(e)?;
        let range = (lower(prefix), Bound::Unbounded);
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
    }

    fn range(
        &self,
        table: u8,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
        cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<u64, String> {
        let r = self.env.read_txn().map_err(e)?;
        let range = (lower(from), Bound::Unbounded);
        let mut n = 0u64;
        for row in self.dbs[table as usize].range(&r, &range).map_err(e)? {
            let (k, v) = row.map_err(e)?;
            if !k.starts_with(prefix) {
                break;
            }
            n += 1;
            if !cb(k, v) || n as usize >= limit {
                break;
            }
        }
        Ok(n)
    }

    fn scan(&self, table: u8) -> Result<(u64, u64), String> {
        let r = self.env.read_txn().map_err(e)?;
        let mut rows = 0u64;
        let mut bytes = 0u64;
        for row in self.dbs[table as usize].iter(&r).map_err(e)? {
            let (k, v) = row.map_err(e)?;
            rows += 1;
            bytes += (k.len() + v.len()) as u64;
        }
        Ok((rows, bytes))
    }

    fn scan_cb(&self, table: u8, cb: &mut dyn FnMut(&[u8], &[u8]) -> bool) -> Result<u64, String> {
        let r = self.env.read_txn().map_err(e)?;
        let mut rows = 0u64;
        for row in self.dbs[table as usize].iter(&r).map_err(e)? {
            let (k, v) = row.map_err(e)?;
            rows += 1;
            if !cb(k, v) {
                break;
            }
        }
        Ok(rows)
    }

    fn export(&self, dest: &Path) -> Result<Vec<ExportPart>, String> {
        // LMDB's own consistent copy: no writer pause, one pass over the
        // b-tree, and the result is a usable environment (§11.6 step 3).
        let t0 = std::time::Instant::now();
        let mut f = std::fs::File::create(dest).map_err(e)?;
        self.env
            .copy_to_file(&mut f, CompactionOption::Disabled)
            .map_err(e)?;
        f.sync_all().map_err(e)?;
        let bytes = f.metadata().map_err(e)?.len();
        let a = ExportPart {
            method: "mdb_env_copy (consistent file copy)",
            bytes,
            ms: t0.elapsed().as_secs_f64() * 1e3,
        };
        let t1 = std::time::Instant::now();
        let mut f2 = std::fs::File::create(dest.with_extension("compacted")).map_err(e)?;
        self.env
            .copy_to_file(&mut f2, CompactionOption::Enabled)
            .map_err(e)?;
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
        self.env
            .copy_to_file(&mut f, CompactionOption::Enabled)
            .map_err(e)?;
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
            crash_model:
                "with MDB_NOSYNC a committed txn is in the mmap and the OS keeps it after kill -9, so the store reopens PAST the last durable commit (measured on macOS: 6/10 kill runs, up to 251 segments referenced beyond the durable file lengths); after a power loss it returns to the last meta page that reached the platter",
        }
    }

    fn reported_disk(&self) -> u64 {
        std::fs::metadata(self.dir.join("data.mdb"))
            .map(|m| m.len())
            .unwrap_or(0)
    }
}
