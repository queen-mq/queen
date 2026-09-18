//! redb 2.6: single-file copy-on-write B-tree, pure Rust.
//!
//! Non-durable commit = `Durability::None` (visible, not fsynced; redb only
//! frees pages at a higher durability level, so file growth between durable
//! points is part of what we measure). Durable point = `Durability::Immediate`.

use super::{Caps, Engine, ExportPart, OpBuf, NTABLES, TABLES};
use redb::{Database, Durability, ReadableTable, TableDefinition};
use std::io::Write;
use std::path::{Path, PathBuf};

type Def = TableDefinition<'static, &'static [u8], &'static [u8]>;

const DEFS: [Def; NTABLES] = [
    TableDefinition::new(TABLES[0]),
    TableDefinition::new(TABLES[1]),
    TableDefinition::new(TABLES[2]),
    TableDefinition::new(TABLES[3]),
    TableDefinition::new(TABLES[4]),
    TableDefinition::new(TABLES[5]),
    TableDefinition::new(TABLES[6]),
    TableDefinition::new(TABLES[7]),
    TableDefinition::new(TABLES[8]),
];

pub struct Redb {
    db: Database,
    path: PathBuf,
}

fn e<E: std::fmt::Display>(x: E) -> String {
    format!("redb: {x}")
}

impl Redb {
    pub fn open(dir: &Path, cache_bytes: u64) -> Result<Self, String> {
        std::fs::create_dir_all(dir).map_err(e)?;
        let path = dir.join("store.redb");
        let mut b = Database::builder();
        b.set_cache_size(cache_bytes as usize);
        let db = b.create(&path).map_err(e)?;
        // Make sure every table exists, so reads on a fresh store do not fail.
        let mut w = db.begin_write().map_err(e)?;
        w.set_durability(Durability::Immediate);
        for d in DEFS.iter() {
            w.open_table(*d).map_err(e)?;
        }
        w.commit().map_err(e)?;
        Ok(Self { db, path })
    }
}

impl Engine for Redb {
    fn name(&self) -> &'static str {
        "redb"
    }

    fn commit(&mut self, ops: &OpBuf, durable: bool) -> Result<(), String> {
        let mut w = self.db.begin_write().map_err(e)?;
        w.set_durability(if durable {
            Durability::Immediate
        } else {
            Durability::None
        });
        {
            // One open table per table id that this entry touches.
            for t in 0..NTABLES {
                if !ops.recs().iter().any(|o| o.table as usize == t) {
                    continue;
                }
                let mut tbl = w.open_table(DEFS[t]).map_err(e)?;
                for o in ops.recs().iter().filter(|o| o.table as usize == t) {
                    if o.del {
                        tbl.remove(ops.key(o)).map_err(e)?;
                    } else {
                        tbl.insert(ops.key(o), ops.val(o)).map_err(e)?;
                    }
                }
            }
        }
        w.commit().map_err(e)
    }

    fn get(&self, table: u8, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let r = self.db.begin_read().map_err(e)?;
        let tbl = r.open_table(DEFS[table as usize]).map_err(e)?;
        Ok(tbl.get(key).map_err(e)?.map(|g| g.value().to_vec()))
    }

    fn prefix_count(&self, table: u8, prefix: &[u8], limit: usize) -> Result<usize, String> {
        let r = self.db.begin_read().map_err(e)?;
        let tbl = r.open_table(DEFS[table as usize]).map_err(e)?;
        let mut n = 0usize;
        for row in tbl.range(prefix..).map_err(e)? {
            let (k, _v) = row.map_err(e)?;
            if !k.value().starts_with(prefix) {
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
        let r = self.db.begin_read().map_err(e)?;
        let tbl = r.open_table(DEFS[table as usize]).map_err(e)?;
        let mut n = 0u64;
        for row in tbl.range(from..).map_err(e)? {
            let (k, v) = row.map_err(e)?;
            if !k.value().starts_with(prefix) {
                break;
            }
            n += 1;
            if !cb(k.value(), v.value()) || n as usize >= limit {
                break;
            }
        }
        Ok(n)
    }

    fn scan(&self, table: u8) -> Result<(u64, u64), String> {
        let r = self.db.begin_read().map_err(e)?;
        let tbl = r.open_table(DEFS[table as usize]).map_err(e)?;
        let mut rows = 0u64;
        let mut bytes = 0u64;
        for row in tbl.iter().map_err(e)? {
            let (k, v) = row.map_err(e)?;
            rows += 1;
            bytes += (k.value().len() + v.value().len()) as u64;
        }
        Ok((rows, bytes))
    }

    fn scan_cb(&self, table: u8, cb: &mut dyn FnMut(&[u8], &[u8]) -> bool) -> Result<u64, String> {
        let r = self.db.begin_read().map_err(e)?;
        let tbl = r.open_table(DEFS[table as usize]).map_err(e)?;
        let mut rows = 0u64;
        for row in tbl.iter().map_err(e)? {
            let (k, v) = row.map_err(e)?;
            rows += 1;
            if !cb(k.value(), v.value()) {
                break;
            }
        }
        Ok(rows)
    }

    fn export(&self, dest: &Path) -> Result<Vec<ExportPart>, String> {
        // A read transaction pins a consistent view; we write a logical dump
        // (§11.6 forbids holding the read txn open for the transfer).
        let t0 = std::time::Instant::now();
        let f = std::fs::File::create(dest).map_err(e)?;
        let mut w = std::io::BufWriter::with_capacity(1 << 20, f);
        let r = self.db.begin_read().map_err(e)?;
        let mut bytes = 0u64;
        for t in 0..NTABLES {
            let tbl = r.open_table(DEFS[t]).map_err(e)?;
            for row in tbl.iter().map_err(e)? {
                let (k, v) = row.map_err(e)?;
                let (k, v) = (k.value(), v.value());
                w.write_all(&[t as u8]).map_err(e)?;
                w.write_all(&(k.len() as u32).to_le_bytes()).map_err(e)?;
                w.write_all(&(v.len() as u32).to_le_bytes()).map_err(e)?;
                w.write_all(k).map_err(e)?;
                w.write_all(v).map_err(e)?;
                bytes += 9 + k.len() as u64 + v.len() as u64;
            }
        }
        let f = w.into_inner().map_err(|x| e(x.to_string()))?;
        f.sync_all().map_err(e)?;
        Ok(vec![ExportPart {
            method: "logical dump (read txn)",
            bytes,
            ms: t0.elapsed().as_secs_f64() * 1e3,
        }])
    }

    fn maintain(&mut self) -> Result<String, String> {
        let t0 = std::time::Instant::now();
        let changed = self.db.compact().map_err(e)?;
        Ok(format!(
            "Database::compact() -> {changed} in {:.0} ms",
            t0.elapsed().as_secs_f64() * 1e3
        ))
    }

    fn caps(&self) -> Caps {
        Caps {
            incremental_checkpoint:
                "no file-level incremental snapshot; persistent savepoints keep an older version alive inside the same file",
            export_method: "logical dump from a read transaction (O(state))",
            writer_pause: "none (MVCC read txn), but held pages are not freed while it lives",
            crash_model:
                "Durability::None does not publish a crash-visible root, so after kill -9 the store reopens at the LAST DURABLE commit and everything applied after it is gone (measured on macOS: 10/10 kill runs, applied == durable)",
        }
    }

    fn reported_disk(&self) -> u64 {
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }
}
