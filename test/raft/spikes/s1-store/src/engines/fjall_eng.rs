//! fjall 2.11: LSM keyspace with partitions (column families), pure Rust.
//!
//! Non-durable commit = a cross-partition `Batch` committed without a persist
//! mode: the journal write goes to the OS, the memtables take the rows.
//! Durable point = `Keyspace::persist(PersistMode::SyncAll)`.

use super::{Caps, Engine, ExportPart, OpBuf, NTABLES, TABLES};
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use std::io::Write;
use std::path::Path;

pub struct Fjall {
    ks: Keyspace,
    parts: Vec<PartitionHandle>,
}

fn e<E: std::fmt::Display>(x: E) -> String {
    format!("fjall: {x}")
}

impl Fjall {
    pub fn open(dir: &Path, cache_bytes: u64) -> Result<Self, String> {
        std::fs::create_dir_all(dir).map_err(e)?;
        let ks = Config::new(dir.join("store"))
            .cache_size(cache_bytes)
            // The harness owns the durable point, so the keyspace must never
            // fsync on a timer of its own.
            .fsync_ms(None)
            .manual_journal_persist(true)
            .open()
            .map_err(e)?;
        let mut parts = Vec::with_capacity(NTABLES);
        for name in TABLES.iter() {
            parts.push(
                ks.open_partition(name, PartitionCreateOptions::default())
                    .map_err(e)?,
            );
        }
        Ok(Self { ks, parts })
    }
}

impl Engine for Fjall {
    fn config_note(&self) -> String {
        "fjall 2.11.2, Batch commit, PersistMode::SyncAll at the durable point".into()
    }

    fn name(&self) -> &'static str {
        "fjall"
    }

    fn commit(&mut self, ops: &OpBuf, durable: bool) -> Result<(), String> {
        let mut b = self.ks.batch();
        for o in ops.recs() {
            let p = &self.parts[o.table as usize];
            if o.del {
                b.remove(p, ops.key(o));
            } else {
                b.insert(p, ops.key(o), ops.val(o));
            }
        }
        b.commit().map_err(e)?;
        if durable {
            self.ks.persist(PersistMode::SyncAll).map_err(e)?;
        }
        Ok(())
    }

    fn get(&self, table: u8, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        Ok(self.parts[table as usize]
            .get(key)
            .map_err(e)?
            .map(|v| v.to_vec()))
    }

    fn prefix_count(&self, table: u8, prefix: &[u8], limit: usize) -> Result<usize, String> {
        let mut n = 0usize;
        for row in self.parts[table as usize].prefix(prefix) {
            row.map_err(e)?;
            n += 1;
            if n >= limit {
                break;
            }
        }
        Ok(n)
    }

    fn scan(&self, table: u8) -> Result<(u64, u64), String> {
        let mut rows = 0u64;
        let mut bytes = 0u64;
        for row in self.parts[table as usize].iter() {
            let (k, v) = row.map_err(e)?;
            rows += 1;
            bytes += (k.len() + v.len()) as u64;
        }
        Ok((rows, bytes))
    }

    fn scan_cb(&self, table: u8, cb: &mut dyn FnMut(&[u8], &[u8]) -> bool) -> Result<u64, String> {
        let mut rows = 0u64;
        for row in self.parts[table as usize].iter() {
            let (k, v) = row.map_err(e)?;
            rows += 1;
            if !cb(&k, &v) {
                break;
            }
        }
        Ok(rows)
    }

    fn export(&self, dest: &Path) -> Result<Vec<ExportPart>, String> {
        let t0 = std::time::Instant::now();
        let f = std::fs::File::create(dest).map_err(e)?;
        let mut w = std::io::BufWriter::with_capacity(1 << 20, f);
        let mut bytes = 0u64;
        for (t, p) in self.parts.iter().enumerate() {
            // One instant per partition snapshot; a real export would take one
            // keyspace-wide instant (fjall::Keyspace::instant()).
            let snap = p.snapshot();
            for row in snap.iter() {
                let (k, v) = row.map_err(e)?;
                w.write_all(&[t as u8]).map_err(e)?;
                w.write_all(&(k.len() as u32).to_le_bytes()).map_err(e)?;
                w.write_all(&(v.len() as u32).to_le_bytes()).map_err(e)?;
                w.write_all(&k).map_err(e)?;
                w.write_all(&v).map_err(e)?;
                bytes += 9 + k.len() as u64 + v.len() as u64;
            }
        }
        let f = w.into_inner().map_err(|x| e(x.to_string()))?;
        f.sync_all().map_err(e)?;
        Ok(vec![ExportPart {
            method: "logical dump (per-partition snapshot)",
            bytes,
            ms: t0.elapsed().as_secs_f64() * 1e3,
        }])
    }

    fn maintain(&mut self) -> Result<String, String> {
        let t0 = std::time::Instant::now();
        for p in &self.parts {
            p.rotate_memtable().map_err(e)?;
        }
        for p in &self.parts {
            p.major_compact().map_err(e)?;
        }
        Ok(format!(
            "rotate_memtable + major_compact on {} partitions in {:.0} ms",
            self.parts.len(),
            t0.elapsed().as_secs_f64() * 1e3
        ))
    }

    fn caps(&self) -> Caps {
        Caps {
            incremental_checkpoint:
                "yes in principle: SSTs are immutable, so a snapshot can hard-link the sealed segments and ship only new ones (fjall has no built-in checkpoint API in 2.11)",
            export_method: "logical dump from per-partition snapshots (O(state)); no built-in backup call",
            writer_pause: "none (snapshots are MVCC seqno views)",
            crash_model:
                "batch.commit() writes the journal without fsync unless a PersistMode is given; after kill -9 the OS keeps those journal bytes, so recovery replays them and the store reopens PAST the last durable commit (measured on macOS: 6/10 kill runs, up to 368 segments referenced beyond the durable file lengths)",
        }
    }

    fn reported_disk(&self) -> u64 {
        self.ks.disk_space()
    }
}
