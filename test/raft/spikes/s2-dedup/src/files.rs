//! Node-local file bookkeeping (§6.2 `files`, §11.7).
//!
//! One row per segment file: its size, how many `segments` rows still point
//! into it (what retention releases), how many `txns` rows still point into it
//! (the hash lists, which outlive retention by D10), its created_at span and —
//! option (b) only — its bloom filter.
//!
//! A file is unlinkable when BOTH counters are zero (I10). A file whose
//! segment counter is zero while its txn counter is not is the case D10 calls
//! out: retention has deleted the messages, the hashes are still inside the
//! txns window. That file is where option (b) either pins the whole payload or
//! pays a hash-only rewrite (§11.7).

use crate::bloom::Bloom;
use std::collections::VecDeque;

pub struct FileInfo {
    pub id: u32,
    pub bytes: u64,
    pub live_segments: u32,
    pub live_txns: u32,
    pub min_created: i64,
    pub max_created: i64,
    pub sealed: bool,
    pub hash_only: bool,
    pub bloom: Option<Bloom>,
}

pub struct FileTable {
    pub buckets: Vec<VecDeque<FileInfo>>,
    bloom_keys: u64,
    bloom_bits: u32,
    use_blooms: bool,
    pub bloom_bytes: usize,
    pub files_opened: u64,
    pub files_unlinked: u64,
    pub bytes_unlinked: u64,
}

impl FileTable {
    pub fn new(nbuckets: usize, bloom_keys: u64, bloom_bits: u32, use_blooms: bool) -> Self {
        let mut buckets = Vec::with_capacity(nbuckets);
        for _ in 0..nbuckets {
            buckets.push(VecDeque::new());
        }
        Self {
            buckets,
            bloom_keys,
            bloom_bits,
            use_blooms,
            bloom_bytes: 0,
            files_opened: 0,
            files_unlinked: 0,
            bytes_unlinked: 0,
        }
    }

    pub fn get_mut(&mut self, bucket: u16, id: u32) -> Option<&mut FileInfo> {
        self.buckets[bucket as usize]
            .iter_mut()
            .find(|f| f.id == id)
    }

    pub fn get(&self, bucket: u16, id: u32) -> Option<&FileInfo> {
        self.buckets[bucket as usize].iter().find(|f| f.id == id)
    }

    /// Records an append into (bucket, file_id). Creates the file row on first
    /// sight and inserts the hashes into its bloom.
    pub fn on_append(
        &mut self,
        bucket: u16,
        id: u32,
        created_at: i64,
        frame_bytes: u64,
        hashes: &[u128],
        sealed_id: Option<u32>,
    ) {
        if let Some(sid) = sealed_id {
            if let Some(f) = self.get_mut(bucket, sid) {
                f.sealed = true;
            }
        }
        let use_blooms = self.use_blooms;
        let (keys, bits) = (self.bloom_keys, self.bloom_bits);
        let q = &mut self.buckets[bucket as usize];
        let idx = q.iter().position(|f| f.id == id);
        let i = match idx {
            Some(i) => i,
            None => {
                let bloom = if use_blooms {
                    Some(Bloom::new(keys, bits))
                } else {
                    None
                };
                if let Some(b) = &bloom {
                    self.bloom_bytes += b.bytes();
                }
                q.push_back(FileInfo {
                    id,
                    bytes: 0,
                    live_segments: 0,
                    live_txns: 0,
                    min_created: created_at,
                    max_created: created_at,
                    sealed: false,
                    hash_only: false,
                    bloom,
                });
                self.files_opened += 1;
                q.len() - 1
            }
        };
        let f = &mut q[i];
        f.bytes += frame_bytes;
        f.live_segments += 1;
        // Only option (b) keeps a hash reference into the file: its frames
        // carry the hash lists and a `txns` row points at them, so the file
        // outlives retention. Option (a) has no such reference — its frames
        // hold payload only — so its files die with their `segments` rows.
        if use_blooms {
            f.live_txns += 1;
        }
        f.min_created = f.min_created.min(created_at);
        f.max_created = f.max_created.max(created_at);
        if let Some(b) = &mut f.bloom {
            for h in hashes {
                b.insert(*h);
            }
        }
    }

    pub fn release_segment(&mut self, bucket: u16, id: u32) {
        if let Some(f) = self.get_mut(bucket, id) {
            f.live_segments = f.live_segments.saturating_sub(1);
        }
    }

    pub fn release_txn(&mut self, bucket: u16, id: u32) {
        if let Some(f) = self.get_mut(bucket, id) {
            f.live_txns = f.live_txns.saturating_sub(1);
        }
    }

    /// Sealed files with nothing left pointing at them (I10).
    pub fn collect_dead(&mut self, out: &mut Vec<(u16, u32)>) {
        out.clear();
        for (b, q) in self.buckets.iter().enumerate() {
            for f in q.iter() {
                if f.sealed && f.live_segments == 0 && f.live_txns == 0 {
                    out.push((b as u16, f.id));
                }
            }
        }
    }

    /// Sealed files whose payloads retention has deleted but whose hash lists
    /// are still needed: the hash-only rewrite candidates (§11.7, D10).
    pub fn collect_hash_only(&self, out: &mut Vec<(u16, u32, u64)>) {
        out.clear();
        for (b, q) in self.buckets.iter().enumerate() {
            for f in q.iter() {
                if f.sealed && !f.hash_only && f.live_segments == 0 && f.live_txns > 0 {
                    out.push((b as u16, f.id, f.bytes));
                }
            }
        }
    }

    pub fn remove(&mut self, bucket: u16, id: u32) -> Option<FileInfo> {
        let q = &mut self.buckets[bucket as usize];
        let i = q.iter().position(|f| f.id == id)?;
        let f = q.remove(i);
        if let Some(fi) = &f {
            if let Some(b) = &fi.bloom {
                self.bloom_bytes -= b.bytes();
            }
        }
        f
    }

    /// Files of one bucket whose created_at span overlaps [lo, hi].
    pub fn overlapping(&self, bucket: u16, lo: i64, hi: i64, out: &mut Vec<u32>) {
        out.clear();
        for f in self.buckets[bucket as usize].iter() {
            if f.max_created >= lo && f.min_created <= hi && f.bloom.is_some() {
                out.push(f.id);
            }
        }
    }

    /// Installs a file row rebuilt after a restart (§11.5 step 4).
    #[allow(clippy::too_many_arguments)]
    pub fn install(
        &mut self,
        bucket: u16,
        id: u32,
        live_segments: u32,
        live_txns: u32,
        min_created: i64,
        max_created: i64,
        bloom: Option<Bloom>,
    ) {
        if let Some(b) = &bloom {
            self.bloom_bytes += b.bytes();
        }
        self.buckets[bucket as usize].push_back(FileInfo {
            id,
            bytes: 0,
            live_segments,
            live_txns,
            min_created,
            max_created,
            sealed: true,
            hash_only: false,
            bloom,
        });
        self.files_opened += 1;
    }

    pub fn live_files(&self) -> usize {
        self.buckets.iter().map(|q| q.len()).sum()
    }

    pub fn ram_bytes(&self) -> usize {
        self.bloom_bytes + self.live_files() * std::mem::size_of::<FileInfo>()
    }
}
