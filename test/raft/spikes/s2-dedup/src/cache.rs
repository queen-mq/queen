//! The bounded recent dedup cache of D10 / §6.3.
//!
//! Shape borrowed from the broker's own cache (server/src/dedup.rs): a
//! temporal ring of immutable SORTED blocks of 16-byte hashes, each fronted by
//! a blocked bloom, plus one arrival-order `hot` block. No per-hash map, so the
//! footprint is 16 B/hash plus the bloom, and the largest allocation is one
//! block.
//!
//! Two differences from the broker's, both deliberate:
//!
//!   * it is GLOBAL, not per partition. A negative answer for a hash is a
//!     negative for every partition (the resident set is a superset of any
//!     partition's), which is the only direction that has to be sound; a
//!     positive is a hint that falls through to the exact path, which is where
//!     the partition and the original offset come from anyway.
//!   * it holds no offsets. A duplicate verdict must report the ORIGINAL
//!     offset (003), which only the exact path knows, exactly as the broker's
//!     `LocalDuplicate` routes the segment to SQL with `p_verified = -1`.
//!
//! Validity rule: the cache may answer "absent, authoritative" only for a
//! window whose start is at or after `covered_from_us`, which starts at the
//! moment the cache opened and rises every time a block is dropped.

use crate::bloom::Bloom;
use std::collections::VecDeque;

struct Block {
    hashes: Box<[u128]>,
    bloom: Bloom,
    max_created: i64,
}

pub struct RecentCache {
    blocks: VecDeque<Block>,
    hot: Vec<u128>,
    hot_bloom: Bloom,
    hot_max_created: i64,
    block_cap: usize,
    bits_per_key: u32,
    budget_bytes: usize,
    covered_from_us: i64,
    pub bytes: usize,
    pub probes: u64,
    pub maybe: u64,
    pub vouched: u64,
    pub blocks_dropped: u64,
}

impl RecentCache {
    pub fn new(budget_bytes: usize, block_cap: usize, bits_per_key: u32, now_us: i64) -> Self {
        Self {
            blocks: VecDeque::new(),
            hot: Vec::with_capacity(block_cap),
            hot_bloom: Bloom::new(block_cap as u64, bits_per_key),
            hot_max_created: 0,
            block_cap,
            bits_per_key,
            budget_bytes,
            covered_from_us: now_us,
            bytes: 0,
            probes: 0,
            maybe: 0,
            vouched: 0,
            blocks_dropped: 0,
        }
    }

    pub fn enabled(&self) -> bool {
        self.budget_bytes > 0
    }

    /// The oldest `created_at` the cache can vouch for.
    pub fn covered_from_us(&self) -> i64 {
        self.covered_from_us
    }

    pub fn ram_bytes(&self) -> usize {
        self.bytes + self.hot.capacity() * 16 + self.hot_bloom.bytes()
    }

    pub fn insert_run(&mut self, hashes: &[u128], created_at: i64) {
        if !self.enabled() {
            return;
        }
        for h in hashes {
            self.hot.push(*h);
            self.hot_bloom.insert(*h);
        }
        self.hot_max_created = self.hot_max_created.max(created_at);
        if self.hot.len() >= self.block_cap {
            self.seal();
        }
        self.enforce_budget();
    }

    fn seal(&mut self) {
        if self.hot.is_empty() {
            return;
        }
        let mut v = std::mem::replace(&mut self.hot, Vec::with_capacity(self.block_cap));
        v.sort_unstable();
        let mut bloom = Bloom::new(v.len() as u64, self.bits_per_key);
        for h in &v {
            bloom.insert(*h);
        }
        self.bytes += v.len() * 16 + bloom.bytes();
        self.blocks.push_back(Block {
            hashes: v.into_boxed_slice(),
            bloom,
            max_created: self.hot_max_created,
        });
        self.hot_bloom = Bloom::new(self.block_cap as u64, self.bits_per_key);
        self.hot_max_created = 0;
    }

    fn enforce_budget(&mut self) {
        while self.bytes > self.budget_bytes {
            let Some(b) = self.blocks.pop_front() else {
                break;
            };
            self.bytes -= b.hashes.len() * 16 + b.bloom.bytes();
            // A dropped block is entirely older than the blocks behind it, so
            // the cache can no longer vouch at or below its newest stamp.
            self.covered_from_us = self.covered_from_us.max(b.max_created);
            self.blocks_dropped += 1;
        }
    }

    /// Drops blocks whose whole span is older than `cutoff_us` (never splits a
    /// block — the broker's invariant 2).
    pub fn expire(&mut self, cutoff_us: i64) {
        while let Some(b) = self.blocks.front() {
            if b.max_created < cutoff_us {
                let b = self.blocks.pop_front().unwrap();
                self.bytes -= b.hashes.len() * 16 + b.bloom.bytes();
                self.covered_from_us = self.covered_from_us.max(b.max_created);
                self.blocks_dropped += 1;
            } else {
                break;
            }
        }
    }

    /// True when the cache cannot rule the hash out. `false` is authoritative
    /// only for the span the cache covers (`covered_from_us`).
    pub fn maybe_contains(&mut self, h: u128) -> bool {
        self.probes += 1;
        if !self.enabled() {
            self.maybe += 1;
            return true;
        }
        if self.hot_bloom.contains(h) && self.hot.contains(&h) {
            self.maybe += 1;
            return true;
        }
        for b in self.blocks.iter().rev() {
            if b.bloom.contains(h) && b.hashes.binary_search(&h).is_ok() {
                self.maybe += 1;
                return true;
            }
        }
        self.vouched += 1;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vouches_for_absent_hashes_and_never_for_present_ones() {
        let mut c = RecentCache::new(1 << 20, 1024, 16, 0);
        let hs: Vec<u128> = (0..5000u128).map(|i| i * 0x9e37 + 1).collect();
        for chunk in hs.chunks(10) {
            c.insert_run(chunk, 1000);
        }
        for h in &hs {
            assert!(c.maybe_contains(*h), "missing {h}");
        }
        let mut absent_vouched = 0;
        for i in 0..5000u128 {
            if !c.maybe_contains(i * 0x9e37 + 2) {
                absent_vouched += 1;
            }
        }
        assert!(
            absent_vouched > 4900,
            "only {absent_vouched} absent hashes ruled out"
        );
    }

    #[test]
    fn dropping_blocks_raises_the_floor() {
        let mut c = RecentCache::new(10_000, 256, 16, 0);
        for i in 0..4000u128 {
            c.insert_run(&[i], 1_000 + i as i64);
        }
        assert!(c.blocks_dropped > 0);
        assert!(c.covered_from_us() > 0);
    }
}
