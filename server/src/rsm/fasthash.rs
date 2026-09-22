//! A fast, non-keyed hasher for the planner's and the store's hot per-cycle
//! maps: an Fx-style word fold with a murmur3 `fmix64` finish. `std`'s
//! SipHash-1-3 showed up at ~2.3% of the broker's CPU at 150k msg/s, mostly
//! hashing keys that are already xxh3-128 digests. NOT DoS-resistant: use it
//! only for maps whose size is bounded by the work in flight.
//!
//! The finish is load-bearing. A plain `(h + w) * K` fold only carries bits
//! UPWARD, and the store's keys end in big-endian integers, whose varying byte
//! lands in the top bits of the last word: 20,000 such keys hit 64 buckets of
//! 65,536 and the store's dirty set went quadratic (measured: p99 1.1 s at
//! 120k msg/s). `fmix64` brings every input bit down to the bucket bits.

use std::hash::{BuildHasherDefault, Hasher};

const K: u64 = 0xf135_7aea_2e62_a9c5;

#[derive(Clone, Copy, Default)]
pub struct FxHasher {
    h: u64,
}

impl FxHasher {
    #[inline]
    fn add(&mut self, w: u64) {
        self.h = (self.h.rotate_left(5) ^ w).wrapping_mul(K);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            self.add(u64::from_le_bytes(c.try_into().expect("8 bytes")));
        }
        let rest = chunks.remainder();
        if !rest.is_empty() {
            let mut b = [0u8; 8];
            b[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(b) ^ ((rest.len() as u64) << 56));
        }
    }
    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.add(i as u64);
    }
    #[inline]
    fn write_u16(&mut self, i: u16) {
        self.add(i as u64);
    }
    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.add(i as u64);
    }
    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.add(i);
    }
    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.add(i as u64);
    }
    #[inline]
    fn finish(&self) -> u64 {
        let mut x = self.h;
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
        x ^= x >> 33;
        x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        x ^ (x >> 33)
    }
}

/// `BuildHasher` for [`FxHasher`]: `HashMap<K, V, FxBuild>`, built with
/// `HashMap::default()`.
pub type FxBuild = BuildHasherDefault<FxHasher>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::{BuildHasher, Hash};

    #[test]
    fn big_endian_suffixed_keys_spread_over_the_bucket_bits() {
        // The store's key shape: a prefix, then big-endian integers.
        let b = FxBuild::default();
        let mut buckets = std::collections::HashSet::new();
        for off in 0u64..20_000 {
            let mut k = b"CURSORS:".to_vec();
            k.extend_from_slice(&7u64.to_be_bytes());
            k.extend_from_slice(&off.to_be_bytes());
            let key: std::sync::Arc<[u8]> = std::sync::Arc::from(k);
            let mut h = b.build_hasher();
            key.hash(&mut h);
            buckets.insert(h.finish() & 0xFFFF);
        }
        // Random hashing puts 20,000 keys in ~17,600 of 65,536 buckets.
        assert!(buckets.len() > 15_000, "only {} buckets", buckets.len());
    }
}
