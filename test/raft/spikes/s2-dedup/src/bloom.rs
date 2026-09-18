//! Blocked bloom filter, one filter per segment file (D10 option (b)).
//!
//! Same shape as the broker's own dedup front (server/src/dedup.rs invariant
//! 4): all k probe bits of a hash land inside ONE 64-byte block, so a
//! definitive "absent" — the answer for 99 % of pushed messages — costs one
//! cache line. `bits_per_key` is a knob because the probe tests EVERY file of
//! the partition's bucket that overlaps the window: with F files tested per
//! hash, the work a false positive causes is F × p × (store range scan + frame
//! read), so p has to be small enough that F × p ≪ 1.

const BLOCK_BITS: u64 = 512;
const BLOCK_BYTES: usize = 64;

pub struct Bloom {
    bits: Vec<u64>, // 8 u64 per block
    blocks: u64,
    k: u32,
    pub inserted: u64,
}

#[inline]
fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

impl Bloom {
    /// `keys` is the expected number of hashes, `bits_per_key` the budget.
    pub fn new(keys: u64, bits_per_key: u32) -> Self {
        let bits = (keys.max(1) * bits_per_key as u64).max(BLOCK_BITS);
        let blocks = bits.div_ceil(BLOCK_BITS);
        // k = ln2 * m/n, clamped: more than 12 probes buys little and costs time.
        let k = (((bits_per_key as f64) * std::f64::consts::LN_2).round() as u32).clamp(4, 12);
        Self {
            bits: vec![0u64; (blocks * 8) as usize],
            blocks,
            k,
            inserted: 0,
        }
    }

    pub fn bytes(&self) -> usize {
        self.bits.len() * 8
    }

    #[inline]
    fn block_of(&self, h: u128) -> usize {
        ((h >> 64) as u64 % self.blocks) as usize * 8
    }

    #[inline]
    pub fn insert(&mut self, h: u128) {
        let b = self.block_of(h);
        let mut x = mix(h as u64);
        for _ in 0..self.k {
            let bit = (x % BLOCK_BITS) as usize;
            self.bits[b + bit / 64] |= 1u64 << (bit % 64);
            x = mix(x);
        }
        self.inserted += 1;
    }

    #[inline]
    pub fn contains(&self, h: u128) -> bool {
        let b = self.block_of(h);
        let mut x = mix(h as u64);
        for _ in 0..self.k {
            let bit = (x % BLOCK_BITS) as usize;
            if self.bits[b + bit / 64] & (1u64 << (bit % 64)) == 0 {
                return false;
            }
            x = mix(x);
        }
        true
    }

    /// Node-local persistence: a sealed file's bloom is written next to it so a
    /// restart does not have to re-read every hash list (§11.5 rebuild).
    pub fn save(&self, path: &std::path::Path) -> std::io::Result<()> {
        let mut v = Vec::with_capacity(self.bytes() + 24);
        v.extend_from_slice(&self.blocks.to_le_bytes());
        v.extend_from_slice(&(self.k as u64).to_le_bytes());
        v.extend_from_slice(&self.inserted.to_le_bytes());
        for w in &self.bits {
            v.extend_from_slice(&w.to_le_bytes());
        }
        std::fs::write(path, &v)
    }

    pub fn load(path: &std::path::Path) -> std::io::Result<Self> {
        let v = std::fs::read(path)?;
        if v.len() < 24 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "short bloom",
            ));
        }
        let blocks = u64::from_le_bytes(v[0..8].try_into().unwrap());
        let k = u64::from_le_bytes(v[8..16].try_into().unwrap()) as u32;
        let inserted = u64::from_le_bytes(v[16..24].try_into().unwrap());
        let mut bits = Vec::with_capacity((blocks * 8) as usize);
        for c in v[24..].chunks_exact(8) {
            bits.push(u64::from_le_bytes(c.try_into().unwrap()));
        }
        if bits.len() != (blocks * 8) as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bloom size",
            ));
        }
        Ok(Self {
            bits,
            blocks,
            k,
            inserted,
        })
    }
}

#[allow(dead_code)]
pub const BLOCK_BYTES_PUB: usize = BLOCK_BYTES;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives_and_a_sane_false_positive_rate() {
        let n = 50_000u64;
        let mut b = Bloom::new(n, 16);
        for i in 0..n as u128 {
            b.insert(i.wrapping_mul(0x9e37_79b9_7f4a_7c15).wrapping_add(7));
        }
        for i in 0..n as u128 {
            assert!(b.contains(i.wrapping_mul(0x9e37_79b9_7f4a_7c15).wrapping_add(7)));
        }
        let mut fp = 0u64;
        let probes = 200_000u128;
        for i in 0..probes {
            let h = (i + 1) << 70 | 0x1234_5678;
            if b.contains(h) {
                fp += 1;
            }
        }
        let rate = fp as f64 / probes as f64;
        assert!(rate < 0.02, "fp rate {rate}");
    }

    #[test]
    fn round_trips_through_a_file() {
        let mut b = Bloom::new(1000, 16);
        for i in 0..1000u128 {
            b.insert(i * 3 + 1);
        }
        let p = std::env::temp_dir().join(format!("s2-bloom-{}.bin", std::process::id()));
        b.save(&p).unwrap();
        let c = Bloom::load(&p).unwrap();
        for i in 0..1000u128 {
            assert!(c.contains(i * 3 + 1));
        }
        assert_eq!(c.inserted, 1000);
        let _ = std::fs::remove_file(&p);
    }
}
