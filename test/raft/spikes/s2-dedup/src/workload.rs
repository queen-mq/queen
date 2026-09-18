//! The push / duplicate / ack-by-hash stream, and the oracle that says what
//! the answer must be.
//!
//! Duplicates are injected at 1 % of messages with an age drawn uniformly from
//! [0, dup_max_age] — deliberately wider than the dedup window, so the run
//! exercises both directions of exactness: every duplicate INSIDE the window
//! must be detected, and no duplicate OUTSIDE it may be reported (003 caps the
//! probe at `created_at >= now - window`).
//!
//! The oracle is a per-second ring of samples, `--samples-per-s` per second,
//! bounded by the widest age it has to serve — a few hundred KiB, so it does
//! not distort the RAM numbers it is there to check.

use xxhash_rust::xxh3::xxh3_128;

#[derive(Clone, Copy)]
pub struct Sample {
    pub h: u128,
    pub pid: u64,
    pub off: u64,
    pub base: u64,
    pub created: i64,
}

pub struct Oracle {
    secs: Vec<Vec<Sample>>,
    stamp: Vec<i64>,
    per_sec: usize,
    t0_us: i64,
    pub taken: u64,
    pub empty: u64,
}

impl Oracle {
    pub fn new(max_age_s: i64, per_sec: usize, t0_us: i64) -> Self {
        let n = (max_age_s + 2) as usize;
        Self {
            secs: (0..n).map(|_| Vec::with_capacity(per_sec)).collect(),
            stamp: vec![i64::MIN; n],
            per_sec,
            t0_us,
            taken: 0,
            empty: 0,
        }
    }

    fn slot(&self, sec: i64) -> usize {
        (sec.rem_euclid(self.secs.len() as i64)) as usize
    }

    pub fn offer(&mut self, s: Sample) {
        let sec = (s.created - self.t0_us) / 1_000_000;
        let i = self.slot(sec);
        if self.stamp[i] != sec {
            self.stamp[i] = sec;
            self.secs[i].clear();
        }
        if self.secs[i].len() < self.per_sec {
            self.secs[i].push(s);
        }
    }

    /// Takes (and removes) a sample created `age_s` ago, so no hash is ever
    /// injected twice and every injected duplicate has exactly one prior
    /// occurrence.
    pub fn take(&mut self, now_us: i64, age_s: i64) -> Option<Sample> {
        let sec = (now_us - self.t0_us) / 1_000_000 - age_s;
        if sec < 0 {
            self.empty += 1;
            return None;
        }
        let i = self.slot(sec);
        if self.stamp[i] != sec {
            self.empty += 1;
            return None;
        }
        let s = self.secs[i].pop();
        if s.is_some() {
            self.taken += 1;
        } else {
            self.empty += 1;
        }
        s
    }

    /// Reads a sample without consuming it: the ack-by-hash probes must not
    /// remove it from the duplicate pool, and a sample still in the pool has
    /// been pushed exactly once, which is what makes their expectation exact.
    pub fn peek(&mut self, now_us: i64, age_s: i64, rng: &mut Rng) -> Option<Sample> {
        let sec = (now_us - self.t0_us) / 1_000_000 - age_s;
        if sec < 0 {
            return None;
        }
        let i = self.slot(sec);
        if self.stamp[i] != sec || self.secs[i].is_empty() {
            return None;
        }
        let j = rng.below(self.secs[i].len() as u64) as usize;
        Some(self.secs[i][j])
    }

    pub fn ram_bytes(&self) -> usize {
        self.secs
            .iter()
            .map(|v| v.capacity() * std::mem::size_of::<Sample>())
            .sum::<usize>()
            + self.stamp.len() * 8
    }
}

pub struct Rng(pub u64);

impl Rng {
    #[inline]
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    #[inline]
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next() % n
        }
    }
}

/// A message's hash, as the receiver computes it (xxh3_128 of the transaction
/// id; §9.1 step 4 — SQL never hashes).
#[inline]
pub fn hash_of(seq: u64) -> u128 {
    let mut b = [0u8; 24];
    b[0..8].copy_from_slice(&seq.to_le_bytes());
    b[8..24].copy_from_slice(b"queen-s2-dedup--");
    xxh3_128(&b)
}
