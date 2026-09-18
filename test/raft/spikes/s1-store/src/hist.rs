//! Fixed-bucket latency histogram, microsecond resolution.
//!
//! Values below 128 µs are exact; above that each power of two is split into
//! 32 sub-buckets, so a reported percentile is at most 3.2 % above the true
//! value. No allocation after construction, so sampling never perturbs the
//! measurement it is taking.

pub const NBUCKETS: usize = 2048;

pub struct Hist {
    buckets: Vec<u64>,
    count: u64,
    sum: u128,
    max: u64,
}

fn bucket_of(v: u64) -> usize {
    if v < 128 {
        return v as usize;
    }
    let k = 63 - v.leading_zeros() as usize; // floor(log2(v)), >= 7
    let sub = ((v >> (k - 5)) & 31) as usize;
    let idx = 128 + (k - 7) * 32 + sub;
    if idx >= NBUCKETS {
        NBUCKETS - 1
    } else {
        idx
    }
}

/// Upper bound of the bucket, in µs.
fn bucket_hi(idx: usize) -> u64 {
    if idx < 128 {
        return idx as u64;
    }
    let k = 7 + (idx - 128) / 32;
    let sub = ((idx - 128) % 32) as u64;
    (((32 + sub + 1) as u64) << (k - 5)) - 1
}

impl Hist {
    pub fn new() -> Self {
        Self {
            buckets: vec![0; NBUCKETS],
            count: 0,
            sum: 0,
            max: 0,
        }
    }

    /// Fold another histogram into this one (thread-aggregate percentiles).
    pub fn merge(&mut self, other: &Hist) {
        for (i, b) in other.buckets.iter().enumerate() {
            self.buckets[i] += b;
        }
        self.count += other.count;
        self.sum += other.sum;
        if other.max > self.max {
            self.max = other.max;
        }
    }

    pub fn record(&mut self, micros: u64) {
        self.buckets[bucket_of(micros)] += 1;
        self.count += 1;
        self.sum += micros as u128;
        if micros > self.max {
            self.max = micros;
        }
    }

    #[allow(dead_code)]
    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn max(&self) -> u64 {
        self.max
    }

    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum as f64 / self.count as f64
        }
    }

    pub fn pct(&self, p: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = ((self.count as f64) * p).ceil() as u64;
        let target = target.max(1);
        let mut seen = 0u64;
        for (i, c) in self.buckets.iter().enumerate() {
            seen += *c;
            if seen >= target {
                return bucket_hi(i);
            }
        }
        self.max
    }

    /// "p50=.. p90=.. p99=.. p999=.. max=.." in ms with 3 decimals.
    pub fn line(&self) -> String {
        format!(
            "n={} p50={:.3} p90={:.3} p99={:.3} p99.9={:.3} max={:.3} mean={:.3} (ms)",
            self.count,
            self.pct(0.50) as f64 / 1000.0,
            self.pct(0.90) as f64 / 1000.0,
            self.pct(0.99) as f64 / 1000.0,
            self.pct(0.999) as f64 / 1000.0,
            self.max as f64 / 1000.0,
            self.mean() / 1000.0,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_below_128() {
        let mut h = Hist::new();
        for v in 0..128u64 {
            h.record(v);
        }
        assert_eq!(h.pct(0.50), 63);
        assert_eq!(h.max(), 127);
    }

    #[test]
    fn error_bounded_above_128() {
        for v in [128u64, 999, 1_000_000, 30_000_000] {
            let hi = bucket_hi(bucket_of(v));
            assert!(hi >= v, "{v} -> {hi}");
            assert!((hi - v) as f64 / v as f64 <= 0.033, "{v} -> {hi}");
        }
    }
}
