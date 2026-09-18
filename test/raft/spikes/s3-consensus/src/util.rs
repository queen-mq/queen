//! Small helpers: term extraction, percentiles, wall clock.

use crate::types::LogId;

/// The term inside a log id (the type config pins the advanced leader id).
pub fn term_of(id: &LogId) -> u64 {
    id.leader_id.term
}

/// Percentile of an unsorted sample, in the sample's own unit.
pub fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = (p / 100.0) * (sorted.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        sorted[lo] + (sorted[hi] - sorted[lo]) * (rank - lo as f64)
    }
}

pub struct Stats {
    pub n: usize,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub max: f64,
    pub mean: f64,
}

pub fn stats(mut v: Vec<f64>) -> Stats {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    let mean = if n == 0 {
        f64::NAN
    } else {
        v.iter().sum::<f64>() / n as f64
    };
    Stats {
        n,
        p50: pct(&v, 50.0),
        p90: pct(&v, 90.0),
        p99: pct(&v, 99.0),
        max: v.last().copied().unwrap_or(f64::NAN),
        mean,
    }
}

impl std::fmt::Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "n={} p50={:.2} p90={:.2} p99={:.2} max={:.2} mean={:.2} (ms)",
            self.n, self.p50, self.p90, self.p99, self.max, self.mean
        )
    }
}
