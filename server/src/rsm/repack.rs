//! The store's background repack: every node copies the rows of its RAM
//! tables into fresh allocations, one bounded step at a time
//! ([`HeedStore::repack_step`]), so the memory that rows deleted and rewritten
//! since the open left in half-used allocator pages can go back to the kernel.
//!
//! Every row is its own allocation, and the rows of every table share the
//! allocator's pages with the rest of the process. A page goes back to the
//! kernel only once every object on it is free, so after hours of partitions
//! created and cleaned up, cursors moved and dedup rows expired, the few rows
//! still alive on a page keep the whole page. The load at open builds every
//! table in one sorted pass, which is why a node uses less memory after a
//! restart (2026-11-25: a follower, 0.955 -> 0.704 GB of anonymous memory,
//! the same rows). A pass does the load's work on the live tables, a stripe
//! at a time, without the restart. Nothing a reader or a checkpoint sees
//! changes.
//!
//! - `QUEEN_STORE_REPACK_EVERY_S` (default 0: off): a pass starts this long
//!   after the previous one started, the first this long after the open.
//! - `QUEEN_STORE_REPACK_ROWS_PER_S` (default 100000): the pace inside a pass,
//!   a tenth of it every 100 ms, in calls of at most [`CALL_ROWS`] rows (a
//!   table's lock is held for about that many copies at most).
//!
//! The end of a pass is an INFO line: the rows and bytes it copied, how long it
//! took, and its longest call (an upper bound on its longest lock hold). A
//! retryable error waits a second; anything else (a poisoned store) stops the
//! repack with an ERROR line.

use std::path::Path;
use std::sync::{Arc, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::rsm::store::{HeedStore, RepackCursor};

/// How often a pass runs and how fast ([`Config::from_env`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// Between the starts of two passes; zero turns the repack off.
    pub every: Duration,
    /// Rows copied per second while a pass runs.
    pub rows_per_s: u64,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            every: Duration::ZERO,
            rows_per_s: 100_000,
        }
    }
}

impl Config {
    /// `QUEEN_STORE_REPACK_EVERY_S` and `QUEEN_STORE_REPACK_ROWS_PER_S`; a value
    /// that does not parse keeps the default.
    pub fn from_env() -> Config {
        Config::from_vars(|k| std::env::var(k).ok())
    }

    pub(crate) fn from_vars(var: impl Fn(&str) -> Option<String>) -> Config {
        let d = Config::default();
        let num = |k: &str| var(k).and_then(|v| v.trim().parse::<u64>().ok());
        Config {
            every: num("QUEEN_STORE_REPACK_EVERY_S").map_or(d.every, Duration::from_secs),
            rows_per_s: num("QUEEN_STORE_REPACK_ROWS_PER_S")
                .filter(|n| *n > 0)
                .unwrap_or(d.rows_per_s),
        }
    }
}

/// A pass takes one tick of steps every this long.
const STEP: Duration = Duration::from_millis(100);
/// Between passes, how often the repack looks whether its store is still open.
const IDLE_CHECK: Duration = Duration::from_secs(1);
/// The wait after a retryable error.
const RETRY: Duration = Duration::from_secs(1);
/// Rows one call copies at most: one call holds a table's lock for about this
/// many copies, rarely two or three locks one after the other (a stripe that
/// ends and the next one that starts).
pub const CALL_ROWS: usize = 1024;
/// Calls a tick makes at most, whatever they copied (empty tables cost a call
/// and copy nothing).
const CALLS_PER_TICK: usize = 256;

/// Start the repack of `store` on a thread of its own. It holds a `Weak`,
/// never the store, so it keeps no closed environment open, and it ends within
/// a second of the last `Arc` going. `None` when `cfg` turns it off (or the
/// thread did not start, which is logged).
pub fn spawn(store: &Arc<HeedStore>, cfg: Config) -> Option<JoinHandle<()>> {
    if cfg.every.is_zero() {
        return None;
    }
    let weak = Arc::downgrade(store);
    let dir = store.path().to_path_buf();
    let started = std::thread::Builder::new()
        .name("queen-store-repack".into())
        .spawn(move || run(&weak, &dir, cfg));
    match started {
        Ok(h) => Some(h),
        Err(e) => {
            tracing::warn!(
                target: "rsm",
                dir = %store.path().display(),
                error = %e,
                "store repack: the thread did not start; the tables are repacked by restarts only",
            );
            None
        }
    }
}

fn run(store: &Weak<HeedStore>, dir: &Path, cfg: Config) {
    let per_tick = cfg.rows_per_s.div_ceil(10).max(1);
    let mut next = Instant::now() + cfg.every;
    loop {
        loop {
            let now = Instant::now();
            if now >= next {
                break;
            }
            if store.strong_count() == 0 {
                return;
            }
            std::thread::sleep((next - now).min(IDLE_CHECK));
        }
        let started = Instant::now();
        next = started + cfg.every;
        let Some((cur, longest)) = pass(store, dir, per_tick) else {
            return;
        };
        tracing::info!(
            target: "rsm",
            dir = %dir.display(),
            rows = cur.rows,
            bytes = cur.bytes,
            secs = started.elapsed().as_secs(),
            longest_call_us = u64::try_from(longest.as_micros()).unwrap_or(u64::MAX),
            "store repack: a pass copied the RAM tables into fresh allocations",
        );
    }
}

/// One whole pass: `Some((cursor, longest call))`, or `None` when the repack
/// stops (the store closed, or an error that is not retryable).
fn pass(store: &Weak<HeedStore>, dir: &Path, per_tick: u64) -> Option<(RepackCursor, Duration)> {
    let mut cur = RepackCursor::default();
    let mut longest = Duration::ZERO;
    loop {
        let s = store.upgrade()?;
        let tick_start = cur.rows;
        let mut outcome = Ok(false);
        for _ in 0..CALLS_PER_TICK {
            let t = Instant::now();
            outcome = s.repack_step(&mut cur, CALL_ROWS);
            longest = longest.max(t.elapsed());
            if !matches!(outcome, Ok(false)) || cur.rows - tick_start >= per_tick {
                break;
            }
        }
        drop(s);
        match outcome {
            Ok(true) => return Some((cur, longest)),
            Ok(false) => std::thread::sleep(STEP),
            Err(e) if e.retryable() => std::thread::sleep(RETRY),
            Err(e) => {
                tracing::error!(
                    target: "rsm",
                    dir = %dir.display(),
                    error = %e,
                    "store repack: stopped",
                );
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_repack_is_off_unless_asked() {
        assert_eq!(Config::from_vars(|_| None), Config::default());
        assert!(Config::default().every.is_zero());
        let c = Config::from_vars(|k| match k {
            "QUEEN_STORE_REPACK_EVERY_S" => Some("3600".into()),
            "QUEEN_STORE_REPACK_ROWS_PER_S" => Some(" 50000 ".into()),
            _ => None,
        });
        assert_eq!(c.every, Duration::from_secs(3600));
        assert_eq!(c.rows_per_s, 50_000);
        // A pace of zero or one that does not parse keeps the default.
        let c = Config::from_vars(|k| match k {
            "QUEEN_STORE_REPACK_ROWS_PER_S" => Some("0".into()),
            _ => None,
        });
        assert_eq!(c.rows_per_s, Config::default().rows_per_s);
    }
}
