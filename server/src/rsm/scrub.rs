//! The store's background scrub: every node walks its own store — the LMDB
//! image, then the RAM tables — one bounded step at a time
//! ([`HeedStore::scrub_step`]).
//!
//! Every boot verifies the whole image as it loads it, so without this a
//! damaged file is found at the next restart: in a rolling upgrade, on several
//! nodes at once. The scrub finds it on the one node while the others are
//! healthy, and that node is restored or rejoins from them.
//!
//! - `QUEEN_STORE_SCRUB_EVERY_S` (default 21600, 6 h; `0` turns it off): a pass
//!   starts this long after the previous one started, the first this long
//!   after the open (the load has just verified every row).
//! - `QUEEN_STORE_SCRUB_ROWS_PER_S` (default 2000): the pace inside a pass, a
//!   tenth of it every 100 ms.
//!
//! A corrupt row is the step's error: the store is poisoned and its
//! `on_corrupt` hook runs — the binary's ends the process — exactly as when a
//! read meets the row. A retryable error (a full reader table) waits a second;
//! anything else stops the scrub with an ERROR line.

use std::path::Path;
use std::sync::{Arc, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::rsm::store::integrity::ScrubCursor;
use crate::rsm::store::{HeedStore, Store, StoreMetrics};

/// How often a pass runs and how fast ([`Config::from_env`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// Between the starts of two passes; zero turns the scrub off.
    pub every: Duration,
    /// Rows verified per second while a pass runs.
    pub rows_per_s: u64,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            every: Duration::from_secs(6 * 3600),
            rows_per_s: 2_000,
        }
    }
}

impl Config {
    /// `QUEEN_STORE_SCRUB_EVERY_S` and `QUEEN_STORE_SCRUB_ROWS_PER_S`; a value
    /// that does not parse keeps the default.
    pub fn from_env() -> Config {
        Config::from_vars(|k| std::env::var(k).ok())
    }

    pub(crate) fn from_vars(var: impl Fn(&str) -> Option<String>) -> Config {
        let d = Config::default();
        let num = |k: &str| var(k).and_then(|v| v.trim().parse::<u64>().ok());
        Config {
            every: num("QUEEN_STORE_SCRUB_EVERY_S").map_or(d.every, Duration::from_secs),
            rows_per_s: num("QUEEN_STORE_SCRUB_ROWS_PER_S")
                .filter(|n| *n > 0)
                .unwrap_or(d.rows_per_s),
        }
    }
}

/// A pass takes one step every this long.
const STEP: Duration = Duration::from_millis(100);
/// Between passes, how often the scrub looks whether its store is still open.
const IDLE_CHECK: Duration = Duration::from_secs(1);
/// The wait after a retryable error.
const RETRY: Duration = Duration::from_secs(1);

/// Start the scrub of `store` on a thread of its own. It holds a `Weak`, never
/// the store, so it keeps no closed environment open, and it ends within a
/// second of the last `Arc` going. `None` when `cfg` turns it off (or the
/// thread did not start, which is logged).
pub fn spawn(store: &Arc<HeedStore>, cfg: Config) -> Option<JoinHandle<()>> {
    if cfg.every.is_zero() {
        return None;
    }
    let weak = Arc::downgrade(store);
    let dir = store.path().to_path_buf();
    let started = std::thread::Builder::new()
        .name("queen-store-scrub".into())
        .spawn(move || run(&weak, &dir, cfg));
    match started {
        Ok(h) => Some(h),
        Err(e) => {
            tracing::warn!(
                target: "rsm",
                dir = %store.path().display(),
                error = %e,
                "store scrub: the thread did not start; the store is verified at boot only",
            );
            None
        }
    }
}

fn run(store: &Weak<HeedStore>, dir: &Path, cfg: Config) {
    let budget = usize::try_from(cfg.rows_per_s / 10)
        .unwrap_or(usize::MAX)
        .max(1);
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
        let Some(rows) = pass(store, dir, budget) else {
            return;
        };
        tracing::info!(
            target: "rsm",
            dir = %dir.display(),
            rows,
            secs = started.elapsed().as_secs(),
            "store scrub: a pass verified every row",
        );
    }
}

/// One whole pass: `Some(rows verified)`, or `None` when the scrub stops (the
/// store closed, or an error that is not retryable).
fn pass(store: &Weak<HeedStore>, dir: &Path, budget: usize) -> Option<u64> {
    let mut cur = ScrubCursor::default();
    let mut rows = 0;
    loop {
        let s = store.upgrade()?;
        let before = StoreMetrics::get(&s.metrics().scrubbed_rows);
        let step = s.scrub_step(&mut cur, budget);
        rows += StoreMetrics::get(&s.metrics().scrubbed_rows).saturating_sub(before);
        drop(s);
        match step {
            Ok(true) => return Some(rows),
            Ok(false) => std::thread::sleep(STEP),
            Err(e) if e.retryable() => std::thread::sleep(RETRY),
            Err(e) => {
                // A corrupt row was logged and poisoned the store where it was
                // found, and its hook ran; this line says the scrub is over.
                tracing::error!(
                    target: "rsm",
                    dir = %dir.display(),
                    error = %e,
                    "store scrub: stopped",
                );
                return None;
            }
        }
    }
}
