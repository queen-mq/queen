//! Background retention (2026-09-28): the partition walk leaves the planner
//! thread.
//!
//! The walk ([`maintenance::judge_partition`]) used to run inside a planning
//! cycle every 5 s, about 1 µs per partition. With many partitions the planner
//! stopped planning client commands for the whole walk: 100k partitions cost
//! ~100 ms, 1M ~1 s, and p99 followed. Here a thread walks every partition of
//! the node in pid order against its own store snapshot, at a set pace
//! (`QUEEN_RAFT_RETENTION_SCAN_PER_S`), and queues what it finds. The planner
//! takes a bounded batch per cycle and judges each proposal again against
//! committed state and every entry in flight ([`judge`]), because what the
//! scanner saw may be old:
//!
//! - a watermark never moves back: apply refuses that on every node, so a stale
//!   proposal keeps the greater of what it says and what the planner sees, or
//!   is dropped;
//! - an idle partition is deleted only if it is still idle now: the whole
//!   [`maintenance::partition_dead`] test runs again on committed state, and a
//!   partition anything in flight touches is kept, as before.
//!
//! Watermarks from an old snapshot are otherwise safe: they only cover data
//! older than the snapshot, and consumer positions in it are behind the current
//! ones. Leader only; a group per batcher, so every raft group has its own.
//! `QUEEN_RAFT_RETENTION_SCAN=0` puts the walk back on the planner thread.
//!
//! # Rounds (2026-10-01)
//!
//! A round — every partition once — starts at most once per maintenance
//! interval (`RETENTION_INTERVAL`, the batcher's `maintenance_every_ms`), as
//! the walk on the planner thread did: [`round_wait`]. Within a round the
//! slices keep their pace, so a big store is still walked at `per_s`, and a
//! round longer than the interval is followed by the next one at once. Before
//! this, the next round began the moment one wrapped: a store smaller than one
//! slice was walked every 41 ms, ~24 times a second, and the prod leader
//! (129 partitions, retention off) spent 0.22 of a core on this thread idle.
//! A new leadership starts its first round at once.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::rsm::effect::{Effect, Pid};
use crate::rsm::maintenance;
use crate::rsm::planner::Overlay;
use crate::rsm::store::{Reads, Result, Store, TypedReads};

/// What the scanner found for one partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Proposal {
    /// Move the partition's watermarks (never back: [`judge`]).
    Watermark {
        pid: Pid,
        log_start: u64,
        txns_start: u64,
    },
    /// The partition looked dead (idle past the cleanup age).
    Delete { pid: Pid },
}

impl Proposal {
    pub(crate) fn pid(&self) -> Pid {
        match *self {
            Proposal::Watermark { pid, .. } | Proposal::Delete { pid } => pid,
        }
    }
}

/// `QUEEN_RAFT_RETENTION_SCAN` (default on) and its pace. The rounds' cadence
/// is the batcher's maintenance interval, handed to [`spawn`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanConfig {
    /// `QUEEN_RAFT_RETENTION_SCAN_PER_S` (default 50,000): partitions walked per
    /// second within a round. A round over 1M partitions takes 20 s, over 10M
    /// ~3.5 min; one over a store smaller than a slice, a few milliseconds.
    pub per_s: u64,
    /// Partitions per store snapshot.
    pub slice: usize,
}

impl ScanConfig {
    /// `None` when `QUEEN_RAFT_RETENTION_SCAN` is `0`/`false`/`off`/`no`.
    pub fn from_env() -> Option<ScanConfig> {
        let off = matches!(
            std::env::var("QUEEN_RAFT_RETENTION_SCAN")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "false" | "off" | "no"
        );
        if off {
            return None;
        }
        let per_s = std::env::var("QUEEN_RAFT_RETENTION_SCAN_PER_S")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(50_000)
            .max(1);
        Some(ScanConfig {
            per_s,
            slice: 2_048,
        })
    }
}

/// Most proposals the planner takes in one cycle.
pub(crate) const TAKE_PER_CYCLE: usize = 1_024;

/// Shared by the scanner thread and the batcher.
pub(crate) struct ScanShared {
    /// This node plans as leader (the batcher's planning gate is open).
    leading: AtomicBool,
    /// Bumped each time [`ScanShared::set_leading`] opens the gate: the
    /// scanner starts a new leadership's first round at once, however briefly
    /// the gate was shut ([`round_wait`]).
    leaderships: AtomicU64,
    stop: AtomicBool,
    queue: Mutex<VecDeque<Proposal>>,
    /// Most proposals waiting: the scanner pauses while the planner catches up.
    cap: usize,
    /// The partition-cleanup settings [`judge`] runs `partition_dead` with.
    cleanup_enabled: bool,
    cleanup_days: i64,
    pub(crate) visited: AtomicU64,
    pub(crate) proposed: AtomicU64,
    pub(crate) planned: AtomicU64,
    pub(crate) dropped: AtomicU64,
    pub(crate) rounds: AtomicU64,
}

impl ScanShared {
    pub(crate) fn new(cfg: &maintenance::Config) -> Arc<ScanShared> {
        Arc::new(ScanShared {
            leading: AtomicBool::new(false),
            leaderships: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            queue: Mutex::new(VecDeque::new()),
            cap: 8 * TAKE_PER_CYCLE,
            cleanup_enabled: cfg.partition_cleanup_enabled,
            cleanup_days: cfg.partition_cleanup_days,
            visited: AtomicU64::new(0),
            proposed: AtomicU64::new(0),
            planned: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            rounds: AtomicU64::new(0),
        })
    }

    /// Open or close the scan with the batcher's planning gate. Closing drops
    /// what is queued: a follower plans nothing, and a later leadership scans
    /// again from its own committed state.
    pub(crate) fn set_leading(&self, on: bool) {
        // The count moves BEFORE the gate opens, so a scanner that sees the
        // gate open sees the new leadership too: counted after, a scanner
        // between the two started a round under the old count, then a second
        // one at once under the new. (One caller: the batcher's driver.)
        if on && !self.leading.load(Ordering::Acquire) {
            self.leaderships.fetch_add(1, Ordering::AcqRel);
        }
        self.leading.store(on, Ordering::Release);
        if !on {
            self.lock().clear();
        }
    }

    pub(crate) fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }

    pub(crate) fn queued(&self) -> usize {
        self.lock().len()
    }

    /// Up to `max` proposals, oldest first.
    pub(crate) fn take(&self, max: usize) -> Vec<Proposal> {
        let mut q = self.lock();
        let n = max.min(q.len());
        q.drain(..n).collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Proposal>> {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Start the scanner thread for this batcher's store. It runs until
/// [`ScanShared::stop`], walking only while [`ScanShared::set_leading`] is on,
/// a round at most every `every` (the maintenance interval, [`round_wait`]).
pub(crate) fn spawn<S: Store + Send + Sync + 'static>(
    store: Arc<S>,
    cfg: maintenance::Config,
    scan: ScanConfig,
    every: Duration,
    shared: Arc<ScanShared>,
) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("queen-rsm-retscan".into())
        .spawn(move || run(store, cfg, scan, every, shared))
        .map(|_| ())
}

/// How long the scanner waits before it starts a round: rounds begin at most
/// `every` apart (`last`: when the previous one of this leadership began), so
/// a store smaller than a slice is walked once per maintenance interval, not
/// once per slice pace. A round that ran longer than `every` is followed at
/// once (the wait is zero), and a leadership's first round (`last` = `None`)
/// starts at once.
pub(crate) fn round_wait(last: Option<Instant>, now: Instant, every: Duration) -> Duration {
    last.map_or(Duration::ZERO, |began| {
        (began + every).saturating_duration_since(now)
    })
}

/// How often a scanner with nothing to do looks again: at the stop flag, the
/// leadership gate, room in the queue, and the next round's start.
const IDLE_POLL: Duration = Duration::from_millis(100);

fn run<S: Store>(
    store: Arc<S>,
    cfg: maintenance::Config,
    scan: ScanConfig,
    every: Duration,
    shared: Arc<ScanShared>,
) {
    let slice = scan.slice.max(1);
    let pace = Duration::from_secs_f64(slice as f64 / scan.per_s.max(1) as f64);
    let mut cursor: Pid = 0;
    // The round gate: when this leadership's last round began (`None` before
    // its first), and which leadership that was.
    let mut last_round: Option<Instant> = None;
    let mut leadership = shared.leaderships.load(Ordering::Acquire);
    let mut round_started = Instant::now();
    let mut round_visited = 0u64;
    let mut round_proposed = 0u64;
    while !shared.stop.load(Ordering::Acquire) {
        if !shared.leading.load(Ordering::Acquire) || shared.queued() + slice > shared.cap {
            std::thread::sleep(IDLE_POLL);
            continue;
        }
        // After the gate: the count it was opened under is visible
        // ([`ScanShared::set_leading`]).
        let now_leadership = shared.leaderships.load(Ordering::Acquire);
        if now_leadership != leadership {
            leadership = now_leadership;
            last_round = None;
        }
        if cursor == 0 {
            // The next slice begins a round (the cursor is back at the first
            // partition only at a round's start).
            let wait = round_wait(last_round, Instant::now(), every);
            if !wait.is_zero() {
                std::thread::sleep(wait.min(IDLE_POLL));
                continue;
            }
            let now = Instant::now();
            last_round = Some(now);
            round_started = now;
            round_visited = 0;
            round_proposed = 0;
        }
        let started = Instant::now();
        let result = store.read(|r| {
            // The RSM clock: this node's wall clock, never behind the last
            // applied entry's (D5), as the planner's.
            let now_us = wall_now_us().max(r.last_now_us()?);
            maintenance::scan_slice(r, now_us, &cfg, &mut cursor, slice)
        });
        match result {
            Ok(found) => {
                let proposed = found.proposals.len() as u64;
                shared
                    .visited
                    .fetch_add(found.visited as u64, Ordering::Relaxed);
                shared.proposed.fetch_add(proposed, Ordering::Relaxed);
                round_visited += found.visited as u64;
                round_proposed += proposed;
                if !found.proposals.is_empty() && shared.leading.load(Ordering::Acquire) {
                    shared.lock().extend(found.proposals);
                }
                if found.wrapped {
                    shared.rounds.fetch_add(1, Ordering::Relaxed);
                    // A small store finishes a round in a few ms: only a big
                    // or slow round is worth a line. `secs` is the walk, from
                    // the round's first slice to its last.
                    let secs = round_started.elapsed().as_secs_f64();
                    if round_visited >= 100_000 || secs >= 10.0 {
                        tracing::info!(
                            target: "rsm",
                            partitions = round_visited,
                            proposals = round_proposed,
                            secs,
                            "rsm retention scan round",
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!(target: "rsm", error = %e, "rsm retention scan failed; retrying");
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
        }
        if let Some(rest) = pace.checked_sub(started.elapsed()) {
            std::thread::sleep(rest);
        }
    }
}

fn wall_now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// The effects to plan for `proposals`, judged again against committed state
/// (`r`) and every entry in flight or planned earlier this cycle (`ov`), and
/// how many were dropped. See the module doc for the two rules.
pub(crate) fn judge<R: Reads + ?Sized>(
    r: &R,
    ov: &Overlay,
    now_us: i64,
    scan: &ScanShared,
    proposals: &[Proposal],
) -> Result<(Vec<Effect>, usize)> {
    let mut effects = Vec::new();
    let mut dropped = 0usize;
    let mut seen: HashSet<Pid> = HashSet::with_capacity(proposals.len());
    for p in proposals {
        let pid = p.pid();
        // One effect per partition per entry: a second one would be judged
        // against state that does not hold the first yet.
        if !seen.insert(pid) || r.garbage(pid)?.is_some() {
            dropped += 1;
            continue;
        }
        let Some(part) = r.partition(pid)? else {
            dropped += 1;
            continue;
        };
        let Some(in_flight) = ov.retention_view(pid, &part.tenant, &part.queue) else {
            dropped += 1;
            continue;
        };
        match *p {
            Proposal::Watermark {
                log_start,
                txns_start,
                ..
            } => {
                let (cur_log, cur_txns) = in_flight.unwrap_or((part.log_start, part.txns_start));
                let log = log_start.max(cur_log);
                let txns = txns_start.max(cur_txns).min(log);
                if log > cur_log || txns > cur_txns {
                    effects.push(Effect::Watermark {
                        pid,
                        log_start: log,
                        txns_start: txns,
                    });
                } else {
                    dropped += 1;
                }
            }
            Proposal::Delete { .. } => {
                let cutoff = now_us.saturating_sub(scan.cleanup_days.max(1) * 86_400 * 1_000_000);
                if scan.cleanup_enabled
                    && !ov.touches_partition(pid)
                    && maintenance::partition_dead(r, pid, &part, cutoff, now_us)?
                {
                    effects.push(Effect::PartitionDelete { pid });
                } else {
                    dropped += 1;
                }
            }
        }
    }
    scan.planned
        .fetch_add(effects.len() as u64, Ordering::Relaxed);
    scan.dropped.fetch_add(dropped as u64, Ordering::Relaxed);
    Ok((effects, dropped))
}
