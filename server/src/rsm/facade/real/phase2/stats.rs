//! The figures of the queue list, the overview, the status page and the
//! backlog chart, at any partition count.
//!
//! Each is a sum over a queue's partitions: its row (the messages it holds),
//! its cursors (what the slowest group has not consumed, what is leased), its
//! segment files, and for the overview's lag the oldest message each pending
//! cursor has not consumed. Those reads walked every partition of the tenant
//! on every read of the list or the overview, and the backlog chart's sampler
//! walked every partition of every tenant once a minute: ~16 s of reads at
//! 10M partitions (2026-09-30).
//!
//! Now a read walks partitions itself only up to a budget,
//! `QUEEN_DASH_EXACT_PARTITIONS` (default 10,000) per read, queue by queue:
//! a deployment that small keeps exact figures, fresh on every read. A queue
//! past the budget shows the figures of its last walk, which come from a
//! sampler: one thread per store (`queen-dash-stats`), started by the first
//! read that ran out of budget, that walks every queue again and again at
//! `QUEEN_DASH_SAMPLE_PER_S` (default 20,000) partitions a second and stops
//! once nothing has read its figures for ten minutes. At 10M partitions a
//! queue's figures are up to ~8 minutes old; the overview says how old
//! (`statsAge`, seconds). A queue never walked yet shows its slowest group's
//! pending count from the group counters, and zeros for the rest. Dead
//! letters and retained bytes are the queue's own counters, exact at any size.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use crate::rsm::effect::{Pid, QueueConfig};
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::store::heed_store::HeedStore;
use crate::rsm::store::keys::{self, Counter};
use crate::rsm::store::rows::{self, PartitionRow};
use crate::rsm::store::{Keyspace, Reads, Store, StoreError, TypedReads};

const QUEUE_MODE: &str = "__QUEUE_MODE__";

/// Partitions one sampler read transaction covers.
const CHUNK: usize = 1024;

/// A sampler nobody has read from for this long stops.
const IDLE_STOP: Duration = Duration::from_secs(600);

/// The least time between two of a sampler's passes over the same queues.
const PASS_GAP: Duration = Duration::from_secs(1);

/// `QUEEN_DASH_EXACT_PARTITIONS` (default 10,000): partitions one read walks.
pub(super) fn exact_budget() -> u64 {
    static N: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *N.get_or_init(|| env_u64("QUEEN_DASH_EXACT_PARTITIONS").unwrap_or(10_000))
}

/// `QUEEN_DASH_SAMPLE_PER_S` (default 20,000): partitions a sampler walks a
/// second.
fn sample_rate() -> u64 {
    static N: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        env_u64("QUEEN_DASH_SAMPLE_PER_S")
            .filter(|v| *v > 0)
            .unwrap_or(20_000)
    })
}

fn env_u64(k: &str) -> Option<u64> {
    std::env::var(k).ok().and_then(|v| v.trim().parse().ok())
}

fn wall_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// One queue's figures, summed over its partitions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Figures {
    pub parts: i64,
    pub segments: i64,
    /// Messages the partitions hold.
    pub total: i64,
    /// Not consumed by the slowest cursor, leased ones included.
    pub pending: i64,
    /// Inside a live lease (a subset of `pending`).
    pub processing: i64,
    /// The most one cursor has pending, and the age in seconds of the oldest
    /// message one cursor has not consumed: the overview's lag inputs (zero
    /// when walked without [`LagReads`]).
    pub lag_pending: u64,
    pub lag_age_s: i64,
    /// When they were read (wall micros); 0 for an estimate.
    pub at_us: i64,
}

/// What the lag figures read besides the store: the stamps of the oldest
/// unconsumed messages.
#[derive(Clone)]
pub(super) struct LagReads {
    pub qlog: Option<QLogReader>,
    pub reader: crate::rsm::segments::Reader,
}

/// One queue's line of a tenant's figures.
pub(super) struct QueueStat {
    pub name: String,
    pub cfg: QueueConfig,
    pub fig: Figures,
    pub dead_letter: i64,
    pub retained: i64,
    /// Walked by this read (not the last walk's figures, nor an estimate).
    pub exact: bool,
}

/// Add one partition to `f`.
fn add_partition<R: TypedReads + ?Sized>(
    f: &mut Figures,
    r: &R,
    tenant: &str,
    pid: Pid,
    p: &PartitionRow,
    now: i64,
    lag: Option<&LagReads>,
) {
    f.parts += 1;
    f.total += (p.last_offset - p.log_start as i64 + 1).max(0);
    let mut cursors = Vec::new();
    let _ = r.scan_cursors(pid, usize::MAX, &mut |g, c| {
        cursors.push((g == QUEUE_MODE, c));
        true
    });
    let mut named_min: Option<i64> = None;
    let mut queue_mode: Option<i64> = None;
    let mut processing = 0i64;
    let mut sealed: Option<Vec<u32>> = None;
    for (qm, c) in &cursors {
        let slot = if *qm { &mut queue_mode } else { &mut named_min };
        *slot = Some(slot.map_or(c.committed, |v| v.min(c.committed)));
        if rows::lease_live(c, now) {
            processing += c
                .batch_end
                .map(|end| (end as i64 - c.committed).max(0))
                .unwrap_or(0);
        }
        if let Some(l) = lag {
            let pending = p.pending_from(c.committed);
            f.lag_pending = f.lag_pending.max(pending);
            if pending > 0 {
                // The segment path needs the partition's sealed files; the
                // queue log answers from its index.
                let files = match &l.qlog {
                    Some(_) => Vec::new(),
                    None => sealed
                        .get_or_insert_with(|| {
                            let mut v = Vec::new();
                            let _ = r.scan_partition_files(pid, usize::MAX, &mut |x| {
                                v.push(x);
                                true
                            });
                            v
                        })
                        .clone(),
                };
                if let Some(t) = super::reads::oldest_unconsumed_us(
                    l.qlog.as_ref(),
                    &l.reader,
                    tenant,
                    pid,
                    p,
                    &|| files.clone(),
                    c.committed,
                ) {
                    let age = ((now - t) as f64 / 1_000_000.0).round() as i64;
                    f.lag_age_s = f.lag_age_s.max(age);
                }
            }
        }
    }
    let pending = p.pending_from(named_min.or(queue_mode).unwrap_or(-1)) as i64;
    f.pending += pending;
    f.processing += processing.min(pending);
    let sealed_n = match &sealed {
        Some(v) => v.len() as i64,
        None => {
            let mut n = 0i64;
            let _ = r.scan_partition_files(pid, usize::MAX, &mut |_| {
                n += 1;
                true
            });
            n
        }
    };
    // The open tail is not in PartitionFiles yet, but it is a live segment
    // from the API's perspective.
    f.segments += sealed_n + i64::from(p.last_offset >= p.log_start as i64);
}

/// One queue's figures, walking at most `budget` of its partitions: `None`
/// when it has more.
pub(super) fn walk_queue<R: TypedReads + ?Sized>(
    r: &R,
    tenant: &str,
    queue: &str,
    now: i64,
    budget: u64,
    lag: Option<&LagReads>,
) -> Result<Option<Figures>, StoreError> {
    let mut pids = Vec::new();
    let cap = usize::try_from(budget).unwrap_or(usize::MAX);
    r.scan_queue_partitions(tenant, queue, None, cap.saturating_add(1), &mut |pid| {
        pids.push(pid);
        true
    })?;
    if pids.len() > cap {
        return Ok(None);
    }
    let mut f = Figures {
        at_us: now,
        ..Figures::default()
    };
    for pid in pids {
        if let Some(p) = r.partition(pid)? {
            add_partition(&mut f, r, tenant, pid, &p, now, lag);
        }
    }
    Ok(Some(f))
}

/// A queue never walked: its slowest group's pending count from the group
/// counters (a named group's, else the queue-mode group's), nothing else.
fn estimate<R: TypedReads + ?Sized>(r: &R, tenant: &str, queue: &str) -> Figures {
    let mut groups = Vec::new();
    let _ = r.scan_groups(tenant, queue, usize::MAX, &mut |g, _| {
        groups.push(g.to_string());
        true
    });
    let mut named: Option<i64> = None;
    let mut queue_mode: Option<i64> = None;
    for g in groups {
        let v = r
            .group_counter(tenant, queue, &g, Counter::Pending)
            .unwrap_or(0);
        let slot = if g == QUEUE_MODE {
            &mut queue_mode
        } else {
            &mut named
        };
        *slot = Some(slot.map_or(v, |x| x.max(v)));
    }
    Figures {
        pending: named.or(queue_mode).unwrap_or(0).max(0),
        ..Figures::default()
    }
}

/// Every queue of `tenant` with its figures: walked while the budget lasts,
/// else the last walk's (`known`), else an estimate.
pub(super) fn tenant_stats<R: TypedReads + ?Sized>(
    r: &R,
    tenant: &str,
    now: i64,
    budget: u64,
    lag: Option<&LagReads>,
    known: Option<&StoreStats>,
) -> Result<Vec<QueueStat>, StoreError> {
    let mut queues = Vec::new();
    r.scan_queues(tenant, usize::MAX, &mut |name, cfg| {
        queues.push((name.to_string(), cfg));
        true
    })?;
    let mut left = budget;
    let mut out = Vec::with_capacity(queues.len());
    for (name, cfg) in queues {
        let last = known.and_then(|k| k.recall(tenant, &name));
        // A queue its last walk found too big for what is left is not
        // walked again here.
        let walked = match last {
            Some(l) if l.parts as u64 > left => None,
            _ => walk_queue(r, tenant, &name, now, left, lag)?,
        };
        let (fig, exact) = match walked {
            Some(f) => {
                left -= f.parts as u64;
                if let Some(k) = known {
                    k.remember(tenant, &name, f);
                }
                (f, true)
            }
            None => (last.unwrap_or_else(|| estimate(r, tenant, &name)), false),
        };
        out.push(QueueStat {
            dead_letter: r.queue_counter(tenant, &name, Counter::DlqCount)?,
            retained: r.queue_counter(tenant, &name, Counter::RetainedBytes)?,
            name,
            cfg,
            fig,
            exact,
        });
    }
    Ok(out)
}

/// One queue's figures: walked within `budget`, else its last walk's (from
/// `known`), else an estimate. Whether it was walked.
pub(super) fn queue_figures<R: TypedReads + ?Sized>(
    r: &R,
    tenant: &str,
    queue: &str,
    now: i64,
    budget: u64,
    known: &StoreStats,
) -> Result<(Figures, bool), StoreError> {
    if let Some(f) = walk_queue(r, tenant, queue, now, budget, None)? {
        known.remember(tenant, queue, f);
        return Ok((f, true));
    }
    Ok((
        known
            .recall(tenant, queue)
            .unwrap_or_else(|| estimate(r, tenant, queue)),
        false,
    ))
}

/// `(tenant, queue, pending, processing)`: one row of the backlog chart.
pub(super) type Backlog = (String, String, i64, i64);

/// Every tenant's backlog and every backlogged queue's, `(tenant, queue,
/// pending, processing)` (see `phase2::tenant_backlogs`), within `budget`
/// partitions walked; the rest from `known`, else estimated. Whether every
/// queue was walked.
pub(super) fn backlogs<R: TypedReads + ?Sized>(
    r: &R,
    now: i64,
    budget: u64,
    known: Option<&StoreStats>,
) -> Result<(Vec<Backlog>, bool), StoreError> {
    let queues = all_queues(r)?;
    let split = |pending: i64, processing: i64| ((pending - processing).max(0), processing);
    let mut out: Vec<Backlog> = Vec::new();
    let mut total: Option<(String, i64, i64, usize)> = None;
    let mut left = budget;
    let mut all = true;
    for (tenant, queue) in queues {
        if total.as_ref().is_some_and(|t| t.0 != tenant) {
            if let Some((t, pending, processing, at)) = total.take() {
                let (p, q) = split(pending, processing);
                out[at] = (t, String::new(), p, q);
            }
        }
        if total.is_none() {
            // The tenant's slot, filled once its last queue is summed.
            out.push((tenant.clone(), String::new(), 0, 0));
            total = Some((tenant.clone(), 0, 0, out.len() - 1));
        }
        let last = known.and_then(|k| k.recall(&tenant, &queue));
        let walked = match last {
            Some(l) if l.parts as u64 > left => None,
            _ => walk_queue(r, &tenant, &queue, now, left, None)?,
        };
        let f = match walked {
            Some(f) => {
                left -= f.parts as u64;
                f
            }
            None => {
                all = false;
                last.unwrap_or_else(|| estimate(r, &tenant, &queue))
            }
        };
        if let Some(t) = total.as_mut() {
            t.1 += f.pending;
            t.2 += f.processing;
        }
        if f.pending > 0 {
            let (p, q) = split(f.pending, f.processing);
            out.push((tenant, queue, p, q));
        }
    }
    if let Some((t, pending, processing, at)) = total {
        let (p, q) = split(pending, processing);
        out[at] = (t, String::new(), p, q);
    }
    Ok((out, all))
}

/// Every (tenant, queue) of the store, a tenant's queues side by side.
fn all_queues<R: Reads + ?Sized>(r: &R) -> Result<Vec<(String, String)>, StoreError> {
    let mut queues: Vec<(String, String)> = Vec::new();
    r.scan_raw(Keyspace::Queues, &[], &[], usize::MAX, &mut |k, _| {
        if let Some((tenant, at)) = keys::read_name(k, 0) {
            if let Some((queue, _)) = keys::read_name(k, at) {
                queues.push((tenant, queue));
            }
        }
        true
    })?;
    Ok(queues)
}

// ---------------------------------------------------------------------------
// The last walk of every queue, and the sampler that keeps it fresh
// ---------------------------------------------------------------------------

/// One store's queue figures as last walked, and its sampler.
pub(super) struct StoreStats {
    store: Weak<HeedStore>,
    figures: RwLock<HashMap<(String, String), Figures>>,
    /// Wall seconds of the last read that used these figures.
    last_read: AtomicI64,
    running: AtomicBool,
    lag: Mutex<Option<LagReads>>,
}

impl StoreStats {
    fn recall(&self, tenant: &str, queue: &str) -> Option<Figures> {
        self.figures
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&(tenant.to_string(), queue.to_string()))
            .copied()
    }

    /// The partitions a queue had at its last walk.
    pub(super) fn lanes(&self, tenant: &str, queue: &str) -> Option<i64> {
        self.recall(tenant, queue).map(|f| f.parts)
    }

    fn remember(&self, tenant: &str, queue: &str, f: Figures) {
        self.figures
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert((tenant.to_string(), queue.to_string()), f);
    }

    /// A read used these figures: keep (or start) the sampler.
    pub(super) fn wake(self: &Arc<Self>, lag: Option<LagReads>) {
        self.last_read
            .store(wall_us() / 1_000_000, Ordering::Relaxed);
        if let Some(l) = lag {
            *self.lag.lock().unwrap_or_else(|p| p.into_inner()) = Some(l);
        }
        if self.running.swap(true, Ordering::AcqRel) {
            return;
        }
        let me = self.clone();
        let spawned = std::thread::Builder::new()
            .name("queen-dash-stats".into())
            .spawn(move || sample(me));
        if let Err(e) = spawned {
            self.running.store(false, Ordering::Release);
            tracing::warn!(target: "metrics", error = %e, "the queue figures sampler did not start");
        }
    }

    fn idle(&self) -> bool {
        let last = self.last_read.load(Ordering::Relaxed);
        wall_us() / 1_000_000 - last > IDLE_STOP.as_secs() as i64
    }
}

/// Every store's figures, one entry per store.
static STATS: Mutex<Vec<Arc<StoreStats>>> = Mutex::new(Vec::new());

/// `store`'s figures (made on first use).
pub(super) fn stats_for(store: &Arc<HeedStore>) -> Arc<StoreStats> {
    let mut all = STATS.lock().unwrap_or_else(|p| p.into_inner());
    all.retain(|s| s.store.strong_count() > 0);
    let weak = Arc::downgrade(store);
    if let Some(s) = all.iter().find(|s| s.store.ptr_eq(&weak)) {
        return s.clone();
    }
    let s = Arc::new(StoreStats {
        store: weak,
        figures: RwLock::new(HashMap::new()),
        last_read: AtomicI64::new(wall_us() / 1_000_000),
        running: AtomicBool::new(false),
        lag: Mutex::new(None),
    });
    all.push(s.clone());
    s
}

/// The sampler: every queue of the store, pass after pass, paced.
fn sample(s: Arc<StoreStats>) {
    let rate = sample_rate();
    loop {
        if s.idle() {
            s.running.store(false, Ordering::Release);
            // A read that woke it meanwhile found it running: go on.
            if !s.idle() && !s.running.swap(true, Ordering::AcqRel) {
                continue;
            }
            return;
        }
        let t_pass = Instant::now();
        let Some(store) = s.store.upgrade() else {
            s.running.store(false, Ordering::Release);
            return;
        };
        let queues = store.read(|r| all_queues(r));
        drop(store);
        let queues = match queues {
            Ok(q) => q,
            Err(e) => {
                tracing::warn!(target: "metrics", error = %e, "queue figures: the queue list is unreadable");
                std::thread::sleep(Duration::from_secs(5));
                continue;
            }
        };
        let seen: HashSet<(String, String)> = queues.iter().cloned().collect();
        for (tenant, queue) in queues {
            let Some(f) = walk_paced(&s, &tenant, &queue, rate) else {
                if s.store.strong_count() == 0 {
                    s.running.store(false, Ordering::Release);
                    return;
                }
                continue;
            };
            s.remember(&tenant, &queue, f);
        }
        // Queues deleted since.
        s.figures
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|k, _| seen.contains(k));
        if let Some(rest) = PASS_GAP.checked_sub(t_pass.elapsed()) {
            std::thread::sleep(rest);
        }
    }
}

/// One queue's figures, [`CHUNK`] partitions per read, at most `rate` a
/// second. `None` when the store is gone or unreadable.
fn walk_paced(s: &StoreStats, tenant: &str, queue: &str, rate: u64) -> Option<Figures> {
    let lag = s.lag.lock().unwrap_or_else(|p| p.into_inner()).clone();
    let mut f = Figures::default();
    let mut from: Option<Pid> = None;
    loop {
        let store = s.store.upgrade()?;
        let t0 = Instant::now();
        let now = wall_us();
        let chunk = store.read(|r| {
            let mut pids = Vec::with_capacity(CHUNK);
            r.scan_queue_partitions(tenant, queue, from, CHUNK, &mut |pid| {
                pids.push(pid);
                true
            })?;
            for &pid in &pids {
                if let Some(p) = r.partition(pid)? {
                    add_partition(&mut f, r, tenant, pid, &p, now, lag.as_ref());
                }
            }
            Ok(pids.last().copied().map(|last| (pids.len(), last)))
        });
        drop(store);
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(target: "metrics", error = %e, tenant, queue, "queue figures: a walk failed");
                return None;
            }
        };
        let Some((n, last)) = chunk else {
            break;
        };
        let want = Duration::from_secs_f64(n as f64 / rate as f64);
        if let Some(rest) = want.checked_sub(t0.elapsed()) {
            std::thread::sleep(rest);
        }
        if n < CHUNK {
            break;
        }
        from = Some(last + 1);
    }
    f.at_us = wall_us();
    Some(f)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rsm::facade::real::RaftFacade;
    use crate::rsm::facade::{PopReq, PushReq, ReqCtx, Rsm};

    fn facade(tag: &str) -> (RaftFacade, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "queen-dash-stats-{tag}-{}-{}",
            std::process::id(),
            wall_us()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let ctx = crate::rsm::facade::RsmBuildCtx {
            data_dir: dir.display().to_string(),
            notifier: crate::notify::Notifier::new(false),
            disk_high_pct: 99.0,
            disk_low_pct: 98.0,
        };
        (RaftFacade::open(&ctx).expect("open facade"), dir)
    }

    fn ctx() -> ReqCtx {
        ReqCtx::new(
            crate::config::DEFAULT_TENANT,
            crate::rsm::facade::Deadline::after(Duration::from_secs(10)),
        )
    }

    /// Within the budget every queue is walked, exactly; past it a queue
    /// shows its last walk, or — never walked — its slowest group's pending
    /// count from the counters.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_read_walks_within_its_budget_and_falls_back_past_it() {
        let (f, dir) = facade("budget");
        for (q, n) in [("a", 3), ("b", 5)] {
            let items: Vec<String> = (0..n)
                .map(|i| format!(r#"{{"queue":"{q}","partition":"p{i}","payload":{i}}}"#))
                .collect();
            f.push(
                ctx(),
                PushReq {
                    raw: format!(r#"{{"items":[{}]}}"#, items.join(",")).into_bytes(),
                },
            )
            .await
            .expect("push");
        }
        // One leased message in "b".
        let popped = f
            .pop_wildcard(
                ctx(),
                PopReq {
                    queue: "b".into(),
                    group: None,
                    batch: 1,
                    auto_ack: false,
                    wait: false,
                    timeout_ms: 1000,
                    options: Default::default(),
                },
            )
            .await
            .expect("pop");
        assert!(!popped.empty);
        let tenant = crate::config::DEFAULT_TENANT;
        let known = stats_for(&f.store);
        let now = wall_us();

        let all = f
            .store
            .read(|r| tenant_stats(r, tenant, now, 100, None, Some(&known)))
            .expect("read");
        let by = |v: &[QueueStat], q: &str| {
            let s = v.iter().find(|s| s.name == q).expect("queue");
            (
                s.exact,
                s.fig.parts,
                s.fig.total,
                s.fig.pending,
                s.fig.processing,
            )
        };
        assert_eq!(by(&all, "a"), (true, 3, 3, 3, 0));
        assert_eq!(by(&all, "b"), (true, 5, 5, 5, 1));

        // Three partitions of budget: "a" fits, "b" (five) shows its last
        // walk.
        let part = f
            .store
            .read(|r| tenant_stats(r, tenant, now, 3, None, Some(&known)))
            .expect("read");
        assert_eq!(by(&part, "a"), (true, 3, 3, 3, 0));
        assert_eq!(
            by(&part, "b"),
            (false, 5, 5, 5, 1),
            "the last walk's figures"
        );

        // Nothing walked before, nothing left: the counters' estimate.
        let fresh = StoreStats {
            store: Arc::downgrade(&f.store),
            figures: RwLock::new(HashMap::new()),
            last_read: AtomicI64::new(0),
            running: AtomicBool::new(false),
            lag: Mutex::new(None),
        };
        let est = f
            .store
            .read(|r| tenant_stats(r, tenant, now, 0, None, Some(&fresh)))
            .expect("read");
        assert_eq!(
            by(&est, "b"),
            (false, 0, 0, 5, 0),
            "the group's pending count"
        );

        // The backlog chart the same way.
        let (rows, all_walked) = f
            .store
            .read(|r| backlogs(r, now, 0, Some(&fresh)))
            .expect("read");
        assert!(!all_walked);
        assert!(
            rows.iter()
                .any(|(t, q, p, _)| t == tenant && q == "b" && *p == 5),
            "{rows:?}"
        );
        let (rows, all_walked) = f
            .store
            .read(|r| backlogs(r, now, 100, Some(&known)))
            .expect("read");
        assert!(all_walked);
        assert!(
            rows.iter()
                .any(|(t, q, p, l)| t == tenant && q == "b" && (*p, *l) == (4, 1)),
            "net of the leased one: {rows:?}"
        );
        drop(known);
        f.shutdown().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A queue with more partitions than an answer lists: `/sizes` and the
    /// queue detail list the first ones and say so; the detail's totals are
    /// the whole queue's.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_per_partition_answers_stop_at_their_bound() {
        let (f, dir) = facade("bound");
        f.push(
            ctx(),
            PushReq {
                raw: br#"{"items":[
                    {"queue":"w","partition":"a","payload":1},
                    {"queue":"w","partition":"b","payload":2},
                    {"queue":"w","partition":"c","payload":3}
                ]}"#
                .to_vec(),
            },
        )
        .await
        .expect("push");
        let tenant = crate::config::DEFAULT_TENANT;
        let known = stats_for(&f.store);
        let (sizes, truncated) = f
            .store
            .read(|r| super::super::queue_sizes(r, tenant, "w", 2))
            .expect("read")
            .expect("queue");
        assert_eq!((sizes.len(), truncated), (2, true));
        let (sizes, truncated) = f
            .store
            .read(|r| super::super::queue_sizes(r, tenant, "w", 3))
            .expect("read")
            .expect("queue");
        assert_eq!((sizes.len(), truncated), (3, false));

        let d = f
            .store
            .read(|r| super::super::queue_detail(r, tenant, "w", wall_us(), 2, &known))
            .expect("read")
            .expect("queue");
        assert_eq!(d["partitions"].as_array().map(Vec::len), Some(2));
        assert_eq!(d["partitionsTruncated"], true);
        assert_eq!(d["totals"]["total"], 3, "the whole queue's: {d}");
        assert_eq!(d["totals"]["pending"], 3);
        let whole = f
            .store
            .read(|r| super::super::queue_detail(r, tenant, "w", wall_us(), 3, &known))
            .expect("read")
            .expect("queue");
        assert_eq!(whole["partitions"].as_array().map(Vec::len), Some(3));
        assert!(whole.get("partitionsTruncated").is_none());
        assert_eq!(whole["totals"], d["totals"]);
        drop(known);
        f.shutdown().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    /// The sampler walks every queue at its pace and keeps the figures a read
    /// past its budget shows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_sampler_keeps_the_figures_of_every_queue() {
        let (f, dir) = facade("sampler");
        f.push(
            ctx(),
            PushReq {
                raw: br#"{"items":[{"queue":"s","partition":"x","payload":1},{"queue":"s","partition":"y","payload":2}]}"#
                    .to_vec(),
            },
        )
        .await
        .expect("push");
        let known = stats_for(&f.store);
        known.wake(None);
        let t0 = Instant::now();
        let got = loop {
            if let Some(g) = known.recall(crate::config::DEFAULT_TENANT, "s") {
                break g;
            }
            assert!(t0.elapsed() < Duration::from_secs(10), "the sampler walked");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!((got.parts, got.total, got.pending), (2, 2, 2));
        assert!(got.at_us > 0);
        drop(known);
        f.shutdown().await;
        let _ = std::fs::remove_dir_all(dir);
    }
}
