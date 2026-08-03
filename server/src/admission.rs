// Single admission arbiter for every PG write transaction — "the commit path".
//
// Replaces the two per-lane Vegas limiters (vegas.rs, removed) and the fusion
// self-gates. Motivation, measured 2026-08-03 (crossbench, 2k ev/s / 1000
// sparse lanes; results in benchmark-queen/crossbench/results/2026-08-03-pgmq-ab):
//
//   - Vegas inferred congestion from per-op RTT. On a WAL-bound path the RTT
//     above base is mostly the group-commit flush-cycle wait — intrinsic cost
//     that admission cannot remove — so the estimator counted the floor as
//     queueing. With absolute alpha/beta the dead band widened at low limits,
//     making them an attractor: vegas_pop sat at the arithmetic fixed point 7
//     while the broker left 2+ cores idle. Forcing the window wide (min 64)
//     tripled p50 at 2k and moved nothing at 5k: neither direction of the old
//     knob was the lever.
//   - N adaptive loops (two Vegas lanes, fusion dispatch gates, per-flush pop
//     admission) each read the others' output as their own signal and
//     self-balanced into that equilibrium.
//
// This module inverts the topology: ONE owner for admission, sensing the
// shared resource directly instead of inferring it from op RTT.
//
// Sensor — passive "trains": group commit makes write completions cluster in
// time (one flush ack releases every transaction that boarded it). Clustering
// the broker's own commit completions therefore measures the flush pipeline
// with zero PG-side telemetry (works on any managed Postgres):
//   train size   = txns per flush  (amortization, measured not assumed)
//   train cadence= flush cycles/s
//   cycle        = gap between train starts (the flush interval)
// Queueing — the thing admission CAN remove — shows up as cycle inflation and
// as in-flight slots older than a couple of cycles. That is the control error.
//
// Control — total budget B of concurrently admitted write transactions,
// AIMD once per tick:
//   grow   when the budget was saturated during the tick AND the cycle is not
//          inflating (the device still absorbs added concurrency);
//   shrink when the cycle inflates vs its recent floor or the oldest admitted
//          slot has waited several cycles (real queueing);
//   hold   otherwise.
// B is clamped to [min, max] and never above pool_size - pool_reserve: admitted
// work must never starve on the connection pool, and the reserve keeps rare
// unmetered writes (admin ops) from deadlocking behind metered ones.
//
// Lanes — Push / Pop / Ack / Maint. A grant is work-conserving: any lane may
// use any free slot below B. Guarantees act on the WAKE order when B is
// exhausted: released slots go first to lanes below their guaranteed share,
// in priority order Ack > Pop > Push > Maint (acks unblock lanes and shorten
// everyone's lap; maintenance can always wait).
//
// Slot accounting is RAII: dropping a Slot releases it — there is no code path
// that can leak in-flight accounting (the class of bug vegas.rs had at eight
// call sites). Feeding the sensor is explicit: Slot::commit_done(rtt) marks a
// real commit completion; a slot dropped without it (pool failure, empty take)
// releases cleanly and contributes no fake train sample.
//
// Degraded signal: with synchronous_commit=off (or a device faster than the
// clustering gap) commit waits collapse and trains carry no information. The
// adapter detects it (commit rtt floor under NOSYNC_FLOOR) and pins B to a
// static cap instead of chasing noise; the mode is visible in telemetry.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

pub const LANES: usize = 4;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Lane {
    Push = 0,
    Pop = 1,
    Ack = 2,
    Maint = 3,
}

impl Lane {
    pub const ALL: [Lane; LANES] = [Lane::Push, Lane::Pop, Lane::Ack, Lane::Maint];

    pub fn name(self) -> &'static str {
        match self {
            Lane::Push => "push",
            Lane::Pop => "pop",
            Lane::Ack => "ack",
            Lane::Maint => "maint",
        }
    }
}

/// Wake priority when B is exhausted: acks first (they unblock lanes), then
/// pops, then pushes, maintenance last.
const WAKE_ORDER: [Lane; LANES] = [Lane::Ack, Lane::Pop, Lane::Push, Lane::Maint];

// ---------------------------------------------------------------------------
// CONTROL CONSTANTS — every tunable of the sensor and of the two control laws
// lives here, named, with unit, role, the evidence that set its value, and the
// failure mode you buy by moving it. Deliberately NOT env knobs: operators get
// the QUEEN_ADMISSION_* bounds; the laws find the operating point. A constant
// tunable from outside is a controller someone has to babysit — the disease
// this module replaced (measured 2026-08-03, crossbench 2026-08-03-pgmq-ab).
// ---------------------------------------------------------------------------

// ---- Sensor (commit trains) ----

/// Below this commit rtt the flush wait is not observable (synchronous_commit
/// off, or a device faster than the clustering gap). Such completions still
/// count as NOSYNC evidence but are excluded from train clustering: they are
/// not flush acks. Evidence: this rig's flush cycle is ~0.4 ms (pg_stat_wal
/// 2 504 sync/s); an empty pop claim completes in 0.3-1 ms. Too high: real
/// commits are dropped from the trains; too low: nosync mode never triggers.
const NOSYNC_FLOOR: Duration = Duration::from_micros(200);

/// Sensor memory. Long enough for stable quantiles at low rates (30 commits/s
/// -> 300 rtt samples), short enough that a finished load phase ages out in
/// one window instead of anchoring the floors for minutes.
const TRAIN_WINDOW: Duration = Duration::from_secs(10);
const RTT_WINDOW: Duration = Duration::from_secs(10);

/// Anti-jam: force-close the open train at span_cap = max(gap x MULT, MIN)
/// even without a >gap silence. Without it a completion stream denser than
/// one per gap (>3.3k/s at the 300 us default) keeps ONE train open forever,
/// closed trains age out, cycle() returns None and the adapter freezes
/// (review finding, 2026-08-03).
const TRAIN_SPAN_CAP_MULT: f64 = 10.0;
const TRAIN_SPAN_CAP_MIN: Duration = Duration::from_millis(3);

/// The cycle estimate is the median of the LAST N gaps against the window p25
/// as floor. N=8: reacts within ~8 flushes (single-digit ms under load). The
/// floor is p25 and not the minimum because over thousands of gaps the raw min
/// is an extreme-value outlier — 2x-min fired on jitter alone and pinned the
/// budget at min (measured: real cycle ~0.4 ms, window min ~0.2, recent p50
/// ~0.7 -> permanent false inflation in the first build).
const CYCLE_RECENT_GAPS: usize = 8;
const CYCLE_FLOOR_DIV: usize = 4; // sorted[len/4] = p25

/// NoSync detection: p10 of the rtt window (sorted[len/10]) under NOSYNC_FLOOR
/// for HYSTERESIS consecutive ticks. p10 and not the min: one anomalously fast
/// sample (empty claim, commit that boarded an already-flushing train) must
/// not flip the mode (review finding). Hysteresis 3 ticks = 1.5 s at the
/// default cadence.
const NOSYNC_Q_DIV: usize = 10;
const NOSYNC_HYSTERESIS_TICKS: u32 = 3;

// ---- Global budget law (B) ----

/// Queueing evidence = the oldest live-lane slot is older than QUEUE_CYCLES
/// flush cycles AND older than QUEUE_AGE_MIN wall-clock. The absolute floor
/// exists because slot age conflates statement EXEC with commit wait — a
/// legitimate pop claim runs 5-50 ms. KNOWN-WEAK: this is the constant the
/// per-pop wait-breakdown probe is meant to replace with a真 commit-wait
/// signal; at 20 ms it still fires on contended-but-healthy claims and causes
/// budget hunting (measured arbiter4: 19<->31).
const QUEUE_CYCLES: f64 = 4.0;
const QUEUE_AGE_MIN: Duration = Duration::from_millis(20);

/// Growth when saturated: max(GROW_MULT x B, ln B, 1). The multiplicative term
/// is for cold start — init 16 must reach a ~30-slot demand in a few ticks
/// (arbiter2 took 80 s with ln-only and the whole run drowned in the
/// transient); ln keeps steps humble near the edge.
const GROW_MULT: f64 = 0.15;

// ---- Per-lane marginal-throughput controller ----

/// Hard floor of any lane cap. The wake-order guarantee (share of B) protects
/// from starvation; it is NOT a cap floor — pop's optimum sits well below its
/// share on sparse lanes (the test that found this: share 0.40 of B=32 would
/// force cap 12 where the lane wants ~8).
const LANE_MIN_CAP: f64 = 2.0;

/// Probe step: max(FRAC x cap, 1) slots, remembered so a failed probe reverts
/// exactly what it raised.
const LANE_PROBE_STEP_FRAC: f64 = 0.10;

/// A probe is kept only if lane throughput grew by this factor — MARGINAL
/// THROUGHPUT ONLY. An efficiency-tolerance arm (keep if compl/slot >= 90% of
/// before) was measured and rejected: while a backlog drains, extra slots
/// always buy a little throughput at halved efficiency, and pop ratcheted to
/// cap 16 / p99 64 ms (arbiter5). 5% sits above tick noise at thousands of
/// completions per tick.
const LANE_PROBE_ACCEPT_GAIN: f64 = 1.05;

/// Ticks a lane holds still after a reverted probe (3 s at default cadence):
/// re-probing the same wall every tick is how ratchets happen.
const LANE_PROBE_BACKOFF_TICKS: u8 = 6;

/// Downward pressure the probe ratchet lacks: a BUSY lane earning under
/// DECAY_EFF_FRAC of its own rolling best compl/slot is in the contention
/// regime and steps its cap down by DECAY_STEP_FRAC (min 1). Best decays by
/// EFF_BEST_DECAY per tick (~70 s half-life at 500 ms): old glories fade, so
/// a genuine regime change re-baselines within a couple of minutes.
const LANE_DECAY_EFF_FRAC: f64 = 0.6;
const LANE_DECAY_STEP_FRAC: f64 = 0.05;
const LANE_DECAY_BACKOFF_TICKS: u8 = 2;
const LANE_EFF_BEST_DECAY: f64 = 0.995;

/// Minimum completions in a tick for any lane judgement (probe verdict, press,
/// decay): below this the efficiency estimate is noise.
const LANE_JUDGE_MIN_DONE: f64 = 8.0;

/// A lane is "pressing" its cap when its busy-average sits within this many
/// slots of the cap; the busy-average floor avoids dividing by ~0 when
/// computing compl/slot on a nearly idle lane.
const LANE_PRESS_SLACK: f64 = 0.5;
const LANE_EFF_MIN_BUSY: f64 = 0.1;

/// Ignore adapter passes closer than this (s): protects the per-tick counters
/// from a double fire.
const LANE_MIN_DT_S: f64 = 0.05;

#[derive(Clone, Debug)]
pub struct AdmissionCfg {
    pub init: u64,
    pub min: u64,
    pub max: u64,
    /// Connections kept out of the budget for unmetered writes and reads.
    pub pool_reserve: u64,
    pub pool_size: u64,
    /// Completions closer than this belong to the same train.
    pub train_gap: Duration,
    /// Adapter cadence.
    pub tick: Duration,
    /// Guaranteed minimum share of B per lane (push, pop, ack, maint).
    pub share: [f64; LANES],
    /// Static budget while the signal is degraded (nosync mode).
    pub nosync_budget: u64,
    pub trace: bool,
}

impl AdmissionCfg {
    fn ceiling(&self) -> u64 {
        self.max
            .min(self.pool_size.saturating_sub(self.pool_reserve))
            .max(self.min)
    }
}

struct Waiter {
    lane: Lane,
    tx: oneshot::Sender<Slot>, // the granted slot itself: if the waiter's
    // future was dropped after a successful send, the Slot is dropped inside
    // the dead channel and its RAII release runs — the grant can never leak.
}

#[derive(Default)]
struct TrainStats {
    // Closed trains: (start instant, size, span start->last completion).
    trains: VecDeque<(Instant, u32, Duration)>,
    // Open train.
    cur_start: Option<Instant>,
    cur_last: Option<Instant>,
    cur_len: u32,
    // Commit rtt samples for the floor estimate / nosync detection.
    rtts: VecDeque<(Instant, Duration)>,
    completions: u64,
}

impl TrainStats {
    fn feed(&mut self, now: Instant, rtt: Duration, gap: Duration) {
        self.completions += 1;
        self.rtts.push_back((now, rtt));
        while let Some(&(t, _)) = self.rtts.front() {
            if now.duration_since(t) > RTT_WINDOW {
                self.rtts.pop_front();
            } else {
                break;
            }
        }
        if rtt < NOSYNC_FLOOR {
            // No observable flush wait: nosync evidence (kept in rtts above),
            // not a flush ack — do not let it shape the trains.
            return;
        }
        let span_cap = gap.mul_f64(TRAIN_SPAN_CAP_MULT).max(TRAIN_SPAN_CAP_MIN);
        match (self.cur_start, self.cur_last) {
            (Some(start), Some(last))
                if now.duration_since(last) <= gap
                    && now.duration_since(start) <= span_cap =>
            {
                self.cur_len += 1;
                self.cur_last = Some(now);
            }
            (Some(start), Some(last)) => {
                self.trains
                    .push_back((start, self.cur_len, last.duration_since(start)));
                self.cur_start = Some(now);
                self.cur_last = Some(now);
                self.cur_len = 1;
            }
            _ => {
                self.cur_start = Some(now);
                self.cur_last = Some(now);
                self.cur_len = 1;
            }
        }
        while let Some(&(t, _, _)) = self.trains.front() {
            if now.duration_since(t) > TRAIN_WINDOW {
                self.trains.pop_front();
            } else {
                break;
            }
        }
    }

    /// (recent cycle, window floor): the RECENT median gap between train
    /// starts — last 8 gaps, so the adapter reacts within a handful of flushes
    /// — against the window minimum as the uninflated floor. A whole-window
    /// median would need the majority of 10s to inflate before moving.
    fn cycle(&self) -> Option<(Duration, Duration)> {
        if self.trains.len() < 3 {
            return None;
        }
        let gaps: Vec<Duration> = self
            .trains
            .iter()
            .zip(self.trains.iter().skip(1))
            .map(|((a, _, _), (b, _, _))| b.duration_since(*a))
            .collect();
        let mut sorted = gaps.clone();
        sorted.sort();
        // p25 as the floor: over thousands of gaps the raw minimum is an
        // extreme-value outlier and 2x-min fires on jitter alone (measured
        // 2026-08-03: real cycle ~0.4 ms, window min ~0.2, recent p50 ~0.7 —
        // permanent false "inflation").
        let floor = sorted[sorted.len() / CYCLE_FLOOR_DIV];
        let mut recent: Vec<Duration> = gaps.iter().rev().take(CYCLE_RECENT_GAPS).copied().collect();
        recent.sort();
        let p50 = recent[recent.len() / 2];
        Some((p50, floor))
    }

    fn txn_per_train(&self) -> (f64, u32) {
        if self.trains.is_empty() {
            return (0.0, 0);
        }
        let sum: u64 = self.trains.iter().map(|&(_, n, _)| n as u64).sum();
        let mut sizes: Vec<u32> = self.trains.iter().map(|&(_, n, _)| n).collect();
        sizes.sort();
        (
            sum as f64 / self.trains.len() as f64,
            sizes[(sizes.len() * 95 / 100).min(sizes.len() - 1)],
        )
    }

    fn trains_per_s(&self) -> f64 {
        self.trains.len() as f64 / TRAIN_WINDOW.as_secs_f64()
    }

    /// p10 of commit rtts: robust nosync evidence. The raw minimum is a single
    /// outlier (an empty claim, a commit that boarded an already-flushing
    /// train) and would flip the mode on one sample.
    fn rtt_p10(&self) -> Option<Duration> {
        if self.rtts.is_empty() {
            return None;
        }
        let mut v: Vec<Duration> = self.rtts.iter().map(|&(_, r)| r).collect();
        v.sort();
        Some(v[v.len() / NOSYNC_Q_DIV])
    }
}

struct Core {
    budget: f64,
    inflight: [u64; LANES],
    waiting: [VecDeque<Waiter>; LANES],
    // Admitted slots by id -> (lane, admission instant): the BTreeMap order is
    // admission order, so the first entry is the oldest in-flight slot.
    slots: BTreeMap<u64, (Lane, Instant)>,
    next_id: u64,
    saturated: bool, // a refusal caused by the GLOBAL budget since last tick
    nosync_ticks: u32,
    // Per-lane marginal-throughput controller state. cap[l] is the lane's own
    // concurrency ceiling inside B: a lane grows it only while its measured
    // completions-per-busy-slot does not degrade (the pop claim path has
    // NEGATIVE returns to concurrency on sparse lanes — measured 2026-08-03:
    // p99_pop 9.25 ms at 8 slots vs 45.75 ms at ~25). B stays the global
    // safety ceiling; caps find each lane's sweet spot inside it.
    cap: [f64; LANES],
    lane_saturated: [bool; LANES],
    done_count: [u64; LANES],
    busy_ns: [u128; LANES],
    busy_touched: Instant,
    lane_e_before: [f64; LANES],
    lane_t_before: [f64; LANES],
    lane_probe: [bool; LANES],
    lane_raise: [f64; LANES],
    // Rolling best completions-per-busy-slot per lane (decaying max, ~1 min
    // memory): the downward pressure the probe ratchet lacks. A lane running
    // far below its own best efficiency while busy is in the contention
    // regime and walks its cap down until efficiency recovers.
    lane_eff_best: [f64; LANES],
    lane_backoff: [u8; LANES],
    last_adapt: Instant,
    trains: TrainStats,
    mode: Mode,
    last_change: &'static str,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    Normal,
    NoSync,
}

pub struct Admission {
    cfg: AdmissionCfg,
    core: Mutex<Core>,
}

/// One admitted write transaction. Dropping it releases the slot (never
/// leaks); call `commit_done` right after a real commit completion so the
/// train sensor sees it.
pub struct Slot {
    adm: Arc<Admission>,
    id: u64,
    lane: Lane,
    done: bool,
}

impl Slot {
    pub fn commit_done(&mut self, sql_rtt: Duration) {
        if self.done {
            return;
        }
        self.done = true;
        let mut c = self.adm.core.lock().unwrap();
        let gap = self.adm.cfg.train_gap;
        c.done_count[self.lane as usize] += 1;
        c.trains.feed(Instant::now(), sql_rtt, gap);
    }

    #[cfg(test)]
    pub fn lane(&self) -> Lane {
        self.lane
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let failed = {
            let mut c = self.adm.core.lock().unwrap();
            Admission::touch_busy(&mut c);
            c.slots.remove(&self.id);
            c.inflight[self.lane as usize] =
                c.inflight[self.lane as usize].saturating_sub(1);
            Admission::wake_next(&self.adm, &mut c)
        };
        drop(failed); // out-of-lock: each re-enters Drop and re-offers capacity
    }
}

impl Admission {
    pub fn new(cfg: AdmissionCfg) -> Arc<Admission> {
        let budget = (cfg.init.clamp(cfg.min, cfg.ceiling())) as f64;
        let cfg2 = cfg.clone();
        let adm = Arc::new(Admission {
            cfg,
            core: Mutex::new(Core {
                budget,
                inflight: [0; LANES],
                waiting: Default::default(),
                slots: BTreeMap::new(),
                next_id: 0,
                saturated: false,
                nosync_ticks: 0,
                cap: {
                    let mut c0 = [1.0f64; LANES];
                    for l in 0..LANES {
                        c0[l] = (budget * cfg2.share[l]).max(2.0);
                    }
                    c0
                },
                lane_saturated: [false; LANES],
                done_count: [0; LANES],
                busy_ns: [0; LANES],
                busy_touched: Instant::now(),
                lane_e_before: [0.0; LANES],
                lane_t_before: [0.0; LANES],
                lane_probe: [false; LANES],
                lane_raise: [0.0; LANES],
                lane_eff_best: [0.0; LANES],
                lane_backoff: [0; LANES],
                last_adapt: Instant::now(),
                trains: TrainStats::default(),
                mode: Mode::Normal,
                last_change: "init",
            }),
        });
        adm.clone().spawn_adapter();
        adm
    }

    fn total_inflight(c: &Core) -> u64 {
        c.inflight.iter().sum()
    }

    fn touch_busy(c: &mut Core) {
        let now = Instant::now();
        let dt = now.duration_since(c.busy_touched).as_nanos();
        for l in 0..LANES {
            c.busy_ns[l] += c.inflight[l] as u128 * dt;
        }
        c.busy_touched = now;
    }

    fn register(self: &Arc<Self>, c: &mut Core, lane: Lane) -> u64 {
        Self::touch_busy(c);
        let id = c.next_id;
        c.next_id += 1;
        c.inflight[lane as usize] += 1;
        c.slots.insert(id, (lane, Instant::now()));
        id
    }

    /// Blocking admission. Fair within a lane (FIFO); across lanes the wake
    /// policy applies once B is exhausted.
    pub async fn acquire(self: &Arc<Self>, lane: Lane) -> Slot {
        let rx = {
            let mut c = self.core.lock().unwrap();
            let under_b = Self::total_inflight(&c) < c.budget as u64;
            let under_cap = (c.inflight[lane as usize] as f64) < c.cap[lane as usize];
            if under_b && under_cap {
                let id = self.register(&mut c, lane);
                return Slot { adm: self.clone(), id, lane, done: false };
            }
            if under_cap {
                c.saturated = true; // refused by B, not by the lane's own cap
            }
            c.lane_saturated[lane as usize] = true;
            let (tx, rx) = oneshot::channel();
            c.waiting[lane as usize].push_back(Waiter { lane, tx });
            rx
        };
        match rx.await {
            Ok(slot) => slot,
            Err(_) => {
                // Granter dropped the sender without granting (shutdown-ish):
                // degrade to a direct registration so the caller never wedges.
                let mut c = self.core.lock().unwrap();
                let id = self.register(&mut c, lane);
                Slot { adm: self.clone(), id, lane, done: false }
            }
        }
    }

    /// Non-blocking probe — the push fire-on-idle path: a single push must
    /// commit in ~1 DB RTT, so availability has to be a synchronous check.
    pub fn try_acquire(self: &Arc<Self>, lane: Lane) -> Option<Slot> {
        let mut c = self.core.lock().unwrap();
        let under_b = Self::total_inflight(&c) < c.budget as u64;
        let under_cap = (c.inflight[lane as usize] as f64) < c.cap[lane as usize];
        if under_b && under_cap {
            let id = self.register(&mut c, lane);
            Some(Slot { adm: self.clone(), id, lane, done: false })
        } else {
            if under_cap {
                c.saturated = true;
            }
            c.lane_saturated[lane as usize] = true;
            None
        }
    }

    /// Hand released capacity to the neediest waiters: first lanes below their
    /// guaranteed share (in WAKE_ORDER), then any waiter in WAKE_ORDER. Every
    /// lane's guarantee floors at ONE slot — a 5% share of a budget of 8 must
    /// still not starve maintenance behind a permanent ack/pop queue.
    ///
    /// Returns the slots whose receiver vanished before the send: the caller
    /// MUST drop them OUTSIDE the core lock (their Drop re-enters it).
    #[must_use]
    fn wake_next(adm: &Arc<Admission>, c: &mut Core) -> Vec<Slot> {
        let mut failed: Vec<Slot> = Vec::new();
        let b = c.budget;
        let pick = |c: &Core| -> Option<Lane> {
            let open = |c: &Core, l: Lane| {
                !c.waiting[l as usize].is_empty()
                    && (c.inflight[l as usize] as f64) < c.cap[l as usize]
            };
            for &l in WAKE_ORDER.iter() {
                let guaranteed = ((b * adm.cfg.share[l as usize]).floor() as u64).max(1);
                if open(c, l) && c.inflight[l as usize] < guaranteed {
                    return Some(l);
                }
            }
            WAKE_ORDER.iter().copied().find(|&l| open(c, *&l))
        };
        while Self::total_inflight(c) < c.budget as u64 {
            let Some(lane) = pick(c) else { break };
            let w = c.waiting[lane as usize].pop_front().unwrap();
            let id = adm.register(c, w.lane);
            let slot = Slot { adm: adm.clone(), id, lane: w.lane, done: false };
            if let Err(slot_back) = w.tx.send(slot) {
                // Receiver already gone (request cancelled before the grant):
                // hand the constructed slot back to the caller for an
                // out-of-lock drop, which releases and re-offers the capacity.
                failed.push(slot_back);
            }
        }
        failed
    }

    fn spawn_adapter(self: Arc<Self>) {
        let tick = self.cfg.tick;
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(tick);
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                iv.tick().await;
                self.adapt();
            }
        });
    }

    /// Per-lane marginal-throughput controller, one step per tick. A lane
    /// presses its cap -> probe one step up; next tick, keep the raise only if
    /// lane throughput grew or completions-per-busy-slot held (>=90%);
    /// otherwise revert and back off. This is what finds the pop lane's
    /// concurrency sweet spot on sparse lanes (negative returns beyond ~8,
    /// measured 2026-08-03) without hardcoding it, and lets the same lane
    /// climb in dense regimes where returns are positive.
    fn adapt_lanes(self: &Arc<Self>, c: &mut Core, now: Instant, ceiling: f64) {
        Self::touch_busy(c);
        let dt = now.duration_since(c.last_adapt).as_secs_f64();
        if dt < LANE_MIN_DT_S {
            return;
        }
        c.last_adapt = now;
        let b = c.budget;
        for l in 0..LANES {
            let busy = std::mem::take(&mut c.busy_ns[l]) as f64 / 1e9;
            let done = std::mem::take(&mut c.done_count[l]) as f64;
            let sat = std::mem::take(&mut c.lane_saturated[l]);
            let avg_if = busy / dt;
            let t_rate = done / dt;
            let eff = t_rate / avg_if.max(LANE_EFF_MIN_BUSY);
            // The wake-order guarantee protects a lane from starvation; it is
            // NOT a floor on the cap — the pop lane's optimum can sit well
            // below its share (sparse-lane regime), and pinning the cap at the
            // share would force the contention this controller exists to avoid.
            let gmin = LANE_MIN_CAP;
            let _ = b;
            if done >= LANE_JUDGE_MIN_DONE && avg_if >= LANE_PRESS_SLACK {
                c.lane_eff_best[l] = (c.lane_eff_best[l] * LANE_EFF_BEST_DECAY).max(eff);
            }
            if c.lane_backoff[l] > 0 {
                c.lane_backoff[l] -= 1;
            } else {
                if c.lane_probe[l] {
                    // Judge the probe on MARGINAL THROUGHPUT alone: keep the
                    // raise only if the lane's completion rate strictly grew
                    // (>=5%, above tick noise at thousands of samples). An
                    // efficiency-tolerance arm was tried first and is wrong:
                    // while a backlog drains, extra slots always buy a little
                    // throughput at halved per-slot efficiency, so the lane
                    // ratchets into the contention regime (measured arbiter5:
                    // pop cap crept to 16, p99_pop 64 ms). Under this criterion
                    // a demand-bound lane's marginal gain is zero by
                    // construction once service meets arrival, so the cap
                    // settles at the smallest value that serves the demand.
                    let ok = t_rate >= c.lane_t_before[l] * LANE_PROBE_ACCEPT_GAIN;
                    if !ok {
                        c.cap[l] -= c.lane_raise[l];
                        c.lane_backoff[l] = LANE_PROBE_BACKOFF_TICKS;
                        if self.cfg.trace {
                            tracing::info!(target: "admission",
                                lane = Lane::ALL[l].name(),
                                cap = c.cap[l] as u64,
                                t_rate = format!("{:.0}", t_rate),
                                eff = format!("{:.1}", eff),
                                "lane probe reverted");
                        }
                    }
                    c.lane_probe[l] = false;
                }
                // Efficiency-memory decay: a busy lane earning well under its
                // own best completions-per-slot is contending (pop on sparse
                // lanes ratchets to cap 16 at ~60% best eff without this) —
                // walk the cap down and let the probes re-earn it if returns
                // ever turn positive again.
                if c.lane_backoff[l] == 0
                    && !c.lane_probe[l]
                    && done >= LANE_JUDGE_MIN_DONE
                    && avg_if + LANE_PRESS_SLACK >= c.cap[l]
                    && c.lane_eff_best[l] > 0.0
                    && eff < c.lane_eff_best[l] * LANE_DECAY_EFF_FRAC
                {
                    c.cap[l] = (c.cap[l] - (c.cap[l] * LANE_DECAY_STEP_FRAC).max(1.0)).max(gmin);
                    c.lane_backoff[l] = LANE_DECAY_BACKOFF_TICKS;
                    if self.cfg.trace {
                        tracing::info!(target: "admission",
                            lane = Lane::ALL[l].name(),
                            cap = c.cap[l] as u64,
                            eff = format!("{:.1}", eff),
                            eff_best = format!("{:.1}", c.lane_eff_best[l]),
                            "lane cap decayed (efficiency far below best)");
                    }
                }
                // Press detection: saturated this tick with real usage and a
                // sample base worth judging. Never re-probe in the same tick
                // as a revert (the backoff must actually hold the lane still).
                if c.lane_backoff[l] == 0
                    && sat
                    && avg_if + LANE_PRESS_SLACK >= c.cap[l]
                    && done >= LANE_JUDGE_MIN_DONE
                {
                    c.lane_t_before[l] = t_rate;
                    c.lane_e_before[l] = eff;
                    let step = (c.cap[l] * LANE_PROBE_STEP_FRAC).max(1.0);
                    c.lane_raise[l] = step;
                    c.cap[l] += step;
                    c.lane_probe[l] = true;
                }
            }
            c.cap[l] = c.cap[l].clamp(gmin, ceiling);
        }
    }

    /// One AIMD step. Kept synchronous and lock-short; called from the adapter
    /// task and directly from tests.
    fn adapt(self: &Arc<Self>) {
        let mut c = self.core.lock().unwrap();
        let ceiling = self.cfg.ceiling() as f64;
        let floor = self.cfg.min as f64;
        let saturated = std::mem::take(&mut c.saturated);
        let now = Instant::now();
        self.adapt_lanes(&mut c, now, ceiling);

        // Signal health: no commit under the nosync floor in the window means
        // flush waits are observable; otherwise the sensor is blind.
        let nosync_now = match c.trains.rtt_p10() {
            Some(f) => f < NOSYNC_FLOOR,
            None => false, // no traffic: hold
        };
        if nosync_now {
            c.nosync_ticks += 1;
        } else {
            c.nosync_ticks = 0;
        }
        let nosync = c.nosync_ticks >= NOSYNC_HYSTERESIS_TICKS;
        if nosync {
            if c.mode != Mode::NoSync {
                c.mode = Mode::NoSync;
                c.last_change = "nosync-detected";
                tracing::info!(target: "admission",
                    budget = self.cfg.nosync_budget,
                    "commit waits below observability floor: pinning static budget");
            }
            c.budget = (self.cfg.nosync_budget as f64).clamp(floor, ceiling);
            drop(c);
            return;
        }
        if c.mode == Mode::NoSync {
            c.mode = Mode::Normal;
            c.last_change = "signal-restored";
        }

        let Some((cycle_p50, _cycle_floor)) = c.trains.cycle() else {
            // Not enough trains to judge: hold.
            return;
        };
        // Maint slots are held across whole stepped cycles by design
        // (retention); they are budget, not queueing — the control error only
        // looks at the live lanes.
        let oldest_age = c
            .slots
            .values()
            .find(|&&(l, _)| l != Lane::Maint)
            .map(|&(_, t)| now.duration_since(t))
            .unwrap_or(Duration::ZERO);

        let old = c.budget;
        // Shrink ONLY on real queueing: an admitted transaction that has waited
        // several flush cycles. Cycle stretching alone is NOT a shrink signal —
        // bigger budget grows the trains and every flush carries more, so the
        // cycle widens exactly when amortization is working (first arbiter
        // build shrank on it and pinned the budget below the workload — the
        // same category of mistake the Vegas estimator made with per-op RTT).
        let queueing =
            oldest_age > cycle_p50.mul_f64(QUEUE_CYCLES) && oldest_age > QUEUE_AGE_MIN;
        let (next, reason) = if queueing {
            (old - old.ln().max(1.0), "oldest-age")
        } else if saturated {
            // Assertive on a cold start (multiplicative while clearly under),
            // additive near the edge via the ln term.
            (old + (old * GROW_MULT).max(old.ln()).max(1.0), "saturated")
        } else {
            (old, "hold")
        };
        c.budget = next.clamp(floor, ceiling);
        if (c.budget - old).abs() >= 1.0 {
            c.last_change = reason;
            let (tpt, _) = c.trains.txn_per_train();
            if self.cfg.trace {
                tracing::info!(target: "admission",
                    budget = c.budget as u64, prev = old as u64, reason,
                    cycle_ms = cycle_p50.as_secs_f64() * 1e3,
                    
                    oldest_ms = oldest_age.as_secs_f64() * 1e3,
                    txn_per_train = tpt,
                    "budget adjusted");
            }
            if c.budget > old {
                let failed = Self::wake_next(self, &mut c);
                drop(c);
                drop(failed);
                return;
            }
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let c = self.core.lock().unwrap();
        let (tpt_avg, tpt_p95) = c.trains.txn_per_train();
        let now = Instant::now();
        Snapshot {
            budget: c.budget as u64,
            mode: c.mode,
            last_change: c.last_change,
            inflight: c.inflight,
            cap: [
                c.cap[0] as u64,
                c.cap[1] as u64,
                c.cap[2] as u64,
                c.cap[3] as u64,
            ],
            waiting: [
                c.waiting[0].len() as u64,
                c.waiting[1].len() as u64,
                c.waiting[2].len() as u64,
                c.waiting[3].len() as u64,
            ],
            trains_per_s: c.trains.trains_per_s(),
            txn_per_train_avg: tpt_avg,
            txn_per_train_p95: tpt_p95,
            cycle_ms: c.trains.cycle().map(|(p50, _)| p50.as_secs_f64() * 1e3),
            oldest_wait_ms: c
                .slots
                .values()
                .find(|&&(l, _)| l != Lane::Maint)
                .map(|&(_, t)| now.duration_since(t).as_secs_f64() * 1e3),
            completions: c.trains.completions,
        }
    }
}

/// Process-global arbiter handle for the long-tail write wrappers (db.rs ack /
/// renew / transaction-wire / streams-cycle, maintenance loops, spool drain).
/// The hot paths (push bundles, pop claims, fused pops) hold explicit slots
/// from their own handles; everything else reaches the SAME arbiter here so
/// "every write transaction passes admission" is a choke-point invariant, not
/// a per-call-site convention. Unset (unit tests) means unmetered.
static GLOBAL: std::sync::OnceLock<Arc<Admission>> = std::sync::OnceLock::new();

pub fn set_global(adm: Arc<Admission>) {
    let _ = GLOBAL.set(adm);
}

/// Acquire a lane slot from the global arbiter; None when no arbiter is
/// installed (tests). Callers hold the Option across their statement and call
/// commit_done on success. ORDERING CONTRACT: acquire the slot BEFORE checking
/// out a pool connection — the reverse order deadlocks against every other
/// admitted path (slot holders parked in pool.get vs connection holders parked
/// here).
pub async fn lane_slot(lane: Lane) -> Option<Slot> {
    match GLOBAL.get() {
        Some(a) => Some(a.acquire(lane).await),
        None => None,
    }
}

/// Feed the train sensor with a commit completion WITHOUT taking admission —
/// for the db.rs write wrappers whose caller already holds both the slot (at
/// handler level) or where the write rides a connection the handler checked
/// out. Never blocks.
pub fn note_commit(lane: Lane, sql_rtt: Duration) {
    if let Some(a) = GLOBAL.get() {
        let mut c = a.core.lock().unwrap();
        let gap = a.cfg.train_gap;
        c.done_count[lane as usize] += 1;
        c.trains.feed(Instant::now(), sql_rtt, gap);
    }
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub budget: u64,
    pub mode: Mode,
    pub last_change: &'static str,
    pub inflight: [u64; LANES],
    pub cap: [u64; LANES],
    pub waiting: [u64; LANES],
    pub trains_per_s: f64,
    pub txn_per_train_avg: f64,
    pub txn_per_train_p95: u32,
    pub cycle_ms: Option<f64>,
    pub oldest_wait_ms: Option<f64>,
    pub completions: u64,
}

impl Snapshot {
    pub fn mode_str(&self) -> &'static str {
        match self.mode {
            Mode::Normal => "normal",
            Mode::NoSync => "nosync",
        }
    }
    /// Compact per-lane "in/wait" figure for the rates log line, e.g.
    /// "push:3w0 pop:7w2 ack:1w0 maint:0w0".
    pub fn lanes_str(&self) -> String {
        Lane::ALL
            .iter()
            .map(|&l| {
                format!("{}:{}/{}w{}", l.name(),
                    self.inflight[l as usize], self.cap[l as usize],
                    self.waiting[l as usize])
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> AdmissionCfg {
        AdmissionCfg {
            init: 4,
            min: 2,
            max: 64,
            pool_reserve: 4,
            pool_size: 32,
            train_gap: Duration::from_micros(300),
            tick: Duration::from_millis(500),
            share: [0.25, 0.40, 0.30, 0.05],
            nosync_budget: 16,
            trace: false,
        }
    }

    #[test]
    fn trains_cluster_and_cycle() {
        let mut ts = TrainStats::default();
        let t0 = Instant::now();
        let gap = Duration::from_micros(300);
        // Three trains of 3 completions each, 5 ms apart.
        for train in 0..3u64 {
            let start = t0 + Duration::from_millis(5 * train);
            for i in 0..3u64 {
                ts.feed(start + Duration::from_micros(100 * i), Duration::from_millis(2), gap);
            }
        }
        // Close the last train by feeding one more far away.
        ts.feed(t0 + Duration::from_millis(50), Duration::from_millis(2), gap);
        let (avg, _) = ts.txn_per_train();
        assert!((avg - 3.0).abs() < 0.01, "avg train size {avg}");
        let (p50, min) = ts.cycle().expect("cycle");
        assert_eq!(p50, Duration::from_millis(5));
        assert_eq!(min, Duration::from_millis(5));
    }

    #[tokio::test]
    async fn slot_drop_never_leaks_inflight() {
        let adm = Admission::new(cfg());
        {
            let _a = adm.acquire(Lane::Pop).await;
            let _b = adm.try_acquire(Lane::Push).expect("free slot");
            let c = adm.core.lock().unwrap();
            assert_eq!(Admission::total_inflight(&c), 2);
        }
        let c = adm.core.lock().unwrap();
        assert_eq!(Admission::total_inflight(&c), 0);
        assert!(c.slots.is_empty());
    }

    #[tokio::test]
    async fn budget_exhaustion_blocks_and_release_wakes_by_priority() {
        let mut c = cfg();
        c.init = 2;
        c.min = 2;
        let adm = Admission::new(c);
        let s1 = adm.acquire(Lane::Push).await;
        let _s2 = adm.acquire(Lane::Push).await;
        assert!(adm.try_acquire(Lane::Push).is_none(), "budget exhausted");
        // Queue an ack waiter and a push waiter; the ack must be woken first.
        let adm_a = adm.clone();
        let ack = tokio::spawn(async move { adm_a.acquire(Lane::Ack).await });
        // Ensure the ack waiter enqueues before the push waiter.
        tokio::task::yield_now().await;
        let adm_p = adm.clone();
        let push = tokio::spawn(async move { adm_p.acquire(Lane::Push).await });
        tokio::task::yield_now().await;
        drop(s1);
        let woken = ack.await.unwrap();
        assert_eq!(woken.lane(), Lane::Ack);
        drop(woken);
        let w2 = push.await.unwrap();
        assert_eq!(w2.lane(), Lane::Push);
    }

    #[tokio::test]
    async fn ceiling_respects_pool_reserve() {
        let mut c = cfg();
        c.max = 1000;
        c.pool_size = 10;
        c.pool_reserve = 4;
        assert_eq!(c.ceiling(), 6);
        let adm = Admission::new(c);
        let ca = adm.core.lock().unwrap();
        assert!(ca.budget <= 6.0);
    }

    #[tokio::test]
    async fn adapter_grows_when_saturated_and_shrinks_on_oldest_age() {
        let adm = Admission::new(cfg());
        let t0 = Instant::now();
        {
            let mut c = adm.core.lock().unwrap();
            let gap = Duration::from_micros(300);
            for k in 0..6u64 {
                c.trains
                    .feed(t0 + Duration::from_millis(2 * k), Duration::from_millis(1), gap);
                c.trains.feed(
                    t0 + Duration::from_millis(2 * k) + Duration::from_micros(100),
                    Duration::from_millis(1),
                    gap,
                );
            }
            c.saturated = true;
        }
        let before = adm.core.lock().unwrap().budget;
        adm.adapt();
        let grown = adm.core.lock().unwrap().budget;
        assert!(grown > before, "saturated must grow ({before} -> {grown})");

        {
            // An admitted live-lane slot stuck for many cycles = real queueing.
            let mut c = adm.core.lock().unwrap();
            let stuck = Instant::now() - Duration::from_millis(200);
            let id = c.next_id;
            c.next_id += 1;
            c.inflight[Lane::Pop as usize] += 1;
            c.slots.insert(id, (Lane::Pop, stuck));
        }
        adm.adapt();
        let shrunk = adm.core.lock().unwrap().budget;
        assert!(shrunk < grown, "oldest-age must shrink ({grown} -> {shrunk})");
        {
            // Maint slots must NOT count as queueing: clear the stuck pop slot,
            // plant an ancient maint slot, saturate — the budget must grow.
            let mut c = adm.core.lock().unwrap();
            c.slots.clear();
            c.inflight = [0; LANES];
            let ancient = Instant::now() - Duration::from_secs(30);
            let id = c.next_id;
            c.next_id += 1;
            c.inflight[Lane::Maint as usize] += 1;
            c.slots.insert(id, (Lane::Maint, ancient));
            c.saturated = true;
        }
        adm.adapt();
        let regrown = adm.core.lock().unwrap().budget;
        assert!(regrown > shrunk, "maint age must not block growth ({shrunk} -> {regrown})");
    }

    #[tokio::test]
    async fn lane_probe_reverts_on_negative_returns() {
        let mut cf = cfg();
        cf.init = 32;
        cf.max = 64;
        cf.pool_size = 128;
        let adm = Admission::new(cf);
        let pop = Lane::Pop as usize;
        // Tick 1: pop lane pressing its cap of 8 with healthy efficiency.
        {
            let mut c = adm.core.lock().unwrap();
            c.cap[pop] = 8.0;
            c.last_adapt = Instant::now() - Duration::from_secs(1);
            c.busy_touched = Instant::now();
            c.busy_ns[pop] = 8 * 1_000_000_000u128;
            c.done_count[pop] = 800;
            c.lane_saturated[pop] = true;
        }
        adm.adapt();
        let probed = adm.core.lock().unwrap().cap[pop];
        assert!(probed > 8.0, "pressed lane must probe up ({probed})");
        // Tick 2: the raise bought nothing — throughput flat, efficiency down.
        {
            let mut c = adm.core.lock().unwrap();
            c.last_adapt = Instant::now() - Duration::from_secs(1);
            c.busy_touched = Instant::now();
            c.busy_ns[pop] = 9 * 1_000_000_000u128;
            c.done_count[pop] = 780;
            c.lane_saturated[pop] = true;
        }
        adm.adapt();
        let reverted = adm.core.lock().unwrap().cap[pop];
        assert!(
            (reverted - 8.0).abs() < 0.01,
            "negative-returns probe must revert ({probed} -> {reverted})"
        );
        assert!(adm.core.lock().unwrap().lane_backoff[pop] > 0, "backoff armed");
    }

    #[tokio::test]
    async fn nosync_mode_pins_static_budget() {
        let adm = Admission::new(cfg());
        {
            let mut c = adm.core.lock().unwrap();
            let gap = Duration::from_micros(300);
            let t0 = Instant::now();
            for k in 0..8u64 {
                // Sub-floor commit rtts: async commit signature.
                c.trains.feed(
                    t0 + Duration::from_millis(k),
                    Duration::from_micros(50),
                    gap,
                );
            }
        }
        // Hysteresis: the mode flips only after three consecutive ticks.
        adm.adapt();
        assert_eq!(adm.core.lock().unwrap().mode, Mode::Normal);
        adm.adapt();
        adm.adapt();
        let c = adm.core.lock().unwrap();
        assert_eq!(c.mode, Mode::NoSync);
        assert_eq!(c.budget as u64, 16);
    }
}
