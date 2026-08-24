//! POP AUTOPILOT — the broker chooses the claim width (`partitions`, W) and the
//! batch (B) for a grouped wildcard pop, per (tenant, queue, group).
//!
//! ---------------------------------------------------------------------------
//! WHY THE SERVER HAS TO BE THE ONE THAT CHOOSES
//!
//! `partitions` is the single largest latency lever the pop wire exposes and its
//! correct value is a fact about the BROKER's state, not the consumer's. Measured
//! on the cloud soak cell (2026-08-22): the same workload at `partitions=5000`
//! ran `ready_age_p95` at 2376 ms with pop accounting for 47% of all PostgreSQL
//! time; at `partitions=10` the same cell served it at 5 ms — a 475x swing on a
//! knob the SDK user picked once, from a laptop, before the queue had any
//! partitions at all. And it does not stay picked: a tenant's partition count
//! grows for years, so a value that was right on the day it was typed is wrong by
//! construction later.
//!
//! The controller is therefore server-side, in-memory, and re-parameterizes the
//! claim the pop was already going to make. It adds ZERO PostgreSQL queries and
//! zero writes anywhere on the pop path — every input it reads is already in
//! broker RAM (see `HotList`, `LapStats`).
//!
//! ---------------------------------------------------------------------------
//! WHY IT IS OPT-IN PER DIMENSION, AND WHY THE OPT-IN IS A NEW PARAMETER
//!
//! Queen's contract is full manual control: an explicit client value is never
//! overridden, ever. The obvious encoding — "field absent ⇒ the broker decides" —
//! is UNAVAILABLE here, and that is not a stylistic preference. `partitions`
//! absent already MEANS 1 and `batch` absent already MEANS 200, and today's SDKs
//! omit both at their defaults (the Go SDK only sends `partitions` when > 1). So
//! "absent ⇒ autopilot" would silently change the behaviour of every consumer in
//! the field that never touched the knob. The opt-in is therefore a NEW query
//! parameter, `autopilot=true`, emitted only by a new SDK and only for the
//! dimensions its user left unset — the same shape (and the same reason) as
//! `conflation`: emitted ONLY when true, so a consumer that does not opt in sends
//! the request it sent before this option existed, byte for byte.
//!
//! Per-dimension independence follows: `autopilot=true&partitions=1` is a MANUAL
//! width of 1 with an automatic batch. A dimension the client sent is the client's,
//! full stop, and the controller does not even look at it (except to note the
//! divergence, see below).
//!
//! ---------------------------------------------------------------------------
//! THE LAW
//!
//! W is a feedback loop on READY-AGE, the arbiter's own variable (`admission.rs`
//! v5, "the pop lane's objective is the measured wait of the ready ring"), and it
//! shares the arbiter's samples rather than measuring the same thing twice: the
//! per-candidate `now - ready_since` that `HotList::take_batch` already computes
//! for `LapStats` rides back on `Candidate::ready_age_ms`.
//!
//!   * WIDEN when the ready-age EWMA is above target: `W <- min(2W, ready, 64)`.
//!     Keyed on age ALONE — deliberately not on "was the batch full", because at
//!     B=200 a full batch is rare on the sparse shapes that need widening most,
//!     and a full-batch precondition would mean the loop never fires there.
//!   * SHRINK when the age EWMA is well under target (< target/4) AND the fill
//!     ratio is partial: `W <- max(1, W/2)`. Both conditions, because a low age
//!     with FULL batches is a lane doing exactly what it should.
//!   * HOLD otherwise. The dead zone is what stops the MIMD pair from
//!     limit-cycling, and the DWELL (at most one change per `dwell_ms` or per
//!     `dwell_pops` pops, whichever comes first, in BOTH directions) is what
//!     bounds the cycle when the signal does straddle the dead zone.
//!
//! ---------------------------------------------------------------------------
//! THE READY-PRESSURE CORRECTION (bench cell A/B, 2026-08-23) — READ THIS BEFORE
//! TOUCHING `LaneState::step`
//!
//! The law above, with ready-age as the SOLE widening driver, was measured and it
//! LOST. Clean steady-state interval, same 1060 msg/s both sides:
//!
//!   baseline  (W=10, B=100)          p50 13.5 ms, p99 112 ms, CPU ~78%
//!   autopilot (converged W=1, B=200) p50 15 ms,   p99 380 ms, CPU ~65%
//!
//! A 3.4x worse tail, with `cands_visit=1.1` per pop and ready depth oscillating
//! 28 -> 4 -> 1: bursts made ~28 partitions ready and a width of 1 drained them
//! one pop at a time.
//!
//! The cause is a SAMPLING BIAS that no amount of tuning fixes, because the
//! signal structurally cannot see the thing it is supposed to protect. Ready-age
//! samples are taken at claim time, on the candidates the sweep VISITS. At W=1
//! the only partition ever visited is the promptly-served head, so the EWMA
//! pinned at ~5 ms; the partitions sitting UNVISITED at the back of the ring —
//! the entire problem — contributed only rare large samples, which an alpha of
//! 0.25 drowns. The controller read "age is fine" and held W=1 while the tail
//! starved.
//!
//! So the SMOOTHED READY COUNT, which the first version used only as a cap, is
//! now the PRIMARY WIDENING DRIVER: it is the one signal that is measured
//! independently of what the sweep chose to look at. `step` evaluates, in order:
//! ready pressure (widen), age (widen, secondary), then shrink — and the shrink
//! arm gained a ready guard so the two drivers cannot fight. The result is a
//! stable band: widen while `W < ready`, shrink only once `W > 2 x ready`, hold
//! in between — no tug-of-war by construction.
//!
//! Widening on ready pressure SNAPS to the smoothed ready count rather than
//! doubling towards it. Doubling alone from W=1 needs ~5 dwell periods, and the
//! backlog drains at width 1 for every one of them — which is precisely the
//! measured failure. `ready` is itself an EWMA, so this is a step to a smoothed
//! target and not a jump onto an instantaneous spike.
//!
//! The feed-forward start is a DIVISION, not a probe: on a cold lane, and after a
//! lane has been idle, `W ~= ready / max(1, M)` where M is the count of this
//! lane's pops currently live in this broker. That is the only place worker
//! COUNT enters, and it enters as an initial condition the feedback loop then
//! corrects — the controller never models worker health, never reads
//! `queen.log_consumers`, and never asks the database anything.
//!
//! The caps, in order: the user's own value (which turns the controller OFF for
//! that dimension), then 64, and nothing else — the smoothed ready count is the
//! TARGET now, not a ceiling, which is the other half of the correction above. 64
//! is the measured checkout ceiling and is load-bearing for the reason spelled
//! out at `k` in `hotlist_pop_attempt` — a claim's partitions are held INFLIGHT
//! for the CLIENT's whole cycle, so a wide checkout strips the ready ring for
//! hundreds of ms however cheap the SQL leg is.
//!
//! B is deliberately left at today's default (200) when it is automatic.
//! Over-asking is free — the claim returns what exists, and the first pop after
//! an idle period doubles as the backlog probe — and B's latency premise was
//! refuted empirically: the e2e tail on the soak cell turned out to be PostgreSQL
//! checkpoint-driven, not batch-size-driven. W is the deliverable; B is echoed so
//! the SDK can see what it got, and nothing more.
//!
//! ---------------------------------------------------------------------------
//! WHAT IT COSTS WHEN NOBODY OPTS IN
//!
//! The EWMAs are maintained for EVERY grouped pop on the hot-list path, not only
//! for opted-in ones, because the divergence log (below) is about the clients that
//! did NOT opt in — they are the ones carrying the accidental `partitions=5000`.
//! That bookkeeping is: one nested-map lookup borrowing (qkey, group) with no
//! allocation, one uncontended mutex, and a handful of f64 updates, once per
//! REQUEST (never per poll iteration, and never on the quiet empty-re-poll
//! short-circuit, which is the O(#queues) storm the discovery-latency fix exists
//! to keep free). `QUEEN_POP_AUTOPILOT=off` removes even that.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// The measured checkout ceiling (see `hotlist_pop_attempt`'s `k`). No autopilot
/// decision may exceed it, whatever the ring says.
pub const W_CEILING: i32 = 64;

/// A lane with no claim for this long re-runs the feed-forward division instead
/// of continuing from its stored W: a burst that arrives after a quiet period
/// must not be met with the width that fit the LAST burst. One second is one full
/// long-poll backoff cycle at the default `POP_WAIT_MAX_INTERVAL_MS`.
const IDLE_MS: i64 = 1_000;

/// EWMA weights. Deliberately heavy on history: every input here is a per-claim
/// sample of a queue whose arrival process is bursty by nature, and the whole
/// point of smoothing the ready count is that the INSTANTANEOUS one is zero right
/// after a claim took everything INFLIGHT.
const ALPHA_READY: f64 = 0.25;
const ALPHA_AGE: f64 = 0.25;
const ALPHA_FILL: f64 = 0.25;

/// Shrink needs the age to be well under target, not merely under it: at exactly
/// the target the loop must hold, or it oscillates across the boundary.
const SHRINK_AGE_FRAC: f64 = 0.25;

/// "Fill ratio partial" — a claim that delivered less than half of what it asked
/// for. Above this the lane is feeding its consumer as fast as it asks, and
/// narrowing it would only add round trips.
const FILL_PARTIAL: f64 = 0.5;

/// The batch the controller picks when — and ONLY when — the client delegated
/// that dimension. `QUEEN_POP_AUTOPILOT_BATCH` overrides it.
///
/// 100, not the 200 an absent `batch` field resolves to in the handler, and the
/// difference between those two numbers is worth stating plainly: 200 was never
/// a fleet reality. Every SDK has defaulted to 100 client-side, so 100 is what
/// the fleet has actually been running for years; the server's 200 is what an
/// absent field happens to mean, which before this feature no real consumer ever
/// exercised. Delegating a knob must not silently double it.
///
/// Then the rig priced it. Matched-state arms on the same cell, same 43M+
/// segments, matched checkpoint intensity (raw/2026-08-24-baseline-matched/):
/// `cands_visit` was 1.0 in BOTH arms — per-lane ready never had 10 partitions to
/// visit, so both swept one partition per pop and B was the ONLY live variable.
///
///   B=100   p99 147.3 ms   worst interval 155 ms   p50 15.9 ms
///   B=200   p99 199.3 ms   worst intervals 393 / 440 ms   p50 14.1 ms
///
/// A bigger batch wins the median slightly and loses the tail badly (+35% p99),
/// by the mechanism the v1 risk note already named: a deep checkout on a
/// backlogged partition is held INFLIGHT for the client's whole serial drain, so
/// the tail is `claim_depth / consumer drain rate`. Same argument as the 64-wide
/// checkout ceiling at `k` in `hotlist_pop_attempt`, one dimension over.
///
/// THE FOLLOW-UP THIS IS NOT (deliberately, for tonight): a constant is a
/// placeholder for a measurement. The right B is DRAIN-AWARE — bound it by the
/// lane's own consumer drain rate times a drain-time budget, `B* ~= drain_rate x
/// budget_ms`, so a fast consumer gets deep batches and a slow one is never
/// handed more than it can retire inside the tail budget. The broker already has
/// the input and needs no new measurement or database traffic: the ack registry
/// timestamps a lease at delivery and sees its acks arrive, so messages retired
/// per second per (queue, group) is derivable from state the ack path already
/// keeps. That is the next slice; this constant is the interim.
pub const AUTO_BATCH_DEFAULT: i32 = 100;

/// Bounds on the knob above. 1 is the floor the whole pop path already assumes
/// (`batch.max(1)` at every claim site); 10_000 is far above any batch the cell
/// has ever measured as useful and exists so a fat-fingered
/// `QUEEN_POP_AUTOPILOT_BATCH=1000000` fails the boot instead of handing every
/// delegating consumer a claim it cannot drain.
pub const AUTO_BATCH_MIN: i32 = 1;
pub const AUTO_BATCH_MAX: i32 = 10_000;

/// A divergence line repeats for the same lane only after this long (24h): it is
/// a diagnostic about a client's static configuration, and a static
/// configuration does not need saying twice a day.
const DIVERGENCE_REARM_MS: i64 = 24 * 60 * 60 * 1000;

/// The divergence threshold, |log2(user / W*)| >= 3, expressed as the integer
/// ratio the code actually compares.
const DIVERGENCE_RATIO: i32 = 8;

/// Windowed reporter cadences, obs.rs discipline: aggregated per window, never
/// one line per request. The decision line carries the LAST decision plus the
/// window's counts; the divergence line is gated far more slowly because it is a
/// per-lane fact and a mismatched fleet must not turn it into a flood — the
/// `note_conflation_conflict` idiom.
///
/// The gates are FIELDS of the controller rather than statics: a static gate is
/// shared by every `PopAutopilot` in a process, which makes one test's log
/// suppress another's and (worse) makes the divergence report's behaviour depend
/// on what else the process logged that minute.
const DECISION_LOG_MS: i64 = 10_000;
const DIVERGENCE_LOG_MS: i64 = 60_000;

/// `QUEEN_POP_AUTOPILOT` — a kill switch, not a feature gate (the distinction is
/// `switches.rs`'s module note): the surface exists on every broker, the switch
/// only says what the controller may DO with it.
///
///   * `on` (the default) — the controller acts on requests that opted in.
///   * `shadow` — opted-in requests get today's defaults; the controller computes
///     what it WOULD have chosen and reports it, windowed. This is the rollout
///     position: it proves the law on a real cell before any claim changes shape.
///   * `off` — the parameter is ignored entirely. No decision, no lane state, no
///     logs. Reverts the broker to the day before this file.
///
/// In every one of the three, a request that does NOT carry `autopilot=true` gets
/// byte-identical treatment: same claim parameters, same response bytes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    Off,
    Shadow,
    On,
}

impl Mode {
    /// Case-insensitive, whitespace-trimmed. The boolean spellings are accepted
    /// because an operator reaching for a kill switch at three in the morning
    /// types `=0`, and `config::parse_bool` has taught the whole broker that this
    /// works everywhere. `None` for anything else — the caller makes it fatal
    /// rather than guessing a direction (the `env_bool` rule).
    pub fn parse(raw: &str) -> Option<Mode> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "on" | "true" | "1" | "yes" => Some(Mode::On),
            "shadow" | "dry-run" | "dryrun" => Some(Mode::Shadow),
            "off" | "false" | "0" | "no" => Some(Mode::Off),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Off => "off",
            Mode::Shadow => "shadow",
            Mode::On => "on",
        }
    }
}

/// Tunables. Every one is an env knob parsed in `config.rs` (so the generated
/// environment reference picks it up) and threaded here.
#[derive(Clone, Copy, Debug)]
pub struct Knobs {
    pub mode: Mode,
    /// The ready-age the loop steers to (ms). Above it the lane widens; a quarter
    /// of it is the shrink threshold. 25 ms is one order of magnitude under the
    /// e2e budget and one above the floor a continuously-visited ring measures
    /// (`POP_AGE_FLOOR_MS` in admission.rs is 10 ms for the same reason).
    pub target_age_ms: f64,
    /// The batch handed to a client that DELEGATED that dimension — see
    /// `AUTO_BATCH_DEFAULT` for why it is 100 and not the handler's 200. A client
    /// that sent its own `batch` never reaches this field.
    pub auto_batch: i32,
    /// Dwell: no lane changes W more than once per this many ms …
    pub dwell_ms: i64,
    /// … or once per this many pops, whichever comes FIRST. Both directions.
    pub dwell_pops: u32,
    /// Hard bound on live lanes. There are deployments with ~100k queues, and a
    /// lane is created by any grouped pop — including a pop of a queue that does
    /// not exist. Past the cap the controller simply stops creating lanes (it
    /// never evicts a live one to make room), which degrades exactly to "today's
    /// defaults", the safe direction.
    pub max_lanes: usize,
}

impl Knobs {
    pub fn defaults() -> Knobs {
        Knobs {
            mode: Mode::On,
            target_age_ms: 25.0,
            auto_batch: AUTO_BATCH_DEFAULT,
            dwell_ms: 500,
            dwell_pops: 16,
            max_lanes: 50_000,
        }
    }
}

/// One reading of a lane's ready set, straight from `HotList::ready_peek`.
///
/// The two numbers travel together because they are one measurement: the ring is
/// walked once, under one brief lock per sub-ring, and BOTH are properties of the
/// whole ready set rather than of the partitions a claim happened to visit. That
/// distinction is the entire bench-A/B correction (module note) — a ready-age
/// measured only on visited candidates reports health at width 1 by construction.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReadyPeek {
    /// Partitions ready to be swept right now.
    pub parts: usize,
    /// How long the oldest of them has been ready, in ms. 0 when `parts == 0`,
    /// which reads as "no sample" and not as "age zero".
    pub oldest_ms: i64,
}

/// What the client asked for, plus what today's broker would have done with it.
/// The defaults are passed IN rather than duplicated here: the handler owns them
/// (`p.batch.unwrap_or(200)`, `p.partitions.unwrap_or(1)`, and conflation's
/// `batch.clamp(1, 64)`), and a second copy of that resolution in this file would
/// be a second place to get it wrong.
#[derive(Clone, Copy, Debug)]
pub struct Ask {
    /// The request carried `autopilot=true`.
    pub autopilot: bool,
    /// The client's explicit values, exactly as they arrived.
    pub partitions: Option<i32>,
    pub batch: Option<i32>,
    /// What the claim would use for each dimension without the controller.
    pub default_partitions: i32,
    pub default_batch: i32,
}

/// The controller's answer for one pop. Only ever produced when the controller
/// ACTED — `PopTicket::plan()` is `None` for every request that did not opt in,
/// which is what makes byte-identity structural rather than asserted: the handler
/// has one `match`, and its `None` arm is the pre-autopilot code path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Plan {
    /// Effective values for the claim — the controller's choice on an automatic
    /// dimension, the client's own value on a manual one.
    pub partitions: i32,
    pub batch: i32,
    pub chose_partitions: bool,
    pub chose_batch: bool,
}

impl Plan {
    #[inline]
    pub fn engaged(&self) -> bool {
        self.chose_partitions || self.chose_batch
    }
}

/// Per-(tenant, queue, group) controller state. In memory only: a restart starts
/// cold and re-converges within a few pops, which is cheaper (and far less
/// dangerous) than a durable copy of a value that is only ever an estimate.
struct Lane {
    st: Mutex<LaneState>,
}

#[derive(Debug)]
struct LaneState {
    /// The width the controller currently believes in. 0 = cold, never chosen.
    w: i32,
    /// Smoothed count of READY partitions in this lane's ring. Smoothed and not
    /// instantaneous on purpose: right after a claim the ring is empty of the
    /// partitions that claim took, so the instantaneous number describes the
    /// previous pop, not the lane.
    ready: f64,
    /// Smoothed per-claim ready-age (ms) — the max over each claim, i.e. the
    /// oldest servable partition that claim reached. The tail is the variable the
    /// user feels; the mean hides exactly the lane this feature exists for.
    age: f64,
    /// Smoothed delivered/requested.
    fill: f64,
    /// Set by `note_claim`, consumed by `record`: is the age EWMA backed by a
    /// sample from THIS pop? A pop that claimed nothing carries no age
    /// information, and widening on a stale age would let an idle lane ratchet.
    age_fresh: bool,
    /// Live local pops of this lane (parked + in flight) — M in the division.
    /// Maintained by `PopTicket`'s construction/Drop pair, so a dropped request
    /// future (client disconnect mid-pop) cannot leak one.
    inflight: u32,
    last_adjust_ms: i64,
    pops_since_adjust: u32,
    /// Last claim that actually took candidates (drives the post-idle re-division;
    /// empty long-poll re-polls deliberately do NOT count as activity).
    last_claim_ms: i64,
    /// Last emitted divergence line for this lane (0 = never).
    diverged_ms: i64,
    /// Diagnostics: how many times W actually changed. The anti-flap test reads
    /// this, and it is the number the windowed log reports.
    adjusts: u64,
    /// CLOCK second chance for the idle sweep — a flag, not a timestamp, so the
    /// per-pop cost is one store and never a clock read (`hotlist::QueueState`
    /// precedent).
    touched: bool,
}

impl LaneState {
    fn new(now_ms: i64) -> LaneState {
        LaneState {
            w: 0,
            ready: 0.0,
            age: 0.0,
            fill: 0.0,
            age_fresh: false,
            inflight: 0,
            last_adjust_ms: now_ms,
            pops_since_adjust: 0,
            last_claim_ms: 0,
            diverged_ms: 0,
            adjusts: 0,
            touched: true,
        }
    }

    /// The width this lane would claim with right now, given the caps. Pure, and
    /// the ONE place the caps compose — `plan` uses it for the real answer and the
    /// shadow/divergence paths use it for the counterfactual, so the two can never
    /// disagree about what the controller "would have chosen".
    fn width(&self, now_ms: i64) -> i32 {
        // Cold, or the lane has been quiet long enough that its stored width
        // describes a burst that is over: re-derive by DIVISION. M includes the
        // caller (its ticket incremented `inflight` before this runs), so the
        // divisor is never 0 and a single worker gets the whole ready set.
        if self.w <= 0 || now_ms.saturating_sub(self.last_claim_ms) > IDLE_MS {
            let m = self.inflight.max(1) as f64;
            return clamp_w((self.ready / m).round() as i32);
        }
        // NOTE the absence of a `.min(ready)` here, and see the module note: the
        // smoothed ready count used to cap this, which is exactly how a lane with
        // 28 partitions waiting stayed at width 1. It is the widening TARGET now.
        // The only ceilings left are the user's value (handled by the caller,
        // which never asks the controller about a dimension the client sent) and
        // the checkout ceiling inside `clamp_w`.
        clamp_w(self.w)
    }

    /// The smoothed ready count as a width TARGET (it stopped being a cap — see
    /// the module note on the bench A/B). Rounded up, because a lane holding 1.4
    /// ready partitions on average can usefully claim 2, and clamped to the
    /// checkout ceiling like every other width.
    fn ready_target(&self) -> i32 {
        clamp_w(self.ready.ceil() as i32)
    }

    /// One step of the law, run once per pop from `record` and gated by the
    /// dwell: ready pressure, then age, then shrink, then hold — in that order,
    /// which is the order the bench A/B forced (module note). Returns true when W
    /// actually changed.
    fn step(&mut self, target_age_ms: f64, dwell_ms: i64, dwell_pops: u32, now_ms: i64) -> bool {
        self.pops_since_adjust = self.pops_since_adjust.saturating_add(1);
        // Dwell: "at most one adjustment per dwell_ms OR per dwell_pops pops,
        // whichever comes first" — so the gate is the OR, and only an actual
        // CHANGE re-arms it. Staying in the dead zone must not consume the budget,
        // or a lane that held still for a minute would then be forbidden from
        // reacting for another dwell.
        let due = now_ms.saturating_sub(self.last_adjust_ms) >= dwell_ms
            || self.pops_since_adjust >= dwell_pops;
        // A COLD lane always takes its first step, dwell or no dwell. Until `w` is
        // committed, `width` re-derives the feed-forward division on every single
        // pop, so the claim width free-runs with the raw ready EWMA (measured:
        // 64, 48, 52, 39, 46 on five consecutive pops) for up to a whole dwell
        // window. Committing the division on the first record ends that churn and
        // hands the loop a defined starting point; it costs nothing, because the
        // value committed is the one those pops were already using.
        if !due && self.w > 0 {
            return false;
        }
        let before = self.width(now_ms);
        let ready = self.ready_target();
        let next = if ready > before {
            // 1. READY PRESSURE — the primary driver, and it does NOT consult the
            // age (see the module note: at W=1 the age EWMA is measured on the
            // one partition the sweep visits, so it reports health while the rest
            // of the ring starves). More lanes are servable than this claim can
            // reach: reach them.
            //
            // SNAP to the smoothed count rather than doubling towards it.
            // Doubling from 1 needs ~5 dwell periods to cover a 28-partition
            // burst and the burst drains at width 1 for every one of them, which
            // is the measured failure. `ready` is itself an EWMA, so this is a
            // step to a smoothed target and not a jump onto an instantaneous
            // spike, and progress is guaranteed without a multiplicative floor:
            // this arm's own precondition is `ready > W`, and both are integers.
            //
            // A `max(2W, ready)` floor was tried here and REMOVED: it only ever
            // binds when `ready < 2W`, i.e. when the width is already close, and
            // there it overshoots past the target into the region the shrink arm
            // wants back — a 10 <-> 20 limit cycle under the depletion feedback
            // (`the_width_settles_inside_the_band_when_the_sweep_depletes_the_ready_set`).
            // It contributes nothing to the burst case it was proposed for, where
            // `ready >> 2W` and the snap term wins anyway.
            ready
        } else if self.age_fresh && self.age > target_age_ms {
            // 2. AGE — the secondary widening trigger, kept from the first law.
            // Nearly a no-op while `ready <= W` (arm 1 owns the interesting
            // cases), but harmless and it still covers the residual shape: a
            // small ready set whose partitions are nonetheless waiting too long,
            // e.g. a claim leg slow enough that one lane per sweep is too few.
            // Bounded doubling, never a snap: there is no measured target here.
            (before.saturating_mul(2)).min(W_CEILING)
        } else if self.age_fresh
            && (self.ready * 2.0) < before as f64
            && self.age < target_age_ms * SHRINK_AGE_FRAC
            && self.fill < FILL_PARTIAL
        {
            // 3. SHRINK — three conditions, and the READY GUARD (`ready < W/2`)
            // is what keeps arms 1 and 3 from fighting: widen while W < ready,
            // shrink only once W > 2 x ready, hold in the band between. The pair
            // therefore cannot limit-cycle regardless of what the age does.
            (before / 2).max(1)
        } else {
            // Dead zone: hold. This is the majority arm at steady state and it is
            // the anti-flap mechanism.
            before
        };
        let next = clamp_w(next);
        self.age_fresh = false;
        if next == before && self.w == before {
            return false;
        }
        self.w = next;
        self.last_adjust_ms = now_ms;
        self.pops_since_adjust = 0;
        if next != before {
            self.adjusts = self.adjusts.saturating_add(1);
            return true;
        }
        false
    }

    /// Diagnostic only, and it never feeds back into anything: when a request
    /// carries an EXPLICIT `partitions` that differs from the controller's
    /// counterfactual by 8x or more in either direction, say so ONCE per lane per
    /// re-arm window. `Some(w_star)` is the controller's counterfactual, i.e. the
    /// second number the line has to carry.
    ///
    /// The re-arm stamp is the CALLER's to write, and only when a line actually
    /// comes out: a report the broker-wide flood gate swallowed leaves this lane
    /// armed, so it is retried on the lane's next pop instead of being lost for
    /// the whole 24h window. Two anti-flood layers, and neither may silently eat
    /// the other's report.
    fn divergence(&self, user_w: i32, now_ms: i64) -> Option<i32> {
        // A cold lane has no opinion worth publishing: W* would be the
        // feed-forward guess off an unseeded ready EWMA.
        if self.w <= 0 {
            return None;
        }
        let star = self.width(now_ms).max(1);
        let user = user_w.max(1);
        if user < star.saturating_mul(DIVERGENCE_RATIO)
            && star < user.saturating_mul(DIVERGENCE_RATIO)
        {
            return None;
        }
        if self.diverged_ms != 0 && now_ms.saturating_sub(self.diverged_ms) < DIVERGENCE_REARM_MS {
            return None;
        }
        Some(star)
    }
}

#[inline]
fn clamp_w(w: i32) -> i32 {
    w.clamp(1, W_CEILING)
}

#[inline]
fn ewma(prev: f64, sample: f64, alpha: f64) -> f64 {
    if prev <= 0.0 {
        // Seed on the first observation instead of crawling up from zero: a cold
        // lane that waited for the EWMA to converge would spend its first dozen
        // pops claiming 1 partition on a ring holding hundreds.
        sample
    } else {
        prev + alpha * (sample - prev)
    }
}

pub struct PopAutopilot {
    k: Knobs,
    /// qkey -> group -> lane. Nested exactly like `AppState::seeded_groups`, so
    /// the steady-state hit borrows `(qkey, group)` as `&str` and allocates
    /// nothing; only a lane's creation allocates.
    lanes: Mutex<HashMap<String, HashMap<String, Arc<Lane>>>>,
    live: AtomicUsize,
    requests: AtomicU64,
    adjusts: AtomicU64,
    divergences: AtomicU64,
    /// Lanes not created because `max_lanes` was reached. Non-zero means the
    /// controller is silently off for part of the cell — worth a number.
    refused: AtomicU64,
    /// The two windowed log gates (see DECISION_LOG_MS / DIVERGENCE_LOG_MS).
    decision_log: crate::obs::Sampler,
    divergence_log: crate::obs::Sampler,
}

impl PopAutopilot {
    pub fn new(k: Knobs) -> Arc<PopAutopilot> {
        Arc::new(PopAutopilot {
            k,
            lanes: Mutex::new(HashMap::new()),
            live: AtomicUsize::new(0),
            requests: AtomicU64::new(0),
            adjusts: AtomicU64::new(0),
            divergences: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            decision_log: crate::obs::Sampler::new(DECISION_LOG_MS),
            divergence_log: crate::obs::Sampler::new(DIVERGENCE_LOG_MS),
        })
    }

    /// One relaxed comparison — the `off` lane must cost the pop path nothing but
    /// this.
    #[inline]
    pub fn active(&self) -> bool {
        self.k.mode != Mode::Off
    }

    /// Begin one pop. Returns a ticket the handler keeps for the life of the
    /// request: it carries the decision, receives the claim observations, and
    /// releases this lane's worker slot on Drop (including a dropped future).
    ///
    /// `ready` is the hot list's CURRENT view of this lane's ready set —
    /// `HotList::ready_peek`, one non-creating lock-brief pass returning the
    /// partition COUNT and the age of the OLDEST ready entry. The smoothing lives
    /// here. Both are measured over the whole ready set rather than over what a
    /// claim happened to visit, which is the correction the bench A/B forced (see
    /// the module note).
    pub fn begin(
        self: &Arc<Self>,
        qkey: &str,
        group: &str,
        ask: Ask,
        ready: ReadyPeek,
        now_ms: i64,
    ) -> PopTicket {
        if !self.active() {
            // `off` means off: no lane, no state, no logs, no decision.
            return PopTicket::inert();
        }
        let lane = match self.lane(qkey, group, now_ms) {
            Some(l) => l,
            None => return PopTicket::inert(),
        };
        self.requests.fetch_add(1, Ordering::Relaxed);

        let (plan, divergence) = {
            let mut s = lane.st.lock().unwrap();
            s.touched = true;
            s.inflight = s.inflight.saturating_add(1);
            s.ready = ewma(s.ready, ready.parts as f64, ALPHA_READY);
            // The UNBIASED age sample, and the reason `ready_peek` returns two
            // numbers: this is the oldest partition in the whole ready set,
            // whether or not any claim has looked at it. A ring with nothing
            // ready carries no age information at all — that is "no sample", not
            // "age zero", so the EWMA and the freshness flag are left alone and
            // the age-reading arms of `step` hold.
            if ready.parts > 0 {
                s.age = ewma(s.age, ready.oldest_ms.max(0) as f64, ALPHA_AGE);
                s.age_fresh = true;
            }

            // The counterfactual, computed for every grouped pop on this path —
            // it is what the shadow mode reports and what the divergence log
            // compares against.
            let star = s.width(now_ms);
            // Two anti-flood layers, in this ORDER on purpose: the lane's 24h
            // re-arm is stamped only once the broker-wide gate has granted the
            // slot, so a report the gate swallowed leaves the lane armed and is
            // retried on its next pop rather than lost for a day. The gate's tick
            // is two relaxed atomics, so asking it under the lane lock is
            // cheaper than releasing and re-taking the lock to stamp.
            let divergence = ask
                .partitions
                .and_then(|u| s.divergence(u, now_ms).map(|w| (u, w)))
                .and_then(|(u, w)| {
                    self.divergence_log.tick_now().map(|suppressed| {
                        s.diverged_ms = now_ms;
                        (u, w, suppressed)
                    })
                });

            // A dimension the client sent is the client's. `chose_*` is what
            // decides whether the response says anything at all.
            let chose_partitions = ask.autopilot && ask.partitions.is_none();
            let chose_batch = ask.autopilot && ask.batch.is_none();
            let ap_batch = self.k.auto_batch;
            let plan = Plan {
                partitions: if chose_partitions {
                    star
                } else {
                    ask.default_partitions
                },
                // A DELEGATED batch is the controller's constant; a batch the
                // client sent is the client's, untouched and unclamped, exactly
                // as it arrives from the handler. `default_batch` already carries
                // the client's value when there is one (`p.batch.unwrap_or(200)`),
                // so this branch is the whole of the difference — and it is why
                // `AUTO_BATCH_DEFAULT` can be 100 without moving a single
                // existing consumer.
                //
                // The earlier "over-asking is free" reading of B was wrong and
                // the matched arms of 2026-08-24 measured the price: see
                // AUTO_BATCH_DEFAULT.
                batch: if chose_batch {
                    ap_batch
                } else {
                    ask.default_batch
                },
                chose_partitions,
                chose_batch,
            };
            (plan, divergence)
        };

        if let Some((user_w, star, suppressed)) = divergence {
            self.report_divergence(qkey, group, user_w, star, suppressed);
        }

        // Shadow computes and reports but never acts; `on` acts. Either way a
        // request that did not opt in leaves with no plan at all.
        let acting = self.k.mode == Mode::On && plan.engaged();
        if self.k.mode == Mode::Shadow && plan.engaged() {
            self.report_decision(qkey, group, plan, "shadow");
        }
        PopTicket {
            ap: Some(self.clone()),
            lane: Some(lane),
            plan: if acting { Some(plan) } else { None },
            qkey: if acting {
                qkey.to_string()
            } else {
                String::new()
            },
            group: if acting {
                group.to_string()
            } else {
                String::new()
            },
        }
    }

    /// get-or-create, with the cap. Held only for the lookup — every mutation
    /// happens on the returned `Arc` (the `hotlist::qstate` shape).
    fn lane(&self, qkey: &str, group: &str, now_ms: i64) -> Option<Arc<Lane>> {
        let mut g = self.lanes.lock().unwrap();
        if let Some(inner) = g.get(qkey) {
            if let Some(l) = inner.get(group) {
                return Some(l.clone());
            }
        }
        if self.live.load(Ordering::Relaxed) >= self.k.max_lanes {
            self.refused.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let l = Arc::new(Lane {
            st: Mutex::new(LaneState::new(now_ms)),
        });
        g.entry(qkey.to_string())
            .or_default()
            .insert(group.to_string(), l.clone());
        self.live.fetch_add(1, Ordering::Relaxed);
        Some(l)
    }

    /// Emit the one divergence line. Both gates have already granted it (the lane's
    /// 24h re-arm and the broker-wide window), so this only formats and counts.
    fn report_divergence(&self, qkey: &str, group: &str, user_w: i32, star: i32, suppressed: u64) {
        self.divergences.fetch_add(1, Ordering::Relaxed);
        let (tenant, queue) = crate::handlers::split_tenant_queue(qkey);
        tracing::warn!(
            target: "autopilot",
            tenant,
            queue,
            group,
            explicit = user_w,
            controller = star,
            suppressed,
            "pop autopilot: queue={queue} group={group} explicit partitions={user_w}, \
             controller would choose {star} — diagnostic only, the explicit value is \
             being used"
        );
    }

    fn report_decision(&self, qkey: &str, group: &str, plan: Plan, what: &'static str) {
        let Some(suppressed) = self.decision_log.tick_now() else {
            return;
        };
        let (tenant, queue) = crate::handlers::split_tenant_queue(qkey);
        tracing::info!(
            target: "autopilot",
            tenant,
            queue,
            group,
            mode = self.k.mode.as_str(),
            partitions = plan.partitions,
            batch = plan.batch,
            requests = self.requests.load(Ordering::Relaxed),
            adjusts = self.adjusts.load(Ordering::Relaxed),
            lanes = self.live.load(Ordering::Relaxed),
            suppressed,
            "pop autopilot {what}"
        );
    }

    /// Idle-lane eviction, run from the hot-list/gate idle sweep
    /// (`reconcile::spawn_idle_sweep`). Same CLOCK second chance as
    /// `HotList::evict_idle`: a lane survives at least one full sweep after its
    /// last access, and one with a live pop on it is never touched.
    pub fn evict_idle(&self) -> usize {
        if !self.active() {
            return 0;
        }
        let mut g = self.lanes.lock().unwrap();
        let mut dropped = 0usize;
        g.retain(|_, inner| {
            inner.retain(|_, lane| {
                // Sole holder ⇒ no ticket is mid-pop on it, and no clone can
                // appear while we hold `lanes`.
                if Arc::strong_count(lane) != 1 {
                    return true;
                }
                let mut s = lane.st.lock().unwrap();
                if s.touched {
                    s.touched = false;
                    return true;
                }
                if s.inflight > 0 {
                    return true;
                }
                drop(s);
                dropped += 1;
                false
            });
            !inner.is_empty()
        });
        self.live.fetch_sub(dropped, Ordering::Relaxed);
        dropped
    }

    pub fn lane_count(&self) -> usize {
        self.live.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn adjust_count(&self) -> u64 {
        self.adjusts.load(Ordering::Relaxed)
    }
}

/// The handler's handle on one pop's controller state. Holding the lane's `Arc`
/// means the claim observation and the outcome record cost NO map lookup — the
/// only lookup in a pop's life is `begin`'s.
pub struct PopTicket {
    ap: Option<Arc<PopAutopilot>>,
    lane: Option<Arc<Lane>>,
    plan: Option<Plan>,
    /// Only populated when the controller is acting, and only for the windowed
    /// decision log (`record`). An inert or shadow ticket allocates neither.
    qkey: String,
    group: String,
}

impl PopTicket {
    /// The ticket a request gets when the controller has nothing to do with it:
    /// the switch is off, or the lane cap was reached. Costs one `Option` check
    /// per touch point and nothing else.
    pub fn inert() -> PopTicket {
        PopTicket {
            ap: None,
            lane: None,
            plan: None,
            qkey: String::new(),
            group: String::new(),
        }
    }

    /// `Some` only when the controller ACTED. The handler's `None` arm is the
    /// pre-autopilot code path, unchanged, which is what makes byte-identity
    /// structural.
    #[inline]
    pub fn plan(&self) -> Option<Plan> {
        self.plan
    }

    /// One claim's observation of the ring, taken where it is free: `take_batch`
    /// already computed each candidate's `now - ready_since` for the arbiter's
    /// `LapStats`, so this is a max over a slice the caller is holding anyway.
    /// `max_age_ms` is the OLDEST servable partition this claim REACHED.
    ///
    /// Note the emphasis, and note that this is no longer the only age input: it
    /// is biased by construction — a narrow sweep only ever reaches the head of
    /// the ring — which is why `begin` now also feeds the EWMA the age of the
    /// oldest entry in the WHOLE ready set. This sample is kept alongside it
    /// because it is the one the admission arbiter shares, it is free, and it is
    /// the only age reading available on a claim that emptied the ring (peek 0,
    /// claim non-empty). It also stamps `last_claim_ms`, which is what the
    /// post-idle re-division measures.
    pub fn note_claim(&self, claimed: usize, max_age_ms: i64, now_ms: i64) {
        let Some(lane) = &self.lane else { return };
        if claimed == 0 {
            return;
        }
        let mut s = lane.st.lock().unwrap();
        s.age = ewma(s.age, max_age_ms.max(0) as f64, ALPHA_AGE);
        s.age_fresh = true;
        s.last_claim_ms = now_ms;
        s.touched = true;
    }

    /// One pop's outcome, recorded once per REQUEST at the point the handler
    /// answers. This is where the loop closes: measure at the end of pop N, apply
    /// at the start of pop N+1.
    pub fn record(&self, requested_batch: i32, delivered: usize, now_ms: i64) {
        let Some(lane) = &self.lane else { return };
        let Some(ap) = &self.ap else { return };
        let changed = {
            let mut s = lane.st.lock().unwrap();
            s.touched = true;
            if requested_batch > 0 {
                let fill = (delivered as f64 / requested_batch as f64).clamp(0.0, 1.0);
                s.fill = ewma(s.fill, fill, ALPHA_FILL);
            }
            s.step(ap.k.target_age_ms, ap.k.dwell_ms, ap.k.dwell_pops, now_ms)
        };
        if changed {
            ap.adjusts.fetch_add(1, Ordering::Relaxed);
            if let Some(plan) = self.plan {
                ap.report_decision(&self.qkey, &self.group, plan, "chose");
            }
        }
    }
}

impl Drop for PopTicket {
    /// Release this lane's worker slot. On Drop rather than at the end of
    /// `record` because a pop future can be DROPPED at any await (an axum client
    /// disconnect mid-claim), and a leaked M would permanently divide the
    /// feed-forward width of a live lane by a worker that no longer exists.
    fn drop(&mut self) {
        if let Some(lane) = &self.lane {
            let mut s = lane.st.lock().unwrap();
            s.inflight = s.inflight.saturating_sub(1);
        }
    }
}

/// Append the additive `autopilot` echo to an already-rendered pop body.
///
/// Rendering by splice rather than by a parameter threaded through
/// `render_pop_parts`: that renderer is shared by four pop routes and every
/// caller of it would have to carry a field only ONE of them can ever set. The
/// splice is exact — the body is this broker's own JSON object, it ends with the
/// closing brace, and nothing here can run for a request that did not opt in
/// (the caller only has an echo when `Plan::engaged`).
///
/// Shape (additive, appended LAST, nothing reordered or retyped):
/// `,"autopilot":{"partitions":W,"batch":B,"waitMs":ms}`
pub fn append_echo(body: &mut String, plan: Plan, wait_ms: u64) {
    if !body.ends_with('}') {
        return;
    }
    body.pop();
    body.push_str(",\"autopilot\":{\"partitions\":");
    body.push_str(&plan.partitions.to_string());
    body.push_str(",\"batch\":");
    body.push_str(&plan.batch.to_string());
    if wait_ms > 0 {
        body.push_str(",\"waitMs\":");
        body.push_str(&wait_ms.to_string());
    }
    body.push_str("}}");
}

#[cfg(test)]
#[path = "tests_unit/pop_autopilot.rs"]
mod pop_autopilot_tests;
