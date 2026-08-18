//! THE OVERSHOOT SIMULATION — the exit criterion of F5 (PLAN_KV_TIMERS.md §16),
//! and the only artefact that proves the correction §9.3 makes to two earlier
//! designs.
//!
//! WHAT THE DESIGNS SAID, AND WHY IT WAS WRONG BY AN ORDER OF MAGNITUDE. The
//! proposed bound on a soft quota was "write rate × 30 s", where 30 s is how
//! often a broker RE-READS the measurement. But the measurement is WRITTEN by
//! the rollup, every 300 s. Nothing a reader does can make it fresher than its
//! writer, so the real bound of a measure-only enforcer is
//!
//!     rate × (rollup period + refresh period + rollup duration)
//!
//! With the plan's own numbers — a free tenant at 20 writes/s against a 1000-row
//! timer quota — that is 6600 rows of overshoot on a 1000-row quota: 660%,
//! sustained, never answering a single 403. "Soft" does not describe that; it
//! describes a limit that does not exist.
//!
//! WHAT IS IMPLEMENTED INSTEAD (§9.3): **the local in-process delta is THE
//! ENFORCER, and the measurement serves only the release.** Every broker holds
//! the measurement it last read plus ITS OWN count of what it has written since,
//! and refuses at `quota - measure` without waiting for a newer measurement.
//!
//! THE PROPERTY THIS FILE PINS, stated so it cannot be weakened by a later edit:
//!
//!   * the measure-only bound GROWS WITH RATE AND WITH TIME — the two things an
//!     operator cannot bound — and case 1 reproduces the plan's 660% exactly, so
//!     a reader can check the arithmetic of §9.3 against a running assertion;
//!   * the local-delta bound is INDEPENDENT OF BOTH. It is `(brokers - 1) ×
//!     headroom`, where `headroom` is what was left at the last refresh: it comes
//!     from fan-out, which the operator sizes, and not from what the tenant does.
//!     Cases 2-4 double the rate, quadruple the run and change nothing;
//!   * with one broker the overshoot is EXACTLY ZERO (case 5) — a single-broker
//!     cell, which is every self-hosted deployment and the v1 shipping target of
//!     §16, gets a HARD quota out of a mechanism that is only soft because of
//!     fan-out;
//!   * the hot cadence of §9.3 (above 80% the rollup drops to the refresh
//!     period) shrinks `headroom` and therefore the bound, which is case 6 —
//!     the reason that clause exists at all.
//!
//! Pure arithmetic on the real gate: no database, no clock, no sleeps, so it
//! runs in a plain `cargo test`.

use super::*;

/// The plan's free-plan numbers (§9.3), so the assertions below are checkable
/// against the prose rather than against themselves.
const ROLLUP_PERIOD_S: i64 = 300;
const REFRESH_PERIOD_S: i64 = 30;
/// The rollup's own duration, the third term the earlier designs dropped.
const ROLLUP_DURATION_S: i64 = 0;
const FREE_TIMER_QUOTA: i64 = 1000;
const FREE_WRITE_RATE: i64 = 20;

/// The enforcer under test, in the two shapes being compared.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Policy {
    /// What the design documents proposed: a broker blocks only once the
    /// MEASUREMENT it last read is at or over the quota. The delta is not
    /// consulted, so everything written since the measurement was computed is
    /// invisible to the gate.
    MeasureOnly,
    /// §9.3 as decided: measurement PLUS this process's own delta.
    LocalDelta,
}

/// One broker of the simulated cell. `measure` is what it last READ (which the
/// rollup last WROTE, which is older still); `delta` is what it has admitted
/// since that read.
struct SimBroker {
    quotas: Quotas,
    /// Which second of the refresh period this broker's refresh lands on.
    ///
    /// The phase is NOT decoration. A broker whose refresh coincides with the
    /// rollup adopts a measurement that is seconds old, and a simulation built
    /// that way would flatter the design by exactly the term §9.3 corrects. The
    /// worst case — and therefore the one a bound must hold for — is the refresh
    /// that lands just BEFORE a rollup and must then wait a whole further period
    /// to see it, which is where `300 + 30` comes from.
    phase: i64,
}

impl SimBroker {
    fn new(i: usize) -> Self {
        // `for_test` builds the real gate with the real hysteresis and no rate
        // limiting, so this file exercises the shipping code path and not a
        // model of it. MeasureOnly is obtained by never charging the delta —
        // which is precisely what the rejected design did.
        SimBroker {
            quotas: Quotas::for_test(TestKnobs {
                cap: 1024,
                require_grant: false,
                release_percent: 90,
                ..TestKnobs::default()
            }),
            phase: (REFRESH_PERIOD_S - 1 + i as i64) % REFRESH_PERIOD_S,
        }
    }

    /// Publish a measurement into this broker, as the 30 s refresh loop does.
    /// Charging the delta is reset by the refresh, exactly as in production:
    /// the delta counts writes SINCE the measurement, so a newer measurement
    /// makes the old delta double counting.
    fn refresh(&self, tenant: &str, timer_rows: i64, quota: i64, computed_at_ms: i64) {
        self.quotas.refresh(vec![TenantRow {
            tenant: tenant.to_string(),
            limits: Some(Limits {
                enabled: true,
                max_timers: Some(quota),
                ..Limits::default()
            }),
            measure: Measure {
                timer_rows,
                computed_at_ms,
                ..Measure::default()
            },
        }]);
    }

    /// One scheduling attempt. Returns whether it was admitted.
    fn try_schedule(&self, tenant: &str, policy: Policy) -> bool {
        match policy {
            // The rejected shape: consult the measurement alone, charge nothing.
            Policy::MeasureOnly => self.quotas.measure_only_admits(tenant, 1),
            Policy::LocalDelta => matches!(self.quotas.charge_timers(tenant, 1), Verdict::Allow),
        }
    }
}

/// One simulated cell.
struct Cell {
    policy: Policy,
    brokers: usize,
    seconds: i64,
    rate: i64,
    quota: i64,
    rollup_period_s: i64,
    /// Where the tenant starts. `quota - 1` puts it one write below the cap at
    /// t=0, which is the alignment the bound of §9.3 is stated for: the crossing
    /// happens immediately after a rollup, so the gate is blind for a whole
    /// rollup period plus a whole refresh period.
    start: i64,
}

impl Cell {
    fn new(policy: Policy, brokers: usize, seconds: i64, rate: i64) -> Self {
        Cell {
            policy,
            brokers,
            seconds,
            rate,
            quota: FREE_TIMER_QUOTA,
            rollup_period_s: ROLLUP_PERIOD_S,
            start: 0,
        }
    }
    fn rollup_every(mut self, s: i64) -> Self {
        self.rollup_period_s = s;
        self
    }
    fn starting_at(mut self, n: i64) -> Self {
        self.start = n;
        self
    }

    /// Run, and return the true number of rows that ended up in the table.
    ///
    /// The clocks are the plan's: the rollup WRITES the truth every
    /// `rollup_period_s` and each broker READS it on its own phase of
    /// `REFRESH_PERIOD_S`, which is what makes the measurement `rollup +
    /// refresh` stale rather than `refresh` stale — the whole point of §9.3's
    /// correction.
    fn run(&self) -> i64 {
        const TENANT: &str = "11111111-1111-1111-1111-111111111111";
        let cell: Vec<SimBroker> = (0..self.brokers).map(SimBroker::new).collect();
        // The truth: what is actually in queen.log_timers.
        let mut actual: i64 = self.start;
        // What the rollup last WROTE, and when it finished writing it.
        let mut published: i64 = self.start;
        // When the rollup that produced `published` STARTED. It is the
        // measurement's identity: a broker that re-reads the same row must be
        // able to tell that it is the same measurement (see `Quotas::refresh`).
        let mut computed_at: i64 = 0;
        let mut published_at: i64 = 0;

        for b in &cell {
            b.refresh(TENANT, published, self.quota, computed_at);
        }

        for t in 1..=self.seconds {
            // The rollup: publishes the truth as of when it STARTED, and takes
            // ROLLUP_DURATION_S to finish — the third term the earlier designs
            // dropped entirely.
            if t % self.rollup_period_s == 0 {
                published = actual;
                computed_at = t;
                published_at = t + ROLLUP_DURATION_S;
            }
            // The refresh: each broker adopts whatever is published when its own
            // phase ticks, and never a measurement still being written.
            for b in &cell {
                if t % REFRESH_PERIOD_S == b.phase && t >= published_at {
                    b.refresh(TENANT, published, self.quota, computed_at);
                }
            }
            for i in 0..self.rate {
                let b = &cell[(i as usize + t as usize) % self.brokers];
                if b.try_schedule(TENANT, self.policy) {
                    actual += 1;
                }
            }
        }
        actual
    }
}

/// CASE 1 — the number in the plan, reproduced. A measure-only enforcer lets a
/// free tenant sit at 660% OVER its timer quota indefinitely, and the overshoot
/// is `rate × (rollup + refresh + duration)`.
///
/// The tenant starts one write below its cap, so the crossing happens
/// immediately after a rollup: that is the alignment §9.3 states the bound for,
/// and any other alignment is strictly luckier.
#[test]
fn measure_only_overshoots_by_rate_times_the_full_staleness_window() {
    let actual = Cell::new(Policy::MeasureOnly, 1, 1800, FREE_WRITE_RATE)
        .starting_at(FREE_TIMER_QUOTA - 1)
        .run();
    let overshoot = actual - FREE_TIMER_QUOTA;
    let predicted = FREE_WRITE_RATE * (ROLLUP_PERIOD_S + REFRESH_PERIOD_S + ROLLUP_DURATION_S);
    // Never MORE than the bound — a bound that the model exceeds is not a bound…
    assert!(
        overshoot <= predicted,
        "measure-only overshoot {overshoot} exceeded rate x (300+30+rollup) = {predicted}"
    );
    // …and not comfortably less either, or the formula would be describing
    // something other than what happens. One refresh tick of quantisation.
    assert!(
        predicted - overshoot <= FREE_WRITE_RATE * REFRESH_PERIOD_S,
        "measure-only overshoot {overshoot} is far under the predicted {predicted}: the \
         model has stopped exercising the staleness window the formula is about"
    );
    // And the ratio the plan calls 660%, which is the sentence an operator reads.
    let ratio = overshoot as f64 / FREE_TIMER_QUOTA as f64;
    assert!(
        ratio > 6.0,
        "the plan says a free tenant overshoots a 1000-timer quota by ~660%; got {ratio:.1}x"
    );
}

/// CASE 2 — the same cell, the same rate, with the enforcer of §9.3. The
/// overshoot collapses from "6600 rows" to "nothing".
#[test]
fn the_local_delta_blocks_a_single_broker_exactly_at_the_quota() {
    let actual = Cell::new(Policy::LocalDelta, 1, 1800, FREE_WRITE_RATE).run();
    assert_eq!(
        actual, FREE_TIMER_QUOTA,
        "with one broker the local delta is a HARD quota — that is the self-hosted \
         and dedicated deployment of §16, which is what v1 ships to"
    );
}

/// CASE 3 — THE PROPERTY THAT MATTERS. The measure-only bound is a function of
/// the rate; the local-delta bound is not. Ten times the rate, same overshoot.
#[test]
fn the_local_delta_bound_does_not_grow_with_the_write_rate() {
    let slow = Cell::new(Policy::LocalDelta, 4, 1800, 20).run();
    let fast = Cell::new(Policy::LocalDelta, 4, 1800, 200).run();
    assert_eq!(
        slow, fast,
        "the local delta must bound the overshoot by FAN-OUT, which the operator \
         sizes, and never by the write rate, which the tenant chooses"
    );

    // The same knob under the rejected policy, for contrast: the whole reason
    // the correction exists, and what stops case 3 being a tautology.
    let slow_bad = Cell::new(Policy::MeasureOnly, 4, 1800, 20).run();
    let fast_bad = Cell::new(Policy::MeasureOnly, 4, 1800, 200).run();
    assert!(
        fast_bad > slow_bad * 5,
        "sanity: the rejected policy IS rate-dependent ({slow_bad} vs {fast_bad})"
    );
}

/// CASE 4 — nor with time. A cell left running four times as long overshoots by
/// the same amount, because the delta is reset by the refresh and re-earned
/// against a measurement that has itself moved.
#[test]
fn the_local_delta_bound_does_not_grow_with_time() {
    let short = Cell::new(Policy::LocalDelta, 4, 900, 50).run();
    let long = Cell::new(Policy::LocalDelta, 4, 3600, 50).run();
    assert_eq!(
        short, long,
        "a soft quota whose error grows with uptime is not a quota; the delta's \
         error is paid ONCE per broker, not once per period"
    );
}

/// CASE 5 — the declared bound, checked as an inequality rather than as an
/// anecdote: `overshoot <= (brokers - 1) x headroom`, and `headroom <= quota`.
/// This is the sentence that goes in the documentation, so it is the one under
/// test.
#[test]
fn the_overshoot_is_bounded_by_fan_out_times_headroom() {
    for brokers in [1usize, 2, 4, 8, 16] {
        let actual = Cell::new(Policy::LocalDelta, brokers, 1800, 50).run();
        let overshoot = (actual - FREE_TIMER_QUOTA).max(0);
        let bound = (brokers as i64 - 1) * FREE_TIMER_QUOTA;
        assert!(
            overshoot <= bound,
            "brokers={brokers}: overshoot {overshoot} exceeded the declared bound {bound}"
        );
    }
}

/// CASE 6 — why §9.3 lowers the rollup cadence to the refresh cadence above 80%.
/// The bound is `(brokers - 1) x headroom`, and `headroom` is what a fresher
/// measurement shrinks: a rollup running at the refresh period leaves less room
/// for every broker to spend independently, so the SAME cell overshoots less.
#[test]
fn the_hot_cadence_shrinks_the_bound_it_is_meant_to_shrink() {
    let cold = Cell::new(Policy::LocalDelta, 8, 1800, 50).run();
    let hot = Cell::new(Policy::LocalDelta, 8, 1800, 50)
        .rollup_every(REFRESH_PERIOD_S)
        .run();
    assert!(
        hot < cold,
        "the hot cadence of §9.3 exists to shrink the headroom each broker may \
         spend independently; if it does not, the clause has no reason to exist \
         (hot={hot}, cold={cold})"
    );
}
