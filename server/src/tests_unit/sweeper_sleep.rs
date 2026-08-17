//! The sweeper's sleep computation: clamp, empty-claim backoff with jitter, empty-table backoff
//! (PLAN_KV_TIMERS §7.1 and §7.2, step D of the cycle).
//!
//! Wired from `src/sweeper.rs` — see `src/tests_unit/README.md` for the three-line block.
//!
//! This is the only piece of the sweeper that can be tested without a database, and it is also
//! the piece where every failure mode is a production incident rather than a wrong number:
//!
//!  - too short on an empty claim  → the spin of §7.2: five brokers, one burst, four of them
//!    issuing 200 probes/s each (64 seeks + a count apiece) exactly while the fifth is writing
//!    WAL and the producers are competing for the same fsync;
//!  - too long when work is due    → `deliverAt` misses by more than the declared ceiling, and
//!    §7.4 leans on `max_ms` being a real bound because the `T_TIMER_DUE` mesh frame is cut;
//!  - too short on an empty table  → the cost that every installation which will never use the
//!    feature pays forever (§7.1), which is the thing the sweeper perf gate measures;
//!  - a panic anywhere             → `panic = "abort"`, so it takes the broker with it.
//!
//! `sleep_ms` is therefore pure: the random draw is a parameter, not a call into the RNG, so
//! the jitter band can be asserted at both ends instead of sampled and hoped over.

use super::*;

fn k() -> SleepKnobs {
    SleepKnobs::defaults()
}

// ---------------------------------------------------------------------------
// The knobs themselves. The defaults are load-bearing numbers from the plan, not taste, and a
// silent edit to one of them changes a delivery guarantee we have written in the docs.
// ---------------------------------------------------------------------------

#[test]
fn defaults_are_the_documented_numbers() {
    let k = k();
    assert_eq!(k.min_ms, 5, "§4.2: a healthy timer lands within ~10 ms above MIN_SLEEP_MS");
    assert_eq!(
        k.max_ms, 1000,
        "§7.4: with no mesh frame, MAX_SLEEP_MS *is* the worst-case recovery window for a timer \
         scheduled by another broker — the docs promise at most one second"
    );
    assert_eq!(k.idle_max_ms, 30_000, "§7.1: the empty-table backoff tops out at 30 s");
    assert_eq!(k.empty_claim_min_ms, 25, "§7.2: the empty-claim band is 25..200 ms");
    assert_eq!(k.empty_claim_max_ms, 200);
    assert!(k.idle_after >= 1, "the backoff must not engage on the very first idle cycle");
}

// ---------------------------------------------------------------------------
// Due-driven: the probe told us when the next timer matures.
// ---------------------------------------------------------------------------

#[test]
fn due_is_clamped_into_the_band() {
    let k = k();
    // Below the floor, at the floor, inside, at the ceiling, above it.
    assert_eq!(sleep_ms(CycleOutcome::Due { next_in_ms: 1 }, 0, &k, 0.0), k.min_ms);
    assert_eq!(sleep_ms(CycleOutcome::Due { next_in_ms: 5 }, 0, &k, 0.0), 5);
    assert_eq!(sleep_ms(CycleOutcome::Due { next_in_ms: 250 }, 0, &k, 0.0), 250);
    assert_eq!(sleep_ms(CycleOutcome::Due { next_in_ms: 1000 }, 0, &k, 0.0), 1000);
    assert_eq!(sleep_ms(CycleOutcome::Due { next_in_ms: 86_400_000 }, 0, &k, 0.0), k.max_ms);
}

#[test]
fn overdue_work_sleeps_the_floor_not_zero() {
    // `nextInMs` is computed by the server and can be zero or negative (§6.2: everything is
    // relative to the server clock, the broker never does timestamp arithmetic). Overdue must
    // mean "go again immediately", but through the floor: a zero sleep on a claim that keeps
    // returning rows is a hot loop on a Maint slot and a pool connection.
    let k = k();
    for late in [0i64, -1, -1_000, i64::MIN] {
        assert_eq!(
            sleep_ms(CycleOutcome::Due { next_in_ms: late }, 0, &k, 0.0),
            k.min_ms,
            "next_in_ms = {late} must floor at MIN_SLEEP_MS, never 0"
        );
    }
}

#[test]
fn due_ignores_the_idle_counter() {
    // A stale idle counter must not survive the first real due probe, or a broker that has been
    // asleep for an hour would keep sleeping 30 s while timers mature under it. (`hint()` also
    // resets it — this pins that the reset is not the ONLY thing standing between us and that.)
    let k = k();
    assert_eq!(sleep_ms(CycleOutcome::Due { next_in_ms: 40 }, 9_999, &k, 0.0), 40);
}

// ---------------------------------------------------------------------------
// Empty claim: work WAS due, another broker is draining it. §7.2's anti-spin.
// ---------------------------------------------------------------------------

#[test]
fn empty_claim_spans_the_jitter_band_end_to_end() {
    let k = k();
    let lo = sleep_ms(CycleOutcome::EmptyClaim, 0, &k, 0.0);
    let hi = sleep_ms(CycleOutcome::EmptyClaim, 0, &k, 1.0);
    assert_eq!(lo, k.empty_claim_min_ms, "jitter 0.0 must give the bottom of the band");
    assert_eq!(hi, k.empty_claim_max_ms, "jitter 1.0 must give the top of the band");
    // Both ends must be reachable, or the jitter does not de-correlate the brokers and the
    // storm reforms one cycle later at a different offset.
    assert!(hi > lo);
}

#[test]
fn empty_claim_is_monotone_and_inside_the_band() {
    let k = k();
    let mut prev = 0u64;
    for i in 0..=100u32 {
        let j = f64::from(i) / 100.0;
        let s = sleep_ms(CycleOutcome::EmptyClaim, 0, &k, j);
        assert!(
            (k.empty_claim_min_ms..=k.empty_claim_max_ms).contains(&s),
            "jitter {j} left the 25..200 band: {s}"
        );
        assert!(s >= prev, "jitter {j} went backwards: {s} < {prev}");
        prev = s;
    }
}

#[test]
fn empty_claim_never_sleeps_past_the_delivery_ceiling() {
    // An operator who tightens MAX_SLEEP_MS below the jitter band means it: the ceiling is the
    // delivery-latency net, and a backoff is not allowed to punch through it.
    let k = SleepKnobs { max_ms: 50, ..k() };
    for i in 0..=10u32 {
        let s = sleep_ms(CycleOutcome::EmptyClaim, 0, &k, f64::from(i) / 10.0);
        assert!(s <= 50, "empty-claim backoff {s} exceeded MAX_SLEEP_MS=50");
    }
}

#[test]
fn a_bad_jitter_draw_cannot_escape_the_band() {
    // The RNG lives at the call site, so this function must treat `jitter` as untrusted input.
    // NaN is the one that matters: `f64::clamp` PROPAGATES NaN, and `NaN as u64` is 0 in Rust,
    // which would silently turn the anti-spin backoff into a zero sleep — the exact failure the
    // band exists to prevent, reachable only from a refactor nobody would review twice.
    let k = k();
    for bad in [f64::NAN, -1.0, -0.0, 2.0, f64::INFINITY, f64::NEG_INFINITY] {
        let s = sleep_ms(CycleOutcome::EmptyClaim, 0, &k, bad);
        assert!(
            (k.empty_claim_min_ms..=k.empty_claim_max_ms).contains(&s),
            "jitter {bad:?} produced {s}, outside the band"
        );
    }
    assert_eq!(
        sleep_ms(CycleOutcome::EmptyClaim, 0, &k, f64::NAN),
        k.empty_claim_min_ms,
        "a non-finite draw must degrade to the bottom of the band, not to zero"
    );
}

// ---------------------------------------------------------------------------
// Empty table: the cost paid by everyone who never uses the feature (§7.1).
// ---------------------------------------------------------------------------

#[test]
fn idle_holds_the_ceiling_until_the_backoff_engages() {
    let k = k();
    for n in 0..=k.idle_after {
        assert_eq!(
            sleep_ms(CycleOutcome::Idle, n, &k, 0.0),
            k.max_ms,
            "idle cycle {n} must still sleep the normal ceiling: the backoff is for a table \
             that has been empty a while, not for one that just drained"
        );
    }
}

#[test]
fn idle_backs_off_progressively_to_the_cap() {
    let k = k();
    let mut prev = sleep_ms(CycleOutcome::Idle, k.idle_after, &k, 0.0);
    let mut reached_cap = false;
    for n in k.idle_after + 1..k.idle_after + 40 {
        let s = sleep_ms(CycleOutcome::Idle, n, &k, 0.0);
        assert!(s >= prev, "idle backoff went backwards at cycle {n}: {s} < {prev}");
        assert!(s <= k.idle_max_ms, "idle backoff {s} passed the 30 s cap at cycle {n}");
        if s == k.idle_max_ms {
            reached_cap = true;
        }
        prev = s;
    }
    assert!(reached_cap, "the backoff must actually reach IDLE_MAX_SLEEP_MS, not creep toward it");
    // It must climb fast enough to matter. Doubling from 1 s reaches 30 s in five steps; a
    // linear ramp would leave a permanently-empty cell probing for minutes.
    assert!(
        sleep_ms(CycleOutcome::Idle, k.idle_after + 5, &k, 0.0) >= 16_000,
        "the ramp is too slow to pay for itself"
    );
}

#[test]
fn a_saturated_idle_counter_does_not_overflow() {
    // The counter is incremented once per cycle and only `hint()` and a non-idle outcome reset
    // it. A cell that is up for a month with both tables empty gets a large count, and a
    // doubling written as `max_ms << (n - idle_after)` is UB/panic past 63 shifts. Whatever the
    // implementation, the answer at saturation is the cap.
    let k = k();
    for n in [63u32, 64, 65, 1_000, u32::MAX - 1, u32::MAX] {
        assert_eq!(
            sleep_ms(CycleOutcome::Idle, n, &k, 0.0),
            k.idle_max_ms,
            "idle cycle {n} must saturate at the cap"
        );
    }
}

#[test]
fn the_idle_cap_can_be_below_the_normal_ceiling() {
    // Nonsensical but reachable from the environment: IDLE_MAX < MAX. The result must still be
    // bounded and must not panic; the idle branch simply never rises above the ceiling.
    let k = SleepKnobs { max_ms: 1000, idle_max_ms: 200, ..k() };
    for n in [0u32, 5, 6, 50, u32::MAX] {
        let s = sleep_ms(CycleOutcome::Idle, n, &k, 0.0);
        assert!(s <= k.max_ms.max(k.idle_max_ms), "idle sleep {s} unbounded at cycle {n}");
    }
}

// ---------------------------------------------------------------------------
// Knob hygiene. Everything here arrives from the environment.
// ---------------------------------------------------------------------------

#[test]
fn inverted_knobs_do_not_panic() {
    // `u64::clamp(min, max)` PANICS when min > max. `QUEEN_SWEEPER_MIN_SLEEP_MS=5000` with the
    // default ceiling is a typo an operator can make in one keystroke, and a panic here aborts
    // the process (`panic = "abort"`) on a background task — a config typo must degrade, never
    // crash. The floor wins: it is the one an operator sets on purpose.
    let k = SleepKnobs {
        min_ms: 5_000,
        max_ms: 1_000,
        idle_max_ms: 100,
        idle_after: 0,
        empty_claim_min_ms: 900,
        empty_claim_max_ms: 100,
    };
    let a = sleep_ms(CycleOutcome::Due { next_in_ms: 10 }, 0, &k, 0.0);
    let b = sleep_ms(CycleOutcome::EmptyClaim, 0, &k, 0.5);
    let c = sleep_ms(CycleOutcome::Idle, u32::MAX, &k, 1.0);
    assert_eq!(a, 5_000, "with an inverted band the floor wins");
    for s in [a, b, c] {
        assert!(s > 0);
    }
}

#[test]
fn zero_knobs_never_produce_a_spin() {
    // `QUEEN_SWEEPER_MIN_SLEEP_MS=0` must not turn the loop into a busy wait against the pool.
    let k = SleepKnobs {
        min_ms: 0,
        max_ms: 0,
        idle_max_ms: 0,
        idle_after: 0,
        empty_claim_min_ms: 0,
        empty_claim_max_ms: 0,
    };
    for o in [CycleOutcome::Due { next_in_ms: 0 }, CycleOutcome::EmptyClaim, CycleOutcome::Idle] {
        let s = sleep_ms(o, 0, &k, 0.0);
        assert!(s >= ABSOLUTE_FLOOR_MS, "a zeroed config produced a {s} ms sleep");
    }
}

#[test]
fn sleep_is_deterministic() {
    // No clock and no RNG inside: the perf gate measures the empty-table cycle cost by counting
    // probes over a fixed window, and that number is only meaningful if the sleep is a pure
    // function of (outcome, idle_cycles, knobs, jitter).
    let k = k();
    for n in [0u32, 3, 6, 12] {
        for j in [0.0, 0.37, 1.0] {
            let a = sleep_ms(CycleOutcome::Idle, n, &k, j);
            let b = sleep_ms(CycleOutcome::Idle, n, &k, j);
            assert_eq!(a, b);
        }
    }
}
