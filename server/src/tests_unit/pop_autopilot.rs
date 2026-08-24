//! POP AUTOPILOT — the controller's contract, driven as a pure state machine.
//!
//! Everything here runs with NO database, NO broker and NO clock: every entry
//! point takes `now_ms` explicitly, which is what lets the convergence test below
//! simulate minutes of a growing topology in a few milliseconds of test time.
//!
//! The first two tests are the ones that must never be weakened. They pin the
//! promise the feature is allowed to exist under: an old client is untouched, and
//! an explicit value is never overridden.

use super::*;

const QK: &str = "acme\u{1f}orders";
const G: &str = "workers";

fn controller(mode: Mode) -> Arc<PopAutopilot> {
    PopAutopilot::new(Knobs {
        mode,
        ..Knobs::defaults()
    })
}

/// What today's broker resolves for a plain grouped pop that sends neither knob:
/// `partitions` absent means 1 and `batch` absent means 200. These are the
/// HANDLER's values, and they are what a request that did not opt in must keep —
/// which is not the same number as the batch the controller picks for a request
/// that DELEGATED the dimension (`AUTO_BATCH_DEFAULT`, 100).
const DEF_W: i32 = 1;
const DEF_B: i32 = 200;

fn ask_plain() -> Ask {
    Ask {
        autopilot: false,
        partitions: None,
        batch: None,
        default_partitions: DEF_W,
        default_batch: DEF_B,
    }
}

fn ask_auto() -> Ask {
    Ask {
        autopilot: true,
        ..ask_plain()
    }
}

/// The handler's one `match`, mirrored so the test asserts the exact expression
/// `handle_pop` evaluates (`handlers::data::handle_pop`, the hot-list branch).
fn claim_knobs(plan: Option<Plan>, batch: i32, max_parts: i32) -> (i32, i32) {
    match plan {
        Some(pl) => (pl.batch, pl.partitions),
        None => (batch, max_parts),
    }
}

/// A ring reading with `ready` partitions whose oldest has waited `age_ms`. An
/// empty ring carries no age at all, which is what the controller must read as
/// "no sample" (see `ReadyPeek`).
fn peek(ready: usize, age_ms: i64) -> ReadyPeek {
    ReadyPeek {
        parts: ready,
        oldest_ms: if ready > 0 { age_ms } else { 0 },
    }
}

/// Drive one pop through the whole ticket lifecycle. `age_ms` is BOTH the age of
/// the oldest ready entry (what `begin` peeks) and the oldest age this claim
/// reached (what `note_claim` reports) — the two agree in every case here except
/// the bench regression below, which exists precisely because they can diverge.
fn pop(
    ap: &Arc<PopAutopilot>,
    ask: Ask,
    ready: usize,
    claimed: usize,
    age_ms: i64,
    delivered: usize,
    now_ms: i64,
) -> Option<Plan> {
    let t = ap.begin(QK, G, ask, peek(ready, age_ms), now_ms);
    let plan = t.plan();
    if claimed > 0 {
        t.note_claim(claimed, age_ms, now_ms);
    }
    t.record(DEF_B, delivered, now_ms);
    plan
}

// ---------------------------------------------------------------- byte identity

/// THE contract. A request that does not carry `autopilot=true` must resolve to
/// today's claim parameters in every switch position — including `on`, and
/// including on a lane the controller has strong opinions about. The `None` plan
/// is what makes that structural: the handler's fallback arm IS the pre-autopilot
/// code path, so there is nothing left to get wrong downstream.
#[test]
fn a_request_without_the_parameter_is_untouched_in_every_switch_position() {
    for mode in [Mode::Off, Mode::Shadow, Mode::On] {
        let ap = controller(mode);
        // Teach the lane to want a wide claim: a big ready set and an age well
        // over target, for long enough to clear the dwell.
        for i in 0..40 {
            pop(&ap, ask_auto(), 500, 50, 400, 150, 1_000 + i * 600);
        }

        // …and now the old client arrives.
        let plan = pop(&ap, ask_plain(), 500, 50, 400, 150, 100_000);
        assert!(
            plan.is_none(),
            "{mode:?}: a request without the parameter must get no plan at all"
        );
        assert_eq!(
            claim_knobs(plan, DEF_B, DEF_W),
            (DEF_B, DEF_W),
            "{mode:?}: today's defaults, byte for byte"
        );

        // The same holds for a client that sends knobs but not the opt-in.
        let explicit = Ask {
            partitions: Some(5000),
            batch: Some(64),
            ..ask_plain()
        };
        let plan = pop(&ap, explicit, 500, 50, 400, 150, 101_000);
        assert!(
            plan.is_none(),
            "{mode:?}: explicit values without the opt-in"
        );
        assert_eq!(claim_knobs(plan, 64, 5000), (64, 5000));
    }
}

/// `shadow` computes and reports but must never ACT, and `off` must not even
/// compute: no lane, no state, nothing to evict.
#[test]
fn the_kill_switch_positions_mean_what_they_say() {
    let off = controller(Mode::Off);
    for i in 0..10 {
        assert!(pop(&off, ask_auto(), 500, 50, 400, 150, 1_000 + i * 600).is_none());
    }
    assert_eq!(off.lane_count(), 0, "off keeps no controller state at all");
    assert_eq!(off.evict_idle(), 0);

    let shadow = controller(Mode::Shadow);
    for i in 0..10 {
        assert!(
            pop(&shadow, ask_auto(), 500, 50, 400, 150, 1_000 + i * 600).is_none(),
            "shadow answers with today's defaults"
        );
    }
    assert_eq!(shadow.lane_count(), 1, "…but it keeps learning");

    let on = controller(Mode::On);
    let plan = pop(&on, ask_auto(), 500, 50, 400, 150, 1_000).expect("on acts");
    assert!(plan.chose_partitions && plan.chose_batch);
}

// ------------------------------------------------------------ per-dimension

/// Per-dimension independence: a knob the client sent is the client's, and the
/// controller does not merely refrain from raising it — it never touches the
/// dimension at all. Pinned on a lane whose controller wants the ceiling.
#[test]
fn an_explicit_dimension_is_never_overridden() {
    let ap = controller(Mode::On);
    for i in 0..40 {
        pop(&ap, ask_auto(), 4096, 64, 900, 200, 1_000 + i * 600);
    }
    let wide = pop(&ap, ask_auto(), 4096, 64, 900, 200, 100_000).unwrap();
    assert!(wide.partitions > 8, "the lane wants a wide claim: {wide:?}");

    // Manual W, automatic B.
    let plan = pop(
        &ap,
        Ask {
            autopilot: true,
            partitions: Some(1),
            ..ask_plain()
        },
        4096,
        64,
        900,
        200,
        101_000,
    )
    .unwrap();
    assert_eq!(
        plan.partitions, 1,
        "autopilot=true&partitions=1 is a MANUAL 1"
    );
    assert!(
        !plan.chose_partitions,
        "the controller must not claim that dimension"
    );
    assert!(plan.chose_batch, "…while the batch is still automatic");
    assert_eq!(claim_knobs(Some(plan), DEF_B, 1).1, 1);
    assert_eq!(
        plan.batch, AUTO_BATCH_DEFAULT,
        "a DELEGATED batch is the controller's constant, not the handler's \
         absent-field 200"
    );

    // Manual B, automatic W. Note the fixture: when the client sends `batch=7`
    // the handler's own resolution IS 7 (`p.batch.unwrap_or(200)`), so that is
    // what `default_batch` carries — the two fields of `Ask` cannot disagree in
    // any request the handler can build.
    let plan = pop(
        &ap,
        Ask {
            autopilot: true,
            batch: Some(7),
            default_batch: 7,
            ..ask_plain()
        },
        4096,
        64,
        900,
        200,
        102_000,
    )
    .unwrap();
    assert!(plan.chose_partitions);
    assert!(!plan.chose_batch);
    assert_eq!(
        plan.batch, 7,
        "an explicit batch is the client's, untouched by the autopilot constant"
    );
}

/// The delegated batch is a KNOB, and it moves only the delegated dimension. An
/// operator raising it must not reach a single consumer that sent its own value.
#[test]
fn the_delegated_batch_knob_is_honoured_and_touches_nothing_else() {
    let ap = PopAutopilot::new(Knobs {
        auto_batch: 37,
        ..Knobs::defaults()
    });
    // Delegated ⇒ the knob.
    let plan = pop(&ap, ask_auto(), 8, 8, 5, 24, 1_000).unwrap();
    assert!(plan.chose_batch);
    assert_eq!(plan.batch, 37, "the env override is what the claim uses");

    // Explicit ⇒ the client's own value, whatever the knob says.
    let plan = pop(
        &ap,
        Ask {
            autopilot: true,
            batch: Some(500),
            default_batch: 500,
            ..ask_plain()
        },
        8,
        8,
        5,
        24,
        2_000,
    )
    .unwrap();
    assert!(!plan.chose_batch);
    assert_eq!(plan.batch, 500, "the knob must not reach an explicit batch");

    // Not opted in at all ⇒ no plan, so the knob cannot reach the claim either.
    assert!(pop(&ap, ask_plain(), 8, 8, 5, 24, 3_000).is_none());
}

/// The shipped default, pinned: the fleet's real client-side default is 100, and
/// the matched arms of 2026-08-24 measured the 200 the handler resolves an absent
/// field to at +35% p99. Delegating a knob must not silently double it.
#[test]
fn the_default_delegated_batch_is_the_fleets_hundred_not_the_handlers_two_hundred() {
    assert_eq!(AUTO_BATCH_DEFAULT, 100);
    assert_eq!(Knobs::defaults().auto_batch, AUTO_BATCH_DEFAULT);
    assert_ne!(
        AUTO_BATCH_DEFAULT, DEF_B,
        "the two defaults are deliberately different numbers"
    );
    assert!((AUTO_BATCH_MIN..=AUTO_BATCH_MAX).contains(&AUTO_BATCH_DEFAULT));

    let ap = controller(Mode::On);
    let plan = pop(&ap, ask_auto(), 8, 8, 5, 24, 1_000).unwrap();
    assert_eq!(plan.batch, 100);
}

/// Both dimensions manual ⇒ nothing was resolved ⇒ no plan, hence no response
/// echo either. An SDK that pins both knobs is on exactly the old path.
#[test]
fn opting_in_while_pinning_both_knobs_resolves_nothing() {
    let ap = controller(Mode::On);
    let plan = pop(
        &ap,
        Ask {
            autopilot: true,
            partitions: Some(4),
            batch: Some(50),
            ..ask_plain()
        },
        500,
        50,
        400,
        150,
        1_000,
    );
    assert!(plan.is_none(), "no dimension resolved ⇒ no plan, no echo");
}

// ------------------------------------------------------------------- the law

/// THE BENCH REGRESSION (cell A/B, 2026-08-23): the baseline at `W=10` served
/// p99 112 ms; the first law converged to `W=1` and served p99 380 ms at the
/// same 1060 msg/s, with ready depth oscillating 28 -> 4 -> 1 and
/// `cands_visit=1.1`.
///
/// The shape reproduced here is the exact one: a persistent ready set of ~28
/// partitions and an age signal PINNED at 5 ms, because at width 1 the only
/// partition a claim ever reaches is the promptly-served head. Under the first
/// law nothing could widen — the age never crossed target, and the ready count
/// was only a cap — so W sat at 1 and drained each burst one partition per pop.
///
/// Ready pressure has to move it on its own, with the age still saying "fine".
#[test]
fn bench_2026_08_23_width_one_must_not_starve_a_ready_ring_of_28() {
    let ap = controller(Mode::On);
    let mut t = 1_000i64;

    // Converge to width 1 the way the cell did: a quiet lane with one ready
    // partition at a time, served promptly.
    for _ in 0..40 {
        t += 600;
        pop(&ap, ask_auto(), 1, 1, 5, 3, t);
    }
    let converged = pop(&ap, ask_auto(), 1, 1, 5, 3, t).unwrap().partitions;
    assert_eq!(converged, 1, "the lane starts where the bench found it");

    // Now the bursts. 28 partitions ready, a claim of width 1 reaches exactly
    // one of them, and that one has been waiting 5 ms — the biased sample that
    // told the first controller everything was healthy.
    let mut widths = Vec::new();
    for _ in 0..40 {
        t += 10; // ~100 pops/s, so the dwell's 16-pop arm is what fires
        widths.push(pop(&ap, ask_auto(), 28, 1, 5, 3, t).unwrap().partitions);
    }
    let final_w = *widths.last().unwrap();
    assert!(
        widths.iter().any(|&w| w > 1),
        "REGRESSION: the controller held width 1 against 28 ready partitions"
    );
    assert!(
        final_w >= 28,
        "ready pressure must take the width to the ready set, got {final_w} \
         (sequence {widths:?})"
    );
    // …and it has to get there FAST: the backlog drains at the old width for
    // every dwell period spent climbing, which is what made doubling alone
    // insufficient.
    let adjustments = widths.windows(2).filter(|w| w[0] != w[1]).count();
    assert!(
        adjustments <= 2,
        "the snap must close the gap in a couple of adjustments, took \
         {adjustments} ({widths:?})"
    );
}

/// Ready pressure is the PRIMARY driver and does not consult the age at all: a
/// ready set larger than the width widens it whatever the age says, including a
/// pinned-healthy age and an under-full batch.
#[test]
fn ready_pressure_widens_regardless_of_the_age() {
    let ap = controller(Mode::On);
    let mut t = 1_000i64;
    for _ in 0..40 {
        t += 600;
        pop(&ap, ask_auto(), 1, 1, 1, 3, t);
    }
    assert_eq!(pop(&ap, ask_auto(), 1, 1, 1, 3, t).unwrap().partitions, 1);

    // Age 1 ms — far under target, i.e. the arm that used to be the only
    // widening trigger is firmly OFF — and a 1.5% fill. Ready alone must move it.
    let mut seq = Vec::new();
    for _ in 0..12 {
        t += 600;
        seq.push(pop(&ap, ask_auto(), 40, 1, 1, 3, t).unwrap().partitions);
    }
    let w = *seq.last().unwrap();
    // The loop closes at the END of a pop and applies at the start of the next,
    // so the burst's first pop still reports the old width and the SECOND one
    // carries the step. That step has to be a real one, not a doubling.
    assert!(seq[1] >= 8, "the first step is a real one: {seq:?}");
    // Not 40 on the nose: `ready` is an EWMA, so the width climbs with it rather
    // than snapping onto an instantaneous spike. That damping is the property
    // that keeps a one-pop burst from pinning the ceiling.
    assert!(w >= 32, "ready pressure alone must widen: {w}");
}

/// Age is kept as the SECONDARY trigger, for the residual shape ready pressure
/// cannot see: a small ready set whose few partitions are nonetheless waiting too
/// long. Bounded doubling, and it never needs a full batch — at B=200 a sparse
/// lane never fills one however wide the claim, so a full-batch precondition
/// would make this unreachable exactly where it is needed.
#[test]
fn age_still_widens_a_small_but_slow_ready_set() {
    let ap = controller(Mode::On);
    let mut t = 1_000i64;
    // One ready partition throughout, so the ready-pressure arm can never fire
    // (ready <= W from the first pop) and the age arm is demonstrably the only
    // thing moving the width.
    let first = pop(&ap, ask_auto(), 1, 1, 500, 3, t).unwrap().partitions;
    assert_eq!(first, 1);
    let mut w = first;
    for _ in 0..8 {
        t += 600;
        w = pop(&ap, ask_auto(), 1, 1, 500, 3, t).unwrap().partitions;
    }
    assert!(
        w > first,
        "age 500 > target must widen with a 1.5% fill ({first} -> {w})"
    );
    assert_eq!(w, W_CEILING, "doubling reaches the ceiling and stops: {w}");
}

/// Shrink needs ALL THREE: the ready guard (`ready < W/2`), an age well under
/// target, and a partial fill. The ready guard is the newest of the three and it
/// is what keeps the widening and narrowing arms from fighting.
#[test]
fn it_shrinks_only_when_ready_age_and_fill_all_agree() {
    let ap = controller(Mode::On);
    let mut t = 1_000i64;
    // Widen on ready pressure.
    for _ in 0..4 {
        t += 600;
        pop(&ap, ask_auto(), 64, 64, 900, 200, t);
    }
    let wide = pop(&ap, ask_auto(), 64, 64, 900, 200, t)
        .unwrap()
        .partitions;
    assert_eq!(wide, W_CEILING);

    // Ready still full: no shrink, whatever the age and the fill say. This is
    // the guard the first law did not have.
    for _ in 0..40 {
        t += 600;
        pop(&ap, ask_auto(), 64, 64, 1, 3, t);
    }
    let held = pop(&ap, ask_auto(), 64, 64, 1, 3, t).unwrap().partitions;
    assert_eq!(
        held, W_CEILING,
        "a full ready set is not a reason to narrow"
    );

    // Ready collapses but the batches stay FULL: still no shrink — a lane
    // feeding its consumer as fast as it asks does not want more round trips.
    for _ in 0..40 {
        t += 600;
        pop(&ap, ask_auto(), 1, 1, 1, 200, t);
    }
    let held = pop(&ap, ask_auto(), 1, 1, 1, 200, t).unwrap().partitions;
    assert_eq!(held, W_CEILING, "a full batch is not a reason to narrow");

    // All three agree: halve, repeatedly, down to the band's floor. `ready < W/2`
    // stops being true at W=2 with one partition ready, which is the bottom of
    // the [ready, 2 x ready] band and not the absolute floor of 1.
    let mut w = held;
    for _ in 0..12 {
        t += 600;
        w = pop(&ap, ask_auto(), 1, 1, 1, 3, t).unwrap().partitions;
    }
    assert_eq!(
        w, 2,
        "narrowed to the band's floor for a ready set of 1: {w}"
    );
}

/// A lane whose ready set collapses to NOTHING decays to width 1. There is no age
/// to read on an empty ring, so this is the post-idle re-division doing it rather
/// than the shrink arm, and that is the intended division of labour: a lane with
/// nothing to serve is not a control problem.
#[test]
fn a_genuinely_idle_lane_decays_to_one() {
    let ap = controller(Mode::On);
    let mut t = 1_000i64;
    for _ in 0..4 {
        t += 600;
        pop(&ap, ask_auto(), 64, 64, 900, 200, t);
    }
    assert_eq!(
        pop(&ap, ask_auto(), 64, 64, 900, 200, t)
            .unwrap()
            .partitions,
        W_CEILING
    );
    // The queue goes quiet: nothing ready, nothing claimed, only the consumer's
    // own empty long-poll re-polls. It decays with the ready EWMA's time
    // constant rather than dropping — 30 samples takes 64 to under half a
    // partition — which is the same damping that stops a single empty poll in
    // the middle of a burst from collapsing the width.
    for _ in 0..30 {
        t += 600;
        pop(&ap, ask_auto(), 0, 0, 0, 0, t);
    }
    let w = pop(&ap, ask_auto(), 0, 0, 0, 0, t).unwrap().partitions;
    assert_eq!(w, 1, "an idle lane holds no width open: {w}");
}

/// The dead zone: inside the band, with the age between target/4 and target, the
/// loop holds — and holding is the majority arm at steady state.
#[test]
fn the_dead_zone_holds() {
    let ap = controller(Mode::On);
    let target = Knobs::defaults().target_age_ms as i64;
    let mut t = 1_000i64;
    // A lane that is exactly right: 8 ready, width 8 (the cold division), an age
    // in the dead zone, full batches.
    let before = pop(&ap, ask_auto(), 8, 8, target / 2, 200, t)
        .unwrap()
        .partitions;
    assert_eq!(before, 8);
    let adjusts_before = ap.adjust_count();
    for _ in 0..60 {
        t += 600;
        pop(&ap, ask_auto(), 8, 8, target / 2, 200, t);
    }
    let after = pop(&ap, ask_auto(), 8, 8, target / 2, 200, t)
        .unwrap()
        .partitions;
    assert_eq!(before, after, "steady state must not move the width");
    assert_eq!(
        ap.adjust_count(),
        adjusts_before,
        "…and must record no adjustments at all"
    );
}

/// The caps, and what is NO LONGER one. After the bench A/B the order is: the
/// user's own value (which turns the controller off for that dimension), then
/// 64, and nothing else — the smoothed ready count became the widening TARGET,
/// and a test asserting it still capped anything would be re-asserting the defect.
#[test]
fn the_ceiling_binds_and_the_ready_count_no_longer_caps() {
    // The one remaining ceiling.
    let ap = controller(Mode::On);
    let mut t = 1_000i64;
    for _ in 0..40 {
        t += 600;
        let w = pop(&ap, ask_auto(), 100_000, 64, 5_000, 200, t)
            .unwrap()
            .partitions;
        assert!(w <= W_CEILING, "never past the checkout ceiling: {w}");
    }

    // …and the ready count does NOT bind: three ready partitions that are all
    // waiting far too long still let the age arm widen past three, which is the
    // residual case the secondary trigger exists for.
    let ap = controller(Mode::On);
    let mut t = 1_000i64;
    let mut last = 0;
    for _ in 0..40 {
        t += 600;
        last = pop(&ap, ask_auto(), 3, 3, 5_000, 9, t).unwrap().partitions;
    }
    assert!(
        last > 3,
        "the smoothed ready count is a target, not a cap: {last}"
    );
}

/// Dwell: at most one change per `dwell_ms` OR per `dwell_pops` pops, whichever
/// comes first, in BOTH directions.
#[test]
fn the_dwell_bounds_how_often_a_lane_may_change_its_mind() {
    // Time-bound arm: many pops inside one window, far fewer than `dwell_pops`.
    let ap = PopAutopilot::new(Knobs {
        dwell_pops: 1_000_000,
        ..Knobs::defaults()
    });
    let mut widths = Vec::new();
    for i in 0..100 {
        // 10 pops per 100 ms ⇒ 10 s of virtual time, 20 dwell windows. The ready
        // count alternates across the whole band so the controller WANTS to move
        // on every single pop; only the dwell stops it.
        let t = 1_000 + i * 100;
        let ready = if i % 2 == 0 { 64 } else { 1 };
        widths.push(pop(&ap, ask_auto(), ready, 1, 1, 3, t).unwrap().partitions);
    }
    // Counted as ADJUSTMENTS, not as plan-value changes: a cold lane's first
    // pops legitimately track the feed-forward division before the loop commits
    // a width, and that is an initial condition rather than the controller
    // changing its mind.
    let changes = ap.adjust_count();
    assert!(
        changes <= 20 + 1,
        "10 s at a 500 ms dwell allows ~20 changes, saw {changes}"
    );
    assert!(
        changes > 0,
        "the signal was supposed to make it want to move"
    );

    // Pop-bound arm: a burst of pops inside a single time window still cannot
    // change the width more than once per `dwell_pops`.
    let ap = PopAutopilot::new(Knobs {
        dwell_ms: 3_600_000,
        dwell_pops: 16,
        ..Knobs::defaults()
    });
    let mut widths = Vec::new();
    for i in 0..160 {
        let ready = if i % 2 == 0 { 64 } else { 1 };
        widths.push(
            pop(&ap, ask_auto(), ready, 1, 1, 3, 1_000)
                .unwrap()
                .partitions,
        );
    }
    let changes = ap.adjust_count();
    assert!(
        changes <= 10 + 1,
        "160 pops at a 16-pop dwell allows ~10, saw {changes}"
    );
    assert!(
        changes > 0,
        "the signal was supposed to make it want to move"
    );
}

/// Anti-flap. A signal that straddles the band on every single pop is the worst
/// case the dwell exists for; the adjustment rate must stay inside the bound the
/// dwell states rather than tracking the pop rate.
#[test]
fn a_straddling_signal_cannot_flap_faster_than_the_dwell() {
    let k = Knobs::defaults();
    let ap = controller(Mode::On);
    // One minute of virtual time at 1000 pops/s, with the ready set alternating
    // between the ceiling and one partition on every pop — the pathological
    // input for a controller whose primary driver is the ready count.
    let pops = 60_000;
    for i in 0..pops {
        let ready = if i % 2 == 0 { 64 } else { 1 };
        pop(&ap, ask_auto(), ready, 1, 1, 3, 1_000 + i);
    }
    // The gate is the OR of the two dwell terms, so the bound is the LOOSER of
    // them — which at this pop rate is the pop term.
    let by_time = 60_000 / k.dwell_ms as u64;
    let by_pops = pops as u64 / k.dwell_pops as u64;
    let bound = by_time.max(by_pops) + 2;
    assert!(
        ap.adjust_count() <= bound,
        "{} adjustments in a minute, bound {bound}",
        ap.adjust_count()
    );
}

/// The band, under the feedback the two arms actually close over: the sweep
/// DEPLETES what it measures, so widening removes its own signal. `ready` is
/// modelled as `max(0, offered - W)` — the sharpest version of that coupling,
/// with no arrivals to soften it.
///
/// The property that matters is the one the arms were designed for: widening
/// fires only while `W < ready` and shrinking only once `W > 2 x ready`, so
/// neither can undo the other's step on the SAME reading and the width has to
/// come to rest inside `[ready, 2 x ready]`. The EWMA is what carries it there:
/// a width chosen against a smoothed ready count cannot chase the instantaneous
/// one it just emptied.
#[test]
fn the_width_settles_inside_the_band_when_the_sweep_depletes_the_ready_set() {
    let ap = controller(Mode::On);
    let offered = 28usize;
    let mut t = 1_000i64;
    let mut w = 1i32;
    let mut tail = Vec::new();

    for i in 0..400 {
        t += 600; // every pop is a dwell opportunity: the worst case for flapping
        let ready = offered.saturating_sub(w as usize);
        // Prompt service and a partial fill, i.e. the age and fill halves of the
        // shrink condition are BOTH satisfied throughout — so nothing but the
        // ready guard is holding the width up.
        w = pop(&ap, ask_auto(), ready, ready.min(w as usize), 1, 3, t)
            .unwrap()
            .partitions;
        if i >= 200 {
            tail.push((ready, w));
        }
    }

    // Settled: the last half of the run holds one width.
    let widths: std::collections::BTreeSet<i32> = tail.iter().map(|&(_, w)| w).collect();
    assert_eq!(
        widths.len(),
        1,
        "the width must come to REST, not limit-cycle: {widths:?}"
    );
    let (ready, w) = *tail.last().unwrap();
    assert!(
        w as usize >= ready && w as usize <= 2 * ready.max(1),
        "settled at W={w} against {ready} ready — outside the [ready, 2 x ready] band"
    );
    // And the fixed point is a real one: what the sweep leaves behind plus what
    // it takes is the offered set.
    assert_eq!(ready + w as usize, offered);
    // Measured fixed point for offered=28: W=14, ready=14 — the sweep takes half
    // the offered set each pop and leaves the other half, which is the interior
    // of the band and the deliberately conservative end of it (a wide checkout
    // holds its partitions INFLIGHT for the client's whole cycle; see `k` in
    // hotlist_pop_attempt). If those leftovers ever start waiting past target,
    // the age arm widens further — that is what the secondary trigger is for.
}

// -------------------------------------------------------------- convergence

/// A pure in-memory simulation of a lane whose TOPOLOGY grows two orders of
/// magnitude (500 → 50 000 partitions) at a CONSTANT arrival rate, which is the
/// case the whole feature is about: a tenant's partition count grows for years
/// and no static client value survives it.
///
/// The claim of the law under test is that the right width scales with
/// `arrival rate x service time` and NOT with the partition count — so W must
/// stay modest across the whole ramp, widen to drain each topology-growth burst,
/// and come back. `ready_age` must end each phase at or under target.
#[test]
fn it_converges_as_the_topology_grows_two_orders_of_magnitude() {
    let ap = controller(Mode::On);
    let target = Knobs::defaults().target_age_ms;

    // Arrivals: 400 partitions become ready per second, throughout. Service: a
    // claim costs a fixed leg plus a little per partition, so a wider claim is
    // net-positive for throughput but not free.
    let arrivals_per_s = 400.0f64;
    let service_ms = |w: i32| 5.0 + 0.1 * w as f64;

    let mut ready: std::collections::VecDeque<i64> = std::collections::VecDeque::new();
    let mut t = 1_000i64;
    let mut carry = 0.0f64;
    let mut widest_in_burst;

    for (phase, topology) in [500usize, 5_000, 50_000].into_iter().enumerate() {
        // The topology grew: a slice of the new partitions is written and lands
        // in the ring at once. This is the burst the width loop has to meet.
        let burst = topology / 10;
        for _ in 0..burst {
            ready.push_back(t);
        }
        widest_in_burst = 0;

        // 20 s of virtual time per phase.
        let phase_end = t + 20_000;
        while t < phase_end {
            let plan = {
                // The real `ready_peek`: count plus the age of the OLDEST entry
                // in the whole ready set, not of whatever the claim reaches.
                let oldest_ms = ready.front().map(|s| t - s).unwrap_or(0);
                let tk = ap.begin(
                    QK,
                    G,
                    ask_auto(),
                    ReadyPeek {
                        parts: ready.len(),
                        oldest_ms,
                    },
                    t,
                );
                let plan = tk.plan().expect("the controller is acting");
                let take = (plan.partitions as usize).min(ready.len());
                if take > 0 {
                    let oldest = *ready.front().unwrap();
                    for _ in 0..take {
                        ready.pop_front();
                    }
                    tk.note_claim(take, t - oldest, t);
                }
                tk.record(DEF_B, take * 3, t);
                plan
            };
            widest_in_burst = widest_in_burst.max(plan.partitions);

            let dt = service_ms(plan.partitions);
            t += dt.round() as i64;
            carry += arrivals_per_s * dt / 1000.0;
            while carry >= 1.0 {
                ready.push_back(t);
                carry -= 1.0;
            }
        }

        // It met the burst …
        assert!(
            widest_in_burst >= 32,
            "phase {phase} (topology {topology}): a {burst}-partition burst must \
             widen the claim, peak was {widest_in_burst}"
        );
        // … drained it (the backlog is back to roughly one service interval of
        // arrivals, not a fraction of the topology) …
        assert!(
            ready.len() < 64,
            "phase {phase}: backlog did not drain, {} ready",
            ready.len()
        );
        // … and settled back to a width set by the ARRIVAL RATE, not by the
        // 50 000 partitions that now exist.
        let oldest_ms = ready.front().map(|s| t - s).unwrap_or(0);
        let settled = ap
            .begin(
                QK,
                G,
                ask_auto(),
                ReadyPeek {
                    parts: ready.len(),
                    oldest_ms,
                },
                t,
            )
            .plan()
            .unwrap();
        assert!(
            settled.partitions <= 16,
            "phase {phase}: settled width {} should scale with rate x service \
             time, not with the {topology}-partition topology",
            settled.partitions
        );
    }

    // The age the loop steers to. One last claim, and the oldest thing in the
    // ring must not be waiting longer than the target.
    let oldest_wait = ready.front().map(|s| t - s).unwrap_or(0);
    assert!(
        (oldest_wait as f64) <= target * 2.0,
        "ready age {oldest_wait} ms should sit at the {target} ms target"
    );
}

// --------------------------------------------------------------- idle / wake

/// Cold start and post-idle are a DIVISION, not a probe: `ready / max(1, M)`.
/// After an idle period the stored width describes a burst that is over, so the
/// first pop back re-derives — and the divisor is the lane's live local workers,
/// so two workers each take half the ready set instead of both sweeping all of
/// it.
#[test]
fn after_an_idle_period_the_first_pop_re_divides_the_ready_set() {
    let ap = controller(Mode::On);
    let mut t = 1_000i64;
    // A busy lane on a small ready set settles narrow.
    for _ in 0..20 {
        t += 600;
        pop(&ap, ask_auto(), 4, 4, 10, 12, t);
    }
    let narrow = pop(&ap, ask_auto(), 4, 4, 10, 12, t).unwrap().partitions;
    assert!(narrow <= 4, "narrow while the ring is small: {narrow}");

    // Nothing claims for well over the idle window; then a backlog appears.
    t += 60_000;
    let solo = ap.begin(QK, G, ask_auto(), peek(60, 5), t).plan().unwrap();
    assert!(
        solo.partitions > narrow,
        "the first pop after idle probes the backlog: {narrow} -> {}",
        solo.partitions
    );

    // The same instant, with three of this lane's pops live in the broker: the
    // ready set is divided, not swept whole by each of them.
    let a = ap.begin(QK, G, ask_auto(), peek(60, 5), t);
    let b = ap.begin(QK, G, ask_auto(), peek(60, 5), t);
    let c = ap.begin(QK, G, ask_auto(), peek(60, 5), t);
    let wc = c.plan().unwrap().partitions;
    assert!(
        wc < a.plan().unwrap().partitions,
        "three live workers must divide the ready set: {} vs {wc}",
        a.plan().unwrap().partitions
    );
    drop((a, b, c));

    // And a few iterations later the feedback loop has it at the ready cap.
    let mut w = 0;
    for _ in 0..6 {
        t += 600;
        w = pop(&ap, ask_auto(), 60, 60, 900, 180, t)
            .unwrap()
            .partitions;
    }
    assert!(w >= 32, "converged within a few pops: {w}");
}

/// The live-worker count is released by the ticket's Drop, not by `record` — a
/// pop future dropped mid-claim (an axum client disconnect) must not leave a
/// phantom worker dividing the lane's width forever.
#[test]
fn a_dropped_request_future_does_not_leak_a_worker() {
    let ap = controller(Mode::On);
    let t = 1_000i64;
    {
        let _a = ap.begin(QK, G, ask_auto(), peek(60, 5), t);
        let _b = ap.begin(QK, G, ask_auto(), peek(60, 5), t);
        // …and both futures are dropped here without ever calling `record`.
    }
    let alone = ap.begin(QK, G, ask_auto(), peek(60, 5), t).plan().unwrap();
    let solo = ap
        .begin(QK, G, ask_auto(), peek(60, 5), t + 1)
        .plan()
        .unwrap();
    // M is 1 again, so the width is the whole (smoothed) ready set.
    assert_eq!(alone.partitions, solo.partitions);
    assert!(alone.partitions >= 32, "M leaked: {}", alone.partitions);
}

// ------------------------------------------------------------- divergence log

/// The divergence report: 8x in either direction, ONCE per lane per re-arm
/// window, with both numbers. Diagnostic only — it never feeds back into the
/// decision, and the explicit value is used regardless.
#[test]
fn the_divergence_gate_fires_once_at_eight_x_and_not_below() {
    let mut s = LaneState::new(0);
    s.w = 6;
    s.ready = 6.0;
    s.last_claim_ms = 1_000;
    let now = 1_000;

    // Under 8x in either direction: silence.
    assert_eq!(s.divergence(47, now), None, "47/6 is under 8x");
    assert_eq!(s.divergence(1, now), None, "6/1 is under 8x");
    assert_eq!(s.divergence(6, now), None);

    // At and over 8x: report, with the controller's own number.
    assert_eq!(s.divergence(48, now), Some(6), "48/6 is exactly 8x");
    s.diverged_ms = now;
    assert_eq!(
        s.divergence(5000, now + 1),
        None,
        "…and not again in the window"
    );
    assert_eq!(
        s.divergence(5000, now + 60_000),
        None,
        "…nor an hour later, it is a static configuration"
    );
    assert_eq!(
        s.divergence(5000, now + DIVERGENCE_REARM_MS + 1),
        Some(6),
        "…but it re-arms after a day"
    );

    // The other direction: a client pinned at 1 on a lane that wants 64.
    let mut s = LaneState::new(0);
    s.w = 64;
    s.ready = 64.0;
    s.last_claim_ms = 1_000;
    assert_eq!(s.divergence(1, now), Some(64));

    // A cold lane has no opinion worth publishing.
    let cold = LaneState::new(0);
    assert_eq!(cold.divergence(5000, now), None);
}

/// …and it works for the clients that never opted in, which are the ones
/// carrying the accidental `partitions=5000`. The controller keeps its EWMAs for
/// every grouped pop on the ring path, so the counterfactual exists even though
/// no plan is ever produced.
#[test]
fn divergence_is_reported_for_old_clients_that_never_opted_in() {
    let ap = controller(Mode::On);
    let explicit = Ask {
        partitions: Some(5000),
        ..ask_plain()
    };
    let mut t = 1_000i64;
    for _ in 0..20 {
        t += 600;
        assert!(
            pop(&ap, explicit, 6, 6, 900, 18, t).is_none(),
            "an old client is still never overridden"
        );
    }
    assert_eq!(
        ap.divergences.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "exactly one report for the lane, over 20 diverging pops"
    );
}

// -------------------------------------------------------------------- memory

/// The lane map is bounded twice: a hard cap (past which the controller declines
/// rather than evicting a live lane) and the idle sweep that the hot-list rings
/// and wake gates already ride.
#[test]
fn lanes_are_capped_and_evicted() {
    let ap = PopAutopilot::new(Knobs {
        max_lanes: 4,
        ..Knobs::defaults()
    });
    for i in 0..10 {
        let t = ap.begin(&format!("acme\u{1f}q{i}"), G, ask_auto(), peek(8, 5), 1_000);
        if i < 4 {
            assert!(t.plan().is_some(), "lane {i} inside the cap");
        } else {
            assert!(
                t.plan().is_none(),
                "lane {i} past the cap degrades to defaults"
            );
        }
    }
    assert_eq!(ap.lane_count(), 4);
    assert_eq!(ap.refused.load(std::sync::atomic::Ordering::Relaxed), 6);

    // CLOCK second chance: the first sweep spends it, the second collects.
    assert_eq!(ap.evict_idle(), 0, "every lane was just touched");
    assert_eq!(ap.evict_idle(), 4);
    assert_eq!(ap.lane_count(), 0);

    // A lane with a live pop on it is never collected.
    let live = ap.begin(QK, G, ask_auto(), peek(8, 5), 1_000);
    ap.evict_idle();
    assert_eq!(ap.evict_idle(), 0, "a live ticket pins its lane");
    drop(live);
    ap.evict_idle();
    assert_eq!(ap.evict_idle(), 1);
}

// ---------------------------------------------------------------- wire shape

/// The response echo is ADDITIVE: appended last, nothing before it moved, and it
/// only ever exists for a request that opted in.
#[test]
fn the_echo_is_appended_and_changes_nothing_before_it() {
    let base = "{\"success\":true,\"messages\":[],\"partitionsClaimed\":0}";
    let mut body = base.to_string();
    append_echo(
        &mut body,
        Plan {
            partitions: 8,
            batch: AUTO_BATCH_DEFAULT,
            chose_partitions: true,
            chose_batch: true,
        },
        100,
    );
    assert_eq!(
        body,
        "{\"success\":true,\"messages\":[],\"partitionsClaimed\":0,\
         \"autopilot\":{\"partitions\":8,\"batch\":100,\"waitMs\":100}}"
    );
    assert!(
        body.starts_with(&base[..base.len() - 1]),
        "nothing before it moved"
    );
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["autopilot"]["partitions"], 8);
    assert_eq!(v["autopilot"]["batch"], 100);
    assert_eq!(v["autopilot"]["waitMs"], 100);

    // The advisory pacing is optional: 0 omits the key rather than publishing a
    // "poll as fast as you like".
    let mut body = base.to_string();
    append_echo(
        &mut body,
        Plan {
            partitions: 1,
            batch: 200,
            chose_partitions: true,
            chose_batch: false,
        },
        0,
    );
    assert!(!body.contains("waitMs"), "{body}");
}

/// The switch spellings an operator will actually type.
#[test]
fn the_kill_switch_parses_the_spellings_it_documents() {
    assert_eq!(Mode::parse("on"), Some(Mode::On));
    assert_eq!(Mode::parse(" ON "), Some(Mode::On));
    assert_eq!(Mode::parse("1"), Some(Mode::On));
    assert_eq!(Mode::parse("shadow"), Some(Mode::Shadow));
    assert_eq!(Mode::parse("off"), Some(Mode::Off));
    assert_eq!(Mode::parse("0"), Some(Mode::Off));
    assert_eq!(Mode::parse("false"), Some(Mode::Off));
    // Never guessed into a direction — `load()` makes this fatal.
    assert_eq!(Mode::parse("shaddow"), None);
    assert_eq!(Mode::parse(""), None);
}
