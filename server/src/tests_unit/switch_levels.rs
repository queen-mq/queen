//! THE THREE RUNGS OF THE SWITCH, and the exact answer each one owes
//! (PLAN_KV_TIMERS.md §9.5, §12.1). Written before `src/switches.rs`.
//!
//! The three are deliberately three different answers, and every one of them
//! carries a distinct instruction to the client:
//!
//! | rung | who decides | answer | what the client should do |
//! |---|---|---|---|
//! | runtime kill switch | the operator, live | **503** + `Retry-After` | temporary, come back |
//! | no grant / not on the plan | the control plane | **403** `feature_gated` | change something, then retry |
//! | occupancy over the cap | the tenant's own data | **403** `kv_quota_exceeded` | free space, then retry |
//!
//! THERE USED TO BE A FOURTH, ABOVE ALL OF THEM, and its absence is now the
//! thing worth testing: a boot flag that answered **404**, "this cell has no such
//! surface". `QUEEN_KV_ENABLED` and `QUEEN_TIMERS_ENABLED` are gone, so no rung
//! can produce that answer any more and `Answer` has no `Gone`. Every cell that
//! runs this binary has both surfaces; what an operator can still do is PAUSE
//! one, which is 503 and invites the retry that 404 forbade.
//!
//! WHY THE WIRE ANSWERS DIFFERENTLY FROM THE ROUTES for the same rung (§12.1
//! stage 7): on `/api/v1/kv` a paused cell is 503 and the client retries, but
//! inside `/api/v1/transaction` the `kv` array is refused with a PERMANENT
//! error, because a transaction that retries a paused KV forever is a client
//! spinning on the hot path of the product, and the bundle it carries has
//! messages in it.

use super::*;

const T: &str = "11111111-1111-1111-1111-111111111111";

fn quotas() -> Quotas {
    let q = Quotas::for_test(TestKnobs::default());
    q.refresh(vec![]);
    q
}

fn full_quotas() -> Quotas {
    let q = Quotas::for_test(TestKnobs::default());
    q.refresh(vec![TenantRow {
        tenant: T.to_string(),
        limits: Some(Limits {
            enabled: true,
            max_rows: Some(10),
            max_timers: Some(10),
            ..Limits::default()
        }),
        measure: Measure {
            kv_rows: 10,
            timer_rows: 10,
            ..Measure::default()
        },
    }]);
    q
}

/// A FRESH PROCESS SERVES EVERY SURFACE. This is what replaces the old rung-one
/// test: there is no configuration, no argument to `Switches` and no
/// environment variable that can produce a broker where one of these is missing,
/// which is the entire content of "kv and timers are not features".
#[test]
fn a_fresh_broker_allows_every_surface() {
    let sw = Switches::for_test();
    let q = quotas();
    for s in [
        Surface::KvRead,
        Surface::KvWrite,
        Surface::TimerSchedule,
        Surface::TimerCancel,
        Surface::TimerRead,
    ] {
        assert_eq!(
            decide(&sw, &q, T, s, 1, 1),
            Answer::Allow,
            "{s:?} must be live on a broker nobody has configured"
        );
    }
}

/// NOTHING ANSWERS 404 ANY MORE. The ladder's whole range is 503 and 403, so an
/// SDK never has to distinguish "this cell does not have the surface" from "an
/// operator paused it" — the first case cannot happen.
#[test]
fn no_rung_can_answer_not_found() {
    let sw = Switches::for_test();
    sw.set_kv(false);
    sw.set_timers_schedule(false);
    let full = full_quotas();
    for origin in [Origin::Route, Origin::Wire] {
        for s in [
            Surface::KvRead,
            Surface::KvWrite,
            Surface::TimerSchedule,
            Surface::TimerCancel,
            Surface::TimerRead,
        ] {
            if let Some(h) = decide(&sw, &full, T, s, 1, 1).http(origin, s) {
                assert_ne!(h.status, 404, "{s:?} on {origin:?} answered 404");
            }
        }
    }
}

/// RUNG 1 — the runtime kill switch. 503 with a `Retry-After`, on the routes.
#[test]
fn rung_one_runtime_off_is_503_with_retry_after() {
    let sw = Switches::for_test();
    sw.set_kv(false);
    let q = quotas();
    let a = decide(&sw, &q, T, Surface::KvWrite, 1, 1);
    assert_eq!(a, Answer::Paused);
    let h = a
        .http(Origin::Route, Surface::KvWrite)
        .expect("Paused must have a status");
    assert_eq!(h.status, 503);
    assert_eq!(h.code, "kv_disabled");
    assert!(h.retry_after.is_some_and(|s| s >= 1), "a 503 owes the client a delay");
}

/// …and the SAME rung is PERMANENT on the transaction wire (§12.1 stage 7), so
/// a bundle carrying a `kv` array does not retry in a loop against a cell that
/// an operator has deliberately paused.
#[test]
fn rung_one_is_permanent_on_the_transaction_wire() {
    let sw = Switches::for_test();
    sw.set_kv(false);
    let q = quotas();
    let h = decide(&sw, &q, T, Surface::KvWrite, 1, 1)
        .http(Origin::Wire, Surface::KvWrite)
        .expect("a paused wire must answer");
    assert_eq!(h.status, 403, "the wire must not invite an infinite retry");
    assert_eq!(h.code, "kv_disabled");
    assert_eq!(h.retry_after, None);
}

/// RUNG 2 — no grant. 403 `feature_gated`, which is "this will not work until
/// something about your plan changes", and is exactly what an unconfigured
/// tenant on a cell with tenancy on must hear (§9.4 point 1).
#[test]
fn rung_two_no_grant_is_403_feature_gated() {
    let sw = Switches::for_test();
    let q = Quotas::for_test(TestKnobs {
        require_grant: true,
        ..TestKnobs::default()
    });
    q.refresh(vec![]);
    let h = decide(&sw, &q, T, Surface::KvWrite, 1, 1)
        .http(Origin::Route, Surface::KvWrite)
        .expect("an ungranted tenant must be refused");
    assert_eq!(h.status, 403);
    assert_eq!(h.code, "feature_gated");
    assert_eq!(h.retry_after, None, "403 means no delay resolves it");
}

/// RUNG 3 — occupancy. 403 with the resource named, and never a 429: a
/// `Retry-After` on a row quota is a lie, and it would make the client retry in
/// a loop exactly when the tenant is already over.
#[test]
fn rung_three_over_quota_is_403_with_the_resource_named() {
    let sw = Switches::for_test();
    let q = full_quotas();
    let h = decide(&sw, &q, T, Surface::KvWrite, 1, 1)
        .http(Origin::Route, Surface::KvWrite)
        .expect("a full tenant must be refused");
    assert_eq!(h.status, 403);
    assert_eq!(h.code, "kv_quota_exceeded");
    assert_eq!(h.retry_after, None);

    let h = decide(&sw, &q, T, Surface::TimerSchedule, 0, 0)
        .http(Origin::Route, Surface::TimerSchedule)
        .expect("a full tenant must be refused");
    assert_eq!(h.status, 403);
    assert_eq!(h.code, "timers_quota_exceeded");
}

/// THE ORDER OF THE RUNGS IS ITSELF A CONTRACT. A tenant that is BOTH over quota
/// and on a paused cell hears "paused", not "over quota": the answer names the
/// OUTERMOST reason, because that is the one the caller can act on, and because
/// leaking "you are over quota" past an operator's switch tells an
/// unauthenticated prober that the tenant exists.
#[test]
fn the_outermost_rung_wins() {
    let q = full_quotas();

    let paused = Switches::for_test();
    paused.set_kv(false);
    assert_eq!(decide(&paused, &q, T, Surface::KvWrite, 1, 1), Answer::Paused);

    // …and with the switch back on, the rung below it speaks.
    paused.set_kv(true);
    assert!(matches!(
        decide(&paused, &q, T, Surface::KvWrite, 1, 1),
        Answer::Refused(_)
    ));
}

// ---------------------------------------------------------------------------
// What must NEVER be blocked, at any rung.
// ---------------------------------------------------------------------------

/// §9.6 — the cancel route. The fire never switches itself off, so a tenant that
/// cannot cancel keeps producing messages it cannot stop until the horizon or
/// an operator. EVERY rung must let it through, with no exception left: the one
/// thing that used to stop a cancel was the boot flag, and there is no boot flag.
#[test]
fn a_cancel_survives_every_rung() {
    let q = full_quotas();
    let sw = Switches::for_test();
    sw.set_timers_schedule(false);
    sw.set_timers_fire(false);
    assert_eq!(decide(&sw, &q, T, Surface::TimerCancel, 0, 0), Answer::Allow);

    let strict = Quotas::for_test(TestKnobs {
        require_grant: true,
        ..TestKnobs::default()
    });
    strict.refresh(vec![]);
    assert_eq!(decide(&sw, &strict, T, Surface::TimerCancel, 0, 0), Answer::Allow);
}

/// §9.5 — reads and deletes are always permitted, or a full tenant can never
/// find out what to free.
#[test]
fn reads_survive_the_quota_rung() {
    let q = full_quotas();
    let sw = Switches::for_test();
    assert_eq!(decide(&sw, &q, T, Surface::KvRead, 0, 0), Answer::Allow);
    assert_eq!(decide(&sw, &q, T, Surface::TimerRead, 0, 0), Answer::Allow);
}

/// …but a read does NOT survive the runtime kill switch: stage 7 of §12.1 says
/// "503 su tutte le rotte KV", and the switch exists precisely to take load off
/// a cell, which reads are part of.
#[test]
fn reads_do_not_survive_the_kill_switch() {
    let sw = Switches::for_test();
    sw.set_kv(false);
    let q = quotas();
    assert_eq!(decide(&sw, &q, T, Surface::KvRead, 0, 0), Answer::Paused);
}

// ---------------------------------------------------------------------------
// §12.1 — the two timer switches are DISTINCT, "perche' le due meta' hanno
// costi opposti: fermare lo schedule e' innocuo e istantaneo, fermare il fuoco
// accumula lavoro promesso".
// ---------------------------------------------------------------------------

#[test]
fn the_schedule_switch_does_not_stop_the_fire() {
    let sw = Switches::for_test();
    sw.set_timers_schedule(false);
    assert!(
        sw.fire_allowed(),
        "pausing the schedule must never pause the fire: the promised work is \
         already promised, and not delivering it reads to a customer as loss"
    );
}

#[test]
fn the_fire_switch_does_not_stop_the_schedule() {
    let sw = Switches::for_test();
    sw.set_timers_fire(false);
    assert!(!sw.fire_allowed());
    let q = quotas();
    assert_eq!(decide(&sw, &q, T, Surface::TimerSchedule, 0, 0), Answer::Allow);
}

/// The fire switch is the ONLY thing that can stop the fire, and only an
/// operator can flip it: no automatic rung of the degradation ladder may
/// (§12.1, "il fuoco dei timer non si spegne mai automaticamente").
#[test]
fn the_fire_is_on_by_default_and_only_an_operator_turns_it_off() {
    let sw = Switches::for_test();
    assert!(sw.fire_allowed(), "a fresh cell fires");
    // Nothing at construction can produce a cell that boots not firing: `new`
    // takes no argument, which is what keeps this a kill switch and not a gate.
    sw.set_timers_fire(false);
    assert!(!sw.fire_allowed(), "and only this call stops it");
    sw.set_timers_fire(true);
    assert!(sw.fire_allowed(), "and it is expected to be turned back on");
}

// ---------------------------------------------------------------------------
// The DB mirror. §12.1: "flag in-process autorevole sul percorso caldo, riga in
// DB come specchio best-effort per propagazione e restart".
// ---------------------------------------------------------------------------

/// An ABSENT row means ON. This is the difference between a kill switch and a
/// feature flag, and getting it backwards would mean every fresh cell boots
/// with the feature dead and no row to explain why.
#[test]
fn an_absent_row_means_enabled() {
    let sw = Switches::for_test();
    sw.adopt("kv_enabled", None);
    sw.adopt("timers_schedule_enabled", None);
    sw.adopt("timers_fire_enabled", None);
    let q = quotas();
    assert_eq!(decide(&sw, &q, T, Surface::KvWrite, 1, 1), Answer::Allow);
    assert_eq!(decide(&sw, &q, T, Surface::TimerSchedule, 0, 0), Answer::Allow);
    assert!(sw.fire_allowed());
}

/// The keys are the plan's, spelled out, because an operator types them into a
/// `psql` prompt during an incident and a rename is a silent no-op.
#[test]
fn the_system_state_keys_are_the_ones_the_plan_names() {
    assert_eq!(Switches::KEY_KV, "kv_enabled");
    assert_eq!(Switches::KEY_TIMERS_SCHEDULE, "timers_schedule_enabled");
    assert_eq!(Switches::KEY_TIMERS_FIRE, "timers_fire_enabled");
    let sw = Switches::for_test();
    sw.adopt(Switches::KEY_KV, Some(false));
    assert!(!sw.kv_on());
    sw.adopt(Switches::KEY_TIMERS_FIRE, Some(false));
    assert!(!sw.fire_allowed());
    // An unknown key is ignored rather than panicking: the row is a mirror, and
    // a typo in a mirror must not take a broker down.
    sw.adopt("kv_enbaled", Some(false));
    assert!(!sw.kv_on());
}

// ===========================================================================
// THE SAME THREE RUNGS FOR THE EPHEMERAL FAMILY (EPHEMERAL_QUEUES.md §1.6, M7)
// ===========================================================================
//
// A separate entry point (`decide_ephemeral`) because its third rung reads an
// authority that is not `Quotas` — the broker IS the meter for a class that
// lives in its own heap — but the SAME contract: same order, same `Answer`, same
// rendering. These cases exist to pin that "same" rather than to trust it.

fn eph(require_grant: bool) -> std::sync::Arc<crate::ephemeral::Ephemeral> {
    crate::ephemeral::Ephemeral::new(
        crate::ephemeral::Knobs { require_grant, ..crate::ephemeral::Knobs::defaults() },
        std::sync::Arc::new(crate::metrics::Metrics::new()),
    )
}

const EPH_SURFACES: [Surface; 4] =
    [Surface::EphPush, Surface::EphPop, Surface::EphAck, Surface::EphAdmin];

/// A fresh broker serves every ephemeral surface, and — with no grant required —
/// serves them to a tenant nobody has ever configured. That is the OSS posture of
/// M9 stated as a test: no boot flag, no row, no setup.
#[test]
fn a_fresh_broker_allows_every_ephemeral_surface() {
    let sw = Switches::for_test();
    let e = eph(false);
    for s in EPH_SURFACES {
        assert_eq!(decide_ephemeral(&sw, &e, T, s, 1), Answer::Allow, "{s:?} was refused");
    }
}

/// NO RUNG MAY ANSWER 404, on this family either. Its routes are registered
/// unconditionally (M9), so a 404 from anywhere under `/api/v1/ephemeral` can
/// only ever mean "this broker predates the feature" — which is exactly what the
/// SDKs map it to, and what a paused surface must never be confused with.
#[test]
fn no_ephemeral_rung_can_answer_not_found() {
    for require_grant in [false, true] {
        for paused in [false, true] {
            let sw = Switches::for_test();
            if paused {
                sw.set_ephemeral(false);
            }
            let e = eph(require_grant);
            for s in EPH_SURFACES {
                if let Some(h) = decide_ephemeral(&sw, &e, T, s, 1).http(Origin::Route, s) {
                    assert_ne!(h.status, 404, "{s:?} answered 404");
                }
            }
        }
    }
}

/// Rung 1: the operator's switch is 503 with a `Retry-After`, and it names the
/// EPHEMERAL family — a paused RAM surface answering `kv_disabled` would send an
/// operator to the wrong runbook.
#[test]
fn ephemeral_rung_one_is_503_with_its_own_code() {
    let sw = Switches::for_test();
    sw.set_ephemeral(false);
    let e = eph(false);
    for s in EPH_SURFACES {
        let h = decide_ephemeral(&sw, &e, T, s, 1)
            .http(Origin::Route, s)
            .expect("a paused surface must refuse");
        assert_eq!(h.status, 503, "{s:?}");
        assert_eq!(h.code, "ephemeral_disabled", "{s:?}");
        assert_eq!(h.retry_after, Some(1), "{s:?}");
    }
    // And the kv switch is independent: pausing one family must not pause the
    // other, which is the whole reason there are three keys and not one.
    assert!(sw.kv_on());
}

/// Rung 2: with `require_grant` on, the ABSENCE of a row is a denial and not a
/// permission — the `config.rs:1210` posture, applied to a class whose overrun is
/// paid in RAM. 403 `feature_gated`, and never a `Retry-After`: waiting does not
/// help, the control plane has to write a row.
#[test]
fn ephemeral_rung_two_no_grant_is_403_feature_gated() {
    let sw = Switches::for_test();
    let e = eph(true);
    for s in EPH_SURFACES {
        let h = decide_ephemeral(&sw, &e, T, s, 1)
            .http(Origin::Route, s)
            .expect("an ungranted tenant must be refused");
        assert_eq!(h.status, 403, "{s:?}");
        assert_eq!(h.code, "feature_gated", "{s:?}");
        assert_eq!(h.retry_after, None, "{s:?}");
    }
}

/// A grant row admits, and `enabled:false` on the row refuses again. The two are
/// tested together because collapsing them — treating a disabled row as no row,
/// or the reverse — is the mistake `quota.rs` calls out by name: it would either
/// deny a tenant the operator never restricted or admit one they never
/// authorised.
#[test]
fn an_ephemeral_grant_row_admits_and_a_disabled_one_does_not() {
    let sw = Switches::for_test();
    let e = eph(true);
    e.apply_grants(vec![crate::ephemeral::Grant {
        tenant: T.to_string(),
        enabled: true,
        max_bytes: None,
        max_queues: None,
        max_msgs_per_sec: None,
    }]);
    assert_eq!(decide_ephemeral(&sw, &e, T, Surface::EphPush, 1), Answer::Allow);

    e.apply_grants(vec![crate::ephemeral::Grant {
        tenant: T.to_string(),
        enabled: false,
        max_bytes: None,
        max_queues: None,
        max_msgs_per_sec: None,
    }]);
    let h = decide_ephemeral(&sw, &e, T, Surface::EphPush, 1)
        .http(Origin::Route, Surface::EphPush)
        .expect("a disabled grant refuses");
    assert_eq!((h.status, h.code), (403, "feature_gated"));

    // A row that DISAPPEARS revokes. Deleting the row is how the control plane
    // ends a plan, and a broker that kept the last grant it saw would keep
    // serving a customer who is no longer entitled.
    e.apply_grants(vec![]);
    let h = decide_ephemeral(&sw, &e, T, Surface::EphPush, 1)
        .http(Origin::Route, Surface::EphPush)
        .expect("a revoked grant refuses");
    assert_eq!((h.status, h.code), (403, "feature_gated"));
}

/// Rung 3: the per-tenant MESSAGE rate, and it is a rate over messages rather
/// than over calls — a bucket that spent one token per request would be defeated
/// by a single batch on a class that encourages batching.
#[test]
fn ephemeral_rung_three_is_a_message_rate_and_answers_429() {
    let sw = Switches::for_test();
    let e = crate::ephemeral::Ephemeral::new(
        crate::ephemeral::Knobs {
            rate: 1,
            burst: 4,
            ..crate::ephemeral::Knobs::defaults()
        },
        std::sync::Arc::new(crate::metrics::Metrics::new()),
    );
    // One call, four messages: the whole burst in a single request.
    assert_eq!(decide_ephemeral(&sw, &e, T, Surface::EphPush, 4), Answer::Allow);
    let h = decide_ephemeral(&sw, &e, T, Surface::EphPush, 4)
        .http(Origin::Route, Surface::EphPush)
        .expect("the second batch is over the burst");
    assert_eq!(h.status, 429);
    assert_eq!(h.code, "rate_limited");
    // A `Retry-After` that is honest, and never zero: zero reads as "immediately"
    // and produces a spin against a cell that has just said no.
    assert!(h.retry_after.unwrap_or(0) >= 1);

    // THE POP AND THE ACK ARE NOT ON THIS RUNG. They are the only ways for a
    // tenant that has filled its allowance to get back under it, so refusing
    // them would be the self-defeating shape §9.5 forbids for kv deletes.
    assert_eq!(decide_ephemeral(&sw, &e, T, Surface::EphPop, 0), Answer::Allow);
    assert_eq!(decide_ephemeral(&sw, &e, T, Surface::EphAck, 0), Answer::Allow);
}

/// THE ORDER IS THE CONTRACT: the answer names the OUTERMOST reason. A tenant
/// that is both ungranted and on a paused cell hears `ephemeral_disabled`, not
/// `feature_gated` — otherwise a prober learns that a tenant exists from a rung
/// that a switch an operator pulled should have hidden (§13.5).
#[test]
fn the_outermost_ephemeral_rung_wins() {
    let sw = Switches::for_test();
    sw.set_ephemeral(false);
    let e = eph(true);
    let h = decide_ephemeral(&sw, &e, T, Surface::EphPush, 1)
        .http(Origin::Route, Surface::EphPush)
        .expect("refused");
    assert_eq!(h.code, "ephemeral_disabled", "the switch must speak before the grant");
}

/// The ephemeral key is the plan's, and it adopts like the other three: an
/// ABSENT row means ON, which is what keeps a fresh cell from booting with the
/// surface dead and no row to explain why.
#[test]
fn the_ephemeral_system_state_key_adopts_like_the_others() {
    assert_eq!(Switches::KEY_EPHEMERAL, "ephemeral_enabled");
    let sw = Switches::for_test();
    sw.adopt(Switches::KEY_EPHEMERAL, Some(false));
    assert!(!sw.eph_on());
    sw.adopt(Switches::KEY_EPHEMERAL, None);
    assert!(sw.eph_on(), "an absent row means ON");
    // And it is independent of the other three.
    assert!(sw.kv_on() && sw.timers_schedule_on() && sw.fire_allowed());
}
