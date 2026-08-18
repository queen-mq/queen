//! The quota gate's semantics (PLAN_KV_TIMERS.md §9.4, §9.5, §9.6, §12.1).
//!
//! Written before `src/quota.rs`, so this file DEFINES the surface: if the
//! implementation names something differently it does not compile, and the fix
//! is the implementation.
//!
//! Each test pins one sentence of the plan, and the sentence is quoted in the
//! doc comment so a future reader can tell an intentional rule from an accident.

use super::*;

const T: &str = "11111111-1111-1111-1111-111111111111";
const OTHER: &str = "22222222-2222-2222-2222-222222222222";

fn limits(max_rows: Option<i64>, max_bytes: Option<i64>, max_timers: Option<i64>) -> Limits {
    Limits {
        enabled: true,
        max_rows,
        max_bytes,
        max_timers,
        ..Limits::default()
    }
}

fn row(tenant: &str, l: Option<Limits>, m: Measure) -> TenantRow {
    TenantRow {
        tenant: tenant.to_string(),
        limits: l,
        measure: m,
    }
}

fn measure(kv_rows: i64, kv_bytes: i64, timer_rows: i64) -> Measure {
    Measure {
        kv_rows,
        kv_bytes,
        timer_rows,
        computed_at_ms: 0,
    }
}

// ---------------------------------------------------------------------------
// §9.5 — "Le letture e le DELETE sono sempre permesse, anche sopra quota,
// altrimenti un tenant pieno non puo' liberarsi."
// ---------------------------------------------------------------------------

/// A tenant far over its row quota still reads. This is not a nicety: the
/// idempotency-marker use case turns a refused read into a repeated external
/// side effect, and a tenant who cannot read cannot find what to delete.
#[test]
fn reads_are_allowed_over_quota() {
    let q = Quotas::for_test(TestKnobs::default());
    q.refresh(vec![row(T, Some(limits(Some(10), None, None)), measure(1_000_000, 0, 0))]);
    assert_eq!(q.check_kv_read(T), Verdict::Allow);
}

/// And so are deletes — the only way out of a full tenant. A delete is a write
/// in SQL and a RELEASE in the quota's terms, and confusing the two is how a
/// full tenant becomes a permanently full tenant.
#[test]
fn deletes_are_allowed_over_quota() {
    let q = Quotas::for_test(TestKnobs::default());
    q.refresh(vec![row(T, Some(limits(Some(10), None, None)), measure(1_000_000, 0, 0))]);
    assert_eq!(q.charge_kv_write(T, 0, 0), Verdict::Allow);
}

/// §9.6 — "La cancel dei timer non e' mai bloccabile." A tenant over its timer
/// quota that could not cancel would keep firing messages it cannot stop, until
/// the horizon or an operator: the block would produce the opposite of its
/// purpose, because the fire never switches itself off (§12).
#[test]
fn timer_cancels_are_never_blocked() {
    let q = Quotas::for_test(TestKnobs::default());
    q.refresh(vec![row(T, Some(limits(None, None, Some(1))), measure(0, 0, 500_000))]);
    assert_eq!(q.check_timers_cancel(T), Verdict::Allow);
    // Even for a tenant with no grant at all, on a cell that requires one.
    let strict = Quotas::for_test(TestKnobs {
        require_grant: true,
        ..TestKnobs::default()
    });
    strict.refresh(vec![]);
    assert_eq!(strict.check_timers_cancel(T), Verdict::Allow);
}

// ---------------------------------------------------------------------------
// §9.5 — occupancy over the cap is 403, and which resource it was must reach
// the client, because the two have different remedies.
// ---------------------------------------------------------------------------

#[test]
fn rows_bytes_and_timers_are_three_separate_verdicts() {
    let q = Quotas::for_test(TestKnobs::default());
    q.refresh(vec![row(
        T,
        Some(limits(Some(100), Some(1000), Some(10))),
        measure(100, 0, 0),
    )]);
    assert_eq!(q.charge_kv_write(T, 1, 0), Verdict::OverQuota(Resource::KvRows));

    let q = Quotas::for_test(TestKnobs::default());
    q.refresh(vec![row(
        T,
        Some(limits(Some(100), Some(1000), Some(10))),
        measure(0, 1000, 0),
    )]);
    assert_eq!(q.charge_kv_write(T, 1, 1), Verdict::OverQuota(Resource::KvBytes));

    let q = Quotas::for_test(TestKnobs::default());
    q.refresh(vec![row(
        T,
        Some(limits(Some(100), Some(1000), Some(10))),
        measure(0, 0, 10),
    )]);
    assert_eq!(q.charge_timers(T, 1), Verdict::OverQuota(Resource::Timers));
}

/// The status taxonomy of §9.5 is closed, and the mapping from verdict to
/// (status, code) lives in ONE place so the HTTP surface and the transaction
/// wire cannot answer differently for the same verdict.
///
///   429 = retry later and it will work.
///   403 = retry all you like, it will not, until something changes.
///   503 = not your fault, it is the cell.
#[test]
fn the_verdict_to_status_mapping_is_the_taxonomy_of_the_plan() {
    assert_eq!(Verdict::Allow.http(), None);
    assert_eq!(
        Verdict::OverQuota(Resource::KvRows).http(),
        Some((403, "kv_quota_exceeded"))
    );
    assert_eq!(
        Verdict::OverQuota(Resource::KvBytes).http(),
        Some((403, "kv_quota_exceeded"))
    );
    assert_eq!(
        Verdict::OverQuota(Resource::Timers).http(),
        Some((403, "timers_quota_exceeded"))
    );
    assert_eq!(Verdict::NotGranted.http(), Some((403, "feature_gated")));
    assert_eq!(Verdict::RateLimited(1).http(), Some((429, "rate_limited")));
    assert_eq!(Verdict::NoRoom.http(), Some((503, "kv_unavailable")));
    // 507 is deliberately absent: it is WebDAV, no client treats it specially,
    // and the proxy already answers 403 for the same concept (§9.5).
    for v in [
        Verdict::OverQuota(Resource::KvRows),
        Verdict::OverQuota(Resource::Timers),
        Verdict::NotGranted,
    ] {
        assert_ne!(v.http().map(|(s, _)| s), Some(507));
    }
}

// ---------------------------------------------------------------------------
// §9.4 point 1 — with tenancy ON the ABSENCE of a quota row is a DENIAL, and
// with tenancy OFF it is a permission. `server/src/tenant.rs:16-19` is explicit
// that a tenant id is opaque and validated against nothing, so a fail-open
// default means an unlimited key space for an invented id.
// ---------------------------------------------------------------------------

#[test]
fn an_absent_row_denies_when_a_grant_is_required() {
    let q = Quotas::for_test(TestKnobs {
        require_grant: true,
        ..TestKnobs::default()
    });
    q.refresh(vec![row(OTHER, Some(limits(None, None, None)), measure(0, 0, 0))]);
    assert_eq!(q.charge_kv_write(T, 1, 10), Verdict::NotGranted);
    assert_eq!(q.charge_timers(T, 1), Verdict::NotGranted);
    // …and the granted tenant next to it is unaffected: NULL is still unlimited.
    assert_eq!(q.charge_kv_write(OTHER, 1, 10), Verdict::Allow);
}

/// With the tenancy header off — every self-hosted deployment — the feature must
/// work with nothing configured. `QUEEN_KV_REQUIRE_GRANT` is DERIVED from the
/// tenancy flag for exactly this reason (§9.4 point 1): the operator should
/// never have to discover why they must configure something.
#[test]
fn an_absent_row_permits_when_no_grant_is_required() {
    let q = Quotas::for_test(TestKnobs::default());
    q.refresh(vec![]);
    assert_eq!(q.charge_kv_write(T, 1, 10), Verdict::Allow);
    assert_eq!(q.charge_timers(T, 1), Verdict::Allow);
}

/// An explicit `enabled = false` row is a denial on every cell, grant required
/// or not: it is the control plane saying "not on this plan", and that is what
/// `feature_gated` means.
#[test]
fn a_disabled_row_is_a_denial_everywhere() {
    let q = Quotas::for_test(TestKnobs::default());
    q.refresh(vec![row(
        T,
        Some(Limits {
            enabled: false,
            ..Limits::default()
        }),
        measure(0, 0, 0),
    )]);
    assert_eq!(q.charge_kv_write(T, 1, 10), Verdict::NotGranted);
    // Reads and cancels survive it: a tenant whose plan lost the feature must
    // still be able to see and free what it already wrote.
    assert_eq!(q.check_kv_read(T), Verdict::Allow);
    assert_eq!(q.check_timers_cancel(T), Verdict::Allow);
}

// ---------------------------------------------------------------------------
// §9.4 point 2 — "Le mappe per tenant in RAM sono size-capped, e il cap NEGA,
// non sfratta." Eviction under an id-rotation attack would recycle the map
// forever and grant every rotated id a fresh bucket and a fresh delta; denying
// makes the attack visible and bounded.
// ---------------------------------------------------------------------------

#[test]
fn the_live_map_cap_denies_and_does_not_evict() {
    let q = Quotas::for_test(TestKnobs {
        cap: 2,
        ..TestKnobs::default()
    });
    q.refresh(vec![]);
    assert_eq!(q.charge_kv_write("a", 1, 1), Verdict::Allow);
    assert_eq!(q.charge_kv_write("b", 1, 1), Verdict::Allow);
    // Third distinct tenant: refused, and NOT at the price of forgetting `a`.
    assert_eq!(q.charge_kv_write("c", 1, 1), Verdict::NoRoom);
    assert_eq!(q.live_len(), 2);
    assert_eq!(q.delta_of("a"), (1, 1, 0), "an existing tenant was evicted");
    // An id already in the map keeps working — the cap bounds the map, it does
    // not become a global stop.
    assert_eq!(q.charge_kv_write("a", 1, 1), Verdict::Allow);
}

/// A tenant that HAS a quota row is never refused for room: the map is bounded
/// by the control plane for it, and the attack the cap defends against is an
/// unvalidated id, not a configured one.
#[test]
fn a_granted_tenant_is_never_refused_for_room() {
    let q = Quotas::for_test(TestKnobs {
        cap: 1,
        ..TestKnobs::default()
    });
    q.refresh(vec![row(T, Some(limits(None, None, None)), measure(0, 0, 0))]);
    assert_eq!(q.charge_kv_write("junk-1", 1, 1), Verdict::Allow);
    assert_eq!(q.charge_kv_write("junk-2", 1, 1), Verdict::NoRoom);
    assert_eq!(q.charge_kv_write(T, 1, 1), Verdict::Allow);
}

// ---------------------------------------------------------------------------
// §9.3 — "Blocco veloce, rilascio lento": the release consults ONLY the true
// measurement, with the hysteresis of `proxy/src/registry.rs:441-449`
// (`decide_over_storage`), because the opposite gives a tenant that oscillates
// in and out of the block on every refresh.
// ---------------------------------------------------------------------------

#[test]
fn the_block_is_immediate_and_the_release_waits_for_the_band() {
    let q = Quotas::for_test(TestKnobs {
        release_percent: 90,
        ..TestKnobs::default()
    });
    q.refresh(vec![row(T, Some(limits(Some(100), None, None)), measure(99, 0, 0))]);
    // Local writes carry it over on THIS broker, with no new measurement: the
    // fast half.
    assert_eq!(q.charge_kv_write(T, 1, 0), Verdict::Allow);
    assert_eq!(q.charge_kv_write(T, 1, 0), Verdict::OverQuota(Resource::KvRows));

    // A measurement that merely dips under the cap does NOT release: 95 > 90.
    q.refresh(vec![row(T, Some(limits(Some(100), None, None)), measure(95, 0, 0))]);
    assert_eq!(q.charge_kv_write(T, 1, 0), Verdict::OverQuota(Resource::KvRows));

    // Under the band, it does.
    q.refresh(vec![row(T, Some(limits(Some(100), None, None)), measure(89, 0, 0))]);
    assert_eq!(q.charge_kv_write(T, 1, 0), Verdict::Allow);
}

/// A tenant already over on the very first pass blocks immediately — nothing is
/// remembered across a restart, and the band may only ever delay the RELEASE
/// side (the same clause as `decide_over_storage`).
#[test]
fn a_tenant_already_over_at_boot_blocks_on_the_first_pass() {
    let q = Quotas::for_test(TestKnobs::default());
    q.refresh(vec![row(T, Some(limits(Some(100), None, None)), measure(500, 0, 0))]);
    assert_eq!(q.charge_kv_write(T, 1, 0), Verdict::OverQuota(Resource::KvRows));
}

// ---------------------------------------------------------------------------
// The delta itself: an UPPER BOUND, never a reconciliation.
// ---------------------------------------------------------------------------

/// The gate charges what the call COULD write, before knowing what it did:
/// a `put` on an existing key adds no row, and the SP does not say so per op.
/// Overestimating blocks early and underestimating blocks late, and only the
/// second is unsafe — so the delta is deliberately a majorant, corrected by the
/// next refresh.
#[test]
fn the_delta_is_an_upper_bound_and_the_refresh_is_what_corrects_it() {
    let q = Quotas::for_test(TestKnobs::default());
    q.refresh(vec![row(T, Some(limits(Some(1000), None, None)), measure(0, 0, 0))]);
    for _ in 0..10 {
        assert_eq!(q.charge_kv_write(T, 1, 100), Verdict::Allow);
    }
    assert_eq!(q.delta_of(T), (10, 1000, 0));
    // The refresh brings the truth — the ten writes all landed on ONE key — and
    // the delta goes back to zero rather than being adjusted: it counts writes
    // SINCE the measurement, so a newer measurement makes it double counting.
    q.refresh(vec![row(T, Some(limits(Some(1000), None, None)), measure(1, 100, 0))]);
    assert_eq!(q.delta_of(T), (0, 0, 0));
}

/// A call that was charged and then FAILED must give the charge back. Without
/// this a database outage — where every call is charged and none commits —
/// would inflate every delta until the tenant answered 403, converting a cell
/// fault (503, "not your fault") into a plan verdict (403, "yours"). §12 is
/// explicit that a stale or failed rollup must block nothing.
#[test]
fn a_failed_call_refunds_its_charge() {
    let q = Quotas::for_test(TestKnobs::default());
    q.refresh(vec![row(T, Some(limits(Some(10), None, None)), measure(0, 0, 0))]);
    assert_eq!(q.charge_kv_write(T, 5, 50), Verdict::Allow);
    q.refund(T, 5, 50, 0);
    assert_eq!(q.delta_of(T), (0, 0, 0));
    // …and a refund can never take a delta negative, which would be a credit
    // the tenant could bank against a later burst.
    q.refund(T, 100, 100, 100);
    assert_eq!(q.delta_of(T), (0, 0, 0));
}

// ---------------------------------------------------------------------------
// §12.1 stage 1 — the per-tenant token bucket, the FIRST rung of the ladder:
// "il cliente sopra il suo tasso rallenta per primo, e sa che e' suo."
// ---------------------------------------------------------------------------

#[test]
fn the_bucket_limits_writes_and_reads_on_separate_budgets() {
    let q = Quotas::for_test(TestKnobs {
        read_rate: 10,
        read_burst: 10,
        write_rate: 2,
        write_burst: 2,
        ..TestKnobs::default()
    });
    q.refresh(vec![]);
    // Two writes fit the burst, the third does not.
    assert_eq!(q.charge_kv_write(T, 1, 1), Verdict::Allow);
    assert_eq!(q.charge_kv_write(T, 1, 1), Verdict::Allow);
    assert!(matches!(q.charge_kv_write(T, 1, 1), Verdict::RateLimited(_)));
    // The read budget is its own: a tenant that exhausted its writes still reads.
    assert_eq!(q.check_kv_read(T), Verdict::Allow);
}

/// `Retry-After` must be a number a client can act on: at least one second, and
/// never zero (which reads as "immediately" and produces a spin).
#[test]
fn rate_limited_always_advertises_a_usable_retry_after() {
    let q = Quotas::for_test(TestKnobs {
        write_rate: 1,
        write_burst: 1,
        ..TestKnobs::default()
    });
    q.refresh(vec![]);
    assert_eq!(q.charge_kv_write(T, 1, 1), Verdict::Allow);
    match q.charge_kv_write(T, 1, 1) {
        Verdict::RateLimited(s) => assert!(s >= 1, "Retry-After {s} is not actionable"),
        v => panic!("expected RateLimited, got {v:?}"),
    }
}

/// A rate-limited call must NOT consume occupancy: it never reached the
/// database, so charging it would make a client's own retry storm look like
/// growth and eventually turn a 429 into a 403.
#[test]
fn a_rate_limited_call_is_not_charged_to_the_occupancy_delta() {
    let q = Quotas::for_test(TestKnobs {
        write_rate: 1,
        write_burst: 1,
        ..TestKnobs::default()
    });
    q.refresh(vec![]);
    assert_eq!(q.charge_kv_write(T, 1, 7), Verdict::Allow);
    assert!(matches!(q.charge_kv_write(T, 1, 7), Verdict::RateLimited(_)));
    assert_eq!(q.delta_of(T), (1, 7, 0));
}

/// The per-tenant limits from `queen.kv_quota` OVERRIDE the cell defaults, and
/// only downward is meaningful — but the row is authoritative either way, since
/// the control plane is what knows the plan.
#[test]
fn a_quota_row_overrides_the_cell_rate_defaults() {
    let q = Quotas::for_test(TestKnobs {
        write_rate: 1000,
        write_burst: 1000,
        ..TestKnobs::default()
    });
    q.refresh(vec![row(
        T,
        Some(Limits {
            enabled: true,
            max_writes_per_sec: Some(1),
            ..Limits::default()
        }),
        measure(0, 0, 0),
    )]);
    assert_eq!(q.charge_kv_write(T, 1, 1), Verdict::Allow);
    assert!(matches!(q.charge_kv_write(T, 1, 1), Verdict::RateLimited(_)));
}

// ---------------------------------------------------------------------------
// §9.3 — the hot watermark, which is what makes the rollup cadence adaptive.
// §14.3 point 5: "con una quota morbida l'80% e' gia' tardi."
// ---------------------------------------------------------------------------

#[test]
fn a_tenant_above_the_watermark_makes_the_cell_hot() {
    let q = Quotas::for_test(TestKnobs::default());
    q.refresh(vec![row(T, Some(limits(Some(100), None, None)), measure(50, 0, 0))]);
    assert!(!q.hot(), "half full is not hot");
    q.refresh(vec![row(T, Some(limits(Some(100), None, None)), measure(81, 0, 0))]);
    assert!(q.hot(), "81% of a soft quota is already late — that is the point");
    // Unlimited is never hot: no quota, no ratio, and no division by zero.
    q.refresh(vec![row(T, Some(limits(None, None, None)), measure(10_000_000, 0, 0))]);
    assert!(!q.hot());
}

/// The horizon is per tenant when the row says so and per cell otherwise
/// (§9.2): `max_timer_horizon_s` is a plan limit, and a plan limit that the cell
/// default could silently widen would be no limit at all.
#[test]
fn the_tenant_horizon_never_widens_the_cell_horizon() {
    let q = Quotas::for_test(TestKnobs::default());
    q.refresh(vec![row(
        T,
        Some(Limits {
            enabled: true,
            max_timer_horizon_s: Some(60),
            ..Limits::default()
        }),
        measure(0, 0, 0),
    )]);
    assert_eq!(q.horizon_ms(T, 7_776_000_000), 60_000);
    // A tenant asking for MORE than the cell allows gets the cell's number.
    q.refresh(vec![row(
        T,
        Some(Limits {
            enabled: true,
            max_timer_horizon_s: Some(999_999_999),
            ..Limits::default()
        }),
        measure(0, 0, 0),
    )]);
    assert_eq!(q.horizon_ms(T, 7_776_000_000), 7_776_000_000);
    // No row, no override.
    assert_eq!(q.horizon_ms(OTHER, 7_776_000_000), 7_776_000_000);
}
