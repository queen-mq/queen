//! The declared timer semantics of PLAN_KV_TIMERS.md §4.2, one scenario each.
//!
//! These four sentences are the whole user-visible contract of a timer, and each one is
//! here because it is the kind of thing that is true in the head of whoever wrote the SQL
//! and nowhere else:
//!
//!   1. `deliverAt` is "not before", never "exactly at".
//!   2. A `deliverAt` in the past is legal and fires on the first cycle.
//!   3. The order is decided at the FIRE (`ORDER BY visible_at`), not at the schedule.
//!   4. `producerSub`, `messageId` and `tenant` are not input fields: present in an op
//!      they are a RAISE 22023, never a field quietly ignored.
//!
//! Number 4 is the security-shaped one. `producer_sub` is the only non-repudiable field of
//! a frame (auth.rs:31-36 is explicit that a client-supplied value is never honored), so a
//! timer op that could set it would be the only way in the whole product to forge a
//! frame's provenance — and an op that is IGNORED rather than REJECTED is exactly how such
//! a hole survives review: the caller sees success and the audit sees a normal frame.
//!
//! ONE test function: see `timers_fault_injection.rs` for why the claim cannot run in
//! parallel with itself.
//!
//! ```bash
//! docker run --rm -d --name queen-timers-pg -e POSTGRES_PASSWORD=postgres -p 5473:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5473 cargo test --test timers_semantics -- --ignored --nocapture
//! ```

mod timers_support;

use timers_support::*;

const LEASE_MS: i32 = 30_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn the_declared_timer_semantics() {
    let rig = boot().await;
    let c = &rig.c;
    let t = TENANT_DEFAULT;
    reset_timers(c).await;

    // ========================================================================
    section("deliverAt is NOT BEFORE: a timer is never delivered early");
    // ========================================================================
    let q = unique("tsem-notbefore");
    let k = unique("k-future");
    let r = apply(
        c,
        &serde_json::json!([schedule_op(&q, &k, "Default", 5_000, "txn-future")]),
        t,
        Some("scheduler"),
        0.0,
    )
    .await
    .expect("schedule");
    assert_eq!(op_status(&r, 0), (true, "scheduled".to_string()), "a new name inserts");

    let d = due(c, 0.0, 2000).await;
    assert_eq!(d["due"].as_i64(), Some(0), "nothing is due yet: {d}");
    let next = d["nextInMs"].as_i64().expect("nextInMs");
    assert!(
        (4_000..=6_000).contains(&next),
        "the wake-up is returned RELATIVE to the server clock, so the broker never does \
         arithmetic on a timestamp and no clock skew between brokers can enter (§4.2); \
         got {next}ms"
    );
    assert!(
        claim(c, 0.0, LEASE_MS, 100, 100).await.is_empty(),
        "and no claim can pull it early: visible_at gates the only scan there is"
    );
    assert!(
        claim(c, 4.9, LEASE_MS, 100, 100).await.is_empty(),
        "not one cycle early either"
    );
    assert_eq!(
        claim(c, 5.1, LEASE_MS, 100, 100).await,
        vec![k.clone()],
        "after deliver_at it is claimable — 'not before', with the floor being one sweep \
         cycle plus the single-hop push, never a promise of exactness"
    );

    // ========================================================================
    section("a deliverAt in the PAST is legal and fires on the first cycle");
    // ========================================================================
    let q_past = unique("tsem-past");
    let k_now = unique("k-now");
    let k_past = unique("k-past");
    apply(
        c,
        &serde_json::json!([
            schedule_op(&q_past, &k_now, "Default", 0, "txn-now"),
            schedule_op(&q_past, &k_past, "Default", -3_600_000, "txn-past"),
        ]),
        t,
        Some("scheduler"),
        0.0,
    )
    .await
    .expect("delayMs of 0 and of an hour ago are both legal");

    let d = due(c, 0.001, 2000).await;
    assert_eq!(
        d["due"].as_i64(),
        Some(2),
        "both are due on the very first probe — 'in the past' is not an error to reject, \
         it is a timer that is simply late already: {d}"
    );
    let late = d["lateMs"].as_i64().expect("lateMs");
    assert!(
        late >= 3_500_000,
        "and lateMs reports the real lateness of the oldest one, computed server-side \
         (§6.2); got {late}"
    );
    let mut got = claim(c, 0.001, LEASE_MS, 100, 100).await;
    got.sort();
    let mut want = vec![k_now.clone(), k_past.clone()];
    want.sort();
    assert_eq!(got, want, "the first cycle takes both");

    // ========================================================================
    section("the ORDER is decided at the FIRE, not at the schedule (§4.2)");
    // Three timers scheduled in one order and delivered in another, with the third one
    // reordered by a BACKOFF — the case that proves the key is visible_at and not
    // deliver_at, and not created_at either.
    // ========================================================================
    let q_ord = unique("tsem-order");
    let (alpha, beta, gamma) = (unique("k-alpha"), unique("k-beta"), unique("k-gamma"));

    // gamma is scheduled FIRST and is due FIRST, then fails once and backs off by 2s.
    seed(c, &Seed::new(t, &q_ord, &gamma).delay_s(-1.0)).await;
    assert_eq!(claim(c, 0.0, LEASE_MS, 100, 100).await, vec![gamma.clone()]);
    let tok = token_of(c, t, &q_ord, &gamma).await;
    fail(c, t, &q_ord, &gamma, &tok, 2_000, "08006 transient", false, 5, 0.0)
        .await
        .expect("backoff");

    // alpha is scheduled next but due last; beta is scheduled last and due first.
    apply(
        c,
        &serde_json::json!([schedule_op(&q_ord, &alpha, "Default", 3_000, "txn-alpha")]),
        t,
        Some("scheduler"),
        0.0,
    )
    .await
    .expect("alpha");
    apply(
        c,
        &serde_json::json!([schedule_op(&q_ord, &beta, "Default", 1_000, "txn-beta")]),
        t,
        Some("scheduler"),
        0.0,
    )
    .await
    .expect("beta");

    let order = claim(c, 4.0, LEASE_MS, 100, 100).await;
    assert_eq!(
        order,
        vec![beta.clone(), gamma.clone(), alpha.clone()],
        "expiry order, not schedule order: gamma was scheduled first and was due first, \
         yet a backoff moved its visible_at and therefore its place in the log. Two timers \
         maturing in the same batch enter the log in the order the CLAIM returned them, \
         which is ORDER BY visible_at (§4.2)"
    );

    // ========================================================================
    section("producerSub is not an input field (§4.2): RAISE 22023");
    // ========================================================================
    let q_in = unique("tsem-input");
    let k_forge = unique("k-forge");
    let mut op = schedule_op(&q_in, &k_forge, "Default", 60_000, "txn-forge");
    op["producerSub"] = serde_json::json!("billing-service");
    let e = apply(c, &serde_json::json!([op]), t, Some("attacker"), 0.0)
        .await
        .expect_err(
            "a forged producerSub must be REJECTED, not ignored: ignored, the caller gets \
             a frame in the log a second later whose provenance is attested by the broker \
             and chosen by the client",
        );
    assert_eq!(
        sqlstate(&e),
        "22023",
        "invalid_parameter_value, so the classifier calls it permanent and nobody retries it"
    );
    assert!(
        timer_row(c, t, &q_in, &k_forge).await.is_none(),
        "and nothing was written"
    );

    // ========================================================================
    section("messageId is not an input field (§4.2): RAISE 22023");
    // The id is minted server-side with util::uuidv7_bytes so the schedule API can answer
    // 'this is the id you will see' (§6.2). A client-spellable messageId would let a caller
    // choose the identity of a frame it does not own.
    // ========================================================================
    let k_mid = unique("k-msgid");
    let mut op = schedule_op(&q_in, &k_mid, "Default", 60_000, "txn-msgid");
    op["messageId"] = serde_json::json!("00000000-0000-7000-8000-00000000dead");
    let e = apply(c, &serde_json::json!([op]), t, Some("scheduler"), 0.0)
        .await
        .expect_err("a client-supplied messageId must raise");
    assert_eq!(sqlstate(&e), "22023");
    assert!(timer_row(c, t, &q_in, &k_mid).await.is_none());

    // ========================================================================
    section("tenant is not an input field (§4.2, §6.2): RAISE 22023");
    // p_tenant is an ARGUMENT. A tenant carried inside an op would be a tenant chosen by
    // the caller, which is the one thing tenancy cannot allow (server/src/tenant.rs:16-19
    // is explicit that the header value is opaque and validated against nothing).
    // ========================================================================
    for field in ["tenant", "tenantId", "_tenant"] {
        let k_t = unique("k-tenant");
        let mut op = schedule_op(&q_in, &k_t, "Default", 60_000, "txn-tenant");
        op[field] = serde_json::json!(TENANT_B);
        match apply(c, &serde_json::json!([op]), TENANT_A, Some("scheduler"), 0.0).await {
            Ok(v) => panic!("'{field}' inside an op must raise 22023, got a verdict: {v}"),
            Err(e) => assert_eq!(
                sqlstate(&e),
                "22023",
                "'{field}' must be rejected: p_tenant is an argument, and a tenant a caller \
                 can spell is a tenant a caller can choose"
            ),
        }
        assert!(timer_row(c, TENANT_A, &q_in, &k_t).await.is_none());
        assert!(
            timer_row(c, TENANT_B, &q_in, &k_t).await.is_none(),
            "'{field}' must never have been able to write into another tenant either"
        );
    }

    // Last, and separable: see the header of the function below.
    an_absolute_deliver_at_is_not_on_the_wire(c).await;

    let _ = rig.broker.shutdown().await;
}

/// Asserts something §4.2 IMPLIES rather than spells out: "only relative durations on the
/// wire (`delayMs`), never absolute instants". An absolute `deliverAt` that is silently
/// ignored would let a caller believe it scheduled a timer for a wall-clock moment while
/// the row carries something else entirely — the same class of quiet acceptance the three
/// rejected fields above exist to prevent. Kept as its own function, called last, so that
/// if the implementation decides unknown fields are ignored instead this is one deletion
/// and the decision goes in §20 next to 20.6.
async fn an_absolute_deliver_at_is_not_on_the_wire(c: &tokio_postgres::Client) {
    section("an absolute deliverAt is not expressible on this wire (inference from §4.2)");
    let q = unique("tsem-absolute");
    let k = unique("k-absolute");
    let mut op = schedule_op(&q, &k, "Default", 60_000, "txn-absolute");
    op.as_object_mut().unwrap().remove("delayMs");
    op["deliverAt"] = serde_json::json!("2030-01-01T00:00:00Z");
    let e = apply(c, &serde_json::json!([op]), TENANT_DEFAULT, Some("scheduler"), 0.0)
        .await
        .expect_err("an absolute instant is not expressible on this wire");
    assert_eq!(
        sqlstate(&e),
        "22023",
        "one clock, and it is Postgres's: deliver_at is computed as p_now + interval so no \
         broker's skew can enter (§4.2)"
    );
    assert!(timer_row(c, TENANT_DEFAULT, &q, &k).await.is_none());
}
