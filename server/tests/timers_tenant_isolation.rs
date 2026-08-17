//! Tenant isolation for timers and KV (PLAN_KV_TIMERS.md §15 "Isolamento fra tenant",
//! §13.1-13.2). **Merge criterion, not a recommendation.**
//!
//! The whole file runs the adversarial setup the plan names: **two tenants with the same
//! queue names, the same `timer_key`s and the same KV namespaces.** That is the shape
//! under which every isolation bug in this feature is invisible — nothing raises, nothing
//! logs, and the wrong customer's message simply appears in the right-looking place.
//!
//! Four things must hold (§15): no row seen, no row written, **no segment fused at the
//! fire**, no DLQ row on the wrong partition. The third is singled out in §13.2 as the
//! easiest to get wrong: grouping the fire by `(queue, partition)` instead of
//! `(tenant_id, queue, partition)` is one word shorter, produces no error, and merges two
//! tenants' timers into one segment.
//!
//! The isolation these tests can prove is exactly the isolation the design has: a WHERE
//! clause inside the SPs. There is no RLS, and `queen.kv` / `queen.log_timers` are granted
//! to PUBLIC (§3.2), so anything that builds SQL against them outside the SPs is outside
//! what any assertion here can see — that gap is closed by the mechanical grep of §15.
//!
//! ONE test function: see the header of `timers_fault_injection.rs` for why (the claim has
//! no name filter).
//!
//! ```bash
//! docker run --rm -d --name queen-timers-pg -e POSTGRES_PASSWORD=postgres -p 5473:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5473 cargo test --test timers_tenant_isolation -- --ignored --nocapture
//! ```

mod timers_support;

use timers_support::*;

const LEASE_MS: i32 = 30_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn two_tenants_sharing_every_name_share_nothing_else() {
    let rig = boot().await;
    let c = &rig.c;
    reset_timers(c).await;

    // Same names everywhere. This is the point of the file.
    let queue = unique("tiso-orders");
    let key = unique("k-shared");
    let ns = unique("saga");
    let kv_key = unique("done-42");

    // ========================================================================
    section("the same (queue, timer_key) under two tenants is TWO rows");
    // §13.1: the tenant is not a filter applied to an id the caller presented — it is part
    // of the primary key, because the caller only ever presents names it chose itself.
    // ========================================================================
    seed(c, &Seed::new(TENANT_A, &queue, &key).delay_s(-1.0).txn("txn-A")).await;
    seed(c, &Seed::new(TENANT_B, &queue, &key).delay_s(-1.0).txn("txn-B")).await;
    assert_eq!(timer_keys(c, TENANT_A, &queue).await, vec![key.clone()]);
    assert_eq!(timer_keys(c, TENANT_B, &queue).await, vec![key.clone()]);
    assert_eq!(
        timer_row(c, TENANT_A, &queue, &key).await.expect("A").txn,
        "txn-A",
        "A's row is A's: a conflict can never be cross-tenant because ON CONFLICT targets \
         the COMPLETE primary key (§3.4)"
    );

    // ========================================================================
    section("no row seen: peek and list are scoped, and a stranger sees nothing");
    // ========================================================================
    let pa = peek(c, TENANT_A, &queue, &key).await;
    let pb = peek(c, TENANT_B, &queue, &key).await;
    assert_eq!(pa["found"].as_bool(), Some(true), "A peeks its own ({pa})");
    assert_eq!(pa["txn"].as_str(), Some("txn-A"), "and gets A's payload identity");
    assert_eq!(pb["txn"].as_str(), Some("txn-B"), "B gets B's ({pb})");
    let pc = peek(c, TENANT_C, &queue, &key).await;
    assert_eq!(
        pc["found"].as_bool(),
        Some(false),
        "a tenant that owns nothing sees nothing — and 'not mine' is INDISTINGUISHABLE \
         from 'does not exist' (§13.1), so it cannot be used to probe for other tenants"
    );

    let la = list(c, TENANT_A, &queue, None, 100).await;
    let rows = la["rows"].as_array().expect("list rows");
    assert_eq!(rows.len(), 1, "A's list holds exactly A's one timer: {la}");
    assert_eq!(rows[0]["txn"].as_str(), Some("txn-A"));
    let lc = list(c, TENANT_C, &queue, None, 100).await;
    assert!(
        lc["rows"].as_array().map(|r| r.is_empty()).unwrap_or(false),
        "an unknown tenant lists empty, never an error and never someone else's page: {lc}"
    );

    // ========================================================================
    section("no row written: B cannot cancel, reschedule or overwrite A's timer");
    // ========================================================================
    let r = apply(
        c,
        &serde_json::json!([schedule_op(&queue, &key, "Default", 60_000, "txn-B-2")]),
        TENANT_B,
        Some("tenant-b-service"),
        0.0,
    )
    .await
    .expect("B reschedules its own");
    assert_eq!(
        op_status(&r, 0).1,
        "rescheduled",
        "B's upsert hits B's row: the discriminator is xmax = 0, under the full PK (§6.2)"
    );
    assert_eq!(
        timer_row(c, TENANT_A, &queue, &key).await.expect("A").txn,
        "txn-A",
        "and A's row is byte-identical afterwards"
    );

    let r = apply(c, &serde_json::json!([cancel_op(&queue, &key)]), TENANT_C, None, 0.0)
        .await
        .expect("C cancels a key it does not own");
    assert_eq!(
        op_status(&r, 0),
        (false, "absent".to_string()),
        "absent with ok:false — not revealing is right, saying ok:true is not (§4.4)"
    );
    assert!(timer_row(c, TENANT_A, &queue, &key).await.is_some(), "A untouched");
    assert!(timer_row(c, TENANT_B, &queue, &key).await.is_some(), "B untouched");

    let r = apply(c, &serde_json::json!([cancel_op(&queue, &key)]), TENANT_B, None, 0.0)
        .await
        .expect("B cancels its own");
    assert_eq!(op_status(&r, 0), (true, "cancelled".to_string()));
    assert!(
        timer_row(c, TENANT_B, &queue, &key).await.is_none(),
        "B's row is gone"
    );
    assert!(
        timer_row(c, TENANT_A, &queue, &key).await.is_some(),
        "A's identically-named row is NOT: one DELETE, one tenant"
    );

    // ========================================================================
    section("NO SEGMENT FUSED AT THE FIRE (§6.2 point 8, §13.2 point 1)");
    // Two timers, same queue name, same partition name, same timer_key, both due in the
    // same claim batch. Grouped by (queue, partition) they become one segment and one
    // customer's message lands in the other's log. Grouped by (tenant, queue, partition)
    // they are two segments on two partitions of two different queue rows.
    // ========================================================================
    seed(c, &Seed::new(TENANT_B, &queue, &key).delay_s(-1.0).txn("txn-B")).await;
    let claimed = claim(c, 0.0, LEASE_MS, 100, 100).await;
    assert_eq!(
        claimed.len(),
        2,
        "one claim batch spans tenants — that is exactly why the ordering inside the claim \
         is (tenant_id, queue, timer_key) while the wire's is (queue, timer_key) (§2.3): \
         restricted to rows the two can contend for, the orders coincide"
    );
    let tok_a = token_of(c, TENANT_A, &queue, &key).await;
    let tok_b = token_of(c, TENANT_B, &queue, &key).await;

    let res = fire(
        c,
        &[
            Seg::new(TENANT_A, &queue, "Default"),
            Seg::new(TENANT_B, &queue, "Default"),
        ],
        &[
            Framed::new(&key, 1, "txn-A", &tok_a),
            Framed::new(&key, 2, "txn-B", &tok_b),
        ],
        0.0,
    )
    .await
    .expect("both segments fire");
    assert_eq!(seg_result(&res, 0), "fired");
    assert_eq!(seg_result(&res, 1), "fired");

    let pa = partition_state(c, TENANT_A, &queue, "Default")
        .await
        .expect("A's partition");
    let pb = partition_state(c, TENANT_B, &queue, "Default")
        .await
        .expect("B's partition");
    assert_ne!(
        pa.id, pb.id,
        "same queue NAME, two queue rows, two partitions — queen.queues is unique on \
         (tenant_id, name), and the fire must resolve the destination under the timer's \
         own tenant"
    );
    assert_eq!(
        (pa.messages_in_segments, pa.segments),
        (1, 1),
        "A got exactly its own one message, in its own one segment"
    );
    assert_eq!((pb.messages_in_segments, pb.segments), (1, 1), "and B likewise");

    // The strongest form of the same statement: a segment declared under one tenant must
    // not be able to reach a row of another, even when it names it exactly.
    let k_bonly = unique("k-b-only");
    seed(c, &Seed::new(TENANT_B, &queue, &k_bonly).delay_s(-1.0).txn("txn-B-only")).await;
    assert_eq!(claim(c, 0.0, LEASE_MS, 100, 100).await, vec![k_bonly.clone()]);
    let tok_bonly = token_of(c, TENANT_B, &queue, &k_bonly).await;
    let res = fire(
        c,
        &[Seg::new(TENANT_A, &queue, "Default")],
        &[Framed::new(&k_bonly, 1, "txn-B-only", &tok_bonly)],
        0.0,
    )
    .await
    .expect("cross-tenant fire is a verdict, not a crash");
    assert_eq!(
        seg_result(&res, 0),
        "stale",
        "the key does not exist UNDER THIS SEGMENT'S TENANT, so nothing verifies"
    );
    let row = timer_row(c, TENANT_B, &queue, &k_bonly)
        .await
        .expect("B's timer is still B's");
    assert_eq!(
        row.claim_token.as_deref(),
        Some(tok_bonly.as_str()),
        "and its lease was never touched by the other tenant's transaction"
    );
    let pa = partition_state(c, TENANT_A, &queue, "Default").await.expect("A");
    assert_eq!(
        pa.messages_in_segments, 1,
        "A's log did not grow by someone else's message"
    );

    // ========================================================================
    section("no DLQ row on the wrong partition (§13.2 point 2)");
    // queen.log_dlq has no tenant column (005_log_ack.sql:52-63): its ONLY scoping is
    // partition_id, so resolving the partition under the timer's tenant is the whole
    // isolation story for dead letters.
    // ========================================================================
    let k_dead = unique("k-dead");
    seed(c, &Seed::new(TENANT_A, &queue, &k_dead).delay_s(-1.0).attempts(5).txn("txn-A-dead")).await;
    seed(c, &Seed::new(TENANT_B, &queue, &k_dead).delay_s(-1.0).attempts(5).txn("txn-B-dead")).await;
    dlq(
        c,
        TENANT_A,
        &queue,
        &k_dead,
        "{\"tenant\":\"a\"}",
        "exhausted",
        5,
        0.0,
    )
    .await
    .expect("archive A's");

    let a_rows = dlq_rows(c, TENANT_A, &queue).await;
    let b_rows = dlq_rows(c, TENANT_B, &queue).await;
    assert_eq!(a_rows.len(), 1, "one dead letter, under A");
    assert_eq!(a_rows[0].2.as_deref(), Some("txn-A-dead"), "and it is A's");
    assert_eq!(
        (a_rows[0].0.as_str(), a_rows[0].1),
        ("__timer__", -1),
        "the synthetic group and the non-position offset (§4.5)"
    );
    assert!(
        b_rows.is_empty(),
        "B's identically-named queue must have NO dead letters: a DLQ row on the wrong \
         partition is a customer reading another customer's failed payload"
    );
    assert!(
        timer_row(c, TENANT_B, &queue, &k_dead).await.is_some(),
        "and B's identically-named timer is still pending"
    );

    // ========================================================================
    section("the same KV (namespace, key) under two tenants is two values");
    // §5, §13.1. Same adversarial shape: identical namespace, identical key.
    // ========================================================================
    kv_apply(
        c,
        &serde_json::json!([
            {"op": "put", "ns": ns, "key": kv_key, "value": {"owner": "A"}, "ttlSeconds": 60}
        ]),
        TENANT_A,
        false,
    )
    .await
    .expect("A writes");
    kv_apply(
        c,
        &serde_json::json!([
            {"op": "put", "ns": ns, "key": kv_key, "value": {"owner": "B"}, "ttlSeconds": 60}
        ]),
        TENANT_B,
        false,
    )
    .await
    .expect("B writes the SAME key and must not collide");

    let ga = kv_apply(
        c,
        &serde_json::json!([{"op": "get", "ns": ns, "key": kv_key}]),
        TENANT_A,
        false,
    )
    .await
    .expect("A reads");
    let gb = kv_apply(
        c,
        &serde_json::json!([{"op": "get", "ns": ns, "key": kv_key}]),
        TENANT_B,
        false,
    )
    .await
    .expect("B reads");
    assert_eq!(ga[0]["value"]["owner"].as_str(), Some("A"), "A reads A's: {ga}");
    assert_eq!(gb[0]["value"]["owner"].as_str(), Some("B"), "B reads B's: {gb}");

    let gc = kv_apply(
        c,
        &serde_json::json!([{"op": "get", "ns": ns, "key": kv_key}]),
        TENANT_C,
        false,
    )
    .await
    .expect("C reads");
    assert_eq!(
        gc[0]["found"].as_bool(),
        Some(false),
        "an unknown namespace reads EMPTY, never an error and never a neighbour's value: {gc}"
    );

    let n: i64 = c
        .query_one(
            "SELECT count(*) FROM queen.kv WHERE namespace = $1 AND key = $2",
            &[&ns, &kv_key],
        )
        .await
        .expect("count kv")
        .get(0);
    assert_eq!(
        n, 2,
        "two rows for one (namespace, key): a unique index on (namespace, key) 'for \
         efficiency' would have collapsed them into one (§3.4)"
    );

    kv_apply(
        c,
        &serde_json::json!([{"op": "delete", "ns": ns, "key": kv_key}]),
        TENANT_B,
        false,
    )
    .await
    .expect("B deletes");
    let ga = kv_apply(
        c,
        &serde_json::json!([{"op": "get", "ns": ns, "key": kv_key}]),
        TENANT_A,
        false,
    )
    .await
    .expect("A reads after B's delete");
    assert_eq!(
        ga[0]["value"]["owner"].as_str(),
        Some("A"),
        "B's delete removed exactly one row, and it was B's: {ga}"
    );

    let _ = rig.broker.shutdown().await;
}
