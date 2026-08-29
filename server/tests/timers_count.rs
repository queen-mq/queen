//! Exact timer prefix counting for queue adapters.
//!
//! The count is intentionally a separate operation from list: Laravel needs
//! one scalar for `Queue::size()`, not every timer row in every keyset page.
//! This test pins tenant/queue isolation, literal prefix semantics, validation,
//! the JSON wire, and the primary-key range scan used to obtain it.
//!
//! ```bash
//! docker run --rm -d --name queen-timers-pg -e POSTGRES_PASSWORD=postgres -p 5473:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5473 cargo test --test timers_count -- --ignored --nocapture
//! ```

mod timers_support;

use timers_support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn timer_count_is_scoped_literal_and_index_driven() {
    let rig = boot().await;
    let c = &rig.c;
    reset_timers(c).await;

    let queue = unique("timer-count");
    let other_queue = unique("timer-count-other");
    for key in [
        "laravel:delay:a",
        "laravel:release:b",
        "laravel:%literal",
        "somebody-elses-namespace",
    ] {
        seed(c, &Seed::new(TENANT_A, &queue, key)).await;
    }
    seed(
        c,
        &Seed::new(TENANT_A, &other_queue, "laravel:delay:other-queue"),
    )
    .await;
    seed(
        c,
        &Seed::new(TENANT_B, &queue, "laravel:delay:other-tenant"),
    )
    .await;

    assert_eq!(
        count_timers(c, TENANT_A, &queue, "laravel:")
            .await
            .expect("count")["count"],
        3,
        "only this tenant, this queue and this literal namespace count"
    );
    assert_eq!(
        count_timers(c, TENANT_A, &queue, "laravel:%")
            .await
            .expect("literal percent")["count"],
        1,
        "% is a normal timer-key byte, not a LIKE wildcard"
    );
    assert_eq!(
        count_timers(c, TENANT_A, &queue, "missing:")
            .await
            .expect("empty match")["count"],
        0
    );

    let empty = count_timers(c, TENANT_A, &queue, "")
        .await
        .expect_err("empty prefix would permit a whole-queue scan");
    assert_eq!(sqlstate(&empty), "22023");
    let oversized = count_timers(c, TENANT_A, &queue, &"x".repeat(129))
        .await
        .expect_err("prefix is bounded in UTF-8 bytes");
    assert_eq!(sqlstate(&oversized), "22023");

    // Force the planner to expose whether the predicate can use the existing
    // PK even on this deliberately tiny fixture. This is the exact predicate
    // in log_timers_count_v1; starts_with supplies semantics while the two
    // comparisons supply the btree range.
    c.batch_execute("SET enable_seqscan = off")
        .await
        .expect("disable seqscan for plan assertion");
    let plan = c
        .query(
            "EXPLAIN (FORMAT TEXT, COSTS OFF)
             SELECT count(*)
               FROM queen.log_timers t
              WHERE t.tenant_id = $1::text::uuid
                AND t.queue = $2
                AND t.timer_key >= $3
                AND t.timer_key < queen.kv_prefix_end_v1($3)
                AND starts_with(t.timer_key, $3)",
            &[&TENANT_A, &queue, &"laravel:"],
        )
        .await
        .expect("explain count")
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("Index") && plan.contains("log_timers_pkey"),
        "timer count must be driven by the composite primary key, got:\n{plan}"
    );
}
