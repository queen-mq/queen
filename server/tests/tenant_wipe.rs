//! The broker half of a tenant wipe — `queen.delete_tenant_data_v1`
//! (031_tenant_purge.sql).
//!
//! The claim under test is narrow and total: **after the purge, ZERO rows of the
//! wiped tenant remain in ANY table that can be attributed to it, and not one row of a
//! second tenant sharing every name has moved.** Both halves matter. A purge that misses
//! a table fails a GDPR erasure silently; a purge that over-reaches takes a paying
//! customer's data with a trial one's, and neither raises.
//!
//! The setup is deliberately adversarial in the same way `timers_tenant_isolation.rs` is:
//! **two tenants with the same queue names, the same consumer groups, the same KV
//! namespace and the same timer shape.** Under that shape a missing `tenant_id = $1` leg
//! is invisible — the wrong customer's rows simply disappear from a plausible-looking
//! place.
//!
//! What each section pins:
//!
//!   1. **The table list is checked against the catalog, not against a document.** The
//!      first assertion enumerates `pg_attribute` for every `queen` table carrying
//!      `tenant_id` and pins the set. It is ten tables, and `queen.consumer_watermarks`
//!      is NOT one of them (its scoping is inherited through the `queen.queues` FK — the
//!      2026-07-31 queue-identity merge). Any new tenant-carrying table fails here AND
//!      inside the function's own coverage check, which is the point: the two must be
//!      updated together.
//!   2. **A queue-delete loop is not a tenant purge.** Nine of those ten tables are
//!      unreachable from `queen.queues(id)`, and the seed puts a row of the wiped tenant
//!      in every one of them — including the `consumer_groups_metadata` DISCOVERY row
//!      (`queue_id IS NULL`) that names no queue at all.
//!   3. **Timers go first.** A surviving `queen.log_timers` row fires from the sweeper,
//!      never through the proxy, and re-creates the queue the purge just deleted.
//!   4. **Idempotency**, which is what makes the wipe safe to retry after a partial
//!      failure, and **the two guards** (the default tenant, and an unknown
//!      tenant-carrying table).
//!   5. **Every delete is bounded**, including the per-queue ones (`message_traces`,
//!      `retention_history`, `queen_streams.state`) — and a queue is never deleted
//!      while its own residue is still there, because the partition ids that attribute
//!      that residue die with it.
//!   6. **The second schema.** `queen_streams.state` is payload-derived, keyed by a
//!      partition id with no FK, and outside every cascade; `queen_streams.queries`
//!      carries no tenant at all and is the documented residue.
//!
//! ONE test function, for the reason `embedded_smoke.rs` gives: the broker is booted
//! once, so a single flow drives a single instance.
//!
//! ```bash
//! docker run --rm -d --name queen-wipe-pg -e POSTGRES_PASSWORD=postgres -p 5488:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5488 cargo test --test tenant_wipe -- --ignored --nocapture
//! ```

use queen::{Broker, BrokerConfig};
use tokio_postgres::Client;

/// The tenant every pre-tenancy caller lands in. Never wiped by these tests — the
/// function refuses it, and that refusal is itself asserted.
const TENANT_DEFAULT: &str = "00000000-0000-0000-0000-000000000001";
/// A is wiped; B shares every name with A and must survive untouched. Neither is the
/// default one, so a missing tenant leg cannot pass by landing there.
const TENANT_A: &str = "aaaaaaaa-0000-4000-8000-0000000000aa";
const TENANT_B: &str = "bbbbbbbb-0000-4000-8000-0000000000bb";
/// A third tenant, owned by the trace-budget section alone: it leaves a half-purged queue
/// behind, and the sections either side of it count A's and B's queues.
const TENANT_C: &str = "cccccccc-0000-4000-8000-0000000000cc";

/// Every table in schema `queen` that carries a `tenant_id` column, i.e. everything the
/// purge has to reach by tenant rather than by cascade. Kept in the same order as
/// `C_TENANT_TABLES` in 031_tenant_purge.sql so a diff between the two reads straight.
const TENANT_TABLES: &[&str] = &[
    "queen.queues",
    "queen.log_timers",
    "queen.consumer_groups_metadata",
    "queen.hotlist_repairs",
    "queen.queue_parked_replica",
    "queen.kv",
    "queen.kv_quota",
    "queen.kv_usage",
    "queen.ephemeral_queues",
    "queen.ephemeral_quota",
];

async fn connect(host: &str, port: u16) -> Client {
    let (c, conn) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres password=postgres dbname=postgres"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

fn section(title: &str) {
    println!("\n=== {title}");
}

/// The server's own message. `tokio_postgres::Error::to_string()` is the useless literal
/// "db error"; the RAISE text lives one level down, and these tests assert on it because a
/// refusal that does not say what it is protecting is a refusal an operator will work
/// around.
fn db_message(e: &tokio_postgres::Error) -> String {
    e.as_db_error().map(|d| d.message().to_string()).unwrap_or_else(|| e.to_string())
}

/// `SELECT count(*) FROM <table> WHERE tenant_id = $1`. The table name is a constant from
/// `TENANT_TABLES`, never input.
async fn tenant_rows(c: &Client, table: &str, tenant: &str) -> i64 {
    let sql = format!("SELECT count(*) FROM {table} WHERE tenant_id = $1::text::uuid");
    c.query_one(&sql, &[&tenant]).await.unwrap_or_else(|e| panic!("count {table}: {e}")).get(0)
}

/// One scalar count, for the queue-scoped tables that carry no tenant of their own.
async fn count(c: &Client, sql: &str, args: &[&(dyn tokio_postgres::types::ToSql + Sync)]) -> i64 {
    c.query_one(sql, args).await.unwrap_or_else(|e| panic!("{sql}: {e}")).get(0)
}

async fn purge(c: &Client, tenant: &str, max_queues: i32) -> serde_json::Value {
    let row = c
        .query_one(
            "SELECT (queen.delete_tenant_data_v1($1::text::uuid, $2::int))::text",
            &[&tenant, &max_queues],
        )
        .await
        .expect("delete_tenant_data_v1");
    let txt: String = row.get(0);
    serde_json::from_str(&txt).expect("purge result is JSON")
}

/// Put a row of `tenant` in every tenant-carrying table, plus everything a queue delete
/// must cascade through, plus the two tables attributed only by `partition_id`.
///
/// Real writers where a real writer exists (`configure_queue_v1`, `log_push_one_v1`,
/// `kv_apply_v1`, `eph_config_set_v1`) and a direct INSERT where the only writer is the
/// live engine — the same split `timers_support` makes, and for the same reason: this
/// file is about the DELETE path, so the seed must depend on the schema, not on how many
/// SPs it takes to reach a given row.
async fn seed(c: &Client, tenant: &str, tag: &str) {
    for (queue, opts) in
        [("orders", r#"{"namespace":"shop","task":"pay"}"#), ("audit", "{}")]
    {
        c.execute(
            "SELECT queen.configure_queue_v1($1, $2::text::jsonb, $3::text::uuid)",
            &[&queue, &opts, &tenant],
        )
        .await
        .expect("configure_queue_v1");
        c.execute(
            "SELECT queen.log_push_one_v1($1, 'Default', 1, decode(repeat('ab',16),'hex'), 0,
                                          convert_to($2, 'UTF8'), NULL, NULL, $3::text::uuid)",
            &[&queue, &tag, &tenant],
        )
        .await
        .expect("log_push_one_v1");
    }

    // Everything below hangs off the 'orders' partition the push just created.
    c.batch_execute(&format!(
        r#"
        DO $seed$
        DECLARE v_q1 UUID; v_pid UUID;
        BEGIN
            SELECT id INTO v_q1 FROM queen.queues
             WHERE tenant_id = '{t}'::uuid AND name = 'orders';
            SELECT id INTO v_pid FROM queen.log_partitions WHERE queue_id = v_q1 LIMIT 1;

            INSERT INTO queen.log_consumers(partition_id, consumer_group, committed)
                VALUES (v_pid, 'g1', 0) ON CONFLICT DO NOTHING;
            INSERT INTO queen.log_dlq(partition_id, consumer_group, "offset",
                                      transaction_id, payload, error)
                VALUES (v_pid, 'g1', 0, 'txn-{g}', '{{"tag":"{g}"}}'::jsonb, 'boom');
            INSERT INTO queen.consumer_watermarks(queue_id, consumer_group)
                VALUES (v_q1, 'g1') ON CONFLICT DO NOTHING;

            -- a queue-scoped group AND a namespace/task DISCOVERY row. The second has
            -- queue_id NULL, names no queue, and is the row a cascade cannot reach.
            INSERT INTO queen.consumer_groups_metadata
                (tenant_id, consumer_group, queue_id, partition_name, namespace, task,
                 subscription_mode, subscription_timestamp)
                VALUES ('{t}'::uuid, 'g1', v_q1, 'Default', '', '', 'all', now())
                ON CONFLICT DO NOTHING;
            INSERT INTO queen.consumer_groups_metadata
                (tenant_id, consumer_group, queue_id, partition_name, namespace, task,
                 subscription_mode, subscription_timestamp)
                VALUES ('{t}'::uuid, 'g1', NULL, '', 'shop', 'pay', 'new', now())
                ON CONFLICT DO NOTHING;

            INSERT INTO queen.queue_lag_metrics(bucket_time, queue_id, pop_count)
                VALUES (date_trunc('minute', now()), v_q1, 7) ON CONFLICT DO NOTHING;
            INSERT INTO queen.stats(stat_type, stat_key, queue_id, total_messages)
                VALUES ('queue', v_q1::text, v_q1, 1) ON CONFLICT DO NOTHING;

            INSERT INTO queen.hotlist_repairs(tenant_id, queue_name, consumer_group, reason)
                VALUES ('{t}'::uuid, 'orders', 'g1', 'seek') ON CONFLICT DO NOTHING;
            INSERT INTO queen.queue_parked_replica
                (bucket_time, queue_name, hostname, worker_id, tenant_id, parked_count)
                VALUES (date_trunc('minute', now()), 'orders', 'h1', 1, '{t}'::uuid, 3)
                ON CONFLICT DO NOTHING;

            PERFORM queen.kv_apply_v1(
                '[{{"op":"put","ns":"saga","key":"done-42","value":{{"tag":"{g}"}},
                    "forever":true}}]'::jsonb,
                '{t}'::uuid, now(), false);
            INSERT INTO queen.kv_quota(tenant_id, max_rows) VALUES ('{t}'::uuid, 1000)
                ON CONFLICT (tenant_id) DO NOTHING;
            INSERT INTO queen.kv_usage(tenant_id, kv_rows) VALUES ('{t}'::uuid, 1)
                ON CONFLICT (tenant_id) DO NOTHING;

            INSERT INTO queen.log_timers(tenant_id, queue, timer_key, "partition", deliver_at,
                                         txn, message_id, payload, payload_zstd, encrypted,
                                         producer_sub)
                VALUES ('{t}'::uuid, 'orders', 'retry-1', 'Default', now() + interval '1 hour',
                        'txn-timer-{g}', gen_random_uuid(), convert_to('{{}}', 'UTF8'),
                        false, false, 'seeder')
                ON CONFLICT DO NOTHING;

            PERFORM queen.eph_config_set_v1('{t}', 'live-prices', '{{}}'::jsonb);
            INSERT INTO queen.ephemeral_quota(tenant_id, max_queues) VALUES ('{t}'::uuid, 5)
                ON CONFLICT (tenant_id) DO NOTHING;

            -- Attributed ONLY by partition_id: no FK, no tenant column. Once the
            -- partitions are gone nothing can ever tell whose these were.
            INSERT INTO queen.message_traces(partition_id, transaction_id, consumer_group,
                                             event_type, data)
                VALUES (v_pid, 'txn-{g}', 'g1', 'push', '{{"tag":"{g}"}}'::jsonb);
            INSERT INTO queen.message_trace_names(trace_id, trace_name)
                SELECT id, 'name-{g}' FROM queen.message_traces
                 WHERE transaction_id = 'txn-{g}';
            INSERT INTO queen.retention_history(partition_id, messages_deleted, retention_type)
                VALUES (v_pid, 4, 'ttl');

            -- The streaming schema. `queries` is globally named (its `name` is UNIQUE
            -- across the database and carries no tenant), so the two tenants get
            -- different query names; `state` is per-partition and IS attributable, and
            -- is what the purge must reach.
            INSERT INTO queen_streams.queries(name, source_queue, config_hash)
                VALUES ('sv-{g}', 'orders', 'h0') ON CONFLICT (name) DO NOTHING;
            INSERT INTO queen_streams.state(query_id, partition_id, key, value)
                SELECT id, v_pid, 'customer-42', '{{"total": 999}}'::jsonb
                  FROM queen_streams.queries WHERE name = 'sv-{g}'
                ON CONFLICT DO NOTHING;
        END;
        $seed$;
        "#,
        t = tenant,
        g = tag
    ))
    .await
    .expect("seed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn a_tenant_wipe_removes_everything_of_one_tenant_and_nothing_of_another() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port) for the tenant wipe test");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // The sweeper would fire the seeded timers; retention and the stats refresh would
    // rewrite rows this file counts. Every background writer off — these tests need to be
    // the only writer.
    std::env::set_var("QUEEN_SWEEPER", "false");

    // Booting the real broker is what applies the real schema. The SQL is
    // include_str!-embedded, so this is also the only proof that the 031 file on disk is
    // the one the binary carries — a psql script would pass against a stale build.
    let _broker = Broker::start(
        BrokerConfig::new()
            .pg(host.clone(), port, "postgres", "postgres", "postgres")
            .pool_size(4)
            .retention(false)
            .stats_refresh(false)
            .system_metrics(false),
    )
    .await
    .expect("broker start");

    let c = connect(&host, port).await;

    // A previous run's rows would make every count below order-dependent noise. These
    // tests own the throwaway Postgres they are pointed at, which is what makes a blanket
    // clear acceptable — and DELETE, not TRUNCATE, because TRUNCATE takes ACCESS
    // EXCLUSIVE (the lock class the KV/timer tables set vacuum_truncate=off to avoid).
    for t in [TENANT_A, TENANT_B, TENANT_C] {
        c.execute("SELECT queen.delete_tenant_data_v1($1::text::uuid, 1000, 1000000)", &[&t])
            .await
            .expect("pre-clean");
    }

    // ========================================================================
    section("the tenant-carrying table list is what the catalog says it is");
    // FINDINGS §2 lists eleven tables and includes queen.consumer_watermarks. The catalog
    // says ten and excludes it: watermarks are keyed by queue_id and inherit their
    // scoping through the FK (schema.sql, 2026-07-31 queue-identity merge). A purge
    // statement written against a column that does not exist would fail at schema-apply
    // time, i.e. at every broker boot — so this is pinned here rather than discovered
    // there.
    // ========================================================================
    let rows = c
        .query(
            "SELECT 'queen.' || cl.relname
               FROM pg_attribute a
               JOIN pg_class     cl ON cl.oid = a.attrelid
               JOIN pg_namespace n  ON n.oid = cl.relnamespace
              WHERE n.nspname = 'queen' AND cl.relkind = 'r'
                AND a.attname = 'tenant_id' AND a.attnum > 0 AND NOT a.attisdropped
              ORDER BY 1",
            &[],
        )
        .await
        .expect("catalog probe");
    let live: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    let mut expected: Vec<String> = TENANT_TABLES.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(
        live, expected,
        "the set of queen.* tables carrying tenant_id changed. Update BOTH this list and \
         C_TENANT_TABLES in 031_tenant_purge.sql — the function refuses to run otherwise, \
         on purpose"
    );
    assert!(
        !live.iter().any(|t| t == "queen.consumer_watermarks"),
        "consumer_watermarks must NOT carry tenant_id: it is keyed by queue_id and \
         cascades. FINDINGS §2 is wrong about this one"
    );

    // ========================================================================
    section("seed two tenants that share every name");
    // ========================================================================
    seed(&c, TENANT_A, "A").await;
    seed(&c, TENANT_B, "B").await;

    for t in TENANT_TABLES {
        assert!(tenant_rows(&c, t, TENANT_A).await > 0, "seed left {t} empty for A");
        assert!(tenant_rows(&c, t, TENANT_B).await > 0, "seed left {t} empty for B");
    }

    // The queue-scoped side, captured by id BEFORE the purge. This is not convenience:
    // queen.log_txns, queen.log_dlq, queen.message_traces and queen.retention_history are
    // all FK-LESS by design, so once the partitions are gone a join-based count reports 0
    // whether the rows were deleted or merely orphaned. Only a count against the ids
    // captured here can tell those two apart — and orphaning is exactly the bug.
    let a_ids = queue_ids(&c, TENANT_A).await;
    let b_ids = queue_ids(&c, TENANT_B).await;
    assert_eq!(a_ids.len(), 2, "A has two queues");
    assert_eq!(b_ids.len(), 2, "B has two queues");
    assert!(
        a_ids.iter().all(|id| !b_ids.contains(id)),
        "same names, different ids — that is the whole reason a purge can be scoped at all"
    );
    let a_parts = partition_ids(&c, &a_ids).await;
    let b_parts = partition_ids(&c, &b_ids).await;
    assert_eq!(a_parts.len(), 2, "A's two queues each got a partition from the push");
    assert_eq!(b_parts.len(), 2, "B's two queues each got a partition from the push");

    let b_before = queue_scoped_counts(&c, &b_ids, &b_parts).await;
    assert!(b_before.iter().all(|(_, n)| *n > 0), "B's queue-scoped seed: {b_before:?}");

    // ========================================================================
    section("purge tenant A");
    // ========================================================================
    let res = purge(&c, TENANT_A, 100).await;
    println!("{res}");
    assert_eq!(res["done"], serde_json::json!(true), "one call was enough: {res}");
    assert_eq!(res["phase"], serde_json::json!("complete"));
    assert_eq!(res["queues"]["deleted"], serde_json::json!(2));
    assert_eq!(res["queues"]["remaining"], serde_json::json!(0));
    // The nine non-queue-scoped tables each reported work. This is the assertion that
    // fails if someone "simplifies" the function into a queue-delete loop.
    for t in TENANT_TABLES.iter().filter(|t| **t != "queen.queues") {
        assert!(
            res["rows"][*t].as_i64().unwrap_or(0) > 0,
            "{t} reported no deleted rows, but the seed put one there: {res}"
        );
    }
    assert!(res["rows"]["queen.message_traces"].as_i64().unwrap_or(0) > 0);
    assert!(res["rows"]["queen.retention_history"].as_i64().unwrap_or(0) > 0);
    assert!(
        res["rows"]["queen_streams.state"].as_i64().unwrap_or(0) > 0,
        "the streaming schema is outside every cascade; the purge must reach it: {res}"
    );

    // The DOCUMENTED residue, asserted so it stays a decision rather than a surprise:
    // queen_streams.queries carries no tenant and its name is globally unique, so it
    // cannot be scoped to one tenant at all. It holds a name, two queue names and a
    // config hash — no payload-derived data — and dropping a query is the streaming
    // application's own operation.
    let queries_left: i64 =
        count(&c, "SELECT count(*) FROM queen_streams.queries WHERE name = 'sv-A'", &[]).await;
    assert_eq!(
        queries_left, 1,
        "queen_streams.queries is the documented residue (031's header); if this changed, \
         the header must change with it"
    );

    // ========================================================================
    section("zero rows remain for A, in every tenant-carrying table");
    // ========================================================================
    for t in TENANT_TABLES {
        assert_eq!(tenant_rows(&c, t, TENANT_A).await, 0, "{t} still holds A rows");
    }

    // ...and in everything reachable only through A's queue and partition ids, including
    // the four FK-less tables that a cascade cannot touch and that would otherwise be
    // left as orphans no query could ever attribute again.
    for (label, n) in queue_scoped_counts(&c, &a_ids, &a_parts).await {
        assert_eq!(n, 0, "{label} still holds rows for A's queues");
    }

    // ========================================================================
    section("B is untouched — every row, not just every table");
    // ========================================================================
    for t in TENANT_TABLES {
        assert!(tenant_rows(&c, t, TENANT_B).await > 0, "{t} lost B's rows");
    }
    assert_eq!(
        queue_scoped_counts(&c, &b_ids, &b_parts).await,
        b_before,
        "B's queue-scoped rows changed while A was purged"
    );
    // The KV value is the sharpest check: same tenant-less key, same namespace, and only
    // the tenant column separates them.
    let b_kv: i64 = count(
        &c,
        "SELECT count(*) FROM queen.kv
          WHERE tenant_id = $1::text::uuid AND namespace = 'saga' AND key = 'done-42'",
        &[&TENANT_B],
    )
    .await;
    assert_eq!(b_kv, 1, "B's KV row shares its name with the one just deleted, and survived");

    // ========================================================================
    section("idempotent: the same call again is a no-op that says so");
    // ========================================================================
    let again = purge(&c, TENANT_A, 100).await;
    assert_eq!(again["done"], serde_json::json!(true), "{again}");
    assert_eq!(again["queues"]["deleted"], serde_json::json!(0));
    for t in TENANT_TABLES.iter().filter(|t| **t != "queen.queues") {
        assert_eq!(again["rows"][*t], serde_json::json!(0), "{t} on the second run: {again}");
    }
    for t in TENANT_TABLES {
        assert!(tenant_rows(&c, t, TENANT_B).await > 0, "{t} lost B's rows on the re-run");
    }

    // ========================================================================
    section("timers are phase 1: the budget stops the wipe before any queue is deleted");
    // Not a performance detail. A timer fires from the sweeper and never passes through
    // the proxy, so a timer that outlives its queue pushes into it — and push
    // auto-creates the queue the wipe just removed.
    // ========================================================================
    c.execute(
        "INSERT INTO queen.log_timers(tenant_id, queue, timer_key, \"partition\", deliver_at,
                                      txn, message_id, payload, payload_zstd, encrypted,
                                      producer_sub)
         SELECT $1::text::uuid, 'orders', 'tk' || g, 'Default', now() + interval '1 hour',
                'x' || g, gen_random_uuid(), convert_to('{}', 'UTF8'), false, false, 's'
           FROM generate_series(1, 4) g",
        &[&TENANT_B],
    )
    .await
    .expect("extra timers");

    let row = c
        .query_one(
            // max_queues 100, max_rows 2 -> the timer sweep cannot drain in one call
            "SELECT (queen.delete_tenant_data_v1($1::text::uuid, 100, 2))::text",
            &[&TENANT_B],
        )
        .await
        .expect("budgeted purge");
    let stalled: serde_json::Value =
        serde_json::from_str(&row.get::<_, String>(0)).expect("json");
    assert_eq!(stalled["done"], serde_json::json!(false), "{stalled}");
    assert_eq!(
        stalled["phase"],
        serde_json::json!("timers"),
        "the call must stop in the timer phase, not carry on into the queues: {stalled}"
    );
    assert_eq!(
        stalled["queues"]["deleted"],
        serde_json::json!(0),
        "not one queue may be deleted while a timer of that tenant is still alive: {stalled}"
    );
    assert_eq!(
        tenant_rows(&c, "queen.queues", TENANT_B).await,
        2,
        "B's queues are still there, as the result claimed"
    );

    // ========================================================================
    section("bounded per call: a queue's traces are budgeted too, BEFORE it is deleted");
    // The one statement that decides how long the wipe transaction runs on a live cell.
    // It used to take no LIMIT at all: at the tightest budget the API allows
    // (p_max_queues => 1, p_max_rows => 1) one call deleted all 50 000 message_traces
    // rows of a single queue in ONE statement, each cascading into
    // queen.message_trace_names. Bounding it is only correct if the queue survives the
    // call — otherwise the partition_ids that attribute the rest are gone — so both
    // halves are pinned here.
    // ========================================================================
    // Its OWN tenant, deliberately: this section leaves a half-purged queue behind, and
    // the sections around it count B's queues. TENANT_C is created and drained here.
    c.execute(
        "SELECT queen.configure_queue_v1('orders', '{}'::jsonb, $1::text::uuid)",
        &[&TENANT_C],
    )
    .await
    .expect("C's queue");
    c.execute(
        "SELECT queen.log_push_one_v1('orders', 'Default', 1, decode(repeat('ab',16),'hex'), 0,
                                      convert_to('c', 'UTF8'), NULL, NULL, $1::text::uuid)",
        &[&TENANT_C],
    )
    .await
    .expect("C's push");
    c.execute(
        "INSERT INTO queen.message_traces(partition_id, transaction_id, consumer_group,
                                          event_type, data)
         SELECT p.id, 'bulk' || g, 'g1', 'push', '{}'::jsonb
           FROM queen.log_partitions p
           JOIN queen.queues q ON q.id = p.queue_id
          CROSS JOIN generate_series(1, 6) g
          WHERE q.tenant_id = $1::text::uuid AND q.name = 'orders'",
        &[&TENANT_C],
    )
    .await
    .expect("bulk traces");
    let row = c
        .query_one(
            "SELECT (queen.delete_tenant_data_v1($1::text::uuid, 100, 2))::text",
            &[&TENANT_C],
        )
        .await
        .expect("budgeted trace purge");
    let residue: serde_json::Value = serde_json::from_str(&row.get::<_, String>(0)).expect("json");
    assert_eq!(residue["done"], serde_json::json!(false), "{residue}");
    assert_eq!(
        residue["phase"],
        serde_json::json!("queue-residue"),
        "the call must stop on the queue whose traces are not drained: {residue}"
    );
    assert_eq!(
        residue["rows"]["queen.message_traces"],
        serde_json::json!(2),
        "the trace delete must respect p_max_rows. `ctid IN (SELECT ... LIMIT n)` does \
         NOT: the planner turns it into a semi-join and re-runs the LIMIT per outer row, \
         which deleted every row of the tenant in one statement: {residue}"
    );
    assert_eq!(
        count(
            &c,
            "SELECT count(*) FROM queen.queues
              WHERE tenant_id = $1::text::uuid AND name = 'orders'",
            &[&TENANT_C],
        )
        .await,
        1,
        "the queue was deleted while its traces were still there — the rest are now \
         orphans no query can attribute: {residue}"
    );
    assert!(
        count(
            &c,
            "SELECT count(*) FROM queen.message_traces t
               JOIN queen.log_partitions p ON p.id = t.partition_id
               JOIN queen.queues q ON q.id = p.queue_id
              WHERE q.tenant_id = $1::text::uuid AND q.name = 'orders'",
            &[&TENANT_C],
        )
        .await > 0,
        "the fixture is meaningless unless traces are actually left over: {residue}"
    );
    // ... and the loop still converges on the SAME tight budget, which is the other half
    // of the contract: bounded per call, and finished after enough calls.
    let mut calls = 1;
    loop {
        let row = c
            .query_one(
                "SELECT (queen.delete_tenant_data_v1($1::text::uuid, 100, 2))::text",
                &[&TENANT_C],
            )
            .await
            .expect("budgeted purge");
        let r: serde_json::Value = serde_json::from_str(&row.get::<_, String>(0)).expect("json");
        calls += 1;
        assert!(calls <= 20, "the trace budget did not converge in 20 calls: {r}");
        if r["done"] == serde_json::json!(true) {
            break;
        }
    }
    assert!(
        calls > 2,
        "two rows per call cannot have drained six traces plus the queue in {calls} call(s) \
         — the LIMIT is not being applied"
    );
    assert_eq!(tenant_rows(&c, "queen.queues", TENANT_C).await, 0, "C is drained");

    // ========================================================================
    section("bounded per call: p_max_queues walks the queues one at a time");
    // ========================================================================
    let mut calls = 0;
    loop {
        let r = purge(&c, TENANT_B, 1).await;
        calls += 1;
        assert!(calls <= 10, "purge did not converge in 10 calls: {r}");
        if r["done"] == serde_json::json!(true) {
            break;
        }
        assert!(
            r["queues"]["deleted"].as_i64().unwrap_or(0) <= 1,
            "p_max_queues=1 deleted more than one queue: {r}"
        );
    }
    assert!(calls > 1, "the budget must have forced more than one call, got {calls}");
    for t in TENANT_TABLES {
        assert_eq!(tenant_rows(&c, t, TENANT_B).await, 0, "{t} still holds B rows");
    }

    // ========================================================================
    section("bounded per call: p_max_rows walks the non-queue-scoped sweep too");
    // The third and last return path. `phase: "sweep"` is not a failure — it means a
    // bounded delete hit its budget — and it is the answer a tenant with a large KV space
    // gets, where nothing else about the wipe is interesting.
    // ========================================================================
    c.execute(
        "SELECT queen.kv_apply_v1(
             (SELECT jsonb_agg(jsonb_build_object('op','put','ns','bulk','key','k'||g,
                                                  'value', to_jsonb(g), 'forever', true))
                FROM generate_series(1, 5) g),
             $1::text::uuid, now(), false)",
        &[&TENANT_A],
    )
    .await
    .expect("bulk kv");
    let row = c
        .query_one(
            // no queues, no timers -> the call goes straight through to the sweep
            "SELECT (queen.delete_tenant_data_v1($1::text::uuid, 100, 2))::text",
            &[&TENANT_A],
        )
        .await
        .expect("budgeted sweep");
    let swept: serde_json::Value = serde_json::from_str(&row.get::<_, String>(0)).expect("json");
    assert_eq!(swept["done"], serde_json::json!(false), "{swept}");
    assert_eq!(swept["phase"], serde_json::json!("sweep"), "{swept}");
    assert_eq!(swept["rows"]["queen.kv"], serde_json::json!(2), "the budget is per table: {swept}");
    let mut calls = 0;
    while purge(&c, TENANT_A, 100).await["done"] != serde_json::json!(true) {
        calls += 1;
        assert!(calls <= 10, "the sweep did not converge in 10 calls");
    }
    assert_eq!(tenant_rows(&c, "queen.kv", TENANT_A).await, 0, "the sweep drained");

    // ========================================================================
    section("guard: the DEFAULT tenant is refused unless asked for twice");
    // On a single-tenant self-hosted broker the default tenant IS the installation.
    // ========================================================================
    let err = c
        .query_one("SELECT (queen.delete_tenant_data_v1($1::text::uuid))::text", &[&TENANT_DEFAULT])
        .await
        .expect_err("the default tenant must be refused");
    let msg = db_message(&err);
    assert!(
        msg.contains("DEFAULT tenant"),
        "the refusal must name what it is protecting, got: {msg}"
    );
    let ok = c
        .query_one(
            "SELECT (queen.delete_tenant_data_v1($1::text::uuid, 100, 50000, true))::text",
            &[&TENANT_DEFAULT],
        )
        .await;
    assert!(ok.is_ok(), "p_allow_default_tenant => true must be the way through: {ok:?}");

    // ========================================================================
    section("guard: a tenant-carrying table the function does not know about is fatal");
    // The failure this closes is the worst one available to a purge: it reports success
    // and leaves customer data behind, on the one code path whose entire job is that
    // this does not happen.
    // ========================================================================
    c.batch_execute("CREATE TABLE queen.zz_wipe_probe(tenant_id UUID NOT NULL, k TEXT)")
        .await
        .expect("probe table");
    let err = c
        .query_one("SELECT (queen.delete_tenant_data_v1($1::text::uuid))::text", &[&TENANT_A])
        .await
        .expect_err("an unknown tenant-carrying table must abort the purge");
    let msg = db_message(&err);
    assert!(
        msg.contains("zz_wipe_probe") && msg.contains("C_TENANT_TABLES"),
        "the refusal must name the table AND where to fix it, got: {msg}"
    );
    c.batch_execute("DROP TABLE queen.zz_wipe_probe").await.expect("drop probe");

    // And the purge works again once the schema is back to something it covers.
    let after = purge(&c, TENANT_A, 100).await;
    assert_eq!(after["done"], serde_json::json!(true), "{after}");

    println!("\nOK — tenant wipe purges one tenant completely and leaves the other intact.");
}

// ------------------------------------------------------------------ helpers

/// A tenant's queue ids, as text — `tokio-postgres` is not built with the `uuid` feature
/// here, and every SQL site casts back with `::text[]::uuid[]`, the same idiom the rest of
/// the suite uses.
async fn queue_ids(c: &Client, tenant: &str) -> Vec<String> {
    c.query(
        "SELECT id::text FROM queen.queues WHERE tenant_id = $1::text::uuid ORDER BY name",
        &[&tenant],
    )
    .await
    .expect("queue ids")
    .iter()
    .map(|r| r.get::<_, String>(0))
    .collect()
}

/// The partition ids under a set of queues. Captured BEFORE a purge, because after one
/// the partitions are gone and the FK-less tables keyed on `partition_id` can no longer be
/// attributed to anyone.
async fn partition_ids(c: &Client, queue_ids: &[String]) -> Vec<String> {
    let ids: Vec<String> = queue_ids.to_vec();
    c.query(
        "SELECT id::text FROM queen.log_partitions
          WHERE queue_id = ANY($1::text[]::uuid[]) ORDER BY 1",
        &[&ids],
    )
    .await
    .expect("partition ids")
    .iter()
    .map(|r| r.get::<_, String>(0))
    .collect()
}

/// Everything reachable ONLY through a set of queue/partition ids: what the FK cascades
/// off `queen.queues(id)` own, plus the four FK-less tables nothing cascades to. Returned
/// as an ordered `(label, count)` list so two snapshots compare directly.
///
/// Every probe binds the ids DIRECTLY rather than joining through `log_partitions` — a
/// join would report 0 for an orphaned row just as happily as for a deleted one, and
/// telling those apart is the entire job of the four FK-less entries below.
async fn queue_scoped_counts(
    c: &Client,
    queue_ids: &[String],
    partition_ids: &[String],
) -> Vec<(&'static str, i64)> {
    let q: Vec<String> = queue_ids.to_vec();
    let p: Vec<String> = partition_ids.to_vec();
    let by_queue: &[(&'static str, &'static str)] = &[
        ("log_partitions", "queen.log_partitions"),
        ("consumer_watermarks", "queen.consumer_watermarks"),
        ("queue_lag_metrics", "queen.queue_lag_metrics"),
        ("stats", "queen.stats"),
        ("consumer_groups_metadata(queue-scoped)", "queen.consumer_groups_metadata"),
    ];
    let by_partition: &[(&'static str, &'static str)] = &[
        ("log_segments", "queen.log_segments"),
        ("log_consumers", "queen.log_consumers"),
        // FK-LESS by design (the purge path must never pay FK-trigger cost —
        // 001_log_schema), so these are cleared only by the explicit statements inside
        // delete_queue_v1 and delete_tenant_data_v1.
        ("log_txns", "queen.log_txns"),
        ("log_dlq", "queen.log_dlq"),
        ("message_traces", "queen.message_traces"),
        ("retention_history", "queen.retention_history"),
        // A SECOND SCHEMA, outside every cascade: payload-derived aggregate values
        // keyed by a partition id with no FK (002_streams_schema declines it on
        // purpose). Missed by the first version of the purge, which was scoped to
        // nspname = 'queen' — the rows survived and their partition_id then joined to
        // nothing, which is data left behind AND permanently unattributable.
        ("queen_streams.state", "queen_streams.state"),
    ];

    let mut out = Vec::with_capacity(by_queue.len() + by_partition.len() + 1);
    for (label, table) in by_queue {
        let sql = format!("SELECT count(*) FROM {table} WHERE queue_id = ANY($1::text[]::uuid[])");
        out.push((*label, count(c, &sql, &[&q]).await));
    }
    for (label, table) in by_partition {
        let sql =
            format!("SELECT count(*) FROM {table} WHERE partition_id = ANY($1::text[]::uuid[])");
        out.push((*label, count(c, &sql, &[&p]).await));
    }
    out.push((
        "message_trace_names",
        count(
            c,
            "SELECT count(*) FROM queen.message_trace_names n
               JOIN queen.message_traces t ON t.id = n.trace_id
              WHERE t.partition_id = ANY($1::text[]::uuid[])",
            &[&p],
        )
        .await,
    ));
    out
}
