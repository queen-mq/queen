//! `queen.kv_quota_refresh_v1` — the contract between the quota tables and the
//! in-process enforcer (PLAN_KV_TIMERS.md §9.3, §9.4; phase F5).
//!
//! WHY THIS IS A TEST AGAINST SQL AND NOT AGAINST THE BROKER. The enforcement
//! logic is pure and already pinned by `src/tests_unit/quota_gate.rs` and
//! `quota_overshoot.rs`, which run in a plain `cargo test`. What those cannot
//! see is the JOIN: whether the row the operator wrote and the row the rollup
//! wrote arrive at the broker as ONE tenant, with the right fields, in the right
//! order, and — the case that decides whether a cell is usable at all —
//! **whether a tenant that has been GRANTED but has never written appears.**
//!
//! THE FAILURE THIS FILE EXISTS TO CATCH. §9.4 point 1 makes the absence of a
//! `queen.kv_quota` row a DENIAL when tenancy is on. So if this function reached
//! for `queen.kv_usage` first and joined the quota onto it — the shape anyone
//! writes by reflex, because usage is the bigger table — a tenant the operator
//! had just granted, and which had not yet written a byte, would be MISSING from
//! the snapshot and would be told `feature_gated` on its very first request. The
//! customer's first impression of the feature would be a 403, and the operator
//! would find a correct-looking quota row in the database. That is why the join
//! is FULL OUTER and why this test asserts the empty-usage direction explicitly.
//!
//! The field names below are the ones `src/quota.rs::parse` reads. They are a
//! contract between two files that cannot see each other, so they are asserted
//! literally here rather than through a helper.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`:
//!
//! ```bash
//! docker run --rm -d --name queen-q-pg -e POSTGRES_PASSWORD=postgres -p 5474:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5474 cargo test --test kv_quota_refresh -- --ignored --nocapture
//! ```

use queen::{Broker, BrokerConfig};
use serde_json::Value;

/// Granted AND measured: the ordinary tenant.
const BOTH: &str = "11111111-1111-1111-1111-111111111111";
/// Granted, never wrote a byte. The case that decides whether a freshly
/// provisioned customer meets its quota or a 403.
const GRANTED_ONLY: &str = "22222222-2222-2222-2222-222222222222";
/// Measured, never granted. Legal with tenancy off (NULL is unlimited) and a
/// denial with tenancy on — the broker decides, but only if it SEES the tenant.
const USAGE_ONLY: &str = "33333333-3333-3333-3333-333333333333";
/// Granted and explicitly switched off by the control plane.
const DISABLED: &str = "44444444-4444-4444-4444-444444444444";
/// Granted, and nearly full: the one that must survive a top-N cap.
const NEARLY_FULL: &str = "55555555-5555-5555-5555-555555555555";

async fn connect(host: &str, port: u16, db: &str) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres password=postgres dbname={db}"),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap_or_else(|e| panic!("connect to {db}: {e}"));
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

async fn refresh(c: &tokio_postgres::Client, max_tenants: i32) -> Vec<Value> {
    let txt: String = c
        .query_one(
            "SELECT (queen.kv_quota_refresh_v1($1::int))::text",
            &[&max_tenants],
        )
        .await
        .expect("kv_quota_refresh_v1")
        .get(0);
    serde_json::from_str::<Value>(&txt)
        .expect("result is JSON")
        .as_array()
        .expect("result is an ARRAY; src/quota.rs::parse iterates it directly")
        .clone()
}

fn find<'a>(rows: &'a [Value], tenant: &str) -> Option<&'a Value> {
    rows.iter()
        .find(|r| r.get("tenant").and_then(|v| v.as_str()) == Some(tenant))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn the_refresh_joins_quota_and_usage_without_losing_either_side() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    let admin = connect(&host, port, "postgres").await;
    for row in admin
        .query(
            "SELECT datname FROM pg_database WHERE datname LIKE 'kvt\\_quota\\_%'",
            &[],
        )
        .await
        .expect("list leftovers")
    {
        let old: String = row.get(0);
        let _ = admin
            .execute(&format!("DROP DATABASE IF EXISTS \"{old}\" WITH (FORCE)"), &[])
            .await;
    }
    let db = format!(
        "kvt_quota_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    admin
        .execute(&format!("CREATE DATABASE \"{db}\""), &[])
        .await
        .expect("create database");

    let result = run(&host, port, &db).await;

    let _ = admin
        .execute(&format!("DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)"), &[])
        .await;

    if let Err(report) = result {
        panic!("\nPLAN_KV_TIMERS §9.3/§9.4 — quota refresh contract (F5)\n{report}\n");
    }
}

async fn run(host: &str, port: u16, db: &str) -> Result<(), String> {
    let mut f: Vec<String> = Vec::new();

    let broker = Broker::start(
        BrokerConfig::new()
            .pg(host.to_string(), port, "postgres", "postgres", db.to_string())
            .pool_size(4)
            .retention(false)
            .stats_refresh(false)
            .system_metrics(false)
            .log_reports(false),
    )
    .await
    .map_err(|e| format!("schema apply failed: {e:?}"))?;

    let c = connect(host, port, db).await;

    // An EMPTY cell answers with an empty array and not with NULL: `parse` reads
    // `as_array()` and a NULL would silently become "no tenants known", which
    // with `require_grant` on is "deny everybody".
    let rows = refresh(&c, 100).await;
    if !rows.is_empty() {
        f.push(format!("  ✗ a virgin cell returned {} rows, expected 0", rows.len()));
    }

    // ------------------------------------------------------------ the fixture
    c.execute(
        "INSERT INTO queen.kv_quota
             (tenant_id, enabled, max_rows, max_bytes, max_timers, max_timer_horizon_s,
              max_reads_per_sec, max_writes_per_sec)
         VALUES ($1::text::uuid, TRUE,  1000, 1048576, 500, 3600, 50, 25),
                ($2::text::uuid, TRUE,  1000, NULL,    500, NULL, NULL, NULL),
                ($3::text::uuid, FALSE, 1000, NULL,    500, NULL, NULL, NULL),
                ($4::text::uuid, TRUE,  1000, NULL,    500, NULL, NULL, NULL)",
        &[&BOTH, &GRANTED_ONLY, &DISABLED, &NEARLY_FULL],
    )
    .await
    .map_err(|e| format!("seed kv_quota: {e}"))?;
    c.execute(
        "INSERT INTO queen.kv_usage
             (tenant_id, computed_at, kv_rows, kv_bytes, timer_rows, timer_oldest)
         VALUES ($1::text::uuid, now(), 100, 2048, 7, now()),
                ($2::text::uuid, now(), 999, 4096, 0, NULL),
                ($3::text::uuid, now(),  50,  512, 1, NULL)",
        &[&BOTH, &NEARLY_FULL, &USAGE_ONLY],
    )
    .await
    .map_err(|e| format!("seed kv_usage: {e}"))?;

    let rows = refresh(&c, 100).await;

    // ---------------------------------------------------------------- (1) both
    match find(&rows, BOTH) {
        None => f.push("  ✗ the granted+measured tenant is missing entirely".into()),
        Some(r) => {
            let g = |k: &str| r.get(k).cloned().unwrap_or(Value::Null);
            for (k, want) in [
                ("granted", Value::Bool(true)),
                ("enabled", Value::Bool(true)),
                ("maxRows", Value::from(1000)),
                ("maxBytes", Value::from(1_048_576)),
                ("maxTimers", Value::from(500)),
                ("maxHorizonS", Value::from(3600)),
                ("maxReadsPerSec", Value::from(50)),
                ("maxWritesPerSec", Value::from(25)),
                ("kvRows", Value::from(100)),
                ("kvBytes", Value::from(2048)),
                ("timerRows", Value::from(7)),
            ] {
                if g(k) != want {
                    f.push(format!(
                        "  ✗ {k} = {} , expected {want}\n      These names are the contract \
                         `src/quota.rs::parse` reads; a rename here is a silent \
                         unlimited-everything on the broker side, because a field it cannot \
                         find reads as NULL and NULL means unlimited.",
                        g(k)
                    ));
                }
            }
            // The measurement's identity. The broker clears its local delta ONLY
            // when it adopts a measurement it has not seen before, and this is
            // the field it compares. A constant zero would make every refresh
            // look identical, the delta would never clear, and a tenant would
            // block permanently on writes it had already had counted.
            match g("computedAtMs").as_i64() {
                Some(ms) if ms > 1_600_000_000_000 => {}
                other => f.push(format!(
                    "  ✗ computedAtMs = {other:?}, expected epoch milliseconds. This is how \
                     the broker tells a NEW measurement from the same one read again \
                     (§9.3); a constant would freeze every local delta for ever."
                )),
            }
        }
    }

    // -------------------------------------------------- (2) granted, no usage
    // THE ONE THAT DECIDES WHETHER A NEW CUSTOMER WORKS.
    match find(&rows, GRANTED_ONLY) {
        None => f.push(
            "  ✗ a tenant with a quota row and NO usage row is MISSING from the snapshot\n      \
             plan: §9.4 point 1 — with tenancy on, absence from the snapshot is a DENIAL, so \
             this tenant would be told `feature_gated` on its first ever request while a \
             perfectly good quota row sat in the database. The join must be FULL OUTER, and \
             the usage columns must COALESCE to zero rather than to NULL."
                .into(),
        ),
        Some(r) => {
            if r.get("granted") != Some(&Value::Bool(true)) {
                f.push("  ✗ a tenant with a quota row is not marked granted".into());
            }
            for k in ["kvRows", "kvBytes", "timerRows"] {
                if r.get(k).and_then(|v| v.as_i64()) != Some(0) {
                    f.push(format!(
                        "  ✗ {k} for a tenant with no usage row is {:?}, expected 0 — a NULL \
                         here would parse as absent and the gate would treat an unmeasured \
                         tenant as unmeasurable",
                        r.get(k)
                    ));
                }
            }
        }
    }

    // -------------------------------------------------- (3) usage, no grant
    match find(&rows, USAGE_ONLY) {
        None => f.push(
            "  ✗ a tenant with usage and no quota row is missing; the broker must SEE it to \
             decide (unlimited with tenancy off, denied with it on) — an invisible tenant is \
             denied by accident rather than by rule"
                .into(),
        ),
        Some(r) => {
            if r.get("granted") != Some(&Value::Bool(false)) {
                f.push(
                    "  ✗ a tenant with no quota row is marked granted; `granted:false` and \
                     `enabled:false` are DIFFERENT facts and collapsing them either denies a \
                     tenant nobody restricted or admits one nobody authorised"
                        .into(),
                );
            }
        }
    }

    // ------------------------------------------------------- (4) disabled row
    match find(&rows, DISABLED) {
        None => f.push("  ✗ an explicitly disabled tenant is missing from the snapshot".into()),
        Some(r) => {
            if r.get("enabled") != Some(&Value::Bool(false)) {
                f.push("  ✗ enabled=false did not survive the join".into());
            }
        }
    }

    // ----------------------------------------------- (5) the top-N cap's order
    // With room for ONE tenant, the one that survives must be the one closest to
    // a limit. Dropping the tenant at 99.9% in favour of one at 10% would turn
    // the enforcer off for exactly the tenant it exists for.
    let one = refresh(&c, 1).await;
    if one.len() != 1 {
        f.push(format!("  ✗ p_max_tenants=1 returned {} rows", one.len()));
    } else if one[0].get("tenant").and_then(|v| v.as_str()) != Some(NEARLY_FULL) {
        f.push(format!(
            "  ✗ the cap kept {:?}, not the tenant at 99.9% of its row quota\n      plan: §9.4 \
             point 2 — the snapshot is bounded, so the ORDER decides who keeps being \
             enforced. Nearest to a limit first.",
            one[0].get("tenant")
        ));
    }

    // ------------------------------------------------ (6) it must never count
    // The refresh runs every 30 s on EVERY broker. A count over queen.kv here
    // would make the cost of enforcing the limit proportional to the data being
    // limited — the counting belongs to the rollup, once every 300 s, on one
    // broker. Checked on the plan, not by timing, so it cannot flake.
    let body: String = c
        .query_one(
            "SELECT prosrc FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
              WHERE n.nspname = 'queen' AND p.proname = 'kv_quota_refresh_v1'",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or_default();
    for forbidden in ["queen.kv ", "queen.kv\n", "queen.log_timers"] {
        if body.contains(forbidden) {
            f.push(format!(
                "  ✗ the refresh reads `{}`\n      It runs every 30 s on every broker. Only \
                 queen.kv_quota and queen.kv_usage may be read here; the counting lives in the \
                 rollup (026_kv_sweeper.sql), once per 300 s, on one broker.",
                forbidden.trim()
            ));
        }
    }
    // …and it must be read-only, or "every broker, every 30 s" becomes a write
    // storm on a table with one row per tenant.
    let volatile: String = c
        .query_one(
            "SELECT p.provolatile::text FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
              WHERE n.nspname = 'queen' AND p.proname = 'kv_quota_refresh_v1'",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or_default();
    if volatile != "s" {
        f.push(format!(
            "  ✗ kv_quota_refresh_v1 is provolatile={volatile}, expected 's' (STABLE). A \
             VOLATILE function here would be free to write, and this is the one function in \
             the feature that must not."
        ));
    }

    // ------------------------------------- (7) the ADD COLUMN rule of §3.2
    //
    // `CREATE TABLE IF NOT EXISTS` is a SILENT NO-OP ON THE SHAPE. A cell that
    // booted on an earlier build keeps the table it already has, and the first
    // read of a column added since fails with `42703` — in production, on the
    // schedule path, classified as configuration and therefore never retried.
    // The always-virgin model covers the SCHEMA and the FUNCTIONS; it does not
    // cover the DATA of a configuration table an operator has written and which
    // therefore cannot be dropped.
    //
    // The plan's answer is `CREATE TABLE IF NOT EXISTS` FOLLOWED BY one
    // `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` per column — the shape
    // `019_worker_metrics.sql:95-119` already uses for twelve columns in a row.
    // A forgotten ALTER is invisible on a virgin database and invisible on a
    // re-apply, so the only way to catch it is to MANUFACTURE the old shape.
    // That is what this leg does: drop a column, re-apply, require it back.
    broker.shutdown().await;
    let dropped = ["max_timers", "max_timer_horizon_s", "max_writes_per_sec"];
    for col in dropped {
        c.execute(
            &format!("ALTER TABLE queen.kv_quota DROP COLUMN {col}"),
            &[],
        )
        .await
        .map_err(|e| format!("simulating the old shape ({col}): {e}"))?;
    }
    c.execute("ALTER TABLE queen.kv_usage DROP COLUMN timer_oldest", &[])
        .await
        .map_err(|e| format!("simulating the old shape (timer_oldest): {e}"))?;

    let broker = Broker::start(
        BrokerConfig::new()
            .pg(host.to_string(), port, "postgres", "postgres", db.to_string())
            .pool_size(4)
            .retention(false)
            .stats_refresh(false)
            .system_metrics(false)
            .log_reports(false),
    )
    .await
    .map_err(|e| format!("re-apply over the OLD table shape failed: {e:?}"))?;

    for (table, col) in dropped
        .iter()
        .map(|c| ("kv_quota", *c))
        .chain(std::iter::once(("kv_usage", "timer_oldest")))
    {
        let present: bool = c
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns
                                 WHERE table_schema='queen' AND table_name=$1 AND column_name=$2)",
                &[&table, &col],
            )
            .await
            .map(|r| r.get(0))
            .unwrap_or(false);
        if !present {
            f.push(format!(
                "  ✗ queen.{table}.{col} did NOT come back after a re-apply over the old \
                 shape\n      plan: §3.2 — CREATE TABLE IF NOT EXISTS is a silent no-op on the \
                 shape, so this column needs its own `ALTER TABLE queen.{table} ADD COLUMN IF \
                 NOT EXISTS {col} ...` line in 024_kv.sql. Without it, a cell upgraded from an \
                 earlier build meets 42703 on the schedule path, in production, and the \
                 classifier calls it configuration and never retries."
            ));
        }
    }
    // …and the refresh must still answer over the healed table, since that is the
    // read whose failure the ALTERs exist to prevent.
    if let Err(e) = c
        .query_one("SELECT (queen.kv_quota_refresh_v1(10))::text", &[])
        .await
    {
        f.push(format!(
            "  ✗ the refresh failed after a re-apply over the old shape: {e}"
        ));
    }

    broker.shutdown().await;

    if f.is_empty() {
        Ok(())
    } else {
        Err(f.join("\n"))
    }
}
