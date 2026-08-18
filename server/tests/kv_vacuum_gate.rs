//! THE VACUUM GATE — the exit criterion of F5 (PLAN_KV_TIMERS.md §15, row
//! "Vacuum gate"; §16 F5): *"soak che dimostra assenza di wobble con
//! `vacuum_truncate = off`, piu' il numero di worker autovacuum occupati dalle
//! tabelle nuove in stato stazionario contro `autovacuum_max_workers`."*
//!
//! WHAT "WOBBLE" IS, AND WHY IT NEEDS A SOAK AND NOT A REVIEW. On 2026-07-24
//! this cluster was caught live, with `pg_stat_progress_vacuum` in phase
//! `truncating heap`, holding an ACCESS EXCLUSIVE lock on a hot table
//! (`001_log_schema.sql:54-63`). Heap truncation is the last phase of a vacuum:
//! it takes the strongest lock there is, and every reader and writer of the
//! table stops dead until it finishes. `queen.kv` is EXACTLY the shape that
//! provokes it — it empties and refills in steady state, which is the entire
//! point of `expires_at` — so §3.2 sets `vacuum_truncate = off` on it and on
//! `queen.log_timers`.
//!
//! A reloption is easy to assert and proves almost nothing: what matters is that
//! the setting is IN FORCE at run time, under churn, on a table autovacuum is
//! actually visiting. This file therefore does four things:
//!
//!   1. asserts the storage parameters §3.2 prescribes, table by table, INCLUDING
//!      the asymmetry that is easy to "tidy up" into a bug: `cost_delay = 0` on
//!      the two hot tables and NOT on the two configuration tables, because
//!      `autovacuum_max_workers` is a GLOBAL budget of three and four unthrottled
//!      tables would compete for it with `log_partitions`, `log_consumers` and
//!      `log_segments`, which the engine depends on;
//!   2. runs a steady-state churn soak (write, expire, prune, repeat) and watches
//!      `pg_stat_progress_vacuum` and `pg_locks`: the gate FAILS if the truncating
//!      phase or an ACCESS EXCLUSIVE lock is ever observed on the new tables;
//!   3. measures the wobble instead of asserting its absence by faith: a trivial
//!      probe against `queen.kv` is timed on every sample, and a stall of the kind
//!      an ACCESS EXCLUSIVE produces shows up as a maximum that dwarfs the median.
//!      The relation size is also sampled, and asserted never to DECREASE — which
//!      is the runtime signature of `vacuum_truncate = off` and the only proof
//!      that the reloption is not merely written down;
//!   4. counts how many autovacuum workers the four new relations occupy AT ONCE
//!      in steady state, and requires that count to stay strictly below
//!      `autovacuum_max_workers`, so the feature can never starve the engine's
//!      own tables of the last worker.
//!
//! THE ACCEPTED COST, stated so nobody reads (3) as a defect: with truncation off
//! the heap never gives pages back, so the peak is permanent. That is the trade
//! §3.2 makes deliberately — a permanent peak against an ACCESS EXCLUSIVE on the
//! hot path — and `queen_kv_expired_not_pruned` (§14.3 point 4) is the signal
//! that watches the other side of it.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`:
//!
//! ```bash
//! docker run --rm -d --name queen-vac-pg -e POSTGRES_PASSWORD=postgres -p 5472:5432 \
//!   postgres:16-alpine -c autovacuum_naptime=1s
//! QUEEN_EMBEDDED_TEST_PG=localhost:5472 cargo test --test kv_vacuum_gate -- --ignored --nocapture
//! ```
//!
//! `autovacuum_naptime=1s` is not required but makes the soak meaningful in
//! seconds instead of minutes: with the default 60 s an autovacuum may never
//! visit at all, and a gate that passes because nothing happened is not a gate.
//! The run prints how many autovacuum passes it actually observed, and SAYS SO
//! when that number is zero, rather than reporting a green it did not earn.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use queen::{Broker, BrokerConfig};

const TENANT: &str = "00000000-0000-0000-0000-000000000001";

/// The two hot tables (§3.2 and §3.3) and the two configuration tables.
const HOT: &[&str] = &["kv", "log_timers"];
const COLD: &[&str] = &["kv_quota", "kv_usage"];

/// Soak length. Long enough that autovacuum with `naptime=1s` visits several
/// times, short enough to live in a normal suite.
fn soak() -> Duration {
    Duration::from_secs(
        std::env::var("QUEEN_VACUUM_GATE_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(25),
    )
}

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

/// The reloptions of one relation, as a sorted `k=v` set.
async fn reloptions(c: &tokio_postgres::Client, rel: &str) -> Vec<String> {
    let row = c
        .query_one(
            "SELECT coalesce(ARRAY(SELECT unnest(c.reloptions) ORDER BY 1), '{}')
               FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = 'queen' AND c.relname = $1",
            &[&rel],
        )
        .await
        .unwrap_or_else(|e| panic!("reloptions for queen.{rel}: {e}"));
    row.get::<_, Vec<String>>(0)
}

// ===========================================================================
// (1) The storage parameters of §3.2 / §3.3
// ===========================================================================

async fn check_storage_params(c: &tokio_postgres::Client, failures: &mut Vec<String>) {
    for rel in HOT {
        let opts = reloptions(c, rel).await;
        let has = |k: &str| opts.iter().any(|o| o == k);
        for want in [
            "autovacuum_vacuum_scale_factor=0",
            "autovacuum_vacuum_threshold=500",
            "vacuum_truncate=off",
        ] {
            if !has(want) {
                failures.push(format!(
                    "  ✗ queen.{rel} is missing `{want}`\n      got: [{}]\n      plan: §3.2 — \
                     copied verbatim from queen.log_partitions (001_log_schema.sql:47-68). \
                     scale_factor 0 keeps autovacuum firing every naptime on a table whose dead \
                     tuples outnumber its live ones between passes; vacuum_truncate=off is the \
                     2026-07-24 wobble fix and must never be re-enabled.",
                    opts.join(", ")
                ));
            }
        }
        if !has("autovacuum_vacuum_cost_delay=0") {
            failures.push(format!(
                "  ✗ queen.{rel} is missing `autovacuum_vacuum_cost_delay=0`\n      got: [{}]",
                opts.join(", ")
            ));
        }
    }
    for rel in COLD {
        let opts = reloptions(c, rel).await;
        let has = |k: &str| opts.iter().any(|o| o == k);
        for want in [
            "autovacuum_vacuum_scale_factor=0",
            "autovacuum_vacuum_threshold=500",
            "vacuum_truncate=off",
        ] {
            if !has(want) {
                failures.push(format!(
                    "  ✗ queen.{rel} is missing `{want}`\n      got: [{}]",
                    opts.join(", ")
                ));
            }
        }
        // THE ASYMMETRY, and the reason it is a test: an unthrottled autovacuum
        // on a tiny configuration table buys nothing and spends one of three
        // GLOBAL workers (§3.2).
        if has("autovacuum_vacuum_cost_delay=0") {
            failures.push(format!(
                "  ✗ queen.{rel} sets `autovacuum_vacuum_cost_delay=0`, and §3.2 says only \
                 queen.kv and queen.log_timers may\n      autovacuum_max_workers is a GLOBAL \
                 budget (default 3). Four tables with scale_factor 0, threshold 500 and \
                 unthrottled I/O would contend for those workers with log_partitions, \
                 log_consumers and log_segments — the tables the engine depends on. These two \
                 are tiny and have no need to be aggressive."
            ));
        }
    }
}

// ===========================================================================
// (2) + (3) + (4) The soak
// ===========================================================================

#[derive(Default)]
struct Soak {
    samples: u32,
    /// Probe latencies, milliseconds. An ACCESS EXCLUSIVE stall shows up here.
    probe_ms: Vec<f64>,
    /// Every `pg_stat_progress_vacuum` phase ever observed on the new tables.
    phases: HashSet<String>,
    /// Autovacuum passes CAUGHT IN FLIGHT. Sampling, so it undercounts badly: a
    /// vacuum of a small table finishes in well under a millisecond and a poll
    /// every 100 ms will miss almost all of them. It is reported for context and
    /// is NOT what decides whether the run proved anything — `autovacuum_count`
    /// below is, because it is a cumulative counter and cannot be missed.
    vacuum_sightings: u32,
    /// The most workers seen on the new tables at the same instant.
    max_concurrent_workers: i64,
    /// ACCESS EXCLUSIVE observed on a new table, by anyone.
    access_exclusive: Vec<String>,
    /// `pg_relation_size(queen.kv)` over time — must be monotonic.
    sizes: Vec<i64>,
}

/// One observation of the cluster, taken while the churn runs.
async fn sample(c: &tokio_postgres::Client, s: &mut Soak) {
    s.samples += 1;

    // Probe: a plain indexed read of queen.kv. Under an ACCESS EXCLUSIVE this
    // blocks for the whole truncation, which is what "wobble" looked like.
    let t0 = Instant::now();
    let _ = c
        .query(
            "SELECT 1 FROM queen.kv WHERE tenant_id = $1::text::uuid AND namespace = 'probe' \
             AND key = 'probe'",
            &[&TENANT],
        )
        .await;
    s.probe_ms.push(t0.elapsed().as_secs_f64() * 1000.0);

    // Vacuum progress on the four relations.
    if let Ok(rows) = c
        .query(
            "SELECT c.relname, v.phase
               FROM pg_stat_progress_vacuum v
               JOIN pg_class c ON c.oid = v.relid
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = 'queen'
                AND c.relname IN ('kv','log_timers','kv_quota','kv_usage')",
            &[],
        )
        .await
    {
        if !rows.is_empty() {
            s.vacuum_sightings += 1;
        }
        for r in &rows {
            let rel: String = r.get(0);
            let phase: String = r.get(1);
            s.phases.insert(format!("{rel}:{phase}"));
        }
    }

    // Concurrent AUTOvacuum workers on the new relations. `pg_stat_progress_vacuum`
    // also reports manual VACUUMs, so the worker count is taken from
    // pg_stat_activity's backend_type, which is what the global budget counts.
    if let Ok(row) = c
        .query_one(
            "SELECT count(*)::bigint
               FROM pg_stat_progress_vacuum v
               JOIN pg_stat_activity a ON a.pid = v.pid
               JOIN pg_class c ON c.oid = v.relid
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = 'queen'
                AND c.relname IN ('kv','log_timers','kv_quota','kv_usage')
                AND a.backend_type = 'autovacuum worker'",
            &[],
        )
        .await
    {
        let n: i64 = row.get(0);
        s.max_concurrent_workers = s.max_concurrent_workers.max(n);
    }

    // Any ACCESS EXCLUSIVE on the new relations, held or waiting.
    if let Ok(rows) = c
        .query(
            "SELECT c.relname, l.mode, l.granted
               FROM pg_locks l
               JOIN pg_class c ON c.oid = l.relation
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = 'queen'
                AND c.relname IN ('kv','log_timers','kv_quota','kv_usage')
                AND l.mode = 'AccessExclusiveLock'",
            &[],
        )
        .await
    {
        for r in &rows {
            let rel: String = r.get(0);
            let granted: bool = r.get(2);
            s.access_exclusive.push(format!("{rel} (granted={granted})"));
        }
    }

    if let Ok(row) = c
        .query_one("SELECT pg_relation_size('queen.kv')::bigint", &[])
        .await
    {
        s.sizes.push(row.get(0));
    }
}

/// Steady state, which is the state that provokes the failure: a working set
/// that is written, expires, is pruned, and is written again. A table that only
/// grows never truncates, so a soak that only inserts proves nothing.
async fn churn_round(c: &tokio_postgres::Client, round: u32, rows: usize) -> Result<(), String> {
    // `QUEEN_KV_MAX_OPS_PER_CALL` is 256 and the stored procedure carries the
    // same ceiling as a constant (§9.2 — the shape rules live in SQL so all seven
    // clients inherit them), so the churn is issued in batches under it rather
    // than in one call the SP would rightly refuse.
    const PER_CALL: usize = 200;
    for chunk in 0..rows.div_ceil(PER_CALL) {
        let lo = chunk * PER_CALL;
        let hi = (lo + PER_CALL).min(rows);
        let ops: Vec<serde_json::Value> = (lo..hi)
            .map(|i| {
                serde_json::json!({
                    "op": "put",
                    "ns": "soak",
                    "key": format!("r{round}-k{i}"),
                    // A value big enough to matter to the heap and small enough to
                    // stay inline, so the churn is heap churn and not TOAST churn.
                    "value": {"n": i, "pad": "x".repeat(400)},
                    // Expires almost immediately: the prune below then makes every
                    // one of these rows dead, which is the shape §3.2 describes.
                    "ttlSeconds": 1
                })
            })
            .collect();
        let ops = serde_json::Value::Array(ops).to_string();
        c.execute(
            "SELECT queen.kv_apply_v1($1::text::jsonb, $2::text::uuid, now(), false)",
            &[&ops, &TENANT],
        )
        .await
        .map_err(|e| {
            format!(
                "kv_apply_v1 round {round}: {}",
                e.as_db_error()
                    .map(|d| format!("{}: {}", d.code().code(), d.message()))
                    .unwrap_or_else(|| e.to_string())
            )
        })?;
    }

    // Prune what has expired, exactly as the sweeper does — the DELETE half of
    // the churn, and the reason autovacuum has anything to do.
    let shards: Vec<i16> = (0..64).collect();
    let budget: i32 = 5000;
    c.execute(
        "SELECT queen.kv_expire_step_v1($1::int2[], now() + interval '5 seconds', $2::int)",
        &[&shards, &budget],
    )
    .await
    .map_err(|e| format!("kv_expire_step_v1 round {round}: {e}"))?;
    Ok(())
}

fn median(v: &mut [f64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    v[v.len() / 2]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn the_new_tables_never_take_an_access_exclusive_and_never_starve_autovacuum() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    let admin = connect(&host, port, "postgres").await;
    for row in admin
        .query(
            "SELECT datname FROM pg_database WHERE datname LIKE 'kvt\\_vac\\_%'",
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
        "kvt_vac_{}",
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
        panic!("\nPLAN_KV_TIMERS §15 — vacuum gate (F5)\n{report}\n");
    }
}

/// Cumulative autovacuum passes over the four new relations, from
/// `pg_stat_user_tables`. Unlike a sampled sighting this cannot be missed, so it
/// is what decides whether the soak exercised anything at all.
async fn autovacuum_count(c: &tokio_postgres::Client) -> i64 {
    c.query_one(
        "SELECT COALESCE(sum(autovacuum_count), 0)::bigint
           FROM pg_stat_user_tables
          WHERE schemaname = 'queen'
            AND relname IN ('kv','log_timers','kv_quota','kv_usage')",
        &[],
    )
    .await
    .map(|r| r.get(0))
    .unwrap_or(0)
}

async fn run(host: &str, port: u16, db: &str) -> Result<(), String> {
    let mut failures: Vec<String> = Vec::new();

    // The apply runs from the include_str!-embedded copy the binary carries, not
    // from server/sql on disk: reading the files would test the working tree and
    // pass happily against a stale `cargo build`.
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
    check_storage_params(&c, &mut failures).await;

    let max_workers: i32 = c
        .query_one("SELECT current_setting('autovacuum_max_workers')::int", &[])
        .await
        .map(|r| r.get(0))
        .unwrap_or(3);
    let naptime: String = c
        .query_one("SELECT current_setting('autovacuum_naptime')", &[])
        .await
        .map(|r| r.get(0))
        .unwrap_or_else(|_| "?".into());

    // ------------------------------------------------------------- the soak
    let observer = connect(host, port, db).await;
    let vacuums_before = autovacuum_count(&observer).await;
    let mut s = Soak::default();
    let deadline = Instant::now() + soak();
    let mut round: u32 = 0;
    while Instant::now() < deadline {
        round += 1;
        if let Err(e) = churn_round(&c, round, 400).await {
            failures.push(format!("  ✗ churn failed: {e}"));
            break;
        }
        sample(&observer, &mut s).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ------------------------------------------------------------ the verdict
    let truncating: Vec<&String> = s
        .phases
        .iter()
        .filter(|p| p.contains("truncating"))
        .collect();
    if !truncating.is_empty() {
        failures.push(format!(
            "  ✗ heap truncation observed: {truncating:?}\n      plan: §3.2 — this is the \
             2026-07-24 wobble, caught live in exactly this phase. Truncation takes ACCESS \
             EXCLUSIVE on a table the hot path reads, and `vacuum_truncate = off` is the fix. \
             Someone has re-enabled it, or the ALTER TABLE never applied."
        ));
    }
    if !s.access_exclusive.is_empty() {
        let mut seen: Vec<&String> = s.access_exclusive.iter().collect();
        seen.sort();
        seen.dedup();
        failures.push(format!(
            "  ✗ ACCESS EXCLUSIVE observed on a kv/timers relation: {seen:?}\n      Nothing in \
             the steady state of this feature may take that lock. Candidates: a re-enabled heap \
             truncation, or an ALTER TABLE running at boot (§3.2 — `SET STORAGE EXTENDED` on a \
             jsonb column is a no-op that still takes ACCESS EXCLUSIVE on EVERY boot, which is \
             why it is deliberately absent)."
        ));
    }
    if s.max_concurrent_workers >= max_workers as i64 {
        failures.push(format!(
            "  ✗ the four new relations occupied {} of {max_workers} autovacuum workers at \
             once\n      plan: §3.2 — autovacuum_max_workers is a GLOBAL budget. If this \
             feature can take all of them, log_partitions / log_consumers / log_segments — the \
             tables the engine runs on — can be left without one.",
            s.max_concurrent_workers
        ));
    }

    // Monotonic size: the runtime proof that truncation is off. A heap that
    // shrinks gave pages back, and the only thing that gives pages back is the
    // phase this gate forbids.
    if let Some(w) = s.sizes.windows(2).find(|w| w[1] < w[0]) {
        failures.push(format!(
            "  ✗ pg_relation_size(queen.kv) DECREASED, {} -> {}\n      With \
             `vacuum_truncate = off` the heap never gives pages back — a permanent peak is the \
             accepted cost of §3.2. A shrink means the truncation this gate exists to prevent \
             actually ran.",
            w[0], w[1]
        ));
    }

    // Wobble, measured. The stall an ACCESS EXCLUSIVE produces is not subtle:
    // it is the whole truncation, on a probe that otherwise costs microseconds.
    let mut probes = s.probe_ms.clone();
    let med = median(&mut probes);
    let worst = probes.last().copied().unwrap_or(0.0);
    // A generous multiple on purpose: this must catch a lock stall, not a
    // scheduling hiccup on a busy CI box. The failure it is aimed at was seconds
    // long against a sub-millisecond probe.
    if med > 0.0 && worst > med * 100.0 && worst > 250.0 {
        failures.push(format!(
            "  ✗ probe latency wobbled: median {med:.2} ms, worst {worst:.2} ms over {} \
             samples\n      A stall of that shape on an indexed single-row read is what an \
             ACCESS EXCLUSIVE looks like from the outside.",
            s.samples
        ));
    }

    // An honest report of what the run actually got to observe. A gate that
    // passes because autovacuum never ran has not proved anything, and saying so
    // is better than a green nobody earned.
    let vacuums = autovacuum_count(&observer).await - vacuums_before;
    println!(
        "vacuum gate: {} rounds, {} samples, {vacuums} autovacuum passes ({} caught in flight), \
         phases={:?}, max concurrent workers={} of {max_workers}, naptime={naptime}, \
         probe median={med:.2} ms worst={worst:.2} ms",
        round, s.samples, s.vacuum_sightings, s.phases, s.max_concurrent_workers
    );
    if vacuums == 0 {
        println!(
            "vacuum gate: WARNING — autovacuum never ran on the new tables during this soak. \
             The lock, phase and relation-size assertions above are vacuously true. Re-run \
             against a server started with `-c autovacuum_naptime=1s`, or raise \
             QUEEN_VACUUM_GATE_SECONDS, before reading this as a pass."
        );
    }

    broker.shutdown().await;

    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("\n"))
    }
}
