//! BOOT IDEMPOTENCE for `024_kv.sql` / `025_log_timers.sql` (PLAN_KV_TIMERS.md
//! §15 row 2, phase F1): apply the schema **twice** on a virgin database and
//! **once more** on a populated one, and require the later boots to pass.
//!
//! WHY THIS IS A TEST AND NOT A REVIEW. `schema.rs` re-applies `schema.sql` plus
//! every `procedures/*.sql` on EVERY boot, and a failed apply kills the process
//! (fail-fast boot). So the second boot is not a corner case, it is what every
//! pod does every restart, and the two ways to break it are both invisible on a
//! virgin database:
//!
//!   * a `DROP FUNCTION` written without `IF EXISTS`, or one whose argument list
//!     no longer matches, leaves the old function in place and the `CREATE` then
//!     fails `42723 duplicate_function` — on the SECOND boot only, i.e. on the
//!     first rolling restart after the release, on every pod at once;
//!   * `CREATE TABLE IF NOT EXISTS` is a **silent no-op on the SHAPE** (§3.2), so
//!     a config table that already exists keeps its old columns and the first
//!     read of a new one is a `42703` in production, on the schedule path,
//!     classified as configuration and therefore never retried. The plan's answer
//!     is `CREATE TABLE IF NOT EXISTS` followed by one `ALTER TABLE ... ADD COLUMN
//!     IF NOT EXISTS` per column, the shape `019_worker_metrics.sql:95-119` already
//!     uses. A forgotten ALTER is exactly what the populated leg below catches.
//!
//! HOW IT IS CHECKED. A catalogue SNAPSHOT — relations, reloptions, every column
//! with its type/notnull/generation/storage/collation/default, every index
//! definition, every `kv_*` and `log_timers_*` function with its signature and
//! volatility, and the ACLs — is taken after each boot and compared byte for
//! byte. Snapshot equality alone would be satisfied by "no objects at all", so
//! the first boot also asserts the schema contract of §3 explicitly.
//!
//! ISOLATION. Each run creates its own database, so "virgin" means virgin rather
//! than "whatever the previous test left". That also keeps this file from
//! colliding with the other `#[ignore]` suites sharing the container.
//!
//! THREE `Broker::start`s IN ONE BINARY, deliberately, against the house note
//! that says one: the global here is the admission arbiter (`admission.rs:942-945`,
//! a `OnceLock` whose `set` result is discarded, so a later boot is a no-op and
//! never a panic), and this file pushes nothing, pops nothing and asks the
//! arbiter for nothing. What it needs is the apply to run three times, and
//! `Broker::start` is the only way to run the `include_str!`-embedded copy — the
//! one the binary actually carries. Reading `server/sql/**` from disk would test
//! the file and pass happily against a stale `cargo build`.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`:
//!
//! ```bash
//! docker run --rm -d --name queen-kvt-pg -e POSTGRES_PASSWORD=postgres -p 5471:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5471 cargo test --test kv_timers_boot_idempotence -- --ignored --nocapture
//! ```

use queen::{Broker, BrokerConfig};

const TENANT: &str = "00000000-0000-0000-0000-000000000001";

/// Everything 024/025 create. Named here so "the file was never added to the
/// PROCEDURES list in schema.rs" reads as four missing relations rather than as
/// an empty snapshot that compares equal to itself (GOTCHA: a new `.sql` file
/// that nobody lists is never applied, and nothing complains).
const RELATIONS: &[(&str, &str)] = &[
    ("kv", "r"),
    ("kv_quota", "r"),
    ("kv_usage", "r"),
    ("log_timers", "r"),
    ("kv_version_seq", "S"),
];

fn snapshot_sql() -> &'static str {
    // ORDER BY 1 over the rendered lines: deterministic without depending on any
    // catalogue oid, which changes between boots by design.
    r#"
WITH rel AS (
    SELECT c.oid, c.relname, c.relkind, c.reloptions, c.relacl
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'queen'
       AND c.relname IN ('kv','kv_quota','kv_usage','log_timers','kv_version_seq')
)
SELECT format('rel  %s kind=%s opts=[%s] acl=[%s]', r.relname, r.relkind,
              coalesce(array_to_string(ARRAY(SELECT unnest(r.reloptions) ORDER BY 1), ','), ''),
              coalesce(array_to_string(r.relacl::text[], ','), ''))
  FROM rel r
UNION ALL
SELECT format('col  %s.%s type=%s notnull=%s gen=%s storage=%s coll=%s default=%s',
              r.relname, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,
              coalesce(nullif(a.attgenerated::text, ''), '-'), a.attstorage,
              coalesce(co.collname, '-'),
              coalesce(pg_get_expr(d.adbin, d.adrelid), '-'))
  FROM rel r
  JOIN pg_attribute a ON a.attrelid = r.oid AND a.attnum > 0 AND NOT a.attisdropped
  LEFT JOIN pg_collation co ON co.oid = a.attcollation
  LEFT JOIN pg_attrdef  d  ON d.adrelid = a.attrelid AND d.adnum = a.attnum
UNION ALL
SELECT format('idx  %s', pg_get_indexdef(i.oid))
  FROM rel r JOIN pg_index x ON x.indrelid = r.oid JOIN pg_class i ON i.oid = x.indexrelid
UNION ALL
SELECT format('fn   %s(%s) -> %s vol=%s par=%s strict=%s', p.proname,
              pg_get_function_identity_arguments(p.oid), pg_get_function_result(p.oid),
              p.provolatile, p.proparallel, p.proisstrict)
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE n.nspname = 'queen'
   AND (p.proname LIKE 'kv\_%' OR p.proname LIKE 'log\_timers\_%')
ORDER BY 1
"#
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

async fn snapshot(c: &tokio_postgres::Client) -> Vec<String> {
    c.query(snapshot_sql(), &[])
        .await
        .expect("catalogue snapshot")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect()
}

/// Boot a broker against `db` purely to run the apply, then shut it down.
async fn boot(host: &str, port: u16, db: &str, leg: &str) {
    let broker = Broker::start(
        BrokerConfig::new()
            .pg(host.to_string(), port, "postgres", "postgres", db.to_string())
            .pool_size(4)
            // Off so the apply is the only thing touching this database. The
            // deadlock-retry loop in schema.rs:88-103 exists because a re-apply
            // can cross a LIVE replica's stats cycle; that is a real hazard and
            // it has its own test, but here it would only turn a schema failure
            // into a flake.
            .retention(false)
            .stats_refresh(false)
            .system_metrics(false)
            .log_reports(false),
    )
    .await
    .unwrap_or_else(|e| {
        panic!(
            "{leg}: schema apply failed — {e:?}\n\n\
             PLAN_KV_TIMERS §15 row 2: every boot re-applies schema.sql + procedures/*.sql, and \
             a failed apply kills the process. 42723 here means a DROP FUNCTION that does not \
             match what the CREATE then defines; 42P07/42701 means a CREATE without IF NOT \
             EXISTS; 42501 means a GRANT EXECUTE whose type list is wrong (§6 — a wrong list \
             fails the apply, and a failed apply takes the process down)."
        )
    });
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn schema_applies_twice_on_virgin_and_again_on_populated() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    let admin = connect(&host, port, "postgres").await;

    // A panicking run leaves its database behind; sweep the previous ones so a
    // long-lived container does not accumulate them.
    for row in admin
        .query(
            "SELECT datname FROM pg_database WHERE datname LIKE 'kvt\\_boot\\_%'",
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
        "kvt_boot_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    admin
        .execute(&format!("CREATE DATABASE \"{db}\""), &[])
        .await
        .expect("create virgin database");

    let result = run_legs(&host, port, &db).await;

    let _ = admin
        .execute(&format!("DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)"), &[])
        .await;

    if let Err(report) = result {
        panic!("\nPLAN_KV_TIMERS §15 row 2 — boot idempotence (F1)\n{report}\n");
    }
}

async fn run_legs(host: &str, port: u16, db: &str) -> Result<(), String> {
    let mut failures: Vec<String> = Vec::new();

    // ------------------------------------------------ leg 1: virgin, first boot
    boot(host, port, db, "boot 1 (virgin)").await;
    let c = connect(host, port, db).await;
    let snap1 = snapshot(&c).await;

    schema_contract(&c, &snap1, &mut failures).await;

    // The apply must be verifiable even under the default-OFF flags of §20.4:
    // §0 says the tables are created at every boot whatever the flags say,
    // because "un modello always-virgin non tollera due schemi possibili".
    if snap1.is_empty() {
        failures.push(
            "  ✗ the catalogue snapshot is EMPTY after the first boot\n      plan: §3.1 — \
             024_kv.sql and 025_log_timers.sql must be appended to the PROCEDURES list in \
             server/src/schema.rs, right after 023_prometheus.sql. A new .sql file that nobody \
             lists is never applied and nothing complains."
                .to_string(),
        );
    }

    // ---------------------------------------------- leg 2: virgin, second boot
    boot(host, port, db, "boot 2 (virgin, re-apply)").await;
    let snap2 = snapshot(&c).await;
    diff(&snap1, &snap2, "boot 1 → boot 2 (virgin re-apply)", &mut failures);

    // ------------------------------------------------------ populate, then boot
    // Rows an operator wrote (quota) and rows the feature wrote (kv, timers).
    // §3.2: always-virgin covers the SCHEMA and the FUNCTIONS, never the DATA of
    // a configuration table, which cannot be dropped.
    let seeded = seed(&c).await;
    if let Err(e) = &seeded {
        // `Display` for a tokio_postgres error is the useless string "db error";
        // the server's own message lives on the DbError.
        let detail = match e.as_db_error() {
            Some(d) => format!(
                "{}: {}",
                e.code().map(|c| c.code()).unwrap_or("<no sqlstate>"),
                d.message()
            ),
            None => format!("{e}"),
        };
        failures.push(format!(
            "  ✗ could not seed the new tables\n      plan: §3.2/§3.3 define them\n      got:  {detail}"
        ));
    }

    boot(host, port, db, "boot 3 (populated)").await;
    let snap3 = snapshot(&c).await;
    diff(&snap1, &snap3, "boot 1 → boot 3 (populated re-apply)", &mut failures);

    if seeded.is_ok() {
        rows_survived(&c, &mut failures).await;
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{}\n\n{} failing check(s). Before F1 they are all \"relation does not exist\" — \
             that is the expected state.",
            failures.join("\n"),
            failures.len()
        ))
    }
}

fn diff(a: &[String], b: &[String], leg: &str, failures: &mut Vec<String>) {
    if a == b {
        return;
    }
    let only_a: Vec<&String> = a.iter().filter(|x| !b.contains(x)).collect();
    let only_b: Vec<&String> = b.iter().filter(|x| !a.contains(x)).collect();
    let render = |v: &Vec<&String>| {
        if v.is_empty() {
            "      (none)".to_string()
        } else {
            v.iter().map(|l| format!("      {l}")).collect::<Vec<_>>().join("\n")
        }
    };
    failures.push(format!(
        "  ✗ the catalogue changed across {leg}\n      plan: §15 row 2 — a re-apply must be a \
         no-op on the shape\n    lost:\n{}\n    gained:\n{}",
        render(&only_a),
        render(&only_b)
    ));
}

// --------------------------------------------------------- the §3 contract

/// The facts of §3 that a snapshot comparison cannot state on its own, because
/// two identical wrong schemas compare equal.
async fn schema_contract(c: &tokio_postgres::Client, snap: &[String], failures: &mut Vec<String>) {
    // ------------------------------------------------------------- relations
    for (name, kind) in RELATIONS {
        let found: Option<String> = c
            .query_opt(
                "SELECT c.relkind::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE n.nspname = 'queen' AND c.relname = $1",
                &[name],
            )
            .await
            .expect("relkind")
            .map(|r| r.get(0));
        match found {
            None => failures.push(format!(
                "  ✗ queen.{name}: does not exist\n      plan: §3.2/§3.3 create it"
            )),
            Some(k) if &k != kind => failures.push(format!(
                "  ✗ queen.{name}: relkind {k:?}, want {kind:?}\n      plan: §3.2/§3.3"
            )),
            Some(_) => {}
        }
    }

    // ------------------------------------------------------------ reloptions
    // GOTCHA 5 / §3.2: `vacuum_truncate = off` is NOT a tuning preference on
    // these tables. Heap truncation takes an ACCESS EXCLUSIVE, these two tables
    // empty and refill in steady state (that is the whole point of expires_at),
    // and that lock was the root cause of the entire "wobble" class, caught live
    // with pg_stat_progress_vacuum in the "truncating heap" phase on 2026-07-24.
    // §2.4 C4 goes further: an MVCC read still takes ACCESS SHARE, which
    // conflicts with ACCESS EXCLUSIVE, so on queen.kv this setting is part of
    // the LOCK ORDER and not only a defence against wobble.
    let hot = ["kv", "log_timers"];
    let cold = ["kv_quota", "kv_usage"];
    for t in hot.iter().chain(cold.iter()) {
        require_reloption(c, t, "vacuum_truncate=off", failures,
            "GOTCHA/§3.2 + §2.4 C4: heap truncation takes ACCESS EXCLUSIVE on a table that \
             empties and refills in steady state — the root cause of the wobble class, and on \
             queen.kv part of the lock order itself").await;
        require_reloption(c, t, "autovacuum_vacuum_scale_factor=0", failures,
            "§3.2, copied verbatim from queen.log_partitions (001_log_schema.sql:47-68): between \
             passes the dead tuples outnumber the live ones, so scale_factor 0 keeps autovacuum \
             re-firing every naptime").await;
        require_reloption(c, t, "autovacuum_vacuum_threshold=500", failures, "§3.2").await;
    }
    for t in hot {
        require_reloption(c, t, "autovacuum_vacuum_cost_delay=0", failures,
            "§3.2: unthrottled, but only on the two hot tables").await;
        require_reloption(c, t, "fillfactor=70", failures,
            "§3.2: by ANALOGY with 001_log_schema.sql:39-46, explicitly NOT measured — to be \
             replaced with a measured number after the first soak").await;
    }
    for t in cold {
        // §3.2 says this in as many words: autovacuum_max_workers is a GLOBAL
        // budget (default 3), and seven unthrottled tables would contend for it
        // with log_partitions, log_consumers and log_segments — the tables the
        // engine depends on. These two are tiny and have no need to be aggressive.
        forbid_reloption(c, t, "autovacuum_vacuum_cost_delay", failures,
            "§3.2: cost_delay 0 ONLY on queen.kv and queen.log_timers, never on the config and \
             measurement tables — autovacuum_max_workers is a GLOBAL budget").await;
    }

    // ------------------------------------------------------ generated columns
    // GENERATED STORED so no UPDATE can ever move a row's shard (§3.2), and so
    // the modulus is frozen at write time — always-virgin covers the SCHEMA, not
    // the DATA, and changing 64 would silently re-shard rows already written.
    for (t, col) in [("kv", "shard"), ("log_timers", "shard"), ("log_timers", "visible_at")] {
        let g: Option<String> = c
            .query_opt(
                "SELECT a.attgenerated::text FROM pg_attribute a
                   JOIN pg_class t ON t.oid = a.attrelid
                   JOIN pg_namespace n ON n.oid = t.relnamespace
                  WHERE n.nspname='queen' AND t.relname=$1 AND a.attname=$2
                    AND a.attnum > 0 AND NOT a.attisdropped",
                &[&t, &col],
            )
            .await
            .expect("attgenerated")
            .map(|r| r.get(0));
        if g.as_deref() != Some("s") {
            failures.push(format!(
                "  ✗ queen.{t}.{col}: not GENERATED ALWAYS ... STORED\n      plan: §3.2/§3.3 — \
                 STORED is what freezes the value at write time, so no UPDATE can move a row's \
                 shard and no major-version change to hashtextextended can re-shard rows that \
                 already exist\n      got:  attgenerated = {g:?} (want Some(\"s\"))"
            ));
        }
    }

    // --------------------------------------------------------- index budget
    // §3.4: exactly ONE secondary index per new table, each with exactly ONE
    // reader (the sweeper). The named non-index is the point: an index on
    // updated_at would make EVERY counter update non-HOT.
    for (t, want_idx) in [("kv", "idx_kv_shard_expires"), ("log_timers", "idx_log_timers_visible")] {
        let extra: Vec<String> = c
            .query(
                "SELECT i.relname FROM pg_index x
                   JOIN pg_class i ON i.oid = x.indexrelid
                   JOIN pg_class t ON t.oid = x.indrelid
                   JOIN pg_namespace n ON n.oid = t.relnamespace
                  WHERE n.nspname='queen' AND t.relname=$1 AND NOT x.indisprimary
                  ORDER BY 1",
                &[&t],
            )
            .await
            .expect("secondary indexes")
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect();
        if extra != vec![want_idx.to_string()] {
            failures.push(format!(
                "  ✗ queen.{t}: secondary indexes are {extra:?}, want [\"{want_idx}\"]\n      \
                 plan: §3.4 — exactly ONE secondary index per new table, with exactly ONE reader \
                 (the sweeper). Deliberately absent: updated_at (would make every counter update \
                 non-HOT), value, deliver_at, (tenant_id, queue), claim_token"
            ));
        }
    }
    // The kv index is PARTIAL, or every "forever" key pays for a scan path it
    // will never be found by (§3.2).
    if !snap.iter().any(|l| {
        l.starts_with("idx  ")
            && l.contains("idx_kv_shard_expires")
            && l.contains("WHERE (expires_at IS NOT NULL)")
    }) {
        failures.push(
            "  ✗ idx_kv_shard_expires is not PARTIAL on expires_at IS NOT NULL\n      plan: \
             §3.2 — the only reader is the sweeper, so a key written with \"forever\" must cost \
             nothing here"
                .to_string(),
        );
    }

    // ------------------------------------------------------------- storage
    // §2.4 D8: `ALTER COLUMN ... SET STORAGE` is cancelled from BOTH DDLs. It is
    // a no-op (EXTENDED is already the default for jsonb and bytea) that takes an
    // ACCESS EXCLUSIVE on every boot, and on queen.log_timers that stops every
    // in-flight wire bundle carrying a `timers` array at step 0b — BEFORE the
    // partition pre-lock, dragging its pushes and acks with it. This asserts the
    // value the plan says is correct; a run that changed it announced a statement
    // that must not be there.
    for (t, col) in [("kv", "value"), ("log_timers", "payload")] {
        let s: Option<String> = c
            .query_opt(
                "SELECT a.attstorage::text FROM pg_attribute a
                   JOIN pg_class t ON t.oid = a.attrelid
                   JOIN pg_namespace n ON n.oid = t.relnamespace
                  WHERE n.nspname='queen' AND t.relname=$1 AND a.attname=$2
                    AND a.attnum > 0 AND NOT a.attisdropped",
                &[&t, &col],
            )
            .await
            .expect("attstorage")
            .map(|r| r.get(0));
        if s.as_deref() != Some("x") {
            failures.push(format!(
                "  ✗ queen.{t}.{col}: attstorage = {s:?}, want Some(\"x\") (EXTENDED)\n      \
                 plan: §2.4 D8 — EXTENDED is already the default and both DDLs must NOT contain \
                 an ALTER COLUMN ... SET STORAGE. A blob is EXTERNAL because zstd already ran; \
                 this is raw JSON/bytea that TOAST compression genuinely shortens"
            ));
        }
    }

    // ---------------------------------------- the ADD COLUMN lists of §3.2
    for (t, col) in [
        ("kv_quota", "enabled"),
        ("kv_quota", "max_rows"),
        ("kv_quota", "max_bytes"),
        ("kv_quota", "max_timers"),
        ("kv_quota", "max_timer_horizon_s"),
        ("kv_quota", "max_reads_per_sec"),
        ("kv_quota", "max_writes_per_sec"),
        ("kv_usage", "kv_rows"),
        ("kv_usage", "kv_bytes"),
        ("kv_usage", "timer_rows"),
        ("kv_usage", "timer_bytes"),
        ("kv_usage", "timer_oldest"),
    ] {
        let exists: bool = c
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_attribute a
                                  JOIN pg_class t ON t.oid = a.attrelid
                                  JOIN pg_namespace n ON n.oid = t.relnamespace
                                 WHERE n.nspname='queen' AND t.relname=$1 AND a.attname=$2
                                   AND a.attnum > 0 AND NOT a.attisdropped)",
                &[&t, &col],
            )
            .await
            .expect("column probe")
            .get(0);
        if !exists {
            failures.push(format!(
                "  ✗ queen.{t}.{col}: missing\n      plan: §3.2 — CREATE TABLE IF NOT EXISTS is a \
                 SILENT no-op on the shape, so every column of a config/measurement table needs \
                 its own ALTER TABLE ... ADD COLUMN IF NOT EXISTS (precedent: \
                 019_worker_metrics.sql:95-119). Without it a cell that already booted an older \
                 version discovers the missing column as a 42703 in production, on the schedule \
                 path, classified as configuration and therefore never retried"
            ));
        }
    }

    // ------------------------------------------------------------ minimum PG
    // §0: GENERATED ALWAYS ... STORED needs 12 and starts_with() needs 11, and
    // neither has a precedent in this schema, so the plan floors the product at
    // 14 and asks schema.rs to say so at boot rather than let the apply spin five
    // times through the deadlock retry and exit with an error that says nothing.
    let vernum: i32 = c
        .query_one("SELECT current_setting('server_version_num')::int", &[])
        .await
        .expect("server_version_num")
        .get(0);
    if vernum < 140000 {
        failures.push(format!(
            "  ✗ this Postgres is {vernum}, below the 140000 floor\n      plan: §0 — the test rig \
             itself is below the minimum this feature declares"
        ));
    }
}

async fn require_reloption(
    c: &tokio_postgres::Client,
    table: &str,
    want: &str,
    failures: &mut Vec<String>,
    why: &str,
) {
    let opts: Option<Vec<String>> = c
        .query_opt(
            "SELECT c.reloptions FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname='queen' AND c.relname=$1",
            &[&table],
        )
        .await
        .expect("reloptions")
        .and_then(|r| r.get(0));
    let opts = opts.unwrap_or_default();
    if !opts.iter().any(|o| o == want) {
        failures.push(format!(
            "  ✗ queen.{table}: reloption {want} not set\n      plan: {why}\n      got:  {opts:?}"
        ));
    }
}

async fn forbid_reloption(
    c: &tokio_postgres::Client,
    table: &str,
    forbidden_key: &str,
    failures: &mut Vec<String>,
    why: &str,
) {
    let opts: Option<Vec<String>> = c
        .query_opt(
            "SELECT c.reloptions FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname='queen' AND c.relname=$1",
            &[&table],
        )
        .await
        .expect("reloptions")
        .and_then(|r| r.get(0));
    let opts = opts.unwrap_or_default();
    if opts.iter().any(|o| o.starts_with(&format!("{forbidden_key}="))) {
        failures.push(format!(
            "  ✗ queen.{table}: reloption {forbidden_key} is set and must not be\n      plan: \
             {why}\n      got:  {opts:?}"
        ));
    }
}

// ------------------------------------------------------------------ data leg

/// Rows of both kinds §3.2 distinguishes: operator-written configuration, which
/// cannot be dropped, and feature-written state.
async fn seed(c: &tokio_postgres::Client) -> Result<(), tokio_postgres::Error> {
    c.execute(
        "INSERT INTO queen.kv (tenant_id, namespace, key, value, version, expires_at)
         VALUES ($1::uuid, 'saga', 'done:tx-1', '{\"ok\": true}'::jsonb,
                 nextval('queen.kv_version_seq'), now() + interval '1 day'),
                ($1::uuid, 'saga', 'forever-marker', 'null'::jsonb,
                 nextval('queen.kv_version_seq'), NULL)",
        &[&TENANT],
    )
    .await?;
    c.execute(
        "INSERT INTO queen.log_timers
             (tenant_id, queue, timer_key, \"partition\", deliver_at, txn, message_id, payload)
         VALUES ($1::uuid, 'reminders', 'trial-end:acme', 'Default',
                 now() + interval '30 days', 'txn-boot-1',
                 '11111111-1111-7111-8111-111111111111'::uuid, '\\x7b7d'::bytea)",
        &[&TENANT],
    )
    .await?;
    c.execute(
        "INSERT INTO queen.kv_quota (tenant_id, max_rows, max_timers) VALUES ($1::uuid, 10000, 1000)",
        &[&TENANT],
    )
    .await?;
    c.execute(
        "INSERT INTO queen.kv_usage (tenant_id, kv_rows, timer_rows) VALUES ($1::uuid, 2, 1)",
        &[&TENANT],
    )
    .await?;
    Ok(())
}

/// The populated leg's real assertion: the re-apply changed no DATA either.
///
/// The version column is checked hardest. §3.2: the token comes from a sequence
/// and never from `version + 1`, so that a key which expired, was pruned and was
/// recreated cannot re-issue a version an old holder still carries — the ABA of a
/// per-lineage counter. A boot that reset `queen.kv_version_seq` would reopen
/// exactly that, and it would be invisible until the day a stale `expect` won.
async fn rows_survived(c: &tokio_postgres::Client, failures: &mut Vec<String>) {
    let rows = c
        .query(
            "SELECT key, value::text, version, (expires_at IS NULL), shard
               FROM queen.kv WHERE tenant_id = $1::uuid AND namespace = 'saga'
              ORDER BY key COLLATE \"C\"",
            &[&TENANT],
        )
        .await
        .expect("kv rows after re-apply");
    if rows.len() != 2 {
        failures.push(format!(
            "  ✗ queen.kv: {} seeded rows survived the re-apply, want 2\n      plan: §3.2 — \
             always-virgin covers the SCHEMA and the FUNCTIONS, never the DATA",
            rows.len()
        ));
    }

    let versions: Vec<i64> = rows.iter().map(|r| r.get::<_, i64>(2)).collect();
    let next: i64 = c
        .query_one("SELECT nextval('queen.kv_version_seq')", &[])
        .await
        .expect("nextval")
        .get(0);
    if let Some(max) = versions.iter().max() {
        if next <= *max {
            failures.push(format!(
                "  ✗ queen.kv_version_seq went BACKWARDS across the re-apply: nextval = {next}, \
                 but a live row already holds {max}\n      plan: §3.2 — the version comes from a \
                 sequence and never from version+1, precisely so a pruned-and-recreated key \
                 cannot re-issue a version an old holder still carries. A boot that resets the \
                 sequence reopens that ABA, and it stays invisible until a stale expect wins"
            ));
        }
    }

    for (t, n) in [("log_timers", 1i64), ("kv_quota", 1), ("kv_usage", 1)] {
        let got: i64 = c
            .query_one(
                &format!("SELECT count(*) FROM queen.{t} WHERE tenant_id = $1::uuid"),
                &[&TENANT],
            )
            .await
            .expect("count after re-apply")
            .get(0);
        if got != n {
            failures.push(format!(
                "  ✗ queen.{t}: {got} seeded rows survived the re-apply, want {n}\n      plan: \
                 §3.2 — a configuration table is written by an operator and cannot be dropped"
            ));
        }
    }
}
