//! Boot-time schema applier.
//!
//! The broker is standalone and owns its schema: at startup it applies
//! `sql/schema.sql` + every `sql/procedures/*.sql` (the PROCEDURES list below),
//! in lexical order. The deployment model is always-virgin — a deployment
//! starts from an empty database and this list IS the schema. There is no
//! upgrade path and no redefinition ordering: each function and table is
//! defined exactly once, by exactly one file, for the one (log) engine.
//!
//! DDL is serialized cluster-wide with a **session advisory lock**, so
//! replicas booting concurrently never interleave statements. Every statement
//! is idempotent (`CREATE OR REPLACE`, `IF NOT EXISTS`), so re-apply on every
//! boot is safe.
//!
//! The SQL is embedded at compile time (`include_str!`) so the binary is
//! self-contained — no need to also ship `sql/` into the container image.

use deadpool_postgres::Pool;

/// Stable key for the schema-apply session advisory lock (arbitrary but fixed).
const SCHEMA_LOCK_KEY: i64 = 778_120_010;

const SCHEMA_SQL: &str = include_str!("../sql/schema.sql");

/// (filename, contents) in the exact order they must be applied. schema.sql runs
/// first (handled separately); these are the `procedures/*.sql` in lexical order.
const PROCEDURES: &[(&str, &str)] = &[
    // Renumbered 2026-07-31 (queue-identity merge + always-virgin deployment
    // model): the migration-only files (seg_* teardown, rows-engine drops, the
    // identity-merge data migration) are GONE — a deployment starts from an
    // empty database and this list IS the schema. Order is load-bearing only
    // where SQL-language function bodies resolve tables at creation time:
    // tables (001, 002) come first, engine SPs next, management plane after,
    // and the partition-counter trigger attachment (020) after both the table
    // it attaches to (001) and the functions it binds (019).
    //
    // 024/025 (kv, timers) come LAST, and that position is itself load-bearing
    // twice over (PLAN_KV_TIMERS §3.1, §1.2):
    //   * 005_log_ack.sql applies BEFORE them and calls queen.kv_apply_v1 and
    //     queen.log_timers_apply_v1. That works only because the body of
    //     log_transaction_wire_v1 is plpgsql, which resolves names at RUNTIME.
    //     Rewriting it as LANGUAGE sql would kill the boot at creation time.
    //   * conversely, nothing that applies before 024/025 can reference their
    //     tables from a LANGUAGE sql body without failing the boot — which is
    //     the free mechanical guard that keeps log_partition_dead_v1 (006,
    //     LANGUAGE sql) from ever growing a veto leg on queen.kv or
    //     queen.log_timers, and with it the O(partitions) scan it fronts.
    ("001_log_schema.sql", include_str!("../sql/procedures/001_log_schema.sql")),
    ("002_streams_schema.sql", include_str!("../sql/procedures/002_streams_schema.sql")),
    ("003_log_push.sql", include_str!("../sql/procedures/003_log_push.sql")),
    ("004_log_pop.sql", include_str!("../sql/procedures/004_log_pop.sql")),
    ("005_log_ack.sql", include_str!("../sql/procedures/005_log_ack.sql")),
    ("006_log_maintenance.sql", include_str!("../sql/procedures/006_log_maintenance.sql")),
    ("007_log_streams.sql", include_str!("../sql/procedures/007_log_streams.sql")),
    ("008_streams_register_query_v1.sql", include_str!("../sql/procedures/008_streams_register_query_v1.sql")),
    ("009_streams_state_get_v1.sql", include_str!("../sql/procedures/009_streams_state_get_v1.sql")),
    ("010_log_admin.sql", include_str!("../sql/procedures/010_log_admin.sql")),
    ("011_log_stats.sql", include_str!("../sql/procedures/011_log_stats.sql")),
    ("012_configure.sql", include_str!("../sql/procedures/012_configure.sql")),
    ("013_analytics.sql", include_str!("../sql/procedures/013_analytics.sql")),
    ("014_consumer_groups.sql", include_str!("../sql/procedures/014_consumer_groups.sql")),
    ("015_status.sql", include_str!("../sql/procedures/015_status.sql")),
    ("016_messages.sql", include_str!("../sql/procedures/016_messages.sql")),
    ("017_traces.sql", include_str!("../sql/procedures/017_traces.sql")),
    ("018_stats.sql", include_str!("../sql/procedures/018_stats.sql")),
    ("019_worker_metrics.sql", include_str!("../sql/procedures/019_worker_metrics.sql")),
    ("020_log_partition_counters.sql", include_str!("../sql/procedures/020_log_partition_counters.sql")),
    ("021_postgres_stats.sql", include_str!("../sql/procedures/021_postgres_stats.sql")),
    ("022_retention_analytics.sql", include_str!("../sql/procedures/022_retention_analytics.sql")),
    ("023_prometheus.sql", include_str!("../sql/procedures/023_prometheus.sql")),
    ("024_kv.sql", include_str!("../sql/procedures/024_kv.sql")),
    ("025_log_timers.sql", include_str!("../sql/procedures/025_log_timers.sql")),
];

/// Minimum PostgreSQL this schema can be applied to, as `server_version_num`
/// (14.0 = 140000).
///
/// PLAN_KV_TIMERS §0 pins the floor at 14 and asks for it to be checked here.
/// `queen.kv` is the first thing in this schema to use `GENERATED ALWAYS AS ...
/// STORED` (needs 12) and `starts_with()` (needs 11); neither has a precedent in
/// the pre-024 files, so nothing before them would have noticed an older server.
/// Without this check the symptom is not a clear error but an apply that fails
/// mid-file, five times, through the deadlock retry below, and then exits with a
/// message about a syntax error that names no version at all.
const MIN_SERVER_VERSION_NUM: i32 = 140_000;

/// Apply the schema at boot. Set `QUEEN_APPLY_SCHEMA=0` to skip (e.g. when the DB
/// is managed externally). Fails the process on any DDL error (fail-fast boot),
/// and — before any of it — on a PostgreSQL older than [`MIN_SERVER_VERSION_NUM`].
/// The version check rides with the apply on purpose: it guards the statements
/// this function is about to run, so `QUEEN_APPLY_SCHEMA=0` skips both together
/// and a boot that writes no DDL is not killed by a check for DDL it never runs.
pub async fn apply(pool: &Pool) -> Result<(), Box<dyn std::error::Error>> {
    if !crate::config::env_bool("QUEEN_APPLY_SCHEMA", true) {
        tracing::info!(target: "schema", "apply skipped (QUEEN_APPLY_SCHEMA=0)");
        return Ok(());
    }

    let client = pool.get().await?;

    // Refuse an unsupported server BEFORE taking the advisory lock and before
    // the first DDL statement: a version too old is a permanent, operator-level
    // fault, and the retry loop below exists for a transient one (deadlock
    // against a live peer). Retrying this five times would only delay the exit
    // and bury the cause.
    let server_version = check_server_version(&client).await?;

    // Serialize DDL across replicas. Session-level lock held on THIS connection;
    // released below before the client returns to the pool.
    client
        .execute("SELECT pg_advisory_lock($1)", &[&SCHEMA_LOCK_KEY])
        .await?;

    // Bounded deadlock retry. The advisory lock serialises appliers against each
    // other, but NOT against a replica that already booted and is running its
    // background cycles: batch_execute runs each file as ONE implicit
    // transaction, so a re-apply holds its early locks (the queen.queues ALTERs)
    // while later statements (the queen.stats CREATE INDEXes) wait on tables the
    // stats refresh is writing — which is itself waiting to read queen.queues.
    // PostgreSQL resolves that by killing one side (SQLSTATE 40P01); observed
    // live on the HA test topology, where a rolling-restarted replica died
    // FATAL at boot because the peer's stats cycle crossed its re-apply. The
    // apply is idempotent by design, so the correct response is to try again,
    // not to die: the peer's transaction is gone by the next attempt.
    let mut result = Ok(());
    for attempt in 1..=5u32 {
        result = apply_all(&client).await;
        match &result {
            Ok(()) => break,
            Err(e) if e.to_string().contains("deadlock detected") && attempt < 5 => {
                tracing::warn!(
                    target: "schema",
                    attempt,
                    "apply hit a deadlock against live traffic; retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(250 * attempt as u64)).await;
            }
            Err(_) => break,
        }
    }

    // Always release the lock, even if apply failed.
    let _ = client
        .execute("SELECT pg_advisory_unlock($1)", &[&SCHEMA_LOCK_KEY])
        .await;

    result?;
    tracing::info!(
        target: "schema",
        procedures = PROCEDURES.len(),
        server_version = %server_version,
        "applied schema.sql + procedures"
    );
    Ok(())
}

/// Fail the boot on a PostgreSQL older than [`MIN_SERVER_VERSION_NUM`], with a
/// message that names the version found and what to do about it. Returns the
/// human-readable version string on success, for the applied-schema log line.
///
/// Takes no lock of any kind, so it is safe anywhere in the boot sequence.
async fn check_server_version(
    client: &deadpool_postgres::Client,
) -> Result<String, Box<dyn std::error::Error>> {
    let row = client
        .query_one(
            "SELECT current_setting('server_version_num')::int, current_setting('server_version')",
            &[],
        )
        .await
        .map_err(|e| format!("could not read the PostgreSQL server version: {e}"))?;
    let version_num: i32 = row.get(0);
    let version: String = row.get(1);

    if version_num < MIN_SERVER_VERSION_NUM {
        // Major derived from the constant, never spelled twice: a message that
        // says "requires 14" next to a floor someone raised to 15 is worse than
        // no message, because it sends the operator to check the wrong thing.
        let min_major = MIN_SERVER_VERSION_NUM / 10_000;
        return Err(format!(
            "PostgreSQL {version} (server_version_num={version_num}) is too old: this schema \
             requires PostgreSQL {min_major} or newer (server_version_num >= \
             {MIN_SERVER_VERSION_NUM}). queen.kv uses GENERATED ALWAYS AS ... STORED and \
             starts_with(), so the apply would die part-way through procedures/024_kv.sql and \
             leave the database half-built. Upgrade this server, or point \
             PG_HOST/PG_PORT/PG_DATABASE at a PostgreSQL {min_major}+ instance. \
             QUEEN_APPLY_SCHEMA=0 skips the apply but does not make the broker work on this \
             version."
        )
        .into());
    }

    Ok(version)
}

async fn apply_all(
    client: &deadpool_postgres::Client,
) -> Result<(), Box<dyn std::error::Error>> {
    client
        .batch_execute(SCHEMA_SQL)
        .await
        .map_err(|e| format!("schema.sql: {e}"))?;
    for (name, sql) in PROCEDURES {
        client
            .batch_execute(sql)
            .await
            .map_err(|e| format!("procedures/{name}: {e}"))?;
    }
    Ok(())
}
