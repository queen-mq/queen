//! Boot-time schema applier.
//!
//! Neither Rust experiment applied schema before — they relied on the C++ broker
//! to create `queen.*`. This broker is standalone, so it applies
//! `lib/schema/schema.sql` + every `procedures/*.sql` at startup, in lexical order
//! (the same order `AsyncQueueManager::initialize_schema()` uses in the C++ broker,
//! so the 04x log-engine redefinitions land after the originals they supersede).
//!
//! Unlike the C++ applier (which only gates on `worker_id == 0` and races across
//! replicas relying on idempotency), we take a **session advisory lock** so DDL is
//! serialized cluster-wide. Every statement is idempotent (`CREATE OR REPLACE`,
//! `IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`), so re-apply on every boot is safe.
//!
//! The SQL is embedded at compile time (`include_str!`) so the binary is
//! self-contained — no need to also ship `lib/schema/` into the container image.

use deadpool_postgres::Pool;

/// Stable key for the schema-apply session advisory lock (arbitrary but fixed).
const SCHEMA_LOCK_KEY: i64 = 778_120_010;

const SCHEMA_SQL: &str = include_str!("../sql/schema.sql");

/// (filename, contents) in the exact order they must be applied. schema.sql runs
/// first (handled separately); these are the `procedures/*.sql` in lexical order.
const PROCEDURES: &[(&str, &str)] = &[
    // 001-006 (rows push/pop/ack/transaction/renew_lease/has_pending) and
    // 016_partition_lookup / 021_streams_cycle_v1 are GONE (2026-07-30): the log
    // engine replaced that message plane wholesale, and a call-graph closure over
    // src/ + the remaining SQL found zero callers for every function they defined.
    // 049 drops them from databases that already applied them. What survives from
    // the 0xx range is the MANAGEMENT plane — configure, consumer groups, stats
    // readers, metrics, traces, streams register/state — which was never
    // rows-specific: it reads queen.queues / queen.stats / queen.worker_metrics,
    // tables both engines share.
    ("007_analytics.sql", include_str!("../sql/procedures/007_analytics.sql")),
    ("008_consumer_groups.sql", include_str!("../sql/procedures/008_consumer_groups.sql")),
    ("009_status.sql", include_str!("../sql/procedures/009_status.sql")),
    ("010_messages.sql", include_str!("../sql/procedures/010_messages.sql")),
    ("011_traces.sql", include_str!("../sql/procedures/011_traces.sql")),
    ("012_configure.sql", include_str!("../sql/procedures/012_configure.sql")),
    ("013_stats.sql", include_str!("../sql/procedures/013_stats.sql")),
    ("014_worker_metrics.sql", include_str!("../sql/procedures/014_worker_metrics.sql")),
    ("015_postgres_stats.sql", include_str!("../sql/procedures/015_postgres_stats.sql")),
    ("017_retention_analytics.sql", include_str!("../sql/procedures/017_retention_analytics.sql")),
    ("018_prometheus.sql", include_str!("../sql/procedures/018_prometheus.sql")),
    ("019_streams_schema.sql", include_str!("../sql/procedures/019_streams_schema.sql")),
    ("020_streams_register_query_v1.sql", include_str!("../sql/procedures/020_streams_register_query_v1.sql")),
    ("022_streams_state_get_v1.sql", include_str!("../sql/procedures/022_streams_state_get_v1.sql")),
    // ------------------------------------------------------------- log engine
    // Greenfield replacement of the seg_* family (18-log-engine.md). 040 first:
    // it idempotently drops every legacy seg_* function/table so old dev DBs boot
    // clean before 041+ create the log schema. 049 and 050 then remove the rows
    // engine's functions and its tables — after this build there is one engine,
    // and the shared management procedures in 007-022 no longer carry a second
    // branch for the other one.
    ("040_log_drop_legacy.sql", include_str!("../sql/procedures/040_log_drop_legacy.sql")),
    ("041_log_schema.sql", include_str!("../sql/procedures/041_log_schema.sql")),
    ("042_log_push.sql", include_str!("../sql/procedures/042_log_push.sql")),
    ("043_log_pop.sql", include_str!("../sql/procedures/043_log_pop.sql")),
    ("044_log_ack.sql", include_str!("../sql/procedures/044_log_ack.sql")),
    ("045_log_maintenance.sql", include_str!("../sql/procedures/045_log_maintenance.sql")),
    ("046_log_streams.sql", include_str!("../sql/procedures/046_log_streams.sql")),
    ("047_log_admin.sql", include_str!("../sql/procedures/047_log_admin.sql")),
    ("048_log_stats.sql", include_str!("../sql/procedures/048_log_stats.sql")),
    // Last: removes the rows message plane from databases that already applied
    // the files deleted above. Runs after everything so a dependency error names
    // a live dependant rather than an apply-order artifact.
    ("049_drop_rows_message_plane.sql", include_str!("../sql/procedures/049_drop_rows_message_plane.sql")),
    // Last of all: the rows tables themselves. Runs after every procedure has
    // been redefined without them, so nothing that could still reference one is
    // created after the drop.
    ("050_drop_rows_tables.sql", include_str!("../sql/procedures/050_drop_rows_tables.sql")),
    // After 041 has created queen.log_partitions and 050 has taken the rows
    // tables away: attach the partition-lifecycle counters to the live table.
    ("051_log_partition_counters.sql", include_str!("../sql/procedures/051_log_partition_counters.sql")),
];

/// Apply the schema at boot. Set `QUEEN_APPLY_SCHEMA=0` to skip (e.g. when the DB
/// is managed externally). Fails the process on any DDL error (fail-fast boot).
pub async fn apply(pool: &Pool) -> Result<(), Box<dyn std::error::Error>> {
    if !crate::config::env_bool("QUEEN_APPLY_SCHEMA", true) {
        tracing::info!(target: "schema", "apply skipped (QUEEN_APPLY_SCHEMA=0)");
        return Ok(());
    }

    let client = pool.get().await?;

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
        "applied schema.sql + procedures"
    );
    Ok(())
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
