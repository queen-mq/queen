//! Boot-time schema applier.
//!
//! Neither Rust experiment applied schema before — they relied on the C++ broker
//! to create `queen.*` / `queen.seg_*`. This broker is standalone, so it applies
//! `lib/schema/schema.sql` + every `procedures/*.sql` at startup, in lexical order
//! (the same order `AsyncQueueManager::initialize_schema()` uses in the C++ broker,
//! so `027`'s dual-engine redefinitions land after the originals they supersede).
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
    ("001_push.sql", include_str!("../sql/procedures/001_push.sql")),
    ("002_pop_unified.sql", include_str!("../sql/procedures/002_pop_unified.sql")),
    ("002b_pop_unified_v2.sql", include_str!("../sql/procedures/002b_pop_unified_v2.sql")),
    ("002c_pop_unified_v3.sql", include_str!("../sql/procedures/002c_pop_unified_v3.sql")),
    ("002d_pop_unified_v4.sql", include_str!("../sql/procedures/002d_pop_unified_v4.sql")),
    ("003_ack.sql", include_str!("../sql/procedures/003_ack.sql")),
    ("004_transaction.sql", include_str!("../sql/procedures/004_transaction.sql")),
    ("005_renew_lease.sql", include_str!("../sql/procedures/005_renew_lease.sql")),
    ("006_has_pending.sql", include_str!("../sql/procedures/006_has_pending.sql")),
    ("007_analytics.sql", include_str!("../sql/procedures/007_analytics.sql")),
    ("008_consumer_groups.sql", include_str!("../sql/procedures/008_consumer_groups.sql")),
    ("009_status.sql", include_str!("../sql/procedures/009_status.sql")),
    ("010_messages.sql", include_str!("../sql/procedures/010_messages.sql")),
    ("011_traces.sql", include_str!("../sql/procedures/011_traces.sql")),
    ("012_configure.sql", include_str!("../sql/procedures/012_configure.sql")),
    ("013_stats.sql", include_str!("../sql/procedures/013_stats.sql")),
    ("014_worker_metrics.sql", include_str!("../sql/procedures/014_worker_metrics.sql")),
    ("015_postgres_stats.sql", include_str!("../sql/procedures/015_postgres_stats.sql")),
    ("016_partition_lookup.sql", include_str!("../sql/procedures/016_partition_lookup.sql")),
    ("017_retention_analytics.sql", include_str!("../sql/procedures/017_retention_analytics.sql")),
    ("018_prometheus.sql", include_str!("../sql/procedures/018_prometheus.sql")),
    ("019_streams_schema.sql", include_str!("../sql/procedures/019_streams_schema.sql")),
    ("020_streams_register_query_v1.sql", include_str!("../sql/procedures/020_streams_register_query_v1.sql")),
    ("021_streams_cycle_v1.sql", include_str!("../sql/procedures/021_streams_cycle_v1.sql")),
    ("022_streams_state_get_v1.sql", include_str!("../sql/procedures/022_streams_state_get_v1.sql")),
    ("023_storage_v2.sql", include_str!("../sql/procedures/023_storage_v2.sql")),
    ("024_storage_v2_pop_ext.sql", include_str!("../sql/procedures/024_storage_v2_pop_ext.sql")),
    ("025_storage_v2_dlq.sql", include_str!("../sql/procedures/025_storage_v2_dlq.sql")),
    ("026_storage_v2_maintenance.sql", include_str!("../sql/procedures/026_storage_v2_maintenance.sql")),
    ("027_storage_v2_observability.sql", include_str!("../sql/procedures/027_storage_v2_observability.sql")),
    ("028_storage_v2_migrate.sql", include_str!("../sql/procedures/028_storage_v2_migrate.sql")),
    ("029_seg_streams.sql", include_str!("../sql/procedures/029_seg_streams.sql")),
    ("030_seg_traces.sql", include_str!("../sql/procedures/030_seg_traces.sql")),
    ("031_seg_consumer_groups.sql", include_str!("../sql/procedures/031_seg_consumer_groups.sql")),
    ("032_seg_push_multi.sql", include_str!("../sql/procedures/032_seg_push_multi.sql")),
    ("033_seg_pop_discover.sql", include_str!("../sql/procedures/033_seg_pop_discover.sql")),
    // Segments-native queen.stats refresh + segments-aware get_queue_detail_v2.
    // Loads after the originals (013/027) it complements/supersedes.
    ("034_seg_stats_refresh.sql", include_str!("../sql/procedures/034_seg_stats_refresh.sql")),
    // Applied LAST: retire the rows engine (drop rows message store + hot-path,
    // redefine kept observability/trace functions segments-only, default to segments).
    ("099_retire_rows.sql", include_str!("../sql/procedures/099_retire_rows.sql")),
];

/// Apply the schema at boot. Set `QUEEN_APPLY_SCHEMA=0` to skip (e.g. when the DB
/// is managed externally). Fails the process on any DDL error (fail-fast boot).
pub async fn apply(pool: &Pool) -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("QUEEN_APPLY_SCHEMA").ok().as_deref() == Some("0") {
        println!("schema: apply skipped (QUEEN_APPLY_SCHEMA=0)");
        return Ok(());
    }

    let client = pool.get().await?;

    // Serialize DDL across replicas. Session-level lock held on THIS connection;
    // released below before the client returns to the pool.
    client
        .execute("SELECT pg_advisory_lock($1)", &[&SCHEMA_LOCK_KEY])
        .await?;

    let result = apply_all(&client).await;

    // Always release the lock, even if apply failed.
    let _ = client
        .execute("SELECT pg_advisory_unlock($1)", &[&SCHEMA_LOCK_KEY])
        .await;

    result?;
    println!(
        "schema: applied schema.sql + {} procedures",
        PROCEDURES.len()
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
