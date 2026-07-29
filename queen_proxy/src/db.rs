//! pxdb pool + migrations. OWNER: Agent B (schema task). The skeleton only
//! builds the pool; migrations list is filled by Agent B (embedded include_str!,
//! applied in order, recorded in queen_proxy.schema_migrations).

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::NoTls;

use crate::config::PxdbConfig;

/// tokio_postgres::Error's own Display is unhelpfully terse for server-side
/// failures: `Kind::Db => "db error"`, full stop -- the actual severity,
/// message, DETAIL/HINT live on the nested `DbError` (via `.as_db_error()`),
/// whose Display carries them. Every `{e}`/`e.to_string()` in this file goes
/// through one of these instead so a migration failure is actually
/// diagnosable.
fn describe_pg_error(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db_err) => db_err.to_string(),
        None => e.to_string(),
    }
}

/// Same, for errors from `pool.get()` (deadpool wraps the backend error in
/// `PoolError::Backend`; the other variants -- Timeout/Closed/... -- have no
/// nested DbError and just use their own Display).
fn describe_pool_error(e: &deadpool_postgres::PoolError) -> String {
    match e {
        deadpool_postgres::PoolError::Backend(db_e) => describe_pg_error(db_e),
        other => other.to_string(),
    }
}

pub async fn create_pool(cfg: &PxdbConfig) -> Result<Pool, String> {
    let mut pg = tokio_postgres::Config::new();
    pg.host(&cfg.host)
        .port(cfg.port)
        .user(&cfg.user)
        .password(&cfg.password)
        .dbname(&cfg.dbname)
        .application_name("queen-proxy");
    let mgr_cfg = ManagerConfig { recycling_method: RecyclingMethod::Fast };
    let pool = if cfg.use_ssl {
        let connector = crate::pgtls::make_connector(cfg.ssl_reject_unauthorized);
        let mgr = Manager::from_config(pg, connector, mgr_cfg);
        Pool::builder(mgr).max_size(cfg.pool_size).build().map_err(|e| e.to_string())?
    } else {
        let mgr = Manager::from_config(pg, NoTls, mgr_cfg);
        Pool::builder(mgr).max_size(cfg.pool_size).build().map_err(|e| e.to_string())?
    };
    // fail fast on unreachable pxdb at boot
    let client = pool.get().await.map_err(|e| format!("pxdb connect: {}", describe_pool_error(&e)))?;
    client
        .simple_query("SELECT 1")
        .await
        .map_err(|e| format!("pxdb ping: {}", describe_pg_error(&e)))?;
    Ok(pool)
}

/// Ordered embedded migrations, applied in order and recorded by name in
/// queen_proxy.schema_migrations (apply_migrations below skips ones already
/// applied). SQL is include_str!-embedded, so `cargo build` after editing a
/// migration file before runtime-testing it (same gotcha as server/).
#[rustfmt::skip]
pub fn migrations() -> Vec<(&'static str, &'static str)> {
    vec![
        ("001_init", include_str!("../migrations/001_init.sql")),
        ("002_functions", include_str!("../migrations/002_functions.sql")),
        ("003_limit_override", include_str!("../migrations/003_limit_override.sql")),
        ("004_lifecycle", include_str!("../migrations/004_lifecycle.sql")),
        ("005_usage_prune", include_str!("../migrations/005_usage_prune.sql")),
    ]
}

pub async fn apply_migrations(pool: &Pool) -> Result<(), String> {
    let client = pool.get().await.map_err(|e| describe_pool_error(&e))?;
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS queen_proxy;
             CREATE TABLE IF NOT EXISTS queen_proxy.schema_migrations (
               name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
        )
        .await
        .map_err(|e| describe_pg_error(&e))?;
    for (name, sql) in migrations() {
        let done = client
            .query_opt("SELECT 1 FROM queen_proxy.schema_migrations WHERE name=$1", &[&name])
            .await
            .map_err(|e| describe_pg_error(&e))?
            .is_some();
        if done {
            continue;
        }
        client
            .batch_execute(sql)
            .await
            .map_err(|e| format!("migration {name}: {}", describe_pg_error(&e)))?;
        client
            .execute("INSERT INTO queen_proxy.schema_migrations(name) VALUES ($1)", &[&name])
            .await
            .map_err(|e| describe_pg_error(&e))?;
        tracing::info!(migration = name, "applied");
    }
    Ok(())
}
