//! pxdb pool + migrations. OWNER: Agent B (schema task). The skeleton only
//! builds the pool; migrations list is filled by Agent B (embedded include_str!,
//! applied in order, recorded in queen_proxy.schema_migrations).

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::NoTls;

use crate::config::PxdbConfig;

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
    let client = pool.get().await.map_err(|e| format!("pxdb connect: {e}"))?;
    client.simple_query("SELECT 1").await.map_err(|e| format!("pxdb ping: {e}"))?;
    Ok(pool)
}

/// Ordered embedded migrations. Agent B: append (name, sql) pairs here.
pub fn migrations() -> Vec<(&'static str, &'static str)> {
    vec![
        // ("001_init", include_str!("../migrations/001_init.sql")),
    ]
}

pub async fn apply_migrations(pool: &Pool) -> Result<(), String> {
    let client = pool.get().await.map_err(|e| e.to_string())?;
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS queen_proxy;
             CREATE TABLE IF NOT EXISTS queen_proxy.schema_migrations (
               name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
        )
        .await
        .map_err(|e| e.to_string())?;
    for (name, sql) in migrations() {
        let done = client
            .query_opt("SELECT 1 FROM queen_proxy.schema_migrations WHERE name=$1", &[&name])
            .await
            .map_err(|e| e.to_string())?
            .is_some();
        if done {
            continue;
        }
        client.batch_execute(sql).await.map_err(|e| format!("migration {name}: {e}"))?;
        client
            .execute("INSERT INTO queen_proxy.schema_migrations(name) VALUES ($1)", &[&name])
            .await
            .map_err(|e| e.to_string())?;
        tracing::info!(migration = name, "applied");
    }
    Ok(())
}
