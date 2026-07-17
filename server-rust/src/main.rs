mod auth;
mod config;
mod crypto;
mod db;
mod engine;
mod features;
mod handlers;
mod metrics;
mod notify;
mod util;

use std::sync::Arc;
use std::time::Duration;

use axum::routing::{get, post};
use axum::Router;
use deadpool_postgres::{PoolConfig, Runtime};
use tokio_postgres::NoTls;

use crate::engine::{Engine, LaneSpec};
use crate::handlers::{AppState, PopCfg};
use crate::metrics::Metrics;
use crate::notify::Notifier;

#[tokio::main]
async fn main() {
    let mut cfg = config::load();

    let pool_size = cfg.global_concurrency + 8;
    cfg.pg.pool = Some(PoolConfig::new(pool_size));
    let pool = cfg
        .pg
        .create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("failed to create pool");

    // Dedicated sidecar pool for background retention + metrics flush, so they
    // never contend with the hot path for connections (C++ SIDECAR_POOL_SIZE).
    let aux_size = std::env::var("SIDECAR_POOL_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64usize);
    let mut aux_cfg = cfg.pg.clone();
    aux_cfg.pool = Some(PoolConfig::new(aux_size));
    let aux_pool = aux_cfg
        .create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("failed to create aux pool");

    // Wait for Postgres.
    for i in 0..60 {
        match pool.get().await {
            Ok(_) => break,
            Err(e) => {
                if i == 59 {
                    panic!("postgres not ready: {e}");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    let pl = db::PartitionLookup::new(pool.clone(), cfg.stmt_timeout, cfg.partition_flush);
    db::start_reconcile(pool.clone(), cfg.reconcile_interval, cfg.reconcile_lookback, cfg.stmt_timeout);

    let metrics = Arc::new(Metrics::new());
    let specs = vec![
        LaneSpec {
            name: "push",
            sql: "SELECT queen.push_messages_v3($1::text::jsonb)::text".into(),
            policy: cfg.push.clone(),
            weight: cfg.push_weight,
            gate: true,
            max_parts: cfg.push_max_parts,
            push_object: true,
            metrics: metrics.push.clone(),
            on_pu: Some(pl.on_pu()),
        },
        LaneSpec {
            name: "pop",
            sql: "SELECT queen.pop_unified_batch_v4($1::text::jsonb)::text".into(),
            policy: cfg.pop.clone(),
            weight: cfg.pop_weight,
            gate: false,
            max_parts: 0,
            push_object: false,
            metrics: metrics.pop.clone(),
            on_pu: None,
        },
        LaneSpec {
            name: "ack",
            sql: "SELECT queen.ack_messages_v2($1::text::jsonb)::text".into(),
            policy: cfg.ack.clone(),
            weight: cfg.ack_weight,
            gate: false,
            max_parts: 0,
            push_object: false,
            metrics: metrics.ack.clone(),
            on_pu: None,
        },
    ];
    let engine = Engine::new(specs, pool.clone(), cfg.global_concurrency, cfg.stmt_timeout);

    let auth = auth::Auth::new(&std::env::var("QUEEN_JWT_SECRET").unwrap_or_default());
    let crypto = crypto::Crypto::new(&std::env::var("QUEEN_ENCRYPTION_KEY").unwrap_or_default());
    let full = std::env::var("QUEEN_FULL_FEATURES").unwrap_or_default() == "1";
    let feats = std::sync::Arc::new(features::Features::new(full, pool.clone(), aux_pool.clone()));
    features::start_background(feats.clone(), cfg.stmt_timeout);
    if full {
        println!("[rust-hotpath] full features (per-queue metrics, lag, config cache, retention, metrics flush)");
    }
    if auth.is_some() {
        println!("[rust-hotpath] auth enabled (per-request JWT HS256 verify)");
    }
    if crypto.is_some() {
        println!("[rust-hotpath] encryption enabled (AES-256-GCM per message)");
    }

    let state = Arc::new(AppState {
        engine,
        metrics: metrics.clone(),
        notifier: Notifier::new(),
        pool: pool.clone(),
        auth,
        crypto,
        features: feats,
        pop: PopCfg {
            initial_ms: cfg.pop_wait_initial_ms,
            threshold: cfg.pop_wait_threshold,
            multiplier: cfg.pop_wait_multiplier,
            max_ms: cfg.pop_wait_max_ms,
            default_timeout_ms: cfg.pop_default_timeout_ms,
            stmt_timeout: cfg.stmt_timeout,
        },
    });

    let app = Router::new()
        .route("/api/v1/push", post(handlers::handle_push))
        .route("/api/v1/pop/queue/:queue", get(handlers::handle_pop_wildcard))
        .route("/api/v1/pop/queue/:queue/partition/:partition", get(handlers::handle_pop_partition))
        .route("/api/v1/ack", post(handlers::handle_ack))
        .route("/api/v1/ack/batch", post(handlers::handle_ack_batch))
        .route("/api/v1/configure", post(handlers::handle_configure))
        .route("/api/v1/status", get(handlers::handle_status))
        .route("/metrics/prometheus", get(handlers::handle_metrics))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cfg.port);
    println!(
        "[rust-hotpath] listening on {} | global={} push{{pref={},hold={:?}}} pop{{pref={},hold={:?}}}",
        addr, cfg.global_concurrency, cfg.push.preferred, cfg.push.max_hold, cfg.pop.preferred, cfg.pop.max_hold
    );
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind failed");
    axum::serve(listener, app).await.expect("server error");
}
