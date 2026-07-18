mod config;
mod db;
mod frames;
mod fusion;
mod handlers;
mod metrics;
mod util;
mod vegas;

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;

use handlers::AppState;

#[tokio::main]
async fn main() {
    let cfg = config::load();
    let pool = db::create_pool(&cfg);

    let push_vegas = vegas::Vegas::new(
        cfg.push_init,
        cfg.push_min,
        cfg.push_max,
        cfg.vegas_alpha,
        cfg.vegas_beta,
    );
    let pop_vegas = vegas::Vegas::new(
        cfg.pop_init,
        cfg.pop_min,
        cfg.pop_max,
        cfg.vegas_alpha,
        cfg.vegas_beta,
    );

    let metrics = Arc::new(metrics::Metrics::new());

    let fusion = fusion::Fusion::new(
        cfg.fusion_shards,
        pool.clone(),
        push_vegas.clone(),
        metrics.clone(),
        cfg.zstd_level,
        cfg.fusion_frames,
        cfg.fusion_hold_ms,
        cfg.stmt_timeout,
    );

    let state = Arc::new(AppState {
        pool: pool.clone(),
        fusion,
        push_vegas: push_vegas.clone(),
        pop_vegas: pop_vegas.clone(),
        metrics: metrics.clone(),
        stmt_timeout: cfg.stmt_timeout,
        pop_default_timeout_ms: cfg.pop_default_timeout_ms,
        pop_wait_poll_ms: cfg.pop_wait_poll_ms,
    });

    let app = Router::new()
        .route("/api/v1/push", post(handlers::handle_push))
        .route("/api/v1/pop/queue/:queue", get(handlers::handle_pop))
        .route("/status", get(handlers::handle_status))
        .route("/metrics", get(handlers::handle_metrics))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cfg.port);
    println!(
        "queen-seg-rust listening on {addr} (fusion shards={} frames={} hold={}ms, zstd={}, pool={})",
        cfg.fusion_shards, cfg.fusion_frames, cfg.fusion_hold_ms, cfg.zstd_level, cfg.pool_size
    );
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
