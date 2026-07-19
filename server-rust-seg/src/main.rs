mod config;
mod db;
mod frames;
mod fusion;
mod handlers;
mod metrics;
mod migrate;
mod retention;
mod schema;
mod util;
mod vegas;

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;

use handlers::AppState;

#[tokio::main]
async fn main() {
    let cfg = config::load();

    // Subcommand dispatch: `queen-seg migrate ...` runs the offline rows→segments
    // migration and exits, without ever starting the HTTP server. Kept minimal so
    // the server path below is untouched.
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("migrate") {
        migrate::run(cfg, args).await;
        return;
    }

    let pool = db::create_pool(&cfg);

    // Apply schema.sql + procedures/*.sql at boot (advisory-locked). Fail fast if
    // the DB can't be brought to the expected shape.
    if let Err(e) = schema::apply(&pool).await {
        eprintln!("FATAL: schema apply failed: {e}");
        std::process::exit(1);
    }

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

    // Background retention + eviction sweep (segments-targeted). Spawned before
    // the HTTP server so the RETENTION_INTERVAL cadence is live as soon as the
    // broker accepts pushes (retention.js starts us with RETENTION_INTERVAL=2000).
    retention::spawn(pool.clone(), &cfg);

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

    // Seed the maintenance flags from queen.system_state (parity with the C++
    // SharedStateManager, which reads the same {"enabled":..} rows at boot).
    let (init_maint, init_pop_maint) = match pool.get().await {
        Ok(c) => (
            db::get_system_flag(&c, "maintenance_mode").await.unwrap_or(false),
            db::get_system_flag(&c, "pop_maintenance_mode").await.unwrap_or(false),
        ),
        Err(_) => (false, false),
    };

    let state = Arc::new(AppState {
        pool: pool.clone(),
        fusion,
        push_vegas: push_vegas.clone(),
        pop_vegas: pop_vegas.clone(),
        metrics: metrics.clone(),
        stmt_timeout: cfg.stmt_timeout,
        pop_default_timeout_ms: cfg.pop_default_timeout_ms,
        pop_wait_poll_ms: cfg.pop_wait_poll_ms,
        zstd_level: cfg.zstd_level,
        lease_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
        maintenance: std::sync::atomic::AtomicBool::new(init_maint),
        pop_maintenance: std::sync::atomic::AtomicBool::new(init_pop_maint),
    });

    let app = Router::new()
        .route("/api/v1/push", post(handlers::handle_push))
        .route("/api/v1/pop/queue/:queue", get(handlers::handle_pop))
        .route(
            "/api/v1/pop/queue/:queue/partition/:partition",
            get(handlers::handle_pop_partition),
        )
        .route("/api/v1/ack", post(handlers::handle_ack))
        .route("/api/v1/ack/batch", post(handlers::handle_ack_batch))
        .route("/api/v1/transaction", post(handlers::handle_transaction))
        .route(
            "/api/v1/lease/:leaseId/extend",
            post(handlers::handle_lease_extend),
        )
        .route("/api/v1/configure", post(handlers::handle_configure))
        .route(
            "/api/v1/resources/queues/:queue",
            get(handlers::handle_get_queue).delete(handlers::handle_delete_queue),
        )
        // ---------------------------------------------------- management surface
        .route("/api/v1/messages", get(handlers::handle_list_messages))
        .route(
            "/api/v1/messages/:partitionId/:transactionId",
            get(handlers::handle_get_message),
        )
        .route("/api/v1/dlq", get(handlers::handle_dlq))
        .route("/api/v1/traces", post(handlers::handle_record_trace))
        .route("/api/v1/traces/names", get(handlers::handle_trace_names))
        .route(
            "/api/v1/traces/by-name/:traceName",
            get(handlers::handle_traces_by_name),
        )
        .route(
            "/api/v1/traces/:partitionId/:transactionId",
            get(handlers::handle_message_traces),
        )
        .route("/api/v1/status", get(handlers::handle_api_status))
        .route("/api/v1/status/queues", get(handlers::handle_status_queues))
        // ------------------------------------------------ consumer groups
        .route(
            "/api/v1/consumer-groups",
            get(handlers::handle_consumer_groups),
        )
        // Static `lagging` registered before the :group param route so it wins.
        .route(
            "/api/v1/consumer-groups/lagging",
            get(handlers::handle_lagging_consumers),
        )
        .route(
            "/api/v1/consumer-groups/:group",
            get(handlers::handle_consumer_group_details)
                .delete(handlers::handle_delete_consumer_group),
        )
        .route(
            "/api/v1/consumer-groups/:group/subscription",
            post(handlers::handle_update_subscription),
        )
        .route(
            "/api/v1/consumer-groups/:group/queues/:queue/seek",
            post(handlers::handle_seek_consumer_group),
        )
        .route(
            "/api/v1/consumer-groups/:group/queues/:queue/partitions/:partition/seek",
            post(handlers::handle_seek_partition),
        )
        .route("/api/v1/stats/refresh", post(handlers::handle_stats_refresh))
        // ------------------------------------------------ system maintenance
        .route(
            "/api/v1/system/maintenance",
            get(handlers::handle_get_maintenance).post(handlers::handle_set_maintenance),
        )
        .route(
            "/api/v1/system/maintenance/pop",
            get(handlers::handle_get_pop_maintenance).post(handlers::handle_set_pop_maintenance),
        )
        .route(
            "/api/v1/system/shared-state",
            get(handlers::handle_shared_state),
        )
        // ------------------------------------------------------------ streams
        .route(
            "/streams/v1/queries",
            post(handlers::handle_streams_register),
        )
        .route(
            "/streams/v1/state/get",
            post(handlers::handle_streams_state_get),
        )
        .route("/streams/v1/cycle", post(handlers::handle_streams_cycle))
        .route("/health", get(handlers::handle_health))
        .route("/metrics/prometheus", get(handlers::handle_prometheus))
        .route("/status", get(handlers::handle_status))
        .route("/metrics", get(handlers::handle_metrics))
        // Raise the request body cap above axum's 2 MiB default so large payloads
        // (pushLargePayload) don't 413. Configurable via QUEEN_MAX_BODY_BYTES.
        .layer(axum::extract::DefaultBodyLimit::max(
            std::env::var("QUEEN_MAX_BODY_BYTES")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(64 * 1024 * 1024),
        ))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cfg.port);
    println!(
        "queen-seg-rust listening on {addr} (fusion shards={} frames={} hold={}ms, zstd={}, pool={})",
        cfg.fusion_shards, cfg.fusion_frames, cfg.fusion_hold_ms, cfg.zstd_level, cfg.pool_size
    );
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
