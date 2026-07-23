mod auth;
mod config;
mod db;
// Broker-side dedup cache (doc 18 §5). Storage-free; wired into the push path
// by the fusion slice of the log-engine rewrite.
#[allow(dead_code)]
mod dedup;
mod encryption;
mod file_buffer;
mod frames;
mod fusion;
mod handlers;
mod httpget;
mod internal;
mod mesh;
mod metrics;
mod migrate;
mod notify;
mod pgtls;
mod reconcile;
mod retention;
mod schema;
mod stats;
mod syscollect;
mod util;
mod vegas;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::routing::{get, post};

/// Broker version, embedded from server.json at build time (see build.rs). Single
/// source of truth shared with the Docker image tag (build.sh) and /health.
pub const VERSION: &str = env!("QUEEN_VERSION");
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

    // JWT auth. Disabled by default (JWT_ENABLED=false) → the middleware is a
    // transparent pass-through and every request is served with no token, exactly
    // as the rest of the test-suite expects.
    // RUSTFIX item 7: validate the auth config at startup and FAIL FAST (like the
    // C++ acceptor_server.cpp:719-726) when enabled with no usable key material.
    if let Err(e) = cfg.auth.validate() {
        eprintln!("FATAL: invalid JWT auth configuration: {e}");
        std::process::exit(1);
    }
    let authenticator = auth::Authenticator::new(cfg.auth.clone());
    if cfg.auth.enabled {
        println!(
            "queen-seg-rust: JWT auth ENABLED (algorithm={}, skip_paths={:?})",
            cfg.auth.algorithm, cfg.auth.skip_paths
        );
        // RUSTFIX item 7: for RS256/EdDSA/auto with a JWKS URL, pre-fetch on boot
        // and refresh on the configured interval (jwt_validator.cpp:64-73,540-545).
        if authenticator.uses_jwks() {
            // Security: a plaintext JWKS URL lets a network MITM substitute the JWT
            // *verification* keys and forge accepted tokens. We fetch it anyway
            // (localhost/in-cluster JWKS-over-http is a legitimate setup) but warn
            // loudly — matching the warn-on-security-downgrade posture elsewhere.
            if cfg.auth.jwks_url.to_ascii_lowercase().starts_with("http://") {
                eprintln!(
                    "WARNING: JWT_JWKS_URL is plaintext http:// ({}) — signing keys are fetched over cleartext and are MITM-forgeable; use https://",
                    cfg.auth.jwks_url
                );
            }
            match authenticator.fetch_jwks().await {
                Ok(n) => println!("queen-seg-rust: JWKS pre-fetch OK ({n} keys)"),
                Err(e) => eprintln!("queen-seg-rust: JWKS pre-fetch failed: {e} (will retry on demand)"),
            }
            let a = authenticator.clone();
            let interval = authenticator.jwks_refresh_interval();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    if let Err(e) = a.fetch_jwks().await {
                        eprintln!("queen-seg-rust: JWKS refresh failed: {e}");
                    }
                }
            });
        }
    }

    // Background retention + eviction sweep (segments-targeted). Spawned before
    // the HTTP server so the RETENTION_INTERVAL cadence is live as soon as the
    // broker accepts pushes (retention.js starts us with RETENTION_INTERVAL=2000).
    retention::spawn(pool.clone(), &cfg);

    // Background stats reconciler: recomputes queen.stats from the segments tables
    // on a cadence (the v1 reconciler reads the empty queen.messages under segments,
    // so this segments-native path is what keeps the dashboard's stats-backed pages
    // from reading zero). Advisory-locked -> one replica refreshes per cycle.
    stats::spawn(pool.clone(), &cfg);

    // Background metrics collector: per-minute worker throughput -> queen.worker_metrics
    // (+ summary trigger) and host/process gauges -> queen.system_metrics. Feeds the
    // System view and the dashboard throughput / lifetime totals.
    syscollect::spawn(pool.clone(), metrics.clone(), &cfg);
    // Scheduler-lag ("event loop") probe + 1 Hz parked-long-poll sampler — the
    // sources for worker_metrics.avg/max_event_loop_lag_ms and
    // queue_lag_metrics.parked_count (dashboard Event-loop and Parked rows).
    metrics::spawn_samplers(metrics.clone());

    let fusion = fusion::Fusion::new(
        cfg.fusion_shards,
        pool.clone(),
        push_vegas.clone(),
        metrics.clone(),
        cfg.zstd_level,
        cfg.fusion_frames,
        cfg.fusion_hold_ms,
        cfg.stmt_timeout,
        cfg.dedup_cache_mb,
        cfg.dedup_cache_enabled,
        cfg.pg_use_ssl,
        cfg.pg_ssl_reject_unauthorized,
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

    // Long-poll waker + inter-instance notifier. Always constructed (the local
    // waker needs no cluster); the UDP transport is attached below only when peers
    // are configured.
    let notifier = notify::Notifier::new();

    // File buffer (RUSTFIX items 1 & 17): DB-outage / maintenance push spool. Drain
    // any leftover on-disk spool BEFORE serving (startup recovery, capped 3600s) so
    // a restart during an outage never loses buffered messages. If maintenance mode
    // is already on at boot, pause draining so the disable path drives it.
    // At-rest payload encryption (RUSTFIX item 8). Disabled unless
    // QUEEN_ENCRYPTION_KEY is a valid 64-hex key.
    let encryption = encryption::Encryption::from_env();

    let file_buffer = Arc::new(file_buffer::FileBufferManager::new(
        cfg.file_buffer.clone(),
        cfg.zstd_level,
    ));
    file_buffer.startup_recovery(&pool).await;
    if init_maint {
        file_buffer.pause_background_drain();
    }
    file_buffer::spawn_drain(file_buffer.clone(), pool.clone());

    let state = Arc::new(AppState {
        pool: pool.clone(),
        fusion,
        push_vegas: push_vegas.clone(),
        pop_vegas: pop_vegas.clone(),
        metrics: metrics.clone(),
        stmt_timeout: cfg.stmt_timeout,
        pop_default_timeout_ms: cfg.pop_default_timeout_ms,
        pop_wait_initial_interval_ms: cfg.pop_wait_initial_interval_ms,
        pop_wait_backoff_threshold: cfg.pop_wait_backoff_threshold,
        pop_wait_backoff_multiplier: cfg.pop_wait_backoff_multiplier,
        pop_wait_max_interval_ms: cfg.pop_wait_max_interval_ms,
        zstd_level: cfg.zstd_level,
        lease_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
        encryption: encryption.clone(),
        enc_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
        maintenance: std::sync::atomic::AtomicBool::new(init_maint),
        pop_maintenance: std::sync::atomic::AtomicBool::new(init_pop_maint),
        notifier: notifier.clone(),
        file_buffer: file_buffer.clone(),
        partition_queue: std::sync::Mutex::new(std::collections::HashMap::new()),
    });

    // RUSTFIX item 16: periodic reconcile — re-read the maintenance flags from
    // queen.system_state and drop the per-queue caches, so a replica heals within
    // one interval after a lost UDP packet. Spawned unconditionally (single-node
    // also benefits from DB reconvergence).
    reconcile::spawn(state.clone(), pool.clone(), cfg.sync.cache_refresh_ms);

    // Inter-instance mesh notifications. Gated on QUEEN_SYNC_ENABLED (default true)
    // AND at least one configured peer — a single stock broker binds nothing and
    // sends nothing, behaving exactly as before (only the in-process waker, wired
    // above, is live). Inbound frames apply peer effects to local state:
    // MESSAGE_AVAILABLE wakes local pops (no re-broadcast), maintenance flips flip
    // the atomics, queue-config changes drop the stale lease-cache entry.
    if cfg.sync.mesh_active() {
        let handlers = mesh::SyncHandlers {
            on_message_available: {
                // A peer push wakes local pops AND records the partition hint, so a
                // woken pop can target it (Phase 2) — no re-broadcast. Both the
                // single and batched MESSAGE_AVAILABLE forms route through here.
                let n = notifier.clone();
                Box::new(move |q: &str, p: &str| n.wake_local_hint(q, p))
            },
            on_maintenance: {
                let s = state.clone();
                Box::new(move |e: bool| {
                    let prev = s.maintenance.swap(e, Ordering::Relaxed);
                    if prev != e {
                        // Keep the file-buffer drain in sync with a peer's flip (item 17).
                        if e {
                            s.file_buffer.pause_background_drain();
                        } else {
                            s.file_buffer.force_finalize_all();
                            s.file_buffer.resume_background_drain();
                        }
                    }
                })
            },
            on_pop_maintenance: {
                let s = state.clone();
                Box::new(move |e: bool| s.pop_maintenance.store(e, Ordering::Relaxed))
            },
            on_queue_config_set: {
                let s = state.clone();
                Box::new(move |q: &str| {
                    s.lease_cache.lock().unwrap().remove(q);
                    // RUSTFIX item 8: a peer's config change may flip encryption_enabled.
                    s.enc_cache.lock().unwrap().remove(q);
                })
            },
            on_queue_config_delete: {
                let s = state.clone();
                Box::new(move |q: &str| {
                    s.lease_cache.lock().unwrap().remove(q);
                    s.enc_cache.lock().unwrap().remove(q);
                })
            },
        };
        match mesh::MeshTransport::bind(&cfg.sync, handlers).await {
            Ok((t, bindings)) => {
                notifier.attach_transport(t.clone());
                t.start(bindings);
            }
            Err(e) => eprintln!(
                "WARN: TCP mesh bind failed on :{} ({e}) — continuing with local waker only",
                cfg.sync.mesh_port
            ),
        }
    } else if cfg.sync.enabled {
        println!("queen-seg-rust: mesh sync enabled but no peers configured — local waker only");
    }

    let app = Router::new()
        .route("/api/v1/push", post(handlers::handle_push))
        // Namespace/task discovery pop (no queue in the path). Registered before
        // the `/pop/queue/:queue` routes; matchit keeps the static `/api/v1/pop`
        // distinct from the deeper queue-scoped paths.
        .route("/api/v1/pop", get(handlers::handle_pop_discover))
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
        // Resources LIST API. Static siblings (queues/overview/namespaces/tasks)
        // registered alongside the `:queue` param route; matchit keeps the static
        // `/resources/queues` distinct from the deeper `/resources/queues/:queue`.
        .route("/api/v1/resources/queues", get(handlers::handle_list_queues))
        .route(
            "/api/v1/resources/overview",
            get(handlers::handle_system_overview),
        )
        .route(
            "/api/v1/resources/namespaces",
            get(handlers::handle_list_namespaces),
        )
        .route("/api/v1/resources/tasks", get(handlers::handle_list_tasks))
        .route(
            "/api/v1/resources/queues/:queue",
            get(handlers::handle_get_queue).delete(handlers::handle_delete_queue),
        )
        // ---------------------------------------------------- management surface
        .route("/api/v1/messages", get(handlers::handle_list_messages))
        .route(
            "/api/v1/messages/:partitionId/:transactionId",
            get(handlers::handle_get_message).delete(handlers::handle_delete_message),
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
        // Static `/status/analytics` + `/status/buffers` registered before the
        // `/status/queues/:queue` param route so matchit keeps them distinct.
        .route("/api/v1/status/analytics", get(handlers::handle_status_analytics))
        .route("/api/v1/status/buffers", get(handlers::handle_status_buffers))
        .route("/api/v1/status/queues", get(handlers::handle_status_queues))
        .route(
            "/api/v1/status/queues/:queue",
            get(handlers::handle_queue_detail),
        )
        // -------------------------------------------------- analytics / metrics
        .route(
            "/api/v1/analytics/system-metrics",
            get(handlers::handle_system_metrics),
        )
        .route(
            "/api/v1/analytics/worker-metrics",
            get(handlers::handle_worker_metrics),
        )
        .route("/api/v1/analytics/queue-lag", get(handlers::handle_queue_lag))
        .route("/api/v1/analytics/queue-ops", get(handlers::handle_queue_ops))
        .route(
            "/api/v1/analytics/queue-parked-replicas",
            get(handlers::handle_queue_parked_replicas),
        )
        .route("/api/v1/analytics/retention", get(handlers::handle_retention))
        .route(
            "/api/v1/analytics/postgres-stats",
            get(handlers::handle_postgres_stats),
        )
        // ------------------------------------------------------------ migration
        .route(
            "/api/v1/migration/test-connection",
            post(handlers::handle_migration_test),
        )
        .route("/api/v1/migration/start", post(handlers::handle_migration_start))
        .route("/api/v1/migration/status", get(handlers::handle_migration_status))
        .route(
            "/api/v1/migration/validate",
            post(handlers::handle_migration_validate),
        )
        .route("/api/v1/migration/reset", post(handlers::handle_migration_reset))
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
            "/api/v1/consumer-groups/:group/queues/:queue",
            axum::routing::delete(handlers::handle_delete_consumer_group_for_queue),
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
        // ------------------------------------- internal inter-instance surface
        .route("/internal/api/notify", post(internal::handle_notify))
        .route(
            "/internal/api/shared-state/stats",
            get(internal::handle_shared_state_stats),
        )
        .route(
            "/internal/api/inter-instance/stats",
            get(internal::handle_inter_instance_stats),
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
        // SPA dashboard: any request not matching a route above is served from
        // QUEEN_STATIC_DIR (default webapp/dist), falling back to index.html for
        // client-side routes. A fallback only, so it never shadows /api/v1; 404s
        // when the dir is absent (dev/CI). Replaces the C++ server's static surface.
        .fallback(handlers::handle_static)
        // Raise the request body cap above axum's 2 MiB default so large payloads
        // (pushLargePayload) don't 413. Configurable via QUEEN_MAX_BODY_BYTES.
        .layer(axum::extract::DefaultBodyLimit::max(
            std::env::var("QUEEN_MAX_BODY_BYTES")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(64 * 1024 * 1024),
        ))
        // Auth runs outermost: it validates the token + route level before any
        // handler, and stamps AuthedSub into request extensions for producer_sub.
        .layer(axum::middleware::from_fn_with_state(
            authenticator.clone(),
            auth::auth_middleware,
        ))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cfg.port);
    println!(
        "queen-seg-rust v{} listening on {addr} (fusion shards={} frames={} hold={}ms, zstd={}, pool={})",
        VERSION, cfg.fusion_shards, cfg.fusion_frames, cfg.fusion_hold_ms, cfg.zstd_level, cfg.pool_size
    );
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    // TCP_NODELAY on every accepted connection (doc 18 §10): the broker's
    // responses are small latency-sensitive JSON frames; Nagle would add up to
    // one delayed-ACK RTT per response. axum 0.7.9's Serve builder exposes this
    // directly (serve.rs sets it right after accept), so no custom accept loop.
    axum::serve(listener, app).tcp_nodelay(true).await.unwrap();
}
