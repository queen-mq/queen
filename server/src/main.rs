mod ack_fusion;
mod ack_registry;
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
mod hotlist;
mod httpget;
mod internal;
mod mesh;
mod metrics;
mod migrate;
mod notify;
mod obs;
mod pgtls;
mod reconcile;
mod retention;
mod schema;
mod stats;
mod syscollect;
mod tenant;
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

/// Drop a queue's cached lease time + encryption flag after a peer's config change.
/// Track B (§5): those caches are keyed by (tenant, name). A frame carrying the
/// tenant removes exactly one entry; a tenant-less frame (a pre-Track-B peer) falls
/// back to dropping every tenant's entry for that name — the cache is lazily
/// re-filled, so over-invalidation costs one lookup and is never wrong.
fn invalidate_queue_caches(st: &Arc<AppState>, tenant: Option<&str>, queue: &str) {
    match tenant {
        Some(t) => {
            let key = handlers::tenant_queue_key(t, queue);
            st.lease_cache.lock().unwrap().remove(&key);
            st.enc_cache.lock().unwrap().remove(&key);
        }
        None => {
            let suffix = format!("\u{1f}{queue}");
            st.lease_cache.lock().unwrap().retain(|k, _| !k.ends_with(&suffix));
            st.enc_cache.lock().unwrap().retain(|k, _| !k.ends_with(&suffix));
        }
    }
}

#[tokio::main]
async fn main() {
    // LOGGING_PLAN.md Phase 0: install the tracing subscriber (honours
    // LOG_LEVEL/RUST_LOG) and the panic hook BEFORE anything can log or panic.
    obs::init();
    obs::install_panic_hook();
    let cfg = config::load();

    // Subcommand dispatch: `queen migrate ...` runs the offline rows→segments
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
        obs::fatal(format!("schema apply failed: {e}"));
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
        obs::fatal(format!("invalid JWT auth configuration: {e}"));
    }
    let authenticator = auth::Authenticator::new(cfg.auth.clone());
    if cfg.auth.enabled {
        tracing::info!(
            target: "auth",
            algorithm = %cfg.auth.algorithm,
            skip_paths = ?cfg.auth.skip_paths,
            "JWT auth ENABLED"
        );
        // RUSTFIX item 7: for RS256/EdDSA/auto with a JWKS URL, pre-fetch on boot
        // and refresh on the configured interval (jwt_validator.cpp:64-73,540-545).
        if authenticator.uses_jwks() {
            // Security: a plaintext JWKS URL lets a network MITM substitute the JWT
            // *verification* keys and forge accepted tokens. We fetch it anyway
            // (localhost/in-cluster JWKS-over-http is a legitimate setup) but warn
            // loudly — matching the warn-on-security-downgrade posture elsewhere.
            if cfg.auth.jwks_url.to_ascii_lowercase().starts_with("http://") {
                tracing::warn!(
                    target: "auth",
                    jwks_url = %cfg.auth.jwks_url,
                    "JWT_JWKS_URL is plaintext http:// — signing keys fetched over cleartext are MITM-forgeable; use https://"
                );
            }
            match authenticator.fetch_jwks().await {
                Ok(n) => tracing::info!(target: "auth", keys = n, "JWKS pre-fetch OK"),
                Err(e) => tracing::warn!(target: "auth", error = %e, "JWKS pre-fetch failed (will retry on demand)"),
            }
            let a = authenticator.clone();
            let interval = authenticator.jwks_refresh_interval();
            tokio::spawn(async move {
                // A dead JWKS endpoint must not spam one WARN per refresh interval.
                static JWKS_FAIL: obs::Sampler = obs::Sampler::new(60_000);
                loop {
                    tokio::time::sleep(interval).await;
                    if let Err(e) = a.fetch_jwks().await {
                        if let Some(suppressed) = JWKS_FAIL.tick_now() {
                            tracing::warn!(target: "auth", error = %e, suppressed, "JWKS refresh failed");
                        }
                    }
                }
            });
        }
    }

    // LOGGING_PLAN.md Phase 3/4: one consolidated config block instead of banners
    // scattered across boot — the EFFECTIVE value of every knob the broker
    // resolved (secrets masked), so an operator can see what was actually
    // understood rather than inferring it from behaviour.
    config::log_effective(&cfg);

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
    // Track B (§5): the tenancy flag decides how a tenant-less peer frame is resolved.
    // OFF ⇒ it provably names the default tenant (one lookup); ON ⇒ it is ambiguous and
    // fans out to the tenants holding that queue name. The flag lives in the type so
    // the mesh callbacks below never have to branch on it.
    let notifier = notify::Notifier::new(cfg.tenancy_header);

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

    // ACK REGISTRY (server/src/ack_registry.rs): in-memory lease → (worker,
    // batch_end, batch-hash-set) map that turns a full-batch completed ack into
    // a single positional cursor advance, skipping the log_ack_by_hash_v1 hash
    // resolution that caps explicit-ack consume. Miss/evicted/expired → SQL path.
    let ack_registry = Arc::new(ack_registry::AckRegistry::new(
        cfg.ack_registry_mb,
        cfg.ack_registry_enabled,
    ));

    // ACK FUSION (server/src/ack_fusion.rs): coalesces registry-fast-path acks
    // into one queen.log_ack_multi_v1 commit per flush (one fsync for N cursors),
    // fire-on-idle. Default OFF ⇒ inert (no shard tasks), the ack path unchanged.
    let ack_fusion = ack_fusion::AckFusion::new(
        cfg.ack_fusion_shards,
        pool.clone(),
        cfg.stmt_timeout,
        cfg.ack_fusion_hold_ms,
        cfg.ack_fusion_enabled,
    );
    if cfg.ack_fusion_enabled {
        tracing::info!(
            target: "boot",
            shards = cfg.ack_fusion_shards,
            hold_ms = cfg.ack_fusion_hold_ms,
            "QUEEN_ACK_FUSION on"
        );
    }

    // Wildcard candidate hot-list (19-wildcard-hotlist.md). Wakes parked pops
    // through the same notifier the long-poll parks on.
    let hotlist = hotlist::HotList::new(
        cfg.hotlist_enabled,
        cfg.hotlist_shards,
        cfg.hotlist_window_batch,
        // §5: only wire the coalesced dirty-hint set when peers are configured — with
        // no mesh the flusher never runs, so the local mark skips it (no per-push
        // lock/alloc for a hint nobody drains).
        cfg.sync.mesh_active(),
        cfg.tenancy_header,
    );
    hotlist.attach_notifier(notifier.clone());
    if cfg.hotlist_enabled {
        tracing::info!(
            target: "boot",
            shards = cfg.hotlist_shards,
            window_batch = cfg.hotlist_window_batch,
            "QUEEN_HOTLIST on"
        );
    }

    let state = Arc::new(AppState {
        pool: pool.clone(),
        fusion,
        ack_registry,
        ack_fusion,
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
        seeded_groups: std::sync::Mutex::new(std::collections::HashMap::new()),
        hotlist: hotlist.clone(),
        hotlist_reseed_ms: cfg.hotlist_reseed_ms,
        tenancy_enabled: cfg.tenancy_header,
        ownership_ok: std::sync::Mutex::new(std::collections::HashSet::new()),
    });

    // LOGGING_PLAN.md Phase 1: periodic `rates` + `sizes` aggregate blocks —
    // throughput/latency/hit-rate/cache-sizes in the log (sampled ~1 line/10s),
    // alongside Prometheus. Sourced from already-collected process state.
    obs::spawn_reporter(obs::ReporterHandles {
        metrics: state.metrics.clone(),
        pool: state.pool.clone(),
        ack_registry: state.ack_registry.clone(),
        file_buffer: state.file_buffer.clone(),
        hotlist: state.hotlist.clone(),
        dedup: state.fusion.dedup_cache(),
        dedup_cap_mb: cfg.dedup_cache_mb,
        push_vegas: state.push_vegas.clone(),
        pop_vegas: state.pop_vegas.clone(),
        interval_ms: cfg.log_rates_ms,
        top_n: cfg.log_top_n_queues,
    });

    // Hot-list background wheel tick (§6): promote due deferred/leased entries to
    // ready and wake parked pops, even when every consumer is parked. Cheap and
    // idle-safe (a no-op when the flag is off / no rings exist).
    if cfg.hotlist_enabled {
        let hl = hotlist.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(std::time::Duration::from_millis(50));
            loop {
                iv.tick().await;
                hl.tick(crate::util::now_epoch_ms());
            }
        });
    }

    // Hot-list COALESCED local-wake tick (C1). With the flag on, the push path no
    // longer wakes parked pops per-push (that was one notify_waiters — O(parked
    // consumers) — per push, the residual producer-RTT gap on the VM A/B). Instead a
    // push-path mark just flags its queue (one atomic store), and this tick issues
    // ONE notify_waiters per marked queue every ~5ms ⇒ from ~18k wake/s down to
    // ~100-200/s, added discovery latency ≤ one tick. It is a pure latency
    // optimization: a parked hot-list pop re-polls on its own backoff
    // (≤ pop_wait_max_interval_ms) regardless, so a late/stopped tick can never
    // strand a consumer (clean by construction; aborted with the runtime on
    // shutdown like its sibling tasks). Off ⇒ never spawned (byte-identical).
    if cfg.hotlist_enabled {
        let hl = hotlist.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(std::time::Duration::from_millis(5));
            loop {
                iv.tick().await;
                hl.wake_tick();
            }
        });
    }

    // Hot-list PERIODIC reseed floor (§8) — the correctness floor the spec promised.
    // Unlike the pop-path empty-ring reseed (a fast path), this runs even when the
    // ring is NON-empty, staggered per (queue,group) by a fixed jitter so groups that
    // registered together (cold start) never scan in one synchronized storm. It is
    // what recovers a partition erroneously dropped from a BUSY ring (a cross-broker
    // uncommitted-lease false-clear, a stale-config hard-clear, a missed mark / dropped
    // mesh hint) within one interval. The same loop emits the observability line
    // (reseeds/s + per-(queue,group) ring sizes) for the VM A/B. Off ⇒ never spawned.
    if cfg.hotlist_enabled {
        let hl = hotlist.clone();
        let pool_r = pool.clone();
        let interval_ms = cfg.hotlist_reseed_ms;
        let dump_top_n = cfg.log_top_n_queues;
        tokio::spawn(async move {
            use std::sync::atomic::Ordering::Relaxed;
            let mut iv = tokio::time::interval(std::time::Duration::from_millis(2000));
            let mut last_reseeds = 0u64;
            let mut last_report = crate::util::now_epoch_ms();
            loop {
                iv.tick().await;
                let now = crate::util::now_epoch_ms();
                // Reseed every due (queue,group). One pooled client at a time (the
                // floor is background — it must never storm the pool the pops need).
                // Track B (§5): the ring key carries the tenant, so each due ring is
                // reseeded under ITS OWN tenant. The loop enumerates only rings that
                // exist — a tenant with no consumer allocates none — so the idle floor
                // stays O(#rings), never O(tenants × queues).
                for d in hl.periodic_reseed_due(now, interval_ms) {
                    match pool_r.get().await {
                        Ok(client) => {
                            crate::handlers::hotlist_reseed_scan(
                                &hl, &client, &d.qkey, &d.group, now,
                            )
                            .await
                        }
                        Err(_) => break, // pool busy — retry next tick
                    }
                }
                // Observability (~30s cadence): reseeds/s + ring sizes per (queue,group).
                if now - last_report >= 30_000 {
                    let reseeds = hl.reseeds.load(Relaxed);
                    let delta = reseeds.saturating_sub(last_reseeds);
                    let secs = ((now - last_report) as f64 / 1000.0).max(1.0);
                    let mut sizes = hl.ring_sizes();
                    let rings = sizes.len();
                    let ready: usize = sizes.iter().map(|x| x.ready).sum();
                    let wheel: usize = sizes.iter().map(|x| x.wheel).sum();
                    tracing::info!(
                        target: "hotlist",
                        reseeds_delta = delta,
                        per_s = format!("{:.2}", delta as f64 / secs),
                        rings,
                        ready,
                        wheel,
                        marks_local = hl.marks_local.load(Relaxed),
                        marks_remote = hl.marks_remote.load(Relaxed),
                        "reseed floor"
                    );
                    // Rank by (ready+wheel) desc and show only the busiest non-empty
                    // rings — the old `.take(24)` over a HashMap surfaced an arbitrary,
                    // unstable subset (the "a caso" the operator complained about).
                    sizes.sort_by(|a, b| (b.ready + b.wheel).cmp(&(a.ready + a.wheel)));
                    let nonempty = sizes.iter().filter(|x| x.ready + x.wheel > 0).count();
                    for r in sizes.iter().filter(|x| x.ready + x.wheel > 0).take(dump_top_n) {
                        tracing::info!(
                            target: "hotlist",
                            tenant = %r.tenant,
                            queue = %r.queue,
                            group = %r.group,
                            ready = r.ready,
                            wheel = r.wheel,
                            shown = format!("{}/{}", dump_top_n.min(nonempty), nonempty),
                            "ring"
                        );
                    }
                    last_reseeds = reseeds;
                    last_report = now;
                }
            }
        });
    }

    // RUSTFIX item 16: periodic reconcile — re-read the maintenance flags from
    // queen.system_state and drop the per-queue caches, so a replica heals within
    // one interval after a lost UDP packet. Spawned unconditionally (single-node
    // also benefits from DB reconvergence).
    reconcile::spawn(state.clone(), pool.clone(), cfg.sync.cache_refresh_ms);
    // Shared-cell memory bound: drop hot-list rings / wake gates for (tenant, queue)
    // pairs that have gone idle, so an untrusted tenant cannot pin per-name state by
    // polling an unbounded set of queue names.
    reconcile::spawn_idle_sweep(state.clone(), cfg.hotlist_idle_sweep_ms);

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
                // Track B (§5): a frame that CARRIES the tenant wakes exactly that
                // (tenant, queue) gate. A frame WITHOUT one (a pre-Track-B peer mid
                // rolling upgrade) fans out to every tenant holding the name — a safe
                // over-wake; attributing it to the default tenant would silently drop
                // the wake for every other tenant.
                let n = notifier.clone();
                Box::new(move |t: Option<&str>, q: &str, p: &str| match t {
                    Some(t) => n.wake_local_hint(&handlers::tenant_queue_key(t, q), p),
                    None => n.wake_local_hint_all_tenants(q, p),
                })
            },
            on_hotlist_dirty: {
                // 19-wildcard-hotlist §5: a peer's dirty hint marks our hot-list
                // (idempotent, no re-broadcast). mark_remote wakes any parked
                // wildcard pop on a vuoto→pending transition. No-op when the flag
                // is off. Disjoint from MESSAGE_AVAILABLE (a pop wake), so no
                // double-marking. Same tenant-less fan-out contract as above.
                let hl = hotlist.clone();
                Box::new(move |t: Option<&str>, q: &str, p: &str| {
                    let now = crate::util::now_epoch_ms();
                    match t {
                        Some(t) => hl.mark_remote(&handlers::tenant_queue_key(t, q), p, now),
                        None => hl.mark_remote_all_tenants(q, p, now),
                    }
                })
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
                Box::new(move |t: Option<&str>, q: &str| {
                    // Track B (§5): the lease/enc caches are keyed by (tenant, name).
                    // A tenant-carrying frame invalidates exactly one entry; a
                    // tenant-less one (older peer) falls back to dropping every
                    // tenant's entry for the name — over-invalidation only forces a
                    // lazy re-fetch.
                    invalidate_queue_caches(&s, t, q);
                })
            },
            on_queue_config_delete: {
                let s = state.clone();
                Box::new(move |t: Option<&str>, q: &str| {
                    invalidate_queue_caches(&s, t, q);
                })
            },
        };
        match mesh::MeshTransport::bind(&cfg.sync, handlers).await {
            Ok((t, bindings)) => {
                notifier.attach_transport(t.clone());
                t.start(bindings);
                // 19-wildcard-hotlist §5: coalescing dirty-hint flusher. Drains
                // the hot-list's local vuoto→pending transitions every ~20ms and
                // sends ONE batched HOTLIST_DIRTY frame to peers, so a hot
                // partition costs tens of hints/s, not one per push. Only runs
                // with the flag on AND a live mesh.
                if cfg.hotlist_enabled {
                    let hl = hotlist.clone();
                    let tt = t.clone();
                    tokio::spawn(async move {
                        let mut iv =
                            tokio::time::interval(std::time::Duration::from_millis(20));
                        loop {
                            iv.tick().await;
                            let items = hl.drain_dirty(50_000);
                            if !items.is_empty() {
                                tt.send_hotlist_dirty_batch(&items);
                            }
                        }
                    });
                }
            }
            Err(e) => tracing::warn!(
                target: "mesh",
                port = cfg.sync.mesh_port,
                error = %e,
                "TCP mesh bind failed, continuing with local waker only"
            ),
        }
    } else if cfg.sync.enabled {
        tracing::info!(target: "mesh", "mesh sync enabled but no peers configured — local waker only");
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
        // DLQ replay: re-push the dead-letter snapshot, then drop the DLQ row.
        .route(
            "/api/v1/messages/:partitionId/:transactionId/retry",
            post(handlers::handle_retry_message),
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
        // SPA dashboard: any request not matching a route above is served from the
        // assets embedded at compile time (server/webapp/dist — there is no
        // runtime static-dir override), falling back to index.html for
        // client-side routes. A fallback only, so it never shadows /api/v1; it
        // answers a JSON 404 for anything under /api/ and for non-GET/HEAD, so a
        // call to a nonexistent endpoint can never read as a 200.
        .fallback(handlers::handle_static)
        // Raise the request body cap above axum's 2 MiB default so large payloads
        // (pushLargePayload) don't 413. Configurable via QUEEN_MAX_BODY_BYTES.
        .layer(axum::extract::DefaultBodyLimit::max(
            std::env::var("QUEEN_MAX_BODY_BYTES")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(64 * 1024 * 1024),
        ))
        // Track B (PLAN_QUEEN_PROXY_CLOUD.md §5): resolve the per-request tenant
        // (default when off/absent, `x-queen-tenant` when on) and stamp it into
        // request extensions. Inner to auth so a 401 short-circuits before this,
        // but otherwise independent — it only reads a header + stamps an extension.
        .layer(axum::middleware::from_fn_with_state(
            tenant::TenancyConfig { enabled: cfg.tenancy_header },
            tenant::tenant_middleware,
        ))
        // Auth runs outermost: it validates the token + route level before any
        // handler, and stamps AuthedSub into request extensions for producer_sub.
        .layer(axum::middleware::from_fn_with_state(
            authenticator.clone(),
            auth::auth_middleware,
        ))
        .with_state(state);

    if cfg.tenancy_header {
        tracing::info!(target: "boot", header = config::TENANT_HEADER, "QUEEN_TENANCY_HEADER on — native tenant scoping ENABLED");
    }

    let addr = format!("0.0.0.0:{}", cfg.port);
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => obs::fatal(format!("cannot bind {addr}: {e}")),
    };
    tracing::info!(
        target: "boot",
        version = VERSION,
        addr = %addr,
        fusion_shards = cfg.fusion_shards,
        fusion_frames = cfg.fusion_frames,
        hold_ms = cfg.fusion_hold_ms,
        zstd = cfg.zstd_level,
        pool = cfg.pool_size,
        "listening"
    );
    // TCP_NODELAY on every accepted connection (doc 18 §10): the broker's
    // responses are small latency-sensitive JSON frames; Nagle would add up to
    // one delayed-ACK RTT per response. axum 0.7.9's Serve builder exposes this
    // directly (serve.rs sets it right after accept), so no custom accept loop.
    // Graceful shutdown (LOGGING_PLAN.md Phase 4): SIGTERM drains in-flight
    // requests, then we report any undrained spool depth before exit.
    if let Err(e) = axum::serve(listener, app)
        .tcp_nodelay(true)
        .with_graceful_shutdown(obs::shutdown_signal())
        .await
    {
        tracing::error!(target: "boot", error = %e, "serve loop ended with error");
    }
    let pending = file_buffer.pending_count();
    if pending > 0 {
        tracing::warn!(target: "shutdown", pending, "spool has undrained events at shutdown");
    }
    tracing::info!(target: "shutdown", "shutdown complete");
}
