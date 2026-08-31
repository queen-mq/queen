mod ack_fusion;
mod pop_autopilot;
mod pop_fusion;
mod ack_registry;
mod auth;
mod config;
mod db;
// Broker-side dedup cache (doc 18 §5). Storage-free; wired into the push path
// by the fusion slice of the log-engine rewrite.
#[allow(dead_code)]
mod dedup;
mod encryption;
// EPHEMERAL_QUEUES.md §3.2 — the in-RAM queue class. In BOTH crate roots (the
// twin-list rule of lib.rs): the embedded `queen::Broker` is by definition a
// single-broker deployment, which is exactly the topology this phase implements,
// so it gets the engine for free.
mod ephemeral;
mod file_buffer;
mod frames;
mod fusion;
mod handlers;
mod hotlist;
mod httpget;
mod internal;
// EMBEDDED MODE for the Kafka wire facade: `QUEEN_KAFKA_EMBEDDED=true` makes this
// process spawn and supervise the queen-kafka binary as a child. In BOTH crate
// roots (the twin-list rule of lib.rs) because `handlers::status` reads the child's
// state from it — in the library target it is never STARTED (the embedded
// `queen::Broker` has no HTTP listener to point a facade at), so the read is `None`
// and /status renders exactly as it always did.
mod kafka_facade;
// EMBEDDED MODE for the SQS/SNS wire facade (PLAN_QUEEN_SQS.md, Architecture):
// the twin of `kafka_facade` above, spawning and supervising the queen-sqs
// binary under `QUEEN_SQS_EMBEDDED=true`. Listed here rather than in alphabetical
// order because the two are read together. In BOTH crate roots for the same
// reason as its twin, and never STARTED in the library target.
mod sqs_facade;
mod lease;
mod mesh;
mod metrics;
mod migrate;
mod notify;
mod obs;
// EPHEMERAL_QUEUES.md §3.6 — the broker→broker forwarding client. Twin of the
// `mod peerclient;` in lib.rs (the twin-list rule of lib.rs's header).
mod peerclient;
mod pgtls;
mod quota;
mod reconcile;
mod retention;
mod schema;
mod stats;
mod switches;
// KV + timers background sweeper (PLAN_KV_TIMERS.md §7). Registered in three places
// like every other background loop: this list, the twin list in lib.rs, and the spawn
// below. The task is spawned on every broker unless QUEEN_SWEEPER=false.
mod sweeper;
mod syscollect;
mod tenant;
mod util;
mod admission;

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

    // EMBEDDED MODE (kafka_facade.rs) is resolved HERE, before the pool and before
    // the schema apply, because both of its failure modes are unfixable by
    // retrying: a binary that is not there, and the one facade knob that has no
    // default. Boot dies on them the way it dies on unusable JWT key material or a
    // bind address carrying a port — loudly, naming the fix, before doing work.
    let kafka_bin = if cfg.kafka_facade.enabled {
        let bin = kafka_facade::resolve_bin(
            &cfg.kafka_facade.bin,
            std::env::current_exe().ok().as_deref(),
        );
        if let Err(e) = kafka_facade::preflight(&bin, std::env::var("QUEEN_KAFKA_ADVERTISED_ADDR").ok().as_deref()) {
            obs::fatal(e);
        }
        // ...and the posture that is legal but almost certainly a mistake: auth on
        // and no credential for the child. A WARN, not a fatal — the facade is not
        // load-bearing for the broker, and an operator mid-rollout is a real case.
        if let Some(msg) = kafka_facade::auth_advisory(
            cfg.auth.enabled,
            std::env::var("QUEEN_TOKEN").ok().as_deref(),
            std::env::var("QUEEN_KAFKA_SASL").ok().as_deref(),
        ) {
            tracing::warn!(target: "kafka", "{msg}");
        }
        Some(bin)
    } else {
        None
    };

    // EMBEDDED MODE for the SQS facade (sqs_facade.rs), resolved on exactly the
    // same terms and in the same place as its Kafka twin above. Its second
    // unfixable-by-retry case is not an advertised address but the credential
    // pair SigV4 — the default mode — has no default for.
    let sqs_bin = if cfg.sqs_facade.enabled {
        let bin = sqs_facade::resolve_bin(
            &cfg.sqs_facade.bin,
            std::env::current_exe().ok().as_deref(),
        );
        if let Err(e) = sqs_facade::preflight(
            &bin,
            std::env::var("QUEEN_SQS_AUTH").ok().as_deref(),
            std::env::var("QUEEN_SQS_CREDENTIALS").ok().as_deref(),
        ) {
            obs::fatal(e);
        }
        if let Some(msg) = sqs_facade::auth_advisory(
            cfg.auth.enabled,
            std::env::var("QUEEN_TOKEN").ok().as_deref(),
            std::env::var("QUEEN_SQS_CREDENTIALS").ok().as_deref(),
        ) {
            tracing::warn!(target: "sqs", "{msg}");
        }
        Some(bin)
    } else {
        None
    };

    let pool = db::create_pool(&cfg);

    // Apply schema.sql + procedures/*.sql at boot (advisory-locked). Fail fast if
    // the DB can't be brought to the expected shape.
    if let Err(e) = schema::apply(&pool).await {
        obs::fatal(format!("schema apply failed: {e}"));
    }

    // ONE admission arbiter for the whole commit path (admission.rs): every
    // write transaction — push bundles, pop claims, acks, maintenance — takes
    // a lane slot from the same budget, sensed via passive commit trains.
    let admission = admission::Admission::new(admission::AdmissionCfg {
        init: cfg.admission_init,
        min: cfg.admission_min,
        max: cfg.admission_max,
        pool_reserve: cfg.admission_pool_reserve,
        pool_size: cfg.pool_size as u64,
        train_gap: std::time::Duration::from_micros(cfg.admission_train_gap_us),
        tick: std::time::Duration::from_millis(cfg.admission_tick_ms),
        share: [
            cfg.admission_share_push,
            cfg.admission_share_pop,
            cfg.admission_share_ack,
            cfg.admission_share_maint,
        ],
        // The maintenance lane's concurrency is KNOWN, not discovered: it is
        // exactly the retention fan-out width plus the cycle's own slot for
        // phases 4-5. Stating it as a floor is what keeps the adaptive cap from
        // decaying to LANE_MIN_CAP and pinning retention at the serial rate —
        // the lane's transactions are far too long and too few to ever earn a
        // probe (see AdmissionCfg::lane_min_cap).
        lane_min_cap: {
            let mut m = [0.0f64; admission::LANES];
            m[admission::Lane::Maint as usize] = (cfg.retention_parallelism + 1) as f64;
            m
        },
        nosync_budget: cfg.admission_nosync_budget,
        trace: cfg.admission_trace,
    });
    // The long-tail writers (db.rs ack/renew/wire/streams wrappers, the
    // maintenance loops, the spool drain) reach the arbiter through the
    // process-global handle.
    admission::set_global(admission.clone());

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
    } else {
        // Not a warning — this is the documented default (dev / single-tenant
        // self-host) — but it must be SAID: with auth off, the API and the
        // dashboard served at / are both open to anyone who can reach the port.
        tracing::info!(
            target: "auth",
            "JWT auth disabled — API and dashboard are open (standalone); set JWT_ENABLED=true to require tokens"
        );
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

    // Background stats reconciler: recomputes queen.stats from the log tables on a
    // cadence, which is what keeps the dashboard's stats-backed pages from reading
    // zero. Advisory-locked -> one replica refreshes per cycle.
    stats::spawn(pool.clone(), &cfg);

    // Retained-bytes slow lane: the O(segments) heap scan that feeds the proxy
    // storage-quota gauge (queen.stats.retained_bytes), on its own much slower
    // cadence — the counters refresh above no longer writes that column.
    stats::spawn_retained_bytes(pool.clone(), &cfg);

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
        admission.clone(),
        metrics.clone(),
        cfg.zstd_level,
        cfg.fusion_frames,
        cfg.fusion_hold_ms,
        cfg.stmt_timeout,
        cfg.dedup_cache_mb,
        cfg.dedup_cache_enabled,
        // Unreachable Err: config::load() already refused to boot on unusable
        // CA material. Fatal rather than silently plaintext-cancelling.
        db::cancel_connector(&cfg).unwrap_or_else(|e| obs::fatal(e)),
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

    // PLAN_KV_TIMERS §12.1 — the runtime kill switches, seeded from the SAME
    // queen.system_state table and on the same best-effort terms as the two
    // above, with ONE difference that is the whole point of them: an ABSENT row
    // means ON. `get_system_flag_opt` is what makes that expressible; using
    // `get_system_flag` here would boot every fresh cell with the features dead
    // and no row to explain why.
    //
    // `new()` takes nothing: they are born ON, and the config has no say. A boot
    // flag that could start one in the off position would make it a feature gate
    // again, which is exactly what was removed (see the header of switches.rs).
    let switches = switches::Switches::new();
    if let Ok(c) = pool.get().await {
        for key in [
            switches::Switches::KEY_KV,
            switches::Switches::KEY_TIMERS_SCHEDULE,
            switches::Switches::KEY_TIMERS_FIRE,
            // EPHEMERAL_QUEUES.md §3.8 — same mirror, same adopt-on-boot rule:
            // an absent row means ON, so a fresh cell never boots with the
            // surface dead and no row to explain why.
            switches::Switches::KEY_EPHEMERAL,
        ] {
            if let Ok(v) = db::get_system_flag_opt(&c, key).await {
                switches.adopt(key, v);
            }
        }
    }

    // PLAN_KV_TIMERS §9.3 — the occupancy gate, and ONE refresh awaited before
    // the listener opens. With `require_grant` on, an empty snapshot denies, so
    // a cell that started serving before its first refresh would answer
    // `feature_gated` to every tenant for a refresh period on every rollout.
    let quota = quota::from_config(&cfg);
    match quota::refresh_once(&quota, &pool, cfg.kv_max_tenants).await {
        Ok(n) => tracing::info!(target: "quota", tenants = n, "quota seeded"),
        Err(e) => tracing::warn!(
            target: "quota", error = %e,
            "quota seed failed; limits are unknown until the first successful refresh"
        ),
    }

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
    // POP FUSION (server/src/pop_fusion.rs): the third fusion leg the seg port
    // dropped — N pop claims, one transaction, one commit. Vegas admission is
    // taken per FLUSH inside the module. Default OFF (QUEEN_POP_FUSION).
    let pop_fusion = pop_fusion::PopFusion::new(
        cfg.pop_fusion_shards,
        pool.clone(),
        cfg.stmt_timeout,
        admission.clone(),
        metrics.clone(),
        cfg.pop_fusion_hold_ms,
        cfg.pop_fusion_max_jobs,
        cfg.pop_fusion_max_inflight,
        cfg.pop_fusion_enabled,
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
    // Standby-ring bound: see `HotList::trim_unserved` / `hotlist_unserved_trim_ms`.
    hotlist.set_unserved_trim_ms(cfg.hotlist_unserved_trim_ms);
    if cfg.hotlist_enabled {
        tracing::info!(
            target: "boot",
            shards = cfg.hotlist_shards,
            window_batch = cfg.hotlist_window_batch,
            "QUEEN_HOTLIST on"
        );
    }

    // POP AUTOPILOT (server/src/pop_autopilot.rs). Constructed unconditionally —
    // it is a kill switch, not a boot gate (switches.rs's module note), so `off`
    // is a controller that declines rather than a controller that is absent. Costs
    // an empty map.
    let autopilot = pop_autopilot::PopAutopilot::new(cfg.pop_autopilot_knobs());

    // EPHEMERAL_QUEUES.md §3.2 — the RAM-class engine. No pool, no schema read,
    // no mesh: it is ready the instant it is constructed, which is why it is
    // built here and not behind any of the boot awaits above.
    let ephemeral = ephemeral::Ephemeral::new(
        ephemeral::Knobs {
            global_max_bytes: cfg.ephemeral_max_bytes,
            queue_max_bytes: cfg.ephemeral_queue_max_bytes,
            queue_max_length: cfg.ephemeral_queue_max_length,
            lease_ms: cfg.ephemeral_lease_s * 1000,
            retry_limit: cfg.ephemeral_retry_limit,
            implicit_idle_ms: cfg.ephemeral_implicit_idle_s * 1000,
            require_grant: cfg.ephemeral_require_grant,
            rate: cfg.ephemeral_rate,
            burst: cfg.ephemeral_burst,
            // The per-tenant map cap is the KV one, on purpose: it bounds the
            // same thing (in-RAM state keyed by an unvalidated, caller-chosen
            // tenant id) on the same cell, and a second knob would be a second
            // number an operator has to keep equal to the first.
            max_tenants: cfg.kv_max_tenants,
        },
        metrics.clone(),
    );
    // §2 — the cold path, ONE read awaited BEFORE the listener opens, exactly
    // like the KV quota seed above and for the sharper version of the same
    // reason: with `require_grant` on, an empty grant map DENIES, so a cell that
    // started serving first would answer `feature_gated` to every tenant for a
    // refresh period on every rollout. It also vivifies the declared queues, so
    // the first request after a restart already sees them (configured and empty,
    // §1.2) rather than creating them implicitly with the defaults.
    match ephemeral::refresh_once(&ephemeral, &pool, cfg.kv_max_tenants).await {
        Ok((grants, configs)) => tracing::info!(
            target: "ephemeral", grants, configs, "ephemeral grants and declared configs seeded"
        ),
        Err(e) => tracing::warn!(
            target: "ephemeral", error = %e,
            "ephemeral seed failed; declared queues will be re-created implicitly and \
             grants stay unknown until the first successful refresh"
        ),
    }
    // The lease-expiry / ttl backstop rides the sweeper's existing waker rather
    // than adding a second timer task to the process (M10). Injected instead of
    // called directly because `sweeper` exists only in this crate root — the
    // embedded broker has no sweeper, and there the on-touch sweep is the whole
    // mechanism.
    ephemeral.attach_wake_hint(sweeper::hint_in_ms);
    tracing::info!(
        target: "boot",
        // The incarnation id every ephemeral message carries (M4). Logged
        // because it is the ONLY way to read an id in a support ticket: an ack
        // answering `stale` names an epoch, and this line is what says whether
        // that epoch was this process.
        eph_epoch = %format!("{:x}", ephemeral.epoch()),
        max_bytes = cfg.ephemeral_max_bytes,
        "ephemeral engine ready"
    );

    let state = Arc::new(AppState {
        pool: pool.clone(),
        fusion,
        ack_registry,
        ack_fusion,
        pop_fusion,
        admission: admission.clone(),
        metrics: metrics.clone(),
        stmt_timeout: cfg.stmt_timeout,
        pop_default_timeout_ms: cfg.pop_default_timeout_ms,
        default_subscription_mode: cfg.default_subscription_mode.clone(),
        pop_pending_gate: cfg.pop_pending_gate,
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
        quota: quota.clone(),
        switches: switches.clone(),
        kv_pressure: std::sync::atomic::AtomicU32::new(0),
        kv_standalone_shed_after: cfg.kv_standalone_shed_after,
        notifier: notifier.clone(),
        file_buffer: file_buffer.clone(),
        partition_queue: std::sync::Mutex::new(std::collections::HashMap::new()),
        seeded_groups: std::sync::Mutex::new(std::collections::HashMap::new()),
        ephemeral: ephemeral.clone(),
        peers: Arc::new(crate::peerclient::PeerClient::new()),
        hotlist: hotlist.clone(),
        autopilot: autopilot.clone(),
        hotlist_reseed_ms: cfg.hotlist_reseed_ms,
        hotlist_reseed_full_ms: cfg.hotlist_reseed_full_ms,
        hotlist_reseed_window_ms: cfg.hotlist_reseed_window_ms,
        tenancy_enabled: cfg.tenancy_header,
        ownership_ok: std::sync::Mutex::new(std::collections::HashSet::new()),
        auth_enabled: cfg.auth.enabled,
        server_id: cfg.sync.server_id.clone(),
    });

    // LOGGING_PLAN.md Phase 1: periodic `rates` + `sizes` aggregate blocks —
    // throughput/latency/hit-rate/cache-sizes in the log (sampled ~1 line/10s),
    // alongside Prometheus. Sourced from already-collected process state.
    // Pop-lane objective sampler: reads the hotlist ready ring (oldest wait,
    // depth, visit counter) every adapter tick and feeds it to the admission
    // arbiter — the arbiter stays ignorant of where the numbers come from.
    {
        let adm = state.admission.clone();
        let hl = state.hotlist.clone();
        let tick = std::time::Duration::from_millis(cfg.admission_tick_ms);
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(tick);
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                iv.tick().await;
                let (oldest_ms, depth) = hl.ready_probe();
                adm.feed_pop_signal(admission::LaneSignal {
                    oldest_ms,
                    depth,
                    visits: hl.lap.visits(),
                });
            }
        });
    }

    obs::spawn_reporter(obs::ReporterHandles {
        metrics: state.metrics.clone(),
        pool: state.pool.clone(),
        ack_registry: state.ack_registry.clone(),
        file_buffer: state.file_buffer.clone(),
        hotlist: state.hotlist.clone(),
        ephemeral: state.ephemeral.clone(),
        dedup: state.fusion.dedup_cache(),
        dedup_cap_mb: cfg.dedup_cache_mb,
        admission: state.admission.clone(),
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
        let full_ms = cfg.hotlist_reseed_full_ms;
        let window_ms = cfg.hotlist_reseed_window_ms;
        let dump_top_n = cfg.log_top_n_queues;
        tokio::spawn(async move {
            use std::sync::atomic::Ordering::Relaxed;
            let mut iv = tokio::time::interval(std::time::Duration::from_millis(2000));
            let (mut last_full, mut last_window) = (0u64, 0u64);
            let (mut last_failed, mut last_dropped) = (0u64, 0u64);
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
                                &hl, &client, &d.qkey, &d.group, now, full_ms, window_ms,
                            )
                            .await
                        }
                        Err(_) => break, // pool busy — retry next tick
                    }
                }
                // Observability (~30s cadence): reseeds/s + ring sizes per (queue,group).
                //
                // F1 (PLAN_HOTLIST_FOLLOWUP.md): full and windowed passes are reported
                // SEPARATELY. They differ by ~130x in database cost (49ms vs 0.375ms
                // measured in prod on 2026-08-11), so one summed counter answered
                // neither "what is the reseed costing" nor "is this ring windowed or
                // full" — on the night of the deploy that split was only readable from
                // Query Insights, because the two modes are separate SQL functions and
                // the database could distinguish what the broker could not.
                if now - last_report >= 30_000 {
                    let full = hl.reseeds_full.load(Relaxed);
                    let window = hl.reseeds_window.load(Relaxed);
                    let failed = hl.reseeds_failed.load(Relaxed);
                    let dropped = hl.reseeds_dropped.load(Relaxed);
                    let full_delta = full.saturating_sub(last_full);
                    let window_delta = window.saturating_sub(last_window);
                    // `reseeds_delta`/`per_s` keep their old names AND their old
                    // meaning (both modes together) so existing greps and dashboards
                    // do not break on the split.
                    let delta = full_delta + window_delta;
                    let secs = ((now - last_report) as f64 / 1000.0).max(1.0);
                    let mut sizes = hl.ring_sizes(now);
                    let rings = sizes.len();
                    let ready: usize = sizes.iter().map(|x| x.ready).sum();
                    let wheel: usize = sizes.iter().map(|x| x.wheel).sum();
                    // "Repaired recently enough?" — twice the interval the full walk is
                    // actually running at, which with the kill switch (full_ms = 0,
                    // every pass full) is the reseed cadence itself. A ring past it is
                    // either failing its full walk (B2) or has never had one.
                    let overdue_ms = if full_ms > 0 { 2 * full_ms } else { 2 * interval_ms };
                    let stale = |x: &crate::hotlist::RingSize| {
                        x.full_age_ms < 0 || x.full_age_ms > overdue_ms
                    };
                    let full_overdue = sizes.iter().filter(|x| stale(x)).count();
                    tracing::info!(
                        target: "hotlist",
                        reseeds_delta = delta,
                        per_s = format!("{:.2}", delta as f64 / secs),
                        full_delta,
                        window_delta,
                        full_per_s = format!("{:.2}", full_delta as f64 / secs),
                        failed_delta = failed.saturating_sub(last_failed),
                        dropped_delta = dropped.saturating_sub(last_dropped),
                        full_overdue,
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
                    //
                    // A ring OVERDUE for its full walk prints too even when it is empty:
                    // that is exactly the shape of the failure the ages exist to reveal
                    // (a ring pinned to Full by a walk that keeps erroring holds nothing,
                    // so a busiest-first filter would hide it precisely when it matters).
                    sizes.sort_by(|a, b| (b.ready + b.wheel).cmp(&(a.ready + a.wheel)));
                    let show = |x: &&crate::hotlist::RingSize| x.ready + x.wheel > 0 || stale(x);
                    let shown_total = sizes.iter().filter(|x| show(&x)).count();
                    for r in sizes.iter().filter(show).take(dump_top_n) {
                        // The mode this ring's NEXT walk will run — the same predicate
                        // reseed_begin uses, so the operator reads the answer instead of
                        // deriving it from an age and a config value. Approximate only
                        // within the ring's de-phasing offset, right at the boundary.
                        let next = if full_ms <= 0 || r.full_age_ms < 0 || r.full_age_ms >= full_ms
                        {
                            "full"
                        } else {
                            "window"
                        };
                        tracing::info!(
                            target: "hotlist",
                            tenant = %r.tenant,
                            queue = %r.queue,
                            group = %r.group,
                            ready = r.ready,
                            wheel = r.wheel,
                            next,
                            // -1 = never walked in that mode. `full_age_ms` past
                            // `overdue_ms` is the ring to look at.
                            full_age_ms = r.full_age_ms,
                            reseed_age_ms = r.reseed_age_ms,
                            shown = format!("{}/{}", dump_top_n.min(shown_total), shown_total),
                            "ring"
                        );
                    }
                    last_full = full;
                    last_window = window;
                    last_failed = failed;
                    last_dropped = dropped;
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
    reconcile::spawn_unserved_trim(state.clone(), cfg.hotlist_unserved_trim_ms);

    // PLAN_KV_TIMERS §9.3 — the quota/measurement refresh. Its own loop and NOT a
    // phase of reconcile above, for two reasons that both matter: its cadence is
    // 30 s against reconcile's 60 s (the refresh period is half of the published
    // release latency, and tying it to a cache-clear interval would make an
    // operator tuning one silently retune the other), and it must run on brokers
    // that do not sweep — the enforcer is per-process, the rollup is not.
    quota::spawn_refresh(
        state.quota.clone(),
        pool.clone(),
        cfg.kv_quota_refresh_ms,
        cfg.kv_max_tenants,
    );

    // EPHEMERAL_QUEUES.md §2 — the grant/config re-read, on the SAME cadence
    // knob. Its own loop for the same reason the quota refresh is not a phase of
    // reconcile: it must run on brokers that do not sweep, because the enforcer
    // is per-process. It is also the backstop the mesh phase relies on when a
    // reset/delete broadcast is dropped (§10 Q4).
    ephemeral::spawn_refresh(
        ephemeral.clone(),
        pool.clone(),
        cfg.kv_quota_refresh_ms,
        cfg.kv_max_tenants,
    );

    // KV + timers background sweeper (PLAN_KV_TIMERS.md §7). Its own module, not a
    // phase of retention.rs: that cycle is fixed-cadence and leader-gated (session
    // advisory lock 737_001, one replica works per cycle), and folding this in would
    // either serialize it behind retention's phases or make retention leaderless. The
    // sweeper is the opposite on both axes — due-driven and LEADERLESS, so every
    // replica drains in parallel and shares the work through `SKIP LOCKED`. It takes
    // no advisory lock at all, which is also what keeps it out of the lock-order
    // graph (§2.1); 737_003 stays reserved for PLAN_S3_ARCHIVE.md.
    //
    // ALWAYS SPAWNED. It used to be conditional on the two boot flags (§7.1), so that
    // an installation which would never use the features paid nothing for them; with
    // the flags gone there is no such installation — kv and timers are live on every
    // cell that runs this binary, and a surface with expiry and a due time needs its
    // reaper running or it only accumulates. The one remaining knob is QUEEN_SWEEPER,
    // and that decision still lives INSIDE `sweeper::spawn` (it returns `None`) rather
    // than being re-stated here: one rule, one owner, and `embedded/boot.rs` gets the
    // same behaviour from the same line.
    let _sweeper = sweeper::spawn(
        pool.clone(),
        metrics.clone(),
        &cfg,
        switches.clone(),
        quota.clone(),
        // EPHEMERAL_QUEUES.md §3.2 — one more sub-cadence on the SAME loop, not
        // a second background task: the ephemeral backstop is due-driven work
        // measured in microseconds, and a process that already runs a due-driven
        // loop does not need a second one to run it in.
        ephemeral.clone(),
    );
    if !cfg.sweeper_enabled {
        // The configuration that silently accumulates: live surfaces with their only
        // reaper switched off. Expired KV rows are never pruned and timers never fire.
        tracing::warn!(
            target: "boot",
            "QUEEN_SWEEPER=false: expired keys will never be pruned and timers will \
             never fire — they are not lost, they simply wait"
        );
    }

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
                // (idempotent, no re-broadcast). The mark wakes any parked wildcard
                // pop on a vuoto→pending transition. No-op when the flag is off.
                // Disjoint from MESSAGE_AVAILABLE (a pop wake), so no double-marking.
                // Same tenant-less fan-out contract as above, now crossed with the
                // hint's scope: a group-less hint (a push, or any peer predating the
                // field) marks every ring of the queue; a group-carrying one (a seek /
                // group-delete repair) marks that group's ring alone. Runs on the mesh
                // inbound task once per frame ITEM, so all four lanes stay one hash
                // lookup — never a scan of `queues`.
                let hl = hotlist.clone();
                Box::new(move |t: Option<&str>, q: &str, p: &str, g: Option<&str>| {
                    let now = crate::util::now_epoch_ms();
                    match (t, g) {
                        (Some(t), None) => {
                            hl.mark_remote(&handlers::tenant_queue_key(t, q), p, now);
                        }
                        (Some(t), Some(g)) => {
                            hl.mark_remote_group(&handlers::tenant_queue_key(t, q), p, g, now);
                        }
                        (None, None) => hl.mark_remote_all_tenants(q, p, now),
                        (None, Some(g)) => {
                            hl.mark_remote_group_all_tenants(q, p, g, now);
                        }
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
            on_eph_admin: {
                // EPHEMERAL_QUEUES.md §3.5 — a peer's admin verb, applied here.
                // NO RE-BROADCAST: the originating broker fanned the frame out to
                // every peer itself, and a re-broadcast on receipt is how a mesh
                // of three becomes a storm of nine.
                //
                // The tenant arrives already validated as a UUID by `dispatch`
                // (a destructive frame that cannot name its tenant is dropped
                // there), so it is safe to build an engine key from it.
                let eph = ephemeral.clone();
                let pool = pool.clone();
                Box::new(move |op: &str, t: &str, q: &str| match op {
                    // Both are pure RAM and run inline on the inbound task —
                    // they take a lock and a map removal, never a connection.
                    "reset" => {
                        eph.reset(t, q);
                    }
                    "delete" => {
                        eph.remove(t, q);
                    }
                    // …and this one is the exception that must NOT run inline:
                    // it re-reads one declared row, which awaits. Spawned, so a
                    // slow database cannot stall the frame reader — and if the
                    // read fails, the periodic refresh converges anyway (§10 Q4),
                    // which is why the error is logged and dropped.
                    "config_set" => {
                        let eph = eph.clone();
                        let pool = pool.clone();
                        let t = t.to_string();
                        let q = q.to_string();
                        tokio::spawn(async move {
                            if let Err(e) = ephemeral::reload_config(&eph, &pool, &t, &q).await {
                                tracing::debug!(
                                    target: "ephemeral", queue = %q, error = %e,
                                    "peer config_set reload failed; the refresh will converge"
                                );
                            }
                        });
                    }
                    // An op from a newer peer. Skipped for the same reason an
                    // unknown FRAME TYPE is skipped (§11.2): a verb this build
                    // does not implement is not a reason to stop reading.
                    _ => {}
                })
            },
        };
        match mesh::MeshTransport::bind(&cfg.sync, handlers, ephemeral.epoch()).await {
            Ok((t, bindings)) => {
                notifier.attach_transport(t.clone());
                // §3.5/§3.7 — the engine's own handle on the mesh: membership for
                // the rendezvous hash, and the broadcast for the admin verbs.
                // Attached here and not at construction because the transport's
                // inbound handlers capture the state the engine is part of.
                ephemeral.attach_mesh(t.clone());
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

    // ONE CHAIN, no `mut`. The kv and timers routes used to be appended afterwards
    // through a rebinding, because §16 step 1 registered them CONDITIONALLY and a
    // cell with the flag off had to have no such surface at all. The flags are gone
    // (see the header of `switches.rs`), so they are links in this chain like every
    // other route, and there is no longer a code path in which the router is built
    // one way on one cell and another way on another.
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
        // PLAN_QUEEN_KAFKA.md C2 — batched read-from-offset with long poll. It
        // sits beside the pop routes because it is the other way to consume,
        // and it is POST because the request is a batch of up to 1024
        // (queue, partition, offset) triples that does not fit a query string.
        // Nothing about it is destructive: no lease, no cursor, no claim.
        .route("/api/v1/fetch", post(handlers::handle_fetch))
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
        // Minimal depth read for relay/scheduler pollers: (partition, pending)
        // via watermark arithmetic only — the cheap sibling of the console-grade
        // :queue detail above.
        .route(
            "/api/v1/resources/queues/:queue/depth",
            get(handlers::handle_queue_depth),
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
        // PLAN_KV_TIMERS §12.1 rungs 7 and 8. MANAGEMENT plane, so unlike the
        // data-path routes below it is registered UNCONDITIONALLY: an operator
        // must be able to read that a cell has the features off, and a route that
        // appeared only when the feature was on would answer 404 for both "not
        // built with it" and "no such endpoint".
        .route(
            "/api/v1/system/kv-timers",
            get(handlers::handle_get_kv_timers).post(handlers::handle_set_kv_timers),
        )
        // EPHEMERAL_QUEUES.md §3.8 — the RAM class's runtime kill switch, on the
        // same management plane and registered on the same terms: an operator
        // must be able to READ that the surface is paused, so this route exists
        // whether or not it is.
        .route(
            "/api/v1/system/ephemeral",
            get(handlers::handle_get_ephemeral).post(handlers::handle_set_ephemeral),
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
        // ------------------------------------- broker-direct dashboard identity
        // The SPA boots from /auth/me wherever it is served. Under the proxy
        // these paths never reach the broker; served broker-direct the broker
        // answers them itself (standalone identity with auth off, a terminal
        // explanation page with auth on). See handlers/standalone.rs.
        .route("/auth/me", get(handlers::handle_auth_me))
        .route("/auth/login", get(handlers::handle_auth_login))
        .route("/auth/logout", post(handlers::handle_auth_logout))
    // ------------------------------------------------------ kv (PLAN_KV_TIMERS.md §8.1)
    //
    // Data path, not management plane.
    //
    // Order matters: matchit keeps the static `/api/v1/kv` distinct from the deeper
    // parametric paths, the same rule this file already follows four times over.
    // `*key` is a CATCH-ALL so `order/9f1/items` can be written naturally, and there
    // is deliberately NO literal segment under `/api/v1/kv/:ns/` — one would make
    // every key named after it unreachable, which is also why `incr` exists only on
    // the POST batch.
    //
    // One rule for status codes: the status describes the outcome of the CALL, not of
    // the business predicate. An absent key, a lost race and a delete that matched
    // nothing are all 200 with an explicit field. The cost is stated (curl does not
    // behave "RESTfully" on a missing key, and whoever scripts against it must read
    // the body); the benefit is that no SDK, proxy, dashboard or retry policy treats
    // the product's most frequent outcome as an error. The house precedent is queue
    // deletion, which keeps 200 on `deleted:false`.
    //
    // No flag to read: /api/v1/kv is on every cell that runs this binary, the same
    // way /api/v1/push is. A registration behind a boot flag was the last thing that
    // made "does this cell have KV?" a question a client could have to ask, and the
    // answer is now always yes.
        .route("/api/v1/kv", post(handlers::handle_kv_batch))
        .route(
            "/api/v1/kv/:ns/*key",
            get(handlers::handle_kv_get)
                .put(handlers::handle_kv_put)
                .delete(handlers::handle_kv_delete),
        )
    // --------------------------------------------------- timers (PLAN_KV_TIMERS.md §8.1)
    //
    // Note what is NOT here: a tenant-wide timer list. `list` is scoped to a queue and
    // the queue is a PATH SEGMENT, not a filter, precisely so that no call can ask for
    // "every timer of this tenant" — that is a scan an end user of the customer could
    // trigger (§4.1).
    //
    // And the cancel has its OWN route and its own authorization class (§9.6). It is
    // the same stored procedure as schedule but NOT the same authorization decision:
    // `POST /api/v1/timers` carries cancels in the same array as schedules, so a
    // tenant over quota would be refused on its cancels too — while the fire never
    // stops on its own (§12), meaning that tenant would keep producing messages it
    // cannot stop, up to the horizon or an operator's intervention. Blocking would
    // produce the opposite of its purpose, so cancel goes through a route the proxy
    // classifies as read/management and never blocks.
    // ------------------------------------- ephemeral (EPHEMERAL_QUEUES.md §3.1)
    //
    // The RAM-class surface: contents live in this process's heap and survive
    // nothing (§1.2). Eight routes — three hot verbs, three management verbs and
    // two status reads.
    //
    // ORDER MATTERS HERE, and this is the sixth time this file says it: matchit
    // needs the STATIC segments registered before their `:param` siblings.
    // `/queues` and `/queues/:queue/depth` are distinct paths so they cannot
    // collide, but `/queue/:queue` (the delete) sits one segment deep under a
    // family whose other members are all static, and registering it first would
    // make `/api/v1/ephemeral/queue/...` shadow nothing today and something
    // tomorrow. Static first, param last, always.
    //
    // Unconditional, like kv and timers above and for the same reason: no flag
    // decides whether this surface exists, so a 404 here can only ever mean the
    // broker predates the feature (which is exactly what the SDKs map it to).
    // Pausing it is the runtime kill switch at /api/v1/system/ephemeral above.
        .route("/api/v1/ephemeral/push", post(handlers::handle_ephemeral_push))
        .route("/api/v1/ephemeral/pop", get(handlers::handle_ephemeral_pop))
        .route("/api/v1/ephemeral/ack", post(handlers::handle_ephemeral_ack))
        .route(
            "/api/v1/ephemeral/configure",
            post(handlers::handle_ephemeral_configure),
        )
        .route("/api/v1/ephemeral/reset", post(handlers::handle_ephemeral_reset))
        .route("/api/v1/ephemeral/queues", get(handlers::handle_ephemeral_queues))
        .route(
            "/api/v1/ephemeral/queues/:queue/depth",
            get(handlers::handle_ephemeral_depth),
        )
        .route(
            "/api/v1/ephemeral/queue/:queue",
            axum::routing::delete(handlers::handle_ephemeral_delete_queue),
        )
        .route("/api/v1/timers", post(handlers::handle_timers_batch))
        .route("/api/v1/timers/:queue", get(handlers::handle_timers_list))
        .route(
            "/api/v1/timers/:queue/*timerKey",
            get(handlers::handle_timer_peek).delete(handlers::handle_timer_cancel),
        );

    let app = app
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

    let addr = config::host_port(&cfg.bind_addr, &cfg.port);
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

    // EMBEDDED MODE (kafka_facade.rs). Spawned HERE and not earlier: the URL the
    // child is handed is derived from the address the listener ACTUALLY bound
    // (`local_addr`, so PORT=0 and a wildcard bind both come out dialable), and the
    // socket is already in the accept backlog by the time the child can reach it.
    // `local_addr` cannot realistically fail on a bound listener; the configured
    // pair is the fallback so a spawn is never skipped over a diagnostic call.
    let kafka = kafka_bin.map(|bin| {
        let url = match listener.local_addr() {
            Ok(a) => kafka_facade::loopback_url(&a),
            Err(e) => {
                tracing::warn!(target: "kafka", error = %e, "listener local_addr failed; using the configured address for QUEEN_URL");
                format!("http://{}", config::host_port(&cfg.bind_addr, &cfg.port))
            }
        };
        kafka_facade::spawn(&cfg.kafka_facade, bin, url)
    });
    // The SQS facade (sqs_facade.rs), spawned HERE for the same reason and from
    // the same bound address. The two are independent: either, both or neither.
    let sqs = sqs_bin.map(|bin| {
        let url = match listener.local_addr() {
            Ok(a) => sqs_facade::loopback_url(&a),
            Err(e) => {
                tracing::warn!(target: "sqs", error = %e, "listener local_addr failed; using the configured address for QUEEN_URL");
                format!("http://{}", config::host_port(&cfg.bind_addr, &cfg.port))
            }
        };
        sqs_facade::spawn(&cfg.sqs_facade, bin, url)
    });
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
    // The child goes down with the broker, and this AWAITS it: an exit that only
    // sent the signal would race the process teardown and leave the Kafka port held
    // by a facade nobody is supervising. Bounded inside `shutdown`.
    if let Some(k) = kafka {
        k.shutdown().await;
    }
    // ...and the SQS facade, on the same terms. Sequential and not joined: each
    // wait is bounded by its own grace window, and a broker that is exiting has
    // nothing better to do with the second one.
    if let Some(s) = sqs {
        s.shutdown().await;
    }
    let pending = file_buffer.pending_count();
    if pending > 0 {
        tracing::warn!(target: "shutdown", pending, "spool has undrained events at shutdown");
    }
    tracing::info!(target: "shutdown", "shutdown complete");
}
