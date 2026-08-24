//! Embedded composition root.
//!
//! Mirrors the binary's boot sequence in `src/main.rs` (which stays the single
//! source of truth for the HTTP server) with three deliberate differences:
//!
//! * no HTTP listener, no auth/tenancy middleware, no JWKS refresh — the
//!   embedding application is in-process and already trusted;
//! * no inter-instance mesh: peers are a static host:port list that does not
//!   fit an application fleet, and every cross-instance channel has a periodic
//!   DB-truth floor (reconcile / hotlist reseed / pop backoff), so N embedded
//!   instances over one Postgres stay correct, each channel at the cost of its
//!   own floor rather than of a frame. The floors are NOT uniformly small and
//!   the module doc in `embedded/mod.rs` states each one: since the windowed
//!   reseed (1.0.1) a cursor moved backwards on another instance is repaired
//!   by the durable marker this file polls below, not by the reseed;
//! * boot failures return [`StartError`] instead of exiting the process, and
//!   the loops spawned HERE keep their `JoinHandle`s so `shutdown()` can abort
//!   them. (Loops spawned inside the engine constructors — fusion shards, the
//!   admission adapter, retention/stats/syscollect, the spool drain — do not
//!   expose handles today and live until the process exits; see the module doc
//!   in `embedded/mod.rs`.)
//!
//! KEEP IN SYNC: if `main.rs` gains or reorders a boot step, this file must
//! follow. The constructor arguments below are copied verbatim from main.rs so
//! an embedded broker is the same broker. Deliberately dropped main.rs steps,
//! beyond the three differences above: `config::log_effective` (a 30-line
//! effective-config banner; the host owns its logs, and the periodic reporter
//! already carries the embedded observability) and `cfg.auth.validate()` +
//! the auth boot lines (no auth middleware exists embedded — JWT env is
//! ignored, with a warning below). The reconcile and idle-sweep loops are
//! inlined from `reconcile.rs` instead of calling its spawn helpers, because
//! those return no `JoinHandle` and `shutdown()` must be able to abort loops
//! that capture the full `AppState`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::{BrokerConfig, StartError};
use crate::handlers::AppState;
use crate::{
    ack_fusion, ack_registry, admission, config, db, encryption, file_buffer, fusion, hotlist,
    metrics, notify, obs, pop_fusion, quota, retention, schema, stats, switches, syscollect,
};

/// Every boolean env knob the boot path reads (config.rs `env_bool` call sites
/// plus the EXTERNAL_BOOL_KEYS pre-pass and schema.rs's QUEEN_APPLY_SCHEMA).
/// The binary exits the process on an unparseable value (`obs::fatal`); a
/// library must not, so boot() pre-validates the same keys through
/// `config::env_bool_checked` and returns `StartError::Config` BEFORE
/// `config::load()` can reach its fatal path.
/// KEEP IN SYNC with the `env_bool` call sites in config.rs.
const BOOT_BOOL_KEYS: &[&str] = &[
    "QUEEN_LOG_JSON",
    "QUEEN_APPLY_SCHEMA",
    "QUEEN_V2_BUNDLE_LOG",
    "PG_USE_SSL",
    "PG_SSL_REJECT_UNAUTHORIZED",
    "JWT_ENABLED",
    "QUEEN_DEDUP_CACHE",
    "QUEEN_ACK_REGISTRY",
    "QUEEN_ACK_FUSION",
    "QUEEN_POP_FUSION",
    "QUEEN_ADMISSION_TRACE",
    "QUEEN_HOTLIST",
    "QUEEN_PARTITION_CLEANUP_ENABLED",
    "QUEEN_SYNC_ENABLED",
    "QUEEN_TENANCY_HEADER",
];

/// Distinguishes the spool dirs of two Brokers started by one process; the
/// random token additionally guards against a LATER process reusing the same
/// pid and silently draining a dead instance's leftover temp spool.
static SPOOL_SEQ: AtomicU64 = AtomicU64::new(0);

fn next_spool_seq() -> u64 {
    SPOOL_SEQ.fetch_add(1, Ordering::Relaxed)
}

fn unique_spool_dir(seq: u64) -> std::path::PathBuf {
    let token: u64 = rand::random();
    std::env::temp_dir().join(format!(
        "queen-embedded-{}-{token:016x}-{seq}",
        std::process::id()
    ))
}

pub(super) struct Booted {
    pub st: Arc<AppState>,
    pub tasks: Vec<tokio::task::JoinHandle<()>>,
    pub auto_spool_dir: Option<std::path::PathBuf>,
}

pub(super) async fn boot(bc: &BrokerConfig) -> Result<Booted, StartError> {
    // Refuse malformed boolean env BEFORE config::load(), whose own reads
    // would exit the host process (env_bool -> obs::fatal). Numeric knobs
    // silently default in config::load, so booleans are the only exit path.
    for k in BOOT_BOOL_KEYS {
        config::env_bool_checked(k, false).map_err(StartError::Config)?;
    }

    // Same env-driven defaults as the binary (QUEEN_* tuning knobs keep
    // working), with the BrokerConfig fields winning over env where set.
    let mut cfg = config::load();

    // Env knobs that only make sense with the HTTP surface are ignored
    // embedded — say so instead of silently dropping them.
    if cfg.tenancy_header {
        tracing::warn!(
            target: "boot",
            "QUEEN_TENANCY_HEADER is ignored embedded — the embedded broker is single-tenant by construction"
        );
    }
    if cfg.auth.enabled {
        tracing::warn!(
            target: "boot",
            "JWT_ENABLED is ignored embedded — there is no HTTP surface; the in-process caller is trusted"
        );
    }

    // Embedded is single-tenant by construction: every call is stamped with the
    // default tenant, exactly like the OSS HTTP path with the flag off.
    cfg.tenancy_header = false;

    if let Some(h) = &bc.pg_host {
        cfg.pg.host = Some(h.clone());
    }
    if let Some(p) = bc.pg_port {
        cfg.pg.port = Some(p);
    }
    if let Some(u) = &bc.pg_user {
        cfg.pg.user = Some(u.clone());
    }
    if let Some(pw) = &bc.pg_password {
        cfg.pg.password = Some(pw.clone());
    }
    if let Some(name) = &bc.pg_database {
        cfg.pg.dbname = Some(name.clone());
    }
    if let Some(on) = bc.pg_use_ssl {
        cfg.pg_use_ssl = on;
    }
    if let Some(on) = bc.pg_ssl_reject_unauthorized {
        cfg.pg_ssl_reject_unauthorized = on;
    }
    if let Some(ms) = bc.stmt_timeout_ms {
        cfg.stmt_timeout = std::time::Duration::from_millis(ms);
    }
    if let Some(ps) = bc.pool_size {
        cfg.pool_size = ps;
        // config::load derives the admission floor from the pool size it read
        // from env; re-derive it for the overridden pool so the budget can
        // never exceed the connections that exist (admission.rs ceiling
        // contract). Explicit QUEEN_ADMISSION_* env still wins — gated on
        // env_int's parse semantics (a set-but-unparseable value counts as
        // absent there, so it must count as absent here too).
        let floor = (ps as u64)
            .saturating_sub(cfg.admission_pool_reserve)
            .saturating_mul(2)
            / 3;
        let floor = floor.max(8);
        let env_set = |k: &str| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.trim().parse::<i64>().ok())
                .is_some()
        };
        if !env_set("QUEEN_ADMISSION_INIT") {
            cfg.admission_init = floor;
        }
        if !env_set("QUEEN_ADMISSION_MIN") {
            cfg.admission_min = floor;
        }
        cfg.admission_max = cfg.admission_max.max(cfg.admission_min);
    }

    // Spool dir policy: an explicit `spool_dir` wins; an explicit
    // FILE_BUFFER_DIR env is an operator's deliberate choice and is honoured —
    // but only for the FIRST in-process instance: spool files carry no
    // instance token, so a second Broker writing the same dir would interleave
    // frames into the first one's files. Later instances get a private subdir.
    // With neither set, a per-instance temp dir (random-tokened, removed on
    // clean shutdown); the server default (/var/lib/queen/buffers) is NOT used
    // implicitly, because two embedded apps on one host sharing a spool dir
    // corrupt each other's files.
    let instance_seq = next_spool_seq();
    let mut auto_spool_dir = None;
    match (&bc.spool_dir, std::env::var("FILE_BUFFER_DIR")) {
        (Some(dir), _) => cfg.file_buffer.dir = dir.display().to_string(),
        (None, Ok(d)) if !d.is_empty() => {
            if instance_seq > 0 {
                let sub = std::path::Path::new(&d)
                    .join(format!("instance-{}-{instance_seq}", std::process::id()));
                tracing::warn!(
                    target: "boot",
                    dir = %sub.display(),
                    "second embedded Broker in this process: using a private subdir of FILE_BUFFER_DIR (spool dirs must never be shared)"
                );
                cfg.file_buffer.dir = sub.display().to_string();
            }
            // else: config::load already picked the env dir up.
        }
        (None, _) => {
            let dir = unique_spool_dir(instance_seq);
            cfg.file_buffer.dir = dir.display().to_string();
            auto_spool_dir = Some(dir);
        }
    }

    // Pool, with the same TLS split as db::create_pool but Err instead of panic.
    let mut pg = cfg.pg.clone();
    pg.pool = Some(deadpool_postgres::PoolConfig::new(cfg.pool_size));
    let pool = if cfg.pg_use_ssl {
        pg.ssl_mode = Some(deadpool_postgres::SslMode::Require);
        let connector = crate::pgtls::make_connector(cfg.pg_ssl_reject_unauthorized);
        pg.create_pool(Some(deadpool_postgres::Runtime::Tokio1), connector)
            .map_err(|e| StartError::Pool(e.to_string()))?
    } else {
        pg.create_pool(Some(deadpool_postgres::Runtime::Tokio1), tokio_postgres::NoTls)
            .map_err(|e| StartError::Pool(e.to_string()))?
    };

    // Fail fast with a clear error if Postgres is unreachable — a library must
    // not let the first push discover a bad connection string.
    {
        let client = pool
            .get()
            .await
            .map_err(|e| StartError::Connect(e.to_string()))?;
        client
            .simple_query("SELECT 1")
            .await
            .map_err(|e| StartError::Connect(e.to_string()))?;
    }

    if bc.apply_schema {
        schema::apply(&pool)
            .await
            .map_err(|e| StartError::Schema(e.to_string()))?;
    }

    // From here on the sequence and every constructor argument mirror main.rs.
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
        // Same reasoning as main.rs: the maintenance lane's concurrency is the
        // retention fan-out width plus the cycle's own slot, and it must not
        // decay below it (AdmissionCfg::lane_min_cap).
        lane_min_cap: {
            let mut m = [0.0f64; admission::LANES];
            m[admission::Lane::Maint as usize] = (cfg.retention_parallelism + 1) as f64;
            m
        },
        nosync_budget: cfg.admission_nosync_budget,
        trace: cfg.admission_trace,
    });
    // Process-global by design (first set wins): a second Broker in the same
    // process shares the first one's arbiter for the long-tail writers. See
    // the one-Broker-per-process note in the module docs.
    admission::set_global(admission.clone());

    let metrics = Arc::new(metrics::Metrics::new());

    if bc.retention {
        retention::spawn(pool.clone(), &cfg);
    }
    if bc.stats_refresh {
        stats::spawn(pool.clone(), &cfg);
        // Retained-bytes slow lane rides the same flag: it is the other half of
        // the queen.stats refresh (011 no longer writes retained_bytes), and an
        // embedded broker that opts out of stats has no reader for the gauge.
        stats::spawn_retained_bytes(pool.clone(), &cfg);
    }
    if bc.system_metrics {
        syscollect::spawn(pool.clone(), metrics.clone(), &cfg);
    }
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
        cfg.pg_use_ssl,
        cfg.pg_ssl_reject_unauthorized,
    );

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
    let switches = switches::Switches::new();
    if let Ok(c) = pool.get().await {
        for key in [
            switches::Switches::KEY_KV,
            switches::Switches::KEY_TIMERS_SCHEDULE,
            switches::Switches::KEY_TIMERS_FIRE,
            // EPHEMERAL_QUEUES.md §3.8 (KEEP IN SYNC with main.rs).
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

    let notifier = notify::Notifier::new(cfg.tenancy_header);
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

    let ack_registry = Arc::new(ack_registry::AckRegistry::new(
        cfg.ack_registry_mb,
        cfg.ack_registry_enabled,
    ));
    let ack_fusion = ack_fusion::AckFusion::new(
        cfg.ack_fusion_shards,
        pool.clone(),
        cfg.stmt_timeout,
        cfg.ack_fusion_hold_ms,
        cfg.ack_fusion_enabled,
    );
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

    // mesh_active is hard-false embedded (no peers, no dirty-hint flusher).
    let hotlist = hotlist::HotList::new(
        cfg.hotlist_enabled,
        cfg.hotlist_shards,
        cfg.hotlist_window_batch,
        false,
        cfg.tenancy_header,
    );
    hotlist.attach_notifier(notifier.clone());
    // KEEP IN SYNC with main.rs. An embedded broker has no HA peer, so it cannot be the
    // standby of a pair — but it can still hold a queue nobody pops (a write-only stretch,
    // a consumer that stopped), which is the same accumulation.
    hotlist.set_unserved_trim_ms(cfg.hotlist_unserved_trim_ms);

    // EPHEMERAL_QUEUES.md §3.2 — KEEP IN SYNC with main.rs (the embedded-API
    // memory rule). The embedded broker is by definition single-broker, which is
    // exactly the ownership topology the engine implements, so nothing about it
    // is degraded here. The one difference is deliberate and stated: there is no
    // sweeper embedded, so no `attach_wake_hint` — the opportunistic expiry
    // sweep on every ring touch is the whole mechanism, and it is the
    // correctness floor on the HTTP broker too.
    let ephemeral = crate::ephemeral::Ephemeral::new(
        crate::ephemeral::Knobs {
            global_max_bytes: cfg.ephemeral_max_bytes,
            queue_max_bytes: cfg.ephemeral_queue_max_bytes,
            queue_max_length: cfg.ephemeral_queue_max_length,
            lease_ms: cfg.ephemeral_lease_s * 1000,
            retry_limit: cfg.ephemeral_retry_limit,
            implicit_idle_ms: cfg.ephemeral_implicit_idle_s * 1000,
            require_grant: cfg.ephemeral_require_grant,
            rate: cfg.ephemeral_rate,
            burst: cfg.ephemeral_burst,
            max_tenants: cfg.kv_max_tenants,
        },
        metrics.clone(),
    );
    // §2 — the grants and the declared configs, ONE read awaited before the
    // facade is handed back (KEEP IN SYNC with main.rs, where the same read is
    // awaited before the listener opens). The embedded broker has no listener,
    // so "before serving" is "before `Broker::start` returns" — and the property
    // being kept is the same one: the first operation must already see the
    // declared queues and the grant map, or a declared queue is silently
    // re-created implicitly with the broker defaults.
    match crate::ephemeral::refresh_once(&ephemeral, &pool, cfg.kv_max_tenants).await {
        Ok((grants, configs)) => tracing::info!(
            target: "ephemeral", grants, configs, "ephemeral grants and declared configs seeded"
        ),
        Err(e) => tracing::warn!(
            target: "ephemeral", error = %e,
            "ephemeral seed failed; declared queues will be re-created implicitly and \
             grants stay unknown until the first successful refresh"
        ),
    }

    let st = Arc::new(AppState {
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
        // POP AUTOPILOT (server/src/pop_autopilot.rs) — same construction as
        // main.rs (KEEP IN SYNC): built unconditionally because it is a kill
        // switch and not a boot gate, and it costs an empty map when off.
        autopilot: crate::pop_autopilot::PopAutopilot::new(cfg.pop_autopilot_knobs()),
        hotlist_reseed_ms: cfg.hotlist_reseed_ms,
        hotlist_reseed_full_ms: cfg.hotlist_reseed_full_ms,
        hotlist_reseed_window_ms: cfg.hotlist_reseed_window_ms,
        tenancy_enabled: cfg.tenancy_header,
        ownership_ok: std::sync::Mutex::new(std::collections::HashSet::new()),
        auth_enabled: cfg.auth.enabled,
        server_id: cfg.sync.server_id.clone(),
    });

    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Pop-lane objective sampler (main.rs "rates" block): feeds the hotlist
    // ready-ring probe to the admission arbiter every adapter tick.
    {
        let adm = st.admission.clone();
        let hl = st.hotlist.clone();
        let tick = std::time::Duration::from_millis(cfg.admission_tick_ms);
        tasks.push(tokio::spawn(async move {
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
        }));
    }

    if bc.log_reports {
        obs::spawn_reporter(obs::ReporterHandles {
            metrics: st.metrics.clone(),
            pool: st.pool.clone(),
            ack_registry: st.ack_registry.clone(),
            file_buffer: st.file_buffer.clone(),
            hotlist: st.hotlist.clone(),
            ephemeral: st.ephemeral.clone(),
            dedup: st.fusion.dedup_cache(),
            dedup_cap_mb: cfg.dedup_cache_mb,
            admission: st.admission.clone(),
            interval_ms: cfg.log_rates_ms,
            top_n: cfg.log_top_n_queues,
        });
    }

    // Hot-list wheel tick: promote due deferred/leased entries and wake parked
    // pops even when every consumer is parked.
    if cfg.hotlist_enabled {
        let hl = hotlist.clone();
        tasks.push(tokio::spawn(async move {
            let mut iv = tokio::time::interval(std::time::Duration::from_millis(50));
            loop {
                iv.tick().await;
                hl.tick(crate::util::now_epoch_ms());
            }
        }));
    }

    // Hot-list coalesced local-wake tick (pure latency optimization — a parked
    // pop re-polls on its own backoff regardless).
    if cfg.hotlist_enabled {
        let hl = hotlist.clone();
        tasks.push(tokio::spawn(async move {
            let mut iv = tokio::time::interval(std::time::Duration::from_millis(5));
            loop {
                iv.tick().await;
                hl.wake_tick();
            }
        }));
    }

    // Hot-list periodic reseed floor — the wildcard-pop correctness floor:
    // recovers a partition erroneously dropped from a busy ring within one
    // interval. Same loop as main.rs, minus the 30s observability dump (the
    // reporter above already logs hotlist aggregates for the embedded case).
    if cfg.hotlist_enabled {
        let hl = hotlist.clone();
        let pool_r = pool.clone();
        let interval_ms = cfg.hotlist_reseed_ms;
        // Embedded HA has no mesh (embedded/mod.rs): cross-instance discovery of a
        // PUSH is this floor and nothing else. The windowed pass carries that job — a
        // peer's push is a recent write by definition — while the full walk stays the
        // repair path underneath it. What the windowed pass cannot see because no write
        // explains it is NOT this loop's to find: a cursor moved backwards elsewhere
        // reaches this instance through the repair markers the reconcile loop polls
        // below, which is the closest embedded gets to a mesh hint.
        let full_ms = cfg.hotlist_reseed_full_ms;
        let window_ms = cfg.hotlist_reseed_window_ms;
        tasks.push(tokio::spawn(async move {
            let mut iv = tokio::time::interval(std::time::Duration::from_millis(2000));
            loop {
                iv.tick().await;
                let now = crate::util::now_epoch_ms();
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
            }
        }));
    }

    // Reconcile: re-read maintenance flags from queen.system_state and drop the
    // per-queue caches so an embedded instance converges with external brokers
    // sharing the same database within one interval. Inlined from
    // reconcile::spawn (KEEP IN SYNC) because that helper returns no handle and
    // this loop captures the full AppState — shutdown() must abort it.
    {
        let state = st.clone();
        let pool_r = pool.clone();
        let interval =
            std::time::Duration::from_millis(cfg.sync.cache_refresh_ms.max(1));
        tasks.push(tokio::spawn(async move {
            use std::sync::atomic::Ordering::Relaxed;
            // Small initial delay so boot-time seeding settles first.
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            // A3: see reconcile::apply_hotlist_repairs. This matters MORE here than in
            // the broker: an embedded instance has no mesh, so the durable marker is its
            // only cross-instance notice that another instance moved a cursor — without
            // it the sole repair is the 300s full walk.
            let mut repaired = crate::reconcile::AppliedRepairs::default();
            loop {
                if let Ok(c) = pool_r.get().await {
                    if let Ok(m) = db::get_system_flag(&c, "maintenance_mode").await {
                        let prev = state.maintenance.swap(m, Relaxed);
                        if prev != m {
                            tracing::info!(target: "reconcile", flag = "maintenance_mode", from = %prev, to = %m, "flag changed");
                            if m {
                                state.file_buffer.pause_background_drain();
                            } else {
                                state.file_buffer.force_finalize_all();
                                state.file_buffer.resume_background_drain();
                            }
                        }
                    }
                    if let Ok(pm) = db::get_system_flag(&c, "pop_maintenance_mode").await {
                        let prev = state.pop_maintenance.swap(pm, Relaxed);
                        if prev != pm {
                            tracing::info!(target: "reconcile", flag = "pop_maintenance_mode", from = %prev, to = %pm, "flag changed");
                        }
                    }
                    // PLAN_KV_TIMERS §12.1 kill switches (KEEP IN SYNC with
                    // reconcile::spawn). `get_system_flag_opt`, because an absent
                    // row means ON for these three.
                    for key in [
                        crate::switches::Switches::KEY_KV,
                        crate::switches::Switches::KEY_TIMERS_SCHEDULE,
                        crate::switches::Switches::KEY_TIMERS_FIRE,
                        crate::switches::Switches::KEY_EPHEMERAL,
                    ] {
                        if let Ok(v) = db::get_system_flag_opt(&c, key).await {
                            state.switches.adopt(key, v);
                        }
                    }
                    crate::reconcile::apply_hotlist_repairs(&state, &c, &mut repaired)
                        .await;
                }
                state.lease_cache.lock().unwrap().clear();
                state.enc_cache.lock().unwrap().clear();
                state.seeded_groups.lock().unwrap().clear();
                tokio::time::sleep(interval).await;
            }
        }));
    }

    // PLAN_KV_TIMERS §9.3 — the quota/measurement refresh. Not inlined: unlike
    // the reconcile loop it captures no AppState, so the shared helper is usable
    // and the handle it returns is what shutdown() aborts.
    tasks.push(quota::spawn_refresh(
        st.quota.clone(),
        pool.clone(),
        cfg.kv_quota_refresh_ms,
        cfg.kv_max_tenants,
    ));

    // EPHEMERAL_QUEUES.md §2 — the grant/config re-read, same cadence knob and
    // same handle discipline (KEEP IN SYNC with main.rs).
    tasks.push(crate::ephemeral::spawn_refresh(
        st.ephemeral.clone(),
        pool.clone(),
        cfg.kv_quota_refresh_ms,
        cfg.kv_max_tenants,
    ));

    // EPHEMERAL_QUEUES.md §3.2 — the RAM-class backstop. On the HTTP broker this
    // is one sub-cadence of `sweeper::run_loop` (phase C-eph, 1 s gate); there is
    // no sweeper here, so it is its own tiny interval task at the same cadence.
    //
    // IT IS NOT OPTIONAL, and that is worth stating because the rest of the
    // sweeper genuinely is absent embedded. Lease expiry and ttl head-drop do
    // happen opportunistically on every ring touch, so a busy queue needs
    // nothing — but IMPLICIT GC has no on-touch path at all (an idle, empty
    // queue is by definition one nobody is touching), so without this loop an
    // embedding application accumulates one ring per short-lived inbox for the
    // life of the process. That is precisely the workload the class exists for.
    //
    // Gated on QUEEN_SWEEPER for one reason only: it is the knob that means
    // "this process runs no reapers", and a reaper that ignored it would make
    // the knob a lie. It takes no connection, no lock outside its own maps and
    // no lane slot, so it is safe to run anywhere else.
    if cfg.sweeper_enabled {
        let eph = st.ephemeral.clone();
        tasks.push(tokio::spawn(async move {
            let mut iv = tokio::time::interval(std::time::Duration::from_millis(1000));
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                iv.tick().await;
                let s = eph.sweep(crate::util::now_epoch_ms());
                if s.redelivered > 0 || s.exhausted > 0 || s.dropped_ttl > 0 || s.gc_queues > 0 {
                    tracing::info!(
                        target: "ephemeral",
                        redelivered = s.redelivered,
                        exhausted = s.exhausted,
                        dropped_ttl = s.dropped_ttl,
                        gc_queues = s.gc_queues,
                        queues = eph.queue_count(),
                        bytes = eph.global_bytes(),
                        "backstop"
                    );
                }
            }
        }));
    }

    // Idle sweep for the hot-list rings and long-poll wake gates (inlined from
    // reconcile::spawn_idle_sweep for the same handle reason; 0 disables).
    if cfg.hotlist_idle_sweep_ms > 0 {
        let state = st.clone();
        let interval = std::time::Duration::from_millis(cfg.hotlist_idle_sweep_ms);
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let rings = state.hotlist.evict_idle();
                let gates = state.notifier.evict_idle();
                let lanes = state.autopilot.evict_idle();
                if rings > 0 || gates > 0 || lanes > 0 {
                    tracing::debug!(
                        target: "reconcile",
                        rings_evicted = rings,
                        gates_evicted = gates,
                        autopilot_lanes_evicted = lanes,
                        "idle sweep"
                    );
                }
            }
        }));
    }

    // Unserved-ring trim (inlined from reconcile::spawn_unserved_trim for the same handle
    // reason; 0 disables). Its own timer, deliberately much faster than the idle sweep —
    // see the rationale on `spawn_unserved_trim`.
    if cfg.hotlist_unserved_trim_ms > 0 {
        let state = st.clone();
        let interval = std::time::Duration::from_millis(cfg.hotlist_unserved_trim_ms as u64);
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let trimmed = state.hotlist.trim_unserved(crate::util::now_epoch_ms());
                if trimmed > 0 {
                    tracing::info!(
                        target: "reconcile",
                        rings_trimmed = trimmed,
                        "unserved-ring trim"
                    );
                }
            }
        }));
    }

    tracing::info!(
        target: "boot",
        version = crate::VERSION,
        mode = "embedded",
        fusion_shards = cfg.fusion_shards,
        pool = cfg.pool_size,
        spool_dir = %cfg.file_buffer.dir,
        "embedded broker started"
    );

    Ok(Booted {
        st,
        tasks,
        auto_spool_dir,
    })
}
