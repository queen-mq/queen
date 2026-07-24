use std::time::Duration;

/// JWT auth configuration, mirroring the C++ `AuthConfig` (server/include/queen/config.hpp).
/// When `enabled` is false (the default) the auth middleware passes every request
/// through untouched — this is how the whole existing test-suite runs.
#[derive(Clone)]
pub struct AuthConfig {
    pub enabled: bool,
    pub algorithm: String,
    pub secret: String,
    /// PEM public key for RS256/EdDSA (static-key deployments). Consulted before
    /// the JWKS cache (RUSTFIX item 7).
    pub public_key: String,
    /// JWKS endpoint for RS256/EdDSA key discovery + rotation (RUSTFIX item 7).
    pub jwks_url: String,
    pub jwks_refresh_interval_seconds: i64,
    pub jwks_request_timeout_ms: i64,
    pub issuer: String,
    pub audience: String,
    pub clock_skew_seconds: i64,
    pub skip_paths: Vec<String>,
    pub roles_claim: String,
    pub roles_array_claim: String,
    pub role_admin: String,
    pub role_read_write: String,
    pub role_read_only: String,
    pub role_write_only: String,
}

impl AuthConfig {
    fn from_env() -> AuthConfig {
        let skip_raw = env_str(
            "JWT_SKIP_PATHS",
            "/health,/metrics/prometheus,/metrics,/",
        );
        let skip_paths: Vec<String> = skip_raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        AuthConfig {
            enabled: env_bool("JWT_ENABLED", false),
            algorithm: env_str("JWT_ALGORITHM", "HS256"),
            secret: env_str("JWT_SECRET", ""),
            public_key: env_str("JWT_PUBLIC_KEY", ""),
            jwks_url: env_str("JWT_JWKS_URL", ""),
            jwks_refresh_interval_seconds: env_int("JWT_JWKS_REFRESH_INTERVAL", 3600),
            jwks_request_timeout_ms: env_int("JWT_JWKS_TIMEOUT_MS", 5000),
            issuer: env_str("JWT_ISSUER", ""),
            audience: env_str("JWT_AUDIENCE", ""),
            clock_skew_seconds: env_int("JWT_CLOCK_SKEW", 30),
            skip_paths,
            roles_claim: env_str("JWT_ROLES_CLAIM", "role"),
            roles_array_claim: env_str("JWT_ROLES_ARRAY_CLAIM", "roles"),
            role_admin: env_str("JWT_ROLE_ADMIN", "admin"),
            role_read_write: env_str("JWT_ROLE_READ_WRITE", "read-write"),
            role_read_only: env_str("JWT_ROLE_READ_ONLY", "read-only"),
            role_write_only: env_str("JWT_ROLE_WRITE_ONLY", "write-only"),
        }
    }

    /// Exact-match, plus trailing-slash prefix match for entries longer than "/"
    /// (identical to the C++ `should_skip_path`).
    pub fn should_skip(&self, path: &str) -> bool {
        for skip in &self.skip_paths {
            if path == skip {
                return true;
            }
            if skip.len() > 1 && skip.ends_with('/') && path.starts_with(skip.as_str()) {
                return true;
            }
        }
        false
    }

    /// Startup config validation (RUSTFIX item 7), mirroring the C++
    /// `AuthConfig::validate` (config.hpp:539-561): when auth is enabled, the
    /// configured algorithm must have usable key material. Returns Err with a
    /// message naming the missing credentials so boot can fail loudly.
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        match self.algorithm.as_str() {
            "HS256" | "auto" => {
                if self.secret.is_empty() && self.jwks_url.is_empty() && self.public_key.is_empty() {
                    return Err(format!(
                        "JWT_ENABLED=true with JWT_ALGORITHM={} but no key material: set JWT_SECRET (HS256), or JWT_PUBLIC_KEY / JWT_JWKS_URL (RS256/EdDSA)",
                        self.algorithm
                    ));
                }
            }
            "RS256" | "RS384" | "RS512" | "EdDSA" => {
                if self.jwks_url.is_empty() && self.public_key.is_empty() {
                    return Err(format!(
                        "JWT_ENABLED=true with JWT_ALGORITHM={} but neither JWT_PUBLIC_KEY nor JWT_JWKS_URL is set",
                        self.algorithm
                    ));
                }
            }
            other => {
                return Err(format!(
                    "JWT_ALGORITHM={other} is not supported (use HS256, RS256, RS384, RS512, EdDSA, or auto)"
                ));
            }
        }
        Ok(())
    }
}

/// Inter-instance sync configuration, mirroring the C++ `InterInstanceConfig`
/// + `SharedStateConfig` (server/include/queen/config.hpp). Drives `mesh.rs` /
/// `notify.rs`. When `enabled` is true but `peers` is empty (a single stock
/// broker) the mesh transport is never bound — the broker behaves exactly as
/// before, except the in-process long-poll waker (which needs no cluster) is live.
#[derive(Clone)]
pub struct SyncConfig {
    pub enabled: bool,
    /// TCP mesh listen/dial port. `QUEEN_MESH_PORT`, falling back to the legacy
    /// `QUEEN_UDP_NOTIFY_PORT` (default 6633).
    pub mesh_port: u16,
    /// Parsed peer list — (host, port) pairs from `QUEEN_MESH_PEERS` (falling back
    /// to the legacy `QUEEN_UDP_PEERS`). Empty ⇒ no peers ⇒ no mesh.
    pub peers: Vec<(String, u16)>,
    /// HMAC-SHA256 secret for the connection handshake. Empty ⇒ open mode (no auth).
    pub secret: String,
    pub heartbeat_ms: u64,
    pub dead_threshold_ms: u64,
    /// `QUEEN_CACHE_REFRESH_INTERVAL_MS` (default 60000). RUSTFIX item 16: drives
    /// the periodic reconcile loop (reconcile.rs) that re-reads the maintenance
    /// flags from queen.system_state and drops the lease cache, so a replica heals
    /// within one interval after a lost UDP packet.
    pub cache_refresh_ms: u64,
    /// This node's identity in heartbeats/stats (env `QUEEN_SERVER_ID`, else a
    /// short random id). Cosmetic — used only for peer identity/logging/stats.
    pub server_id: String,
}

impl SyncConfig {
    fn from_env() -> SyncConfig {
        // Prefer the new QUEEN_MESH_* names; fall back to the legacy QUEEN_UDP_*
        // ones so existing deployments keep working unchanged.
        let mesh_port = env_int("QUEEN_MESH_PORT", env_int("QUEEN_UDP_NOTIFY_PORT", 6633)) as u16;
        let peers_raw = {
            let mesh = env_str("QUEEN_MESH_PEERS", "");
            if !mesh.is_empty() {
                mesh
            } else {
                env_str("QUEEN_UDP_PEERS", "")
            }
        };
        let peers = parse_mesh_peers(&peers_raw, mesh_port);
        let server_id = std::env::var("QUEEN_SERVER_ID")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| std::env::var("HOSTNAME").ok().filter(|v| !v.is_empty()))
            .unwrap_or_else(|| format!("queen-{:08x}", rand::random::<u32>()));
        SyncConfig {
            enabled: env_bool("QUEEN_SYNC_ENABLED", true),
            mesh_port,
            peers,
            secret: env_str("QUEEN_SYNC_SECRET", ""),
            heartbeat_ms: env_int("QUEEN_SYNC_HEARTBEAT_MS", 1000).max(1) as u64,
            dead_threshold_ms: env_int("QUEEN_SYNC_DEAD_THRESHOLD_MS", 5000).max(1) as u64,
            cache_refresh_ms: env_int("QUEEN_CACHE_REFRESH_INTERVAL_MS", 60000).max(1) as u64,
            server_id,
        }
    }

    /// The mesh transport only starts when sync is enabled AND at least one peer is
    /// configured (matches the C++ `enabled_ = shared_state.enabled && has_udp_peers()`).
    pub fn mesh_active(&self) -> bool {
        self.enabled && !self.peers.is_empty()
    }
}

/// Parse the peer list: comma-separated `host` or `host:port` entries (a bare host
/// uses `default_port`). Tolerates an `http://` prefix and surrounding whitespace,
/// matching the C++ `parse_udp_peers`.
fn parse_mesh_peers(raw: &str, default_port: u16) -> Vec<(String, u16)> {
    raw.split(',')
        .filter_map(|entry| {
            let mut s = entry.trim();
            if let Some(rest) = s.strip_prefix("http://") {
                s = rest;
            }
            if let Some(rest) = s.strip_prefix("https://") {
                s = rest;
            }
            if s.is_empty() {
                return None;
            }
            // Split host:port on the LAST ':' so IPv6 literals (unlikely here) or
            // stray colons degrade gracefully; a non-numeric tail keeps default port.
            match s.rsplit_once(':') {
                Some((host, port)) if !host.is_empty() => match port.trim().parse::<u16>() {
                    Ok(p) => Some((host.trim().to_string(), p)),
                    Err(_) => Some((s.to_string(), default_port)),
                },
                _ => Some((s.to_string(), default_port)),
            }
        })
        .collect()
}

pub struct Config {
    pub port: String,
    pub pg: deadpool_postgres::Config,
    pub pool_size: usize,
    // Postgres TLS (RUSTFIX item 5). C++ `PG_USE_SSL` (default false) /
    // `PG_SSL_REJECT_UNAUTHORIZED` (default true), config.hpp:90-91. When
    // `pg_use_ssl` is false the pool is created with `NoTls` (byte-for-byte
    // unchanged); when true a rustls connector is wired in `db.rs`, with cert
    // verification disabled iff `pg_ssl_reject_unauthorized` is false.
    pub pg_use_ssl: bool,
    pub pg_ssl_reject_unauthorized: bool,
    pub stmt_timeout: Duration,
    pub zstd_level: i32,
    // Vegas per-lane: initial / min / max / alpha / beta.
    pub push_init: u64,
    pub push_min: u64,
    pub push_max: u64,
    pub pop_init: u64,
    pub pop_min: u64,
    pub pop_max: u64,
    pub vegas_alpha: f64,
    pub vegas_beta: f64,
    // pop long-poll (RUSTFIX item 19). Default idle wait 30s (DEFAULT_TIMEOUT); the
    // re-query interval backs off exponentially instead of a fixed poll.
    pub pop_default_timeout_ms: u64,
    pub pop_wait_initial_interval_ms: u64,
    pub pop_wait_backoff_threshold: u32,
    pub pop_wait_backoff_multiplier: f64,
    pub pop_wait_max_interval_ms: u64,
    // cross-request fusion
    pub fusion_shards: usize,
    pub fusion_frames: usize,
    pub fusion_hold_ms: u64,
    // background retention/eviction sweep cadence (ms)
    pub retention_interval_ms: u64,
    // Broker-side dedup cache (doc 18 §5, server/src/dedup.rs). `dedup_cache_mb`
    // caps the global cache footprint (whole-partition LRU eviction);
    // `dedup_cache_enabled` is the kill switch — disabled, the broker always
    // sends p_verified = -1 and SQL probes the full window (always sound).
    //
    // SIZING: the cache holds ~16 bytes per in-window message hash across all
    // active partitions, so the working set is roughly
    //     needed_mb ≈ 16 × msg_rate(msg/s) × dedup_window_seconds / 1e6
    // (e.g. 700k msg/s × 300s ≈ 3.4 GB → set QUEEN_DEDUP_CACHE_MB≈6144). Under
    // the cap the cache degrades GRACEFULLY, not catastrophically: partitions
    // that don't fit are SUPPRESSED (they push with p_verified=-1 and pay a PG
    // full-window probe instead of a broker re-hydration) rather than triggering
    // a re-hydration thrash loop. A one-line rate-limited warning with this
    // formula is logged while suppression is active.
    #[allow(dead_code)]
    pub dedup_cache_mb: usize,
    #[allow(dead_code)]
    pub dedup_cache_enabled: bool,
    // Broker-side ACK REGISTRY (server/src/ack_registry.rs). Caps the in-memory
    // lease→(worker, batch_end, batch-hash-set) map that turns a full-batch
    // completed ack into one positional cursor advance (no PG hash resolution).
    // Whole-entry LRU eviction under the cap; an evicted/expired lease just takes
    // the unchanged queen.log_ack_by_hash_v1 path — always sound. Entries are
    // small and short-lived (consumed on ack), so 64 MB holds a very large number
    // of concurrently in-flight leases.
    pub ack_registry_mb: usize,
    // Kill switch (QUEEN_ACK_REGISTRY=0 disables). Disabled ⇒ every ack takes the
    // unchanged log_ack_by_hash_v1 path — exactly the pre-registry behaviour.
    pub ack_registry_enabled: bool,
    // Broker-side ACK FUSION (server/src/ack_fusion.rs). Coalesces registry-fast-
    // path full-batch acks into ONE queen.log_ack_multi_v1 transaction per flush
    // (one commit / one fsync for N cursor advances), fire-on-idle like the push
    // fusion. `ack_fusion_enabled` is the flag (QUEEN_ACK_FUSION, default ON;
    // "0"/"false" reverts to the synchronous log_ack_at_v1 fast path, byte-
    // identical to the pre-fusion behavior);
    // `ack_fusion_shards` buffers acks by hash(partition) (defaults to
    // fusion_shards); `ack_fusion_hold_ms` is the liveness backstop tick, NOT a
    // latency floor (fire-on-idle commits a lone ack in one RTT).
    pub ack_fusion_enabled: bool,
    pub ack_fusion_shards: usize,
    pub ack_fusion_hold_ms: u64,
    // Retention/metrics background-job knobs (RUSTFIX item 20 — C++ JobsConfig,
    // config.hpp:286-329). `retention_batch_size` bounds each metrics-purge DELETE;
    // `metrics_retention_days` is the worker/system-metrics purge window (default
    // 90, matching the C++ RetentionService, not the old hardcoded 7). Under the
    // segments engine `retention_parallelism` and `partition_cleanup_days` have no
    // work to do (the sweep is a single advisory-locked txn, no partitioned message
    // table to drop) — read for config-compat and documented inert.
    pub retention_batch_size: usize,
    #[allow(dead_code)]
    pub retention_parallelism: usize,
    pub metrics_retention_days: i32,
    #[allow(dead_code)]
    pub partition_cleanup_days: i32,
    // stats reconciler cadence (ms) — segments-native queen.stats refresh
    // (server/src/stats.rs). Mirrors the C++ StatsService STATS_INTERVAL_MS.
    pub stats_interval_ms: u64,
    // worker/system metrics flush cadence (ms) — server/src/syscollect.rs writes
    // one queen.worker_metrics + queen.system_metrics row per replica per window.
    pub metrics_flush_ms: u64,
    // JWT authentication (disabled by default).
    pub auth: AuthConfig,
    // Inter-instance mesh notifications (enabled by default, but inert with no peers).
    pub sync: SyncConfig,
    // Disk spool for DB-outage / maintenance push durability (RUSTFIX items 1, 17).
    pub file_buffer: FileBufferConfig,
    // Wildcard candidate hot-list (19-wildcard-hotlist.md, server/src/hotlist.rs).
    // Broker-side candidate selection for wildcard pops. Default ON;
    // QUEEN_HOTLIST=0 (or =false) reverts to the legacy per-pop SQL candidate
    // scan, byte-identical (every hook is a no-op / one branch). `hotlist_shards` sub-
    // shards each (queue,group) ring (defaults to fusion_shards); `hotlist_window_batch`
    // is the windowBuffer early-promotion threshold (§6).
    pub hotlist_enabled: bool,
    pub hotlist_shards: usize,
    pub hotlist_window_batch: u32,
    // Reseed / cold-start interval (ms) — §8 correctness floor for missed marks /
    // dropped mesh hints. Default 30s; lower for tests / tighter cross-broker floor.
    pub hotlist_reseed_ms: i64,
    // LOGGING_PLAN.md Phase 1: cadence of the periodic `rates`/`sizes` aggregate
    // log blocks (ms), and how many hot queues the per-queue lines rank & show.
    pub log_rates_ms: u64,
    pub log_top_n_queues: usize,
}

/// File-buffer configuration, mirroring the C++ `FileBufferConfig`
/// (config.hpp:356-392). Drives `file_buffer.rs`. Defaults match C++ exactly.
#[derive(Clone)]
pub struct FileBufferConfig {
    pub dir: String,
    pub flush_interval_ms: u64,
    pub max_batch_size: usize,
    pub max_events_per_file: usize,
}

impl FileBufferConfig {
    fn from_env() -> FileBufferConfig {
        FileBufferConfig {
            // Plan pins the Linux default; the C++ Apple `/tmp/queen` default is
            // dev-only. Must be a writable, persistent path for durability.
            dir: env_str("FILE_BUFFER_DIR", "/var/lib/queen/buffers"),
            flush_interval_ms: env_int("FILE_BUFFER_FLUSH_MS", 100).max(1) as u64,
            max_batch_size: env_int("FILE_BUFFER_MAX_BATCH", 100).max(1) as usize,
            max_events_per_file: env_int("FILE_BUFFER_EVENTS_PER_FILE", 10000).max(1) as usize,
        }
    }
}

// C++ `get_env_string` parity (config.hpp:29-33): a present-but-empty env var
// returns "" verbatim; only a genuinely-unset var falls back to the default.
// (RUSTFIX item 6 — the old `.filter(|v| !v.is_empty())` treated ""` as unset,
// which silently restored default lists e.g. for JWT_SKIP_PATHS="".)
fn env_str(k: &str, def: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| def.to_string())
}
fn env_int(k: &str, def: i64) -> i64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(def)
}
// RUSTFIX item 4: PG_DATABASE (Rust name) → PG_DB (C++ name, config.hpp:86) →
// "postgres". Uses a non-empty filter independent of `env_str`'s item-6 semantics
// so an explicitly-empty PG_DATABASE still falls through instead of yielding "".
// Must match the migration source resolution in handlers/migration.rs.
pub fn resolve_db_name() -> String {
    std::env::var("PG_DATABASE")
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| std::env::var("PG_DB").ok().filter(|v| !v.is_empty()))
        .unwrap_or_else(|| "postgres".to_string())
}
fn env_f64(k: &str, def: f64) -> f64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(def)
}
// C++ `get_env_bool` parity (config.hpp:11-15): only the exact literal "true" is
// truthy (case-sensitive). "1"/"yes"/"on"/"TRUE" are all false. (RUSTFIX item 6 —
// the old permissive parser turned JWT_ENABLED=1 into an accidental auth-enable.)
fn env_bool(k: &str, def: bool) -> bool {
    match std::env::var(k) {
        Ok(v) => v == "true",
        Err(_) => def,
    }
}

pub fn load() -> Config {
    let mut pg = deadpool_postgres::Config::new();
    pg.host = Some(env_str("PG_HOST", "localhost"));
    pg.port = Some(env_int("PG_PORT", 5432) as u16);
    pg.user = Some(env_str("PG_USER", "postgres"));
    pg.password = Some(env_str("PG_PASSWORD", "postgres"));
    // RUSTFIX item 4: prefer PG_DATABASE, fall back to the C++ name PG_DB, then
    // "postgres". Explicit non-empty chain (not env_str) so an empty PG_DATABASE
    // still falls through to PG_DB rather than resolving to "" under item 6.
    pg.dbname = Some(resolve_db_name());

    Config {
        port: env_str("PORT", "6632"),
        pg,
        pool_size: env_int("DB_POOL_SIZE", 160) as usize,
        pg_use_ssl: env_bool("PG_USE_SSL", false),
        pg_ssl_reject_unauthorized: env_bool("PG_SSL_REJECT_UNAUTHORIZED", true),
        stmt_timeout: Duration::from_millis(env_int("QUEEN_STMT_TIMEOUT_MS", 30000) as u64),
        zstd_level: env_int("QUEEN_V2_ZSTD_LEVEL", 3) as i32,
        push_init: env_int("QUEEN_SEG_PUSH_INIT", 16) as u64,
        push_min: env_int("QUEEN_SEG_PUSH_MIN", 4) as u64,
        push_max: env_int("QUEEN_SEG_PUSH_MAX", 64) as u64,
        pop_init: env_int("QUEEN_SEG_POP_INIT", 16) as u64,
        pop_min: env_int("QUEEN_SEG_POP_MIN", 4) as u64,
        pop_max: env_int("QUEEN_SEG_POP_MAX", 64) as u64,
        vegas_alpha: env_f64("QUEEN_VEGAS_ALPHA", 3.0),
        vegas_beta: env_f64("QUEEN_VEGAS_BETA", 6.0),
        // RUSTFIX item 19: honor DEFAULT_TIMEOUT (C++ name) first, then
        // POP_DEFAULT_TIMEOUT_MS, then 30000 — not the old 2000.
        pop_default_timeout_ms: env_int("DEFAULT_TIMEOUT", env_int("POP_DEFAULT_TIMEOUT_MS", 30000))
            .max(1) as u64,
        pop_wait_initial_interval_ms: env_int("POP_WAIT_INITIAL_INTERVAL_MS", 100).max(1) as u64,
        pop_wait_backoff_threshold: env_int("POP_WAIT_BACKOFF_THRESHOLD", 3).max(0) as u32,
        pop_wait_backoff_multiplier: env_f64("POP_WAIT_BACKOFF_MULTIPLIER", 2.0),
        pop_wait_max_interval_ms: env_int("POP_WAIT_MAX_INTERVAL_MS", 1000).max(1) as u64,
        fusion_shards: env_int("QUEEN_V2_FUSION_SHARDS", 8).max(1) as usize,
        fusion_frames: env_int("QUEEN_V2_FUSION_FRAMES", 500).max(1) as usize,
        fusion_hold_ms: env_int("QUEEN_V2_FUSION_HOLD_MS", 15).max(1) as u64,
        // Segments/log-engine sweep cadence (docs 17-18): retention runs as
        // frequent, incremental, bounded steps (advisory-locked, p_max_rows per
        // call) instead of the old C++ 5-minute monolith — a 5s default keeps
        // each step tiny so sweeps never stall pushes with long row locks.
        retention_interval_ms: env_int("RETENTION_INTERVAL", 5000).max(1) as u64,
        dedup_cache_mb: env_int("QUEEN_DEDUP_CACHE_MB", 512).max(1) as usize,
        // Kill switch semantics (doc 18 §5): QUEEN_DEDUP_CACHE=0 disables; any
        // other value (or unset) enables. Correctness never depends on the cache.
        dedup_cache_enabled: std::env::var("QUEEN_DEDUP_CACHE")
            .map(|v| v != "0")
            .unwrap_or(true),
        ack_registry_mb: env_int("QUEEN_ACK_REGISTRY_MB", 64).max(1) as usize,
        ack_registry_enabled: std::env::var("QUEEN_ACK_REGISTRY")
            .map(|v| v != "0")
            .unwrap_or(true),
        // ACK FUSION default ON (operator decision 2026-07-24 after the VM A/B:
        // rows_per_commit 104-107 under fsync backpressure, push_multi 38→21ms).
        // "0"/"false" disables — the kill switch mirrors the QUEEN_HOTLIST parse.
        ack_fusion_enabled: std::env::var("QUEEN_ACK_FUSION")
            .map(|v| v != "0" && v != "false")
            .unwrap_or(true),
        ack_fusion_shards: env_int("QUEEN_ACK_FUSION_SHARDS", env_int("QUEEN_V2_FUSION_SHARDS", 8))
            .max(1) as usize,
        ack_fusion_hold_ms: env_int("QUEEN_ACK_FUSION_HOLD_MS", 3).max(1) as u64,
        retention_batch_size: env_int("RETENTION_BATCH_SIZE", 1000).max(1) as usize,
        retention_parallelism: env_int("RETENTION_PARALLELISM", 1).max(1) as usize,
        metrics_retention_days: env_int("METRICS_RETENTION_DAYS", 90).max(1) as i32,
        partition_cleanup_days: env_int("PARTITION_CLEANUP_DAYS", 30).max(1) as i32,
        stats_interval_ms: env_int("STATS_INTERVAL_MS", 10000).max(1000) as u64,
        metrics_flush_ms: env_int("METRICS_FLUSH_MS", 60000).max(1000) as u64,
        auth: AuthConfig::from_env(),
        sync: SyncConfig::from_env(),
        file_buffer: FileBufferConfig::from_env(),
        // HOT-LIST default ON (operator decision 2026-07-24 after the VM A/B:
        // candidate scans gone from the profile, ingress lag flat, combo with
        // ack fusion beats the scan path on total delivered even on a slow
        // disk). "0"/"false" disables — the legacy SQL candidate-scan path
        // stays intact behind the kill switch.
        hotlist_enabled: std::env::var("QUEEN_HOTLIST")
            .map(|v| v != "0" && v != "false")
            .unwrap_or(true),
        hotlist_shards: env_int("QUEEN_HOTLIST_SHARDS", env_int("QUEEN_V2_FUSION_SHARDS", 8))
            .max(1) as usize,
        hotlist_window_batch: env_int("QUEEN_HOTLIST_WINDOW_BATCH", 100).max(1) as u32,
        hotlist_reseed_ms: env_int("QUEEN_HOTLIST_RESEED_MS", 30000).max(1),
        log_rates_ms: env_int("QUEEN_LOG_RATES_MS", 10000).max(1000) as u64,
        log_top_n_queues: env_int("QUEEN_LOG_TOPN_QUEUES", 10).max(1) as usize,
    }
}
