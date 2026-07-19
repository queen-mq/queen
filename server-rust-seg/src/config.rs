use std::time::Duration;

/// JWT auth configuration, mirroring the C++ `AuthConfig` (server/include/queen/config.hpp).
/// When `enabled` is false (the default) the auth middleware passes every request
/// through untouched — this is how the whole existing test-suite runs.
#[derive(Clone)]
pub struct AuthConfig {
    pub enabled: bool,
    pub algorithm: String,
    pub secret: String,
    /// PEM public key for RS256/EdDSA. Reserved for parity with the C++ config;
    /// this broker only verifies HS256 today (the auth tests are HS256-only), so
    /// it is loaded but not yet consulted.
    #[allow(dead_code)]
    pub public_key: String,
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
}

pub struct Config {
    pub port: String,
    pub pg: deadpool_postgres::Config,
    pub pool_size: usize,
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
    // pop long-poll
    pub pop_default_timeout_ms: u64,
    pub pop_wait_poll_ms: u64,
    // cross-request fusion
    pub fusion_shards: usize,
    pub fusion_frames: usize,
    pub fusion_hold_ms: u64,
    // background retention/eviction sweep cadence (ms)
    pub retention_interval_ms: u64,
    // JWT authentication (disabled by default).
    pub auth: AuthConfig,
}

fn env_str(k: &str, def: &str) -> String {
    std::env::var(k).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| def.to_string())
}
fn env_int(k: &str, def: i64) -> i64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(def)
}
fn env_f64(k: &str, def: f64) -> f64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(def)
}
fn env_bool(k: &str, def: bool) -> bool {
    match std::env::var(k).ok().map(|v| v.to_ascii_lowercase()) {
        Some(v) => matches!(v.as_str(), "1" | "true" | "yes" | "on"),
        None => def,
    }
}

pub fn load() -> Config {
    let mut pg = deadpool_postgres::Config::new();
    pg.host = Some(env_str("PG_HOST", "localhost"));
    pg.port = Some(env_int("PG_PORT", 5432) as u16);
    pg.user = Some(env_str("PG_USER", "postgres"));
    pg.password = Some(env_str("PG_PASSWORD", "postgres"));
    pg.dbname = Some(env_str("PG_DATABASE", "postgres"));

    Config {
        port: env_str("PORT", "6632"),
        pg,
        pool_size: env_int("DB_POOL_SIZE", 160) as usize,
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
        pop_default_timeout_ms: env_int("POP_DEFAULT_TIMEOUT_MS", 2000) as u64,
        pop_wait_poll_ms: env_int("POP_WAIT_POLL_MS", 25) as u64,
        fusion_shards: env_int("QUEEN_V2_FUSION_SHARDS", 8).max(1) as usize,
        fusion_frames: env_int("QUEEN_V2_FUSION_FRAMES", 500).max(1) as usize,
        fusion_hold_ms: env_int("QUEEN_V2_FUSION_HOLD_MS", 15).max(1) as u64,
        // RetentionService cadence (C++ parity). retention.js starts the server
        // with RETENTION_INTERVAL=2000; default matches the C++ 5-minute sweep.
        retention_interval_ms: env_int("RETENTION_INTERVAL", 300000).max(1) as u64,
        auth: AuthConfig::from_env(),
    }
}
