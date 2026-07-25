//! Env-driven configuration. Same parsing semantics as the broker's config.rs:
//! only the literal "true" is truthy, empty strings are preserved, ints fall back
//! on parse failure.

use std::env;

pub fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

pub fn env_opt(key: &str) -> Option<String> {
    env::var(key).ok().filter(|s| !s.is_empty())
}

pub fn env_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(v) => v == "true",
        Err(_) => default,
    }
}

pub fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// The header the proxy injects toward the broker to scope queue identity
/// (Track B). Trusted only inside the cell network.
pub const TENANT_HEADER: &str = "x-queen-tenant";
pub const REQUEST_ID_HEADER: &str = "x-queen-request-id";
/// Fixed default tenant UUID — must match the broker's Track B constant.
pub const DEFAULT_TENANT_UUID: &str = "00000000-0000-0000-0000-000000000001";

#[derive(Clone, Debug)]
pub struct PxdbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub use_ssl: bool,
    pub ssl_reject_unauthorized: bool,
    pub pool_size: usize,
}

/// Static single-cluster fallback for local dev without a pxdb (smoke tests).
#[derive(Clone, Debug)]
pub struct DevStaticCluster {
    pub cell_url: String,
    pub cell_token: Option<String>,
    pub broker_tenant: String,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub port: u16,
    pub pxdb: Option<PxdbConfig>,
    /// false = shadow mode: limit decisions are computed+logged+metered but not enforced.
    pub enforce: bool,
    /// Dev only: skip authentication entirely (never set in cloud).
    pub dev_insecure: bool,
    pub dev_static: Option<DevStaticCluster>,
    /// Send the Track B tenant header upstream.
    pub send_tenant_header: bool,
    pub max_body_bytes: usize,
    pub default_max_batch_items: u64,
    pub upstream_connect_timeout_ms: u64,
    pub upstream_request_timeout_ms: u64,
    pub longpoll_margin_ms: u64,
    pub longpoll_max_ms: u64,
    // identity / tokens
    pub jwt_issuer: String,
    pub jwt_hs_secret: Option<String>,
    pub jwt_ed25519_pem: Option<String>,
    pub jwt_ttl_s: u64,
    pub cookie_name: String,
    pub cookie_domain: Option<String>,
    pub auth_host_mode: bool,
    pub public_base_url: Option<String>,
    pub google_client_id: Option<String>,
    pub google_client_secret: Option<String>,
    pub github_client_id: Option<String>,
    pub github_client_secret: Option<String>,
    // metering
    pub meter_flush_ms: u64,
    pub spool_dir: String,
}

impl Config {
    pub fn load() -> Config {
        let pxdb = env_opt("PXDB_HOST").map(|host| PxdbConfig {
            host,
            port: env_u64("PXDB_PORT", 5432) as u16,
            user: env_str("PXDB_USER", "postgres"),
            password: env_str("PXDB_PASSWORD", ""),
            dbname: env_str("PXDB_DB", "queen_proxy"),
            use_ssl: env_bool("PXDB_USE_SSL", false),
            ssl_reject_unauthorized: env_bool("PXDB_SSL_REJECT_UNAUTHORIZED", true),
            pool_size: env_u64("PXDB_POOL_SIZE", 16) as usize,
        });
        let dev_static = env_opt("QUEEN_PROXY_DEV_CELL_URL").map(|cell_url| DevStaticCluster {
            cell_url,
            cell_token: env_opt("QUEEN_PROXY_DEV_CELL_TOKEN"),
            broker_tenant: env_str("QUEEN_PROXY_DEV_TENANT", DEFAULT_TENANT_UUID),
        });
        Config {
            port: env_u64("QUEEN_PROXY_PORT", 6711) as u16,
            pxdb,
            enforce: env_bool("QUEEN_PROXY_ENFORCE", false),
            dev_insecure: env_bool("QUEEN_PROXY_DEV_INSECURE", false),
            dev_static,
            send_tenant_header: env_bool("QUEEN_PROXY_TENANT_HEADER", true),
            max_body_bytes: env_u64("QUEEN_PROXY_MAX_BODY_BYTES", 16 * 1024 * 1024) as usize,
            default_max_batch_items: env_u64("QUEEN_PROXY_MAX_BATCH_ITEMS", 10_000),
            upstream_connect_timeout_ms: env_u64("QUEEN_PROXY_UPSTREAM_CONNECT_TIMEOUT_MS", 5_000),
            upstream_request_timeout_ms: env_u64("QUEEN_PROXY_UPSTREAM_TIMEOUT_MS", 35_000),
            longpoll_margin_ms: env_u64("QUEEN_PROXY_LONGPOLL_MARGIN_MS", 10_000),
            longpoll_max_ms: env_u64("QUEEN_PROXY_LONGPOLL_MAX_MS", 90_000),
            jwt_issuer: env_str("QUEEN_PROXY_JWT_ISS", "queen-proxy"),
            jwt_hs_secret: env_opt("QUEEN_PROXY_JWT_SECRET"),
            jwt_ed25519_pem: env_opt("QUEEN_PROXY_JWT_ED25519_PEM"),
            jwt_ttl_s: env_u64("QUEEN_PROXY_JWT_TTL_S", 86_400),
            cookie_name: env_str("QUEEN_PROXY_COOKIE_NAME", "queen_session"),
            cookie_domain: env_opt("QUEEN_PROXY_COOKIE_DOMAIN"),
            auth_host_mode: env_bool("QUEEN_PROXY_AUTH_HOST", false),
            public_base_url: env_opt("QUEEN_PROXY_PUBLIC_URL"),
            google_client_id: env_opt("GOOGLE_CLIENT_ID"),
            google_client_secret: env_opt("GOOGLE_CLIENT_SECRET"),
            github_client_id: env_opt("GITHUB_CLIENT_ID"),
            github_client_secret: env_opt("GITHUB_CLIENT_SECRET"),
            meter_flush_ms: env_u64("QUEEN_PROXY_METER_FLUSH_MS", 15_000),
            spool_dir: env_str("QUEEN_PROXY_SPOOL_DIR", "./queen-proxy-spool"),
        }
    }
}
