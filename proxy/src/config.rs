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

/// The roles `queen_proxy.cluster_roles.role` accepts (001_init.sql CHECK).
/// Kept here so a bad `QUEEN_PROXY_DEFAULT_ROLE` is caught at boot rather than
/// as a constraint violation on somebody's first login.
pub const CLUSTER_ROLES: [&str; 4] = ["admin", "producer", "consumer", "viewer"];

/// Validate `QUEEN_PROXY_DEFAULT_ROLE`, falling back to the least-privileged
/// role. Deliberately NOT fatal: an unusable default role only matters when
/// auto-provisioning is on, and downgrading to `viewer` cannot escalate
/// anything. The warning is what makes it findable.
pub fn resolve_default_role(raw: Option<String>) -> String {
    match raw {
        None => "viewer".to_string(),
        Some(v) => {
            let v = v.trim().to_ascii_lowercase();
            if CLUSTER_ROLES.contains(&v.as_str()) {
                v
            } else {
                tracing::warn!(
                    target: "config",
                    value = %v,
                    valid = ?CLUSTER_ROLES,
                    "QUEEN_PROXY_DEFAULT_ROLE is not a valid cluster role; using viewer"
                );
                "viewer".to_string()
            }
        }
    }
}

/// Comma-separated list, trimmed and lowercased, empty entries dropped. An
/// unset or all-whitespace value yields an empty Vec.
pub fn csv_lower(key: &str) -> Vec<String> {
    env::var(key)
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Grace window past a cache entry's TTL during which the last known-good
/// value may still be served, but only when pxdb failed to *answer* (PLAN §2:
/// "keep serving cached clusters (fail-open for known good) ... pxdb down ≠
/// data plane down"). Ten minutes covers the outages this exists to survive —
/// a managed-PG failover, a restart, a migration — while bounding the damage:
/// nothing is refreshed while pxdb is down, so this window is also the
/// worst-case delay before a control-plane decision taken during the outage
/// (suspend, delete, key revocation) takes effect. A clean "no such row"
/// never uses it — see cache.rs::fallback. Read here rather than off `Config`
/// (like QUEEN_PROXY_RECONCILE_MS in registry.rs) because one module consumes
/// it and it is read once, at construction.
pub fn stale_grace() -> std::time::Duration {
    std::time::Duration::from_millis(env_u64("QUEEN_PROXY_STALE_GRACE_MS", 600_000))
}

/// Deny-list policy when the pxdb revocation lookup itself fails (pool or
/// query error, not a clean "no such row"). Default false = fail OPEN: a
/// transient pxdb blip must not 401 every session, and the token still had to
/// pass signature + exp. true = fail CLOSED for deployments that would rather
/// lose availability than honour a token that may have been revoked. Read here
/// rather than off `Config` (like `stale_grace` above) because one module
/// consumes it and it is read once, when `auth::Keys` is built.
pub fn revocation_strict() -> bool {
    env_bool("QUEEN_PROXY_REVOCATION_STRICT", false)
}

/// How often `queen_proxy.sweep_revoked_tokens()` drops deny-list rows whose
/// own `exp` has passed. Pure GC — an expired row can no longer change a
/// verify's outcome — so hourly is ample; 0 disables the sweep.
pub fn revocation_sweep_interval() -> std::time::Duration {
    std::time::Duration::from_millis(env_u64("QUEEN_PROXY_REVOCATION_SWEEP_MS", 3_600_000))
}

/// Bound on the metering drain that runs after the listener stops accepting.
/// The drain spools to disk when pxdb is unreachable, so this only exists so a
/// dead pxdb cannot hold the process open indefinitely.
pub fn shutdown_drain_budget() -> std::time::Duration {
    std::time::Duration::from_millis(env_u64("QUEEN_PROXY_SHUTDOWN_DRAIN_MS", 5_000))
}

/// Paths to the OPTIONAL TLS listener material — the Cloudflare-origin leg
/// (PLAN §9: "origin cert between CF and the cell proxy"). Both must be set:
/// TLS is opt-in, and a half-configured listener is a misconfiguration worth
/// failing on rather than silently serving the plaintext port.
#[derive(Clone, Debug)]
pub struct TlsMaterial {
    pub cert_path: String,
    pub key_path: String,
}

pub fn tls_material() -> Result<Option<TlsMaterial>, String> {
    match (env_opt("QUEEN_PROXY_TLS_CERT"), env_opt("QUEEN_PROXY_TLS_KEY")) {
        (Some(cert_path), Some(key_path)) => Ok(Some(TlsMaterial { cert_path, key_path })),
        (None, None) => Ok(None),
        (Some(_), None) => Err("QUEEN_PROXY_TLS_CERT set without QUEEN_PROXY_TLS_KEY".to_string()),
        (None, Some(_)) => Err("QUEEN_PROXY_TLS_KEY set without QUEEN_PROXY_TLS_CERT".to_string()),
    }
}

/// The header the proxy injects toward the broker to scope queue identity
/// (Track B). Trusted only inside the cell network.
pub const TENANT_HEADER: &str = "x-queen-tenant";
pub const REQUEST_ID_HEADER: &str = "x-queen-request-id";
/// Header a HUMAN SESSION uses to say which cluster the request acts on, when
/// the Host header cannot (one webapp hostname fronting every cluster). Never
/// honoured for an API key: a data-plane credential stays bound to the cluster
/// it was issued on. See acting.rs for the full matrix.
pub const ACT_CLUSTER_HEADER: &str = "x-queen-act-cluster";
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
    /// Bound on every pxdb I/O the proxy does outside a query: TCP connect,
    /// pool checkout wait, connection create/recycle (db.rs, and the LISTEN
    /// connection in cache.rs). The caches only fall back to a stale entry
    /// once a lookup has FAILED, so without this a black-holed pxdb turned
    /// every cache miss into a request parked for the OS TCP timeout, and a
    /// saturated pool into an unbounded queue of requests behind it.
    pub timeout_ms: u64,
}

impl PxdbConfig {
    pub fn timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.timeout_ms.max(100))
    }
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
    /// Dev/demo only: slug to fall back to when Host resolves to no cluster
    /// (browsers on localhost). Never set in cloud.
    pub default_cluster: Option<String>,
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
    /// PER-CELL gate for the operator (super-admin) capability. Default FALSE,
    /// and meant to stay false forever on customer/shared cells.
    ///
    /// The routes an operator may open are blocked at the proxy because they
    /// are not tenant-scopable (cell-wide status, PG internals, host metrics,
    /// maintenance). Today no role can reach them at all, which is a
    /// STRUCTURAL guarantee — there is no code path, so there is no bug that
    /// can open one. `queen_proxy.users.is_operator` converts that into a
    /// runtime check, and this flag is what keeps the structural version on
    /// every cell that has no business serving an internal dashboard: with it
    /// off, `classify`'s operator class 404s before authentication even runs,
    /// exactly as a hard block does.
    pub operator_enabled: bool,
    pub google_client_id: Option<String>,
    pub google_client_secret: Option<String>,
    /// Google Workspace domains allowed to sign in, lowercased. EMPTY MEANS ANY
    /// verified Google account, which is only a closed door because
    /// auto-provision is off by default — an unknown identity still has to
    /// match an existing `queen_proxy.users` row. Turn auto-provision on
    /// without setting this and any verified Google account on earth can create
    /// itself an account.
    ///
    /// Checked against the `hd` claim OR the email's domain (see
    /// `validate_google_claims`).
    pub google_allowed_domains: Vec<String>,
    /// Cluster role granted to an auto-provisioned user, on every cluster of
    /// the auto-provision tenant. Without a grant a provisioned user logs in
    /// successfully and then 403s on everything, which reads as a broken deploy
    /// rather than a permissions decision — so this exists to make
    /// auto-provisioning produce a usable account or none at all.
    ///
    /// `QUEEN_PROXY_DEFAULT_ROLE`, default `viewer`. An unrecognised value
    /// falls back to `viewer` with a warning: a typo must not be able to
    /// escalate, and `viewer` is the least it can be.
    pub autoprovision_default_role: String,
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
            timeout_ms: env_u64("PXDB_TIMEOUT_MS", 5_000),
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
            default_cluster: env_opt("QUEEN_PROXY_DEFAULT_CLUSTER"),
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
            operator_enabled: env_bool("QUEEN_PROXY_OPERATOR_ENABLED", false),
            google_client_id: env_opt("GOOGLE_CLIENT_ID"),
            google_client_secret: env_opt("GOOGLE_CLIENT_SECRET"),
            google_allowed_domains: csv_lower("GOOGLE_ALLOWED_DOMAINS"),
            autoprovision_default_role: resolve_default_role(env_opt("QUEEN_PROXY_DEFAULT_ROLE")),
            github_client_id: env_opt("GITHUB_CLIENT_ID"),
            github_client_secret: env_opt("GITHUB_CLIENT_SECRET"),
            meter_flush_ms: env_u64("QUEEN_PROXY_METER_FLUSH_MS", 15_000),
            spool_dir: env_str("QUEEN_PROXY_SPOOL_DIR", "./queen-proxy-spool"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_role_unset_is_viewer() {
        assert_eq!(resolve_default_role(None), "viewer");
    }

    #[test]
    fn default_role_accepts_every_valid_role() {
        for r in CLUSTER_ROLES {
            assert_eq!(resolve_default_role(Some(r.to_string())), r);
        }
    }

    #[test]
    fn default_role_is_case_and_space_insensitive() {
        assert_eq!(resolve_default_role(Some("  ADMIN ".to_string())), "admin");
    }

    #[test]
    fn default_role_invalid_downgrades_to_viewer() {
        // `member` is the plausible wrong guess: it is not in the CHECK, and
        // silently becoming `admin` would be the dangerous failure mode.
        assert_eq!(resolve_default_role(Some("member".to_string())), "viewer");
        assert_eq!(resolve_default_role(Some("".to_string())), "viewer");
        assert_eq!(resolve_default_role(Some("root".to_string())), "viewer");
    }

    #[test]
    fn csv_lower_trims_lowercases_and_drops_empties() {
        std::env::set_var("QUEEN_TEST_CSV", " Smartpricing.IT , ,smartness.com,");
        assert_eq!(csv_lower("QUEEN_TEST_CSV"), vec!["smartpricing.it", "smartness.com"]);
        std::env::remove_var("QUEEN_TEST_CSV");
    }

    #[test]
    fn csv_lower_unset_is_empty() {
        std::env::remove_var("QUEEN_TEST_CSV_ABSENT");
        assert!(csv_lower("QUEEN_TEST_CSV_ABSENT").is_empty());
    }
}
