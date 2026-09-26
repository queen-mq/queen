use std::time::Duration;

/// Track B (native tenant scoping, PLAN_QUEEN_PROXY_CLOUD.md §5). The broker gains
/// ONE opaque concept: a `tenant_id` scoping key on queue identity, taken from a
/// trusted header the colocated proxy sets. These constants are the fixed contract.
///
/// The default tenant — used for EVERY request when the feature is off, and when
/// the header is absent while it is on — so OSS/self-host behaviour is byte-identical
/// (the DDL column defaults to this same value, so no backfill is ever needed).
pub const DEFAULT_TENANT: &str = "00000000-0000-0000-0000-000000000001";
/// The trusted header the proxy injects. The broker never validates the tenant
/// against anything (it is opaque; the trust is network — the cell boundary).
pub const TENANT_HEADER: &str = "x-queen-tenant";

/// The `JWT_ALGORITHM` values the broker accepts, in the order the boot error
/// lists them. This is the ONE spelling of the set: `AuthConfig::validate` is
/// matched against it and its "not supported" message is BUILT from it, so the
/// message can no longer name a set the validation does not enforce.
///
/// `auth.rs::check_alg_allowed` is the other half of the contract — the value
/// accepted at boot must be a value the verifier accepts at request time. The
/// two used to disagree about HS384/HS512 (refused here, implemented there,
/// which left pinning HS512 impossible and pushed operators to the strictly
/// wider `auto`); `auth::tests::boot_and_the_verifier_accept_the_same_algorithms`
/// now fails if they ever drift apart again.
pub const SUPPORTED_JWT_ALGORITHMS: &[&str] = &[
    "HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "EdDSA", "auto",
];

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
        let skip_raw = env_str("JWT_SKIP_PATHS", "/health,/metrics/prometheus,/metrics,/");
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
            // The whole HMAC family, not just HS256: `auth.rs` verifies HS384 and
            // HS512 off the same `JWT_SECRET` bytes, so refusing them here only
            // denied operators the ability to PIN one — `auto`, the workaround,
            // accepts all seven algorithms and is the weaker posture.
            "HS256" | "HS384" | "HS512" | "auto" => {
                if self.secret.is_empty() && self.jwks_url.is_empty() && self.public_key.is_empty()
                {
                    return Err(format!(
                        "JWT_ENABLED=true with JWT_ALGORITHM={} but no key material: set JWT_SECRET (HS256/HS384/HS512), or JWT_PUBLIC_KEY / JWT_JWKS_URL (RS256/EdDSA)",
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
                let mut list = SUPPORTED_JWT_ALGORITHMS.join(", ");
                if let Some(i) = list.rfind(", ") {
                    list.replace_range(i..i + 2, ", or ");
                }
                return Err(format!(
                    "JWT_ALGORITHM={other} is not supported (use {list})"
                ));
            }
        }
        Ok(())
    }
}

/// This node's name: `QUEEN_SERVER_ID`, else `HOSTNAME`, else a random one. The
/// dashboard's cell pages and `/auth/me` name the broker with it.
fn resolve_server_id() -> String {
    std::env::var("QUEEN_SERVER_ID")
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok().filter(|v| !v.is_empty()))
        .unwrap_or_else(|| format!("queen-{:08x}", rand::random::<u32>()))
}

pub struct Config {
    pub port: String,
    /// Host the HTTP listener binds — `QUEEN_BIND_ADDR`, default `0.0.0.0`:
    /// every interface, which is what the address was hardcoded to before the
    /// knob existed, so an upgrade changes nothing until someone sets it.
    ///
    /// HOST only. The port is `PORT` and nowhere else, because a variable that
    /// could carry its own port would be a second place to write one, and the
    /// two disagreeing is not a conflict anything downstream could resolve —
    /// so `load` rejects a value with a port instead of picking a winner.
    pub bind_addr: String,
    pub stmt_timeout: Duration,
    // pop long-poll (RUSTFIX item 19). Default idle wait 30s (DEFAULT_TIMEOUT); the
    // re-query interval backs off exponentially instead of a fixed poll.
    pub pop_default_timeout_ms: u64,
    /// Canonical `new` | `all`, already normalized by `normalize_subscription_mode`.
    pub default_subscription_mode: String,
    // JWT authentication (disabled by default).
    pub auth: AuthConfig,
    /// This node's name (`QUEEN_SERVER_ID` → `HOSTNAME` → random).
    pub server_id: String,
    // EMBEDDED MODE for the Kafka wire facade (server/src/kafka_facade.rs).
    // Off by default: nothing is spawned, nothing is logged, no behaviour changes.
    pub kafka_facade: KafkaFacadeConfig,
    // Track B (PLAN_QUEEN_PROXY_CLOUD.md §5): native tenant scoping on queue
    // identity. Default OFF ⇒ every request uses config::DEFAULT_TENANT and the
    // broker behaves byte-identically to today. ON ⇒ the tenant is read from the
    // `x-queen-tenant` header (absent ⇒ default tenant; malformed ⇒ 400). The
    // broker never validates the value — it is opaque; the trust is the cell
    // network (the proxy is the only thing that can set the header).
    pub tenancy_header: bool,
    /// §11.1 — the data directory (`QUEEN_RAFT_DIR`). Carries the consensus log,
    /// the store, the segment files and the IDENTITY. Boot refuses without it.
    pub raft_dir: String,
    /// §14.1 — `/health` answers `200 healthy` while the apply lag is under this
    /// (`QUEEN_RAFT_READY_LAG_MS`), else `503 settling`.
    pub raft_ready_lag_ms: u64,
    /// The disk gate: writes that grow storage are refused (`507`) once the
    /// data directory's filesystem is at least this percent used
    /// (`QUEEN_RAFT_DISK_HIGH_PCT`, default 85).
    pub raft_disk_high_pct: f64,
    /// Once the disk gate has closed, writes are accepted again below this
    /// percent (`QUEEN_RAFT_DISK_LOW_PCT`, default 80).
    pub raft_disk_low_pct: f64,
    /// The depth of the planner's bounded command channel
    /// (`QUEEN_RAFT_PLANNER_QUEUE_DEPTH`), printed at boot.
    pub raft_planner_queue_depth: usize,
    /// With tenancy ON, the ABSENCE of a `queen.kv_quota` row is a DENIAL (403
    /// `feature_gated`), not a permission (§9.4 correction 1): the tenant header is
    /// opaque and unvalidated, so a client that rotates it on every request would
    /// otherwise mint a fresh unlimited tenant each time. DERIVED from the tenancy
    /// flag so a self-hosted operator never has to understand why they should
    /// configure anything.
    pub kv_require_grant: bool,
    /// Per-tenant token bucket, evaluated before the state machine is asked, answering 429 +
    /// `Retry-After`. In the BROKER and not only in the proxy, because self-hosted and
    /// dedicated-without-proxy are real deployments and a defence that exists only in
    /// the proxy is not a defence of the product. 200 reads/s is ~20% of one core at
    /// 0,3-1 ms per PK read, and the rule being defended is that KV reads must not be
    /// able to consume more than ~10% of the backend CPU serving the log.
    pub kv_read_rate: u32,
    pub kv_read_burst: u32,
    /// 100 writes/s: every write is a durable commit (~4 ms fsync).
    pub kv_write_rate: u32,
    pub kv_write_burst: u32,
    /// Size cap on the per-tenant in-RAM maps (token bucket, quota cache). The cap
    /// DENIES, it does not evict (§9.4 correction 2) — eviction under an unvalidated,
    /// attacker-chosen tenant id is just a slower unbounded map. The house precedent
    /// is `handlers/mod.rs`: negatives are never cached, for exactly this reason.
    pub kv_max_tenants: usize,
    /// Release band, as a percentage of the cap. Block above the cap, release only
    /// under this — the same number and the same hysteresis as the proxy's
    /// `STORAGE_RELEASE_PERCENT` (`proxy/src/registry.rs:44`). Without a band a tenant
    /// oscillates in and out of the block on every refresh.
    pub kv_quota_release_percent: i64,
    /// The watermark, in percent, above which a tenant makes the cell "hot" and the
    /// usage rollup drops to the refresh cadence (§9.3). 80, because §14.3 point 5
    /// says that with a soft quota 80% is already late.
    pub kv_quota_hot_percent: i64,
    // --- Ephemeral queues (EPHEMERAL_QUEUES.md §3.8). RAM-class queues whose
    // contents survive nothing.
    //
    // THERE IS NO `QUEEN_EPHEMERAL_ENABLED`, and its absence is the same decision
    // the KV and timer flags were removed for (see the header of `switches.rs`):
    // the routes are registered unconditionally, so no client can ever have to ask
    // whether a cell "has ephemeral queues". Pausing the surface at runtime is a
    // kill switch, and cloud gating is a grant — neither is a boot flag.
    //
    // Every knob below is a CEILING on a resource in this process's heap, which is
    // why they read differently from the durable ones: there is no database to
    // absorb an overrun, so the bound is the only thing between a mispublishing
    // producer and the cell's memory.
    /// `QUEEN_EPHEMERAL_MAX_BYTES` (256 MiB). The cell's total ephemeral footprint;
    /// crossing it answers 503 `ephemeral_unavailable` (§1.6 rung 3). ~3% of a
    /// 2c/8 GB free cell at full burn (§10 Q2); dedicated cells raise it.
    pub ephemeral_max_bytes: i64,
    /// `QUEEN_EPHEMERAL_QUEUE_MAX_BYTES` (16 MiB) — the per-queue DEFAULT, and the
    /// ceiling a `configure` may not exceed. Both roles, one number, deliberately:
    /// a separate "max the tenant may ask for" is a knob nobody would ever set
    /// differently and a second value to keep consistent.
    pub ephemeral_queue_max_bytes: i64,
    /// `QUEEN_EPHEMERAL_QUEUE_MAX_LENGTH` (10 000) — same dual role. A byte cap
    /// alone does not bound the per-message bookkeeping (cursors, leases, attempt
    /// counts), which is what a flood of tiny messages actually consumes.
    pub ephemeral_queue_max_length: i64,
    /// `QUEEN_EPHEMERAL_LEASE_S` (30). At-least-once holds only while the owning
    /// incarnation lives (§1.3), so this is a redelivery latency, never a
    /// durability window.
    pub ephemeral_lease_s: i64,
    /// `QUEEN_EPHEMERAL_RETRY_LIMIT` (5). Exhausted attempts DROP the message and
    /// count `eph_dropped_retry` — there is no DLQ in this class (§9), and a
    /// silent infinite retry would be a poison-message loop with no exit.
    pub ephemeral_retry_limit: u32,
    /// `QUEEN_EPHEMERAL_IMPLICIT_IDLE_S` (300). How long an IMPLICIT queue may sit
    /// empty and unpolled before it is collected. Declared queues are never
    /// collected: their configuration is durable and their emptiness is normal.
    pub ephemeral_implicit_idle_s: i64,
    /// `QUEEN_EPHEMERAL_REQUIRE_GRANT`. With tenancy ON, the ABSENCE of a grant row
    /// is a DENIAL (403 `feature_gated`) and not a permission — the identical
    /// posture, and the identical derivation, as `kv_require_grant` above (§1.6
    /// rung 2 / M7). The tenant header is opaque and unvalidated, so a client that
    /// rotates it on every request would otherwise mint a fresh unlimited tenant —
    /// and on THIS class an unlimited tenant is unlimited RAM.
    pub ephemeral_require_grant: bool,
    /// `QUEEN_EPHEMERAL_RATE` / `_BURST`: per-tenant token bucket over PUSHED
    /// MESSAGES per second (§1.6 rung 2), evaluated before a single byte is
    /// charged, answering 429 + `Retry-After`. A grant row's `max_msgs_per_sec`
    /// overrides both. The default is deliberately an order of magnitude above the
    /// durable commit-bound ceiling of a free cell: this class costs a `VecDeque`
    /// push, so the rate is a runaway-producer guard and not a throughput policy.
    pub ephemeral_rate: u32,
    pub ephemeral_burst: u32,
}

/// The Kafka wire facade, run IN-PROCESS on its own runtime (kafka_inproc.rs)
/// when `QUEEN_KAFKA_EMBEDDED=true`. Every other `QUEEN_KAFKA_*` variable the
/// facade documents is read by the facade itself.
#[derive(Clone)]
pub struct KafkaFacadeConfig {
    pub enabled: bool,
    /// How long the stopping facade has to hand its registry row back.
    pub shutdown_grace_ms: u64,
}

impl KafkaFacadeConfig {
    fn from_env() -> KafkaFacadeConfig {
        KafkaFacadeConfig {
            enabled: env_bool("QUEEN_KAFKA_EMBEDDED", false),
            // Floored at 100ms because a grace of zero is an abort with extra steps.
            shutdown_grace_ms: env_int("QUEEN_KAFKA_SHUTDOWN_GRACE_MS", 5000).max(100) as u64,
        }
    }
}

// C++ `get_env_string` parity (config.hpp:29-33): a present-but-empty env var
// returns "" verbatim; only a genuinely-unset var falls back to the default.
// (RUSTFIX item 6 — the old `.filter(|v| !v.is_empty())` treated ""` as unset,
// which silently restored default lists e.g. for JWT_SKIP_PATHS="".)
/// Canonicalize a subscription mode to one of the two spellings the SQL actually
/// understands.
///
/// `004_log_pop.sql` matches with `COALESCE(p_sub_mode,'all') = 'new'` — an EXACT
/// string compare — so every other spelling falls through to full-backlog replay.
/// That silently broke a documented alias: the Go SDK exports
/// `SubscriptionModeNewOnly = "new-only"` ("alias for SubscriptionModeNew"), the
/// CLI advertises `all|new|new-only` in `--from-mode`, and the JS README shows
/// `.subscriptionMode('new-only')` as real usage — yet `new-only` reached the SQL
/// verbatim, missed the `= 'new'` arm, and replayed everything: the exact opposite
/// of what it advertises, on every client, with no test catching it.
///
/// Anything not recognized as "start at the tail" still resolves to `all`, which is
/// both the historical fallthrough and the safe direction (a consumer that replays
/// too much is recoverable; one that skips messages is not). That deliberately keeps
/// working the informal spellings already in the test suites, e.g. `from_beginning`.
pub(crate) fn normalize_subscription_mode(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "new" | "new-only" | "new_only" | "newonly" => "new".to_string(),
        _ => "all".to_string(),
    }
}

/// Join a bind host and a port into an authority, bracketing an IPv6 literal.
/// Unbracketed, `::1` + `6632` is `::1:6632`, which is NOT a `SocketAddr` —
/// that grammar requires the brackets — so it falls through to the name
/// resolver, where it survives only because the platform's getaddrinfo happens
/// to take a bare numeric v6 host. Bracketing keeps the bind on the parse path
/// rather than resting on that, and puts the canonical form in the boot line,
/// which is the string an operator pastes into curl. A hostname passes through
/// untouched and is resolved at bind time.
pub fn host_port(host: &str, port: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// The HTTP listener's bind host, validated. Also the mesh listener's default,
/// which is why it is a function and not an inline `env_str`.
fn http_bind_addr() -> String {
    checked_bind_addr(
        "QUEEN_BIND_ADDR",
        "PORT",
        env_str("QUEEN_BIND_ADDR", "0.0.0.0"),
    )
}

/// FATAL on a bind host that carries a port, or that is explicitly empty.
/// Both would otherwise be joined with the port into an address nothing
/// listens on, and the process would die at bind quoting a string no operator
/// ever wrote (`0.0.0.0:7000:6632`). An IP literal — v4 or v6 — or a hostname
/// is accepted here; whether the hostname RESOLVES is still the bind's problem,
/// because resolution can fail for reasons that have nothing to do with config.
/// `port_key` is the variable that owns the port for THIS listener, so the
/// message points at the right one rather than at `PORT` for the mesh.
fn checked_bind_addr(k: &str, port_key: &str, v: String) -> String {
    if v.is_empty() {
        crate::obs::fatal(format!(
            "{k} is set to the empty string — give it a host or IP, or unset it for 0.0.0.0"
        ));
    }
    if v.parse::<std::net::IpAddr>().is_err() && v.contains(':') {
        crate::obs::fatal(format!(
            "{k}={v} carries a port — set the host or IP only, the port comes from {port_key}"
        ));
    }
    v
}

fn env_str(k: &str, def: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| def.to_string())
}
fn env_int(k: &str, def: i64) -> i64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(def)
}
/// A percentage knob: unset, unparsable or outside [`is_pct`] keeps `def`.
fn env_pct(k: &str, def: f64) -> f64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| is_pct(*v))
        .unwrap_or(def)
}
/// The range a percentage knob accepts (NaN is outside it).
pub(crate) fn is_pct(v: f64) -> bool {
    (1.0..=100.0).contains(&v)
}
// ---------------------------------------------------------------------------
// Boolean env parsing — ONE parser for every boolean knob in the broker.
//
// History: the C++ `get_env_bool` (config.hpp:11-15) accepted only the exact
// literal "true", and RUSTFIX item 6 kept that parity to stop a permissive parser
// from turning `JWT_ENABLED=1` into an accidental auth-ENABLE. But strictness with
// a silent fallback fails in the OTHER direction just as badly: `JWT_ENABLED=1`
// resolved to the `false` default, i.e. a broker running with authentication OFF
// and not a word in the log.
//
// The fix keeps the intent (no value is ever guessed into a security-relevant
// direction) while removing the silence:
//   * the common spellings are accepted, case-insensitively — true/false, 1/0,
//     yes/no, on/off (surrounding whitespace trimmed);
//   * anything else non-empty is a FATAL config error naming the variable and the
//     bad value — the default is never used to paper over a typo;
//   * unset (or present-but-empty, e.g. `JWT_ENABLED=` in a compose file) uses the
//     documented default.
// Every boolean env read in the broker routes through here, so `=on` and `=0` mean
// the same thing everywhere.
// ---------------------------------------------------------------------------

/// The accepted spellings, listed in the error message so an operator who gets it
/// wrong is told exactly what is legal.
const BOOL_SPELLINGS: &str = "true/false, 1/0, yes/no, on/off";

/// Pure parser: `Some(bool)` for a recognised spelling, `None` otherwise.
/// Case-insensitive, whitespace-trimmed. An empty/whitespace-only string is NOT
/// recognised here — callers treat that as "unset" (see `resolve_bool`).
pub(crate) fn parse_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Resolution rules, factored out of the env lookup so they are unit-testable
/// without mutating process-global state.
fn resolve_bool(key: &str, raw: Option<&str>, def: bool) -> Result<bool, String> {
    match raw {
        // Unset, or set to the empty string (a very common way to "leave it
        // alone" in compose/Helm) ⇒ the documented default.
        None => Ok(def),
        Some(v) if v.trim().is_empty() => Ok(def),
        Some(v) => parse_bool(v)
            .ok_or_else(|| format!("{key}=\"{v}\" is not a boolean (expected {BOOL_SPELLINGS})")),
    }
}

/// Env-reading form that reports a bad value instead of aborting — used by
/// `validate_bools` to collect every mistake in one pass.
pub(crate) fn env_bool_checked(k: &str, def: bool) -> Result<bool, String> {
    resolve_bool(k, std::env::var(k).ok().as_deref(), def)
}

/// The call-site form: an unparseable value kills the process with a clear message
/// rather than silently resolving to `def`.
pub(crate) fn env_bool(k: &str, def: bool) -> bool {
    match env_bool_checked(k, def) {
        Ok(v) => v,
        Err(e) => crate::obs::fatal(e),
    }
}

/// Render a secret for the startup config block: never the value, only whether it
/// is set and how long it is (enough to tell "I forgot to mount the secret" from
/// "I mounted the wrong one" without putting key material in a log shipper).
fn mask(secret: &str) -> String {
    if secret.is_empty() {
        "<unset>".to_string()
    } else {
        format!("<set:{} chars>", secret.chars().count())
    }
}

pub fn log_effective(cfg: &Config) {
    tracing::info!(
        target: "boot",
        version = crate::VERSION,
        port = %cfg.port,
        bind = %cfg.bind_addr,
        server_id = %cfg.server_id,
        stmt_timeout_ms = cfg.stmt_timeout.as_millis() as u64,
        max_body_bytes = %env_str("QUEEN_MAX_BODY_BYTES", "<default>"),
        "config: server"
    );
    tracing::info!(
        target: "boot",
        data_dir = %cfg.raft_dir,
        ready_lag_ms = cfg.raft_ready_lag_ms,
        disk_high_pct = cfg.raft_disk_high_pct,
        disk_low_pct = cfg.raft_disk_low_pct,
        planner_queue_depth = cfg.raft_planner_queue_depth,
        "config: storage"
    );
    tracing::info!(
        target: "boot",
        enabled = cfg.auth.enabled,
        algorithm = %cfg.auth.algorithm,
        secret = %mask(&cfg.auth.secret),
        public_key = %mask(&cfg.auth.public_key),
        jwks_url = %cfg.auth.jwks_url,
        issuer = %cfg.auth.issuer,
        audience = %cfg.auth.audience,
        clock_skew_s = cfg.auth.clock_skew_seconds,
        skip_paths = ?cfg.auth.skip_paths,
        "config: auth"
    );
    if cfg.kafka_facade.enabled {
        tracing::info!(
            target: "boot",
            shutdown_grace_ms = cfg.kafka_facade.shutdown_grace_ms,
            "config: kafka_facade"
        );
    }
    tracing::info!(
        target: "boot",
        encryption_key = %mask(&env_str("QUEEN_ENCRYPTION_KEY", "")),
        tenancy_header = cfg.tenancy_header,
        "config: security"
    );
    tracing::info!(
        target: "boot",
        kv_require_grant = cfg.kv_require_grant,
        kv_rate = %format!(
            "{}r/{}rb/{}w/{}wb",
            cfg.kv_read_rate, cfg.kv_read_burst, cfg.kv_write_rate, cfg.kv_write_burst),
        kv_max_tenants = cfg.kv_max_tenants,
        kv_quota = %format!(
            "release {}% / hot above {}%",
            cfg.kv_quota_release_percent, cfg.kv_quota_hot_percent),
        "config: kv_timers"
    );
    // EPHEMERAL_QUEUES.md §3.8: the three CEILINGS — the only feature in the
    // broker whose overrun is paid in RAM rather than in disk or latency.
    tracing::info!(
        target: "boot",
        eph_max_bytes = cfg.ephemeral_max_bytes,
        eph_queue = %format!(
            "{}B/{}msgs",
            cfg.ephemeral_queue_max_bytes, cfg.ephemeral_queue_max_length),
        eph_lease_s = cfg.ephemeral_lease_s,
        eph_retry_limit = cfg.ephemeral_retry_limit,
        eph_implicit_idle_s = cfg.ephemeral_implicit_idle_s,
        eph_require_grant = cfg.ephemeral_require_grant,
        eph_rate = %format!("{}/{}burst msgs/s", cfg.ephemeral_rate, cfg.ephemeral_burst),
        "config: ephemeral"
    );
    tracing::info!(
        target: "boot",
        level = %env_str("RUST_LOG", &env_str("LOG_LEVEL", "info")),
        json = env_bool("QUEEN_LOG_JSON", false),
        "config: logging"
    );
}

/// Boolean knobs read OUTSIDE this module (obs.rs reads `QUEEN_LOG_JSON` before
/// the subscriber exists), validated eagerly here so EVERY boolean in the
/// broker fails at the same moment, with the same message.
const EXTERNAL_BOOL_KEYS: &[(&str, bool)] = &[("QUEEN_LOG_JSON", false)];

pub fn load() -> Config {
    for (k, def) in EXTERNAL_BOOL_KEYS {
        let _ = env_bool(k, *def);
    }

    let tenancy_header = env_bool("QUEEN_TENANCY_HEADER", false);
    // §9.4 correction 4. `tenant.rs` is explicit that with the header on, the tenant
    // is OPAQUE and validated against nothing — the trust is the cell network. With
    // KV on top of that, a client rotating the header per request gets a fresh key
    // space, a fresh (unlimited) quota lookup and a permanent entry in the broker's
    // maps, on a deployment the design itself calls real (self-hosted / dedicated
    // with no proxy). So the combination requires the operator to STATE that a proxy
    // sets the header, and the boot dies otherwise: a safety interlock must fail
    // closed, and `env_bool` already treats a typo as fatal for the same reason.
    // The unsafe thing here is NOT the KV surface, it is an opaque tenant identity
    // that nothing validates. KV only made it visible, because it is the first
    // surface addressable purely BY NAME.
    //
    // So the interlock is KEYED ON THE TENANCY MODE ALONE. It used to also test the
    // KV flag; with that flag gone the requirement is simply unconditional for anyone
    // running with the header, which is the honest shape it always had: choosing
    // QUEEN_TENANCY_HEADER=1 IS the assertion that a proxy in front sets the header
    // and strips the client's, and a deployment that cannot assert it is not a
    // deployment missing a feature, it is a deployment with an open door.
    //
    // FATAL, and there is no third option to offer. There is no longer a flag that
    // could switch KV off to make this safe — and there should not be: "the engine is
    // missing on some cells" is worse than a boot that tells you which env to set. If
    // an operator genuinely has to take the KV surface down on a live cell, that is
    // the RUNTIME kill switch (`switches.rs`, `kv_enabled` in `queen.system_state`,
    // POST /api/v1/system/kv-timers), which is a different instrument for a different
    // situation and does not make an unvalidated tenant header safe anyway.
    if tenancy_header && !env_bool("QUEEN_KV_TRUSTED_PROXY", false) {
        crate::obs::fatal(
            "QUEEN_TENANCY_HEADER=1 without QUEEN_KV_TRUSTED_PROXY=1: the tenant header \
             is opaque and validated against nothing, so any caller could read and write \
             another tenant's KV state BY NAME. Set QUEEN_KV_TRUSTED_PROXY=1 to affirm \
             that a proxy in front sets x-queen-tenant and strips the client's. If you \
             cannot affirm that, do not run with QUEEN_TENANCY_HEADER=1 — the KV surface \
             is part of the engine and cannot be switched off at boot to make an \
             unvalidated tenant identity safe (an operator pausing KV on a running cell \
             wants the runtime kill switch: POST /api/v1/system/kv-timers)",
        );
    }

    Config {
        port: env_str("PORT", "6632"),
        bind_addr: http_bind_addr(),
        stmt_timeout: Duration::from_millis(env_int("QUEEN_STMT_TIMEOUT_MS", 30000) as u64),
        // RUSTFIX item 19: honor DEFAULT_TIMEOUT (C++ name) first, then
        // POP_DEFAULT_TIMEOUT_MS, then 30000 — not the old 2000.
        pop_default_timeout_ms: env_int("DEFAULT_TIMEOUT", env_int("POP_DEFAULT_TIMEOUT_MS", 30000))
            .max(1) as u64,
        // Subscription mode applied when a pop names a consumer group but does NOT
        // send subscriptionMode. `new` = the group starts at the tail; `all` = it
        // replays the whole retained backlog.
        //
        // Two things happened here. The C++ broker honored DEFAULT_SUBSCRIPTION_MODE
        // and the shipped charts set it to `new`; the Rust port dropped the env var
        // (this file had no "subscription" in it at all) AND hardcoded the
        // per-request default to `all`. So a chart that still says `new` was
        // silently serving `all`, and the first group created after traffic resumed
        // replayed from the start. This restores the env var — same treatment as
        // DEFAULT_TIMEOUT above — and makes `new` the default it always claimed to
        // be. Set DEFAULT_SUBSCRIPTION_MODE=all to get the old Rust behavior back
        // without a rebuild.
        default_subscription_mode: normalize_subscription_mode(&env_str(
            "DEFAULT_SUBSCRIPTION_MODE",
            "new",
        )),
        auth: AuthConfig::from_env(),
        server_id: resolve_server_id(),
        kafka_facade: KafkaFacadeConfig::from_env(),
        tenancy_header,
        raft_dir: env_str("QUEEN_RAFT_DIR", "/var/lib/queen/raft"),
        raft_ready_lag_ms: env_int("QUEEN_RAFT_READY_LAG_MS", 2000).max(0) as u64,
        raft_disk_high_pct: env_pct("QUEEN_RAFT_DISK_HIGH_PCT", 85.0),
        raft_disk_low_pct: env_pct("QUEEN_RAFT_DISK_LOW_PCT", 80.0),
        raft_planner_queue_depth: env_int("QUEEN_RAFT_PLANNER_QUEUE_DEPTH", 1024).max(1) as usize,
        kv_require_grant: env_bool("QUEEN_KV_REQUIRE_GRANT", tenancy_header),
        kv_read_rate: env_int("QUEEN_KV_READ_RATE", 200).max(1) as u32,
        kv_read_burst: env_int("QUEEN_KV_READ_BURST", 400).max(1) as u32,
        kv_write_rate: env_int("QUEEN_KV_WRITE_RATE", 100).max(1) as u32,
        kv_write_burst: env_int("QUEEN_KV_WRITE_BURST", 200).max(1) as u32,
        kv_max_tenants: env_int("QUEEN_KV_MAX_TENANTS", 10_000).max(1) as usize,
        // Clamped to 1..=99: 100 would mean "release exactly at the cap", i.e. no
        // band and the oscillation the band exists to remove; 0 would mean a tenant
        // that ever blocked can only be released by emptying itself completely.
        kv_quota_release_percent: env_int("QUEEN_KV_QUOTA_RELEASE_PERCENT", 90).clamp(1, 99),
        kv_quota_hot_percent: env_int("QUEEN_KV_QUOTA_HOT_PERCENT", 80).clamp(1, 100),
        // ------------------------------------- ephemeral (EPHEMERAL_QUEUES.md §3.8)
        // `.max(1)` and not `.max(0)` on the three budgets: 0 in this engine means
        // UNLIMITED, and an operator who types `QUEEN_EPHEMERAL_MAX_BYTES=0`
        // meaning "off" would get a cell with no memory ceiling at all. There is
        // no way to spell "off" here on purpose (no boot gate — M9), so the floor
        // makes the typo harmless instead of catastrophic.
        ephemeral_max_bytes: env_int("QUEEN_EPHEMERAL_MAX_BYTES", 256 * 1024 * 1024).max(1),
        ephemeral_queue_max_bytes: env_int("QUEEN_EPHEMERAL_QUEUE_MAX_BYTES", 16 * 1024 * 1024)
            .max(1),
        ephemeral_queue_max_length: env_int("QUEEN_EPHEMERAL_QUEUE_MAX_LENGTH", 10_000).max(1),
        // Clamped to a day: a lease longer than that is indistinguishable from a
        // lost message on a class whose contents do not survive a deploy.
        ephemeral_lease_s: env_int("QUEEN_EPHEMERAL_LEASE_S", 30).clamp(1, 86_400),
        ephemeral_retry_limit: env_int("QUEEN_EPHEMERAL_RETRY_LIMIT", 5).clamp(1, 1000) as u32,
        // 0 IS legal here and means "never collect implicit queues" — unlike the
        // budgets, an operator who wants that is asking for something coherent.
        ephemeral_implicit_idle_s: env_int("QUEEN_EPHEMERAL_IMPLICIT_IDLE_S", 300).max(0),
        // DERIVED from the tenancy flag, exactly like `kv_require_grant` above: a
        // self-hosted operator never has to discover why they should configure
        // something, and a cloud cell is off until the plan grants it.
        ephemeral_require_grant: env_bool("QUEEN_EPHEMERAL_REQUIRE_GRANT", tenancy_header),
        ephemeral_rate: env_int("QUEEN_EPHEMERAL_RATE", 5000).max(1) as u32,
        ephemeral_burst: env_int("QUEEN_EPHEMERAL_BURST", 10_000).max(1) as u32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_accepted_spelling_case_insensitively() {
        for t in [
            "true", "TRUE", "True", "1", "yes", "YES", "on", "ON", " true ", "\tOn\n",
        ] {
            assert_eq!(parse_bool(t), Some(true), "{t:?} should parse as true");
        }
        for f in [
            "false", "FALSE", "False", "0", "no", "NO", "off", "OFF", " false ", "\tOff\n",
        ] {
            assert_eq!(parse_bool(f), Some(false), "{f:?} should parse as false");
        }
    }

    #[test]
    fn rejects_unrecognised_values() {
        // Including the near-misses that a permissive parser would guess at.
        for bad in [
            "maybe", "y", "n", "t", "f", "2", "-1", "enabled", "disabled", "truthy", "0.0",
        ] {
            assert_eq!(parse_bool(bad), None, "{bad:?} should not parse");
        }
    }

    #[test]
    fn unrecognised_value_is_an_error_not_the_default() {
        // The regression this whole change exists for: a bad value must NOT
        // silently resolve to the default in either direction.
        let err = resolve_bool("JWT_ENABLED", Some("maybe"), false).unwrap_err();
        assert_eq!(
            err,
            "JWT_ENABLED=\"maybe\" is not a boolean (expected true/false, 1/0, yes/no, on/off)"
        );
        // A default of `true` is just as unusable a fallback.
        assert!(resolve_bool("QUEEN_TENANCY_HEADER", Some("sure"), true).is_err());
        // The message names the variable and quotes the offending value verbatim.
        let err = resolve_bool("QUEEN_HOTLIST", Some(" Yep "), true).unwrap_err();
        assert!(err.starts_with("QUEEN_HOTLIST=\" Yep \""), "got: {err}");
    }

    #[test]
    fn unset_and_empty_use_the_default() {
        assert_eq!(resolve_bool("JWT_ENABLED", None, false), Ok(false));
        assert_eq!(resolve_bool("QUEEN_HOTLIST", None, true), Ok(true));
        // Present-but-empty (`JWT_ENABLED=` in a compose/Helm file) means "leave
        // it alone", NOT false — the old parser resolved "" to false, which for a
        // default-true knob like PG_SSL_REJECT_UNAUTHORIZED was a silent downgrade.
        assert_eq!(resolve_bool("JWT_ENABLED", Some(""), false), Ok(false));
        assert_eq!(
            resolve_bool("QUEEN_TENANCY_HEADER", Some("   "), true),
            Ok(true)
        );
    }

    #[test]
    fn set_values_win_over_the_default() {
        assert_eq!(resolve_bool("JWT_ENABLED", Some("1"), false), Ok(true));
        assert_eq!(resolve_bool("QUEEN_HOTLIST", Some("off"), true), Ok(false));
    }

    #[test]
    fn env_lookup_round_trips_through_the_same_rules() {
        // Uniquely named so parallel tests can't observe each other's env.
        const K: &str = "QUEEN_TEST_ENV_BOOL_ROUNDTRIP";
        assert_eq!(env_bool_checked(K, true), Ok(true)); // unset -> default
        std::env::set_var(K, "YES");
        assert_eq!(env_bool_checked(K, false), Ok(true));
        std::env::set_var(K, "banana");
        assert!(env_bool_checked(K, false).is_err());
        std::env::remove_var(K);
    }

    #[test]
    fn secrets_are_masked_never_printed() {
        assert_eq!(mask(""), "<unset>");
        let m = mask("hunter2-super-secret");
        assert!(!m.contains("hunter2"), "mask leaked the secret: {m}");
        assert_eq!(m, "<set:20 chars>");
    }
}

#[cfg(test)]
mod subscription_mode_tests {
    use super::normalize_subscription_mode;

    /// `new-only` is documented as an alias of `new` by the Go SDK constant, the
    /// CLI --from-mode help and the JS README, but the SQL compares the literal
    /// exactly, so it used to reach `= 'new'`, miss, and replay the whole backlog:
    /// the exact opposite of what it advertises.
    #[test]
    fn documented_aliases_of_new_all_resolve_to_new() {
        for raw in [
            "new",
            "new-only",
            "new_only",
            "newonly",
            "NEW",
            " New-Only ",
        ] {
            assert_eq!(normalize_subscription_mode(raw), "new", "{raw}");
        }
    }

    /// Anything unrecognized keeps resolving to `all`: that is both the historical
    /// fallthrough and the safe direction. Replaying too much is recoverable;
    /// skipping messages is not.
    #[test]
    fn everything_else_resolves_to_all() {
        for raw in ["all", "ALL", "from_beginning", "earliest", "", "garbage"] {
            assert_eq!(normalize_subscription_mode(raw), "all", "{raw}");
        }
    }

    /// The output must be one of the two spellings the SQL understands, never a
    /// pass-through of the caller's text.
    #[test]
    fn output_is_always_one_of_the_two_sql_literals() {
        for raw in ["new", "all", "whatever", "new-only"] {
            let m = normalize_subscription_mode(raw);
            assert!(m == "new" || m == "all", "{raw} -> {m}");
        }
    }
}
