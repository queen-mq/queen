//! Env-driven configuration. Same parsing semantics as the broker's config.rs:
//! only the literal "true" is truthy, empty strings are preserved, ints fall back
//! on parse failure.

use std::env;

/// Join a bind host and a port into an authority, bracketing an IPv6 literal.
/// Unbracketed, `::1` + `6711` is `::1:6711`, which is NOT a `SocketAddr` —
/// that grammar requires the brackets — so it falls through to the name
/// resolver instead of the parse path. A hostname passes through untouched and
/// is resolved at bind time.
pub fn host_port(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Exit on a bind host that carries a port, or that is explicitly empty —
/// both would be joined with the port into an address nothing listens on, and
/// the failure would quote a string no operator ever wrote. Checked here, in
/// `load`, so it lands with the rest of the boot validation and before any
/// TLS material is read. An IP literal (v4 or v6) or a hostname is accepted;
/// whether a hostname resolves is still the bind's problem.
fn checked_bind_addr(key: &str, v: String) -> String {
    if v.is_empty() {
        tracing::error!("{key} is set to the empty string — give it a host or IP, or unset it for 0.0.0.0");
        std::process::exit(1);
    }
    if v.parse::<std::net::IpAddr>().is_err() && v.contains(':') {
        tracing::error!(
            "{key}={v} carries a port — set the host or IP only, the port comes from QUEEN_PROXY_PORT"
        );
        std::process::exit(1);
    }
    v
}

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

/// A Host header value with its `:port` stripped. Only strips when what follows
/// the last colon is all digits, so an IPv6 literal (`[::1]`) and any other
/// colon survive untouched.
pub fn host_without_port(host: &str) -> &str {
    let host = host.trim();
    match host.rsplit_once(':') {
        Some((h, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => h,
        _ => host,
    }
}

/// The comparable form of a Host header: trimmed, `:port` stripped, and the DNS
/// ROOT LABEL dropped — `try.queenmq.cloud.` and `try.queenmq.cloud` are the
/// same name and must not be two different routing decisions.
///
/// One implementation, shared by `Config::is_shared_host` and by cache.rs's
/// `slug_from_host`: "which host is this request for" must have exactly one
/// answer, or a host could be shared for one of them and a slug for the other.
/// That is not hypothetical — the fully-qualified form is legal in a Host
/// header, some resolvers and clients emit it, and it survives Host-forwarding
/// proxies. Before this stripped it, `Host: try.queenmq.cloud.` missed
/// `is_shared_host`, fell through to `cache::resolve_host`, and was absorbed by
/// `QUEEN_PROXY_DEFAULT_CLUSTER` (or answered 421 where there is no default) —
/// a one-character variant defeating the classification the shared-host feature
/// rests on.
///
/// Repeated dots (`host..`) are all dropped: the empty labels they name are not
/// valid either way, and leaving one behind would just move the mismatch.
pub fn canonical_host(host: &str) -> &str {
    host_without_port(host).trim_end_matches('.')
}

/// Normalize `QUEEN_PROXY_SHARED_HOSTS` entries: `csv_lower` has already
/// trimmed and lowercased them, so all that is left is tolerating an operator
/// who pasted a `host:port` out of a browser bar or a `host.` out of a zone
/// file. `canonical_host`, the same one every Host header goes through, so the
/// configured side and the request side cannot disagree. Pure, so the shape is
/// pinned by tests rather than by reading `load`.
fn normalize_shared_hosts(raw: Vec<String>) -> Vec<String> {
    raw.into_iter()
        .map(|h| canonical_host(&h).to_string())
        .filter(|h| !h.is_empty())
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

// ---------------------------------------------------------------------------
// identity material — what this proxy is expected to do with session tokens
// ---------------------------------------------------------------------------

/// The OPTIONAL separately-supplied Ed25519 PUBLIC-key PEM: it lets a host hold
/// only public material and verify tokens some other host minted.
///
/// Read here rather than at its use site so the boot gate below and the signer
/// `auth::Keys` builds cannot disagree about whether it is set — a divergence
/// there would print one mode and run another.
pub fn jwt_ed25519_pub_pem() -> Option<String> {
    env::var("QUEEN_PROXY_JWT_ED25519_PUB_PEM").ok().filter(|s| !s.trim().is_empty())
}

/// The host of an absolute URL: scheme dropped, userinfo dropped, path/query/
/// fragment cut. What is left still goes through `canonical_host` for the
/// `:port` and root-label stripping, so a configured audience and a Host header
/// are normalized by exactly one implementation.
fn host_from_url(url: &str) -> &str {
    let url = url.trim();
    let after_scheme = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    let authority = after_scheme.split(['/', '?', '#']).next().unwrap_or("");
    authority.rsplit_once('@').map(|(_, host)| host).unwrap_or(authority)
}

/// The audience this proxy accepts on a session token, or `None` for "accept
/// whatever audience a token names".
///
/// A verify-only cell holds the fleet's public key, so it accepts every token
/// the control plane minted for ANY of its siblings: one leaked token replays
/// across the whole fleet. `aud` is what closes that, and the rule is
/// deliberately one-sided (see `auth::audience_ok`): a token that names an
/// audience must name OURS, a token that names none is accepted exactly as it
/// is today. That asymmetry is what lets this ship before any minter sets the
/// claim, which is the only way to roll it out without a flag day.
///
/// `QUEEN_PROXY_JWT_AUD` when set, else the host of `QUEEN_PROXY_PUBLIC_URL`,
/// which is the name this proxy already answers to: the fleet-wide default
/// needs no per-cell variable at all. Both sides are lowercased here and compared
/// case-insensitively at verify time, because the value is a hostname and
/// `Cell-A.example.com` refusing `cell-a.example.com` would be a fail-CLOSED
/// typo: every session on the cell, 401, with the data plane green.
///
/// Pure, and read through this one function for the same reason
/// `jwt_ed25519_pub_pem` is: the boot log names the audience the verifier
/// actually enforces.
pub fn resolve_jwt_audience(explicit: Option<String>, public_url: Option<&str>) -> Option<String> {
    let explicit = explicit.map(|v| v.trim().to_ascii_lowercase()).filter(|v| !v.is_empty());
    if explicit.is_some() {
        return explicit;
    }
    let derived = canonical_host(host_from_url(public_url?)).to_ascii_lowercase();
    (!derived.is_empty()).then_some(derived)
}

/// Default link text for `auth_portal_url`, in the cadence the sign-in page
/// already uses for its provider buttons ("Continue with Google").
pub const AUTH_PORTAL_LABEL: &str = "Continue to the control plane";

/// Validate `QUEEN_PROXY_AUTH_PORTAL_URL`: where a VERIFY-ONLY host points
/// someone who lands on `/auth/login`, since sign-in happens on the control
/// plane that mints the sessions this host only verifies (oauth.rs's
/// `render_login`).
///
/// http(s) only, and no whitespace or control characters. The value is rendered
/// into an `href` on a page served unauthenticated to anyone who can reach the
/// proxy, so a `javascript:` or `data:` value would be operator-supplied script
/// on the one page that exists before any session does.
///
/// Deliberately NOT fatal, unlike the bind address and the JWT gate: this is one
/// link on one page, and stopping the data plane over it would be out of all
/// proportion. A refused value renders the same notice an operator who never set
/// the variable gets, an explanation with no link, and the warning is what makes
/// it findable.
pub fn resolve_auth_portal_url(raw: Option<String>) -> Option<String> {
    let v = raw?.trim().to_string();
    let scheme_ok = {
        let lower = v.to_ascii_lowercase();
        lower.starts_with("https://") || lower.starts_with("http://")
    };
    if scheme_ok && !v.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Some(v);
    }
    tracing::warn!(
        target: "config",
        value = %v,
        "QUEEN_PROXY_AUTH_PORTAL_URL is not a plain http(s) URL and is IGNORED: the verify-only \
         sign-in notice will explain without linking anywhere."
    );
    None
}

/// What this proxy is expected to DO with session tokens, decided from the
/// environment alone.
///
/// The distinction that matters is whether a human can log in HERE. The console
/// (`/auth/login`, the SPA gate in webapp.rs, `/api/console/*`) is mounted
/// unconditionally, but a login can only ever succeed against a real
/// `queen_proxy.users` row — so a pxdb is what turns the console from decoration
/// into a surface that must be able to mint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JwtMode {
    /// Issues session tokens itself. Needs a signer that can mint AND verify.
    Mint,
    /// Accepts session tokens minted elsewhere (the auth host) and issues none.
    /// Needs a verification key, and nothing else.
    VerifyOnly,
    /// No user table at all — dev-static / pure data plane. Nothing can log in,
    /// so no JWT material is required and none is missed.
    NoIdentity,
}

impl JwtMode {
    pub fn as_str(self) -> &'static str {
        match self {
            JwtMode::Mint => "mint",
            JwtMode::VerifyOnly => "verify-only",
            JwtMode::NoIdentity => "no-identity",
        }
    }
}

/// What the environment supplied (which knobs are set) and what the signer built
/// from it can actually do (`auth::Keys::can_mint` / `can_verify`). Booleans
/// only: nothing here can carry key material into a log line.
#[derive(Clone, Copy, Debug)]
pub struct JwtMaterial {
    pub has_pxdb: bool,
    pub ed_private: bool,
    pub ed_public: bool,
    pub hs_secret: bool,
    pub can_mint: bool,
    pub can_verify: bool,
}

/// Detected mode plus what to say about it. `fatal` is a boot refusal.
#[derive(Clone, Debug)]
pub struct JwtBoot {
    pub mode: JwtMode,
    pub fatal: Option<String>,
    pub warn: Option<String>,
}

/// Classify the identity material and refuse the combinations that cannot serve
/// the mode they are in. Pure, so the whole matrix is pinned by tests rather
/// than discovered on a cell.
///
/// The failure this exists to stop was measured on the first cloud cell: a proxy
/// with a pxdb and no JWT secret boots, passes every health check, serves the
/// API-key data plane perfectly — and answers 500 to every single console login,
/// because `oauth::establish_session` cannot mint. Nothing in the process says
/// so until a human tries to log in.
///
/// The rules, and they are deliberately about CAPABILITY, not about which knob
/// is set:
///   * mint mode must be able to mint (or every login 500s) AND to verify (or
///     every login succeeds and bounces straight back to the login page);
///   * verify-only mode must be able to verify (or every session 401s);
///   * no-identity mode requires nothing.
///
/// SUPPLIED-BUT-BROKEN IS FATAL; ABSENT IS NOT. A proxy with a pxdb and NO JWT
/// material at all is a legal, documented shape — the API-key-only self-hosted
/// proxy that never serves a console login (webdoc `deploy/proxy.mdx`:
/// "`PXDB_HOST` is the only variable it refuses to start without"). Refusing to
/// boot it would break that contract on upgrade for every external self-hoster,
/// for a console they do not use. So that one case is the loud warning spec §9b
/// allows ("fail fast at boot, or warn loudly"), and `oauth::establish_session`
/// names the same two variables if anyone ever does try to log in. Every case
/// where material WAS supplied and cannot serve its mode stays fatal: an
/// operator who set a key meant to use it, and a signer that silently does
/// nothing is the failure this gate exists for.
pub fn jwt_boot(m: JwtMaterial) -> JwtBoot {
    let mode = if !m.has_pxdb {
        JwtMode::NoIdentity
    } else if m.ed_public && !m.ed_private && !m.hs_secret {
        // The operator supplied a public key and nothing else: an explicit
        // "somebody else mints". Any private material outranks it.
        JwtMode::VerifyOnly
    } else {
        JwtMode::Mint
    };

    // Contradictory material. Not fatal — HS256 is a complete, working signer —
    // but silently ignoring a public key an operator deliberately supplied is
    // how a fleet ends up half-migrated to EdDSA without anyone noticing.
    let mut warn = (m.ed_public && m.hs_secret && !m.ed_private).then(|| {
        "QUEEN_PROXY_JWT_ED25519_PUB_PEM is IGNORED while QUEEN_PROXY_JWT_SECRET is set: HS256 wins, \
         and this proxy mints and verifies with the shared secret. Unset QUEEN_PROXY_JWT_SECRET to \
         make this a verify-only host."
            .to_string()
    });

    // Nothing was supplied at all. Mint mode by classification (there is a
    // pxdb, so there is a user table and a console), but this is also exactly
    // the shape of an API-key-only proxy, so it is reported rather than
    // refused. The wording is the fatal one's, in the imperative rather than
    // the past tense, and it must keep naming the way out.
    let nothing_supplied = !m.ed_private && !m.ed_public && !m.hs_secret;
    if mode == JwtMode::Mint && nothing_supplied && !m.can_mint {
        warn = Some(
            "NO JWT SIGNER is configured and PXDB_HOST is set: the console login at /auth/login \
             will answer 503 \"no JWT signer\" while /healthz and the whole API-key data plane stay \
             green. This is fine for an API-KEY-ONLY proxy and is why it is not a boot refusal. To \
             serve the console, set QUEEN_PROXY_JWT_ED25519_PEM (Ed25519 private key PEM — \
             production) or QUEEN_PROXY_JWT_SECRET (HS256 — dev); to verify sessions minted by an \
             auth host instead, set QUEEN_PROXY_JWT_ED25519_PUB_PEM."
                .to_string(),
        );
    }

    let fatal = match mode {
        JwtMode::NoIdentity => None,
        JwtMode::VerifyOnly if !m.can_verify => Some(
            "JWT mode VERIFY-ONLY detected (QUEEN_PROXY_JWT_ED25519_PUB_PEM is set; \
             QUEEN_PROXY_JWT_ED25519_PEM and QUEEN_PROXY_JWT_SECRET are not) but the public key is \
             UNUSABLE: it did not parse as an Ed25519 public key. Expected a PEM \"PUBLIC KEY\" block \
             holding either the 44-byte SPKI DER or the raw 32-byte key. Every session would answer \
             401 while the API-key data plane stayed green."
                .to_string(),
        ),
        JwtMode::VerifyOnly => None,
        // Reported by the warning above instead: absent material is a shape,
        // not a mistake. Only material that WAS supplied and does not work
        // reaches the refusal below.
        // (`nothing_supplied` with a capable signer is not a reachable state —
        // a signer is built FROM this material — but the exemption is written
        // over the whole arm so the property stays unconditional: this process
        // is never refused for material it was not given.)
        JwtMode::Mint if nothing_supplied => None,
        JwtMode::Mint if !m.can_mint => Some(format!(
            "JWT mode MINT detected (PXDB_HOST is set, so this proxy serves the console login at \
             /auth/login against a real user table and must issue session tokens) but NO USABLE JWT \
             SIGNER is configured{}. Every login would answer 503 \"no JWT signer\" while \
             /healthz and the whole API-key data plane stayed green. Set QUEEN_PROXY_JWT_ED25519_PEM \
             (Ed25519 private key PEM — production) or QUEEN_PROXY_JWT_SECRET (HS256 — dev). To run \
             this proxy as a verify-only host instead, set QUEEN_PROXY_JWT_ED25519_PUB_PEM and leave \
             both of those unset.",
            if m.ed_private {
                ": QUEEN_PROXY_JWT_ED25519_PEM is set but did not load as an Ed25519 private key"
            } else {
                ""
            }
        )),
        JwtMode::Mint if !m.can_verify => Some(
            "JWT mode MINT detected, and the signer CANNOT VERIFY WHAT IT MINTS: the Ed25519 public \
             key could not be derived from QUEEN_PROXY_JWT_ED25519_PEM. Every session token this \
             proxy issued would be rejected by its own verifier — a login that succeeds and bounces \
             straight back to /auth/login. Supply the public key explicitly in \
             QUEEN_PROXY_JWT_ED25519_PUB_PEM, or fix the private key PEM."
                .to_string(),
        ),
        JwtMode::Mint => None,
    };

    JwtBoot { mode, fatal, warn }
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
    /// `PXDB_SSL_ROOT_CERT` — the PEM **CONTENT** of the CA that signed the
    /// pxdb's certificate, not a path to it (same family shape as
    /// `PXDB_PASSWORD` and `QUEEN_PROXY_JWT_ED25519_PEM`: the value IS the
    /// material). Set it and the chain is verified against exactly that CA
    /// instead of the compiled-in Mozilla set, which is what a managed
    /// PostgreSQL on a private CA needs — before this knob existed the only way
    /// to connect at all was `PXDB_SSL_REJECT_UNAUTHORIZED=false`, i.e.
    /// encryption with no authentication.
    ///
    /// A supplied CA REPLACES the Mozilla set and outranks
    /// `PXDB_SSL_REJECT_UNAUTHORIZED=false`; both rules, and why, are in
    /// `pgtls.rs`'s header — the same file, byte for byte, that the broker
    /// uses for `PG_SSL_ROOT_CERT`. Malformed PEM is fatal at boot, never a
    /// silent fall-back.
    ///
    /// **Trap:** a multi-line value does not survive `docker --env-file` or
    /// systemd's `EnvironmentFile` — they stop at the first newline. Use
    /// `-e PXDB_SSL_ROOT_CERT="$(cat ca.pem)"`, a compose block scalar, or a
    /// Kubernetes secret; or put the PEM on one line with `\n` for the
    /// newlines, which the parser un-escapes.
    pub ssl_root_cert: Option<String>,
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

    /// What this pool ACTUALLY trusts, as one word for the boot log, from the
    /// same pure functions `db.rs` and `cache.rs` feed the connector — so the
    /// line cannot describe a policy other than the one installed. The broker
    /// prints the identical pair for its own PostgreSQL link.
    pub fn trust_label(&self) -> &'static str {
        crate::pgtls::trust_label(
            self.use_ssl,
            self.ssl_root_cert.is_some(),
            self.ssl_reject_unauthorized,
        )
    }

    /// Is the pxdb link AUTHENTICATED, not merely encrypted?
    pub fn link_authenticated(&self) -> bool {
        crate::pgtls::link_authenticated(
            self.use_ssl,
            self.ssl_root_cert.is_some(),
            self.ssl_reject_unauthorized,
        )
    }
}

/// `PXDB_SSL_ROOT_CERT` as raw PEM, or `None`. Whitespace-only counts as unset —
/// an env file that leaves the variable blank must mean "not configured", not
/// "configured with nothing", which would otherwise boot-fail every deployment
/// that templates the variable in unconditionally.
///
/// Read through this ONE function so `Config::load`'s boot gate and the two
/// connector sites (`db::create_pool`, `cache.rs`'s LISTEN connection) cannot
/// disagree about whether a CA is configured.
pub fn pxdb_ssl_root_cert() -> Option<String> {
    // Same shape as `jwt_ed25519_pub_pem` above. (The broker's twin reads its
    // own variable through `env_str` instead, because ITS reference page is
    // scraped from the env_* call sites; the proxy's is hand-written, in
    // webdoc/src/content/docs/deploy/proxy.mdx.)
    env::var("PXDB_SSL_ROOT_CERT").ok().filter(|v| !v.trim().is_empty())
}

/// Validate the CA material and report the anchor count, WITHOUT exiting, so it
/// is unit-testable and so `load` keeps every `exit(1)` in one place.
///
/// Validated whenever the variable is SET, even with `PXDB_USE_SSL=false` or no
/// `PXDB_HOST` at all: a variable that is set is a statement of intent, and
/// finding out at boot that the PEM was truncated by an env file beats finding
/// out at the next TLS handshake.
pub fn check_pxdb_ssl_root_cert() -> Result<Option<usize>, String> {
    match pxdb_ssl_root_cert() {
        None => Ok(None),
        Some(pem) => match crate::pgtls::root_store_from_pem(&pem) {
            Ok(roots) => Ok(Some(roots.len())),
            Err(e) => Err(format!(
                "PXDB_SSL_ROOT_CERT is set but unusable: {e}. It takes the PEM CONTENT of the CA \
                 certificate that signed the pxdb's certificate — not a path, and not a \
                 fingerprint. Fix it or unset it; it is not ignored, because a database link \
                 believed to be verified and silently is not is the failure this variable exists \
                 to remove."
            )),
        },
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
    /// Host the listener binds — `QUEEN_PROXY_BIND_ADDR`, default `0.0.0.0`:
    /// every interface, which is what the address was hardcoded to before the
    /// knob existed, so an upgrade changes nothing until someone sets it.
    /// HOST only — the port stays `QUEEN_PROXY_PORT`, and `load` refuses a
    /// value carrying one rather than joining it into an address nothing
    /// listens on. Applies to the TLS listener too: there is one socket here.
    pub bind_addr: String,
    pub pxdb: Option<PxdbConfig>,
    /// false = shadow mode: limit decisions are computed+logged+metered but not enforced.
    pub enforce: bool,
    /// Dev only: skip authentication entirely (never set in cloud).
    pub dev_insecure: bool,
    pub dev_static: Option<DevStaticCluster>,
    /// Dev/demo only: slug to fall back to when Host resolves to no cluster
    /// (browsers on localhost). Never set in cloud.
    pub default_cluster: Option<String>,
    /// `QUEEN_PROXY_SHARED_HOSTS` — hostnames that front MANY clusters, where
    /// the cluster is resolved from the CREDENTIAL instead of from the Host
    /// label: an API key names its own cluster (`queen_proxy.api_keys`), a
    /// human session names one with `x-queen-act-cluster` and is checked
    /// against `cluster_roles`. On such a host a missing or invalid key is a
    /// 401, never a 421 — there is no host label left to be misdirected.
    ///
    /// Full hostnames (`try.queenmq.cloud`), comma-separated, lowercased, any
    /// `:port` stripped. Deliberately NOT first-labels: `try` would silently
    /// make every `try.*` domain this proxy answers on shared too.
    ///
    /// This is a different feature from `default_cluster` and cannot interact
    /// with it: `acting::resolve_route` checks this list FIRST and never
    /// reaches `cache::resolve_host`, where the default lives. One maps every
    /// unresolvable Host to ONE cluster; this maps a named host to WHICHEVER
    /// cluster the credential belongs to.
    ///
    /// Empty (the default) = every host keeps today's behaviour exactly.
    pub shared_hosts: Vec<String>,
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
    /// The `aud` a session token may name, resolved by `resolve_jwt_audience`.
    /// `None` means no audience is enforced, which is what every proxy that
    /// sets neither `QUEEN_PROXY_JWT_AUD` nor `QUEEN_PROXY_PUBLIC_URL` gets.
    pub jwt_audience: Option<String>,
    pub jwt_hs_secret: Option<String>,
    pub jwt_ed25519_pem: Option<String>,
    pub jwt_ttl_s: u64,
    pub cookie_name: String,
    pub cookie_domain: Option<String>,
    pub auth_host_mode: bool,
    pub public_base_url: Option<String>,
    /// `QUEEN_PROXY_AUTH_PORTAL_URL`, validated by `resolve_auth_portal_url`:
    /// the control plane a VERIFY-ONLY host sends people to, because it mints
    /// the sessions this host only verifies. `None` (the default, and every
    /// proxy that mints its own) leaves the notice without a link.
    pub auth_portal_url: Option<String>,
    /// Link text for `auth_portal_url` (`QUEEN_PROXY_AUTH_PORTAL_LABEL`), so an
    /// operator whose control plane has a name of its own can use it. Ignored
    /// when there is no URL to label.
    pub auth_portal_label: String,
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
    /// Does this Host header name a shared host? Port-insensitive and
    /// case-insensitive on both sides; an empty list short-circuits to false,
    /// which is what keeps the feature entirely inert when it is not configured.
    /// Does this LISTENER front many tenants on a shared name at all?
    ///
    /// Decision z's property — "a missing or invalid credential is a 401, never
    /// a 421" — is a property of the front door, not of one exact string. A
    /// listener with a shared host behind a wildcard DNS record answers every
    /// `*.queenmq.cloud`, so if the non-shared Host values beside it kept
    /// answering 401 for a slug that exists and 421 for one that does not, an
    /// unauthenticated caller could read off the whole cluster-slug list (and,
    /// through the pre-auth status gate, each one's suspended/deleting state).
    /// Slugs are CP-assigned precisely so they cannot be squatted or phished.
    ///
    /// So when this is true, gateway.rs holds every cluster-specific answer —
    /// the 421, the status gate, the plan gates — until the caller has proved a
    /// live credential. When it is false (every self-hosted proxy that never
    /// sets the variable) nothing changes at all.
    pub fn has_shared_hosts(&self) -> bool {
        !self.shared_hosts.is_empty()
    }

    pub fn is_shared_host(&self, host: &str) -> bool {
        if self.shared_hosts.is_empty() {
            return false;
        }
        let host = canonical_host(host).to_ascii_lowercase();
        !host.is_empty() && self.shared_hosts.iter().any(|h| *h == host)
    }

    pub fn load() -> Config {
        let pxdb = env_opt("PXDB_HOST").map(|host| PxdbConfig {
            host,
            port: env_u64("PXDB_PORT", 5432) as u16,
            user: env_str("PXDB_USER", "postgres"),
            password: env_str("PXDB_PASSWORD", ""),
            dbname: env_str("PXDB_DB", "queen_proxy"),
            use_ssl: env_bool("PXDB_USE_SSL", false),
            ssl_reject_unauthorized: env_bool("PXDB_SSL_REJECT_UNAUTHORIZED", true),
            ssl_root_cert: pxdb_ssl_root_cert(),
            pool_size: env_u64("PXDB_POOL_SIZE", 16) as usize,
            timeout_ms: env_u64("PXDB_TIMEOUT_MS", 5_000),
        });

        // --- pxdb TLS trust (spec §0: PXDB_SSL_ROOT_CERT) ------------------
        // Malformed CA material is FATAL here, beside the bind-address and
        // shared-hosts gates. It is never a fall-back to the compiled-in
        // Mozilla set: falling back would be a downgrade with no signal, which
        // is exactly what the default of PXDB_SSL_REJECT_UNAUTHORIZED=true
        // exists to prevent. The advisory below covers the combinations that
        // are legal but almost certainly not what was meant; its wording lives
        // in pgtls.rs and the broker prints the identical sentence for PG_*.
        let ca_anchors = match check_pxdb_ssl_root_cert() {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(target: "config", "{e}");
                std::process::exit(1);
            }
        };
        match &pxdb {
            Some(px) => {
                if let Some(msg) = crate::pgtls::boot_advisory(
                    px.use_ssl,
                    px.ssl_root_cert.is_some(),
                    px.ssl_reject_unauthorized,
                    crate::pgtls::PROXY_VARS,
                ) {
                    tracing::warn!(target: "config", anchors = ca_anchors, "{msg}");
                }
            }
            // No pxdb at all: the CA describes a connection this process will
            // never open. Its own sentence, because "the connection is
            // plaintext" would be a lie about a connection that does not exist.
            None if ca_anchors.is_some() => tracing::warn!(
                target: "config",
                "PXDB_SSL_ROOT_CERT is set but PXDB_HOST is not — this proxy opens no pxdb \
                 connection, so the CA is not used by anything."
            ),
            None => {}
        }
        let dev_static = env_opt("QUEEN_PROXY_DEV_CELL_URL").map(|cell_url| DevStaticCluster {
            cell_url,
            cell_token: env_opt("QUEEN_PROXY_DEV_CELL_TOKEN"),
            broker_tenant: env_str("QUEEN_PROXY_DEV_TENANT", DEFAULT_TENANT_UUID),
        });
        // A shared host resolves its cluster out of `queen_proxy.api_keys` and
        // `queen_proxy.cluster_roles`. Without a pxdb neither table exists, so
        // every request to a listed host would answer 401 forever — a silent,
        // unexplainable failure. Boot-fatal instead, like the bind address: a
        // half-configured feature is a misconfiguration, not a degraded mode.
        let shared_hosts = normalize_shared_hosts(csv_lower("QUEEN_PROXY_SHARED_HOSTS"));
        if !shared_hosts.is_empty() && pxdb.is_none() {
            tracing::error!(
                hosts = ?shared_hosts,
                "QUEEN_PROXY_SHARED_HOSTS is set but PXDB_HOST is not — a shared host resolves its \
                 cluster from queen_proxy.api_keys/cluster_roles, and there is no such table \
                 without a pxdb. Every request to these hosts would answer 401."
            );
            std::process::exit(1);
        }
        // Read before the literal because the JWT audience defaults to this
        // URL's host: one read, so the value the verifier enforces cannot drift
        // from the one the OAuth callbacks are built on.
        let public_base_url = env_opt("QUEEN_PROXY_PUBLIC_URL");
        Config {
            port: env_u64("QUEEN_PROXY_PORT", 6711) as u16,
            bind_addr: checked_bind_addr(
                "QUEEN_PROXY_BIND_ADDR",
                env_str("QUEEN_PROXY_BIND_ADDR", "0.0.0.0"),
            ),
            pxdb,
            enforce: env_bool("QUEEN_PROXY_ENFORCE", false),
            dev_insecure: env_bool("QUEEN_PROXY_DEV_INSECURE", false),
            dev_static,
            default_cluster: env_opt("QUEEN_PROXY_DEFAULT_CLUSTER"),
            shared_hosts,
            send_tenant_header: env_bool("QUEEN_PROXY_TENANT_HEADER", true),
            max_body_bytes: env_u64("QUEEN_PROXY_MAX_BODY_BYTES", 16 * 1024 * 1024) as usize,
            default_max_batch_items: env_u64("QUEEN_PROXY_MAX_BATCH_ITEMS", 10_000),
            upstream_connect_timeout_ms: env_u64("QUEEN_PROXY_UPSTREAM_CONNECT_TIMEOUT_MS", 5_000),
            upstream_request_timeout_ms: env_u64("QUEEN_PROXY_UPSTREAM_TIMEOUT_MS", 35_000),
            longpoll_margin_ms: env_u64("QUEEN_PROXY_LONGPOLL_MARGIN_MS", 10_000),
            longpoll_max_ms: env_u64("QUEEN_PROXY_LONGPOLL_MAX_MS", 90_000),
            jwt_issuer: env_str("QUEEN_PROXY_JWT_ISS", "queen-proxy"),
            jwt_audience: resolve_jwt_audience(
                env_opt("QUEEN_PROXY_JWT_AUD"),
                public_base_url.as_deref(),
            ),
            jwt_hs_secret: env_opt("QUEEN_PROXY_JWT_SECRET"),
            jwt_ed25519_pem: env_opt("QUEEN_PROXY_JWT_ED25519_PEM"),
            jwt_ttl_s: env_u64("QUEEN_PROXY_JWT_TTL_S", 86_400),
            cookie_name: env_str("QUEEN_PROXY_COOKIE_NAME", "queen_session"),
            cookie_domain: env_opt("QUEEN_PROXY_COOKIE_DOMAIN"),
            auth_host_mode: env_bool("QUEEN_PROXY_AUTH_HOST", false),
            public_base_url,
            auth_portal_url: resolve_auth_portal_url(env_opt("QUEEN_PROXY_AUTH_PORTAL_URL")),
            auth_portal_label: env_opt("QUEEN_PROXY_AUTH_PORTAL_LABEL")
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| AUTH_PORTAL_LABEL.to_string()),
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

/// A Config with nothing set but the shared-host list — every other field is an
/// inert placeholder, like meter.rs's `cfg_with_dir`. Built by hand rather than
/// through `load()` + `set_var`, because the tests that use it run concurrently
/// in-process and mutating a process-global env var from parallel threads is a
/// real data race. Shared with acting.rs's routing tests, which need a whole
/// `St` around it.
#[cfg(test)]
pub(crate) fn test_config(hosts: &[&str]) -> Config {
    Config {
        port: 0,
        bind_addr: "0.0.0.0".to_string(),
        pxdb: None,
        enforce: false,
        dev_insecure: false,
        dev_static: None,
        default_cluster: None,
        shared_hosts: normalize_shared_hosts(hosts.iter().map(|h| h.to_string()).collect()),
        send_tenant_header: true,
        max_body_bytes: 1024,
        default_max_batch_items: 100,
        upstream_connect_timeout_ms: 1000,
        upstream_request_timeout_ms: 1000,
        longpoll_margin_ms: 1000,
        longpoll_max_ms: 1000,
        jwt_issuer: "test".to_string(),
        jwt_audience: None,
        jwt_hs_secret: None,
        jwt_ed25519_pem: None,
        jwt_ttl_s: 60,
        cookie_name: "test".to_string(),
        cookie_domain: None,
        auth_host_mode: false,
        public_base_url: None,
        auth_portal_url: None,
        auth_portal_label: AUTH_PORTAL_LABEL.to_string(),
        operator_enabled: false,
        google_client_id: None,
        google_client_secret: None,
        google_allowed_domains: Vec::new(),
        autoprovision_default_role: "viewer".to_string(),
        github_client_id: None,
        github_client_secret: None,
        meter_flush_ms: 1000,
        spool_dir: "/tmp".to_string(),
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

    // ---- the verify-only sign-in portal -------------------------------------

    fn portal(v: &str) -> Option<String> {
        resolve_auth_portal_url(Some(v.to_string()))
    }

    #[test]
    fn auth_portal_url_takes_http_and_https_and_trims() {
        assert_eq!(portal("https://auth.queenmq.cloud/login").as_deref(), Some("https://auth.queenmq.cloud/login"));
        // Plain http is a real deployment (a control plane on a private
        // network), so it is not refused here.
        assert_eq!(portal("http://control.internal:8080/").as_deref(), Some("http://control.internal:8080/"));
        assert_eq!(portal("  https://auth.test/login\n").as_deref(), Some("https://auth.test/login"));
        assert_eq!(portal("HTTPS://Auth.Test/Login").as_deref(), Some("HTTPS://Auth.Test/Login"));
        assert_eq!(resolve_auth_portal_url(None), None, "unset is the default shape");
    }

    #[test]
    fn auth_portal_url_refuses_anything_that_is_not_a_plain_http_url() {
        // The value lands in an href on the ONE page served with no session at
        // all. A script URL there would be operator-supplied script in front of
        // every unauthenticated visitor.
        assert_eq!(portal("javascript:alert(1)"), None);
        assert_eq!(portal("data:text/html,<script>alert(1)</script>"), None);
        assert_eq!(portal("  javascript:alert(1)"), None, "trimming does not launder a scheme");
        // Relative and scheme-less values would resolve against this host, which
        // is exactly the host that cannot sign anyone in.
        assert_eq!(portal("/auth/login"), None);
        assert_eq!(portal("auth.example.test"), None);
        assert_eq!(portal(""), None);
        // A header-splitting or attribute-breaking value never reaches the page.
        assert_eq!(portal("https://ok.test/a b"), None);
        assert_eq!(portal("https://ok.test/\nX"), None);
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

    // ---- shared hosts (decision z) -----------------------------------------

    fn cfg_with_shared(hosts: &[&str]) -> Config {
        super::test_config(hosts)
    }

    #[test]
    fn host_without_port_strips_only_a_real_port() {
        assert_eq!(host_without_port("try.queenmq.cloud:6711"), "try.queenmq.cloud");
        assert_eq!(host_without_port("try.queenmq.cloud"), "try.queenmq.cloud");
        assert_eq!(host_without_port("  try.queenmq.cloud:443  "), "try.queenmq.cloud");
        // IPv6 literal: the last colon is inside the brackets, not a port.
        assert_eq!(host_without_port("[::1]"), "[::1]");
        assert_eq!(host_without_port("[::1]:6711"), "[::1]");
        // Not a port -> nothing is stripped, rather than a silent wrong guess.
        assert_eq!(host_without_port("weird:notaport.example.com"), "weird:notaport.example.com");
    }

    #[test]
    fn canonical_host_drops_the_dns_root_label() {
        // A fully-qualified Host is legal, is emitted by some clients and
        // resolvers, and names the SAME host. It must not be a second routing
        // decision (it used to miss `is_shared_host` and land on the default
        // cluster instead).
        assert_eq!(canonical_host("try.queenmq.cloud."), "try.queenmq.cloud");
        assert_eq!(canonical_host("try.queenmq.cloud.:443"), "try.queenmq.cloud");
        assert_eq!(canonical_host("  try.queenmq.cloud.  "), "try.queenmq.cloud");
        // Degenerate shapes stay degenerate rather than becoming something
        // else: nothing here may invent a label.
        assert_eq!(canonical_host("."), "");
        assert_eq!(canonical_host("try.queenmq.cloud.."), "try.queenmq.cloud");
        assert_eq!(canonical_host("[::1]:6711"), "[::1]");
    }

    #[test]
    fn shared_host_matching_ignores_case_and_port() {
        let cfg = cfg_with_shared(&["try.queenmq.cloud"]);
        assert!(cfg.is_shared_host("try.queenmq.cloud"));
        assert!(cfg.is_shared_host("try.queenmq.cloud:6711"));
        assert!(cfg.is_shared_host("TRY.QueenMQ.Cloud:443"));
        assert!(cfg.is_shared_host("  try.queenmq.cloud  "));
    }

    #[test]
    fn a_fully_qualified_host_is_still_the_shared_host() {
        // The bypass this closes: `Host: try.queenmq.cloud.` classified as NOT
        // shared, fell through to cache::resolve_host, and was absorbed by
        // QUEEN_PROXY_DEFAULT_CLUSTER (dev cell) or answered 421 (trial cell).
        let mut cfg = cfg_with_shared(&["try.queenmq.cloud"]);
        cfg.default_cluster = Some("dev".to_string());
        assert!(cfg.is_shared_host("try.queenmq.cloud."));
        assert!(cfg.is_shared_host("TRY.QueenMQ.Cloud.:443"));
        // And the configured side may carry the root dot too.
        let cfg = cfg_with_shared(&["try.queenmq.cloud."]);
        assert!(cfg.is_shared_host("try.queenmq.cloud"));
        assert!(cfg.is_shared_host("try.queenmq.cloud."));
        // A neighbouring name is still not shared: the dot is dropped, not the
        // label before it.
        assert!(!cfg.is_shared_host("try.queenmq.cloud.evil.test"));
    }

    #[test]
    fn a_configured_entry_may_carry_a_port() {
        // Pasted out of a browser bar. Normalized at load, so it still matches
        // the portless Host a real client sends.
        let cfg = cfg_with_shared(&["shared.local:6711"]);
        assert!(cfg.is_shared_host("shared.local"));
        assert!(cfg.is_shared_host("shared.local:6711"));
    }

    #[test]
    fn shared_host_is_an_exact_full_hostname_never_a_label_or_a_suffix() {
        let cfg = cfg_with_shared(&["try.queenmq.cloud"]);
        // The whole reason the knob takes hostnames and not first labels: a
        // per-cluster subdomain that happens to start with the same label must
        // keep its Host-label routing.
        assert!(!cfg.is_shared_host("try.other.example"));
        assert!(!cfg.is_shared_host("try"));
        assert!(!cfg.is_shared_host("acme.try.queenmq.cloud"));
        assert!(!cfg.is_shared_host("nottry.queenmq.cloud"));
        assert!(!cfg.is_shared_host("queenmq.cloud"));
    }

    #[test]
    fn an_unset_list_makes_no_host_shared() {
        let cfg = cfg_with_shared(&[]);
        assert!(cfg.shared_hosts.is_empty());
        for h in ["try.queenmq.cloud", "acme.queenmq.cloud", "localhost:6711", ""] {
            assert!(!cfg.is_shared_host(h), "{h} must keep today's Host-label routing");
        }
    }

    #[test]
    fn an_empty_or_missing_host_header_is_never_shared() {
        // `resolve_route` passes "" when the header is absent or unreadable;
        // that must fall through to the Host path (and its 421), not become a
        // shared host by accident.
        let cfg = cfg_with_shared(&["try.queenmq.cloud", ""]);
        assert_eq!(cfg.shared_hosts, vec!["try.queenmq.cloud"]);
        assert!(!cfg.is_shared_host(""));
        assert!(!cfg.is_shared_host("   "));
        assert!(!cfg.is_shared_host(":6711"));
    }

    #[test]
    fn several_hosts_may_be_shared_at_once() {
        let cfg = cfg_with_shared(&["try.queenmq.cloud", "shared.local"]);
        assert!(cfg.is_shared_host("try.queenmq.cloud"));
        assert!(cfg.is_shared_host("shared.local:6711"));
        assert!(!cfg.is_shared_host("acme.queenmq.cloud"));
    }

    // ---- jwt audience ------------------------------------------------------

    #[test]
    fn an_unconfigured_audience_stays_unset() {
        // Neither variable set: nothing to enforce, and every proxy that never
        // heard of this claim keeps verifying exactly as it did.
        assert_eq!(resolve_jwt_audience(None, None), None);
        // A blank or whitespace value is "not configured", not "configured with
        // nothing" (which would refuse every stamped token on a cell that
        // templates the variable in unconditionally).
        assert_eq!(resolve_jwt_audience(Some("".into()), None), None);
        assert_eq!(resolve_jwt_audience(Some("   ".into()), None), None);
        assert_eq!(resolve_jwt_audience(None, Some("")), None);
        assert_eq!(resolve_jwt_audience(None, Some("https://")), None);
    }

    #[test]
    fn an_explicit_audience_wins_over_the_public_url() {
        assert_eq!(
            resolve_jwt_audience(Some("fleet-a".into()), Some("https://cell-a.queenmq.cloud")),
            Some("fleet-a".to_string())
        );
        // Trimmed and lowercased, like every other host-shaped value here.
        assert_eq!(
            resolve_jwt_audience(Some("  Cell-A.QueenMQ.Cloud \n".into()), None),
            Some("cell-a.queenmq.cloud".to_string())
        );
    }

    #[test]
    fn the_default_audience_is_the_public_url_host() {
        for url in [
            "https://cell-a.queenmq.cloud",
            "https://cell-a.queenmq.cloud/",
            "https://cell-a.queenmq.cloud/console?next=/x",
            "http://Cell-A.QueenMQ.Cloud",
            // A port, a trailing root label and userinfo are all stripped: the
            // audience is a name, and it is the same name a Host header
            // canonicalizes to.
            "https://cell-a.queenmq.cloud:8443",
            "https://cell-a.queenmq.cloud./",
            "https://user:pw@cell-a.queenmq.cloud",
            // No scheme at all, which is what an operator who pasted a bare
            // hostname into QUEEN_PROXY_PUBLIC_URL leaves behind.
            "cell-a.queenmq.cloud",
        ] {
            assert_eq!(
                resolve_jwt_audience(None, Some(url)),
                Some("cell-a.queenmq.cloud".to_string()),
                "{url}"
            );
        }
    }

    #[test]
    fn the_derived_audience_matches_what_a_host_header_canonicalizes_to() {
        // The default only works as a default because the two sides agree: the
        // audience the control plane stamps is the hostname it routes to.
        let aud = resolve_jwt_audience(None, Some("https://cell-a.queenmq.cloud")).unwrap();
        for host in ["cell-a.queenmq.cloud", "cell-a.queenmq.cloud:443", "cell-a.queenmq.cloud."] {
            assert_eq!(canonical_host(host).to_ascii_lowercase(), aud, "{host}");
        }
    }

    // ---- jwt boot gate (spec §9b) ------------------------------------------

    /// The material a working cell carries: a pxdb and an HS secret, signer
    /// fully capable. Tests below vary one axis at a time from here.
    fn mat() -> JwtMaterial {
        JwtMaterial {
            has_pxdb: true,
            ed_private: false,
            ed_public: false,
            hs_secret: true,
            can_mint: true,
            can_verify: true,
        }
    }

    #[test]
    fn hs_secret_with_a_pxdb_is_mint_mode_and_boots() {
        let b = jwt_boot(mat());
        assert_eq!(b.mode, JwtMode::Mint);
        assert!(b.fatal.is_none(), "{:?}", b.fatal);
        assert!(b.warn.is_none());
    }

    #[test]
    fn ed25519_private_key_is_mint_mode_and_boots() {
        let m = JwtMaterial { ed_private: true, hs_secret: false, ..mat() };
        let b = jwt_boot(m);
        assert_eq!(b.mode, JwtMode::Mint);
        assert!(b.fatal.is_none(), "{:?}", b.fatal);
        // The public key derived from the private one is the normal shape and
        // must not be called contradictory.
        let b = jwt_boot(JwtMaterial { ed_public: true, ..m });
        assert_eq!(b.mode, JwtMode::Mint);
        assert!(b.fatal.is_none());
        assert!(b.warn.is_none());
    }

    /// THE MEASURED INCIDENT: a pxdb, a console, and nothing to sign with.
    ///
    /// Reported LOUDLY, not refused (spec §9b allows either): this is also the
    /// shape of the API-key-only self-hosted proxy, and `deploy/proxy.mdx`
    /// promises `PXDB_HOST` is the only variable it refuses to start without.
    #[test]
    fn mint_mode_without_a_signer_warns_loudly_and_still_boots() {
        let b = jwt_boot(JwtMaterial {
            hs_secret: false,
            can_mint: false,
            can_verify: false,
            ..mat()
        });
        assert_eq!(b.mode, JwtMode::Mint);
        assert!(b.fatal.is_none(), "an API-key-only proxy must keep booting: {:?}", b.fatal);
        let msg = b.warn.expect("a console that cannot mint must say so at boot");
        // The message has one job: name the way out. Both knobs, and the
        // verify-only escape, or the operator is back to reading source.
        for needle in [
            "QUEEN_PROXY_JWT_ED25519_PEM",
            "QUEEN_PROXY_JWT_SECRET",
            "QUEEN_PROXY_JWT_ED25519_PUB_PEM",
            "PXDB_HOST",
        ] {
            assert!(msg.contains(needle), "boot warning must name {needle}: {msg}");
        }
        assert!(msg.contains("503"), "boot warning must name what a login would do: {msg}");
    }

    #[test]
    fn an_unloadable_private_key_says_so_rather_than_no_signer_configured() {
        // ed_private set, nothing usable built: `Keys::build` logged "invalid
        // QUEEN_PROXY_JWT_ED25519_PEM; falling back" and there was nothing to
        // fall back to. Reporting a bare "not configured" would send the
        // operator to set a variable that is already set.
        let msg = jwt_boot(JwtMaterial {
            ed_private: true,
            hs_secret: false,
            can_mint: false,
            can_verify: false,
            ..mat()
        })
        .fatal
        .expect("fatal");
        assert!(msg.contains("did not load"), "{msg}");
    }

    #[test]
    fn a_signer_that_cannot_verify_what_it_mints_refuses_to_boot() {
        // Ed private loaded, public underivable: today that is an error! line
        // and a running proxy whose every login bounces back to /auth/login.
        let b = jwt_boot(JwtMaterial {
            ed_private: true,
            hs_secret: false,
            can_mint: true,
            can_verify: false,
            ..mat()
        });
        assert_eq!(b.mode, JwtMode::Mint);
        let msg = b.fatal.expect("minting tokens it rejects is not a bootable state");
        assert!(msg.contains("QUEEN_PROXY_JWT_ED25519_PUB_PEM"), "{msg}");
    }

    #[test]
    fn public_key_only_is_verify_only_and_boots() {
        let b = jwt_boot(JwtMaterial {
            ed_public: true,
            hs_secret: false,
            can_mint: false, // a verify-only host mints nothing, by design
            ..mat()
        });
        assert_eq!(b.mode, JwtMode::VerifyOnly);
        assert!(b.fatal.is_none(), "verify-only must keep booting: {:?}", b.fatal);
        assert!(b.warn.is_none());
    }

    #[test]
    fn verify_only_with_an_unparseable_public_key_refuses_to_boot() {
        let b = jwt_boot(JwtMaterial {
            ed_public: true,
            hs_secret: false,
            can_mint: false,
            can_verify: false,
            ..mat()
        });
        assert_eq!(b.mode, JwtMode::VerifyOnly);
        let msg = b.fatal.expect("a verify-only host that verifies nothing 401s every session");
        assert!(msg.contains("VERIFY-ONLY"), "{msg}");
        assert!(msg.contains("QUEEN_PROXY_JWT_ED25519_PUB_PEM"), "{msg}");
    }

    #[test]
    fn a_public_key_beside_an_hs_secret_is_hs256_with_a_warning() {
        // HS wins (that is what Keys::build does, and changing it would move a
        // live cell's signer under it), so this is not fatal — but the ignored
        // public key is exactly the half-finished EdDSA migration worth saying
        // out loud.
        let b = jwt_boot(JwtMaterial { ed_public: true, ..mat() });
        assert_eq!(b.mode, JwtMode::Mint);
        assert!(b.fatal.is_none());
        let w = b.warn.expect("an ignored public key must be reported");
        assert!(w.contains("IGNORED"), "{w}");
    }

    #[test]
    fn no_pxdb_is_no_identity_and_never_fatal() {
        // dev-static / pure data plane: `/auth/login` cannot resolve a user
        // without a users table, so nothing here can ever ask for a mint. This
        // is the shape the smoke scripts run, and it must keep booting with no
        // JWT material at all.
        let b = jwt_boot(JwtMaterial {
            has_pxdb: false,
            hs_secret: false,
            can_mint: false,
            can_verify: false,
            ..mat()
        });
        assert_eq!(b.mode, JwtMode::NoIdentity);
        assert!(b.fatal.is_none(), "{:?}", b.fatal);
    }

    #[test]
    fn every_combination_that_can_serve_its_mode_boots() {
        // Exhaustive over the six booleans: the ONLY refusals are a mode whose
        // capability is missing. Stated as a property so a future arm cannot
        // quietly start rejecting a shape that works.
        for bits in 0u8..64 {
            let m = JwtMaterial {
                has_pxdb: bits & 1 != 0,
                ed_private: bits & 2 != 0,
                ed_public: bits & 4 != 0,
                hs_secret: bits & 8 != 0,
                can_mint: bits & 16 != 0,
                can_verify: bits & 32 != 0,
            };
            let b = jwt_boot(m);
            let nothing_supplied = !m.ed_private && !m.ed_public && !m.hs_secret;
            let want_fatal = match b.mode {
                JwtMode::NoIdentity => false,
                JwtMode::VerifyOnly => !m.can_verify,
                // The one exemption: no material at all is the API-key-only
                // proxy, warned about rather than refused.
                JwtMode::Mint if nothing_supplied => false,
                JwtMode::Mint => !m.can_mint || !m.can_verify,
            };
            assert_eq!(b.fatal.is_some(), want_fatal, "{m:?} -> {b:?}");
        }
    }

    #[test]
    fn every_refusal_names_material_the_operator_actually_set() {
        // The contract that keeps `deploy/proxy.mdx` true: a proxy is only ever
        // refused for material it WAS given. Absent material is a shape, and
        // the only variable this process refuses to start without stays
        // PXDB_HOST.
        for bits in 0u8..64 {
            let m = JwtMaterial {
                has_pxdb: bits & 1 != 0,
                ed_private: bits & 2 != 0,
                ed_public: bits & 4 != 0,
                hs_secret: bits & 8 != 0,
                can_mint: bits & 16 != 0,
                can_verify: bits & 32 != 0,
            };
            if jwt_boot(m).fatal.is_some() {
                assert!(
                    m.ed_private || m.ed_public || m.hs_secret,
                    "refused a proxy that was given no JWT material at all: {m:?}"
                );
            }
        }
    }

    #[test]
    fn shared_hosts_and_default_cluster_are_independent_knobs() {
        // Decision z: `QUEEN_PROXY_DEFAULT_CLUSTER` is a different feature.
        // Nothing here reads it, and `resolve_route` answers the shared
        // question BEFORE `cache::resolve_host` (where the default lives) is
        // ever called — so the default can never absorb a shared host.
        let mut cfg = cfg_with_shared(&["shared.local"]);
        cfg.default_cluster = Some("dev".to_string());
        assert!(cfg.is_shared_host("shared.local"));
        assert!(!cfg.is_shared_host("anything-else.local"));
    }
}
