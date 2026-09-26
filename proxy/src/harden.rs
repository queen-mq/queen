//! W7 hardening (PLAN_SINGLE_BINARY.md): once the proxy router runs inside the
//! broker, the broker's public listener is internet-facing. The edge controls
//! live here as reusable axum layers and helpers, so every listener of the
//! single binary wires the same code.
//!
//! * **Client IP + per-IP rate limit** — [`ClientIpResolver`] keys on the real
//!   socket peer (`ConnectInfo`). `X-Forwarded-For` is honoured ONLY when the
//!   peer is inside `QUEEN_EDGE_TRUSTED_PROXIES`, walking the chain right to
//!   left to the first untrusted hop (SECURITY-ASSESSMENT-2026-09-05 #3: a
//!   client-supplied XFF reset the login throttle). [`IpRateLimiter`] is a
//!   sharded token bucket per IP (IPv6 per /64), bounded memory, `429` +
//!   `Retry-After`.
//! * **Request limits** — [`RequestLimits`]: header count/size and URI length
//!   sanity (`431`/`414`), a per-request timeout that honours long-poll pops
//!   (their own `timeout` + grace, capped), [`body_limit`].
//! * **Web listener rules** — [`WebRules`]: CSRF (a state-changing request that
//!   carries the session cookie, or any `Origin`, must come from an allowed
//!   origin), CORS from an allowlist (never `*` with credentials), security
//!   headers (SECURITY-ASSESSMENT #2).
//! * **Login brute force** — [`LoginGuard`]: per-IP and per-account failure
//!   counters, exponential backoff, a lockout cap, bounded memory.
//! * **TLS + serving** — [`tls_config_from_env`] and [`serve`]: plain or rustls,
//!   `ConnectInfo` + [`ConnMeta`] on every request, header-read and handshake
//!   timeouts, a connection cap, graceful shutdown.
//! * **Fuzzing** — [`fuzz`] holds the entry points `proxy/fuzz` drives; the
//!   seed-corpus tests at the bottom run them on stable.
//!
//! Wiring (the orchestrator's): [`Edge::from_env`] once at boot, then
//! [`Edge::data_plane`] on the data-plane router, [`Edge::web_plane`] on the
//! console/oauth/operator routers, and [`serve`] for the listener.

use std::collections::HashMap;
use std::future::Future;
use std::hash::BuildHasher;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, DefaultBodyLimit, Request, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::middleware::{from_fn_with_state, Next};
use axum::response::{IntoResponse, Response};
use axum::Router;

use crate::errors;

/// `code` of a request rejected for oversized headers (`431`) or URI (`414`).
pub const CODE_HEADERS_TOO_LARGE: &str = "request_headers_too_large";
/// `code` of a request the edge timed out (`504`).
pub const CODE_REQUEST_TIMEOUT: &str = "request_timeout";
/// `code` of a state-changing request refused by the CSRF rule (`403`).
pub const CODE_CSRF: &str = "csrf_rejected";

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // W1: a panic elsewhere must not turn every later request into a panic.
    // All state behind these locks is valid between single statements.
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn env_opt(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn env_num<T: std::str::FromStr>(key: &str, default: T) -> Result<T, String> {
    match env_opt(key) {
        None => Ok(default),
        Some(v) => v
            .parse()
            .map_err(|_| format!("{key}={v} is not a valid number")),
    }
}

fn env_list(key: &str) -> Vec<String> {
    env_opt(key)
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn ceil_secs(d: Duration) -> u64 {
    let s = d.as_secs() + u64::from(d.subsec_nanos() > 0);
    s.max(1)
}

// ---------------------------------------------------------------------------
// IP networks and the client IP
// ---------------------------------------------------------------------------

/// An IPv4-mapped IPv6 address (`::ffff:a.b.c.d`) is its IPv4 address.
pub fn canonical_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        v4 => v4,
    }
}

/// A CIDR block: `10.0.0.0/8`, `fd00::/8`, or a bare address (`/32`, `/128`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IpNet {
    net: IpAddr,
    prefix: u8,
}

impl IpNet {
    pub fn new(ip: IpAddr, prefix: u8) -> Result<IpNet, String> {
        let ip = canonical_ip(ip);
        let max = if ip.is_ipv4() { 32 } else { 128 };
        if prefix > max {
            return Err(format!("prefix /{prefix} is longer than /{max}"));
        }
        Ok(IpNet {
            net: mask(ip, prefix),
            prefix,
        })
    }

    pub fn parse(s: &str) -> Result<IpNet, String> {
        let s = s.trim();
        let (addr, prefix) = match s.split_once('/') {
            Some((a, p)) => (a, Some(p)),
            None => (s, None),
        };
        let ip: IpAddr = addr
            .parse()
            .map_err(|_| format!("`{s}` is not an IP address or CIDR"))?;
        let ip = canonical_ip(ip);
        let prefix = match prefix {
            Some(p) => p
                .parse::<u8>()
                .map_err(|_| format!("`{s}` has a bad prefix length"))?,
            None if ip.is_ipv4() => 32,
            None => 128,
        };
        IpNet::new(ip, prefix)
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        let ip = canonical_ip(ip);
        ip.is_ipv4() == self.net.is_ipv4() && mask(ip, self.prefix) == self.net
    }
}

fn mask(ip: IpAddr, prefix: u8) -> IpAddr {
    match ip {
        IpAddr::V4(v4) => {
            let bits = u32::from(v4);
            let m = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - u32::from(prefix.min(32)))
            };
            IpAddr::V4(Ipv4Addr::from(bits & m))
        }
        IpAddr::V6(v6) => {
            let bits = u128::from(v6);
            let m = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - u32::from(prefix.min(128)))
            };
            IpAddr::V6(Ipv6Addr::from(bits & m))
        }
    }
}

/// Parse a trusted-proxy list: CIDRs or addresses, plus the aliases
/// `loopback` and `private` (RFC 1918, CGNAT 100.64/10, ULA fc00::/7).
pub fn parse_trusted_proxies(items: &[String]) -> Result<Vec<IpNet>, String> {
    let mut out = Vec::new();
    for it in items {
        match it.to_ascii_lowercase().as_str() {
            "loopback" => {
                out.push(IpNet::parse("127.0.0.0/8")?);
                out.push(IpNet::parse("::1/128")?);
            }
            "private" => {
                for c in [
                    "10.0.0.0/8",
                    "172.16.0.0/12",
                    "192.168.0.0/16",
                    "100.64.0.0/10",
                    "fc00::/7",
                ] {
                    out.push(IpNet::parse(c)?);
                }
            }
            _ => out.push(IpNet::parse(it)?),
        }
    }
    Ok(out)
}

/// The resolved client address of a request, inserted into the request
/// extensions by [`client_ip_mw`]. Security controls (rate limits, the login
/// guard) key on this, never on a raw header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientIp(pub IpAddr);

/// Inserted by [`client_ip_mw`] when a TRUSTED proxy says the original request
/// was HTTPS (`X-Forwarded-Proto: https`). See [`is_https`].
#[derive(Clone, Copy, Debug)]
pub struct ViaHttps;

/// Per-connection facts, inserted into every request by [`serve`].
#[derive(Clone, Copy, Debug)]
pub struct ConnMeta {
    pub peer: SocketAddr,
    pub tls: bool,
}

/// Whether the browser reached us over HTTPS: this listener terminated TLS
/// ([`ConnMeta`]), or a trusted proxy said so ([`ViaHttps`]). The cookie
/// `Secure` flag should follow this, not a raw `X-Forwarded-Proto`.
pub fn is_https(ext: &axum::http::Extensions) -> bool {
    ext.get::<ConnMeta>().is_some_and(|m| m.tls) || ext.get::<ViaHttps>().is_some()
}

/// Derives the client IP from the socket peer and, only behind a trusted
/// proxy, from the forwarding header.
#[derive(Clone, Debug)]
pub struct ClientIpResolver {
    trusted: Vec<IpNet>,
    /// `x-forwarded-for` (walked right to left), or a single-address header
    /// such as `cf-connecting-ip` / `x-real-ip`.
    header: HeaderName,
}

/// At most this many XFF hops are examined (bounded work per request).
const MAX_XFF_HOPS: usize = 32;

impl ClientIpResolver {
    pub fn new(trusted: Vec<IpNet>, header: Option<&str>) -> Result<ClientIpResolver, String> {
        let header = match header {
            None => HeaderName::from_static("x-forwarded-for"),
            Some(h) => HeaderName::from_bytes(h.trim().to_ascii_lowercase().as_bytes())
                .map_err(|_| format!("`{h}` is not a valid header name"))?,
        };
        Ok(ClientIpResolver { trusted, header })
    }

    /// `QUEEN_EDGE_TRUSTED_PROXIES` (CIDR list; default: trust nobody) and
    /// `QUEEN_EDGE_REAL_IP_HEADER` (default `x-forwarded-for`).
    pub fn from_env() -> Result<ClientIpResolver, String> {
        let trusted = parse_trusted_proxies(&env_list("QUEEN_EDGE_TRUSTED_PROXIES"))
            .map_err(|e| format!("QUEEN_EDGE_TRUSTED_PROXIES: {e}"))?;
        ClientIpResolver::new(trusted, env_opt("QUEEN_EDGE_REAL_IP_HEADER").as_deref())
    }

    pub fn is_trusted(&self, ip: IpAddr) -> bool {
        self.trusted.iter().any(|n| n.contains(ip))
    }

    /// The client address for a request that arrived from `peer`.
    pub fn resolve(&self, peer: IpAddr, headers: &HeaderMap) -> IpAddr {
        let peer = canonical_ip(peer);
        if !self.is_trusted(peer) {
            return peer;
        }
        if self.header == "x-forwarded-for" {
            // Every XFF line, in order, as one comma list; walk it from the
            // right: each trusted hop vouches for the address to its left, the
            // first untrusted one is the client.
            let hops: Vec<&str> = headers
                .get_all(&self.header)
                .iter()
                .filter_map(|v| v.to_str().ok())
                .flat_map(|v| v.split(','))
                .collect();
            let mut client = peer;
            for hop in hops.iter().rev().take(MAX_XFF_HOPS) {
                match parse_forwarded_ip(hop) {
                    Some(ip) => {
                        client = ip;
                        if !self.is_trusted(ip) {
                            return ip;
                        }
                    }
                    // A hop we cannot read ends the walk: whatever lies left
                    // of it was written by someone we cannot vouch for.
                    None => return client,
                }
            }
            client
        } else {
            headers
                .get(&self.header)
                .and_then(|v| v.to_str().ok())
                .and_then(parse_forwarded_ip)
                .unwrap_or(peer)
        }
    }

    /// `X-Forwarded-Proto: https` from a trusted peer.
    pub fn forwarded_https(&self, peer: IpAddr, headers: &HeaderMap) -> bool {
        self.is_trusted(peer)
            && headers
                .get("x-forwarded-proto")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.split(',').next())
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("https"))
    }
}

/// One forwarding-header hop: `1.2.3.4`, `1.2.3.4:5678`, `2001:db8::1`,
/// `[2001:db8::1]:443`. Anything else is `None`.
pub fn parse_forwarded_ip(s: &str) -> Option<IpAddr> {
    let s = s.trim();
    if s.is_empty() || s.len() > 64 {
        return None;
    }
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Some(canonical_ip(ip));
    }
    if let Ok(sa) = s.parse::<SocketAddr>() {
        return Some(canonical_ip(sa.ip()));
    }
    // `[v6]` without a port.
    s.strip_prefix('[')
        .and_then(|r| r.strip_suffix(']'))
        .and_then(|r| r.parse::<Ipv6Addr>().ok())
        .map(|v6| canonical_ip(IpAddr::V6(v6)))
}

fn peer_of(req: &Request) -> Option<SocketAddr> {
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0)
        .or_else(|| req.extensions().get::<ConnMeta>().map(|m| m.peer))
}

static NO_PEER_WARNED: AtomicBool = AtomicBool::new(false);

/// Resolve [`ClientIp`] (and [`ViaHttps`]) once per request. Needs the socket
/// peer: serve with [`serve`] or `into_make_service_with_connect_info`.
/// Without it the request passes unresolved (the per-IP controls then fail
/// open) and a warning is logged once.
pub async fn client_ip_mw(
    State(r): State<Arc<ClientIpResolver>>,
    mut req: Request,
    next: Next,
) -> Response {
    if req.extensions().get::<ClientIp>().is_none() {
        match peer_of(&req) {
            Some(peer) => {
                let ip = r.resolve(peer.ip(), req.headers());
                let https = r.forwarded_https(peer.ip(), req.headers());
                req.extensions_mut().insert(ClientIp(ip));
                if https {
                    req.extensions_mut().insert(ViaHttps);
                }
                // What the handlers read (the cookie `Secure` flag, the OAuth
                // redirect URI) is what this edge verified, never a
                // client-supplied `X-Forwarded-Proto`.
                let verified = is_https(req.extensions());
                let h = req.headers_mut();
                h.remove("x-forwarded-proto");
                if verified {
                    h.insert("x-forwarded-proto", axum::http::HeaderValue::from_static("https"));
                }
            }
            None => {
                if !NO_PEER_WARNED.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        target: "edge",
                        "no socket peer on the request: serve with harden::serve or into_make_service_with_connect_info; per-IP limits are OFF"
                    );
                }
            }
        }
    }
    next.run(req).await
}

// ---------------------------------------------------------------------------
// Per-IP token bucket
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RateLimitConfig {
    /// Sustained requests per second per client key; `0` disables the limiter.
    pub rps: f64,
    /// Bucket size (the burst a quiet client may spend at once).
    pub burst: f64,
    /// Upper bound on tracked keys (memory bound: ~64 B each).
    pub max_tracked: usize,
    /// IPv6 clients are keyed per prefix of this length (default /64: one
    /// subscriber's allocation, not one of its 2^64 addresses).
    pub ipv6_prefix: u8,
}

impl RateLimitConfig {
    /// `{prefix}_RPS_PER_IP`, `{prefix}_BURST_PER_IP` (default `2 × rps`),
    /// `QUEEN_EDGE_MAX_TRACKED_IPS`, `QUEEN_EDGE_IPV6_PREFIX`.
    pub fn from_env(
        prefix: &str,
        default_rps: f64,
        default_burst: f64,
    ) -> Result<RateLimitConfig, String> {
        let rps: f64 = env_num(&format!("{prefix}_RPS_PER_IP"), default_rps)?;
        let burst_default = if default_burst > 0.0 {
            default_burst
        } else {
            (rps * 2.0).max(1.0)
        };
        let burst: f64 = env_num(&format!("{prefix}_BURST_PER_IP"), burst_default)?;
        let cfg = RateLimitConfig {
            rps,
            burst,
            max_tracked: env_num("QUEEN_EDGE_MAX_TRACKED_IPS", 100_000usize)?,
            ipv6_prefix: env_num("QUEEN_EDGE_IPV6_PREFIX", 64u8)?,
        };
        if !(cfg.rps >= 0.0 && cfg.rps.is_finite() && cfg.burst >= 1.0 && cfg.burst.is_finite()) {
            return Err(format!(
                "{prefix}_RPS_PER_IP must be >= 0 and {prefix}_BURST_PER_IP >= 1"
            ));
        }
        if cfg.ipv6_prefix > 128 {
            return Err("QUEEN_EDGE_IPV6_PREFIX must be <= 128".into());
        }
        Ok(cfg)
    }
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

struct Shard {
    map: HashMap<IpAddr, Bucket>,
    last_sweep: Option<Instant>,
}

const SHARDS: usize = 16;

/// Sharded per-IP token buckets with a hard cap on tracked keys. At the cap a
/// shard first drops every bucket that has refilled (indistinguishable from a
/// fresh one), at most once a second, then evicts an arbitrary entry — which
/// hands that key a fresh bucket, a bounded leak in exchange for O(1) memory.
pub struct IpRateLimiter {
    cfg: RateLimitConfig,
    per_shard: usize,
    hasher: std::collections::hash_map::RandomState,
    shards: Vec<Mutex<Shard>>,
}

impl IpRateLimiter {
    pub fn new(cfg: RateLimitConfig) -> IpRateLimiter {
        let per_shard = (cfg.max_tracked / SHARDS).max(1);
        IpRateLimiter {
            cfg,
            per_shard,
            hasher: std::collections::hash_map::RandomState::new(),
            shards: (0..SHARDS)
                .map(|_| {
                    Mutex::new(Shard {
                        map: HashMap::new(),
                        last_sweep: None,
                    })
                })
                .collect(),
        }
    }

    pub fn config(&self) -> RateLimitConfig {
        self.cfg
    }

    pub fn enabled(&self) -> bool {
        self.cfg.rps > 0.0
    }

    pub fn key(&self, ip: IpAddr) -> IpAddr {
        match canonical_ip(ip) {
            IpAddr::V6(v6) => mask(IpAddr::V6(v6), self.cfg.ipv6_prefix),
            v4 => v4,
        }
    }

    /// Take one token for `ip`; `Err(retry_after)` when its bucket is empty.
    pub fn check(&self, ip: IpAddr) -> Result<(), Duration> {
        self.check_at(ip, Instant::now())
    }

    pub fn check_at(&self, ip: IpAddr, now: Instant) -> Result<(), Duration> {
        if !self.enabled() {
            return Ok(());
        }
        let key = self.key(ip);
        let idx = (self.hasher.hash_one(key) as usize) % SHARDS;
        let mut shard = lock(&self.shards[idx]);
        if !shard.map.contains_key(&key) && shard.map.len() >= self.per_shard {
            self.make_room(&mut shard, now);
        }
        let (rps, burst) = (self.cfg.rps, self.cfg.burst);
        let b = shard.map.entry(key).or_insert(Bucket {
            tokens: burst,
            last: now,
        });
        let elapsed = now.saturating_duration_since(b.last).as_secs_f64();
        b.tokens = (b.tokens + elapsed * rps).min(burst);
        b.last = now;
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            Ok(())
        } else {
            Err(Duration::from_secs_f64(((1.0 - b.tokens) / rps).max(0.001)))
        }
    }

    fn make_room(&self, shard: &mut Shard, now: Instant) {
        let due = shard
            .last_sweep
            .is_none_or(|t| now.saturating_duration_since(t) >= Duration::from_secs(1));
        if due {
            shard.last_sweep = Some(now);
            let (rps, burst) = (self.cfg.rps, self.cfg.burst);
            shard.map.retain(|_, b| {
                b.tokens + now.saturating_duration_since(b.last).as_secs_f64() * rps < burst
            });
        }
        while shard.map.len() >= self.per_shard {
            let Some(k) = shard.map.keys().next().copied() else {
                break;
            };
            shard.map.remove(&k);
        }
    }

    /// Keys currently tracked (tests, metrics).
    pub fn tracked(&self) -> usize {
        self.shards.iter().map(|s| lock(s).map.len()).sum()
    }
}

/// `429` + `Retry-After` when the client's bucket is empty. Keys on
/// [`ClientIp`] (so layer it inside [`client_ip_mw`]), else the raw peer.
pub async fn rate_limit_mw(
    State(l): State<Arc<IpRateLimiter>>,
    req: Request,
    next: Next,
) -> Response {
    if !l.enabled() {
        return next.run(req).await;
    }
    let ip = req
        .extensions()
        .get::<ClientIp>()
        .map(|c| c.0)
        .or_else(|| peer_of(&req).map(|p| p.ip()));
    let Some(ip) = ip else {
        return next.run(req).await;
    };
    match l.check(ip) {
        Ok(()) => next.run(req).await,
        Err(retry) => errors::err_429(
            errors::CODE_RATE_LIMITED,
            ceil_secs(retry),
            "too many requests from this address",
        ),
    }
}

// ---------------------------------------------------------------------------
// Request limits
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct RequestLimits {
    pub max_headers: usize,
    /// Sum of name + value bytes over all headers.
    pub max_header_bytes: usize,
    pub max_header_value_bytes: usize,
    /// Path + query.
    pub max_uri_bytes: usize,
    /// Default per-request budget; `None` disables the timeout.
    pub timeout: Option<Duration>,
    /// Added to a long-poll pop's own `timeout`.
    pub long_poll_grace: Duration,
    /// Ceiling for a long-poll pop, whatever it asks for.
    pub long_poll_max: Duration,
    /// Path prefixes with no edge timeout at all.
    pub timeout_exempt: Vec<String>,
}

impl Default for RequestLimits {
    fn default() -> RequestLimits {
        RequestLimits {
            max_headers: 100,
            max_header_bytes: 64 * 1024,
            max_header_value_bytes: 16 * 1024,
            max_uri_bytes: 8 * 1024,
            timeout: Some(Duration::from_secs(60)),
            long_poll_grace: Duration::from_secs(10),
            long_poll_max: Duration::from_secs(310),
            timeout_exempt: Vec::new(),
        }
    }
}

impl RequestLimits {
    /// `QUEEN_EDGE_MAX_HEADERS`, `QUEEN_EDGE_MAX_HEADER_BYTES`,
    /// `QUEEN_EDGE_MAX_HEADER_VALUE_BYTES`, `QUEEN_EDGE_MAX_URI_BYTES`,
    /// `QUEEN_EDGE_REQUEST_TIMEOUT_MS` (`0` = off), `QUEEN_EDGE_LONG_POLL_GRACE_MS`,
    /// `QUEEN_EDGE_LONG_POLL_MAX_MS`, `QUEEN_EDGE_TIMEOUT_EXEMPT` (path prefixes).
    pub fn from_env() -> Result<RequestLimits, String> {
        let d = RequestLimits::default();
        let timeout_ms: u64 = env_num("QUEEN_EDGE_REQUEST_TIMEOUT_MS", 60_000u64)?;
        Ok(RequestLimits {
            max_headers: env_num("QUEEN_EDGE_MAX_HEADERS", d.max_headers)?,
            max_header_bytes: env_num("QUEEN_EDGE_MAX_HEADER_BYTES", d.max_header_bytes)?,
            max_header_value_bytes: env_num(
                "QUEEN_EDGE_MAX_HEADER_VALUE_BYTES",
                d.max_header_value_bytes,
            )?,
            max_uri_bytes: env_num("QUEEN_EDGE_MAX_URI_BYTES", d.max_uri_bytes)?,
            timeout: (timeout_ms > 0).then(|| Duration::from_millis(timeout_ms)),
            long_poll_grace: Duration::from_millis(env_num(
                "QUEEN_EDGE_LONG_POLL_GRACE_MS",
                10_000u64,
            )?),
            long_poll_max: Duration::from_millis(env_num(
                "QUEEN_EDGE_LONG_POLL_MAX_MS",
                310_000u64,
            )?),
            timeout_exempt: env_list("QUEEN_EDGE_TIMEOUT_EXEMPT"),
        })
    }

    /// The budget for one request: exempt prefixes get none, a long-poll pop
    /// gets its own `timeout` (default 30 s) plus the grace, capped.
    pub fn timeout_for(&self, path: &str, query: Option<&str>) -> Option<Duration> {
        if self
            .timeout_exempt
            .iter()
            .any(|p| path.starts_with(p.as_str()))
        {
            return None;
        }
        if crate::routes::is_wait_pop(path, query) {
            let asked = crate::routes::poll_timeout_ms(query).unwrap_or(30_000);
            let t = Duration::from_millis(asked).saturating_add(self.long_poll_grace);
            return Some(t.min(self.long_poll_max));
        }
        self.timeout
    }

    /// `Err((status, reason))` when the head of the request is out of bounds.
    pub fn check_head(
        &self,
        uri_len: usize,
        headers: &HeaderMap,
    ) -> Result<(), (StatusCode, &'static str)> {
        if uri_len > self.max_uri_bytes {
            return Err((StatusCode::URI_TOO_LONG, "request URI too long"));
        }
        if headers.len() > self.max_headers {
            return Err((
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                "too many request headers",
            ));
        }
        let mut total = 0usize;
        for (name, value) in headers {
            if value.len() > self.max_header_value_bytes {
                return Err((
                    StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                    "request header too large",
                ));
            }
            total += name.as_str().len() + value.len();
        }
        if total > self.max_header_bytes {
            return Err((
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                "request headers too large",
            ));
        }
        Ok(())
    }
}

/// Header count/size and URI length sanity (`431` / `414`).
pub async fn header_limits_mw(
    State(l): State<Arc<RequestLimits>>,
    req: Request,
    next: Next,
) -> Response {
    let uri_len = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().len())
        .unwrap_or(0);
    if let Err((status, msg)) = l.check_head(uri_len, req.headers()) {
        return errors::json_error(status, CODE_HEADERS_TOO_LARGE, msg);
    }
    next.run(req).await
}

/// The per-request timeout ([`RequestLimits::timeout_for`]); `504` on expiry.
/// Dropping the handler future is what a client disconnect already does, so
/// every handler is cancellation-tolerant by construction.
pub async fn timeout_mw(State(l): State<Arc<RequestLimits>>, req: Request, next: Next) -> Response {
    let budget = l.timeout_for(req.uri().path(), req.uri().query());
    match budget {
        None => next.run(req).await,
        Some(d) => match tokio::time::timeout(d, next.run(req)).await {
            Ok(resp) => resp,
            Err(_) => errors::json_error(
                StatusCode::GATEWAY_TIMEOUT,
                CODE_REQUEST_TIMEOUT,
                "request timed out",
            ),
        },
    }
}

/// The request-body cap for axum extractors (`Json`, `Form`, `Bytes`).
pub fn body_limit(bytes: usize) -> DefaultBodyLimit {
    DefaultBodyLimit::max(bytes)
}

// ---------------------------------------------------------------------------
// Web listener: CSRF, CORS, security headers
// ---------------------------------------------------------------------------

/// `scheme://host[:port]`, lowercased, default port dropped, for an `Origin`
/// value or any absolute http(s) URL (a `Referer`). `None` for anything else,
/// including the opaque origin `null`.
pub fn normalize_origin(s: &str) -> Option<String> {
    let s = s.trim();
    if s.len() > 2048 {
        return None;
    }
    let (scheme, rest) = s.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    let default_port = match scheme.as_str() {
        "http" => 80,
        "https" => 443,
        _ => return None,
    };
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..end];
    let authority = authority
        .rsplit_once('@')
        .map(|(_, a)| a)
        .unwrap_or(authority);
    let (host, port) = split_host_port(authority)?;
    let host = host.to_ascii_lowercase();
    match port {
        Some(p) if p != default_port => Some(format!("{scheme}://{host}:{p}")),
        _ => Some(format!("{scheme}://{host}")),
    }
}

/// `host[:port]` → (host, port). Brackets are kept on an IPv6 host.
fn split_host_port(a: &str) -> Option<(&str, Option<u16>)> {
    if a.is_empty() {
        return None;
    }
    let (host, port) = if a.starts_with('[') {
        let close = a.find(']')?;
        let host = &a[..=close];
        host[1..close].parse::<Ipv6Addr>().ok()?;
        match &a[close + 1..] {
            "" => (host, None),
            p => (host, Some(p.strip_prefix(':')?)),
        }
    } else {
        match a.rsplit_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (a, None),
        }
    };
    let valid = !host.is_empty()
        && (host.starts_with('[')
            || host
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.' || b == b'_'));
    if !valid {
        return None;
    }
    let port = match port {
        None => None,
        Some(p) => Some(p.parse::<u16>().ok()?),
    };
    Some((host, port))
}

/// HSTS policy for [`security_headers_mw`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hsts {
    Off,
    /// Always (the listener sits behind a TLS edge that is always HTTPS).
    On {
        max_age_s: u64,
    },
    /// Only on requests that arrived over HTTPS ([`is_https`]).
    Auto {
        max_age_s: u64,
    },
}

/// Rules for the cookie-authenticated web surfaces (console, oauth, operator).
#[derive(Clone, Debug)]
pub struct WebRules {
    /// Origins allowed to make state-changing requests IN ADDITION to the
    /// request's own origin (an `Origin` matching the `Host` header is always
    /// allowed: a same-origin request is never a forgery). Needed for a
    /// sibling host that posts here, or a proxy that rewrites `Host`.
    pub public_origins: Vec<String>,
    /// Cookie names that make a request cookie-authenticated.
    pub session_cookies: Vec<String>,
    /// CORS allowlist (normalized origins). Credentials are allowed only for
    /// these exact origins.
    pub cors_origins: Vec<String>,
    /// `*` was configured: any origin may READ, never with credentials.
    pub cors_any: bool,
    pub cors_methods: String,
    pub cors_headers: String,
    pub cors_max_age_s: u64,
    pub hsts: Hsts,
}

impl WebRules {
    /// `QUEEN_PUBLIC_ORIGINS` (csv; default: the origin of
    /// `QUEEN_PROXY_PUBLIC_URL`, else same-origin), the session cookie names
    /// derived from `QUEEN_PROXY_COOKIE_NAME` (default `queen_session`: the
    /// fleet cookie plus its `_cell` / `__Host-…_cell` twins) plus
    /// `QUEEN_EDGE_SESSION_COOKIES`, `QUEEN_EDGE_CORS_ORIGINS` (csv or `*`),
    /// `QUEEN_EDGE_CORS_HEADERS`, `QUEEN_EDGE_HSTS` (`auto` default / `on` /
    /// `off`), `QUEEN_EDGE_HSTS_MAX_AGE_S`.
    pub fn from_env() -> Result<WebRules, String> {
        let mut public_origins = Vec::new();
        for o in env_list("QUEEN_PUBLIC_ORIGINS") {
            public_origins.push(
                normalize_origin(&o).ok_or_else(|| {
                    format!("QUEEN_PUBLIC_ORIGINS: `{o}` is not an http(s) origin")
                })?,
            );
        }
        if public_origins.is_empty() {
            if let Some(u) = env_opt("QUEEN_PROXY_PUBLIC_URL") {
                public_origins.extend(normalize_origin(&u));
            }
        }
        let fleet = env_opt("QUEEN_PROXY_COOKIE_NAME").unwrap_or_else(|| "queen_session".into());
        let mut session_cookies = vec![
            fleet.clone(),
            format!("{fleet}_cell"),
            format!("__Host-{fleet}_cell"),
        ];
        session_cookies.extend(env_list("QUEEN_EDGE_SESSION_COOKIES"));
        let mut cors_origins = Vec::new();
        let mut cors_any = false;
        for o in env_list("QUEEN_EDGE_CORS_ORIGINS") {
            if o == "*" {
                cors_any = true;
            } else {
                cors_origins.push(normalize_origin(&o).ok_or_else(|| {
                    format!("QUEEN_EDGE_CORS_ORIGINS: `{o}` is not an http(s) origin")
                })?);
            }
        }
        let max_age_s: u64 = env_num("QUEEN_EDGE_HSTS_MAX_AGE_S", 31_536_000u64)?;
        let hsts = match env_opt("QUEEN_EDGE_HSTS")
            .map(|v| v.to_ascii_lowercase())
            .as_deref()
        {
            None | Some("auto") => Hsts::Auto { max_age_s },
            Some("on") | Some("true") => Hsts::On { max_age_s },
            Some("off") | Some("false") => Hsts::Off,
            Some(v) => return Err(format!("QUEEN_EDGE_HSTS={v}: use auto, on or off")),
        };
        Ok(WebRules {
            public_origins,
            session_cookies,
            cors_origins,
            cors_any,
            cors_methods: "GET, POST, PUT, PATCH, DELETE, OPTIONS".into(),
            cors_headers: env_opt("QUEEN_EDGE_CORS_HEADERS")
                .unwrap_or_else(|| "authorization, content-type, x-request-id".into()),
            cors_max_age_s: 600,
            hsts,
        })
    }

    fn origin_allowed(&self, origin: &str, headers: &HeaderMap) -> bool {
        let Some(o) = normalize_origin(origin) else {
            return false;
        };
        if self.public_origins.contains(&o) {
            return true;
        }
        // Same origin: the origin's authority is this request's Host (the
        // scheme-default port dropped the same way on both sides). A forged
        // request carries the attacker page's origin, never ours; a rebound
        // DNS name matches but carries no cookie of ours.
        let Some(host) = headers.get(header::HOST).and_then(|v| v.to_str().ok()) else {
            return false;
        };
        let scheme = o.split_once("://").map(|(s, _)| s).unwrap_or("https");
        normalize_origin(&format!("{scheme}://{host}")).is_some_and(|h| h == o)
    }

    /// Whether the request presents one of the session cookies.
    pub fn has_session_cookie(&self, headers: &HeaderMap) -> bool {
        headers
            .get_all(header::COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(';'))
            .filter_map(|kv| kv.split_once('=').map(|(k, _)| k.trim()))
            .any(|k| self.session_cookies.iter().any(|c| c == k))
    }

    /// The CSRF rule. Safe methods pass. A state-changing request is refused
    /// when it carries a session cookie and cannot show an allowed origin
    /// (`Origin`, else `Referer`, else `Sec-Fetch-Site: same-origin`), and —
    /// cookie or not — whenever it shows a cross-site `Origin`/`Referer`/
    /// `Sec-Fetch-Site` (login CSRF). A cookie-less request with no browser
    /// headers at all (curl, an SDK) passes: it carries no ambient authority.
    pub fn csrf_verdict(&self, method: &Method, headers: &HeaderMap) -> Result<(), &'static str> {
        if matches!(
            *method,
            Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
        ) {
            return Ok(());
        }
        let cookie = self.has_session_cookie(headers);
        if let Some(origin) = headers.get(header::ORIGIN) {
            let origin = origin.to_str().unwrap_or("");
            return if self.origin_allowed(origin, headers) {
                Ok(())
            } else {
                Err("cross-origin request refused")
            };
        }
        if let Some(referer) = headers.get(header::REFERER) {
            let referer = referer.to_str().unwrap_or("");
            return if self.origin_allowed(referer, headers) {
                Ok(())
            } else {
                Err("cross-origin referer refused")
            };
        }
        match headers.get("sec-fetch-site").and_then(|v| v.to_str().ok()) {
            Some(s) if s.eq_ignore_ascii_case("same-origin") => Ok(()),
            Some(_) => Err("cross-site request refused"),
            None if cookie => Err("a cookie-authenticated request must carry Origin"),
            None => Ok(()),
        }
    }

    /// The data-plane variant: only a request that carries a session cookie is
    /// judged (the webapp's cookie-authenticated XHR); a cookie-less caller —
    /// an SDK with a bearer, from any origin — is not this rule's business.
    pub fn csrf_verdict_cookie_only(
        &self,
        method: &Method,
        headers: &HeaderMap,
    ) -> Result<(), &'static str> {
        if !self.has_session_cookie(headers) {
            return Ok(());
        }
        self.csrf_verdict(method, headers)
    }
}

/// CSRF enforcement for the cookie-authenticated routes: `403 csrf_rejected`.
pub async fn csrf_mw(State(w): State<Arc<WebRules>>, req: Request, next: Next) -> Response {
    if let Err(why) = w.csrf_verdict(req.method(), req.headers()) {
        return csrf_refused(&req, why);
    }
    next.run(req).await
}

/// [`WebRules::csrf_verdict_cookie_only`] for the data plane.
pub async fn csrf_cookie_mw(State(w): State<Arc<WebRules>>, req: Request, next: Next) -> Response {
    if let Err(why) = w.csrf_verdict_cookie_only(req.method(), req.headers()) {
        return csrf_refused(&req, why);
    }
    next.run(req).await
}

fn csrf_refused(req: &Request, why: &'static str) -> Response {
    tracing::info!(target: "edge", method = %req.method(), path = %req.uri().path(), reason = why, "csrf: refused");
    errors::json_error(StatusCode::FORBIDDEN, CODE_CSRF, why)
}

fn append_vary(h: &mut HeaderMap, v: &'static str) {
    h.append(header::VARY, HeaderValue::from_static(v));
}

/// CORS from the allowlist. A preflight from an allowed origin is answered
/// here (`204`); from any other origin it is refused (`403`, no CORS
/// headers). Credentials are only ever allowed for an exact allowlisted
/// origin, never with `*`. Not an authorization layer: a disallowed origin's
/// simple request still reaches the router (the CSRF rule guards writes); the
/// browser just cannot read the answer.
pub async fn cors_mw(State(w): State<Arc<WebRules>>, req: Request, next: Next) -> Response {
    let Some(origin) = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
    else {
        return next.run(req).await;
    };
    let exact = normalize_origin(&origin).is_some_and(|o| w.cors_origins.contains(&o));
    let allowed = exact || w.cors_any;
    let preflight = req.method() == Method::OPTIONS
        && req
            .headers()
            .contains_key(header::ACCESS_CONTROL_REQUEST_METHOD);
    if preflight {
        if !allowed {
            let mut resp = StatusCode::FORBIDDEN.into_response();
            append_vary(resp.headers_mut(), "origin");
            return resp;
        }
        let mut resp = StatusCode::NO_CONTENT.into_response();
        let h = resp.headers_mut();
        set_allow_origin(h, &origin, exact);
        if let Ok(v) = HeaderValue::from_str(&w.cors_methods) {
            h.insert(header::ACCESS_CONTROL_ALLOW_METHODS, v);
        }
        if let Ok(v) = HeaderValue::from_str(&w.cors_headers) {
            h.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, v);
        }
        h.insert(
            header::ACCESS_CONTROL_MAX_AGE,
            HeaderValue::from(w.cors_max_age_s),
        );
        append_vary(h, "origin");
        append_vary(h, "access-control-request-method");
        append_vary(h, "access-control-request-headers");
        return resp;
    }
    let mut resp = next.run(req).await;
    let h = resp.headers_mut();
    append_vary(h, "origin");
    if allowed {
        set_allow_origin(h, &origin, exact);
        h.insert(
            header::ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_static("retry-after"),
        );
    }
    resp
}

fn set_allow_origin(h: &mut HeaderMap, origin: &str, exact: bool) {
    if exact {
        if let Ok(v) = HeaderValue::from_str(origin) {
            h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, v);
            h.insert(
                header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }
    } else {
        // `*` never travels with credentials.
        h.insert(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("*"),
        );
        h.remove(header::ACCESS_CONTROL_ALLOW_CREDENTIALS);
    }
}

/// Security response headers (SECURITY-ASSESSMENT-2026-09-05 #2):
/// `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY` and
/// `Content-Security-Policy: frame-ancestors 'none'` (unless the handler set a
/// CSP), `Referrer-Policy: same-origin`, and HSTS per [`Hsts`].
///
/// `same-origin`, not the `no-referrer` the assessment suggested: under
/// `no-referrer` browsers send `Origin: null` on same-origin form POSTs, which
/// the CSRF rule must refuse — the login form would break. `same-origin`
/// leaks nothing cross-site and keeps `Origin` truthful.
pub async fn security_headers_mw(
    State(w): State<Arc<WebRules>>,
    req: Request,
    next: Next,
) -> Response {
    let https = is_https(req.extensions());
    let mut resp = next.run(req).await;
    apply_security_headers(resp.headers_mut(), w.hsts, https);
    resp
}

pub fn apply_security_headers(h: &mut HeaderMap, hsts: Hsts, https: bool) {
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    if !h.contains_key(header::X_FRAME_OPTIONS) {
        h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    }
    if !h.contains_key(header::CONTENT_SECURITY_POLICY) {
        h.insert(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static("frame-ancestors 'none'"),
        );
    }
    if !h.contains_key(header::REFERRER_POLICY) {
        h.insert(
            header::REFERRER_POLICY,
            HeaderValue::from_static("same-origin"),
        );
    }
    let max_age = match hsts {
        Hsts::On { max_age_s } => Some(max_age_s),
        Hsts::Auto { max_age_s } if https => Some(max_age_s),
        _ => None,
    };
    if let Some(s) = max_age {
        if let Ok(v) = HeaderValue::from_str(&format!("max-age={s}; includeSubDomains")) {
            h.insert(header::STRICT_TRANSPORT_SECURITY, v);
        }
    }
}

// ---------------------------------------------------------------------------
// The assembled edge
// ---------------------------------------------------------------------------

/// Everything the public listener needs, built once at boot.
#[derive(Clone)]
pub struct Edge {
    pub resolver: Arc<ClientIpResolver>,
    /// Data plane: `QUEEN_EDGE_RPS_PER_IP` (default 0 = off — a producer fleet
    /// behind one NAT would otherwise be throttled as one client).
    pub limiter: Arc<IpRateLimiter>,
    /// Web plane: `QUEEN_EDGE_WEB_RPS_PER_IP` (default 20, burst 60).
    pub web_limiter: Arc<IpRateLimiter>,
    pub limits: Arc<RequestLimits>,
    pub web: Arc<WebRules>,
    /// `QUEEN_EDGE_MAX_BODY_BYTES` (default 64 MiB, the broker's own cap).
    pub max_body_bytes: usize,
    /// `QUEEN_EDGE_WEB_MAX_BODY_BYTES` (default 1 MiB).
    pub web_max_body_bytes: usize,
}

impl Edge {
    pub fn from_env() -> Result<Edge, String> {
        Ok(Edge {
            resolver: Arc::new(ClientIpResolver::from_env()?),
            limiter: Arc::new(IpRateLimiter::new(RateLimitConfig::from_env(
                "QUEEN_EDGE",
                0.0,
                0.0,
            )?)),
            web_limiter: Arc::new(IpRateLimiter::new(RateLimitConfig::from_env(
                "QUEEN_EDGE_WEB",
                20.0,
                60.0,
            )?)),
            limits: Arc::new(RequestLimits::from_env()?),
            web: Arc::new(WebRules::from_env()?),
            max_body_bytes: env_num("QUEEN_EDGE_MAX_BODY_BYTES", 64usize * 1024 * 1024)?,
            web_max_body_bytes: env_num("QUEEN_EDGE_WEB_MAX_BODY_BYTES", 1024usize * 1024)?,
        })
    }

    /// Layers for the data-plane router (outermost first): security headers,
    /// head limits, client IP, per-IP rate limit, CSRF for cookie-carrying
    /// requests only (the webapp's XHR; SDK callers carry no cookie),
    /// timeout, body cap.
    pub fn data_plane<S: Clone + Send + Sync + 'static>(&self, r: Router<S>) -> Router<S> {
        r.layer(body_limit(self.max_body_bytes))
            .layer(from_fn_with_state(self.limits.clone(), timeout_mw))
            .layer(from_fn_with_state(self.web.clone(), csrf_cookie_mw))
            .layer(from_fn_with_state(self.limiter.clone(), rate_limit_mw))
            .layer(from_fn_with_state(self.resolver.clone(), client_ip_mw))
            .layer(from_fn_with_state(self.limits.clone(), header_limits_mw))
            .layer(from_fn_with_state(self.web.clone(), security_headers_mw))
    }

    /// Layers for the cookie-authenticated web routers (console, oauth,
    /// operator), outermost first: security headers, head limits, client IP,
    /// the web rate limit, CORS, CSRF, timeout, a small body cap.
    pub fn web_plane<S: Clone + Send + Sync + 'static>(&self, r: Router<S>) -> Router<S> {
        r.layer(body_limit(self.web_max_body_bytes))
            .layer(from_fn_with_state(self.limits.clone(), timeout_mw))
            .layer(from_fn_with_state(self.web.clone(), csrf_mw))
            .layer(from_fn_with_state(self.web.clone(), cors_mw))
            .layer(from_fn_with_state(self.web_limiter.clone(), rate_limit_mw))
            .layer(from_fn_with_state(self.resolver.clone(), client_ip_mw))
            .layer(from_fn_with_state(self.limits.clone(), header_limits_mw))
            .layer(from_fn_with_state(self.web.clone(), security_headers_mw))
    }

    /// One line for the boot log.
    pub fn describe(&self) -> String {
        let l = self.limiter.config();
        let w = self.web_limiter.config();
        format!(
            "edge: rps/ip={} burst={} web rps/ip={} burst={} trusted_proxies={} timeout={:?} public_origins={:?} cors={} hsts={:?}",
            l.rps,
            l.burst,
            w.rps,
            w.burst,
            self.resolver.trusted.len(),
            self.limits.timeout,
            self.web.public_origins,
            if self.web.cors_any { "*".to_string() } else { format!("{:?}", self.web.cors_origins) },
            self.web.hsts,
        )
    }
}

// ---------------------------------------------------------------------------
// Login brute-force guard
// ---------------------------------------------------------------------------

/// How long the caller must wait before the next login attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryAfter(pub Duration);

impl RetryAfter {
    /// Whole seconds, rounded up, at least 1 (the `Retry-After` value).
    pub fn secs(&self) -> u64 {
        ceil_secs(self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoginGuardConfig {
    /// Failures per IP (window) before backoff starts.
    pub ip_free_failures: u32,
    /// Failures per account (window) before backoff starts.
    pub account_free_failures: u32,
    /// First backoff step; doubles per further failure.
    pub base_backoff: Duration,
    /// The lockout window: the backoff never exceeds it.
    pub lockout: Duration,
    /// A key whose last failure is older than this starts over.
    pub window: Duration,
    /// Upper bound on tracked IPs + accounts (each map).
    pub max_tracked: usize,
}

impl Default for LoginGuardConfig {
    fn default() -> LoginGuardConfig {
        LoginGuardConfig {
            ip_free_failures: 10,
            account_free_failures: 5,
            base_backoff: Duration::from_secs(1),
            lockout: Duration::from_secs(15 * 60),
            window: Duration::from_secs(15 * 60),
            max_tracked: 100_000,
        }
    }
}

impl LoginGuardConfig {
    /// `QUEEN_LOGIN_FREE_FAILURES_IP` (10), `QUEEN_LOGIN_FREE_FAILURES_ACCOUNT`
    /// (5), `QUEEN_LOGIN_BACKOFF_BASE_MS` (1000), `QUEEN_LOGIN_LOCKOUT_S` (900),
    /// `QUEEN_LOGIN_WINDOW_S` (900), `QUEEN_LOGIN_MAX_TRACKED` (100000).
    pub fn from_env() -> Result<LoginGuardConfig, String> {
        let d = LoginGuardConfig::default();
        Ok(LoginGuardConfig {
            ip_free_failures: env_num("QUEEN_LOGIN_FREE_FAILURES_IP", d.ip_free_failures)?,
            account_free_failures: env_num(
                "QUEEN_LOGIN_FREE_FAILURES_ACCOUNT",
                d.account_free_failures,
            )?,
            base_backoff: Duration::from_millis(env_num("QUEEN_LOGIN_BACKOFF_BASE_MS", 1000u64)?),
            lockout: Duration::from_secs(env_num("QUEEN_LOGIN_LOCKOUT_S", 900u64)?),
            window: Duration::from_secs(env_num("QUEEN_LOGIN_WINDOW_S", 900u64)?),
            max_tracked: env_num("QUEEN_LOGIN_MAX_TRACKED", d.max_tracked)?.max(1),
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct Strikes {
    failures: u32,
    last_failure: Instant,
    blocked_until: Option<Instant>,
}

#[derive(Default)]
struct GuardMaps {
    ips: HashMap<IpAddr, Strikes>,
    accounts: HashMap<String, Strikes>,
}

/// Per-IP and per-account login failure counters with exponential backoff
/// capped at a lockout window, in bounded memory. Call [`LoginGuard::check`]
/// BEFORE verifying the password (a blocked attempt never reaches bcrypt, so
/// it is neither an oracle nor a CPU cost), then exactly one of
/// [`LoginGuard::record_failure`] / [`LoginGuard::record_success`].
///
/// The account key is the normalized login name whether or not the account
/// exists, so the guard's answers do not reveal which accounts exist. A
/// per-account lockout lets someone keep a known account's logins slowed
/// down — the standard trade, bounded here by the lockout cap.
pub struct LoginGuard {
    cfg: LoginGuardConfig,
    maps: Mutex<GuardMaps>,
}

impl LoginGuard {
    pub fn new(cfg: LoginGuardConfig) -> LoginGuard {
        LoginGuard {
            cfg,
            maps: Mutex::new(GuardMaps::default()),
        }
    }

    /// The process-wide guard, configured from the environment on first use
    /// (a malformed knob logs and falls back to the defaults).
    pub fn global() -> &'static LoginGuard {
        static G: OnceLock<LoginGuard> = OnceLock::new();
        G.get_or_init(|| {
            let cfg = LoginGuardConfig::from_env().unwrap_or_else(|e| {
                tracing::error!(target: "edge", "login guard config: {e}; using defaults");
                LoginGuardConfig::default()
            });
            LoginGuard::new(cfg)
        })
    }

    pub fn account_key(account: &str) -> String {
        let a = account.trim().to_lowercase();
        // Bounded key size whatever the form carried.
        match a.char_indices().nth(320) {
            Some((i, _)) => a[..i].to_string(),
            None => a,
        }
    }

    fn ip_key(ip: IpAddr) -> IpAddr {
        match canonical_ip(ip) {
            IpAddr::V6(v6) => mask(IpAddr::V6(v6), 64),
            v4 => v4,
        }
    }

    pub fn check(&self, ip: IpAddr, account: &str) -> Result<(), RetryAfter> {
        self.check_at(ip, account, Instant::now())
    }

    pub fn check_at(&self, ip: IpAddr, account: &str, now: Instant) -> Result<(), RetryAfter> {
        let m = lock(&self.maps);
        let wait = [
            m.ips.get(&Self::ip_key(ip)).and_then(|s| s.blocked_until),
            m.accounts
                .get(&Self::account_key(account))
                .and_then(|s| s.blocked_until),
        ]
        .into_iter()
        .flatten()
        .filter(|until| *until > now)
        .map(|until| until - now)
        .max();
        match wait {
            Some(w) => Err(RetryAfter(w)),
            None => Ok(()),
        }
    }

    /// Count a failed attempt; returns the wait now in force, if any.
    pub fn record_failure(&self, ip: IpAddr, account: &str) -> Option<RetryAfter> {
        self.record_failure_at(ip, account, Instant::now())
    }

    pub fn record_failure_at(&self, ip: IpAddr, account: &str, now: Instant) -> Option<RetryAfter> {
        let cfg = self.cfg;
        let mut m = lock(&self.maps);
        let a = strike(
            &mut m.ips,
            Self::ip_key(ip),
            cfg.ip_free_failures,
            &cfg,
            now,
        );
        let b = strike(
            &mut m.accounts,
            Self::account_key(account),
            cfg.account_free_failures,
            &cfg,
            now,
        );
        a.into_iter().chain(b).max().map(RetryAfter)
    }

    /// A successful login clears the ACCOUNT's strikes. The IP's stay and age
    /// out: otherwise one valid account would let its owner reset the IP
    /// counter between guesses at other accounts.
    pub fn record_success(&self, _ip: IpAddr, account: &str) {
        lock(&self.maps)
            .accounts
            .remove(&Self::account_key(account));
    }

    /// Tracked (IPs, accounts) — tests and metrics.
    pub fn tracked(&self) -> (usize, usize) {
        let m = lock(&self.maps);
        (m.ips.len(), m.accounts.len())
    }
}

fn strike<K: std::hash::Hash + Eq + Clone>(
    map: &mut HashMap<K, Strikes>,
    key: K,
    free: u32,
    cfg: &LoginGuardConfig,
    now: Instant,
) -> Option<Duration> {
    if !map.contains_key(&key) && map.len() >= cfg.max_tracked {
        // Drop what has aged out and is not blocked; if the map is still
        // full, evict an arbitrary unblocked entry, and only then a blocked
        // one (memory is bounded no matter what an attacker sprays).
        map.retain(|_, s| {
            s.blocked_until.is_some_and(|u| u > now)
                || now.saturating_duration_since(s.last_failure) < cfg.window
        });
        if map.len() >= cfg.max_tracked {
            let victim = map
                .iter()
                .find(|(_, s)| s.blocked_until.is_none_or(|u| u <= now))
                .or_else(|| map.iter().next())
                .map(|(k, _)| k.clone());
            if let Some(k) = victim {
                map.remove(&k);
            }
        }
    }
    let s = map.entry(key).or_insert(Strikes {
        failures: 0,
        last_failure: now,
        blocked_until: None,
    });
    if now.saturating_duration_since(s.last_failure) >= cfg.window {
        s.failures = 0;
        s.blocked_until = None;
    }
    s.failures = s.failures.saturating_add(1);
    s.last_failure = now;
    if s.failures > free {
        let step = (s.failures - free - 1).min(30);
        let backoff = cfg
            .base_backoff
            .saturating_mul(1u32 << step)
            .min(cfg.lockout);
        s.blocked_until = Some(now + backoff);
        Some(backoff)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// TLS and serving
// ---------------------------------------------------------------------------

/// `{prefix}_TLS_CERT` / `{prefix}_TLS_KEY` (PEM paths). Both or neither:
/// half a configuration is an error, not a silent plaintext listener.
/// Prefixes in use: `QUEEN` (the broker's public listener), `QUEEN_RAFT` (the
/// raft RPC listener).
pub fn tls_paths_from_env(prefix: &str) -> Result<Option<(String, String)>, String> {
    let (ck, kk) = (format!("{prefix}_TLS_CERT"), format!("{prefix}_TLS_KEY"));
    match (env_opt(&ck), env_opt(&kk)) {
        (Some(c), Some(k)) => Ok(Some((c, k))),
        (None, None) => Ok(None),
        (Some(_), None) => Err(format!("{ck} set without {kk}")),
        (None, Some(_)) => Err(format!("{kk} set without {ck}")),
    }
}

/// A rustls server config from `{prefix}_TLS_CERT` / `{prefix}_TLS_KEY`, or
/// `None` when neither is set.
pub fn tls_config_from_env(prefix: &str) -> Result<Option<Arc<rustls::ServerConfig>>, String> {
    match tls_paths_from_env(prefix)? {
        None => Ok(None),
        Some((c, k)) => {
            let cert = std::fs::read(&c).map_err(|e| format!("read {c}: {e}"))?;
            let key = std::fs::read(&k).map_err(|e| format!("read {k}: {e}"))?;
            tls_server_config_from_pem(&cert, &key).map(|cfg| Some(Arc::new(cfg)))
        }
    }
}

/// rustls (ring) server config from a PEM chain (leaf first) and a PEM private
/// key (PKCS#8, PKCS#1 or SEC1). HTTP/1.1 only, like the listener.
pub fn tls_server_config_from_pem(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<rustls::ServerConfig, String> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert_pem)
        .collect::<Result<_, _>>()
        .map_err(|e| format!("certificate PEM: {e}"))?;
    if chain.is_empty() {
        return Err("no CERTIFICATE block found".into());
    }
    let key =
        PrivateKeyDer::from_pem_slice(key_pem).map_err(|e| format!("private key PEM: {e}"))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut cfg = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|e| e.to_string())?;
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(cfg)
}

#[derive(Clone, Copy, Debug)]
pub struct ServeOptions {
    /// Slowloris bound: the whole request head must arrive within this.
    pub header_read_timeout: Duration,
    pub tls_handshake_timeout: Duration,
    /// Accepted connections beyond this are closed at once.
    pub max_connections: usize,
    /// hyper's own header-count limit (answers `431` before any layer runs).
    pub max_headers: usize,
    /// How long live connections get to finish after shutdown is signalled.
    pub shutdown_grace: Duration,
}

impl Default for ServeOptions {
    fn default() -> ServeOptions {
        ServeOptions {
            header_read_timeout: Duration::from_secs(30),
            tls_handshake_timeout: Duration::from_secs(10),
            max_connections: 20_000,
            max_headers: 100,
            shutdown_grace: Duration::from_secs(25),
        }
    }
}

impl ServeOptions {
    /// `QUEEN_EDGE_HEADER_READ_TIMEOUT_MS`, `QUEEN_EDGE_TLS_HANDSHAKE_TIMEOUT_MS`,
    /// `QUEEN_EDGE_MAX_CONNS`, `QUEEN_EDGE_MAX_HEADERS`, `QUEEN_EDGE_SHUTDOWN_GRACE_MS`.
    pub fn from_env() -> Result<ServeOptions, String> {
        let d = ServeOptions::default();
        Ok(ServeOptions {
            header_read_timeout: Duration::from_millis(env_num(
                "QUEEN_EDGE_HEADER_READ_TIMEOUT_MS",
                30_000u64,
            )?),
            tls_handshake_timeout: Duration::from_millis(env_num(
                "QUEEN_EDGE_TLS_HANDSHAKE_TIMEOUT_MS",
                10_000u64,
            )?),
            max_connections: env_num("QUEEN_EDGE_MAX_CONNS", d.max_connections)?.max(1),
            max_headers: env_num("QUEEN_EDGE_MAX_HEADERS", d.max_headers)?,
            shutdown_grace: Duration::from_millis(env_num(
                "QUEEN_EDGE_SHUTDOWN_GRACE_MS",
                25_000u64,
            )?),
        })
    }
}

trait Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin> Io for T {}

/// Serve `app` on an already-bound listener, plain or TLS. Every request
/// carries `ConnectInfo<SocketAddr>` and [`ConnMeta`], so [`client_ip_mw`] and
/// [`is_https`] work. HTTP/1.1 (hyper), with a header-read timeout, a TLS
/// handshake timeout and a connection cap. Returns after `shutdown` resolves
/// and live connections finished (or the grace elapsed).
pub async fn serve(
    listener: tokio::net::TcpListener,
    app: Router,
    tls: Option<Arc<rustls::ServerConfig>>,
    opts: ServeOptions,
    shutdown: impl Future<Output = ()> + Send,
) {
    let acceptor = tls.map(tokio_rustls::TlsAcceptor::from);
    let conns = Arc::new(tokio::sync::Semaphore::new(opts.max_connections));
    let (shut_tx, shut_rx) = tokio::sync::watch::channel(false);
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let accepted = tokio::select! {
            _ = &mut shutdown => break,
            a = listener.accept() => a,
        };
        let (stream, peer) = match accepted {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(target: "edge", err = %e, "accept failed");
                // EMFILE and friends: do not spin.
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let Ok(permit) = conns.clone().try_acquire_owned() else {
            tracing::warn!(target: "edge", peer = %peer, max = opts.max_connections, "connection cap reached; closing");
            continue;
        };
        let _ = stream.set_nodelay(true);
        let acceptor = acceptor.clone();
        let app = app.clone();
        let mut shut_rx = shut_rx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let meta = ConnMeta {
                peer,
                tls: acceptor.is_some(),
            };
            let io: Box<dyn Io> = match acceptor {
                None => Box::new(stream),
                Some(acc) => {
                    match tokio::time::timeout(opts.tls_handshake_timeout, acc.accept(stream)).await
                    {
                        Ok(Ok(s)) => Box::new(s),
                        Ok(Err(e)) => {
                            tracing::debug!(target: "edge", peer = %peer, err = %e, "tls handshake failed");
                            return;
                        }
                        Err(_) => {
                            tracing::debug!(target: "edge", peer = %peer, "tls handshake timed out");
                            return;
                        }
                    }
                }
            };
            let svc = tower::ServiceExt::map_request(
                app,
                move |mut req: axum::http::Request<hyper::body::Incoming>| {
                    req.extensions_mut().insert(ConnectInfo(peer));
                    req.extensions_mut().insert(meta);
                    req
                },
            );
            let svc = hyper_util::service::TowerToHyperService::new(svc);
            let mut builder = hyper::server::conn::http1::Builder::new();
            builder
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(opts.header_read_timeout)
                .max_headers(opts.max_headers);
            let conn = builder.serve_connection(hyper_util::rt::TokioIo::new(io), svc);
            tokio::pin!(conn);
            let mut asked = false;
            loop {
                tokio::select! {
                    res = conn.as_mut() => {
                        if let Err(e) = res {
                            tracing::debug!(target: "edge", peer = %peer, err = %e, "connection ended");
                        }
                        break;
                    }
                    _ = shut_rx.changed(), if !asked => {
                        asked = true;
                        conn.as_mut().graceful_shutdown();
                    }
                }
            }
        });
    }
    let _ = shut_tx.send(true);
    drop(listener);
    // Every live connection holds a permit: all permits back = all done.
    let all = u32::try_from(opts.max_connections).unwrap_or(u32::MAX);
    if tokio::time::timeout(opts.shutdown_grace, conns.acquire_many(all))
        .await
        .is_err()
    {
        tracing::warn!(target: "edge", "connections still live after the shutdown grace; closing anyway");
    }
}

// ---------------------------------------------------------------------------
// Fuzz entry points
// ---------------------------------------------------------------------------

/// Entry points for `proxy/fuzz` (cargo-fuzz). Each takes arbitrary bytes and
/// must never panic; the seed-corpus tests below run them on stable.
#[doc(hidden)]
pub mod fuzz {
    use super::*;

    fn split3(data: &[u8]) -> (&str, &str, &str) {
        let s = std::str::from_utf8(data).unwrap_or("");
        let mut it = s.splitn(3, '\n');
        (
            it.next().unwrap_or(""),
            it.next().unwrap_or(""),
            it.next().unwrap_or(""),
        )
    }

    /// The route classifier and the long-poll predicates:
    /// `METHOD\nPATH\nQUERY`.
    pub fn route_classify(data: &[u8]) {
        let (m, path, query) = split3(data);
        let method = Method::from_bytes(m.as_bytes()).unwrap_or(Method::GET);
        let _ = crate::routes::classify(&method, path);
        let q = (!query.is_empty()).then_some(query);
        let _ = crate::routes::is_wait_pop(path, q);
        let _ = crate::routes::poll_timeout_ms(q);
        let _ = RequestLimits::default().timeout_for(path, q);
    }

    /// The edge's own header parsers: forwarding chains, CIDRs, origins,
    /// cookies and the CSRF verdict: `XFF\nORIGIN-OR-REFERER\nCOOKIE`.
    pub fn edge_headers(data: &[u8]) {
        let (xff, origin, cookie) = split3(data);
        let _ = IpNet::parse(xff);
        let _ = parse_forwarded_ip(origin);
        let _ = normalize_origin(origin);
        let resolver = ClientIpResolver::new(
            parse_trusted_proxies(&["private".to_string()]).unwrap_or_default(),
            None,
        )
        .expect("static header name");
        let mut h = HeaderMap::new();
        for (name, v) in [
            ("x-forwarded-for", xff),
            ("origin", origin),
            ("referer", origin),
            ("cookie", cookie),
            ("host", xff),
        ] {
            if let Ok(v) = HeaderValue::from_str(v) {
                h.insert(HeaderName::from_static(name), v);
            }
        }
        let peer = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let ip = resolver.resolve(peer, &h);
        let _ = resolver.forwarded_https(peer, &h);
        let rules = WebRules {
            public_origins: Vec::new(),
            session_cookies: vec!["queen_session".into()],
            cors_origins: Vec::new(),
            cors_any: false,
            cors_methods: String::new(),
            cors_headers: String::new(),
            cors_max_age_s: 0,
            hsts: Hsts::Off,
        };
        for m in [Method::POST, Method::DELETE, Method::GET] {
            let _ = rules.csrf_verdict(&m, &h);
        }
        let guard = LoginGuard::new(LoginGuardConfig {
            max_tracked: 4,
            ..LoginGuardConfig::default()
        });
        let _ = guard.record_failure(ip, cookie);
        let _ = guard.check(ip, cookie);
        let _ = RequestLimits::default().check_head(xff.len(), &h);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::{get, post};
    use tower::ServiceExt;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn hdrs(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.append(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    // --- client IP (SECURITY-ASSESSMENT #3) ---------------------------------

    #[test]
    fn cidr_parse_and_contains() {
        let n = IpNet::parse("10.1.2.3/8").unwrap();
        assert!(n.contains(ip("10.200.0.1")));
        assert!(!n.contains(ip("11.0.0.1")));
        assert!(IpNet::parse("::ffff:10.0.0.1")
            .unwrap()
            .contains(ip("10.0.0.1")));
        assert!(IpNet::parse("fd00::/8").unwrap().contains(ip("fd12::1")));
        assert!(!IpNet::parse("fd00::/8").unwrap().contains(ip("10.0.0.1")));
        assert!(IpNet::parse("0.0.0.0/0").unwrap().contains(ip("8.8.8.8")));
        assert!(IpNet::parse("10.0.0.0/33").is_err());
        assert!(IpNet::parse("nope").is_err());
        assert!(IpNet::parse("10.0.0.0/x").is_err());
        let t = parse_trusted_proxies(&["loopback".into(), "private".into()]).unwrap();
        assert!(t.iter().any(|n| n.contains(ip("192.168.1.1"))));
        assert!(t.iter().any(|n| n.contains(ip("::1"))));
    }

    #[test]
    fn xff_is_ignored_from_an_untrusted_peer() {
        let r = ClientIpResolver::new(parse_trusted_proxies(&["10.0.0.0/8".into()]).unwrap(), None)
            .unwrap();
        let h = hdrs(&[("x-forwarded-for", "1.2.3.4")]);
        // The finding: a direct client rotating XFF must not become a new key.
        assert_eq!(r.resolve(ip("203.0.113.9"), &h), ip("203.0.113.9"));
        assert_eq!(
            r.resolve(ip("203.0.113.9"), &HeaderMap::new()),
            ip("203.0.113.9")
        );
    }

    #[test]
    fn xff_is_walked_right_to_left_behind_trusted_proxies() {
        let r = ClientIpResolver::new(parse_trusted_proxies(&["10.0.0.0/8".into()]).unwrap(), None)
            .unwrap();
        // client spoofs a left-most entry; the edge (10.0.0.5) appended the
        // real client (198.51.100.7); the peer is the last proxy (10.0.0.9).
        let h = hdrs(&[("x-forwarded-for", "6.6.6.6, 198.51.100.7, 10.0.0.5")]);
        assert_eq!(r.resolve(ip("10.0.0.9"), &h), ip("198.51.100.7"));
        // Two header lines are one list.
        let h = hdrs(&[
            ("x-forwarded-for", "6.6.6.6"),
            ("x-forwarded-for", "198.51.100.7:4411"),
        ]);
        assert_eq!(r.resolve(ip("10.0.0.9"), &h), ip("198.51.100.7"));
        // Every hop trusted: the left-most one.
        let h = hdrs(&[("x-forwarded-for", "10.1.1.1, 10.2.2.2")]);
        assert_eq!(r.resolve(ip("10.0.0.9"), &h), ip("10.1.1.1"));
        // A garbage hop ends the walk at the last address we could vouch for.
        let h = hdrs(&[("x-forwarded-for", "6.6.6.6, garbage, 10.0.0.5")]);
        assert_eq!(r.resolve(ip("10.0.0.9"), &h), ip("10.0.0.5"));
        // No header: the peer.
        assert_eq!(r.resolve(ip("10.0.0.9"), &HeaderMap::new()), ip("10.0.0.9"));
        // IPv6 with brackets and a port; v4-mapped peers are v4.
        let h = hdrs(&[("x-forwarded-for", "[2001:db8::7]:443")]);
        assert_eq!(r.resolve(ip("::ffff:10.0.0.9"), &h), ip("2001:db8::7"));
    }

    #[test]
    fn single_ip_header_mode() {
        let r = ClientIpResolver::new(
            parse_trusted_proxies(&["10.0.0.0/8".into()]).unwrap(),
            Some("CF-Connecting-IP"),
        )
        .unwrap();
        let h = hdrs(&[
            ("cf-connecting-ip", "198.51.100.7"),
            ("x-forwarded-for", "6.6.6.6"),
        ]);
        assert_eq!(r.resolve(ip("10.0.0.9"), &h), ip("198.51.100.7"));
        assert_eq!(r.resolve(ip("8.8.8.8"), &h), ip("8.8.8.8"));
        assert!(r.forwarded_https(ip("10.0.0.9"), &hdrs(&[("x-forwarded-proto", "https")])));
        assert!(!r.forwarded_https(ip("8.8.8.8"), &hdrs(&[("x-forwarded-proto", "https")])));
    }

    // --- rate limiter -------------------------------------------------------

    fn limiter(rps: f64, burst: f64, max: usize) -> IpRateLimiter {
        IpRateLimiter::new(RateLimitConfig {
            rps,
            burst,
            max_tracked: max,
            ipv6_prefix: 64,
        })
    }

    #[test]
    fn token_bucket_burst_then_refill() {
        let l = limiter(10.0, 3.0, 1000);
        let t0 = Instant::now();
        let a = ip("198.51.100.1");
        for _ in 0..3 {
            assert!(l.check_at(a, t0).is_ok());
        }
        let retry = l.check_at(a, t0).unwrap_err();
        assert!(
            retry <= Duration::from_millis(100) && retry > Duration::ZERO,
            "{retry:?}"
        );
        // Another client is unaffected.
        assert!(l.check_at(ip("198.51.100.2"), t0).is_ok());
        // 100 ms later one token is back.
        assert!(l.check_at(a, t0 + Duration::from_millis(100)).is_ok());
        assert!(l.check_at(a, t0 + Duration::from_millis(100)).is_err());
        // Disabled limiter lets everything through.
        let off = limiter(0.0, 1.0, 10);
        for _ in 0..100 {
            assert!(off.check_at(a, t0).is_ok());
        }
    }

    #[test]
    fn ipv6_clients_share_their_64() {
        let l = limiter(1.0, 1.0, 1000);
        let t0 = Instant::now();
        assert!(l.check_at(ip("2001:db8:1:2::1"), t0).is_ok());
        assert!(
            l.check_at(ip("2001:db8:1:2:ffff::9"), t0).is_err(),
            "same /64"
        );
        assert!(l.check_at(ip("2001:db8:1:3::1"), t0).is_ok(), "another /64");
    }

    #[test]
    fn tracked_keys_stay_bounded() {
        let l = limiter(1.0, 5.0, 64);
        let t0 = Instant::now();
        for i in 0..10_000u32 {
            let a = IpAddr::V4(Ipv4Addr::from(0x0a00_0000 + i));
            let _ = l.check_at(a, t0 + Duration::from_millis(u64::from(i)));
        }
        assert!(l.tracked() <= 64, "tracked {}", l.tracked());
    }

    // --- request limits -----------------------------------------------------

    #[test]
    fn timeouts_honour_long_poll_pops() {
        let l = RequestLimits {
            timeout_exempt: vec!["/streams/".into()],
            ..RequestLimits::default()
        };
        assert_eq!(
            l.timeout_for("/api/v1/push", None),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            l.timeout_for("/api/v1/pop/queue/q", Some("wait=true&timeout=120000")),
            Some(Duration::from_secs(130))
        );
        assert_eq!(
            l.timeout_for("/api/v1/pop/queue/q", Some("wait=true")),
            Some(Duration::from_secs(40))
        );
        assert_eq!(
            l.timeout_for("/api/v1/pop/queue/q", Some("wait=true&timeout=99999999")),
            Some(Duration::from_secs(310)),
            "capped"
        );
        assert_eq!(l.timeout_for("/streams/v1/cycle", None), None);
    }

    #[test]
    fn head_limits() {
        let l = RequestLimits {
            max_headers: 3,
            max_header_bytes: 64,
            max_header_value_bytes: 32,
            max_uri_bytes: 20,
            ..RequestLimits::default()
        };
        assert!(l.check_head(10, &hdrs(&[("a", "b")])).is_ok());
        assert_eq!(
            l.check_head(21, &HeaderMap::new()).unwrap_err().0,
            StatusCode::URI_TOO_LONG
        );
        let many = hdrs(&[("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")]);
        assert_eq!(
            l.check_head(1, &many).unwrap_err().0,
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
        );
        let big = "x".repeat(33);
        assert!(l.check_head(1, &hdrs(&[("a", &big)])).is_err());
        let total = "y".repeat(30);
        assert!(l
            .check_head(1, &hdrs(&[("a", &total), ("b", &total), ("c", &total)]))
            .is_err());
    }

    // --- web rules ----------------------------------------------------------

    fn rules(origins: &[&str]) -> WebRules {
        WebRules {
            public_origins: origins
                .iter()
                .map(|o| normalize_origin(o).unwrap())
                .collect(),
            session_cookies: vec!["queen_session".into(), "__Host-queen_session_cell".into()],
            cors_origins: vec![normalize_origin("https://app.example.com").unwrap()],
            cors_any: false,
            cors_methods: "GET, POST".into(),
            cors_headers: "content-type".into(),
            cors_max_age_s: 60,
            hsts: Hsts::Auto { max_age_s: 100 },
        }
    }

    #[test]
    fn origin_normalization() {
        assert_eq!(
            normalize_origin("HTTPS://Console.Example.com:443").as_deref(),
            Some("https://console.example.com")
        );
        assert_eq!(
            normalize_origin("http://localhost:6632/x?y#z").as_deref(),
            Some("http://localhost:6632")
        );
        assert_eq!(
            normalize_origin("https://u:p@example.com/login").as_deref(),
            Some("https://example.com")
        );
        assert_eq!(
            normalize_origin("http://[::1]:8080").as_deref(),
            Some("http://[::1]:8080")
        );
        assert_eq!(normalize_origin("null"), None);
        assert_eq!(normalize_origin("javascript://x"), None);
        assert_eq!(normalize_origin("https://exa mple.com"), None);
        assert_eq!(normalize_origin("https://example.com:99999"), None);
    }

    #[test]
    fn csrf_rules() {
        let w = rules(&["https://console.example.com"]);
        let cookie = ("cookie", "a=b; queen_session=tok");
        // Safe methods always pass.
        assert!(w
            .csrf_verdict(
                &Method::GET,
                &hdrs(&[cookie, ("origin", "https://evil.test")])
            )
            .is_ok());
        // Allowed origin passes, foreign origin fails, with or without cookie.
        assert!(w
            .csrf_verdict(
                &Method::POST,
                &hdrs(&[cookie, ("origin", "https://console.example.com")])
            )
            .is_ok());
        assert!(w
            .csrf_verdict(
                &Method::POST,
                &hdrs(&[cookie, ("origin", "https://evil.test")])
            )
            .is_err());
        assert!(
            w.csrf_verdict(&Method::POST, &hdrs(&[("origin", "https://evil.test")]))
                .is_err(),
            "login CSRF"
        );
        assert!(w
            .csrf_verdict(&Method::DELETE, &hdrs(&[cookie, ("origin", "null")]))
            .is_err());
        // Referer fallback.
        assert!(w
            .csrf_verdict(
                &Method::POST,
                &hdrs(&[cookie, ("referer", "https://console.example.com/keys")])
            )
            .is_ok());
        assert!(w
            .csrf_verdict(
                &Method::POST,
                &hdrs(&[cookie, ("referer", "https://evil.test/")])
            )
            .is_err());
        // Sec-Fetch-Site fallback.
        assert!(w
            .csrf_verdict(
                &Method::POST,
                &hdrs(&[cookie, ("sec-fetch-site", "same-origin")])
            )
            .is_ok());
        assert!(w
            .csrf_verdict(
                &Method::POST,
                &hdrs(&[cookie, ("sec-fetch-site", "cross-site")])
            )
            .is_err());
        // Cookie but no browser evidence at all: refused. No cookie: an SDK.
        assert!(w.csrf_verdict(&Method::POST, &hdrs(&[cookie])).is_err());
        assert!(w
            .csrf_verdict(
                &Method::POST,
                &hdrs(&[("cookie", "__Host-queen_session_cell=t")])
            )
            .is_err());
        assert!(w.csrf_verdict(&Method::POST, &HeaderMap::new()).is_ok());
        assert!(w
            .csrf_verdict(&Method::POST, &hdrs(&[("cookie", "other=1")]))
            .is_ok());
        // Same origin is always allowed, configured list or not.
        assert!(w
            .csrf_verdict(
                &Method::POST,
                &hdrs(&[
                    cookie,
                    ("host", "cell-7.example.com"),
                    ("origin", "https://cell-7.example.com")
                ])
            )
            .is_ok());
        // Data-plane variant: only cookie-carrying requests are judged.
        assert!(w
            .csrf_verdict_cookie_only(&Method::POST, &hdrs(&[("origin", "https://sdk-user.test")]))
            .is_ok());
        assert!(w
            .csrf_verdict_cookie_only(
                &Method::POST,
                &hdrs(&[cookie, ("origin", "https://evil.test")])
            )
            .is_err());
        // No configured origin: same-origin against Host.
        let same = rules(&[]);
        assert!(same
            .csrf_verdict(
                &Method::POST,
                &hdrs(&[
                    cookie,
                    ("host", "localhost:6632"),
                    ("origin", "http://localhost:6632")
                ])
            )
            .is_ok());
        assert!(same
            .csrf_verdict(
                &Method::POST,
                &hdrs(&[
                    cookie,
                    ("host", "example.com"),
                    ("origin", "https://example.com")
                ])
            )
            .is_ok());
        assert!(same
            .csrf_verdict(
                &Method::POST,
                &hdrs(&[
                    cookie,
                    ("host", "localhost:6632"),
                    ("origin", "http://localhost:6633")
                ])
            )
            .is_err());
    }

    #[test]
    fn security_headers_and_hsts() {
        let mut h = HeaderMap::new();
        apply_security_headers(&mut h, Hsts::Auto { max_age_s: 100 }, false);
        assert_eq!(h["x-content-type-options"], "nosniff");
        assert_eq!(h["x-frame-options"], "DENY");
        assert_eq!(h["content-security-policy"], "frame-ancestors 'none'");
        assert_eq!(h["referrer-policy"], "same-origin");
        assert!(
            !h.contains_key("strict-transport-security"),
            "auto: not over http"
        );
        let mut h = HeaderMap::new();
        h.insert(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static("default-src 'self'"),
        );
        apply_security_headers(&mut h, Hsts::Auto { max_age_s: 100 }, true);
        assert_eq!(
            h["content-security-policy"], "default-src 'self'",
            "a handler's CSP wins"
        );
        assert_eq!(
            h["strict-transport-security"],
            "max-age=100; includeSubDomains"
        );
        let mut h = HeaderMap::new();
        apply_security_headers(&mut h, Hsts::Off, true);
        assert!(!h.contains_key("strict-transport-security"));
    }

    // --- the assembled layers, through a real Router ------------------------

    fn edge(rps: f64, burst: f64) -> Edge {
        Edge {
            resolver: Arc::new(
                ClientIpResolver::new(parse_trusted_proxies(&["10.0.0.0/8".into()]).unwrap(), None)
                    .unwrap(),
            ),
            limiter: Arc::new(limiter(rps, burst, 1000)),
            web_limiter: Arc::new(limiter(rps, burst, 1000)),
            limits: Arc::new(RequestLimits {
                timeout: Some(Duration::from_millis(50)),
                ..RequestLimits::default()
            }),
            web: Arc::new(rules(&["https://console.example.com"])),
            max_body_bytes: 1024,
            web_max_body_bytes: 16,
        }
    }

    fn req(method: &str, uri: &str, peer: &str, headers: &[(&str, &str)], body: &str) -> Request {
        let mut b = axum::http::Request::builder().method(method).uri(uri);
        for (k, v) in headers {
            b = b.header(*k, *v);
        }
        let mut r = b.body(Body::from(body.to_string())).unwrap();
        r.extensions_mut().insert(ConnectInfo::<SocketAddr>(
            format!("{peer}:5555").parse().unwrap(),
        ));
        r
    }

    #[tokio::test]
    async fn forwarded_proto_is_what_the_edge_verified() {
        let app = edge(100.0, 100.0).data_plane(Router::new().route(
            "/p",
            post(|h: HeaderMap| async move {
                h.get("x-forwarded-proto")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("-")
                    .to_string()
            }),
        ));
        let proto = |peer: &'static str, hdrs: &'static [(&'static str, &'static str)]| {
            let app = app.clone();
            async move {
                let resp = app.oneshot(req("POST", "/p", peer, hdrs, "")).await.unwrap();
                let b = axum::body::to_bytes(resp.into_body(), 64).await.unwrap();
                String::from_utf8_lossy(&b).to_string()
            }
        };
        // A direct client cannot claim HTTPS (it would mint Secure cookies and
        // https redirect URIs over plain HTTP).
        assert_eq!(proto("203.0.113.9", &[("x-forwarded-proto", "https")]).await, "-");
        // A trusted proxy can.
        assert_eq!(proto("10.0.0.2", &[("x-forwarded-proto", "https")]).await, "https");
        assert_eq!(proto("10.0.0.2", &[("x-forwarded-proto", "http")]).await, "-");
    }

    #[tokio::test]
    async fn data_plane_rate_limits_by_real_peer_not_xff() {
        let app = edge(1.0, 2.0).data_plane(Router::new().route(
            "/api/v1/push",
            post(|axum::Extension(c): axum::Extension<ClientIp>| async move { c.0.to_string() }),
        ));
        let mut codes = Vec::new();
        for i in 0..4 {
            // A direct client rotating XFF: still one key.
            let xff = format!("1.2.3.{i}");
            let r = req(
                "POST",
                "/api/v1/push",
                "203.0.113.9",
                &[("x-forwarded-for", &xff)],
                "{}",
            );
            let resp = app.clone().oneshot(r).await.unwrap();
            codes.push(resp.status().as_u16());
            if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                assert!(resp.headers().contains_key(header::RETRY_AFTER));
                let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
                assert!(String::from_utf8_lossy(&body).contains("rate_limited"));
            } else {
                let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
                assert_eq!(&body[..], b"203.0.113.9", "ClientIp is the socket peer");
            }
        }
        assert_eq!(codes, vec![200, 200, 429, 429]);
        // Behind a trusted proxy the forwarded client is the key.
        let r = req(
            "POST",
            "/api/v1/push",
            "10.0.0.2",
            &[("x-forwarded-for", "198.51.100.4")],
            "{}",
        );
        let resp = app.clone().oneshot(r).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        assert_eq!(&body[..], b"198.51.100.4");
        assert_eq!(resp_headers_nosniff(&app).await, "nosniff");
    }

    #[tokio::test]
    async fn data_plane_csrf_only_judges_cookie_requests() {
        let app =
            edge(0.0, 1.0).data_plane(Router::new().route("/api/v1/push", post(|| async { "ok" })));
        // An SDK in someone else's page, bearer only: through.
        let r = req(
            "POST",
            "/api/v1/push",
            "10.0.0.1",
            &[("origin", "https://customer.test")],
            "{}",
        );
        assert_eq!(
            app.clone().oneshot(r).await.unwrap().status(),
            StatusCode::OK
        );
        // The session cookie riding a cross-site POST: refused.
        let r = req(
            "POST",
            "/api/v1/push",
            "10.0.0.1",
            &[
                ("origin", "https://evil.test"),
                ("cookie", "queen_session=t"),
            ],
            "{}",
        );
        assert_eq!(
            app.clone().oneshot(r).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
        // The webapp's own same-origin XHR: through.
        let r = req(
            "POST",
            "/api/v1/push",
            "10.0.0.1",
            &[
                ("origin", "http://cell.test:6632"),
                ("host", "cell.test:6632"),
                ("cookie", "queen_session=t"),
            ],
            "{}",
        );
        assert_eq!(
            app.clone().oneshot(r).await.unwrap().status(),
            StatusCode::OK
        );
    }

    async fn resp_headers_nosniff(app: &Router) -> String {
        let r = req("POST", "/api/v1/push", "10.0.0.3", &[], "{}");
        let resp = app.clone().oneshot(r).await.unwrap();
        resp.headers()["x-content-type-options"]
            .to_str()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn timeout_layer_answers_504_but_not_for_long_polls() {
        let app = edge(0.0, 1.0).data_plane(
            Router::new()
                .route(
                    "/slow",
                    get(|| async {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        "late"
                    }),
                )
                .route(
                    "/api/v1/pop/queue/:q",
                    get(|| async {
                        tokio::time::sleep(Duration::from_millis(120)).await;
                        "popped"
                    }),
                ),
        );
        let resp = app
            .clone()
            .oneshot(req("GET", "/slow", "10.0.0.1", &[], ""))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
        let resp = app
            .clone()
            .oneshot(req(
                "GET",
                "/api/v1/pop/queue/q?wait=true&timeout=100",
                "10.0.0.1",
                &[],
                "",
            ))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "a long poll gets its own timeout + grace"
        );
    }

    #[tokio::test]
    async fn web_plane_csrf_cors_and_body_cap() {
        let app = edge(0.0, 1.0).web_plane(Router::new().route(
            "/api/console/keys",
            get(|| async { "list" }).post(|b: String| async move { b }),
        ));
        let cookie = ("cookie", "queen_session=t");
        // Cross-site POST with the session cookie: 403.
        let r = req(
            "POST",
            "/api/console/keys",
            "10.0.0.1",
            &[cookie, ("origin", "https://evil.test")],
            "x",
        );
        let resp = app.clone().oneshot(r).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(resp.headers()["x-frame-options"], "DENY");
        // Same POST from the console origin: through.
        let r = req(
            "POST",
            "/api/console/keys",
            "10.0.0.1",
            &[cookie, ("origin", "https://console.example.com")],
            "ok",
        );
        assert_eq!(
            app.clone().oneshot(r).await.unwrap().status(),
            StatusCode::OK
        );
        // Body over the web cap (16 B): 413 from the extractor.
        let r = req(
            "POST",
            "/api/console/keys",
            "10.0.0.1",
            &[cookie, ("origin", "https://console.example.com")],
            &"z".repeat(64),
        );
        assert_eq!(
            app.clone().oneshot(r).await.unwrap().status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        // CORS preflight: allowlisted origin gets exact echo + credentials.
        let r = req(
            "OPTIONS",
            "/api/console/keys",
            "10.0.0.1",
            &[
                ("origin", "https://app.example.com"),
                ("access-control-request-method", "POST"),
            ],
            "",
        );
        let resp = app.clone().oneshot(r).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            resp.headers()["access-control-allow-origin"],
            "https://app.example.com"
        );
        assert_eq!(resp.headers()["access-control-allow-credentials"], "true");
        // Any other origin: refused, no CORS headers.
        let r = req(
            "OPTIONS",
            "/api/console/keys",
            "10.0.0.1",
            &[
                ("origin", "https://evil.test"),
                ("access-control-request-method", "POST"),
            ],
            "",
        );
        let resp = app.clone().oneshot(r).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert!(!resp.headers().contains_key("access-control-allow-origin"));
        // A simple GET from a foreign origin is served but not readable.
        let r = req(
            "GET",
            "/api/console/keys",
            "10.0.0.1",
            &[("origin", "https://evil.test")],
            "",
        );
        let resp = app.clone().oneshot(r).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(!resp.headers().contains_key("access-control-allow-origin"));
    }

    #[tokio::test]
    async fn cors_wildcard_never_carries_credentials() {
        let mut e = edge(0.0, 1.0);
        let mut w = rules(&[]);
        w.cors_origins.clear();
        w.cors_any = true;
        e.web = Arc::new(w);
        let app = e.web_plane(Router::new().route("/x", get(|| async { "x" })));
        let r = req(
            "GET",
            "/x",
            "10.0.0.1",
            &[("origin", "https://anyone.test")],
            "",
        );
        let resp = app.clone().oneshot(r).await.unwrap();
        assert_eq!(resp.headers()["access-control-allow-origin"], "*");
        assert!(!resp
            .headers()
            .contains_key("access-control-allow-credentials"));
    }

    // --- login guard --------------------------------------------------------

    fn guard() -> LoginGuard {
        LoginGuard::new(LoginGuardConfig {
            ip_free_failures: 4,
            account_free_failures: 2,
            base_backoff: Duration::from_secs(1),
            lockout: Duration::from_secs(60),
            window: Duration::from_secs(600),
            max_tracked: 1000,
        })
    }

    #[test]
    fn login_guard_account_backoff_doubles_and_caps() {
        let g = guard();
        let t0 = Instant::now();
        let a = ip("198.51.100.1");
        assert!(g.check_at(a, "Alice@Example.com", t0).is_ok());
        assert_eq!(g.record_failure_at(a, "alice@example.com", t0), None);
        assert_eq!(g.record_failure_at(a, "ALICE@example.com ", t0), None);
        // Third failure: past the 2 free ones, 1 s.
        assert_eq!(
            g.record_failure_at(a, "alice@example.com", t0),
            Some(RetryAfter(Duration::from_secs(1)))
        );
        assert_eq!(
            g.check_at(a, "alice@example.com", t0),
            Err(RetryAfter(Duration::from_secs(1)))
        );
        // From ANOTHER IP the account is still slowed down.
        assert!(g
            .check_at(ip("203.0.113.5"), "alice@example.com", t0)
            .is_err());
        // After the wait it may try again; next failure doubles.
        let t1 = t0 + Duration::from_secs(1);
        assert!(g.check_at(a, "alice@example.com", t1).is_ok());
        assert_eq!(
            g.record_failure_at(ip("203.0.113.5"), "alice@example.com", t1),
            Some(RetryAfter(Duration::from_secs(2)))
        );
        // Many more: capped at the lockout window.
        let mut last = None;
        for i in 0..20 {
            last = g.record_failure_at(
                ip("203.0.113.6"),
                "alice@example.com",
                t1 + Duration::from_millis(i),
            );
        }
        assert_eq!(last, Some(RetryAfter(Duration::from_secs(60))));
        // Success clears the account.
        g.record_success(a, "alice@example.com");
        assert!(g
            .check_at(
                ip("192.0.2.1"),
                "alice@example.com",
                t1 + Duration::from_secs(1)
            )
            .is_ok());
    }

    #[test]
    fn login_guard_per_ip_spray_is_slowed() {
        let g = guard();
        let t0 = Instant::now();
        let a = ip("198.51.100.1");
        // One IP spraying different accounts: the IP counter trips after 4.
        for i in 0..4 {
            assert_eq!(g.record_failure_at(a, &format!("user{i}"), t0), None);
        }
        assert!(g.record_failure_at(a, "user4", t0).is_some());
        assert!(g.check_at(a, "someone-new", t0).is_err());
        assert!(g.check_at(ip("198.51.100.2"), "someone-new", t0).is_ok());
        // Success on one account does not reset the IP.
        g.record_success(a, "user0");
        assert!(g.check_at(a, "someone-new", t0).is_err());
        // The window forgets.
        let later = t0 + Duration::from_secs(601);
        assert!(g.check_at(a, "someone-new", later).is_ok());
        assert_eq!(
            g.record_failure_at(a, "user9", later),
            None,
            "counter restarted"
        );
    }

    #[test]
    fn login_guard_memory_is_bounded() {
        let g = LoginGuard::new(LoginGuardConfig {
            max_tracked: 50,
            ..LoginGuardConfig::default()
        });
        let t0 = Instant::now();
        for i in 0..5000u32 {
            g.record_failure_at(IpAddr::V4(Ipv4Addr::from(i)), &format!("acct{i}"), t0);
        }
        let (ips, accts) = g.tracked();
        assert!(ips <= 50 && accts <= 50, "{ips} {accts}");
        assert_eq!(
            LoginGuard::account_key(&"é".repeat(1000)).chars().count(),
            320
        );
    }

    // --- TLS + serve --------------------------------------------------------

    #[test]
    fn tls_env_needs_both_halves() {
        std::env::set_var("QUEEN_HARDEN_T1_TLS_CERT", "/nonexistent/c.pem");
        assert!(tls_paths_from_env("QUEEN_HARDEN_T1").is_err());
        std::env::set_var("QUEEN_HARDEN_T1_TLS_KEY", "/nonexistent/k.pem");
        assert!(tls_config_from_env("QUEEN_HARDEN_T1")
            .unwrap_err()
            .contains("read /nonexistent/c.pem"));
        assert!(tls_config_from_env("QUEEN_HARDEN_T2").unwrap().is_none());
        assert!(tls_server_config_from_pem(b"garbage", b"garbage").is_err());
    }

    #[tokio::test]
    async fn serve_stamps_the_socket_peer() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let e = edge(0.0, 1.0);
        let app = e.data_plane(Router::new().route(
            "/who",
            get(
                |axum::Extension(c): axum::Extension<ClientIp>,
                 axum::Extension(m): axum::Extension<ConnMeta>| async move {
                    format!("{} tls={}", c.0, m.tls)
                },
            ),
        ));
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve(l, app, None, ServeOptions::default(), async {
            let _ = stop_rx.await;
        }));
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET /who HTTP/1.1\r\nHost: t\r\nX-Forwarded-For: 6.6.6.6\r\nConnection: close\r\n\r\n").await.unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf);
        assert!(text.starts_with("HTTP/1.1 200"), "{text}");
        assert!(text.ends_with("127.0.0.1 tls=false"), "{text}");
        assert!(
            text.to_ascii_lowercase().contains("x-frame-options: deny"),
            "{text}"
        );
        let _ = stop_tx.send(());
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
    }

    /// TLS end to end: a throwaway CA + leaf minted with the `openssl` CLI at
    /// test time (no key material in the repo); skipped when it is missing.
    #[tokio::test]
    async fn serve_tls_end_to_end() {
        use rustls::pki_types::pem::PemObject;
        let dir = std::env::temp_dir().join(format!("queen-harden-tls-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let run = |args: &[&str]| {
            std::process::Command::new("openssl")
                .args(args)
                .current_dir(&dir)
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
        };
        // named_curve: LibreSSL defaults to explicit EC parameters, which
        // ring (rightly) refuses.
        let ec = [
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-pkeyopt",
            "ec_param_enc:named_curve",
            "-nodes",
        ];
        let ok = run(&[&["req", "-x509"][..], &ec[..], &["-sha256", "-keyout", "ca.key", "-out", "ca.pem", "-days", "2", "-subj", "/CN=queen-test-ca", "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign"][..]].concat())
            && run(&[&["req"][..], &ec[..], &["-keyout", "leaf.key", "-out", "leaf.csr", "-subj", "/CN=localhost"][..]].concat())
            && std::fs::write(dir.join("ext.cnf"), "subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n").is_ok()
            && run(&["x509", "-req", "-sha256", "-in", "leaf.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-out", "leaf.pem", "-days", "2", "-extfile", "ext.cnf"]);
        if !ok {
            eprintln!("serve_tls_end_to_end: openssl unavailable, skipped");
            return;
        }
        std::env::set_var("QUEEN_HARDEN_TLSE2E_TLS_CERT", dir.join("leaf.pem"));
        std::env::set_var("QUEEN_HARDEN_TLSE2E_TLS_KEY", dir.join("leaf.key"));
        let cfg = tls_config_from_env("QUEEN_HARDEN_TLSE2E")
            .unwrap()
            .expect("configured");

        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let e = edge(0.0, 1.0);
        let app =
            e.web_plane(Router::new().route(
                "/who",
                get(|axum::Extension(m): axum::Extension<ConnMeta>| async move {
                    format!("tls={}", m.tls)
                }),
            ));
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve(l, app, Some(cfg), ServeOptions::default(), async {
            let _ = stop_rx.await;
        }));

        let mut roots = rustls::RootCertStore::empty();
        let ca = std::fs::read(dir.join("ca.pem")).unwrap();
        for c in rustls::pki_types::CertificateDer::pem_slice_iter(&ca) {
            roots.add(c.unwrap()).unwrap();
        }
        let client = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        let conn = tokio_rustls::TlsConnector::from(Arc::new(client));
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut s = conn.connect(name, tcp).await.expect("TLS handshake");
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        s.write_all(b"GET /who HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        let _ = s.read_to_end(&mut buf).await;
        let text = String::from_utf8_lossy(&buf).to_ascii_lowercase();
        assert!(text.starts_with("http/1.1 200"), "{text}");
        assert!(text.ends_with("tls=true"), "{text}");
        assert!(
            text.contains("strict-transport-security: max-age=100"),
            "HSTS auto over TLS: {text}"
        );

        // A plaintext client on the TLS port gets no HTTP answer.
        let mut p = tokio::net::TcpStream::connect(addr).await.unwrap();
        p.write_all(b"GET /who HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut b2 = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), p.read_to_end(&mut b2)).await;
        assert!(!String::from_utf8_lossy(&b2).contains("200"));

        let _ = stop_tx.send(());
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- fuzz seeds (the fuzz targets' entry points, on stable) -------------

    #[test]
    fn fuzz_seed_corpus_route_classify() {
        for seed in [
            "POST\n/api/v1/push\n",
            "GET\n/api/v1/pop/queue/orders\nwait=true&timeout=30000",
            "GET\n/api/v1/ephemeral/pop\nqueue=inbox&wait=true&timeout=x",
            "POST\n/api/v1/messages/p1/tx1/retry\n",
            "POST\n/streams/v1/cycle\n",
            "DELETE\n/api/v1/resources/queues/%2e%2e\n",
            "GET\n/internal/../api/v1/system/shared-state\n",
            "\n\n\n",
            "G\u{0}T\n\u{7f}\n&&==&",
        ] {
            fuzz::route_classify(seed.as_bytes());
        }
        fuzz::route_classify(&[0xff, 0xfe, b'\n', 0x00]);
    }

    #[test]
    fn fuzz_seed_corpus_edge_headers() {
        for seed in [
            "1.2.3.4, 10.0.0.1\nhttps://console.example.com\nqueen_session=abc",
            "[2001:db8::1]:443\nhttp://[::1]:8080/x\na=b; queen_session=",
            "garbage,,,\nnull\n;;;=",
            "10.0.0.0/8\nhttps://u:p@host:99999\n=queen_session",
            "::ffff:10.0.0.1/200\njavascript://x\nqueen_session=x; queen_session=y",
            "\n\n",
        ] {
            fuzz::edge_headers(seed.as_bytes());
        }
        fuzz::edge_headers(&[0xff, b'\n', 0xc3, b'\n', 0x80]);
    }
}
