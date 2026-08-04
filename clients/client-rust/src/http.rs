//! Transport: retry, 429 backoff, failover, and Host pinning.
//!
//! Three retry mechanisms sit on top of each other and are deliberately kept
//! apart:
//!
//! 1. **429 in place** — the backend is alive and shedding load. Retried
//!    against the *same* backend with backoff; moving elsewhere would spread
//!    the very traffic the proxy is trying to reduce.
//! 2. **Failover** — a 5xx or a transport fault means this backend is suspect.
//!    Mark it down and try the next one.
//! 3. **Retry** — with a single backend (or failover off) there is nowhere to
//!    move, so the same URL is retried with exponential backoff.
//!
//! A 4xx other than 429 never triggers any of them: it is the caller's
//! mistake, and repeating it just delays the error.

use std::time::Duration;

use rand::Rng;
use serde::de::DeserializeOwned;
use serde::Serialize;

use queen_protocol::ErrorBody;

use crate::config::{Config, RetryKind};
use crate::error::{Error, Result};
use crate::lb::LoadBalancer;

/// Per-request knobs.
#[derive(Debug, Clone, Default)]
pub(crate) struct Opts {
    /// Overrides the client timeout. A long-poll pop needs its own, larger than
    /// the poll window.
    pub timeout: Option<Duration>,
    /// Pins this request to a backend on the affinity ring.
    pub affinity_key: Option<String>,
    pub retry_kind: RetryKindOpt,
}

pub(crate) type RetryKindOpt = Option<RetryKind>;

impl Opts {
    pub fn affinity(key: Option<String>) -> Self {
        Self {
            affinity_key: key,
            ..Default::default()
        }
    }

    pub fn kind(mut self, kind: RetryKind) -> Self {
        self.retry_kind = Some(kind);
        self
    }

    pub fn timeout(mut self, t: Duration) -> Self {
        self.timeout = Some(t);
        self
    }
}

pub(crate) struct HttpClient {
    /// One entry per backend when a Host override is configured (each pins its
    /// connections to that backend's address while advertising the same virtual
    /// Host); otherwise a single shared client used for every backend.
    clients: Vec<reqwest::Client>,
    lb: LoadBalancer,
    config: Config,
}

impl HttpClient {
    pub fn new(config: Config) -> Result<Self> {
        config.validate()?;

        let lb = LoadBalancer::new(
            config.urls.clone(),
            config.strategy,
            config.affinity_hash_ring,
            config.health_retry_after,
        );

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        if let Some(token) = &config.bearer_token {
            let mut v = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| Error::Config("bearer token is not a valid header value".into()))?;
            v.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, v);
        }
        for (name, value) in &config.headers {
            let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| Error::Config(format!("invalid header name '{name}'")))?;
            let value = reqwest::header::HeaderValue::from_str(value)
                .map_err(|_| Error::Config(format!("invalid value for header '{name}'")))?;
            headers.insert(name, value);
        }

        let clients = match &config.host_header {
            None => vec![build_client(&headers, None)?],
            Some(h) => {
                // curl --resolve semantics: the URL (and so the Host header and
                // the TLS SNI) names the tenant cluster, while the socket goes
                // to the configured address. One client per backend keeps
                // failover working with a single virtual Host.
                let mut out = Vec::with_capacity(lb.urls().len());
                for url in lb.urls() {
                    let addrs = resolve_backend(url)?;
                    out.push(build_client(&headers, Some((&h.hostname, addrs)))?);
                }
                out
            }
        };

        Ok(Self {
            clients,
            lb,
            config,
        })
    }

    /// The URL a request to `backend` should carry.
    ///
    /// With a Host override this is the *virtual* authority; the connection
    /// still reaches the real backend through that backend's pinned client.
    fn request_url(&self, backend: usize, path: &str) -> String {
        let base = &self.lb.urls()[backend];
        match &self.config.host_header {
            None => format!("{base}{path}"),
            Some(h) => {
                let scheme = if base.starts_with("https://") {
                    "https"
                } else {
                    "http"
                };
                match h.port {
                    Some(p) => format!("{scheme}://{}:{}{}", h.hostname, p, path),
                    None => format!("{scheme}://{}{}", h.hostname, path),
                }
            }
        }
    }

    fn client_for(&self, backend: usize) -> &reqwest::Client {
        self.clients.get(backend).unwrap_or(&self.clients[0])
    }

    /// Full send: failover across backends, retry within one, 429 in place.
    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Vec<u8>>,
        opts: &Opts,
    ) -> Result<Option<Vec<u8>>> {
        let n = self.lb.urls().len();
        let use_failover = self.config.failover && n > 1;
        let rounds = if use_failover { n } else { 1 };

        let mut attempted: Vec<usize> = Vec::with_capacity(rounds);
        let mut last: Option<Error> = None;

        for _ in 0..rounds {
            let backend = self.lb.pick(opts.affinity_key.as_deref());
            if use_failover && attempted.contains(&backend) {
                continue;
            }
            attempted.push(backend);

            let result = if use_failover {
                // Failover already provides the "try elsewhere" behaviour, so
                // one attempt per backend.
                self.attempt_with_429(method.clone(), backend, path, body.clone(), opts)
                    .await
            } else {
                self.attempt_with_retry(method.clone(), backend, path, body.clone(), opts)
                    .await
            };

            match result {
                Ok(v) => {
                    self.lb.mark_healthy(backend);
                    return Ok(v);
                }
                Err(e) => {
                    // 429 is not a health signal — the backend answered. Nor is
                    // any other 4xx, which is the caller's fault.
                    let status = e.status();
                    let is_client_error = matches!(status, Some(s) if (400..500).contains(&s));
                    if !is_client_error {
                        self.lb.mark_unhealthy(backend);
                    }
                    if is_client_error {
                        return Err(e);
                    }
                    last = Some(e);
                }
            }
        }

        Err(match last {
            Some(e) if attempted.len() > 1 => Error::AllBackendsFailed {
                attempted: attempted.len(),
                last: Box::new(e),
            },
            Some(e) => e,
            None => Error::Network("no backend available".into()),
        })
    }

    /// Single backend, `retry_attempts` tries with exponential backoff.
    async fn attempt_with_retry(
        &self,
        method: reqwest::Method,
        backend: usize,
        path: &str,
        body: Option<Vec<u8>>,
        opts: &Opts,
    ) -> Result<Option<Vec<u8>>> {
        let mut last: Option<Error> = None;
        for attempt in 0..self.config.retry_attempts {
            match self
                .attempt_with_429(method.clone(), backend, path, body.clone(), opts)
                .await
            {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if !e.is_retryable() {
                        return Err(e);
                    }
                    if attempt + 1 < self.config.retry_attempts {
                        let delay = self.config.retry_delay * 2u32.pow(attempt);
                        tracing::warn!(
                            path,
                            attempt = attempt + 1,
                            delay_ms = delay.as_millis() as u64,
                            error = %e,
                            "retrying request"
                        );
                        tokio::time::sleep(delay).await;
                    }
                    last = Some(e);
                }
            }
        }
        Err(last.unwrap_or_else(|| Error::Network("retry loop produced no error".into())))
    }

    /// One logical request, transparently absorbing 429s per the policy.
    async fn attempt_with_429(
        &self,
        method: reqwest::Method,
        backend: usize,
        path: &str,
        body: Option<Vec<u8>>,
        opts: &Opts,
    ) -> Result<Option<Vec<u8>>> {
        let policy = &self.config.retry_429;
        let max_attempts = match opts.retry_kind {
            // A long-poll pop keeps waiting through a throttle unless the
            // application explicitly bounded it.
            Some(RetryKind::Pop) => policy.max_attempts.filter(|_| policy_was_customized(policy)),
            _ => policy.max_attempts.or(Some(10)),
        };

        let mut tries: u32 = 0;
        loop {
            tries += 1;
            match self
                .attempt_once(method.clone(), backend, path, body.clone(), opts)
                .await
            {
                Ok(v) => return Ok(v),
                Err(e) if e.status() != Some(429) => return Err(e),
                Err(e) => {
                    if let Some(max) = max_attempts {
                        if tries >= max {
                            tracing::warn!(path, attempts = tries, "429 budget exhausted");
                            return Err(e);
                        }
                    }
                    let delay = self.backoff_429(tries - 1, e.retry_after_seconds());
                    tracing::warn!(
                        path,
                        attempt = tries,
                        delay_ms = delay.as_millis() as u64,
                        "rate limited, backing off"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// `Retry-After` when the server sent one, else exponential backoff — both
    /// jittered ±20% so a fleet of consumers does not come back in lockstep.
    fn backoff_429(&self, attempt: u32, retry_after_seconds: Option<f64>) -> Duration {
        let policy = &self.config.retry_429;
        let base = match retry_after_seconds {
            Some(s) if s.is_finite() && s >= 0.0 => Duration::from_secs_f64(s),
            _ => policy
                .base
                .saturating_mul(2u32.saturating_pow(attempt.min(16)))
                .min(policy.cap),
        };
        let jitter = rand::thread_rng().gen_range(0.8..1.2);
        Duration::from_secs_f64((base.as_secs_f64() * jitter).max(0.0))
    }

    async fn attempt_once(
        &self,
        method: reqwest::Method,
        backend: usize,
        path: &str,
        body: Option<Vec<u8>>,
        opts: &Opts,
    ) -> Result<Option<Vec<u8>>> {
        let url = self.request_url(backend, path);
        let timeout = opts.timeout.unwrap_or(self.config.timeout);

        let mut req = self.client_for(backend).request(method, &url).timeout(timeout);
        if let Some(b) = body {
            req = req.body(b);
        }

        let resp = req.send().await.map_err(|e| map_reqwest_error(e, timeout))?;
        let status = resp.status();

        if status == reqwest::StatusCode::NO_CONTENT {
            // 204 still carries a body on the pop-maintenance path
            // ({"messages":[],"paused":true}), so read it rather than assuming
            // empty — discarding it would turn "paused" into "no messages" and
            // hide maintenance mode from the caller.
            let bytes = resp.bytes().await.unwrap_or_default();
            return Ok(if bytes.is_empty() {
                None
            } else {
                Some(bytes.to_vec())
            });
        }

        if !status.is_success() {
            let retry_after = status
                .as_u16()
                .eq(&429)
                .then(|| {
                    resp.headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<f64>().ok())
                })
                .flatten();

            let bytes = resp.bytes().await.unwrap_or_default();
            let parsed: Option<ErrorBody> = serde_json::from_slice(&bytes).ok();
            let (message, code) = match parsed {
                Some(b) => (
                    b.error
                        .unwrap_or_else(|| status.canonical_reason().unwrap_or("error").to_string()),
                    b.code,
                ),
                None => (
                    status.canonical_reason().unwrap_or("error").to_string(),
                    None,
                ),
            };

            return Err(Error::Http {
                status: status.as_u16(),
                message,
                code,
                retry_after_seconds: retry_after,
            });
        }

        let bytes = resp.bytes().await.map_err(|e| Error::Network(e.to_string()))?;
        Ok(if bytes.is_empty() {
            None
        } else {
            Some(bytes.to_vec())
        })
    }

    // ---------------------------------------------------------- typed helpers

    pub async fn get_json<T: DeserializeOwned>(&self, path: &str, opts: &Opts) -> Result<Option<T>> {
        let body = self.send(reqwest::Method::GET, path, None, opts).await?;
        decode(body)
    }

    pub async fn post_json<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        opts: &Opts,
    ) -> Result<Option<T>> {
        let bytes = serde_json::to_vec(body)?;
        let out = self
            .send(reqwest::Method::POST, path, Some(bytes), opts)
            .await?;
        decode(out)
    }

    pub async fn post_empty<T: DeserializeOwned>(
        &self,
        path: &str,
        opts: &Opts,
    ) -> Result<Option<T>> {
        let out = self
            .send(reqwest::Method::POST, path, Some(b"{}".to_vec()), opts)
            .await?;
        decode(out)
    }

    pub async fn delete_json<T: DeserializeOwned>(
        &self,
        path: &str,
        opts: &Opts,
    ) -> Result<Option<T>> {
        let out = self.send(reqwest::Method::DELETE, path, None, opts).await?;
        decode(out)
    }

    /// For endpoints that answer with something other than JSON (Prometheus
    /// metrics, notably).
    pub async fn get_text(&self, path: &str, opts: &Opts) -> Result<String> {
        let out = self.send(reqwest::Method::GET, path, None, opts).await?;
        Ok(out
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default())
    }
}

fn decode<T: DeserializeOwned>(body: Option<Vec<u8>>) -> Result<Option<T>> {
    match body {
        None => Ok(None),
        Some(b) => serde_json::from_slice(&b).map(Some).map_err(|e| {
            Error::Decode(format!(
                "{e} (body: {})",
                truncate(&String::from_utf8_lossy(&b), 256)
            ))
        }),
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

/// Whether the application set its own 429 bounds. A long-poll pop is unbounded
/// *by default*, but an explicit `max_attempts` applies to it too.
fn policy_was_customized(policy: &crate::config::Retry429) -> bool {
    policy.max_attempts != crate::config::Retry429::default().max_attempts
}

fn build_client(
    headers: &reqwest::header::HeaderMap,
    pin: Option<(&str, Vec<std::net::SocketAddr>)>,
) -> Result<reqwest::Client> {
    let mut b = reqwest::Client::builder()
        .default_headers(headers.clone())
        .pool_idle_timeout(Duration::from_secs(30))
        .use_rustls_tls();
    if let Some((host, addrs)) = pin {
        b = b.resolve_to_addrs(host, &addrs);
    }
    b.build()
        .map_err(|e| Error::Config(format!("could not build HTTP client: {e}")))
}

/// Resolve a backend URL's authority to socket addresses, so a Host override
/// can dial it while advertising a different name.
fn resolve_backend(url: &str) -> Result<Vec<std::net::SocketAddr>> {
    use std::net::ToSocketAddrs;

    let rest = url
        .strip_prefix("https://")
        .map(|r| (r, 443u16))
        .or_else(|| url.strip_prefix("http://").map(|r| (r, 80u16)));
    let (rest, default_port) =
        rest.ok_or_else(|| Error::Config(format!("backend URL is not http(s): '{url}'")))?;
    let authority = rest.split('/').next().unwrap_or(rest);

    let (host, port) = if let Some(inner) = authority.strip_prefix('[') {
        let (h, tail) = inner
            .split_once(']')
            .ok_or_else(|| Error::Config(format!("unclosed IPv6 bracket in '{url}'")))?;
        let port = tail
            .strip_prefix(':')
            .map(|p| p.parse::<u16>())
            .transpose()
            .map_err(|_| Error::Config(format!("invalid port in '{url}'")))?
            .unwrap_or(default_port);
        (h.to_string(), port)
    } else {
        match authority.rsplit_once(':') {
            Some((h, p)) => (
                h.to_string(),
                p.parse::<u16>()
                    .map_err(|_| Error::Config(format!("invalid port in '{url}'")))?,
            ),
            None => (authority.to_string(), default_port),
        }
    };

    let addrs: Vec<std::net::SocketAddr> = (host.as_str(), port)
        .to_socket_addrs()
        .map_err(|e| {
            Error::Config(format!(
                "host_header is set, so '{url}' must resolve at construction: {e}"
            ))
        })?
        .collect();
    if addrs.is_empty() {
        return Err(Error::Config(format!("'{url}' resolved to no addresses")));
    }
    Ok(addrs)
}

fn map_reqwest_error(e: reqwest::Error, timeout: Duration) -> Error {
    if e.is_timeout() {
        return Error::Timeout {
            millis: timeout.as_millis() as u64,
        };
    }
    // reqwest nests the real cause (ECONNREFUSED, socket hang up, …) — surface
    // it, otherwise every transport failure reads as a bare "error sending
    // request" and is undiagnosable.
    let mut msg = e.to_string();
    let mut src: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(&e);
    while let Some(s) = src {
        msg = format!("{msg}: {s}");
        src = s.source();
    }
    Error::Network(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Retry429;

    fn client(cfg: Config) -> HttpClient {
        HttpClient::new(cfg).unwrap()
    }

    #[test]
    fn plain_urls_pass_through_unchanged() {
        let c = client(Config::new("http://localhost:6789"));
        assert_eq!(
            c.request_url(0, "/api/v1/push"),
            "http://localhost:6789/api/v1/push"
        );
    }

    #[test]
    fn host_override_rewrites_the_authority_only() {
        let cfg = Config::new("http://127.0.0.1:6789")
            .host_header("acme.eu1.queenmq.cloud")
            .unwrap();
        let c = client(cfg);
        // No port on the override: the virtual URL drops to the scheme default
        // while the socket still goes to 6789.
        assert_eq!(
            c.request_url(0, "/api/v1/push"),
            "http://acme.eu1.queenmq.cloud/api/v1/push"
        );
    }

    #[test]
    fn host_override_keeps_an_explicit_port() {
        let cfg = Config::new("http://127.0.0.1:6789")
            .host_header("acme.local:6711")
            .unwrap();
        let c = client(cfg);
        assert_eq!(
            c.request_url(0, "/health"),
            "http://acme.local:6711/health"
        );
    }

    #[test]
    fn host_override_preserves_the_scheme() {
        let cfg = Config::new("https://127.0.0.1:6789")
            .host_header("acme.eu1.queenmq.cloud")
            .unwrap();
        let c = client(cfg);
        assert!(c.request_url(0, "/x").starts_with("https://acme."));
    }

    #[test]
    fn host_override_builds_one_pinned_client_per_backend() {
        let cfg = Config::urls(["http://127.0.0.1:6789", "http://127.0.0.1:6790"])
            .host_header("acme.eu1.queenmq.cloud")
            .unwrap();
        let c = client(cfg);
        assert_eq!(c.clients.len(), 2, "failover needs one pinned client each");
    }

    #[test]
    fn without_an_override_one_client_is_shared() {
        let c = client(Config::urls([
            "http://127.0.0.1:6789",
            "http://127.0.0.1:6790",
        ]));
        assert_eq!(c.clients.len(), 1);
    }

    #[test]
    fn backoff_honours_retry_after_within_jitter() {
        let c = client(Config::new("http://localhost:6789"));
        for _ in 0..50 {
            let d = c.backoff_429(0, Some(2.0)).as_secs_f64();
            assert!((1.6..=2.4).contains(&d), "{d} outside ±20% of 2s");
        }
    }

    #[test]
    fn backoff_grows_exponentially_and_is_capped() {
        let cfg = Config::new("http://localhost:6789").retry_429(Retry429 {
            max_attempts: Some(10),
            base: Duration::from_millis(500),
            cap: Duration::from_secs(4),
        });
        let c = client(cfg);
        // attempt 0 → ~500ms, attempt 1 → ~1s, attempt 2 → ~2s
        assert!((0.4..=0.6).contains(&c.backoff_429(0, None).as_secs_f64()));
        assert!((0.8..=1.2).contains(&c.backoff_429(1, None).as_secs_f64()));
        // and never past the cap (plus jitter)
        for attempt in 3..20 {
            let d = c.backoff_429(attempt, None).as_secs_f64();
            assert!(d <= 4.8, "attempt {attempt} produced {d}s, past the cap");
        }
    }

    #[test]
    fn pop_is_unbounded_by_default_but_respects_an_explicit_bound() {
        let default = Retry429::default();
        assert!(!policy_was_customized(&default));

        let custom = Retry429 {
            max_attempts: Some(3),
            ..Default::default()
        };
        assert!(policy_was_customized(&custom));

        let unbounded = Retry429 {
            max_attempts: None,
            ..Default::default()
        };
        assert!(policy_was_customized(&unbounded));
    }

    #[test]
    fn decode_maps_an_empty_body_to_none() {
        let got: Option<serde_json::Value> = decode(None).unwrap();
        assert!(got.is_none());
    }

    #[test]
    fn decode_reports_the_offending_body() {
        let err = decode::<serde_json::Value>(Some(b"not json".to_vec())).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not json"), "{msg}");
    }

    #[test]
    fn truncate_respects_char_boundaries() {
        let s = "à".repeat(200);
        let t = truncate(&s, 11);
        assert!(t.ends_with('…'));
        assert!(t.len() <= 14);
    }

    #[test]
    fn resolve_backend_handles_the_authority_forms() {
        assert!(resolve_backend("http://127.0.0.1:6789").is_ok());
        assert!(resolve_backend("http://127.0.0.1").is_ok());
        assert!(resolve_backend("http://[::1]:6789").is_ok());
        assert!(resolve_backend("http://127.0.0.1:6789/some/path").is_ok());
        assert!(resolve_backend("ftp://127.0.0.1").is_err());
        assert!(resolve_backend("http://127.0.0.1:notaport").is_err());
    }
}
