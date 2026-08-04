//! Client configuration.

use std::time::Duration;

use crate::error::{Error, Result};
use crate::lb::Strategy;

/// Backoff policy for HTTP 429, separate from the 5xx/network retry budget.
///
/// A 429 means the backend is alive and asking for less traffic, so it is
/// retried *in place* rather than failed over to another replica — moving the
/// request elsewhere would just spread the load the proxy is trying to shed.
#[derive(Debug, Clone, PartialEq)]
pub struct Retry429 {
    /// `None` means unbounded, which is the default for long-poll pops: a
    /// consumer that is meant to wait should keep waiting through a throttle,
    /// not give up and exit its loop.
    pub max_attempts: Option<u32>,
    pub base: Duration,
    pub cap: Duration,
}

impl Default for Retry429 {
    fn default() -> Self {
        Self {
            max_attempts: Some(10),
            base: Duration::from_millis(500),
            cap: Duration::from_secs(30),
        }
    }
}

/// Which 429 budget a request gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryKind {
    /// Bounded. Push, ack, admin — anything the caller is waiting on.
    Default,
    /// Unbounded by default. A `wait=true` pop is supposed to block, so a
    /// throttle should slow it down, not end it.
    Pop,
}

/// Host to advertise on every request, independent of the address dialled.
///
/// `queen_proxy` picks the tenant cluster from the Host header's first DNS
/// label, so this is what selects a cluster when the base URL points at a
/// shared address — an IP, a cell endpoint, a local rig. The socket still goes
/// to the configured address; only the request authority and the TLS SNI are
/// rewritten, exactly like `curl --resolve`.
///
/// In production each cluster has its own hostname and the base URL already
/// carries the right Host, so this normally stays unset.
#[derive(Debug, Clone, PartialEq)]
pub struct HostHeader {
    pub(crate) authority: String,
    pub(crate) hostname: String,
    pub(crate) port: Option<u16>,
}

impl HostHeader {
    /// Parse a bare authority: `acme`, `acme.eu1.queenmq.cloud`,
    /// `acme.local:6711`, `[::1]:6711`.
    ///
    /// Anything URL-shaped is rejected here rather than at request time,
    /// because a wrong Host at a proxy does not fail loudly — on a cell with a
    /// default cluster it lands the traffic in somebody else's data.
    pub fn parse(value: &str) -> Result<Self> {
        let authority = value.trim();
        if authority.is_empty() {
            return Err(Error::Config(
                "hostHeader must be a non-empty authority, e.g. 'acme.eu1.queenmq.cloud'".into(),
            ));
        }
        if authority.contains("://")
            || authority
                .contains(|c: char| c.is_whitespace() || matches!(c, '/' | '\\' | '?' | '#' | '@'))
        {
            return Err(Error::Config(format!(
                "hostHeader must be a bare authority ('acme.eu1.queenmq.cloud' or \
                 'acme.local:6711'), not a URL — got '{authority}'"
            )));
        }

        // Split host[:port], keeping bracketed IPv6 literals intact.
        let (hostname, port) = if let Some(rest) = authority.strip_prefix('[') {
            let (h, tail) = rest.split_once(']').ok_or_else(|| {
                Error::Config(format!("hostHeader has an unclosed IPv6 bracket — got '{authority}'"))
            })?;
            let port = match tail.strip_prefix(':') {
                Some(p) => Some(parse_port(p, authority)?),
                None if tail.is_empty() => None,
                None => {
                    return Err(Error::Config(format!(
                        "hostHeader is not a valid host[:port] — got '{authority}'"
                    )))
                }
            };
            (h.to_string(), port)
        } else {
            match authority.rsplit_once(':') {
                Some((h, p)) => (h.to_string(), Some(parse_port(p, authority)?)),
                None => (authority.to_string(), None),
            }
        };

        if hostname.is_empty() {
            return Err(Error::Config(format!(
                "hostHeader is not a valid host[:port] — got '{authority}'"
            )));
        }

        Ok(Self {
            authority: authority.to_string(),
            hostname,
            port,
        })
    }
}

fn parse_port(s: &str, authority: &str) -> Result<u16> {
    s.parse::<u16>().map_err(|_| {
        Error::Config(format!(
            "hostHeader has an invalid port — got '{authority}'"
        ))
    })
}

/// How to reach a Queen deployment and how hard to try.
#[derive(Debug, Clone)]
pub struct Config {
    /// One or more broker URLs. More than one turns on load balancing and
    /// failover.
    pub urls: Vec<String>,

    pub timeout: Duration,

    /// Attempts per request against 5xx and transport faults. 1 disables retry.
    pub retry_attempts: u32,

    /// First retry delay; doubles each attempt.
    pub retry_delay: Duration,

    pub strategy: Strategy,

    /// Virtual nodes per backend on the affinity ring.
    pub affinity_hash_ring: usize,

    /// Try other backends when one fails.
    pub failover: bool,

    /// How long a backend stays out of the pool after a failure.
    pub health_retry_after: Duration,

    pub bearer_token: Option<String>,

    /// Sent on every request. A `Host` entry here is rejected — use
    /// [`Config::host_header`], which actually works.
    pub headers: Vec<(String, String)>,

    pub host_header: Option<HostHeader>,

    pub retry_429: Retry429,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            urls: Vec::new(),
            timeout: Duration::from_secs(30),
            retry_attempts: 3,
            retry_delay: Duration::from_secs(1),
            strategy: Strategy::Affinity,
            // The JS LoadBalancer's own fallback is 150, but its client always
            // passes 128 from CLIENT_DEFAULTS, so 128 is the effective default
            // everywhere and the one to match.
            affinity_hash_ring: 128,
            failover: true,
            health_retry_after: Duration::from_secs(5),
            bearer_token: None,
            headers: Vec::new(),
            host_header: None,
            retry_429: Retry429::default(),
        }
    }
}

impl Config {
    /// Point at a single broker.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            urls: vec![url.into()],
            ..Default::default()
        }
    }

    /// Point at several brokers, enabling load balancing and failover.
    pub fn urls<I, S>(urls: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            urls: urls.into_iter().map(Into::into).collect(),
            ..Default::default()
        }
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }

    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn strategy(mut self, strategy: Strategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn retry_attempts(mut self, attempts: u32) -> Self {
        self.retry_attempts = attempts;
        self
    }

    pub fn failover(mut self, enabled: bool) -> Self {
        self.failover = enabled;
        self
    }

    pub fn retry_429(mut self, policy: Retry429) -> Self {
        self.retry_429 = policy;
        self
    }

    /// Set the Host to advertise. See [`HostHeader`].
    pub fn host_header(mut self, authority: impl AsRef<str>) -> Result<Self> {
        self.host_header = Some(HostHeader::parse(authority.as_ref())?);
        Ok(self)
    }

    /// Reject configurations that cannot work, at construction rather than on
    /// the first request.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.urls.is_empty() {
            return Err(Error::Config("at least one broker URL is required".into()));
        }
        for u in &self.urls {
            if !(u.starts_with("http://") || u.starts_with("https://")) {
                return Err(Error::Config(format!(
                    "broker URL must start with http:// or https:// — got '{u}'"
                )));
            }
        }
        // A Host in `headers` would be silently overridden by the request
        // authority; say so rather than let it look configured.
        if let Some((name, value)) = self
            .headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case("host"))
        {
            return Err(Error::Config(format!(
                "a '{name}' header cannot be set through `headers` — it is decided by the request \
                 authority. Use Config::host_header(\"{value}\") instead, which rewrites the \
                 authority and pins the connection to the configured address."
            )));
        }
        if self.retry_attempts == 0 {
            return Err(Error::Config("retry_attempts must be at least 1".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_other_sdks() {
        let c = Config::new("http://localhost:6789");
        assert_eq!(c.timeout, Duration::from_secs(30));
        assert_eq!(c.retry_attempts, 3);
        assert_eq!(c.retry_delay, Duration::from_secs(1));
        assert_eq!(c.strategy, Strategy::Affinity);
        assert_eq!(c.affinity_hash_ring, 128);
        assert!(c.failover);
        assert_eq!(c.health_retry_after, Duration::from_secs(5));
        assert_eq!(c.retry_429.max_attempts, Some(10));
        assert_eq!(c.retry_429.base, Duration::from_millis(500));
        assert_eq!(c.retry_429.cap, Duration::from_secs(30));
    }

    #[test]
    fn rejects_empty_and_non_http_urls() {
        assert!(Config::urls(Vec::<String>::new()).validate().is_err());
        assert!(Config::new("localhost:6789").validate().is_err());
        assert!(Config::new("ftp://localhost").validate().is_err());
        assert!(Config::new("http://localhost:6789").validate().is_ok());
        assert!(Config::new("https://a.queenmq.cloud").validate().is_ok());
    }

    #[test]
    fn rejects_a_host_header_smuggled_through_headers() {
        let c = Config::new("http://10.0.0.1:6789").header("Host", "acme.queenmq.cloud");
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("host_header"), "{err}");

        // case-insensitively
        let c = Config::new("http://10.0.0.1:6789").header("host", "acme.queenmq.cloud");
        assert!(c.validate().is_err());
    }

    #[test]
    fn host_header_accepts_bare_authorities() {
        let h = HostHeader::parse("acme.eu1.queenmq.cloud").unwrap();
        assert_eq!(h.hostname, "acme.eu1.queenmq.cloud");
        assert_eq!(h.port, None);

        let h = HostHeader::parse("acme.local:6711").unwrap();
        assert_eq!(h.hostname, "acme.local");
        assert_eq!(h.port, Some(6711));

        let h = HostHeader::parse("[::1]:6711").unwrap();
        assert_eq!(h.hostname, "::1");
        assert_eq!(h.port, Some(6711));

        let h = HostHeader::parse("acme").unwrap();
        assert_eq!(h.hostname, "acme");
    }

    #[test]
    fn host_header_rejects_anything_url_shaped() {
        // Failing here is the whole point: a wrong Host at a proxy silently
        // reaches another tenant instead of erroring.
        for bad in [
            "http://acme.queenmq.cloud",
            "acme.queenmq.cloud/path",
            "acme.queenmq.cloud?x=1",
            "acme queenmq",
            "user@acme",
            "",
            "   ",
            "acme:notaport",
            "acme:99999",
        ] {
            assert!(HostHeader::parse(bad).is_err(), "should reject {bad:?}");
        }
    }
}
