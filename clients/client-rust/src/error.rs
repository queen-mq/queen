//! Client errors.

use std::fmt;

use queen_protocol::ErrorCode;

/// Everything that can go wrong in a client call.
#[derive(Debug)]
pub enum Error {
    /// Bad client configuration, caught at construction.
    Config(String),

    /// The broker (or a proxy in front of it) answered with a non-2xx status.
    ///
    /// `code` is the proxy's machine-readable reason when there was one;
    /// branch on it rather than on `message`, which is prose and changes.
    Http {
        status: u16,
        message: String,
        code: Option<ErrorCode>,
        /// Seconds from a `Retry-After` header, on a 429.
        retry_after_seconds: Option<f64>,
    },

    /// The request did not complete within the configured timeout.
    Timeout { millis: u64 },

    /// Connection refused, reset, DNS failure — anything below HTTP.
    Network(String),

    /// A response did not match the shape this client expects. Usually a
    /// version skew between client and broker.
    Decode(String),

    /// Every backend in the load balancer was tried and none answered.
    AllBackendsFailed { attempted: usize, last: Box<Error> },

    /// The caller passed something the broker would reject, caught client-side
    /// so the mistake surfaces at the call rather than as a rejected ack.
    Invalid(String),
}

impl Error {
    /// The HTTP status, when this was an HTTP error.
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Http { status, .. } => Some(*status),
            Self::AllBackendsFailed { last, .. } => last.status(),
            _ => None,
        }
    }

    pub fn code(&self) -> Option<&ErrorCode> {
        match self {
            Self::Http { code, .. } => code.as_ref(),
            Self::AllBackendsFailed { last, .. } => last.code(),
            _ => None,
        }
    }

    /// Rate limited. The retry policy handles these internally; seeing one
    /// escape means the policy's attempt budget ran out.
    pub fn is_rate_limited(&self) -> bool {
        self.status() == Some(429)
    }

    /// A refusal that will not resolve by retrying — a suspended cluster, an
    /// exhausted storage quota, a gated feature.
    pub fn is_terminal_refusal(&self) -> bool {
        self.status() == Some(403)
    }

    /// Whether retrying the identical request could plausibly succeed.
    ///
    /// 4xx other than 429 is a client mistake and stays failed. 5xx, timeouts
    /// and network faults are worth another backend or another attempt.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Http { status, .. } => *status == 429 || *status >= 500,
            Self::Timeout { .. } | Self::Network(_) => true,
            Self::AllBackendsFailed { last, .. } => last.is_retryable(),
            _ => false,
        }
    }

    pub(crate) fn retry_after_seconds(&self) -> Option<f64> {
        match self {
            Self::Http {
                retry_after_seconds,
                ..
            } => *retry_after_seconds,
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(m) => write!(f, "invalid configuration: {m}"),
            Self::Http {
                status,
                message,
                code,
                ..
            } => match code {
                Some(c) => write!(f, "HTTP {status} ({}): {message}", c.as_str()),
                None => write!(f, "HTTP {status}: {message}"),
            },
            Self::Timeout { millis } => write!(f, "request timed out after {millis}ms"),
            Self::Network(m) => write!(f, "network error: {m}"),
            Self::Decode(m) => write!(f, "unexpected response shape: {m}"),
            Self::AllBackendsFailed { attempted, last } => {
                write!(f, "all {attempted} backends failed; last error: {last}")
            }
            Self::Invalid(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::AllBackendsFailed { last, .. } => Some(last.as_ref()),
            _ => None,
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Decode(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    fn http(status: u16) -> Error {
        Error::Http {
            status,
            message: "x".into(),
            code: None,
            retry_after_seconds: None,
        }
    }

    #[test]
    fn client_errors_are_not_retryable_except_429() {
        assert!(!http(400).is_retryable());
        assert!(!http(404).is_retryable());
        assert!(!http(403).is_retryable());
        assert!(http(429).is_retryable());
    }

    #[test]
    fn server_errors_and_transport_faults_are_retryable() {
        assert!(http(500).is_retryable());
        assert!(http(503).is_retryable());
        assert!(Error::Timeout { millis: 10 }.is_retryable());
        assert!(Error::Network("ECONNREFUSED".into()).is_retryable());
    }

    #[test]
    fn terminal_refusal_and_rate_limit_are_distinguishable() {
        assert!(http(403).is_terminal_refusal());
        assert!(!http(403).is_rate_limited());
        assert!(http(429).is_rate_limited());
        assert!(!http(429).is_terminal_refusal());
    }

    #[test]
    fn all_backends_failed_delegates_to_the_last_error() {
        let e = Error::AllBackendsFailed {
            attempted: 3,
            last: Box::new(http(503)),
        };
        assert_eq!(e.status(), Some(503));
        assert!(e.is_retryable());
        assert!(e.to_string().contains("all 3 backends failed"));
    }
}
