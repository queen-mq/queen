//! Error bodies.
//!
//! The broker itself answers failures with a bare `{"error": "..."}`. A
//! deployment behind `queen_proxy` adds a machine-readable `code`, which is the
//! only reliable way to tell a retryable throttle from a terminal refusal —
//! string-matching the message is not.

use serde::{Deserialize, Serialize};

/// The `code` a proxy attaches to a 429 or 403.
///
/// Unknown codes parse into [`ErrorCode::Other`] rather than failing, so a
/// client keeps working against a newer proxy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    // --- 429, retryable ---
    /// Over the request-rate limit. Back off and retry; `Retry-After` says how
    /// long.
    RateLimited,
    /// Over a usage quota for the current period. Retryable in principle, but
    /// backing off will not help until the period rolls over.
    QuotaExceeded,

    // --- 403, terminal ---
    /// The cluster is suspended. This never resolves on its own — retrying is
    /// pointless.
    ClusterSuspended,
    /// The tenant is out of storage. Pushes will keep failing until data is
    /// removed or the plan changes.
    StorageQuotaExceeded,
    /// The endpoint exists but the tenant's plan does not include it.
    FeatureGated,
    /// Generic refusal.
    Forbidden,

    #[serde(untagged)]
    Other(String),
}

impl ErrorCode {
    /// Whether backing off and retrying the same request can plausibly succeed.
    ///
    /// [`ErrorCode::QuotaExceeded`] counts as retryable — the quota does roll
    /// over — but a client should pace itself far more slowly than for a plain
    /// rate limit. An unknown code is treated as *not* retryable, which is the
    /// safe direction: it fails loudly instead of hot-looping.
    pub fn retryable(&self) -> bool {
        matches!(self, Self::RateLimited | Self::QuotaExceeded)
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::RateLimited => "rate_limited",
            Self::QuotaExceeded => "quota_exceeded",
            Self::ClusterSuspended => "cluster_suspended",
            Self::StorageQuotaExceeded => "storage_quota_exceeded",
            Self::FeatureGated => "feature_gated",
            Self::Forbidden => "forbidden",
            Self::Other(s) => s,
        }
    }
}

/// A JSON error body. Every field is optional because the broker, the proxy and
/// the stored procedures each populate a different subset.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ErrorBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<ErrorCode>,
}

impl ErrorBody {
    pub fn message(&self) -> &str {
        self.error.as_deref().unwrap_or("unknown error")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_proxy_429_contract() {
        let got: ErrorBody =
            serde_json::from_str(r#"{"error":"too many requests","code":"rate_limited"}"#).unwrap();
        assert_eq!(got.code, Some(ErrorCode::RateLimited));
        assert!(got.code.unwrap().retryable());
    }

    #[test]
    fn parses_the_proxy_403_contract() {
        let got: ErrorBody =
            serde_json::from_str(r#"{"error":"suspended","code":"cluster_suspended"}"#).unwrap();
        let code = got.code.unwrap();
        assert_eq!(code, ErrorCode::ClusterSuspended);
        assert!(!code.retryable(), "a suspended cluster must not be retried");
    }

    #[test]
    fn parses_a_bare_broker_error() {
        let got: ErrorBody = serde_json::from_str(r#"{"error":"queue is required"}"#).unwrap();
        assert_eq!(got.message(), "queue is required");
        assert!(got.code.is_none());
    }

    #[test]
    fn unknown_codes_survive_and_are_not_retried() {
        let got: ErrorBody =
            serde_json::from_str(r#"{"error":"nope","code":"some_future_code"}"#).unwrap();
        let code = got.code.unwrap();
        assert_eq!(code.as_str(), "some_future_code");
        assert!(!code.retryable());
    }

    #[test]
    fn code_round_trips() {
        for c in [
            ErrorCode::RateLimited,
            ErrorCode::QuotaExceeded,
            ErrorCode::ClusterSuspended,
            ErrorCode::StorageQuotaExceeded,
            ErrorCode::FeatureGated,
            ErrorCode::Forbidden,
        ] {
            let s = serde_json::to_string(&c).unwrap();
            let back: ErrorCode = serde_json::from_str(&s).unwrap();
            assert_eq!(back, c, "{s}");
        }
    }
}
