//! Error bodies.
//!
//! The broker itself answers failures with a bare `{"error": "..."}`. A
//! deployment behind `queen_proxy` adds a machine-readable `code`, which is the
//! only reliable way to tell a retryable throttle from a terminal refusal —
//! string-matching the message is not.

use serde::{Deserialize, Serialize};

/// The `code` a proxy attaches to a refusal.
///
/// Six of `queen_proxy`'s fourteen codes are named here (`proxy/src/errors.rs`
/// declares the full list at :9-22); the rest parse into [`ErrorCode::Other`]
/// rather than failing, so a client keeps working against a newer proxy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    // --- the only code that ever rides a 429 ---
    /// Over the request-rate limit. Back off and retry; `Retry-After` says how
    /// long.
    ///
    /// Every 429 the proxy emits carries this code and nothing else: the three
    /// gateway call sites (`proxy/src/gateway.rs:188`, `:246`, `:565`) all take
    /// it from `Decision::Deny`, which only ever sets `CODE_RATE_LIMITED`
    /// (`proxy/src/limits.rs:295` and `:359`), and the login limiter hard-codes
    /// it (`proxy/src/oauth.rs:131`).
    RateLimited,

    // --- 403, terminal on the wire ---
    /// Over a usage quota. **403, not 429** — despite the name this never
    /// arrives as a throttle.
    ///
    /// It covers three unrelated causes: the exhausted monthly message quota
    /// (`proxy/src/gateway.rs:132`), which does roll over at the next calendar
    /// month; the plan's queue/partition ceiling (`:536`, `:548`, `:617`,
    /// `:642`), which never clears on its own; and a `retentionSeconds` above
    /// the plan's maximum (`:651`), which is a rejected request, not a busy
    /// one. [`ErrorCode::retryable`] still reports `true` for it — see the note
    /// there before relying on that.
    QuotaExceeded,
    /// The cluster is suspended (`proxy/src/gateway.rs:81`). This never
    /// resolves on its own — retrying is pointless.
    ClusterSuspended,
    /// The tenant is out of storage (`proxy/src/gateway.rs:126`). Pushes will
    /// keep failing until data is removed or the plan changes.
    StorageQuotaExceeded,
    /// The endpoint exists but the tenant's plan does not include it
    /// (`proxy/src/gateway.rs:104`).
    FeatureGated,
    /// Generic refusal — the credential is not permitted here.
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
    ///
    /// This is advice about the *cause*, not about the transport, and the two
    /// disagree for `quota_exceeded`: it arrives as a 403, which the client's
    /// own status-based retry (`Error::is_retryable`) treats as terminal, and
    /// two of its three causes never clear no matter how long you wait. Retry
    /// on the status; use this to decide how loudly to complain.
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

    /// Every code `queen_proxy` can attach, with the HTTP status it actually
    /// rides and whether this crate models it.
    ///
    /// The names are the constants at `proxy/src/errors.rs:9-22`; the statuses
    /// come from following each constant to its call sites, not from the
    /// constant's name — which is the point, since `quota_exceeded` reads like
    /// a throttle and is emitted only as a 403.
    const PROXY_CODE_TABLE: &[(&str, u16, bool)] = &[
        // (code, status, modelled as a named variant)
        ("unauthorized", 401, false),          // errors.rs:42-44
        ("forbidden", 403, true),              // acting.rs:92/116/160, auth.rs:235/260/276/337
        ("rate_limited", 429, true),           // limits.rs:295/359 -> gateway.rs:188/246/565
        ("quota_exceeded", 403, true),         // gateway.rs:132/536/548/617/642/651
        ("storage_quota_exceeded", 403, true), // gateway.rs:126
        ("cluster_suspended", 403, true),      // gateway.rs:81
        ("push_blocked", 403, false),          // gateway.rs:139
        ("feature_gated", 403, true),          // gateway.rs:104
        ("route_blocked", 404, false),         // gateway.rs:89/96/152
        ("cluster_unknown", 421, false),       // errors.rs:54-56
        ("scope_unknown", 0, false),           // declared at errors.rs:19, never emitted
        ("payload_too_large", 413, false),     // errors.rs:58-60
        ("bad_gateway", 502, false),           // errors.rs:62-64
        ("upstream_timeout", 504, false),      // errors.rs:66-68
    ];

    #[test]
    fn every_code_the_proxy_can_send_survives_the_decode() {
        // The eight unmodelled codes are the ones that matter here: a client
        // that fails to parse an error body loses the reason for the failure
        // and reports a decode fault instead, which is strictly worse than
        // reporting a code it does not recognize.
        for (code, _status, modelled) in PROXY_CODE_TABLE {
            let body = format!(r#"{{"error":"nope","code":"{code}"}}"#);
            let got: ErrorBody = serde_json::from_str(&body)
                .unwrap_or_else(|e| panic!("`{code}` failed to decode: {e}"));
            let parsed = got.code.expect("the code key was dropped");
            assert_eq!(
                parsed.as_str(),
                *code,
                "`{code}` did not survive the round trip intact"
            );
            assert_eq!(
                matches!(parsed, ErrorCode::Other(_)),
                !*modelled,
                "`{code}` is on the wrong side of the modelled/degraded split"
            );
        }
    }

    #[test]
    fn rate_limited_is_the_only_code_that_ever_arrives_as_a_throttle() {
        // `retryable()` says "back off and try again", and the only refusal the
        // proxy actually asks a client to wait on is the 429. Everything else
        // in the table is a 4xx that will answer the same way forever, or a
        // 5xx the client already retries on status. If a second code ever
        // starts riding a 429, this table has to grow with it — otherwise the
        // new throttle degrades to `Other` and reads as terminal.
        let throttles: Vec<&str> = PROXY_CODE_TABLE
            .iter()
            .filter(|(_, status, _)| *status == 429)
            .map(|(code, _, _)| *code)
            .collect();
        assert_eq!(throttles, vec!["rate_limited"]);
    }

    #[test]
    fn the_terminal_403_codes_are_never_retried() {
        // These four are the refusals a consumer will actually meet in a cloud
        // deployment, and retrying any of them is a busy-loop against a wall.
        for code in [
            ErrorCode::ClusterSuspended,
            ErrorCode::StorageQuotaExceeded,
            ErrorCode::FeatureGated,
            ErrorCode::Forbidden,
        ] {
            assert!(
                !code.retryable(),
                "{} is a terminal 403 and must not be retried",
                code.as_str()
            );
        }
    }

    #[test]
    fn a_transient_upstream_fault_degrades_to_a_terminal_code() {
        // `bad_gateway` (502) and `upstream_timeout` (504) genuinely are worth
        // retrying, but they are not modelled, so `Other` reports them as
        // terminal. That costs nothing today because the client retries on the
        // STATUS (`Error::is_retryable` covers all 5xx) and never consults the
        // code — but anyone who starts branching on `retryable()` alone would
        // stop retrying a transient upstream blip.
        for code in ["bad_gateway", "upstream_timeout"] {
            let got: ErrorBody =
                serde_json::from_str(&format!(r#"{{"error":"upstream","code":"{code}"}}"#))
                    .unwrap();
            let parsed = got.code.unwrap();
            assert_eq!(parsed, ErrorCode::Other(code.to_string()));
            assert!(!parsed.retryable());
        }
    }

    #[test]
    fn an_error_body_survives_a_missing_or_null_code() {
        // The broker itself never sends `code` (only a proxy does), and a
        // failure that reaches the client from a direct broker deployment must
        // still carry its message.
        let absent: ErrorBody = serde_json::from_str(r#"{"error":"queue is required"}"#).unwrap();
        let null: ErrorBody =
            serde_json::from_str(r#"{"error":"queue is required","code":null}"#).unwrap();
        assert_eq!(absent, null, "an absent and a null code must read the same");
        assert_eq!(absent.message(), "queue is required");

        // And an entirely empty body still has something to say rather than
        // panicking on an unwrap.
        let empty: ErrorBody = serde_json::from_str("{}").unwrap();
        assert_eq!(empty.message(), "unknown error");
    }

    #[test]
    fn an_error_body_from_a_newer_proxy_still_parses() {
        let got: ErrorBody = serde_json::from_str(
            r#"{"error":"too many requests","code":"rate_limited","requestId":"abc","retryAfter":3}"#,
        )
        .expect("an unmodelled key must not fail the decode");
        assert_eq!(got.code, Some(ErrorCode::RateLimited));
    }

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
