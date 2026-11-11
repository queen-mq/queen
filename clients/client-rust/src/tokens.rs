//! Client tokens: a credential for ONE entity — a device, a browser user —
//! minted from a scoped key (`POST /api/v1/tokens`), and the
//! [`TokenSource`] a client uses to fetch one, keep it, and fetch another.
//!
//! The key is the role: its grants say which queues, partitions, KV keys and
//! timers it reaches, and may name the entity as `{sub}` (`telemetry` /
//! `{sub}`: each device its own partition). The token adds the entity and
//! never reaches more than its key. The application's backend holds the key
//! and mints; the device holds only its token:
//!
//! ```no_run
//! # async fn demo() -> queen_mq::Result<()> {
//! use queen_mq::{Config, MintTokenRequest, Queen, TokenSource};
//!
//! // backend, with the scoped key `devices`
//! let backend = Queen::connect(Config::new("https://acme.queen.example").bearer_token("qk_live_..."))?;
//! let minted = backend
//!     .tokens()
//!     .mint(&MintTokenRequest::new("dev-8f3a").ttl_seconds(3600))
//!     .await?;
//!
//! // device: a source, so the client fetches a new token before this one
//! // expires and after a 401
//! let device = Queen::connect(Config::new("https://acme.queen.example").token_source(
//!     TokenSource::new(|| async { Ok(fetch_token_from_my_backend().await) }),
//! ))?;
//! # let _ = (minted, device);
//! # Ok(())
//! # }
//! # async fn fetch_token_from_my_backend() -> String { String::new() }
//! ```
//!
//! Revoking or narrowing the key reaches every token it minted within about
//! a second; blocking one `sub` refuses that entity's tokens on every node.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use queen_protocol::scope::{Grant, GrantOp, MintTokenRequest, MintTokenResponse};

use crate::error::{Error, Result};
use crate::http::Opts;
use crate::inner::Inner;

/// How long before a client token's `exp` it is fetched again.
pub const REFRESH_AHEAD: Duration = Duration::from_secs(30);

/// What every client token starts with.
pub const CLIENT_TOKEN_PREFIX: &str = "qt_";

/// What a token function's error may be.
pub type TokenError = Box<dyn std::error::Error + Send + Sync>;

type Fetch = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = std::result::Result<String, TokenError>> + Send>>
        + Send
        + Sync,
>;

/// Where a client's bearer token comes from when it is not a fixed string: a
/// function that fetches one, typically from the application's own backend.
///
/// The client calls it before its first request and keeps the token. It calls
/// it again shortly before a client token expires ([`REFRESH_AHEAD`], from the
/// `exp` the token carries), and after an answer of 401, when it then sends
/// that request once more. One call at a time: requests that need a token
/// while one is fetched wait for that call. Cheap to clone; clones share the
/// token.
#[derive(Clone)]
pub struct TokenSource {
    fetch: Fetch,
    cached: Arc<tokio::sync::Mutex<Option<Cached>>>,
}

struct Cached {
    token: String,
    expires_at: Option<SystemTime>,
}

impl std::fmt::Debug for TokenSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TokenSource { .. }")
    }
}

impl TokenSource {
    pub fn new<F, Fut>(fetch: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<String, TokenError>> + Send + 'static,
    {
        TokenSource {
            fetch: Arc::new(move || Box::pin(fetch())),
            cached: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// The token to send. `failed`: the token a request was just refused with
    /// (401); a new one is fetched unless another request already replaced it.
    pub(crate) async fn get(&self, failed: Option<&str>) -> Result<String> {
        let mut cached = self.cached.lock().await;
        if let Some(c) = cached.as_ref() {
            let replaced = failed.is_some_and(|f| f != c.token);
            let fresh = failed.is_none() && !c.expiring();
            if replaced || fresh {
                return Ok(c.token.clone());
            }
        }
        let token = (self.fetch)()
            .await
            .map_err(|e| Error::Token(e.to_string()))?
            .trim()
            .to_string();
        if token.is_empty() {
            return Err(Error::Token(
                "the token source gave an empty token".to_string(),
            ));
        }
        if reqwest::header::HeaderValue::from_str(&format!("Bearer {token}")).is_err() {
            return Err(Error::Token(
                "the token source gave a token that is not a valid header value".to_string(),
            ));
        }
        *cached = Some(Cached {
            expires_at: expires_at(&token),
            token: token.clone(),
        });
        Ok(token)
    }
}

impl Cached {
    fn expiring(&self) -> bool {
        self.expires_at.is_some_and(|at| {
            at.duration_since(SystemTime::now())
                .map_or(true, |left| left < REFRESH_AHEAD)
        })
    }
}

/// A client token's expiry, read from its `exp` claim; `None` for anything
/// else (an API key, an opaque token). Read, never verified: the broker
/// verifies.
pub fn expires_at(token: &str) -> Option<SystemTime> {
    let jwt = token.strip_prefix(CLIENT_TOKEN_PREFIX)?;
    let mut parts = jwt.split('.');
    let (_, payload, _, None) = (parts.next()?, parts.next()?, parts.next()?, parts.next()) else {
        return None;
    };
    let bytes = base64url_decode(payload)?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let exp = claims.get("exp")?.as_u64()?;
    Some(UNIX_EPOCH + Duration::from_secs(exp))
}

/// Base64url without padding (RFC 4648 §5), the JWT segment encoding.
fn base64url_decode(s: &str) -> Option<Vec<u8>> {
    let value = |c: u8| -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return None,
        } as u32)
    };
    let bytes = s.trim_end_matches('=').as_bytes();
    if bytes.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        let mut acc = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            acc |= value(c)? << (18 - 6 * i);
        }
        out.push((acc >> 16) as u8);
        if chunk.len() > 2 {
            out.push((acc >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(acc as u8);
        }
    }
    Some(out)
}

/// `POST /api/v1/tokens`. Obtained from [`crate::Queen::tokens`].
pub struct Tokens {
    inner: Arc<Inner>,
}

impl Tokens {
    pub(crate) fn new(inner: Arc<Inner>) -> Self {
        Self { inner }
    }

    /// Mint a token for `req.sub`, with this client's credential: a scoped
    /// key mints for itself; an `admin` key for the scoped key `req.key`
    /// names (its id or its name).
    pub async fn mint(&self, req: &MintTokenRequest) -> Result<MintTokenResponse> {
        if req.sub.is_empty() {
            return Err(Error::Invalid(
                "tokens.mint: sub is required (the entity the token is for)".into(),
            ));
        }
        let out: Option<MintTokenResponse> = self
            .inner
            .http
            .post_json("/api/v1/tokens", req, &Opts::default())
            .await?;
        out.ok_or_else(|| Error::Decode("POST /api/v1/tokens answered no body".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(exp_from_now: i64) -> String {
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + exp_from_now;
        let enc = |v: serde_json::Value| base64url_encode(v.to_string().as_bytes());
        format!(
            "qt_{}.{}.sig",
            enc(serde_json::json!({"alg": "HS256"})),
            enc(serde_json::json!({"sub": "dev1", "exp": exp}))
        )
    }

    fn base64url_encode(b: &[u8]) -> String {
        const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut out = String::new();
        for chunk in b.chunks(3) {
            let n = chunk.iter().enumerate().fold(0u32, |acc, (i, &c)| acc | (c as u32) << (16 - 8 * i));
            for i in 0..=chunk.len() {
                out.push(A[(n >> (18 - 6 * i) & 63) as usize] as char);
            }
        }
        out
    }

    #[test]
    fn the_expiry_is_read_from_a_client_token_and_nothing_else() {
        let at = expires_at(&token(100)).expect("a client token's exp");
        let left = at.duration_since(SystemTime::now()).unwrap();
        assert!(left > Duration::from_secs(90) && left <= Duration::from_secs(100));
        assert!(expires_at("qk_live_abc").is_none());
        assert!(expires_at("qt_not.a").is_none());
        assert!(expires_at("qt_a.b.c.d").is_none());
        assert!(expires_at("qt_a.!!!.c").is_none());
    }

    #[test]
    fn base64url_round_trips() {
        for s in ["", "a", "ab", "abc", "abcd", "{\"exp\":1}"] {
            assert_eq!(
                base64url_decode(&base64url_encode(s.as_bytes())).unwrap(),
                s.as_bytes()
            );
        }
    }

    #[tokio::test]
    async fn a_source_keeps_its_token_until_it_nears_its_exp() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c = calls.clone();
        let long = TokenSource::new(move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(token(3600))
            }
        });
        for _ in 0..3 {
            long.get(None).await.unwrap();
        }
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c = calls.clone();
        let short = TokenSource::new(move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(token(10))
            }
        });
        short.get(None).await.unwrap();
        short.get(None).await.unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_refusal_met_by_many_refreshes_once() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c = calls.clone();
        let source = TokenSource::new(move || {
            let n = c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move { Ok(format!("token-{n}")) }
        });
        let first = source.get(None).await.unwrap();
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let s = source.clone();
            let f = first.clone();
            set.spawn(async move { s.get(Some(&f)).await.unwrap() });
        }
        let mut seen = Vec::new();
        while let Some(t) = set.join_next().await {
            seen.push(t.unwrap());
        }
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(seen.iter().all(|t| t == "token-1"), "{seen:?}");
    }

    #[tokio::test]
    async fn a_failing_or_empty_source_is_a_token_error() {
        let bad = TokenSource::new(|| async { Err::<String, TokenError>("backend down".into()) });
        assert!(matches!(bad.get(None).await, Err(Error::Token(m)) if m.contains("backend down")));
        let empty = TokenSource::new(|| async { Ok(String::new()) });
        assert!(matches!(empty.get(None).await, Err(Error::Token(_))));
    }
}
