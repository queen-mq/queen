//! SASL/PLAIN: the password is the Queen token, sent as the bearer of every
//! call this connection makes. Checked once, by a call the token must be
//! allowed to make.

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_protocol::messages::{
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};

use super::s;
use crate::conn::Conn;
use crate::core::CallError;
use crate::errors::*;
use crate::sasl::{parse_plain, PLAIN};
use crate::server::Ctx;

/// How long a token check is remembered.
const LOGIN_TTL: Duration = Duration::from_secs(30);
/// How many tokens are remembered at once.
const LOGIN_CACHE: usize = 1024;

/// Token checks remembered for [`LOGIN_TTL`], by a keyed hash of the token
/// (never the token itself): a client reconnecting in a loop costs one check
/// per token every 30 seconds, not a call into the broker per login. Refusals
/// are remembered too, so a stream of logins with a bad token costs no more.
pub struct Logins {
    keys: RandomState,
    seen: Mutex<HashMap<u64, (Instant, bool)>>,
}

impl Default for Logins {
    fn default() -> Logins {
        Logins {
            keys: RandomState::new(),
            seen: Mutex::new(HashMap::new()),
        }
    }
}

impl Logins {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, (Instant, bool)>> {
        self.seen.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The remembered answer for `token`, while it is fresh.
    fn get(&self, token: &str) -> Option<bool> {
        let key = self.keys.hash_one(token);
        self.lock()
            .get(&key)
            .filter(|(at, _)| at.elapsed() < LOGIN_TTL)
            .map(|(_, ok)| *ok)
    }

    fn put(&self, token: &str, ok: bool) {
        let key = self.keys.hash_one(token);
        let mut seen = self.lock();
        if seen.len() >= LOGIN_CACHE && !seen.contains_key(&key) {
            seen.retain(|_, (at, _)| at.elapsed() < LOGIN_TTL);
            if seen.len() >= LOGIN_CACHE {
                seen.clear();
            }
        }
        seen.insert(key, (Instant::now(), ok));
    }

    /// Forget every check (the tokens' rights may have changed).
    #[allow(dead_code)]
    pub fn clear(&self) {
        self.lock().clear();
    }
}

pub fn handshake(conn: &mut Conn, req: SaslHandshakeRequest) -> SaslHandshakeResponse {
    let ok = req.mechanism.as_str() == PLAIN;
    conn.handshaken = ok;
    SaslHandshakeResponse::default()
        .with_error_code(if ok { NONE } else { UNSUPPORTED_SASL_MECHANISM })
        .with_mechanisms(vec![s(PLAIN)])
}

/// The answer, and whether the connection closes after it.
pub async fn authenticate(
    ctx: &Arc<Ctx>,
    conn: &mut Conn,
    req: SaslAuthenticateRequest,
) -> (SaslAuthenticateResponse, bool) {
    let fail = |code: i16, why: String| {
        (
            SaslAuthenticateResponse::default()
                .with_error_code(code)
                .with_error_message(Some(s(why)))
                .with_auth_bytes(Bytes::new()),
            true,
        )
    };
    if !conn.handshaken || conn.authenticated {
        return fail(
            ILLEGAL_SASL_STATE,
            "SaslHandshake(PLAIN) first, once".into(),
        );
    }
    let plain = match parse_plain(&req.auth_bytes) {
        Ok(p) => p,
        Err(e) => return fail(SASL_AUTHENTICATION_FAILED, e.to_string()),
    };
    let allowed = match ctx.logins.get(&plain.password) {
        Some(ok) => ok,
        None => match ctx.queen.check_token(&plain.password).await {
            Ok(()) => {
                ctx.logins.put(&plain.password, true);
                true
            }
            Err(CallError::Status {
                code: 401 | 403, ..
            }) => {
                ctx.logins.put(&plain.password, false);
                false
            }
            Err(e) => {
                return fail(
                    SASL_AUTHENTICATION_FAILED,
                    format!("cannot check the token: {e}"),
                )
            }
        },
    };
    if !allowed {
        return fail(SASL_AUTHENTICATION_FAILED, "the token was refused".into());
    }
    conn.bearer = Some(plain.password);
    conn.authenticated = true;
    (
        SaslAuthenticateResponse::default()
            .with_error_code(NONE)
            .with_auth_bytes(Bytes::new()),
        false,
    )
}
