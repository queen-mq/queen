//! SaslAuthenticate — where the credential actually arrives, and where it is
//! checked against Queen.
//!
//! The check is one authenticated call to the broker
//! (`GET /api/v1/resources/queues`, through [`crate::queen::Catalog::refresh`]).
//! That route is the right one for three reasons and not because it was handy:
//! it is READ_ONLY rather than public, so a bad token is refused by the same
//! auth layer every later call passes through (server/src/auth.rs
//! `route_access_level`); it is not on the proxy's blocked list, so it means the
//! same thing in Cloud as it does against a bare broker (proxy/src/routes.rs
//! `classify` blocks `/status` and `/metrics`, which would otherwise be the
//! cheaper probes); and it is the call the connection's very first Metadata was
//! going to make anyway, so the answer is not thrown away — it lands in this
//! credential's catalog entry and the first Metadata is free.
//!
//! `refresh` and not `list` is deliberate. `list` serves the LAST GOOD answer
//! when a call fails, which is exactly right for a metadata refresh and exactly
//! wrong for an authentication: a token revoked a second ago would keep being
//! admitted from a cached list for as long as connections kept arriving.
//! `refresh` reports the failure. What it does keep is the failure itself, for
//! one cache TTL, which turns a fleet retrying a bad password from a call per
//! connection into a call per window.
//!
//! ## Rejected, or unchecked
//!
//! Two failures that look alike and must not be answered alike:
//!
//! * the broker said **401/403** — the token is not a credential. The client is
//!   told SASL_AUTHENTICATION_FAILED and the connection closes. Every client
//!   treats that as fatal and stops retrying, which is correct: no amount of
//!   retrying makes a wrong password right;
//! * the broker said **nothing usable** — unreachable, a 429, a 5xx. The token
//!   may be perfectly good. Answering SASL_AUTHENTICATION_FAILED here would
//!   make a client give up permanently because Queen was restarting, so the
//!   connection is CLOSED WITHOUT A RESPONSE instead. A disconnect during SASL
//!   is what every Kafka client has treated as retriable since before KIP-152
//!   gave failures an error code at all — the explicit code is what means
//!   "fatal", and its absence is what means "try again".
//!
//! ## No artificial delay
//!
//! Apache Kafka holds a failed connection open for
//! `connection.failed.authentication.delay.ms` before closing it, to slow brute
//! force. There is none here on purpose: the secret is a bearer token, not a
//! password, and holding failed connections open is a cheaper denial of service
//! than the one it prevents on a listener that has no connection cap yet.

use bytes::Bytes;
use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::{SaslAuthenticateRequest, SaslAuthenticateResponse};
use kafka_protocol::protocol::StrBytes;

use crate::queen;
use crate::sasl::{self, SaslState};
use crate::Facade;

/// KIP-368: how long this credential is good for. Zero is "for as long as the
/// connection lasts", and it is the truth — nothing here expires a token, and
/// a client that is told a positive lifetime would re-authenticate on a timer
/// this facade has no reason to run.
const NO_SESSION_EXPIRY: i64 = 0;

/// What one authentication attempt decided.
#[derive(Debug)]
pub enum Outcome {
    /// The credential is good. The connection carries it from now on.
    Admitted,
    /// The credential is not one. Answerable, and fatal for the client.
    Rejected(String),
    /// The credential could not be CHECKED. The connection closes with no
    /// answer, so the client retries. See the module header.
    Unavailable(String),
    /// The request had no business being here — a second authentication, or one
    /// on a listener with no SASL. Answered ILLEGAL_SASL_STATE and the
    /// connection carries on, because nothing about it has gone wrong: this is
    /// the ordinary request path, and `KafkaApis.handleSaslAuthenticateRequest`
    /// answers and returns too.
    OutOfPlace(String),
}

/// Handle one SaslAuthenticate. The response is written whatever the outcome
/// except [`Outcome::Unavailable`], where the caller closes instead.
pub async fn handle(
    facade: &Facade,
    req: &SaslAuthenticateRequest,
    state: &mut SaslState,
) -> (SaslAuthenticateResponse, Outcome) {
    // Out of place, and answerable — the two states Apache Kafka's own
    // `KafkaApis.handleSaslAuthenticateRequest` answers ILLEGAL_SASL_STATE for.
    // A SaslAuthenticate with no handshake in front of it is NOT one of them:
    // it never reaches the request path in Kafka, and it does not reach here
    // either (`crate::conn` closes the connection), because a client that sends
    // one has not agreed a mechanism and there is nothing to interpret its
    // bytes as.
    if !matches!(
        state,
        SaslState::AwaitingAuthenticate | SaslState::AwaitingRawToken
    ) {
        let why = match state {
            SaslState::Authenticated { .. } => "this connection has already authenticated",
            _ => "this listener has no SASL (QUEEN_KAFKA_SASL is unset)",
        };
        return (
            refusal(ResponseError::IllegalSaslState, why),
            Outcome::OutOfPlace(why.to_string()),
        );
    }

    let outcome = authenticate(facade, &req.auth_bytes, state).await;
    let response = match &outcome {
        Outcome::Admitted => SaslAuthenticateResponse::default()
            .with_error_code(0)
            // PLAIN's server response is empty: there is no challenge and
            // nothing to say back (RFC 4616).
            .with_auth_bytes(Bytes::new())
            .with_session_lifetime_ms(NO_SESSION_EXPIRY),
        Outcome::Rejected(why) => refusal(ResponseError::SaslAuthenticationFailed, why),
        // Not written — the caller closes — but built anyway so this function
        // has one return shape and the caller cannot forget which is which.
        Outcome::Unavailable(why) => refusal(ResponseError::SaslAuthenticationFailed, why),
        // Answered above; the arm exists because `authenticate` cannot produce
        // this and the match must still be total.
        Outcome::OutOfPlace(why) => refusal(ResponseError::IllegalSaslState, why),
    };
    (response, outcome)
}

/// The credential check itself, over the raw RFC 4616 bytes.
///
/// Shared with the **v0** flow, where the same bytes arrive as a bare frame
/// with no Kafka request around them ([`crate::conn`]). One implementation, so
/// the two flows cannot come to different conclusions about the same password.
pub async fn authenticate(facade: &Facade, bytes: &[u8], state: &mut SaslState) -> Outcome {
    let plain = match sasl::parse_plain(bytes) {
        Ok(p) => p,
        // The parse errors carry nothing the client sent — that is enforced in
        // `sasl`'s own tests — so this is safe to hand back and to log.
        Err(e) => return Outcome::Rejected(e.to_string()),
    };

    match facade.verify(&plain.password).await {
        Ok(()) => {
            *state = SaslState::Authenticated {
                token: plain.password,
                user: plain.username,
            };
            Outcome::Admitted
        }
        // The auth layer's own answer, at the broker (server/src/auth.rs) or at
        // the proxy (proxy/src/errors.rs `err_401`). Fatal and final.
        Err(queen::Error::Status {
            code: code @ (401 | 403),
            ..
        }) => Outcome::Rejected(format!(
            "Queen refused this credential (HTTP {code}). The password must be the bearer token \
             an SDK would send, and it must be valid for this cluster"
        )),
        // Everything else: the token is unjudged. See the module header.
        Err(e) => Outcome::Unavailable(format!("the credential could not be verified: {e}")),
    }
}

/// A refusal with its reason in the wire field, which SaslAuthenticate has at
/// every version — unlike SaslHandshake, whose reasons only reach the log.
fn refusal(error: ResponseError, why: &str) -> SaslAuthenticateResponse {
    SaslAuthenticateResponse::default()
        .with_error_code(error.code())
        .with_error_message(Some(StrBytes::from_string(why.to_string())))
        .with_auth_bytes(Bytes::new())
        .with_session_lifetime_ms(NO_SESSION_EXPIRY)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queen::testing::FakeQueen;
    use std::sync::Arc;

    fn plain(user: &str, password: &str) -> Bytes {
        Bytes::from(format!("\0{user}\0{password}"))
    }

    fn facade() -> (Facade, Arc<FakeQueen>) {
        crate::handlers::testing::facade_and_queen(&[("orders", 2)])
    }

    fn request(bytes: Bytes) -> SaslAuthenticateRequest {
        SaslAuthenticateRequest::default().with_auth_bytes(bytes)
    }

    #[tokio::test]
    async fn a_good_credential_admits_the_connection_and_is_what_queen_was_asked_with() {
        let (f, api) = facade();
        let mut state = SaslState::AwaitingAuthenticate;
        let (resp, outcome) = handle(&f, &request(plain("acme", "tenant-token")), &mut state).await;

        assert_eq!(resp.error_code, 0);
        assert!(matches!(outcome, Outcome::Admitted), "{outcome:?}");
        assert!(resp.auth_bytes.is_empty(), "PLAIN answers no challenge");
        assert_eq!(
            resp.session_lifetime_ms, 0,
            "a lifetime would make clients re-authenticate on a timer we do not run"
        );
        assert_eq!(
            state,
            SaslState::Authenticated {
                token: "tenant-token".into(),
                user: "acme".into()
            }
        );
        // The check was a REAL authenticated call, carrying the token the
        // client presented and not the process credential.
        assert_eq!(
            api.tokens.lock().unwrap().as_slice(),
            [Some("tenant-token".to_string())]
        );
    }

    #[tokio::test]
    async fn a_refused_credential_is_fatal_and_says_so_on_the_wire() {
        let (f, api) = facade();
        api.fail_list(queen::Error::status(401, "unauthorized"));
        let mut state = SaslState::AwaitingAuthenticate;
        let (resp, outcome) = handle(&f, &request(plain("acme", "wrong")), &mut state).await;

        assert_eq!(
            resp.error_code,
            ResponseError::SaslAuthenticationFailed.code()
        );
        assert!(matches!(outcome, Outcome::Rejected(_)), "{outcome:?}");
        assert_eq!(state, SaslState::AwaitingAuthenticate, "it was admitted");
        let message = resp.error_message.as_ref().unwrap().as_str();
        assert!(message.contains("401"), "{message}");
        assert!(!message.contains("wrong"), "the refusal echoed the token");
    }

    /// The distinction the whole module exists for: an unreachable Queen must
    /// not be reported as a wrong password, because a client stops retrying
    /// after one of those and retries after the other.
    #[tokio::test]
    async fn an_unreachable_queen_is_not_a_wrong_password() {
        for failure in [
            queen::Error::Transport("connection refused".into()),
            queen::Error::status(503, "draining"),
            queen::Error::status(429, "rate_limited"),
            queen::Error::status(500, "boom"),
            queen::Error::Body("not json".into()),
        ] {
            let (f, api) = facade();
            api.fail_list(failure.clone());
            let mut state = SaslState::AwaitingAuthenticate;
            let (_, outcome) = handle(&f, &request(plain("acme", "token")), &mut state).await;
            assert!(
                matches!(outcome, Outcome::Unavailable(_)),
                "{failure} was read as a rejection: {outcome:?}"
            );
            assert_eq!(state, SaslState::AwaitingAuthenticate);
        }
    }

    #[tokio::test]
    async fn a_malformed_response_is_rejected_without_asking_queen() {
        let (f, api) = facade();
        let mut state = SaslState::AwaitingAuthenticate;
        for bytes in [
            Bytes::from_static(b""),
            Bytes::from_static(b"no-nuls-at-all"),
            plain("", "token"),
            plain("acme", ""),
            Bytes::from_static(b"boss\0acme\0token"),
        ] {
            let (resp, outcome) = handle(&f, &request(bytes.clone()), &mut state).await;
            assert_eq!(
                resp.error_code,
                ResponseError::SaslAuthenticationFailed.code(),
                "{bytes:?}"
            );
            assert!(matches!(outcome, Outcome::Rejected(_)), "{bytes:?}");
        }
        assert!(
            api.tokens.lock().unwrap().is_empty(),
            "a malformed response reached Queen"
        );
    }

    #[tokio::test]
    async fn authenticating_twice_is_illegal_sasl_state() {
        let (f, _) = facade();
        let mut state = SaslState::Authenticated {
            token: "t".into(),
            user: "acme".into(),
        };
        let (resp, _) = handle(&f, &request(plain("acme", "t")), &mut state).await;
        assert_eq!(resp.error_code, ResponseError::IllegalSaslState.code());

        let mut disabled = SaslState::Disabled;
        let (resp, _) = handle(&f, &request(plain("acme", "t")), &mut disabled).await;
        assert_eq!(resp.error_code, ResponseError::IllegalSaslState.code());
    }
}
