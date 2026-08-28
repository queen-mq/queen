//! SaslHandshake — "which mechanism are we going to speak?", and the fork
//! between the two SASL flows Kafka has.
//!
//! v0 and v1 differ in one thing and it is not a field: after a **v0**
//! handshake the SASL tokens travel as RAW bytes in the ordinary length-
//! prefixed frames, and after a **v1** handshake they travel inside
//! SaslAuthenticate requests. Both are supported here, because both fit the
//! framing this listener already has — a frame is a 4-byte big-endian length
//! and its bytes either way ([`crate::conn`]), so the v0 flow needs a state
//! rather than a second codec. Apache Kafka kept v0 for the same reason and
//! still answers it.
//!
//! The mechanism list in the response is the whole of the negotiation: a client
//! that asked for something else reads it, and its error message then names
//! what the broker DOES offer instead of only what it refused.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::{SaslHandshakeRequest, SaslHandshakeResponse};
use kafka_protocol::protocol::StrBytes;

use crate::sasl::{SaslState, PLAIN};

/// Handle one SaslHandshake, moving the connection's state on when it is
/// accepted.
///
/// `api_version` is load-bearing rather than informational: it is what decides
/// whether the NEXT frame on this connection is a raw SASL token or a
/// SaslAuthenticate request.
pub fn handle(
    req: &SaslHandshakeRequest,
    api_version: i16,
    state: &mut SaslState,
) -> SaslHandshakeResponse {
    // A handshake on a connection that has already authenticated. Apache Kafka
    // answers this one rather than closing — `KafkaApis.handleSaslHandshakeRequest`
    // returns ILLEGAL_SASL_STATE — because at this point the channel is out of
    // the authenticator's hands and is serving ordinary requests, so there IS a
    // response path. Re-authentication (KIP-368) is what a client would be
    // doing here, and it never asks: the session lifetime this facade reports
    // is 0, which means "this credential does not expire on me"
    // (`crate::handlers::sasl_authenticate`).
    if matches!(state, SaslState::Authenticated { .. }) {
        return refused(
            ResponseError::IllegalSaslState,
            "this connection has already authenticated",
        );
    }
    // ...and on a listener with no SASL at all. Same code, same reason it is a
    // response and not a close: nothing is gating this connection, so the
    // request reached the ordinary request path. It is also the single most
    // useful error this facade can give a client whose `security.protocol` says
    // SASL_PLAINTEXT while the listener is plaintext, which is a configuration
    // mistake that otherwise shows up as a hang or a parse error.
    if matches!(state, SaslState::Disabled) {
        return refused(
            ResponseError::IllegalSaslState,
            "this listener has no SASL (QUEEN_KAFKA_SASL is unset), so there is nothing to \
             handshake — connect with security.protocol=PLAINTEXT or SSL",
        );
    }

    let asked = req.mechanism.as_str();
    if !asked.eq_ignore_ascii_case(PLAIN) {
        // The state is NOT advanced: a client that asked for SCRAM may ask
        // again for PLAIN on the same connection, which is what a client with
        // a mechanism list does.
        tracing::info!(
            target: "kafka",
            mechanism = asked,
            "sasl handshake for a mechanism this facade does not offer"
        );
        return refused(
            ResponseError::UnsupportedSaslMechanism,
            "the only mechanism this facade offers is PLAIN",
        );
    }

    *state = if api_version == 0 {
        SaslState::AwaitingRawToken
    } else {
        SaslState::AwaitingAuthenticate
    };
    SaslHandshakeResponse::default()
        .with_error_code(0)
        .with_mechanisms(vec![StrBytes::from_static_str(PLAIN)])
}

/// A refusal, with the mechanism list still in it.
///
/// The list is what a client uses to pick again, and Kafka sends it on the
/// refusal for exactly that reason — an empty list on an UNSUPPORTED_SASL_MECHANISM
/// tells a client only that it was wrong.
///
/// `why` is not a wire field: SaslHandshakeResponse has no error message at any
/// version, so the sentence goes to the log, where an operator can find it.
fn refused(error: ResponseError, why: &str) -> SaslHandshakeResponse {
    tracing::warn!(target: "kafka", error = ?error, "sasl handshake refused: {why}");
    SaslHandshakeResponse::default()
        .with_error_code(error.code())
        .with_mechanisms(vec![StrBytes::from_static_str(PLAIN)])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(mechanism: &str) -> SaslHandshakeRequest {
        SaslHandshakeRequest::default().with_mechanism(StrBytes::from_string(mechanism.to_string()))
    }

    /// The version is what picks the flow, and getting it wrong desynchronises
    /// the connection: a v0 client's next frame is raw bytes and a v1 client's
    /// is a Kafka request.
    #[test]
    fn the_handshake_version_picks_the_flow() {
        let mut state = SaslState::Unauthenticated;
        let resp = handle(&request("PLAIN"), 0, &mut state);
        assert_eq!(resp.error_code, 0);
        assert_eq!(state, SaslState::AwaitingRawToken);

        let mut state = SaslState::Unauthenticated;
        let resp = handle(&request("PLAIN"), 1, &mut state);
        assert_eq!(resp.error_code, 0);
        assert_eq!(
            resp.mechanisms
                .iter()
                .map(|m| m.as_str())
                .collect::<Vec<_>>(),
            ["PLAIN"]
        );
        assert_eq!(state, SaslState::AwaitingAuthenticate);
    }

    /// Mechanism names are compared case-insensitively, because that is what
    /// the client's own config string is.
    #[test]
    fn the_mechanism_name_is_not_case_sensitive() {
        for spelling in ["PLAIN", "plain", "Plain"] {
            let mut state = SaslState::Unauthenticated;
            assert_eq!(handle(&request(spelling), 1, &mut state).error_code, 0);
            assert_eq!(state, SaslState::AwaitingAuthenticate);
        }
    }

    #[test]
    fn another_mechanism_is_refused_and_still_lists_what_there_is() {
        let mut state = SaslState::Unauthenticated;
        for asked in ["SCRAM-SHA-256", "GSSAPI", "OAUTHBEARER", ""] {
            let resp = handle(&request(asked), 1, &mut state);
            assert_eq!(
                resp.error_code,
                ResponseError::UnsupportedSaslMechanism.code(),
                "{asked}"
            );
            assert_eq!(
                resp.mechanisms
                    .iter()
                    .map(|m| m.as_str())
                    .collect::<Vec<_>>(),
                ["PLAIN"],
                "{asked}: the client cannot pick again from an empty list"
            );
            assert_eq!(
                state,
                SaslState::Unauthenticated,
                "{asked}: a refusal moved the connection on"
            );
        }
    }

    /// The two states where a handshake is answerable but wrong, which is
    /// exactly where Apache Kafka answers ILLEGAL_SASL_STATE rather than
    /// closing.
    #[test]
    fn a_handshake_out_of_its_place_is_illegal_sasl_state() {
        let mut authenticated = SaslState::Authenticated {
            token: "t".into(),
            user: "acme".into(),
        };
        let resp = handle(&request("PLAIN"), 1, &mut authenticated);
        assert_eq!(resp.error_code, ResponseError::IllegalSaslState.code());
        assert!(
            matches!(authenticated, SaslState::Authenticated { .. }),
            "a refused handshake dropped the connection's credential"
        );

        let mut disabled = SaslState::Disabled;
        let resp = handle(&request("PLAIN"), 1, &mut disabled);
        assert_eq!(resp.error_code, ResponseError::IllegalSaslState.code());
        assert_eq!(disabled, SaslState::Disabled);
    }
}
