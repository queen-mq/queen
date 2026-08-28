//! SASL/PLAIN: how a Kafka client presents a tenant credential, and what the
//! connection is allowed to do before it has.
//!
//! PLAN_QUEEN_KAFKA.md M5: "SASL/PLAIN mapping username/password to the tenant
//! token forwarded as normal proxy auth". The mapping is deliberately blunt:
//!
//! * the **password is the Queen bearer token**, verbatim. Not a lookup key,
//!   not a secret to be exchanged for one — the same string an SDK would put in
//!   `Authorization: Bearer …`. There is no directory here to look anything up
//!   in, and inventing one would be inventing a second credential store beside
//!   the proxy's;
//! * the **username is a label**. It is logged, and it is what makes a
//!   connection identifiable in the log without the token being in it. It is
//!   NOT checked against anything, because there is nothing here that could
//!   check it — the token names the tenant at the Queen end, and a username
//!   that disagreed with it would be either ignored or a second, weaker
//!   authorisation decision made in the wrong process.
//!
//! Both of those are stated in the operator docs rather than implied: a tenant
//! sets `sasl.username=<anything memorable>` and `sasl.password=<their token>`.
//!
//! ## Why PLAIN and only PLAIN
//!
//! SCRAM would need the facade to hold a per-tenant salted verifier, i.e. a
//! credential store — the thing this facade exists not to have. OAUTHBEARER
//! would be closer (the token IS a bearer token) but its handshake carries a
//! `kvsep`-framed GS2 header and an extensions map that every client builds
//! differently, and it buys nothing here: the token still arrives in one field
//! and is still validated by one call to Queen. PLAIN over TLS is what the
//! managed Kafka services offer for exactly this reason, and it is why the TLS
//! listener and this module land in the same milestone. A PLAIN listener on a
//! plaintext socket is a token in cleartext, and [`crate::conn`] says so at
//! boot rather than leaving it to be noticed.
//!
//! ## The state machine, and what Apache Kafka does at each step
//!
//! ```text
//!   Unauthenticated ── SaslHandshake(PLAIN) v0 ──▶ AwaitingRawToken
//!         │                                              │  raw RFC 4616 bytes
//!         │                                              ▼
//!         ├── SaslHandshake(PLAIN) v1 ─▶ AwaitingAuthenticate ─▶ Authenticated
//!         │                                (SaslAuthenticate)
//!         └── anything else ─────────────────────────────────▶ the connection closes
//! ```
//!
//! Before authentication a broker accepts ApiVersions and SaslHandshake and
//! nothing else: `SaslServerAuthenticator.handleKafkaRequest` throws
//! `IllegalSaslStateException` for every other API, which is an
//! `AuthenticationException`, which closes the channel WITHOUT a response. That
//! is not laziness on Kafka's part and it is not on ours either — a client that
//! sends a Produce before authenticating is not going to be helped by an error
//! code it did not ask for, and a listener that answers unauthenticated
//! requests at all is a listener that can be probed.
//!
//! AFTER authentication the same two APIs are answerable, and Kafka answers
//! them: `KafkaApis.handleSaslHandshakeRequest` and `handleSaslAuthenticateRequest`
//! return `ILLEGAL_SASL_STATE` with a message. So a response is written exactly
//! where Kafka writes one, and the connection closes exactly where Kafka closes
//! it. See [`crate::handlers::sasl_handshake`] and
//! [`crate::handlers::sasl_authenticate`].

/// The one mechanism this facade offers, spelled the way the wire spells it.
pub const PLAIN: &str = "PLAIN";

/// Where a connection is in the SASL conversation.
///
/// [`SaslState::Disabled`] is the whole of the non-SASL deployment: every
/// connection starts there, reaches Queen with `QUEEN_TOKEN`, and never leaves.
#[derive(Clone, PartialEq, Eq)]
pub enum SaslState {
    /// `QUEEN_KAFKA_SASL` is unset. No credential is expected and none is
    /// accepted; the process credential is what every call carries.
    Disabled,
    /// SASL is on and nothing has been presented. Only ApiVersions and
    /// SaslHandshake are answerable here.
    Unauthenticated,
    /// A SaslHandshake **v0** was accepted: the next frame is a raw RFC 4616
    /// token with no Kafka request around it.
    AwaitingRawToken,
    /// A SaslHandshake **v1** was accepted: the next frame is a Kafka
    /// SaslAuthenticate carrying the same bytes.
    AwaitingAuthenticate,
    /// Done. `token` is this connection's credential for every call to Queen;
    /// `user` is the label it named itself with.
    Authenticated { token: String, user: String },
}

/// Hand-written, and the only thing it does differently from a derived one is
/// the point of it: the token is REDACTED.
///
/// [`SaslState::Authenticated`] holds a credential, so a derived `Debug` would
/// put it in any log line, panic message or `dbg!` that ever names the state —
/// which is a rule ("tokens never in logs at any level") enforced by a habit
/// rather than by the type. This makes it the type's. The same reasoning is why
/// [`Plain`] has no `Debug` at all; there it can be absent, here it cannot,
/// because the state machine is worth being able to print.
impl std::fmt::Debug for SaslState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SaslState::Disabled => write!(f, "Disabled"),
            SaslState::Unauthenticated => write!(f, "Unauthenticated"),
            SaslState::AwaitingRawToken => write!(f, "AwaitingRawToken"),
            SaslState::AwaitingAuthenticate => write!(f, "AwaitingAuthenticate"),
            SaslState::Authenticated { user, .. } => {
                write!(f, "Authenticated {{ user: {user:?}, token: <redacted> }}")
            }
        }
    }
}

impl SaslState {
    /// Has this connection earned the right to send ordinary requests?
    ///
    /// True for [`SaslState::Disabled`] — a listener with no SASL authenticates
    /// nobody and refuses nobody — and for [`SaslState::Authenticated`].
    pub fn admitted(&self) -> bool {
        matches!(self, SaslState::Disabled | SaslState::Authenticated { .. })
    }

    /// The credential this connection reaches Queen with, if it presented one.
    pub fn token(&self) -> Option<&str> {
        match self {
            SaslState::Authenticated { token, .. } => Some(token),
            _ => None,
        }
    }

    /// The label this connection named itself with, for the log.
    pub fn user(&self) -> Option<&str> {
        match self {
            SaslState::Authenticated { user, .. } => Some(user),
            _ => None,
        }
    }
}

/// A parsed PLAIN initial response.
///
/// `password` is a credential and is deliberately not `Debug`-printable — the
/// whole struct is not `Debug`, so it cannot reach a log line by being
/// interpolated into one by accident.
pub struct Plain {
    /// The authorization identity. RFC 4616 allows a client to ask to act as
    /// somebody else; nothing here can grant that, so a non-empty one is
    /// refused rather than ignored (see [`parse_plain`]).
    pub authzid: String,
    pub username: String,
    pub password: String,
}

/// Why a PLAIN initial response was not usable.
///
/// Every variant's `Display` is safe to log and to put in a
/// `SaslAuthenticate` `error_message`: none of them can contain the password,
/// because none of them carries any of the input. That is the point of an enum
/// here rather than a formatted string built at the parse site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlainError {
    /// Not `[authzid] NUL authcid NUL passwd`.
    Separators(usize),
    /// The bytes are not UTF-8. RFC 4616 says they must be.
    NotUtf8,
    /// No username. Apache Kafka's own `PlainSaslServer` refuses this too, and
    /// every client requires the field, so it is a configuration mistake rather
    /// than a shape this facade could helpfully accept.
    NoUsername,
    /// No password, i.e. no token. The one field this facade cannot invent.
    NoPassword,
    /// An authorization identity was asked for. See [`Plain::authzid`].
    Impersonation,
}

impl std::fmt::Display for PlainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlainError::Separators(n) => write!(
                f,
                "a PLAIN response is authzid\\0username\\0password and this one has {n} NUL \
                 separators, not 2"
            ),
            PlainError::NotUtf8 => write!(f, "a PLAIN response must be UTF-8 (RFC 4616)"),
            PlainError::NoUsername => write!(
                f,
                "no username. It is a label rather than a check, but it must be there: set \
                 sasl.username to anything that identifies this client in a log"
            ),
            PlainError::NoPassword => write!(
                f,
                "no password. The password is the Queen bearer token for this connection: set \
                 sasl.password to the token an SDK would send as `Authorization: Bearer …`"
            ),
            PlainError::Impersonation => write!(
                f,
                "an authorization identity was requested. This facade authorizes the token in \
                 the password field and nothing else, so it cannot honour one"
            ),
        }
    }
}

/// Parse an RFC 4616 PLAIN initial response: `[authzid] NUL authcid NUL passwd`.
///
/// Strict about the separator count because the alternative is worse than a
/// refusal: a password containing a NUL would split into a shorter token, which
/// would then fail to authenticate for a reason nobody could see. RFC 4616
/// forbids NUL in every field, so counting them is exact rather than heuristic.
pub fn parse_plain(bytes: &[u8]) -> Result<Plain, PlainError> {
    let nuls = bytes.iter().filter(|b| **b == 0).count();
    if nuls != 2 {
        return Err(PlainError::Separators(nuls));
    }
    let text = std::str::from_utf8(bytes).map_err(|_| PlainError::NotUtf8)?;
    let mut parts = text.split('\0');
    // Three parts exactly, because there are exactly two separators.
    let authzid = parts.next().unwrap_or("");
    let username = parts.next().unwrap_or("");
    let password = parts.next().unwrap_or("");
    if !authzid.is_empty() {
        return Err(PlainError::Impersonation);
    }
    if username.is_empty() {
        return Err(PlainError::NoUsername);
    }
    if password.is_empty() {
        return Err(PlainError::NoPassword);
    }
    Ok(Plain {
        authzid: authzid.to_string(),
        username: username.to_string(),
        password: password.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain(authzid: &str, user: &str, password: &str) -> Vec<u8> {
        format!("{authzid}\0{user}\0{password}").into_bytes()
    }

    #[test]
    fn a_well_formed_response_yields_the_label_and_the_token() {
        let got = parse_plain(&plain("", "acme", "eyJhbGciOi.stub.token")).unwrap();
        assert_eq!(got.username, "acme");
        assert_eq!(got.password, "eyJhbGciOi.stub.token");
        assert!(got.authzid.is_empty());
    }

    /// A token is an arbitrary string: JWTs carry dots and dashes, an API key
    /// may carry anything but a NUL. None of it may be mangled.
    #[test]
    fn the_token_is_taken_verbatim() {
        for token in [
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhIn0.c2ln",
            "qk_live_AbCd-1234_5678",
            "  leading and trailing spaces  ",
            "a:colon:separated:thing",
            "üñïçø∂é",
        ] {
            let got = parse_plain(&plain("", "acme", token)).unwrap();
            assert_eq!(got.password, token, "the token was rewritten");
        }
    }

    /// The refusal alone. `Plain` is deliberately neither `Debug` nor
    /// `PartialEq` — it holds a token, and a struct that cannot be printed
    /// cannot be printed by accident — so a test compares the ERROR and asks
    /// for the value only when it expects one.
    fn refusal(bytes: &[u8]) -> PlainError {
        parse_plain(bytes).map(|_| ()).unwrap_err()
    }

    #[test]
    fn the_separators_are_counted_exactly() {
        // No NUL at all, one NUL, and one too many.
        assert_eq!(refusal(b"acmetoken"), PlainError::Separators(0));
        assert_eq!(refusal(b"acme\0token"), PlainError::Separators(1));
        assert_eq!(refusal(b"\0acme\0tok\0en"), PlainError::Separators(3));
        // The empty frame — what a client sends when it has nothing to say, and
        // what a raw-SASL connection sends if it gets the flow wrong.
        assert_eq!(refusal(b""), PlainError::Separators(0));
    }

    #[test]
    fn the_two_fields_that_must_be_there_are_named_when_they_are_not() {
        assert_eq!(refusal(&plain("", "", "token")), PlainError::NoUsername);
        assert_eq!(refusal(&plain("", "acme", "")), PlainError::NoPassword);
        // Both missing: the username is reported, because it is read first and
        // one clear reason beats two.
        assert_eq!(refusal(&plain("", "", "")), PlainError::NoUsername);
    }

    #[test]
    fn impersonation_is_refused_rather_than_ignored() {
        assert_eq!(
            refusal(&plain("someone-else", "acme", "token")),
            PlainError::Impersonation
        );
    }

    #[test]
    fn non_utf8_is_refused() {
        let mut bytes = b"\0acme\0".to_vec();
        bytes.push(0xff);
        assert_eq!(refusal(&bytes), PlainError::NotUtf8);
    }

    /// The secrets rule, enforced where it can be: no error this parser can
    /// produce carries anything the client sent.
    #[test]
    fn no_refusal_can_carry_the_password() {
        let secret = "super-secret-token";
        for bytes in [
            plain("", "", secret),
            plain("boss", "acme", secret),
            b"nonuls".to_vec(),
        ] {
            let e = refusal(&bytes);
            let said = format!("{e} {e:?}");
            assert!(
                !said.contains(secret),
                "the refusal leaked the token: {said}"
            );
        }
    }

    /// The type carries a credential, so printing it must not print one —
    /// whatever a future log line does with it.
    #[test]
    fn printing_the_state_never_prints_the_token() {
        let state = SaslState::Authenticated {
            token: "s3cr3t-token".into(),
            user: "acme".into(),
        };
        let printed = format!("{state:?}");
        assert!(!printed.contains("s3cr3t-token"), "{printed}");
        assert!(printed.contains("redacted"), "{printed}");
        // The label is kept: it is what makes a printed state identify anyone.
        assert!(printed.contains("acme"), "{printed}");
    }

    #[test]
    fn only_two_states_may_send_ordinary_requests() {
        assert!(SaslState::Disabled.admitted());
        assert!(SaslState::Authenticated {
            token: "t".into(),
            user: "acme".into()
        }
        .admitted());
        for state in [
            SaslState::Unauthenticated,
            SaslState::AwaitingRawToken,
            SaslState::AwaitingAuthenticate,
        ] {
            assert!(!state.admitted(), "{state:?} was admitted");
            assert_eq!(state.token(), None);
        }
        // A listener with SASL off carries the PROCESS credential, which is
        // why it has none of its own.
        assert_eq!(SaslState::Disabled.token(), None);
        assert_eq!(
            SaslState::Authenticated {
                token: "t".into(),
                user: "acme".into()
            }
            .token(),
            Some("t")
        );
    }
}
