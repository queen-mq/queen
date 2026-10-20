//! SASL/PLAIN: the password is the Queen token, sent as the bearer of every
//! call this connection makes. Checked once, by a call the token must be
//! allowed to make.

use std::sync::Arc;

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
    match ctx.queen.queues(Some(&plain.password)).await {
        Ok(_) => {}
        Err(CallError::Status {
            code: 401 | 403, ..
        }) => return fail(SASL_AUTHENTICATION_FAILED, "the token was refused".into()),
        Err(e) => {
            return fail(
                SASL_AUTHENTICATION_FAILED,
                format!("cannot check the token: {e}"),
            )
        }
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
