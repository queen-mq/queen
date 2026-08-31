//! Human identity endpoints. OWNER: Agent G (Track A).
//!
//!   * local login (bcrypt) + session JWT in an httpOnly cookie
//!   * Google OAuth (authorization-code + OIDC id_token, port of
//!     proxy/src/google-auth.js): token exchange, id_token verify against
//!     Google's JWKS, verified-email-only linking, opt-in auto-provision
//!   * GitHub OAuth (OAuth2 *without* OIDC): /user + /user/emails, verified
//!     primary email only, same resolve/link/provision path
//!   * `/auth/me`, `/auth/session-token` (short Bearer for the SPA, and the
//!     fleet dashboard handoff, see `session_token`), logout
//!
//! Sessions are proxy-minted user JWTs (`auth::Keys::mint_user_jwt`). A session
//! token is NOT cluster-scoped (`cluster=None`): its `role` claim is a
//! placeholder — the real per-cluster role is resolved live from `cluster_roles`
//! by `auth::authenticate`, so a session grants nothing without a membership row.
//!
//! Injectability for tests: the security-critical logic is factored into pure
//! functions (`sign_state`/`verify_state`, `is_safe`, `decide_resolution`,
//! `validate_google_claims`, `select_github_email`) exercised directly with
//! synthetic token-exchange / userinfo payloads — no network or DB in unit tests.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::{Form, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use base64::engine::general_purpose::STANDARD as B64;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64_URL;
use base64::Engine;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use rand::rngs::OsRng;
use rand::RngCore;
use ring::hmac;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::errors;
use crate::httpget;
use crate::state::{Role, St};

// ---------------------------------------------------------------------------
// constants
// ---------------------------------------------------------------------------

const GOOGLE_AUTHORIZE_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const GOOGLE_JWKS_URL: &str = "https://www.googleapis.com/oauth2/v3/certs";
const GOOGLE_ISSUERS: [&str; 2] = ["https://accounts.google.com", "accounts.google.com"];

const GITHUB_AUTHORIZE_URL: &str = "https://github.com/login/oauth/authorize";
const GITHUB_TOKEN_URL: &str = "https://github.com/login/oauth/access_token";
const GITHUB_USER_URL: &str = "https://api.github.com/user";
const GITHUB_EMAILS_URL: &str = "https://api.github.com/user/emails";

/// Placeholder role for a non-cluster-scoped session token (see module docs).
const SESSION_ROLE: &str = "viewer";
/// Signed OAuth `state` lifetime (nonce + next round-trip through the provider).
const STATE_TTL_S: i64 = 300;
/// Short Bearer handed to the SPA by `/auth/session-token`.
const SESSION_TOKEN_TTL_S: u64 = 900;
/// Per-IP login throttle: at most LOGIN_MAX attempts per LOGIN_WINDOW.
const LOGIN_WINDOW: Duration = Duration::from_secs(60);
const LOGIN_MAX: u32 = 10;
/// Google JWKS cache lifetime.
const JWKS_TTL: Duration = Duration::from_secs(3600);
/// Timeout for every outbound provider call (token exchange, userinfo, JWKS).
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

pub fn router() -> Router<St> {
    Router::new()
        .route("/login", get(login_page).post(login_post))
        .route("/logout", post(logout))
        .route("/me", get(me))
        .route("/session-token", get(session_token))
        .route("/google", get(google_start))
        .route("/google/callback", get(google_callback))
        .route("/github", get(github_start))
        .route("/github/callback", get(github_callback))
}

// ---------------------------------------------------------------------------
// request shapes
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct StartQuery {
    next: Option<String>,
}

/// `/auth/session-token`. Both absent on the SPA's own call; both present on a
/// dashboard handoff. Unknown parameters are ignored rather than rejected: the
/// minter is a different codebase, and a 400 here would be a broken console.
#[derive(Deserialize)]
struct SessionTokenQuery {
    token: Option<String>,
    next: Option<String>,
}

#[derive(Deserialize)]
struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct LoginForm {
    email: String,
    password: String,
    next: Option<String>,
}

/// A resolved local user + its owning tenant (for session mint + audit).
#[derive(Clone, Debug, PartialEq, Eq)]
struct UserRef {
    user_id: Uuid,
    tenant_id: Uuid,
}

// ---------------------------------------------------------------------------
// local login
// ---------------------------------------------------------------------------

async fn login_page(State(st): State<St>, Query(q): Query<StartQuery>) -> Response {
    let next = safe_next(q.next.as_deref());
    html(StatusCode::OK, render_login(&st, &next, None))
}

async fn login_post(
    State(st): State<St>,
    headers: HeaderMap,
    Form(form): Form<LoginForm>,
) -> Response {
    let ip = client_ip(&headers);
    if let Err(retry) = throttle(&ip) {
        return errors::err_429(errors::CODE_RATE_LIMITED, retry, "too many login attempts");
    }
    let next = safe_next(form.next.as_deref());
    match verify_local(&st, &form.email, &form.password).await {
        Some(user) => establish_session(&st, &headers, user, &next).await,
        None => html(
            StatusCode::UNAUTHORIZED,
            render_login(&st, &next, Some("Invalid email or password.")),
        ),
    }
}

/// bcrypt-verify a local password. None on: no DB, unknown email, OAuth-only
/// user (NULL password_hash), or wrong password. Never distinguishes them to
/// the client (the caller returns a single generic message).
async fn verify_local(st: &St, email: &str, password: &str) -> Option<UserRef> {
    let pool = st.db.as_ref()?;
    let client = pool.get().await.ok()?;
    let row = client
        .query_opt(
            "SELECT id::text, tenant_id::text, password_hash \
             FROM queen_proxy.users WHERE email = lower($1)",
            &[&email],
        )
        .await
        .ok()??;
    let hash: Option<String> = row.get(2);
    let hash = hash?;
    if bcrypt::verify(password, &hash).unwrap_or(false) {
        row_to_userref(&row)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// sessions
// ---------------------------------------------------------------------------

/// Mint a session JWT, record a `login` op, and redirect to `next` with the
/// session cookie set.
async fn establish_session(st: &St, headers: &HeaderMap, user: UserRef, next: &str) -> Response {
    let token = match st.keys.mint_user_jwt(user.user_id, SESSION_ROLE, None, st.cfg.jwt_ttl_s) {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(target: "oauth", err = %e, "session mint failed");
            // "No signer at all" is a deliberate, bootable configuration, so it
            // gets the configuration answer rather than an opaque 500.
            if !st.keys.can_mint() {
                return err_no_signer();
            }
            return err_500("session token could not be minted");
        }
    };
    record_op(st, user.tenant_id, user.user_id, "login", Some(user.user_id.to_string()), json!({})).await;
    let cookie = session_cookie(st, headers, &token);
    redirect(StatusCode::SEE_OTHER, next, Some(&cookie))
}

/// Log out: deny-list the presented session, then clear the cookie.
///
/// Revocation is what actually ends the session. Clearing the cookie only stops
/// *this browser* from sending a token that stays valid for the rest of its TTL
/// (`jwt_ttl_s`, a day by default) — anything that copied it keeps working.
///
/// Scope: only the cookie's own jti is deny-listed. The short bearers handed to
/// the SPA by `/auth/session-token` carry their own jti and are not tracked, so
/// they run out their `SESSION_TOKEN_TTL_S` (15 min).
async fn logout(State(st): State<St>, headers: HeaderMap) -> Response {
    revoke_presented_session(&st, &headers).await;
    let mut resp = json_response(StatusCode::OK, json!({ "ok": true }));
    for cookie in clear_cookies(&st, &headers) {
        if let Ok(v) = HeaderValue::from_str(&cookie) {
            resp.headers_mut().append(header::SET_COOKIE, v);
        }
    }
    resp
}

/// Best-effort `queen_proxy.revoke_session(jti, exp, 'user', sub)` for whatever
/// session the request presents. Every miss is silent by design — no cookie, a
/// malformed or already-expired one, no pxdb: there is nothing left to revoke
/// and the logout still has to succeed (a logout that 500s leaves the user
/// looking logged in). A failed DB call is warned, not surfaced: the cookie is
/// cleared either way.
async fn revoke_presented_session(st: &St, headers: &HeaderMap) {
    let Some(tok) = presented_session(st, headers) else { return };
    let Ok(claims) = st.keys.verify_jwt_claims(&tok) else { return };
    let Some(pool) = st.db.as_ref() else { return };
    let Ok(client) = pool.get().await else {
        tracing::warn!(target: "oauth", "pxdb unavailable; session cookie cleared but token stays valid until exp");
        return;
    };
    // revoke_session(p_jti TEXT, p_expires_at TIMESTAMPTZ, p_actor TEXT,
    // p_actor_id UUID) — exp is the token's own, so the sweep can drop the row
    // once it expires. `$3::text::uuid` for the same reason as record_op above.
    match client
        .execute(
            "SELECT queen_proxy.revoke_session($1, to_timestamp($2), 'user', $3::text::uuid)",
            &[&claims.jti, &(claims.exp as f64), &claims.user_id.to_string()],
        )
        .await
    {
        Ok(_) => {
            st.keys.note_revoked(&claims.jti);
            tracing::info!(target: "oauth", user_id = %claims.user_id, "session revoked on logout");
        }
        Err(e) => {
            tracing::warn!(target: "oauth", err = %e, "revoke_session failed; cookie cleared but token stays valid until exp");
        }
    }
}

/// The user's own row. `is_operator` is the STORED bit; whether the capability
/// is live also depends on this cell's `QUEEN_PROXY_OPERATOR_ENABLED`, and
/// `/auth/me` reports both so the SPA can tell "you are not an operator" from
/// "not on this cell" without guessing.
const ME_USER_SQL: &str = "
    SELECT u.email, u.is_operator, t.slug
    FROM queen_proxy.users u
    JOIN queen_proxy.tenants t ON t.id = u.tenant_id
    WHERE u.id = $1::text::uuid";

/// The clusters a NORMAL user may select, with the role on each.
const ME_CLUSTERS_SQL: &str = "
    SELECT c.id::text, c.slug, cr.role, t.slug, t.id::text, c.status, ce.slug
    FROM queen_proxy.cluster_roles cr
    JOIN queen_proxy.clusters c ON c.id = cr.cluster_id
    JOIN queen_proxy.tenants  t ON t.id = c.tenant_id
    JOIN queen_proxy.cells    ce ON ce.id = c.cell_id
    WHERE cr.user_id = $1::text::uuid
    ORDER BY t.slug, c.slug";

/// The clusters an OPERATOR may select: all of them, as admin — the effective
/// role acting.rs gives them, membership row or not, so the nav the SPA draws
/// matches what the data plane will actually allow.
const ME_CLUSTERS_OPERATOR_SQL: &str = "
    SELECT c.id::text, c.slug, 'admin'::text, t.slug, t.id::text, c.status, ce.slug
    FROM queen_proxy.clusters c
    JOIN queen_proxy.tenants t ON t.id = c.tenant_id
    JOIN queen_proxy.cells   ce ON ce.id = c.cell_id
    ORDER BY t.slug, c.slug";

/// Everything the SPA needs to render itself, in one call: who you are,
/// whether you are an operator (and whether that means anything on this cell),
/// which cluster you are acting on, and the list you may switch to. The single
/// source of truth for the nav and the tenant selector — the SPA must never
/// infer a permission.
///
/// Deliberately NOT under `/api/console/*`: those endpoints resolve a cluster
/// from Host and 421 when it names none, which is exactly the situation one
/// webapp hostname puts every request in. This one needs no cluster at all.
async fn me(State(st): State<St>, headers: HeaderMap) -> Response {
    // An API-KEY bearer is answered by `kafka_identity` (see that module for
    // why this route and not a new one). `None` for everything else — no api
    // key, a session bearer, a cookie — so the browser path below is unchanged.
    if let Some(resp) = crate::kafka_identity::bearer_me(&st, &headers).await {
        return resp;
    }
    // Cookie only: `/auth/me` describes the BROWSER's session. A bearer would
    // answer the same, but the SPA boots from the cookie and keeping one
    // source here avoids reporting a session the browser does not hold.
    let Some(tok) = presented_session(&st, &headers) else {
        return errors::err_401("no session");
    };
    let claims = match st.keys.verify_jwt_claims(&tok) {
        Ok(c) => c,
        Err(_) => return errors::err_401("invalid session"),
    };
    // A signature-valid session is not necessarily a live one: logout
    // deny-lists the jti, so the deny-list has to be consulted here too
    // or a copied cookie keeps reporting a healthy session after its
    // owner signed out.
    if st.keys.is_revoked(&st.db, &claims.jti).await {
        return errors::err_401("invalid session");
    }

    let is_operator = st.keys.is_operator(&st.db, claims.user_id).await;
    let operator_live = st.cfg.operator_enabled && is_operator;

    let mut email: Option<String> = None;
    let mut tenant_slug: Option<String> = None;
    let mut clusters: Vec<Value> = Vec::new();
    if let Some(pool) = st.db.as_ref() {
        match pool.get().await {
            Ok(client) => {
                let uid = claims.user_id.to_string();
                match client.query_opt(ME_USER_SQL, &[&uid]).await {
                    Ok(Some(row)) => {
                        email = Some(row.get::<_, String>(0));
                        tenant_slug = Some(row.get::<_, String>(2));
                    }
                    Ok(None) => {
                        // The session outlived its user row (deleted, or a dev
                        // pxdb reset). Nothing to render — send the SPA to
                        // login rather than an identity with no owner.
                        return errors::err_401("session no longer valid");
                    }
                    Err(e) => tracing::warn!(target: "oauth", err = %e, "me: user lookup failed"),
                }
                let (sql, params): (&str, Vec<&(dyn tokio_postgres::types::ToSql + Sync)>) =
                    if operator_live {
                        (ME_CLUSTERS_OPERATOR_SQL, vec![])
                    } else {
                        (ME_CLUSTERS_SQL, vec![&uid])
                    };
                match client.query(sql, &params).await {
                    Ok(rows) => {
                        clusters = rows
                            .iter()
                            .map(|r| {
                                json!({
                                    "id": r.get::<_, String>(0),
                                    "slug": r.get::<_, String>(1),
                                    "role": r.get::<_, String>(2),
                                    "tenant_slug": r.get::<_, String>(3),
                                    "tenant_id": r.get::<_, String>(4),
                                    "status": r.get::<_, String>(5),
                                    // The CELL backing this cluster. A cluster row
                                    // names both a tenant and a cell, so one
                                    // selector picks the tenant to scope by AND the
                                    // broker to forward to — and the UI can label
                                    // cell-level numbers as such instead of letting
                                    // them read as the tenant's own.
                                    "cell_slug": r.get::<_, String>(6),
                                })
                            })
                            .collect();
                    }
                    Err(e) => tracing::warn!(target: "oauth", err = %e, "me: cluster list failed"),
                }
            }
            Err(e) => tracing::warn!(target: "oauth", err = %e, "me: pool.get failed"),
        }
    }

    // The cluster this very request would act on, resolved by the same code
    // the data plane uses (act-as header, else Host — and on a shared host, the
    // credential). A failure is reported as `null`, never as an error:
    // /auth/me must still answer so the SPA can draw the selector and let the
    // user pick a cluster that DOES work. On a shared host that is the NORMAL
    // first load: the session has not named a cluster yet, so `null` here is
    // precisely the signal that the selector must be shown.
    let acting = crate::acting::peek_ctx(&st, &headers)
        .await
        .map(|ctx| json!({ "id": ctx.cluster_id.to_string(), "slug": ctx.slug }));

    json_response(
        StatusCode::OK,
        json!({
            "user_id": claims.user_id.to_string(),
            "email": email,
            "tenant_slug": tenant_slug,
            // The stored bit vs. what it grants HERE. Both, because a cell
            // with the flag off must render as a plain tenant dashboard.
            "is_operator": is_operator,
            "operator_enabled": st.cfg.operator_enabled,
            "operator_live": operator_live,
            "acting_cluster": acting,
            "clusters": clusters,
            "act_cluster_header": crate::config::ACT_CLUSTER_HEADER,
            // Kept from the original shape. `role` is the session token's
            // PLACEHOLDER claim, not a permission — the per-cluster roles in
            // `clusters` above are the real ones.
            "role": role_str(claims.role),
            "cluster": claims.cluster.map(|u| u.to_string()),
        }),
    )
}

/// TWO endpoints share this path, told apart by what the host can DO.
///
/// **Mint-capable host**: the SPA's own call. Reads the session cookie and
/// mints the short Bearer it uses for the data plane. Unchanged, query or no
/// query: the auth host keeps exactly the behaviour it has always had.
///
/// **Verify-only host with `?token=`**: the fleet dashboard handoff. The
/// control plane redirects a browser here with a session it minted
/// (`queen-control`'s `cell/console.rs`), and this host, which holds the public
/// key and no signer, VERIFIES that token and adopts it as the cookie.
///
/// Establishing was wired to minting, and on a verify-only host minting is the
/// one thing that cannot work: the handoff answered 503 `not_configured` with
/// the token sitting unread in the URL beside it, because the handler had no
/// query extractor at all. Measured against a real cell on 2026-08-28. The two
/// operations were never the same one: a host with no signer can still verify
/// a signature perfectly well, and that is all an establish needs.
///
/// The cookie's life is the TOKEN's own remaining life, not `jwt_ttl_s`: this
/// host cannot mint a replacement, so the console lives exactly as long as what
/// it was handed and no longer. A cookie outliving its token would just be a
/// session that 401s while looking present.
async fn session_token(
    State(st): State<St>,
    headers: HeaderMap,
    Query(q): Query<SessionTokenQuery>,
) -> Response {
    if is_handoff(st.keys.can_mint(), q.token.as_deref()) {
        // `is_handoff` already established it is Some and non-blank.
        let presented = q.token.as_deref().unwrap_or("").trim();
        return establish_handoff(&st, &headers, presented, q.next.as_deref()).await;
    }
    let Some(tok) = presented_session(&st, &headers) else {
        return errors::err_401("no session");
    };
    let claims = match st.keys.verify_jwt_claims(&tok) {
        Ok(c) => c,
        Err(_) => return errors::err_401("invalid session"),
    };
    // Minting from a revoked session would hand out a NEW jti that the
    // deny-list has never seen — a logout could then be outlived by a bearer
    // derived from the dead cookie. The check has to happen before the mint.
    if st.keys.is_revoked(&st.db, &claims.jti).await {
        return errors::err_401("invalid session");
    }
    match st
        .keys
        .mint_user_jwt(claims.user_id, SESSION_ROLE, claims.cluster, SESSION_TOKEN_TTL_S)
    {
        Ok(token) => json_response(
            StatusCode::OK,
            json!({ "token": token, "expires_in": SESSION_TOKEN_TTL_S }),
        ),
        Err(e) => {
            tracing::error!(target: "oauth", err = %e, "session-token mint failed");
            if !st.keys.can_mint() {
                return err_no_signer();
            }
            err_500("session token could not be minted")
        }
    }
}

/// Is this request the handoff rather than the SPA's mint call?
///
/// A host that CAN mint is never the handoff, whatever the query says. That is
/// what keeps the auth host's `/auth/session-token` byte-for-byte what it was:
/// the new behaviour is unreachable on the only host that had the old one.
fn is_handoff(can_mint: bool, token: Option<&str>) -> bool {
    !can_mint && token.map(|t| !t.trim().is_empty()).unwrap_or(false)
}

/// What a presented handoff token earns. Pure (signature, issuer, audience and
/// expiry, plus the redirect target), so the whole matrix is pinned by tests
/// without a `Config`, a pool or a router. The deny-list, which needs the DB, is
/// the caller's half.
#[derive(Debug, PartialEq, Eq)]
enum Handoff {
    /// Verified. Adopt the presented token as the cookie for `max_age_s` and
    /// send the browser on to `next`.
    Establish { next: String, max_age_s: u64, jti: String },
    /// Refused, carrying the reason for the LOG. Every one of them answers the
    /// client the same 401: which of these it was is not the browser's business,
    /// and the portal that sent it here can read its own audit trail.
    Refuse(&'static str),
}

/// Verify a handoff token against this host's configured public key.
///
/// `now` is injected so the expiry edges are testable. Note what is NOT checked
/// here: `role`. A handoff asserts identity only; the cell resolves the real
/// per-cluster role from its own `cluster_roles`, exactly as it does for a
/// cookie, so a token whose placeholder role says `viewer` grants nothing by
/// saying it.
fn decide_handoff(keys: &crate::auth::Keys, token: &str, next: Option<&str>, now: i64) -> Handoff {
    let claims = match keys.verify_jwt_claims(token) {
        Ok(c) => c,
        Err(e) => {
            // The reason is a fixed string per variant, never the token or any
            // claim inside it.
            return Handoff::Refuse(match e {
                crate::auth::JwtReject::Expired => "expired",
                crate::auth::JwtReject::BadIssuer => "issuer mismatch",
                crate::auth::JwtReject::BadAudience => "minted for another cell",
                crate::auth::JwtReject::BadSignature => "signature invalid",
                crate::auth::JwtReject::NotConfigured => "no verification key configured",
                _ => "malformed token",
            });
        }
    };
    // Verification passes a token that died within jsonwebtoken's 60s clock
    // leeway. That leeway is there so authentication survives skew; it is not a
    // licence to hand out a session with nothing left in it, and a cookie whose
    // Max-Age is zero is a browser instruction to forget it immediately.
    let left = claims.exp - now;
    if left <= 0 {
        return Handoff::Refuse("expired");
    }
    Handoff::Establish {
        next: safe_next(next),
        max_age_s: left as u64,
        jti: claims.jti,
    }
}

/// The verify-only half of `/auth/session-token`: adopt a control-plane session
/// as this host's cookie, then get the token out of the URL.
///
/// The `303` is load-bearing twice over. It re-issues the navigation as a GET of
/// `next` alone, so the address bar, the history entry and every `Referer` the
/// next page emits carry no token: the query is stripped by going somewhere
/// else, not by asking the browser nicely. And it is a redirect the browser
/// performs itself, which is the only way the cookie this response sets is the
/// cookie the next request presents.
async fn establish_handoff(
    st: &St,
    headers: &HeaderMap,
    token: &str,
    next: Option<&str>,
) -> Response {
    let (next, max_age_s, jti) = match decide_handoff(&st.keys, token, next, now_secs()) {
        Handoff::Establish { next, max_age_s, jti } => (next, max_age_s, jti),
        Handoff::Refuse(reason) => {
            tracing::info!(target: "oauth", reason, "console handoff refused");
            return handoff_refused();
        }
    };
    // The handoff token's jti is the control-plane session's own, so one
    // deny-list row kills the fleet cookie and every console it opened. A
    // logged-out session must not be able to open a new one here.
    if st.keys.is_revoked(&st.db, &jti).await {
        tracing::info!(target: "oauth", reason = "revoked", "console handoff refused");
        return handoff_refused();
    }
    tracing::info!(target: "oauth", jti = %jti, ttl_s = max_age_s, "console handoff accepted");
    // The adopted token goes in a CELL-SCOPED cookie, never the fleet one.
    //
    // It names this cell in `aud` and every sibling enforces its own, so
    // writing it to the fleet cookie — which is what this used to do — REPLACED
    // the browser's `.queenmq.cloud` session with a credential only this host
    // accepts, and every sibling console answered 401 BadAudience until the
    // next portal login. Harmless on a one-cell fleet, wrong with two.
    // `auth::cell_cookie_name_of` carries the rest of the argument.
    let secure = cookie_is_secure(st, headers);
    let cookie = handoff_cookie(&st.cfg.cookie_name, token, secure, max_age_s);
    no_store(redirect(StatusCode::SEE_OTHER, &next, Some(&cookie)))
}

/// The `Set-Cookie` an accepted handoff emits. Pure, so the shape the handler
/// ships is the shape the tests pin — the attributes ARE the fix, and a test
/// that rebuilt them beside the handler would keep passing while the handler
/// drifted back to the fleet cookie.
fn handoff_cookie(fleet_name: &str, token: &str, secure: bool, max_age_s: u64) -> String {
    build_session_cookie(
        &crate::auth::cell_cookie_name_of(fleet_name, secure),
        token,
        // No `Domain`: host-only is what keeps the siblings' cookie intact, and
        // it is also what `__Host-` requires of the name above.
        None,
        secure,
        max_age_s,
    )
}

/// One answer for every rejected handoff: a 401 on the errors contract, and
/// never a redirect. A cell that bounced the browser back toward the portal on
/// a token it would refuse again would loop it between two hosts forever.
fn handoff_refused() -> Response {
    no_store(errors::err_401("this session token is not valid on this host"))
}

/// `Cache-Control: no-store` + `Referrer-Policy: no-referrer`, for the two
/// responses whose request URL has a credential in it. The 303's own `Location`
/// is the next page the browser loads, and without the referrer policy that
/// page, and every subresource it pulls, is handed the token in a header.
fn no_store(mut resp: Response) -> Response {
    let h = resp.headers_mut();
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    h.insert(header::REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    resp
}

// ---------------------------------------------------------------------------
// Google OAuth
// ---------------------------------------------------------------------------

async fn google_start(
    State(st): State<St>,
    headers: HeaderMap,
    Query(q): Query<StartQuery>,
) -> Response {
    // Both halves, checked HERE and not only in the callback: without the
    // secret this flow cannot complete, and sending the caller out to Google
    // first turns a configuration error into a mystery round trip.
    let Some(cid) = st.cfg.google_client_id.clone() else {
        return errors::err_404("not_configured", "google login not configured");
    };
    if st.cfg.google_client_secret.is_none() {
        return errors::err_404("not_configured", "google login not configured");
    }
    let redirect_uri = match redirect_uri(&st, &headers, "google") {
        Some(u) => u,
        None => return err_500("public base url required (auth-host mode)"),
    };
    let next = safe_next(q.next.as_deref());
    let nonce = random_hex(24);
    let state = sign_state(state_key(), &nonce, &next, STATE_TTL_S);
    let mut params: Vec<(&str, &str)> = vec![
        ("client_id", &cid),
        ("redirect_uri", &redirect_uri),
        ("response_type", "code"),
        ("scope", "openid email profile"),
        ("access_type", "online"),
        ("include_granted_scopes", "true"),
        ("prompt", "select_account"),
        ("state", &state),
        ("nonce", &nonce),
    ];
    // With exactly one allowed domain, hint Google's account chooser at it. This
    // is UX, NOT a control: `hd` on the authorize URL is a suggestion the user
    // can ignore, and the enforcement lives in validate_google_claims.
    if let [only] = st.cfg.google_allowed_domains.as_slice() {
        params.push(("hd", only));
    }
    let url = format!("{GOOGLE_AUTHORIZE_URL}?{}", qs(&params));
    redirect(StatusCode::FOUND, &url, None)
}

async fn google_callback(
    State(st): State<St>,
    headers: HeaderMap,
    Query(q): Query<CallbackQuery>,
) -> Response {
    if let Some(e) = q.error {
        return err_400("invalid_request", &format!("provider returned error: {e}"));
    }
    let Some(cid) = st.cfg.google_client_id.clone() else {
        return errors::err_404("not_configured", "google login not configured");
    };
    let Some(secret) = st.cfg.google_client_secret.clone() else {
        return errors::err_404("not_configured", "google login not configured");
    };
    let Some(state) = q.state else {
        return err_400("invalid_state", "missing state");
    };
    let Some((nonce, next)) = verify_state(state_key(), &state) else {
        return err_400("invalid_state", "state failed verification");
    };
    let Some(code) = q.code else {
        return err_400("invalid_request", "missing authorization code");
    };
    let redirect_uri = match redirect_uri(&st, &headers, "google") {
        Some(u) => u,
        None => return err_500("public base url required (auth-host mode)"),
    };

    let tokens = match httpget::post_form(
        GOOGLE_TOKEN_URL,
        &[
            ("code", &code),
            ("client_id", &cid),
            ("client_secret", &secret),
            ("redirect_uri", &redirect_uri),
            ("grant_type", "authorization_code"),
        ],
        HTTP_TIMEOUT,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(target: "oauth", err = %e, "google token exchange failed");
            return errors::err_502("google token exchange failed");
        }
    };
    let Some(id_token) = tokens.get("id_token").and_then(|v| v.as_str()) else {
        return errors::err_502("google token response missing id_token");
    };

    let claims = match verify_google_id_token(id_token, &cid).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "oauth", err = %e, "google id_token verify failed");
            return err_400("invalid_request", "google id_token verification failed");
        }
    };
    let gid = match validate_google_claims(&claims, &nonce, &st.cfg.google_allowed_domains) {
        Ok(g) => g,
        Err(e) => {
            if matches!(e, ClaimErr::DomainNotAllowed) {
                tracing::warn!(
                    target: "oauth",
                    hd = claims.hd.as_deref().unwrap_or(""),
                    "google login refused: domain not in GOOGLE_ALLOWED_DOMAINS"
                );
            }
            return claim_err_response(e);
        }
    };

    finish_oauth(&st, &headers, "google", &gid.sub, &gid.email, &next).await
}

/// Verify a Google id_token's signature (RS256 against Google's JWKS, selected
/// by the header `kid`) and standard claims (issuer, audience = our client id,
/// exp). Returns the decoded claims for `validate_google_claims`.
async fn verify_google_id_token(id_token: &str, client_id: &str) -> Result<GoogleClaims, String> {
    let header = decode_header(id_token).map_err(|e| format!("id_token header: {e}"))?;
    let kid = header.kid.ok_or_else(|| "id_token missing kid".to_string())?;
    let jwks = google_jwks().await?;
    let (n, e) = find_rsa_key(&jwks, &kid).ok_or_else(|| "no matching jwks key".to_string())?;
    let key = DecodingKey::from_rsa_components(&n, &e).map_err(|e| format!("jwks key: {e}"))?;
    let mut v = Validation::new(Algorithm::RS256);
    v.algorithms = vec![Algorithm::RS256];
    v.set_issuer(&GOOGLE_ISSUERS);
    v.set_audience(&[client_id]);
    v.validate_exp = true;
    let data = decode::<GoogleClaims>(id_token, &key, &v).map_err(|e| format!("id_token: {e}"))?;
    Ok(data.claims)
}

/// Google's JWKS, cached `JWKS_TTL`. On a refresh failure a still-present stale
/// copy is reused (key rotation is slow; a transient fetch error shouldn't down
/// all logins).
async fn google_jwks() -> Result<Value, String> {
    static CACHE: OnceLock<Mutex<Option<(Instant, Value)>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(None));
    if let Some((at, v)) = cache.lock().unwrap().as_ref() {
        if at.elapsed() < JWKS_TTL {
            return Ok(v.clone());
        }
    }
    match httpget::get_json(GOOGLE_JWKS_URL, HTTP_TIMEOUT).await {
        Ok(v) => {
            *cache.lock().unwrap() = Some((Instant::now(), v.clone()));
            Ok(v)
        }
        Err(e) => match cache.lock().unwrap().as_ref() {
            Some((_, v)) => {
                tracing::warn!(target: "oauth", err = %e, "google jwks refresh failed; using stale");
                Ok(v.clone())
            }
            None => Err(e),
        },
    }
}

fn find_rsa_key(jwks: &Value, kid: &str) -> Option<(String, String)> {
    for k in jwks.get("keys")?.as_array()? {
        if k.get("kid").and_then(|v| v.as_str()) == Some(kid)
            && k.get("kty").and_then(|v| v.as_str()) == Some("RSA")
        {
            let n = k.get("n")?.as_str()?.to_string();
            let e = k.get("e")?.as_str()?.to_string();
            return Some((n, e));
        }
    }
    None
}

#[derive(Deserialize)]
struct GoogleClaims {
    sub: Option<String>,
    email: Option<String>,
    email_verified: Option<Value>,
    nonce: Option<String>,
    /// Google Workspace hosted domain. Present only for Workspace accounts;
    /// absent for consumer (gmail.com) ones. Carried so the domain allowlist can
    /// be enforced — see `validate_google_claims`.
    hd: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
struct OidcIdentity {
    sub: String,
    email: String,
}

#[derive(Debug, PartialEq, Eq)]
enum ClaimErr {
    NonceMismatch,
    MissingSub,
    MissingEmail,
    EmailUnverified,
    DomainNotAllowed,
}

/// Pure OIDC claim check: nonce must match the signed-state nonce, sub + email
/// must be present, the email MUST be verified (`email_verified` accepted as
/// bool `true` or the string `"true"`, matching real Google payloads), and — when
/// `allowed_domains` is non-empty — the account must belong to one of those
/// domains.
///
/// The domain is taken from the `hd` claim OR from the email's own domain.
/// Either is Google-attested: `hd` is set for Workspace accounts, and a verified
/// email at a Workspace domain can only be issued by that Workspace. Accepting
/// both means a consumer account with a verified address at an allowed domain
/// still gets in, which is the behaviour operators expect from something spelled
/// "allowed domains".
///
/// An EMPTY list means no domain restriction. That is not the same as "open":
/// with auto-provision off, an identity that matches no existing user row is
/// rejected regardless. The list is what closes the door once auto-provision is
/// on.
fn validate_google_claims(
    claims: &GoogleClaims,
    expected_nonce: &str,
    allowed_domains: &[String],
) -> Result<OidcIdentity, ClaimErr> {
    match claims.nonce.as_deref() {
        Some(n) if n == expected_nonce => {}
        _ => return Err(ClaimErr::NonceMismatch),
    }
    let sub = claims.sub.as_deref().map(str::trim).filter(|s| !s.is_empty());
    let Some(sub) = sub else { return Err(ClaimErr::MissingSub) };
    let email = claims.email.as_deref().map(str::trim).filter(|s| !s.is_empty());
    let Some(email) = email else { return Err(ClaimErr::MissingEmail) };
    if !json_truthy(claims.email_verified.as_ref()) {
        return Err(ClaimErr::EmailUnverified);
    }
    let email = email.to_lowercase();
    if !allowed_domains.is_empty() {
        let hd = claims.hd.as_deref().unwrap_or("").trim().to_lowercase();
        let email_domain = email.rsplit_once('@').map(|(_, d)| d).unwrap_or("");
        let ok = allowed_domains.iter().any(|d| d == &hd)
            || allowed_domains.iter().any(|d| d == email_domain);
        if !ok {
            return Err(ClaimErr::DomainNotAllowed);
        }
    }
    Ok(OidcIdentity { sub: sub.to_string(), email })
}

fn claim_err_response(e: ClaimErr) -> Response {
    match e {
        ClaimErr::NonceMismatch => err_400("invalid_state", "id_token nonce mismatch"),
        ClaimErr::MissingSub => err_400("invalid_request", "id_token missing subject"),
        ClaimErr::MissingEmail => err_400("invalid_request", "id_token missing email"),
        ClaimErr::EmailUnverified => {
            errors::err_403("email_unverified", "provider email is not verified")
        }
        // Deliberately does not echo the rejected domain: the caller already
        // knows which account they used, and the reply is reachable by anyone.
        ClaimErr::DomainNotAllowed => {
            errors::err_403("domain_not_allowed", "google account domain is not allowed")
        }
    }
}

/// `true` for JSON `true` or the string `"true"`; everything else (incl. absent,
/// `false`, `"false"`) is false.
fn json_truthy(v: Option<&Value>) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s.eq_ignore_ascii_case("true"),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// GitHub OAuth (OAuth2 without OIDC)
// ---------------------------------------------------------------------------

async fn github_start(
    State(st): State<St>,
    headers: HeaderMap,
    Query(q): Query<StartQuery>,
) -> Response {
    // Both halves — see google_start.
    let Some(cid) = st.cfg.github_client_id.clone() else {
        return errors::err_404("not_configured", "github login not configured");
    };
    if st.cfg.github_client_secret.is_none() {
        return errors::err_404("not_configured", "github login not configured");
    }
    let redirect_uri = match redirect_uri(&st, &headers, "github") {
        Some(u) => u,
        None => return err_500("public base url required (auth-host mode)"),
    };
    let next = safe_next(q.next.as_deref());
    let nonce = random_hex(24);
    let state = sign_state(state_key(), &nonce, &next, STATE_TTL_S);
    let url = format!(
        "{GITHUB_AUTHORIZE_URL}?{}",
        qs(&[
            ("client_id", &cid),
            ("redirect_uri", &redirect_uri),
            ("scope", "user:email"),
            ("state", &state),
        ])
    );
    redirect(StatusCode::FOUND, &url, None)
}

async fn github_callback(
    State(st): State<St>,
    headers: HeaderMap,
    Query(q): Query<CallbackQuery>,
) -> Response {
    if let Some(e) = q.error {
        return err_400("invalid_request", &format!("provider returned error: {e}"));
    }
    let Some(cid) = st.cfg.github_client_id.clone() else {
        return errors::err_404("not_configured", "github login not configured");
    };
    let Some(secret) = st.cfg.github_client_secret.clone() else {
        return errors::err_404("not_configured", "github login not configured");
    };
    let Some(state) = q.state else {
        return err_400("invalid_state", "missing state");
    };
    let Some((_nonce, next)) = verify_state(state_key(), &state) else {
        return err_400("invalid_state", "state failed verification");
    };
    let Some(code) = q.code else {
        return err_400("invalid_request", "missing authorization code");
    };
    let redirect_uri = match redirect_uri(&st, &headers, "github") {
        Some(u) => u,
        None => return err_500("public base url required (auth-host mode)"),
    };

    let tokens = match httpget::post_form(
        GITHUB_TOKEN_URL,
        &[
            ("client_id", &cid),
            ("client_secret", &secret),
            ("code", &code),
            ("redirect_uri", &redirect_uri),
        ],
        HTTP_TIMEOUT,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(target: "oauth", err = %e, "github token exchange failed");
            return errors::err_502("github token exchange failed");
        }
    };
    let Some(access) = tokens.get("access_token").and_then(|v| v.as_str()) else {
        return errors::err_502("github token response missing access_token");
    };

    let auth = format!("Bearer {access}");
    let hdrs = [
        ("Authorization", auth.as_str()),
        ("User-Agent", "queen-proxy"),
        ("Accept", "application/vnd.github+json"),
    ];
    let user = match httpget::get_json_with_headers(GITHUB_USER_URL, &hdrs, HTTP_TIMEOUT).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(target: "oauth", err = %e, "github /user failed");
            return errors::err_502("github user lookup failed");
        }
    };
    let emails = match httpget::get_json_with_headers(GITHUB_EMAILS_URL, &hdrs, HTTP_TIMEOUT).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(target: "oauth", err = %e, "github /user/emails failed");
            return errors::err_502("github email lookup failed");
        }
    };

    let Some(email) = select_github_email(&emails) else {
        return errors::err_403("email_unverified", "github account has no verified primary email");
    };
    let Some(provider_id) = github_user_id(&user) else {
        return errors::err_502("github user response missing id");
    };

    finish_oauth(&st, &headers, "github", &provider_id, &email, &next).await
}

/// The verified primary email from GitHub's `/user/emails` array, lowercased.
/// None when there is no primary+verified entry (the only email we accept).
fn select_github_email(emails: &Value) -> Option<String> {
    for e in emails.as_array()? {
        let primary = e.get("primary").and_then(|v| v.as_bool()).unwrap_or(false);
        let verified = e.get("verified").and_then(|v| v.as_bool()).unwrap_or(false);
        if primary && verified {
            if let Some(addr) = e.get("email").and_then(|v| v.as_str()) {
                if !addr.trim().is_empty() {
                    return Some(addr.trim().to_lowercase());
                }
            }
        }
    }
    None
}

fn github_user_id(user: &Value) -> Option<String> {
    match user.get("id") {
        Some(Value::Number(n)) => Some(n.to_string()),
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// resolve / link / provision (shared by both providers)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
enum Resolution {
    Login(UserRef),
    Link(UserRef),
    Provision,
    Deny,
}

/// Pure resolution policy (mirrors proxy/src/google-auth.js `resolveUser`):
///   1. a linked identity → log in as that user;
///   2. else, for a *verified* email that matches an existing user → link;
///   3. else auto-provision only when enabled;
///   4. else deny.
fn decide_resolution(
    identity_hit: Option<UserRef>,
    email_hit: Option<UserRef>,
    email_verified: bool,
    autoprovision: bool,
) -> Resolution {
    if let Some(u) = identity_hit {
        return Resolution::Login(u);
    }
    if email_verified {
        if let Some(u) = email_hit {
            return Resolution::Link(u);
        }
    }
    if autoprovision {
        Resolution::Provision
    } else {
        Resolution::Deny
    }
}

enum ResolveErr {
    NoDb,
    NoTenant,
    NotProvisioned,
    Db(String),
}

async fn finish_oauth(
    st: &St,
    headers: &HeaderMap,
    provider: &str,
    provider_id: &str,
    email: &str,
    next: &str,
) -> Response {
    match resolve_oauth(st, provider, provider_id, email).await {
        Ok(user) => establish_session(st, headers, user, next).await,
        Err(ResolveErr::NotProvisioned) => {
            errors::err_403("not_provisioned", "no account for this identity (auto-provision disabled)")
        }
        Err(ResolveErr::NoTenant) => {
            err_500("auto-provision enabled but QUEEN_PROXY_AUTOPROVISION_TENANT is unset/unknown")
        }
        Err(ResolveErr::NoDb) => err_500("identity store unavailable"),
        Err(ResolveErr::Db(e)) => {
            tracing::error!(target: "oauth", err = %e, "identity store error");
            errors::err_502("identity store error")
        }
    }
}

async fn resolve_oauth(
    st: &St,
    provider: &str,
    provider_id: &str,
    email: &str,
) -> Result<UserRef, ResolveErr> {
    let Some(pool) = st.db.as_ref() else { return Err(ResolveErr::NoDb) };
    // Verified email is guaranteed by both provider paths (Google
    // email_verified, GitHub primary+verified); pass it through explicitly.
    let email_verified = true;
    let autoprovision = std::env::var("QUEEN_PROXY_AUTOPROVISION")
        .map(|v| v == "true")
        .unwrap_or(false);

    let identity_hit = find_by_identity(pool, provider, provider_id).await.map_err(ResolveErr::Db)?;
    let email_hit = if email_verified {
        find_user_by_email(pool, email).await.map_err(ResolveErr::Db)?
    } else {
        None
    };

    match decide_resolution(identity_hit, email_hit, email_verified, autoprovision) {
        Resolution::Login(u) => Ok(u),
        Resolution::Link(u) => {
            link_identity(pool, &u, provider, provider_id, email).await.map_err(ResolveErr::Db)?;
            record_op(
                st,
                u.tenant_id,
                u.user_id,
                "identity_linked",
                Some(u.user_id.to_string()),
                json!({ "provider": provider }),
            )
            .await;
            Ok(u)
        }
        Resolution::Provision => {
            let u =
                provision_user(pool, email, provider, provider_id, &st.cfg.autoprovision_default_role)
                    .await?;
            record_op(
                st,
                u.tenant_id,
                u.user_id,
                "signup",
                Some(u.user_id.to_string()),
                json!({ "provider": provider, "email": email }),
            )
            .await;
            Ok(u)
        }
        Resolution::Deny => Err(ResolveErr::NotProvisioned),
    }
}

async fn find_by_identity(
    pool: &deadpool_postgres::Pool,
    provider: &str,
    provider_id: &str,
) -> Result<Option<UserRef>, String> {
    let client = pool.get().await.map_err(|e| e.to_string())?;
    let row = client
        .query_opt(
            "SELECT u.id::text, u.tenant_id::text \
             FROM queen_proxy.identities i \
             JOIN queen_proxy.users u ON u.id = i.user_id \
             WHERE i.provider = $1 AND i.provider_id = $2",
            &[&provider, &provider_id],
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(row.as_ref().and_then(row_to_userref))
}

async fn find_user_by_email(
    pool: &deadpool_postgres::Pool,
    email: &str,
) -> Result<Option<UserRef>, String> {
    let client = pool.get().await.map_err(|e| e.to_string())?;
    let row = client
        .query_opt(
            "SELECT id::text, tenant_id::text FROM queen_proxy.users WHERE email = lower($1)",
            &[&email],
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(row.as_ref().and_then(row_to_userref))
}

async fn link_identity(
    pool: &deadpool_postgres::Pool,
    user: &UserRef,
    provider: &str,
    provider_id: &str,
    email: &str,
) -> Result<(), String> {
    let client = pool.get().await.map_err(|e| e.to_string())?;
    client
        .execute(
            "INSERT INTO queen_proxy.identities(user_id, provider, provider_id, email, verified) \
             VALUES ($1::text::uuid, $2, $3, lower($4), true) \
             ON CONFLICT (provider, provider_id) DO NOTHING",
            &[&user.user_id.to_string(), &provider, &provider_id, &email],
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Auto-provision a brand-new OAuth user: pick the tenant named by
/// `QUEEN_PROXY_AUTOPROVISION_TENANT` (slug), then INSERT users + identities in
/// one transaction. The auth host is the sanctioned direct writer of
/// users/identities (PLAN §2), so this bypasses the control-plane create_user
/// SQL function to keep the identity row atomic and record a single `signup` op.
async fn provision_user(
    pool: &deadpool_postgres::Pool,
    email: &str,
    provider: &str,
    provider_id: &str,
    default_role: &str,
) -> Result<UserRef, ResolveErr> {
    let tenant_slug = std::env::var("QUEEN_PROXY_AUTOPROVISION_TENANT")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let Some(tenant_slug) = tenant_slug else { return Err(ResolveErr::NoTenant) };

    let mut client = pool.get().await.map_err(|e| ResolveErr::Db(e.to_string()))?;
    let tx = client.transaction().await.map_err(|e| ResolveErr::Db(e.to_string()))?;

    let trow = tx
        .query_opt("SELECT id::text FROM queen_proxy.tenants WHERE slug = $1", &[&tenant_slug])
        .await
        .map_err(|e| ResolveErr::Db(e.to_string()))?;
    let Some(trow) = trow else { return Err(ResolveErr::NoTenant) };
    let tenant_id: Uuid = trow
        .get::<_, String>(0)
        .parse()
        .map_err(|_| ResolveErr::Db("tenant id parse".to_string()))?;

    let urow = tx
        .query_one(
            "INSERT INTO queen_proxy.users(tenant_id, email) \
             VALUES ($1::text::uuid, lower($2)) RETURNING id::text",
            &[&tenant_id.to_string(), &email],
        )
        .await
        .map_err(|e| ResolveErr::Db(e.to_string()))?;
    let user_id: Uuid = urow
        .get::<_, String>(0)
        .parse()
        .map_err(|_| ResolveErr::Db("user id parse".to_string()))?;

    tx.execute(
        "INSERT INTO queen_proxy.identities(user_id, provider, provider_id, email, verified) \
         VALUES ($1::text::uuid, $2, $3, lower($4), true)",
        &[&user_id.to_string(), &provider, &provider_id, &email],
    )
    .await
    .map_err(|e| ResolveErr::Db(e.to_string()))?;

    // Grant the default role on every cluster of the auto-provision tenant.
    // Without this the account exists, the login succeeds, /auth/me returns an
    // empty `clusters` array and every call 403s — which looks like a broken
    // deploy rather than a permissions decision. In the same transaction as the
    // user and identity rows on purpose: a half-provisioned human is exactly
    // the state that is confusing to diagnose.
    //
    // "Every cluster of the tenant" is the only rule that needs no extra
    // configuration and is correct on a single-cluster cell, which is what a
    // self-hosted proxy is. `role` is validated at boot (config::CLUSTER_ROLES),
    // so it cannot violate the CHECK here.
    let granted = tx
        .execute(
            "INSERT INTO queen_proxy.cluster_roles(user_id, cluster_id, role) \
             SELECT $1::text::uuid, c.id, $2 \
               FROM queen_proxy.clusters c \
              WHERE c.tenant_id = $3::text::uuid \
             ON CONFLICT (user_id, cluster_id) DO NOTHING",
            &[&user_id.to_string(), &default_role, &tenant_id.to_string()],
        )
        .await
        .map_err(|e| ResolveErr::Db(e.to_string()))?;

    tx.commit().await.map_err(|e| ResolveErr::Db(e.to_string()))?;

    if granted == 0 {
        // Not an error: the tenant genuinely has no clusters yet. Say so, because
        // the symptom the user reports ("I can log in but everything is empty")
        // is identical to the bug this grant exists to prevent.
        tracing::warn!(
            target: "oauth",
            tenant = %tenant_slug,
            email = %email,
            "auto-provisioned a user but the tenant has no clusters to grant on"
        );
    } else {
        tracing::info!(
            target: "oauth",
            email = %email,
            role = %default_role,
            clusters = granted,
            "auto-provisioned user granted default role"
        );
    }
    Ok(UserRef { user_id, tenant_id })
}

/// Append a `queen_proxy.operations` audit row (actor = user). Best-effort:
/// no DB, an unavailable pool, or a failed call is skipped, never fatal to the
/// login it records.
async fn record_op(
    st: &St,
    tenant_id: Uuid,
    actor_id: Uuid,
    action: &str,
    target: Option<String>,
    meta: Value,
) {
    let Some(pool) = st.db.as_ref() else { return };
    let Ok(client) = pool.get().await else { return };
    let meta_s = meta.to_string();
    if let Err(e) = client
        .execute(
            // `$5::text::jsonb` (not `$5::jsonb`): the inner ::text pins the bind
            // param to text so a Rust String serializes, exactly as the ::text::uuid
            // casts do for the UUID params (tokio-postgres has no jsonb/uuid feature).
            "SELECT queen_proxy.record_operation($1::text::uuid, NULL, 'user', $2::text::uuid, $3, $4, $5::text::jsonb)",
            &[&tenant_id.to_string(), &actor_id.to_string(), &action, &target, &meta_s],
        )
        .await
    {
        tracing::warn!(target: "oauth", action, err = %e, "record_operation failed (non-fatal)");
    }
}

fn row_to_userref(row: &tokio_postgres::Row) -> Option<UserRef> {
    let user_id: Uuid = row.get::<_, String>(0).parse().ok()?;
    let tenant_id: Uuid = row.get::<_, String>(1).parse().ok()?;
    Some(UserRef { user_id, tenant_id })
}

// ---------------------------------------------------------------------------
// signed state (anti-CSRF), next validation, redirect_uri
// ---------------------------------------------------------------------------

/// Process-wide HMAC key for signing OAuth `state`. Prefers a dedicated secret,
/// falls back to the JWT HS secret, else a random per-process key (fine for a
/// single instance; set the env for a load-balanced auth host).
fn state_key() -> &'static hmac::Key {
    static KEY: OnceLock<hmac::Key> = OnceLock::new();
    KEY.get_or_init(|| {
        let secret = std::env::var("QUEEN_PROXY_OAUTH_STATE_SECRET")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| std::env::var("QUEEN_PROXY_JWT_SECRET").ok().filter(|s| !s.is_empty()))
            .map(String::into_bytes)
            .unwrap_or_else(|| {
                let mut b = [0u8; 32];
                OsRng.fill_bytes(&mut b);
                b.to_vec()
            });
        hmac::Key::new(hmac::HMAC_SHA256, &secret)
    })
}

/// Compact signed token `base64url(payload).base64url(hmac)` carrying the OAuth
/// nonce + validated `next` + expiry. Not a JWT — no header, single fixed alg.
fn sign_state(key: &hmac::Key, nonce: &str, next: &str, ttl_s: i64) -> String {
    let payload = json!({ "n": nonce, "x": next, "e": now_secs() + ttl_s }).to_string();
    let p = B64_URL.encode(payload.as_bytes());
    let tag = hmac::sign(key, p.as_bytes());
    let s = B64_URL.encode(tag.as_ref());
    format!("{p}.{s}")
}

/// Verify a state token: constant-time HMAC check, then expiry. Returns
/// `(nonce, next)`; `next` is re-validated for open-redirect safety (defense in
/// depth). Any tamper, bad base64, wrong signature, or expiry → None.
fn verify_state(key: &hmac::Key, token: &str) -> Option<(String, String)> {
    let (p, s) = token.split_once('.')?;
    let sig = B64_URL.decode(s).ok()?;
    hmac::verify(key, p.as_bytes(), &sig).ok()?;
    let raw = B64_URL.decode(p).ok()?;
    let v: Value = serde_json::from_slice(&raw).ok()?;
    if now_secs() > v.get("e")?.as_i64()? {
        return None;
    }
    let nonce = v.get("n")?.as_str()?.to_string();
    let next_raw = v.get("x")?.as_str()?.to_string();
    let next = if is_safe(&next_raw) { next_raw } else { "/".to_string() };
    Some((nonce, next))
}

/// Anti open-redirect: accept only same-origin relative paths. Rejects `//host`
/// (protocol-relative), `/\host` (some browsers normalize `\`→`/`), any
/// scheme (`://`), and control chars. Anything else falls back to `/`.
fn safe_next(next: Option<&str>) -> String {
    match next {
        Some(n) if is_safe(n) => n.to_string(),
        _ => "/".to_string(),
    }
}

/// `pub(crate)` for webapp.rs, which validates its own `next` before building
/// the login redirect — same rule, one implementation.
pub(crate) fn is_safe(n: &str) -> bool {
    n.starts_with('/')
        && !n.starts_with("//")
        && !n.starts_with("/\\")
        && !n.contains("://")
        && !n.bytes().any(|b| b < 0x20)
}

/// The OAuth callback base for `provider`. In auth-host mode the public base URL
/// is mandatory (providers forbid wildcard callbacks → one pinned host, PLAN
/// §9); otherwise prefer the configured public URL, else derive from the request
/// (X-Forwarded-Proto + Host).
fn redirect_uri(st: &St, headers: &HeaderMap, provider: &str) -> Option<String> {
    let base = if let Some(u) = &st.cfg.public_base_url {
        u.trim_end_matches('/').to_string()
    } else if st.cfg.auth_host_mode {
        return None;
    } else {
        let proto = header_str(headers, "x-forwarded-proto").unwrap_or("http");
        let host = header_str(headers, "host")?;
        format!("{proto}://{host}")
    };
    Some(format!("{base}/auth/{provider}/callback"))
}

// ---------------------------------------------------------------------------
// cookies, client ip, throttle
// ---------------------------------------------------------------------------

/// Secure flag: set when the edge terminated TLS (X-Forwarded-Proto=https) or a
/// cookie Domain is configured (cloud is always HTTPS behind Cloudflare).
///
/// `pub(crate)` because it also picks which of the two cell-cookie names a
/// request is read under (`auth::cell_cookie_name_of`): the name a session is
/// SET under and the name it is READ under must be derived the same way, or a
/// handoff would set a cookie nothing ever looks for.
pub(crate) fn cookie_is_secure(st: &St, headers: &HeaderMap) -> bool {
    st.cfg.cookie_domain.is_some()
        || header_str(headers, "x-forwarded-proto").map(|v| v.eq_ignore_ascii_case("https")).unwrap_or(false)
}

/// The one place a session cookie's attributes are spelled out. Pure, so the
/// handoff's shorter-lived cookie is the same cookie with a different Max-Age
/// rather than a second format string that can drift from this one.
fn build_session_cookie(
    name: &str,
    value: &str,
    domain: Option<&str>,
    secure: bool,
    max_age_s: u64,
) -> String {
    let mut c = format!("{name}={value}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age_s}");
    if let Some(dom) = domain {
        c.push_str(&format!("; Domain={dom}"));
    }
    if secure {
        c.push_str("; Secure");
    }
    c
}

fn session_cookie(st: &St, headers: &HeaderMap, token: &str) -> String {
    build_session_cookie(
        &st.cfg.cookie_name,
        token,
        st.cfg.cookie_domain.as_deref(),
        cookie_is_secure(st, headers),
        st.cfg.jwt_ttl_s,
    )
}

/// BOTH `Set-Cookie` lines a logout emits: the fleet cookie, and the
/// cell-scoped one this host may have set when it adopted a console handoff.
///
/// Together, deliberately. The cell cookie is the credential the reader
/// PREFERS on this host (`auth::read_session_cookie`), so a logout that cleared
/// only the fleet cookie would leave the console it established still open in
/// the same browser — the visible half of the session gone and the load-bearing
/// half untouched.
fn clear_cookies(st: &St, headers: &HeaderMap) -> [String; 2] {
    clear_cookie_pair(
        &st.cfg.cookie_name,
        st.cfg.cookie_domain.as_deref(),
        cookie_is_secure(st, headers),
    )
}

/// The pair on its own, for the same reason `handoff_cookie` is pure.
fn clear_cookie_pair(fleet_name: &str, fleet_domain: Option<&str>, secure: bool) -> [String; 2] {
    [
        build_session_cookie(fleet_name, "", fleet_domain, secure, 0),
        handoff_cookie(fleet_name, "", secure, 0),
    ]
}

/// The session token THIS BROWSER presents to THIS host, read under the one
/// precedence every surface shares (`auth::read_session_cookie`): the
/// cell-scoped cookie a console handoff established here, else the fleet
/// cookie. Endpoints that describe or end the browser's session — `/auth/me`,
/// `/auth/session-token`, logout's revoke — must all see the same one it
/// authenticates with, or a console opened by handoff would be a session
/// `/auth/me` reports as absent and logout leaves running.
fn presented_session(st: &St, headers: &HeaderMap) -> Option<String> {
    crate::auth::read_session_cookie(&st.cfg.cookie_name, cookie_is_secure(st, headers), headers)
}

/// Client IP for the login throttle: leftmost X-Forwarded-For, else X-Real-IP,
/// else a shared "unknown" bucket (no ConnectInfo is wired in main.rs; the proxy
/// sits behind Cloudflare so XFF is authoritative there).
fn client_ip(headers: &HeaderMap) -> String {
    if let Some(xff) = header_str(headers, "x-forwarded-for") {
        if let Some(first) = xff.split(',').next() {
            let ip = first.trim();
            if !ip.is_empty() {
                return ip.to_string();
            }
        }
    }
    if let Some(xr) = header_str(headers, "x-real-ip") {
        if !xr.trim().is_empty() {
            return xr.trim().to_string();
        }
    }
    "unknown".to_string()
}

struct AttemptBucket {
    count: u32,
    window_start: Instant,
}

/// Per-IP fixed-window login throttle. Ok when under cap; Err(retry_after_s)
/// once LOGIN_MAX is reached inside LOGIN_WINDOW. Process-local (one proxy per
/// cell, PLAN §2) — no shared store needed.
fn throttle(ip: &str) -> Result<(), u64> {
    static BUCKETS: OnceLock<Mutex<HashMap<String, AttemptBucket>>> = OnceLock::new();
    let buckets = BUCKETS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = buckets.lock().unwrap();
    let now = Instant::now();
    if map.len() > 100_000 {
        map.retain(|_, b| now.duration_since(b.window_start) < LOGIN_WINDOW);
    }
    let b = map.entry(ip.to_string()).or_insert(AttemptBucket { count: 0, window_start: now });
    if now.duration_since(b.window_start) >= LOGIN_WINDOW {
        b.count = 0;
        b.window_start = now;
    }
    if b.count >= LOGIN_MAX {
        let elapsed = now.duration_since(b.window_start).as_secs();
        return Err(LOGIN_WINDOW.as_secs().saturating_sub(elapsed).max(1));
    }
    b.count += 1;
    Ok(())
}

// ---------------------------------------------------------------------------
// small response / encoding helpers
// ---------------------------------------------------------------------------

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

fn role_str(r: Role) -> &'static str {
    match r {
        Role::Admin => "admin",
        Role::Producer => "producer",
        Role::Consumer => "consumer",
        Role::Viewer => "viewer",
    }
}

fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

fn random_hex(n: usize) -> String {
    let mut b = vec![0u8; n];
    OsRng.fill_bytes(&mut b);
    hex::encode(b)
}

fn html(status: StatusCode, body: String) -> Response {
    (status, Html(body)).into_response()
}

fn json_response(status: StatusCode, value: Value) -> Response {
    let mut resp = (status, value.to_string()).into_response();
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp
}

fn err_400(code: &str, msg: &str) -> Response {
    errors::json_error(StatusCode::BAD_REQUEST, code, msg)
}

fn err_500(msg: &str) -> Response {
    errors::json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal", msg)
}

/// A login that cannot be served because this proxy holds no signer.
///
/// 503 + a message that names the variables, not a bare 500 "internal": a
/// proxy with a pxdb and no JWT material boots on purpose (config::jwt_boot —
/// it is the API-key-only shape), so this is a CONFIGURATION answer, not a
/// crash, and the operator reading it is the one who can fix it. The boot
/// warning says the same thing; this is what anyone who never reads the boot
/// log gets instead.
fn err_no_signer() -> Response {
    errors::json_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "not_configured",
        "this proxy has no JWT signer, so it cannot issue sessions: set QUEEN_PROXY_JWT_SECRET \
         (HS256) or QUEEN_PROXY_JWT_ED25519_PEM (Ed25519) and restart it",
    )
}

fn redirect(status: StatusCode, location: &str, set_cookie: Option<&str>) -> Response {
    let mut resp = status.into_response();
    let h = resp.headers_mut();
    if let Ok(v) = HeaderValue::from_str(location) {
        h.insert(header::LOCATION, v);
    }
    if let Some(c) = set_cookie {
        if let Ok(v) = HeaderValue::from_str(c) {
            h.insert(header::SET_COOKIE, v);
        }
    }
    resp
}

/// Percent-encode one query/form component (unreserved chars pass through).
/// `pub(crate)` for webapp.rs's login redirect.
pub(crate) fn pct(s: &str) -> String {
    let mut out = String::new();
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn qs(params: &[(&str, &str)]) -> String {
    let mut out = String::new();
    for (i, (k, v)) in params.iter().enumerate() {
        if i > 0 {
            out.push('&');
        }
        out.push_str(&pct(k));
        out.push('=');
        out.push_str(&pct(v));
    }
    out
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

/// Which OAuth providers this instance can actually complete a login with, in
/// the order the sign-in page lists them.
///
/// BOTH halves of the credential are required, not just the client id. The
/// callback needs the secret to exchange the code (`google_callback` /
/// `github_callback` 404 `not_configured` without it), so an instance with an
/// id and no secret would otherwise render a button that sends the user all
/// the way out to the provider and 404s them on the way back. A provider we
/// cannot finish is a provider we do not offer.
///
/// A provider missing here is a provider this deployment did not configure —
/// `GOOGLE_CLIENT_ID` + `GOOGLE_CLIENT_SECRET`, `GITHUB_CLIENT_ID` +
/// `GITHUB_CLIENT_SECRET` (proxy/src/config.rs). Nothing is hidden by
/// accident: with neither set, the page is local login only, which is what a
/// dev cell looks like.
fn configured_providers(st: &St) -> Vec<(&'static str, &'static str)> {
    provider_list(
        st.cfg.google_client_id.is_some(),
        st.cfg.google_client_secret.is_some(),
        st.cfg.github_client_id.is_some(),
        st.cfg.github_client_secret.is_some(),
    )
}

/// The rule on its own, so it can be tested without building a `Config` and a
/// connection pool.
fn provider_list(
    google_id: bool,
    google_secret: bool,
    github_id: bool,
    github_secret: bool,
) -> Vec<(&'static str, &'static str)> {
    let mut out = Vec::new();
    if google_id && google_secret {
        out.push(("google", "Google"));
    }
    if github_id && github_secret {
        out.push(("github", "GitHub"));
    }
    out
}

/// What the sign-in panel can honestly offer on this host.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SignIn {
    /// This proxy issues its own sessions: the email/password form, plus any
    /// provider button it can actually complete.
    Here,
    /// Verify-only host. It accepts sessions the auth host minted and issues
    /// none, so the page explains where sign-in happens instead.
    Elsewhere,
}

/// Which of the two surfaces this host can serve, decided from what it can DO
/// with a session token rather than from which knob is set (the same rule
/// `config::jwt_boot` classifies by).
///
/// VERIFY-ONLY IS EXACTLY `can_verify && !can_mint`: a public key and no signer,
/// `config::JwtMode::VerifyOnly`. A verify-only host that cannot verify never
/// reaches this page at all, because the boot gate refuses to start it.
///
/// Measured on a Queen Cloud cell (2026-08-28): the page rendered its form, and
/// every submission it invited reached `establish_session`, which answers 503
/// "no JWT signer" because minting is precisely what this host is deployed not
/// to do. The OAuth buttons are the same dead end one redirect later, since both
/// callbacks end in `establish_session` too, so the whole panel is withheld
/// rather than the form alone.
///
/// The OTHER host that cannot mint, the one with no JWT material at all, KEEPS
/// THE FORM. It is the API-key-only proxy `config::jwt_boot` warns about instead
/// of refusing: nothing mints its sessions anywhere, so there is no control
/// plane to send anyone to, and the 503 its form produces names the two
/// variables to set (`err_no_signer`), which is the answer its operator needs.
///
/// A data-plane proxy holding the same public key with NO pxdb behind it
/// (`JwtMode::NoIdentity`, which is a classification of the user table rather
/// than of the key material) lands on the notice too, and that is right: it
/// verifies the auth host's sessions exactly like a cell does, and a form there
/// could not resolve a user even if a signer existed.
fn sign_in_surface(can_mint: bool, can_verify: bool) -> SignIn {
    if can_verify && !can_mint {
        SignIn::Elsewhere
    } else {
        SignIn::Here
    }
}

/// Minimal self-contained sign-in page, in whichever of its two forms this host
/// can serve: the email/password form with the provider buttons it can complete
/// (`next` carried through as a hidden field and on those links), or, on a
/// verify-only host, the notice that says where sign-in happens instead. Sober
/// inline CSS either way.
fn render_login(st: &St, next: &str, error: Option<&str>) -> String {
    login_html(
        sign_in_surface(st.keys.can_mint(), st.keys.can_verify()),
        &configured_providers(st),
        st.cfg.auth_portal_url.as_deref().map(|u| (u, st.cfg.auth_portal_label.as_str())),
        next,
        error,
    )
}

/// The page itself, with every input already resolved: rendered from plain
/// values so both surfaces can be exercised without building a `Config` and a
/// connection pool, the way `provider_list` is.
///
/// `portal` is the operator's sign-in URL and its link text, and only the
/// `Elsewhere` panel has anywhere to put them.
fn login_html(
    sign_in: SignIn,
    providers: &[(&str, &str)],
    portal: Option<(&str, &str)>,
    next: &str,
    error: Option<&str>,
) -> String {
    let next_e = esc(next);
    let next_q = pct(next);
    let err_html = match error {
        Some(e) => format!("<div class=\"err\">{}</div>", esc(e)),
        None => String::new(),
    };
    let panel = match sign_in {
        SignIn::Here => {
            let mut buttons = String::new();
            for (slug, label) in providers {
                buttons.push_str(&format!(
                    "<a class=\"oauth\" href=\"/auth/{slug}?next={next_q}\">Continue with {label}</a>"
                ));
            }
            let divider = if buttons.is_empty() {
                String::new()
            } else {
                format!("{buttons}<div class=\"sep\">or</div>")
            };
            format!(
                "{divider}<form method=\"post\" action=\"/auth/login\">\
<input type=\"hidden\" name=\"next\" value=\"{next_e}\">\
<label for=\"email\">Email</label>\
<input id=\"email\" name=\"email\" type=\"email\" autocomplete=\"username\" required autofocus>\
<label for=\"password\">Password</label>\
<input id=\"password\" name=\"password\" type=\"password\" autocomplete=\"current-password\" required>\
<button type=\"submit\">Sign in</button></form>"
            )
        }
        // No `next` on the link: it names a path on THIS host, and the portal is
        // a different service that has no use for it.
        SignIn::Elsewhere => {
            let link = match portal {
                Some((url, label)) => {
                    format!("<a class=\"oauth\" href=\"{}\">{}</a>", esc(url), esc(label))
                }
                None => String::new(),
            };
            format!(
                "<p class=\"note\">This host verifies sessions but does not issue them. \
Sign in through the control plane that operates it, then return here.</p>{link}"
            )
        }
    };
    let favicon = favicon_data_uri();
    let badge = brand_badge_data_uri();
    let mark = if badge.is_empty() {
        String::new()
    } else {
        format!("<img class=\"mark\" src=\"{badge}\" alt=\"\">")
    };
    // The sign-in page is plain server-rendered HTML: it exists BEFORE the SPA
    // (and, under mandatory auth, before any of its assets are reachable), so it
    // cannot import the webapp's stylesheet. It therefore restates the app's
    // design tokens inline — surfaces ink-0/2/3/4, borders bd/bd-hi, text
    // hi/low/faint, the #dedede primary, ember for errors, the 6/8px radius
    // ladder and the 2px focus ring — and the brand block copies .brand /
    // .brand-mark / .brand-word from the sidebar so the two read as one product.
    //
    // THESE VALUES ARE A COPY BY NECESSITY, NOT BY CHOICE. They must be kept in
    // step with the `:root` block in app/src/style.css by hand; there is no
    // build step that checks it. The current set is the docs-derived palette
    // (page #020202, card #0f0f0f, input #171717, border #262626/#404040).
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
<title>Sign in · Queen</title>\
<link rel=\"icon\" type=\"image/svg+xml\" href=\"{favicon}\">\
<link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">\
<link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>\
<link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap\" rel=\"stylesheet\">\
<style>\
:root{{color-scheme:dark}}\
*{{box-sizing:border-box}}\
body{{font:15px/1.5 'Inter',ui-sans-serif,system-ui,-apple-system,sans-serif;letter-spacing:-.005em;\
margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;\
background:#020202;color:#f5f5f5;padding:24px}}\
.card{{width:100%;max-width:340px}}\
.brand{{display:flex;align-items:center;gap:9px;margin-bottom:20px}}\
.mark{{width:28px;height:28px;flex:none;object-fit:contain}}\
.word{{font-size:15px;font-weight:600;line-height:1;color:#f5f5f5;letter-spacing:-.015em}}\
.word b{{font-weight:500;font-size:11px;letter-spacing:.06em;margin-left:3px;color:#9e9e9e}}\
.panel{{background:#0f0f0f;border:1px solid #262626;border-radius:8px;padding:22px 20px}}\
h1{{font-size:14px;margin:0 0 16px;font-weight:600;letter-spacing:-.01em;color:#f5f5f5}}\
label{{display:block;font-size:11px;font-weight:500;letter-spacing:.04em;text-transform:uppercase;\
color:#808080;margin:14px 0 5px}}\
input{{width:100%;padding:9px 10px;border:1px solid #262626;border-radius:6px;font-size:14px;\
font-family:inherit;background:#171717;color:#f5f5f5}}\
input:hover{{border-color:#404040}}\
input:focus{{outline:2px solid rgba(222,222,222,.35);outline-offset:2px;border-color:#404040;background:#1e1e1e}}\
button{{width:100%;margin-top:18px;padding:9px;border:0;border-radius:6px;background:#dedede;color:#020202;\
font-family:inherit;font-size:14px;font-weight:600;letter-spacing:-.005em;cursor:pointer}}\
button:hover{{background:#9e9e9e}}\
.oauth{{display:block;text-align:center;margin-bottom:8px;padding:9px;border:1px solid #262626;\
border-radius:6px;text-decoration:none;color:#f5f5f5;background:#0f0f0f;font-size:14px;font-weight:500}}\
.oauth:hover{{background:#171717;border-color:#404040}}\
.sep{{display:flex;align-items:center;gap:10px;color:#525252;font-size:10px;margin:14px 0 2px;\
text-transform:uppercase;letter-spacing:.08em}}\
.sep:before,.sep:after{{content:'';flex:1;height:1px;background:#262626}}\
.err{{background:rgba(244,63,94,.12);border:1px solid rgba(244,63,94,.28);color:#fb7185;\
padding:8px 10px;border-radius:6px;font-size:13px;margin-bottom:14px}}\
.note{{margin:0;color:#9e9e9e;font-size:13px;line-height:1.6}}\
.note+.oauth{{margin:16px 0 0}}\
</style></head><body><div class=\"card\">\
<div class=\"brand\">{mark}<span class=\"word\">Queen<b>MQ</b></span></div>\
<div class=\"panel\"><h1>Sign in</h1>{err_html}{panel}</div></div></body></html>"
    )
}

/// The brand mark as a `data:` URI — the SAME built asset the sidebar shows
/// (`app/public/queen-mark.svg`, the geometry with its fill hard-set to white
/// because this page is dark), pulled out of the embedded webapp so the
/// sign-in page and the app it fronts can never drift apart, and so
/// regenerating the brand (assets/generate-brand.py + `npm run build`) updates
/// this page too.
///
/// It is inlined rather than linked because webapp.rs's gate is deliberately
/// absolute — not one byte without a live session — so a plain
/// `<img src="/queen-mark.svg">` here would 302 back to this very page.
/// Encoded once: the bytes are fixed for the life of the process.
///
/// Empty when the webapp is not built. The page then renders without a mark
/// rather than with a broken one; `the_login_brand_is_embedded` fails the build
/// long before that reaches anyone.
fn brand_badge_data_uri() -> &'static str {
    static URI: OnceLock<String> = OnceLock::new();
    URI.get_or_init(|| match crate::webapp::embedded_asset(BRAND_BADGE) {
        Some(bytes) => format!("data:image/svg+xml;base64,{}", B64.encode(bytes)),
        None => String::new(),
    })
}

/// The tab icon: the webapp's own favicon — the same geometry again, but the
/// theme-adaptive cut, which carries a `prefers-color-scheme` rule so it reads
/// on a light or a dark tab strip without a second file.
///
/// Both marks are now pure vector paths of a few hundred bytes, so carrying the
/// icon and the badge separately costs nothing worth optimising away.
fn favicon_data_uri() -> &'static str {
    static URI: OnceLock<String> = OnceLock::new();
    URI.get_or_init(|| match crate::webapp::embedded_asset(BRAND_FAVICON) {
        Some(bytes) => format!("data:image/svg+xml;base64,{}", B64.encode(bytes)),
        None => String::new(),
    })
}

/// Brand art inside the built webapp (`app/public/` is copied to the Vite
/// output root): the dark-surface mark the sidebar shows, and the tab icon.
const BRAND_BADGE: &str = "queen-mark.svg";
const BRAND_FAVICON: &str = "favicon.svg";

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> hmac::Key {
        hmac::Key::new(hmac::HMAC_SHA256, b"unit-test-state-secret")
    }

    fn uref() -> UserRef {
        UserRef { user_id: Uuid::new_v4(), tenant_id: Uuid::new_v4() }
    }

    // --- signed state -------------------------------------------------------

    #[test]
    fn state_roundtrip_ok() {
        let k = test_key();
        let tok = sign_state(&k, "nonce123", "/dashboard", STATE_TTL_S);
        let (n, x) = verify_state(&k, &tok).expect("valid state verifies");
        assert_eq!(n, "nonce123");
        assert_eq!(x, "/dashboard");
    }

    #[test]
    fn state_tamper_rejected() {
        let k = test_key();
        let tok = sign_state(&k, "nonce123", "/dashboard", STATE_TTL_S);
        // flip one char of the payload segment -> signature no longer matches
        let (p, s) = tok.split_once('.').unwrap();
        let mut pv: Vec<char> = p.chars().collect();
        pv[0] = if pv[0] == 'A' { 'B' } else { 'A' };
        let tampered = format!("{}.{}", pv.into_iter().collect::<String>(), s);
        assert!(verify_state(&k, &tampered).is_none(), "tampered payload must be rejected");
        // wrong key also rejected
        let other = hmac::Key::new(hmac::HMAC_SHA256, b"a-different-secret");
        assert!(verify_state(&other, &tok).is_none(), "foreign signature must be rejected");
    }

    #[test]
    fn state_expiry_rejected() {
        let k = test_key();
        let tok = sign_state(&k, "n", "/", -10); // already expired
        assert!(verify_state(&k, &tok).is_none());
    }

    #[test]
    fn state_next_sanitized_on_verify() {
        // A signed-but-unsafe next (e.g. if it ever slipped in) is neutralized
        // to "/" at verify time — defense in depth.
        let k = test_key();
        let tok = sign_state(&k, "n", "//evil.com", STATE_TTL_S);
        let (_, x) = verify_state(&k, &tok).unwrap();
        assert_eq!(x, "/");
    }

    // --- open-redirect ------------------------------------------------------

    #[test]
    fn next_open_redirect_rules() {
        assert!(is_safe("/ok"));
        assert!(is_safe("/a/b?c=d"));
        assert!(!is_safe("//evil.com"));
        assert!(!is_safe("/\\evil.com"));
        assert!(!is_safe("https://evil.com"));
        assert!(!is_safe("http://evil.com"));
        assert!(!is_safe("javascript:alert(1)"));
        assert!(!is_safe("/bad\nline"));
        assert!(!is_safe("relative"));
        // safe_next falls back to "/" for the rejected ones and None
        assert_eq!(safe_next(Some("//evil.com")), "/");
        assert_eq!(safe_next(Some("/ok")), "/ok");
        assert_eq!(safe_next(None), "/");
    }

    // --- dashboard handoff --------------------------------------------------

    /// The auth host that mints and the verify-only cell that trusts it. Real
    /// Ed25519 on both sides: the point of this path is that a host with no
    /// signer can still check a signature, and a stub would test the branch
    /// while leaving the claim untested.
    fn handoff_pair() -> (crate::auth::Keys, crate::auth::Keys) {
        crate::auth::testkit::ed_handoff_pair("queen-proxy", None)
    }

    fn establish_of(h: Handoff) -> (String, u64) {
        match h {
            Handoff::Establish { next, max_age_s, .. } => (next, max_age_s),
            Handoff::Refuse(r) => panic!("expected an establish, got Refuse({r})"),
        }
    }

    #[test]
    fn verify_only_host_establishes_a_valid_handoff_and_sets_the_cookie() {
        let (auth_host, cell) = handoff_pair();
        assert!(auth_host.can_mint(), "the auth host mints");
        assert!(!cell.can_mint(), "the cell holds a public key and no signer");
        assert!(cell.can_verify(), "and it can still verify with it");

        let now = now_secs();
        let token =
            crate::auth::testkit::mint_at(&auth_host, Uuid::new_v4(), now + 3600, None).unwrap();

        let (next, max_age_s) =
            establish_of(decide_handoff(&cell, &token, Some("/console"), now));
        assert_eq!(next, "/console");
        // The cookie lives as long as the TOKEN, not as long as this host's
        // jwt_ttl_s: it cannot mint a replacement when the token dies.
        assert_eq!(max_age_s, 3600);

        // The cookie establish_handoff ships: the CELL-scoped name, host-only.
        let cookie = handoff_cookie("queen_session", &token, true, max_age_s);
        assert!(
            cookie.starts_with(&format!("__Host-queen_session_cell={token};")),
            "cookie is the token, under the cell name: {cookie}"
        );
        for attr in ["Path=/", "HttpOnly", "SameSite=Lax", "Max-Age=3600", "Secure"] {
            assert!(cookie.contains(attr), "missing {attr} in {cookie}");
        }
    }

    /// ⚠ The regression this cookie's shape exists for.
    ///
    /// The handoff token names ONE cell in `aud`, and every sibling enforces
    /// its own (`auth::audience_ok`). This host used to adopt it under the
    /// FLEET cookie's name and the fleet's `Domain`, which is one jar key: the
    /// browser's `.queenmq.cloud` cookie was REPLACED by a credential only this
    /// cell accepts, and every sibling console answered 401 BadAudience until
    /// the user logged in at the portal again. Invisible on a one-cell fleet,
    /// which is why it shipped.
    ///
    /// Two properties keep it fixed, and neither is sufficient alone: no
    /// `Domain` (so the fleet cookie is untouched everywhere else), and a name
    /// of its own (so the two do not collide in the jar HERE — see
    /// `auth::cell_cookie_name_of`).
    #[test]
    fn an_established_handoff_leaves_the_fleet_cookie_alone() {
        let (auth_host, cell) = handoff_pair();
        let now = now_secs();
        let token =
            crate::auth::testkit::mint_at(&auth_host, Uuid::new_v4(), now + 3600, None).unwrap();
        let (_, max_age_s) = establish_of(decide_handoff(&cell, &token, None, now));

        let cookie = handoff_cookie("queen_session", &token, true, max_age_s);
        assert!(
            !cookie.contains("Domain="),
            "host-only, or the siblings lose their session: {cookie}"
        );
        assert!(
            !cookie.starts_with("queen_session="),
            "must not be the fleet cookie's own name: {cookie}"
        );
        // `__Host-` is only legal on exactly this attribute set, and the
        // browser drops the cookie outright if it is not met.
        assert!(cookie.contains("Secure") && cookie.contains("Path=/"));
    }

    /// The fleet cookie's shape is unchanged: `Domain` is still what the login
    /// path sets, so ordinary SSO is untouched by the handoff's cookie.
    #[test]
    fn the_login_cookie_is_still_the_fleet_wide_one() {
        let cookie = build_session_cookie("queen_session", "tok", Some(".queenmq.cloud"), true, 60);
        assert!(cookie.starts_with("queen_session=tok;"));
        assert!(cookie.contains("Domain=.queenmq.cloud"), "{cookie}");
    }

    /// A logout has to clear BOTH, or it ends the half of the session the
    /// browser shows and leaves the half this host actually reads.
    #[test]
    fn a_logout_clears_the_cell_cookie_as_well_as_the_fleet_one() {
        let cleared = clear_cookie_pair("queen_session", Some(".queenmq.cloud"), true);
        assert!(cleared[0].starts_with("queen_session=;"), "{}", cleared[0]);
        assert!(cleared[0].contains("Domain=.queenmq.cloud"));
        assert!(cleared[1].starts_with("__Host-queen_session_cell=;"), "{}", cleared[1]);
        assert!(!cleared[1].contains("Domain="), "{}", cleared[1]);
        for c in &cleared {
            assert!(c.contains("Max-Age=0"), "a clear is Max-Age=0: {c}");
        }
    }

    #[test]
    fn a_handoff_token_from_another_auth_host_is_refused() {
        // Two unrelated keypairs: a token minted by a fleet this cell does not
        // trust must not open its console, however well-formed it is.
        let (_, cell) = handoff_pair();
        let (stranger, _) = handoff_pair();
        let now = now_secs();
        let token =
            crate::auth::testkit::mint_at(&stranger, Uuid::new_v4(), now + 3600, None).unwrap();
        assert_eq!(
            decide_handoff(&cell, &token, Some("/console"), now),
            Handoff::Refuse("signature invalid")
        );
    }

    #[test]
    fn an_expired_handoff_token_is_refused() {
        let (auth_host, cell) = handoff_pair();
        let now = now_secs();
        // Well past jsonwebtoken's 60s leeway.
        let token =
            crate::auth::testkit::mint_at(&auth_host, Uuid::new_v4(), now - 3600, None).unwrap();
        assert_eq!(
            decide_handoff(&cell, &token, Some("/console"), now),
            Handoff::Refuse("expired")
        );

        // And the edge the leeway lets through: verification accepts a token
        // 30s dead, but there is no session left to hand over, so it is refused
        // here rather than set as a cookie the browser drops on arrival.
        let just_dead =
            crate::auth::testkit::mint_at(&auth_host, Uuid::new_v4(), now - 30, None).unwrap();
        assert_eq!(
            decide_handoff(&cell, &just_dead, Some("/console"), now),
            Handoff::Refuse("expired")
        );
    }

    #[test]
    fn a_handoff_minted_for_another_cell_is_refused() {
        // The `aud` binding, through the handoff path: this cell expects
        // `cell-a`, and a token stamped for `cell-b` verifies on signature and
        // issuer alone. auth.rs owns the rule; this pins that the handoff
        // inherits it instead of routing around it.
        let (auth_host, cell) = crate::auth::testkit::ed_handoff_pair("queen-proxy", Some("cell-a"));
        let now = now_secs();
        let ours =
            crate::auth::testkit::mint_at(&auth_host, Uuid::new_v4(), now + 3600, Some("cell-a"))
                .unwrap();
        assert_eq!(establish_of(decide_handoff(&cell, &ours, Some("/"), now)).0, "/");

        let theirs =
            crate::auth::testkit::mint_at(&auth_host, Uuid::new_v4(), now + 3600, Some("cell-b"))
                .unwrap();
        assert_eq!(
            decide_handoff(&cell, &theirs, Some("/console"), now),
            Handoff::Refuse("minted for another cell")
        );
    }

    #[test]
    fn a_refused_handoff_carries_an_unsafe_next_nowhere() {
        // A refusal is a 401, never a redirect, so `next` is not even consulted
        // on that path, and an accepted one is sanitized by `safe_next`.
        let (auth_host, cell) = handoff_pair();
        let now = now_secs();
        let token =
            crate::auth::testkit::mint_at(&auth_host, Uuid::new_v4(), now + 3600, None).unwrap();
        assert_eq!(establish_of(decide_handoff(&cell, &token, Some("//evil.com"), now)).0, "/");
        assert_eq!(establish_of(decide_handoff(&cell, &token, None, now)).0, "/");
    }

    #[test]
    fn a_mint_capable_host_is_never_the_handoff() {
        // The whole guarantee that the auth host's endpoint is unchanged: on a
        // host that can mint, `?token=` routes nowhere new.
        assert!(!is_handoff(true, Some("a.b.c")));
        assert!(!is_handoff(true, None));
        // Verify-only, but no token: the SPA's own call, which still answers
        // the 503 it always did rather than being read as an empty handoff.
        // That 503 is a contract, not a dead end — see the test below.
        assert!(!is_handoff(false, None));
        assert!(!is_handoff(false, Some("")));
        assert!(!is_handoff(false, Some("   ")));
        assert!(is_handoff(false, Some("a.b.c")));
    }

    #[tokio::test]
    async fn the_no_signer_answer_is_the_contract_the_console_reads() {
        // Where the case above lands: a verify-only cell cannot mint, so the
        // SPA's own /auth/session-token call falls through to `err_no_signer`.
        //
        // The console reads THIS pair — 503 plus `code: "not_configured"` — to
        // decide it must run on the session cookie instead of a bearer
        // (console/src/api.js, `isNoSigner`). It is deliberately both halves:
        // a bare 503 is also what a load balancer says in front of a restarting
        // instance, and the console must keep treating that one as an error.
        //
        // Change either half without changing api.js and every fleet cell's
        // /console/ goes back to dying on load with "could not start a session
        // (HTTP 503)" — measured against a real cell on 2026-08-29.
        let resp = err_no_signer();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.headers().get(header::CONTENT_TYPE).unwrap(), "application/json");

        let bytes = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["code"], "not_configured");
        // The prose half belongs to the operator reading it, not to the
        // console, so it may be reworded freely — but it has to be there.
        assert!(body["error"].as_str().is_some_and(|s| !s.is_empty()));
    }

    // --- resolution policy --------------------------------------------------

    #[test]
    fn resolve_existing_identity_logs_in() {
        let u = uref();
        assert_eq!(decide_resolution(Some(u.clone()), None, true, false), Resolution::Login(u));
    }

    #[test]
    fn resolve_links_verified_email_match() {
        let u = uref();
        // no identity yet, but a verified email matches an existing user -> Link
        assert_eq!(decide_resolution(None, Some(u.clone()), true, false), Resolution::Link(u));
    }

    #[test]
    fn resolve_unverified_email_does_not_link() {
        let u = uref();
        // email match present but NOT verified -> must not link; no autoprovision -> Deny
        assert_eq!(decide_resolution(None, Some(u), false, false), Resolution::Deny);
    }

    #[test]
    fn resolve_autoprovision_off_denies() {
        assert_eq!(decide_resolution(None, None, true, false), Resolution::Deny);
    }

    #[test]
    fn resolve_autoprovision_on_provisions() {
        assert_eq!(decide_resolution(None, None, true, true), Resolution::Provision);
    }

    // --- Google claims ------------------------------------------------------

    fn gclaims(email_verified: Value, nonce: &str) -> GoogleClaims {
        GoogleClaims {
            sub: Some("google-sub-123".to_string()),
            email: Some("User@Example.com".to_string()),
            email_verified: Some(email_verified),
            nonce: Some(nonce.to_string()),
            hd: None,
        }
    }

    /// No domain restriction — the shape every pre-existing test asserts.
    const ANY_DOMAIN: &[String] = &[];

    fn domains(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn google_verified_email_ok_and_lowercased() {
        let c = gclaims(json!(true), "n1");
        let id = validate_google_claims(&c, "n1", ANY_DOMAIN).expect("verified claims ok");
        assert_eq!(id.sub, "google-sub-123");
        assert_eq!(id.email, "user@example.com");
        // string "true" also accepted (real Google payloads vary)
        let c2 = gclaims(json!("true"), "n1");
        assert!(validate_google_claims(&c2, "n1", ANY_DOMAIN).is_ok());
    }

    #[test]
    fn google_unverified_email_rejected() {
        let c = gclaims(json!(false), "n1");
        assert_eq!(validate_google_claims(&c, "n1", ANY_DOMAIN), Err(ClaimErr::EmailUnverified));
        let c2 = gclaims(json!("false"), "n1");
        assert_eq!(validate_google_claims(&c2, "n1", ANY_DOMAIN), Err(ClaimErr::EmailUnverified));
    }

    #[test]
    fn google_nonce_mismatch_rejected() {
        let c = gclaims(json!(true), "n1");
        assert_eq!(validate_google_claims(&c, "different", ANY_DOMAIN), Err(ClaimErr::NonceMismatch));
    }

    #[test]
    fn google_missing_email_rejected() {
        let c = GoogleClaims {
            sub: Some("s".to_string()),
            email: None,
            email_verified: Some(json!(true)),
            nonce: Some("n1".to_string()),
            hd: None,
        };
        assert_eq!(validate_google_claims(&c, "n1", ANY_DOMAIN), Err(ClaimErr::MissingEmail));
    }

    // --- Google domain allowlist --------------------------------------------

    #[test]
    fn google_domain_allowed_via_hd_claim() {
        let mut c = gclaims(json!(true), "n1");
        c.hd = Some("Smartpricing.IT".to_string());
        c.email = Some("alice@smartpricing.it".to_string());
        let id = validate_google_claims(&c, "n1", &domains(&["smartpricing.it"]))
            .expect("hd matches the allowlist");
        assert_eq!(id.email, "alice@smartpricing.it");
    }

    #[test]
    fn google_domain_allowed_via_email_when_hd_absent() {
        // Workspace sets `hd`; this covers the case where it does not arrive but
        // the verified address is still at an allowed domain.
        let mut c = gclaims(json!(true), "n1");
        c.hd = None;
        c.email = Some("alice@smartpricing.it".to_string());
        assert!(validate_google_claims(&c, "n1", &domains(&["smartpricing.it"])).is_ok());
    }

    #[test]
    fn google_domain_rejected_when_neither_matches() {
        let mut c = gclaims(json!(true), "n1");
        c.hd = Some("evil.example".to_string());
        c.email = Some("attacker@evil.example".to_string());
        assert_eq!(
            validate_google_claims(&c, "n1", &domains(&["smartpricing.it"])),
            Err(ClaimErr::DomainNotAllowed)
        );
    }

    #[test]
    fn google_domain_empty_allowlist_allows_any() {
        let mut c = gclaims(json!(true), "n1");
        c.email = Some("someone@gmail.com".to_string());
        assert!(validate_google_claims(&c, "n1", ANY_DOMAIN).is_ok());
    }

    #[test]
    fn google_domain_check_runs_after_verification() {
        // An unverified email must not be reported as a domain problem: the
        // stronger rejection wins so the log says what actually happened.
        let mut c = gclaims(json!(false), "n1");
        c.email = Some("attacker@evil.example".to_string());
        assert_eq!(
            validate_google_claims(&c, "n1", &domains(&["smartpricing.it"])),
            Err(ClaimErr::EmailUnverified)
        );
    }

    #[test]
    fn google_domain_multiple_and_subdomain_is_not_a_match() {
        let mut c = gclaims(json!(true), "n1");
        c.hd = None;
        c.email = Some("u@sub.smartpricing.it".to_string());
        // Exact match only — a subdomain is a different domain.
        assert_eq!(
            validate_google_claims(&c, "n1", &domains(&["smartpricing.it", "smartness.com"])),
            Err(ClaimErr::DomainNotAllowed)
        );
        c.email = Some("u@smartness.com".to_string());
        assert!(validate_google_claims(&c, "n1", &domains(&["smartpricing.it", "smartness.com"])).is_ok());
    }

    // --- GitHub email selection --------------------------------------------

    #[test]
    fn github_unverified_primary_rejected() {
        let emails = json!([
            { "email": "primary@example.com", "primary": true, "verified": false },
            { "email": "old@example.com", "primary": false, "verified": true }
        ]);
        assert_eq!(select_github_email(&emails), None);
    }

    #[test]
    fn github_verified_primary_selected_and_lowercased() {
        let emails = json!([
            { "email": "Other@example.com", "primary": false, "verified": true },
            { "email": "Primary@Example.com", "primary": true, "verified": true }
        ]);
        assert_eq!(select_github_email(&emails), Some("primary@example.com".to_string()));
    }

    #[test]
    fn github_user_id_from_number_or_string() {
        assert_eq!(github_user_id(&json!({ "id": 42 })), Some("42".to_string()));
        assert_eq!(github_user_id(&json!({ "id": "42" })), Some("42".to_string()));
        assert_eq!(github_user_id(&json!({ "login": "x" })), None);
    }

    // --- misc ---------------------------------------------------------------

    #[test]
    fn qs_encodes_reserved() {
        let s = qs(&[("scope", "openid email profile"), ("redirect_uri", "https://a/b?x=1")]);
        assert_eq!(s, "scope=openid%20email%20profile&redirect_uri=https%3A%2F%2Fa%2Fb%3Fx%3D1");
    }

    // --- sign-in providers --------------------------------------------------

    #[test]
    fn google_is_offered_whenever_it_is_fully_configured() {
        // The regression this guards: the sign-in page used to list a provider
        // on its client id alone, while the callback also needs the secret —
        // so a half-configured instance showed a Google button that walked the
        // user out to Google and 404'd them on return.
        assert_eq!(provider_list(true, true, false, false), vec![("google", "Google")]);
        assert_eq!(provider_list(true, false, false, false), vec![]);
        assert_eq!(provider_list(false, true, false, false), vec![]);
    }

    #[test]
    fn github_is_offered_on_the_same_terms_and_comes_second() {
        assert_eq!(provider_list(false, false, true, true), vec![("github", "GitHub")]);
        assert_eq!(provider_list(false, false, true, false), vec![]);
        assert_eq!(
            provider_list(true, true, true, true),
            vec![("google", "Google"), ("github", "GitHub")]
        );
    }

    #[test]
    fn no_providers_configured_is_local_login_only() {
        // What a dev cell looks like — and it must not be mistaken for a
        // provider having gone missing.
        assert!(provider_list(false, false, false, false).is_empty());
    }

    // --- which sign-in surface the host serves -------------------------------

    /// Both providers configured, so any test that renders the `Here` panel
    /// shows the whole thing.
    const BOTH: &[(&str, &str)] = &[("google", "Google"), ("github", "GitHub")];

    #[test]
    fn verify_only_is_the_host_that_verifies_and_cannot_mint() {
        // The capability, not the knob: this is config::JwtMode::VerifyOnly as
        // auth::Keys ends up building it.
        assert_eq!(sign_in_surface(false, true), SignIn::Elsewhere);
        // A minting host serves the form, whichever signer it built.
        assert_eq!(sign_in_surface(true, true), SignIn::Here);
        // NO MATERIAL AT ALL KEEPS THE FORM. It is the API-key-only proxy, and
        // nothing mints its sessions anywhere, so there is no control plane to
        // point it at; its form produces the 503 that names the variables to
        // set. Losing this line would put a control-plane notice on every
        // self-hosted proxy that never configured a signer.
        assert_eq!(sign_in_surface(false, false), SignIn::Here);
    }

    #[test]
    fn verify_only_replaces_the_form_with_the_notice() {
        // The dead end this closes, measured on a Queen Cloud cell: the page
        // offered email/password on a host that cannot mint a session, so every
        // submission it invited came back 503.
        let html = login_html(SignIn::Elsewhere, BOTH, None, "/", None);
        assert!(!html.contains("<form"), "no form on a host that cannot complete one");
        assert!(!html.contains("type=\"password\""), "and nothing to type a password into");
        assert!(
            html.contains("This host verifies sessions but does not issue them."),
            "the notice says why in the page's own voice"
        );
        assert!(html.contains("control plane"), "and where sign-in actually happens");
        // The provider buttons are the same dead end one redirect later: both
        // callbacks end in `establish_session`, which cannot mint here either.
        assert!(!html.contains("Continue with Google"), "no OAuth button that 503s on return");
        assert!(!html.contains("class=\"sep\""), "and no divider left over one");
    }

    #[test]
    fn the_portal_link_is_rendered_only_when_it_is_configured() {
        let with = login_html(
            SignIn::Elsewhere,
            &[],
            Some(("https://auth.example.test/login", "Continue to the control plane")),
            "/",
            None,
        );
        assert!(
            with.contains("<a class=\"oauth\" href=\"https://auth.example.test/login\">"),
            "the configured URL is the link"
        );
        assert!(with.contains(">Continue to the control plane</a>"), "labelled as configured");
        // Absent is a supported shape, not a broken one: the same notice, with
        // nothing to click.
        let without = login_html(SignIn::Elsewhere, &[], None, "/", None);
        assert!(without.contains("This host verifies sessions"), "still explains");
        assert!(!without.contains("<a class=\"oauth\""), "with no empty link left behind");
    }

    #[test]
    fn the_portal_link_carries_no_next_and_escapes_what_it_is_given() {
        // `next` names a path on THIS host; the portal is a different service.
        let html = login_html(
            SignIn::Elsewhere,
            &[],
            Some(("https://auth.example.test/login", "Sign in")),
            "/clusters/prod",
            None,
        );
        assert!(!html.contains("/clusters/prod"), "the portal link is not a return path");
        // Config refuses a non-http(s) URL long before this, but the page escapes
        // anyway: this is the one surface served with no session at all.
        let nasty = login_html(
            SignIn::Elsewhere,
            &[],
            Some(("https://x.test/\"><script>alert(1)</script>", "L<b>")),
            "/",
            None,
        );
        assert!(!nasty.contains("<script>"), "no markup escapes the href");
        assert!(!nasty.contains("L<b>"), "nor the label");
    }

    #[test]
    fn a_minting_host_renders_the_form_exactly_as_before() {
        let html = login_html(SignIn::Here, BOTH, None, "/clusters/prod", Some("Invalid email or password."));
        assert!(html.contains("<form method=\"post\" action=\"/auth/login\">"));
        assert!(html.contains("autocomplete=\"current-password\""));
        assert!(html.contains("value=\"/clusters/prod\""), "next rides the form");
        assert!(html.contains("Continue with Google") && html.contains("Continue with GitHub"));
        assert!(html.contains("Invalid email or password."), "and a failed post still says so");
        // A portal URL set on a minting host is inert: it has a form that works.
        let with_portal =
            login_html(SignIn::Here, &[], Some(("https://auth.example.test", "Portal")), "/", None);
        assert!(with_portal.contains("<form"), "the form is what this host offers");
        assert!(!with_portal.contains("https://auth.example.test"), "the portal is not offered");
    }

    #[test]
    fn the_notice_is_not_dressed_as_an_error() {
        // A verify-only cell is a deliberate deployment shape, not a fault. The
        // error styling (the rose `.err` block) belongs to a failed login, and
        // wearing it here would read as "something is broken" on every cloud
        // cell's sign-in page.
        let html = login_html(SignIn::Elsewhere, &[], None, "/", None);
        assert!(!html.contains("class=\"err\""), "the notice is routine, not an alarm");
        assert!(html.contains("class=\"note\""));
    }

    // --- sign-in page brand -------------------------------------------------

    #[test]
    fn the_login_brand_is_embedded() {
        // The sign-in page can only inline what the build actually embedded.
        // If `app/public/queen-mark.svg` is renamed or the webapp is not built,
        // fail HERE rather than shipping a login page with no mark.
        let decode = |uri: &str, what: &str| {
            let b64 = uri
                .strip_prefix("data:image/svg+xml;base64,")
                .unwrap_or_else(|| panic!("{what} is an svg data URI"));
            String::from_utf8(B64.decode(b64).expect("valid base64")).unwrap()
        };

        let badge = decode(brand_badge_data_uri(), "badge");
        assert!(badge.contains("<svg"), "decodes back to the mark the sidebar shows");
        // The sign-in page is dark, so the badge must carry an explicit light
        // fill: `currentColor` would resolve against a <img> context and land
        // on black, i.e. an invisible mark on this page.
        assert!(!badge.contains("currentColor"), "badge fill is resolved, not inherited");

        let icon = decode(favicon_data_uri(), "favicon");
        assert!(icon.contains("<svg"), "decodes back to the tab icon");

        // Inlining is pointless if the art then reaches for a URL of its own:
        // webapp.rs would answer that with a 302 back to this page. Both marks
        // must be self-contained geometry, with nothing to fetch.
        for (what, svg) in [("badge", &badge), ("favicon", &icon)] {
            assert!(!svg.contains("href=\"/"), "no same-origin refs inside the {what}");
            assert!(!svg.contains("href=\"http"), "no remote refs inside the {what}");
            assert!(!svg.contains("<image"), "{what} is vector, with no raster to fetch");
        }
    }
}
