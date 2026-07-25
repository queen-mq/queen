//! Human identity endpoints. OWNER: Agent G (Track A).
//!
//!   * local login (bcrypt) + session JWT in an httpOnly cookie
//!   * Google OAuth (authorization-code + OIDC id_token, port of
//!     proxy/src/google-auth.js): token exchange, id_token verify against
//!     Google's JWKS, verified-email-only linking, opt-in auto-provision
//!   * GitHub OAuth (OAuth2 *without* OIDC): /user + /user/emails, verified
//!     primary email only, same resolve/link/provision path
//!   * `/auth/me`, `/auth/session-token` (short Bearer for the SPA), logout
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
            return err_500("session token could not be minted");
        }
    };
    record_op(st, user.tenant_id, user.user_id, "login", Some(user.user_id.to_string()), json!({})).await;
    let cookie = session_cookie(st, headers, &token);
    redirect(StatusCode::SEE_OTHER, next, Some(&cookie))
}

async fn logout(State(st): State<St>, headers: HeaderMap) -> Response {
    let cookie = clear_cookie(&st, &headers);
    let mut resp = json_response(StatusCode::OK, json!({ "ok": true }));
    if let Ok(v) = HeaderValue::from_str(&cookie) {
        resp.headers_mut().insert(header::SET_COOKIE, v);
    }
    resp
}

async fn me(State(st): State<St>, headers: HeaderMap) -> Response {
    let Some(tok) = read_cookie(&headers, &st.cfg.cookie_name) else {
        return errors::err_401("no session");
    };
    match st.keys.verify_jwt_claims(&tok) {
        Ok(c) => json_response(
            StatusCode::OK,
            json!({
                "user_id": c.user_id.to_string(),
                "role": role_str(c.role),
                "cluster": c.cluster.map(|u| u.to_string()),
            }),
        ),
        Err(_) => errors::err_401("invalid session"),
    }
}

async fn session_token(State(st): State<St>, headers: HeaderMap) -> Response {
    let Some(tok) = read_cookie(&headers, &st.cfg.cookie_name) else {
        return errors::err_401("no session");
    };
    let claims = match st.keys.verify_jwt_claims(&tok) {
        Ok(c) => c,
        Err(_) => return errors::err_401("invalid session"),
    };
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
            err_500("session token could not be minted")
        }
    }
}

// ---------------------------------------------------------------------------
// Google OAuth
// ---------------------------------------------------------------------------

async fn google_start(
    State(st): State<St>,
    headers: HeaderMap,
    Query(q): Query<StartQuery>,
) -> Response {
    let Some(cid) = st.cfg.google_client_id.clone() else {
        return errors::err_404("not_configured", "google login not configured");
    };
    let redirect_uri = match redirect_uri(&st, &headers, "google") {
        Some(u) => u,
        None => return err_500("public base url required (auth-host mode)"),
    };
    let next = safe_next(q.next.as_deref());
    let nonce = random_hex(24);
    let state = sign_state(state_key(), &nonce, &next, STATE_TTL_S);
    let url = format!(
        "{GOOGLE_AUTHORIZE_URL}?{}",
        qs(&[
            ("client_id", &cid),
            ("redirect_uri", &redirect_uri),
            ("response_type", "code"),
            ("scope", "openid email profile"),
            ("access_type", "online"),
            ("include_granted_scopes", "true"),
            ("prompt", "select_account"),
            ("state", &state),
            ("nonce", &nonce),
        ])
    );
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
    let gid = match validate_google_claims(&claims, &nonce) {
        Ok(g) => g,
        Err(e) => return claim_err_response(e),
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
}

/// Pure OIDC claim check: nonce must match the signed-state nonce, sub + email
/// must be present, and the email MUST be verified (`email_verified` accepted as
/// bool `true` or the string `"true"`, matching real Google payloads).
fn validate_google_claims(claims: &GoogleClaims, expected_nonce: &str) -> Result<OidcIdentity, ClaimErr> {
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
    Ok(OidcIdentity { sub: sub.to_string(), email: email.to_lowercase() })
}

fn claim_err_response(e: ClaimErr) -> Response {
    match e {
        ClaimErr::NonceMismatch => err_400("invalid_state", "id_token nonce mismatch"),
        ClaimErr::MissingSub => err_400("invalid_request", "id_token missing subject"),
        ClaimErr::MissingEmail => err_400("invalid_request", "id_token missing email"),
        ClaimErr::EmailUnverified => {
            errors::err_403("email_unverified", "provider email is not verified")
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
    let Some(cid) = st.cfg.github_client_id.clone() else {
        return errors::err_404("not_configured", "github login not configured");
    };
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
            let u = provision_user(pool, email, provider, provider_id).await?;
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

    tx.commit().await.map_err(|e| ResolveErr::Db(e.to_string()))?;
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

fn is_safe(n: &str) -> bool {
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
fn cookie_is_secure(st: &St, headers: &HeaderMap) -> bool {
    st.cfg.cookie_domain.is_some()
        || header_str(headers, "x-forwarded-proto").map(|v| v.eq_ignore_ascii_case("https")).unwrap_or(false)
}

fn session_cookie(st: &St, headers: &HeaderMap, token: &str) -> String {
    let mut c = format!(
        "{}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}",
        st.cfg.cookie_name, token, st.cfg.jwt_ttl_s
    );
    if let Some(dom) = &st.cfg.cookie_domain {
        c.push_str(&format!("; Domain={dom}"));
    }
    if cookie_is_secure(st, headers) {
        c.push_str("; Secure");
    }
    c
}

fn clear_cookie(st: &St, headers: &HeaderMap) -> String {
    let mut c = format!("{}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0", st.cfg.cookie_name);
    if let Some(dom) = &st.cfg.cookie_domain {
        c.push_str(&format!("; Domain={dom}"));
    }
    if cookie_is_secure(st, headers) {
        c.push_str("; Secure");
    }
    c
}

fn read_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    for kv in raw.split(';') {
        if let Some((k, v)) = kv.split_once('=') {
            if k.trim() == name {
                return Some(v.trim().to_string());
            }
        }
    }
    None
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
fn pct(s: &str) -> String {
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

/// Minimal self-contained login page: email/password form, provider buttons
/// when configured, sober inline CSS. `next` is carried through as a hidden
/// field and on the provider links.
fn render_login(st: &St, next: &str, error: Option<&str>) -> String {
    let next_e = esc(next);
    let next_q = pct(next);
    let err_html = match error {
        Some(e) => format!("<div class=\"err\">{}</div>", esc(e)),
        None => String::new(),
    };
    let mut providers = String::new();
    if st.cfg.google_client_id.is_some() {
        providers.push_str(&format!(
            "<a class=\"oauth\" href=\"/auth/google?next={next_q}\">Continue with Google</a>"
        ));
    }
    if st.cfg.github_client_id.is_some() {
        providers.push_str(&format!(
            "<a class=\"oauth\" href=\"/auth/github?next={next_q}\">Continue with GitHub</a>"
        ));
    }
    let divider = if providers.is_empty() {
        String::new()
    } else {
        format!("{providers}<div class=\"sep\">or</div>")
    };
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
<title>Sign in · queen-proxy</title><style>\
:root{{color-scheme:light dark}}\
*{{box-sizing:border-box}}\
body{{font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:0;\
min-height:100vh;display:flex;align-items:center;justify-content:center;background:#f4f5f7;color:#111}}\
.card{{width:100%;max-width:360px;background:#fff;border:1px solid #e3e5e8;border-radius:12px;\
padding:28px 26px;box-shadow:0 1px 3px rgba(0,0,0,.06)}}\
h1{{font-size:19px;margin:0 0 18px;font-weight:600}}\
label{{display:block;font-size:13px;color:#555;margin:12px 0 4px}}\
input{{width:100%;padding:9px 10px;border:1px solid #ccd0d5;border-radius:7px;font-size:14px;background:#fff;color:#111}}\
input:focus{{outline:2px solid #4c6ef5;outline-offset:0;border-color:#4c6ef5}}\
button{{width:100%;margin-top:18px;padding:10px;border:0;border-radius:7px;background:#4c6ef5;color:#fff;\
font-size:14px;font-weight:600;cursor:pointer}}\
button:hover{{background:#3b5bdb}}\
.oauth{{display:block;text-align:center;margin-top:10px;padding:9px;border:1px solid #ccd0d5;border-radius:7px;\
text-decoration:none;color:#111;background:#fff}}\
.oauth:hover{{background:#f0f1f3}}\
.sep{{text-align:center;color:#999;font-size:12px;margin:16px 0 4px;text-transform:uppercase;letter-spacing:.05em}}\
.err{{background:#fdecec;border:1px solid #f5c2c2;color:#a12; padding:8px 10px;border-radius:7px;font-size:13px;margin-bottom:6px}}\
@media(prefers-color-scheme:dark){{body{{background:#16181d;color:#e8eaed}}\
.card{{background:#1e2126;border-color:#2c3038}}h1{{color:#f1f3f5}}label{{color:#aab}}\
input{{background:#111318;border-color:#333;color:#e8eaed}}\
.oauth{{background:#1e2126;border-color:#333;color:#e8eaed}}.oauth:hover{{background:#262a31}}\
.err{{background:#2a1719;border-color:#5a2a2e;color:#f1a9ad}}}}\
</style></head><body><div class=\"card\"><h1>Sign in</h1>{err_html}{divider}\
<form method=\"post\" action=\"/auth/login\">\
<input type=\"hidden\" name=\"next\" value=\"{next_e}\">\
<label for=\"email\">Email</label>\
<input id=\"email\" name=\"email\" type=\"email\" autocomplete=\"username\" required autofocus>\
<label for=\"password\">Password</label>\
<input id=\"password\" name=\"password\" type=\"password\" autocomplete=\"current-password\" required>\
<button type=\"submit\">Sign in</button></form></div></body></html>"
    )
}

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
        }
    }

    #[test]
    fn google_verified_email_ok_and_lowercased() {
        let c = gclaims(json!(true), "n1");
        let id = validate_google_claims(&c, "n1").expect("verified claims ok");
        assert_eq!(id.sub, "google-sub-123");
        assert_eq!(id.email, "user@example.com");
        // string "true" also accepted (real Google payloads vary)
        let c2 = gclaims(json!("true"), "n1");
        assert!(validate_google_claims(&c2, "n1").is_ok());
    }

    #[test]
    fn google_unverified_email_rejected() {
        let c = gclaims(json!(false), "n1");
        assert_eq!(validate_google_claims(&c, "n1"), Err(ClaimErr::EmailUnverified));
        let c2 = gclaims(json!("false"), "n1");
        assert_eq!(validate_google_claims(&c2, "n1"), Err(ClaimErr::EmailUnverified));
    }

    #[test]
    fn google_nonce_mismatch_rejected() {
        let c = gclaims(json!(true), "n1");
        assert_eq!(validate_google_claims(&c, "different"), Err(ClaimErr::NonceMismatch));
    }

    #[test]
    fn google_missing_email_rejected() {
        let c = GoogleClaims {
            sub: Some("s".to_string()),
            email: None,
            email_verified: Some(json!(true)),
            nonce: Some("n1".to_string()),
        };
        assert_eq!(validate_google_claims(&c, "n1"), Err(ClaimErr::MissingEmail));
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
}
