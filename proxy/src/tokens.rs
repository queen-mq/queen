//! `POST /api/v1/tokens`: mint a client token for one entity (clients.rs,
//! `queen_protocol::scope::MintTokenRequest`).
//!
//! Who mints, and for which key:
//! - a SCOPED key, used directly: a token of its own, for any `sub` — the
//!   backend that holds the role key mints for its devices or users;
//! - an `admin` key, or a console admin session: a token of the scoped key the
//!   body names (`key`, its id or its name);
//! - a client token: never (a token does not mint tokens), and a whole-cluster
//!   key without `admin` neither.
//!
//! The token is never stored: minting writes nothing. It is refused for an
//! entity on the key's block list, and it never lives longer than the key's
//! ceiling (`token_ttl_max_s`).

use std::sync::Arc;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use queen_protocol::scope::{MintTokenRequest, MintTokenResponse};
use uuid::Uuid;

use crate::cache::KeyFound;
use crate::clients::{ClientClaims, DEFAULT_TOKEN_TTL_S, MAX_TOKEN_GRANTS};
use crate::errors;
use crate::limits::Decision;
use crate::state::{ClusterCtx, Principal, Role, St};

/// The route.
pub const PATH: &str = "/api/v1/tokens";

/// A mint request is a few fields and at most a handful of grants.
const BODY_CAP: usize = 64 * 1024;

pub async fn handle(st: St, req: Request) -> Response {
    if req.method() != Method::POST {
        let mut r = errors::json_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "POST /api/v1/tokens mints a client token",
        );
        r.headers_mut().insert(
            axum::http::header::ALLOW,
            axum::http::HeaderValue::from_static("POST"),
        );
        return r;
    }
    let (parts, body) = req.into_parts();

    // The cluster and the caller, the way the data plane resolves them.
    let (ctx, principal) = match crate::acting::resolve_route(&st, &parts.headers).await {
        Ok(crate::acting::Route::Fixed(ctx)) => {
            match crate::acting::authenticate_for(&st, &parts.headers, &ctx).await {
                Ok(p) => (ctx, p),
                Err(r) => return r,
            }
        }
        Ok(crate::acting::Route::FromCredential) => {
            match crate::acting::resolve_from_credential(&st, &parts.headers).await {
                Ok(v) => v,
                Err(r) => return r,
            }
        }
        Ok(crate::acting::Route::UnknownHost) => {
            return crate::acting::unknown_host_refusal(&st, &parts.headers).await
        }
        Err(r) => return r,
    };
    if let Some(r) = crate::gateway::status_gate(&ctx) {
        return r;
    }
    if let Decision::Deny {
        retry_after_s,
        code,
    } = st.limits.check_req(&ctx)
    {
        if st.limits.enforcing() {
            return errors::err_429(code, retry_after_s, "request rate limit exceeded");
        }
    }

    let raw = match axum::body::to_bytes(body, BODY_CAP).await {
        Ok(b) => b,
        Err(_) => return errors::err_413("a mint request is at most 64 KiB"),
    };
    let ask: MintTokenRequest = match serde_json::from_slice(&raw) {
        Ok(a) => a,
        Err(e) => return bad_request(&format!("bad body: {e}")),
    };
    if !st.keys.can_mint() {
        return errors::json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "not_configured",
            "this proxy cannot sign tokens: set QUEEN_PROXY_JWT_ED25519_PEM (or \
             QUEEN_PROXY_JWT_SECRET) on it",
        );
    }

    let key = match key_for(&st, &ctx, &principal, ask.key.as_deref()).await {
        Ok(k) => k,
        Err(r) => return r,
    };
    let Some(policy) = key.policy.as_ref() else {
        return bad_request("the key is not a scoped key: only a key with grants mints tokens");
    };

    if !queen_protocol::scope::valid_sub(&ask.sub) {
        return bad_request(&format!(
            "sub must be 1 to {} letters, digits and . _ - @ + (no / : * or spaces)",
            queen_protocol::scope::SUB_MAX_BYTES
        ));
    }
    let ttl = ask
        .ttl_seconds
        .unwrap_or(DEFAULT_TOKEN_TTL_S.min(policy.token_ttl_max_s));
    if ttl == 0 || ttl > policy.token_ttl_max_s {
        return bad_request(&format!(
            "ttlSeconds is {ttl}: between 1 and this key's ceiling, {} s",
            policy.token_ttl_max_s
        ));
    }
    if let Some(allow) = &ask.allow {
        // A token that reaches nothing is a mistake, most often an SDK that
        // sends an empty list where it meant none.
        if allow.is_empty() {
            return bad_request("allow is empty: leave it out for the key's own grants");
        }
        if allow.len() > MAX_TOKEN_GRANTS {
            return bad_request(&format!(
                "allow holds {} grants, at most {MAX_TOKEN_GRANTS}",
                allow.len()
            ));
        }
        if let Err(e) = queen_protocol::scope::check_grants(allow) {
            return bad_request(&format!("allow: {e}"));
        }
    }
    if st
        .clients
        .blocks
        .is_blocked(&st.store, ctx.cluster_id, key.key_id, &ask.sub)
        .await
    {
        st.clients.log.refused(
            &ctx.slug,
            &format!("key {} sub {}", key.key_id, ask.sub),
            "POST",
            PATH,
            "mint for a blocked client",
        );
        return crate::clients::err_blocked();
    }

    let claims = ClientClaims::new(
        &st.cfg.jwt_issuer,
        &ask.sub,
        key.key_id,
        ctx.cluster_id,
        ttl,
        ask.allow,
    );
    let token = match st.keys.mint_client_token(&claims) {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(target: "clientauth", error = %e, "client token signing failed");
            return errors::json_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "not_configured",
                "this proxy could not sign the token",
            );
        }
    };
    st.clients
        .log
        .minted(&ctx.slug, key.key_id, &claims.sub, &claims.jti, claims.exp);
    let out = MintTokenResponse {
        token,
        sub: claims.sub,
        key: key.key_id.to_string(),
        expires_at: crate::store::data::iso_utc(claims.exp * 1_000_000),
        expires_in: ttl,
    };
    (
        StatusCode::CREATED,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        Body::from(serde_json::to_string(&out).unwrap_or_default()),
    )
        .into_response()
}

fn bad_request(msg: &str) -> Response {
    errors::json_error(StatusCode::BAD_REQUEST, "invalid_request", msg)
}

/// The scoped key a token is minted from: the caller itself (a scoped key),
/// or the one an admin names.
async fn key_for(
    st: &St,
    ctx: &Arc<ClusterCtx>,
    principal: &Principal,
    named: Option<&str>,
) -> Result<KeyFound, Response> {
    match principal {
        Principal::Client(c) if c.sub.is_none() => {
            let me = st
                .cache
                .by_key_id(c.key_id)
                .await
                .filter(|k| k.ctx.cluster_id == ctx.cluster_id)
                .ok_or_else(|| errors::err_401("unknown or revoked api key"))?;
            match named {
                None => Ok(me),
                Some(n) if n == me.key_id.to_string() || n == &*me.name => Ok(me),
                Some(_) => Err(errors::err_403(
                    errors::CODE_FORBIDDEN,
                    "a scoped key mints tokens of its own only: drop `key`, or mint with an \
                     admin key",
                )),
            }
        }
        Principal::Client(_) => Err(errors::err_403(
            errors::CODE_FORBIDDEN,
            "a client token cannot mint tokens",
        )),
        Principal::ApiKey { scopes, .. } if scopes.admin => named_key(st, ctx, named).await,
        Principal::User {
            role: Role::Admin, ..
        } => named_key(st, ctx, named).await,
        _ => Err(errors::err_403(
            errors::CODE_FORBIDDEN,
            "minting a token needs the scoped key itself, an admin key, or a console admin",
        )),
    }
}

/// The live scoped key of this cluster an admin names, by id or by name.
async fn named_key(
    st: &St,
    ctx: &Arc<ClusterCtx>,
    named: Option<&str>,
) -> Result<KeyFound, Response> {
    let Some(named) = named.map(str::trim).filter(|n| !n.is_empty()) else {
        return Err(bad_request(
            "an admin mints for a scoped key: name it in `key` (its id or its name)",
        ));
    };
    let id = match Uuid::parse_str(named) {
        Ok(id) => id,
        Err(_) => {
            let rows = crate::store::web::list_cluster_keys(&st.store, ctx.cluster_id)
                .await
                .map_err(|e| {
                    tracing::warn!(target: "clientauth", error = %e, "mint: key list failed");
                    errors::err_502("store unavailable")
                })?;
            let live: Vec<_> = rows
                .into_iter()
                .filter(|r| r.name == named && r.revoked_at.is_none())
                .collect();
            match live.as_slice() {
                [one] => Uuid::parse_str(&one.id).map_err(|_| errors::err_502("bad key id"))?,
                [] => {
                    return Err(errors::err_404(
                        "key_unknown",
                        "no live key of that name on this cluster",
                    ))
                }
                _ => {
                    return Err(errors::json_error(
                        StatusCode::CONFLICT,
                        "key_ambiguous",
                        "several live keys have that name: name the key by its id",
                    ))
                }
            }
        }
    };
    st.cache
        .by_key_id(id)
        .await
        .filter(|k| k.ctx.cluster_id == ctx.cluster_id)
        .ok_or_else(|| errors::err_404("key_unknown", "no live key of that id on this cluster"))
}

#[cfg(test)]
mod tests {
    //! Client tokens through the data plane, end to end on this proxy: a
    //! scoped key mints, the token reaches a recording broker with its scope,
    //! and the refusals that must happen here happen here.

    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::extract::{Request, State};
    use axum::http::{header, HeaderMap, Method, StatusCode};
    use axum::response::Response;
    use queen_protocol::scope::{Grant, Scope, SCOPE_HEADER};
    use serde_json::{json, Value};
    use uuid::Uuid;

    use crate::state::{AppState, St};
    use crate::store::data::{self, PolicyInput};
    use crate::store::schema::KeyLimitsDoc;

    /// What the broker was handed: path, headers, and the typed scope when
    /// one rode along in-process.
    type Seen = Arc<Mutex<Vec<(String, HeaderMap, Option<Arc<Scope>>)>>>;

    fn broker(seen: Seen) -> axum::Router {
        axum::Router::new().fallback(move |req: Request| {
            let seen = seen.clone();
            async move {
                let typed = req.extensions().get::<Arc<Scope>>().cloned();
                seen.lock().unwrap().push((
                    req.uri().path().to_string(),
                    req.headers().clone(),
                    typed,
                ));
                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap()
            }
        })
    }

    struct World {
        st: St,
        seen: Seen,
        cluster: Uuid,
        /// The tenant's bootstrap key: whole cluster, every scope.
        admin_key: String,
        /// A scoped key: the device role.
        devices: String,
        devices_id: Uuid,
    }

    fn device_grants() -> Vec<Grant> {
        serde_json::from_value(json!([
            {"queue": "telemetry", "partition": "{sub}", "ops": ["produce"]},
            {"queue": "commands", "partition": "{sub}", "ops": ["consume"]}
        ]))
        .unwrap()
    }

    async fn world(limits: KeyLimitsDoc) -> World {
        let mut cfg = crate::config::test_config(&[]);
        cfg.jwt_hs_secret = Some("a-test-secret-of-some-length".to_string());
        cfg.max_body_bytes = 1 << 20;
        let (store, cluster, admin_key) = data::test_world("acme", "inprocess://self", "pro").await;
        let devices = crate::auth::generate_api_key("test");
        let (devices_id, _) = data::ensure_scoped_key(
            &store,
            cluster,
            "devices",
            &crate::auth::key_hash_hex(&devices),
            &PolicyInput {
                grants: device_grants(),
                limits,
                token_ttl_max_s: Some(3600),
            },
        )
        .await
        .expect("a scoped key");
        let seen = Seen::default();
        let st = Arc::new(AppState {
            cache: crate::cache::ClusterCache::new(&cfg, store.clone()),
            limits: crate::limits::Limits::new(&cfg),
            meter: Arc::new(crate::meter::Meter::new(&cfg)),
            registry: crate::registry::Registry::new(crate::store::Store::None),
            keys: crate::auth::Keys::from_config(&cfg),
            clients: Default::default(),
            upstream: crate::upstream::Upstream::InProcess(broker(seen.clone())),
            s3: None,
            store,
            cfg,
        });
        World {
            st,
            seen,
            cluster,
            admin_key,
            devices,
            devices_id,
        }
    }

    async fn call(
        st: &St,
        method: Method,
        path: &str,
        bearer: &str,
        body: Option<Value>,
        extra: &[(&str, &str)],
    ) -> (StatusCode, Value) {
        let mut req = Request::builder()
            .method(method)
            .uri(path)
            .header(header::HOST, "acme.test")
            .header(header::AUTHORIZATION, format!("Bearer {bearer}"));
        for (k, v) in extra {
            req = req.header(*k, *v);
        }
        let body = match body {
            Some(b) => {
                req = req.header(header::CONTENT_TYPE, "application/json");
                Body::from(b.to_string())
            }
            None => Body::empty(),
        };
        let resp = crate::webapp::route_fallback(State(st.clone()), req.body(body).unwrap()).await;
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn mint(w: &World, bearer: &str, body: Value) -> (StatusCode, Value) {
        call(&w.st, Method::POST, super::PATH, bearer, Some(body), &[]).await
    }

    async fn token(w: &World, sub: &str) -> String {
        let (s, v) = mint(w, &w.devices, json!({"sub": sub})).await;
        assert_eq!(s, StatusCode::CREATED, "{v}");
        v["token"].as_str().unwrap().to_string()
    }

    fn push(partition: &str) -> Value {
        json!({"items": [{"queue": "telemetry", "partition": partition, "payload": {}}]})
    }

    #[tokio::test]
    async fn a_scoped_key_mints_a_token_that_reaches_the_broker_with_its_scope() {
        let w = world(KeyLimitsDoc::default()).await;
        let (s, v) = mint(
            &w,
            &w.devices,
            json!({"sub": "dev-8f3a", "ttlSeconds": 600}),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED, "{v}");
        let tok = v["token"].as_str().unwrap();
        assert!(tok.starts_with("qt_"), "{tok}");
        assert_eq!(v["sub"], "dev-8f3a");
        assert_eq!(v["key"], w.devices_id.to_string());
        assert_eq!(v["expiresIn"], 600);

        // A scope header the client sends is never the one the broker reads.
        let forged = r#"{"v":1,"layers":[[{"kind":"queue","name":"*","partition":"*","group":"*","key":"*","ops":["produce"]}]]}"#;
        let (s, v) = call(
            &w.st,
            Method::POST,
            "/api/v1/push",
            tok,
            Some(push("dev-8f3a")),
            &[(SCOPE_HEADER, forged)],
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{v}");
        let seen = w.seen.lock().unwrap().clone();
        let (path, headers, typed) = seen.last().expect("forwarded");
        assert_eq!(path, "/api/v1/push");
        let scope = Scope::decode(headers.get(SCOPE_HEADER).expect("scope header").as_bytes())
            .expect("a scope");
        assert_eq!(
            headers.get_all(SCOPE_HEADER).iter().count(),
            1,
            "one scope, ours"
        );
        assert_eq!(
            Some(&scope),
            typed.as_deref(),
            "the same scope, typed in-process"
        );
        let ask = |p| queen_protocol::scope::Ask::Produce {
            queue: "telemetry",
            partition: p,
        };
        assert!(scope.allows(&ask("dev-8f3a")));
        assert!(!scope.allows(&ask("dev-8f3b")), "{scope:?}");

        // The key used directly is scoped too: its grants name {sub}, so
        // without a token it reaches nothing, and the broker is told so.
        let (s, _) = call(
            &w.st,
            Method::POST,
            "/api/v1/push",
            &w.devices,
            Some(push("x")),
            &[],
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        let seen = w.seen.lock().unwrap().clone();
        let scope =
            Scope::decode(seen.last().unwrap().1.get(SCOPE_HEADER).unwrap().as_bytes()).unwrap();
        assert!(!scope.allows(&ask("x")));

        // A whole-cluster key travels without a scope, as before.
        let (s, _) = call(
            &w.st,
            Method::POST,
            "/api/v1/push",
            &w.admin_key,
            Some(push("x")),
            &[(SCOPE_HEADER, forged)],
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        let seen = w.seen.lock().unwrap().clone();
        let (_, headers, typed) = seen.last().unwrap();
        assert!(
            headers.get(SCOPE_HEADER).is_none(),
            "the client's header is dropped"
        );
        assert!(typed.is_none());
    }

    #[tokio::test]
    async fn a_scoped_client_reaches_only_the_routes_that_check_names() {
        let w = world(KeyLimitsDoc::default()).await;
        let tok = token(&w, "dev1").await;
        let before = w.seen.lock().unwrap().len();
        for (m, p) in [
            (Method::GET, "/api/v1/resources/queues"),
            (Method::POST, "/api/v1/configure"),
            (Method::GET, "/api/v1/dlq"),
            (Method::POST, "/api/v1/locks"),
            (Method::POST, "/api/v1/ephemeral/push"),
            (Method::POST, "/streams/v1/claim"),
            (Method::POST, "/api/v1/fetch"),
        ] {
            let (s, v) = call(&w.st, m.clone(), p, &tok, Some(json!({})), &[]).await;
            assert!(
                s == StatusCode::FORBIDDEN || s == StatusCode::NOT_FOUND,
                "{m} {p}: {s} {v}"
            );
        }
        assert_eq!(
            w.seen.lock().unwrap().len(),
            before,
            "none of them was forwarded"
        );
        let (s, _) = call(
            &w.st,
            Method::GET,
            "/api/v1/pop/queue/commands/partition/dev1",
            &tok,
            None,
            &[],
        )
        .await;
        assert_eq!(s, StatusCode::OK, "a route that checks names is forwarded");
    }

    #[tokio::test]
    async fn a_token_follows_its_key_and_its_block() {
        let w = world(KeyLimitsDoc::default()).await;
        let tok = token(&w, "dev1").await;
        let ok = || {
            call(
                &w.st,
                Method::POST,
                "/api/v1/push",
                &tok,
                Some(push("dev1")),
                &[],
            )
        };
        assert_eq!(ok().await.0, StatusCode::OK);

        data::block_client(
            &w.st.store,
            w.cluster,
            w.devices_id,
            "dev1",
            Some("stolen"),
            ("test", None),
        )
        .await
        .unwrap();
        w.st.clients.blocks.invalidate(w.cluster);
        let (s, v) = ok().await;
        assert_eq!(
            (s, &v["code"]),
            (StatusCode::FORBIDDEN, &json!("client_blocked")),
            "{v}"
        );
        let (s, v) = mint(&w, &w.devices, json!({"sub": "dev1"})).await;
        assert_eq!(
            (s, &v["code"]),
            (StatusCode::FORBIDDEN, &json!("client_blocked")),
            "no new token for it: {v}"
        );
        let other = token(&w, "dev2").await;
        let (s, _) = call(
            &w.st,
            Method::POST,
            "/api/v1/push",
            &other,
            Some(push("dev2")),
            &[],
        )
        .await;
        assert_eq!(s, StatusCode::OK, "another entity of the key is untouched");

        assert!(
            data::unblock_client(&w.st.store, w.cluster, w.devices_id, "dev1", ("test", None))
                .await
                .unwrap()
        );
        w.st.clients.blocks.invalidate(w.cluster);
        assert_eq!(ok().await.0, StatusCode::OK, "unblocked");

        data::revoke_api_key(&w.st.store, w.devices_id)
            .await
            .unwrap();
        w.st.cache.invalidate(w.cluster);
        let (s, _) = ok().await;
        assert_eq!(
            s,
            StatusCode::UNAUTHORIZED,
            "a revoked key takes its tokens with it"
        );
    }

    #[tokio::test]
    async fn a_narrowed_key_narrows_the_tokens_it_minted() {
        let w = world(KeyLimitsDoc::default()).await;
        let tok = token(&w, "dev1").await;
        let narrowed: Vec<Grant> = serde_json::from_value(json!([
            {"queue": "commands", "partition": "{sub}", "ops": ["consume"]}
        ]))
        .unwrap();
        data::set_key_policy(
            &w.st.store,
            w.cluster,
            w.devices_id,
            &PolicyInput {
                grants: narrowed,
                limits: KeyLimitsDoc::default(),
                token_ttl_max_s: None,
            },
            ("test", None),
        )
        .await
        .unwrap();
        w.st.cache.invalidate(w.cluster);
        let (s, _) = call(
            &w.st,
            Method::POST,
            "/api/v1/push",
            &tok,
            Some(push("dev1")),
            &[],
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        let seen = w.seen.lock().unwrap().clone();
        let scope =
            Scope::decode(seen.last().unwrap().1.get(SCOPE_HEADER).unwrap().as_bytes()).unwrap();
        assert!(
            !scope.allows(&queen_protocol::scope::Ask::Produce {
                queue: "telemetry",
                partition: "dev1"
            }),
            "the push grant is gone from the token's scope: {scope:?}"
        );
    }

    #[tokio::test]
    async fn an_expired_or_foreign_token_is_refused_with_its_reason() {
        let w = world(KeyLimitsDoc::default()).await;
        let mut claims = crate::clients::ClientClaims::new(
            &w.st.cfg.jwt_issuer,
            "dev1",
            w.devices_id,
            w.cluster,
            60,
            None,
        );
        claims.exp = claims.iat - 120;
        let expired = w.st.keys.mint_client_token(&claims).unwrap();
        let (s, v) = call(
            &w.st,
            Method::POST,
            "/api/v1/push",
            &expired,
            Some(push("dev1")),
            &[],
        )
        .await;
        assert_eq!(
            (s, &v["code"]),
            (StatusCode::UNAUTHORIZED, &json!("token_expired")),
            "{v}"
        );

        // Another cluster's id in the token: refused here, whatever it signs.
        let mut foreign = crate::clients::ClientClaims::new(
            &w.st.cfg.jwt_issuer,
            "dev1",
            w.devices_id,
            Uuid::new_v4(),
            60,
            None,
        );
        foreign.exp = foreign.iat + 60;
        let foreign = w.st.keys.mint_client_token(&foreign).unwrap();
        let (s, _) = call(
            &w.st,
            Method::POST,
            "/api/v1/push",
            &foreign,
            Some(push("dev1")),
            &[],
        )
        .await;
        assert_eq!(s, StatusCode::FORBIDDEN);

        // Signed by somebody else: one byte of the signature.
        let tok = token(&w, "dev1").await;
        let mut bad = tok.clone();
        let last = bad.pop().unwrap();
        bad.push(if last == 'A' { 'B' } else { 'A' });
        let (s, _) = call(
            &w.st,
            Method::POST,
            "/api/v1/push",
            &bad,
            Some(push("dev1")),
            &[],
        )
        .await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);

        // A session token is not a client token, prefix or not.
        let session =
            w.st.keys
                .mint_user_jwt(Uuid::new_v4(), "admin", Some(w.cluster), 60)
                .unwrap();
        let (s, _) = call(
            &w.st,
            Method::POST,
            "/api/v1/push",
            &format!("qt_{session}"),
            Some(push("dev1")),
            &[],
        )
        .await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
        // And a client token is not a session.
        let jwt = tok.strip_prefix("qt_").unwrap();
        let (s, _) = call(
            &w.st,
            Method::POST,
            "/api/v1/push",
            jwt,
            Some(push("dev1")),
            &[],
        )
        .await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn the_mint_route_refuses_what_it_must() {
        let w = world(KeyLimitsDoc::default()).await;
        let tok = token(&w, "dev1").await;
        let cases = [
            (
                tok.as_str(),
                json!({"sub": "dev2"}),
                StatusCode::FORBIDDEN,
                "a token mints nothing",
            ),
            (
                w.devices.as_str(),
                json!({"sub": "dev1", "ttlSeconds": 7200}),
                StatusCode::BAD_REQUEST,
                "over the key's ceiling",
            ),
            (
                w.devices.as_str(),
                json!({"sub": "dev/1"}),
                StatusCode::BAD_REQUEST,
                "not a sub",
            ),
            (
                w.devices.as_str(),
                json!({"sub": "dev1", "ttl": 60}),
                StatusCode::BAD_REQUEST,
                "an unknown field",
            ),
            (
                w.devices.as_str(),
                json!({"sub": "dev1", "allow": [{"queue": "q", "partition": "{sub}*", "ops": ["produce"]}]}),
                StatusCode::BAD_REQUEST,
                "a grant that would leak",
            ),
            (
                w.devices.as_str(),
                json!({"sub": "dev1", "allow": []}),
                StatusCode::BAD_REQUEST,
                "an empty allow: a token that reaches nothing",
            ),
            (
                w.devices.as_str(),
                json!({"sub": "dev1", "key": "another"}),
                StatusCode::FORBIDDEN,
                "a scoped key mints its own tokens only",
            ),
            (
                w.admin_key.as_str(),
                json!({"sub": "dev1"}),
                StatusCode::BAD_REQUEST,
                "an admin names the key",
            ),
            (
                w.admin_key.as_str(),
                json!({"sub": "dev1", "key": "nope"}),
                StatusCode::NOT_FOUND,
                "no such key",
            ),
        ];
        for (bearer, body, want, why) in cases {
            let (s, v) = mint(&w, bearer, body).await;
            assert_eq!(s, want, "{why}: {v}");
        }
        // An admin key mints for the scoped key it names, by name or by id.
        for named in ["devices".to_string(), w.devices_id.to_string()] {
            let (s, v) = mint(&w, &w.admin_key, json!({"sub": "dev9", "key": named})).await;
            assert_eq!(s, StatusCode::CREATED, "{named}: {v}");
            assert_eq!(v["key"], w.devices_id.to_string());
        }
        let (s, _) = call(&w.st, Method::GET, super::PATH, &w.devices, None, &[]).await;
        assert_eq!(s, StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn an_entity_over_its_own_rate_is_refused_here() {
        let w = world(KeyLimitsDoc {
            key_rps: None,
            key_burst: None,
            sub_rps: Some(1),
            sub_burst: Some(1),
        })
        .await;
        let tok = token(&w, "dev1").await;
        let (s, _) = call(
            &w.st,
            Method::POST,
            "/api/v1/push",
            &tok,
            Some(push("dev1")),
            &[],
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        let (s, v) = call(
            &w.st,
            Method::POST,
            "/api/v1/push",
            &tok,
            Some(push("dev1")),
            &[],
        )
        .await;
        assert_eq!(
            (s, &v["code"]),
            (StatusCode::TOO_MANY_REQUESTS, &json!("client_rate_limited")),
            "{v}"
        );
        let other = token(&w, "dev2").await;
        let (s, _) = call(
            &w.st,
            Method::POST,
            "/api/v1/push",
            &other,
            Some(push("dev2")),
            &[],
        )
        .await;
        assert_eq!(s, StatusCode::OK, "another entity has its own bucket");
    }
}
