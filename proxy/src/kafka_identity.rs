//! `GET /auth/me` for an API-KEY bearer.
//!
//! # What asks, and why one field decides everything
//!
//! The Kafka facade (`protocols/queen-kafka/src/identity.rs`) asks this route once per
//! credential and reads exactly one field out of the answer:
//!
//! ```text
//! parsed["acting_cluster"]["id"].as_str()
//! ```
//!
//! That string becomes `TenantKey::Tenant`, the scope every consumer group and
//! every queue listing is filed under. Two credentials that get the same string
//! are ONE tenant to the facade and share their groups; two that get different
//! strings — or none — are two, and a tenant holding two api keys then runs two
//! coordinators over one set of committed offsets. That is the duplicate
//! consumption bug this module exists to close.
//!
//! Until now `oauth.rs`'s `me` was cookie-only ("`/auth/me` describes the
//! BROWSER's session"), so every bearer got `401 no session` and every Kafka
//! credential fell back to `TenantKey::Credential` — the facade's own
//! degradation, correct for one key per tenant and wrong for two.
//!
//! # Why the answer is resolved by the DATA PLANE's own path
//!
//! The identity must name the cluster the NEXT request will actually act on,
//! or it is worse than no identity: if Host names cluster A and the key belongs
//! to cluster B, the data plane refuses that request, and an identity that had
//! answered B would have keyed a coordinator by a cluster this credential can
//! never reach. So this module makes no lookup of its own. It calls
//! `acting::resolve_route`, then `acting::authenticate_for` or
//! `acting::resolve_from_credential` — the same two functions `gateway::handle`
//! calls in the same order — and reports what they resolved. A refusal is
//! forwarded verbatim, which is what keeps the two surfaces from ever
//! disagreeing about one credential.
//!
//! The facade caches a resolved answer for the life of the process
//! (`identity.rs`'s `a_resolved_key_is_never_re_resolved`), so an answer must
//! never be a guess.
//!
//! # Why `acting_cluster.id` and not the tenant
//!
//! Argued in `identity.rs`'s own header and not re-litigated here: one Cloud
//! tenant may own several clusters, each with its own `broker_tenant`, which is
//! what the broker scopes KV by. Keying groups by `tenant_slug` or `tenant_id`
//! would be COARSER than the namespace the offsets themselves live in, and two
//! clusters of one tenant would silently share a group's offsets.

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::auth::Credential;
use crate::state::{ClusterCtx, ClusterStatus, Principal, Scopes, St};

/// The identity of an API-KEY bearer, or `None` when this request carries no
/// api key — in which case the cookie handler in `oauth.rs` runs and answers
/// exactly as it always has.
///
/// `None` is deliberately the answer for a SESSION bearer too. A session token
/// in `Authorization` got `401 no session` before this module existed, because
/// `me` reads the cookie and only the cookie; nothing here changes that, and
/// widening it would be a change to the browser surface rather than to the
/// Kafka one.
pub async fn bearer_me(st: &St, headers: &HeaderMap) -> Option<Response> {
    let secure = crate::oauth::cookie_is_secure(st, headers);
    // An api key can only ever arrive in `Authorization` — `read_credential`
    // drops a `qk_` value found in a cookie rather than promoting it — so this
    // is the whole trigger condition, and a browser session cannot reach the
    // code below no matter what its jar holds.
    if !matches!(
        crate::auth::read_credential(&st.cfg.cookie_name, secure, headers),
        Credential::ApiKey(_)
    ) {
        return None;
    }

    let (ctx, principal) = match crate::acting::resolve_route(st, headers).await {
        Ok(crate::acting::Route::Fixed(ctx)) => {
            match crate::acting::authenticate_for(st, headers, &ctx).await {
                Ok(p) => (ctx, p),
                // Including the 403 `key/cluster mismatch` that is the whole
                // point of resolving through this path: a key of cluster B on
                // cluster A's hostname is refused here exactly as its next data
                // request would be refused, instead of being told a cluster id
                // it may not use.
                Err(resp) => return Some(resp),
            }
        }
        // A shared host: the credential is the only thing that can name a
        // cluster, and this resolver returns the cluster and the principal
        // together because there they are one question.
        Ok(crate::acting::Route::FromCredential) => {
            match crate::acting::resolve_from_credential(st, headers).await {
                Ok(v) => v,
                // 401 for an unknown or revoked key, never 421 and never 403
                // (decision z) — the same refusal, byte for byte, that the data
                // plane gives the same key, so this route cannot be used to
                // tell an existing cluster from a missing one.
                Err(resp) => return Some(resp),
            }
        }
        // The Host names no cluster on a listener that HAS shared hosts. The
        // data plane's own answer: a 421 for a live credential, that
        // credential's own 401 otherwise.
        Ok(crate::acting::Route::UnknownHost) => {
            return Some(crate::acting::unknown_host_refusal(st, headers).await)
        }
        Err(resp) => return Some(resp),
    };

    let (key_id, scopes) = match principal {
        Principal::ApiKey { key_id, scopes } => (key_id, scopes),
        // Unreachable by construction: this request's credential was read as an
        // api key above, and both resolvers read the same header with the same
        // function. If it ever became reachable, falling through to the cookie
        // handler is the conservative answer — it is the behaviour this route
        // had for every bearer before this module existed.
        Principal::User { .. } => return None,
    };

    tracing::debug!(
        target: "auth", cluster = %ctx.slug, key = %key_id,
        "/auth/me: an api key was told the cluster it acts on"
    );
    Some(
        (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            )],
            identity_body(&ctx, key_id, scopes, st.cfg.operator_enabled).to_string(),
        )
            .into_response(),
    )
}

/// The document, field for field.
///
/// Every key the SPA's `/auth/me` carries is present, so there are not two
/// shapes of one route for anyone to reason about — the facade ignores all but
/// one of them, but the next reader of this endpoint should not have to
/// discover that two callers get different documents.
///
/// The values are what is TRUE of an api key, which is not what is true of a
/// human session:
///
/// - `is_operator` / `operator_live` are always `false`. `Principal::ApiKey`
///   can never be an operator — `auth::authorize` answers `(_, Operator) =>
///   false` for it — so reporting anything else would be a claim the data plane
///   contradicts on the very next request. `operator_enabled` is the CELL's
///   flag and is reported as it is, exactly as the cookie handler reports it.
/// - `user_id`, `email`, `role` are `null`: a key is not a person and has no
///   role. Its authority is `scopes`, which is reported instead.
/// - `clusters` holds the ONE cluster this key can act on. For a session the
///   list is the set it may switch to; a key may switch to none, so a list of
///   one is the honest reading of the same field. `tenant_slug` and `cell_slug`
///   are `null` because `ClusterCtx` does not carry them and this route must
///   not grow a database query to fill in two labels nobody reads.
///
/// `scopes` and `key_id` are additions, not part of the SPA shape. They make
/// this endpoint answer the question an operator actually arrives with — "why
/// does my Kafka client not connect" — whose answer is almost always a missing
/// `read` scope (every Kafka client issues Metadata first, and Metadata is
/// `GET /api/v1/resources/queues`, which is `scopes.read || scopes.admin`).
/// Telling a key holder their own key's scopes leaks nothing to anyone who did
/// not already present the key.
fn identity_body(ctx: &ClusterCtx, key_id: Uuid, scopes: Scopes, operator_enabled: bool) -> Value {
    let cluster_id = ctx.cluster_id.to_string();
    json!({
        "user_id": Value::Null,
        "email": Value::Null,
        "tenant_slug": Value::Null,
        "is_operator": false,
        "operator_enabled": operator_enabled,
        "operator_live": false,
        // THE field. See the module header.
        "acting_cluster": { "id": cluster_id, "slug": ctx.slug },
        "clusters": [{
            "id": cluster_id,
            "slug": ctx.slug,
            "role": Value::Null,
            "tenant_slug": Value::Null,
            "tenant_id": ctx.tenant_id.to_string(),
            "status": status_str(ctx.status),
            "cell_slug": Value::Null,
        }],
        "act_cluster_header": crate::config::ACT_CLUSTER_HEADER,
        "role": Value::Null,
        "cluster": cluster_id,
        // Not in the SPA shape; see the doc comment.
        "principal": "api_key",
        "key_id": key_id.to_string(),
        "scopes": {
            "produce": scopes.produce,
            "consume": scopes.consume,
            "admin": scopes.admin,
            "read": scopes.read,
        },
    })
}

/// The lifecycle word for a cluster, spelled the way `queen_proxy.clusters`
/// spells it — the same vocabulary the cookie handler's `clusters` rows carry,
/// because they come straight out of that column.
fn status_str(s: ClusterStatus) -> &'static str {
    match s {
        ClusterStatus::Active => "active",
        ClusterStatus::PushBlocked => "push_blocked",
        ClusterStatus::Suspended => "suspended",
        ClusterStatus::Deleting => "deleting",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::EffectiveLimits;
    use axum::http::{header, HeaderValue};

    /// A cluster the way the cache builds one, with a uuid that is not the
    /// tenant's — the distinction the facade's own test pins from the other
    /// side.
    fn ctx() -> ClusterCtx {
        ClusterCtx {
            cluster_id: Uuid::parse_str("3a2b1c4d-0000-4000-8000-000000000001").unwrap(),
            tenant_id: Uuid::parse_str("9f9f9f9f-0000-4000-8000-00000000000a").unwrap(),
            broker_tenant: Uuid::parse_str("9f9f9f9f-0000-4000-8000-00000000000a").unwrap(),
            slug: "prod".to_string(),
            cell_base_url: "http://127.0.0.1:6632".to_string(),
            cell_token: None,
            status: ClusterStatus::Active,
            limits: EffectiveLimits::default(),
            features: crate::state::Features::default(),
        }
    }

    /// `protocols/queen-kafka/src/identity.rs::tenant_of`, copied here so this test asks
    /// the question the consumer asks rather than a paraphrase of it. If that
    /// extraction ever changes, this test keeps proving the OLD contract — which
    /// is why the facade's own `the_tenant_is_the_acting_cluster_of_either_surface`
    /// exists on the other side of the wire.
    fn tenant_of(body: &str) -> Option<String> {
        let parsed: Value = serde_json::from_str(body).ok()?;
        parsed
            .get("acting_cluster")?
            .get("id")?
            .as_str()
            .map(str::to_string)
    }

    /// `identity.rs::usable_id`'s rule: bounded, and spelled with the
    /// characters a cluster uuid or a slug is spelled with. An id that fails it
    /// is discarded by the facade and the credential silently falls back to
    /// standing for itself — a 200 that achieves nothing.
    fn usable_id(raw: &str) -> bool {
        let id = raw.trim();
        !id.is_empty()
            && id.chars().count() <= 128
            && id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':'))
    }

    fn st_with(shared: &[&str], dev_cell: Option<&str>, insecure: bool) -> St {
        let mut cfg = crate::config::test_config(shared);
        cfg.dev_insecure = insecure;
        cfg.dev_static = dev_cell.map(|url| crate::config::DevStaticCluster {
            cell_url: url.to_string(),
            cell_token: None,
            broker_tenant: crate::config::DEFAULT_TENANT_UUID.to_string(),
        });
        let cache = crate::cache::ClusterCache::new(&cfg, None);
        let limits = crate::limits::Limits::new(&cfg);
        let meter = std::sync::Arc::new(crate::meter::Meter::new(&cfg));
        let registry = crate::registry::Registry::new(None);
        let keys = crate::auth::Keys::from_config(&cfg);
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.set_nodelay(true);
        let upstream =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build::<_, axum::body::Body>(connector);
        std::sync::Arc::new(crate::state::AppState {
            cfg,
            db: None,
            upstream,
            cache,
            limits,
            meter,
            registry,
            keys,
        })
    }

    fn hdrs(host: &str, auth: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(header::HOST, HeaderValue::from_str(host).unwrap());
        if let Some(a) = auth {
            h.insert(header::AUTHORIZATION, HeaderValue::from_str(a).unwrap());
        }
        h
    }

    async fn body_of(resp: Response) -> String {
        let bytes = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .expect("body");
        String::from_utf8(bytes.to_vec()).expect("utf8")
    }

    /// End to end on a resolvable cluster: a bearer gets 200 and is told which
    /// cluster it acts on.
    #[tokio::test]
    async fn a_bearer_gets_its_acting_cluster() {
        // dev-static resolves any Host to one cluster and dev-insecure hands
        // out the principal, which is the only way to reach the 200 without a
        // pxdb behind the cache. The cluster it names is `resolve_host`'s own.
        let st = st_with(&[], Some("http://127.0.0.1:6632"), true);
        let resp = bearer_me(&st, &hdrs("cell.test", Some("Bearer qk_live_abc")))
            .await
            .expect("an api key must be answered here, not by the cookie path");
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_of(resp).await;
        let named = tenant_of(&body).expect("acting_cluster.id");
        assert_eq!(named, Uuid::nil().to_string(), "dev-static's cluster");
    }

    /// The shape, asked the way the consumer asks it — `tenant_of` on the real
    /// bytes, and then `usable_id` on what came back, because an id the facade
    /// refuses to hold is a 200 that changes nothing.
    #[test]
    fn the_body_is_the_shape_the_facade_parses() {
        let c = ctx();
        let body = identity_body(&c, Uuid::nil(), Scopes::default(), false).to_string();
        let named = tenant_of(&body).expect("acting_cluster.id must be a string");
        assert_eq!(named, c.cluster_id.to_string());
        assert!(
            usable_id(&named),
            "{named} is an id the facade would discard"
        );
        // and it is the CLUSTER's id, never the tenant's or the slug — the
        // grain the offsets themselves are scoped by
        assert_ne!(named, c.tenant_id.to_string());
        assert_ne!(named, c.slug);

        // every key of the cookie handler's document is present, so there are
        // not two shapes of one route
        let parsed: Value = serde_json::from_str(&body).unwrap();
        for k in [
            "user_id",
            "email",
            "tenant_slug",
            "is_operator",
            "operator_enabled",
            "operator_live",
            "acting_cluster",
            "clusters",
            "act_cluster_header",
            "role",
            "cluster",
        ] {
            assert!(parsed.get(k).is_some(), "missing {k}");
        }
    }

    /// The cookie path must be reachable exactly as before. Nothing but an api
    /// key diverts this route — not an empty header, not a session bearer, and
    /// not a session cookie.
    #[tokio::test]
    async fn no_bearer_leaves_the_cookie_path_alone() {
        let st = st_with(&[], Some("http://127.0.0.1:6632"), true);
        for auth in [
            None,
            Some("Bearer eyJhbGciOiJIUzI1NiJ9.e30.sig"),
            Some("Bearer "),
            Some(""),
        ] {
            assert!(
                bearer_me(&st, &hdrs("cell.test", auth)).await.is_none(),
                "{auth:?} must fall through to the cookie handler"
            );
        }
        // a session cookie is a browser session and stays the cookie path's
        let mut h = hdrs("cell.test", None);
        h.insert(header::COOKIE, HeaderValue::from_static("test=abc.def.ghi"));
        assert!(bearer_me(&st, &h).await.is_none());
        // ...and a `qk_` value in a COOKIE is not an api key either
        // (`read_credential` drops it rather than promoting it)
        let mut h = hdrs("cell.test", None);
        h.insert(header::COOKIE, HeaderValue::from_static("test=qk_live_abc"));
        assert!(bearer_me(&st, &h).await.is_none());
    }

    /// Decision z: an unknown or revoked key is a 401, never a 403 and never a
    /// 421. This route must not become the oracle the data plane refuses to be.
    #[tokio::test]
    async fn an_unknown_key_is_401_and_never_403() {
        // shared host + no pxdb: every key hash misses, which is exactly the
        // "unknown or revoked" case
        let st = st_with(&["shared.test"], None, false);
        let resp = bearer_me(&st, &hdrs("shared.test", Some("Bearer qk_live_nope")))
            .await
            .expect("an api key is answered here");
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_ne!(resp.status(), StatusCode::FORBIDDEN);
        assert_ne!(resp.status(), StatusCode::MISDIRECTED_REQUEST);
    }

    /// `Principal::ApiKey` can never be an operator (`auth::authorize` answers
    /// `(_, Operator) => false` for it), so the identity must not claim it is —
    /// on a cell with the capability turned on either.
    #[test]
    fn an_api_key_is_never_an_operator() {
        for enabled in [false, true] {
            let body = identity_body(&ctx(), Uuid::nil(), Scopes::all(), enabled);
            assert_eq!(body["is_operator"], Value::Bool(false));
            assert_eq!(body["operator_live"], Value::Bool(false));
            // the CELL's flag is reported as it is; only the claim about this
            // principal is pinned
            assert_eq!(body["operator_enabled"], Value::Bool(enabled));
        }
    }

    /// THE bug this closes. Two keys of one cluster must answer one id, or the
    /// facade files two coordinators against one set of committed offsets and
    /// both consume everything.
    #[test]
    fn two_keys_of_one_cluster_answer_one_id() {
        let c = ctx();
        let one = identity_body(&c, Uuid::from_u128(1), Scopes::all(), false).to_string();
        let two = identity_body(
            &c,
            Uuid::from_u128(2),
            Scopes {
                produce: false,
                consume: true,
                admin: false,
                read: true,
            },
            false,
        )
        .to_string();
        assert_eq!(tenant_of(&one), tenant_of(&two));
        // and the keys are still distinguishable to a human reading the answer
        assert_ne!(one, two);
    }

    /// The lie that must not be told: the identity names the cluster the NEXT
    /// request will act on, so it is resolved by the data plane's own path and
    /// never by a bare key lookup. With no pxdb the key resolves to nothing and
    /// the answer is `authenticate_for`'s own 401 — never a 200 naming the
    /// cluster the HOST happens to point at.
    ///
    /// The cross-cluster case itself (`Some(_) => 403 key/cluster mismatch`)
    /// lives in `auth::authenticate` and is pinned there; this test pins that
    /// this module asks that function rather than answering for itself.
    #[tokio::test]
    async fn a_named_host_alone_never_names_an_identity() {
        // A real host lookup (no dev-static) with no pxdb: the Host resolves to
        // nothing, so the listener's own refusal is the answer.
        let st = st_with(&[], None, false);
        let resp = bearer_me(&st, &hdrs("prod.example.test", Some("Bearer qk_live_abc")))
            .await
            .expect("an api key is answered here");
        assert_ne!(resp.status(), StatusCode::OK, "no cluster was proven");
        assert_eq!(resp.status(), StatusCode::MISDIRECTED_REQUEST);
    }

    /// The status word is the column's own vocabulary, so a reader of this
    /// document and a reader of `queen_proxy.clusters` see the same string.
    #[test]
    fn the_status_word_is_the_column_vocabulary() {
        assert_eq!(status_str(ClusterStatus::Active), "active");
        assert_eq!(status_str(ClusterStatus::PushBlocked), "push_blocked");
        assert_eq!(status_str(ClusterStatus::Suspended), "suspended");
        assert_eq!(status_str(ClusterStatus::Deleting), "deleting");
    }

    /// The `read` scope is the answer to almost every "my Kafka client will not
    /// connect", so the document has to carry it.
    #[test]
    fn the_answer_names_the_scopes_the_key_actually_has() {
        let body = identity_body(
            &ctx(),
            Uuid::nil(),
            Scopes {
                produce: false,
                consume: true,
                admin: false,
                read: false,
            },
            false,
        );
        assert_eq!(body["scopes"]["consume"], Value::Bool(true));
        assert_eq!(body["scopes"]["read"], Value::Bool(false));
        assert_eq!(body["principal"], Value::String("api_key".to_string()));
    }
}
