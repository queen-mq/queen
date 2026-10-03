//! A small control-plane API over HTTP: how an operator, a provisioning
//! script or a cloud cell's agent manages the proxy's state, which is the
//! broker's replicated KV (PLAN_SINGLE_BINARY.md W3/W4):
//!
//! | route | does |
//! |---|---|
//! | `POST /api/cp/bootstrap` | `bootstrap_tenant`: tenant + cluster + admin + key |
//! | `POST /api/cp/provision` | `provision`: tenant + cluster + user (the caller's id) + role + key (the caller's hash) + overrides, one batch |
//! | `POST /api/cp/tenants` | `create_tenant` |
//! | `DELETE /api/cp/tenants/:slug` | `delete_tenant` (`?force=true` skips the `deleting` gate) |
//! | `PUT /api/cp/tenants/:slug/status` | `set_tenant_status_by_slug` |
//! | `POST /api/cp/tenants/:slug/purge` | the tenant's S3 sinks removed, then the broker's tenant purge, in-process, for every cluster of a `deleting` tenant |
//! | `GET /api/cp/clusters` | every cluster (`?plan=<code>` filters) |
//! | `POST /api/cp/clusters` | ensure a cluster (and its tenant) exists |
//! | `GET /api/cp/clusters/:slug` | the cluster with its tenant, plan, statuses and overrides |
//! | `GET /api/cp/clusters/:slug/queues` | the broker's queue listing, for the cluster's broker tenant |
//! | `POST /api/cp/clusters/:slug/configure` | the broker's `configure`, for the cluster's broker tenant |
//! | `PUT /api/cp/clusters/:slug/s3` | the cluster's S3 sink: config checked by the broker, `secretKey` sealed with the cell's `QUEEN_ENCRYPTION_KEY`, one row (`px.s3sinks`) |
//! | `GET /api/cp/clusters/:slug/s3` | the cluster's S3 sink, redacted: never the secret, sealed or not |
//! | `DELETE /api/cp/clusters/:slug/s3` | the cluster's S3 sink removed |
//! | `PUT /api/cp/clusters/:id/overrides` | `set_limit_override` (body JSON or `null`) |
//! | `PUT /api/cp/clusters/:id/status` | `set_cluster_status` |
//! | `GET /api/cp/clusters/:id/usage` | the last hour of metered minutes |
//! | `POST /api/cp/activity` | per cluster slug: last traffic, first push, last key use, retained bytes |
//! | `POST /api/cp/keys` | `ensure_api_key` (the caller hashes; the plaintext never reaches the proxy) |
//! | `DELETE /api/cp/keys/:id` | `ensure_api_key_revoked` |
//!
//! Guarded by `QUEEN_PROXY_CP_TOKEN` (header `x-queen-cp-token`, compared in
//! constant time). Unset: every route answers 404, the surface does not exist.
//!
//! The caller is an at-least-once bus, so every route is idempotent: a retry
//! of a call that already took effect answers success with the same ids.
//! Errors are the proxy's envelope `{"error", "code"}`. The broker tenant a
//! call acts on is always the cluster's own, never one a request names, and
//! the broker's default tenant and the proxy's own (where this whole state
//! lives) are refused.
//!
//! The S3 routes (`/clusters/:slug/s3`) exist only on a broker that hands the
//! proxy its sink ([`crate::s3::S3Sinks`]; otherwise 404 `s3_unavailable`).
//! A PUT body is the sink config plus `secretKey` (required while the cluster
//! has no sink, omitted or `null` afterwards to keep the stored one) and
//! `enabled` (default `true`); the config is everything else, checked by the
//! broker as the sink will read it. The secret is stored only sealed, so a
//! cell without `QUEEN_ENCRYPTION_KEY` refuses one (409
//! `encryption_required`); no answer and no error ever carries it, sealed or
//! not. A cluster or tenant being deleted gets no sink (409 `deleting`), and
//! its sinks go with the purge and the delete.

// A handler's helpers answer early with the `Response` itself (`tri!`):
// boxing it would buy nothing on a control-plane call.
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, RawQuery, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::errors::{json_error, CODE_CLUSTER_UNKNOWN};
use crate::s3::S3Sinks;
use crate::state::St;
use crate::store::data::{self, BrokerTarget, ClusterKey, DataError, Lookup};
use crate::store::schema::S3SinkDoc;

pub fn router() -> Router<St> {
    Router::new()
        .route("/bootstrap", post(bootstrap))
        .route("/provision", post(provision))
        .route("/tenants", post(create_tenant))
        .route("/tenants/:slug", delete(delete_tenant))
        .route("/tenants/:slug/status", put(set_tenant_status))
        .route("/tenants/:slug/purge", post(purge_tenant))
        .route("/clusters", get(list_clusters).post(ensure_cluster))
        .route("/clusters/:slug", get(get_cluster))
        .route("/clusters/:slug/queues", get(cluster_queues))
        .route("/clusters/:slug/configure", post(configure_queue))
        .route(
            "/clusters/:slug/s3",
            put(put_s3).get(get_s3).delete(delete_s3),
        )
        .route("/clusters/:id/overrides", put(set_overrides))
        .route("/clusters/:id/status", put(set_status))
        .route("/clusters/:id/usage", get(usage))
        .route("/activity", post(activity))
        .route("/keys", post(issue_key))
        .route("/keys/:id", delete(revoke_key))
}

/// The codes this surface answers with besides `cluster_unknown`
/// (`errors.rs`). Load-bearing: its caller switches on them.
pub const CODE_INVALID: &str = "invalid";
pub const CODE_CONFLICT: &str = "conflict";
pub const CODE_UNAVAILABLE: &str = "unavailable";
pub const CODE_NO_STORE: &str = "no_store";
pub const CODE_TENANT_UNKNOWN: &str = "tenant_unknown";
pub const CODE_KEY_UNKNOWN: &str = "key_unknown";
pub const CODE_NOT_DELETING: &str = "not_deleting";
pub const CODE_SYSTEM_TENANT: &str = "system_tenant";
/// A tenant or cluster being torn down refuses what would add to it.
pub const CODE_DELETING: &str = "deleting";
/// A tenant whose broker data is still there refuses its delete.
pub const CODE_NOT_PURGED: &str = "not_purged";
/// 404: this broker runs no S3 sink (`QUEEN_S3_EMBEDDED` is not true, or it
/// was built without one): the S3 routes do not exist here.
pub const CODE_S3_UNAVAILABLE: &str = "s3_unavailable";
/// 404: the cluster has no S3 sink.
pub const CODE_S3_UNSET: &str = "s3_unset";
/// 409: the cell has no `QUEEN_ENCRYPTION_KEY`, so it cannot store an S3
/// secret (it never stores one in clear). Nothing was written.
pub const CODE_ENCRYPTION_REQUIRED: &str = "encryption_required";

/// The most cluster slugs one `POST /api/cp/activity` may name.
pub const MAX_ACTIVITY_CLUSTERS: usize = 256;
/// The largest request body this surface reads (`413` above it).
pub const MAX_BODY_BYTES: usize = 1024 * 1024;
/// The longest S3 `secretKey` taken, in bytes (an AWS secret key is 40): with
/// the config's own cap ([`data::S3_CONFIG_MAX_BYTES`]) a sink row stays
/// under the broker's KV value ceiling once sealed.
pub const MAX_S3_SECRET_BYTES: usize = 4096;

/// How long one cluster's purge may take at the broker: it deletes every
/// partition of the tenant before it answers.
const PURGE_TIMEOUT: Duration = Duration::from_secs(300);
/// How long a passed-through broker call (queues, configure) may take.
const BROKER_TIMEOUT: Duration = Duration::from_secs(30);
/// The largest broker answer read back.
const BROKER_ANSWER_CAP: usize = 64 * 1024 * 1024;

fn token() -> Option<String> {
    std::env::var("QUEEN_PROXY_CP_TOKEN")
        .ok()
        .filter(|t| !t.trim().is_empty())
}

/// Whether `h` carries the control-plane token (`x-queen-cp-token`, or
/// `Authorization: Bearer` for a scraper that can only send that). `None`
/// when the surface is off.
pub fn operator_token_ok(h: &HeaderMap) -> Option<bool> {
    let want = token()?;
    let got = h
        .get("x-queen-cp-token")
        .and_then(|v| v.to_str().ok())
        .or_else(|| {
            h.get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
        })
        .unwrap_or("");
    let (a, b) = (want.as_bytes(), got.as_bytes());
    Some(a.len() == b.len() && a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0)
}

/// 404 when the surface is off, 401 on a wrong token.
fn guard(h: &HeaderMap) -> Result<(), Response> {
    check_token(token().as_deref(), h)
}

/// [`guard`] against `want`, the configured token.
fn check_token(want: Option<&str>, h: &HeaderMap) -> Result<(), Response> {
    let Some(want) = want else {
        return Err(crate::errors::err_404("not_found", "not found"));
    };
    let got = h
        .get("x-queen-cp-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let (a, b) = (want.as_bytes(), got.as_bytes());
    let same = a.len() == b.len() && a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0;
    if same {
        Ok(())
    } else {
        Err(crate::errors::err_401("bad control-plane token"))
    }
}

/// `?` for a handler: an `Err(response)` is the answer.
macro_rules! tri {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(r) => return r,
        }
    };
}

fn fail(e: DataError) -> Response {
    let (status, code) = match &e {
        DataError::Invalid(_) => (StatusCode::BAD_REQUEST, CODE_INVALID),
        DataError::Conflict(_) => (StatusCode::CONFLICT, CODE_CONFLICT),
        DataError::Unavailable(_) => (StatusCode::SERVICE_UNAVAILABLE, CODE_UNAVAILABLE),
        DataError::NoStore => (StatusCode::SERVICE_UNAVAILABLE, CODE_NO_STORE),
        DataError::Refused { code, .. } => (StatusCode::CONFLICT, *code),
    };
    json_error(status, code, &e.to_string())
}

fn invalid(msg: &str) -> Response {
    json_error(StatusCode::BAD_REQUEST, CODE_INVALID, msg)
}

fn answer(status: StatusCode, v: Value) -> Response {
    (status, Json(v)).into_response()
}

/// The token first, from the head alone, and only then the body, at most
/// [`MAX_BODY_BYTES`] of it: nothing of an unauthenticated request is read.
async fn guarded_body(req: Request<Body>) -> Result<Bytes, Response> {
    guard(req.headers())?;
    let over = || crate::errors::err_413("control-plane request body over 1 MiB");
    let declared = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok());
    if declared.is_some_and(|n| n > MAX_BODY_BYTES as u64) {
        return Err(over());
    }
    axum::body::to_bytes(req.into_body(), MAX_BODY_BYTES)
        .await
        .map_err(|_| over())
}

/// A JSON request body, or the 400 that says why not.
fn parse<T: DeserializeOwned>(body: &[u8]) -> Result<T, Response> {
    serde_json::from_slice(body).map_err(|e| invalid(&format!("invalid JSON body: {e}")))
}

/// A path segment that must be a uuid.
fn uuid_of(s: &str, what: &str) -> Result<Uuid, Response> {
    Uuid::parse_str(s.trim()).map_err(|_| invalid(&format!("{what} must be a uuid")))
}

/// A tenant or cluster slug from a request, through the store's one gate
/// (`data::slug_of`): a slug no row can carry is a 400 here, never a lookup.
fn slug_param(s: &str, what: &str) -> Result<String, Response> {
    data::slug_of(s).ok_or_else(|| {
        invalid(&format!(
            "{what} must be a DNS label: a-z, 0-9 and '-', at most 63 characters"
        ))
    })
}

/// The broker's default tenant and the proxy's own: no control-plane call
/// acts on either (the second holds this whole state).
fn is_system_tenant(t: &Uuid) -> bool {
    let t = t.to_string();
    t == crate::config::DEFAULT_TENANT_UUID || t == crate::store::schema::PROXY_TENANT
}

fn system_tenant(x: &BrokerTarget) -> Response {
    json_error(
        StatusCode::CONFLICT,
        CODE_SYSTEM_TENANT,
        &format!(
            "cluster {} is bound to the reserved broker tenant {}: refusing to act on it",
            x.slug, x.broker_tenant
        ),
    )
}

/// A call that acts on the broker needs the tenant header the data plane
/// sends. Without it (`QUEEN_PROXY_TENANT_HEADER=false`, the single-tenant
/// front door) every cluster's data is the broker's default tenant, which
/// this surface never acts on.
fn tenant_header_on(st: &St) -> Result<(), Response> {
    if st.cfg.send_tenant_header {
        return Ok(());
    }
    Err(json_error(
        StatusCode::CONFLICT,
        CODE_SYSTEM_TENANT,
        "QUEEN_PROXY_TENANT_HEADER is off: every cluster is the broker's default tenant, \
         which the control plane never acts on",
    ))
}

fn tenant_unknown(slug: &str) -> Response {
    crate::errors::err_404(CODE_TENANT_UNKNOWN, &format!("no tenant {slug}"))
}

fn not_deleting(t: &crate::store::schema::TenantDoc) -> Response {
    json_error(
        StatusCode::CONFLICT,
        CODE_NOT_DELETING,
        &format!(
            "tenant {} is {}, not deleting: set its status to deleting first",
            t.slug, t.status
        ),
    )
}

#[derive(Deserialize)]
struct BootstrapIn {
    tenant_slug: String,
    tenant_name: Option<String>,
    cluster_slug: Option<String>,
    plan: Option<String>,
    admin_email: String,
    password: Option<String>,
    key_name: Option<String>,
}

async fn bootstrap(State(st): State<St>, req: Request<Body>) -> Response {
    let raw = tri!(guarded_body(req).await);
    let b: BootstrapIn = tri!(parse(&raw));
    let cell = self_cell(&st).await;
    let a = data::Bootstrap {
        cluster_slug: b.cluster_slug.unwrap_or_else(|| b.tenant_slug.clone()),
        tenant_slug: b.tenant_slug,
        tenant_name: b.tenant_name,
        plan_code: b.plan.unwrap_or_else(|| "free".into()),
        cell,
        admin_email: b.admin_email,
        password: b.password,
        key_name: b.key_name,
    };
    match data::bootstrap_tenant(&st.store, &a).await {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => fail(e),
    }
}

#[derive(Deserialize)]
struct ProvisionIn {
    tenant_slug: String,
    tenant_name: Option<String>,
    cluster_slug: String,
    plan: String,
    user_id: Uuid,
    email: String,
    role: String,
    key_name: String,
    key_hash: String,
    scopes: Vec<String>,
    /// Absent or `null`: `{}`.
    #[serde(default)]
    overrides: Value,
}

/// The 1.x `tenant.provision` in one call (`data::provision`).
async fn provision(State(st): State<St>, req: Request<Body>) -> Response {
    let raw = tri!(guarded_body(req).await);
    let p: ProvisionIn = tri!(parse(&raw));
    // A cluster this call creates goes on this node's own cell, which the node
    // seeds at boot together with the plans: until then, a retry.
    let Some(cell) = self_cell(&st).await else {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            CODE_UNAVAILABLE,
            "the proxy's state is not seeded yet (no cell `local`): retry",
        );
    };
    let a = data::Provision {
        tenant_slug: p.tenant_slug,
        tenant_name: p.tenant_name,
        cluster_slug: p.cluster_slug,
        plan_code: p.plan,
        cell: Some(cell),
        user_id: p.user_id,
        email: p.email,
        role: p.role,
        key_name: p.key_name,
        key_hash: p.key_hash,
        scopes: p.scopes,
        overrides: if p.overrides.is_null() {
            json!({})
        } else {
            p.overrides
        },
    };
    match data::provision(&st.store, &a).await {
        Ok(v) => {
            if let Some(c) = v["cluster_id"]
                .as_str()
                .and_then(|c| Uuid::parse_str(c).ok())
            {
                crate::store::web::invalidate_local(&st, &[c]);
            }
            tracing::info!(
                target: "cp", tenant = %a.tenant_slug, cluster = %a.cluster_slug,
                created = %v["created"], "tenancy provisioned"
            );
            answer(StatusCode::OK, v)
        }
        Err(e) => fail(e),
    }
}

#[derive(Deserialize)]
struct TenantIn {
    slug: String,
    name: Option<String>,
}

async fn create_tenant(State(st): State<St>, req: Request<Body>) -> Response {
    let raw = tri!(guarded_body(req).await);
    let t: TenantIn = tri!(parse(&raw));
    let name = t.name.unwrap_or_else(|| t.slug.clone());
    match data::create_tenant(&st.store, &t.slug, &name).await {
        Ok(id) => (StatusCode::CREATED, Json(json!({"id": id}))).into_response(),
        Err(e) => fail(e),
    }
}

/// `{status}` with `active | grace | suspended | deleting`.
async fn set_tenant_status(
    State(st): State<St>,
    Path(slug): Path<String>,
    req: Request<Body>,
) -> Response {
    let raw = tri!(guarded_body(req).await);
    let slug = tri!(slug_param(&slug, "tenant slug"));
    let s: StatusIn = tri!(parse(&raw));
    match data::set_tenant_status_by_slug(&st.store, &slug, &s.status).await {
        Ok(Some((t, clusters))) => {
            let ids: Vec<Uuid> = clusters.iter().map(|c| c.id).collect();
            crate::store::web::invalidate_local(&st, &ids);
            tracing::info!(target: "cp", tenant = %t.slug, status = %t.status, "tenant status set");
            let clusters: Vec<Value> = clusters
                .iter()
                .map(|c| json!({"id": c.id, "slug": c.slug, "broker_tenant_uuid": c.broker_tenant_uuid}))
                .collect();
            answer(
                StatusCode::OK,
                json!({"ok": true, "tenant_id": t.id, "clusters": clusters}),
            )
        }
        Ok(None) => tenant_unknown(&slug),
        Err(e) => fail(e),
    }
}

/// The broker's own tenant purge (`DELETE /api/v1/resources/tenant` on the
/// router this proxy runs inside, which the data plane blocks for every
/// client) for each cluster of a `deleting` tenant. Safe to repeat, and
/// repeated until `done`: a cluster is done only on a pass the broker
/// confirms (`done: true`) that found nothing left to delete
/// ([`purge_verdict`]), so the pass that deletes is followed by one that
/// proves it.
async fn purge_tenant(State(st): State<St>, h: HeaderMap, Path(slug): Path<String>) -> Response {
    tri!(guard(&h));
    let slug = tri!(slug_param(&slug, "tenant slug"));
    let t = match data::tenant_by_slug(&st.store, &slug).await {
        Ok(Some(t)) => t,
        Ok(None) => {
            return answer(
                StatusCode::OK,
                json!({"existed": false, "done": true, "clusters": []}),
            )
        }
        Err(e) => return fail(e),
    };
    if t.status != "deleting" {
        return not_deleting(&t);
    }
    tri!(tenant_header_on(&st));
    let found = match data::broker_targets_of_tenant(&st.store, t.id).await {
        Ok(v) => v,
        Err(e) => return fail(e),
    };
    // Checked for every cluster before any is purged: a tenancy bound to a
    // reserved tenant is an operator's problem, never half a purge.
    if let Some(x) = found
        .targets
        .iter()
        .find(|x| is_system_tenant(&x.broker_tenant))
    {
        return system_tenant(x);
    }
    // The tenant's S3 sinks stop before its data goes: a sink left running
    // would keep reading, and leasing in, a tenant being wiped (a `deleting`
    // tenant gets no new one). Every pass removes whatever is there.
    let ids: Vec<Uuid> = found
        .targets
        .iter()
        .map(|x| x.cluster_id)
        .chain(found.missing.iter().copied())
        .collect();
    let removed = match data::delete_s3_sinks(&st.store, &ids).await {
        Ok(n) => n,
        Err(e) => return fail(e),
    };
    if removed > 0 {
        tracing::info!(target: "cp", tenant = %t.slug, removed, "tenant purge: s3 sinks removed");
    }
    // A cluster the tenant's index lists but whose document is gone has a
    // broker tenant nobody knows any more: nothing can vouch for it.
    let mut all_done = found.missing.is_empty();
    let mut clusters = Vec::with_capacity(found.targets.len());
    for x in &found.targets {
        // The tenant both ways: the header scopes the call like the tenant's
        // own traffic, and the query names the purge's target in the request
        // line itself.
        let path = format!("/api/v1/resources/tenant?tenant={}", x.broker_tenant);
        let (done, deleted, error) = purge_verdict(
            broker_call(&st, x, Method::DELETE, &path, Bytes::new(), PURGE_TIMEOUT).await,
        );
        all_done &= done;
        tracing::info!(
            target: "cp", tenant = %t.slug, cluster = %x.slug, broker_tenant = %x.broker_tenant,
            done, partitions_deleted = ?deleted, error = ?error, "tenant purge"
        );
        let mut c = json!({
            "slug": x.slug,
            "broker_tenant_uuid": x.broker_tenant,
            "done": done,
            "partitions_deleted": deleted,
        });
        if let Some(e) = error {
            c["error"] = json!(e);
        }
        clusters.push(c);
    }
    let mut out = json!({"existed": true, "done": all_done, "clusters": clusters});
    if !found.missing.is_empty() {
        tracing::warn!(
            target: "cp", tenant = %t.slug, missing = ?found.missing,
            "tenant purge: clusters without a document; not done"
        );
        out["missing_clusters"] = json!(found.missing);
    }
    answer(StatusCode::OK, out)
}

/// Whether the broker data of every cluster of `t` is gone: each broker
/// tenant lists no queue (the broker's purge drops every queue's metadata
/// first). A listing that cannot be read is a 503, a cluster bound to a
/// reserved tenant a 409 `system_tenant`, as for the purge. A cluster whose
/// document is gone is not asked: its broker tenant is unknown, and the
/// delete is what finishes such a cut-short cascade.
async fn purged(st: &St, t: &crate::store::schema::TenantDoc) -> Result<(), Response> {
    tenant_header_on(st)?;
    let found = data::broker_targets_of_tenant(&st.store, t.id)
        .await
        .map_err(fail)?;
    if let Some(x) = found
        .targets
        .iter()
        .find(|x| is_system_tenant(&x.broker_tenant))
    {
        return Err(system_tenant(x));
    }
    for x in &found.targets {
        let path = "/api/v1/resources/queues?stats=cached";
        let listed = match broker_call(st, x, Method::GET, path, Bytes::new(), BROKER_TIMEOUT).await
        {
            Ok(a) if a.status.is_success() => serde_json::from_slice::<Value>(&a.body)
                .ok()
                .and_then(|v| v.get("queues").and_then(Value::as_array).map(Vec::len)),
            _ => None,
        };
        match listed {
            Some(0) => {}
            Some(n) => {
                return Err(json_error(
                    StatusCode::CONFLICT,
                    CODE_NOT_PURGED,
                    &format!(
                        "cluster {} still lists {n} queue(s) at the broker: purge the tenant first",
                        x.slug
                    ),
                ))
            }
            None => {
                let msg = format!(
                    "cluster {}: the broker's queue listing is unreadable; retry",
                    x.slug
                );
                return Err(json_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    CODE_UNAVAILABLE,
                    &msg,
                ));
            }
        }
    }
    Ok(())
}

/// One cluster's purge pass -> `(done, partitions_deleted, error)`. Done only
/// when the broker answered 2xx with `done: true` and deleted nothing on this
/// pass: a pass that deleted partitions, an answer without a readable `done`,
/// or a body that is not JSON all leave the cluster to the next pass.
fn purge_verdict(r: Result<BrokerAnswer, BrokerError>) -> (bool, Option<i64>, Option<String>) {
    match r {
        Ok(a) if a.status.is_success() => {
            let v: Value = serde_json::from_slice(&a.body).unwrap_or(Value::Null);
            let deleted = v.get("partitionsDeleted").and_then(Value::as_i64);
            let confirmed = v.get("done").and_then(Value::as_bool) == Some(true);
            let error = (!confirmed).then(|| {
                format!(
                    "broker answered {} without done: {}",
                    a.status.as_u16(),
                    snippet(&a.body)
                )
            });
            (confirmed && deleted == Some(0), deleted, error)
        }
        Ok(a) => (
            false,
            None,
            Some(format!(
                "broker answered {}: {}",
                a.status.as_u16(),
                snippet(&a.body)
            )),
        ),
        Err(e) => (false, None, Some(e.to_string())),
    }
}

/// `data::delete_tenant`: refused (409 `not_deleting`) unless the tenant is
/// `deleting`, and (409 `not_purged`) while any of its clusters' broker
/// tenants still lists a queue, unless `?force=true`, which skips both.
async fn delete_tenant(
    State(st): State<St>,
    h: HeaderMap,
    Path(slug): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    tri!(guard(&h));
    let slug = tri!(slug_param(&slug, "tenant slug"));
    let force = match q
        .get("force")
        .map(|v| v.trim().to_ascii_lowercase())
        .as_deref()
    {
        None | Some("false") => false,
        Some("true") => true,
        Some(_) => return invalid("force must be true or false"),
    };
    let t = match data::tenant_by_slug(&st.store, &slug).await {
        Ok(Some(t)) => t,
        Ok(None) => return answer(StatusCode::OK, json!({"existed": false, "clusters": []})),
        Err(e) => return fail(e),
    };
    if t.status != "deleting" && !force {
        return not_deleting(&t);
    }
    if !force {
        tri!(purged(&st, &t).await);
    }
    match data::delete_tenant(&st.store, t.id, force).await {
        Ok(v) if v["existed"] == json!(true) => {
            let clusters: Vec<Value> = v["clusters"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|c| {
                            json!({
                                "cluster_id": c["cluster_id"],
                                "slug": c["slug"],
                                "broker_tenant_uuid": c["broker_tenant_uuid"],
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            let ids: Vec<Uuid> = clusters
                .iter()
                .filter_map(|c| {
                    c["cluster_id"]
                        .as_str()
                        .and_then(|c| Uuid::parse_str(c).ok())
                })
                .collect();
            crate::store::web::invalidate_local(&st, &ids);
            tracing::info!(target: "cp", tenant = %t.slug, forced = force, clusters = ids.len(), "tenant deleted");
            answer(
                StatusCode::OK,
                json!({"existed": true, "clusters": clusters}),
            )
        }
        // Deleted by another call between the read above and this one.
        Ok(_) => answer(StatusCode::OK, json!({"existed": false, "clusters": []})),
        // The store's own gate: the status moved meanwhile.
        Err(DataError::Invalid(m)) => json_error(StatusCode::CONFLICT, CODE_NOT_DELETING, &m),
        Err(e) => fail(e),
    }
}

/// This single-binary node's own cell (`local`), when there is one.
async fn self_cell(st: &St) -> Option<Uuid> {
    let kv = st.store.kv()?;
    crate::store::kv::get::<Uuid>(
        kv.as_ref(),
        crate::store::schema::ns::CELL_SLUG,
        &crate::store::schema::key(crate::store::seed::SELF_CELL),
    )
    .await
    .ok()
    .flatten()
    .map(|d| d.value)
}

async fn tenant_id(st: &St, slug: &str) -> Option<Uuid> {
    let kv = st.store.kv()?;
    crate::store::kv::get::<Uuid>(
        kv.as_ref(),
        crate::store::schema::ns::TENANT_SLUG,
        &crate::store::schema::key(slug),
    )
    .await
    .ok()
    .flatten()
    .map(|d| d.value)
}

#[derive(Deserialize)]
struct ClusterIn {
    tenant_slug: String,
    tenant_name: Option<String>,
    slug: String,
    plan: Option<String>,
    cell_id: Option<Uuid>,
}

async fn ensure_cluster(State(st): State<St>, req: Request<Body>) -> Response {
    let raw = tri!(guarded_body(req).await);
    let c: ClusterIn = tri!(parse(&raw));
    let slug = tri!(slug_param(&c.slug, "slug"));
    let tenant_slug = tri!(slug_param(&c.tenant_slug, "tenant_slug"));
    let key = ClusterKey::Slug(slug.clone());
    match data::cluster_row(&st.store, &key).await {
        Lookup::Found(row) => return (StatusCode::OK, Json(row)).into_response(),
        Lookup::Unavailable => return fail(DataError::Unavailable("store".into())),
        Lookup::Absent => {}
    }
    let tenant = match tenant_id(&st, &tenant_slug).await {
        Some(t) => t,
        None => {
            let name = c.tenant_name.clone().unwrap_or_else(|| tenant_slug.clone());
            match data::create_tenant(&st.store, &tenant_slug, &name).await {
                Ok(id) => id,
                // Created concurrently: read it back.
                Err(DataError::Conflict(_)) => match tenant_id(&st, &tenant_slug).await {
                    Some(t) => t,
                    None => return fail(DataError::Unavailable("tenant vanished".into())),
                },
                Err(e) => return fail(e),
            }
        }
    };
    let Some(cell) = c.cell_id.or(self_cell(&st).await) else {
        return fail(DataError::Invalid("no cell: pass cell_id".into()));
    };
    let plan = c.plan.unwrap_or_else(|| "free".into());
    match data::create_cluster(&st.store, tenant, &slug, &plan, cell).await {
        Ok(_) | Err(DataError::Conflict(_)) => {}
        Err(e) => return fail(e),
    }
    match data::cluster_row(&st.store, &key).await {
        Lookup::Found(row) => (StatusCode::CREATED, Json(row)).into_response(),
        _ => fail(DataError::Unavailable("cluster not readable yet".into())),
    }
}

/// Every cluster: `{clusters: [ClusterRow]}`, `?plan=<code>` keeps one plan's.
async fn list_clusters(
    State(st): State<St>,
    h: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    tri!(guard(&h));
    let plan = q.get("plan").map(|p| p.trim()).filter(|p| !p.is_empty());
    match data::list_clusters(&st.store, plan).await {
        Ok(rows) => answer(StatusCode::OK, json!({"clusters": rows})),
        Err(e) => fail(e),
    }
}

async fn get_cluster(State(st): State<St>, h: HeaderMap, Path(slug): Path<String>) -> Response {
    tri!(guard(&h));
    let slug = tri!(slug_param(&slug, "cluster slug"));
    match data::cluster_row(&st.store, &ClusterKey::Slug(slug)).await {
        Lookup::Found(row) => (StatusCode::OK, Json(row)).into_response(),
        Lookup::Absent => crate::errors::err_404(CODE_CLUSTER_UNKNOWN, "no such cluster"),
        Lookup::Unavailable => fail(DataError::Unavailable("store".into())),
    }
}

/// The cluster named `slug`, gated by nothing but its existence: what a route
/// that only reads or removes the proxy's own rows of a cluster resolves.
async fn cluster_of(st: &St, slug: &str) -> Result<BrokerTarget, Response> {
    let slug = slug_param(slug, "cluster slug")?;
    match data::broker_target(&st.store, &slug).await {
        Ok(Some(x)) => Ok(x),
        Ok(None) => Err(crate::errors::err_404(
            CODE_CLUSTER_UNKNOWN,
            "no such cluster",
        )),
        Err(e) => Err(fail(e)),
    }
}

/// The cluster named `slug`, as a broker tenant this surface may act on.
async fn target_of(st: &St, slug: &str) -> Result<BrokerTarget, Response> {
    let x = cluster_of(st, slug).await?;
    tenant_header_on(st)?;
    if is_system_tenant(&x.broker_tenant) {
        return Err(system_tenant(&x));
    }
    Ok(x)
}

/// The broker's `GET /api/v1/resources/queues` for the cluster's broker
/// tenant, its query string passed along, its answer passed back.
async fn cluster_queues(
    State(st): State<St>,
    h: HeaderMap,
    Path(slug): Path<String>,
    RawQuery(q): RawQuery,
) -> Response {
    tri!(guard(&h));
    let x = tri!(target_of(&st, &slug).await);
    let path = match q.filter(|q| !q.is_empty()) {
        Some(q) => format!("/api/v1/resources/queues?{q}"),
        None => "/api/v1/resources/queues".to_string(),
    };
    relay(broker_call(&st, &x, Method::GET, &path, Bytes::new(), BROKER_TIMEOUT).await)
}

/// The broker's `POST /api/v1/configure` for the cluster's broker tenant: the
/// body (`{queue, options}`) passed through, the answer passed back. A slug
/// no cluster has is a 404; this never creates one.
async fn configure_queue(
    State(st): State<St>,
    Path(slug): Path<String>,
    req: Request<Body>,
) -> Response {
    let raw = tri!(guarded_body(req).await);
    let v: Value = tri!(parse(&raw));
    if v.get("queue")
        .and_then(Value::as_str)
        .is_none_or(|q| q.trim().is_empty())
    {
        return invalid("configure: queue must be a non-empty string");
    }
    let x = tri!(target_of(&st, &slug).await);
    // A wipe in progress gets no queue back: a configure creates the queue's
    // metadata at the broker, which the purge has just removed.
    if x.deleting() {
        return json_error(
            StatusCode::CONFLICT,
            CODE_DELETING,
            &format!(
                "cluster {} or its tenant is deleting: nothing is configured during a wipe",
                x.slug
            ),
        );
    }
    relay(
        broker_call(
            &st,
            &x,
            Method::POST,
            "/api/v1/configure",
            raw,
            BROKER_TIMEOUT,
        )
        .await,
    )
}

// ---------------------------------------------------------------------------
// a cluster's S3 sink
// ---------------------------------------------------------------------------

/// The broker's S3 sink hook, or the 404 that says this broker has none.
fn s3_hook(st: &St) -> Result<Arc<dyn S3Sinks>, Response> {
    st.s3.clone().ok_or_else(|| {
        crate::errors::err_404(
            CODE_S3_UNAVAILABLE,
            "this broker runs no S3 sink (QUEEN_S3_EMBEDDED is not true, or it was built without \
             one): no cluster can mirror its \
             queues to S3 here",
        )
    })
}

/// A `PUT /clusters/:slug/s3` body, split: the sink config (everything but
/// `secretKey` and `enabled`), the secret, the switch.
struct S3In {
    config: Value,
    secret: Option<String>,
    enabled: bool,
}

/// [`S3In`] from a request body. No refusal here quotes a value of the body:
/// one of them is the secret.
fn s3_in(raw: &[u8]) -> Result<S3In, Response> {
    let Value::Object(mut o) = parse::<Value>(raw)? else {
        return Err(invalid(
            "s3: the body must be a JSON object: the sink config, secretKey and enabled",
        ));
    };
    let secret = match o.remove("secretKey") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s.len() > MAX_S3_SECRET_BYTES => {
            return Err(invalid(&format!(
                "s3: secretKey is longer than {MAX_S3_SECRET_BYTES} bytes"
            )))
        }
        Some(Value::String(s)) if !s.trim().is_empty() => Some(s),
        Some(_) => {
            return Err(invalid(
                "s3: secretKey must be a non-empty string (omit it to keep the stored one)",
            ))
        }
    };
    let enabled = match o.remove("enabled") {
        None | Some(Value::Null) => true,
        Some(Value::Bool(b)) => b,
        Some(_) => return Err(invalid("s3: enabled must be true or false")),
    };
    let config = Value::Object(o);
    data::check_s3_config(&config).map_err(fail)?;
    Ok(S3In {
        config,
        secret,
        enabled,
    })
}

/// What the S3 routes answer for a sink: never its secret, sealed or not.
fn s3_view(slug: &str, d: &S3SinkDoc) -> Value {
    json!({
        "cluster": slug,
        "tenant": d.broker_tenant,
        "enabled": d.enabled,
        "config": d.config,
        "secretKeySet": !d.secret_key_sealed.is_empty(),
        "updatedAt": crate::store::web::utc_iso(d.updated_at_us),
    })
}

/// 409 `encryption_required`: the seal refused, the cell has no
/// `QUEEN_ENCRYPTION_KEY`. The hook's sentence is the answer when it names
/// the key, else it rides along in ours; either way with the secret scrubbed
/// out of it should it be there.
fn encryption_required(reason: &str, secret: &str) -> Response {
    let reason = if secret.is_empty() {
        reason.to_string()
    } else {
        reason.replace(secret, "***")
    };
    let msg = if reason.contains("QUEEN_ENCRYPTION_KEY") {
        reason
    } else {
        format!(
            "this cell has no QUEEN_ENCRYPTION_KEY, and an S3 secret is only ever stored sealed \
             with it: set QUEEN_ENCRYPTION_KEY (the same on every node) and retry ({reason})"
        )
    };
    json_error(StatusCode::CONFLICT, CODE_ENCRYPTION_REQUIRED, &msg)
}

/// `PUT /clusters/:slug/s3`: set the cluster's S3 sink (module header) ->
/// 200 with the redacted sink. A repeat leaves the same sink; one with a
/// secret re-seals it and moves `updatedAt` (a new secret cannot be told from
/// a repeated one, and the broker rebuilds a sink whose row moved), one
/// without writes nothing and answers the very same.
async fn put_s3(State(st): State<St>, Path(slug): Path<String>, req: Request<Body>) -> Response {
    let raw = tri!(guarded_body(req).await);
    let hook = tri!(s3_hook(&st));
    let b = tri!(s3_in(&raw));
    let x = tri!(target_of(&st, &slug).await);
    // A wipe in progress gets no sink: it would read a tenant being purged.
    if x.deleting() {
        return json_error(
            StatusCode::CONFLICT,
            CODE_DELETING,
            &format!(
                "cluster {} or its tenant is deleting: no S3 sink is set during a wipe",
                x.slug
            ),
        );
    }
    if let Err(m) = hook.validate(&x.broker_tenant.to_string(), &b.config) {
        return invalid(&m);
    }
    let sealed = match &b.secret {
        Some(s) => match hook.seal(s) {
            Ok(v) => Some(v),
            Err(e) => return encryption_required(&e, s),
        },
        None => None,
    };
    let secret = if sealed.is_some() { "set" } else { "kept" };
    let put = data::S3SinkPut {
        enabled: b.enabled,
        config: b.config,
        secret_key_sealed: sealed,
    };
    match data::put_s3_sink(&st.store, x.cluster_id, &put).await {
        Ok(Some(d)) => {
            tracing::info!(
                target: "cp", cluster = %x.slug, broker_tenant = %d.broker_tenant,
                enabled = d.enabled, secret, "s3 sink set"
            );
            answer(StatusCode::OK, s3_view(&x.slug, &d))
        }
        // Deleted by another call since the lookup above.
        Ok(None) => crate::errors::err_404(CODE_CLUSTER_UNKNOWN, "no such cluster"),
        Err(e) => fail(e),
    }
}

/// `GET /clusters/:slug/s3`: the cluster's S3 sink, redacted; 404 `s3_unset`
/// when it has none.
async fn get_s3(State(st): State<St>, h: HeaderMap, Path(slug): Path<String>) -> Response {
    tri!(guard(&h));
    tri!(s3_hook(&st));
    let x = tri!(cluster_of(&st, &slug).await);
    match data::s3_sink(&st.store, x.cluster_id).await {
        Ok(Some(d)) => answer(StatusCode::OK, s3_view(&x.slug, &d)),
        Ok(None) => {
            crate::errors::err_404(CODE_S3_UNSET, &format!("cluster {} has no S3 sink", x.slug))
        }
        Err(e) => fail(e),
    }
}

/// `DELETE /clusters/:slug/s3` -> 200 `{cluster, removed}`, `removed: false`
/// when there was no sink (a retry). A wipe in progress may remove one too.
async fn delete_s3(State(st): State<St>, h: HeaderMap, Path(slug): Path<String>) -> Response {
    tri!(guard(&h));
    tri!(s3_hook(&st));
    let x = tri!(cluster_of(&st, &slug).await);
    match data::delete_s3_sink(&st.store, x.cluster_id).await {
        Ok(removed) => {
            if removed {
                tracing::info!(
                    target: "cp", cluster = %x.slug, broker_tenant = %x.broker_tenant,
                    "s3 sink removed"
                );
            }
            answer(
                StatusCode::OK,
                json!({"cluster": x.slug, "removed": removed}),
            )
        }
        Err(e) => fail(e),
    }
}

async fn set_overrides(
    State(st): State<St>,
    Path(id): Path<String>,
    req: Request<Body>,
) -> Response {
    let raw = tri!(guarded_body(req).await);
    let id = tri!(uuid_of(&id, "cluster id"));
    let v: Value = tri!(parse(&raw));
    let ov = if v.is_null() { None } else { Some(&v) };
    match data::set_limit_override(&st.store, id, ov).await {
        Ok(()) => (StatusCode::OK, Json(json!({"ok": true}))).into_response(),
        Err(e) => fail(e),
    }
}

#[derive(Deserialize)]
struct StatusIn {
    status: String,
}

async fn set_status(State(st): State<St>, Path(id): Path<String>, req: Request<Body>) -> Response {
    let raw = tri!(guarded_body(req).await);
    let id = tri!(uuid_of(&id, "cluster id"));
    let s: StatusIn = tri!(parse(&raw));
    match data::set_cluster_status(&st.store, id, &s.status).await {
        Ok(()) => (StatusCode::OK, Json(json!({"ok": true}))).into_response(),
        Err(e) => fail(e),
    }
}

#[derive(Deserialize)]
struct ActivityIn {
    clusters: Vec<String>,
}

/// The 1.x `tenant.activity`: per requested slug, in request order, whether
/// the cluster exists and, when it does, the newest minute with traffic
/// (push, delivery, txn or read; summed over nodes), the oldest kept minute
/// with pushed messages, the newest use of any of its keys, and the storage
/// registry's last measured total. Absent data is `null`, never 0.
async fn activity(State(st): State<St>, req: Request<Body>) -> Response {
    let raw = tri!(guarded_body(req).await);
    let a: ActivityIn = tri!(parse(&raw));
    if a.clusters.len() > MAX_ACTIVITY_CLUSTERS {
        return invalid(&format!(
            "at most {MAX_ACTIVITY_CLUSTERS} clusters per call, got {}",
            a.clusters.len()
        ));
    }
    let found = match data::activity_clusters(&st.store, &a.clusters).await {
        Ok(f) => f,
        Err(e) => return fail(e),
    };
    let mut out = Vec::with_capacity(found.len());
    for (slug, f) in a.clusters.iter().zip(found) {
        let Some((id, key_used)) = f else {
            out.push(json!({
                "slug": slug, "found": false, "cluster_id": null, "last_activity_at": null,
                "first_push_at": null, "last_key_used_at": null, "retained_bytes": null,
            }));
            continue;
        };
        let act = match crate::store::usage::cluster_activity(&st.store, id).await {
            Ok(a) => a,
            Err(e) => return fail(DataError::Unavailable(e)),
        };
        out.push(json!({
            "slug": slug,
            "found": true,
            "cluster_id": id,
            "last_activity_at": act.last_traffic_us.map(crate::store::usage::iso_minute),
            "first_push_at": act.first_push_us.map(crate::store::usage::iso_minute),
            "last_key_used_at": key_used.map(crate::store::usage::iso_minute),
            "retained_bytes": st.registry.retained_total(id),
        }));
    }
    answer(StatusCode::OK, json!({"clusters": out}))
}

#[derive(Deserialize)]
struct KeyIn {
    cluster_id: Uuid,
    name: String,
    key_hash: String,
    scopes: Vec<String>,
}

/// 201 `{id, existed: false}` for a new key; 200 `{id, existed: true}` when
/// this cluster already holds a live key with the hash (a retry).
async fn issue_key(State(st): State<St>, req: Request<Body>) -> Response {
    let raw = tri!(guarded_body(req).await);
    let k: KeyIn = tri!(parse(&raw));
    match data::ensure_api_key(&st.store, k.cluster_id, &k.name, &k.key_hash, &k.scopes).await {
        Ok((id, false)) => answer(StatusCode::CREATED, json!({"id": id, "existed": false})),
        Ok((id, true)) => answer(StatusCode::OK, json!({"id": id, "existed": true})),
        Err(e) => fail(e),
    }
}

/// 200 `{ok: true, already_revoked}`; 404 `key_unknown`.
async fn revoke_key(State(st): State<St>, h: HeaderMap, Path(id): Path<String>) -> Response {
    tri!(guard(&h));
    let id = tri!(uuid_of(&id, "key id"));
    match data::ensure_api_key_revoked(&st.store, id).await {
        Ok(Some(already)) => answer(
            StatusCode::OK,
            json!({"ok": true, "already_revoked": already}),
        ),
        Ok(None) => crate::errors::err_404(CODE_KEY_UNKNOWN, &format!("no key {id}")),
        Err(e) => fail(e),
    }
}

/// The last hour of a cluster's metered minutes, summed over nodes (what
/// billing and the smoke's meter check read).
async fn usage(State(st): State<St>, h: HeaderMap, Path(id): Path<String>) -> Response {
    tri!(guard(&h));
    let id = tri!(uuid_of(&id, "cluster id"));
    match crate::store::web::usage_minutes(&st.store, id, 1).await {
        Ok(rows) => {
            let rows: Vec<Value> = rows
                .into_iter()
                .map(|r| {
                    json!({"minute": r.minute, "op": r.op, "reqs": r.reqs, "msgs": r.msgs,
                                "bytes_in": r.bytes_in, "bytes_out": r.bytes_out})
                })
                .collect();
            (StatusCode::OK, Json(json!({"minutes": rows}))).into_response()
        }
        Err(e) => json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            CODE_UNAVAILABLE,
            &format!("{e:?}"),
        ),
    }
}

// ---------------------------------------------------------------------------
// calls to the broker itself
// ---------------------------------------------------------------------------

/// What the broker answered a call.
struct BrokerAnswer {
    status: StatusCode,
    content_type: Option<HeaderValue>,
    body: Bytes,
}

#[derive(Debug)]
enum BrokerError {
    /// No answer inside the deadline (the call may still complete).
    Timeout,
    /// The call could not be made, or its answer not read.
    Failed(String),
}

impl std::fmt::Display for BrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BrokerError::Timeout => write!(f, "broker call timed out"),
            BrokerError::Failed(m) => write!(f, "broker call failed: {m}"),
        }
    }
}

/// One call to the broker this proxy runs inside (or, over HTTP, a cell's)
/// for `x`'s broker tenant, through the data plane's own upstream and with
/// the tenant header the gateway sets, so the broker scopes it exactly as it
/// scopes that tenant's own traffic (the routes refuse before this while the
/// header is off: [`tenant_header_on`]). Nothing of the caller's request
/// travels with it but the body and query a route passes on.
async fn broker_call(
    st: &St,
    x: &BrokerTarget,
    method: Method,
    path_q: &str,
    body: Bytes,
    limit: Duration,
) -> Result<BrokerAnswer, BrokerError> {
    // In the single binary the broker is this process: its router matches on
    // the path alone (upstream.rs).
    let uri = if st.upstream.in_process() {
        path_q.to_string()
    } else {
        let Some(base) = x.base_url.as_deref() else {
            return Err(BrokerError::Failed(format!(
                "cluster {} has no cell",
                x.slug
            )));
        };
        format!("{}{}", base.trim_end_matches('/'), path_q)
    };
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .header(crate::config::TENANT_HEADER, x.broker_tenant.to_string())
        .header(header::ACCEPT, "application/json");
    if !body.is_empty() {
        req = req.header(header::CONTENT_TYPE, "application/json");
    }
    if let Some(secret) = &x.cell_secret {
        req = req.header(header::AUTHORIZATION, format!("Bearer {secret}"));
    }
    let req = req
        .body(Body::from(body))
        .map_err(|e| BrokerError::Failed(format!("request: {e}")))?;
    let call = async {
        let resp = st.upstream.call(req).await.map_err(BrokerError::Failed)?;
        let status = resp.status();
        let content_type = resp.headers().get(header::CONTENT_TYPE).cloned();
        let body = axum::body::to_bytes(resp.into_body(), BROKER_ANSWER_CAP)
            .await
            .map_err(|e| BrokerError::Failed(format!("read: {e}")))?;
        Ok(BrokerAnswer {
            status,
            content_type,
            body,
        })
    };
    tokio::time::timeout(limit, call)
        .await
        .map_err(|_| BrokerError::Timeout)?
}

/// A passed-through call's answer, as the broker gave it.
fn relay(r: Result<BrokerAnswer, BrokerError>) -> Response {
    match r {
        Ok(a) => {
            let mut resp = (a.status, a.body).into_response();
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                a.content_type
                    .unwrap_or_else(|| HeaderValue::from_static("application/json")),
            );
            resp
        }
        Err(e @ BrokerError::Timeout) => crate::errors::err_504(&e.to_string()),
        Err(e) => crate::errors::err_502(&e.to_string()),
    }
}

/// The head of a broker answer, for an error message.
fn snippet(body: &[u8]) -> String {
    String::from_utf8_lossy(body).chars().take(300).collect()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, Once};

    use tower::ServiceExt;

    use super::*;
    use crate::state::ClusterStatus;
    use crate::store::kv::{self, Expect, Ttl};
    use crate::store::memkv::MemKv;
    use crate::store::schema::{self, ns, ClusterDoc, UserDoc};
    use crate::store::usage::{self, DAY_US, MINUTE_US};
    use crate::store::Store;
    use crate::upstream::Upstream;

    const TOKEN: &str = "cp-test-token";
    const DEFAULT_TENANT: &str = "00000000-0000-0000-0000-000000000001";

    /// Every test here talks to the surface with one token. The environment
    /// is the whole test binary's: one value, set once, read by nothing else.
    fn token_on() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| std::env::set_var("QUEEN_PROXY_CP_TOKEN", TOKEN));
    }

    /// One request the stub broker was handed.
    #[derive(Clone, Debug)]
    struct Seen {
        method: String,
        path: String,
        query: Option<String>,
        tenant: Option<String>,
        auth: Option<String>,
        body: String,
    }

    type Calls = Arc<Mutex<Vec<Seen>>>;

    /// How the stub broker answers a tenant purge.
    #[derive(Clone, Copy, PartialEq)]
    enum Purge {
        /// Like the raft facade: the first pass for a tenant deletes its 3
        /// partitions and drops its queues; every later pass finds nothing.
        Works,
        /// 503, as with no leader.
        Fails,
        /// 2xx without a `done`.
        NoDone,
        /// 2xx whose body is not JSON.
        NotJson,
    }

    #[derive(Clone, Copy)]
    struct Mode {
        purge: Purge,
        /// The queue listing answers 500.
        listing_fails: bool,
    }

    /// The broker router a single binary hands the proxy, reduced to the
    /// three routes this surface calls, writing down every request.
    fn stub_broker(calls: Calls, mode: Mode) -> axum::Router {
        let purged: Arc<Mutex<std::collections::HashSet<String>>> = Arc::default();
        axum::Router::new().fallback(move |req: Request<Body>| {
            let (calls, purged) = (calls.clone(), purged.clone());
            async move {
                let (parts, body) = req.into_parts();
                let body = axum::body::to_bytes(body, 1 << 20).await.unwrap_or_default();
                let head = |n: &str| parts.headers.get(n).and_then(|v| v.to_str().ok()).map(str::to_string);
                let tenant = head(crate::config::TENANT_HEADER);
                calls.lock().unwrap().push(Seen {
                    method: parts.method.to_string(),
                    path: parts.uri.path().to_string(),
                    query: parts.uri.query().map(str::to_string),
                    tenant: tenant.clone(),
                    auth: head("authorization"),
                    body: String::from_utf8_lossy(&body).into(),
                });
                let gone = purged.lock().unwrap().contains(tenant.as_deref().unwrap_or(""));
                let (status, v) = match (parts.method.as_str(), parts.uri.path()) {
                    ("DELETE", "/api/v1/resources/tenant") => match mode.purge {
                        Purge::Fails => (StatusCode::SERVICE_UNAVAILABLE, json!({"error": "no leader"})),
                        Purge::NoDone => (StatusCode::OK, json!({"success": true})),
                        Purge::NotJson => return (StatusCode::OK, "purged").into_response(),
                        Purge::Works => {
                            purged.lock().unwrap().insert(tenant.clone().unwrap_or_default());
                            let n = if gone { 0 } else { 3 };
                            (
                                StatusCode::OK,
                                json!({"success": true, "tenant": tenant, "partitionsDeleted": n, "done": true}),
                            )
                        }
                    },
                    ("GET", "/api/v1/resources/queues") if mode.listing_fails => {
                        (StatusCode::INTERNAL_SERVER_ERROR, json!({"error": "boom"}))
                    }
                    ("GET", "/api/v1/resources/queues") if gone => {
                        (StatusCode::OK, json!({"queues": [], "tenant": tenant}))
                    }
                    ("GET", "/api/v1/resources/queues") => (
                        StatusCode::OK,
                        json!({"queues": [{"name": "orders", "partitions": 2}], "tenant": tenant}),
                    ),
                    ("POST", "/api/v1/configure") => (StatusCode::CREATED, json!({"configured": true})),
                    _ => (StatusCode::NOT_FOUND, json!({"error": "no route"})),
                };
                (status, Json(v)).into_response()
            }
        })
    }

    fn st_with(store: Store, upstream: Upstream) -> St {
        st_cfg(store, upstream, crate::config::test_config(&[]))
    }

    fn st_cfg(store: Store, upstream: Upstream, cfg: crate::config::Config) -> St {
        st_full(store, upstream, cfg, None)
    }

    fn st_full(
        store: Store,
        upstream: Upstream,
        cfg: crate::config::Config,
        s3: Option<Arc<dyn S3Sinks>>,
    ) -> St {
        let cache = crate::cache::ClusterCache::new(&cfg, store.clone());
        let limits = crate::limits::Limits::new(&cfg);
        let meter = Arc::new(crate::meter::Meter::new(&cfg));
        let registry = crate::registry::Registry::new(store.clone());
        let keys = crate::auth::Keys::from_config(&cfg);
        Arc::new(crate::state::AppState {
            cfg,
            store,
            upstream,
            cache,
            limits,
            meter,
            registry,
            keys,
            s3,
        })
    }

    async fn seeded(base_url: &str, secret: Option<&str>) -> (Store, Arc<MemKv>) {
        token_on();
        let kv = Arc::new(MemKv::new());
        let store = Store::Kv(kv.clone());
        data::seed_default_plans(&store).await.unwrap();
        data::upsert_cell(
            &store,
            &data::CellSpec {
                slug: "local".into(),
                region: "local".into(),
                base_url: base_url.into(),
                class: "shared".into(),
                capacity_slots: 0,
                cell_secret: secret.map(str::to_string),
            },
        )
        .await
        .unwrap();
        (store, kv)
    }

    /// A seeded single binary (plans, its own cell, no tenant) whose broker
    /// is the stub, in-process.
    async fn world_mode(mode: Mode) -> (St, Arc<MemKv>, Calls) {
        let (store, kv) = seeded("inprocess://self", None).await;
        let calls = Calls::default();
        let st = st_with(store, Upstream::InProcess(stub_broker(calls.clone(), mode)));
        (st, kv, calls)
    }

    async fn world_with(purge: Purge) -> (St, Arc<MemKv>, Calls) {
        world_mode(Mode {
            purge,
            listing_fails: false,
        })
        .await
    }

    async fn world() -> (St, Arc<MemKv>, Calls) {
        world_with(Purge::Works).await
    }

    const WORKS: Mode = Mode {
        purge: Purge::Works,
        listing_fails: false,
    };

    async fn call(st: &St, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
        call_with(st, method, uri, body, &[("x-queen-cp-token", TOKEN)]).await
    }

    async fn call_with(
        st: &St,
        method: &str,
        uri: &str,
        body: Option<Value>,
        headers: &[(&str, &str)],
    ) -> (StatusCode, Value) {
        let app = Router::new()
            .nest("/api/cp", router())
            .with_state(st.clone());
        let mut req = Request::builder().method(method).uri(uri);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let req = req
            .body(body.map_or_else(Body::empty, |v| Body::from(v.to_string())))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let v = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()))
        };
        (status, v)
    }

    /// 64 lowercase hex characters: a key hash.
    fn hash(n: u8) -> String {
        format!("{n:064x}")
    }

    fn prov(tenant: &str, cluster: &str, user: Uuid, email: &str, key: &str) -> Value {
        json!({
            "tenant_slug": tenant,
            "tenant_name": format!("{tenant} Inc"),
            "cluster_slug": cluster,
            "plan": "free",
            "user_id": user,
            "email": email,
            "role": "admin",
            "key_name": "signup",
            "key_hash": key,
            "scopes": ["produce", "consume", "read", "admin"],
            "overrides": {"max_req_per_sec": 10, "req_burst": 50, "max_retained_bytes": 1_073_741_824},
        })
    }

    /// `prov` sent, asserted 200; the answer.
    async fn provisioned(st: &St, body: Value) -> Value {
        let (s, v) = call(st, "POST", "/api/cp/provision", Some(body)).await;
        assert_eq!(s, StatusCode::OK, "{v}");
        v
    }

    fn id(v: &Value) -> Uuid {
        Uuid::parse_str(v.as_str().expect("a uuid string")).expect("uuid")
    }

    fn calls_to(calls: &Calls, path: &str) -> Vec<Seen> {
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.path == path)
            .cloned()
            .collect()
    }

    /// Point a cluster's document at another broker tenant, as a hand-made row
    /// would.
    async fn rebind(kv: &MemKv, cluster: Uuid, broker_tenant: &str) {
        let mut doc: ClusterDoc = kv::get(kv, ns::CLUSTERS, &schema::key(cluster))
            .await
            .unwrap()
            .unwrap()
            .value;
        doc.broker_tenant_uuid = Uuid::parse_str(broker_tenant).unwrap();
        kv::write(
            kv,
            vec![kv::put_op(
                ns::CLUSTERS,
                &schema::key(cluster),
                &doc,
                Expect::Any,
                Ttl::Forever,
                false,
            )],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn the_surface_is_off_without_a_token_and_refuses_a_wrong_one() {
        let mut h = HeaderMap::new();
        h.insert("x-queen-cp-token", HeaderValue::from_static(TOKEN));
        assert_eq!(
            check_token(None, &h).unwrap_err().status(),
            StatusCode::NOT_FOUND
        );
        assert!(check_token(Some(TOKEN), &h).is_ok());
        assert_eq!(
            check_token(Some("other"), &h).unwrap_err().status(),
            StatusCode::UNAUTHORIZED
        );

        let (st, kv, _) = world().await;
        let body = prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1));
        for headers in [vec![("x-queen-cp-token", "wrong")], vec![]] {
            let (s, v) = call_with(
                &st,
                "POST",
                "/api/cp/provision",
                Some(body.clone()),
                &headers,
            )
            .await;
            assert_eq!(
                (s, v["code"].clone()),
                (StatusCode::UNAUTHORIZED, json!("unauthorized"))
            );
        }
        assert!(kv.keys(ns::TENANTS).is_empty());
    }

    #[tokio::test]
    async fn provision_creates_everything_once_and_a_retry_answers_the_same_ids() {
        let (st, kv, _) = world().await;
        let user = Uuid::new_v4();
        let body = prov("acme", "acme", user, " Ops@Acme.io ", &hash(1));
        let a = provisioned(&st, body.clone()).await;
        assert_eq!(
            a["created"],
            json!({"tenant": true, "cluster": true, "user": true, "key": true})
        );
        assert_eq!(
            (a["user_id"].clone(), a["plan_code"].clone()),
            (json!(user), json!("free"))
        );
        assert_eq!(
            a["overrides"],
            json!({"max_req_per_sec": 10, "req_burst": 50, "max_retained_bytes": 1_073_741_824})
        );
        let cluster = id(&a["cluster_id"]);

        // The caller's key is the cluster's credential, under the overrides.
        let Lookup::Found((ctx, key_id, scopes)) = data::lookup_api_key(&st.store, &hash(1)).await
        else {
            panic!("the provisioned key resolves");
        };
        assert_eq!((ctx.cluster_id, key_id), (cluster, id(&a["key_id"])));
        assert_eq!(ctx.broker_tenant, id(&a["broker_tenant_uuid"]));
        assert_eq!(scopes, crate::state::Scopes::all());
        assert_eq!(
            (ctx.limits.max_req_per_sec, ctx.limits.max_queues),
            (Some(10), Some(20))
        );
        // The user under the caller's id, its email lowercased, admin.
        let u: UserDoc = kv::get(kv.as_ref(), ns::USERS, &schema::key(user))
            .await
            .unwrap()
            .unwrap()
            .value;
        assert_eq!(
            (u.email.as_str(), u.tenant_id),
            ("ops@acme.io", id(&a["tenant_id"]))
        );
        assert_eq!(
            data::cluster_role(&st.store, user, cluster)
                .await
                .unwrap()
                .as_deref(),
            Some("admin")
        );
        let tenant = data::tenant_by_slug(&st.store, "acme")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tenant.name, "acme Inc");

        // The retry: the same ids, nothing created, nothing written.
        let ops = kv.keys(ns::OPS).len();
        let b = provisioned(&st, body).await;
        for k in [
            "tenant_id",
            "cluster_id",
            "broker_tenant_uuid",
            "user_id",
            "key_id",
            "plan_code",
            "overrides",
        ] {
            assert_eq!(a[k], b[k], "{k}");
        }
        assert_eq!(
            b["created"],
            json!({"tenant": false, "cluster": false, "user": false, "key": false})
        );
        assert_eq!(kv.keys(ns::OPS).len(), ops, "a retry writes nothing");
        assert_eq!(kv.keys(ns::KEYS).len(), 1);
    }

    #[tokio::test]
    async fn provision_keeps_a_clusters_plan_and_replaces_its_overrides() {
        let (st, _, _) = world().await;
        let user = Uuid::new_v4();
        let a = provisioned(&st, prov("acme", "acme", user, "a@acme.io", &hash(1))).await;
        let mut again = prov("acme", "acme", user, "a@acme.io", &hash(1));
        again["plan"] = json!("pro");
        again["overrides"] = json!({"max_queues": 5});
        let b = provisioned(&st, again.clone()).await;
        assert_eq!(
            b["plan_code"],
            json!("free"),
            "an existing cluster keeps its plan"
        );
        assert_eq!(
            b["overrides"],
            json!({"max_queues": 5}),
            "replaced, not merged"
        );
        assert_eq!(b["cluster_id"], a["cluster_id"]);
        let ctx = match data::lookup_cluster(&st.store, &ClusterKey::Slug("acme".into())).await {
            Lookup::Found(c) => c,
            _ => panic!("cluster"),
        };
        assert_eq!(
            (ctx.limits.max_queues, ctx.limits.max_req_per_sec),
            (Some(5), Some(5))
        );
        // null is {}
        again["overrides"] = Value::Null;
        assert_eq!(
            provisioned(&st, again.clone()).await["overrides"],
            json!({})
        );
        again.as_object_mut().unwrap().remove("overrides");
        assert_eq!(provisioned(&st, again).await["overrides"], json!({}));
        // An unknown plan is refused even for an existing cluster.
        let mut gold = prov("acme", "acme", user, "a@acme.io", &hash(1));
        gold["plan"] = json!("gold");
        let (s, v) = call(&st, "POST", "/api/cp/provision", Some(gold)).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::BAD_REQUEST, json!("invalid")),
            "{v}"
        );
    }

    #[tokio::test]
    async fn provision_adopts_the_live_key_whatever_its_name_and_moves_the_email() {
        let (st, kv, _) = world().await;
        let user = Uuid::new_v4();
        let a = provisioned(&st, prov("acme", "acme", user, "a@acme.io", &hash(1))).await;
        let mut again = prov("acme", "acme", user, "New@Acme.io", &hash(1));
        again["key_name"] = json!("renamed");
        let b = provisioned(&st, again).await;
        assert_eq!(b["key_id"], a["key_id"]);
        assert_eq!(
            b["created"],
            json!({"tenant": false, "cluster": false, "user": false, "key": false})
        );
        assert_eq!(
            kv.keys(ns::USER_EMAIL),
            vec!["#new@acme.io".to_string()],
            "the old address is released"
        );
        let u: UserDoc = kv::get(kv.as_ref(), ns::USERS, &schema::key(user))
            .await
            .unwrap()
            .unwrap()
            .value;
        assert_eq!(u.email, "new@acme.io");
        // The released address is free for someone else.
        provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
    }

    #[tokio::test]
    async fn provision_refuses_bad_input_writing_nothing() {
        let (st, kv, _) = world().await;
        let base = prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1));
        let cases: Vec<(&str, Value)> = vec![
            ("plan", json!("gold")),
            ("role", json!("owner")),
            ("key_hash", json!("XYZ")),
            ("key_hash", json!(hash(0xab).to_uppercase())),
            ("scopes", json!([])),
            ("scopes", json!(["write"])),
            ("overrides", json!({"max_bogus": 1})),
            ("overrides", json!({"max_queues": -1})),
            ("overrides", json!({"max_queues": "5"})),
            ("overrides", json!("none")),
            ("tenant_slug", json!("Bad_Slug")),
            ("cluster_slug", json!("-x")),
            ("email", json!("nobody")),
            ("key_name", json!("  ")),
            ("user_id", json!("not-a-uuid")),
            ("user_id", json!(Uuid::nil())),
        ];
        for (field, value) in cases {
            let mut body = base.clone();
            body[field] = value.clone();
            let (s, v) = call(&st, "POST", "/api/cp/provision", Some(body)).await;
            assert_eq!(
                (s, v["code"].clone()),
                (StatusCode::BAD_REQUEST, json!("invalid")),
                "{field} = {value}: {v}"
            );
        }
        let mut body = base.clone();
        body.as_object_mut().unwrap().remove("user_id");
        assert_eq!(
            call(&st, "POST", "/api/cp/provision", Some(body)).await.0,
            StatusCode::BAD_REQUEST
        );
        assert!(kv.keys(ns::TENANTS).is_empty() && kv.keys(ns::OPS).is_empty());
    }

    #[tokio::test]
    async fn provision_conflicts_are_409_and_write_nothing() {
        let (st, kv, _) = world().await;
        let (ua, ub) = (Uuid::new_v4(), Uuid::new_v4());
        let a = provisioned(&st, prov("acme", "acme", ua, "a@acme.io", &hash(1))).await;
        provisioned(&st, prov("beta", "beta", ub, "b@beta.io", &hash(2))).await;
        let ops = kv.keys(ns::OPS).len();
        let conflicts = [
            // a cluster slug of another tenant
            prov("evil", "acme", Uuid::new_v4(), "e@evil.io", &hash(3)),
            // an email of another user
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
            // a user of another tenant
            prov("acme", "acme", ub, "x@acme.io", &hash(1)),
            // a key hash another cluster holds
            prov("acme", "acme", ua, "a@acme.io", &hash(2)),
        ];
        for body in conflicts {
            let (s, v) = call(&st, "POST", "/api/cp/provision", Some(body.clone())).await;
            assert_eq!(
                (s, v["code"].clone()),
                (StatusCode::CONFLICT, json!("conflict")),
                "{body}: {v}"
            );
        }
        // a hash its revoked key still holds
        data::ensure_api_key_revoked(&st.store, id(&a["key_id"]))
            .await
            .unwrap();
        let (s, v) = call(
            &st,
            "POST",
            "/api/cp/provision",
            Some(prov("acme", "acme", ua, "a@acme.io", &hash(1))),
        )
        .await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::CONFLICT, json!("conflict")),
            "{v}"
        );
        assert_eq!(
            kv.keys(ns::TENANT_SLUG),
            vec!["#acme".to_string(), "#beta".to_string()]
        );
        assert_eq!(
            kv.keys(ns::OPS).len(),
            ops + 1,
            "only the revocation's own audit row"
        );
    }

    #[tokio::test]
    async fn clusters_read_back_with_plan_statuses_and_overrides() {
        let (st, _, _) = world().await;
        let a = provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        let mut big = prov("big", "big", Uuid::new_v4(), "b@big.io", &hash(2));
        big["plan"] = json!("pro");
        provisioned(&st, big).await;

        let (s, all) = call(&st, "GET", "/api/cp/clusters", None).await;
        assert_eq!(s, StatusCode::OK);
        let slugs: Vec<&str> = all["clusters"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["slug"].as_str().unwrap())
            .collect();
        assert_eq!(slugs, vec!["acme", "big"]);
        let (_, free) = call(&st, "GET", "/api/cp/clusters?plan=free", None).await;
        let free = free["clusters"].as_array().unwrap().clone();
        assert_eq!(free.len(), 1);
        assert_eq!(
            free[0],
            json!({
                "id": a["cluster_id"], "slug": "acme", "tenant_id": a["tenant_id"], "tenant_slug": "acme",
                "broker_tenant_uuid": a["broker_tenant_uuid"], "plan_code": "free", "status": "active",
                "tenant_status": "active", "overrides": a["overrides"],
            })
        );
        let (_, none) = call(&st, "GET", "/api/cp/clusters?plan=gold", None).await;
        assert_eq!(none, json!({"clusters": []}));

        let (s, one) = call(&st, "GET", "/api/cp/clusters/acme", None).await;
        assert_eq!((s, &one), (StatusCode::OK, &free[0]));
        let (s, v) = call(&st, "GET", "/api/cp/clusters/nope", None).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::NOT_FOUND, json!("cluster_unknown"))
        );
    }

    #[tokio::test]
    async fn tenant_status_reaches_every_cluster_and_repeats_quietly() {
        let (st, kv, _) = world().await;
        let a = provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        let want = json!({
            "ok": true,
            "tenant_id": a["tenant_id"],
            "clusters": [{"id": a["cluster_id"], "slug": "acme", "broker_tenant_uuid": a["broker_tenant_uuid"]}],
        });
        let (s, v) = call(
            &st,
            "PUT",
            "/api/cp/tenants/acme/status",
            Some(json!({"status": "suspended"})),
        )
        .await;
        assert_eq!((s, &v), (StatusCode::OK, &want));
        let Lookup::Found(ctx) =
            data::lookup_cluster(&st.store, &ClusterKey::Id(id(&a["cluster_id"]))).await
        else {
            panic!("cluster");
        };
        assert_eq!(ctx.status, ClusterStatus::Suspended);
        let ops = kv.keys(ns::OPS).len();
        let (s, v) = call(
            &st,
            "PUT",
            "/api/cp/tenants/acme/status",
            Some(json!({"status": "suspended"})),
        )
        .await;
        assert_eq!((s, &v), (StatusCode::OK, &want));
        assert_eq!(
            kv.keys(ns::OPS).len(),
            ops,
            "the same status writes nothing"
        );

        let (s, v) = call(
            &st,
            "PUT",
            "/api/cp/tenants/acme/status",
            Some(json!({"status": "paused"})),
        )
        .await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::BAD_REQUEST, json!("invalid"))
        );
        let (s, v) = call(
            &st,
            "PUT",
            "/api/cp/tenants/nope/status",
            Some(json!({"status": "grace"})),
        )
        .await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::NOT_FOUND, json!("tenant_unknown"))
        );
    }

    #[tokio::test]
    async fn purge_runs_the_brokers_purge_per_cluster_once_the_tenant_is_deleting() {
        let (st, _, calls) = world().await;
        let (s, v) = call(&st, "POST", "/api/cp/tenants/acme/purge", None).await;
        assert_eq!(
            (s, v),
            (
                StatusCode::OK,
                json!({"existed": false, "done": true, "clusters": []})
            )
        );

        let a = provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        let (s, c2) = call(
            &st,
            "POST",
            "/api/cp/clusters",
            Some(json!({"tenant_slug": "acme", "slug": "acme-2"})),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED, "{c2}");
        let (s, v) = call(&st, "POST", "/api/cp/tenants/acme/purge", None).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::CONFLICT, json!("not_deleting")),
            "{v}"
        );
        assert!(
            calls.lock().unwrap().is_empty(),
            "nothing reached the broker"
        );

        call(
            &st,
            "PUT",
            "/api/cp/tenants/acme/status",
            Some(json!({"status": "deleting"})),
        )
        .await;
        // The pass that deletes is not done: only the next one, which finds
        // nothing left, proves it.
        let pass = |done: bool, n: i64| {
            json!({
                "existed": true,
                "done": done,
                "clusters": [
                    {"slug": "acme", "broker_tenant_uuid": a["broker_tenant_uuid"], "done": done, "partitions_deleted": n},
                    {"slug": "acme-2", "broker_tenant_uuid": c2["broker_tenant_uuid"], "done": done, "partitions_deleted": n},
                ],
            })
        };
        let (s, v) = call(&st, "POST", "/api/cp/tenants/acme/purge", None).await;
        assert_eq!((s, &v), (StatusCode::OK, &pass(false, 3)));
        let seen = calls_to(&calls, "/api/v1/resources/tenant");
        assert_eq!(seen.len(), 2);
        for (c, x) in seen
            .iter()
            .zip([&a["broker_tenant_uuid"], &c2["broker_tenant_uuid"]])
        {
            let x = x.as_str().unwrap();
            assert_eq!(c.method, "DELETE");
            assert_eq!(
                c.tenant.as_deref(),
                Some(x),
                "scoped to the cluster's own broker tenant"
            );
            assert_eq!(c.query.as_deref(), Some(format!("tenant={x}").as_str()));
        }
        let (s, v) = call(&st, "POST", "/api/cp/tenants/acme/purge", None).await;
        assert_eq!((s, &v), (StatusCode::OK, &pass(true, 0)));
        // Safe to repeat.
        let (s, again) = call(&st, "POST", "/api/cp/tenants/acme/purge", None).await;
        assert_eq!((s, again), (StatusCode::OK, pass(true, 0)));
    }

    /// `done` is the broker's confirmation of an empty pass, never a default:
    /// an answer without `done` and a body that is not JSON are not done.
    #[tokio::test]
    async fn a_purge_answer_that_does_not_confirm_is_not_done() {
        for purge in [Purge::NoDone, Purge::NotJson] {
            let (st, _, _) = world_with(purge).await;
            provisioned(
                &st,
                prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
            )
            .await;
            call(
                &st,
                "PUT",
                "/api/cp/tenants/acme/status",
                Some(json!({"status": "deleting"})),
            )
            .await;
            for _ in 0..2 {
                let (s, v) = call(&st, "POST", "/api/cp/tenants/acme/purge", None).await;
                assert_eq!(
                    (s, v["done"].clone()),
                    (StatusCode::OK, json!(false)),
                    "{v}"
                );
                assert_eq!(v["clusters"][0]["done"], json!(false));
                assert!(
                    v["clusters"][0]["error"]
                        .as_str()
                        .unwrap()
                        .contains("without done"),
                    "{v}"
                );
            }
        }
    }

    /// A cluster the tenant's index lists whose document is gone has a
    /// broker tenant nobody knows: the purge never answers done.
    #[tokio::test]
    async fn a_purge_with_a_cluster_document_gone_is_never_done() {
        let (st, kv, _) = world().await;
        provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        let (_, c2) = call(
            &st,
            "POST",
            "/api/cp/clusters",
            Some(json!({"tenant_slug": "acme", "slug": "acme-2"})),
        )
        .await;
        call(
            &st,
            "PUT",
            "/api/cp/tenants/acme/status",
            Some(json!({"status": "deleting"})),
        )
        .await;
        kv::write(
            kv.as_ref(),
            vec![kv::delete_op(
                ns::CLUSTERS,
                &schema::key(id(&c2["id"])),
                Expect::Any,
                false,
            )],
        )
        .await
        .unwrap();
        for _ in 0..3 {
            let (s, v) = call(&st, "POST", "/api/cp/tenants/acme/purge", None).await;
            assert_eq!(
                (s, v["done"].clone()),
                (StatusCode::OK, json!(false)),
                "{v}"
            );
            assert_eq!(v["missing_clusters"], json!([c2["id"]]));
            assert_eq!(
                v["clusters"].as_array().unwrap().len(),
                1,
                "the known cluster is still purged"
            );
        }
    }

    #[tokio::test]
    async fn a_purge_the_broker_did_not_finish_is_not_done() {
        let (st, _, calls) = world_with(Purge::Fails).await;
        let a = provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        call(
            &st,
            "PUT",
            "/api/cp/tenants/acme/status",
            Some(json!({"status": "deleting"})),
        )
        .await;
        let (s, v) = call(&st, "POST", "/api/cp/tenants/acme/purge", None).await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(
            (v["existed"].clone(), v["done"].clone()),
            (json!(true), json!(false))
        );
        let c = &v["clusters"][0];
        assert_eq!(
            (c["slug"].clone(), c["done"].clone()),
            (json!("acme"), json!(false))
        );
        assert_eq!(c["broker_tenant_uuid"], a["broker_tenant_uuid"]);
        assert!(c["partitions_deleted"].is_null());
        assert!(c["error"].as_str().unwrap().contains("503"), "{c}");
        assert_eq!(calls_to(&calls, "/api/v1/resources/tenant").len(), 1);
    }

    #[tokio::test]
    async fn the_system_tenants_are_never_acted_on() {
        for reserved in [DEFAULT_TENANT, schema::PROXY_TENANT] {
            let (st, kv, calls) = world().await;
            let a = provisioned(
                &st,
                prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
            )
            .await;
            call(
                &st,
                "PUT",
                "/api/cp/tenants/acme/status",
                Some(json!({"status": "deleting"})),
            )
            .await;
            rebind(&kv, id(&a["cluster_id"]), reserved).await;
            for (method, uri, body) in [
                ("POST", "/api/cp/tenants/acme/purge", None),
                ("DELETE", "/api/cp/tenants/acme", None),
                ("GET", "/api/cp/clusters/acme/queues", None),
                (
                    "POST",
                    "/api/cp/clusters/acme/configure",
                    Some(json!({"queue": "q"})),
                ),
            ] {
                let (s, v) = call(&st, method, uri, body).await;
                assert_eq!(
                    (s, v["code"].clone()),
                    (StatusCode::CONFLICT, json!("system_tenant")),
                    "{uri}: {v}"
                );
            }
            assert!(
                calls.lock().unwrap().is_empty(),
                "{reserved}: nothing reached the broker"
            );
        }
    }

    #[tokio::test]
    async fn delete_tenant_waits_for_deleting_unless_forced_and_repeats() {
        let (st, _, _) = world().await;
        let a = provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        let (s, v) = call(&st, "DELETE", "/api/cp/tenants/acme", None).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::CONFLICT, json!("not_deleting"))
        );
        let (s, v) = call(&st, "DELETE", "/api/cp/tenants/acme?force=maybe", None).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::BAD_REQUEST, json!("invalid"))
        );

        call(
            &st,
            "PUT",
            "/api/cp/tenants/acme/status",
            Some(json!({"status": "deleting"})),
        )
        .await;
        // Its broker tenant still lists a queue: not purged yet.
        let (s, v) = call(&st, "DELETE", "/api/cp/tenants/acme", None).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::CONFLICT, json!("not_purged")),
            "{v}"
        );
        assert!(data::tenant_by_slug(&st.store, "acme")
            .await
            .unwrap()
            .is_some());
        call(&st, "POST", "/api/cp/tenants/acme/purge", None).await;
        let (s, v) = call(&st, "DELETE", "/api/cp/tenants/acme", None).await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(
            v,
            json!({"existed": true, "clusters": [{
                "cluster_id": a["cluster_id"], "slug": "acme", "broker_tenant_uuid": a["broker_tenant_uuid"],
            }]})
        );
        assert!(matches!(
            data::lookup_api_key(&st.store, &hash(1)).await,
            Lookup::Absent
        ));
        let (s, v) = call(&st, "DELETE", "/api/cp/tenants/acme", None).await;
        assert_eq!(
            (s, v),
            (StatusCode::OK, json!({"existed": false, "clusters": []}))
        );

        // force skips the gate, and only the gate
        provisioned(
            &st,
            prov("live", "live", Uuid::new_v4(), "l@live.io", &hash(2)),
        )
        .await;
        let (s, v) = call(&st, "DELETE", "/api/cp/tenants/live?force=true", None).await;
        assert_eq!(
            (s, v["existed"].clone()),
            (StatusCode::OK, json!(true)),
            "{v}"
        );
        assert!(data::tenant_by_slug(&st.store, "live")
            .await
            .unwrap()
            .is_none());
    }

    /// The purge check needs the broker's answer: a listing it cannot read is
    /// a 503 to retry, never a delete.
    #[tokio::test]
    async fn delete_tenant_cannot_confirm_the_purge_without_the_listing() {
        let (st, _, _) = world_mode(Mode {
            purge: Purge::Works,
            listing_fails: true,
        })
        .await;
        provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        call(
            &st,
            "PUT",
            "/api/cp/tenants/acme/status",
            Some(json!({"status": "deleting"})),
        )
        .await;
        call(&st, "POST", "/api/cp/tenants/acme/purge", None).await;
        let (s, v) = call(&st, "DELETE", "/api/cp/tenants/acme", None).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::SERVICE_UNAVAILABLE, json!("unavailable")),
            "{v}"
        );
        assert!(data::tenant_by_slug(&st.store, "acme")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn activity_reads_traffic_pushes_keys_and_retained_bytes() {
        let (st, kv, _) = world().await;
        let a = provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        provisioned(
            &st,
            prov("quiet", "quiet", Uuid::new_v4(), "q@quiet.io", &hash(2)),
        )
        .await;
        let c = id(&a["cluster_id"]);
        let today = usage::day_of(usage::now_us());
        let row = |space: &str, k: String, msgs: i64| {
            let doc = schema::UsageDoc {
                msgs,
                reqs: 1,
                bytes_in: 0,
                bytes_out: 0,
            };
            kv::put_op(space, &k, &doc, Expect::Any, Ttl::Forever, false)
        };
        let pushed = (today - 10) * DAY_US + 3 * 60 * MINUTE_US;
        let busy = today * DAY_US + 7 * 60 * MINUTE_US;
        kv::write(
            kv.as_ref(),
            vec![
                // rolled days: a push ten days ago, reads two days ago
                row(
                    ns::USAGE_DAY,
                    usage::day_key(c, &usage::day_str(today - 10), "push"),
                    2,
                ),
                row(
                    ns::USAGE_DAY,
                    usage::day_key(c, &usage::day_str(today - 2), "read"),
                    0,
                ),
                // the minute that push day kept
                row(ns::USAGE_MIN, usage::minute_key(c, pushed, "push", "n1"), 2),
                // today: a delivery, then a configure (administration, not traffic)
                row(
                    ns::USAGE_MIN,
                    usage::minute_key(c, busy, "delivery", "n1"),
                    1,
                ),
                row(
                    ns::USAGE_MIN,
                    usage::minute_key(c, busy + 60 * MINUTE_US, "configure", "n2"),
                    0,
                ),
            ],
        )
        .await
        .unwrap();
        data::touch_api_keys(&st.store, &[id(&a["key_id"])])
            .await
            .unwrap();
        st.registry.set_retained_for_test(c, 4096);

        let (s, v) = call(
            &st,
            "POST",
            "/api/cp/activity",
            Some(json!({"clusters": ["acme", "nope", "quiet"]})),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{v}");
        let got = v["clusters"].as_array().unwrap();
        assert_eq!(got.len(), 3, "one entry per slug, in request order");
        assert_eq!(got[0]["slug"], json!("acme"));
        assert_eq!(got[0]["found"], json!(true));
        assert_eq!(got[0]["cluster_id"], a["cluster_id"]);
        assert_eq!(got[0]["last_activity_at"], json!(usage::iso_minute(busy)));
        assert_eq!(got[0]["first_push_at"], json!(usage::iso_minute(pushed)));
        let used = got[0]["last_key_used_at"].as_str().unwrap();
        let on = |d: i64| used.starts_with(&format!("{}T", usage::day_str(d)));
        assert!(
            (on(today) || on(today + 1)) && used.ends_with('Z') && used.len() == 20,
            "{used}"
        );
        assert_eq!(got[0]["retained_bytes"], json!(4096));
        assert_eq!(
            got[1],
            json!({"slug": "nope", "found": false, "cluster_id": null, "last_activity_at": null,
                   "first_push_at": null, "last_key_used_at": null, "retained_bytes": null})
        );
        assert_eq!(got[2]["found"], json!(true));
        for k in [
            "last_activity_at",
            "first_push_at",
            "last_key_used_at",
            "retained_bytes",
        ] {
            assert!(got[2][k].is_null(), "absent data is null, never 0: {k}");
        }

        let many: Vec<String> = (0..=MAX_ACTIVITY_CLUSTERS)
            .map(|i| format!("c{i}"))
            .collect();
        let (s, v) = call(
            &st,
            "POST",
            "/api/cp/activity",
            Some(json!({"clusters": many})),
        )
        .await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::BAD_REQUEST, json!("invalid"))
        );
        let (s, v) = call(
            &st,
            "POST",
            "/api/cp/activity",
            Some(json!({"clusters": []})),
        )
        .await;
        assert_eq!((s, v), (StatusCode::OK, json!({"clusters": []})));
    }

    #[tokio::test]
    async fn keys_are_retry_safe_both_ways() {
        let (st, _, _) = world().await;
        let a = provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        let b = provisioned(
            &st,
            prov("beta", "beta", Uuid::new_v4(), "b@beta.io", &hash(2)),
        )
        .await;
        let key = |cluster: &Value, name: &str, h: &str| json!({"cluster_id": cluster, "name": name, "key_hash": h, "scopes": ["read"]});
        let (s, v) = call(
            &st,
            "POST",
            "/api/cp/keys",
            Some(key(&a["cluster_id"], "ci", &hash(3))),
        )
        .await;
        assert_eq!(
            (s, v["existed"].clone()),
            (StatusCode::CREATED, json!(false)),
            "{v}"
        );
        let kid = v["id"].clone();
        let (s, v) = call(
            &st,
            "POST",
            "/api/cp/keys",
            Some(key(&a["cluster_id"], "renamed", &hash(3))),
        )
        .await;
        assert_eq!(
            (s, v),
            (StatusCode::OK, json!({"id": kid, "existed": true}))
        );
        let (s, v) = call(
            &st,
            "POST",
            "/api/cp/keys",
            Some(key(&b["cluster_id"], "ci", &hash(3))),
        )
        .await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::CONFLICT, json!("conflict")),
            "another cluster's hash"
        );

        let uri = format!("/api/cp/keys/{}", kid.as_str().unwrap());
        let (s, v) = call(&st, "DELETE", &uri, None).await;
        assert_eq!(
            (s, v),
            (
                StatusCode::OK,
                json!({"ok": true, "already_revoked": false})
            )
        );
        let (s, v) = call(&st, "DELETE", &uri, None).await;
        assert_eq!(
            (s, v),
            (StatusCode::OK, json!({"ok": true, "already_revoked": true}))
        );
        let (s, v) = call(
            &st,
            "POST",
            "/api/cp/keys",
            Some(key(&a["cluster_id"], "ci", &hash(3))),
        )
        .await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::CONFLICT, json!("conflict")),
            "a revoked key's hash"
        );

        let (s, v) = call(
            &st,
            "DELETE",
            &format!("/api/cp/keys/{}", Uuid::new_v4()),
            None,
        )
        .await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::NOT_FOUND, json!("key_unknown"))
        );
        let (s, v) = call(&st, "DELETE", "/api/cp/keys/not-a-uuid", None).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::BAD_REQUEST, json!("invalid"))
        );
        let (s, v) = call(
            &st,
            "POST",
            "/api/cp/keys",
            Some(key(&json!(Uuid::new_v4()), "ci", &hash(4))),
        )
        .await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::BAD_REQUEST, json!("invalid")),
            "an unknown cluster"
        );
    }

    #[tokio::test]
    async fn queues_and_configure_reach_the_broker_as_the_clusters_tenant() {
        let (st, _, calls) = world().await;
        let a = provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        let broker_tenant = a["broker_tenant_uuid"].as_str().unwrap().to_string();
        // A tenant header from the caller is never the one that travels.
        let headers = [
            ("x-queen-cp-token", TOKEN),
            ("x-queen-tenant", DEFAULT_TENANT),
        ];

        let (s, v) = call_with(
            &st,
            "GET",
            "/api/cp/clusters/acme/queues?stats=cached",
            None,
            &headers,
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(
            v,
            json!({"queues": [{"name": "orders", "partitions": 2}], "tenant": broker_tenant})
        );
        let options = json!({"queue": "orders", "options": {"retentionSeconds": 86400}});
        let (s, v) = call_with(
            &st,
            "POST",
            "/api/cp/clusters/acme/configure",
            Some(options.clone()),
            &headers,
        )
        .await;
        assert_eq!((s, v), (StatusCode::CREATED, json!({"configured": true})));

        let seen = calls.lock().unwrap().clone();
        assert_eq!(seen.len(), 2);
        assert_eq!(
            (seen[0].method.as_str(), seen[0].path.as_str()),
            ("GET", "/api/v1/resources/queues")
        );
        assert_eq!(seen[0].query.as_deref(), Some("stats=cached"));
        assert_eq!(
            (seen[1].method.as_str(), seen[1].path.as_str()),
            ("POST", "/api/v1/configure")
        );
        assert_eq!(
            serde_json::from_str::<Value>(&seen[1].body).unwrap(),
            options
        );
        assert!(seen
            .iter()
            .all(|c| c.tenant.as_deref() == Some(broker_tenant.as_str())));
        assert!(
            seen.iter().all(|c| c.auth.is_none()),
            "the single binary's cell has no secret"
        );

        // Never a cluster this surface does not know, never a body without a queue.
        let (s, v) = call(
            &st,
            "POST",
            "/api/cp/clusters/nope/configure",
            Some(json!({"queue": "q"})),
        )
        .await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::NOT_FOUND, json!("cluster_unknown"))
        );
        let (s, v) = call(&st, "GET", "/api/cp/clusters/nope/queues", None).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::NOT_FOUND, json!("cluster_unknown"))
        );
        let (s, v) = call(
            &st,
            "POST",
            "/api/cp/clusters/acme/configure",
            Some(json!({"options": {}})),
        )
        .await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::BAD_REQUEST, json!("invalid"))
        );
        assert_eq!(
            calls.lock().unwrap().len(),
            2,
            "none of those reached the broker"
        );
    }

    /// The same purge over HTTP, to a cell broker in another process: the
    /// cell's base URL and its secret are what the call carries.
    #[tokio::test]
    async fn a_purge_reaches_a_cell_broker_over_http_with_its_secret() {
        let calls = Calls::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = stub_broker(calls.clone(), WORKS);
        let _broker = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let (store, _) = seeded(&format!("http://{addr}"), Some("s3cret")).await;
        let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
        connector.set_nodelay(true);
        let client =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build::<_, Body>(connector);
        let st = st_with(store, Upstream::Http(client));

        let a = provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        call(
            &st,
            "PUT",
            "/api/cp/tenants/acme/status",
            Some(json!({"status": "deleting"})),
        )
        .await;
        let (s, v) = call(&st, "POST", "/api/cp/tenants/acme/purge", None).await;
        assert_eq!(
            (s, v["done"].clone()),
            (StatusCode::OK, json!(false)),
            "{v}"
        );
        assert_eq!(v["clusters"][0]["partitions_deleted"], json!(3));
        let (s, v) = call(&st, "POST", "/api/cp/tenants/acme/purge", None).await;
        assert_eq!((s, v["done"].clone()), (StatusCode::OK, json!(true)), "{v}");
        let seen = calls_to(&calls, "/api/v1/resources/tenant");
        assert_eq!(seen.len(), 2);
        assert!(seen
            .iter()
            .all(|c| c.tenant.as_deref() == a["broker_tenant_uuid"].as_str()));
        assert!(seen
            .iter()
            .all(|c| c.auth.as_deref() == Some("Bearer s3cret")));
    }

    #[tokio::test]
    async fn provision_waits_for_the_seed() {
        token_on();
        let st = st_with(
            Store::Kv(Arc::new(MemKv::new())),
            Upstream::InProcess(axum::Router::new()),
        );
        let (s, v) = call(
            &st,
            "POST",
            "/api/cp/provision",
            Some(prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1))),
        )
        .await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::SERVICE_UNAVAILABLE, json!("unavailable"))
        );
    }

    /// With the tenant header off (the single-tenant front door) every
    /// cluster's data is the broker's default tenant: nothing that acts on the
    /// broker runs, and the store-only routes still answer.
    #[tokio::test]
    async fn nothing_acts_on_the_broker_while_the_tenant_header_is_off() {
        let (store, _) = seeded("inprocess://self", None).await;
        let calls = Calls::default();
        let mut cfg = crate::config::test_config(&[]);
        cfg.send_tenant_header = false;
        let st = st_cfg(
            store,
            Upstream::InProcess(stub_broker(calls.clone(), WORKS)),
            cfg,
        );
        provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        let (s, _) = call(
            &st,
            "PUT",
            "/api/cp/tenants/acme/status",
            Some(json!({"status": "deleting"})),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        for (method, uri, body) in [
            ("POST", "/api/cp/tenants/acme/purge", None),
            ("GET", "/api/cp/clusters/acme/queues", None),
            (
                "POST",
                "/api/cp/clusters/acme/configure",
                Some(json!({"queue": "q"})),
            ),
        ] {
            let (s, v) = call(&st, method, uri, body).await;
            assert_eq!(
                (s, v["code"].clone()),
                (StatusCode::CONFLICT, json!("system_tenant")),
                "{uri}: {v}"
            );
        }
        assert!(
            calls.lock().unwrap().is_empty(),
            "nothing reached the broker"
        );
        let (s, v) = call(&st, "GET", "/api/cp/clusters/nope/queues", None).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::NOT_FOUND, json!("cluster_unknown"))
        );
        // The purge cannot be confirmed either: only a forced delete runs.
        let (s, v) = call(&st, "DELETE", "/api/cp/tenants/acme", None).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::CONFLICT, json!("system_tenant"))
        );
        assert!(
            calls.lock().unwrap().is_empty(),
            "nothing reached the broker"
        );
        let (s, v) = call(&st, "DELETE", "/api/cp/tenants/acme?force=true", None).await;
        assert_eq!((s, v["existed"].clone()), (StatusCode::OK, json!(true)));
    }

    /// A wipe in progress gets no queue back through configure, whether the
    /// cluster or its tenant is the one being deleted; listing still answers.
    #[tokio::test]
    async fn configure_refuses_a_cluster_or_tenant_being_deleted() {
        let (st, _, calls) = world().await;
        let a = provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        let configure = |st: St| async move {
            call(
                &st,
                "POST",
                "/api/cp/clusters/acme/configure",
                Some(json!({"queue": "q"})),
            )
            .await
        };
        assert_eq!(configure(st.clone()).await.0, StatusCode::CREATED);

        call(
            &st,
            "PUT",
            "/api/cp/tenants/acme/status",
            Some(json!({"status": "deleting"})),
        )
        .await;
        let (s, v) = configure(st.clone()).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::CONFLICT, json!("deleting")),
            "{v}"
        );
        let (s, _) = call(&st, "GET", "/api/cp/clusters/acme/queues", None).await;
        assert_eq!(s, StatusCode::OK);

        call(
            &st,
            "PUT",
            "/api/cp/tenants/acme/status",
            Some(json!({"status": "active"})),
        )
        .await;
        let uri = format!(
            "/api/cp/clusters/{}/status",
            a["cluster_id"].as_str().unwrap()
        );
        call(&st, "PUT", &uri, Some(json!({"status": "deleting"}))).await;
        let (s, v) = configure(st.clone()).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::CONFLICT, json!("deleting")),
            "{v}"
        );
        assert_eq!(
            calls_to(&calls, "/api/v1/configure").len(),
            1,
            "only the first one reached the broker"
        );
    }

    /// `PUT /clusters/:id/overrides` takes what provision takes: the limit
    /// names, each a non-negative integer or null; `null` clears.
    #[tokio::test]
    async fn the_overrides_route_validates_like_provision() {
        let (st, _, _) = world().await;
        let a = provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        let uri = format!(
            "/api/cp/clusters/{}/overrides",
            a["cluster_id"].as_str().unwrap()
        );
        for bad in [
            json!({"max_bogus": 1}),
            json!({"max_queues": -1}),
            json!({"max_queues": 1.5}),
            json!("x"),
        ] {
            let (s, v) = call(&st, "PUT", &uri, Some(bad.clone())).await;
            assert_eq!(
                (s, v["code"].clone()),
                (StatusCode::BAD_REQUEST, json!("invalid")),
                "{bad}"
            );
        }
        let (s, _) = call(
            &st,
            "PUT",
            &uri,
            Some(json!({"max_queues": 7, "max_retained_bytes": null})),
        )
        .await;
        assert_eq!(s, StatusCode::OK);
        let (_, row) = call(&st, "GET", "/api/cp/clusters/acme", None).await;
        assert_eq!(
            row["overrides"],
            json!({"max_queues": 7, "max_retained_bytes": null})
        );
        let (s, _) = call(&st, "PUT", &uri, Some(Value::Null)).await;
        assert_eq!(s, StatusCode::OK);
        let (_, row) = call(&st, "GET", "/api/cp/clusters/acme", None).await;
        assert_eq!(row["overrides"], json!({}));
    }

    #[tokio::test]
    async fn provision_refuses_a_tenant_being_deleted() {
        let (st, _, _) = world().await;
        let user = Uuid::new_v4();
        provisioned(&st, prov("acme", "acme", user, "a@acme.io", &hash(1))).await;
        call(
            &st,
            "PUT",
            "/api/cp/tenants/acme/status",
            Some(json!({"status": "deleting"})),
        )
        .await;
        let (s, v) = call(
            &st,
            "POST",
            "/api/cp/provision",
            Some(prov("acme", "acme", user, "a@acme.io", &hash(1))),
        )
        .await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::CONFLICT, json!("deleting")),
            "{v}"
        );
    }

    /// The token is read from the head before any of the body, and a body
    /// over 1 MiB is refused unread.
    #[tokio::test]
    async fn the_token_comes_before_the_body_and_bodies_are_capped() {
        let (st, kv, _) = world().await;
        let big = format!(r#"{{"clusters": ["{}"]}}"#, "a".repeat(MAX_BODY_BYTES));
        let send = |token: &'static str, body: String| {
            let st = st.clone();
            async move {
                let app = Router::new().nest("/api/cp", router()).with_state(st);
                let req = Request::builder()
                    .method("POST")
                    .uri("/api/cp/activity")
                    .header("x-queen-cp-token", token)
                    .body(Body::from(body))
                    .unwrap();
                app.oneshot(req).await.unwrap().status()
            }
        };
        // These requests carry no content-length: the cap holds as the body
        // is read.
        assert_eq!(
            send("wrong", big.clone()).await,
            StatusCode::UNAUTHORIZED,
            "the guard ran first"
        );
        assert_eq!(send(TOKEN, big).await, StatusCode::PAYLOAD_TOO_LARGE);
        // A declared length over the cap is refused before a byte is read.
        let app = Router::new()
            .nest("/api/cp", router())
            .with_state(st.clone());
        let req = Request::builder()
            .method("POST")
            .uri("/api/cp/provision")
            .header("x-queen-cp-token", TOKEN)
            .header(header::CONTENT_LENGTH, (MAX_BODY_BYTES + 1).to_string())
            .body(Body::from("{}"))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let v: Value = serde_json::from_slice(
            &axum::body::to_bytes(resp.into_body(), 1 << 16)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(v["code"], json!("payload_too_large"));
        assert!(kv.keys(ns::TENANTS).is_empty());
    }

    /// Every slug from a request goes through one gate: trimmed and
    /// lowercased, a DNS label or a 400, never a lookup (on a store that
    /// cannot answer, every one of these would otherwise be a 503).
    #[tokio::test]
    async fn slugs_are_gated_before_any_lookup() {
        token_on();
        let st = st_with(
            Store::Kv(Arc::new(crate::store::memkv::Down)),
            Upstream::InProcess(axum::Router::new()),
        );
        let huge = "a".repeat(10_000);
        for (method, uri, body) in [
            ("GET", format!("/api/cp/clusters/{huge}"), None),
            ("GET", "/api/cp/clusters/Bad_Slug".to_string(), None),
            ("GET", format!("/api/cp/clusters/{huge}/queues"), None),
            (
                "POST",
                format!("/api/cp/clusters/{huge}/configure"),
                Some(json!({"queue": "q"})),
            ),
            (
                "PUT",
                format!("/api/cp/tenants/{huge}/status"),
                Some(json!({"status": "grace"})),
            ),
            ("POST", format!("/api/cp/tenants/{huge}/purge"), None),
            ("DELETE", format!("/api/cp/tenants/{huge}"), None),
            (
                "POST",
                "/api/cp/clusters".to_string(),
                Some(json!({"tenant_slug": huge, "slug": "x"})),
            ),
        ] {
            let (s, v) = call(&st, method, &uri, body).await;
            assert_eq!(
                (s, v["code"].clone()),
                (StatusCode::BAD_REQUEST, json!("invalid")),
                "{method} {}",
                &uri[..40.min(uri.len())]
            );
        }
        let (s, v) = call(
            &st,
            "POST",
            "/api/cp/activity",
            Some(json!({"clusters": [huge, "Bad_Slug"]})),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{v}");
        assert_eq!(v["clusters"][0]["found"], json!(false));
        assert_eq!(v["clusters"][1]["found"], json!(false));

        // The same gate on a working store: case and padding are one spelling.
        let (st, _, _) = world().await;
        provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        let (s, row) = call(&st, "GET", "/api/cp/clusters/%20ACME%20", None).await;
        assert_eq!((s, row["slug"].clone()), (StatusCode::OK, json!("acme")));
        let (s, c) = call(
            &st,
            "POST",
            "/api/cp/clusters",
            Some(json!({"tenant_slug": " ACME ", "slug": "ACME-2"})),
        )
        .await;
        assert_eq!(
            (s, c["slug"].clone(), c["tenant_slug"].clone()),
            (StatusCode::CREATED, json!("acme-2"), json!("acme"))
        );
    }

    // ---- a cluster's S3 sink --------------------------------------------------

    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    /// The broker's sink hook, faked: a config is valid when its `bucket` is a
    /// name without a space; the n-th seal is `sealed:<n>:<the secret's bytes
    /// xor 0x5a, hex>` (no two alike, never the secret), or, on a cell without
    /// a key, `no_key` with `{secret}` replaced: a refusal careless enough to
    /// quote the secret.
    #[derive(Default)]
    struct FakeS3 {
        no_key: Option<&'static str>,
        /// Every `(broker tenant, config)` the check was handed.
        validated: Mutex<Vec<(String, Value)>>,
        seals: Mutex<u32>,
    }

    impl S3Sinks for FakeS3 {
        fn validate(&self, broker_tenant: &str, config: &Value) -> Result<(), String> {
            self.validated
                .lock()
                .unwrap()
                .push((broker_tenant.to_string(), config.clone()));
            match config.get("bucket").and_then(Value::as_str) {
                Some(b) if !b.is_empty() && !b.contains(' ') => Ok(()),
                _ => Err("bucket is not a bucket name: one name, no slash, no space".into()),
            }
        }

        fn seal(&self, secret: &str) -> Result<String, String> {
            if let Some(refusal) = self.no_key {
                return Err(refusal.replace("{secret}", secret));
            }
            let mut n = self.seals.lock().unwrap();
            *n += 1;
            let hex: String = secret
                .bytes()
                .map(|b| format!("{:02x}", b ^ 0x5a))
                .collect();
            Ok(format!("sealed:{n}:{hex}"))
        }
    }

    impl FakeS3 {
        fn sealed(&self) -> u32 {
            *self.seals.lock().unwrap()
        }
    }

    /// A seeded single binary whose broker hands the proxy `hook`, and one
    /// provisioned tenancy `acme` (its answer).
    async fn s3_world(hook: Arc<FakeS3>) -> (St, Arc<MemKv>, Value) {
        let (store, kv) = seeded("inprocess://self", None).await;
        let broker = stub_broker(Calls::default(), WORKS);
        let cfg = crate::config::test_config(&[]);
        let st = st_full(store, Upstream::InProcess(broker), cfg, Some(hook));
        let a = provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        (st, kv, a)
    }

    /// A sink config (no secret, no switch).
    fn sink(bucket: &str) -> Value {
        json!({
            "endpoint": "https://s3.eu-west-1.amazonaws.com",
            "region": "eu-west-1",
            "bucket": bucket,
            "accessKey": "AKIAIOSFODNN7EXAMPLE",
            "queues": "orders,payments",
            "format": "parquet",
        })
    }

    fn with_secret(mut config: Value, secret: &str) -> Value {
        config["secretKey"] = json!(secret);
        config
    }

    async fn put_sink(st: &St, slug: &str, body: Value) -> (StatusCode, Value) {
        call(
            st,
            "PUT",
            &format!("/api/cp/clusters/{slug}/s3"),
            Some(body),
        )
        .await
    }

    /// The stored row and its version.
    async fn sink_row(kv: &MemKv, cluster: Uuid) -> Option<kv::Doc<S3SinkDoc>> {
        kv::get::<S3SinkDoc>(kv, ns::S3SINKS, &schema::key(cluster))
            .await
            .unwrap()
    }

    /// The audit rows of `action`, as JSON text.
    async fn audit(kv: &MemKv, action: &str) -> Vec<String> {
        kv::scan::<schema::OperationDoc>(kv, ns::OPS, "#")
            .await
            .unwrap()
            .into_iter()
            .filter(|(_, d)| d.value.action == action)
            .map(|(_, d)| serde_json::to_string(&d.value).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn an_s3_sink_round_trips_redacted() {
        let hook = Arc::new(FakeS3::default());
        let (st, kv, a) = s3_world(hook.clone()).await;
        let cluster = id(&a["cluster_id"]);
        let tenant = a["broker_tenant_uuid"].clone();

        let (s, v) = put_sink(&st, "acme", with_secret(sink("lake"), SECRET)).await;
        assert_eq!(s, StatusCode::OK, "{v}");
        let at = v["updatedAt"].as_str().unwrap().to_string();
        assert!(at.ends_with('Z') && at.len() == 20, "{at}");
        assert_eq!(
            v,
            json!({
                "cluster": "acme", "tenant": tenant, "enabled": true, "config": sink("lake"),
                "secretKeySet": true, "updatedAt": at,
            })
        );
        // The broker checked the config alone, for the cluster's own tenant.
        assert_eq!(
            *hook.validated.lock().unwrap(),
            vec![(tenant.as_str().unwrap().to_string(), sink("lake"))]
        );
        // The row holds the secret sealed, and nothing else holds it at all.
        let row = sink_row(&kv, cluster).await.unwrap().value;
        assert_eq!(
            (row.cluster_id, row.broker_tenant, row.enabled),
            (cluster, id(&tenant), true)
        );
        assert_eq!(row.config, sink("lake"));
        assert!(row.secret_key_sealed.starts_with("sealed:1:"));
        let stored = kv::get::<Value>(kv.as_ref(), ns::S3SINKS, &schema::key(cluster))
            .await
            .unwrap()
            .unwrap()
            .value
            .to_string();
        assert!(!stored.contains(SECRET), "{stored}");
        assert!(!format!("{row:?}").contains(&row.secret_key_sealed));
        let set = audit(&kv, "s3_sink_set").await;
        assert_eq!(set.len(), 1);
        assert!(
            !set[0].contains(SECRET) && !set[0].contains("sealed:"),
            "{set:?}"
        );

        let (s, got) = call(&st, "GET", "/api/cp/clusters/acme/s3", None).await;
        assert_eq!((s, &got), (StatusCode::OK, &v));
        for body in [&v, &got] {
            let text = body.to_string();
            assert!(
                !text.contains(SECRET) && !text.contains("sealed:"),
                "{text}"
            );
        }

        let (s, d) = call(&st, "DELETE", "/api/cp/clusters/acme/s3", None).await;
        assert_eq!(
            (s, d),
            (StatusCode::OK, json!({"cluster": "acme", "removed": true}))
        );
        assert!(kv.keys(ns::S3SINKS).is_empty());
        assert_eq!(audit(&kv, "s3_sink_removed").await.len(), 1);
        let (s, d) = call(&st, "DELETE", "/api/cp/clusters/acme/s3", None).await;
        assert_eq!(
            (s, d),
            (StatusCode::OK, json!({"cluster": "acme", "removed": false})),
            "a retry"
        );
        assert_eq!(
            audit(&kv, "s3_sink_removed").await.len(),
            1,
            "a retry writes nothing"
        );
        let (s, v) = call(&st, "GET", "/api/cp/clusters/acme/s3", None).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::NOT_FOUND, json!("s3_unset"))
        );
    }

    #[tokio::test]
    async fn the_secret_is_required_first_and_kept_when_omitted() {
        let hook = Arc::new(FakeS3::default());
        let (st, kv, a) = s3_world(hook.clone()).await;
        let cluster = id(&a["cluster_id"]);
        // No sink yet: no secret to keep.
        let mut null = sink("lake");
        null["secretKey"] = Value::Null;
        for body in [sink("lake"), null] {
            let (s, v) = put_sink(&st, "acme", body).await;
            assert_eq!(
                (s, v["code"].clone()),
                (StatusCode::BAD_REQUEST, json!("invalid")),
                "{v}"
            );
            assert!(
                v["error"]
                    .as_str()
                    .unwrap()
                    .contains("secretKey is required"),
                "{v}"
            );
        }
        assert!(kv.keys(ns::S3SINKS).is_empty());
        assert_eq!(hook.sealed(), 0, "nothing was sealed");

        let (s, _) = put_sink(&st, "acme", with_secret(sink("lake"), SECRET)).await;
        assert_eq!(s, StatusCode::OK);
        let first = sink_row(&kv, cluster).await.unwrap().value;

        // Omitted: the stored secret stays, everything else is the request's.
        let mut off = sink("lake");
        off["prefix"] = json!("mirror/acme");
        off["enabled"] = json!(false);
        let (s, v) = put_sink(&st, "acme", off).await;
        assert_eq!(s, StatusCode::OK, "{v}");
        assert_eq!(v["enabled"], json!(false));
        assert_eq!(v["config"]["prefix"], json!("mirror/acme"));
        assert!(v["config"].get("enabled").is_none(), "{v}");
        let kept = sink_row(&kv, cluster).await.unwrap().value;
        assert_eq!(kept.secret_key_sealed, first.secret_key_sealed);
        assert!(!kept.enabled);
        assert_eq!(hook.sealed(), 1);
        let set = audit(&kv, "s3_sink_set").await;
        assert!(
            set.iter().any(|o| o.contains(r#""secret":"kept""#)),
            "{set:?}"
        );

        // A new secret replaces it; `enabled` defaults to true again.
        let (s, v) = put_sink(&st, "acme", with_secret(sink("lake"), "rotated-secret")).await;
        assert_eq!((s, v["enabled"].clone()), (StatusCode::OK, json!(true)));
        let rotated = sink_row(&kv, cluster).await.unwrap().value;
        assert!(rotated.secret_key_sealed.starts_with("sealed:2:"));
        assert!(
            rotated.config.get("prefix").is_none(),
            "replaced, not merged"
        );
    }

    /// The bus may send a call twice: the same request leaves the same sink.
    /// One with a secret re-seals it and moves the revision (a rotation looks
    /// exactly like a repeat, and the broker must see a rotation); one
    /// without writes nothing and answers the very same.
    #[tokio::test]
    async fn an_s3_put_repeats_quietly() {
        let hook = Arc::new(FakeS3::default());
        let (st, kv, a) = s3_world(hook.clone()).await;
        let cluster = id(&a["cluster_id"]);
        let body = with_secret(sink("lake"), SECRET);
        let (s1, mut v1) = put_sink(&st, "acme", body.clone()).await;
        let r1 = sink_row(&kv, cluster).await.unwrap().value;
        let (s2, mut v2) = put_sink(&st, "acme", body).await;
        let r2 = sink_row(&kv, cluster).await.unwrap().value;
        assert_eq!((s1, s2), (StatusCode::OK, StatusCode::OK));
        assert_ne!(r1.secret_key_sealed, r2.secret_key_sealed, "sealed afresh");
        assert!(
            r2.updated_at_us > r1.updated_at_us,
            "a written secret moves the revision"
        );
        assert_eq!((&r1.config, r1.enabled), (&r2.config, r2.enabled));
        let last = v2.clone();
        for v in [&mut v1, &mut v2] {
            v.as_object_mut().unwrap().remove("updatedAt");
        }
        assert_eq!(v1, v2, "the same sink but for its revision");

        let version = sink_row(&kv, cluster).await.unwrap().version;
        let ops = kv.keys(ns::OPS).len();
        for _ in 0..2 {
            let (s, v) = put_sink(&st, "acme", sink("lake")).await;
            assert_eq!((s, &v), (StatusCode::OK, &last));
        }
        assert_eq!(sink_row(&kv, cluster).await.unwrap().version, version);
        assert_eq!(
            kv.keys(ns::OPS).len(),
            ops,
            "nothing changed: nothing written"
        );
        assert_eq!(hook.sealed(), 2);
    }

    #[tokio::test]
    async fn the_brokers_refusal_is_a_400_with_its_sentence_and_writes_nothing() {
        let hook = Arc::new(FakeS3::default());
        let (st, kv, _) = s3_world(hook.clone()).await;
        let ops = kv.keys(ns::OPS).len();
        let (s, v) = put_sink(&st, "acme", with_secret(sink("my lake"), SECRET)).await;
        assert_eq!(
            (s, v),
            (
                StatusCode::BAD_REQUEST,
                json!({"error": "bucket is not a bucket name: one name, no slash, no space", "code": "invalid"})
            )
        );
        assert_eq!(hook.sealed(), 0, "checked before anything is sealed");
        assert!(kv.keys(ns::S3SINKS).is_empty());
        assert_eq!(kv.keys(ns::OPS).len(), ops);
    }

    /// The hook's sentence is the answer when it names the key, else ours
    /// carries it; the secret is scrubbed out of either.
    #[tokio::test]
    async fn a_cell_without_an_encryption_key_refuses_a_secret_and_writes_nothing() {
        let cases = [
            (
                "QUEEN_ENCRYPTION_KEY is not set: cannot seal {secret}",
                "QUEEN_ENCRYPTION_KEY is not set: cannot seal ***".to_string(),
            ),
            (
                "sealing {secret} failed",
                "this cell has no QUEEN_ENCRYPTION_KEY, and an S3 secret is only ever stored \
                 sealed with it: set QUEEN_ENCRYPTION_KEY (the same on every node) and retry \
                 (sealing *** failed)"
                    .to_string(),
            ),
        ];
        for (refusal, want) in cases {
            let hook = Arc::new(FakeS3 {
                no_key: Some(refusal),
                ..Default::default()
            });
            let (st, kv, _) = s3_world(hook).await;
            let ops = kv.keys(ns::OPS).len();
            let (s, v) = put_sink(&st, "acme", with_secret(sink("lake"), SECRET)).await;
            assert_eq!(
                (s, v),
                (
                    StatusCode::CONFLICT,
                    json!({"error": want, "code": "encryption_required"})
                )
            );
            assert!(kv.keys(ns::S3SINKS).is_empty());
            assert_eq!(kv.keys(ns::OPS).len(), ops);
        }
    }

    /// A body the surface refuses is a 400 that never quotes the body, and
    /// nothing named like a secret is ever stored in clear.
    #[tokio::test]
    async fn malformed_s3_bodies_are_400_and_never_quote_the_secret() {
        let hook = Arc::new(FakeS3::default());
        let (st, kv, _) = s3_world(hook.clone()).await;
        let mut big = with_secret(sink("lake"), SECRET);
        big["queues"] = json!("q,".repeat(data::S3_CONFIG_MAX_BYTES / 2));
        let long = format!("{SECRET}{}", "x".repeat(MAX_S3_SECRET_BYTES));
        let cases = [
            with_secret(sink("lake"), &long),
            json!([SECRET]),
            json!(SECRET),
            json!({"bucket": "lake", "secretKey": 42}),
            json!({"bucket": "lake", "secretKey": "  "}),
            json!({"bucket": "lake", "secretKey": [SECRET]}),
            json!({"bucket": "lake", "secretKey": SECRET, "enabled": "yes"}),
            json!({"bucket": "lake", "secret_key": SECRET}),
            json!({"bucket": "lake", "secretKey": SECRET, "SecretAccessKey": SECRET}),
            big,
        ];
        for body in cases {
            let (s, v) = put_sink(&st, "acme", body.clone()).await;
            assert_eq!(
                (s, v["code"].clone()),
                (StatusCode::BAD_REQUEST, json!("invalid")),
                "{v}"
            );
            assert!(!v.to_string().contains(SECRET), "{v}");
        }
        // Not JSON at all.
        let app = Router::new()
            .nest("/api/cp", router())
            .with_state(st.clone());
        let req = Request::builder()
            .method("PUT")
            .uri("/api/cp/clusters/acme/s3")
            .header("x-queen-cp-token", TOKEN)
            .body(Body::from(format!(r#"{{"secretKey": "{SECRET}"#)))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let text = axum::body::to_bytes(resp.into_body(), 1 << 16)
            .await
            .unwrap();
        assert!(!String::from_utf8_lossy(&text).contains(SECRET));
        assert!(kv.keys(ns::S3SINKS).is_empty());
        assert!(hook.validated.lock().unwrap().is_empty() && hook.sealed() == 0);
    }

    /// The existing rule: no sink acts on the broker's default tenant or the
    /// proxy's own, nor on any while the tenant header is off. Reading and
    /// removing touch only the proxy's own row.
    #[tokio::test]
    async fn no_s3_sink_is_set_on_a_reserved_tenant_or_with_the_tenant_header_off() {
        for reserved in [DEFAULT_TENANT, schema::PROXY_TENANT] {
            let hook = Arc::new(FakeS3::default());
            let (st, kv, a) = s3_world(hook.clone()).await;
            rebind(&kv, id(&a["cluster_id"]), reserved).await;
            let (s, v) = put_sink(&st, "acme", with_secret(sink("lake"), SECRET)).await;
            assert_eq!(
                (s, v["code"].clone()),
                (StatusCode::CONFLICT, json!("system_tenant")),
                "{reserved}: {v}"
            );
            assert!(kv.keys(ns::S3SINKS).is_empty());
            assert!(hook.validated.lock().unwrap().is_empty() && hook.sealed() == 0);
            let (s, v) = call(&st, "GET", "/api/cp/clusters/acme/s3", None).await;
            assert_eq!(
                (s, v["code"].clone()),
                (StatusCode::NOT_FOUND, json!("s3_unset"))
            );
            let (s, v) = call(&st, "DELETE", "/api/cp/clusters/acme/s3", None).await;
            assert_eq!((s, v["removed"].clone()), (StatusCode::OK, json!(false)));
        }

        let (store, kv) = seeded("inprocess://self", None).await;
        let mut cfg = crate::config::test_config(&[]);
        cfg.send_tenant_header = false;
        let hook: Arc<dyn S3Sinks> = Arc::new(FakeS3::default());
        let st = st_full(store, Upstream::InProcess(Router::new()), cfg, Some(hook));
        provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        let (s, v) = put_sink(&st, "acme", with_secret(sink("lake"), SECRET)).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::CONFLICT, json!("system_tenant"))
        );
        assert!(kv.keys(ns::S3SINKS).is_empty());
    }

    #[tokio::test]
    async fn an_unknown_cluster_is_404_on_every_s3_route() {
        let (st, kv, _) = s3_world(Arc::new(FakeS3::default())).await;
        let body = with_secret(sink("lake"), SECRET);
        for (method, body) in [("PUT", Some(body)), ("GET", None), ("DELETE", None)] {
            let (s, v) = call(&st, method, "/api/cp/clusters/nope/s3", body.clone()).await;
            assert_eq!(
                (s, v["code"].clone()),
                (StatusCode::NOT_FOUND, json!("cluster_unknown")),
                "{method}"
            );
            let (s, v) = call(&st, method, "/api/cp/clusters/Bad_Slug/s3", body).await;
            assert_eq!(
                (s, v["code"].clone()),
                (StatusCode::BAD_REQUEST, json!("invalid")),
                "{method}"
            );
        }
        assert!(kv.keys(ns::S3SINKS).is_empty());
    }

    /// A broker built without the sink has no S3 routes: 404 `s3_unavailable`
    /// behind the same token.
    #[tokio::test]
    async fn without_the_brokers_sink_the_s3_routes_are_404() {
        let (st, kv, _) = world().await;
        provisioned(
            &st,
            prov("acme", "acme", Uuid::new_v4(), "a@acme.io", &hash(1)),
        )
        .await;
        let body = with_secret(sink("lake"), SECRET);
        for (method, body) in [("PUT", Some(body)), ("GET", None), ("DELETE", None)] {
            let uri = "/api/cp/clusters/acme/s3";
            let (s, v) = call(&st, method, uri, body.clone()).await;
            assert_eq!(
                (s, v["code"].clone()),
                (StatusCode::NOT_FOUND, json!("s3_unavailable")),
                "{method}: {v}"
            );
            let wrong = [("x-queen-cp-token", "wrong")];
            let (s, _) = call_with(&st, method, uri, body, &wrong).await;
            assert_eq!(s, StatusCode::UNAUTHORIZED, "{method}: the token first");
        }
        assert!(kv.keys(ns::S3SINKS).is_empty());
    }

    /// A wipe gets no sink, and the sinks of a tenant go with its purge and
    /// with its delete; a neighbour keeps its own.
    #[tokio::test]
    async fn a_wipe_refuses_an_s3_sink_and_the_purge_and_the_delete_remove_them() {
        let hook = Arc::new(FakeS3::default());
        let (st, kv, a) = s3_world(hook).await;
        let (s, c2) = call(
            &st,
            "POST",
            "/api/cp/clusters",
            Some(json!({"tenant_slug": "acme", "slug": "acme-2"})),
        )
        .await;
        assert_eq!(s, StatusCode::CREATED, "{c2}");
        let b = provisioned(
            &st,
            prov("beta", "beta", Uuid::new_v4(), "b@beta.io", &hash(2)),
        )
        .await;
        for slug in ["acme", "acme-2", "beta"] {
            let (s, v) = put_sink(&st, slug, with_secret(sink("lake"), SECRET)).await;
            assert_eq!(s, StatusCode::OK, "{slug}: {v}");
        }
        let beta_row = vec![schema::key(id(&b["cluster_id"]))];
        assert_eq!(kv.keys(ns::S3SINKS).len(), 3);

        // A cluster being deleted gets no sink...
        let c2_status = format!("/api/cp/clusters/{}/status", c2["id"].as_str().unwrap());
        call(&st, "PUT", &c2_status, Some(json!({"status": "deleting"}))).await;
        let (s, v) = put_sink(&st, "acme-2", with_secret(sink("lake"), SECRET)).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::CONFLICT, json!("deleting")),
            "{v}"
        );
        call(&st, "PUT", &c2_status, Some(json!({"status": "active"}))).await;

        // ...nor does one whose tenant is; reading and removing still answer.
        call(
            &st,
            "PUT",
            "/api/cp/tenants/acme/status",
            Some(json!({"status": "deleting"})),
        )
        .await;
        let before = sink_row(&kv, id(&a["cluster_id"])).await.unwrap();
        let (s, v) = put_sink(&st, "acme", sink("other")).await;
        assert_eq!(
            (s, v["code"].clone()),
            (StatusCode::CONFLICT, json!("deleting")),
            "{v}"
        );
        let after = sink_row(&kv, id(&a["cluster_id"])).await.unwrap();
        assert_eq!(after.version, before.version, "nothing written");
        let (s, _) = call(&st, "GET", "/api/cp/clusters/acme/s3", None).await;
        assert_eq!(s, StatusCode::OK);

        // The purge removes the tenant's sinks, and only the tenant's.
        let (s, v) = call(&st, "POST", "/api/cp/tenants/acme/purge", None).await;
        assert_eq!(s, StatusCode::OK, "{v}");
        assert_eq!(kv.keys(ns::S3SINKS), beta_row);

        // A row a cut-short wipe left behind goes with the tenant's delete.
        kv::write(
            kv.as_ref(),
            vec![kv::put_op(
                ns::S3SINKS,
                &schema::key(id(&a["cluster_id"])),
                &before.value,
                Expect::Any,
                Ttl::Forever,
                false,
            )],
        )
        .await
        .unwrap();
        let (s, v) = call(&st, "DELETE", "/api/cp/tenants/acme", None).await;
        assert_eq!(
            (s, v["existed"].clone()),
            (StatusCode::OK, json!(true)),
            "{v}"
        );
        assert_eq!(kv.keys(ns::S3SINKS), beta_row);

        // A forced delete, no purge first: the cascade alone removes it.
        let (s, v) = call(&st, "DELETE", "/api/cp/tenants/beta?force=true", None).await;
        assert_eq!(
            (s, v["existed"].clone()),
            (StatusCode::OK, json!(true)),
            "{v}"
        );
        assert!(kv.keys(ns::S3SINKS).is_empty());
    }
}
