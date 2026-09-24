//! A small control-plane API for deployments without a SQL console — above
//! all the single binary, whose state is the broker's replicated KV
//! (PLAN_SINGLE_BINARY.md W3/W4). What an operator or a provisioning script
//! did with `psql` against the proxy's Postgres, over HTTP:
//!
//! | route | does |
//! |---|---|
//! | `POST /api/cp/bootstrap` | `bootstrap_tenant`: tenant + cluster + admin + key |
//! | `POST /api/cp/tenants` | `create_tenant` |
//! | `POST /api/cp/clusters` | ensure a cluster (and its tenant) exists |
//! | `GET /api/cp/clusters/:slug` | the cluster's ids |
//! | `PUT /api/cp/clusters/:id/overrides` | `set_limit_override` (body JSON or `null`) |
//! | `PUT /api/cp/clusters/:id/status` | `set_cluster_status` |
//! | `POST /api/cp/keys` | `issue_api_key` (the caller hashes; the plaintext never reaches the proxy) |
//! | `DELETE /api/cp/keys/:id` | `revoke_api_key` |
//!
//! Guarded by `QUEEN_PROXY_CP_TOKEN` (header `x-queen-cp-token`, compared in
//! constant time). Unset: every route answers 404, the surface does not exist.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::state::St;
use crate::store::data::{self, ClusterKey, DataError, Lookup};

pub fn router() -> Router<St> {
    Router::new()
        .route("/bootstrap", post(bootstrap))
        .route("/tenants", post(create_tenant))
        .route("/clusters", post(ensure_cluster))
        .route("/clusters/:slug", get(get_cluster))
        .route("/clusters/:id/overrides", put(set_overrides))
        .route("/clusters/:id/status", put(set_status))
        .route("/keys", post(issue_key))
        .route("/keys/:id", delete(revoke_key))
}

fn token() -> Option<String> {
    std::env::var("QUEEN_PROXY_CP_TOKEN")
        .ok()
        .filter(|t| !t.trim().is_empty())
}

/// 404 when the surface is off, 401 on a wrong token.
fn guard(h: &HeaderMap) -> Result<(), Response> {
    let Some(want) = token() else {
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

fn fail(e: DataError) -> Response {
    let (status, code) = match &e {
        DataError::Invalid(_) => (StatusCode::BAD_REQUEST, "invalid"),
        DataError::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
        DataError::Unavailable(_) => (StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
        DataError::NoStore => (StatusCode::SERVICE_UNAVAILABLE, "no_store"),
    };
    crate::errors::json_error(status, code, &e.to_string())
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

async fn bootstrap(State(st): State<St>, h: HeaderMap, Json(b): Json<BootstrapIn>) -> Response {
    if let Err(r) = guard(&h) {
        return r;
    }
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
struct TenantIn {
    slug: String,
    name: Option<String>,
}

async fn create_tenant(State(st): State<St>, h: HeaderMap, Json(t): Json<TenantIn>) -> Response {
    if let Err(r) = guard(&h) {
        return r;
    }
    let name = t.name.unwrap_or_else(|| t.slug.clone());
    match data::create_tenant(&st.store, &t.slug, &name).await {
        Ok(id) => (StatusCode::CREATED, Json(json!({"id": id}))).into_response(),
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
    crate::store::kv::get::<Uuid>(kv.as_ref(), crate::store::schema::ns::TENANT_SLUG, &crate::store::schema::key(slug))
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

async fn ensure_cluster(State(st): State<St>, h: HeaderMap, Json(c): Json<ClusterIn>) -> Response {
    if let Err(r) = guard(&h) {
        return r;
    }
    if let Lookup::Found(ctx) = data::lookup_cluster(&st.store, &ClusterKey::Slug(c.slug.clone())).await {
        return (StatusCode::OK, Json(cluster_json(&ctx))).into_response();
    }
    let tenant = match tenant_id(&st, &c.tenant_slug).await {
        Some(t) => t,
        None => {
            let name = c.tenant_name.clone().unwrap_or_else(|| c.tenant_slug.clone());
            match data::create_tenant(&st.store, &c.tenant_slug, &name).await {
                Ok(id) => id,
                // Created concurrently: read it back.
                Err(DataError::Conflict(_)) => match tenant_id(&st, &c.tenant_slug).await {
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
    match data::create_cluster(&st.store, tenant, &c.slug, &plan, cell).await {
        Ok(_) | Err(DataError::Conflict(_)) => {}
        Err(e) => return fail(e),
    }
    match data::lookup_cluster(&st.store, &ClusterKey::Slug(c.slug)).await {
        Lookup::Found(ctx) => (StatusCode::CREATED, Json(cluster_json(&ctx))).into_response(),
        _ => fail(DataError::Unavailable("cluster not readable yet".into())),
    }
}

fn cluster_json(ctx: &crate::state::ClusterCtx) -> Value {
    json!({
        "id": ctx.cluster_id,
        "tenant_id": ctx.tenant_id,
        "broker_tenant_uuid": ctx.broker_tenant,
        "slug": ctx.slug,
    })
}

async fn get_cluster(State(st): State<St>, h: HeaderMap, Path(slug): Path<String>) -> Response {
    if let Err(r) = guard(&h) {
        return r;
    }
    match data::lookup_cluster(&st.store, &ClusterKey::Slug(slug)).await {
        Lookup::Found(ctx) => (StatusCode::OK, Json(cluster_json(&ctx))).into_response(),
        Lookup::Absent => crate::errors::err_404("cluster_unknown", "no such cluster"),
        Lookup::Unavailable => fail(DataError::Unavailable("store".into())),
    }
}

async fn set_overrides(State(st): State<St>, h: HeaderMap, Path(id): Path<Uuid>, Json(v): Json<Value>) -> Response {
    if let Err(r) = guard(&h) {
        return r;
    }
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

async fn set_status(State(st): State<St>, h: HeaderMap, Path(id): Path<Uuid>, Json(s): Json<StatusIn>) -> Response {
    if let Err(r) = guard(&h) {
        return r;
    }
    match data::set_cluster_status(&st.store, id, &s.status).await {
        Ok(()) => (StatusCode::OK, Json(json!({"ok": true}))).into_response(),
        Err(e) => fail(e),
    }
}

#[derive(Deserialize)]
struct KeyIn {
    cluster_id: Uuid,
    name: String,
    key_hash: String,
    scopes: Vec<String>,
}

async fn issue_key(State(st): State<St>, h: HeaderMap, Json(k): Json<KeyIn>) -> Response {
    if let Err(r) = guard(&h) {
        return r;
    }
    match data::issue_api_key(&st.store, k.cluster_id, &k.name, &k.key_hash, &k.scopes).await {
        Ok(id) => (StatusCode::CREATED, Json(json!({"id": id}))).into_response(),
        Err(e) => fail(e),
    }
}

async fn revoke_key(State(st): State<St>, h: HeaderMap, Path(id): Path<Uuid>) -> Response {
    if let Err(r) = guard(&h) {
        return r;
    }
    match data::revoke_api_key(&st.store, id).await {
        Ok(()) => (StatusCode::OK, Json(json!({"ok": true}))).into_response(),
        Err(e) => fail(e),
    }
}
