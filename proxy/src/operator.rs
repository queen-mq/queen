//! Cell-operator account management (`/api/operator/*`).
//!
//! These routes are served directly by the proxy because users, tenants and
//! cluster roles live in the proxy's own store (the broker's KV, under the
//! proxy's system tenant — every read and write goes through `store::web`)
//! rather than in the broker's queues. Every request first resolves the
//! acting cluster and requires a live human operator. The acting cluster
//! supplies the cell boundary: callers can only see tenants with a cluster on
//! that cell and can only change roles on those clusters.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::errors;
use crate::state::{ClusterCtx, Principal, St};
use crate::store::web;

const VALID_ROLES: [&str; 4] = ["admin", "producer", "consumer", "viewer"];
const VALID_PROVIDERS: [&str; 3] = ["local", "google", "github"];

pub fn router() -> Router<St> {
    Router::new()
        .route("/users", get(list_users).post(create_user))
        .route("/users/:user_id", axum::routing::patch(update_user))
        .route(
            "/users/:user_id/roles/:cluster_id",
            axum::routing::put(set_role).delete(remove_role),
        )
}

/// Resolve the selected cluster and require the live operator capability.
/// Refusals deliberately use the gateway's operator-route 404 contract so a
/// tenant principal cannot use this surface to discover privileged features.
async fn operator_ctx(
    st: &St,
    headers: &HeaderMap,
    path: &str,
) -> Result<(Arc<ClusterCtx>, Uuid), Response> {
    if !st.cfg.operator_enabled {
        return Err(route_blocked());
    }
    if !st.store.is_some() {
        return Err(errors::json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "not_configured",
            "operator user management requires a store",
        ));
    }

    let route = crate::acting::resolve_route(st, headers).await?;
    let (ctx, principal) = match route {
        crate::acting::Route::Fixed(ctx) => {
            let principal = crate::acting::authenticate_for(st, headers, &ctx).await?;
            (ctx, principal)
        }
        crate::acting::Route::FromCredential => {
            crate::acting::resolve_from_credential(st, headers).await?
        }
        crate::acting::Route::UnknownHost => {
            return Err(crate::acting::unknown_host_refusal(st, headers).await)
        }
    };

    match principal {
        Principal::User {
            user_id,
            operator: true,
            ..
        } => {
            crate::acting::note_operator_access(user_id, &ctx.slug, path);
            Ok((ctx, user_id))
        }
        Principal::User { .. } | Principal::ApiKey { .. } => Err(route_blocked()),
    }
}

fn route_blocked() -> Response {
    errors::err_404(errors::CODE_ROUTE_BLOCKED, "not available")
}

async fn list_users(State(st): State<St>, headers: HeaderMap) -> Response {
    let (ctx, _actor_id) = match operator_ctx(&st, &headers, "/api/operator/users").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let listing = match web::operator_listing(&st.store, ctx.cluster_id).await {
        Ok(l) => l,
        Err(refusal) => return refusal.response(),
    };

    let tenants: Vec<Value> = listing
        .tenants
        .into_iter()
        .map(|t| json!({ "id": t.id, "slug": t.slug, "name": t.name }))
        .collect();
    let clusters: Vec<Value> = listing
        .clusters
        .into_iter()
        .map(|c| json!({ "id": c.id, "slug": c.slug, "tenant_id": c.tenant_id, "status": c.status }))
        .collect();

    let mut user_index = HashMap::new();
    let mut users: Vec<Value> = listing
        .users
        .into_iter()
        .enumerate()
        .map(|(i, u)| {
            user_index.insert(u.id.clone(), i);
            json!({
                "id": u.id,
                "email": u.email,
                "name": u.name,
                "tenant_id": u.tenant_id,
                "tenant_slug": u.tenant_slug,
                "is_operator": u.is_operator,
                "has_local_password": u.has_local_password,
                "created_at": u.created_at,
                "last_login_at": u.last_login_at,
                "roles": [],
            })
        })
        .collect();

    for r in listing.roles {
        let Some(index) = user_index.get(&r.user_id).copied() else {
            continue;
        };
        let role = json!({
            "cluster_id": r.cluster_id,
            "cluster_slug": r.cluster_slug,
            "role": r.role,
        });
        users[index]["roles"]
            .as_array_mut()
            .expect("roles is an array")
            .push(role);
    }

    json_ok(json!({
        "cell": { "id": listing.cell_id, "slug": listing.cell_slug },
        "tenants": tenants,
        "clusters": clusters,
        "users": users,
    }))
}

#[derive(Deserialize)]
struct CreateUserReq {
    tenant_id: String,
    email: String,
    name: String,
    provider: String,
    password: Option<String>,
    cluster_id: String,
    role: String,
}

async fn create_user(
    State(st): State<St>,
    headers: HeaderMap,
    Json(body): Json<CreateUserReq>,
) -> Response {
    let (ctx, actor_id) = match operator_ctx(&st, &headers, "/api/operator/users").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let tenant_id = match parse_uuid(&body.tenant_id, "tenant_id") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let cluster_id = match parse_uuid(&body.cluster_id, "cluster_id") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let email = match validate_email(&body.email) {
        Ok(v) => v,
        Err(msg) => return err_400(msg),
    };
    let name = match validate_name(&body.name) {
        Ok(v) => v,
        Err(msg) => return err_400(msg),
    };
    let provider = match validate_provider(&body.provider) {
        Ok(v) => v,
        Err(msg) => return err_400(msg),
    };
    let role = match validate_role(&body.role) {
        Ok(v) => v,
        Err(msg) => return err_400(msg),
    };
    let password = match validate_password(&provider, body.password.as_deref()) {
        Ok(v) => v,
        Err(msg) => return err_400(msg),
    };

    let password_hash = if let Some(password) = password {
        match tokio::task::spawn_blocking(move || bcrypt::hash(password, bcrypt::DEFAULT_COST))
            .await
        {
            Ok(Ok(hash)) => Some(hash),
            Ok(Err(e)) => {
                tracing::warn!(target: "operator", err = %e, "password hashing failed");
                return err_500("password could not be hashed");
            }
            Err(e) => {
                tracing::warn!(target: "operator", err = %e, "password hashing task failed");
                return err_500("password could not be hashed");
            }
        }
    } else {
        None
    };

    // One transaction: the scope check (tenant + cluster on this cell),
    // create_user, set_user_name, the first role and the audit row.
    let new_user = web::NewUser {
        tenant_id,
        cluster_id,
        email: &email,
        name: &name,
        provider: &provider,
        password_hash,
        role: &role,
    };
    let user_id = match web::operator_create_user(&st.store, ctx.cluster_id, &new_user, actor_id).await {
        Ok(id) => id,
        Err(refusal) => return refusal.response(),
    };
    web::invalidate_local(&st, &[cluster_id]);

    json_created(json!({
        "id": user_id,
        "email": email,
        "name": name,
        "tenant_id": tenant_id,
        "cluster_id": cluster_id,
        "role": role,
    }))
}

#[derive(Deserialize)]
struct UpdateUserReq {
    name: String,
}

async fn update_user(
    State(st): State<St>,
    headers: HeaderMap,
    Path(user_id): Path<String>,
    Json(body): Json<UpdateUserReq>,
) -> Response {
    let (ctx, actor_id) = match operator_ctx(&st, &headers, "/api/operator/users/:id").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let user_id = match parse_uuid(&user_id, "user_id") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let name = match validate_name(&body.name) {
        Ok(v) => v,
        Err(msg) => return err_400(msg),
    };

    // Unchanged or renamed, the answer is the same: the name it now has.
    if let Err(refusal) = web::operator_rename_user(&st.store, ctx.cluster_id, user_id, &name, actor_id).await {
        return refusal.response();
    }

    json_ok(json!({ "ok": true, "id": user_id, "name": name }))
}

#[derive(Deserialize)]
struct SetRoleReq {
    role: String,
}

async fn set_role(
    State(st): State<St>,
    headers: HeaderMap,
    Path((user_id, cluster_id)): Path<(String, String)>,
    Json(body): Json<SetRoleReq>,
) -> Response {
    let role = match validate_role(&body.role) {
        Ok(v) => v,
        Err(msg) => return err_400(msg),
    };
    change_role(&st, &headers, &user_id, &cluster_id, Some(role)).await
}

async fn remove_role(
    State(st): State<St>,
    headers: HeaderMap,
    Path((user_id, cluster_id)): Path<(String, String)>,
) -> Response {
    change_role(&st, &headers, &user_id, &cluster_id, None).await
}

async fn change_role(
    st: &St,
    headers: &HeaderMap,
    user_id_raw: &str,
    cluster_id_raw: &str,
    new_role: Option<String>,
) -> Response {
    let (ctx, actor_id) =
        match operator_ctx(st, headers, "/api/operator/users/:id/roles/:cluster").await {
            Ok(v) => v,
            Err(e) => return e,
        };
    let user_id = match parse_uuid(user_id_raw, "user_id") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let cluster_id = match parse_uuid(cluster_id_raw, "cluster_id") {
        Ok(v) => v,
        Err(e) => return e,
    };

    // The last-admin guard is decided on the same reads the write commits on
    // (version-checked admin seats).
    if let Err(refusal) = web::operator_change_role(
        &st.store,
        ctx.cluster_id,
        user_id,
        cluster_id,
        new_role.as_deref(),
        would_orphan_admins,
        actor_id,
    )
    .await
    {
        return refusal.response();
    }

    st.keys.invalidate_role(user_id, cluster_id);
    web::invalidate_local(st, &[cluster_id]);
    json_ok(json!({ "ok": true, "role": new_role }))
}

fn parse_uuid(raw: &str, field: &str) -> Result<Uuid, Response> {
    Uuid::parse_str(raw).map_err(|_| err_400(&format!("{field} must be a UUID")))
}

fn validate_email(raw: &str) -> Result<String, &'static str> {
    let email = raw.trim().to_lowercase();
    if email.is_empty() || !email.contains('@') || email.len() > 320 {
        return Err("email must be a valid address");
    }
    Ok(email)
}

fn validate_name(raw: &str) -> Result<String, &'static str> {
    let name = raw.trim();
    if name.is_empty() {
        return Err("name is required");
    }
    if name.chars().count() > 160 {
        return Err("name must be at most 160 characters");
    }
    Ok(name.to_string())
}

fn validate_provider(raw: &str) -> Result<String, &'static str> {
    let provider = raw.trim().to_lowercase();
    if !VALID_PROVIDERS.contains(&provider.as_str()) {
        return Err("provider must be one of local, google, github");
    }
    Ok(provider)
}

fn validate_role(raw: &str) -> Result<String, &'static str> {
    let role = raw.trim().to_lowercase();
    if !VALID_ROLES.contains(&role.as_str()) {
        return Err("role must be one of admin, producer, consumer, viewer");
    }
    Ok(role)
}

fn validate_password(provider: &str, raw: Option<&str>) -> Result<Option<String>, &'static str> {
    if provider != "local" {
        if raw.is_some_and(|p| !p.is_empty()) {
            return Err("password is only accepted for local users");
        }
        return Ok(None);
    }
    let password = raw.ok_or("local users require a password")?;
    if password.len() < 12 {
        return Err("password must be at least 12 characters");
    }
    if password.len() > 128 {
        return Err("password must be at most 128 characters");
    }
    Ok(Some(password.to_string()))
}

fn would_orphan_admins(
    current_role: Option<&str>,
    admin_count: i64,
    new_role: Option<&str>,
) -> bool {
    current_role == Some("admin") && new_role != Some("admin") && admin_count <= 1
}

fn json_ok(value: Value) -> Response {
    json_response(StatusCode::OK, value)
}

fn json_created(value: Value) -> Response {
    json_response(StatusCode::CREATED, value)
}

fn json_response(status: StatusCode, value: Value) -> Response {
    let mut response = (status, value.to_string()).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

fn err_400(message: &str) -> Response {
    errors::json_error(StatusCode::BAD_REQUEST, "invalid_request", message)
}

fn err_500(message: &str) -> Response {
    errors::json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_validation_normalizes_and_rejects_invalid_values() {
        assert_eq!(
            validate_email(" Dev@Example.com ").unwrap(),
            "dev@example.com"
        );
        assert!(validate_email("missing-at").is_err());
        assert_eq!(validate_name(" Ada Lovelace ").unwrap(), "Ada Lovelace");
        assert!(validate_name("   ").is_err());
        assert!(validate_name(&"a".repeat(161)).is_err());
        assert_eq!(validate_role(" Admin ").unwrap(), "admin");
        assert!(validate_role("owner").is_err());
        assert_eq!(validate_provider(" GitHub ").unwrap(), "github");
        assert!(validate_provider("saml").is_err());
    }

    #[test]
    fn local_password_policy_is_provider_aware() {
        assert!(validate_password("local", None).is_err());
        assert!(validate_password("local", Some("short")).is_err());
        assert_eq!(
            validate_password("local", Some("twelve-chars!")),
            Ok(Some("twelve-chars!".to_string()))
        );
        assert_eq!(validate_password("google", None), Ok(None));
        assert!(validate_password("google", Some("not-used-here")).is_err());
    }

    #[test]
    fn last_admin_guard_only_blocks_orphaning_change() {
        assert!(would_orphan_admins(Some("admin"), 1, Some("viewer")));
        assert!(would_orphan_admins(Some("admin"), 1, None));
        assert!(!would_orphan_admins(Some("admin"), 2, None));
        assert!(!would_orphan_admins(Some("viewer"), 1, None));
        assert!(!would_orphan_admins(Some("admin"), 1, Some("admin")));
    }
}
