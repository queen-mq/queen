//! Cell-operator account management (`/api/operator/*`).
//!
//! These routes are served directly by the proxy because users, tenants and
//! cluster roles live in pxdb rather than in the broker. Every request first
//! resolves the acting cluster and requires a live human operator. The acting
//! cluster supplies the cell boundary: callers can only see tenants with a
//! cluster on that cell and can only change roles on those clusters.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_postgres::error::SqlState;
use uuid::Uuid;

use crate::errors;
use crate::state::{ClusterCtx, Principal, St};

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
    if st.db.is_none() {
        return Err(errors::json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "not_configured",
            "operator user management requires pxdb",
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

async fn cell_for(
    client: &tokio_postgres::Client,
    ctx: &ClusterCtx,
) -> Result<(String, String), Response> {
    let row = client
        .query_opt(
            "SELECT ce.id::text, ce.slug
               FROM queen_proxy.clusters c
               JOIN queen_proxy.cells ce ON ce.id = c.cell_id
              WHERE c.id = $1::text::uuid",
            &[&ctx.cluster_id.to_string()],
        )
        .await
        .map_err(|e| {
            tracing::warn!(target: "operator", err = %e, "cell lookup failed");
            errors::err_502("cell lookup failed")
        })?;
    row.map(|r| (r.get(0), r.get(1)))
        .ok_or_else(|| errors::err_421("acting cluster no longer exists"))
}

async fn list_users(State(st): State<St>, headers: HeaderMap) -> Response {
    let (ctx, _actor_id) = match operator_ctx(&st, &headers, "/api/operator/users").await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let pool = st.db.as_ref().expect("operator_ctx guarantees pxdb");
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "list_users: pool.get failed");
            return errors::err_502("pxdb unavailable");
        }
    };
    let (cell_id, cell_slug) = match cell_for(&client, &ctx).await {
        Ok(v) => v,
        Err(e) => return e,
    };

    let tenant_rows = match client
        .query(
            "SELECT DISTINCT t.id::text, t.slug, t.name
               FROM queen_proxy.tenants t
               JOIN queen_proxy.clusters c ON c.tenant_id = t.id
              WHERE c.cell_id = $1::text::uuid
              ORDER BY t.slug",
            &[&cell_id],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "tenant list failed");
            return errors::err_502("tenant list failed");
        }
    };
    let tenants: Vec<Value> = tenant_rows
        .iter()
        .map(|r| json!({ "id": r.get::<_, String>(0), "slug": r.get::<_, String>(1), "name": r.get::<_, String>(2) }))
        .collect();

    let cluster_rows = match client
        .query(
            "SELECT id::text, slug, tenant_id::text, status
               FROM queen_proxy.clusters
              WHERE cell_id = $1::text::uuid
              ORDER BY slug",
            &[&cell_id],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "cluster list failed");
            return errors::err_502("cluster list failed");
        }
    };
    let clusters: Vec<Value> = cluster_rows
        .iter()
        .map(|r| {
            json!({
                "id": r.get::<_, String>(0),
                "slug": r.get::<_, String>(1),
                "tenant_id": r.get::<_, String>(2),
                "status": r.get::<_, String>(3),
            })
        })
        .collect();

    let user_rows = match client
        .query(
            "SELECT u.id::text, u.email, u.name, u.tenant_id::text, t.slug,
                    u.is_operator, (u.password_hash IS NOT NULL),
                    to_char(u.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
                    to_char(u.last_login_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
               FROM queen_proxy.users u
               JOIN queen_proxy.tenants t ON t.id = u.tenant_id
              WHERE EXISTS (
                    SELECT 1 FROM queen_proxy.clusters c
                     WHERE c.tenant_id = u.tenant_id
                       AND c.cell_id = $1::text::uuid)
              ORDER BY t.slug, u.email",
            &[&cell_id],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "user list failed");
            return errors::err_502("user list failed");
        }
    };

    let mut user_index = HashMap::new();
    let mut users: Vec<Value> = user_rows
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let id = r.get::<_, String>(0);
            user_index.insert(id.clone(), i);
            json!({
                "id": id,
                "email": r.get::<_, String>(1),
                "name": r.get::<_, Option<String>>(2),
                "tenant_id": r.get::<_, String>(3),
                "tenant_slug": r.get::<_, String>(4),
                "is_operator": r.get::<_, bool>(5),
                "has_local_password": r.get::<_, bool>(6),
                "created_at": r.get::<_, String>(7),
                "last_login_at": r.get::<_, Option<String>>(8),
                "roles": [],
            })
        })
        .collect();

    let role_rows = match client
        .query(
            "SELECT cr.user_id::text, c.id::text, c.slug, cr.role
               FROM queen_proxy.cluster_roles cr
               JOIN queen_proxy.clusters c ON c.id = cr.cluster_id
              WHERE c.cell_id = $1::text::uuid
              ORDER BY c.slug",
            &[&cell_id],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "role list failed");
            return errors::err_502("role list failed");
        }
    };
    for row in role_rows {
        let user_id = row.get::<_, String>(0);
        let Some(index) = user_index.get(&user_id).copied() else {
            continue;
        };
        let role = json!({
            "cluster_id": row.get::<_, String>(1),
            "cluster_slug": row.get::<_, String>(2),
            "role": row.get::<_, String>(3),
        });
        users[index]["roles"]
            .as_array_mut()
            .expect("roles is an array")
            .push(role);
    }

    json_ok(json!({
        "cell": { "id": cell_id, "slug": cell_slug },
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

    let pool = st.db.as_ref().expect("operator_ctx guarantees pxdb");
    let mut client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "create_user: pool.get failed");
            return errors::err_502("pxdb unavailable");
        }
    };
    let (cell_id, _) = match cell_for(&client, &ctx).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let tx = match client.transaction().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "create_user: transaction failed");
            return errors::err_502("user creation failed");
        }
    };

    let scoped = match tx
        .query_opt(
            "SELECT 1
               FROM queen_proxy.clusters
              WHERE id = $1::text::uuid
                AND tenant_id = $2::text::uuid
                AND cell_id = $3::text::uuid",
            &[&cluster_id.to_string(), &tenant_id.to_string(), &cell_id],
        )
        .await
    {
        Ok(row) => row.is_some(),
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "create_user: scope check failed");
            return errors::err_502("cluster lookup failed");
        }
    };
    if !scoped {
        return errors::err_404("not_found", "tenant and cluster are not on this cell");
    }

    let user_row = match tx
        .query_one(
            "SELECT queen_proxy.create_user($1::text::uuid, $2, $3, $4)::text",
            &[&tenant_id.to_string(), &email, &password_hash, &provider],
        )
        .await
    {
        Ok(row) => row,
        Err(e) if e.code() == Some(&SqlState::UNIQUE_VIOLATION) => {
            return errors::json_error(
                StatusCode::CONFLICT,
                "conflict",
                "a user with this email already exists",
            )
        }
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "create_user failed");
            return errors::err_502("user creation failed");
        }
    };
    let user_id = user_row.get::<_, String>(0);

    if let Err(e) = tx
        .execute(
            "SELECT queen_proxy.set_user_name($1::text::uuid, $2)",
            &[&user_id, &name],
        )
        .await
    {
        tracing::warn!(target: "operator", err = %e, "initial user name failed");
        return errors::err_502("user name could not be saved");
    }

    if let Err(e) = tx
        .execute(
            "SELECT queen_proxy.grant_cluster_role($1::text::uuid, $2, $3)",
            &[&cluster_id.to_string(), &email, &role],
        )
        .await
    {
        tracing::warn!(target: "operator", err = %e, "initial role grant failed");
        return errors::err_502("initial role grant failed");
    }
    let meta =
        json!({ "email": email, "name": name, "provider": provider, "role": role }).to_string();
    if let Err(e) = tx
        .execute(
            "SELECT queen_proxy.record_operation($1::text::uuid, $2::text::uuid, 'user', $3::text::uuid, 'operator_user_created', $4, $5::text::jsonb)",
            &[&tenant_id.to_string(), &cluster_id.to_string(), &actor_id.to_string(), &user_id, &meta],
        )
        .await
    {
        tracing::warn!(target: "operator", err = %e, "operator user audit failed");
        return errors::err_502("user creation audit failed");
    }
    if let Err(e) = tx.commit().await {
        tracing::warn!(target: "operator", err = %e, "create_user commit failed");
        return errors::err_502("user creation failed");
    }

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

    let pool = st.db.as_ref().expect("operator_ctx guarantees pxdb");
    let mut client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "update_user: pool.get failed");
            return errors::err_502("pxdb unavailable");
        }
    };
    let (cell_id, _) = match cell_for(&client, &ctx).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let tx = match client.transaction().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "update_user: transaction failed");
            return errors::err_502("user update failed");
        }
    };
    let target = match tx
        .query_opt(
            "SELECT u.tenant_id::text, u.name
               FROM queen_proxy.users u
              WHERE u.id = $1::text::uuid
                AND EXISTS (
                    SELECT 1 FROM queen_proxy.clusters c
                     WHERE c.tenant_id = u.tenant_id
                       AND c.cell_id = $2::text::uuid)",
            &[&user_id.to_string(), &cell_id],
        )
        .await
    {
        Ok(Some(row)) => row,
        Ok(None) => return errors::err_404("not_found", "user is not on this cell"),
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "update_user: scope lookup failed");
            return errors::err_502("user lookup failed");
        }
    };
    let tenant_id = target.get::<_, String>(0);
    let old_name = target.get::<_, Option<String>>(1);
    if old_name.as_deref() == Some(name.as_str()) {
        return json_ok(json!({ "ok": true, "id": user_id, "name": name }));
    }

    if let Err(e) = tx
        .execute(
            "SELECT queen_proxy.set_user_name($1::text::uuid, $2)",
            &[&user_id.to_string(), &name],
        )
        .await
    {
        tracing::warn!(target: "operator", err = %e, "set_user_name failed");
        return errors::err_502("user update failed");
    }
    let meta = json!({ "old_name": old_name, "name": name }).to_string();
    if let Err(e) = tx
        .execute(
            "SELECT queen_proxy.record_operation($1::text::uuid, NULL, 'user', $2::text::uuid, 'operator_user_updated', $3, $4::text::jsonb)",
            &[&tenant_id, &actor_id.to_string(), &user_id.to_string(), &meta],
        )
        .await
    {
        tracing::warn!(target: "operator", err = %e, "operator user update audit failed");
        return errors::err_502("user update audit failed");
    }
    if let Err(e) = tx.commit().await {
        tracing::warn!(target: "operator", err = %e, "update_user commit failed");
        return errors::err_502("user update failed");
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

    let pool = st.db.as_ref().expect("operator_ctx guarantees pxdb");
    let mut client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "change_role: pool.get failed");
            return errors::err_502("pxdb unavailable");
        }
    };
    let (cell_id, _) = match cell_for(&client, &ctx).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let tx = match client.transaction().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "change_role: transaction failed");
            return errors::err_502("role change failed");
        }
    };
    if let Err(e) = tx
        .batch_execute("LOCK TABLE queen_proxy.cluster_roles IN SHARE ROW EXCLUSIVE MODE")
        .await
    {
        tracing::warn!(target: "operator", err = %e, "role lock failed");
        return errors::err_502("role change failed");
    }

    let standing = match tx
        .query_opt(
            "SELECT u.email, u.tenant_id::text, cr.role,
                    (SELECT count(*) FROM queen_proxy.cluster_roles admins
                      WHERE admins.cluster_id = c.id AND admins.role = 'admin')
               FROM queen_proxy.users u
               JOIN queen_proxy.clusters c
                 ON c.id = $2::text::uuid AND c.tenant_id = u.tenant_id
               LEFT JOIN queen_proxy.cluster_roles cr
                 ON cr.user_id = u.id AND cr.cluster_id = c.id
              WHERE u.id = $1::text::uuid
                AND c.cell_id = $3::text::uuid",
            &[&user_id.to_string(), &cluster_id.to_string(), &cell_id],
        )
        .await
    {
        Ok(Some(row)) => row,
        Ok(None) => {
            return errors::err_404(
                "not_found",
                "user and cluster are not on this cell or tenant",
            )
        }
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "role standing lookup failed");
            return errors::err_502("role lookup failed");
        }
    };
    let email = standing.get::<_, String>(0);
    let tenant_id = standing.get::<_, String>(1);
    let current_role = standing.get::<_, Option<String>>(2);
    let admin_count = standing.get::<_, i64>(3);

    if new_role.is_none() && current_role.is_none() {
        return errors::err_404("not_found", "user has no access to this cluster");
    }
    if would_orphan_admins(current_role.as_deref(), admin_count, new_role.as_deref()) {
        return err_400("cannot remove or demote the last admin of this cluster");
    }

    let action = if let Some(role) = new_role.as_deref() {
        if let Err(e) = tx
            .execute(
                "SELECT queen_proxy.grant_cluster_role($1::text::uuid, $2, $3)",
                &[&cluster_id.to_string(), &email, &role],
            )
            .await
        {
            tracing::warn!(target: "operator", err = %e, "role grant failed");
            return errors::err_502("role grant failed");
        }
        "operator_role_granted"
    } else {
        if let Err(e) = tx
            .execute(
                "SELECT queen_proxy.revoke_cluster_role($1::text::uuid, $2)",
                &[&cluster_id.to_string(), &email],
            )
            .await
        {
            tracing::warn!(target: "operator", err = %e, "role revocation failed");
            return errors::err_502("role revocation failed");
        }
        "operator_role_revoked"
    };

    let meta =
        json!({ "email": email, "role": new_role, "previous_role": current_role }).to_string();
    if let Err(e) = tx
        .execute(
            "SELECT queen_proxy.record_operation($1::text::uuid, $2::text::uuid, 'user', $3::text::uuid, $4, $5, $6::text::jsonb)",
            &[&tenant_id, &cluster_id.to_string(), &actor_id.to_string(), &action, &user_id.to_string(), &meta],
        )
        .await
    {
        tracing::warn!(target: "operator", err = %e, "operator role audit failed");
        return errors::err_502("role change audit failed");
    }
    if let Err(e) = tx.commit().await {
        tracing::warn!(target: "operator", err = %e, "role change commit failed");
        return errors::err_502("role change failed");
    }

    st.keys.invalidate_role(user_id, cluster_id);
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
