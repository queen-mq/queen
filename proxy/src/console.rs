//! Cluster console: human-facing API (`/api/console/*`) + the embedded slim
//! Vue SPA (`/console`, `/console/*`). OWNER: Agent H.
//!
//! Scope (PLAN_QUEEN_PROXY_CLOUD.md §9 "cluster console" — deliberately NOT
//! the account/billing console, which is a separate private-repo product):
//! cluster overview (plan code, limits, status, and month-to-date usage
//! against the plan's monthly quota), usage graphs sourced from
//! `usage_minutes`, a read-only queues list (proxied through to the cell
//! broker with the SPA's own session Bearer), cluster API-key lifecycle and
//! cluster membership (`cluster_roles`, admin-only).
//! Human-only: every endpoint here requires a resolved `Principal::User` —
//! cluster API keys (`qk_...`) are rejected with 403 by design (task brief:
//! "API key qk_ NON valide qui, solo umani").
//!
//! Auth path (mirrors `auth::authenticate`, does not reimplement any of its
//! logic): resolve the cluster from Host (421 if unknown), require pxdb (503
//! `{"code":"not_configured"}` in dev-static mode — there is no user/key
//! table to serve without it), then lift the session cookie into a synthetic
//! `Authorization: Bearer` header when the request has no Authorization
//! header of its own, and hand off to `auth::authenticate` verbatim — so JWT
//! verification, the revocation deny-list, and cluster-role resolution
//! (`cluster_roles`, fail-closed on a missing row → the "no role on this
//! cluster" 403 the task brief asks for) all come from Agent C's code, not a
//! parallel reimplementation. A caveat inherited from `authenticate` as-is:
//! `QUEEN_PROXY_DEV_INSECURE=true` short-circuits to `Principal::ApiKey`
//! unconditionally, so the console is (correctly, if a little surprisingly)
//! *always* 403 in that mode — human session, not the insecure bypass, is
//! what dev-insecure was ever meant to skip on the data plane. See report.
//!
//! SPA embed: `rust_embed` over `console/dist` (Vite build, `base:
//! "/console/"`), same pattern as `server/src/handlers/static_files.rs`. The
//! `console/dist` directory is checked into the working tree (not
//! gitignored) so the crate builds without npm — see `console/README.md` for
//! the rebuild step.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get};
use axum::{Json, Router};
use rust_embed::RustEmbed;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::auth;
use crate::errors;
use crate::limits::PushBlock;
use crate::state::{ClusterCtx, ClusterStatus, EffectiveLimits, Features, Principal, Role, St};

/// Scopes an api_keys row (and `queen_proxy.issue_api_key`) accepts — mirrors
/// the CHECK constraint on `queen_proxy.api_keys.scopes` (001_init.sql).
const VALID_SCOPES: [&str; 4] = ["produce", "consume", "admin", "read"];

/// Roles a cluster_roles row (and `queen_proxy.grant_cluster_role`) accepts —
/// mirrors the CHECK constraint on `queen_proxy.cluster_roles.role`
/// (001_init.sql) and the same list inside grant_cluster_role (004_lifecycle).
const VALID_ROLES: [&str; 4] = ["admin", "producer", "consumer", "viewer"];

// ---------------------------------------------------------------------------
// router
// ---------------------------------------------------------------------------

/// API routes, meant to be `.nest("/api/console", console::router())` — see
/// the report for the exact main.rs diff. Paths below are relative to that
/// mount point.
pub fn router() -> Router<St> {
    Router::new()
        .route("/overview", get(overview))
        .route("/usage", get(usage))
        .route("/keys", get(list_keys).post(create_key))
        .route("/keys/:id", delete(delete_key))
        // Members are addressed by email, not by id: an email is what an admin
        // types, and it is the only handle `grant_cluster_role` /
        // `revoke_cluster_role` (004_lifecycle.sql) take. It is carried in the
        // JSON body rather than the path on DELETE too, so the address never
        // has to survive percent-encoding — same-origin axum, no intermediary
        // that could strip a DELETE body.
        .route("/members", get(list_members).post(grant_member).delete(revoke_member))
}

// ---------------------------------------------------------------------------
// auth (shared by every handler below)
// ---------------------------------------------------------------------------

/// Resolve the cluster + authenticate the human caller. Returns the ready
/// error `Response` (421/503/401/403) on any failure, otherwise the resolved
/// `ClusterCtx`, `user_id` and cluster `Role` — a role is only ever `Some`
/// via a real `cluster_roles` row (see module doc), so callers needing "any
/// member" need no extra check; admin-only endpoints call `require_admin`.
async fn console_ctx(st: &St, headers: &HeaderMap) -> Result<(Arc<ClusterCtx>, Uuid, Role), Response> {
    let host = headers.get(header::HOST).and_then(|v| v.to_str().ok()).unwrap_or("");
    let ctx = match st.cache.resolve_host(host).await {
        Some(c) => c,
        None => return Err(errors::err_421("unknown cluster")),
    };
    require_db(st)?;

    // `auth::authenticate` reads the session cookie itself now
    // (auth::read_credential), so the console no longer lifts it into a
    // synthetic Authorization header of its own — one implementation of
    // "which credential is this", shared with the data plane.
    let principal = auth::authenticate(st, headers, ctx.cluster_id).await?;
    match principal {
        Principal::ApiKey { .. } => {
            Err(errors::err_403(errors::CODE_FORBIDDEN, "api keys cannot access the console (human session required)"))
        }
        Principal::User { user_id, role, .. } => Ok((ctx, user_id, role)),
    }
}

fn require_db(st: &St) -> Result<(), Response> {
    if st.db.is_some() {
        Ok(())
    } else {
        Err(err_not_configured())
    }
}

fn require_admin(role: Role) -> Result<(), Response> {
    if matches!(role, Role::Admin) {
        Ok(())
    } else {
        Err(errors::err_403(errors::CODE_FORBIDDEN, "admin role required"))
    }
}

// ---------------------------------------------------------------------------
// GET /overview
// ---------------------------------------------------------------------------

/// The plan identity + monthly counter the cached `ClusterCtx` does not carry:
/// `EffectiveLimits` is the merged plan+overrides numbers only, with no plan
/// code and no monthly_msgs_quota (that quota is not a per-request limit, so
/// the data plane never needed it). `cluster_month_msgs` (004_lifecycle.sql)
/// reads usage_days plus the not-yet-rolled-up usage_minutes remainder, so
/// month-to-date never under-counts today.
const PLAN_USAGE_SQL: &str = "
    SELECT p.code,
           p.monthly_msgs_quota,
           queen_proxy.cluster_month_msgs(c.id, (now() AT TIME ZONE 'UTC')::date),
           to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM')
    FROM queen_proxy.clusters c
    JOIN queen_proxy.plans   p ON p.id = c.plan_id
    WHERE c.id = $1::text::uuid";

async fn overview(State(st): State<St>, headers: HeaderMap) -> Response {
    let (ctx, _user_id, role) = match console_ctx(&st, &headers).await {
        Ok(v) => v,
        Err(e) => return e,
    };

    let pool = st.db.as_ref().expect("console_ctx guarantees db is configured");
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "overview: pool.get failed");
            return errors::err_502("pxdb unavailable");
        }
    };
    let cluster_id_str = ctx.cluster_id.to_string();
    let row = match client.query_opt(PLAN_USAGE_SQL, &[&cluster_id_str]).await {
        Ok(Some(r)) => r,
        Ok(None) => {
            tracing::warn!(target: "console", cluster = %ctx.cluster_id, "overview: cluster row vanished");
            return errors::err_502("cluster not found");
        }
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "overview: plan/usage query failed");
            return errors::err_502("plan query failed");
        }
    };

    let push_block = st.limits.push_block_reason(ctx.cluster_id);
    json_ok(json!({
        "slug": ctx.slug,
        "status": status_str(ctx.status),
        // The caller's own role, so the SPA can hide admin-only tabs instead
        // of offering them and letting them 403.
        "role": role_str(role),
        "plan": {
            "code": row.get::<_, String>(0),
            "monthly_msgs_quota": row.get::<_, Option<i64>>(1),
        },
        "usage": {
            "month": row.get::<_, String>(3),
            "msgs": row.get::<_, i64>(2),
        },
        // The live quota flags themselves, not a second opinion computed here:
        // these are the same ones gateway.rs turns into a 403 on Produce, so a
        // tenant reading this page can never be told they are fine while their
        // pushes are being rejected. `storage` comes from registry.rs summing
        // the broker's per-queue retainedBytes (with hysteresis) via main.rs's
        // pump; `monthly_quota` from the rollup task.
        "push_block": push_block.map(push_block_str),
        "storage": {
            "max_retained_bytes": ctx.limits.max_retained_bytes,
            "over_quota": push_block == Some(PushBlock::Storage),
        },
        "limits": limits_json(&ctx.limits),
        "features": features_json(ctx.features),
    }))
}

fn status_str(s: ClusterStatus) -> &'static str {
    match s {
        ClusterStatus::Active => "active",
        ClusterStatus::PushBlocked => "push_blocked",
        ClusterStatus::Suspended => "suspended",
        ClusterStatus::Deleting => "deleting",
    }
}

/// Stable wire names for the live push-block reasons — deliberately the same
/// spellings gateway.rs's 403 codes are built from, so a tenant comparing a
/// rejected push against this page sees one vocabulary, not two.
fn push_block_str(b: PushBlock) -> &'static str {
    match b {
        PushBlock::Storage => "storage",
        PushBlock::MonthlyQuota => "monthly_quota",
    }
}

fn role_str(r: Role) -> &'static str {
    match r {
        Role::Admin => "admin",
        Role::Producer => "producer",
        Role::Consumer => "consumer",
        Role::Viewer => "viewer",
    }
}

/// `Option<i64>` serializes to `null` for `None` via `serde_json::json!` —
/// exactly the "None = null = unlimited" convention the task asks for, with
/// no extra mapping needed.
fn limits_json(l: &EffectiveLimits) -> Value {
    json!({
        "max_req_per_sec": l.max_req_per_sec,
        "req_burst": l.req_burst,
        "max_msgs_per_sec": l.max_msgs_per_sec,
        "msgs_burst": l.msgs_burst,
        "max_queues": l.max_queues,
        "max_partitions_per_queue": l.max_partitions_per_queue,
        "max_parked_pops": l.max_parked_pops,
        "max_payload_bytes": l.max_payload_bytes,
        "max_batch_items": l.max_batch_items,
        "max_retained_bytes": l.max_retained_bytes,
        "max_retention_seconds": l.max_retention_seconds,
    })
}

fn features_json(f: Features) -> Value {
    json!({
        "streams": f.streams,
        "traces": f.traces,
        // PLAN_KV_TIMERS.md §9.8 P1. Reported so the console shows the same
        // answer the data plane enforces — a tenant told "not in your plan" by
        // a 403 can see which flag said it.
        "kv": f.kv,
        "timers": f.timers,
        // EPHEMERAL_QUEUES.md §5.1, same reason: a tenant refused with
        // `feature_gated` can see which flag said it.
        "ephemeral": f.ephemeral,
    })
}

// ---------------------------------------------------------------------------
// GET /usage?hours=24
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct UsageQuery {
    hours: Option<i64>,
}

const USAGE_SQL: &str = "
    SELECT to_char(minute AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS minute,
           op_class, reqs, msgs, bytes_in, bytes_out
    FROM queen_proxy.usage_minutes
    WHERE cluster_id = $1::text::uuid
      AND minute >= now() - make_interval(hours => $2::int)
    ORDER BY minute ASC, op_class ASC";

async fn usage(State(st): State<St>, headers: HeaderMap, Query(q): Query<UsageQuery>) -> Response {
    let (ctx, _user_id, _role) = match console_ctx(&st, &headers).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let hours = clamp_hours(q.hours) as i32;

    // require_db already checked by console_ctx -- safe to unwrap the pool.
    let pool = st.db.as_ref().expect("console_ctx guarantees db is configured");
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "usage: pool.get failed");
            return errors::err_502("pxdb unavailable");
        }
    };
    let cluster_id_str = ctx.cluster_id.to_string();
    let rows = match client.query(USAGE_SQL, &[&cluster_id_str, &hours]).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "usage: query failed");
            return errors::err_502("usage query failed");
        }
    };

    let items: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "minute": r.get::<_, String>(0),
                "op": r.get::<_, String>(1),
                "reqs": r.get::<_, i64>(2),
                "msgs": r.get::<_, i64>(3),
                "bytes_in": r.get::<_, i64>(4),
                "bytes_out": r.get::<_, i64>(5),
            })
        })
        .collect();
    json_ok(json!(items))
}

/// Default 24, cap 168 (task spec). Also floors at 1 so a caller-supplied
/// zero/negative value can't turn into an unbounded (or backwards) window.
fn clamp_hours(hours: Option<i64>) -> i64 {
    hours.unwrap_or(24).clamp(1, 168)
}

// ---------------------------------------------------------------------------
// GET /keys, POST /keys, DELETE /keys/:id
// ---------------------------------------------------------------------------

const LIST_KEYS_SQL: &str = "
    SELECT id::text, name, scopes,
           to_char(created_at   AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
           to_char(last_used_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
           to_char(revoked_at   AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
    FROM queen_proxy.api_keys
    WHERE cluster_id = $1::text::uuid
    ORDER BY created_at DESC";

async fn list_keys(State(st): State<St>, headers: HeaderMap) -> Response {
    let (ctx, _user_id, role) = match console_ctx(&st, &headers).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    if let Err(e) = require_admin(role) {
        return e;
    }
    let pool = st.db.as_ref().expect("console_ctx guarantees db is configured");
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "list_keys: pool.get failed");
            return errors::err_502("pxdb unavailable");
        }
    };
    let cluster_id_str = ctx.cluster_id.to_string();
    let rows = match client.query(LIST_KEYS_SQL, &[&cluster_id_str]).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "list_keys: query failed");
            return errors::err_502("keys query failed");
        }
    };

    // MAI hash: key_hash is never selected above, let alone returned here.
    let items: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "id": r.get::<_, String>(0),
                "name": r.get::<_, String>(1),
                "scopes": r.get::<_, Vec<String>>(2),
                "created_at": r.get::<_, String>(3),
                "last_used_at": r.get::<_, Option<String>>(4),
                "revoked_at": r.get::<_, Option<String>>(5),
            })
        })
        .collect();
    json_ok(json!(items))
}

#[derive(Deserialize)]
struct CreateKeyReq {
    name: String,
    scopes: Vec<String>,
}

const ISSUE_KEY_SQL: &str = "SELECT queen_proxy.issue_api_key($1::text::uuid, $2, $3, $4)::text AS id";

async fn create_key(State(st): State<St>, headers: HeaderMap, Json(body): Json<CreateKeyReq>) -> Response {
    let (ctx, user_id, role) = match console_ctx(&st, &headers).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    if let Err(e) = require_admin(role) {
        return e;
    }
    let name = match validate_name(&body.name) {
        Ok(n) => n,
        Err(msg) => return err_400("invalid_request", msg),
    };
    if let Err(msg) = validate_scopes(&body.scopes) {
        return err_400("invalid_request", msg);
    }

    let plaintext = auth::generate_api_key("live");
    let hash = auth::key_hash_hex(&plaintext);

    let pool = st.db.as_ref().expect("console_ctx guarantees db is configured");
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "create_key: pool.get failed");
            return errors::err_502("pxdb unavailable");
        }
    };
    let cluster_id_str = ctx.cluster_id.to_string();
    let row = match client.query_one(ISSUE_KEY_SQL, &[&cluster_id_str, &name, &hash, &body.scopes]).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "create_key: issue_api_key failed");
            return errors::err_502("key creation failed");
        }
    };
    let id: String = row.get(0);

    // issue_api_key() already appends its own operations row (actor =
    // 'control_plane', 002_functions.sql) -- this is the SECOND, deliberately
    // user-attributed row the task brief asks for ("scrivi operations via
    // record_operation (actor user)"). record_operation's own header comment
    // earmarks exactly this caller shape ("a future self-serve admin API
    // acting as 'user'"). Best-effort: the key already exists either way.
    record_user_op(
        &st,
        ctx.tenant_id,
        ctx.cluster_id,
        user_id,
        "console_api_key_issued",
        Some(&id),
        json!({ "name": name, "scopes": body.scopes }),
    )
    .await;

    json_ok(json!({ "id": id, "key": plaintext }))
}

async fn delete_key(State(st): State<St>, headers: HeaderMap, Path(key_id): Path<String>) -> Response {
    let (ctx, _user_id, role) = match console_ctx(&st, &headers).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    if let Err(e) = require_admin(role) {
        return e;
    }
    if Uuid::parse_str(&key_id).is_err() {
        return err_400("invalid_request", "malformed key id");
    }

    let pool = st.db.as_ref().expect("console_ctx guarantees db is configured");
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "delete_key: pool.get failed");
            return errors::err_502("pxdb unavailable");
        }
    };

    // `queen_proxy.revoke_api_key(uuid)` (002_functions.sql) looks the key up
    // GLOBALLY by id -- it takes no cluster_id and never checks one. Enforce
    // cluster ownership HERE so a console admin on cluster A can never revoke
    // cluster B's key even by guessing/observing its uuid; see report.
    let cluster_id_str = ctx.cluster_id.to_string();
    let owned = match client
        .query_opt(
            "SELECT 1 FROM queen_proxy.api_keys \
             WHERE id = $1::text::uuid AND cluster_id = $2::text::uuid AND revoked_at IS NULL",
            &[&key_id, &cluster_id_str],
        )
        .await
    {
        Ok(r) => r.is_some(),
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "delete_key: ownership check failed");
            return errors::err_502("key lookup failed");
        }
    };
    if !owned {
        return errors::err_404("not_found", "no such active api key on this cluster");
    }

    if let Err(e) = client.execute("SELECT queen_proxy.revoke_api_key($1::text::uuid)", &[&key_id]).await {
        tracing::warn!(target: "console", err = %e, "delete_key: revoke_api_key failed");
        return errors::err_502("key revocation failed");
    }
    json_ok(json!({ "ok": true }))
}

fn validate_name(raw: &str) -> Result<String, &'static str> {
    let name = raw.trim();
    if name.is_empty() {
        return Err("name must not be empty");
    }
    Ok(name.to_string())
}

fn validate_scopes(scopes: &[String]) -> Result<(), &'static str> {
    if scopes.is_empty() {
        return Err("at least one scope is required");
    }
    if scopes.iter().any(|s| !VALID_SCOPES.contains(&s.as_str())) {
        return Err("scopes must be a subset of produce, consume, admin, read");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// GET /members, POST /members, DELETE /members
//
// `queen_proxy.cluster_roles` is what every gate above reads, and until now
// nothing wrote it at runtime: a second human on a cluster needed a
// hand-written INSERT. Writes go through `queen_proxy.grant_cluster_role` /
// `revoke_cluster_role` (004_lifecycle.sql), never through direct DML, so the
// same-tenant check, the audit row and the invalidation notify all stay in the
// one place that implements them.
// ---------------------------------------------------------------------------

/// Password hashes are on `queen_proxy.users` — never selected here, never
/// returned. Same discipline as `LIST_KEYS_SQL` and `key_hash`.
const LIST_MEMBERS_SQL: &str = "
    SELECT u.id::text, u.email, cr.role,
           to_char(cr.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
    FROM queen_proxy.cluster_roles cr
    JOIN queen_proxy.users u ON u.id = cr.user_id
    WHERE cr.cluster_id = $1::text::uuid
    ORDER BY u.email ASC";

async fn list_members(State(st): State<St>, headers: HeaderMap) -> Response {
    let (ctx, _user_id, role) = match console_ctx(&st, &headers).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    if let Err(e) = require_admin(role) {
        return e;
    }
    let pool = st.db.as_ref().expect("console_ctx guarantees db is configured");
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "list_members: pool.get failed");
            return errors::err_502("pxdb unavailable");
        }
    };
    let cluster_id_str = ctx.cluster_id.to_string();
    let rows = match client.query(LIST_MEMBERS_SQL, &[&cluster_id_str]).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "list_members: query failed");
            return errors::err_502("members query failed");
        }
    };

    let items: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "user_id": r.get::<_, String>(0),
                "email": r.get::<_, String>(1),
                "role": r.get::<_, String>(2),
                "granted_at": r.get::<_, String>(3),
            })
        })
        .collect();
    json_ok(json!(items))
}

#[derive(Deserialize)]
struct GrantMemberReq {
    email: String,
    role: String,
}

#[derive(Deserialize)]
struct RevokeMemberReq {
    email: String,
}

/// Target user, resolved inside THIS cluster's tenant. `grant_cluster_role`
/// enforces the same-tenant rule itself and would raise, but resolving here
/// keeps a mistyped address a 404 instead of a 502, and — the reason this is
/// not just ergonomics — makes the answer identical whether the address is
/// unknown or belongs to another tenant, so the console can't be used to probe
/// for other tenants' users.
const MEMBER_CANDIDATE_SQL: &str = "
    SELECT id::text FROM queen_proxy.users WHERE email = $1 AND tenant_id = $2::text::uuid";

/// Current role of one user on this cluster (NULL if none) + how many admins
/// the cluster has, for the last-admin guard.
const MEMBER_STANDING_SQL: &str = "
    SELECT (SELECT role FROM queen_proxy.cluster_roles
             WHERE cluster_id = $1::text::uuid AND user_id = $2::text::uuid),
           (SELECT count(*) FROM queen_proxy.cluster_roles
             WHERE cluster_id = $1::text::uuid AND role = 'admin')";

async fn grant_member(State(st): State<St>, headers: HeaderMap, Json(body): Json<GrantMemberReq>) -> Response {
    let (ctx, user_id, role) = match console_ctx(&st, &headers).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    if let Err(e) = require_admin(role) {
        return e;
    }
    let email = match validate_email(&body.email) {
        Ok(e) => e,
        Err(msg) => return err_400("invalid_request", msg),
    };
    let new_role = match validate_role(&body.role) {
        Ok(r) => r,
        Err(msg) => return err_400("invalid_request", msg),
    };

    let pool = st.db.as_ref().expect("console_ctx guarantees db is configured");
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "grant_member: pool.get failed");
            return errors::err_502("pxdb unavailable");
        }
    };

    let tenant_id_str = ctx.tenant_id.to_string();
    let target_id = match client.query_opt(MEMBER_CANDIDATE_SQL, &[&email, &tenant_id_str]).await {
        Ok(Some(r)) => r.get::<_, String>(0),
        Ok(None) => return errors::err_404("not_found", "no such user in this cluster's tenant"),
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "grant_member: user lookup failed");
            return errors::err_502("user lookup failed");
        }
    };

    let cluster_id_str = ctx.cluster_id.to_string();
    let standing = match client.query_one(MEMBER_STANDING_SQL, &[&cluster_id_str, &target_id]).await {
        Ok(r) => (r.get::<_, Option<String>>(0), r.get::<_, i64>(1)),
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "grant_member: standing query failed");
            return errors::err_502("member lookup failed");
        }
    };
    if would_orphan_admins(standing.0.as_deref(), standing.1, Some(&new_role)) {
        return err_400("invalid_request", "cannot demote the last admin of this cluster");
    }

    // p_cluster is ALWAYS this session's cluster: the same discipline
    // delete_key spells out. grant_cluster_role takes the cluster as a
    // parameter and does not know who is calling, so a console admin on
    // cluster A must never be able to name cluster B — the id is never read
    // from the request.
    if let Err(e) = client
        .execute(
            "SELECT queen_proxy.grant_cluster_role($1::text::uuid, $2, $3)",
            &[&cluster_id_str, &email, &new_role],
        )
        .await
    {
        tracing::warn!(target: "console", err = %e, "grant_member: grant_cluster_role failed");
        return errors::err_502("role grant failed");
    }

    // grant_cluster_role appends its own operations row with actor
    // 'control_plane' and actor_id = the GRANTEE; this second row is the
    // user-attributed one that says who did it (same pairing as create_key).
    record_user_op(
        &st,
        ctx.tenant_id,
        ctx.cluster_id,
        user_id,
        "console_member_granted",
        Some(&target_id),
        json!({ "email": email, "role": new_role }),
    )
    .await;

    json_ok(json!({ "ok": true, "email": email, "role": new_role }))
}

/// Member standing on THIS cluster, by email — the ownership check that keeps
/// a revoke inside the caller's own cluster (`cluster_id` is bound, not sent).
const MEMBER_ON_CLUSTER_SQL: &str = "
    SELECT u.id::text, cr.role,
           (SELECT count(*) FROM queen_proxy.cluster_roles
             WHERE cluster_id = $1::text::uuid AND role = 'admin')
    FROM queen_proxy.cluster_roles cr
    JOIN queen_proxy.users u ON u.id = cr.user_id
    WHERE cr.cluster_id = $1::text::uuid AND u.email = $2";

async fn revoke_member(State(st): State<St>, headers: HeaderMap, Json(body): Json<RevokeMemberReq>) -> Response {
    let (ctx, user_id, role) = match console_ctx(&st, &headers).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    if let Err(e) = require_admin(role) {
        return e;
    }
    let email = match validate_email(&body.email) {
        Ok(e) => e,
        Err(msg) => return err_400("invalid_request", msg),
    };

    let pool = st.db.as_ref().expect("console_ctx guarantees db is configured");
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "console", err = %e, "revoke_member: pool.get failed");
            return errors::err_502("pxdb unavailable");
        }
    };

    let cluster_id_str = ctx.cluster_id.to_string();
    let (target_id, current_role, admin_count) =
        match client.query_opt(MEMBER_ON_CLUSTER_SQL, &[&cluster_id_str, &email]).await {
            Ok(Some(r)) => (r.get::<_, String>(0), r.get::<_, String>(1), r.get::<_, i64>(2)),
            Ok(None) => return errors::err_404("not_found", "no such member on this cluster"),
            Err(e) => {
                tracing::warn!(target: "console", err = %e, "revoke_member: member lookup failed");
                return errors::err_502("member lookup failed");
            }
        };
    if would_orphan_admins(Some(&current_role), admin_count, None) {
        return err_400("invalid_request", "cannot remove the last admin of this cluster");
    }

    // Bound to this session's cluster for the same reason as the grant above.
    if let Err(e) = client
        .execute("SELECT queen_proxy.revoke_cluster_role($1::text::uuid, $2)", &[&cluster_id_str, &email])
        .await
    {
        tracing::warn!(target: "console", err = %e, "revoke_member: revoke_cluster_role failed");
        return errors::err_502("role revocation failed");
    }

    record_user_op(
        &st,
        ctx.tenant_id,
        ctx.cluster_id,
        user_id,
        "console_member_revoked",
        Some(&target_id),
        json!({ "email": email, "role": current_role }),
    )
    .await;

    json_ok(json!({ "ok": true }))
}

/// Normalized exactly the way `grant_cluster_role`/`revoke_cluster_role`
/// normalize it (`lower(btrim(...))`) and checked against the same rule they
/// apply (`position('@' IN ...) = 0`), so the row this looks up is the row
/// they will write.
fn validate_email(raw: &str) -> Result<String, &'static str> {
    let email = raw.trim().to_lowercase();
    if email.is_empty() || !email.contains('@') {
        return Err("email must be a valid address");
    }
    Ok(email)
}

fn validate_role(raw: &str) -> Result<String, &'static str> {
    let role = raw.trim().to_lowercase();
    if !VALID_ROLES.contains(&role.as_str()) {
        return Err("role must be one of admin, producer, consumer, viewer");
    }
    Ok(role)
}

/// Whether applying `new_role` (None = revoke) to a member currently holding
/// `current_role` would leave the cluster with no admin at all. A cluster with
/// no admin can never be administered again through the console — no endpoint
/// here can create the first admin back, that takes control-plane SQL — so
/// this is refused rather than merely warned about. It covers self-demotion
/// and self-revocation too: the caller is an admin, so a sole admin removing
/// themselves is exactly `admin_count == 1`.
fn would_orphan_admins(current_role: Option<&str>, admin_count: i64, new_role: Option<&str>) -> bool {
    if current_role != Some("admin") {
        return false; // not touching an admin seat
    }
    if new_role == Some("admin") {
        return false; // still an admin afterwards
    }
    admin_count <= 1
}

/// Append a `queen_proxy.operations` row attributed to the console user (see
/// `create_key`'s call site for why this exists alongside issue_api_key's own
/// internal audit row). Mirrors oauth.rs's private `record_op` bind pattern
/// (`::text::uuid` / `::text::jsonb` casts — no uuid/jsonb tokio-postgres
/// feature enabled in this crate). Best-effort: never fails the request.
async fn record_user_op(
    st: &St,
    tenant_id: Uuid,
    cluster_id: Uuid,
    actor_id: Uuid,
    action: &str,
    target: Option<&str>,
    meta: Value,
) {
    let Some(pool) = st.db.as_ref() else { return };
    let Ok(client) = pool.get().await else { return };
    let meta_s = meta.to_string();
    if let Err(e) = client
        .execute(
            "SELECT queen_proxy.record_operation($1::text::uuid, $2::text::uuid, 'user', $3::text::uuid, $4, $5, $6::text::jsonb)",
            &[&tenant_id.to_string(), &cluster_id.to_string(), &actor_id.to_string(), &action, &target, &meta_s],
        )
        .await
    {
        tracing::warn!(target: "console", action, err = %e, "record_operation failed (non-fatal)");
    }
}

// ---------------------------------------------------------------------------
// small response helpers
// ---------------------------------------------------------------------------

fn json_ok(v: Value) -> Response {
    let mut resp = (StatusCode::OK, v.to_string()).into_response();
    resp.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp
}

fn err_400(code: &str, msg: &str) -> Response {
    errors::json_error(StatusCode::BAD_REQUEST, code, msg)
}

/// Literal shape per the task brief: `{"code":"not_configured"}` — no `error`
/// key, unlike `errors::json_error`'s envelope.
fn err_not_configured() -> Response {
    let mut resp = (StatusCode::SERVICE_UNAVAILABLE, "{\"code\":\"not_configured\"}").into_response();
    resp.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp
}

// ---------------------------------------------------------------------------
// SPA embed (GET /console, GET /console/*)
// ---------------------------------------------------------------------------

/// Vite build output, `base: "/console/"` (console/vite.config.js). Path is
/// relative to this crate's manifest dir (proxy/), matching
/// server/src/handlers/static_files.rs's `#[folder = "webapp/dist"]`.
#[derive(RustEmbed)]
#[folder = "console/dist"]
struct SpaAssets;

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "application/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" | "map" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "ttf" => "font/ttf",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn spa_serve(path: &str) -> Option<Response> {
    SpaAssets::get(path).map(|f| ([(header::CONTENT_TYPE, content_type(path))], f.data.into_owned()).into_response())
}

/// `/console` (or any sub-path with no matching built asset) relative to
/// `console/dist`'s root — `""` maps to `index.html`, matching Vite's own
/// `base: "/console/"` output layout. Pure (no I/O), unit-tested below.
fn spa_rel_path(path: &str) -> &str {
    let rel = path.strip_prefix("/console").unwrap_or(path);
    let rel = rel.trim_start_matches('/');
    if rel.is_empty() {
        "index.html"
    } else {
        rel
    }
}

/// SPA handler for `GET /console` and `GET /console/*` (mounted by main.rs —
/// see report). Serves the embedded, Vite-built `console/dist`, falling back
/// to `index.html` for any path with no matching built asset (client-side tab
/// state on a hard refresh / deep link). 404 only when the SPA was never
/// built at all (no `index.html` in the embed).
pub async fn spa(uri: Uri) -> Response {
    let rel = spa_rel_path(uri.path());
    if let Some(resp) = spa_serve(rel) {
        return resp;
    }
    match spa_serve("index.html") {
        Some(resp) => resp,
        None => (StatusCode::NOT_FOUND, "console not built (console/dist is empty — see console/README.md)")
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- pure JSON/status mapping -------------------------------------------

    #[test]
    fn status_str_all_variants() {
        assert_eq!(status_str(ClusterStatus::Active), "active");
        assert_eq!(status_str(ClusterStatus::PushBlocked), "push_blocked");
        assert_eq!(status_str(ClusterStatus::Suspended), "suspended");
        assert_eq!(status_str(ClusterStatus::Deleting), "deleting");
    }

    #[test]
    fn limits_json_none_is_null() {
        let l = EffectiveLimits::default();
        let v = limits_json(&l);
        assert!(v["max_req_per_sec"].is_null());
        assert!(v["max_retained_bytes"].is_null());
    }

    #[test]
    fn limits_json_some_is_number() {
        let l = EffectiveLimits { max_queues: Some(20), max_retained_bytes: Some(1024), ..Default::default() };
        let v = limits_json(&l);
        assert_eq!(v["max_queues"], 20);
        assert_eq!(v["max_retained_bytes"], 1024);
        // untouched fields stay null
        assert!(v["max_req_per_sec"].is_null());
    }

    #[test]
    fn features_json_roundtrip() {
        let v = features_json(Features {
            streams: true,
            traces: false,
            kv: true,
            timers: false,
            ephemeral: true,
        });
        assert_eq!(v["streams"], true);
        assert_eq!(v["traces"], false);
        assert_eq!(v["kv"], true);
        assert_eq!(v["timers"], false);
        assert_eq!(v["ephemeral"], true);
    }

    // --- role gate ------------------------------------------------------------

    #[test]
    fn require_admin_gate() {
        assert!(require_admin(Role::Admin).is_ok());
        assert!(require_admin(Role::Producer).is_err());
        assert!(require_admin(Role::Consumer).is_err());
        assert!(require_admin(Role::Viewer).is_err());
    }

    // --- hours clamp ------------------------------------------------------------

    #[test]
    fn clamp_hours_defaults_and_bounds() {
        assert_eq!(clamp_hours(None), 24);
        assert_eq!(clamp_hours(Some(1)), 1);
        assert_eq!(clamp_hours(Some(168)), 168);
        assert_eq!(clamp_hours(Some(500)), 168);
        assert_eq!(clamp_hours(Some(0)), 1);
        assert_eq!(clamp_hours(Some(-10)), 1);
    }

    // --- key create validation -------------------------------------------------

    #[test]
    fn validate_name_trims_and_rejects_empty() {
        assert_eq!(validate_name("  my key  ").unwrap(), "my key");
        assert!(validate_name("").is_err());
        assert!(validate_name("   ").is_err());
    }

    #[test]
    fn validate_scopes_rules() {
        assert!(validate_scopes(&["produce".to_string(), "read".to_string()]).is_ok());
        assert!(validate_scopes(&[]).is_err());
        assert!(validate_scopes(&["produce".to_string(), "sudo".to_string()]).is_err());
    }

    // --- member management -----------------------------------------------------

    #[test]
    fn role_str_all_variants() {
        assert_eq!(role_str(Role::Admin), "admin");
        assert_eq!(role_str(Role::Producer), "producer");
        assert_eq!(role_str(Role::Consumer), "consumer");
        assert_eq!(role_str(Role::Viewer), "viewer");
        // Every variant must be one grant_cluster_role would accept back.
        for r in [Role::Admin, Role::Producer, Role::Consumer, Role::Viewer] {
            assert!(VALID_ROLES.contains(&role_str(r)));
        }
    }

    #[test]
    fn push_block_str_all_variants() {
        assert_eq!(push_block_str(PushBlock::Storage), "storage");
        assert_eq!(push_block_str(PushBlock::MonthlyQuota), "monthly_quota");
    }

    #[test]
    fn validate_email_normalizes_like_the_sql_does() {
        assert_eq!(validate_email("  Dev@Localhost \n").unwrap(), "dev@localhost");
        assert!(validate_email("").is_err());
        assert!(validate_email("   ").is_err());
        assert!(validate_email("not-an-address").is_err());
    }

    #[test]
    fn validate_role_rules() {
        assert_eq!(validate_role(" Admin ").unwrap(), "admin");
        assert_eq!(validate_role("viewer").unwrap(), "viewer");
        assert!(validate_role("owner").is_err());
        assert!(validate_role("").is_err());
    }

    #[test]
    fn last_admin_cannot_be_demoted_or_removed() {
        assert!(would_orphan_admins(Some("admin"), 1, None), "sole admin revoked");
        assert!(would_orphan_admins(Some("admin"), 1, Some("viewer")), "sole admin demoted");
    }

    #[test]
    fn other_admin_changes_are_allowed() {
        assert!(!would_orphan_admins(Some("admin"), 2, None), "one of two admins revoked");
        assert!(!would_orphan_admins(Some("admin"), 1, Some("admin")), "sole admin re-granted admin");
        assert!(!would_orphan_admins(Some("viewer"), 1, None), "non-admin revoked");
        assert!(!would_orphan_admins(None, 1, Some("admin")), "brand new member");
        assert!(!would_orphan_admins(None, 0, Some("viewer")), "brand new member, no admins yet");
    }

    // (the cookie-to-bearer lift moved to auth::read_credential, with its
    // precedence tests, when the data plane started accepting cookies too)

    // --- SPA path mapping --------------------------------------------------------

    #[test]
    fn spa_rel_path_variants() {
        assert_eq!(spa_rel_path("/console"), "index.html");
        assert_eq!(spa_rel_path("/console/"), "index.html");
        assert_eq!(spa_rel_path("/console/assets/index-abc123.js"), "assets/index-abc123.js");
        assert_eq!(spa_rel_path("/console/keys"), "keys"); // client-side route; spa() falls back to index.html
    }

    #[test]
    fn content_type_table() {
        assert_eq!(content_type("index.html"), "text/html; charset=utf-8");
        assert_eq!(content_type("assets/app.js"), "application/javascript; charset=utf-8");
        assert_eq!(content_type("assets/app.css"), "text/css; charset=utf-8");
        assert_eq!(content_type("weird.bin"), "application/octet-stream");
    }
}
