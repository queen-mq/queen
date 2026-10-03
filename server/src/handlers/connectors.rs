//! `/api/v1/connectors` — the Postgres connectors' configuration
//! (PLAN_PG_CONNECTORS.md §3.1). Feature `pg`.
//!
//! | Route | Access | Does |
//! |---|---|---|
//! | `GET /api/v1/connectors` | read | the calling tenant's connectors, redacted, each with this node's `status` and a source's `state` |
//! | `GET /api/v1/connectors/:name` | read | one |
//! | `PUT /api/v1/connectors/:name` | admin | create or replace: validated, the password sealed; 200 with the redacted document |
//! | `DELETE /api/v1/connectors/:name[?dropSlot=true\|false]` | admin | a sink: gone. A source: its owner drops the slot and the managed publication first (202), or, with `dropSlot=false`, gone now with the slot left on the database (200) |
//! | `POST /api/v1/connectors/:name/resync` | admin | a source: forget the pointer, recreate the slot, snapshot again under a new epoch (202) |
//!
//! ## Where the documents live, and why there
//! In the broker-internal tenant [`SYSTEM_TENANT`], KV namespace `queen-pg`,
//! key `conn:<tenant>:<name>`, forever. Every node's manager
//! (`pg_inproc.rs`) reads them all every `QUEEN_PG_RELOAD_MS`, so a document
//! written here reaches every node through the replicated KV, and no client
//! can reach it other than through these routes: the proxy maps a client to
//! its own tenant, and with tenancy off every request is the default tenant.
//! A request that names the internal tenant itself is refused here.
//!
//! A source's runtime state — its pointer, the exactly-once marker, and its
//! lease — lives in the connector's OWN tenant (`src:<name>:pointer`,
//! `src:<name>:lease`), because the pointer commits in the same transaction
//! as the pushes and a transaction is single-tenant. The reads show it under
//! `state`.
//!
//! ## The password is write-only
//! `connection.password` (or one inside `connection.url`) is sealed with this
//! cell's `QUEEN_ENCRYPTION_KEY` — the queue-payload envelope, AES-256-GCM —
//! into `connection.passwordSealed`, so the replicated KV, the raft log and
//! every snapshot hold it sealed and only a node with the same key opens it.
//! Without a key the `PUT` is refused (`encryption_required`): a password is
//! never stored in clear. A `PUT` without a password keeps the stored one, so
//! a client can change other fields without sending it again; an empty one
//! removes it (PostgreSQL refuses empty passwords anyway). No read returns the
//! password or its sealed form.
//!
//! ## Writes do not race
//! Every write is conditional on the version this request read (`expect` +
//! `required`): two `PUT`s racing cannot silently drop a resync request or
//! the sealed password the other kept. The loser answers 409 `conflict`, and
//! reading again is the fix. The broker-written fields — `updatedAt`,
//! `resyncRequestedAt`, `deleting`, `passwordSealed` — are the broker's: what
//! a body says about them is replaced, so a document read and sent back as is
//! stays what it was.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use queen_pg::config::{valid_connector_name, ConnectorDoc, DeleteRequest, Kind};
use queen_pg::queen::KV_NAMESPACE;
use serde_json::{Map, Value};

use super::{json, AppState};
use crate::config::SYSTEM_TENANT;
use crate::encryption::Encryption;
use crate::pg_inproc::{doc_key, now_iso, parse_doc_key, DOC_PREFIX};
use crate::rsm::facade::{Deadline, KvFailure, KvReq, ReqCtx};
use crate::tenant::Tenant;

/// The largest `PUT` body: a document with 256 tables and a certificate
/// bundle fits; the KV value ceiling (`QUEEN_KV_MAX_VALUE_BYTES`, 64 KiB by
/// default) is checked on the stored form, which is what it bounds.
pub(crate) const MAX_BODY_BYTES: usize = 256 * 1024;

/// Keys per `getMany` of the sources' runtime state: two per source, well
/// inside the call's read budget.
const STATE_KEYS_PER_CALL: usize = 500;

/// The longest a `PUT` waits for its host's name to resolve for the egress
/// check. A lookup that takes longer is not the check's verdict: every
/// connect checks again.
const EGRESS_LOOKUP_MAX: Duration = Duration::from_secs(5);

fn pointer_key(name: &str) -> String {
    format!("src:{name}:pointer")
}

fn lease_key(name: &str) -> String {
    format!("src:{name}:lease")
}

/// This cell's key, read once: `None` when the broker has none, and then no
/// password is ever accepted.
fn process_key() -> Option<&'static Encryption> {
    static KEY: OnceLock<Arc<Encryption>> = OnceLock::new();
    let key = KEY.get_or_init(Encryption::from_env);
    key.is_enabled().then_some(key.as_ref())
}

// ---------------------------------------------------------------------------
// Answers.
// ---------------------------------------------------------------------------

/// `{"error": <code>, "detail": <the human half>}`: clients branch on `error`.
fn refuse(status: StatusCode, code: &str, detail: impl Into<String>) -> Response {
    json(
        status,
        serde_json::json!({"error": code, "detail": detail.into()}).to_string(),
    )
}

fn not_found(name: &str) -> Response {
    refuse(
        StatusCode::NOT_FOUND,
        "connector_not_found",
        format!("no connector named `{name}`"),
    )
}

/// The internal tenant holds the documents of every tenant: it has no
/// connectors of its own, and a request scoped to it is not a client's.
fn reserved(tenant: &str) -> Option<Response> {
    (tenant == SYSTEM_TENANT).then(|| {
        refuse(
            StatusCode::FORBIDDEN,
            "forbidden",
            "the broker-internal tenant has no connectors",
        )
    })
}

/// A KV refusal of this file's own calls. A lost precondition is a write
/// that raced another one; everything else is the KV route's own answer.
fn kv_failure(f: KvFailure) -> Response {
    match f {
        KvFailure::Precondition { .. } => refuse(
            StatusCode::CONFLICT,
            "conflict",
            "the connector changed while this request was being applied: read it again and retry",
        ),
        KvFailure::Invalid {
            status: 413,
            reason,
            detail,
        } => refuse(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            format!("{reason}: {detail}"),
        ),
        KvFailure::Invalid { reason, detail, .. } => refuse(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("{reason}: {detail}"),
        ),
        KvFailure::Rsm(e) => crate::handlers::raft::err_response(e),
    }
}

// ---------------------------------------------------------------------------
// The KV underneath.
// ---------------------------------------------------------------------------

/// One KV call as `tenant`, answered by the state machine (no tenant rate
/// ladder: these are the broker's own documents).
async fn kv(st: &AppState, tenant: &str, ops: Vec<Value>) -> Result<Vec<Value>, Response> {
    let count = ops.len();
    let ctx = ReqCtx::new(tenant, Deadline::after(st.stmt_timeout));
    let results = st
        .rsm
        .kv(ctx, KvReq { ops })
        .await
        .map(|out| out.results)
        .map_err(kv_failure)?;
    if results.len() != count {
        return Err(refuse(
            StatusCode::INTERNAL_SERVER_ERROR,
            "kv_error",
            "the KV answer is not aligned with its operations",
        ));
    }
    Ok(results)
}

/// A stored document and the version it was read at.
struct Stored {
    value: Value,
    version: i64,
}

async fn read_stored(st: &AppState, tenant: &str, name: &str) -> Result<Option<Stored>, Response> {
    let results = kv(
        st,
        SYSTEM_TENANT,
        vec![serde_json::json!({"op": "get", "ns": KV_NAMESPACE, "key": doc_key(tenant, name)})],
    )
    .await?;
    let found = &results[0];
    if found["found"] != Value::Bool(true) {
        return Ok(None);
    }
    Ok(Some(Stored {
        value: found["value"].clone(),
        version: found["version"].as_i64().unwrap_or(0),
    }))
}

/// Write `value` as connector `name`'s document, if the stored one is still
/// at `expect` (0: there is none).
async fn write_doc(
    st: &AppState,
    tenant: &str,
    name: &str,
    value: Value,
    expect: i64,
) -> Result<(), Response> {
    kv(
        st,
        SYSTEM_TENANT,
        vec![serde_json::json!({
            "op": "put", "ns": KV_NAMESPACE, "key": doc_key(tenant, name),
            "value": value, "forever": true, "expect": expect, "required": true,
        })],
    )
    .await
    .map(|_| ())
}

/// Remove connector `name`'s document, if the stored one is still at
/// `expect`.
async fn delete_doc(st: &AppState, tenant: &str, name: &str, expect: i64) -> Result<(), Response> {
    kv(
        st,
        SYSTEM_TENANT,
        vec![serde_json::json!({
            "op": "delete", "ns": KV_NAMESPACE, "key": doc_key(tenant, name),
            "expect": expect, "required": true,
        })],
    )
    .await
    .map(|_| ())
}

/// The runtime state of the sources `names` of `tenant`, by name:
/// `{"pointer": …, "lease": …}`, `null` for what is not there (a lease is
/// gone once its TTL has passed).
async fn source_states(
    st: &AppState,
    tenant: &str,
    names: &[String],
) -> Result<HashMap<String, Value>, Response> {
    let keys: Vec<String> = names
        .iter()
        .flat_map(|n| [pointer_key(n), lease_key(n)])
        .collect();
    let mut rows: HashMap<String, Value> = HashMap::new();
    for chunk in keys.chunks(STATE_KEYS_PER_CALL) {
        let results = kv(
            st,
            tenant,
            vec![serde_json::json!({"op": "getMany", "ns": KV_NAMESPACE, "keys": chunk})],
        )
        .await?;
        for row in results[0]["rows"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or(&[])
        {
            if let Some(key) = row["key"].as_str() {
                rows.insert(key.to_string(), row["value"].clone());
            }
        }
    }
    Ok(names
        .iter()
        .map(|n| {
            let state = serde_json::json!({
                "pointer": rows.get(&pointer_key(n)).cloned().unwrap_or(Value::Null),
                "lease": rows.get(&lease_key(n)).cloned().unwrap_or(Value::Null),
            });
            (n.clone(), state)
        })
        .collect())
}

fn is_source(value: &Value) -> bool {
    value["kind"] == Kind::Source.as_str()
}

/// A stored document as the reads show it: never the password, never its
/// sealed form. One this broker cannot parse (written by a newer one) is
/// shown with its password fields removed by hand.
fn redacted(value: &Value) -> Value {
    match ConnectorDoc::from_json(value) {
        Ok(doc) => doc.redacted(),
        Err(_) => {
            let mut v = value.clone();
            if let Some(conn) = v.get_mut("connection").and_then(Value::as_object_mut) {
                let sealed = conn.remove("passwordSealed").is_some();
                let clear = conn.remove("password").is_some();
                conn.remove("url");
                conn.remove("passwordSet");
                conn.insert("passwordSet".into(), Value::Bool(sealed || clear));
            }
            v
        }
    }
}

/// One connector of a read: its name, the redacted document, this node's
/// status of it, and a source's runtime state (`null` for a sink).
fn entry(tenant: &str, name: &str, value: &Value, state: Value) -> Value {
    let mut out = match redacted(value) {
        Value::Object(m) => m,
        _ => Map::new(),
    };
    out.insert("name".into(), Value::String(name.to_string()));
    out.insert(
        "status".into(),
        crate::pg_inproc::status_of(tenant, name)
            .unwrap_or_else(|| serde_json::json!({"phase": "unknown"})),
    );
    out.insert("state".into(), state);
    Value::Object(out)
}

// ---------------------------------------------------------------------------
// The routes.
// ---------------------------------------------------------------------------

/// `GET /api/v1/connectors`.
pub async fn handle_connectors_list(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<Tenant>,
) -> Response {
    list(&st, tenant.as_str()).await
}

/// `GET /api/v1/connectors/:name`.
pub async fn handle_connector_get(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<Tenant>,
    Path(name): Path<String>,
) -> Response {
    get_one(&st, tenant.as_str(), &name).await
}

/// `PUT /api/v1/connectors/:name`.
pub async fn handle_connector_put(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<Tenant>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    put(&st, process_key(), tenant.as_str(), &name, &body).await
}

/// `DELETE /api/v1/connectors/:name[?dropSlot=true|false]`.
pub async fn handle_connector_delete(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<Tenant>,
    Path(name): Path<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    delete(
        &st,
        tenant.as_str(),
        &name,
        q.get("dropSlot").map(String::as_str),
    )
    .await
}

/// `POST /api/v1/connectors/:name/resync`.
pub async fn handle_connector_resync(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<Tenant>,
    Path(name): Path<String>,
) -> Response {
    resync(&st, tenant.as_str(), &name).await
}

pub(crate) async fn list(st: &AppState, tenant: &str) -> Response {
    if let Some(r) = reserved(tenant) {
        return r;
    }
    let prefix = format!("{DOC_PREFIX}{tenant}:");
    let mut docs: Vec<(String, Value)> = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let mut op = serde_json::json!({
            "op": "getPrefix", "ns": KV_NAMESPACE, "prefix": prefix,
            "limit": queen_pg::queen::MAX_KV_PREFIX_LIMIT,
        });
        if let Some(a) = &after {
            op["after"] = Value::String(a.clone());
        }
        let page = match kv(st, SYSTEM_TENANT, vec![op]).await {
            Ok(mut r) => r.swap_remove(0),
            Err(resp) => return resp,
        };
        for row in page["rows"].as_array().map(Vec::as_slice).unwrap_or(&[]) {
            let key = row["key"].as_str().unwrap_or_default();
            if let Some((_, name)) = parse_doc_key(key) {
                docs.push((name.to_string(), row["value"].clone()));
            }
        }
        match page["nextAfter"].as_str() {
            Some(next) if !next.is_empty() && after.as_deref() != Some(next) => {
                after = Some(next.to_string())
            }
            _ => break,
        }
    }
    let sources: Vec<String> = docs
        .iter()
        .filter(|(_, v)| is_source(v))
        .map(|(n, _)| n.clone())
        .collect();
    let mut states = match source_states(st, tenant, &sources).await {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let connectors: Vec<Value> = docs
        .iter()
        .map(|(name, value)| {
            let state = states.remove(name).unwrap_or(Value::Null);
            entry(tenant, name, value, state)
        })
        .collect();
    json(
        StatusCode::OK,
        serde_json::json!({ "connectors": connectors }).to_string(),
    )
}

pub(crate) async fn get_one(st: &AppState, tenant: &str, name: &str) -> Response {
    if let Some(r) = reserved(tenant) {
        return r;
    }
    if !valid_connector_name(name) {
        return not_found(name);
    }
    let stored = match read_stored(st, tenant, name).await {
        Ok(Some(s)) => s,
        Ok(None) => return not_found(name),
        Err(resp) => return resp,
    };
    let state = if is_source(&stored.value) {
        match source_states(st, tenant, &[name.to_string()]).await {
            Ok(mut s) => s.remove(name).unwrap_or(Value::Null),
            Err(resp) => return resp,
        }
    } else {
        Value::Null
    };
    json(
        StatusCode::OK,
        entry(tenant, name, &stored.value, state).to_string(),
    )
}

/// `PUT`: the document checked by the crate's own rules
/// ([`ConnectorDoc::validate`], the one place they live), the password
/// sealed, the broker's fields set, stored if nothing else wrote it since it
/// was read.
pub(crate) async fn put(
    st: &AppState,
    sealer: Option<&Encryption>,
    tenant: &str,
    name: &str,
    body: &[u8],
) -> Response {
    if let Some(r) = reserved(tenant) {
        return r;
    }
    if !valid_connector_name(name) {
        return refuse(
            StatusCode::BAD_REQUEST,
            "bad_connector_name",
            format!(
                "`{name}` is not a connector name: lowercase letters, digits, `_` and `-`, \
                 starting with a letter or a digit, at most 48 characters \
                 (^[a-z0-9][a-z0-9_-]{{0,47}}$)"
            ),
        );
    }
    if body.len() > MAX_BODY_BYTES {
        return refuse(
            StatusCode::PAYLOAD_TOO_LARGE,
            "body_too_large",
            format!(
                "the document is {} bytes; the ceiling is {MAX_BODY_BYTES}",
                body.len()
            ),
        );
    }
    let value: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            return refuse(
                StatusCode::BAD_REQUEST,
                "bad_body",
                format!("the body is not JSON: {e}"),
            )
        }
    };
    let mut doc = match ConnectorDoc::from_json(&value) {
        Ok(d) => d,
        Err(e) => return refuse(StatusCode::BAD_REQUEST, "invalid_connector", e.to_string()),
    };
    // The broker's fields are the broker's (see the module header).
    doc.updated_at = None;
    doc.resync_requested_at = None;
    doc.deleting = None;
    doc.connection.password_sealed = None;
    if let Err(e) = doc.normalize() {
        return refuse(StatusCode::BAD_REQUEST, "invalid_connector", e.to_string());
    }
    if let Err(e) = doc.validate(name) {
        return refuse(StatusCode::BAD_REQUEST, "invalid_connector", e.to_string());
    }
    if let Some(refused) = egress_refusal(&doc).await {
        return refused;
    }
    let stored = match read_stored(st, tenant, name).await {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    if let Some(old) = stored
        .as_ref()
        .and_then(|s| s.value["kind"].as_str())
        .filter(|k| *k != doc.kind.as_str())
    {
        return refuse(
            StatusCode::CONFLICT,
            "kind_change",
            format!(
                "connector `{name}` is a {old}, and a connector never changes kind: delete it \
                 first, then create the {}",
                doc.kind.as_str()
            ),
        );
    }
    match doc.connection.password.take() {
        // Removing the stored password: no key needed for that.
        Some(p) if p.is_empty() => {}
        Some(p) => {
            let Some(key) = sealer else {
                return refuse(
                    StatusCode::BAD_REQUEST,
                    "encryption_required",
                    "this broker has no QUEEN_ENCRYPTION_KEY, and a connector's password is \
                     never stored in clear: set the same 64-hex-character key on every node, \
                     then send the document again",
                );
            };
            match key
                .encrypt(p.as_bytes())
                .and_then(|b| String::from_utf8(b).ok())
            {
                Some(sealed) => doc.connection.password_sealed = Some(sealed),
                None => {
                    return refuse(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "seal_failed",
                        "sealing the password failed",
                    )
                }
            }
        }
        // No password sent: the stored one stays.
        None => {
            doc.connection.password_sealed = stored.as_ref().and_then(|s| {
                s.value["connection"]["passwordSealed"]
                    .as_str()
                    .map(str::to_string)
            })
        }
    }
    // A resync asked for and not yet carried out survives an edit; a delete
    // in progress does not: writing the connector again is taking it back.
    doc.resync_requested_at = stored
        .as_ref()
        .and_then(|s| s.value["resyncRequestedAt"].as_str().map(str::to_string));
    doc.deleting = None;
    doc.updated_at = Some(now_iso());
    let value = match serde_json::to_value(&doc) {
        Ok(v) => v,
        Err(e) => {
            return refuse(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                format!("the document does not serialize: {e}"),
            )
        }
    };
    let size = value.to_string().len();
    let ceiling = crate::rsm::planner::kv::max_value_bytes();
    if size > ceiling {
        return refuse(
            StatusCode::PAYLOAD_TOO_LARGE,
            "document_too_large",
            format!(
                "the stored document would be {size} bytes, and the broker's KV value ceiling \
                 is {ceiling} (QUEEN_KV_MAX_VALUE_BYTES)"
            ),
        );
    }
    let expect = stored.as_ref().map_or(0, |s| s.version);
    if let Err(resp) = write_doc(st, tenant, name, value, expect).await {
        return resp;
    }
    let mut out = match doc.redacted() {
        Value::Object(m) => m,
        _ => Map::new(),
    };
    out.insert("name".into(), Value::String(name.to_string()));
    json(StatusCode::OK, Value::Object(out).to_string())
}

/// With `QUEEN_PG_ALLOW_PRIVATE_NETWORKS=false`, a host that resolves to a
/// loopback, private, link-local or unspecified address is refused here, as
/// it is again at every connect (a name can resolve differently later). A
/// name that does not resolve yet, or not within [`EGRESS_LOOKUP_MAX`], is
/// not this check's verdict.
async fn egress_refusal(doc: &ConnectorDoc) -> Option<Response> {
    let policy = crate::pg_inproc::egress_policy();
    if policy.allow_private {
        return None;
    }
    let lookup = queen_pg::pg::connect::resolve(&doc.connection.host, doc.connection.port, &policy);
    match tokio::time::timeout(EGRESS_LOOKUP_MAX, lookup).await {
        Ok(Err(e)) if e.code() == "egress" => Some(refuse(
            StatusCode::BAD_REQUEST,
            "egress_refused",
            e.to_string(),
        )),
        _ => None,
    }
}

pub(crate) async fn delete(
    st: &AppState,
    tenant: &str,
    name: &str,
    drop_slot: Option<&str>,
) -> Response {
    if let Some(r) = reserved(tenant) {
        return r;
    }
    let drop_slot = match drop_slot.map(str::trim) {
        None | Some("true" | "1") => true,
        Some("false" | "0") => false,
        Some(other) => {
            return refuse(
                StatusCode::BAD_REQUEST,
                "bad_request",
                format!("dropSlot must be true or false, not `{other}`"),
            )
        }
    };
    if !valid_connector_name(name) {
        return not_found(name);
    }
    let stored = match read_stored(st, tenant, name).await {
        Ok(Some(s)) => s,
        Ok(None) => return not_found(name),
        Err(resp) => return resp,
    };
    let doc = ConnectorDoc::from_json(&stored.value).ok();
    // A sink holds nothing outside Queen but its progress table, which is the
    // target database's own; a document this broker cannot read runs no
    // engine that could carry out a teardown. Both simply go.
    let Some(mut doc) = doc.filter(|d| d.kind == Kind::Source) else {
        return match delete_doc(st, tenant, name, stored.version).await {
            Ok(()) => json(
                StatusCode::OK,
                serde_json::json!({"name": name, "deleted": true}).to_string(),
            ),
            Err(resp) => resp,
        };
    };
    let slot = doc.slot_name(name);
    if drop_slot {
        // The owner drops the slot and a managed publication, removes the
        // source's runtime state and reports the teardown; the manager then
        // removes the document (pg_inproc.rs). Asked once: a second DELETE
        // while it runs restarts nothing.
        let detail = format!(
            "the owner drops the replication slot `{slot}` (and the publication, when the \
             connector manages it), then the connector is gone; if the database is gone for \
             good, DELETE …?dropSlot=false removes the connector without it"
        );
        if doc.deleting.as_ref().is_some_and(|d| d.drop_slot) {
            return json(
                StatusCode::ACCEPTED,
                serde_json::json!({"name": name, "deleting": true, "detail": detail}).to_string(),
            );
        }
        let now = now_iso();
        doc.deleting = Some(DeleteRequest {
            drop_slot: true,
            requested_at: now.clone(),
        });
        doc.updated_at = Some(now);
        let value = match serde_json::to_value(&doc) {
            Ok(v) => v,
            Err(e) => {
                return refuse(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    format!("the document does not serialize: {e}"),
                )
            }
        };
        return match write_doc(st, tenant, name, value, stored.version).await {
            Ok(()) => json(
                StatusCode::ACCEPTED,
                serde_json::json!({"name": name, "deleting": true, "detail": detail}).to_string(),
            ),
            Err(resp) => resp,
        };
    }
    // `dropSlot=false`: gone now, the database untouched. The document first,
    // so every node stops the source within a reload; then its runtime state.
    // A source that was mid-restart on some node in that window can write its
    // pointer again before it stops: a later DELETE removes that too.
    if let Err(resp) = delete_doc(st, tenant, name, stored.version).await {
        return resp;
    }
    let runtime = vec![
        serde_json::json!({"op": "delete", "ns": KV_NAMESPACE, "key": pointer_key(name)}),
        serde_json::json!({"op": "delete", "ns": KV_NAMESPACE, "key": lease_key(name)}),
    ];
    if let Err(resp) = kv(st, tenant, runtime).await {
        return resp;
    }
    let publication = if doc.source.as_ref().is_some_and(|s| s.manage_publication) {
        format!(" and `DROP PUBLICATION {}`", doc.publication_name(name))
    } else {
        String::new()
    };
    json(
        StatusCode::OK,
        serde_json::json!({
            "name": name,
            "deleted": true,
            "warning": format!(
                "the replication slot `{slot}` still exists on the database and keeps its WAL \
                 until it is dropped there: `SELECT pg_drop_replication_slot('{slot}')`{publication}"
            ),
        })
        .to_string(),
    )
}

pub(crate) async fn resync(st: &AppState, tenant: &str, name: &str) -> Response {
    if let Some(r) = reserved(tenant) {
        return r;
    }
    if !valid_connector_name(name) {
        return not_found(name);
    }
    let stored = match read_stored(st, tenant, name).await {
        Ok(Some(s)) => s,
        Ok(None) => return not_found(name),
        Err(resp) => return resp,
    };
    let mut doc = match ConnectorDoc::from_json(&stored.value) {
        Ok(d) => d,
        Err(e) => {
            return refuse(
                StatusCode::CONFLICT,
                "unreadable_document",
                format!("the stored document does not parse ({e}): PUT it again first"),
            )
        }
    };
    if doc.kind != Kind::Source {
        return refuse(
            StatusCode::BAD_REQUEST,
            "not_a_source",
            format!(
                "connector `{name}` is a sink: only a source has a slot and a snapshot to redo"
            ),
        );
    }
    if doc.deleting.is_some() {
        return refuse(
            StatusCode::CONFLICT,
            "deleting",
            format!("connector `{name}` is being deleted"),
        );
    }
    let now = now_iso();
    doc.resync_requested_at = Some(now.clone());
    doc.updated_at = Some(now.clone());
    let value = match serde_json::to_value(&doc) {
        Ok(v) => v,
        Err(e) => {
            return refuse(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                format!("the document does not serialize: {e}"),
            )
        }
    };
    match write_doc(st, tenant, name, value, stored.version).await {
        Ok(()) => json(
            StatusCode::ACCEPTED,
            serde_json::json!({"name": name, "resyncRequestedAt": now}).to_string(),
        ),
        Err(resp) => resp,
    }
}

/// The routes' logic over a REAL single-node state machine: what is stored in
/// the internal tenant, what a read shows, and every refusal. The documents
/// are checked by the crate's own rules (`config_validate.rs`).
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use serde_json::json;

    use super::*;
    use crate::config::DEFAULT_TENANT;
    use crate::rsm::facade::real::RaftFacade;
    use crate::rsm::facade::{Rsm, RsmBuildCtx};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    const OTHER: &str = "33333333-3333-4333-8333-333333333333";

    /// A raft `AppState` over a fresh single-node facade. Its data directory
    /// (a few hundred KB) is left behind, like the S3 sink tests'.
    fn state(tag: &str) -> Arc<AppState> {
        std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
        let dir = std::env::temp_dir().join(format!(
            "queen-pg-api-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::env::set_var("QUEEN_RAFT_DIR", dir.join("cfg").display().to_string());
        let cfg = crate::config::load();
        let facade = RaftFacade::open(&RsmBuildCtx {
            data_dir: dir.display().to_string(),
            notifier: crate::notify::Notifier::new(false),
            disk_high_pct: 99.5,
            disk_low_pct: 99.0,
        })
        .expect("open the facade");
        let rsm: Arc<dyn Rsm> = Arc::new(facade);
        crate::handlers::raft::build_raft_state_with(&cfg, Some(rsm)).expect("raft state")
    }

    async fn answer(resp: Response) -> (StatusCode, Value) {
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("a body");
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    /// The document as stored, broker fields and sealed password included.
    async fn stored(st: &AppState, tenant: &str, name: &str) -> Option<Value> {
        match read_stored(st, tenant, name).await {
            Ok(s) => s.map(|s| s.value),
            Err(_) => panic!("reading {tenant}/{name} failed"),
        }
    }

    fn sink_doc(password: Option<&str>) -> Value {
        let mut v = json!({
            "kind": "sink",
            "connection": {"host": "db.example.com", "port": 5432, "database": "app",
                           "user": "writer", "sslMode": "require"},
            "sink": {"queue": "orders", "table": "public.orders_copy", "mode": "upsert",
                     "key": ["id"]},
        });
        if let Some(p) = password {
            v["connection"]["password"] = Value::String(p.to_string());
        }
        v
    }

    fn source_doc(password: Option<&str>) -> Value {
        let mut v = json!({
            "kind": "source",
            "connection": {"host": "db.example.com", "port": 5432, "database": "app",
                           "user": "cdc", "sslMode": "require"},
            "source": {"tables": [{"table": "public.orders", "queue": "orders"}]},
        });
        if let Some(p) = password {
            v["connection"]["password"] = Value::String(p.to_string());
        }
        v
    }

    async fn put_ok(
        st: &AppState,
        enc: &Encryption,
        tenant: &str,
        name: &str,
        doc: &Value,
    ) -> Value {
        let (status, body) =
            answer(put(st, Some(enc), tenant, name, doc.to_string().as_bytes()).await).await;
        assert_eq!(status, StatusCode::OK, "PUT {tenant}/{name}: {body}");
        body
    }

    async fn kv_as(st: &AppState, tenant: &str, ops: Vec<Value>) -> Vec<Value> {
        match kv(st, tenant, ops).await {
            Ok(r) => r,
            Err(_) => panic!("a KV call as {tenant} failed"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn put_validates_seals_and_never_returns_the_password() {
        let st = state("put");
        let enc = Encryption::for_test([5u8; 32]);

        // The crate's rules, with the field named: an unknown field, a limit.
        let mut unknown = sink_doc(Some("pw"));
        unknown["sink"]["bogus"] = json!(1);
        let (status, body) = answer(
            put(
                &st,
                Some(&enc),
                DEFAULT_TENANT,
                "copy",
                unknown.to_string().as_bytes(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"], "invalid_connector", "{body}");
        assert!(
            body["detail"].as_str().unwrap_or("").contains("bogus"),
            "{body}"
        );
        let mut limit = sink_doc(Some("pw"));
        limit["sink"]["batch"] = json!(0);
        let (status, body) = answer(
            put(
                &st,
                Some(&enc),
                DEFAULT_TENANT,
                "copy",
                limit.to_string().as_bytes(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            body["detail"].as_str().unwrap_or("").contains("batch"),
            "{body}"
        );
        assert!(
            stored(&st, DEFAULT_TENANT, "copy").await.is_none(),
            "nothing stored"
        );

        let body = put_ok(&st, &enc, DEFAULT_TENANT, "copy", &sink_doc(Some("s3cr3t"))).await;
        assert_eq!(body["name"], "copy", "{body}");
        assert!(!body.to_string().contains("s3cr3t"), "{body}");
        assert!(body["connection"].get("passwordSealed").is_none(), "{body}");
        let raw = stored(&st, DEFAULT_TENANT, "copy").await.expect("stored");
        assert!(raw["connection"].get("password").is_none(), "{raw}");
        let sealed = raw["connection"]["passwordSealed"]
            .as_str()
            .expect("sealed");
        assert!(!sealed.contains("s3cr3t"), "{sealed}");
        assert_eq!(
            enc.decrypt_payload_bytes(sealed.as_bytes()).as_deref(),
            Some(&b"s3cr3t"[..])
        );
        assert!(
            raw["updatedAt"].as_str().is_some_and(|t| t.ends_with('Z')),
            "{raw}"
        );
        // The reads never show it either.
        let (status, one) = answer(get_one(&st, DEFAULT_TENANT, "copy").await).await;
        assert_eq!(status, StatusCode::OK, "{one}");
        assert!(!one.to_string().contains("s3cr3t"), "{one}");
        assert!(!one.to_string().contains(sealed), "{one}");

        // A password inside a URL is the same password.
        let mut by_url = sink_doc(None);
        by_url["connection"] =
            json!({"url": "postgres://writer:urlpw@db.example.com:5432/app?sslmode=require"});
        put_ok(&st, &enc, DEFAULT_TENANT, "by-url", &by_url).await;
        let raw = stored(&st, DEFAULT_TENANT, "by-url").await.expect("stored");
        assert!(!raw.to_string().contains("urlpw"), "{raw}");
        let sealed = raw["connection"]["passwordSealed"]
            .as_str()
            .expect("sealed");
        assert_eq!(
            enc.decrypt_payload_bytes(sealed.as_bytes()).as_deref(),
            Some(&b"urlpw"[..])
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn without_a_key_a_password_is_refused_and_an_edit_keeps_the_sealed_one() {
        let st = state("seal");
        let (status, body) = answer(
            put(
                &st,
                None,
                DEFAULT_TENANT,
                "copy",
                sink_doc(Some("pw")).to_string().as_bytes(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"], "encryption_required", "{body}");
        assert!(stored(&st, DEFAULT_TENANT, "copy").await.is_none());

        let enc = Encryption::for_test([5u8; 32]);
        put_ok(&st, &enc, DEFAULT_TENANT, "copy", &sink_doc(Some("first"))).await;
        let sealed = stored(&st, DEFAULT_TENANT, "copy").await.expect("stored")["connection"]
            ["passwordSealed"]
            .clone();
        assert!(sealed.is_string());

        // An edit without the password, by a node with no key at all: the
        // stored one stays, the edit lands.
        let mut edit = sink_doc(None);
        edit["sink"]["batch"] = json!(50);
        let (status, body) = answer(
            put(
                &st,
                None,
                DEFAULT_TENANT,
                "copy",
                edit.to_string().as_bytes(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let raw = stored(&st, DEFAULT_TENANT, "copy").await.expect("stored");
        assert_eq!(raw["connection"]["passwordSealed"], sealed, "{raw}");
        assert_eq!(raw["sink"]["batch"], 50, "{raw}");

        // A body naming the broker's own fields does not set them.
        let mut forged = sink_doc(None);
        forged["connection"]["passwordSealed"] = json!("{\"encrypted\":\"x\"}");
        forged["updatedAt"] = json!("1999-01-01T00:00:00Z");
        put_ok(&st, &enc, DEFAULT_TENANT, "copy", &forged).await;
        let raw = stored(&st, DEFAULT_TENANT, "copy").await.expect("stored");
        assert_eq!(raw["connection"]["passwordSealed"], sealed, "{raw}");
        assert_ne!(raw["updatedAt"], "1999-01-01T00:00:00Z", "{raw}");

        // An empty password removes it.
        put_ok(&st, &enc, DEFAULT_TENANT, "copy", &sink_doc(Some(""))).await;
        let raw = stored(&st, DEFAULT_TENANT, "copy").await.expect("stored");
        assert!(raw["connection"].get("passwordSealed").is_none(), "{raw}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn reads_list_the_callers_connectors_with_their_state() {
        let st = state("reads");
        let enc = Encryption::for_test([5u8; 32]);
        put_ok(&st, &enc, DEFAULT_TENANT, "copy", &sink_doc(Some("pw"))).await;
        put_ok(&st, &enc, DEFAULT_TENANT, "cdc", &source_doc(Some("pw"))).await;
        put_ok(&st, &enc, OTHER, "copy", &sink_doc(None)).await;
        // The source's runtime state, in its own tenant.
        kv_as(
            &st,
            DEFAULT_TENANT,
            vec![
                json!({"op": "put", "ns": "queen-pg", "key": "src:cdc:pointer",
                        "value": {"v": 1, "lsn": "0/16B3748"}, "forever": true}),
            ],
        )
        .await;

        let (status, body) = answer(list(&st, DEFAULT_TENANT).await).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let all = body["connectors"].as_array().expect("connectors");
        let names: Vec<&str> = all.iter().filter_map(|c| c["name"].as_str()).collect();
        assert_eq!(names, vec!["cdc", "copy"], "{body}");
        let cdc = &all[0];
        assert_eq!(cdc["kind"], "source", "{cdc}");
        assert_eq!(cdc["state"]["pointer"]["lsn"], "0/16B3748", "{cdc}");
        assert_eq!(cdc["state"]["lease"], Value::Null, "{cdc}");
        assert!(cdc["status"]["phase"].is_string(), "{cdc}");
        let copy = &all[1];
        assert_eq!(copy["state"], Value::Null, "{copy}");
        assert!(!body.to_string().contains("passwordSealed"), "{body}");

        let (status, other) = answer(list(&st, OTHER).await).await;
        assert_eq!(status, StatusCode::OK, "{other}");
        assert_eq!(
            other["connectors"].as_array().map(Vec::len),
            Some(1),
            "{other}"
        );

        let (status, one) = answer(get_one(&st, DEFAULT_TENANT, "cdc").await).await;
        assert_eq!(status, StatusCode::OK, "{one}");
        assert_eq!(one["name"], "cdc");
        assert_eq!(one["state"]["pointer"]["lsn"], "0/16B3748", "{one}");
        let (status, missing) = answer(get_one(&st, DEFAULT_TENANT, "nope").await).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{missing}");
        assert_eq!(missing["error"], "connector_not_found", "{missing}");
        // Another tenant's connector is not this tenant's.
        let (status, _) = answer(get_one(&st, OTHER, "cdc").await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn a_connector_never_changes_kind_and_writes_do_not_race() {
        let st = state("kind");
        let enc = Encryption::for_test([5u8; 32]);
        put_ok(&st, &enc, DEFAULT_TENANT, "x", &sink_doc(None)).await;
        let (status, body) = answer(
            put(
                &st,
                Some(&enc),
                DEFAULT_TENANT,
                "x",
                source_doc(None).to_string().as_bytes(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["error"], "kind_change", "{body}");
        assert_eq!(
            stored(&st, DEFAULT_TENANT, "x").await.expect("stored")["kind"],
            "sink"
        );

        // A write on a version that moved meanwhile: 409, nothing written.
        let read = read_stored(&st, DEFAULT_TENANT, "x")
            .await
            .ok()
            .flatten()
            .expect("stored");
        put_ok(&st, &enc, DEFAULT_TENANT, "x", &sink_doc(None)).await;
        let stale = write_doc(
            &st,
            DEFAULT_TENANT,
            "x",
            json!({"kind": "sink"}),
            read.version,
        )
        .await;
        let (status, body) = answer(stale.expect_err("a stale write")).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["error"], "conflict", "{body}");
        assert!(stored(&st, DEFAULT_TENANT, "x").await.expect("stored")["connection"].is_object());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn deletes_remove_a_sink_and_ask_a_source_to_drop_its_slot() {
        let st = state("delete");
        let enc = Encryption::for_test([5u8; 32]);

        // A sink: gone.
        put_ok(&st, &enc, DEFAULT_TENANT, "copy", &sink_doc(None)).await;
        let (status, body) = answer(delete(&st, DEFAULT_TENANT, "copy", None).await).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["deleted"], true, "{body}");
        assert!(stored(&st, DEFAULT_TENANT, "copy").await.is_none());
        let (status, _) = answer(delete(&st, DEFAULT_TENANT, "copy", None).await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // A source: its owner is asked to drop the slot first.
        put_ok(&st, &enc, DEFAULT_TENANT, "cdc", &source_doc(None)).await;
        let before = stored(&st, DEFAULT_TENANT, "cdc").await.expect("stored");
        let (status, body) = answer(delete(&st, DEFAULT_TENANT, "cdc", None).await).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        assert_eq!(body["deleting"], true, "{body}");
        let raw = stored(&st, DEFAULT_TENANT, "cdc")
            .await
            .expect("still stored");
        assert_eq!(raw["deleting"]["dropSlot"], true, "{raw}");
        assert!(raw["deleting"]["requestedAt"].is_string(), "{raw}");
        assert_ne!(
            raw["updatedAt"], before["updatedAt"],
            "the units restart: {raw}"
        );
        // Asked twice, written once.
        let (status, _) = answer(delete(&st, DEFAULT_TENANT, "cdc", Some("true")).await).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(
            stored(&st, DEFAULT_TENANT, "cdc").await.expect("stored"),
            raw
        );
        // An edit takes the connector back.
        put_ok(&st, &enc, DEFAULT_TENANT, "cdc", &source_doc(None)).await;
        assert!(stored(&st, DEFAULT_TENANT, "cdc")
            .await
            .expect("stored")
            .get("deleting")
            .is_none());

        // `dropSlot=false`: gone now, its runtime state with it, and the
        // answer says what is left on the database.
        kv_as(
            &st,
            DEFAULT_TENANT,
            vec![
                json!({"op": "put", "ns": "queen-pg", "key": "src:cdc:pointer",
                       "value": {"lsn": "0/1"}, "forever": true}),
                json!({"op": "put", "ns": "queen-pg", "key": "src:cdc:lease",
                       "value": {"node": "n1"}, "ttlSeconds": 60}),
                json!({"op": "put", "ns": "queen-pg", "key": "src:other:pointer",
                       "value": {"lsn": "0/2"}, "forever": true}),
            ],
        )
        .await;
        let (status, body) = answer(delete(&st, DEFAULT_TENANT, "cdc", Some("false")).await).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(
            body["warning"]
                .as_str()
                .is_some_and(|w| w.contains("queen_cdc")),
            "{body}"
        );
        assert!(stored(&st, DEFAULT_TENANT, "cdc").await.is_none());
        let left = kv_as(
            &st,
            DEFAULT_TENANT,
            vec![json!({"op": "getMany", "ns": "queen-pg",
                        "keys": ["src:cdc:pointer", "src:cdc:lease", "src:other:pointer"]})],
        )
        .await;
        let keys: Vec<&str> = left[0]["rows"]
            .as_array()
            .expect("rows")
            .iter()
            .filter_map(|r| r["key"].as_str())
            .collect();
        assert_eq!(keys, vec!["src:other:pointer"], "{left:?}");

        let (status, body) = answer(delete(&st, DEFAULT_TENANT, "cdc", Some("maybe")).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn resync_marks_a_source_and_survives_an_edit() {
        let st = state("resync");
        let enc = Encryption::for_test([5u8; 32]);
        put_ok(&st, &enc, DEFAULT_TENANT, "cdc", &source_doc(None)).await;
        let (status, body) = answer(resync(&st, DEFAULT_TENANT, "cdc").await).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let at = body["resyncRequestedAt"]
            .as_str()
            .expect("when")
            .to_string();
        let raw = stored(&st, DEFAULT_TENANT, "cdc").await.expect("stored");
        assert_eq!(raw["resyncRequestedAt"], at.as_str(), "{raw}");
        assert_eq!(raw["updatedAt"], at.as_str(), "the units restart: {raw}");
        // An edit before the owner carried it out keeps the request.
        let mut edit = source_doc(None);
        edit["source"]["lingerMs"] = json!(50);
        put_ok(&st, &enc, DEFAULT_TENANT, "cdc", &edit).await;
        let raw = stored(&st, DEFAULT_TENANT, "cdc").await.expect("stored");
        assert_eq!(raw["resyncRequestedAt"], at.as_str(), "{raw}");

        put_ok(&st, &enc, DEFAULT_TENANT, "copy", &sink_doc(None)).await;
        let (status, body) = answer(resync(&st, DEFAULT_TENANT, "copy").await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"], "not_a_source", "{body}");
        let (status, _) = answer(resync(&st, DEFAULT_TENANT, "nope").await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        // Being deleted: no resync.
        let (status, _) = answer(delete(&st, DEFAULT_TENANT, "cdc", None).await).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let (status, body) = answer(resync(&st, DEFAULT_TENANT, "cdc").await).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn names_bodies_and_the_internal_tenant_are_refused() {
        let st = state("refusals");
        let enc = Encryption::for_test([5u8; 32]);
        let doc = sink_doc(None).to_string();
        let long = "x".repeat(49);
        for bad in ["Copy", "-copy", "has space", long.as_str(), ""] {
            let (status, body) =
                answer(put(&st, Some(&enc), DEFAULT_TENANT, bad, doc.as_bytes()).await).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad:?}: {body}");
            assert_eq!(body["error"], "bad_connector_name", "{body}");
        }
        let (status, body) =
            answer(put(&st, Some(&enc), DEFAULT_TENANT, "copy", b"{not json").await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"], "bad_body", "{body}");
        let huge = vec![b' '; MAX_BODY_BYTES + 1];
        let (status, _) = answer(put(&st, Some(&enc), DEFAULT_TENANT, "copy", &huge).await).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);

        for resp in [
            list(&st, SYSTEM_TENANT).await,
            get_one(&st, SYSTEM_TENANT, "copy").await,
            put(&st, Some(&enc), SYSTEM_TENANT, "copy", doc.as_bytes()).await,
            delete(&st, SYSTEM_TENANT, "copy", None).await,
            resync(&st, SYSTEM_TENANT, "copy").await,
        ] {
            let (status, body) = answer(resp).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        }
    }

    /// The routes as the raft router registers them, through the whole
    /// router (auth off, tenancy off): the methods, the `:name` segment, the
    /// document body limit, and the handlers' own key (none in a test
    /// process: a password is refused, a document without one is stored).
    #[cfg(feature = "kafka")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn the_router_serves_the_connectors_routes() {
        use axum::http::{Method, Request};
        use tower::ServiceExt;

        let st = state("router");
        let auth = crate::auth::Authenticator::new(crate::config::AuthConfig {
            enabled: false,
            algorithm: "HS256".into(),
            secret: String::new(),
            public_key: String::new(),
            jwks_url: String::new(),
            jwks_refresh_interval_seconds: 3600,
            jwks_request_timeout_ms: 5000,
            issuer: String::new(),
            audience: String::new(),
            clock_skew_seconds: 30,
            skip_paths: Vec::new(),
            roles_claim: "role".into(),
            roles_array_claim: "roles".into(),
            role_admin: "admin".into(),
            role_read_write: "read-write".into(),
            role_read_only: "read-only".into(),
            role_write_only: "write-only".into(),
        });
        let router = crate::handlers::raft::build_raft_router(st, auth, false);
        let call = |method: Method, path: &str, body: Vec<u8>| {
            let router = router.clone();
            let req = Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .expect("a request");
            async move { answer(router.oneshot(req).await.expect("an answer")).await }
        };

        let doc = sink_doc(None).to_string().into_bytes();
        let (status, body) = call(Method::PUT, "/api/v1/connectors/copy", doc.clone()).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["name"], "copy", "{body}");
        let (status, body) = call(
            Method::PUT,
            "/api/v1/connectors/copy",
            sink_doc(Some("pw")).to_string().into_bytes(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"], "encryption_required", "{body}");
        let (status, body) = call(Method::GET, "/api/v1/connectors", Vec::new()).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["connectors"].as_array().map(Vec::len),
            Some(1),
            "{body}"
        );
        let (status, body) = call(Method::GET, "/api/v1/connectors/copy", Vec::new()).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = call(Method::POST, "/api/v1/connectors/copy/resync", Vec::new()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"], "not_a_source", "{body}");
        // A body past the document limit is refused before it is read whole.
        let (status, _) = call(
            Method::PUT,
            "/api/v1/connectors/big",
            vec![b' '; MAX_BODY_BYTES + 1],
        )
        .await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        let (status, body) = call(
            Method::DELETE,
            "/api/v1/connectors/copy?dropSlot=false",
            Vec::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, _) = call(Method::GET, "/api/v1/connectors/copy", Vec::new()).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// A stored document this broker cannot parse (a newer broker's) is still
    /// shown without its password, and can still be deleted.
    #[test]
    fn an_unreadable_document_is_shown_without_its_password() {
        let raw = json!({
            "kind": "source",
            "futureField": true,
            "connection": {"host": "h", "password": "clear", "passwordSealed": "{\"encrypted\":\"x\"}",
                           "url": "postgres://u:clear@h/db"},
        });
        let shown = redacted(&raw);
        assert!(!shown.to_string().contains("clear"), "{shown}");
        assert!(!shown.to_string().contains("encrypted"), "{shown}");
        assert_eq!(shown["connection"]["passwordSet"], true, "{shown}");
        assert_eq!(shown["futureField"], true, "{shown}");
    }
}
