//! The web plane's state (PLAN_SINGLE_BINARY.md W4): users, identities,
//! operations, outbox — plus every read or write the web-plane modules
//! (console.rs, oauth.rs, operator.rs; acting.rs and webapp.rs have no SQL of
//! their own) make on the data-plane tables, collected in the section marked
//! "data-plane reads/writes used by the web plane" for the merge to dedupe
//! against `data.rs`.
//!
//! Every function answers from either backend:
//!
//! - `Store::Pg` — the SQL the handler ran before this seam, moved here
//!   verbatim (same statements, same stored functions, same transactions and
//!   the same error answers), so the standalone proxy behaves as it did.
//! - `Store::Kv` — documents per [`super::schema`], and the stored functions of
//!   migrations 002–011 reimplemented in Rust with the same validation, the
//!   same messages and the same results. Each call commits its writes as ONE
//!   atomic KV batch; the reads before it are covered by version
//!   preconditions, and a lost race re-reads and retries ([`KV_RETRIES`]).
//!   Unique indexes are `putIfAbsent` + `required` in the row's own batch.
//! - `Store::None` (dev-static) — [`WebError::NotConfigured`]; the handlers
//!   check `Store::is_some` first, exactly where they checked the pool.
//!
//! Notification: on Postgres every mutation with a cluster emits
//! `queen_proxy_inval` through `record_operation`. The KV has no channel, so a
//! handler that wrote calls [`invalidate_local`] (this node's caches; the
//! other nodes converge on their cache TTLs).
//!
//! Sessions: the deny-list is `px.revoked #<jti>` → [`RevokedDoc`], written by
//! [`revoke_session`] with a TTL that ends at the token's own `exp` (so no
//! sweep is needed); the auth hot path (data plane) reads the same key.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};

use axum::http::StatusCode;
use axum::response::Response;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};
use tokio_postgres::error::SqlState;
use uuid::Uuid;

use super::kv::{self, Doc, Expect, KvBackend, KvError, Ttl};
use super::schema::{
    self, ns, ApiKeyDoc, CellDoc, ClusterDoc, IdentityDoc, OperationDoc, OutboxDoc, PlanDoc, QueueDoc, RevokedDoc,
    RoleDoc, TenantDoc, UsageDoc, UserDoc,
};
use super::Store;
use crate::errors;

/// `operations.actor` CHECK (001_init).
const ACTORS: [&str; 4] = ["user", "api_key", "control_plane", "system"];
/// `cluster_roles.role` CHECK (001_init) and grant_cluster_role's list.
const ROLES: [&str; 4] = ["admin", "producer", "consumer", "viewer"];
/// `api_keys.scopes` CHECK (001_init) and issue_api_key's list.
const SCOPES: [&str; 4] = ["produce", "consume", "admin", "read"];
/// `identities.provider` CHECK (001_init) and create_user's list.
const PROVIDERS: [&str; 3] = ["local", "google", "github"];
/// `tenants.status` CHECK (001_init) and set_tenant_status's list.
const TENANT_STATUSES: [&str; 4] = ["active", "grace", "suspended", "deleting"];
/// How many times a KV write that lost a version race is re-read and retried.
pub const KV_RETRIES: usize = 5;
/// Ops per batch for the non-atomic bulk deletes of a tenant wipe (the
/// broker's wire ceiling is 64 ops per call).
const DELETE_CHUNK: usize = 64;
/// The value of a "rows of X" index entry (schema.rs: an empty value).
const ENTRY: &str = "";

// ===========================================================================
// errors
// ===========================================================================

/// Why a repository call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebError {
    /// No store behind this proxy (dev-static).
    NotConfigured,
    /// The store could not be reached: a pool checkout failed, or the broker's
    /// KV had no answer (no leader, timeout). The handlers' "pxdb unavailable".
    Unavailable(String),
    /// A statement or KV call failed.
    Db(String),
    /// A unique index already holds the value (Postgres 23505; a KV unique
    /// index entry that already exists).
    Conflict(String),
    /// A stored function refused: its RAISE EXCEPTION message (Postgres
    /// P0001), or the same message from the KV port.
    Raised(String),
    /// KV only, internal: a document changed between the read and the write.
    /// Retried; surfaces as `Db` once [`KV_RETRIES`] run out.
    Contended(String),
}

impl WebError {
    pub fn is_unavailable(&self) -> bool {
        matches!(self, WebError::Unavailable(_))
    }
}

impl std::fmt::Display for WebError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebError::NotConfigured => write!(f, "no store configured (dev-static)"),
            WebError::Unavailable(m) => write!(f, "store unavailable: {m}"),
            WebError::Db(m) | WebError::Raised(m) => write!(f, "{m}"),
            WebError::Conflict(m) => write!(f, "unique violation: {m}"),
            WebError::Contended(m) => write!(f, "concurrent update: {m}"),
        }
    }
}

impl std::error::Error for WebError {}

impl From<crate::store::data::DataError> for WebError {
    fn from(e: crate::store::data::DataError) -> WebError {
        use crate::store::data::DataError as D;
        match e {
            D::Invalid(m) => WebError::Raised(m),
            D::Conflict(m) => WebError::Conflict(m),
            D::Unavailable(m) => WebError::Unavailable(m),
            D::NoStore => WebError::NotConfigured,
        }
    }
}

impl From<KvError> for WebError {
    fn from(e: KvError) -> WebError {
        match e {
            KvError::Unavailable(m) => WebError::Unavailable(m),
            // A `required` op nobody mapped to a unique index: a document we
            // read moved underneath us.
            KvError::Precondition { detail } => WebError::Contended(detail.to_string()),
            other @ KvError::Invalid { .. } => WebError::Db(other.to_string()),
        }
    }
}

fn pg_err(e: tokio_postgres::Error) -> WebError {
    match e.code() {
        Some(c) if *c == SqlState::UNIQUE_VIOLATION => WebError::Conflict(e.to_string()),
        Some(c) if *c == SqlState::RAISE_EXCEPTION => {
            WebError::Raised(e.as_db_error().map(|d| d.message().to_string()).unwrap_or_else(|| e.to_string()))
        }
        _ => WebError::Db(e.to_string()),
    }
}

async fn pg(pool: &deadpool_postgres::Pool) -> Result<deadpool_postgres::Object, WebError> {
    pool.get().await.map_err(|e| WebError::Unavailable(e.to_string()))
}

fn raised(msg: impl Into<String>) -> WebError {
    WebError::Raised(msg.into())
}

/// A multi-step handler's refusal, carried as data: the status and message
/// the handler answered before the SQL moved here, so moving it changed no
/// byte of any answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// 502 `bad_gateway`.
    BadGateway(&'static str),
    /// 404 `not_found`.
    NotFound(&'static str),
    /// 409 `conflict`.
    Conflict(&'static str),
    /// 421 `cluster_unknown`.
    Misdirected(&'static str),
    /// 400 `invalid_request`.
    BadRequest(&'static str),
}

impl Refusal {
    pub fn response(&self) -> Response {
        match self {
            Refusal::BadGateway(m) => errors::err_502(m),
            Refusal::NotFound(m) => errors::err_404("not_found", m),
            Refusal::Conflict(m) => errors::json_error(StatusCode::CONFLICT, "conflict", m),
            Refusal::Misdirected(m) => errors::err_421(m),
            Refusal::BadRequest(m) => errors::json_error(StatusCode::BAD_REQUEST, "invalid_request", m),
        }
    }
}

/// What `pg_notify('queen_proxy_inval', cluster)` does for the standalone
/// proxy, done for the single binary: drop THIS node's cached view of each
/// cluster (host/key caches and every `on_invalidate` hook). A no-op on
/// Postgres, where the NOTIFY already reached every proxy.
pub fn invalidate_local(st: &crate::state::AppState, clusters: &[Uuid]) {
    if matches!(st.store, Store::Kv(_)) {
        for c in clusters {
            st.cache.invalidate(*c);
        }
    }
}

/// Retry a KV attempt while it loses version races (`WebError::Contended`).
macro_rules! retrying {
    ($attempt:expr) => {{
        let mut tries = 0usize;
        loop {
            match $attempt.await {
                Err(WebError::Contended(why)) => {
                    tries += 1;
                    if tries >= KV_RETRIES {
                        break Err(WebError::Db(format!("gave up after {tries} concurrent updates: {why}")));
                    }
                }
                other => break other,
            }
        }
    }};
}

/// Inside a script attempt (`Result<Result<T, Refusal>, WebError>`): a store
/// outage or a lost race propagates (answered / retried by the caller); any
/// other failure is the named step's 502, as on Postgres.
macro_rules! step {
    ($res:expr, $step:expr) => {
        match $res {
            Ok(v) => v,
            Err(e @ (WebError::Unavailable(_) | WebError::Contended(_))) => return Err(e),
            Err(e) => {
                tracing::warn!(target: "operator", err = %e, step = $step, "store step failed");
                return Ok(Err(Refusal::BadGateway($step)));
            }
        }
    };
}

/// A script's final mapping: its own refusal, "pxdb unavailable" for an
/// outage, `conflict` for a unique index when the script has a 409, and the
/// failed step's 502 for anything else.
fn script_answer<T>(
    r: Result<Result<T, Refusal>, WebError>,
    failed: &'static str,
    conflict: Option<&'static str>,
) -> Result<T, Refusal> {
    match r {
        Ok(v) => v,
        Err(e) if e.is_unavailable() => Err(Refusal::BadGateway("pxdb unavailable")),
        Err(WebError::Conflict(_)) if conflict.is_some() => Err(Refusal::Conflict(conflict.unwrap_or(failed))),
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "{failed}");
            Err(Refusal::BadGateway(failed))
        }
    }
}

// ===========================================================================
// time (UTC, epoch microseconds; no date crate in this tree)
// ===========================================================================

const US_PER_DAY: i64 = 86_400_000_000;

fn wall_us() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_micros() as i64).unwrap_or(0)
}

/// Wall-clock microseconds, strictly increasing within this process, so two
/// operations recorded back to back keep their order in the newest-first
/// `px.ops` keys.
pub(crate) fn now_us() -> i64 {
    static LAST: AtomicI64 = AtomicI64::new(0);
    let wall = wall_us();
    let mut prev = LAST.load(Ordering::Relaxed);
    loop {
        let next = wall.max(prev + 1);
        match LAST.compare_exchange_weak(prev, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(seen) => prev = seen,
        }
    }
}

/// (year, month, day) of a day number since 1970-01-01 (H. Hinnant).
fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let y = yoe + era * 400;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Day number since 1970-01-01 of a civil date.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let m = m as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')`.
pub(crate) fn utc_iso(us: i64) -> String {
    let secs = us.div_euclid(1_000_000);
    let (y, m, d) = civil(secs.div_euclid(86_400));
    let sod = secs.rem_euclid(86_400);
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", sod / 3_600, sod % 3_600 / 60, sod % 60)
}

/// `YYYY-MM-DD` (UTC) — the `px.usage.day` key segment.
fn utc_day(us: i64) -> String {
    let (y, m, d) = civil(us.div_euclid(US_PER_DAY));
    format!("{y:04}-{m:02}-{d:02}")
}

/// `YYYY-MM` (UTC).
fn utc_month(us: i64) -> String {
    let (y, m, _) = civil(us.div_euclid(US_PER_DAY));
    format!("{y:04}-{m:02}")
}

/// `[start, end)` of the UTC calendar month containing `us`.
fn month_bounds_us(us: i64) -> (i64, i64) {
    let (y, m, _) = civil(us.div_euclid(US_PER_DAY));
    let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
    (days_from_civil(y, m, 1) * US_PER_DAY, days_from_civil(ny, nm, 1) * US_PER_DAY)
}

// ===========================================================================
// KV plumbing
// ===========================================================================

/// What losing a `required` op means.
#[derive(Clone, Copy, Debug)]
enum Guard {
    /// A unique index entry: a conflict the caller reports.
    Unique(&'static str),
    /// A document read earlier changed (or appeared): re-read and retry.
    Fresh,
}

/// One atomic KV batch under construction.
#[derive(Default)]
struct Tx {
    ops: Vec<Value>,
    guards: Vec<(usize, Guard)>,
}

impl Tx {
    fn push(&mut self, op: Value, guard: Option<Guard>) {
        if let Some(g) = guard {
            self.guards.push((self.ops.len(), g));
        }
        self.ops.push(op);
    }

    /// Upsert, no precondition (a fresh id's row, an index entry).
    fn put(&mut self, ns: &str, key: String, value: &impl Serialize) {
        self.push(kv::put_op(ns, &key, value, Expect::Any, Ttl::Forever, false), None);
    }

    /// A UNIQUE index entry: must not exist, or the whole batch fails.
    fn unique(&mut self, ns: &str, key: String, value: &impl Serialize, what: &'static str) {
        self.push(kv::put_op(ns, &key, value, Expect::Absent, Ttl::Forever, true), Some(Guard::Unique(what)));
    }

    /// Rewrite a document read at `version` (`None`: read as absent, and it
    /// must still be absent).
    fn fresh(&mut self, ns: &str, key: String, value: &impl Serialize, version: Option<u64>) {
        let expect = version.map_or(Expect::Absent, Expect::Version);
        self.push(kv::put_op(ns, &key, value, expect, Ttl::Forever, true), Some(Guard::Fresh));
    }

    fn del(&mut self, ns: &str, key: String) {
        self.push(kv::delete_op(ns, &key, Expect::Any, false), None);
    }

    /// Delete a document read at `version`.
    fn del_fresh(&mut self, ns: &str, key: String, version: u64) {
        self.push(kv::delete_op(ns, &key, Expect::Version(version), true), Some(Guard::Fresh));
    }

    /// `queen_proxy.record_operation` (002_functions): validate, then append
    /// the `px.ops` row (+ its `px.ops.cluster` entry). Returns the row id.
    fn record(&mut self, a: &Audit<'_>) -> Result<Uuid, WebError> {
        check_audit(a)?;
        let at_us = now_us();
        let id = Uuid::new_v4();
        let doc = OperationDoc {
            id,
            tenant_id: a.tenant_id,
            cluster_id: a.cluster_id,
            actor: a.actor.to_string(),
            actor_id: a.actor_id,
            action: a.action.to_string(),
            target: a.target.clone(),
            meta: if a.meta.is_null() { json!({}) } else { a.meta.clone() },
            at_us,
        };
        self.put(ns::OPS, ops_key(a.tenant_id, at_us, id), &doc);
        if let Some(c) = a.cluster_id {
            self.put(ns::OPS_CLUSTER, ops_key(c, at_us, id), &ENTRY);
        }
        Ok(id)
    }

    /// `queen_proxy.emit_outbox` (004_lifecycle).
    fn outbox(&mut self, kind: &str, payload: Value) -> Result<Uuid, WebError> {
        let kind = kind.trim();
        if kind.is_empty() {
            return Err(raised("emit_outbox: kind must not be empty"));
        }
        let at_us = now_us();
        let id = Uuid::new_v4();
        let doc = OutboxDoc {
            id,
            kind: kind.to_string(),
            payload: if payload.is_null() { json!({}) } else { payload },
            created_at_us: at_us,
            consumed_at_us: None,
        };
        self.put(ns::OUTBOX, schema::key2(schema::ordered(at_us), id), &doc);
        Ok(id)
    }
}

/// Commit one batch; a lost unique index is `Conflict(what)`, a lost version
/// `Contended`.
async fn commit(kv: &dyn KvBackend, tx: Tx) -> Result<(), WebError> {
    if tx.ops.is_empty() {
        return Ok(());
    }
    let Tx { ops, guards } = tx;
    match kv::write(kv, ops).await {
        Ok(_) => Ok(()),
        Err(e) => {
            let guard = e.precondition_index().and_then(|i| guards.iter().find(|(j, _)| *j == i).map(|(_, g)| *g));
            Err(match guard {
                Some(Guard::Unique(what)) => WebError::Conflict(what.to_string()),
                _ => WebError::from(e),
            })
        }
    }
}

/// Non-atomic bulk delete (a tenant wipe), in wire-sized chunks.
async fn delete_all(kv: &dyn KvBackend, keys: Vec<(&'static str, String)>) -> Result<(), WebError> {
    for chunk in keys.chunks(DELETE_CHUNK) {
        let ops = chunk.iter().map(|(n, k)| kv::delete_op(n, k, Expect::Any, false)).collect();
        kv::write(kv, ops).await?;
    }
    Ok(())
}

/// `#<owner>/<inv_at_us>/<id>` — newest first under `#<owner>/`.
fn ops_key(owner: Uuid, at_us: i64, id: Uuid) -> String {
    schema::key2(owner, format!("{}/{}", schema::inverted(at_us), id))
}

fn identity_key(provider: &str, provider_id: &str) -> String {
    schema::key(format!("{provider}:{provider_id}"))
}

async fn doc_at<T: DeserializeOwned>(kv: &dyn KvBackend, ns: &str, key: &str) -> Result<Option<Doc<T>>, WebError> {
    Ok(kv::get(kv, ns, key).await?)
}

async fn by_id<T: DeserializeOwned>(kv: &dyn KvBackend, ns: &str, id: Uuid) -> Result<Option<Doc<T>>, WebError> {
    doc_at(kv, ns, &schema::key(id)).await
}

/// The row id a unique index entry names.
async fn index_target(kv: &dyn KvBackend, ns: &str, key: &str) -> Result<Option<Uuid>, WebError> {
    Ok(doc_at::<Uuid>(kv, ns, key).await?.map(|d| d.value))
}

/// The ids listed under a "rows of X" index (`#<parent>/<id>`).
async fn children(kv: &dyn KvBackend, ns: &str, parent: Uuid) -> Result<Vec<Uuid>, WebError> {
    Ok(kv::scan_keys(kv, ns, &schema::prefix(parent))
        .await?
        .iter()
        .filter_map(|k| Uuid::parse_str(schema::tail(k)).ok())
        .collect())
}

/// The documents of `ids` that exist (a dangling index entry reads as absent,
/// as an inner join would drop it).
async fn many<T: DeserializeOwned>(kv: &dyn KvBackend, ns: &str, ids: &[Uuid]) -> Result<Vec<Doc<T>>, WebError> {
    let keys: Vec<String> = ids.iter().map(schema::key).collect();
    Ok(kv::get_many::<T>(kv, ns, &keys).await?.into_iter().flatten().collect())
}

/// One `getPrefix` page (newest-first listings): at most `limit` rows.
async fn page<T: DeserializeOwned>(
    kv: &dyn KvBackend,
    ns: &str,
    prefix: &str,
    limit: usize,
) -> Result<Vec<(String, Doc<T>)>, WebError> {
    let res = kv.kv(vec![json!({"op":"getPrefix","ns":ns,"prefix":prefix,"limit":limit.clamp(1, 1000)})]).await?;
    let rows = res.first().and_then(|r| r.get("rows")).and_then(Value::as_array).cloned().unwrap_or_default();
    Ok(rows
        .iter()
        .filter_map(|row| {
            let k = row.get("key")?.as_str()?.to_string();
            let value = serde_json::from_value::<T>(row.get("value")?.clone()).ok()?;
            let version = row.get("version").and_then(Value::as_u64).unwrap_or(0);
            Some((k, Doc { value, version }))
        })
        .collect())
}

/// `kv::scan`, starting after `after` (a key) instead of at the prefix start.
async fn scan_from<T: DeserializeOwned>(
    kv: &dyn KvBackend,
    ns: &str,
    prefix: &str,
    mut after: Option<String>,
) -> Result<Vec<(String, Doc<T>)>, WebError> {
    let mut out = Vec::new();
    loop {
        let mut op = json!({"op":"getPrefix","ns":ns,"prefix":prefix,"limit":1000});
        if let (Some(a), Some(m)) = (&after, op.as_object_mut()) {
            m.insert("after".into(), Value::String(a.clone()));
        }
        let res = kv.kv(vec![op]).await?;
        let Some(r) = res.into_iter().next() else {
            break;
        };
        let rows = r.get("rows").and_then(Value::as_array).cloned().unwrap_or_default();
        let mut last = None;
        for row in &rows {
            let Some(k) = row.get("key").and_then(Value::as_str) else {
                continue;
            };
            last = Some(k.to_string());
            if let Some(v) = row.get("value").and_then(|v| serde_json::from_value::<T>(v.clone()).ok()) {
                let version = row.get("version").and_then(Value::as_u64).unwrap_or(0);
                out.push((k.to_string(), Doc { value: v, version }));
            }
        }
        let truncated = r.get("truncated").and_then(Value::as_bool).unwrap_or(false);
        let next = r.get("nextAfter").and_then(Value::as_str).map(str::to_string).or(last);
        if !truncated || rows.is_empty() || next.is_none() {
            break;
        }
        after = next;
    }
    Ok(out)
}

/// The first key under `prefix`, if any.
async fn first_key(kv: &dyn KvBackend, ns: &str, prefix: &str) -> Result<Option<String>, WebError> {
    let res = kv.kv(vec![json!({"op":"getPrefix","ns":ns,"prefix":prefix,"limit":1,"keysOnly":true})]).await?;
    Ok(res
        .first()
        .and_then(|r| r.get("rows"))
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("key"))
        .and_then(Value::as_str)
        .map(str::to_string))
}

fn check_audit(a: &Audit<'_>) -> Result<(), WebError> {
    if !ACTORS.contains(&a.actor) {
        return Err(raised(format!("record_operation: invalid actor {}", a.actor)));
    }
    if a.action.trim().is_empty() {
        return Err(raised("record_operation: action must not be empty"));
    }
    Ok(())
}

/// `^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$` — the tenants/clusters slug CHECK.
fn dns_label(s: &str) -> bool {
    let b = s.as_bytes();
    let alnum = |c: u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    !b.is_empty() && b.len() <= 63 && alnum(b[0]) && alnum(b[b.len() - 1]) && b.iter().all(|&c| alnum(c) || c == b'-')
}

fn all_scopes() -> Vec<String> {
    SCOPES.iter().map(|s| s.to_string()).collect()
}

// ===========================================================================
// shared shapes
// ===========================================================================

/// A resolved local user and its owning tenant (session mint + audit).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserRef {
    pub user_id: Uuid,
    pub tenant_id: Uuid,
}

fn row_to_userref(row: &tokio_postgres::Row) -> Option<UserRef> {
    let user_id: Uuid = row.get::<_, String>(0).parse().ok()?;
    let tenant_id: Uuid = row.get::<_, String>(1).parse().ok()?;
    Some(UserRef { user_id, tenant_id })
}

/// One `queen_proxy.operations` row to append (`record_operation`'s
/// arguments). `meta` Null is stored as `{}` (the function's COALESCE).
#[derive(Clone, Debug)]
pub struct Audit<'a> {
    pub tenant_id: Uuid,
    pub cluster_id: Option<Uuid>,
    /// user | api_key | control_plane | system
    pub actor: &'a str,
    pub actor_id: Option<Uuid>,
    pub action: &'a str,
    pub target: Option<String>,
    pub meta: Value,
}

// ===========================================================================
// operations (audit)
// ===========================================================================

/// `queen_proxy.record_operation(...)`: append one audit row (and, on
/// Postgres, the invalidation NOTIFY when it names a cluster).
pub async fn record_operation(store: &Store, a: &Audit<'_>) -> Result<(), WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let meta = a.meta.to_string();
            // `::text::uuid` / `::text::jsonb`: the crate has no uuid/jsonb
            // tokio-postgres feature, so the binds are text (oauth.rs'
            // original pattern). A None cluster/actor binds SQL NULL, which
            // is what the literal NULL in the old per-caller statements was.
            client
                .execute(
                    "SELECT queen_proxy.record_operation($1::text::uuid, $2::text::uuid, $3, $4::text::uuid, $5, $6, $7::text::jsonb)",
                    &[
                        &a.tenant_id.to_string(),
                        &a.cluster_id.map(|c| c.to_string()),
                        &a.actor,
                        &a.actor_id.map(|u| u.to_string()),
                        &a.action,
                        &a.target,
                        &meta,
                    ],
                )
                .await
                .map_err(pg_err)?;
            Ok(())
        }
        Store::Kv(kv) => {
            let mut tx = Tx::default();
            tx.record(a)?;
            commit(kv.as_ref(), tx).await
        }
        Store::None => Err(WebError::NotConfigured),
    }
}

/// One audit row, as the operator page shows it.
#[derive(Clone, Debug, PartialEq)]
pub struct OperationRow {
    pub id: String,
    pub cluster_id: Option<String>,
    pub actor: String,
    pub actor_id: Option<String>,
    pub action: String,
    pub target: Option<String>,
    pub meta: Value,
    pub at: String,
}

const LIST_OPERATIONS_SQL: &str = "
    SELECT id::text, cluster_id::text, actor, actor_id::text, action, target, meta::text,
           to_char(at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
    FROM queen_proxy.operations
    WHERE tenant_id = $1::text::uuid
      AND ($2::text IS NULL OR cluster_id = $2::text::uuid)
    ORDER BY at DESC
    LIMIT $3";

/// A tenant's audit trail, NEWEST FIRST, optionally one cluster's only.
/// `limit` is clamped to 1..=1000.
pub async fn list_operations(
    store: &Store,
    tenant_id: Uuid,
    cluster_id: Option<Uuid>,
    limit: usize,
) -> Result<Vec<OperationRow>, WebError> {
    let limit = limit.clamp(1, 1000);
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let rows = client
                .query(
                    LIST_OPERATIONS_SQL,
                    &[&tenant_id.to_string(), &cluster_id.map(|c| c.to_string()), &(limit as i64)],
                )
                .await
                .map_err(pg_err)?;
            Ok(rows
                .iter()
                .map(|r| OperationRow {
                    id: r.get(0),
                    cluster_id: r.get(1),
                    actor: r.get(2),
                    actor_id: r.get(3),
                    action: r.get(4),
                    target: r.get(5),
                    meta: serde_json::from_str(&r.get::<_, String>(6)).unwrap_or(Value::Null),
                    at: r.get(7),
                })
                .collect())
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            // The key IS the order: `#<owner>/<i64::MAX - at_us>/<id>`.
            let docs: Vec<OperationDoc> = match cluster_id {
                None => page::<OperationDoc>(kv, ns::OPS, &schema::prefix(tenant_id), limit)
                    .await?
                    .into_iter()
                    .map(|(_, d)| d.value)
                    .collect(),
                Some(c) => {
                    let own = schema::prefix(c);
                    let keys: Vec<String> = page::<Value>(kv, ns::OPS_CLUSTER, &own, limit)
                        .await?
                        .into_iter()
                        .filter_map(|(k, _)| k.strip_prefix(&own).map(|rest| schema::key2(tenant_id, rest)))
                        .collect();
                    kv::get_many::<OperationDoc>(kv, ns::OPS, &keys)
                        .await?
                        .into_iter()
                        .flatten()
                        .map(|d| d.value)
                        .collect()
                }
            };
            Ok(docs
                .into_iter()
                .map(|o| OperationRow {
                    id: o.id.to_string(),
                    cluster_id: o.cluster_id.map(|c| c.to_string()),
                    actor: o.actor,
                    actor_id: o.actor_id.map(|u| u.to_string()),
                    action: o.action,
                    target: o.target,
                    meta: o.meta,
                    at: utc_iso(o.at_us),
                })
                .collect())
        }
        Store::None => Err(WebError::NotConfigured),
    }
}

// ===========================================================================
// oauth.rs: local login, sessions, /auth/me, identity resolution
// ===========================================================================

/// The login row for a local password check: who, and the bcrypt hash
/// (`None` for an OAuth-only account). `email` is lowercased, not trimmed —
/// `WHERE email = lower($1)`.
pub async fn user_login_by_email(store: &Store, email: &str) -> Result<Option<(UserRef, Option<String>)>, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let row = client
                .query_opt(
                    "SELECT id::text, tenant_id::text, password_hash \
                     FROM queen_proxy.users WHERE email = lower($1)",
                    &[&email],
                )
                .await
                .map_err(pg_err)?;
            Ok(row.and_then(|r| row_to_userref(&r).map(|u| (u, r.get::<_, Option<String>>(2)))))
        }
        Store::Kv(kv) => Ok(user_by_email(kv.as_ref(), &email.to_lowercase())
            .await?
            .map(|u| (UserRef { user_id: u.value.id, tenant_id: u.value.tenant_id }, u.value.password_hash))),
        Store::None => Err(WebError::NotConfigured),
    }
}

/// `queen_proxy.record_user_login(user)` (011): stamp `last_login_at` and
/// append the `login` audit row, atomically.
pub async fn record_user_login(store: &Store, user_id: Uuid) -> Result<(), WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            client
                .execute("SELECT queen_proxy.record_user_login($1::text::uuid)", &[&user_id.to_string()])
                .await
                .map_err(pg_err)?;
            Ok(())
        }
        Store::Kv(kv) => retrying!(kv_record_login_once(kv.as_ref(), user_id)),
        Store::None => Err(WebError::NotConfigured),
    }
}

async fn kv_record_login_once(kv: &dyn KvBackend, user_id: Uuid) -> Result<(), WebError> {
    let Some(u) = by_id::<UserDoc>(kv, ns::USERS, user_id).await? else {
        return Err(raised(format!("record_user_login: unknown user {user_id}")));
    };
    let mut doc = u.value.clone();
    doc.last_login_at_us = Some(now_us());
    let mut tx = Tx::default();
    tx.fresh(ns::USERS, schema::key(user_id), &doc, Some(u.version));
    tx.record(&Audit {
        tenant_id: doc.tenant_id,
        cluster_id: None,
        actor: "user",
        actor_id: Some(user_id),
        action: "login",
        target: Some(user_id.to_string()),
        meta: json!({}),
    })?;
    commit(kv, tx).await
}

/// `queen_proxy.revoke_session(jti, exp, 'user', user)` (004): deny-list a
/// session until its own expiry, and audit it. A second revocation of the
/// same jti is a no-op (`ON CONFLICT DO NOTHING`), not an error.
///
/// KV: `px.revoked #<jti>` → [`RevokedDoc`], TTL = the token's remaining life
/// (the row dies with the token; the data plane's `is_revoked` reads it).
pub async fn revoke_session(store: &Store, jti: &str, exp_secs: i64, user_id: Uuid) -> Result<(), WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            // revoke_session(p_jti TEXT, p_expires_at TIMESTAMPTZ, p_actor TEXT,
            // p_actor_id UUID) — exp is the token's own, so the sweep can drop
            // the row once it expires.
            client
                .execute(
                    "SELECT queen_proxy.revoke_session($1, to_timestamp($2), 'user', $3::text::uuid)",
                    &[&jti, &(exp_secs as f64), &user_id.to_string()],
                )
                .await
                .map_err(pg_err)?;
            Ok(())
        }
        // data.rs's write also moves the replicated revocation epoch, so the
        // logout holds on every node within a poll, not a cache TTL.
        Store::Kv(_) => crate::store::data::revoke_session(store, jti, exp_secs, "user", user_id)
            .await
            .map_err(WebError::from),
        Store::None => Err(WebError::NotConfigured),
    }
}

/// `/auth/me`'s own row. `is_operator` is the STORED bit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MeUser {
    pub email: String,
    pub is_operator: bool,
    pub tenant_slug: String,
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

/// The session's user row joined to its tenant; `None` when the user (or its
/// tenant) is gone — the session outlived its owner.
pub async fn me_user(store: &Store, user_id: Uuid) -> Result<Option<MeUser>, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let row = client.query_opt(ME_USER_SQL, &[&user_id.to_string()]).await.map_err(pg_err)?;
            Ok(row.map(|r| MeUser { email: r.get(0), is_operator: r.get(1), tenant_slug: r.get(2) }))
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let Some(u) = by_id::<UserDoc>(kv, ns::USERS, user_id).await? else {
                return Ok(None);
            };
            let Some(t) = by_id::<TenantDoc>(kv, ns::TENANTS, u.value.tenant_id).await? else {
                return Ok(None);
            };
            Ok(Some(MeUser { email: u.value.email, is_operator: u.value.is_operator, tenant_slug: t.value.slug }))
        }
        Store::None => Err(WebError::NotConfigured),
    }
}

/// One entry of `/auth/me`'s cluster selector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MeCluster {
    pub id: String,
    pub slug: String,
    pub role: String,
    pub tenant_slug: String,
    pub tenant_id: String,
    pub status: String,
    pub cell_slug: String,
}

/// The clusters a session may select, ordered by tenant slug then cluster
/// slug: its memberships, or — for a LIVE operator — every cluster, as admin.
pub async fn me_clusters(store: &Store, user_id: Uuid, operator: bool) -> Result<Vec<MeCluster>, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let uid = user_id.to_string();
            let (sql, params): (&str, Vec<&(dyn tokio_postgres::types::ToSql + Sync)>) =
                if operator { (ME_CLUSTERS_OPERATOR_SQL, vec![]) } else { (ME_CLUSTERS_SQL, vec![&uid]) };
            let rows = client.query(sql, &params).await.map_err(pg_err)?;
            Ok(rows
                .iter()
                .map(|r| MeCluster {
                    id: r.get(0),
                    slug: r.get(1),
                    role: r.get(2),
                    tenant_slug: r.get(3),
                    tenant_id: r.get(4),
                    status: r.get(5),
                    cell_slug: r.get(6),
                })
                .collect())
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let pairs: Vec<(ClusterDoc, String)> = if operator {
                kv::scan::<ClusterDoc>(kv, ns::CLUSTERS, schema::K)
                    .await?
                    .into_iter()
                    .map(|(_, d)| (d.value, "admin".to_string()))
                    .collect()
            } else {
                let roles: Vec<RoleDoc> = kv::scan::<RoleDoc>(kv, ns::ROLES, &schema::prefix(user_id))
                    .await?
                    .into_iter()
                    .map(|(_, d)| d.value)
                    .collect();
                let ids: Vec<Uuid> = roles.iter().map(|r| r.cluster_id).collect();
                let clusters: HashMap<Uuid, ClusterDoc> = many::<ClusterDoc>(kv, ns::CLUSTERS, &ids)
                    .await?
                    .into_iter()
                    .map(|d| (d.value.id, d.value))
                    .collect();
                roles.into_iter().filter_map(|r| clusters.get(&r.cluster_id).map(|c| (c.clone(), r.role))).collect()
            };
            let (tenants, cells) = tenants_and_cells(kv, pairs.iter().map(|(c, _)| c)).await?;
            let mut out: Vec<MeCluster> = pairs
                .into_iter()
                .filter_map(|(c, role)| {
                    let t = tenants.get(&c.tenant_id)?;
                    let ce = cells.get(&c.cell_id)?;
                    Some(MeCluster {
                        id: c.id.to_string(),
                        slug: c.slug,
                        role,
                        tenant_slug: t.slug.clone(),
                        tenant_id: t.id.to_string(),
                        status: c.status,
                        cell_slug: ce.slug.clone(),
                    })
                })
                .collect();
            out.sort_by(|a, b| (&a.tenant_slug, &a.slug).cmp(&(&b.tenant_slug, &b.slug)));
            Ok(out)
        }
        Store::None => Err(WebError::NotConfigured),
    }
}

/// The user a linked `(provider, provider_id)` identity logs in as.
pub async fn find_user_by_identity(
    store: &Store,
    provider: &str,
    provider_id: &str,
) -> Result<Option<UserRef>, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let row = client
                .query_opt(
                    "SELECT u.id::text, u.tenant_id::text \
                     FROM queen_proxy.identities i \
                     JOIN queen_proxy.users u ON u.id = i.user_id \
                     WHERE i.provider = $1 AND i.provider_id = $2",
                    &[&provider, &provider_id],
                )
                .await
                .map_err(pg_err)?;
            Ok(row.as_ref().and_then(row_to_userref))
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let Some(iid) = index_target(kv, ns::IDENTITY_PROVIDER, &identity_key(provider, provider_id)).await? else {
                return Ok(None);
            };
            let Some(i) = by_id::<IdentityDoc>(kv, ns::IDENTITIES, iid).await? else {
                return Ok(None);
            };
            Ok(by_id::<UserDoc>(kv, ns::USERS, i.value.user_id)
                .await?
                .map(|u| UserRef { user_id: u.value.id, tenant_id: u.value.tenant_id }))
        }
        Store::None => Err(WebError::NotConfigured),
    }
}

/// The user an email names (`WHERE email = lower($1)`).
pub async fn find_user_by_email(store: &Store, email: &str) -> Result<Option<UserRef>, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let row = client
                .query_opt("SELECT id::text, tenant_id::text FROM queen_proxy.users WHERE email = lower($1)", &[&email])
                .await
                .map_err(pg_err)?;
            Ok(row.as_ref().and_then(row_to_userref))
        }
        Store::Kv(kv) => Ok(user_by_email(kv.as_ref(), &email.to_lowercase())
            .await?
            .map(|u| UserRef { user_id: u.value.id, tenant_id: u.value.tenant_id })),
        Store::None => Err(WebError::NotConfigured),
    }
}

/// Link a verified provider identity to an existing user. First link wins:
/// an identity already linked (to anyone) is left alone (`ON CONFLICT
/// (provider, provider_id) DO NOTHING`).
pub async fn link_identity(
    store: &Store,
    user: &UserRef,
    provider: &str,
    provider_id: &str,
    email: &str,
) -> Result<(), WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            client
                .execute(
                    "INSERT INTO queen_proxy.identities(user_id, provider, provider_id, email, verified) \
                     VALUES ($1::text::uuid, $2, $3, lower($4), true) \
                     ON CONFLICT (provider, provider_id) DO NOTHING",
                    &[&user.user_id.to_string(), &provider, &provider_id, &email],
                )
                .await
                .map_err(pg_err)?;
            Ok(())
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            if !PROVIDERS.contains(&provider) {
                return Err(WebError::Db(format!(
                    "new row for relation \"identities\" violates check constraint (provider {provider})"
                )));
            }
            // The foreign key: identities.user_id must name a user.
            if by_id::<UserDoc>(kv, ns::USERS, user.user_id).await?.is_none() {
                return Err(WebError::Db(format!(
                    "insert on \"identities\" violates foreign key: no user {}",
                    user.user_id
                )));
            }
            let mut tx = Tx::default();
            plan_identity(&mut tx, user.user_id, provider, provider_id, email);
            match commit(kv, tx).await {
                Err(WebError::Conflict(_)) => Ok(()),
                other => other,
            }
        }
        Store::None => Err(WebError::NotConfigured),
    }
}

/// An `identities` row + its two index entries (verified, email lowercased).
fn plan_identity(tx: &mut Tx, user_id: Uuid, provider: &str, provider_id: &str, email: &str) -> Uuid {
    let id = Uuid::new_v4();
    let doc = IdentityDoc {
        id,
        user_id,
        provider: provider.to_string(),
        provider_id: provider_id.to_string(),
        email: email.to_lowercase(),
        verified: true,
        created_at_us: now_us(),
    };
    tx.put(ns::IDENTITIES, schema::key(id), &doc);
    tx.unique(ns::IDENTITY_PROVIDER, identity_key(provider, provider_id), &id, "identities (provider, provider_id)");
    tx.put(ns::IDENTITY_USER, schema::key2(user_id, id), &ENTRY);
    id
}

/// Why auto-provisioning failed (oauth.rs maps these to its answers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProvisionError {
    /// `QUEEN_PROXY_AUTOPROVISION_TENANT` names no tenant.
    NoTenant,
    Db(String),
}

/// A freshly provisioned OAuth user, and how many clusters it was granted
/// the default role on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Provisioned {
    pub user: UserRef,
    pub granted: u64,
}

/// Auto-provision a brand-new OAuth user in the tenant named by `tenant_slug`:
/// the user, its identity and the default role on EVERY cluster of that
/// tenant, all or nothing. The auth host is the sanctioned direct writer of
/// users/identities (PLAN §2), so this writes no `user_created` row — the
/// caller records one `signup`.
pub async fn provision_oauth_user(
    store: &Store,
    tenant_slug: &str,
    email: &str,
    provider: &str,
    provider_id: &str,
    default_role: &str,
) -> Result<Provisioned, ProvisionError> {
    match store {
        Store::Pg(pool) => pg_provision(pool, tenant_slug, email, provider, provider_id, default_role).await,
        Store::Kv(kv) => kv_provision(kv.as_ref(), tenant_slug, email, provider, provider_id, default_role).await,
        Store::None => Err(ProvisionError::Db(WebError::NotConfigured.to_string())),
    }
}

async fn pg_provision(
    pool: &deadpool_postgres::Pool,
    tenant_slug: &str,
    email: &str,
    provider: &str,
    provider_id: &str,
    default_role: &str,
) -> Result<Provisioned, ProvisionError> {
    let mut client = pool.get().await.map_err(|e| ProvisionError::Db(e.to_string()))?;
    let tx = client.transaction().await.map_err(|e| ProvisionError::Db(e.to_string()))?;

    let trow = tx
        .query_opt("SELECT id::text FROM queen_proxy.tenants WHERE slug = $1", &[&tenant_slug])
        .await
        .map_err(|e| ProvisionError::Db(e.to_string()))?;
    let Some(trow) = trow else {
        return Err(ProvisionError::NoTenant);
    };
    let tenant_id: Uuid =
        trow.get::<_, String>(0).parse().map_err(|_| ProvisionError::Db("tenant id parse".to_string()))?;

    let urow = tx
        .query_one(
            "INSERT INTO queen_proxy.users(tenant_id, email) \
             VALUES ($1::text::uuid, lower($2)) RETURNING id::text",
            &[&tenant_id.to_string(), &email],
        )
        .await
        .map_err(|e| ProvisionError::Db(e.to_string()))?;
    let user_id: Uuid =
        urow.get::<_, String>(0).parse().map_err(|_| ProvisionError::Db("user id parse".to_string()))?;

    tx.execute(
        "INSERT INTO queen_proxy.identities(user_id, provider, provider_id, email, verified) \
         VALUES ($1::text::uuid, $2, $3, lower($4), true)",
        &[&user_id.to_string(), &provider, &provider_id, &email],
    )
    .await
    .map_err(|e| ProvisionError::Db(e.to_string()))?;

    // Grant the default role on every cluster of the auto-provision tenant,
    // in the same transaction as the user and identity rows (a
    // half-provisioned human is exactly the state that is confusing to
    // diagnose). `role` is validated at boot (config::CLUSTER_ROLES).
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
        .map_err(|e| ProvisionError::Db(e.to_string()))?;

    tx.commit().await.map_err(|e| ProvisionError::Db(e.to_string()))?;
    Ok(Provisioned { user: UserRef { user_id, tenant_id }, granted })
}

async fn kv_provision(
    kv: &dyn KvBackend,
    tenant_slug: &str,
    email: &str,
    provider: &str,
    provider_id: &str,
    default_role: &str,
) -> Result<Provisioned, ProvisionError> {
    let db = |e: WebError| ProvisionError::Db(e.to_string());
    let Some(t) = tenant_by_slug(kv, tenant_slug).await.map_err(db)? else {
        return Err(ProvisionError::NoTenant);
    };
    let tenant_id = t.value.id;
    let clusters = children(kv, ns::CLUSTER_TENANT, tenant_id).await.map_err(db)?;
    let now = now_us();
    let user_id = Uuid::new_v4();
    let email = email.to_lowercase();
    let doc = UserDoc {
        id: user_id,
        tenant_id,
        email: email.clone(),
        password_hash: None,
        name: None,
        is_operator: false,
        last_login_at_us: None,
        created_at_us: now,
    };
    let mut tx = Tx::default();
    tx.put(ns::USERS, schema::key(user_id), &doc);
    tx.unique(ns::USER_EMAIL, schema::key(&email), &user_id, "users_email_key");
    tx.put(ns::USER_TENANT, schema::key2(tenant_id, user_id), &ENTRY);
    plan_identity(&mut tx, user_id, provider, provider_id, &email);
    for c in &clusters {
        let role = RoleDoc { user_id, cluster_id: *c, role: default_role.to_string(), created_at_us: now };
        tx.put(ns::ROLES, schema::key2(user_id, c), &role);
        tx.put(ns::ROLE_CLUSTER, schema::key2(c, user_id), &ENTRY);
    }
    commit(kv, tx).await.map_err(|e| match e {
        WebError::Conflict(what) => {
            ProvisionError::Db(format!("duplicate key value violates unique constraint ({what})"))
        }
        other => db(other),
    })?;
    Ok(Provisioned { user: UserRef { user_id, tenant_id }, granted: clusters.len() as u64 })
}

// ===========================================================================
// console.rs: overview, usage, keys, members
// ===========================================================================

/// The plan identity + monthly counter the cached `ClusterCtx` does not carry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanUsage {
    pub code: String,
    pub monthly_msgs_quota: Option<i64>,
    /// `YYYY-MM`, UTC.
    pub month: String,
    pub msgs: i64,
}

/// `cluster_month_msgs` (004_lifecycle.sql) reads usage_days plus the
/// not-yet-rolled-up usage_minutes remainder, so month-to-date never
/// under-counts today.
const PLAN_USAGE_SQL: &str = "
    SELECT p.code,
           p.monthly_msgs_quota,
           queen_proxy.cluster_month_msgs(c.id, (now() AT TIME ZONE 'UTC')::date),
           to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM')
    FROM queen_proxy.clusters c
    JOIN queen_proxy.plans   p ON p.id = c.plan_id
    WHERE c.id = $1::text::uuid";

/// `None` when the cluster (or its plan) row is gone.
pub async fn cluster_plan_usage(store: &Store, cluster_id: Uuid) -> Result<Option<PlanUsage>, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let row = client.query_opt(PLAN_USAGE_SQL, &[&cluster_id.to_string()]).await.map_err(pg_err)?;
            Ok(row.map(|r| PlanUsage { code: r.get(0), monthly_msgs_quota: r.get(1), msgs: r.get(2), month: r.get(3) }))
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let Some(c) = by_id::<ClusterDoc>(kv, ns::CLUSTERS, cluster_id).await? else {
                return Ok(None);
            };
            let Some(p) = by_id::<PlanDoc>(kv, ns::PLANS, c.value.plan_id).await? else {
                return Ok(None);
            };
            let now = wall_us();
            let msgs = cluster_month_msgs(kv, cluster_id, now).await?;
            Ok(Some(PlanUsage {
                code: p.value.code,
                monthly_msgs_quota: p.value.monthly_msgs_quota,
                month: utc_month(now),
                msgs,
            }))
        }
        Store::None => Err(WebError::NotConfigured),
    }
}

/// One `/api/console/usage` row: a minute and an op class, summed over nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageMinute {
    /// `YYYY-MM-DDTHH:MM:SSZ`
    pub minute: String,
    pub op: String,
    pub reqs: i64,
    pub msgs: i64,
    pub bytes_in: i64,
    pub bytes_out: i64,
}

const USAGE_SQL: &str = "
    SELECT to_char(minute AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS minute,
           op_class, reqs, msgs, bytes_in, bytes_out
    FROM queen_proxy.usage_minutes
    WHERE cluster_id = $1::text::uuid
      AND minute >= now() - make_interval(hours => $2::int)
    ORDER BY minute ASC, op_class ASC";

/// The last `hours` of a cluster's minute usage, oldest first. KV: the
/// metering writes one `px.usage.min` row per NODE; they are summed here.
pub async fn usage_minutes(store: &Store, cluster_id: Uuid, hours: i32) -> Result<Vec<UsageMinute>, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let rows = client.query(USAGE_SQL, &[&cluster_id.to_string(), &hours]).await.map_err(pg_err)?;
            Ok(rows
                .iter()
                .map(|r| UsageMinute {
                    minute: r.get(0),
                    op: r.get(1),
                    reqs: r.get(2),
                    msgs: r.get(3),
                    bytes_in: r.get(4),
                    bytes_out: r.get(5),
                })
                .collect())
        }
        Store::Kv(kv) => {
            let since = wall_us() - i64::from(hours) * 3_600_000_000;
            let mut summed: BTreeMap<(i64, String), UsageDoc> = BTreeMap::new();
            for (minute, op, d) in usage_minute_rows(kv.as_ref(), cluster_id, since, i64::MAX).await? {
                let s = summed.entry((minute, op)).or_default();
                s.msgs += d.msgs;
                s.reqs += d.reqs;
                s.bytes_in += d.bytes_in;
                s.bytes_out += d.bytes_out;
            }
            Ok(summed
                .into_iter()
                .map(|((minute, op), s)| UsageMinute {
                    minute: utc_iso(minute),
                    op,
                    reqs: s.reqs,
                    msgs: s.msgs,
                    bytes_in: s.bytes_in,
                    bytes_out: s.bytes_out,
                })
                .collect())
        }
        Store::None => Err(WebError::NotConfigured),
    }
}

/// One console `/keys` row. Never the hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRow {
    pub id: String,
    pub name: String,
    pub scopes: Vec<String>,
    pub created_at: String,
    pub last_used_at: Option<String>,
    pub revoked_at: Option<String>,
}

const LIST_KEYS_SQL: &str = "
    SELECT id::text, name, scopes,
           to_char(created_at   AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
           to_char(last_used_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
           to_char(revoked_at   AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
    FROM queen_proxy.api_keys
    WHERE cluster_id = $1::text::uuid
    ORDER BY created_at DESC";

/// A cluster's API keys, newest first (revoked ones included).
pub async fn list_cluster_keys(store: &Store, cluster_id: Uuid) -> Result<Vec<KeyRow>, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let rows = client.query(LIST_KEYS_SQL, &[&cluster_id.to_string()]).await.map_err(pg_err)?;
            // MAI hash: key_hash is never selected above, let alone returned.
            Ok(rows
                .iter()
                .map(|r| KeyRow {
                    id: r.get(0),
                    name: r.get(1),
                    scopes: r.get(2),
                    created_at: r.get(3),
                    last_used_at: r.get(4),
                    revoked_at: r.get(5),
                })
                .collect())
        }
        Store::Kv(kv) => {
            let mut docs: Vec<ApiKeyDoc> =
                cluster_key_docs(kv.as_ref(), cluster_id).await?.into_iter().map(|d| d.value).collect();
            docs.sort_by(|a, b| b.created_at_us.cmp(&a.created_at_us));
            Ok(docs
                .into_iter()
                .map(|k| KeyRow {
                    id: k.id.to_string(),
                    name: k.name,
                    scopes: k.scopes,
                    created_at: utc_iso(k.created_at_us),
                    last_used_at: k.last_used_at_us.map(utc_iso),
                    revoked_at: k.revoked_at_us.map(utc_iso),
                })
                .collect())
        }
        Store::None => Err(WebError::NotConfigured),
    }
}

/// One console `/members` row. Never the password hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberRow {
    pub user_id: String,
    pub email: String,
    pub role: String,
    pub granted_at: String,
}

/// Password hashes are on `queen_proxy.users` — never selected here, never
/// returned. Same discipline as `LIST_KEYS_SQL` and `key_hash`.
const LIST_MEMBERS_SQL: &str = "
    SELECT u.id::text, u.email, cr.role,
           to_char(cr.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
    FROM queen_proxy.cluster_roles cr
    JOIN queen_proxy.users u ON u.id = cr.user_id
    WHERE cr.cluster_id = $1::text::uuid
    ORDER BY u.email ASC";

/// A cluster's members, by email.
pub async fn list_cluster_members(store: &Store, cluster_id: Uuid) -> Result<Vec<MemberRow>, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let rows = client.query(LIST_MEMBERS_SQL, &[&cluster_id.to_string()]).await.map_err(pg_err)?;
            Ok(rows
                .iter()
                .map(|r| MemberRow { user_id: r.get(0), email: r.get(1), role: r.get(2), granted_at: r.get(3) })
                .collect())
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let roles = cluster_member_roles(kv, cluster_id).await?;
            let ids: Vec<Uuid> = roles.iter().map(|r| r.value.user_id).collect();
            let users: HashMap<Uuid, UserDoc> =
                many::<UserDoc>(kv, ns::USERS, &ids).await?.into_iter().map(|d| (d.value.id, d.value)).collect();
            let mut out: Vec<MemberRow> = roles
                .into_iter()
                .filter_map(|r| {
                    let u = users.get(&r.value.user_id)?;
                    Some(MemberRow {
                        user_id: u.id.to_string(),
                        email: u.email.clone(),
                        role: r.value.role,
                        granted_at: utc_iso(r.value.created_at_us),
                    })
                })
                .collect();
            out.sort_by(|a, b| a.email.cmp(&b.email));
            Ok(out)
        }
        Store::None => Err(WebError::NotConfigured),
    }
}

/// Target user, resolved inside THIS cluster's tenant: `None` whether the
/// address is unknown or another tenant's (the console must not be a probe).
const MEMBER_CANDIDATE_SQL: &str = "
    SELECT id::text FROM queen_proxy.users WHERE email = $1 AND tenant_id = $2::text::uuid";

pub async fn user_id_in_tenant(store: &Store, email: &str, tenant_id: Uuid) -> Result<Option<String>, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let row =
                client.query_opt(MEMBER_CANDIDATE_SQL, &[&email, &tenant_id.to_string()]).await.map_err(pg_err)?;
            Ok(row.map(|r| r.get::<_, String>(0)))
        }
        Store::Kv(kv) => Ok(user_by_email(kv.as_ref(), email)
            .await?
            .filter(|u| u.value.tenant_id == tenant_id)
            .map(|u| u.value.id.to_string())),
        Store::None => Err(WebError::NotConfigured),
    }
}

/// Current role of one user on this cluster (NULL if none) + how many admins
/// the cluster has, for the last-admin guard.
const MEMBER_STANDING_SQL: &str = "
    SELECT (SELECT role FROM queen_proxy.cluster_roles
             WHERE cluster_id = $1::text::uuid AND user_id = $2::text::uuid),
           (SELECT count(*) FROM queen_proxy.cluster_roles
             WHERE cluster_id = $1::text::uuid AND role = 'admin')";

pub async fn member_standing(
    store: &Store,
    cluster_id: Uuid,
    user_id: &str,
) -> Result<(Option<String>, i64), WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let r =
                client.query_one(MEMBER_STANDING_SQL, &[&cluster_id.to_string(), &user_id]).await.map_err(pg_err)?;
            Ok((r.get::<_, Option<String>>(0), r.get::<_, i64>(1)))
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let user = Uuid::parse_str(user_id).map_err(|_| WebError::Db(format!("invalid uuid {user_id:?}")))?;
            let role = role_doc(kv, user, cluster_id).await?.map(|d| d.value.role);
            Ok((role, admin_count(&cluster_member_roles(kv, cluster_id).await?)))
        }
        Store::None => Err(WebError::NotConfigured),
    }
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

/// `(user_id, role, admin_count)` of the member with this email, if any.
pub async fn member_on_cluster(
    store: &Store,
    cluster_id: Uuid,
    email: &str,
) -> Result<Option<(String, String, i64)>, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let row =
                client.query_opt(MEMBER_ON_CLUSTER_SQL, &[&cluster_id.to_string(), &email]).await.map_err(pg_err)?;
            Ok(row.map(|r| (r.get::<_, String>(0), r.get::<_, String>(1), r.get::<_, i64>(2))))
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let Some(u) = user_by_email(kv, email).await? else {
                return Ok(None);
            };
            let Some(role) = role_doc(kv, u.value.id, cluster_id).await? else {
                return Ok(None);
            };
            let admins = admin_count(&cluster_member_roles(kv, cluster_id).await?);
            Ok(Some((u.value.id.to_string(), role.value.role, admins)))
        }
        Store::None => Err(WebError::NotConfigured),
    }
}

// ===========================================================================
// operator.rs: the cell's users, their names and roles
// ===========================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantRow {
    pub id: String,
    pub slug: String,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterRow {
    pub id: String,
    pub slug: String,
    pub tenant_id: String,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserRow {
    pub id: String,
    pub email: String,
    pub name: Option<String>,
    pub tenant_id: String,
    pub tenant_slug: String,
    pub is_operator: bool,
    pub has_local_password: bool,
    pub created_at: String,
    pub last_login_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoleRow {
    pub user_id: String,
    pub cluster_id: String,
    pub cluster_slug: String,
    pub role: String,
}

/// Everything `GET /api/operator/users` renders, for the acting cluster's cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperatorListing {
    pub cell_id: String,
    pub cell_slug: String,
    pub tenants: Vec<TenantRow>,
    pub clusters: Vec<ClusterRow>,
    pub users: Vec<UserRow>,
    pub roles: Vec<RoleRow>,
}

/// The cell of the acting cluster: `(cell id, cell slug)`.
async fn pg_cell_for(client: &tokio_postgres::Client, cluster_id: Uuid) -> Result<(String, String), Refusal> {
    let row = client
        .query_opt(
            "SELECT ce.id::text, ce.slug
               FROM queen_proxy.clusters c
               JOIN queen_proxy.cells ce ON ce.id = c.cell_id
              WHERE c.id = $1::text::uuid",
            &[&cluster_id.to_string()],
        )
        .await
        .map_err(|e| {
            tracing::warn!(target: "operator", err = %e, "cell lookup failed");
            Refusal::BadGateway("cell lookup failed")
        })?;
    row.map(|r| (r.get(0), r.get(1))).ok_or(Refusal::Misdirected("acting cluster no longer exists"))
}

async fn kv_cell_for(kv: &dyn KvBackend, cluster_id: Uuid) -> Result<Result<(Uuid, String), Refusal>, WebError> {
    let gone = Refusal::Misdirected("acting cluster no longer exists");
    let Some(c) = step!(by_id::<ClusterDoc>(kv, ns::CLUSTERS, cluster_id).await, "cell lookup failed") else {
        return Ok(Err(gone));
    };
    let Some(cell) = step!(by_id::<CellDoc>(kv, ns::CELLS, c.value.cell_id).await, "cell lookup failed") else {
        return Ok(Err(gone));
    };
    Ok(Ok((cell.value.id, cell.value.slug)))
}

/// The acting cluster's cell, and every tenant, cluster, user and role on it.
pub async fn operator_listing(store: &Store, acting_cluster: Uuid) -> Result<OperatorListing, Refusal> {
    match store {
        Store::Pg(pool) => pg_operator_listing(pool, acting_cluster).await,
        Store::Kv(kv) => {
            script_answer(kv_operator_listing(kv.as_ref(), acting_cluster).await, "user list failed", None)
        }
        Store::None => Err(Refusal::BadGateway("pxdb unavailable")),
    }
}

async fn pg_operator_listing(pool: &deadpool_postgres::Pool, acting_cluster: Uuid) -> Result<OperatorListing, Refusal> {
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "list_users: pool.get failed");
            return Err(Refusal::BadGateway("pxdb unavailable"));
        }
    };
    let (cell_id, cell_slug) = pg_cell_for(&client, acting_cluster).await?;

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
            return Err(Refusal::BadGateway("tenant list failed"));
        }
    };
    let tenants = tenant_rows.iter().map(|r| TenantRow { id: r.get(0), slug: r.get(1), name: r.get(2) }).collect();

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
            return Err(Refusal::BadGateway("cluster list failed"));
        }
    };
    let clusters = cluster_rows
        .iter()
        .map(|r| ClusterRow { id: r.get(0), slug: r.get(1), tenant_id: r.get(2), status: r.get(3) })
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
            return Err(Refusal::BadGateway("user list failed"));
        }
    };
    let users = user_rows
        .iter()
        .map(|r| UserRow {
            id: r.get(0),
            email: r.get(1),
            name: r.get(2),
            tenant_id: r.get(3),
            tenant_slug: r.get(4),
            is_operator: r.get(5),
            has_local_password: r.get(6),
            created_at: r.get(7),
            last_login_at: r.get(8),
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
            return Err(Refusal::BadGateway("role list failed"));
        }
    };
    let roles = role_rows
        .iter()
        .map(|r| RoleRow { user_id: r.get(0), cluster_id: r.get(1), cluster_slug: r.get(2), role: r.get(3) })
        .collect();

    Ok(OperatorListing { cell_id, cell_slug, tenants, clusters, users, roles })
}

async fn kv_operator_listing(
    kv: &dyn KvBackend,
    acting_cluster: Uuid,
) -> Result<Result<OperatorListing, Refusal>, WebError> {
    let (cell_id, cell_slug) = match kv_cell_for(kv, acting_cluster).await? {
        Ok(v) => v,
        Err(r) => return Ok(Err(r)),
    };
    let mut clusters: Vec<ClusterDoc> = step!(cell_clusters(kv, cell_id).await, "cluster list failed");
    clusters.sort_by(|a, b| a.slug.cmp(&b.slug));
    let tenant_ids: Vec<Uuid> = clusters.iter().map(|c| c.tenant_id).collect::<HashSet<_>>().into_iter().collect();
    let mut tenants: Vec<TenantDoc> =
        step!(many::<TenantDoc>(kv, ns::TENANTS, &tenant_ids).await, "tenant list failed")
            .into_iter()
            .map(|d| d.value)
            .collect();
    tenants.sort_by(|a, b| a.slug.cmp(&b.slug));

    let mut users: Vec<UserRow> = Vec::new();
    for t in &tenants {
        let ids = step!(children(kv, ns::USER_TENANT, t.id).await, "user list failed");
        for u in step!(many::<UserDoc>(kv, ns::USERS, &ids).await, "user list failed") {
            let u = u.value;
            users.push(UserRow {
                id: u.id.to_string(),
                email: u.email,
                name: u.name,
                tenant_id: t.id.to_string(),
                tenant_slug: t.slug.clone(),
                is_operator: u.is_operator,
                has_local_password: u.password_hash.is_some(),
                created_at: utc_iso(u.created_at_us),
                last_login_at: u.last_login_at_us.map(utc_iso),
            });
        }
    }
    users.sort_by(|a, b| (&a.tenant_slug, &a.email).cmp(&(&b.tenant_slug, &b.email)));

    let mut roles: Vec<RoleRow> = Vec::new();
    for c in &clusters {
        let mut members = step!(cluster_member_roles(kv, c.id).await, "role list failed");
        members.sort_by_key(|r| r.value.user_id);
        roles.extend(members.into_iter().map(|r| RoleRow {
            user_id: r.value.user_id.to_string(),
            cluster_id: c.id.to_string(),
            cluster_slug: c.slug.clone(),
            role: r.value.role,
        }));
    }

    Ok(Ok(OperatorListing {
        cell_id: cell_id.to_string(),
        cell_slug,
        tenants: tenants.into_iter().map(|t| TenantRow { id: t.id.to_string(), slug: t.slug, name: t.name }).collect(),
        clusters: clusters
            .into_iter()
            .map(|c| ClusterRow {
                id: c.id.to_string(),
                slug: c.slug,
                tenant_id: c.tenant_id.to_string(),
                status: c.status,
            })
            .collect(),
        users,
        roles,
    }))
}

/// An operator-created account (`POST /api/operator/users`), validated by
/// the handler; `password_hash` is already bcrypt.
#[derive(Clone, Debug)]
pub struct NewUser<'a> {
    pub tenant_id: Uuid,
    pub cluster_id: Uuid,
    pub email: &'a str,
    pub name: &'a str,
    pub provider: &'a str,
    pub password_hash: Option<String>,
    pub role: &'a str,
}

/// Create a user on the acting cluster's cell with its name, first role and
/// the operator's audit row — all or nothing. Returns the new user's id.
pub async fn operator_create_user(
    store: &Store,
    acting_cluster: Uuid,
    u: &NewUser<'_>,
    actor_id: Uuid,
) -> Result<String, Refusal> {
    match store {
        Store::Pg(pool) => pg_operator_create_user(pool, acting_cluster, u, actor_id).await,
        Store::Kv(kv) => script_answer(
            retrying!(kv_operator_create_user_once(kv.as_ref(), acting_cluster, u, actor_id)),
            "user creation failed",
            Some("a user with this email already exists"),
        )
        .map(|id| id.to_string()),
        Store::None => Err(Refusal::BadGateway("pxdb unavailable")),
    }
}

async fn pg_operator_create_user(
    pool: &deadpool_postgres::Pool,
    acting_cluster: Uuid,
    u: &NewUser<'_>,
    actor_id: Uuid,
) -> Result<String, Refusal> {
    let mut client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "create_user: pool.get failed");
            return Err(Refusal::BadGateway("pxdb unavailable"));
        }
    };
    let (cell_id, _) = pg_cell_for(&client, acting_cluster).await?;
    let tx = match client.transaction().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "create_user: transaction failed");
            return Err(Refusal::BadGateway("user creation failed"));
        }
    };

    let scoped = match tx
        .query_opt(
            "SELECT 1
               FROM queen_proxy.clusters
              WHERE id = $1::text::uuid
                AND tenant_id = $2::text::uuid
                AND cell_id = $3::text::uuid",
            &[&u.cluster_id.to_string(), &u.tenant_id.to_string(), &cell_id],
        )
        .await
    {
        Ok(row) => row.is_some(),
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "create_user: scope check failed");
            return Err(Refusal::BadGateway("cluster lookup failed"));
        }
    };
    if !scoped {
        return Err(Refusal::NotFound("tenant and cluster are not on this cell"));
    }

    let user_row = match tx
        .query_one(
            "SELECT queen_proxy.create_user($1::text::uuid, $2, $3, $4)::text",
            &[&u.tenant_id.to_string(), &u.email, &u.password_hash, &u.provider],
        )
        .await
    {
        Ok(row) => row,
        Err(e) if e.code() == Some(&SqlState::UNIQUE_VIOLATION) => {
            return Err(Refusal::Conflict("a user with this email already exists"))
        }
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "create_user failed");
            return Err(Refusal::BadGateway("user creation failed"));
        }
    };
    let user_id = user_row.get::<_, String>(0);

    if let Err(e) = tx.execute("SELECT queen_proxy.set_user_name($1::text::uuid, $2)", &[&user_id, &u.name]).await {
        tracing::warn!(target: "operator", err = %e, "initial user name failed");
        return Err(Refusal::BadGateway("user name could not be saved"));
    }

    if let Err(e) = tx
        .execute(
            "SELECT queen_proxy.grant_cluster_role($1::text::uuid, $2, $3)",
            &[&u.cluster_id.to_string(), &u.email, &u.role],
        )
        .await
    {
        tracing::warn!(target: "operator", err = %e, "initial role grant failed");
        return Err(Refusal::BadGateway("initial role grant failed"));
    }
    let meta = json!({ "email": u.email, "name": u.name, "provider": u.provider, "role": u.role }).to_string();
    if let Err(e) = tx
        .execute(
            "SELECT queen_proxy.record_operation($1::text::uuid, $2::text::uuid, 'user', $3::text::uuid, 'operator_user_created', $4, $5::text::jsonb)",
            &[&u.tenant_id.to_string(), &u.cluster_id.to_string(), &actor_id.to_string(), &user_id, &meta],
        )
        .await
    {
        tracing::warn!(target: "operator", err = %e, "operator user audit failed");
        return Err(Refusal::BadGateway("user creation audit failed"));
    }
    if let Err(e) = tx.commit().await {
        tracing::warn!(target: "operator", err = %e, "create_user commit failed");
        return Err(Refusal::BadGateway("user creation failed"));
    }
    Ok(user_id)
}

async fn kv_operator_create_user_once(
    kv: &dyn KvBackend,
    acting_cluster: Uuid,
    u: &NewUser<'_>,
    actor_id: Uuid,
) -> Result<Result<Uuid, Refusal>, WebError> {
    let (cell_id, _) = match kv_cell_for(kv, acting_cluster).await? {
        Ok(v) => v,
        Err(r) => return Ok(Err(r)),
    };
    let cluster = step!(by_id::<ClusterDoc>(kv, ns::CLUSTERS, u.cluster_id).await, "cluster lookup failed");
    let Some(cluster) = cluster.map(|c| c.value).filter(|c| c.tenant_id == u.tenant_id && c.cell_id == cell_id) else {
        return Ok(Err(Refusal::NotFound("tenant and cluster are not on this cell")));
    };
    let tenant = step!(by_id::<TenantDoc>(kv, ns::TENANTS, u.tenant_id).await, "user creation failed");
    let Some(tenant) = tenant.map(|t| t.value) else {
        tracing::warn!(target: "operator", tenant = %u.tenant_id, "create_user: unknown tenant");
        return Ok(Err(Refusal::BadGateway("user creation failed")));
    };

    let mut tx = Tx::default();
    let name = step!(check_user_name(u.name), "user name could not be saved");
    // create_user + set_user_name, as one document: the name is written with
    // the row, and set_user_name's own audit row follows create_user's.
    let user = step!(
        plan_create_user(&mut tx, &tenant, u.email, u.password_hash.clone(), u.provider, Some(name.clone())),
        "user creation failed"
    );
    step!(
        tx.record(&Audit {
            tenant_id: tenant.id,
            cluster_id: None,
            actor: "control_plane",
            actor_id: Some(user.id),
            action: "user_name_changed",
            target: Some(user.id.to_string()),
            meta: json!({ "old_name": Value::Null, "name": name }),
        }),
        "user name could not be saved"
    );
    step!(plan_grant_role(&mut tx, &cluster, &user, u.role, None), "initial role grant failed");
    step!(
        tx.record(&Audit {
            tenant_id: u.tenant_id,
            cluster_id: Some(u.cluster_id),
            actor: "user",
            actor_id: Some(actor_id),
            action: "operator_user_created",
            target: Some(user.id.to_string()),
            meta: json!({ "email": u.email, "name": u.name, "provider": u.provider, "role": u.role }),
        }),
        "user creation audit failed"
    );
    commit(kv, tx).await?;
    Ok(Ok(user.id))
}

/// What a rename did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rename {
    /// The name was already this: nothing written, nothing audited.
    Unchanged,
    Renamed,
}

/// Rename a user of the acting cluster's cell (`PATCH /api/operator/users/:id`).
pub async fn operator_rename_user(
    store: &Store,
    acting_cluster: Uuid,
    user_id: Uuid,
    name: &str,
    actor_id: Uuid,
) -> Result<Rename, Refusal> {
    match store {
        Store::Pg(pool) => pg_operator_rename_user(pool, acting_cluster, user_id, name, actor_id).await,
        Store::Kv(kv) => script_answer(
            retrying!(kv_operator_rename_user_once(kv.as_ref(), acting_cluster, user_id, name, actor_id)),
            "user update failed",
            None,
        ),
        Store::None => Err(Refusal::BadGateway("pxdb unavailable")),
    }
}

async fn pg_operator_rename_user(
    pool: &deadpool_postgres::Pool,
    acting_cluster: Uuid,
    user_id: Uuid,
    name: &str,
    actor_id: Uuid,
) -> Result<Rename, Refusal> {
    let mut client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "update_user: pool.get failed");
            return Err(Refusal::BadGateway("pxdb unavailable"));
        }
    };
    let (cell_id, _) = pg_cell_for(&client, acting_cluster).await?;
    let tx = match client.transaction().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "update_user: transaction failed");
            return Err(Refusal::BadGateway("user update failed"));
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
        Ok(None) => return Err(Refusal::NotFound("user is not on this cell")),
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "update_user: scope lookup failed");
            return Err(Refusal::BadGateway("user lookup failed"));
        }
    };
    let tenant_id = target.get::<_, String>(0);
    let old_name = target.get::<_, Option<String>>(1);
    if old_name.as_deref() == Some(name) {
        return Ok(Rename::Unchanged);
    }

    if let Err(e) =
        tx.execute("SELECT queen_proxy.set_user_name($1::text::uuid, $2)", &[&user_id.to_string(), &name]).await
    {
        tracing::warn!(target: "operator", err = %e, "set_user_name failed");
        return Err(Refusal::BadGateway("user update failed"));
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
        return Err(Refusal::BadGateway("user update audit failed"));
    }
    if let Err(e) = tx.commit().await {
        tracing::warn!(target: "operator", err = %e, "update_user commit failed");
        return Err(Refusal::BadGateway("user update failed"));
    }
    Ok(Rename::Renamed)
}

async fn kv_operator_rename_user_once(
    kv: &dyn KvBackend,
    acting_cluster: Uuid,
    user_id: Uuid,
    name: &str,
    actor_id: Uuid,
) -> Result<Result<Rename, Refusal>, WebError> {
    let (cell_id, _) = match kv_cell_for(kv, acting_cluster).await? {
        Ok(v) => v,
        Err(r) => return Ok(Err(r)),
    };
    let not_here = Refusal::NotFound("user is not on this cell");
    let Some(user) = step!(by_id::<UserDoc>(kv, ns::USERS, user_id).await, "user lookup failed") else {
        return Ok(Err(not_here));
    };
    if !step!(tenant_on_cell(kv, user.value.tenant_id, cell_id).await, "user lookup failed") {
        return Ok(Err(not_here));
    }
    let old_name = user.value.name.clone();
    if old_name.as_deref() == Some(name) {
        return Ok(Ok(Rename::Unchanged));
    }
    let mut tx = Tx::default();
    let new_name = step!(plan_set_user_name(&mut tx, &user, name), "user update failed");
    step!(
        tx.record(&Audit {
            tenant_id: user.value.tenant_id,
            cluster_id: None,
            actor: "user",
            actor_id: Some(actor_id),
            action: "operator_user_updated",
            target: Some(user_id.to_string()),
            meta: json!({ "old_name": old_name, "name": new_name }),
        }),
        "user update audit failed"
    );
    commit(kv, tx).await?;
    Ok(Ok(Rename::Renamed))
}

/// Grant (`Some(role)`) or remove (`None`) a user's role on a cluster of the
/// acting cluster's cell. `would_orphan(current, admin_count, new)` is the
/// caller's last-admin policy; it is decided against the SAME reads the
/// write commits on (Postgres: `LOCK TABLE cluster_roles`; KV: every admin
/// seat it counted is version-checked in the write's batch).
pub async fn operator_change_role(
    store: &Store,
    acting_cluster: Uuid,
    user_id: Uuid,
    cluster_id: Uuid,
    new_role: Option<&str>,
    would_orphan: fn(Option<&str>, i64, Option<&str>) -> bool,
    actor_id: Uuid,
) -> Result<(), Refusal> {
    match store {
        Store::Pg(pool) => {
            pg_operator_change_role(pool, acting_cluster, user_id, cluster_id, new_role, would_orphan, actor_id).await
        }
        Store::Kv(kv) => script_answer(
            retrying!(kv_operator_change_role_once(
                kv.as_ref(),
                acting_cluster,
                user_id,
                cluster_id,
                new_role,
                would_orphan,
                actor_id
            )),
            "role change failed",
            None,
        ),
        Store::None => Err(Refusal::BadGateway("pxdb unavailable")),
    }
}

async fn pg_operator_change_role(
    pool: &deadpool_postgres::Pool,
    acting_cluster: Uuid,
    user_id: Uuid,
    cluster_id: Uuid,
    new_role: Option<&str>,
    would_orphan: fn(Option<&str>, i64, Option<&str>) -> bool,
    actor_id: Uuid,
) -> Result<(), Refusal> {
    let mut client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "change_role: pool.get failed");
            return Err(Refusal::BadGateway("pxdb unavailable"));
        }
    };
    let (cell_id, _) = pg_cell_for(&client, acting_cluster).await?;
    let tx = match client.transaction().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "change_role: transaction failed");
            return Err(Refusal::BadGateway("role change failed"));
        }
    };
    if let Err(e) = tx.batch_execute("LOCK TABLE queen_proxy.cluster_roles IN SHARE ROW EXCLUSIVE MODE").await {
        tracing::warn!(target: "operator", err = %e, "role lock failed");
        return Err(Refusal::BadGateway("role change failed"));
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
        Ok(None) => return Err(Refusal::NotFound("user and cluster are not on this cell or tenant")),
        Err(e) => {
            tracing::warn!(target: "operator", err = %e, "role standing lookup failed");
            return Err(Refusal::BadGateway("role lookup failed"));
        }
    };
    let email = standing.get::<_, String>(0);
    let tenant_id = standing.get::<_, String>(1);
    let current_role = standing.get::<_, Option<String>>(2);
    let admin_count = standing.get::<_, i64>(3);

    if new_role.is_none() && current_role.is_none() {
        return Err(Refusal::NotFound("user has no access to this cluster"));
    }
    if would_orphan(current_role.as_deref(), admin_count, new_role) {
        return Err(Refusal::BadRequest("cannot remove or demote the last admin of this cluster"));
    }

    let action = if let Some(role) = new_role {
        if let Err(e) = tx
            .execute(
                "SELECT queen_proxy.grant_cluster_role($1::text::uuid, $2, $3)",
                &[&cluster_id.to_string(), &email, &role],
            )
            .await
        {
            tracing::warn!(target: "operator", err = %e, "role grant failed");
            return Err(Refusal::BadGateway("role grant failed"));
        }
        "operator_role_granted"
    } else {
        if let Err(e) = tx
            .execute("SELECT queen_proxy.revoke_cluster_role($1::text::uuid, $2)", &[&cluster_id.to_string(), &email])
            .await
        {
            tracing::warn!(target: "operator", err = %e, "role revocation failed");
            return Err(Refusal::BadGateway("role revocation failed"));
        }
        "operator_role_revoked"
    };

    let meta = json!({ "email": email, "role": new_role, "previous_role": current_role }).to_string();
    if let Err(e) = tx
        .execute(
            "SELECT queen_proxy.record_operation($1::text::uuid, $2::text::uuid, 'user', $3::text::uuid, $4, $5, $6::text::jsonb)",
            &[&tenant_id, &cluster_id.to_string(), &actor_id.to_string(), &action, &user_id.to_string(), &meta],
        )
        .await
    {
        tracing::warn!(target: "operator", err = %e, "operator role audit failed");
        return Err(Refusal::BadGateway("role change audit failed"));
    }
    if let Err(e) = tx.commit().await {
        tracing::warn!(target: "operator", err = %e, "role change commit failed");
        return Err(Refusal::BadGateway("role change failed"));
    }
    Ok(())
}

async fn kv_operator_change_role_once(
    kv: &dyn KvBackend,
    acting_cluster: Uuid,
    user_id: Uuid,
    cluster_id: Uuid,
    new_role: Option<&str>,
    would_orphan: fn(Option<&str>, i64, Option<&str>) -> bool,
    actor_id: Uuid,
) -> Result<Result<(), Refusal>, WebError> {
    let (cell_id, _) = match kv_cell_for(kv, acting_cluster).await? {
        Ok(v) => v,
        Err(r) => return Ok(Err(r)),
    };
    let user = step!(by_id::<UserDoc>(kv, ns::USERS, user_id).await, "role lookup failed");
    let cluster = step!(by_id::<ClusterDoc>(kv, ns::CLUSTERS, cluster_id).await, "role lookup failed");
    let (Some(user), Some(cluster)) = (user, cluster) else {
        return Ok(Err(Refusal::NotFound("user and cluster are not on this cell or tenant")));
    };
    let (user, cluster) = (user.value, cluster.value);
    if cluster.tenant_id != user.tenant_id || cluster.cell_id != cell_id {
        return Ok(Err(Refusal::NotFound("user and cluster are not on this cell or tenant")));
    }
    let current = step!(role_doc(kv, user_id, cluster_id).await, "role lookup failed");
    let seats = step!(cluster_member_roles(kv, cluster_id).await, "role lookup failed");
    let admins: Vec<Doc<RoleDoc>> = seats.into_iter().filter(|r| r.value.role == "admin").collect();
    let current_role = current.as_ref().map(|d| d.value.role.clone());

    if new_role.is_none() && current_role.is_none() {
        return Ok(Err(Refusal::NotFound("user has no access to this cluster")));
    }
    if would_orphan(current_role.as_deref(), admins.len() as i64, new_role) {
        return Ok(Err(Refusal::BadRequest("cannot remove or demote the last admin of this cluster")));
    }

    let mut tx = Tx::default();
    // The KV's LOCK TABLE: taking an admin seat away is decided on the admin
    // count read above, so every OTHER admin seat is re-asserted at the
    // version counted. A concurrent demotion of one of them fails this batch
    // (or theirs), the loser re-reads, and the last admin survives.
    if current_role.as_deref() == Some("admin") && new_role != Some("admin") {
        for seat in admins.iter().filter(|s| s.value.user_id != user_id) {
            tx.fresh(ns::ROLES, schema::key2(seat.value.user_id, cluster_id), &seat.value, Some(seat.version));
        }
    }
    let action = match (new_role, current) {
        (Some(role), current) => {
            step!(plan_grant_role(&mut tx, &cluster, &user, role, current), "role grant failed");
            "operator_role_granted"
        }
        (None, Some(current)) => {
            step!(plan_revoke_role(&mut tx, &cluster, &user, &current), "role revocation failed");
            "operator_role_revoked"
        }
        (None, None) => return Ok(Err(Refusal::NotFound("user has no access to this cluster"))),
    };
    step!(
        tx.record(&Audit {
            tenant_id: user.tenant_id,
            cluster_id: Some(cluster_id),
            actor: "user",
            actor_id: Some(actor_id),
            action,
            target: Some(user_id.to_string()),
            meta: json!({ "email": user.email, "role": new_role, "previous_role": current_role }),
        }),
        "role change audit failed"
    );
    commit(kv, tx).await?;
    Ok(Ok(()))
}

// ===========================================================================
// control-plane functions with no HTTP route (in the standalone proxy they
// are called over psql; in the single binary there is no psql, so these are
// their only implementation): set_operator, bootstrap_tenant,
// set_tenant_status, delete_tenant
// ===========================================================================

/// `queen_proxy.set_operator(email, enabled)` (006). Deliberately no HTTP
/// route may call this (006's header): it is the fleet operator's lever.
pub async fn set_operator(store: &Store, email: &str, enabled: bool) -> Result<(), WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            client.execute("SELECT queen_proxy.set_operator($1, $2)", &[&email, &enabled]).await.map_err(pg_err)?;
            Ok(())
        }
        Store::Kv(kv) => retrying!(kv_set_operator_once(kv.as_ref(), email, enabled)),
        Store::None => Err(WebError::NotConfigured),
    }
}

async fn kv_set_operator_once(kv: &dyn KvBackend, email: &str, enabled: bool) -> Result<(), WebError> {
    if !email.contains('@') {
        return Err(raised(format!("set_operator: invalid email {email}")));
    }
    let email = email.trim().to_lowercase();
    let Some(u) = user_by_email(kv, &email).await? else {
        return Err(raised(format!("set_operator: unknown user {email}")));
    };
    let was = u.value.is_operator;
    let mut doc = u.value.clone();
    doc.is_operator = enabled;
    let mut tx = Tx::default();
    tx.fresh(ns::USERS, schema::key(doc.id), &doc, Some(u.version));
    // Audited even when the bit does not change (006).
    tx.record(&Audit {
        tenant_id: doc.tenant_id,
        cluster_id: None,
        actor: "control_plane",
        actor_id: Some(doc.id),
        action: if enabled { "operator_granted" } else { "operator_revoked" },
        target: Some(email.clone()),
        meta: json!({ "is_operator": enabled, "was": was }),
    })?;
    commit(kv, tx).await
}

/// `queen_proxy.set_tenant_status(tenant, status)` (002). Returns the
/// tenant's clusters for [`invalidate_local`] (KV; on Postgres the function
/// NOTIFYs each one itself and this is empty).
pub async fn set_tenant_status(store: &Store, tenant_id: Uuid, status: &str) -> Result<Vec<Uuid>, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            client
                .execute("SELECT queen_proxy.set_tenant_status($1::text::uuid, $2)", &[&tenant_id.to_string(), &status])
                .await
                .map_err(pg_err)?;
            Ok(Vec::new())
        }
        Store::Kv(kv) => retrying!(kv_set_tenant_status_once(kv.as_ref(), tenant_id, status)),
        Store::None => Err(WebError::NotConfigured),
    }
}

async fn kv_set_tenant_status_once(kv: &dyn KvBackend, tenant_id: Uuid, status: &str) -> Result<Vec<Uuid>, WebError> {
    if !TENANT_STATUSES.contains(&status) {
        return Err(raised(format!("set_tenant_status: invalid status {status}")));
    }
    let Some(t) = by_id::<TenantDoc>(kv, ns::TENANTS, tenant_id).await? else {
        return Err(raised(format!("set_tenant_status: unknown tenant {tenant_id}")));
    };
    let mut doc = t.value.clone();
    doc.status = status.to_string();
    let mut tx = Tx::default();
    tx.fresh(ns::TENANTS, schema::key(tenant_id), &doc, Some(t.version));
    tx.record(&Audit {
        tenant_id,
        cluster_id: None,
        actor: "control_plane",
        actor_id: None,
        action: "tenant_status_changed",
        target: Some(tenant_id.to_string()),
        meta: json!({ "status": status }),
    })?;
    commit(kv, tx).await?;
    children(kv, ns::CLUSTER_TENANT, tenant_id).await
}

/// `queen_proxy.bootstrap_tenant(...)`'s arguments (008's signature).
#[derive(Clone, Debug)]
pub struct Bootstrap<'a> {
    pub tenant_slug: &'a str,
    /// `None`: the tenant is named after its slug.
    pub tenant_name: Option<&'a str>,
    pub cluster_slug: &'a str,
    pub plan_code: &'a str,
    pub cell: Uuid,
    pub admin_email: &'a str,
    /// Plaintext; `None` creates a password-less (OAuth or API-key-only) admin.
    pub password: Option<&'a str>,
    /// 008's default is `"default"`.
    pub key_name: &'a str,
}

/// `queen_proxy.bootstrap_tenant(...)` (008): tenant + cluster + admin user +
/// admin role + a full-scope API key in one call, idempotent on the slugs.
/// Returns `{tenant_id, cluster_id, user_id, api_key, password_set,
/// can_login}`; `api_key` is the PLAINTEXT, shown once (null on a re-run).
pub async fn bootstrap_tenant(store: &Store, b: &Bootstrap<'_>) -> Result<Value, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let row = client
                .query_one(
                    "SELECT (queen_proxy.bootstrap_tenant($1, $2, $3, $4, $5::text::uuid, $6, $7, $8))::text",
                    &[
                        &b.tenant_slug,
                        &b.tenant_name,
                        &b.cluster_slug,
                        &b.plan_code,
                        &b.cell.to_string(),
                        &b.admin_email,
                        &b.password,
                        &b.key_name,
                    ],
                )
                .await
                .map_err(pg_err)?;
            serde_json::from_str(&row.get::<_, String>(0)).map_err(|e| WebError::Db(e.to_string()))
        }
        Store::Kv(kv) => {
            if b.tenant_slug.trim().is_empty() {
                return Err(raised("bootstrap_tenant: tenant_slug must not be empty"));
            }
            if b.cluster_slug.trim().is_empty() {
                return Err(raised("bootstrap_tenant: cluster_slug must not be empty"));
            }
            if !b.admin_email.contains('@') {
                return Err(raised(format!("bootstrap_tenant: invalid admin_email {}", b.admin_email)));
            }
            if b.key_name.trim().is_empty() {
                return Err(raised("bootstrap_tenant: key_name must not be empty"));
            }
            // `crypt(p_password, gen_salt('bf', 10))`: bcrypt cost 10, off the
            // runtime threads.
            let hash = match b.password {
                None => None,
                Some(p) => {
                    let p = p.to_string();
                    let h = tokio::task::spawn_blocking(move || bcrypt::hash(p, 10))
                        .await
                        .map_err(|e| WebError::Db(e.to_string()))?
                        .map_err(|e| WebError::Db(e.to_string()))?;
                    Some(h)
                }
            };
            retrying!(kv_bootstrap_once(kv.as_ref(), b, hash.as_deref()))
        }
        Store::None => Err(WebError::NotConfigured),
    }
}

async fn kv_bootstrap_once(
    kv: &dyn KvBackend,
    b: &Bootstrap<'_>,
    password_hash: Option<&str>,
) -> Result<Value, WebError> {
    let tenant_slug = b.tenant_slug.trim().to_lowercase();
    let cluster_slug = b.cluster_slug.trim().to_lowercase();
    let email = b.admin_email.trim().to_lowercase();
    let key_name = b.key_name.trim();
    let mut tx = Tx::default();

    // tenant
    let (tenant, new_tenant) = match tenant_by_slug(kv, &tenant_slug).await? {
        Some(t) => (t.value, false),
        None => {
            // COALESCE(btrim(p_tenant_name), v_tenant_slug)
            let name = b.tenant_name.map(|n| n.trim().to_string()).unwrap_or_else(|| tenant_slug.clone());
            (plan_create_tenant(&mut tx, &tenant_slug, &name)?, true)
        }
    };

    // cluster (plan + cell validated as create_cluster does)
    let (cluster, new_cluster) = match cluster_by_slug(kv, &cluster_slug).await? {
        Some(c) if c.value.tenant_id != tenant.id => {
            return Err(raised(format!(
                "bootstrap_tenant: cluster slug {cluster_slug} already belongs to tenant {}",
                c.value.tenant_id
            )))
        }
        Some(c) => (c.value, false),
        None => {
            let Some(plan) = plan_by_code(kv, b.plan_code).await? else {
                return Err(raised(format!("create_cluster: unknown plan code {}", b.plan_code)));
            };
            if by_id::<CellDoc>(kv, ns::CELLS, b.cell).await?.is_none() {
                return Err(raised(format!("create_cluster: unknown cell {}", b.cell)));
            }
            (plan_create_cluster(&mut tx, &tenant, &cluster_slug, &plan.value, b.cell, b.plan_code)?, true)
        }
    };

    // admin user
    let (user, new_user) = match user_by_email(kv, &email).await? {
        Some(u) if u.value.tenant_id != tenant.id => {
            return Err(raised(format!(
                "bootstrap_tenant: user {email} already belongs to tenant {}",
                u.value.tenant_id
            )))
        }
        Some(u) => (u.value, false),
        None => (plan_create_user(&mut tx, &tenant, &email, password_hash.map(str::to_string), "local", None)?, true),
    };

    let existing = if new_user || new_cluster { None } else { role_doc(kv, user.id, cluster.id).await? };
    plan_grant_role(&mut tx, &cluster, &user, "admin", existing)?;

    // Can this admin sign in? A local password, or a linked identity.
    let has_identity = !new_user && !children(kv, ns::IDENTITY_USER, user.id).await?.is_empty();
    let can_login = user.password_hash.is_some() || has_identity;
    if !can_login {
        tracing::warn!(
            target: "store",
            admin = %email,
            "bootstrap_tenant: admin has NO password and NO linked identity — it cannot sign in to the \
             console with a password. The tenant, cluster, admin role and API key were all created normally."
        );
    }

    // API key, keyed off the name: a re-run neither duplicates it nor
    // pretends to return a key it can no longer produce.
    let live_named = !new_cluster
        && cluster_key_docs(kv, cluster.id)
            .await?
            .iter()
            .any(|k| k.value.name == key_name && k.value.revoked_at_us.is_none());
    let api_key = if live_named {
        None
    } else {
        let plaintext = crate::auth::generate_api_key("live");
        plan_issue_key(&mut tx, &cluster, key_name, &crate::auth::key_hash_hex(&plaintext), &all_scopes(), None)?;
        Some(plaintext)
    };

    let password_set = b.password.is_some();
    tx.record(&Audit {
        tenant_id: tenant.id,
        cluster_id: Some(cluster.id),
        actor: "control_plane",
        actor_id: Some(user.id),
        action: "tenant_bootstrapped",
        target: Some(tenant.id.to_string()),
        meta: json!({
            "tenant_slug": tenant_slug, "cluster_slug": cluster_slug,
            "plan_code": b.plan_code, "admin_email": email,
            "key_issued": api_key.is_some(),
            "password_set": password_set,
            "can_login": can_login,
        }),
    })?;
    // Signup event — only on the run that created the tenant.
    if new_tenant {
        tx.outbox(
            "tenant_bootstrapped",
            json!({
                "tenant_id": tenant.id, "tenant_slug": tenant_slug,
                "cluster_id": cluster.id, "cluster_slug": cluster_slug,
                "plan_code": b.plan_code, "admin_email": email,
            }),
        )?;
    }
    commit(kv, tx).await?;
    Ok(json!({
        "tenant_id": tenant.id,
        "cluster_id": cluster.id,
        "user_id": user.id,
        "api_key": api_key,
        "password_set": password_set,
        "can_login": can_login,
    }))
}

/// `queen_proxy.delete_tenant(tenant, force)` (007): hard-delete a tenant and
/// everything under it. Refuses a tenant not in status `deleting` unless
/// `force`; a second call answers `{"deleted": false, "existed": false}`.
/// The result carries the clusters' `broker_tenant_uuid`s (they exist
/// nowhere else afterwards) and the counts.
///
/// KV: not one transaction — a wipe can be far larger than a batch. The
/// order keeps a partial failure safe to re-run: the `tenant_deleted` outbox
/// event (with the uuids) FIRST, then keys (hash index first: the data plane
/// stops authenticating them at once), roles, users + identities, queues,
/// usage, audit rows, outbox redaction, clusters, and the tenant row LAST —
/// so a re-run finds the tenant and finishes the job (its counts are then
/// what was left, and the event is emitted again: at-least-once).
pub async fn delete_tenant(store: &Store, tenant_id: Uuid, force: bool) -> Result<Value, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let row = client
                .query_one(
                    "SELECT (queen_proxy.delete_tenant($1::text::uuid, $2))::text",
                    &[&tenant_id.to_string(), &force],
                )
                .await
                .map_err(pg_err)?;
            serde_json::from_str(&row.get::<_, String>(0)).map_err(|e| WebError::Db(e.to_string()))
        }
        Store::Kv(kv) => kv_delete_tenant(kv.as_ref(), tenant_id, force).await,
        Store::None => Err(WebError::NotConfigured),
    }
}

async fn kv_delete_tenant(kv: &dyn KvBackend, tenant_id: Uuid, force: bool) -> Result<Value, WebError> {
    let Some(tenant) = by_id::<TenantDoc>(kv, ns::TENANTS, tenant_id).await? else {
        return Ok(json!({ "deleted": false, "existed": false, "tenant_id": tenant_id }));
    };
    let t = tenant.value;
    if t.status != "deleting" && !force {
        return Err(raised(format!(
            "delete_tenant: tenant {} is {}, not deleting -- call queen_proxy.set_tenant_status({}, 'deleting') \
             first (and purge the cell with queen.delete_tenant_data_v1), or pass p_force => true",
            t.slug, t.status, tenant_id
        )));
    }

    // Capture BEFORE anything is deleted.
    let mut clusters: Vec<ClusterDoc> = tenant_clusters(kv, tenant_id).await?;
    clusters.sort_by(|a, b| a.slug.cmp(&b.slug));
    let ids: Vec<Uuid> = clusters.iter().map(|c| c.id).collect();
    let id_set: HashSet<Uuid> = ids.iter().copied().collect();
    let clusters_json: Vec<Value> = clusters
        .iter()
        .map(|c| {
            json!({
                "cluster_id": c.id, "slug": c.slug, "cell_id": c.cell_id,
                "broker_tenant_uuid": c.broker_tenant_uuid,
            })
        })
        .collect();

    let user_ids = children(kv, ns::USER_TENANT, tenant_id).await?;
    let users: Vec<UserDoc> = many::<UserDoc>(kv, ns::USERS, &user_ids).await?.into_iter().map(|d| d.value).collect();

    // (user, cluster): the roles ON the tenant's clusters are the count; every
    // role the tenant's users hold is the cascade.
    let mut role_pairs: HashSet<(Uuid, Uuid)> = HashSet::new();
    for c in &ids {
        for u in children(kv, ns::ROLE_CLUSTER, *c).await? {
            role_pairs.insert((u, *c));
        }
    }
    let v_roles = role_pairs.len();
    for u in &user_ids {
        for k in kv::scan_keys(kv, ns::ROLES, &schema::prefix(u)).await? {
            if let Ok(c) = Uuid::parse_str(schema::tail(&k)) {
                role_pairs.insert((*u, c));
            }
        }
    }

    let mut keys: Vec<ApiKeyDoc> = Vec::new();
    for c in &ids {
        keys.extend(cluster_key_docs(kv, *c).await?.into_iter().map(|d| d.value));
    }
    let v_keys = keys.len();
    let v_revoked = keys.iter().filter(|k| k.revoked_at_us.is_none()).count();

    // No px.queues.cluster index: a full scan (a wipe is rare), which also
    // finds soft-deleted rows the live-name index no longer lists.
    let queues: Vec<QueueDoc> = kv::scan::<QueueDoc>(kv, ns::QUEUES, schema::K)
        .await?
        .into_iter()
        .map(|(_, d)| d.value)
        .filter(|q| id_set.contains(&q.cluster_id))
        .collect();

    // The surviving audit, first (see the function doc).
    let counts = json!({
        "clusters": clusters.len(), "users": users.len(), "cluster_roles": v_roles,
        "queues": queues.len(), "api_keys": v_keys, "api_keys_revoked": v_revoked,
    });
    let mut tx = Tx::default();
    tx.outbox(
        "tenant_deleted",
        json!({
            "tenant_id": tenant_id, "tenant_slug": t.slug, "status_was": t.status,
            "forced": force, "clusters": clusters_json, "counts": counts, "at": utc_iso(wall_us()),
        }),
    )?;
    commit(kv, tx).await?;

    let mut dead: Vec<(&'static str, String)> = Vec::new();
    for k in &keys {
        dead.push((ns::KEY_HASH, schema::key(&k.key_hash)));
    }
    for k in &keys {
        dead.push((ns::KEYS, schema::key(k.id)));
        dead.push((ns::KEY_CLUSTER, schema::key2(k.cluster_id, k.id)));
    }
    for (u, c) in &role_pairs {
        dead.push((ns::ROLES, schema::key2(u, c)));
        dead.push((ns::ROLE_CLUSTER, schema::key2(c, u)));
    }
    for u in &users {
        let identity_ids = children(kv, ns::IDENTITY_USER, u.id).await?;
        for i in many::<IdentityDoc>(kv, ns::IDENTITIES, &identity_ids).await? {
            dead.push((ns::IDENTITY_PROVIDER, identity_key(&i.value.provider, &i.value.provider_id)));
            dead.push((ns::IDENTITIES, schema::key(i.value.id)));
        }
        for i in identity_ids {
            dead.push((ns::IDENTITY_USER, schema::key2(u.id, i)));
        }
        dead.push((ns::USER_EMAIL, schema::key(&u.email)));
        dead.push((ns::USERS, schema::key(u.id)));
    }
    for u in &user_ids {
        dead.push((ns::USER_TENANT, schema::key2(tenant_id, u)));
    }
    for q in &queues {
        dead.push((ns::QUEUES, schema::key(q.id)));
    }
    for c in &ids {
        for n in [ns::QUEUE_NAME, ns::USAGE_MIN, ns::USAGE_DAY, ns::OPS_CLUSTER] {
            for k in kv::scan_keys(kv, n, &schema::prefix(c)).await? {
                dead.push((n, k));
            }
        }
    }
    // operations: the GDPR-relevant delete (meta/target carry addresses).
    let op_keys = kv::scan_keys(kv, ns::OPS, &schema::prefix(tenant_id)).await?;
    let v_ops = op_keys.len();
    dead.extend(op_keys.into_iter().map(|k| (ns::OPS, k)));
    delete_all(kv, dead).await?;

    let v_outbox = redact_outbox(kv, tenant_id, &id_set).await?;

    let mut dead: Vec<(&'static str, String)> = Vec::new();
    for c in &clusters {
        dead.push((ns::CLUSTER_SLUG, schema::key(&c.slug)));
        dead.push((ns::CLUSTER_TENANT, schema::key2(tenant_id, c.id)));
        dead.push((ns::CLUSTER_CELL, schema::key2(c.cell_id, c.id)));
        dead.push((ns::CLUSTERS, schema::key(c.id)));
    }
    delete_all(kv, dead).await?;
    let mut tx = Tx::default();
    tx.del(ns::TENANT_SLUG, schema::key(&t.slug));
    tx.del(ns::TENANTS, schema::key(tenant_id));
    commit(kv, tx).await?;

    Ok(json!({
        "deleted": true,
        "existed": true,
        "tenant_id": tenant_id,
        "tenant_slug": t.slug,
        "status_was": t.status,
        "forced": force,
        "clusters": clusters_json,
        "counts": {
            "clusters": clusters.len(),
            "users": users.len(),
            "cluster_roles": v_roles,
            "queues": queues.len(),
            "api_keys": v_keys,
            "api_keys_revoked": v_revoked,
            "operations": v_ops,
            "outbox_redacted": v_outbox,
        },
    }))
}

/// Strip `admin_email` from every outbox payload about this tenant or one of
/// its clusters (007: the rows stay — the outbox is a queue — the address
/// does not). Returns how many rows were redacted.
async fn redact_outbox(kv: &dyn KvBackend, tenant_id: Uuid, clusters: &HashSet<Uuid>) -> Result<usize, WebError> {
    let tenant = tenant_id.to_string();
    let clusters: HashSet<String> = clusters.iter().map(Uuid::to_string).collect();
    let about = |p: &Value| {
        let Some(o) = p.as_object() else { return false };
        o.contains_key("admin_email")
            && (o.get("tenant_id").and_then(Value::as_str) == Some(tenant.as_str())
                || o.get("cluster_id").and_then(Value::as_str).is_some_and(|c| clusters.contains(c)))
    };
    let mut redacted = 0;
    for (key, doc) in kv::scan::<OutboxDoc>(kv, ns::OUTBOX, schema::K).await? {
        if !about(&doc.value.payload) {
            continue;
        }
        let mut current = doc;
        for _ in 0..KV_RETRIES {
            let mut next = current.value.clone();
            if let Some(o) = next.payload.as_object_mut() {
                o.remove("admin_email");
            }
            let w = kv::write(
                kv,
                vec![kv::put_op(ns::OUTBOX, &key, &next, Expect::Version(current.version), Ttl::Forever, false)],
            )
            .await?;
            if w.first().is_some_and(|w| w.applied) {
                redacted += 1;
                break;
            }
            // A drain marked it consumed in between: re-read and redact that.
            match doc_at::<OutboxDoc>(kv, ns::OUTBOX, &key).await? {
                Some(d) if about(&d.value.payload) => current = d,
                _ => break,
            }
        }
    }
    Ok(redacted)
}

// ===========================================================================
// usage reads (px.usage.min / px.usage.day, written by the metering)
// ===========================================================================

/// Every `px.usage.min` row of `cluster` with `from_us <= minute < to_us`,
/// as `(minute_us, op_class, doc)` — one per NODE; callers sum. Keys are
/// `#<cluster>/<minute_us>/<op_class>/<node>`; the walk starts at `from_us`
/// when the minute segment's width can be read off the first key (plain or
/// zero-padded, both sort numerically at a fixed width), else at the prefix.
async fn usage_minute_rows(
    kv: &dyn KvBackend,
    cluster: Uuid,
    from_us: i64,
    to_us: i64,
) -> Result<Vec<(i64, String, UsageDoc)>, WebError> {
    let prefix = schema::prefix(cluster);
    let after = match first_key(kv, ns::USAGE_MIN, &prefix).await? {
        Some(first) => usage_after(&prefix, &first, from_us),
        None => return Ok(Vec::new()),
    };
    let mut out = Vec::new();
    for (k, d) in scan_from::<UsageDoc>(kv, ns::USAGE_MIN, &prefix, after).await? {
        let Some((minute, op)) = parse_usage_key(&k, &prefix) else {
            continue;
        };
        if minute < from_us || minute >= to_us {
            continue;
        }
        out.push((minute, op, d.value));
    }
    Ok(out)
}

/// `(minute_us, op_class)` of a `px.usage.min` key under `prefix`.
fn parse_usage_key(key: &str, prefix: &str) -> Option<(i64, String)> {
    let mut parts = key.strip_prefix(prefix)?.split('/');
    let minute = parts.next()?.parse::<i64>().ok()?;
    let op = parts.next()?;
    Some((minute, op.to_string()))
}

/// The `after` key that starts a walk at `from_us`, formatted like `sample`'s
/// minute segment; `None` (walk everything) when that cannot be done safely.
fn usage_after(prefix: &str, sample: &str, from_us: i64) -> Option<String> {
    let seg = sample.strip_prefix(prefix)?.split('/').next()?;
    if seg.is_empty() || !seg.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let bound = from_us.checked_sub(1)?.max(0);
    let formatted = if seg.len() > 1 && seg.starts_with('0') {
        format!("{bound:0width$}", width = seg.len())
    } else {
        bound.to_string()
    };
    (formatted.len() == seg.len()).then(|| format!("{prefix}{formatted}"))
}

/// `cluster_month_msgs` (004), KV side: per UTC day of the month containing
/// `now_us`, the larger of the rolled-up `px.usage.day` total and the live
/// `px.usage.min` total (summed over op classes and nodes) — never
/// double-counting a day, never under-counting today.
async fn cluster_month_msgs(kv: &dyn KvBackend, cluster: Uuid, now_us: i64) -> Result<i64, WebError> {
    let (start, end) = month_bounds_us(now_us);
    let prefix = schema::prefix(cluster);
    let mut days: BTreeMap<String, (i64, i64)> = BTreeMap::new();
    let month_prefix = format!("{prefix}{}-", utc_month(now_us));
    for (k, d) in kv::scan::<UsageDoc>(kv, ns::USAGE_DAY, &month_prefix).await? {
        let Some(day) = k.strip_prefix(&prefix).and_then(|r| r.split('/').next()) else {
            continue;
        };
        days.entry(day.to_string()).or_default().0 += d.value.msgs;
    }
    for (minute, _, d) in usage_minute_rows(kv, cluster, start, end).await? {
        days.entry(utc_day(minute)).or_default().1 += d.msgs;
    }
    Ok(days.values().map(|(rolled, live)| (*rolled).max(*live)).sum())
}

// ===========================================================================
// ---- data-plane reads/writes used by the web plane (dedupe with data.rs at merge)
//
// Tables owned by W3 (tenants, cells, plans, clusters, cluster_roles,
// api_keys, revoked_tokens, queues), in schema.rs's layout exactly. Repo
// functions: grant_cluster_role, revoke_cluster_role, issue_api_key,
// api_key_active_on_cluster, revoke_api_key. KV planners (one batch's worth
// of ops, validation included): plan_create_tenant, plan_create_cluster,
// plan_create_user, plan_set_user_name, plan_grant_role, plan_revoke_role,
// plan_issue_key. KV reads: user_by_email, tenant_by_slug, cluster_by_slug,
// plan_by_code, role_doc, cluster_member_roles, cluster_key_docs,
// tenant_clusters, cell_clusters, tenant_on_cell, tenants_and_cells.
// ===========================================================================

/// `queen_proxy.grant_cluster_role(cluster, email, role)` (004): upsert the
/// role, same-tenant rule enforced, audited as `cluster_role_granted`.
pub async fn grant_cluster_role(store: &Store, cluster_id: Uuid, email: &str, role: &str) -> Result<(), WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            client
                .execute(
                    "SELECT queen_proxy.grant_cluster_role($1::text::uuid, $2, $3)",
                    &[&cluster_id.to_string(), &email, &role],
                )
                .await
                .map_err(pg_err)?;
            Ok(())
        }
        Store::Kv(_) => crate::store::data::grant_cluster_role(store, cluster_id, email, role).await.map_err(WebError::from),
        Store::None => Err(WebError::NotConfigured),
    }
}

/// `queen_proxy.revoke_cluster_role(cluster, email)` (004): raises when there
/// is no grant to remove; audited as `cluster_role_revoked`.
pub async fn revoke_cluster_role(store: &Store, cluster_id: Uuid, email: &str) -> Result<(), WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            client
                .execute(
                    "SELECT queen_proxy.revoke_cluster_role($1::text::uuid, $2)",
                    &[&cluster_id.to_string(), &email],
                )
                .await
                .map_err(pg_err)?;
            Ok(())
        }
        Store::Kv(_) => crate::store::data::revoke_cluster_role(store, cluster_id, email).await.map_err(WebError::from),
        Store::None => Err(WebError::NotConfigured),
    }
}

const ISSUE_KEY_SQL: &str = "SELECT queen_proxy.issue_api_key($1::text::uuid, $2, $3, $4)::text AS id";

/// `queen_proxy.issue_api_key(cluster, name, key_hash, scopes)` (002): the
/// hash only, never the plaintext. Returns the key id.
pub async fn issue_api_key(
    store: &Store,
    cluster_id: Uuid,
    name: &str,
    key_hash: &str,
    scopes: &[String],
) -> Result<String, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let row = client
                .query_one(ISSUE_KEY_SQL, &[&cluster_id.to_string(), &name, &key_hash, &scopes])
                .await
                .map_err(pg_err)?;
            Ok(row.get::<_, String>(0))
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let Some(c) = by_id::<ClusterDoc>(kv, ns::CLUSTERS, cluster_id).await? else {
                return Err(raised(format!("issue_api_key: unknown cluster {cluster_id}")));
            };
            let mut tx = Tx::default();
            let id = plan_issue_key(&mut tx, &c.value, name, key_hash, scopes, None)?;
            commit(kv, tx).await?;
            Ok(id.to_string())
        }
        Store::None => Err(WebError::NotConfigured),
    }
}

/// Is `key_id` a live (unrevoked) key of `cluster_id`? `revoke_api_key`
/// looks keys up GLOBALLY, so this is the console's ownership check.
pub async fn api_key_active_on_cluster(store: &Store, key_id: &str, cluster_id: Uuid) -> Result<bool, WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            let row = client
                .query_opt(
                    "SELECT 1 FROM queen_proxy.api_keys \
                     WHERE id = $1::text::uuid AND cluster_id = $2::text::uuid AND revoked_at IS NULL",
                    &[&key_id, &cluster_id.to_string()],
                )
                .await
                .map_err(pg_err)?;
            Ok(row.is_some())
        }
        Store::Kv(kv) => {
            let id = Uuid::parse_str(key_id).map_err(|_| WebError::Db(format!("invalid uuid {key_id:?}")))?;
            Ok(by_id::<ApiKeyDoc>(kv.as_ref(), ns::KEYS, id)
                .await?
                .is_some_and(|k| k.value.cluster_id == cluster_id && k.value.revoked_at_us.is_none()))
        }
        Store::None => Err(WebError::NotConfigured),
    }
}

/// `queen_proxy.revoke_api_key(key)` (002): stamps `revoked_at`; raises on an
/// unknown or already-revoked key. The hash index entry stays (as the
/// Postgres row does): `by_key_hash` reads `revoked_at` on the row.
pub async fn revoke_api_key(store: &Store, key_id: &str) -> Result<(), WebError> {
    match store {
        Store::Pg(pool) => {
            let client = pg(pool).await?;
            client.execute("SELECT queen_proxy.revoke_api_key($1::text::uuid)", &[&key_id]).await.map_err(pg_err)?;
            Ok(())
        }
        // data.rs's write carries the cross-node cache invalidation in its
        // batch: a revoked key stops working on every node, not just this one.
        Store::Kv(_) => {
            let id = uuid::Uuid::parse_str(key_id).map_err(|e| WebError::Raised(e.to_string()))?;
            crate::store::data::revoke_api_key(store, id).await.map_err(WebError::from)
        }
        Store::None => Err(WebError::NotConfigured),
    }
}

// ---- KV planners: validate like the stored function, push its writes -------

/// `create_tenant(slug, name)` (002).
fn plan_create_tenant(tx: &mut Tx, slug: &str, name: &str) -> Result<TenantDoc, WebError> {
    if slug.trim().is_empty() {
        return Err(raised("create_tenant: slug must not be empty"));
    }
    if name.trim().is_empty() {
        return Err(raised("create_tenant: name must not be empty"));
    }
    let slug = slug.trim().to_lowercase();
    if !dns_label(&slug) {
        return Err(WebError::Db(format!(
            "new row for relation \"tenants\" violates check constraint \"tenants_slug_check\" ({slug})"
        )));
    }
    let doc = TenantDoc {
        id: Uuid::new_v4(),
        slug,
        name: name.trim().to_string(),
        status: "active".to_string(),
        created_at_us: now_us(),
    };
    tx.put(ns::TENANTS, schema::key(doc.id), &doc);
    tx.unique(ns::TENANT_SLUG, schema::key(&doc.slug), &doc.id, "tenants_slug_key");
    tx.record(&Audit {
        tenant_id: doc.id,
        cluster_id: None,
        actor: "control_plane",
        actor_id: None,
        action: "tenant_created",
        target: Some(doc.id.to_string()),
        meta: json!({ "slug": doc.slug, "name": doc.name }),
    })?;
    Ok(doc)
}

/// `create_cluster(tenant, slug, plan_code, cell)` (002), plan and cell
/// already resolved by the caller.
fn plan_create_cluster(
    tx: &mut Tx,
    tenant: &TenantDoc,
    slug: &str,
    plan: &PlanDoc,
    cell: Uuid,
    plan_code: &str,
) -> Result<ClusterDoc, WebError> {
    if slug.trim().is_empty() {
        return Err(raised("create_cluster: slug must not be empty"));
    }
    let slug = slug.trim().to_lowercase();
    if !dns_label(&slug) {
        return Err(WebError::Db(format!(
            "new row for relation \"clusters\" violates check constraint \"clusters_slug_check\" ({slug})"
        )));
    }
    let doc = ClusterDoc {
        id: Uuid::new_v4(),
        tenant_id: tenant.id,
        cell_id: cell,
        plan_id: plan.id,
        slug,
        broker_tenant_uuid: Uuid::new_v4(),
        status: "active".to_string(),
        limit_overrides: json!({}),
        created_at_us: now_us(),
    };
    tx.put(ns::CLUSTERS, schema::key(doc.id), &doc);
    tx.unique(ns::CLUSTER_SLUG, schema::key(&doc.slug), &doc.id, "clusters_slug_key");
    tx.put(ns::CLUSTER_TENANT, schema::key2(tenant.id, doc.id), &ENTRY);
    tx.put(ns::CLUSTER_CELL, schema::key2(cell, doc.id), &ENTRY);
    tx.record(&Audit {
        tenant_id: tenant.id,
        cluster_id: Some(doc.id),
        actor: "control_plane",
        actor_id: None,
        action: "cluster_created",
        target: Some(doc.id.to_string()),
        meta: json!({ "slug": doc.slug, "plan_code": plan_code, "cell_id": cell }),
    })?;
    Ok(doc)
}

/// `create_user(tenant, email, password_hash, provider)` (002). `name` is
/// what an immediately following `set_user_name` would write (the operator
/// path); create_user itself never sets one.
fn plan_create_user(
    tx: &mut Tx,
    tenant: &TenantDoc,
    email: &str,
    password_hash: Option<String>,
    provider: &str,
    name: Option<String>,
) -> Result<UserDoc, WebError> {
    if !email.contains('@') {
        return Err(raised(format!("create_user: invalid email {email}")));
    }
    if !PROVIDERS.contains(&provider) {
        return Err(raised(format!("create_user: invalid provider {provider}")));
    }
    let doc = UserDoc {
        id: Uuid::new_v4(),
        tenant_id: tenant.id,
        email: email.trim().to_lowercase(),
        password_hash,
        name,
        is_operator: false,
        last_login_at_us: None,
        created_at_us: now_us(),
    };
    tx.put(ns::USERS, schema::key(doc.id), &doc);
    tx.unique(ns::USER_EMAIL, schema::key(&doc.email), &doc.id, "users_email_key");
    tx.put(ns::USER_TENANT, schema::key2(tenant.id, doc.id), &ENTRY);
    tx.record(&Audit {
        tenant_id: tenant.id,
        cluster_id: None,
        actor: "control_plane",
        actor_id: Some(doc.id),
        action: "user_created",
        target: Some(doc.id.to_string()),
        meta: json!({ "email": doc.email, "provider": provider }),
    })?;
    Ok(doc)
}

/// `set_user_name`'s rule (010): trimmed, 1 to 160 characters.
fn check_user_name(name: &str) -> Result<String, WebError> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 160 {
        return Err(raised("set_user_name: name must contain 1 to 160 characters"));
    }
    Ok(name.to_string())
}

/// `set_user_name(user, name)` (010) on a user read at `user.version`.
/// Returns the stored (trimmed) name.
fn plan_set_user_name(tx: &mut Tx, user: &Doc<UserDoc>, name: &str) -> Result<String, WebError> {
    let name = check_user_name(name)?;
    let mut doc = user.value.clone();
    let old = doc.name.replace(name.clone());
    tx.fresh(ns::USERS, schema::key(doc.id), &doc, Some(user.version));
    tx.record(&Audit {
        tenant_id: doc.tenant_id,
        cluster_id: None,
        actor: "control_plane",
        actor_id: Some(doc.id),
        action: "user_name_changed",
        target: Some(doc.id.to_string()),
        meta: json!({ "old_name": old, "name": name }),
    })?;
    Ok(name)
}

/// `grant_cluster_role` (004) once the cluster and user are resolved:
/// same-tenant rule, upsert (the original `created_at` is kept), audit.
fn plan_grant_role(
    tx: &mut Tx,
    cluster: &ClusterDoc,
    user: &UserDoc,
    role: &str,
    existing: Option<Doc<RoleDoc>>,
) -> Result<(), WebError> {
    if !ROLES.contains(&role) {
        return Err(raised(format!("grant_cluster_role: invalid role {role}")));
    }
    if user.tenant_id != cluster.tenant_id {
        return Err(raised(format!(
            "grant_cluster_role: user {} belongs to tenant {}, cluster {} to tenant {}",
            user.email, user.tenant_id, cluster.id, cluster.tenant_id
        )));
    }
    let (created_at_us, version) = match &existing {
        Some(d) => (d.value.created_at_us, Some(d.version)),
        None => (now_us(), None),
    };
    let doc = RoleDoc { user_id: user.id, cluster_id: cluster.id, role: role.to_string(), created_at_us };
    tx.fresh(ns::ROLES, schema::key2(user.id, cluster.id), &doc, version);
    tx.put(ns::ROLE_CLUSTER, schema::key2(cluster.id, user.id), &ENTRY);
    tx.record(&Audit {
        tenant_id: cluster.tenant_id,
        cluster_id: Some(cluster.id),
        actor: "control_plane",
        actor_id: Some(user.id),
        action: "cluster_role_granted",
        target: Some(cluster.id.to_string()),
        meta: json!({ "email": user.email, "role": role }),
    })?;
    Ok(())
}

/// `revoke_cluster_role` (004) once the grant is resolved.
fn plan_revoke_role(
    tx: &mut Tx,
    cluster: &ClusterDoc,
    user: &UserDoc,
    existing: &Doc<RoleDoc>,
) -> Result<(), WebError> {
    tx.del_fresh(ns::ROLES, schema::key2(user.id, cluster.id), existing.version);
    tx.del(ns::ROLE_CLUSTER, schema::key2(cluster.id, user.id));
    tx.record(&Audit {
        tenant_id: cluster.tenant_id,
        cluster_id: Some(cluster.id),
        actor: "control_plane",
        actor_id: Some(user.id),
        action: "cluster_role_revoked",
        target: Some(cluster.id.to_string()),
        meta: json!({ "email": user.email, "role": existing.value.role }),
    })?;
    Ok(())
}

/// `issue_api_key` (002) once the cluster is resolved. Returns the key id.
fn plan_issue_key(
    tx: &mut Tx,
    cluster: &ClusterDoc,
    name: &str,
    key_hash: &str,
    scopes: &[String],
    created_by: Option<Uuid>,
) -> Result<Uuid, WebError> {
    let name = name.trim();
    if name.is_empty() {
        return Err(raised("issue_api_key: name must not be empty"));
    }
    if key_hash.len() != 64 || !key_hash.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
        return Err(raised("issue_api_key: key_hash must be 64 lowercase hex chars (sha256)"));
    }
    if scopes.is_empty() {
        return Err(raised("issue_api_key: at least one scope is required"));
    }
    if let Some(bad) = scopes.iter().find(|s| !SCOPES.contains(&s.as_str())) {
        return Err(raised(format!("issue_api_key: invalid scope {bad}")));
    }
    let id = Uuid::new_v4();
    let doc = ApiKeyDoc {
        id,
        cluster_id: cluster.id,
        name: name.to_string(),
        key_hash: key_hash.to_string(),
        scopes: scopes.to_vec(),
        created_by,
        created_at_us: now_us(),
        last_used_at_us: None,
        revoked_at_us: None,
    };
    tx.put(ns::KEYS, schema::key(id), &doc);
    tx.unique(ns::KEY_HASH, schema::key(key_hash), &id, "api_keys_key_hash_key");
    tx.put(ns::KEY_CLUSTER, schema::key2(cluster.id, id), &ENTRY);
    tx.record(&Audit {
        tenant_id: cluster.tenant_id,
        cluster_id: Some(cluster.id),
        actor: "control_plane",
        actor_id: None,
        action: "api_key_issued",
        target: Some(id.to_string()),
        meta: json!({ "name": name, "scopes": scopes }),
    })?;
    Ok(id)
}

// ---- KV reads ---------------------------------------------------------------

/// The user an (already normalized) email names, through `px.users.email`.
async fn user_by_email(kv: &dyn KvBackend, email: &str) -> Result<Option<Doc<UserDoc>>, WebError> {
    let Some(id) = index_target(kv, ns::USER_EMAIL, &schema::key(email)).await? else {
        return Ok(None);
    };
    Ok(by_id::<UserDoc>(kv, ns::USERS, id).await?.filter(|u| u.value.email == email))
}

async fn tenant_by_slug(kv: &dyn KvBackend, slug: &str) -> Result<Option<Doc<TenantDoc>>, WebError> {
    let Some(id) = index_target(kv, ns::TENANT_SLUG, &schema::key(slug)).await? else {
        return Ok(None);
    };
    by_id(kv, ns::TENANTS, id).await
}

async fn cluster_by_slug(kv: &dyn KvBackend, slug: &str) -> Result<Option<Doc<ClusterDoc>>, WebError> {
    let Some(id) = index_target(kv, ns::CLUSTER_SLUG, &schema::key(slug)).await? else {
        return Ok(None);
    };
    by_id(kv, ns::CLUSTERS, id).await
}

async fn plan_by_code(kv: &dyn KvBackend, code: &str) -> Result<Option<Doc<PlanDoc>>, WebError> {
    let Some(id) = index_target(kv, ns::PLAN_CODE, &schema::key(code)).await? else {
        return Ok(None);
    };
    by_id(kv, ns::PLANS, id).await
}

/// `cluster_roles` PK lookup.
async fn role_doc(kv: &dyn KvBackend, user_id: Uuid, cluster_id: Uuid) -> Result<Option<Doc<RoleDoc>>, WebError> {
    doc_at(kv, ns::ROLES, &schema::key2(user_id, cluster_id)).await
}

/// Every role row on a cluster (through `px.roles.cluster`).
async fn cluster_member_roles(kv: &dyn KvBackend, cluster_id: Uuid) -> Result<Vec<Doc<RoleDoc>>, WebError> {
    let keys: Vec<String> =
        children(kv, ns::ROLE_CLUSTER, cluster_id).await?.into_iter().map(|u| schema::key2(u, cluster_id)).collect();
    Ok(kv::get_many::<RoleDoc>(kv, ns::ROLES, &keys).await?.into_iter().flatten().collect())
}

fn admin_count(roles: &[Doc<RoleDoc>]) -> i64 {
    roles.iter().filter(|r| r.value.role == "admin").count() as i64
}

/// Every API key row of a cluster, revoked ones included.
async fn cluster_key_docs(kv: &dyn KvBackend, cluster_id: Uuid) -> Result<Vec<Doc<ApiKeyDoc>>, WebError> {
    let ids = children(kv, ns::KEY_CLUSTER, cluster_id).await?;
    many(kv, ns::KEYS, &ids).await
}

async fn tenant_clusters(kv: &dyn KvBackend, tenant_id: Uuid) -> Result<Vec<ClusterDoc>, WebError> {
    let ids = children(kv, ns::CLUSTER_TENANT, tenant_id).await?;
    Ok(many::<ClusterDoc>(kv, ns::CLUSTERS, &ids).await?.into_iter().map(|d| d.value).collect())
}

async fn cell_clusters(kv: &dyn KvBackend, cell_id: Uuid) -> Result<Vec<ClusterDoc>, WebError> {
    let ids = children(kv, ns::CLUSTER_CELL, cell_id).await?;
    Ok(many::<ClusterDoc>(kv, ns::CLUSTERS, &ids).await?.into_iter().map(|d| d.value).collect())
}

/// Does the tenant own a cluster on this cell (the operator's cell boundary)?
async fn tenant_on_cell(kv: &dyn KvBackend, tenant_id: Uuid, cell_id: Uuid) -> Result<bool, WebError> {
    Ok(tenant_clusters(kv, tenant_id).await?.iter().any(|c| c.cell_id == cell_id))
}

/// The tenant and cell rows the given clusters name, by id.
async fn tenants_and_cells<'a>(
    kv: &dyn KvBackend,
    clusters: impl Iterator<Item = &'a ClusterDoc>,
) -> Result<(HashMap<Uuid, TenantDoc>, HashMap<Uuid, CellDoc>), WebError> {
    let (mut t, mut c): (HashSet<Uuid>, HashSet<Uuid>) = (HashSet::new(), HashSet::new());
    for cl in clusters {
        t.insert(cl.tenant_id);
        c.insert(cl.cell_id);
    }
    let t: Vec<Uuid> = t.into_iter().collect();
    let c: Vec<Uuid> = c.into_iter().collect();
    let tenants = many::<TenantDoc>(kv, ns::TENANTS, &t).await?.into_iter().map(|d| (d.value.id, d.value)).collect();
    let cells = many::<CellDoc>(kv, ns::CELLS, &c).await?.into_iter().map(|d| (d.value.id, d.value)).collect();
    Ok((tenants, cells))
}

// ===========================================================================
// tests: the KV side, against MemKv (the broker's KV semantics)
// ===========================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::store::memkv::MemKv;

    /// MemKv plus the broker's per-call rules MemKv does not check
    /// (server/src/rsm/planner/kv.rs `parse_ops`, HTTP ceilings): at most 256
    /// ops, at most ONE write per key (`kv_duplicate_key_in_call`), at most
    /// 4096 keys named. Every KV-side test below therefore also proves that
    /// the batches it drives are ones the broker would accept.
    struct Strict(MemKv);

    impl std::ops::Deref for Strict {
        type Target = MemKv;
        fn deref(&self) -> &MemKv {
            &self.0
        }
    }

    impl KvBackend for Strict {
        fn kv(&self, ops: Vec<Value>) -> kv::BoxFut<'_, Result<Vec<Value>, KvError>> {
            let mut written = HashSet::new();
            let mut named = 0usize;
            for o in &ops {
                let op = o["op"].as_str().unwrap_or("");
                named += match op {
                    "getMany" => o["keys"].as_array().map_or(0, Vec::len),
                    "getPrefix" => o["limit"].as_u64().unwrap_or(100).clamp(1, 1000) as usize,
                    _ => 1,
                };
                if matches!(op, "put" | "putIfAbsent" | "delete" | "incr") {
                    let k = format!("{}/{}", o["ns"].as_str().unwrap_or(""), o["key"].as_str().unwrap_or(""));
                    assert!(written.insert(k.clone()), "kv_duplicate_key_in_call: {k}");
                }
            }
            assert!(ops.len() <= 256, "kv_too_many_ops: {}", ops.len());
            assert!(named <= 4096, "kv_too_many_keys: {named}");
            self.0.kv(ops)
        }
    }

    fn store() -> (Arc<Strict>, Store) {
        let m = Arc::new(Strict(MemKv::new()));
        (m.clone(), Store::Kv(m))
    }

    /// A cell and the `free` plan, as the import (or an operator) seeds them.
    async fn seed(m: &MemKv, cell_slug: &str) -> Uuid {
        let cell = CellDoc {
            id: Uuid::new_v4(),
            slug: cell_slug.to_string(),
            region: "local".into(),
            base_url: "http://127.0.0.1:6632".into(),
            class: "shared".into(),
            capacity_slots: 10,
            used_slots: 0,
            broker_version: None,
            status: "active".into(),
            cell_secret: None,
            created_at_us: 1,
        };
        let mut ops = vec![
            kv::put_op(ns::CELLS, &schema::key(cell.id), &cell, Expect::Any, Ttl::Forever, false),
            kv::put_op(ns::CELL_SLUG, &schema::key(&cell.slug), &cell.id, Expect::Any, Ttl::Forever, false),
        ];
        if kv::get::<Uuid>(m, ns::PLAN_CODE, "#free").await.unwrap().is_none() {
            let plan = PlanDoc {
                id: Uuid::new_v4(),
                code: "free".into(),
                cell_class: "shared".into(),
                monthly_msgs_quota: Some(1_000_000),
                features: json!({"kv": true}),
                ..Default::default()
            };
            ops.push(kv::put_op(ns::PLANS, &schema::key(plan.id), &plan, Expect::Any, Ttl::Forever, false));
            ops.push(kv::put_op(ns::PLAN_CODE, "#free", &plan.id, Expect::Any, Ttl::Forever, false));
        }
        kv::write(m, ops).await.unwrap();
        cell.id
    }

    fn boot<'a>(
        tenant: &'a str,
        cluster: &'a str,
        cell: Uuid,
        email: &'a str,
        password: Option<&'a str>,
    ) -> Bootstrap<'a> {
        Bootstrap {
            tenant_slug: tenant,
            tenant_name: None,
            cluster_slug: cluster,
            plan_code: "free",
            cell,
            admin_email: email,
            password,
            key_name: "default",
        }
    }

    fn uid(v: &Value, field: &str) -> Uuid {
        Uuid::parse_str(v[field].as_str().unwrap_or_else(|| panic!("{field} missing in {v}"))).unwrap()
    }

    async fn ops_of(st: &Store, tenant: Uuid) -> Vec<String> {
        list_operations(st, tenant, None, 1000).await.unwrap().into_iter().map(|o| o.action).collect()
    }

    fn no_operator(current: Option<&str>, admins: i64, new: Option<&str>) -> bool {
        current == Some("admin") && new != Some("admin") && admins <= 1
    }

    // ---- signup / bootstrap -------------------------------------------------

    #[tokio::test]
    async fn bootstrap_creates_the_tenant_once_and_is_idempotent() {
        let (m, st) = store();
        let cell = seed(&m, "local").await;
        let b = Bootstrap {
            tenant_name: Some(" Acme Inc "),
            ..boot("Acme", "acme-c", cell, " Admin@Acme.io ", Some("correct-horse-battery"))
        };
        let first = bootstrap_tenant(&st, &b).await.unwrap();
        let (tenant, cluster, user) = (uid(&first, "tenant_id"), uid(&first, "cluster_id"), uid(&first, "user_id"));
        let key = first["api_key"].as_str().unwrap().to_string();
        assert!(key.starts_with("qk_live_"), "{key}");
        assert_eq!(first["password_set"], true);
        assert_eq!(first["can_login"], true);

        let t = kv::get::<TenantDoc>(m.as_ref(), ns::TENANTS, &schema::key(tenant)).await.unwrap().unwrap().value;
        assert_eq!((t.slug.as_str(), t.name.as_str(), t.status.as_str()), ("acme", "Acme Inc", "active"));
        let c = kv::get::<ClusterDoc>(m.as_ref(), ns::CLUSTERS, &schema::key(cluster)).await.unwrap().unwrap().value;
        assert_eq!((c.tenant_id, c.cell_id, c.slug.as_str()), (tenant, cell, "acme-c"));
        assert_eq!(m.keys(ns::CLUSTER_TENANT), vec![schema::key2(tenant, cluster)]);
        assert_eq!(m.keys(ns::CLUSTER_CELL), vec![schema::key2(cell, cluster)]);

        // the admin: lowercased, bcrypt'd, admin on the cluster
        let (who, hash) = user_login_by_email(&st, "ADMIN@acme.io").await.unwrap().unwrap();
        assert_eq!(who, UserRef { user_id: user, tenant_id: tenant });
        assert!(bcrypt::verify("correct-horse-battery", &hash.unwrap()).unwrap());
        assert_eq!(member_standing(&st, cluster, &user.to_string()).await.unwrap(), (Some("admin".into()), 1));

        // the key: hash-indexed, full scopes, never the plaintext
        let kid = kv::get::<Uuid>(m.as_ref(), ns::KEY_HASH, &schema::key(crate::auth::key_hash_hex(&key)))
            .await
            .unwrap()
            .unwrap()
            .value;
        let keys = list_cluster_keys(&st, cluster).await.unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].id, kid.to_string());
        assert_eq!(keys[0].scopes, all_scopes());

        // audit, newest first, and ONE signup event
        assert_eq!(
            ops_of(&st, tenant).await,
            [
                "tenant_bootstrapped",
                "api_key_issued",
                "cluster_role_granted",
                "user_created",
                "cluster_created",
                "tenant_created"
            ]
        );
        let outbox = kv::scan::<OutboxDoc>(m.as_ref(), ns::OUTBOX, "#").await.unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(outbox[0].1.value.kind, "tenant_bootstrapped");
        assert_eq!(outbox[0].1.value.payload["admin_email"], "admin@acme.io");

        // a re-run: same ids, no second key, no second signup, the grant re-asserted
        let again = bootstrap_tenant(&st, &b).await.unwrap();
        assert_eq!(
            (uid(&again, "tenant_id"), uid(&again, "cluster_id"), uid(&again, "user_id")),
            (tenant, cluster, user)
        );
        assert!(again["api_key"].is_null());
        assert_eq!(list_cluster_keys(&st, cluster).await.unwrap().len(), 1);
        assert_eq!(kv::scan::<OutboxDoc>(m.as_ref(), ns::OUTBOX, "#").await.unwrap().len(), 1);
        assert_eq!(ops_of(&st, tenant).await[..2], ["tenant_bootstrapped", "cluster_role_granted"]);
        assert_eq!(list_cluster_members(&st, cluster).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn bootstrap_without_a_password_says_it_cannot_log_in() {
        let (m, st) = store();
        let cell = seed(&m, "local").await;
        let r = bootstrap_tenant(&st, &boot("keyonly", "keyonly-c", cell, "ops@keyonly.io", None)).await.unwrap();
        assert_eq!(r["password_set"], false);
        assert_eq!(r["can_login"], false);
        assert!(r["api_key"].as_str().is_some());
        let tenant = uid(&r, "tenant_id");
        let meta = &list_operations(&st, tenant, None, 1).await.unwrap()[0].meta;
        assert_eq!(meta["can_login"], false, "the durable copy of the warning");
    }

    #[tokio::test]
    async fn bootstrap_refuses_other_tenants_slugs_and_addresses_writing_nothing() {
        let (m, st) = store();
        let cell = seed(&m, "local").await;
        let a = bootstrap_tenant(&st, &boot("a", "a-c", cell, "boss@a.io", Some("pw-pw-pw-pw-pw"))).await.unwrap();
        let tenants_before = m.keys(ns::TENANTS).len();

        let e = bootstrap_tenant(&st, &boot("b", "a-c", cell, "boss@b.io", None)).await.unwrap_err();
        assert!(matches!(&e, WebError::Raised(m) if m.contains("cluster slug a-c already belongs to tenant")), "{e}");
        let e = bootstrap_tenant(&st, &boot("b", "b-c", cell, "BOSS@a.io", None)).await.unwrap_err();
        assert!(matches!(&e, WebError::Raised(m) if m.contains("user boss@a.io already belongs to tenant")), "{e}");
        assert_eq!(m.keys(ns::TENANTS).len(), tenants_before, "a refused bootstrap writes nothing");
        assert!(!m.keys(ns::TENANT_SLUG).contains(&"#b".to_string()));

        let e = bootstrap_tenant(&st, &Bootstrap { plan_code: "gold", ..boot("c", "c-c", cell, "x@c.io", None) })
            .await
            .unwrap_err();
        assert_eq!(e, WebError::Raised("create_cluster: unknown plan code gold".into()));
        let e = bootstrap_tenant(&st, &boot("Not A Slug", "d-c", cell, "x@d.io", None)).await.unwrap_err();
        assert!(matches!(e, WebError::Db(_)), "the slug CHECK");
        assert_eq!(list_cluster_keys(&st, uid(&a, "cluster_id")).await.unwrap().len(), 1, "A untouched");
    }

    // ---- duplicate email ---------------------------------------------------

    #[tokio::test]
    async fn an_operator_created_user_is_all_or_nothing_and_emails_are_unique() {
        let (m, st) = store();
        let cell = seed(&m, "local").await;
        let a = bootstrap_tenant(&st, &boot("acme", "acme-c", cell, "admin@acme.io", None)).await.unwrap();
        let (tenant, cluster, admin) = (uid(&a, "tenant_id"), uid(&a, "cluster_id"), uid(&a, "user_id"));
        let new = NewUser {
            tenant_id: tenant,
            cluster_id: cluster,
            email: "dev@acme.io",
            name: "Ada Lovelace",
            provider: "local",
            password_hash: Some(bcrypt::hash("x", 4).unwrap()),
            role: "producer",
        };
        let id = operator_create_user(&st, cluster, &new, admin).await.unwrap();
        let listing = operator_listing(&st, cluster).await.unwrap();
        let row = listing.users.iter().find(|u| u.id == id).unwrap();
        assert_eq!(
            (row.name.as_deref(), row.has_local_password, row.tenant_slug.as_str()),
            (Some("Ada Lovelace"), true, "acme")
        );
        assert!(listing.roles.iter().any(|r| r.user_id == id && r.role == "producer"));
        assert_eq!(
            ops_of(&st, tenant).await[..4],
            ["operator_user_created", "cluster_role_granted", "user_name_changed", "user_created"]
        );

        let users_before = m.keys(ns::USERS).len();
        let again = operator_create_user(&st, cluster, &NewUser { email: "DEV@acme.io", ..new.clone() }, admin).await;
        assert_eq!(again, Err(Refusal::Conflict("a user with this email already exists")));
        assert_eq!(m.keys(ns::USERS).len(), users_before, "the losing batch wrote nothing");

        // A tenant/cluster pair from another cell is not this operator's.
        let other_cell = seed(&m, "far").await;
        let b = bootstrap_tenant(&st, &boot("far", "far-c", other_cell, "admin@far.io", None)).await.unwrap();
        let far = NewUser {
            tenant_id: uid(&b, "tenant_id"),
            cluster_id: uid(&b, "cluster_id"),
            email: "x@far.io",
            ..new.clone()
        };
        assert_eq!(
            operator_create_user(&st, cluster, &far, admin).await,
            Err(Refusal::NotFound("tenant and cluster are not on this cell"))
        );
        assert_eq!(operator_listing(&st, cluster).await.unwrap().tenants.len(), 1, "the cell boundary");
    }

    // ---- OAuth identity link + provisioning ----------------------------------

    #[tokio::test]
    async fn an_oauth_identity_links_once_and_logs_in_as_its_user() {
        let (m, st) = store();
        let cell = seed(&m, "local").await;
        let a = bootstrap_tenant(&st, &boot("acme", "acme-c", cell, "admin@acme.io", None)).await.unwrap();
        let me = UserRef { user_id: uid(&a, "user_id"), tenant_id: uid(&a, "tenant_id") };

        assert_eq!(find_user_by_identity(&st, "github", "4242").await.unwrap(), None);
        assert_eq!(find_user_by_email(&st, "Admin@Acme.io").await.unwrap(), Some(me.clone()));
        link_identity(&st, &me, "github", "4242", "Admin@Acme.io").await.unwrap();
        assert_eq!(find_user_by_identity(&st, "github", "4242").await.unwrap(), Some(me.clone()));

        // ON CONFLICT DO NOTHING: the first link wins, whoever asks next.
        let two = provision_oauth_user(&st, "acme", "two@acme.io", "google", "g-2", "viewer").await.unwrap();
        link_identity(&st, &me, "github", "4242", "admin@acme.io").await.unwrap();
        link_identity(&st, &two.user, "github", "4242", "two@acme.io").await.unwrap();
        assert_eq!(find_user_by_identity(&st, "github", "4242").await.unwrap(), Some(me.clone()));
        assert_eq!(m.keys(ns::IDENTITIES).len(), 2, "one row per linked identity, no duplicate");

        // The foreign key: an identity names a user that exists.
        let ghost = UserRef { user_id: Uuid::new_v4(), tenant_id: me.tenant_id };
        assert!(matches!(link_identity(&st, &ghost, "github", "9", "x@y.z").await, Err(WebError::Db(_))));

        let mine = kv::scan::<IdentityDoc>(m.as_ref(), ns::IDENTITIES, "#")
            .await
            .unwrap()
            .into_iter()
            .map(|(_, d)| d.value)
            .find(|i| i.provider == "github")
            .unwrap();
        assert_eq!((mine.user_id, mine.email.as_str(), mine.verified), (me.user_id, "admin@acme.io", true));
        assert!(m.keys(ns::IDENTITY_USER).contains(&schema::key2(me.user_id, mine.id)));
    }

    #[tokio::test]
    async fn oauth_provisioning_grants_the_default_role_on_every_cluster_of_the_tenant() {
        let (m, st) = store();
        let cell = seed(&m, "local").await;
        let a = bootstrap_tenant(&st, &boot("trial", "trial-1", cell, "admin@trial.io", None)).await.unwrap();
        let b = bootstrap_tenant(&st, &boot("trial", "trial-2", cell, "admin@trial.io", None)).await.unwrap();
        assert_eq!(uid(&a, "tenant_id"), uid(&b, "tenant_id"));

        let p = provision_oauth_user(&st, "trial", "New@Trial.io", "google", "g-1", "viewer").await.unwrap();
        assert_eq!(p.granted, 2);
        assert_eq!(p.user.tenant_id, uid(&a, "tenant_id"));
        assert_eq!(find_user_by_identity(&st, "google", "g-1").await.unwrap(), Some(p.user.clone()));
        let clusters = me_clusters(&st, p.user.user_id, false).await.unwrap();
        assert_eq!(
            clusters.iter().map(|c| (c.slug.as_str(), c.role.as_str())).collect::<Vec<_>>(),
            [("trial-1", "viewer"), ("trial-2", "viewer")]
        );

        let dup = provision_oauth_user(&st, "trial", "new@trial.io", "github", "gh-1", "viewer").await;
        assert!(matches!(dup, Err(ProvisionError::Db(ref m)) if m.contains("users_email_key")), "{dup:?}");
        assert_eq!(
            provision_oauth_user(&st, "nosuch", "z@z.io", "google", "g-9", "viewer").await,
            Err(ProvisionError::NoTenant)
        );
        assert!(
            find_user_by_identity(&st, "github", "gh-1").await.unwrap().is_none(),
            "the losing batch linked nothing"
        );
    }

    // ---- operator flag ---------------------------------------------------------

    #[tokio::test]
    async fn the_operator_flag_round_trips_and_is_audited_every_time() {
        let (m, st) = store();
        let cell = seed(&m, "local").await;
        let a = bootstrap_tenant(&st, &boot("acme", "acme-c", cell, "admin@acme.io", None)).await.unwrap();
        let (tenant, user) = (uid(&a, "tenant_id"), uid(&a, "user_id"));

        set_operator(&st, " Admin@ACME.io ", true).await.unwrap();
        assert!(me_user(&st, user).await.unwrap().unwrap().is_operator);
        let op = &list_operations(&st, tenant, None, 1).await.unwrap()[0];
        assert_eq!((op.action.as_str(), op.target.as_deref()), ("operator_granted", Some("admin@acme.io")));
        assert_eq!(op.meta, json!({"is_operator": true, "was": false}));

        set_operator(&st, "admin@acme.io", true).await.unwrap();
        assert_eq!(
            list_operations(&st, tenant, None, 1).await.unwrap()[0].meta["was"],
            true,
            "a repeat is audited too"
        );
        set_operator(&st, "admin@acme.io", false).await.unwrap();
        assert!(!me_user(&st, user).await.unwrap().unwrap().is_operator);
        assert_eq!(list_operations(&st, tenant, None, 1).await.unwrap()[0].action, "operator_revoked");

        assert_eq!(
            set_operator(&st, "nobody@acme.io", true).await,
            Err(WebError::Raised("set_operator: unknown user nobody@acme.io".into()))
        );
        assert_eq!(
            set_operator(&st, "no-at-sign", true).await,
            Err(WebError::Raised("set_operator: invalid email no-at-sign".into()))
        );
    }

    // ---- user rename -------------------------------------------------------------

    #[tokio::test]
    async fn an_operator_renames_a_user_of_the_cell_and_a_no_op_writes_nothing() {
        let (m, st) = store();
        let cell = seed(&m, "local").await;
        let a = bootstrap_tenant(&st, &boot("acme", "acme-c", cell, "admin@acme.io", None)).await.unwrap();
        let (tenant, cluster, user) = (uid(&a, "tenant_id"), uid(&a, "cluster_id"), uid(&a, "user_id"));
        let actor = Uuid::new_v4();

        assert_eq!(operator_rename_user(&st, cluster, user, "Grace Hopper", actor).await, Ok(Rename::Renamed));
        let row = operator_listing(&st, cluster).await.unwrap().users.remove(0);
        assert_eq!(row.name.as_deref(), Some("Grace Hopper"));
        let ops = list_operations(&st, tenant, None, 2).await.unwrap();
        assert_eq!(ops[0].action, "operator_user_updated");
        assert_eq!(ops[0].actor_id, Some(actor.to_string()));
        assert_eq!(ops[1].action, "user_name_changed");
        assert_eq!(ops[1].meta, json!({"old_name": null, "name": "Grace Hopper"}));

        let n = m.keys(ns::OPS).len();
        assert_eq!(operator_rename_user(&st, cluster, user, "Grace Hopper", actor).await, Ok(Rename::Unchanged));
        assert_eq!(m.keys(ns::OPS).len(), n, "unchanged: no write, no audit");

        let far_cell = seed(&m, "far").await;
        let b = bootstrap_tenant(&st, &boot("far", "far-c", far_cell, "admin@far.io", None)).await.unwrap();
        assert_eq!(
            operator_rename_user(&st, cluster, uid(&b, "user_id"), "X", actor).await,
            Err(Refusal::NotFound("user is not on this cell"))
        );
        assert_eq!(
            check_user_name(&"x".repeat(161)),
            Err(WebError::Raised("set_user_name: name must contain 1 to 160 characters".into()))
        );
        assert_eq!(check_user_name("  Ada  ").unwrap(), "Ada");
    }

    // ---- console key lifecycle ----------------------------------------------------

    #[tokio::test]
    async fn console_keys_are_issued_listed_owned_and_revoked_once() {
        let (m, st) = store();
        let cell = seed(&m, "local").await;
        let a = bootstrap_tenant(&st, &boot("acme", "acme-c", cell, "admin@acme.io", None)).await.unwrap();
        let b = bootstrap_tenant(&st, &boot("beta", "beta-c", cell, "admin@beta.io", None)).await.unwrap();
        let (tenant, cluster, other) = (uid(&a, "tenant_id"), uid(&a, "cluster_id"), uid(&b, "cluster_id"));

        let plaintext = crate::auth::generate_api_key("live");
        let hash = crate::auth::key_hash_hex(&plaintext);
        let scopes = vec!["produce".to_string(), "read".to_string()];
        let id = issue_api_key(&st, cluster, "  ci  ", &hash, &scopes).await.unwrap();
        let keys = list_cluster_keys(&st, cluster).await.unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!((keys[0].id.as_str(), keys[0].name.as_str()), (id.as_str(), "ci"), "newest first");
        assert_eq!(keys[0].scopes, scopes);
        assert!(keys[0].revoked_at.is_none());

        assert!(api_key_active_on_cluster(&st, &id, cluster).await.unwrap());
        assert!(!api_key_active_on_cluster(&st, &id, other).await.unwrap(), "ownership is per cluster");
        assert!(
            matches!(issue_api_key(&st, other, "dup", &hash, &scopes).await, Err(WebError::Conflict(_))),
            "the hash is UNIQUE"
        );
        assert!(matches!(
            issue_api_key(&st, cluster, "bad", &"a".repeat(64), &["sudo".to_string()]).await,
            Err(WebError::Raised(ref m)) if m == "issue_api_key: invalid scope sudo"
        ));
        assert!(matches!(issue_api_key(&st, cluster, "bad", "XYZ", &scopes).await, Err(WebError::Raised(_))));

        revoke_api_key(&st, &id).await.unwrap();
        assert!(!api_key_active_on_cluster(&st, &id, cluster).await.unwrap());
        assert!(list_cluster_keys(&st, cluster).await.unwrap()[0].revoked_at.is_some());
        assert_eq!(
            revoke_api_key(&st, &id).await,
            Err(WebError::Raised(format!("revoke_api_key: unknown or already-revoked key {id}")))
        );
        // The hash stays indexed (the row stays, revoked), as in Postgres.
        assert!(m.keys(ns::KEY_HASH).contains(&schema::key(&hash)));
        let ops = list_operations(&st, tenant, Some(cluster), 2).await.unwrap();
        assert_eq!((ops[0].action.as_str(), ops[1].action.as_str()), ("api_key_revoked", "api_key_issued"));
    }

    // ---- members (console) -----------------------------------------------------------

    #[tokio::test]
    async fn console_members_grant_and_revoke_within_the_tenant() {
        let (m, st) = store();
        let cell = seed(&m, "local").await;
        let a = bootstrap_tenant(&st, &boot("acme", "acme-c", cell, "zed@acme.io", None)).await.unwrap();
        let b = bootstrap_tenant(&st, &boot("beta", "beta-c", cell, "admin@beta.io", None)).await.unwrap();
        let (tenant, cluster) = (uid(&a, "tenant_id"), uid(&a, "cluster_id"));
        let p = provision_oauth_user(&st, "acme", "amy@acme.io", "google", "g", "viewer").await.unwrap();

        assert_eq!(user_id_in_tenant(&st, "amy@acme.io", tenant).await.unwrap(), Some(p.user.user_id.to_string()));
        assert_eq!(
            user_id_in_tenant(&st, "admin@beta.io", tenant).await.unwrap(),
            None,
            "another tenant's user reads as unknown"
        );
        grant_cluster_role(&st, cluster, "AMY@acme.io", "producer").await.unwrap();
        let members = list_cluster_members(&st, cluster).await.unwrap();
        assert_eq!(
            members.iter().map(|m| (m.email.as_str(), m.role.as_str())).collect::<Vec<_>>(),
            [("amy@acme.io", "producer"), ("zed@acme.io", "admin")]
        );
        assert_eq!(
            member_on_cluster(&st, cluster, "amy@acme.io").await.unwrap(),
            Some((p.user.user_id.to_string(), "producer".into(), 1))
        );

        let cross = grant_cluster_role(&st, uid(&b, "cluster_id"), "amy@acme.io", "viewer").await;
        assert!(matches!(cross, Err(WebError::Raised(ref m)) if m.contains("belongs to tenant")), "{cross:?}");
        assert!(matches!(grant_cluster_role(&st, cluster, "amy@acme.io", "owner").await, Err(WebError::Raised(_))));

        revoke_cluster_role(&st, cluster, "amy@acme.io").await.unwrap();
        assert_eq!(member_on_cluster(&st, cluster, "amy@acme.io").await.unwrap(), None);
        assert_eq!(
            revoke_cluster_role(&st, cluster, "amy@acme.io").await,
            Err(WebError::Raised(format!("revoke_cluster_role: user amy@acme.io has no role on cluster {cluster}")))
        );
        assert_eq!(ops_of(&st, tenant).await[..2], ["cluster_role_revoked", "cluster_role_granted"]);
    }

    #[tokio::test]
    async fn the_operator_cannot_orphan_a_cluster_and_the_guard_is_in_the_batch() {
        let (m, st) = store();
        let cell = seed(&m, "local").await;
        let a = bootstrap_tenant(&st, &boot("acme", "acme-c", cell, "one@acme.io", None)).await.unwrap();
        let (cluster, one) = (uid(&a, "cluster_id"), uid(&a, "user_id"));
        let actor = Uuid::new_v4();
        let demote = |u| operator_change_role(&st, cluster, u, cluster, Some("viewer"), no_operator, actor);
        assert_eq!(
            demote(one).await,
            Err(Refusal::BadRequest("cannot remove or demote the last admin of this cluster"))
        );

        let p = provision_oauth_user(&st, "acme", "two@acme.io", "google", "g2", "viewer").await.unwrap();
        let two = p.user.user_id;
        assert_eq!(operator_change_role(&st, cluster, two, cluster, None, no_operator, actor).await, Ok(()));
        assert_eq!(
            operator_change_role(&st, cluster, two, cluster, None, no_operator, actor).await,
            Err(Refusal::NotFound("user has no access to this cluster"))
        );
        assert_eq!(operator_change_role(&st, cluster, two, cluster, Some("admin"), no_operator, actor).await, Ok(()));
        assert_eq!(demote(one).await, Ok(()), "two admins: one may step down");
        assert_eq!(
            demote(two).await,
            Err(Refusal::BadRequest("cannot remove or demote the last admin of this cluster"))
        );
        let last = list_operations(&st, p.user.tenant_id, Some(cluster), 1).await.unwrap().remove(0);
        assert_eq!(last.action, "operator_role_granted");
        assert_eq!(last.meta, json!({"email": "one@acme.io", "role": "viewer", "previous_role": "admin"}));

        // The guard itself: a seat counted at version v is re-asserted at v.
        let mut tx = Tx::default();
        let seat = role_doc(m.as_ref(), two, cluster).await.unwrap().unwrap();
        tx.fresh(ns::ROLES, schema::key2(two, cluster), &seat.value, Some(seat.version));
        grant_cluster_role(&st, cluster, "two@acme.io", "admin").await.unwrap(); // bumps the version
        assert!(matches!(commit(m.as_ref(), tx).await, Err(WebError::Contended(_))));
    }

    // ---- operations audit ------------------------------------------------------

    #[tokio::test]
    async fn the_audit_lists_newest_first_per_tenant_and_per_cluster() {
        let (m, st) = store();
        let cell = seed(&m, "local").await;
        let a = bootstrap_tenant(&st, &boot("acme", "acme-c", cell, "admin@acme.io", None)).await.unwrap();
        let (tenant, cluster, user) = (uid(&a, "tenant_id"), uid(&a, "cluster_id"), uid(&a, "user_id"));
        for (i, c) in [(1, None), (2, Some(cluster)), (3, None)] {
            record_operation(
                &st,
                &Audit {
                    tenant_id: tenant,
                    cluster_id: c,
                    actor: "user",
                    actor_id: Some(user),
                    action: &format!("step_{i}"),
                    target: None,
                    meta: Value::Null,
                },
            )
            .await
            .unwrap();
        }
        let all = list_operations(&st, tenant, None, 3).await.unwrap();
        assert_eq!(all.iter().map(|o| o.action.as_str()).collect::<Vec<_>>(), ["step_3", "step_2", "step_1"]);
        assert_eq!(all[0].meta, json!({}), "a null meta is stored as {{}}");
        let on_cluster = list_operations(&st, tenant, Some(cluster), 1).await.unwrap();
        assert_eq!(
            (on_cluster[0].action.as_str(), on_cluster[0].cluster_id.clone()),
            ("step_2", Some(cluster.to_string()))
        );
        assert!(list_operations(&st, Uuid::new_v4(), None, 10).await.unwrap().is_empty());

        let bad = Audit {
            tenant_id: tenant,
            cluster_id: None,
            actor: "robot",
            actor_id: None,
            action: "x",
            target: None,
            meta: json!({}),
        };
        assert_eq!(
            record_operation(&st, &bad).await,
            Err(WebError::Raised("record_operation: invalid actor robot".into()))
        );
        let bad = Audit { actor: "system", action: "  ", ..bad };
        assert_eq!(
            record_operation(&st, &bad).await,
            Err(WebError::Raised("record_operation: action must not be empty".into()))
        );
    }

    // ---- sessions --------------------------------------------------------------------

    #[tokio::test]
    async fn logout_writes_a_deny_list_doc_that_dies_with_the_token() {
        let (m, st) = store();
        let cell = seed(&m, "local").await;
        let a = bootstrap_tenant(&st, &boot("acme", "acme-c", cell, "admin@acme.io", Some("pw-pw-pw-pw-pw")))
            .await
            .unwrap();
        let (tenant, user) = (uid(&a, "tenant_id"), uid(&a, "user_id"));
        let exp = wall_us() / 1_000_000 + 3_600;
        revoke_session(&st, " jti-1 ", exp, user).await.unwrap();
        let d = kv::get::<RevokedDoc>(m.as_ref(), ns::REVOKED, "#jti-1").await.unwrap().unwrap().value;
        assert_eq!(d, RevokedDoc { jti: "jti-1".into(), expires_at_us: exp * 1_000_000 });
        revoke_session(&st, "jti-1", exp, user).await.unwrap(); // a double logout is fine
        assert_eq!(ops_of(&st, tenant).await[..2], ["session_revoked", "session_revoked"]);

        assert_eq!(
            revoke_session(&st, "  ", exp, user).await,
            Err(WebError::Raised("revoke_session: jti must not be empty".into()))
        );
        let ghost = Uuid::new_v4();
        assert_eq!(
            revoke_session(&st, "j", exp, ghost).await,
            Err(WebError::Raised(format!("revoke_session: actor_id must be a known user {ghost}")))
        );

        record_user_login(&st, user).await.unwrap();
        assert!(operator_listing(&st, uid(&a, "cluster_id")).await.unwrap().users[0].last_login_at.is_some());
        assert_eq!(ops_of(&st, tenant).await[0], "login");
    }

    // ---- /auth/me ------------------------------------------------------------------------

    #[tokio::test]
    async fn me_lists_memberships_and_an_operator_sees_every_cluster() {
        let (m, st) = store();
        let cell = seed(&m, "local").await;
        let a = bootstrap_tenant(&st, &boot("zeta", "zeta-c", cell, "admin@zeta.io", None)).await.unwrap();
        bootstrap_tenant(&st, &boot("alpha", "alpha-c", cell, "admin@alpha.io", None)).await.unwrap();
        let user = uid(&a, "user_id");
        assert_eq!(
            me_user(&st, user).await.unwrap(),
            Some(MeUser { email: "admin@zeta.io".into(), is_operator: false, tenant_slug: "zeta".into() })
        );
        assert_eq!(me_user(&st, Uuid::new_v4()).await.unwrap(), None);
        let mine = me_clusters(&st, user, false).await.unwrap();
        assert_eq!(mine.len(), 1);
        assert_eq!(
            (mine[0].slug.as_str(), mine[0].role.as_str(), mine[0].cell_slug.as_str()),
            ("zeta-c", "admin", "local")
        );
        let all = me_clusters(&st, user, true).await.unwrap();
        assert_eq!(
            all.iter().map(|c| c.tenant_slug.as_str()).collect::<Vec<_>>(),
            ["alpha", "zeta"],
            "tenant slug order"
        );
        assert!(all.iter().all(|c| c.role == "admin" && c.status == "active"));
    }

    // ---- tenant delete cascade ------------------------------------------------------------

    #[tokio::test]
    async fn a_tenant_delete_cascades_everything_and_spares_the_neighbour() {
        let (m, st) = store();
        let cell = seed(&m, "local").await;
        let a = bootstrap_tenant(&st, &boot("wipe-a", "wipe-a-c", cell, "admin@a.io", Some("pw-pw-pw-pw-pw")))
            .await
            .unwrap();
        let b = bootstrap_tenant(&st, &boot("keep-b", "keep-b-c", cell, "admin@b.io", None)).await.unwrap();
        let (ta, ca, ua) = (uid(&a, "tenant_id"), uid(&a, "cluster_id"), uid(&a, "user_id"));
        let (tb, cb) = (uid(&b, "tenant_id"), uid(&b, "cluster_id"));

        // Give A one of everything.
        let me = UserRef { user_id: ua, tenant_id: ta };
        link_identity(&st, &me, "github", "77", "admin@a.io").await.unwrap();
        provision_oauth_user(&st, "wipe-a", "second@a.io", "google", "g-77", "viewer").await.unwrap();
        let spare =
            issue_api_key(&st, ca, "spare", &crate::auth::key_hash_hex("qk_live_spare"), &all_scopes()).await.unwrap();
        revoke_api_key(&st, &spare).await.unwrap();
        let queue = QueueDoc {
            id: Uuid::new_v4(),
            cluster_id: ca,
            name: "orders".into(),
            partitions_count: 1,
            created_at_us: 1,
            deleted_at_us: None,
        };
        let minute = (wall_us() / 60_000_000) * 60_000_000;
        let usage = UsageDoc { msgs: 5, reqs: 1, bytes_in: 10, bytes_out: 0 };
        kv::write(
            m.as_ref(),
            vec![
                kv::put_op(ns::QUEUES, &schema::key(queue.id), &queue, Expect::Any, Ttl::Forever, false),
                kv::put_op(ns::QUEUE_NAME, &schema::key2(ca, "orders"), &queue.id, Expect::Any, Ttl::Forever, false),
                kv::put_op(
                    ns::USAGE_MIN,
                    &format!("#{ca}/{minute}/push/n1"),
                    &usage,
                    Expect::Any,
                    Ttl::Seconds(3_600),
                    false,
                ),
                kv::put_op(
                    ns::USAGE_DAY,
                    &format!("#{ca}/{}/push", utc_day(minute)),
                    &usage,
                    Expect::Any,
                    Ttl::Forever,
                    false,
                ),
                kv::put_op(
                    ns::USAGE_MIN,
                    &format!("#{cb}/{minute}/push/n1"),
                    &usage,
                    Expect::Any,
                    Ttl::Seconds(3_600),
                    false,
                ),
            ],
        )
        .await
        .unwrap();

        // The two-step order is enforced.
        let e = delete_tenant(&st, ta, false).await.unwrap_err();
        assert!(matches!(&e, WebError::Raised(m) if m.contains("tenant wipe-a is active, not deleting")), "{e}");
        assert_eq!(set_tenant_status(&st, ta, "deleting").await.unwrap(), vec![ca]);
        assert!(matches!(set_tenant_status(&st, ta, "gone").await, Err(WebError::Raised(_))));

        let r = delete_tenant(&st, ta, false).await.unwrap();
        assert_eq!(
            (r["deleted"].clone(), r["existed"].clone(), r["status_was"].clone()),
            (json!(true), json!(true), json!("deleting"))
        );
        let c = &r["clusters"][0];
        assert_eq!(uid(c, "cluster_id"), ca);
        let left = kv::scan::<ClusterDoc>(m.as_ref(), ns::CLUSTERS, "#").await.unwrap();
        assert!(left.iter().all(|(_, d)| d.value.tenant_id == tb), "A's cluster rows are gone");
        // operations: bootstrap's 6 + the spare key's issue/revoke + the status change
        assert_eq!(
            r["counts"],
            json!({
                "clusters": 1, "users": 2, "cluster_roles": 2, "queues": 1, "api_keys": 2,
                "api_keys_revoked": 1, "operations": 9, "outbox_redacted": 1,
            })
        );

        // Nothing of A survives anywhere; B keeps every row.
        let a_ids = [ta.to_string(), ca.to_string(), ua.to_string()];
        for n in [
            ns::TENANTS,
            ns::TENANT_SLUG,
            ns::USERS,
            ns::USER_EMAIL,
            ns::USER_TENANT,
            ns::IDENTITIES,
            ns::IDENTITY_PROVIDER,
            ns::IDENTITY_USER,
            ns::CLUSTERS,
            ns::CLUSTER_SLUG,
            ns::CLUSTER_TENANT,
            ns::CLUSTER_CELL,
            ns::ROLES,
            ns::ROLE_CLUSTER,
            ns::KEYS,
            ns::KEY_HASH,
            ns::KEY_CLUSTER,
            ns::QUEUES,
            ns::QUEUE_NAME,
            ns::USAGE_MIN,
            ns::USAGE_DAY,
            ns::OPS,
            ns::OPS_CLUSTER,
        ] {
            let keys = m.keys(n);
            assert!(
                !keys.is_empty()
                    || matches!(
                        n,
                        ns::IDENTITIES
                            | ns::IDENTITY_PROVIDER
                            | ns::IDENTITY_USER
                            | ns::QUEUES
                            | ns::QUEUE_NAME
                            | ns::USAGE_DAY
                    ),
                "{n} lost B's rows too"
            );
            assert!(
                keys.iter().all(|k| !a_ids.iter().any(|id| k.contains(id.as_str()))),
                "{n} still holds A: {keys:?}"
            );
        }
        assert_eq!(m.keys(ns::TENANT_SLUG), vec!["#keep-b".to_string()]);
        assert_eq!(m.keys(ns::USER_EMAIL), vec!["#admin@b.io".to_string()], "second@a.io went with its tenant");
        assert!(list_operations(&st, ta, None, 10).await.unwrap().is_empty());
        assert!(!list_operations(&st, tb, None, 10).await.unwrap().is_empty());
        assert_eq!(list_cluster_keys(&st, cb).await.unwrap().len(), 1);

        // The outbox keeps every event, and A's address is gone from it.
        let outbox: Vec<OutboxDoc> = kv::scan::<OutboxDoc>(m.as_ref(), ns::OUTBOX, "#")
            .await
            .unwrap()
            .into_iter()
            .map(|(_, d)| d.value)
            .collect();
        assert_eq!(
            outbox.iter().map(|o| o.kind.as_str()).collect::<Vec<_>>(),
            ["tenant_bootstrapped", "tenant_bootstrapped", "tenant_deleted"]
        );
        assert!(outbox[0].payload.get("admin_email").is_none(), "A's signup event is redacted");
        assert_eq!(outbox[1].payload["admin_email"], "admin@b.io", "B's is not");
        assert_eq!(outbox[2].payload["clusters"][0]["broker_tenant_uuid"], c["broker_tenant_uuid"]);

        // Idempotent.
        assert_eq!(
            delete_tenant(&st, ta, false).await.unwrap(),
            json!({"deleted": false, "existed": false, "tenant_id": ta})
        );
        // force skips only the status precondition
        let r = delete_tenant(&st, tb, true).await.unwrap();
        assert_eq!((r["forced"].clone(), r["status_was"].clone()), (json!(true), json!("active")));
        assert!(m.keys(ns::TENANTS).is_empty() && m.keys(ns::KEY_HASH).is_empty() && m.keys(ns::OPS).is_empty());
    }

    // ---- usage reads ---------------------------------------------------------------------

    #[tokio::test]
    async fn usage_sums_nodes_windows_hours_and_takes_the_larger_of_rolled_and_live() {
        for padded in [false, true] {
            let (m, st) = store();
            let cell = seed(&m, "local").await;
            let a = bootstrap_tenant(&st, &boot("acme", "acme-c", cell, "admin@acme.io", None)).await.unwrap();
            let cluster = uid(&a, "cluster_id");
            let fmt = |us: i64| {
                if padded {
                    schema::ordered(us)
                } else {
                    us.to_string()
                }
            };
            let now_min = (wall_us() / 60_000_000) * 60_000_000;
            let recent = now_min - 10 * 60_000_000;
            let old = now_min - 30 * 3_600_000_000; // outside a 24h window
            let doc = |msgs| UsageDoc { msgs, reqs: 1, bytes_in: 100, bytes_out: 7 };
            let put = |key: String, d: UsageDoc| {
                kv::put_op(ns::USAGE_MIN, &key, &d, Expect::Any, Ttl::Seconds(86_400 * 40), false)
            };
            kv::write(
                m.as_ref(),
                vec![
                    put(format!("#{cluster}/{}/push/node-a", fmt(recent)), doc(3)),
                    put(format!("#{cluster}/{}/push/node-b", fmt(recent)), doc(4)),
                    put(format!("#{cluster}/{}/delivery/node-a", fmt(recent)), doc(2)),
                    put(format!("#{cluster}/{}/push/node-a", fmt(old)), doc(50)),
                ],
            )
            .await
            .unwrap();
            let rows = usage_minutes(&st, cluster, 24).await.unwrap();
            assert_eq!(
                rows,
                vec![
                    UsageMinute {
                        minute: utc_iso(recent),
                        op: "delivery".into(),
                        reqs: 1,
                        msgs: 2,
                        bytes_in: 100,
                        bytes_out: 7
                    },
                    UsageMinute {
                        minute: utc_iso(recent),
                        op: "push".into(),
                        reqs: 2,
                        msgs: 7,
                        bytes_in: 200,
                        bytes_out: 14
                    },
                ],
                "padded={padded}"
            );
            assert_eq!(usage_minutes(&st, cluster, 48).await.unwrap().len(), 3);

            // Month to date: per day the larger of rolled-up and live — a
            // rolled day ABOVE its live minutes wins (the minutes were
            // pruned), and the live minutes count in full where nothing was
            // rolled yet. (`old` is always a different UTC day than `recent`.)
            let (start, _) = month_bounds_us(now_min);
            let mut expect = if recent >= start { 9 } else { 0 }; // 3 + 4 + 2
            if old >= start {
                let day = format!("#{cluster}/{}/push", utc_day(old));
                kv::write(
                    m.as_ref(),
                    vec![kv::put_op(ns::USAGE_DAY, &day, &doc(80), Expect::Any, Ttl::Forever, false)],
                )
                .await
                .unwrap();
                expect += 80; // max(80 rolled, 50 live)
            }
            let usage = cluster_plan_usage(&st, cluster).await.unwrap().unwrap();
            assert_eq!((usage.code.as_str(), usage.monthly_msgs_quota), ("free", Some(1_000_000)));
            assert_eq!(usage.month, utc_month(wall_us()));
            assert_eq!(usage.msgs, expect, "padded={padded}");
            assert_eq!(cluster_plan_usage(&st, Uuid::new_v4()).await.unwrap(), None);
        }
    }

    #[test]
    fn usage_after_matches_the_key_format_or_walks_everything() {
        let p = "#c/";
        assert_eq!(
            usage_after(p, "#c/1790257500000000/push/n", 1790257560000000).as_deref(),
            Some("#c/1790257559999999")
        );
        assert_eq!(
            usage_after(p, &format!("#c/{}/push/n", schema::ordered(1790257500000000)), 1790257560000000).as_deref(),
            Some("#c/00001790257559999999")
        );
        assert_eq!(usage_after(p, "#c/abc/push/n", 5), None);
        assert_eq!(usage_after(p, "#c/99/push/n", 1790257560000000), None, "width mismatch: no shortcut");
        assert_eq!(parse_usage_key("#c/00001790257500000000/push/n1", p), Some((1790257500000000, "push".into())));
    }

    // ---- plumbing ---------------------------------------------------------------------------

    #[test]
    fn utc_formatting_matches_postgres_to_char() {
        assert_eq!(utc_iso(0), "1970-01-01T00:00:00Z");
        assert_eq!(utc_iso(1_790_257_507_000_000), "2026-09-24T13:45:07Z");
        assert_eq!(utc_iso(1_709_251_199_999_999), "2024-02-29T23:59:59Z");
        assert_eq!(utc_iso(951_868_800_000_000), "2000-03-01T00:00:00Z");
        assert_eq!(utc_day(1_798_761_599_000_000), "2026-12-31");
        assert_eq!(utc_month(1_798_761_599_000_000), "2026-12");
        let (start, end) = month_bounds_us(1_798_761_599_000_000);
        assert_eq!((utc_iso(start), utc_iso(end)), ("2026-12-01T00:00:00Z".into(), "2027-01-01T00:00:00Z".into()));
        let (start, end) = month_bounds_us(1_709_251_199_000_000);
        assert_eq!((utc_iso(start), utc_iso(end)), ("2024-02-01T00:00:00Z".into(), "2024-03-01T00:00:00Z".into()));
        for d in [-1_000, 0, 10_957, 19_782, 20_720, 100_000] {
            let (y, mo, da) = civil(d);
            assert_eq!(days_from_civil(y, mo, da), d);
        }
    }

    #[test]
    fn now_us_is_strictly_increasing() {
        let mut last = now_us();
        for _ in 0..10_000 {
            let n = now_us();
            assert!(n > last);
            last = n;
        }
    }

    #[test]
    fn slug_check_is_the_dns_label_rule() {
        for ok in ["a", "acme", "a-b", "0x", &"a".repeat(63)] {
            assert!(dns_label(ok), "{ok}");
        }
        for bad in ["", "-a", "a-", "A", "a_b", "a b", &"a".repeat(64)] {
            assert!(!dns_label(bad), "{bad}");
        }
    }

    #[test]
    fn refusals_keep_their_wire_shapes() {
        assert_eq!(Refusal::BadGateway("x").response().status(), StatusCode::BAD_GATEWAY);
        assert_eq!(Refusal::NotFound("x").response().status(), StatusCode::NOT_FOUND);
        assert_eq!(Refusal::Conflict("x").response().status(), StatusCode::CONFLICT);
        assert_eq!(Refusal::Misdirected("x").response().status(), StatusCode::MISDIRECTED_REQUEST);
        assert_eq!(Refusal::BadRequest("x").response().status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn dev_static_has_no_store() {
        assert_eq!(me_user(&Store::None, Uuid::new_v4()).await, Err(WebError::NotConfigured));
        assert_eq!(operator_listing(&Store::None, Uuid::new_v4()).await, Err(Refusal::BadGateway("pxdb unavailable")));
    }
}
