//! W3 data plane repositories (PLAN_SINGLE_BINARY.md W3): tenants, cells,
//! plans, clusters, cluster_roles, api_keys, revoked_tokens, queues.
//!
//! Every function answers from either backend of [`Store`]:
//!
//! - **`Store::Pg`** — the standalone proxy. The SQL is the statement the
//!   calling module ran before this file existed, moved here VERBATIM, and the
//!   control-plane writers call the same `queen_proxy.*` function (migrations
//!   002–011) they always did. Production runs this arm: it must not drift.
//! - **`Store::Kv`** — the single binary. Documents per [`super::schema`];
//!   every stored function a writer here stands in for is reimplemented in
//!   Rust with the same validation, the same RAISE text (as
//!   [`DataError::Invalid`]) and the same result. A UNIQUE index is a
//!   `putIfAbsent` + `required` in the SAME batch as the row, so two nodes can
//!   never both win; the loser gets [`DataError::Conflict`], Postgres' 23505.
//! - **`Store::None`** — dev-static: reads answer "nothing", writes
//!   [`DataError::NoStore`].
//!
//! # Invalidation (the Kv arm of `queen_proxy_inval`)
//!
//! Postgres' `record_operation` NOTIFYs `queen_proxy_inval` with a cluster id
//! and every proxy drops that cluster's cached state. The KV arm writes, in the
//! SAME batch as the change, an `incr` of `px.meta #inval` and a mark
//! `px.meta #inval/<cluster>` (TTL [`INVAL_MARK_TTL_S`]). Every node polls the
//! counter (one local read per second, [`InvalFeed`]); when it moved, it lists
//! the marks and invalidates the clusters whose mark has a version it has not
//! seen. Versions are unique and never re-issued, so "differs" is the test —
//! never "greater" (monotonicity is not promised, server/src/rsm/planner/kv.rs).
//! Exactly the writes that NOTIFY in Postgres mark here: an operation row with
//! a cluster, plus the explicit per-cluster fan-outs (set_tenant_status,
//! delete_tenant). Queue rows, `last_used_at` and revocations do not.
//!
//! Layout additions (px.meta only, nothing else changes): `#inval` (counter)
//! and `#inval/<cluster uuid>` (marks).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};
use tokio_postgres::error::SqlState;
use uuid::Uuid;

use super::kv::{self, Doc, Expect, KvBackend, KvError, Ttl};
use super::schema::{
    self, ns, ApiKeyDoc, CellDoc, ClusterDoc, IdentityDoc, OperationDoc, OutboxDoc, PlanDoc, QueueDoc, RevokedDoc,
    RoleDoc, TenantDoc, UserDoc, K,
};
use super::Store;
use crate::state::{ClusterCtx, EffectiveLimits, Scopes};

/// The broker's per-call op ceiling on the HTTP path (`MAX_OPS_HTTP`). Every
/// batch built here stays under it; chunked writers use [`CHUNK_OPS`].
const MAX_OPS: usize = 256;
/// Ops per chunk for the writers that split (sweeps, cascades, touches).
const CHUNK_OPS: usize = 240;
/// Optimistic-concurrency retries for a read-modify-write that lost its
/// version race.
const ATTEMPTS: usize = 5;
/// How long an invalidation mark lives. A node whose poll is down longer than
/// this may miss a mark; the caches' own TTLs (30 s) still bound the damage.
pub const INVAL_MARK_TTL_S: u64 = 3600;
const INVAL_COUNTER: &str = "#inval";
const INVAL_MARK_PREFIX: &str = "#inval/";
/// A key's `last_used_at` is rewritten at most once per this window
/// cluster-wide: every node touches independently, and a replicated write per
/// active key per flush per node buys nothing a minute of precision does not.
const TOUCH_MIN_INTERVAL_US: i64 = 60_000_000;

// ===========================================================================
// errors
// ===========================================================================

/// Why a repository call did not do what it was asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataError {
    /// The input or a precondition was refused: a stored function's `RAISE
    /// EXCEPTION` (P0001), a CHECK or a foreign key. The text is the SQL one.
    Invalid(String),
    /// A UNIQUE index already holds the value (23505).
    Conflict(String),
    /// The store did not answer (pool, connection, KV leader, a document that
    /// does not decode).
    Unavailable(String),
    /// No store configured (dev-static).
    NoStore,
}

impl std::fmt::Display for DataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataError::Invalid(m) => write!(f, "{m}"),
            DataError::Conflict(m) => write!(f, "{m}"),
            DataError::Unavailable(m) => write!(f, "store unavailable: {m}"),
            DataError::NoStore => write!(f, "no store configured"),
        }
    }
}

impl std::error::Error for DataError {}

impl DataError {
    /// Postgres' UNIQUE_VIOLATION, on either backend.
    pub fn is_conflict(&self) -> bool {
        matches!(self, DataError::Conflict(_))
    }

    fn pg(e: &tokio_postgres::Error) -> DataError {
        let msg = match e.as_db_error() {
            Some(db) => db.message().to_string(),
            None => e.to_string(),
        };
        match e.code() {
            Some(c) if *c == SqlState::UNIQUE_VIOLATION => DataError::Conflict(msg),
            // P0001 (RAISE), class 23 (integrity: CHECK, FK, NOT NULL) and
            // class 22 (a malformed uuid/jsonb argument) are the caller's input.
            Some(c) if c.code() == "P0001" || c.code().starts_with("23") || c.code().starts_with("22") => {
                DataError::Invalid(msg)
            }
            _ => DataError::Unavailable(msg),
        }
    }

    fn pool(e: deadpool_postgres::PoolError) -> DataError {
        DataError::Unavailable(format!("pool.get: {e}"))
    }

    fn kv(e: KvError) -> DataError {
        DataError::Unavailable(e.to_string())
    }
}

/// What one lookup told us. `Absent` (the store answered: no such row) and
/// `Unavailable` (it never answered, or the row does not decode) are
/// deliberately distinct: the cache's fail-open is only sound while "the store
/// said no" can never be confused with "the store did not answer".
#[derive(Clone, Debug)]
pub enum Lookup<T> {
    Found(T),
    Absent,
    Unavailable,
}

/// How a cluster is named: the Host label / act-as slug, or its uuid.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterKey {
    Slug(String),
    Id(Uuid),
}

/// What a caller that still holds the retiring `AppState.db` may pass where a
/// `&Store` is wanted (auth.rs's lookups take `impl Into<Store>` so both the
/// modules already on `st.store` and those still on `st.db` compile).
impl From<&Option<deadpool_postgres::Pool>> for Store {
    fn from(db: &Option<deadpool_postgres::Pool>) -> Store {
        db.clone().map(Store::Pg).unwrap_or(Store::None)
    }
}

impl From<Option<deadpool_postgres::Pool>> for Store {
    fn from(db: Option<deadpool_postgres::Pool>) -> Store {
        db.map(Store::Pg).unwrap_or(Store::None)
    }
}

impl From<&Store> for Store {
    fn from(s: &Store) -> Store {
        s.clone()
    }
}

// ===========================================================================
// small helpers
// ===========================================================================

pub(crate) fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// Days since 1970-01-01 -> (year, month, day), proleptic Gregorian.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// A timestamp the way Postgres renders a `timestamptz` inside jsonb at UTC:
/// `2026-09-24T10:11:12.5+00:00` (fraction trimmed, absent when zero).
pub(crate) fn iso_utc(us: i64) -> String {
    let secs = us.div_euclid(1_000_000);
    let frac = us.rem_euclid(1_000_000);
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let sod = secs.rem_euclid(86_400);
    let mut s = format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}",
        sod / 3600,
        (sod / 60) % 60,
        sod % 60
    );
    if frac != 0 {
        let f = format!("{frac:06}");
        s.push('.');
        s.push_str(f.trim_end_matches('0'));
    }
    s.push_str("+00:00");
    s
}

/// Postgres' `btrim(text)`: spaces only, both ends.
fn btrim(s: &str) -> &str {
    s.trim_matches(' ')
}

/// `lower(btrim(x))`.
fn norm(s: &str) -> String {
    btrim(s).to_lowercase()
}

/// The CHECK on tenants.slug and clusters.slug: `^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`.
fn dns_label_ok(s: &str) -> bool {
    let b = s.as_bytes();
    let edge = |c: u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    !b.is_empty() && b.len() <= 63 && edge(b[0]) && edge(b[b.len() - 1]) && b.iter().all(|&c| edge(c) || c == b'-')
}

/// The CHECK on api_keys.key_hash: `^[0-9a-f]{64}$`.
fn key_hash_ok(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// The CHECK on plans.code: `^[a-z][a-z0-9-]*$`.
fn plan_code_ok(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b[0].is_ascii_lowercase()
        && b.iter()
            .all(|&c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
}

fn check_violation(table: &str, column: &str) -> DataError {
    DataError::Invalid(format!(
        "new row for relation \"{table}\" violates check constraint \"{table}_{column}_check\""
    ))
}

const ROLES: [&str; 4] = ["admin", "producer", "consumer", "viewer"];
const SCOPES: [&str; 4] = ["produce", "consume", "admin", "read"];
const ACTORS: [&str; 4] = ["user", "api_key", "control_plane", "system"];
const TENANT_STATUSES: [&str; 4] = ["active", "grace", "suspended", "deleting"];
const CLUSTER_STATUSES: [&str; 4] = ["active", "push_blocked", "suspended", "deleting"];

fn scopes_of(list: &[String]) -> Scopes {
    Scopes {
        produce: list.iter().any(|s| s == "produce"),
        consume: list.iter().any(|s| s == "consume"),
        admin: list.iter().any(|s| s == "admin"),
        read: list.iter().any(|s| s == "read"),
    }
}

// ===========================================================================
// KV plumbing
// ===========================================================================

fn get_op(ns: &str, key: &str) -> Value {
    json!({"op":"get","ns":ns,"key":key})
}

/// One `get` answer, decoded. `Ok(None)` = no such key; `Err` = the document
/// does not decode as `T` (a malformed row is "no answer", never "no row").
fn got<T: DeserializeOwned>(r: Option<&Value>) -> Result<Option<Doc<T>>, DataError> {
    let Some(r) = r else {
        return Err(DataError::Unavailable("kv: missing answer".into()));
    };
    if r.get("found").and_then(Value::as_bool) != Some(true) {
        return Ok(None);
    }
    let version = r.get("version").and_then(Value::as_u64).unwrap_or(0);
    let value = r.get("value").cloned().unwrap_or(Value::Null);
    serde_json::from_value::<T>(value)
        .map(|value| Some(Doc { value, version }))
        .map_err(|e| {
            let key = r.get("key").and_then(Value::as_str).unwrap_or("?");
            tracing::error!(target: "store", key, error = %e, "kv document does not decode");
            DataError::Unavailable(format!("kv document {key} does not decode: {e}"))
        })
}

/// Several gets in ONE call (one round trip); answers index-aligned.
async fn gets(kv: &dyn KvBackend, keys: &[(&str, String)]) -> Result<Vec<Value>, DataError> {
    let ops = keys.iter().map(|(n, k)| get_op(n, k)).collect();
    kv.kv(ops).await.map_err(DataError::kv)
}

async fn read1<T: DeserializeOwned>(kv: &dyn KvBackend, ns: &str, key: &str) -> Result<Option<Doc<T>>, DataError> {
    let out = gets(kv, &[(ns, key.to_string())]).await?;
    got(out.first())
}

/// Every id listed under a "rows of X" index prefix (`#<x>/<id>` keys).
async fn index_ids(kv: &dyn KvBackend, ns: &str, of: Uuid) -> Result<Vec<Uuid>, DataError> {
    let keys = kv::scan_keys(kv, ns, &schema::prefix(of))
        .await
        .map_err(DataError::kv)?;
    Ok(keys
        .iter()
        .filter_map(|k| Uuid::parse_str(schema::tail(k)).ok())
        .collect())
}

/// Documents by id, missing ones dropped.
async fn docs_by_id<T: DeserializeOwned>(kv: &dyn KvBackend, ns: &str, ids: &[Uuid]) -> Result<Vec<Doc<T>>, DataError> {
    let keys: Vec<String> = ids.iter().map(schema::key).collect();
    Ok(kv::get_many::<T>(kv, ns, &keys)
        .await
        .map_err(DataError::kv)?
        .into_iter()
        .flatten()
        .collect())
}

/// One atomic KV write call being assembled.
#[derive(Default)]
struct Batch {
    ops: Vec<Value>,
    /// Per op: the UNIQUE constraint it enforces, when it is an index claim.
    unique: Vec<Option<&'static str>>,
    /// Clusters whose cached state this batch changes (marks, see module doc).
    inval: BTreeSet<Uuid>,
}

impl Batch {
    fn op(&mut self, op: Value) {
        self.ops.push(op);
        self.unique.push(None);
    }
    /// Upsert, no precondition.
    fn put(&mut self, ns: &str, key: &str, value: &impl Serialize) {
        self.op(kv::put_op(ns, key, value, Expect::Any, Ttl::Forever, false));
    }
    /// A new row: the key must not exist (the whole batch fails otherwise).
    fn put_new(&mut self, ns: &str, key: &str, value: &impl Serialize) {
        self.op(kv::put_op(ns, key, value, Expect::Absent, Ttl::Forever, true));
    }
    /// A read-modify-write: the row must still be at `version`.
    fn put_at(&mut self, ns: &str, key: &str, value: &impl Serialize, version: u64) {
        self.op(kv::put_op(ns, key, value, Expect::Version(version), Ttl::Forever, true));
    }
    /// A UNIQUE index claim: `#<value> -> id`, absent or the batch fails as a
    /// [`DataError::Conflict`] naming `constraint`.
    fn claim(&mut self, ns: &str, key: &str, id: Uuid, constraint: &'static str) {
        self.ops
            .push(kv::put_op(ns, key, &id, Expect::Absent, Ttl::Forever, true));
        self.unique.push(Some(constraint));
    }
    fn del(&mut self, ns: &str, key: &str) {
        self.op(kv::delete_op(ns, key, Expect::Any, false));
    }
    fn del_at(&mut self, ns: &str, key: &str, version: u64) {
        self.op(kv::delete_op(ns, key, Expect::Version(version), true));
    }
    fn invalidate(&mut self, cluster: Uuid) {
        self.inval.insert(cluster);
    }
    fn len(&self) -> usize {
        self.ops.len() + if self.inval.is_empty() { 0 } else { 1 + self.inval.len() }
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// How a batch failed.
enum WriteErr {
    /// A UNIQUE claim lost: 23505.
    Conflict(String),
    /// A version (or absence) expectation lost: re-read and try again.
    Moved,
    Fail(DataError),
}

impl From<DataError> for WriteErr {
    fn from(e: DataError) -> Self {
        WriteErr::Fail(e)
    }
}

/// Marks per call when a fan-out is too wide to ride with its change.
const MARKS_PER_CALL: usize = 200;

/// The invalidation ops for `clusters`: the counter bump plus one mark each,
/// in calls of at most [`MARKS_PER_CALL`] marks (each with its own bump).
fn inval_chunks(clusters: &BTreeSet<Uuid>, at_us: i64) -> Vec<Vec<Value>> {
    let ids: Vec<&Uuid> = clusters.iter().collect();
    ids.chunks(MARKS_PER_CALL)
        .map(|chunk| {
            let mut out = Vec::with_capacity(chunk.len() + 1);
            out.push(kv::incr_op(ns::META, INVAL_COUNTER, 1, Ttl::Forever));
            for c in chunk {
                out.push(kv::put_op(
                    ns::META,
                    &format!("{INVAL_MARK_PREFIX}{c}"),
                    &at_us,
                    Expect::Any,
                    Ttl::Seconds(INVAL_MARK_TTL_S),
                    false,
                ));
            }
            out
        })
        .collect()
}

/// The Kv arm of `pg_notify('queen_proxy_inval', cluster)` for writers
/// outside this module (store/web.rs): append these ops to the SAME batch as
/// the change. At most [`MARKS_PER_CALL`] clusters per call; one write per key.
pub fn invalidation_ops(clusters: &[Uuid]) -> Vec<Value> {
    let set: BTreeSet<Uuid> = clusters.iter().copied().collect();
    inval_chunks(&set, now_us()).into_iter().flatten().collect()
}

/// Commit one batch. Its invalidation marks ride in the SAME atomic call when
/// they fit (always, but for a fan-out over hundreds of clusters); otherwise
/// they follow the change in calls of their own.
async fn commit(kv: &dyn KvBackend, b: Batch) -> Result<Vec<kv::Written>, WriteErr> {
    let Batch { mut ops, unique, inval } = b;
    let mut marks = inval_chunks(&inval, now_us());
    if marks.len() == 1 && ops.len() + marks[0].len() <= MAX_OPS {
        ops.append(&mut marks[0]);
        marks.clear();
    }
    let written = if ops.is_empty() {
        Vec::new()
    } else {
        debug_assert!(ops.len() <= MAX_OPS, "kv batch of {} ops", ops.len());
        match kv::write(kv, ops).await {
            Ok(w) => w,
            Err(e @ KvError::Precondition { .. }) => {
                return match e.precondition_index().and_then(|i| unique.get(i).copied().flatten()) {
                    Some(constraint) => Err(WriteErr::Conflict(format!(
                        "duplicate key value violates unique constraint \"{constraint}\""
                    ))),
                    None => Err(WriteErr::Moved),
                };
            }
            Err(e) => return Err(WriteErr::Fail(DataError::kv(e))),
        }
    };
    for chunk in marks {
        kv::write(kv, chunk)
            .await
            .map_err(|e| WriteErr::Fail(DataError::kv(e)))?;
    }
    Ok(written)
}

/// `queen_proxy.record_operation`: the audit row (px.ops, and px.ops.cluster
/// when it names a cluster) — and, as in Postgres, a cluster names an
/// invalidation.
#[allow(clippy::too_many_arguments)]
fn record_op(
    b: &mut Batch,
    at_us: i64,
    tenant: Uuid,
    cluster: Option<Uuid>,
    actor: &str,
    actor_id: Option<Uuid>,
    action: &str,
    target: Option<String>,
    meta: Value,
) {
    let id = Uuid::new_v4();
    let inv = schema::inverted(at_us);
    let doc = OperationDoc {
        id,
        tenant_id: tenant,
        cluster_id: cluster,
        actor: actor.to_string(),
        actor_id,
        action: action.to_string(),
        target,
        meta: if meta.is_null() { json!({}) } else { meta },
        at_us,
    };
    b.put(ns::OPS, &format!("{K}{tenant}/{inv}/{id}"), &doc);
    if let Some(c) = cluster {
        b.put(ns::OPS_CLUSTER, &format!("{K}{c}/{inv}/{id}"), &"");
        b.invalidate(c);
    }
}

/// `queen_proxy.emit_outbox`.
fn emit_outbox(b: &mut Batch, at_us: i64, kind: &str, payload: Value) {
    let id = Uuid::new_v4();
    let doc = OutboxDoc {
        id,
        kind: kind.to_string(),
        payload,
        created_at_us: at_us,
        consumed_at_us: None,
    };
    b.put(ns::OUTBOX, &format!("{K}{}/{id}", schema::ordered(at_us)), &doc);
}

/// Run `batches` one after the other (a cascade too large for one call).
async fn commit_all(kv: &dyn KvBackend, batches: Vec<Batch>) -> Result<(), DataError> {
    for b in batches {
        match commit(kv, b).await {
            Ok(_) => {}
            Err(WriteErr::Fail(e)) => return Err(e),
            Err(WriteErr::Conflict(m)) => return Err(DataError::Conflict(m)),
            Err(WriteErr::Moved) => return Err(DataError::Unavailable("kv: concurrent change, retry".into())),
        }
    }
    Ok(())
}

/// Unconditional deletes, de-duplicated (one write per key per call) and
/// chunked under the op ceiling.
fn delete_batches(keys: Vec<(&'static str, String)>) -> Vec<Batch> {
    let mut seen = HashSet::new();
    let mut out: Vec<Batch> = Vec::new();
    let mut cur = Batch::default();
    for (n, k) in keys {
        if !seen.insert((n, k.clone())) {
            continue;
        }
        if cur.len() >= CHUNK_OPS {
            out.push(std::mem::take(&mut cur));
        }
        cur.del(n, &k);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn kv_of(store: &Store) -> Option<&dyn KvBackend> {
    store.kv().map(|k| k.as_ref())
}

// ===========================================================================
// the hot path: Host / slug / id -> ClusterCtx, key hash -> (ctx, key, scopes)
// ===========================================================================

/// The projection `ctx_from_row` reads, shared by every cluster lookup keyed
/// on the clusters table. A macro (not a const) so `concat!` can glue a WHERE
/// onto it at compile time and the two queries cannot drift apart.
macro_rules! cluster_select {
    () => {
        "
    SELECT c.id::text                  AS cluster_id,
           c.tenant_id::text           AS tenant_id,
           c.broker_tenant_uuid::text  AS broker_tenant,
           c.slug                      AS slug,
           ce.base_url                 AS base_url,
           ce.cell_secret              AS cell_secret,
           t.status                    AS tenant_status,
           c.status                    AS cluster_status,
           p.max_req_per_sec, p.req_burst, p.max_msgs_per_sec, p.msgs_burst,
           p.max_queues, p.max_partitions_per_queue, p.max_parked_pops,
           p.max_payload_bytes, p.max_batch_items, p.max_retained_bytes, p.max_retention_seconds,
           (p.features)::text          AS features_json,
           (c.limit_overrides)::text   AS overrides_json
    FROM queen_proxy.clusters c
    JOIN queen_proxy.tenants t ON t.id = c.tenant_id
    JOIN queen_proxy.cells   ce ON ce.id = c.cell_id
    JOIN queen_proxy.plans   p  ON p.id = c.plan_id
    "
    };
}

const RESOLVE_HOST_SQL: &str = concat!(cluster_select!(), "WHERE c.slug = $1");

/// Act-as-cluster by uuid. `$1::text::uuid` for the same reason every other
/// query in this crate does it: no uuid feature on tokio-postgres.
const RESOLVE_BY_ID_SQL: &str = concat!(cluster_select!(), "WHERE c.id = $1::text::uuid");

const BY_KEY_HASH_SQL: &str = "
    SELECT ak.id::text                 AS key_id,
           ak.scopes                   AS scopes,
           c.id::text                  AS cluster_id,
           c.tenant_id::text           AS tenant_id,
           c.broker_tenant_uuid::text  AS broker_tenant,
           c.slug                      AS slug,
           ce.base_url                 AS base_url,
           ce.cell_secret              AS cell_secret,
           t.status                    AS tenant_status,
           c.status                    AS cluster_status,
           p.max_req_per_sec, p.req_burst, p.max_msgs_per_sec, p.msgs_burst,
           p.max_queues, p.max_partitions_per_queue, p.max_parked_pops,
           p.max_payload_bytes, p.max_batch_items, p.max_retained_bytes, p.max_retention_seconds,
           (p.features)::text          AS features_json,
           (c.limit_overrides)::text   AS overrides_json
    FROM queen_proxy.api_keys ak
    JOIN queen_proxy.clusters c ON c.id = ak.cluster_id
    JOIN queen_proxy.tenants  t ON t.id = c.tenant_id
    JOIN queen_proxy.cells    ce ON ce.id = c.cell_id
    JOIN queen_proxy.plans    p  ON p.id = c.plan_id
    WHERE ak.key_hash = $1 AND ak.revoked_at IS NULL";

/// Resolve a cluster (the ClusterCache miss path).
pub async fn lookup_cluster(store: &Store, key: &ClusterKey) -> Lookup<ClusterCtx> {
    match store {
        Store::Pg(pool) => {
            let (sql, param) = match key {
                ClusterKey::Slug(s) => (RESOLVE_HOST_SQL, s.clone()),
                ClusterKey::Id(id) => (RESOLVE_BY_ID_SQL, id.to_string()),
            };
            pg_lookup_host(pool, sql, &param).await
        }
        Store::Kv(kv) => kv_lookup_cluster(kv.as_ref(), key).await,
        Store::None => Lookup::Absent,
    }
}

/// Resolve an API key by its sha256 hex (the ClusterCache miss path).
pub async fn lookup_api_key(store: &Store, hash_hex: &str) -> Lookup<(ClusterCtx, Uuid, Scopes)> {
    match store {
        Store::Pg(pool) => pg_lookup_key(pool, hash_hex).await,
        Store::Kv(kv) => kv_lookup_key(kv.as_ref(), hash_hex).await,
        Store::None => Lookup::Absent,
    }
}

async fn pg_lookup_host(pool: &deadpool_postgres::Pool, sql: &str, param: &str) -> Lookup<ClusterCtx> {
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "resolve_host: pxdb pool.get failed");
            return Lookup::Unavailable;
        }
    };
    let row_opt = match client.query_opt(sql, &[&param]).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, cluster = %param, "resolve_host: query failed");
            return Lookup::Unavailable;
        }
    };
    let Some(row) = row_opt else { return Lookup::Absent };
    match ctx_from_row(&row) {
        Ok(c) => Lookup::Found(c),
        Err(e) => {
            tracing::error!(error = %e, cluster = %param, "resolve_host: malformed row");
            Lookup::Unavailable
        }
    }
}

async fn pg_lookup_key(pool: &deadpool_postgres::Pool, hash_hex: &str) -> Lookup<(ClusterCtx, Uuid, Scopes)> {
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "by_key_hash: pxdb pool.get failed");
            return Lookup::Unavailable;
        }
    };
    let row_opt = match client.query_opt(BY_KEY_HASH_SQL, &[&hash_hex]).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "by_key_hash: query failed");
            return Lookup::Unavailable;
        }
    };
    let Some(row) = row_opt else { return Lookup::Absent };
    match build_key_result(&row) {
        Ok(r) => Lookup::Found(r),
        Err(e) => {
            tracing::error!(error = %e, "by_key_hash: malformed row");
            Lookup::Unavailable
        }
    }
}

fn build_key_result(row: &tokio_postgres::Row) -> Result<(ClusterCtx, Uuid, Scopes), String> {
    let key_id = parse_uuid(row, "key_id")?;
    let scopes_vec: Vec<String> = row.try_get("scopes").map_err(|e| format!("scopes: {e}"))?;
    let ctx = ctx_from_row(row)?;
    Ok((ctx, key_id, scopes_of(&scopes_vec)))
}

/// Shared by resolve_host and by_key_hash: both SELECTs alias to the same
/// column names (cluster_id, tenant_id, broker_tenant, slug, base_url,
/// cell_secret, tenant_status, cluster_status, the plan limit columns,
/// features_json, overrides_json) precisely so this one builder works for
/// either.
fn ctx_from_row(row: &tokio_postgres::Row) -> Result<ClusterCtx, String> {
    let cluster_id = parse_uuid(row, "cluster_id")?;
    let tenant_id = parse_uuid(row, "tenant_id")?;
    let broker_tenant = parse_uuid(row, "broker_tenant")?;
    let slug: String = row.try_get("slug").map_err(|e| format!("slug: {e}"))?;
    let cell_base_url: String = row.try_get("base_url").map_err(|e| format!("base_url: {e}"))?;
    let cell_token: Option<String> = row.try_get("cell_secret").map_err(|e| format!("cell_secret: {e}"))?;
    let tenant_status: String = row
        .try_get("tenant_status")
        .map_err(|e| format!("tenant_status: {e}"))?;
    let cluster_status: String = row
        .try_get("cluster_status")
        .map_err(|e| format!("cluster_status: {e}"))?;
    let status = crate::cache::merge_status(&tenant_status, &cluster_status);

    let base = EffectiveLimits {
        max_req_per_sec: get_i32_as_i64(row, "max_req_per_sec")?,
        req_burst: get_i32_as_i64(row, "req_burst")?,
        max_msgs_per_sec: get_i32_as_i64(row, "max_msgs_per_sec")?,
        msgs_burst: get_i32_as_i64(row, "msgs_burst")?,
        max_queues: get_i32_as_i64(row, "max_queues")?,
        max_partitions_per_queue: get_i32_as_i64(row, "max_partitions_per_queue")?,
        max_parked_pops: get_i32_as_i64(row, "max_parked_pops")?,
        max_payload_bytes: get_i32_as_i64(row, "max_payload_bytes")?,
        max_batch_items: get_i32_as_i64(row, "max_batch_items")?,
        max_retained_bytes: row
            .try_get("max_retained_bytes")
            .map_err(|e| format!("max_retained_bytes: {e}"))?,
        max_retention_seconds: get_i32_as_i64(row, "max_retention_seconds")?,
    };
    let overrides_json: String = row
        .try_get("overrides_json")
        .map_err(|e| format!("overrides_json: {e}"))?;
    let overrides: Value = serde_json::from_str(&overrides_json).unwrap_or(Value::Null);
    let limits = crate::cache::merge_limits(base, &overrides);

    let features_json: String = row
        .try_get("features_json")
        .map_err(|e| format!("features_json: {e}"))?;
    let features = crate::cache::parse_features(&features_json);

    Ok(ClusterCtx {
        cluster_id,
        tenant_id,
        broker_tenant,
        slug,
        cell_base_url,
        cell_token,
        status,
        limits,
        features,
    })
}

fn parse_uuid(row: &tokio_postgres::Row, col: &str) -> Result<Uuid, String> {
    let s: String = row.try_get(col).map_err(|e| format!("{col}: {e}"))?;
    Uuid::parse_str(&s).map_err(|e| format!("{col}: bad uuid {s:?}: {e}"))
}

fn get_i32_as_i64(row: &tokio_postgres::Row, col: &str) -> Result<Option<i64>, String> {
    let v: Option<i32> = row.try_get(col).map_err(|e| format!("{col}: {e}"))?;
    Ok(v.map(i64::from))
}

/// The same ClusterCtx the Postgres JOIN builds, from the four documents.
fn ctx_from_docs(c: &ClusterDoc, t: &TenantDoc, ce: &CellDoc, p: &PlanDoc) -> ClusterCtx {
    let base = EffectiveLimits {
        max_req_per_sec: p.max_req_per_sec,
        req_burst: p.req_burst,
        max_msgs_per_sec: p.max_msgs_per_sec,
        msgs_burst: p.msgs_burst,
        max_queues: p.max_queues,
        max_partitions_per_queue: p.max_partitions_per_queue,
        max_parked_pops: p.max_parked_pops,
        max_payload_bytes: p.max_payload_bytes,
        max_batch_items: p.max_batch_items,
        max_retained_bytes: p.max_retained_bytes,
        max_retention_seconds: p.max_retention_seconds,
    };
    ClusterCtx {
        cluster_id: c.id,
        tenant_id: c.tenant_id,
        broker_tenant: c.broker_tenant_uuid,
        slug: c.slug.clone(),
        cell_base_url: ce.base_url.clone(),
        cell_token: ce.cell_secret.clone(),
        status: crate::cache::merge_status(&t.status, &c.status),
        limits: crate::cache::merge_limits(base, &c.limit_overrides),
        features: crate::cache::parse_features_value(&p.features),
    }
}

/// Why a KV lookup step produced nothing: the two non-`Found` outcomes.
#[derive(Clone, Copy)]
enum Gap {
    Absent,
    Unavailable,
}

impl Gap {
    fn lookup<T>(self) -> Lookup<T> {
        match self {
            Gap::Absent => Lookup::Absent,
            Gap::Unavailable => Lookup::Unavailable,
        }
    }
}

fn lookup_of<T>(r: Result<Option<T>, DataError>, what: &str) -> Result<T, Gap> {
    match r {
        Ok(Some(v)) => Ok(v),
        Ok(None) => Err(Gap::Absent),
        Err(e) => {
            tracing::warn!(error = %e, "{what}: kv lookup failed");
            Err(Gap::Unavailable)
        }
    }
}

/// The JOIN: tenant, cell and plan of `c`, in one read. A missing parent is
/// the JOIN matching no row: Absent.
async fn kv_ctx_for(kv: &dyn KvBackend, c: &ClusterDoc) -> Lookup<ClusterCtx> {
    let out = match gets(
        kv,
        &[
            (ns::TENANTS, schema::key(c.tenant_id)),
            (ns::CELLS, schema::key(c.cell_id)),
            (ns::PLANS, schema::key(c.plan_id)),
        ],
    )
    .await
    {
        Ok(o) => o,
        Err(e) => {
            tracing::warn!(error = %e, cluster = %c.id, "resolve_host: kv lookup failed");
            return Lookup::Unavailable;
        }
    };
    let t = got::<TenantDoc>(out.first());
    let ce = got::<CellDoc>(out.get(1));
    let p = got::<PlanDoc>(out.get(2));
    match (t, ce, p) {
        (Ok(Some(t)), Ok(Some(ce)), Ok(Some(p))) => Lookup::Found(ctx_from_docs(c, &t.value, &ce.value, &p.value)),
        (Err(_), _, _) | (_, Err(_), _) | (_, _, Err(_)) => Lookup::Unavailable,
        _ => Lookup::Absent,
    }
}

async fn kv_cluster_doc(kv: &dyn KvBackend, key: &ClusterKey) -> Result<ClusterDoc, Gap> {
    let id = match key {
        ClusterKey::Id(id) => *id,
        ClusterKey::Slug(slug) => {
            lookup_of(
                read1::<Uuid>(kv, ns::CLUSTER_SLUG, &schema::key(slug)).await,
                "resolve_host",
            )?
            .value
        }
    };
    Ok(lookup_of(
        read1::<ClusterDoc>(kv, ns::CLUSTERS, &schema::key(id)).await,
        "resolve_host",
    )?
    .value)
}

async fn kv_lookup_cluster(kv: &dyn KvBackend, key: &ClusterKey) -> Lookup<ClusterCtx> {
    match kv_cluster_doc(kv, key).await {
        Ok(c) => kv_ctx_for(kv, &c).await,
        Err(g) => g.lookup(),
    }
}

async fn kv_lookup_key(kv: &dyn KvBackend, hash_hex: &str) -> Lookup<(ClusterCtx, Uuid, Scopes)> {
    let id = match lookup_of(
        read1::<Uuid>(kv, ns::KEY_HASH, &schema::key(hash_hex)).await,
        "by_key_hash",
    ) {
        Ok(d) => d.value,
        Err(g) => return g.lookup(),
    };
    let key = match lookup_of(read1::<ApiKeyDoc>(kv, ns::KEYS, &schema::key(id)).await, "by_key_hash") {
        Ok(d) => d.value,
        Err(g) => return g.lookup(),
    };
    // `ak.revoked_at IS NULL`, and the hash the index claims (a dangling or
    // stale index entry is no key at all).
    if key.revoked_at_us.is_some() || key.key_hash != hash_hex {
        return Lookup::Absent;
    }
    let cluster = match kv_cluster_doc(kv, &ClusterKey::Id(key.cluster_id)).await {
        Ok(c) => c,
        Err(g) => return g.lookup(),
    };
    match kv_ctx_for(kv, &cluster).await {
        Lookup::Found(ctx) => Lookup::Found((ctx, key.id, scopes_of(&key.scopes))),
        Lookup::Absent => Lookup::Absent,
        Lookup::Unavailable => Lookup::Unavailable,
    }
}

/// `api_keys.last_used_at = now()` for every key a lookup found since the
/// last flush (the batched touch). Kv: a version-checked rewrite of each
/// ApiKeyDoc — a lost race (a revoke in between) skips that key rather than
/// resurrecting it — and at most one per key per [`TOUCH_MIN_INTERVAL_US`].
pub async fn touch_api_keys(store: &Store, ids: &[Uuid]) -> Result<(), DataError> {
    if ids.is_empty() {
        return Ok(());
    }
    match store {
        Store::Pg(pool) => {
            let ids: Vec<String> = ids.iter().map(Uuid::to_string).collect();
            let client = pool.get().await.map_err(DataError::pool)?;
            client
                .execute(
                    "UPDATE queen_proxy.api_keys SET last_used_at = now() WHERE id = ANY($1::text[]::uuid[])",
                    &[&ids],
                )
                .await
                .map(|_| ())
                .map_err(|e| DataError::pg(&e))
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let docs: Vec<Doc<ApiKeyDoc>> = docs_by_id(kv, ns::KEYS, ids).await?;
            let now = now_us();
            let mut cur = Batch::default();
            let mut batches = Vec::new();
            for d in docs {
                if d.value.last_used_at_us.is_some_and(|t| now - t < TOUCH_MIN_INTERVAL_US) {
                    continue;
                }
                let mut v = d.value;
                v.last_used_at_us = Some(now);
                if cur.len() >= CHUNK_OPS {
                    batches.push(std::mem::take(&mut cur));
                }
                // NOT required: a key rewritten meanwhile keeps its newer self.
                cur.op(kv::put_op(
                    ns::KEYS,
                    &schema::key(v.id),
                    &v,
                    Expect::Version(d.version),
                    Ttl::Forever,
                    false,
                ));
            }
            if !cur.is_empty() {
                batches.push(cur);
            }
            commit_all(kv, batches).await
        }
        Store::None => Ok(()),
    }
}

// ===========================================================================
// the invalidation feed (Kv arm of queen_proxy_inval)
// ===========================================================================

/// What one poll of the feed found.
#[derive(Debug, PartialEq, Eq)]
pub enum InvalPoll {
    /// Nothing moved since the last poll.
    Quiet,
    /// The first answer this feed ever got: nothing is known to have changed,
    /// but nothing cached before it can be vouched for either.
    Baseline,
    /// These clusters changed.
    Changed(Vec<Uuid>),
}

/// One node's cursor over the invalidation marks (module doc). Held by the
/// cache's poll task.
#[derive(Default)]
pub struct InvalFeed {
    /// The counter's version at the last poll (`Some(0)`: never bumped).
    head: Option<u64>,
    /// cluster -> version of its mark at the last listing.
    marks: HashMap<Uuid, u64>,
}

impl InvalFeed {
    pub fn new() -> InvalFeed {
        InvalFeed::default()
    }

    pub async fn poll(&mut self, kv: &dyn KvBackend) -> Result<InvalPoll, DataError> {
        let head = read1::<Value>(kv, ns::META, INVAL_COUNTER)
            .await?
            .map(|d| d.version)
            .unwrap_or(0);
        if self.head == Some(head) {
            return Ok(InvalPoll::Quiet);
        }
        let rows: Vec<(String, Doc<Value>)> = kv::scan(kv, ns::META, INVAL_MARK_PREFIX).await.map_err(DataError::kv)?;
        let now: HashMap<Uuid, u64> = rows
            .into_iter()
            .filter_map(|(k, d)| {
                Uuid::parse_str(&k[INVAL_MARK_PREFIX.len()..])
                    .ok()
                    .map(|id| (id, d.version))
            })
            .collect();
        let first = self.head.is_none();
        let changed: Vec<Uuid> = if first {
            Vec::new()
        } else {
            let mut c: Vec<Uuid> = now
                .iter()
                .filter(|(id, v)| self.marks.get(id) != Some(v))
                .map(|(id, _)| *id)
                .collect();
            c.sort();
            c
        };
        self.marks = now;
        self.head = Some(head);
        Ok(if first {
            InvalPoll::Baseline
        } else {
            InvalPoll::Changed(changed)
        })
    }
}

// ===========================================================================
// auth: the deny-list, memberships, the operator bit
// ===========================================================================

/// Is this jti on the deny-list (`revoked_tokens`)? Err = the store did not
/// answer: auth.rs applies its fail-open/closed policy to that.
pub async fn is_jti_revoked(store: &Store, jti: &str) -> Result<bool, DataError> {
    match store {
        Store::Pg(pool) => {
            let client = pool.get().await.map_err(DataError::pool)?;
            client
                .query_opt(
                    // jti column is TEXT by design (001_init.sql): the deny-list
                    // must accept non-UUID jtis from foreign token mints too.
                    "SELECT 1 FROM queen_proxy.revoked_tokens WHERE jti = $1",
                    &[&jti],
                )
                .await
                .map(|row| row.is_some())
                .map_err(|e| DataError::pg(&e))
        }
        Store::Kv(kv) => Ok(read1::<RevokedDoc>(kv.as_ref(), ns::REVOKED, &schema::key(jti))
            .await?
            .is_some()),
        Store::None => Ok(false),
    }
}

/// Does this user row exist?
pub async fn user_exists(store: &Store, user_id: Uuid) -> Result<bool, DataError> {
    match store {
        Store::Pg(pool) => {
            let client = pool.get().await.map_err(DataError::pool)?;
            client
                .query_opt(
                    "SELECT 1 FROM queen_proxy.users WHERE id = $1::text::uuid",
                    &[&user_id.to_string()],
                )
                .await
                .map(|row| row.is_some())
                .map_err(|e| DataError::pg(&e))
        }
        Store::Kv(kv) => Ok(read1::<UserDoc>(kv.as_ref(), ns::USERS, &schema::key(user_id))
            .await?
            .is_some()),
        Store::None => Ok(false),
    }
}

/// The user's role on the cluster (`cluster_roles.role`), if any.
pub async fn cluster_role(store: &Store, user_id: Uuid, cluster_id: Uuid) -> Result<Option<String>, DataError> {
    match store {
        Store::Pg(pool) => {
            let client = pool.get().await.map_err(DataError::pool)?;
            let uid = user_id.to_string();
            let cid = cluster_id.to_string();
            client
                .query_opt(
                    "SELECT role FROM queen_proxy.cluster_roles \
                     WHERE user_id = $1::text::uuid AND cluster_id = $2::text::uuid",
                    &[&uid, &cid],
                )
                .await
                .map(|row| row.map(|r| r.get::<_, String>(0)))
                .map_err(|e| DataError::pg(&e))
        }
        Store::Kv(kv) => Ok(
            read1::<RoleDoc>(kv.as_ref(), ns::ROLES, &schema::key2(user_id, cluster_id))
                .await?
                .map(|d| d.value.role),
        ),
        Store::None => Ok(None),
    }
}

/// The stored operator bit (`users.is_operator`); false for no such user.
pub async fn user_is_operator(store: &Store, user_id: Uuid) -> Result<bool, DataError> {
    match store {
        Store::Pg(pool) => {
            let client = pool.get().await.map_err(DataError::pool)?;
            client
                .query_opt(
                    "SELECT is_operator FROM queen_proxy.users WHERE id = $1::text::uuid",
                    &[&user_id.to_string()],
                )
                .await
                .map(|row| row.is_some_and(|r| r.get::<_, bool>(0)))
                .map_err(|e| DataError::pg(&e))
        }
        Store::Kv(kv) => Ok(read1::<UserDoc>(kv.as_ref(), ns::USERS, &schema::key(user_id))
            .await?
            .is_some_and(|d| d.value.is_operator)),
        Store::None => Ok(false),
    }
}

/// `queen_proxy.revoke_session(jti, to_timestamp(expires_at), actor,
/// actor_id)` (004_lifecycle). Idempotent: a second revoke of one jti is not
/// an error. Kv: the deny-list row carries a TTL until the token's own expiry,
/// so the store sweeps it by itself.
pub async fn revoke_session(
    store: &Store,
    jti: &str,
    expires_at_unix: i64,
    actor: &str,
    actor_id: Uuid,
) -> Result<(), DataError> {
    match store {
        Store::Pg(pool) => {
            let client = pool.get().await.map_err(DataError::pool)?;
            client
                .execute(
                    "SELECT queen_proxy.revoke_session($1, to_timestamp($2), $3, $4::text::uuid)",
                    &[&jti, &(expires_at_unix as f64), &actor, &actor_id.to_string()],
                )
                .await
                .map(|_| ())
                .map_err(|e| DataError::pg(&e))
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let jti = btrim(jti);
            if jti.is_empty() {
                return Err(DataError::Invalid("revoke_session: jti must not be empty".into()));
            }
            let Some(user) = read1::<UserDoc>(kv, ns::USERS, &schema::key(actor_id)).await? else {
                return Err(DataError::Invalid(format!(
                    "revoke_session: actor_id must be a known user {actor_id}"
                )));
            };
            if !ACTORS.contains(&actor) {
                return Err(DataError::Invalid(format!("record_operation: invalid actor {actor}")));
            }
            let now = now_us();
            let exp_us = expires_at_unix.saturating_mul(1_000_000);
            let ttl = ((exp_us - now) / 1_000_000 + 1).max(1) as u64;
            let mut b = Batch::default();
            // ON CONFLICT (jti) DO NOTHING: not required, a double logout is fine.
            b.op(kv::put_op(
                ns::REVOKED,
                &schema::key(jti),
                &RevokedDoc {
                    jti: jti.to_string(),
                    expires_at_us: exp_us,
                },
                Expect::Absent,
                Ttl::Seconds(ttl),
                false,
            ));
            record_op(
                &mut b,
                now,
                user.value.tenant_id,
                None,
                actor,
                Some(actor_id),
                "session_revoked",
                Some(jti.to_string()),
                json!({ "expires_at": iso_utc(exp_us) }),
            );
            commit_all(kv, vec![b]).await
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// `queen_proxy.sweep_revoked_tokens()`: drop deny-list rows past their own
/// token's expiry; the number dropped. Kv: the rows' TTL already does it; this
/// catches any written without one (an import).
pub async fn sweep_revoked_tokens(store: &Store) -> Result<i64, DataError> {
    match store {
        Store::Pg(pool) => {
            let client = pool.get().await.map_err(DataError::pool)?;
            let row = client
                .query_one("SELECT queen_proxy.sweep_revoked_tokens()", &[])
                .await
                .map_err(|e| DataError::pg(&e))?;
            Ok(i64::from(row.get::<_, i32>(0)))
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let now = now_us();
            let rows: Vec<(String, Doc<RevokedDoc>)> = kv::scan(kv, ns::REVOKED, K).await.map_err(DataError::kv)?;
            let mut batches = Vec::new();
            let mut cur = Batch::default();
            let mut n = 0i64;
            for (k, d) in rows {
                if d.value.expires_at_us > now {
                    continue;
                }
                if cur.len() >= CHUNK_OPS {
                    batches.push(std::mem::take(&mut cur));
                }
                cur.op(kv::delete_op(ns::REVOKED, &k, Expect::Version(d.version), false));
                n += 1;
            }
            if !cur.is_empty() {
                batches.push(cur);
            }
            commit_all(kv, batches).await?;
            Ok(n)
        }
        Store::None => Ok(0),
    }
}

// ===========================================================================
// registry: the queue-row cache behind plan-cap admission
// ===========================================================================

/// queen_proxy.queues.partitions_count is INTEGER (001_init.sql), while
/// partition counts are carried as i64 throughout registry.rs. Binding the i64
/// straight into the statement makes tokio-postgres reject EVERY upsert with
/// "error serializing parameter 2", which is how the table stayed permanently
/// empty — silently, since both write paths only warn. Narrow at the bind sites
/// and keep the in-process arithmetic in i64.
fn clamp_partitions(n: i64) -> i32 {
    n.clamp(0, i32::MAX as i64) as i32
}

/// Live queues of a cluster and their recorded partition counts
/// (`deleted_at IS NULL`).
pub async fn live_queues(store: &Store, cluster_id: Uuid) -> Result<Vec<(String, i64)>, DataError> {
    match store {
        Store::Pg(pool) => {
            let client = pool.get().await.map_err(DataError::pool)?;
            let cluster_id_str = cluster_id.to_string();
            let stmt = "SELECT name, partitions_count FROM queen_proxy.queues \
                        WHERE cluster_id = $1::text::uuid AND deleted_at IS NULL";
            let rows = client
                .query(stmt, &[&cluster_id_str])
                .await
                .map_err(|e| DataError::pg(&e))?;
            Ok(rows
                .into_iter()
                .map(|r| {
                    let name: String = r.get(0);
                    let count: i32 = r.get(1);
                    (name, count as i64)
                })
                .collect())
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let pfx = schema::prefix(cluster_id);
            let idx: Vec<(String, Doc<Uuid>)> = kv::scan(kv, ns::QUEUE_NAME, &pfx).await.map_err(DataError::kv)?;
            let ids: Vec<Uuid> = idx.iter().map(|(_, d)| d.value).collect();
            let docs: HashMap<Uuid, QueueDoc> = docs_by_id::<QueueDoc>(kv, ns::QUEUES, &ids)
                .await?
                .into_iter()
                .map(|d| (d.value.id, d.value))
                .collect();
            Ok(idx
                .into_iter()
                .filter_map(|(k, d)| {
                    let q = docs.get(&d.value)?;
                    (q.deleted_at_us.is_none() && q.cluster_id == cluster_id)
                        .then(|| (k[pfx.len()..].to_string(), q.partitions_count))
                })
                .collect())
        }
        Store::None => Ok(Vec::new()),
    }
}

/// One multi-row upsert per flush. Arrays bind as text/int4 and cast in SQL
/// (no uuid feature on tokio-postgres, like every other query in this crate);
/// `GREATEST` keeps the row a floor that only the reconciler, which knows the
/// broker's true count, ever lowers. Each (cluster, name) appears at most once
/// per batch (it is the map key), which `ON CONFLICT DO UPDATE` requires.
const PERSIST_SQL: &str = "INSERT INTO queen_proxy.queues(cluster_id, name, partitions_count) \
    SELECT c::uuid, n, p FROM UNNEST($1::text[], $2::text[], $3::int4[]) AS t(c, n, p) \
    ON CONFLICT (cluster_id, name) WHERE deleted_at IS NULL \
    DO UPDATE SET partitions_count = GREATEST(queen_proxy.queues.partitions_count, EXCLUDED.partitions_count)";

/// The registry persister's write: each (cluster, queue) row raised to at
/// least `count` (created when absent). All or nothing from the caller's view:
/// Err means "put the batch back for the next tick".
pub async fn persist_queue_floors(store: &Store, rows: &[(Uuid, String, i64)]) -> Result<(), DataError> {
    if rows.is_empty() {
        return Ok(());
    }
    match store {
        Store::Pg(pool) => {
            let mut ids = Vec::with_capacity(rows.len());
            let mut names = Vec::with_capacity(rows.len());
            let mut counts = Vec::with_capacity(rows.len());
            for (cluster_id, name, count) in rows {
                ids.push(cluster_id.to_string());
                names.push(name.clone());
                counts.push(clamp_partitions(*count));
            }
            let client = pool.get().await.map_err(DataError::pool)?;
            client
                .execute(PERSIST_SQL, &[&ids, &names, &counts])
                .await
                .map(|_| ())
                .map_err(|e| DataError::pg(&e))
        }
        Store::Kv(kv) => {
            let done = kv_upsert_queues(kv.as_ref(), rows, true).await?;
            if done.iter().all(|d| *d) {
                Ok(())
            } else {
                Err(DataError::Unavailable(
                    "queue upsert contended; retrying next tick".into(),
                ))
            }
        }
        Store::None => Ok(()),
    }
}

/// Upsert queue rows by (cluster, name) among LIVE rows — the partial unique
/// index `uq_queues_cluster_name_live`. `grow_only`: GREATEST (the persister);
/// else the count is set (the reconciler). Per-row success.
async fn kv_upsert_queues(
    kv: &dyn KvBackend,
    rows: &[(Uuid, String, i64)],
    grow_only: bool,
) -> Result<Vec<bool>, DataError> {
    let mut ok = vec![false; rows.len()];
    let mut todo: Vec<usize> = (0..rows.len()).collect();
    for _ in 0..ATTEMPTS {
        if todo.is_empty() {
            break;
        }
        let mut failed = Vec::new();
        // 2 writes per new row at most.
        for chunk in todo.chunks(CHUNK_OPS / 2) {
            let idx_keys: Vec<String> = chunk.iter().map(|&i| schema::key2(rows[i].0, &rows[i].1)).collect();
            let idx = kv::get_many::<Uuid>(kv, ns::QUEUE_NAME, &idx_keys)
                .await
                .map_err(DataError::kv)?;
            let ids: Vec<Uuid> = idx.iter().flatten().map(|d| d.value).collect();
            let docs: HashMap<Uuid, Doc<QueueDoc>> = docs_by_id::<QueueDoc>(kv, ns::QUEUES, &ids)
                .await?
                .into_iter()
                .map(|d| (d.value.id, d))
                .collect();
            let now = now_us();
            let mut b = Batch::default();
            let mut members = Vec::new();
            for (j, &i) in chunk.iter().enumerate() {
                let (cluster, name, count) = (&rows[i].0, &rows[i].1, rows[i].2.max(0));
                let fresh = || QueueDoc {
                    id: Uuid::new_v4(),
                    cluster_id: *cluster,
                    name: name.clone(),
                    partitions_count: count,
                    created_at_us: now,
                    deleted_at_us: None,
                };
                match &idx[j] {
                    Some(ix) => match docs.get(&ix.value) {
                        Some(d) if d.value.deleted_at_us.is_none() => {
                            let old = d.value.partitions_count;
                            let new = if grow_only { old.max(count) } else { count };
                            if new == old {
                                ok[i] = true;
                                continue;
                            }
                            let mut q = d.value.clone();
                            q.partitions_count = new;
                            b.put_at(ns::QUEUES, &schema::key(q.id), &q, d.version);
                        }
                        // The index names a row that is gone or tombstoned: a
                        // fresh live row takes the name over.
                        _ => {
                            let q = fresh();
                            b.put_at(ns::QUEUE_NAME, &idx_keys[j], &q.id, ix.version);
                            b.put_new(ns::QUEUES, &schema::key(q.id), &q);
                        }
                    },
                    None => {
                        let q = fresh();
                        b.claim(ns::QUEUE_NAME, &idx_keys[j], q.id, "uq_queues_cluster_name_live");
                        b.put_new(ns::QUEUES, &schema::key(q.id), &q);
                    }
                }
                members.push(i);
            }
            if b.is_empty() {
                continue;
            }
            match commit(kv, b).await {
                Ok(_) => members.into_iter().for_each(|i| ok[i] = true),
                // Another node created or rewrote one of them: re-read the chunk.
                Err(WriteErr::Moved) | Err(WriteErr::Conflict(_)) => failed.extend(members),
                Err(WriteErr::Fail(e)) => return Err(e),
            }
        }
        todo = failed;
    }
    Ok(ok)
}

/// A cluster the reconciler polls: where its cell is, who it is upstream, and
/// its effective storage cap (plan merged with overrides).
#[derive(Clone, Debug, PartialEq)]
pub struct ReconcileTarget {
    pub cluster_id: Uuid,
    pub broker_tenant: String,
    pub base_url: String,
    pub cell_secret: Option<String>,
    pub max_retained_bytes: Option<i64>,
}

/// Every cluster not being torn down, with its cell and storage cap.
pub async fn reconcile_targets(store: &Store) -> Result<Vec<ReconcileTarget>, DataError> {
    match store {
        Store::Pg(pool) => {
            let client = pool.get().await.map_err(DataError::pool)?;
            // Reconcile everything not being torn down -- push_blocked clusters
            // especially still need this loop: it's the only thing that re-evaluates
            // their byte count, so skipping them would make the block permanent.
            let stmt = "SELECT c.id::text, c.broker_tenant_uuid::text, ce.base_url, ce.cell_secret, \
                               p.max_retained_bytes, (c.limit_overrides)::text \
                        FROM queen_proxy.clusters c \
                        JOIN queen_proxy.cells  ce ON ce.id = c.cell_id \
                        JOIN queen_proxy.plans  p  ON p.id = c.plan_id \
                        WHERE c.status <> 'deleting'";
            let rows = client.query(stmt, &[]).await.map_err(|e| DataError::pg(&e))?;
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let id_str: String = row.get(0);
                let Ok(cluster_id) = Uuid::parse_str(&id_str) else {
                    tracing::warn!(id = %id_str, "registry reconciler: unparseable cluster id, skipping");
                    continue;
                };
                let broker_tenant: String = row.get(1);
                let base_url: String = row.get(2);
                let cell_secret: Option<String> = row.get(3);
                let max_retained_plan: Option<i64> = row.get(4);
                let overrides_json: String = row.get(5);
                let overrides: Value = serde_json::from_str(&overrides_json).unwrap_or(Value::Null);
                let max_retained_bytes = crate::cache::override_or(&overrides, "max_retained_bytes", max_retained_plan);
                out.push(ReconcileTarget {
                    cluster_id,
                    broker_tenant,
                    base_url,
                    cell_secret,
                    max_retained_bytes,
                });
            }
            Ok(out)
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let clusters: Vec<ClusterDoc> = kv::scan::<ClusterDoc>(kv, ns::CLUSTERS, K)
                .await
                .map_err(DataError::kv)?
                .into_iter()
                .map(|(_, d)| d.value)
                .filter(|c| c.status != "deleting")
                .collect();
            let cell_ids: Vec<Uuid> = clusters
                .iter()
                .map(|c| c.cell_id)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let plan_ids: Vec<Uuid> = clusters
                .iter()
                .map(|c| c.plan_id)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let cells: HashMap<Uuid, CellDoc> = docs_by_id::<CellDoc>(kv, ns::CELLS, &cell_ids)
                .await?
                .into_iter()
                .map(|d| (d.value.id, d.value))
                .collect();
            let plans: HashMap<Uuid, PlanDoc> = docs_by_id::<PlanDoc>(kv, ns::PLANS, &plan_ids)
                .await?
                .into_iter()
                .map(|d| (d.value.id, d.value))
                .collect();
            Ok(clusters
                .into_iter()
                .filter_map(|c| {
                    let cell = cells.get(&c.cell_id)?;
                    let plan = plans.get(&c.plan_id)?;
                    Some(ReconcileTarget {
                        cluster_id: c.id,
                        broker_tenant: c.broker_tenant_uuid.to_string(),
                        base_url: cell.base_url.clone(),
                        cell_secret: cell.cell_secret.clone(),
                        max_retained_bytes: crate::cache::override_or(
                            &c.limit_overrides,
                            "max_retained_bytes",
                            plan.max_retained_bytes,
                        ),
                    })
                })
                .collect())
        }
        Store::None => Ok(Vec::new()),
    }
}

/// The reconciler's write of the broker's own counts: each row SET to the
/// broker's number (created when absent). Outer Err: the store could not be
/// reached at all (skip this cluster's sync); inner: per row.
pub async fn reconcile_queue_counts(
    store: &Store,
    cluster_id: Uuid,
    rows: &[(String, i64)],
) -> Result<Vec<Result<(), String>>, DataError> {
    match store {
        Store::Pg(pool) => {
            let client = pool.get().await.map_err(DataError::pool)?;
            let cluster_id_str = cluster_id.to_string();
            let mut out = Vec::with_capacity(rows.len());
            for (name, partitions) in rows {
                let count = clamp_partitions(*partitions);
                let stmt = "INSERT INTO queen_proxy.queues(cluster_id, name, partitions_count) \
                            VALUES ($1::text::uuid, $2, $3) \
                            ON CONFLICT (cluster_id, name) WHERE deleted_at IS NULL \
                            DO UPDATE SET partitions_count = EXCLUDED.partitions_count";
                out.push(
                    client
                        .execute(stmt, &[&cluster_id_str, name, &count])
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string()),
                );
            }
            Ok(out)
        }
        Store::Kv(kv) => {
            // One write per key per call: a name listed twice is one row (the
            // last count wins, as the second of two sequential upserts would).
            let mut pos: HashMap<&str, usize> = HashMap::new();
            let mut uniq: Vec<(Uuid, String, i64)> = Vec::with_capacity(rows.len());
            for (n, c) in rows {
                match pos.get(n.as_str()) {
                    Some(&i) => uniq[i].2 = *c,
                    None => {
                        pos.insert(n.as_str(), uniq.len());
                        uniq.push((cluster_id, n.clone(), *c));
                    }
                }
            }
            let done = kv_upsert_queues(kv.as_ref(), &uniq, false).await?;
            Ok(rows
                .iter()
                .map(|(n, _)| {
                    if done[pos[n.as_str()]] {
                        Ok(())
                    } else {
                        Err("contended".to_string())
                    }
                })
                .collect())
        }
        Store::None => Ok(rows.iter().map(|_| Ok(())).collect()),
    }
}

/// Soft-delete the live queue rows of a cluster the broker no longer lists
/// (`deleted_at = now()`); the name becomes reusable.
pub async fn sweep_deleted_queues(store: &Store, cluster_id: Uuid, seen_names: &[String]) -> Result<(), DataError> {
    match store {
        Store::Pg(pool) => {
            let client = pool.get().await.map_err(DataError::pool)?;
            let cluster_id_str = cluster_id.to_string();
            let sweep_stmt = "UPDATE queen_proxy.queues SET deleted_at = now() \
                               WHERE cluster_id = $1::text::uuid AND deleted_at IS NULL AND NOT (name = ANY($2))";
            client
                .execute(sweep_stmt, &[&cluster_id_str, &seen_names])
                .await
                .map(|_| ())
                .map_err(|e| DataError::pg(&e))
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let pfx = schema::prefix(cluster_id);
            let seen: HashSet<&str> = seen_names.iter().map(String::as_str).collect();
            let idx: Vec<(String, Doc<Uuid>)> = kv::scan(kv, ns::QUEUE_NAME, &pfx).await.map_err(DataError::kv)?;
            let gone: Vec<(String, Doc<Uuid>)> = idx
                .into_iter()
                .filter(|(k, _)| !seen.contains(&k[pfx.len()..]))
                .collect();
            if gone.is_empty() {
                return Ok(());
            }
            let ids: Vec<Uuid> = gone.iter().map(|(_, d)| d.value).collect();
            let docs: HashMap<Uuid, Doc<QueueDoc>> = docs_by_id::<QueueDoc>(kv, ns::QUEUES, &ids)
                .await?
                .into_iter()
                .map(|d| (d.value.id, d))
                .collect();
            let now = now_us();
            let mut batches = Vec::new();
            let mut cur = Batch::default();
            for (k, ix) in gone {
                if cur.len() + 2 > CHUNK_OPS {
                    batches.push(std::mem::take(&mut cur));
                }
                if let Some(d) = docs.get(&ix.value).filter(|d| d.value.deleted_at_us.is_none()) {
                    let mut q = d.value.clone();
                    q.deleted_at_us = Some(now);
                    cur.put_at(ns::QUEUES, &schema::key(q.id), &q, d.version);
                }
                cur.del_at(ns::QUEUE_NAME, &k, ix.version);
            }
            if !cur.is_empty() {
                batches.push(cur);
            }
            commit_all(kv, batches).await
        }
        Store::None => Ok(()),
    }
}

// ===========================================================================
// control plane: the queen_proxy.* writers of these tables
// ===========================================================================

async fn pg_uuid(
    pool: &deadpool_postgres::Pool,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<Uuid, DataError> {
    let client = pool.get().await.map_err(DataError::pool)?;
    let row = client.query_one(sql, params).await.map_err(|e| DataError::pg(&e))?;
    let s: String = row.get(0);
    Uuid::parse_str(&s).map_err(|e| DataError::Unavailable(format!("bad uuid {s:?}: {e}")))
}

async fn pg_exec(
    pool: &deadpool_postgres::Pool,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<(), DataError> {
    let client = pool.get().await.map_err(DataError::pool)?;
    client
        .execute(sql, params)
        .await
        .map(|_| ())
        .map_err(|e| DataError::pg(&e))
}

async fn pg_json(
    pool: &deadpool_postgres::Pool,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<Value, DataError> {
    let client = pool.get().await.map_err(DataError::pool)?;
    let row = client.query_one(sql, params).await.map_err(|e| DataError::pg(&e))?;
    let s: String = row.get(0);
    serde_json::from_str(&s).map_err(|e| DataError::Unavailable(format!("bad json: {e}")))
}

/// Drive a read-modify-write attempt to completion: `Moved` retries.
macro_rules! attempts {
    ($body:expr) => {{
        let mut last = DataError::Unavailable("kv: concurrent change, gave up".into());
        let mut out = None;
        for _ in 0..ATTEMPTS {
            match $body {
                Ok(v) => {
                    out = Some(Ok(v));
                    break;
                }
                Err(WriteErr::Moved) => continue,
                Err(WriteErr::Conflict(m)) => {
                    last = DataError::Conflict(m);
                    break;
                }
                Err(WriteErr::Fail(e)) => {
                    last = e;
                    break;
                }
            }
        }
        out.unwrap_or(Err(last))
    }};
}

async fn plan_by_code(kv: &dyn KvBackend, code: &str) -> Result<Option<PlanDoc>, DataError> {
    let Some(id) = read1::<Uuid>(kv, ns::PLAN_CODE, &schema::key(code)).await? else {
        return Ok(None);
    };
    Ok(read1::<PlanDoc>(kv, ns::PLANS, &schema::key(id.value))
        .await?
        .map(|d| d.value))
}

async fn user_by_email(kv: &dyn KvBackend, email: &str) -> Result<Option<Doc<UserDoc>>, DataError> {
    let Some(id) = read1::<Uuid>(kv, ns::USER_EMAIL, &schema::key(email)).await? else {
        return Ok(None);
    };
    read1::<UserDoc>(kv, ns::USERS, &schema::key(id.value)).await
}

async fn cluster_doc(kv: &dyn KvBackend, id: Uuid) -> Result<Option<Doc<ClusterDoc>>, DataError> {
    read1::<ClusterDoc>(kv, ns::CLUSTERS, &schema::key(id)).await
}

/// The rows of a new tenant (create_tenant's INSERT + audit).
fn new_tenant(b: &mut Batch, now: i64, slug: &str, name: &str) -> Uuid {
    let id = Uuid::new_v4();
    b.claim(ns::TENANT_SLUG, &schema::key(slug), id, "tenants_slug_key");
    b.put_new(
        ns::TENANTS,
        &schema::key(id),
        &TenantDoc {
            id,
            slug: slug.to_string(),
            name: name.to_string(),
            status: "active".into(),
            created_at_us: now,
        },
    );
    record_op(
        b,
        now,
        id,
        None,
        "control_plane",
        None,
        "tenant_created",
        Some(id.to_string()),
        json!({"slug": slug, "name": name}),
    );
    id
}

/// The rows of a new cluster (create_cluster's INSERT + audit + NOTIFY).
fn new_cluster(b: &mut Batch, now: i64, tenant: Uuid, slug: &str, plan: &PlanDoc, cell: Uuid) -> Uuid {
    let id = Uuid::new_v4();
    let doc = ClusterDoc {
        id,
        tenant_id: tenant,
        cell_id: cell,
        plan_id: plan.id,
        slug: slug.to_string(),
        broker_tenant_uuid: Uuid::new_v4(),
        status: "active".into(),
        limit_overrides: json!({}),
        created_at_us: now,
    };
    b.claim(ns::CLUSTER_SLUG, &schema::key(slug), id, "clusters_slug_key");
    b.put_new(ns::CLUSTERS, &schema::key(id), &doc);
    b.put(ns::CLUSTER_TENANT, &schema::key2(tenant, id), &"");
    b.put(ns::CLUSTER_CELL, &schema::key2(cell, id), &"");
    record_op(
        b,
        now,
        tenant,
        Some(id),
        "control_plane",
        None,
        "cluster_created",
        Some(id.to_string()),
        json!({"slug": slug, "plan_code": plan.code, "cell_id": cell}),
    );
    id
}

/// The rows of a new user (create_user's INSERT + audit). users is the web
/// plane's table (store/web.rs); bootstrap_tenant needs to create one.
fn new_user(b: &mut Batch, now: i64, tenant: Uuid, email: &str, password_hash: Option<String>, provider: &str) -> Uuid {
    let id = Uuid::new_v4();
    let doc = UserDoc {
        id,
        tenant_id: tenant,
        email: email.to_string(),
        password_hash,
        name: None,
        is_operator: false,
        last_login_at_us: None,
        created_at_us: now,
    };
    b.claim(ns::USER_EMAIL, &schema::key(email), id, "users_email_key");
    b.put_new(ns::USERS, &schema::key(id), &doc);
    b.put(ns::USER_TENANT, &schema::key2(tenant, id), &"");
    record_op(
        b,
        now,
        tenant,
        None,
        "control_plane",
        Some(id),
        "user_created",
        Some(id.to_string()),
        json!({"email": email, "provider": provider}),
    );
    id
}

/// The rows of a new api key (issue_api_key's INSERT + audit + NOTIFY).
#[allow(clippy::too_many_arguments)]
fn new_api_key(
    b: &mut Batch,
    now: i64,
    tenant: Uuid,
    cluster: Uuid,
    name: &str,
    key_hash: &str,
    scopes: &[String],
) -> Uuid {
    let id = Uuid::new_v4();
    let doc = ApiKeyDoc {
        id,
        cluster_id: cluster,
        name: name.to_string(),
        key_hash: key_hash.to_string(),
        scopes: scopes.to_vec(),
        created_by: None,
        created_at_us: now,
        last_used_at_us: None,
        revoked_at_us: None,
    };
    b.claim(ns::KEY_HASH, &schema::key(key_hash), id, "api_keys_key_hash_key");
    b.put_new(ns::KEYS, &schema::key(id), &doc);
    b.put(ns::KEY_CLUSTER, &schema::key2(cluster, id), &"");
    record_op(
        b,
        now,
        tenant,
        Some(cluster),
        "control_plane",
        None,
        "api_key_issued",
        Some(id.to_string()),
        json!({"name": name, "scopes": scopes}),
    );
    id
}

/// grant_cluster_role's upsert: the role row keeps its created_at.
fn put_role(b: &mut Batch, now: i64, existing: Option<&RoleDoc>, user: Uuid, cluster: Uuid, role: &str) {
    let doc = RoleDoc {
        user_id: user,
        cluster_id: cluster,
        role: role.to_string(),
        created_at_us: existing.map_or(now, |r| r.created_at_us),
    };
    b.put(ns::ROLES, &schema::key2(user, cluster), &doc);
    b.put(ns::ROLE_CLUSTER, &schema::key2(cluster, user), &"");
}

/// `queen_proxy.create_tenant(slug, name)` -> tenant id.
pub async fn create_tenant(store: &Store, slug: &str, name: &str) -> Result<Uuid, DataError> {
    match store {
        Store::Pg(pool) => pg_uuid(pool, "SELECT queen_proxy.create_tenant($1, $2)::text", &[&slug, &name]).await,
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            if btrim(slug).is_empty() {
                return Err(DataError::Invalid("create_tenant: slug must not be empty".into()));
            }
            if btrim(name).is_empty() {
                return Err(DataError::Invalid("create_tenant: name must not be empty".into()));
            }
            let slug = norm(slug);
            if !dns_label_ok(&slug) {
                return Err(check_violation("tenants", "slug"));
            }
            let mut b = Batch::default();
            let id = new_tenant(&mut b, now_us(), &slug, btrim(name));
            commit(kv, b).await.map(|_| id).map_err(write_err)
        }
        Store::None => Err(DataError::NoStore),
    }
}

fn write_err(e: WriteErr) -> DataError {
    match e {
        WriteErr::Conflict(m) => DataError::Conflict(m),
        WriteErr::Moved => DataError::Unavailable("kv: concurrent change, retry".into()),
        WriteErr::Fail(e) => e,
    }
}

/// `queen_proxy.create_cluster(tenant, slug, plan_code, cell)` -> cluster id.
pub async fn create_cluster(
    store: &Store,
    tenant_id: Uuid,
    slug: &str,
    plan_code: &str,
    cell_id: Uuid,
) -> Result<Uuid, DataError> {
    match store {
        Store::Pg(pool) => {
            pg_uuid(
                pool,
                "SELECT queen_proxy.create_cluster($1::text::uuid, $2, $3, $4::text::uuid)::text",
                &[&tenant_id.to_string(), &slug, &plan_code, &cell_id.to_string()],
            )
            .await
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            if btrim(slug).is_empty() {
                return Err(DataError::Invalid("create_cluster: slug must not be empty".into()));
            }
            if read1::<TenantDoc>(kv, ns::TENANTS, &schema::key(tenant_id))
                .await?
                .is_none()
            {
                return Err(DataError::Invalid(format!(
                    "create_cluster: unknown tenant {tenant_id}"
                )));
            }
            let Some(plan) = plan_by_code(kv, plan_code).await? else {
                return Err(DataError::Invalid(format!(
                    "create_cluster: unknown plan code {plan_code}"
                )));
            };
            if read1::<CellDoc>(kv, ns::CELLS, &schema::key(cell_id)).await?.is_none() {
                return Err(DataError::Invalid(format!("create_cluster: unknown cell {cell_id}")));
            }
            let slug = norm(slug);
            if !dns_label_ok(&slug) {
                return Err(check_violation("clusters", "slug"));
            }
            let mut b = Batch::default();
            let id = new_cluster(&mut b, now_us(), tenant_id, &slug, &plan, cell_id);
            commit(kv, b).await.map(|_| id).map_err(write_err)
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// Rewrite one cluster doc under its version, with its audit row.
async fn kv_edit_cluster(
    kv: &dyn KvBackend,
    cluster_id: Uuid,
    unknown: &str,
    action: &str,
    meta: Value,
    edit: impl Fn(&mut ClusterDoc),
) -> Result<(), DataError> {
    attempts!(
        async {
            let Some(c) = cluster_doc(kv, cluster_id).await? else {
                return Err(WriteErr::Fail(DataError::Invalid(format!("{unknown} {cluster_id}"))));
            };
            let mut doc = c.value.clone();
            edit(&mut doc);
            let mut b = Batch::default();
            let now = now_us();
            b.put_at(ns::CLUSTERS, &schema::key(cluster_id), &doc, c.version);
            record_op(
                &mut b,
                now,
                doc.tenant_id,
                Some(cluster_id),
                "control_plane",
                None,
                action,
                Some(cluster_id.to_string()),
                meta.clone(),
            );
            commit(kv, b).await.map(|_| ())
        }
        .await
    )
}

/// `queen_proxy.assign_plan(cluster, plan_code)`.
pub async fn assign_plan(store: &Store, cluster_id: Uuid, plan_code: &str) -> Result<(), DataError> {
    match store {
        Store::Pg(pool) => {
            pg_exec(
                pool,
                "SELECT queen_proxy.assign_plan($1::text::uuid, $2)",
                &[&cluster_id.to_string(), &plan_code],
            )
            .await
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let Some(plan) = plan_by_code(kv, plan_code).await? else {
                return Err(DataError::Invalid(format!(
                    "assign_plan: unknown plan code {plan_code}"
                )));
            };
            kv_edit_cluster(
                kv,
                cluster_id,
                "assign_plan: unknown cluster",
                "plan_assigned",
                json!({"plan_code": plan_code}),
                |c| c.plan_id = plan.id,
            )
            .await
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// `queen_proxy.set_cluster_status(cluster, status)`.
pub async fn set_cluster_status(store: &Store, cluster_id: Uuid, status: &str) -> Result<(), DataError> {
    match store {
        Store::Pg(pool) => {
            pg_exec(
                pool,
                "SELECT queen_proxy.set_cluster_status($1::text::uuid, $2)",
                &[&cluster_id.to_string(), &status],
            )
            .await
        }
        Store::Kv(kv) => {
            if !CLUSTER_STATUSES.contains(&status) {
                return Err(DataError::Invalid(format!(
                    "set_cluster_status: invalid status {status}"
                )));
            }
            kv_edit_cluster(
                kv.as_ref(),
                cluster_id,
                "set_cluster_status: unknown cluster",
                "cluster_status_changed",
                json!({"status": status}),
                |c| c.status = status.to_string(),
            )
            .await
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// `queen_proxy.set_limit_override(cluster, overrides)`; None clears (`'{}'`).
pub async fn set_limit_override(store: &Store, cluster_id: Uuid, overrides: Option<&Value>) -> Result<(), DataError> {
    match store {
        Store::Pg(pool) => {
            let json = overrides.map(Value::to_string);
            pg_exec(
                pool,
                "SELECT queen_proxy.set_limit_override($1::text::uuid, $2::text::jsonb)",
                &[&cluster_id.to_string(), &json],
            )
            .await
        }
        Store::Kv(kv) => {
            let o = overrides.cloned().unwrap_or_else(|| json!({}));
            kv_edit_cluster(
                kv.as_ref(),
                cluster_id,
                "set_limit_override: unknown cluster",
                "limit_override_set",
                o.clone(),
                |c| c.limit_overrides = o.clone(),
            )
            .await
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// `queen_proxy.set_tenant_status(tenant, status)`: every cluster of the
/// tenant is invalidated (its effective status is the worse of the two).
pub async fn set_tenant_status(store: &Store, tenant_id: Uuid, status: &str) -> Result<(), DataError> {
    match store {
        Store::Pg(pool) => {
            pg_exec(
                pool,
                "SELECT queen_proxy.set_tenant_status($1::text::uuid, $2)",
                &[&tenant_id.to_string(), &status],
            )
            .await
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            if !TENANT_STATUSES.contains(&status) {
                return Err(DataError::Invalid(format!(
                    "set_tenant_status: invalid status {status}"
                )));
            }
            attempts!(
                async {
                    let Some(t) = read1::<TenantDoc>(kv, ns::TENANTS, &schema::key(tenant_id)).await? else {
                        return Err(WriteErr::Fail(DataError::Invalid(format!(
                            "set_tenant_status: unknown tenant {tenant_id}"
                        ))));
                    };
                    let clusters = index_ids(kv, ns::CLUSTER_TENANT, tenant_id).await?;
                    let mut doc = t.value.clone();
                    doc.status = status.to_string();
                    let now = now_us();
                    let mut b = Batch::default();
                    b.put_at(ns::TENANTS, &schema::key(tenant_id), &doc, t.version);
                    record_op(
                        &mut b,
                        now,
                        tenant_id,
                        None,
                        "control_plane",
                        None,
                        "tenant_status_changed",
                        Some(tenant_id.to_string()),
                        json!({"status": status}),
                    );
                    clusters.into_iter().for_each(|c| b.invalidate(c));
                    commit(kv, b).await.map(|_| ())
                }
                .await
            )
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// `queen_proxy.issue_api_key(cluster, name, key_hash, scopes)` -> key id.
/// `key_hash` is already the sha256 hex (auth::key_hash_hex).
pub async fn issue_api_key(
    store: &Store,
    cluster_id: Uuid,
    name: &str,
    key_hash: &str,
    scopes: &[String],
) -> Result<Uuid, DataError> {
    match store {
        Store::Pg(pool) => {
            pg_uuid(
                pool,
                "SELECT queen_proxy.issue_api_key($1::text::uuid, $2, $3, $4)::text AS id",
                &[&cluster_id.to_string(), &name, &key_hash, &scopes],
            )
            .await
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let Some(c) = cluster_doc(kv, cluster_id).await? else {
                return Err(DataError::Invalid(format!(
                    "issue_api_key: unknown cluster {cluster_id}"
                )));
            };
            if btrim(name).is_empty() {
                return Err(DataError::Invalid("issue_api_key: name must not be empty".into()));
            }
            if !key_hash_ok(key_hash) {
                return Err(DataError::Invalid(
                    "issue_api_key: key_hash must be 64 lowercase hex chars (sha256)".into(),
                ));
            }
            if scopes.is_empty() {
                return Err(DataError::Invalid(
                    "issue_api_key: at least one scope is required".into(),
                ));
            }
            if let Some(bad) = scopes.iter().find(|s| !SCOPES.contains(&s.as_str())) {
                return Err(DataError::Invalid(format!("issue_api_key: invalid scope {bad}")));
            }
            let mut b = Batch::default();
            let id = new_api_key(
                &mut b,
                now_us(),
                c.value.tenant_id,
                cluster_id,
                btrim(name),
                key_hash,
                scopes,
            );
            commit(kv, b).await.map(|_| id).map_err(write_err)
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// `queen_proxy.revoke_api_key(key)`: refuses an unknown or already-revoked key.
pub async fn revoke_api_key(store: &Store, key_id: Uuid) -> Result<(), DataError> {
    match store {
        Store::Pg(pool) => {
            pg_exec(
                pool,
                "SELECT queen_proxy.revoke_api_key($1::text::uuid)",
                &[&key_id.to_string()],
            )
            .await
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let unknown = || {
                WriteErr::Fail(DataError::Invalid(format!(
                    "revoke_api_key: unknown or already-revoked key {key_id}"
                )))
            };
            attempts!(
                async {
                    let Some(k) = read1::<ApiKeyDoc>(kv, ns::KEYS, &schema::key(key_id)).await? else {
                        return Err(unknown());
                    };
                    if k.value.revoked_at_us.is_some() {
                        return Err(unknown());
                    }
                    // The JOIN on clusters: a key whose cluster is gone is unknown.
                    let Some(c) = cluster_doc(kv, k.value.cluster_id).await? else {
                        return Err(unknown());
                    };
                    let now = now_us();
                    let mut doc = k.value.clone();
                    doc.revoked_at_us = Some(now);
                    let mut b = Batch::default();
                    b.put_at(ns::KEYS, &schema::key(key_id), &doc, k.version);
                    record_op(
                        &mut b,
                        now,
                        c.value.tenant_id,
                        Some(doc.cluster_id),
                        "control_plane",
                        None,
                        "api_key_revoked",
                        Some(key_id.to_string()),
                        json!({}),
                    );
                    commit(kv, b).await.map(|_| ())
                }
                .await
            )
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// `queen_proxy.grant_cluster_role(cluster, email, role)`: upsert. The user
/// and the cluster MUST share a tenant (a security boundary, 004_lifecycle).
pub async fn grant_cluster_role(store: &Store, cluster_id: Uuid, email: &str, role: &str) -> Result<(), DataError> {
    match store {
        Store::Pg(pool) => {
            pg_exec(
                pool,
                "SELECT queen_proxy.grant_cluster_role($1::text::uuid, $2, $3)",
                &[&cluster_id.to_string(), &email, &role],
            )
            .await
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            if !ROLES.contains(&role) {
                return Err(DataError::Invalid(format!("grant_cluster_role: invalid role {role}")));
            }
            if !email.contains('@') {
                return Err(DataError::Invalid(format!("grant_cluster_role: invalid email {email}")));
            }
            let email = norm(email);
            let Some(c) = cluster_doc(kv, cluster_id).await? else {
                return Err(DataError::Invalid(format!(
                    "grant_cluster_role: unknown cluster {cluster_id}"
                )));
            };
            let Some(u) = user_by_email(kv, &email).await? else {
                return Err(DataError::Invalid(format!("grant_cluster_role: unknown user {email}")));
            };
            let tenant = c.value.tenant_id;
            if u.value.tenant_id != tenant {
                return Err(DataError::Invalid(format!(
                    "grant_cluster_role: user {email} belongs to tenant {}, cluster {cluster_id} to tenant {tenant}",
                    u.value.tenant_id
                )));
            }
            let user = u.value.id;
            let existing = read1::<RoleDoc>(kv, ns::ROLES, &schema::key2(user, cluster_id)).await?;
            let now = now_us();
            let mut b = Batch::default();
            put_role(&mut b, now, existing.as_ref().map(|d| &d.value), user, cluster_id, role);
            record_op(
                &mut b,
                now,
                tenant,
                Some(cluster_id),
                "control_plane",
                Some(user),
                "cluster_role_granted",
                Some(cluster_id.to_string()),
                json!({"email": email, "role": role}),
            );
            commit(kv, b).await.map(|_| ()).map_err(write_err)
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// `queen_proxy.revoke_cluster_role(cluster, email)`: refuses when there is no
/// grant to remove.
pub async fn revoke_cluster_role(store: &Store, cluster_id: Uuid, email: &str) -> Result<(), DataError> {
    match store {
        Store::Pg(pool) => {
            pg_exec(
                pool,
                "SELECT queen_proxy.revoke_cluster_role($1::text::uuid, $2)",
                &[&cluster_id.to_string(), &email],
            )
            .await
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            if !email.contains('@') {
                return Err(DataError::Invalid(format!(
                    "revoke_cluster_role: invalid email {email}"
                )));
            }
            let email = norm(email);
            attempts!(
                async {
                    let Some(c) = cluster_doc(kv, cluster_id).await? else {
                        return Err(WriteErr::Fail(DataError::Invalid(format!(
                            "revoke_cluster_role: unknown cluster {cluster_id}"
                        ))));
                    };
                    let Some(u) = user_by_email(kv, &email).await? else {
                        return Err(WriteErr::Fail(DataError::Invalid(format!(
                            "revoke_cluster_role: unknown user {email}"
                        ))));
                    };
                    let user = u.value.id;
                    let Some(r) = read1::<RoleDoc>(kv, ns::ROLES, &schema::key2(user, cluster_id)).await? else {
                        return Err(WriteErr::Fail(DataError::Invalid(format!(
                            "revoke_cluster_role: user {email} has no role on cluster {cluster_id}"
                        ))));
                    };
                    let now = now_us();
                    let mut b = Batch::default();
                    b.del_at(ns::ROLES, &schema::key2(user, cluster_id), r.version);
                    b.del(ns::ROLE_CLUSTER, &schema::key2(cluster_id, user));
                    record_op(
                        &mut b,
                        now,
                        c.value.tenant_id,
                        Some(cluster_id),
                        "control_plane",
                        Some(user),
                        "cluster_role_revoked",
                        Some(cluster_id.to_string()),
                        json!({"email": email, "role": r.value.role}),
                    );
                    commit(kv, b).await.map(|_| ())
                }
                .await
            )
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// `queen_proxy.bootstrap_tenant(...)`'s arguments (008_bootstrap_password).
#[derive(Clone, Debug, Default)]
pub struct Bootstrap {
    pub tenant_slug: String,
    /// NULL -> the slug.
    pub tenant_name: Option<String>,
    pub cluster_slug: String,
    pub plan_code: String,
    pub cell: Option<Uuid>,
    pub admin_email: String,
    /// NULL creates a password-less (OAuth/API-key-only) admin.
    pub password: Option<String>,
    /// NULL -> 'default'.
    pub key_name: Option<String>,
}

/// `queen_proxy.bootstrap_tenant(...)` -> `{tenant_id, cluster_id, user_id,
/// api_key, password_set, can_login}`. Idempotent on the slugs; `api_key` is
/// the plaintext, shown once, null on a re-run. Kv: ONE batch — no
/// half-created tenant, exactly like the plpgsql transaction.
pub async fn bootstrap_tenant(store: &Store, a: &Bootstrap) -> Result<Value, DataError> {
    let key_name = a.key_name.clone().unwrap_or_else(|| "default".to_string());
    match store {
        Store::Pg(pool) => {
            pg_json(
                pool,
                "SELECT (queen_proxy.bootstrap_tenant($1, $2, $3, $4, $5::text::uuid, $6, $7, $8))::text",
                &[
                    &a.tenant_slug,
                    &a.tenant_name,
                    &a.cluster_slug,
                    &a.plan_code,
                    &a.cell.map(|c| c.to_string()),
                    &a.admin_email,
                    &a.password,
                    &key_name,
                ],
            )
            .await
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            if btrim(&a.tenant_slug).is_empty() {
                return Err(DataError::Invalid(
                    "bootstrap_tenant: tenant_slug must not be empty".into(),
                ));
            }
            if btrim(&a.cluster_slug).is_empty() {
                return Err(DataError::Invalid(
                    "bootstrap_tenant: cluster_slug must not be empty".into(),
                ));
            }
            if !a.admin_email.contains('@') {
                return Err(DataError::Invalid(format!(
                    "bootstrap_tenant: invalid admin_email {}",
                    a.admin_email
                )));
            }
            let Some(cell) = a.cell else {
                return Err(DataError::Invalid("bootstrap_tenant: cell is required".into()));
            };
            if btrim(&key_name).is_empty() {
                return Err(DataError::Invalid(
                    "bootstrap_tenant: key_name must not be empty".into(),
                ));
            }
            // bcrypt once, off the runtime threads, before any attempt:
            // pgcrypto's `crypt(p, gen_salt('bf', 10))`.
            let password_hash = match &a.password {
                Some(p) => {
                    let p = p.clone();
                    let h = tokio::task::spawn_blocking(move || bcrypt::hash(p, 10))
                        .await
                        .map_err(|e| DataError::Unavailable(format!("bcrypt task: {e}")))?
                        .map_err(|e| DataError::Unavailable(format!("bcrypt: {e}")))?;
                    Some(h)
                }
                None => None,
            };
            let mut last = DataError::Unavailable("bootstrap_tenant: concurrent change, gave up".into());
            for _ in 0..ATTEMPTS {
                match kv_bootstrap_once(kv, a, cell, btrim(&key_name), password_hash.clone()).await {
                    Ok(v) => return Ok(v),
                    // A concurrent bootstrap created the same slug/email: the
                    // re-read finds it and takes the idempotent path.
                    Err(WriteErr::Moved) => continue,
                    Err(WriteErr::Conflict(m)) => {
                        last = DataError::Conflict(m);
                        continue;
                    }
                    Err(WriteErr::Fail(e)) => return Err(e),
                }
            }
            Err(last)
        }
        Store::None => Err(DataError::NoStore),
    }
}

async fn kv_bootstrap_once(
    kv: &dyn KvBackend,
    a: &Bootstrap,
    cell: Uuid,
    key_name: &str,
    password_hash: Option<String>,
) -> Result<Value, WriteErr> {
    let tenant_slug = norm(&a.tenant_slug);
    let cluster_slug = norm(&a.cluster_slug);
    let email = norm(&a.admin_email);
    let now = now_us();
    let mut b = Batch::default();

    // tenant
    let (tenant, new_tenant_created) = match read1::<Uuid>(kv, ns::TENANT_SLUG, &schema::key(&tenant_slug)).await? {
        Some(id) => (id.value, false),
        None => {
            let name = match &a.tenant_name {
                Some(n) => btrim(n).to_string(),
                None => tenant_slug.clone(),
            };
            if name.is_empty() {
                return Err(DataError::Invalid("create_tenant: name must not be empty".into()).into());
            }
            if !dns_label_ok(&tenant_slug) {
                return Err(check_violation("tenants", "slug").into());
            }
            (new_tenant(&mut b, now, &tenant_slug, &name), true)
        }
    };

    // cluster (plan + cell are validated as create_cluster does)
    let (cluster, cluster_is_new) = match read1::<Uuid>(kv, ns::CLUSTER_SLUG, &schema::key(&cluster_slug)).await? {
        Some(id) => {
            let Some(c) = cluster_doc(kv, id.value).await? else {
                return Err(WriteErr::Moved);
            };
            if c.value.tenant_id != tenant {
                return Err(DataError::Invalid(format!(
                    "bootstrap_tenant: cluster slug {cluster_slug} already belongs to tenant {}",
                    c.value.tenant_id
                ))
                .into());
            }
            (c.value.id, false)
        }
        None => {
            let Some(plan) = plan_by_code(kv, &a.plan_code).await? else {
                return Err(DataError::Invalid(format!("create_cluster: unknown plan code {}", a.plan_code)).into());
            };
            if read1::<CellDoc>(kv, ns::CELLS, &schema::key(cell)).await?.is_none() {
                return Err(DataError::Invalid(format!("create_cluster: unknown cell {cell}")).into());
            }
            if !dns_label_ok(&cluster_slug) {
                return Err(check_violation("clusters", "slug").into());
            }
            (new_cluster(&mut b, now, tenant, &cluster_slug, &plan, cell), true)
        }
    };

    // admin user
    let (user, user_doc) = match user_by_email(kv, &email).await? {
        Some(u) => {
            if u.value.tenant_id != tenant {
                return Err(DataError::Invalid(format!(
                    "bootstrap_tenant: user {email} already belongs to tenant {}",
                    u.value.tenant_id
                ))
                .into());
            }
            (u.value.id, Some(u.value))
        }
        None => (
            new_user(&mut b, now, tenant, &email, password_hash.clone(), "local"),
            None,
        ),
    };

    // grant_cluster_role(cluster, email, 'admin') — same tenant by construction.
    let existing_role = if cluster_is_new || user_doc.is_none() {
        None
    } else {
        read1::<RoleDoc>(kv, ns::ROLES, &schema::key2(user, cluster))
            .await?
            .map(|d| d.value)
    };
    put_role(&mut b, now, existing_role.as_ref(), user, cluster, "admin");
    record_op(
        &mut b,
        now,
        tenant,
        Some(cluster),
        "control_plane",
        Some(user),
        "cluster_role_granted",
        Some(cluster.to_string()),
        json!({"email": email, "role": "admin"}),
    );

    // can this admin sign in? a password, or a linked identity.
    let password_set = a.password.is_some();
    let can_login = match &user_doc {
        None => password_hash.is_some(),
        Some(u) => {
            u.password_hash.is_some()
                || !kv::scan_keys(kv, ns::IDENTITY_USER, &schema::prefix(user))
                    .await
                    .map_err(DataError::kv)?
                    .is_empty()
        }
    };
    if !can_login {
        tracing::warn!(
            target: "store",
            admin = %email,
            "bootstrap_tenant: admin has NO password and NO linked identity — it cannot sign in to the console with \
             a password. The tenant, cluster, admin role and API key were all created normally."
        );
    }

    // api key, keyed off the name among the cluster's live keys.
    let mut api_key: Option<String> = None;
    let has_key = if cluster_is_new {
        false
    } else {
        let ids = index_ids(kv, ns::KEY_CLUSTER, cluster).await?;
        docs_by_id::<ApiKeyDoc>(kv, ns::KEYS, &ids)
            .await?
            .iter()
            .any(|d| d.value.name == key_name && d.value.revoked_at_us.is_none())
    };
    if !has_key {
        let plaintext = crate::auth::generate_api_key("live");
        let hash = crate::auth::key_hash_hex(&plaintext);
        let scopes: Vec<String> = SCOPES.iter().map(|s| s.to_string()).collect();
        new_api_key(&mut b, now, tenant, cluster, key_name, &hash, &scopes);
        api_key = Some(plaintext);
    }

    record_op(
        &mut b,
        now,
        tenant,
        Some(cluster),
        "control_plane",
        Some(user),
        "tenant_bootstrapped",
        Some(tenant.to_string()),
        json!({
            "tenant_slug": tenant_slug, "cluster_slug": cluster_slug,
            "plan_code": a.plan_code, "admin_email": email,
            "key_issued": api_key.is_some(),
            "password_set": password_set,
            "can_login": can_login,
        }),
    );
    if new_tenant_created {
        emit_outbox(
            &mut b,
            now,
            "tenant_bootstrapped",
            json!({
                "tenant_id": tenant, "tenant_slug": tenant_slug,
                "cluster_id": cluster, "cluster_slug": cluster_slug,
                "plan_code": a.plan_code, "admin_email": email,
            }),
        );
    }
    commit(kv, b).await?;
    Ok(json!({
        "tenant_id": tenant,
        "cluster_id": cluster,
        "user_id": user,
        "api_key": api_key,
        "password_set": password_set,
        "can_login": can_login,
    }))
}

/// `queen_proxy.delete_tenant(tenant, force)` (007_tenant_delete): hard-delete
/// one tenant and everything under it; refuses a tenant not in `deleting`
/// unless `force`; idempotent (`{"deleted":false,"existed":false}` after).
/// Returns the clusters' broker_tenant_uuids, which exist nowhere else after.
///
/// Kv: the cascade is explicit and runs in several batches when it does not
/// fit one (schema.rs "Cascades"), the tenant row LAST, so a re-run after a
/// partial failure finds the tenant and finishes the job.
pub async fn delete_tenant(store: &Store, tenant_id: Uuid, force: bool) -> Result<Value, DataError> {
    match store {
        Store::Pg(pool) => {
            pg_json(
                pool,
                "SELECT (queen_proxy.delete_tenant($1::text::uuid, $2))::text",
                &[&tenant_id.to_string(), &force],
            )
            .await
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let mut last = DataError::Unavailable("delete_tenant: concurrent change, gave up".into());
            for _ in 0..ATTEMPTS {
                match kv_delete_tenant_once(kv, tenant_id, force).await {
                    Ok(v) => return Ok(v),
                    Err(WriteErr::Moved) => continue,
                    Err(WriteErr::Conflict(m)) => {
                        last = DataError::Conflict(m);
                        break;
                    }
                    Err(WriteErr::Fail(e)) => return Err(e),
                }
            }
            Err(last)
        }
        Store::None => Err(DataError::NoStore),
    }
}

async fn kv_delete_tenant_once(kv: &dyn KvBackend, tenant_id: Uuid, force: bool) -> Result<Value, WriteErr> {
    let Some(t) = read1::<TenantDoc>(kv, ns::TENANTS, &schema::key(tenant_id)).await? else {
        return Ok(json!({"deleted": false, "existed": false, "tenant_id": tenant_id}));
    };
    let (slug, status) = (t.value.slug.clone(), t.value.status.clone());
    if status != "deleting" && !force {
        return Err(DataError::Invalid(format!(
            "delete_tenant: tenant {slug} is {status}, not deleting -- call queen_proxy.set_tenant_status({tenant_id}, \
             'deleting') first (and purge the cell with queen.delete_tenant_data_v1), or pass p_force => true"
        ))
        .into());
    }

    // --- what goes -------------------------------------------------------
    let cluster_ids = index_ids(kv, ns::CLUSTER_TENANT, tenant_id).await?;
    let mut clusters: Vec<ClusterDoc> = docs_by_id::<ClusterDoc>(kv, ns::CLUSTERS, &cluster_ids)
        .await?
        .into_iter()
        .map(|d| d.value)
        .collect();
    clusters.sort_by(|a, b| a.slug.cmp(&b.slug));
    let ids: HashSet<Uuid> = cluster_ids
        .iter()
        .copied()
        .chain(clusters.iter().map(|c| c.id))
        .collect();
    let id_strs: HashSet<String> = ids.iter().map(Uuid::to_string).collect();

    let user_ids = index_ids(kv, ns::USER_TENANT, tenant_id).await?;
    let users: Vec<UserDoc> = docs_by_id::<UserDoc>(kv, ns::USERS, &user_ids)
        .await?
        .into_iter()
        .map(|d| d.value)
        .collect();

    let mut dels: Vec<(&'static str, String)> = Vec::new();
    for u in &users {
        dels.push((ns::USER_EMAIL, schema::key(&u.email)));
    }
    for uid in &user_ids {
        dels.push((ns::USERS, schema::key(uid)));
        dels.push((ns::USER_TENANT, schema::key2(tenant_id, uid)));
        let idents = index_ids(kv, ns::IDENTITY_USER, *uid).await?;
        for d in docs_by_id::<IdentityDoc>(kv, ns::IDENTITIES, &idents).await? {
            dels.push((
                ns::IDENTITY_PROVIDER,
                schema::key(format!("{}:{}", d.value.provider, d.value.provider_id)),
            ));
        }
        for iid in idents {
            dels.push((ns::IDENTITIES, schema::key(iid)));
            dels.push((ns::IDENTITY_USER, schema::key2(uid, iid)));
        }
        // every role of the user (cluster_roles.user_id ON DELETE CASCADE)
        for k in kv::scan_keys(kv, ns::ROLES, &schema::prefix(uid))
            .await
            .map_err(DataError::kv)?
        {
            if let Ok(c) = Uuid::parse_str(schema::tail(&k)) {
                dels.push((ns::ROLE_CLUSTER, schema::key2(c, uid)));
            }
            dels.push((ns::ROLES, k));
        }
    }

    let mut roles = 0i64;
    let mut keys_total = 0i64;
    let mut live_keys: Vec<Doc<ApiKeyDoc>> = Vec::new();
    for c in &ids {
        for u in index_ids(kv, ns::ROLE_CLUSTER, *c).await? {
            roles += 1;
            dels.push((ns::ROLES, schema::key2(u, c)));
            dels.push((ns::ROLE_CLUSTER, schema::key2(c, u)));
        }
        let kids = index_ids(kv, ns::KEY_CLUSTER, *c).await?;
        for d in docs_by_id::<ApiKeyDoc>(kv, ns::KEYS, &kids).await? {
            keys_total += 1;
            dels.push((ns::KEY_HASH, schema::key(&d.value.key_hash)));
            if d.value.revoked_at_us.is_none() {
                live_keys.push(d);
            }
        }
        for kid in kids {
            dels.push((ns::KEYS, schema::key(kid)));
            dels.push((ns::KEY_CLUSTER, schema::key2(c, kid)));
        }
        for k in kv::scan_keys(kv, ns::QUEUE_NAME, &schema::prefix(c))
            .await
            .map_err(DataError::kv)?
        {
            dels.push((ns::QUEUE_NAME, k));
        }
        for n in [ns::USAGE_MIN, ns::USAGE_DAY, ns::OPS_CLUSTER] {
            for k in kv::scan_keys(kv, n, &schema::prefix(c)).await.map_err(DataError::kv)? {
                dels.push((n, k));
            }
        }
    }
    for cl in &clusters {
        dels.push((ns::CLUSTER_SLUG, schema::key(&cl.slug)));
        dels.push((ns::CLUSTER_CELL, schema::key2(cl.cell_id, cl.id)));
    }
    for c in &ids {
        dels.push((ns::CLUSTERS, schema::key(c)));
        dels.push((ns::CLUSTER_TENANT, schema::key2(tenant_id, c)));
    }
    // queues: live AND tombstoned rows (the name index only holds live ones).
    let mut queues = 0i64;
    if !ids.is_empty() {
        for (k, d) in kv::scan::<QueueDoc>(kv, ns::QUEUES, K).await.map_err(DataError::kv)? {
            if ids.contains(&d.value.cluster_id) {
                queues += 1;
                dels.push((ns::QUEUES, k));
            }
        }
    }
    // operations (their tenant_id FK was the RESTRICT that forced this delete)
    let mut ops = 0i64;
    for (k, d) in kv::scan::<OperationDoc>(kv, ns::OPS, &schema::prefix(tenant_id))
        .await
        .map_err(DataError::kv)?
    {
        ops += 1;
        if let Some(c) = d.value.cluster_id {
            let rest = &k[schema::prefix(tenant_id).len()..];
            dels.push((ns::OPS_CLUSTER, format!("{K}{c}/{rest}")));
        }
        dels.push((ns::OPS, k));
    }

    let clusters_json: Vec<Value> = clusters
        .iter()
        .map(|c| json!({"cluster_id": c.id, "slug": c.slug, "cell_id": c.cell_id, "broker_tenant_uuid": c.broker_tenant_uuid}))
        .collect();
    let now = now_us();
    let counts = json!({
        "clusters": clusters_json.len(),
        "users": users.len(),
        "cluster_roles": roles,
        "queues": queues,
        "api_keys": keys_total,
        "api_keys_revoked": live_keys.len(),
    });

    // --- phase A: revoke the live keys, redact the outbox -------------------
    // Idempotent: a lost version race (a redaction target rewritten
    // meanwhile) re-runs the whole delete, which finds these already done.
    let mut phase_a: Vec<Batch> = Vec::new();
    let mut cur = Batch::default();
    for d in &live_keys {
        if cur.len() >= CHUNK_OPS {
            phase_a.push(std::mem::take(&mut cur));
        }
        let mut k = d.value.clone();
        k.revoked_at_us = Some(now);
        cur.op(kv::put_op(
            ns::KEYS,
            &schema::key(k.id),
            &k,
            Expect::Version(d.version),
            Ttl::Forever,
            false,
        ));
    }
    let mut redacted = 0i64;
    for (k, d) in kv::scan::<OutboxDoc>(kv, ns::OUTBOX, K).await.map_err(DataError::kv)? {
        let p = &d.value.payload;
        let mine = p.get("tenant_id").and_then(Value::as_str) == Some(tenant_id.to_string().as_str())
            || p.get("cluster_id")
                .and_then(Value::as_str)
                .is_some_and(|c| id_strs.contains(c));
        if p.get("admin_email").is_none() || !mine {
            continue;
        }
        let mut doc = d.value.clone();
        if let Some(o) = doc.payload.as_object_mut() {
            o.remove("admin_email");
        }
        if cur.len() >= CHUNK_OPS {
            phase_a.push(std::mem::take(&mut cur));
        }
        cur.put_at(ns::OUTBOX, &k, &doc, d.version);
        redacted += 1;
    }
    if !cur.is_empty() {
        phase_a.push(cur);
    }
    for b in phase_a {
        commit(kv, b).await?;
    }

    // --- phase B: the cascade ------------------------------------------------
    commit_all(kv, delete_batches(dels)).await?;

    // --- phase C: the tenant row, the surviving audit, the invalidations -----
    // The row goes under the version read above: of two concurrent deletes one
    // loses here, re-reads, finds no tenant and answers existed=false — the
    // FOR UPDATE of the plpgsql, and the event is written exactly once.
    let mut last = Batch::default();
    emit_outbox(
        &mut last,
        now,
        "tenant_deleted",
        json!({
            "tenant_id": tenant_id, "tenant_slug": slug, "status_was": status, "forced": force,
            "clusters": clusters_json, "counts": counts, "at": iso_utc(now),
        }),
    );
    last.del_at(ns::TENANTS, &schema::key(tenant_id), t.version);
    last.del(ns::TENANT_SLUG, &schema::key(&slug));
    ids.iter().for_each(|c| last.invalidate(*c));
    commit(kv, last).await?;

    let mut counts = counts;
    counts["operations"] = json!(ops);
    counts["outbox_redacted"] = json!(redacted);
    Ok(json!({
        "deleted": true,
        "existed": true,
        "tenant_id": tenant_id,
        "tenant_slug": slug,
        "status_was": status,
        "forced": force,
        "clusters": clusters_json,
        "counts": counts,
    }))
}

// ===========================================================================
// catalog seeding (the single binary has no psql and no migration seed)
// ===========================================================================

/// The four plans 002_functions seeds, with 009_default_families' features.
pub fn default_plans() -> Vec<PlanDoc> {
    const KIB: i64 = 1024;
    const GIB: i64 = 1024 * 1024 * 1024;
    const DAY: i64 = 86_400;
    let fam = |extra: bool| {
        let mut f = json!({"kv": true, "timers": true, "ephemeral": true});
        if extra {
            f["streams"] = json!(true);
            f["traces"] = json!(true);
        }
        f
    };
    let plan = |code: &str, class: &str, r: [i64; 4], q: [i64; 3], p: [i64; 4], features: Value| PlanDoc {
        id: Uuid::new_v4(),
        code: code.to_string(),
        cell_class: class.to_string(),
        max_req_per_sec: Some(r[0]),
        req_burst: Some(r[1]),
        max_msgs_per_sec: Some(r[2]),
        msgs_burst: Some(r[3]),
        max_queues: Some(q[0]),
        max_partitions_per_queue: Some(q[1]),
        max_parked_pops: Some(q[2]),
        max_payload_bytes: Some(p[0]),
        max_batch_items: Some(p[1]),
        max_retained_bytes: Some(p[2]),
        max_retention_seconds: Some(p[3]),
        monthly_msgs_quota: None,
        features,
        created_at_us: now_us(),
    };
    vec![
        plan(
            "free",
            "shared",
            [5, 25, 20, 100],
            [20, 8, 50],
            [256 * KIB, 1000, GIB, 7 * DAY],
            fam(false),
        ),
        plan(
            "dev",
            "shared",
            [10, 50, 40, 200],
            [50, 16, 150],
            [512 * KIB, 2000, 3 * GIB, 14 * DAY],
            fam(false),
        ),
        plan(
            "pro",
            "shared",
            [50, 200, 200, 800],
            [200, 32, 1000],
            [1024 * KIB, 5000, 20 * GIB, 30 * DAY],
            fam(true),
        ),
        plan(
            "dedicated-s",
            "dedicated",
            [200, 800, 600, 2000],
            [1000, 64, 5000],
            [4096 * KIB, 10000, 200 * GIB, 90 * DAY],
            fam(true),
        ),
    ]
}

/// Seed the plan catalog when absent (`ON CONFLICT (code) DO NOTHING`); the
/// number of plans written. Pg: the migrations already did it.
pub async fn seed_default_plans(store: &Store) -> Result<usize, DataError> {
    let Some(kv) = kv_of(store) else {
        return if store.is_some() {
            Ok(0)
        } else {
            Err(DataError::NoStore)
        };
    };
    let mut n = 0;
    for p in default_plans() {
        if !plan_code_ok(&p.code) {
            continue;
        }
        let mut b = Batch::default();
        b.claim(ns::PLAN_CODE, &schema::key(&p.code), p.id, "plans_code_key");
        b.put_new(ns::PLANS, &schema::key(p.id), &p);
        match commit(kv, b).await {
            Ok(_) => n += 1,
            Err(WriteErr::Conflict(_)) | Err(WriteErr::Moved) => {}
            Err(WriteErr::Fail(e)) => return Err(e),
        }
    }
    Ok(n)
}

/// A cell as ops provision it (there is no queen_proxy.* function for cells).
#[derive(Clone, Debug)]
pub struct CellSpec {
    pub slug: String,
    pub region: String,
    pub base_url: String,
    /// shared | dedicated
    pub class: String,
    pub capacity_slots: i64,
    pub cell_secret: Option<String>,
}

/// Create the cell named `slug`, or bring its region/base_url/class/capacity/
/// secret up to `spec`; its id. The single binary's own cell is this.
pub async fn upsert_cell(store: &Store, spec: &CellSpec) -> Result<Uuid, DataError> {
    for (v, col) in [
        (&spec.slug, "slug"),
        (&spec.region, "region"),
        (&spec.base_url, "base_url"),
    ] {
        if btrim(v).is_empty() {
            return Err(check_violation("cells", col));
        }
    }
    if !["shared", "dedicated"].contains(&spec.class.as_str()) {
        return Err(check_violation("cells", "class"));
    }
    if spec.capacity_slots < 0 {
        return Err(check_violation("cells", "capacity_slots"));
    }
    match store {
        Store::Pg(pool) => {
            let capacity = clamp_partitions(spec.capacity_slots);
            pg_uuid(
                pool,
                "INSERT INTO queen_proxy.cells(slug, region, base_url, class, capacity_slots, cell_secret) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT (slug) DO UPDATE SET region = EXCLUDED.region, base_url = EXCLUDED.base_url, \
                   class = EXCLUDED.class, capacity_slots = EXCLUDED.capacity_slots, cell_secret = EXCLUDED.cell_secret \
                 RETURNING id::text",
                &[&spec.slug, &spec.region, &spec.base_url, &spec.class, &capacity, &spec.cell_secret],
            )
            .await
        }
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            attempts!(
                async {
                    let now = now_us();
                    let mut b = Batch::default();
                    let id = match read1::<Uuid>(kv, ns::CELL_SLUG, &schema::key(&spec.slug)).await? {
                        None => {
                            let id = Uuid::new_v4();
                            b.claim(ns::CELL_SLUG, &schema::key(&spec.slug), id, "cells_slug_key");
                            b.put_new(
                                ns::CELLS,
                                &schema::key(id),
                                &CellDoc {
                                    id,
                                    slug: spec.slug.clone(),
                                    region: spec.region.clone(),
                                    base_url: spec.base_url.clone(),
                                    class: spec.class.clone(),
                                    capacity_slots: spec.capacity_slots,
                                    used_slots: 0,
                                    broker_version: None,
                                    status: "active".into(),
                                    cell_secret: spec.cell_secret.clone(),
                                    created_at_us: now,
                                },
                            );
                            id
                        }
                        Some(ix) => {
                            let Some(d) = read1::<CellDoc>(kv, ns::CELLS, &schema::key(ix.value)).await? else {
                                return Err(WriteErr::Moved);
                            };
                            let mut c = d.value.clone();
                            c.region = spec.region.clone();
                            c.base_url = spec.base_url.clone();
                            c.class = spec.class.clone();
                            c.capacity_slots = spec.capacity_slots;
                            c.cell_secret = spec.cell_secret.clone();
                            if c != d.value {
                                b.put_at(ns::CELLS, &schema::key(c.id), &c, d.version);
                                // every cluster on the cell carries its url/secret
                                for cl in index_ids(kv, ns::CLUSTER_CELL, c.id).await? {
                                    b.invalidate(cl);
                                }
                            }
                            c.id
                        }
                    };
                    commit(kv, b).await.map(|_| id)
                }
                .await
            )
        }
        Store::None => Err(DataError::NoStore),
    }
}

// ===========================================================================
// tests: the KV side against MemKv (the Postgres side is the SQL it always ran)
// ===========================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::memkv::MemKv;
    use super::*;
    use crate::state::ClusterStatus;

    /// MemKv plus the two receiver refusals it does not model and every writer
    /// here must respect: the op ceiling and one write per key per call. Every
    /// call yields first, as a real (raft) call does, so concurrent writers in
    /// one test genuinely interleave between their reads and their writes.
    struct StrictKv(MemKv);

    impl KvBackend for StrictKv {
        fn kv(&self, ops: Vec<Value>) -> kv::BoxFut<'_, Result<Vec<Value>, KvError>> {
            let mut seen = HashSet::new();
            for o in &ops {
                let op = o.get("op").and_then(Value::as_str).unwrap_or("");
                if matches!(op, "put" | "putIfAbsent" | "delete" | "incr") {
                    let k = (
                        o["ns"].as_str().unwrap_or("").to_string(),
                        o["key"].as_str().unwrap_or("").to_string(),
                    );
                    assert!(seen.insert(k.clone()), "two writes of {k:?} in one call");
                }
            }
            assert!(ops.len() <= MAX_OPS, "{} ops in one call", ops.len());
            Box::pin(async move {
                tokio::task::yield_now().await;
                self.0.kv(ops).await
            })
        }
    }

    fn store() -> (Store, Arc<StrictKv>) {
        let kv = Arc::new(StrictKv(MemKv::new()));
        (Store::Kv(kv.clone()), kv)
    }

    async fn cell(s: &Store) -> Uuid {
        upsert_cell(
            s,
            &CellSpec {
                slug: "local".into(),
                region: "eu".into(),
                base_url: "http://127.0.0.1:6632".into(),
                class: "shared".into(),
                capacity_slots: 10,
                cell_secret: Some("s3cret".into()),
            },
        )
        .await
        .unwrap()
    }

    /// A seeded catalog, a cell, and one bootstrapped tenant.
    async fn world(s: &Store) -> (Uuid, Uuid, Uuid, String) {
        assert_eq!(seed_default_plans(s).await.unwrap(), 4);
        let cell = cell(s).await;
        let out = bootstrap_tenant(
            s,
            &Bootstrap {
                tenant_slug: "Acme".into(),
                tenant_name: Some(" Acme Inc ".into()),
                cluster_slug: "acme-prod".into(),
                plan_code: "pro".into(),
                cell: Some(cell),
                admin_email: " Admin@Acme.io ".into(),
                password: None,
                key_name: None,
            },
        )
        .await
        .unwrap();
        let t = Uuid::parse_str(out["tenant_id"].as_str().unwrap()).unwrap();
        let c = Uuid::parse_str(out["cluster_id"].as_str().unwrap()).unwrap();
        let u = Uuid::parse_str(out["user_id"].as_str().unwrap()).unwrap();
        (t, c, u, out["api_key"].as_str().unwrap().to_string())
    }

    fn found<T>(l: Lookup<T>) -> T {
        match l {
            Lookup::Found(v) => v,
            Lookup::Absent => panic!("absent"),
            Lookup::Unavailable => panic!("unavailable"),
        }
    }

    #[test]
    fn iso_utc_matches_postgres_jsonb_rendering() {
        assert_eq!(iso_utc(0), "1970-01-01T00:00:00+00:00");
        assert_eq!(iso_utc(1_790_000_000_500_000), "2026-09-21T14:13:20.5+00:00");
        assert_eq!(iso_utc(951_782_400_000_001), "2000-02-29T00:00:00.000001+00:00");
    }

    #[test]
    fn the_check_constraints() {
        assert!(dns_label_ok("acme-prod") && dns_label_ok("a") && dns_label_ok("0x"));
        assert!(!dns_label_ok("-a") && !dns_label_ok("a-") && !dns_label_ok("A") && !dns_label_ok(""));
        assert!(!dns_label_ok(&"a".repeat(64)) && dns_label_ok(&"a".repeat(63)));
        assert!(key_hash_ok(&"ab".repeat(32)) && !key_hash_ok(&"AB".repeat(32)) && !key_hash_ok("ab"));
        assert!(plan_code_ok("dedicated-s") && !plan_code_ok("1x"));
    }

    #[tokio::test]
    async fn bootstrap_then_resolve_by_slug_id_and_key() {
        let (s, _) = store();
        let (t, c, _u, key) = world(&s).await;
        let ctx = found(lookup_cluster(&s, &ClusterKey::Slug("acme-prod".into())).await);
        assert_eq!((ctx.cluster_id, ctx.tenant_id, ctx.slug.as_str()), (c, t, "acme-prod"));
        assert_eq!(ctx.cell_base_url, "http://127.0.0.1:6632");
        assert_eq!(ctx.cell_token.as_deref(), Some("s3cret"));
        assert_eq!(ctx.status, ClusterStatus::Active);
        assert_eq!(ctx.limits.max_queues, Some(200), "pro's catalog numbers");
        assert!(ctx.features.streams && ctx.features.kv && ctx.features.ephemeral);
        assert_eq!(found(lookup_cluster(&s, &ClusterKey::Id(c)).await).slug, "acme-prod");
        assert!(matches!(
            lookup_cluster(&s, &ClusterKey::Slug("nope".into())).await,
            Lookup::Absent
        ));

        let (kctx, _kid, scopes) = found(lookup_api_key(&s, &crate::auth::key_hash_hex(&key)).await);
        assert_eq!(kctx.cluster_id, c);
        assert_eq!(scopes, Scopes::all());
        assert!(matches!(lookup_api_key(&s, &"0".repeat(64)).await, Lookup::Absent));
    }

    #[tokio::test]
    async fn bootstrap_is_idempotent_and_normalises() {
        let (s, kv) = store();
        let (t, c, u, _) = world(&s).await;
        let again = bootstrap_tenant(
            &s,
            &Bootstrap {
                tenant_slug: "acme".into(),
                cluster_slug: "ACME-PROD".into(),
                plan_code: "pro".into(),
                cell: Some(cell(&s).await),
                admin_email: "admin@acme.io".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(again["tenant_id"], json!(t));
        assert_eq!(again["cluster_id"], json!(c));
        assert_eq!(again["user_id"], json!(u));
        assert_eq!(
            again["api_key"],
            Value::Null,
            "the plaintext is unrecoverable; no second key"
        );
        assert_eq!(again["can_login"], json!(false));
        assert_eq!(kv.0.keys(ns::KEYS).len(), 1);
        assert_eq!(kv.0.keys(ns::TENANT_SLUG), vec!["#acme".to_string()]);
        assert_eq!(kv.0.keys(ns::USER_EMAIL), vec!["#admin@acme.io".to_string()]);
        let tenant: TenantDoc = read1(&kv.0, ns::TENANTS, &schema::key(t)).await.unwrap().unwrap().value;
        assert_eq!(tenant.name, "Acme Inc");
        // one signup event, from the run that created the tenant
        assert_eq!(kv.0.keys(ns::OUTBOX).len(), 1);
    }

    #[tokio::test]
    async fn bootstrap_refuses_foreign_slugs_and_emails_writing_nothing() {
        let (s, kv) = store();
        let (_, _, _, _) = world(&s).await;
        let cell = cell(&s).await;
        let before = kv.0.keys(ns::TENANTS).len();
        let err = bootstrap_tenant(
            &s,
            &Bootstrap {
                tenant_slug: "other".into(),
                cluster_slug: "acme-prod".into(),
                plan_code: "free".into(),
                cell: Some(cell),
                admin_email: "x@other.io".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, DataError::Invalid(m) if m.contains("already belongs to tenant")),
            "{err}"
        );
        let err = bootstrap_tenant(
            &s,
            &Bootstrap {
                tenant_slug: "other".into(),
                cluster_slug: "other-c".into(),
                plan_code: "free".into(),
                cell: Some(cell),
                admin_email: "ADMIN@acme.io".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, DataError::Invalid(m) if m.contains("user admin@acme.io already belongs")),
            "{err}"
        );
        assert_eq!(
            kv.0.keys(ns::TENANTS).len(),
            before,
            "a refused bootstrap leaves no half-created tenant"
        );
        let err = bootstrap_tenant(
            &s,
            &Bootstrap {
                tenant_slug: "x".into(),
                cluster_slug: "y".into(),
                plan_code: "gold".into(),
                cell: Some(cell),
                admin_email: "a@b".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err, DataError::Invalid("create_cluster: unknown plan code gold".into()));
    }

    #[tokio::test]
    async fn bootstrap_with_a_password_can_log_in() {
        let (s, kv) = store();
        seed_default_plans(&s).await.unwrap();
        let cell = cell(&s).await;
        let out = bootstrap_tenant(
            &s,
            &Bootstrap {
                tenant_slug: "pw".into(),
                cluster_slug: "pw-c".into(),
                plan_code: "free".into(),
                cell: Some(cell),
                admin_email: "a@pw.io".into(),
                password: Some("hunter2".into()),
                key_name: Some("ci".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            (out["password_set"].clone(), out["can_login"].clone()),
            (json!(true), json!(true))
        );
        let u: UserDoc = user_by_email(&kv.0, "a@pw.io").await.unwrap().unwrap().value;
        assert!(bcrypt::verify("hunter2", u.password_hash.as_deref().unwrap()).unwrap());
    }

    #[tokio::test]
    async fn unique_slugs_collide_as_conflicts() {
        let (s, _) = store();
        let (t, _, _, _) = world(&s).await;
        let err = create_tenant(&s, " ACME ", "dup").await.unwrap_err();
        assert!(err.is_conflict(), "{err}");
        assert!(err.to_string().contains("tenants_slug_key"));
        let cell = cell(&s).await;
        let err = create_cluster(&s, t, "acme-prod", "free", cell).await.unwrap_err();
        assert!(
            err.is_conflict() && err.to_string().contains("clusters_slug_key"),
            "{err}"
        );
        assert_eq!(
            create_tenant(&s, "Bad_Slug", "x").await.unwrap_err(),
            check_violation("tenants", "slug")
        );
        let ghost = Uuid::new_v4();
        assert_eq!(
            create_cluster(&s, ghost, "c2", "free", cell).await.unwrap_err(),
            DataError::Invalid(format!("create_cluster: unknown tenant {ghost}"))
        );
        assert_eq!(
            create_cluster(&s, t, "c2", "free", ghost).await.unwrap_err(),
            DataError::Invalid(format!("create_cluster: unknown cell {ghost}"))
        );
    }

    #[tokio::test]
    async fn key_hash_collision_is_a_conflict_and_bad_input_is_invalid() {
        let (s, _) = store();
        let (_, c, _, key) = world(&s).await;
        let hash = crate::auth::key_hash_hex(&key);
        let scopes = vec!["produce".to_string()];
        let err = issue_api_key(&s, c, "again", &hash, &scopes).await.unwrap_err();
        assert!(
            err.is_conflict() && err.to_string().contains("api_keys_key_hash_key"),
            "{err}"
        );
        let other = "ab".repeat(32);
        assert_eq!(
            issue_api_key(&s, c, "  ", &other, &scopes).await.unwrap_err(),
            DataError::Invalid("issue_api_key: name must not be empty".into())
        );
        assert_eq!(
            issue_api_key(&s, c, "k", &other, &["write".to_string()])
                .await
                .unwrap_err(),
            DataError::Invalid("issue_api_key: invalid scope write".into())
        );
        assert_eq!(
            issue_api_key(&s, c, "k", "XYZ", &scopes).await.unwrap_err(),
            DataError::Invalid("issue_api_key: key_hash must be 64 lowercase hex chars (sha256)".into())
        );
        let id = issue_api_key(&s, c, " reader ", &other, &["read".to_string()])
            .await
            .unwrap();
        let (_, kid, sc) = found(lookup_api_key(&s, &other).await);
        assert_eq!(
            (kid, sc),
            (
                id,
                Scopes {
                    read: true,
                    ..Default::default()
                }
            )
        );
    }

    #[tokio::test]
    async fn a_revoked_key_stops_resolving_and_cannot_be_revoked_twice() {
        let (s, kv) = store();
        let (_, _, _, key) = world(&s).await;
        let hash = crate::auth::key_hash_hex(&key);
        let (_, kid, _) = found(lookup_api_key(&s, &hash).await);
        revoke_api_key(&s, kid).await.unwrap();
        assert!(matches!(lookup_api_key(&s, &hash).await, Lookup::Absent));
        assert_eq!(
            revoke_api_key(&s, kid).await.unwrap_err(),
            DataError::Invalid(format!("revoke_api_key: unknown or already-revoked key {kid}"))
        );
        // the hash stays claimed, as the UNIQUE constraint on the row does
        assert_eq!(kv.0.keys(ns::KEY_HASH).len(), 1);
    }

    #[tokio::test]
    async fn touch_writes_last_used_once_per_window() {
        let (s, kv) = store();
        let (_, _, _, key) = world(&s).await;
        let (_, kid, _) = found(lookup_api_key(&s, &crate::auth::key_hash_hex(&key)).await);
        touch_api_keys(&s, &[kid, Uuid::new_v4()]).await.unwrap();
        let d: Doc<ApiKeyDoc> = read1(&kv.0, ns::KEYS, &schema::key(kid)).await.unwrap().unwrap();
        assert!(d.value.last_used_at_us.is_some());
        touch_api_keys(&s, &[kid]).await.unwrap();
        let again: Doc<ApiKeyDoc> = read1(&kv.0, ns::KEYS, &schema::key(kid)).await.unwrap().unwrap();
        assert_eq!(again.version, d.version, "inside the window: no second write");
    }

    #[tokio::test]
    async fn roles_grant_upsert_and_revoke_with_the_tenant_boundary() {
        let (s, _) = store();
        let (t, c, u, _) = world(&s).await;
        assert_eq!(cluster_role(&s, u, c).await.unwrap().as_deref(), Some("admin"));
        grant_cluster_role(&s, c, "ADMIN@acme.io", "viewer").await.unwrap();
        assert_eq!(cluster_role(&s, u, c).await.unwrap().as_deref(), Some("viewer"));
        assert_eq!(
            grant_cluster_role(&s, c, "admin@acme.io", "owner").await.unwrap_err(),
            DataError::Invalid("grant_cluster_role: invalid role owner".into())
        );
        assert_eq!(
            grant_cluster_role(&s, c, "ghost@acme.io", "viewer").await.unwrap_err(),
            DataError::Invalid("grant_cluster_role: unknown user ghost@acme.io".into())
        );
        // a user of ANOTHER tenant can never be seated on this cluster
        let cell = cell(&s).await;
        let other = bootstrap_tenant(
            &s,
            &Bootstrap {
                tenant_slug: "evil".into(),
                cluster_slug: "evil-c".into(),
                plan_code: "free".into(),
                cell: Some(cell),
                admin_email: "e@evil.io".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let err = grant_cluster_role(&s, c, "e@evil.io", "admin").await.unwrap_err();
        let want = format!(
            "grant_cluster_role: user e@evil.io belongs to tenant {}, cluster {c} to tenant {t}",
            other["tenant_id"].as_str().unwrap()
        );
        assert_eq!(err, DataError::Invalid(want));

        revoke_cluster_role(&s, c, "admin@acme.io").await.unwrap();
        assert_eq!(cluster_role(&s, u, c).await.unwrap(), None);
        assert_eq!(
            revoke_cluster_role(&s, c, "admin@acme.io").await.unwrap_err(),
            DataError::Invalid(format!(
                "revoke_cluster_role: user admin@acme.io has no role on cluster {c}"
            ))
        );
    }

    #[tokio::test]
    async fn statuses_overrides_and_plans_reach_the_ctx() {
        let (s, _) = store();
        let (t, c, _, _) = world(&s).await;
        set_limit_override(&s, c, Some(&json!({"max_queues": 5, "max_payload_bytes": null})))
            .await
            .unwrap();
        let ctx = found(lookup_cluster(&s, &ClusterKey::Id(c)).await);
        assert_eq!((ctx.limits.max_queues, ctx.limits.max_payload_bytes), (Some(5), None));
        assign_plan(&s, c, "free").await.unwrap();
        set_limit_override(&s, c, None).await.unwrap();
        let ctx = found(lookup_cluster(&s, &ClusterKey::Id(c)).await);
        assert_eq!(ctx.limits.max_queues, Some(20));
        assert!(!ctx.features.streams);
        set_tenant_status(&s, t, "grace").await.unwrap();
        assert_eq!(
            found(lookup_cluster(&s, &ClusterKey::Id(c)).await).status,
            ClusterStatus::PushBlocked
        );
        set_cluster_status(&s, c, "suspended").await.unwrap();
        assert_eq!(
            found(lookup_cluster(&s, &ClusterKey::Id(c)).await).status,
            ClusterStatus::Suspended
        );
        assert_eq!(
            set_cluster_status(&s, c, "paused").await.unwrap_err(),
            DataError::Invalid("set_cluster_status: invalid status paused".into())
        );
        assert_eq!(
            assign_plan(&s, c, "gold").await.unwrap_err(),
            DataError::Invalid("assign_plan: unknown plan code gold".into())
        );
        let ghost = Uuid::new_v4();
        assert_eq!(
            set_cluster_status(&s, ghost, "active").await.unwrap_err(),
            DataError::Invalid(format!("set_cluster_status: unknown cluster {ghost}"))
        );
    }

    #[tokio::test]
    async fn the_feed_names_exactly_the_clusters_that_changed() {
        let (s, kv) = store();
        let (t, c, _, key) = world(&s).await;
        let mut feed = InvalFeed::new();
        assert_eq!(feed.poll(kv.as_ref()).await.unwrap(), InvalPoll::Baseline);
        assert_eq!(feed.poll(kv.as_ref()).await.unwrap(), InvalPoll::Quiet);
        set_cluster_status(&s, c, "push_blocked").await.unwrap();
        assert_eq!(feed.poll(kv.as_ref()).await.unwrap(), InvalPoll::Changed(vec![c]));
        assert_eq!(feed.poll(kv.as_ref()).await.unwrap(), InvalPoll::Quiet);
        // queue rows, touches and session revocations do not NOTIFY in Postgres either
        persist_queue_floors(&s, &[(c, "orders".into(), 3)]).await.unwrap();
        let (_, kid, _) = found(lookup_api_key(&s, &crate::auth::key_hash_hex(&key)).await);
        touch_api_keys(&s, &[kid]).await.unwrap();
        assert_eq!(feed.poll(kv.as_ref()).await.unwrap(), InvalPoll::Quiet);
        // a tenant status fans out to every cluster of the tenant
        let cell = cell(&s).await;
        let c2 = create_cluster(&s, t, "acme-stage", "free", cell).await.unwrap();
        assert_eq!(feed.poll(kv.as_ref()).await.unwrap(), InvalPoll::Changed(vec![c2]));
        set_tenant_status(&s, t, "suspended").await.unwrap();
        let mut both = vec![c, c2];
        both.sort();
        assert_eq!(feed.poll(kv.as_ref()).await.unwrap(), InvalPoll::Changed(both));
    }

    #[tokio::test]
    async fn queue_rows_admit_grow_set_and_soft_delete() {
        let (s, kv) = store();
        let (_, c, _, _) = world(&s).await;
        persist_queue_floors(&s, &[(c, "orders".into(), 3), (c, "a/b".into(), 1)])
            .await
            .unwrap();
        persist_queue_floors(&s, &[(c, "orders".into(), 2)]).await.unwrap();
        let mut live = live_queues(&s, c).await.unwrap();
        live.sort();
        assert_eq!(
            live,
            vec![("a/b".to_string(), 1), ("orders".to_string(), 3)],
            "GREATEST: a floor never lowers"
        );
        // the reconciler SETS the broker's count, lowering included
        let res = reconcile_queue_counts(&s, c, &[("orders".into(), 1), ("new".into(), 4)])
            .await
            .unwrap();
        assert!(res.iter().all(Result::is_ok));
        let mut live = live_queues(&s, c).await.unwrap();
        live.sort();
        assert_eq!(live, vec![("a/b".into(), 1), ("new".into(), 4), ("orders".into(), 1)]);
        // the broker lists only "new": the rest are tombstoned, names reusable
        sweep_deleted_queues(&s, c, &["new".to_string()]).await.unwrap();
        assert_eq!(live_queues(&s, c).await.unwrap(), vec![("new".to_string(), 4)]);
        assert_eq!(kv.0.keys(ns::QUEUES).len(), 3, "soft delete keeps the rows");
        persist_queue_floors(&s, &[(c, "orders".into(), 7)]).await.unwrap();
        let mut live = live_queues(&s, c).await.unwrap();
        live.sort();
        assert_eq!(
            live,
            vec![("new".into(), 4), ("orders".into(), 7)],
            "a swept name comes back as a new row"
        );
        assert_eq!(kv.0.keys(ns::QUEUES).len(), 4);
        // an empty confirmed inventory sweeps everything
        sweep_deleted_queues(&s, c, &[]).await.unwrap();
        assert!(live_queues(&s, c).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_queue_ramp_larger_than_one_call_persists() {
        let (s, _) = store();
        let (_, c, _, _) = world(&s).await;
        let rows: Vec<(Uuid, String, i64)> = (0..700).map(|i| (c, format!("q{i:04}"), i)).collect();
        persist_queue_floors(&s, &rows).await.unwrap();
        assert_eq!(live_queues(&s, c).await.unwrap().len(), 700);
    }

    #[tokio::test]
    async fn reconcile_targets_skip_deleting_and_merge_the_storage_cap() {
        let (s, _) = store();
        let (t, c, _, _) = world(&s).await;
        let cell = cell(&s).await;
        let c2 = create_cluster(&s, t, "acme-gone", "free", cell).await.unwrap();
        set_cluster_status(&s, c2, "deleting").await.unwrap();
        set_limit_override(&s, c, Some(&json!({"max_retained_bytes": 1234})))
            .await
            .unwrap();
        let targets = reconcile_targets(&s).await.unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].cluster_id, c);
        assert_eq!(targets[0].max_retained_bytes, Some(1234));
        assert_eq!(targets[0].base_url, "http://127.0.0.1:6632");
        assert_eq!(targets[0].cell_secret.as_deref(), Some("s3cret"));
    }

    #[tokio::test]
    async fn sessions_revoke_idempotently_and_sweep() {
        let (s, kv) = store();
        let (_, _, u, _) = world(&s).await;
        let exp = now_us() / 1_000_000 + 3600;
        assert!(!is_jti_revoked(&s, "j1").await.unwrap());
        revoke_session(&s, "j1", exp, "user", u).await.unwrap();
        revoke_session(&s, " j1 ", exp, "user", u).await.unwrap();
        assert!(is_jti_revoked(&s, "j1").await.unwrap());
        assert_eq!(
            revoke_session(&s, "j2", exp, "user", Uuid::nil()).await.unwrap_err(),
            DataError::Invalid(format!("revoke_session: actor_id must be a known user {}", Uuid::nil()))
        );
        assert_eq!(
            revoke_session(&s, " ", exp, "user", u).await.unwrap_err(),
            DataError::Invalid("revoke_session: jti must not be empty".into())
        );
        // a row that outlived its token (an import without a TTL) is swept
        kv::write(
            &kv.0,
            vec![kv::put_op(
                ns::REVOKED,
                "#old",
                &RevokedDoc {
                    jti: "old".into(),
                    expires_at_us: 1,
                },
                Expect::Any,
                Ttl::Forever,
                false,
            )],
        )
        .await
        .unwrap();
        assert_eq!(sweep_revoked_tokens(&s).await.unwrap(), 1);
        assert!(!is_jti_revoked(&s, "old").await.unwrap());
        assert!(is_jti_revoked(&s, "j1").await.unwrap());
    }

    #[tokio::test]
    async fn auth_reads_users() {
        let (s, _) = store();
        let (_, _, u, _) = world(&s).await;
        assert!(user_exists(&s, u).await.unwrap());
        assert!(!user_exists(&s, Uuid::new_v4()).await.unwrap());
        assert!(!user_is_operator(&s, u).await.unwrap());
        assert!(!user_is_operator(&s, Uuid::new_v4()).await.unwrap());
    }

    #[tokio::test]
    async fn delete_tenant_cascades_everything_and_is_idempotent() {
        let (s, kv) = store();
        let (t, c, u, key) = world(&s).await;
        let cell = cell(&s).await;
        let kept = bootstrap_tenant(
            &s,
            &Bootstrap {
                tenant_slug: "keep".into(),
                cluster_slug: "keep-c".into(),
                plan_code: "free".into(),
                cell: Some(cell),
                admin_email: "k@keep.io".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let other_c = Uuid::parse_str(kept["cluster_id"].as_str().unwrap()).unwrap();
        persist_queue_floors(&s, &[(c, "orders".into(), 2), (other_c, "kept".into(), 1)])
            .await
            .unwrap();
        sweep_deleted_queues(&s, c, &[]).await.unwrap(); // a tombstone must go too
        persist_queue_floors(&s, &[(c, "orders".into(), 1)]).await.unwrap();
        revoke_session(&s, "j", now_us() / 1_000_000 + 60, "user", u)
            .await
            .unwrap();

        let err = delete_tenant(&s, t, false).await.unwrap_err();
        assert!(
            matches!(&err, DataError::Invalid(m) if m.contains("tenant acme is active, not deleting")),
            "{err}"
        );
        set_tenant_status(&s, t, "deleting").await.unwrap();
        let mut feed = InvalFeed::new();
        feed.poll(kv.as_ref()).await.unwrap();
        let out = delete_tenant(&s, t, false).await.unwrap();
        assert_eq!(out["deleted"], json!(true));
        assert_eq!(out["clusters"][0]["slug"], json!("acme-prod"));
        assert!(out["clusters"][0]["broker_tenant_uuid"].is_string());
        let counts = &out["counts"];
        assert_eq!(
            (
                counts["clusters"].clone(),
                counts["users"].clone(),
                counts["cluster_roles"].clone(),
                counts["queues"].clone()
            ),
            (json!(1), json!(1), json!(1), json!(2))
        );
        assert_eq!(
            (counts["api_keys"].clone(), counts["api_keys_revoked"].clone()),
            (json!(1), json!(1))
        );
        assert!(counts["operations"].as_i64().unwrap() > 0);
        assert_eq!(counts["outbox_redacted"], json!(1));
        assert_eq!(feed.poll(kv.as_ref()).await.unwrap(), InvalPoll::Changed(vec![c]));

        // nothing of the tenant is left; the other tenant is untouched
        assert!(matches!(lookup_cluster(&s, &ClusterKey::Id(c)).await, Lookup::Absent));
        assert!(matches!(
            lookup_api_key(&s, &crate::auth::key_hash_hex(&key)).await,
            Lookup::Absent
        ));
        assert!(!user_exists(&s, u).await.unwrap());
        assert_eq!(kv.0.keys(ns::TENANTS).len(), 1);
        assert_eq!(kv.0.keys(ns::TENANT_SLUG), vec!["#keep".to_string()]);
        assert_eq!(kv.0.keys(ns::CLUSTERS).len(), 1);
        assert_eq!(kv.0.keys(ns::CLUSTER_SLUG), vec!["#keep-c".to_string()]);
        assert_eq!(kv.0.keys(ns::USERS).len(), 1);
        assert_eq!(kv.0.keys(ns::USER_EMAIL), vec!["#k@keep.io".to_string()]);
        assert_eq!(kv.0.keys(ns::ROLES).len(), 1);
        assert_eq!(kv.0.keys(ns::ROLE_CLUSTER).len(), 1);
        assert_eq!(kv.0.keys(ns::KEYS).len(), 1);
        assert_eq!(kv.0.keys(ns::KEY_HASH).len(), 1);
        assert_eq!(kv.0.keys(ns::KEY_CLUSTER).len(), 1);
        assert_eq!(kv.0.keys(ns::QUEUES).len(), 1);
        assert_eq!(kv.0.keys(ns::QUEUE_NAME).len(), 1);
        assert!(kv.0.keys(ns::OPS).iter().all(|k| !k.starts_with(&schema::prefix(t))));
        assert!(kv
            .0
            .keys(ns::OPS_CLUSTER)
            .iter()
            .all(|k| !k.starts_with(&schema::prefix(c))));
        assert!(kv
            .0
            .keys(ns::CLUSTER_TENANT)
            .iter()
            .all(|k| !k.starts_with(&schema::prefix(t))));
        assert!(
            is_jti_revoked(&s, "j").await.unwrap(),
            "the deny-list carries no tenant (as in Postgres)"
        );
        // the outbox keeps its events; the address is gone from this tenant's
        let outbox: Vec<(String, Doc<OutboxDoc>)> = kv::scan(&kv.0, ns::OUTBOX, K).await.unwrap();
        let mine: Vec<&OutboxDoc> = outbox
            .iter()
            .map(|(_, d)| &d.value)
            .filter(|d| d.payload["tenant_id"] == json!(t))
            .collect();
        assert_eq!(mine.len(), 2, "tenant_bootstrapped + tenant_deleted");
        assert!(mine.iter().all(|d| d.payload.get("admin_email").is_none()));
        assert!(outbox
            .iter()
            .any(|(_, d)| d.value.payload["admin_email"] == json!("k@keep.io")));

        let again = delete_tenant(&s, t, false).await.unwrap();
        assert_eq!(again, json!({"deleted": false, "existed": false, "tenant_id": t}));
    }

    /// Fan-outs wider than one call: 260 clusters under one tenant (the
    /// status change carries 260 marks) and 250 live keys revoked by a delete.
    /// StrictKv refuses any call over the broker's op ceiling.
    #[tokio::test]
    async fn wide_tenants_stay_under_the_op_ceiling() {
        let (s, kv) = store();
        let (t, c, _, _) = world(&s).await;
        let cell = cell(&s).await;
        let mut all = vec![c];
        for i in 0..259 {
            all.push(create_cluster(&s, t, &format!("acme-{i}"), "free", cell).await.unwrap());
        }
        for i in 0..250 {
            let hash = format!("{:064x}", i + 1);
            issue_api_key(&s, c, &format!("k{i}"), &hash, &["read".to_string()])
                .await
                .unwrap();
        }
        let mut feed = InvalFeed::new();
        feed.poll(kv.as_ref()).await.unwrap();
        set_tenant_status(&s, t, "deleting").await.unwrap();
        all.sort();
        assert_eq!(feed.poll(kv.as_ref()).await.unwrap(), InvalPoll::Changed(all.clone()));
        let out = delete_tenant(&s, t, false).await.unwrap();
        assert_eq!(out["counts"]["clusters"], json!(260));
        assert_eq!(
            (
                out["counts"]["api_keys"].clone(),
                out["counts"]["api_keys_revoked"].clone()
            ),
            (json!(251), json!(251))
        );
        assert!(
            kv.0.keys(ns::CLUSTERS).is_empty() && kv.0.keys(ns::KEYS).is_empty() && kv.0.keys(ns::TENANTS).is_empty()
        );
        assert_eq!(feed.poll(kv.as_ref()).await.unwrap(), InvalPoll::Changed(all));
    }

    /// The plpgsql's `FOR UPDATE`: of two concurrent deletes one wins, the
    /// other answers existed=false, and the surviving audit is written once.
    #[tokio::test]
    async fn concurrent_deletes_of_one_tenant_have_one_winner() {
        let (s, kv) = store();
        let (t, _, _, _) = world(&s).await;
        set_tenant_status(&s, t, "deleting").await.unwrap();
        let (a, b) = tokio::join!(delete_tenant(&s, t, false), delete_tenant(&s, t, false));
        let (a, b) = (a.unwrap(), b.unwrap());
        let winners = [&a, &b].iter().filter(|r| r["deleted"] == json!(true)).count();
        assert_eq!(winners, 1, "{a} / {b}");
        let events: Vec<(String, Doc<OutboxDoc>)> = kv::scan(&kv.0, ns::OUTBOX, K).await.unwrap();
        assert_eq!(
            events.iter().filter(|(_, d)| d.value.kind == "tenant_deleted").count(),
            1
        );
        assert!(kv.0.keys(ns::TENANTS).is_empty());
    }

    /// Two nodes bootstrapping the same tenant at once: the UNIQUE claims make
    /// one of them lose its batch, and its retry takes the idempotent path —
    /// one tenant, one cluster, one admin, one key, one signup event.
    #[tokio::test]
    async fn concurrent_bootstraps_converge_on_one_tenant() {
        let (s, kv) = store();
        seed_default_plans(&s).await.unwrap();
        let cell = cell(&s).await;
        let args = Bootstrap {
            tenant_slug: "race".into(),
            cluster_slug: "race-c".into(),
            plan_code: "free".into(),
            cell: Some(cell),
            admin_email: "r@race.io".into(),
            ..Default::default()
        };
        let (a, b) = tokio::join!(bootstrap_tenant(&s, &args), bootstrap_tenant(&s, &args));
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(
            (a["tenant_id"].clone(), a["cluster_id"].clone()),
            (b["tenant_id"].clone(), b["cluster_id"].clone())
        );
        assert_eq!(
            [&a, &b].iter().filter(|r| r["api_key"].is_string()).count(),
            1,
            "{a} / {b}"
        );
        assert_eq!(kv.0.keys(ns::TENANTS).len(), 1);
        assert_eq!(kv.0.keys(ns::CLUSTERS).len(), 1);
        assert_eq!(kv.0.keys(ns::USERS).len(), 1);
        assert_eq!(kv.0.keys(ns::KEYS).len(), 1);
        assert_eq!(kv.0.keys(ns::OUTBOX).len(), 1);
    }

    #[tokio::test]
    async fn delete_tenant_force_skips_only_the_status_gate() {
        let (s, kv) = store();
        let t = create_tenant(&s, "lonely", "Lonely").await.unwrap();
        let out = delete_tenant(&s, t, true).await.unwrap();
        assert_eq!(
            (out["deleted"].clone(), out["forced"].clone(), out["status_was"].clone()),
            (json!(true), json!(true), json!("active"))
        );
        assert!(kv.0.keys(ns::TENANTS).is_empty() && kv.0.keys(ns::TENANT_SLUG).is_empty());
        assert!(kv.0.keys(ns::OPS).is_empty());
    }

    #[tokio::test]
    async fn cells_upsert_by_slug_and_invalidate_their_clusters() {
        let (s, kv) = store();
        let (_, c, _, _) = world(&s).await;
        let mut feed = InvalFeed::new();
        feed.poll(kv.as_ref()).await.unwrap();
        let id = cell(&s).await;
        assert_eq!(
            feed.poll(kv.as_ref()).await.unwrap(),
            InvalPoll::Quiet,
            "unchanged: no write"
        );
        let moved = upsert_cell(
            &s,
            &CellSpec {
                slug: "local".into(),
                region: "eu".into(),
                base_url: "http://10.0.0.1:6632".into(),
                class: "shared".into(),
                capacity_slots: 10,
                cell_secret: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(moved, id);
        assert_eq!(feed.poll(kv.as_ref()).await.unwrap(), InvalPoll::Changed(vec![c]));
        let ctx = found(lookup_cluster(&s, &ClusterKey::Id(c)).await);
        assert_eq!(
            (ctx.cell_base_url.as_str(), ctx.cell_token.clone()),
            ("http://10.0.0.1:6632", None)
        );
        assert_eq!(seed_default_plans(&s).await.unwrap(), 0, "the catalog is seeded once");
    }

    #[tokio::test]
    async fn no_store_answers_nothing_and_refuses_writes() {
        let s = Store::None;
        assert!(matches!(
            lookup_cluster(&s, &ClusterKey::Slug("x".into())).await,
            Lookup::Absent
        ));
        assert!(!is_jti_revoked(&s, "j").await.unwrap());
        assert!(live_queues(&s, Uuid::nil()).await.unwrap().is_empty());
        assert_eq!(create_tenant(&s, "a", "b").await.unwrap_err(), DataError::NoStore);
        assert_eq!(seed_default_plans(&s).await.unwrap_err(), DataError::NoStore);
    }
}
