//! W3 data plane repositories (PLAN_SINGLE_BINARY.md W3): tenants, cells,
//! plans, clusters, cluster_roles, api_keys, revoked_tokens, queues, and each
//! cluster's S3 sink.
//!
//! Every function answers from [`Store`]:
//!
//! - **`Store::Kv`** — the broker's replicated KV. Documents per
//!   [`super::schema`]; every writer validates its input and refuses with a
//!   [`DataError::Invalid`] naming the rule. A UNIQUE index is a
//!   `putIfAbsent` + `required` in the SAME batch as the row, so two nodes can
//!   never both win; the loser gets [`DataError::Conflict`].
//! - **`Store::None`** — no store (tests): reads answer "nothing", writes
//!   [`DataError::NoStore`].
//!
//! # Invalidation
//!
//! A change to a cluster must reach every node's caches. Every writer writes,
//! in the SAME batch as the change, an `incr` of `px.meta #inval` and a mark
//! `px.meta #inval/<cluster>` (TTL [`INVAL_MARK_TTL_S`]). Every node polls the
//! counter (one local read per second, [`InvalFeed`]); when it moved, it lists
//! the marks and invalidates the clusters whose mark has a version it has not
//! seen. Versions are unique and never re-issued, so "differs" is the test —
//! never "greater" (monotonicity is not promised, server/src/rsm/planner/kv.rs).
//! The writes that mark: an operation row with a cluster, plus the explicit
//! per-cluster fan-outs (set_tenant_status, delete_tenant). Queue rows,
//! `last_used_at` and revocations do not.
//!
//! Layout additions (px.meta only, nothing else changes): `#inval` (counter)
//! and `#inval/<cluster uuid>` (marks).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use super::kv::{self, Doc, Expect, KvBackend, KvError, Ttl};
use super::schema::{
    self, ns, ApiKeyDoc, CellDoc, ClusterDoc, IdentityDoc, OperationDoc, OutboxDoc, PlanDoc,
    QueueDoc, RevokedDoc, RoleDoc, S3SinkDoc, TenantDoc, UserDoc, K,
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
    /// The input or a precondition was refused (a validation rule, a missing
    /// referenced row). The text names the rule.
    Invalid(String),
    /// A UNIQUE index already holds the value.
    Conflict(String),
    /// The store did not answer (no KV leader, a timeout, a document that
    /// does not decode).
    Unavailable(String),
    /// No store configured.
    NoStore,
    /// A row is in a state that refuses the call, `code` naming the state
    /// for the caller (`deleting`). Nothing was written.
    Refused { code: &'static str, msg: String },
}

impl std::fmt::Display for DataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataError::Invalid(m) => write!(f, "{m}"),
            DataError::Conflict(m) => write!(f, "{m}"),
            DataError::Unavailable(m) => write!(f, "store unavailable: {m}"),
            DataError::NoStore => write!(f, "no store configured"),
            DataError::Refused { msg, .. } => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for DataError {}

impl DataError {
    /// A unique-index violation.
    pub fn is_conflict(&self) -> bool {
        matches!(self, DataError::Conflict(_))
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

/// auth.rs's lookups take `impl Into<Store>`: a `&Store` (`&st.store`) or a
/// `Store`.
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

/// A UTC timestamp in the API's established JSON shape:
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

/// Trim spaces only (not other whitespace), both ends.
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

/// The one spelling of a tenant or cluster slug, for the writers that store
/// one and the lookups that look one up alike: trimmed, ASCII-lowercased and
/// a DNS label (the CHECK above). `None` for anything else, which no row can
/// carry, so a caller answers it without asking the store (a key the KV
/// refuses would read as the store being down).
pub fn slug_of(s: &str) -> Option<String> {
    let s = s.trim().to_ascii_lowercase();
    dns_label_ok(&s).then_some(s)
}

/// The longest tenant or api key name a writer accepts, in characters.
pub const NAME_MAX: usize = 200;
/// The longest email a writer accepts (RFC 5321's path limit), in bytes.
pub const EMAIL_MAX: usize = 254;
/// The longest plan code looked up.
const PLAN_CODE_MAX: usize = 63;

/// An input as a refusal quotes it: at most 64 characters of it.
fn shown(s: &str) -> String {
    match s.char_indices().nth(64) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_string(),
    }
}

/// A name within [`NAME_MAX`]; `who` names the writer in the refusal.
fn check_name(who: &str, field: &str, name: &str) -> Result<(), DataError> {
    if name.chars().count() > NAME_MAX {
        return Err(DataError::Invalid(format!(
            "{who}: {field} is longer than {NAME_MAX} characters"
        )));
    }
    Ok(())
}

/// An email as the writers store it (`lower(btrim)`), with an `@` and within
/// [`EMAIL_MAX`].
fn email_of(who: &str, email: &str) -> Result<String, DataError> {
    let e = norm(email);
    if e.len() > EMAIL_MAX {
        return Err(DataError::Invalid(format!(
            "{who}: email is longer than {EMAIL_MAX} bytes"
        )));
    }
    if !e.contains('@') {
        return Err(DataError::Invalid(format!("{who}: invalid email {email}")));
    }
    Ok(e)
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
    /// A UNIQUE claim lost.
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

/// The invalidation marks of `clusters`, for writers outside this module
/// (store/web.rs): append these ops to the SAME batch as the change. At most
/// [`MARKS_PER_CALL`] clusters per call; one write per key.
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

/// The audit row (px.ops, and px.ops.cluster when it names a cluster) — and
/// a cluster names an invalidation.
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

/// Append a control-plane-bound event to the outbox.
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

/// Resolve a cluster (the ClusterCache miss path).
pub async fn lookup_cluster(store: &Store, key: &ClusterKey) -> Lookup<ClusterCtx> {
    match store {
        Store::Kv(kv) => kv_lookup_cluster(kv.as_ref(), key).await,
        Store::None => Lookup::Absent,
    }
}

/// Resolve an API key by its sha256 hex (the ClusterCache miss path).
pub async fn lookup_api_key(store: &Store, hash_hex: &str) -> Lookup<(ClusterCtx, Uuid, Scopes)> {
    match store {
        Store::Kv(kv) => kv_lookup_key(kv.as_ref(), hash_hex).await,
        Store::None => Lookup::Absent,
    }
}

/// A cluster's ClusterCtx, from its four documents (cluster, tenant, cell,
/// plan).
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
// the invalidation feed
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
        Store::Kv(kv) => Ok(read1::<RevokedDoc>(kv.as_ref(), ns::REVOKED, &schema::key(jti))
            .await?
            .is_some()),
        Store::None => Ok(false),
    }
}

/// Does this user row exist?
pub async fn user_exists(store: &Store, user_id: Uuid) -> Result<bool, DataError> {
    match store {
        Store::Kv(kv) => Ok(read1::<UserDoc>(kv.as_ref(), ns::USERS, &schema::key(user_id))
            .await?
            .is_some()),
        Store::None => Ok(false),
    }
}

/// The user's role on the cluster (`cluster_roles.role`), if any.
pub async fn cluster_role(store: &Store, user_id: Uuid, cluster_id: Uuid) -> Result<Option<String>, DataError> {
    match store {
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
        Store::Kv(kv) => Ok(read1::<UserDoc>(kv.as_ref(), ns::USERS, &schema::key(user_id))
            .await?
            .is_some_and(|d| d.value.is_operator)),
        Store::None => Ok(false),
    }
}

/// `revoke_session(jti, expires_at, actor, actor_id)`. Idempotent: a second
/// revoke of one jti is not an error. The deny-list row carries a TTL until
/// the token's own expiry, so the store sweeps it by itself.
pub async fn revoke_session(
    store: &Store,
    jti: &str,
    expires_at_unix: i64,
    actor: &str,
    actor_id: Uuid,
) -> Result<(), DataError> {
    match store {
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
            // The replicated revocation epoch (PLAN_SINGLE_BINARY.md W3): the
            // nil "cluster" tells every node's invalidation poller to drop its
            // cached "not revoked" answers, so a logout holds cell-wide within
            // a poll instead of a cache TTL.
            b.invalidate(Uuid::nil());
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

/// `sweep_revoked_tokens()`: drop deny-list rows past their own
/// token's expiry; the number dropped. The rows' TTL already does it; this
/// catches any written without one.
pub async fn sweep_revoked_tokens(store: &Store) -> Result<i64, DataError> {
    match store {
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

/// Live queues of a cluster and their recorded partition counts
/// (`deleted_at IS NULL`).
pub async fn live_queues(store: &Store, cluster_id: Uuid) -> Result<Vec<(String, i64)>, DataError> {
    match store {
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

/// The registry persister's write: each (cluster, queue) row raised to at
/// least `count` (created when absent). All or nothing from the caller's view:
/// Err means "put the batch back for the next tick".
pub async fn persist_queue_floors(store: &Store, rows: &[(Uuid, String, i64)]) -> Result<(), DataError> {
    if rows.is_empty() {
        return Ok(());
    }
    match store {
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
// control plane: the writers of these tables
// ===========================================================================

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
    // No plan can carry another shape: answered without a read.
    if code.len() > PLAN_CODE_MAX || !plan_code_ok(code) {
        return Ok(None);
    }
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

/// The rows of a new cluster (the cluster, its audit row, its invalidation).
fn new_cluster(b: &mut Batch, now: i64, tenant: Uuid, slug: &str, plan: &PlanDoc, cell: Uuid) -> Uuid {
    new_cluster_doc(b, now, tenant, slug, plan, cell, json!({})).id
}

/// [`new_cluster`] born with `overrides` as its `limit_overrides`; the new
/// document.
fn new_cluster_doc(
    b: &mut Batch,
    now: i64,
    tenant: Uuid,
    slug: &str,
    plan: &PlanDoc,
    cell: Uuid,
    overrides: Value,
) -> ClusterDoc {
    let id = Uuid::new_v4();
    let doc = ClusterDoc {
        id,
        tenant_id: tenant,
        cell_id: cell,
        plan_id: plan.id,
        slug: slug.to_string(),
        broker_tenant_uuid: Uuid::new_v4(),
        status: "active".into(),
        limit_overrides: overrides,
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
    doc
}

/// The rows of a new user (create_user's INSERT + audit). users is the web
/// plane's table (store/web.rs); bootstrap_tenant needs to create one.
fn new_user(b: &mut Batch, now: i64, tenant: Uuid, email: &str, password_hash: Option<String>, provider: &str) -> Uuid {
    let id = Uuid::new_v4();
    b.claim(ns::USER_EMAIL, &schema::key(email), id, "users_email_key");
    user_rows(b, now, tenant, id, email, password_hash, provider);
    id
}

/// A new user's own rows under `id` (the document, its tenant index, its
/// audit row). The email index is the caller's to write.
fn user_rows(
    b: &mut Batch,
    now: i64,
    tenant: Uuid,
    id: Uuid,
    email: &str,
    password_hash: Option<String>,
    provider: &str,
) {
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
}

/// The rows of a new api key (the key, its audit row, its invalidation).
/// `index`: `None` claims a free `px.keys.hash` entry; `Some(version)` takes
/// over a dangling one (an entry no key holds the hash of) under that version.
#[allow(clippy::too_many_arguments)]
fn new_api_key(
    b: &mut Batch,
    now: i64,
    tenant: Uuid,
    cluster: Uuid,
    name: &str,
    key_hash: &str,
    scopes: &[String],
    index: Option<u64>,
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
    match index {
        None => b.claim(
            ns::KEY_HASH,
            &schema::key(key_hash),
            id,
            "api_keys_key_hash_key",
        ),
        Some(version) => b.put_at(ns::KEY_HASH, &schema::key(key_hash), &id, version),
    }
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

/// `create_tenant(slug, name)` -> tenant id.
pub async fn create_tenant(store: &Store, slug: &str, name: &str) -> Result<Uuid, DataError> {
    match store {
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            if btrim(slug).is_empty() {
                return Err(DataError::Invalid("create_tenant: slug must not be empty".into()));
            }
            if btrim(name).is_empty() {
                return Err(DataError::Invalid("create_tenant: name must not be empty".into()));
            }
            check_name("create_tenant", "name", btrim(name))?;
            let Some(slug) = slug_of(slug) else {
                return Err(check_violation("tenants", "slug"));
            };
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

/// `create_cluster(tenant, slug, plan_code, cell)` -> cluster id.
pub async fn create_cluster(
    store: &Store,
    tenant_id: Uuid,
    slug: &str,
    plan_code: &str,
    cell_id: Uuid,
) -> Result<Uuid, DataError> {
    match store {
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
            let Some(slug) = slug_of(slug) else {
                return Err(check_violation("clusters", "slug"));
            };
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

/// `assign_plan(cluster, plan_code)`.
pub async fn assign_plan(store: &Store, cluster_id: Uuid, plan_code: &str) -> Result<(), DataError> {
    match store {
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

/// `set_cluster_status(cluster, status)`.
pub async fn set_cluster_status(store: &Store, cluster_id: Uuid, status: &str) -> Result<(), DataError> {
    match store {
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

/// `set_limit_override(cluster, overrides)`; None clears (`'{}'`). The
/// document is refused unless [`check_overrides`] accepts it.
pub async fn set_limit_override(store: &Store, cluster_id: Uuid, overrides: Option<&Value>) -> Result<(), DataError> {
    match store {
        Store::Kv(kv) => {
            let o = overrides.cloned().unwrap_or_else(|| json!({}));
            // The rule provision applies: the limit names, each a
            // non-negative integer or null.
            check_overrides(&o)?;
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

/// `set_tenant_status(tenant, status)`: every cluster of the
/// tenant is invalidated (its effective status is the worse of the two).
pub async fn set_tenant_status(store: &Store, tenant_id: Uuid, status: &str) -> Result<(), DataError> {
    match store {
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
                    commit(kv, tenant_status_batch(&t, status, &clusters))
                        .await
                        .map(|_| ())
                }
                .await
            )
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// The writes of a tenant status change: the tenant under the version read,
/// its audit row, and a mark for every cluster of the tenant (each one's
/// effective status is the worse of the two).
fn tenant_status_batch(t: &Doc<TenantDoc>, status: &str, clusters: &[Uuid]) -> Batch {
    let mut doc = t.value.clone();
    doc.status = status.to_string();
    let mut b = Batch::default();
    b.put_at(ns::TENANTS, &schema::key(doc.id), &doc, t.version);
    record_op(
        &mut b,
        now_us(),
        doc.id,
        None,
        "control_plane",
        None,
        "tenant_status_changed",
        Some(doc.id.to_string()),
        json!({"status": status}),
    );
    clusters.iter().for_each(|c| b.invalidate(*c));
    b
}

/// The input rules of an api key: a name, a sha256 hex hash, a non-empty
/// subset of the scopes. `who` names the writer in the refusal.
fn check_key_input(
    who: &str,
    name: &str,
    key_hash: &str,
    scopes: &[String],
) -> Result<(), DataError> {
    if btrim(name).is_empty() {
        return Err(DataError::Invalid(format!("{who}: name must not be empty")));
    }
    check_name(who, "name", btrim(name))?;
    if !key_hash_ok(key_hash) {
        return Err(DataError::Invalid(format!(
            "{who}: key_hash must be 64 lowercase hex chars (sha256)"
        )));
    }
    if scopes.is_empty() {
        return Err(DataError::Invalid(format!(
            "{who}: at least one scope is required"
        )));
    }
    if let Some(bad) = scopes.iter().find(|s| !SCOPES.contains(&s.as_str())) {
        return Err(DataError::Invalid(format!("{who}: invalid scope {bad}")));
    }
    Ok(())
}

/// `issue_api_key(cluster, name, key_hash, scopes)` -> key id.
/// `key_hash` is already the sha256 hex (auth::key_hash_hex).
pub async fn issue_api_key(
    store: &Store,
    cluster_id: Uuid,
    name: &str,
    key_hash: &str,
    scopes: &[String],
) -> Result<Uuid, DataError> {
    match store {
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let Some(c) = cluster_doc(kv, cluster_id).await? else {
                return Err(DataError::Invalid(format!(
                    "issue_api_key: unknown cluster {cluster_id}"
                )));
            };
            check_key_input("issue_api_key", name, key_hash, scopes)?;
            let mut b = Batch::default();
            let id = new_api_key(
                &mut b,
                now_us(),
                c.value.tenant_id,
                cluster_id,
                btrim(name),
                key_hash,
                scopes,
                None,
            );
            commit(kv, b).await.map(|_| id).map_err(write_err)
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// `revoke_api_key(key)`: refuses an unknown or already-revoked key.
pub async fn revoke_api_key(store: &Store, key_id: Uuid) -> Result<(), DataError> {
    match store {
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
                    commit(kv, revoke_batch(&k, c.value.tenant_id))
                        .await
                        .map(|_| ())
                }
                .await
            )
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// The writes of a key revocation: `revoked_at` under the version read, and
/// its audit row (which marks the key's cluster).
fn revoke_batch(k: &Doc<ApiKeyDoc>, tenant: Uuid) -> Batch {
    let now = now_us();
    let mut doc = k.value.clone();
    doc.revoked_at_us = Some(now);
    let mut b = Batch::default();
    b.put_at(ns::KEYS, &schema::key(doc.id), &doc, k.version);
    record_op(
        &mut b,
        now,
        tenant,
        Some(doc.cluster_id),
        "control_plane",
        None,
        "api_key_revoked",
        Some(doc.id.to_string()),
        json!({}),
    );
    b
}

/// `grant_cluster_role(cluster, email, role)`: upsert. The user
/// and the cluster MUST share a tenant (a security boundary).
pub async fn grant_cluster_role(store: &Store, cluster_id: Uuid, email: &str, role: &str) -> Result<(), DataError> {
    match store {
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

/// `revoke_cluster_role(cluster, email)`: refuses when there is no
/// grant to remove.
pub async fn revoke_cluster_role(store: &Store, cluster_id: Uuid, email: &str) -> Result<(), DataError> {
    match store {
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

/// `bootstrap_tenant(...)`'s arguments.
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

/// `bootstrap_tenant(...)` -> `{tenant_id, cluster_id, user_id,
/// api_key, password_set, can_login}`. Idempotent on the slugs; `api_key` is
/// the plaintext, shown once, null on a re-run. ONE batch — no
/// half-created tenant.
pub async fn bootstrap_tenant(store: &Store, a: &Bootstrap) -> Result<Value, DataError> {
    let key_name = a.key_name.clone().unwrap_or_else(|| "default".to_string());
    match store {
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
            // Every key this call looks up is checked before the first read:
            // a slug or an email no row can carry is a refusal, never a key
            // the store refuses.
            let Some(tenant_slug) = slug_of(&a.tenant_slug) else {
                return Err(check_violation("tenants", "slug"));
            };
            let Some(cluster_slug) = slug_of(&a.cluster_slug) else {
                return Err(check_violation("clusters", "slug"));
            };
            email_of("bootstrap_tenant", &a.admin_email)?;
            check_name("bootstrap_tenant", "key_name", btrim(&key_name))?;
            if let Some(n) = &a.tenant_name {
                check_name("bootstrap_tenant", "tenant_name", btrim(n))?;
            }
            let a = &Bootstrap {
                tenant_slug,
                cluster_slug,
                ..a.clone()
            };
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
    // Both slugs come through `slug_of` (bootstrap_tenant).
    let tenant_slug = a.tenant_slug.clone();
    let cluster_slug = a.cluster_slug.clone();
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
        new_api_key(&mut b, now, tenant, cluster, key_name, &hash, &scopes, None);
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

/// `delete_tenant(tenant, force)`: hard-delete
/// one tenant and everything under it; refuses a tenant not in `deleting`
/// unless `force`; idempotent (`{"deleted":false,"existed":false}` after).
/// Returns the clusters' broker_tenant_uuids, which exist nowhere else after.
///
/// Kv: the cascade is explicit and runs in several batches when it does not
/// fit one (schema.rs "Cascades"), the tenant row LAST, so a re-run after a
/// partial failure finds the tenant and finishes the job.
pub async fn delete_tenant(store: &Store, tenant_id: Uuid, force: bool) -> Result<Value, DataError> {
    match store {
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
            "delete_tenant: tenant {slug} is {status}, not deleting -- set its status to 'deleting' first \
             (tenant {tenant_id}), or pass force"
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
        // its S3 sink: the broker's manager stops the sink when the row goes
        dels.push((ns::S3SINKS, schema::key(c)));
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
    // loses here, re-reads, finds no tenant and answers existed=false, and
    // the event is written exactly once.
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
// control plane: a cloud cell's lifecycle (`/api/cp`, cp.rs)
// ===========================================================================
//
// What a cloud cell's agent drives over `/api/cp`: a tenancy provisioned in
// one call, clusters read back, the tenant lifecycle (status, the broker
// purge, the delete) and retry-safe keys. Its caller is an at-least-once bus,
// so every writer here is idempotent: a retry of a call that already took
// effect writes nothing and answers the same ids.

/// The limit keys a cluster's `limit_overrides` may carry: the eleven
/// [`crate::cache::merge_limits`] reads, and the monthly allowance the quota
/// pass reads (store/usage.rs).
pub const OVERRIDE_KEYS: [&str; 12] = [
    "max_req_per_sec",
    "req_burst",
    "max_msgs_per_sec",
    "msgs_burst",
    "max_queues",
    "max_partitions_per_queue",
    "max_parked_pops",
    "max_payload_bytes",
    "max_batch_items",
    "max_retained_bytes",
    "max_retention_seconds",
    "monthly_msgs_quota",
];

/// An overrides document the control plane may store: an object of
/// [`OVERRIDE_KEYS`], each a non-negative integer or `null` (unlimited).
pub fn check_overrides(v: &Value) -> Result<(), DataError> {
    let Some(o) = v.as_object() else {
        return Err(DataError::Invalid("overrides must be an object".into()));
    };
    for (k, val) in o {
        if !OVERRIDE_KEYS.contains(&k.as_str()) {
            return Err(DataError::Invalid(format!("overrides: unknown limit {k}")));
        }
        if !(val.is_null() || val.as_i64().is_some_and(|n| n >= 0)) {
            return Err(DataError::Invalid(format!(
                "overrides: {k} must be a non-negative integer or null"
            )));
        }
    }
    Ok(())
}

/// A refusal decided from what was read: answered, never retried.
fn conflict(m: String) -> WriteErr {
    WriteErr::Fail(DataError::Conflict(m))
}

/// `provision(...)`'s arguments: the 1.x `tenant.provision` transaction.
#[derive(Clone, Debug, Default)]
pub struct Provision {
    pub tenant_slug: String,
    /// The name of a tenant this call creates (NULL -> the slug).
    pub tenant_name: Option<String>,
    pub cluster_slug: String,
    /// A seeded plan code, given to a cluster this call creates. An existing
    /// cluster keeps its plan.
    pub plan_code: String,
    /// The cell a cluster this call creates is placed on.
    pub cell: Option<Uuid>,
    /// The caller's own id for the user (the `sub` of the sessions it mints).
    pub user_id: Uuid,
    pub email: String,
    /// One of the cluster roles.
    pub role: String,
    /// The name of a key this call creates (an adopted key keeps its own).
    pub key_name: String,
    /// sha256 hex of the key the caller minted: the plaintext never comes here.
    pub key_hash: String,
    pub scopes: Vec<String>,
    /// The cluster's whole `limit_overrides`, replacing the old document
    /// (`{}` or `null`: none).
    pub overrides: Value,
}

/// `provision(...)` -> `{tenant_id, cluster_id, broker_tenant_uuid, user_id,
/// key_id, plan_code, overrides, created: {tenant, cluster, user, key}}`.
///
/// Ensures, in ONE atomic batch: the tenant (by slug, created with its name);
/// the cluster (by slug, created on `plan_code`; a cluster of another tenant
/// is a conflict); the user under the caller's id (created, or its email moved
/// to `email`; an email of another user, or a user of another tenant, is a
/// conflict); `role` on the cluster (upsert); the key (a live key of this
/// cluster holding the hash is adopted, whatever its name; the hash held by a
/// revoked key or by another cluster's key is a conflict); the overrides. A
/// repeat writes nothing and answers the same ids. `plan_code` in the answer
/// is the cluster's own plan. A tenant in `deleting` is refused
/// ([`DataError::Refused`] `deleting`), and the batch carries the tenant's
/// version, so a wipe that begins between the read and the write is what the
/// retry reads.
pub async fn provision(store: &Store, a: &Provision) -> Result<Value, DataError> {
    match store {
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let a = provision_input(a)?;
            let Some(plan) = plan_by_code(kv, &a.plan_code).await? else {
                return Err(DataError::Invalid(format!(
                    "provision: unknown plan code {}",
                    shown(&a.plan_code)
                )));
            };
            let mut last = DataError::Unavailable("provision: concurrent change, gave up".into());
            for _ in 0..ATTEMPTS {
                match kv_provision_once(kv, &a, &plan).await {
                    Ok(v) => return Ok(v),
                    Err(WriteErr::Moved) => continue,
                    // A concurrent provision claimed a slug, the email or the
                    // hash first: the re-read takes the idempotent path, or
                    // names the real conflict.
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

/// [`provision`]'s input, validated and normalised the way the writers store
/// it: slugs and the email `lower(btrim)`, names `btrim`.
fn provision_input(a: &Provision) -> Result<Provision, DataError> {
    let invalid = |m: String| DataError::Invalid(format!("provision: {m}"));
    let Some(tenant_slug) = slug_of(&a.tenant_slug) else {
        return Err(check_violation("tenants", "slug"));
    };
    let Some(cluster_slug) = slug_of(&a.cluster_slug) else {
        return Err(check_violation("clusters", "slug"));
    };
    let tenant_name = match &a.tenant_name {
        Some(n) if btrim(n).is_empty() => {
            return Err(invalid("tenant_name must not be empty".into()))
        }
        Some(n) => {
            check_name("provision", "tenant_name", btrim(n))?;
            Some(btrim(n).to_string())
        }
        None => None,
    };
    if a.user_id.is_nil() {
        return Err(invalid("user_id must not be the nil uuid".into()));
    }
    let email = email_of("provision", &a.email)?;
    if !ROLES.contains(&a.role.as_str()) {
        return Err(invalid(format!("invalid role {}", shown(&a.role))));
    }
    check_key_input("provision key", &a.key_name, &a.key_hash, &a.scopes)?;
    let overrides = if a.overrides.is_null() {
        json!({})
    } else {
        a.overrides.clone()
    };
    check_overrides(&overrides)?;
    Ok(Provision {
        tenant_slug,
        tenant_name,
        cluster_slug,
        plan_code: a.plan_code.clone(),
        cell: a.cell,
        user_id: a.user_id,
        email,
        role: a.role.clone(),
        key_name: btrim(&a.key_name).to_string(),
        key_hash: a.key_hash.clone(),
        scopes: a.scopes.clone(),
        overrides,
    })
}

async fn kv_provision_once(
    kv: &dyn KvBackend,
    a: &Provision,
    plan: &PlanDoc,
) -> Result<Value, WriteErr> {
    let now = now_us();
    let mut b = Batch::default();

    // tenant, by slug: never one being wiped
    let (tenant, tenant_doc) =
        match read1::<Uuid>(kv, ns::TENANT_SLUG, &schema::key(&a.tenant_slug)).await? {
            Some(id) => {
                let Some(t) = read1::<TenantDoc>(kv, ns::TENANTS, &schema::key(id.value)).await?
                else {
                    return Err(WriteErr::Moved);
                };
                if t.value.status == "deleting" {
                    return Err(WriteErr::Fail(DataError::Refused {
                        code: "deleting",
                        msg: format!("provision: tenant {} is deleting", a.tenant_slug),
                    }));
                }
                (t.value.id, Some(t))
            }
            None => {
                let name = a
                    .tenant_name
                    .clone()
                    .unwrap_or_else(|| a.tenant_slug.clone());
                (new_tenant(&mut b, now, &a.tenant_slug, &name), None)
            }
        };
    let tenant_new = tenant_doc.is_none();

    // cluster, by slug: its plan is chosen at birth only
    let (cluster, cluster_new) =
        match read1::<Uuid>(kv, ns::CLUSTER_SLUG, &schema::key(&a.cluster_slug)).await? {
            Some(id) => {
                let Some(c) = cluster_doc(kv, id.value).await? else {
                    return Err(WriteErr::Moved);
                };
                if c.value.tenant_id != tenant {
                    return Err(conflict(format!(
                        "provision: cluster {} belongs to another tenant ({})",
                        a.cluster_slug, c.value.tenant_id
                    )));
                }
                let mut doc = c.value.clone();
                if doc.limit_overrides != a.overrides {
                    doc.limit_overrides = a.overrides.clone();
                    b.put_at(ns::CLUSTERS, &schema::key(doc.id), &doc, c.version);
                    record_op(
                        &mut b,
                        now,
                        tenant,
                        Some(doc.id),
                        "control_plane",
                        None,
                        "limit_override_set",
                        Some(doc.id.to_string()),
                        a.overrides.clone(),
                    );
                }
                (doc, false)
            }
            None => {
                let Some(cell) = a.cell else {
                    return Err(DataError::Invalid(
                        "provision: no cell to place a new cluster on".into(),
                    )
                    .into());
                };
                if read1::<CellDoc>(kv, ns::CELLS, &schema::key(cell))
                    .await?
                    .is_none()
                {
                    return Err(
                        DataError::Invalid(format!("provision: unknown cell {cell}")).into(),
                    );
                }
                let doc = new_cluster_doc(
                    &mut b,
                    now,
                    tenant,
                    &a.cluster_slug,
                    plan,
                    cell,
                    a.overrides.clone(),
                );
                (doc, true)
            }
        };
    let plan_code = if cluster_new {
        Some(plan.code.clone())
    } else {
        read1::<PlanDoc>(kv, ns::PLANS, &schema::key(cluster.plan_id))
            .await?
            .map(|d| d.value.code)
    };

    // the user, under the caller's id
    let user = a.user_id;
    let user_new = match read1::<UserDoc>(kv, ns::USERS, &schema::key(user)).await? {
        Some(u) => {
            if u.value.tenant_id != tenant {
                return Err(conflict(format!(
                    "provision: user {user} belongs to another tenant ({})",
                    u.value.tenant_id
                )));
            }
            if u.value.email != a.email {
                claim_email(kv, &mut b, &a.email, user).await?;
                // The old address goes, when it still names this user.
                let old_key = schema::key(&u.value.email);
                if let Some(old) = read1::<Uuid>(kv, ns::USER_EMAIL, &old_key).await? {
                    if old.value == user {
                        b.del_at(ns::USER_EMAIL, &old_key, old.version);
                    }
                }
                let mut doc = u.value.clone();
                doc.email = a.email.clone();
                b.put_at(ns::USERS, &schema::key(user), &doc, u.version);
                record_op(
                    &mut b,
                    now,
                    tenant,
                    None,
                    "control_plane",
                    Some(user),
                    "user_email_changed",
                    Some(user.to_string()),
                    json!({"from": u.value.email, "to": a.email}),
                );
            }
            false
        }
        None => {
            claim_email(kv, &mut b, &a.email, user).await?;
            user_rows(&mut b, now, tenant, user, &a.email, None, "control_plane");
            true
        }
    };

    // the role on the cluster (upsert)
    let existing_role = if cluster_new || user_new {
        None
    } else {
        read1::<RoleDoc>(kv, ns::ROLES, &schema::key2(user, cluster.id)).await?
    };
    if existing_role.as_ref().map(|d| d.value.role.as_str()) != Some(a.role.as_str()) {
        put_role(
            &mut b,
            now,
            existing_role.as_ref().map(|d| &d.value),
            user,
            cluster.id,
            &a.role,
        );
        record_op(
            &mut b,
            now,
            tenant,
            Some(cluster.id),
            "control_plane",
            Some(user),
            "cluster_role_granted",
            Some(cluster.id.to_string()),
            json!({"email": a.email, "role": a.role}),
        );
    }

    // the key: a retry adopts the one it created
    let (key_id, key_new) = match hash_state(kv, &a.key_hash).await? {
        HashState::Held(k) => (adopt_key(&k, cluster.id, "provision")?, false),
        free => {
            let index = match free {
                HashState::Dangling(version) => Some(version),
                _ => None,
            };
            let id = new_api_key(
                &mut b,
                now,
                tenant,
                cluster.id,
                &a.key_name,
                &a.key_hash,
                &a.scopes,
                index,
            );
            (id, true)
        }
    };

    let created =
        json!({"tenant": tenant_new, "cluster": cluster_new, "user": user_new, "key": key_new});
    if !b.is_empty() {
        // The tenant as read, rewritten under its version (the KV has no
        // check-only op): a status set between the read and this batch (a
        // wipe beginning) fails the batch, and the retry reads it.
        if let Some(t) = &tenant_doc {
            b.put_at(ns::TENANTS, &schema::key(t.value.id), &t.value, t.version);
        }
        record_op(
            &mut b,
            now,
            tenant,
            Some(cluster.id),
            "control_plane",
            Some(user),
            "tenant_provisioned",
            Some(tenant.to_string()),
            json!({
                "tenant_slug": a.tenant_slug, "cluster_slug": a.cluster_slug,
                "plan_code": plan_code, "email": a.email, "role": a.role,
                "key_id": key_id, "created": created,
            }),
        );
        commit(kv, b).await?;
    }
    Ok(json!({
        "tenant_id": tenant,
        "cluster_id": cluster.id,
        "broker_tenant_uuid": cluster.broker_tenant_uuid,
        "user_id": user,
        "key_id": key_id,
        "plan_code": plan_code,
        "overrides": cluster.limit_overrides,
        "created": created,
    }))
}

/// Point `px.users.email #<email>` at `user` in `b`: a free address is
/// claimed (unique), one already naming `user` stays, and a dangling one (its
/// user is gone) is taken over under its version. An address of another live
/// user is a conflict.
async fn claim_email(
    kv: &dyn KvBackend,
    b: &mut Batch,
    email: &str,
    user: Uuid,
) -> Result<(), WriteErr> {
    let k = schema::key(email);
    match read1::<Uuid>(kv, ns::USER_EMAIL, &k).await? {
        None => b.claim(ns::USER_EMAIL, &k, user, "users_email_key"),
        Some(ix) if ix.value == user => {}
        Some(ix) => {
            if read1::<UserDoc>(kv, ns::USERS, &schema::key(ix.value))
                .await?
                .is_some()
            {
                return Err(conflict(format!(
                    "provision: email {email} belongs to another user ({})",
                    ix.value
                )));
            }
            b.put_at(ns::USER_EMAIL, &k, &user, ix.version);
        }
    }
    Ok(())
}

/// Who holds an api key hash.
enum HashState {
    /// Nobody: no index entry.
    Free,
    /// An index entry no key holds the hash of (its key is gone): taken over
    /// under this version.
    Dangling(u64),
    /// This key, live or revoked.
    Held(ApiKeyDoc),
}

async fn hash_state(kv: &dyn KvBackend, key_hash: &str) -> Result<HashState, DataError> {
    let Some(ix) = read1::<Uuid>(kv, ns::KEY_HASH, &schema::key(key_hash)).await? else {
        return Ok(HashState::Free);
    };
    Ok(
        match read1::<ApiKeyDoc>(kv, ns::KEYS, &schema::key(ix.value)).await? {
            Some(k) if k.value.key_hash == key_hash => HashState::Held(k.value),
            _ => HashState::Dangling(ix.version),
        },
    )
}

/// A retry's answer for a hash a key already holds: that key, when it is live
/// and on `cluster`. Revoked, or another cluster's, the hash is taken.
fn adopt_key(k: &ApiKeyDoc, cluster: Uuid, who: &str) -> Result<Uuid, DataError> {
    if k.revoked_at_us.is_some() {
        return Err(DataError::Conflict(format!(
            "{who}: key_hash belongs to revoked key {}",
            k.id
        )));
    }
    if k.cluster_id != cluster {
        return Err(DataError::Conflict(format!(
            "{who}: key_hash belongs to a key of another cluster ({})",
            k.cluster_id
        )));
    }
    Ok(k.id)
}

/// `issue_api_key` for a caller that retries -> `(key id, existed)`. A live
/// key of this cluster holding the hash is the answer to a retry and comes
/// back as it is (whatever its name and scopes); the hash held by a revoked
/// key or by another cluster's key is a [`DataError::Conflict`].
pub async fn ensure_api_key(
    store: &Store,
    cluster_id: Uuid,
    name: &str,
    key_hash: &str,
    scopes: &[String],
) -> Result<(Uuid, bool), DataError> {
    match store {
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            check_key_input("issue_api_key", name, key_hash, scopes)?;
            let mut last =
                DataError::Unavailable("issue_api_key: concurrent change, gave up".into());
            for _ in 0..ATTEMPTS {
                let Some(c) = cluster_doc(kv, cluster_id).await? else {
                    return Err(DataError::Invalid(format!(
                        "issue_api_key: unknown cluster {cluster_id}"
                    )));
                };
                let index = match hash_state(kv, key_hash).await? {
                    HashState::Held(k) => {
                        return adopt_key(&k, cluster_id, "issue_api_key").map(|id| (id, true))
                    }
                    HashState::Free => None,
                    HashState::Dangling(version) => Some(version),
                };
                let mut b = Batch::default();
                let id = new_api_key(
                    &mut b,
                    now_us(),
                    c.value.tenant_id,
                    cluster_id,
                    btrim(name),
                    key_hash,
                    scopes,
                    index,
                );
                match commit(kv, b).await {
                    Ok(_) => return Ok((id, false)),
                    Err(WriteErr::Moved) => continue,
                    // Created concurrently: the re-read adopts it or names the
                    // conflict.
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

/// `revoke_api_key` for a caller that retries: `Some(false)` revoked by this
/// call, `Some(true)` it already was, `None` no such key (or its cluster is
/// gone: the JOIN `revoke_api_key` makes).
pub async fn ensure_api_key_revoked(
    store: &Store,
    key_id: Uuid,
) -> Result<Option<bool>, DataError> {
    match store {
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            attempts!(
                async {
                    let Some(k) = read1::<ApiKeyDoc>(kv, ns::KEYS, &schema::key(key_id)).await?
                    else {
                        return Ok(None);
                    };
                    if k.value.revoked_at_us.is_some() {
                        return Ok(Some(true));
                    }
                    let Some(c) = cluster_doc(kv, k.value.cluster_id).await? else {
                        return Ok(None);
                    };
                    commit(kv, revoke_batch(&k, c.value.tenant_id))
                        .await
                        .map(|_| Some(false))
                }
                .await
            )
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// A cluster as the control plane reads it back: its own document joined
/// with its tenant and its plan. A tenant or plan document that is gone (a
/// delete in progress) reads as `None`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ClusterRow {
    pub id: Uuid,
    pub slug: String,
    pub tenant_id: Uuid,
    pub tenant_slug: Option<String>,
    pub broker_tenant_uuid: Uuid,
    pub plan_code: Option<String>,
    /// The cluster's own status: `active | push_blocked | suspended | deleting`.
    pub status: String,
    /// The tenant's: `active | grace | suspended | deleting`.
    pub tenant_status: Option<String>,
    /// The cluster's `limit_overrides`.
    pub overrides: Value,
}

fn cluster_row_of(c: ClusterDoc, t: Option<&TenantDoc>, p: Option<&PlanDoc>) -> ClusterRow {
    ClusterRow {
        id: c.id,
        slug: c.slug,
        tenant_id: c.tenant_id,
        tenant_slug: t.map(|t| t.slug.clone()),
        broker_tenant_uuid: c.broker_tenant_uuid,
        plan_code: p.map(|p| p.code.clone()),
        status: c.status,
        tenant_status: t.map(|t| t.status.clone()),
        overrides: c.limit_overrides,
    }
}

/// Every cluster as a [`ClusterRow`], by slug; `plan` keeps the clusters on
/// that plan code only.
pub async fn list_clusters(
    store: &Store,
    plan: Option<&str>,
) -> Result<Vec<ClusterRow>, DataError> {
    match store {
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let clusters: Vec<ClusterDoc> = kv::scan::<ClusterDoc>(kv, ns::CLUSTERS, K)
                .await
                .map_err(DataError::kv)?
                .into_iter()
                .map(|(_, d)| d.value)
                .collect();
            let ids = |f: fn(&ClusterDoc) -> Uuid| -> Vec<Uuid> {
                clusters
                    .iter()
                    .map(f)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            };
            let tenants: HashMap<Uuid, TenantDoc> =
                docs_by_id::<TenantDoc>(kv, ns::TENANTS, &ids(|c| c.tenant_id))
                    .await?
                    .into_iter()
                    .map(|d| (d.value.id, d.value))
                    .collect();
            let plans: HashMap<Uuid, PlanDoc> =
                docs_by_id::<PlanDoc>(kv, ns::PLANS, &ids(|c| c.plan_id))
                    .await?
                    .into_iter()
                    .map(|d| (d.value.id, d.value))
                    .collect();
            let mut rows: Vec<ClusterRow> = clusters
                .into_iter()
                .map(|c| {
                    let (t, p) = (tenants.get(&c.tenant_id), plans.get(&c.plan_id));
                    cluster_row_of(c, t, p)
                })
                .filter(|r| plan.is_none_or(|code| r.plan_code.as_deref() == Some(code)))
                .collect();
            rows.sort_by(|a, b| a.slug.cmp(&b.slug));
            Ok(rows)
        }
        Store::None => Ok(Vec::new()),
    }
}

/// One cluster as a [`ClusterRow`], by slug or id.
pub async fn cluster_row(store: &Store, key: &ClusterKey) -> Lookup<ClusterRow> {
    let Store::Kv(kv) = store else {
        return Lookup::Absent;
    };
    let kv = kv.as_ref();
    let c = match kv_cluster_doc(kv, key).await {
        Ok(c) => c,
        Err(g) => return g.lookup(),
    };
    let out = match gets(
        kv,
        &[
            (ns::TENANTS, schema::key(c.tenant_id)),
            (ns::PLANS, schema::key(c.plan_id)),
        ],
    )
    .await
    {
        Ok(o) => o,
        Err(e) => {
            tracing::warn!(error = %e, cluster = %c.id, "cluster_row: kv lookup failed");
            return Lookup::Unavailable;
        }
    };
    match (got::<TenantDoc>(out.first()), got::<PlanDoc>(out.get(1))) {
        (Ok(t), Ok(p)) => {
            let row = cluster_row_of(
                c,
                t.as_ref().map(|d| &d.value),
                p.as_ref().map(|d| &d.value),
            );
            Lookup::Found(row)
        }
        _ => Lookup::Unavailable,
    }
}

async fn kv_tenant_by_slug(
    kv: &dyn KvBackend,
    slug: &str,
) -> Result<Option<Doc<TenantDoc>>, DataError> {
    let Some(slug) = slug_of(slug) else {
        return Ok(None);
    };
    let Some(id) = read1::<Uuid>(kv, ns::TENANT_SLUG, &schema::key(slug)).await? else {
        return Ok(None);
    };
    read1::<TenantDoc>(kv, ns::TENANTS, &schema::key(id.value)).await
}

/// A tenant by its slug.
pub async fn tenant_by_slug(store: &Store, slug: &str) -> Result<Option<TenantDoc>, DataError> {
    match store {
        Store::Kv(kv) => Ok(kv_tenant_by_slug(kv.as_ref(), slug).await?.map(|d| d.value)),
        Store::None => Ok(None),
    }
}

/// Documents of `ids`, by slug.
async fn clusters_by_slug_order(
    kv: &dyn KvBackend,
    ids: &[Uuid],
) -> Result<Vec<ClusterDoc>, DataError> {
    let mut out: Vec<ClusterDoc> = docs_by_id::<ClusterDoc>(kv, ns::CLUSTERS, ids)
        .await?
        .into_iter()
        .map(|d| d.value)
        .collect();
    out.sort_by(|a, b| a.slug.cmp(&b.slug));
    Ok(out)
}

/// `set_tenant_status` by the tenant's slug -> the tenant as it now is and
/// its clusters (by slug), `None` for no such tenant. The status a tenant
/// already has writes nothing.
pub async fn set_tenant_status_by_slug(
    store: &Store,
    slug: &str,
    status: &str,
) -> Result<Option<(TenantDoc, Vec<ClusterDoc>)>, DataError> {
    match store {
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            if !TENANT_STATUSES.contains(&status) {
                return Err(DataError::Invalid(format!(
                    "set_tenant_status: invalid status {status}"
                )));
            }
            attempts!(
                async {
                    let Some(t) = kv_tenant_by_slug(kv, slug).await? else {
                        return Ok(None);
                    };
                    let ids = index_ids(kv, ns::CLUSTER_TENANT, t.value.id).await?;
                    if t.value.status != status {
                        commit(kv, tenant_status_batch(&t, status, &ids)).await?;
                    }
                    let mut doc = t.value.clone();
                    doc.status = status.to_string();
                    Ok(Some((doc, clusters_by_slug_order(kv, &ids).await?)))
                }
                .await
            )
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// Where a cluster's broker tenant is served: what a control-plane call that
/// acts on the broker itself (a purge, a queue listing, a configure) needs.
#[derive(Clone, Debug, PartialEq)]
pub struct BrokerTarget {
    pub cluster_id: Uuid,
    pub slug: String,
    pub broker_tenant: Uuid,
    /// The cluster's own status.
    pub status: String,
    /// Its tenant's status (`None`: the tenant document is gone).
    pub tenant_status: Option<String>,
    /// The cell's base URL (`None`: the cell document is gone).
    pub base_url: Option<String>,
    pub cell_secret: Option<String>,
}

impl BrokerTarget {
    /// The cluster, or its tenant, is being torn down (a tenant document
    /// already gone counts as one).
    pub fn deleting(&self) -> bool {
        self.status == "deleting"
            || self
                .tenant_status
                .as_deref()
                .is_none_or(|s| s == "deleting")
    }
}

async fn broker_targets(
    kv: &dyn KvBackend,
    clusters: Vec<ClusterDoc>,
) -> Result<Vec<BrokerTarget>, DataError> {
    let ids = |f: fn(&ClusterDoc) -> Uuid| -> Vec<Uuid> {
        clusters
            .iter()
            .map(f)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    };
    let cells: HashMap<Uuid, CellDoc> = docs_by_id::<CellDoc>(kv, ns::CELLS, &ids(|c| c.cell_id))
        .await?
        .into_iter()
        .map(|d| (d.value.id, d.value))
        .collect();
    let tenants: HashMap<Uuid, TenantDoc> =
        docs_by_id::<TenantDoc>(kv, ns::TENANTS, &ids(|c| c.tenant_id))
            .await?
            .into_iter()
            .map(|d| (d.value.id, d.value))
            .collect();
    Ok(clusters
        .into_iter()
        .map(|c| {
            let cell = cells.get(&c.cell_id);
            BrokerTarget {
                cluster_id: c.id,
                slug: c.slug,
                broker_tenant: c.broker_tenant_uuid,
                status: c.status,
                tenant_status: tenants.get(&c.tenant_id).map(|t| t.status.clone()),
                base_url: cell.map(|x| x.base_url.clone()),
                cell_secret: cell.and_then(|x| x.cell_secret.clone()),
            }
        })
        .collect())
}

/// The cluster named `slug` as a [`BrokerTarget`].
pub async fn broker_target(store: &Store, slug: &str) -> Result<Option<BrokerTarget>, DataError> {
    match store {
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let Some(slug) = slug_of(slug) else {
                return Ok(None);
            };
            let Some(id) = read1::<Uuid>(kv, ns::CLUSTER_SLUG, &schema::key(slug)).await? else {
                return Ok(None);
            };
            let Some(c) = cluster_doc(kv, id.value).await? else {
                return Ok(None);
            };
            Ok(broker_targets(kv, vec![c.value]).await?.pop())
        }
        Store::None => Ok(None),
    }
}

/// A tenant's clusters as [`BrokerTarget`]s, by slug, and the ids its index
/// lists whose cluster document is gone (a cascade cut short): their broker
/// tenants are unknown, so nothing can vouch for them.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TenantTargets {
    pub targets: Vec<BrokerTarget>,
    pub missing: Vec<Uuid>,
}

/// Every cluster of a tenant as a [`BrokerTarget`] ([`TenantTargets`]).
pub async fn broker_targets_of_tenant(
    store: &Store,
    tenant_id: Uuid,
) -> Result<TenantTargets, DataError> {
    match store {
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let ids = index_ids(kv, ns::CLUSTER_TENANT, tenant_id).await?;
            let clusters = clusters_by_slug_order(kv, &ids).await?;
            let found: HashSet<Uuid> = clusters.iter().map(|c| c.id).collect();
            let mut missing: Vec<Uuid> = ids.into_iter().filter(|id| !found.contains(id)).collect();
            missing.sort();
            missing.dedup();
            Ok(TenantTargets {
                targets: broker_targets(kv, clusters).await?,
                missing,
            })
        }
        Store::None => Ok(TenantTargets::default()),
    }
}

/// What `POST /api/cp/activity` reads per slug from the proxy's own
/// documents, in `slugs` order: the cluster's id and the newest use of any of
/// its keys (revoked ones included), `None` for a slug no cluster has.
pub async fn activity_clusters(
    store: &Store,
    slugs: &[String],
) -> Result<Vec<Option<(Uuid, Option<i64>)>>, DataError> {
    let Store::Kv(kv) = store else {
        return Ok(vec![None; slugs.len()]);
    };
    let kv = kv.as_ref();
    // One read per distinct valid slug (getMany answers a repeated key once);
    // a slug no cluster can carry is not looked up at all.
    let keys: Vec<String> = slugs
        .iter()
        .filter_map(|s| slug_of(s).map(schema::key))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let named = kv::get_many::<Uuid>(kv, ns::CLUSTER_SLUG, &keys)
        .await
        .map_err(DataError::kv)?;
    let ids: HashMap<String, Uuid> = keys
        .iter()
        .cloned()
        .zip(named)
        .filter_map(|(k, d)| d.map(|d| (k, d.value)))
        .collect();
    let wanted: Vec<Uuid> = ids
        .values()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let live: HashSet<Uuid> = docs_by_id::<ClusterDoc>(kv, ns::CLUSTERS, &wanted)
        .await?
        .into_iter()
        .map(|d| d.value.id)
        .collect();
    let mut last_used: HashMap<Uuid, Option<i64>> = HashMap::new();
    for id in &live {
        let kids = index_ids(kv, ns::KEY_CLUSTER, *id).await?;
        let newest = docs_by_id::<ApiKeyDoc>(kv, ns::KEYS, &kids)
            .await?
            .iter()
            .filter_map(|d| d.value.last_used_at_us)
            .max();
        last_used.insert(*id, newest);
    }
    Ok(slugs
        .iter()
        .map(|s| {
            let id = *ids.get(&schema::key(slug_of(s)?))?;
            live.contains(&id)
                .then(|| (id, last_used.get(&id).copied().flatten()))
        })
        .collect())
}

// ===========================================================================
// S3 sinks (`/api/cp/clusters/:slug/s3`, cp.rs)
// ===========================================================================
//
// One row per cluster (`px.s3sinks #<cluster>`), read by the broker's sink
// manager with one getPrefix. The secret arrives here only as the broker
// sealed it (`S3Sinks::seal`, the cell's QUEEN_ENCRYPTION_KEY): nothing in
// this module sees it in clear, and no message names the sealed string.

/// The largest sink config one row takes, serialized: the row stays well under
/// the broker's KV value ceiling (64 KiB by default) with the sealed secret
/// beside it, so a config too big is a 400, never a write the KV refuses.
pub const S3_CONFIG_MAX_BYTES: usize = 32 * 1024;

/// A sink config as a row stores it: a JSON object of at most
/// [`S3_CONFIG_MAX_BYTES`], with no field named like a secret (the one secret
/// is `secretKey`, which never reaches the config: it is sealed apart). Its
/// fields are the broker's to check (`S3Sinks::validate`), not this one's.
pub fn check_s3_config(v: &Value) -> Result<(), DataError> {
    let Some(o) = v.as_object() else {
        return Err(DataError::Invalid(
            "s3 sink: the config must be a JSON object".into(),
        ));
    };
    if let Some(k) = o.keys().find(|k| k.to_ascii_lowercase().contains("secret")) {
        return Err(DataError::Invalid(format!(
            "s3 sink: {} is named like a secret: the one secret field is secretKey, which is only ever \
             stored sealed",
            shown(k)
        )));
    }
    let n = serde_json::to_vec(v).map_or(usize::MAX, |b| b.len());
    if n > S3_CONFIG_MAX_BYTES {
        return Err(DataError::Invalid(format!(
            "s3 sink: the config is {n} bytes, more than the {S3_CONFIG_MAX_BYTES} one sink row holds"
        )));
    }
    Ok(())
}

/// A cluster's S3 sink row, if it has one.
pub async fn s3_sink(store: &Store, cluster_id: Uuid) -> Result<Option<S3SinkDoc>, DataError> {
    match store {
        Store::Kv(kv) => Ok(
            read1::<S3SinkDoc>(kv.as_ref(), ns::S3SINKS, &schema::key(cluster_id))
                .await?
                .map(|d| d.value),
        ),
        Store::None => Ok(None),
    }
}

/// What [`put_s3_sink`] writes.
#[derive(Clone)]
pub struct S3SinkPut {
    /// Whether the broker runs the sink.
    pub enabled: bool,
    /// The sink config as `S3Sinks::validate` accepted it.
    pub config: Value,
    /// The request's secret as `S3Sinks::seal` returned it; `None` keeps the
    /// stored one.
    pub secret_key_sealed: Option<String>,
}

/// The stored row as a write finds it: its version, and its document when it
/// decodes (a row that does not is overwritten or removed, never a 503 that
/// no retry gets past).
fn s3_row(r: Option<&Value>) -> Result<Option<(u64, Option<S3SinkDoc>)>, DataError> {
    Ok(got::<Value>(r)?.map(|d| (d.version, serde_json::from_value::<S3SinkDoc>(d.value).ok())))
}

/// Set a cluster's S3 sink -> the row as it now is; `None` for no such
/// cluster. Refused: a cluster or tenant being deleted ([`DataError::Refused`]
/// `deleting`; the batch carries both documents' versions, so a wipe that
/// begins between the read and the write is what the retry reads), a config
/// [`check_s3_config`] refuses, and no secret while the cluster has no row
/// to keep one from.
///
/// Idempotent: a call without a secret that changes nothing writes nothing and
/// answers the row as it is. A given secret is always written (its sealed
/// bytes differ per seal, so a new secret cannot be told from a repeated
/// one), and every write moves `updated_at_us`, the row's revision: the
/// broker's sink manager rebuilds a tenant's sink when it moves, which is how
/// a rotated secret reaches the sink.
pub async fn put_s3_sink(
    store: &Store,
    cluster_id: Uuid,
    p: &S3SinkPut,
) -> Result<Option<S3SinkDoc>, DataError> {
    match store {
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            check_s3_config(&p.config)?;
            if p.secret_key_sealed.as_deref().is_some_and(str::is_empty) {
                return Err(DataError::Invalid(
                    "s3 sink: the sealed secret is empty".into(),
                ));
            }
            attempts!(kv_put_s3_sink_once(kv, cluster_id, p).await)
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// [`put_s3_sink`]'s refusal of a first sink without a secret.
fn secret_required(slug: &str) -> DataError {
    DataError::Invalid(format!(
        "s3 sink: secretKey is required: cluster {slug} has no S3 sink yet, so there is no stored \
         secret to keep"
    ))
}

async fn kv_put_s3_sink_once(
    kv: &dyn KvBackend,
    cluster_id: Uuid,
    p: &S3SinkPut,
) -> Result<Option<S3SinkDoc>, WriteErr> {
    let key = schema::key(cluster_id);
    let out = gets(
        kv,
        &[(ns::CLUSTERS, key.clone()), (ns::S3SINKS, key.clone())],
    )
    .await?;
    let Some(c) = got::<ClusterDoc>(out.first())? else {
        return Ok(None);
    };
    let row = s3_row(out.get(1))?;
    let deleting = || {
        WriteErr::Fail(DataError::Refused {
            code: "deleting",
            msg: format!(
                "s3 sink: cluster {} or its tenant is deleting: no sink is set during a wipe",
                c.value.slug
            ),
        })
    };
    // A tenant document already gone is a wipe in progress too.
    let Some(t) = read1::<TenantDoc>(kv, ns::TENANTS, &schema::key(c.value.tenant_id)).await?
    else {
        return Err(deleting());
    };
    if c.value.status == "deleting" || t.value.status == "deleting" {
        return Err(deleting());
    }
    let old = row.as_ref().and_then(|(_, d)| d.as_ref());
    let sealed = match (&p.secret_key_sealed, old) {
        (Some(s), _) => s.clone(),
        (None, Some(o)) => o.secret_key_sealed.clone(),
        (None, None) => return Err(secret_required(&c.value.slug).into()),
    };
    let unchanged = old.filter(|o| {
        p.secret_key_sealed.is_none()
            && o.enabled == p.enabled
            && o.config == p.config
            && o.broker_tenant == c.value.broker_tenant_uuid
    });
    if let Some(o) = unchanged {
        return Ok(Some(o.clone()));
    }
    let now = now_us();
    let doc = S3SinkDoc {
        cluster_id,
        broker_tenant: c.value.broker_tenant_uuid,
        enabled: p.enabled,
        config: p.config.clone(),
        secret_key_sealed: sealed,
        updated_at_us: now,
    };
    let mut b = Batch::default();
    match &row {
        Some((version, _)) => b.put_at(ns::S3SINKS, &key, &doc, *version),
        None => b.put_new(ns::S3SINKS, &key, &doc),
    }
    // The cluster and its tenant as read, rewritten under their versions (the
    // KV has no check-only op): a status set between the read and this batch
    // fails it, and the retry reads the wipe.
    b.put_at(ns::CLUSTERS, &key, &c.value, c.version);
    b.put_at(ns::TENANTS, &schema::key(t.value.id), &t.value, t.version);
    let secret = if p.secret_key_sealed.is_some() {
        "set"
    } else {
        "kept"
    };
    record_op(
        &mut b,
        now,
        t.value.id,
        Some(cluster_id),
        "control_plane",
        None,
        "s3_sink_set",
        Some(cluster_id.to_string()),
        json!({"enabled": p.enabled, "secret": secret}),
    );
    commit(kv, b).await?;
    Ok(Some(doc))
}

/// Remove a cluster's S3 sink -> whether it had one. Idempotent.
pub async fn delete_s3_sink(store: &Store, cluster_id: Uuid) -> Result<bool, DataError> {
    match store {
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            attempts!(
                async {
                    let key = schema::key(cluster_id);
                    let out = gets(
                        kv,
                        &[(ns::S3SINKS, key.clone()), (ns::CLUSTERS, key.clone())],
                    )
                    .await?;
                    let Some((version, _)) = s3_row(out.first())? else {
                        return Ok(false);
                    };
                    let mut b = Batch::default();
                    b.del_at(ns::S3SINKS, &key, version);
                    if let Some(c) = got::<ClusterDoc>(out.get(1))? {
                        record_op(
                            &mut b,
                            now_us(),
                            c.value.tenant_id,
                            Some(cluster_id),
                            "control_plane",
                            None,
                            "s3_sink_removed",
                            Some(cluster_id.to_string()),
                            json!({}),
                        );
                    }
                    commit(kv, b).await.map(|_| true)
                }
                .await
            )
        }
        Store::None => Err(DataError::NoStore),
    }
}

/// Remove the S3 sinks of `clusters` (a tenant's, before its purge) -> how
/// many there were. Idempotent.
pub async fn delete_s3_sinks(store: &Store, clusters: &[Uuid]) -> Result<usize, DataError> {
    match store {
        Store::Kv(kv) => {
            let kv = kv.as_ref();
            let keys: BTreeSet<String> = clusters.iter().map(schema::key).collect();
            let keys: Vec<String> = keys.into_iter().collect();
            let mut removed = 0;
            for chunk in keys.chunks(CHUNK_OPS) {
                let ops = chunk
                    .iter()
                    .map(|k| kv::delete_op(ns::S3SINKS, k, Expect::Any, false))
                    .collect();
                let written = kv::write(kv, ops).await.map_err(DataError::kv)?;
                removed += written.iter().filter(|w| w.applied).count();
            }
            Ok(removed)
        }
        Store::None => Ok(0),
    }
}

// ===========================================================================
// catalog seeding
// ===========================================================================

/// The four default plans and their feature families.
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

/// Seed the plan catalog when absent (an existing plan code is left alone);
/// the number of plans written.
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

/// A cell as ops provision it.
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

/// A test world in a fresh in-memory KV: the default plans, one cell at
/// `base_url`, and one tenant whose cluster is `slug` on `plan`. Returns the
/// store, the cluster id and the bootstrap API key (plaintext).
#[cfg(test)]
pub(crate) async fn test_world(slug: &str, base_url: &str, plan: &str) -> (Store, Uuid, String) {
    let store = Store::Kv(std::sync::Arc::new(super::memkv::MemKv::new()));
    seed_default_plans(&store).await.expect("plans");
    let cell = upsert_cell(
        &store,
        &CellSpec {
            slug: "local".into(),
            region: "local".into(),
            base_url: base_url.into(),
            class: "shared".into(),
            capacity_slots: 1,
            cell_secret: None,
        },
    )
    .await
    .expect("cell");
    let out = bootstrap_tenant(
        &store,
        &Bootstrap {
            tenant_slug: slug.into(),
            cluster_slug: slug.into(),
            plan_code: plan.into(),
            cell: Some(cell),
            admin_email: format!("admin@{slug}.test"),
            ..Default::default()
        },
    )
    .await
    .expect("bootstrap");
    let cluster = Uuid::parse_str(out["cluster_id"].as_str().expect("cluster_id")).expect("uuid");
    let key = out["api_key"].as_str().expect("api_key").to_string();
    (store, cluster, key)
}

// ===========================================================================
// tests: against MemKv
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
    fn iso_utc_trims_the_fraction_and_names_the_offset() {
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
        // queue rows, touches and session revocations do not invalidate
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
        // a row that outlived its token (written without a TTL) is swept
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
        for cluster in [c, other_c] {
            let sink = S3SinkPut {
                enabled: true,
                config: json!({"bucket": "lake"}),
                secret_key_sealed: Some("sealed".into()),
            };
            put_s3_sink(&s, cluster, &sink).await.unwrap().unwrap();
        }

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
        assert_eq!(kv.0.keys(ns::S3SINKS), vec![schema::key(other_c)]);
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
            "the deny-list carries no tenant"
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

    /// Of two concurrent deletes one wins, the other answers existed=false,
    /// and the surviving audit is written once.
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

    /// StrictKv that counts the calls carrying a write.
    struct Counting {
        inner: StrictKv,
        writes: std::sync::atomic::AtomicUsize,
    }

    impl KvBackend for Counting {
        fn kv(&self, ops: Vec<Value>) -> kv::BoxFut<'_, Result<Vec<Value>, KvError>> {
            let writes = ops.iter().any(|o| {
                matches!(
                    o["op"].as_str(),
                    Some("put" | "putIfAbsent" | "delete" | "incr")
                )
            });
            if writes {
                self.writes
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            self.inner.kv(ops)
        }
    }

    fn provision_args(cell: Uuid, user: Uuid) -> Provision {
        Provision {
            tenant_slug: " Trial ".into(),
            tenant_name: None,
            cluster_slug: "trial-c".into(),
            plan_code: "free".into(),
            cell: Some(cell),
            user_id: user,
            email: "Ops@Trial.io".into(),
            role: "admin".into(),
            key_name: "signup".into(),
            key_hash: "ab".repeat(32),
            scopes: vec!["produce".into(), "consume".into()],
            overrides: json!({"max_req_per_sec": 10, "monthly_msgs_quota": null}),
        }
    }

    /// The whole tenancy is ONE write call (StrictKv also refuses a call over
    /// the op ceiling or with two writes of one key), a repeat writes nothing,
    /// and a change (the email, the overrides) is one call again.
    #[tokio::test]
    async fn provision_is_one_write_and_a_repeat_writes_none() {
        let kv = Arc::new(Counting {
            inner: StrictKv(MemKv::new()),
            writes: Default::default(),
        });
        let s = Store::Kv(kv.clone());
        seed_default_plans(&s).await.unwrap();
        let cell = cell(&s).await;
        let user = Uuid::new_v4();
        let writes = || kv.writes.load(std::sync::atomic::Ordering::SeqCst);
        let before = writes();
        let a = provision(&s, &provision_args(cell, user)).await.unwrap();
        assert_eq!(writes() - before, 1, "one atomic batch");
        assert_eq!(
            a["created"],
            json!({"tenant": true, "cluster": true, "user": true, "key": true})
        );
        let cluster = Uuid::parse_str(a["cluster_id"].as_str().unwrap()).unwrap();
        let m = &kv.inner.0;
        assert_eq!(m.keys(ns::TENANT_SLUG), vec!["#trial".to_string()]);
        assert_eq!(m.keys(ns::USER_EMAIL), vec!["#ops@trial.io".to_string()]);
        assert_eq!(m.keys(ns::USERS), vec![schema::key(user)]);
        assert_eq!(m.keys(ns::ROLES), vec![schema::key2(user, cluster)]);
        assert_eq!(m.keys(ns::KEY_HASH), vec![schema::key("ab".repeat(32))]);
        let tenant: TenantDoc = read1(
            m,
            ns::TENANTS,
            &schema::key(Uuid::parse_str(a["tenant_id"].as_str().unwrap()).unwrap()),
        )
        .await
        .unwrap()
        .unwrap()
        .value;
        assert_eq!(tenant.name, "trial", "no name: the slug");

        let b = provision(&s, &provision_args(cell, user)).await.unwrap();
        assert_eq!(writes() - before, 1, "the repeat writes nothing");
        assert_eq!(
            (a["key_id"].clone(), a["cluster_id"].clone()),
            (b["key_id"].clone(), b["cluster_id"].clone())
        );

        let mut moved = provision_args(cell, user);
        moved.email = "new@trial.io".into();
        moved.overrides = Value::Null;
        let c = provision(&s, &moved).await.unwrap();
        assert_eq!(writes() - before, 2, "a change is one batch too");
        assert_eq!(c["overrides"], json!({}));
        assert_eq!(m.keys(ns::USER_EMAIL), vec!["#new@trial.io".to_string()]);
        let ctx = found(lookup_cluster(&s, &ClusterKey::Id(cluster)).await);
        assert_eq!(ctx.limits.max_req_per_sec, Some(5), "the plan's again");
    }

    /// Two nodes provisioning one tenancy at once: the unique claims make one
    /// of them lose its batch, and its retry takes the idempotent path.
    #[tokio::test]
    async fn concurrent_provisions_converge_on_one_tenancy() {
        let (s, kv) = store();
        seed_default_plans(&s).await.unwrap();
        let cell = cell(&s).await;
        let args = provision_args(cell, Uuid::new_v4());
        let (a, b) = tokio::join!(provision(&s, &args), provision(&s, &args));
        let (a, b) = (a.unwrap(), b.unwrap());
        for k in [
            "tenant_id",
            "cluster_id",
            "broker_tenant_uuid",
            "user_id",
            "key_id",
        ] {
            assert_eq!(a[k], b[k], "{k}");
        }
        let creators = [&a, &b]
            .iter()
            .filter(|r| r["created"]["tenant"] == json!(true))
            .count();
        assert_eq!(creators, 1, "{a} / {b}");
        for space in [ns::TENANTS, ns::CLUSTERS, ns::USERS, ns::KEYS, ns::ROLES] {
            assert_eq!(kv.0.keys(space).len(), 1, "{space}");
        }
    }

    /// Index entries whose rows are gone (a cascade cut short) do not block a
    /// provision or a key: the email and the hash are taken over under their
    /// versions.
    #[tokio::test]
    async fn dangling_index_entries_are_taken_over() {
        let (s, kv) = store();
        seed_default_plans(&s).await.unwrap();
        let cell = cell(&s).await;
        let ghost = Uuid::new_v4();
        let args = provision_args(cell, Uuid::new_v4());
        let other = "cd".repeat(32);
        let ops = [
            ("#ops@trial.io".to_string(), ns::USER_EMAIL),
            (schema::key(&args.key_hash), ns::KEY_HASH),
            (schema::key(&other), ns::KEY_HASH),
        ]
        .iter()
        .map(|(k, space)| kv::put_op(space, k, &ghost, Expect::Any, Ttl::Forever, false))
        .collect();
        kv::write(&kv.0, ops).await.unwrap();

        let out = provision(&s, &args).await.unwrap();
        assert_eq!(
            out["created"],
            json!({"tenant": true, "cluster": true, "user": true, "key": true})
        );
        let ix: Uuid = read1(&kv.0, ns::USER_EMAIL, "#ops@trial.io")
            .await
            .unwrap()
            .unwrap()
            .value;
        assert_eq!(ix, args.user_id);
        let (_, kid, _) = found(lookup_api_key(&s, &args.key_hash).await);
        assert_eq!(json!(kid), out["key_id"]);

        let cluster = Uuid::parse_str(out["cluster_id"].as_str().unwrap()).unwrap();
        let (id, existed) = ensure_api_key(&s, cluster, "ci", &other, &["read".to_string()])
            .await
            .unwrap();
        assert!(!existed);
        assert_eq!(found(lookup_api_key(&s, &other).await).1, id);
        assert_eq!(
            ensure_api_key(&s, cluster, "x", &other, &["read".to_string()])
                .await
                .unwrap(),
            (id, true)
        );
    }

    /// `OVERRIDE_KEYS` names what the merge reads: each of the eleven limit
    /// keys moves exactly one effective limit; the twelfth is the quota pass's.
    #[test]
    fn override_keys_are_the_limits_the_merge_reads() {
        let one = Some(1);
        let base = EffectiveLimits {
            max_req_per_sec: one,
            req_burst: one,
            max_msgs_per_sec: one,
            msgs_burst: one,
            max_queues: one,
            max_partitions_per_queue: one,
            max_parked_pops: one,
            max_payload_bytes: one,
            max_batch_items: one,
            max_retained_bytes: one,
            max_retention_seconds: one,
        };
        for k in &OVERRIDE_KEYS[..11] {
            let mut o = serde_json::Map::new();
            o.insert(k.to_string(), json!(7));
            let merged = format!(
                "{:?}",
                crate::cache::merge_limits(base.clone(), &Value::Object(o))
            );
            assert_eq!(merged.matches("Some(7)").count(), 1, "{k}: {merged}");
        }
        assert_eq!(OVERRIDE_KEYS[11], "monthly_msgs_quota");
        assert!(check_overrides(&json!({})).is_ok());
        assert!(check_overrides(&json!({"max_queues": 0, "monthly_msgs_quota": null})).is_ok());
        for bad in [
            json!(null),
            json!([]),
            json!({"x": 1}),
            json!({"max_queues": -1}),
            json!({"max_queues": 1.5}),
            json!({"max_queues": "1"}),
        ] {
            assert!(check_overrides(&bad).is_err(), "{bad}");
        }
    }

    /// The control plane's reads: every cluster joined with its tenant and
    /// plan, a tenant's clusters as broker targets, and a status set by slug
    /// that writes once.
    #[tokio::test]
    async fn cluster_rows_targets_and_status_by_slug() {
        let (s, kv) = store();
        let (t, c, _, _) = world(&s).await;
        let cell = cell(&s).await;
        let c2 = create_cluster(&s, t, "acme-dev", "free", cell)
            .await
            .unwrap();
        let rows = list_clusters(&s, None).await.unwrap();
        assert_eq!(
            rows.iter().map(|r| r.slug.as_str()).collect::<Vec<_>>(),
            vec!["acme-dev", "acme-prod"]
        );
        assert_eq!(list_clusters(&s, Some("pro")).await.unwrap().len(), 1);
        let row = match cluster_row(&s, &ClusterKey::Id(c)).await {
            Lookup::Found(r) => r,
            _ => panic!("row"),
        };
        assert_eq!(
            (
                row.tenant_slug.as_deref(),
                row.plan_code.as_deref(),
                row.tenant_status.as_deref()
            ),
            (Some("acme"), Some("pro"), Some("active"))
        );
        let listed = broker_targets_of_tenant(&s, t).await.unwrap();
        let targets = &listed.targets;
        assert!(listed.missing.is_empty());
        assert_eq!(
            targets.iter().map(|x| x.cluster_id).collect::<Vec<_>>(),
            vec![c2, c]
        );
        assert_eq!(
            targets[1].base_url.as_deref(),
            Some("http://127.0.0.1:6632")
        );
        assert_eq!(targets[1].cell_secret.as_deref(), Some("s3cret"));
        assert_eq!(
            (
                targets[1].status.as_str(),
                targets[1].tenant_status.as_deref()
            ),
            ("active", Some("active"))
        );
        assert!(!targets[1].deleting());
        assert_eq!(
            broker_target(&s, "ACME-PROD")
                .await
                .unwrap()
                .map(|x| x.cluster_id),
            Some(c)
        );

        let ops = kv.0.keys(ns::OPS).len();
        let (doc, clusters) = set_tenant_status_by_slug(&s, " Acme ", "grace")
            .await
            .unwrap()
            .unwrap();
        assert_eq!((doc.status.as_str(), clusters.len()), ("grace", 2));
        assert_eq!(kv.0.keys(ns::OPS).len(), ops + 1);
        set_tenant_status_by_slug(&s, "acme", "grace")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(kv.0.keys(ns::OPS).len(), ops + 1, "unchanged: no write");
        assert!(set_tenant_status_by_slug(&s, "nope", "grace")
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            found(lookup_cluster(&s, &ClusterKey::Id(c2)).await).status,
            ClusterStatus::PushBlocked
        );

        // A cluster whose tenant document is gone still reads back.
        kv.0.kv(vec![kv::delete_op(
            ns::TENANTS,
            &schema::key(t),
            Expect::Any,
            false,
        )])
        .await
        .unwrap();
        let orphan = list_clusters(&s, None).await.unwrap();
        assert!(orphan
            .iter()
            .all(|r| r.tenant_slug.is_none() && r.tenant_status.is_none()));
        // ...and a broker target without its tenant counts as being torn down.
        let x = broker_target(&s, "acme-dev").await.unwrap().unwrap();
        assert!(x.tenant_status.is_none() && x.deleting());

        // A cluster the tenant's index lists but whose document is gone is
        // missing, never silently dropped.
        kv.0.kv(vec![kv::delete_op(
            ns::CLUSTERS,
            &schema::key(c2),
            Expect::Any,
            false,
        )])
        .await
        .unwrap();
        let listed = broker_targets_of_tenant(&s, t).await.unwrap();
        assert_eq!(
            listed
                .targets
                .iter()
                .map(|x| x.cluster_id)
                .collect::<Vec<_>>(),
            vec![c]
        );
        assert_eq!(listed.missing, vec![c2]);
    }

    /// The one slug gate, and the bounds every key a writer looks up keeps
    /// within (a key the KV refuses would read as the store being down).
    #[tokio::test]
    async fn slugs_names_emails_and_plan_codes_are_gated_before_any_read() {
        assert_eq!(slug_of(" Acme-Prod\t").as_deref(), Some("acme-prod"));
        for bad in ["", "-x", "a_b", "é", &"a".repeat(64)] {
            assert_eq!(slug_of(bad), None, "{bad:?}");
        }
        assert_eq!(shown(&"x".repeat(100)).chars().count(), 65);

        // Every call below would fail with Unavailable if it reached the store.
        let s = Store::Kv(Arc::new(crate::store::memkv::Down));
        let huge = "a".repeat(10_000);
        assert_eq!(tenant_by_slug(&s, &huge).await.unwrap(), None);
        assert_eq!(broker_target(&s, &huge).await.unwrap(), None);
        assert_eq!(
            activity_clusters(&s, std::slice::from_ref(&huge))
                .await
                .unwrap(),
            vec![None]
        );
        assert!(set_tenant_status_by_slug(&s, &huge, "grace")
            .await
            .unwrap()
            .is_none());
        let p = Provision {
            plan_code: huge.clone(),
            ..provision_args(Uuid::new_v4(), Uuid::new_v4())
        };
        let err = provision(&s, &p).await.unwrap_err();
        assert!(
            matches!(&err, DataError::Invalid(m) if m.contains("unknown plan code") && m.len() < 200),
            "{err}"
        );
        for p in [
            Provision {
                tenant_slug: huge.clone(),
                ..provision_args(Uuid::new_v4(), Uuid::new_v4())
            },
            Provision {
                email: format!("{huge}@x.io"),
                ..provision_args(Uuid::new_v4(), Uuid::new_v4())
            },
            Provision {
                tenant_name: Some(huge.clone()),
                ..provision_args(Uuid::new_v4(), Uuid::new_v4())
            },
            Provision {
                key_name: huge.clone(),
                ..provision_args(Uuid::new_v4(), Uuid::new_v4())
            },
        ] {
            assert!(matches!(
                provision(&s, &p).await,
                Err(DataError::Invalid(_))
            ));
        }
        let b = Bootstrap {
            tenant_slug: huge.clone(),
            cluster_slug: "c".into(),
            plan_code: "free".into(),
            cell: Some(Uuid::new_v4()),
            admin_email: "a@b.io".into(),
            ..Default::default()
        };
        assert_eq!(
            bootstrap_tenant(&s, &b).await.unwrap_err(),
            check_violation("tenants", "slug")
        );
        let b = Bootstrap {
            tenant_slug: "t".into(),
            admin_email: format!("{huge}@x.io"),
            ..b
        };
        assert!(matches!(
            bootstrap_tenant(&s, &b).await,
            Err(DataError::Invalid(_))
        ));
    }

    /// A tenant being wiped is never provisioned again, and a wipe that
    /// begins between provision's read and its write is what the retry sees:
    /// the batch carries the tenant's version.
    #[tokio::test]
    async fn provision_refuses_a_tenant_being_deleted_even_one_that_starts_meanwhile() {
        let (s, _) = store();
        seed_default_plans(&s).await.unwrap();
        let cell = cell(&s).await;
        let user = Uuid::new_v4();
        let out = provision(&s, &provision_args(cell, user)).await.unwrap();
        let t = Uuid::parse_str(out["tenant_id"].as_str().unwrap()).unwrap();

        // The race: a status set lands right before provision's batch.
        let inner = Arc::new(StrictKv(MemKv::new()));
        let racing = Arc::new(Racing {
            inner: inner.clone(),
            armed: std::sync::atomic::AtomicBool::new(false),
        });
        let r = Store::Kv(racing.clone());
        seed_default_plans(&r).await.unwrap();
        let rcell = upsert_cell(
            &r,
            &CellSpec {
                slug: "local".into(),
                region: "eu".into(),
                base_url: "http://127.0.0.1:6632".into(),
                class: "shared".into(),
                capacity_slots: 1,
                cell_secret: None,
            },
        )
        .await
        .unwrap();
        provision(&r, &provision_args(rcell, user)).await.unwrap();
        let mut changed = provision_args(rcell, user);
        changed.overrides = json!({"max_queues": 3});
        racing
            .armed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let err = provision(&r, &changed).await.unwrap_err();
        assert_eq!(
            err,
            DataError::Refused {
                code: "deleting",
                msg: "provision: tenant trial is deleting".into()
            }
        );
        let ctx = found(lookup_cluster(&r, &ClusterKey::Slug("trial-c".into())).await);
        assert_eq!(
            ctx.limits.max_queues,
            Some(20),
            "the refused batch wrote nothing"
        );

        // ...and the plain case.
        set_tenant_status(&s, t, "deleting").await.unwrap();
        let err = provision(&s, &provision_args(cell, user))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                DataError::Refused {
                    code: "deleting",
                    ..
                }
            ),
            "{err}"
        );
    }

    /// A KV that, once armed, sets the tenant of the first batch carrying a
    /// tenant version precondition to `deleting` just before that batch runs.
    struct Racing {
        inner: Arc<StrictKv>,
        armed: std::sync::atomic::AtomicBool,
    }

    impl KvBackend for Racing {
        fn kv(&self, ops: Vec<Value>) -> kv::BoxFut<'_, Result<Vec<Value>, KvError>> {
            Box::pin(async move {
                let guarded = ops
                    .iter()
                    .find(|o| o["ns"] == ns::TENANTS && o.get("expect").is_some());
                if let Some(o) = guarded {
                    if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
                        let mut doc: TenantDoc =
                            serde_json::from_value(o["value"].clone()).unwrap();
                        doc.status = "deleting".into();
                        let key = o["key"].as_str().unwrap().to_string();
                        self.inner
                            .0
                            .kv(vec![kv::put_op(
                                ns::TENANTS,
                                &key,
                                &doc,
                                Expect::Any,
                                Ttl::Forever,
                                false,
                            )])
                            .await
                            .unwrap();
                    }
                }
                self.inner.kv(ops).await
            })
        }
    }

    fn sink_put(sealed: Option<&str>, enabled: bool) -> S3SinkPut {
        S3SinkPut {
            enabled,
            config: json!({"bucket": "lake"}),
            secret_key_sealed: sealed.map(str::to_string),
        }
    }

    /// A sink row: no secret until there is one to keep, kept when omitted,
    /// nothing written for a repeat, a row that does not decode overwritten
    /// or removed rather than stuck, and no sink for a tenant being wiped.
    #[tokio::test]
    async fn s3_sink_rows_keep_their_secret_and_refuse_a_wipe() {
        let (s, kv) = store();
        let (t, c, _, _) = world(&s).await;
        let err = put_s3_sink(&s, c, &sink_put(None, true)).await.unwrap_err();
        assert!(
            matches!(&err, DataError::Invalid(m) if m.contains("secretKey is required")),
            "{err}"
        );
        let nobody = put_s3_sink(&s, Uuid::new_v4(), &sink_put(Some("x"), true)).await;
        assert_eq!(nobody, Ok(None), "no such cluster");
        for bad in [
            json!([1]),
            json!({"bucket": "lake", "awsSecret": "x"}),
            json!({"bucket": "a".repeat(S3_CONFIG_MAX_BYTES)}),
        ] {
            let p = S3SinkPut {
                config: bad,
                ..sink_put(Some("x"), true)
            };
            assert!(matches!(
                put_s3_sink(&s, c, &p).await,
                Err(DataError::Invalid(_))
            ));
        }
        assert!(kv.0.keys(ns::S3SINKS).is_empty());

        let first = put_s3_sink(&s, c, &sink_put(Some("sealed-1"), true))
            .await
            .unwrap()
            .unwrap();
        let kept = put_s3_sink(&s, c, &sink_put(None, false))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (kept.secret_key_sealed.as_str(), kept.enabled),
            ("sealed-1", false)
        );
        assert!(kept.updated_at_us >= first.updated_at_us);
        assert_eq!(s3_sink(&s, c).await.unwrap(), Some(kept.clone()));
        let key = schema::key(c);
        let v1 = kv::get::<Value>(&kv.0, ns::S3SINKS, &key)
            .await
            .unwrap()
            .unwrap()
            .version;
        let again = put_s3_sink(&s, c, &sink_put(None, false)).await.unwrap();
        assert_eq!(again, Some(kept));
        let v2 = kv::get::<Value>(&kv.0, ns::S3SINKS, &key)
            .await
            .unwrap()
            .unwrap()
            .version;
        assert_eq!(v2, v1, "a repeat writes nothing");

        let garbage = || {
            let junk = json!({"v": 0});
            kv::put_op(ns::S3SINKS, &key, &junk, Expect::Any, Ttl::Forever, false)
        };
        kv::write(&kv.0, vec![garbage()]).await.unwrap();
        assert!(matches!(
            s3_sink(&s, c).await,
            Err(DataError::Unavailable(_))
        ));
        assert!(matches!(
            put_s3_sink(&s, c, &sink_put(None, true)).await,
            Err(DataError::Invalid(_))
        ));
        let fixed = put_s3_sink(&s, c, &sink_put(Some("sealed-2"), true))
            .await
            .unwrap();
        assert_eq!(s3_sink(&s, c).await.unwrap(), fixed);
        kv::write(&kv.0, vec![garbage()]).await.unwrap();
        assert!(delete_s3_sink(&s, c).await.unwrap());
        assert!(!delete_s3_sink(&s, c).await.unwrap());

        set_tenant_status(&s, t, "deleting").await.unwrap();
        let err = put_s3_sink(&s, c, &sink_put(Some("sealed-3"), true))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                DataError::Refused {
                    code: "deleting",
                    ..
                }
            ),
            "{err}"
        );
        assert!(kv.0.keys(ns::S3SINKS).is_empty());
        assert_eq!(delete_s3_sinks(&s, &[c, c, Uuid::new_v4()]).await, Ok(0));
        assert_eq!(delete_s3_sinks(&Store::None, &[c]).await, Ok(0));
        assert_eq!(
            put_s3_sink(&Store::None, c, &sink_put(Some("x"), true)).await,
            Err(DataError::NoStore)
        );
    }

    /// A wipe that begins between the read and the write of a sink is what
    /// the retry reads: the batch carries the tenant's version.
    #[tokio::test]
    async fn an_s3_sink_set_as_a_wipe_begins_is_refused() {
        let inner = Arc::new(StrictKv(MemKv::new()));
        let racing = Arc::new(Racing {
            inner: inner.clone(),
            armed: std::sync::atomic::AtomicBool::new(false),
        });
        let r = Store::Kv(racing.clone());
        let (_, c, _, _) = world(&r).await;
        racing
            .armed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let err = put_s3_sink(&r, c, &sink_put(Some("sealed"), true))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                DataError::Refused {
                    code: "deleting",
                    ..
                }
            ),
            "{err}"
        );
        assert!(
            inner.0.keys(ns::S3SINKS).is_empty(),
            "the refused batch wrote nothing"
        );
    }

    /// The override document is checked wherever it is written.
    #[tokio::test]
    async fn the_overrides_route_writes_only_what_the_merge_reads() {
        let (s, _) = store();
        let (_, c, _, _) = world(&s).await;
        for bad in [
            json!({"max_bogus": 1}),
            json!({"max_queues": -1}),
            json!([1]),
        ] {
            assert!(
                matches!(
                    set_limit_override(&s, c, Some(&bad)).await,
                    Err(DataError::Invalid(_))
                ),
                "{bad}"
            );
        }
        set_limit_override(
            &s,
            c,
            Some(&json!({"max_queues": 9, "monthly_msgs_quota": null})),
        )
        .await
        .unwrap();
        assert_eq!(
            found(lookup_cluster(&s, &ClusterKey::Id(c)).await)
                .limits
                .max_queues,
            Some(9)
        );
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
