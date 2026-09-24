//! W5 — the one-shot import of a standalone proxy's Postgres into the broker's
//! replicated KV (PLAN_SINGLE_BINARY.md W5). The broker calls
//! [`import_from_pg`] at boot when told to; nothing else writes through here.
//!
//! **What moves.** All 14 proxy tables, each row as its [`schema`] document
//! WITH its index keys, in the same atomic batch (a row never lands without
//! its indexes). Batches of at most [`usage::KV_BATCH`] ops / 512 KiB.
//! Read from ONE `REPEATABLE READ READ ONLY` transaction, table by table
//! through server-side cursors: a consistent snapshot, bounded memory.
//!
//! **What does not.** `revoked_tokens` rows already past their expiry (the
//! JWT is dead on its own) and `usage_minutes` rows past the KV retention
//! (`usage::minute_ttl_secs` — PLAN W5's "usage history cut to the retention
//! window"; `usage_days` keeps the totals forever). Imported minutes carry
//! ONE node label (`node`, e.g. [`DEFAULT_USAGE_NODE`]), so they never
//! collide with a live node's own rows. `queues.deleted_at` rows keep their
//! document but not the live-name index. `schema_migrations` is Postgres
//! bookkeeping, not proxy state.
//!
//! **Idempotent.** `px.meta #schema` records the run: an in-progress marker
//! first, then `imported_at_us` + per-table counts (`MetaDoc::imported_rows`).
//! A finished import makes the next call a no-op; an interrupted one is
//! resumed (every write is a plain put of the same document, so re-writing
//! the rows it already wrote changes nothing). A KV that holds tenant data
//! that did NOT come from an import is refused unless `force` — the catalog
//! (plans, cells) does not count, since a fresh broker may seed it: a seeded
//! plan or cell whose code/slug a Postgres row also has is replaced by the
//! Postgres row (same code, the Postgres id wins; the orphan document goes).
//!
//! A row the broker refuses on its own (a value over its 64 KiB ceiling) is
//! skipped, logged and counted (`<table>_refused`), not fatal; "no leader" is
//! retried with backoff; anything else stops the import where it is (resume
//! by calling again).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use deadpool_postgres::Pool;
use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use super::kv::{self, BoxFut, Expect, KvBackend, KvError, Ttl};
use super::schema::{
    self, key, key2, ns, ApiKeyDoc, CellDoc, ClusterDoc, IdentityDoc, MetaDoc, OperationDoc, OutboxDoc, PlanDoc,
    QueueDoc, RevokedDoc, RoleDoc, TenantDoc, UsageDoc, UserDoc, SCHEMA_VERSION,
};
use super::usage;

/// The node label imported usage_minutes carry.
pub const DEFAULT_USAGE_NODE: &str = "imported";

const META_KEY: &str = "#schema";
/// `MetaDoc::imported_rows` keys that are not table counts.
const IN_PROGRESS: &str = "_in_progress";
const STARTED: &str = "_started_at_us";
const SKIPPED: &str = "_skipped";
const USAGE_NODE: &str = "_usage_node";

const MAX_BATCH_BYTES: usize = 512 * 1024;
const PG_PAGE: usize = 2000;
/// "No leader" retries per batch (100 ms doubling to 5 s: ~35 s in all).
const RETRIES: u32 = 10;

/// The proxy's tables, in import order (catalog, then owners before owned).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Table {
    Plans,
    Cells,
    Tenants,
    Users,
    Identities,
    Clusters,
    ClusterRoles,
    ApiKeys,
    Queues,
    UsageDays,
    UsageMinutes,
    Operations,
    RevokedTokens,
    Outbox,
}

impl Table {
    pub const ALL: [Table; 14] = [
        Table::Plans,
        Table::Cells,
        Table::Tenants,
        Table::Users,
        Table::Identities,
        Table::Clusters,
        Table::ClusterRoles,
        Table::ApiKeys,
        Table::Queues,
        Table::UsageDays,
        Table::UsageMinutes,
        Table::Operations,
        Table::RevokedTokens,
        Table::Outbox,
    ];

    /// The Postgres table name (`queen_proxy.<name>`), also the report key.
    pub fn name(self) -> &'static str {
        match self {
            Table::Plans => "plans",
            Table::Cells => "cells",
            Table::Tenants => "tenants",
            Table::Users => "users",
            Table::Identities => "identities",
            Table::Clusters => "clusters",
            Table::ClusterRoles => "cluster_roles",
            Table::ApiKeys => "api_keys",
            Table::Queues => "queues",
            Table::UsageDays => "usage_days",
            Table::UsageMinutes => "usage_minutes",
            Table::Operations => "operations",
            Table::RevokedTokens => "revoked_tokens",
            Table::Outbox => "outbox",
        }
    }
}

/// One Postgres row, already in its KV document shape.
#[derive(Clone, Debug, PartialEq)]
pub enum ImportRow {
    Tenant(TenantDoc),
    User(UserDoc),
    Identity(IdentityDoc),
    Plan(PlanDoc),
    Cell(CellDoc),
    Cluster(ClusterDoc),
    Role(RoleDoc),
    ApiKey(ApiKeyDoc),
    Queue(QueueDoc),
    UsageMinute { cluster_id: Uuid, minute_us: i64, op_class: String, usage: UsageDoc },
    UsageDay { cluster_id: Uuid, day: String, op_class: String, usage: UsageDoc },
    Operation(OperationDoc),
    Revoked(RevokedDoc),
    Outbox(OutboxDoc),
}

/// Where the rows come from: the proxy's Postgres ([`PgSource`]), or a fake.
pub trait ImportSource: Send {
    /// The next page of `table`'s rows; an empty page means the table is done.
    /// Tables are asked for in [`Table::ALL`] order, each until it is done.
    fn next_page(&mut self, table: Table) -> BoxFut<'_, Result<Vec<ImportRow>, String>>;
}

#[derive(Clone, Debug)]
pub struct ImportOptions {
    /// Import although the KV holds tenant data that did not come from an
    /// import (overwriting by id), or again over a finished import.
    pub force: bool,
    /// Minute retention: the TTL of imported minutes, and the cut of older ones.
    pub usage_keep_days: u64,
    /// Rows per Postgres FETCH.
    pub page_rows: usize,
}

impl Default for ImportOptions {
    fn default() -> Self {
        ImportOptions { force: false, usage_keep_days: usage::keep_days_from_env(), page_rows: PG_PAGE }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct ImportReport {
    /// A finished import was already recorded: this call did nothing.
    pub already_imported: bool,
    pub forced: bool,
    /// When the import finished (this call's, or the recorded one's).
    pub imported_at_us: Option<i64>,
    /// Rows written, per Postgres table (all 14 keys).
    pub counts: BTreeMap<String, u64>,
    /// Rows not written, per reason: `revoked_tokens_expired`,
    /// `usage_minutes_expired`, `<table>_refused`.
    pub skipped: BTreeMap<String, u64>,
    /// The node label the imported usage_minutes carry.
    pub usage_node: String,
}

/// Copy the standalone proxy's Postgres into the KV (see the module header).
/// `node` labels the imported usage_minutes (e.g. [`DEFAULT_USAGE_NODE`]).
pub async fn import_from_pg(pool: &Pool, kv: &dyn KvBackend, node: &str) -> Result<ImportReport, String> {
    import_from_pg_with(pool, kv, node, &ImportOptions::default()).await
}

/// [`import_from_pg`] with options (`force`, retention, page size).
pub async fn import_from_pg_with(
    pool: &Pool,
    kv: &dyn KvBackend,
    node: &str,
    opts: &ImportOptions,
) -> Result<ImportReport, String> {
    let node = usage::node_label(node);
    let meta = match preflight(kv, opts).await? {
        Preflight::Done(report) => return Ok(report),
        Preflight::Go(meta) => meta,
    };
    let mut src = PgSource::open(pool, opts.page_rows).await?;
    let res = run(&mut src, kv, &node, opts, meta).await;
    src.close().await;
    res
}

/// The import from any [`ImportSource`].
pub async fn import_from(
    src: &mut dyn ImportSource,
    kv: &dyn KvBackend,
    node: &str,
    opts: &ImportOptions,
) -> Result<ImportReport, String> {
    let node = usage::node_label(node);
    match preflight(kv, opts).await? {
        Preflight::Done(report) => Ok(report),
        Preflight::Go(meta) => run(src, kv, &node, opts, meta).await,
    }
}

// ---------------------------------------------------------------------------
// Preflight: done already, resumable, or refused
// ---------------------------------------------------------------------------

enum Preflight {
    Done(ImportReport),
    Go(Option<kv::Doc<MetaDoc>>),
}

/// Namespaces holding tenant data. Plans and cells are the catalog a fresh
/// broker may seed; they are reconciled by code/slug instead.
const TENANT_SPACES: [&str; 12] = [
    ns::TENANTS,
    ns::USERS,
    ns::IDENTITIES,
    ns::CLUSTERS,
    ns::ROLES,
    ns::KEYS,
    ns::QUEUES,
    ns::USAGE_MIN,
    ns::USAGE_DAY,
    ns::OPS,
    ns::REVOKED,
    ns::OUTBOX,
];

async fn preflight(kv: &dyn KvBackend, opts: &ImportOptions) -> Result<Preflight, String> {
    let meta = retry(|| kv::get::<MetaDoc>(kv, ns::META, META_KEY)).await.map_err(|e| e.to_string())?;
    if let Some(m) = &meta {
        if m.value.imported_at_us.is_some() && !opts.force {
            return Ok(Preflight::Done(report_from_meta(&m.value)));
        }
    }
    let resuming = meta.as_ref().is_some_and(|m| {
        m.value.imported_at_us.is_none() && m.value.imported_rows.get(IN_PROGRESS) == Some(&Value::Bool(true))
    });
    if !resuming && !opts.force {
        if let Some(space) = occupied(kv).await? {
            return Err(format!(
                "refusing to import: the KV already holds proxy data in {space} that did not come from an import \
                 (force imports over it)"
            ));
        }
    }
    Ok(Preflight::Go(meta))
}

/// The first tenant-data namespace that has any row.
async fn occupied(kv: &dyn KvBackend) -> Result<Option<&'static str>, String> {
    let ops: Vec<Value> = TENANT_SPACES
        .iter()
        .map(|s| json!({"op":"getPrefix","ns":s,"prefix":schema::K,"limit":1,"keysOnly":true}))
        .collect();
    let out = retry(|| kv.kv(ops.clone())).await.map_err(|e| e.to_string())?;
    for (space, r) in TENANT_SPACES.iter().zip(out) {
        if r.get("rows").and_then(Value::as_array).is_some_and(|a| !a.is_empty()) {
            return Ok(Some(space));
        }
    }
    Ok(None)
}

fn report_from_meta(m: &MetaDoc) -> ImportReport {
    let mut report = ImportReport { already_imported: true, imported_at_us: m.imported_at_us, ..Default::default() };
    if let Some(o) = m.imported_rows.as_object() {
        for (k, v) in o {
            if let (false, Some(n)) = (k.starts_with('_'), v.as_u64()) {
                report.counts.insert(k.clone(), n);
            }
        }
        if let Some(s) = o.get(SKIPPED).and_then(Value::as_object) {
            for (k, v) in s {
                report.skipped.insert(k.clone(), v.as_u64().unwrap_or(0));
            }
        }
        report.usage_node = o.get(USAGE_NODE).and_then(Value::as_str).unwrap_or_default().to_string();
    }
    report
}

fn rows_json(report: &ImportReport) -> Value {
    let mut o = serde_json::Map::new();
    for (k, v) in &report.counts {
        o.insert(k.clone(), Value::from(*v));
    }
    o.insert(SKIPPED.into(), json!(report.skipped));
    o.insert(USAGE_NODE.into(), Value::from(report.usage_node.clone()));
    Value::Object(o)
}

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

async fn run(
    src: &mut dyn ImportSource,
    kv: &dyn KvBackend,
    node: &str,
    opts: &ImportOptions,
    meta: Option<kv::Doc<MetaDoc>>,
) -> Result<ImportReport, String> {
    let started = usage::now_us();
    let version = meta.as_ref().map_or(SCHEMA_VERSION, |m| m.value.version.max(SCHEMA_VERSION));
    let marker = MetaDoc {
        version,
        imported_at_us: None,
        imported_rows: json!({IN_PROGRESS: true, STARTED: started, USAGE_NODE: node}),
    };
    let expect = meta.as_ref().map_or(Expect::Absent, |m| Expect::Version(m.version));
    let marker_version = put_meta(kv, &marker, expect).await?;
    tracing::info!(target: "store", node, force = opts.force, resumed = meta.is_some(), "proxy import from postgres starting");

    let mut report = ImportReport { forced: opts.force, usage_node: node.to_string(), ..Default::default() };
    let mut w = Writer { kv, groups: Vec::new(), ops: 0, bytes: 0, keys: HashSet::new() };
    for table in Table::ALL {
        report.counts.entry(table.name().to_string()).or_insert(0);
        loop {
            let page = src.next_page(table).await?;
            if page.is_empty() {
                break;
            }
            let stale = if opts.force { HashMap::new() } else { catalog_stale(kv, &page).await? };
            for row in &page {
                match row_ops(row, node, opts.usage_keep_days, started) {
                    Ok(mut ops) => {
                        if let Some((space, old)) =
                            row_catalog(row).and_then(|(id, space, _)| stale.get(&id).map(|o| (space, o)))
                        {
                            ops.push(kv::delete_op(space, &key(old), Expect::Any, false));
                        }
                        w.push(table, ops, &mut report).await?;
                    }
                    Err(reason) => *report.skipped.entry(reason.to_string()).or_default() += 1,
                }
            }
        }
        w.flush(&mut report).await?;
        tracing::info!(target: "store", table = table.name(), rows = report.counts[table.name()], "proxy import: table done");
    }

    let now = usage::now_us();
    let done = MetaDoc { version, imported_at_us: Some(now), imported_rows: rows_json(&report) };
    put_meta(kv, &done, Expect::Version(marker_version)).await?;
    report.imported_at_us = Some(now);
    tracing::info!(target: "store", counts = ?report.counts, skipped = ?report.skipped, "proxy import from postgres done");
    Ok(report)
}

/// Write `px.meta #schema` under `expect`. A retry whose first attempt landed
/// (lost ack) finds its own document there and counts as done.
async fn put_meta(kv: &dyn KvBackend, doc: &MetaDoc, expect: Expect) -> Result<u64, String> {
    let op = kv::put_op(ns::META, META_KEY, doc, expect, Ttl::Forever, true);
    match retry(|| kv::write(kv, vec![op.clone()])).await {
        Ok(w) => Ok(w.first().map(|w| w.version).unwrap_or(0)),
        Err(KvError::Precondition { detail }) => match kv::get::<MetaDoc>(kv, ns::META, META_KEY).await {
            Ok(Some(cur)) if cur.value == *doc => Ok(cur.version),
            _ => Err(format!("px.meta #schema changed under the import (another import running?): {detail}")),
        },
        Err(e) => Err(e.to_string()),
    }
}

/// For plan and cell rows: `(row id, doc namespace, (unique index ns, key))`.
fn row_catalog(row: &ImportRow) -> Option<(Uuid, &'static str, (&'static str, String))> {
    match row {
        ImportRow::Plan(p) => Some((p.id, ns::PLANS, (ns::PLAN_CODE, key(&p.code)))),
        ImportRow::Cell(c) => Some((c.id, ns::CELLS, (ns::CELL_SLUG, key(&c.slug)))),
        _ => None,
    }
}

/// Catalog rows of `page` whose code/slug the KV already maps to ANOTHER id
/// (a seeded plan or cell): row id → that other id, whose document goes.
async fn catalog_stale(kv: &dyn KvBackend, page: &[ImportRow]) -> Result<HashMap<Uuid, Uuid>, String> {
    let mut by_space: BTreeMap<&'static str, Vec<(Uuid, String)>> = BTreeMap::new();
    for row in page {
        if let Some((id, _, (space, k))) = row_catalog(row) {
            by_space.entry(space).or_default().push((id, k));
        }
    }
    // An id this very page imports is never "stale" (codes renamed in
    // Postgres between an interrupted run and its resume).
    let imported: HashSet<Uuid> = by_space.values().flatten().map(|(id, _)| *id).collect();
    let mut stale = HashMap::new();
    for (space, rows) in by_space {
        for chunk in rows.chunks(usage::KV_BATCH) {
            let keys: Vec<String> = chunk.iter().map(|(_, k)| k.clone()).collect();
            let got = retry(|| kv::get_many::<Uuid>(kv, space, &keys)).await.map_err(|e| e.to_string())?;
            for ((id, _), found) in chunk.iter().zip(got) {
                if let Some(d) = found.filter(|d| d.value != *id && !imported.contains(&d.value)) {
                    stale.insert(*id, d.value);
                }
            }
        }
    }
    Ok(stale)
}

fn put<T: Serialize>(space: &str, k: &str, v: &T) -> Value {
    kv::put_op(space, k, v, Expect::Any, Ttl::Forever, false)
}

/// A row's writes — its document and every index key (schema.rs) — or why it
/// is skipped. `now_us` decides the expiries (revoked tokens, minute TTLs).
pub fn row_ops(row: &ImportRow, node: &str, keep_days: u64, now_us: i64) -> Result<Vec<Value>, &'static str> {
    const NONE: &str = "";
    Ok(match row {
        ImportRow::Tenant(t) => vec![put(ns::TENANTS, &key(t.id), t), put(ns::TENANT_SLUG, &key(&t.slug), &t.id)],
        ImportRow::User(u) => vec![
            put(ns::USERS, &key(u.id), u),
            put(ns::USER_EMAIL, &key(&u.email), &u.id),
            put(ns::USER_TENANT, &key2(u.tenant_id, u.id), &NONE),
        ],
        ImportRow::Identity(i) => vec![
            put(ns::IDENTITIES, &key(i.id), i),
            put(ns::IDENTITY_PROVIDER, &key(format!("{}:{}", i.provider, i.provider_id)), &i.id),
            put(ns::IDENTITY_USER, &key2(i.user_id, i.id), &NONE),
        ],
        ImportRow::Plan(p) => vec![put(ns::PLANS, &key(p.id), p), put(ns::PLAN_CODE, &key(&p.code), &p.id)],
        ImportRow::Cell(c) => vec![put(ns::CELLS, &key(c.id), c), put(ns::CELL_SLUG, &key(&c.slug), &c.id)],
        ImportRow::Cluster(c) => vec![
            put(ns::CLUSTERS, &key(c.id), c),
            put(ns::CLUSTER_SLUG, &key(&c.slug), &c.id),
            put(ns::CLUSTER_TENANT, &key2(c.tenant_id, c.id), &NONE),
            put(ns::CLUSTER_CELL, &key2(c.cell_id, c.id), &NONE),
        ],
        ImportRow::Role(r) => vec![
            put(ns::ROLES, &key2(r.user_id, r.cluster_id), r),
            put(ns::ROLE_CLUSTER, &key2(r.cluster_id, r.user_id), &NONE),
        ],
        ImportRow::ApiKey(k) => vec![
            put(ns::KEYS, &key(k.id), k),
            put(ns::KEY_HASH, &key(&k.key_hash), &k.id),
            put(ns::KEY_CLUSTER, &key2(k.cluster_id, k.id), &NONE),
        ],
        ImportRow::Queue(q) => {
            let mut ops = vec![put(ns::QUEUES, &key(q.id), q)];
            if q.deleted_at_us.is_none() {
                ops.push(put(ns::QUEUE_NAME, &key2(q.cluster_id, &q.name), &q.id));
            }
            ops
        }
        ImportRow::UsageMinute { cluster_id, minute_us, op_class, usage: doc } => {
            let ttl = usage::minute_ttl_secs(*minute_us, keep_days, now_us).ok_or("usage_minutes_expired")?;
            vec![kv::put_op(
                ns::USAGE_MIN,
                &usage::minute_key(*cluster_id, *minute_us, op_class, node),
                doc,
                Expect::Any,
                Ttl::Seconds(ttl),
                false,
            )]
        }
        ImportRow::UsageDay { cluster_id, day, op_class, usage: doc } => {
            vec![put(ns::USAGE_DAY, &usage::day_key(*cluster_id, day, op_class), doc)]
        }
        ImportRow::Operation(o) => {
            let tail = format!("{}/{}", schema::inverted(o.at_us), o.id);
            let mut ops = vec![put(ns::OPS, &key2(o.tenant_id, &tail), o)];
            if let Some(c) = o.cluster_id {
                ops.push(put(ns::OPS_CLUSTER, &key2(c, &tail), &NONE));
            }
            ops
        }
        ImportRow::Revoked(r) => {
            let left = r.expires_at_us - now_us;
            if left <= 0 {
                return Err("revoked_tokens_expired");
            }
            let ttl = ((left + 999_999) / 1_000_000) as u64;
            vec![kv::put_op(ns::REVOKED, &key(&r.jti), r, Expect::Any, Ttl::Seconds(ttl), false)]
        }
        ImportRow::Outbox(o) => vec![put(ns::OUTBOX, &key2(schema::ordered(o.created_at_us), o.id), o)],
    })
}

// ---------------------------------------------------------------------------
// Batching
// ---------------------------------------------------------------------------

/// Row groups (a row's doc + index ops) packed into atomic batches: a group is
/// never split, a batch never writes one key twice (the broker refuses that).
struct Writer<'a> {
    kv: &'a dyn KvBackend,
    groups: Vec<(Table, Vec<Value>)>,
    ops: usize,
    bytes: usize,
    keys: HashSet<(String, String)>,
}

impl Writer<'_> {
    async fn push(&mut self, table: Table, ops: Vec<Value>, report: &mut ImportReport) -> Result<(), String> {
        let bytes: usize = ops.iter().map(|o| o.to_string().len()).sum();
        let keys: Vec<(String, String)> = ops
            .iter()
            .map(|o| {
                let s = |f: &str| o.get(f).and_then(Value::as_str).unwrap_or_default().to_string();
                (s("ns"), s("key"))
            })
            .collect();
        let clash = keys.iter().any(|k| self.keys.contains(k));
        if !self.groups.is_empty()
            && (clash || self.ops + ops.len() > usage::KV_BATCH || self.bytes + bytes > MAX_BATCH_BYTES)
        {
            self.flush(report).await?;
        }
        self.ops += ops.len();
        self.bytes += bytes;
        self.keys.extend(keys);
        self.groups.push((table, ops));
        Ok(())
    }

    async fn flush(&mut self, report: &mut ImportReport) -> Result<(), String> {
        if self.groups.is_empty() {
            return Ok(());
        }
        let groups = std::mem::take(&mut self.groups);
        self.ops = 0;
        self.bytes = 0;
        self.keys.clear();
        let all: Vec<Value> = groups.iter().flat_map(|(_, ops)| ops.iter().cloned()).collect();
        match retry(|| kv::write(self.kv, all.clone())).await {
            Ok(_) => {
                for (t, _) in &groups {
                    *report.counts.entry(t.name().to_string()).or_default() += 1;
                }
                Ok(())
            }
            // One row the broker will not take must not sink the import:
            // write the groups one by one and skip the one that fails alone.
            Err(KvError::Invalid { .. }) => {
                for (t, ops) in groups {
                    match retry(|| kv::write(self.kv, ops.clone())).await {
                        Ok(_) => *report.counts.entry(t.name().to_string()).or_default() += 1,
                        Err(KvError::Invalid { status, reason, detail }) => {
                            let k = ops.first().and_then(|o| o.get("key")).and_then(Value::as_str).unwrap_or("?");
                            tracing::warn!(target: "store", table = t.name(), key = k, status, reason = %reason, detail = %detail, "proxy import: the broker refused a row; skipped");
                            *report.skipped.entry(format!("{}_refused", t.name())).or_default() += 1;
                        }
                        Err(e) => return Err(e.to_string()),
                    }
                }
                Ok(())
            }
            Err(e) => Err(e.to_string()),
        }
    }
}

/// `f()` again while the broker answers "unavailable" (no leader, election),
/// with backoff; anything else is returned as is.
async fn retry<T, F, Fut>(f: F) -> Result<T, KvError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, KvError>>,
{
    let mut delay = Duration::from_millis(100);
    let mut attempt = 0;
    loop {
        match f().await {
            Err(KvError::Unavailable(m)) if attempt < RETRIES => {
                attempt += 1;
                tracing::warn!(target: "store", attempt, error = %m, "proxy import: kv unavailable, retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
            other => return other,
        }
    }
}

// ---------------------------------------------------------------------------
// The Postgres source
// ---------------------------------------------------------------------------

/// Epoch microseconds of a timestamptz column, exact on every Postgres
/// version (`extract(epoch)` is a double before 14): whole seconds, plus the
/// microseconds within the second.
fn us(col: &str) -> String {
    format!(
        "(extract(epoch from date_trunc('second', {col}))::bigint * 1000000 \
         + extract(microseconds from {col})::bigint % 1000000)"
    )
}

fn select_sql(t: Table) -> String {
    match t {
        Table::Tenants => format!("SELECT id::text, slug, name, status, {} FROM queen_proxy.tenants ORDER BY id", us("created_at")),
        Table::Users => format!(
            "SELECT id::text, tenant_id::text, email, password_hash, name, is_operator, {}, {} \
             FROM queen_proxy.users ORDER BY id",
            us("last_login_at"),
            us("created_at")
        ),
        Table::Identities => format!(
            "SELECT id::text, user_id::text, provider, provider_id, email, verified, {} \
             FROM queen_proxy.identities ORDER BY id",
            us("created_at")
        ),
        Table::Plans => format!(
            "SELECT id::text, code, cell_class, max_req_per_sec::bigint, req_burst::bigint, \
                    max_msgs_per_sec::bigint, msgs_burst::bigint, max_queues::bigint, \
                    max_partitions_per_queue::bigint, max_parked_pops::bigint, max_payload_bytes::bigint, \
                    max_batch_items::bigint, max_retained_bytes, max_retention_seconds::bigint, \
                    monthly_msgs_quota, features::text, {} \
             FROM queen_proxy.plans ORDER BY id",
            us("created_at")
        ),
        Table::Cells => format!(
            "SELECT id::text, slug, region, base_url, class, capacity_slots::bigint, used_slots::bigint, \
                    broker_version, status, cell_secret, {} \
             FROM queen_proxy.cells ORDER BY id",
            us("created_at")
        ),
        Table::Clusters => format!(
            "SELECT id::text, tenant_id::text, cell_id::text, plan_id::text, slug, broker_tenant_uuid::text, \
                    status, limit_overrides::text, {} \
             FROM queen_proxy.clusters ORDER BY id",
            us("created_at")
        ),
        Table::ClusterRoles => format!(
            "SELECT user_id::text, cluster_id::text, role, {} FROM queen_proxy.cluster_roles ORDER BY user_id, cluster_id",
            us("created_at")
        ),
        Table::ApiKeys => format!(
            "SELECT id::text, cluster_id::text, name, key_hash, scopes, created_by::text, {}, {}, {} \
             FROM queen_proxy.api_keys ORDER BY id",
            us("created_at"),
            us("last_used_at"),
            us("revoked_at")
        ),
        Table::Queues => format!(
            "SELECT id::text, cluster_id::text, name, partitions_count::bigint, {}, {} \
             FROM queen_proxy.queues ORDER BY id",
            us("created_at"),
            us("deleted_at")
        ),
        Table::UsageDays => "SELECT cluster_id::text, to_char(day, 'YYYY-MM-DD'), op_class, msgs, reqs, bytes_in, bytes_out \
             FROM queen_proxy.usage_days ORDER BY cluster_id, day, op_class"
            .to_string(),
        Table::UsageMinutes => format!(
            "SELECT cluster_id::text, {}, op_class, msgs, reqs, bytes_in, bytes_out \
             FROM queen_proxy.usage_minutes ORDER BY cluster_id, minute, op_class",
            us("minute")
        ),
        Table::Operations => format!(
            "SELECT id::text, tenant_id::text, cluster_id::text, actor, actor_id::text, action, target, meta::text, {} \
             FROM queen_proxy.operations ORDER BY at, id",
            us("at")
        ),
        Table::RevokedTokens => format!("SELECT jti, {} FROM queen_proxy.revoked_tokens ORDER BY jti", us("expires_at")),
        Table::Outbox => format!(
            "SELECT id::text, kind, payload::text, {}, {} FROM queen_proxy.outbox ORDER BY created_at, id",
            us("created_at"),
            us("consumed_at")
        ),
    }
}

fn pg_err(e: tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => db.to_string(),
        None => e.to_string(),
    }
}

fn col<'a, T: tokio_postgres::types::FromSql<'a>>(r: &'a tokio_postgres::Row, i: usize) -> Result<T, String> {
    r.try_get(i).map_err(|e| format!("column {i}: {e}"))
}

fn uuid_col(r: &tokio_postgres::Row, i: usize) -> Result<Uuid, String> {
    let s: String = col(r, i)?;
    Uuid::parse_str(&s).map_err(|e| format!("column {i}: {e}"))
}

fn opt_uuid_col(r: &tokio_postgres::Row, i: usize) -> Result<Option<Uuid>, String> {
    let s: Option<String> = col(r, i)?;
    s.map(|s| Uuid::parse_str(&s).map_err(|e| format!("column {i}: {e}"))).transpose()
}

/// A jsonb read as text; `{}` if it somehow does not parse.
fn json_col(r: &tokio_postgres::Row, i: usize) -> Result<Value, String> {
    let s: String = col(r, i)?;
    Ok(serde_json::from_str(&s).unwrap_or_else(|_| json!({})))
}

fn usage_doc(r: &tokio_postgres::Row, first: usize) -> Result<UsageDoc, String> {
    Ok(UsageDoc {
        msgs: col(r, first)?,
        reqs: col(r, first + 1)?,
        bytes_in: col(r, first + 2)?,
        bytes_out: col(r, first + 3)?,
    })
}

fn parse_row(t: Table, r: &tokio_postgres::Row) -> Result<ImportRow, String> {
    Ok(match t {
        Table::Tenants => ImportRow::Tenant(TenantDoc {
            id: uuid_col(r, 0)?,
            slug: col(r, 1)?,
            name: col(r, 2)?,
            status: col(r, 3)?,
            created_at_us: col(r, 4)?,
        }),
        Table::Users => ImportRow::User(UserDoc {
            id: uuid_col(r, 0)?,
            tenant_id: uuid_col(r, 1)?,
            email: col(r, 2)?,
            password_hash: col(r, 3)?,
            name: col(r, 4)?,
            is_operator: col(r, 5)?,
            last_login_at_us: col(r, 6)?,
            created_at_us: col(r, 7)?,
        }),
        Table::Identities => ImportRow::Identity(IdentityDoc {
            id: uuid_col(r, 0)?,
            user_id: uuid_col(r, 1)?,
            provider: col(r, 2)?,
            provider_id: col(r, 3)?,
            email: col(r, 4)?,
            verified: col(r, 5)?,
            created_at_us: col(r, 6)?,
        }),
        Table::Plans => ImportRow::Plan(PlanDoc {
            id: uuid_col(r, 0)?,
            code: col(r, 1)?,
            cell_class: col(r, 2)?,
            max_req_per_sec: col(r, 3)?,
            req_burst: col(r, 4)?,
            max_msgs_per_sec: col(r, 5)?,
            msgs_burst: col(r, 6)?,
            max_queues: col(r, 7)?,
            max_partitions_per_queue: col(r, 8)?,
            max_parked_pops: col(r, 9)?,
            max_payload_bytes: col(r, 10)?,
            max_batch_items: col(r, 11)?,
            max_retained_bytes: col(r, 12)?,
            max_retention_seconds: col(r, 13)?,
            monthly_msgs_quota: col(r, 14)?,
            features: json_col(r, 15)?,
            created_at_us: col(r, 16)?,
        }),
        Table::Cells => ImportRow::Cell(CellDoc {
            id: uuid_col(r, 0)?,
            slug: col(r, 1)?,
            region: col(r, 2)?,
            base_url: col(r, 3)?,
            class: col(r, 4)?,
            capacity_slots: col(r, 5)?,
            used_slots: col(r, 6)?,
            broker_version: col(r, 7)?,
            status: col(r, 8)?,
            cell_secret: col(r, 9)?,
            created_at_us: col(r, 10)?,
        }),
        Table::Clusters => ImportRow::Cluster(ClusterDoc {
            id: uuid_col(r, 0)?,
            tenant_id: uuid_col(r, 1)?,
            cell_id: uuid_col(r, 2)?,
            plan_id: uuid_col(r, 3)?,
            slug: col(r, 4)?,
            broker_tenant_uuid: uuid_col(r, 5)?,
            status: col(r, 6)?,
            limit_overrides: json_col(r, 7)?,
            created_at_us: col(r, 8)?,
        }),
        Table::ClusterRoles => ImportRow::Role(RoleDoc {
            user_id: uuid_col(r, 0)?,
            cluster_id: uuid_col(r, 1)?,
            role: col(r, 2)?,
            created_at_us: col(r, 3)?,
        }),
        Table::ApiKeys => ImportRow::ApiKey(ApiKeyDoc {
            id: uuid_col(r, 0)?,
            cluster_id: uuid_col(r, 1)?,
            name: col(r, 2)?,
            key_hash: col(r, 3)?,
            scopes: col(r, 4)?,
            created_by: opt_uuid_col(r, 5)?,
            created_at_us: col(r, 6)?,
            last_used_at_us: col(r, 7)?,
            revoked_at_us: col(r, 8)?,
        }),
        Table::Queues => ImportRow::Queue(QueueDoc {
            id: uuid_col(r, 0)?,
            cluster_id: uuid_col(r, 1)?,
            name: col(r, 2)?,
            partitions_count: col(r, 3)?,
            created_at_us: col(r, 4)?,
            deleted_at_us: col(r, 5)?,
        }),
        Table::UsageDays => ImportRow::UsageDay {
            cluster_id: uuid_col(r, 0)?,
            day: col(r, 1)?,
            op_class: col(r, 2)?,
            usage: usage_doc(r, 3)?,
        },
        Table::UsageMinutes => ImportRow::UsageMinute {
            cluster_id: uuid_col(r, 0)?,
            minute_us: col(r, 1)?,
            op_class: col(r, 2)?,
            usage: usage_doc(r, 3)?,
        },
        Table::Operations => ImportRow::Operation(OperationDoc {
            id: uuid_col(r, 0)?,
            tenant_id: uuid_col(r, 1)?,
            cluster_id: opt_uuid_col(r, 2)?,
            actor: col(r, 3)?,
            actor_id: opt_uuid_col(r, 4)?,
            action: col(r, 5)?,
            target: col(r, 6)?,
            meta: json_col(r, 7)?,
            at_us: col(r, 8)?,
        }),
        Table::RevokedTokens => ImportRow::Revoked(RevokedDoc { jti: col(r, 0)?, expires_at_us: col(r, 1)? }),
        Table::Outbox => ImportRow::Outbox(OutboxDoc {
            id: uuid_col(r, 0)?,
            kind: col(r, 1)?,
            payload: json_col(r, 2)?,
            created_at_us: col(r, 3)?,
            consumed_at_us: col(r, 4)?,
        }),
    })
}

/// The last migration the import's queries are written against.
const NEEDS_MIGRATION: &str = "011_user_last_login";

/// The proxy's Postgres, read through one `REPEATABLE READ READ ONLY`
/// transaction (a consistent snapshot of all 14 tables) and one server-side
/// cursor per table. Call [`PgSource::close`] when done: the connection must
/// not go back to its pool inside the transaction (it is dropped from the
/// pool instead when the COMMIT fails).
pub struct PgSource {
    client: deadpool_postgres::Object,
    open: Option<Table>,
    page: usize,
}

impl PgSource {
    pub async fn open(pool: &Pool, page: usize) -> Result<PgSource, String> {
        let client = pool.get().await.map_err(|e| format!("pxdb: {e}"))?;
        // A database the standalone proxy never started on has no ledger at
        // all: the same advice as one migrated only part of the way.
        let ledger: Option<String> = client
            .query_one("SELECT to_regclass('queen_proxy.schema_migrations')::text", &[])
            .await
            .map_err(pg_err)?
            .get(0);
        let applied = match ledger {
            Some(_) => client
                .query_opt("SELECT 1 FROM queen_proxy.schema_migrations WHERE name = $1", &[&NEEDS_MIGRATION])
                .await
                .map_err(pg_err)?,
            None => None,
        };
        if applied.is_none() {
            return Err(format!(
                "the proxy's postgres is not migrated through {NEEDS_MIGRATION}: start the standalone proxy on it once, then import"
            ));
        }
        client.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await.map_err(pg_err)?;
        Ok(PgSource { client, open: None, page: page.max(1) })
    }

    pub async fn close(self) {
        if self.client.batch_execute("COMMIT").await.is_err() {
            let _ = deadpool_postgres::Object::take(self.client);
        }
    }

    fn cursor(t: Table) -> String {
        format!("px_import_{}", t.name())
    }
}

impl ImportSource for PgSource {
    fn next_page(&mut self, table: Table) -> BoxFut<'_, Result<Vec<ImportRow>, String>> {
        Box::pin(async move {
            if self.open != Some(table) {
                if let Some(prev) = self.open.take() {
                    self.client.batch_execute(&format!("CLOSE {}", Self::cursor(prev))).await.map_err(pg_err)?;
                }
                self.client
                    .batch_execute(&format!(
                        "DECLARE {} NO SCROLL CURSOR FOR {}",
                        Self::cursor(table),
                        select_sql(table)
                    ))
                    .await
                    .map_err(|e| format!("{}: {}", table.name(), pg_err(e)))?;
                self.open = Some(table);
            }
            let rows = self
                .client
                .query(&format!("FETCH FORWARD {} FROM {}", self.page, Self::cursor(table)), &[])
                .await
                .map_err(|e| format!("{}: {}", table.name(), pg_err(e)))?;
            rows.iter().map(|r| parse_row(table, r).map_err(|e| format!("{}: {e}", table.name()))).collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::memkv::MemKv;
    use super::*;
    use serde::de::DeserializeOwned;

    const DAY: i64 = usage::DAY_US;

    /// Pages of in-memory rows per table.
    struct Fake {
        rows: HashMap<Table, Vec<ImportRow>>,
        at: HashMap<Table, usize>,
        page: usize,
        /// Fail when this table is first asked for (an interrupted import).
        fail_on: Option<Table>,
    }

    impl Fake {
        fn new(rows: Vec<ImportRow>, page: usize) -> Fake {
            let mut by: HashMap<Table, Vec<ImportRow>> = HashMap::new();
            for r in rows {
                by.entry(table_of(&r)).or_default().push(r);
            }
            Fake { rows: by, at: HashMap::new(), page, fail_on: None }
        }
    }

    fn table_of(r: &ImportRow) -> Table {
        match r {
            ImportRow::Tenant(_) => Table::Tenants,
            ImportRow::User(_) => Table::Users,
            ImportRow::Identity(_) => Table::Identities,
            ImportRow::Plan(_) => Table::Plans,
            ImportRow::Cell(_) => Table::Cells,
            ImportRow::Cluster(_) => Table::Clusters,
            ImportRow::Role(_) => Table::ClusterRoles,
            ImportRow::ApiKey(_) => Table::ApiKeys,
            ImportRow::Queue(_) => Table::Queues,
            ImportRow::UsageMinute { .. } => Table::UsageMinutes,
            ImportRow::UsageDay { .. } => Table::UsageDays,
            ImportRow::Operation(_) => Table::Operations,
            ImportRow::Revoked(_) => Table::RevokedTokens,
            ImportRow::Outbox(_) => Table::Outbox,
        }
    }

    impl ImportSource for Fake {
        fn next_page(&mut self, table: Table) -> BoxFut<'_, Result<Vec<ImportRow>, String>> {
            Box::pin(async move {
                if self.fail_on == Some(table) {
                    self.fail_on = None;
                    return Err("postgres went away".to_string());
                }
                let all = self.rows.get(&table).cloned().unwrap_or_default();
                let at = self.at.entry(table).or_insert(0);
                let end = (*at + self.page).min(all.len());
                let page = all[*at..end].to_vec();
                *at = end;
                Ok(page)
            })
        }
    }

    struct World {
        now: i64,
        tenant: TenantDoc,
        user: UserDoc,
        identity: IdentityDoc,
        plan: PlanDoc,
        cell: CellDoc,
        cluster: ClusterDoc,
        role: RoleDoc,
        key: ApiKeyDoc,
        live_queue: QueueDoc,
        dead_queue: QueueDoc,
        op_cluster: OperationDoc,
        op_tenant: OperationDoc,
        revoked: RevokedDoc,
        outbox: OutboxDoc,
        minute_us: i64,
    }

    fn world() -> World {
        let now = usage::now_us();
        let tenant = TenantDoc {
            id: Uuid::new_v4(),
            slug: "acme".into(),
            name: "Acme".into(),
            status: "active".into(),
            created_at_us: now - 10 * DAY,
        };
        let user = UserDoc {
            id: Uuid::new_v4(),
            tenant_id: tenant.id,
            email: "admin@acme.io".into(),
            password_hash: Some("$2a$10$x".into()),
            name: Some("Ada".into()),
            is_operator: true,
            last_login_at_us: Some(now - 3_600_000_000),
            created_at_us: now - 9 * DAY + 123_456,
        };
        let identity = IdentityDoc {
            id: Uuid::new_v4(),
            user_id: user.id,
            provider: "github".into(),
            provider_id: "4242".into(),
            email: user.email.clone(),
            verified: true,
            created_at_us: now - 9 * DAY,
        };
        let plan = PlanDoc {
            id: Uuid::new_v4(),
            code: "pro".into(),
            cell_class: "shared".into(),
            max_req_per_sec: Some(50),
            monthly_msgs_quota: Some(1_000_000),
            features: json!({"streams": true, "kv": true}),
            created_at_us: now - 100 * DAY,
            ..Default::default()
        };
        let cell = CellDoc {
            id: Uuid::new_v4(),
            slug: "eu1-shared-a".into(),
            region: "eu1".into(),
            base_url: "http://cell:6632".into(),
            class: "shared".into(),
            capacity_slots: 10,
            used_slots: 1,
            broker_version: Some("1.6.0".into()),
            status: "active".into(),
            cell_secret: Some("s3cret".into()),
            created_at_us: now - 100 * DAY,
        };
        let cluster = ClusterDoc {
            id: Uuid::new_v4(),
            tenant_id: tenant.id,
            cell_id: cell.id,
            plan_id: plan.id,
            slug: "acme-prod".into(),
            broker_tenant_uuid: Uuid::new_v4(),
            status: "active".into(),
            limit_overrides: json!({"max_queues": 3, "monthly_msgs_quota": null}),
            created_at_us: now - 10 * DAY,
        };
        let role =
            RoleDoc { user_id: user.id, cluster_id: cluster.id, role: "admin".into(), created_at_us: now - 10 * DAY };
        let key = ApiKeyDoc {
            id: Uuid::new_v4(),
            cluster_id: cluster.id,
            name: "default".into(),
            key_hash: "ab".repeat(32),
            scopes: vec!["produce".into(), "consume".into()],
            created_by: Some(user.id),
            created_at_us: now - 10 * DAY,
            last_used_at_us: Some(now - 60_000_000),
            revoked_at_us: None,
        };
        let live_queue = QueueDoc {
            id: Uuid::new_v4(),
            cluster_id: cluster.id,
            name: "orders".into(),
            partitions_count: 4,
            created_at_us: now - DAY,
            deleted_at_us: None,
        };
        let dead_queue = QueueDoc {
            id: Uuid::new_v4(),
            cluster_id: cluster.id,
            name: "orders".into(),
            partitions_count: 1,
            created_at_us: now - 5 * DAY,
            deleted_at_us: Some(now - 2 * DAY),
        };
        let op_cluster = OperationDoc {
            id: Uuid::new_v4(),
            tenant_id: tenant.id,
            cluster_id: Some(cluster.id),
            actor: "control_plane".into(),
            actor_id: Some(user.id),
            action: "cluster_created".into(),
            target: Some(cluster.id.to_string()),
            meta: json!({"slug": "acme-prod"}),
            at_us: now - 10 * DAY + 7,
        };
        let op_tenant = OperationDoc {
            id: Uuid::new_v4(),
            cluster_id: None,
            action: "tenant_created".into(),
            at_us: now - 10 * DAY,
            ..op_cluster.clone()
        };
        let revoked = RevokedDoc { jti: "jti-live".into(), expires_at_us: now + 3_600_000_000 };
        let outbox = OutboxDoc {
            id: Uuid::new_v4(),
            kind: "tenant_bootstrapped".into(),
            payload: json!({"tenant_slug": "acme"}),
            created_at_us: now - 10 * DAY,
            consumed_at_us: None,
        };
        let minute_us = (now - 3 * DAY).div_euclid(usage::MINUTE_US) * usage::MINUTE_US;
        World {
            now,
            tenant,
            user,
            identity,
            plan,
            cell,
            cluster,
            role,
            key,
            live_queue,
            dead_queue,
            op_cluster,
            op_tenant,
            revoked,
            outbox,
            minute_us,
        }
    }

    fn rows(w: &World) -> Vec<ImportRow> {
        let used = UsageDoc { msgs: 10, reqs: 2, bytes_in: 100, bytes_out: 5 };
        vec![
            ImportRow::Tenant(w.tenant.clone()),
            ImportRow::User(w.user.clone()),
            ImportRow::Identity(w.identity.clone()),
            ImportRow::Plan(w.plan.clone()),
            ImportRow::Cell(w.cell.clone()),
            ImportRow::Cluster(w.cluster.clone()),
            ImportRow::Role(w.role.clone()),
            ImportRow::ApiKey(w.key.clone()),
            ImportRow::Queue(w.live_queue.clone()),
            ImportRow::Queue(w.dead_queue.clone()),
            ImportRow::UsageMinute {
                cluster_id: w.cluster.id,
                minute_us: w.minute_us,
                op_class: "push".into(),
                usage: used.clone(),
            },
            // Far past the retention: cut.
            ImportRow::UsageMinute {
                cluster_id: w.cluster.id,
                minute_us: w.minute_us - 400 * DAY,
                op_class: "push".into(),
                usage: used.clone(),
            },
            ImportRow::UsageDay {
                cluster_id: w.cluster.id,
                day: usage::day_str(usage::day_of(w.minute_us)),
                op_class: "push".into(),
                usage: used.clone(),
            },
            ImportRow::UsageDay {
                cluster_id: w.cluster.id,
                day: "2025-01-01".into(),
                op_class: "read".into(),
                usage: used,
            },
            ImportRow::Operation(w.op_cluster.clone()),
            ImportRow::Operation(w.op_tenant.clone()),
            ImportRow::Revoked(w.revoked.clone()),
            ImportRow::Revoked(RevokedDoc { jti: "jti-dead".into(), expires_at_us: w.now - 1 }),
            ImportRow::Outbox(w.outbox.clone()),
        ]
    }

    fn opts() -> ImportOptions {
        ImportOptions { force: false, usage_keep_days: 90, page_rows: 2 }
    }

    async fn doc<T: DeserializeOwned>(kv: &dyn KvBackend, space: &str, k: &str) -> T {
        kv::get::<T>(kv, space, k).await.unwrap().unwrap_or_else(|| panic!("{space} {k} missing")).value
    }

    async fn snapshot(kv: &MemKv) -> Vec<(String, String, u64)> {
        let mut out = Vec::new();
        for space in ALL_SPACES {
            for (k, d) in kv::scan::<Value>(kv, space, "#").await.unwrap() {
                out.push((space.to_string(), k, d.version));
            }
        }
        out
    }

    const ALL_SPACES: [&str; 29] = [
        ns::TENANTS,
        ns::TENANT_SLUG,
        ns::USERS,
        ns::USER_EMAIL,
        ns::USER_TENANT,
        ns::IDENTITIES,
        ns::IDENTITY_PROVIDER,
        ns::IDENTITY_USER,
        ns::PLANS,
        ns::PLAN_CODE,
        ns::CELLS,
        ns::CELL_SLUG,
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
        ns::REVOKED,
        ns::OUTBOX,
    ];

    #[tokio::test]
    async fn every_row_lands_with_its_index_keys() {
        let kv = MemKv::new();
        let w = world();
        let report = import_from(&mut Fake::new(rows(&w), 2), &kv, DEFAULT_USAGE_NODE, &opts()).await.unwrap();

        let want: BTreeMap<String, u64> = [
            ("plans", 1),
            ("cells", 1),
            ("tenants", 1),
            ("users", 1),
            ("identities", 1),
            ("clusters", 1),
            ("cluster_roles", 1),
            ("api_keys", 1),
            ("queues", 2),
            ("usage_days", 2),
            ("usage_minutes", 1),
            ("operations", 2),
            ("revoked_tokens", 1),
            ("outbox", 1),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        assert_eq!(report.counts, want);
        assert_eq!(report.skipped.get("usage_minutes_expired"), Some(&1));
        assert_eq!(report.skipped.get("revoked_tokens_expired"), Some(&1));
        assert!(!report.already_imported && report.imported_at_us.is_some());

        // Documents, exactly.
        assert_eq!(doc::<TenantDoc>(&kv, ns::TENANTS, &key(w.tenant.id)).await, w.tenant);
        assert_eq!(doc::<UserDoc>(&kv, ns::USERS, &key(w.user.id)).await, w.user);
        assert_eq!(doc::<IdentityDoc>(&kv, ns::IDENTITIES, &key(w.identity.id)).await, w.identity);
        assert_eq!(doc::<PlanDoc>(&kv, ns::PLANS, &key(w.plan.id)).await, w.plan);
        assert_eq!(doc::<CellDoc>(&kv, ns::CELLS, &key(w.cell.id)).await, w.cell);
        assert_eq!(doc::<ClusterDoc>(&kv, ns::CLUSTERS, &key(w.cluster.id)).await, w.cluster);
        assert_eq!(doc::<RoleDoc>(&kv, ns::ROLES, &key2(w.user.id, w.cluster.id)).await, w.role);
        assert_eq!(doc::<ApiKeyDoc>(&kv, ns::KEYS, &key(w.key.id)).await, w.key);
        assert_eq!(doc::<QueueDoc>(&kv, ns::QUEUES, &key(w.live_queue.id)).await, w.live_queue);
        assert_eq!(doc::<QueueDoc>(&kv, ns::QUEUES, &key(w.dead_queue.id)).await, w.dead_queue);
        let tail = |o: &OperationDoc| format!("{}/{}", schema::inverted(o.at_us), o.id);
        assert_eq!(doc::<OperationDoc>(&kv, ns::OPS, &key2(w.tenant.id, tail(&w.op_cluster))).await, w.op_cluster);
        assert_eq!(doc::<OperationDoc>(&kv, ns::OPS, &key2(w.tenant.id, tail(&w.op_tenant))).await, w.op_tenant);
        assert_eq!(doc::<RevokedDoc>(&kv, ns::REVOKED, &key("jti-live")).await, w.revoked);
        assert_eq!(
            doc::<OutboxDoc>(&kv, ns::OUTBOX, &key2(schema::ordered(w.outbox.created_at_us), w.outbox.id)).await,
            w.outbox
        );
        let used = UsageDoc { msgs: 10, reqs: 2, bytes_in: 100, bytes_out: 5 };
        assert_eq!(
            doc::<UsageDoc>(&kv, ns::USAGE_MIN, &usage::minute_key(w.cluster.id, w.minute_us, "push", "imported"))
                .await,
            used
        );
        assert_eq!(
            doc::<UsageDoc>(&kv, ns::USAGE_DAY, &usage::day_key(w.cluster.id, "2025-01-01", "read")).await,
            used
        );

        // Unique indexes → id.
        let id = |u: Uuid| Value::String(u.to_string());
        assert_eq!(doc::<Value>(&kv, ns::TENANT_SLUG, "#acme").await, id(w.tenant.id));
        assert_eq!(doc::<Value>(&kv, ns::USER_EMAIL, "#admin@acme.io").await, id(w.user.id));
        assert_eq!(doc::<Value>(&kv, ns::IDENTITY_PROVIDER, "#github:4242").await, id(w.identity.id));
        assert_eq!(doc::<Value>(&kv, ns::PLAN_CODE, "#pro").await, id(w.plan.id));
        assert_eq!(doc::<Value>(&kv, ns::CELL_SLUG, "#eu1-shared-a").await, id(w.cell.id));
        assert_eq!(doc::<Value>(&kv, ns::CLUSTER_SLUG, "#acme-prod").await, id(w.cluster.id));
        assert_eq!(doc::<Value>(&kv, ns::KEY_HASH, &key(&w.key.key_hash)).await, id(w.key.id));
        assert_eq!(
            doc::<Value>(&kv, ns::QUEUE_NAME, &key2(w.cluster.id, "orders")).await,
            id(w.live_queue.id),
            "the live row owns the name"
        );

        // "Rows of" indexes → "".
        let empty = Value::String(String::new());
        for (space, k) in [
            (ns::USER_TENANT, key2(w.tenant.id, w.user.id)),
            (ns::IDENTITY_USER, key2(w.user.id, w.identity.id)),
            (ns::CLUSTER_TENANT, key2(w.tenant.id, w.cluster.id)),
            (ns::CLUSTER_CELL, key2(w.cell.id, w.cluster.id)),
            (ns::ROLE_CLUSTER, key2(w.cluster.id, w.user.id)),
            (ns::KEY_CLUSTER, key2(w.cluster.id, w.key.id)),
            (ns::OPS_CLUSTER, key2(w.cluster.id, tail(&w.op_cluster))),
        ] {
            assert_eq!(doc::<Value>(&kv, space, &k).await, empty, "{space} {k}");
        }
        assert_eq!(kv.keys(ns::OPS_CLUSTER).len(), 1, "the tenant-level operation has no cluster entry");
        assert_eq!(kv.keys(ns::QUEUE_NAME).len(), 1, "a deleted queue does not hold its name");
        assert_eq!(kv.keys(ns::USAGE_MIN).len(), 1);
        assert_eq!(kv.keys(ns::REVOKED), vec!["#jti-live".to_string()]);

        // And the KV side reads it back through the usage repository.
        let store = super::super::Store::Kv(std::sync::Arc::new(MemKv::new()));
        drop(store);
        let got = usage::kv_usage_by_minute(&kv, w.cluster.id, w.minute_us, Some(w.minute_us + usage::MINUTE_US))
            .await
            .unwrap();
        assert_eq!(got[0].msgs, 10);

        // The run is recorded.
        let meta = doc::<MetaDoc>(&kv, ns::META, META_KEY).await;
        assert_eq!(meta.version, SCHEMA_VERSION);
        assert_eq!(meta.imported_at_us, report.imported_at_us);
        assert_eq!(meta.imported_rows["users"], 1);
        assert_eq!(meta.imported_rows["_usage_node"], "imported");
    }

    #[tokio::test]
    async fn a_second_run_is_a_no_op() {
        let kv = MemKv::new();
        let w = world();
        let first = import_from(&mut Fake::new(rows(&w), 3), &kv, "imported", &opts()).await.unwrap();
        let before = snapshot(&kv).await;
        let second = import_from(&mut Fake::new(rows(&w), 3), &kv, "imported", &opts()).await.unwrap();
        assert!(second.already_imported);
        assert_eq!(second.counts, first.counts);
        assert_eq!(second.skipped, first.skipped);
        assert_eq!(second.imported_at_us, first.imported_at_us);
        assert_eq!(snapshot(&kv).await, before, "not one key rewritten");
    }

    #[tokio::test]
    async fn foreign_data_is_refused_unless_forced() {
        let kv = MemKv::new();
        let w = world();
        let local = TenantDoc {
            id: Uuid::new_v4(),
            slug: "local".into(),
            name: "Local".into(),
            status: "active".into(),
            created_at_us: 0,
        };
        kv::write(&kv, vec![put(ns::TENANTS, &key(local.id), &local)]).await.unwrap();
        let err = import_from(&mut Fake::new(rows(&w), 5), &kv, "imported", &opts()).await.unwrap_err();
        assert!(err.contains(ns::TENANTS), "{err}");
        assert!(kv.keys(ns::META).is_empty(), "a refusal writes nothing");

        let forced = ImportOptions { force: true, ..opts() };
        let report = import_from(&mut Fake::new(rows(&w), 5), &kv, "imported", &forced).await.unwrap();
        assert!(report.forced);
        assert_eq!(kv.keys(ns::TENANTS).len(), 2, "imported over, by id");
    }

    #[tokio::test]
    async fn an_interrupted_import_resumes_without_force() {
        let kv = MemKv::new();
        let w = world();
        let mut src = Fake::new(rows(&w), 2);
        src.fail_on = Some(Table::Queues);
        let err = import_from(&mut src, &kv, "imported", &opts()).await.unwrap_err();
        assert!(err.contains("went away"));
        let meta = doc::<MetaDoc>(&kv, ns::META, META_KEY).await;
        assert_eq!((meta.imported_at_us, &meta.imported_rows[IN_PROGRESS]), (None, &Value::Bool(true)));
        assert!(!kv.keys(ns::TENANTS).is_empty(), "the tables before it are in");

        // Tenants are there now, but they are the import's own: no force.
        let report = import_from(&mut Fake::new(rows(&w), 2), &kv, "imported", &opts()).await.unwrap();
        assert_eq!(report.counts["queues"], 2);
        assert_eq!(report.counts["tenants"], 1);
        assert!(doc::<MetaDoc>(&kv, ns::META, META_KEY).await.imported_at_us.is_some());
    }

    #[tokio::test]
    async fn a_seeded_catalog_row_gives_way_to_the_imported_one() {
        let kv = MemKv::new();
        let w = world();
        let seeded =
            PlanDoc { id: Uuid::new_v4(), code: "pro".into(), cell_class: "shared".into(), ..Default::default() };
        kv::write(&kv, vec![put(ns::PLANS, &key(seeded.id), &seeded), put(ns::PLAN_CODE, "#pro", &seeded.id)])
            .await
            .unwrap();
        // A meta marker a fresh broker might have written: not an import.
        let fresh = MetaDoc { version: SCHEMA_VERSION, imported_at_us: None, imported_rows: Value::Null };
        kv::write(&kv, vec![put(ns::META, META_KEY, &fresh)]).await.unwrap();

        import_from(&mut Fake::new(rows(&w), 4), &kv, "imported", &opts()).await.unwrap();
        assert_eq!(doc::<Value>(&kv, ns::PLAN_CODE, "#pro").await, Value::String(w.plan.id.to_string()));
        assert_eq!(kv.keys(ns::PLANS), vec![key(w.plan.id)], "the seeded twin is gone");
    }

    /// Refuses (400) any batch that writes `bad`.
    struct Picky {
        inner: MemKv,
        bad: String,
    }
    impl KvBackend for Picky {
        fn kv(&self, ops: Vec<Value>) -> BoxFut<'_, Result<Vec<Value>, KvError>> {
            Box::pin(async move {
                if ops.iter().any(|o| o.get("key").and_then(Value::as_str) == Some(self.bad.as_str())) {
                    return Err(KvError::Invalid {
                        status: 413,
                        reason: "kv_value_too_large".into(),
                        detail: "too big".into(),
                    });
                }
                self.inner.kv(ops).await
            })
        }
    }

    #[tokio::test]
    async fn a_row_the_broker_refuses_is_skipped_not_fatal() {
        let w = world();
        let kv = Picky {
            inner: MemKv::new(),
            bad: key2(w.tenant.id, format!("{}/{}", schema::inverted(w.op_tenant.at_us), w.op_tenant.id)),
        };
        let report = import_from(&mut Fake::new(rows(&w), 10), &kv, "imported", &opts()).await.unwrap();
        assert_eq!(report.counts["operations"], 1);
        assert_eq!(report.skipped.get("operations_refused"), Some(&1));
        assert_eq!(kv.inner.keys(ns::OPS).len(), 1, "its batch-mates still landed");
        assert_eq!(report.counts["tenants"], 1);
    }

    #[test]
    fn batches_never_split_a_row_or_write_a_key_twice() {
        // 64 ops per batch at most, a row's ops together: a cluster row is 4.
        let w = world();
        let ops = row_ops(&ImportRow::Cluster(w.cluster.clone()), "imported", 90, w.now).unwrap();
        assert_eq!(ops.len(), 4);
        let ns_keys: HashSet<(String, String)> = ops
            .iter()
            .map(|o| (o["ns"].as_str().unwrap().to_string(), o["key"].as_str().unwrap().to_string()))
            .collect();
        assert_eq!(ns_keys.len(), 4);
        // Minute rows carry their TTL, revoked tokens theirs.
        let m = row_ops(
            &ImportRow::UsageMinute {
                cluster_id: w.cluster.id,
                minute_us: w.minute_us,
                op_class: "push".into(),
                usage: UsageDoc::default(),
            },
            "imported",
            90,
            w.now,
        )
        .unwrap();
        assert!(m[0]["ttlSeconds"].as_u64().unwrap() > 86 * 86_400);
        let r = row_ops(&ImportRow::Revoked(w.revoked.clone()), "imported", 90, w.now).unwrap();
        assert_eq!(r[0]["ttlSeconds"], 3600);
    }

    // ---------------------------------------------------------------- live
    //
    // The import against a real Postgres: the SQL (cursors, casts, the exact
    // microsecond epoch), not just the layout. Needs a THROWAWAY postgres —
    // it drops and re-applies the `queen_proxy` schema:
    //
    //   QUEEN_PROXY_TEST_PG=/path/to/socket/dir:5432 \
    //     cargo test --lib store::import::tests::live -- --ignored
    //
    // (`host:port`; a host starting with `/` is a unix-socket directory.)
    #[tokio::test]
    #[ignore = "requires a THROWAWAY postgres (QUEEN_PROXY_TEST_PG) -- it drops the queen_proxy schema"]
    async fn live_import_from_a_real_postgres() {
        let target = std::env::var("QUEEN_PROXY_TEST_PG").unwrap_or_else(|_| "127.0.0.1:5489".to_string());
        let (host, port) = target
            .rsplit_once(':')
            .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
            .unwrap_or((target.clone(), 5432));
        let pxcfg = crate::config::PxdbConfig {
            host,
            port,
            user: std::env::var("QUEEN_PROXY_TEST_PG_USER").unwrap_or_else(|_| "postgres".to_string()),
            password: "postgres".to_string(),
            dbname: "postgres".to_string(),
            use_ssl: false,
            ssl_reject_unauthorized: false,
            ssl_root_cert: None,
            pool_size: 2,
            timeout_ms: 5_000,
        };
        let pool = crate::db::create_pool(&pxcfg).await.expect("connect to the throwaway postgres");
        pool.get()
            .await
            .unwrap()
            .batch_execute("DROP SCHEMA IF EXISTS queen_proxy CASCADE")
            .await
            .expect("clean slate");
        crate::db::apply_migrations(&pool).await.expect("apply migrations");
        let c = pool.get().await.unwrap();

        let cell: String = c
            .query_one(
                "INSERT INTO queen_proxy.cells(slug, region, base_url, class, cell_secret)
                 VALUES ('eu1-shared-a', 'eu1', 'http://cell:6632', 'shared', 's3cret') RETURNING id::text",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        let boot: String = c
            .query_one(
                "SELECT queen_proxy.bootstrap_tenant('acme', 'Acme', 'acme-prod', 'pro', $1::text::uuid,
                                                     'admin@acme.io', 'pw')::text",
                &[&cell],
            )
            .await
            .unwrap()
            .get(0);
        let boot: Value = serde_json::from_str(&boot).unwrap();
        let (tenant, cluster, user) = (
            boot["tenant_id"].as_str().unwrap(),
            boot["cluster_id"].as_str().unwrap(),
            boot["user_id"].as_str().unwrap(),
        );
        c.batch_execute(&format!(
            "UPDATE queen_proxy.users SET name = 'Ada', is_operator = true,
                    last_login_at = '2026-09-01 10:11:12.345678+00' WHERE id = '{user}';
             INSERT INTO queen_proxy.identities(user_id, provider, provider_id, email, verified)
                    VALUES ('{user}', 'github', '4242', 'admin@acme.io', true);
             INSERT INTO queen_proxy.queues(cluster_id, name, partitions_count) VALUES ('{cluster}', 'orders', 4);
             INSERT INTO queen_proxy.queues(cluster_id, name, partitions_count, deleted_at)
                    VALUES ('{cluster}', 'orders', 1, now() - interval '1 day');
             INSERT INTO queen_proxy.usage_minutes(cluster_id, minute, op_class, msgs, reqs, bytes_in, bytes_out)
                    VALUES ('{cluster}', date_trunc('minute', now()) - interval '2 days', 'push', 10, 2, 100, 5),
                           ('{cluster}', date_trunc('minute', now()) - interval '400 days', 'push', 1, 1, 1, 1);
             INSERT INTO queen_proxy.usage_days(cluster_id, day, op_class, msgs, reqs, bytes_in, bytes_out)
                    VALUES ('{cluster}', '2025-01-01', 'read', 7, 7, 7, 7);
             INSERT INTO queen_proxy.revoked_tokens(jti, expires_at) VALUES ('jti-live', now() + interval '1 hour'),
                                                                            ('jti-dead', now() - interval '1 hour');
             SELECT queen_proxy.set_limit_override('{cluster}', '{{\"max_queues\": 3}}'::jsonb);"
        ))
        .await
        .expect("seed");
        let count = |t: &'static str| {
            let c = &c;
            async move {
                let n: i64 = c.query_one(&format!("SELECT count(*) FROM queen_proxy.{t}"), &[]).await.unwrap().get(0);
                n as u64
            }
        };

        let kv = MemKv::new();
        let report = import_from_pg_with(&pool, &kv, "imported", &ImportOptions { page_rows: 3, ..opts() })
            .await
            .expect("import");
        for t in Table::ALL {
            let pg = count(t.name()).await;
            let skipped = report.skipped.iter().filter(|(k, _)| k.starts_with(t.name())).map(|(_, v)| *v).sum::<u64>();
            assert_eq!(report.counts[t.name()] + skipped, pg, "{}: {report:?}", t.name());
        }
        assert_eq!(report.skipped.get("usage_minutes_expired"), Some(&1));
        assert_eq!(report.skipped.get("revoked_tokens_expired"), Some(&1));
        assert_eq!(report.counts["plans"], 4, "002's seeded catalog comes along");

        // Spot checks against Postgres' own rendering of the same rows.
        let t: TenantDoc = doc(&kv, ns::TENANTS, &format!("#{tenant}")).await;
        assert_eq!(t.slug, "acme");
        let u: UserDoc = doc(&kv, ns::USERS, &format!("#{user}")).await;
        let pg_login: i64 = c
            .query_one(&format!("SELECT {} FROM queen_proxy.users WHERE id = '{user}'", us("last_login_at")), &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(u.last_login_at_us, Some(pg_login));
        assert_eq!(pg_login % 1_000_000, 345_678, "microseconds survive");
        assert_eq!((u.name.as_deref(), u.is_operator), (Some("Ada"), true));
        let cl: ClusterDoc = doc(&kv, ns::CLUSTERS, &format!("#{cluster}")).await;
        assert_eq!(cl.limit_overrides, json!({"max_queues": 3}));
        let plan: PlanDoc = doc(&kv, ns::PLANS, &format!("#{}", cl.plan_id)).await;
        assert_eq!((plan.code.as_str(), plan.max_req_per_sec), ("pro", Some(50)));
        assert_eq!(plan.features["kv"], true, "009's families");
        let key_row: (String, String) = {
            let r = c.query_one("SELECT id::text, key_hash FROM queen_proxy.api_keys", &[]).await.unwrap();
            (r.get(0), r.get(1))
        };
        assert_eq!(doc::<Value>(&kv, ns::KEY_HASH, &format!("#{}", key_row.1)).await, Value::String(key_row.0.clone()));
        let k: ApiKeyDoc = doc(&kv, ns::KEYS, &format!("#{}", key_row.0)).await;
        assert_eq!(k.scopes, vec!["produce", "consume", "admin", "read"]);
        assert_eq!(kv.keys(ns::QUEUE_NAME).len(), 1);
        assert!(!kv.keys(ns::OUTBOX).is_empty(), "bootstrap_tenant's signup event");
        assert!(kv.keys(ns::OPS).len() >= 4, "the audit trail");
        let minute = usage::kv_usage_by_minute(&kv, Uuid::parse_str(cluster).unwrap(), 0, None).await.unwrap();
        assert_eq!(minute.len(), 1);
        assert_eq!((minute[0].msgs, minute[0].bytes_out), (10, 5));

        // Idempotent against the real thing too.
        let again = import_from_pg(&pool, &kv, "imported").await.unwrap();
        assert!(again.already_imported);
        assert_eq!(again.counts, report.counts);

        // The PG repository reads of store/usage.rs, on the same database.
        let store = super::super::Store::Pg(pool.clone());
        let cid = Uuid::parse_str(cluster).unwrap();
        let recent = usage::cluster_usage_recent(&store, cid, 72).await.unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].minute, usage::iso_minute(recent[0].minute_us));
        assert_eq!(usage::cluster_usage_by_minute(&store, cid, 0, None).await.unwrap().len(), 2);
        let days =
            usage::cluster_usage_by_day(&store, cid, "2025-01-01", &usage::day_str(usage::day_of(usage::now_us())))
                .await
                .unwrap();
        assert_eq!(days.first().map(|d| (d.day.as_str(), d.msgs)), Some(("2025-01-01", 7)));
        assert!(days.iter().any(|d| d.op_class == "push" && d.msgs == 10), "live minutes of an unrolled day: {days:?}");
        let month = usage::cluster_month_msgs(&store, cid).await.unwrap();
        assert_eq!(month.month.len(), 7);
        assert_eq!(usage::rollup_days(&store, 90, &[]).await.unwrap(), 2, "two closed days with minutes");
        assert_eq!(usage::prune_minutes(&store, 90).await.unwrap(), 1, "the 400-day-old minute, now rolled");
        assert!(usage::quota_rows(&store).await.unwrap().is_empty(), "pro has no monthly quota");
        usage::emit_outbox(
            &store,
            "cluster_monthly_quota_warning",
            &json!({"cluster_id": cluster, "month": "2026-09"}),
        )
        .await
        .unwrap();
        assert!(usage::quota_event_seen(&store, "cluster_monthly_quota_warning", cluster, "2026-09").await.unwrap());
        // The standalone flush: ADDITIVE, twice the same key in one pass included.
        let row = crate::meter::UsageRow {
            cluster_id: cid,
            minute: 29_000_000,
            op: "txn".into(),
            reqs: 1,
            msgs: 2,
            bytes_in: 3,
            bytes_out: 4,
        };
        usage::pg_add_minutes(&pool, &[row.clone(), row]).await.unwrap();
        let m = usage::cluster_usage_by_minute(
            &store,
            cid,
            29_000_000 * usage::MINUTE_US,
            Some(29_000_001 * usage::MINUTE_US),
        )
        .await
        .unwrap();
        assert_eq!((m.len(), m[0].msgs, m[0].bytes_out), (1, 4, 8));
    }
}
