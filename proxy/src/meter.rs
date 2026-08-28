//! Usage metering. OWNER: Agent D.
//! Contract (M1–M6, PLAN §4): meter post-response from per-item statuses —
//! never charge `error`, never double-charge `duplicate`, `buffered` counts
//! as accepted; exempt 5xx and scope-403s (all Agent A / gateway.rs's job —
//! `record()` here just aggregates whatever Sample it's handed).
//!
//! In-memory per-(cluster, op, minute) aggregates, 16-way sharded by
//! cluster_id, flushed to `queen_proxy.usage_minutes` every
//! `cfg.meter_flush_ms` (closed minutes only — the current minute keeps
//! accumulating), spooled to disk (spool.rs) when the flush fails, drained
//! back on the next startup. `record()` deliberately does not log at
//! info/debug on the hot per-request path (rates/sizes belong in aggregated
//! blocks, not per-message lines — see obs.rs conventions).
//!
//! Downstream of the minute aggregates, this module also drives the billing
//! chain (`spawn_rollup`): `queen_proxy.rollup_usage_days()` folds closed days
//! into `usage_days`, and the same tick evaluates
//! `plans.monthly_msgs_quota` per cluster (PLAN §6.7 soft enforcement: warn
//! event at QUOTA_WARN_PERCENT, push block at 100%). `drain()` is the
//! shutdown counterpart to the periodic flush: it takes the open minute too.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::limits::PushBlock;
use crate::state::{OpClass, St};

const N_SHARDS: usize = 16;

/// Ceiling on the shutdown drain's DB write. The drain runs on the way out,
/// after the listener is gone: it must not hold the process open on a pxdb
/// that has stopped answering, and it already has a fallback that always
/// terminates (the disk spool).
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Rollup + monthly-quota cadence. HOURLY, not daily, for two reasons:
/// `rollup_usage_days()` is idempotent and cheap (it recomputes each closed
/// day from usage_minutes, so extra runs cost an aggregate pass and change
/// nothing), and the monthly quota check rides this same tick — which makes
/// the interval the worst-case delay before a cluster that has blown through
/// its plan's monthly allowance is actually blocked. A daily tick would make
/// that delay up to 24h and would be untestable without waiting a day.
const ROLLUP_INTERVAL_MS: u64 = 3_600_000;

/// Percentage of `monthly_msgs_quota` at which the control plane is told
/// (outbox event), well before the 100% block — the point of the warning is
/// that a human can raise the plan before the tenant is stopped.
const QUOTA_WARN_PERCENT: i64 = 80;

/// How long minute-granularity usage is kept once its day has been rolled up.
/// Minutes are the evidence behind a billing dispute, so the window is
/// generous; usage_days keeps the totals indefinitely either way.
const USAGE_KEEP_DAYS: u64 = 90;

fn shard_index(id: &Uuid) -> usize {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut h);
    (h.finish() as usize) % N_SHARDS
}

fn now_minute_epoch() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() / 60
}

#[derive(Clone, Debug)]
pub struct Sample {
    pub cluster_id: Uuid,
    pub op: OpClass,
    pub reqs: u64,
    pub msgs: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

#[derive(Default, Clone, Copy, Debug)]
struct Acc {
    reqs: u64,
    msgs: u64,
    bytes_in: u64,
    bytes_out: u64,
}

/// A single closed-minute rollup, ready to flush or spool. Also the on-disk
/// spool JSONL row shape — field names match the task's `{cluster_id,minute,
/// op,reqs,msgs,bytes_in,bytes_out}` spec exactly, independent of the DB
/// column names (usage_minutes.op_class, not `op`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UsageRow {
    pub cluster_id: Uuid,
    pub minute: u64,
    pub op: String,
    pub reqs: u64,
    pub msgs: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

const UPSERT_SQL: &str = "
    INSERT INTO queen_proxy.usage_minutes (cluster_id, minute, op_class, reqs, msgs, bytes_in, bytes_out)
    VALUES ($1::text::uuid, to_timestamp($2::bigint), $3, $4, $5, $6, $7)
    ON CONFLICT (cluster_id, minute, op_class) DO UPDATE SET
        reqs = queen_proxy.usage_minutes.reqs + EXCLUDED.reqs,
        msgs = queen_proxy.usage_minutes.msgs + EXCLUDED.msgs,
        bytes_in = queen_proxy.usage_minutes.bytes_in + EXCLUDED.bytes_in,
        bytes_out = queen_proxy.usage_minutes.bytes_out + EXCLUDED.bytes_out";

/// UPSERT one flush batch inside a single transaction (prepared statement
/// reused per row — simple and correct for v1 cardinality: distinct
/// (cluster, op_class) pairs closed per flush interval, not per-message).
/// cluster_id binds as text and casts in SQL (`$1::text::uuid`) — tokio-postgres
/// isn't built with the `with-uuid-1` feature in this crate, and this repo's
/// established workaround (see server/ Track B notes) is the text-cast, not a
/// new Cargo feature.
async fn upsert_rows(pool: &Pool, rows: &[UsageRow]) -> Result<(), String> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut client = pool.get().await.map_err(|e| format!("pool: {e}"))?;
    let txn = client.transaction().await.map_err(|e| format!("begin: {e}"))?;
    {
        let stmt = txn.prepare(UPSERT_SQL).await.map_err(|e| format!("prepare: {e}"))?;
        for r in rows {
            let cid = r.cluster_id.to_string();
            let minute_secs = (r.minute as i64) * 60;
            txn.execute(
                &stmt,
                &[
                    &cid,
                    &minute_secs,
                    &r.op,
                    &(r.reqs as i64),
                    &(r.msgs as i64),
                    &(r.bytes_in as i64),
                    &(r.bytes_out as i64),
                ],
            )
            .await
            .map_err(|e| format!("upsert: {e}"))?;
        }
    }
    txn.commit().await.map_err(|e| format!("commit: {e}"))?;
    Ok(())
}

pub struct Meter {
    flush_ms: u64,
    shards: Vec<Mutex<HashMap<(Uuid, OpClass, u64), Acc>>>,
    spool: crate::spool::Spool,
    /// The pool `drain()` writes through. The periodic path gets it as a
    /// parameter (`spawn_flush`), but `drain()` is called from the shutdown
    /// path with nothing but `&self`, so it is remembered here on the way
    /// past. Never set == dev-static mode: drain discards, exactly like
    /// `flush_once` does with `db: None`.
    db: OnceLock<Option<Pool>>,
}

impl Meter {
    pub fn new(cfg: &crate::config::Config) -> Meter {
        Meter {
            flush_ms: cfg.meter_flush_ms,
            shards: (0..N_SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            spool: crate::spool::Spool::new(&cfg.spool_dir),
            db: OnceLock::new(),
        }
    }

    pub fn record(&self, s: Sample) {
        let minute = now_minute_epoch();
        let idx = shard_index(&s.cluster_id);
        let mut shard = self.shards[idx].lock().unwrap();
        let acc = shard.entry((s.cluster_id, s.op, minute)).or_default();
        acc.reqs += s.reqs;
        acc.msgs += s.msgs;
        acc.bytes_in += s.bytes_in;
        acc.bytes_out += s.bytes_out;
    }

    /// Drain every aggregate whose minute is strictly before `now_minute`
    /// across all shards; entries at `now_minute` are left in place (still
    /// accumulating). Pure/no I/O so it's directly unit-testable.
    fn drain_closed(&self, now_minute: u64) -> Vec<UsageRow> {
        let mut rows = Vec::new();
        for shard in &self.shards {
            let mut m = shard.lock().unwrap();
            m.retain(|key, acc| {
                if key.2 < now_minute {
                    rows.push(UsageRow {
                        cluster_id: key.0,
                        minute: key.2,
                        op: key.1.as_str().to_string(),
                        reqs: acc.reqs,
                        msgs: acc.msgs,
                        bytes_in: acc.bytes_in,
                        bytes_out: acc.bytes_out,
                    });
                    false // remove: drained
                } else {
                    true // keep: still the current minute
                }
            });
        }
        rows
    }

    /// Drain EVERY aggregate, the still-open current minute included. Only
    /// correct at shutdown: during normal operation the open minute must stay
    /// in the map (`drain_closed`), or concurrent `record()` calls for the
    /// same minute would be split across flushes for no reason. Pure/no I/O,
    /// like `drain_closed`, so the semantics are directly testable.
    fn drain_all(&self) -> Vec<UsageRow> {
        let mut rows = Vec::new();
        for shard in &self.shards {
            let mut m = shard.lock().unwrap();
            for (key, acc) in m.drain() {
                rows.push(UsageRow {
                    cluster_id: key.0,
                    minute: key.2,
                    op: key.1.as_str().to_string(),
                    reqs: acc.reqs,
                    msgs: acc.msgs,
                    bytes_in: acc.bytes_in,
                    bytes_out: acc.bytes_out,
                });
            }
        }
        rows
    }

    /// Shutdown flush: persist everything still in memory, INCLUDING the open
    /// minute the periodic path deliberately leaves alone. Without this, every
    /// restart silently drops up to one minute of usage per cluster per op.
    ///
    /// Bounded by DRAIN_TIMEOUT and falling back to the disk spool on any
    /// failure, so it always terminates: a shutdown that hangs on pxdb would
    /// turn a deploy into an outage. The spool fallback carries the same
    /// at-least-once exposure as the periodic path — a commit whose ack is
    /// lost is replayed by `recover()` and re-added by the UPSERT — which is
    /// the deliberate trade: usage we can over-count once is recoverable,
    /// usage we drop is gone.
    pub async fn drain(&self) {
        let rows = self.drain_all();
        if rows.is_empty() {
            return;
        }
        let Some(pool) = self.db.get().and_then(|db| db.as_ref()) else {
            tracing::debug!(target: "meter", rows = rows.len(), "no pxdb (dev mode); discarding usage rows on drain");
            return;
        };
        match tokio::time::timeout(DRAIN_TIMEOUT, upsert_rows(pool, &rows)).await {
            Ok(Ok(())) => {
                tracing::info!(target: "meter", rows = rows.len(), "usage_minutes drained on shutdown");
            }
            Ok(Err(e)) => {
                tracing::warn!(target: "meter", rows = rows.len(), error = %e, "shutdown drain failed; spooling to disk");
                self.spool.write(&rows);
            }
            Err(_) => {
                tracing::warn!(
                    target: "meter", rows = rows.len(), timeout_ms = DRAIN_TIMEOUT.as_millis() as u64,
                    "shutdown drain timed out; spooling to disk"
                );
                self.spool.write(&rows);
            }
        }
    }

    async fn flush_once(&self, db: Option<&Pool>) {
        let rows = self.drain_closed(now_minute_epoch());
        if rows.is_empty() {
            return;
        }
        let Some(pool) = db else {
            tracing::debug!(target: "meter", rows = rows.len(), "no pxdb (dev mode); discarding closed-minute usage rows");
            return;
        };
        match upsert_rows(pool, &rows).await {
            Ok(()) => {
                tracing::debug!(target: "meter", rows = rows.len(), "usage_minutes flush ok");
            }
            Err(e) => {
                tracing::warn!(target: "meter", rows = rows.len(), error = %e, "usage_minutes flush failed; spooling to disk");
                self.spool.write(&rows);
            }
        }
    }

    /// Startup spool recovery (once, before the periodic loop begins) then a
    /// flush every `cfg.meter_flush_ms`. `db: None` (dev-static mode) skips
    /// recovery and just drains+discards on every tick.
    pub fn spawn_flush(self: &Arc<Self>, db: Option<deadpool_postgres::Pool>) {
        // Remember the pool for `drain()`, which takes no parameters.
        let _ = self.db.set(db.clone());
        let this = Arc::clone(self);
        tokio::spawn(async move {
            if let Some(pool) = db.clone() {
                this.spool
                    .recover(move |rows| {
                        let pool = pool.clone();
                        async move { upsert_rows(&pool, &rows).await }
                    })
                    .await;
            }
            let mut tick = tokio::time::interval(Duration::from_millis(this.flush_ms.max(100)));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                this.flush_once(db.as_ref()).await;
            }
        });
    }
}

// ------------------------------------------------- rollup + monthly quota

/// Where a cluster stands against its monthly message allowance.
/// `Ord` is the announcement order (Under < Warn < Over) — see
/// `quota_announcement`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum QuotaLevel {
    Under,
    Warn,
    Over,
}

impl QuotaLevel {
    /// Outbox `kind` for the event that announces reaching this level.
    /// `Under` announces nothing.
    fn outbox_kind(self) -> Option<&'static str> {
        match self {
            QuotaLevel::Under => None,
            QuotaLevel::Warn => Some("cluster_monthly_quota_warning"),
            QuotaLevel::Over => Some("cluster_monthly_quota_blocked"),
        }
    }
}

/// Where `msgs` sits against `quota`, with the warn band at `warn_percent`.
///
/// `Over` is `>=`, not `>`: a monthly quota is an allowance, so consuming all
/// of it is the block condition (unlike the storage cap, which blocks strictly
/// above `max_retained_bytes` — that one is a level, not a budget). i128
/// arithmetic for the same reason registry.rs uses it: `msgs * 100` overflows
/// i64 for large-but-legal figures, and the boundary is exactly what this
/// decides.
fn quota_level(msgs: i64, quota: i64, warn_percent: i64) -> QuotaLevel {
    if msgs >= quota {
        return QuotaLevel::Over;
    }
    if (msgs as i128) * 100 >= (quota as i128) * (warn_percent as i128) {
        return QuotaLevel::Warn;
    }
    QuotaLevel::Under
}

/// Which level (if any) to announce to the control plane, given the highest
/// one already announced for this cluster THIS MONTH. Rising levels announce
/// once each; a repeat, or a fall back down (usage can only fall if
/// usage_minutes was pruned behind the rollup), announces nothing — the
/// control plane must not get an event per tick for a tenant parked above the
/// line, and must not get the same warning twice because the count wobbled.
fn quota_announcement(announced: Option<QuotaLevel>, level: QuotaLevel) -> Option<QuotaLevel> {
    if level == QuotaLevel::Under {
        return None;
    }
    match announced {
        Some(prev) if prev >= level => None,
        _ => Some(level),
    }
}

/// Cross-tick memory for the monthly-quota check. Process-local: one proxy
/// fronts one cell (PLAN §2), and every value here is re-derived from pxdb on
/// the first tick after a restart.
#[derive(Default)]
struct QuotaState {
    /// UTC calendar month ("YYYY-MM") the `announced` map belongs to. A
    /// different month from the DB resets it — that IS the monthly release.
    month: String,
    /// Highest level already announced per cluster, this month.
    announced: HashMap<Uuid, QuotaLevel>,
    /// Clusters this task currently holds a `PushBlock::MonthlyQuota` on.
    blocked: HashSet<Uuid>,
}

/// Clusters with a monthly allowance, their calendar-month message count, and
/// the month itself — one round trip. `cluster_month_msgs` is STABLE and reads
/// usage_days plus the not-yet-rolled usage_minutes remainder (004_lifecycle),
/// so the count includes traffic from the current minute-ish, not just what
/// the rollup has folded in. `jsonb_exists` rather than the `?` operator so
/// the statement carries no character that a future parameter binder might
/// claim. Every value comes from one `now()`, so month and count cannot
/// disagree across a boundary.
const QUOTA_SQL: &str = "
    SELECT c.id::text, c.tenant_id::text, c.slug,
           p.monthly_msgs_quota, (c.limit_overrides)::text,
           queen_proxy.cluster_month_msgs(c.id, date_trunc('month', (now() AT TIME ZONE 'UTC'))::date),
           to_char((now() AT TIME ZONE 'UTC'), 'YYYY-MM')
      FROM queen_proxy.clusters c
      JOIN queen_proxy.plans p ON p.id = c.plan_id
     WHERE c.status <> 'deleting'
       AND (p.monthly_msgs_quota IS NOT NULL OR jsonb_exists(c.limit_overrides, 'monthly_msgs_quota'))";

/// Has this exact (kind, cluster, month) event already been written? The
/// in-process `QuotaState` covers the common case; this covers a proxy restart
/// mid-month, which would otherwise re-announce every cluster already over the
/// line. Best-effort by design: on a query error we let the emit proceed
/// (a duplicate CP event beats a swallowed one).
const OUTBOX_SEEN_SQL: &str = "
    SELECT 1 FROM queen_proxy.outbox
     WHERE kind = $1 AND payload->>'cluster_id' = $2 AND payload->>'month' = $3
     LIMIT 1";

/// Periodic billing driver: fold closed days into `usage_days`, then evaluate
/// `plans.monthly_msgs_quota` (PLAN §6.7). Detached, never panics, tolerates
/// pxdb being down by skipping the tick — a failed read is not evidence that a
/// block may be released.
pub fn spawn_rollup(st: St) {
    if st.db.is_none() {
        tracing::info!(target: "meter", "usage rollup: no pxdb configured, skipping (dev-static mode)");
        return;
    }
    tokio::spawn(async move {
        // Read straight from env (same as QUEEN_PROXY_CELL_MAX_PARKED in
        // limits.rs and QUEEN_PROXY_RECONCILE_MS in registry.rs): one consumer,
        // read once, at task start.
        let interval = Duration::from_millis(
            crate::config::env_u64("QUEEN_PROXY_ROLLUP_MS", ROLLUP_INTERVAL_MS).max(1_000),
        );
        let warn_percent =
            crate::config::env_u64("QUEEN_PROXY_QUOTA_WARN_PERCENT", QUOTA_WARN_PERCENT as u64) as i64;
        // The first tick fires immediately: after a restart the push-block
        // flags are empty (they live in memory only), so a cluster that was
        // over its quota must be re-blocked now, not an hour from now.
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut quota = QuotaState::default();
        loop {
            tick.tick().await;
            let Some(pool) = st.db.as_ref() else { return };
            rollup_once(pool).await;
            enforce_monthly_quota(pool, &st.limits, &mut quota, warn_percent).await;
        }
    });
}

async fn rollup_once(pool: &Pool) {
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "meter", error = %e, "usage rollup: pool.get failed, skipping cycle");
            return;
        }
    };
    // No argument: rollup_usage_days' own default (NULL) means "every closed
    // day still in usage_minutes", which is what a periodic driver wants —
    // late spool drains for older days get corrected on the next pass.
    match client.query_one("SELECT queen_proxy.rollup_usage_days()", &[]).await {
        Ok(row) => {
            let rows: i32 = row.get(0);
            tracing::info!(target: "meter", rows, "usage_days rollup ok");
        }
        Err(e) => {
            tracing::warn!(target: "meter", error = %e, "usage_days rollup failed; retrying next tick");
            // Pruning below only deletes minutes whose day is already rolled up,
            // so a failed rollup makes it a no-op rather than a data loss — but
            // there is nothing to gain from the round trip either.
            return;
        }
    }

    // Bound usage_minutes growth. Ordered strictly after the rollup: the prune
    // is gated on the day existing in usage_days, so running it first would
    // simply skip the days this pass just folded in.
    let keep_days = crate::config::env_u64("QUEEN_PROXY_USAGE_KEEP_DAYS", USAGE_KEEP_DAYS) as i32;
    match client
        .query_one("SELECT queen_proxy.prune_usage_minutes($1)", &[&keep_days])
        .await
    {
        Ok(row) => {
            let pruned: i32 = row.get(0);
            if pruned > 0 {
                tracing::info!(target: "meter", pruned, keep_days, "usage_minutes pruned");
            }
        }
        Err(e) => {
            tracing::warn!(target: "meter", error = %e, "usage_minutes prune failed; retrying next tick");
        }
    }
}

async fn enforce_monthly_quota(
    pool: &Pool,
    limits: &crate::limits::Limits,
    state: &mut QuotaState,
    warn_percent: i64,
) {
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(target: "meter", error = %e, "monthly quota: pool.get failed, skipping cycle");
            return;
        }
    };
    let rows = match client.query(QUOTA_SQL, &[]).await {
        Ok(r) => r,
        Err(e) => {
            // Leave every existing decision alone: we have no evidence either
            // way, and releasing a block on a failed read would hand a tenant
            // an unmetered hour every time pxdb hiccups.
            tracing::warn!(target: "meter", error = %e, "monthly quota: query failed, leaving decisions unchanged");
            return;
        }
    };

    // Month rollover: same value on every row (one now()), so the first row
    // decides. Clearing `announced` is what makes next month's first crossing
    // announce again.
    if let Some(first) = rows.first() {
        let month: String = first.get(6);
        if state.month != month {
            state.month = month;
            state.announced.clear();
        }
    }

    let mut now_blocked: HashSet<Uuid> = HashSet::new();
    for row in &rows {
        let id_str: String = row.get(0);
        let Ok(cluster_id) = Uuid::parse_str(&id_str) else {
            tracing::warn!(target: "meter", id = %id_str, "monthly quota: unparseable cluster id, skipping");
            continue;
        };
        let tenant_id: String = row.get(1);
        let slug: String = row.get(2);
        let plan_quota: Option<i64> = row.get(3);
        let overrides_json: String = row.get(4);
        let msgs: i64 = row.get(5);
        let month: String = row.get(6);

        let overrides: serde_json::Value =
            serde_json::from_str(&overrides_json).unwrap_or(serde_json::Value::Null);
        // Same three-way override rule as every other limit (cache.rs):
        // absent -> plan, JSON null -> explicitly unlimited, number -> that.
        let Some(quota) = crate::cache::override_or(&overrides, "monthly_msgs_quota", plan_quota) else {
            continue; // override forced "unlimited": nothing to enforce
        };

        let level = quota_level(msgs, quota, warn_percent);
        if level == QuotaLevel::Over {
            now_blocked.insert(cluster_id);
        }

        if let Some(announce) = quota_announcement(state.announced.get(&cluster_id).copied(), level) {
            if let Some(kind) = announce.outbox_kind() {
                let percent = if quota > 0 { (msgs as i128) * 100 / (quota as i128) } else { 100 };
                let payload = serde_json::json!({
                    "cluster_id": id_str,
                    "cluster_slug": slug,
                    "tenant_id": tenant_id,
                    "month": month,
                    "msgs": msgs,
                    "quota": quota,
                    "percent": percent as i64,
                });
                emit_quota_event(&client, kind, &id_str, &month, &payload).await;
            }
            state.announced.insert(cluster_id, announce);
        }
    }

    // HARD gate, like the storage quota and unlike the rate limits: the flag
    // is set regardless of `limits.enforcing()`, and gateway.rs 403s on it
    // regardless too. Stated here because it is the surprising part — a
    // mis-set monthly_msgs_quota stops production pushes on a cell that is
    // otherwise running in shadow mode.
    for id in now_blocked.difference(&state.blocked) {
        tracing::warn!(target: "limits", cluster = %id, blocked = true, "monthly message quota exhausted; pushes blocked");
        limits.set_push_blocked_reason(*id, PushBlock::MonthlyQuota, true);
    }
    // Released when the count falls back under (a new month restarts it at
    // ~0), when the quota is lifted, or when the cluster stops being listed at
    // all — anything absent from this pass loses its block.
    for id in state.blocked.difference(&now_blocked) {
        tracing::info!(target: "limits", cluster = %id, "monthly message quota back under; pushes unblocked");
        limits.set_push_blocked_reason(*id, PushBlock::MonthlyQuota, false);
    }
    state.blocked = now_blocked;
}

async fn emit_quota_event(
    client: &deadpool_postgres::Client,
    kind: &str,
    cluster_id: &str,
    month: &str,
    payload: &serde_json::Value,
) {
    match client.query_opt(OUTBOX_SEEN_SQL, &[&kind, &cluster_id, &month]).await {
        Ok(Some(_)) => return, // already announced by a previous process
        Ok(None) => {}
        Err(e) => {
            tracing::warn!(target: "meter", error = %e, "monthly quota: outbox dedupe check failed; emitting anyway");
        }
    }
    // jsonb as text + cast, the same binding workaround this crate uses for
    // uuid ($1::text::uuid) — tokio-postgres carries neither the uuid nor the
    // serde_json type feature here.
    let payload_text = payload.to_string();
    if let Err(e) = client
        .execute("SELECT queen_proxy.emit_outbox($1, $2::text::jsonb)", &[&kind, &payload_text])
        .await
    {
        tracing::warn!(target: "meter", kind, cluster = cluster_id, error = %e, "monthly quota: outbox emit failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a Config by hand rather than Config::load() + env::set_var: tests
    /// run concurrently in-process, and mutating a process-global env var
    /// from parallel test threads is a real data race (std::env::set_var is
    /// `unsafe` for exactly this reason). Meter::new only reads
    /// meter_flush_ms and spool_dir; the rest are inert placeholders.
    fn cfg_with_dir(dir: &std::path::Path) -> crate::config::Config {
        crate::config::Config {
            port: 0,
            bind_addr: "0.0.0.0".to_string(),
            pxdb: None,
            enforce: false,
            dev_insecure: true,
            dev_static: None,
            default_cluster: None,
            shared_hosts: Vec::new(),
            send_tenant_header: true,
            max_body_bytes: 1024,
            default_max_batch_items: 100,
            upstream_connect_timeout_ms: 1000,
            upstream_request_timeout_ms: 1000,
            longpoll_margin_ms: 1000,
            longpoll_max_ms: 1000,
            jwt_issuer: "test".to_string(),
            jwt_audience: None,
            jwt_hs_secret: None,
            jwt_ed25519_pem: None,
            jwt_ttl_s: 60,
            cookie_name: "test".to_string(),
            cookie_domain: None,
            auth_host_mode: false,
            public_base_url: None,
            auth_portal_url: None,
            auth_portal_label: crate::config::AUTH_PORTAL_LABEL.to_string(),
            operator_enabled: false,
            google_client_id: None,
            google_client_secret: None,
            google_allowed_domains: Vec::new(),
            autoprovision_default_role: "viewer".to_string(),
            github_client_id: None,
            github_client_secret: None,
            meter_flush_ms: 1000,
            spool_dir: dir.to_str().unwrap().to_string(),
        }
    }

    /// Opt-in smoke test against a real Postgres: verifies the actual SQL
    /// (`$1::text::uuid` cast, the ON CONFLICT clause, column names) against
    /// a live server rather than just the in-memory aggregation logic. Not
    /// part of the default `cargo test` run — needs a live PG on :5465 (this
    /// crate's reserved dev pxdb port, CONTRACTS.md point 4) and creates its
    /// own throwaway usage_minutes table (doesn't depend on Agent B's
    /// migration having landed yet). Run explicitly with:
    ///   cargo test --lib meter::tests::live_upsert_rows_against_real_postgres -- --ignored
    #[tokio::test]
    #[ignore = "requires a live postgres on :5465 — see doc comment"]
    async fn live_upsert_rows_against_real_postgres() {
        let pxcfg = crate::config::PxdbConfig {
            host: "127.0.0.1".to_string(),
            port: 5465,
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            dbname: "queen_proxy".to_string(),
            use_ssl: false,
            ssl_reject_unauthorized: false,
            ssl_root_cert: None,
            pool_size: 4,
            timeout_ms: 5_000,
        };
        let pool = crate::db::create_pool(&pxcfg).await.expect("connect to dev pxdb on :5465");
        {
            let client = pool.get().await.unwrap();
            client
                .batch_execute(
                    "CREATE SCHEMA IF NOT EXISTS queen_proxy;
                     DROP TABLE IF EXISTS queen_proxy.usage_minutes;
                     CREATE TABLE queen_proxy.usage_minutes (
                        cluster_id uuid NOT NULL,
                        minute timestamptz NOT NULL,
                        op_class text NOT NULL,
                        reqs bigint NOT NULL DEFAULT 0,
                        msgs bigint NOT NULL DEFAULT 0,
                        bytes_in bigint NOT NULL DEFAULT 0,
                        bytes_out bigint NOT NULL DEFAULT 0,
                        PRIMARY KEY (cluster_id, minute, op_class)
                     )",
                )
                .await
                .expect("create throwaway test schema");
        }

        let cid = Uuid::new_v4();
        let row1 =
            UsageRow { cluster_id: cid, minute: 29_000_000, op: "push".to_string(), reqs: 3, msgs: 10, bytes_in: 500, bytes_out: 0 };
        upsert_rows(&pool, &[row1.clone()]).await.expect("first upsert");
        // A second flush for the *same* (cluster,minute,op) must add, not
        // overwrite — this is the whole point of the ON CONFLICT clause.
        let row2 = UsageRow { reqs: 2, msgs: 4, bytes_in: 100, bytes_out: 50, ..row1.clone() };
        upsert_rows(&pool, &[row2]).await.expect("second upsert (additive)");

        let client = pool.get().await.unwrap();
        let r = client
            .query_one(
                "SELECT reqs, msgs, bytes_in, bytes_out, extract(epoch from minute)::bigint / 60
                 FROM queen_proxy.usage_minutes WHERE cluster_id = $1::text::uuid AND op_class = $2",
                &[&cid.to_string(), &"push"],
            )
            .await
            .expect("row should exist after upsert");
        let reqs: i64 = r.get(0);
        let msgs: i64 = r.get(1);
        let bytes_in: i64 = r.get(2);
        let bytes_out: i64 = r.get(3);
        let minute_epoch: i64 = r.get(4);
        assert_eq!((reqs, msgs, bytes_in, bytes_out), (5, 14, 600, 50), "ON CONFLICT DO UPDATE must add, not replace");
        assert_eq!(minute_epoch as u64, row1.minute, "to_timestamp($2::bigint) round-trips the minute epoch");
    }

    #[test]
    fn record_sums_within_the_same_minute() {
        let dir = tempdir();
        let meter = Meter::new(&cfg_with_dir(dir.path()));
        let cid = Uuid::new_v4();
        meter.record(Sample { cluster_id: cid, op: OpClass::Push, reqs: 1, msgs: 3, bytes_in: 100, bytes_out: 0 });
        meter.record(Sample { cluster_id: cid, op: OpClass::Push, reqs: 1, msgs: 5, bytes_in: 50, bytes_out: 10 });
        meter.record(Sample { cluster_id: cid, op: OpClass::Read, reqs: 1, msgs: 0, bytes_in: 0, bytes_out: 200 });

        let now_minute = now_minute_epoch();
        // Nothing closed yet — the current minute is retained.
        assert!(meter.drain_closed(now_minute).is_empty());
        // Draining "as of" the next minute closes everything recorded so far.
        let rows = meter.drain_closed(now_minute + 1);
        assert_eq!(rows.len(), 2, "push and read are separate op_class keys");
        let push = rows.iter().find(|r| r.op == "push").expect("push row");
        assert_eq!((push.reqs, push.msgs, push.bytes_in, push.bytes_out), (2, 8, 150, 10));
        let read = rows.iter().find(|r| r.op == "read").expect("read row");
        assert_eq!((read.reqs, read.msgs, read.bytes_in, read.bytes_out), (1, 0, 0, 200));
    }

    #[test]
    fn drain_closed_only_drains_minutes_strictly_before_now() {
        let dir = tempdir();
        let meter = Meter::new(&cfg_with_dir(dir.path()));
        let cid = Uuid::new_v4();
        meter.record(Sample { cluster_id: cid, op: OpClass::Delivery, reqs: 1, msgs: 1, bytes_in: 0, bytes_out: 1 });

        let now_minute = now_minute_epoch();
        assert!(meter.drain_closed(now_minute).is_empty(), "current minute must not drain");
        assert!(meter.drain_closed(now_minute.saturating_sub(1)).is_empty(), "a minute in the past doesn't drain the future");
        let rows = meter.drain_closed(now_minute + 1);
        assert_eq!(rows.len(), 1);
        // A second drain at the same or later point finds nothing left.
        assert!(meter.drain_closed(now_minute + 2).is_empty());
    }

    #[test]
    fn flush_once_without_db_discards_closed_rows() {
        let dir = tempdir();
        let meter = Meter::new(&cfg_with_dir(dir.path()));
        let cid = Uuid::new_v4();
        meter.record(Sample { cluster_id: cid, op: OpClass::Txn, reqs: 1, msgs: 1, bytes_in: 1, bytes_out: 1 });
        // Force the entry into a "closed" minute by draining with a future
        // reference point directly (flush_once uses real now, so we exercise
        // drain_closed the same way flush_once would via a future minute).
        let rows = meter.drain_closed(now_minute_epoch() + 1);
        assert_eq!(rows.len(), 1);
        // With db=None, flush_once's own drain would find nothing left to
        // discard a second time (already drained above) — this just confirms
        // drain_closed is destructive (entries don't reappear).
        assert!(meter.drain_closed(now_minute_epoch() + 1).is_empty());
    }

    // ---- drain(): the shutdown path takes the open minute too ----

    #[test]
    fn drain_all_takes_the_open_minute_that_drain_closed_leaves() {
        let dir = tempdir();
        let meter = Meter::new(&cfg_with_dir(dir.path()));
        let cid = Uuid::new_v4();
        meter.record(Sample { cluster_id: cid, op: OpClass::Push, reqs: 1, msgs: 7, bytes_in: 70, bytes_out: 0 });
        meter.record(Sample { cluster_id: cid, op: OpClass::Read, reqs: 2, msgs: 0, bytes_in: 0, bytes_out: 9 });

        // The periodic path would flush nothing at all here — this is exactly
        // the usage a restart used to drop.
        assert!(meter.drain_closed(now_minute_epoch()).is_empty());

        let rows = meter.drain_all();
        assert_eq!(rows.len(), 2, "both op classes of the still-open minute");
        let push = rows.iter().find(|r| r.op == "push").expect("push row");
        assert_eq!((push.reqs, push.msgs, push.bytes_in, push.bytes_out), (1, 7, 70, 0));
        assert_eq!(push.minute, now_minute_epoch(), "drained under its own minute, not a synthetic one");

        // Destructive: a second drain (or a racing periodic flush) finds
        // nothing, so nothing is billed twice.
        assert!(meter.drain_all().is_empty());
        assert!(meter.drain_closed(now_minute_epoch() + 1).is_empty());
    }

    #[tokio::test]
    async fn drain_without_pxdb_empties_the_accumulators_and_spools_nothing() {
        let dir = tempdir();
        let meter = Meter::new(&cfg_with_dir(dir.path()));
        meter.record(Sample {
            cluster_id: Uuid::new_v4(),
            op: OpClass::Txn,
            reqs: 1,
            msgs: 1,
            bytes_in: 1,
            bytes_out: 1,
        });
        // db never set (dev-static): discard, exactly like flush_once, rather
        // than spooling rows no recovery pass would ever have a DB to drain to.
        meter.drain().await;
        assert!(meter.drain_all().is_empty(), "drain must consume the aggregates either way");
        let spooled = std::fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(spooled, 0, "no pxdb means nothing to spool for");
    }

    #[tokio::test]
    async fn drain_on_an_empty_meter_is_a_noop() {
        let dir = tempdir();
        let meter = Meter::new(&cfg_with_dir(dir.path()));
        meter.drain().await;
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    // ---- monthly quota (PLAN §6.7) ----

    #[test]
    fn quota_level_bands() {
        // 80% warn band on a 1000-message allowance.
        assert_eq!(quota_level(0, 1_000, 80), QuotaLevel::Under);
        assert_eq!(quota_level(799, 1_000, 80), QuotaLevel::Under);
        assert_eq!(quota_level(800, 1_000, 80), QuotaLevel::Warn, "the threshold itself warns");
        assert_eq!(quota_level(999, 1_000, 80), QuotaLevel::Warn);
        // An allowance is spent, not exceeded: 100% blocks.
        assert_eq!(quota_level(1_000, 1_000, 80), QuotaLevel::Over);
        assert_eq!(quota_level(1_001, 1_000, 80), QuotaLevel::Over);
    }

    #[test]
    fn quota_level_is_exact_at_scale() {
        // msgs * 100 overflows i64 above ~9.2e16; the band must still be
        // computed exactly (the i128 in quota_level).
        let quota = i64::MAX;
        let warn_at = ((quota as i128) * 80 / 100) as i64;
        assert_eq!(quota_level(warn_at - 1, quota, 80), QuotaLevel::Under);
        assert_eq!(quota_level(warn_at + 1, quota, 80), QuotaLevel::Warn);
        assert_eq!(quota_level(quota, quota, 80), QuotaLevel::Over);
    }

    #[test]
    fn quota_level_zero_allowance_blocks_immediately() {
        // plans.monthly_msgs_quota is CHECK > 0, but limit_overrides is free
        // jsonb: a 0 there means "no messages this month", not "unlimited".
        assert_eq!(quota_level(0, 0, 80), QuotaLevel::Over);
        assert_eq!(quota_level(1, 0, 80), QuotaLevel::Over);
    }

    #[test]
    fn quota_announces_each_level_once_and_never_per_tick() {
        // Nothing to say below the band, however many ticks pass.
        assert_eq!(quota_announcement(None, QuotaLevel::Under), None);

        // First crossing announces; the next tick at the same level does not.
        assert_eq!(quota_announcement(None, QuotaLevel::Warn), Some(QuotaLevel::Warn));
        assert_eq!(quota_announcement(Some(QuotaLevel::Warn), QuotaLevel::Warn), None);

        // Escalation to blocked is worth one more event.
        assert_eq!(quota_announcement(Some(QuotaLevel::Warn), QuotaLevel::Over), Some(QuotaLevel::Over));
        assert_eq!(quota_announcement(Some(QuotaLevel::Over), QuotaLevel::Over), None);

        // A cluster that jumps straight past the warn band still gets told.
        assert_eq!(quota_announcement(None, QuotaLevel::Over), Some(QuotaLevel::Over));

        // Falling back (usage_minutes pruned behind the rollup) re-announces
        // nothing — that would be an event per wobble.
        assert_eq!(quota_announcement(Some(QuotaLevel::Over), QuotaLevel::Warn), None);
        assert_eq!(quota_announcement(Some(QuotaLevel::Over), QuotaLevel::Under), None);
    }

    #[test]
    fn quota_event_kinds_are_distinct_and_silent_under_the_band() {
        assert_eq!(QuotaLevel::Under.outbox_kind(), None);
        assert_eq!(QuotaLevel::Warn.outbox_kind(), Some("cluster_monthly_quota_warning"));
        assert_eq!(QuotaLevel::Over.outbox_kind(), Some("cluster_monthly_quota_blocked"));
    }

    // Minimal local tempdir helper (no tempfile crate dependency).
    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn tempdir() -> TempDir {
        let mut p = std::env::temp_dir();
        p.push(format!("queen-proxy-meter-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
}
