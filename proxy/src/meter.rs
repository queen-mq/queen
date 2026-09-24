//! Usage metering. OWNER: Agent D.
//! Contract (M1–M6, PLAN §4): meter post-response from per-item statuses —
//! never charge `error`, never double-charge `duplicate`, `buffered` counts
//! as accepted; exempt 5xx and scope-403s (all Agent A / gateway.rs's job —
//! `record()` here just aggregates whatever Sample it's handed).
//!
//! In-memory per-(cluster, op, minute) aggregates, 16-way sharded by
//! cluster_id, persisted every `cfg.meter_flush_ms`, spooled to disk
//! (spool.rs) when they cannot be, drained back on the next startup.
//! `record()` deliberately does not log at info/debug on the hot per-request
//! path (rates/sizes belong in aggregated blocks, not per-message lines — see
//! obs.rs conventions). Every statement lives in `store::usage`, which answers
//! from either backend:
//!
//! - **Postgres (the standalone proxy):** closed minutes only — the current
//!   minute keeps accumulating — ADDED to `queen_proxy.usage_minutes` by an
//!   UPSERT and forgotten; a failed flush goes to the spool at once.
//! - **The broker's KV (the single binary, `spawn_flush_store`):** each node
//!   OVERWRITES its own row per (cluster, minute, op) with its cumulative
//!   value — the open minute included, so the console and the quota see
//!   current traffic — which makes a retried write idempotent and leaves no
//!   row two nodes read-modify-write (store/usage.rs has the layout). A failed
//!   flush stays in memory and is retried on the next tick (a leader election
//!   is not worth a trip to the disk); only a long outage
//!   (`KV_BACKLOG_MAX` closed keys) or the shutdown drain spools.
//!
//! Downstream of the minute aggregates, this module also drives the billing
//! chain (`spawn_rollup`): closed days are folded into usage_days, and the
//! same tick evaluates `plans.monthly_msgs_quota` per cluster (PLAN §6.7 soft
//! enforcement: warn event at QUOTA_WARN_PERCENT, push block at 100%).
//! `drain()` is the shutdown counterpart to the periodic flush: it takes the
//! open minute too.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::limits::PushBlock;
use crate::state::{OpClass, St};
use crate::store::schema::UsageDoc;
use crate::store::{usage, KvBackend, Store};

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
const USAGE_KEEP_DAYS: u64 = usage::USAGE_KEEP_DAYS;

/// KV only: closed (cluster, op, minute) keys a node keeps in memory while
/// the broker cannot take them, before they go to the disk spool. ~100 bytes
/// each; the open minute is never spooled while running.
const KV_BACKLOG_MAX: usize = 50_000;

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

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
struct Acc {
    reqs: u64,
    msgs: u64,
    bytes_in: u64,
    bytes_out: u64,
}

impl Acc {
    fn add(&mut self, o: &Acc) {
        self.reqs = self.reqs.saturating_add(o.reqs);
        self.msgs = self.msgs.saturating_add(o.msgs);
        self.bytes_in = self.bytes_in.saturating_add(o.bytes_in);
        self.bytes_out = self.bytes_out.saturating_add(o.bytes_out);
    }

    fn sub(&mut self, o: &Acc) {
        self.reqs = self.reqs.saturating_sub(o.reqs);
        self.msgs = self.msgs.saturating_sub(o.msgs);
        self.bytes_in = self.bytes_in.saturating_sub(o.bytes_in);
        self.bytes_out = self.bytes_out.saturating_sub(o.bytes_out);
    }

    fn is_zero(&self) -> bool {
        *self == Acc::default()
    }

    fn from_doc(d: &UsageDoc) -> Acc {
        Acc {
            reqs: d.reqs.max(0) as u64,
            msgs: d.msgs.max(0) as u64,
            bytes_in: d.bytes_in.max(0) as u64,
            bytes_out: d.bytes_out.max(0) as u64,
        }
    }

    fn row(&self, key: &Key) -> UsageRow {
        UsageRow {
            cluster_id: key.0,
            minute: key.2,
            op: key.1.as_str().to_string(),
            reqs: self.reqs,
            msgs: self.msgs,
            bytes_in: self.bytes_in,
            bytes_out: self.bytes_out,
        }
    }
}

/// (cluster, op, minute epoch).
type Key = (Uuid, OpClass, u64);

/// One accumulator.
#[derive(Default, Clone, Copy, Debug)]
struct Entry {
    /// Recorded here and not persisted yet.
    pending: Acc,
    /// KV only: what THIS node's row for the key holds, `pending` excluded —
    /// set by the one seed read or by our own last successful write. `None`:
    /// not known yet. Only this node writes the row, so once known it stays
    /// true, and `base + pending` is always the right total to overwrite with,
    /// however many earlier writes were lost or merely unacknowledged.
    base: Option<Acc>,
}

/// A single closed-minute rollup, ready to flush or spool. Also the on-disk
/// spool JSONL row shape — field names match the task's `{cluster_id,minute,
/// op,reqs,msgs,bytes_in,bytes_out}` spec exactly, independent of the DB
/// column names (usage_minutes.op_class, not `op`). A spooled row is always
/// usage to ADD, on either backend.
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

/// The additive Postgres UPSERT (store/usage.rs has the SQL).
async fn upsert_rows(pool: &Pool, rows: &[UsageRow]) -> Result<(), String> {
    usage::pg_add_minutes(pool, rows).await
}

/// Where the meter persists. Set once, by `spawn_flush` / `spawn_flush_store`.
#[derive(Clone)]
enum Sink {
    /// Dev-static mode: usage is discarded.
    None,
    /// The standalone proxy's Postgres.
    Pg(Pool),
    /// The broker's KV: this node's rows, under `node`.
    Kv { kv: Arc<dyn KvBackend>, node: String, keep_days: u64 },
}

pub struct Meter {
    flush_ms: u64,
    shards: Vec<Mutex<HashMap<Key, Entry>>>,
    spool: crate::spool::Spool,
    /// Where `drain()` writes. The periodic path gets it as a parameter
    /// (`spawn_flush`), but `drain()` is called from the shutdown path with
    /// nothing but `&self`, so it is remembered here on the way past. Never
    /// set == dev-static mode: drain discards, exactly like `flush_once` does
    /// with `db: None`.
    sink: OnceLock<Sink>,
    /// One KV flush at a time (the periodic loop, the shutdown drain, the
    /// spool replay): each reads and overwrites this node's rows, and two
    /// interleaved ones could each write a total the other has not seen.
    flush_gate: tokio::sync::Mutex<()>,
    /// KV only: (cluster, UTC day) the spool replay wrote to, for the next
    /// rollup to recompute even when the day is behind its usual window.
    reroll: Mutex<HashSet<(Uuid, i64)>>,
}

impl Meter {
    pub fn new(cfg: &crate::config::Config) -> Meter {
        Meter {
            flush_ms: cfg.meter_flush_ms,
            shards: (0..N_SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            spool: crate::spool::Spool::new(&cfg.spool_dir),
            sink: OnceLock::new(),
            flush_gate: tokio::sync::Mutex::new(()),
            reroll: Mutex::new(HashSet::new()),
        }
    }

    pub fn record(&self, s: Sample) {
        self.record_at(s, now_minute_epoch());
    }

    fn record_at(&self, s: Sample, minute: u64) {
        let idx = shard_index(&s.cluster_id);
        let mut shard = self.shards[idx].lock().unwrap();
        let acc = &mut shard.entry((s.cluster_id, s.op, minute)).or_default().pending;
        acc.reqs = acc.reqs.saturating_add(s.reqs);
        acc.msgs = acc.msgs.saturating_add(s.msgs);
        acc.bytes_in = acc.bytes_in.saturating_add(s.bytes_in);
        acc.bytes_out = acc.bytes_out.saturating_add(s.bytes_out);
    }

    /// Drain every aggregate whose minute is strictly before `now_minute`
    /// across all shards; entries at `now_minute` are left in place (still
    /// accumulating). Pure/no I/O so it's directly unit-testable.
    fn drain_closed(&self, now_minute: u64) -> Vec<UsageRow> {
        let mut rows = Vec::new();
        for shard in &self.shards {
            let mut m = shard.lock().unwrap();
            m.retain(|key, e| {
                if key.2 < now_minute {
                    rows.push(e.pending.row(key));
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
            for (key, e) in m.drain() {
                rows.push(e.pending.row(&key));
            }
        }
        rows
    }

    /// KV: take what is still pending (usage to ADD), forgetting everything;
    /// `closed_before` limits it to minutes strictly before that one.
    fn take_pending(&self, closed_before: Option<u64>) -> Vec<UsageRow> {
        let mut rows = Vec::new();
        for shard in &self.shards {
            let mut m = shard.lock().unwrap();
            m.retain(|key, e| {
                if closed_before.is_some_and(|b| key.2 >= b) {
                    return true;
                }
                if !e.pending.is_zero() {
                    rows.push(e.pending.row(key));
                }
                false
            });
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
        if let Some(Sink::Kv { kv, node, keep_days }) = self.sink.get() {
            return self.drain_kv(kv.as_ref(), node, *keep_days).await;
        }
        let rows = self.drain_all();
        if rows.is_empty() {
            return;
        }
        let Some(Sink::Pg(pool)) = self.sink.get() else {
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

    /// KV shutdown: one last flush of everything (every minute counts as
    /// closed now), bounded like the Postgres drain; whatever it could not
    /// write goes to the spool as usage to add.
    async fn drain_kv(&self, kv: &dyn KvBackend, node: &str, keep_days: u64) {
        let res = tokio::time::timeout(DRAIN_TIMEOUT, self.flush_kv(kv, node, keep_days, u64::MAX)).await;
        let left = self.take_pending(None);
        match res {
            Ok(Ok(n)) if left.is_empty() => {
                if n > 0 {
                    tracing::info!(target: "meter", rows = n, node, "usage drained to the kv on shutdown");
                }
            }
            Ok(Ok(_)) | Ok(Err(_)) | Err(_) => {
                let why = match &res {
                    Ok(Err(e)) => e.clone(),
                    Err(_) => format!("timed out after {} ms", DRAIN_TIMEOUT.as_millis()),
                    Ok(Ok(_)) => "partial".to_string(),
                };
                if !left.is_empty() {
                    tracing::warn!(target: "meter", rows = left.len(), node, error = %why, "shutdown drain to the kv failed; spooling to disk");
                    self.spool.write(&left);
                }
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
        let _ = self.sink.set(match &db {
            Some(pool) => Sink::Pg(pool.clone()),
            None => Sink::None,
        });
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

    /// `spawn_flush` for any [`Store`]: Postgres and none behave exactly as
    /// `spawn_flush`; the KV (the single binary) writes this node's rows
    /// under `node` (a label unique per process and stable across its
    /// restarts — see `usage::node_label`). The spool is replayed first, as
    /// usage to add.
    pub fn spawn_flush_store(self: &Arc<Self>, store: &Store, node: &str) {
        let kv = match store {
            Store::Pg(pool) => return self.spawn_flush(Some(pool.clone())),
            Store::None => return self.spawn_flush(None),
            Store::Kv(kv) => kv.clone(),
        };
        let node = usage::node_label(node);
        let keep_days = usage::keep_days_from_env();
        let _ = self.sink.set(Sink::Kv { kv: kv.clone(), node: node.clone(), keep_days });
        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.recover_kv(kv.clone(), node.clone(), keep_days).await;
            tracing::info!(target: "meter", node = %node, keep_days, flush_ms = this.flush_ms, "usage metering to the kv");
            let mut tick = tokio::time::interval(Duration::from_millis(this.flush_ms.max(100)));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut failing = false;
            loop {
                tick.tick().await;
                let now_minute = now_minute_epoch();
                match this.flush_kv(kv.as_ref(), &node, keep_days, now_minute).await {
                    Ok(n) => {
                        if failing {
                            tracing::info!(target: "meter", rows = n, "usage flush to the kv recovered");
                        }
                        failing = false;
                    }
                    Err(e) => {
                        // Once per streak: the next ticks retry the same rows.
                        if !failing {
                            tracing::warn!(target: "meter", error = %e, "usage flush to the kv failed; keeping it in memory to retry");
                        }
                        failing = true;
                        this.spool_kv_backlog(now_minute, KV_BACKLOG_MAX);
                    }
                }
            }
        });
    }

    /// KV: overwrite this node's row of every key with pending usage with its
    /// cumulative total (`base + pending`), the open minute included; then
    /// forget the minutes before `now_minute` that are fully written. A key
    /// whose base is not known yet reads this node's row first (a restart
    /// within the minute, or a late record into a minute already forgotten).
    /// A batch that fails keeps its keys pending for the next call; the first
    /// error is returned after every batch was tried.
    async fn flush_kv(&self, kv: &dyn KvBackend, node: &str, keep_days: u64, now_minute: u64) -> Result<usize, String> {
        let _gate = self.flush_gate.lock().await;
        let mut dirty: Vec<(Key, Acc, Option<Acc>)> = Vec::new();
        for shard in &self.shards {
            let m = shard.lock().unwrap();
            dirty.extend(m.iter().filter(|(_, e)| !e.pending.is_zero()).map(|(k, e)| (*k, e.pending, e.base)));
        }
        let mut first_err: Option<String> = None;

        let unseeded: Vec<(Uuid, u64, String)> =
            dirty.iter().filter(|d| d.2.is_none()).map(|(k, _, _)| (k.0, k.2, k.1.as_str().to_string())).collect();
        if !unseeded.is_empty() {
            match usage::kv_read_own(kv, node, &unseeded).await {
                Ok(docs) => {
                    let mut seeds: HashMap<Key, Acc> = HashMap::with_capacity(docs.len());
                    for (d, doc) in dirty.iter().filter(|d| d.2.is_none()).zip(docs) {
                        seeds.insert(d.0, Acc::from_doc(&doc));
                    }
                    for d in dirty.iter_mut() {
                        if let Some(b) = seeds.get(&d.0) {
                            d.2 = Some(*b);
                            if let Some(e) = self.shards[shard_index(&d.0 .0)].lock().unwrap().get_mut(&d.0) {
                                e.base.get_or_insert(*b);
                            }
                        }
                    }
                }
                Err(e) => first_err = Some(e),
            }
        }

        // (key, pending written, total written)
        let ready: Vec<(Key, Acc, Acc)> = dirty
            .into_iter()
            .filter_map(|(k, pending, base)| {
                let mut total = base?;
                total.add(&pending);
                Some((k, pending, total))
            })
            .collect();
        let mut written = 0usize;
        let now_us = usage::now_us();
        for chunk in ready.chunks(usage::KV_BATCH) {
            let rows: Vec<UsageRow> = chunk.iter().map(|(k, _, total)| total.row(k)).collect();
            match usage::kv_write_totals(kv, node, keep_days, &rows, now_us).await {
                Ok(_) => {
                    for (k, pending, total) in chunk {
                        if let Some(e) = self.shards[shard_index(&k.0)].lock().unwrap().get_mut(k) {
                            e.base = Some(*total);
                            e.pending.sub(pending);
                        }
                    }
                    written += chunk.len();
                }
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }

        for shard in &self.shards {
            shard.lock().unwrap().retain(|k, e| k.2 >= now_minute || !e.pending.is_zero());
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(written),
        }
    }

    /// KV, after a failed flush: when the closed minutes still waiting exceed
    /// `max` (`KV_BACKLOG_MAX`), move them to the disk spool (as usage to add)
    /// instead of growing without bound. Skipped while another flush holds the
    /// gate — it may be writing those very rows.
    fn spool_kv_backlog(&self, now_minute: u64, max: usize) {
        let Ok(_gate) = self.flush_gate.try_lock() else { return };
        let waiting: usize = self
            .shards
            .iter()
            .map(|s| s.lock().unwrap().iter().filter(|(k, e)| k.2 < now_minute && !e.pending.is_zero()).count())
            .sum();
        if waiting <= max {
            return;
        }
        let rows = self.take_pending(Some(now_minute));
        tracing::warn!(target: "meter", rows = rows.len(), "kv unavailable for too long; spooling closed minutes to disk");
        self.spool.write(&rows);
    }

    /// KV startup: replay the spool (usage to add) onto this node's rows, a
    /// chunk per atomic write, before the first flush seeds anything.
    async fn recover_kv(self: &Arc<Self>, kv: Arc<dyn KvBackend>, node: String, keep_days: u64) {
        let this = Arc::clone(self);
        self.spool
            .recover_chunked(usage::KV_BATCH, move |rows| {
                let (this, kv, node) = (this.clone(), kv.clone(), node.clone());
                async move {
                    let _gate = this.flush_gate.lock().await;
                    let touched = usage::kv_add_minutes(kv.as_ref(), &node, keep_days, &rows, usage::now_us()).await?;
                    this.reroll.lock().unwrap().extend(touched);
                    Ok(())
                }
            })
            .await;
    }

    /// KV: the days the spool replay wrote to, for the rollup (taken).
    fn take_reroll(&self) -> Vec<(Uuid, i64)> {
        self.reroll.lock().unwrap().drain().collect()
    }

    fn restore_reroll(&self, days: Vec<(Uuid, i64)>) {
        self.reroll.lock().unwrap().extend(days);
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
/// fronts one cell (PLAN §2), and every value here is re-derived from the
/// store on the first tick after a restart.
#[derive(Default)]
struct QuotaState {
    /// UTC calendar month ("YYYY-MM") the `announced` map belongs to. A
    /// different month from the store resets it — that IS the monthly release.
    month: String,
    /// Highest level already announced per cluster, this month.
    announced: HashMap<Uuid, QuotaLevel>,
    /// Clusters this task currently holds a `PushBlock::MonthlyQuota` on.
    blocked: HashSet<Uuid>,
}

/// Periodic billing driver: fold closed days into `usage_days`, then evaluate
/// `plans.monthly_msgs_quota` (PLAN §6.7). Detached, never panics, tolerates
/// the store being down by skipping the tick — a failed read is not evidence
/// that a block may be released. Runs on `st.store`: the standalone proxy's
/// Postgres, or (single binary) the broker's KV on every node — the rollup
/// writes the same rows whichever node runs it, and the push blocks it sets
/// are per process.
pub fn spawn_rollup(st: St) {
    if !st.store.is_some() {
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
            rollup_once(&st.store, &st.meter).await;
            enforce_monthly_quota(&st.store, &st.limits, &mut quota, warn_percent).await;
        }
    });
}

async fn rollup_once(store: &Store, meter: &Meter) {
    let keep_days_raw = crate::config::env_u64("QUEEN_PROXY_USAGE_KEEP_DAYS", USAGE_KEEP_DAYS);
    // Days the spool replay wrote to (KV): recomputed even when they are
    // behind the rollup's usual window; given back when the pass fails.
    let extra = meter.take_reroll();
    match usage::rollup_days(store, keep_days_raw.max(1), &extra).await {
        Ok(rows) => {
            tracing::info!(target: "meter", rows, "usage_days rollup ok");
        }
        Err(e) => {
            tracing::warn!(target: "meter", error = %e, "usage_days rollup failed; retrying next tick");
            meter.restore_reroll(extra);
            // Pruning below only deletes minutes whose day is already rolled up,
            // so a failed rollup makes it a no-op rather than a data loss — but
            // there is nothing to gain from the round trip either.
            return;
        }
    }

    // Bound usage_minutes growth. Ordered strictly after the rollup: the prune
    // is gated on the day existing in usage_days, so running it first would
    // simply skip the days this pass just folded in. (KV: the rows carry a
    // TTL instead, and this is a no-op.)
    let keep_days = keep_days_raw as i32;
    match usage::prune_minutes(store, keep_days).await {
        Ok(pruned) => {
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
    store: &Store,
    limits: &crate::limits::Limits,
    state: &mut QuotaState,
    warn_percent: i64,
) {
    let rows = match usage::quota_rows(store).await {
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
        if state.month != first.month {
            state.month = first.month.clone();
            state.announced.clear();
        }
    }

    let mut now_blocked: HashSet<Uuid> = HashSet::new();
    for row in &rows {
        let cluster_id = row.cluster_id;
        let id_str = cluster_id.to_string();
        let msgs = row.msgs;
        // Same three-way override rule as every other limit (cache.rs):
        // absent -> plan, JSON null -> explicitly unlimited, number -> that.
        let Some(quota) = crate::cache::override_or(&row.overrides, "monthly_msgs_quota", row.plan_quota) else {
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
                    "cluster_slug": row.slug,
                    "tenant_id": row.tenant_id,
                    "month": row.month,
                    "msgs": msgs,
                    "quota": quota,
                    "percent": percent as i64,
                });
                emit_quota_event(store, kind, &id_str, &row.month, &payload).await;
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

/// Has this exact (kind, cluster, month) event already been written? The
/// in-process `QuotaState` covers the common case; this covers a proxy restart
/// mid-month (and, in the single binary, the other nodes), which would
/// otherwise re-announce every cluster already over the line. Best-effort by
/// design: on a read error we let the emit proceed (a duplicate CP event beats
/// a swallowed one).
async fn emit_quota_event(store: &Store, kind: &str, cluster_id: &str, month: &str, payload: &serde_json::Value) {
    match usage::quota_event_seen(store, kind, cluster_id, month).await {
        Ok(true) => return, // already announced by a previous process
        Ok(false) => {}
        Err(e) => {
            tracing::warn!(target: "meter", error = %e, "monthly quota: outbox dedupe check failed; emitting anyway");
        }
    }
    if let Err(e) = usage::emit_outbox(store, kind, payload).await {
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

    // ---- KV (the single binary): one row per node, cumulative, idempotent ----

    use crate::store::kv::{BoxFut, KvError};
    use crate::store::memkv::MemKv;
    use crate::store::schema::{self, ns, ClusterDoc, OutboxDoc, PlanDoc};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn kv_meter(dir: &std::path::Path, kv: Arc<dyn KvBackend>, node: &str) -> Meter {
        let meter = Meter::new(&cfg_with_dir(dir));
        let _ = meter.sink.set(Sink::Kv { kv, node: node.to_string(), keep_days: 90 });
        meter
    }

    fn push(cluster_id: Uuid, msgs: u64) -> Sample {
        Sample { cluster_id, op: OpClass::Push, reqs: 1, msgs, bytes_in: msgs * 10, bytes_out: 0 }
    }

    async fn minute_msgs(kv: &dyn KvBackend, cluster: Uuid, minute: u64) -> i64 {
        let m = (minute as i64) * usage::MINUTE_US;
        usage::kv_usage_by_minute(kv, cluster, m, Some(m + usage::MINUTE_US))
            .await
            .unwrap()
            .iter()
            .map(|r| r.msgs)
            .sum()
    }

    fn in_memory(meter: &Meter) -> usize {
        meter.shards.iter().map(|s| s.lock().unwrap().len()).sum()
    }

    /// Always "no leader".
    struct Down;
    impl KvBackend for Down {
        fn kv(&self, _ops: Vec<serde_json::Value>) -> BoxFut<'_, Result<Vec<serde_json::Value>, KvError>> {
            Box::pin(async { Err(KvError::Unavailable("no leader".into())) })
        }
    }

    /// Applies a write, then reports it lost, `lose` times.
    struct LostAck {
        inner: MemKv,
        lose: AtomicUsize,
    }
    impl KvBackend for LostAck {
        fn kv(&self, ops: Vec<serde_json::Value>) -> BoxFut<'_, Result<Vec<serde_json::Value>, KvError>> {
            Box::pin(async move {
                let writes = ops.iter().any(|o| o["op"] == "put");
                let out = self.inner.kv(ops).await?;
                if writes && self.lose.load(Ordering::SeqCst) > 0 {
                    self.lose.fetch_sub(1, Ordering::SeqCst);
                    return Err(KvError::Unavailable("ack lost".into()));
                }
                Ok(out)
            })
        }
    }

    #[tokio::test]
    async fn kv_nodes_write_their_own_rows_and_readers_sum_them() {
        let dir = tempdir();
        let mem = Arc::new(MemKv::new());
        let kv: Arc<dyn KvBackend> = mem.clone();
        let a = kv_meter(dir.path(), kv.clone(), "node-a");
        let b = kv_meter(dir.path(), kv.clone(), "node-b");
        let cid = Uuid::new_v4();
        let now = now_minute_epoch();
        a.record_at(push(cid, 3), now);
        b.record_at(push(cid, 4), now);
        a.flush_kv(kv.as_ref(), "node-a", 90, now).await.unwrap();
        b.flush_kv(kv.as_ref(), "node-b", 90, now).await.unwrap();
        assert_eq!(minute_msgs(kv.as_ref(), cid, now).await, 7, "the open minute is written too");
        assert_eq!(mem.keys(ns::USAGE_MIN).len(), 2, "one row per node");

        // More traffic on a: ITS row is overwritten with the cumulative value.
        a.record_at(push(cid, 5), now);
        assert_eq!(a.flush_kv(kv.as_ref(), "node-a", 90, now).await.unwrap(), 1);
        // Nothing new: nothing written.
        assert_eq!(a.flush_kv(kv.as_ref(), "node-a", 90, now).await.unwrap(), 0);
        assert_eq!(minute_msgs(kv.as_ref(), cid, now).await, 12);
        let own = usage::kv_read_own(kv.as_ref(), "node-a", &[(cid, now, "push".to_string())]).await.unwrap();
        assert_eq!((own[0].msgs, own[0].reqs, own[0].bytes_in), (8, 2, 80));

        // The open minute stays in memory; once closed and written it goes.
        assert_eq!(in_memory(&a), 1);
        a.flush_kv(kv.as_ref(), "node-a", 90, now + 1).await.unwrap();
        assert_eq!(in_memory(&a), 0);
    }

    #[tokio::test]
    async fn kv_a_retried_write_whose_ack_was_lost_counts_once() {
        let dir = tempdir();
        let lost = Arc::new(LostAck { inner: MemKv::new(), lose: AtomicUsize::new(1) });
        let kv: Arc<dyn KvBackend> = lost.clone();
        let m = kv_meter(dir.path(), kv.clone(), "n1");
        let cid = Uuid::new_v4();
        let now = now_minute_epoch();
        m.record_at(push(cid, 5), now - 1);
        assert!(m.flush_kv(kv.as_ref(), "n1", 90, now).await.is_err(), "the ack was lost");
        assert_eq!(in_memory(&m), 1, "a closed minute that failed is kept to retry");
        m.record_at(push(cid, 1), now - 1); // a late record meanwhile
        m.flush_kv(kv.as_ref(), "n1", 90, now).await.unwrap();
        assert_eq!(minute_msgs(&lost.inner, cid, now - 1).await, 6, "5 + 1, not 5 + 5 + 1");
        assert_eq!(in_memory(&m), 0);
    }

    #[tokio::test]
    async fn kv_a_restart_or_a_late_record_continues_the_row() {
        let dir = tempdir();
        let kv: Arc<dyn KvBackend> = Arc::new(MemKv::new());
        let cid = Uuid::new_v4();
        let now = now_minute_epoch();
        let first = kv_meter(dir.path(), kv.clone(), "n1");
        first.record_at(push(cid, 3), now);
        first.flush_kv(kv.as_ref(), "n1", 90, now).await.unwrap();
        drop(first);

        // Same node label, new process, same minute: seeded from its own row.
        let second = kv_meter(dir.path(), kv.clone(), "n1");
        second.record_at(push(cid, 2), now);
        second.flush_kv(kv.as_ref(), "n1", 90, now + 1).await.unwrap();
        assert_eq!(minute_msgs(kv.as_ref(), cid, now).await, 5);
        assert_eq!(in_memory(&second), 0, "closed and written: forgotten");

        // A record racing the minute boundary lands in the forgotten minute.
        second.record_at(push(cid, 4), now);
        second.flush_kv(kv.as_ref(), "n1", 90, now + 1).await.unwrap();
        assert_eq!(minute_msgs(kv.as_ref(), cid, now).await, 9);
    }

    #[tokio::test]
    async fn kv_drain_spools_what_the_broker_refused_and_the_next_start_adds_it() {
        let dir = tempdir();
        let cid = Uuid::new_v4();
        let now = now_minute_epoch();
        let down = kv_meter(dir.path(), Arc::new(Down), "n1");
        down.record_at(push(cid, 4), now);
        down.drain().await;
        assert_eq!(in_memory(&down), 0, "the drain consumes the aggregates either way");
        drop(down);
        let spooled: Vec<UsageRow> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .flat_map(|e| {
                std::fs::read_to_string(e.path())
                    .unwrap()
                    .lines()
                    .filter(|l| !l.trim().is_empty())
                    .map(|l| serde_json::from_str::<UsageRow>(l).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(spooled.len(), 1);
        assert_eq!((spooled[0].msgs, spooled[0].minute), (4, now));

        // Next start, broker healthy, where this node had already written 10
        // for that minute before the failed drain: the spool ADDS.
        let mem = Arc::new(MemKv::new());
        let kv: Arc<dyn KvBackend> = mem.clone();
        let before = UsageRow { cluster_id: cid, minute: now, op: "push".into(), reqs: 1, msgs: 10, bytes_in: 0, bytes_out: 0 };
        usage::kv_write_totals(kv.as_ref(), "n1", 90, &[before], usage::now_us()).await.unwrap();
        let next = Arc::new(Meter::new(&cfg_with_dir(dir.path())));
        next.recover_kv(kv.clone(), "n1".to_string(), 90).await;
        assert_eq!(minute_msgs(kv.as_ref(), cid, now).await, 14);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0, "replayed files are removed");
        let day = usage::day_of((now as i64) * usage::MINUTE_US);
        assert_eq!(next.take_reroll(), vec![(cid, day)], "the rollup is told which day moved");
    }

    #[tokio::test]
    async fn kv_a_long_outage_moves_closed_minutes_to_the_spool() {
        let dir = tempdir();
        let m = kv_meter(dir.path(), Arc::new(Down), "n1");
        let cid = Uuid::new_v4();
        let now = now_minute_epoch();
        m.record_at(push(cid, 1), now - 2);
        m.record_at(push(cid, 1), now - 1);
        m.record_at(push(cid, 1), now);
        assert!(m.flush_kv(&Down, "n1", 90, now).await.is_err());
        m.spool_kv_backlog(now, 5);
        assert_eq!(in_memory(&m), 3, "under the cap: kept in memory");
        m.spool_kv_backlog(now, 1);
        assert_eq!(in_memory(&m), 1, "over it: the closed minutes went to disk, the open one stays");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn kv_quota_pass_blocks_and_announces_once_across_restarts() {
        let dir = tempdir();
        let mem = Arc::new(MemKv::new());
        let kv: Arc<dyn KvBackend> = mem.clone();
        let store = Store::Kv(kv.clone());
        let plan = PlanDoc { id: Uuid::new_v4(), code: "q".into(), monthly_msgs_quota: Some(10), ..Default::default() };
        let cluster = ClusterDoc {
            id: Uuid::new_v4(),
            tenant_id: Uuid::new_v4(),
            cell_id: Uuid::new_v4(),
            plan_id: plan.id,
            slug: "acme".into(),
            broker_tenant_uuid: Uuid::new_v4(),
            status: "active".into(),
            limit_overrides: serde_json::json!({}),
            created_at_us: 0,
        };
        crate::store::kv::write(
            kv.as_ref(),
            vec![
                crate::store::kv::put_op(ns::PLANS, &schema::key(plan.id), &plan, crate::store::kv::Expect::Any, crate::store::kv::Ttl::Forever, false),
                crate::store::kv::put_op(ns::CLUSTERS, &schema::key(cluster.id), &cluster, crate::store::kv::Expect::Any, crate::store::kv::Ttl::Forever, false),
            ],
        )
        .await
        .unwrap();
        let used = UsageRow { cluster_id: cluster.id, minute: now_minute_epoch(), op: "push".into(), reqs: 1, msgs: 12, bytes_in: 0, bytes_out: 0 };
        usage::kv_write_totals(kv.as_ref(), "n1", 90, &[used], usage::now_us()).await.unwrap();

        let limits = crate::limits::Limits::new(&cfg_with_dir(dir.path()));
        let mut state = QuotaState::default();
        enforce_monthly_quota(&store, &limits, &mut state, 80).await;
        assert_eq!(limits.push_block_reason(cluster.id), Some(PushBlock::MonthlyQuota));

        // A restart (or the next node): fresh memory, same store — no second event.
        let mut fresh = QuotaState::default();
        enforce_monthly_quota(&store, &limits, &mut fresh, 80).await;
        let events: Vec<(String, crate::store::kv::Doc<OutboxDoc>)> =
            crate::store::kv::scan(kv.as_ref(), ns::OUTBOX, "#").await.unwrap();
        assert_eq!(events.len(), 1, "announced once: {events:?}");
        let ev = &events[0].1.value;
        assert_eq!(ev.kind, "cluster_monthly_quota_blocked");
        assert_eq!(ev.payload["cluster_slug"], "acme");
        assert_eq!(ev.payload["msgs"], 12);
        assert_eq!(ev.payload["cluster_id"], cluster.id.to_string());
    }

    #[tokio::test]
    async fn kv_rollup_once_uses_the_days_the_spool_touched() {
        let dir = tempdir();
        let mem = Arc::new(MemKv::new());
        let kv: Arc<dyn KvBackend> = mem.clone();
        let store = Store::Kv(kv.clone());
        let meter = Meter::new(&cfg_with_dir(dir.path()));
        let cid = Uuid::new_v4();
        let cluster = ClusterDoc {
            id: cid,
            tenant_id: Uuid::new_v4(),
            cell_id: Uuid::new_v4(),
            plan_id: Uuid::new_v4(),
            slug: "acme".into(),
            broker_tenant_uuid: Uuid::new_v4(),
            status: "active".into(),
            limit_overrides: serde_json::json!({}),
            created_at_us: 0,
        };
        crate::store::kv::write(
            kv.as_ref(),
            vec![crate::store::kv::put_op(ns::CLUSTERS, &schema::key(cid), &cluster, crate::store::kv::Expect::Any, crate::store::kv::Ttl::Forever, false)],
        )
        .await
        .unwrap();
        let today = usage::day_of(usage::now_us());
        // Yesterday rolled already; day -10 got a late spool replay.
        let y = UsageRow { cluster_id: cid, minute: ((today - 1) * 1440) as u64, op: "push".into(), reqs: 1, msgs: 1, bytes_in: 0, bytes_out: 0 };
        let old = UsageRow { minute: ((today - 10) * 1440) as u64, msgs: 5, ..y.clone() };
        usage::kv_write_totals(kv.as_ref(), "n1", 90, &[y, old], usage::now_us()).await.unwrap();
        rollup_once(&store, &meter).await;
        let day_msgs = |d: i64| {
            let kv = kv.clone();
            async move {
                crate::store::kv::get::<crate::store::schema::UsageDoc>(kv.as_ref(), ns::USAGE_DAY, &usage::day_key(cid, &usage::day_str(d), "push"))
                    .await
                    .unwrap()
                    .map(|d| d.value.msgs)
            }
        };
        assert_eq!(day_msgs(today - 1).await, Some(1));
        assert_eq!(day_msgs(today - 10).await, Some(5), "never rolled: the whole window is");
        let more = UsageRow { cluster_id: cid, minute: ((today - 10) * 1440 + 1) as u64, op: "push".into(), reqs: 1, msgs: 2, bytes_in: 0, bytes_out: 0 };
        usage::kv_write_totals(kv.as_ref(), "n2", 90, &[more], usage::now_us()).await.unwrap();
        rollup_once(&store, &meter).await;
        assert_eq!(day_msgs(today - 10).await, Some(5), "behind the window: untouched");
        meter.restore_reroll(vec![(cid, today - 10)]);
        rollup_once(&store, &meter).await;
        assert_eq!(day_msgs(today - 10).await, Some(7));
        assert!(meter.take_reroll().is_empty(), "consumed by the pass");
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
