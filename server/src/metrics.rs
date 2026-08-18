use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

pub struct OpMetrics {
    pub name: &'static str,
    pub requests: AtomicU64,
    pub messages: AtomicU64,
    pub empty: AtomicU64,
    pub batches_fired: AtomicU64,
    pub items_fired: AtomicU64,
    pub completions_ok: AtomicU64,
    pub completions_err: AtomicU64,
    rtt: Mutex<Vec<f64>>, // bounded ring of recent RTT ms
    rtt_head: AtomicU64,
}

const RTT_CAP: usize = 1024;

impl OpMetrics {
    pub fn new(name: &'static str) -> OpMetrics {
        OpMetrics {
            name,
            requests: AtomicU64::new(0),
            messages: AtomicU64::new(0),
            empty: AtomicU64::new(0),
            batches_fired: AtomicU64::new(0),
            items_fired: AtomicU64::new(0),
            completions_ok: AtomicU64::new(0),
            completions_err: AtomicU64::new(0),
            rtt: Mutex::new(vec![0.0; RTT_CAP]),
            rtt_head: AtomicU64::new(0),
        }
    }

    pub fn record_request(&self, msgs: usize) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        if msgs > 0 {
            self.messages.fetch_add(msgs as u64, Ordering::Relaxed);
        } else {
            self.empty.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_batch(&self, items: usize, ok: bool, rtt: Duration) {
        self.batches_fired.fetch_add(1, Ordering::Relaxed);
        self.items_fired.fetch_add(items as u64, Ordering::Relaxed);
        if ok {
            self.completions_ok.fetch_add(1, Ordering::Relaxed);
        } else {
            self.completions_err.fetch_add(1, Ordering::Relaxed);
        }
        let ms = rtt.as_secs_f64() * 1000.0;
        let h = (self.rtt_head.fetch_add(1, Ordering::Relaxed) as usize) % RTT_CAP;
        if let Ok(mut r) = self.rtt.lock() {
            r[h] = ms;
        }
    }

    pub fn rtt_percentile(&self, p: f64) -> f64 {
        let mut v = { self.rtt.lock().unwrap().clone() };
        v.retain(|&x| x > 0.0);
        if v.is_empty() {
            return 0.0;
        }
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((p / 100.0) * (v.len() - 1) as f64) as usize;
        v[idx.min(v.len() - 1)]
    }
}

pub struct Metrics {
    pub push: std::sync::Arc<OpMetrics>,
    pub pop: std::sync::Arc<OpMetrics>,
    pub ack: std::sync::Arc<OpMetrics>,
    /// Transaction requests (execute_transaction). Kept alongside push/pop/ack so
    /// the worker-metrics collector (syscollect.rs) can flush a per-minute count.
    pub transactions: AtomicU64,
    /// Successful / failed ack items, accumulated across all ack calls. ack.requests
    /// counts ACK API calls; these count individual acknowledged items by outcome so
    /// the collector can populate worker_metrics.ack_{success,failed}_count.
    pub ack_success: AtomicU64,
    pub ack_failed: AtomicU64,
    /// DLQ transitions observed on the ack path (worker_metrics parity).
    pub dlq_moved: AtomicU64,
    /// Database failures observed on the DATA paths (push/pop/ack/transaction):
    /// a statement error, a statement timeout, or a pool acquisition failure.
    /// Bump it ONLY through `record_db_error(s)` — the gauge is charted as
    /// "DB errors", so a path that fails without counting reads as healthy.
    pub db_errors: AtomicU64,
    /// Pop path split (Phase 2): a woken long-poll that drains a partition hint
    /// issues a targeted single-partition pop (`pop_targeted`) instead of the
    /// wildcard candidate scan (`pop_wildcard`). `pop_targeted / (pop_targeted +
    /// pop_wildcard)` is the fraction of scans the hint mailbox replaced.
    pub pop_targeted: AtomicU64,
    pub pop_wildcard: AtomicU64,
    /// TASK M (minimum pop wait): how many pops held an UNDER-FULL batch back to
    /// let it fatten, and the total microseconds they spent doing so. The ratio
    /// is the average window actually paid; `pop_fill_wait / pop_requests` is how
    /// often the lever engaged at all. Both stay 0 on every queue with
    /// `minPopWaitTime = 0`, which is the default.
    pub pop_fill_wait: AtomicU64,
    pub pop_fill_wait_us: AtomicU64,
    /// RUSTFIX item 24: per-queue throughput, flushed into queen.queue_lag_metrics.
    pub per_queue: PerQueue,
    /// Parked long-poll gauge (dashboard Parked row / queue_parked_replica).
    pub parked: Parked,
    /// Scheduler ("event loop") lag probe accumulators: a 100 ms ticker measures
    /// sleep overshoot — the tokio analogue of Node's event-loop lag that the
    /// dashboard's "Event loop" row reads from worker_metrics.avg/max_event_loop_lag_ms.
    /// sum/count are cumulative (collector diffs); max is swap(0)-drained per flush.
    pub evl_sum_us: AtomicU64,
    pub evl_count: AtomicU64,
    pub evl_max_us: AtomicU64,
    /// KV / timers / sweeper series (PLAN_KV_TIMERS.md §14). Always constructed —
    /// the struct is a few hundred bytes of atomics — but INERT and, crucially,
    /// UNEXPOSED until `enable_kv_timers` says a flag is on, so a broker that never
    /// uses the feature emits a byte-identical `/metrics/prometheus`.
    pub kvt: KvTimers,
    start: Instant,
}

/// A point-in-time read of the cumulative counters, taken by the collector each
/// minute; the collector diffs successive snapshots to derive per-minute deltas.
#[derive(Clone, Copy, Default)]
pub struct Counters {
    pub push_requests: u64,
    pub push_messages: u64,
    pub pop_requests: u64,
    pub pop_messages: u64,
    pub ack_requests: u64,
    pub ack_messages: u64,
    pub ack_success: u64,
    pub ack_failed: u64,
    pub transactions: u64,
    pub dlq_moved: u64,
    pub db_errors: u64,
}

/// RUSTFIX item 24: per-queue throughput counters, flushed each minute into
/// queen.queue_lag_metrics so the per-queue Prometheus families
/// (queen_queue_*_per_minute) and the /analytics/queue-lag|queue-ops views show
/// real data instead of zeros.
///
/// ack_* is attributed via the AppState partition->queue cache (the ack wire is
/// keyed by partitionId only); pop lag is the per-message age at delivery,
/// computed from each segment's createdAt in the pop renderer. Wildcard-discover
/// pops (which span queues) remain unattributed.
#[derive(Default)]
pub struct QueueCounters {
    pub push_requests: AtomicU64,
    pub push_messages: AtomicU64,
    pub pop_count: AtomicU64,
    pub pop_empty: AtomicU64,
    pub transactions: AtomicU64,
    pub ack_requests: AtomicU64,
    pub ack_success: AtomicU64,
    pub ack_failed: AtomicU64,
    // Pop lag: cumulative sum of message ages (ms) + message count for a
    // weighted average, and an interval max the collector swap(0)-drains.
    pub lag_sum_ms: AtomicU64,
    pub lag_count: AtomicU64,
    pub lag_max_ms: AtomicU64,
}

/// A per-queue snapshot the collector diffs into per-minute deltas.
/// (`lag_max_ms` is NOT here — a max isn't diffable; the collector drains it
/// separately via take_lag_max.)
#[derive(Clone, Copy, Default)]
pub struct QueueSnap {
    pub push_requests: u64,
    pub push_messages: u64,
    pub pop_count: u64,
    pub pop_empty: u64,
    pub transactions: u64,
    pub ack_requests: u64,
    pub ack_success: u64,
    pub ack_failed: u64,
    pub lag_sum_ms: u64,
    pub lag_count: u64,
}

#[derive(Default)]
pub struct PerQueue {
    map: RwLock<HashMap<String, Arc<QueueCounters>>>,
}

impl PerQueue {
    /// Fast path: a read lock + Arc clone for an already-seen key; the write lock
    /// is taken only the first time a key is observed (bounded by #(tenant,queue)).
    /// Track B (§5): the map is keyed by `tenant_queue_key(tenant, queue)` so two
    /// tenants sharing a queue NAME accumulate separate counters (else their
    /// per-minute lag/ops rows would merge in-process, before the DB ever sees
    /// them). Flag OFF ⇒ every key is `<default>\x1f<queue>` ⇒ byte-identical.
    fn counters(&self, key: &str) -> Arc<QueueCounters> {
        if let Some(c) = self.map.read().unwrap().get(key) {
            return c.clone();
        }
        self.map.write().unwrap().entry(key.to_string()).or_default().clone()
    }
    pub fn add_push(&self, tenant: &str, queue: &str, msgs: u64) {
        let c = self.counters(&crate::handlers::tenant_queue_key(tenant, queue));
        c.push_requests.fetch_add(1, Ordering::Relaxed);
        c.push_messages.fetch_add(msgs, Ordering::Relaxed);
    }
    pub fn add_pop(&self, tenant: &str, queue: &str, msgs: u64) {
        self.counters(&crate::handlers::tenant_queue_key(tenant, queue))
            .pop_count
            .fetch_add(msgs, Ordering::Relaxed);
    }
    pub fn add_pop_empty(&self, tenant: &str, queue: &str) {
        self.counters(&crate::handlers::tenant_queue_key(tenant, queue))
            .pop_empty
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn add_transaction(&self, tenant: &str, queue: &str) {
        self.counters(&crate::handlers::tenant_queue_key(tenant, queue))
            .transactions
            .fetch_add(1, Ordering::Relaxed);
    }
    /// Ack outcome counts for one ack call touching `queue` (ok/failed item counts).
    pub fn add_ack(&self, tenant: &str, queue: &str, ok: u64, failed: u64) {
        let c = self.counters(&crate::handlers::tenant_queue_key(tenant, queue));
        c.ack_requests.fetch_add(1, Ordering::Relaxed);
        c.ack_success.fetch_add(ok, Ordering::Relaxed);
        c.ack_failed.fetch_add(failed, Ordering::Relaxed);
    }
    /// Pop lag observed on one delivery: `sum_ms` across `n` messages + batch max.
    pub fn add_pop_lag(&self, tenant: &str, queue: &str, sum_ms: u64, max_ms: u64, n: u64) {
        if n == 0 {
            return;
        }
        let c = self.counters(&crate::handlers::tenant_queue_key(tenant, queue));
        c.lag_sum_ms.fetch_add(sum_ms, Ordering::Relaxed);
        c.lag_count.fetch_add(n, Ordering::Relaxed);
        c.lag_max_ms.fetch_max(max_ms, Ordering::Relaxed);
    }
    /// Snapshot every queue's cumulative counters (Relaxed loads — the collector
    /// diffs successive snapshots, so it only needs eventual monotone values).
    pub fn snapshot(&self) -> HashMap<String, QueueSnap> {
        self.map
            .read()
            .unwrap()
            .iter()
            .map(|(k, c)| {
                (
                    k.clone(),
                    QueueSnap {
                        push_requests: c.push_requests.load(Ordering::Relaxed),
                        push_messages: c.push_messages.load(Ordering::Relaxed),
                        pop_count: c.pop_count.load(Ordering::Relaxed),
                        pop_empty: c.pop_empty.load(Ordering::Relaxed),
                        transactions: c.transactions.load(Ordering::Relaxed),
                        ack_requests: c.ack_requests.load(Ordering::Relaxed),
                        ack_success: c.ack_success.load(Ordering::Relaxed),
                        ack_failed: c.ack_failed.load(Ordering::Relaxed),
                        lag_sum_ms: c.lag_sum_ms.load(Ordering::Relaxed),
                        lag_count: c.lag_count.load(Ordering::Relaxed),
                    },
                )
            })
            .collect()
    }
    /// Drain each queue's interval-max pop lag (swap to 0 so the next interval
    /// starts fresh). Called once per collector flush.
    pub fn take_lag_max(&self) -> HashMap<String, u64> {
        self.map
            .read()
            .unwrap()
            .iter()
            .filter_map(|(k, c)| {
                let v = c.lag_max_ms.swap(0, Ordering::Relaxed);
                if v > 0 {
                    Some((k.clone(), v))
                } else {
                    None
                }
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Parked long-poll gauge (dashboard "Parked" row + queen_queue_parked_consumers)
// ---------------------------------------------------------------------------
//
// C++ sampled the currently-parked long-poll POPs per queue at ~1Hz and flushed
// the minute-average into queue_lag_metrics.parked_count (a gauge: SUM across
// workers, AVG across buckets — see the parked_count gauge notes in
// 019_worker_metrics.sql). Here:
// each parked pop holds a ParkedGuard for the duration of its wait; a 1 Hz
// sampler (spawn_samplers) accumulates the instantaneous per-queue gauge, and
// syscollect drains sum/samples once per flush to compute the same average.
#[derive(Default)]
pub struct Parked {
    current: RwLock<HashMap<String, Arc<std::sync::atomic::AtomicI64>>>,
    acc: Mutex<HashMap<String, (u64, u32)>>, // queue -> (sum of samples, n samples)
}

pub struct ParkedGuard {
    gauge: Arc<std::sync::atomic::AtomicI64>,
}

impl Drop for ParkedGuard {
    fn drop(&mut self) {
        self.gauge.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Parked {
    /// Mark one pop as parked on `queue` until the returned guard drops.
    /// Track B (§5): keyed by (tenant, queue) so the per-tenant parked gauge and
    /// queue_parked_replica series don't merge two tenants sharing a queue name.
    pub fn enter(&self, tenant: &str, queue: &str) -> ParkedGuard {
        let gauge = self.gauge(&crate::handlers::tenant_queue_key(tenant, queue));
        gauge.fetch_add(1, Ordering::Relaxed);
        ParkedGuard { gauge }
    }

    // Early-return style is load-bearing: in edition 2021 an `if let/else`
    // scrutinee temporary (the read guard) lives across the else branch, so
    // taking the write lock there self-deadlocks the thread. Returning out of
    // the `if let` drops the read guard before the write() below.
    fn gauge(&self, key: &str) -> Arc<std::sync::atomic::AtomicI64> {
        if let Some(g) = self.current.read().unwrap().get(key) {
            return g.clone();
        }
        self.current
            .write()
            .unwrap()
            .entry(key.to_string())
            .or_default()
            .clone()
    }

    /// One 1 Hz sample: fold every queue's live gauge into the accumulator.
    /// Zero samples are skipped — the flush divides by elapsed seconds, so an
    /// absent queue naturally averages toward 0.
    fn sample(&self) {
        let live: Vec<(String, i64)> = self
            .current
            .read()
            .unwrap()
            .iter()
            .map(|(k, g)| (k.clone(), g.load(Ordering::Relaxed)))
            .collect();
        let mut acc = self.acc.lock().unwrap();
        for (q, v) in live {
            if v > 0 {
                let e = acc.entry(q).or_insert((0, 0));
                e.0 += v as u64;
                e.1 += 1;
            }
        }
    }

    /// Drain the per-queue accumulated (sum, samples) pairs for this interval.
    pub fn drain(&self) -> HashMap<String, (u64, u32)> {
        std::mem::take(&mut *self.acc.lock().unwrap())
    }

    /// Live instantaneous total per QUEUE (for the in-process Prometheus block).
    /// The map is keyed by (tenant, queue) composites; the operator gauge sums
    /// across tenants so the exposition keeps one line per queue name (flag OFF ⇒
    /// a single tenant ⇒ byte-identical, and never a duplicate `{queue=...}` set).
    pub fn live(&self) -> Vec<(String, i64)> {
        let mut agg: HashMap<String, i64> = HashMap::new();
        for (k, g) in self.current.read().unwrap().iter() {
            let v = g.load(Ordering::Relaxed).max(0);
            if v > 0 {
                let (_t, q) = crate::handlers::split_tenant_queue(k);
                *agg.entry(q.to_string()).or_insert(0) += v;
            }
        }
        agg.into_iter().collect()
    }
}

// ---------------------------------------------------------------------------
// KV + timers + sweeper series (PLAN_KV_TIMERS.md §14)
// ---------------------------------------------------------------------------
//
// THE CARDINALITY RULE, which this feature is the first in the product to risk
// for real (§14.1), and which is enforced here by TYPES rather than by review:
//
//   The `tenant` label is allowed ONLY on occupancy gauges — one series per
//   tenant, written by the sweeper, cardinality equal to the number of clusters
//   on the cell and bounded by the control plane. It is FORBIDDEN on per-operation
//   counters, where it would be tenant x op x outcome, i.e. cardinality chosen by
//   the user: the very disease the endpoint we are defending suffers from. The
//   per-tenant view of the hot path lives in the top-N log line and the JSON
//   endpoint, not in Prometheus.
//
// Every label below is a Rust enum with a fixed `as_str`, so no call site can mint
// a new label value; the ONE map keyed by a caller-supplied string is the fire-lag
// gauge, and it is capped and DENIES past the cap (never evicts — under an opaque,
// unvalidated tenant id, eviction is just a slower unbounded map).
//
// The motivated exception: `queen_timers_fire_lag_seconds` DOES carry `tenant`,
// because it is an occupancy gauge and because without it a backlog caused by one
// tenant does not name the culprit (§14.1, §6.2 fairness).
//
// NOT here, on purpose: `queen_kv_rows{tenant}`, `queen_kv_bytes{tenant}`,
// `queen_kv_quota_ratio{tenant,kind}` and `queen_timers_pending{tenant}`. Those
// come from the SLOW rollup in `queen.kv_usage` and are emitted by the cluster-plan
// block in `queen.get_prometheus_metrics_v1` (§14.5), which READS the rollup table
// and never counts the tables — a `count(*)` inside the Prometheus endpoint would
// run the rollup on every scrape.

/// A bounded ring of recent samples giving p50/p99 for free — the accumulation
/// pattern already in the house (`OpMetrics::rtt`), lifted out because §14 needs it
/// for three more families (kv op duration, sweeper cycle, timer fire lag).
pub struct Ring {
    v: Mutex<Vec<f64>>,
    head: AtomicU64,
}

impl Default for Ring {
    fn default() -> Self {
        Ring {
            v: Mutex::new(vec![0.0; RTT_CAP]),
            head: AtomicU64::new(0),
        }
    }
}

impl Ring {
    pub fn record(&self, ms: f64) {
        let h = (self.head.fetch_add(1, Ordering::Relaxed) as usize) % RTT_CAP;
        if let Ok(mut r) = self.v.lock() {
            r[h] = ms;
        }
    }
    pub fn percentile(&self, p: f64) -> f64 {
        let mut v = match self.v.lock() {
            Ok(g) => g.clone(),
            Err(_) => return 0.0,
        };
        v.retain(|&x| x > 0.0);
        if v.is_empty() {
            return 0.0;
        }
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = ((p / 100.0) * (v.len() - 1) as f64) as usize;
        v[idx.min(v.len() - 1)]
    }
}

/// The five KV code paths (§5: seven names, five code paths — `putIfAbsent` is an
/// alias that desugars to `put` with `expect:0` at the entry of `kv_apply_v1`, so it
/// is NOT a label of its own; one code path, one series).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum KvOp {
    Get,
    GetMany,
    GetPrefix,
    Put,
    Delete,
    Incr,
}
const KV_OPS: usize = 6;
impl KvOp {
    pub fn as_str(self) -> &'static str {
        match self {
            KvOp::Get => "get",
            KvOp::GetMany => "getMany",
            KvOp::GetPrefix => "getPrefix",
            KvOp::Put => "put",
            KvOp::Delete => "delete",
            KvOp::Incr => "incr",
        }
    }
    const ALL: [KvOp; KV_OPS] = [
        KvOp::Get,
        KvOp::GetMany,
        KvOp::GetPrefix,
        KvOp::Put,
        KvOp::Delete,
        KvOp::Incr,
    ];
}

/// `applied|rejected|error`, and the split is load-bearing: a lost precondition is
/// the EXPECTED outcome of every legitimate redelivery (§8.3), so it must be
/// `rejected` — a verdict — and must never be counted as an `error`, or the most
/// frequent outcome of the product's number-one use case reads as a fault.
#[derive(Clone, Copy)]
pub enum KvResult {
    Applied,
    Rejected,
    Error,
}
const KV_RESULTS: usize = 3;
impl KvResult {
    pub fn as_str(self) -> &'static str {
        match self {
            KvResult::Applied => "applied",
            KvResult::Rejected => "rejected",
            KvResult::Error => "error",
        }
    }
    const ALL: [KvResult; KV_RESULTS] = [KvResult::Applied, KvResult::Rejected, KvResult::Error];
}

/// Why a KV READ was turned away before it reached the database. `rate_limited` on a
/// tenant that was previously at zero is the earliest of the six pre-incident
/// signals (§14.3.1): it is not a fault, it is the advance warning of the one NEW
/// failure this feature introduces — a customer who has just put KV reads on their
/// own end users' path.
#[derive(Clone, Copy)]
pub enum KvReject {
    RateLimited,
    Quota,
    Pool,
    Disabled,
}
const KV_REJECTS: usize = 4;
impl KvReject {
    pub fn as_str(self) -> &'static str {
        match self {
            KvReject::RateLimited => "rate_limited",
            KvReject::Quota => "quota",
            KvReject::Pool => "pool",
            KvReject::Disabled => "disabled",
        }
    }
    const ALL: [KvReject; KV_REJECTS] = [
        KvReject::RateLimited,
        KvReject::Quota,
        KvReject::Pool,
        KvReject::Disabled,
    ];
}

/// Outcome of one fired segment. `stale` means another broker re-claimed the rows
/// while we were packing; `duplicate` means the timer's fixed `txn` was already in
/// the log, i.e. it had already been delivered.
#[derive(Clone, Copy)]
pub enum FireResult {
    Fired,
    Duplicate,
    Stale,
}
const FIRE_RESULTS: usize = 3;
impl FireResult {
    pub fn as_str(self) -> &'static str {
        match self {
            FireResult::Fired => "fired",
            FireResult::Duplicate => "duplicate",
            FireResult::Stale => "stale",
        }
    }
    const ALL: [FireResult; FIRE_RESULTS] =
        [FireResult::Fired, FireResult::Duplicate, FireResult::Stale];
}

/// The three SQLSTATE classes of the single broker-wide classifier (§7.6). `config`
/// is split out of `permanent` because "the destination queue name is malformed" is
/// not the same operational event as "this payload violates a constraint": an
/// operator can repair a `config`, nobody can repair a `permanent`, and only
/// `permanent`/`config` consume the timer's `attempts` budget.
#[derive(Clone, Copy)]
pub enum FireFailure {
    Transient,
    Permanent,
    Config,
}
const FIRE_FAILURES: usize = 3;
impl FireFailure {
    pub fn as_str(self) -> &'static str {
        match self {
            FireFailure::Transient => "transient",
            FireFailure::Permanent => "permanent",
            FireFailure::Config => "config",
        }
    }
    const ALL: [FireFailure; FIRE_FAILURES] = [
        FireFailure::Transient,
        FireFailure::Permanent,
        FireFailure::Config,
    ];
}

/// Why a timer SCHEDULE was refused. Mirrors the status/code table of §9.5, so a
/// spike here maps one-to-one onto what the caller saw.
#[derive(Clone, Copy)]
pub enum ScheduleReject {
    Shape,
    PayloadTooLarge,
    Quota,
    Horizon,
    Gated,
    RateLimited,
    Unavailable,
    Disabled,
}
const SCHEDULE_REJECTS: usize = 8;
impl ScheduleReject {
    pub fn as_str(self) -> &'static str {
        match self {
            ScheduleReject::Shape => "shape",
            ScheduleReject::PayloadTooLarge => "payload_too_large",
            ScheduleReject::Quota => "quota",
            ScheduleReject::Horizon => "horizon",
            ScheduleReject::Gated => "gated",
            ScheduleReject::RateLimited => "rate_limited",
            ScheduleReject::Unavailable => "unavailable",
            ScheduleReject::Disabled => "disabled",
        }
    }
    const ALL: [ScheduleReject; SCHEDULE_REJECTS] = [
        ScheduleReject::Shape,
        ScheduleReject::PayloadTooLarge,
        ScheduleReject::Quota,
        ScheduleReject::Horizon,
        ScheduleReject::Gated,
        ScheduleReject::RateLimited,
        ScheduleReject::Unavailable,
        ScheduleReject::Disabled,
    ];
}

/// The sweeper's three phases, in the priority order it sheds them (§12.1): fire
/// first (never shed automatically), then the usage rollup, then the KV prune. The
/// `phase_skipped` counter is what makes the degradation ladder VISIBLE instead of
/// inferred from a hole in another series.
#[derive(Clone, Copy)]
pub enum SweepPhase {
    Fire,
    KvExpire,
    Usage,
}
const SWEEP_PHASES: usize = 3;
impl SweepPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            SweepPhase::Fire => "fire",
            SweepPhase::KvExpire => "kv_expire",
            SweepPhase::Usage => "usage",
        }
    }
    const ALL: [SweepPhase; SWEEP_PHASES] =
        [SweepPhase::Fire, SweepPhase::KvExpire, SweepPhase::Usage];
}

/// Ceiling on distinct tenants in the fire-lag gauge. Past it the gauge DENIES the
/// new tenant (it keeps serving the ones it has) rather than evicting: the tenant id
/// is opaque and unvalidated (§9.4), so an evicting map is an unbounded map with
/// extra steps, and a scrape whose label set is chosen by a caller is the exact
/// cardinality failure §14.1 forbids. `queen_timers_fire_lag_tenants_dropped` (not a
/// plan series, a self-diagnostic) says when the cap is biting.
const FIRE_LAG_TENANT_CAP: usize = 1024;

/// The kv/timers series. NO EXPOSITION GATE: this block used to be suppressed when
/// the two boot flags were off, so a broker that never turned them on scraped
/// byte-identically to the pre-feature broker. The flags are gone (see the header of
/// `switches.rs`), so the series are ALWAYS emitted — a dashboard or an alert rule
/// may now assume `queen_kv_ops_total` exists on every cell, which is the point.
/// Zeros are the truthful reading for a cell nobody has written a key to.
#[derive(Default)]
pub struct KvTimers {
    // ---- kv ------------------------------------------------------------------
    kv_ops: [[AtomicU64; KV_RESULTS]; KV_OPS],
    kv_dur: [Ring; KV_OPS],
    kv_bytes_in: AtomicU64,
    kv_bytes_out: AtomicU64,
    kv_read_rejected: [AtomicU64; KV_REJECTS],
    kv_singleflight_coalesced: AtomicU64,
    /// Expired-but-not-yet-pruned rows, CAPPED at the SQL level: the reported value
    /// saturates and `kv_expired_not_pruned_capped` says so, because an exact count
    /// is O(backlog) precisely in the failure it exists to detect. This is the one
    /// signal that separates "sweeper behind" from "all well" in a failure mode that
    /// DISGUISES ITSELF AS SUCCESS — reads stay perfectly correct (expiry is a
    /// predicate, not the physical absence of the row) while the table grows (§14.3.4).
    kv_expired_not_pruned: AtomicI64,
    kv_expired_not_pruned_capped: AtomicI64,
    kv_expiry_lag_ms: AtomicI64,
    kv_pool_size: AtomicI64,
    kv_pool_available: AtomicI64,
    kv_pool_waiting: AtomicI64,

    // ---- timers --------------------------------------------------------------
    /// From the due probe, and CAPPED there for the same reason as above.
    timers_due: AtomicI64,
    timers_due_capped: AtomicI64,
    timers_oldest_late_ms: AtomicI64,
    timers_fired: [AtomicU64; FIRE_RESULTS],
    timers_dlq: AtomicU64,
    timers_fire_failures: [AtomicU64; FIRE_FAILURES],
    timers_poisoned: AtomicU64,
    timers_schedule_rejected: [AtomicU64; SCHEDULE_REJECTS],
    /// tenant -> ring of fire-lag samples (ms). The one string-keyed map here; see
    /// FIRE_LAG_TENANT_CAP.
    fire_lag: RwLock<HashMap<String, Arc<Ring>>>,
    fire_lag_dropped: AtomicU64,

    // ---- sweeper -------------------------------------------------------------
    sweeper_cycle: [Ring; SWEEP_PHASES],
    sweeper_rows: [AtomicU64; SWEEP_PHASES],
    sweeper_skip_locked: AtomicU64,
    sweeper_phase_skipped: [AtomicU64; SWEEP_PHASES],
    sweeper_sleep_ms: AtomicI64,

    /// Last per-tenant occupancy snapshot published by the sweeper's slow rollup
    /// (§7.5). It is the ONLY per-tenant view in this struct besides the fire lag,
    /// and it is legitimate for the same reason: one row per tenant, written by the
    /// sweeper, bounded by the control plane rather than by callers. `queen_kv_rows`,
    /// `queen_kv_bytes`, `queen_kv_quota_ratio` and `queen_timers_pending` are
    /// EXPOSED from the cluster-plan block that reads `queen.kv_usage` directly
    /// (§14.5) — this copy exists for the `sizes` block and the top-N log lines, so
    /// that an incident has the numbers in the log next to everything else.
    usage: RwLock<Vec<TenantUsage>>,
}

/// One tenant's row in the slow rollup. `kv_bytes` is an ESTIMATE and is labelled as
/// such wherever it is shown: it is `pg_column_size(k.value)` on the COLUMN (never on
/// a whole-row var, which would either detoast every value in the table every cycle
/// or count an 18-byte external pointer instead of the value and so under-count
/// exactly the large rows the quota exists to bound — §7.5).
#[derive(Clone, Default)]
pub struct TenantUsage {
    pub tenant: String,
    pub kv_rows: i64,
    pub kv_bytes: i64,
    pub timers_pending: i64,
    /// Occupancy over quota, 0.0 when the tenant has no quota row (unlimited).
    /// 0.8 is already late with a soft quota (§14.3.5).
    pub kv_quota_ratio: f64,
    pub timers_quota_ratio: f64,
}

impl KvTimers {
    // ---- recording (hot path: one relaxed add, or one add plus a ring slot) ----

    /// One KV operation with its outcome and its own duration.
    pub fn kv_op(&self, op: KvOp, result: KvResult, ms: f64) {
        self.kv_ops[op as usize][result as usize].fetch_add(1, Ordering::Relaxed);
        self.kv_dur[op as usize].record(ms);
    }
    /// Value bytes in (written) / out (read). The byte is the real resource (§6.1
    /// point 4), so it gets its own counter and is not inferred from op counts.
    pub fn kv_bytes(&self, r#in: u64, out: u64) {
        if r#in > 0 {
            self.kv_bytes_in.fetch_add(r#in, Ordering::Relaxed);
        }
        if out > 0 {
            self.kv_bytes_out.fetch_add(out, Ordering::Relaxed);
        }
    }
    pub fn kv_read_rejected(&self, why: KvReject) {
        self.kv_read_rejected[why as usize].fetch_add(1, Ordering::Relaxed);
    }
    /// Two in-flight `GET`s for the same `(tenant, ns, key)` shared one query. The
    /// ONLY safe amplification mechanism for this store (a cached VALUE is forbidden
    /// outright, §8.5) — and also the tell that a customer has put KV on their web
    /// path, which is why it is a series of its own.
    ///
    /// NO CALLER YET. Single-flight is one of the two §8.4 defences that own live
    /// state (the other is the dedicated KV pool, `set_kv_pool` below); both are
    /// declared-not-implemented in this pass and live on `AppState`, not here. The
    /// counter and the exported series exist already so that turning the defence on
    /// is a change in one module rather than a change in three — and so that the
    /// series name is fixed BEFORE any dashboard is built on it.
    #[allow(dead_code)]
    pub fn kv_singleflight_coalesced(&self, n: u64) {
        if n > 0 {
            self.kv_singleflight_coalesced.fetch_add(n, Ordering::Relaxed);
        }
    }
    pub fn set_kv_expiry(&self, unpruned: i64, capped: bool, lag_ms: i64) {
        self.kv_expired_not_pruned.store(unpruned, Ordering::Relaxed);
        self.kv_expired_not_pruned_capped
            .store(capped as i64, Ordering::Relaxed);
        self.kv_expiry_lag_ms.store(lag_ms, Ordering::Relaxed);
    }
    /// NO CALLER YET — see `kv_singleflight_coalesced`. The dedicated KV pool of
    /// §8.4 point 1 is the defence that turns a slow database into 503s on the KV
    /// surface instead of into connections stolen from the message path, and it is
    /// the one whose gauges an operator needs FIRST during that incident. The
    /// three series are exported now so the gauge exists the day the pool does.
    #[allow(dead_code)]
    pub fn set_kv_pool(&self, size: i64, available: i64, waiting: i64) {
        self.kv_pool_size.store(size, Ordering::Relaxed);
        self.kv_pool_available.store(available, Ordering::Relaxed);
        self.kv_pool_waiting.store(waiting, Ordering::Relaxed);
    }

    pub fn set_timers_due(&self, due: i64, capped: bool, oldest_late_ms: i64) {
        self.timers_due.store(due, Ordering::Relaxed);
        self.timers_due_capped.store(capped as i64, Ordering::Relaxed);
        self.timers_oldest_late_ms
            .store(oldest_late_ms, Ordering::Relaxed);
    }
    pub fn timers_fired(&self, result: FireResult, n: u64) {
        if n > 0 {
            self.timers_fired[result as usize].fetch_add(n, Ordering::Relaxed);
        }
    }
    pub fn timers_dlq(&self, n: u64) {
        if n > 0 {
            self.timers_dlq.fetch_add(n, Ordering::Relaxed);
        }
    }
    pub fn timers_fire_failure(&self, class: FireFailure) {
        self.timers_fire_failures[class as usize].fetch_add(1, Ordering::Relaxed);
    }
    pub fn timers_poisoned(&self, n: u64) {
        if n > 0 {
            self.timers_poisoned.fetch_add(n, Ordering::Relaxed);
        }
    }
    pub fn timers_schedule_rejected(&self, why: ScheduleReject) {
        self.timers_schedule_rejected[why as usize].fetch_add(1, Ordering::Relaxed);
    }
    /// One delivery's lateness (ms between due and fire), attributed to its tenant.
    /// The value comes from the SERVER (`r_late_ms`): the broker never does timestamp
    /// arithmetic, there is one clock and it is Postgres's (§4.2).
    pub fn fire_lag(&self, tenant: &str, late_ms: f64) {
        if let Some(r) = self.fire_lag.read().unwrap().get(tenant) {
            r.record(late_ms);
            return;
        }
        let mut w = self.fire_lag.write().unwrap();
        if w.len() >= FIRE_LAG_TENANT_CAP && !w.contains_key(tenant) {
            drop(w);
            self.fire_lag_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        w.entry(tenant.to_string()).or_default().record(late_ms);
    }

    pub fn sweeper_cycle(&self, phase: SweepPhase, ms: f64, rows: u64) {
        self.sweeper_cycle[phase as usize].record(ms);
        if rows > 0 {
            self.sweeper_rows[phase as usize].fetch_add(rows, Ordering::Relaxed);
        }
    }
    pub fn sweeper_skip_locked(&self, n: u64) {
        if n > 0 {
            self.sweeper_skip_locked.fetch_add(n, Ordering::Relaxed);
        }
    }
    pub fn sweeper_phase_skipped(&self, phase: SweepPhase) {
        self.sweeper_phase_skipped[phase as usize].fetch_add(1, Ordering::Relaxed);
    }
    pub fn set_sweeper_sleep(&self, ms: i64) {
        self.sweeper_sleep_ms.store(ms, Ordering::Relaxed);
    }

    // ---- reads, for the log blocks (obs.rs) -----------------------------------

    /// Every KV op, whatever the outcome — the numerator of `kv_ops_s`.
    pub fn kv_ops_total(&self) -> u64 {
        let mut n = 0;
        for o in 0..KV_OPS {
            for r in 0..KV_RESULTS {
                n += self.kv_ops[o][r].load(Ordering::Relaxed);
            }
        }
        n
    }
    pub fn kv_rejected_total(&self) -> u64 {
        self.kv_read_rejected
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum()
    }
    /// Worst p99 across the code paths. One number for the log line; Prometheus keeps
    /// the per-op split.
    pub fn kv_duration_p99(&self) -> f64 {
        self.kv_dur
            .iter()
            .map(|r| r.percentile(99.0))
            .fold(0.0, f64::max)
    }
    pub fn timers_fired_total(&self) -> u64 {
        self.timers_fired
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum()
    }
    pub fn timers_due(&self) -> i64 {
        self.timers_due.load(Ordering::Relaxed)
    }
    pub fn kv_expired_not_pruned(&self) -> i64 {
        self.kv_expired_not_pruned.load(Ordering::Relaxed)
    }
    pub fn kv_pool(&self) -> (i64, i64) {
        (
            self.kv_pool_size.load(Ordering::Relaxed) - self.kv_pool_available.load(Ordering::Relaxed),
            self.kv_pool_size.load(Ordering::Relaxed),
        )
    }
    /// Publish a rollup pass. Last-writer-wins, exactly like the `computed_at`
    /// discipline of the `queen.kv_usage` row it mirrors.
    pub fn set_usage(&self, rows: Vec<TenantUsage>) {
        *self.usage.write().unwrap() = rows;
    }
    /// Cell-wide totals for the `sizes` block: (kv rows, kv bytes, timers pending).
    pub fn usage_totals(&self) -> (i64, i64, i64) {
        let u = self.usage.read().unwrap();
        (
            u.iter().map(|t| t.kv_rows).sum(),
            u.iter().map(|t| t.kv_bytes).sum(),
            u.iter().map(|t| t.timers_pending).sum(),
        )
    }
    /// The `n` tenants worth a log line, ranked by the thing that actually causes an
    /// incident: how close they are to a quota first, then raw occupancy.
    pub fn usage_top(&self, n: usize) -> (Vec<TenantUsage>, usize) {
        let mut v: Vec<TenantUsage> = self.usage.read().unwrap().clone();
        let total = v.len();
        v.sort_by(|a, b| {
            let ka = a.kv_quota_ratio.max(a.timers_quota_ratio);
            let kb = b.kv_quota_ratio.max(b.timers_quota_ratio);
            kb.partial_cmp(&ka)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then((b.kv_rows + b.timers_pending).cmp(&(a.kv_rows + a.timers_pending)))
        });
        v.truncate(n);
        (v, total)
    }
    /// Worst p95 fire lag across tenants, in SECONDS — the §14.3.3 signal (warn above
    /// 5 s), collapsed for the log line.
    pub fn fire_lag_p95_seconds(&self) -> f64 {
        self.fire_lag
            .read()
            .unwrap()
            .values()
            .map(|r| r.percentile(95.0))
            .fold(0.0, f64::max)
            / 1000.0
    }
    /// One tenant's p95 fire lag, for the per-tenant log line — the whole reason the
    /// gauge carries `tenant` at all: a backlog that does not name its culprit puts
    /// every tenant on the same alert.
    pub fn fire_lag_p95_seconds_of(&self, tenant: &str) -> f64 {
        self.fire_lag
            .read()
            .unwrap()
            .get(tenant)
            .map(|r| r.percentile(95.0) / 1000.0)
            .unwrap_or(0.0)
    }

    fn render(&self, s: &mut String, ht: &dyn Fn(&mut String, &str, &str, &str), g: &dyn Fn(&mut String, &str, &str, String)) {
        ht(s, "queen_kv_ops_total", "KV operations by code path and outcome", "counter");
        for op in KvOp::ALL {
            for res in KvResult::ALL {
                g(
                    s,
                    "queen_kv_ops_total",
                    &format!("{{op=\"{}\",result=\"{}\"}}", op.as_str(), res.as_str()),
                    self.kv_ops[op as usize][res as usize]
                        .load(Ordering::Relaxed)
                        .to_string(),
                );
            }
        }
        ht(s, "queen_kv_op_duration_milliseconds", "KV operation latency", "gauge");
        for op in KvOp::ALL {
            for (q, p) in [("0.5", 50.0), ("0.99", 99.0)] {
                g(
                    s,
                    "queen_kv_op_duration_milliseconds",
                    &format!("{{op=\"{}\",quantile=\"{}\"}}", op.as_str(), q),
                    format!("{:.3}", self.kv_dur[op as usize].percentile(p)),
                );
            }
        }
        ht(s, "queen_kv_bytes_total", "KV value bytes written / read", "counter");
        g(s, "queen_kv_bytes_total", "{dir=\"in\"}", self.kv_bytes_in.load(Ordering::Relaxed).to_string());
        g(s, "queen_kv_bytes_total", "{dir=\"out\"}", self.kv_bytes_out.load(Ordering::Relaxed).to_string());
        ht(s, "queen_kv_expired_not_pruned", "Expired KV rows the sweeper has not pruned yet (capped)", "gauge");
        g(s, "queen_kv_expired_not_pruned", "", self.kv_expired_not_pruned.load(Ordering::Relaxed).to_string());
        ht(s, "queen_kv_expired_not_pruned_capped", "1 when the unpruned count hit its cap and is a floor", "gauge");
        g(s, "queen_kv_expired_not_pruned_capped", "", self.kv_expired_not_pruned_capped.load(Ordering::Relaxed).to_string());
        ht(s, "queen_kv_expiry_lag_seconds", "Age of the oldest expired, unpruned KV row", "gauge");
        g(s, "queen_kv_expiry_lag_seconds", "", format!("{:.3}", self.kv_expiry_lag_ms.load(Ordering::Relaxed) as f64 / 1000.0));
        ht(s, "queen_kv_read_rejected_total", "KV reads refused before reaching the database", "counter");
        for why in KvReject::ALL {
            g(
                s,
                "queen_kv_read_rejected_total",
                &format!("{{reason=\"{}\"}}", why.as_str()),
                self.kv_read_rejected[why as usize].load(Ordering::Relaxed).to_string(),
            );
        }
        ht(s, "queen_kv_pool", "Dedicated KV connection pool", "gauge");
        g(s, "queen_kv_pool", "{state=\"size\"}", self.kv_pool_size.load(Ordering::Relaxed).to_string());
        g(s, "queen_kv_pool", "{state=\"available\"}", self.kv_pool_available.load(Ordering::Relaxed).to_string());
        g(s, "queen_kv_pool", "{state=\"waiting\"}", self.kv_pool_waiting.load(Ordering::Relaxed).to_string());
        ht(s, "queen_kv_singleflight_coalesced_total", "KV reads that shared an in-flight query", "counter");
        g(s, "queen_kv_singleflight_coalesced_total", "", self.kv_singleflight_coalesced.load(Ordering::Relaxed).to_string());
        ht(s, "queen_timers_due", "Timers due now, from the sweep probe (capped)", "gauge");
        g(s, "queen_timers_due", "", self.timers_due.load(Ordering::Relaxed).to_string());
        ht(s, "queen_timers_due_capped", "1 when the due count hit its cap and is a floor", "gauge");
        g(s, "queen_timers_due_capped", "", self.timers_due_capped.load(Ordering::Relaxed).to_string());
        ht(s, "queen_timers_oldest_late_seconds", "Lateness of the oldest due timer", "gauge");
        g(s, "queen_timers_oldest_late_seconds", "", format!("{:.3}", self.timers_oldest_late_ms.load(Ordering::Relaxed) as f64 / 1000.0));
        // The motivated exception to the cardinality rule (§14.1): an occupancy
        // gauge, and the only series that names which tenant caused a backlog.
        ht(s, "queen_timers_fire_lag_seconds", "Delivery lateness of fired timers, per tenant", "gauge");
        for (tenant, ring) in self.fire_lag.read().unwrap().iter() {
            for (q, p) in [("0.5", 50.0), ("0.95", 95.0)] {
                g(
                    s,
                    "queen_timers_fire_lag_seconds",
                    &format!("{{tenant=\"{}\",quantile=\"{}\"}}", escape_label(tenant), q),
                    format!("{:.3}", ring.percentile(p) / 1000.0),
                );
            }
        }
        ht(s, "queen_timers_fire_lag_tenants_dropped_total", "Fire-lag samples dropped because the tenant cap was reached", "counter");
        g(s, "queen_timers_fire_lag_tenants_dropped_total", "", self.fire_lag_dropped.load(Ordering::Relaxed).to_string());
        ht(s, "queen_timers_fired_total", "Fired timer segments by outcome", "counter");
        for r in FireResult::ALL {
            g(s, "queen_timers_fired_total", &format!("{{result=\"{}\"}}", r.as_str()), self.timers_fired[r as usize].load(Ordering::Relaxed).to_string());
        }
        ht(s, "queen_timers_dlq_total", "Timers dead-lettered after exhausting attempts", "counter");
        g(s, "queen_timers_dlq_total", "", self.timers_dlq.load(Ordering::Relaxed).to_string());
        ht(s, "queen_timers_fire_failures_total", "Failed fire transactions by SQLSTATE class", "counter");
        for c in FireFailure::ALL {
            g(s, "queen_timers_fire_failures_total", &format!("{{class=\"{}\"}}", c.as_str()), self.timers_fire_failures[c as usize].load(Ordering::Relaxed).to_string());
        }
        ht(s, "queen_timers_poisoned_total", "Batches replayed one segment per call after a permanent error", "counter");
        g(s, "queen_timers_poisoned_total", "", self.timers_poisoned.load(Ordering::Relaxed).to_string());
        ht(s, "queen_timers_schedule_rejected_total", "Timer schedules refused", "counter");
        for w in ScheduleReject::ALL {
            g(s, "queen_timers_schedule_rejected_total", &format!("{{reason=\"{}\"}}", w.as_str()), self.timers_schedule_rejected[w as usize].load(Ordering::Relaxed).to_string());
        }
        // The sweeper serves both surfaces; the phase labels say which half did the
        // work.
        ht(s, "queen_sweeper_cycle_milliseconds", "Sweeper phase duration", "gauge");
        for ph in SweepPhase::ALL {
            for (q, p) in [("0.5", 50.0), ("0.99", 99.0)] {
                g(
                    s,
                    "queen_sweeper_cycle_milliseconds",
                    &format!("{{phase=\"{}\",quantile=\"{}\"}}", ph.as_str(), q),
                    format!("{:.3}", self.sweeper_cycle[ph as usize].percentile(p)),
                );
            }
        }
        ht(s, "queen_sweeper_rows_total", "Rows handled by each sweeper phase", "counter");
        for ph in SweepPhase::ALL {
            g(s, "queen_sweeper_rows_total", &format!("{{phase=\"{}\"}}", ph.as_str()), self.sweeper_rows[ph as usize].load(Ordering::Relaxed).to_string());
        }
        ht(s, "queen_sweeper_skip_locked_total", "Rows another broker was already holding", "counter");
        g(s, "queen_sweeper_skip_locked_total", "", self.sweeper_skip_locked.load(Ordering::Relaxed).to_string());
        ht(s, "queen_sweeper_phase_skipped_total", "Phases shed under pressure (the degradation ladder, made visible)", "counter");
        for ph in SweepPhase::ALL {
            g(s, "queen_sweeper_phase_skipped_total", &format!("{{phase=\"{}\"}}", ph.as_str()), self.sweeper_phase_skipped[ph as usize].load(Ordering::Relaxed).to_string());
        }
        ht(s, "queen_sweeper_sleep_milliseconds", "Sleep the sweeper chose after the last cycle", "gauge");
        g(s, "queen_sweeper_sleep_milliseconds", "", self.sweeper_sleep_ms.load(Ordering::Relaxed).to_string());
    }
}

/// Prometheus label-value escaping. The tenant id is caller-supplied and opaque, so
/// it is escaped rather than trusted — an unescaped quote does not corrupt one line,
/// it corrupts the whole exposition from that point on.
fn escape_label(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

impl Metrics {
    /// One database failure on a data path (statement error, statement timeout,
    /// or pool acquisition failure). Feeds worker_metrics.db_error_count and the
    /// dashboard "DB errors" series.
    #[inline]
    pub fn record_db_error(&self) {
        self.db_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// `n` database failures at once (e.g. every item of a push whose commit
    /// failed). A no-op for n == 0.
    #[inline]
    pub fn record_db_errors(&self, n: u64) {
        if n > 0 {
            self.db_errors.fetch_add(n, Ordering::Relaxed);
        }
    }

    pub fn new() -> Metrics {
        Metrics {
            push: std::sync::Arc::new(OpMetrics::new("push")),
            pop: std::sync::Arc::new(OpMetrics::new("pop")),
            ack: std::sync::Arc::new(OpMetrics::new("ack")),
            transactions: AtomicU64::new(0),
            ack_success: AtomicU64::new(0),
            ack_failed: AtomicU64::new(0),
            dlq_moved: AtomicU64::new(0),
            db_errors: AtomicU64::new(0),
            pop_targeted: AtomicU64::new(0),
            pop_wildcard: AtomicU64::new(0),
            pop_fill_wait: AtomicU64::new(0),
            pop_fill_wait_us: AtomicU64::new(0),
            per_queue: PerQueue::default(),
            parked: Parked::default(),
            evl_sum_us: AtomicU64::new(0),
            evl_count: AtomicU64::new(0),
            evl_max_us: AtomicU64::new(0),
            kvt: KvTimers::default(),
            start: Instant::now(),
        }
    }

    /// Snapshot the cumulative counters (Relaxed loads — the collector only needs
    /// eventual, monotone values to diff, not a consistent cross-counter instant).
    pub fn snapshot(&self) -> Counters {
        Counters {
            push_requests: self.push.requests.load(Ordering::Relaxed),
            push_messages: self.push.messages.load(Ordering::Relaxed),
            pop_requests: self.pop.requests.load(Ordering::Relaxed),
            pop_messages: self.pop.messages.load(Ordering::Relaxed),
            ack_requests: self.ack.requests.load(Ordering::Relaxed),
            ack_messages: self.ack.messages.load(Ordering::Relaxed),
            ack_success: self.ack_success.load(Ordering::Relaxed),
            ack_failed: self.ack_failed.load(Ordering::Relaxed),
            transactions: self.transactions.load(Ordering::Relaxed),
            dlq_moved: self.dlq_moved.load(Ordering::Relaxed),
            db_errors: self.db_errors.load(Ordering::Relaxed),
        }
    }

    /// RUSTFIX item 21: accessors so the JSON /metrics handler can read the private
    /// process state (uptime + RSS).
    pub fn uptime_seconds(&self) -> u64 {
        self.start.elapsed().as_secs()
    }
    pub fn resident_bytes(&self) -> u64 {
        resident_bytes()
    }

    pub fn prometheus(&self) -> String {
        let mut s = String::with_capacity(2048);
        // RUSTFIX item 24: emit `# HELP`/`# TYPE` before every family.
        let ht = |s: &mut String, name: &str, help: &str, typ: &str| {
            s.push_str("# HELP ");
            s.push_str(name);
            s.push(' ');
            s.push_str(help);
            s.push_str("\n# TYPE ");
            s.push_str(name);
            s.push(' ');
            s.push_str(typ);
            s.push('\n');
        };
        let g = |s: &mut String, name: &str, labels: &str, v: String| {
            s.push_str(name);
            s.push_str(labels);
            s.push(' ');
            s.push_str(&v);
            s.push('\n');
        };
        ht(&mut s, "queen_uptime_seconds", "Process uptime in seconds", "gauge");
        g(&mut s, "queen_uptime_seconds", "", (self.start.elapsed().as_secs()).to_string());
        ht(&mut s, "queen_process_resident_memory_bytes", "Resident memory of this process", "gauge");
        g(&mut s, "queen_process_resident_memory_bytes", "", resident_bytes().to_string());
        // RUSTFIX item 24: these are PER-PROCESS counters (reset on restart), so they
        // are named queen_process_* — the queen_cluster_* namespace is reclaimed by
        // the DB-backed lifetime totals in status.rs (which survive restart).
        let process = [
            ("queen_process_push_requests_total", "Push API requests handled by this process", &self.push.requests),
            ("queen_process_pop_requests_total", "Pop API requests handled by this process", &self.pop.requests),
            ("queen_process_ack_requests_total", "Ack API requests handled by this process", &self.ack.requests),
            ("queen_process_push_messages_total", "Messages pushed by this process", &self.push.messages),
            ("queen_process_pop_messages_total", "Messages popped by this process", &self.pop.messages),
            ("queen_process_ack_messages_total", "Messages acked by this process", &self.ack.messages),
        ];
        for (name, help, ctr) in process {
            ht(&mut s, name, help, "counter");
            g(&mut s, name, "", ctr.load(Ordering::Relaxed).to_string());
        }
        // Pop path split (Phase 2): targeted hint-driven pops vs wildcard scans.
        ht(&mut s, "queen_pop_targeted_total", "Hinted targeted single-partition pops issued", "counter");
        g(&mut s, "queen_pop_targeted_total", "", self.pop_targeted.load(Ordering::Relaxed).to_string());
        ht(&mut s, "queen_pop_wildcard_total", "Wildcard candidate-scan pops issued", "counter");
        g(&mut s, "queen_pop_wildcard_total", "", self.pop_wildcard.load(Ordering::Relaxed).to_string());
        ht(&mut s, "queen_pop_fill_wait_total", "Pops that held an under-full batch back (minPopWaitTime)", "counter");
        g(&mut s, "queen_pop_fill_wait_total", "", self.pop_fill_wait.load(Ordering::Relaxed).to_string());
        ht(&mut s, "queen_pop_fill_wait_microseconds_total", "Total time pops spent fattening an under-full batch", "counter");
        g(&mut s, "queen_pop_fill_wait_microseconds_total", "", self.pop_fill_wait_us.load(Ordering::Relaxed).to_string());
        // Live parked long-polls per queue (instantaneous; the DB-backed
        // queen_queue_parked_consumers in status.rs is the minute-average).
        ht(&mut s, "queen_parked_long_polls", "Currently parked long-poll pops on this process", "gauge");
        for (q, v) in self.parked.live() {
            let mut lbl = String::from("{queue=\"");
            for c in q.chars() {
                match c {
                    '\\' => lbl.push_str("\\\\"),
                    '"' => lbl.push_str("\\\""),
                    '\n' => lbl.push_str("\\n"),
                    _ => lbl.push(c),
                }
            }
            lbl.push_str("\"}");
            g(&mut s, "queen_parked_long_polls", &lbl, v.to_string());
        }
        // Scheduler lag (the worker_metrics event-loop columns carry the
        // per-minute view; this is the live cumulative probe state).
        let evl_n = self.evl_count.load(Ordering::Relaxed);
        let evl_avg_ms = if evl_n > 0 {
            self.evl_sum_us.load(Ordering::Relaxed) as f64 / evl_n as f64 / 1000.0
        } else {
            0.0
        };
        ht(&mut s, "queen_event_loop_lag_avg_milliseconds", "Mean scheduler (event-loop) lag since start", "gauge");
        g(&mut s, "queen_event_loop_lag_avg_milliseconds", "", format!("{:.3}", evl_avg_ms));
        ht(&mut s, "queen_batches_fired_total", "Fusion batches flushed", "counter");
        ht(&mut s, "queen_batch_items_fired_total", "Items flushed across fusion batches", "counter");
        ht(&mut s, "queen_fusion_items_per_batch", "Mean items per fusion batch", "gauge");
        ht(&mut s, "queen_batch_rtt_milliseconds", "Fusion batch round-trip latency", "gauge");
        for op in [&self.push, &self.pop, &self.ack] {
            let lbl = format!("{{op=\"{}\"}}", op.name);
            g(&mut s, "queen_batches_fired_total", &lbl, op.batches_fired.load(Ordering::Relaxed).to_string());
            g(&mut s, "queen_batch_items_fired_total", &lbl, op.items_fired.load(Ordering::Relaxed).to_string());
            let bf = op.batches_fired.load(Ordering::Relaxed);
            let ratio = if bf > 0 { op.items_fired.load(Ordering::Relaxed) as f64 / bf as f64 } else { 0.0 };
            g(&mut s, "queen_fusion_items_per_batch", &lbl, format!("{:.2}", ratio));
            g(&mut s, "queen_batch_rtt_milliseconds", &format!("{{op=\"{}\",quantile=\"0.5\"}}", op.name), format!("{:.3}", op.rtt_percentile(50.0)));
            g(&mut s, "queen_batch_rtt_milliseconds", &format!("{{op=\"{}\",quantile=\"0.99\"}}", op.name), format!("{:.3}", op.rtt_percentile(99.0)));
        }
        // PLAN_KV_TIMERS.md §14.2. Appended LAST, and UNCONDITIONALLY: the block used
        // to be suppressed while the two boot flags were off, so that a broker which
        // never enabled them scraped byte-identically to the pre-feature broker. The
        // flags are gone, so the parity that mattered is the other one — every cell
        // running this binary exposes the same series, and an alert rule can be
        // written once for the fleet.
        self.kvt.render(&mut s, &ht, &g);
        s
    }
}

/// Spawn the background samplers feeding the dashboard-facing gauges:
///  * a 100 ms scheduler-lag probe (sleep-overshoot => "event loop" lag), and
///  * a 1 Hz parked-long-poll sampler (minute-averaged into queue_lag_metrics
///    by syscollect).
/// Both are tiny (two atomic ops / a map scan per tick).
pub fn spawn_samplers(metrics: Arc<Metrics>) {
    // Scheduler-lag probe. sleep(100ms) resolving late == the runtime (or the
    // host) was too busy to run a ready timer — the same signal Node's
    // monitorEventLoopDelay gives, which the C++ server reported per worker.
    {
        let m = metrics.clone();
        tokio::spawn(async move {
            const TICK: Duration = Duration::from_millis(100);
            loop {
                let t0 = Instant::now();
                tokio::time::sleep(TICK).await;
                let lag = t0.elapsed().saturating_sub(TICK);
                let us = lag.as_micros() as u64;
                m.evl_sum_us.fetch_add(us, Ordering::Relaxed);
                m.evl_count.fetch_add(1, Ordering::Relaxed);
                m.evl_max_us.fetch_max(us, Ordering::Relaxed);
            }
        });
    }
    // Parked 1 Hz sampler.
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            metrics.parked.sample();
        }
    });
}

fn resident_bytes() -> u64 {
    if let Ok(data) = std::fs::read_to_string("/proc/self/statm") {
        if let Some(field) = data.split_whitespace().nth(1) {
            if let Ok(pages) = field.parse::<u64>() {
                return pages * 4096;
            }
        }
    }
    0
}

#[cfg(test)]
mod kv_timers_tests {
    use super::*;

    /// Two renders of the same `Metrics` are NOT byte-identical on their own, and the
    /// reason is not kv or timers: `queen_uptime_seconds` and
    /// `queen_process_resident_memory_bytes` are live samples of the process taken at
    /// render time, and the second render happens after the recording calls below have
    /// allocated. On Linux `resident_bytes()` reads /proc/self/statm and the two
    /// renders differ by whatever the allocator did in between; on macOS there is no
    /// /proc, the gauge reads 0 both times, and the difference is invisible. Comparing
    /// them raw is therefore a test that is green on the author's laptop and red in
    /// CI — which is exactly how it was found — for a reason that has nothing to do
    /// with the property it claims to hold. So the two live samples are normalised to
    /// their family name and everything else, every family and label set and value,
    /// still has to match byte for byte.
    fn without_live_process_samples(exposition: &str) -> String {
        const LIVE: [&str; 2] = ["queen_uptime_seconds", "queen_process_resident_memory_bytes"];
        exposition
            .lines()
            .map(|line| match LIVE.iter().find(|n| line.starts_with(&format!("{n} "))) {
                Some(name) => format!("{name} <live sample>"),
                None => line.to_string(),
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// THE INVERSE OF THE OLD GATE, and deliberately so. §15 used to demand that a
    /// broker with both boot flags off scrape byte-identically to a pre-feature
    /// broker. The flags are gone, so what has to be proven now is that the families
    /// are THERE on a virgin process: an alert rule or a dashboard panel written once
    /// must apply to every cell in the fleet, and a series that appears only after the
    /// first write is a series that pages nobody the day it matters.
    #[test]
    fn every_family_is_exposed_on_a_virgin_broker() {
        let m = Metrics::new();
        let out = without_live_process_samples(&m.prometheus());
        for probe in [
            "queen_kv_ops_total",
            "queen_kv_bytes_total",
            "queen_kv_expired_not_pruned",
            "queen_kv_read_rejected_total",
            "queen_kv_pool",
            "queen_timers_due",
            "queen_timers_fired_total",
            "queen_timers_dlq_total",
            "queen_timers_schedule_rejected_total",
            "queen_sweeper_cycle_milliseconds",
            "queen_sweeper_rows_total",
        ] {
            assert!(out.contains(probe), "{probe} missing on a fresh broker");
        }
    }

    /// Recording stays cheap and unconditional, and every recorded number reaches the
    /// exposition. There is no longer any latch between the two, which is the point:
    /// the one that existed could be left unset and silently swallow the lot.
    #[test]
    fn what_is_recorded_is_exposed() {
        let m = Metrics::new();
        m.kvt.kv_op(KvOp::Put, KvResult::Applied, 1.5);
        m.kvt.kv_bytes(128, 256);
        m.kvt.kv_read_rejected(KvReject::RateLimited);
        m.kvt.kv_singleflight_coalesced(3);
        m.kvt.set_kv_expiry(50_000, true, 900_000);
        m.kvt.set_kv_pool(16, 15, 1);
        m.kvt.set_timers_due(2000, true, 12_000);
        m.kvt.timers_fired(FireResult::Fired, 7);
        m.kvt.sweeper_cycle(SweepPhase::Fire, 12.0, 40);
        let out = m.prometheus();
        assert!(out.contains("queen_kv_ops_total{op=\"put\",result=\"applied\"} 1"));
        assert!(out.contains("queen_kv_bytes_total{dir=\"in\"} 128"));
        assert!(out.contains("queen_kv_singleflight_coalesced_total 3"));
        assert!(out.contains("queen_timers_due 2000"));
        assert!(out.contains("queen_timers_fired_total{result=\"fired\"} 7"));
    }

    /// §14.1: `tenant` is allowed on the fire-lag gauge and NOWHERE else. This is the
    /// one series a caller can add label values to, so it is also the one that has to
    /// be proven bounded and proven escaped.
    #[test]
    fn tenant_appears_only_on_the_fire_lag_gauge() {
        let m = Metrics::new();
        m.kvt.kv_op(KvOp::Get, KvResult::Applied, 0.4);
        m.kvt.timers_fired(FireResult::Fired, 1);
        m.kvt.fire_lag("acme", 1000.0);
        for line in m.prometheus().lines() {
            if line.starts_with('#') || !line.contains("tenant=") {
                continue;
            }
            assert!(
                line.starts_with("queen_timers_fire_lag_seconds{"),
                "tenant label on a series that must not carry it: {line}"
            );
        }
    }

    /// A tenant id is opaque and unvalidated (§9.4), so it is escaped rather than
    /// trusted: one unescaped quote does not corrupt a line, it corrupts every line
    /// after it, and a scrape that fails to parse is a monitoring outage.
    #[test]
    fn tenant_label_is_escaped() {
        let m = Metrics::new();
        m.kvt.fire_lag("ev\"il\\", 10.0);
        let out = m.prometheus();
        assert!(out.contains(r#"tenant="ev\"il\\""#), "unescaped label: {out}");
    }

    /// The cap DENIES, it does not evict (§9.4 correction 2). Under an attacker-chosen
    /// tenant id, an evicting map is an unbounded map with extra steps: it keeps
    /// allocating, and the series it does keep are whichever arrived last rather than
    /// whichever matter. The drop counter is what makes the cap visible when it bites.
    #[test]
    fn fire_lag_tenant_map_denies_past_the_cap_and_says_so() {
        let m = Metrics::new();
        for i in 0..(FIRE_LAG_TENANT_CAP + 25) {
            m.kvt.fire_lag(&format!("tenant-{i}"), 1.0);
        }
        assert_eq!(m.kvt.fire_lag.read().unwrap().len(), FIRE_LAG_TENANT_CAP);
        assert_eq!(m.kvt.fire_lag_dropped.load(Ordering::Relaxed), 25);
        // The tenants admitted BEFORE the cap keep reporting — denial must not cost
        // the series that were already there.
        assert!(m.kvt.fire_lag_p95_seconds_of("tenant-0") > 0.0);
    }

    /// The rollup snapshot ranks by proximity to a quota first (§14.3.5: 0.8 is
    /// already late with a soft quota), and only then by raw occupancy — the top of
    /// this list is who to call, not who is biggest.
    #[test]
    fn usage_top_ranks_by_quota_pressure_then_size() {
        let m = Metrics::new();
        m.kvt.set_usage(vec![
            TenantUsage { tenant: "huge-but-fine".into(), kv_rows: 1_000_000, kv_quota_ratio: 0.1, ..Default::default() },
            TenantUsage { tenant: "small-but-full".into(), kv_rows: 900, kv_quota_ratio: 0.95, ..Default::default() },
            TenantUsage { tenant: "timers-full".into(), timers_pending: 990, timers_quota_ratio: 0.99, ..Default::default() },
        ]);
        let (top, total) = m.kvt.usage_top(2);
        assert_eq!(total, 3);
        assert_eq!(top[0].tenant, "timers-full");
        assert_eq!(top[1].tenant, "small-but-full");
        let (rows, _bytes, pending) = m.kvt.usage_totals();
        assert_eq!(rows, 1_000_900);
        assert_eq!(pending, 990);
    }
}
