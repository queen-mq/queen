//! Rate limiting + parked-consumer gauges. OWNER: Agent D.
//! Design: rev 2.3 T4a — dual token bucket per cluster (capacity = burst
//! column, refill = sustained column), 16-way sharded lock map keyed by
//! cluster_id; per-cluster AtomicI64 parked gauge (same sharding pattern)
//! plus the pre-existing cell-wide gauge, RAII release on both. `None` on
//! either side of a limit pair means unlimited: the check returns `Allow`
//! without ever touching a shard.
//!
//! Shadow mode: `check_req`/`check_msgs` always return the *true* computed
//! Decision, enforce or not — the gateway is the one that consults
//! `enforcing()` and decides whether to actually reject. `parked_slot` is the
//! one exception: because it's a stateful RAII acquisition (the gauge must
//! stay correct across the guard's lifetime), it enforces internally and
//! always hands back a live guard in shadow mode even when over cap, so the
//! gauge accounting stays honest regardless of whether we're rejecting. All
//! three paths log a uniform `target: "limits"` warn whenever the decision
//! *would* deny, tagged `blocked=true` (enforcing) or `would_block=true`
//! (shadow) — see `log_deny`.
//!
//! Storage: a per-cluster account (same 16-way sharding) holding the last
//! retained-bytes figure the reconciler published plus the push payload bytes
//! accepted since that figure was computed. `push_block_reason_for` answers
//! the gateway off both, so the quota bites inside a broker recompute window
//! instead of after it. See `StorageAccount` for the 2026-09-03 measurement
//! that made it necessary.
//!
//! GC: idle (>10min, zero outstanding parked count) cluster entries are
//! pruned lazily, piggybacked on real traffic (`maybe_gc`), throttled to at
//! most one sweep per `GC_INTERVAL` via a non-blocking `try_lock` so it never
//! adds latency to the hot path.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::state::ClusterCtx;

const N_SHARDS: usize = 16;
/// Cluster entries idle longer than this are eligible for GC.
const IDLE_TTL: Duration = Duration::from_secs(600);
/// GC sweeps are throttled to at most once per this interval (lazy, piggybacked
/// on request traffic — see `maybe_gc`).
const GC_INTERVAL: Duration = Duration::from_secs(120);
/// Retry-After handed back for an over-cap parked pop (cell- or cluster-wide).
const PARKED_RETRY_AFTER_S: u64 = 5;

#[derive(Debug)]
pub enum Decision {
    Allow,
    /// Deny with Retry-After seconds. Callers map to err_429.
    Deny { retry_after_s: u64, code: &'static str },
}

/// Why a cluster's pushes are blocked by a live (in-process) flag. The two
/// sources are independent — a cluster can be over its storage cap and out of
/// monthly messages at the same time, and releasing one must not release the
/// other — so the flag set is keyed by (cluster, reason) rather than by
/// cluster alone. Each maps to its own client-facing 403 code in gateway.rs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PushBlock {
    /// Retained bytes over `max_retained_bytes` (registry reconciler ->
    /// storage-quota pump in main.rs).
    Storage,
    /// Calendar-month messages at or over `plans.monthly_msgs_quota` (the
    /// rollup task, meter::spawn_rollup).
    MonthlyQuota,
}

/// Precedence when a cluster carries more than one live block: the first
/// match wins the 403 code. Storage leads because it is the cell-protecting
/// one — a tenant whose disk pressure is blocking pushes needs to hear that
/// before a billing ceiling that will clear on its own at the month boundary.
const PUSH_BLOCK_PRECEDENCE: &[PushBlock] = &[PushBlock::Storage, PushBlock::MonthlyQuota];

/// Per-cluster storage estimate: the last retained-bytes figure published by
/// the registry reconciler, plus the push payload bytes accepted since that
/// figure was computed.
///
/// WHY THIS EXISTS (measured on the trial cell, 2026-09-03/04). The retained
/// figure is recomputed BY THE BROKER on its own slow lane
/// (RETAINED_BYTES_INTERVAL_MS, default 600s), and the proxy then picks it up
/// with a lag of its own (up to ~96s observed). Between the two, the gate saw
/// a frozen number and refused nothing: pushing straight after a recompute,
/// one key put 2.5 GB past a 256 MiB quota in 174s with no 403 at all, and the
/// first refusal arrived 96s after the NEXT recompute. Shortening the broker
/// lane to 60s bounds that to ~2 GB per window; it does not close it. Counting
/// what we accept between computations does.
struct StorageAccount {
    /// Last total published by the reconciler (`publish_retained`). Zero until
    /// the first publication, which is the honest value for a cluster this
    /// process has not yet measured: the same posture as the push-blocked
    /// flag, which is likewise unset until the first reconcile pass.
    measured: i64,
    /// Push payload bytes accepted since `measured` last CHANGED.
    in_flight: i64,
    last_seen: Instant,
}

impl StorageAccount {
    fn new(now: Instant) -> StorageAccount {
        StorageAccount { measured: 0, in_flight: 0, last_seen: now }
    }

    /// Estimated retained bytes right now. Saturating: a malformed or huge
    /// measurement plus a long in-flight run must not wrap into a small number,
    /// which would read as "plenty of room".
    fn estimate(&self) -> i64 {
        self.measured.saturating_add(self.in_flight)
    }
}

pub struct ParkedGuard {
    cell_gauge: Option<Arc<AtomicI64>>,
    cluster_gauge: Option<Arc<AtomicI64>>,
}

impl Drop for ParkedGuard {
    fn drop(&mut self) {
        if let Some(g) = self.cell_gauge.take() {
            g.fetch_sub(1, Ordering::Relaxed);
        }
        if let Some(g) = self.cluster_gauge.take() {
            g.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// One side (req or msgs) of a cluster's dual bucket. `initialized` lets a
/// freshly-created bucket start *full* (burst immediately available) on its
/// first `configure()` call, since capacity/refill aren't known until then.
struct TokenBucket {
    tokens: f64,
    capacity: f64,
    refill_per_sec: f64,
    updated: Instant,
    initialized: bool,
}

impl TokenBucket {
    fn new(now: Instant) -> TokenBucket {
        TokenBucket { tokens: 0.0, capacity: 0.0, refill_per_sec: 0.0, updated: now, initialized: false }
    }

    /// Apply the caller's current plan values. First call fills the bucket;
    /// later calls only ever clamp tokens *down* on a capacity shrink (a plan
    /// upgrade doesn't hand out free tokens).
    fn configure(&mut self, capacity: f64, refill_per_sec: f64) {
        if !self.initialized {
            self.tokens = capacity;
            self.initialized = true;
        } else if self.tokens > capacity {
            self.tokens = capacity;
        }
        self.capacity = capacity;
        self.refill_per_sec = refill_per_sec;
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.updated).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
            self.updated = now;
        }
    }

    /// Debit `n` tokens iff available. Never goes negative.
    fn try_take(&mut self, n: f64) -> bool {
        if self.tokens >= n {
            self.tokens -= n;
            true
        } else {
            false
        }
    }

    /// How many tokens short of `n` we are right now (0 if already enough).
    fn deficit(&self, n: f64) -> f64 {
        (n - self.tokens).max(0.0)
    }

    /// Unconditional debit — may drive the bucket negative. Post-completion
    /// delivery accounting only (rev 2.3: parked pops exempt at entry, debited
    /// at completion).
    fn force_take(&mut self, n: f64) {
        self.tokens -= n;
    }
}

struct ClusterBuckets {
    req: TokenBucket,
    msgs: TokenBucket,
    last_seen: Instant,
}

impl ClusterBuckets {
    fn new(now: Instant) -> ClusterBuckets {
        ClusterBuckets { req: TokenBucket::new(now), msgs: TokenBucket::new(now), last_seen: now }
    }
}

struct ParkedEntry {
    count: Arc<AtomicI64>,
    last_seen: Instant,
}

#[derive(Clone, Copy)]
enum Kind {
    Req,
    Msgs,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::Req => "req",
            Kind::Msgs => "msgs",
        }
    }
}

fn shard_index(id: &Uuid) -> usize {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut h);
    (h.finish() as usize) % N_SHARDS
}

fn new_shards<V>() -> Vec<Mutex<HashMap<Uuid, V>>> {
    (0..N_SHARDS).map(|_| Mutex::new(HashMap::new())).collect()
}

pub struct Limits {
    enforce: bool,
    /// cell-wide parked cap protecting the broker (5k comfortable on a free cell)
    cell_parked: Arc<AtomicI64>,
    cell_parked_max: i64,
    buckets: Vec<Mutex<HashMap<Uuid, ClusterBuckets>>>,
    parked: Vec<Mutex<HashMap<Uuid, ParkedEntry>>>,
    push_blocked: RwLock<HashSet<(Uuid, PushBlock)>>,
    /// Per-cluster storage estimate, sharded like the buckets. Read on every
    /// push (`storage_over_cap`), written by the storage pump in main.rs
    /// (`publish_retained`) and by the produce path (`note_accepted_bytes`).
    storage: Vec<Mutex<HashMap<Uuid, StorageAccount>>>,
    gc_next: Mutex<Instant>,
}

impl Limits {
    pub fn new(cfg: &crate::config::Config) -> Limits {
        Limits::with_params(
            cfg.enforce,
            crate::config::env_u64("QUEEN_PROXY_CELL_MAX_PARKED", 5000) as i64,
        )
    }

    fn with_params(enforce: bool, cell_parked_max: i64) -> Limits {
        Limits {
            enforce,
            cell_parked: Arc::new(AtomicI64::new(0)),
            cell_parked_max,
            buckets: new_shards(),
            parked: new_shards(),
            push_blocked: RwLock::new(HashSet::new()),
            storage: new_shards(),
            gc_next: Mutex::new(Instant::now() + GC_INTERVAL),
        }
    }

    pub fn enforcing(&self) -> bool {
        self.enforce
    }

    /// One request token. Call once per proxied request, post-authn.
    pub fn check_req(&self, ctx: &ClusterCtx) -> Decision {
        let Some(rate) = ctx.limits.max_req_per_sec else { return Decision::Allow };
        let rate = rate.max(0) as f64;
        let capacity = ctx.limits.req_burst.map(|b| b.max(0) as f64).unwrap_or(rate * 2.0);
        let now = Instant::now();
        self.maybe_gc(now);
        let decision = self.decide(ctx.cluster_id, now, capacity, rate, 1.0, Kind::Req);
        if let Decision::Deny { retry_after_s, .. } = &decision {
            self.log_deny(Kind::Req.as_str(), ctx.cluster_id, *retry_after_s);
        }
        decision
    }

    /// n message tokens (push items / transaction push-ops). May also be
    /// debited post-completion for deliveries (bucket may go negative).
    pub fn check_msgs(&self, ctx: &ClusterCtx, n: u64) -> Decision {
        if n == 0 {
            return Decision::Allow;
        }
        let Some(rate) = ctx.limits.max_msgs_per_sec else { return Decision::Allow };
        let rate = rate.max(0) as f64;
        let capacity = ctx.limits.msgs_burst.map(|b| b.max(0) as f64).unwrap_or(rate * 2.0);
        let now = Instant::now();
        self.maybe_gc(now);
        let decision = self.decide(ctx.cluster_id, now, capacity, rate, n as f64, Kind::Msgs);
        if let Decision::Deny { retry_after_s, .. } = &decision {
            self.log_deny(Kind::Msgs.as_str(), ctx.cluster_id, *retry_after_s);
        }
        decision
    }

    pub fn debit_deliveries(&self, ctx: &ClusterCtx, n: u64) {
        if n == 0 {
            return;
        }
        let Some(rate) = ctx.limits.max_msgs_per_sec else { return };
        let rate = rate.max(0) as f64;
        let capacity = ctx.limits.msgs_burst.map(|b| b.max(0) as f64).unwrap_or(rate * 2.0);
        let now = Instant::now();
        self.maybe_gc(now);
        let idx = shard_index(&ctx.cluster_id);
        let mut shard = self.buckets[idx].lock().unwrap();
        let entry = shard.entry(ctx.cluster_id).or_insert_with(|| ClusterBuckets::new(now));
        entry.last_seen = now;
        entry.msgs.configure(capacity, rate);
        entry.msgs.refill(now);
        entry.msgs.force_take(n as f64);
    }

    /// Acquire a parked-consumer slot (wait=true pop). Err -> 429.
    pub fn parked_slot(&self, ctx: &ClusterCtx) -> Result<ParkedGuard, Decision> {
        let now = Instant::now();
        self.maybe_gc(now);

        let cell_before = self.cell_parked.fetch_add(1, Ordering::Relaxed);
        let cell_over = cell_before >= self.cell_parked_max;

        let cluster_gauge = self.parked_gauge_for(ctx.cluster_id, now);
        let cluster_before = cluster_gauge.fetch_add(1, Ordering::Relaxed);
        let cluster_over = ctx.limits.max_parked_pops.map_or(false, |m| cluster_before >= m);

        if cell_over || cluster_over {
            self.log_deny("parked", ctx.cluster_id, PARKED_RETRY_AFTER_S);
            if self.enforce {
                self.cell_parked.fetch_sub(1, Ordering::Relaxed);
                cluster_gauge.fetch_sub(1, Ordering::Relaxed);
                return Err(Decision::Deny {
                    retry_after_s: PARKED_RETRY_AFTER_S,
                    code: crate::errors::CODE_RATE_LIMITED,
                });
            }
            // Shadow mode: still hand back a live guard so the gauges we just
            // bumped get correctly released on drop — the request proceeds
            // either way, so the gauge must track it either way.
        }
        Ok(ParkedGuard { cell_gauge: Some(self.cell_parked.clone()), cluster_gauge: Some(cluster_gauge) })
    }

    /// Storage quota state, updated by the registry reconciler (the
    /// storage-quota pump in main.rs). Kept as the reason-less signature the
    /// pump already calls; it is `PushBlock::Storage` and nothing else.
    pub fn set_push_blocked(&self, cluster_id: Uuid, blocked: bool) {
        self.set_push_blocked_reason(cluster_id, PushBlock::Storage, blocked);
    }

    /// Same flag set, one reason at a time. Reasons are independent: setting
    /// or clearing one never touches the other, so two pumps on different
    /// cadences (storage every 10s, monthly quota hourly) can own one each
    /// without racing over a shared boolean.
    pub fn set_push_blocked_reason(&self, cluster_id: Uuid, reason: PushBlock, blocked: bool) {
        let mut set = self.push_blocked.write().unwrap();
        if blocked {
            set.insert((cluster_id, reason));
        } else {
            set.remove(&(cluster_id, reason));
        }
    }

    /// Fast in-process read of the push-blocked flags set by the quota pumps,
    /// resolved to the reason the client should be told about
    /// (PUSH_BLOCK_PRECEDENCE). Gateway: consult this alongside `ctx.status ==
    /// ClusterStatus::PushBlocked` at the same pipeline point (step 2, the
    /// cluster-status gate) — these are the faster-reacting signals between
    /// pump ticks; ctx.status is the DB/cache-backed one.
    pub fn push_block_reason(&self, cluster_id: Uuid) -> Option<PushBlock> {
        let set = self.push_blocked.read().unwrap();
        PUSH_BLOCK_PRECEDENCE
            .iter()
            .copied()
            .find(|reason| set.contains(&(cluster_id, *reason)))
    }

    /// `push_block_reason` plus the in-flight storage estimate, which needs the
    /// cluster's own `max_retained_bytes` and so cannot be answered from a
    /// cluster id alone. This is what the gateway calls; the id-only form stays
    /// for callers that only care about the pump-set flags.
    ///
    /// The estimate can only ever ADD a `Storage` reason, never remove one, and
    /// it is resolved through the same PUSH_BLOCK_PRECEDENCE so a cluster that
    /// is both over storage and out of monthly messages still reports storage.
    pub fn push_block_reason_for(&self, ctx: &ClusterCtx) -> Option<PushBlock> {
        // Computed BEFORE the flag lock is taken, not inside the closure: two
        // locks held at once here and in the other order anywhere else is how a
        // deadlock gets built, and nothing about this needs them together.
        let over_estimate = self.storage_over_cap(ctx);
        let set = self.push_blocked.read().unwrap();
        PUSH_BLOCK_PRECEDENCE.iter().copied().find(|reason| {
            set.contains(&(ctx.cluster_id, *reason))
                || (*reason == PushBlock::Storage && over_estimate)
        })
    }

    /// Is this cluster at or over its retained-bytes cap once the bytes we have
    /// accepted since the last computation are counted?
    ///
    /// `>=`, where `decide_over_storage` uses `>`. Not an inconsistency: that
    /// one judges a MEASURED total, so "exactly at the cap" is a fact and the
    /// tenant gets the benefit of it. This one runs BEFORE the batch in hand is
    /// counted, so it is always at least one batch behind the truth, and the
    /// boundary case is resolved against the tenant rather than against the
    /// cell's disk. The gate order also means exactly one batch may cross the
    /// cap before anything is refused, which is the least overshoot a
    /// check-then-forward proxy can have.
    ///
    /// HARD, like the pump-set storage flag it backstops (see
    /// gateway::push_block_response): a quota that only warns protects neither
    /// the cell's disk nor the bill, so `enforcing()` is deliberately not
    /// consulted. `None` (unlimited) is answered without touching the shard.
    pub fn storage_over_cap(&self, ctx: &ClusterCtx) -> bool {
        let Some(max) = ctx.limits.max_retained_bytes else { return false };
        let idx = shard_index(&ctx.cluster_id);
        let shard = self.storage[idx].lock().unwrap();
        shard.get(&ctx.cluster_id).is_some_and(|a| a.estimate() >= max)
    }

    /// Record payload bytes the proxy has just accepted for forwarding.
    ///
    /// Counted at the moment of the decision rather than off the broker's
    /// response: an over-count (the broker later rejects the batch) is
    /// conservative and is erased by the next computation, whereas waiting for
    /// the response would reopen the window this whole mechanism exists to
    /// close. The measure is the raw JSON payload text, the same basis every
    /// other size cap in gateway.rs uses; the broker stores those bytes
    /// COMPRESSED, so the estimate runs high, which again only ever refuses
    /// earlier.
    pub fn note_accepted_bytes(&self, cluster_id: Uuid, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let now = Instant::now();
        let idx = shard_index(&cluster_id);
        let mut shard = self.storage[idx].lock().unwrap();
        let account = shard.entry(cluster_id).or_insert_with(|| StorageAccount::new(now));
        account.last_seen = now;
        account.in_flight = account.in_flight.saturating_add(bytes.min(i64::MAX as u64) as i64);
    }

    /// Publish the reconciler's latest retained-bytes total for a cluster, from
    /// the storage pump in main.rs.
    ///
    /// The in-flight counter is reset only when the VALUE CHANGES, which is the
    /// only evidence of a fresh computation the broker gives us: its
    /// `resources/queues` response carries the number and no timestamp for it
    /// (server/sql/procedures/018_stats.sql), so republishing an identical
    /// total means the slow lane has not run again and the bytes we counted
    /// since are still uncounted by it. Resetting on every poll instead would
    /// forget most of a 600s broker window within one 10s proxy tick and put
    /// the 2026-09-03 hole straight back.
    ///
    /// This cannot deadlock a cluster into a permanent estimate-driven refusal:
    /// the in-flight bytes are ones the broker really did store, so its next
    /// recompute must move the total, which resets the counter. Pushes and
    /// expiry cancelling to the exact byte across a whole broker period is the
    /// only way the value holds still while in-flight grows, and a cluster in
    /// that state is by definition sitting at a steady occupancy that the
    /// measured total already describes.
    pub fn publish_retained(&self, cluster_id: Uuid, measured: i64) {
        let now = Instant::now();
        let idx = shard_index(&cluster_id);
        let mut shard = self.storage[idx].lock().unwrap();
        let account = shard.entry(cluster_id).or_insert_with(|| StorageAccount::new(now));
        account.last_seen = now;
        if account.measured != measured {
            account.measured = measured;
            account.in_flight = 0;
        }
    }

    /// Core dual-bucket decision: get-or-create the cluster's bucket pair,
    /// refresh it to the caller's current plan values, refill for elapsed
    /// time, then try to debit `n`. Only debits on success — a Deny never
    /// changes bucket state, so shadow-mode denies don't skew future decisions.
    fn decide(&self, cluster_id: Uuid, now: Instant, capacity: f64, refill: f64, n: f64, kind: Kind) -> Decision {
        let idx = shard_index(&cluster_id);
        let mut shard = self.buckets[idx].lock().unwrap();
        let entry = shard.entry(cluster_id).or_insert_with(|| ClusterBuckets::new(now));
        entry.last_seen = now;
        let bucket = match kind {
            Kind::Req => &mut entry.req,
            Kind::Msgs => &mut entry.msgs,
        };
        bucket.configure(capacity, refill);
        bucket.refill(now);
        if bucket.try_take(n) {
            Decision::Allow
        } else {
            let deficit = bucket.deficit(n);
            let secs = if refill > 0.0 { (deficit / refill).ceil() as u64 } else { 60 };
            Decision::Deny { retry_after_s: secs.clamp(1, 60), code: crate::errors::CODE_RATE_LIMITED }
        }
    }

    fn parked_gauge_for(&self, cluster_id: Uuid, now: Instant) -> Arc<AtomicI64> {
        let idx = shard_index(&cluster_id);
        let mut shard = self.parked[idx].lock().unwrap();
        let entry = shard
            .entry(cluster_id)
            .or_insert_with(|| ParkedEntry { count: Arc::new(AtomicI64::new(0)), last_seen: now });
        entry.last_seen = now;
        entry.count.clone()
    }

    /// Uniform shadow-mode-aware log line for every kind of limit decision.
    /// Always logs when called (callers only call it on a would-deny path);
    /// the field name is the only thing that varies with enforce/shadow.
    fn log_deny(&self, kind: &'static str, cluster_id: Uuid, retry_after_s: u64) {
        if self.enforce {
            tracing::warn!(
                target: "limits",
                cluster = %cluster_id,
                kind,
                retry_after_s,
                blocked = true,
                "limit exceeded"
            );
        } else {
            tracing::warn!(
                target: "limits",
                cluster = %cluster_id,
                kind,
                retry_after_s,
                would_block = true,
                "limit exceeded (shadow)"
            );
        }
    }

    /// Piggyback a throttled GC sweep on real traffic: at most one sweep per
    /// GC_INTERVAL, via a non-blocking try_lock so a concurrent/recent sweep
    /// just gets skipped rather than adding latency.
    fn maybe_gc(&self, now: Instant) {
        if let Ok(mut next) = self.gc_next.try_lock() {
            if now >= *next {
                *next = now + GC_INTERVAL;
                drop(next);
                self.gc_sweep(now);
            }
        }
    }

    fn gc_sweep(&self, now: Instant) {
        let mut bucket_evicted = 0usize;
        for shard in &self.buckets {
            let mut m = shard.lock().unwrap();
            let before = m.len();
            m.retain(|_, v| now.saturating_duration_since(v.last_seen) < IDLE_TTL);
            bucket_evicted += before - m.len();
        }
        let mut parked_evicted = 0usize;
        for shard in &self.parked {
            let mut m = shard.lock().unwrap();
            let before = m.len();
            // Never evict an entry with outstanding parked guards, regardless
            // of idle time — the Arc is shared with live ParkedGuards and
            // dropping it from the map would orphan future accounting for
            // that cluster (a fresh entry would start recounting from 0).
            m.retain(|_, v| {
                v.count.load(Ordering::Relaxed) > 0 || now.saturating_duration_since(v.last_seen) < IDLE_TTL
            });
            parked_evicted += before - m.len();
        }
        // Storage accounts are refreshed by the pump for every cluster the
        // reconciler still targets (`c.status <> 'deleting'`), so a LIVE
        // cluster's entry is never idle and never evicted here. What ages out
        // is a cluster nobody publishes for any more: deleted, or moved off
        // this cell. Losing an idle account's `in_flight` is safe on the same
        // grounds: IDLE_TTL is ten minutes, so a cluster that has neither
        // pushed nor been measured in that long has had any bytes it did push
        // counted by the broker's slow lane long since.
        let mut storage_evicted = 0usize;
        for shard in &self.storage {
            let mut m = shard.lock().unwrap();
            let before = m.len();
            m.retain(|_, v| now.saturating_duration_since(v.last_seen) < IDLE_TTL);
            storage_evicted += before - m.len();
        }
        if bucket_evicted > 0 || parked_evicted > 0 || storage_evicted > 0 {
            tracing::debug!(
                target: "limits",
                bucket_evicted,
                parked_evicted,
                storage_evicted,
                "gc: pruned idle cluster entries"
            );
        }
    }

    #[cfg(test)]
    fn new_for_test(enforce: bool) -> Limits {
        Limits::with_params(enforce, 5000)
    }

    #[cfg(test)]
    fn new_for_test_with_cap(enforce: bool, cell_parked_max: i64) -> Limits {
        Limits::with_params(enforce, cell_parked_max)
    }

    /// White-box peek for tests only: current raw msgs-bucket token count, if
    /// the cluster has an entry at all.
    #[cfg(test)]
    fn debug_msgs_tokens(&self, cluster_id: Uuid) -> Option<f64> {
        let idx = shard_index(&cluster_id);
        let shard = self.buckets[idx].lock().unwrap();
        shard.get(&cluster_id).map(|e| e.msgs.tokens)
    }

    /// White-box peek for tests only: measured + in-flight for the cluster, if
    /// it has a storage account at all.
    #[cfg(test)]
    fn debug_storage_estimate(&self, cluster_id: Uuid) -> Option<i64> {
        let idx = shard_index(&cluster_id);
        let shard = self.storage[idx].lock().unwrap();
        shard.get(&cluster_id).map(StorageAccount::estimate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{ClusterStatus, EffectiveLimits, Features};

    fn test_ctx(limits: EffectiveLimits) -> ClusterCtx {
        ClusterCtx {
            cluster_id: Uuid::new_v4(),
            tenant_id: Uuid::new_v4(),
            broker_tenant: Uuid::new_v4(),
            slug: "test".to_string(),
            cell_base_url: "http://localhost".to_string(),
            cell_token: None,
            status: ClusterStatus::Active,
            limits,
            features: Features::default(),
        }
    }

    // ---- TokenBucket: refill over time (injected Instants, no real sleep) ----

    #[test]
    fn token_bucket_refill_over_time() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(t0);
        b.configure(10.0, 5.0); // capacity 10, refill 5/s
        assert_eq!(b.tokens, 10.0, "fresh bucket starts full");

        assert!(b.try_take(10.0));
        assert_eq!(b.tokens, 0.0);

        b.refill(t0 + Duration::from_millis(1_000));
        assert!((b.tokens - 5.0).abs() < 1e-9, "1s at 5/s refills 5 tokens, got {}", b.tokens);

        b.refill(t0 + Duration::from_millis(3_000));
        assert_eq!(b.tokens, 10.0, "refill caps at capacity");
    }

    #[test]
    fn token_bucket_burst_then_sustained() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(t0);
        b.configure(3.0, 1.0); // burst 3, sustained 1/s

        assert!(b.try_take(1.0));
        assert!(b.try_take(1.0));
        assert!(b.try_take(1.0));
        assert!(!b.try_take(1.0), "burst exhausted");

        // Sustained rate: after 1s exactly one more token is available.
        b.refill(t0 + Duration::from_millis(1_000));
        assert!(b.try_take(1.0), "sustained refill should allow one more");
        assert!(!b.try_take(1.0), "no double-refill within the same second");
    }

    #[test]
    fn token_bucket_configure_shrinks_but_never_grows_tokens() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(t0);
        b.configure(10.0, 5.0);
        b.try_take(2.0); // tokens = 8
        b.configure(5.0, 5.0); // plan downgrade: capacity shrinks below current tokens
        assert_eq!(b.tokens, 5.0, "tokens clamp down to the new capacity");
        b.configure(100.0, 5.0); // plan upgrade
        assert_eq!(b.tokens, 5.0, "an upgrade doesn't hand out free tokens");
    }

    // ---- Limits::check_req / check_msgs end-to-end (real Instant::now(), fast rates) ----

    #[test]
    fn check_req_burst_then_denied_then_gateway_would_still_see_deny_in_shadow() {
        let limits = Limits::new_for_test(true); // enforcing — irrelevant to the Decision itself
        let ctx = test_ctx(EffectiveLimits { max_req_per_sec: Some(1000), req_burst: Some(2), ..Default::default() });

        assert!(matches!(limits.check_req(&ctx), Decision::Allow));
        assert!(matches!(limits.check_req(&ctx), Decision::Allow));
        match limits.check_req(&ctx) {
            Decision::Deny { retry_after_s, code } => {
                assert!(retry_after_s >= 1 && retry_after_s <= 60);
                assert_eq!(code, crate::errors::CODE_RATE_LIMITED);
            }
            Decision::Allow => panic!("expected burst to be exhausted"),
        }
    }

    #[test]
    fn shadow_mode_still_returns_true_deny_not_allow() {
        // "shadow log non blocca": logging must not turn a Deny into an Allow —
        // the gateway is the one that decides whether to actually reject.
        let limits = Limits::new_for_test(false); // shadow mode
        let ctx = test_ctx(EffectiveLimits { max_req_per_sec: Some(1000), req_burst: Some(1), ..Default::default() });

        assert!(matches!(limits.check_req(&ctx), Decision::Allow));
        assert!(
            matches!(limits.check_req(&ctx), Decision::Deny { .. }),
            "shadow mode must still compute+return the true Deny"
        );
    }

    #[test]
    fn check_msgs_zero_is_free() {
        let limits = Limits::new_for_test(true);
        let ctx = test_ctx(EffectiveLimits { max_msgs_per_sec: Some(1), msgs_burst: Some(1), ..Default::default() });
        // Drain the bucket, then confirm n=0 is still always Allow and doesn't
        // touch bucket state.
        assert!(matches!(limits.check_msgs(&ctx, 1), Decision::Allow));
        assert!(matches!(limits.check_msgs(&ctx, 0), Decision::Allow));
        assert_eq!(limits.debug_msgs_tokens(ctx.cluster_id), Some(0.0));
    }

    #[test]
    fn check_msgs_n_over_capacity_always_denies() {
        let limits = Limits::new_for_test(true);
        let ctx =
            test_ctx(EffectiveLimits { max_msgs_per_sec: Some(10), msgs_burst: Some(5), ..Default::default() });
        match limits.check_msgs(&ctx, 999) {
            Decision::Deny { retry_after_s, .. } => assert_eq!(retry_after_s, 60, "n far above capacity clamps to 60s"),
            Decision::Allow => panic!("n > capacity can never be satisfied"),
        }
    }

    #[test]
    fn retry_after_is_computed_and_clamped() {
        let limits = Limits::new_for_test(true);
        // capacity=5, refill=5/s: draining 5 then asking for 2 more needs a
        // 0.4s wait, which clamps up to the 1s floor.
        let ctx = test_ctx(EffectiveLimits { max_msgs_per_sec: Some(5), msgs_burst: Some(5), ..Default::default() });
        assert!(matches!(limits.check_msgs(&ctx, 5), Decision::Allow));
        match limits.check_msgs(&ctx, 2) {
            Decision::Deny { retry_after_s, .. } => assert_eq!(retry_after_s, 1, "sub-1s deficit floors to 1"),
            Decision::Allow => panic!("bucket should be empty"),
        }

        // A big deficit at a slow rate clamps at the 60s ceiling.
        let ctx2 = test_ctx(EffectiveLimits { max_msgs_per_sec: Some(1), msgs_burst: Some(1), ..Default::default() });
        assert!(matches!(limits.check_msgs(&ctx2, 1), Decision::Allow));
        match limits.check_msgs(&ctx2, 1) {
            Decision::Deny { retry_after_s, .. } => assert_eq!(retry_after_s, 1),
            Decision::Allow => panic!("bucket should be empty"),
        }
    }

    #[test]
    fn unlimited_none_allows_without_creating_shard_entry() {
        let limits = Limits::new_for_test(true);
        let ctx = test_ctx(EffectiveLimits::default()); // everything None
        for _ in 0..5 {
            assert!(matches!(limits.check_req(&ctx), Decision::Allow));
            assert!(matches!(limits.check_msgs(&ctx, 1_000_000), Decision::Allow));
        }
        assert_eq!(
            limits.debug_msgs_tokens(ctx.cluster_id),
            None,
            "unlimited checks must never touch the shard map"
        );
    }

    // ---- debit_deliveries: unconditional, can go negative, never denies ----

    #[test]
    fn debit_deliveries_goes_negative_and_then_throttles_checks() {
        let limits = Limits::new_for_test(true);
        let ctx =
            test_ctx(EffectiveLimits { max_msgs_per_sec: Some(10), msgs_burst: Some(10), ..Default::default() });

        // Parked-pop delivery debited after the fact, well beyond the bucket.
        limits.debit_deliveries(&ctx, 50);
        let tokens = limits.debug_msgs_tokens(ctx.cluster_id).expect("entry created");
        assert!(tokens < 0.0, "debit_deliveries must be able to drive the bucket negative, got {tokens}");
        assert_eq!(tokens, -40.0);

        // A subsequent check for even 1 token must deny while in debt.
        assert!(matches!(limits.check_msgs(&ctx, 1), Decision::Deny { .. }));
    }

    #[test]
    fn debit_deliveries_is_a_noop_when_unlimited() {
        let limits = Limits::new_for_test(true);
        let ctx = test_ctx(EffectiveLimits::default());
        limits.debit_deliveries(&ctx, 1_000_000);
        assert_eq!(limits.debug_msgs_tokens(ctx.cluster_id), None, "unlimited debit must not touch the shard");
    }

    // ---- parked_slot / ParkedGuard ----

    #[test]
    fn parked_guard_drop_decrements_both_gauges() {
        let limits = Limits::new_for_test(true);
        let ctx = test_ctx(EffectiveLimits { max_parked_pops: Some(10), ..Default::default() });

        let guard = limits.parked_slot(&ctx).expect("under cap");
        assert_eq!(limits.cell_parked.load(Ordering::Relaxed), 1);
        assert_eq!(limits.parked_gauge_for(ctx.cluster_id, Instant::now()).load(Ordering::Relaxed), 1);

        drop(guard);
        assert_eq!(limits.cell_parked.load(Ordering::Relaxed), 0);
        assert_eq!(limits.parked_gauge_for(ctx.cluster_id, Instant::now()).load(Ordering::Relaxed), 0);
    }

    #[test]
    fn parked_over_cluster_cap_enforced_denies_and_rolls_back() {
        let limits = Limits::new_for_test(true); // enforcing
        let ctx = test_ctx(EffectiveLimits { max_parked_pops: Some(1), ..Default::default() });

        let _g1 = limits.parked_slot(&ctx).expect("first slot ok");
        match limits.parked_slot(&ctx) {
            Err(Decision::Deny { retry_after_s, code }) => {
                assert_eq!(retry_after_s, PARKED_RETRY_AFTER_S);
                assert_eq!(code, crate::errors::CODE_RATE_LIMITED);
            }
            other => panic!("expected Deny over cap, enforcing; got Ok={}", other.is_ok()),
        }
        // Rolled back: gauge must still read exactly 1 (only _g1 outstanding).
        assert_eq!(limits.parked_gauge_for(ctx.cluster_id, Instant::now()).load(Ordering::Relaxed), 1);
    }

    #[test]
    fn parked_over_cap_shadow_still_grants_guard_and_tracks_gauge() {
        let limits = Limits::new_for_test(false); // shadow
        let ctx = test_ctx(EffectiveLimits { max_parked_pops: Some(1), ..Default::default() });

        let _g1 = limits.parked_slot(&ctx).expect("first slot ok");
        let g2 = limits.parked_slot(&ctx).expect("shadow mode must still grant a guard over cap");
        // Both reservations are live: gauge reflects 2 outstanding even though
        // the plan cap is 1 — this is what makes shadow-mode observability honest.
        assert_eq!(limits.parked_gauge_for(ctx.cluster_id, Instant::now()).load(Ordering::Relaxed), 2);
        drop(g2);
        assert_eq!(limits.parked_gauge_for(ctx.cluster_id, Instant::now()).load(Ordering::Relaxed), 1);
    }

    #[test]
    fn parked_cell_wide_cap_applies_regardless_of_cluster_limit() {
        let limits = Limits::new_for_test_with_cap(true, 1); // cell-wide cap of 1
        let ctx_a = test_ctx(EffectiveLimits::default()); // unlimited per-cluster
        let ctx_b = test_ctx(EffectiveLimits::default());

        let _g1 = limits.parked_slot(&ctx_a).expect("first cell-wide slot ok");
        assert!(limits.parked_slot(&ctx_b).is_err(), "second cluster still hits the cell-wide cap");
    }

    // ---- push_blocked ----

    #[test]
    fn push_blocked_set_and_query() {
        let limits = Limits::new_for_test(true);
        let id = Uuid::new_v4();
        assert_eq!(limits.push_block_reason(id), None);
        limits.set_push_blocked(id, true);
        // The reason-less setter is the storage one — that is what the
        // storage-quota pump in main.rs calls.
        assert_eq!(limits.push_block_reason(id), Some(PushBlock::Storage));
        limits.set_push_blocked(id, false);
        assert_eq!(limits.push_block_reason(id), None);
    }

    #[test]
    fn push_block_reasons_are_independent_and_ordered() {
        let limits = Limits::new_for_test(true);
        let id = Uuid::new_v4();

        limits.set_push_blocked_reason(id, PushBlock::MonthlyQuota, true);
        assert_eq!(limits.push_block_reason(id), Some(PushBlock::MonthlyQuota));

        // Both held at once: storage wins the reported reason...
        limits.set_push_blocked(id, true);
        assert_eq!(limits.push_block_reason(id), Some(PushBlock::Storage));

        // ...and releasing storage must NOT release the monthly quota block
        // (the bug a single shared boolean would have): the tenant is still
        // out of messages for the month.
        limits.set_push_blocked(id, false);
        assert_eq!(limits.push_block_reason(id), Some(PushBlock::MonthlyQuota));

        limits.set_push_blocked_reason(id, PushBlock::MonthlyQuota, false);
        assert_eq!(limits.push_block_reason(id), None);
    }

    #[test]
    fn push_block_is_per_cluster() {
        let limits = Limits::new_for_test(true);
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        limits.set_push_blocked_reason(a, PushBlock::MonthlyQuota, true);
        assert_eq!(limits.push_block_reason(a), Some(PushBlock::MonthlyQuota));
        assert_eq!(limits.push_block_reason(b), None, "one cluster's quota must not block another's");
    }

    // ---- in-flight storage accounting (2026-09-03 blind-window defect) ----

    /// A cluster 1 byte under a 1000-byte cap: the reconciler's number alone
    /// says "fine", and before this counter that stayed true for a whole broker
    /// recompute period no matter how much arrived. Bytes accepted inside the
    /// window have to refuse the push WITHOUT waiting for a new computation.
    #[test]
    fn accepted_bytes_refuse_inside_the_window_with_no_new_computation() {
        let limits = Limits::new_for_test(true);
        let ctx = test_ctx(EffectiveLimits { max_retained_bytes: Some(1_000), ..Default::default() });

        limits.publish_retained(ctx.cluster_id, 999);
        assert!(!limits.storage_over_cap(&ctx), "the measured figure is under the cap");
        assert_eq!(limits.push_block_reason_for(&ctx), None);

        limits.note_accepted_bytes(ctx.cluster_id, 1);
        assert!(limits.storage_over_cap(&ctx), "999 measured + 1 accepted reaches the cap");
        assert_eq!(
            limits.push_block_reason_for(&ctx),
            Some(PushBlock::Storage),
            "the gateway must see Storage without any pump or reconciler tick"
        );
        // The pump-set flag is untouched: this is a second, faster route to the
        // same verdict, not a rewrite of the first.
        assert_eq!(limits.push_block_reason(ctx.cluster_id), None);
    }

    #[test]
    fn a_new_computation_resets_the_in_flight_counter() {
        let limits = Limits::new_for_test(true);
        let ctx = test_ctx(EffectiveLimits { max_retained_bytes: Some(1_000), ..Default::default() });

        limits.publish_retained(ctx.cluster_id, 500);
        limits.note_accepted_bytes(ctx.cluster_id, 600);
        assert!(limits.storage_over_cap(&ctx), "500 + 600 is over 1000");

        // The broker recomputed and the tenant had meanwhile deleted data: the
        // fresh figure supersedes everything counted against the old one.
        limits.publish_retained(ctx.cluster_id, 400);
        assert!(!limits.storage_over_cap(&ctx), "a fresh computation clears the in-flight bytes");
        assert_eq!(limits.debug_storage_estimate(ctx.cluster_id), Some(400));
    }

    /// The counterpart, and the reason the reset is keyed on the VALUE: the
    /// pump republishes the same figure every tick, and each republication must
    /// neither drop the bytes counted since (which would reopen the window) nor
    /// add them again (which would refuse a tenant that is nowhere near its cap).
    #[test]
    fn republishing_an_unchanged_figure_neither_forgets_nor_double_counts() {
        let limits = Limits::new_for_test(true);
        let ctx = test_ctx(EffectiveLimits { max_retained_bytes: Some(1_000), ..Default::default() });

        limits.publish_retained(ctx.cluster_id, 100);
        limits.note_accepted_bytes(ctx.cluster_id, 50);
        assert_eq!(limits.debug_storage_estimate(ctx.cluster_id), Some(150));

        for _ in 0..5 {
            limits.publish_retained(ctx.cluster_id, 100);
        }
        assert_eq!(
            limits.debug_storage_estimate(ctx.cluster_id),
            Some(150),
            "five pump ticks on the same broker figure must not move the estimate"
        );

        limits.note_accepted_bytes(ctx.cluster_id, 25);
        assert_eq!(limits.debug_storage_estimate(ctx.cluster_id), Some(175));
    }

    #[test]
    fn unlimited_retained_bytes_never_refuses_and_never_touches_the_shard() {
        let limits = Limits::new_for_test(true);
        let ctx = test_ctx(EffectiveLimits::default()); // max_retained_bytes: None
        limits.note_accepted_bytes(ctx.cluster_id, 1_000_000_000);
        assert!(!limits.storage_over_cap(&ctx), "no cap, no refusal");
        assert_eq!(limits.push_block_reason_for(&ctx), None);
    }

    #[test]
    fn storage_estimate_is_per_cluster() {
        let limits = Limits::new_for_test(true);
        let a = test_ctx(EffectiveLimits { max_retained_bytes: Some(100), ..Default::default() });
        let b = test_ctx(EffectiveLimits { max_retained_bytes: Some(100), ..Default::default() });
        limits.note_accepted_bytes(a.cluster_id, 500);
        assert!(limits.storage_over_cap(&a));
        assert!(!limits.storage_over_cap(&b), "one tenant's bytes must not block another's");
    }

    /// Shadow mode does not soften this one, by the same rule the pump-set
    /// storage flag already follows (gateway::push_block_response: both live
    /// flags are hard gates).
    #[test]
    fn the_storage_estimate_is_a_hard_gate_in_shadow_mode() {
        let limits = Limits::new_for_test(false);
        let ctx = test_ctx(EffectiveLimits { max_retained_bytes: Some(10), ..Default::default() });
        limits.note_accepted_bytes(ctx.cluster_id, 10);
        assert_eq!(limits.push_block_reason_for(&ctx), Some(PushBlock::Storage));
    }

    /// Storage leads MonthlyQuota whichever of the two routes raised it.
    #[test]
    fn an_estimated_storage_block_still_outranks_a_monthly_quota_flag() {
        let limits = Limits::new_for_test(true);
        let ctx = test_ctx(EffectiveLimits { max_retained_bytes: Some(10), ..Default::default() });
        limits.set_push_blocked_reason(ctx.cluster_id, PushBlock::MonthlyQuota, true);
        assert_eq!(limits.push_block_reason_for(&ctx), Some(PushBlock::MonthlyQuota));
        limits.note_accepted_bytes(ctx.cluster_id, 10);
        assert_eq!(limits.push_block_reason_for(&ctx), Some(PushBlock::Storage));
    }

    /// A huge measurement plus a huge in-flight run must saturate, not wrap:
    /// wrapping would turn "far over quota" into "plenty of room".
    #[test]
    fn the_estimate_saturates_instead_of_wrapping() {
        let limits = Limits::new_for_test(true);
        let ctx = test_ctx(EffectiveLimits { max_retained_bytes: Some(i64::MAX), ..Default::default() });
        limits.publish_retained(ctx.cluster_id, i64::MAX - 1);
        limits.note_accepted_bytes(ctx.cluster_id, u64::MAX);
        assert_eq!(limits.debug_storage_estimate(ctx.cluster_id), Some(i64::MAX));
        assert!(limits.storage_over_cap(&ctx));
    }

    #[test]
    fn gc_prunes_a_storage_account_nobody_publishes_for_any_more() {
        let limits = Limits::new_for_test(true);
        let ctx = test_ctx(EffectiveLimits { max_retained_bytes: Some(10), ..Default::default() });
        limits.note_accepted_bytes(ctx.cluster_id, 1);
        assert_eq!(limits.debug_storage_estimate(ctx.cluster_id), Some(1));

        limits.gc_sweep(Instant::now() + IDLE_TTL + Duration::from_secs(1));
        assert_eq!(
            limits.debug_storage_estimate(ctx.cluster_id),
            None,
            "an account the pump has stopped refreshing ages out"
        );
    }

    // ---- GC (synthetic future Instant, no real sleep) ----

    #[test]
    fn gc_prunes_idle_bucket_entries_after_ttl() {
        let limits = Limits::new_for_test(true);
        let ctx = test_ctx(EffectiveLimits { max_req_per_sec: Some(10), req_burst: Some(10), ..Default::default() });
        assert!(matches!(limits.check_req(&ctx), Decision::Allow));
        let idx = shard_index(&ctx.cluster_id);
        assert!(limits.buckets[idx].lock().unwrap().contains_key(&ctx.cluster_id));

        let future = Instant::now() + IDLE_TTL + Duration::from_secs(1);
        limits.gc_sweep(future);

        assert!(
            !limits.buckets[idx].lock().unwrap().contains_key(&ctx.cluster_id),
            "idle entry should have been pruned"
        );
    }

    #[test]
    fn gc_never_prunes_parked_entry_with_outstanding_guard() {
        let limits = Limits::new_for_test(true);
        let ctx = test_ctx(EffectiveLimits::default());
        let guard = limits.parked_slot(&ctx).expect("slot ok");

        let future = Instant::now() + IDLE_TTL + Duration::from_secs(1);
        limits.gc_sweep(future);

        let idx = shard_index(&ctx.cluster_id);
        assert!(
            limits.parked[idx].lock().unwrap().contains_key(&ctx.cluster_id),
            "an entry with a live outstanding guard must survive GC regardless of idle time"
        );
        drop(guard);
    }
}
