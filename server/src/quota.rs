//! THE OCCUPANCY GATE — PLAN_KV_TIMERS.md §9.3, §9.4, §9.5, and rungs 1 and 6 of
//! the degradation ladder of §12.1.
//!
//! ---------------------------------------------------------------------------
//! WHY THERE IS NO EXACT COUNTER, which is the first thing a reader will ask
//!
//! An exact quota needs a per-tenant counter that every write of that tenant
//! updates. That counter is ONE ROW PER TENANT that every KV write must lock,
//! and it would live in the OUTERMOST of the six lock spaces (§2.1), held for
//! the whole duration of a bundle. It would serialise every write of a tenant on
//! a single row, in front of the message path. It is not merely expensive: it is
//! the exact anti-pattern the lock order exists to forbid (§2.4 D7), and the
//! wire header names it as a prohibition.
//!
//! So the quota is soft. What §9.3 corrects — and what this module implements —
//! is HOW soft.
//!
//! ---------------------------------------------------------------------------
//! THE CORRECTION, and the order-of-magnitude error it repairs
//!
//! Two design documents bounded the overrun at "write rate × 30 s", 30 s being
//! how often a broker RE-READS the measurement. But the measurement is WRITTEN
//! by the rollup, every 300 s, and no reader can be fresher than its writer. The
//! true bound of a measure-only gate is
//!
//! ```text
//! rate x (rollup period + refresh period + rollup duration)
//! ```
//!
//! which for a free tenant at 20 writes/s against a 1000-timer quota is 6600
//! rows of overrun on a 1000-row cap — 660%, sustained, never answering a single
//! 403. That does not describe a soft limit; it describes no limit.
//!
//! **THE ENFORCER IS THE LOCAL IN-PROCESS DELTA. THE MEASUREMENT ONLY RELEASES.**
//!
//!   * every broker holds the measurement it last read PLUS its own count of
//!     what it has admitted since, and refuses at `quota − measure` without
//!     waiting for a newer measurement. The block is therefore immediate for the
//!     writer that overruns, and the residual is bounded by FAN-OUT — a number
//!     the operator sizes — instead of by rate and time, which the tenant
//!     chooses. `src/tests_unit/quota_overshoot.rs` is the proof, and it runs;
//!   * the RELEASE looks only at the true measurement, with the hysteresis of
//!     `proxy/src/registry.rs:441-449`: block above the cap, release only under
//!     `release_percent` of it. Fast block, slow release. The other way round is
//!     a tenant that oscillates in and out of the block on every refresh;
//!   * the write path pays ONE HASHMAP LOOKUP AND NO SQL. That is the property
//!     that makes this legal at all.
//!
//! ---------------------------------------------------------------------------
//! THE DELTA IS A MAJORANT, NEVER A RECONCILIATION
//!
//! The gate charges what a call COULD write, before the database says what it
//! did: a `put` over an existing key adds no row, and the stored procedure does
//! not report that per op. Overestimating blocks a little early; underestimating
//! blocks late. Only the second is unsafe, so the delta is deliberately an upper
//! bound and the next refresh — not an adjustment — is what corrects it. A call
//! that was charged and then FAILED is refunded, because a database outage that
//! inflated every delta would turn a cell fault (503, "not your fault") into a
//! plan verdict (403, "yours"), and §12 says a stale or failed rollup must block
//! nothing.
//!
//! ---------------------------------------------------------------------------
//! WHAT IS NEVER BLOCKED, and why each one would be self-defeating
//!
//!   * **reads** (§9.5): use case number one is the idempotency marker, where a
//!     refused read makes the caller repeat an external side effect; and a
//!     tenant who cannot read cannot find out what to free;
//!   * **deletes** (§9.5): the only way out of a full tenant. Structurally, not
//!     as a special case: a call that adds no rows and no bytes cannot take
//!     anyone over, so the occupancy test does not even run for it;
//!   * **timer cancels** (§9.6): the fire never switches itself off, so a tenant
//!     that cannot cancel keeps producing messages it cannot stop until the
//!     horizon or an operator. Blocking it produces the opposite of its purpose.
//!     A cancel is not rate limited either, for the same reason.
//!
//! ---------------------------------------------------------------------------
//! THE UNVALIDATED TENANT (§9.4), which is what shapes the defaults
//!
//! `server/src/tenant.rs:16-19` is explicit: with the header on, the tenant is
//! opaque and validated against nothing. A client that rotates the header every
//! request would otherwise get a fresh bucket (no rate limit), a missing quota
//! row (unlimited), a fresh key space, and a permanent entry in every per-tenant
//! map. Hence:
//!
//!   1. with `require_grant` (derived from the tenancy flag), the ABSENCE of a
//!      `queen.kv_quota` row is a DENIAL and not a permission;
//!   2. the live map is SIZE-CAPPED and the cap **denies** rather than evicts —
//!      eviction would recycle the map forever and hand every rotated id a fresh
//!      budget. A tenant that HAS a quota row is exempt: it is bounded by the
//!      control plane, and the attack is an invented id, not a configured one;
//!   3. the rollup does not create rows for a tenant with neither quota nor
//!      queues (that half lives in `026_kv_sweeper.sql`).
//!
//! The house precedent for (2) is `handlers/mod.rs`, which never caches ownership
//! NEGATIVES precisely so a forged pid cannot grow a map.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// Which resource ran out. The three have different remedies, so the client is
/// told which one rather than a single "quota exceeded".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resource {
    KvRows,
    KvBytes,
    Timers,
}

/// The closed verdict set of §9.5. One enum, so the HTTP routes and the
/// transaction wire cannot answer differently for the same decision.
///
/// > **429 means retry later and it will work. 403 means retry all you like, it
/// > will not, until something changes. 503 means it is not your fault, it is
/// > the cell.**
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Allow,
    /// 403. Never 429: a `Retry-After` on a row quota is a lie — no delay
    /// resolves it — and it would make the client retry in a loop exactly when
    /// the tenant is already over. Never 507 either: that is WebDAV, no client
    /// treats it specially, and the proxy already answers 403 for the same
    /// concept, so a third status would be a second dialect of "out of space" to
    /// keep aligned across seven clients forever.
    OverQuota(Resource),
    /// 403. Not on the plan, or (with tenancy on) no quota row at all.
    NotGranted,
    /// 429, carrying the seconds to advertise in `Retry-After`.
    RateLimited(u32),
    /// 503. The per-tenant table is full (§9.4 point 2) — a cell condition, not
    /// the tenant's doing, so it gets the cell's status and not a 403.
    NoRoom,
}

impl Verdict {
    /// `(status, error code)`, or `None` when the call may proceed. The single
    /// mapping point named by `src/tests_unit/quota_gate.rs`.
    pub fn http(self) -> Option<(u16, &'static str)> {
        match self {
            Verdict::Allow => None,
            Verdict::OverQuota(Resource::KvRows) | Verdict::OverQuota(Resource::KvBytes) => {
                Some((403, "kv_quota_exceeded"))
            }
            Verdict::OverQuota(Resource::Timers) => Some((403, "timers_quota_exceeded")),
            Verdict::NotGranted => Some((403, "feature_gated")),
            Verdict::RateLimited(_) => Some((429, "rate_limited")),
            Verdict::NoRoom => Some((503, "kv_unavailable")),
        }
    }

    /// Seconds for `Retry-After`, and only where one is honest: a 403 that
    /// carried one would be telling the client that waiting helps.
    pub fn retry_after(self) -> Option<u32> {
        match self {
            Verdict::RateLimited(s) => Some(s.max(1)),
            Verdict::NoRoom => Some(1),
            _ => None,
        }
    }
}

/// One tenant's configured limits, from `queen.kv_quota`.
///
/// `None` is UNLIMITED, the same convention as the proxy's plan columns — and it
/// is only reachable when `require_grant` is off, i.e. when tenancy is off and
/// the operator is the customer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Limits {
    pub enabled: bool,
    pub max_rows: Option<i64>,
    pub max_bytes: Option<i64>,
    pub max_timers: Option<i64>,
    pub max_timer_horizon_s: Option<i64>,
    pub max_reads_per_sec: Option<u32>,
    pub max_writes_per_sec: Option<u32>,
}

/// One tenant's measured occupancy, from `queen.kv_usage` (written by the
/// rollup, on its own slow clock).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Measure {
    pub kv_rows: i64,
    pub kv_bytes: i64,
    pub timer_rows: i64,
    pub computed_at_ms: i64,
}

/// One row of a refresh: what the control plane says, and what the rollup saw.
#[derive(Clone, Debug)]
pub struct TenantRow {
    pub tenant: String,
    pub limits: Option<Limits>,
    pub measure: Measure,
}

/// Everything the gate is tuned by. Built from `Config` in production; `Default`
/// exists for the unit tests and is deliberately permissive, so a test that does
/// not set a knob is not silently exercising a limit.
#[derive(Clone, Copy, Debug)]
pub struct Knobs {
    /// Cap on the live per-tenant map. Denies, never evicts (§9.4 point 2).
    pub cap: usize,
    /// With tenancy on, no row means no. Derived from `QUEEN_TENANCY_HEADER`, so
    /// a self-hosted operator never has to discover why they must configure
    /// something (§9.4 point 1).
    pub require_grant: bool,
    /// Release band, as a percentage of the cap. 90, the same number and the
    /// same reasoning as `STORAGE_RELEASE_PERCENT` in the proxy.
    pub release_percent: i64,
    /// Cell-default rates, overridden downward or upward by a quota row.
    pub read_rate: u32,
    pub read_burst: u32,
    pub write_rate: u32,
    pub write_burst: u32,
    /// The watermark above which a tenant makes the cell "hot" and the rollup
    /// drops to the refresh cadence (§9.3). 0.8, because with a soft quota 80%
    /// is already late (§14.3 point 5).
    pub hot_ratio: f64,
}

impl Default for Knobs {
    fn default() -> Self {
        Knobs {
            cap: 10_000,
            require_grant: false,
            release_percent: 90,
            // Effectively no rate limit: a unit test that has not asked for one
            // must not trip over it.
            read_rate: u32::MAX,
            read_burst: u32::MAX,
            write_rate: u32::MAX,
            write_burst: u32::MAX,
            hot_ratio: 0.8,
        }
    }
}

#[cfg(test)]
pub(crate) type TestKnobs = Knobs;

/// What the last refresh published for one tenant.
#[derive(Clone, Copy, Debug)]
struct Snap {
    limits: Option<Limits>,
    measure: Measure,
}

/// A token bucket. Each carries its OWN last-refill instant: sharing one would
/// make an idle read budget stop accruing while writes flow, which is the
/// opposite of what separate budgets are for.
#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last_ms: i64,
}

impl Bucket {
    fn new(burst: u32) -> Self {
        Bucket {
            tokens: burst as f64,
            last_ms: crate::util::now_epoch_ms(),
        }
    }

    /// Spend one token, refilling first. `Retry-After` is never zero: it reads as
    /// "immediately" and produces a spin against a cell that just said no.
    fn take(&mut self, rate: u32, burst: u32) -> Verdict {
        let now = crate::util::now_epoch_ms();
        let dt = (now - self.last_ms).max(0) as f64 / 1000.0;
        self.last_ms = now;
        self.tokens = (self.tokens + dt * rate as f64).min(burst as f64);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return Verdict::Allow;
        }
        let need = 1.0 - self.tokens;
        let secs = if rate == 0 { 1.0 } else { need / rate as f64 };
        Verdict::RateLimited(secs.ceil().max(1.0) as u32)
    }
}

/// What THIS process has done since that refresh, plus the state that must
/// survive one: the block flags (whose release is hysteretic) and the buckets.
#[derive(Debug)]
struct Live {
    d_rows: i64,
    d_bytes: i64,
    d_timers: i64,
    blocked_rows: bool,
    blocked_bytes: bool,
    blocked_timers: bool,
    read: Bucket,
    write: Bucket,
}

impl Live {
    fn new(k: &Knobs) -> Self {
        Live {
            d_rows: 0,
            d_bytes: 0,
            d_timers: 0,
            blocked_rows: false,
            blocked_bytes: false,
            blocked_timers: false,
            read: Bucket::new(k.read_burst),
            write: Bucket::new(k.write_burst),
        }
    }
}

pub struct Quotas {
    /// Replaced wholesale by each refresh. Read on every gated call, so it is an
    /// `RwLock` and the write side is one swap per refresh period.
    snapshot: RwLock<HashMap<String, Snap>>,
    live: Mutex<HashMap<String, Live>>,
    k: Knobs,
    /// Is any tenant above the watermark? Read by the sweeper to decide the
    /// rollup cadence, so it is an atomic and not a lock.
    hot: AtomicBool,
    /// Epoch ms of the last successful refresh, 0 if never. Exposed so an
    /// operator can see a frozen measurement (§12, "computed_at che invecchia").
    refreshed_ms: AtomicI64,
    /// Tenants in the last refresh, for the log line and the gauges.
    known: AtomicI64,
}

/// Block above the cap, release only under the band — `decide_over_storage`
/// (`proxy/src/registry.rs:441-449`) in one line, including its clause that a
/// subject already over on the FIRST pass blocks immediately, because nothing is
/// remembered across a restart and the band may only ever delay the RELEASE.
fn decide_over(blocked: bool, value: i64, max: i64, release_percent: i64) -> bool {
    if !blocked {
        return value > max;
    }
    // i128 so the percentage is exact for any i64 cap, with no overflow and no
    // float rounding at the boundary this exists to stabilise.
    let release_at = (max as i128 * release_percent as i128 / 100) as i64;
    value > release_at
}

impl Quotas {
    pub fn new(k: Knobs) -> Arc<Quotas> {
        Arc::new(Quotas {
            snapshot: RwLock::new(HashMap::new()),
            live: Mutex::new(HashMap::new()),
            k,
            hot: AtomicBool::new(false),
            refreshed_ms: AtomicI64::new(0),
            known: AtomicI64::new(0),
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test(k: Knobs) -> Quotas {
        Quotas {
            snapshot: RwLock::new(HashMap::new()),
            live: Mutex::new(HashMap::new()),
            k,
            hot: AtomicBool::new(false),
            refreshed_ms: AtomicI64::new(0),
            known: AtomicI64::new(0),
        }
    }

    // -----------------------------------------------------------------------
    // The refresh: the only writer of the snapshot, and the only thing that
    // resets a delta.
    // -----------------------------------------------------------------------

    /// Adopt a measurement.
    ///
    /// TWO RULES DECIDE WHEN A DELTA IS CLEARED, and both are load-bearing —
    /// each one is a way to hand a tenant an unlimited budget by accident.
    ///
    /// 1. **Only for tenants the snapshot contains.** A tenant the rollup has
    ///    not reached yet — brand new, or past the top-N cap — has a measurement
    ///    of zero, so its delta is the ONLY record that it has written anything.
    ///    Clearing it would give exactly the tenants the gate cannot see a fresh
    ///    budget every refresh.
    /// 2. **Only when the measurement is genuinely NEW.** The refresh runs ten
    ///    times per rollup: the measurement it reads is usually the same one it
    ///    read last time. The delta means "written since THAT measurement", so
    ///    clearing it against an unchanged measurement re-grants the whole
    ///    headroom every 30 s and turns the enforcer back into the rate-and-time
    ///    bound §9.3 exists to remove — silently, and only under multi-broker
    ///    load. `quota_overshoot.rs` case 2 is what catches it.
    ///
    /// "New" is measurement inequality, which `computed_at` alone would nearly
    /// give (the rollup stamps it with its own `p_now`) but not quite: a cell
    /// whose rollup has stopped keeps a frozen `computed_at`, and that is
    /// precisely when the delta must keep accumulating.
    pub fn refresh(&self, rows: Vec<TenantRow>) {
        let prev: HashMap<String, Measure> = match self.snapshot.read() {
            Ok(g) => g.iter().map(|(k, v)| (k.clone(), v.measure)).collect(),
            Err(p) => p.into_inner().iter().map(|(k, v)| (k.clone(), v.measure)).collect(),
        };
        let mut next: HashMap<String, Snap> = HashMap::with_capacity(rows.len());
        let mut hot = false;
        {
            let mut live = match self.live.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            for r in rows {
                if let Some(e) = live.get_mut(&r.tenant) {
                    // The RELEASE half, and it consults the measurement alone:
                    // the delta is this broker's own and says nothing about
                    // whether the tenant as a whole has come back under.
                    if let Some(l) = r.limits {
                        if let Some(max) = l.max_rows {
                            e.blocked_rows = decide_over(
                                e.blocked_rows,
                                r.measure.kv_rows,
                                max,
                                self.k.release_percent,
                            );
                        }
                        if let Some(max) = l.max_bytes {
                            e.blocked_bytes = decide_over(
                                e.blocked_bytes,
                                r.measure.kv_bytes,
                                max,
                                self.k.release_percent,
                            );
                        }
                        if let Some(max) = l.max_timers {
                            e.blocked_timers = decide_over(
                                e.blocked_timers,
                                r.measure.timer_rows,
                                max,
                                self.k.release_percent,
                            );
                        }
                    } else {
                        e.blocked_rows = false;
                        e.blocked_bytes = false;
                        e.blocked_timers = false;
                    }
                    if prev.get(&r.tenant) != Some(&r.measure) {
                        e.d_rows = 0;
                        e.d_bytes = 0;
                        e.d_timers = 0;
                    }
                }
                hot |= is_hot(&r, self.k.hot_ratio);
                next.insert(
                    r.tenant,
                    Snap {
                        limits: r.limits,
                        measure: r.measure,
                    },
                );
            }
        }
        self.known.store(next.len() as i64, Ordering::Relaxed);
        if let Ok(mut g) = self.snapshot.write() {
            *g = next;
        }
        self.hot.store(hot, Ordering::Relaxed);
        self.refreshed_ms
            .store(crate::util::now_epoch_ms(), Ordering::Relaxed);
    }

    /// Is any tenant at or above the watermark? The sweeper reads this to decide
    /// whether the rollup runs on its slow clock or on the refresh clock (§9.3).
    pub fn hot(&self) -> bool {
        self.hot.load(Ordering::Relaxed)
    }

    /// Epoch ms of the last refresh, 0 if the gate has never seen a measurement.
    pub fn refreshed_ms(&self) -> i64 {
        self.refreshed_ms.load(Ordering::Relaxed)
    }

    /// Tenants in the last snapshot — the log line's number, not a gauge with a
    /// tenant label (§14.1).
    pub fn known(&self) -> i64 {
        self.known.load(Ordering::Relaxed)
    }

    // -----------------------------------------------------------------------
    // The gates.
    // -----------------------------------------------------------------------

    /// A read. No grant check and no occupancy check — §9.5 makes reads
    /// unconditional — but it does take a token, because rung 1 of the ladder is
    /// "the customer over their own rate slows down first, and knows it is
    /// theirs" and a read storm is the commonest way to get there.
    pub fn check_kv_read(&self, tenant: &str) -> Verdict {
        self.with_live(tenant, |k, snap, e| {
            let (rate, burst) = read_rate(k, snap);
            e.read.take(rate, burst)
        })
    }

    /// A KV write, charged at its upper bound. `add_rows`/`add_bytes` are what
    /// the call could ADD; a call that adds neither is a delete or an in-place
    /// update and is never refused on occupancy.
    pub fn charge_kv_write(&self, tenant: &str, add_rows: i64, add_bytes: i64) -> Verdict {
        self.charge(tenant, add_rows, add_bytes, 0)
    }

    /// A timer schedule, `n` timers. A reschedule counts as a schedule: it is the
    /// same upsert and, at schedule time, indistinguishable (§9.7).
    pub fn charge_timers(&self, tenant: &str, n: i64) -> Verdict {
        self.charge(tenant, 0, 0, n)
    }

    /// §9.6 — never blocked, by anything, ever. Not by quota, not by a grant, not
    /// by a rate. The fire does not stop, so the stop button must not either.
    pub fn check_timers_cancel(&self, _tenant: &str) -> Verdict {
        Verdict::Allow
    }

    /// The horizon in force for this tenant: the cell's, narrowed by the plan's
    /// if the plan's is narrower. A tenant row may only ever tighten it — a plan
    /// limit that the cell default could silently widen would be no limit
    /// (§9.2).
    pub fn horizon_ms(&self, tenant: &str, cell_ms: i64) -> i64 {
        let snap = match self.snapshot.read() {
            Ok(g) => g.get(tenant).copied(),
            Err(p) => p.into_inner().get(tenant).copied(),
        };
        match snap.and_then(|s| s.limits).and_then(|l| l.max_timer_horizon_s) {
            Some(s) if s > 0 => cell_ms.min(s.saturating_mul(1000)),
            _ => cell_ms,
        }
    }

    /// Give back a charge whose call did not commit. Saturating at zero: a
    /// refund that could take a delta negative would be a credit the tenant
    /// could bank against a later burst.
    pub fn refund(&self, tenant: &str, rows: i64, bytes: i64, timers: i64) {
        let mut live = match self.live.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if let Some(e) = live.get_mut(tenant) {
            e.d_rows = (e.d_rows - rows).max(0);
            e.d_bytes = (e.d_bytes - bytes).max(0);
            e.d_timers = (e.d_timers - timers).max(0);
        }
    }

    // -----------------------------------------------------------------------
    // Internals.
    // -----------------------------------------------------------------------

    fn charge(&self, tenant: &str, add_rows: i64, add_bytes: i64, add_timers: i64) -> Verdict {
        // Rung 3, before anything else is spent: no grant, no answer.
        let snap = match self.snapshot.read() {
            Ok(g) => g.get(tenant).copied(),
            Err(p) => p.into_inner().get(tenant).copied(),
        };
        match snap.map(|s| s.limits) {
            None if self.k.require_grant => return Verdict::NotGranted,
            Some(Some(l)) if !l.enabled => return Verdict::NotGranted,
            _ => {}
        }

        let mut live = match self.live.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        // Rung 2 of §9.4: the cap denies. A tenant the control plane knows about
        // is exempt — the map is bounded for it by whoever wrote the row, and the
        // attack this defends against is an INVENTED id, not a configured one.
        let known = snap.is_some();
        if !live.contains_key(tenant) && !known && live.len() >= self.k.cap {
            return Verdict::NoRoom;
        }
        let e = live
            .entry(tenant.to_string())
            .or_insert_with(|| Live::new(&self.k));

        // Rung 1: the bucket, before the occupancy test, because a client over
        // its rate must hear 429 ("slow down") and not 403 ("you are full").
        let (rate, burst) = write_rate(&self.k, &snap);
        let v = e.write.take(rate, burst);
        if v != Verdict::Allow {
            return v;
        }

        // Rung 6: occupancy, and it is ONLY ever the blocking half.
        //
        // THE RELEASE IS NOT HERE, and that separation is the whole of "blocco
        // veloce, rilascio lento" (§9.3). A charge sees no new measurement, so it
        // has nothing new to release ON: consulting the stale measurement here
        // would clear the block on the very next call — the measurement still
        // says zero, because the writes that filled the tenant are in the delta —
        // and the gate would then oscillate, admitting one more write per call
        // for ever. That failure passes every static reading of the code and
        // shows up only as a quota that leaks; `quota_overshoot.rs` case 2 is
        // what pins it. The release lives in `refresh`, against the true
        // measurement, with the band.
        //
        // A call that adds nothing cannot take anyone over, so deletes and
        // in-place updates never reach this test at all (§9.5).
        if let Some(Some(l)) = snap.map(|s| s.limits) {
            let m = snap.map(|s| s.measure).unwrap_or_default();
            let held = (m.kv_rows + e.d_rows, m.kv_bytes + e.d_bytes, m.timer_rows + e.d_timers);
            if over(&mut e.blocked_rows, add_rows, held.0, l.max_rows) {
                return Verdict::OverQuota(Resource::KvRows);
            }
            if over(&mut e.blocked_bytes, add_bytes, held.1, l.max_bytes) {
                return Verdict::OverQuota(Resource::KvBytes);
            }
            if over(&mut e.blocked_timers, add_timers, held.2, l.max_timers) {
                return Verdict::OverQuota(Resource::Timers);
            }
        }

        e.d_rows += add_rows.max(0);
        e.d_bytes += add_bytes.max(0);
        e.d_timers += add_timers.max(0);
        Verdict::Allow
    }

    /// Shared preamble for the read gate: resolve the snapshot, honour the cap,
    /// and run `f` against this tenant's live state.
    fn with_live<F>(&self, tenant: &str, f: F) -> Verdict
    where
        F: FnOnce(&Knobs, &Option<Snap>, &mut Live) -> Verdict,
    {
        let snap = match self.snapshot.read() {
            Ok(g) => g.get(tenant).copied(),
            Err(p) => p.into_inner().get(tenant).copied(),
        };
        let mut live = match self.live.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if !live.contains_key(tenant) && snap.is_none() && live.len() >= self.k.cap {
            return Verdict::NoRoom;
        }
        let e = live
            .entry(tenant.to_string())
            .or_insert_with(|| Live::new(&self.k));
        f(&self.k, &snap, e)
    }

    // ---- test-only introspection ------------------------------------------

    #[cfg(test)]
    pub(crate) fn live_len(&self) -> usize {
        self.live.lock().map(|g| g.len()).unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn delta_of(&self, tenant: &str) -> (i64, i64, i64) {
        self.live
            .lock()
            .ok()
            .and_then(|g| g.get(tenant).map(|e| (e.d_rows, e.d_bytes, e.d_timers)))
            .unwrap_or((0, 0, 0))
    }

    /// THE REJECTED DESIGN, kept only so `quota_overshoot.rs` can measure it.
    /// It consults the measurement alone and charges nothing — which is exactly
    /// what "the broker re-reads the measure every 30 s" amounts to when the
    /// measure is written every 300 s.
    #[cfg(test)]
    pub(crate) fn measure_only_admits(&self, tenant: &str, n: i64) -> bool {
        let snap = match self.snapshot.read() {
            Ok(g) => g.get(tenant).copied(),
            Err(p) => p.into_inner().get(tenant).copied(),
        };
        match snap {
            Some(s) => match s.limits.and_then(|l| l.max_timers) {
                Some(max) => s.measure.timer_rows + n <= max,
                None => true,
            },
            None => true,
        }
    }
}

/// One resource's blocking half: already blocked stays blocked (only a refresh
/// releases), and otherwise the test is "would THIS call take the tenant over?".
/// A call that adds nothing, or a resource with no cap, never blocks.
fn over(blocked: &mut bool, add: i64, held: i64, max: Option<i64>) -> bool {
    if add <= 0 {
        return false;
    }
    let Some(max) = max else { return false };
    if *blocked {
        return true;
    }
    if held + add > max {
        *blocked = true;
        return true;
    }
    false
}

/// Is this tenant at or above the watermark on any capped resource? An
/// unlimited resource has no ratio: 0, never a division by zero and never a
/// fake 1.0 (the same rule `kv_usage_step_v1` applies to the gauges).
fn is_hot(r: &TenantRow, ratio: f64) -> bool {
    let Some(l) = r.limits else { return false };
    let over = |v: i64, max: Option<i64>| match max {
        Some(m) if m > 0 => v as f64 / m as f64 >= ratio,
        _ => false,
    };
    over(r.measure.kv_rows, l.max_rows)
        || over(r.measure.kv_bytes, l.max_bytes)
        || over(r.measure.timer_rows, l.max_timers)
}

fn read_rate(k: &Knobs, snap: &Option<Snap>) -> (u32, u32) {
    match snap.and_then(|s| s.limits).and_then(|l| l.max_reads_per_sec) {
        Some(r) if r > 0 => (r, r.max(1)),
        _ => (k.read_rate, k.read_burst),
    }
}

fn write_rate(k: &Knobs, snap: &Option<Snap>) -> (u32, u32) {
    match snap.and_then(|s| s.limits).and_then(|l| l.max_writes_per_sec) {
        Some(r) if r > 0 => (r, r.max(1)),
        _ => (k.write_rate, k.write_burst),
    }
}

// ===========================================================================
// The refresh loop (§9.3): one read, on its own clock, on EVERY broker.
// ===========================================================================

/// Build the gate from the resolved configuration.
pub fn from_config(cfg: &crate::config::Config) -> Arc<Quotas> {
    Quotas::new(Knobs {
        cap: cfg.kv_max_tenants,
        require_grant: cfg.kv_require_grant,
        release_percent: cfg.kv_quota_release_percent,
        read_rate: cfg.kv_read_rate,
        read_burst: cfg.kv_read_burst,
        write_rate: cfg.kv_write_rate,
        write_burst: cfg.kv_write_burst,
        hot_ratio: cfg.kv_quota_hot_percent as f64 / 100.0,
    })
}

static REFRESH_ERR: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

fn parse(txt: &str) -> Vec<TenantRow> {
    let v: serde_json::Value = match serde_json::from_str(txt) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let Some(arr) = v.as_array() else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|t| {
            let tenant = t.get("tenant")?.as_str()?.to_string();
            let granted = t.get("granted").and_then(|x| x.as_bool()).unwrap_or(false);
            let num = |k: &str| t.get(k).and_then(|x| x.as_i64());
            let small = |k: &str| num(k).map(|n| n.clamp(0, u32::MAX as i64) as u32);
            Some(TenantRow {
                tenant,
                // A tenant with usage but NO quota row carries no limits at all,
                // which the gate reads as "unlimited, unless a grant is
                // required". The distinction is the whole of §9.4 point 1 and it
                // must survive the JSON: `granted:false` is not the same as
                // `enabled:false`, and collapsing them would either deny a
                // tenant the operator never restricted or admit one they never
                // authorised.
                limits: granted.then(|| Limits {
                    enabled: t.get("enabled").and_then(|x| x.as_bool()).unwrap_or(true),
                    max_rows: num("maxRows"),
                    max_bytes: num("maxBytes"),
                    max_timers: num("maxTimers"),
                    max_timer_horizon_s: num("maxHorizonS"),
                    max_reads_per_sec: small("maxReadsPerSec"),
                    max_writes_per_sec: small("maxWritesPerSec"),
                }),
                measure: Measure {
                    kv_rows: num("kvRows").unwrap_or(0),
                    kv_bytes: num("kvBytes").unwrap_or(0),
                    timer_rows: num("timerRows").unwrap_or(0),
                    computed_at_ms: num("computedAtMs").unwrap_or(0),
                },
            })
        })
        .collect()
}

/// One refresh. Separated from the loop so the boot can await ONE before the
/// listener opens: with `require_grant` on, an empty snapshot denies, and a cell
/// that answered `feature_gated` to everybody for the first 30 seconds of every
/// rollout would look exactly like an outage.
pub async fn refresh_once(
    q: &Quotas,
    pool: &deadpool_postgres::Pool,
    max_tenants: usize,
) -> Result<usize, String> {
    let client = pool.get().await.map_err(|e| e.to_string())?;
    let txt = crate::db::kv_quota_refresh(&client, max_tenants.clamp(1, i32::MAX as usize) as i32)
        .await
        .map_err(|e| {
            // Name the 42883 case, because it is the broker-new/database-old
            // shape and its remedy is one environment variable away.
            match e.as_db_error().map(|d| d.code().code().to_string()) {
                Some(code) if code.starts_with("42") => format!(
                    "{code}: queen.kv_quota_refresh_v1 is missing or does not match — apply the \
                     schema (QUEEN_APPLY_SCHEMA=1) and restart"
                ),
                _ => e.to_string(),
            }
        })?;
    let rows = parse(&txt);
    let n = rows.len();
    q.refresh(rows);
    Ok(n)
}

/// The periodic refresh. EVERY broker runs it, including one with
/// `QUEEN_SWEEPER=false`: that broker does not produce the measurement, but it
/// still has to enforce against it.
///
/// Returns the handle so the embedded broker can abort it on `shutdown()`; the
/// binary simply drops it, as it does for every other background loop.
pub fn spawn_refresh(
    q: Arc<Quotas>,
    pool: deadpool_postgres::Pool,
    interval_ms: u64,
    max_tenants: usize,
) -> tokio::task::JoinHandle<()> {
    let interval = std::time::Duration::from_millis(interval_ms.max(1000));
    tracing::info!(
        target: "quota",
        interval_ms = interval.as_millis() as u64,
        max_tenants,
        require_grant = q.k.require_grant,
        "kv/timers quota refresh started"
    );
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            match refresh_once(&q, &pool, max_tenants).await {
                Ok(n) => {
                    tracing::debug!(target: "quota", tenants = n, hot = q.hot(), "quota refreshed");
                }
                Err(e) => {
                    // Logged and swallowed: the loop must survive a transient DB
                    // outage, and a stale measurement blocks nothing by itself —
                    // the local delta keeps accumulating against the last one it
                    // adopted, which remains a correct lower bound (§12).
                    if let Some(suppressed) = REFRESH_ERR.tick_now() {
                        tracing::warn!(
                            target: "quota", error = %e, suppressed,
                            "quota refresh failed; the enforcer keeps using the last measurement \
                             plus its local delta, so nothing is unblocked by this"
                        );
                    }
                }
            }
        }
    })
}

// ===========================================================================
// Tests (PLAN_KV_TIMERS §15). Written before this module; see
// `src/tests_unit/README.md`.
// ===========================================================================

#[cfg(test)]
#[path = "tests_unit/quota_gate.rs"]
mod quota_gate_tests;

#[cfg(test)]
#[path = "tests_unit/quota_overshoot.rs"]
mod quota_overshoot_tests;
