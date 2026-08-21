//! The sweeper: ONE background component, TWO clocks (PLAN_KV_TIMERS.md §7).
//!
//! It fires due timers out of `queen.log_timers` into the log, and it prunes
//! expired rows out of `queen.kv`. The asymmetry between the two is a principle
//! and not a convenience (§1.7): a late fire is product latency and the customer
//! sees it, while a late KV prune is invisible — `kv_live_v1` already hides an
//! expired row on the first read — and costs only table size. So the fire is
//! DUE-DRIVEN on `min(visible_at)` and the prune has its own slow sub-cadence,
//! the same shape `retention.rs` gives `PARTITION_SWEEP_EVERY` ("this phase is
//! O(space), not O(work), so it gets its own clock").
//!
//! ---------------------------------------------------------------------------
//! LOCK ORDER (§2), because this file is one of the seven actors in that proof
//!
//!     queen.kv -> queen.log_timers -> ADV -> queen.queues
//!               -> queen.log_partitions -> queen.log_consumers -> leaves
//!
//! The rule, and the word `ACQUIRE` is load-bearing: **no actor may ACQUIRE a
//! lock on `queen.kv` or `queen.log_timers` after acquiring one on
//! `queen.queues`, `queen.log_partitions` or `queen.log_consumers`.** Re-touching
//! a row THIS transaction already holds is not an acquisition — it is a no-op on
//! the wait-for graph — which is what makes the fire's final DELETE legal.
//!
//! What this file contributes to the proof, actor by actor (§2.3):
//!
//!   * actor 5, the CLAIM: `queen.log_timers` only, `FOR UPDATE SKIP LOCKED`
//!     inside the SP. **It never waits**, so it cannot be an edge of the graph
//!     whatever order it visits;
//!   * actor 6, the FIRE: `T(verify, SKIP LOCKED) -> Q -> P -> T(delete of rows
//!     already held)`. It waits on Q and P, **never on T**;
//!   * actor 7, the KV PRUNE: `queen.kv` only, `SKIP LOCKED`. Never waits;
//!   * actor 8, the USAGE ROLLUP: reads without row locks, writes one leaf.
//!
//! And what this file must never grow:
//!
//!   * **no advisory lock.** The sweeper takes none (§1.9), so it consumes no
//!     number and cannot close a cycle through the advisory space. Whoever adds
//!     one later must take it BEFORE touching kv or timers, or not at all.
//!     `737_003` stays reserved for `PLAN_S3_ARCHIVE.md`, which claimed it first;
//!   * **no leader gate.** `retention.rs:76` takes session advisory `737_001` and
//!     exactly one replica sweeps per cycle; `stats.rs` takes `737_002`. This
//!     loop is deliberately the opposite on both axes — due-driven and leaderless
//!     — because every replica must drain in parallel, sharing the work through
//!     `SKIP LOCKED`;
//!   * **no ownership of shards.** `QUEEN_SWEEPER_SHARDS` does not exist and must
//!     not (§1.10). The 64 shards SPREAD contention; they do not partition
//!     responsibility. Every broker scans every shard, and the only thing derived
//!     from this process's identity is which shard it STARTS at (rotated per
//!     cycle) so that N brokers do not all queue on the same index head. Any
//!     ownership scheme orphans the shards of a dead broker, and an orphaned
//!     timer never fires — the worst failure this feature can have;
//!   * **no `LISTEN`/`NOTIFY`, of any kind** (§7.4, decided). It would need a
//!     dedicated connection held open forever OUTSIDE the pool, which deadpool
//!     does not have; and PostgreSQL's notify queue is a shared 8 GB area which,
//!     when full, makes the COMMITTING transaction fail — a failure of the
//!     wake-up would become a failure of the user's schedule. A wake-up must be
//!     losable and must never do damage. The wake here is local and in-process;
//!   * **no mesh frame.** `T_TIMER_DUE` is cut from v1 (§18.3). The net that
//!     makes it unnecessary is `QUEEN_SWEEPER_MAX_SLEEP_MS` (1 s), which IS the
//!     worst-case recovery window for a timer scheduled on another broker. A
//!     multi-broker cluster with no mesh is correct, with at most one second of
//!     delay, because `deliverAt` is "no earlier than" and never "exactly at".
//!     Nobody may tie the correctness of this feature to the mesh.
//!
//! ---------------------------------------------------------------------------
//! THE CYCLE (§7.2)
//!
//!   A. PROBE      one read-only call, one server `now()`
//!   B. DRAIN      claim and fire in the SAME iteration, never 5000 rows ahead
//!   C. SUB-CLOCKS usage rollup, then KV prune — each on its own clock
//!   D. SLEEP      due-driven, clamped, interruptible by the local wake
//!
//! **Claim and fire in the same iteration, with a ceiling on work in flight.**
//! The naive form drains `CYCLE_MAX_ROWS` in 25 rounds; if the fires are slow —
//! which happens precisely under load, contending for partitions with the pushes
//! — the first round's claims expire before the twenty-fifth fire commits.
//! Another broker re-claims them, our fire finds different tokens, every segment
//! comes back `stale`, the packing is thrown away, and under sustained load two
//! brokers steal work from each other while `lateMs` climbs and `fired` stays
//! low: a LIVELOCK observable only as "the sweeper is slow". The ceiling is
//! `lease_budget_ok()` — stop claiming once this pass has spent half of
//! `QUEEN_SWEEPER_LEASE_MS`.
//!
//! **The fire transaction's ceiling is in BYTES, not segments.** Retention's
//! lesson is that ITS step cost is per ROW, so a bigger batch absorbs no more
//! work and only stalls pushes; here the cost is per byte of WAL, so the cap is
//! `QUEEN_SWEEPER_MAX_FIRE_BYTES` and a batch over it splits into several calls,
//! each its own transaction.
//!
//! **Backoff with jitter on an empty claim.** When `nextInMs <= 0` but the claim
//! takes nothing because another broker is draining, a 5 ms floor is a SPIN:
//! five brokers, one burst, one drains and four probe 200 times a second each
//! (64 seeks plus a count apiece), roughly 800 extra queries per second landing
//! exactly while the fire is writing WAL and the producers are competing for the
//! same fsync. The empty claim backs off into a jittered 25..200 ms band; the
//! floor is only for when work is actually available.
//!
//! ---------------------------------------------------------------------------
//! PRIORITY AND DEGRADATION (§12.1)
//!
//! Internal priority, from the top: **fire, then usage rollup, then KV prune**,
//! and it sheds from the BOTTOM. The KV prune goes first because it is the only
//! phase whose cost is purely disk — reads stay perfectly correct while the
//! table grows, which is why `queen_kv_expired_not_pruned` exists as an alarm.
//! The rollup goes second: it is precision, not delivery.
//!
//! **The fire never switches itself off.** Under pressure the sweeper shrinks the
//! batch and lengthens the sleep, and the visible result is `fire_lag` rising.
//! Turning it off would convert a delay into what the customer reads as loss.
//! The two runtime kill switches are DISTINCT (`timers_schedule_enabled`,
//! `timers_fire_enabled`) because the halves have opposite costs — stopping the
//! schedule is harmless and instant, stopping the fire accumulates promised work
//! — and they are operator switches in `queen.system_state`, not this loop's.
//!
//! ---------------------------------------------------------------------------
//! INHERITED RULES, one by one (§7.2)
//!
//!   * **slot before connection, always**: `admission::lane_slot(Lane::Maint)`
//!     and THEN `pool.get()`. Inverting it deadlocks the pool under load;
//!   * lane `Maint`, never `Push`: the lanes were validated at four and this
//!     feature does not re-tune them;
//!   * a failing cycle is **logged and swallowed** so the loop survives, with the
//!     ERROR rate-limited by an `obs::Sampler`;
//!   * **speak only when it worked**: an idle cluster must not write a line per
//!     second, so a working cycle is INFO and an idle one is DEBUG;
//!   * **timeout and cancellation on every call**, delegated to
//!     `db::resolve_query_timeout`: a fire wedged behind a partition lock whose
//!     backend keeps running would hold locks on TIMER rows indefinitely, and
//!     those are what block a user's cancel;
//!   * **zero `unwrap`**: `panic = "abort"`, so a panic in this task takes the
//!     broker with it.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;
use tokio::sync::Notify;

use crate::config::Config;
use crate::db::{self, SqlClass, TimerClaim};
use crate::frames::{pack_segment, uuid_string_to_bytes, zstd_decompress, FrameIn};
use crate::metrics::{FireFailure, FireResult, Metrics, SweepPhase};

/// Ceiling on `QUEEN_SWEEPER_PARALLELISM`, mirroring `retention::MAX_PARALLELISM`
/// including its compile-time floor assertion.
///
/// The DEFAULT is 1, and that is a decision rather than caution (§7.3): the fire
/// is a WRITING transaction that holds partition locks, not a maintenance delete,
/// so two concurrent fires on the same partitions contend for the very
/// serializer the pushes use. It errs on the side of the message path. Raise it
/// only if `lateMs` grows under load AND a profile says the bottleneck is the
/// round trip rather than the commit.
pub const MAX_PARALLELISM: usize = 8;
const _: () = assert!(MAX_PARALLELISM >= 2);

/// FIXED AT 64 FOREVER, matching the `shard` generated column of both tables.
/// The always-virgin model covers the SCHEMA, not the DATA: changing this would
/// silently re-shard rows already written. There is no env for it and there must
/// never be one (§1.10).
const SHARDS: i16 = 64;

/// EPHEMERAL_QUEUES.md §3.2 — cadence of the ephemeral BACKSTOP phase.
///
/// A constant and not a knob, deliberately: it is not a lever on anything an
/// operator can observe. Lease expiry, ttl head-drop and implicit GC all happen
/// opportunistically on every ring touch, so this pass only ever finds work on
/// rings NOBODY is touching — and for those the deadline that matters is the
/// lease, which `Wake::hint` already rings to the millisecond. One second is
/// simply the floor under a lost hint.
const EPH_SWEEP_EVERY: Duration = Duration::from_millis(1000);

// ===========================================================================
// The sleep computation (§7.1 step D, §7.2)
// ===========================================================================

/// Absolute floor, below which no arithmetic may take the loop.
///
/// Every knob below arrives from the environment, and
/// `QUEEN_SWEEPER_MIN_SLEEP_MS=0` is a one-keystroke typo. A background loop
/// that sleeps zero is a busy wait against the pool, on a `Lane::Maint` slot.
pub(crate) const ABSOLUTE_FLOOR_MS: u64 = 1;

/// The four bands the sleep is computed from. Defaults are the plan's numbers,
/// not taste: each one is a delivery property written down in the documentation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SleepKnobs {
    /// `QUEEN_SWEEPER_MIN_SLEEP_MS` (5). Floor when there IS work.
    pub min_ms: u64,
    /// `QUEEN_SWEEPER_MAX_SLEEP_MS` (1000). With the mesh frame cut, this IS the
    /// worst-case recovery window for a timer scheduled on another broker (§7.4).
    pub max_ms: u64,
    /// `QUEEN_SWEEPER_IDLE_MAX_SLEEP_MS` (30 000). Empty-table backoff cap (§7.1).
    pub idle_max_ms: u64,
    /// `QUEEN_SWEEPER_IDLE_AFTER_CYCLES` (5). Consecutive idle cycles before the
    /// backoff engages, so a table that just drained keeps the normal ceiling.
    pub idle_after: u32,
    /// `QUEEN_SWEEPER_EMPTY_CLAIM_MIN_MS` (25) / `..._MAX_MS` (200): the
    /// anti-spin band of §7.2.
    pub empty_claim_min_ms: u64,
    pub empty_claim_max_ms: u64,
}

impl SleepKnobs {
    pub(crate) fn defaults() -> Self {
        SleepKnobs {
            min_ms: 5,
            max_ms: 1000,
            idle_max_ms: 30_000,
            idle_after: 5,
            empty_claim_min_ms: 25,
            empty_claim_max_ms: 200,
        }
    }
}

/// What the cycle found, which is the only input the sleep needs besides the
/// knobs and the idle counter.
#[derive(Clone, Copy, Debug)]
pub(crate) enum CycleOutcome {
    /// The probe answered with `nextInMs`, which may be zero or negative: the
    /// value is computed by the SERVER and the broker never does timestamp
    /// arithmetic (§6.2).
    Due { next_in_ms: i64 },
    /// Work was due and the claim took nothing — another broker is draining it.
    EmptyClaim,
    /// `nextInMs` was NULL (empty table) and nothing was pruned.
    Idle,
}

/// Pure: no clock, no RNG, no environment. `jitter` is the caller's random draw
/// so the band is assertable at both ends instead of sampled and hoped over, and
/// so the empty-table cycle cost the perf gate measures is a deterministic
/// function of the configuration.
pub(crate) fn sleep_ms(o: CycleOutcome, idle_cycles: u32, k: &SleepKnobs, jitter: f64) -> u64 {
    // `u64::clamp` PANICS when min > max, and `QUEEN_SWEEPER_MIN_SLEEP_MS=5000`
    // against the default ceiling is a legal thing for an operator to type. The
    // floor wins, because it is the one somebody set on purpose.
    let band = |v: u64| v.min(k.max_ms).max(k.min_ms);
    let ms = match o {
        CycleOutcome::Due { next_in_ms } => {
            // Overdue (<= 0) means "go again now", but through the floor: a zero
            // sleep on a claim that keeps returning rows is a hot loop holding a
            // Maint slot and a pooled connection.
            band(next_in_ms.max(0) as u64)
        }
        CycleOutcome::EmptyClaim => {
            // NaN is the draw that matters: `f64::clamp` PROPAGATES NaN and
            // `NaN as u64` is 0 in Rust, so a careless refactor would turn the
            // anti-spin backoff into the spin it exists to prevent. A non-finite
            // draw degrades to the bottom of the band, never to zero.
            let j = if jitter.is_finite() { jitter.clamp(0.0, 1.0) } else { 0.0 };
            let lo = k.empty_claim_min_ms;
            let span = k.empty_claim_max_ms as f64 - lo as f64;
            let v = lo as f64 + j * span;
            let v = if v.is_finite() && v >= 0.0 { v as u64 } else { lo };
            band(v)
        }
        CycleOutcome::Idle => {
            if idle_cycles <= k.idle_after {
                // Just drained is not the same as long empty: keep the ceiling.
                band(k.max_ms)
            } else {
                // Doubling, saturating: `max_ms << n` is UB past 63 shifts, and a
                // cell that has been up a month with both tables empty gets a
                // very large counter. NOT clamped to `max_ms` — that ceiling is a
                // net for DELIVERY latency and has no meaning when there is
                // nothing to deliver (§7.1).
                let shift = idle_cycles - k.idle_after;
                k.max_ms.checked_shl(shift).unwrap_or(u64::MAX).min(k.idle_max_ms)
            }
        }
    };
    ms.max(ABSOLUTE_FLOOR_MS)
}

// ===========================================================================
// The local wake-up (§7.4)
// ===========================================================================

/// The in-process, best-effort wake: an `AtomicI64` holding the nearest locally
/// committed `deliver_at` plus a `Notify`.
///
/// The handler of `POST /api/v1/timers` and the transaction wire ring it **after
/// the commit and never before** — a wake for a transaction that then rolls back
/// costs a wasted cycle and, worse, teaches the loop that work exists which does
/// not. The cost is one CAS, and the anti-storm property is free: the hint only
/// applies when the new minimum is EARLIER, so scheduling a million timers for
/// next week produces exactly ONE wake.
///
/// Losing a hint costs latency and never correctness: `QUEEN_SWEEPER_MAX_SLEEP_MS`
/// is the recovery window and `deliverAt` is "no earlier than".
pub struct Wake {
    /// Epoch ms of the nearest hinted delivery, `i64::MAX` when nothing is
    /// pending. Re-armed at the top of every cycle.
    earliest_ms: AtomicI64,
    notify: Notify,
}

impl Default for Wake {
    fn default() -> Self {
        Self::new()
    }
}

impl Wake {
    pub fn new() -> Self {
        Wake { earliest_ms: AtomicI64::new(i64::MAX), notify: Notify::new() }
    }

    /// Ring for a timer that becomes due in `delay_ms`. A past or negative delay
    /// is legal (§4.2: a `deliverAt` in the past fires on the first cycle).
    pub fn hint(&self, delay_ms: i64) {
        let at = crate::util::now_epoch_ms().saturating_add(delay_ms.max(0));
        loop {
            let cur = self.earliest_ms.load(Ordering::Relaxed);
            if at >= cur {
                return; // not earlier than what we already promised to wake for
            }
            if self
                .earliest_ms
                .compare_exchange_weak(cur, at, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                // `notify_one` stores a permit when nobody is waiting, so a hint
                // that lands mid-cycle is not lost.
                self.notify.notify_one();
                return;
            }
        }
    }

    /// Re-arm at the top of a cycle: anything hinted from here on is news.
    fn arm(&self) {
        self.earliest_ms.store(i64::MAX, Ordering::Relaxed);
    }

    async fn notified(&self) {
        self.notify.notified().await;
    }
}

static WAKE: OnceLock<Arc<Wake>> = OnceLock::new();

/// The process-wide waker. Process-global rather than a field of `AppState`
/// because the embedded facade, the HTTP handlers and this loop all need the
/// same one, and a second `Broker` in one process shares the first one's sweeper
/// exactly as it shares its admission arbiter.
pub fn wake() -> &'static Arc<Wake> {
    WAKE.get_or_init(|| Arc::new(Wake::new()))
}

/// One-liner for the two commit seams (§7.4). Safe to call whether or not the
/// sweeper task was ever spawned.
#[allow(dead_code)] // no caller until the timers handler and the wire ring it (F4)
pub fn hint_in_ms(delay_ms: i64) {
    wake().hint(delay_ms);
}

// ===========================================================================
// Spawn
// ===========================================================================

/// Everything the loop needs, resolved once at boot so the detached task owns
/// plain values instead of a `&Config`.
#[derive(Clone)]
struct Knobs {
    sleep: SleepKnobs,
    lease_ms: i32,
    claim_batch: i32,
    cycle_max_rows: i64,
    due_cap: i32,
    max_fire_bytes: usize,
    max_attempts: i32,
    per_tenant: i32,
    isolate_on_permanent: bool,
    backoff_min_ms: i64,
    backoff_max_ms: i64,
    /// Fixed short backoff for a TRANSIENT failure. Deliberately not exponential
    /// and deliberately not counted: the DLQ budget is never spent on
    /// infrastructure (§12).
    transient_backoff_ms: i32,
    zstd_level: i32,
    stmt_timeout: Duration,
    kv_expire_every: Duration,
    kv_expire_batch: i32,
    usage_every: Duration,
    /// The cadence the usage rollup drops to once ANY tenant is above the
    /// watermark (§9.3). It is the quota REFRESH period, and the equality is the
    /// point rather than a coincidence: the published overrun bound is
    /// `rate x (rollup + refresh + duration)`, and no reader can be fresher than
    /// its writer — so for the tenants that are actually near a limit, the writer
    /// is made as fast as the reader and the first term stops dominating.
    usage_hot_every: Duration,
    parallelism: usize,
}

/// Launch the sweeper. Returns the `JoinHandle` so `embedded/boot.rs` can
/// `tasks.push(...)` it instead of growing a fourth hand-written copy of the
/// boot sequence (§7.1); `main.rs` ignores it, as it does for every other
/// background loop.
///
/// **`None` means the task was not spawned at all**, and now only
/// `QUEEN_SWEEPER=false` can produce it. It used to also return `None` when both
/// kv/timers boot flags were off, so that an installation which would never use
/// either paid nothing for them; those flags are gone (see the header of
/// `switches.rs`), the surfaces are live on every cell, and a surface with expiry
/// and a due time without its reaper only accumulates.
pub fn spawn(
    pool: Pool,
    metrics: Arc<Metrics>,
    cfg: &Config,
    switches: Arc<crate::switches::Switches>,
    quotas: Arc<crate::quota::Quotas>,
    ephemeral: Arc<crate::ephemeral::Ephemeral>,
) -> Option<tokio::task::JoinHandle<()>> {
    if !cfg.sweeper_enabled {
        // NOTE for the ephemeral backstop (EPHEMERAL_QUEUES.md §3.2): with the
        // sweeper off, an ephemeral ring that nobody pushes to or pops from
        // never runs its lease expiry, its ttl drop or its implicit GC. Nothing
        // is lost and nothing is served wrongly — every one of those also runs
        // on the next touch — but an idle cell stops reclaiming. Said here
        // rather than in a second warn line, because the boot warning main.rs
        // already prints for this flag is the one an operator reads.
        tracing::info!(target: "sweeper", "disabled by QUEEN_SWEEPER=false");
        return None;
    }
    let knobs = Knobs {
        // The plan's numbers live in exactly one place, `SleepKnobs::defaults()`,
        // and are the env defaults here rather than being written twice.
        sleep: {
            let d = SleepKnobs::defaults();
            SleepKnobs {
                min_ms: cfg.sweeper_min_sleep_ms,
                max_ms: cfg.sweeper_max_sleep_ms,
                idle_max_ms: cfg.sweeper_idle_max_sleep_ms,
                // Not in `Config` yet — see the note on `env_u64` below.
                idle_after: env_u64("QUEEN_SWEEPER_IDLE_AFTER_CYCLES", d.idle_after as u64)
                    .clamp(1, 1000) as u32,
                empty_claim_min_ms: env_u64(
                    "QUEEN_SWEEPER_EMPTY_CLAIM_MIN_MS",
                    d.empty_claim_min_ms,
                )
                .max(1),
                empty_claim_max_ms: env_u64(
                    "QUEEN_SWEEPER_EMPTY_CLAIM_MAX_MS",
                    d.empty_claim_max_ms,
                )
                .max(1),
            }
        },
        lease_ms: clamp_i32(cfg.sweeper_lease_ms as i64),
        claim_batch: clamp_i32(cfg.sweeper_claim_batch),
        cycle_max_rows: cfg.sweeper_cycle_max_rows,
        due_cap: clamp_i32(cfg.sweeper_due_cap),
        max_fire_bytes: cfg.sweeper_max_fire_bytes,
        max_attempts: cfg.sweeper_max_attempts,
        per_tenant: clamp_i32(cfg.sweeper_fire_rate_per_tenant),
        isolate_on_permanent: cfg.sweeper_isolate_on_permanent,
        backoff_min_ms: cfg.sweeper_backoff_min_ms,
        backoff_max_ms: cfg.sweeper_backoff_max_ms,
        transient_backoff_ms: clamp_i32(
            env_u64("QUEEN_SWEEPER_TRANSIENT_BACKOFF_MS", 1000).max(1) as i64,
        ),
        zstd_level: cfg.zstd_level,
        stmt_timeout: cfg.stmt_timeout,
        kv_expire_every: Duration::from_millis(env_u64("QUEEN_KV_EXPIRE_EVERY_MS", 1000).max(1)),
        kv_expire_batch: clamp_i32(env_u64("QUEEN_KV_EXPIRE_BATCH", 1000).max(1) as i64),
        usage_every: Duration::from_millis(cfg.kv_usage_every_ms.max(1000)),
        // Never SLOWER than the cold cadence: `min` and not the raw refresh
        // period, so an operator who sets a refresh interval longer than the
        // rollup interval does not accidentally make the hot path the slow one.
        usage_hot_every: Duration::from_millis(
            cfg.kv_quota_refresh_ms.max(1000).min(cfg.kv_usage_every_ms.max(1000)),
        ),
        parallelism: cfg.sweeper_parallelism.clamp(1, MAX_PARALLELISM),
    };
    tracing::info!(
        target: "sweeper",
        sleep_ms = %format!("{}..{} (idle {})", knobs.sleep.min_ms, knobs.sleep.max_ms, knobs.sleep.idle_max_ms),
        lease_ms = knobs.lease_ms,
        claim_batch = knobs.claim_batch,
        cycle_max_rows = knobs.cycle_max_rows,
        max_fire_bytes = knobs.max_fire_bytes,
        parallelism = knobs.parallelism,
        shards = SHARDS,
        // Said out loud because somebody WILL go looking for the env var: the
        // shards spread contention, they do not partition ownership (§1.10).
        leaderless = true,
        "service started"
    );
    Some(tokio::spawn(async move {
        run_loop(pool, metrics, knobs, switches, quotas, ephemeral).await
    }))
}

/// `config.rs` owns the env parsers and keeps them private; three knobs of §7
/// have no `Config` field yet (`QUEEN_SWEEPER_IDLE_AFTER_CYCLES`,
/// `QUEEN_SWEEPER_EMPTY_CLAIM_*`, `QUEEN_KV_EXPIRE_*`,
/// `QUEEN_SWEEPER_TRANSIENT_BACKOFF_MS`). They are read here with the same
/// semantics (`unset or unparseable => the documented default`) until they move
/// into `Config` beside the rest of the sweeper block, which is where
/// `gen-config.mjs` will look for them.
fn env_u64(k: &str, def: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(def)
}

fn clamp_i32(v: i64) -> i32 {
    v.clamp(1, i32::MAX as i64) as i32
}

// ===========================================================================
// The loop
// ===========================================================================

/// A sub-cadence with its own clock, in the shape of retention's
/// `PARTITION_SWEEP_EVERY`: it starts DUE so the first cycle after boot runs it,
/// and it is only advanced when the phase actually ran, so a cycle that shed the
/// phase does not consume its interval.
struct Gate {
    last: Option<Instant>,
    every: Duration,
}

impl Gate {
    fn new(every: Duration) -> Self {
        Gate { last: None, every }
    }
    fn due(&self, now: Instant) -> bool {
        self.due_every(now, self.every)
    }
    /// Same gate against a cadence supplied by the caller, for the one phase
    /// whose period is not constant: the usage rollup runs on its slow clock
    /// normally and on the quota refresh clock once any tenant is above the
    /// watermark (§9.3).
    fn due_every(&self, now: Instant, every: Duration) -> bool {
        self.last.is_none_or(|at| now.duration_since(at) >= every)
    }
    fn mark(&mut self, now: Instant) {
        self.last = Some(now);
    }
}

/// A phase that a class-42 answer has switched off for the life of the process.
///
/// A missing stored procedure (`42883`) is the broker-new/database-old case the
/// plan declares (§6.3): configuration, permanent, and NOT retryable. Retrying
/// it once a second forever would fill the log and burn a Maint slot for an
/// answer that cannot change without a restart, so the phase stops and says so
/// once, loudly, naming what to fix.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PhaseState {
    On,
    OffConfig,
}

static CYCLE_ERR: crate::obs::Sampler = crate::obs::Sampler::new(30_000);
static FIRE_ERR: crate::obs::Sampler = crate::obs::Sampler::new(30_000);
static PRUNE_ERR: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
static USAGE_ERR: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
static POISON_WARN: crate::obs::Sampler = crate::obs::Sampler::new(30_000);

async fn run_loop(
    pool: Pool,
    metrics: Arc<Metrics>,
    k: Knobs,
    switches: Arc<crate::switches::Switches>,
    quotas: Arc<crate::quota::Quotas>,
    ephemeral: Arc<crate::ephemeral::Ephemeral>,
) {
    let mut idle_cycles: u32 = 0;
    // Consecutive cycles in which the fire pass hit a ceiling. This is the ONLY
    // input to the degradation ladder, and the ladder only ever sheds phases
    // BELOW the fire (§12.1).
    let mut pressure: u32 = 0;
    let mut kv_gate = Gate::new(k.kv_expire_every);
    let mut usage_gate = Gate::new(k.usage_every);
    let mut eph_gate = Gate::new(EPH_SWEEP_EVERY);
    let mut kv_phase = PhaseState::On;
    let mut usage_phase = PhaseState::On;
    // Which shard this broker starts its scan at, rotated every cycle so N
    // brokers do not all queue on the same head of the index. Seeded from the
    // process identity, NOT from a configured id: this decides an order, never
    // an ownership (§1.10).
    let mut rot: i16 = (crate::util::now_epoch_ms() as i16).rem_euclid(SHARDS);

    loop {
        let cycle_start = Instant::now();
        // Re-arm BEFORE the work: a hint that lands while we are firing must
        // survive into this cycle's sleep decision.
        wake().arm();
        rot = (rot + 1).rem_euclid(SHARDS);
        let shards = shard_ring(rot);

        let mut outcome = CycleOutcome::Idle;
        let mut worked = false;
        let mut fired_rows: u64 = 0;

        // ------------------------------------------------------------- A + B
        // The fire. NEVER shed automatically, whatever the pressure — under load
        // it takes smaller batches and sleeps longer, and `fire_lag` rising is
        // the visible result. The ONE thing that stops it is an operator's
        // `timers_fire_enabled`, and that switch is separate from
        // `timers_schedule_enabled` because the two halves have opposite costs:
        // pausing the schedule promises nothing new, while pausing the fire
        // accumulates work already promised, which a customer reads as loss.
        //
        // No lost timer: `deliverAt` is "not before", so the backlog drains from
        // the oldest when the switch goes back on. The visible cost is
        // `queen_timers_due_backlog`, and that is exactly what an operator who
        // flipped this switch should be watching.
        // The transition is logged by `Switches::flip`, at the instant of the
        // flip and on every broker including those that do not sweep — so this
        // loop deliberately does NOT log it again. Two lines for one event is how
        // a log stops being read.
        if switches.fire_allowed() {
            let t0 = Instant::now();
            match fire_pass(&pool, &metrics, &k, &shards, cycle_budget(&k, pressure)).await {
                Ok(pass) => {
                    fired_rows = pass.rows;
                    worked |= pass.rows > 0;
                    outcome = pass.outcome;
                    if pass.capped {
                        pressure = pressure.saturating_add(1);
                    } else {
                        pressure = 0;
                    }
                    metrics.kvt.sweeper_cycle(
                        SweepPhase::Fire,
                        t0.elapsed().as_secs_f64() * 1000.0,
                        pass.rows,
                    );
                }
                Err(e) => {
                    // A whole-pass failure (the probe, the claim, the pool) is
                    // logged and swallowed: the loop must survive a transient DB
                    // outage. Per-batch fire failures are handled inside the
                    // pass, where the timers they concern are known.
                    if let Some(suppressed) = FIRE_ERR.tick_now() {
                        tracing::error!(
                            target: "sweeper", error = %e.describe(), suppressed, "fire pass error"
                        );
                    }
                    pressure = pressure.saturating_add(1);
                    outcome = CycleOutcome::EmptyClaim;
                }
            }
        }

        // ----------------------------------------------------------------- C
        // Sub-cadences, in PRIORITY order and shed from the BOTTOM (§12.1):
        // stage 3 drops the KV prune (it is the only phase whose cost is purely
        // disk — reads stay correct while the table grows), stage 4 also drops
        // the usage rollup (precision, not delivery). The fire is NOT on this
        // ladder at any rung: under pressure it shrinks its batch and lengthens
        // its sleep, and `fire_lag` rising is the visible result.
        let stage: u8 = match pressure {
            0 => 0,
            1 => 3,
            _ => 4,
        };
        // On-change only, through the shared gate: a stage that has been active
        // for an hour must not write an hour of lines (§14.4).
        static KV_PRUNE_SHED: crate::obs::OnChange<bool> = crate::obs::OnChange::new();
        static USAGE_SHED: crate::obs::OnChange<bool> = crate::obs::OnChange::new();
        crate::obs::sweeper_stage(
            &KV_PRUNE_SHED,
            "kv_prune",
            stage >= 3,
            "fire pass hit its row or lease ceiling",
        );
        crate::obs::sweeper_stage(
            &USAGE_SHED,
            "usage_rollup",
            stage >= 4,
            "fire pass hit its ceiling on consecutive cycles",
        );

        // §9.3: "la cadenza del rollup scende a quella del rinfresco per i tenant
        // sopra l'80%". A tenant near a cap is exactly the tenant whose headroom
        // decides how far the whole cell can overrun before the block lands —
        // `(brokers - 1) x headroom`, `quota_overshoot.rs` case 6 — so a fresher
        // measurement there is worth the scan, and nowhere else is.
        //
        // The rollup is NOT filtered to those tenants, deliberately. `queen.kv`
        // has one partial secondary index and no access path for "every row of a
        // tenant" (§3.4), so restricting the aggregate would not restrict the
        // SCAN, which is the whole cost — it would only duplicate a sixty-line
        // query for a saving of nothing. What the watermark buys is the CADENCE,
        // and the cadence is what §9.3 asks for.
        let hot = quotas.hot();
        let usage_every = if hot { k.usage_hot_every } else { k.usage_every };
        static USAGE_HOT: crate::obs::OnChange<bool> = crate::obs::OnChange::new();
        if let Some(Some(prev)) = USAGE_HOT.changed(hot) {
            if prev != hot {
                tracing::info!(
                    target: "sweeper",
                    hot,
                    every_ms = usage_every.as_millis() as u64,
                    "usage rollup cadence changed: a tenant crossed the quota watermark"
                );
            }
        }
        if usage_phase == PhaseState::On && usage_gate.due_every(cycle_start, usage_every) {
            if stage >= 4 {
                metrics.kvt.sweeper_phase_skipped(SweepPhase::Usage);
            } else {
                let t0 = Instant::now();
                match usage_rollup(&pool, &metrics, &k, &shards).await {
                    Ok(n) => {
                        usage_gate.mark(cycle_start);
                        worked |= n > 0;
                        metrics.kvt.sweeper_cycle(
                            SweepPhase::Usage,
                            t0.elapsed().as_secs_f64() * 1000.0,
                            n,
                        );
                    }
                    Err(e) => {
                        if e.class() == SqlClass::Config {
                            usage_phase = PhaseState::OffConfig;
                            tracing::warn!(
                                target: "sweeper", error = %e.describe(),
                                "usage rollup disabled for this process: queen.kv_usage_step_v1 \
                                 is missing or does not match. Quota gauges freeze; nothing else \
                                 is affected. Apply the schema (QUEEN_APPLY_SCHEMA=1) and restart"
                            );
                        } else if let Some(suppressed) = USAGE_ERR.tick_now() {
                            tracing::error!(
                                target: "sweeper", error = %e.describe(), suppressed,
                                "usage rollup error"
                            );
                        }
                    }
                }
            }
        }

        if kv_phase == PhaseState::On && kv_gate.due(cycle_start) {
            if stage >= 3 {
                metrics.kvt.sweeper_phase_skipped(SweepPhase::KvExpire);
            } else {
                let t0 = Instant::now();
                match kv_prune(&pool, &metrics, &k, &shards).await {
                    Ok(n) => {
                        kv_gate.mark(cycle_start);
                        worked |= n > 0;
                        metrics.kvt.sweeper_cycle(
                            SweepPhase::KvExpire,
                            t0.elapsed().as_secs_f64() * 1000.0,
                            n,
                        );
                    }
                    Err(e) => {
                        if e.class() == SqlClass::Config {
                            kv_phase = PhaseState::OffConfig;
                            tracing::warn!(
                                target: "sweeper", error = %e.describe(),
                                "KV prune disabled for this process: queen.kv_expire_step_v1 is \
                                 missing or does not match. READS STAY CORRECT (kv_live_v1 hides \
                                 an expired row), but queen.kv grows without bound — watch \
                                 queen_kv_expired_not_pruned. Apply the schema and restart"
                            );
                        } else if let Some(suppressed) = PRUNE_ERR.tick_now() {
                            tracing::error!(
                                target: "sweeper", error = %e.describe(), suppressed,
                                "kv prune error"
                            );
                        }
                    }
                }
            }
        }

        // ------------------------------------------------------------ C-eph
        // EPHEMERAL_QUEUES.md §3.2 — the RAM-class backstop: expire leases,
        // head-drop ttl'd messages and collect idle implicit queues on the
        // rings NOBODY touched this second.
        //
        // NOT ON THE DEGRADATION LADDER, at any rung, and that is the point of
        // putting it here rather than next to the two phases above. Every rung
        // of that ladder is a response to DATABASE pressure — the fire hit its
        // ceiling, so shed the phases whose cost is the database. This phase
        // takes no connection, no lane slot and no lock outside its own maps;
        // shedding it would relieve nothing and would let a cell under DB
        // pressure stop reclaiming the one resource that has no database behind
        // it to absorb the overrun.
        //
        // `worked` is deliberately NOT set by this phase either: it feeds the
        // idle backoff, whose job is to keep an empty TIMER table from spinning
        // the loop, and an ephemeral ring's work is already re-armed by
        // `Wake::hint` from the engine.
        if eph_gate.due(cycle_start) {
            eph_gate.mark(cycle_start);
            let s = ephemeral.sweep(crate::util::now_epoch_ms());
            if s.redelivered > 0 || s.exhausted > 0 || s.dropped_ttl > 0 || s.gc_queues > 0 {
                tracing::info!(
                    target: "ephemeral",
                    redelivered = s.redelivered,
                    exhausted = s.exhausted,
                    dropped_ttl = s.dropped_ttl,
                    gc_queues = s.gc_queues,
                    queues = ephemeral.queue_count(),
                    bytes = ephemeral.global_bytes(),
                    "backstop"
                );
            }
        }

        // ----------------------------------------------------------------- D
        idle_cycles = match outcome {
            CycleOutcome::Idle if !worked => idle_cycles.saturating_add(1),
            _ => 0,
        };
        let base = sleep_ms(outcome, idle_cycles, &k.sleep, rand::random::<f64>());
        // The fire's OTHER lever under pressure (§12.1). It is never shed, so all
        // it can do is take smaller batches (`cycle_budget`) and pause longer
        // between them. Bounded by MAX_SLEEP_MS, which is the delivery-latency
        // net, and never shorter than the sleep the pure function chose.
        let ms = if pressure == 0 {
            base
        } else {
            base.saturating_mul(1u64 << pressure.min(12)).min(k.sleep.max_ms).max(base)
        };
        metrics.kvt.set_sweeper_sleep(ms as i64);

        if worked {
            tracing::info!(
                target: "sweeper",
                fired = fired_rows,
                elapsed_ms = cycle_start.elapsed().as_millis() as u64,
                sleep_ms = ms,
                stage,
                "swept"
            );
        } else {
            tracing::debug!(
                target: "sweeper",
                elapsed_ms = cycle_start.elapsed().as_millis() as u64,
                sleep_ms = ms,
                idle_cycles,
                "idle cycle"
            );
        }

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(ms)) => {}
            // A local hint (§7.4). It resets the idle backoff, which is what
            // makes a 30 s empty-table sleep safe to have at all.
            _ = wake().notified() => { idle_cycles = 0; }
        }
    }
}

/// The 64 shards, rotated so this cycle starts at a different head of the index.
/// EVERY shard is always present: the rotation changes visit order, never
/// coverage (§1.10).
fn shard_ring(start: i16) -> Vec<i16> {
    (0..SHARDS).map(|i| (start + i).rem_euclid(SHARDS)).collect()
}

/// Rows the fire may drain this cycle. Under pressure this SHRINKS — the plan's
/// "reduce the batch and lengthen the sleep" — but never below one claim batch,
/// because a fire that cannot drain a single batch makes no progress at all and
/// the fire is the one phase that is never allowed to stop.
fn cycle_budget(k: &Knobs, pressure: u32) -> i64 {
    let shrunk = k.cycle_max_rows >> pressure.min(16);
    shrunk.max(k.claim_batch as i64)
}

// ===========================================================================
// One database call: slot, connection, timeout, cancellation
// ===========================================================================

/// A call's failure, with the SQLSTATE preserved.
///
/// `db::resolve_query_timeout` collapses "the statement failed" into `None`,
/// which is right for the pop paths and wrong here: the whole taxonomy of §4.5
/// and §12 — does this timer spend an attempt, is this a poisoned segment, does
/// this phase stop — is a function of the SQLSTATE, and losing it would make
/// every failure look permanent (and burn DLQ budget on a network blip).
enum CallErr {
    /// No connection could be checked out. No SQLSTATE, so transient by the
    /// house classifier — the same verdict a connection-level failure gets.
    Pool(String),
    /// Broker-side timeout. The server-side statement was cancelled and the
    /// connection quarantined by `db::resolve_query_timeout`.
    Timeout(&'static str),
    Sql(tokio_postgres::Error),
}

impl CallErr {
    fn class(&self) -> SqlClass {
        match self {
            CallErr::Pool(_) | CallErr::Timeout(_) => SqlClass::Transient,
            CallErr::Sql(e) => db::classify_sql(e),
        }
    }
    /// Safe to log: a SQLSTATE and the server's message, never a namespace or a
    /// timer key (§13.5 keeps names out of shared log shippers).
    fn describe(&self) -> String {
        match self {
            CallErr::Pool(e) => format!("pool: {e}"),
            CallErr::Timeout(what) => format!("timeout in {what}"),
            CallErr::Sql(e) => match e.as_db_error() {
                Some(d) => format!("{} {}", d.code().code(), d.message()),
                None => e.to_string(),
            },
        }
    }
}

/// A checked-out connection with its admission slot and its cancel token.
///
/// SLOT BEFORE CONNECTION, ALWAYS. Taking the connection first and then queueing
/// for a lane slot deadlocks the pool under load, and it is the one ordering
/// rule every background loop in this codebase repeats.
struct Conn {
    slot: Option<crate::admission::Slot>,
    client: deadpool_postgres::Client,
    cancel: tokio_postgres::CancelToken,
}

async fn checkout(pool: &Pool) -> Result<Conn, CallErr> {
    let slot = crate::admission::lane_slot(crate::admission::Lane::Maint).await;
    match pool.get().await {
        Ok(client) => {
            let cancel = client.cancel_token();
            Ok(Conn { slot, client, cancel })
        }
        Err(e) => Err(CallErr::Pool(e.to_string())),
    }
}

impl Conn {
    /// Resolve a timed call, preserving the error.
    ///
    /// The TIMEOUT arm is delegated to `db::resolve_query_timeout` rather than
    /// reimplemented: that function owns the server-side cancel (which needs the
    /// TLS connector matching how the pool was built) and the deadpool
    /// quarantine, and a second copy of that logic is exactly how the two drift.
    /// §7.2 makes it mandatory here — a fire wedged behind a partition lock
    /// whose backend keeps running holds locks on TIMER rows indefinitely, and
    /// those block users' cancels.
    fn finish<T>(
        self,
        res: Result<Result<T, tokio_postgres::Error>, tokio::time::error::Elapsed>,
        what: &'static str,
        metrics: &Metrics,
    ) -> Result<T, CallErr> {
        let Conn { slot, client, cancel } = self;
        let out = match res {
            Ok(Ok(v)) => {
                drop(client);
                Ok(v)
            }
            Ok(Err(e)) => {
                drop(client);
                Err(CallErr::Sql(e))
            }
            Err(elapsed) => {
                let _: Option<T> =
                    db::resolve_query_timeout(Err(elapsed), client, cancel, what, metrics);
                Err(CallErr::Timeout(what))
            }
        };
        drop(slot);
        out
    }
}

// ===========================================================================
// Phase A + B: probe, claim, pack, fire
// ===========================================================================

struct PassResult {
    outcome: CycleOutcome,
    rows: u64,
    /// The pass stopped at a ceiling (row budget or lease budget) rather than
    /// because the work ran out. This is the pressure signal.
    capped: bool,
}

/// `{"nextInMs","due","dueCapped","lateMs","now"}` — everything already relative
/// to the SERVER's clock, which is what keeps timestamp arithmetic out of the
/// broker entirely (§6.2).
struct Probe {
    next_in_ms: Option<i64>,
    due: i64,
    due_capped: bool,
    late_ms: i64,
}

fn parse_probe(txt: &str) -> Probe {
    let v: serde_json::Value = serde_json::from_str(txt).unwrap_or(serde_json::Value::Null);
    Probe {
        next_in_ms: v.get("nextInMs").and_then(|x| x.as_i64()),
        due: v.get("due").and_then(|x| x.as_i64()).unwrap_or(0),
        due_capped: v.get("dueCapped").and_then(|x| x.as_bool()).unwrap_or(false),
        late_ms: v.get("lateMs").and_then(|x| x.as_i64()).unwrap_or(0),
    }
}

/// Shared across the drain workers. Everything here is a ceiling or a fact about
/// the pass as a whole, never per-worker state.
#[derive(Default)]
struct DrainState {
    /// Rows still allowed this cycle.
    budget: AtomicI64,
    drained: AtomicI64,
    /// Any worker took at least one row. False for all of them is the
    /// EMPTY CLAIM of §7.2 — work was due, another broker is draining it.
    claimed_any: AtomicBool,
    /// The pass stopped at a ceiling rather than because the work ran out.
    capped: AtomicBool,
    /// One worker found the claim empty: there is nothing left for anyone right
    /// now, so the others stop too instead of spinning on empty claims.
    stop: AtomicBool,
}

async fn fire_pass(
    pool: &Pool,
    metrics: &Arc<Metrics>,
    k: &Knobs,
    shards: &[i16],
    budget_rows: i64,
) -> Result<PassResult, CallErr> {
    // ------------------------------------------------------------------- A
    let c = checkout(pool).await?;
    let res =
        tokio::time::timeout(k.stmt_timeout, db::timers_due(&c.client, shards, k.due_cap)).await;
    let probe = parse_probe(&c.finish(res, "timers_due", metrics)?);
    metrics.kvt.set_timers_due(probe.due, probe.due_capped, probe.late_ms);

    let Some(next_in_ms) = probe.next_in_ms else {
        // NULL means the table is empty. This is the signal §7.1 turns into the
        // progressive idle backoff, and it is the only path to it.
        return Ok(PassResult { outcome: CycleOutcome::Idle, rows: 0, capped: false });
    };
    if probe.due == 0 {
        return Ok(PassResult { outcome: CycleOutcome::Due { next_in_ms }, rows: 0, capped: false });
    }

    // ------------------------------------------------------------------- B
    // Half the lease, measured from the start of the DRAIN and shared by every
    // worker. Past it, whatever is claimed now would be re-claimable by another
    // broker before our own fire commits — the livelock of §7.2.
    let deadline = Instant::now() + Duration::from_millis((k.lease_ms as u64 / 2).max(1));
    let st = Arc::new(DrainState::default());
    st.budget.store(budget_rows, Ordering::Relaxed);

    let n = k.parallelism.clamp(1, MAX_PARALLELISM);
    let err = if n == 1 {
        // The default. Kept off the spawn path so the common configuration is
        // exactly one task, doing one thing, on one connection at a time.
        drain_worker(
            pool.clone(),
            Arc::clone(metrics),
            k.clone(),
            shards.to_vec(),
            Arc::clone(&st),
            deadline,
        )
        .await
        .err()
    } else {
        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            handles.push(tokio::spawn(drain_worker(
                pool.clone(),
                Arc::clone(metrics),
                k.clone(),
                shards.to_vec(),
                Arc::clone(&st),
                deadline,
            )));
        }
        // Join ALL of them before reporting anything: a worker that failed must
        // not leave its siblings running against a pass the caller has abandoned
        // (retention's rule, for the same reason).
        let mut first: Option<CallErr> = None;
        for h in handles {
            match h.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    first.get_or_insert(e);
                }
                // Unreachable under `panic = "abort"` — a panicking task takes
                // the process with it — but a JoinError must not be an unwrap.
                Err(_join) => {}
            }
        }
        first
    };

    let drained = st.drained.load(Ordering::Relaxed);
    if let Some(e) = err {
        if drained == 0 {
            return Err(e);
        }
        // Rows DID move, so the pass is not a failure to report as one; losing
        // the count would make the cycle look idle and let the idle backoff
        // engage on a table that is anything but.
        if let Some(suppressed) = FIRE_ERR.tick_now() {
            tracing::error!(
                target: "sweeper", error = %e.describe(), suppressed, drained,
                "drain worker failed after moving rows"
            );
        }
        st.capped.store(true, Ordering::Relaxed);
    }

    if !st.claimed_any.load(Ordering::Relaxed) {
        // Work was due and we took nothing: another broker is draining. This is
        // the branch the jittered 25..200 ms band exists for.
        return Ok(PassResult {
            outcome: CycleOutcome::EmptyClaim,
            rows: 0,
            capped: st.capped.load(Ordering::Relaxed),
        });
    }
    Ok(PassResult {
        // We moved rows, so the pre-drain `nextInMs` is stale by construction.
        // Ask again immediately, through the floor.
        outcome: CycleOutcome::Due { next_in_ms: 0 },
        rows: drained.max(0) as u64,
        capped: st.capped.load(Ordering::Relaxed),
    })
}

/// One drain worker: claim, pack, fire, repeat, until a ceiling or an empty
/// claim. Owned arguments so it can be `tokio::spawn`ed when parallelism > 1.
async fn drain_worker(
    pool: Pool,
    metrics: Arc<Metrics>,
    k: Knobs,
    shards: Vec<i16>,
    st: Arc<DrainState>,
    deadline: Instant,
) -> Result<(), CallErr> {
    loop {
        if st.stop.load(Ordering::Relaxed) {
            return Ok(());
        }
        if st.budget.load(Ordering::Relaxed) <= 0 || Instant::now() >= deadline {
            st.capped.store(true, Ordering::Relaxed);
            return Ok(());
        }
        let c = checkout(&pool).await?;
        let res = tokio::time::timeout(
            k.stmt_timeout,
            db::timers_claim(&c.client, &shards, k.lease_ms, k.claim_batch, k.per_tenant),
        )
        .await;
        let claims = c.finish(res, "timers_claim", &metrics)?;
        if claims.is_empty() {
            st.stop.store(true, Ordering::Relaxed);
            return Ok(());
        }
        let n = claims.len() as i64;
        st.claimed_any.store(true, Ordering::Relaxed);
        st.budget.fetch_sub(n, Ordering::Relaxed);
        st.drained.fetch_add(n, Ordering::Relaxed);
        // Pack OUTSIDE any transaction (§1.3 phase b): the claim has already
        // committed and the fire has not started, so nothing is held while zstd
        // runs.
        for batch in group_and_pack(claims, &k) {
            fire_batch(&pool, &metrics, &k, batch).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Grouping and packing
// ---------------------------------------------------------------------------

/// One timer, decompressed and ready to become a frame.
struct Row {
    key: String,
    token: String,
    txn: String,
    message_id: [u8; 16],
    producer_sub: Option<String>,
    /// Decompressed. NOT decrypted: encryption happens at SCHEDULE (§13.4) and
    /// the bit travels with the frame, so the fire is not a place that can see
    /// plaintext it otherwise could not.
    payload: Vec<u8>,
    encrypted: bool,
    attempts: i32,
    late_ms: i64,
}

/// A packed segment plus everything the failure paths need about its timers.
struct Packed {
    tenant: String,
    queue: String,
    partition: String,
    /// `PackedSegment::count`, carried rather than recomputed from `rows.len()`.
    /// It is what goes into `p_msg_counts`, and the SP checks it against BOTH
    /// blobs (`octet_length(hashes[i]) = counts[i] * 16`, and exactly `counts[i]`
    /// keys with `seg_of = i`). Taking it from the packer means the number the SP
    /// verifies is the number the packer actually produced, not a second opinion
    /// about it.
    count: i32,
    hashes: Vec<u8>,
    blob: Vec<u8>,
    rows: Vec<Row>,
}

impl Packed {
    fn bytes(&self) -> usize {
        self.blob.len() + self.hashes.len()
    }
}

/// GROUP BY `(tenant, queue, partition)` — **never** by `(queue, partition)`.
///
/// A tenant-blind grouping fuses two tenants' timers into one segment: the worst
/// isolation hole this feature can have, one word shorter to write, producing no
/// error at all (§6.2 point 8). The SP enforces its half (each segment resolves
/// under its own tenant, and two live segments naming the same triple are
/// rejected), and there is a two-tenant regression test with identical queue
/// names and timer keys that is a merge criterion.
///
/// Claim order is preserved inside a group, and that is a CONTRACT, not an
/// artifact: §4.2 says the order of a timer inside its key is decided AT THE
/// FIRE, and the claim returns rows in `visible_at` order — expiry order, not
/// schedule order.
///
/// Returns a list of BATCHES. Two properties hold of every batch, and each is
/// load-bearing for a different reason:
///
///   * **at most one segment per triple**, because the SP rejects two live
///     segments naming the same `(tenant, queue, partition)`. A group too big
///     for one transaction is therefore split across SUCCESSIVE batches, not
///     across segments of the same call; its rows stay claimed by us with a
///     valid token in the meantime, so the later call verifies fine;
///   * **total packed bytes within `QUEEN_SWEEPER_MAX_FIRE_BYTES`**, because the
///     cost of the fire transaction is per byte of WAL (§7.2).
fn group_and_pack(claims: Vec<TimerClaim>, k: &Knobs) -> Vec<Vec<Packed>> {
    // Insertion-ordered grouping: a HashMap would make the visit order — and so
    // the order two groups' partitions are provisioned in — depend on hashing.
    let mut order: Vec<(String, String, String)> = Vec::new();
    let mut groups: Vec<Vec<Row>> = Vec::new();
    let mut index: crate::util::FnvHashMap<(String, String, String), usize> = Default::default();

    for c in claims {
        let Some(mid) = uuid_string_to_bytes(&c.message_id) else {
            // Unreachable from the SP (the column is a UUID), and if it ever
            // happened, dropping the row is the only safe move: it stays claimed
            // and returns after the lease with nothing corrupted.
            continue;
        };
        let payload = if c.payload_zstd { zstd_decompress(&c.payload) } else { c.payload };
        let row = Row {
            key: c.timer_key,
            token: c.token,
            txn: c.txn,
            message_id: mid,
            producer_sub: c.producer_sub,
            payload,
            encrypted: c.encrypted,
            attempts: c.attempts,
            late_ms: c.late_ms,
        };
        let key = (c.tenant, c.queue, c.partition);
        match index.get(&key) {
            Some(&i) => groups[i].push(row),
            None => {
                index.insert(key.clone(), groups.len());
                order.push(key);
                groups.push(vec![row]);
            }
        }
    }

    // Chunk each group so no single segment can exceed the transaction ceiling
    // on its own. Raw payload bytes are a conservative stand-in for the packed
    // size (zstd only shrinks), and at least one row always goes in, so a single
    // oversized timer still makes progress instead of wedging its group.
    let cap = k.max_fire_bytes.max(1);
    let mut chunked: Vec<((String, String, String), Vec<Vec<Row>>)> = Vec::new();
    for (key, rows) in order.into_iter().zip(groups) {
        let mut chunks: Vec<Vec<Row>> = Vec::new();
        let mut cur: Vec<Row> = Vec::new();
        let mut cur_bytes = 0usize;
        for r in rows {
            let sz = r.payload.len() + r.txn.len() + 64;
            if !cur.is_empty() && cur_bytes + sz > cap {
                chunks.push(std::mem::take(&mut cur));
                cur_bytes = 0;
            }
            cur_bytes += sz;
            cur.push(r);
        }
        if !cur.is_empty() {
            chunks.push(cur);
        }
        chunked.push((key, chunks));
    }

    // Assemble batches ROUND BY ROUND: round r takes chunk r of every group, so
    // a batch can never contain two chunks of the same triple. Inside a round we
    // split further on the byte ceiling.
    let rounds = chunked.iter().map(|(_, c)| c.len()).max().unwrap_or(0);
    let mut batches: Vec<Vec<Packed>> = Vec::new();
    for r in 0..rounds {
        let mut batch: Vec<Packed> = Vec::new();
        let mut bytes = 0usize;
        for (key, chunks) in chunked.iter_mut() {
            let Some(rows) = chunks.get_mut(r) else { continue };
            if rows.is_empty() {
                continue;
            }
            let rows = std::mem::take(rows);
            let packed = pack_one(key, rows, k.zstd_level);
            if !batch.is_empty() && bytes + packed.bytes() > cap {
                batches.push(std::mem::take(&mut batch));
                bytes = 0;
            }
            bytes += packed.bytes();
            batch.push(packed);
        }
        if !batch.is_empty() {
            batches.push(batch);
        }
    }
    batches
}

fn pack_one(key: &(String, String, String), rows: Vec<Row>, zstd_level: i32) -> Packed {
    // `pack_segment` is the SAME recipe the fusion flush uses (§15): same frame
    // codec, same zstd level, same 16-byte hash stride in frame order. The fire
    // must not grow a second one — the bytes land in the same
    // `queen.log_segments.blob` column and are read back by the same pop.
    let frames: Vec<FrameIn> = rows
        .iter()
        .map(|r| FrameIn {
            message_id: r.message_id,
            txn: &r.txn,
            // No trace on a fired timer: the schedule's trace context is not the
            // delivery's, and inventing one would attach the timer's frames to a
            // request that ended days earlier.
            trace_id: None,
            // Stamped from the AUTHENTICATED sub recorded at schedule time. A
            // client-supplied producerSub was already refused by the SP (§4.2).
            producer_sub: r.producer_sub.as_deref(),
            payload: &r.payload,
            encrypted: r.encrypted,
        })
        .collect();
    let seg = pack_segment(&frames, zstd_level);
    Packed {
        tenant: key.0.clone(),
        queue: key.1.clone(),
        partition: key.2.clone(),
        count: seg.count as i32,
        hashes: seg.hashes,
        blob: seg.blob,
        rows,
    }
}

// ---------------------------------------------------------------------------
// The fire itself
// ---------------------------------------------------------------------------

async fn fire_batch(pool: &Pool, metrics: &Metrics, k: &Knobs, batch: Vec<Packed>) {
    if batch.is_empty() {
        return;
    }
    if !fire_attempt(pool, metrics, k, &batch).await {
        return;
    }
    // POISON ISOLATION (§7.6). One poisoned segment fails the whole transaction,
    // and without this the same batch would fail FOREVER while its healthy
    // segments never arrive — the most insidious failure of the feature, because
    // the symptom (`lateMs` rising) does not name the culprit. Replaying one
    // segment per call costs one extra round trip, and only on the failure path.
    metrics.kvt.timers_poisoned(1);
    if let Some(suppressed) = POISON_WARN.tick_now() {
        tracing::warn!(
            target: "sweeper",
            segments = batch.len(),
            suppressed,
            "permanent fire failure on a multi-segment batch; replaying one segment per call"
        );
    }
    for seg in batch {
        let one = vec![seg];
        // A single segment can no longer be isolated further, so the verdict is
        // final and `fire_attempt` takes the fail/DLQ path itself.
        let _ = fire_attempt(pool, metrics, k, &one).await;
    }
}

/// One `log_timers_fire_v1` call. Returns `true` when the caller should replay
/// the batch one segment at a time.
async fn fire_attempt(pool: &Pool, metrics: &Metrics, k: &Knobs, batch: &[Packed]) -> bool {
    // Two families of arrays. Mixing them up is exactly what the SP's alignment
    // guards exist to catch loudly rather than quietly.
    //   per SEGMENT, byte for byte the order log_push_multi_v1 uses:
    let mut tenants: Vec<String> = Vec::with_capacity(batch.len());
    let mut queues: Vec<String> = Vec::with_capacity(batch.len());
    let mut partitions: Vec<String> = Vec::with_capacity(batch.len());
    let mut counts: Vec<i32> = Vec::with_capacity(batch.len());
    let mut hashes: Vec<Vec<u8>> = Vec::with_capacity(batch.len());
    let mut blobs: Vec<Vec<u8>> = Vec::with_capacity(batch.len());
    //   per TIMER, the frame -> timer map:
    let mut keys: Vec<String> = Vec::new();
    let mut seg_of: Vec<i32> = Vec::new();
    let mut tokens: Vec<String> = Vec::new();

    for (i, s) in batch.iter().enumerate() {
        tenants.push(s.tenant.clone());
        queues.push(s.queue.clone());
        partitions.push(s.partition.clone());
        counts.push(s.count);
        hashes.push(s.hashes.clone());
        blobs.push(s.blob.clone());
        for r in &s.rows {
            keys.push(r.key.clone());
            // 1-BASED: PostgreSQL arrays are, and the SP indexes the segment
            // arrays with this value directly.
            seg_of.push(i as i32 + 1);
            tokens.push(r.token.clone());
        }
    }

    let c = match checkout(pool).await {
        Ok(c) => c,
        Err(e) => {
            fail_batch(pool, metrics, k, batch, &e).await;
            return false;
        }
    };
    let res = tokio::time::timeout(
        k.stmt_timeout,
        db::timers_fire(
            &c.client, &tenants, &queues, &partitions, &counts, &hashes, &blobs, &keys, &seg_of,
            &tokens,
        ),
    )
    .await;
    match c.finish(res, "timers_fire", metrics) {
        Ok(txt) => {
            account_fire(metrics, batch, &txt);
            false
        }
        Err(e) => {
            let class = e.class();
            if class != SqlClass::Transient && batch.len() > 1 && k.isolate_on_permanent {
                return true;
            }
            fail_batch(pool, metrics, k, batch, &e).await;
            false
        }
    }
}

/// Read the fire's per-segment verdicts. The taxonomy is CLOSED —
/// `fired | duplicate | stale` — and anything else is a contract break, counted
/// as `stale` so the loop keeps going rather than silently dropping the count.
fn account_fire(metrics: &Metrics, batch: &[Packed], txt: &str) {
    let v: serde_json::Value = serde_json::from_str(txt).unwrap_or(serde_json::Value::Null);
    let arr = v.as_array().map(|a| a.as_slice()).unwrap_or(&[]);
    for (i, s) in batch.iter().enumerate() {
        let r = arr.get(i);
        let result = r.and_then(|x| x.get("result")).and_then(|x| x.as_str()).unwrap_or("stale");
        match result {
            "fired" => {
                let n = s.rows.len() as u64;
                metrics.kvt.timers_fired(FireResult::Fired, n);
                // The delivery lateness the customer feels, labelled by tenant
                // — the one motivated exception to §14.1's cardinality rule,
                // because a backlog caused by one tenant must name the culprit.
                // The value comes from the SERVER (`r_late_ms`); the broker
                // holds no timestamp.
                let worst = s.rows.iter().map(|r| r.late_ms).max().unwrap_or(0);
                metrics.kvt.fire_lag(&s.tenant, worst as f64);
            }
            "duplicate" => {
                // The fixed txn was already in the log: those timers were
                // already delivered. The SP deleted them and released the rest
                // of the group, so the group makes progress on the next pass.
                let n = r
                    .and_then(|x| x.get("dups"))
                    .and_then(|x| x.as_array())
                    .map(|a| a.len() as u64)
                    .unwrap_or(0);
                metrics.kvt.timers_fired(FireResult::Duplicate, n.max(1));
            }
            _ => {
                metrics.kvt.timers_fired(FireResult::Stale, 1);
                // A stale segment means another broker re-claimed our rows while
                // we were packing: the SKIP LOCKED contest, observed from the
                // losing side. The SP has already released everything this
                // transaction held and did not deliver (§6.2 point 3), so there
                // is nothing for us to repair — the rows are claimable again
                // immediately, not after the lease.
                metrics.kvt.sweeper_skip_locked(s.rows.len() as u64);
            }
        }
    }
}

/// The fire failed. Push the lease out by a backoff, NULL the claim token — so
/// the rows are in BACKOFF and in NOBODY'S hands, which is what keeps a poisoned
/// timer cancellable — and, for whatever is exhausted, archive to the DLQ.
async fn fail_batch(pool: &Pool, metrics: &Metrics, k: &Knobs, batch: &[Packed], e: &CallErr) {
    let class = e.class();
    let (failure, count_attempt) = match class {
        SqlClass::Transient => (FireFailure::Transient, false),
        SqlClass::Config => (FireFailure::Config, true),
        SqlClass::Permanent => (FireFailure::Permanent, true),
    };
    metrics.kvt.timers_fire_failure(failure);

    let msg = e.describe();
    match class {
        // Actionable by an operator, so NOT sampled and it names the destination
        // — this is the one failure a human can go and fix (§4.5).
        SqlClass::Config => tracing::warn!(
            target: "sweeper",
            error = %msg,
            queues = %batch.iter().map(|s| s.queue.as_str()).collect::<Vec<_>>().join(","),
            "fire failed with a CONFIGURATION error (class 42): the timer spends an attempt"
        ),
        SqlClass::Permanent => {
            if let Some(suppressed) = CYCLE_ERR.tick_now() {
                tracing::warn!(target: "sweeper", error = %msg, suppressed, "fire failed permanently");
            }
        }
        SqlClass::Transient => {
            if let Some(suppressed) = CYCLE_ERR.tick_now() {
                tracing::warn!(
                    target: "sweeper", error = %msg, suppressed,
                    "fire failed transiently; no attempt spent"
                );
            }
        }
    }

    // The backoff is `min(min_ms * 2^attempts, max_ms)` and `attempts` is
    // per-timer, while the SP takes ONE backoff per call. Rather than pick a
    // single wrong number for the whole batch, group by `attempts` — there are
    // at most `max_attempts + 1` distinct values, so this is a handful of calls
    // on a path that is already the failure path.
    let mut buckets: std::collections::BTreeMap<i32, (Vec<String>, Vec<String>, Vec<String>, Vec<String>)> =
        Default::default();
    for s in batch {
        for r in &s.rows {
            let b = buckets.entry(if count_attempt { r.attempts } else { 0 }).or_default();
            b.0.push(s.tenant.clone());
            b.1.push(s.queue.clone());
            b.2.push(r.key.clone());
            b.3.push(r.token.clone());
        }
    }

    let mut exhausted: Vec<(String, String, String)> = Vec::new();
    for (attempts, (tenants, queues, keys, tokens)) in buckets {
        let backoff = if count_attempt {
            let shift = attempts.clamp(0, 30) as u32;
            k.backoff_min_ms
                .saturating_mul(1i64 << shift)
                .min(k.backoff_max_ms)
        } else {
            k.transient_backoff_ms as i64
        };
        let c = match checkout(pool).await {
            Ok(c) => c,
            Err(_) => return, // the lease will expire and another broker retries
        };
        let res = tokio::time::timeout(
            k.stmt_timeout,
            db::timers_fail(
                &c.client,
                &tenants,
                &queues,
                &keys,
                &tokens,
                clamp_i32(backoff),
                &msg,
                count_attempt,
                k.max_attempts,
            ),
        )
        .await;
        // If the fail call itself fails there is nothing to repair: the lease
        // expires and the rows come back on their own (§12).
        let Ok(txt) = c.finish(res, "timers_fail", metrics) else { return };
        let v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
        if let Some(a) = v.get("exhausted").and_then(|x| x.as_array()) {
            for e in a {
                let (Some(t), Some(q), Some(kk)) = (
                    e.get("tenant").and_then(|x| x.as_str()),
                    e.get("queue").and_then(|x| x.as_str()),
                    e.get("timerKey").and_then(|x| x.as_str()),
                ) else {
                    continue;
                };
                exhausted.push((t.to_string(), q.to_string(), kk.to_string()));
            }
        }
    }

    if !exhausted.is_empty() {
        dlq(pool, metrics, k, batch, &exhausted, &msg).await;
    }
}

/// Archive exhausted timers into the destination queue's DLQ.
///
/// The snapshot is the payload the broker ALREADY holds from the claim,
/// decompressed and parsed as JSON. When it is not valid JSON we write
/// `{"_raw_b64": "..."}` instead of failing: the DLQ is the last resort and must
/// not have a branch that loses the message.
async fn dlq(
    pool: &Pool,
    metrics: &Metrics,
    k: &Knobs,
    batch: &[Packed],
    exhausted: &[(String, String, String)],
    err: &str,
) {
    use base64::Engine;
    let mut tenants = Vec::new();
    let mut queues = Vec::new();
    let mut keys = Vec::new();
    let mut payloads = Vec::new();
    let mut errors = Vec::new();

    for (t, q, key) in exhausted {
        let Some(row) = batch
            .iter()
            .filter(|s| &s.tenant == t && &s.queue == q)
            .flat_map(|s| s.rows.iter())
            .find(|r| &r.key == key)
        else {
            continue;
        };
        let snapshot = match std::str::from_utf8(&row.payload) {
            Ok(s) if serde_json::from_str::<serde_json::Value>(s).is_ok() => s.to_string(),
            _ => {
                let b64 = base64::engine::general_purpose::STANDARD.encode(&row.payload);
                serde_json::json!({ "_raw_b64": b64 }).to_string()
            }
        };
        tenants.push(t.clone());
        queues.push(q.clone());
        keys.push(key.clone());
        payloads.push(snapshot);
        errors.push(err.to_string());
    }
    if tenants.is_empty() {
        return;
    }

    let Ok(c) = checkout(pool).await else { return };
    let res = tokio::time::timeout(
        k.stmt_timeout,
        db::timers_dlq(&c.client, &tenants, &queues, &keys, &payloads, &errors, k.max_attempts),
    )
    .await;
    let Ok(txt) = c.finish(res, "timers_dlq", metrics) else { return };
    let v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
    let archived = v.get("archived").and_then(|x| x.as_i64()).unwrap_or(0);
    if archived > 0 {
        metrics.kvt.timers_dlq(archived as u64);
        // On-change and unsampled: a timer reaching the DLQ is a product event,
        // not a rate (§14.4).
        tracing::warn!(
            target: "sweeper",
            archived,
            error = %err,
            "poisoned timers archived to the DLQ (consumer_group = __timer__)"
        );
    }
}

// ===========================================================================
// Phase C: the KV prune and the usage rollup
// ===========================================================================

/// `queen.kv_expire_step_v1(p_shards, p_now, p_max_rows) -> JSONB`, bounded like
/// every retention step: `{"deleted":N,"done":bool,...}`, looped until done or
/// until the cycle's row budget runs out.
///
/// `SKIP LOCKED` lives inside the SP, so the prune never waits on a `queen.kv`
/// row: actor 7 of §2.3 is a singleton in the KV space that cannot be an edge of
/// the wait-for graph.
const KV_EXPIRE_SQL: &str =
    "SELECT (queen.kv_expire_step_v1($1::int2[], now(), $2::int))::text";

/// `queen.kv_usage_step_v1(p_shards, p_now, p_max_tenants) -> JSONB` — the slow
/// phase of §7.5, on `QUEEN_KV_USAGE_EVERY_MS`.
///
/// It must NEVER move onto the due-driven path. The three corrections that make
/// it affordable live in the SP, not here: `pg_column_size(k.value)` on the
/// COLUMN and never on a whole-row var (which would either detoast every value
/// in the table every pass, or count an 18-byte external pointer and so
/// under-count exactly the large rows the quota exists to bound); an INCREMENTAL
/// count with a rare guarded full scan; and a `SKIP LOCKED` per shard so two
/// brokers do not repeat the same work.
const KV_USAGE_SQL: &str =
    "SELECT (queen.kv_usage_step_v1($1::int2[], now(), $2::int))::text";

/// Run one `SELECT (<sp>)::text` under the slot / connection / timeout / cancel
/// discipline, keeping the SQLSTATE.
///
/// These two SPs have no wrapper in `db.rs` yet — they belong to the KV file
/// (`024_kv.sql`) and to F5 — so the statements live here as named consts, the
/// way `retention.rs` holds `WORK_LIST_SQL` and `Phase::sql()`. When the
/// wrappers land, this helper and both consts should move behind them: the rule
/// that only `db.rs` names `queen.kv` / `queen.log_timers` is worth more than
/// the two lines it costs.
async fn query_json(
    pool: &Pool,
    metrics: &Metrics,
    what: &'static str,
    timeout: Duration,
    sql: &'static str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<String, CallErr> {
    let c = checkout(pool).await?;
    let stmt = match c.client.prepare_cached(sql).await {
        Ok(s) => s,
        // A prepare failure is where the missing-SP 42883 shows up, and it is
        // the answer that switches the phase off for the life of the process.
        Err(e) => return Err(CallErr::Sql(e)),
    };
    let res = tokio::time::timeout(timeout, c.client.query_one(&stmt, params))
        .await
        .map(|r| r.map(|row| row.get::<_, String>(0)));
    c.finish(res, what, metrics)
}

async fn kv_prune(
    pool: &Pool,
    metrics: &Metrics,
    k: &Knobs,
    shards: &[i16],
) -> Result<u64, CallErr> {
    let mut deleted_total: u64 = 0;
    let mut budget = k.cycle_max_rows;
    loop {
        let txt = query_json(
            pool,
            metrics,
            "kv_expire",
            k.stmt_timeout,
            KV_EXPIRE_SQL,
            &[&shards, &k.kv_expire_batch],
        )
        .await?;
        let v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
        let deleted = v.get("deleted").and_then(|x| x.as_i64()).unwrap_or(0);
        let done = v.get("done").and_then(|x| x.as_bool()).unwrap_or(true);
        // The alarm gauge of §14.3.4: this is the ONLY signal that separates
        // "the sweeper is behind" from "all is well" in a failure mode that
        // disguises itself as success, because reads stay perfectly correct
        // while the table grows.
        metrics.kvt.set_kv_expiry(
            v.get("unpruned").and_then(|x| x.as_i64()).unwrap_or(0),
            v.get("unprunedCapped").and_then(|x| x.as_bool()).unwrap_or(false),
            v.get("lagMs").and_then(|x| x.as_i64()).unwrap_or(0),
        );
        deleted_total += deleted.max(0) as u64;
        budget -= deleted.max(0);
        // `deleted == 0` also covers "a full batch was selected but the
        // under-lock re-check spared every row", which must not spin.
        if done || deleted == 0 || budget <= 0 {
            return Ok(deleted_total);
        }
    }
}

async fn usage_rollup(
    pool: &Pool,
    metrics: &Metrics,
    k: &Knobs,
    shards: &[i16],
) -> Result<u64, CallErr> {
    let max_tenants: i32 = 10_000;
    let txt = query_json(
        pool,
        metrics,
        "kv_usage",
        k.stmt_timeout,
        KV_USAGE_SQL,
        &[&shards, &max_tenants],
    )
    .await?;
    let v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
    let rows = v.get("tenants").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    let usage: Vec<crate::metrics::TenantUsage> = rows
        .iter()
        .filter_map(|t| {
            Some(crate::metrics::TenantUsage {
                tenant: t.get("tenant")?.as_str()?.to_string(),
                kv_rows: t.get("kvRows").and_then(|x| x.as_i64()).unwrap_or(0),
                kv_bytes: t.get("kvBytes").and_then(|x| x.as_i64()).unwrap_or(0),
                timers_pending: t.get("timerRows").and_then(|x| x.as_i64()).unwrap_or(0),
                kv_quota_ratio: t.get("kvQuotaRatio").and_then(|x| x.as_f64()).unwrap_or(0.0),
                timers_quota_ratio: t
                    .get("timerQuotaRatio")
                    .and_then(|x| x.as_f64())
                    .unwrap_or(0.0),
            })
        })
        .collect();
    let n = usage.len() as u64;
    metrics.kvt.set_usage(usage);
    Ok(n)
}

// ===========================================================================
// Tests
// ===========================================================================

// PLAN_KV_TIMERS §15 "Unit Rust". Both files were written before this module
// existed and are its contract; see `src/tests_unit/README.md`. `fire_sql_pin`
// is wired HERE rather than from `db.rs` only because it needs `TIMERS_FIRE_SQL`
// in scope via `use super::*` and either owner satisfies that — the pin is about
// the SQL text, not about which module declares the test.
#[cfg(test)]
#[path = "tests_unit/sweeper_sleep.rs"]
mod sweeper_sleep_tests;

#[cfg(test)]
mod tests {
    use super::*;

    /// The rotation must change the ORDER and never the COVERAGE. An
    /// off-by-one that dropped a shard would orphan 1/64 of every tenant's
    /// timers — invisibly, because the probe would simply never see them.
    #[test]
    fn the_shard_ring_always_covers_every_shard() {
        for start in 0..SHARDS {
            let ring = shard_ring(start);
            assert_eq!(ring.len(), SHARDS as usize);
            let mut sorted = ring.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(sorted.len(), SHARDS as usize, "start={start} lost a shard");
            assert_eq!(ring[0], start, "start={start} did not start where it said");
        }
    }

    /// Under pressure the budget shrinks, but the fire must never be starved to
    /// the point of making no progress: it is the one phase that is never shed
    /// (§12.1), so its floor is one claim batch.
    #[test]
    fn the_cycle_budget_shrinks_but_never_below_one_batch() {
        let k = Knobs {
            sleep: SleepKnobs::defaults(),
            lease_ms: 30_000,
            claim_batch: 200,
            cycle_max_rows: 5000,
            due_cap: 2000,
            max_fire_bytes: 8 << 20,
            max_attempts: 5,
            per_tenant: 50,
            isolate_on_permanent: true,
            backoff_min_ms: 1000,
            backoff_max_ms: 60_000,
            transient_backoff_ms: 1000,
            zstd_level: 3,
            stmt_timeout: Duration::from_secs(30),
            kv_expire_every: Duration::from_millis(1000),
            kv_expire_batch: 1000,
            usage_every: Duration::from_millis(300_000),
            usage_hot_every: Duration::from_millis(30_000),
            parallelism: 1,
        };
        assert_eq!(cycle_budget(&k, 0), 5000);
        assert_eq!(cycle_budget(&k, 1), 2500);
        assert_eq!(cycle_budget(&k, 2), 1250);
        // Saturates at the floor rather than reaching zero, and does not panic
        // on a shift wider than the type.
        assert_eq!(cycle_budget(&k, 40), 200);
    }

    /// `hint` applies only when the new minimum is EARLIER. That single property
    /// is the whole anti-storm defence of §7.4: a million timers scheduled for
    /// next week must produce exactly one wake, not a million.
    #[test]
    fn a_later_hint_does_not_move_the_minimum() {
        let w = Wake::new();
        w.arm();
        w.hint(1000);
        let after_first = w.earliest_ms.load(Ordering::Relaxed);
        assert!(after_first < i64::MAX);
        w.hint(60_000);
        assert_eq!(
            w.earliest_ms.load(Ordering::Relaxed),
            after_first,
            "a later delivery must not move the minimum"
        );
        w.hint(1);
        assert!(w.earliest_ms.load(Ordering::Relaxed) < after_first, "an earlier one must");
    }

    /// A negative delay is legal (§4.2: a `deliverAt` in the past fires on the
    /// first cycle) and must not underflow into a distant past that would then
    /// swallow every later hint.
    #[test]
    fn a_past_hint_is_clamped_to_now_not_to_the_epoch() {
        let w = Wake::new();
        w.arm();
        w.hint(i64::MIN);
        let v = w.earliest_ms.load(Ordering::Relaxed);
        assert!(v > 0, "a past hint must not underflow: {v}");
        assert!(v <= crate::util::now_epoch_ms() + 1);
    }
}
