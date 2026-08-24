//! Background retention + eviction service for the log engine (18-log-engine.md §8).
//!
//! Runs every `RETENTION_INTERVAL` ms — now defaulting to 5000, and since the
//! claim-row scheduler (crate::lease, 029_maintenance_leases, task
//! 'retention') that value is the TRUE cluster cadence, independent of the
//! replica count — on ONE dedicated pooled connection, with advisory lock
//! 737001 kept inside the cycle as belt. The shape of the work is inverted
//! from the old seg-engine service: instead of one
//! `seg_retention_sweep_v1()` call = one giant transaction over EVERY partition,
//! each cycle loops the bounded STEP functions of 006_log_maintenance.sql:
//!
//!   * `queen.log_retention_step_v1(pid, all_cutoff, completed_cutoff, max_rows)`
//!     — rules 1+2 (retention_seconds / completed_retention_seconds), at most
//!     `max_rows` segment rows per call, looped until `{"done":true}`.
//!   * `queen.log_txns_purge_step_v1(pid, cutoff, max_rows)` — the log_txns
//!     hash-sidecar purge, cutoff = now() - GREATEST(dedup_window_seconds,
//!     completed_retention_seconds, 900).
//!   * `queen.log_evict_max_wait_step_v1(pid, cutoff, max_rows)` — the
//!     max_wait_time_seconds age eviction that used to live Rust-side in
//!     db::seg_evict_max_wait; retention.rs now owns it via this SP.
//!   * `queen.log_partition_cleanup_step_v1(cutoff, max_rows)` — deletes EMPTY
//!     partitions inactive for PARTITION_CLEANUP_DAYS (the restored C++
//!     cleanup_inactive_partitions). Not per-partition like the others: one call
//!     handles a batch, and the phase runs on its own slow sub-cadence
//!     (PARTITION_SWEEP_EVERY) because its candidate scan is O(partitions) and a
//!     30-day window has no use for 5-second resolution.
//!
//! CONCURRENCY SAFETY (the whole point of R1 — doc 17's "retention-sweep
//! effect" / doc 18 §8, §12.5 "no sweep stalls"): there is NO wrapping
//! transaction. Every step call autocommits, and each step takes exactly ONE
//! `queen.log_partitions` row lock (`FOR UPDATE`, the same serializer the push
//! allocator uses) and holds it only for its own bounded batch — so a huge
//! backlog is drained in many short transactions instead of one sweep-length
//! lock hold, and pushes interleave between batches. Cutoffs are computed
//! CALLER-side from one `now()` (one consistent clock per cycle); a NULL cutoff
//! disables that rule.
//!
//! PARALLELISM (2026-08-10): phases 1-3 fan out over `RETENTION_PARALLELISM`
//! workers, each on its own pooled connection and its own maintenance-lane slot,
//! pulling partitions off a shared cursor. This is the ONLY way to raise the
//! deletion rate: the per-step row count is bounded by the push-latency budget
//! (the step holds the same `log_partitions` row lock the push allocator takes,
//! so a bigger batch buys nothing and stalls pushes — measured at 1M msg/s,
//! batch 8000 pushed client p99 from 0,6 s to 20 s and absorbed no more rows,
//! because the step cost is per-ROW, not per-CALL). Concurrency is safe for the
//! same reason the batching is: every step autocommits and takes exactly ONE
//! partition row lock, so two workers on two partitions never contend and a
//! single-row lock cannot deadlock whatever order partitions are visited in.
//! What the fan-out gives up is the ascending-id global visit order; nothing
//! depends on it — 006_log_maintenance's multi-partition lockers order among
//! THEMSELVES, and no step driven here ever takes a second lock. Phase 4 is
//! excluded on purpose: it is the one step that is not per-partition ("one call
//! handles a batch"), so it is the one that can hold more than one lock, and it
//! stays serial, on the cycle's tail connection (never the lock holder's —
//! that transaction runs nothing but the advisory-lock take).
//!
//! The advisory lock is TRANSACTION-scoped (`pg_try_advisory_xact_lock`),
//! taken inside an explicit transaction on a dedicated holder connection whose
//! ONLY statement is that take; the work list and every phase run on OTHER
//! pooled connections while the holder sits idle-in-transaction. Scope = release:
//! commit, rollback, a dropped cycle future (the transaction guard's Drop
//! enqueues ROLLBACK) and a dead connection all free the lock, so it cannot
//! outlive the work on any exit path. It used to be SESSION-scoped with an
//! explicit unlock "on every exit path" — and the 2026-08-24 bench cell showed
//! the paths that promise misses: the cycle's first eligibility SELECT over a
//! 91M-row queen.log_segments died on a statement timeout, the connection went
//! back to the pool with the unlock never run, and the healthy pooled session
//! held 737001 for 28 minutes while every replica logged "claimed but advisory
//! lock busy" — retention deadlocked cluster-wide until pg_terminate_backend
//! (same failure class as the PgBouncer advisory-lock recycling trap; xact
//! scope is also the flavor that survives transaction pooling). Session and
//! xact advisory locks share one lock space, so the belt still excludes
//! old-image session holders and vice versa. Since the claim-row scheduler the
//! lock is BELT, not scheduler: the lease row decides who sweeps each period,
//! and the lock only excludes overlap against old-image pods (mixed-fleet
//! window) so replicas never double-delete.
//!
//! THE HOLDER TRANSACTION TOUCHES NO TABLE (2026-08-24, boot-DDL half). It used
//! to run the work-list query inside itself — one statement, and the cheapest
//! thing in the cycle — but PostgreSQL releases a relation lock at the end of
//! the TRANSACTION, not of the statement, so that one SELECT left `AccessShare`
//! on `queen.log_partitions` and `queen.queues` held for the ENTIRE cycle, the
//! holder sitting `idle in transaction` (as observed on the cell) while the
//! phases drained. That made this loop the broker's own standing blocker of
//! boot DDL: `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` on log_partitions waits
//! for `AccessExclusive`, which conflicts with any AccessShare, and a cycle
//! burning a backlog holds it for MINUTES — the schema applier's sliced retries
//! (schema.rs) lost 10 of 10 boots against exactly this, a blocker with no gaps.
//! It was a traffic cost too: while that AccessExclusive request queues, every
//! NEW acquirer of the table queues behind it. So the work list moved to its own
//! pooled connection in its own short transaction — same single statement, same
//! one `now()`, same cutoffs, same fencing — and its locks now live for the
//! length of the query instead of the length of the cycle. The holder keeps its
//! transaction open for the whole cycle on purpose (that IS the lock's scope),
//! and it is now a transaction that has touched no user relation at all.
//!
//! WORK LIST = O(DELETABLE WORK), not Θ(#partitions) (2026-08-24 redesign).
//! The cycle used to materialize ONE ROW PER PARTITION — `queen.queues JOIN
//! queen.log_partitions ORDER BY p.id`, 827k rows on the soak cell, the exact
//! query whose statement timeout produced the belt-lock incident above — and
//! then make a per-partition step call for every one of them. At 2-5 ms a call
//! a serial pass is 30-70 MINUTES, and 99.9% of the visits found nothing
//! eligible: the measured delete rate was ~20 segments/s against ~500/s of
//! newly-eligible segments, i.e. 25x behind with no path to catching up,
//! because the cost was set by the partition COUNT and not by the work.
//!
//! Now each partition carries the age of its own oldest live data as an indexed
//! scalar (`log_partitions.oldest_live_at` / `oldest_txn_at`, maintained by the
//! step functions and the push allocator — 001_log_schema states the invariant
//! and names every writer), so the cycle asks each queue's index directly for
//! the partitions that CAN yield something:
//!
//!   * Θ(#queues) rule rows (230 on the cell) computed from queue config at
//!     query time — the columns store the FACT (age of the oldest live data),
//!     never the POLICY (eligibility), so an edit to retention_seconds applies
//!     next cycle with nothing to invalidate;
//!   * one indexed range scan per queue per rule, `LIMIT` the due cap, ordered
//!     by the watermark so progress is oldest-first and anything past the cap
//!     is simply the next cycle's head;
//!   * a rule whose cutoff is NULL for a queue emits NOTHING (`col < NULL` is
//!     never true), so the max_wait phase costs zero on the queues — the
//!     majority — that never configured it.
//!
//! NO-OP BACKOFF: a partition can be due BY AGE and still delete nothing —
//! rule 2 (completed_retention) is capped at the slowest cursor's next wanted
//! offset, so an unconsumed or unconsumable partition no-ops without advancing
//! its watermark and would re-appear at the head of the due list every cycle
//! forever. [`Backoff`] skips such a partition for a doubling number of cycles.
//!
//! THE PER-CYCLE PATH NEVER FALLS BACK TO Θ(#partitions). The full walk still
//! exists, but only as the watermark BACKFILL + SAFETY WALK (`walk_loop`
//! below): its own lease row, its own slow cadence, its own bounded batches.
//! There is deliberately no `QUEEN_RETENTION_LEGACY_WORKLIST` escape hatch —
//! the legacy list IS the query that deadlocked the cluster at this scale, so
//! an env that restores it is an env that restores the incident. The operator
//! lever for "visit everything now" is `QUEEN_RETENTION_SAFETY_WALK_MS` set
//! low, which does the same job in bounded batches that cannot time out.
//!
//! A failing cycle is logged and swallowed so the loop survives — a transient DB
//! error must not kill background maintenance.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;

use crate::config::Config;
use crate::db;

/// Advisory lock shared with the C++ retention/eviction services
/// (server/src/services/retention_service.cpp:73), now transaction-level (see
/// module docs). A replica that can't acquire it skips the cycle.
const CLEANUP_LOCK_ID: i64 = 737_001;

/// The one lock statement. `xact` is load-bearing (module docs): the lock's
/// lifetime is the holder transaction's, so no broker-side exit path — error,
/// timeout, cancellation, drop — can leave it held on a pooled connection.
const LOCK_SQL: &str = "SELECT pg_try_advisory_xact_lock($1)";

/// Ceiling on RETENTION_PARALLELISM. Each worker holds a maintenance-lane
/// admission slot AND a pooled connection for the length of its share of the
/// cycle, so an operator who types a big number would starve the lane rather
/// than speed it up (the lane is a fraction of the admission budget —
/// QUEEN_ADMISSION_SHARE_MAINT). 16 is above any measured need: the 2026-08-10
/// 1M msg/s rig needs ~14.6k step rows/s, which 4 workers cover ~3x over.
const MAX_PARALLELISM: usize = 16;

/// The ceiling must leave room for the measured need: 1M msg/s wants ~14.6k step
/// rows/s against the ~13.8k serial ceiling, i.e. 2 workers to break even and 4
/// for margin. Lowering this below 4 would silently cap the fix, so it fails the
/// build instead.
const _: () = assert!(MAX_PARALLELISM >= 4);

/// Sub-cadence of the partition-cleanup phase. The other phases are keyed to
/// message age and want the 5s cycle; this one is keyed to a window measured in
/// DAYS, and its candidate scan is O(#partitions) rather than O(work done) — the
/// exact cost class that made background maintenance scale with partitions
/// instead of messages. Once a minute is 300x more often than the C++ service
/// managed (which ran it on the 5-minute retention interval) and still rounds to
/// nothing against a 30-day window.
const PARTITION_SWEEP_EVERY: Duration = Duration::from_secs(60);

/// Multiplier of the derived due cap (`QUEEN_RETENTION_DUE_CAP=0`): the cap is
/// `RETENTION_BATCH_SIZE x RETENTION_PARALLELISM x` this. Batch size is the
/// operator's declared appetite for work per step call and parallelism is how
/// many calls a cycle can actually execute in its period, so their product is
/// the honest budget; 5 puts the default (1000 x 1 x 5 = 5000) at ~2x the ~2500
/// partition visits the 2026-08-24 cell needs per 5 s cycle to keep pace with
/// ~500 newly-eligible segments/s.
pub const DUE_CAP_FACTOR: usize = 5;

/// Ceiling on `QUEEN_RETENTION_DUE_CAP`. A cycle that pulled a million due
/// partitions per queue per rule could not execute the step calls in any
/// period — the lever for "delete faster" is RETENTION_PARALLELISM, not a
/// bigger list — and the list itself would be the Θ(#partitions)
/// materialization this redesign removed. Range-checked at boot, never clamped.
pub const DUE_CAP_MAX: usize = 1_000_000;

/// Ceiling on `QUEEN_RETENTION_SAFETY_WALK_MS`: one year. Past that the walk is
/// indistinguishable from off, and off has its own spelling (0).
pub const SAFETY_WALK_MAX_MS: u64 = 365 * 24 * 3_600_000;

/// The cycle work list. ONE statement, so every cutoff AND every due-ness test
/// shares one `now()` (all columns of one statement do), and the step calls
/// cast the prerendered `timestamptz::text` back. A NULL cutoff = that rule is
/// disabled for the queue (the `retention_seconds > 0` gating inherited from
/// the retired seg engine's sweep, decided here, not in SQL —
/// 006_log_maintenance header) and emits no partitions at all, because
/// `col < NULL` is never true.
///
/// Shape: a phase-0 row per queue carrying the four cutoffs ONCE, then one
/// tagged row per due partition carrying only (pid, queue id). The cutoffs are
/// deliberately not repeated per partition — that duplication is what made the
/// old list heavy in bytes as well as in rows.
///
/// Phases 1 and 3 probe `oldest_live_at`, phase 2 probes `oldest_txn_at`: the
/// sidecar needs its OWN fact because its cutoff (now() - GREATEST(dedup_window,
/// completed_retention, 900), 3600 s by default) is much SHORTER than a typical
/// retention window, so a segment-derived due test would be true for every
/// partition holding more than an hour of data — 827k of them on the measured
/// cell, i.e. the cost class this whole list exists to remove (001_log_schema).
///
/// Phase 1 tests against `GREATEST(all_cutoff, completed_cutoff)` because the
/// step takes both rules in one call: if the oldest live segment is FRESHER
/// than both cutoffs, neither rule can delete anything, so the test is
/// necessary and cheap. It is not sufficient — that is what [`Backoff`] is for.
///
/// The due cap is INLINED as a literal, not bound as `$1`: a parametric LIMIT
/// makes the planner assume the limit retains ~10% of the rows, which is
/// exactly how the hot-list full reseed lost its index plan and chose a PK walk
/// over every partition. The cap is fixed at boot, so one string per process.
///
/// Queue identity is the queen.queues id: log_partitions.queue_id references
/// queen.queues(id) directly, so partitions meet their config row BY ID — there
/// is no name+tenant bridge left to get wrong, and one tenant's cutoffs can
/// never be emitted against another tenant's partitions (which the step calls
/// below execute as DELETEs).
fn work_list_sql(due_cap: usize) -> String {
    format!(
        "WITH q AS ( \
             SELECT qq.id, \
                    CASE WHEN qq.retention_enabled AND COALESCE(qq.retention_seconds, 0) > 0 \
                         THEN now() - make_interval(secs => qq.retention_seconds) \
                         END AS all_cutoff, \
                    CASE WHEN qq.retention_enabled \
                          AND COALESCE(qq.completed_retention_seconds, 0) > 0 \
                         THEN now() - make_interval(secs => qq.completed_retention_seconds) \
                         END AS completed_cutoff, \
                    now() - make_interval(secs => GREATEST( \
                        qq.dedup_window_seconds, \
                        COALESCE(qq.completed_retention_seconds, 0), \
                        900)) AS txns_cutoff, \
                    CASE WHEN COALESCE(qq.max_wait_time_seconds, 0) > 0 \
                         THEN now() - make_interval(secs => qq.max_wait_time_seconds) \
                         END AS max_wait_cutoff \
             FROM queen.queues qq \
         ) \
         SELECT 0::int AS phase, NULL::text AS pid, q.id::text AS queue_id, \
                q.all_cutoff::text, q.completed_cutoff::text, \
                q.txns_cutoff::text, q.max_wait_cutoff::text \
         FROM q \
         UNION ALL \
         SELECT 1, p.id::text, q.id::text, NULL::text, NULL::text, NULL::text, NULL::text \
         FROM q CROSS JOIN LATERAL ( \
             SELECT p.id FROM queen.log_partitions p \
             WHERE p.queue_id = q.id \
               AND p.oldest_live_at < GREATEST(q.all_cutoff, q.completed_cutoff) \
             ORDER BY p.oldest_live_at LIMIT {due_cap}) p \
         UNION ALL \
         SELECT 2, p.id::text, q.id::text, NULL::text, NULL::text, NULL::text, NULL::text \
         FROM q CROSS JOIN LATERAL ( \
             SELECT p.id FROM queen.log_partitions p \
             WHERE p.queue_id = q.id \
               AND p.oldest_txn_at < q.txns_cutoff \
             ORDER BY p.oldest_txn_at LIMIT {due_cap}) p \
         UNION ALL \
         SELECT 3, p.id::text, q.id::text, NULL::text, NULL::text, NULL::text, NULL::text \
         FROM q CROSS JOIN LATERAL ( \
             SELECT p.id FROM queen.log_partitions p \
             WHERE p.queue_id = q.id \
               AND p.oldest_live_at < q.max_wait_cutoff \
             ORDER BY p.oldest_live_at LIMIT {due_cap}) p"
    )
}

/// One bounded batch of the watermark backfill / safety walk
/// (006_log_maintenance's `log_watermark_walk_step_v1`). `$1` is the previous
/// batch's last id (NULL to start), `$2` the batch size.
const WALK_STEP_SQL: &str =
    "SELECT (queen.log_watermark_walk_step_v1($1::text::uuid, $2::int))::text";

/// Lease-table task name for the watermark backfill / safety walk. Its OWN row,
/// not the retention cycle's: the two have cadences three orders of magnitude
/// apart, and a walk that consumed the 5 s cycle's budget would stall deletion
/// for the length of a full pass.
const WALK_TASK: &str = "retention_watermark";

/// Partitions per walk statement. The batch is a lock-hold trade, and only on
/// the values that actually CHANGE: the walk's UPDATE carries an
/// `IS DISTINCT FROM` predicate, so a steady-state pass locks NOTHING and this
/// number only bounds the initial backfill, where every value moves NULL ->
/// timestamp. At ~0.05 ms per PK probe a 5000-row batch is ~250 ms of row locks
/// on those 5000 partitions, once, at boot — the same order as one
/// retention step's batch of 1000 deletes, and the reason this is a constant
/// rather than a knob: there is one right answer and it is paid once a day.
const WALK_BATCH: i32 = 5_000;

/// Schedule used when `QUEEN_RETENTION_SAFETY_WALK_MS=0`. The knob disables the
/// RECURRING walk; it must not disable the FIRST one, because a database whose
/// watermark columns are still NULL has an empty work list and retention stops
/// dead with nothing in the log to say so. A year out is "never" for any real
/// deployment while keeping exactly one code path (config.rs WARNs at boot).
const WALK_DISABLED_PERIOD_MS: u64 = 365 * 24 * 3_600_000;

/// "Disabled" must be at least as far out as the widest schedule an operator can
/// legally ask for, or `QUEEN_RETENTION_SAFETY_WALK_MS=0` would walk MORE often
/// than the maximum. Compile-time, like the parallelism floor above.
const _: () = assert!(WALK_DISABLED_PERIOD_MS >= SAFETY_WALK_MAX_MS);

/// Launch the retention/eviction background loop. Non-blocking: spawns a detached
/// tokio task and returns immediately. Call once at boot, before `axum::serve`.
pub fn spawn(pool: Pool, cfg: &Config) {
    let interval = Duration::from_millis(cfg.retention_interval_ms);
    let knobs = Knobs {
        metrics_retention_days: cfg.metrics_retention_days,
        batch_size: cfg.retention_batch_size,
        parallelism: cfg.retention_parallelism.clamp(1, MAX_PARALLELISM),
        partition_cleanup_days: cfg.partition_cleanup_enabled.then_some(cfg.partition_cleanup_days),
        lock_stmt_timeout_ms: cfg.retention_interval_ms.clamp(30_000, 900_000),
    };
    // 0 is not reachable here (config.rs derives it), but a caller that built a
    // Config by hand must not silently get a due list of nothing.
    let due_cap = cfg.retention_due_cap.clamp(1, DUE_CAP_MAX);
    // ONE string per process: the due cap is a literal inside it (see
    // work_list_sql), so building it per cycle would be pure churn.
    let work_list = Arc::new(work_list_sql(due_cap));
    // 0 = the recurring walk is off; the FIRST one still runs (see
    // WALK_DISABLED_PERIOD_MS).
    let walk_ms = if cfg.retention_safety_walk_ms == 0 {
        WALK_DISABLED_PERIOD_MS
    } else {
        cfg.retention_safety_walk_ms.min(SAFETY_WALK_MAX_MS)
    };
    tracing::info!(
        target: "retention",
        interval_ms = cfg.retention_interval_ms,
        advisory_lock = CLEANUP_LOCK_ID,
        batch_size = knobs.batch_size,
        parallelism = knobs.parallelism,
        due_cap,
        safety_walk_ms = walk_ms,
        metrics_retention_days = knobs.metrics_retention_days,
        // None = QUEEN_PARTITION_CLEANUP_ENABLED=false, i.e. phase 4 never runs.
        partition_cleanup_days = ?knobs.partition_cleanup_days,
        "service started"
    );
    let holder = crate::lease::holder_id(cfg);
    // The walk is its own loop on its own lease row: it is the only thing left
    // that visits every partition, and it must never share a budget with the
    // 5 s deletion cycle.
    let walk_pool = pool.clone();
    let walk_holder = holder.clone();
    tokio::spawn(async move { walk_loop(walk_pool, walk_ms, walk_holder).await });
    tokio::spawn(async move { run_loop(pool, interval, knobs, work_list, holder).await });
}

/// Lease-table task name for the whole maintenance cycle
/// (029_maintenance_leases). One row for the cycle, not one per phase: the
/// phases share a work list, one consistent now(), and the belt lock,
/// and phase ordering (steps before txns purge before cleanup) is load-bearing.
/// Phase 4 keeps its own sub-cadence inside the cycle (PARTITION_SWEEP_EVERY).
const TASK: &str = "retention";

/// Cfg values the maintenance cycle needs. A small Copy struct so the values
/// move cleanly into the detached task. `batch_size` = p_max_rows per step call
/// (RETENTION_BATCH_SIZE) and also bounds each metrics-purge DELETE.
#[derive(Clone, Copy)]
struct Knobs {
    metrics_retention_days: i32,
    batch_size: usize,
    /// Concurrent per-partition step workers in phases 1-3 (RETENTION_PARALLELISM,
    /// clamped to MAX_PARALLELISM). 1 = the historical serial cycle.
    parallelism: usize,
    /// `None` = partition cleanup disabled (QUEEN_PARTITION_CLEANUP_ENABLED
    /// false). `Some(days)` = the inactivity window for phase 4. Folding the
    /// flag into the Option keeps "is it on" and "how old" from disagreeing.
    partition_cleanup_days: Option<i32>,
    /// `SET LOCAL statement_timeout` for BOTH short transactions the cycle
    /// leader opens — the holder's (whose only statement is the lock take) and
    /// the work list's, on its own connection — the stats.rs T0.2 bound,
    /// derived the same way from the task cadence. Maintenance-grade on both
    /// sides: it exempts the work list from whatever ambient default the
    /// database runs with, and it bounds how long a cancelled cycle's in-flight
    /// statement can delay the queued ROLLBACK that frees the belt lock. The
    /// list is Θ(due work) since the 2026-08-24 redesign, so this is now a
    /// backstop rather than a bound the query is expected to approach — the day
    /// it starts firing again, something has un-indexed the watermark probes.
    lock_stmt_timeout_ms: u64,
}

impl Knobs {
    /// `p_max_rows` for the step SPs (their INT arg).
    fn max_rows(&self) -> i32 {
        self.batch_size.min(i32::MAX as usize) as i32
    }
}

async fn run_loop(
    pool: Pool,
    period: Duration,
    knobs: Knobs,
    work_list: Arc<String>,
    holder: String,
) {
    let period_ms = period.as_millis() as u64;
    let poll = crate::lease::poll_interval(period);
    let lease_ms = crate::lease::lease_ms(period_ms);
    // The schedule row must exist before the first claim; the DB may still be
    // coming up at boot, so retry on the poll cadence rather than dying.
    while let Err(e) = crate::lease::ensure_row(&pool, TASK, period_ms).await {
        if let Some(suppressed) = CYCLE_ERR.tick_now() {
            tracing::error!(target: "retention", error = %e, suppressed, "lease row upsert failed");
        }
        tokio::time::sleep(poll).await;
    }
    // Phase 4's own clock (see PARTITION_SWEEP_EVERY). Starts elapsed so the
    // first cycle after boot sweeps; only advanced when the phase actually ran,
    // so cycles this replica did not win don't consume the interval.
    let mut partitions_swept_at: Option<Instant> = None;
    loop {
        // Durable schedule (029_maintenance_leases): RETENTION_INTERVAL is the
        // TRUE cluster cadence — whoever wins the claim sweeps this period.
        let fence = match crate::lease::claim(&pool, TASK, lease_ms, &holder).await {
            Ok(Some(fence)) => fence,
            Ok(None) => {
                tokio::time::sleep(poll).await;
                continue;
            }
            Err(e) => {
                if let Some(suppressed) = CYCLE_ERR.tick_now() {
                    tracing::error!(target: "retention", error = %e, suppressed, "lease claim error");
                }
                tokio::time::sleep(poll).await;
                continue;
            }
        };
        let start = Instant::now();
        let sweep_partitions = partitions_swept_at
            .is_none_or(|at| start.duration_since(at) >= PARTITION_SWEEP_EVERY);
        match run_cycle(&pool, knobs, &work_list, sweep_partitions).await {
            Ok(Outcome::Skipped) => {
                // Belt session lock busy: an old-image pod's own timer is
                // sweeping right now (mixed-fleet window). Count the period
                // served rather than churning the claim against its lock.
                tracing::info!(
                    target: "retention",
                    "claimed but advisory lock busy; period served by another replica"
                );
                let elapsed_ms = start.elapsed().as_millis() as i32;
                crate::lease::release(&pool, TASK, fence, crate::lease::Release::Advance { elapsed_ms })
                    .await;
            }
            Ok(Outcome::Ran {
                queues,
                due,
                backed_off,
                segments_deleted,
                txns_purged,
                max_wait,
                partitions_deleted,
                partitions_scanned,
                metrics,
            }) => {
                if partitions_scanned {
                    partitions_swept_at = Some(start);
                }
                let elapsed_ms = start.elapsed().as_millis() as u64;
                // LOGGING_PLAN.md Phase 2: only speak when the cycle actually
                // deleted something — an idle cluster used to emit this at INFO
                // every RETENTION_INTERVAL (5s) with all-zero counters. The metrics
                // purge (90-day window) rarely fires, so it stays out of the gate;
                // a working cycle names the counts + elapsed, an idle one is DEBUG.
                //
                // `due` and `backed_off` are the two numbers that explain a
                // delete RATE (2026-08-24 redesign): due at the cap every cycle
                // means the cycle is capped, not idle, and a large backed_off
                // means the due partitions are cursor-capped rather than
                // deletable. Without them the only observable was the delete
                // count, which is why "20 segments/s" took a soak to diagnose.
                if segments_deleted > 0 || txns_purged > 0 || max_wait > 0 || partitions_deleted > 0
                {
                    tracing::info!(
                        target: "retention",
                        queues,
                        due,
                        backed_off,
                        segments_deleted,
                        txns_purged,
                        max_wait_evicted = max_wait,
                        partitions_deleted,
                        metrics_purge = %metrics.trim(),
                        elapsed_ms,
                        "swept"
                    );
                } else {
                    tracing::debug!(
                        target: "retention", queues, due, backed_off, elapsed_ms, "idle cycle");
                }
                crate::lease::release(
                    &pool,
                    TASK,
                    fence,
                    crate::lease::Release::Advance { elapsed_ms: elapsed_ms as i32 },
                )
                .await;
            }
            Err(e) => {
                // A sustained DB outage must not emit one ERROR every 5s.
                if let Some(suppressed) = CYCLE_ERR.tick_now() {
                    tracing::error!(target: "retention", error = %e, suppressed, "cycle error");
                }
                crate::lease::release(&pool, TASK, fence, crate::lease::Release::Retry).await;
            }
        }
        // The poll doubles as the anti-spin floor when the cycle overruns the
        // period (lease::poll_interval — the exact failure that produced the
        // 60s -> 300s -> 900s interval escalation at 26k partitions).
        tokio::time::sleep(poll).await;
    }
}

enum Outcome {
    /// Advisory lock was held by another replica; cycle skipped.
    Skipped,
    /// Cycle ran; carries the numeric work counts (so the loop can suppress
    /// idle no-op cycles) + a short metrics-purge summary.
    Ran {
        queues: usize,
        /// Due partitions the work list returned this cycle, summed over the
        /// three phases, BEFORE the no-op backoff filtered it.
        due: usize,
        /// How many of those the backoff skipped (see [`Backoff`]).
        backed_off: usize,
        segments_deleted: i64,
        txns_purged: i64,
        max_wait: i64,
        partitions_deleted: i64,
        /// Whether phase 4 actually ran this cycle (it has its own sub-cadence
        /// and can be disabled), so the loop only restarts that clock when the
        /// scan really happened.
        partitions_scanned: bool,
        metrics: String,
    },
}

/// Rate-limit the cycle-error ERROR so a sustained DB outage doesn't emit one
/// line per RETENTION_INTERVAL (LOGGING_PLAN.md WARN/ERROR doctrine).
static CYCLE_ERR: crate::obs::Sampler = crate::obs::Sampler::new(30_000);
static PURGE_ERR: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// One maintenance cycle: open the lock-holder transaction, try the XACT
/// advisory lock inside it, run the work list and the phases on OTHER
/// connections (each an autocommitting SP call — no wrapping transaction around
/// the work), then end the holder transaction. There is deliberately NO explicit
/// unlock left anywhere: the lock's release IS the transaction's end, which
/// happens on the commit and rollback arms below, on the guard's Drop when this
/// future is cancelled, and with the session when the connection dies — so no
/// exit path can hand a still-locked connection back to the pool (the
/// 2026-08-24 incident, module docs). The holder is idle-in-transaction while
/// the cycle runs; that is the design, not a leak — the take is its ONLY
/// statement, so the transaction holds no relation lock and no snapshot-pinning
/// work, and no `idle_in_transaction_session_timeout` is set because a
/// legitimate backlog-drain cycle can idle the holder for minutes and severing
/// it mid-cycle would drop the belt while workers are still deleting.
async fn run_cycle(
    pool: &Pool,
    knobs: Knobs,
    work_list: &str,
    sweep_partitions: bool,
) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    // Maintenance-lane admission for the leader's OWN database work: the
    // advisory lock and the work-list query. The individual step commits do not
    // feed the train sensor — a held slot without commit_done contributes
    // accounting, never fake samples.
    let slot = crate::admission::lane_slot(crate::admission::Lane::Maint).await;
    let mut lock_client = pool.get().await?;
    let lock_tx = lock_client.transaction().await?;
    // SET LOCAL dies with the transaction, so the bound cannot leak into the
    // pooled connection (stats.rs T0.2 precedent; see Knobs::lock_stmt_timeout_ms).
    lock_tx
        .batch_execute(&format!("SET LOCAL statement_timeout = {}", knobs.lock_stmt_timeout_ms))
        .await?;
    // The take is the LAST table-free statement this transaction may ever run.
    // Anything added here that reads a queen table holds its AccessShare for the
    // whole cycle and re-creates the boot-DDL blocker (module docs); the work
    // list itself was moved out for exactly that reason.
    let got: bool = lock_tx.query_one(LOCK_SQL, &[&CLEANUP_LOCK_ID]).await?.get(0);
    if !got {
        // Early return drops lock_tx: ROLLBACK is enqueued and the connection
        // goes back to the pool clean.
        return Ok(Outcome::Skipped);
    }
    // Hand the slot to cycle_body, which releases it before the fan-out and
    // takes a fresh one for the phases that need a connection of their own (4
    // and 5). Holding it across the fan-out would spend a maintenance slot on a
    // task that is only awaiting its workers — at the lane's decayed cap of 2
    // that was HALF the lane, and it made the fan-out exactly as slow as the
    // serial cycle it replaced (measured 2026-08-10).
    let res = cycle_body(pool, slot, knobs, work_list, sweep_partitions).await;
    // Both arms release the belt lock; errors are ignored because a connection
    // that cannot end its transaction is a dead session, and the server has
    // already released the lock with it.
    match &res {
        Ok(_) => {
            let _ = lock_tx.commit().await;
        }
        // The holder transaction cannot itself be aborted any more (its one
        // statement succeeded, or we never got here), but ROLLBACK is valid in
        // either state and stays the error arm's exit.
        Err(_) => {
            let _ = lock_tx.rollback().await;
        }
    }
    res
}

async fn cycle_body(
    pool: &Pool,
    leader_slot: Option<crate::admission::Slot>,
    knobs: Knobs,
    work_list: &str,
    sweep_partitions: bool,
) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    // Work list, ONCE per cycle: the per-queue rule rows plus, per phase, the
    // DUE partitions only — those whose indexed watermark says they can yield
    // something under this cycle's cutoffs (see work_list_sql).
    //
    // On its OWN pooled connection, in its OWN short transaction, NEVER on the
    // lock holder's (module docs, boot-DDL half): relation locks live until the
    // transaction ends, so running this inside the holder pinned AccessShare on
    // queen.log_partitions for the whole cycle and blocked boot DDL for as long
    // as a backlog took to burn. Here the locks die with the query. The
    // transaction exists only to carry SET LOCAL statement_timeout — the bound
    // that must not leak into a pooled connection — and it is the ONE statement
    // of the cycle that historically timed out, so it keeps that bound.
    //
    // Still ONE statement, so every cutoff and every due-ness test still share
    // one server-side now() (work_list_sql): moving it off the holder changed
    // which connection asks, not how many clocks answer.
    let mut list_client = pool.get().await?;
    let list_tx = list_client.transaction().await?;
    list_tx
        .batch_execute(&format!("SET LOCAL statement_timeout = {}", knobs.lock_stmt_timeout_ms))
        .await?;
    let stmt = list_tx.prepare_cached(work_list).await?;
    let rows = list_tx.query(&stmt, &[]).await?;
    let (rules, mut due) = parse_work_list(&rows);
    // Read-only: commit and rollback are equivalent here, and both release the
    // locks. Commit is the arm that also proves the connection is healthy
    // before it goes back to the pool.
    list_tx.commit().await?;
    // Back to the pool BEFORE the fan-out: a connection idling here is one the
    // phase workers cannot have (the cycle is sized against pool capacity).
    drop(list_client);
    let rules = Arc::new(rules);
    let due_total = due.iter().map(Vec::len).sum::<usize>();
    let max_rows = knobs.max_rows();

    // NO-OP BACKOFF, applied before the fan-out so one map lock covers a whole
    // phase: a partition that is due BY AGE but yielded nothing last time (the
    // rule-2 cursor cap) is skipped for a doubling number of cycles instead of
    // costing a step call at the head of every list (see Backoff).
    let cycle = backoff().next_cycle();
    let mut backed_off = 0usize;
    for (phase_idx, list) in due.iter_mut().enumerate() {
        backed_off += backoff().retain_due(phase_idx as u8, cycle, list);
    }
    let [due_retention, due_txns, due_max_wait] = due;

    // The swept-queue count is keyed by queues.id, not name: names repeat
    // across tenants, so a name-keyed set would collapse distinct queues into
    // one. Every due row already belongs to a rule-applicable queue (a NULL
    // cutoff emits nothing), so counting the list IS counting the swept queues.
    let retention_queues: std::collections::HashSet<usize> =
        due_retention.iter().map(|d| d.q).collect();

    // Phases 1-3 are per-partition and independent, so each runs as a FAN-OUT
    // over `knobs.parallelism` workers (see run_phase). The phases still run in
    // order relative to each other — only the visit order WITHIN a phase stops
    // being ascending-by-id, which nothing depends on (module docs).
    let due_retention = Arc::new(due_retention);
    let due_txns = Arc::new(due_txns);
    let due_max_wait = Arc::new(due_max_wait);
    let par = knobs.parallelism;
    // The leader does no database work until phase 4: release its admission
    // slot so the fan-out workers can have it (see run_cycle).
    drop(leader_slot);

    // Phase 1: retention rules 1+2, gated as in the retired seg engine's sweep
    // (retention_enabled AND a positive window — encoded as at least one
    // non-NULL cutoff, which the work list turns into "this queue emits no
    // partitions at all").
    let segments_deleted =
        run_phase(pool, Phase::Retention, &rules, &due_retention, par, max_rows, cycle).await?;

    // Phase 2: log_txns hash-sidecar purge, on the partitions whose SIDECAR
    // watermark (oldest_txn_at) is past the window — not the segment one, which
    // would nominate every partition holding more than an hour of data
    // (work_list_sql / 001_log_schema).
    let txns_purged =
        run_phase(pool, Phase::Txns, &rules, &due_txns, par, max_rows, cycle).await?;

    // Phase 3: max_wait_time_seconds eviction — applies regardless of
    // retention_enabled (a queue configured with ONLY maxWaitTimeSeconds still
    // gets swept), matching the old db::seg_evict_max_wait / C++ EvictionService.
    let max_wait =
        run_phase(pool, Phase::MaxWait, &rules, &due_max_wait, par, max_rows, cycle).await?;

    // Phase 4: delete EMPTY, long-inactive partitions —
    // queen.log_partition_cleanup_step_v1, the restored C++
    // cleanup_inactive_partitions (PARTITION_CLEANUP_DAYS). Runs LAST of the data
    // phases on purpose: a partition that phases 1-3 just emptied becomes
    // eligible in the same cycle instead of waiting for the next one.
    //
    // Not driven by the due lists — the step selects and locks its own batch,
    // so a per-partition list would only add a round trip. Its own sub-cadence
    // (PARTITION_SWEEP_EVERY) and its off switch are folded into
    // knobs.partition_cleanup_days. This is the one phase still keyed to the
    // partition COUNT, which is exactly why it runs once a minute and not every
    // cycle; the 2026-08-24 work-list redesign leaves it untouched.
    // Phases 4 and 5 need a connection of their own: their statements must
    // keep autocommitting (one bounded transaction per step), which the lock
    // holder's open transaction cannot provide. Slot back first, then the
    // connection — slot BEFORE pool.get(), always.
    let _slot = crate::admission::lane_slot(crate::admission::Lane::Maint).await;
    let client = pool.get().await?;
    let mut partitions_deleted: i64 = 0;
    let partitions_scanned = sweep_partitions && knobs.partition_cleanup_days.is_some();
    if let (true, Some(days)) = (sweep_partitions, knobs.partition_cleanup_days) {
        // One clock for the phase, server-side, like every other cutoff here.
        let cutoff: String = client
            .query_one(
                "SELECT (now() - make_interval(days => $1::int))::text",
                &[&days],
            )
            .await?
            .get(0);
        let stmt = client
            .prepare_cached(
                "SELECT (queen.log_partition_cleanup_step_v1($1::text::timestamptz, \
                 $2::int))::text",
            )
            .await?;
        loop {
            let row = client.query_one(&stmt, &[&cutoff, &max_rows]).await?;
            let (deleted, done) = step_result(row.get(0));
            partitions_deleted += deleted;
            // deleted == 0 also covers "a full batch was selected but the
            // under-lock re-check spared every row", which must not spin.
            if done || deleted == 0 {
                break;
            }
        }
    }

    // Phase 5: purge stale metrics (RUSTFIX item 20; C++ RetentionService::
    // cleanup_old_metrics, retention_service.cpp:432-476) on the configured
    // window. Plain autocommit calls on the same client — the old SAVEPOINT
    // bracketing is gone because there is no shared transaction left to protect:
    // a failed purge statement can no longer poison the retention deletes, which
    // committed statement-by-statement above. Errors are logged, not fatal.
    let metrics = match purge_metrics(&client, knobs).await {
        Ok(s) => s,
        Err(e) => {
            if let Some(suppressed) = PURGE_ERR.tick_now() {
                tracing::error!(target: "retention", error = %e, suppressed, "metrics purge error");
            }
            "error".to_string()
        }
    };

    Ok(Outcome::Ran {
        queues: retention_queues.len(),
        due: due_total,
        backed_off,
        segments_deleted,
        txns_purged,
        max_wait,
        partitions_deleted,
        partitions_scanned,
        metrics,
    })
}

/// Which per-partition step family a fan-out round drives. Phase 4 is
/// deliberately absent — see the module docs' PARALLELISM note.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Retention,
    Txns,
    MaxWait,
}

impl Phase {
    /// Dense index, shared by the due-list array and the backoff key. The
    /// work-list SQL tags its rows `idx + 1` (0 is reserved for the rule rows).
    fn idx(self) -> u8 {
        match self {
            Phase::Retention => 0,
            Phase::Txns => 1,
            Phase::MaxWait => 2,
        }
    }

    fn sql(self) -> &'static str {
        match self {
            Phase::Retention => {
                "SELECT (queen.log_retention_step_v1($1::text::uuid, $2::text::timestamptz, \
                 $3::text::timestamptz, $4::int))::text"
            }
            Phase::Txns => {
                "SELECT (queen.log_txns_purge_step_v1($1::text::uuid, $2::text::timestamptz, \
                 $3::int))::text"
            }
            Phase::MaxWait => {
                "SELECT (queen.log_evict_max_wait_step_v1($1::text::uuid, $2::text::timestamptz, \
                 $3::int))::text"
            }
        }
    }
}

/// Drive one phase across its DUE list with `parallelism` workers.
///
/// The split is a shared CURSOR, not a static chunking: partition backlogs are
/// wildly uneven (one partition can hold a million rows while its neighbour
/// holds none), and a static split would leave workers idle behind whoever drew
/// the deep ones.
///
/// Returns the rows deleted by the phase. Partitions that yielded NOTHING are
/// reported straight into [`BACKOFF`] by the workers rather than plumbed back
/// here: the map is process-global and the phases never overlap, so a per-worker
/// insert is one uncontended lock and saves carrying a second vector of ids
/// through the join.
async fn run_phase(
    pool: &Pool,
    phase: Phase,
    rules: &Arc<Vec<Rules>>,
    due: &Arc<Vec<Due>>,
    parallelism: usize,
    max_rows: i32,
    cycle: u64,
) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    if due.is_empty() {
        return Ok(0);
    }
    let n = parallelism.clamp(1, due.len());
    let cursor = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let pool = pool.clone();
        let rules = Arc::clone(rules);
        let due = Arc::clone(due);
        let cursor = Arc::clone(&cursor);
        handles.push(tokio::spawn(async move {
            phase_worker(pool, phase, rules, due, cursor, max_rows, cycle).await
        }));
    }
    // Join ALL workers before reporting an error: a worker that fails must not
    // leave its siblings running against a connection the cycle has abandoned.
    // The cycle then fails as it always did (run_loop logs and retries next
    // tick), so a partial sweep is retried, never counted.
    let mut deleted = 0i64;
    let mut first_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;
    for h in handles {
        match h.await {
            Ok(Ok(d)) => deleted += d,
            Ok(Err(e)) => {
                first_err.get_or_insert(e);
            }
            Err(join) => {
                first_err.get_or_insert(Box::new(join));
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(deleted),
    }
}

/// One fan-out worker: its own admission slot, its own pooled connection, its
/// own prepared statement, pulling partitions off the shared cursor until the
/// due list is exhausted.
async fn phase_worker(
    pool: Pool,
    phase: Phase,
    rules: Arc<Vec<Rules>>,
    due: Arc<Vec<Due>>,
    cursor: Arc<AtomicUsize>,
    max_rows: i32,
    cycle: u64,
) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    let _slot = crate::admission::lane_slot(crate::admission::Lane::Maint).await;
    let client = pool.get().await?;
    let stmt = client.prepare_cached(phase.sql()).await?;
    let mut deleted_total: i64 = 0;
    loop {
        let Some(w) = due.get(cursor.fetch_add(1, Ordering::Relaxed)) else {
            break;
        };
        let Some(r) = rules.get(w.q) else { continue };
        // The step contract (006_log_maintenance) is done:true whenever nothing
        // was deleted, so `!done` implies progress; the deleted==0 arm is a
        // defensive stop against a contract break looping us forever.
        let mut here: i64 = 0;
        match phase {
            Phase::Retention => loop {
                let row = client
                    .query_one(&stmt, &[&w.pid, &r.all_cutoff, &r.completed_cutoff, &max_rows])
                    .await?;
                let (deleted, done) = step_result(row.get(0));
                here += deleted;
                if done || deleted == 0 {
                    break;
                }
            },
            Phase::Txns => {
                // The ONE cutoff that must never be sent as NULL. The other two
                // steps treat a NULL cutoff as "rule disabled" and no-op;
                // log_txns_purge_step_v1 does not — its walk finds no fresh row,
                // concludes the whole window is stale, and purges up to the
                // batch horizon. The work list cannot produce a NULL here (the
                // cutoff has a 900 s floor and no CASE around it), so this guard
                // is for the day someone adds one.
                let Some(cutoff) = &r.txns_cutoff else { continue };
                loop {
                    let row = client.query_one(&stmt, &[&w.pid, cutoff, &max_rows]).await?;
                    let (deleted, done) = step_result(row.get(0));
                    here += deleted;
                    if done || deleted == 0 {
                        break;
                    }
                }
            }
            Phase::MaxWait => loop {
                let row = client
                    .query_one(&stmt, &[&w.pid, &r.max_wait_cutoff, &max_rows])
                    .await?;
                let (deleted, done) = step_result(row.get(0));
                here += deleted;
                if done || deleted == 0 {
                    break;
                }
            },
        }
        deleted_total += here;
        backoff().observe(phase.idx(), cycle, &w.pid, here > 0);
    }
    Ok(deleted_total)
}

/// One queue's rules for this cycle: the four cutoffs, prerendered
/// `timestamptz::text` from ONE `now()` (`None` = that rule is disabled for the
/// queue). Held ONCE per queue and referenced by index from the due lists —
/// repeating four timestamps on every partition row is what made the old
/// 827k-row list heavy in bytes as well as in rows.
struct Rules {
    all_cutoff: Option<String>,
    completed_cutoff: Option<String>,
    txns_cutoff: Option<String>,
    max_wait_cutoff: Option<String>,
}

/// One due partition: its id and the index of its queue's [`Rules`].
struct Due {
    pid: String,
    q: usize,
}

/// One work-list row, decoded off the wire. `phase` 0 = a queue's rule row
/// (cutoffs populated), 1..3 = a due partition of `queue_id` for that phase.
struct RawRow {
    phase: i32,
    pid: Option<String>,
    queue_id: Option<String>,
    /// all / completed / txns / max_wait, in the SELECT's order.
    cutoffs: [Option<String>; 4],
}

/// Decode the pg rows and hand them to [`assemble_work_list`]. Split in two so
/// the assembly — the part with the actual rules in it — is unit-testable
/// without a live database (`tokio_postgres::Row` cannot be constructed).
fn parse_work_list(rows: &[tokio_postgres::Row]) -> (Vec<Rules>, [Vec<Due>; 3]) {
    assemble_work_list(rows.iter().map(|r| RawRow {
        phase: r.get(0),
        pid: r.get(1),
        queue_id: r.get(2),
        cutoffs: [r.get(3), r.get(4), r.get(5), r.get(6)],
    }))
}

/// Split the work-list result into the per-queue rule table and the three
/// per-phase due lists (indexes 0..2 = Retention, Txns, MaxWait — [`Phase::idx`]).
///
/// Two passes, because `UNION ALL` guarantees no row order: the phase-0 rows
/// (one per queue, carrying the cutoffs) are collected first, then every due row
/// is resolved against them. A due row whose queue is somehow absent is DROPPED
/// rather than defaulted — a partition swept with the wrong queue's cutoffs
/// would be a data-loss bug, and the work list is re-derived every cycle anyway.
fn assemble_work_list(rows: impl IntoIterator<Item = RawRow>) -> (Vec<Rules>, [Vec<Due>; 3]) {
    let rows: Vec<RawRow> = rows.into_iter().collect();
    let mut rules: Vec<Rules> = Vec::new();
    let mut index: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for r in rows.iter().filter(|r| r.phase == 0) {
        let Some(queue_id) = r.queue_id.as_deref() else { continue };
        index.insert(queue_id, rules.len());
        let [all, completed, txns, max_wait] = &r.cutoffs;
        rules.push(Rules {
            all_cutoff: all.clone(),
            completed_cutoff: completed.clone(),
            txns_cutoff: txns.clone(),
            max_wait_cutoff: max_wait.clone(),
        });
    }
    let mut due: [Vec<Due>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    for r in rows.iter().filter(|r| r.phase != 0) {
        let (Some(pid), Some(queue_id)) = (r.pid.as_deref(), r.queue_id.as_deref()) else {
            continue;
        };
        let (Some(&q), Some(list)) = (index.get(queue_id), due.get_mut((r.phase - 1) as usize))
        else {
            continue;
        };
        list.push(Due { pid: pid.to_string(), q });
    }
    (rules, due)
}

// ---------------------------------------------------------------------------
// NO-OP BACKOFF
// ---------------------------------------------------------------------------

/// Cycles skipped after a partition's FIRST fruitless visit. Small on purpose:
/// the common cause is a rule-2 cursor that is about to move, and 8 cycles is
/// 40 s at the default cadence — short enough that a consumer catching up does
/// not have to wait for retention.
const BACKOFF_BASE_CYCLES: u64 = 8;

/// The skip DOUBLES per consecutive fruitless visit, capped at
/// `BACKOFF_BASE_CYCLES << this` = 512 cycles (~43 min at the 5 s default).
/// A flat 8 is not enough on its own: a queue configured with
/// completed_retention only, whose partitions nobody consumes, is due by age
/// FOREVER and would cost `partitions / 8` step calls every cycle — 103k on the
/// measured cell. Doubling turns that into `partitions / 512` (~1.6k) while
/// keeping the first retry fast, and any real delete resets the strike count.
const BACKOFF_MAX_SHIFT: u32 = 6;

/// Entries the map may hold, across all three phases. ~100 bytes an entry, so
/// ~10 MB at the cap — the same order as the other bounded in-memory maps in
/// this broker, sized for fleets with 100k+ queues. FAIL-OPEN when full: the
/// partition is simply visited again, i.e. the pre-backoff behaviour, never a
/// skipped delete.
const BACKOFF_MAX_ENTRIES: usize = 100_000;

/// Process-local skip list for partitions that are due BY AGE but yield
/// nothing.
///
/// Why they exist: rule 2 (completed_retention) caps its boundary at the
/// slowest cursor's next wanted offset, so a partition whose consumers are
/// behind — or which has no consumer rows at all — is older than the cutoff and
/// still has nothing deletable. The step no-ops, which means it cannot advance
/// the watermark, which means the work list nominates it again next cycle, at
/// the HEAD of the list (it is ordered oldest-first). Left alone that is an
/// unbounded tax on every cycle, paid in the 2-5 ms step calls this redesign
/// exists to stop making.
///
/// Process-local and lossy by design: it is a cost optimisation, never a
/// correctness input. A restart, an eviction, a leadership change — all of them
/// just mean one extra visit.
struct Backoff {
    seq: AtomicU64,
    /// ONE map per phase, not one map keyed by (phase, pid): a tuple key would
    /// force a `String` allocation on every LOOKUP, i.e. thousands of throwaway
    /// heap allocations per cycle, where a per-phase map borrows the id.
    phases: std::sync::Mutex<[std::collections::HashMap<String, Strike>; 3]>,
}

#[derive(Clone, Copy)]
struct Strike {
    /// First cycle at which this partition may be visited again.
    until: u64,
    /// Consecutive fruitless visits, for the doubling.
    strikes: u32,
}

/// The one process-global instance. `OnceLock` (the house idiom for a global
/// map — admission.rs, db.rs) because `HashMap::new` is not a `const fn`.
fn backoff() -> &'static Backoff {
    static B: std::sync::OnceLock<Backoff> = std::sync::OnceLock::new();
    B.get_or_init(Backoff::new)
}

impl Backoff {
    /// A fresh, empty instance. The broker has exactly one (see [`backoff`]);
    /// the tests build their own so they never race the global.
    fn new() -> Self {
        Backoff {
            seq: AtomicU64::new(0),
            phases: std::sync::Mutex::new(Default::default()),
        }
    }

    /// Cycle number for this pass. Monotone per process; the map's `until`
    /// values are relative to it, so wrapping is not a concern at any real
    /// cadence (u64 cycles at 5 s outlasts the hardware by a wide margin).
    fn next_cycle(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Drop the still-skipped partitions from a phase's due list IN PLACE,
    /// returning how many were dropped. Called once per phase, on the cycle
    /// thread, so the fan-out never contends on the lock for filtering.
    fn retain_due(&self, phase: u8, cycle: u64, due: &mut Vec<Due>) -> usize {
        let Ok(phases) = self.phases.lock() else { return 0 };
        let Some(map) = phases.get(phase as usize) else { return 0 };
        if map.is_empty() {
            return 0;
        }
        let before = due.len();
        due.retain(|d| map.get(d.pid.as_str()).is_none_or(|s| s.until <= cycle));
        before - due.len()
    }

    /// Record the outcome of one partition's visit: progress clears its strikes,
    /// a fruitless visit adds one and pushes the next visit out.
    fn observe(&self, phase: u8, cycle: u64, pid: &str, progressed: bool) {
        let Ok(mut phases) = self.phases.lock() else { return };
        let total: usize = phases.iter().map(|m| m.len()).sum();
        let Some(map) = phases.get_mut(phase as usize) else { return };
        if progressed {
            map.remove(pid);
            return;
        }
        let strikes = map.get(pid).map_or(0, |s| s.strikes).saturating_add(1);
        let shift = (strikes - 1).min(BACKOFF_MAX_SHIFT);
        let until = cycle + (BACKOFF_BASE_CYCLES << shift);
        if total >= BACKOFF_MAX_ENTRIES && !map.contains_key(pid) {
            // Expired entries first — they are pure garbage and dropping them
            // costs nothing. Only if that still leaves no room do we fail open.
            map.retain(|_, s| s.until > cycle);
            if map.len() >= BACKOFF_MAX_ENTRIES {
                return;
            }
        }
        map.insert(pid.to_string(), Strike { until, strikes });
    }
}

// ---------------------------------------------------------------------------
// WATERMARK BACKFILL + SAFETY WALK
// ---------------------------------------------------------------------------

/// Rate-limit the walk's own errors and its in-progress line.
static WALK_ERR: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
static WALK_PROGRESS: crate::obs::Sampler = crate::obs::Sampler::new(10_000);

/// The watermark loop: BACKFILL on the first run, SAFETY WALK on every one
/// after (they are the same statement — 006's log_watermark_walk_step_v1).
///
/// SCHEDULING is the durable lease row, and that row IS the "do we need a
/// backfill?" sentinel: a virgin database (or one upgraded into the columns)
/// has no row, `ensure_row` creates it with `next_due_at = now()`, and the very
/// first claim runs the full pass. Deterministic (the DB clock decides, not a
/// pod's), cheap (one bounded single-row UPDATE per poll — the measured
/// ~0.02 ms loser cost), cluster-singleton and fenced, and it survives restarts
/// without a second table. The alternative — probing for "any NULL watermark on
/// a partition that has live segments" every cycle — is not cheap at all: NULL
/// is the LEGITIMATE value for an empty partition, so the probe would have to
/// walk every empty partition on a clean database, forever.
///
/// NO ADVISORY LOCK, deliberately, on the sweeper's §1.9 reasoning: the belt
/// locks exist for the mixed-fleet window against old-image pods, and no old
/// image has this task at all, so there is nothing to exclude and no new number
/// is burned in the 737_00x space. Two replicas walking at once would only
/// compute the same truth twice, and the lease row already prevents that.
///
/// It also takes NO cross-batch resource: one admission slot and one pooled
/// connection PER BATCH, released between them, so a full pass interleaves with
/// the 5 s deletion cycles instead of monopolising the maintenance lane for its
/// whole length.
async fn walk_loop(pool: Pool, period_ms: u64, holder: String) {
    let period = Duration::from_millis(period_ms.max(1));
    let poll = crate::lease::poll_interval(period);
    let lease_ms = crate::lease::lease_ms(period_ms);
    while let Err(e) = crate::lease::ensure_row(&pool, WALK_TASK, period_ms).await {
        if let Some(suppressed) = WALK_ERR.tick_now() {
            tracing::error!(target: "retention", error = %e, suppressed,
                "watermark walk lease row upsert failed");
        }
        tokio::time::sleep(poll).await;
    }
    loop {
        match crate::lease::claim(&pool, WALK_TASK, lease_ms, &holder).await {
            Ok(Some(fence)) => {
                let start = Instant::now();
                match run_walk(&pool).await {
                    Ok((scanned, repaired, batches)) => {
                        let elapsed_ms = start.elapsed().as_millis() as u64;
                        // ALWAYS at INFO, unlike the deletion cycle's gated
                        // line: this runs once a day, and "the watermarks were
                        // verified and N were wrong" is the single most useful
                        // sentence in the retention log — a non-zero `repaired`
                        // on a settled cluster means a writer is not
                        // maintaining the columns (001_log_schema names them).
                        tracing::info!(
                            target: "retention",
                            scanned,
                            repaired,
                            batches,
                            elapsed_ms,
                            "watermark walk complete"
                        );
                        crate::lease::release(
                            &pool,
                            WALK_TASK,
                            fence,
                            crate::lease::Release::Advance { elapsed_ms: elapsed_ms as i32 },
                        )
                        .await;
                    }
                    Err(e) => {
                        if let Some(suppressed) = WALK_ERR.tick_now() {
                            tracing::error!(target: "retention", error = %e, suppressed,
                                "watermark walk error");
                        }
                        // Retry, NOT advance: an interrupted backfill must not
                        // wait a whole period, or retention runs a day on a
                        // work list that is missing whatever the walk had not
                        // reached yet.
                        crate::lease::release(&pool, WALK_TASK, fence, crate::lease::Release::Retry)
                            .await;
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                if let Some(suppressed) = WALK_ERR.tick_now() {
                    tracing::error!(
                        target: "retention", error = %e, suppressed, "watermark walk claim error");
                }
            }
        }
        tokio::time::sleep(poll).await;
    }
}

/// One full pass: loop the bounded walk step from the start of the id space to
/// the end. Returns (partitions scanned, watermarks repaired, batches).
async fn run_walk(
    pool: &Pool,
) -> Result<(i64, i64, u64), Box<dyn std::error::Error + Send + Sync>> {
    let mut after: Option<String> = None;
    let (mut scanned, mut repaired, mut batches) = (0i64, 0i64, 0u64);
    loop {
        // Slot BEFORE the connection, always; both released at the end of this
        // iteration so the deletion cycle can interleave (see walk_loop).
        let next = {
            let _slot = crate::admission::lane_slot(crate::admission::Lane::Maint).await;
            let client = pool.get().await?;
            let stmt = client.prepare_cached(WALK_STEP_SQL).await?;
            let row = client.query_one(&stmt, &[&after, &WALK_BATCH]).await?;
            walk_result(row.get(0))
        };
        scanned += next.scanned;
        repaired += next.repaired;
        batches += 1;
        if next.done || next.last_id.is_none() {
            break;
        }
        // A cursor that fails to advance would spin this loop forever against a
        // database that keeps answering "not done". Nothing in the step can
        // produce it (last_id is the batch's greatest id, strictly above the
        // previous bound), so treat it as a broken contract and stop loudly.
        if next.last_id == after {
            tracing::warn!(
                target: "retention",
                scanned,
                batches,
                "watermark walk cursor did not advance; stopping this pass"
            );
            break;
        }
        after = next.last_id;
        if let Some(suppressed) = WALK_PROGRESS.tick_now() {
            tracing::info!(target: "retention", scanned, repaired, batches, suppressed,
                "watermark walk running");
        }
    }
    Ok((scanned, repaired, batches))
}

/// Parsed `log_watermark_walk_step_v1` return.
struct WalkStep {
    scanned: i64,
    repaired: i64,
    last_id: Option<String>,
    done: bool,
}

/// Parse the walk step's JSONB::text. Unparseable output degrades to
/// "done, nothing scanned" — same posture as [`step_result`]: a broken contract
/// stops the loop instead of spinning it.
fn walk_result(s: String) -> WalkStep {
    let v: serde_json::Value = serde_json::from_str(&s).unwrap_or(serde_json::Value::Null);
    WalkStep {
        scanned: v.get("scanned").and_then(|x| x.as_i64()).unwrap_or(0),
        repaired: v.get("repaired").and_then(|x| x.as_i64()).unwrap_or(0),
        last_id: v.get("last_id").and_then(|x| x.as_str()).map(str::to_string),
        done: v.get("done").and_then(|x| x.as_bool()).unwrap_or(true),
    }
}

/// Parse a step SP's `{"deleted":N,...,"done":bool}` JSONB::text return.
/// Unparseable output degrades to (0, done=true) so a broken contract stops the
/// loop instead of spinning it.
fn step_result(s: String) -> (i64, bool) {
    let v: serde_json::Value = serde_json::from_str(&s).unwrap_or(serde_json::Value::Null);
    let deleted = v.get("deleted").and_then(|d| d.as_i64()).unwrap_or(0);
    let done = v.get("done").and_then(|d| d.as_bool()).unwrap_or(true);
    (deleted, done)
}

/// Trim worker/lag/parked metrics (SP) and queen.system_metrics (batched DELETE)
/// older than `metrics_retention_days`. Returns a short summary for the log line.
/// Runs as plain autocommit statements — no transaction, no savepoints (see
/// cycle_body phase 5).
async fn purge_metrics(
    client: &deadpool_postgres::Client,
    knobs: Knobs,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let worker = db::cleanup_worker_metrics(client, knobs.metrics_retention_days).await?;
    let system = db::cleanup_system_metrics(
        client,
        knobs.metrics_retention_days,
        knobs.batch_size,
    )
    .await?;
    Ok(format!("worker={} system_rows={}", worker.trim(), system))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The 2026-08-24 incident regression pin (see module docs): the belt lock
    /// must be TRANSACTION-scoped, and no session-scoped take or explicit
    /// unlock may resurface — an explicit unlock is exactly the "on every exit
    /// path" promise that a timed-out/cancelled cycle broke, leaving 737001
    /// held by an idle pooled connection for 28 minutes. Textual, like the
    /// other SQL pins here; the behavioural half (an aborted or abandoned
    /// holder transaction must free the lock for the next claimant) lives in
    /// tests/retention_lock_scope.rs against a live Postgres.
    #[test]
    fn belt_lock_is_transaction_scoped() {
        assert_eq!(LOCK_SQL, "SELECT pg_try_advisory_xact_lock($1)");
        let src = include_str!("retention.rs");
        // The lock statement must be issued on the holder transaction, so its
        // release is structurally tied to that transaction's end.
        assert!(src.contains("lock_tx.query_one(LOCK_SQL"), "lock must be taken on the holder tx");
        // The session-scoped take and the explicit unlock must stay dead. The
        // needles are concat!-split so this test's own literals don't match.
        assert!(
            !src.contains(concat!("SELECT pg_try_advisory_", "lock")),
            "session-scoped take resurfaced"
        );
        assert!(
            !src.contains(concat!("SELECT pg_advisory_", "unlock")),
            "explicit unlock resurfaced"
        );
    }

    /// The other half of the same incident (module docs, boot-DDL half): the
    /// holder transaction must run NOTHING that touches a table, because a
    /// relation lock taken inside it lives until the cycle ends and becomes a
    /// minutes-long blocker of boot DDL on queen.log_partitions. The work list
    /// was the one such statement and it moved to its own connection; this pin
    /// stops it — or anything else — from moving back. Textual, like
    /// `belt_lock_is_transaction_scoped`; the behavioural half (the holder
    /// backend appears in pg_locks with no lock on queen.log_partitions while a
    /// cycle is mid-flight) is `the_belt_lock_holder_holds_no_table_lock` below.
    #[test]
    fn the_holder_transaction_runs_nothing_but_the_lock_take() {
        let src = include_str!("retention.rs");
        let (_, after) = src.split_once("async fn run_cycle(").expect("run_cycle present");
        let (run_cycle, rest) =
            after.split_once("async fn cycle_body(").expect("cycle_body follows run_cycle");
        // Preparing a statement on the holder means running one on it.
        assert!(
            !run_cycle.contains("prepare_cached"),
            "the holder transaction prepares a statement again — its only statement is the take"
        );
        let (cycle_body, _) = rest
            .split_once("/// Which per-partition step family")
            .expect("cycle_body ends at the Phase enum");
        // The cycle body must not even be able to reach the holder transaction.
        assert!(
            !cycle_body.contains("lock_tx"),
            "cycle_body sees the holder transaction again; anything it runs there holds its \
             relation locks for the whole cycle"
        );
        // ...and the work list runs on a connection of its own, in a
        // transaction that ends with the query.
        assert!(
            cycle_body.contains("let rows = list_tx.query(&stmt, &[]).await?;"),
            "the work list must run on the list transaction"
        );
        assert!(
            cycle_body.contains("list_tx.commit().await?;"),
            "the work list's transaction must END, or its locks outlive the query anyway"
        );
    }

    /// Shape guard only — the cycle itself needs a live pool, so behavioural
    /// coverage lives in tests/retention_lock_scope.rs. What this pins down is
    /// the invariant that keeps the work list tenant-safe now that queues has
    /// absorbed the old log_queues table: partitions meet their config row BY
    /// ID (p.queue_id = q.id), never through a (name, tenant) bridge that a
    /// same-named queue of another tenant could cross.
    #[test]
    fn work_list_joins_by_id() {
        let sql = work_list_sql(5000);
        assert_eq!(sql.matches("WHERE p.queue_id = q.id").count(), 3, "one probe per phase");
        // The dead name-join must not resurface in any form.
        assert!(!sql.contains("log_queues"));
        assert!(!sql.contains("lq.name"));
    }

    /// THE regression pin for the 2026-08-24 redesign: the cycle path must
    /// never again materialize one row per partition. The old list was
    /// `queues JOIN log_partitions ORDER BY p.id` — 827k rows on the soak cell,
    /// and the query whose statement timeout deadlocked retention cluster-wide.
    /// Every partition the new list names is named by an INDEXED watermark
    /// probe, bounded by a LITERAL limit (a parametric one loses the index plan,
    /// exactly as the hot-list full reseed did).
    #[test]
    fn the_cycle_work_list_is_bounded_by_the_watermark_indexes() {
        let sql = work_list_sql(1234);
        assert!(
            !sql.contains("JOIN queen.log_partitions p ON p.queue_id = qq.id"),
            "the Theta(partitions) join resurfaced on the cycle path"
        );
        // Three probes, each ordered by the column it filters on, so progress
        // is oldest-first and the cap is a head, never a sample.
        assert!(sql.contains(
            "AND p.oldest_live_at < GREATEST(q.all_cutoff, q.completed_cutoff) \
             ORDER BY p.oldest_live_at LIMIT 1234"
        ));
        assert!(sql
            .contains("AND p.oldest_txn_at < q.txns_cutoff ORDER BY p.oldest_txn_at LIMIT 1234"));
        assert!(sql.contains(
            "AND p.oldest_live_at < q.max_wait_cutoff ORDER BY p.oldest_live_at LIMIT 1234"
        ));
        // The cap is inlined, never bound.
        assert!(!sql.contains("LIMIT $"), "a parametric LIMIT loses the index plan");
        assert_eq!(sql.matches("LIMIT 1234").count(), 3);
        // The sidecar phase must NOT be driven by the segment watermark: its
        // cutoff is far shorter, so that test nominates every partition holding
        // more than an hour of data (001_log_schema).
        assert!(!sql.contains("p.oldest_live_at < q.txns_cutoff"));
    }

    /// The rule rows are emitted ONCE per queue and the due rows carry only
    /// (pid, queue) — the four cutoffs must not be repeated per partition, which
    /// is what made the old list heavy in bytes as well as in rows.
    #[test]
    fn work_list_emits_cutoffs_once_per_queue() {
        let sql = work_list_sql(10);
        assert_eq!(sql.matches("q.all_cutoff::text").count(), 1);
        assert_eq!(sql.matches("q.txns_cutoff::text").count(), 1);
        // Three due branches, each with the four cutoff slots NULLed out.
        assert_eq!(
            sql.matches("NULL::text, NULL::text, NULL::text, NULL::text").count(),
            3
        );
    }

    /// A rule whose cutoff is NULL for a queue must emit NOTHING — that is what
    /// makes the max_wait phase free on the queues that never configured it, and
    /// it is expressed as SQL's `col < NULL` (never true), not as a Rust-side
    /// filter that a later refactor could drop.
    #[test]
    fn a_disabled_rule_emits_no_partitions() {
        let sql = work_list_sql(10);
        // The gating lives in the CASE expressions that build the cutoffs...
        assert!(sql
            .contains("CASE WHEN qq.retention_enabled AND COALESCE(qq.retention_seconds, 0) > 0"));
        assert!(sql.contains("CASE WHEN COALESCE(qq.max_wait_time_seconds, 0) > 0"));
        // ...and nothing downstream re-tests it, so a NULL cutoff is the ONLY
        // thing standing between a queue and a sweep.
        assert!(!sql.contains("all_cutoff IS NOT NULL"));
        assert!(!sql.contains("max_wait_cutoff IS NOT NULL"));
        // The txns cutoff has a 900 s floor and is therefore never NULL: the
        // phase is gated by the watermark alone.
        assert!(sql.contains("GREATEST( ") && sql.contains("qq.dedup_window_seconds"));
    }

    fn raw(phase: i32, pid: Option<&str>, queue: &str, cutoffs: [Option<&str>; 4]) -> RawRow {
        RawRow {
            phase,
            pid: pid.map(str::to_string),
            queue_id: Some(queue.to_string()),
            cutoffs: cutoffs.map(|c| c.map(str::to_string)),
        }
    }

    /// Assembly: rule rows land in the rule table once, due rows resolve to
    /// their own queue's cutoffs, and the three phases stay separate.
    #[test]
    fn work_list_assembly_resolves_each_partition_to_its_own_queue() {
        let rows = vec![
            raw(0, None, "qa", [Some("A"), None, Some("TA"), None]),
            raw(0, None, "qb", [None, Some("CB"), Some("TB"), Some("WB")]),
            raw(1, Some("p1"), "qa", [None; 4]),
            raw(1, Some("p2"), "qb", [None; 4]),
            raw(2, Some("p3"), "qb", [None; 4]),
            raw(3, Some("p4"), "qb", [None; 4]),
        ];
        let (rules, [r1, r2, r3]) = assemble_work_list(rows);
        assert_eq!(rules.len(), 2);
        assert_eq!(r1.len(), 2);
        assert_eq!(r2.len(), 1);
        assert_eq!(r3.len(), 1);
        assert_eq!(rules[r1[0].q].all_cutoff.as_deref(), Some("A"));
        assert_eq!(rules[r1[0].q].completed_cutoff, None);
        assert_eq!(rules[r1[1].q].completed_cutoff.as_deref(), Some("CB"));
        assert_eq!(rules[r3[0].q].max_wait_cutoff.as_deref(), Some("WB"));
    }

    /// A due row naming a queue with no rule row is DROPPED, never defaulted:
    /// sweeping a partition with another queue's cutoffs deletes data the
    /// operator asked to keep.
    #[test]
    fn work_list_assembly_drops_orphan_and_malformed_rows() {
        let rows = vec![
            raw(0, None, "qa", [Some("A"), None, Some("T"), None]),
            raw(1, Some("orphan"), "ghost", [None; 4]),
            raw(1, None, "qa", [None; 4]),
            // A phase tag outside 1..=3 must not index out of the array.
            raw(9, Some("p"), "qa", [None; 4]),
            raw(1, Some("good"), "qa", [None; 4]),
        ];
        let (rules, [r1, r2, r3]) = assemble_work_list(rows);
        assert_eq!(rules.len(), 1);
        assert_eq!(r1.len(), 1, "only the well-formed due row survives");
        assert_eq!(r1[0].pid, "good");
        assert!(r2.is_empty() && r3.is_empty());
    }

    fn due_list(pids: &[&str]) -> Vec<Due> {
        pids.iter().map(|p| Due { pid: p.to_string(), q: 0 }).collect()
    }

    /// The backoff skips a fruitless partition and lets it back in when the
    /// window expires — and the FIRST window is the small one, so a cursor that
    /// moves does not wait long.
    #[test]
    fn backoff_skips_a_fruitless_partition_and_expires() {
        let b = Backoff::new();
        let c1 = b.next_cycle();
        b.observe(0, c1, "p", false);
        let mut list = due_list(&["p", "q"]);
        let skipped = b.retain_due(0, c1 + 1, &mut list);
        assert_eq!(skipped, 1);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].pid, "q");
        // Still skipped one cycle before the window ends, back in on it.
        let mut list = due_list(&["p"]);
        assert_eq!(b.retain_due(0, c1 + BACKOFF_BASE_CYCLES - 1, &mut list), 1);
        let mut list = due_list(&["p"]);
        assert_eq!(b.retain_due(0, c1 + BACKOFF_BASE_CYCLES, &mut list), 0);
    }

    /// The phases have independent skip lists: a partition that cannot yield
    /// segments (rule-2 cursor cap) may still have sidecar rows to purge.
    #[test]
    fn backoff_is_per_phase() {
        let b = Backoff::new();
        let c = b.next_cycle();
        b.observe(Phase::Retention.idx(), c, "p", false);
        let mut list = due_list(&["p"]);
        assert_eq!(b.retain_due(Phase::Txns.idx(), c, &mut list), 0, "other phases unaffected");
        let mut list = due_list(&["p"]);
        assert_eq!(b.retain_due(Phase::Retention.idx(), c, &mut list), 1);
    }

    /// Consecutive fruitless visits DOUBLE the skip up to the ceiling, and any
    /// real delete clears the strikes. Without the doubling a
    /// completed-retention-only queue whose partitions nobody consumes costs
    /// `partitions / 8` step calls every cycle, forever.
    #[test]
    fn backoff_doubles_to_a_ceiling_and_resets_on_progress() {
        let b = Backoff::new();
        let mut cycle = 0u64;
        let mut seen: Vec<u64> = Vec::new();
        for _ in 0..10 {
            cycle = b.next_cycle();
            b.observe(0, cycle, "p", false);
            let skip = {
                let phases = b.phases.lock().unwrap();
                phases[0]["p"].until - cycle
            };
            seen.push(skip);
        }
        assert_eq!(seen[0], BACKOFF_BASE_CYCLES);
        assert_eq!(seen[1], BACKOFF_BASE_CYCLES * 2);
        let ceiling = BACKOFF_BASE_CYCLES << BACKOFF_MAX_SHIFT;
        assert_eq!(*seen.last().unwrap(), ceiling);
        assert_eq!(ceiling, 512, "the documented ~43 min ceiling at the 5 s default");
        // One delete wipes the record entirely.
        b.observe(0, cycle, "p", true);
        let mut list = due_list(&["p"]);
        assert_eq!(b.retain_due(0, cycle, &mut list), 0);
    }

    /// Memory bound: the map never grows past its cap, and when it is full it
    /// FAILS OPEN (the partition is visited again — the pre-backoff behaviour),
    /// never closed (which would be a skipped delete).
    #[test]
    fn backoff_is_memory_bounded_and_fails_open() {
        let b = Backoff::new();
        let c = b.next_cycle();
        for i in 0..(BACKOFF_MAX_ENTRIES + 1_000) {
            b.observe(0, c, &format!("p{i}"), false);
        }
        let len = b.phases.lock().unwrap()[0].len();
        assert!(len <= BACKOFF_MAX_ENTRIES, "cap breached: {len}");
        // The overflow partitions were simply not recorded, so they are due.
        let mut list = due_list(&[&format!("p{}", BACKOFF_MAX_ENTRIES + 500)]);
        assert_eq!(b.retain_due(0, c, &mut list), 0);
    }

    /// The walk step's contract, including the degraded parse: an answer this
    /// broker cannot read must stop the pass, never spin it.
    #[test]
    fn walk_result_parses_the_step_contract() {
        let id = "11111111-1111-1111-1111-111111111111";
        let ok = walk_result(format!(
            r#"{{"scanned":5000,"repaired":12,"last_id":"{id}","done":false}}"#
        ));
        assert_eq!((ok.scanned, ok.repaired, ok.done), (5000, 12, false));
        assert_eq!(ok.last_id.as_deref(), Some(id));
        let last =
            walk_result(r#"{"scanned":7,"repaired":0,"last_id":null,"done":true}"#.to_string());
        assert!(last.done && last.last_id.is_none());
        let broken = walk_result("not json".to_string());
        assert!(broken.done, "an unreadable answer must terminate the pass");
        assert_eq!((broken.scanned, broken.repaired), (0, 0));
    }

    /// The walk batch stays in the band its lock-hold arithmetic was written
    /// for; the due cap derivation stays in the band the measured cell needs.
    #[test]
    fn the_work_list_budgets_keep_their_measured_shape() {
        assert!((5_000..=10_000).contains(&WALK_BATCH));
        // Defaults: RETENTION_BATCH_SIZE=1000, RETENTION_PARALLELISM=1.
        let (batch, par) = (1000usize, 1usize);
        let derived = batch * par * DUE_CAP_FACTOR;
        assert!(
            derived >= 2_500,
            "the cell needs ~2500 partition visits per 5 s cycle to keep pace with \
             ~500 newly-eligible segments/s; derived cap is {derived}"
        );
        assert!(derived <= DUE_CAP_MAX);
        // 0 must never survive into the SQL as a limit of nothing.
        assert_eq!(0usize.clamp(1, DUE_CAP_MAX), 1);
    }

    /// The walk is scheduled on its OWN lease row: sharing the retention row
    /// would put a full-partition pass inside the 5 s deletion cadence, which is
    /// the cost the redesign removed.
    #[test]
    fn the_walk_has_its_own_schedule() {
        assert_ne!(WALK_TASK, TASK);
        assert_eq!(WALK_TASK, "retention_watermark");
        // Disabled means "a year out", not "never": the FIRST walk is the
        // backfill, without which the work list is empty and nothing is deleted.
        // (The relation to SAFETY_WALK_MAX_MS is pinned at compile time next to
        // the constants; what belongs here is the default's own scale.)
        assert_eq!(WALK_DISABLED_PERIOD_MS / 86_400_000, 365, "disabled = a year of days");
    }

    /// The legacy full walk must not be reachable from the cycle path under any
    /// env — it IS the query that deadlocked the cluster (module docs), so an
    /// operator lever that restores it restores the incident. The needle is
    /// concat!-split so this test and the module docs' explanation of WHY there
    /// is no such knob do not match themselves.
    #[test]
    fn there_is_no_legacy_worklist_escape_hatch() {
        let cfg = include_str!("config.rs");
        assert!(
            !cfg.contains(concat!("QUEEN_RETENTION_LEGACY", "_WORKLIST")),
            "a knob that restores the Theta(partitions) work list was added"
        );
        // The PRODUCTION half of the file only: the PG-gated tests below read
        // QUEEN_EMBEDDED_TEST_PG to find their throwaway server, which is a
        // harness address and not a knob of the cycle.
        let src = include_str!("retention.rs")
            .split_once("#[cfg(test)]")
            .expect("the test module is the last thing in this file")
            .0;
        // The cycle reads no environment of its own: every knob arrives through
        // Config, so there is nowhere for a hidden fallback to be switched on.
        assert!(!src.contains(concat!("std::env", "::var")));
        // The only remaining full-partition visit is the walk step, and it is
        // bounded per call.
        assert!(src.contains("log_watermark_walk_step_v1"));
    }

    /// The flag and the window must not be able to disagree: an enabled=false
    /// config has to produce None, because phase 4 keys purely off the Option.
    #[test]
    fn partition_cleanup_flag_gates_the_window() {
        let on = Knobs {
            metrics_retention_days: 90,
            batch_size: 1000,
            parallelism: 1,
            partition_cleanup_days: true.then_some(30),
            lock_stmt_timeout_ms: 30_000,
        };
        let off = Knobs {
            partition_cleanup_days: false.then_some(30),
            ..on
        };
        assert_eq!(on.partition_cleanup_days, Some(30));
        assert_eq!(off.partition_cleanup_days, None);
    }

    /// Each fan-out phase must drive its OWN step SP. A copy-paste here would
    /// not fail loudly: the cycle would keep reporting deletions while one rule
    /// silently never ran, which is exactly the class of bug that let log_txns
    /// grow unbounded at 1M msg/s before the parallelism landed.
    #[test]
    fn each_phase_drives_its_own_step_sp() {
        assert!(Phase::Retention.sql().contains("log_retention_step_v1"));
        assert!(Phase::Txns.sql().contains("log_txns_purge_step_v1"));
        assert!(Phase::MaxWait.sql().contains("log_evict_max_wait_step_v1"));
        // Phase 4 is serial by design and must never be reachable from the fan-out.
        for p in [Phase::Retention, Phase::Txns, Phase::MaxWait] {
            assert!(!p.sql().contains("log_partition_cleanup_step_v1"));
        }
    }

    /// The clamp floors at 1 (0 must not mean "no retention") and caps at the
    /// ceiling, which itself is pinned at compile time next to MAX_PARALLELISM.
    #[test]
    fn parallelism_ceiling_covers_the_measured_need() {
        assert_eq!(0usize.clamp(1, MAX_PARALLELISM), 1);
        assert_eq!(99usize.clamp(1, MAX_PARALLELISM), MAX_PARALLELISM);
    }

    /// Pins the guards that make the delete safe. All three tables are keyed by
    /// partition_id with NO foreign key, so a predicate that stopped consulting
    /// one of them would not fail loudly — it would silently orphan DLQ payloads
    /// or streams state, or leak the log_txns sidecar forever.
    #[test]
    fn partition_cleanup_predicate_keeps_its_vetoes() {
        let sql = include_str!("../sql/procedures/006_log_maintenance.sql");
        let (_, pred) = sql
            .split_once("CREATE OR REPLACE FUNCTION queen.log_partition_dead_v1")
            .expect("predicate function present");
        let (pred, _) = pred.split_once("$$;").expect("predicate body terminated");
        for table in [
            "queen.log_segments",
            "queen.log_dlq",
            "queen_streams.state",
            "queen.log_consumers",
        ] {
            assert!(pred.contains(table), "{table} veto missing from the predicate");
        }
        // The lease veto must stay bounded by expiry: an unbounded
        // `batch_end IS NOT NULL` pins a partition forever once a worker dies
        // mid-batch, because an empty partition never gets the pop that would
        // clear it.
        assert!(pred.contains("c.batch_end IS NOT NULL"), "live-lease veto missing");
        assert!(
            pred.contains("c.lease_expires_at IS NULL OR c.lease_expires_at > now()"),
            "live-lease veto is not bounded by lease expiry"
        );

        // The step must purge the FK-less sidecar itself, and must take its
        // batch of row locks in the one global order (ascending id) with SKIP
        // LOCKED so it never queues in front of a pusher.
        let (_, step) = sql
            .split_once("CREATE OR REPLACE FUNCTION queen.log_partition_cleanup_step_v1")
            .expect("step function present");
        assert!(step.contains("DELETE FROM queen.log_txns"), "sidecar purge missing");
        assert!(step.contains("ORDER BY p.id"), "lock order missing");
        assert!(step.contains("FOR UPDATE OF p SKIP LOCKED"), "lock mode changed");
    }

    // -- PG-gated: the holder transaction, observed in pg_locks --------------
    //
    // ```bash
    // docker run --rm -d --name queen-retlock-pg -e POSTGRES_PASSWORD=postgres \
    //   -p 5466:5432 postgres:16-alpine
    // QUEEN_EMBEDDED_TEST_PG=localhost:5466 cargo test --lib retention -- --ignored --nocapture
    // ```

    async fn pg_target() -> (String, u16) {
        let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
            .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
        target
            .split_once(':')
            .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
            .unwrap_or((target.clone(), 5432))
    }

    async fn raw_connect(host: &str, port: u16, db: &str) -> tokio_postgres::Client {
        let (c, conn) = tokio_postgres::connect(
            &format!("host={host} port={port} user=postgres password=postgres dbname={db}"),
            tokio_postgres::NoTls,
        )
        .await
        .unwrap_or_else(|e| panic!("connect to {db}: {e}"));
        tokio::spawn(async move {
            let _ = conn.await;
        });
        c
    }

    /// THE behavioural assertion for the 2026-08-24 boot-DDL half: while a
    /// cycle is mid-flight, the backend holding belt lock 737001 must hold NO
    /// lock on queen.log_partitions — or on any queen table. That is the
    /// property the schema applier's sliced waits depend on: before the fix the
    /// same backend sat `idle in transaction` on AccessShare for the length of
    /// the cycle, which on a backlog is minutes, which is a boot that never
    /// comes up.
    ///
    /// Mid-flight is ARRANGED, not raced: a monitor transaction takes ACCESS
    /// EXCLUSIVE on queen.system_metrics, which phase 5 (the metrics purge, on
    /// the cycle's tail connection) must then wait for. The cycle parks there
    /// with the holder transaction open and the belt lock held, for as long as
    /// the assertions need. The holder is identified exactly as an operator
    /// would: it is the backend holding advisory lock 737001 in this database.
    ///
    /// Its own scratch DATABASE, because it applies the real schema and runs
    /// the real cycle; the other embedded suites share `postgres` and this one
    /// must not have its pg_locks reading polluted by theirs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
    async fn the_belt_lock_holder_holds_no_table_lock() {
        let (host, port) = pg_target().await;
        let admin = raw_connect(&host, port, "postgres").await;
        for row in admin
            .query("SELECT datname FROM pg_database WHERE datname LIKE 'qret\\_hold\\_%'", &[])
            .await
            .expect("list leftovers")
        {
            let old: String = row.get(0);
            let _ = admin
                .execute(&format!("DROP DATABASE IF EXISTS \"{old}\" WITH (FORCE)"), &[])
                .await;
        }
        let db = format!(
            "qret_hold_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        admin.execute(&format!("CREATE DATABASE \"{db}\""), &[]).await.expect("create database");

        let mut dp = deadpool_postgres::Config::new();
        dp.host = Some(host.clone());
        dp.port = Some(port);
        dp.user = Some("postgres".into());
        dp.password = Some("postgres".into());
        dp.dbname = Some(db.clone());
        dp.pool = Some(deadpool_postgres::PoolConfig::new(8));
        let pool = dp
            .create_pool(Some(deadpool_postgres::Runtime::Tokio1), tokio_postgres::NoTls)
            .expect("pool");
        crate::schema::apply(&pool).await.expect("schema apply");

        // A due partition, so the cycle really has a work list to build and
        // phases to run — the assertion is about the holder, but a cycle that
        // found nothing to do would prove less.
        let seeder = raw_connect(&host, port, &db).await;
        let qid: String = seeder
            .query_one(
                "INSERT INTO queen.queues (name, retention_enabled, retention_seconds) \
                 VALUES ('hold', true, 1) RETURNING id::text",
                &[],
            )
            .await
            .expect("seed queue")
            .get(0);
        seeder
            .execute(
                "INSERT INTO queen.log_partitions (queue_id, name, oldest_live_at) \
                 VALUES ($1::text::uuid, 'p1', now() - interval '1 hour')",
                &[&qid],
            )
            .await
            .expect("seed partition");

        // Park the cycle in phase 5.
        let monitor = raw_connect(&host, port, &db).await;
        monitor.batch_execute("BEGIN").await.expect("begin");
        monitor
            .batch_execute("LOCK TABLE queen.system_metrics IN ACCESS EXCLUSIVE MODE")
            .await
            .expect("park lock");

        let knobs = Knobs {
            metrics_retention_days: 90,
            batch_size: 1000,
            parallelism: 1,
            partition_cleanup_days: Some(30),
            lock_stmt_timeout_ms: 30_000,
        };
        let work_list = work_list_sql(5_000);
        let cycle_pool = pool.clone();
        // sweep_partitions = false: phase 4 is skipped, so the cycle reaches the
        // parked phase 5 promptly and the window opens.
        let cycle =
            tokio::spawn(async move { run_cycle(&cycle_pool, knobs, &work_list, false).await });

        // Wait for the parked state: the belt lock is held by SOME backend of
        // this database, and something is queued behind our ACCESS EXCLUSIVE.
        let mut holder: Option<i32> = None;
        for _ in 0..300 {
            let row = monitor
                .query_opt(
                    "SELECT l.pid FROM pg_locks l \
                     WHERE l.locktype = 'advisory' AND l.granted AND l.objid = 737001 \
                       AND l.database = (SELECT oid FROM pg_database \
                                         WHERE datname = current_database())",
                    &[],
                )
                .await
                .expect("advisory probe");
            let waiting: i64 = monitor
                .query_one(
                    "SELECT count(*) FROM pg_locks \
                     WHERE relation = 'queen.system_metrics'::regclass AND NOT granted",
                    &[],
                )
                .await
                .expect("wait probe")
                .get(0);
            if let (Some(r), true) = (row, waiting > 0) {
                holder = Some(r.get(0));
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let holder = holder.expect("no parked cycle appeared: belt lock holder + phase-5 waiter");

        // THE assertion: the holder's transaction is open (it holds 737001) and
        // it holds no relation lock on anything of ours.
        let held: String = monitor
            .query_one(
                "SELECT COALESCE(string_agg(DISTINCT n.nspname || '.' || c.relname || ' ' \
                                            || l.mode, ', '), '') \
                 FROM pg_locks l \
                 JOIN pg_class c ON c.oid = l.relation \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE l.pid = $1 AND n.nspname LIKE 'queen%'",
                &[&holder],
            )
            .await
            .expect("holder lock probe")
            .get(0);
        assert_eq!(
            held, "",
            "the belt-lock holder (pid {holder}) is holding relation locks for the length of \
             the cycle — that is the blocker of boot DDL the work list was moved out to remove"
        );

        // Release the park and let the cycle finish.
        monitor.batch_execute("ROLLBACK").await.expect("rollback");
        let outcome = tokio::time::timeout(Duration::from_secs(60), cycle)
            .await
            .expect("the cycle must finish once phase 5 is unblocked")
            .expect("join")
            .expect("cycle error");
        assert!(matches!(outcome, Outcome::Ran { .. }), "the cycle must have run, not skipped");
        // The belt lock died with the holder transaction.
        let still: i64 = monitor
            .query_one(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND objid = 737001 \
                 AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
                &[],
            )
            .await
            .expect("belt probe")
            .get(0);
        assert_eq!(still, 0, "the belt lock outlived the cycle");

        drop(seeder);
        drop(monitor);
        drop(pool);
        admin
            .execute(&format!("DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)"), &[])
            .await
            .expect("drop scratch database");
    }
}
