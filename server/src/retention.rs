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
//! stays serial on the cycle's own connection.
//!
//! The advisory lock is now SESSION-scoped (`pg_try_advisory_lock`) because
//! there is no cycle transaction to scope it to; it is explicitly released with
//! `pg_advisory_unlock` on every exit path. If the connection dies mid-cycle the
//! backend session dies with it and PostgreSQL releases the lock anyway (and
//! deadpool recycles the broken connection), so the lock cannot leak. Since the
//! claim-row scheduler the lock is BELT, not scheduler: the lease row decides
//! who sweeps each period, and the lock only excludes overlap against
//! old-image pods (mixed-fleet window) so replicas never double-delete.
//!
//! A failing cycle is logged and swallowed so the loop survives — a transient DB
//! error must not kill background maintenance.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;

use crate::config::Config;
use crate::db;

/// Advisory lock shared with the C++ retention/eviction services
/// (server/src/services/retention_service.cpp:73), now session-level (see module
/// docs). A replica that can't acquire it skips the cycle.
const CLEANUP_LOCK_ID: i64 = 737_001;

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

/// One consistent `now()` per cycle: the work-list query computes every cutoff
/// as `timestamptz::text` server-side (all columns of one statement share one
/// `now()`), and the step calls cast the text back. A NULL column = that rule is
/// disabled for the queue (the `retention_seconds > 0` gating inherited from
/// the retired seg engine's retention sweep, decided here, not in SQL —
/// 006_log_maintenance header).
///
/// Queue identity is now the queen.queues id: log_partitions.queue_id
/// references queen.queues(id) directly, so partitions join their config row
/// BY ID — there is no name+tenant bridge left to get wrong, and one tenant's
/// cutoffs can never be emitted against another tenant's partitions (which the
/// step calls below execute as deletes). Every queue is a log queue, so there
/// is no engine filter either.
const WORK_LIST_SQL: &str = "\
    SELECT p.id::text, \
           qq.id::text, \
           CASE WHEN qq.retention_enabled AND COALESCE(qq.retention_seconds, 0) > 0 \
                THEN (now() - make_interval(secs => qq.retention_seconds))::text END, \
           CASE WHEN qq.retention_enabled AND COALESCE(qq.completed_retention_seconds, 0) > 0 \
                THEN (now() - make_interval(secs => qq.completed_retention_seconds))::text END, \
           (now() - make_interval(secs => GREATEST(qq.dedup_window_seconds, \
                                                   COALESCE(qq.completed_retention_seconds, 0), \
                                                   900)))::text, \
           CASE WHEN COALESCE(qq.max_wait_time_seconds, 0) > 0 \
                THEN (now() - make_interval(secs => qq.max_wait_time_seconds))::text END \
    FROM queen.queues qq \
    JOIN queen.log_partitions p ON p.queue_id = qq.id \
    ORDER BY p.id";

/// Launch the retention/eviction background loop. Non-blocking: spawns a detached
/// tokio task and returns immediately. Call once at boot, before `axum::serve`.
pub fn spawn(pool: Pool, cfg: &Config) {
    let interval = Duration::from_millis(cfg.retention_interval_ms);
    let knobs = Knobs {
        metrics_retention_days: cfg.metrics_retention_days,
        batch_size: cfg.retention_batch_size,
        parallelism: cfg.retention_parallelism.clamp(1, MAX_PARALLELISM),
        partition_cleanup_days: cfg.partition_cleanup_enabled.then_some(cfg.partition_cleanup_days),
    };
    tracing::info!(
        target: "retention",
        interval_ms = cfg.retention_interval_ms,
        advisory_lock = CLEANUP_LOCK_ID,
        batch_size = knobs.batch_size,
        parallelism = knobs.parallelism,
        metrics_retention_days = knobs.metrics_retention_days,
        // None = QUEEN_PARTITION_CLEANUP_ENABLED=false, i.e. phase 4 never runs.
        partition_cleanup_days = ?knobs.partition_cleanup_days,
        "service started"
    );
    let holder = crate::lease::holder_id(cfg);
    tokio::spawn(async move { run_loop(pool, interval, knobs, holder).await });
}

/// Lease-table task name for the whole maintenance cycle
/// (029_maintenance_leases). One row for the cycle, not one per phase: the
/// phases share a work list, one consistent now(), and the session belt lock,
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
}

impl Knobs {
    /// `p_max_rows` for the step SPs (their INT arg).
    fn max_rows(&self) -> i32 {
        self.batch_size.min(i32::MAX as usize) as i32
    }
}

async fn run_loop(pool: Pool, period: Duration, knobs: Knobs, holder: String) {
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
        match run_cycle(&pool, knobs, sweep_partitions).await {
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
                if segments_deleted > 0 || txns_purged > 0 || max_wait > 0 || partitions_deleted > 0
                {
                    tracing::info!(
                        target: "retention",
                        queues,
                        segments_deleted,
                        txns_purged,
                        max_wait_evicted = max_wait,
                        partitions_deleted,
                        metrics_purge = %metrics.trim(),
                        elapsed_ms,
                        "swept"
                    );
                } else {
                    tracing::debug!(target: "retention", queues, elapsed_ms, "idle cycle");
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

/// One maintenance cycle on a dedicated connection: try the SESSION advisory
/// lock, run the phases (each an autocommitting SP call — no wrapping
/// transaction), then release the lock on EVERY exit path. Autocommit statements
/// never leave the connection in an aborted-transaction state, so the connection
/// goes back to the pool clean even after a mid-cycle error.
async fn run_cycle(
    pool: &Pool,
    knobs: Knobs,
    sweep_partitions: bool,
) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    // Maintenance-lane admission for the leader's OWN database work: the
    // advisory lock and the work-list query. The individual step commits do not
    // feed the train sensor — a held slot without commit_done contributes
    // accounting, never fake samples.
    let slot = crate::admission::lane_slot(crate::admission::Lane::Maint).await;
    let client = pool.get().await?;
    let got: bool = client
        .query_one("SELECT pg_try_advisory_lock($1)", &[&CLEANUP_LOCK_ID])
        .await?
        .get(0);
    if !got {
        return Ok(Outcome::Skipped);
    }
    // Hand the slot to cycle_body, which releases it before the fan-out and
    // takes a fresh one for the phases that DO run on this connection (4 and 5).
    // Holding it across the fan-out would spend a maintenance slot on a task
    // that is only awaiting its workers — at the lane's decayed cap of 2 that
    // was HALF the lane, and it made the fan-out exactly as slow as the serial
    // cycle it replaced (measured 2026-08-10).
    let res = cycle_body(pool, &client, slot, knobs, sweep_partitions).await;
    // Session lock: MUST unlock before the connection returns to the pool, or a
    // healthy pooled session would keep leader-gating every other cycle forever.
    // If this fails the connection itself is almost certainly broken — the
    // backend session then dies and PostgreSQL releases the lock with it (see
    // module docs), so log loudly but don't turn a successful cycle into an error.
    match client.query_one("SELECT pg_advisory_unlock($1)", &[&CLEANUP_LOCK_ID]).await {
        Ok(row) => {
            let released: bool = row.get(0);
            if !released {
                tracing::warn!(
                    target: "retention",
                    advisory_lock = CLEANUP_LOCK_ID,
                    "pg_advisory_unlock reported not-held (lock-protocol invariant broken)"
                );
            }
        }
        Err(e) => tracing::warn!(
            target: "retention",
            error = %e,
            "advisory unlock failed; lock releases with the session if the connection is recycled"
        ),
    }
    res
}

async fn cycle_body(
    pool: &Pool,
    client: &deadpool_postgres::Client,
    leader_slot: Option<crate::admission::Slot>,
    knobs: Knobs,
    sweep_partitions: bool,
) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
    // Work list, ONCE per cycle: every partition of every log ('segments')
    // queue, with the per-queue cutoffs precomputed from one now() (see
    // WORK_LIST_SQL). Which phases apply per row is encoded by NULL-ness.
    let stmt = client.prepare_cached(WORK_LIST_SQL).await?;
    let rows = client.query(&stmt, &[]).await?;
    let work: Vec<WorkItem> = rows
        .iter()
        .map(|r| WorkItem {
            pid: r.get(0),
            queue_id: r.get(1),
            all_cutoff: r.get(2),
            completed_cutoff: r.get(3),
            txns_cutoff: r.get(4),
            max_wait_cutoff: r.get(5),
        })
        .collect();
    let max_rows = knobs.max_rows();

    // Phase 1: retention rules 1+2 (queues gated as in the retired seg
    // engine's retention sweep: retention_enabled
    // AND a positive window — encoded as at least one non-NULL cutoff). The
    // swept-queue count is keyed by queues.id, not name: names repeat across
    // tenants, so a name-keyed set would collapse distinct queues into one.
    // Phases 1-3 are per-partition and independent, so each runs as a FAN-OUT
    // over `knobs.parallelism` workers (see run_phase). The phases still run in
    // order relative to each other — only the visit order WITHIN a phase stops
    // being ascending-by-id, which nothing depends on (module docs).
    let work = Arc::new(work);
    let par = knobs.parallelism;
    // The leader does no database work until phase 4: release its admission
    // slot so the fan-out workers can have it (see run_cycle).
    drop(leader_slot);

    // Phase 1: retention rules 1+2, gated as in the retired seg engine's sweep
    // (retention_enabled AND a positive window — encoded as at least one
    // non-NULL cutoff). The swept-queue count is keyed by queues.id, not name:
    // names repeat across tenants, so a name-keyed set would collapse distinct
    // queues into one.
    let (segments_deleted, touched) = run_phase(pool, Phase::Retention, &work, par, max_rows).await?;
    let retention_queues: std::collections::HashSet<String> = touched.into_iter().collect();

    // Phase 2: log_txns hash-sidecar purge — EVERY log partition (the 900s floor
    // in the cutoff makes the window always applicable; 001_log_schema header).
    let (txns_purged, _) = run_phase(pool, Phase::Txns, &work, par, max_rows).await?;

    // Phase 3: max_wait_time_seconds eviction — applies regardless of
    // retention_enabled (a queue configured with ONLY maxWaitTimeSeconds still
    // gets swept), matching the old db::seg_evict_max_wait / C++ EvictionService.
    let (max_wait, _) = run_phase(pool, Phase::MaxWait, &work, par, max_rows).await?;

    // Phase 4: delete EMPTY, long-inactive partitions —
    // queen.log_partition_cleanup_step_v1, the restored C++
    // cleanup_inactive_partitions (PARTITION_CLEANUP_DAYS). Runs LAST of the data
    // phases on purpose: a partition that phases 1-3 just emptied becomes
    // eligible in the same cycle instead of waiting for the next one.
    //
    // Not driven by `work` — the step selects and locks its own batch, so the
    // per-partition list would only add a round trip. Its own sub-cadence
    // (PARTITION_SWEEP_EVERY) and its off switch are folded into
    // knobs.partition_cleanup_days.
    // Phases 4 and 5 run on THIS connection again, so take a slot back.
    let _slot = crate::admission::lane_slot(crate::admission::Lane::Maint).await;
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
    let metrics = match purge_metrics(client, knobs).await {
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

/// Drive one phase across the whole work list with `parallelism` workers.
///
/// The split is a shared CURSOR, not a static chunking: partition backlogs are
/// wildly uneven (one partition can hold a million rows while its neighbour
/// holds none), and a static split would leave workers idle behind whoever drew
/// the deep ones.
///
/// Returns (rows deleted by the phase, queue ids touched — only phase 1
/// populates it, for the swept-queue count).
async fn run_phase(
    pool: &Pool,
    phase: Phase,
    work: &Arc<Vec<WorkItem>>,
    parallelism: usize,
    max_rows: i32,
) -> Result<(i64, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
    if work.is_empty() {
        return Ok((0, Vec::new()));
    }
    let n = parallelism.clamp(1, work.len());
    let cursor = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let pool = pool.clone();
        let work = Arc::clone(work);
        let cursor = Arc::clone(&cursor);
        handles.push(tokio::spawn(async move {
            phase_worker(pool, phase, work, cursor, max_rows).await
        }));
    }
    // Join ALL workers before reporting an error: a worker that fails must not
    // leave its siblings running against a connection the cycle has abandoned.
    // The cycle then fails as it always did (run_loop logs and retries next
    // tick), so a partial sweep is retried, never counted.
    let mut deleted = 0i64;
    let mut queues: Vec<String> = Vec::new();
    let mut first_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;
    for h in handles {
        match h.await {
            Ok(Ok((d, q))) => {
                deleted += d;
                queues.extend(q);
            }
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
        None => Ok((deleted, queues)),
    }
}

/// One fan-out worker: its own admission slot, its own pooled connection, its
/// own prepared statement, pulling partitions off the shared cursor until the
/// work list is exhausted.
async fn phase_worker(
    pool: Pool,
    phase: Phase,
    work: Arc<Vec<WorkItem>>,
    cursor: Arc<AtomicUsize>,
    max_rows: i32,
) -> Result<(i64, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
    let _slot = crate::admission::lane_slot(crate::admission::Lane::Maint).await;
    let client = pool.get().await?;
    let stmt = client.prepare_cached(phase.sql()).await?;
    let mut deleted_total: i64 = 0;
    let mut queues: Vec<String> = Vec::new();
    loop {
        let Some(w) = work.get(cursor.fetch_add(1, Ordering::Relaxed)) else {
            break;
        };
        // The step contract (006_log_maintenance) is done:true whenever nothing
        // was deleted, so `!done` implies progress; the deleted==0 arm is a
        // defensive stop against a contract break looping us forever.
        match phase {
            Phase::Retention => {
                if w.all_cutoff.is_none() && w.completed_cutoff.is_none() {
                    continue;
                }
                queues.push(w.queue_id.clone());
                loop {
                    let row = client
                        .query_one(&stmt, &[&w.pid, &w.all_cutoff, &w.completed_cutoff, &max_rows])
                        .await?;
                    let (deleted, done) = step_result(row.get(0));
                    deleted_total += deleted;
                    if done || deleted == 0 {
                        break;
                    }
                }
            }
            Phase::Txns => loop {
                let row = client
                    .query_one(&stmt, &[&w.pid, &w.txns_cutoff, &max_rows])
                    .await?;
                let (deleted, done) = step_result(row.get(0));
                deleted_total += deleted;
                if done || deleted == 0 {
                    break;
                }
            },
            Phase::MaxWait => {
                let Some(cutoff) = &w.max_wait_cutoff else {
                    continue;
                };
                loop {
                    let row = client.query_one(&stmt, &[&w.pid, cutoff, &max_rows]).await?;
                    let (deleted, done) = step_result(row.get(0));
                    deleted_total += deleted;
                    if done || deleted == 0 {
                        break;
                    }
                }
            }
        }
    }
    Ok((deleted_total, queues))
}

/// One partition's row in the cycle work list. Cutoffs are prerendered
/// `timestamptz::text` values (None = rule disabled for this queue).
struct WorkItem {
    pid: String,
    queue_id: String,
    all_cutoff: Option<String>,
    completed_cutoff: Option<String>,
    txns_cutoff: String,
    max_wait_cutoff: Option<String>,
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

    /// Shape guard only — the cycle itself needs a live pool, so behavioural
    /// coverage lives in the two-tenant isolation smoke. What this pins down is
    /// the invariant that keeps the work list tenant-safe now that queues has
    /// absorbed the old log_queues table: partitions join their config row BY
    /// ID (p.queue_id = qq.id), never through a (name, tenant) bridge that a
    /// same-named queue of another tenant could cross.
    #[test]
    fn work_list_joins_by_id() {
        assert!(WORK_LIST_SQL.contains("JOIN queen.log_partitions p ON p.queue_id = qq.id"));
        // The dead name-join must not resurface in any form.
        assert!(!WORK_LIST_SQL.contains("log_queues"));
        assert!(!WORK_LIST_SQL.contains("lq.name"));
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
}
