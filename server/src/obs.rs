//! Observability foundation (LOGGING_PLAN.md, Phase 0 + Phase 1).
//!
//! The broker had no logging framework: 86 raw `println!/eprintln!`, no
//! timestamps, no levels, and the Helm-injected `LOG_LEVEL` was never read. This
//! module installs a `tracing` subscriber whose `EnvFilter` finally honours
//! `RUST_LOG`/`LOG_LEVEL`, a panic hook that turns a silent task death into a
//! structured ERROR (see the `panic = "abort"` note below), a single load-safe
//! sampling primitive (`Sampler`, generalising `ack_registry::maybe_report`), and
//! the periodic `rates` / `sizes` aggregate reporters.
//!
//! ## Line shape
//! `<RFC3339>  <LEVEL>  <target>  <message>  key=val …` — every line carries a
//! broker-generated UTC timestamp and a subsystem `target`, and (by the WHERE
//! rule) at least one of queue/partition/group/worker/peer/offset.
//!
//! ## panic = "abort"
//! `Cargo.toml` sets `panic = "abort"` for release, so a panic in any of the ~8
//! detached background loops aborts the WHOLE process (it does not "keep serving
//! minus one subsystem" — that is only the dev/unwind build). In-process
//! catch-and-restart therefore cannot work under the production profile; the
//! correct, mode-independent fix is a panic HOOK that emits a structured ERROR
//! (the std hook runs before the abort). k8s then restarts the pod and
//! `file_buffer::startup_recovery` drains any spooled data.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;

use crate::ack_registry::AckRegistry;
use crate::dedup::DedupCache;
use crate::file_buffer::FileBufferManager;
use crate::hotlist::HotList;
use crate::metrics::Metrics;
use std::sync::Arc;

/// Install the process-wide `tracing` subscriber. Call ONCE, first thing in
/// `main`, before any `info!/warn!/error!`. Verbosity is resolved from
/// `RUST_LOG`, else the Helm-injected `LOG_LEVEL`, else `info`; both accept the
/// full `EnvFilter` syntax (`info,queen::pop=debug`). `QUEEN_LOG_JSON=1`
/// switches to a one-object-per-line JSON formatter for structured shippers.
///
/// Binary-only (`server` feature): the library must never install a global
/// subscriber — the embedding application owns tracing.
#[cfg(feature = "server")]
pub fn init() {
    use tracing_subscriber::fmt::time::UtcTime;
    use tracing_subscriber::EnvFilter;

    let directive = std::env::var("RUST_LOG")
        .ok()
        .or_else(|| std::env::var("LOG_LEVEL").ok())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "info".to_string());
    let env_filter = EnvFilter::try_new(&directive).unwrap_or_else(|_| EnvFilter::new("info"));

    // Same boolean spellings as every other knob. This runs BEFORE the subscriber
    // exists, so an unparseable value cannot be reported here — it falls back to
    // text and `config::load()` (which validates QUEEN_LOG_JSON eagerly, moments
    // later) fails the boot with the real message.
    let json = std::env::var("QUEEN_LOG_JSON")
        .ok()
        .and_then(|v| crate::config::parse_bool(&v))
        .unwrap_or(false);
    let builder = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_timer(UtcTime::rfc_3339())
        .with_target(true);
    if json {
        builder.json().flatten_event(true).init();
    } else {
        // ANSI off: broker logs are almost always captured by a container/journal
        // driver where escape codes are noise.
        builder.with_ansi(false).init();
    }
}

/// Turn a silent background-task / request panic into a structured ERROR before
/// the process aborts (see the module note on `panic = "abort"`). Chains the
/// previous hook so backtraces still print.
pub fn install_panic_hook() {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let location = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let msg = info
            .payload()
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<non-string panic payload>".to_string());
        let thread = std::thread::current()
            .name()
            .unwrap_or("unnamed")
            .to_string();
        tracing::error!(
            target: "panic",
            location = %location,
            thread = %thread,
            "task panicked (process will abort under panic=abort): {msg}"
        );
        prev(info);
    }));
}

/// Structured fatal: log the reason at ERROR, then exit(1). Replaces the scattered
/// `eprintln!("FATAL: …"); process::exit(1)` boot aborts so a fatal is greppable
/// by level and carries a timestamp.
pub fn fatal(reason: impl std::fmt::Display) -> ! {
    tracing::error!(target: "boot", "FATAL: {reason}");
    std::process::exit(1);
}

/// Load-safe sampling primitive — the ONE sanctioned way to log from a
/// per-request / per-message path. A wall-clock time-window gate (generalising
/// `ack_registry::maybe_report`): at most one emit per `interval_ms` per instance,
/// process-wide, chosen by a CAS so exactly one thread wins. Returns the number of
/// events suppressed since the last emit so the caller can print `suppressed=N`.
///
/// ```ignore
/// static ENC_WARN: Sampler = Sampler::new(10_000);
/// if let Some(suppressed) = ENC_WARN.tick_now() {
///     warn!(target: "push", queue = %q, suppressed, "encryption failed; stored plaintext");
/// }
/// ```
pub struct Sampler {
    last_ms: AtomicI64,
    interval_ms: i64,
    suppressed: AtomicU64,
}

impl Sampler {
    pub const fn new(interval_ms: i64) -> Self {
        Sampler {
            last_ms: AtomicI64::new(0),
            interval_ms,
            suppressed: AtomicU64::new(0),
        }
    }

    /// `Some(suppressed_since_last)` when it is time to emit (and this thread won
    /// the slot); `None` otherwise, having counted this call as suppressed.
    pub fn tick(&self, now_ms: i64) -> Option<u64> {
        let prev = self.last_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(prev) < self.interval_ms {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        if self
            .last_ms
            .compare_exchange(prev, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        Some(self.suppressed.swap(0, Ordering::Relaxed))
    }

    /// `tick` using the process clock.
    pub fn tick_now(&self) -> Option<u64> {
        self.tick(crate::util::now_epoch_ms())
    }
}

/// On-change gate: the counterpart to `Sampler` for signals that must be logged
/// when they FLIP, not on a cadence (PLAN_KV_TIMERS.md §14.4).
///
/// The house precedent is the spool's enter/leave-buffered-mode transition in the
/// reporter below — "the genuinely important signal is logged when it changes, not
/// every interval". The sweeper needs the same shape for the degradation ladder of
/// §12.1: a stage that has been active for an hour must not write a line an hour
/// long, and entering it must not wait for the next reporter tick.
///
/// ```ignore
/// static KV_PRUNE_SHED: OnChange<bool> = OnChange::new();
/// if let Some(prev) = KV_PRUNE_SHED.changed(shed) { … }
/// ```
pub struct OnChange<T> {
    last: std::sync::Mutex<Option<T>>,
}

impl<T: PartialEq + Clone> OnChange<T> {
    pub const fn new() -> Self {
        OnChange {
            last: std::sync::Mutex::new(None),
        }
    }

    /// `Some(previous)` when the value differs from the last observation (the first
    /// observation ever reports `Some(None)`), `None` when it is unchanged.
    pub fn changed(&self, v: T) -> Option<Option<T>> {
        let mut g = match self.last.lock() {
            Ok(g) => g,
            // A poisoned gate must not silence an operational signal: fall back to
            // "report it" rather than swallowing a stage transition.
            Err(p) => p.into_inner(),
        };
        if g.as_ref() == Some(&v) {
            return None;
        }
        Some(std::mem::replace(&mut *g, Some(v)))
    }
}

impl<T: PartialEq + Clone> Default for OnChange<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// One line for one rung of the sweeper's degradation ladder (§12.1), emitted only
/// on entry and exit. `stage` is the rung's name (`kv_prune`, `usage_rollup`,
/// `kv_standalone_writes`, `kv_quota`, `kv_kill_switch`, `timers_schedule`), and the
/// level is deliberately asymmetric: shedding is a WARN because the cell has started
/// giving something up, recovering is an INFO.
///
/// What must NEVER appear here is the fire: it is not shed automatically at any rung
/// (§12.1). Under pressure the sweeper shrinks the batch and lengthens the sleep, and
/// the visible result is `fire_lag` rising — turning the fire off would convert a
/// delay into what the customer reads as message loss.
pub fn sweeper_stage(gate: &OnChange<bool>, stage: &'static str, shed: bool, reason: &str) {
    match gate.changed(shed) {
        // Unchanged: the whole point of the gate.
        None => return,
        // FIRST observation, and nothing is shed. Not a transition — the cell has
        // simply started healthy — and reporting it writes one "degradation stage
        // LEFT" per rung at every boot, for rungs nobody has ever entered. A log
        // that announces the absence of a problem at startup is a log whose real
        // announcements get skimmed past.
        Some(None) if !shed => return,
        _ => {}
    }
    if shed {
        tracing::warn!(target: "sweeper", stage, reason = %reason, "degradation stage ENTERED");
    } else {
        tracing::info!(target: "sweeper", stage, reason = %reason, "degradation stage LEFT");
    }
}

/// Resolve when either SIGTERM (k8s pod termination) or Ctrl-C arrives, logging
/// the signal. Fed to axum's `with_graceful_shutdown` so in-flight requests
/// finish and the file-buffer spool can be reported before exit.
pub async fn shutdown_signal() {
    use tokio::signal;
    let ctrl_c = async {
        let _ = signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    tracing::info!(target: "shutdown", "signal received, draining in-flight requests");
}

// ---------------------------------------------------------------------------
// Phase 1: the `rates` and `sizes` periodic aggregate reporters.
// One task, one line per block per interval (global) + top-N hot queues. All
// values come from already-collected process state (metrics/pool/registry/…),
// so this adds no per-request cost — it is the load-safe way to put throughput,
// latency, hit-rate AND cache/memory sizes into the log, next to Prometheus.
// ---------------------------------------------------------------------------

/// Handles the reporter reads from. All are cheap `Arc` clones / a `Pool` handle.
pub struct ReporterHandles {
    pub metrics: Arc<Metrics>,
    pub pool: Pool,
    pub ack_registry: Arc<AckRegistry>,
    pub file_buffer: Arc<FileBufferManager>,
    pub hotlist: Arc<HotList>,
    pub dedup: Arc<DedupCache>,
    pub dedup_cap_mb: usize,
    pub admission: Arc<crate::admission::Admission>,
    pub interval_ms: u64,
    pub top_n: usize,
}

pub fn spawn_reporter(h: ReporterHandles) {
    tokio::spawn(async move {
        let interval = Duration::from_millis(h.interval_ms.max(1000));
        let mut prev = h.metrics.snapshot();
        let mut prev_q = h.metrics.per_queue.snapshot();
        let (mut prev_hits, mut prev_misses) = h.ack_registry.hit_stats();
        let mut prev_empty = h.metrics.pop.empty.load(Ordering::Relaxed);
        // Track the spool's DB-health so we can emit the enter/leave-buffered-mode
        // transition (the single most important durability signal) on-change.
        let mut prev_healthy = h.file_buffer.db_healthy();
        // PLAN_KV_TIMERS.md §14.4: the kv/timers rate fields are deltas over the same
        // window as everything else in this block, so they need their own previous
        // marks. Read unconditionally (three relaxed loads); only the LINE is gated.
        let mut prev_kv_ops = h.metrics.kvt.kv_ops_total();
        let mut prev_kv_rej = h.metrics.kvt.kv_rejected_total();
        let mut prev_fired = h.metrics.kvt.timers_fired_total();
        let (mut prev_visits, mut prev_cands, _, _) = h.hotlist.lap.snapshot();
        static POOL_SAT: Sampler = Sampler::new(10_000);
        let mut last = Instant::now();
        loop {
            tokio::time::sleep(interval).await;
            let secs = last.elapsed().as_secs_f64().max(0.001);
            last = Instant::now();

            // ---- rates (global) ----
            let cur = h.metrics.snapshot();
            let d = |now: u64, was: u64| (now.saturating_sub(was) as f64) / secs;
            let (hits, misses) = h.ack_registry.hit_stats();
            let win_hits = hits.saturating_sub(prev_hits);
            let win_miss = misses.saturating_sub(prev_misses);
            let ack_hit = {
                let tot = win_hits + win_miss;
                if tot > 0 {
                    100.0 * win_hits as f64 / tot as f64
                } else {
                    0.0
                }
            };
            let cur_empty = h.metrics.pop.empty.load(Ordering::Relaxed);
            let pop_reqs = cur.pop_requests.saturating_sub(prev.pop_requests);
            let pop_empty_pct = if pop_reqs > 0 {
                100.0 * cur_empty.saturating_sub(prev_empty) as f64 / pop_reqs as f64
            } else {
                0.0
            };
            let parked: i64 = h.metrics.parked.live().iter().map(|(_, v)| *v).sum();
            let st = h.pool.status();
            let buffered = !h.file_buffer.db_healthy();

            // Pool saturation (LOGGING_PLAN.md HA/pool gap): connections queueing on
            // the DB pool is a top backpressure signal and was invisible before.
            if st.waiting > 0 {
                if let Some(suppressed) = POOL_SAT.tick(crate::util::now_epoch_ms()) {
                    tracing::warn!(
                        target: "pool",
                        waiting = st.waiting,
                        size = st.size,
                        max = st.max_size,
                        suppressed,
                        "connection pool saturated — requests are queueing on the DB pool"
                    );
                }
            }
            // Durability: enter/leave the DB-down buffered spool mode, on-change —
            // the single most important operational signal (was unlogged).
            let healthy = !buffered;
            if healthy != prev_healthy {
                if healthy {
                    tracing::info!(target: "spool", "DB recovered — leaving buffered mode");
                } else {
                    tracing::warn!(
                        target: "spool",
                        pending = h.file_buffer.pending_count(),
                        "DB down — entering buffered mode (degraded durability)"
                    );
                }
                prev_healthy = healthy;
            }

            let adm = h.admission.snapshot();
            let (lap_visits, lap_cands, age_p50, age_p95) = h.hotlist.lap.snapshot();
            let (ring_oldest, ring_depth) = h.hotlist.ready_probe();
            let d_visits = lap_visits.saturating_sub(prev_visits);
            let d_cands = lap_cands.saturating_sub(prev_cands);
            prev_visits = lap_visits;
            prev_cands = lap_cands;
            // PLAN_KV_TIMERS.md §14.4: NO new periodic target. A third block is a line
            // nobody reads; `rates` and `sizes` are the two people actually open during
            // an incident, so the kv/timers numbers ride them.
            //
            // The six kv/timers fields used to be appended through a macro, so that a
            // cell with the boot flags off emitted the line WITHOUT them. The flags are
            // gone, every cell has both surfaces, and the fields are now permanent
            // columns of `broker rates` — one field list, always the same shape, which
            // is what a log parser wanted in the first place.
            let kvt = &h.metrics.kvt;
            let kv_ops = kvt.kv_ops_total();
            let kv_rej = kvt.kv_rejected_total();
            let fired = kvt.timers_fired_total();
            tracing::info!(
                target: "rates",
                scope = "global",
                push_s = format!("{:.0}", d(cur.push_messages, prev.push_messages)),
                pop_s = format!("{:.0}", d(cur.pop_messages, prev.pop_messages)),
                ack_s = format!("{:.0}", d(cur.ack_messages, prev.ack_messages)),
                txn_s = format!("{:.0}", d(cur.transactions, prev.transactions)),
                p50_push_ms = format!("{:.2}", h.metrics.push.rtt_percentile(50.0)),
                p99_push_ms = format!("{:.2}", h.metrics.push.rtt_percentile(99.0)),
                p99_pop_ms = format!("{:.2}", h.metrics.pop.rtt_percentile(99.0)),
                p99_ack_ms = format!("{:.2}", h.metrics.ack.rtt_percentile(99.0)),
                ack_hit_pct = format!("{:.1}", ack_hit),
                pop_empty_pct = format!("{:.1}", pop_empty_pct),
                parked = parked,
                pool = format!("{}/{}", st.size, st.max_size),
                pool_waiting = st.waiting,
                adm_budget = adm.budget,
                adm_mode = adm.mode_str(),
                adm_lanes = %adm.lanes_str(),
                trains_s = format!("{:.1}", adm.trains_per_s),
                txn_train = format!("{:.1}", adm.txn_per_train_avg),
                txn_train_p95 = adm.txn_per_train_p95,
                cycle_ms = format!("{:.2}", adm.cycle_ms.unwrap_or(0.0)),
                oldest_wait_ms = format!("{:.1}", adm.oldest_wait_ms.unwrap_or(0.0)),
                adm_last = adm.last_change,
                visits_s = format!("{:.0}", d_visits as f64 / secs),
                cands_visit = format!("{:.1}", d_cands as f64 / (d_visits.max(1)) as f64),
                ready_age_p50 = format!("{:.0}", age_p50),
                ready_age_p95 = format!("{:.0}", age_p95),
                ring_oldest_ms = format!("{:.0}", ring_oldest),
                ring_depth = ring_depth,
                buffered = buffered,
                kv_ops_s = format!("{:.0}", d(kv_ops, prev_kv_ops)),
                kv_p99_ms = format!("{:.2}", kvt.kv_duration_p99()),
                // Non-zero on a tenant that was at zero is the EARLIEST of the six
                // pre-incident signals (§14.3.1) — a customer has just put KV reads
                // on their end users' path. The per-tenant list is the `sizes`
                // top-N line below; this is the cell-wide total.
                kv_rej_s = format!("{:.1}", d(kv_rej, prev_kv_rej)),
                timers_fire_s = format!("{:.1}", d(fired, prev_fired)),
                timers_backlog = kvt.timers_due(),
                fire_lag_p95 = format!("{:.2}", kvt.fire_lag_p95_seconds()),
                // PLAN_CONFLATION §6.1: log positions per second retired WITHOUT a
                // handler invocation, and declaration conflicts per second. Deltas
                // over the same window as everything else on this line; both stay
                // 0.0 on every deployment where no group conflates. NO third
                // periodic block and no per-message line (§6).
                conflated_s = format!("{:.0}", d(cur.conflated, prev.conflated)),
                cfl_conflict_s = format!("{:.1}", d(cur.conflation_conflicts, prev.conflation_conflicts)),
                "broker rates"
            );
            prev_kv_ops = kv_ops;
            prev_kv_rej = kv_rej;
            prev_fired = fired;

            // ---- rates (top-N hot queues) ----
            let cur_q = h.metrics.per_queue.snapshot();
            #[allow(clippy::type_complexity)]
            let mut ranked: Vec<(String, f64, f64, f64, f64, f64, f64)> = Vec::new();
            for (q, now) in &cur_q {
                let was = prev_q.get(q).copied().unwrap_or_default();
                let push_s = d(now.push_messages, was.push_messages);
                let pop_s = d(now.pop_count, was.pop_count);
                let ack_s = d(now.ack_success + now.ack_failed, was.ack_success + was.ack_failed);
                let activity = push_s + pop_s + ack_s;
                if activity <= 0.0 {
                    continue;
                }
                // Weighted avg lag over the window.
                let lag_cnt = now.lag_count.saturating_sub(was.lag_count);
                let lag_ms = if lag_cnt > 0 {
                    now.lag_sum_ms.saturating_sub(was.lag_sum_ms) as f64 / lag_cnt as f64
                } else {
                    0.0
                };
                let pop_reqs_q = (now.pop_count + now.pop_empty)
                    .saturating_sub(was.pop_count + was.pop_empty);
                let empty_pct = if pop_reqs_q > 0 {
                    100.0 * now.pop_empty.saturating_sub(was.pop_empty) as f64 / pop_reqs_q as f64
                } else {
                    0.0
                };
                // PLAN_CONFLATION §6.1: positions/s this queue retired without a
                // handler invocation. 0.0 unless a group on it conflates.
                let conflated_s = d(now.conflated, was.conflated);
                ranked.push((q.clone(), activity, push_s, pop_s, ack_s, lag_ms, conflated_s));
                let _ = empty_pct; // folded into the line below
            }
            ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            let total_hot = ranked.len();
            for (q, _act, push_s, pop_s, ack_s, lag_ms, conflated_s) in ranked.into_iter().take(h.top_n) {
                // The counters are keyed by the (tenant, queue) composite, whose
                // separator is invisible — logging it raw renders as the tenant uuid
                // glued to the queue name.
                let (tenant, queue) = crate::handlers::split_tenant_queue(&q);
                tracing::info!(
                    target: "rates",
                    tenant = %tenant,
                    queue = %queue,
                    push_s = format!("{:.0}", push_s),
                    pop_s = format!("{:.0}", pop_s),
                    ack_s = format!("{:.0}", ack_s),
                    lag_ms = format!("{:.0}", lag_ms),
                    conflated_s = format!("{:.0}", conflated_s),
                    hot = format!("{}/{}", h.top_n.min(total_hot), total_hot),
                    "queue rates"
                );
            }

            // ---- sizes ----
            let dedup_mb = h.dedup.resident_bytes() as f64 / (1024.0 * 1024.0);
            let dedup_pct = if h.dedup_cap_mb > 0 {
                100.0 * dedup_mb / h.dedup_cap_mb as f64
            } else {
                0.0
            };
            let (ack_entries, ack_bytes) = h.ack_registry.footprint();
            let rings = h.hotlist.ring_sizes(crate::util::now_epoch_ms());
            let ready: usize = rings.iter().map(|x| x.ready).sum();
            let wheel: usize = rings.iter().map(|x| x.wheel).sum();
            let rss_gb = h.metrics.resident_bytes() as f64 / (1024.0 * 1024.0 * 1024.0);
            // Occupancy, from the sweeper's slow rollup (§7.5) — never a count(*)
            // from here. The measure is stale BY CONSTRUCTION and the quota it
            // feeds is soft: the enforcer is each broker's own in-process delta,
            // and the measure only drives RELEASE (§9.3).
            let (kv_rows, kv_bytes, timers_pending) = kvt.usage_totals();
            let (pool_used, pool_max) = kvt.kv_pool();
            tracing::info!(
                target: "sizes",
                dedup = format!("{:.0}/{}MB({:.0}%)", dedup_mb, h.dedup_cap_mb, dedup_pct),
                dedup_suppressed = h.dedup.suppressed_partitions(),
                ack_reg = format!("{}e/{:.1}MB", ack_entries, ack_bytes as f64 / (1024.0 * 1024.0)),
                hotlist = format!("{}rings/{}ready/{}wheel", rings.len(), ready, wheel),
                spool_pending = h.file_buffer.pending_count(),
                spool_healthy = h.file_buffer.db_healthy(),
                pool = format!("{}/{}", st.size, st.max_size),
                pool_waiting = st.waiting,
                adm_commits = adm.completions,
                rss_gb = format!("{:.2}", rss_gb),
                kv = format!("{}rows/{:.1}MB", kv_rows, kv_bytes as f64 / (1024.0 * 1024.0)),
                // The failure that DISGUISES ITSELF AS SUCCESS: reads stay
                // perfectly correct while the table grows, because expiry is a
                // predicate and not the physical absence of the row. Alarm above
                // 50 000 rows OR an expiry lag over 600 s, whichever comes first
                // (§14.3.4).
                kv_unpruned = kvt.kv_expired_not_pruned(),
                timers = format!("{}pending", timers_pending),
                kv_pool = format!("{}/{}", pool_used, pool_max),
                "broker sizes"
            );
            // Per-tenant top-N, modelled on the per-queue lines above. This is
            // where the per-tenant view of these surfaces LIVES (§14.1): it is a
            // log line and a JSON endpoint, never a Prometheus label on a
            // per-operation counter, because there the cardinality would be
            // tenant x op x outcome, i.e. chosen by the user.
            //
            // Still SKIPPED for a tenant with nothing stored and nothing pending, so
            // a cell that has the surfaces but no traffic on them prints no per-tenant
            // lines at all — the same anti-flood rule the per-queue block follows.
            let (top, total_tenants) = kvt.usage_top(h.top_n);
            for t in top {
                if t.kv_rows == 0 && t.timers_pending == 0 {
                    continue;
                }
                tracing::info!(
                    target: "sizes",
                    tenant = %t.tenant,
                    kv_rows = t.kv_rows,
                    // ESTIMATE, and labelled as one everywhere it is shown (§7.5).
                    kv_mb_est = format!("{:.1}", t.kv_bytes as f64 / (1024.0 * 1024.0)),
                    timers_pending = t.timers_pending,
                    kv_quota_pct = format!("{:.0}", t.kv_quota_ratio * 100.0),
                    timers_quota_pct = format!("{:.0}", t.timers_quota_ratio * 100.0),
                    fire_lag_p95 = format!("{:.2}", kvt.fire_lag_p95_seconds_of(&t.tenant)),
                    top = format!("{}/{}", h.top_n.min(total_tenants), total_tenants),
                    "tenant sizes"
                );
            }

            prev = cur;
            prev_q = cur_q;
            prev_hits = hits;
            prev_misses = misses;
            prev_empty = cur_empty;
        }
    });
}
