//! `rsm/timing.rs` — node-local timing metrics (PLAN_RAFT.md O18, D17; PERF-1).
//!
//! One process-global registry of lock-free histograms and counters the leader
//! pipeline feeds and `/metrics/prometheus` reads. Everything here is
//! NODE-LOCAL and NEVER replicated (D17, like `local_metrics`): a duration is a
//! measurement of THIS node's wall clock, so two replicas will disagree on it
//! and that is correct. Nothing here is ever written to committed state, so it
//! is outside the I2 line — but the measurement itself must not read a clock
//! from inside the I2-denied files (`apply`, `state`, `store`). It does not:
//!
//! - the coarse apply / commit / durable-point stage timings are taken in
//!   `apply::run` through the INJECTED [`apply::Clock`] (the one clock the apply
//!   side already turns on the cadence, never `Instant::now()` in `apply.rs`);
//! - the fsync-bound sub-stages are measured where the syscall is, in
//!   `segments` and the replicator `log`/`local` (none under the I2 deny);
//! - the planner, batcher and facade stages are measured in `batcher` and
//!   `facade` (the planner is exempt from the deny, §5.2).
//!
//! The store layer deliberately keeps NO clock (`store/mod.rs`: "the metrics
//! below are counts"), so the per-entry LMDB puts are not clocked here; the
//! per-entry split the exporter shows is `segment` (the file `write_all`, the
//! real per-entry I/O) versus `other` (everything else, = total − segment).
//!
//! # The knob
//!
//! `QUEEN_RAFT_METRICS` (default ON). When it is `0`/`false`/`off`/`no`, every
//! `record`, `publish` and the 10 s timing log is a single cached-bool branch
//! and returns before touching an atomic — so the VM pass can ablate the whole
//! instrumentation and price it (PERF-1: keep the cost < 1%). Read once, at
//! first use, into a `LazyLock`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use crate::rsm::apply::ApplyStats;
use crate::rsm::planner::CommandKind;

// ---------------------------------------------------------------------------
// The knob
// ---------------------------------------------------------------------------

static ENABLED: LazyLock<bool> = LazyLock::new(|| {
    // timing.rs is not under the I2 `disallowed_methods` deny (that scope is
    // apply/state/store); a node-local knob read here is exactly what D17
    // allows. Resolved once and cached.
    match std::env::var("QUEEN_RAFT_METRICS") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        Err(_) => true,
    }
});

/// Is the instrumentation on? A cached bool; the ablation knob is
/// `QUEEN_RAFT_METRICS` (default on).
#[inline(always)]
pub fn enabled() -> bool {
    *ENABLED
}

/// A stage timestamp, or `None` when the instrumentation is off.
///
/// Every hot-path `Instant::now()` that exists ONLY to feed a histogram takes
/// its start time through this, and every stage records only when it gets a
/// `Some` back. So `QUEEN_RAFT_METRICS=0` removes the CLOCK READ itself, not
/// just the histogram write — the difference the refutation of PERF-1 caught:
/// gating `Histogram::record` alone left ~a-dozen `Instant::now()`/`elapsed()`
/// per entry always on, so the knob never priced the whole lever and the VM
/// ablation would have understated the true cost. With `stamp` on the read
/// side, the ON-minus-OFF delta is exactly what the knob switches.
///
/// It is NOT for the apply thread: `apply.rs` may read only the injected
/// `apply::Clock` (I2), so it gates its own `clock.now()` on [`enabled`]
/// directly rather than calling this.
#[inline(always)]
pub fn stamp() -> Option<Instant> {
    if enabled() {
        Some(Instant::now())
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// PERF-K: the per-cycle / per-group trace (`QUEEN_RAFT_CYCLE_TRACE`)
// ---------------------------------------------------------------------------

/// `QUEEN_RAFT_CYCLE_TRACE` (default OFF). When on, the batcher emits one
/// `CYCLETRACE` line per planning cycle and the log writer emits one
/// `GROUPTRACE` line per fsync group, straight to stderr (so it lands in the
/// broker log regardless of the `RUST_LOG`/`LOG_LEVEL` filter, and never
/// competes with the tracing subscriber's own formatting). It is a DIAGNOSTIC
/// switch — high-frequency, one line per cycle — off in every measurement run;
/// only the PERF-K trace-analysis run turns it on. Read once and cached; only
/// "1"/"true"/"on"/"yes" turns it on.
static CYCLE_TRACE: LazyLock<bool> =
    LazyLock::new(|| match std::env::var("QUEEN_RAFT_CYCLE_TRACE") {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "on" | "yes"
        ),
        Err(_) => false,
    });

/// Is the PERF-K per-cycle/per-group trace on? A cached bool.
#[inline(always)]
pub fn cycle_trace_enabled() -> bool {
    *CYCLE_TRACE
}

/// Microseconds since the first trace read, the monotone clock the `CYCLETRACE`
/// and `GROUPTRACE` lines share so the batcher cycle and the writer group can be
/// interleaved on one timeline in the analysis. Only ever called under
/// [`cycle_trace_enabled`], so its `Instant::now()` is off the hot path when the
/// trace is off.
pub fn trace_now_us() -> u64 {
    static START: LazyLock<Instant> = LazyLock::new(Instant::now);
    START.elapsed().as_micros() as u64
}

/// The trace sink: a bounded channel to a dedicated writer thread that owns a
/// buffered stderr. The hot-path emit (the batcher task, the log writer thread)
/// only formats and moves a `String` onto the channel — no syscall, no lock on
/// a shared stderr — so the trace does not throttle the very cycle it measures
/// (the first PERF-K trace put `eprintln!` inline and slowed the batcher to a
/// crawl, so every cycle looked arrival-driven). A bounded channel with
/// try-send drops a line rather than block a producer or grow without bound
/// (this is a DIAGNOSTIC path, only live under `QUEEN_RAFT_CYCLE_TRACE`, so a
/// dropped line is a gap in the trace, never a stall); the drop count is
/// reported at shutdown.
struct TraceSink {
    tx: std::sync::mpsc::SyncSender<String>,
    dropped: AtomicU64,
}

static TRACE_SINK: LazyLock<Option<TraceSink>> = LazyLock::new(|| {
    if !cycle_trace_enabled() {
        return None;
    }
    // 1<<17 lines of headroom; the writer keeps up with far more than the
    // batcher produces (a line is ~150 B, ≈20 MB of stderr per 100k lines).
    let (tx, rx) = std::sync::mpsc::sync_channel::<String>(1 << 17);
    std::thread::Builder::new()
        .name("queen-rsm-trace".into())
        .spawn(move || {
            use std::io::Write;
            let stderr = std::io::stderr();
            let mut w = std::io::BufWriter::with_capacity(1 << 20, stderr.lock());
            let mut since_flush = 0usize;
            loop {
                match rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(line) => {
                        let _ = w.write_all(line.as_bytes());
                        let _ = w.write_all(b"\n");
                        since_flush += 1;
                        if since_flush >= 4096 {
                            let _ = w.flush();
                            since_flush = 0;
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        let _ = w.flush();
                        since_flush = 0;
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        let _ = w.flush();
                        break;
                    }
                }
            }
        })
        .ok()
        .map(|_| TraceSink {
            tx,
            dropped: AtomicU64::new(0),
        })
});

/// Emit one trace line (the caller has already checked [`cycle_trace_enabled`]).
/// Moves the `String` onto the writer thread's channel; a full channel drops the
/// line and bumps the drop counter rather than blocking the producer.
pub fn cycle_trace_line(line: String) {
    if let Some(sink) = &*TRACE_SINK {
        if sink.tx.try_send(line).is_err() {
            sink.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// How many trace lines were dropped because the writer thread fell behind
/// (reported at shutdown so a gap in the trace is accounted for).
pub fn cycle_trace_dropped() -> u64 {
    TRACE_SINK
        .as_ref()
        .map(|s| s.dropped.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// How often `apply::run` emits the "rsm timing" summary line and republishes
/// the apply counters. `QUEEN_RAFT_TIMING_LOG_MS` overrides it (still gated by
/// `QUEEN_RAFT_METRICS`); 0 disables only the log line, not the histograms.
pub fn timing_log_interval() -> Duration {
    static MS: LazyLock<u64> = LazyLock::new(|| match std::env::var("QUEEN_RAFT_TIMING_LOG_MS") {
        Ok(v) => v.trim().parse().unwrap_or(10_000),
        Err(_) => 10_000,
    });
    Duration::from_millis(*MS)
}

// ---------------------------------------------------------------------------
// The histogram
// ---------------------------------------------------------------------------

const BUCKETS: usize = 65;

/// A lock-free log2-bucket histogram. Bucket `k` (k ≥ 1) holds the values in
/// `[2^(k-1), 2^k − 1]`; bucket 0 holds exactly 0. Records are a handful of
/// relaxed atomic adds and one `fetch_max`; reads walk the 65 buckets. Values
/// are unitless u64 — a duration in nanoseconds for a latency, a raw count for
/// a size.
pub struct Histogram {
    buckets: [AtomicU64; BUCKETS],
    count: AtomicU64,
    sum: AtomicU64,
    max: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Histogram {
        Histogram {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }
}

#[inline]
fn bucket_index(v: u64) -> usize {
    if v == 0 {
        0
    } else {
        (64 - v.leading_zeros()) as usize
    }
}

/// The upper bound of bucket `idx`, the value a quantile in it reports.
#[inline]
fn bucket_upper(idx: usize) -> u64 {
    if idx == 0 {
        0
    } else if idx >= 64 {
        u64::MAX
    } else {
        (1u64 << idx) - 1
    }
}

impl Histogram {
    /// Record one raw value (a nanosecond count or a size).
    #[inline]
    pub fn record(&self, v: u64) {
        if !enabled() {
            return;
        }
        self.buckets[bucket_index(v)].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(v, Ordering::Relaxed);
        self.max.fetch_max(v, Ordering::Relaxed);
    }

    /// Record a duration, in nanoseconds. `u64` holds ~584 years of ns.
    #[inline]
    pub fn record_dur(&self, d: Duration) {
        // `record` gates on `enabled()`; the `as u64` saturates far past any
        // real stage duration.
        self.record(d.as_nanos().min(u64::MAX as u128) as u64);
    }

    /// A point-in-time read. `count`/`sum`/`max` are exact; the quantiles are
    /// the bucket upper bounds, so an at-most estimate.
    pub fn snapshot(&self) -> HistSnapshot {
        let mut counts = [0u64; BUCKETS];
        for (i, b) in self.buckets.iter().enumerate() {
            counts[i] = b.load(Ordering::Relaxed);
        }
        let total = self.count.load(Ordering::Relaxed);
        HistSnapshot {
            count: total,
            sum: self.sum.load(Ordering::Relaxed),
            max: self.max.load(Ordering::Relaxed),
            p50: quantile(&counts, total, 0.50),
            p90: quantile(&counts, total, 0.90),
            p99: quantile(&counts, total, 0.99),
            p999: quantile(&counts, total, 0.999),
        }
    }
}

fn quantile(counts: &[u64; BUCKETS], total: u64, q: f64) -> u64 {
    if total == 0 {
        return 0;
    }
    let target = ((q * total as f64).ceil() as u64).clamp(1, total);
    let mut cum = 0u64;
    for (idx, c) in counts.iter().enumerate() {
        cum += *c;
        if cum >= target {
            return bucket_upper(idx);
        }
    }
    bucket_upper(BUCKETS - 1)
}

/// What [`Histogram::snapshot`] returns. Raw units (ns for a latency).
#[derive(Clone, Copy, Debug, Default)]
pub struct HistSnapshot {
    pub count: u64,
    pub sum: u64,
    pub max: u64,
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
}

// ---------------------------------------------------------------------------
// The registry
// ---------------------------------------------------------------------------

const KIND_COUNT: usize = 13;

const ALL_KINDS: [CommandKind; KIND_COUNT] = [
    CommandKind::Push,
    CommandKind::PopPinned,
    CommandKind::PopWildcard,
    CommandKind::PopDiscover,
    CommandKind::Ack,
    CommandKind::AckPositional,
    CommandKind::Nack,
    CommandKind::Renew,
    CommandKind::DlqHead,
    CommandKind::Transaction,
    CommandKind::Kv,
    CommandKind::Timers,
    CommandKind::Effects,
];

#[inline]
fn kind_index(k: CommandKind) -> usize {
    match k {
        CommandKind::Push => 0,
        CommandKind::PopPinned => 1,
        CommandKind::PopWildcard => 2,
        CommandKind::PopDiscover => 3,
        CommandKind::Ack => 4,
        CommandKind::AckPositional => 5,
        CommandKind::Nack => 6,
        CommandKind::Renew => 7,
        CommandKind::DlqHead => 8,
        CommandKind::Transaction => 9,
        CommandKind::Kv => 10,
        CommandKind::Timers => 11,
        CommandKind::Effects => 12,
    }
}

/// The per-command-kind planner counters (O18).
#[derive(Default)]
pub struct KindCounters {
    planned: [AtomicU64; KIND_COUNT],
    slow: [AtomicU64; KIND_COUNT],
    plan_ns: [Histogram; KIND_COUNT],
}

impl KindCounters {
    /// One planned command of `kind`, whose per-command planning took `plan`.
    /// `slow` is set when it crossed `QUEEN_RAFT_SLOW_COMMAND_MS` (the O18 log
    /// line is emitted by the caller, which has the tenant/queue).
    pub fn record(&self, kind: CommandKind, plan: Duration, slow: bool) {
        if !enabled() {
            return;
        }
        let i = kind_index(kind);
        self.planned[i].fetch_add(1, Ordering::Relaxed);
        self.plan_ns[i].record_dur(plan);
        if slow {
            self.slow[i].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Commands of `kind` planned so far (for tests and callers).
    pub fn planned(&self, kind: CommandKind) -> u64 {
        self.planned[kind_index(kind)].load(Ordering::Relaxed)
    }
}

/// Mirror of [`ApplyStats`] as atomics, republished by `apply::run` every
/// timing-log interval so the exporter can read the apply counters without a
/// handle on the apply thread's `Applier`.
#[derive(Default)]
pub struct ApplyStatsMirror {
    pub entries: AtomicU64,
    pub skipped: AtomicU64,
    pub effects: AtomicU64,
    pub appends: AtomicU64,
    pub messages: AtomicU64,
    pub bytes_appended: AtomicU64,
    pub commits: AtomicU64,
    pub durable_points: AtomicU64,
    pub durable_points_failed: AtomicU64,
    pub outcomes_recorded: AtomicU64,
    pub wakes: AtomicU64,
    pub files_unlinked: AtomicU64,
    pub gc_deferred: AtomicU64,
    pub rows_swept: AtomicU64,
    pub missing_rows: AtomicU64,
}

impl ApplyStatsMirror {
    fn publish(&self, s: &ApplyStats) {
        self.entries.store(s.entries, Ordering::Relaxed);
        self.skipped.store(s.skipped, Ordering::Relaxed);
        self.effects.store(s.effects, Ordering::Relaxed);
        self.appends.store(s.appends, Ordering::Relaxed);
        self.messages.store(s.messages, Ordering::Relaxed);
        self.bytes_appended
            .store(s.bytes_appended, Ordering::Relaxed);
        self.commits.store(s.commits, Ordering::Relaxed);
        self.durable_points
            .store(s.durable_points, Ordering::Relaxed);
        self.durable_points_failed
            .store(s.durable_points_failed, Ordering::Relaxed);
        self.outcomes_recorded
            .store(s.outcomes_recorded, Ordering::Relaxed);
        self.wakes.store(s.wakes, Ordering::Relaxed);
        self.files_unlinked
            .store(s.files_unlinked, Ordering::Relaxed);
        self.gc_deferred.store(s.gc_deferred, Ordering::Relaxed);
        self.rows_swept.store(s.rows_swept, Ordering::Relaxed);
        self.missing_rows.store(s.missing_rows, Ordering::Relaxed);
    }
}

/// The whole `queen_raft_*` surface. One process-global instance, reached
/// through [`metrics`].
#[derive(Default)]
pub struct RsmMetrics {
    // -- batcher / planner (latency, ns) --
    /// Whole planning cycle on the blocking pool.
    pub plan: Histogram,
    /// MEASURE: thread CPU time of the same command loop `plan` times (ns).
    pub plan_cpu: Histogram,
    /// MEASURE: the whole `plan_cycle_blocking` call (overlay and ring rebuild,
    /// command loop, leader steps) — wall and thread CPU (ns).
    pub plan_whole_wall: Histogram,
    pub plan_whole_cpu: Histogram,
    /// MEASURE: commands drained into a cycle but deferred unplanned by the
    /// `plan_budget_ms` cut (raw count per cycle).
    pub plan_deferred: Histogram,
    /// A command's arrival on the facade channel → its entry proposed.
    pub arrival_to_proposed: Histogram,
    /// PERF-G split of `arrival_to_proposed`, leg 1: a command's arrival on the
    /// facade channel → the cycle that drained it (time it sat in the driver's
    /// queue waiting for a pipeline slot). This is the "waited ~4 ms before it
    /// was proposed" leg the round-3 push-p50 work targets.
    pub queue_wait: Histogram,
    /// PERF-G split, leg 2: the cycle drain → its entry proposed (the plan on
    /// the blocking pool, the `spawn_blocking` hop, the encode, the inline
    /// submit). Measured once per cycle that proposes an entry.
    pub drain_to_propose: Histogram,
    /// Entry proposed → its waiters answered (commit + local apply + notify).
    pub propose_roundtrip: Histogram,
    // -- batcher / writer (size) --
    /// Drain size, in commands.
    pub drain_commands: Histogram,
    /// Drain size, in messages (push items; one for every other kind).
    pub drain_messages: Histogram,
    // -- replicator log writer --
    /// Entry proposed → its group fsynced (committed).
    pub proposed_to_committed: Histogram,
    /// PERF-G: entry proposed → the writer begins THIS entry's group fsync
    /// (time queued at the writer behind the previous group's fsync). With
    /// `proposed_to_committed` and `log_fsync` this isolates writer queueing
    /// from the fsync itself: `proposed_to_committed ≈ writer_pickup + fsync`.
    pub writer_pickup: Histogram,
    /// One group's fsync duration.
    pub log_fsync: Histogram,
    /// Group size, in entries.
    pub group_entries: Histogram,
    /// Group size, in bytes.
    pub group_bytes: Histogram,
    // -- apply thread --
    /// Per-entry apply duration, total (measured through the injected clock).
    pub apply_entry: Histogram,
    /// The segment `write_all` portion of an entry (the per-entry file I/O).
    pub apply_segment: Histogram,
    /// Per-entry remainder: store puts (page cache, not clocked) + dispatch +
    /// derived-RAM. Recorded as `apply_entry − apply_segment` by `apply::run`.
    pub apply_other: Histogram,
    /// Apply channel depth observed at each receive.
    pub apply_channel_depth: Histogram,
    /// Store commit duration (§11.3).
    pub store_commit: Histogram,
    // -- durable point (§11.4) --
    /// Whole durable point.
    pub durable_point: Histogram,
    /// The segment-file fsync portion.
    pub durable_seg_fsync: Histogram,
    /// The directory fsync portion.
    pub durable_dir_fsync: Histogram,
    // -- facade --
    /// Pop payload read latency (the blocking segment render).
    pub pop_read: Histogram,
    // -- PERF-J: the PUSH HTTP boundary, push-only (the mixed `arrival_to_proposed`
    // is dominated by the empty polling POP commands, so its p50 hides the push).
    /// Whole raft push handler: `dispatch_push` entry → response body built.
    /// Compared to goload's per-request latency this isolates the server-side
    /// cost from the loader / axum-accept / middleware wrapper.
    pub push_h_total: Histogram,
    /// Push pre-submit work: handler entry → the first command handed to the
    /// batcher channel (parse + name-check + intra-request dedup + frame pack).
    pub push_h_prep: Histogram,
    /// Push channel enqueue: the `cmd_tx.send` await for a push's group commands
    /// (a full bounded channel blocks here — facade back-pressure, invisible to
    /// `arrival_to_proposed` which stamps just before the send).
    pub push_h_submit: Histogram,
    /// Push reply wait: last command sent → every group's reply collected
    /// (propose + commit + local apply + answer), PUSH-ONLY. This is the push
    /// equivalent of the pop-aliased `arrival_to_proposed + propose_roundtrip`.
    pub push_h_await: Histogram,
    /// Whole raft pop handler: `pop_run` entry → response body built, POP-ONLY.
    /// Confirms the empty polling pops are cheap and dominate the mixed stages.
    pub pop_h_total: Histogram,
    // -- counters --
    pub kinds: KindCounters,
    /// Commands whose per-command planning crossed the slow threshold (O18).
    pub slow_commands: AtomicU64,
    pub apply_stats: ApplyStatsMirror,
    /// A monotone counter incremented per apply-channel receive, so the
    /// exporter can show the apply-thread receive rate alongside the depth.
    pub apply_receives: AtomicU64,
}

impl RsmMetrics {
    /// Republish the apply counters (called on the timing-log cadence).
    pub fn publish_apply_stats(&self, s: &ApplyStats) {
        if !enabled() {
            return;
        }
        self.apply_stats.publish(s);
    }
}

static METRICS: LazyLock<RsmMetrics> = LazyLock::new(RsmMetrics::default);

/// The process-global registry every instrumentation site feeds.
#[inline(always)]
pub fn metrics() -> &'static RsmMetrics {
    &METRICS
}

// ---------------------------------------------------------------------------
// The apply channel depth gauge
// ---------------------------------------------------------------------------

static APPLY_DEPTH: AtomicU64 = AtomicU64::new(0);

/// Monotone total of segment `write_all` time, in ns, so `apply::run` can
/// split a per-entry apply into its segment portion and the rest without a
/// clock of its own (I2: apply.rs holds none). Only the single apply thread
/// appends in phase 1, so the delta a run-loop reads across one `apply` call is
/// that entry's segment write time.
static SEG_WRITE_NS: AtomicU64 = AtomicU64::new(0);

/// `segments` calls this after each payload `write_all`: it records the
/// per-append duration into `apply_segment` and adds it to the running total
/// `apply::run` reads for the per-entry split.
#[inline]
pub fn record_segment_write(d: Duration) {
    if !enabled() {
        return;
    }
    let ns = d.as_nanos().min(u64::MAX as u128) as u64;
    metrics().apply_segment.record(ns);
    SEG_WRITE_NS.fetch_add(ns, Ordering::Relaxed);
}

/// The running total of segment write ns (see [`record_segment_write`]).
#[inline]
pub fn segment_write_ns_total() -> u64 {
    SEG_WRITE_NS.load(Ordering::Relaxed)
}

/// The log writer calls this immediately before handing a `Committed` to the
/// apply channel, so the apply thread can read how deep the channel ran.
#[inline]
pub fn apply_channel_send() {
    if enabled() {
        APPLY_DEPTH.fetch_add(1, Ordering::Relaxed);
    }
}

/// The apply thread calls this right after a successful receive. It returns the
/// depth the channel held (including the entry just taken) and decrements the
/// gauge, saturating at 0 so a test that drives `apply::run` without the writer
/// never underflows.
#[inline]
pub fn apply_channel_recv() -> u64 {
    if !enabled() {
        return 0;
    }
    metrics().apply_receives.fetch_add(1, Ordering::Relaxed);
    let prev = APPLY_DEPTH.load(Ordering::Relaxed);
    if prev > 0 {
        let _ = APPLY_DEPTH.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |d| {
            Some(d.saturating_sub(1))
        });
    }
    let depth = prev.max(1);
    metrics().apply_channel_depth.record(depth);
    depth
}

// ---------------------------------------------------------------------------
// Prometheus rendering (quantiles precomputed; the exporter is text)
// ---------------------------------------------------------------------------

/// Append the whole `queen_raft_*` families to a Prometheus text body. Called
/// by the raft-mode `/metrics/prometheus` handler after the in-process gauges.
pub fn render_prometheus(out: &mut String) {
    // The knob ablates the whole surface: with `QUEEN_RAFT_METRICS` off nothing
    // is recorded, so every family would render all-zero. Skip them entirely,
    // as the raft `/metrics/prometheus` handler documents.
    if !enabled() {
        return;
    }
    let m = metrics();

    // Latency summaries (ns → seconds).
    let lat: [(&str, &str, &Histogram); 24] = [
        ("queen_raft_plan_seconds", "Planner cycle duration", &m.plan),
        ("queen_raft_plan_cpu_seconds", "Planner command loop, thread CPU time", &m.plan_cpu),
        ("queen_raft_plan_whole_wall_seconds", "Whole plan_cycle_blocking call, wall", &m.plan_whole_wall),
        ("queen_raft_plan_whole_cpu_seconds", "Whole plan_cycle_blocking call, thread CPU", &m.plan_whole_cpu),
        (
            "queen_raft_push_h_total_seconds",
            "PERF-J: whole raft push handler (entry to response), push-only",
            &m.push_h_total,
        ),
        (
            "queen_raft_push_h_prep_seconds",
            "PERF-J: push pre-submit (parse+pack), entry to first channel send",
            &m.push_h_prep,
        ),
        (
            "queen_raft_push_h_submit_seconds",
            "PERF-J: push channel enqueue (cmd_tx.send await), back-pressure",
            &m.push_h_submit,
        ),
        (
            "queen_raft_push_h_await_seconds",
            "PERF-J: push reply wait (propose+commit+apply+answer), push-only",
            &m.push_h_await,
        ),
        (
            "queen_raft_pop_h_total_seconds",
            "PERF-J: whole raft pop handler (entry to response), pop-only",
            &m.pop_h_total,
        ),
        (
            "queen_raft_arrival_to_proposed_seconds",
            "Command arrival to its entry proposed",
            &m.arrival_to_proposed,
        ),
        (
            "queen_raft_queue_wait_seconds",
            "Command arrival to the cycle that drained it (PERF-G leg 1)",
            &m.queue_wait,
        ),
        (
            "queen_raft_drain_to_propose_seconds",
            "Cycle drain to its entry proposed (PERF-G leg 2)",
            &m.drain_to_propose,
        ),
        (
            "queen_raft_propose_roundtrip_seconds",
            "Entry proposed to its waiters answered (commit+apply+notify)",
            &m.propose_roundtrip,
        ),
        (
            "queen_raft_proposed_to_committed_seconds",
            "Entry proposed to its group fsynced",
            &m.proposed_to_committed,
        ),
        (
            "queen_raft_writer_pickup_seconds",
            "Entry proposed to the writer starting its group fsync (PERF-G)",
            &m.writer_pickup,
        ),
        (
            "queen_raft_log_fsync_seconds",
            "Log group fsync duration",
            &m.log_fsync,
        ),
        (
            "queen_raft_apply_entry_seconds",
            "Per-entry apply duration (total)",
            &m.apply_entry,
        ),
        (
            "queen_raft_apply_segment_seconds",
            "Per-entry segment write_all duration",
            &m.apply_segment,
        ),
        (
            "queen_raft_apply_other_seconds",
            "Per-entry apply remainder (store puts + dispatch + derived)",
            &m.apply_other,
        ),
        (
            "queen_raft_store_commit_seconds",
            "Store commit duration",
            &m.store_commit,
        ),
        (
            "queen_raft_durable_point_seconds",
            "Durable point duration (total)",
            &m.durable_point,
        ),
        (
            "queen_raft_durable_seg_fsync_seconds",
            "Durable point segment-file fsync duration",
            &m.durable_seg_fsync,
        ),
        (
            "queen_raft_durable_dir_fsync_seconds",
            "Durable point directory fsync duration",
            &m.durable_dir_fsync,
        ),
        (
            "queen_raft_pop_read_seconds",
            "Pop payload read (segment render) latency",
            &m.pop_read,
        ),
    ];
    for (name, help, h) in lat {
        render_summary(out, name, help, &h.snapshot(), 1e-9, true);
    }

    // Size summaries (raw counts).
    let sizes: [(&str, &str, &Histogram); 6] = [
        (
            "queen_raft_plan_deferred",
            "Commands drained but deferred unplanned by the plan budget cut",
            &m.plan_deferred,
        ),
        (
            "queen_raft_drain_commands",
            "Drain size in commands",
            &m.drain_commands,
        ),
        (
            "queen_raft_drain_messages",
            "Drain size in messages",
            &m.drain_messages,
        ),
        (
            "queen_raft_group_entries",
            "Log group size in entries",
            &m.group_entries,
        ),
        (
            "queen_raft_group_bytes",
            "Log group size in bytes",
            &m.group_bytes,
        ),
        (
            "queen_raft_apply_channel_depth",
            "Apply channel depth at receive",
            &m.apply_channel_depth,
        ),
    ];
    for (name, help, h) in sizes {
        render_summary(out, name, help, &h.snapshot(), 1.0, false);
    }

    // Per-kind planner counters (O18).
    out.push_str("# HELP queen_raft_planner_commands_total Commands planned, by kind\n");
    out.push_str("# TYPE queen_raft_planner_commands_total counter\n");
    for k in ALL_KINDS {
        let i = kind_index(k);
        let v = m.kinds.planned[i].load(Ordering::Relaxed);
        out.push_str(&format!(
            "queen_raft_planner_commands_total{{kind=\"{}\"}} {}\n",
            k.name(),
            v
        ));
    }
    out.push_str("# HELP queen_raft_planner_slow_total Slow-planned commands, by kind (O18)\n");
    out.push_str("# TYPE queen_raft_planner_slow_total counter\n");
    for k in ALL_KINDS {
        let i = kind_index(k);
        let v = m.kinds.slow[i].load(Ordering::Relaxed);
        out.push_str(&format!(
            "queen_raft_planner_slow_total{{kind=\"{}\"}} {}\n",
            k.name(),
            v
        ));
    }
    // Per-kind planning-time summary: ONE HELP/TYPE for the family, then a
    // sample line per kind (a Prometheus family carries its HELP/TYPE once).
    let pname = "queen_raft_planner_plan_seconds";
    out.push_str(&format!(
        "# HELP {pname} Per-command planning duration, by kind\n# TYPE {pname} summary\n"
    ));
    for k in ALL_KINDS {
        let i = kind_index(k);
        let snap = m.kinds.plan_ns[i].snapshot();
        if snap.count == 0 {
            continue;
        }
        render_kind_summary(out, pname, k.name(), &snap);
    }

    out.push_str(
        "# HELP queen_raft_slow_commands_total Commands over QUEEN_RAFT_SLOW_COMMAND_MS\n",
    );
    out.push_str("# TYPE queen_raft_slow_commands_total counter\n");
    out.push_str(&format!(
        "queen_raft_slow_commands_total {}\n",
        m.slow_commands.load(Ordering::Relaxed)
    ));
    out.push_str("# HELP queen_raft_apply_receives_total Apply-channel receives\n");
    out.push_str("# TYPE queen_raft_apply_receives_total counter\n");
    out.push_str(&format!(
        "queen_raft_apply_receives_total {}\n",
        m.apply_receives.load(Ordering::Relaxed)
    ));

    // ApplyStats mirror (counters).
    let s = &m.apply_stats;
    let stats: [(&str, u64); 15] = [
        ("entries", s.entries.load(Ordering::Relaxed)),
        ("skipped", s.skipped.load(Ordering::Relaxed)),
        ("effects", s.effects.load(Ordering::Relaxed)),
        ("appends", s.appends.load(Ordering::Relaxed)),
        ("messages", s.messages.load(Ordering::Relaxed)),
        ("bytes_appended", s.bytes_appended.load(Ordering::Relaxed)),
        ("commits", s.commits.load(Ordering::Relaxed)),
        ("durable_points", s.durable_points.load(Ordering::Relaxed)),
        (
            "durable_points_failed",
            s.durable_points_failed.load(Ordering::Relaxed),
        ),
        (
            "outcomes_recorded",
            s.outcomes_recorded.load(Ordering::Relaxed),
        ),
        ("wakes", s.wakes.load(Ordering::Relaxed)),
        ("files_unlinked", s.files_unlinked.load(Ordering::Relaxed)),
        ("gc_deferred", s.gc_deferred.load(Ordering::Relaxed)),
        ("rows_swept", s.rows_swept.load(Ordering::Relaxed)),
        ("missing_rows", s.missing_rows.load(Ordering::Relaxed)),
    ];
    out.push_str("# HELP queen_raft_apply_stats Apply thread counters (ApplyStats)\n");
    out.push_str("# TYPE queen_raft_apply_stats counter\n");
    for (field, v) in stats {
        out.push_str(&format!(
            "queen_raft_apply_stats{{field=\"{field}\"}} {v}\n"
        ));
    }
    crate::rsm::dbgctr::render(out);
}

fn render_summary(
    out: &mut String,
    name: &str,
    help: &str,
    snap: &HistSnapshot,
    scale: f64,
    seconds: bool,
) {
    out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} summary\n"));
    let q = |v: u64| -> String {
        if seconds {
            format!("{:.9}", v as f64 * scale)
        } else {
            format!("{v}")
        }
    };
    out.push_str(&format!("{name}{{quantile=\"0.5\"}} {}\n", q(snap.p50)));
    out.push_str(&format!("{name}{{quantile=\"0.9\"}} {}\n", q(snap.p90)));
    out.push_str(&format!("{name}{{quantile=\"0.99\"}} {}\n", q(snap.p99)));
    out.push_str(&format!("{name}{{quantile=\"0.999\"}} {}\n", q(snap.p999)));
    out.push_str(&format!("{name}{{quantile=\"1\"}} {}\n", q(snap.max)));
    out.push_str(&format!("{name}_count {}\n", snap.count));
    if seconds {
        out.push_str(&format!("{name}_sum {:.9}\n", snap.sum as f64 * scale));
    } else {
        out.push_str(&format!("{name}_sum {}\n", snap.sum));
    }
}

fn render_kind_summary(out: &mut String, name: &str, kind: &str, snap: &HistSnapshot) {
    let q = |v: u64| format!("{:.9}", v as f64 * 1e-9);
    out.push_str(&format!(
        "{name}{{kind=\"{kind}\",quantile=\"0.5\"}} {}\n",
        q(snap.p50)
    ));
    out.push_str(&format!(
        "{name}{{kind=\"{kind}\",quantile=\"0.99\"}} {}\n",
        q(snap.p99)
    ));
    out.push_str(&format!(
        "{name}{{kind=\"{kind}\",quantile=\"1\"}} {}\n",
        q(snap.max)
    ));
    out.push_str(&format!("{name}_count{{kind=\"{kind}\"}} {}\n", snap.count));
}

// ---------------------------------------------------------------------------
// The 10 s "rsm timing" log line
// ---------------------------------------------------------------------------

/// Build the periodic "rsm timing" summary and republish the apply counters.
/// Called by `apply::run` on the timing-log cadence. `stats` is the live
/// [`ApplyStats`]; the pipeline stages are read from the global registry.
pub fn emit_timing_log(stats: &ApplyStats) {
    if !enabled() {
        return;
    }
    let m = metrics();
    m.publish_apply_stats(stats);

    let us = |ns: u64| ns as f64 / 1000.0; // ns → µs for a readable line
    let plan = m.plan.snapshot();
    let a2p = m.arrival_to_proposed.snapshot();
    let qw = m.queue_wait.snapshot();
    let d2p = m.drain_to_propose.snapshot();
    let rt = m.propose_roundtrip.snapshot();
    let ptc = m.proposed_to_committed.snapshot();
    let pickup = m.writer_pickup.snapshot();
    let fsync = m.log_fsync.snapshot();
    let ae = m.apply_entry.snapshot();
    let seg = m.apply_segment.snapshot();
    let sc = m.store_commit.snapshot();
    let dp = m.durable_point.snapshot();
    let depth = m.apply_channel_depth.snapshot();
    let popr = m.pop_read.snapshot();
    // PERF-J: the push-only HTTP boundary.
    let pht = m.push_h_total.snapshot();
    let php = m.push_h_prep.snapshot();
    let phs = m.push_h_submit.snapshot();
    let pha = m.push_h_await.snapshot();
    let poh = m.pop_h_total.snapshot();

    tracing::info!(
        target: "rsm",
        // PERF-J push boundary, p50/p99 in microseconds (push-only)
        push_total_us_p50 = us(pht.p50), push_total_us_p99 = us(pht.p99),
        push_prep_us_p50 = us(php.p50), push_prep_us_p99 = us(php.p99),
        push_submit_us_p50 = us(phs.p50), push_submit_us_p99 = us(phs.p99),
        push_await_us_p50 = us(pha.p50), push_await_us_p99 = us(pha.p99),
        pop_total_us_p50 = us(poh.p50), pop_total_us_p99 = us(poh.p99),
        // stages, p50/p99 in microseconds
        plan_us_p50 = us(plan.p50), plan_us_p99 = us(plan.p99),
        arrival_to_proposed_us_p50 = us(a2p.p50), arrival_to_proposed_us_p99 = us(a2p.p99),
        queue_wait_us_p50 = us(qw.p50), queue_wait_us_p99 = us(qw.p99),
        drain_to_propose_us_p50 = us(d2p.p50), drain_to_propose_us_p99 = us(d2p.p99),
        roundtrip_us_p50 = us(rt.p50), roundtrip_us_p99 = us(rt.p99),
        proposed_to_committed_us_p50 = us(ptc.p50), proposed_to_committed_us_p99 = us(ptc.p99),
        writer_pickup_us_p50 = us(pickup.p50), writer_pickup_us_p99 = us(pickup.p99),
        log_fsync_us_p50 = us(fsync.p50), log_fsync_us_p99 = us(fsync.p99),
        apply_us_p50 = us(ae.p50), apply_us_p99 = us(ae.p99),
        apply_seg_us_p50 = us(seg.p50), apply_seg_us_p99 = us(seg.p99),
        store_commit_us_p50 = us(sc.p50), store_commit_us_p99 = us(sc.p99),
        durable_us_p50 = us(dp.p50), durable_us_p99 = us(dp.p99),
        pop_read_us_p50 = us(popr.p50), pop_read_us_p99 = us(popr.p99),
        // queue depths
        apply_depth_p50 = depth.p50, apply_depth_p99 = depth.p99,
        // ApplyStats
        entries = stats.entries, appends = stats.appends, messages = stats.messages,
        commits = stats.commits, durable_points = stats.durable_points,
        durable_points_failed = stats.durable_points_failed, wakes = stats.wakes,
        "rsm timing",
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_edges() {
        assert_eq!(bucket_index(0), 0);
        assert_eq!(bucket_index(1), 1);
        assert_eq!(bucket_index(2), 2);
        assert_eq!(bucket_index(3), 2);
        assert_eq!(bucket_index(4), 3);
        assert_eq!(bucket_index(u64::MAX), 64);
        assert_eq!(bucket_upper(0), 0);
        assert_eq!(bucket_upper(1), 1);
        assert_eq!(bucket_upper(2), 3);
        assert_eq!(bucket_upper(3), 7);
    }

    #[test]
    fn quantiles_track_the_bulk() {
        let h = Histogram::default();
        for _ in 0..99 {
            h.record(5); // bucket 3, upper 7
        }
        h.record(1_000_000); // one big outlier
        let s = h.snapshot();
        assert_eq!(s.count, 100);
        assert_eq!(s.p50, 7, "median sits in the small bucket");
        assert_eq!(s.p90, 7);
        assert_eq!(s.max, 1_000_000, "max is exact");
        assert!(
            s.p999 >= 1_000_000 - 1,
            "the tail reaches the outlier bucket"
        );
    }

    #[test]
    fn empty_histogram_is_all_zero() {
        let s = Histogram::default().snapshot();
        assert_eq!(s.count, 0);
        assert_eq!(s.p50, 0);
        assert_eq!(s.max, 0);
    }
}

/// MEASURE: this thread's CPU time, in nanoseconds (`CLOCK_THREAD_CPUTIME_ID`).
pub fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid, writable timespec for the call's duration.
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}
