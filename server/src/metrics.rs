use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// DLQ transitions and DB errors observed on the ack path (worker_metrics parity).
    pub dlq_moved: AtomicU64,
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
// workers, AVG across buckets — see 014_worker_metrics.sql:104-135). Here:
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

impl Metrics {
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
