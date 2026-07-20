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
    /// RUSTFIX item 24: per-queue throughput, flushed into queen.queue_lag_metrics.
    pub per_queue: PerQueue,
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
/// Only fields the broker can attribute to a queue CHEAPLY on the hot path are
/// tracked here — the queue name is in scope at push and at keyed pop. Deliberately
/// NOT tracked (left 0 in the bucket): ack_* (the ack wire is keyed by partitionId,
/// with no queue), pop lag (needs per-message age), wildcard-discover pops (span
/// queues), and parked_count (a gauge). See syscollect.rs for the flush.
#[derive(Default)]
pub struct QueueCounters {
    pub push_requests: AtomicU64,
    pub push_messages: AtomicU64,
    pub pop_count: AtomicU64,
    pub pop_empty: AtomicU64,
    pub transactions: AtomicU64,
}

/// A per-queue snapshot the collector diffs into per-minute deltas.
#[derive(Clone, Copy, Default)]
pub struct QueueSnap {
    pub push_requests: u64,
    pub push_messages: u64,
    pub pop_count: u64,
    pub pop_empty: u64,
    pub transactions: u64,
}

#[derive(Default)]
pub struct PerQueue {
    map: RwLock<HashMap<String, Arc<QueueCounters>>>,
}

impl PerQueue {
    /// Fast path: a read lock + Arc clone for an already-seen queue; the write lock
    /// is taken only the first time a queue is observed (bounded by #queues).
    fn counters(&self, queue: &str) -> Arc<QueueCounters> {
        if let Some(c) = self.map.read().unwrap().get(queue) {
            return c.clone();
        }
        self.map.write().unwrap().entry(queue.to_string()).or_default().clone()
    }
    pub fn add_push(&self, queue: &str, msgs: u64) {
        let c = self.counters(queue);
        c.push_requests.fetch_add(1, Ordering::Relaxed);
        c.push_messages.fetch_add(msgs, Ordering::Relaxed);
    }
    pub fn add_pop(&self, queue: &str, msgs: u64) {
        self.counters(queue).pop_count.fetch_add(msgs, Ordering::Relaxed);
    }
    pub fn add_pop_empty(&self, queue: &str) {
        self.counters(queue).pop_empty.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add_transaction(&self, queue: &str) {
        self.counters(queue).transactions.fetch_add(1, Ordering::Relaxed);
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
                    },
                )
            })
            .collect()
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
            per_queue: PerQueue::default(),
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
