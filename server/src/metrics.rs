use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
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
    start: Instant,
}

impl Metrics {
    pub fn new() -> Metrics {
        Metrics {
            push: std::sync::Arc::new(OpMetrics::new("push")),
            pop: std::sync::Arc::new(OpMetrics::new("pop")),
            ack: std::sync::Arc::new(OpMetrics::new("ack")),
            start: Instant::now(),
        }
    }

    pub fn prometheus(&self) -> String {
        let mut s = String::with_capacity(2048);
        let g = |s: &mut String, name: &str, labels: &str, v: String| {
            s.push_str(name);
            s.push_str(labels);
            s.push(' ');
            s.push_str(&v);
            s.push('\n');
        };
        g(&mut s, "queen_uptime_seconds", "", (self.start.elapsed().as_secs()).to_string());
        g(&mut s, "queen_process_resident_memory_bytes", "", resident_bytes().to_string());
        g(&mut s, "queen_cluster_push_requests_total", "{scope=\"cluster\"}", self.push.requests.load(Ordering::Relaxed).to_string());
        g(&mut s, "queen_cluster_pop_requests_total", "{scope=\"cluster\"}", self.pop.requests.load(Ordering::Relaxed).to_string());
        g(&mut s, "queen_cluster_ack_requests_total", "{scope=\"cluster\"}", self.ack.requests.load(Ordering::Relaxed).to_string());
        g(&mut s, "queen_cluster_push_messages_total", "{scope=\"cluster\"}", self.push.messages.load(Ordering::Relaxed).to_string());
        g(&mut s, "queen_cluster_pop_messages_total", "{scope=\"cluster\"}", self.pop.messages.load(Ordering::Relaxed).to_string());
        g(&mut s, "queen_cluster_ack_messages_total", "{scope=\"cluster\"}", self.ack.messages.load(Ordering::Relaxed).to_string());
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
