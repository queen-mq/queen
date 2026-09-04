//! Observability: the windowed-log [`Sampler`] the broker and both facades
//! share, and the metric registry `/metrics` renders (plan §6.8).
//!
//! Two rules, both inherited rather than invented:
//!
//! * **Rate and sizes, aggregated over a window** (server/src/obs.rs, and
//!   protocols/queen-kafka/src/obs.rs, from which [`Sampler`] is copied
//!   verbatim). The sink's noisiest lines are the ones a misconfigured bucket
//!   or a stalled broker controls — one line per failed PUT at 800 ops/s is an
//!   amplifier, not a diagnosis.
//! * **No label a user chooses without a bound.** The label set of every series
//!   below is fixed by the plan: `queue`, `format`, `result`, `op`, `code`. A
//!   `partition` label would be one series per entity, which is the cardinality
//!   the whole connector exists to survive.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Sampler — copied verbatim from protocols/queen-kafka/src/obs.rs.
// ---------------------------------------------------------------------------

/// A wall-clock window gate: at most one emit per `interval_ms` per instance,
/// process-wide, chosen by a CAS so exactly one thread wins. Returns the number
/// of events suppressed since the last emit, so the line that is printed says
/// how many it stands for.
///
/// ```ignore
/// static UPLOAD_FAIL: Sampler = Sampler::new(10_000);
/// if let Some(suppressed) = UPLOAD_FAIL.tick_now() {
///     warn!(target: "queen-s3", suppressed, "object upload failed");
/// }
/// ```
pub struct Sampler {
    last_ms: AtomicI64,
    interval_ms: i64,
    suppressed: AtomicU64,
}

impl Sampler {
    pub const fn new(interval_ms: i64) -> Sampler {
        Sampler {
            last_ms: AtomicI64::new(0),
            interval_ms,
            suppressed: AtomicU64::new(0),
        }
    }

    /// `Some(suppressed_since_last)` when it is this caller's turn to emit;
    /// `None` otherwise, having counted this call as suppressed.
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

    /// [`Sampler::tick`] against the process clock.
    pub fn tick_now(&self) -> Option<u64> {
        self.tick(now_epoch_ms())
    }
}

/// Milliseconds since the epoch, or 0 on a clock before it.
///
/// The sink's own wall clock. It is used for log sampling, for the health
/// verdict and for `committedAt` — NEVER for a window boundary, which comes
/// from PostgreSQL's clock alone (plan §12, and the header of
/// [`crate::types::Micros`]).
pub fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// The registry
// ---------------------------------------------------------------------------

/// One series: a metric name plus its label values, in label order. Ordered so
/// the exposition is deterministic — a `/metrics` scrape that reshuffles lines
/// between calls is a diff nobody can read.
type Key = (&'static str, Vec<(&'static str, String)>);

/// The bucket ladder of `queen_s3_window_records`.
const RECORD_BUCKETS: &[f64] = &[
    1.0,
    10.0,
    100.0,
    1_000.0,
    10_000.0,
    100_000.0,
    1_000_000.0,
    10_000_000.0,
];

/// The bucket ladder of `queen_s3_window_bytes`, in bytes: 1 MiB doubling to
/// 1 GiB, which brackets `QUEEN_S3_TARGET_MB` in both directions.
const BYTE_BUCKETS: &[f64] = &[
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    67_108_864.0,
    134_217_728.0,
    268_435_456.0,
    536_870_912.0,
    1_073_741_824.0,
];

/// One histogram's state. Cumulative buckets are computed at render time from
/// the per-bucket counts, so an observation is one index and one add.
#[derive(Clone)]
struct Hist {
    bounds: &'static [f64],
    counts: Vec<u64>,
    sum: f64,
    count: u64,
}

impl Hist {
    fn new(bounds: &'static [f64]) -> Hist {
        Hist {
            bounds,
            counts: vec![0; bounds.len() + 1],
            sum: 0.0,
            count: 0,
        }
    }

    fn observe(&mut self, v: f64) {
        let idx = self
            .bounds
            .iter()
            .position(|b| v <= *b)
            .unwrap_or(self.bounds.len());
        self.counts[idx] += 1;
        self.sum += v;
        self.count += 1;
    }
}

/// What a metric family is, for the `# TYPE` line.
#[derive(Copy, Clone, PartialEq, Eq)]
enum Kind {
    Counter,
    Gauge,
    Histogram,
}

/// Every family this process exposes, in exposition order. The table is the
/// single place a metric NAME is spelled: the typed methods below take their
/// name from these constants, so a series can never be introduced by a typo in
/// one call site.
const FAMILIES: &[(&str, Kind, &str)] = &[
    (
        M_LAG_SECONDS,
        Kind::Gauge,
        "Seconds between now and the last committed window's tEnd — the SLO",
    ),
    (
        M_SAFE_LAG_SECONDS,
        Kind::Gauge,
        "Seconds between now and the broker's safeTime",
    ),
    (
        M_WINDOWS_COMMITTED,
        Kind::Counter,
        "Windows committed to the lake",
    ),
    (
        M_RECORDS_WRITTEN,
        Kind::Counter,
        "Records written into objects",
    ),
    (
        M_BYTES_WRITTEN,
        Kind::Counter,
        "Object bytes written, after compression",
    ),
    (
        M_RECORDS_LOST,
        Kind::Counter,
        "Records retention deleted before the sink read them (plan §4.6)",
    ),
    (M_WINDOW_RECORDS, Kind::Histogram, "Records per window"),
    (M_WINDOW_BYTES, Kind::Histogram, "Object bytes per window"),
    (
        M_BUFFER_BYTES,
        Kind::Gauge,
        "Buffered record bytes across every queue",
    ),
    (M_FETCH_CALLS, Kind::Counter, "POST /api/v1/fetch calls"),
    (
        M_DISCOVERY_PARTITIONS,
        Kind::Gauge,
        "Partitions the last discovery sweep returned",
    ),
    (M_S3_REQUESTS, Kind::Counter, "S3 API requests"),
    (
        M_PRECONDITION_LOST,
        Kind::Counter,
        "KV commit batches a lost precondition rolled back",
    ),
    (
        M_CHECKPOINT_AGE,
        Kind::Gauge,
        "Windows committed since the last position checkpoint",
    ),
];

pub const M_LAG_SECONDS: &str = "queen_s3_lag_seconds";
pub const M_SAFE_LAG_SECONDS: &str = "queen_s3_safe_lag_seconds";
pub const M_WINDOWS_COMMITTED: &str = "queen_s3_windows_committed_total";
pub const M_RECORDS_WRITTEN: &str = "queen_s3_records_written_total";
pub const M_BYTES_WRITTEN: &str = "queen_s3_bytes_written_total";
pub const M_RECORDS_LOST: &str = "queen_s3_records_lost_total";
pub const M_WINDOW_RECORDS: &str = "queen_s3_window_records";
pub const M_WINDOW_BYTES: &str = "queen_s3_window_bytes";
pub const M_BUFFER_BYTES: &str = "queen_s3_buffer_bytes";
pub const M_FETCH_CALLS: &str = "queen_s3_fetch_calls_total";
pub const M_DISCOVERY_PARTITIONS: &str = "queen_s3_discovery_partitions";
pub const M_S3_REQUESTS: &str = "queen_s3_s3_requests_total";
pub const M_PRECONDITION_LOST: &str = "queen_s3_commit_precondition_lost_total";
pub const M_CHECKPOINT_AGE: &str = "queen_s3_checkpoint_age_windows";

/// The process-wide metric registry.
///
/// One `RwLock` per shape rather than an atomic per series, because the series
/// SET is dynamic (a `queue` label is a queue name, learned at discovery) and a
/// scrape is a rare, whole-registry read. The write path takes the lock for the
/// length of one map lookup and one add; at the sink's rates — a handful of
/// events per window per queue, and one per S3 request — that is nothing next
/// to the request it describes.
#[derive(Default)]
pub struct Metrics {
    counters: RwLock<BTreeMap<Key, u64>>,
    gauges: RwLock<BTreeMap<Key, f64>>,
    hists: RwLock<BTreeMap<Key, Hist>>,
}

impl Metrics {
    pub fn new() -> Metrics {
        Metrics::default()
    }

    // ---- writes ----------------------------------------------------------

    fn add(&self, name: &'static str, labels: Vec<(&'static str, String)>, n: u64) {
        *self
            .counters
            .write()
            .expect("metrics counters lock")
            .entry((name, labels))
            .or_insert(0) += n;
    }

    fn set(&self, name: &'static str, labels: Vec<(&'static str, String)>, v: f64) {
        self.gauges
            .write()
            .expect("metrics gauges lock")
            .insert((name, labels), v);
    }

    fn observe(&self, name: &'static str, bounds: &'static [f64], v: f64) {
        self.hists
            .write()
            .expect("metrics histograms lock")
            .entry((name, Vec::new()))
            .or_insert_with(|| Hist::new(bounds))
            .observe(v);
    }

    /// `now − committed.tEnd`, in seconds. The number an operator alarms on
    /// BEFORE a retention overrun happens (plan §4.6).
    pub fn set_lag_seconds(&self, queue: &str, seconds: f64) {
        self.set(M_LAG_SECONDS, vec![("queue", queue.to_string())], seconds);
    }

    /// `now − safeTime`, in seconds: how far behind the broker's own visibility
    /// floor is, which a long read-only transaction moves (plan §5.2).
    pub fn set_safe_lag_seconds(&self, seconds: f64) {
        self.set(M_SAFE_LAG_SECONDS, Vec::new(), seconds);
    }

    pub fn window_committed(&self, queue: &str) {
        self.add(M_WINDOWS_COMMITTED, vec![("queue", queue.to_string())], 1);
    }

    pub fn records_written(&self, queue: &str, n: u64) {
        self.add(M_RECORDS_WRITTEN, vec![("queue", queue.to_string())], n);
    }

    pub fn bytes_written(&self, queue: &str, format: &str, n: u64) {
        self.add(
            M_BYTES_WRITTEN,
            vec![("queue", queue.to_string()), ("format", format.to_string())],
            n,
        );
    }

    pub fn records_lost(&self, queue: &str, n: u64) {
        self.add(M_RECORDS_LOST, vec![("queue", queue.to_string())], n);
    }

    /// One committed window's shape. Unlabelled on purpose: a histogram per
    /// queue is `buckets × queues` series, and the per-queue answer worth
    /// having is the lag gauge, not the size distribution.
    pub fn observe_window(&self, records: u64, bytes: u64) {
        self.observe(M_WINDOW_RECORDS, RECORD_BUCKETS, records as f64);
        self.observe(M_WINDOW_BYTES, BYTE_BUCKETS, bytes as f64);
    }

    pub fn set_buffer_bytes(&self, bytes: u64) {
        self.set(M_BUFFER_BYTES, Vec::new(), bytes as f64);
    }

    /// `result` is `ok`, `empty`, `error` or a per-entry marker — a bounded
    /// vocabulary the caller owns, never a message from the wire.
    pub fn fetch_call(&self, queue: &str, result: &str) {
        self.add(
            M_FETCH_CALLS,
            vec![("queue", queue.to_string()), ("result", result.to_string())],
            1,
        );
    }

    pub fn set_discovery_partitions(&self, queue: &str, n: u64) {
        self.set(
            M_DISCOVERY_PARTITIONS,
            vec![("queue", queue.to_string())],
            n as f64,
        );
    }

    /// `op` is `put`/`get`/`head`/`list`/`delete`/`multipart_*`; `code` is the
    /// HTTP status, or 0 when the request never got one.
    pub fn s3_request(&self, op: &str, code: u16) {
        self.add(
            M_S3_REQUESTS,
            vec![("op", op.to_string()), ("code", code.to_string())],
            1,
        );
    }

    pub fn commit_precondition_lost(&self) {
        self.add(M_PRECONDITION_LOST, Vec::new(), 1);
    }

    pub fn set_checkpoint_age_windows(&self, queue: &str, windows: u64) {
        self.set(
            M_CHECKPOINT_AGE,
            vec![("queue", queue.to_string())],
            windows as f64,
        );
    }

    // ---- reads -----------------------------------------------------------

    /// One counter's value, for tests and for the log line that reports a
    /// window. `0` for a series that has never been touched.
    pub fn counter(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        let want: Vec<(&str, String)> =
            labels.iter().map(|(k, v)| (*k, (*v).to_string())).collect();
        self.counters
            .read()
            .expect("metrics counters lock")
            .iter()
            .find(|((n, l), _)| *n == name && same_labels(l, &want))
            .map(|(_, v)| *v)
            .unwrap_or(0)
    }

    /// One gauge's value, or `None` when it has never been set.
    pub fn gauge(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let want: Vec<(&str, String)> =
            labels.iter().map(|(k, v)| (*k, (*v).to_string())).collect();
        self.gauges
            .read()
            .expect("metrics gauges lock")
            .iter()
            .find(|((n, l), _)| *n == name && same_labels(l, &want))
            .map(|(_, v)| *v)
    }

    /// The whole registry as Prometheus text exposition (version 0.0.4).
    ///
    /// `# HELP` and `# TYPE` are emitted once per family and always BEFORE that
    /// family's samples, which is what the format requires and what a registry
    /// that renders straight out of a map gets wrong.
    pub fn render(&self) -> String {
        let counters = self.counters.read().expect("metrics counters lock");
        let gauges = self.gauges.read().expect("metrics gauges lock");
        let hists = self.hists.read().expect("metrics histograms lock");
        let mut out = String::with_capacity(2048);
        for (name, kind, help) in FAMILIES {
            let type_name = match kind {
                Kind::Counter => "counter",
                Kind::Gauge => "gauge",
                Kind::Histogram => "histogram",
            };
            // The header goes out FIRST, always: a family nobody has touched
            // yet renders its HELP/TYPE and no samples, which is legal
            // exposition and tells a scraper the series exists — the same
            // reason the broker declares its families up front rather than on
            // first use.
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} {type_name}");
            match kind {
                Kind::Counter => {
                    for ((n, labels), v) in counters.iter() {
                        if n == name {
                            let _ = writeln!(out, "{n}{} {v}", render_labels(labels));
                        }
                    }
                }
                Kind::Gauge => {
                    for ((n, labels), v) in gauges.iter() {
                        if n == name {
                            let _ = writeln!(out, "{n}{} {}", render_labels(labels), num(*v));
                        }
                    }
                }
                Kind::Histogram => {
                    for ((n, _), h) in hists.iter() {
                        if n != name {
                            continue;
                        }
                        let mut cum = 0u64;
                        for (i, bound) in h.bounds.iter().enumerate() {
                            cum += h.counts[i];
                            let _ = writeln!(out, "{n}_bucket{{le=\"{}\"}} {cum}", num(*bound));
                        }
                        cum += h.counts[h.bounds.len()];
                        let _ = writeln!(out, "{n}_bucket{{le=\"+Inf\"}} {cum}");
                        let _ = writeln!(out, "{n}_sum {}", num(h.sum));
                        let _ = writeln!(out, "{n}_count {}", h.count);
                    }
                }
            }
        }
        out
    }
}

fn same_labels(have: &[(&'static str, String)], want: &[(&str, String)]) -> bool {
    have.len() == want.len()
        && have
            .iter()
            .zip(want)
            .all(|((hn, hv), (wn, wv))| hn == wn && hv == wv)
}

/// `{a="1",b="2"}`, or the empty string for an unlabelled series.
fn render_labels(labels: &[(&'static str, String)]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let mut out = String::from("{");
    for (i, (name, value)) in labels.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(name);
        out.push_str("=\"");
        out.push_str(&escape_label(value));
        out.push('"');
    }
    out.push('}');
    out
}

/// The exposition format's own escaping: backslash, double quote, newline.
/// A queue name is chosen by whoever creates the queue, so this is the same
/// trust boundary the broker's own renderer sits on.
fn escape_label(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// A float as the exposition format wants it: an integer when it is one, so
/// `queen_s3_buffer_bytes 4096` rather than `4096.0000000001`.
fn num(v: f64) -> String {
    if v.is_finite() && v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_emit_per_window_and_the_rest_are_counted() {
        let s = Sampler::new(1_000);
        assert_eq!(s.tick(10_000), Some(0));
        assert_eq!(s.tick(10_100), None);
        assert_eq!(s.tick(10_999), None);
        assert_eq!(s.tick(11_000), Some(2));
        assert_eq!(s.tick(12_000), Some(0));
    }

    #[test]
    fn a_backwards_clock_does_not_wedge_it() {
        let s = Sampler::new(1_000);
        assert_eq!(s.tick(10_000), Some(0));
        assert_eq!(s.tick(9_000), None, "before the window, still suppressed");
        assert_eq!(s.tick(11_000), Some(1));
    }

    #[test]
    fn counters_accumulate_per_label_set() {
        let m = Metrics::new();
        m.records_written("orders", 10);
        m.records_written("orders", 5);
        m.records_written("clicks", 1);
        assert_eq!(m.counter(M_RECORDS_WRITTEN, &[("queue", "orders")]), 15);
        assert_eq!(m.counter(M_RECORDS_WRITTEN, &[("queue", "clicks")]), 1);
        assert_eq!(m.counter(M_RECORDS_WRITTEN, &[("queue", "nope")]), 0);
    }

    #[test]
    fn gauges_replace_rather_than_add() {
        let m = Metrics::new();
        m.set_lag_seconds("orders", 3.5);
        m.set_lag_seconds("orders", 1.25);
        assert_eq!(m.gauge(M_LAG_SECONDS, &[("queue", "orders")]), Some(1.25));
        assert_eq!(m.gauge(M_LAG_SECONDS, &[("queue", "other")]), None);
    }

    #[test]
    fn exposition_declares_every_family_and_sorts_its_series() {
        let m = Metrics::new();
        m.bytes_written("orders", "jsonl", 100);
        m.bytes_written("clicks", "parquet", 7);
        m.s3_request("put", 200);
        m.commit_precondition_lost();
        m.set_buffer_bytes(4096);
        let text = m.render();
        for (name, _, _) in FAMILIES {
            assert!(
                text.contains(&format!("# TYPE {name} ")),
                "{name} has no TYPE line in:\n{text}"
            );
        }
        let clicks = text
            .find("queen_s3_bytes_written_total{queue=\"clicks\",format=\"parquet\"} 7")
            .expect("clicks series");
        let orders = text
            .find("queen_s3_bytes_written_total{queue=\"orders\",format=\"jsonl\"} 100")
            .expect("orders series");
        assert!(clicks < orders, "series must render in sorted order");
        let type_line = text.find("# TYPE queen_s3_bytes_written_total").unwrap();
        assert!(type_line < clicks, "TYPE must precede its samples");
        assert!(text.contains("queen_s3_s3_requests_total{op=\"put\",code=\"200\"} 1"));
        assert!(text.contains("queen_s3_commit_precondition_lost_total 1"));
        assert!(text.contains("queen_s3_buffer_bytes 4096"));
    }

    #[test]
    fn histograms_render_cumulative_buckets() {
        let m = Metrics::new();
        m.observe_window(5, 2_000_000);
        m.observe_window(500, 2_000_000);
        let text = m.render();
        assert!(
            text.contains("queen_s3_window_records_bucket{le=\"1\"} 0"),
            "{text}"
        );
        assert!(
            text.contains("queen_s3_window_records_bucket{le=\"10\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("queen_s3_window_records_bucket{le=\"1000\"} 2"),
            "{text}"
        );
        assert!(
            text.contains("queen_s3_window_records_bucket{le=\"+Inf\"} 2"),
            "{text}"
        );
        assert!(text.contains("queen_s3_window_records_sum 505"), "{text}");
        assert!(text.contains("queen_s3_window_records_count 2"), "{text}");
        assert!(text.contains("queen_s3_window_bytes_count 2"), "{text}");
    }

    #[test]
    fn label_values_are_escaped() {
        let m = Metrics::new();
        m.records_lost("we\"ird\\name", 1);
        let text = m.render();
        assert!(
            text.contains("queen_s3_records_lost_total{queue=\"we\\\"ird\\\\name\"} 1"),
            "{text}"
        );
    }
}
