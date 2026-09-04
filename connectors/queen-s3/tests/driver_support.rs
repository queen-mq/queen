//! The fixture the `driver_*` tests share: a whole sink with no network in it.
//!
//! [`FakeQueen`] is the broker (the log, the discovery index, `safeTime` and the
//! key/value store, with 032/033/024's own semantics — see its own test file)
//! and [`MemoryStore`] is the bucket. What is under test is everything between
//! them: [`QueueDriver`], the lease, the writers and the layout.
//!
//! # Why the driver is ticked by hand
//!
//! [`QueueDriver::tick`] is one round of the protocol and these tests call it in
//! a plain loop, on one task, with no spawning and no polling for a condition.
//! That is what makes a crash matrix reproducible: the interleaving is the same
//! on every run and on every machine, so "the object of window 3 is
//! byte-identical" is an assertion about the protocol rather than about a race
//! the test happened to win. `tokio::time::pause` covers the sleeps — an
//! `Action::Idle` costs virtual time and nothing else.
//!
//! This file is included by the other `driver_*.rs` tests through
//! `#[path = "driver_support.rs"] mod support;`, so it is also compiled as a
//! test target of its own with nothing in it — hence the blanket `dead_code`
//! allowance.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use serde_json::Value;

use queen_s3::config::CrashAt;
use queen_s3::driver::{
    CrashMode, DriverConfig, MemoryBudget, QueueDriver, Runtime, Shutdown, Stop,
};
use queen_s3::health::HealthState;
use queen_s3::lease::{Acquired, Lease};
use queen_s3::obs::Metrics;
use queen_s3::queen::FakeQueen;
use queen_s3::s3::MemoryStore;
use queen_s3::types::{Align, Layout, Manifest, Micros, Start};
use queen_s3::window::{EngineConfig, EngineState};
use queen_s3::writer::{factory, WriterConfig, WriterFactory};

/// A broker timestamp, from the spelling the broker uses.
pub fn t(iso: &str) -> Micros {
    Micros::parse_iso(iso).unwrap_or_else(|e| panic!("{iso}: {e}"))
}

/// The test defaults.
///
/// Two of them are not the product's and both are deliberate. `safe_guard` is
/// zero because [`FakeQueen`]'s derived `safeTime` is one microsecond past the
/// newest pushed segment — the real broker's is `now − 5 s` over a log that is
/// seconds old — so the product default would put every record above the
/// ceiling and nothing would ever close. `backoff_base_ms` is 1 because the
/// backoff's shape is pinned by its own unit test and a test that waited out a
/// real one would be measuring `tokio::time`.
pub fn test_cfg() -> DriverConfig {
    DriverConfig {
        sink: "default".to_string(),
        prefix: "queen".to_string(),
        layout: Layout::Merged,
        align: Align::Hour,
        engine: EngineConfig {
            safe_guard: Micros(0),
            start: Start::Earliest,
            checkpoint_every: 2,
            discovery_interval_ms: 50,
            ..EngineConfig::default()
        },
        crash_at: CrashAt::Never,
        // The in-process matrix: a crash point returns from the task instead of
        // aborting the test binary.
        crash_mode: CrashMode::Return,
        seek_probe_records: 1,
        seek_probe_attempts: 2,
        backoff_base_ms: 1,
        backoff_max_ms: 4,
        checkpoint_attempts: 2,
        drain_min_bytes: 1024 * 1024,
        drain_min_age: Micros::from_millis(10_000),
    }
}

/// The same configuration with one alignment, kept consistent between the
/// driver (which builds keys) and the engine (which closes windows) — a pair
/// that disagreed would put a window in the wrong Hive bucket.
pub fn with_align(mut cfg: DriverConfig, align: Align) -> DriverConfig {
    cfg.align = align;
    cfg.engine.align = align;
    cfg
}

/// One process: a Queen, a bucket, a metric registry and a budget.
pub struct Rig {
    pub queen: Arc<FakeQueen>,
    pub store: Arc<MemoryStore>,
    pub metrics: Arc<Metrics>,
    pub health: Arc<HealthState>,
    pub budget: Arc<MemoryBudget>,
    pub writers: Arc<dyn WriterFactory>,
    pub cfg: DriverConfig,
    pub shutdown: Shutdown,
}

impl Rig {
    pub fn new(cfg: DriverConfig) -> Rig {
        Rig::with_writer(cfg, WriterConfig::default())
    }

    pub fn with_writer(cfg: DriverConfig, wcfg: WriterConfig) -> Rig {
        let metrics = Arc::new(Metrics::new());
        Rig {
            queen: Arc::new(FakeQueen::new()),
            store: Arc::new(MemoryStore::new()),
            health: Arc::new(HealthState::new(
                metrics.clone(),
                cfg.engine.max_window.0 as u64,
            )),
            metrics,
            budget: Arc::new(MemoryBudget::new(1024 * 1024 * 1024)),
            writers: Arc::from(factory(&wcfg)),
            cfg,
            shutdown: Shutdown::new(),
        }
    }

    /// A global budget small enough to force early closes (plan §4.4).
    pub fn with_budget(mut self, limit: usize) -> Rig {
        self.budget = Arc::new(MemoryBudget::new(limit));
        self
    }

    /// A fresh stop signal for the drivers built AFTER this call — how a test
    /// restarts a process that was shut down, without the new one draining on
    /// the old one's signal.
    pub fn rearm_shutdown(&mut self) {
        self.shutdown = Shutdown::new();
    }

    pub fn runtime(&self) -> Runtime {
        self.runtime_with(self.cfg.clone())
    }

    pub fn runtime_with(&self, cfg: DriverConfig) -> Runtime {
        Runtime {
            cfg: Arc::new(cfg),
            queen: self.queen.clone(),
            store: self.store.clone(),
            writers: self.writers.clone(),
            metrics: self.metrics.clone(),
            health: self.health.clone(),
            budget: self.budget.clone(),
            shutdown: self.shutdown.clone(),
        }
    }

    /// Take the queue's lease, and insist on it.
    pub async fn own(&self, queue: &str, instance: &str) -> Arc<Lease> {
        let lease = Arc::new(Lease::new(
            self.queen.clone(),
            &self.cfg.sink,
            queue,
            instance,
            30_000,
        ));
        assert_eq!(
            lease.acquire().await.expect("the claim must be answered"),
            Acquired::Taken,
            "{instance} must own {queue} for this test"
        );
        lease
    }

    /// A driver holding a fresh lease.
    pub async fn driver(&self, queue: &str) -> QueueDriver {
        let lease = self.own(queue, "test-a").await;
        QueueDriver::new(self.runtime(), queue.to_string(), lease)
    }

    /// A driver on an existing lease — a RESTART of the same instance over the
    /// same Queen and the same bucket, which is what the crash matrix is.
    pub fn restart(&self, cfg: DriverConfig, queue: &str, lease: Arc<Lease>) -> QueueDriver {
        QueueDriver::new(self.runtime_with(cfg), queue.to_string(), lease)
    }
}

/// Tick until `done`, or panic with the engine's state. Never silently short.
pub async fn run_until(
    d: &mut QueueDriver,
    max_ticks: usize,
    done: impl Fn(&QueueDriver) -> bool,
) -> Option<Stop> {
    for _ in 0..max_ticks {
        if done(d) {
            return None;
        }
        if let Some(stop) = d.tick().await {
            return Some(stop);
        }
    }
    if !done(d) {
        panic!(
            "ran out of ticks: state={:?} committed_k={} buffered={} t_prev={:?}",
            d.engine().state(),
            d.engine().committed_k(),
            d.engine().buffered_bytes(),
            d.engine().t_prev()
        );
    }
    None
}

/// Tick until the queue has been quiet for `quiet` rounds with no window in
/// flight — the in-process reading of "it has shipped everything it can".
pub async fn run_until_quiet(d: &mut QueueDriver, max_ticks: usize, quiet: usize) -> Option<Stop> {
    let mut last = d.engine().committed_k();
    let mut still = 0usize;
    for _ in 0..max_ticks {
        if let Some(stop) = d.tick().await {
            return Some(stop);
        }
        let now = d.engine().committed_k();
        if now == last {
            still += 1;
        } else {
            still = 0;
            last = now;
        }
        if still >= quiet && d.engine().state() == EngineState::Filling {
            return None;
        }
    }
    panic!(
        "ran out of ticks: state={:?} committed_k={} buffered={}",
        d.engine().state(),
        d.engine().committed_k(),
        d.engine().buffered_bytes()
    );
}

// ---------------------------------------------------------------------------
// Reading the bucket back
// ---------------------------------------------------------------------------

/// One row as a reader sees it, whatever the format wrote it.
///
/// `queue` is NOT a field of the record: it is the `queue=<esc>` key of the
/// object's path, and this reader recovers it from the key exactly as a reader
/// with `hive_partitioning = true` does (plan §6.3).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Row {
    pub queue: String,
    pub partition: String,
    pub offset: i64,
    pub transaction_id: String,
    pub ts: String,
    pub payload: Option<String>,
}

/// Every data object, in key order — which is window order, because `k` is
/// zero-padded and leads (plan §6.3).
pub fn data_keys(store: &MemoryStore) -> Vec<String> {
    let mut keys: Vec<String> = store
        .keys()
        .into_iter()
        .filter(|k| !k.contains("/_queen/"))
        .collect();
    keys.sort();
    keys
}

pub fn manifest_keys(store: &MemoryStore) -> Vec<String> {
    let mut keys: Vec<String> = store
        .keys()
        .into_iter()
        .filter(|k| k.contains("/windows/"))
        .collect();
    keys.sort();
    keys
}

pub fn checkpoint_keys(store: &MemoryStore) -> Vec<String> {
    let mut keys: Vec<String> = store
        .keys()
        .into_iter()
        .filter(|k| k.contains("/checkpoint/"))
        .collect();
    keys.sort();
    keys
}

pub fn manifests(store: &MemoryStore) -> Vec<Manifest> {
    let mut out: Vec<Manifest> = manifest_keys(store)
        .into_iter()
        .map(|k| {
            let bytes = store.bytes_of(&k).expect("a listed manifest must be there");
            serde_json::from_slice::<Manifest>(&bytes)
                .unwrap_or_else(|e| panic!("manifest {k} does not parse: {e}"))
        })
        .collect();
    out.sort_by_key(|m| m.k);
    out
}

pub fn bytes_of(store: &MemoryStore, key: &str) -> Bytes {
    store
        .bytes_of(key)
        .unwrap_or_else(|| panic!("no object at {key}"))
}

/// Decode one data object. The extension decides, exactly as a reader's glob
/// would.
pub fn rows_of(store: &MemoryStore, key: &str) -> Vec<Row> {
    let raw = bytes_of(store, key);
    let queue = queue_of_key(key);
    if key.ends_with(".parquet") {
        return parquet_rows(&raw, &queue);
    }
    let text = if key.ends_with(".jsonl.zst") {
        zstd::stream::decode_all(&raw[..]).expect("the object must be one zstd frame")
    } else if key.ends_with(".jsonl.gz") {
        use std::io::Read;
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(&raw[..])
            .read_to_end(&mut out)
            .expect("the object must be one gzip member");
        out
    } else {
        raw.to_vec()
    };
    String::from_utf8(text)
        .expect("JSONL is UTF-8")
        .lines()
        .filter(|l| !l.is_empty())
        .map(|line| {
            let v: Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("line is not JSON: {e}: {line}"));
            Row {
                queue: queue.clone(),
                partition: v["partition"].as_str().unwrap_or_default().to_string(),
                offset: v["offset"].as_i64().unwrap_or(-1),
                transaction_id: v["transactionId"].as_str().unwrap_or_default().to_string(),
                ts: v["ts"].as_str().unwrap_or_default().to_string(),
                payload: match &v["payload"] {
                    Value::Null => None,
                    other => Some(other.to_string()),
                },
            }
        })
        .collect()
}

/// The `queue=<esc>` key of a data key, unescaped — the hive partition column a
/// reader gets for free, and the only place the queue name is written.
pub fn queue_of_key(key: &str) -> String {
    key.split('/')
        .find_map(|seg| seg.strip_prefix("queue="))
        .map(queen_s3::layout::unescape)
        .unwrap_or_else(|| panic!("no queue= key in {key}"))
}

fn parquet_rows(bytes: &[u8], queue: &str) -> Vec<Row> {
    let reader = SerializedFileReader::new(Bytes::copy_from_slice(bytes)).expect("a Parquet file");
    // The footer names the queue too, so an object lifted out of the layout is
    // still self-describing; here it must AGREE with the path key.
    let kv = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("the envelope markers");
    assert_eq!(kv[1].key, "queen.queue");
    assert_eq!(
        kv[1].value.as_deref(),
        Some(queue),
        "the footer's queue must agree with the queue= path key"
    );
    reader
        .get_row_iter(None)
        .expect("a row iterator")
        .map(|row| {
            let row = row.expect("a row");
            Row {
                queue: queue.to_string(),
                partition: row.get_string(0).expect("partition").clone(),
                offset: row.get_long(1).expect("offset"),
                transaction_id: row.get_string(2).expect("transaction_id").clone(),
                ts: Micros(row.get_timestamp_micros(3).expect("ts")).to_iso(),
                payload: match row.is_null(4).unwrap_or(true) {
                    true => None,
                    false => Some(row.get_string(4).expect("payload").clone()),
                },
            }
        })
        .collect()
}

/// Every row of every data object, in key order then in object order — which is
/// the order a reader that globs the bucket and sorts by key would see.
pub fn all_rows(store: &MemoryStore) -> Vec<Row> {
    data_keys(store)
        .iter()
        .flat_map(|k| rows_of(store, k))
        .collect()
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

/// The lake mirrors the log: every expected `(partition, offset)` exactly once,
/// nothing else, and each object sorted by `(partition, offset)`.
pub fn assert_exactly_once(store: &MemoryStore, expected: &[(String, i64)]) {
    let rows = all_rows(store);
    let mut got: Vec<(String, i64)> = rows
        .iter()
        .map(|r| (r.partition.clone(), r.offset))
        .collect();
    let before = got.len();
    got.sort();
    let mut deduped = got.clone();
    deduped.dedup();
    assert_eq!(
        before,
        deduped.len(),
        "a record appears more than once across the objects"
    );
    let mut want: Vec<(String, i64)> = expected.to_vec();
    want.sort();
    assert_eq!(deduped, want, "the lake must mirror the log");

    for key in data_keys(store) {
        let rows = rows_of(store, &key);
        let keys: Vec<(String, i64)> = rows
            .iter()
            .map(|r| (r.partition.clone(), r.offset))
            .collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "{key} is not sorted by (partition, offset)");
    }
}

/// Per-partition offsets are contiguous: no gap inside what was shipped.
pub fn assert_no_gaps(store: &MemoryStore) {
    let mut by_partition: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for r in all_rows(store) {
        by_partition.entry(r.partition).or_default().push(r.offset);
    }
    for (p, mut offsets) in by_partition {
        offsets.sort();
        for pair in offsets.windows(2) {
            assert_eq!(
                pair[1],
                pair[0] + 1,
                "partition {p} has a gap between {} and {}",
                pair[0],
                pair[1]
            );
        }
    }
}

/// Windows tile `[T_0, committed.tEnd)`: numbering without a skip, and no
/// overlap or gap in time.
pub fn assert_windows_tile(manifests: &[Manifest]) {
    let mut prev: Option<&Manifest> = None;
    for m in manifests {
        assert!(m.t_start < m.t_end, "window {} is empty in time", m.k);
        if let Some(q) = prev {
            assert_eq!(m.k, q.k + 1, "window numbering skipped at {}", m.k);
            assert_eq!(
                m.t_start, q.t_end,
                "window {} starts at {} where window {} ended at {}",
                m.k, m.t_start, q.k, q.t_end
            );
        }
        prev = Some(m);
    }
}

/// The manifest describes the objects that are actually there, byte for byte.
pub fn assert_manifests_match_objects(store: &MemoryStore) {
    let mut named: BTreeSet<String> = BTreeSet::new();
    for m in manifests(store) {
        let mut records = 0u64;
        let mut bytes = 0u64;
        for obj in &m.objects {
            let raw = bytes_of(store, &obj.key);
            assert_eq!(raw.len() as u64, obj.bytes, "{}: size", obj.key);
            assert_eq!(
                queen_s3::s3::sigv4::sha256_hex(&raw),
                obj.sha256,
                "{}: sha256",
                obj.key
            );
            assert_eq!(
                rows_of(store, &obj.key).len() as u64,
                obj.records,
                "{}: record count",
                obj.key
            );
            records += obj.records;
            bytes += obj.bytes;
            named.insert(obj.key.clone());
        }
        assert_eq!(m.records, records, "manifest {} totals", m.k);
        assert_eq!(m.bytes, bytes, "manifest {} totals", m.k);
        assert!(
            !m.committed_at.is_empty(),
            "manifest {} must carry a committedAt",
            m.k
        );
    }
    let objects: BTreeSet<String> = data_keys(store).into_iter().collect();
    assert_eq!(
        objects, named,
        "every data object must be named by exactly one manifest"
    );
}

/// Every data object's key and bytes, for comparing two runs.
pub fn fingerprint(store: &MemoryStore) -> BTreeMap<String, String> {
    data_keys(store)
        .into_iter()
        .map(|k| {
            let sha = queen_s3::s3::sigv4::sha256_hex(&bytes_of(store, &k));
            (k, sha)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Seeding
// ---------------------------------------------------------------------------

/// A deterministic two-hour log: `parts` partitions, five segments each, laid
/// across the 10:00 and 11:00 buckets. Returns the `(partition, offset)` pairs
/// it pushed, which is what the lake must hold.
pub fn seed_two_hours(queen: &FakeQueen, queue: &str, parts: &[&str]) -> Vec<(String, i64)> {
    let stamps = [
        "2026-09-04T10:05:00.000000Z",
        "2026-09-04T10:15:00.000000Z",
        "2026-09-04T10:45:00.000000Z",
        "2026-09-04T11:05:00.000000Z",
        "2026-09-04T11:20:00.000000Z",
    ];
    let mut expected = Vec::new();
    for (s, stamp) in stamps.iter().enumerate() {
        for p in parts {
            let payloads: Vec<String> = (0..3)
                .map(|i| format!("{{\"seg\":{s},\"i\":{i},\"p\":\"{p}\"}}"))
                .collect();
            let refs: Vec<&str> = payloads.iter().map(String::as_str).collect();
            let base = queen.push(queue, p, t(stamp), &refs);
            for i in 0..refs.len() as i64 {
                expected.push(((*p).to_string(), base + i));
            }
        }
    }
    expected
}
