//! A broker in a hundred lines: the fixture every `engine_*` test drives the
//! window engine against.
//!
//! It is not a mock — it reimplements the parts of the broker the engine's
//! correctness depends on, from the broker's reads
//! (server/src/rsm/facade/real/phase2/reads.rs) rather than from the engine's
//! expectations:
//!
//! * **fetch** (`fetch_once`): records in offset order from the requested
//!   offset, `high = last_offset + 1` and `logStart` reported whether or not
//!   anything came back, `offset < logStart` and `offset > high` both
//!   `OFFSET_OUT_OF_RANGE`, `offset = high` valid and empty, and a per-entry
//!   budget. The simulator's budget is coarser than the broker's — whole
//!   appends, counted in `segment_bytes` — and it adds a per-CALL budget the
//!   broker does not have, so that a later entry of a call can come back empty;
//!   the first append of the call is exempt. Both make answers shorter than the
//!   broker's would be, which is the harder case for the engine, never an
//!   easier one.
//! * **discovery** (`changed_page`, plan §5.1): partitions in creation order
//!   with and without `since`, each with its id, `lastWriteAt` the exact stamp
//!   of the last record, paginated with an opaque cursor. A partition written
//!   after a page went by it is not seen by the rest of that pass, exactly as
//!   on the broker.
//! * **`safeTime`** (plan §5.2): trailing the simulated log clock by
//!   `safe_lag`, which only the test advances.
//!
//! Nothing here reads a real clock either: `Sim::clock` is the log clock and a
//! test moves it by hand.
//!
//! This file is included by the other `engine_*.rs` tests through
//! `#[path = "engine_support.rs"] mod support;`, so it is also compiled as a
//! test target of its own with nothing in it — hence the blanket `dead_code`
//! allowance.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::value::RawValue;

use queen_s3::seek::{Seek, SeekStep};
use queen_s3::types::{
    ChangedEntry, Checkpoint, Committed, FetchError, FetchRequestEntry, FetchedEntry, Intent,
    Micros, PartitionBounds, Record,
};
use queen_s3::window::{Action, Engine, WindowPlan};

// ---------------------------------------------------------------------------
// A seeded LCG — the only source of "randomness" in these tests, so a failure
// reproduces from its seed.
// ---------------------------------------------------------------------------

pub struct Lcg(u64);

impl Lcg {
    pub fn new(seed: u64) -> Lcg {
        Lcg(seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1))
    }
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 11
    }
    /// Uniform-ish in `0..n`. `n == 0` answers 0.
    pub fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next_u64() % n as u64) as usize
        }
    }
    pub fn chance(&mut self, percent: u64) -> bool {
        self.next_u64() % 100 < percent
    }
}

// ---------------------------------------------------------------------------
// The log
// ---------------------------------------------------------------------------

/// One append: a contiguous run of records sharing one stamp.
#[derive(Clone, Debug)]
pub struct Segment {
    pub base: i64,
    pub count: i64,
    pub ts: Micros,
    /// What the fetch budgets are spent in.
    pub bytes: usize,
}

#[derive(Clone, Debug, Default)]
pub struct SimPartition {
    /// New for every incarnation of the name.
    pub id: String,
    /// Creation order: discovery's order, and what its cursor names.
    pub seq: u64,
    pub segments: Vec<Segment>,
    pub last_offset: i64,
    pub log_start: i64,
    pub last_write_at: Micros,
}

pub struct Sim {
    pub queue: String,
    pub parts: BTreeMap<String, SimPartition>,
    /// The log clock. Only a test moves it.
    pub clock: Micros,
    /// How far `safe_time` trails the clock: how far behind the newest stamp
    /// the node's applied state is.
    pub safe_lag: Micros,
    pub degraded: bool,
    pub payload_bytes: usize,
    pub segment_bytes: usize,
    /// Whole-call ceiling over `segment_bytes` — a simulator knob, see the
    /// module header.
    pub call_budget: usize,
    /// Per-entry record ceiling (10 000 on the broker).
    pub max_records_per_entry: usize,
    pub unknown_queue: bool,
    pub fetch_calls: usize,
    pub next_seq: u64,
}

impl Sim {
    pub fn new(queue: &str, start: Micros) -> Sim {
        Sim {
            queue: queue.to_string(),
            parts: BTreeMap::new(),
            clock: start,
            safe_lag: Micros(5 * Micros::SECOND.0),
            degraded: false,
            payload_bytes: 32,
            segment_bytes: 4096,
            call_budget: 64 * 1024 * 1024,
            max_records_per_entry: 10_000,
            unknown_queue: false,
            fetch_calls: 0,
            next_seq: 0,
        }
    }

    pub fn safe_time(&self) -> Micros {
        self.clock.saturating_sub(self.safe_lag)
    }

    /// Move the log clock forward. `safe_time` follows.
    pub fn advance(&mut self, d: Micros) {
        self.clock = self.clock.saturating_add(d);
    }

    /// Push one append of `count` records to `partition` at the current clock.
    pub fn push(&mut self, partition: &str, count: i64) {
        let clock = self.clock;
        let bytes = self.segment_bytes;
        let p = self.entry(partition);
        let base = p.last_offset + 1;
        p.segments.push(Segment {
            base,
            count,
            ts: clock,
            bytes,
        });
        p.last_offset += count;
        // The exact stamp of the last record, as the broker keeps it.
        p.last_write_at = clock;
    }

    /// Create the partition without any records: its `lastWriteAt` is its
    /// creation stamp until it has one.
    pub fn touch(&mut self, partition: &str) {
        self.entry(partition);
    }

    /// Delete the partition, the way retention deletes one that has been idle
    /// and empty for `PARTITION_CLEANUP_DAYS`, or with its queue. The next push
    /// to the name creates a new incarnation: a new id, offsets from 0, and the
    /// last place in creation order.
    pub fn delete(&mut self, partition: &str) {
        self.parts.remove(partition);
    }

    /// The partition's current id.
    pub fn id(&self, partition: &str) -> String {
        self.parts[partition].id.clone()
    }

    fn entry(&mut self, partition: &str) -> &mut SimPartition {
        let clock = self.clock;
        if !self.parts.contains_key(partition) {
            let seq = self.next_seq;
            self.next_seq += 1;
            self.parts.insert(
                partition.to_string(),
                SimPartition {
                    id: format!("00000000-0000-4000-8000-{seq:012x}"),
                    seq,
                    segments: Vec::new(),
                    last_offset: -1,
                    log_start: 0,
                    last_write_at: clock,
                },
            );
        }
        self.parts.get_mut(partition).expect("just inserted")
    }

    /// Retention: delete every segment wholly below `new_log_start`.
    pub fn retain_from(&mut self, partition: &str, new_log_start: i64) {
        if let Some(p) = self.parts.get_mut(partition) {
            p.segments.retain(|s| s.base + s.count > new_log_start);
            p.log_start = p.log_start.max(new_log_start);
        }
    }

    pub fn bounds(&self, partition: &str) -> (i64, i64) {
        match self.parts.get(partition) {
            Some(p) => (p.last_offset, p.log_start),
            None => (-1, 0),
        }
    }

    /// Every record the log still holds, in `(partition, offset)` order — the
    /// ground truth the tests compare a run's committed windows against.
    pub fn all_records(&self) -> Vec<Record> {
        let mut out = Vec::new();
        for (name, p) in &self.parts {
            for s in &p.segments {
                for i in 0..s.count {
                    let offset = s.base + i;
                    if offset < p.log_start {
                        continue;
                    }
                    out.push(self.record(name, offset, s.ts));
                }
            }
        }
        out
    }

    pub fn record(&self, partition: &str, offset: i64, ts: Micros) -> Record {
        let pad = "x".repeat(self.payload_bytes);
        Record {
            partition: Arc::from(partition),
            offset,
            transaction_id: format!("{partition}#{offset}"),
            ts,
            payload: Some(
                RawValue::from_string(format!("{{\"o\":{offset},\"pad\":\"{pad}\"}}")).unwrap(),
            ),
        }
    }

    /// `POST /api/v1/fetch` (reads.rs `fetch_once`, with the simulator's
    /// coarser budgets).
    pub fn fetch(&mut self, entries: &[FetchRequestEntry]) -> Vec<FetchedEntry> {
        self.fetch_calls += 1;
        let mut budget = self.call_budget as i64;
        let mut any = false;
        let mut out = Vec::with_capacity(entries.len());
        for e in entries {
            if self.unknown_queue || e.queue != self.queue {
                out.push(FetchedEntry {
                    queue: e.queue.clone(),
                    partition: e.partition.clone(),
                    records: Vec::new(),
                    high_watermark: 0,
                    log_start_offset: 0,
                    error: Some(FetchError::UnknownTopicOrPartition),
                });
                continue;
            }
            let (last, start) = self.bounds(&e.partition);
            let high = last + 1;
            if e.offset < start || e.offset > high {
                out.push(FetchedEntry {
                    queue: e.queue.clone(),
                    partition: e.partition.clone(),
                    records: Vec::new(),
                    high_watermark: high,
                    log_start_offset: start,
                    error: Some(FetchError::OffsetOutOfRange),
                });
                continue;
            }
            let mut records = Vec::new();
            if e.offset <= last {
                let p = self
                    .parts
                    .get(&*e.partition)
                    .expect("bounds said it exists");
                let maxb = e.max_bytes.unwrap_or(1024 * 1024).max(1);
                let mut taken: i64 = 0;
                let mut bytes: i64 = 0;
                for s in &p.segments {
                    if s.base + s.count - 1 < e.offset {
                        continue;
                    }
                    // The one exemption is the first append of the CALL, never
                    // of each entry: stricter than the broker, which exempts
                    // the first record of every entry.
                    if (taken > 0 || any)
                        && (bytes >= maxb
                            || budget <= 0
                            || taken >= self.max_records_per_entry as i64)
                    {
                        break;
                    }
                    let start_idx = (e.offset - s.base).max(0);
                    let mut take = s.count - start_idx;
                    if taken + take > self.max_records_per_entry as i64 {
                        take = self.max_records_per_entry as i64 - taken;
                    }
                    if take <= 0 {
                        continue;
                    }
                    for i in 0..take {
                        records.push(self.record(&e.partition, s.base + start_idx + i, s.ts));
                    }
                    taken += take;
                    bytes += s.bytes as i64;
                    budget -= s.bytes as i64;
                    any = true;
                }
            }
            out.push(FetchedEntry {
                queue: e.queue.clone(),
                partition: e.partition.clone(),
                records,
                high_watermark: high,
                log_start_offset: start,
                error: None,
            });
        }
        out
    }

    /// `POST /api/v1/partitions/changed` (reads.rs `changed_page`, plan
    /// §5.1): creation order, `since` keeping `lastWriteAt >= since`, a cursor
    /// naming the page's last partition, issued exactly when the page is full.
    pub fn changed(
        &self,
        since: Option<Micros>,
        after: Option<&str>,
        limit: usize,
    ) -> ChangedEntry {
        let refuse = |error: &str| ChangedEntry {
            queue: self.queue.clone(),
            partitions: Vec::new(),
            next: None,
            error: Some(error.into()),
        };
        if self.unknown_queue {
            return refuse("UNKNOWN_TOPIC_OR_PARTITION");
        }
        let after: Option<u64> = match after {
            None | Some("") => None,
            Some(c) => match c.strip_prefix("p|").and_then(|s| s.parse().ok()) {
                Some(seq) => Some(seq),
                None => return refuse("BAD_CURSOR"),
            },
        };
        let mut rows: Vec<(&String, &SimPartition)> = self
            .parts
            .iter()
            .filter(|(_, p)| after.is_none_or(|a| p.seq > a))
            .filter(|(_, p)| since.is_none_or(|t| p.last_write_at >= t))
            .collect();
        rows.sort_by_key(|(_, p)| p.seq);
        rows.truncate(limit.max(1));
        let next = match (rows.len() >= limit.max(1), rows.last()) {
            (true, Some((_, p))) => Some(format!("p|{}", p.seq)),
            _ => None,
        };
        ChangedEntry {
            queue: self.queue.clone(),
            partitions: rows
                .into_iter()
                .map(|(n, p)| PartitionBounds {
                    name: Arc::from(n.as_str()),
                    id: Some(p.id.clone()),
                    last_offset: p.last_offset,
                    log_start: p.log_start,
                    last_write_at: Some(p.last_write_at),
                })
                .collect(),
            next,
            error: None,
        }
    }
}

// ---------------------------------------------------------------------------
// The driver
// ---------------------------------------------------------------------------

/// What one [`Driver`] run observed. Assertions live in the tests; this is the
/// evidence.
#[derive(Default)]
pub struct Trace {
    pub intents: Vec<Intent>,
    pub uploads: Vec<WindowPlan>,
    pub committed: Vec<WindowPlan>,
    pub checkpoints: Vec<Checkpoint>,
    /// The largest number of entries any one `Fetch` action carried.
    pub max_fetch_entries: usize,
    /// The largest number of `Fetch` actions in flight at once.
    pub max_calls_in_flight: usize,
    /// The largest `Engine::buffered_bytes` ever observed.
    pub peak_buffered: usize,
    /// Probes each executed seek needed.
    pub seek_probes: Vec<u32>,
    pub idles: usize,
    /// `(T_k, safeTime the engine held at the close)` for every closed window —
    /// the invariant of rule 1 is checked over this.
    pub closes: Vec<(Micros, Micros)>,
}

pub struct Driver {
    pub engine: Engine,
    pub sim: Sim,
    pub trace: Trace,
    pub discovery_limit: usize,
    pub safe_guard: Micros,
    /// Stop the run as soon as an intent has been written — the crash of plan
    /// §4.3's "between 4 and 6".
    pub stop_after_intent: bool,
    pub stopped: bool,
    /// What an `Idle` costs on the simulated log clock.
    pub idle_advance: Micros,
    calls_in_flight: usize,
}

impl Driver {
    pub fn new(engine: Engine, sim: Sim, safe_guard: Micros) -> Driver {
        Driver {
            engine,
            sim,
            trace: Trace::default(),
            discovery_limit: 1000,
            safe_guard,
            stop_after_intent: false,
            stopped: false,
            idle_advance: Micros(Micros::SECOND.0),
            calls_in_flight: 0,
        }
    }

    /// One tick: ask the engine what to do, then do all of it, in order.
    pub fn tick(&mut self) {
        let actions = self.engine.next_actions();
        self.observe(&actions);
        for a in actions {
            if self.stopped {
                return;
            }
            self.execute(a);
        }
        self.trace.peak_buffered = self.trace.peak_buffered.max(self.engine.buffered_bytes());
    }

    /// Tick `n` times, whatever happens. For "let it run and prove nothing
    /// happened" assertions.
    pub fn run(&mut self, n: usize) {
        for _ in 0..n {
            if self.stopped {
                return;
            }
            self.tick();
        }
    }

    /// Tick until `done` or `max_ticks`. Panics if it runs out of ticks, because
    /// a test that silently stops short proves nothing.
    pub fn run_until(&mut self, max_ticks: usize, done: impl Fn(&Driver) -> bool) {
        for _ in 0..max_ticks {
            if self.stopped || done(self) {
                return;
            }
            self.tick();
        }
        if !done(self) && !self.stopped {
            panic!(
                "ran out of ticks: state={:?} committed={} buffered={} t_prev={:?}",
                self.engine.state(),
                self.engine.committed_k(),
                self.engine.buffered_bytes(),
                self.engine.t_prev()
            );
        }
    }

    fn observe(&mut self, actions: &[Action]) {
        let mut calls = 0;
        for a in actions {
            if let Action::Fetch(entries) = a {
                calls += 1;
                self.trace.max_fetch_entries = self.trace.max_fetch_entries.max(entries.len());
            }
        }
        self.calls_in_flight = calls;
        self.trace.max_calls_in_flight = self.trace.max_calls_in_flight.max(calls);
    }

    pub fn execute(&mut self, a: Action) {
        match a {
            Action::Discover { since, after } => {
                let page = self
                    .sim
                    .changed(since, after.as_deref(), self.discovery_limit);
                let safe = self.sim.safe_time();
                let degraded = self.sim.degraded;
                self.engine.on_discovery(&page, safe, degraded);
            }
            Action::Fetch(entries) => {
                let answer = self.sim.fetch(&entries);
                self.engine.on_fetch(answer);
                self.trace.peak_buffered =
                    self.trace.peak_buffered.max(self.engine.buffered_bytes());
            }
            Action::Seek {
                partition,
                t,
                last_offset,
                log_start,
            } => {
                let mut s = Seek::new(
                    self.sim.queue.clone(),
                    partition.clone(),
                    t,
                    last_offset,
                    log_start,
                    1,
                );
                let answer = loop {
                    match s.step() {
                        SeekStep::Found(o) => break Some(o),
                        SeekStep::Failed(_) => break None,
                        SeekStep::Continue => {}
                    }
                    let Some(probe) = s.next_probe() else {
                        break None;
                    };
                    let entry = self.sim.fetch(&[probe]).remove(0);
                    s.on_result(&entry);
                };
                self.trace.seek_probes.push(s.probes());
                match answer {
                    Some(o) => self.engine.on_seek_result(&partition, o),
                    None => self.engine.on_seek_failed(&partition),
                }
            }
            Action::WriteIntent(intent) => {
                let safe = self.engine.safe_time().expect("a close needs a safeTime");
                self.trace.closes.push((intent.t_end, safe));
                self.trace.intents.push(intent.clone());
                if self.stop_after_intent {
                    self.stopped = true;
                    return;
                }
                self.engine.on_intent_written(intent.k);
            }
            Action::Upload(plan) => {
                let safe = self.engine.safe_time().expect("a close needs a safeTime");
                if self.trace.closes.last().map(|c| c.0) != Some(plan.t_end) {
                    self.trace.closes.push((plan.t_end, safe));
                }
                let k = plan.k;
                let records = plan.records.len() as u64;
                let bytes = plan.bytes_estimate as u64;
                self.trace.uploads.push(plan);
                self.engine
                    .on_uploaded(k, format!("_queen/windows/{k:010}.json"), records, bytes);
            }
            Action::Commit(c) => {
                let plan = self
                    .trace
                    .uploads
                    .iter()
                    .rev()
                    .find(|p| p.k == c.k)
                    .cloned()
                    .expect("a commit follows its upload");
                self.trace.committed.push(plan);
                self.engine.on_committed(c.k);
            }
            Action::Checkpoint(cp) => {
                self.engine.on_checkpointed(cp.k);
                self.trace.checkpoints.push(cp);
            }
            Action::Idle { .. } => {
                self.trace.idles += 1;
                let d = self.idle_advance;
                self.sim.advance(d);
            }
        }
    }

    /// Every record every committed window shipped, in commit order.
    pub fn shipped(&self) -> Vec<Record> {
        self.trace
            .committed
            .iter()
            .flat_map(|p| p.records.iter().cloned())
            .collect()
    }

    /// The commit pointer the KV store would hold — what a restart restores.
    pub fn committed_pointer(&self) -> Option<Committed> {
        self.trace.committed.last().map(|p| Committed {
            k: p.k,
            t_end: p.t_end,
            manifest: format!("_queen/windows/{:010}.json", p.k),
            records: p.records.len() as u64,
            bytes: p.bytes_estimate as u64,
            committed_at_ms: 0,
        })
    }
}

/// Assert that a run shipped exactly the records the log holds below the last
/// committed `t_end`: no gap, no duplicate, in `(partition, offset)` order
/// within each window.
pub fn assert_exactly_once(driver: &Driver) {
    let Some(last) = driver.trace.committed.last() else {
        assert!(driver.shipped().is_empty());
        return;
    };
    let t_end = last.t_end;
    let first_start = driver.trace.committed[0].t_start;

    let mut expected: Vec<(String, i64)> = driver
        .sim
        .all_records()
        .into_iter()
        .filter(|r| r.ts >= first_start && r.ts < t_end)
        .map(|r| (r.partition.to_string(), r.offset))
        .collect();
    expected.sort();

    let mut got: Vec<(String, i64)> = driver
        .shipped()
        .into_iter()
        .map(|r| (r.partition.to_string(), r.offset))
        .collect();
    let before = got.len();
    got.sort();
    got.dedup();
    assert_eq!(before, got.len(), "a record was shipped twice");
    assert_eq!(got, expected, "the lake must mirror the log below tEnd");

    for p in &driver.trace.committed {
        let keys: Vec<(&str, i64)> = p
            .records
            .iter()
            .map(|r| (r.partition.as_ref(), r.offset))
            .collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "window {} is not sorted", p.k);
        for r in &p.records {
            assert!(
                r.ts >= p.t_start && r.ts < p.t_end,
                "record outside window {}",
                p.k
            );
        }
    }
}

/// Assert that consecutive committed windows tile without overlap.
pub fn assert_tiling(driver: &Driver) {
    let mut prev: Option<&WindowPlan> = None;
    for p in &driver.trace.committed {
        if let Some(q) = prev {
            assert_eq!(p.k, q.k + 1, "window numbering skipped");
            assert!(
                p.t_start >= q.t_end,
                "window {} starts at {} before window {} ended at {}",
                p.k,
                p.t_start,
                q.k,
                q.t_end
            );
        }
        assert!(p.t_start < p.t_end, "window {} is empty in time", p.k);
        prev = Some(p);
    }
}
