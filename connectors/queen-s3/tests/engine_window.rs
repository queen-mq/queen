//! The window engine against a simulated broker: one test per rule of plan §4.
//!
//! Every timestamp in here is on the simulator's PG clock. Nothing reads the
//! host's — a test that did would be testing the wrong thing (plan §12).

#[path = "engine_support.rs"]
mod support;

use std::sync::Arc;

use queen_s3::types::{
    Align, ChangedEntry, Checkpoint, Committed, Compression, FetchError, FetchRequestEntry,
    FetchedEntry, Format, Layout, Micros, PartitionBounds, Start,
};
use queen_s3::window::{Action, Engine, EngineConfig, EngineState, IntentMeta, WindowPlan};

use support::{assert_exactly_once, assert_tiling, Driver, Lcg, Sim};

/// A round PG timestamp to hang every test off.
const T0: Micros = Micros(1_788_480_000_000_000); // 2026-09-04T00:00:00Z
const MS: i64 = 1_000;
const SEC: i64 = 1_000_000;

fn meta() -> IntentMeta {
    IntentMeta {
        writer: "queen-s3/test jsonl+zstd".into(),
        format: Format::Jsonl,
        compression: Compression::Zstd,
        layout: Layout::Merged,
    }
}

/// Small everything, so a test closes windows in a handful of ticks.
fn cfg() -> EngineConfig {
    EngineConfig {
        target_bytes: 100_000,
        max_window: Micros(2 * SEC),
        align: Align::None,
        safe_guard: Micros(10 * MS),
        start: Start::Earliest,
        checkpoint_every: 2,
        fetch_entries_per_call: 8,
        fetch_concurrency: 2,
        max_bytes_per_entry: 1 << 20,
        discovery_slack: Micros(2 * SEC),
        full_sweep_every: 4,
        discovery_limit: 4,
        prune_after_windows: 3,
        discovery_interval_ms: 100,
    }
}

fn driver(cfg: EngineConfig) -> Driver {
    let engine = Engine::new("orders".into(), cfg.clone(), meta());
    let mut sim = Sim::new("orders", T0);
    sim.safe_lag = Micros(100 * MS);
    let mut d = Driver::new(engine, sim, cfg.safe_guard);
    d.discovery_limit = cfg.discovery_limit as usize;
    d.idle_advance = Micros(200 * MS);
    d
}

// ---------------------------------------------------------------------------
// Rule 1 — windows, and the safeTime invariant
// ---------------------------------------------------------------------------

#[test]
fn rule1_a_window_never_closes_above_safe_time_minus_guard() {
    let c = cfg();
    let mut d = driver(c.clone());
    for phase in 0..3 {
        for _ in 0..4 {
            d.sim.push("p1", 3);
            d.sim.push("p2", 2);
            d.sim.advance(Micros(300 * MS));
        }
        d.sim.advance(Micros(3 * SEC));
        d.run_until(500, |d| d.trace.committed.len() > phase);
    }

    assert!(!d.trace.closes.is_empty());
    for (t_end, safe) in &d.trace.closes {
        assert!(
            *t_end <= safe.saturating_sub(c.safe_guard),
            "closed at {t_end} with safeTime {safe} and guard {}",
            c.safe_guard.0
        );
    }
    assert_exactly_once(&d);
    assert_tiling(&d);
}

#[test]
fn rule1_randomized_interleavings_hold_the_invariant_and_lose_nothing() {
    // Pushes, clock advances and action execution in an arbitrary but seeded
    // order: the invariant must survive every one of them.
    for seed in 0..24u64 {
        let mut rng = Lcg::new(seed);
        let c = EngineConfig {
            target_bytes: 3_000,
            max_window: Micros(SEC),
            fetch_concurrency: 3,
            fetch_entries_per_call: 3,
            discovery_limit: 3,
            full_sweep_every: 3,
            ..cfg()
        };
        let mut d = driver(c.clone());
        let names: Vec<String> = (0..7).map(|i| format!("p{i}")).collect();

        let mut queued: Vec<Action> = Vec::new();
        for _ in 0..400 {
            // The world moves whether or not the sink is looking.
            if rng.chance(45) {
                let n = &names[rng.below(names.len())];
                d.sim.push(n, 1 + rng.below(3) as i64);
            }
            if rng.chance(60) {
                d.sim
                    .advance(Micros(20 * MS + (rng.below(120) as i64) * MS));
            }
            queued.extend(d.engine.next_actions());
            // Execute what is queued in an arbitrary order.
            while !queued.is_empty() {
                let i = rng.below(queued.len());
                let a = queued.remove(i);
                if matches!(a, Action::Idle { .. }) && rng.chance(50) {
                    continue; // an Idle the driver simply ignores
                }
                d.execute(a);
                if rng.chance(30) {
                    break; // interleave: go back and move the world again
                }
            }
        }
        // Drain: stop pushing, let the clock run out, finish the commits.
        d.sim.advance(Micros(5 * SEC));
        for _ in 0..400 {
            queued.extend(d.engine.next_actions());
            if queued.is_empty() {
                break;
            }
            while !queued.is_empty() {
                let a = queued.remove(0);
                d.execute(a);
            }
        }

        for (t_end, safe) in &d.trace.closes {
            assert!(
                *t_end <= safe.saturating_sub(c.safe_guard),
                "seed {seed}: closed at {t_end} above safeTime {safe}"
            );
        }
        assert_tiling(&d);
        assert_exactly_once(&d);
        assert!(
            d.trace.committed.len() >= 2,
            "seed {seed}: the run committed only {} windows",
            d.trace.committed.len()
        );
    }
}

// ---------------------------------------------------------------------------
// Rule 2 — the frontier, alignment, size, age, and the empty window
// ---------------------------------------------------------------------------

#[test]
fn rule2_the_frontier_caps_the_window_at_the_slowest_partition() {
    // p2 holds a record the sink has not read yet — because a byte budget of one
    // segment per call means each fetch can only take one — so no window may
    // close above that record's ts.
    let c = EngineConfig {
        max_window: Micros(50 * MS),
        ..cfg()
    };
    let mut d = driver(c);
    d.sim.call_budget = 1; // one segment per call, then the budget is spent
    d.sim.push("p1", 1);
    d.sim.advance(Micros(SEC));
    d.sim.push("p2", 1);
    let late = d.sim.clock;
    d.sim.advance(Micros(SEC));
    d.sim.push("p2", 1);
    d.sim.advance(Micros(3 * SEC));

    d.run_until(200, |d| !d.trace.committed.is_empty());
    let first = &d.trace.committed[0];
    assert!(
        first.t_end <= late.saturating_add(Micros(SEC)),
        "the window ran past the frontier of p2: {} > {}",
        first.t_end,
        late
    );
    assert_exactly_once(&d);
}

#[test]
fn rule2_alignment_never_straddles_an_hour() {
    let c = EngineConfig {
        align: Align::Hour,
        max_window: Micros(10 * SEC),
        start: Start::Earliest,
        ..cfg()
    };
    let mut d = driver(c);
    // Two records either side of an hour boundary.
    d.sim.clock = Micros(T0.0 + 3_500 * SEC); // 00:58:20
    d.sim.push("p1", 2);
    d.sim.advance(Micros(200 * SEC)); // 01:01:40
    d.sim.push("p1", 2);
    d.sim.advance(Micros(30 * SEC));

    d.run_until(400, |d| d.trace.committed.len() >= 2);
    for p in &d.trace.committed {
        assert_eq!(
            p.t_start.floor_to(Micros::HOUR),
            p.t_end.saturating_sub(Micros(1)).floor_to(Micros::HOUR),
            "window {} straddles an hour: [{}, {})",
            p.k,
            p.t_start,
            p.t_end
        );
    }
    assert_tiling(&d);
    assert_exactly_once(&d);
}

#[test]
fn rule2_an_idle_aligned_queue_steps_over_empty_hours_without_an_object() {
    // Five silent hours between two records: nothing may be emitted for them,
    // and the engine must not wedge on the first boundary either.
    let c = EngineConfig {
        align: Align::Hour,
        max_window: Micros(10 * SEC),
        ..cfg()
    };
    let mut d = driver(c);
    d.sim.clock = T0;
    d.sim.push("p1", 1);
    d.sim.advance(Micros(5 * Micros::HOUR.0 + 30 * SEC));
    d.sim.push("p1", 1);
    d.sim.advance(Micros(30 * SEC));

    d.run_until(2_000, |d| d.trace.committed.len() >= 2);
    assert_eq!(
        d.trace.committed.len(),
        2,
        "one window per record-bearing hour"
    );
    assert!(
        d.engine.stats().boundaries_skipped >= 4,
        "empty hours were stepped over"
    );
    for p in &d.trace.committed {
        assert!(!p.records.is_empty(), "an empty window was emitted");
    }
    assert_exactly_once(&d);
}

#[test]
fn rule2_size_closes_at_a_record_timestamp_boundary() {
    let c = EngineConfig {
        target_bytes: 2_000,
        max_window: Micros(10 * SEC),
        ..cfg()
    };
    let mut d = driver(c.clone());
    // Ten segments, each worth well over 2 000 bytes of weight, one per 100 ms.
    for _ in 0..10 {
        d.sim.push("p1", 20);
        d.sim.advance(Micros(100 * MS));
    }
    d.sim.advance(Micros(3 * SEC));
    d.run_until(500, |d| d.trace.committed.len() >= 3);

    for p in &d.trace.committed {
        // Closed on size: about the target, and never a boundary the engine
        // invented — t_end is one of the record timestamps.
        assert!(
            p.bytes_estimate >= c.target_bytes || p.k == d.trace.committed.last().unwrap().k,
            "window {} carried {} bytes, under the {} target",
            p.k,
            p.bytes_estimate,
            c.target_bytes
        );
    }
    let all: Vec<Micros> = d.sim.all_records().iter().map(|r| r.ts).collect();
    for p in &d.trace.committed[..d.trace.committed.len() - 1] {
        assert!(
            all.contains(&p.t_end),
            "window {} ended at {}, which is not a record timestamp",
            p.k,
            p.t_end
        );
    }
    assert_exactly_once(&d);
}

#[test]
fn rule2_age_closes_a_window_that_is_far_under_target() {
    let c = EngineConfig {
        target_bytes: 100 * 1024 * 1024,
        max_window: Micros(500 * MS),
        ..cfg()
    };
    let mut d = driver(c);
    d.sim.push("p1", 1);
    d.sim.advance(Micros(3 * SEC));
    d.run_until(200, |d| !d.trace.committed.is_empty());
    assert_eq!(d.trace.committed[0].records.len(), 1);
    assert!(d.trace.committed[0].bytes_estimate < 100 * 1024 * 1024);
}

#[test]
fn rule2_an_empty_window_is_never_emitted() {
    let c = EngineConfig {
        max_window: Micros(100 * MS),
        ..cfg()
    };
    let mut d = driver(c);
    d.sim.touch("p1"); // a lane exists, but nothing was ever pushed
    d.sim.advance(Micros(10 * SEC));
    d.run(200);
    assert!(d.trace.intents.is_empty(), "an empty window was closed");
    assert!(d.trace.committed.is_empty());
    assert!(d.trace.idles > 0, "an idle queue must idle");
}

// ---------------------------------------------------------------------------
// Rule 3 — the ts filter
// ---------------------------------------------------------------------------

#[test]
fn rule3_records_below_t_prev_are_dropped_on_arrival() {
    // A restart whose checkpoint is stale by a whole window: the re-read records
    // are dropped rather than shipped twice.
    let c = cfg();
    let mut d = driver(c.clone());
    for phase in 0..2 {
        for _ in 0..3 {
            d.sim.push("p1", 4);
            d.sim.advance(Micros(300 * MS));
        }
        d.sim.advance(Micros(3 * SEC));
        d.run_until(500, |d| d.trace.committed.len() > phase);
    }

    let committed = d.committed_pointer().unwrap();
    let shipped: Vec<(String, i64)> = d
        .shipped()
        .iter()
        .map(|r| (r.partition.to_string(), r.offset))
        .collect();

    // Restart with a checkpoint that points at offset 0 — as stale as it gets.
    let mut e2 = Engine::new("orders".into(), c.clone(), meta());
    e2.restore(
        Some(committed.clone()),
        None,
        Some(Checkpoint {
            k: committed.k,
            t_end: committed.t_end,
            positions: vec![("p1".into(), 0)],
        }),
    );
    let mut d2 = Driver::new(e2, Sim::new("orders", T0), c.safe_guard);
    d2.sim = std::mem::replace(&mut d.sim, Sim::new("orders", T0));
    d2.discovery_limit = c.discovery_limit as usize;
    d2.sim.push("p1", 4);
    d2.sim.advance(Micros(3 * SEC));
    d2.run_until(500, |d| !d.trace.committed.is_empty());

    let again: Vec<(String, i64)> = d2
        .shipped()
        .iter()
        .map(|r| (r.partition.to_string(), r.offset))
        .collect();
    for key in &again {
        assert!(
            !shipped.contains(key),
            "{key:?} was shipped twice across the restart"
        );
    }
    assert!(
        d2.engine.stats().records_dropped > 0,
        "the stale position should have re-read and dropped records"
    );
}

// ---------------------------------------------------------------------------
// Rule 4 — scheduling and the memory bound
// ---------------------------------------------------------------------------

#[test]
fn rule4_fetch_calls_respect_the_entry_and_concurrency_ceilings() {
    let c = EngineConfig {
        fetch_entries_per_call: 3,
        fetch_concurrency: 2,
        discovery_limit: 1000,
        ..cfg()
    };
    let mut d = driver(c.clone());
    d.discovery_limit = 1000;
    for i in 0..40 {
        d.sim.push(&format!("p{i:03}"), 2);
    }
    d.sim.advance(Micros(3 * SEC));
    d.run_until(600, |d| !d.trace.committed.is_empty());

    assert!(
        d.trace.max_fetch_entries <= 3,
        "{}",
        d.trace.max_fetch_entries
    );
    assert!(
        d.trace.max_calls_in_flight <= 2,
        "{}",
        d.trace.max_calls_in_flight
    );
    assert_exactly_once(&d);
}

/// The outcome of one cardinality run: what the sink held, and what it had to
/// do to get there.
struct Cardinality {
    peak_buffered: usize,
    seeks: u64,
    tracked: usize,
    fetch_entries: usize,
    calls_in_flight: usize,
    committed_records: usize,
}

/// A queue of `cold` partitions of which `HOT` are then written to, run until
/// the first window commits.
fn run_cardinality(cold: usize) -> Cardinality {
    const HOT: usize = 2_000;
    let c = EngineConfig {
        target_bytes: 200_000,
        max_window: Micros(SEC),
        start: Start::Latest,
        fetch_entries_per_call: 256,
        fetch_concurrency: 4,
        discovery_limit: 1000,
        // No reconciliation sweeps: the incremental `since` must carry the run,
        // which is the O(moved) claim of plan §2.
        full_sweep_every: 1_000_000,
        ..cfg()
    };
    let mut d = driver(c.clone());
    d.discovery_limit = 1000;
    d.sim.segment_bytes = 512;
    for i in 0..cold {
        d.sim.push(&format!("p{i:06}"), 1);
    }
    // Well past the `lastWriteAt` quantization slack, so rule 6 takes the branch
    // that needs no seek and no read.
    d.sim.advance(Micros(60 * SEC));
    d.run_until(20_000, |d| d.engine.tracked_partitions() >= cold);
    assert_eq!(d.engine.tracked_partitions(), cold);
    assert_eq!(d.engine.buffered_bytes(), 0, "a cold partition was read");

    for i in 0..HOT.min(cold) {
        d.sim.push(&format!("p{i:06}"), 4);
    }
    d.sim.advance(Micros(3 * SEC));
    d.run_until(40_000, |d| !d.trace.committed.is_empty());

    Cardinality {
        peak_buffered: d.trace.peak_buffered,
        seeks: d.engine.stats().seeks_issued,
        tracked: d.engine.tracked_partitions(),
        fetch_entries: d.trace.max_fetch_entries,
        calls_in_flight: d.trace.max_calls_in_flight,
        committed_records: d.trace.committed.iter().map(|p| p.records.len()).sum(),
    }
}

#[test]
fn rule4_memory_is_independent_of_partition_cardinality() {
    // The shape plan §2 is about: 10k-1M partitions per queue, nearly all cold.
    // Twenty times the partitions, the same hot set — and the same buffer. What
    // bounds the buffer is the window (plan §4.4), never the partition count:
    // a cold partition is positioned at its head from `lastWriteAt` alone, with
    // no seek and no fetch, so it costs a position entry and nothing else.
    let small = run_cardinality(5_000);
    let big = run_cardinality(100_000);

    assert_eq!(big.tracked, 100_000);
    assert_eq!(small.tracked, 5_000);
    assert_eq!(
        big.peak_buffered, small.peak_buffered,
        "buffered bytes scaled with the partition count: {} at 100k vs {} at 5k",
        big.peak_buffered, small.peak_buffered
    );
    assert_eq!(big.committed_records, small.committed_records);
    assert_eq!(big.seeks, 0, "a cold partition must not need a seek");
    assert_eq!(small.seeks, 0);

    // And the absolute figure is what plan §4.4 says it is: the window's own
    // content plus what the in-flight calls returned — nothing else.
    const HOT: usize = 2_000;
    let window_content = HOT * 4 * 120; // records x weight
    let one_call = 256 * 4 * 120; // entries x records x weight
    let ceiling = window_content + 4 * one_call;
    assert!(
        big.peak_buffered <= ceiling,
        "buffered {} bytes, over the {} ceiling",
        big.peak_buffered,
        ceiling
    );
    assert!(big.fetch_entries <= 256);
    assert!(big.calls_in_flight <= 4);
}

#[test]
fn rule4_a_caught_up_partition_is_not_refetched_until_discovery_says_it_moved() {
    let c = cfg();
    let mut d = driver(c);
    d.sim.push("p1", 2);
    d.sim.advance(Micros(3 * SEC));
    d.run_until(200, |d| !d.trace.committed.is_empty());
    let calls = d.sim.fetch_calls;

    // Nothing new: the clock moves, discovery runs, but p1 must not be read.
    d.sim.advance(Micros(2 * SEC));
    d.run(60);
    assert_eq!(
        d.sim.fetch_calls, calls,
        "a caught-up partition was refetched"
    );

    // Now it moves, and it is read again.
    d.sim.push("p1", 1);
    d.sim.advance(Micros(3 * SEC));
    d.run_until(300, |d| d.trace.committed.len() >= 2);
    assert!(d.sim.fetch_calls > calls);
    assert_exactly_once(&d);
}

// ---------------------------------------------------------------------------
// Rule 5 — discovery cadence and pagination
// ---------------------------------------------------------------------------

#[test]
fn rule5_the_first_discovery_is_a_full_sweep_and_the_rest_are_incremental() {
    let c = EngineConfig {
        full_sweep_every: 3,
        ..cfg()
    };
    let mut e = Engine::new("orders".into(), c.clone(), meta());
    assert_eq!(
        e.next_actions(),
        vec![Action::Discover {
            since: None,
            after: None
        }],
        "the first pass enumerates everything"
    );
    // Feed it a T_{k-1} so the incremental `since` has something to compute.
    let page = ChangedEntry {
        queue: "orders".into(),
        partitions: vec![],
        next: None,
        error: None,
    };
    e.on_discovery(&page, Micros(T0.0 + SEC), false);

    // Nothing to fetch, so the next discovery is cadence-driven: one Idle, then
    // the call.
    let a = e.next_actions();
    assert!(matches!(a.as_slice(), [Action::Idle { min_wait_ms }] if *min_wait_ms == 100));
    let a = e.next_actions();
    assert_eq!(a.len(), 1);
    match &a[0] {
        // start=earliest keeps T_{k-1} at −∞, which has no useful `since`.
        Action::Discover { since, after } => {
            assert_eq!(*since, None);
            assert_eq!(*after, None);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn rule5_incremental_discovery_asks_from_t_prev_minus_the_slack() {
    let c = EngineConfig {
        full_sweep_every: 1_000,
        start: Start::Latest,
        discovery_slack: Micros(2 * SEC),
        ..cfg()
    };
    let mut e = Engine::new("orders".into(), c, meta());
    e.restore(
        Some(Committed {
            k: 4,
            t_end: Micros(T0.0 + 100 * SEC),
            manifest: "m".into(),
            records: 1,
            bytes: 1,
            committed_at_ms: 0,
        }),
        None,
        None,
    );
    let _ = e.next_actions(); // the mandatory first full sweep
    let page = ChangedEntry {
        queue: "orders".into(),
        partitions: vec![],
        next: None,
        error: None,
    };
    e.on_discovery(&page, Micros(T0.0 + 200 * SEC), false);
    let _ = e.next_actions(); // Idle
    let a = e.next_actions();
    assert_eq!(
        a,
        vec![Action::Discover {
            since: Some(Micros(T0.0 + 98 * SEC)),
            after: None
        }],
        "since = T_prev - slack"
    );
}

#[test]
fn rule5_discovery_paginates_until_next_is_null() {
    let c = EngineConfig {
        discovery_limit: 3,
        ..cfg()
    };
    let mut d = driver(c);
    d.discovery_limit = 3;
    for i in 0..10 {
        d.sim.push(&format!("p{i}"), 1);
    }
    d.sim.advance(Micros(3 * SEC));
    d.run_until(400, |d| d.engine.tracked_partitions() >= 10);
    assert_eq!(
        d.engine.tracked_partitions(),
        10,
        "pagination lost a partition"
    );
    d.run_until(400, |d| !d.trace.committed.is_empty());
    assert_exactly_once(&d);
}

// ---------------------------------------------------------------------------
// Rule 6 — positions of newly seen partitions
// ---------------------------------------------------------------------------

fn bounds(name: &str, last: i64, start: i64, wrote: Micros) -> PartitionBounds {
    PartitionBounds {
        name: Arc::from(name),
        last_offset: last,
        log_start: start,
        last_write_at: Some(wrote),
    }
}

fn rec(partition: &str, offset: i64, ts: Micros) -> queen_s3::types::Record {
    queen_s3::types::Record {
        partition: Arc::from(partition),
        offset,
        transaction_id: format!("{partition}#{offset}"),
        ts,
        payload: None,
    }
}

fn entry(
    partition: &str,
    records: Vec<queen_s3::types::Record>,
    high: i64,
    start: i64,
) -> FetchedEntry {
    FetchedEntry {
        queue: "orders".into(),
        partition: Arc::from(partition),
        records,
        high_watermark: high,
        log_start_offset: start,
        error: None,
    }
}

fn page(partitions: Vec<PartitionBounds>) -> ChangedEntry {
    ChangedEntry {
        queue: "orders".into(),
        partitions,
        next: None,
        error: None,
    }
}

#[test]
fn rule6_latest_positions_a_long_idle_partition_at_the_head_with_no_seek() {
    let c = EngineConfig {
        start: Start::Latest,
        ..cfg()
    };
    let mut e = Engine::new("orders".into(), c, meta());
    let _ = e.next_actions();
    let safe = Micros(T0.0 + 100 * SEC);
    // Last write a minute below T_0: every record it holds is already outside.
    e.on_discovery(
        &page(vec![bounds("cold", 41, 0, Micros(T0.0 + 40 * SEC))]),
        safe,
        false,
    );
    assert_eq!(e.positions(), vec![("cold".to_string(), 42)]);
    assert_eq!(e.stats().seeks_issued, 0);
    // Nothing to read: the next action is not a fetch.
    let a = e.next_actions();
    assert!(
        !a.iter().any(|x| matches!(x, Action::Fetch(_))),
        "a partition below T_0 must not be read: {a:?}"
    );
}

#[test]
fn rule6_a_partition_written_near_t_prev_is_seeked() {
    let c = EngineConfig {
        start: Start::Latest,
        ..cfg()
    };
    let mut e = Engine::new("orders".into(), c, meta());
    let _ = e.next_actions();
    let safe = Micros(T0.0 + 100 * SEC);
    // Written inside the quantization slack of T_0 — the cheap branch is unsafe.
    e.on_discovery(
        &page(vec![bounds("hot", 41, 0, Micros(T0.0 + 100 * SEC))]),
        safe,
        false,
    );
    let a = e.next_actions();
    let seek = a
        .iter()
        .find_map(|x| match x {
            Action::Seek {
                partition,
                t,
                last_offset,
                log_start,
            } => Some((partition.clone(), *t, *last_offset, *log_start)),
            _ => None,
        })
        .expect("a seek was required");
    assert_eq!(&*seek.0, "hot");
    assert_eq!(seek.1, safe, "the seek target is T_0");
    assert_eq!((seek.2, seek.3), (41, 0));
    assert_eq!(e.stats().seeks_issued, 1);

    e.on_seek_result("hot", 30);
    assert_eq!(e.positions(), vec![("hot".to_string(), 30)]);
}

#[test]
fn rule6_earliest_positions_at_log_start() {
    let c = EngineConfig {
        start: Start::Earliest,
        ..cfg()
    };
    let mut e = Engine::new("orders".into(), c, meta());
    let _ = e.next_actions();
    e.on_discovery(
        &page(vec![bounds("p", 500, 120, Micros(T0.0 + 40 * SEC))]),
        Micros(T0.0 + 100 * SEC),
        false,
    );
    assert_eq!(e.positions(), vec![("p".to_string(), 120)]);
    assert_eq!(e.stats().seeks_issued, 0);
}

#[test]
fn rule6_a_checkpoint_entry_beats_both() {
    for start in [Start::Latest, Start::Earliest] {
        let c = EngineConfig { start, ..cfg() };
        let mut e = Engine::new("orders".into(), c, meta());
        e.restore(
            Some(Committed {
                k: 3,
                t_end: Micros(T0.0 + 50 * SEC),
                manifest: "m".into(),
                records: 1,
                bytes: 1,
                committed_at_ms: 0,
            }),
            None,
            Some(Checkpoint {
                k: 3,
                t_end: Micros(T0.0 + 50 * SEC),
                positions: vec![("p".into(), 314)],
            }),
        );
        let _ = e.next_actions();
        e.on_discovery(
            &page(vec![bounds("p", 500, 120, Micros(T0.0 + 60 * SEC))]),
            Micros(T0.0 + 100 * SEC),
            false,
        );
        assert_eq!(e.positions(), vec![("p".to_string(), 314)], "{start:?}");
        assert_eq!(e.stats().seeks_issued, 0, "{start:?}");
    }
}

// ---------------------------------------------------------------------------
// Rule 7 — retention overrun, and the one terminal error
// ---------------------------------------------------------------------------

#[test]
fn rule7_a_fetch_past_log_start_records_a_lost_range_and_resumes() {
    let c = cfg();
    let mut e = Engine::new("orders".into(), c, meta());
    let _ = e.next_actions();
    e.on_discovery(
        &page(vec![bounds("p", 100, 0, Micros(T0.0 + 10 * SEC))]),
        Micros(T0.0 + 100 * SEC),
        false,
    );
    let _ = e.next_actions();
    // Retention moved to 40 while the call was in flight.
    e.on_fetch(vec![FetchedEntry {
        queue: "orders".into(),
        partition: Arc::from("p"),
        records: vec![],
        high_watermark: 101,
        log_start_offset: 40,
        error: Some(FetchError::OffsetOutOfRange),
    }]);
    assert_eq!(
        e.positions(),
        vec![("p".to_string(), 40)],
        "resumed at logStart"
    );
    assert_eq!(e.stats().records_lost, 40, "the gap is 0..=39");
    assert_eq!(e.state(), EngineState::Filling, "the sink keeps committing");
}

#[test]
fn rule7_discovery_showing_a_higher_log_start_records_the_gap_too() {
    let c = cfg();
    let mut e = Engine::new("orders".into(), c, meta());
    let _ = e.next_actions();
    e.on_discovery(
        &page(vec![bounds("p", 100, 0, Micros(T0.0 + 10 * SEC))]),
        Micros(T0.0 + 100 * SEC),
        false,
    );
    assert_eq!(e.stats().records_lost, 0);
    e.on_discovery(
        &page(vec![bounds("p", 100, 55, Micros(T0.0 + 10 * SEC))]),
        Micros(T0.0 + 110 * SEC),
        false,
    );
    assert_eq!(e.stats().records_lost, 55);
    assert_eq!(e.positions(), vec![("p".to_string(), 55)]);
}

#[test]
fn rule7_a_lost_range_travels_in_the_window_that_found_it() {
    // The sink reads offsets 0..9, then retention takes 10..14 before it gets
    // to them. The gap must land in the manifest of the window that found it.
    let c = EngineConfig {
        max_window: Micros(200 * MS),
        ..cfg()
    };
    let mut e = Engine::new("orders".into(), c, meta());
    let t1 = T0;
    let t2 = Micros(T0.0 + SEC);
    let _ = e.next_actions();

    e.on_discovery(
        &page(vec![bounds("p", 9, 0, t1)]),
        Micros(T0.0 + 10 * MS),
        false,
    );
    let _ = e.next_actions();
    e.on_fetch(vec![entry(
        "p",
        (0..10).map(|o| rec("p", o, t1)).collect(),
        10,
        0,
    )]);
    assert_eq!(e.stats().records_lost, 0);

    // Ten more records arrived at t2, and 10..14 are already gone.
    e.on_discovery(
        &page(vec![bounds("p", 19, 15, t2)]),
        Micros(T0.0 + 5 * SEC),
        false,
    );
    assert_eq!(e.stats().records_lost, 5, "the gap is 10..=14");
    let _ = e.next_actions();
    e.on_fetch(vec![entry(
        "p",
        (15..20).map(|o| rec("p", o, t2)).collect(),
        20,
        15,
    )]);

    // A pass with a safeTime past both timestamps lets the window close.
    e.on_discovery(
        &page(vec![bounds("p", 19, 15, t2)]),
        Micros(T0.0 + 10 * SEC),
        false,
    );
    let a = e.next_actions();
    let Some(Action::WriteIntent(i)) = a.into_iter().find(|x| matches!(x, Action::WriteIntent(_)))
    else {
        panic!("no close")
    };
    e.on_intent_written(i.k);
    let a = e.next_actions();
    let Some(Action::Upload(plan)) = a.into_iter().find(|x| matches!(x, Action::Upload(_))) else {
        panic!("no upload")
    };
    assert_eq!(
        plan.records.len(),
        15,
        "the five lost records are simply absent"
    );
    assert_eq!(plan.lost.len(), 1, "{:?}", plan.lost);
    assert_eq!((plan.lost[0].from, plan.lost[0].to), (10, 14));
    assert_eq!(plan.lost[0].partition, "p");

    // And the gap is not repeated in the next window.
    e.on_uploaded(plan.k, "m".into(), 15, 1);
    e.on_committed(plan.k);
    assert!(e.next_actions().iter().all(|x| match x {
        Action::Upload(p) => p.lost.is_empty(),
        _ => true,
    }));
}

#[test]
fn rule7_unknown_topic_or_partition_is_terminal() {
    for kill in ["fetch", "discovery"] {
        let c = cfg();
        let mut e = Engine::new("orders".into(), c, meta());
        let _ = e.next_actions();
        if kill == "fetch" {
            e.on_discovery(
                &page(vec![bounds("p", 10, 0, Micros(T0.0 + 10 * SEC))]),
                Micros(T0.0 + 100 * SEC),
                false,
            );
            let _ = e.next_actions();
            e.on_fetch(vec![FetchedEntry {
                queue: "orders".into(),
                partition: Arc::from("p"),
                records: vec![],
                high_watermark: 0,
                log_start_offset: 0,
                error: Some(FetchError::UnknownTopicOrPartition),
            }]);
        } else {
            e.on_discovery(
                &ChangedEntry {
                    queue: "orders".into(),
                    partitions: vec![],
                    next: None,
                    error: Some("UNKNOWN_TOPIC_OR_PARTITION".into()),
                },
                Micros(T0.0 + 100 * SEC),
                false,
            );
        }
        assert!(
            matches!(e.state(), EngineState::Failed(_)),
            "{kill}: {:?}",
            e.state()
        );
        assert!(e.next_actions().is_empty(), "{kill}: a failed engine acts");
    }
}

#[test]
fn rule7_any_other_fetch_error_is_retried_and_still_blocks_the_close() {
    let c = EngineConfig {
        max_window: Micros(10 * MS),
        ..cfg()
    };
    let mut e = Engine::new("orders".into(), c, meta());
    let _ = e.next_actions();
    e.on_discovery(
        &page(vec![bounds("p", 10, 0, Micros(T0.0 + 10 * SEC))]),
        Micros(T0.0 + 100 * SEC),
        false,
    );
    let a = e.next_actions();
    assert!(a.iter().any(|x| matches!(x, Action::Fetch(_))));
    e.on_fetch(vec![FetchedEntry {
        queue: "orders".into(),
        partition: Arc::from("p"),
        records: vec![],
        high_watermark: 11,
        log_start_offset: 0,
        error: Some(FetchError::Other("INTERNAL".into())),
    }]);
    assert_eq!(e.stats().fetch_errors, 1);
    // The partition is schedulable again and no window closed over it.
    let a = e.next_actions();
    assert!(a.iter().any(|x| matches!(x, Action::Fetch(_))), "{a:?}");
    assert!(!a.iter().any(|x| matches!(x, Action::WriteIntent(_))));
}

// ---------------------------------------------------------------------------
// Rule 8 — the commit sequence
// ---------------------------------------------------------------------------

#[test]
fn rule8_the_sequence_is_intent_upload_commit_then_checkpoint() {
    let c = EngineConfig {
        max_window: Micros(200 * MS),
        checkpoint_every: 2,
        ..cfg()
    };
    let mut e = Engine::new("orders".into(), c, meta());
    let mut sim = Sim::new("orders", T0);
    sim.safe_lag = Micros(100 * MS);
    sim.push("p", 3);
    sim.advance(Micros(3 * SEC));
    let bounds_p = bounds("p", 2, 0, T0);
    let _ = e.next_actions();
    e.on_discovery(&page(vec![bounds_p]), sim.safe_time(), false);
    let a = e.next_actions();
    let Action::Fetch(entries) = a
        .into_iter()
        .find(|x| matches!(x, Action::Fetch(_)))
        .unwrap()
    else {
        unreachable!()
    };
    e.on_fetch(sim.fetch(&entries));

    // 1. intent, and nothing else, and the engine will not act until it lands.
    let a = e.next_actions();
    assert_eq!(a.len(), 1, "{a:?}");
    let Action::WriteIntent(intent) = &a[0] else {
        panic!("{a:?}")
    };
    assert_eq!(intent.k, 1);
    assert_eq!(intent.format, Format::Jsonl);
    assert_eq!(intent.writer, meta().writer);
    assert_eq!(e.state(), EngineState::Intent(1));
    assert!(
        e.next_actions().is_empty(),
        "the engine waits for the intent"
    );

    // 2. upload.
    e.on_intent_written(1);
    assert_eq!(e.state(), EngineState::Upload(1));
    let a = e.next_actions();
    let Action::Upload(plan) = &a[0] else {
        panic!("{a:?}")
    };
    assert_eq!(plan.k, 1);
    assert_eq!(plan.records.len(), 3);
    assert_eq!(plan.t_end, intent.t_end);
    assert_eq!(plan.t_start, intent.t_start);
    assert_eq!(plan.partitions, 1);
    assert_eq!(plan.min_ts, Some(T0));
    assert_eq!(plan.max_ts, Some(T0));
    assert!(
        e.next_actions().is_empty(),
        "the engine waits for the upload"
    );

    // 3. commit, with the wall clock left to the driver.
    e.on_uploaded(1, "_queen/windows/1.json".into(), 3, 999);
    assert_eq!(e.state(), EngineState::Commit(1));
    let a = e.next_actions();
    let Action::Commit(c) = &a[0] else {
        panic!("{a:?}")
    };
    assert_eq!(c.k, 1);
    assert_eq!(c.records, 3);
    assert_eq!(c.bytes, 999);
    assert_eq!(c.t_end, intent.t_end);
    assert_eq!(c.committed_at_ms, 0, "the engine has no wall clock");

    // 4. committed: back to filling, and no checkpoint at k=1 with every=2.
    e.on_committed(1);
    assert_eq!(e.state(), EngineState::Filling);
    assert_eq!(e.committed_k(), 1);
    assert_eq!(e.t_prev(), Some(intent.t_end));
    let a = e.next_actions();
    assert!(
        !a.iter().any(|x| matches!(x, Action::Checkpoint(_))),
        "{a:?}"
    );
}

#[test]
fn rule8_a_checkpoint_lands_every_n_windows() {
    let c = EngineConfig {
        max_window: Micros(150 * MS),
        checkpoint_every: 2,
        ..cfg()
    };
    let mut d = driver(c);
    for phase in 0..4 {
        d.sim.push("p", 2);
        d.sim.advance(Micros(3 * SEC));
        d.run_until(600, |d| d.trace.committed.len() > phase);
    }
    d.run(4); // drain the checkpoint the last commit queued
    let ks: Vec<u64> = d.trace.checkpoints.iter().map(|c| c.k).collect();
    assert_eq!(ks, vec![2, 4], "{ks:?}");
    let cp = &d.trace.checkpoints[0];
    assert_eq!(cp.positions.len(), 1);
    assert_eq!(cp.positions[0].0, "p");
}

// ---------------------------------------------------------------------------
// Rule 9 — restore and redo
// ---------------------------------------------------------------------------

#[test]
fn rule9_a_crash_after_the_intent_rebuilds_an_identical_window() {
    let c = EngineConfig {
        max_window: Micros(300 * MS),
        target_bytes: 2_000,
        ..cfg()
    };

    // Run 1: stop the instant the intent is written — plan §4.3's "between 4
    // and 6".
    let mut d1 = driver(c.clone());
    d1.stop_after_intent = true;
    for _ in 0..8 {
        d1.sim.push("p1", 3);
        d1.sim.push("p2", 2);
        d1.sim.advance(Micros(120 * MS));
    }
    d1.sim.advance(Micros(3 * SEC));
    d1.run_until(400, |d| d.stopped);
    let intent = d1
        .trace
        .intents
        .last()
        .cloned()
        .expect("an intent was written");
    assert_eq!(intent.k, 1);

    // What the first attempt WOULD have uploaded: the same engine, allowed to
    // carry on.
    let mut d0 = driver(c.clone());
    d0.sim = clone_sim(&d1.sim);
    d0.run_until(400, |d| !d.trace.uploads.is_empty());
    let original = d0.trace.uploads[0].clone();
    assert_eq!(original.t_end, intent.t_end, "the intent fixed T_k");

    // Run 2: a fresh engine, restored from (no commit, that intent).
    let mut e2 = Engine::new("orders".into(), c.clone(), meta());
    e2.restore(None, Some(intent.clone()), None);
    assert!(e2.redoing());
    let mut d2 = Driver::new(e2, clone_sim(&d1.sim), c.safe_guard);
    d2.discovery_limit = c.discovery_limit as usize;
    // The world moved on while the sink was down.
    d2.sim.push("p1", 5);
    d2.sim.advance(Micros(5 * SEC));
    d2.run_until(600, |d| !d.trace.uploads.is_empty());
    let redone = d2.trace.uploads[0].clone();

    assert_eq!(redone.k, original.k);
    assert_eq!(redone.t_start, original.t_start);
    assert_eq!(redone.t_end, original.t_end);
    assert_eq!(
        keys(&redone),
        keys(&original),
        "the redo produced a different record set"
    );
    assert_eq!(redone.records, original.records);
    assert_eq!(redone.partitions, original.partitions);
    assert_eq!(redone.min_ts, original.min_ts);
    assert_eq!(redone.max_ts, original.max_ts);
    // And the intent is not written a second time.
    assert!(
        d2.trace.intents.is_empty(),
        "a redo must not re-write the intent"
    );
}

fn keys(p: &WindowPlan) -> Vec<(String, i64)> {
    p.records
        .iter()
        .map(|r| (r.partition.to_string(), r.offset))
        .collect()
}

fn clone_sim(s: &Sim) -> Sim {
    let mut out = Sim::new(&s.queue, s.clock);
    out.parts = s.parts.clone();
    out.safe_lag = s.safe_lag;
    out.payload_bytes = s.payload_bytes;
    out.segment_bytes = s.segment_bytes;
    out.call_budget = s.call_budget;
    out.max_records_per_entry = s.max_records_per_entry;
    out
}

#[test]
fn rule9_restore_from_a_commit_continues_where_it_stopped() {
    let c = cfg();
    let mut d1 = driver(c.clone());
    for _ in 0..3 {
        d1.sim.push("p1", 3);
        d1.sim.advance(Micros(300 * MS));
    }
    d1.sim.advance(Micros(3 * SEC));
    d1.run_until(400, |d| !d.trace.committed.is_empty());
    let committed = d1.committed_pointer().unwrap();
    let first_shipped = d1.shipped().len();

    let mut e2 = Engine::new("orders".into(), c.clone(), meta());
    e2.restore(
        Some(committed.clone()),
        None,
        Some(Checkpoint {
            k: committed.k,
            t_end: committed.t_end,
            positions: e2_positions(&d1),
        }),
    );
    assert_eq!(e2.committed_k(), committed.k);
    let mut d2 = Driver::new(e2, clone_sim(&d1.sim), c.safe_guard);
    d2.discovery_limit = c.discovery_limit as usize;
    d2.sim.push("p1", 3);
    d2.sim.advance(Micros(3 * SEC));
    d2.run_until(400, |d| !d.trace.committed.is_empty());

    let mut all: Vec<(String, i64)> = d1
        .shipped()
        .iter()
        .chain(d2.shipped().iter())
        .map(|r| (r.partition.to_string(), r.offset))
        .collect();
    let n = all.len();
    all.sort();
    all.dedup();
    assert_eq!(
        n,
        all.len(),
        "a record was shipped on both sides of the restart"
    );
    assert!(all.len() > first_shipped, "the restart made no progress");
}

fn e2_positions(d: &Driver) -> Vec<(String, i64)> {
    d.engine.positions()
}

// ---------------------------------------------------------------------------
// Rule 10 — pruning
// ---------------------------------------------------------------------------

#[test]
fn rule10_a_partition_discovery_stops_naming_is_pruned() {
    let c = EngineConfig {
        max_window: Micros(150 * MS),
        prune_after_windows: 2,
        // No full sweeps: the incremental `since` is what stops naming it.
        full_sweep_every: 1_000_000,
        start: Start::Latest,
        ..cfg()
    };
    let mut d = driver(c);
    d.sim.push("gone", 1);
    d.sim.push("busy", 1);
    d.sim.advance(Micros(2 * SEC));
    d.run_until(300, |d| d.engine.tracked_partitions() == 2);

    // `gone` is deleted from the log; `busy` keeps writing.
    d.sim.parts.remove("gone");
    for _ in 0..8 {
        d.sim.push("busy", 1);
        d.sim.advance(Micros(2 * SEC));
        d.run(120);
        if d.engine.stats().partitions_pruned > 0 {
            break;
        }
    }
    assert!(
        d.engine.stats().partitions_pruned >= 1,
        "nothing was pruned after {} windows",
        d.engine.committed_k()
    );
    let names: Vec<String> = d.engine.positions().into_iter().map(|(n, _)| n).collect();
    assert_eq!(names, vec!["busy".to_string()]);
}

// ---------------------------------------------------------------------------
// Rule 11 — determinism of the plan
// ---------------------------------------------------------------------------

#[test]
fn rule11_the_plan_is_independent_of_fetch_arrival_order() {
    let c = EngineConfig {
        max_window: Micros(400 * MS),
        fetch_entries_per_call: 1, // one partition per call, so order can vary
        fetch_concurrency: 6,
        ..cfg()
    };
    let mut plans: Vec<WindowPlan> = Vec::new();
    for seed in [1u64, 7, 99, 12345] {
        let mut rng = Lcg::new(seed);
        let mut d = driver(c.clone());
        d.discovery_limit = 100;
        for i in 0..6 {
            d.sim.push(&format!("p{i}"), 3);
        }
        d.sim.advance(Micros(3 * SEC));

        let mut queued: Vec<Action> = Vec::new();
        for _ in 0..400 {
            if !d.trace.uploads.is_empty() {
                break;
            }
            queued.extend(d.engine.next_actions());
            while !queued.is_empty() {
                let i = rng.below(queued.len());
                let a = queued.remove(i);
                d.execute(a);
                if !d.trace.uploads.is_empty() {
                    break;
                }
            }
        }
        plans.push(d.trace.uploads[0].clone());
    }
    for p in &plans[1..] {
        assert_eq!(p.t_start, plans[0].t_start);
        assert_eq!(p.t_end, plans[0].t_end);
        assert_eq!(
            keys(p),
            keys(&plans[0]),
            "arrival order moved the record set"
        );
        assert_eq!(p.records, plans[0].records);
    }
    // Sorted by (partition, offset), and strictly: an offset is unique.
    let k = keys(&plans[0]);
    let mut sorted = k.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(k, sorted);
}

// ---------------------------------------------------------------------------
// Odds and ends the driver relies on
// ---------------------------------------------------------------------------

#[test]
fn force_close_ends_a_window_early_but_never_illegally() {
    let c = EngineConfig {
        target_bytes: 100 * 1024 * 1024,
        max_window: Micros(600 * SEC),
        // `earliest` has T_{k-1} = -infinity, which is older than any
        // max_window: the age trigger would fire on its own.
        start: Start::Latest,
        ..cfg()
    };
    let mut d = driver(c.clone());
    d.sim.push("p", 4);
    d.sim.advance(Micros(SEC));
    d.run(6); // let it take T_0 at the head of the log
    d.sim.push("p", 4);
    d.sim.advance(Micros(3 * SEC));
    // Neither size nor age can fire; only the memory budget can.
    d.run(60);
    assert!(d.trace.intents.is_empty());
    d.engine.force_close();
    d.run_until(60, |d| !d.trace.committed.is_empty());
    assert_eq!(d.trace.committed.len(), 1);
    let (t_end, safe) = d.trace.closes[0];
    assert!(t_end <= safe.saturating_sub(c.safe_guard));
}

#[test]
fn lag_and_degraded_are_reported_from_the_brokers_clock() {
    let c = cfg();
    let mut d = driver(c);
    d.sim.degraded = true;
    d.sim.push("p", 1);
    d.sim.advance(Micros(3 * SEC));
    d.run_until(300, |d| !d.trace.committed.is_empty());
    assert!(d.engine.safe_time_degraded());
    let t_end = d.trace.committed[0].t_end;
    let safe = d.sim.safe_time();
    assert_eq!(d.engine.lag(safe), safe.saturating_sub(t_end));
}

#[test]
fn a_failed_call_releases_its_partitions() {
    let c = cfg();
    let mut e = Engine::new("orders".into(), c, meta());
    let _ = e.next_actions();
    e.on_discovery(
        &page(vec![bounds("p", 10, 0, Micros(T0.0 + 10 * SEC))]),
        Micros(T0.0 + 100 * SEC),
        false,
    );
    let a = e.next_actions();
    let Some(Action::Fetch(entries)) = a.into_iter().find(|x| matches!(x, Action::Fetch(_))) else {
        panic!("no fetch")
    };
    let names: Vec<Arc<str>> = entries.iter().map(|e| e.partition.clone()).collect();
    e.on_fetch_failed(&names);
    let a = e.next_actions();
    assert!(
        a.iter().any(|x| matches!(x, Action::Fetch(_))),
        "the partition was not rescheduled: {a:?}"
    );
    // And the request the engine builds carries the configured ceiling.
    let Some(Action::Fetch(entries)) = a.into_iter().find(|x| matches!(x, Action::Fetch(_))) else {
        unreachable!()
    };
    assert_eq!(
        entries[0],
        FetchRequestEntry {
            queue: "orders".into(),
            partition: Arc::from("p"),
            offset: 0,
            max_bytes: Some(1 << 20),
        }
    );
}

#[test]
fn rule9_a_redo_under_start_latest_seeks_its_way_back_to_the_same_set() {
    // The realistic production config (plan D6). A restart in redo mode has no
    // checkpoint for any partition, so rule 6 has to put every one of them back
    // at T_{k-1} — by the `lastWriteAt` shortcut where it can, by a probe-seek
    // where it cannot — and the rebuilt window must still be the same set.
    let c = EngineConfig {
        start: Start::Latest,
        max_window: Micros(300 * MS),
        target_bytes: 2_000,
        ..cfg()
    };

    // The same scenario played twice: T_0 has to be taken BEFORE the hot writes,
    // so the run cannot be cloned from a finished simulator — it is replayed.
    let play = |stop: bool| {
        let mut d = driver(c.clone());
        d.stop_after_intent = stop;
        // A partition that goes quiet well before T_0, and two that write across it.
        d.sim.push("quiet", 4);
        d.sim.advance(Micros(30 * SEC));
        d.run(8); // take T_0 at the head of the log
        for _ in 0..6 {
            d.sim.push("hot1", 2);
            d.sim.push("hot2", 3);
            d.sim.advance(Micros(120 * MS));
        }
        d.sim.advance(Micros(3 * SEC));
        d
    };

    let mut d1 = play(true);
    d1.run_until(400, |d| d.stopped);
    let intent = d1
        .trace
        .intents
        .last()
        .cloned()
        .expect("an intent was written");

    let mut d0 = play(false);
    d0.run_until(400, |d| !d.trace.uploads.is_empty());
    let original = d0.trace.uploads[0].clone();
    assert_eq!(original.t_end, intent.t_end, "the intent fixed T_k");

    let mut e2 = Engine::new("orders".into(), c.clone(), meta());
    e2.restore(None, Some(intent.clone()), None);
    let mut d2 = Driver::new(e2, clone_sim(&d1.sim), c.safe_guard);
    d2.discovery_limit = c.discovery_limit as usize;
    d2.sim.push("hot1", 9); // the world moved on while the sink was down
    d2.sim.advance(Micros(5 * SEC));
    d2.run_until(600, |d| !d.trace.uploads.is_empty());
    let redone = d2.trace.uploads[0].clone();

    assert!(
        d2.engine.stats().seeks_issued > 0,
        "a redo with no checkpoint must seek its positions back"
    );
    assert_eq!(redone.t_start, original.t_start);
    assert_eq!(redone.t_end, original.t_end);
    assert_eq!(keys(&redone), keys(&original));
    assert_eq!(redone.records, original.records);
    assert!(
        !keys(&redone).iter().any(|(p, _)| p == "quiet"),
        "the pre-T_0 partition must stay out of the window"
    );
    assert!(
        d2.trace.intents.is_empty(),
        "a redo must not re-write the intent"
    );
}

#[test]
fn a_checkpoint_position_is_where_the_next_window_starts() {
    // Records for window k+1 can already be buffered when window k commits. The
    // checkpoint must name the first of THOSE, not the read position: restoring
    // from `next_offset` would start window k+1 past records it has to ship.
    let c = EngineConfig {
        max_window: Micros(200 * MS),
        checkpoint_every: 1,
        ..cfg()
    };
    let mut e = Engine::new("orders".into(), c, meta());
    let t1 = T0;
    let t2 = Micros(T0.0 + 10 * SEC);
    let _ = e.next_actions();
    e.on_discovery(
        &page(vec![bounds("p", 5, 0, t1)]),
        Micros(T0.0 + 20 * MS),
        false,
    );
    let _ = e.next_actions();
    // Three records in the window, three far above it.
    let mut records: Vec<_> = (0..3).map(|o| rec("p", o, t1)).collect();
    records.extend((3..6).map(|o| rec("p", o, t2)));
    e.on_fetch(vec![entry("p", records, 6, 0)]);
    assert_eq!(
        e.positions(),
        vec![("p".to_string(), 0)],
        "nothing shipped yet"
    );

    // A safeTime between the two timestamps closes a window over the first three.
    e.on_discovery(
        &page(vec![bounds("p", 5, 0, t1)]),
        Micros(T0.0 + 5 * SEC),
        false,
    );
    let a = e.next_actions();
    let Some(Action::WriteIntent(i)) = a.into_iter().find(|x| matches!(x, Action::WriteIntent(_)))
    else {
        panic!("no close")
    };
    e.on_intent_written(i.k);
    let a = e.next_actions();
    let Some(Action::Upload(plan)) = a.into_iter().find(|x| matches!(x, Action::Upload(_))) else {
        panic!("no upload")
    };
    assert_eq!(plan.records.len(), 3);
    e.on_uploaded(plan.k, "m".into(), 3, 1);
    let a = e.next_actions();
    let Some(Action::Commit(commit)) = a.into_iter().find(|x| matches!(x, Action::Commit(_)))
    else {
        panic!("no commit")
    };
    e.on_committed(commit.k);

    let a = e.next_actions();
    let Some(Action::Checkpoint(cp)) = a.into_iter().find(|x| matches!(x, Action::Checkpoint(_)))
    else {
        panic!("no checkpoint")
    };
    assert_eq!(cp.k, 1);
    assert_eq!(cp.t_end, plan.t_end);
    assert_eq!(
        cp.positions,
        vec![("p".to_string(), 3)],
        "the read position is 6; the next window starts at 3"
    );
}
