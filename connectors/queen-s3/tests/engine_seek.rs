//! The backwards probe-seek (plan §4.5(2)) over a simulated partition of a
//! million records.
//!
//! The property under test is exactness, not approximation: `seek(p, T)` must
//! answer *the* first offset with `ts >= T`, because that offset is where a
//! window starts reading and anything below it is silently skipped.

#[path = "engine_support.rs"]
mod support;

use queen_s3::seek::{Seek, SeekStep};
use queen_s3::types::Micros;

use support::Sim;

const T0: Micros = Micros(1_788_480_000_000_000);
const SEC: i64 = 1_000_000;

/// A million records in ten thousand segments of a hundred, one segment per
/// second of the PG clock.
fn million() -> Sim {
    let mut sim = Sim::new("orders", T0);
    for _ in 0..10_000 {
        sim.push("p", 100);
        sim.advance(Micros(SEC));
    }
    sim
}

/// The answer, by exhaustive walk over the segments: the first live offset whose
/// segment timestamp is at or after `t`.
fn brute(sim: &Sim, t: Micros) -> i64 {
    let p = &sim.parts["p"];
    for s in &p.segments {
        if s.ts >= t {
            return s.base.max(p.log_start);
        }
    }
    p.last_offset + 1
}

fn run(sim: &mut Sim, t: Micros, probe_records: usize) -> (SeekStep, u32, u32) {
    let (last, start) = sim.bounds("p");
    let mut s = Seek::new("orders".into(), "p".into(), t, last, start, probe_records);
    loop {
        match s.step() {
            SeekStep::Continue => {}
            done => return (done, s.probes(), s.probe_budget()),
        }
        let Some(probe) = s.next_probe() else {
            return (s.step(), s.probes(), s.probe_budget());
        };
        let entry = sim.fetch(&[probe]).remove(0);
        s.on_result(&entry);
    }
}

#[test]
fn finds_the_exact_offset_across_a_million_records() {
    let mut sim = million();
    // One record per probe: the worst case for the probe count, and the one the
    // bound is stated for.
    sim.max_records_per_entry = 1;
    let mut worst = 0;
    for secs in [0i64, 1, 2, 37, 500, 4_999, 5_000, 5_001, 9_998, 9_999] {
        let t = Micros(T0.0 + secs * SEC);
        let (step, probes, budget) = run(&mut sim, t, 1);
        assert_eq!(
            step,
            SeekStep::Found(brute(&sim, t)),
            "seek to +{secs}s missed the flip point"
        );
        assert!(
            probes <= budget,
            "{probes} probes over a budget of {budget}"
        );
        worst = worst.max(probes);
    }
    // 2*log2(1e6) + 2 = 42.
    assert!(worst <= 42, "worst case was {worst} probes");
}

#[test]
fn a_timestamp_between_two_segments_lands_on_the_later_one() {
    let mut sim = million();
    sim.max_records_per_entry = 1;
    // Halfway through second 700: no segment carries this ts, so the answer is
    // the first segment above it — offset 701 * 100.
    let t = Micros(T0.0 + 700 * SEC + 500_000);
    let (step, _, _) = run(&mut sim, t, 1);
    assert_eq!(step, SeekStep::Found(70_100));
    assert_eq!(step, SeekStep::Found(brute(&sim, t)));
}

#[test]
fn a_timestamp_exactly_on_a_segment_boundary_takes_that_segment() {
    let mut sim = million();
    sim.max_records_per_entry = 1;
    let t = Micros(T0.0 + 700 * SEC);
    let (step, _, _) = run(&mut sim, t, 1);
    assert_eq!(
        step,
        SeekStep::Found(70_000),
        "the boundary belongs to [T, ...)"
    );
}

#[test]
fn below_everything_is_log_start_and_above_everything_is_the_head() {
    let mut sim = million();
    sim.max_records_per_entry = 1;

    let (step, probes, _) = run(&mut sim, Micros(T0.0 - SEC), 1);
    assert_eq!(step, SeekStep::Found(0), "everything is at or after T");
    assert!(probes <= 22, "galloping to logStart took {probes} probes");

    let (step, probes, _) = run(&mut sim, Micros(T0.0 + 100_000 * SEC), 1);
    assert_eq!(step, SeekStep::Found(1_000_000), "nothing is at or after T");
    assert_eq!(probes, 1, "one probe at the head settles it");
}

#[test]
fn retention_moves_the_floor_up() {
    let mut sim = million();
    sim.max_records_per_entry = 1;
    // The first 5 000 seconds are gone.
    sim.retain_from("p", 500_000);
    let (last, _) = sim.bounds("p");

    // A target below the floor answers logStart: every surviving record is at
    // or after it.
    let mut s = Seek::new("orders".into(), "p".into(), T0, last, 0, 1);
    let step = loop {
        match s.step() {
            SeekStep::Continue => {}
            done => break done,
        }
        let probe = s.next_probe().unwrap();
        let entry = sim.fetch(&[probe]).remove(0);
        s.on_result(&entry);
    };
    assert_eq!(step, SeekStep::Found(500_000));

    // And one above it is still exact.
    let t = Micros(T0.0 + 7_000 * SEC);
    let (step, _, _) = run(&mut sim, t, 1);
    assert_eq!(step, SeekStep::Found(700_000));
}

#[test]
fn a_multi_record_probe_finishes_faster_and_agrees() {
    let mut sim = million();
    // The broker answers in whole segments, so a probe that straddles T ends the
    // search on the spot.
    let t = Micros(T0.0 + 6_123 * SEC);
    let (step, few, _) = run(&mut sim, t, 64);
    assert_eq!(step, SeekStep::Found(brute(&sim, t)));

    sim.max_records_per_entry = 1;
    let (step2, many, _) = run(&mut sim, t, 1);
    assert_eq!(step2, step, "the answer must not depend on the probe size");
    assert!(
        few < many,
        "{few} multi-record probes vs {many} single-record"
    );
}

#[test]
fn an_empty_and_a_never_written_partition_answer_without_a_probe() {
    let mut sim = Sim::new("orders", T0);
    sim.touch("p");
    let (step, probes, _) = run(&mut sim, T0, 1);
    assert_eq!(step, SeekStep::Found(0));
    assert_eq!(probes, 0);

    // One record, then retention takes it: log_start == high.
    let mut sim = Sim::new("orders", T0);
    sim.push("p", 1);
    sim.retain_from("p", 1);
    let (step, probes, _) = run(&mut sim, T0, 1);
    assert_eq!(step, SeekStep::Found(1));
    assert_eq!(probes, 0);
}

#[test]
fn a_single_record_partition_is_exact_in_both_directions() {
    let mut sim = Sim::new("orders", T0);
    sim.push("p", 1);
    let (step, _, _) = run(&mut sim, T0, 1);
    assert_eq!(step, SeekStep::Found(0));
    let (step, _, _) = run(&mut sim, Micros(T0.0 + 1), 1);
    assert_eq!(step, SeekStep::Found(1));
}

#[test]
fn every_offset_of_a_small_partition_is_reachable() {
    // Exhaustive: 200 records in 20 segments, every possible target.
    let mut sim = Sim::new("orders", T0);
    for _ in 0..20 {
        sim.push("p", 10);
        sim.advance(Micros(SEC));
    }
    sim.max_records_per_entry = 1;
    for secs in -1..22i64 {
        for delta in [0i64, -1, 1] {
            let t = Micros(T0.0 + secs * SEC + delta);
            let (step, probes, budget) = run(&mut sim, t, 1);
            assert_eq!(step, SeekStep::Found(brute(&sim, t)), "t = {t}");
            assert!(probes <= budget);
        }
    }
}

#[test]
fn an_unknown_partition_fails_rather_than_looping() {
    let mut sim = million();
    sim.unknown_queue = true;
    let (step, probes, _) = run(&mut sim, T0, 1);
    assert!(matches!(step, SeekStep::Failed(_)), "{step:?}");
    assert_eq!(
        probes, 1,
        "it gives up on the first answer, not after a budget"
    );
}
