//! PERF-I — the O(claimed) bounded claim path (`QUEEN_RAFT_CLAIM_FROM_RING`)
//! against the baseline `segs_from(log_start)` + `hashes_in_range` path.
//!
//! The bounded path reads one txns row per segment the claim actually touches
//! (from the segment covering `wanted`, stopping at the budget or a deferred
//! segment) and folds the delivered hashes into that pass; the baseline scans
//! the partition's whole retained history and reads the delivered set with a
//! second scan. They must produce IDENTICAL outcomes and IDENTICAL committed
//! state — the claim is a pure local read optimisation, the effects do not move.
//!
//! - [`bounded_and_baseline_agree_on_random_workloads`] is the differential
//!   fuzzer: 50 seeds, each a random push/pop/ack workload replayed on two cells
//!   (one forced on, one forced off), comparing every pop outcome and every
//!   cursor row after every cycle. 0 divergences is the gate.
//! - [`the_bounded_gather_is_o_claimed_not_o_history`] is the in-process
//!   benchmark: a lagging consumer over a partition with a growing segment
//!   history, timing the pop both ways — the baseline grows with the history,
//!   the bounded path stays flat.

use super::planner_harness::{ack_pos, push_cfg, qcfg, rid, Cell, Cmd, TENANT};
use crate::rsm::effect::{CursorRow, Pid, QueueConfig};
use crate::rsm::entry::{Outcome, PopClaim};
use crate::rsm::planner::{Plan, Planned, PopCommand, SubIntent};

// A fixed-seed SplitMix64, written out so a failure is reproducible from the
// printed seed (the same discipline `tests/fuzz.rs` follows).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
    fn chance(&mut self, num: usize, den: usize) -> bool {
        self.below(den) < num
    }
}

const QUEUE: &str = "q";
const GROUPS: [&str; 2] = ["g0", "g1"];
const PARTS: [&str; 4] = ["p0", "p1", "p2", "p3"];
const WORKERS: [&str; 2] = ["w0", "w1"];

/// A queue config the fuzzer varies: some seeds turn on `delayed_processing`,
/// which exercises the freshness break inside the bounded gather.
fn cfg_for(delayed: i32) -> QueueConfig {
    let mut c = qcfg();
    c.delayed_processing = delayed;
    c
}

fn wildcard(id: u64, group: &str, worker: &str, f: impl FnOnce(&mut PopCommand)) -> Cmd {
    let mut c = PopCommand {
        request_id: rid(id),
        tenant: TENANT.to_string(),
        queue: QUEUE.to_string(),
        partition: None,
        group: group.to_string(),
        worker: worker.to_string(),
        budget: 100,
        max_parts: 10,
        lease_seconds: 60,
        auto_ack: false,
        conflate: false,
        sub: SubIntent {
            mode: "all".to_string(),
            from_us: None,
            now: false,
        },
        skip_window_debounce: false,
        namespace: String::new(),
        task: String::new(),
        create_cfg: Some(qcfg()),
        deadline_us: 0,
    };
    f(&mut c);
    Cmd::PopWildcard(c)
}

fn pinned(
    id: u64,
    partition: &str,
    group: &str,
    worker: &str,
    f: impl FnOnce(&mut PopCommand),
) -> Cmd {
    let mut c = PopCommand {
        request_id: rid(id),
        tenant: TENANT.to_string(),
        queue: QUEUE.to_string(),
        partition: Some(partition.to_string()),
        group: group.to_string(),
        worker: worker.to_string(),
        budget: 100,
        max_parts: 10,
        lease_seconds: 60,
        auto_ack: false,
        conflate: false,
        sub: SubIntent {
            mode: "all".to_string(),
            from_us: None,
            now: false,
        },
        skip_window_debounce: false,
        namespace: String::new(),
        task: String::new(),
        create_cfg: Some(qcfg()),
        deadline_us: 0,
    };
    f(&mut c);
    Cmd::PopPinned(c)
}

fn pop_claims(o: &Outcome) -> Vec<PopClaim> {
    match o {
        Outcome::Pop(p) => p.claims.clone(),
        _ => Vec::new(),
    }
}

/// Every cursor row the two cells could differ on, for the after-cycle compare.
fn cursor_snapshot(c: &Cell) -> Vec<(Pid, &'static str, Option<CursorRow>)> {
    let mut out = Vec::new();
    for name in PARTS {
        if let Some(pid) = c.pid_of(QUEUE, name) {
            for g in GROUPS {
                out.push((pid, g, c.cursor(pid, g)));
            }
        }
    }
    out
}

/// One random workload replayed on both cells, asserting identical pop outcomes
/// and identical committed cursor state at every step. Returns the number of
/// pop commands that actually claimed something (so the test can assert the
/// workload was not vacuous).
fn run_seed(seed: u64) -> usize {
    let mut rng = Rng(seed);
    let delayed = if rng.chance(1, 3) { 1 } else { 0 };
    let cfg = cfg_for(delayed);

    let mut on = Cell::new(&format!("perfi-on-{seed}"));
    on.claim_from_ring(true);
    let mut off = Cell::new(&format!("perfi-off-{seed}"));
    off.claim_from_ring(false);

    let mut id: u64 = 1;
    let steps = 20 + rng.below(20);
    let mut claims_seen = 0usize;

    for _ in 0..steps {
        // Build one cycle's batch. Pushes reference partition names; acks resolve
        // the pid from the (already identical) committed state of the ON cell.
        let mut cmds: Vec<Cmd> = Vec::new();
        let batch = 1 + rng.below(3);
        for _ in 0..batch {
            match rng.below(10) {
                0..=3 => {
                    // push a random run of frames to a random partition
                    let part = PARTS[rng.below(PARTS.len())];
                    let n = 1 + rng.below(6);
                    let txns: Vec<String> = (0..n)
                        .map(|_| format!("m{}", id.wrapping_mul(2_654_435_761) ^ rng.next()))
                        .collect();
                    let refs: Vec<&str> = txns.iter().map(|s| s.as_str()).collect();
                    cmds.push(push_cfg(id, QUEUE, part, &refs, cfg.clone()));
                }
                4..=6 => {
                    // wildcard pop, random budget / max_parts / auto_ack
                    let g = GROUPS[rng.below(GROUPS.len())];
                    let w = WORKERS[rng.below(WORKERS.len())];
                    let budget = 1 + rng.below(8) as i32;
                    let mp = 1 + rng.below(PARTS.len()) as i32;
                    let auto = rng.chance(1, 3);
                    cmds.push(wildcard(id, g, w, |c| {
                        c.budget = budget;
                        c.max_parts = mp;
                        c.auto_ack = auto;
                    }));
                }
                7 => {
                    // pinned pop
                    let part = PARTS[rng.below(PARTS.len())];
                    let g = GROUPS[rng.below(GROUPS.len())];
                    let w = WORKERS[rng.below(WORKERS.len())];
                    let budget = 1 + rng.below(8) as i32;
                    let auto = rng.chance(1, 4);
                    cmds.push(pinned(id, part, g, w, |c| {
                        c.budget = budget;
                        c.auto_ack = auto;
                    }));
                }
                _ => {
                    // ack the current batch of a random (pid, group) if it holds
                    // a lease, using the ON cell's committed view (== OFF's).
                    let part = PARTS[rng.below(PARTS.len())];
                    let g = GROUPS[rng.below(GROUPS.len())];
                    if let Some(pid) = on.pid_of(QUEUE, part) {
                        if let Some(cur) = on.cursor(pid, g) {
                            if let (Some(be), Some(w)) = (cur.batch_end, cur.worker.clone()) {
                                if rng.chance(1, 2) {
                                    // positional ack up to the batch end
                                    let count = (be as i64 - cur.committed).max(0) as i32;
                                    cmds.push(ack_pos(
                                        id,
                                        pid,
                                        QUEUE,
                                        g,
                                        &w,
                                        Some(be as i64),
                                        true,
                                        count,
                                    ));
                                } else {
                                    // nack: release the lease, cursor untouched
                                    cmds.push(ack_pos(id, pid, QUEUE, g, &w, None, false, 0));
                                }
                            }
                        }
                    }
                }
            }
            id += 1;
        }

        // Advance the clock sometimes so leases can expire and delayed frames
        // become visible.
        if rng.chance(1, 3) {
            let us = 1_000_000 * (1 + rng.below(120)) as i64;
            on.advance(us);
            off.advance(us);
        }

        let ca = on.run(&cmds);
        let cb = off.run(&cmds);

        // Same per-command outcomes for every pop (the claim's own product).
        for (i, cmd) in cmds.iter().enumerate() {
            if matches!(cmd, Cmd::PopWildcard(_) | Cmd::PopPinned(_)) {
                let oa = ca.plan(i);
                let ob = cb.plan(i);
                let (pa, pb) = (as_outcome(oa), as_outcome(ob));
                let claims_a = pa.as_ref().map(pop_claims).unwrap_or_default();
                let claims_b = pb.as_ref().map(pop_claims).unwrap_or_default();
                assert_eq!(
                    claims_a, claims_b,
                    "seed {seed}: pop outcome diverged at command {i}",
                );
                claims_seen += claims_a.len();
            }
        }

        // Same committed cursor state after the cycle: the CursorSet effects the
        // two paths emitted were byte-identical.
        assert_eq!(
            cursor_snapshot(&on),
            cursor_snapshot(&off),
            "seed {seed}: committed cursor state diverged",
        );
    }
    claims_seen
}

fn as_outcome(p: &Planned) -> Option<Outcome> {
    match p {
        Ok(Plan::Logged { outcome, .. }) => Some(outcome.clone()),
        Ok(Plan::Empty(o)) => Some(o.clone()),
        _ => None,
    }
}

#[test]
fn bounded_and_baseline_agree_on_random_workloads() {
    let mut total_claims = 0usize;
    for seed in 0..50u64 {
        total_claims += run_seed(0xA11CE_0000 ^ seed.wrapping_mul(0x9E37_79B9));
    }
    // The workloads must actually claim frames, or the differential proves
    // nothing about the claim path.
    assert!(
        total_claims > 200,
        "the fuzz workloads were near-vacuous ({total_claims} claims across 50 seeds)"
    );
}

/// A single-partition sanity that the two paths agree even when the consumer
/// lags far behind a long, multi-segment backlog (the shape the bounded gather
/// is for) — and that the bounded path DID fire (a plain non-conflating,
/// no-gap, live claim).
#[test]
fn a_lagging_consumer_claims_identically_both_ways() {
    fn drive(claim_from_ring: bool) -> (Vec<PopClaim>, Option<CursorRow>) {
        let mut c = Cell::new(&format!("perfi-lag-{claim_from_ring}"));
        c.claim_from_ring(claim_from_ring);
        // 200 single-frame appends → 200 segments, retention off.
        let mut idn = 1u64;
        for i in 0..200u64 {
            c.run(&[push_cfg(idn, QUEUE, "p0", &[&format!("m{i}")], qcfg())]);
            idn += 1;
        }
        c.advance(1_000_000);
        // The consumer is at -1 (fresh group): claim a 30-frame budget from the
        // head of a 200-segment history.
        let cy = c.run(&[pinned(idn, "p0", "g0", "w0", |p| p.budget = 30)]);
        let claims = pop_claims(&cy.outcome(0));
        let pid = c.pid_of(QUEUE, "p0").unwrap();
        (claims, c.cursor(pid, "g0"))
    }
    let on = drive(true);
    let off = drive(false);
    assert_eq!(on.0, off.0, "claims diverged");
    assert_eq!(on.1, off.1, "cursor diverged");
    // The claim delivered exactly the budgeted run [0, 29].
    assert_eq!(on.0.len(), 1);
    assert_eq!((on.0[0].start_offset, on.0[0].end_offset), (0, 29));
    let cur = on.1.unwrap();
    assert_eq!(
        cur.delivered.len(),
        30,
        "the delivered set is the 30-frame batch"
    );
}

/// In-process benchmark (ignored; a measurement, not a gate): the baseline
/// scans the whole history per pop, the bounded path scans O(budget). We time a
/// single pop of a 30-frame budget against a partition whose history grows, and
/// print both — the baseline should climb with the history, the bounded path
/// stay flat. Run with:
///   cargo test -p queen-engine --lib rsm::tests::pop_bounded::the_bounded_gather_is_o_claimed -- --ignored --nocapture
#[test]
#[ignore = "measurement, not a gate"]
fn the_bounded_gather_is_o_claimed_not_o_history() {
    use std::time::Instant;

    fn build(history: u64) -> Cell {
        let mut c = Cell::new(&format!("perfi-bench-{history}"));
        let mut idn = 1u64;
        for i in 0..history {
            c.run(&[push_cfg(idn, QUEUE, "p0", &[&format!("m{i}")], qcfg())]);
            idn += 1;
        }
        c.advance(1_000_000);
        c
    }

    // Measure the pop-plan time (plan only) at a fresh group over a growing
    // history, both paths. Reuse one built store per history size; re-plan the
    // SAME fresh pop many times (plan_only does not apply, so the group stays
    // fresh and the claim shape is identical every iteration).
    println!("history  baseline_us/pop  bounded_us/pop");
    for history in [50u64, 200, 800, 3200] {
        let c = build(history);
        let iters = 200u32;

        let bench = |cell: &mut Cell, on: bool| -> f64 {
            cell.claim_from_ring(on);
            let t = Instant::now();
            for k in 0..iters {
                let _ = cell.plan_only(&[pinned(1_000_000 + k as u64, "p0", "g0", "w0", |p| {
                    p.budget = 30
                })]);
            }
            t.elapsed().as_secs_f64() * 1e6 / iters as f64
        };

        let mut cell = c;
        let base = bench(&mut cell, false);
        let bound = bench(&mut cell, true);
        println!("{history:7}  {base:14.2}  {bound:14.2}");
    }
}
