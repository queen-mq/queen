//! Overlay equivalence and request-id replay (§7.2, §5.4, D6/I6).
//!
//! The overlay is how a later command in a cycle sees an earlier one's effects
//! without anything being applied. Its correctness property: planning N commands
//! in ONE cycle reaches the same committed OFFSET/CURSOR state as planning them
//! one per cycle — "modulo now", because a batched cycle stamps one `now` and a
//! sequential run stamps a rising one, so the absolute timestamps differ while
//! the offsets, cursors and dedup verdicts do not.

use super::planner_harness::{ack_pos, pop_wildcard_with, push, Cell};
use crate::rsm::effect::Pid;
use crate::rsm::entry::{Outcome, PushOutcome, PushVerdict};
use crate::rsm::planner::{Plan, SubIntent};

/// The now-independent shape of a partition's committed state.
#[derive(Debug, PartialEq, Eq)]
struct Shape {
    last_offset: i64,
    log_start: u64,
    cursor: Option<(i64, Option<u64>, Option<String>, u64)>, // committed, batch_end, worker, total_consumed
}

fn shape(c: &Cell, pid: Pid, group: &str) -> Shape {
    let p = c.partition(pid).unwrap();
    let cur = c.cursor(pid, group).map(|cu| {
        (
            cu.committed,
            cu.batch_end,
            cu.worker.clone(),
            cu.total_consumed,
        )
    });
    Shape {
        last_offset: p.last_offset,
        log_start: p.log_start,
        cursor: cur,
    }
}

fn all_mode() -> SubIntent {
    SubIntent {
        mode: "all".to_string(),
        from_us: None,
        now: false,
    }
}

#[test]
fn two_pushes_to_one_partition_in_one_cycle_are_gapless() {
    // The second push must see the first's overlay tail.
    let mut batched = Cell::new("ov-gapless-batched");
    let cy = batched.run(&[
        push(1, "q", "p0", &["a", "b"]),
        push(2, "q", "p0", &["c", "d"]),
    ]);
    // Both logged in one entry.
    assert!(cy.logged);
    match &cy.outcome(1) {
        Outcome::Push(PushOutcome { items }) => match &items[0] {
            PushVerdict::Created { offset: 2, .. } => {}
            v => panic!("the second push did not see the overlay tail: {v:?}"),
        },
        o => panic!("{o:?}"),
    }
    let pid = batched.pid_of("q", "p0").unwrap();
    assert_eq!(batched.partition(pid).unwrap().last_offset, 3);

    // Same, one per cycle.
    let mut seq = Cell::new("ov-gapless-seq");
    seq.run(&[push(1, "q", "p0", &["a", "b"])]);
    seq.advance(1000);
    seq.run(&[push(2, "q", "p0", &["c", "d"])]);
    let pid2 = seq.pid_of("q", "p0").unwrap();
    assert_eq!(pid, pid2, "pid allocation is deterministic");
    assert_eq!(
        shape(&batched, pid, "g"),
        shape(&seq, pid2, "g"),
        "batched and sequential reach the same committed shape"
    );
}

#[test]
fn a_push_then_a_wildcard_pop_in_one_cycle_claims_the_overlay_append() {
    // The pop is a later command; it must see the partition the push created and
    // its frames, even though nothing has been applied yet.
    let mut batched = Cell::new("ov-pop-batched");
    let cy = batched.run(&[
        push(1, "q", "p0", &["a", "b"]),
        push(2, "q", "p1", &["c"]),
        pop_wildcard_with(3, "q", "g", "w1", |p| {
            p.sub = all_mode();
            p.budget = 100;
        }),
    ]);
    let claims = match cy.outcome(2) {
        Outcome::Pop(o) => o.claims,
        o => panic!("{o:?}"),
    };
    let total: i64 = claims
        .iter()
        .map(|c| c.end_offset as i64 - c.start_offset as i64 + 1)
        .sum();
    assert_eq!(
        total, 3,
        "all three overlay frames were claimed in the same cycle"
    );

    // The same three commands, one per cycle, reach the same committed shape.
    let mut seq = Cell::new("ov-pop-seq");
    seq.run(&[push(1, "q", "p0", &["a", "b"])]);
    seq.advance(1000);
    seq.run(&[push(2, "q", "p1", &["c"])]);
    seq.advance(1000);
    seq.run(&[pop_wildcard_with(3, "q", "g", "w1", |p| {
        p.sub = all_mode();
        p.budget = 100;
    })]);

    let p0 = batched.pid_of("q", "p0").unwrap();
    let p1 = batched.pid_of("q", "p1").unwrap();
    assert_eq!(p0, seq.pid_of("q", "p0").unwrap());
    assert_eq!(p1, seq.pid_of("q", "p1").unwrap());
    assert_eq!(shape(&batched, p0, "g"), shape(&seq, p0, "g"));
    assert_eq!(shape(&batched, p1, "g"), shape(&seq, p1, "g"));
}

#[test]
fn a_replayed_request_id_returns_the_recorded_outcome_and_plans_nothing() {
    let mut c = Cell::new("ov-replay");
    let first = c.run(&[push(1, "q", "p0", &["a", "b", "c"])]);
    let pid = c.pid_of("q", "p0").unwrap();
    assert_eq!(c.partition(pid).unwrap().last_offset, 2);
    let recorded = first.outcome(0);

    // The SAME command again (same request id): the recorded outcome, no entry.
    c.advance(1_000_000);
    let again = c.run(&[push(1, "q", "p0", &["a", "b", "c"])]);
    assert!(!again.logged, "a replay is not logged (I6)");
    assert_eq!(
        again.outcome(0),
        recorded,
        "the recorded outcome is returned"
    );
    assert_eq!(
        c.partition(pid).unwrap().last_offset,
        2,
        "the replay appended nothing"
    );
    // And it is answered from the record, not by planning.
    assert!(matches!(again.plan(0), Ok(Plan::Empty(_))));
}

#[test]
fn a_replayed_ack_does_not_advance_the_cursor_twice() {
    let mut c = Cell::new("ov-replay-ack");
    c.run(&[push(1, "q", "p0", &["a", "b"])]);
    c.advance(1_000_000);
    c.run(&[super::planner_harness::pop_pinned(2, "q", "p0", "g", "w1")]);
    let pid = c.pid_of("q", "p0").unwrap();
    c.advance(1000);
    // Ack the whole batch (id 3).
    c.run(&[ack_pos(3, pid, "q", "g", "w1", Some(1), true, 2)]);
    assert_eq!(c.cursor(pid, "g").unwrap().committed, 1);
    // Replay the ack: the recorded outcome, the cursor does not move again.
    c.advance(1000);
    let again = c.run(&[ack_pos(3, pid, "q", "g", "w1", Some(1), true, 2)]);
    assert!(!again.logged);
    assert_eq!(c.cursor(pid, "g").unwrap().committed, 1);
}
