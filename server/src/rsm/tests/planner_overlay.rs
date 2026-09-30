//! Overlay equivalence and request-id replay (§7.2, §5.4, D6/I6).
//!
//! The overlay is how a later command in a cycle sees an earlier one's effects
//! without anything being applied. Its correctness property: planning N commands
//! in ONE cycle reaches the same committed OFFSET/CURSOR state as planning them
//! one per cycle — "modulo now", because a batched cycle stamps one `now` and a
//! sequential run stamps a rising one, so the absolute timestamps differ while
//! the offsets, cursors and dedup verdicts do not.

use super::planner_harness::{cursor_set, push, Cell};
use crate::rsm::effect::Pid;
use crate::rsm::entry::{Outcome, PushOutcome, PushVerdict};
use crate::rsm::planner::Plan;

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
fn a_cursor_row_on_a_partition_created_earlier_in_the_cycle_is_planned_over_the_overlay() {
    // The checkpoint is a later command; the partition the push creates is
    // only in the overlay, and the cursor row must see it there (a partition
    // the checkpoint cannot see is refused as gone).
    let mut batched = Cell::new("ov-cursor-batched");
    let first = batched.run(&[push(1, "q", "seed", &["s"])]);
    assert!(first.logged);
    let next = batched.pid_of("q", "seed").unwrap() + 1;
    let cy = batched.run(&[push(2, "q", "p0", &["a", "b"]), cursor_set(3, next, "g", 0)]);
    assert!(
        matches!(cy.plan(1), Ok(Plan::Logged { .. })),
        "the cursor row saw the partition created in flight: {:?}",
        cy.plan(1)
    );
    let p0 = batched.pid_of("q", "p0").unwrap();
    assert_eq!(p0, next);

    // The same commands, one per cycle, reach the same committed shape.
    let mut seq = Cell::new("ov-cursor-seq");
    seq.run(&[push(1, "q", "seed", &["s"])]);
    seq.advance(1000);
    seq.run(&[push(2, "q", "p0", &["a", "b"])]);
    seq.advance(1000);
    seq.run(&[cursor_set(3, next, "g", 0)]);
    assert_eq!(seq.pid_of("q", "p0").unwrap(), p0);
    assert_eq!(shape(&batched, p0, "g"), shape(&seq, p0, "g"));
}

#[test]
fn a_cursor_row_on_an_unknown_partition_is_refused_retryably() {
    let mut c = Cell::new("ov-cursor-gone");
    c.run(&[push(1, "q", "p0", &["a"])]);
    let pid = c.pid_of("q", "p0").unwrap();
    let cy = c.run(&[cursor_set(2, pid + 100, "g", 0)]);
    match cy.plan(0) {
        Err(r) => assert!(r.retryable && r.code == "partition_gone", "{r:?}"),
        other => panic!("a cursor row on no partition was planned: {other:?}"),
    }
    assert!(!cy.logged);
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
fn a_replayed_checkpoint_does_not_move_the_cursor_twice() {
    let mut c = Cell::new("ov-replay-checkpoint");
    c.run(&[push(1, "q", "p0", &["a", "b"])]);
    let pid = c.pid_of("q", "p0").unwrap();
    c.advance(1000);
    c.run(&[cursor_set(3, pid, "g", 1)]);
    assert_eq!(c.cursor(pid, "g").unwrap().committed, 1);
    // A later checkpoint moves it back (a seek) ...
    c.advance(1000);
    c.run(&[cursor_set(4, pid, "g", 0)]);
    assert_eq!(c.cursor(pid, "g").unwrap().committed, 0);
    // ... and a replay of the first is answered from its record: nothing moves.
    c.advance(1000);
    let again = c.run(&[cursor_set(3, pid, "g", 1)]);
    assert!(!again.logged);
    assert_eq!(c.cursor(pid, "g").unwrap().committed, 0);
}
