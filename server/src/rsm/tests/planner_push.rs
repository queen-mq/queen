//! Push against the tiny apply loop — the port of `native/tests/push.rs`
//! plus the pitfalls of §8 row 003.

use super::planner_harness::{push, push_cfg, qcfg, Cell, TENANT};
use crate::rsm::entry::{Outcome, PushOutcome, PushVerdict};
use crate::rsm::planner::Plan;
use crate::rsm::store::keys::Counter;

fn verdicts(o: &Outcome) -> &Vec<PushVerdict> {
    match o {
        Outcome::Push(PushOutcome { items }) => items,
        other => panic!("not a push outcome: {other:?}"),
    }
}

#[test]
fn a_first_push_provisions_the_queue_and_partition_and_assigns_gapless_offsets() {
    let mut c = Cell::new("push-first");
    let cy = c.run(&[push(1, "orders", "p0", &["a", "b", "c"])]);
    let v = verdicts(&cy.outcome(0)).clone();
    assert_eq!(v.len(), 3);
    let pid = c.pid_of("orders", "p0").expect("partition created");
    assert!(c.queue("orders").is_some(), "queue provisioned implicitly");
    match (&v[0], &v[1], &v[2]) {
        (
            PushVerdict::Created {
                offset: 0, pid: p0, ..
            },
            PushVerdict::Created { offset: 1, .. },
            PushVerdict::Created { offset: 2, .. },
        ) => assert_eq!(*p0, pid),
        other => panic!("expected three gapless Created verdicts: {other:?}"),
    }
    let p = c.partition(pid).unwrap();
    assert_eq!(p.last_offset, 2);
    assert_eq!(c.partition_counter(pid, Counter::Pushed), 3);
}

#[test]
fn a_duplicate_returns_the_original_offset_and_writes_nothing() {
    let mut cfg = qcfg();
    cfg.dedup_window_seconds = 600;
    let mut c = Cell::new("push-dup");
    c.run(&[push_cfg(1, "q", "p0", &["a", "b"], cfg.clone())]);
    let pid = c.pid_of("q", "p0").unwrap();
    assert_eq!(c.partition(pid).unwrap().last_offset, 1);

    // "b" repeats and "d" is new: b duplicates at its ORIGINAL offset (1), d
    // gets the next gapless offset (2).
    c.advance(1000);
    let cy = c.run(&[push_cfg(2, "q", "p0", &["b", "d"], cfg)]);
    let v = verdicts(&cy.outcome(0)).clone();
    match (&v[0], &v[1]) {
        (PushVerdict::Duplicate { offset: 1, .. }, PushVerdict::Created { offset: 2, .. }) => {}
        other => panic!("expected (Duplicate@1, Created@2): {other:?}"),
    }
    assert_eq!(c.partition(pid).unwrap().last_offset, 2);
}

#[test]
fn an_all_duplicate_push_is_not_logged() {
    let mut cfg = qcfg();
    cfg.dedup_window_seconds = 600;
    let mut c = Cell::new("push-alldup");
    c.run(&[push_cfg(1, "q", "p0", &["a", "b"], cfg.clone())]);
    c.advance(1000);
    let cy = c.run(&[push_cfg(2, "q", "p0", &["a", "b"], cfg)]);
    assert!(
        !cy.logged,
        "a push with no survivor produces no entry (§5.4)"
    );
    match cy.plan(0) {
        Ok(Plan::Empty(Outcome::Push(o))) => {
            assert!(o
                .items
                .iter()
                .all(|v| matches!(v, PushVerdict::Duplicate { .. })));
        }
        other => panic!("expected an Empty all-duplicate push: {other:?}"),
    }
}

#[test]
fn created_at_is_strictly_monotone_per_partition() {
    let mut c = Cell::new("push-mono");
    let cy1 = c.run(&[push(1, "q", "p0", &["a"])]);
    let now1 = match &verdicts(&cy1.outcome(0))[0] {
        PushVerdict::Created { created_at_us, .. } => *created_at_us,
        v => panic!("{v:?}"),
    };
    // Same wall time as the previous cycle: the stamp still rises by at least
    // 1µs (PUSHSER, via max(now, last_created + 1)).
    let cy2 = c.run(&[push(2, "q", "p0", &["b"])]);
    let now2 = match &verdicts(&cy2.outcome(0))[0] {
        PushVerdict::Created { created_at_us, .. } => *created_at_us,
        v => panic!("{v:?}"),
    };
    assert!(now2 > now1, "created_at did not advance: {now1} -> {now2}");
}

#[test]
fn dedup_off_never_probes() {
    // With no dedup window every "a" is a fresh frame at a new offset.
    let mut c = Cell::new("push-nodedup");
    c.run(&[push(1, "q", "p0", &["a"])]);
    c.advance(1000);
    let cy = c.run(&[push(2, "q", "p0", &["a"])]);
    match &verdicts(&cy.outcome(0))[0] {
        PushVerdict::Created { offset: 1, .. } => {}
        v => panic!("dedup-off should have queued a second 'a' at offset 1: {v:?}"),
    }
}

#[test]
fn two_partitions_of_one_queue_get_distinct_pids_and_independent_offsets() {
    let mut c = Cell::new("push-two");
    c.run(&[push(1, "q", "p0", &["a", "b"])]);
    c.advance(1000);
    c.run(&[push(2, "q", "p1", &["x"])]);
    let p0 = c.pid_of("q", "p0").unwrap();
    let p1 = c.pid_of("q", "p1").unwrap();
    assert_ne!(p0, p1);
    assert_eq!(c.partition(p0).unwrap().last_offset, 1);
    assert_eq!(c.partition(p1).unwrap().last_offset, 0);
    let _ = TENANT;
}
