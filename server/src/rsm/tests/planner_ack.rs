//! Ack, renew and the DLQ head against the tiny apply loop — the port of
//! `native/tests/ack.rs` and the pitfalls of §8 row 005.

use super::planner_harness::{
    ack, ack_pos, ack_pos_with_release, nack, pop_pinned, pop_pinned_with, push, push_cfg, qcfg,
    renew, Cell,
};
use crate::rsm::entry::{AckResult, Outcome, PopClaim, PopOutcome, RenewOutcome};
use crate::rsm::planner::{AckStatus::*, Plan};
use crate::rsm::store::keys::Counter;
use crate::rsm::store::rows::PartitionRow;

fn ackres(o: &Outcome) -> AckResult {
    match o {
        Outcome::Ack(a) => {
            assert_eq!(a.results.len(), 1, "one target");
            a.results[0].clone()
        }
        other => panic!("not an ack outcome: {other:?}"),
    }
}

fn one_claim(o: &Outcome) -> PopClaim {
    match o {
        Outcome::Pop(PopOutcome { claims }) => {
            assert_eq!(claims.len(), 1);
            claims[0].clone()
        }
        other => panic!("not a pop: {other:?}"),
    }
}

/// Push `txns` to a fresh partition and lease the whole batch to `worker`.
/// Returns the pid.
fn lease(
    c: &mut Cell,
    base: u64,
    queue: &str,
    partition: &str,
    txns: &[&str],
    worker: &str,
) -> u64 {
    c.run(&[push(base, queue, partition, txns)]);
    c.advance(1_000_000);
    let cy = c.run(&[pop_pinned(base + 1, queue, partition, "g", worker)]);
    let _ = one_claim(&cy.outcome(0));
    c.advance(1000);
    c.pid_of(queue, partition).unwrap()
}

#[test]
fn acking_the_last_message_implicitly_completes_the_whole_batch() {
    let mut c = Cell::new("ack-implicit");
    let pid = lease(&mut c, 1, "q", "p0", &["a", "b", "c"], "w1");
    let cy = c.run(&[ack(3, pid, "q", "g", "w1", &[("c", Ok)])]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.committed, 2, "the whole batch completed");
    assert!(r.lease_released);
    assert!(c.cursor(pid, "g").unwrap().worker.is_none());
}

#[test]
fn a_partial_ack_keeps_the_lease_and_parks_the_attempt_marker() {
    let mut c = Cell::new("ack-partial");
    let pid = lease(&mut c, 1, "q", "p0", &["a", "b", "c"], "w1");
    let cy = c.run(&[ack(3, pid, "q", "g", "w1", &[("b", Ok)])]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.committed, 1, "a and b completed, c still leased");
    assert!(!r.lease_released);
    let cur = c.cursor(pid, "g").unwrap();
    assert_eq!(cur.batch_end, Some(2), "the lease is kept");
    assert_eq!(
        cur.attempt_offset,
        Some(2),
        "the attempt marker parks on the tail"
    );
}

#[test]
fn an_explicit_signal_is_never_skipped_by_a_later_completed_ack() {
    let mut c = Cell::new("ack-signal");
    let pid = lease(&mut c, 1, "q", "p0", &["a", "b", "c"], "w1");
    // a failed (offset 0), c completed (offset 2): the cursor must stop below a.
    let cy = c.run(&[ack(3, pid, "q", "g", "w1", &[("a", Failed), ("c", Ok)])]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.committed, -1, "nothing crosses the failed head");
    assert_eq!(r.batch_retry_count, 1, "failed charged the budget once");
    assert!(r.lease_released, "budget remained, so the batch redelivers");
}

#[test]
fn at_the_same_lowest_offset_dlq_beats_failed() {
    let mut c = Cell::new("ack-rank");
    let pid = lease(&mut c, 1, "q", "p0", &["a", "b"], "w1");
    // Both statuses at offset 0 (same hash "a"): dlq outranks failed.
    let cy = c.run(&[ack(3, pid, "q", "g", "w1", &[("a", Failed), ("a", Dlq)])]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.dlq, 1, "a dlq was filed, not a retry");
    assert_eq!(c.dlq_rows().len(), 1);
}

#[test]
fn a_forced_dlq_files_the_head_and_advances_past_it() {
    let mut c = Cell::new("ack-dlq");
    let pid = lease(&mut c, 1, "q", "p0", &["a", "b", "c"], "w1");
    let cy = c.run(&[ack(3, pid, "q", "g", "w1", &[("a", Dlq)])]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.committed, 0, "advanced past the poison at 0");
    assert_eq!(r.dlq, 1);
    let rows = c.dlq_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].offset, 0);
    assert_eq!(rows[0].txn, "a");
    assert!(r.lease_released);
    assert_eq!(c.partition_counter(pid, Counter::DlqCount), 1);
}

#[test]
fn retries_exhausted_files_the_dlq_head_and_the_budget_is_charged_only_by_failed() {
    let mut cfg = qcfg();
    cfg.retry_limit = 1;
    let mut c = Cell::new("ack-exhaust");
    c.run(&[push_cfg(1, "q", "p0", &["a", "b"], cfg.clone())]);
    c.advance(1_000_000);
    let pid = c.pid_of("q", "p0").unwrap();
    // Lease, fail: budget 0 < 1, released, count = 1.
    c.run(&[pop_pinned(2, "q", "p0", "g", "w1")]);
    c.advance(1000);
    let cy = c.run(&[ack(3, pid, "q", "g", "w1", &[("a", Failed)])]);
    assert_eq!(ackres(&cy.outcome(0)).batch_retry_count, 1);
    // Re-lease, fail again: budget 1 !< 1, DLQ enabled -> file the head.
    c.advance(1000);
    c.run(&[pop_pinned(4, "q", "p0", "g", "w1")]);
    c.advance(1000);
    let cy = c.run(&[ack(5, pid, "q", "g", "w1", &[("a", Failed)])]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.dlq, 1, "the poison went to the DLQ");
    assert_eq!(c.dlq_rows().len(), 1);
    assert_eq!(
        r.batch_retry_count, 0,
        "a completed batch resets the budget"
    );
}

#[test]
fn retries_exhausted_with_no_dlq_drops_the_poison_and_advances() {
    let mut cfg = qcfg();
    cfg.retry_limit = 0;
    cfg.dead_letter_queue = false;
    cfg.dlq_after_max_retries = false;
    let mut c = Cell::new("ack-drop");
    c.run(&[push_cfg(1, "q", "p0", &["a", "b"], cfg.clone())]);
    c.advance(1_000_000);
    let pid = c.pid_of("q", "p0").unwrap();
    c.run(&[pop_pinned(2, "q", "p0", "g", "w1")]);
    c.advance(1000);
    let cy = c.run(&[ack(3, pid, "q", "g", "w1", &[("a", Failed)])]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.committed, 0, "the poison at 0 was dropped and passed");
    assert_eq!(r.dlq, 0);
    assert!(c.dlq_rows().is_empty());
}

#[test]
fn an_explicit_retry_releases_the_lease_without_charging_the_budget() {
    let mut c = Cell::new("ack-retry");
    let pid = lease(&mut c, 1, "q", "p0", &["a", "b"], "w1");
    let cy = c.run(&[ack(3, pid, "q", "g", "w1", &[("a", Retry)])]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.committed, -1);
    assert_eq!(r.batch_retry_count, 0, "retry never charges the budget");
    assert!(r.lease_released);
}

#[test]
fn a_below_cursor_ack_is_a_noop_and_a_below_cursor_signal_is_stale() {
    let mut c = Cell::new("ack-below");
    let pid = lease(&mut c, 1, "q", "p0", &["a", "b", "c"], "w1");
    // Complete a and b (partial, lease kept).
    c.run(&[ack(3, pid, "q", "g", "w1", &[("b", Ok)])]);
    assert_eq!(c.cursor(pid, "g").unwrap().committed, 1);
    c.advance(1000);
    // Now ack a completed (below cursor) -> noop; ack a failed -> stale.
    let cy = c.run(&[ack(4, pid, "q", "g", "w1", &[("a", Ok)])]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(
        r.noop_hashes.len(),
        1,
        "a completed ack below the cursor is a noop"
    );
    c.advance(1000);
    let cy = c.run(&[ack(5, pid, "q", "g", "w1", &[("a", Failed)])]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(
        r.stale_hashes.len(),
        1,
        "a signal below the cursor is stale"
    );
}

#[test]
fn an_unknown_hash_is_reported_stale_and_never_counted() {
    let mut c = Cell::new("ack-unknown");
    let pid = lease(&mut c, 1, "q", "p0", &["a", "b"], "w1");
    let cy = c.run(&[ack(3, pid, "q", "g", "w1", &[("never-pushed", Ok)])]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(
        r.committed, -1,
        "the cursor did not move for an unresolved hash"
    );
    assert_eq!(r.stale_hashes.len(), 1);
}

#[test]
fn an_ack_from_the_wrong_worker_is_rejected() {
    let mut c = Cell::new("ack-wrongworker");
    let pid = lease(&mut c, 1, "q", "p0", &["a"], "w1");
    let cy = c.run(&[ack(3, pid, "q", "g", "w2", &[("a", Ok)])]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.committed, -1, "no advance on a foreign worker");
    assert_eq!(r.stale_hashes.len(), 1);
    assert!(!cy.logged, "a rejected ack writes nothing");
}

#[test]
fn a_positional_full_batch_ack_lands_where_the_hash_ack_does() {
    let mut c = Cell::new("ack-pos");
    let pid = lease(&mut c, 1, "q", "p0", &["a", "b", "c"], "w1");
    let cy = c.run(&[ack_pos(3, pid, "q", "g", "w1", Some(2), true, 3)]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.committed, 2);
    assert!(r.lease_released);
}

#[test]
fn a_streams_full_batch_ack_uses_the_recorded_batch_end() {
    let mut c = Cell::new("ack-pos-streams-full");
    let pid = lease(&mut c, 1, "q", "p0", &["a", "b", "c"], "w1");
    let cy = c.run(&[ack_pos_with_release(
        3, pid, "q", "g", "w1", None, true, true, 3,
    )]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.committed, 2, "the recorded batch end is authoritative");
    assert_eq!(r.acked, 3);
    assert!(r.lease_released);
    let cur = c.cursor(pid, "g").unwrap();
    assert!(cur.worker.is_none());
    assert!(cur.batch_end.is_none());
}

#[test]
fn a_streams_partial_ack_advances_the_count_and_retains_the_lease() {
    let mut c = Cell::new("ack-pos-streams-partial");
    let pid = lease(&mut c, 1, "q", "p0", &["a", "b", "c"], "w1");
    let cy = c.run(&[ack_pos_with_release(
        3, pid, "q", "g", "w1", None, true, false, 2,
    )]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.committed, 1, "exactly two delivered frames advance");
    assert_eq!(r.acked, 2);
    assert!(!r.lease_released);
    let cur = c.cursor(pid, "g").unwrap();
    assert_eq!(cur.worker.as_deref(), Some("w1"));
    assert_eq!(cur.batch_end, Some(2));
    assert_eq!(cur.attempt_offset, Some(2));
}

#[test]
fn the_o16_fast_path_advances_a_full_delivered_set() {
    // The whole delivered set is acked in one call: the fast path advances the
    // cursor to batch_end and releases, no per-hash resolution.
    let mut c = Cell::new("ack-fast");
    let pid = lease(&mut c, 1, "q", "p0", &["a", "b", "c"], "w1");
    let cy = c.run(&[ack(
        3,
        pid,
        "q",
        "g",
        "w1",
        &[("a", Ok), ("b", Ok), ("c", Ok)],
    )]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.committed, 2);
    assert!(r.lease_released);
}

#[test]
fn a_partial_set_takes_the_slow_path_and_advances_implicitly() {
    // A partial delivered set misses the fast path; the slow path advances the
    // cursor implicitly to the highest acked offset and keeps the lease.
    let mut c = Cell::new("ack-partialset");
    let pid = lease(&mut c, 1, "q", "p0", &["x", "y", "z"], "w1");
    let cy = c.run(&[ack(3, pid, "q", "g", "w1", &[("x", Ok), ("y", Ok)])]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.committed, 1, "implicit ack to y, z still leased");
    assert!(!r.lease_released);
}

#[test]
fn a_positional_nack_releases_the_lease_without_moving_the_cursor() {
    let mut c = Cell::new("ack-nack");
    let pid = lease(&mut c, 1, "q", "p0", &["a", "b"], "w1");
    let cy = c.run(&[nack(3, pid, "q", "g", "w1")]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.committed, -1);
    assert!(r.lease_released);
    assert!(c.cursor(pid, "g").unwrap().worker.is_none());
}

#[test]
fn a_positional_ack_beyond_the_leased_batch_is_refused() {
    let mut c = Cell::new("ack-beyond");
    let pid = lease(&mut c, 1, "q", "p0", &["a", "b"], "w1");
    match c.one(ack_pos(3, pid, "q", "g", "w1", Some(9), true, 1)) {
        Err(r) => assert_eq!(r.code, "beyond_batch"),
        other => panic!("expected a refusal: {other:?}"),
    }
}

#[test]
fn a_clean_ack_of_a_conflating_lease_retires_the_whole_span() {
    let mut c = Cell::new("ack-conflate");
    c.run(&[push(1, "q", "p0", &["a", "b", "c"])]);
    c.advance(1_000_000);
    let cy = c.run(&[pop_pinned_with(2, "q", "p0", "g", "w1", |p| {
        p.conflate = true
    })]);
    let claim = one_claim(&cy.outcome(0));
    assert!(claim.conflated);
    let pid = c.pid_of("q", "p0").unwrap();
    c.advance(1000);
    // The delivered set is {c}; acking c retires (committed, 2] = 3 positions.
    let cy = c.run(&[ack(3, pid, "q", "g", "w1", &[("c", Ok)])]);
    let r = ackres(&cy.outcome(0));
    assert_eq!(r.committed, 2);
    assert_eq!(r.conflated, 2, "two skipped frames counted");
    assert_eq!(c.cursor(pid, "g").unwrap().total_consumed, 3);
}

#[test]
fn renew_extends_only_the_live_leases_of_that_worker() {
    let mut c = Cell::new("ack-renew");
    let pid0 = lease(&mut c, 1, "q", "p0", &["a"], "w1");
    let _pid1 = lease(&mut c, 10, "q", "p1", &["b"], "w2");
    let before = c.cursor(pid0, "g").unwrap().lease_expires_at_us.unwrap();
    c.advance(1000);
    let cy = c.run(&[renew(30, "w1", 300)]);
    let ro: RenewOutcome = match cy.outcome(0) {
        Outcome::Renew(r) => r,
        o => panic!("{o:?}"),
    };
    assert_eq!(ro.renewed, 1, "only w1's one live lease");
    let after = c.cursor(pid0, "g").unwrap().lease_expires_at_us.unwrap();
    assert!(
        after >= before,
        "GREATEST never shortens: {before} -> {after}"
    );
    assert_eq!(ro.min_expires_at_us, Some(after));
}

#[test]
fn a_renew_keeps_the_delivered_set_so_the_fast_path_survives() {
    let mut c = Cell::new("ack-renew-fast");
    let pid = lease(&mut c, 1, "q", "p0", &["a", "b"], "w1");
    let before = c.cursor(pid, "g").unwrap().delivered.clone();
    c.advance(1000);
    c.run(&[renew(3, "w1", 300)]);
    assert_eq!(
        c.cursor(pid, "g").unwrap().delivered,
        before,
        "renew left the delivered set intact"
    );
    // The full-batch ack still lands.
    c.advance(1000);
    let cy = c.run(&[ack(4, pid, "q", "g", "w1", &[("a", Ok), ("b", Ok)])]);
    assert_eq!(ackres(&cy.outcome(0)).committed, 1);
}

#[test]
fn the_dlq_row_outlives_its_frame_and_carries_the_snapshot() {
    let mut c = Cell::new("ack-dlq-snapshot");
    let pid = lease(&mut c, 1, "q", "p0", &["poison", "b"], "w1");
    c.run(&[ack(3, pid, "q", "g", "w1", &[("poison", Dlq)])]);
    let rows = c.dlq_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].txn, "poison");
    assert!(
        !rows[0].payload.is_empty(),
        "the receiver's snapshot is filed"
    );
    let _ = PartitionRow::new([0u8; 16], "t", "q", "p", 0);
    let _ = Plan::Empty(Outcome::Empty);
}
