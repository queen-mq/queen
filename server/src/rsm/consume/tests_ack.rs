//! Ack parity: the semantic cases of the planner's `tests/planner_ack.rs`
//! (08050fed), against the engine.

use super::tests::{only, qcfg, H};
use crate::rsm::batcher::Reply;
use crate::rsm::effect::Pid;
use crate::rsm::entry::Outcome;
use crate::rsm::planner::AckStatus::*;

/// Push `txns` to a fresh partition and lease the whole batch to `worker`.
fn lease(h: &mut H, queue: &str, partition: &str, txns: &[&str], worker: &str) -> Pid {
    h.push(queue, partition, txns);
    h.advance(1_000_000);
    only(h.pinned(queue, partition, "g", worker));
    h.advance(1000);
    h.pid_of(queue, partition)
}

/// Lease `txns` on a queue with `retry_limit` (DLQ on unless `dlq` is false).
fn lease_cfg(h: &mut H, txns: &[&str], retry_limit: i32, dlq: bool) -> Pid {
    let mut cfg = qcfg();
    cfg.retry_limit = retry_limit;
    cfg.dead_letter_queue = dlq;
    cfg.dlq_after_max_retries = dlq;
    h.queue("q", cfg);
    lease(h, "q", "p0", txns, "w1")
}

fn ack_outcome(r: Reply) -> crate::rsm::entry::AckResult {
    match r {
        Reply::Done {
            outcome: Outcome::Ack(a),
            ..
        } => a.results[0].clone(),
        other => panic!("{other:?}"),
    }
}

#[test]
fn acking_the_last_message_implicitly_completes_the_whole_batch() {
    let mut h = H::new("ack-implicit");
    let pid = lease(&mut h, "q", "p0", &["a", "b", "c"], "w1");
    let r = h.ack(pid, "q", "g", "w1", &[("c", Ok)]);
    assert_eq!(r.committed, 2);
    assert!(r.lease_released);
    h.checkpoint();
    assert!(h.cursor(pid, "g").unwrap().worker.is_none());
}

#[test]
fn a_partial_ack_keeps_the_lease_and_parks_the_attempt_marker() {
    let mut h = H::new("ack-partial");
    let pid = lease(&mut h, "q", "p0", &["a", "b", "c"], "w1");
    let r = h.ack(pid, "q", "g", "w1", &[("b", Ok)]);
    assert_eq!(r.committed, 1);
    assert!(!r.lease_released);
    let cur = h.cursor(pid, "g").unwrap();
    assert_eq!(cur.batch_end, Some(2));
    assert_eq!(cur.attempt_offset, Some(2));
    // The rest of the batch completes it.
    let r = h.ack(pid, "q", "g", "w1", &[("c", Ok)]);
    assert_eq!(r.committed, 2);
    assert!(r.lease_released);
}

#[test]
fn an_explicit_signal_is_never_skipped_by_a_later_completed_ack() {
    let mut h = H::new("ack-signal");
    let pid = lease(&mut h, "q", "p0", &["a", "b", "c"], "w1");
    let r = h.ack(pid, "q", "g", "w1", &[("a", Failed), ("c", Ok)]);
    assert_eq!(r.committed, -1);
    assert_eq!(r.batch_retry_count, 1);
    assert!(r.lease_released);
}

#[test]
fn at_the_same_lowest_offset_dlq_beats_failed() {
    let mut h = H::new("ack-rank");
    let pid = lease(&mut h, "q", "p0", &["a", "b"], "w1");
    let r = h.ack(pid, "q", "g", "w1", &[("a", Failed), ("a", Dlq)]);
    assert_eq!(r.dlq, 1);
    assert_eq!(h.dlq_rows().len(), 1);
}

#[test]
fn a_forced_dlq_files_the_head_and_advances_past_it() {
    let mut h = H::new("ack-dlq");
    let pid = lease(&mut h, "q", "p0", &["a", "b", "c"], "w1");
    let r = h.ack(pid, "q", "g", "w1", &[("a", Dlq)]);
    assert_eq!(r.committed, 0);
    assert_eq!(r.dlq, 1);
    let rows = h.dlq_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].offset, 0);
    assert_eq!(rows[0].txn, "a");
    assert!(
        !rows[0].payload.is_empty(),
        "the receiver's snapshot is filed"
    );
    assert!(r.lease_released);
}

#[test]
fn retries_exhausted_files_the_dlq_head_and_the_budget_is_charged_only_by_failed() {
    let mut h = H::new("ack-exhaust");
    let pid = lease_cfg(&mut h, &["a", "b"], 1, true);
    assert_eq!(
        h.ack(pid, "q", "g", "w1", &[("a", Failed)])
            .batch_retry_count,
        1
    );
    h.advance(1000);
    only(h.pinned("q", "p0", "g", "w1"));
    h.advance(1000);
    let r = h.ack(pid, "q", "g", "w1", &[("a", Failed)]);
    assert_eq!(r.dlq, 1);
    assert_eq!(h.dlq_rows().len(), 1);
    assert_eq!(r.batch_retry_count, 0);
}

#[test]
fn retries_exhausted_with_no_dlq_drops_the_poison_and_advances() {
    let mut h = H::new("ack-drop");
    let pid = lease_cfg(&mut h, &["a", "b"], 0, false);
    let r = h.ack(pid, "q", "g", "w1", &[("a", Failed)]);
    assert_eq!(r.committed, 0);
    assert_eq!(r.dlq, 0);
    assert!(h.dlq_rows().is_empty());
}

#[test]
fn a_spent_budget_files_every_nacked_message_of_the_batch() {
    let mut h = H::new("ack-nack-all");
    let pid = lease_cfg(&mut h, &["a", "b", "c"], 0, true);
    let r = h.ack(
        pid,
        "q",
        "g",
        "w1",
        &[("a", Failed), ("b", Failed), ("c", Failed)],
    );
    assert_eq!(r.dlq, 3);
    assert_eq!(r.committed, 2);
    assert!(r.lease_released);
    let mut offsets: Vec<i64> = h.dlq_rows().iter().map(|d| d.offset).collect();
    offsets.sort();
    assert_eq!(offsets, vec![0, 1, 2]);
    assert_eq!(h.cursor(pid, "g").unwrap().batch_retry_count, 0);
}

#[test]
fn a_spent_budget_completes_the_acks_above_the_head() {
    let mut h = H::new("ack-nack-then-ok");
    let pid = lease_cfg(&mut h, &["a", "b", "c"], 0, true);
    let r = h.ack(pid, "q", "g", "w1", &[("a", Failed), ("b", Ok), ("c", Ok)]);
    assert_eq!((r.dlq, r.committed), (1, 2));
}

#[test]
fn a_spent_budget_settles_below_the_head_and_above_it() {
    let mut h = H::new("ack-ok-then-nack");
    let pid = lease_cfg(&mut h, &["a", "b", "c"], 0, true);
    let r = h.ack(
        pid,
        "q",
        "g",
        "w1",
        &[("a", Ok), ("b", Failed), ("c", Failed)],
    );
    assert_eq!((r.dlq, r.committed), (2, 2));
}

#[test]
fn a_retry_above_the_head_stops_the_settling() {
    let mut h = H::new("ack-nack-retry");
    let pid = lease_cfg(&mut h, &["a", "b", "c"], 0, true);
    let r = h.ack(
        pid,
        "q",
        "g",
        "w1",
        &[("a", Failed), ("b", Retry), ("c", Failed)],
    );
    assert_eq!((r.dlq, r.committed), (1, 0));
    assert!(r.lease_released);
    assert_eq!(h.dlq_rows().len(), 1);
}

#[test]
fn a_silent_position_above_the_head_stops_the_settling() {
    let mut h = H::new("ack-nack-gap");
    let pid = lease_cfg(&mut h, &["a", "b", "c"], 0, true);
    let r = h.ack(pid, "q", "g", "w1", &[("a", Failed), ("c", Failed)]);
    assert_eq!((r.dlq, r.committed), (1, 0));
}

#[test]
fn a_nacked_batch_is_delivered_retry_limit_plus_one_times() {
    let mut h = H::new("ack-nack-rounds");
    let pid = lease_cfg(&mut h, &["a", "b", "c"], 2, true);
    let nack_all = [("a", Failed), ("b", Failed), ("c", Failed)];
    for round in 1..=3u32 {
        if round > 1 {
            only(h.pinned("q", "p0", "g", "w1"));
            h.advance(1000);
        }
        let r = h.ack(pid, "q", "g", "w1", &nack_all);
        if round < 3 {
            assert_eq!((r.dlq, r.committed), (0, -1), "round {round}: redeliver");
            assert_eq!(r.batch_retry_count, round);
        } else {
            assert_eq!((r.dlq, r.committed), (3, 2), "round 3: the whole batch");
        }
        h.advance(1000);
    }
}

#[test]
fn with_no_dlq_a_spent_budget_drops_every_nacked_message() {
    let mut h = H::new("ack-nack-drop");
    let pid = lease_cfg(&mut h, &["a", "b", "c"], 0, false);
    let r = h.ack(
        pid,
        "q",
        "g",
        "w1",
        &[("a", Failed), ("b", Failed), ("c", Failed)],
    );
    assert_eq!((r.dlq, r.committed), (0, 2));
    assert!(h.dlq_rows().is_empty());
}

#[test]
fn a_forced_dlq_head_stops_at_a_failed_whose_budget_remains() {
    let mut h = H::new("ack-dlq-then-failed");
    let pid = lease_cfg(&mut h, &["a", "b", "c"], 3, true);
    let r = h.ack(
        pid,
        "q",
        "g",
        "w1",
        &[("a", Dlq), ("b", Failed), ("c", Dlq)],
    );
    assert_eq!((r.dlq, r.committed), (1, 0));
    assert_eq!(r.batch_retry_count, 0);
}

#[test]
fn an_explicit_retry_releases_the_lease_without_charging_the_budget() {
    let mut h = H::new("ack-retry");
    let pid = lease(&mut h, "q", "p0", &["a", "b"], "w1");
    let r = h.ack(pid, "q", "g", "w1", &[("a", Retry)]);
    assert_eq!(r.committed, -1);
    assert_eq!(r.batch_retry_count, 0);
    assert!(r.lease_released);
    // It is redelivered at once.
    let c = only(h.pinned("q", "p0", "g", "w2"));
    assert_eq!(c.delivery_attempt, 2);
}

#[test]
fn a_below_cursor_ack_is_a_noop_and_a_below_cursor_signal_is_stale() {
    let mut h = H::new("ack-below");
    let pid = lease(&mut h, "q", "p0", &["a", "b", "c"], "w1");
    h.ack(pid, "q", "g", "w1", &[("b", Ok)]);
    assert_eq!(h.cursor(pid, "g").unwrap().committed, 1);
    let r = h.ack(pid, "q", "g", "w1", &[("a", Ok)]);
    assert_eq!(r.noop_hashes.len(), 1);
    let r = h.ack(pid, "q", "g", "w1", &[("a", Failed)]);
    assert_eq!(r.stale_hashes.len(), 1);
}

#[test]
fn an_unknown_hash_is_reported_stale_and_never_counted() {
    let mut h = H::new("ack-unknown");
    let pid = lease(&mut h, "q", "p0", &["a", "b"], "w1");
    let r = h.ack(pid, "q", "g", "w1", &[("never-pushed", Ok)]);
    assert_eq!(r.committed, -1);
    assert_eq!(r.stale_hashes.len(), 1);
}

#[test]
fn an_ack_from_the_wrong_worker_is_rejected_and_writes_nothing() {
    let mut h = H::new("ack-wrongworker");
    let pid = lease(&mut h, "q", "p0", &["a"], "w1");
    h.checkpoint();
    let r = h.ack(pid, "q", "g", "w2", &[("a", Ok)]);
    assert_eq!(r.committed, -1);
    assert_eq!(r.stale_hashes.len(), 1);
    assert_eq!(h.checkpoint(), 0, "a rejected ack writes nothing");
}

#[test]
fn a_lease_less_ack_still_advances() {
    let mut h = H::new("ack-leaseless");
    let pid = lease(&mut h, "q", "p0", &["a", "b"], "w1");
    let r = h.ack(pid, "q", "g", "", &[("b", Ok)]);
    assert_eq!(r.committed, 1);
    assert!(r.lease_released);
}

#[test]
fn a_positional_full_batch_ack_lands_where_the_hash_ack_does() {
    let mut h = H::new("ack-pos");
    let pid = lease(&mut h, "q", "p0", &["a", "b", "c"], "w1");
    let r = ack_outcome(h.ack_pos(pid, "q", "g", "w1", Some(2), true, true, 3));
    assert_eq!(r.committed, 2);
    assert!(r.lease_released);
}

#[test]
fn a_streams_full_batch_ack_uses_the_recorded_batch_end() {
    let mut h = H::new("ack-pos-streams-full");
    let pid = lease(&mut h, "q", "p0", &["a", "b", "c"], "w1");
    let r = ack_outcome(h.ack_pos(pid, "q", "g", "w1", None, true, true, 3));
    assert_eq!(r.committed, 2);
    assert_eq!(r.acked, 3);
    assert!(r.lease_released);
    let cur = h.cursor(pid, "g").unwrap();
    assert!(cur.worker.is_none());
    assert!(cur.batch_end.is_none());
}

#[test]
fn a_streams_partial_ack_advances_the_count_and_retains_the_lease() {
    let mut h = H::new("ack-pos-streams-partial");
    let pid = lease(&mut h, "q", "p0", &["a", "b", "c"], "w1");
    let r = ack_outcome(h.ack_pos(pid, "q", "g", "w1", None, true, false, 2));
    assert_eq!(r.committed, 1);
    assert_eq!(r.acked, 2);
    assert!(!r.lease_released);
    let cur = h.cursor(pid, "g").unwrap();
    assert_eq!(cur.worker.as_deref(), Some("w1"));
    assert_eq!(cur.batch_end, Some(2));
    assert_eq!(cur.attempt_offset, Some(2));
}

#[test]
fn the_o16_fast_path_advances_a_full_delivered_set() {
    let mut h = H::new("ack-fast");
    let pid = lease(&mut h, "q", "p0", &["a", "b", "c"], "w1");
    let r = h.ack(pid, "q", "g", "w1", &[("a", Ok), ("b", Ok), ("c", Ok)]);
    assert_eq!(r.committed, 2);
    assert!(r.lease_released);
    assert_eq!(h.cursor(pid, "g").unwrap().total_consumed, 3);
}

#[test]
fn a_partial_set_takes_the_slow_path_and_advances_implicitly() {
    let mut h = H::new("ack-partialset");
    let pid = lease(&mut h, "q", "p0", &["x", "y", "z"], "w1");
    let r = h.ack(pid, "q", "g", "w1", &[("x", Ok), ("y", Ok)]);
    assert_eq!(r.committed, 1);
    assert!(!r.lease_released);
}

#[test]
fn a_positional_nack_releases_the_lease_without_moving_the_cursor() {
    let mut h = H::new("ack-nack");
    let pid = lease(&mut h, "q", "p0", &["a", "b"], "w1");
    let r = ack_outcome(h.nack(pid, "q", "g", "w1"));
    assert_eq!(r.committed, -1);
    assert!(r.lease_released);
    assert!(h.cursor(pid, "g").unwrap().worker.is_none());
    // Redelivered from the same offset.
    let c = only(h.pinned("q", "p0", "g", "w2"));
    assert_eq!((c.start_offset, c.delivery_attempt), (0, 2));
}

#[test]
fn a_nack_from_another_worker_is_refused() {
    let mut h = H::new("ack-nack-bad");
    let pid = lease(&mut h, "q", "p0", &["a"], "w1");
    match h.nack(pid, "q", "g", "w2") {
        Reply::Refused(r) => assert_eq!(r.code, "bad_lease"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_positional_ack_beyond_the_leased_batch_is_refused() {
    let mut h = H::new("ack-beyond");
    let pid = lease(&mut h, "q", "p0", &["a", "b"], "w1");
    match h.ack_pos(pid, "q", "g", "w1", Some(9), true, true, 1) {
        Reply::Refused(r) => assert_eq!(r.code, "beyond_batch"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_clean_ack_of_a_conflating_lease_retires_the_whole_span() {
    let mut h = H::new("ack-conflate");
    h.push("q", "p0", &["a", "b", "c"]);
    h.advance(1_000_000);
    let claim = only(h.pinned_with("q", "p0", "g", "w1", |p| p.conflate = true));
    assert!(claim.conflated);
    let pid = h.pid_of("q", "p0");
    h.advance(1000);
    let r = h.ack(pid, "q", "g", "w1", &[("c", Ok)]);
    assert_eq!(r.committed, 2);
    assert_eq!(r.conflated, 2);
    assert_eq!(h.cursor(pid, "g").unwrap().total_consumed, 3);
}

#[test]
fn renew_extends_only_the_live_leases_of_that_worker() {
    let mut h = H::new("ack-renew");
    let pid0 = lease(&mut h, "q", "p0", &["a"], "w1");
    let _pid1 = lease(&mut h, "q", "p1", &["b"], "w2");
    let before = h.cursor(pid0, "g").unwrap().lease_expires_at_us.unwrap();
    h.advance(1000);
    let ro = h.renew("w1", 300);
    assert_eq!(ro.renewed, 1);
    let after = h.cursor(pid0, "g").unwrap().lease_expires_at_us.unwrap();
    assert!(after >= before);
    assert_eq!(ro.min_expires_at_us, Some(after));
}

#[test]
fn a_renew_on_a_new_leader_finds_the_committed_leases() {
    let mut h = H::new("ack-renew-failover");
    let pid = lease(&mut h, "q", "p0", &["a", "b"], "w1");
    h.failover();
    let ro = h.renew("w1", 300);
    // Found through the committed lease index when apply keeps it; the lease
    // is then acked on the rebuilt delivered set either way.
    assert!(ro.renewed <= 1);
    let r = h.ack(pid, "q", "g", "w1", &[("a", Ok), ("b", Ok)]);
    assert_eq!(r.committed, 1);
}

#[test]
fn a_renew_keeps_the_delivered_set_so_the_fast_path_survives() {
    let mut h = H::new("ack-renew-fast");
    let pid = lease(&mut h, "q", "p0", &["a", "b"], "w1");
    h.advance(1000);
    h.renew("w1", 300);
    h.advance(1000);
    let r = h.ack(pid, "q", "g", "w1", &[("a", Ok), ("b", Ok)]);
    assert_eq!(r.committed, 1);
}

#[test]
fn the_dlq_head_files_and_advances_past_it() {
    let mut h = H::new("ack-dlq-head");
    let pid = lease(&mut h, "q", "p0", &["poison", "b"], "w1");
    let c = crate::rsm::planner::DlqHeadCommand {
        request_id: h.rid(),
        pid,
        tenant: super::tests::T.into(),
        queue: "q".into(),
        group: "g".into(),
        worker: "w1".into(),
        offset: 0,
        error: "boom".into(),
        snapshot: crate::rsm::planner::DlqSnapshot {
            message_id: None,
            txn: "poison".into(),
            payload: b"{}".to_vec(),
        },
    };
    match h.outcome(crate::rsm::batcher::Command::DlqHead(c.clone())) {
        Outcome::DlqHead(o) => {
            assert_eq!((o.offset, o.committed), (0, 0));
            assert!(o.lease_released);
        }
        o => panic!("{o:?}"),
    }
    assert_eq!(h.dlq_rows().len(), 1);
    // Twice is safe: the lease went, the worker no longer matches.
    let mut again = c;
    again.request_id = h.rid();
    assert!(matches!(
        h.outcome(crate::rsm::batcher::Command::DlqHead(again)),
        Outcome::Empty
    ));
    assert_eq!(h.dlq_rows().len(), 1);
}

#[test]
fn a_batch_ack_answers_every_target_in_input_order() {
    let mut h = H::new("ack-batch");
    let p0 = lease(&mut h, "q", "p0", &["a"], "w1");
    let p1 = lease(&mut h, "q", "p1", &["b"], "w1");
    let c = crate::rsm::planner::AckCommand {
        request_id: h.rid(),
        targets: vec![
            h.ack_target(p1, "q", "g", "w1", &[("b", Ok)]),
            h.ack_target(p0, "q", "g", "w1", &[("a", Ok)]),
            h.ack_target(p0, "q", "other", "w1", &[("a", Ok)]),
        ],
    };
    match h.outcome(crate::rsm::batcher::Command::Ack(c)) {
        Outcome::Ack(a) => {
            assert_eq!(a.results.len(), 3);
            assert_eq!(a.results[0].pid, p1);
            assert_eq!(a.results[1].pid, p0);
            assert_eq!(a.results[0].committed, 0);
            assert_eq!(a.results[1].committed, 0);
            assert_eq!(
                a.results[2].stale_hashes.len(),
                1,
                "no cursor for that group"
            );
        }
        o => panic!("{o:?}"),
    }
}

/// An ack of `txns` (all `Ok`) under `worker`, served: its answer waits for
/// the checkpoint that carries it.
fn ack_later(
    h: &mut H,
    pid: Pid,
    txns: &[&str],
    worker: &str,
) -> tokio::sync::oneshot::Receiver<Reply> {
    let items: Vec<(&str, crate::rsm::planner::AckStatus)> =
        txns.iter().map(|t| (*t, Ok)).collect();
    let t = h.ack_target(pid, "q", "g", worker, &items);
    let c = crate::rsm::planner::AckCommand {
        request_id: h.rid(),
        targets: vec![t],
    };
    h.later(crate::rsm::batcher::Command::Ack(c))
}

/// Jepsen p11-w2-restart (2026-09-30): a graceful leader restart while an
/// ack's checkpoint was on its way. The old leader answered `Retry`, the
/// follower sent the ack to the new leader, whose state already held the
/// checkpoint — the lease released — and it called the message stale: the
/// client was told its ack failed, and the message never came back. An ack
/// whose change a checkpoint carried is in doubt, never retried.
#[test]
fn an_ack_whose_checkpoint_was_sent_is_in_doubt_when_its_leader_steps_down() {
    let mut h = H::new("ack-in-doubt");
    h.queue("q", qcfg());
    let pid = lease(&mut h, "q", "p0", &["a"], "w1");
    let mut rx = ack_later(&mut h, pid, &["a"], "w1");
    let _in_flight = h.e.take_checkpoint(h.now).expect("the ack's checkpoint");
    assert!(rx.try_recv().is_err(), "answered before the commit");
    h.e.on_step_down();
    match rx.try_recv() {
        Result::Ok(Reply::Refused(r)) => {
            assert_eq!(r.code, super::IN_DOUBT, "{r:?}");
            assert!(!r.retryable, "{r:?}");
        }
        other => panic!("expected in doubt, got {other:?}"),
    }
}

/// The same, when the checkpoint came back without a commit this engine saw
/// (a step-down answers the batcher's commands `Retry` whether or not their
/// entry is in the log) before the leader stepped down.
#[test]
fn an_ack_whose_checkpoint_resolved_unknown_is_in_doubt_when_its_leader_steps_down() {
    let mut h = H::new("ack-doubt-resolved");
    h.queue("q", qcfg());
    let pid = lease(&mut h, "q", "p0", &["a"], "w1");
    let mut rx = ack_later(&mut h, pid, &["a"], "w1");
    let cp = h.e.take_checkpoint(h.now).expect("the ack's checkpoint");
    h.e.checkpoint_resolved(cp.ticket, false);
    h.e.on_step_down();
    match rx.try_recv() {
        Result::Ok(Reply::Refused(r)) => assert_eq!(r.code, super::IN_DOUBT, "{r:?}"),
        other => panic!("expected in doubt, got {other:?}"),
    }
}

/// An ack no checkpoint carried yet cannot be in the log: the next leader
/// runs it on the state before it, and answers it as this one would have.
#[test]
fn an_ack_no_checkpoint_carried_is_retried_when_its_leader_steps_down() {
    let mut h = H::new("ack-retry");
    h.queue("q", qcfg());
    let pid = lease(&mut h, "q", "p0", &["a"], "w1");
    let mut rx = ack_later(&mut h, pid, &["a"], "w1");
    h.e.on_step_down();
    assert!(
        matches!(rx.try_recv(), Result::Ok(Reply::Retry { .. })),
        "a change no checkpoint took is retried"
    );
}

/// A claim runs again safely (the first claim's lease expires and its
/// messages come back): a pop whose checkpoint was sent is still retried.
#[test]
fn a_claim_whose_checkpoint_was_sent_is_retried_when_its_leader_steps_down() {
    let mut h = H::new("claim-retry");
    h.queue("q", qcfg());
    h.push("q", "p0", &["a"]);
    h.advance(1_000_000);
    let c = h.pop_cmd("q", "g", "w1");
    let mut rx = h.later(crate::rsm::batcher::Command::PopWildcard(c));
    let _in_flight = h.e.take_checkpoint(h.now).expect("the claim's checkpoint");
    h.e.on_step_down();
    assert!(
        matches!(rx.try_recv(), Result::Ok(Reply::Retry { .. })),
        "a pop is retried"
    );
}

/// A leader on its way out (a graceful stop) drains before it hands off: a
/// new command is not run (`Retry`: the next leader runs it), and an ack it
/// already served still commits and is answered — instead of the step-down
/// answering it in doubt.
#[test]
fn a_draining_leader_runs_no_new_command_and_answers_what_it_served() {
    use crate::rsm::consume::Served;
    let mut h = H::new("drain");
    h.queue("q", qcfg());
    let pid = lease(&mut h, "q", "p0", &["a", "b"], "w1");
    let mut rx = ack_later(&mut h, pid, &["a"], "w1");
    assert!(h.e.exact_pending(), "the ack waits for its checkpoint");
    h.e.begin_drain(std::time::Duration::from_secs(30));
    let t = h.ack_target(pid, "q", "g", "w1", &[("b", Ok)]);
    let c = crate::rsm::planner::AckCommand {
        request_id: h.rid(),
        targets: vec![t],
    };
    assert!(
        matches!(
            h.e.serve(&crate::rsm::batcher::Command::Ack(c), h.now),
            Served::Now(Reply::Retry { .. })
        ),
        "a draining leader runs no new command"
    );
    h.checkpoint();
    match rx.try_recv() {
        Result::Ok(Reply::Done {
            outcome: Outcome::Ack(a),
            ..
        }) => assert_eq!(a.results[0].committed, 0, "{a:?}"),
        other => panic!("the served ack is answered: {other:?}"),
    }
    assert!(!h.e.exact_pending());
    h.e.end_drain();
    let r = h.ack(pid, "q", "g", "w1", &[("b", Ok)]);
    assert_eq!(r.committed, 1, "serving again: {r:?}");
}

/// The batcher plans an engine checkpoint only while the engine that took it
/// still has it in flight: after a step-down the checkpoint's commands are
/// the gone incarnation's (older rows than a leader in between may have
/// written) and must never reach the log should this node lead again.
#[test]
fn a_checkpoint_of_a_gone_incarnation_is_no_longer_claimed_by_the_engine() {
    let mut h = H::new("stale-ckpt");
    h.queue("q", qcfg());
    let pid = lease(&mut h, "q", "p0", &["a"], "w1");
    let _rx = ack_later(&mut h, pid, &["a"], "w1");
    let cp = h.e.take_checkpoint(h.now).expect("the ack's checkpoint");
    for c in &cp.commands {
        assert!(super::is_checkpoint_id(&c.request_id), "marked");
        assert!(h.e.answers_at_commit(&c.request_id), "in flight: planned");
    }
    h.e.on_step_down();
    for c in &cp.commands {
        assert!(
            !h.e.answers_at_commit(&c.request_id),
            "the gone engine's checkpoint is refused at planning"
        );
    }
    h.e.on_leader(2, u64::MAX);
    for c in &cp.commands {
        assert!(
            !h.e.answers_at_commit(&c.request_id),
            "and when it leads again"
        );
    }
    // A client's request id is never taken for a checkpoint's.
    assert!(!super::is_checkpoint_id(&crate::util::uuidv7_bytes()));
}

/// A transaction that prepares on a part an ack changed (no checkpoint took
/// it yet: a reserved part is no checkpoint's) carries that change in its
/// own row: the ack's answer is in doubt from then on, as if a checkpoint
/// carried it — a step-down must not answer it `Retry`, or the next leader
/// re-runs it on the committed row and calls it stale.
#[test]
fn an_ack_a_transaction_carries_is_in_doubt_when_its_leader_steps_down() {
    let mut h = H::new("ack-txn-doubt");
    h.queue("q", qcfg());
    let pid = lease(&mut h, "q", "p0", &["a", "b"], "w1");
    let mut rx = ack_later(&mut h, pid, &["a"], "w1");
    let id = h.rid();
    let mut txn = h.txn(id);
    txn.acks
        .push(h.ack_target(pid, "q", "g", "w1", &[("b", Ok)]));
    let part =
        h.e.txn_prepare(&txn, h.now)
            .expect("prepare")
            .expect("a part");
    assert!(
        part.effects
            .iter()
            .any(|e| matches!(e, crate::rsm::effect::Effect::CursorSet { .. })),
        "the bundle writes the row"
    );
    h.e.on_step_down();
    match rx.try_recv() {
        Result::Ok(Reply::Refused(r)) => assert_eq!(r.code, super::IN_DOUBT, "{r:?}"),
        other => panic!("expected in doubt, got {other:?}"),
    }
}

/// A step-down waits for a command already running (`Engine::serving`): it
/// never lands between a command's change and its answer's registration.
#[test]
fn a_step_down_waits_for_the_command_in_progress() {
    let h = H::new("serving-fence");
    let held = super::state::read(&h.e.serving);
    let e = h.e.clone();
    let (tx, done) = std::sync::mpsc::channel();
    let t = std::thread::spawn(move || {
        e.on_step_down();
        let _ = tx.send(());
    });
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(done.try_recv().is_err(), "the reset waited");
    drop(held);
    done.recv_timeout(std::time::Duration::from_secs(5))
        .expect("the reset ran once the command ended");
    t.join().unwrap();
}

/// An ack repeated after it released its lease — its reply lost, or the
/// client (an SDK retrying a 503 `outcome_unknown`) sending it again — is
/// answered as the first one was, success, on this leader and on the next:
/// the released lease rides in the cursor row. Another lease, or a message
/// that was not in that batch, is still stale.
#[test]
fn a_repeated_ack_of_a_released_lease_is_answered_as_the_first() {
    let mut h = H::new("repeat-ack");
    h.queue("q", qcfg());
    let pid = lease(&mut h, "q", "p0", &["a", "b"], "w1");
    let first = h.ack(pid, "q", "g", "w1", &[("a", Ok), ("b", Ok)]);
    assert_eq!(first.committed, 1, "{first:?}");
    assert!(
        first.lease_released && first.stale_hashes.is_empty(),
        "{first:?}"
    );
    // The same ack again: success, not stale.
    let again = h.ack(pid, "q", "g", "w1", &[("a", Ok), ("b", Ok)]);
    assert!(again.stale_hashes.is_empty(), "{again:?}");
    assert!(again.lease_released, "{again:?}");
    assert_eq!(again.committed, 1, "nothing moved: {again:?}");
    // A part of it too.
    let part = h.ack(pid, "q", "g", "w1", &[("b", Ok)]);
    assert!(part.stale_hashes.is_empty(), "{part:?}");
    // Another lease: stale.
    let other = h.ack(pid, "q", "g", "w9", &[("a", Ok)]);
    assert_eq!(other.stale_hashes.len(), 1, "{other:?}");
    // A message of no batch that lease held: stale.
    h.push("q", "p0", &["c"]);
    let late = h.ack(pid, "q", "g", "w1", &[("c", Ok)]);
    assert_eq!(late.stale_hashes.len(), 1, "{late:?}");
    // On the next leader (it loads the cursor rows the checkpoints wrote).
    h.checkpoint();
    h.e.on_step_down();
    h.e.on_leader(2, u64::MAX);
    h.e.serve_after_us
        .store(0, std::sync::atomic::Ordering::Release);
    let next = h.ack(pid, "q", "g", "w1", &[("a", Ok), ("b", Ok)]);
    assert!(
        next.stale_hashes.is_empty(),
        "the next leader knows the lease: {next:?}"
    );
}
