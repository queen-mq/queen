//! WP-2.3 — timers on the RSM (025 `log_timers_*` ported): apply, planner, and
//! the real facade end to end.
//!
//! - **apply**: the three kinds maintain the `timers` row AND its `timers_due`
//!   fire-order entry, deterministically (two nodes, one digest), and replay
//!   skips what is already applied.
//! - **planner**: schedule/cancel verdicts; the fire step appends ONCE and
//!   deletes the timer in the same command; a cancel drained in the fire's own
//!   cycle wins; an in-flight fire is never planned twice; the dedup net; the
//!   permanent backoff ladder and the `__timer__` dead letter; the transient
//!   backoff that spends no attempt; tenant isolation; the step's bounds.
//! - **facade**: schedule → fires into the queue at its due time → poppable;
//!   cancel before due → never fires; the backoff/DLQ path; a retried request
//!   id answered from the recorded outcome; list and count; a clean restart.
//!
//! The kill -9 half (no durable point) is `timers_crash.rs`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use base64::Engine;
use serde_json::{json, Value};

use super::apply::{open_at, Build, Node, BASE_US as APPLY_BASE_US};
use super::planner_harness::{rid, Cell, Cmd, TENANT};
use crate::rsm::batcher::BatcherConfig;
use crate::rsm::effect::{Effect, TimerRow};
use crate::rsm::entry::Outcome;
use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{
    Deadline, PopReq, PushReq, ReqCtx, Rsm, RsmBuildCtx, RsmError, TimerPeekReq, TimersCountReq,
    TimersListReq, TimersReq,
};
use crate::rsm::planner::timers::{
    parse_timer_ops, timers_results, TimerFireConfig, TimerOp, TimersCommand, TIMER_DLQ_GROUP,
};
use crate::rsm::planner::Overlay;
use crate::rsm::store::rows::timer_due_us;
use crate::rsm::store::{Store, TypedReads};

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

fn b64(s: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

fn sched_op(queue: &str, key: &str, delay_ms: i64, txn: &str, payload: &str) -> Value {
    json!({
        "op": "schedule", "queue": queue, "timerKey": key,
        "delayMs": delay_ms, "txn": txn, "payload": b64(payload)
    })
}

fn cancel_op(queue: &str, key: &str) -> Value {
    json!({"op": "cancel", "queue": queue, "timerKey": key})
}

/// A timers command for the planner cell, built through the receiver path.
fn timers_cmd(id: u64, tenant: &str, ops: &[Value]) -> Cmd {
    Cmd::Timers(TimersCommand {
        request_id: rid(id),
        tenant: tenant.to_string(),
        ops: parse_timer_ops(ops, Some("svc")).expect("valid ops"),
    })
}

fn results_of(o: &Outcome) -> Vec<Value> {
    timers_results(o).expect("a timers outcome")
}

fn fire_cfg() -> TimerFireConfig {
    TimerFireConfig::default()
}

fn timer_of(cell: &Cell, tenant: &str, queue: &str, key: &str) -> Option<TimerRow> {
    cell.node
        .store()
        .read(|r| r.timer(tenant, queue, key))
        .expect("read timer")
}

fn due_index(cell: &Cell) -> Vec<(i64, String, String, String)> {
    cell.node
        .store()
        .read(|r| {
            let mut out = Vec::new();
            r.scan_timers_due(usize::MAX, &mut |d, t, q, k| {
                out.push((d, t.to_string(), q.to_string(), k.to_string()));
                true
            })?;
            Ok(out)
        })
        .expect("read due index")
}

fn appends(effects: &[Effect]) -> Vec<(u64, u32)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Append { pid, count, .. } => Some((*pid, *count)),
            _ => None,
        })
        .collect()
}

fn deletes(effects: &[Effect]) -> Vec<String> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::TimerDelete { key, .. } => Some(key.clone()),
            _ => None,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Apply
// ---------------------------------------------------------------------------

fn row(deliver_at_us: i64, txn: &str) -> TimerRow {
    TimerRow {
        partition: "Default".into(),
        deliver_at_us,
        visible_at_us: None,
        frame: crate::frames::pack_frames(&[crate::frames::FrameIn {
            message_id: [7u8; 16],
            txn,
            trace_id: None,
            producer_sub: None,
            payload: b"{}",
            encrypted: false,
        }]),
        payload_zstd: false,
        encrypted: false,
        txn: txn.into(),
        message_id: [7u8; 16],
        attempts: 0,
        last_error: None,
        producer_sub: None,
        created_at_us: deliver_at_us - 10,
        updated_at_us: deliver_at_us - 10,
    }
}

fn node_timer(node: &Node, key: &str) -> Option<TimerRow> {
    node.store()
        .read(|r| r.timer("t1", "q", key))
        .expect("read")
}

fn node_due(node: &Node) -> Vec<(i64, String)> {
    node.store()
        .read(|r| {
            let mut out = Vec::new();
            r.scan_timers_due(usize::MAX, &mut |d, _t, _q, k| {
                out.push((d, k.to_string()));
                true
            })?;
            Ok(out)
        })
        .expect("read")
}

/// The entry script both apply tests run.
fn timer_script() -> Vec<crate::rsm::apply::Committed> {
    let t = APPLY_BASE_US;
    vec![
        // 1: two timers.
        Build::new(t + 1, 1, 0)
            .cmd(vec![
                Effect::TimerUpsert {
                    tenant: "t1".into(),
                    queue: "q".into(),
                    key: "a".into(),
                    row: row(t + 5_000, "ta"),
                },
                Effect::TimerUpsert {
                    tenant: "t1".into(),
                    queue: "q".into(),
                    key: "b".into(),
                    row: row(t + 9_000, "tb"),
                },
            ])
            .at(1, 1),
        // 2: reschedule `a` later, back `b` off.
        Build::new(t + 2, 1, 10)
            .cmd(vec![
                Effect::TimerUpsert {
                    tenant: "t1".into(),
                    queue: "q".into(),
                    key: "a".into(),
                    row: row(t + 20_000, "ta2"),
                },
                Effect::TimerBackoff {
                    tenant: "t1".into(),
                    queue: "q".into(),
                    key: "b".into(),
                    visible_at_us: t + 30_000,
                    attempts: 1,
                    last_error: Some("boom".into()),
                    updated_at_us: t + 2,
                },
            ])
            .at(2, 1),
        // 3: delete `a`, and a delete / backoff of a key that is not there.
        Build::new(t + 3, 1, 20)
            .cmd(vec![
                Effect::TimerDelete {
                    tenant: "t1".into(),
                    queue: "q".into(),
                    key: "a".into(),
                },
                Effect::TimerDelete {
                    tenant: "t1".into(),
                    queue: "q".into(),
                    key: "ghost".into(),
                },
                Effect::TimerBackoff {
                    tenant: "t1".into(),
                    queue: "q".into(),
                    key: "ghost".into(),
                    visible_at_us: t + 1,
                    attempts: 9,
                    last_error: None,
                    updated_at_us: t + 3,
                },
            ])
            .at(3, 1),
    ]
}

#[test]
fn timer_effects_maintain_the_row_and_its_fire_order_entry() {
    let node = Node::new("timers-apply");
    let t = APPLY_BASE_US;
    let script = timer_script();
    {
        let (mut a, _) = open_at(&node);
        a.apply(&script[0]).expect("entry 1");
        a.durable_point().expect("durable");
    }
    assert_eq!(
        node_due(&node),
        vec![(t + 5_000, "a".to_string()), (t + 9_000, "b".to_string())],
        "one fire-order entry per timer, earliest first"
    );
    {
        let (mut a, _) = open_at(&node);
        a.apply(&script[1]).expect("entry 2");
        a.durable_point().expect("durable");
    }
    let a_row = node_timer(&node, "a").expect("a rescheduled");
    assert_eq!(a_row.txn, "ta2");
    let b_row = node_timer(&node, "b").expect("b backed off");
    assert_eq!(b_row.attempts, 1);
    assert_eq!(b_row.last_error.as_deref(), Some("boom"));
    assert_eq!(b_row.visible_at_us, Some(t + 30_000));
    assert_eq!(b_row.deliver_at_us, t + 9_000, "a backoff keeps deliver_at");
    assert_eq!(
        node_due(&node),
        vec![(t + 20_000, "a".to_string()), (t + 30_000, "b".to_string())],
        "the old entries moved, none left behind"
    );
    {
        let (mut a, _) = open_at(&node);
        a.apply(&script[2])
            .expect("entry 3 (missing rows are no-ops)");
        a.durable_point().expect("durable");
    }
    assert!(node_timer(&node, "a").is_none());
    assert!(
        node_timer(&node, "ghost").is_none(),
        "a backoff never creates a row"
    );
    assert_eq!(node_due(&node), vec![(t + 30_000, "b".to_string())]);

    // Replay from before the durable point is a no-op (apply's idempotence).
    let before = node.digest();
    {
        let (mut a, _) = open_at(&node);
        for c in &script {
            a.apply(c).expect("re-apply is skipped");
        }
        a.durable_point().ok();
    }
    assert_eq!(node.digest().whole, before.whole);
}

#[test]
fn two_nodes_apply_timer_entries_to_the_same_digest() {
    let script = timer_script();
    let digest = |tag: &str| {
        let node = Node::new(tag);
        {
            let (mut a, _) = open_at(&node);
            for c in &script {
                a.apply(c).expect("apply");
            }
            a.durable_point().expect("durable");
        }
        node.digest()
    };
    let (x, y) = (digest("timers-i2-a"), digest("timers-i2-b"));
    assert_eq!(x.whole, y.whole, "I2: {:?}", x.first_difference(&y));
    assert!(
        x.per_keyspace
            .iter()
            .any(|(n, _, rows)| *n == "timers" && *rows == 1),
        "the timers keyspace is replicated state: {:?}",
        x.per_keyspace
    );
}

// ---------------------------------------------------------------------------
// Planner
// ---------------------------------------------------------------------------

#[test]
fn a_schedule_answers_its_verdict_and_a_reschedule_is_one_row() {
    let mut cell = Cell::new("timers-plan-sched");
    let c = cell.run(&[timers_cmd(
        1,
        TENANT,
        &[sched_op("q", "k", 1_000, "t1", r#"{"n":1}"#)],
    )]);
    assert!(c.logged);
    let r = results_of(&c.outcome(0));
    assert_eq!(r[0]["status"], "scheduled");
    assert_eq!(r[0]["ok"], true);
    let first = timer_of(&cell, TENANT, "q", "k").expect("stored");
    assert_eq!(first.deliver_at_us, c.now_us + 1_000_000);
    assert_eq!(first.producer_sub.as_deref(), Some("svc"));

    let c2 = cell.run(&[timers_cmd(
        2,
        TENANT,
        &[sched_op("q", "k", 5_000, "t2", r#"{"n":2}"#)],
    )]);
    let r2 = results_of(&c2.outcome(0));
    assert_eq!(r2[0]["status"], "rescheduled");
    let second = timer_of(&cell, TENANT, "q", "k").expect("stored");
    assert_eq!(
        second.txn, "t2",
        "a reschedule overwrites the txn (025 §20.2)"
    );
    assert_eq!(
        second.created_at_us, first.created_at_us,
        "created_at is kept"
    );
    assert_eq!(due_index(&cell).len(), 1, "one row, one fire-order entry");

    // A cancel of a key that is not there: `absent`, nothing logged.
    let c3 = cell.run(&[timers_cmd(3, TENANT, &[cancel_op("q", "nope")])]);
    assert!(!c3.logged, "an all-absent call plans nothing (§5.4)");
    let r3 = results_of(&c3.outcome(0));
    assert_eq!(r3[0]["status"], "absent");
    assert_eq!(r3[0]["ok"], false);
}

#[test]
fn the_fire_appends_once_and_deletes_the_timer_in_one_command() {
    let mut cell = Cell::new("timers-plan-fire");
    let fc = fire_cfg();
    let c = cell.run(&[timers_cmd(
        1,
        TENANT,
        &[sched_op("orders", "k", 1_000, "tx-1", r#"{"n":1}"#)],
    )]);
    let scheduled = results_of(&c.outcome(0));
    let mid = scheduled[0]["messageId"].as_str().unwrap().to_string();

    // Not due yet: the step plans nothing.
    let (_, rep, eff) = cell.run_fire(&[], Some(&fc));
    assert_eq!(rep.fired, 0);
    assert!(eff.is_empty());
    assert!(timer_of(&cell, TENANT, "orders", "k").is_some());

    // Due: ONE command carries the implicit creation, the append and the
    // delete — the message and the timer's removal cannot be separated.
    cell.advance(1_500_000);
    let (_, rep, eff) = cell.run_fire(&[], Some(&fc));
    assert_eq!(rep.fired, 1);
    assert!(matches!(eff.first(), Some(Effect::QueueUpsert { .. })));
    assert!(eff
        .iter()
        .any(|e| matches!(e, Effect::PartitionCreate { .. })));
    assert_eq!(appends(&eff).len(), 1);
    assert_eq!(appends(&eff)[0].1, 1);
    assert_eq!(deletes(&eff), vec!["k".to_string()]);
    assert!(timer_of(&cell, TENANT, "orders", "k").is_none());
    assert!(due_index(&cell).is_empty());

    // The appended frame is the one the schedule packed: same message id.
    let blob = eff
        .iter()
        .find_map(|e| match e {
            Effect::Append { blob, .. } => Some(blob.clone()),
            _ => None,
        })
        .unwrap();
    let frames = crate::frames::unpack_frames_ref(&blob).expect("frame");
    assert_eq!(
        crate::frames::uuid_bytes_to_string(&frames[0].message_id),
        mid
    );
    assert_eq!(frames[0].txn, "tx-1");
    assert_eq!(frames[0].payload, br#"{"n":1}"#);

    let pid = cell
        .pid_of("orders", "Default")
        .expect("partition born at the fire");
    assert_eq!(cell.partition(pid).unwrap().last_offset, 0);

    // Exactly once: the next step finds nothing.
    cell.advance(1_000_000);
    let (_, rep, eff) = cell.run_fire(&[], Some(&fc));
    assert_eq!(rep.fired, 0);
    assert!(eff.is_empty());
    assert_eq!(cell.partition(pid).unwrap().last_offset, 0);
}

#[test]
fn a_cancel_drained_in_the_fires_cycle_wins() {
    let mut cell = Cell::new("timers-plan-cancel-race");
    let fc = fire_cfg();
    cell.run(&[timers_cmd(1, TENANT, &[sched_op("q", "k", 1, "tx", "{}")])]);
    cell.advance(10_000);
    // The cycle carries the cancel AND runs the fire step after it.
    let (c, rep, eff) = cell.run_fire(&[timers_cmd(2, TENANT, &[cancel_op("q", "k")])], Some(&fc));
    let r = results_of(&c.outcome(0));
    assert_eq!(r[0]["status"], "cancelled");
    assert_eq!(r[0]["txn"], "tx");
    assert_eq!(rep.fired, 0, "the cancelled timer never fires");
    assert!(eff.is_empty());
    assert!(cell.pid_of("q", "Default").is_none(), "nothing was pushed");
}

#[test]
fn a_timer_scheduled_already_due_fires_in_its_own_entry() {
    let mut cell = Cell::new("timers-plan-same-entry");
    let fc = fire_cfg();
    let (c, rep, eff) = cell.run_fire(
        &[timers_cmd(1, TENANT, &[sched_op("q", "k", -5, "tx", "{}")])],
        Some(&fc),
    );
    assert!(c.logged);
    assert_eq!(rep.fired, 1, "a delay in the past fires on the first cycle");
    assert_eq!(appends(&eff).len(), 1);
    assert!(timer_of(&cell, TENANT, "q", "k").is_none());
}

#[test]
fn a_fire_still_in_flight_is_never_planned_twice() {
    let mut cell = Cell::new("timers-plan-inflight");
    let fc = fire_cfg();
    cell.run(&[timers_cmd(1, TENANT, &[sched_op("q", "k", 1, "tx", "{}")])]);
    cell.advance(10_000);
    // The fire entry is planned but NOT applied: it is in the pipeline.
    let in_flight = cell.plan_entry(&[], Some(&fc)).expect("a fire entry");
    assert!(
        timer_of(&cell, TENANT, "q", "k").is_some(),
        "not applied yet"
    );
    // The next cycle folds it: the timer is gone from its view.
    let (rep, eff) = cell.plan_fire_over(std::slice::from_ref(&in_flight), &fc);
    assert_eq!(rep.fired, 0, "exactly once in effect: {rep:?}");
    assert!(eff.is_empty());
    // Without the fold it WOULD fire — the overlay is what prevents it.
    let (rep, _) = cell.plan_fire_over(&[], &fc);
    assert_eq!(rep.fired, 1);
}

#[test]
fn a_fired_txn_already_in_the_dedup_window_is_done_without_an_append() {
    let mut cell = Cell::new("timers-plan-dup");
    let fc = fire_cfg();
    let mut cfg = super::planner_harness::qcfg();
    cfg.dedup_window_seconds = 3600;
    cell.run(&[super::planner_harness::push_cfg(
        1,
        "q",
        "Default",
        &["same-txn"],
        cfg,
    )]);
    let pid = cell.pid_of("q", "Default").unwrap();
    cell.run(&[timers_cmd(
        2,
        TENANT,
        &[sched_op("q", "k", 1, "same-txn", "{}")],
    )]);
    cell.advance(10_000);
    let (_, rep, eff) = cell.run_fire(&[], Some(&fc));
    assert_eq!(rep.duplicates, 1);
    assert_eq!(rep.fired, 0);
    assert!(
        appends(&eff).is_empty(),
        "025's duplicate arm: nothing appended"
    );
    assert_eq!(
        deletes(&eff),
        vec!["k".to_string()],
        "…and the timer is done"
    );
    assert_eq!(cell.partition(pid).unwrap().last_offset, 0);
}

#[test]
fn two_timers_sharing_a_txn_in_one_step_deliver_once() {
    let mut cell = Cell::new("timers-plan-shared-txn");
    let fc = fire_cfg();
    cell.run(&[timers_cmd(
        1,
        TENANT,
        &[
            sched_op("q", "a", 1, "shared", r#"{"k":"a"}"#),
            sched_op("q", "b", 2, "shared", r#"{"k":"b"}"#),
        ],
    )]);
    cell.advance(10_000);
    let (_, rep, eff) = cell.run_fire(&[], Some(&fc));
    assert_eq!((rep.fired, rep.duplicates), (1, 1), "{rep:?}");
    assert_eq!(appends(&eff).iter().map(|a| a.1).sum::<u32>(), 1);
    let mut d = deletes(&eff);
    d.sort();
    assert_eq!(d, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn a_permanent_failure_backs_off_exponentially_then_dead_letters() {
    let mut cell = Cell::new("timers-plan-backoff");
    let fc = TimerFireConfig {
        fail_queue: Some("poison".into()),
        max_attempts: 3,
        backoff_min_ms: 1_000,
        backoff_max_ms: 60_000,
        ..TimerFireConfig::default()
    };
    cell.run(&[timers_cmd(
        1,
        TENANT,
        &[sched_op("poison", "k", 1, "tx-p", r#"{"bad":true}"#)],
    )]);
    cell.advance(10_000);

    // Attempt 1 fails: backoff min * 2^0.
    let (c, rep, eff) = cell.run_fire(&[], Some(&fc));
    assert_eq!(rep.backed_off, 1);
    assert!(appends(&eff).is_empty());
    let t = timer_of(&cell, TENANT, "poison", "k").expect("still pending");
    assert_eq!(t.attempts, 1);
    assert_eq!(t.visible_at_us, Some(c.now_us + 1_000_000));
    assert!(t.last_error.as_deref().unwrap().contains("injected"));
    assert_eq!(timer_due_us(&t), c.now_us + 1_000_000);

    // Not visible yet: nothing is planned for it.
    cell.advance(500_000);
    let (_, rep, _) = cell.run_fire(&[], Some(&fc));
    assert_eq!(rep, Default::default());

    // Attempt 2: backoff min * 2^1.
    cell.advance(600_000);
    let (c, rep, _) = cell.run_fire(&[], Some(&fc));
    assert_eq!(rep.backed_off, 1);
    let t = timer_of(&cell, TENANT, "poison", "k").unwrap();
    assert_eq!(t.attempts, 2);
    assert_eq!(t.visible_at_us, Some(c.now_us + 2_000_000));

    // Attempt 3 exhausts the budget: dead-lettered, the timer deleted, the
    // destination provisioned so the dead letter is findable.
    cell.advance(2_100_000);
    let (_, rep, eff) = cell.run_fire(&[], Some(&fc));
    assert_eq!(rep.dead_lettered, 1, "{rep:?}");
    assert!(appends(&eff).is_empty(), "a dead letter is not a delivery");
    assert!(timer_of(&cell, TENANT, "poison", "k").is_none());
    let pid = cell.pid_of("poison", "Default").expect("provisioned");
    let dlq = cell.dlq_rows();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].group, TIMER_DLQ_GROUP);
    assert_eq!(dlq[0].offset, -1);
    assert_eq!(dlq[0].pid, pid);
    assert_eq!(dlq[0].txn, "tx-p");
    assert_eq!(dlq[0].retry_count, 3);
    assert_eq!(dlq[0].payload, br#"{"bad":true}"#);
    assert!(dlq[0].message_id.is_some());
    assert_eq!(
        cell.partition(pid).unwrap().last_offset,
        -1,
        "nothing appended"
    );
}

#[test]
fn a_transient_failure_backs_off_without_spending_an_attempt() {
    let mut cell = Cell::new("timers-plan-transient");
    let fc = TimerFireConfig {
        fail_queue: Some("flaky".into()),
        fail_transient: true,
        max_attempts: 1,
        transient_backoff_ms: 250,
        ..TimerFireConfig::default()
    };
    cell.run(&[timers_cmd(
        1,
        TENANT,
        &[sched_op("flaky", "k", 1, "tx", "{}")],
    )]);
    for _ in 0..3 {
        cell.advance(300_000);
        let (c, rep, _) = cell.run_fire(&[], Some(&fc));
        assert_eq!(rep.backed_off, 1);
        let t = timer_of(&cell, TENANT, "flaky", "k").expect("never dead-lettered");
        assert_eq!(t.attempts, 0, "infrastructure never spends the DLQ budget");
        assert_eq!(t.visible_at_us, Some(c.now_us + 250_000));
    }
    assert!(cell.dlq_rows().is_empty());
}

#[test]
fn a_reschedule_after_a_backoff_is_a_new_timer_with_a_fresh_budget() {
    let mut cell = Cell::new("timers-plan-resched-backoff");
    let fc = TimerFireConfig {
        fail_queue: Some("p".into()),
        ..TimerFireConfig::default()
    };
    cell.run(&[timers_cmd(1, TENANT, &[sched_op("p", "k", 1, "tx", "{}")])]);
    cell.advance(10_000);
    cell.run_fire(&[], Some(&fc));
    assert_eq!(timer_of(&cell, TENANT, "p", "k").unwrap().attempts, 1);
    // A row in backoff stays CANCELLABLE and RESCHEDULABLE (025 §4.3).
    let c = cell.run(&[timers_cmd(
        2,
        TENANT,
        &[sched_op("p", "k", 1_000, "tx2", "{}")],
    )]);
    assert_eq!(results_of(&c.outcome(0))[0]["status"], "rescheduled");
    let t = timer_of(&cell, TENANT, "p", "k").unwrap();
    assert_eq!(
        (t.attempts, t.last_error.clone(), t.visible_at_us),
        (0, None, None)
    );
    let c = cell.run(&[timers_cmd(3, TENANT, &[cancel_op("p", "k")])]);
    assert_eq!(results_of(&c.outcome(0))[0]["status"], "cancelled");
}

#[test]
fn two_tenants_with_the_same_names_never_share_a_push() {
    let mut cell = Cell::new("timers-plan-tenants");
    let fc = fire_cfg();
    cell.run(&[
        timers_cmd(
            1,
            "tenant-a",
            &[sched_op("q", "k", 1, "tx", r#"{"t":"a"}"#)],
        ),
        timers_cmd(
            2,
            "tenant-b",
            &[sched_op("q", "k", 1, "tx", r#"{"t":"b"}"#)],
        ),
    ]);
    cell.advance(10_000);
    let (_, rep, eff) = cell.run_fire(&[], Some(&fc));
    assert_eq!(
        rep.fired, 2,
        "same txn, different tenants: no dedup across them"
    );
    let creates: Vec<String> = eff
        .iter()
        .filter_map(|e| match e {
            Effect::PartitionCreate { tenant, .. } => Some(tenant.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(creates.len(), 2, "one partition per tenant: {creates:?}");
    assert_eq!(appends(&eff).len(), 2, "one push per tenant, never fused");
}

#[test]
fn the_fire_step_is_bounded_and_says_so() {
    let mut cell = Cell::new("timers-plan-bounded");
    let fc = TimerFireConfig {
        batch: 2,
        ..TimerFireConfig::default()
    };
    let ops: Vec<Value> = (0..5)
        .map(|i| sched_op("q", &format!("k{i}"), i, &format!("tx{i}"), "{}"))
        .collect();
    cell.run(&[timers_cmd(1, TENANT, &ops)]);
    cell.advance(10_000);
    let (_, rep, _) = cell.run_fire(&[], Some(&fc));
    assert_eq!(rep.fired, 2);
    assert!(rep.more, "three more are due now");
    // Earliest first.
    assert!(timer_of(&cell, TENANT, "q", "k0").is_none());
    assert!(timer_of(&cell, TENANT, "q", "k1").is_none());
    let (_, rep, _) = cell.run_fire(&[], Some(&fc));
    assert_eq!((rep.fired, rep.more), (2, true));
    let (_, rep, _) = cell.run_fire(&[], Some(&fc));
    assert_eq!((rep.fired, rep.more), (1, false));
    let pid = cell.pid_of("q", "Default").unwrap();
    assert_eq!(
        cell.partition(pid).unwrap().last_offset,
        4,
        "five messages, gapless"
    );
}

#[test]
fn the_reusable_helper_refuses_a_repeated_key_and_folds_nothing() {
    let cell = Cell::new("timers-plan-helper");
    let ops = vec![
        TimerOp::Cancel {
            queue: "q".into(),
            key: "k".into(),
            txn: None,
        },
        TimerOp::Cancel {
            queue: "q".into(),
            key: "k".into(),
            txn: None,
        },
    ];
    cell.node
        .store()
        .read(|r| {
            let d = crate::rsm::state::Derived::default();
            let committed = crate::rsm::state::Committed::new(r, &d);
            let front = crate::rsm::dedup::DedupFront::disabled();
            let p = crate::rsm::planner::Planner::new(
                committed,
                super::planner_harness::BASE_US,
                crate::rsm::planner::PlanConfig::default(),
                &front,
                None,
            );
            let mut ov = Overlay::new(1, 1);
            let err = p.plan_timer_ops(&mut ov, TENANT, &ops).unwrap_err();
            assert_eq!(err.code, "timers_bad_request");
            // Nothing folded: a schedule of the same key is still `scheduled`.
            let sched = parse_timer_ops(&[sched_op("q", "k", 1, "tx", "{}")], None).unwrap();
            let (effects, results) = p.plan_timer_ops(&mut ov, TENANT, &sched).unwrap();
            assert_eq!(effects.len(), 1);
            assert!(matches!(
                results[0],
                crate::rsm::planner::timers::TimerOpResult::Scheduled {
                    rescheduled: false,
                    ..
                }
            ));
            // ...and the helper's effects ARE folded: a cancel now finds it.
            let cancel = vec![TimerOp::Cancel {
                queue: "q".into(),
                key: "k".into(),
                txn: None,
            }];
            let (effects, _) = p.plan_timer_ops(&mut ov, TENANT, &cancel).unwrap();
            assert!(matches!(effects[0], Effect::TimerDelete { .. }));
            Ok(())
        })
        .unwrap();
}

#[test]
fn an_oversized_call_is_refused_before_anything_is_planned() {
    let cell = Cell::new("timers-plan-toolarge");
    let big = "x".repeat(4096);
    let cmd = TimersCommand {
        request_id: rid(1),
        tenant: TENANT.into(),
        ops: parse_timer_ops(&[sched_op("q", "k", 1, "tx", &big)], None).unwrap(),
    };
    cell.node
        .store()
        .read(|r| {
            let d = crate::rsm::state::Derived::default();
            let committed = crate::rsm::state::Committed::new(r, &d);
            let front = crate::rsm::dedup::DedupFront::disabled();
            let cfg = crate::rsm::planner::PlanConfig {
                entry_max_bytes: 1024,
                ..Default::default()
            };
            let p = crate::rsm::planner::Planner::new(
                committed,
                super::planner_harness::BASE_US,
                cfg,
                &front,
                None,
            );
            let mut ov = Overlay::new(1, 1);
            match p.plan_timers(&mut ov, &cmd) {
                Err(r) => assert_eq!(r.code, "too_large"),
                other => panic!("expected too_large, got {other:?}"),
            }
            Ok(())
        })
        .unwrap();
}

// ---------------------------------------------------------------------------
// The real facade, end to end
// ---------------------------------------------------------------------------

static SEQ: AtomicU64 = AtomicU64::new(0);

pub(super) fn scratch(tag: &str) -> PathBuf {
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-timers-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

pub(super) fn build_ctx(dir: &Path) -> RsmBuildCtx {
    RsmBuildCtx {
        data_dir: dir.display().to_string(),
        notifier: crate::notify::Notifier::new(false),
    }
}

pub(super) fn ctx() -> ReqCtx {
    ReqCtx::new("default", Deadline::after(Duration::from_secs(5)))
}

/// A fast tick so a test waits milliseconds, not the production 50 ms.
pub(super) fn fast_cfg() -> BatcherConfig {
    BatcherConfig {
        timer_tick_ms: 10,
        ..BatcherConfig::from_env()
    }
}

pub(super) async fn apply(f: &RaftFacade, ops: Vec<Value>) -> Vec<Value> {
    f.timers_apply(
        ctx(),
        TimersReq {
            ops,
            producer_sub: Some("svc".into()),
        },
    )
    .await
    .expect("timers apply")
    .results
}

pub(super) async fn peek(f: &RaftFacade, queue: &str, key: &str) -> Value {
    let out = f
        .timer_peek(
            ctx(),
            TimerPeekReq {
                queue: queue.into(),
                timer_key: key.into(),
            },
        )
        .await
        .expect("peek");
    serde_json::from_str(&out.body).expect("peek JSON")
}

/// Poll until the timer is gone (fired, cancelled or dead-lettered).
pub(super) async fn wait_gone(f: &RaftFacade, queue: &str, key: &str, within: Duration) -> bool {
    let end = Instant::now() + within;
    while Instant::now() < end {
        if peek(f, queue, key).await["found"] == false {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

pub(super) async fn pop_all(f: &RaftFacade, queue: &str) -> Vec<Value> {
    let out = f
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: queue.into(),
                group: None,
                batch: 100,
                auto_ack: true,
                wait: false,
                timeout_ms: 1000,
            },
        )
        .await
        .expect("pop");
    let v: Value = serde_json::from_str(&out.body).expect("pop JSON");
    v["messages"].as_array().cloned().unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_timer_fires_into_its_queue_at_its_due_time_and_is_poppable() {
    let dir = scratch("fire");
    let f = RaftFacade::open_with(&build_ctx(&dir), fast_cfg()).expect("open");

    let t0 = Instant::now();
    let r = apply(
        &f,
        vec![sched_op(
            "reminders",
            "user/42",
            600,
            "tx-42",
            r#"{"n":42}"#,
        )],
    )
    .await;
    assert_eq!(r.len(), 1);
    assert_eq!(r[0]["ok"], true);
    assert_eq!(r[0]["status"], "scheduled");
    assert_eq!(r[0]["timerKey"], "user/42");
    let mid = r[0]["messageId"]
        .as_str()
        .expect("messageId promised")
        .to_string();
    assert!(r[0]["deliverAt"].as_str().is_some_and(|s| s.ends_with('Z')));

    // Pending, and NOT delivered before its time.
    let p = peek(&f, "reminders", "user/42").await;
    assert_eq!(p["found"], true, "{p}");
    assert_eq!(p["claimed"], false);
    assert_eq!(p["attempts"], 0);
    assert_eq!(p["producerSub"], "svc");
    assert_eq!(p["payload"], b64(r#"{"n":42}"#));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(pop_all(&f, "reminders").await.is_empty(), "fired early");
    assert_eq!(peek(&f, "reminders", "user/42").await["found"], true);

    // Due: it fires on its own (the leader loop), within a few ticks.
    assert!(
        wait_gone(&f, "reminders", "user/42", Duration::from_secs(5)).await,
        "the timer never fired"
    );
    assert!(
        t0.elapsed() >= Duration::from_millis(590),
        "fired before its due time: {:?}",
        t0.elapsed()
    );
    let msgs = pop_all(&f, "reminders").await;
    assert_eq!(msgs.len(), 1, "exactly one delivery: {msgs:?}");
    assert_eq!(msgs[0]["id"], mid.as_str(), "the id promised at schedule");
    assert_eq!(msgs[0]["transactionId"], "tx-42");
    assert_eq!(msgs[0]["data"]["n"], 42);
    assert_eq!(msgs[0]["producerSub"], "svc");
    assert_eq!(msgs[0]["partition"], "Default");

    // And never again.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(pop_all(&f, "reminders").await.is_empty());

    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_timer_cancelled_before_its_due_time_never_fires() {
    let dir = scratch("cancel");
    let f = RaftFacade::open_with(&build_ctx(&dir), fast_cfg()).expect("open");

    apply(&f, vec![sched_op("q", "k", 500, "tx-c", "{}")]).await;
    let mut op = cancel_op("q", "k");
    op["txn"] = json!("tx-expected");
    let r = apply(&f, vec![op.clone()]).await;
    assert_eq!(r[0]["ok"], true);
    assert_eq!(r[0]["status"], "cancelled");
    assert_eq!(r[0]["txn"], "tx-c", "the removed timer's own txn");

    // A second cancel is `absent` and echoes the txn the caller expected.
    let r = apply(&f, vec![op]).await;
    assert_eq!(r[0]["ok"], false);
    assert_eq!(r[0]["status"], "absent");
    assert_eq!(r[0]["txn"], "tx-expected");

    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(pop_all(&f, "q").await.is_empty(), "a cancelled timer fired");
    assert_eq!(peek(&f, "q", "k").await["found"], false);

    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_failing_fire_backs_off_stays_cancellable_and_dead_letters() {
    let dir = scratch("backoff");
    let mut cfg = fast_cfg();
    cfg.timer_fire.fail_queue = Some("poison".into());
    cfg.timer_fire.max_attempts = 3;
    // Wide enough that the cancel below lands between attempts 1 and 2 of
    // `saved` on a loaded machine: 150 ms, then 300 ms, then the dead letter.
    cfg.timer_fire.backoff_min_ms = 150;
    cfg.timer_fire.backoff_max_ms = 1_000;
    let f = RaftFacade::open_with(&build_ctx(&dir), cfg).expect("open");

    apply(
        &f,
        vec![
            sched_op("poison", "doomed", 0, "tx-d", r#"{"bad":1}"#),
            sched_op("poison", "saved", 0, "tx-s", r#"{"bad":2}"#),
        ],
    )
    .await;

    // The first failure: still pending, one attempt spent, the error kept.
    let end = Instant::now() + Duration::from_secs(5);
    let mut seen = Value::Null;
    while Instant::now() < end {
        seen = peek(&f, "poison", "saved").await;
        if seen["attempts"].as_i64().unwrap_or(0) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(seen["attempts"].as_i64().unwrap_or(0) >= 1, "{seen}");
    assert!(seen["lastError"]
        .as_str()
        .unwrap_or("")
        .contains("injected"));
    assert_eq!(
        seen["claimed"], false,
        "a row in backoff is in nobody's hands"
    );

    // In backoff it is still cancellable (025 §4.3).
    let r = apply(&f, vec![cancel_op("poison", "saved")]).await;
    assert_eq!(r[0]["status"], "cancelled");

    // The other one exhausts its attempts and is dead-lettered.
    assert!(
        wait_gone(&f, "poison", "doomed", Duration::from_secs(5)).await,
        "never dead-lettered"
    );
    let mut dlq = f.dlq_rows();
    for _ in 0..200 {
        if !dlq.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        dlq = f.dlq_rows();
    }
    assert_eq!(dlq.len(), 1, "one dead letter: {dlq:?}");
    assert_eq!(dlq[0].group, TIMER_DLQ_GROUP);
    assert_eq!(dlq[0].offset, -1);
    assert_eq!(dlq[0].txn, "tx-d");
    assert_eq!(dlq[0].retry_count, 3);
    assert_eq!(dlq[0].payload, br#"{"bad":1}"#);
    assert!(
        pop_all(&f, "poison").await.is_empty(),
        "nothing was delivered"
    );

    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_retried_request_is_answered_from_the_recorded_outcome() {
    let dir = scratch("retry");
    let f = RaftFacade::open_with(&build_ctx(&dir), fast_cfg()).expect("open");
    let c = ctx();
    let req = TimersReq {
        ops: vec![sched_op("q", "k", 60_000, "tx", "{}")],
        producer_sub: None,
    };
    let first = f
        .timers_apply(c.clone(), req.clone())
        .await
        .expect("first")
        .results;
    let again = f.timers_apply(c, req).await.expect("retry").results;
    assert_eq!(first[0]["status"], "scheduled");
    assert_eq!(
        again, first,
        "the same request id is answered from state (D6, I6), not re-planned"
    );
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn list_and_count_read_the_pending_timers_in_byte_order() {
    let dir = scratch("list");
    let f = RaftFacade::open_with(&build_ctx(&dir), fast_cfg()).expect("open");
    let ops: Vec<Value> = ["laravel:b", "laravel:a", "other:z", "laravel:c"]
        .iter()
        .map(|k| sched_op("jobs", k, 600_000, &format!("tx-{k}"), "{}"))
        .collect();
    apply(&f, ops).await;

    let page = |after: Option<&str>, limit: i32| {
        let req = TimersListReq {
            queue: "jobs".into(),
            after: after.map(str::to_string),
            limit,
        };
        let f = &f;
        async move {
            let out = f.timers_list(ctx(), req).await.expect("list");
            serde_json::from_str::<Value>(&out.body).expect("list JSON")
        }
    };
    let p1 = page(None, 2).await;
    let keys: Vec<&str> = p1["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["timerKey"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["laravel:a", "laravel:b"]);
    assert_eq!(p1["truncated"], true);
    assert_eq!(p1["nextAfter"], "laravel:b");
    assert!(
        p1["rows"][0].get("payload").is_none(),
        "a list carries no payload"
    );
    let p2 = page(Some("laravel:b"), 10).await;
    let keys: Vec<&str> = p2["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["timerKey"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["laravel:c", "other:z"]);
    assert_eq!(p2["truncated"], false);
    assert_eq!(p2["nextAfter"], Value::Null);

    let count = f
        .timers_count(
            ctx(),
            TimersCountReq {
                queue: "jobs".into(),
                prefix: "laravel:".into(),
            },
        )
        .await
        .expect("count");
    assert_eq!(count.body, r#"{"count":3}"#);

    // A bad call is refused whole, with the SP's code.
    let bad = f
        .timers_apply(
            ctx(),
            TimersReq {
                ops: vec![json!({"op":"schedule","queue":"jobs","timerKey":"x","delayMs":1,"payload":"e30="})],
                producer_sub: None,
            },
        )
        .await;
    match bad {
        Err(RsmError::Rejected { code, message }) => {
            assert_eq!(code, "timers_bad_request");
            assert_eq!(message, "QTIMER op 0: txn is required");
        }
        other => panic!("expected a 400-class refusal, got {other:?}"),
    }

    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_pending_timer_survives_a_clean_restart_and_fires_once() {
    let dir = scratch("restart");
    {
        let f = RaftFacade::open_with(&build_ctx(&dir), fast_cfg()).expect("open 1");
        apply(&f, vec![sched_op("r", "k", 700, "tx-r", r#"{"r":1}"#)]).await;
        f.shutdown().await;
    }
    {
        let f = RaftFacade::open_with(&build_ctx(&dir), fast_cfg()).expect("reopen");
        assert!(wait_gone(&f, "r", "k", Duration::from_secs(5)).await);
        let msgs = pop_all(&f, "r").await;
        assert_eq!(msgs.len(), 1, "{msgs:?}");
        assert_eq!(msgs[0]["transactionId"], "tx-r");
        f.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A push and a timer land on one partition in their commit order: the fired
/// message takes the next offset of a queue that already has traffic.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_fired_message_takes_the_next_offset_of_a_live_queue() {
    let dir = scratch("mixed");
    let f = RaftFacade::open_with(&build_ctx(&dir), fast_cfg()).expect("open");
    f.push(
        ctx(),
        PushReq {
            raw: br#"{"items":[{"queue":"mix","payload":{"p":1},"transactionId":"p1"}]}"#.to_vec(),
        },
    )
    .await
    .expect("push");
    apply(&f, vec![sched_op("mix", "k", 0, "t1", r#"{"t":1}"#)]).await;
    assert!(wait_gone(&f, "mix", "k", Duration::from_secs(5)).await);
    let msgs = pop_all(&f, "mix").await;
    assert_eq!(msgs.len(), 2, "{msgs:?}");
    assert_eq!(msgs[0]["transactionId"], "p1");
    assert_eq!(msgs[0]["offset"], 0);
    assert_eq!(msgs[1]["transactionId"], "t1");
    assert_eq!(msgs[1]["offset"], 1);
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
