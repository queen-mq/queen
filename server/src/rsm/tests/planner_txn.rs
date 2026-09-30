//! The transaction planner with the consumption engine as participant: what
//! it plans (pushes, KV, timers, positions on partitions that do not exist
//! yet), what it records (the engine's per-target results, so a retry of the
//! request id reads them), and what it refuses (acks, which never reach it; a
//! position on a partition that exists; an engine row on a partition that is
//! gone).

use super::planner_harness::{self as h, Cell, Cmd, TENANT};
use crate::rsm::effect::Effect;
use crate::rsm::entry::{AckOutcome, AckResult};
use crate::rsm::planner::positions::PositionOp;
use crate::rsm::planner::txn::TxnOutcome;
use crate::rsm::planner::{AckTarget, Plan, SubIntent};
use crate::rsm::store::{Store, TypedReads};

fn position(queue: &str, partition: &str, group: &str, offset: Option<u64>) -> PositionOp {
    PositionOp {
        queue: queue.to_string(),
        partition: partition.to_string(),
        group: group.to_string(),
        offset,
        metadata: "m".to_string(),
        sub: SubIntent {
            mode: "all".to_string(),
            from_us: None,
            now: false,
        },
    }
}

fn result(pid: u64, committed: i64) -> AckResult {
    AckResult {
        pid,
        committed,
        acked: 1,
        conflated: 0,
        dlq: 0,
        lease_released: true,
        batch_retry_count: 0,
        noop_hashes: Vec::new(),
        stale_hashes: Vec::new(),
    }
}

#[test]
fn acks_never_reach_the_planner() {
    let mut c = Cell::new("txn-acks");
    c.run(&[h::push(1, "q", "p0", &["a"])]);
    let pid = c.pid_of("q", "p0").unwrap();
    let mut t = h::txn(2);
    t.acks.push(AckTarget {
        pid,
        tenant: TENANT.to_string(),
        queue: "q".to_string(),
        group: "g".to_string(),
        worker: "w".to_string(),
        items: Vec::new(),
    });
    let cy = c.run(&[Cmd::Txn(t)]);
    match cy.plan(0) {
        Err(r) => assert!(r.retryable && r.code == "internal", "{r:?}"),
        other => panic!("an ack was planned: {other:?}"),
    }
    assert!(!cy.logged);
}

#[test]
fn the_engines_rows_commit_with_the_bundle_and_its_results_are_recorded() {
    let mut c = Cell::new("txn-engine-rows");
    c.run(&[h::push(1, "q", "p0", &["a", "b"])]);
    let pid = c.pid_of("q", "p0").unwrap();
    // A consumer-group commit: the engine's cursor row and result, a key.
    let mut t = h::txn(2);
    t.extra_effects.push(Effect::CursorSet {
        pid,
        group: "g".into(),
        row: h::cursor_row(1),
    });
    t.engine_acks = AckOutcome {
        results: vec![result(pid, 1)],
    };
    t.kv = crate::rsm::planner::kv::parse_ops(
        &[serde_json::json!({"op":"put","ns":"n","key":"k","value":{"v":1},"forever":true})],
        TENANT,
        true,
        511,
        crate::rsm::planner::kv::MAX_VALUE_BYTES_DEFAULT,
    )
    .expect("kv op");
    let cy = c.run(&[Cmd::Txn(t.clone())]);
    assert!(cy.logged);
    let out = TxnOutcome::from_outcome(&cy.outcome(0)).expect("a transaction outcome");
    assert_eq!(out.acks.results, vec![result(pid, 1)]);
    assert_eq!(out.kv.results.len(), 1);
    assert_eq!(c.cursor(pid, "g").map(|r| r.committed), Some(1));

    // A retry of the id answers the recorded outcome, engine results included.
    c.advance(1_000);
    let again = c.run(&[Cmd::Txn(t)]);
    assert!(!again.logged);
    let replay = TxnOutcome::from_outcome(&again.outcome(0)).expect("recorded");
    assert_eq!(replay.acks.results, vec![result(pid, 1)]);
}

#[test]
fn an_engine_row_on_a_partition_being_deleted_refuses_the_whole_bundle() {
    let mut c = Cell::new("txn-gone");
    c.run(&[h::push(1, "q", "p0", &["a"])]);
    let pid = c.pid_of("q", "p0").unwrap();
    let mut t = h::txn(3);
    t.pushes.push(match h::push(4, "other", "p0", &["x"]) {
        Cmd::Push(p) => p,
        _ => unreachable!(),
    });
    t.extra_effects.push(Effect::CursorSet {
        pid,
        group: "g".into(),
        row: h::cursor_row(0),
    });
    // The delete is planned first in the same cycle: the row names a
    // partition going away.
    let cy = c.run(&[h::delete_queue(2, "q", &[pid]), Cmd::Txn(t)]);
    match cy.plan(1) {
        Err(r) => assert!(r.retryable && r.code == "partition_gone", "{r:?}"),
        other => panic!("a row on a deleted partition was planned: {other:?}"),
    }
    assert!(
        c.pid_of("other", "p0").is_none(),
        "the bundle's push rolled back with it"
    );
}

#[test]
fn a_position_on_a_new_partition_creates_it_and_registers_the_group() {
    let mut c = Cell::new("txn-position-new");
    c.run(&[h::push(1, "q", "p0", &["a"])]);
    let mut t = h::txn(2);
    t.positions = vec![
        position("q", "fresh", "g", Some(5)),
        // Forgetting on a partition that does not exist writes nothing.
        position("q", "nowhere", "g", None),
    ];
    let cy = c.run(&[Cmd::Txn(t)]);
    let effects = match cy.plan(0) {
        Ok(Plan::Logged { effects, .. }) => effects.clone(),
        other => panic!("{other:?}"),
    };
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::PartitionCreate { partition, .. } if partition == "fresh")));
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::GroupUpsert { group, .. } if group == "g")));
    let pid = c.pid_of("q", "fresh").expect("created");
    let row = c.cursor(pid, "g").expect("the position's row");
    assert_eq!(row.committed, 4, "the next offset read is 5");
    assert_eq!(row.metadata, "m");
    assert!(c.group_exists("q", "g"));
    assert!(c.pid_of("q", "nowhere").is_none());

    // The group registered: a second new partition does not register it again.
    let mut t = h::txn(3);
    t.positions = vec![position("q", "fresh2", "g", Some(1))];
    let cy = c.run(&[Cmd::Txn(t)]);
    match cy.plan(0) {
        Ok(Plan::Logged { effects, .. }) => assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::GroupUpsert { .. })),
            "{effects:?}"
        ),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_position_sees_a_registration_in_flight() {
    let mut c = Cell::new("txn-position-inflight-group");
    c.run(&[h::push(1, "q", "p0", &["a"])]);
    // The engine registers the group in an entry still in flight, and a
    // commit on a new partition is planned in the same cycle.
    let register = h::effects(
        2,
        vec![Effect::GroupUpsert {
            tenant: TENANT.into(),
            queue: "q".into(),
            group: "g".into(),
            meta: crate::rsm::planner::group_meta_for_registration(
                [1; 16],
                "",
                "",
                "",
                &SubIntent::default(),
                false,
                h::BASE_US,
            ),
        }],
    );
    let mut t = h::txn(3);
    t.positions = vec![position("q", "fresh", "g", Some(2))];
    let cy = c.run(&[register, Cmd::Txn(t)]);
    match cy.plan(1) {
        Ok(Plan::Logged { effects, .. }) => assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::GroupUpsert { .. })),
            "registered twice: {effects:?}"
        ),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_position_on_an_existing_partition_is_the_engines() {
    let mut c = Cell::new("txn-position-existing");
    c.run(&[h::push(1, "q", "p0", &["a"])]);
    let mut t = h::txn(2);
    t.positions = vec![position("q", "p0", "g", Some(1))];
    let cy = c.run(&[Cmd::Txn(t)]);
    match cy.plan(0) {
        Err(r) => assert!(r.retryable && r.code == "internal", "{r:?}"),
        other => panic!("a position on an existing partition was planned: {other:?}"),
    }
    let mut t = h::txn(3);
    t.positions = vec![position("missing-queue", "p0", "g", Some(1))];
    match c.run(&[Cmd::Txn(t)]).plan(0) {
        Err(r) => assert!(!r.retryable && r.code == "queue_not_found", "{r:?}"),
        other => panic!("{other:?}"),
    }
    // Nothing was written.
    let pid = c.pid_of("q", "p0").unwrap();
    assert!(c.cursor(pid, "g").is_none());
    let rows = c
        .node
        .store()
        .read(|r| r.group(TENANT, "q", "g"))
        .expect("read");
    assert!(rows.is_none());
}
