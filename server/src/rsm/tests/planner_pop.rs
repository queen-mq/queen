//! Pop against the tiny apply loop — the port of `native/tests/pop.rs` and the
//! pitfalls of §8 row 004. The `hw`/unconfirmed-append cases of pgless do not
//! port: in the RSM a segment exists only once a committed `Append` applied, so
//! `last_offset` is the visible tail.

use super::planner_harness::{
    ack_pos, pop_pinned, pop_pinned_with, pop_wildcard, pop_wildcard_with, push, push_cfg, qcfg,
    Cell,
};
use crate::rsm::entry::{Outcome, PopClaim, PopOutcome};
use crate::rsm::planner::SubIntent;

fn claims(o: &Outcome) -> Vec<PopClaim> {
    match o {
        Outcome::Pop(PopOutcome { claims }) => claims.clone(),
        other => panic!("not a pop outcome: {other:?}"),
    }
}

fn only(o: &Outcome) -> PopClaim {
    let mut c = claims(o);
    assert_eq!(c.len(), 1, "expected exactly one claim: {c:?}");
    c.remove(0)
}

#[test]
fn a_pinned_pop_delivers_fifo_and_leases_the_batch() {
    let mut c = Cell::new("pop-fifo");
    c.run(&[push(1, "q", "p0", &["a", "b", "c"])]);
    c.advance(1000);
    let cy = c.run(&[pop_pinned(2, "q", "p0", "g", "w1")]);
    let claim = only(&cy.outcome(0));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 2));
    assert_eq!(claim.delivery_attempt, 1);
    assert!(claim.lease_expires_at_us.is_some());
    let pid = c.pid_of("q", "p0").unwrap();
    let cur = c.cursor(pid, "g").unwrap();
    assert_eq!(cur.committed, -1, "a lease does not advance committed");
    assert_eq!(cur.batch_end, Some(2));
    assert_eq!(cur.worker.as_deref(), Some("w1"));
    assert_eq!(
        cur.delivered.len(),
        3,
        "the delivered set is on the cursor (O16)"
    );
}

#[test]
fn the_budget_slices_a_run_and_the_next_pop_resumes() {
    let mut c = Cell::new("pop-budget");
    c.run(&[push(1, "q", "p0", &["a", "b", "c"])]);
    c.advance(1000);
    let cy = c.run(&[pop_pinned_with(2, "q", "p0", "g", "w1", |p| p.budget = 2)]);
    let claim = only(&cy.outcome(0));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 1));
    let pid = c.pid_of("q", "p0").unwrap();
    // Ack the batch, then pop the rest.
    c.advance(1000);
    c.run(&[ack_pos(3, pid, "q", "g", "w1", Some(1), true, 2)]);
    assert_eq!(c.cursor(pid, "g").unwrap().committed, 1);
    c.advance(1000);
    let cy = c.run(&[pop_pinned(4, "q", "p0", "g", "w1")]);
    let claim = only(&cy.outcome(0));
    assert_eq!((claim.start_offset, claim.end_offset), (2, 2));
}

#[test]
fn a_live_lease_excludes_a_concurrent_claim() {
    let mut c = Cell::new("pop-leased");
    c.run(&[push(1, "q", "p0", &["a", "b"])]);
    c.advance(1000);
    c.run(&[pop_pinned(2, "q", "p0", "g", "w1")]);
    c.advance(1000);
    // A second worker finds the partition leased and gets nothing.
    let cy = c.run(&[pop_pinned(3, "q", "p0", "g", "w2")]);
    assert!(claims(&cy.outcome(0)).is_empty());
    assert!(
        !cy.logged,
        "a claim that delivered nothing writes no cursor"
    );
}

#[test]
fn wildcard_fastpath_is_empty_only_when_provably_empty() {
    // PERF-J: the facade's empty-pop fastpath. `wildcard_pop_provably_empty` must
    // say "empty" (skip the batcher) ONLY when `plan_pop_wildcard` would return
    // `Plan::Empty` — never for a first-contact (registration is an effect) nor a
    // ready partition. New this round, so this fails to build on the pre-fix tree.
    use super::planner_harness::TENANT;
    use crate::rsm::planner::pop::{wildcard_pop_provably_empty, POP_FASTPATH_SCAN_CAP};
    use crate::rsm::store::Store;

    let mut c = Cell::new("pop-fastpath");
    let probe = |c: &Cell| -> bool {
        c.node
            .store()
            .read(|r| {
                wildcard_pop_provably_empty(r, TENANT, "q", "g", c.now(), POP_FASTPATH_SCAN_CAP)
            })
            .expect("read")
    };

    // (1) Unknown queue + unregistered group: a wildcard pop MAY create/register
    //     (an effect), so it is NOT provably empty -> must submit.
    assert!(
        !probe(&c),
        "unknown queue must submit (may create/register)"
    );

    // (2) Push, then a first-contact wildcard pop registers the group and claims.
    c.run(&[push(1, "q", "p0", &["a", "b"])]);
    c.advance(1000);
    let cy = c.run(&[pop_wildcard(2, "q", "g", "w1")]);
    assert_eq!(
        claims(&cy.outcome(0)).len(),
        1,
        "the first pop claims the run"
    );
    let pid = c.pid_of("q", "p0").unwrap();

    // The partition is now LEASED (claimed, unacked), deferred to lease expiry, so
    // another wildcard pop finds nothing claimable -> provably empty.
    c.advance(1000);
    assert!(
        probe(&c),
        "a fully-leased group has nothing ready -> fastpath empty"
    );

    // (3) Ack the batch: the partition drains and its pending row is deleted.
    c.run(&[ack_pos(3, pid, "q", "g", "w1", Some(1), true, 2)]);
    assert_eq!(c.cursor(pid, "g").unwrap().committed, 1);
    c.advance(1000);
    assert!(probe(&c), "a registered, drained group is provably empty");

    // (4) A new push makes a partition ready NOW -> not provably empty -> submit,
    //     and a real wildcard pop then delivers it (no claim is stranded).
    c.run(&[push(4, "q", "p0", &["z"])]);
    c.advance(1000);
    assert!(!probe(&c), "a ready partition is not provably empty");
    let cy = c.run(&[pop_wildcard(5, "q", "g", "w1")]);
    assert_eq!(
        claims(&cy.outcome(0)).len(),
        1,
        "the ready message is delivered by a real pop"
    );
}

#[test]
fn lease_expiry_redelivers_with_attempt_up_and_the_retry_budget_untouched() {
    let mut c = Cell::new("pop-expiry");
    c.run(&[push(1, "q", "p0", &["a", "b"])]);
    c.advance(1000);
    c.run(&[pop_pinned_with(2, "q", "p0", "g", "w1", |p| {
        p.lease_seconds = 1
    })]);
    let pid = c.pid_of("q", "p0").unwrap();
    assert_eq!(c.cursor(pid, "g").unwrap().batch_retry_count, 0);
    // Let the lease expire, then re-pop the same batch.
    c.advance(5_000_000);
    let cy = c.run(&[pop_pinned(3, "q", "p0", "g", "w1")]);
    let claim = only(&cy.outcome(0));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 1));
    assert_eq!(claim.delivery_attempt, 2, "a redelivery counts up");
    assert_eq!(
        c.cursor(pid, "g").unwrap().batch_retry_count,
        0,
        "a lease expiry never eats retry budget"
    );
}

#[test]
fn auto_ack_commits_the_delivery_and_takes_no_lease() {
    let mut c = Cell::new("pop-autoack");
    c.run(&[push(1, "q", "p0", &["a", "b"])]);
    c.advance(1000);
    let cy = c.run(&[pop_pinned_with(2, "q", "p0", "g", "w1", |p| {
        p.auto_ack = true
    })]);
    let claim = only(&cy.outcome(0));
    assert!(claim.lease_expires_at_us.is_none());
    let pid = c.pid_of("q", "p0").unwrap();
    let cur = c.cursor(pid, "g").unwrap();
    assert_eq!(cur.committed, 1, "auto-ack advances committed");
    assert!(cur.worker.is_none());
    assert_eq!(cur.total_consumed, 2);
}

#[test]
fn subscription_all_delivers_the_whole_backlog() {
    let mut c = Cell::new("pop-all");
    c.run(&[push(1, "q", "p0", &["a", "b", "c"])]);
    c.advance(1_000_000);
    // The group registers `all` on this first pop; the backlog is delivered.
    let cy = c.run(&[pop_wildcard_with(2, "q", "g", "w1", |p| {
        p.sub = mode("all")
    })]);
    let claim = only(&cy.outcome(0));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 2));
}

#[test]
fn subscription_new_skips_the_backlog_and_seeds_at_the_tail() {
    let mut c = Cell::new("pop-new");
    c.run(&[push(1, "q", "p0", &["a", "b", "c"])]);
    c.advance(1_000_000);
    // `new` at registration seeds past the backlog; a wildcard `new` group has
    // no candidate at all until a NEW push arrives.
    let cy = c.run(&[pop_wildcard_with(2, "q", "g", "w1", |p| {
        p.sub = mode("new")
    })]);
    assert!(claims(&cy.outcome(0)).is_empty());
    // A push after the registration IS delivered.
    c.advance(1_000_000);
    c.run(&[push(3, "q", "p0", &["d"])]);
    c.advance(1_000_000);
    let cy = c.run(&[pop_wildcard(4, "q", "g", "w1")]);
    let claim = only(&cy.outcome(0));
    assert_eq!((claim.start_offset, claim.end_offset), (3, 3));
}

#[test]
fn a_stored_policy_beats_the_pop_carried_intent() {
    let mut c = Cell::new("pop-stored");
    c.run(&[push(1, "q", "p0", &["a", "b"])]);
    c.advance(1_000_000);
    // Register `all`.
    c.run(&[pop_wildcard_with(2, "q", "g", "w1", |p| {
        p.sub = mode("all")
    })]);
    let pid = c.pid_of("q", "p0").unwrap();
    // Ack the backlog so the ring drains for the group.
    c.advance(1000);
    c.run(&[ack_pos(3, pid, "q", "g", "w1", Some(1), true, 2)]);
    // A later push; a pop asking for `new` must NOT skip it (stored `all` wins).
    c.advance(1_000_000);
    c.run(&[push(4, "q", "p0", &["cc"])]);
    c.advance(1_000_000);
    let cy = c.run(&[pop_wildcard_with(5, "q", "g", "w2", |p| {
        p.sub = mode("new")
    })]);
    let claim = only(&cy.outcome(0));
    assert_eq!((claim.start_offset, claim.end_offset), (2, 2));
}

#[test]
fn a_late_partition_seeds_from_the_registration_not_the_tail() {
    // A `new` group registers over the queue while p0 exists; p1 is created
    // AFTER the registration, so its whole content is "new" and is delivered.
    let mut c = Cell::new("pop-late");
    c.run(&[push(1, "q", "p0", &["a"])]);
    c.advance(1_000_000);
    c.run(&[pop_wildcard_with(2, "q", "g", "w1", |p| {
        p.sub = mode("new")
    })]);
    c.advance(1_000_000);
    c.run(&[push(3, "q", "p1", &["b", "c"])]); // p1 created after registration
    c.advance(1_000_000);
    let cy = c.run(&[pop_wildcard(4, "q", "g", "w1")]);
    let cs = claims(&cy.outcome(0));
    assert_eq!(cs.len(), 1);
    let p1 = c.pid_of("q", "p1").unwrap();
    assert_eq!(cs[0].pid, p1);
    assert_eq!((cs[0].start_offset, cs[0].end_offset), (0, 1));
}

#[test]
fn delayed_processing_hides_a_fresh_frame_until_its_deadline() {
    let mut cfg = qcfg();
    cfg.delayed_processing = 5;
    let mut c = Cell::new("pop-delayed");
    c.run(&[push_cfg(1, "q", "p0", &["a"], cfg.clone())]);
    c.advance(1_000_000); // 1s < 5s
    let cy = c.run(&[pop_pinned(2, "q", "p0", "g", "w1")]);
    assert!(claims(&cy.outcome(0)).is_empty(), "too fresh to deliver");
    c.advance(6_000_000); // now well past the 5s deadline
    let cy = c.run(&[pop_pinned(3, "q", "p0", "g", "w1")]);
    let claim = only(&cy.outcome(0));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 0));
}

#[test]
fn window_buffer_debounces_a_partition_just_written() {
    let mut cfg = qcfg();
    cfg.window_buffer = 5;
    let mut c = Cell::new("pop-window");
    c.run(&[push_cfg(1, "q", "p0", &["a"], cfg.clone())]);
    c.advance(1_000_000);
    let cy = c.run(&[pop_pinned(2, "q", "p0", "g", "w1")]);
    assert!(claims(&cy.outcome(0)).is_empty(), "still inside the window");
    // The hot-list wheel can take over the hold.
    let cy = c.run(&[pop_pinned_with(3, "q", "p0", "g", "w1", |p| {
        p.skip_window_debounce = true
    })]);
    assert_eq!(
        claims(&cy.outcome(0)).len(),
        1,
        "skip_window_debounce delivers"
    );
}

#[test]
fn conflation_serves_the_newest_frame_and_leases_the_whole_span() {
    let mut c = Cell::new("pop-conflate");
    c.run(&[push(1, "q", "p0", &["a", "b", "c"])]);
    c.advance(1_000_000);
    let cy = c.run(&[pop_pinned_with(2, "q", "p0", "g", "w1", |p| {
        p.conflate = true
    })]);
    let claim = only(&cy.outcome(0));
    assert!(claim.conflated);
    assert_eq!(
        (claim.start_offset, claim.end_offset),
        (2, 2),
        "only the tail frame"
    );
    let pid = c.pid_of("q", "p0").unwrap();
    let cur = c.cursor(pid, "g").unwrap();
    assert_eq!(cur.batch_end, Some(2), "the lease spans (committed, 2]");
    assert!(cur.lease_conflated);
}

#[test]
fn a_wildcard_pop_shares_one_budget_across_partitions_in_fifo_order() {
    let mut c = Cell::new("pop-wild-budget");
    c.run(&[push(1, "q", "p0", &["a", "b"])]);
    c.advance(1000);
    c.run(&[push(2, "q", "p1", &["x", "y"])]);
    c.advance(1_000_000);
    // Budget 3 across two partitions: 2 from the first, 1 from the second.
    let cy = c.run(&[pop_wildcard_with(3, "q", "g", "w1", |p| {
        p.sub = mode("all");
        p.budget = 3;
    })]);
    let cs = claims(&cy.outcome(0));
    let total: i64 = cs
        .iter()
        .map(|c| c.end_offset as i64 - c.start_offset as i64 + 1)
        .sum();
    assert_eq!(total, 3, "the whole budget was spent");
    assert!(!cs.is_empty());
}

#[test]
fn a_wildcard_pop_honours_max_parts() {
    let mut c = Cell::new("pop-maxparts");
    c.run(&[push(1, "q", "p0", &["a"])]);
    c.advance(1000);
    c.run(&[push(2, "q", "p1", &["b"])]);
    c.advance(1_000_000);
    let cy = c.run(&[pop_wildcard_with(3, "q", "g", "w1", |p| {
        p.sub = mode("all");
        p.max_parts = 1;
    })]);
    assert_eq!(
        claims(&cy.outcome(0)).len(),
        1,
        "max_parts capped the claim"
    );
}

#[test]
fn the_first_wildcard_contact_seeds_every_partition_of_the_queue() {
    // Two partitions with backlog exist before the group registers; the first
    // wildcard pop (`all`) must reach BOTH, not just the one the ring holds.
    let mut c = Cell::new("pop-enumerate");
    c.run(&[push(1, "q", "p0", &["a"])]);
    c.advance(1000);
    c.run(&[push(2, "q", "p1", &["b"])]);
    c.advance(1_000_000);
    let cy = c.run(&[pop_wildcard_with(3, "q", "g", "w1", |p| {
        p.sub = mode("all");
        p.budget = 100;
    })]);
    let cs = claims(&cy.outcome(0));
    assert_eq!(cs.len(), 2, "both partitions delivered on first contact");
}

#[test]
fn an_empty_partition_cursor_is_sealed_so_it_stops_being_a_candidate() {
    // A partition whose only frame was auto-acked keeps committed at the tail;
    // a later pop finds nothing and writes no cursor (nothing changed).
    let mut c = Cell::new("pop-seal");
    c.run(&[push(1, "q", "p0", &["a"])]);
    c.advance(1_000_000);
    c.run(&[pop_pinned_with(2, "q", "p0", "g", "w1", |p| {
        p.auto_ack = true
    })]);
    let pid = c.pid_of("q", "p0").unwrap();
    assert_eq!(c.cursor(pid, "g").unwrap().committed, 0);
    c.advance(1_000_000);
    let cy = c.run(&[pop_pinned(3, "q", "p0", "g", "w1")]);
    assert!(claims(&cy.outcome(0)).is_empty());
    assert!(!cy.logged, "a caught-up partition writes nothing");
}

#[test]
fn a_pinned_pop_of_an_unknown_partition_is_empty_and_provisions_nothing() {
    let mut c = Cell::new("pop-unknown");
    c.run(&[push(1, "q", "p0", &["a"])]);
    c.advance(1000);
    let cy = c.run(&[pop_pinned(2, "q", "does-not-exist", "g", "w1")]);
    assert!(claims(&cy.outcome(0)).is_empty());
    assert!(
        c.pid_of("q", "does-not-exist").is_none(),
        "a pop never creates a partition"
    );
}

#[test]
fn a_plain_pinned_pop_does_not_register_the_group() {
    // I12/§8 parity (refuted WP-1.5): a NON-conflating pinned pop must not
    // register the group. 004's registrar branch is guarded by
    // `IF v_from_ts IS NULL AND v_conflate` (004:226); a plain pinned pop takes
    // the pop-carried-intent seed path (004 ≈320) and writes NO group row.
    let mut c = Cell::new("pop-pinned-no-register");
    c.run(&[push(1, "q", "p0", &["a", "b"])]);
    c.advance(1_000_000);
    let cy = c.run(&[pop_pinned(2, "q", "p0", "g", "w1")]);
    let claim = only(&cy.outcome(0));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 1));
    assert!(
        !c.group_exists("q", "g"),
        "a plain pinned pop registers no group row (004: only a conflating one does)"
    );
}

#[test]
fn a_pinned_pop_then_a_wildcard_still_delivers_the_other_partitions_backlog() {
    // The refuted scenario, verbatim: two partitions with backlog, a pinned pop
    // drains one under g, then a wildcard pop of g must still reach the other
    // (004 keeps a group registered by a pinned pop and one registered by a
    // wildcard pop indistinguishable). Before the fix the pinned pop registered
    // g, so the wildcard saw it as already-present, skipped the first-contact
    // enumeration, and p1's backlog was stranded for ever.
    let mut c = Cell::new("pop-pinned-then-wildcard");
    c.run(&[push(1, "q", "p0", &["a", "b"])]);
    c.advance(1000);
    c.run(&[push(2, "q", "p1", &["x", "y"])]);
    c.advance(1_000_000);
    // Pinned, auto-ack: drains p0 [0,1] and (post-fix) registers nothing.
    let cy = c.run(&[pop_pinned_with(3, "q", "p0", "g", "w1", |p| {
        p.budget = 2;
        p.auto_ack = true;
    })]);
    let claim = only(&cy.outcome(0));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 1));
    assert!(!c.group_exists("q", "g"));
    // The wildcard pop is therefore first contact: it enumerates the queue and
    // delivers p1's pre-existing backlog.
    c.advance(1_000_000);
    let cy = c.run(&[pop_wildcard(4, "q", "g", "w2")]);
    let cs = claims(&cy.outcome(0));
    assert_eq!(cs.len(), 1, "the wildcard reaches p1 (the bug stranded it)");
    let p1 = c.pid_of("q", "p1").unwrap();
    assert_eq!(cs[0].pid, p1);
    assert_eq!((cs[0].start_offset, cs[0].end_offset), (0, 1));
}

#[test]
fn a_conflating_pinned_pop_registers_and_bulk_seeds_the_whole_queue() {
    // The conflation exception (004:226 `IF v_from_ts IS NULL AND v_conflate`):
    // a conflating pinned pop IS the registrar, so it must ALSO run the
    // queue-wide bulk seed (004:243-296). A later wildcard pop of the now
    // registered group does not enumerate, so every partition must be made a
    // candidate here or the conflating group could never learn about them.
    let mut c = Cell::new("pop-pinned-conflate-seed");
    c.run(&[push(1, "q", "p0", &["a", "b"])]);
    c.advance(1000);
    c.run(&[push(2, "q", "p1", &["x", "y"])]);
    c.advance(1_000_000);
    let cy = c.run(&[pop_pinned_with(3, "q", "p0", "g", "w1", |p| {
        p.conflate = true
    })]);
    let claim = only(&cy.outcome(0));
    assert!(claim.conflated);
    assert_eq!(
        (claim.start_offset, claim.end_offset),
        (1, 1),
        "p0's newest frame"
    );
    assert!(
        c.group_exists("q", "g"),
        "a conflating pinned pop registers the group (the registrar branch)"
    );
    // p1 was bulk-seeded at the floor (committed = -1 < last_offset), so it is a
    // wildcard candidate even though the pinned pop never touched it.
    let p1 = c.pid_of("q", "p1").unwrap();
    assert_eq!(
        c.cursor(p1, "g").unwrap().committed,
        -1,
        "the bulk seed put p1's cursor at the floor"
    );
    // A later wildcard pop of g (already registered, no enumeration) still
    // reaches p1 through the seeded `pending`, and the stored conflation wins.
    c.advance(1_000_000);
    let cy = c.run(&[pop_wildcard(4, "q", "g", "w2")]);
    let cs = claims(&cy.outcome(0));
    assert_eq!(cs.len(), 1, "the bulk seed made p1 a candidate");
    assert_eq!(cs[0].pid, p1);
    assert!(cs[0].conflated, "the stored conflation policy wins");
    assert_eq!(
        (cs[0].start_offset, cs[0].end_offset),
        (1, 1),
        "p1's newest frame"
    );
}

#[test]
fn a_conflating_pinned_pop_whose_partition_is_already_seeded_does_not_register_or_bulk_seed() {
    // WP-1.5 refutation (I12 / §8 row 004, the OUTER first-contact guard
    // 004:189-190). 004 nests its conflating registrar (004:226) INSIDE
    // `NOT EXISTS(cursor for the PINNED partition + group)`. So a conflating
    // pinned pop whose partition ALREADY carries a cursor — one an earlier plain
    // pinned pop seeded while leaving the group unregistered (004 ≈320) — finds
    // the outer guard FALSE and registers NOTHING and bulk-seeds NOTHING; it just
    // serves the existing cursor. The refuted revision gated the registrar on the
    // group alone, so it fired here, manufacturing a durable conflating group and
    // a queue-wide seed the oracle never writes. The observable consequence is on
    // the LATER wildcard: with the group wrongly present + conflation=TRUE it saw
    // the group as already-registered, skipped first-contact enumeration, and
    // delivered only the newest frame (dropping every partition's backlog);
    // correctly, the group is still unregistered, so the wildcard is first
    // contact and delivers the full backlog PLAIN.
    let mut c = Cell::new("pop-plain-then-conflate-pinned");
    c.run(&[push(1, "q", "p0", &["a", "b"])]);
    c.advance(1000);
    c.run(&[push(2, "q", "p1", &["x", "y"])]);
    c.advance(1_000_000);

    // (1) plain pinned pop of p0/g (auto-ack): seeds p0's cursor, registers no
    //     group row (004 ≈320).
    c.run(&[pop_pinned_with(3, "q", "p0", "g", "w1", |p| {
        p.auto_ack = true
    })]);
    let p0 = c.pid_of("q", "p0").unwrap();
    let p1 = c.pid_of("q", "p1").unwrap();
    assert!(
        c.cursor(p0, "g").is_some(),
        "the plain pinned pop seeded p0"
    );
    assert!(!c.group_exists("q", "g"), "and registered no group");

    // (2) conflating pinned pop of the SAME p0/g. The outer NOT EXISTS is now
    //     false (p0 already has a cursor), so it must NOT register g and must NOT
    //     bulk-seed p1 — exactly what 004 does when the outer guard fails.
    c.advance(1_000_000);
    c.run(&[pop_pinned_with(4, "q", "p0", "g", "w1", |p| {
        p.conflate = true
    })]);
    assert!(
        !c.group_exists("q", "g"),
        "a conflating pop whose pinned partition already has a cursor is NOT the registrar"
    );
    assert!(
        c.cursor(p1, "g").is_none(),
        "and it runs no queue-wide bulk seed (p1 is left untouched)"
    );

    // (3) a wildcard pop of g is therefore STILL first contact: it registers g
    //     PLAIN (pop-carried default), enumerates the queue, and delivers p1's
    //     WHOLE backlog unconflated — not the single newest frame a wrongly
    //     stored conflation=TRUE policy would have forced.
    c.advance(1_000_000);
    let cy = c.run(&[pop_wildcard(5, "q", "g", "w2")]);
    let cs = claims(&cy.outcome(0));
    assert_eq!(cs.len(), 1, "the wildcard reaches p1 on first contact");
    assert_eq!(cs[0].pid, p1);
    assert!(!cs[0].conflated, "delivered PLAIN, not conflated");
    assert_eq!(
        (cs[0].start_offset, cs[0].end_offset),
        (0, 1),
        "p1's whole backlog, not just the newest frame"
    );
}

fn mode(m: &str) -> SubIntent {
    SubIntent {
        mode: m.to_string(),
        from_us: None,
        now: false,
    }
}
