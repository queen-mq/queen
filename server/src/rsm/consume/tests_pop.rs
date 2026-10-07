//! Pop parity: the semantic cases of the planner's `tests/planner_pop.rs` and
//! `tests/pop_bounded.rs` (08050fed), against the engine.

use super::tests::{mode, only, qcfg, H, T};
use super::Served;
use crate::rsm::batcher::Command;
use crate::rsm::planner::AckStatus;
use crate::rsm::store::Store;

#[test]
fn a_pinned_pop_delivers_fifo_and_leases_the_batch() {
    let mut h = H::new("pop-fifo");
    h.push("q", "p0", &["a", "b", "c"]);
    let claim = only(h.pinned("q", "p0", "g", "w1"));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 2));
    assert_eq!(claim.delivery_attempt, 1);
    assert!(claim.lease_expires_at_us.is_some());
    let pid = h.pid_of("q", "p0");
    let cur = h.cursor(pid, "g").unwrap();
    assert_eq!(cur.committed, -1, "a lease does not advance committed");
    assert_eq!(cur.batch_end, Some(2));
    assert_eq!(cur.worker.as_deref(), Some("w1"));
}

#[test]
fn the_budget_slices_a_run_and_the_next_pop_resumes() {
    let mut h = H::new("pop-budget");
    h.push("q", "p0", &["a", "b", "c"]);
    let claim = only(h.pinned_with("q", "p0", "g", "w1", |p| p.budget = 2));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 1));
    let pid = h.pid_of("q", "p0");
    h.ack_pos(pid, "q", "g", "w1", Some(1), true, true, 2);
    assert_eq!(h.cursor(pid, "g").unwrap().committed, 1);
    let claim = only(h.pinned("q", "p0", "g", "w1"));
    assert_eq!((claim.start_offset, claim.end_offset), (2, 2));
}

#[test]
fn a_budget_spans_appends() {
    // Several appends, one budget: the claim crosses append boundaries.
    let mut h = H::new("pop-appends");
    h.push("q", "p0", &["a", "b"]);
    h.push("q", "p0", &["c"]);
    h.push("q", "p0", &["d", "e"]);
    let claim = only(h.pinned_with("q", "p0", "g", "w1", |p| p.budget = 4));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 3));
    let pid = h.pid_of("q", "p0");
    let r = h.ack(
        pid,
        "q",
        "g",
        "w1",
        &[
            ("a", AckStatus::Ok),
            ("b", AckStatus::Ok),
            ("c", AckStatus::Ok),
            ("d", AckStatus::Ok),
        ],
    );
    assert_eq!(r.committed, 3);
    assert!(r.lease_released);
    let claim = only(h.pinned("q", "p0", "g", "w1"));
    assert_eq!((claim.start_offset, claim.end_offset), (4, 4));
}

#[test]
fn a_live_lease_excludes_a_concurrent_claim() {
    let mut h = H::new("pop-leased");
    h.push("q", "p0", &["a", "b"]);
    only(h.pinned("q", "p0", "g", "w1"));
    assert!(h.pinned("q", "p0", "g", "w2").is_empty());
    assert!(h.wildcard("q", "g", "w3").is_empty());
    assert_eq!(
        h.checkpoint(),
        0,
        "a claim that delivered nothing writes nothing"
    );
}

#[test]
fn no_double_delivery_across_many_pops() {
    let mut h = H::new("pop-nodouble");
    for i in 0..8 {
        h.push("q", &format!("p{i}"), &["a", "b", "c"]);
    }
    let mut seen = std::collections::HashSet::new();
    for w in 0..20 {
        for c in h.wildcard_with("q", "g", &format!("w{w}"), |c| {
            c.budget = 2;
            c.max_parts = 3;
        }) {
            for off in c.start_offset..=c.end_offset {
                assert!(seen.insert((c.pid, off)), "delivered twice: {c:?}");
            }
        }
    }
    // Every partition got exactly its first two frames leased, once.
    assert_eq!(seen.len(), 16);
}

#[test]
fn lease_expiry_redelivers_with_attempt_up_and_the_retry_budget_untouched() {
    let mut h = H::new("pop-expiry");
    h.push("q", "p0", &["a", "b"]);
    only(h.pinned_with("q", "p0", "g", "w1", |p| p.lease_seconds = 1));
    let pid = h.pid_of("q", "p0");
    assert_eq!(h.cursor(pid, "g").unwrap().batch_retry_count, 0);
    h.advance(5_000_000);
    let claim = only(h.pinned("q", "p0", "g", "w1"));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 1));
    assert_eq!(claim.delivery_attempt, 2, "a redelivery counts up");
    assert_eq!(h.cursor(pid, "g").unwrap().batch_retry_count, 0);
    // The wildcard path redelivers an expired lease too.
    h.advance(120_000_000);
    let claim = only(h.wildcard("q", "g", "w9"));
    assert_eq!(claim.delivery_attempt, 3);
}

#[test]
fn auto_ack_commits_the_delivery_and_takes_no_lease() {
    let mut h = H::new("pop-autoack");
    h.push("q", "p0", &["a", "b"]);
    let claim = only(h.pinned_with("q", "p0", "g", "w1", |p| p.auto_ack = true));
    assert!(claim.lease_expires_at_us.is_none());
    let pid = h.pid_of("q", "p0");
    let cur = h.cursor(pid, "g").unwrap();
    assert_eq!(cur.committed, 1, "auto-ack advances committed");
    assert!(cur.worker.is_none());
    assert_eq!(cur.total_consumed, 2);
}

#[test]
fn subscription_all_delivers_the_whole_backlog() {
    let mut h = H::new("pop-all");
    h.push("q", "p0", &["a", "b", "c"]);
    h.advance(1_000_000);
    let claim = only(h.wildcard_with("q", "g", "w1", |p| p.sub = mode("all")));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 2));
}

#[test]
fn subscription_new_skips_the_backlog_and_seeds_at_the_tail() {
    let mut h = H::new("pop-new");
    h.push("q", "p0", &["a", "b", "c"]);
    h.advance(1_000_000);
    assert!(h
        .wildcard_with("q", "g", "w1", |p| p.sub = mode("new"))
        .is_empty());
    h.advance(1_000_000);
    h.push("q", "p0", &["d"]);
    let claim = only(h.wildcard("q", "g", "w1"));
    assert_eq!((claim.start_offset, claim.end_offset), (3, 3));
    // A new leader seeds the same way from the stored registration.
    h.failover();
    h.advance(1_000_000);
    h.push("q", "p1", &["late"]);
    let claim = only(h.wildcard("q", "g", "w2"));
    assert_eq!(claim.pid, h.pid_of("q", "p1"));
}

#[test]
fn subscription_timestamp_seeds_from_the_instant() {
    let mut h = H::new("pop-ts");
    let t0 = h.now;
    h.push_at("q", "p0", &["old"], t0);
    h.push_at("q", "p0", &["new1", "new2"], t0 + 10_000_000);
    h.now = t0 + 20_000_000;
    let claim = only(h.wildcard_with("q", "g", "w1", |p| {
        p.sub.mode = "timestamp".into();
        p.sub.from_us = Some(t0 + 5_000_000);
    }));
    assert_eq!((claim.start_offset, claim.end_offset), (1, 2));
}

#[test]
fn a_stored_policy_beats_the_pop_carried_intent() {
    let mut h = H::new("pop-stored");
    h.push("q", "p0", &["a", "b"]);
    h.advance(1_000_000);
    only(h.wildcard_with("q", "g", "w1", |p| p.sub = mode("all")));
    let pid = h.pid_of("q", "p0");
    h.ack_pos(pid, "q", "g", "w1", Some(1), true, true, 2);
    h.advance(1_000_000);
    h.push("q", "p0", &["cc"]);
    h.advance(1_000_000);
    let claim = only(h.wildcard_with("q", "g", "w2", |p| p.sub = mode("new")));
    assert_eq!((claim.start_offset, claim.end_offset), (2, 2));
}

#[test]
fn a_late_partition_seeds_from_the_registration_not_the_tail() {
    let mut h = H::new("pop-late");
    h.push("q", "p0", &["a"]);
    h.advance(1_000_000);
    assert!(h
        .wildcard_with("q", "g", "w1", |p| p.sub = mode("new"))
        .is_empty());
    h.advance(1_000_000);
    let p1 = h.push("q", "p1", &["b", "c"]);
    h.advance(1_000_000);
    let cs = h.wildcard("q", "g", "w1");
    assert_eq!(cs.len(), 1);
    assert_eq!(cs[0].pid, p1);
    assert_eq!((cs[0].start_offset, cs[0].end_offset), (0, 1));
}

#[test]
fn a_new_partitions_first_message_reaches_every_group_whatever_the_load_order() {
    // 2026-10-05: a group that loaded a new partition before its first append
    // was written waited for that append's hook; a second group loading it
    // after the write raised the shared tail and armed only its own part, so
    // the hook found nothing new and the first group never got the message
    // (not until the partition's next one).
    let mut h = H::new("pop-load-order");
    h.queue("q", qcfg());
    assert!(h.wildcard("q", "a", "w1").is_empty());
    assert!(h.wildcard("q", "b", "w2").is_empty());
    h.advance(1_000_000);
    let pid = h.create_partition("q", "p0");
    assert!(h.wildcard("q", "a", "w1").is_empty(), "nothing in it yet");
    let now = h.now;
    let (_, _, last) = h.write_append("q", "p0", &["m"], now);
    let c = only(h.wildcard("q", "b", "w2"));
    assert_eq!(
        (c.pid, c.start_offset),
        (pid, 0),
        "b loads it after the write"
    );
    h.e.on_append(pid, last);
    let c = only(h.wildcard("q", "a", "w1"));
    assert_eq!((c.pid, c.start_offset, c.end_offset), (pid, 0, 0));
}

#[test]
fn delayed_processing_hides_a_fresh_frame_until_its_deadline() {
    let mut h = H::new("pop-delayed");
    let mut cfg = qcfg();
    cfg.delayed_processing = 5;
    h.queue("q", cfg);
    h.push("q", "p0", &["a"]);
    h.advance(1_000_000);
    assert!(h.pinned("q", "p0", "g", "w1").is_empty(), "too fresh");
    assert!(
        h.wildcard("q", "g2", "w1").is_empty(),
        "too fresh for a wildcard"
    );
    h.advance(6_000_000);
    let claim = only(h.pinned("q", "p0", "g", "w1"));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 0));
    let claim = only(h.wildcard("q", "g2", "w1"));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 0));
}

#[test]
fn window_buffer_debounces_a_partition_just_written() {
    let mut h = H::new("pop-window");
    let mut cfg = qcfg();
    cfg.window_buffer = 5;
    h.queue("q", cfg);
    h.push("q", "p0", &["a"]);
    h.advance(1_000_000);
    assert!(
        h.pinned("q", "p0", "g", "w1").is_empty(),
        "inside the window"
    );
    assert_eq!(
        h.pinned_with("q", "p0", "g", "w1", |p| p.skip_window_debounce = true)
            .len(),
        1,
        "skip_window_debounce delivers"
    );
    h.push("q", "p1", &["b"]);
    assert!(h
        .wildcard("q", "g2", "w")
        .iter()
        .all(|c| c.pid != h.pid_of("q", "p1")));
    h.advance(6_000_000);
    assert!(h
        .wildcard("q", "g2", "w")
        .iter()
        .any(|c| c.pid == h.pid_of("q", "p1")));
}

#[test]
fn conflation_serves_the_newest_frame_and_leases_the_whole_span() {
    let mut h = H::new("pop-conflate");
    h.push("q", "p0", &["a", "b", "c"]);
    h.advance(1_000_000);
    let claim = only(h.pinned_with("q", "p0", "g", "w1", |p| p.conflate = true));
    assert!(claim.conflated);
    assert_eq!((claim.start_offset, claim.end_offset), (2, 2));
    let pid = h.pid_of("q", "p0");
    let cur = h.cursor(pid, "g").unwrap();
    assert_eq!(cur.batch_end, Some(2));
    assert!(cur.lease_conflated);
}

#[test]
fn a_wildcard_pop_shares_one_budget_across_partitions() {
    let mut h = H::new("pop-wild-budget");
    h.push("q", "p0", &["a", "b"]);
    h.push("q", "p1", &["x", "y"]);
    h.advance(1_000_000);
    let cs = h.wildcard_with("q", "g", "w1", |p| {
        p.sub = mode("all");
        p.budget = 3;
    });
    let total: i64 = cs
        .iter()
        .map(|c| c.end_offset as i64 - c.start_offset as i64 + 1)
        .sum();
    assert_eq!(total, 3, "the whole budget was spent: {cs:?}");
}

#[test]
fn a_wildcard_pop_honours_max_parts() {
    let mut h = H::new("pop-maxparts");
    h.push("q", "p0", &["a"]);
    h.push("q", "p1", &["b"]);
    h.advance(1_000_000);
    let cs = h.wildcard_with("q", "g", "w1", |p| p.max_parts = 1);
    assert_eq!(cs.len(), 1);
    assert_eq!(
        h.e.ready_count(T, "q", "g"),
        Some(1),
        "the other stays ready"
    );
}

#[test]
fn the_first_wildcard_contact_reaches_every_partition_of_the_queue() {
    let mut h = H::new("pop-enumerate");
    h.push("q", "p0", &["a"]);
    h.push("q", "p1", &["b"]);
    h.advance(1_000_000);
    let cs = h.wildcard_with("q", "g", "w1", |p| p.budget = 100);
    assert_eq!(cs.len(), 2);
}

#[test]
fn an_empty_retained_partition_is_sealed() {
    let mut h = H::new("pop-seal");
    let pid = h.push("q", "p0", &["a", "b"]);
    // Retention took every frame the group never read.
    h.watermark(pid, 2);
    h.advance(1_000_000);
    assert!(h.pinned("q", "p0", "g", "w1").is_empty());
    h.checkpoint();
    assert_eq!(
        h.cursor(pid, "g").unwrap().committed,
        1,
        "sealed to the tail so it stops being a candidate"
    );
    // A registered group behind the log start is sealed the same way.
    let p1 = h.push("q", "p1", &["x", "y"]);
    only(h.wildcard_with("q", "g2", "w", |c| {
        c.budget = 1;
        c.auto_ack = true;
    }));
    h.watermark(p1, 2);
    h.advance(1_000_000);
    assert!(h.wildcard("q", "g2", "w").is_empty());
    h.checkpoint();
    assert_eq!(h.cursor(p1, "g2").unwrap().committed, 1);
}

#[test]
fn a_retention_gap_starts_the_batch_at_the_log_start() {
    let mut h = H::new("pop-gap");
    let pid = h.push("q", "p0", &["a", "b"]);
    h.push("q", "p0", &["c", "d"]);
    h.watermark(pid, 2);
    let claim = only(h.pinned("q", "p0", "g", "w1"));
    assert_eq!((claim.start_offset, claim.end_offset), (2, 3));
}

#[test]
fn a_caught_up_partition_writes_nothing() {
    let mut h = H::new("pop-caughtup");
    let pid = h.push("q", "p0", &["a"]);
    only(h.pinned_with("q", "p0", "g", "w1", |p| p.auto_ack = true));
    assert_eq!(h.cursor(pid, "g").unwrap().committed, 0);
    assert!(h.pinned("q", "p0", "g", "w1").is_empty());
    assert_eq!(h.checkpoint(), 0);
}

#[test]
fn a_pinned_pop_of_an_unknown_partition_is_empty_and_provisions_nothing() {
    let mut h = H::new("pop-unknown");
    h.push("q", "p0", &["a"]);
    assert!(h.pinned("q", "does-not-exist", "g", "w1").is_empty());
    assert!(h
        .store
        .read(|r| crate::rsm::store::TypedReads::pid_of(r, T, "q", "does-not-exist"))
        .unwrap()
        .is_none());
}

#[test]
fn a_plain_pinned_pop_does_not_register_the_group() {
    let mut h = H::new("pop-pinned-no-register");
    h.push("q", "p0", &["a", "b"]);
    h.advance(1_000_000);
    let claim = only(h.pinned("q", "p0", "g", "w1"));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 1));
    h.checkpoint();
    assert!(!h.group_exists("q", "g"));
}

#[test]
fn a_pinned_pop_then_a_wildcard_still_delivers_the_other_partitions_backlog() {
    let mut h = H::new("pop-pinned-then-wildcard");
    h.push("q", "p0", &["a", "b"]);
    h.push("q", "p1", &["x", "y"]);
    h.advance(1_000_000);
    let claim = only(h.pinned_with("q", "p0", "g", "w1", |p| {
        p.budget = 2;
        p.auto_ack = true;
    }));
    assert_eq!((claim.start_offset, claim.end_offset), (0, 1));
    assert!(!h.group_exists("q", "g"));
    h.advance(1_000_000);
    let cs = h.wildcard("q", "g", "w2");
    assert_eq!(cs.len(), 1, "{cs:?}");
    let p1 = h.pid_of("q", "p1");
    assert_eq!(cs[0].pid, p1);
    assert_eq!((cs[0].start_offset, cs[0].end_offset), (0, 1));
}

#[test]
fn a_conflating_pinned_pop_registers_and_bulk_seeds_the_whole_queue() {
    let mut h = H::new("pop-pinned-conflate-seed");
    h.push("q", "p0", &["a", "b"]);
    h.push("q", "p1", &["x", "y"]);
    h.advance(1_000_000);
    let claim = only(h.pinned_with("q", "p0", "g", "w1", |p| p.conflate = true));
    assert!(claim.conflated);
    assert_eq!((claim.start_offset, claim.end_offset), (1, 1));
    h.checkpoint();
    assert!(h.group_exists("q", "g"));
    let p1 = h.pid_of("q", "p1");
    assert_eq!(h.cursor(p1, "g").unwrap().committed, -1, "bulk seeded");
    h.advance(1_000_000);
    let cs = h.wildcard("q", "g", "w2");
    assert_eq!(cs.len(), 1);
    assert_eq!(cs[0].pid, p1);
    assert!(cs[0].conflated, "the stored conflation policy wins");
    assert_eq!((cs[0].start_offset, cs[0].end_offset), (1, 1));
}

#[test]
fn a_conflating_pinned_pop_whose_partition_is_already_seeded_does_not_register() {
    let mut h = H::new("pop-plain-then-conflate-pinned");
    h.push("q", "p0", &["a", "b"]);
    h.push("q", "p1", &["x", "y"]);
    h.advance(1_000_000);
    only(h.pinned_with("q", "p0", "g", "w1", |p| p.auto_ack = true));
    let p1 = h.pid_of("q", "p1");
    h.checkpoint();
    assert!(!h.group_exists("q", "g"));
    h.advance(1_000_000);
    h.pinned_with("q", "p0", "g", "w1", |p| p.conflate = true);
    h.checkpoint();
    assert!(!h.group_exists("q", "g"), "not the registrar");
    assert!(h.cursor(p1, "g").is_none(), "no bulk seed");
    h.advance(1_000_000);
    let cs = h.wildcard("q", "g", "w2");
    assert_eq!(cs.len(), 1);
    assert_eq!(cs[0].pid, p1);
    assert!(!cs[0].conflated, "delivered PLAIN");
    assert_eq!((cs[0].start_offset, cs[0].end_offset), (0, 1));
}

#[test]
fn a_pop_past_its_deadline_claims_nothing() {
    let mut h = H::new("pop-deadline");
    h.push("q", "p0", &["a"]);
    let got = h.wildcard_with("q", "g", "w1", |p| p.deadline_us = 1);
    assert!(got.is_empty(), "nobody can receive it");
    assert_eq!(only(h.wildcard("q", "g", "w1")).start_offset, 0);
}

#[test]
fn a_wildcard_pop_creates_its_missing_queue_and_registers() {
    let mut h = H::new("pop-create");
    assert!(h
        .wildcard_with("q", "g", "w1", |p| p.sub = mode("new"))
        .is_empty());
    assert!(h.group_exists("q", "g"));
    assert!(h
        .store
        .read(|r| crate::rsm::store::TypedReads::queue(r, T, "q"))
        .unwrap()
        .is_some());
    h.advance(1_000_000);
    h.push("q", "p0", &["a"]);
    let c = only(h.wildcard("q", "g", "w1"));
    assert_eq!((c.start_offset, c.end_offset), (0, 0));
}

#[test]
fn a_discovery_pop_walks_the_matching_queues() {
    let mut h = H::new("pop-discover");
    let mut a = qcfg();
    a.namespace = Some("ns".into());
    h.queue("qa", a.clone());
    h.queue("qb", a);
    h.queue("qc", qcfg());
    h.push("qa", "p", &["1"]);
    h.push("qb", "p", &["2"]);
    h.push("qc", "p", &["3"]);
    let mut c = h.pop_cmd("", "g", "w1");
    c.namespace = "ns".into();
    c.queue = String::new();
    let cs = super::tests::claims(&h.outcome(crate::rsm::batcher::Command::PopDiscover(c)));
    assert_eq!(cs.len(), 2, "{cs:?}");
    let pc = h.pid_of("qc", "p");
    assert!(cs.iter().all(|c| c.pid != pc));
}

/// The pop autopilot leaves its width to the engine ([`MAX_PARTS_AUTO`]): the
/// group's ready partitions shared among its waiting pops and this one, up to
/// 64 — what a follower's forwarded pop gets too, since only the leader knows
/// the count.
#[test]
fn an_autopilot_pop_takes_the_ready_partitions_up_to_64() {
    use crate::rsm::planner::MAX_PARTS_AUTO;
    let mut h = H::new("pop-auto-width");
    for p in 0..8 {
        h.push("q", &format!("p{p}"), &[&format!("a{p}")]);
    }
    let c = h.wildcard_with("q", "g", "w1", |c| {
        c.max_parts = MAX_PARTS_AUTO;
        c.budget = 1000;
    });
    assert_eq!(c.len(), 8, "every ready partition: {c:?}");

    for p in 0..70 {
        h.push("q2", &format!("p{p}"), &[&format!("b{p}")]);
    }
    let c = h.wildcard_with("q2", "g", "w2", |c| {
        c.max_parts = MAX_PARTS_AUTO;
        c.budget = 1000;
    });
    assert_eq!(c.len(), 64, "the widest checkout");
    // The six left go to the next pop.
    let rest = h.wildcard_with("q2", "g", "w3", |c| {
        c.max_parts = MAX_PARTS_AUTO;
        c.budget = 1000;
    });
    assert_eq!(rest.len(), 6);
}

// A pop's answer waits for the checkpoint that holds its lease. When nobody
// can receive it any more (the facade gave up at its deadline, the client
// left), the engine hands the leases back at once. No worker saw those
// messages, so the next delivery is still the first: counting the dropped
// claim as an attempt made a Laravel job with tries = 1 fail without running.
#[test]
fn a_claim_nobody_receives_is_released_without_counting_an_attempt() {
    let mut h = H::new("pop-unanswered-engine");
    h.push("q", "p0", &["a"]);
    let mut c = h.pop_cmd("q", "g", "w1");
    c.partition = Some("p0".to_string());
    h.e.tick(h.now);
    match h.e.serve(&Command::PopPinned(c), h.now) {
        Served::Later(rx) => drop(rx),
        Served::Now(r) => panic!("a claim's answer waits for its checkpoint: {r:?}"),
        Served::NotMine => panic!("not the engine's"),
    }
    h.e.wait_loads();
    h.checkpoint();

    let again = only(h.pinned("q", "p0", "g", "w2"));
    assert_eq!(again.start_offset, 0);
    assert_eq!(again.delivery_attempt, 1, "nobody received the first claim");
}

// The facade's twin: a follower whose caller is gone, or a pop answered after
// its deadline, hands each leased claim back with a Nack.
#[test]
fn a_claim_handed_back_by_a_nack_does_not_count_an_attempt() {
    let mut h = H::new("pop-unanswered-nack");
    h.push("q", "p0", &["a"]);
    let first = only(h.pinned("q", "p0", "g", "w1"));
    assert_eq!(first.delivery_attempt, 1);
    let pid = h.pid_of("q", "p0");
    h.nack(pid, "q", "g", &first.worker);

    let again = only(h.pinned("q", "p0", "g", "w2"));
    assert_eq!(again.start_offset, first.start_offset);
    assert_eq!(again.delivery_attempt, 1, "nobody received the first claim");
}

// Handing back a dropped claim takes back only its own attempt: a real
// redelivery before it still counts.
#[test]
fn a_dropped_redelivery_keeps_the_attempts_before_it() {
    let mut h = H::new("pop-unanswered-redelivery");
    h.push("q", "p0", &["a"]);
    only(h.pinned_with("q", "p0", "g", "w1", |p| p.lease_seconds = 1));
    h.advance(5_000_000);
    let second = only(h.pinned("q", "p0", "g", "w2"));
    assert_eq!(second.delivery_attempt, 2, "the expiry redelivered it");
    let pid = h.pid_of("q", "p0");
    h.nack(pid, "q", "g", &second.worker);

    let third = only(h.pinned("q", "p0", "g", "w3"));
    assert_eq!(
        third.delivery_attempt, 2,
        "the dropped claim is not an attempt"
    );
}
