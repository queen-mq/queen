//! The idle-part unloader ([`super::unload`]): what goes, what stays, and that
//! a part loaded back answers as the one dropped would have.
#![allow(clippy::needless_range_loop)]

use crate::rsm::batcher::{Command, Reply};
use crate::rsm::entry::{Outcome, PopClaim};
use crate::rsm::planner::AckStatus;

use std::collections::HashMap;

use super::tests::{aggressive_unload, knobs, qcfg, H};
use super::{wall_us, Knobs};

const IDLE: i64 = 60_000_000;

fn unloading() -> Knobs {
    Knobs {
        idle_unload_us: IDLE,
        ..knobs()
    }
}

/// `n` partitions of `q` holding one message each, consumed and acked by `g`
/// (a wildcard group: it holds the whole queue), every change durable.
fn drained(h: &mut H, q: &str, g: &str, n: usize) {
    h.queue(q, qcfg());
    for i in 0..n {
        h.push(q, &format!("p{i}"), &[&format!("m{i}")]);
    }
    let mut seen = 0;
    while seen < n {
        let claims = h.wildcard(q, g, "w1");
        assert!(!claims.is_empty(), "every partition gets consumed");
        for c in &claims {
            let txn = format!("m{}", c.pid - 1);
            let r = h.ack(c.pid, q, g, &c.worker, &[(txn.as_str(), AckStatus::Ok)]);
            assert!(r.lease_released, "{r:?}");
            seen += 1;
        }
    }
    h.checkpoint();
}

/// The unloader's two looks: the first starts each part's idle clock, the
/// second (the window later) drops what stayed idle.
fn idle_out(h: &mut H) -> u64 {
    h.e.unload_all(h.now);
    h.advance(IDLE + 1);
    h.e.unload_all(h.now)
}

#[test]
fn idle_parts_go_and_an_append_brings_its_part_back() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with("unload-basic", unloading());
    drained(&mut h, "q", "g", 20);
    let before = h.e.engine_stats();
    assert_eq!(before.parts, 20, "{before:?}");
    assert_eq!(before.partitions, 20, "{before:?}");

    assert_eq!(idle_out(&mut h), 20);
    let after = h.e.engine_stats();
    assert_eq!(after.parts, 0, "{after:?}");
    assert_eq!(
        after.partitions, 20,
        "the partitions keep their watchers: {after:?}"
    );
    assert_eq!(after.unloaded_total, 20);

    // A message on one of them: the part comes back, at the cursor it had.
    let pid = h.push("q", "p7", &["again"]);
    let claims = h.wildcard("q", "g", "w2");
    assert_eq!(claims.len(), 1, "{claims:?}");
    let c: &PopClaim = &claims[0];
    assert_eq!(c.pid, pid);
    assert_eq!(
        (c.start_offset, c.end_offset),
        (1, 1),
        "only the new message: {c:?}"
    );
    assert_eq!(c.delivery_attempt, 1);
    let r = h.ack(pid, "q", "g", &c.worker, &[("again", AckStatus::Ok)]);
    assert!(r.lease_released && r.committed == 1, "{r:?}");
    let s = h.e.engine_stats();
    assert_eq!(s.parts, 1, "only the partition that got a message: {s:?}");
    assert_eq!(s.loaded_total, before.loaded_total + 1, "{s:?}");
}

/// The clock work takes at most a batch of ended idle windows from a shard per
/// tick ([`super::wait::IDLE_PER_TICK`]): a tick holds the shard's lock, and
/// idle windows end together by the thousand (a new leader loads a whole queue
/// at once). Every part still goes, over the next ticks, and no tick takes
/// more than its batch.
#[test]
fn the_clock_work_drops_due_parts_in_bounded_batches() {
    if aggressive_unload() {
        return;
    }
    use super::state::{lock, Cur, GroupCfg, GroupShard, Load, Part, PidInfo};
    use super::wait::IDLE_PER_TICK;
    let h = H::with("unload-batches", unloading());
    let g = h.e.intern(
        super::tests::T,
        "q",
        "g",
        GroupCfg {
            queue: qcfg(),
            meta: None,
        },
        Load::Full,
    );
    // Three batches and some, all in shard 0, all idle from the same instant.
    let n = 3 * IDLE_PER_TICK + 100;
    let now = h.now;
    {
        let mut sh = lock(&h.e.shards[0]);
        let sh = &mut *sh;
        for i in 1..=n as u64 {
            let pid = i * 16;
            sh.pids.insert(
                pid,
                PidInfo {
                    tail: 5,
                    log_start: 0,
                    txns_start: 0,
                    rows_start: 0,
                    last_append_us: 0,
                    watchers: smallvec::smallvec![g.id],
                },
            );
            sh.groups
                .entry(g.id)
                .or_insert_with(|| GroupShard::new(g.clone()))
                .parts
                .insert(pid, Box::new(Part::new(Cur::fresh(5, 0), true)));
            super::unload::arm_idle_in(sh, g.id, pid, now);
        }
    }
    assert_eq!(h.e.engine_stats().parts, n);
    h.e.tick(now + IDLE + 1);
    let s = h.e.engine_stats();
    assert_eq!(
        (s.parts, s.unloaded_total),
        (n - IDLE_PER_TICK, IDLE_PER_TICK as u64),
        "one tick, one batch: {s:?}"
    );
    for t in 0..10 {
        h.e.tick(now + IDLE + 2 + t);
    }
    let s = h.e.engine_stats();
    assert_eq!((s.parts, s.unloaded_total), (0, n as u64), "{s:?}");
}

/// The same from the engine's own clock work, which takes a turn every few
/// milliseconds ([`super::Engine::tick`]): idle parts go without anyone
/// asking, a window after their last change.
#[test]
fn the_clock_work_unloads_idle_parts_by_itself() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with("unload-clock", unloading());
    drained(&mut h, "q", "g", 40);
    // The clock work runs, the window passes, the clock work runs again.
    for _ in 0..40 {
        h.advance(5_000);
        h.e.tick(h.now);
    }
    assert_eq!(h.e.engine_stats().parts, 40, "nothing is idle yet");
    h.advance(IDLE + 5_000_000);
    for _ in 0..40 {
        h.advance(5_000);
        h.e.tick(h.now);
    }
    let s = h.e.engine_stats();
    assert_eq!((s.parts, s.unloaded_total), (0, 40), "{s:?}");
}

#[test]
fn a_part_that_holds_anything_stays() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with("unload-holds", unloading());
    drained(&mut h, "q", "g", 3);
    // p0: a lease nobody acked. p1: a message waiting. p2: acked, not yet
    // checkpointed (dirty).
    h.push("q", "p0", &["lease"]);
    // A lease longer than the idle window: still held when the unloader looks.
    let leased = h.wildcard_with("q", "g", "w1", |c| {
        c.max_parts = 1;
        c.lease_seconds = 3600;
    });
    assert_eq!(leased.len(), 1);
    let p0 = leased[0].pid;
    h.push("q", "p1", &["waiting"]);
    h.push("q", "p2", &["dirty"]);
    // Claim p2 only (pinned), ack it, and do not checkpoint.
    let c2 = h.pinned("q", "p2", "g", "w3");
    assert_eq!(c2.len(), 1);
    let ack = crate::rsm::planner::AckCommand {
        request_id: h.rid(),
        targets: vec![h.ack_target(
            c2[0].pid,
            "q",
            "g",
            &c2[0].worker,
            &[("dirty", AckStatus::Ok)],
        )],
    };
    let mut rx = h.later(Command::Ack(ack));

    assert_eq!(
        idle_out(&mut h),
        0,
        "nothing idle: {:?}",
        h.e.engine_stats()
    );
    assert_eq!(h.e.engine_stats().parts, 3);

    // The ack becomes durable; the lease is acked; the waiting message is
    // consumed: all three go after a window of quiet.
    for _ in 0..100 {
        h.checkpoint();
        if rx.try_recv().is_ok() {
            break;
        }
    }
    let r = h.ack(p0, "q", "g", &leased[0].worker, &[("lease", AckStatus::Ok)]);
    assert!(r.lease_released, "{r:?}");
    let waiting = h.wildcard("q", "g", "w4");
    assert_eq!(waiting.len(), 1);
    let r = h.ack(
        waiting[0].pid,
        "q",
        "g",
        &waiting[0].worker,
        &[("waiting", AckStatus::Ok)],
    );
    assert!(r.lease_released, "{r:?}");
    h.checkpoint();
    assert_eq!(idle_out(&mut h), 3, "{:?}", h.e.engine_stats());
    assert_eq!(h.e.engine_stats().parts, 0);
}

#[test]
fn a_parked_pop_gets_a_message_pushed_to_an_unloaded_partition() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with("unload-park", unloading());
    drained(&mut h, "q", "g", 5);
    assert_eq!(idle_out(&mut h), 5);
    let mut c = h.pop_cmd("q", "g", "w9");
    c.wait = true;
    c.deadline_us = wall_us() + 10_000_000;
    h.now = wall_us();
    let mut rx = h.later(Command::PopWildcard(c));
    assert!(rx.try_recv().is_err(), "nothing to claim yet");
    h.now = wall_us();
    let pid = h.push("q", "p3", &["late"]);
    for _ in 0..3000 {
        h.checkpoint();
        if let Ok(r) = rx.try_recv() {
            match r {
                Reply::Done {
                    outcome: Outcome::Pop(p),
                    ..
                } => {
                    assert_eq!(p.claims.len(), 1, "{p:?}");
                    assert_eq!(p.claims[0].pid, pid);
                    assert_eq!(p.claims[0].start_offset, 1);
                    return;
                }
                other => panic!("{other:?}"),
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    panic!("the parked pop never got the message");
}

#[test]
fn a_parked_pinned_pop_gets_a_message_pushed_to_its_unloaded_partition() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with("unload-park-pinned", unloading());
    drained(&mut h, "q", "g", 4);
    assert_eq!(idle_out(&mut h), 4);
    let mut c = h.pop_cmd("q", "g", "w9");
    c.partition = Some("p2".to_string());
    c.wait = true;
    c.deadline_us = wall_us() + 10_000_000;
    h.now = wall_us();
    let mut rx = h.later(Command::PopPinned(c));
    assert!(rx.try_recv().is_err(), "nothing to claim yet");
    // The pinned pop loaded p2 back; drop it again under the parked pop.
    h.advance(IDLE + 1);
    h.e.unload_now(h.now);
    h.now = wall_us();
    let pid = h.push("q", "p2", &["late"]);
    for _ in 0..3000 {
        h.checkpoint();
        if let Ok(r) = rx.try_recv() {
            match r {
                Reply::Done {
                    outcome: Outcome::Pop(p),
                    ..
                } => {
                    assert_eq!(p.claims.len(), 1, "{p:?}");
                    assert_eq!(p.claims[0].pid, pid);
                    return;
                }
                other => panic!("{other:?}"),
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    panic!("the parked pinned pop never got the message");
}

#[test]
fn a_repeated_ack_after_an_unload_is_answered_as_the_first() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with("unload-reack", unloading());
    h.queue("q", qcfg());
    let pid = h.push("q", "p0", &["a", "b"]);
    let claims = h.wildcard("q", "g", "w1");
    assert_eq!(claims.len(), 1);
    let w = claims[0].worker.clone();
    let first = h.ack(
        pid,
        "q",
        "g",
        &w,
        &[("a", AckStatus::Ok), ("b", AckStatus::Ok)],
    );
    assert!(first.lease_released, "{first:?}");
    h.checkpoint();
    assert_eq!(idle_out(&mut h), 1);
    // The client never saw the answer and sends the same ack again: the
    // released lease came back with the part, so the repeat is answered
    // success (`repeat_of_released`), not stale.
    let again = h.ack(
        pid,
        "q",
        "g",
        &w,
        &[("a", AckStatus::Ok), ("b", AckStatus::Ok)],
    );
    assert_eq!(again.committed, first.committed, "{again:?}");
    assert!(again.lease_released, "{again:?}");
    assert!(again.stale_hashes.is_empty(), "nothing stale: {again:?}");
}

#[test]
fn a_partial_group_keeps_its_parts() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with("unload-partial", unloading());
    h.queue("q", qcfg());
    let pid = h.push("q", "p0", &["a"]);
    // Only pinned pops: the group holds the partitions they named.
    let c = h.pinned("q", "p0", "solo", "w1");
    assert_eq!(c.len(), 1);
    let r = h.ack(pid, "q", "solo", &c[0].worker, &[("a", AckStatus::Ok)]);
    assert!(r.lease_released);
    h.checkpoint();
    assert_eq!(idle_out(&mut h), 0);
    assert_eq!(h.e.engine_stats().parts, 1);
}

#[test]
fn nothing_goes_when_unloading_is_off() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with(
        "unload-off",
        Knobs {
            idle_unload_us: 0,
            ..knobs()
        },
    );
    drained(&mut h, "q", "g", 5);
    h.e.unload_all(h.now);
    h.advance(10 * IDLE);
    assert_eq!(h.e.unload_all(h.now), 0);
    assert_eq!(h.e.engine_stats().parts, 5);
}

#[test]
fn a_new_leader_loads_what_was_unloaded() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with("unload-failover", unloading());
    drained(&mut h, "q", "g", 6);
    assert_eq!(idle_out(&mut h), 6);
    h.failover_with(unloading());
    let pid = h.push("q", "p4", &["after"]);
    let claims = h.wildcard("q", "g", "w5");
    assert_eq!(claims.len(), 1, "{claims:?}");
    assert_eq!(claims[0].pid, pid);
    assert_eq!(claims[0].start_offset, 1);
}

/// Consume and ack, as the wildcard group `g`, every message `txn_of` names
/// (one message per partition, keyed by its pid), every change durable.
fn consume_all(h: &mut H, q: &str, g: &str, txn_of: &HashMap<u64, String>) {
    let mut seen = 0;
    while seen < txn_of.len() {
        let claims = h.wildcard(q, g, "w1");
        assert!(!claims.is_empty(), "every partition gets consumed");
        for c in &claims {
            let t = txn_of[&c.pid].clone();
            let r = h.ack(c.pid, q, g, &c.worker, &[(t.as_str(), AckStatus::Ok)]);
            assert!(r.lease_released, "{r:?}");
            seen += 1;
        }
    }
    h.checkpoint();
}

/// The engine's own clock work, `n` ticks 5 ms apart from now.
fn ticks(h: &mut H, n: usize) {
    for _ in 0..n {
        h.advance(5_000);
        h.e.tick(h.now);
    }
}

/// A part goes when its window of quiet ends: the first clock tick past it
/// drops it, with no earlier look needed. (The id-walking pass looked twice, a
/// window apart, and began a pass at most every 16 s: a part could stay up to
/// two windows plus the pass's own length.)
#[test]
fn an_idle_part_goes_when_its_window_ends() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with("unload-on-time", unloading());
    drained(&mut h, "q", "g", 20);
    let quiet = h.now;
    h.now = quiet + IDLE - 1_000_000;
    h.e.tick(h.now);
    assert_eq!(
        h.e.engine_stats().parts,
        20,
        "a second before the window ends, all stay"
    );
    // Seconds of slack: an append the serve thread took over is stamped by
    // the engine's own clock, a little past the test's.
    h.now = quiet + IDLE + 5_000_000;
    h.e.tick(h.now);
    let s = h.e.engine_stats();
    assert_eq!(
        (s.parts, s.unloaded_total),
        (0, 20),
        "the first tick past the window drops them all: {s:?}"
    );
}

/// However many partition ids a busy queue used and gave back before, an idle
/// part goes on time. The id-walking pass visited every id from the lowest
/// live partition to the highest ever seen, one step per group for each: ten
/// million ids took it hundreds of turns a shard, and at prod's settings hours.
#[test]
fn an_idle_part_goes_on_time_however_many_ids_came_before() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with("unload-sparse", unloading());
    h.queue("q", qcfg());
    // A partition that lives long, then ten million ids of partitions that
    // came and went, then twenty more.
    let mut txn_of: HashMap<u64, String> = HashMap::new();
    txn_of.insert(h.push("q", "old", &["m-old"]), "m-old".to_string());
    h.skip_pids(10_000_000);
    for i in 0..20 {
        let t = format!("m{i}");
        txn_of.insert(h.push("q", &format!("p{i}"), &[t.as_str()]), t);
    }
    consume_all(&mut h, "q", "g", &txn_of);
    assert_eq!(h.e.engine_stats().parts, 21);
    // The clock work runs a while (any first look it needs), the window
    // passes, the clock work runs a while again.
    ticks(&mut h, 100);
    assert_eq!(h.e.engine_stats().parts, 21, "nothing is idle yet");
    h.advance(IDLE + 5_000_000);
    ticks(&mut h, 100);
    let s = h.e.engine_stats();
    assert_eq!((s.parts, s.unloaded_total), (0, 21), "{s:?}");
}

/// A part that gets work inside its window waits a whole window from its last
/// change: the look due for its first quiet finds it changed, and it goes a
/// window after the second.
#[test]
fn a_part_that_got_work_waits_a_whole_window_from_its_last_change() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with("unload-rearm", unloading());
    drained(&mut h, "q", "g", 20);
    let quiet = h.now;
    // Half a window later p7 gets a message, consumed and acked.
    h.now = quiet + IDLE / 2;
    let pid = h.push("q", "p7", &["again"]);
    let c = h.wildcard("q", "g", "w2");
    assert_eq!(c.len(), 1, "{c:?}");
    let r = h.ack(pid, "q", "g", &c[0].worker, &[("again", AckStatus::Ok)]);
    assert!(r.lease_released, "{r:?}");
    h.checkpoint();
    let busy = h.now;
    // The first window ends: the other nineteen go, p7 stays.
    h.now = quiet + IDLE + 5_000_000;
    h.e.tick(h.now);
    let s = h.e.engine_stats();
    assert_eq!((s.parts, s.unloaded_total), (1, 19), "{s:?}");
    // A second before its own window ends it is still there; past it, it goes.
    h.now = busy + IDLE - 1_000_000;
    h.e.tick(h.now);
    assert_eq!(h.e.engine_stats().parts, 1);
    h.now = busy + IDLE + 5_000_000;
    h.e.tick(h.now);
    let s = h.e.engine_stats();
    assert_eq!((s.parts, s.unloaded_total), (0, 20), "{s:?}");
    // Its partition gets a message: the part comes back at the cursor it had.
    let again = h.push("q", "p7", &["third"]);
    let c = h.wildcard("q", "g", "w3");
    assert_eq!(c.len(), 1, "{c:?}");
    assert_eq!((c[0].pid, c[0].start_offset), (again, 2), "{c:?}");
}

/// A new leader's first wildcard pop loads every partition of the queue. With
/// nothing to deliver in any, each part goes one window later, with no other
/// look: the load leaves them armed although the group became whole-queue
/// only once the load ended.
#[test]
fn a_new_leader_drops_what_its_load_brought_in() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with("unload-load", unloading());
    drained(&mut h, "q", "g", 30);
    h.failover_with(unloading());
    let claims = h.wildcard("q", "g", "w2");
    assert!(claims.is_empty(), "nothing left to deliver: {claims:?}");
    let loaded = h.e.engine_stats();
    assert_eq!(
        loaded.parts, 30,
        "the load brought every partition in: {loaded:?}"
    );
    let t = h.now;
    h.now = t + IDLE - 1_000_000;
    h.e.tick(h.now);
    assert_eq!(h.e.engine_stats().parts, 30);
    h.now = t + IDLE + 5_000_000;
    h.e.tick(h.now);
    let s = h.e.engine_stats();
    assert_eq!((s.parts, s.unloaded_total), (0, 30), "{s:?}");
}

/// What the shards' idle wheels hold: entries, and the slots they are in.
fn wheel(h: &H) -> (usize, usize) {
    let (mut entries, mut slots) = (0, 0);
    for s in h.e.shards.iter() {
        let sh = super::state::lock(s);
        entries += sh.idle.len();
        slots += sh.idle.slots();
    }
    (entries, slots)
}

/// The wheel holds one entry per part that went quiet, and nothing once their
/// windows have ended: a slot it emptied is let go.
#[test]
fn the_idle_wheel_lets_go_of_what_it_held() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with("unload-wheel-empty", unloading());
    drained(&mut h, "q", "g", 200);
    let (entries, slots) = wheel(&h);
    assert_eq!(entries, 200, "one entry per part that went quiet");
    assert!(slots >= 1);
    h.advance(IDLE + 5_000_000);
    ticks(&mut h, 4);
    let s = h.e.engine_stats();
    assert_eq!((s.parts, s.unloaded_total), (0, 200), "{s:?}");
    assert_eq!(wheel(&h), (0, 0), "every entry taken, every slot let go");
}

/// A part that keeps changing inside its window keeps one entry: each quiet
/// moment notes when, the entry already due stays as it is, and its end looks
/// again and files the part for the window after the last change.
#[test]
fn a_busy_part_keeps_one_wheel_entry() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with("unload-wheel-busy", unloading());
    drained(&mut h, "q", "g", 1);
    assert_eq!(wheel(&h).0, 1);
    let quiet = h.now;
    let mut last = h.now;
    for i in 0..30 {
        h.advance(1_000_000);
        let t = format!("again{i}");
        let pid = h.push("q", "p0", &[t.as_str()]);
        let c = h.wildcard("q", "g", "w2");
        assert_eq!(c.len(), 1, "{c:?}");
        let r = h.ack(pid, "q", "g", &c[0].worker, &[(t.as_str(), AckStatus::Ok)]);
        assert!(r.lease_released, "{r:?}");
        h.checkpoint();
        last = h.now;
        assert_eq!(wheel(&h).0, 1, "change {i}: still one entry");
    }
    // The first window ends: its entry finds the part changed and files it
    // for a window after the last change.
    h.now = quiet + IDLE + 5_000_000;
    h.e.tick(h.now);
    assert_eq!(h.e.engine_stats().parts, 1);
    assert_eq!(wheel(&h).0, 1, "filed again, once");
    h.now = last + IDLE + 5_000_000;
    h.e.tick(h.now);
    let s = h.e.engine_stats();
    assert_eq!((s.parts, s.unloaded_total), (0, 1), "{s:?}");
    assert_eq!(wheel(&h), (0, 0));
}

/// Windows that end out of the order their parts were filed in still end on
/// time: the wheel files by due time, and inside one slot takes only what is
/// due. (Checkpoints resolve out of order, each arming its parts at its own
/// take: entries are filed out of order all the time under load.)
#[test]
fn idle_windows_that_end_out_of_order_end_on_time() {
    if aggressive_unload() {
        return;
    }
    use super::state::{lock, Cur, GroupCfg, GroupShard, Load, Part, PidInfo};
    let h = H::with("unload-wheel-order", unloading());
    let g = h.e.intern(
        super::tests::T,
        "q",
        "g",
        GroupCfg {
            queue: qcfg(),
            meta: None,
        },
        Load::Full,
    );
    // Quiet since (filed in this order): A at +3 s, B at +1 s, C at +2 s,
    // then D and E 4 ms apart in one 10 ms slot of the wheel.
    let t0 = h.now;
    let d_due = ((t0 + 5_000_000 + IDLE) / 10_000 + 1) * 10_000 + 1_000;
    let quiet = [
        (16u64, t0 + 3_000_000),
        (32, t0 + 1_000_000),
        (48, t0 + 2_000_000),
        (64, d_due - IDLE),
        (80, d_due - IDLE + 4_000),
    ];
    {
        let mut sh = lock(&h.e.shards[0]);
        let sh = &mut *sh;
        for (pid, at) in quiet {
            sh.pids.insert(
                pid,
                PidInfo {
                    tail: 5,
                    log_start: 0,
                    txns_start: 0,
                    rows_start: 0,
                    last_append_us: 0,
                    watchers: smallvec::smallvec![g.id],
                },
            );
            sh.groups
                .entry(g.id)
                .or_insert_with(|| GroupShard::new(g.clone()))
                .parts
                .insert(pid, Box::new(Part::new(Cur::fresh(5, 0), true)));
            super::unload::arm_idle_in(sh, g.id, pid, at);
        }
    }
    let held = |h: &H| -> Vec<u64> {
        let sh = lock(&h.e.shards[0]);
        let mut v: Vec<u64> = sh
            .groups
            .get(&g.id)
            .map(|gs| gs.parts.keys().copied().collect())
            .unwrap_or_default();
        v.sort_unstable();
        v
    };
    h.e.tick(t0 + 1_000_000 + IDLE - 1);
    assert_eq!(
        held(&h),
        vec![16, 32, 48, 64, 80],
        "a microsecond early, all stay"
    );
    h.e.tick(t0 + 1_000_000 + IDLE + 500_000);
    assert_eq!(
        held(&h),
        vec![16, 48, 64, 80],
        "B goes first, filed after A"
    );
    h.e.tick(t0 + 2_000_000 + IDLE + 500_000);
    assert_eq!(held(&h), vec![16, 64, 80], "then C");
    h.e.tick(t0 + 3_000_000 + IDLE + 500_000);
    assert_eq!(held(&h), vec![64, 80], "then A");
    h.e.tick(d_due + 2_000);
    assert_eq!(
        held(&h),
        vec![80],
        "D's end is in E's slot, 4 ms before E's"
    );
    h.e.tick(d_due + 4_000);
    assert_eq!(held(&h), Vec::<u64>::new());
    assert_eq!(wheel(&h), (0, 0));
}

/// A part that went (here: dropped before its window ended) and came back is
/// another part: the entry the one before left in the wheel neither drops it
/// early nor files it twice. It goes a window after its own last change.
#[test]
fn a_part_that_came_back_is_not_dropped_by_the_entry_of_the_one_before() {
    if aggressive_unload() {
        return;
    }
    let mut h = H::with("unload-wheel-stale", unloading());
    drained(&mut h, "q", "g", 1);
    let quiet = h.now;
    h.advance(1_000_000);
    assert_eq!(h.e.unload_now(h.now), 1, "dropped before its window ended");
    assert_eq!(wheel(&h).0, 1, "its entry is still filed");
    // Half a window later its partition gets a message: the part comes back.
    h.now = quiet + IDLE / 2;
    let pid = h.push("q", "p0", &["again"]);
    let c = h.wildcard("q", "g", "w2");
    assert_eq!(c.len(), 1, "{c:?}");
    let r = h.ack(pid, "q", "g", &c[0].worker, &[("again", AckStatus::Ok)]);
    assert!(r.lease_released, "{r:?}");
    h.checkpoint();
    let back = h.now;
    assert_eq!(wheel(&h).0, 2, "the old entry and its own");
    // The old entry falls due: nothing goes, nothing is filed again.
    h.now = quiet + IDLE + 5_000_000;
    h.e.tick(h.now);
    assert_eq!(h.e.engine_stats().parts, 1);
    assert_eq!(wheel(&h).0, 1, "only its own entry left");
    h.now = back + IDLE + 5_000_000;
    h.e.tick(h.now);
    let s = h.e.engine_stats();
    assert_eq!(s.parts, 0, "{s:?}");
    assert_eq!(wheel(&h), (0, 0));
}

/// xorshift64: a seeded sequence, the same on every run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// A claim as both engines must answer it (partitions are claimed in the
/// order they became ready, which a part loaded back may change: the sets
/// are compared, not the order).
fn key(c: &PopClaim) -> (u64, u64, u64, u32) {
    (c.pid, c.start_offset, c.end_offset, c.delivery_attempt)
}

/// The same random history on two engines, one of them losing every part it
/// can at random moments: no answer may differ. Pops take everything ready,
/// leases run out under the clock, acks, nacks and repeated acks land on
/// parts that were dropped, and both engines fail over now and then.
#[test]
fn unloading_changes_no_answer() {
    for seed in [
        0x9E37_79B9_7F4A_7C15u64,
        0xD1B5_4A32_D192_ED03,
        0xC2B2_AE3D_27D4_EB4F,
    ] {
        differential(seed, 1500, false);
    }
}

/// The same with parts dropped only by their own timers, as in production:
/// a 2 s window and the clock work at every command, so parts go and come
/// back between pops, acks, nacks, leases running out and failovers.
#[test]
fn idle_timers_change_no_answer() {
    for seed in [
        0x2545_F491_4F6C_DD1Du64,
        0x9E37_79B9_7F4A_7C15,
        0x6A09_E667_F3BC_C908,
    ] {
        differential(seed, 1500, true);
    }
}

/// `timers`: the dropping engine loses parts only to their idle timers (a 2 s
/// window); otherwise every part that could go is dropped at random moments.
fn differential(seed: u64, steps: usize, timers: bool) {
    let dropping = || {
        if timers {
            Knobs {
                idle_unload_us: 2_000_000,
                ..knobs()
            }
        } else {
            unloading()
        }
    };
    let mut rng = Rng(seed);
    let mut keep = H::with(
        "unload-diff-keep",
        Knobs {
            idle_unload_us: 0,
            ..knobs()
        },
    );
    let mut drop = H::with("unload-diff-drop", dropping());
    let base = wall_us();
    keep.now = base;
    drop.now = base;
    keep.queue("q", qcfg());
    drop.queue("q", qcfg());
    let groups = ["ga", "gb"];
    // Each partition's messages, by offset.
    let mut msgs: std::collections::HashMap<u64, Vec<String>> = std::collections::HashMap::new();
    // Claims not acked or nacked yet: (group, claim).
    let mut open: Vec<(&str, PopClaim)> = Vec::new();
    // Acks already answered, for repeats: (group, claim, items).
    let mut done: Vec<(&str, PopClaim, Vec<String>)> = Vec::new();
    let mut n = 0u64;
    let mut worker = 0u64;
    // Parts the dropping engine let go, and loaded, over every incarnation.
    let (mut unloaded, mut loaded) = (0u64, 0u64);
    for step in 0..steps {
        let ctx = format!("seed {seed:#x} step {step}");
        match rng.below(100) {
            // Push 1-3 messages to one of 12 partitions.
            0..=24 => {
                let part = format!("p{}", rng.below(12));
                let k = 1 + rng.below(3) as usize;
                let txns: Vec<String> = (0..k)
                    .map(|_| {
                        n += 1;
                        format!("t{n}")
                    })
                    .collect();
                let refs: Vec<&str> = txns.iter().map(String::as_str).collect();
                let pk = keep.push("q", &part, &refs);
                let pd = drop.push("q", &part, &refs);
                assert_eq!(pk, pd, "{ctx}");
                msgs.entry(pk).or_default().extend(txns);
            }
            // A pop that takes everything ready.
            25..=54 => {
                let g = groups[rng.below(2) as usize];
                worker += 1;
                let w = format!("w{worker}");
                let all = |c: &mut crate::rsm::planner::PopCommand| {
                    c.max_parts = 0;
                    c.budget = 100_000;
                };
                let mut a = keep.wildcard_with("q", g, &w, all);
                let mut b = drop.wildcard_with("q", g, &w, all);
                let mut ka: Vec<_> = a.iter().map(key).collect();
                let mut kb: Vec<_> = b.iter().map(key).collect();
                ka.sort_unstable();
                kb.sort_unstable();
                assert_eq!(ka, kb, "{ctx}: pop {g}");
                a.sort_by_key(key);
                b.sort_by_key(key);
                for c in a {
                    open.push((g, c));
                }
            }
            // Ack one open claim wholly, or every open claim (a consumer
            // that caught up: its parts become idle).
            55..=69 if !open.is_empty() => {
                let take = if rng.below(2) == 0 { 1 } else { open.len() };
                for _ in 0..take {
                    let i = rng.below(open.len() as u64) as usize;
                    let (g, c) = open.swap_remove(i);
                    let items: Vec<String> =
                        msgs[&c.pid][c.start_offset as usize..=c.end_offset as usize].to_vec();
                    let it: Vec<(&str, AckStatus)> =
                        items.iter().map(|t| (t.as_str(), AckStatus::Ok)).collect();
                    let ra = keep.ack(c.pid, "q", g, &c.worker, &it);
                    let rb = drop.ack(c.pid, "q", g, &c.worker, &it);
                    assert_eq!(ra, rb, "{ctx}: ack {g} {c:?}");
                    done.push((g, c, items));
                }
            }
            // Nack one open claim.
            70..=74 if !open.is_empty() => {
                let i = rng.below(open.len() as u64) as usize;
                let (g, c) = open.swap_remove(i);
                let ra = keep.nack(c.pid, "q", g, &c.worker);
                let rb = drop.nack(c.pid, "q", g, &c.worker);
                assert_eq!(
                    format!("{ra:?}"),
                    format!("{rb:?}"),
                    "{ctx}: nack {g} {c:?}"
                );
            }
            // The same ack again (a client that lost its answer).
            75..=79 if !done.is_empty() => {
                let i = rng.below(done.len() as u64) as usize;
                let (g, c, items) = done[i].clone();
                let it: Vec<(&str, AckStatus)> =
                    items.iter().map(|t| (t.as_str(), AckStatus::Ok)).collect();
                let ra = keep.ack(c.pid, "q", g, &c.worker, &it);
                let rb = drop.ack(c.pid, "q", g, &c.worker, &it);
                assert_eq!(ra, rb, "{ctx}: repeated ack {g} {c:?}");
            }
            // Time passes: sometimes past the 60 s leases.
            80..=89 => {
                let us = match rng.below(4) {
                    0 => 61_000_000,
                    1 => 5_000_000,
                    _ => 1 + rng.below(50_000) as i64,
                };
                keep.advance(us);
                drop.advance(us);
            }
            // The unloader, on one engine only; or, with the timers, the
            // seconds that end idle windows.
            90..=97 => {
                if timers {
                    let us = 1 + rng.below(3_000_000) as i64;
                    keep.advance(us);
                    drop.advance(us);
                } else {
                    drop.e.unload_now(drop.now);
                }
            }
            // A new leader on both.
            98 => {
                let s = drop.e.engine_stats();
                (unloaded, loaded) = (unloaded + s.unloaded_total, loaded + s.loaded_total);
                keep.failover_with(Knobs {
                    idle_unload_us: 0,
                    ..knobs()
                });
                drop.failover_with(dropping());
            }
            _ => {}
        }
    }
    let s = drop.e.engine_stats();
    (unloaded, loaded) = (unloaded + s.unloaded_total, loaded + s.loaded_total);
    // The history did what it is for: parts dropped, and loaded back.
    eprintln!(
        "differential {seed:#x} (timers {timers}): {steps} steps, {unloaded} parts unloaded, {loaded} loaded"
    );
    assert!(unloaded > 50, "{seed:#x}: only {unloaded} parts unloaded");
}

/// What the clock work costs while it holds a shard when a whole shard's idle
/// windows end together (run by hand, release:
/// `cargo test --release --lib consume::tests_unload::drop_cost -- --ignored --nocapture`).
/// One shard of a 1M-part engine (62.5k parts): arming every part, the ticks
/// that keep them (not idle yet), the ticks that drop them, a batch a tick.
#[test]
#[ignore]
fn drop_cost_at_one_million_parts() {
    use super::state::{lock, Cur, GroupCfg, GroupShard, Load, Part, PidInfo};
    use std::time::Instant;
    let h = H::with("unload-cost", unloading());
    let g = h.e.intern(
        super::tests::T,
        "q",
        "g",
        GroupCfg {
            queue: qcfg(),
            meta: None,
        },
        Load::Full,
    );
    let per_shard = 1_000_000 / super::state::SHARDS;
    let now = h.now;
    let t = Instant::now();
    {
        let mut sh = lock(&h.e.shards[0]);
        let sh = &mut *sh;
        for i in 1..=per_shard as u64 {
            let pid = i * 16;
            sh.pids.insert(
                pid,
                PidInfo {
                    tail: 5,
                    log_start: 0,
                    txns_start: 0,
                    rows_start: 0,
                    last_append_us: 0,
                    watchers: smallvec::smallvec![g.id],
                },
            );
            sh.groups
                .entry(g.id)
                .or_insert_with(|| GroupShard::new(g.clone()))
                .parts
                .insert(pid, Box::new(Part::new(Cur::fresh(5, 0), true)));
            super::unload::arm_idle_in(sh, g.id, pid, now);
        }
    }
    eprintln!("{per_shard} parts inserted and armed: {:?}", t.elapsed());
    let t = Instant::now();
    h.e.tick(now + IDLE / 2);
    eprintln!("a tick with nothing due: {:?}", t.elapsed());
    let (mut ticks, mut worst) = (0u32, std::time::Duration::ZERO);
    let t = Instant::now();
    while h.e.engine_stats().parts > 0 {
        let one = Instant::now();
        h.e.tick(now + IDLE + 1);
        worst = worst.max(one.elapsed());
        ticks += 1;
    }
    eprintln!(
        "dropped them all in {ticks} ticks, {:?} in all, the longest tick {worst:?}",
        t.elapsed()
    );
    assert_eq!(h.e.engine_stats().unloaded_total, per_shard as u64);
}
