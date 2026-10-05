//! The idle-part unloader ([`super::unload`]): what goes, what stays, and that
//! a part loaded back answers as the one dropped would have.
#![allow(clippy::needless_range_loop)]

use crate::rsm::batcher::{Command, Reply};
use crate::rsm::entry::{Outcome, PopClaim};
use crate::rsm::planner::AckStatus;

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
        differential(seed, 1500);
    }
}

fn differential(seed: u64, steps: usize) {
    let mut rng = Rng(seed);
    let mut keep = H::with(
        "unload-diff-keep",
        Knobs {
            idle_unload_us: 0,
            ..knobs()
        },
    );
    let mut drop = H::with("unload-diff-drop", unloading());
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
            // The unloader, on one engine only.
            90..=97 => {
                drop.e.unload_now(drop.now);
            }
            // A new leader on both.
            98 => {
                let s = drop.e.engine_stats();
                (unloaded, loaded) = (unloaded + s.unloaded_total, loaded + s.loaded_total);
                keep.failover_with(Knobs {
                    idle_unload_us: 0,
                    ..knobs()
                });
                drop.failover_with(unloading());
            }
            _ => {}
        }
    }
    let s = drop.e.engine_stats();
    (unloaded, loaded) = (unloaded + s.unloaded_total, loaded + s.loaded_total);
    // The history did what it is for: parts dropped, and loaded back.
    eprintln!("differential {seed:#x}: {steps} steps, {unloaded} parts unloaded, {loaded} loaded");
    assert!(unloaded > 50, "{seed:#x}: only {unloaded} parts unloaded");
}

/// What one unloader pass costs while it holds a shard (run by hand, release:
/// `cargo test --release --lib consume::tests_unload::sweep_cost -- --ignored --nocapture`).
/// One shard of a 1M-part engine (62.5k parts): the pass that starts their
/// idle clocks, the pass that keeps them (not idle yet), the pass that drops
/// them all.
#[test]
#[ignore]
fn sweep_cost_at_one_million_parts() {
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
    {
        let mut sh = lock(&h.e.shards[0]);
        let gs = sh
            .groups
            .entry(g.id)
            .or_insert_with(|| GroupShard::new(g.clone()));
        for i in 0..per_shard as u64 {
            gs.parts.insert(i * 16, Part::new(Cur::fresh(5, 0), true));
        }
        for i in 0..per_shard as u64 {
            sh.pids.insert(
                i * 16,
                PidInfo {
                    tail: 5,
                    log_start: 0,
                    txns_start: 0,
                    last_append_us: 0,
                    watchers: smallvec::smallvec![g.id],
                },
            );
        }
    }
    let now = h.now;
    for (label, at) in [
        ("first look (starts the clocks)", now),
        ("not idle yet (keeps all)", now + IDLE / 2),
        ("idle (drops all)", now + IDLE + 1),
    ] {
        let t = Instant::now();
        let n = h.e.unload_shards_for_test(&[0], at);
        eprintln!(
            "{per_shard} parts, {label}: {:?} ({n} dropped)",
            t.elapsed()
        );
    }
    assert_eq!(h.e.engine_stats().parts, 0);
}
