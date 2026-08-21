//! RENDEZVOUS PLACEMENT — EPHEMERAL_QUEUES.md §3.7, the pure half.
//!
//! The hash is the whole of the multi-broker design: there is no lease, no
//! heartbeat write and no fencing protocol, so "every broker computes the same
//! answer from the same membership" is not an optimization to be verified later,
//! it is the correctness argument. These tests pin the three properties that
//! argument rests on — determinism, order-independence, and minimal disruption —
//! plus the short-circuit that keeps every single-broker cell on its old path.
//!
//! Nothing here needs a database, a mesh or an HTTP stack: `hrw_pick` is a pure
//! function of (key, node set), and that is the point.
//!
//! `#[path]` resolves relative to `src/`, and the module is a child of
//! `src/ephemeral.rs`, so `use super::*` reaches its private items
//! (`src/tests_unit/README.md`).

use super::*;

const T: &str = "11111111-1111-1111-1111-111111111111";

fn nodes(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| s.to_string()).collect()
}

/// A realistic key population: one queue per client inbox, one partition each —
/// the req/reply shape this class exists for (§1.1).
fn keys(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| rendezvous_key(T, &format!("inbox-{i}"), DEFAULT_PARTITION))
        .collect()
}

// ===========================================================================
// Determinism — the property every other one is built on
// ===========================================================================

/// "Every broker computes the same answer from the same membership" (§3.7).
/// Two brokers do not share an iteration order, so the answer must not depend on
/// one: the same set in a different order is the same set.
#[test]
fn the_pick_is_independent_of_the_node_list_order() {
    let a = nodes(&["queen-a", "queen-b", "queen-c"]);
    let b = nodes(&["queen-c", "queen-a", "queen-b"]);
    let c = nodes(&["queen-b", "queen-c", "queen-a"]);
    for k in keys(500) {
        let pa = hrw_pick(&k, &a).unwrap();
        assert_eq!(pa, hrw_pick(&k, &b).unwrap(), "key {k:?} moved with the order");
        assert_eq!(pa, hrw_pick(&k, &c).unwrap(), "key {k:?} moved with the order");
    }
}

/// The same key and the same set give the same answer on every call — there is
/// no RNG, no clock and no interior state anywhere in the decision.
#[test]
fn the_pick_is_stable_across_calls() {
    let ns = nodes(&["a", "b", "c", "d"]);
    let k = rendezvous_key(T, "orders", "Default");
    let first = hrw_pick(&k, &ns).unwrap().to_string();
    for _ in 0..100 {
        assert_eq!(hrw_pick(&k, &ns).unwrap(), first);
    }
}

/// A single candidate wins whatever the key — the case every single-broker cell
/// would take if it ever reached the hash at all.
#[test]
fn one_candidate_always_wins() {
    let solo = nodes(&["only"]);
    for k in keys(50) {
        assert_eq!(hrw_pick(&k, &solo), Some("only"));
    }
    assert_eq!(hrw_pick("anything", &[]), None);
}

/// The scores are keyed by BOTH halves: one node scores differently for
/// different keys, and one key scores differently for different nodes. Without
/// that the "hash" would be a constant and every key would land on one broker.
#[test]
fn the_score_depends_on_both_the_node_and_the_key() {
    assert_ne!(hrw_score("a", "k1"), hrw_score("a", "k2"));
    assert_ne!(hrw_score("a", "k1"), hrw_score("b", "k1"));
}

// ===========================================================================
// Minimal disruption — why HRW and not a modulo
// ===========================================================================

/// "Adding or removing ONE node moves only the keys that node wins or loses."
///
/// On this class a moved key is an EMPTIED RING (§1.2), so this test is
/// measuring data loss: with a modulo placement, one broker joining would
/// reshuffle nearly every key and wipe the cell. The bound asserted here is the
/// exact one the property promises — a key may only move TO the new node, never
/// between two nodes that were both there before and both stayed.
#[test]
fn adding_a_node_moves_only_keys_that_move_to_it() {
    let before = nodes(&["queen-a", "queen-b", "queen-c"]);
    let after = nodes(&["queen-a", "queen-b", "queen-c", "queen-d"]);
    let ks = keys(3000);
    let mut moved = 0usize;
    for k in &ks {
        let was = hrw_pick(k, &before).unwrap();
        let now = hrw_pick(k, &after).unwrap();
        if was != now {
            assert_eq!(
                now, "queen-d",
                "key {k:?} moved from {was} to {now}, and neither is the new node"
            );
            moved += 1;
        }
    }
    // ~1/4 of the keys should land on the fourth node. The window is wide on
    // purpose: this is a property test of the SHAPE, not a pin on one hash
    // function's exact output.
    let frac = moved as f64 / ks.len() as f64;
    assert!(
        (0.15..0.35).contains(&frac),
        "adding a fourth node moved {frac:.3} of the keys, not about a quarter"
    );
}

/// The other direction — a broker leaving (a crash, a rolling restart). Only the
/// keys it held may move, and every one of them must land somewhere that was
/// already in the ring.
#[test]
fn removing_a_node_moves_only_its_own_keys() {
    let before = nodes(&["queen-a", "queen-b", "queen-c"]);
    let after = nodes(&["queen-a", "queen-b"]);
    let ks = keys(3000);
    let mut moved = 0usize;
    for k in &ks {
        let was = hrw_pick(k, &before).unwrap();
        let now = hrw_pick(k, &after).unwrap();
        if was != now {
            assert_eq!(was, "queen-c", "key {k:?} moved but was not on the departed node");
            assert!(now == "queen-a" || now == "queen-b");
            moved += 1;
        }
    }
    let frac = moved as f64 / ks.len() as f64;
    assert!(
        (0.20..0.47).contains(&frac),
        "removing one of three nodes moved {frac:.3} of the keys, not about a third"
    );
}

/// Placement is spread, not clumped: "high queue cardinality spreads owners
/// evenly" (§1.5) is the sentence that makes single-owner placement acceptable —
/// if it were false, one broker would hold the whole cell's ephemeral traffic.
#[test]
fn keys_spread_across_the_nodes() {
    let ns = nodes(&["queen-a", "queen-b", "queen-c"]);
    let ks = keys(3000);
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for k in &ks {
        *counts.entry(hrw_pick(k, &ns).unwrap()).or_insert(0) += 1;
    }
    assert_eq!(counts.len(), 3, "some node got nothing at all");
    for (n, c) in counts {
        let frac = c as f64 / ks.len() as f64;
        assert!((0.25..0.42).contains(&frac), "{n} took {frac:.3} of the keys");
    }
}

/// The partition is part of the key, so the partitions of ONE queue are placed
/// independently — which is exactly the multi-owner queue the partition-less pop
/// rule of `route_queue` has to cope with.
#[test]
fn partitions_of_one_queue_are_placed_independently() {
    let ns = nodes(&["queen-a", "queen-b", "queen-c"]);
    let owners: std::collections::HashSet<String> = (0..30)
        .map(|i| {
            let k = rendezvous_key(T, "orders", &format!("p{i}"));
            hrw_pick(&k, &ns).unwrap().to_string()
        })
        .collect();
    assert!(
        owners.len() > 1,
        "thirty partitions of one queue all landed on one broker"
    );
}

/// Two tenants using the SAME queue name are two different keys, so they are
/// placed independently — the isolation rule of `handlers/mod.rs` reaching into
/// placement, and the reason the rendezvous key is built from `qkey`.
#[test]
fn the_key_carries_the_tenant_and_the_eph_namespace() {
    let k = rendezvous_key(T, "inbox", "Default");
    assert_eq!(k, format!("{T}\x1feph:inbox\x1fDefault"));
    let other = rendezvous_key("22222222-2222-2222-2222-222222222222", "inbox", "Default");
    assert_ne!(k, other);
    // …and a durable queue literally named `eph:inbox` would not collide with the
    // ephemeral one either, because the prefix is applied, never assumed.
    assert_ne!(rendezvous_key(T, "eph:inbox", "Default"), k);
}

// ===========================================================================
// The single-broker short-circuit (§3.7)
// ===========================================================================

/// "Single-broker (no peers ⇒ `mesh_active()` false) short-circuits: self is
/// always owner." With no transport attached the engine must answer `Local`
/// without hashing anything — this is the path every free-tier cell and the
/// embedded `queen::Broker` run, and it has to be the SAME instructions it was
/// before placement existed.
#[test]
fn with_no_mesh_every_partition_is_local() {
    let e = Ephemeral::new(Knobs::defaults(), Arc::new(Metrics::new()));
    assert_eq!(e.route(T, "inbox", "Default"), Route::Local);
    assert_eq!(e.route(T, "inbox", "some-other-partition"), Route::Local);
    assert_eq!(e.route_queue(T, "inbox"), Route::Local);
    // Whatever the name, and however many of them: there is no key that can
    // hash away from a broker that is alone.
    for i in 0..200 {
        assert_eq!(e.route(T, &format!("q{i}"), "Default"), Route::Local);
    }
    // …and the periodic reap has nothing to do, so it never walks the map.
    assert_eq!(e.reap_foreign(), 0);
}

/// The short-circuit is also a no-op for the admin broadcast: the three admin
/// verbs call it unconditionally, so with no mesh it has to be safe and silent.
#[test]
fn with_no_mesh_the_admin_broadcast_is_a_noop() {
    let e = Ephemeral::new(Knobs::defaults(), Arc::new(Metrics::new()));
    e.broadcast_admin("reset", T, "inbox");
    e.broadcast_admin("delete", T, "inbox");
    e.broadcast_admin("config_set", T, "inbox");
}

/// A wipe drops the ring, refunds every budget it charged and counts ONE move
/// (§3.7). Driven through the private `wipe_ring` because the public entry
/// (`route`) cannot reach it without a live mesh — the arithmetic is the part
/// that has to be right, and it is the same arithmetic either way.
#[test]
fn a_wipe_frees_the_ring_and_counts_one_move() {
    let m = Arc::new(Metrics::new());
    let e = Ephemeral::new(Knobs::defaults(), m.clone());
    let payloads: Vec<Box<[u8]>> = (0..5)
        .map(|i| format!("{{\"n\":{i}}}").into_bytes().into_boxed_slice())
        .collect();
    let bytes: i64 = payloads.iter().map(|p| p.len() as i64).sum();
    e.push(T, "inbox", "Default", payloads, 1_000).unwrap();
    assert_eq!(e.depth(T, "inbox"), Some((5, bytes)));
    assert_eq!(e.global_bytes(), bytes);

    assert!(e.wipe_ring(T, "inbox", "Default"));
    // Every budget back to zero — the queue's, the tenant's (via `release`) and
    // the cell's. A refund that reached two of the three would be an invisible
    // leak that only shows up as a cell that slowly stops accepting.
    assert_eq!(e.depth(T, "inbox"), Some((0, 0)));
    assert_eq!(e.global_bytes(), 0);
    assert_eq!(m.eph_wipes.load(Ordering::Relaxed), 1);
    // The partition is GONE, not emptied: a second wipe finds nothing and does
    // not count a second move.
    assert!(!e.wipe_ring(T, "inbox", "Default"));
    assert_eq!(m.eph_wipes.load(Ordering::Relaxed), 1);
    // A wipe is not a DROP: the three drop counters answer "is this cell losing
    // messages because it is under-sized", and an ownership move is a different
    // question with a different remedy.
    assert_eq!(m.eph_dropped_bounds.load(Ordering::Relaxed), 0);
    assert_eq!(m.eph_dropped_ttl.load(Ordering::Relaxed), 0);
    assert_eq!(m.eph_dropped_retry.load(Ordering::Relaxed), 0);
}

// ===========================================================================
// §10 Q4 — the ghost backstop
// ===========================================================================

/// "A lost frame leaves a ghost ring for at most one reconcile interval."
/// `drop_undeclared` is that interval's work: a declared queue whose row is gone
/// is forgotten, and an IMPLICIT one — which never had a row (§1.1) — is not
/// touched, because dropping those would delete every live req/reply inbox on
/// the cell twice a minute.
#[test]
fn the_refresh_forgets_declared_queues_whose_row_is_gone() {
    let e = Ephemeral::new(Knobs::defaults(), Arc::new(Metrics::new()));
    e.set_config(T, "declared-kept", QueueOptions::default(), true);
    e.set_config(T, "declared-gone", QueueOptions::default(), true);
    // An implicit queue, born of a push exactly as a real one would be.
    e.push(T, "implicit", "Default", vec![b"1".to_vec().into_boxed_slice()], 1)
        .unwrap();
    assert_eq!(e.list(T).len(), 3);

    let present: std::collections::HashSet<String> =
        ["declared-kept".to_string()].into_iter().collect();
    assert_eq!(e.drop_undeclared(T, &present), 1);

    let names: Vec<String> = e.list(T).into_iter().map(|q| q.name).collect();
    assert_eq!(names, vec!["declared-kept".to_string(), "implicit".to_string()]);
    // Idempotent: a second pass with the same row set drops nothing more.
    assert_eq!(e.drop_undeclared(T, &present), 0);
}

/// One tenant's refresh never reaches another's queues — the isolation rule
/// applied to the backstop, where getting it wrong would silently delete a
/// neighbour's declared queues on every refresh.
#[test]
fn the_ghost_backstop_is_tenant_scoped() {
    let other = "22222222-2222-2222-2222-222222222222";
    let e = Ephemeral::new(Knobs::defaults(), Arc::new(Metrics::new()));
    e.set_config(T, "shared-name", QueueOptions::default(), true);
    e.set_config(other, "shared-name", QueueOptions::default(), true);
    // T has no rows at all any more; `other`'s row set is not consulted.
    assert_eq!(e.drop_undeclared(T, &std::collections::HashSet::new()), 1);
    assert!(e.list(T).is_empty());
    assert_eq!(e.list(other).len(), 1);
}
