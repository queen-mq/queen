//! The ephemeral engine's pure logic — EPHEMERAL_QUEUES.md §7.1, the half that
//! needs no PostgreSQL and no HTTP stack.
//!
//! Every test here pins one sentence of the plan, and the sentence is quoted in
//! the doc comment so a later reader can tell an intentional rule from an
//! accident. What is deliberately NOT here is anything that needs a broker
//! process (route shapes, long-poll wake timing, cross-tenant HTTP isolation) —
//! that is `server/tests/ephemeral_semantics.rs`, a later phase.
//!
//! `#[path]` on a non-inline `mod` resolves relative to `src/`, and the module
//! is a child of `src/ephemeral.rs`, so `use super::*` reaches its private
//! items — the mechanism `src/tests_unit/README.md` documents.

use super::*;

const T: &str = "11111111-1111-1111-1111-111111111111";
const OTHER: &str = "22222222-2222-2222-2222-222222222222";

fn engine(k: Knobs) -> Arc<Ephemeral> {
    Ephemeral::new(k, Arc::new(Metrics::new()))
}

fn small() -> Knobs {
    Knobs { implicit_idle_ms: 1_000, ..Knobs::defaults() }
}

fn body(s: &str) -> Box<[u8]> {
    s.as_bytes().to_vec().into_boxed_slice()
}

fn bodies(n: usize) -> Vec<Box<[u8]>> {
    (0..n).map(|i| body(&format!("{{\"n\":{i}}}"))).collect()
}

fn payload_of(d: &Delivered) -> String {
    String::from_utf8(d.payload.to_vec()).unwrap()
}

// ===========================================================================
// §3.1 — message ids
// ===========================================================================

/// "Message id: `e:<owner_epoch_hex>:<partition>:<seq>` — opaque to clients,
/// self-describing to the broker."
#[test]
fn id_round_trips() {
    let id = mint_id(0xdead_beef, "Default", 42);
    assert_eq!(id, "e:deadbeef:Default:42");
    assert_eq!(parse_id(&id), Some((0xdead_beef, "Default", 42)));
}

/// A partition name may contain `:` (the durable engine forbids only control
/// characters), so the parse has to come from BOTH ends. A left-to-right split
/// would truncate `orders:eu` and attribute the ack to the wrong partition —
/// silently, because the truncated name is still a legal name.
#[test]
fn id_parses_a_partition_containing_colons() {
    let id = mint_id(1, "orders:eu:west", 7);
    assert_eq!(parse_id(&id), Some((1, "orders:eu:west", 7)));
}

#[test]
fn id_rejects_foreign_shapes() {
    // Not ours at all (a durable uuid), missing fields, non-hex epoch,
    // non-numeric seq, and an empty partition.
    for bad in [
        "0198b3f0-0000-7000-8000-000000000000",
        "e:1:2",
        "e:zz:p:1",
        "e:1:p:x",
        "e:1::1",
        "",
    ] {
        assert_eq!(parse_id(bad), None, "{bad} must not parse");
    }
}

// ===========================================================================
// §1.6 — budget arithmetic, charge before append with refund on failure
// ===========================================================================

/// "charge-before-append with refund on failure (the `quota.rs` discipline)".
/// A refused charge must leave the counter EXACTLY where it was: a charge that
/// half-applies is how a cell slowly stops accepting with no visible occupancy.
#[test]
fn budget_charges_refuses_and_refunds_exactly() {
    let b = Budget::new(100);
    assert!(b.charge(60));
    assert_eq!(b.used(), 60);
    assert!(!b.charge(50), "60 + 50 crosses the cap of 100");
    assert_eq!(b.used(), 60, "a refused charge changes nothing");
    assert!(b.charge(40), "the boundary itself is admissible");
    assert_eq!(b.used(), 100);
    b.refund(100);
    assert_eq!(b.used(), 0);
}

/// `cap <= 0` is UNLIMITED, and every rung whose limit has not landed carries
/// it — so an unwired rung is inert rather than a denial. Getting this backwards
/// would make every OSS cell refuse its first push against a grant table nobody
/// writes.
#[test]
fn budget_cap_zero_is_unlimited() {
    let b = Budget::new(0);
    assert!(b.charge(i64::MAX / 4));
    assert!(b.charge(i64::MAX / 4));
}

/// A double refund must clamp at zero. A NEGATIVE occupancy is worse than a
/// slightly high one: it grants unlimited room to everyone until the counter
/// climbs back, which is unbounded RAM on a bug that is otherwise cosmetic.
#[test]
fn budget_refund_saturates_at_zero() {
    let b = Budget::new(100);
    assert!(b.charge(10));
    b.refund(10);
    b.refund(10);
    assert_eq!(b.used(), 0);
    assert!(b.charge(100), "the cap is still honoured after the clamp");
}

/// The cell budget is charged by the ENGINE, not only by the leaf counter: a
/// push that lands moves `global_bytes`, and one that is refused leaves it
/// untouched (the refund path of §1.6).
#[test]
fn a_refused_push_refunds_every_rung() {
    let e = engine(Knobs { global_max_bytes: 40, ..small() });
    assert!(e.push(T, "q", "Default", vec![body("0123456789")], 0).is_ok());
    let after_one = e.global_bytes();
    assert_eq!(after_one, 10);
    // 5 x 7 bytes = 35, against a 40-byte cell that already holds 10.
    let r = e.push(T, "q", "Default", bodies(5), 0);
    assert_eq!(r, Err(Refusal::NoRoom));
    assert_eq!(e.global_bytes(), after_one, "the cell budget is back where it was");
    assert_eq!(e.depth(T, "q"), Some((1, 10)), "and so is the queue's");
}

// ===========================================================================
// §1.4 / §1.5 — FIFO, group cursors, fan-out vs competing
// ===========================================================================

/// "FIFO per (queue, partition) within one ownership incarnation."
#[test]
fn ring_is_fifo() {
    let e = engine(small());
    e.push(T, "q", "Default", bodies(3), 0).unwrap();
    let got = e.pop(T, "q", None, Some("g"), 10, true, 0);
    let seen: Vec<String> = got.iter().map(payload_of).collect();
    assert_eq!(seen, vec!["{\"n\":0}", "{\"n\":1}", "{\"n\":2}"]);
}

/// "Fan-out = each subscriber pops with its OWN group; every group has its own
/// cursor over the one ring and receives everything."
#[test]
fn every_group_sees_everything() {
    let e = engine(small());
    // The subscribers park FIRST, which is the shape of every fan-out
    // workload this class targets (presence, typing, cache invalidation).
    for g in ["a", "b", "c"] {
        assert!(e.pop(T, "q", None, Some(g), 10, true, 0).is_empty());
    }
    e.push(T, "q", "Default", bodies(2), 0).unwrap();
    for g in ["a", "b", "c"] {
        let got = e.pop(T, "q", None, Some(g), 10, true, 0);
        assert_eq!(got.len(), 2, "group {g} must see the whole ring");
    }
    // ONE copy in RAM however many groups read it: three groups that each read
    // two messages must not cost six messages' worth of bytes.
    assert_eq!(e.depth(T, "q"), Some((0, 0)), "reclaimed once every cursor passed");
}

/// "Competing consumers = the same `group`" — two pops of one group take
/// DISJOINT messages, which is what makes a work queue a work queue.
#[test]
fn one_group_competes() {
    let e = engine(small());
    e.push(T, "q", "Default", bodies(4), 0).unwrap();
    let a = e.pop(T, "q", None, Some("w"), 2, true, 0);
    let b = e.pop(T, "q", None, Some("w"), 2, true, 0);
    assert_eq!(a.len(), 2);
    assert_eq!(b.len(), 2);
    let mut all: Vec<String> = a.iter().chain(b.iter()).map(payload_of).collect();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 4, "no message may be handed to both pops");
}

/// "or no group at all — the group-less queue-mode, mirroring the durable
/// `__QUEUE_MODE__` sentinel". Observably: two group-less pops compete, and a
/// pop that names the sentinel explicitly lands on the same cursor.
#[test]
fn groupless_pops_share_one_cursor_like_queue_mode() {
    let e = engine(small());
    e.push(T, "q", "Default", bodies(2), 0).unwrap();
    let a = e.pop(T, "q", None, None, 1, true, 0);
    let b = e.pop(T, "q", None, Some(QUEUE_MODE), 1, true, 0);
    assert_eq!(payload_of(&a[0]), "{\"n\":0}");
    assert_eq!(payload_of(&b[0]), "{\"n\":1}", "the sentinel IS the group-less cursor");
    assert!(e.pop(T, "q", None, None, 10, true, 0).is_empty());
}

/// A brand-new group starts at the ring's HEAD, not at its tail: the messages
/// still in RAM are the only history that exists (§1.2 — no replay, no
/// subscriptionMode), so a subscriber that arrives while a push is still
/// buffered gets what is buffered for it.
#[test]
fn a_new_group_starts_at_what_is_still_in_ram() {
    let e = engine(small());
    e.push(T, "q", "Default", bodies(3), 0).unwrap();
    let late = e.pop(T, "q", None, Some("late"), 10, true, 0);
    assert_eq!(late.len(), 3);
}

/// …and the corollary, which is the rule a fan-out user has to know: a group
/// that has NEVER popped is owed nothing, so a message every existing cursor
/// has passed is reclaimed and a group arriving afterwards does not see it.
/// There is no subscriptionMode here to ask for otherwise, and retaining "for
/// a subscriber who might turn up" has no bound anyone could name.
#[test]
fn a_group_that_arrives_after_the_reclaim_sees_nothing() {
    let e = engine(small());
    e.push(T, "q", "Default", bodies(3), 0).unwrap();
    assert_eq!(e.pop(T, "q", None, Some("early"), 10, true, 0).len(), 3);
    assert!(e.pop(T, "q", None, Some("late"), 10, true, 0).is_empty());
}

/// THE PARKING RACE, which is why `known_groups` exists. Two subscribers park
/// on an inbox that has no partition yet; the push creates the ring and wakes
/// both; the faster one takes the message. Without name pre-seeding the slower
/// one would register its cursor only AFTER the reclaim and miss a message for
/// being microseconds late — a fan-out bug with no symptom but a gap.
#[test]
fn a_parked_group_is_seeded_into_a_ring_created_after_it() {
    let e = engine(small());
    // Both park before the queue has any partition at all.
    assert!(e.pop(T, "q", None, Some("fast"), 10, true, 0).is_empty());
    assert!(e.pop(T, "q", None, Some("slow"), 10, true, 0).is_empty());
    e.push(T, "q", "Default", bodies(1), 0).unwrap();
    assert_eq!(e.pop(T, "q", None, Some("fast"), 10, true, 0).len(), 1);
    assert_eq!(
        e.pop(T, "q", None, Some("slow"), 10, true, 0).len(),
        1,
        "the slower subscriber must still get it"
    );
}

/// Every engine map key transits `tenant_queue_key` (§7.2), so two tenants may
/// hold the same queue NAME and never see each other's messages.
#[test]
fn two_tenants_may_share_a_queue_name() {
    let e = engine(small());
    e.push(T, "orders", "Default", vec![body("\"mine\"")], 0).unwrap();
    assert!(e.pop(OTHER, "orders", None, None, 10, true, 0).is_empty());
    assert_eq!(e.pop(T, "orders", None, None, 10, true, 0).len(), 1);
}

/// The queue half of the key carries the `eph:` prefix, never the tenant half
/// (M5): the mesh splits the composite on the FIRST `\x1f` and a prefix in the
/// tenant half would not survive the round trip.
#[test]
fn qkey_namespaces_the_queue_half() {
    let k = Ephemeral::qkey(T, "orders");
    assert_eq!(k, format!("{T}\u{1f}eph:orders"));
    let (tenant, queue) = crate::handlers::split_tenant_queue(&k);
    assert_eq!(tenant, T);
    assert_eq!(queue, "eph:orders");
}

// ===========================================================================
// §1.3 — leases, redelivery, attempts, retry exhaustion
// ===========================================================================

/// "undelivered acks redeliver on lease expiry (`leaseSeconds`, default 30) …
/// `failed`/`retry` redeliver with `attempts+1`".
#[test]
fn an_expired_lease_redelivers_with_attempts_incremented() {
    let e = engine(Knobs { lease_ms: 1_000, ..small() });
    e.push(T, "q", "Default", bodies(1), 0).unwrap();
    let first = e.pop(T, "q", None, Some("g"), 10, false, 0);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].attempts, 1);
    // Before the deadline the message is nobody else's.
    assert!(e.pop(T, "q", None, Some("g"), 10, false, 500).is_empty());
    // After it, the same message comes back with the count advanced.
    let again = e.pop(T, "q", None, Some("g"), 10, false, 1_001);
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].attempts, 2);
    assert_eq!(again[0].id, first[0].id, "redelivery keeps the id");
}

/// "`autoAck=true` → at-most-once. Cursor advances at delivery; no lease
/// bookkeeping at all." A message delivered with autoAck never comes back, so a
/// consumer that dies holding it has lost it — which is the contract.
#[test]
fn auto_ack_leaves_no_lease() {
    let e = engine(Knobs { lease_ms: 1_000, ..small() });
    e.push(T, "q", "Default", bodies(1), 0).unwrap();
    assert_eq!(e.pop(T, "q", None, Some("g"), 10, true, 0).len(), 1);
    assert!(e.pop(T, "q", None, Some("g"), 10, true, 10_000).is_empty());
    assert_eq!(e.depth(T, "q"), Some((0, 0)), "and the byte is reclaimed at once");
}

/// A `completed` ack retires the message; a `failed` one puts it back with the
/// attempt spent — and the answer names which of the two happened.
#[test]
fn ack_outcomes_are_acked_and_redelivered() {
    let e = engine(small());
    e.push(T, "q", "Default", bodies(2), 0).unwrap();
    let got = e.pop(T, "q", None, Some("g"), 2, false, 0);
    let r = e.ack(
        T,
        "q",
        Some("g"),
        &[
            (got[0].id.clone(), AckStatus::Completed),
            (got[1].id.clone(), AckStatus::Failed),
        ],
        0,
    );
    assert_eq!(r[0].1, AckOutcome::Acked);
    assert_eq!(r[1].1, AckOutcome::Redelivered);
    // The redelivered one comes back IMMEDIATELY (no lease wait) with attempts 2.
    let back = e.pop(T, "q", None, Some("g"), 10, false, 0);
    assert_eq!(back.len(), 1);
    assert_eq!(back[0].attempts, 2);
    assert_eq!(back[0].id, got[1].id);
}

/// "attempts exhausted at `retryLimit` (default 5) → dropped and counted".
/// The closed outcome vocabulary has ONE terminal answer, so the exhausting ack
/// answers `acked` and `eph_dropped_retry` is where the drop is visible.
#[test]
fn retry_limit_exhaustion_drops_and_counts() {
    let e = engine(Knobs { retry_limit: 2, ..small() });
    e.push(T, "q", "Default", bodies(1), 0).unwrap();
    // attempt 1 -> nack, attempt 2 -> nack (now at the limit), attempt 3 -> gone
    let a = e.pop(T, "q", None, Some("g"), 1, false, 0);
    assert_eq!(a[0].attempts, 1);
    assert_eq!(
        e.ack(T, "q", Some("g"), &[(a[0].id.clone(), AckStatus::Failed)], 0)[0].1,
        AckOutcome::Redelivered
    );
    let b = e.pop(T, "q", None, Some("g"), 1, false, 0);
    assert_eq!(b[0].attempts, 2);
    let last = e.ack(T, "q", Some("g"), &[(b[0].id.clone(), AckStatus::Failed)], 0);
    assert_eq!(last[0].1, AckOutcome::Acked, "terminal: the message is retired");
    assert!(e.pop(T, "q", None, Some("g"), 10, false, 0).is_empty());
    assert_eq!(e.metrics.eph_dropped_retry.load(Ordering::Relaxed), 1);
}

/// Exhaustion is PER GROUP: on a fan-out ring one group giving up must not take
/// the message away from another group that is still owed it.
#[test]
fn exhaustion_is_per_group() {
    let e = engine(Knobs { retry_limit: 1, ..small() });
    // Both groups are subscribers of this fan-out queue before the push.
    assert!(e.pop(T, "q", None, Some("a"), 1, false, 0).is_empty());
    assert!(e.pop(T, "q", None, Some("b"), 1, false, 0).is_empty());
    e.push(T, "q", "Default", bodies(1), 0).unwrap();
    let a = e.pop(T, "q", None, Some("a"), 1, false, 0);
    e.ack(T, "q", Some("a"), &[(a[0].id.clone(), AckStatus::Failed)], 0);
    assert!(e.pop(T, "q", None, Some("a"), 10, false, 0).is_empty(), "a gave up");
    assert_eq!(e.pop(T, "q", None, Some("b"), 10, false, 0).len(), 1, "b is still owed it");
}

/// "An ack whose epoch is not the current owner's returns `stale`, never an
/// error — the fencing that survives restarts and ownership moves."
#[test]
fn a_foreign_epoch_answers_stale_and_ours_without_a_lease_answers_unknown() {
    let e = engine(small());
    e.push(T, "q", "Default", bodies(1), 0).unwrap();
    let got = e.pop(T, "q", None, Some("g"), 1, false, 0);
    // Same shape, another incarnation.
    let foreign = mint_id(e.epoch() ^ 0xffff_ffff, "Default", 0);
    let r = e.ack(
        T,
        "q",
        Some("g"),
        &[
            (foreign, AckStatus::Completed),
            (got[0].id.clone(), AckStatus::Completed),
            (got[0].id.clone(), AckStatus::Completed),
            ("not-an-eph-id".to_string(), AckStatus::Completed),
        ],
        0,
    );
    assert_eq!(r[0].1, AckOutcome::Stale);
    assert_eq!(r[1].1, AckOutcome::Acked);
    assert_eq!(r[2].1, AckOutcome::Unknown, "the lease was already released");
    assert_eq!(r[3].1, AckOutcome::Unknown);
}

/// An ack that arrives for a queue that no longer exists still distinguishes
/// "your broker restarted" from "you are too late" — which is the whole reason
/// the epoch is in the id.
#[test]
fn acking_an_unknown_queue_still_reports_the_epoch_verdict() {
    let e = engine(small());
    let mine = mint_id(e.epoch(), "Default", 0);
    let theirs = mint_id(e.epoch().wrapping_add(1), "Default", 0);
    let r = e.ack(
        T,
        "gone",
        None,
        &[(mine, AckStatus::Completed), (theirs, AckStatus::Completed)],
        0,
    );
    assert_eq!(r[0].1, AckOutcome::Unknown);
    assert_eq!(r[1].1, AckOutcome::Stale);
}

// ===========================================================================
// §1.6 — bounds, both policies, and the cursor skip
// ===========================================================================

/// "policy `reject` (429 `queue_full` …)". A full queue under `reject` refuses
/// the arrival and keeps what it has — the opposite of `dropOldest`, and the
/// distinction is the whole knob.
#[test]
fn policy_reject_refuses_and_keeps_the_backlog() {
    let e = engine(small());
    e.set_config(T, "q", QueueOptions { max_length: Some(2), ..Default::default() }, true);
    e.push(T, "q", "Default", bodies(2), 0).unwrap();
    assert_eq!(e.push(T, "q", "Default", bodies(1), 0), Err(Refusal::QueueFull));
    let got = e.pop(T, "q", None, None, 10, true, 0);
    assert_eq!(got.len(), 2);
    assert_eq!(payload_of(&got[0]), "{\"n\":0}", "the OLDEST survived");
}

/// "or `dropOldest` (feed semantics; `eph_dropped_bounds` counted)". The same
/// overflow keeps the NEWEST and counts what it threw away.
#[test]
fn policy_drop_oldest_evicts_the_head_and_counts_it() {
    let e = engine(small());
    e.set_config(
        T,
        "q",
        QueueOptions {
            max_length: Some(2),
            policy: Some(Policy::DropOldest),
            ..Default::default()
        },
        true,
    );
    e.push(T, "q", "Default", bodies(2), 0).unwrap();
    e.push(T, "q", "Default", vec![body("\"newest\"")], 0).unwrap();
    let got = e.pop(T, "q", None, None, 10, true, 0);
    assert_eq!(got.len(), 2);
    assert_eq!(payload_of(&got[0]), "{\"n\":1}", "n:0 was dropped from the head");
    assert_eq!(payload_of(&got[1]), "\"newest\"");
    assert_eq!(e.metrics.eph_dropped_bounds.load(Ordering::Relaxed), 1);
}

/// "a group parked below an evicted range skips forward and the skip is counted
/// per group" (§3.2). For a fan-out consumer this is the difference between
/// "slow" and "lost data", so it must never be silent.
#[test]
fn an_evicted_range_advances_the_cursor_and_counts_the_skip() {
    let e = engine(small());
    e.set_config(
        T,
        "q",
        QueueOptions {
            max_length: Some(2),
            policy: Some(Policy::DropOldest),
            ..Default::default()
        },
        true,
    );
    e.push(T, "q", "Default", bodies(2), 0).unwrap();
    // Register the group at the head WITHOUT consuming, then overflow under it.
    assert_eq!(e.pop(T, "q", None, Some("slow"), 0, true, 0).len(), 0);
    e.pop(T, "q", Some("Default"), Some("slow"), 1, true, 0);
    e.push(T, "q", "Default", bodies(2), 0).unwrap();
    assert!(e.skipped(T, "q", "Default", "slow") >= 1, "the skip must be counted");
}

// ===========================================================================
// §1.7 — ttl and windowBuffer
// ===========================================================================

/// "`ttlSeconds`: head-drop of messages older than the limit, enforced lazily on
/// push/pop plus the sweeper backstop; `eph_dropped_ttl` counted."
#[test]
fn ttl_head_drops_on_the_next_touch() {
    let e = engine(small());
    e.set_config(T, "q", QueueOptions { ttl_ms: Some(1_000), ..Default::default() }, true);
    e.push(T, "q", "Default", bodies(2), 0).unwrap();
    // Still young.
    assert_eq!(e.pop(T, "q", None, Some("g"), 10, true, 500).len(), 2);
    e.push(T, "q", "Default", bodies(2), 2_000).unwrap();
    // The two pushed at t=2000 are young; nothing from t=0 survives, and the
    // ones already consumed were reclaimed rather than counted as ttl drops.
    let got = e.pop(T, "q", None, Some("g2"), 10, true, 2_500);
    assert_eq!(got.len(), 2);
    e.push(T, "q", "Default", bodies(1), 0).unwrap();
    let dropped_before = e.metrics.eph_dropped_ttl.load(Ordering::Relaxed);
    e.pop(T, "q", None, Some("g3"), 10, true, 10_000);
    assert!(
        e.metrics.eph_dropped_ttl.load(Ordering::Relaxed) > dropped_before,
        "an unconsumed message past its ttl must be dropped AND counted"
    );
}

/// The ttl drop reclaims the bytes it drops — an age limit that freed nothing
/// would bound the queue's length and not its memory, which is the resource.
#[test]
fn ttl_returns_the_bytes_to_every_budget() {
    let e = engine(small());
    e.set_config(T, "q", QueueOptions { ttl_ms: Some(1_000), ..Default::default() }, true);
    e.push(T, "q", "Default", bodies(4), 0).unwrap();
    assert!(e.global_bytes() > 0);
    // Any touch past the deadline runs the drop.
    e.pop(T, "q", Some("Default"), Some("g"), 1, true, 5_000);
    assert_eq!(e.global_bytes(), 0);
    assert_eq!(e.depth(T, "q"), Some((0, 0)));
}

/// "`windowBuffer {ms, count}`: a waiting pop returns when `count` messages are
/// ready or `ms` elapsed since the first, bounded by the pop's own `timeout`."
#[test]
fn window_buffer_decision() {
    let w = Window { ms: 50, count: 4 };
    // Nothing in hand keeps waiting — an empty early return would turn the long
    // poll back into a poll loop.
    assert!(!window_ready(0, 10, 1_000, w));
    // Under both thresholds: keep fattening.
    assert!(!window_ready(2, 10, 10, w));
    // Either threshold alone releases it.
    assert!(window_ready(4, 10, 0, w));
    assert!(window_ready(1, 10, 50, w));
    // A FULL batch always returns: the window fattens a batch, never caps one.
    assert!(window_ready(10, 10, 0, w));
    // Window off ⇒ the first message returns immediately (durable behaviour).
    assert!(window_ready(1, 10, 0, Window::OFF));
    // A count-only window ignores time, and a time-only window ignores count.
    assert!(!window_ready(1, 10, 10_000, Window { ms: 0, count: 3 }));
    assert!(window_ready(1, 10, 10_000, Window { ms: 5, count: 0 }));
}

// ===========================================================================
// §1.1 / §3.2 — implicit queues and the backstop
// ===========================================================================

/// "Implicit: created by the first `push`/`pop` that names it … garbage-
/// collected after `QUEEN_EPHEMERAL_IMPLICIT_IDLE_S` of being empty and
/// unpolled." A DECLARED queue is never collected: its configuration is durable
/// and its emptiness is normal.
#[test]
fn implicit_queues_are_collected_and_declared_ones_are_not() {
    let e = engine(small()); // implicit_idle_ms = 1000
    e.push(T, "implicit", "Default", bodies(1), 0).unwrap();
    e.pop(T, "implicit", None, None, 10, true, 0);
    e.set_config(T, "declared", QueueOptions::default(), true);
    assert_eq!(e.queue_count(), 2);

    // Too soon: the queue is empty but was touched at t=0.
    let s = e.sweep(500);
    assert_eq!(s.gc_queues, 0);
    // Past the idle window.
    let s = e.sweep(5_000);
    assert_eq!(s.gc_queues, 1);
    assert_eq!(e.queue_count(), 1);
    assert!(e.depth(T, "declared").is_some(), "the declared config survives");
}

/// THE ABA HAZARD the seq base exists to close. An implicit queue is collected
/// while the process lives, so the epoch alone does not fence its ids: a client
/// flushing an ack it buffered before the collection would name `…:Default:0`,
/// which a ring restarting at 0 has just handed to somebody else — and a
/// `completed` would retire a message nobody processed. Silent loss, not the
/// duplication-or-loss §1.4 declares legal.
#[test]
fn a_recreated_ring_never_reuses_the_ids_of_the_one_it_replaced() {
    let e = engine(small());
    e.push(T, "inbox", "Default", bodies(1), 0).unwrap();
    let old = e.pop(T, "inbox", None, Some("g"), 1, true, 0);
    let old_id = old[0].id.clone();
    assert_eq!(e.sweep(5_000).gc_queues, 1, "the emptied inbox is collected");

    e.push(T, "inbox", "Default", bodies(1), 6_000).unwrap();
    let new = e.pop(T, "inbox", None, Some("g"), 1, false, 6_000);
    assert_ne!(new[0].id, old_id, "the recreated ring must mint a fresh id");
    // …and the stale ack lands on nothing rather than on the new message.
    let r = e.ack(T, "inbox", Some("g"), &[(old_id, AckStatus::Completed)], 6_000);
    assert_eq!(r[0].1, AckOutcome::Unknown);
    assert_eq!(
        e.pop(T, "inbox", None, Some("g"), 1, false, 40_000).len(),
        1,
        "the new message is still outstanding, not retired by the stale ack"
    );
}

/// A queue that still HOLDS something is never collected, however idle — the
/// contents are the reason it exists, and dropping them here would be a loss
/// that no bound and no policy asked for.
#[test]
fn a_non_empty_implicit_queue_survives_the_gc() {
    let e = engine(small());
    e.push(T, "q", "Default", bodies(1), 0).unwrap();
    assert_eq!(e.sweep(10_000).gc_queues, 0);
    assert_eq!(e.queue_count(), 1);
}

/// The backstop does on an UNTOUCHED ring what every touch does on a busy one:
/// expire the lease and hand the message back.
#[test]
fn the_backstop_expires_leases_nobody_touched() {
    let e = engine(Knobs { lease_ms: 1_000, ..small() });
    e.push(T, "q", "Default", bodies(1), 0).unwrap();
    e.pop(T, "q", None, Some("g"), 1, false, 0);
    let s = e.sweep(2_000);
    assert_eq!(s.redelivered, 1);
    assert_eq!(e.pop(T, "q", None, Some("g"), 10, false, 2_000).len(), 1);
}

/// The sweep reports the nearest lease deadline so the sweeper can ring
/// `hint_in_ms` for it instead of waiting out its own cadence (§3.2).
#[test]
fn the_backstop_reports_the_next_expiry() {
    let e = engine(Knobs { lease_ms: 5_000, ..small() });
    e.push(T, "q", "Default", bodies(1), 0).unwrap();
    e.pop(T, "q", None, Some("g"), 1, false, 1_000);
    assert_eq!(e.sweep(1_100).next_expiry_ms, Some(6_000));
}

// ===========================================================================
// Config clamping
// ===========================================================================

/// `configure` may not raise a queue above the broker's own knob: an operator
/// who lowers `QUEEN_EPHEMERAL_QUEUE_MAX_BYTES` under queues declared when it
/// was higher must actually get the lower number. This is why 030_ephemeral.sql
/// validates nothing — the clamp has to live here or the two authorities drift.
#[test]
fn options_are_clamped_against_the_knobs() {
    let k = Knobs { queue_max_bytes: 1_000, queue_max_length: 10, ..Knobs::defaults() };
    let c = QueueOptions {
        max_bytes: Some(i64::MAX),
        max_length: Some(1_000_000),
        ttl_ms: Some(-5),
        ..Default::default()
    }
    .apply(QueueConfig::from_knobs(&k), &k);
    assert_eq!(c.max_bytes, 1_000);
    assert_eq!(c.max_length, 10);
    assert_eq!(c.ttl_ms, 0, "a negative ttl is no ttl, never a drop-everything");
}
