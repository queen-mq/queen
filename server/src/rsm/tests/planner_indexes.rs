//! The planner's indexes over the state it keeps, each against what it stands
//! for: whatever an index answers, a scan of the underlying state answers the
//! same.
//!
//! - B13, the overlay's append index: a wildcard pop's in-flight candidates
//!   are the queue's own appended pids, where it used to read the partition
//!   row of every appended pid of every queue. Checked against that scan, over
//!   placed (hinted by the push), unplaced (folded from an entry and read
//!   once) and created-in-flight pids, with deletes in flight.

use std::sync::Arc;

use super::planner_harness::{self as h, Cell, TENANT};
use crate::rsm::dedup::DedupFront;
use crate::rsm::effect::{Effect, GarbageScope, Pid};
use crate::rsm::entry::Entry;
use crate::rsm::planner::kept::KeptOverlay;
use crate::rsm::planner::{Overlay, Plan, PlanConfig, Planner};
use crate::rsm::state::{Committed, Derived};
use crate::rsm::store::{Store, TypedReads};

fn push_cmd(
    id: u64,
    queue: &str,
    partition: &str,
    txns: &[&str],
) -> crate::rsm::planner::PushCommand {
    match h::push(id, queue, partition, txns) {
        h::Cmd::Push(c) => c,
        _ => unreachable!(),
    }
}

/// Every queue's in-flight candidates, by the index and by the scan: the
/// index offers the scan's pids in the same order, plus only pids whose
/// partition the claim would find gone (`extra_ok`).
fn assert_index_matches_scan<R: crate::rsm::store::Reads + ?Sized>(
    planner: &Planner<'_, R>,
    ov: &mut Overlay,
    queues: &[&str],
    extra_ok: &[Pid],
) {
    planner.place_unplaced(ov).expect("place");
    assert!(ov.appended_placed(), "every appended pid placed or unknown");
    assert_eq!(ov.append_index_diff(), None);
    for q in queues {
        let by_index: Vec<Pid> = ov.appended_candidates(TENANT, q).collect();
        let by_scan = planner
            .appended_candidates_by_scan(ov, TENANT, q)
            .expect("scan");
        let kept: Vec<Pid> = by_index
            .iter()
            .copied()
            .filter(|p| !extra_ok.contains(p))
            .collect();
        assert_eq!(
            kept, by_scan,
            "queue {q}: the index offers what the scan did"
        );
        for p in &by_index {
            assert!(
                by_scan.contains(p) || extra_ok.contains(p),
                "queue {q}: pid {p} offered by the index only"
            );
        }
    }
}

#[test]
fn the_append_index_offers_what_a_scan_of_every_appended_partition_offers() {
    let mut cell = Cell::new("b13-index-vs-scan");
    let queues = ["q0", "q1", "q2", "qd"];
    let mut id = 0u64;
    for q in queues {
        for p in 0..5 {
            id += 1;
            cell.run(&[h::push(id, q, &format!("p{p}"), &[&format!("m-{q}-{p}")])]);
        }
    }
    // An entry in flight planned elsewhere (control, a rebuild): folded
    // without hints, so its pids wait to be placed.
    let in_flight: Entry = cell
        .plan_entry(
            &[
                h::push(100, "q0", "p1", &["x1"]),
                h::push(101, "q1", "p2", &["x2"]),
                h::push(102, "q2", "p3", &["x3"]),
                h::push(103, "qd", "p0", &["x4"]),
            ],
            None,
        )
        .expect("an entry");
    let wall = cell.now();
    cell.node
        .store()
        .read(|r| {
            let d = Derived::default();
            let now = Committed::new(r, &d).plan_now(wall)?;
            let mut ov = Overlay::new(r.next_pid()?, r.kv_version_next()?);
            ov.ingest_entry(&in_flight);
            ov.mark_cycle_start();
            let front = DedupFront::disabled();
            let planner = Planner::new(
                Committed::new(r, &d),
                now,
                PlanConfig::default(),
                &front,
                None,
            );
            assert!(
                !ov.appended_placed(),
                "the ingested pids are not placed yet"
            );
            // This cycle's pushes (hinted), a create in flight, and pushes to
            // pids another append of which is already in flight.
            for (i, (q, p)) in [
                ("q0", "p0"),
                ("q0", "p1"),
                ("q1", "new-a"),
                ("q2", "p4"),
                ("q2", "p3"),
                ("q1", "p0"),
            ]
            .iter()
            .enumerate()
            {
                let c = push_cmd(200 + i as u64, q, p, &[&format!("y{i}")]);
                assert!(matches!(
                    planner.plan_push(&mut ov, &c),
                    Ok(Plan::Logged { .. })
                ));
            }
            assert_index_matches_scan(&planner, &mut ov, &queues, &[]);

            // A delete in flight takes one appended pid; a queue delete takes
            // every committed partition of "qd", and a push re-creates the
            // queue with a partition of its own.
            let gone = r.pid_of(TENANT, "q2", "p4")?.expect("q2/p4");
            let qd: Vec<Pid> = (0..5)
                .map(|p| {
                    r.pid_of(TENANT, "qd", &format!("p{p}"))
                        .map(|x| x.expect("qd"))
                })
                .collect::<Result<_, _>>()?;
            ov.apply_effects(&[
                Effect::PartitionDelete { pid: gone },
                Effect::QueueDelete {
                    tenant: TENANT.to_string(),
                    queue: "qd".to_string(),
                },
                Effect::GarbageAdd {
                    pids: qd.clone(),
                    scope: GarbageScope::Queue,
                    deleted_at_us: now,
                },
            ]);
            let c = push_cmd(300, "qd", "reborn", &["z"]);
            assert!(matches!(
                planner.plan_push(&mut ov, &c),
                Ok(Plan::Logged { .. })
            ));
            assert_index_matches_scan(&planner, &mut ov, &queues, &[]);
            let reborn: Vec<Pid> = ov.appended_candidates(TENANT, "qd").collect();
            assert_eq!(
                reborn.len(),
                1,
                "only the partition created after the delete"
            );
            assert!(!ov.appended_candidates(TENANT, "q2").any(|p| p == gone));

            // An append to a pid with no partition anywhere: nobody's candidate.
            let ghost: Pid = 1 << 40;
            ov.apply_effects(&[Effect::Append {
                pid: ghost,
                bucket: 0,
                base_offset: 0,
                count: 1,
                created_at_us: now,
                hashes: vec![7u8; 16],
                blob: Vec::new(),
            }]);
            assert_index_matches_scan(&planner, &mut ov, &queues, &[]);
            Ok(())
        })
        .expect("read");
}

#[test]
fn the_append_index_follows_its_entries_as_they_land() {
    // Entries fold and land through the kept overlay; after every landing the
    // kept index is the index of what is still in flight, and equal to the
    // index a rebuild from the same entries builds (`KeptOverlay::diff`).
    let mut cell = Cell::new("b13-landing");
    let mut id = 0u64;
    for q in ["a", "b"] {
        for p in 0..4 {
            id += 1;
            cell.run(&[h::push(id, q, &format!("p{p}"), &["seed"])]);
        }
    }
    let mut entries: Vec<(u64, Arc<Entry>)> = Vec::new();
    for round in 0..6u64 {
        let cmds: Vec<h::Cmd> = (0..4u64)
            .map(|k| {
                let q = if (round + k) % 2 == 0 { "a" } else { "b" };
                h::push(
                    1000 + round * 10 + k,
                    q,
                    &format!("p{}", (round + k) % 5),
                    &[&format!("r{round}k{k}")],
                )
            })
            .collect();
        let e = cell.plan_entry(&cmds, None).expect("an entry");
        entries.push((round + 1, Arc::new(e)));
    }
    let (base_pid, base_kv) = cell
        .node
        .store()
        .read(|r| Ok((r.next_pid()?, r.kv_version_next()?)))
        .expect("read");
    // Nothing applied: the planning read is at index 0 throughout, and the
    // kept overlay lands entries by being told the applied index moved.
    let mut kept = KeptOverlay::rebuild(&entries, 0, base_pid, base_kv);
    assert_eq!(kept.overlay().append_index_diff(), None);
    for applied in 1..=entries.len() as u64 {
        let remaining: Vec<(u64, Arc<Entry>)> = entries
            .iter()
            .filter(|(i, _)| *i > applied)
            .cloned()
            .collect();
        kept = kept
            .advance(&remaining, applied, base_pid, base_kv)
            .expect("advance");
        let reference = KeptOverlay::rebuild(&remaining, applied, base_pid, base_kv);
        assert_eq!(kept.diff(&reference), None, "applied {applied}");
        assert_eq!(kept.overlay().append_index_diff(), None);
    }
}

// ---------------------------------------------------------------------------
// B27: control's overlay without the partition state, folded in on demand
// ---------------------------------------------------------------------------

/// A small deterministic stream of numbers.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n.max(1)
    }
}

/// Entry `i` of a synthetic stream that exercises every map the overlay
/// keeps: creates, appends (to the new partition and older ones), cursor
/// writes and deletes, watermarks, KV, timers, groups, queue upserts and
/// deletes, garbage and partition deletes. Offsets are unique per partition
/// (a landing takes an append out by its base).
fn synthetic_entry(i: u64, base_pid: u64) -> Entry {
    use super::samples::{cursor_row, queue_config, timer_row};
    use crate::rsm::effect::GroupMeta;
    let now = 1_800_000_000_000_000 + i as i64 * 1_000;
    let mut e = Entry::new(now, base_pid, 1);
    let q = format!("q{}", i % 3);
    let pid_new = base_pid + i;
    let mut effects = vec![
        Effect::PartitionCreate {
            pid: pid_new,
            uuid: [i as u8; 16],
            tenant: TENANT.into(),
            queue: q.clone(),
            partition: format!("p{i}"),
            created_at_us: now,
        },
        Effect::Append {
            pid: pid_new,
            bucket: 0,
            base_offset: 0,
            count: 2,
            created_at_us: now,
            hashes: [[i as u8; 16], [i as u8 ^ 0x55; 16]].concat(),
            blob: Vec::new(),
        },
    ];
    if i > 1 {
        let older = base_pid + i - 1;
        effects.push(Effect::Append {
            pid: older,
            bucket: 0,
            base_offset: 2 + i * 10,
            count: 1,
            created_at_us: now + 1,
            hashes: vec![(i as u8).wrapping_mul(7); 16],
            blob: Vec::new(),
        });
    }
    if i > 2 {
        let pid = base_pid + i - 2;
        effects.push(if i.is_multiple_of(4) {
            Effect::CursorDelete {
                pid,
                group: "g".into(),
            }
        } else {
            let mut row = cursor_row();
            row.committed = i as i64;
            Effect::CursorSet {
                pid,
                group: if i.is_multiple_of(2) {
                    "g".into()
                } else {
                    "h".into()
                },
                row,
            }
        });
        effects.push(Effect::Watermark {
            pid,
            log_start: i,
            txns_start: i,
        });
    }
    effects.push(Effect::KvPut {
        tenant: TENANT.into(),
        ns: "ns".into(),
        key: format!("k{}", i % 3),
        value: vec![i as u8],
        version: i,
        expires_at_us: None,
        created_at_us: now,
        updated_at_us: now,
    });
    let tkey = format!("t{}", i % 2);
    effects.push(match i % 3 {
        0 => Effect::TimerUpsert {
            tenant: TENANT.into(),
            queue: "tq".into(),
            key: tkey,
            row: timer_row(),
        },
        1 => Effect::TimerBackoff {
            tenant: TENANT.into(),
            queue: "tq".into(),
            key: tkey,
            visible_at_us: now + 5,
            attempts: i as i32,
            last_error: None,
            updated_at_us: now,
        },
        _ => Effect::TimerDelete {
            tenant: TENANT.into(),
            queue: "tq".into(),
            key: tkey,
        },
    });
    effects.push(Effect::GroupUpsert {
        tenant: TENANT.into(),
        queue: q.clone(),
        group: "g".into(),
        meta: GroupMeta {
            id: [9; 16],
            partition_name: String::new(),
            namespace: String::new(),
            task: String::new(),
            mode: crate::rsm::effect::SubscriptionMode::All,
            subscription_timestamp_us: 0,
            conflation: false,
            seeded: false,
            registered_at_us: now,
        },
    });
    if i.is_multiple_of(5) {
        effects.push(Effect::QueueUpsert {
            tenant: TENANT.into(),
            queue: q.clone(),
            cfg: queue_config(),
        });
    }
    if i.is_multiple_of(7) {
        effects.push(Effect::QueueDelete {
            tenant: TENANT.into(),
            queue: "q2".into(),
        });
        effects.push(Effect::GarbageAdd {
            pids: vec![base_pid + i - 3],
            scope: GarbageScope::Queue,
            deleted_at_us: now,
        });
    }
    if i.is_multiple_of(9) {
        effects.push(Effect::PartitionDelete {
            pid: base_pid + i - 4,
        });
    }
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&i.to_be_bytes());
    let (a, b) = effects.split_at(effects.len() / 2);
    e.add_command(id, crate::rsm::entry::Outcome::Empty, a.to_vec())
        .expect("command");
    id[8] = 1;
    e.add_command(id, crate::rsm::entry::Outcome::Empty, b.to_vec())
        .expect("command");
    e
}

#[test]
fn control_s_catalog_overlay_folds_to_exactly_a_rebuild_whenever_it_needs_partitions() {
    use crate::rsm::planner::kept::PARTITIONS_IDLE_CYCLES;
    let base_pid = 100u64;
    let entries: Vec<(u64, Arc<Entry>)> = (1..=160u64)
        .map(|i| (i, Arc::new(synthetic_entry(i, base_pid))))
        .collect();
    let mut rng = Rng(0x05EE_DB27);
    let mut kept = KeptOverlay::rebuild_catalog(&[], 0, base_pid, 1);
    let (mut applied, mut hi, mut cycle) = (0u64, 0u64, 0u64);
    let (mut full, mut catalog, mut folded_in, mut dropped) = (0, 0, 0, 0);
    while applied < 150 {
        cycle += if rng.below(10) == 0 {
            PARTITIONS_IDLE_CYCLES + 2
        } else {
            1
        };
        applied = (applied + rng.below(3)).min(hi);
        let top = (hi + 1 + rng.below(2)).min(entries.len() as u64);
        let folded: Vec<(u64, Arc<Entry>)> = entries
            .iter()
            .filter(|(i, _)| *i > applied && *i <= top)
            .cloned()
            .collect();
        // Most entries are ingested by the catch-up; some are "planned" by
        // control in this cycle and kept with `push_entry`, as the merge does.
        let own_cycle = top > hi && rng.below(3) == 0;
        let ingest: Vec<(u64, Arc<Entry>)> = if own_cycle {
            folded.iter().filter(|(i, _)| *i < top).cloned().collect()
        } else {
            folded.clone()
        };
        kept = kept
            .advance_ingest(&ingest, applied, base_pid, 1)
            .expect("advance");
        let needs = rng.below(4) == 0;
        let had = kept.has_partitions();
        kept.partitions(needs, cycle);
        match (had, kept.has_partitions()) {
            (false, true) => folded_in += 1,
            (true, false) => dropped += 1,
            _ => {}
        }
        // What the cycle plans against: exactly a rebuild from the same
        // entries (keeping the same things).
        let reference = if kept.has_partitions() {
            full += 1;
            KeptOverlay::rebuild(&ingest, applied, base_pid, 1)
        } else {
            catalog += 1;
            KeptOverlay::rebuild_catalog(&ingest, applied, base_pid, 1)
        };
        let diff = if kept.has_partitions() {
            kept.diff_state(&reference, false)
        } else {
            kept.diff(&reference)
        };
        assert_eq!(
            diff, None,
            "cycle {cycle}, applied {applied}, in flight to {top}"
        );
        if own_cycle {
            let (_, e) = folded.last().expect("the cycle's entry").clone();
            let tag = kept.begin_cycle();
            let folds0 = kept.overlay().folds();
            kept.overlay_mut().apply_effects(&e.effects);
            assert_eq!(kept.overlay().folds() - folds0, e.effects.len() as u64);
            kept.push_entry(e, tag).expect("push");
        }
        hi = top;
    }
    assert!(
        full > 10 && catalog > 10 && folded_in > 3 && dropped > 1,
        "both modes and both switches exercised: full {full} catalog {catalog} \
         folded in {folded_in} dropped {dropped}"
    );
}

#[test]
fn a_request_id_in_flight_answers_its_outcome_by_reference() {
    // The single planner's kept overlay records the ids in flight by
    // reference to their entries; a lookup answers the recorded outcome.
    let mut cell = Cell::new("b27-ids");
    cell.run(&[h::push(1, "q", "p", &["a"])]);
    let entry = cell
        .plan_entry(&[h::push(2, "q", "p", &["b", "c"])], None)
        .expect("an entry");
    let want = entry.commands[0].outcome.clone();
    let id = entry.commands[0].request_id;
    let folded = vec![(10u64, Arc::new(entry))];
    let kept = KeptOverlay::rebuild(&folded, 0, 1 << 20, 1);
    cell.node
        .store()
        .read(|r| {
            let d = Derived::default();
            let front = DedupFront::disabled();
            let planner = Planner::new(
                Committed::new(r, &d),
                cell.now(),
                PlanConfig::default(),
                &front,
                None,
            );
            match planner.lookup_request_id(kept.overlay(), &id) {
                Ok(crate::rsm::planner::Lookup::InFlight(o)) => assert_eq!(o, want),
                other => panic!("an in-flight hit, got {other:?}"),
            }
            Ok(())
        })
        .expect("read");
    // A catalog-only overlay records none (the router answers them).
    let catalog = KeptOverlay::rebuild_catalog(&folded, 0, 1 << 20, 1);
    assert!(catalog.exact());
}

// ---------------------------------------------------------------------------
// B28: the kept rings' deadline index
// ---------------------------------------------------------------------------

/// What a rebuild at `now` offers for rows `rows`: the ready pids in walk
/// order, the deferred count and the next deadline.
fn split_at(rows: &[(Pid, i64)], now: i64) -> (Vec<Pid>, usize, Option<i64>) {
    let mut ready: Vec<(i64, Pid)> = rows
        .iter()
        .filter(|(_, a)| *a <= now)
        .map(|(p, a)| (*a, *p))
        .collect();
    ready.sort_unstable();
    let deferred: Vec<i64> = rows
        .iter()
        .filter(|(_, a)| *a > now)
        .map(|(_, a)| *a)
        .collect();
    (
        ready.into_iter().map(|(_, p)| p).collect(),
        deferred.len(),
        deferred.iter().min().copied(),
    )
}

#[test]
fn promotion_by_the_deadline_index_equals_resplitting_every_ring() {
    use crate::rsm::state::{PlanRings, RingKey};
    let keys: Vec<RingKey> = (0..6)
        .map(|i| {
            (
                TENANT.to_string(),
                format!("q{}", i % 3),
                format!("g{}", i / 3),
            )
        })
        .collect();
    let mut rings = PlanRings::new(0, 1_000);
    for k in &keys {
        rings.keep_rows(k, &[]);
    }
    let mut rng = Rng(0xB28);
    let mut now = 1_000i64;
    for round in 0..3_000u32 {
        let k = &keys[rng.below(keys.len() as u64) as usize];
        let pid = rng.below(20);
        match rng.below(10) {
            0..=4 => {
                let at = now - 50 + rng.below(200) as i64;
                rings.plan_set(&k.0, &k.1, &k.2, pid, Some(at));
            }
            5 => rings.plan_set(&k.0, &k.1, &k.2, pid, None),
            6..=8 => {
                now += rng.below(40) as i64;
                rings.promote(now);
            }
            _ => {
                // The clock going back re-splits.
                now -= rng.below(30) as i64;
                rings.promote(now);
            }
        }
        assert_eq!(rings.due_diff(), None, "round {round}");
        for k in &keys {
            let snap = rings.snapshot(&k.0, &k.1, &k.2).expect("kept");
            let (ready, deferred, next) = split_at(&snap.rows, now);
            assert_eq!(
                (snap.ready.clone(), snap.deferred, snap.next_deadline),
                (ready, deferred, next),
                "round {round}, ring {k:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// B34: the kept rings checked a little every cycle
// ---------------------------------------------------------------------------

/// A cell with queue "vq" of 40 partitions, each with work for group "g".
fn pending_cell(tag: &str) -> (Cell, Vec<Pid>) {
    let mut cell = Cell::new(tag);
    let mut pids = Vec::new();
    for p in 0..40u64 {
        cell.run(&[h::push(p + 1, "vq", &format!("p{p}"), &[&format!("m{p}")])]);
        pids.push(cell.pid_of("vq", &format!("p{p}")).expect("pid"));
    }
    // Register "g" with a pop that takes nothing (budget spent on one
    // partition): every partition has a pending row.
    cell.run(&[h::pop_wildcard_with(100, "vq", "g", "w", |c| {
        c.max_parts = 1;
        c.budget = 1;
    })]);
    (cell, pids)
}

#[test]
fn the_ring_check_passes_rings_that_match_pending_and_drops_one_that_does_not() {
    use crate::rsm::state::PlanRings;
    let (cell, pids) = pending_cell("b34-check");
    let key = (TENANT.to_string(), "vq".to_string(), "g".to_string());
    cell.node
        .store()
        .read(|r| {
            let applied = r.applied_index()?;
            let now = cell.now() + 1_000_000;
            let mut rings = PlanRings::new(applied, now);
            rings.ensure(r, &key, 1)?;
            // Many passes over a matching ring: nothing is dropped.
            for _ in 0..20 {
                rings.advance(r, &[], applied, now)?;
            }
            assert_eq!(rings.verify_drops, 0);
            assert!(rings.has(&key.0, &key.1, &key.2));

            // A row an entry in flight writes may differ: skipped.
            let mut e = Entry::new(now, 1 << 20, 1);
            e.add_command(
                [7; 16],
                crate::rsm::entry::Outcome::Empty,
                vec![Effect::CursorSet {
                    pid: pids[3],
                    group: "g".into(),
                    row: super::samples::cursor_row(),
                }],
            )
            .expect("command");
            let in_flight = vec![(applied + 1, Arc::new(e))];
            rings.plan_set(&key.0, &key.1, &key.2, pids[3], Some(now + 5_000_000));
            for _ in 0..20 {
                rings.advance(r, &in_flight, applied, now)?;
            }
            assert_eq!(rings.verify_drops, 0, "a row in flight is not checked");

            // The same row with nothing in flight: the check finds it.
            for _ in 0..20 {
                rings.advance(r, &[], applied, now)?;
            }
            assert_eq!(rings.verify_drops, 1, "the differing ring is dropped");
            assert!(!rings.has(&key.0, &key.1, &key.2));

            // A row pending does not have: found too.
            rings.ensure(r, &key, 2)?;
            rings.plan_set(&key.0, &key.1, &key.2, 1 << 30, Some(now));
            for _ in 0..20 {
                rings.advance(r, &[], applied, now)?;
            }
            assert_eq!(rings.verify_drops, 2);
            Ok(())
        })
        .expect("read");
}

// ---------------------------------------------------------------------------
// B14: a lane's rows planned once
// ---------------------------------------------------------------------------

fn entry_with(now: i64, commands: &[([u8; 16], Vec<Effect>)]) -> Entry {
    let mut e = Entry::new(now, 1 << 20, 1);
    for (id, effects) in commands {
        e.add_command(*id, crate::rsm::entry::Outcome::Empty, effects.clone())
            .expect("command");
    }
    e
}

#[test]
fn a_lane_plans_the_rows_of_an_entry_once_and_its_own_commands_never_twice() {
    use crate::rsm::state::PlanRings;
    let cursor = |pid: Pid| Effect::CursorSet {
        pid,
        group: "g".into(),
        row: super::samples::cursor_row(),
    };
    let own = [1u8; 16];
    let routed = [2u8; 16];
    let e1 = Arc::new(entry_with(
        1,
        &[(own, vec![cursor(2)]), (routed, vec![cursor(4), cursor(6)])],
    ));
    let e2 = Arc::new(entry_with(2, &[([3u8; 16], vec![cursor(8)])]));
    let mut rings = PlanRings::new(0, 0);
    rings.set_lane(0, 2);

    // The job that logged `own` planned its rows; the entry shows up with the
    // merge's routed command after it: only that one is planned now.
    let mut sub = Entry::new(1, 1 << 20, 1);
    sub.add_command(own, crate::rsm::entry::Outcome::Empty, vec![cursor(2)])
        .expect("command");
    rings.planned_own(&sub);
    let folded = vec![(1u64, e1.clone())];
    let rows: Vec<&[Effect]> = rings.rows_to_plan(&folded, 0, false);
    assert_eq!(rows, vec![&e1.effects[1..3]]);
    // Seen: never again, until a ring is scanned afresh.
    let folded = vec![(1u64, e1.clone()), (2u64, e2.clone())];
    let rows = rings.rows_to_plan(&folded, 0, false);
    assert_eq!(rows, vec![&e2.effects[..]], "only the new entry, whole");
    assert!(rings.rows_to_plan(&folded, 0, false).is_empty());
    let rows = rings.rows_to_plan(&folded, 0, true);
    assert_eq!(
        rows,
        vec![&e1.effects[..], &e2.effects[..]],
        "a scan: every row in flight"
    );
    let rows = rings.rows_to_plan(&folded, 1, true);
    assert_eq!(
        rows,
        vec![&e2.effects[..]],
        "a landed entry has no row to plan"
    );
}

#[test]
fn a_landing_leaves_a_row_an_entry_in_flight_writes_at_its_planned_state() {
    use crate::rsm::state::PlanRings;
    let (cell, pids) = pending_cell("b14-landing");
    let key = (TENANT.to_string(), "vq".to_string(), "g".to_string());
    let pid = pids[5];
    cell.node
        .store()
        .read(|r| {
            let applied = r.applied_index()?;
            let now = cell.now() + 1_000_000;
            let committed = r
                .pending_at(TENANT, "vq", "g", pid)?
                .expect("a pending row");
            let planned = now + 30_000_000;
            let write = || {
                Arc::new(entry_with(
                    now,
                    &[(
                        [pid as u8; 16],
                        vec![Effect::CursorSet {
                            pid,
                            group: "g".into(),
                            row: super::samples::cursor_row(),
                        }],
                    )],
                ))
            };
            for (lanes, keeps_planned) in [(2u64, true), (1u64, false)] {
                // Lane `pid % lanes` keeps the ring; E1 (landing) and E2 (in
                // flight) both write the row, which E2's planning moved.
                let mut rings = PlanRings::new(applied - 1, now);
                rings.set_lane(pid % lanes, lanes);
                rings.ensure(r, &key, 1)?;
                rings.plan_set(&key.0, &key.1, &key.2, pid, Some(planned));
                let folded = vec![(applied, write()), (applied + 1, write())];
                rings.advance(r, &folded, applied, now)?;
                let got = rings
                    .snapshot(&key.0, &key.1, &key.2)
                    .and_then(|s| s.rows.iter().find(|(p, _)| *p == pid).map(|(_, a)| *a));
                let want = if keeps_planned { planned } else { committed };
                assert_eq!(got, Some(want), "lanes {lanes}");
                // Once the newest writer lands too, the committed row is back.
                rings.advance(r, &folded, applied + 1, now)?;
                let got = rings
                    .snapshot(&key.0, &key.1, &key.2)
                    .and_then(|s| s.rows.iter().find(|(p, _)| *p == pid).map(|(_, a)| *a));
                assert_eq!(got, Some(committed), "lanes {lanes}: landed");
            }
            Ok(())
        })
        .expect("read");
}

// ---------------------------------------------------------------------------
// Measurements (not gates): run with --ignored --nocapture
// ---------------------------------------------------------------------------

#[test]
#[ignore = "measurement, not a gate"]
fn measure_wildcard_in_flight_gather() {
    // 40 queues x 25 partitions, every partition appended in flight (one
    // entry folded without hints, as control's or a rebuild's): the in-flight
    // gather of one wildcard pop, by the old scan and by the index.
    let mut cell = Cell::new("b13-measure");
    let (queues, parts) = (40u64, 25u64);
    let mut cmds = Vec::new();
    let mut id = 0u64;
    for q in 0..queues {
        for p in 0..parts {
            id += 1;
            cmds.push(h::push(id, &format!("q{q}"), &format!("p{p}"), &["seed"]));
        }
    }
    cell.run(&cmds);
    let in_flight: Vec<h::Cmd> = (0..queues * parts)
        .map(|i| {
            h::push(
                10_000 + i,
                &format!("q{}", i / parts),
                &format!("p{}", i % parts),
                &[&format!("m{i}")],
            )
        })
        .collect();
    let entry = cell.plan_entry(&in_flight, None).expect("an entry");
    let wall = cell.now();
    cell.node
        .store()
        .read(|r| {
            let d = Derived::default();
            let now = Committed::new(r, &d).plan_now(wall)?;
            let mut ov = Overlay::new(r.next_pid()?, r.kv_version_next()?);
            ov.ingest_entry(&entry);
            let front = DedupFront::disabled();
            let planner = Planner::new(
                Committed::new(r, &d),
                now,
                PlanConfig::default(),
                &front,
                None,
            );
            let pops = 200u64;
            let t0 = std::time::Instant::now();
            let mut n_old = 0usize;
            for k in 0..pops {
                n_old += planner
                    .appended_candidates_by_scan(&ov, TENANT, &format!("q{}", k % queues))
                    .expect("scan")
                    .len();
            }
            let old = t0.elapsed();
            let t1 = std::time::Instant::now();
            planner.place_unplaced(&mut ov).expect("place");
            let placed = t1.elapsed();
            let t2 = std::time::Instant::now();
            let mut n_new = 0usize;
            for k in 0..pops {
                n_new += ov
                    .appended_candidates(TENANT, &format!("q{}", k % queues))
                    .count();
            }
            let new = t2.elapsed();
            assert_eq!(n_old, n_new);
            eprintln!(
                "B13 gather, {} appended pids, {pops} pops: scan {:?} ({:.1} us/pop) vs index {:?} \
                 ({:.2} us/pop) + one placement pass {:?}",
                queues * parts,
                old,
                old.as_secs_f64() * 1e6 / pops as f64,
                new,
                new.as_secs_f64() * 1e6 / pops as f64,
                placed
            );
            Ok(())
        })
        .expect("read");
}

#[test]
#[ignore = "measurement, not a gate"]
fn measure_control_overlay_catch_up() {
    // Control's per-cycle catch-up (unfold what landed, fold what is new) over
    // entries of 100-message appends: with the partition state (the old
    // control) and without it (B27).
    let entry = |i: u64| -> Arc<Entry> {
        let mut e = Entry::new(1_800_000_000_000_000 + i as i64, 1 << 30, 1);
        let mut effects = Vec::new();
        for p in 0..8u64 {
            let hashes: Vec<u8> = (0..100u64)
                .flat_map(|m| {
                    let mut h = [0u8; 16];
                    h[..8].copy_from_slice(&(i * 1_000_000 + p * 1_000 + m).to_le_bytes());
                    h
                })
                .collect();
            effects.push(Effect::Append {
                pid: p,
                bucket: 0,
                base_offset: i * 100,
                count: 100,
                created_at_us: 1_800_000_000_000_000 + i as i64,
                hashes,
                blob: Vec::new(),
            });
        }
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&i.to_be_bytes());
        e.add_command(id, crate::rsm::entry::Outcome::Empty, effects)
            .expect("command");
        Arc::new(e)
    };
    let cycles = 400u64;
    let entries: Vec<(u64, Arc<Entry>)> = (1..=cycles + 8).map(|i| (i, entry(i))).collect();
    for (name, catalog) in [("full", false), ("catalog", true)] {
        let mut kept = if catalog {
            KeptOverlay::rebuild_catalog(&[], 0, 1 << 30, 1)
        } else {
            KeptOverlay::rebuild(&[], 0, 1 << 30, 1)
        };
        let t0 = std::time::Instant::now();
        for c in 0..cycles {
            let folded: Vec<(u64, Arc<Entry>)> = entries[c as usize..c as usize + 8].to_vec();
            kept = kept
                .advance_ingest(&folded, c, 1 << 30, 1)
                .expect("advance");
        }
        let dt = t0.elapsed();
        eprintln!(
            "B27 control catch-up ({name}): {:.1} us/cycle, {:.0} ns per message (8 x 100 per entry)",
            dt.as_secs_f64() * 1e6 / cycles as f64,
            dt.as_secs_f64() * 1e9 / (cycles * 800) as f64
        );
    }
}
