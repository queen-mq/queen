//! The planner's kept overlay against what it stands for: whatever the kept
//! state answers, a rebuild from the same entries answers the same.
//!
//! - B27, control's catalog-only overlay: it folds the partition state of its
//!   entries in flight only for a cycle that needs it, and is then exactly the
//!   overlay a rebuild of those entries gives.
//! - the request ids in flight are recorded by reference to their entries.

use std::sync::Arc;

use super::planner_harness::{self as h, Cell, TENANT};
use crate::rsm::dedup::DedupFront;
use crate::rsm::effect::{Effect, GarbageScope};
use crate::rsm::entry::Entry;
use crate::rsm::planner::kept::KeptOverlay;
use crate::rsm::planner::{PlanConfig, Planner};
use crate::rsm::state::Committed;
use crate::rsm::store::Store;

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
            rows: None,
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
            let front = DedupFront::disabled();
            let planner = Planner::new(
                Committed::new(r),
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
