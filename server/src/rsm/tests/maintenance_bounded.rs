//! Catalog and partition work are bounded independently, and both cursors progress.
use super::store::TempStore;
use crate::rsm::effect::Effect;
use crate::rsm::maintenance::{self, Config};
use crate::rsm::store::{
    keys, rows::PartitionRow, Keyspace, Reads, Result, Store, TypedWrites, Writes,
};
use std::cell::RefCell;
const NOW: i64 = 100 * 86_400 * 1_000_000;

struct CountRead<'a, R: Reads + ?Sized> {
    r: &'a R,
    queues: RefCell<Vec<Vec<u8>>>,
    partitions: RefCell<Vec<Vec<u8>>>,
}
impl<'a, R: Reads + ?Sized> CountRead<'a, R> {
    fn new(r: &'a R) -> Self {
        Self {
            r,
            queues: RefCell::default(),
            partitions: RefCell::default(),
        }
    }
}
impl<R: Reads + ?Sized> Reads for CountRead<'_, R> {
    fn get_raw(&self, ks: Keyspace, k: &[u8]) -> Result<Option<&[u8]>> {
        self.r.get_raw(ks, k)
    }
    fn max_key_len(&self) -> usize {
        self.r.max_key_len()
    }
    fn scan_raw(
        &self,
        ks: Keyspace,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
        cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<usize> {
        self.r.scan_raw(ks, from, prefix, limit, &mut |k, v| {
            if ks == Keyspace::Queues {
                self.queues.borrow_mut().push(k.to_vec());
            }
            if ks == Keyspace::QueuePartitions {
                self.partitions.borrow_mut().push(k.to_vec());
            }
            cb(k, v)
        })
    }
    fn scan_rev_raw(
        &self,
        ks: Keyspace,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
        cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<usize> {
        self.r.scan_rev_raw(ks, from, prefix, limit, cb)
    }
}
fn queues(n: usize) -> TempStore {
    let s = TempStore::new();
    let mut w = s.s().write().unwrap();
    for i in 0..n {
        w.put_queue("t", &format!("q{i:04}"), &super::planner_harness::qcfg())
            .unwrap();
    }
    w.commit().unwrap();
    drop(w);
    s
}

#[test]
fn a_disabled_partition_walk_does_not_read_the_queue_catalog() {
    let s = queues(1000);
    s.s()
        .read(|r| {
            let counted = CountRead::new(r);
            let out = maintenance::plan(
                &counted,
                NOW,
                &Config {
                    partition_walk: false,
                    ..Default::default()
                },
            )?;
            assert!(out.effects.is_empty());
            assert!(counted.queues.borrow().is_empty());
            assert!(counted.partitions.borrow().is_empty());
            Ok(())
        })
        .unwrap();
}

#[test]
fn empty_queues_consume_the_catalog_budget_and_the_cursor_wraps() {
    let s = queues(7);
    let cfg = Config {
        visit_total: 2,
        ..Default::default()
    };
    s.s()
        .read(|r| {
            let mut starts = Vec::new();
            for expected in ["q0000", "q0002", "q0004", "q0006", "q0000"] {
                let counted = CountRead::new(r);
                let out = maintenance::plan(&counted, NOW, &cfg)?;
                assert!(out.effects.is_empty());
                let rows = counted.queues.borrow();
                assert!(rows.len() <= 3, "two queues plus one lookahead");
                assert_eq!(
                    rows[0],
                    keys::queues("t", expected),
                    "no rescan before the resume key"
                );
                starts.push(rows[0].clone());
            }
            assert_eq!(starts.first(), starts.last());
            Ok(())
        })
        .unwrap();
}

#[test]
fn a_deleted_resume_queue_seeks_to_its_successor_and_wraps() {
    let s = queues(4);
    let cfg = Config {
        visit_total: 1,
        ..Default::default()
    };
    s.s().read(|r| maintenance::plan(r, NOW, &cfg)).unwrap();
    assert_eq!(
        *cfg.walk_queue.lock().unwrap(),
        Some(("t".into(), "q0001".into()))
    );
    let mut w = s.s().write().unwrap();
    w.del_queue("t", "q0001").unwrap();
    w.commit().unwrap();
    s.s()
        .read(|r| {
            let cr = CountRead::new(r);
            maintenance::plan(&cr, NOW, &cfg)?;
            assert_eq!(cr.queues.borrow()[0], keys::queues("t", "q0002"));
            Ok(())
        })
        .unwrap();
    *cfg.walk_queue.lock().unwrap() = Some(("t".into(), "zz-deleted".into()));
    s.s().read(|r| maintenance::plan(r, NOW, &cfg)).unwrap();
    assert_eq!(*cfg.walk_queue.lock().unwrap(), None);
    s.s()
        .read(|r| {
            let cr = CountRead::new(r);
            maintenance::plan(&cr, NOW, &cfg)?;
            assert_eq!(cr.queues.borrow()[0], keys::queues("t", "q0000"));
            Ok(())
        })
        .unwrap();
}

#[test]
fn effect_and_partition_caps_preserve_progress_within_a_queue() {
    for visit in [1, 3] {
        let s = queues(2);
        let mut w = s.s().write().unwrap();
        for pid in 1..=5 {
            let mut row = PartitionRow::new([1; 16], "t", "q0000", &format!("p{pid}"), 1);
            row.last_write_at_us = 1;
            w.create_partition(pid, &row).unwrap();
        }
        w.commit().unwrap();
        let cfg = Config {
            visit_total: visit,
            visit_cap: visit,
            row_limit: 1,
            ..Default::default()
        };
        s.s()
            .read(|r| {
                let mut deleted = Vec::new();
                for _ in 0..5 {
                    let cr = CountRead::new(r);
                    let out = maintenance::plan(&cr, NOW, &cfg)?;
                    assert!(cr.queues.borrow().len() <= visit + 1);
                    assert!(cr.partitions.borrow().len() <= visit);
                    let pids: Vec<_> = out
                        .effects
                        .iter()
                        .filter_map(|e| match e {
                            Effect::PartitionDelete { pid } => Some(*pid),
                            _ => None,
                        })
                        .collect();
                    assert_eq!(pids.len(), 1);
                    deleted.extend(pids);
                }
                assert_eq!(
                    deleted,
                    vec![1, 2, 3, 4, 5],
                    "effect budget must not reset or skip the pid cursor"
                );
                Ok(())
            })
            .unwrap();
    }
}

#[test]
fn a_queue_larger_than_the_partition_budget_does_not_starve_later_queues() {
    let s = queues(2);
    let mut w = s.s().write().unwrap();
    for (pid, queue) in [(1, "q0000"), (2, "q0000"), (3, "q0000"), (4, "q0001")] {
        let mut row = PartitionRow::new([1; 16], "t", queue, &format!("p{pid}"), 1);
        row.last_write_at_us = NOW; // Live, so work is bounded by visits, not effects.
        w.create_partition(pid, &row).unwrap();
    }
    w.commit().unwrap();
    let cfg = Config {
        visit_total: 1,
        visit_cap: 1,
        ..Default::default()
    };
    s.s()
        .read(|r| {
            let mut visited = Vec::new();
            for _ in 0..5 {
                let cr = CountRead::new(r);
                maintenance::plan(&cr, NOW, &cfg)?;
                assert!(cr.partitions.borrow().len() <= 1);
                visited.extend(cr.partitions.borrow().iter().cloned());
            }
            assert_eq!(
                visited,
                vec![
                    keys::queue_partitions("t", "q0000", 1),
                    keys::queue_partitions("t", "q0000", 2),
                    keys::queue_partitions("t", "q0000", 3),
                    keys::queue_partitions("t", "q0001", 4),
                ]
            );
            Ok(())
        })
        .unwrap();
}
