//! The store against a real LMDB environment: the keyspaces, the scans, the
//! isolation the planner depends on, and the four pins of D9.
//!
//! Everything here opens a throwaway environment under the system temp
//! directory and removes it on the way out ([`TempStore`]). Nothing writes
//! into the repository and nothing is left behind.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::rsm::dedup;
use crate::rsm::effect::{CursorRow, GarbageScope, GroupMeta, SubscriptionMode};
use crate::rsm::state::Derived;
use crate::rsm::store::keys::{self, Counter};
use crate::rsm::store::rows::{
    self, DlqRow, FileRow, GarbageRow, GroupRow, PartitionRow, SegLocRow,
};
use crate::rsm::store::{
    HeedStore, Keyspace, MapUsage, Reads, Scope, Store, StoreError, StoreMetrics, StoreOpts,
    TypedReads, TypedWrites, Writes,
};

use super::samples;

// ---------------------------------------------------------------------------
// A throwaway store
// ---------------------------------------------------------------------------

static SEQ: AtomicU64 = AtomicU64::new(0);

/// A store in a unique temp directory, removed when the guard drops.
pub struct TempStore {
    pub store: Option<HeedStore>,
    pub dir: PathBuf,
}

impl TempStore {
    pub fn with(opts: StoreOpts) -> TempStore {
        let dir = std::env::temp_dir().join(format!(
            "queen-rsm-store-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = HeedStore::open(&dir, &opts).expect("open");
        TempStore {
            store: Some(store),
            dir,
        }
    }

    pub fn new() -> TempStore {
        // A small map keeps the sparse file small on a laptop; the production
        // default is the rule of §11.8.
        TempStore::with(StoreOpts {
            map_bytes: Some(64 << 20),
            ..Default::default()
        })
    }

    pub fn s(&self) -> &HeedStore {
        self.store.as_ref().expect("store is open")
    }

    /// Close and reopen at the same path, as recovery does (§11.5).
    pub fn reopen(&mut self, opts: StoreOpts) {
        if let Some(s) = self.store.take() {
            s.close();
        }
        self.store = Some(HeedStore::open(&self.dir, &opts).expect("reopen"));
    }
}

impl Drop for TempStore {
    fn drop(&mut self) {
        if let Some(s) = self.store.take() {
            s.close();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn a_group_row() -> GroupRow {
    GroupRow {
        meta: GroupMeta {
            id: samples::uuid(4),
            partition_name: String::new(),
            namespace: "ns".into(),
            task: "task".into(),
            mode: SubscriptionMode::New,
            subscription_timestamp_us: 0,
            conflation: false,
            seeded: true,
            registered_at_us: 1_000,
        },
        reg_index: 12,
        reg_effect: 3,
    }
}

fn a_cursor(committed: i64) -> CursorRow {
    let mut c = rows::cursor_fresh(committed, 1_000);
    c.total_consumed = 5;
    c
}

// ---------------------------------------------------------------------------
// CRUD and range scans, keyspace by keyspace
// ---------------------------------------------------------------------------

#[test]
fn every_message_path_keyspace_round_trips_through_a_commit() {
    let t = TempStore::new();
    let s = t.s();
    let cfg = samples::queue_config();
    let grp = a_group_row();
    let part = PartitionRow::new(samples::uuid(1), "t1", "q1", "p0", 5_000);
    let cur = a_cursor(7);
    let dlq = DlqRow {
        pid: 1,
        group: "g".into(),
        offset: 9,
        message_id: Some(samples::uuid(2)),
        txn: "txn-1".into(),
        payload: b"{}".to_vec(),
        error: "boom".into(),
        retry_count: 1,
        failed_at_us: 42,
    };
    let seg = SegLocRow {
        bucket: 3,
        file_id: 4,
        offset: 128,
        len: 64,
    };
    let file = FileRow {
        len: 4096,
        durable_len: 4096,
        sealed: false,
        frames: 2,
        retained_frames: 2,
        retained_bytes: 4096,
        window_frames: 2,
        snapshot_refs: 0,
    };

    {
        let mut w = s.write().unwrap();
        w.set_applied(11, 2).unwrap();
        w.set_meta_u64(crate::rsm::store::meta::NEXT_PID, 2)
            .unwrap();
        w.set_meta_i64(crate::rsm::store::meta::LAST_NOW_US, 5_000)
            .unwrap();
        w.put_queue("t1", "q1", &cfg).unwrap();
        w.put_group("t1", "q1", "g", &grp).unwrap();
        w.create_partition(1, &part).unwrap();
        w.put_partition_file(1, 4).unwrap();
        w.put_cursor(1, "g", &cur).unwrap();
        w.put_lease("worker-1", 1, "g", 9_000).unwrap();
        w.put_pending("t1", "q1", "g", 1, 5_500).unwrap();
        w.put_dlq("t1", "q1", &samples::uuid(3), &dlq).unwrap();
        w.put_request_outcome(&samples::uuid(5), 5_000, b"outcome")
            .unwrap();
        w.add_counter(&keys::counter_partition(1, Counter::Pushed), 3)
            .unwrap();
        w.put_garbage(
            9,
            &GarbageRow {
                deleted_at_us: 1,
                scope: GarbageScope::Queue,
                queue_id: None,
                resume: Vec::new(),
            },
        )
        .unwrap();
        w.put_seg_loc(1, 0, &seg).unwrap();
        w.put_file(3, 4, &file).unwrap();
        w.commit().unwrap();
    }

    s.read(|r| {
        assert_eq!(r.applied_index()?, 11);
        assert_eq!(r.applied_term()?, 2);
        assert_eq!(r.next_pid()?, 2);
        assert_eq!(r.last_now_us()?, 5_000);
        assert_eq!(r.queue("t1", "q1")?.as_ref(), Some(&cfg));
        assert_eq!(r.group("t1", "q1", "g")?.as_ref(), Some(&grp));
        assert_eq!(r.partition(1)?.as_ref(), Some(&part));
        assert_eq!(r.pid_of("t1", "q1", "p0")?, Some(1));
        assert_eq!(r.cursor(1, "g")?.as_ref(), Some(&cur));
        assert_eq!(r.pending_at("t1", "q1", "g", 1)?, Some(5_500));
        assert_eq!(r.dlq("t1", "q1", &samples::uuid(3))?.as_ref(), Some(&dlq));
        assert_eq!(r.dlq_id_at(1, "g", 9)?, Some(samples::uuid(3)));
        assert_eq!(
            r.request_outcome(&samples::uuid(5))?.map(|x| x.outcome),
            Some(b"outcome".to_vec())
        );
        assert_eq!(r.partition_counter(1, Counter::Pushed)?, 3);
        assert_eq!(
            r.partition_counter(1, Counter::Pending)?,
            0,
            "never written"
        );
        assert!(r.garbage(9)?.is_some());
        assert_eq!(r.seg_loc(1, 0)?, Some(seg));
        assert_eq!(r.file(3, 4)?, Some(file));
        Ok(())
    })
    .unwrap();

    // …and every delete removes both the row and its index.
    {
        let mut w = s.write().unwrap();
        assert!(w.del_queue("t1", "q1").unwrap());
        assert!(w.del_group("t1", "q1", "g").unwrap());
        assert!(w.del_cursor(1, "g").unwrap());
        assert!(w.del_lease("worker-1", 1, "g").unwrap());
        assert!(w.del_pending("t1", "q1", "g", 1).unwrap());
        assert!(w.del_dlq("t1", "q1", &samples::uuid(3), &dlq).unwrap());
        assert!(w.del_partition_file(1, 4).unwrap());
        assert!(w.del_partition(1, &part).unwrap());
        assert!(w.del_garbage(9).unwrap());
        assert!(w.del_seg_loc(1, 0).unwrap());
        assert!(w.del_file(3, 4).unwrap());
        assert!(w.del_request_id(&samples::uuid(5), 5_000).unwrap());
        // A partition's counters go by prefix sweep, as `PartitionDelete` and
        // a `DeleteChunk` do it.
        let cp = keys::counter_partition_prefix(1);
        let (n, resume) = w.delete_range(Keyspace::Counters, &cp, &cp, 100).unwrap();
        assert_eq!(n, 1);
        assert!(resume.is_none());
        w.commit().unwrap();
    }

    s.read(|r| {
        for ks in Keyspace::ALL {
            let n = r.count(ks)?;
            // meta keeps what was written: applied index and term,
            // next_pid and last_now_us.
            let want = if ks == Keyspace::Meta { 4 } else { 0 };
            assert_eq!(n, want, "{} still holds {n} rows", ks.name());
        }
        assert_eq!(r.pid_of("t1", "q1", "p0")?, None);
        assert_eq!(r.dlq_id_at(1, "g", 9)?, None);
        Ok(())
    })
    .unwrap();
}

#[test]
fn range_scans_are_in_key_order_and_bounded_by_their_prefix() {
    let t = TempStore::new();
    let s = t.s();
    {
        let mut w = s.write().unwrap();
        for (i, q) in ["a", "b", "c"].iter().enumerate() {
            let mut cfg = samples::queue_config();
            cfg.priority = i as i32;
            w.put_queue("t1", q, &cfg).unwrap();
            w.put_queue("t2", q, &cfg).unwrap();
        }
        for pid in [7u64, 2, 30, 1] {
            w.put_raw(
                Keyspace::QueuePartitions,
                &keys::queue_partitions("t1", "a", pid),
                rows::UNIT,
            )
            .unwrap();
        }
        // A second tenant AFTER t1 in key order, so neither the forward nor
        // the reverse walk below can pass by sitting at an end of the
        // keyspace that happens to be t1's.
        w.put_raw(
            Keyspace::QueuePartitions,
            &keys::queue_partitions("t2", "a", 5),
            rows::UNIT,
        )
        .unwrap();
        for g in ["g2", "g1"] {
            w.put_cursor(7, g, &a_cursor(1)).unwrap();
        }
        w.commit().unwrap();
    }

    s.read(|r| {
        let mut seen = Vec::new();
        r.scan_queues("t1", 100, &mut |name, _cfg| {
            seen.push(name.to_string());
            true
        })?;
        assert_eq!(seen, vec!["a", "b", "c"], "one tenant only, in name order");

        let mut pids = Vec::new();
        r.scan_queue_partitions("t1", "a", None, 100, &mut |p| {
            pids.push(p);
            true
        })?;
        assert_eq!(pids, vec![1, 2, 7, 30], "numeric order, not lexicographic");

        // A resume point: everything from pid 7 on.
        let mut rest = Vec::new();
        r.scan_queue_partitions("t1", "a", Some(7), 100, &mut |p| {
            rest.push(p);
            true
        })?;
        assert_eq!(rest, vec![7, 30]);

        // A limit stops the walk.
        let mut two = Vec::new();
        r.scan_queue_partitions("t1", "a", None, 2, &mut |p| {
            two.push(p);
            true
        })?;
        assert_eq!(two, vec![1, 2]);

        // …and so does a callback that says stop.
        let mut one = Vec::new();
        r.scan_queue_partitions("t1", "a", None, 100, &mut |p| {
            one.push(p);
            false
        })?;
        assert_eq!(one, vec![1]);

        let mut groups = Vec::new();
        r.scan_cursors(7, 100, &mut |g, _c| {
            groups.push(g.to_string());
            true
        })?;
        assert_eq!(groups, vec!["g1", "g2"]);

        // A reverse scan gives the same rows backwards, with t2's row sitting
        // at the end of the keyspace.
        let mut back = Vec::new();
        r.scan_rev_raw(
            Keyspace::QueuePartitions,
            &[],
            &keys::queue_partitions_prefix("t1", "a"),
            100,
            &mut |k, _v| {
                back.push(keys::queue_partitions_pid_of(k).unwrap());
                true
            },
        )?;
        assert_eq!(back, vec![30, 7, 2, 1]);
        Ok(())
    })
    .unwrap();
}

#[test]
fn a_reverse_scan_finds_a_prefix_that_is_not_at_the_end_of_the_keyspace() {
    // The shape §8's DLQ and trace listings read newest-first with. Bounded at
    // one end only, the walk started at the LAST key of the WHOLE keyspace —
    // another tenant's row — failed the prefix test on its first step and
    // answered an EMPTY list: a silent wrong answer, not an error, and the
    // class of the "false retry/DLQ successes" already on record for the
    // webapp. A test whose keyspace holds one prefix cannot see it.
    let t = TempStore::new();
    let s = t.s();
    let row = |offset: i64| DlqRow {
        pid: 1,
        group: "g".into(),
        offset,
        message_id: Some(samples::uuid(2)),
        txn: format!("txn-{offset}"),
        payload: b"{}".to_vec(),
        error: "boom".into(),
        retry_count: 0,
        failed_at_us: 42,
    };
    let id = |n: u8| [n; 16];
    {
        let mut w = s.write().unwrap();
        // t1 is NOT the last tenant in the keyspace, and t3 is not the first.
        for n in [1u8, 2, 3] {
            w.put_dlq("t1", "q", &id(n), &row(n as i64)).unwrap();
        }
        w.put_dlq("t2", "q", &id(9), &row(9)).unwrap();
        w.put_dlq("t3", "q", &id(8), &row(8)).unwrap();
        w.commit().unwrap();
    }
    s.read(|r| {
        // Newest first, for a tenant with rows on BOTH sides of it.
        let mut back: Vec<u8> = Vec::new();
        let n = r.scan_rev_raw(
            Keyspace::Dlq,
            &[],
            &keys::dlq_prefix("t1", "q"),
            100,
            &mut |k, _v| {
                back.push(k[k.len() - 1]);
                true
            },
        )?;
        assert_eq!(n, 3);
        assert_eq!(back, vec![3, 2, 1], "t1 got another tenant's answer");

        // The LAST prefix of the keyspace: its upper bound is past every key
        // that exists, which is the other end of the same fix.
        let mut last: Vec<u8> = Vec::new();
        r.scan_rev_raw(
            Keyspace::Dlq,
            &[],
            &keys::dlq_prefix("t3", "q"),
            100,
            &mut |k, _v| {
                last.push(k[k.len() - 1]);
                true
            },
        )?;
        assert_eq!(last, vec![8]);

        // A limit takes the NEWEST rows, not the oldest.
        let mut two: Vec<u8> = Vec::new();
        r.scan_rev_raw(
            Keyspace::Dlq,
            &[],
            &keys::dlq_prefix("t1", "q"),
            2,
            &mut |k, _v| {
                two.push(k[k.len() - 1]);
                true
            },
        )?;
        assert_eq!(two, vec![3, 2]);

        // A resume point inside the range still pages backwards…
        let mut page: Vec<u8> = Vec::new();
        r.scan_rev_raw(
            Keyspace::Dlq,
            &keys::dlq("t1", "q", &id(2)),
            &keys::dlq_prefix("t1", "q"),
            100,
            &mut |k, _v| {
                page.push(k[k.len() - 1]);
                true
            },
        )?;
        assert_eq!(page, vec![2, 1]);

        // …and one past the end of the range is clamped into it rather than
        // starting the walk on the next tenant's rows.
        let mut clamped: Vec<u8> = Vec::new();
        r.scan_rev_raw(
            Keyspace::Dlq,
            &keys::dlq("t2", "q", &id(9)),
            &keys::dlq_prefix("t1", "q"),
            100,
            &mut |k, _v| {
                clamped.push(k[k.len() - 1]);
                true
            },
        )?;
        assert_eq!(clamped, vec![3, 2, 1]);

        // The forward direction has the same contract: an empty `from` starts
        // at the beginning of the PREFIX range, not of the keyspace.
        let mut fwd: Vec<u8> = Vec::new();
        r.scan_raw(
            Keyspace::Dlq,
            &[],
            &keys::dlq_prefix("t3", "q"),
            100,
            &mut |k, _v| {
                fwd.push(k[k.len() - 1]);
                true
            },
        )?;
        assert_eq!(fwd, vec![8], "the forward walk stopped on t1's first row");
        Ok(())
    })
    .unwrap();
}

#[test]
fn a_worker_s_leases_come_back_in_pid_and_group_order() {
    let t = TempStore::new();
    let s = t.s();
    {
        let mut w = s.write().unwrap();
        w.put_lease("w1", 9, "gb", 100).unwrap();
        w.put_lease("w1", 9, "ga", 200).unwrap();
        w.put_lease("w1", 2, "gz", 300).unwrap();
        w.put_lease("w2", 1, "g", 400).unwrap();
        w.commit().unwrap();
    }
    s.read(|r| {
        let mut got = Vec::new();
        r.scan_worker_leases("w1", 100, &mut |pid, g, at| {
            got.push((pid, g.to_string(), at));
            true
        })?;
        assert_eq!(
            got,
            vec![
                (2, "gz".to_string(), 300),
                (9, "ga".to_string(), 200),
                (9, "gb".to_string(), 100),
            ],
            "log_renew_lease_v1 walks this index, never a hash map"
        );
        Ok(())
    })
    .unwrap();
}

#[test]
fn delete_range_is_bounded_and_resumes_where_it_stopped() {
    let t = TempStore::new();
    let s = t.s();
    {
        let mut w = s.write().unwrap();
        for pid in 0..10u64 {
            w.put_raw(
                Keyspace::Dedup,
                &keys::dedup(1, &[pid as u8; 16]),
                &[0u8; 16],
            )
            .unwrap();
        }
        w.put_raw(Keyspace::Dedup, &keys::dedup(2, &[0u8; 16]), &[0u8; 16])
            .unwrap();
        w.commit().unwrap();

        let prefix = keys::dedup_prefix(1);
        let (n, resume) = w
            .delete_range(Keyspace::Dedup, &prefix, &prefix, 4)
            .unwrap();
        assert_eq!(n, 4);
        let resume = resume.expect("more to do");
        let (n2, resume2) = w
            .delete_range(Keyspace::Dedup, &resume, &prefix, 4)
            .unwrap();
        assert_eq!(n2, 4);
        let (n3, resume3) = w
            .delete_range(Keyspace::Dedup, &resume2.unwrap(), &prefix, 4)
            .unwrap();
        assert_eq!(n3, 2);
        assert!(resume3.is_none(), "the range is exhausted");
        w.commit().unwrap();
    }
    s.read(|r| {
        assert_eq!(r.count(Keyspace::Dedup)?, 1, "pid 2 was never in the range");
        Ok(())
    })
    .unwrap();
}

#[test]
fn an_install_clears_every_row_that_names_a_local_file() {
    // §11.6 / D8 / I7: the receiver of a snapshot keeps NO row naming the
    // sender's files. Every keyspace whose key or value carries a `file_id` or
    // a position — `seg_loc`, `files` and `partition_files` — has to go, and
    // the replicated rows have to stay. `partition_files` is the one the plan
    // is ambiguous about (§6.1 lists it as replicated, §6.2/§11.7/I7 make its
    // key node-local); this test is what a wrong label breaks.
    let t = TempStore::new();
    let s = t.s();
    let part = PartitionRow::new(samples::uuid(1), "t", "q", "p", 1);
    let cur = a_cursor(3);
    {
        let mut w = s.write().unwrap();
        w.create_partition(1, &part).unwrap();
        w.put_cursor(1, "g", &cur).unwrap();
        w.put_seg_loc(
            1,
            0,
            &SegLocRow {
                bucket: 1,
                file_id: 1,
                offset: 0,
                len: 8,
            },
        )
        .unwrap();
        w.put_file(
            1,
            1,
            &FileRow {
                len: 8,
                durable_len: 8,
                sealed: true,
                frames: 1,
                retained_frames: 1,
                retained_bytes: 8,
                window_frames: 1,
                snapshot_refs: 0,
            },
        )
        .unwrap();
        // The sender's file id for this partition.
        w.put_partition_file(1, 1).unwrap();
        w.commit().unwrap();
        // §11.6: an install clears this node's positions before rebuilding
        // them from its own files (D8, I7).
        w.clear_node_local().unwrap();
        w.commit().unwrap();
    }
    s.read(|r| {
        for ks in Keyspace::ALL {
            let n = r.count(ks)?;
            if ks.scope() == Scope::NodeLocal {
                assert_eq!(n, 0, "{} survived the clear", ks.name());
            }
        }
        // Named explicitly, so that moving `partition_files` back to the
        // replicated scope fails HERE and not at WP-4.6.
        assert_eq!(
            r.count(Keyspace::PartitionFiles)?,
            0,
            "the receiver still names the sender's files"
        );
        assert_eq!(r.partition(1)?.as_ref(), Some(&part));
        assert_eq!(r.cursor(1, "g")?.as_ref(), Some(&cur));
        Ok(())
    })
    .unwrap();
}

#[test]
fn contending_writers_are_refused_or_served_never_stuck() {
    // The same rule under contention: every caller comes back — with a handle
    // or with WriterBusy — and what the served ones wrote is all there. If the
    // guard released the write right before LMDB's writer mutex were free, a
    // caller would block on it with no deadline instead (I15).
    let t = TempStore::new();
    let s = t.s();
    let served = AtomicU64::new(0);
    let refused = AtomicU64::new(0);
    let (sv, rf) = (&served, &refused);
    std::thread::scope(|sc| {
        for th in 0..8u64 {
            sc.spawn(move || {
                for i in 0..200u64 {
                    match s.write() {
                        Ok(mut w) => {
                            let n = sv.fetch_add(1, Ordering::SeqCst);
                            w.set_counter(&keys::counter_partition(n, Counter::Pushed), n as i64)
                                .unwrap();
                            w.commit().unwrap();
                        }
                        Err(StoreError::WriterBusy) => {
                            rf.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(e) => panic!("thread {th} iteration {i}: {e}"),
                    }
                }
            });
        }
    });
    let served = served.load(Ordering::SeqCst);
    assert_eq!(served + refused.load(Ordering::SeqCst), 8 * 200);
    assert!(served > 0, "nobody got the writer");
    s.read(|r| {
        assert_eq!(
            r.count(Keyspace::Counters)?,
            served,
            "a served write was lost"
        );
        Ok(())
    })
    .unwrap();
    assert_eq!(
        StoreMetrics::get(&s.metrics().writer_busy),
        refused.load(Ordering::SeqCst)
    );
}

#[test]
fn a_second_write_handle_is_refused_not_queued() {
    // I1: the apply thread is the only writer. I15: nothing blocks without a
    // deadline. LMDB queues writers on a process-shared mutex with no timeout,
    // so a second caller must be refused BEFORE it reaches the engine — a
    // blocked one would never come back, and on a tokio worker it would take
    // the worker with it.
    let t = TempStore::new();
    let s = t.s();
    let mut w = s.write().unwrap();

    // Another thread, as a handler or a loop would be.
    let (res, took) = std::thread::scope(|sc| {
        sc.spawn(|| {
            let start = std::time::Instant::now();
            let r = s.write().map(|_| ());
            (r, start.elapsed())
        })
        .join()
        .expect("the second writer thread panicked")
    });
    assert_eq!(res, Err(StoreError::WriterBusy));
    assert!(
        took < std::time::Duration::from_secs(5),
        "the second writer waited {took:?} instead of being refused"
    );
    assert!(
        StoreError::WriterBusy.retryable(),
        "a busy writer is a retry, not a failure"
    );
    assert_eq!(StoreMetrics::get(&s.metrics().writer_busy), 1);

    // The same thread is refused too: it would deadlock against itself.
    assert_eq!(s.write().map(|_| ()), Err(StoreError::WriterBusy));

    // The right comes back when the handle goes, and a commit keeps it.
    w.put_queue("t", "q", &samples::queue_config()).unwrap();
    w.commit().unwrap();
    assert_eq!(s.write().map(|_| ()), Err(StoreError::WriterBusy));
    drop(w);
    let w2 = s.write().expect("the writer was released");
    drop(w2);
    let w3 = s.write().expect("still released after a drop");
    drop(w3);
}

// ---------------------------------------------------------------------------
// Isolation
// ---------------------------------------------------------------------------

#[test]
fn a_reader_sees_the_last_committed_transaction_and_not_the_open_one() {
    let t = TempStore::new();
    let s = t.s();
    let v1 = samples::queue_config();
    let mut v2 = v1.clone();
    v2.priority = 99;

    {
        let mut w = s.write().unwrap();
        w.put_queue("t", "q", &v1).unwrap();
        w.commit().unwrap();
    }

    // Hold an OPEN write transaction with an uncommitted change…
    let mut w = s.write().unwrap();
    w.put_queue("t", "q", &v2).unwrap();
    w.put_queue("t", "q2", &v2).unwrap();

    // …and read from another thread, which is where a local read runs
    // (spawn_blocking, §9.4). It must see v1 and no q2.
    std::thread::scope(|scope| {
        scope.spawn(|| {
            s.read(|r| {
                assert_eq!(r.queue("t", "q")?.unwrap().priority, v1.priority);
                assert!(r.queue("t", "q2")?.is_none());
                Ok(())
            })
            .unwrap();
        });
    });

    // The WRITER sees its own writes: apply must read what it has just
    // written in this entry (a counter, a dedup occurrence list).
    assert_eq!(w.queue("t", "q").unwrap().unwrap().priority, v2.priority);

    w.commit().unwrap();

    std::thread::scope(|scope| {
        scope.spawn(|| {
            s.read(|r| {
                assert_eq!(r.queue("t", "q")?.unwrap().priority, v2.priority);
                assert!(r.queue("t", "q2")?.is_some());
                Ok(())
            })
            .unwrap();
        });
    });
}

#[test]
fn an_aborted_transaction_leaves_nothing_behind() {
    let t = TempStore::new();
    let s = t.s();
    {
        let mut w = s.write().unwrap();
        w.put_queue("t", "q", &samples::queue_config()).unwrap();
        w.abort().unwrap();
        // The handle is usable again: abort starts the next transaction.
        w.set_applied(1, 1).unwrap();
        w.commit().unwrap();
    }
    s.read(|r| {
        assert!(r.queue("t", "q")?.is_none());
        assert_eq!(r.applied_index()?, 1);
        Ok(())
    })
    .unwrap();
}

// ---------------------------------------------------------------------------
// The pins
// ---------------------------------------------------------------------------

/// PIN 2. A second read transaction on one thread is refused by construction,
/// before LMDB is asked (where it would be `MDB_BAD_RSLOT`).
#[test]
fn a_second_read_on_one_thread_is_impossible() {
    let t = TempStore::new();
    let s = t.s();
    let err = s
        .read(|_outer| {
            // The handle cannot escape the closure (its lifetime is the
            // call's), so the only way to get a second one is to ask again —
            // and that is what this refuses.
            s.read(|_inner| Ok(()))
        })
        .unwrap_err();
    assert_eq!(err, StoreError::NestedRead);
    assert!(err.retryable());
    assert_eq!(StoreMetrics::get(&s.metrics().nested_read), 1);

    // The flag is per-thread and is cleared on the way out: the next read on
    // this thread works, and another thread was never blocked.
    s.read(|r| {
        assert_eq!(r.applied_index()?, 0);
        Ok(())
    })
    .unwrap();
    std::thread::scope(|scope| {
        scope.spawn(|| s.read(|_r| Ok(())).unwrap());
    });
}

/// PIN 2, the other half: the guard survives a panic inside the closure, so a
/// tokio blocking worker is not poisoned for every later read.
#[test]
fn a_panic_inside_a_read_does_not_poison_the_thread() {
    let t = TempStore::new();
    let s = t.s();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = s.read(|_r| -> crate::rsm::store::Result<()> { panic!("boom") });
    }));
    assert!(panicked.is_err());
    s.read(|_r| Ok(())).unwrap();
}

/// PIN 3. `max_readers` is a ceiling and `MDB_READERS_FULL` is a RETRYABLE
/// refusal, not a panic.
#[test]
fn running_out_of_reader_slots_is_a_refusal() {
    let t = TempStore::with(StoreOpts {
        map_bytes: Some(16 << 20),
        max_readers: 2,
        ..Default::default()
    });
    let s = t.s();
    assert!(s.map_usage().max_readers >= 2);

    let (tx, rx) = std::sync::mpsc::channel::<std::result::Result<(), StoreError>>();
    let (rel_tx, rel_rx) = std::sync::mpsc::channel::<()>();
    let rel_rx = std::sync::Mutex::new(rel_rx);
    let n = 4usize;
    let mut full = 0;
    std::thread::scope(|scope| {
        for _ in 0..n {
            let tx = tx.clone();
            let rel_rx = &rel_rx;
            scope.spawn(move || {
                let r = s.read(|_h| {
                    // Say so BEFORE blocking, so the parent always gets n
                    // messages whatever happens.
                    tx.send(Ok(())).unwrap();
                    let _ = rel_rx.lock().unwrap().recv();
                    Ok(())
                });
                if let Err(e) = r {
                    tx.send(Err(e)).unwrap();
                }
            });
        }
        for _ in 0..n {
            match rx.recv_timeout(std::time::Duration::from_secs(20)).unwrap() {
                Ok(()) => {}
                Err(StoreError::ReadersFull { max_readers }) => {
                    assert!(max_readers >= 2);
                    full += 1;
                }
                Err(other) => panic!("unexpected {other}"),
            }
        }
        for _ in 0..n {
            let _ = rel_tx.send(());
        }
    });
    assert!(full > 0, "{n} concurrent readers over a 2-slot table");
    assert_eq!(StoreMetrics::get(&s.metrics().readers_full), full as u64);
}

/// PIN 4. `MDB_MAP_FULL` is a typed error with a metric, and it is FATAL for
/// this node: a node-local liveness cliff the leader cannot see (§11.8, R-18).
#[test]
fn a_full_map_is_a_typed_error_and_a_metric() {
    let t = TempStore::with(StoreOpts {
        // One megabyte: small enough to fill in a fraction of a second.
        map_bytes: Some(1 << 20),
        ..Default::default()
    });
    let s = t.s();
    let usage = s.map_usage();
    assert!(usage.map_bytes <= (2 << 20), "{usage:?}");
    assert!(usage.pct() < 100.0);

    let mut w = s.write().unwrap();
    let mut hit: Option<StoreError> = None;
    let val = vec![7u8; 4096];
    for i in 0..10_000u64 {
        let k = keys::dedup(i, &[0u8; 16]);
        match w.put_raw(Keyspace::Dedup, &k, &val) {
            Ok(()) => {}
            Err(e) => {
                hit = Some(e);
                break;
            }
        }
    }
    let err = hit.expect("a 1 MiB map does not hold 40 MiB");
    match err {
        StoreError::MapFull {
            used_bytes,
            map_bytes,
        } => {
            assert!(map_bytes > 0);
            assert!(used_bytes <= map_bytes);
        }
        other => panic!("expected MapFull, got {other}"),
    }
    assert!(
        err.fatal(),
        "the node stops; it does not retry into the wall"
    );
    assert!(!err.retryable());
    assert_eq!(StoreMetrics::get(&s.metrics().map_full), 1);
    // Nothing is lost: the Raft log is the write-ahead log, so the aborted
    // transaction is re-applied after a restart with a larger map.
    drop(w);
}

#[test]
fn map_usage_is_what_status_reports() {
    let t = TempStore::with(StoreOpts {
        map_bytes: Some(8 << 20),
        ..Default::default()
    });
    let s = t.s();
    let before = s.map_usage();
    {
        let mut w = s.write().unwrap();
        for i in 0..200u64 {
            w.put_raw(Keyspace::Dedup, &keys::dedup(i, &[1u8; 16]), &[9u8; 512])
                .unwrap();
        }
        w.commit().unwrap();
    }
    let after = s.map_usage();
    assert!(after.used_bytes > before.used_bytes);
    assert_eq!(after.map_bytes, before.map_bytes);
    assert!(after.pct() > 0.0 && after.pct() < 100.0);
    assert!(!after.over_high_water(), "{after:?}");
    let full = MapUsage {
        map_bytes: 100,
        used_bytes: 90,
        readers_in_use: 0,
        max_readers: 4,
    };
    assert!(full.over_high_water(), "the §11.8 gate is 85%");
}

/// A key over LMDB's limit is a refusal the planner can turn into a 4xx, not a
/// truncation and not a panic. A `(tenant, queue, group)` key of three long
/// names reaches it, and the postgres schema does not bound `consumer_group`.
#[test]
fn an_over_long_key_is_refused_with_its_numbers() {
    let t = TempStore::new();
    let s = t.s();
    let max = s.max_key_len();
    assert!(max >= 511, "LMDB's default is 511 B, got {max}");
    let long = "x".repeat(max);
    let mut w = s.write().unwrap();
    let k = keys::groups("t", "q", &long);
    let err = w.put_raw(Keyspace::Groups, &k, b"v").unwrap_err();
    match err {
        StoreError::KeyTooLong {
            keyspace,
            len,
            max: m,
        } => {
            assert_eq!(keyspace, "groups");
            assert!(len > m);
            assert_eq!(m, max);
        }
        other => panic!("expected KeyTooLong, got {other}"),
    }
    assert_eq!(StoreMetrics::get(&s.metrics().key_too_long), 1);
    // A key at the limit is accepted.
    // Two escaped names of one byte cost 3 bytes each, the third costs
    // len + 2: the key is exactly `max` bytes.
    let ok = "y".repeat(max - 8);
    let k = keys::groups("t", "q", &ok);
    assert_eq!(k.len(), max);
    w.put_raw(Keyspace::Groups, &k, b"v").unwrap();
}

// ---------------------------------------------------------------------------
// Reopen
// ---------------------------------------------------------------------------

/// NOT a durability check, and it must not be read as one: a clean reopen sees
/// a plain `commit()` exactly as it sees a `durable_commit()`, because the
/// bytes never left the page cache either way. What it does check is the shape
/// of the call — the transaction ends, the next one opens, the rows are
/// readable after the environment is closed and opened again, and the durable
/// commit is counted separately from the ordinary one (§11.3 vs §11.4). The
/// check that can tell the two apart is the dropped-unflushed-writes run on
/// the Linux VM (WP-1.8; see `store_crash.rs`).
#[test]
fn a_commit_and_a_durable_commit_both_survive_a_clean_reopen() {
    let mut t = TempStore::new();
    {
        let s = t.s();
        let mut w = s.write().unwrap();
        // The ordinary commit first: on a clean reopen it is indistinguishable
        // from the durable one below.
        w.put_queue("t", "q0", &samples::queue_config()).unwrap();
        w.commit().unwrap();
        w.put_queue("t", "q", &samples::queue_config()).unwrap();
        w.set_applied(7, 1).unwrap();
        w.set_meta_u64(crate::rsm::store::meta::DURABLE_INDEX, 7)
            .unwrap();
        w.durable_commit().unwrap();
        assert_eq!(StoreMetrics::get(&s.metrics().durable_commits), 1);
        assert_eq!(StoreMetrics::get(&s.metrics().commits), 1);
    }
    t.reopen(StoreOpts {
        map_bytes: Some(64 << 20),
        ..Default::default()
    });
    t.s()
        .read(|r| {
            assert_eq!(r.applied_index()?, 7);
            assert_eq!(r.durable_index()?, 7);
            assert!(r.queue("t", "q")?.is_some());
            assert!(r.queue("t", "q0")?.is_some(), "the non-durable commit");
            Ok(())
        })
        .unwrap();
}

// ---------------------------------------------------------------------------
// The durable point that did not happen (§11.4)
// ---------------------------------------------------------------------------

#[test]
fn a_durable_point_whose_sync_fails_is_fatal_and_reports_no_durable_index() {
    // §11.4's durable point has two legs — `mdb_txn_commit` and
    // `mdb_env_sync(env, 1)` — and step 3 reports the durable index to the
    // replicator only when BOTH happened. A reported durable index bounds
    // recovery replay and lets `LocalReplicator` truncate its log behind it,
    // so a durable point that never reached the platter turns the next crash
    // into acknowledged effects no replay can bring back (I4, I11).
    //
    // On Linux the kernel CONSUMES an fsync error and drops the dirty pages,
    // so the next sync succeeds and reports nothing: the return value below is
    // the only notice there will ever be. A taxonomy in which it is neither
    // retryable nor fatal is a taxonomy that says "carry on".
    //
    // The sync failure is INJECTED, and what this test proves is the
    // CLASSIFICATION — that a durable point which did not happen is refused,
    // named and fatal. It proves nothing about whether a durable point that
    // returns `Ok` reached the platter; only the dropped-unflushed-writes run
    // on the Linux VM can (WP-1.8, D-01/D-02), as `store_crash.rs` says at its
    // head.
    let t = TempStore::new();
    let s = t.s();
    let mut w = s.write().unwrap();
    w.put_queue("t", "q", &samples::queue_config()).unwrap();
    w.set_applied(9, 1).unwrap();
    w.set_meta_u64(crate::rsm::store::meta::DURABLE_INDEX, 9)
        .unwrap();

    s.fail_next_sync();
    let e = w.durable_commit().unwrap_err();
    match &e {
        StoreError::CommitFailed { durable, detail } => {
            assert!(durable, "the commit that failed was the durable point");
            assert!(detail.contains("EIO"), "the cause is named: {detail}");
        }
        other => panic!("expected CommitFailed, got {other}"),
    }
    assert!(e.fatal(), "a durable point that did not happen is fatal");
    assert!(!e.retryable(), "there is nothing left to retry on");
    assert!(
        e.lost_durable_point(),
        "the caller has to be able to see that step 3 must report nothing"
    );
    assert_eq!(
        StoreMetrics::get(&s.metrics().durable_commits),
        0,
        "a durable point was counted that never happened"
    );
    assert_eq!(StoreMetrics::get(&s.metrics().commit_failed), 1);

    // The handle is DEAD, not quietly reusable: every later call answers the
    // same fatal error instead of a vague "the write transaction is closed",
    // which `fatal()` would read as continuable.
    let again = w
        .put_queue("t", "q2", &samples::queue_config())
        .unwrap_err();
    assert_eq!(again, e);
    assert!(again.fatal());
    assert_eq!(w.commit().unwrap_err(), e);
    assert_eq!(w.abort().unwrap_err(), e);
    assert!(w
        .get_raw(Keyspace::Queues, &keys::queues("t", "q"))
        .is_err());
}

#[test]
fn the_engines_end_of_life_code_is_fatal_not_anonymous() {
    // `MDB_PANIC` is what LMDB answers when "update of meta page failed or
    // environment had fatal error", and every later transaction on that
    // environment answers it too. In the anonymous arm it was neither
    // retryable nor fatal, so the apply thread would have carried on against a
    // store that can no longer commit anything.
    let t = TempStore::new();
    let s = t.s();
    let e = crate::rsm::store::heed_store::err(s, heed::Error::Mdb(heed::MdbError::Panic));
    match &e {
        StoreError::EnvDead { .. } => {}
        other => panic!("MDB_PANIC came back as {other}"),
    }
    assert!(e.fatal());
    assert!(!e.retryable());
}

#[test]
fn every_error_the_store_answers_is_classified_on_purpose() {
    // The classification IS the contract: WP-1.4 has exactly two questions to
    // ask an error — retry, or stop — so a variant that answers no to both is
    // a decision, never an oversight. `Mdb` and `Io` are the two deliberate
    // "the caller decides" cases, and the conditions that used to hide in them
    // (a failed durable point, `MDB_PANIC`) are named variants now.
    let cases: [(StoreError, bool, bool); 10] = [
        (StoreError::NestedRead, true, false),
        (StoreError::ReadersFull { max_readers: 4 }, true, false),
        (StoreError::WriterBusy, true, false),
        (
            StoreError::MapFull {
                used_bytes: 1,
                map_bytes: 2,
            },
            false,
            true,
        ),
        (
            StoreError::Corrupt {
                keyspace: "queues",
                detail: "x".into(),
            },
            false,
            true,
        ),
        (
            StoreError::CommitFailed {
                durable: true,
                detail: "x".into(),
            },
            false,
            true,
        ),
        (
            StoreError::CommitFailed {
                durable: false,
                detail: "x".into(),
            },
            false,
            true,
        ),
        (StoreError::EnvDead { detail: "x".into() }, false, true),
        // A 4xx refusal for the planner, and never a truncation.
        (
            StoreError::KeyTooLong {
                keyspace: "groups",
                len: 512,
                max: 511,
            },
            false,
            false,
        ),
        (StoreError::Io("x".into()), false, false),
    ];
    for (e, retryable, fatal) in cases {
        assert_eq!(e.retryable(), retryable, "retryable of {e}");
        assert_eq!(e.fatal(), fatal, "fatal of {e}");
        assert!(!(retryable && fatal), "{e} is both");
        assert_eq!(
            e.lost_durable_point(),
            matches!(e, StoreError::CommitFailed { durable: true, .. })
        );
    }
}

// ---------------------------------------------------------------------------
// Keys at the engine's limit: chunked scans and deletes
// ---------------------------------------------------------------------------

/// A `groups` key of EXACTLY the engine's limit. `(tenant, queue, group)` is
/// three unbounded names in the postgres schema
/// (`consumer_groups_metadata.consumer_group` is `TEXT`), so 511 B is reached
/// by one long consumer group name.
fn group_name_at_the_limit(max: usize, suffix: char) -> String {
    let mut g = "y".repeat(max - 9);
    g.push(suffix);
    g
}

#[test]
fn a_delete_chunk_resumes_past_keys_at_the_engine_s_limit() {
    // `DeleteChunk` (§5.2) walks a keyspace in bounded chunks, each starting
    // where the last stopped. With `last ‖ 0x00` as the resume key, a keyspace
    // whose keys can reach the limit refused every chunk after the first with
    // `KeyTooLong` — which is neither retryable nor fatal, so the delete could
    // never finish and the rows would leak for good.
    let t = TempStore::new();
    let s = t.s();
    let max = s.max_key_len();
    let names: Vec<String> = ['a', 'b', 'c']
        .into_iter()
        .map(|c| group_name_at_the_limit(max, c))
        .collect();
    let mut w = s.write().unwrap();
    for g in &names {
        let k = keys::groups("t", "q", g);
        assert_eq!(k.len(), max, "the key must sit exactly at the limit");
        w.put_raw(Keyspace::Groups, &k, b"v").unwrap();
    }
    w.commit().unwrap();

    let prefix = keys::groups_prefix("t", "q");
    let mut from = prefix.clone();
    let mut deleted = 0usize;
    for chunk in 0..8 {
        let (n, resume) = w
            .delete_range(Keyspace::Groups, &from, &prefix, 1)
            .unwrap_or_else(|e| panic!("chunk {chunk}: {e}"));
        deleted += n;
        match resume {
            Some(k) => {
                assert!(
                    k.len() <= max,
                    "the resume key is {} B, over the {max} B limit",
                    k.len()
                );
                from = k;
            }
            None => break,
        }
    }
    assert_eq!(deleted, 3, "the chunk loop never finished");
    w.commit().unwrap();
    s.read(|r| {
        assert_eq!(r.count(Keyspace::Groups)?, 0);
        Ok(())
    })
    .unwrap();
}

#[test]
fn the_ready_rings_rebuild_over_a_chunk_boundary_of_max_length_keys() {
    // §11.5 step 4 rebuilds the rings from `pending` in chunks of
    // `REBUILD_CHUNK`. `pending` is `(tenant, queue, group, pid)` — the same
    // three unbounded names — and Queen routinely runs tens of thousands of
    // partitions per queue, so a group whose names total the limit with more
    // than one chunk of pending partitions is an ordinary state, not an edge:
    // with a resume key one byte over the limit the rebuild could never
    // finish, and the node could never serve.
    let t = TempStore::new();
    let s = t.s();
    let max = s.max_key_len();
    // name(t) + name(q) + name(g) + 8 = 3 + 3 + (len + 2) + 8.
    let g = "g".repeat(max - 16);
    let n = crate::rsm::state::REBUILD_CHUNK + 4;
    {
        let mut w = s.write().unwrap();
        for pid in 0..n as u64 {
            let k = keys::pending("t", "q", &g, pid);
            assert_eq!(k.len(), max);
            w.put_pending("t", "q", &g, pid, 100).unwrap();
        }
        w.commit().unwrap();
    }
    let d = s.read(|r| Derived::rebuild(r, 1_000)).unwrap();
    assert_eq!(d.pending_rows(), n as u64, "the rebuild stopped early");
    assert_eq!(d.rings_len(), 1);
    assert_eq!(d.ring("t", "q", &g).unwrap().live_len(), n);
}

// ---------------------------------------------------------------------------
// Dedup (D10 option (a), lean) over a real store
// ---------------------------------------------------------------------------

const WINDOW: i64 = 3_600_000_000; // 1 h in µs, the product default

fn h(n: u8) -> [u8; 16] {
    [n; 16]
}

#[test]
fn a_recorded_hash_probes_as_a_duplicate_at_its_original_offset() {
    let t = TempStore::new();
    let s = t.s();
    {
        let mut w = s.write().unwrap();
        dedup::record(&mut w, 1, 0, 1, &[(h(1), 0), (h(2), 1)], 1_000).unwrap();
        w.commit().unwrap();
    }
    s.read(|r| {
        let mut out = Vec::new();
        dedup::probe(r, 1, &[h(1), h(2), h(3)], 2_000, WINDOW, &mut out)?;
        assert_eq!(out, vec![Some(0), Some(1), None]);
        // Another partition does not see them: the key is (pid, hash).
        dedup::probe(r, 2, &[h(1)], 2_000, WINDOW, &mut out)?;
        assert_eq!(out, vec![None]);
        // Past the window, the bytes are still there and the verdict is "new".
        dedup::probe(r, 1, &[h(1)], 1_000 + WINDOW + 1, WINDOW, &mut out)?;
        assert_eq!(out, vec![None]);
        Ok(())
    })
    .unwrap();
}

#[test]
fn a_repeated_hash_keeps_every_occurrence_and_the_probe_takes_the_first() {
    let t = TempStore::new();
    let s = t.s();
    {
        let mut w = s.write().unwrap();
        dedup::record(&mut w, 1, 0, 0, &[(h(1), 0)], 1_000).unwrap();
        // 005 allows a hash to appear more than once (5 591 of 17.6 M rows in
        // S2's cell): the row is a LIST, and the probe answers MIN.
        dedup::record(&mut w, 1, 5, 5, &[(h(1), 5)], 2_000).unwrap();
        w.commit().unwrap();
    }
    s.read(|r| {
        let mut out = Vec::new();
        dedup::probe(r, 1, &[h(1)], 3_000, WINDOW, &mut out)?;
        assert_eq!(out, vec![Some(0)]);
        Ok(())
    })
    .unwrap();
}

#[test]
fn ack_by_hash_answers_both_legs_of_005() {
    let t = TempStore::new();
    let s = t.s();
    {
        let mut w = s.write().unwrap();
        dedup::record(&mut w, 1, 0, 2, &[(h(1), 0), (h(2), 1), (h(1), 2)], 1_000).unwrap();
        w.commit().unwrap();
    }
    s.read(|r| {
        // committed = 0, the leased batch is (0, 2]: h(1) occurs at 0 (below
        // the cursor) and at 2 (inside the batch).
        let res = dedup::resolve(r, 1, &h(1), 1, 2, 0)?;
        assert_eq!(res.eff, Some(2));
        assert!(res.below, "below-cursor honesty");
        // h(2) is inside the batch and never below it.
        let res = dedup::resolve(r, 1, &h(2), 1, 2, 0)?;
        assert_eq!(res.eff, Some(1));
        assert!(!res.below);
        // An unknown hash resolves to nothing at all (a `stale` in 005's
        // vocabulary).
        let res = dedup::resolve(r, 1, &h(9), 1, 2, 0)?;
        assert_eq!(res, dedup::AckRes::default());
        Ok(())
    })
    .unwrap();
}

#[test]
fn pruning_walks_the_txns_rows_and_is_bounded() {
    let t = TempStore::new();
    let s = t.s();
    {
        let mut w = s.write().unwrap();
        for i in 0..6u64 {
            let hash = h(i as u8 + 1);
            dedup::record(&mut w, 1, i, i, &[(hash, i)], 1_000 + i as i64).unwrap();
        }
        w.commit().unwrap();
    }
    {
        let mut w = s.write().unwrap();
        // A budget of two appends at a cutoff that covers four of them.
        let step = dedup::prune(&mut w, 1, 0, 1_004, 2).unwrap();
        assert_eq!(step.appends, 2);
        assert_eq!(step.rows_deleted, 2);
        assert_eq!(step.txns_start, 2);
        assert!(!step.exhausted, "the budget ran out, not the cutoff");

        let step = dedup::prune(&mut w, 1, step.txns_start, 1_004, 10).unwrap();
        assert_eq!(step.appends, 2, "offsets 2 and 3; 4 is at the cutoff");
        assert_eq!(step.txns_start, 4);
        assert!(step.exhausted);
        w.commit().unwrap();
    }
    s.read(|r| {
        let mut out = Vec::new();
        dedup::probe(r, 1, &[h(1), h(5), h(6)], 1_005, WINDOW, &mut out)?;
        assert_eq!(
            out,
            vec![None, Some(4), Some(5)],
            "only the pruned are gone"
        );
        assert_eq!(r.count(Keyspace::Txns)?, 2);
        assert_eq!(r.count(Keyspace::Dedup)?, 2);
        Ok(())
    })
    .unwrap();
}

/// The case D10 and WP-0.4 both name: retention has deleted the SEGMENT, and
/// the hash list is still inside the txns window. A re-push must still be a
/// duplicate and an ack-by-hash below the cursor must still resolve.
#[test]
fn hash_lists_outlive_the_segments_retention_deleted() {
    let t = TempStore::new();
    let s = t.s();
    let mut part = PartitionRow::new(samples::uuid(1), "t", "q", "p", 500);
    {
        let mut w = s.write().unwrap();
        dedup::record(&mut w, 1, 0, 1, &[(h(1), 0), (h(2), 1)], 1_000).unwrap();
        part.last_offset = 1;
        // Retention moved log_start past both offsets; txns_start did not
        // move, which is the whole of §11.7's rule.
        part.log_start = 2;
        part.txns_start = 0;
        w.create_partition(1, &part).unwrap();
        w.commit().unwrap();
    }
    s.read(|r| {
        let p = r.partition(1)?.unwrap();
        assert!(
            p.txns_start < p.log_start,
            "the window outlives the payload"
        );
        let mut out = Vec::new();
        dedup::probe(r, 1, &[h(1)], 2_000, WINDOW, &mut out)?;
        assert_eq!(out, vec![Some(0)], "a re-push is still a duplicate");
        let res = dedup::resolve(r, 1, &h(1), p.txns_start, 1, 1)?;
        assert!(res.below, "an ack below the cursor still resolves");
        Ok(())
    })
    .unwrap();
}

#[test]
fn deleting_a_partition_s_dedup_data_is_chunked() {
    let t = TempStore::new();
    let s = t.s();
    {
        let mut w = s.write().unwrap();
        for i in 0..8u64 {
            dedup::record(&mut w, 1, i, i, &[(h(i as u8), i)], 1_000).unwrap();
        }
        dedup::record(&mut w, 2, 0, 0, &[(h(9), 0)], 1_000).unwrap();
        w.commit().unwrap();

        let mut resume: Vec<u8> = Vec::new();
        let mut rounds = 0;
        loop {
            let (n, next) = dedup::delete_partition_chunk(&mut w, 1, &resume, 3).unwrap();
            rounds += 1;
            assert!(rounds < 20, "the chunks did not converge");
            match next {
                Some(r) if n > 0 => resume = r,
                _ => break,
            }
        }
        w.commit().unwrap();
    }
    s.read(|r| {
        assert_eq!(r.count(Keyspace::Dedup)?, 1, "pid 2 survives");
        assert_eq!(r.count(Keyspace::Txns)?, 1);
        Ok(())
    })
    .unwrap();
}

#[test]
fn a_corrupt_occurrence_list_is_fatal_not_guessed() {
    let t = TempStore::new();
    let s = t.s();
    {
        let mut w = s.write().unwrap();
        w.put_raw(Keyspace::Dedup, &keys::dedup(1, &h(1)), &[0u8; 7])
            .unwrap();
        w.commit().unwrap();
    }
    s.read(|r| {
        let err = dedup::probe_one(r, 1, &h(1), 0).unwrap_err();
        assert!(err.fatal(), "{err}");
        Ok(())
    })
    .unwrap();
}

// ---------------------------------------------------------------------------
// The derived indexes (§6.3)
// ---------------------------------------------------------------------------

#[test]
fn the_ready_rings_rebuild_from_pending_and_the_leases_from_their_index() {
    let t = TempStore::new();
    let s = t.s();
    {
        let mut w = s.write().unwrap();
        w.put_pending("t", "q", "g1", 1, 100).unwrap();
        w.put_pending("t", "q", "g1", 2, 5_000).unwrap();
        w.put_pending("t", "q", "g2", 1, 100).unwrap();
        w.put_pending("t2", "q", "g1", 7, 100).unwrap();
        w.put_lease("w1", 1, "g1", 9_000).unwrap();
        w.put_lease("w2", 2, "g1", 1_000).unwrap();
        w.commit().unwrap();
    }
    let d = s.read(|r| Derived::rebuild(r, 1_000)).unwrap();
    assert_eq!(d.rings_len(), 3, "one per (tenant, queue, group)");
    assert_eq!(d.pending_rows(), 4);
    let ring = d.ring("t", "q", "g1").unwrap();
    assert!(ring.contains(1), "ready_at 100 <= now 1000");
    assert!(!ring.contains(2), "ready_at 5000 is deferred");
    assert_eq!(ring.next_deadline(), Some(5_000));
    assert_eq!(d.lease_count(), 2);
    assert_eq!(d.next_lease_deadline(), Some(1_000));
    assert_eq!(d.expired_leases(1_000, 10).len(), 1);

    // The planner walks the ring without changing it (I1).
    s.read(|r| {
        let v = crate::rsm::state::Committed::new(r, &d);
        let mut seen = Vec::new();
        v.candidates("t", "q", "g1", 10, &mut |pid| {
            seen.push(pid);
            true
        });
        assert_eq!(seen, vec![1]);
        Ok(())
    })
    .unwrap();
    assert!(
        d.ring("t", "q", "g1").unwrap().contains(1),
        "the walk consumed nothing"
    );
}

#[test]
fn the_committed_view_hides_garbage_pids_and_stamps_a_monotone_now() {
    let t = TempStore::new();
    let s = t.s();
    let part = PartitionRow::new(samples::uuid(1), "t", "q", "p", 1_000);
    {
        let mut w = s.write().unwrap();
        w.create_partition(1, &part).unwrap();
        w.set_meta_i64(crate::rsm::store::meta::LAST_NOW_US, 9_000)
            .unwrap();
        w.set_meta_i64(crate::rsm::store::meta::MAX_CREATED_AT_US, 9_500)
            .unwrap();
        w.commit().unwrap();
    }
    let d = Derived::default();
    s.read(|r| {
        let v = crate::rsm::state::Committed::new(r, &d);
        assert_eq!(v.pid_of("t", "q", "p")?, Some(1));
        assert!(v.partition(1)?.is_some());
        // D5/I5: never below the last committed now, never below the highest
        // created_at, and the wall clock is the CALLER's (no clock in here).
        assert_eq!(v.plan_now(0)?, 9_501);
        assert_eq!(v.plan_now(20_000)?, 20_000);
        Ok(())
    })
    .unwrap();

    {
        let mut w = s.write().unwrap();
        w.put_garbage(
            1,
            &GarbageRow {
                deleted_at_us: 1,
                scope: GarbageScope::Queue,
                queue_id: None,
                resume: Vec::new(),
            },
        )
        .unwrap();
        w.commit().unwrap();
    }
    s.read(|r| {
        let v = crate::rsm::state::Committed::new(r, &d);
        // §5.2: readers and planners ignore garbage pids, and the NAME is
        // reusable at once.
        assert_eq!(v.pid_of("t", "q", "p")?, None);
        assert!(v.partition(1)?.is_none());
        assert!(v.garbage(1)?.is_some());
        // The raw row is still there for the DeleteChunk loop.
        assert!(r.partition(1)?.is_some());
        Ok(())
    })
    .unwrap();
}

// ---------------------------------------------------------------------------
// Measurement (not a gate: run it by name)
// ---------------------------------------------------------------------------

/// Write amplification and commit cost over 10 000 entries in the §11.3
/// shape. LAPTOP ONLY, and therefore smoke: §0.3 says numbers to quote come
/// from the Linux VM, because macOS serializes `F_FULLFSYNC`.
///
/// `cargo test -p queen-engine --lib rsm::tests::store::measure -- --ignored --nocapture`
#[test]
#[ignore = "measurement, not a gate"]
fn measure_write_amplification_and_commit_cost() {
    use std::time::Instant;

    const ENTRIES: usize = 10_000;
    const MSGS_PER_ENTRY: usize = 8;
    const COMMIT_ENTRIES: usize = 256; // QUEEN_RAFT_STORE_COMMIT_ENTRIES

    let t = TempStore::with(StoreOpts {
        map_bytes: Some(4 << 30),
        ..Default::default()
    });
    let s = t.s();
    let before_disk = s.disk_bytes();
    let mut commit_us: Vec<u128> = Vec::with_capacity(ENTRIES / COMMIT_ENTRIES + 1);
    let mut durable_us: Vec<u128> = Vec::new();
    let t0 = Instant::now();

    let mut w = s.write().unwrap();
    let mut offset = 0u64;
    for e in 0..ENTRIES {
        let pid = (e % 64) as u64;
        let mut accepted = Vec::with_capacity(MSGS_PER_ENTRY);
        for m in 0..MSGS_PER_ENTRY {
            let mut hash = [0u8; 16];
            hash[0..8].copy_from_slice(&(e as u64).to_le_bytes());
            hash[8] = m as u8;
            accepted.push((hash, offset + m as u64));
        }
        dedup::record(
            &mut w,
            pid,
            offset,
            offset + MSGS_PER_ENTRY as u64 - 1,
            &accepted,
            1_000_000 + e as i64,
        )
        .unwrap();
        let mut part = PartitionRow::new(samples::uuid(1), "t", "q", "p", 1);
        part.last_offset = (offset + MSGS_PER_ENTRY as u64 - 1) as i64;
        w.put_partition(pid, &part).unwrap();
        w.put_cursor(pid, "g", &a_cursor(offset as i64)).unwrap();
        w.put_pending("t", "q", "g", pid, 1_000).unwrap();
        w.put_seg_loc(
            pid,
            offset,
            &SegLocRow {
                bucket: (pid % 256) as u16,
                file_id: 1,
                offset,
                len: 512,
            },
        )
        .unwrap();
        w.set_file_len((pid % 256) as u16, 1, offset + 512).unwrap();
        w.add_counter(&keys::counter_partition(pid, Counter::Pushed), 8)
            .unwrap();
        w.put_request_outcome(&samples::uuid(e as u8), 1_000_000 + e as i64, b"ok")
            .unwrap();
        w.set_applied(e as u64 + 1, 1).unwrap();
        offset += MSGS_PER_ENTRY as u64;

        if (e + 1) % COMMIT_ENTRIES == 0 {
            let c0 = Instant::now();
            w.commit().unwrap();
            commit_us.push(c0.elapsed().as_micros());
        }
        if (e + 1) % (COMMIT_ENTRIES * 8) == 0 {
            let d0 = Instant::now();
            w.durable_commit().unwrap();
            durable_us.push(d0.elapsed().as_micros());
        }
    }
    let d0 = Instant::now();
    w.durable_commit().unwrap();
    durable_us.push(d0.elapsed().as_micros());
    drop(w);
    let elapsed = t0.elapsed();

    let after_disk = s.disk_bytes();
    let logical = StoreMetrics::get(&s.metrics().logical_bytes);
    let rows = StoreMetrics::get(&s.metrics().rows_put);
    let msgs = (ENTRIES * MSGS_PER_ENTRY) as u64;
    commit_us.sort_unstable();
    durable_us.sort_unstable();
    let p = |v: &[u128], q: f64| -> u128 {
        if v.is_empty() {
            return 0;
        }
        v[(((v.len() - 1) as f64) * q) as usize]
    };

    println!("--- WP-1.2 store measurement (macOS laptop, debug build: SMOKE ONLY) ---");
    println!("entries               {ENTRIES} ({msgs} messages, {MSGS_PER_ENTRY}/entry)");
    println!("wall                  {:.2} s", elapsed.as_secs_f64());
    println!(
        "store rows written    {rows} ({:.2}/message)",
        rows as f64 / msgs as f64
    );
    println!(
        "logical bytes         {logical} ({:.1} B/message)",
        logical as f64 / msgs as f64
    );
    println!(
        "data.mdb growth       {} B ({:.1} B/message)",
        after_disk - before_disk,
        (after_disk - before_disk) as f64 / msgs as f64
    );
    println!(
        "file amplification    {:.2}x (data.mdb growth / logical bytes)",
        (after_disk - before_disk) as f64 / logical as f64
    );
    println!(
        "commit ({} entries)  n={} p50 {} µs  p99 {} µs",
        COMMIT_ENTRIES,
        commit_us.len(),
        p(&commit_us, 0.5),
        p(&commit_us, 0.99)
    );
    println!(
        "durable commit        n={} p50 {} µs  p99 {} µs",
        durable_us.len(),
        p(&durable_us, 0.5),
        p(&durable_us, 0.99)
    );
    println!("map usage             {:?}", s.map_usage());
    for (k, v) in s.metrics().snapshot() {
        println!("metric {k:22} {v}");
    }
}
