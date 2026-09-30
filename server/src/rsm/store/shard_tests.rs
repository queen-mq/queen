//! Shard writers ([`super::ShardWrite`]): the concurrent writers of the sharded
//! apply.
//!
//! What is checked: N threads writing DISJOINT keys through shard writers —
//! pid-keyed rows and the name-keyed rows partitions share a keyspace in
//! (`pending`, `leases_by_worker`, `queue_partitions`, `dlq`) alike — leave
//! EXACTLY the state, the dirty set (the checkpoint cut) and the reopened
//! checkpoint that one writer doing the same operations leaves; a shard
//! writer's reads see every writer's rows; it refuses to commit and cuts
//! nothing; the typed helpers are its too; and a cut taken while one is alive
//! is caught in a debug build.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use super::keys::{self, Counter};
use super::rows;
use super::{
    CheckpointCut, HeedStore, Keyspace, Reads, ShardWrite, Store, StoreMetrics, StoreOpts,
    TypedReads, TypedWrites, Writes,
};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Tmp {
    store: Option<HeedStore>,
    dir: PathBuf,
}

impl Tmp {
    fn new(tag: &str) -> Tmp {
        let dir = std::env::temp_dir().join(format!(
            "queen-rsm-shard-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = HeedStore::open(&dir, &opts()).expect("open");
        Tmp {
            store: Some(store),
            dir,
        }
    }
    fn s(&self) -> &HeedStore {
        self.store.as_ref().expect("open")
    }
    fn reopen(&mut self) {
        if let Some(s) = self.store.take() {
            s.close();
        }
        self.store = Some(HeedStore::open(&self.dir, &opts()).expect("reopen"));
    }
}

impl Drop for Tmp {
    fn drop(&mut self) {
        if let Some(s) = self.store.take() {
            s.close();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn opts() -> StoreOpts {
    StoreOpts {
        map_bytes: Some(256 << 20),
        ..Default::default()
    }
}

/// One operation of the workload. Every key it touches embeds its pid, so
/// the operations of different pids touch disjoint keys.
#[derive(Clone, Debug)]
enum Op {
    Put(Keyspace, Vec<u8>, Vec<u8>),
    Del(Keyspace, Vec<u8>),
    /// A read-modify-write of a partition counter (`add_counter`).
    Add(Pid, i64),
    /// A range delete of one partition's prefix.
    DeleteRange(Keyspace, Vec<u8>),
}

type Pid = u64;

fn lcg(x: &mut u64) -> u64 {
    *x = x
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *x >> 17
}

/// A workload over `pids`: per pid, a mix of the rows apply writes for it.
fn workload(pids: &[Pid], per_pid: usize, seed: u64) -> Vec<(Pid, Op)> {
    let mut x = seed;
    let mut ops = Vec::new();
    for round in 0..per_pid {
        for &p in pids {
            let v = |x: &mut u64| {
                let n = (lcg(x) % 300) as usize;
                (0..n).map(|i| (i as u64 ^ *x) as u8).collect::<Vec<u8>>()
            };
            let op = match lcg(&mut x) % 11 {
                0 => Op::Put(Keyspace::Partitions, keys::pid(p), v(&mut x)),
                1 => Op::Put(
                    Keyspace::Cursors,
                    keys::cursors(
                        p,
                        if lcg(&mut x).is_multiple_of(2) {
                            "g1"
                        } else {
                            "g2"
                        },
                    ),
                    v(&mut x),
                ),
                2 => Op::Put(
                    Keyspace::Txns,
                    keys::txns(p, round as u64 * 10 + lcg(&mut x) % 4),
                    v(&mut x),
                ),
                3 => Op::Add(p, (lcg(&mut x) % 100) as i64 - 20),
                // Name-keyed rows of this partition, in keyspaces every
                // partition shares.
                4 => Op::Put(
                    Keyspace::Pending,
                    keys::pending("tenant", "queue", "g1", p),
                    (lcg(&mut x) as i64).to_le_bytes().to_vec(),
                ),
                5 => Op::Put(
                    Keyspace::LeasesByWorker,
                    keys::leases_by_worker("w1", p, "g1"),
                    (lcg(&mut x) as i64).to_le_bytes().to_vec(),
                ),
                6 => Op::Put(
                    Keyspace::QueuePartitions,
                    keys::queue_partitions("tenant", "queue", p),
                    rows::UNIT.to_vec(),
                ),
                7 => {
                    let mut id = [0u8; 16];
                    id[..8].copy_from_slice(&p.to_be_bytes());
                    id[8] = (lcg(&mut x) % 3) as u8;
                    Op::Put(Keyspace::Dlq, keys::dlq("tenant", "queue", &id), v(&mut x))
                }
                8 => Op::Del(
                    Keyspace::Cursors,
                    keys::cursors(
                        p,
                        if lcg(&mut x).is_multiple_of(2) {
                            "g1"
                        } else {
                            "g2"
                        },
                    ),
                ),
                9 => Op::Del(Keyspace::Pending, keys::pending("tenant", "queue", "g1", p)),
                _ => {
                    if lcg(&mut x).is_multiple_of(4) {
                        Op::DeleteRange(Keyspace::Txns, keys::txns_prefix(p))
                    } else {
                        Op::Del(Keyspace::Partitions, keys::pid(p))
                    }
                }
            };
            ops.push((p, op));
        }
    }
    ops
}

fn apply_op<W: Writes + ?Sized>(w: &mut W, op: &Op) {
    match op {
        Op::Put(ks, k, v) => w.put_raw(*ks, k, v).unwrap(),
        Op::Del(ks, k) => {
            w.del_raw(*ks, k).unwrap();
        }
        Op::Add(p, d) => {
            w.add_counter(&keys::counter_partition(*p, Counter::Pushed), *d)
                .unwrap();
        }
        Op::DeleteRange(ks, prefix) => {
            let mut from = prefix.clone();
            loop {
                let (n, next) = w.delete_range(*ks, &from, prefix, 2).unwrap();
                match next {
                    Some(k) if n == 2 => from = k,
                    _ => break,
                }
            }
        }
    }
}

type Image = BTreeMap<(&'static str, Vec<u8>), Vec<u8>>;

/// Every row of every keyspace.
fn image<R: Reads>(r: &R) -> Image {
    let mut out = Image::new();
    for ks in Keyspace::ALL {
        r.scan_raw(ks, &[], &[], usize::MAX, &mut |k, v| {
            out.insert((ks.name(), k.to_vec()), v.to_vec());
            true
        })
        .unwrap();
    }
    out
}

/// A cut's rows: `(keyspace, key) → value` (`None`: a delete).
fn cut_rows(cut: &CheckpointCut) -> HashMap<(&'static str, Vec<u8>), Option<Vec<u8>>> {
    let mut out = HashMap::new();
    for (ks, maps) in &cut.rows {
        for m in maps {
            for (k, v) in m {
                let prev = out.insert((ks.name(), k.to_vec()), v.as_ref().map(|v| v.to_vec()));
                assert!(prev.is_none(), "a key in two dirty sets");
            }
        }
    }
    out
}

#[test]
fn shard_writers_on_disjoint_keys_leave_what_one_writer_leaves() {
    const SHARDS: usize = 6;
    // Pids spread over stripes and block rows; several share a stripe.
    let pids: Vec<Pid> = (0..96u64).map(|i| i * 37 + (i % 5) * 1024).collect();
    let ops = workload(&pids, 40, 7);

    // One writer, in the workload's order.
    let mut one = Tmp::new("one");
    {
        let s = one.s();
        let mut w = s.write().unwrap();
        w.set_applied(10, 1).unwrap();
        w.durable_commit().unwrap();
        for (_, op) in &ops {
            apply_op(&mut w, op);
        }
    }

    // SHARDS threads, each the operations of its pids (`pid % SHARDS`), in
    // the same per-pid order, all at once, with readers scanning throughout.
    let mut many = Tmp::new("many");
    {
        let s = many.s();
        {
            let mut w = s.write().unwrap();
            w.set_applied(10, 1).unwrap();
            w.durable_commit().unwrap();
        }
        let stop = AtomicBool::new(false);
        std::thread::scope(|sc| {
            let readers: Vec<_> = (0..2)
                .map(|i| {
                    let stop = &stop;
                    sc.spawn(move || {
                        let mut scans = 0u64;
                        while !stop.load(Ordering::Relaxed) {
                            s.read(|r| {
                                let ks = if i == 0 {
                                    Keyspace::Cursors
                                } else {
                                    Keyspace::Pending
                                };
                                let mut prev: Option<Vec<u8>> = None;
                                r.scan_raw(ks, &[], &[], usize::MAX, &mut |k, _v| {
                                    if let Some(p) = &prev {
                                        assert!(k > &p[..], "a scan out of order");
                                    }
                                    prev = Some(k.to_vec());
                                    true
                                })?;
                                Ok(())
                            })
                            .unwrap();
                            scans += 1;
                        }
                        scans
                    })
                })
                .collect();
            let writers: Vec<_> = (0..SHARDS)
                .map(|shard| {
                    let mine: Vec<Op> = ops
                        .iter()
                        .filter(|(p, _)| (*p as usize) % SHARDS == shard)
                        .map(|(_, op)| op.clone())
                        .collect();
                    sc.spawn(move || {
                        let mut w = s.shard_writer().unwrap();
                        for op in &mine {
                            apply_op(&mut w, op);
                        }
                        mine.len()
                    })
                })
                .collect();
            let written: usize = writers.into_iter().map(|h| h.join().unwrap()).sum();
            assert_eq!(written, ops.len());
            stop.store(true, Ordering::Relaxed);
            for h in readers {
                h.join().unwrap();
            }
        });
        assert_eq!(s.shard_writers(), 0, "every shard writer dropped");
    }

    // The same rows, live.
    let a = one.s().read(|r| Ok(image(r))).unwrap();
    let b = many.s().read(|r| Ok(image(r))).unwrap();
    assert!(!a.is_empty());
    assert!(a == b, "the shards left another state");
    // The same metrics (the shard writers flush theirs at the drop).
    for m in ["rows_put", "rows_deleted", "logical_bytes"] {
        let get = |s: &HeedStore| {
            s.metrics()
                .snapshot()
                .into_iter()
                .find(|(n, _)| *n == m)
                .unwrap()
                .1
        };
        assert_eq!(get(one.s()), get(many.s()), "{m}");
    }
    // The same dirty set: the cut is identical, key for key.
    let (cut_a, cut_b) = {
        let mut wa = one.s().write().unwrap();
        let mut wb = many.s().write().unwrap();
        (
            wa.take_cut().unwrap().expect("a cut"),
            wb.take_cut().unwrap().expect("a cut"),
        )
    };
    assert_eq!(cut_a.keys(), cut_b.keys());
    assert!(cut_rows(&cut_a) == cut_rows(&cut_b), "the cuts differ");
    // Written, and reopened: the same checkpoint.
    let (mut ca, mut cb) = (cut_a, cut_b);
    one.s().write_cut(&mut ca).unwrap();
    many.s().write_cut(&mut cb).unwrap();
    one.reopen();
    many.reopen();
    let a = one.s().read(|r| Ok(image(r))).unwrap();
    let b = many.s().read(|r| Ok(image(r))).unwrap();
    assert!(a == b, "the reopened checkpoints differ");
}

#[test]
fn a_shard_writer_reads_every_writer_and_types_like_the_write_handle() {
    fn assert_send<T: Send>() {}
    assert_send::<ShardWrite<'static>>();

    let t = Tmp::new("typed");
    let s = t.s();
    let mut main = s.write().unwrap();
    let row = rows::PartitionRow::new([1u8; 16], "t", "q", "p", 1_000);
    main.create_partition(7, &row).unwrap();
    let mut a = s.shard_writer().unwrap();
    let mut b = s.shard_writer().unwrap();
    assert_eq!(s.shard_writers(), 2);
    // A shard writer sees the write handle's rows and the other shard's, live.
    assert_eq!(a.partition(7).unwrap(), Some(row.clone()));
    assert_eq!(a.pid_of("t", "q", "p").unwrap(), Some(7));
    b.put_cursor(7, "g", &rows::cursor_fresh(-1, 5)).unwrap();
    assert_eq!(a.cursor(7, "g").unwrap().unwrap().committed, -1);
    assert_eq!(
        b.add_counter(&keys::counter_partition(7, Counter::Pushed), 3)
            .unwrap(),
        3
    );
    assert_eq!(a.partition_counter(7, Counter::Pushed).unwrap(), 3);
    a.put_lease("w", 7, "g", 44).unwrap();
    let mut leases = Vec::new();
    main.scan_worker_leases("w", 4, &mut |pid, g, at| {
        leases.push((pid, g.to_string(), at));
        true
    })
    .unwrap();
    assert_eq!(leases, vec![(7, "g".to_string(), 44)]);
    let mut seen = Vec::new();
    b.scan_cursors(7, 10, &mut |g, _c| {
        seen.push(g.to_string());
        true
    })
    .unwrap();
    assert_eq!(seen, vec!["g"]);
    // get_raw keeps what it hands out until the next `&mut` call.
    assert!(a
        .get_raw(Keyspace::Partitions, &keys::pid(7))
        .unwrap()
        .is_some());
    assert_eq!(a.arena_len(), 1);
    a.del_raw(Keyspace::Pending, b"none").unwrap();
    assert_eq!(a.arena_len(), 0);

    // No transaction: it neither commits nor cuts; abort is a no-op.
    assert!(a.commit().is_err());
    assert!(a.durable_commit().is_err());
    assert!(!a.can_cut());
    assert!(a.take_cut().unwrap().is_none());
    assert!(a.clear_node_local().is_err());
    a.abort().unwrap();
    assert_eq!(
        b.cursor(7, "g").unwrap().unwrap().committed,
        -1,
        "abort undid nothing"
    );
    drop(a);
    drop(b);
    assert_eq!(s.shard_writers(), 0);
    // The coordinator commits once they are gone: their rows are in the cut.
    let before = StoreMetrics::get(&s.metrics().rows_put);
    assert!(before >= 4);
    main.durable_commit().unwrap();
    drop(main);
    std::thread::scope(|sc| {
        sc.spawn(|| {
            assert!(s
                .checkpoint_get(
                    Keyspace::LeasesByWorker,
                    &keys::leases_by_worker("w", 7, "g")
                )
                .unwrap()
                .is_some());
            assert!(s
                .checkpoint_get(Keyspace::Cursors, &keys::cursors(7, "g"))
                .unwrap()
                .is_some());
        })
        .join()
        .unwrap();
    });
}

#[test]
fn a_poisoned_store_refuses_a_shard_writer() {
    let t = Tmp::new("poisoned");
    let s = t.s();
    {
        let mut w = s.write().unwrap();
        w.put_raw(Keyspace::Queues, b"q", b"v").unwrap();
    }
    s.ram_put_stored(Keyspace::Queues, b"q", &[1, 2, 3]);
    assert!(s
        .read(|r| r.get_raw(Keyspace::Queues, b"q").map(|_| ()))
        .is_err());
    assert!(s.shard_writer().is_err());
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "shard writer alive")]
fn a_cut_with_a_shard_writer_alive_is_caught_in_a_debug_build() {
    let t = Tmp::new("contract");
    let s = t.s();
    let mut w = s.write().unwrap();
    let _shard = s.shard_writer().unwrap();
    w.put_raw(Keyspace::Queues, b"q", b"v").unwrap();
    let _ = w.take_cut();
}

/// A read-modify-write is ONE operation under the row's lock: shard writers
/// adding to the same counter lose no update, and a partition's tail rewrite
/// from a shard writer is what `put_partition` of the rewritten row is.
#[test]
fn a_read_modify_write_is_atomic_across_shard_writers() {
    let t = Tmp::new("rmw");
    let s = t.s();
    let key = keys::counter_queue("t", "q", Counter::Pushed);
    std::thread::scope(|sc| {
        for _ in 0..4 {
            let key = &key;
            sc.spawn(move || {
                let mut w = s.shard_writer().unwrap();
                for _ in 0..2_000 {
                    w.add_counter(key, 1).unwrap();
                }
            });
        }
    });
    s.read(|r| {
        assert_eq!(r.counter_at(&key)?, 8_000, "an add was lost");
        Ok(())
    })
    .unwrap();

    // The tail rewrite, against the owned write of the same row.
    let mut w = s.write().unwrap();
    let row = rows::PartitionRow::new([1u8; 16], "tenant-0000", "q", "p", 1_000);
    w.create_partition(7, &row).unwrap();
    w.create_partition(8, &row).unwrap();
    drop(w);
    let mut sh = s.shard_writer().unwrap();
    assert!(sh
        .update_partition_tail(7, |h| {
            h.last_offset = 41;
            h.last_write_at_us = 5_000;
            h.last_created_at_us = 4_999;
            h.oldest_live_at_us = Some(4_000);
        })
        .unwrap());
    assert!(
        !sh.update_partition_tail(9, |h| h.last_offset = 1).unwrap(),
        "no row 9"
    );
    drop(sh);
    let mut want = row.clone();
    want.last_offset = 41;
    want.last_write_at_us = 5_000;
    want.last_created_at_us = 4_999;
    want.oldest_live_at_us = Some(4_000);
    let mut w = s.write().unwrap();
    w.put_partition(8, &want).unwrap();
    assert_eq!(w.partition(7).unwrap(), Some(want.clone()));
    assert_eq!(
        w.get_raw(Keyspace::Partitions, &keys::pid(7))
            .unwrap()
            .map(<[u8]>::to_vec),
        w.get_raw(Keyspace::Partitions, &keys::pid(8))
            .unwrap()
            .map(<[u8]>::to_vec),
        "the same bytes as the owned write"
    );
    // A rewrite that changes nothing writes nothing.
    let before = StoreMetrics::get(&s.metrics().rows_put);
    assert!(w.update_partition_tail(7, |_| {}).unwrap());
    assert_eq!(StoreMetrics::get(&s.metrics().rows_put), before);
    // Dirty, and in the checkpoint.
    w.durable_commit().unwrap();
    drop(w);
    std::thread::scope(|sc| {
        sc.spawn(|| {
            let got = s
                .checkpoint_get(Keyspace::Partitions, &keys::pid(7))
                .unwrap();
            assert_eq!(got.map(|b| rows::partition_decode(&b).unwrap()), Some(want));
        })
        .join()
        .unwrap();
    });
}
