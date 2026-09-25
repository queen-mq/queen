//! Store format 1 (a checksum in every stored value, `rsm::store::integrity`)
//! through apply and through the reads the planner and pop make.
//!
//! - [`a_workload_digests_the_same_on_both_formats_and_after_a_migration`]:
//!   the §12.9 digest of a real workload is the same on a format-1 store, on a
//!   format-0 one, and on that format-0 store after its migration — the digest
//!   reads through the store API, which hands out logical bytes only.
//! - [`format_1_overhead_ab`] (`#[ignore]`, a measurement): the same apply
//!   shapes, the same checkpoint, and the same planner and pop reads on both
//!   formats in ONE binary — interleaved rounds, medians. Format 0 runs the
//!   identical code minus the checksum work, so the difference IS its cost.
//!   Run with:
//!   `cargo test --release --lib rsm::tests::store_integrity::format_1_overhead_ab -- --ignored --nocapture`
//! - [`format_1_open_cost`] (`#[ignore]`, a measurement): the boot cost — an
//!   unverified format-0 load, a verified format-1 load, and the one-time
//!   migration of a format-0 store at its open.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use super::apply::{
    cfg, group_meta, hashes, queue_config, seg_opts, settle, uuid, Build, Workload, BASE_US, QUEUE,
    TENANT,
};
use super::samples;
use crate::rsm::apply::{local_digest, state_digest, Applier, NoNotify, StateDigest};
use crate::rsm::effect::{Effect, GroupMeta, SubscriptionMode};
use crate::rsm::segments;
use crate::rsm::store::integrity::StoreFormat;
use crate::rsm::store::keys::{self, Counter};
use crate::rsm::store::rows::{self, GroupRow, PartitionRow};
use crate::rsm::store::{
    HeedStore, Keyspace, Reads, Store, StoreOpts, TypedReads, TypedWrites, Writes,
};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn tmp(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-fmt-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// A store in `format`: a new format-1 store, or a new format-0 one written
/// exactly as a build before format 1 wrote it.
fn opts(format: StoreFormat) -> StoreOpts {
    StoreOpts {
        map_bytes: Some(1 << 30),
        create_legacy: format == StoreFormat::Legacy,
        migrate_legacy: false,
        ..Default::default()
    }
}

fn digests(store: &HeedStore) -> (StateDigest, StateDigest) {
    store
        .read(|r| Ok((state_digest(r)?, local_digest(r)?)))
        .expect("digest")
}

/// Apply `count` entries of the shared workload to a fresh store in `format`,
/// with durable points and file GC as `run_workload` does, and settle.
fn run_workload_on(
    dir: &Path,
    format: StoreFormat,
    seed: u64,
    count: u64,
) -> (StateDigest, StateDigest) {
    let store = HeedStore::open(&dir.join("store"), &opts(format)).expect("open");
    assert_eq!(store.format(), format);
    {
        let (mut a, _) = Applier::open(
            &store,
            &dir.join("seg"),
            seg_opts(),
            cfg(),
            Arc::new(NoNotify),
        )
        .expect("applier");
        let mut w = Workload::new(seed);
        for n in 1..=count {
            a.apply(&w.next()).expect("apply");
            if n % 64 == 0 {
                a.durable_point().expect("durable point");
            }
            a.gc_pass().expect("gc");
        }
        settle(&mut a);
    }
    let d = digests(&store);
    store.close();
    d
}

#[test]
fn a_workload_digests_the_same_on_both_formats_and_after_a_migration() {
    let v1 = tmp("v1");
    let v0 = tmp("v0");
    let (s1, _) = run_workload_on(&v1, StoreFormat::V1, 0x5EED, 300);
    let (s0, l0) = run_workload_on(&v0, StoreFormat::Legacy, 0x5EED, 300);
    assert_eq!(
        s1,
        s0,
        "replicated state differs between the formats at {:?}",
        s1.first_difference(&s0)
    );
    // The format-0 node is migrated at its next open: the same replicated
    // state, and — the one comparison node-local state allows, one node across
    // a restart — the same node-local state.
    let store = HeedStore::open(
        &v0.join("store"),
        &StoreOpts {
            map_bytes: Some(1 << 30),
            ..Default::default()
        },
    )
    .expect("reopen with the migration");
    assert_eq!(store.format(), StoreFormat::V1);
    let (m, lm) = digests(&store);
    store.close();
    assert_eq!(m, s0, "the migration changed the replicated state");
    assert_eq!(lm, l0, "the migration changed the node-local state");
    // And the format-1 node reopens verified, at the same digest.
    let store = HeedStore::open(&v1.join("store"), &opts(StoreFormat::V1)).expect("reopen");
    assert_eq!(digests(&store).0, s1);
    store.close();
    let _ = std::fs::remove_dir_all(&v1);
    let _ = std::fs::remove_dir_all(&v0);
}

// ---------------------------------------------------------------------------
// The A/B measurement
// ---------------------------------------------------------------------------

/// Segment files large enough that apply is not sealing a file every few
/// hundred messages: the measurement is about the store, not file churn.
fn perf_seg_opts() -> segments::Options {
    segments::Options {
        segment_bytes: 64 << 20,
        ..seg_opts()
    }
}

/// Apply ns per `Append` entry in the PERF-D shape: `parts` partitions,
/// `batch` messages per append, `groups` groups, `windows` × 256 entries with
/// a store commit per 256 (the shipped cadence).
fn apply_appends(format: StoreFormat, parts: u64, batch: u32, groups: u64, windows: u64) -> f64 {
    let dir = tmp("ab-apply");
    let store = HeedStore::open(&dir.join("store"), &opts(format)).expect("open");
    let ns = {
        let (mut a, _) = Applier::open(
            &store,
            &dir.join("seg"),
            perf_seg_opts(),
            cfg(),
            Arc::new(NoNotify),
        )
        .expect("applier");
        let mut now = BASE_US;
        let mut ids = 0u64;
        let mut effects = vec![Effect::QueueUpsert {
            tenant: TENANT.into(),
            queue: QUEUE.into(),
            cfg: queue_config(now),
        }];
        for g in 0..groups {
            effects.push(Effect::GroupUpsert {
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                group: format!("g{g}"),
                meta: group_meta(g, now),
            });
        }
        for p in 0..parts {
            effects.push(Effect::PartitionCreate {
                pid: 1 + p,
                uuid: uuid(1 + p),
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                partition: format!("p{p}"),
                created_at_us: now,
            });
        }
        let setup = {
            let b = Build::new(now, 1, ids).cmd(effects);
            ids += 1;
            b.at(1, 1)
        };
        a.apply(&setup).expect("setup");
        a.commit().expect("commit setup");
        let pid_base = 1 + parts;
        let mut last_off = vec![-1i64; parts as usize];
        let (mut k, mut idx) = (0u64, 1u64);
        let t0 = Instant::now();
        for _ in 0..windows {
            for _ in 0..256 {
                now += 1000;
                idx += 1;
                ids += 1;
                let pi = (k % parts) as usize;
                let base = (last_off[pi] + 1) as u64;
                last_off[pi] += batch as i64;
                let app = Effect::Append {
                    pid: 1 + pi as u64,
                    bucket: (pi % 8) as u16,
                    base_offset: base,
                    count: batch,
                    created_at_us: now,
                    hashes: hashes(k, batch),
                    blob: vec![0xAB; 24 * batch as usize],
                };
                let c = Build::new(now, pid_base, ids).cmd(vec![app]).at(idx, 1);
                a.apply(&c).expect("append");
                k += 1;
            }
            a.commit().expect("commit window");
        }
        t0.elapsed().as_nanos() as f64 / k as f64
    };
    store.close();
    let _ = std::fs::remove_dir_all(&dir);
    ns
}

/// Apply ns per entry of the mixed workload (appends, cursor moves, dead
/// letters, watermarks), durable point every 256.
fn apply_workload(format: StoreFormat, count: u64) -> f64 {
    let dir = tmp("ab-workload");
    let store = HeedStore::open(&dir.join("store"), &opts(format)).expect("open");
    let ns = {
        let (mut a, _) = Applier::open(
            &store,
            &dir.join("seg"),
            perf_seg_opts(),
            cfg(),
            Arc::new(NoNotify),
        )
        .expect("applier");
        let mut w = Workload::new(0xAB);
        let entries: Vec<_> = (0..count).map(|_| w.next()).collect();
        let t0 = Instant::now();
        for (n, c) in entries.iter().enumerate() {
            a.apply(c).expect("apply");
            if (n + 1) % 256 == 0 {
                a.commit().expect("commit");
            }
        }
        t0.elapsed().as_nanos() as f64 / count as f64
    };
    store.close();
    let _ = std::fs::remove_dir_all(&dir);
    ns
}

/// The shape of a planner's and pop's state: `queues` queues of `parts`
/// partitions and `groups` groups each, with their cursors, pending rows and
/// counters — every row written through the typed writes.
struct ReadFixture {
    dir: PathBuf,
    store: HeedStore,
    queues: u64,
    parts: u64,
    groups: u64,
}

fn group_row(n: u64) -> GroupRow {
    GroupRow {
        meta: GroupMeta {
            id: uuid(n),
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

impl ReadFixture {
    fn new(format: StoreFormat, queues: u64, parts: u64, groups: u64) -> ReadFixture {
        let dir = tmp("ab-reads");
        let store = HeedStore::open(&dir, &opts(format)).expect("open");
        {
            let mut w = store.write().expect("write");
            let qc = samples::queue_config();
            let mut pid = 0u64;
            for q in 0..queues {
                let queue = format!("queue-{q}");
                w.put_queue(TENANT, &queue, &qc).unwrap();
                for g in 0..groups {
                    w.put_group(TENANT, &queue, &format!("group-{g}"), &group_row(g))
                        .unwrap();
                }
                for p in 0..parts {
                    pid += 1;
                    let row = PartitionRow::new(uuid(pid), TENANT, &queue, &format!("p{p}"), 5_000);
                    w.create_partition(pid, &row).unwrap();
                    w.put_partition(pid, &row).unwrap();
                    for g in 0..groups {
                        let group = format!("group-{g}");
                        let mut c = rows::cursor_fresh(pid as i64, 1_000);
                        c.total_consumed = pid;
                        w.put_cursor(pid, &group, &c).unwrap();
                        w.put_pending(TENANT, &queue, &group, pid, 5_000 + pid as i64)
                            .unwrap();
                    }
                    w.add_counter(&keys::counter_partition(pid, Counter::Pushed), 3)
                        .unwrap();
                }
            }
            w.set_applied(1, 1).unwrap();
            w.durable_commit().unwrap();
        }
        ReadFixture {
            dir,
            store,
            queues,
            parts,
            groups,
        }
    }

    fn close(self) {
        self.store.close();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A cheap deterministic sequence for picking rows.
fn lcg(x: &mut u64) -> u64 {
    *x = x
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *x >> 17
}

/// ns per PLAN READ: one read transaction with what a push/pop plan reads for
/// one (queue, partition, group) — the queue config, the partition's pid and
/// row, the group, its cursor, its pending row, a counter (7 typed gets).
fn planner_reads(f: &ReadFixture, n: u64) -> f64 {
    let mut x = 7u64;
    let picks: Vec<(String, String, String, u64)> = (0..4096)
        .map(|_| {
            let q = lcg(&mut x) % f.queues;
            let p = lcg(&mut x) % f.parts;
            let g = lcg(&mut x) % f.groups;
            (
                format!("queue-{q}"),
                format!("p{p}"),
                format!("group-{g}"),
                1 + q * f.parts + p,
            )
        })
        .collect();
    let mut hits = 0u64;
    let t0 = Instant::now();
    for i in 0..n {
        let (queue, part, group, pid) = &picks[(i % 4096) as usize];
        f.store
            .read(|r| {
                hits += r.queue(TENANT, queue)?.is_some() as u64;
                let got = r.pid_of(TENANT, queue, part)?.unwrap_or(0);
                hits += r.partition(got)?.is_some() as u64;
                hits += r.group(TENANT, queue, group)?.is_some() as u64;
                hits += r.cursor(*pid, group)?.is_some() as u64;
                hits += r.pending_at(TENANT, queue, group, *pid)?.is_some() as u64;
                hits += r.partition_counter(*pid, Counter::Pushed)? as u64;
                Ok(())
            })
            .unwrap();
    }
    let ns = t0.elapsed().as_nanos() as f64 / n as f64;
    assert!(hits > 0);
    ns
}

/// ns per POP READ: what a pop plan reads for one (queue, group) once the
/// in-memory ready ring has offered its candidates — the queue and the group,
/// then per candidate (4) the garbage check, the partition row and the cursor
/// (the committed view's `partition` + `cursor`), in one read transaction.
fn pop_reads(f: &ReadFixture, n: u64) -> f64 {
    let mut x = 11u64;
    let picks: Vec<(String, String, [u64; 4])> = (0..4096)
        .map(|_| {
            let q = lcg(&mut x) % f.queues;
            let g = lcg(&mut x) % f.groups;
            let mut pids = [0u64; 4];
            for p in pids.iter_mut() {
                *p = 1 + q * f.parts + lcg(&mut x) % f.parts;
            }
            (format!("queue-{q}"), format!("group-{g}"), pids)
        })
        .collect();
    let mut seen = 0u64;
    let t0 = Instant::now();
    for i in 0..n {
        let (queue, group, pids) = &picks[(i % 4096) as usize];
        f.store
            .read(|r| {
                seen += r.queue(TENANT, queue)?.is_some() as u64;
                seen += r.group(TENANT, queue, group)?.is_some() as u64;
                for pid in pids {
                    if r.garbage(*pid)?.is_some() {
                        continue;
                    }
                    seen += r.partition(*pid)?.is_some() as u64;
                    seen += r.cursor(*pid, group)?.is_some() as u64;
                }
                Ok(())
            })
            .unwrap();
    }
    let ns = t0.elapsed().as_nanos() as f64 / n as f64;
    assert!(seen > 0);
    ns
}

/// ns per row of a chunked walk of a whole keyspace — the shape of the
/// ready-ring rebuild (§6.3) and of a `DeleteChunk`: every `pending` row,
/// decoded, 256 per read transaction.
fn pending_walk(f: &ReadFixture) -> f64 {
    let mut rows_seen = 0u64;
    let mut last: Vec<u8> = Vec::with_capacity(128);
    let mut sum = 0i64;
    let t0 = Instant::now();
    for _ in 0..20 {
        let mut from: Vec<u8> = Vec::new();
        loop {
            last.clear();
            let n = f
                .store
                .read(|r| {
                    r.scan_raw(Keyspace::Pending, &from, &[], 256, &mut |k, v| {
                        sum = sum.wrapping_add(rows::i64_decode(v).unwrap_or(0));
                        last.clear();
                        last.extend_from_slice(k);
                        true
                    })
                })
                .unwrap();
            rows_seen += n as u64;
            if n < 256 {
                break;
            }
            match crate::rsm::store::resume_after(&last, 511) {
                Some(next) => from = next,
                None => break,
            }
        }
    }
    assert!(sum != 0);
    t0.elapsed().as_nanos() as f64 / rows_seen.max(1) as f64
}

/// Raw store micro-ops on 64 B values over 100k keys.
fn raw_ops(format: StoreFormat) -> RawOps {
    const KEYS: u64 = 100_000;
    let dir = tmp("ab-raw");
    let store = HeedStore::open(&dir, &opts(format)).expect("open");
    let val = [0x5Au8; 64];
    let key = |i: u64| -> [u8; 12] {
        let mut k = [0u8; 12];
        k[..4].copy_from_slice(b"raw/");
        k[4..].copy_from_slice(&i.to_be_bytes());
        k
    };
    let mut w = store.write().unwrap();
    for i in 0..KEYS {
        w.put_raw(Keyspace::Cursors, &key(i), &val).unwrap();
    }
    w.durable_commit().unwrap();
    // put: overwrite random keys.
    let mut x = 3u64;
    let n_put = 400_000u64;
    let t0 = Instant::now();
    for _ in 0..n_put {
        let i = lcg(&mut x) % KEYS;
        w.put_raw(Keyspace::Cursors, &key(i), &val).unwrap();
    }
    let put = t0.elapsed().as_nanos() as f64 / n_put as f64;
    // the durable point: every key is dirty now (random overwrites of 100k).
    let dirty = store.dirty_len(Keyspace::Cursors) as f64;
    let t0 = Instant::now();
    w.durable_commit().unwrap();
    let ckpt = t0.elapsed().as_nanos() as f64 / dirty.max(1.0);
    drop(w);
    // get: random keys, 256 per read transaction. Two readers: one that only
    // takes the slice's length (it never touches the value's bytes — format 0
    // then never loads the value's cache line, format 1 must, to verify it) and
    // one that reads the bytes as every real caller's decode does.
    let get_with = |x: &mut u64, touch: bool| -> f64 {
        let n_get = 1_000_000u64;
        let mut sum = 0u64;
        let mut done = 0u64;
        let t0 = Instant::now();
        while done < n_get {
            store
                .read(|r| {
                    for _ in 0..256 {
                        let i = lcg(x) % KEYS;
                        if let Some(v) = r.get_raw(Keyspace::Cursors, &key(i))? {
                            sum += if touch {
                                v.iter().map(|b| *b as u64).sum::<u64>()
                            } else {
                                v.len() as u64
                            };
                        }
                    }
                    Ok(())
                })
                .unwrap();
            done += 256;
        }
        assert!(sum > 0);
        t0.elapsed().as_nanos() as f64 / done as f64
    };
    let get_len = get_with(&mut x, false);
    let get_read = get_with(&mut x, true);
    // scan: 256-row walks from random starts, the same two readers.
    let scan_with = |x: &mut u64, touch: bool| -> f64 {
        let n_scan = 4_000u64;
        let (mut rows_seen, mut sum) = (0u64, 0u64);
        let t0 = Instant::now();
        for _ in 0..n_scan {
            let i = lcg(x) % (KEYS - 300);
            store
                .read(|r| {
                    r.scan_raw(Keyspace::Cursors, &key(i), b"raw/", 256, &mut |_k, v| {
                        rows_seen += 1;
                        sum += if touch {
                            v.iter().map(|b| *b as u64).sum::<u64>()
                        } else {
                            v.len() as u64
                        };
                        true
                    })
                })
                .unwrap();
        }
        assert!(sum > 0);
        t0.elapsed().as_nanos() as f64 / rows_seen.max(1) as f64
    };
    let scan_len = scan_with(&mut x, false);
    let scan_read = scan_with(&mut x, true);
    store.close();
    let _ = std::fs::remove_dir_all(&dir);
    RawOps {
        get_len,
        get_read,
        put,
        scan_len,
        scan_read,
        ckpt,
    }
}

struct RawOps {
    get_len: f64,
    get_read: f64,
    put: f64,
    scan_len: f64,
    scan_read: f64,
    ckpt: f64,
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    v[v.len() / 2]
}

#[test]
#[ignore = "measurement, not a gate; run with --release --ignored --nocapture"]
fn format_1_overhead_ab() {
    const ROUNDS: usize = 7;
    let names = [
        "apply A20k (100 parts, batch 10), ns/entry",
        "apply C1000 (1000 parts, batch 1), ns/entry",
        "apply mixed workload, ns/entry",
        "planner read (7 typed gets), ns/plan",
        "pop read (queue, group, 4 x garbage/partition/cursor), ns/pop",
        "pending walk (256-row chunks, decoded), ns/row",
        "raw get_raw 64 B, value unread, ns/get",
        "raw get_raw 64 B, value read, ns/get",
        "raw put_raw 64 B, ns/put",
        "raw scan_raw, value unread, ns/row",
        "raw scan_raw, value read, ns/row",
        "durable point (checkpoint), ns/dirty row",
    ];
    // samples[metric][format 0 = legacy, 1 = v1]
    let mut samples: Vec<[Vec<f64>; 2]> = names.iter().map(|_| [Vec::new(), Vec::new()]).collect();
    for round in 0..ROUNDS {
        // Alternate which format goes first, so neither always runs warm.
        let order = if round % 2 == 0 {
            [StoreFormat::Legacy, StoreFormat::V1]
        } else {
            [StoreFormat::V1, StoreFormat::Legacy]
        };
        for format in order {
            let fi = usize::from(format == StoreFormat::V1);
            samples[0][fi].push(apply_appends(format, 100, 10, 1, 40));
            samples[1][fi].push(apply_appends(format, 1000, 1, 1, 40));
            samples[2][fi].push(apply_workload(format, 8_000));
            let f = ReadFixture::new(format, 50, 20, 2);
            samples[3][fi].push(planner_reads(&f, 200_000));
            samples[4][fi].push(pop_reads(&f, 100_000));
            samples[5][fi].push(pending_walk(&f));
            f.close();
            let raw = raw_ops(format);
            samples[6][fi].push(raw.get_len);
            samples[7][fi].push(raw.get_read);
            samples[8][fi].push(raw.put);
            samples[9][fi].push(raw.scan_len);
            samples[10][fi].push(raw.scan_read);
            samples[11][fi].push(raw.ckpt);
        }
    }
    println!();
    println!(
        "{:<62} {:>10} {:>10} {:>8}   samples (format 0 | format 1)",
        "store format 1 vs 0 (median of 7 interleaved rounds)", "format 0", "format 1", "delta"
    );
    for (i, name) in names.iter().enumerate() {
        let (raw0, raw1) = (samples[i][0].clone(), samples[i][1].clone());
        let legacy = median(&mut samples[i][0]);
        let v1 = median(&mut samples[i][1]);
        let fmt = |v: &[f64]| {
            v.iter()
                .map(|s| format!("{s:.0}"))
                .collect::<Vec<_>>()
                .join(" ")
        };
        println!(
            "{:<62} {:>10.1} {:>10.1} {:>7.1}%   {} | {}",
            name,
            legacy,
            v1,
            (v1 - legacy) * 100.0 / legacy,
            fmt(&raw0),
            fmt(&raw1)
        );
    }
}

/// Fill a new store in `format` with `n` rows spread over three keyspaces
/// and three value sizes (8 B, 64 B, 150 B), checkpoint it, and close it.
fn open_fixture(dir: &Path, format: StoreFormat, n: u64) {
    let store = HeedStore::open(dir, &opts(format)).expect("open");
    {
        let mut w = store.write().expect("write");
        for i in 0..n {
            let k = i.to_be_bytes();
            match i % 3 {
                0 => w.put_raw(Keyspace::Pending, &k, &[1u8; 8]).unwrap(),
                1 => w.put_raw(Keyspace::Cursors, &k, &[2u8; 64]).unwrap(),
                _ => w.put_raw(Keyspace::Queues, &k, &[3u8; 150]).unwrap(),
            }
        }
        w.set_applied(1, 1).unwrap();
        w.durable_commit().unwrap();
    }
    store.close();
}

fn copy_dir(from: &Path, to: &Path) {
    let _ = std::fs::remove_dir_all(to);
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
}

/// The boot cost of format 1: opening (loading every row into RAM) a store of
/// 300k rows as format 0 (nothing verified), as format 1 (every value, the key
/// order and each B-tree's row count verified), and a format-0 store MIGRATED
/// at the open (the one-time cost: one transaction rewriting every row, then
/// the verified load). Interleaved, medians of 5. Run with:
/// `cargo test --release --lib rsm::tests::store_integrity::format_1_open_cost -- --ignored --nocapture`
#[test]
#[ignore = "measurement, not a gate; run with --release --ignored --nocapture"]
fn format_1_open_cost() {
    const ROWS: u64 = 300_000;
    let v0 = tmp("open-v0");
    let v1 = tmp("open-v1");
    let work = tmp("open-migrate");
    open_fixture(&v0, StoreFormat::Legacy, ROWS);
    open_fixture(&v1, StoreFormat::V1, ROWS);
    let keep_v0 = StoreOpts {
        map_bytes: Some(1 << 30),
        migrate_legacy: false,
        ..Default::default()
    };
    let migrate = StoreOpts {
        map_bytes: Some(1 << 30),
        ..Default::default()
    };
    let (mut plain, mut verified, mut migrated) = (Vec::new(), Vec::new(), Vec::new());
    for _ in 0..5 {
        let t = Instant::now();
        let s = HeedStore::open(&v0, &keep_v0).expect("open format 0");
        plain.push(t.elapsed().as_nanos() as f64 / ROWS as f64);
        assert_eq!(s.format(), StoreFormat::Legacy);
        s.close();

        let t = Instant::now();
        let s = HeedStore::open(&v1, &opts(StoreFormat::V1)).expect("open format 1");
        verified.push(t.elapsed().as_nanos() as f64 / ROWS as f64);
        assert_eq!(s.format(), StoreFormat::V1);
        s.close();

        copy_dir(&v0, &work);
        let t = Instant::now();
        let s = HeedStore::open(&work, &migrate).expect("migrate at open");
        migrated.push(t.elapsed().as_nanos() as f64 / ROWS as f64);
        assert_eq!(s.format(), StoreFormat::V1);
        s.close();
    }
    let med = |v: &mut Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
        v[v.len() / 2]
    };
    let (p, v, m) = (med(&mut plain), med(&mut verified), med(&mut migrated));
    println!();
    println!("open of a {ROWS}-row store (median of 5), ns/row:");
    println!("  format 0, unverified load        {p:>8.1}");
    println!(
        "  format 1, verified load          {v:>8.1}   ({:+.1}%)",
        (v - p) * 100.0 / p
    );
    println!(
        "  format 0 -> 1 migration + load   {m:>8.1}   (one time; {:.0} ms for {ROWS} rows)",
        m * ROWS as f64 / 1e6
    );
    for d in [v0, v1, work] {
        let _ = std::fs::remove_dir_all(d);
    }
}
