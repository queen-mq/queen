//! Traces on disk (`PLAN_TRACES_ON_DISK.md`), through apply, the leader's
//! maintenance and the facade:
//!
//! - catalogue version 4 records into the trace environment and leaves the
//!   RAM keyspaces alone; version 1 still records into RAM, as before;
//! - a crash replay records and trims each trace exactly once — with the
//!   trace environment ahead of the store, and with it lost back to an older
//!   copy — and ends where an uninterrupted run did;
//! - a trim deletes at most its limit from each store, oldest first; the old
//!   `TraceExpire` still deletes every legacy row past its cutoff;
//! - a tenant purge takes the tenant's traces from both stores;
//! - two nodes reach one digest, and a lagging node built from a snapshot's
//!   copies (store checkpoint + trace environment) catches up to it;
//! - a trace fsync that fails is a durable point that did not happen;
//! - maintenance plans the bounded trim at version 4 and `TraceExpire` below;
//! - the facade: every route answers the legacy and the disk rows together,
//!   newest first, paged and tenant-scoped; a too-long key is a 400; the
//!   cluster version decides which kind a POST writes; and retention drains a
//!   backlog in bounded steps.

use std::path::Path;
use std::time::{Duration, Instant};

use serde_json::Value;

use super::apply::{open_at, tmp_dir, Build, Node};
use crate::rsm::apply::{Applier, StateDigest};
use crate::rsm::effect::{Effect, TraceEvent};
use crate::rsm::store::{HeedStore, Keyspace, Reads, Store};

const T0: &str = "acme";
const BASE: i64 = 1_768_000_000_000_000;

fn ev(tenant: &str, pid: Option<u64>, txn: &str, names: &[&str], at: i64) -> TraceEvent {
    TraceEvent {
        trace_id: [(at % 241) as u8; 16],
        tenant: tenant.into(),
        pid,
        message_id: None,
        txn: txn.into(),
        consumer_group: None,
        event_type: "info".into(),
        data: format!(r#"{{"at":{at},"blob":"{}"}}"#, "y".repeat(5000)).into_bytes(),
        worker: Some("w".into()),
        names: names.iter().map(|s| s.to_string()).collect(),
        created_at_us: at,
    }
}

fn record(e: TraceEvent) -> Effect {
    Effect::TraceRecord { event: e }
}

fn append(e: TraceEvent) -> Effect {
    Effect::TraceAppend { event: e }
}

fn trace_digest(a: &Applier<'_, HeedStore>) -> u128 {
    a.trace_store().read(|r| r.digest()).expect("trace digest")
}

/// `(bodies, by_txn, by_name, expiry)` of the trace environment.
fn disk_rows(a: &Applier<'_, HeedStore>) -> (u64, u64, u64, u64) {
    a.trace_store().read(|r| r.rows()).expect("rows")
}

fn ram_rows(node: &Node) -> (u64, u64, u64) {
    node.store()
        .read(|r| {
            Ok((
                r.count(Keyspace::Traces)?,
                r.count(Keyspace::TraceNames)?,
                r.count(Keyspace::TraceExpiry)?,
            ))
        })
        .expect("count")
}

/// Forty entries of every trace kind: disk records (two per entry, two
/// tenants, several messages), a legacy RAM record now and then, bounded
/// trims that catch both, and a tenant purge.
fn workload() -> Vec<crate::rsm::apply::Committed> {
    let mut out = Vec::new();
    for i in 1..=40u64 {
        let now = BASE + i as i64 * 1_000;
        let effects = match i {
            17 | 33 => vec![Effect::TraceTrim {
                cutoff_us: now - 9_500,
                limit: 3,
            }],
            29 => vec![Effect::TenantPurge {
                tenant: "beta".into(),
            }],
            _ => {
                let tenant = if i % 3 == 0 { "beta" } else { T0 };
                let mut v = vec![
                    record(ev(
                        tenant,
                        Some(i % 4),
                        &format!("txn-{}", i % 5),
                        &["a", if i % 2 == 0 { "b" } else { "c" }],
                        now,
                    )),
                    record(ev(T0, None, &format!("solo-{i}"), &["a"], now + 1)),
                ];
                if i % 7 == 0 {
                    v.push(append(ev(T0, Some(1), "legacy", &["a"], now + 2)));
                }
                v
            }
        };
        out.push(Build::new(now, 1, i * 10).cmd(effects).at(i, 1));
    }
    out
}

/// An uninterrupted run of [`workload`]: the store's digest and the trace
/// environment's.
fn reference() -> (StateDigest, u128) {
    let node = Node::new("traces-ref");
    let (mut a, _) = open_at(&node);
    for c in &workload() {
        a.apply(c).expect("apply");
    }
    a.flush().expect("flush");
    let t = trace_digest(&a);
    drop(a);
    (node.digest(), t)
}

/// Copy every file under `src` to `dst` (what a snapshot ships of `seg/`).
fn copy_tree(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("mkdir");
    for e in std::fs::read_dir(src).expect("read dir").flatten() {
        let to = dst.join(e.file_name());
        if e.file_type().expect("type").is_dir() {
            copy_tree(&e.path(), &to);
        } else {
            std::fs::copy(e.path(), &to).expect("copy");
        }
    }
}

fn assert_same(got: &StateDigest, want: &StateDigest) {
    assert_eq!(
        got.whole,
        want.whole,
        "first difference: {:?}",
        got.first_difference(want)
    );
}

// ---------------------------------------------------------------------------
// Apply
// ---------------------------------------------------------------------------

#[test]
fn version_four_records_on_disk_and_version_one_still_in_ram() {
    let node = Node::new("traces-where");
    let (mut a, _) = open_at(&node);
    let c = Build::new(BASE, 1, 0)
        .cmd(vec![record(ev(T0, Some(3), "m", &["a", "b"], BASE))])
        .cmd(vec![append(ev(T0, Some(3), "m", &["a"], BASE + 1))])
        .at(1, 1);
    a.apply(&c).expect("apply");
    assert_eq!(disk_rows(&a), (1, 1, 2, 1));
    assert_eq!(ram_rows(&node), (1, 1, 1));
    assert_eq!(a.trace_store().applied_index(), 1);
    assert_eq!(a.stats().trace_writes, 1);
    // An entry with no trace effect leaves the trace environment alone.
    a.apply(&Build::new(BASE + 1, 1, 10).cmd(vec![Effect::Noop]).at(2, 1))
        .expect("apply");
    assert_eq!(a.trace_store().applied_index(), 1);
    assert_eq!(a.stats().trace_writes, 1);
    // The record is the event as posted, keyed by (entry, ordinal).
    let id = crate::rsm::traces::TraceId {
        index: 1,
        ordinal: 0,
    };
    let body = a
        .trace_store()
        .read(|r| r.body(id))
        .expect("read")
        .expect("body");
    assert_eq!(body, ev(T0, Some(3), "m", &["a", "b"], BASE));
}

#[test]
fn a_crash_replay_records_and_trims_each_trace_once() {
    let (want_state, want_traces) = reference();
    let dir = tmp_dir("traces-crash");
    {
        let mut node = Node::at(dir.clone());
        node.keep();
        let (mut a, _) = open_at(&node);
        for c in &workload() {
            a.apply(c).expect("apply");
            if c.index == 20 {
                a.durable_point().expect("durable point");
            }
        }
        // The crash: no flush. The store reopens at its durable point (20);
        // the trace environment, committed per entry, is at 40.
        assert_eq!(a.durable_index(), 20);
        assert_eq!(a.trace_store().applied_index(), 40);
        drop(a);
        node.close();
    }
    let node = Node::at(dir.clone());
    let (mut a, rec) = open_at(&node);
    assert_eq!(rec.replay_after, 20);
    assert_eq!(a.trace_store().applied_index(), 40);
    for c in workload().iter().filter(|c| c.index > 20) {
        a.apply(c).expect("re-apply");
    }
    // Every entry above 20 carries traces; none was written twice.
    assert_eq!(a.stats().trace_writes, 0);
    assert_eq!(a.stats().trace_replays, 20);
    a.flush().expect("flush");
    assert_eq!(trace_digest(&a), want_traces);
    drop(a);
    assert_same(&node.digest(), &want_state);
}

#[test]
fn a_replay_over_a_trace_store_that_lost_its_unsynced_tail_redoes_the_tail() {
    // A power loss after the durable point at 20 can take the trace
    // environment back to any commit at or after it. Here it is an exact copy
    // taken at 30: the replay skips the trace half of 21-30 and redoes 31-40.
    let (want_state, want_traces) = reference();
    let dir = tmp_dir("traces-lost");
    let at30 = tmp_dir("traces-lost-copy");
    {
        let mut node = Node::at(dir.clone());
        node.keep();
        let (mut a, _) = open_at(&node);
        for c in &workload() {
            a.apply(c).expect("apply");
            if c.index == 20 {
                a.durable_point().expect("durable point");
            }
            if c.index == 30 {
                a.trace_store().copy_to(&at30).expect("copy");
            }
        }
        drop(a);
        node.close();
    }
    let traces = dir.join(crate::rsm::traces::DIR);
    std::fs::remove_dir_all(&traces).expect("drop the trace store");
    std::fs::rename(&at30, &traces).expect("the older copy");
    let node = Node::at(dir.clone());
    let (mut a, _) = open_at(&node);
    assert_eq!(a.trace_store().applied_index(), 30);
    for c in workload().iter().filter(|c| c.index > 20) {
        a.apply(c).expect("re-apply");
    }
    assert_eq!(a.stats().trace_replays, 10);
    assert_eq!(a.stats().trace_writes, 10);
    a.flush().expect("flush");
    assert_eq!(trace_digest(&a), want_traces);
    drop(a);
    assert_same(&node.digest(), &want_state);
}

#[test]
fn two_nodes_reach_one_digest_and_a_snapshot_catches_a_third_up() {
    let (want_state, want_traces) = reference();
    // A second node, the same entries.
    let node = Node::new("traces-two");
    let (mut a, _) = open_at(&node);
    let entries = workload();
    for c in &entries {
        a.apply(c).expect("apply");
        if c.index % 10 == 0 && c.index < 40 {
            a.durable_point().expect("durable point");
        }
    }
    assert_eq!(trace_digest(&a), want_traces);

    // A lagging node built from what a snapshot carries: the store's
    // checkpoint (at 30), then the trace environment (at 40), then the
    // segment files.
    let third = tmp_dir("traces-snap");
    let (applied, _) = node
        .store()
        .copy_checkpoint(&third.join("store"))
        .expect("copy the store");
    assert_eq!(applied, 30);
    a.trace_store()
        .copy_to(&third.join(crate::rsm::traces::DIR))
        .expect("copy the traces");
    copy_tree(&node.seg_dir(), &third.join("seg"));
    a.flush().expect("flush");
    drop(a);
    assert_same(&node.digest(), &want_state);

    let follower = Node::at(third);
    let (mut f, rec) = open_at(&follower);
    assert_eq!(rec.replay_after, 30);
    assert_eq!(f.trace_store().applied_index(), 40);
    for c in entries.iter().filter(|c| c.index > 30) {
        f.apply(c).expect("catch up");
    }
    assert_eq!(f.stats().trace_writes, 0);
    f.flush().expect("flush");
    assert_eq!(trace_digest(&f), want_traces);
    drop(f);
    assert_same(&follower.digest(), &want_state);
}

#[test]
fn a_trim_is_bounded_in_both_stores_and_the_old_expire_is_not() {
    let node = Node::new("traces-trim");
    let (mut a, _) = open_at(&node);
    let ram: Vec<Effect> = (0..10)
        .map(|i| append(ev(T0, Some(1), &format!("r{i}"), &["a"], BASE + i)))
        .collect();
    let disk: Vec<Effect> = (0..10)
        .map(|i| record(ev(T0, Some(1), &format!("d{i}"), &["a", "b"], BASE + i)))
        .collect();
    a.apply(&Build::new(BASE, 1, 0).cmd(ram).cmd(disk).at(1, 1))
        .expect("apply");
    assert_eq!(ram_rows(&node), (10, 10, 10));
    assert_eq!(disk_rows(&a), (10, 10, 20, 10));

    // Six of each are due; a step takes four of each, the oldest.
    let trim = |index: u64, ids: u64| {
        Build::new(BASE + 100, 1, ids)
            .cmd(vec![Effect::TraceTrim {
                cutoff_us: BASE + 6,
                limit: 4,
            }])
            .at(index, 1)
    };
    a.apply(&trim(2, 10)).expect("trim");
    assert_eq!(ram_rows(&node), (6, 6, 6));
    assert_eq!(disk_rows(&a), (6, 6, 12, 6));
    let oldest_disk = a
        .trace_store()
        .read(|r| r.due(BASE + 1_000, 1))
        .expect("due");
    assert_eq!(oldest_disk, 1);
    let mut left = Vec::new();
    a.trace_store()
        .read(|r| {
            r.name_rev(T0, "b", &mut |at, _, _| {
                left.push(at - BASE);
                true
            })
        })
        .expect("read");
    assert_eq!(left, vec![9, 8, 7, 6, 5, 4]);
    // The rest of what is due, then nothing more.
    a.apply(&trim(3, 20)).expect("trim");
    assert_eq!(ram_rows(&node), (4, 4, 4));
    assert_eq!(disk_rows(&a), (4, 4, 8, 4));
    a.apply(&trim(4, 30)).expect("trim");
    assert_eq!(ram_rows(&node), (4, 4, 4));

    // Version 1's expiry: every legacy row past its cutoff in one step, and
    // the trace environment untouched.
    a.apply(
        &Build::new(BASE + 200, 1, 40)
            .cmd(vec![Effect::TraceExpire {
                cutoff_us: BASE + 1_000,
            }])
            .at(5, 1),
    )
    .expect("expire");
    assert_eq!(ram_rows(&node), (0, 0, 0));
    assert_eq!(disk_rows(&a), (4, 4, 8, 4));
}

#[test]
fn a_tenant_purge_takes_the_tenants_traces_from_both_stores() {
    let node = Node::new("traces-purge");
    let (mut a, _) = open_at(&node);
    a.apply(
        &Build::new(BASE, 1, 0)
            .cmd(vec![
                record(ev("gone", Some(1), "m", &["a"], BASE)),
                record(ev("kept", Some(1), "m", &["a"], BASE + 1)),
                append(ev("gone", Some(1), "m", &["a"], BASE + 2)),
                append(ev("kept", Some(1), "m", &["a"], BASE + 3)),
            ])
            .at(1, 1),
    )
    .expect("apply");
    a.apply(
        &Build::new(BASE + 10, 1, 10)
            .cmd(vec![Effect::TenantPurge {
                tenant: "gone".into(),
            }])
            .at(2, 1),
    )
    .expect("purge");
    assert_eq!(ram_rows(&node), (1, 1, 1));
    assert_eq!(disk_rows(&a), (1, 1, 1, 1));
    let mut kept = 0;
    a.trace_store()
        .read(|r| {
            r.name_rev("kept", "a", &mut |_, _, _| {
                kept += 1;
                true
            })
        })
        .expect("read");
    assert_eq!(kept, 1);
}

#[test]
fn a_trace_store_that_cannot_sync_is_a_durable_point_that_did_not_happen() {
    let node = Node::new("traces-sync");
    let (mut a, _) = open_at(&node);
    a.apply(
        &Build::new(BASE, 1, 0)
            .cmd(vec![record(ev(T0, None, "m", &["a"], BASE))])
            .at(1, 1),
    )
    .expect("apply");
    a.trace_store().fail_next_sync();
    let e = a.durable_point().expect_err("the point must not happen");
    assert!(e.lost_durable_point(), "{e}");
    assert_eq!(a.durable_index(), 0);
}

/// The apply cost of a trace and of its expiry, disk against the legacy RAM
/// rows. Not a correctness test; run on purpose, in release:
///
/// ```text
/// cargo test --release --lib -- --ignored --nocapture trace_apply_and_expiry_bench
/// ```
///
/// `QUEEN_TRACE_BENCH_N` traces (default 100 000) of ~5 KB, one per entry
/// (`QUEEN_TRACE_BENCH_PER_ENTRY`, default 1); a durable point every 1 000
/// entries (not timed). Then every one of them expires: `TraceTrim` steps of
/// 512 on disk, the old single `TraceExpire` step in RAM.
#[test]
#[ignore]
fn trace_apply_and_expiry_bench() {
    let num = |k: &str, d: u64| {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(d)
    };
    let n = num("QUEEN_TRACE_BENCH_N", 100_000);
    let per = num("QUEEN_TRACE_BENCH_PER_ENTRY", 1).max(1);
    for disk in [true, false] {
        // The test `Node`'s 256 MiB map is too small for the RAM rows here.
        let dir = tmp_dir(if disk { "bench-disk" } else { "bench-ram" });
        let store = HeedStore::open(
            &dir.join("store"),
            &crate::rsm::store::StoreOpts {
                map_bytes: Some(16 << 30),
                ..Default::default()
            },
        )
        .expect("store");
        let (mut a, _) = Applier::open(
            &store,
            &dir.join("seg"),
            super::apply::seg_opts(),
            super::apply::cfg(),
            std::sync::Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        let ram_left = || -> u64 { store.read(|r| r.count(Keyspace::Traces)).expect("count") };
        let (mut applying, mut index) = (Duration::ZERO, 0u64);
        let mut worst = Duration::ZERO;
        let mut i = 0u64;
        while i < n {
            index += 1;
            let effects: Vec<Effect> = (0..per)
                .map(|j| {
                    let k = i + j;
                    let e = ev(
                        T0,
                        None,
                        &format!("txn-{}", k / 5),
                        &[&format!("name-{}", k % 50), "load"],
                        BASE + k as i64,
                    );
                    if disk {
                        record(e)
                    } else {
                        append(e)
                    }
                })
                .collect();
            i += per;
            let c = Build::new(BASE + index as i64, 1, index * per)
                .cmd(effects)
                .at(index, 1);
            let t0 = Instant::now();
            a.apply(&c).expect("apply");
            let dt = t0.elapsed();
            applying += dt;
            worst = worst.max(dt);
            if index % 1000 == 0 {
                a.durable_point().expect("durable point");
            }
        }
        a.durable_point().expect("durable point");
        let what = if disk {
            "disk (TraceRecord)"
        } else {
            "RAM (TraceAppend)"
        };
        println!(
            "{what}: {n} traces in {index} entries: apply {:.1} µs/trace, {:.1} µs/entry, worst entry {:.2} ms",
            applying.as_secs_f64() * 1e6 / n as f64,
            applying.as_secs_f64() * 1e6 / index as f64,
            worst.as_secs_f64() * 1e3,
        );
        if disk {
            println!(
                "  trace store: {} MiB on disk, rows {:?}",
                a.trace_store().disk_bytes() >> 20,
                disk_rows(&a)
            );
        }
        // Everything is due.
        let cutoff = BASE + n as i64 + 1;
        let (mut steps, mut total, mut worst) = (0u32, Duration::ZERO, Duration::ZERO);
        loop {
            let left = if disk { disk_rows(&a).0 } else { ram_left() };
            if left == 0 {
                break;
            }
            index += 1;
            let e = if disk {
                Effect::TraceTrim {
                    cutoff_us: cutoff,
                    limit: 512,
                }
            } else {
                Effect::TraceExpire { cutoff_us: cutoff }
            };
            let c = Build::new(BASE + index as i64, 1, index * per)
                .cmd(vec![e])
                .at(index, 1);
            let t0 = Instant::now();
            a.apply(&c).expect("expire");
            let dt = t0.elapsed();
            total += dt;
            worst = worst.max(dt);
            steps += 1;
        }
        println!(
            "  expiry of {n}: {steps} step(s), worst {:.1} ms, mean {:.2} ms, total {:.0} ms",
            worst.as_secs_f64() * 1e3,
            total.as_secs_f64() * 1e3 / steps.max(1) as f64,
            total.as_secs_f64() * 1e3,
        );
        a.flush().expect("flush");
        drop(a);
        store.close();
        let _ = std::fs::remove_dir_all(&dir);
    }
}

// ---------------------------------------------------------------------------
// The leader's maintenance
// ---------------------------------------------------------------------------

#[test]
fn maintenance_plans_a_bounded_trim_at_version_four_and_the_old_expire_below() {
    use crate::rsm::maintenance::{plan, Config};
    let node = Node::new("traces-plan");
    let (mut a, _) = open_at(&node);
    let mut effects: Vec<Effect> = (0..5)
        .map(|i| record(ev(T0, None, &format!("d{i}"), &["a"], BASE + i)))
        .collect();
    effects.push(append(ev(T0, None, "r", &["a"], BASE)));
    a.apply(&Build::new(BASE, 1, 0).cmd(effects).at(1, 1))
        .expect("apply");
    let cfg = |limit: usize| Config {
        trace_retention_s: 1,
        trace_trim_limit: limit,
        traces_dir: Some(node.path().join(crate::rsm::traces::DIR)),
        ..Config::default()
    };
    let now = BASE + 10_000_000;
    let traces_of = |p: &crate::rsm::maintenance::Planned| -> Vec<Effect> {
        p.effects
            .iter()
            .filter(|e| matches!(e, Effect::TraceTrim { .. } | Effect::TraceExpire { .. }))
            .cloned()
            .collect()
    };

    // Version 3 (no ClusterVersionSet yet): the old expiry, unbounded.
    let p = node.store().read(|r| plan(r, now, &cfg(2))).expect("plan");
    assert_eq!(
        traces_of(&p),
        vec![Effect::TraceExpire {
            cutoff_us: now - 1_000_000
        }]
    );

    a.apply(
        &Build::new(BASE + 1, 1, 10)
            .cmd(vec![Effect::ClusterVersionSet { version: 4 }])
            .at(2, 1),
    )
    .expect("raise");
    // Version 4: one bounded step, and more to come (five due, two a step).
    let p = node.store().read(|r| plan(r, now, &cfg(2))).expect("plan");
    assert_eq!(
        traces_of(&p),
        vec![Effect::TraceTrim {
            cutoff_us: now - 1_000_000,
            limit: 2
        }]
    );
    assert!(p.more);
    // A limit above what is due: one step, nothing more.
    let p = node.store().read(|r| plan(r, now, &cfg(64))).expect("plan");
    assert_eq!(traces_of(&p).len(), 1);
    assert!(!p.more);
    // Nothing due: nothing planned.
    let p = node
        .store()
        .read(|r| plan(r, BASE, &cfg(64)))
        .expect("plan");
    assert!(traces_of(&p).is_empty());
    drop(a);
}

// ---------------------------------------------------------------------------
// The facade
// ---------------------------------------------------------------------------

mod facade {
    use super::*;
    use crate::rsm::batcher::BatcherConfig;
    use crate::rsm::facade::real::RaftFacade;
    use crate::rsm::facade::{ApiReq, Deadline, PopReq, PushReq, ReqCtx, Rsm, RsmBuildCtx};

    fn build_ctx(dir: &Path) -> RsmBuildCtx {
        std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
        RsmBuildCtx {
            data_dir: dir.display().to_string(),
            notifier: crate::notify::Notifier::new(false),
            disk_high_pct: 99.9,
            disk_low_pct: 99.8,
        }
    }

    fn ctx(tenant: &str) -> ReqCtx {
        ReqCtx::new(tenant, Deadline::after(Duration::from_secs(10)))
    }

    /// A node; its leader raises the cluster version every `raise_ms` (0:
    /// never, so it stays at the baseline, 3), and runs maintenance every
    /// `maintenance_ms` with the given trace retention.
    async fn open(dir: &Path, raise_ms: u64, maintenance_ms: u64, retention_s: i64) -> RaftFacade {
        let ctx = build_ctx(dir);
        let mut b = BatcherConfig {
            cluster_version_every_ms: raise_ms,
            maintenance_every_ms: maintenance_ms,
            ..BatcherConfig::from_env()
        };
        b.maintenance.trace_retention_s = retention_s;
        b.maintenance.trace_trim_limit = 7;
        tokio::task::spawn_blocking(move || RaftFacade::open_with(&ctx, b))
            .await
            .expect("open task")
            .expect("open facade")
    }

    async fn call(
        f: &RaftFacade,
        tenant: &str,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: Value,
    ) -> (u16, Value) {
        let out = f
            .api(
                ctx(tenant),
                ApiReq {
                    method: method.into(),
                    path: path.into(),
                    query: query.map(str::to_string),
                    body: if body.is_null() {
                        Vec::new()
                    } else {
                        body.to_string().into_bytes()
                    },
                },
            )
            .await
            .unwrap_or_else(|e| panic!("{method} {path}: {e:?}"));
        let v = serde_json::from_str(&out.body).unwrap_or(Value::Null);
        (out.status, v)
    }

    async fn post(f: &RaftFacade, tenant: &str, body: Value) -> Value {
        let (status, v) = call(f, tenant, "POST", "/api/v1/traces", None, body).await;
        assert_eq!(status, 201, "{v}");
        v
    }

    /// The partition id the queue's one message lives in (a pop names it).
    async fn partition_of(f: &RaftFacade, queue: &str) -> String {
        f.push(
            ctx(T0),
            PushReq {
                raw: format!(r#"{{"items":[{{"queue":"{queue}","payload":{{"n":1}},"transactionId":"m-1"}}]}}"#)
                    .into_bytes(),
            },
        )
        .await
        .expect("push");
        let p = f
            .pop_wildcard(
                ctx(T0),
                PopReq {
                    queue: queue.into(),
                    group: None,
                    batch: 1,
                    auto_ack: false,
                    wait: false,
                    timeout_ms: 1000,
                    options: Default::default(),
                },
            )
            .await
            .expect("pop");
        let v: Value = serde_json::from_str(&p.body).expect("pop body");
        v["partitionId"].as_str().expect("partitionId").to_string()
    }

    /// Until the cluster version admits `want`. A node alone raises it to
    /// what this build reads, which is at or above every version a test names.
    async fn until_version(f: &RaftFacade, want: u32) {
        let end = Instant::now() + Duration::from_secs(20);
        while f.cluster_version() < want {
            assert!(
                Instant::now() < end,
                "the cluster version never reached {want}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn close(f: RaftFacade) {
        f.shutdown().await;
    }

    fn created(v: &Value) -> Vec<String> {
        v["traces"]
            .as_array()
            .expect("traces")
            .iter()
            .map(|t| t["data"]["k"].as_str().unwrap_or("?").to_string())
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn every_route_answers_ram_and_disk_rows_together() {
        let dir = tmp_dir("traces-routes");
        // Version 3: the old kinds, rows in RAM.
        let f = open(&dir, 0, 0, 7 * 24 * 3600).await;
        assert_eq!(f.cluster_version(), 3);
        let part = partition_of(&f, "tq").await;
        for k in ["r1", "r2", "r3"] {
            post(&f, T0, serde_json::json!({"transactionId":"m-1","partitionId":part,"traceNames":["shared","old"],"eventType":"step","data":{"k":k}})).await;
        }
        post(
            &f,
            T0,
            serde_json::json!({"transactionId":"solo","traceNames":["shared"],"data":{"k":"r4"}}),
        )
        .await;
        post(
            &f,
            "other",
            serde_json::json!({"transactionId":"m-1","traceNames":["shared"],"data":{"k":"x1"}}),
        )
        .await;
        assert_eq!(f.trace_rows_for_test(), (5, 0));
        close(f).await;

        // Every member reads 4 (one node): the cluster version rises, and
        // the same routes write to disk.
        let f = open(&dir, 50, 0, 7 * 24 * 3600).await;
        until_version(&f, 4).await;
        for k in ["d1", "d2"] {
            post(&f, T0, serde_json::json!({"transactionId":"m-1","partitionId":part,"traceNames":["shared","new"],"eventType":"step","data":{"k":k}})).await;
        }
        post(&f, T0, serde_json::json!({"transactionId":"solo","traceNames":["shared","shared"],"data":{"k":"d3"}})).await;
        post(
            &f,
            "other",
            serde_json::json!({"transactionId":"m-1","traceNames":["shared"],"data":{"k":"x2"}}),
        )
        .await;
        assert_eq!(f.trace_rows_for_test(), (5, 4));

        // By message: both stores, newest first, this tenant only.
        let path = format!("/api/v1/traces/{part}/m-1");
        let (s, v) = call(&f, T0, "GET", &path, None, Value::Null).await;
        assert_eq!(s, 200);
        assert_eq!(created(&v), vec!["d2", "d1", "r3", "r2", "r1"]);
        assert_eq!(v["total"], 5);
        assert_eq!(v["events"], v["traces"]);
        let t = &v["traces"][0];
        assert_eq!(t["event_type"], "step");
        assert_eq!(t["transaction_id"], "m-1");
        assert_eq!(t["queue_name"], "tq");
        assert_eq!(t["trace_names"], serde_json::json!(["shared", "new"]));
        // Paged: the second and third of five.
        let (_, v) = call(&f, T0, "GET", &path, Some("limit=2&offset=1"), Value::Null).await;
        assert_eq!(created(&v), vec!["d1", "r3"]);
        assert_eq!(v["total"], 5);
        assert_eq!(v["pagination"], serde_json::json!({"limit":2,"offset":1}));
        // Another tenant's partition id answers nothing.
        let (_, v) = call(&f, "other", "GET", &path, None, Value::Null).await;
        assert_eq!(v["total"], 0);

        // By name.
        let (_, v) = call(
            &f,
            T0,
            "GET",
            "/api/v1/traces/by-name/shared",
            None,
            Value::Null,
        )
        .await;
        assert_eq!(created(&v), vec!["d3", "d2", "d1", "r4", "r3", "r2", "r1"]);
        assert_eq!(v["total"], 7);
        let (_, v) = call(
            &f,
            T0,
            "GET",
            "/api/v1/traces/by-name/shared",
            Some("limit=3&offset=2"),
            Value::Null,
        )
        .await;
        assert_eq!(created(&v), vec!["d1", "r4", "r3"]);
        let (_, v) = call(
            &f,
            T0,
            "GET",
            "/api/v1/traces/by-name/old",
            None,
            Value::Null,
        )
        .await;
        assert_eq!(created(&v), vec!["r3", "r2", "r1"]);
        let (_, v) = call(
            &f,
            "other",
            "GET",
            "/api/v1/traces/by-name/shared",
            None,
            Value::Null,
        )
        .await;
        assert_eq!(created(&v), vec!["x2", "x1"]);

        // The name list: counts over both stores; a name repeated in one
        // trace counts twice, as before; messages are distinct.
        let (_, v) = call(&f, T0, "GET", "/api/v1/traces/names", None, Value::Null).await;
        assert_eq!(v["total"], 3);
        let rows = v["trace_names"].as_array().expect("rows");
        let names: Vec<&str> = rows
            .iter()
            .map(|r| r["trace_name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["new", "old", "shared"]);
        assert_eq!(rows[2]["trace_count"], 8);
        assert_eq!(rows[2]["message_count"], 2);
        assert_eq!(rows[0]["trace_count"], 2);
        assert_eq!(rows[1]["message_count"], 1);
        let (_, v) = call(
            &f,
            T0,
            "GET",
            "/api/v1/traces/names",
            Some("limit=1&offset=1"),
            Value::Null,
        )
        .await;
        assert_eq!(v["trace_names"][0]["trace_name"], "old");
        assert_eq!(v["total"], 3);

        // A trace whose key would pass the store's limit is refused, never
        // proposed.
        let long = "z".repeat(600);
        let (s, _) = call(
            &f,
            T0,
            "POST",
            "/api/v1/traces",
            None,
            serde_json::json!({"transactionId":long,"traceNames":["a"]}),
        )
        .await;
        assert_eq!(s, 400);
        let (s, _) = call(
            &f,
            T0,
            "POST",
            "/api/v1/traces",
            None,
            serde_json::json!({"transactionId":"ok","traceNames":[long]}),
        )
        .await;
        assert_eq!(s, 400);
        assert_eq!(f.trace_rows_for_test(), (5, 4));

        // A restart reads the same.
        close(f).await;
        let f = open(&dir, 50, 0, 7 * 24 * 3600).await;
        let (_, v) = call(
            &f,
            T0,
            "GET",
            "/api/v1/traces/by-name/shared",
            None,
            Value::Null,
        )
        .await;
        assert_eq!(v["total"], 7);
        close(f).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn retention_drains_both_stores_in_bounded_steps() {
        let dir = tmp_dir("traces-retention");
        let f = open(&dir, 0, 0, 7 * 24 * 3600).await;
        for i in 0..9 {
            post(&f, T0, serde_json::json!({"transactionId":format!("r{i}"),"traceNames":["n"],"data":{"k":"r"}})).await;
        }
        close(f).await;
        let f = open(&dir, 50, 0, 7 * 24 * 3600).await;
        until_version(&f, 4).await;
        for i in 0..20 {
            post(&f, T0, serde_json::json!({"transactionId":format!("d{i}"),"traceNames":["n"],"data":{"k":"d"}})).await;
        }
        assert_eq!(f.trace_rows_for_test(), (9, 20));
        close(f).await;
        // A one-second retention and a fast tick: everything is due, and the
        // leader trims seven a step from each store until none is left.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let f = open(&dir, 50, 100, 1).await;
        let end = Instant::now() + Duration::from_secs(30);
        while f.trace_rows_for_test() != (0, 0) {
            assert!(
                Instant::now() < end,
                "retention left {:?}",
                f.trace_rows_for_test()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (_, v) = call(&f, T0, "GET", "/api/v1/traces/by-name/n", None, Value::Null).await;
        assert_eq!(v["total"], 0);
        close(f).await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
