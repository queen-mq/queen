//! The trace environment on its own: writes, every read, the bounded trim,
//! the tenant purge, the watermark across a reopen, an aborted entry, the
//! snapshot copy, the pager's order and the key limit.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::*;

static SEQ: AtomicU64 = AtomicU64::new(0);

/// LMDB's default key limit, which every build of this broker runs with.
const MAX_KEY: usize = 511;

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-traces-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn small() -> TraceOpts {
    TraceOpts {
        map_bytes: Some(64 << 20),
        ..TraceOpts::default()
    }
}

fn ev(tenant: &str, pid: Option<Pid>, txn: &str, names: &[&str], at: i64) -> TraceEvent {
    TraceEvent {
        trace_id: [(at % 251) as u8; 16],
        tenant: tenant.into(),
        pid,
        message_id: None,
        txn: txn.into(),
        consumer_group: Some("g".into()),
        event_type: "info".into(),
        data: format!(r#"{{"at":{at},"pad":"{}"}}"#, "x".repeat(5000)).into_bytes(),
        worker: None,
        names: names.iter().map(|s| s.to_string()).collect(),
        created_at_us: at,
    }
}

/// `(created_at, id)` of a message's traces, newest first.
fn by_message(t: &TraceStore, tenant: &str, pid: Option<Pid>, txn: &str) -> Vec<(i64, TraceId)> {
    let mut out = Vec::new();
    t.read(|r| {
        r.message_rev(tenant, pid, txn, &mut |at, _, id| {
            out.push((at, id));
            true
        })
    })
    .unwrap();
    out
}

fn by_name(t: &TraceStore, tenant: &str, name: &str) -> Vec<(i64, TraceId)> {
    let mut out = Vec::new();
    t.read(|r| {
        r.name_rev(tenant, name, &mut |at, _, id| {
            out.push((at, id));
            true
        })
    })
    .unwrap();
    out
}

fn rows(t: &TraceStore) -> (u64, u64, u64, u64) {
    t.read(|r| r.rows()).unwrap()
}

#[test]
fn a_recorded_trace_is_read_back_by_message_by_name_and_in_the_name_list() {
    let dir = scratch("rw");
    let t = open(&dir, small()).unwrap();
    assert_eq!(t.applied_index(), 0);
    let a = ev("acme", Some(7), "txn-1", &["checkout", "pay"], 1_000);
    let b = ev("acme", Some(7), "txn-1", &["checkout"], 2_000);
    let c = ev("acme", None, "txn-2", &["checkout", "checkout"], 3_000);
    let other = ev("zeta", Some(7), "txn-1", &["checkout"], 4_000);
    t.write_entry(10, |w| {
        w.record(0, &a)?;
        w.record(2, &b)?;
        w.record(3, &c)?;
        w.record(4, &other)
    })
    .unwrap();
    assert_eq!(t.applied_index(), 10);

    // One message, newest first, this tenant only.
    let m = by_message(&t, "acme", Some(7), "txn-1");
    assert_eq!(
        m,
        vec![
            (
                2_000,
                TraceId {
                    index: 10,
                    ordinal: 2
                }
            ),
            (
                1_000,
                TraceId {
                    index: 10,
                    ordinal: 0
                }
            ),
        ]
    );
    assert!(by_message(&t, "acme", Some(8), "txn-1").is_empty());
    assert!(by_message(&t, "acme", None, "txn-1").is_empty());

    // A name, newest first; a name repeated in one trace is one row.
    let n = by_name(&t, "acme", "checkout");
    assert_eq!(
        n.iter().map(|(at, _)| *at).collect::<Vec<_>>(),
        vec![3_000, 2_000, 1_000]
    );
    assert_eq!(by_name(&t, "acme", "pay").len(), 1);
    assert!(by_name(&t, "acme", "nope").is_empty());
    assert_eq!(by_name(&t, "zeta", "checkout").len(), 1);

    // The listing: a repeated name counts twice in `traces`, as the legacy
    // listing did; messages are distinct (pid?, txn).
    let mut stats = BTreeMap::new();
    t.read(|r| r.name_stats("acme", &mut stats)).unwrap();
    assert_eq!(
        stats.keys().cloned().collect::<Vec<_>>(),
        vec!["checkout", "pay"]
    );
    let ck = &stats["checkout"];
    assert_eq!(ck.traces, 4);
    assert_eq!(ck.messages.len(), 2);
    assert_eq!(ck.last_seen_us, 3_000);
    assert_eq!(stats["pay"].traces, 1);
    assert_eq!(stats["pay"].last_seen_us, 1_000);

    // The bodies come back whole.
    let body = t
        .read(|r| {
            r.body(TraceId {
                index: 10,
                ordinal: 3,
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(body, c);
    assert!(t
        .read(|r| r.body(TraceId {
            index: 10,
            ordinal: 1
        }))
        .unwrap()
        .is_none());
    drop(t);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_trim_deletes_at_most_its_limit_oldest_first_and_every_row_of_each() {
    let dir = scratch("trim");
    let t = open(&dir, small()).unwrap();
    // Ten traces, written out of time order across two entries.
    t.write_entry(1, |w| {
        for i in [5i64, 1, 9, 3, 7] {
            w.record(
                i as u32,
                &ev("acme", Some(1), &format!("t{i}"), &["a", "b"], i * 100),
            )?;
        }
        Ok(())
    })
    .unwrap();
    t.write_entry(2, |w| {
        for i in [2i64, 8, 4, 10, 6] {
            w.record(
                i as u32,
                &ev("acme", Some(1), &format!("t{i}"), &["a"], i * 100),
            )?;
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(rows(&t), (10, 10, 15, 10));
    assert_eq!(t.read(|r| r.due(650, usize::MAX)).unwrap(), 6);
    assert_eq!(t.read(|r| r.due(650, 4)).unwrap(), 4);

    // Bounded: four of the six due go, the oldest.
    let n = t.write_entry(3, |w| w.trim(650, 4)).unwrap();
    assert_eq!(n, 4);
    let left: Vec<i64> = by_name(&t, "acme", "a")
        .into_iter()
        .map(|(at, _)| at)
        .collect();
    assert_eq!(left, vec![1000, 900, 800, 700, 600, 500]);
    assert_eq!(t.read(|r| r.due(650, usize::MAX)).unwrap(), 2);
    // Every row of a trimmed trace went: bodies, both indexes, expiry.
    let (b, x, nm, e) = rows(&t);
    assert_eq!((b, x, e), (6, 6, 6));
    // Names left: 600, 800 and 1000 carry "a" only; 500, 700 and 900 "a" and "b".
    assert_eq!(nm, 9);
    assert!(by_message(&t, "acme", Some(1), "t1").is_empty());
    assert_eq!(by_message(&t, "acme", Some(1), "t5").len(), 1);

    // The rest of what is due, then nothing.
    assert_eq!(t.write_entry(4, |w| w.trim(650, 4)).unwrap(), 2);
    assert_eq!(t.write_entry(5, |w| w.trim(650, 4)).unwrap(), 0);
    assert_eq!(rows(&t).0, 4);
    drop(t);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_tenant_purge_takes_every_trace_of_that_tenant_and_no_other() {
    let dir = scratch("purge");
    let t = open(&dir, small()).unwrap();
    t.write_entry(1, |w| {
        w.record(0, &ev("acme", Some(1), "t1", &["a"], 100))?;
        w.record(1, &ev("acme", None, "t2", &["a", "b"], 200))?;
        // A tenant whose name is a prefix of the other's.
        w.record(2, &ev("acm", Some(1), "t1", &["a"], 300))?;
        w.record(3, &ev("acmez", Some(1), "t1", &["a"], 400))
    })
    .unwrap();
    assert_eq!(t.write_entry(2, |w| w.purge_tenant("acme")).unwrap(), 2);
    assert!(by_name(&t, "acme", "a").is_empty());
    assert!(by_name(&t, "acme", "b").is_empty());
    assert_eq!(by_name(&t, "acm", "a").len(), 1);
    assert_eq!(by_name(&t, "acmez", "a").len(), 1);
    assert_eq!(rows(&t), (2, 2, 2, 2));
    assert_eq!(t.read(|r| r.due(i64::MAX, usize::MAX)).unwrap(), 2);
    drop(t);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_applied_index_survives_a_reopen_and_refuses_an_entry_at_or_below_it() {
    let dir = scratch("watermark");
    let t = open(&dir, small()).unwrap();
    t.write_entry(41, |w| w.record(0, &ev("acme", None, "t", &["a"], 1)))
        .unwrap();
    let digest = t.read(|r| r.digest()).unwrap();
    drop(t);
    let t = open(&dir, small()).unwrap();
    assert_eq!(t.applied_index(), 41);
    assert_eq!(t.read(|r| r.applied_index()).unwrap(), 41);
    assert_eq!(t.read(|r| r.digest()).unwrap(), digest);
    // A replay of entry 41 (or anything older) is the caller's to skip; here
    // it is refused rather than written twice.
    assert!(t
        .write_entry(41, |w| w.record(0, &ev("acme", None, "t", &["a"], 1)))
        .is_err());
    assert!(t.write_entry(7, |_| Ok(())).is_err());
    assert_eq!(rows(&t), (1, 1, 1, 1));
    drop(t);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_entry_that_fails_half_way_leaves_nothing_behind() {
    let dir = scratch("abort");
    let t = open(&dir, small()).unwrap();
    let r = t.write_entry(5, |w| {
        w.record(0, &ev("acme", None, "t", &["a"], 1))?;
        Err::<(), _>(StoreError::Io("injected".into()))
    });
    assert!(r.is_err());
    assert_eq!(t.applied_index(), 0);
    assert_eq!(rows(&t), (0, 0, 0, 0));
    // The same entry applies cleanly afterwards.
    t.write_entry(5, |w| w.record(0, &ev("acme", None, "t", &["a"], 1)))
        .unwrap();
    assert_eq!(rows(&t), (1, 1, 1, 1));
    drop(t);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_key_over_the_limit_is_refused_before_anything_is_written() {
    let long = "n".repeat(600);
    let e = ev("acme", None, &long, &["a"], 1);
    assert!(check_keys(&e, MAX_KEY).is_err());
    let e = ev("acme", None, "t", &["ok", &long], 1);
    assert!(check_keys(&e, MAX_KEY).is_err());
    let e = ev("acme", Some(3), "t", &["ok", "fine"], 1);
    assert!(check_keys(&e, MAX_KEY).is_ok());

    let dir = scratch("keys");
    let t = open(&dir, small()).unwrap();
    let r = t.write_entry(1, |w| {
        w.record(0, &ev("acme", None, "t", &["ok", &long], 1))
    });
    assert!(matches!(r, Err(StoreError::KeyTooLong { .. })), "{r:?}");
    assert_eq!(rows(&t), (0, 0, 0, 0));
    drop(t);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_copy_reopens_with_the_same_rows_and_applied_index() {
    let dir = scratch("copy-src");
    let to = scratch("copy-dst");
    let t = open(&dir, small()).unwrap();
    t.write_entry(3, |w| {
        for i in 0..20u32 {
            w.record(
                i,
                &ev(
                    "acme",
                    Some(1),
                    &format!("t{}", i % 4),
                    &["a", "b"],
                    i as i64,
                ),
            )?;
        }
        Ok(())
    })
    .unwrap();
    t.write_entry(9, |w| w.trim(5, 100)).unwrap();
    t.copy_to(&to).unwrap();
    let want = t.read(|r| r.digest()).unwrap();
    let c = open(&to, small()).unwrap();
    assert_eq!(c.applied_index(), 9);
    assert_eq!(c.read(|r| r.digest()).unwrap(), want);
    assert_eq!(rows(&c), (15, 15, 30, 15));
    drop((t, c));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&to);
}

#[test]
fn one_directory_is_one_instance_and_a_failed_sync_is_retried() {
    let dir = scratch("registry");
    let a = open(&dir, small()).unwrap();
    let b = open(&dir, TraceOpts::default()).unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    assert!(lookup(&dir).is_some_and(|c| Arc::ptr_eq(&a, &c)));
    // Nothing committed: no sync to do.
    a.sync().unwrap();
    a.write_entry(1, |w| w.record(0, &ev("acme", None, "t", &["a"], 1)))
        .unwrap();
    a.fail_next_sync();
    assert!(matches!(
        a.sync(),
        Err(StoreError::CommitFailed { durable: true, .. })
    ));
    // The failed point owes its sync to the next one.
    a.sync().unwrap();
    drop((a, b));
    assert!(lookup(&dir).is_none());
    // Closed: the same directory opens again in this process.
    let c = open(&dir, small()).unwrap();
    assert_eq!(c.applied_index(), 1);
    drop(c);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_pager_pages_as_a_stable_sort_by_time_then_tie() {
    // Rows fed newest first (ties in any order) page exactly as sorting them
    // all by (created desc, tie asc) and skipping/taking does.
    let mut rows: Vec<(i64, Vec<u8>, usize)> = Vec::new();
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for i in 0..300 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let at = (x % 40) as i64;
        let tie = vec![(x >> 8) as u8, (x >> 16) as u8, i as u8];
        rows.push((at, tie, i));
    }
    let mut sorted = rows.clone();
    sorted.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    // Fed by time only (descending), ties shuffled by their insertion order.
    let mut fed = rows.clone();
    fed.sort_by(|a, b| b.0.cmp(&a.0));
    for (offset, limit) in [(0, 10), (0, 1000), (7, 13), (295, 10), (300, 5), (1000, 3)] {
        let mut p = Pager::new(offset, limit);
        for (at, tie, i) in &fed {
            p.push(*at, tie, *i);
        }
        let (total, page) = p.finish();
        let want: Vec<usize> = sorted
            .iter()
            .skip(offset)
            .take(limit)
            .map(|r| r.2)
            .collect();
        assert_eq!(total, 300);
        assert_eq!(page, want, "offset {offset} limit {limit}");
    }
}

#[test]
fn ties_order_a_legacy_row_before_a_disk_row_of_the_same_message() {
    let legacy = keys::trace("acme", Some(7), "t", 3);
    let lt = legacy_tie("acme", &legacy).unwrap();
    let dir = scratch("tie");
    let t = open(&dir, small()).unwrap();
    t.write_entry(1, |w| w.record(0, &ev("acme", Some(7), "t", &["a"], 5)))
        .unwrap();
    let mut ties = Vec::new();
    t.read(|r| {
        r.message_rev("acme", Some(7), "t", &mut |_, tie, _| {
            ties.push(tie.to_vec());
            true
        })?;
        r.name_rev("acme", "a", &mut |_, tie, _| {
            ties.push(tie.to_vec());
            true
        })
    })
    .unwrap();
    assert_eq!(ties.len(), 2);
    // The same message's tie from either index.
    assert_eq!(ties[0], ties[1]);
    assert!(lt < ties[0]);
    // A message that sorts lower comes first whatever its source.
    let lower = legacy_tie("acme", &keys::trace("acme", Some(7), "s", 99)).unwrap();
    assert!(lower < ties[0]);
    drop(t);
    let _ = std::fs::remove_dir_all(&dir);
}
