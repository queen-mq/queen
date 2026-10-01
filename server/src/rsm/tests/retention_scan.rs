//! Background retention ([`crate::rsm::retention_scan`], 2026-09-28): what the
//! scanner proposes from an old snapshot is judged again against committed
//! state and every entry in flight before it is planned.

use crate::rsm::effect::Effect;
use crate::rsm::maintenance;
use crate::rsm::planner::Overlay;
use crate::rsm::retention_scan::{judge, Proposal, ScanShared};
use crate::rsm::store::rows::PartitionRow;
use crate::rsm::store::{HeedStore, Store, TypedReads, TypedWrites, Writes};

const DAY_US: i64 = 86_400 * 1_000_000;
/// "Now": 100 days after the epoch, so a partition written at 1 µs is idle
/// past the 30-day cleanup age.
const NOW: i64 = 100 * DAY_US;

struct Dir(std::path::PathBuf);

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn store(tag: &str, rows: &[(u64, PartitionRow)]) -> (Dir, HeedStore) {
    let d = std::env::temp_dir().join(format!(
        "queen-rsm-retention-scan-{tag}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let store = HeedStore::open(&d.join("store"), &super::apply::store_opts()).unwrap();
    {
        let mut w = store.write().unwrap();
        for (pid, row) in rows {
            w.put_partition(*pid, row).unwrap();
        }
        w.commit().unwrap();
    }
    (Dir(d), store)
}

fn part(log_start: u64, txns_start: u64, last_offset: i64, last_write_at_us: i64) -> PartitionRow {
    let mut row = PartitionRow::new([7; 16], "t", "q", "p", 1);
    row.log_start = log_start;
    row.txns_start = txns_start;
    row.last_offset = last_offset;
    row.last_write_at_us = last_write_at_us;
    row
}

fn watermark(pid: u64, log_start: u64, txns_start: u64) -> Proposal {
    Proposal::Watermark {
        pid,
        log_start,
        txns_start,
    }
}

fn wm_effect(pid: u64, log_start: u64, txns_start: u64) -> Effect {
    Effect::Watermark {
        pid,
        log_start,
        txns_start,
    }
}

#[test]
fn a_stale_watermark_proposal_never_moves_back() {
    let (_d, store) = store("watermark", &[(1, part(10, 5, 20, NOW))]);
    let scan = ScanShared::new(&maintenance::Config::default());
    store
        .read(|r| {
            let mut ov = Overlay::new(r.next_pid()?, r.kv_version_next()?);
            let run = |ov: &Overlay, p: &[Proposal]| judge(r, ov, NOW, &scan, p).unwrap();

            // Behind what is committed: dropped (apply would refuse it).
            assert_eq!(run(&ov, &[watermark(1, 8, 4)]), (vec![], 1));
            // Ahead: planned as proposed.
            assert_eq!(
                run(&ov, &[watermark(1, 12, 7)]).0,
                vec![wm_effect(1, 12, 7)]
            );
            // Half behind: the watermark that moves moves, the other stays.
            assert_eq!(
                run(&ov, &[watermark(1, 12, 3)]).0,
                vec![wm_effect(1, 12, 5)]
            );
            // One per partition per entry.
            assert_eq!(
                run(&ov, &[watermark(1, 12, 7), watermark(1, 13, 8)]),
                (vec![wm_effect(1, 12, 7)], 1)
            );

            // An entry in flight already moved it further: judged against that.
            ov.apply_effects(&[wm_effect(1, 15, 9)]);
            assert_eq!(run(&ov, &[watermark(1, 12, 7)]), (vec![], 1));
            assert_eq!(
                run(&ov, &[watermark(1, 18, 10)]).0,
                vec![wm_effect(1, 18, 10)]
            );

            // A partition that is gone: dropped.
            assert_eq!(run(&ov, &[watermark(99, 18, 10)]), (vec![], 1));
            Ok(())
        })
        .unwrap();
}

#[test]
fn a_stale_cleanup_proposal_is_judged_again() {
    // pid 2: empty and idle since 1 µs. pid 3: written just now.
    let (_d, store) = store(
        "cleanup",
        &[(2, part(21, 21, 20, 1)), (3, part(21, 21, 20, NOW))],
    );
    let scan = ScanShared::new(&maintenance::Config::default());
    store
        .read(|r| {
            let mut ov = Overlay::new(r.next_pid()?, r.kv_version_next()?);
            let run = |ov: &Overlay, p: &[Proposal]| judge(r, ov, NOW, &scan, p).unwrap();
            let delete = |pid| Proposal::Delete { pid };

            assert_eq!(
                run(&ov, &[delete(2)]).0,
                vec![Effect::PartitionDelete { pid: 2 }]
            );
            // Written since the scanner looked: no longer dead.
            assert_eq!(run(&ov, &[delete(3)]), (vec![], 1));
            // Gone already.
            assert_eq!(run(&ov, &[delete(99)]), (vec![], 1));
            // Anything in flight names it: kept, as the walk keeps it.
            ov.apply_effects(&[wm_effect(2, 21, 21)]);
            assert_eq!(run(&ov, &[delete(2)]), (vec![], 1));
            Ok(())
        })
        .unwrap();
}

#[test]
fn the_scanner_walks_every_partition_in_slices_and_wraps() {
    let rows: Vec<(u64, PartitionRow)> = (1..=5).map(|pid| (pid, part(0, 0, -1, NOW))).collect();
    let (_d, store) = store("slices", &rows);
    let cfg = maintenance::Config::default();
    let mut cursor = 0;
    let mut visited = Vec::new();
    let mut wraps = 0;
    for _ in 0..6 {
        let slice = store
            .read(|r| maintenance::scan_slice(r, NOW, &cfg, &mut cursor, 2))
            .unwrap();
        visited.push(slice.visited);
        wraps += usize::from(slice.wrapped);
        // No queue row: nothing to judge, nothing proposed.
        assert!(slice.proposals.is_empty());
    }
    assert_eq!(visited, vec![2, 2, 1, 2, 2, 1]);
    assert_eq!(wraps, 2, "a round is 5 partitions, then it starts over");
}

/// End to end through the facade: with the scanner on (and so the walk off the
/// planning thread), a queue's retention still expires its messages.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn with_the_scanner_on_retention_still_expires_messages() {
    use crate::rsm::batcher::BatcherConfig;
    use crate::rsm::facade::real::RaftFacade;
    use crate::rsm::facade::{ApiReq, Deadline, PopReq, PushReq, ReqCtx, Rsm, RsmBuildCtx};
    use crate::rsm::retention_scan::ScanConfig;
    use std::time::Duration;

    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-retention-scan-e2e-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let _cleanup = Dir(dir.clone());
    let ctx = || {
        ReqCtx::new(
            crate::config::DEFAULT_TENANT,
            Deadline::after(Duration::from_secs(5)),
        )
    };
    let cfg = BatcherConfig {
        maintenance_every_ms: 200,
        retention_scan: Some(ScanConfig {
            per_s: 10_000,
            slice: 64,
        }),
        ..BatcherConfig::default()
    };
    let facade = RaftFacade::open_with(
        &RsmBuildCtx {
            data_dir: dir.display().to_string(),
            notifier: crate::notify::Notifier::new(false),
            disk_high_pct: 85.0,
            disk_low_pct: 80.0,
        },
        cfg,
    )
    .expect("open facade");

    let configured = facade
        .api(
            ctx(),
            ApiReq {
                method: "POST".into(),
                path: "/api/v1/configure".into(),
                query: None,
                body: serde_json::to_vec(&serde_json::json!({
                    "queue": "ret",
                    "options": {"retentionEnabled": true, "retentionSeconds": 1}
                }))
                .unwrap(),
            },
        )
        .await
        .expect("configure");
    assert_eq!(configured.status, 200, "{}", configured.body);
    // "ret" keeps its messages 1 s; "keep" has no retention: the control.
    facade
        .push(
            ctx(),
            PushReq {
                raw: br#"{"items":[
                    {"queue":"ret","payload":{"n":1}},
                    {"queue":"ret","payload":{"n":2}},
                    {"queue":"ret","payload":{"n":3}},
                    {"queue":"keep","payload":{"n":1}},
                    {"queue":"keep","payload":{"n":2}},
                    {"queue":"keep","payload":{"n":3}}
                ]}"#
                .to_vec(),
            },
        )
        .await
        .expect("push");
    let pop = |queue: &'static str| {
        facade.pop_wildcard(
            ctx(),
            PopReq {
                queue: queue.into(),
                group: None,
                batch: 10,
                auto_ack: false,
                wait: false,
                timeout_ms: 100,
                options: Default::default(),
            },
        )
    };
    let count = |popped: &crate::rsm::facade::PopOut| -> usize {
        if popped.empty {
            return 0;
        }
        let body: serde_json::Value = serde_json::from_str(&popped.body).expect("pop body");
        body["messages"].as_array().expect("messages").len()
    };

    // Past the 1 s cutoff, the scanner proposes the watermark and the planner
    // plans it: a pop of "ret" then finds nothing left.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let mut left = usize::MAX;
    for _ in 0..40 {
        left = count(&pop("ret").await.expect("pop ret"));
        if left == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(left, 0, "retention never expired the messages");
    assert_eq!(
        count(&pop("keep").await.expect("pop keep")),
        3,
        "the queue without retention keeps its messages"
    );
}

// ---------------------------------------------------------------------------
// Rounds: at most one per maintenance interval (2026-10-01)
// ---------------------------------------------------------------------------

/// The gate on its own: no wait for a leadership's first round, the rest of
/// the interval since the last one began, none once a round outran it.
#[test]
fn a_round_waits_out_the_interval_since_the_last_one_began() {
    use crate::rsm::retention_scan::round_wait;
    use std::time::{Duration, Instant};
    let every = Duration::from_millis(5_000);
    let t0 = Instant::now();
    assert_eq!(round_wait(None, t0, every), Duration::ZERO, "first round");
    assert_eq!(
        round_wait(Some(t0), t0 + Duration::from_millis(41), every),
        Duration::from_millis(4_959),
        "a small store wrapped after one slice: the rest of the interval"
    );
    assert_eq!(round_wait(Some(t0), t0 + every, every), Duration::ZERO);
    assert_eq!(
        round_wait(Some(t0), t0 + Duration::from_secs(20), every),
        Duration::ZERO,
        "a 20 s round over 1M partitions: the next starts at once"
    );
}

/// Wait until `f` holds, for at most `within`.
fn eventually(within: std::time::Duration, f: impl Fn() -> bool) -> bool {
    let end = std::time::Instant::now() + within;
    while std::time::Instant::now() < end {
        if f() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    f()
}

/// The scanner thread over a store smaller than one slice: before 2026-10-01
/// it walked it every slice pace (2,048 / 50,000 per s = 41 ms, ~24 rounds a
/// second, 0.22 of a core on the prod leader); now once per maintenance
/// interval, a new leadership's first round at once.
#[test]
fn a_store_smaller_than_a_slice_is_walked_once_per_interval() {
    use crate::rsm::retention_scan::{spawn, ScanConfig};
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    let rows: Vec<(u64, PartitionRow)> = (1..=3).map(|pid| (pid, part(0, 0, -1, NOW))).collect();
    let (_d, store) = store("rounds", &rows);
    let store = std::sync::Arc::new(store);
    let cfg = maintenance::Config::default();
    let scan = ScanConfig {
        per_s: 50_000,
        slice: 2_048,
    };
    let rounds = |s: &ScanShared| s.rounds.load(Ordering::Relaxed);

    // A 300 ms interval for one second: at most 1 + elapsed / interval.
    let every = Duration::from_millis(300);
    let shared = ScanShared::new(&cfg);
    spawn(store.clone(), cfg.clone(), scan, every, shared.clone()).expect("spawn");
    let t0 = Instant::now();
    shared.set_leading(true);
    assert!(
        eventually(Duration::from_secs(5), || rounds(&shared) >= 1),
        "a round ran"
    );
    std::thread::sleep(Duration::from_millis(1_000));
    let n = rounds(&shared);
    let bound = 1 + (t0.elapsed().as_millis() / every.as_millis()) as u64;
    shared.stop();
    assert!(
        n <= bound,
        "{n} rounds in {:?}, at most {bound}",
        t0.elapsed()
    );

    // A long interval: one round, then nothing until a NEW leadership, whose
    // first round starts at once (not an interval later).
    let shared = ScanShared::new(&cfg);
    spawn(
        store.clone(),
        cfg,
        scan,
        Duration::from_secs(3_600),
        shared.clone(),
    )
    .expect("spawn");
    shared.set_leading(true);
    assert!(
        eventually(Duration::from_secs(5), || rounds(&shared) == 1),
        "the first round starts at once"
    );
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(rounds(&shared), 1, "no second round inside the interval");
    shared.set_leading(false);
    shared.set_leading(true);
    assert!(
        eventually(Duration::from_secs(5), || rounds(&shared) == 2),
        "a new leadership's first round starts at once"
    );
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(rounds(&shared), 2);
    shared.stop();
}

// ---------------------------------------------------------------------------
// Judging a partition nothing can move (2026-10-01)
// ---------------------------------------------------------------------------

/// A read handle that counts what reaches the `txns` keyspace.
struct Counting<'a, R: crate::rsm::store::Reads + ?Sized> {
    r: &'a R,
    txns_scans: std::cell::Cell<usize>,
    txns_rows: std::cell::Cell<usize>,
}

impl<'a, R: crate::rsm::store::Reads + ?Sized> Counting<'a, R> {
    fn new(r: &'a R) -> Counting<'a, R> {
        Counting {
            r,
            txns_scans: std::cell::Cell::new(0),
            txns_rows: std::cell::Cell::new(0),
        }
    }

    fn note(&self, ks: crate::rsm::store::Keyspace, rows: usize) {
        if ks == crate::rsm::store::Keyspace::Txns {
            self.txns_scans.set(self.txns_scans.get() + 1);
            self.txns_rows.set(self.txns_rows.get() + rows);
        }
    }
}

impl<R: crate::rsm::store::Reads + ?Sized> crate::rsm::store::Reads for Counting<'_, R> {
    fn get_raw(
        &self,
        ks: crate::rsm::store::Keyspace,
        key: &[u8],
    ) -> crate::rsm::store::Result<Option<&[u8]>> {
        self.r.get_raw(ks, key)
    }

    fn get_with(
        &self,
        ks: crate::rsm::store::Keyspace,
        key: &[u8],
        f: &mut dyn FnMut(&[u8]),
    ) -> crate::rsm::store::Result<bool> {
        self.r.get_with(ks, key, f)
    }

    fn max_key_len(&self) -> usize {
        self.r.max_key_len()
    }

    fn scan_raw(
        &self,
        ks: crate::rsm::store::Keyspace,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
        cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> crate::rsm::store::Result<usize> {
        let n = self.r.scan_raw(ks, from, prefix, limit, cb)?;
        self.note(ks, n);
        Ok(n)
    }

    fn scan_rev_raw(
        &self,
        ks: crate::rsm::store::Keyspace,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
        cb: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> crate::rsm::store::Result<usize> {
        let n = self.r.scan_rev_raw(ks, from, prefix, limit, cb)?;
        self.note(ks, n);
        Ok(n)
    }
}

/// `n` one-message appends to `pid` from offset `base`, each with its txns
/// row stamped `created`.
fn txns_rows(store: &HeedStore, pid: u64, base: u64, n: u64, created: i64) {
    let mut w = store.write().unwrap();
    for off in base..base + n {
        let mut h = [0u8; 16];
        h[..8].copy_from_slice(&off.to_be_bytes());
        crate::rsm::dedup::record_txns(&mut w, pid, off, off, &[(h, off)], created).unwrap();
    }
    w.commit().unwrap();
}

const HOUR_US: i64 = 3_600 * 1_000_000;

/// The prod shape (2026-10-01): retention off, a 1 h dedup window, 1,500
/// messages pushed a day ago, both watermarks at 0. Nothing can move, and the
/// judgment reads no txns row; the walk before read 1,002 of them (the oldest,
/// then `row_limit + 1`) on every visit, to the same verdict.
#[test]
fn a_partition_nothing_can_move_is_judged_without_reading_its_txns_rows() {
    let (_d, store) = store("skip", &[(1, part(0, 0, 1_499, NOW - DAY_US))]);
    txns_rows(&store, 1, 0, 1_500, NOW - DAY_US);
    let cfg = maintenance::Config::default();
    store
        .read(|r| {
            let qcfg = super::apply::queue_config(0);
            let cut = maintenance::queue_cutoffs(r, NOW, &cfg, "t", "q", &qcfg);
            let part = r.partition(1)?.expect("partition");
            let now = Counting::new(r);
            let got = maintenance::judge_partition(&now, NOW, &cfg, 1, &part, &cut)?;
            assert_eq!(now.txns_scans.get(), 0, "no txns row read");
            let before = Counting::new(r);
            let want = maintenance::judge_partition_unskipped(&before, NOW, &cfg, 1, &part, &cut)?;
            assert_eq!(before.txns_rows.get(), 1 + cfg.row_limit + 1);
            assert_eq!(got, want);
            assert_eq!(got, None, "it holds messages: no delete either");
            Ok(())
        })
        .unwrap();
}

/// A partition WITH retention moves its watermarks exactly as before: the
/// log past every row older than the retention, the txns watermark past the
/// rows older than the dedup window, never past the log one.
#[test]
fn a_partition_with_retention_moves_its_watermarks_as_before() {
    // 10 rows two days old, then 10 from a minute ago.
    let (_d, store) = store("retained", &[(1, part(0, 0, 19, NOW - 60_000_000))]);
    txns_rows(&store, 1, 0, 10, NOW - 2 * DAY_US);
    txns_rows(&store, 1, 10, 10, NOW - 60_000_000);
    let cfg = maintenance::Config::default();
    store
        .read(|r| {
            let mut qcfg = super::apply::queue_config(0);
            qcfg.retention_enabled = true;
            qcfg.retention_seconds = 86_400;
            let cut = maintenance::queue_cutoffs(r, NOW, &cfg, "t", "q", &qcfg);
            let part = r.partition(1)?.expect("partition");
            let got = maintenance::judge_partition(r, NOW, &cfg, 1, &part, &cut)?;
            assert_eq!(
                got,
                Some(maintenance::Verdict::Watermark {
                    log_start: 10,
                    txns_start: 10,
                })
            );
            assert_eq!(
                got,
                maintenance::judge_partition_unskipped(r, NOW, &cfg, 1, &part, &cut)?
            );
            Ok(())
        })
        .unwrap();
}

/// The shortcuts against the walk as it was, over every combination that
/// decides them: which cutoffs apply, where the watermarks stand, how old the
/// rows are, whether the partition still holds anything, and none at all.
/// The verdict never differs.
#[test]
fn the_shortcuts_give_the_walks_verdict_in_every_case() {
    let ages = [None, Some(2 * DAY_US), Some(HOUR_US / 2), Some(2 * HOUR_US)];
    let mut cases = 0;
    // (nothing, a watermark, a delete): the grid reaches all three.
    let mut seen = [0usize; 3];
    for (i, age) in ages.iter().enumerate() {
        for (log_start, txns_start, last_offset) in [
            (0u64, 0u64, 19i64),
            (5, 5, 19),
            (10, 5, 19),
            (20, 20, 19),
            (20, 10, 19),
        ] {
            let tag = format!("grid-{i}-{log_start}-{txns_start}");
            let created = NOW - 40 * DAY_US;
            let mut row = part(log_start, txns_start, last_offset, 1);
            row.created_at_us = created;
            let (_d, store) = store(&tag, &[(1, row)]);
            if let Some(age) = age {
                txns_rows(&store, 1, 0, 20, NOW - age);
            }
            let cfg = maintenance::Config::default();
            let day_ago = Some(NOW - DAY_US);
            let cuts = [
                maintenance::cutoffs_for_test(None, None, None, NOW - HOUR_US),
                maintenance::cutoffs_for_test(day_ago, None, None, NOW - HOUR_US),
                maintenance::cutoffs_for_test(None, None, day_ago, NOW - HOUR_US),
                maintenance::cutoffs_for_test(None, day_ago, None, NOW - HOUR_US),
                maintenance::cutoffs_for_test(Some(NOW - HOUR_US / 4), None, None, NOW),
            ];
            store
                .read(|r| {
                    let part = r.partition(1)?.expect("partition");
                    for cut in &cuts {
                        let got = maintenance::judge_partition(r, NOW, &cfg, 1, &part, cut)?;
                        let want =
                            maintenance::judge_partition_unskipped(r, NOW, &cfg, 1, &part, cut)?;
                        assert_eq!(got, want, "{tag} {cut:?}");
                        cases += 1;
                        seen[match got {
                            None => 0,
                            Some(maintenance::Verdict::Watermark { .. }) => 1,
                            Some(maintenance::Verdict::Delete) => 2,
                        }] += 1;
                    }
                    Ok(())
                })
                .unwrap();
        }
    }
    assert_eq!(cases, 4 * 5 * 5);
    assert!(seen.iter().all(|n| *n > 0), "verdicts seen: {seen:?}");
}
