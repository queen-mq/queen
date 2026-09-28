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
