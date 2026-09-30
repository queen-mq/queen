//! Multi-push ([`Command::MultiPush`]): one command for a push request that
//! names several partitions. The single planner's plan is the reference — the
//! groups planned one after another against one overlay — and the lanes must
//! give the same verdicts, the same offsets and one logged command, whichever
//! lanes the groups' partitions sit in, whether they exist yet or are created
//! by the request, and however many other commands share the cycle.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::rsm::apply::SystemClock;
use crate::rsm::batcher::{
    Batcher, BatcherConfig, Command, CommandTx, MultiPushCommand, Reply, Submission,
};
use crate::rsm::entry::{Outcome, PushVerdict};
use crate::rsm::planner::PushCommand;
use crate::rsm::replicator::local::{LocalReplicator, NoWaker, OpenConfig};
use crate::rsm::replicator::log::{Fsync, LogOptions};
use crate::rsm::store::{HeedStore, Store, TypedReads};

use super::apply::{cfg, seg_opts, store_opts};
use super::planner_harness::{item, qcfg, rid, TENANT};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-multipush-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn open_repl(dir: &Path, store: Arc<HeedStore>) -> LocalReplicator<HeedStore> {
    LocalReplicator::open(
        store,
        OpenConfig {
            node_id: 1,
            log_dir: dir.join("log"),
            log_opts: LogOptions {
                segment_bytes: 64 << 10,
                fsync: Fsync::Off,
            },
            seg_root: dir.join("seg"),
            seg_opts: seg_opts(),
            apply_cfg: cfg(),
            apply_channel_capacity: 64,
            replay_deadline: Duration::from_secs(30),
            writer_pipeline: crate::rsm::replicator::local::writer_pipeline_from_env(),
        },
        Arc::new(NoWaker),
        Arc::new(SystemClock),
    )
    .expect("open replicator")
}

struct Node {
    tx: CommandTx,
    handle: tokio::task::JoinHandle<()>,
    repl: Arc<LocalReplicator<HeedStore>>,
    store: Arc<HeedStore>,
    dir: PathBuf,
}

impl Node {
    fn open(tag: &str, lanes: u64) -> Node {
        let dir = scratch(tag);
        let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
        let repl = Arc::new(open_repl(&dir, store.clone()));
        let cfg = BatcherConfig {
            lanes,
            pipeline: 4,
            propose_ms: 5_000,
            request_expire_every_ms: 3_600_000,
            ..BatcherConfig::default()
        };
        let (tx, handle) = Batcher::new(store.clone(), repl.clone(), cfg).spawn();
        Node {
            tx,
            handle,
            repl,
            store,
            dir,
        }
    }

    async fn close(self) {
        drop(self.tx);
        let _ = self.handle.await;
        drop(self.repl);
        drop(self.store);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn submit(tx: &CommandTx, command: Command) -> Reply {
    let (sub, rx) = Submission::new(command);
    tx.send(sub).await.expect("send command");
    rx.await.expect("await reply")
}

fn group(id: u64, queue: &str, partition: &str, txns: &[String]) -> PushCommand {
    PushCommand {
        request_id: rid(id),
        tenant: TENANT.to_string(),
        queue: queue.to_string(),
        partition: partition.to_string(),
        items: txns.iter().map(|t| item(t)).collect(),
        create_cfg: {
            let mut c = qcfg();
            c.dedup_window_seconds = 600;
            c
        },
    }
}

fn multi(id: u64, groups: Vec<PushCommand>) -> Command {
    Command::MultiPush(MultiPushCommand {
        request_id: rid(id),
        pushes: groups,
    })
}

fn verdicts(reply: &Reply) -> Vec<PushVerdict> {
    match reply {
        Reply::Done {
            outcome: Outcome::Push(p),
            ..
        } => p.items.clone(),
        other => panic!("expected a push outcome, got {other:?}"),
    }
}

/// `(partition name, offset)` for every Created verdict, and the count of
/// duplicates: comparable across nodes (pids may differ in value, not in what
/// they name).
fn shape(v: &[PushVerdict], names: &std::collections::HashMap<u64, String>) -> Vec<String> {
    v.iter()
        .map(|x| match x {
            PushVerdict::Created { pid, offset, .. } => {
                format!("{}@{offset}", names.get(pid).cloned().unwrap_or_default())
            }
            PushVerdict::Duplicate { pid, offset } => {
                format!(
                    "dup:{}@{offset}",
                    names.get(pid).cloned().unwrap_or_default()
                )
            }
            PushVerdict::Refused { code, .. } => format!("refused:{code}"),
        })
        .collect()
}

fn pid_names(
    store: &HeedStore,
    queue: &str,
    parts: &[String],
) -> std::collections::HashMap<u64, String> {
    store
        .read(|r| {
            let mut m = std::collections::HashMap::new();
            for p in parts {
                if let Some(pid) = r.pid_of(TENANT, queue, p)? {
                    m.insert(pid, p.clone());
                }
            }
            Ok(m)
        })
        .expect("read pids")
}

/// The same workload on the single planner and on 8 lanes: partitions that
/// exist (spread over every lane) and ones the request creates, a duplicate
/// item, multi-pushes interleaved with single pushes in the same cycles. Every
/// verdict is the single planner's, and a retry of a multi-push's id answers
/// its recorded outcome without planning it again.
async fn run_workload(lanes: u64) -> (Vec<Vec<String>>, Vec<(String, i64)>) {
    let node = Node::open(&format!("wl{lanes}"), lanes);
    let q = "mq";
    let parts: Vec<String> = (0..12).map(|i| format!("p{i}")).collect();
    // Existing partitions p0..p7, created one by one (consecutive pids: every lane).
    for (i, p) in parts.iter().take(8).enumerate() {
        let r = submit(
            &node.tx,
            multi(
                1_000 + i as u64,
                vec![group(10_000 + i as u64, q, p, &[format!("seed-{p}")])],
            ),
        )
        .await;
        assert!(
            matches!(verdicts(&r)[0], PushVerdict::Created { .. }),
            "{r:?}"
        );
    }
    let mut shapes = Vec::new();
    // Rounds of concurrent multi-pushes: each names 5 partitions, one of them
    // new in round 0 (p8..p11), and repeats one txn of an earlier round.
    for round in 0..4u64 {
        let mut calls = Vec::new();
        for k in 0..4u64 {
            let tx = node.tx.clone();
            let mut groups = Vec::new();
            for j in 0..5u64 {
                let p = &parts[((round + k * 3 + j * 2) % 12) as usize];
                let txns = vec![
                    format!("r{round}-k{k}-j{j}-a"),
                    format!("r{round}-k{k}-j{j}-b"),
                ];
                groups.push(group(20_000 + round * 100 + k * 10 + j, q, p, &txns));
            }
            if round > 0 {
                // A duplicate of an item the previous round pushed to its first
                // group's partition (never one of this round's own: an odd
                // step from every even one), in a group of its own.
                let p = &parts[((round - 1 + k * 3) % 12) as usize];
                let dup = vec![format!("r{}-k{k}-j0-a", round - 1)];
                groups.push(group(20_000 + round * 100 + k * 10 + 9, q, p, &dup));
            }
            // Group order is the request's: the verdicts must follow it.
            let id = 30_000 + round * 10 + k;
            calls.push(tokio::spawn(async move {
                (id, submit(&tx, multi(id, groups)).await)
            }));
            // A single push sharing the cycle.
            let tx = node.tx.clone();
            let p = parts[(k % 12) as usize].clone();
            calls.push(tokio::spawn(async move {
                let id = 40_000 + round * 10 + k;
                (
                    id,
                    submit(
                        &tx,
                        multi(
                            id,
                            vec![group(
                                50_000 + round * 10 + k,
                                "mq",
                                &p,
                                &[format!("single-{round}-{k}")],
                            )],
                        ),
                    )
                    .await,
                )
            }));
        }
        let mut replies = Vec::new();
        for c in calls {
            replies.push(c.await.expect("join"));
        }
        replies.sort_by_key(|(id, _)| *id);
        let names = pid_names(&node.store, q, &parts);
        for (id, r) in &replies {
            // A retry of the same id: the recorded outcome, nothing planned again.
            let again = submit(&node.tx, multi(*id, Vec::new())).await;
            assert_eq!(
                verdicts(&again),
                verdicts(r),
                "a retry of {id} answers its recorded outcome"
            );
            shapes.push(shape(&verdicts(r), &names));
        }
    }
    let tails: Vec<(String, i64)> = node
        .store
        .read(|r| {
            let mut out = Vec::new();
            for p in &parts {
                let pid = r.pid_of(TENANT, q, p)?.expect("partition exists");
                out.push((p.clone(), r.partition(pid)?.expect("row").last_offset));
            }
            Ok(out)
        })
        .expect("read tails");
    node.close().await;
    (shapes, tails)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_push_plans_like_its_groups_in_order_on_one_planner_and_on_lanes() {
    let (one_shapes, one_tails) = run_workload(1).await;
    let (lanes_shapes, lanes_tails) = run_workload(8).await;
    // Every partition's tail is the same: the same messages were stored.
    assert_eq!(one_tails, lanes_tails, "partition tails");
    // Gapless per partition: the verdicts' offsets of each partition, sorted,
    // run from its seed to its tail, on both.
    for shapes in [&one_shapes, &lanes_shapes] {
        let mut per: std::collections::BTreeMap<String, Vec<u64>> = Default::default();
        for s in shapes.iter().flatten() {
            if let Some((p, off)) = s.split_once('@') {
                if !p.starts_with("dup:") && !p.starts_with("refused") {
                    per.entry(p.to_string())
                        .or_default()
                        .push(off.parse().expect("offset"));
                }
            }
        }
        for (p, mut offs) in per {
            offs.sort_unstable();
            let tail = one_tails
                .iter()
                .find(|(n, _)| *n == p)
                .map(|(_, t)| *t)
                .unwrap();
            let first = *offs.first().unwrap();
            assert_eq!(
                offs,
                (first..=tail as u64).collect::<Vec<_>>(),
                "{p}: every offset once, gapless to the tail"
            );
        }
        // The duplicates the workload planted come back as duplicates.
        let dups = shapes
            .iter()
            .flatten()
            .filter(|s| s.starts_with("dup:"))
            .count();
        assert_eq!(
            dups,
            3 * 4,
            "one planted duplicate per multi-push after round 0"
        );
    }
}

/// A multi-push whose queue does not exist yet goes to control whole (it must
/// create the queue), and still answers every group, in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_multi_push_to_a_new_queue_creates_it_and_answers_every_group() {
    let node = Node::open("newq", 8);
    let groups: Vec<PushCommand> = (0..6)
        .map(|i| {
            group(
                70_000 + i,
                "brand-new",
                &format!("k{i}"),
                &[format!("t{i}"), format!("u{i}")],
            )
        })
        .collect();
    let r = submit(&node.tx, multi(71_000, groups)).await;
    let v = verdicts(&r);
    assert_eq!(v.len(), 12);
    for (i, x) in v.iter().enumerate() {
        match x {
            PushVerdict::Created { offset, .. } => assert_eq!(*offset, (i % 2) as u64, "item {i}"),
            other => panic!("item {i}: {other:?}"),
        }
    }
    node.close().await;
}

fn pop_pinned(id: u64, queue: &str, partition: &str, worker: &str) -> Command {
    Command::PopPinned(crate::rsm::planner::PopCommand {
        wait: false,
        request_id: rid(id),
        tenant: TENANT.to_string(),
        queue: queue.to_string(),
        partition: Some(partition.to_string()),
        group: "g".to_string(),
        worker: worker.to_string(),
        budget: 100,
        max_parts: 10,
        lease_seconds: 60,
        auto_ack: false,
        conflate: false,
        sub: crate::rsm::planner::SubIntent {
            mode: "all".to_string(),
            from_us: None,
            now: false,
        },
        skip_window_debounce: false,
        namespace: String::new(),
        task: String::new(),
        create_cfg: Some(qcfg()),
        deadline_us: 0,
    })
}

/// An ack whose targets sit in several lanes is split by lane and logged as
/// ONE command: one outcome, the results in target order, every cursor moved,
/// and a retry of its id answers the recorded outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn an_ack_across_lanes_is_split_and_logged_as_one_command() {
    use crate::rsm::planner::{AckCommand, AckItem, AckStatus, AckTarget};
    let node = Node::open("ack-split", 8);
    let q = "aq";
    // Four partitions, consecutive pids: four different lanes.
    let mut pids = Vec::new();
    for i in 0..4u64 {
        let r = submit(
            &node.tx,
            multi(
                80_000 + i,
                vec![group(80_100 + i, q, &format!("a{i}"), &[format!("m{i}")])],
            ),
        )
        .await;
        match verdicts(&r)[0] {
            PushVerdict::Created { pid, .. } => pids.push(pid),
            ref other => panic!("{other:?}"),
        }
    }
    assert_eq!(
        pids.iter()
            .map(|p| p % 8)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        4,
        "four lanes: {pids:?}"
    );
    // One worker leases each partition.
    for i in 0..4u64 {
        let r = submit(&node.tx, pop_pinned(81_000 + i, q, &format!("a{i}"), "w")).await;
        match &r {
            Reply::Done {
                outcome: Outcome::Pop(p),
                ..
            } => assert_eq!(p.claims.len(), 1, "a{i}: {p:?}"),
            other => panic!("{other:?}"),
        }
    }
    // One ack naming all four, in a scrambled order.
    let order = [2usize, 0, 3, 1];
    let targets: Vec<AckTarget> = order
        .iter()
        .map(|&i| AckTarget {
            pid: pids[i],
            tenant: TENANT.to_string(),
            queue: q.to_string(),
            group: "g".to_string(),
            worker: "w".to_string(),
            items: vec![AckItem {
                hash: crate::util::txn_hash128(&format!("m{i}")),
                status: AckStatus::Ok,
                error: None,
                snapshot: None,
            }],
        })
        .collect();
    let ack = Command::Ack(AckCommand {
        request_id: rid(82_000),
        targets: targets.clone(),
    });
    let r = submit(&node.tx, ack).await;
    let results = match &r {
        Reply::Done {
            outcome: Outcome::Ack(a),
            ..
        } => a.results.clone(),
        other => panic!("{other:?}"),
    };
    assert_eq!(
        results.iter().map(|x| x.pid).collect::<Vec<_>>(),
        order.iter().map(|&i| pids[i]).collect::<Vec<_>>(),
        "results in target order"
    );
    assert!(
        results.iter().all(|x| x.acked == 1 && x.committed == 0),
        "{results:?}"
    );
    // The retry answers the recorded outcome.
    let again = submit(
        &node.tx,
        Command::Ack(AckCommand {
            request_id: rid(82_000),
            targets,
        }),
    )
    .await;
    match &again {
        Reply::Done {
            outcome: Outcome::Ack(a),
            ..
        } => assert_eq!(a.results, results),
        other => panic!("{other:?}"),
    }
    // Every cursor moved.
    node.store
        .read(|r| {
            for pid in &pids {
                let c = r.cursor(*pid, "g")?.expect("cursor");
                assert_eq!(c.committed, 0, "pid {pid}");
                assert!(c.worker.is_none(), "pid {pid}: lease released");
            }
            Ok(())
        })
        .expect("read");
    node.close().await;
}
