//! A follower serving its own clients (`QUEEN_RAFT_CLIENT_OFFLOAD`): what it
//! waits for before it answers, and how its commands reach the leader.
//!
//! - **answer at commit** ([`a_follower_knows_a_reply_committed_with_its_term_or_superseded`],
//!   [`only_pushes_and_acks_that_render_from_their_outcome_answer_at_commit`]):
//!   a push or an ack answered by the leader is answered by the follower once
//!   it knows the entry committed with the reply's term — a reply whose index
//!   holds another term's entry (Jepsen W3c) is `Superseded`, never answered;
//!   pops and duplicate pushes still wait for the follower's own apply.
//! - **batched forwarding** ([`a_follower_serves_push_pop_ack_over_its_streams`]):
//!   the follower's commands travel over its streams to the leader, and the
//!   whole push/pop/ack round trip works through them, duplicate included.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::rsm::apply::SystemClock;
use crate::rsm::batcher::{Command, Reply};
use crate::rsm::entry::{encode_entry, AckOutcome, Outcome, PopOutcome, PushOutcome, PushVerdict};
use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{AckReq, Deadline, PopReq, PushReq, ReqCtx, Rsm};
use crate::rsm::replicator::local::{NoWaker, OpenConfig};
use crate::rsm::replicator::log::{Fsync, LogOptions};
use crate::rsm::replicator::raft::{
    qlog_options, snapshot, ClusterConfig, Landed, RaftOpts, RaftReplicator,
};
use crate::rsm::replicator::{AppliedAt, Replicator, Role};
use crate::rsm::store::HeedStore;

use super::apply::{cfg, seg_opts, store_opts, Workload};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// One cluster at a time from this file (each runs three full nodes).
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-edge-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn free_ports(n: usize) -> Vec<u16> {
    let ls: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind"))
        .collect();
    ls.iter()
        .map(|l| l.local_addr().expect("addr").port())
        .collect()
}

fn node_config(dir: &Path, id: u64) -> OpenConfig {
    OpenConfig {
        node_id: id,
        log_dir: dir.join("log"),
        log_opts: LogOptions {
            segment_bytes: 64 << 10,
            fsync: Fsync::Off,
        },
        seg_root: dir.join("seg"),
        seg_opts: crate::rsm::segments::Options {
            segment_bytes: 64 << 10,
            ..seg_opts()
        },
        apply_cfg: crate::rsm::apply::ApplyConfig {
            qlog: true,
            ..cfg()
        },
        apply_channel_capacity: 64,
        replay_deadline: Duration::from_secs(60),
        writer_pipeline: false,
    }
}

fn cluster_config(ports: &[u16], id: u64) -> ClusterConfig {
    let peers = ports
        .iter()
        .enumerate()
        .map(|(i, p)| format!("{}=127.0.0.1:{p}/127.0.0.1:1", i + 1))
        .collect::<Vec<_>>()
        .join(",");
    ClusterConfig::parse(
        id,
        &peers,
        Some(&format!("127.0.0.1:{}", ports[id as usize - 1])),
        None,
    )
    .expect("cluster config")
}

fn test_opts() -> RaftOpts {
    RaftOpts {
        exit_on_restart: false,
        purge_hold: Duration::from_secs(600),
        log_keep: 4096,
        purge_batch: 1024,
        cache_cap: 512 << 20,
        join: false,
        force_recover: None,
        apply_skip: Vec::new(),
        promote_max_lag: 1000,
    }
}

fn open_node(dir: &Path, id: u64, ports: &[u16]) -> RaftReplicator<HeedStore> {
    let cfg = node_config(dir, id);
    snapshot::apply_pending(dir, qlog_options(&cfg)).expect("apply a pending snapshot");
    let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
    RaftReplicator::open_with(
        store,
        cfg,
        Some(cluster_config(ports, id)),
        Arc::new(NoWaker),
        Arc::new(SystemClock),
        test_opts(),
    )
    .unwrap_or_else(|e| panic!("open node {id}: {e}"))
}

fn close_node(r: RaftReplicator<HeedStore>) {
    let (_s, store) = r.shutdown().expect("shutdown");
    if let Ok(store) = Arc::try_unwrap(store) {
        store.close();
    }
}

/// Three openraft nodes on localhost, and the index of the one that leads.
async fn three_nodes(tag: &str) -> (Vec<RaftReplicator<HeedStore>>, Vec<PathBuf>, usize) {
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch(tag)).collect();
    let ports = free_ports(3);
    let nodes: Vec<RaftReplicator<HeedStore>> = std::thread::scope(|s| {
        let hs: Vec<_> = dirs
            .iter()
            .enumerate()
            .map(|(i, d)| {
                let ports = &ports;
                s.spawn(move || open_node(d, i as u64 + 1, ports))
            })
            .collect();
        hs.into_iter().map(|h| h.join().expect("open")).collect()
    });
    let end = Instant::now() + Duration::from_secs(30);
    let l = loop {
        if let Some(l) = nodes
            .iter()
            .position(|r| matches!(r.role(), Role::Leader { .. }))
        {
            break l;
        }
        assert!(Instant::now() < end, "no leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    (nodes, dirs, l)
}

/// Propose `n` workload entries on the leader; the last one's `(index, term)`.
async fn propose(leader: &RaftReplicator<HeedStore>, seed: u64, n: usize) -> AppliedAt {
    let mut w = Workload::new(seed);
    let mut last = None;
    for _ in 0..n {
        let e = bytes::Bytes::from(encode_entry(&w.next().entry).expect("encode"));
        last = Some(
            leader
                .propose(e, Instant::now() + Duration::from_secs(30))
                .await
                .expect("propose"),
        );
    }
    last.expect("proposed")
}

fn close_all(nodes: Vec<RaftReplicator<HeedStore>>, dirs: Vec<PathBuf>) {
    for n in nodes {
        close_node(n);
    }
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A follower's own view decides: the entry at the reply's index is
/// committed there with the reply's term (`Committed`), or with another
/// (`Superseded`: the reply's entry never committed), or not known committed
/// by the deadline (`NotYet`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_knows_a_reply_committed_with_its_term_or_superseded() {
    let _one = ONE_AT_A_TIME.lock().await;
    let (nodes, dirs, l) = three_nodes("landed").await;
    let f = (l + 1) % 3;
    let at = propose(&nodes[l], 0xED6E, 50).await;
    let soon = || Instant::now() + Duration::from_secs(10);

    assert_eq!(
        nodes[f].wait_committed(at.index, at.term, soon()).await,
        Landed::Committed,
        "the leader's entry, with its term"
    );
    assert_eq!(
        nodes[f].wait_committed(at.index, at.term + 1, soon()).await,
        Landed::Superseded,
        "another term at that index: the reply's entry never committed"
    );
    let t0 = Instant::now();
    assert_eq!(
        nodes[f]
            .wait_committed(
                at.index + 10_000,
                at.term,
                Instant::now() + Duration::from_millis(200)
            )
            .await,
        Landed::NotYet,
        "never committed: the deadline answers"
    );
    assert!(t0.elapsed() < Duration::from_secs(2));
    assert!(
        nodes[f].wait_applied(at.index, soon()).await,
        "the applied waiters wake"
    );
    close_all(nodes, dirs);
}

/// A follower's concurrent read barriers share their read-index calls to the
/// leader, and each still reads at least every write committed before it
/// started.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_read_barriers_on_a_follower_share_their_calls() {
    let _one = ONE_AT_A_TIME.lock().await;
    let (nodes, dirs, l) = three_nodes("reads").await;
    let f = (l + 1) % 3;
    let at = propose(&nodes[l], 0xBEAD, 20).await;
    let nodes = Arc::new(nodes);
    let calls0 = nodes[f].read_index_calls_for_test();
    let mut ts = Vec::new();
    for _ in 0..300 {
        let nodes = nodes.clone();
        ts.push(tokio::spawn(async move {
            nodes[f]
                .read_barrier(Instant::now() + Duration::from_secs(10))
                .await
        }));
    }
    for t in ts {
        let index = t.await.expect("task").expect("read barrier");
        assert!(
            index >= at.index,
            "a barrier started after {} committed read {index}",
            at.index
        );
        assert!(
            nodes[f].applied_index() >= index,
            "and applied that far here"
        );
    }
    let calls = nodes[f].read_index_calls_for_test() - calls0;
    assert!(
        (1..=60).contains(&calls),
        "300 barriers, {calls} read-index calls"
    );
    let nodes = Arc::try_unwrap(nodes).unwrap_or_else(|_| panic!("still shared"));
    close_all(nodes, dirs);
}

fn push_cmd() -> Command {
    Command::Push(crate::rsm::planner::PushCommand {
        request_id: [1; 16],
        tenant: "t".into(),
        queue: "q".into(),
        partition: "p".into(),
        items: Vec::new(),
        create_cfg: crate::rsm::planner::timers::implicit_queue_config_for("q"),
    })
}

fn done(outcome: Outcome) -> Reply {
    Reply::Done {
        outcome,
        at: Some(AppliedAt { index: 9, term: 2 }),
    }
}

#[test]
fn only_pushes_and_acks_that_render_from_their_outcome_answer_at_commit() {
    let created = PushVerdict::Created {
        pid: 1,
        offset: 0,
        created_at_us: 0,
    };
    let dup = PushVerdict::Duplicate { pid: 1, offset: 0 };
    let push = |items: Vec<PushVerdict>| done(Outcome::Push(PushOutcome { items }));
    assert!(RaftFacade::answers_at_commit(
        &push_cmd(),
        &push(vec![created.clone(), created.clone()])
    ));
    assert!(
        !RaftFacade::answers_at_commit(&push_cmd(), &push(vec![created.clone(), dup.clone()])),
        "a duplicate's answer reads the original id from this node's queue log"
    );
    let multi = Command::MultiPush(crate::rsm::batcher::MultiPushCommand {
        request_id: [2; 16],
        pushes: Vec::new(),
    });
    assert!(RaftFacade::answers_at_commit(
        &multi,
        &push(vec![created.clone()])
    ));
    assert!(!RaftFacade::answers_at_commit(&multi, &push(vec![dup])));
    let ack = Command::Ack(crate::rsm::planner::AckCommand {
        request_id: [3; 16],
        targets: Vec::new(),
    });
    assert!(RaftFacade::answers_at_commit(
        &ack,
        &done(Outcome::Ack(AckOutcome {
            results: Vec::new()
        }))
    ));
    let nack = Command::Nack(crate::rsm::planner::NackCommand {
        request_id: [4; 16],
        pid: 1,
        tenant: String::new(),
        queue: String::new(),
        group: "g".into(),
        worker: "w".into(),
    });
    assert!(
        !RaftFacade::answers_at_commit(&nack, &done(Outcome::Empty)),
        "anything else waits for this node's apply"
    );
    assert!(
        !RaftFacade::answers_at_commit(
            &push_cmd(),
            &done(Outcome::Pop(PopOutcome { claims: Vec::new() }))
        ),
        "an outcome of another kind: no"
    );
    assert!(!RaftFacade::answers_at_commit(
        &push_cmd(),
        &Reply::Retry { hint: None }
    ));
}

/// A refusal of the whole cycle while the batcher's pipeline changed is taken
/// to the leader again; a client error or another retryable code is answered.
#[test]
fn only_an_unavailable_refusal_is_retried_by_the_facade() {
    use crate::rsm::planner::Refusal;
    assert!(RaftFacade::retries_refusal(&Refusal::retry(
        "unavailable",
        "the pipeline changed while this cycle planned"
    )));
    assert!(!RaftFacade::retries_refusal(&Refusal::retry(
        "internal",
        "entry build"
    )));
    assert!(!RaftFacade::retries_refusal(&Refusal::client(
        "unavailable",
        "not retryable"
    )));
}

fn open_facade(dir: &Path, cluster: ClusterConfig) -> RaftFacade {
    let ctx = crate::rsm::facade::RsmBuildCtx {
        data_dir: dir.display().to_string(),
        notifier: crate::notify::Notifier::new(false),
        disk_high_pct: 99.0,
        disk_low_pct: 98.0,
    };
    let batcher = crate::rsm::batcher::BatcherConfig {
        maintenance_every_ms: 0,
        ..crate::rsm::batcher::BatcherConfig::from_env()
    };
    let id = cluster.node_id;
    RaftFacade::open_cluster_node_for_test(&ctx, batcher, cluster, test_opts())
        .unwrap_or_else(|e| panic!("open facade {id}: {e}"))
}

async fn close_facade(f: Arc<RaftFacade>) {
    let end = Instant::now() + Duration::from_secs(10);
    let mut f = f;
    loop {
        match Arc::try_unwrap(f) {
            Ok(f) => return f.shutdown().await,
            Err(still) => {
                assert!(Instant::now() < end, "a facade is still shared at close");
                f = still;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

fn parse(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("bad JSON body: {e}\n{body}"))
}

/// Push (several partitions, then a duplicate), pop and ack through a
/// follower: every command reaches the leader over the follower's streams,
/// and every answer is the one the leader's own client would get.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_follower_serves_push_pop_ack_over_its_streams() {
    let _one = ONE_AT_A_TIME.lock().await;
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("streams")).collect();
    let ports = free_ports(3);
    let mut hs = Vec::new();
    for (i, d) in dirs.iter().enumerate() {
        let (d, c) = (d.clone(), cluster_config(&ports, i as u64 + 1));
        hs.push(tokio::task::spawn_blocking(move || open_facade(&d, c)));
    }
    let mut nodes = Vec::new();
    for h in hs {
        nodes.push(Arc::new(h.await.expect("open")));
    }
    let end = Instant::now() + Duration::from_secs(30);
    let l = loop {
        let leaders: Vec<usize> = (0..3)
            .filter(|i| nodes[*i].health().role == "leader")
            .collect();
        if leaders.len() == 1 {
            break leaders[0];
        }
        assert!(Instant::now() < end, "no single ready leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let follower = nodes[(l + 1) % 3].clone();
    let leader = nodes[l].clone();
    let ctx = || {
        ReqCtx::new(
            crate::config::DEFAULT_TENANT,
            Deadline::after(Duration::from_secs(10)),
        )
    };
    let sent0 = crate::rsm::replicator::raft::forward::sent_for_test();

    // Three partitions in one push, through the follower.
    let push = follower
        .push(
            ctx(),
            PushReq {
                raw: br#"{"items":[
                    {"queue":"edge","partition":"a","payload":{"n":1},"transactionId":"t1"},
                    {"queue":"edge","partition":"b","payload":{"n":2},"transactionId":"t2"},
                    {"queue":"edge","partition":"c","payload":{"n":3},"transactionId":"t3"}
                ]}"#
                .to_vec(),
            },
        )
        .await
        .expect("push through the follower");
    let items = parse(&push.body);
    let first_id = items[0]["message_id"].as_str().expect("id").to_string();
    for it in items.as_array().expect("array") {
        assert_eq!(it["status"], "queued", "{it}");
    }
    assert!(
        crate::rsm::replicator::raft::forward::sent_for_test() > sent0,
        "the follower's commands went over its streams"
    );

    // The same message again: a duplicate, answered with the ORIGINAL id
    // (read from the follower's own queue log once it applied).
    let again = follower
        .push(
            ctx(),
            PushReq {
                raw: br#"{"items":[{"queue":"edge","partition":"a","payload":{"n":1},"transactionId":"t1"}]}"#
                    .to_vec(),
            },
        )
        .await
        .expect("duplicate push");
    let again = parse(&again.body);
    assert_eq!(again[0]["status"], "duplicate", "{again}");
    assert_eq!(again[0]["message_id"].as_str(), Some(first_id.as_str()));

    // Pop and ack through the follower.
    let mut acked = 0;
    let t0 = Instant::now();
    while acked < 3 {
        assert!(t0.elapsed() < Duration::from_secs(20), "drained in time");
        let popped = follower
            .pop_wildcard(
                ctx(),
                PopReq {
                    queue: "edge".into(),
                    group: None,
                    batch: 10,
                    auto_ack: false,
                    wait: false,
                    timeout_ms: 2000,
                    options: Default::default(),
                },
            )
            .await
            .expect("pop through the follower");
        if popped.empty {
            continue;
        }
        let pop = parse(&popped.body);
        let pid = pop["partitionId"]
            .as_str()
            .expect("partitionId")
            .to_string();
        let lease = pop["leaseId"].as_str().expect("leaseId").to_string();
        let msgs = pop["messages"].as_array().expect("messages").clone();
        let body = serde_json::json!({
            "consumerGroup": "__QUEUE_MODE__",
            "acknowledgments": msgs.iter().map(|m| serde_json::json!({
                "transactionId": m["transactionId"],
                "partitionId": pid,
                "status": "completed",
                "leaseId": lease,
            })).collect::<Vec<_>>(),
        });
        let out = follower
            .ack(
                ctx(),
                AckReq {
                    queue: None,
                    group: "__QUEUE_MODE__".into(),
                    raw: body.to_string().into_bytes(),
                },
            )
            .await
            .expect("ack through the follower");
        let out = parse(&out.body);
        for r in out.as_array().expect("ack array") {
            assert_eq!(r["success"], true, "{out}");
        }
        acked += msgs.len();
    }
    // Nothing behind: both answer that a local read is linearizable (a
    // follower's pop fast path asks before it reads its own state).
    assert!(follower
        .caught_up(&ctx())
        .await
        .expect("follower read index"));
    assert!(leader.caught_up(&ctx()).await.expect("leader read index"));

    // Nothing is left for the leader's own consumer.
    let left = leader
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "edge".into(),
                group: None,
                batch: 10,
                auto_ack: false,
                wait: false,
                timeout_ms: 500,
                options: Default::default(),
            },
        )
        .await
        .expect("pop on the leader");
    assert!(left.empty, "all acked: {}", left.body);
    drop((follower, leader));
    for n in nodes {
        close_facade(n).await;
    }
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}
