//! Three openraft nodes in one process, talking real HTTP on localhost.
//!
//! What this file proves:
//!
//! - **replication** ([`three_nodes_apply_the_same_entries`]): the entries the
//!   leader commits give every node the same replicated state (log positions
//!   included), and that state is the single-node reference's.
//! - **failover** ([`a_new_leader_takes_over_and_the_old_one_catches_up`]):
//!   the leader stops, another is elected, more entries commit, the old leader
//!   rejoins and catches up.
//! - **catch-up from disk** ([`a_follower_catches_up_from_the_queue_logs`]): a
//!   follower away while the leader's cache let go of its entries catches up
//!   through the queue logs, payloads restored.
//! - **snapshot** ([`a_follower_behind_the_purge_point_gets_a_snapshot`]): a
//!   follower behind the leader's purge point receives a snapshot, restarts on
//!   it, and converges.
//! - **hand-off** ([`a_leader_on_its_way_out_hands_leadership_to_a_live_peer`],
//!   [`a_node_that_handed_off_is_not_handed_leadership_back`]): a leader on its
//!   way out (SIGTERM) hands leadership to a live peer faster than an election
//!   could elect one, and is not handed it back while it drains.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::rsm::apply::{self, state_digest, StateDigest, SystemClock};
use crate::rsm::entry::encode_entry;
use crate::rsm::replicator::local::{NoWaker, OpenConfig};
use crate::rsm::replicator::log::{Fsync, LogOptions};
use crate::rsm::replicator::raft::{
    qlog_options, snapshot, ClusterConfig, RaftOpts, RaftReplicator,
};
use crate::rsm::replicator::{Replicator, Role};
use crate::rsm::store::{HeedStore, Store};

use super::apply::{cfg, run_workload, seg_opts, store_opts, Node, Workload};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// One cluster test at a time: each runs three full nodes (stores, apply
/// threads, runtimes), and the suite runs hundreds of store tests beside them.
static ONE_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    ONE_AT_A_TIME.lock().unwrap_or_else(|p| p.into_inner())
}

/// `QUEEN_TEST_LOG=<filter>` prints the nodes' logs (one test at a time).
fn log_init() {
    if let Ok(f) = std::env::var("QUEEN_TEST_LOG") {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(f))
            .with_test_writer()
            .try_init();
    }
}
const SEED: u64 = 0x0_0C1A_5700_0001;

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-cluster-{tag}-{}-{}",
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
        // Small queue-log files: catch-up and snapshots cross many of them.
        seg_opts: crate::rsm::segments::Options {
            segment_bytes: 64 << 10,
            ..seg_opts()
        },
        apply_cfg: apply::ApplyConfig {
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

/// Open node `id` over `dir`, as the binary does: a pending snapshot first.
fn open_node(dir: &Path, id: u64, ports: &[u16], opts: RaftOpts) -> RaftReplicator<HeedStore> {
    open_node_in(dir, id, cluster_config(ports, id), opts)
}

fn open_node_in(
    dir: &Path,
    id: u64,
    cluster: ClusterConfig,
    opts: RaftOpts,
) -> RaftReplicator<HeedStore> {
    let cfg = node_config(dir, id);
    snapshot::apply_pending(dir, qlog_options(&cfg)).expect("apply a pending snapshot");
    let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("store"));
    RaftReplicator::open_with(
        store,
        cfg,
        Some(cluster),
        Arc::new(NoWaker),
        Arc::new(SystemClock),
        opts,
    )
    .unwrap_or_else(|e| panic!("open node {id}: {e}"))
}

/// Open every node at once (each waits for a leader, which needs a quorum).
fn open_all(
    dirs: &[PathBuf],
    ports: &[u16],
    opts: &[RaftOpts],
) -> Vec<Option<RaftReplicator<HeedStore>>> {
    open_all_in(dirs, opts, |id| cluster_config(ports, id))
}

/// [`open_all`], node `id` with the cluster configuration `cluster(id)`.
fn open_all_in(
    dirs: &[PathBuf],
    opts: &[RaftOpts],
    cluster: impl Fn(u64) -> ClusterConfig + Sync,
) -> Vec<Option<RaftReplicator<HeedStore>>> {
    let cluster = &cluster;
    std::thread::scope(|s| {
        let hs: Vec<_> = dirs
            .iter()
            .enumerate()
            .map(|(i, d)| {
                let o = opts[i].clone();
                let id = i as u64 + 1;
                s.spawn(move || open_node_in(d, id, cluster(id), o))
            })
            .collect();
        hs.into_iter()
            .map(|h| Some(h.join().expect("open thread")))
            .collect()
    })
}

/// The index (into `nodes`) of the one ready leader, waiting for it.
fn leader_of(nodes: &[Option<RaftReplicator<HeedStore>>]) -> usize {
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        let leaders: Vec<usize> = nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| {
                n.as_ref()
                    .is_some_and(|r| matches!(r.role(), Role::Leader { .. }))
            })
            .map(|(i, _)| i)
            .collect();
        if leaders.len() == 1 {
            return leaders[0];
        }
        assert!(Instant::now() < end, "no single ready leader within 30 s");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn workload(seed: u64, n: u64) -> Vec<Bytes> {
    let mut w = Workload::new(seed);
    (0..n)
        .map(|_| Bytes::from(encode_entry(&w.next().entry).expect("encode")))
        .collect()
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

async fn propose_all(r: &RaftReplicator<HeedStore>, entries: &[Bytes]) -> u64 {
    let mut last = 0;
    for e in entries {
        let at = r.propose(e.clone(), deadline()).await.expect("propose");
        assert!(at.index > last, "indexes ascend");
        last = at.index;
    }
    last
}

/// Wait until every open node has applied `index`.
fn wait_applied(nodes: &[Option<RaftReplicator<HeedStore>>], index: u64) {
    let end = Instant::now() + Duration::from_secs(60);
    loop {
        let behind: Vec<(usize, u64)> = nodes
            .iter()
            .enumerate()
            .filter_map(|(i, n)| n.as_ref().map(|r| (i, r.applied_index())))
            .filter(|(_, a)| *a < index)
            .collect();
        if behind.is_empty() {
            return;
        }
        assert!(
            Instant::now() < end,
            "nodes {behind:?} did not apply {index} within 60 s"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn close(r: RaftReplicator<HeedStore>) -> StateDigest {
    let (_s, store) = r.shutdown().expect("shutdown");
    let store = Arc::try_unwrap(store).unwrap_or_else(|_| panic!("the store is still shared"));
    let d = store
        .read(|r| Ok(state_digest(r).expect("digest")))
        .expect("read");
    store.close();
    d
}

/// Every keyspace but `meta` (its durable indexes are node-local timing).
fn replicated(d: &StateDigest) -> Vec<(&'static str, u128, u64)> {
    d.per_keyspace
        .iter()
        .filter(|(name, _, _)| *name != "meta")
        .cloned()
        .collect()
}

/// Every keyspace but the two that record log positions, for the single-node
/// reference (openraft's own entries shift the indexes).
fn beyond_log_positions(d: &StateDigest) -> Vec<(&'static str, u128, u64)> {
    d.per_keyspace
        .iter()
        .filter(|(name, _, _)| *name != "meta" && *name != "groups")
        .cloned()
        .collect()
}

/// Close every node, check they all hold the same state, and that it is the
/// single-node run of `total` workload entries.
fn converge(nodes: Vec<Option<RaftReplicator<HeedStore>>>, total: u64, tag: &str) {
    let digests: Vec<StateDigest> = nodes.into_iter().flatten().map(close).collect();
    for (i, d) in digests.iter().enumerate().skip(1) {
        assert_eq!(
            replicated(d),
            replicated(&digests[0]),
            "{tag}: node {} built different state from node 1",
            i + 1
        );
    }
    let want = run_workload(&Node::new(&format!("cluster-ref-{tag}")), SEED, total, 97);
    assert_eq!(
        beyond_log_positions(&digests[0]),
        beyond_log_positions(&want),
        "{tag}: the cluster's state differs from the single-node run"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_apply_the_same_entries() {
    let _one = serial();
    const N: u64 = 300;
    let entries = workload(SEED, N);
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("replicate")).collect();
    let ports = free_ports(3);
    let opts = vec![test_opts(); 3];
    let nodes = tokio::task::block_in_place(|| open_all(&dirs, &ports, &opts));
    let l = tokio::task::block_in_place(|| leader_of(&nodes));
    let last = propose_all(nodes[l].as_ref().unwrap(), &entries).await;
    tokio::task::block_in_place(|| wait_applied(&nodes, last));
    tokio::task::block_in_place(|| converge(nodes, N, "replicate"));
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_leader_takes_over_and_the_old_one_catches_up() {
    let _one = serial();
    const N: u64 = 150;
    let entries = workload(SEED, 2 * N);
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("failover")).collect();
    let ports = free_ports(3);
    let opts = vec![test_opts(); 3];
    let mut nodes = tokio::task::block_in_place(|| open_all(&dirs, &ports, &opts));
    let first = tokio::task::block_in_place(|| leader_of(&nodes));
    propose_all(nodes[first].as_ref().unwrap(), &entries[..N as usize]).await;

    // The leader stops; the other two elect a new one and go on.
    let old = nodes[first].take().unwrap();
    let old_term = match old.role() {
        Role::Leader { term } => term,
        r => panic!("the old leader is {r:?}"),
    };
    let _ = close(old);
    let second = tokio::task::block_in_place(|| leader_of(&nodes));
    assert_ne!(second, first);
    let Role::Leader { term } = nodes[second].as_ref().unwrap().role() else {
        unreachable!()
    };
    assert!(term > old_term, "the new leader has a newer term");
    let last = propose_all(nodes[second].as_ref().unwrap(), &entries[N as usize..]).await;

    // The old leader comes back as a follower and catches up.
    let id = first as u64 + 1;
    let (d, p, o) = (dirs[first].clone(), ports.clone(), test_opts());
    nodes[first] = Some(tokio::task::block_in_place(move || {
        open_node(&d, id, &p, o)
    }));
    tokio::task::block_in_place(|| wait_applied(&nodes, last));
    assert!(
        !matches!(nodes[first].as_ref().unwrap().role(), Role::Leader { .. }),
        "the rejoining node does not depose the leader"
    );
    tokio::task::block_in_place(|| converge(nodes, 2 * N, "failover"));
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// When `member` was last heard from as `node` knows it — the leader's figure
/// plus the age of the copy — and how old that copy is.
fn heard(r: &RaftReplicator<HeedStore>, member: u64) -> Option<(u64, u64)> {
    let cm = r.members();
    let age = cm.view_age?.as_millis() as u64;
    let seen = cm.view?.members.into_iter().find(|m| m.id == member)?;
    Some((seen.last_ack_ms? + age, age))
}

/// Liveness from raft (`GET /api/v1/raft/liveness`): the leader reports every
/// member heard from within a few heartbeats, and each follower serves a fresh
/// copy of the leader's view — carried on the leader's own appends. A member
/// that stops is reported silent, by the leader AND by the other follower
/// through its copy (one authority, so every node agrees), and heard again once
/// back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_node_reports_the_leaders_view_of_who_is_live() {
    let _one = serial();
    log_init();
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("members")).collect();
    let ports = free_ports(3);
    let opts = vec![test_opts(); 3];
    let mut nodes = tokio::task::block_in_place(|| open_all(&dirs, &ports, &opts));
    let l = tokio::task::block_in_place(|| leader_of(&nodes));
    let (f, other) = ((l + 1) % 3, (l + 2) % 3);
    let (lid, fid) = (l as u64 + 1, f as u64 + 1);

    // Everyone heard from recently, as every node reports it.
    type Nodes = [Option<RaftReplicator<HeedStore>>];
    let poll = |nodes: &Nodes, want: &dyn Fn(&Nodes) -> bool, what: &str| {
        let end = Instant::now() + Duration::from_secs(20);
        while !want(nodes) {
            assert!(Instant::now() < end, "{what}");
            std::thread::sleep(Duration::from_millis(50));
        }
    };
    tokio::task::block_in_place(|| {
        poll(
            &nodes,
            &|nodes| {
                nodes.iter().flatten().all(|r| {
                    (1..=3)
                        .all(|m| heard(r, m).is_some_and(|(ack, age)| ack < 1_000 && age < 1_000))
                })
            },
            "not every node reported every member heard from",
        )
    });
    // A follower's copy names the leader that took it.
    let cm = nodes[f].as_ref().unwrap().members();
    assert_eq!(cm.view.as_ref().unwrap().leader, lid);
    assert_eq!(cm.node_id, fid);

    // A follower stops: the leader, and the other follower through its copy,
    // report it silent while the copy itself stays fresh.
    let _ = close(nodes[f].take().unwrap());
    tokio::task::block_in_place(|| {
        poll(
            &nodes,
            &|nodes| {
                let at_leader = heard(nodes[l].as_ref().unwrap(), fid);
                let at_other = heard(nodes[other].as_ref().unwrap(), fid);
                at_leader.is_some_and(|(ack, _)| ack >= 2_000)
                    && at_other.is_some_and(|(ack, age)| ack >= 2_000 && age < 1_500)
            },
            "the stopped follower was not reported silent everywhere",
        )
    });
    // The two that are up are still heard from.
    for m in [lid, other as u64 + 1] {
        let (ack, _) = heard(nodes[other].as_ref().unwrap(), m).unwrap();
        assert!(ack < 1_500, "member {m} silent for {ack} ms");
    }

    // Back: heard again.
    let (d, p, o) = (dirs[f].clone(), ports.clone(), test_opts());
    nodes[f] = Some(tokio::task::block_in_place(move || {
        open_node(&d, fid, &p, o)
    }));
    tokio::task::block_in_place(|| {
        poll(
            &nodes,
            &|nodes| heard(nodes[l].as_ref().unwrap(), fid).is_some_and(|(ack, _)| ack < 1_000),
            "the restarted follower was not heard from again",
        )
    });

    // The LEADER stops. Its successor has never heard from it in its own
    // term, and still counts its silence from the last append it received
    // from it — from the stop, not from its own election.
    let stopped = Instant::now();
    let _ = close(nodes[l].take().unwrap());
    let next = tokio::task::block_in_place(|| leader_of(&nodes));
    let (ack, _) = heard(nodes[next].as_ref().unwrap(), lid).expect("the old leader is listed");
    let floor = stopped.elapsed().as_millis() as u64;
    assert!(
        ack + 250 >= floor,
        "the dead leader's silence ({ack} ms) counts from the election, not from its stop \
         ({floor} ms ago)"
    );
    for n in nodes.into_iter().flatten() {
        let _ = close(n);
    }
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_catches_up_from_the_queue_logs() {
    let _one = serial();
    const N: u64 = 200;
    let entries = workload(SEED, 2 * N);
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("disk")).collect();
    let ports = free_ports(3);
    // A tiny cache: once applied, entries leave memory at once, and a follower
    // that is away reads them back from the queue logs.
    let tiny = RaftOpts {
        cache_cap: 1,
        ..test_opts()
    };
    let opts = vec![tiny.clone(); 3];
    let mut nodes = tokio::task::block_in_place(|| open_all(&dirs, &ports, &opts));
    let l = tokio::task::block_in_place(|| leader_of(&nodes));
    propose_all(nodes[l].as_ref().unwrap(), &entries[..N as usize]).await;

    let f = (l + 1) % 3;
    let away = nodes[f].take().unwrap();
    let _ = close(away);
    let last = propose_all(nodes[l].as_ref().unwrap(), &entries[N as usize..]).await;
    let (cached, _) = nodes[l].as_ref().unwrap().log_cache();
    assert!(
        cached < N as usize,
        "the leader's cache let go of the entries the follower misses ({cached} cached)"
    );

    let id = f as u64 + 1;
    let (d, p) = (dirs[f].clone(), ports.clone());
    nodes[f] = Some(tokio::task::block_in_place(move || {
        open_node(&d, id, &p, tiny)
    }));
    tokio::task::block_in_place(|| wait_applied(&nodes, last));
    tokio::task::block_in_place(|| converge(nodes, 2 * N, "disk"));
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_behind_the_purge_point_gets_a_snapshot() {
    let _one = serial();
    log_init();
    const N: u64 = 200;
    let entries = workload(SEED, 3 * N);
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("snapshot")).collect();
    let ports = free_ports(3);
    // Purge eagerly, and wait only 2 s for a follower that stopped making
    // progress (one that is catching up keeps the log it needs).
    let eager = RaftOpts {
        purge_hold: Duration::from_secs(2),
        log_keep: 0,
        purge_batch: 1,
        ..test_opts()
    };
    let opts = vec![eager.clone(); 3];
    let mut nodes = tokio::task::block_in_place(|| open_all(&dirs, &ports, &opts));
    let l = tokio::task::block_in_place(|| leader_of(&nodes));
    propose_all(nodes[l].as_ref().unwrap(), &entries[..N as usize]).await;

    let f = (l + 1) % 3;
    let away = nodes[f].take().unwrap();
    let away_last = away.applied_index();
    let _ = close(away);
    let last = propose_all(
        nodes[l].as_ref().unwrap(),
        &entries[N as usize..2 * N as usize],
    )
    .await;

    // The leader's log is purged past everything the follower has.
    let end = Instant::now() + Duration::from_secs(60);
    loop {
        let m = nodes[l].as_ref().unwrap().metrics();
        let purged = nodes[l].as_ref().unwrap().purged_index();
        // Durable points need activity to move: keep the log growing a little.
        let _ = m;
        if purged > away_last + 1 {
            break;
        }
        assert!(
            Instant::now() < end,
            "the leader did not purge past {away_last} (purged {purged}, applied {})",
            m.applied_index
        );
        // Durable points need activity to move.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // The follower returns, receives a snapshot, and stops to load it.
    let id = f as u64 + 1;
    let (d, p, o) = (dirs[f].clone(), ports.clone(), eager.clone());
    let back = tokio::task::block_in_place(move || open_node(&d, id, &p, o));
    let end = Instant::now() + Duration::from_secs(60);
    while back.restart_requested().is_none() {
        assert!(
            Instant::now() < end,
            "the follower never asked to restart on a snapshot"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        dirs[f].join(snapshot::PENDING).exists(),
        "the snapshot is staged"
    );
    let _ = close(back);

    // It restarts on the snapshot and catches up.
    let (d, p, o) = (dirs[f].clone(), ports.clone(), eager.clone());
    nodes[f] = Some(tokio::task::block_in_place(move || {
        open_node(&d, id, &p, o)
    }));
    assert!(
        !dirs[f].join(snapshot::PENDING).exists(),
        "the snapshot was swapped in"
    );
    let more = propose_all(nodes[l].as_ref().unwrap(), &entries[2 * N as usize..]).await;
    assert!(more > last);
    tokio::task::block_in_place(|| wait_applied(&nodes, more));
    tokio::task::block_in_place(|| converge(nodes, 3 * N, "snapshot"));
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A leader on its way out (the binary's SIGTERM) hands leadership to a live
/// peer: another node leads well within the election timeout (1 s — an
/// election after the leader stops takes at least that), never the member that
/// stopped answering, and the cluster goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_on_its_way_out_hands_leadership_to_a_live_peer() {
    let _one = serial();
    log_init();
    const N: u64 = 100;
    let entries = workload(SEED, 2 * N);
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("handoff")).collect();
    let ports = free_ports(3);
    let opts = vec![test_opts(); 3];
    let mut nodes = tokio::task::block_in_place(|| open_all(&dirs, &ports, &opts));
    let l = tokio::task::block_in_place(|| leader_of(&nodes));
    let first = propose_all(nodes[l].as_ref().unwrap(), &entries[..N as usize]).await;
    tokio::task::block_in_place(|| wait_applied(&nodes, first));

    // The follower with the higher id stops, caught up: its matched index
    // ties the live one's, so only its silence tells them apart.
    let followers: Vec<usize> = (0..3).filter(|i| *i != l).collect();
    let (live, gone) = (followers[0], followers[1]);
    let _ = close(nodes[gone].take().unwrap());
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let t0 = Instant::now();
    let to = nodes[l]
        .as_ref()
        .unwrap()
        .hand_off_leadership(Duration::from_secs(3))
        .await;
    let took = t0.elapsed();
    assert_eq!(to, Some(live as u64 + 1), "handed to the live follower");
    assert!(
        took < Duration::from_millis(800),
        "a transfer, not an election timeout: {took:?}"
    );
    assert_eq!(tokio::task::block_in_place(|| leader_of(&nodes)), live);
    // The old leader, now following, has nothing left to hand.
    assert_eq!(
        nodes[l]
            .as_ref()
            .unwrap()
            .hand_off_leadership(Duration::from_secs(3))
            .await,
        None
    );

    let last = propose_all(nodes[live].as_ref().unwrap(), &entries[N as usize..]).await;
    let id = gone as u64 + 1;
    let (d, p) = (dirs[gone].clone(), ports.clone());
    nodes[gone] = Some(tokio::task::block_in_place(move || {
        open_node(&d, id, &p, test_opts())
    }));
    tokio::task::block_in_place(|| wait_applied(&nodes, last));
    tokio::task::block_in_place(|| converge(nodes, 2 * N, "handoff"));
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// With a preferred leader (group 0 prefers node 1,
/// [`ClusterConfig::for_group`]), the preferred node that handed leadership
/// away on its way out is not handed it back while it drains: that would end
/// in an election when it exits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_handed_off_is_not_handed_leadership_back() {
    let _one = serial();
    log_init();
    const N: u64 = 100;
    let entries = workload(SEED, 2 * N);
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("handback")).collect();
    let ports = free_ports(3);
    let opts = vec![test_opts(); 3];
    let nodes = tokio::task::block_in_place(|| {
        open_all_in(&dirs, &opts, |id| {
            cluster_config(&ports, id).for_group(0).expect("group 0")
        })
    });
    let end = Instant::now() + Duration::from_secs(30);
    while tokio::task::block_in_place(|| leader_of(&nodes)) != 0 {
        assert!(Instant::now() < end, "node 1, the preferred one, never led");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    propose_all(nodes[0].as_ref().unwrap(), &entries[..N as usize]).await;

    let to = nodes[0]
        .as_ref()
        .unwrap()
        .hand_off_leadership(Duration::from_secs(3))
        .await
        .expect("node 1 hands off");
    let new = to as usize - 1;
    // The new leader checks its preference every 500 ms: without the hold it
    // hands node 1 its leadership back at the first check.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        tokio::task::block_in_place(|| leader_of(&nodes)),
        new,
        "node {to} still leads"
    );
    let last = propose_all(nodes[new].as_ref().unwrap(), &entries[N as usize..]).await;
    tokio::task::block_in_place(|| wait_applied(&nodes, last));
    tokio::task::block_in_place(|| converge(nodes, 2 * N, "handback"));
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

// ---------------------------------------------------------------------------
// Whole nodes: the facade (batcher, client offload) over openraft
// ---------------------------------------------------------------------------

use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::Rsm;

/// A whole cluster node over `dir`: the facade with its batcher, the
/// openraft replicator of `cluster`, `opts`.
fn open_facade(dir: &Path, cluster: ClusterConfig, opts: RaftOpts) -> RaftFacade {
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
    RaftFacade::open_cluster_node_for_test(&ctx, batcher, cluster, opts)
        .unwrap_or_else(|e| panic!("open facade {id}: {e}"))
}

/// Open every facade at once (each waits for a leader, which needs a quorum).
async fn open_facades(
    dirs: &[PathBuf],
    opts: &[RaftOpts],
    cluster: impl Fn(u64) -> ClusterConfig,
) -> Vec<Option<Arc<RaftFacade>>> {
    let mut hs = Vec::new();
    for (i, d) in dirs.iter().enumerate() {
        let (d, c, o) = (d.clone(), cluster(i as u64 + 1), opts[i].clone());
        hs.push(tokio::task::spawn_blocking(move || open_facade(&d, c, o)));
    }
    let mut out = Vec::new();
    for h in hs {
        out.push(Some(Arc::new(h.await.expect("open task"))));
    }
    out
}

/// The index of the one facade whose node leads (I13: ready), waiting for it.
async fn facade_leader(nodes: &[Option<Arc<RaftFacade>>]) -> usize {
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        let leaders: Vec<usize> = nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.as_ref().is_some_and(|f| f.health().role == "leader"))
            .map(|(i, _)| i)
            .collect();
        if leaders.len() == 1 {
            return leaders[0];
        }
        assert!(Instant::now() < end, "no single ready leader within 30 s");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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

fn push_body(n: u64) -> Vec<u8> {
    format!(
        "{{\"items\":[{{\"queue\":\"handoff\",\"partition\":\"p{}\",\"payload\":{{\"n\":{n}}}}}]}}",
        n % 4
    )
    .into_bytes()
}

/// A request in flight while leadership moves (the preferred-leader step, the
/// quorum-loss step-down and the SIGTERM hand-off all transfer it) gets its
/// answer — or a retry the facade takes to the new leader — within about the
/// transfer, never by waiting out its deadline. Clients push on every node
/// (a follower offloads its commands to the leader over `/raft/v1/submit`)
/// while leadership is handed around three times.
///
/// The hang this pins: a node that stopped leading kept every command that
/// reached its batcher afterwards (a follower still forwarding to it, a local
/// command already past the role check), and every command queued at the
/// step-down, until it led again — each caller waited out its whole deadline
/// (Jepsen P8 smokes: KV puts 10 s, a push 5 s whose entry had committed).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_request_in_flight_across_a_leader_transfer_is_answered_quickly() {
    let _one = serial();
    log_init();
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("transfer")).collect();
    let ports = free_ports(3);
    let opts = vec![test_opts(); 3];
    let nodes = open_facades(&dirs, &opts, |id| cluster_config(&ports, id)).await;
    facade_leader(&nodes).await;

    const DEADLINE: Duration = Duration::from_secs(10);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seq = Arc::new(AtomicU64::new(0));
    let mut clients = Vec::new();
    for node in nodes.iter().flatten() {
        for _ in 0..4 {
            let (node, stop, seq) = (node.clone(), stop.clone(), seq.clone());
            clients.push(tokio::spawn(async move {
                let mut worst = Duration::ZERO;
                let (mut ok, mut failed) = (0u64, Vec::new());
                while !stop.load(Ordering::Relaxed) {
                    let n = seq.fetch_add(1, Ordering::Relaxed);
                    let ctx = crate::rsm::facade::ReqCtx::new(
                        crate::config::DEFAULT_TENANT,
                        crate::rsm::facade::Deadline::after(DEADLINE),
                    );
                    let t0 = Instant::now();
                    let r = node
                        .push(ctx, crate::rsm::facade::PushReq { raw: push_body(n) })
                        .await;
                    worst = worst.max(t0.elapsed());
                    match r {
                        Ok(_) => ok += 1,
                        Err(e) => failed.push((t0.elapsed(), e.to_string())),
                    }
                }
                (worst, ok, failed)
            }));
        }
    }

    // Leadership moves three times while the clients push.
    tokio::time::sleep(Duration::from_millis(500)).await;
    for _ in 0..3 {
        let l = facade_leader(&nodes).await;
        let to = ((l + 1) % 3) as u64 + 1;
        let repl = nodes[l].as_ref().unwrap().repl_for_test();
        crate::rsm::replicator::Replicator::transfer_leadership(
            &*repl,
            Some(to),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("transfer");
        drop(repl);
        let end = Instant::now() + Duration::from_secs(10);
        while facade_leader(&nodes).await != to as usize - 1 {
            assert!(Instant::now() < end, "node {to} never took over");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
    stop.store(true, Ordering::Relaxed);

    let mut worst = Duration::ZERO;
    let (mut ok, mut failed) = (0u64, Vec::new());
    for c in clients {
        let (w, o, f) = c.await.expect("client");
        worst = worst.max(w);
        ok += o;
        failed.extend(f);
    }
    eprintln!("pushes answered: {ok}; the slowest across three transfers: {worst:?}");
    assert!(ok > 100, "the clients pushed ({ok} answered)");
    assert!(
        worst < Duration::from_secs(3),
        "a push waited {worst:?} across a leader transfer (deadline {DEADLINE:?}); failures: {failed:?}"
    );
    assert!(
        failed.is_empty(),
        "every push is answered (a retry is taken to the new leader): {failed:?}"
    );
    for n in nodes.into_iter().flatten() {
        close_facade(n).await;
    }
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

// ---------------------------------------------------------------------------
// Membership changes, the single-survivor recovery, the apply skip
// ---------------------------------------------------------------------------

use crate::rsm::replicator::raft::{MembershipStatus, SkipSpec};
use crate::rsm::replicator::{MembershipChange, ReplError};

/// [`open_node_in`], answering the error instead of panicking on it (the
/// store is closed again, so the directory reopens in this process).
fn try_open_node_in(
    dir: &Path,
    id: u64,
    cluster: ClusterConfig,
    opts: RaftOpts,
) -> std::io::Result<RaftReplicator<HeedStore>> {
    let cfg = node_config(dir, id);
    snapshot::apply_pending(dir, qlog_options(&cfg))?;
    let store = Arc::new(
        HeedStore::open(&dir.join("store"), &store_opts())
            .map_err(|e| std::io::Error::other(e.to_string()))?,
    );
    let keep = store.clone();
    match RaftReplicator::open_with(
        store,
        cfg,
        Some(cluster),
        Arc::new(NoWaker),
        Arc::new(SystemClock),
        opts,
    ) {
        Ok(r) => Ok(r),
        Err(e) => {
            close_store(keep);
            Err(e)
        }
    }
}

fn close_store(store: Arc<HeedStore>) {
    let end = Instant::now() + Duration::from_secs(10);
    let mut s = store;
    loop {
        match Arc::try_unwrap(s) {
            Ok(s) => return s.close(),
            Err(still) => {
                assert!(Instant::now() < end, "the store is still shared");
                s = still;
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

/// Close a node whose apply thread may have stopped on an error.
fn close_stopped(r: RaftReplicator<HeedStore>) {
    let store = r.store_for_test();
    let _ = r.shutdown();
    close_store(store);
}

fn node_addr(ports: &[u16], id: u64) -> String {
    format!("127.0.0.1:{}", ports[id as usize - 1])
}

async fn change(
    r: &RaftReplicator<HeedStore>,
    c: MembershipChange,
) -> Result<MembershipStatus, ReplError> {
    r.admin_change(c, Instant::now() + Duration::from_secs(20))
        .await
}

fn refused_code(r: Result<MembershipStatus, ReplError>) -> String {
    match r {
        Err(ReplError::Refused { code, .. }) => code,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// Wait until the leader `r` reports member `id` silent.
async fn until_silent(r: &RaftReplicator<HeedStore>, id: u64) {
    let end = Instant::now() + Duration::from_secs(20);
    loop {
        let st = r.membership_status(deadline()).await;
        if st.members.iter().any(|m| m.node_id == id && !m.live) {
            return;
        }
        assert!(
            Instant::now() < end,
            "member {id} was never reported silent: {st:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A voter whose disk is gone for good is removed — asked on a FOLLOWER,
/// which forwards the change to the leader — after the change that would have
/// cost the quorum is refused; the two left keep writing, and removing it
/// again changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_voter_is_removed_and_the_cluster_keeps_writing() {
    let _one = serial();
    log_init();
    const N: u64 = 100;
    let entries = workload(SEED, 2 * N);
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("remove")).collect();
    let ports = free_ports(3);
    let opts = vec![test_opts(); 3];
    let mut nodes = tokio::task::block_in_place(|| open_all(&dirs, &ports, &opts));
    let l = tokio::task::block_in_place(|| leader_of(&nodes));
    propose_all(nodes[l].as_ref().unwrap(), &entries[..N as usize]).await;
    let (dead, live) = ((l + 1) % 3, (l + 2) % 3);
    let (lid, did, fid) = (l as u64 + 1, dead as u64 + 1, live as u64 + 1);
    let _ = close(nodes[dead].take().unwrap());
    until_silent(nodes[l].as_ref().unwrap(), did).await;

    // Removing the LIVE follower would leave the leader and the dead voter:
    // one live voter of two, no quorum.
    let r = change(
        nodes[l].as_ref().unwrap(),
        MembershipChange::Remove { node: fid },
    )
    .await;
    assert_eq!(refused_code(r), "no_quorum");
    // The dead one goes, asked on the follower.
    let st = change(
        nodes[live].as_ref().unwrap(),
        MembershipChange::Remove { node: did },
    )
    .await
    .expect("remove the dead voter");
    let mut want = vec![lid, fid];
    want.sort_unstable();
    assert_eq!(st.voters, want, "{st:?}");
    assert!(st.members.iter().all(|m| m.node_id != did), "{st:?}");
    assert!(!st.change_in_flight, "{st:?}");
    assert_eq!(st.source, "leader");
    // Again: nothing to do.
    let again = change(
        nodes[l].as_ref().unwrap(),
        MembershipChange::Remove { node: did },
    )
    .await
    .expect("remove again");
    assert_eq!(again.voters, want);

    let last = propose_all(nodes[l].as_ref().unwrap(), &entries[N as usize..]).await;
    tokio::task::block_in_place(|| wait_applied(&nodes, last));

    // The follower restarts with the SAME peer list, which still names the
    // removed node: the membership in its log wins.
    let _ = close(nodes[live].take().unwrap());
    let (d, p) = (dirs[live].clone(), ports.clone());
    nodes[live] = Some(tokio::task::block_in_place(move || {
        open_node(&d, fid, &p, test_opts())
    }));
    let st = nodes[live]
        .as_ref()
        .unwrap()
        .membership_status(deadline())
        .await;
    assert_eq!(st.voters, want, "{st:?}");
    assert!(st.members.iter().all(|m| m.node_id != did), "{st:?}");
    // Its own membership, not only the leader's view it fetches.
    let own = nodes[live].as_ref().unwrap().membership().await;
    let mut own_voters = own.voters.clone();
    own_voters.sort_unstable();
    assert_eq!(own_voters, want, "{own:?}");
    tokio::task::block_in_place(|| wait_applied(&nodes, last));
    tokio::task::block_in_place(|| converge(nodes, 2 * N, "remove"));
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A new node joins a running cluster: it starts empty with `join` (no
/// initialize of its own), the leader adds it as a learner, it receives a
/// snapshot (the founders purge eagerly) and restarts on it, catches up, and
/// is promoted; the four converge. Promoting before it is a member, or while
/// it holds nothing, is refused; adding it again is a no-op, with other
/// addresses a refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_node_joins_as_a_learner_catches_up_and_is_promoted() {
    let _one = serial();
    log_init();
    const N: u64 = 150;
    let entries = workload(SEED, 3 * N);
    let dirs: Vec<PathBuf> = (0..4).map(|_| scratch("join")).collect();
    let ports = free_ports(4);
    let eager = RaftOpts {
        purge_hold: Duration::from_secs(2),
        log_keep: 0,
        purge_batch: 1,
        ..test_opts()
    };
    let founders = |id| cluster_config(&ports[..3], id);
    let mut nodes =
        tokio::task::block_in_place(|| open_all_in(&dirs[..3], &vec![eager.clone(); 3], founders));
    let l = tokio::task::block_in_place(|| leader_of(&nodes));
    let leader = nodes[l].as_ref().unwrap();
    propose_all(leader, &entries[..N as usize]).await;
    let end = Instant::now() + Duration::from_secs(60);
    while leader.purged_index() < 2 {
        assert!(Instant::now() < end, "the leader never purged its log");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Node 4: an empty directory, joining.
    let joiner = RaftOpts {
        join: true,
        ..eager.clone()
    };
    let (d4, c4, j4) = (dirs[3].clone(), cluster_config(&ports, 4), joiner.clone());
    let four = tokio::task::block_in_place(move || open_node_in(&d4, 4, c4, j4));
    let leader = nodes[l].as_ref().unwrap();
    let promote = |force| MembershipChange::Promote {
        nodes: vec![4],
        force,
    };
    assert_eq!(
        refused_code(change(leader, promote(false)).await),
        "not_a_learner"
    );
    let add = MembershipChange::AddLearner {
        node: 4,
        raft: node_addr(&ports, 4),
        http: "127.0.0.1:1".into(),
    };
    let st = change(leader, add.clone()).await.expect("add the learner");
    assert!(st.learners.contains(&4), "{st:?}");
    assert_eq!(
        refused_code(change(leader, promote(false)).await),
        "learner_behind"
    );
    change(leader, add)
        .await
        .expect("adding it again is a no-op");
    let elsewhere = MembershipChange::AddLearner {
        node: 4,
        raft: "127.0.0.1:9".into(),
        http: "127.0.0.1:1".into(),
    };
    assert_eq!(
        refused_code(change(leader, elsewhere).await),
        "address_mismatch"
    );

    // It receives a snapshot, stops to load it, and comes back on it.
    let end = Instant::now() + Duration::from_secs(60);
    while four.restart_requested().is_none() {
        assert!(
            Instant::now() < end,
            "the learner never received a snapshot"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = close(four);
    let (d4, c4) = (dirs[3].clone(), cluster_config(&ports, 4));
    nodes.push(Some(tokio::task::block_in_place(move || {
        open_node_in(&d4, 4, c4, joiner)
    })));
    let mid = propose_all(
        nodes[l].as_ref().unwrap(),
        &entries[N as usize..2 * N as usize],
    )
    .await;
    tokio::task::block_in_place(|| wait_applied(&nodes, mid));
    let st = change(nodes[l].as_ref().unwrap(), promote(false))
        .await
        .expect("promote the caught-up learner");
    assert_eq!(st.voters, vec![1, 2, 3, 4], "{st:?}");
    assert!(st.learners.is_empty(), "{st:?}");

    let last = propose_all(nodes[l].as_ref().unwrap(), &entries[2 * N as usize..]).await;
    tokio::task::block_in_place(|| wait_applied(&nodes, last));
    tokio::task::block_in_place(|| converge(nodes, 3 * N, "join"));
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Two of three disks are gone for good. The survivor, restarted with
/// `force_recover` naming itself, becomes its only voter in a new term and
/// serves; two new nodes on empty directories join it (learner, catch up,
/// promote) and the three converge on everything the survivor held plus what
/// came after. The setting refuses another node's id, refuses to leave no
/// voter, and — left on after members were added — refuses to recover again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_survivor_is_force_recovered_and_two_new_nodes_join_it() {
    let _one = serial();
    log_init();
    const N: u64 = 100;
    let entries = workload(SEED, 3 * N);
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("recover")).collect();
    let ports = free_ports(3);
    let opts = vec![test_opts(); 3];
    let mut nodes = tokio::task::block_in_place(|| open_all(&dirs, &ports, &opts));
    let l = tokio::task::block_in_place(|| leader_of(&nodes));
    let first = propose_all(nodes[l].as_ref().unwrap(), &entries[..N as usize]).await;
    tokio::task::block_in_place(|| wait_applied(&nodes, first));
    for n in nodes.iter_mut() {
        let _ = close(n.take().unwrap());
    }
    // Nodes 2 and 3 lose their disks.
    for d in &dirs[1..] {
        std::fs::remove_dir_all(d).expect("remove");
        std::fs::create_dir_all(d).expect("recreate");
    }

    let wrong = RaftOpts {
        force_recover: Some(2),
        ..test_opts()
    };
    let (d1, c1) = (dirs[0].clone(), cluster_config(&ports, 1));
    let e = tokio::task::block_in_place(move || try_open_node_in(&d1, 1, c1, wrong))
        .err()
        .expect("another node's id is refused");
    assert!(e.to_string().contains("QUEEN_RAFT_FORCE_RECOVER=2"), "{e}");

    let recover = RaftOpts {
        force_recover: Some(1),
        ..test_opts()
    };
    let (d1, c1, r1) = (dirs[0].clone(), cluster_config(&ports, 1), recover.clone());
    let one = tokio::task::block_in_place(move || open_node_in(&d1, 1, c1, r1));
    let st = one.membership_status(deadline()).await;
    assert_eq!(st.voters, vec![1], "{st:?}");
    assert!(st.learners.is_empty(), "{st:?}");
    assert!(dirs[0].join("raft").join("force_recovered.json").exists());
    let mid = propose_all(&one, &entries[N as usize..2 * N as usize]).await;
    assert_eq!(
        refused_code(change(&one, MembershipChange::Remove { node: 1 }).await),
        "last_voter"
    );

    // Two new nodes join it.
    let join = RaftOpts {
        join: true,
        ..test_opts()
    };
    let mut nodes = vec![Some(one), None, None];
    for id in [2u64, 3] {
        let (d, c, j) = (
            dirs[id as usize - 1].clone(),
            cluster_config(&ports, id),
            join.clone(),
        );
        nodes[id as usize - 1] = Some(tokio::task::block_in_place(move || {
            open_node_in(&d, id, c, j)
        }));
        change(
            nodes[0].as_ref().unwrap(),
            MembershipChange::AddLearner {
                node: id,
                raft: node_addr(&ports, id),
                http: "127.0.0.1:1".into(),
            },
        )
        .await
        .expect("add a learner");
    }
    tokio::task::block_in_place(|| wait_applied(&nodes, mid));
    let st = change(
        nodes[0].as_ref().unwrap(),
        MembershipChange::Promote {
            nodes: vec![2, 3],
            force: false,
        },
    )
    .await
    .expect("promote both");
    assert_eq!(st.voters, vec![1, 2, 3], "{st:?}");

    // Left on after members were added, the setting refuses to recover again.
    let _ = close(nodes[0].take().unwrap());
    let (d1, c1) = (dirs[0].clone(), cluster_config(&ports, 1));
    let e = tokio::task::block_in_place(move || try_open_node_in(&d1, 1, c1, recover))
        .err()
        .expect("a second recovery is refused");
    assert!(e.to_string().contains("already recovered"), "{e}");
    let (d1, c1) = (dirs[0].clone(), cluster_config(&ports, 1));
    nodes[0] = Some(tokio::task::block_in_place(move || {
        open_node_in(&d1, 1, c1, test_opts())
    }));
    let l = tokio::task::block_in_place(|| leader_of(&nodes));
    let last = propose_all(nodes[l].as_ref().unwrap(), &entries[2 * N as usize..]).await;
    tokio::task::block_in_place(|| wait_applied(&nodes, last));
    tokio::task::block_in_place(|| converge(nodes, 3 * N, "recover"));
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Cloning a survivor's data directory onto the two nodes that lost theirs —
/// the replacements keeping their ids and addresses, every node stopped
/// during the copy — gives back a working three-node cluster with no file
/// removed or rewritten: each clone starts from the survivor's log, vote and
/// commit point, and the old membership needs two of the three, which the
/// clones now are. What is lost is the same as with a forced recovery:
/// whatever the survivor did not hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_survivors_directory_cloned_onto_two_new_nodes_forms_the_cluster_again() {
    let _one = serial();
    log_init();
    const N: u64 = 100;
    let entries = workload(SEED, 2 * N);
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("clone")).collect();
    let ports = free_ports(3);
    let opts = vec![test_opts(); 3];
    let mut nodes = tokio::task::block_in_place(|| open_all(&dirs, &ports, &opts));
    let l = tokio::task::block_in_place(|| leader_of(&nodes));
    let first = propose_all(nodes[l].as_ref().unwrap(), &entries[..N as usize]).await;
    tokio::task::block_in_place(|| wait_applied(&nodes, first));
    for n in nodes.iter_mut() {
        let _ = close(n.take().unwrap());
    }
    fn copy_dir(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).expect("mkdir");
        for e in std::fs::read_dir(from).expect("read_dir") {
            let e = e.expect("entry");
            let (src, dst) = (e.path(), to.join(e.file_name()));
            if e.file_type().expect("type").is_dir() {
                copy_dir(&src, &dst);
            } else {
                std::fs::copy(&src, &dst).expect("copy");
            }
        }
    }
    for d in &dirs[1..] {
        std::fs::remove_dir_all(d).expect("remove");
        copy_dir(&dirs[0], d);
    }
    let mut nodes = tokio::task::block_in_place(|| open_all(&dirs, &ports, &opts));
    let l = tokio::task::block_in_place(|| leader_of(&nodes));
    let last = propose_all(nodes[l].as_ref().unwrap(), &entries[N as usize..]).await;
    tokio::task::block_in_place(|| wait_applied(&nodes, last));
    // Every clone answers for itself (its own id), not as the survivor.
    for (i, n) in nodes.iter().enumerate() {
        assert_eq!(n.as_ref().unwrap().node_id(), i as u64 + 1);
    }
    let _ = nodes.iter_mut();
    tokio::task::block_in_place(|| converge(nodes, 2 * N, "clone"));
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// The request id of the entry the apply-skip test poisons.
const POISON: [u8; 16] = *b"poisoned-entry!!";

/// The workload's first appended partition (pid, bucket) and its next offset
/// after `entries`.
fn next_append_slot(entries: &[Bytes]) -> (u64, u16, u64) {
    use crate::rsm::effect::Effect;
    let mut first = None;
    let mut next = std::collections::HashMap::new();
    for b in entries {
        let e = crate::rsm::entry::decode_entry(b).expect("decode");
        for eff in &e.effects {
            if let Effect::Append {
                pid,
                bucket,
                base_offset,
                count,
                ..
            } = eff
            {
                first.get_or_insert((*pid, *bucket));
                let n = next.entry(*pid).or_insert(0u64);
                *n = (*n).max(base_offset + *count as u64);
            }
        }
    }
    let (pid, bucket) = first.expect("an append");
    (pid, bucket, next[&pid])
}

/// An entry that appends three messages to `pid` at `base` — the offsets the
/// workload's next append to it takes — right after `after`, under the
/// poisoned request id: its payload is 0xEE bytes, the workload's 0xAB.
fn poisoned_entry(
    after: &Bytes,
    pid: u64,
    bucket: u16,
    base: u64,
) -> (Bytes, crate::rsm::entry::Entry) {
    use crate::rsm::effect::Effect;
    use crate::rsm::entry::{Entry, Outcome};
    let prev = crate::rsm::entry::decode_entry(after).expect("decode");
    let mut e = Entry::new(prev.now_us, prev.pid_base, prev.kv_version_base);
    e.add_command(
        POISON,
        Outcome::Empty,
        vec![Effect::Append {
            pid,
            bucket,
            base_offset: base,
            count: 3,
            created_at_us: prev.now_us,
            hashes: super::apply::hashes(0xBAD, 3),
            blob: vec![0xEE; 72],
        }],
    )
    .expect("add command");
    (Bytes::from(encode_entry(&e).expect("encode")), e)
}

/// An entry every node refuses to apply (a deterministic bug, injected) stops
/// every node that applies it, with the index, term, effect, command and
/// digest in the failure; restarted as they are they stop on it again. With
/// `apply_skip` naming it (and its digest) on every node they resume: the
/// entry is a no-op everywhere, the digests agree, and its records are gone —
/// the partition's next messages take its offsets, and every read of them
/// (pop payload, dedup hashes, the entry record) finds the new ones. A wrong
/// digest is refused at boot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entry_every_node_refuses_is_skipped_and_leaves_nothing_behind() {
    let _one = serial();
    log_init();
    const K: u64 = 60;
    const N: u64 = 160;
    let entries = workload(SEED, N);
    let (pid, bucket, base) = next_append_slot(&entries[..K as usize]);
    let (x, x_entry) = poisoned_entry(&entries[K as usize - 1], pid, bucket, base);
    let x_digest = crate::rsm::entry::entry_digest(&x_entry);
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("skip")).collect();
    let ports = free_ports(3);
    let opts = vec![test_opts(); 3];
    let mut nodes = tokio::task::block_in_place(|| open_all(&dirs, &ports, &opts));
    let l = tokio::task::block_in_place(|| leader_of(&nodes));
    let before = propose_all(nodes[l].as_ref().unwrap(), &entries[..K as usize]).await;
    tokio::task::block_in_place(|| wait_applied(&nodes, before));

    crate::rsm::faults::refuse_apply_of(POISON);
    let res = nodes[l]
        .as_ref()
        .unwrap()
        .propose(x.clone(), Instant::now() + Duration::from_secs(10))
        .await;
    assert!(res.is_err(), "the poisoned entry never applies: {res:?}");
    // Every node that applies it stops on it (at least a quorum does: the
    // last one may never learn that it committed).
    let failures = tokio::task::block_in_place(|| {
        let end = Instant::now() + Duration::from_secs(30);
        loop {
            let f: Vec<(usize, apply::ApplyFailure)> = nodes
                .iter()
                .enumerate()
                .filter_map(|(i, n)| {
                    let v = n.as_ref()?.apply_status();
                    let f = v.get("failure").filter(|f| !f.is_null())?;
                    Some((i, serde_json::from_value(f.clone()).expect("a failure")))
                })
                .collect();
            if f.len() >= 2 {
                return f;
            }
            assert!(
                Instant::now() < end,
                "the poisoned entry did not stop two nodes"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    });
    let index = failures[0].1.index;
    for (i, f) in &failures {
        assert_eq!(f.index, index, "node {}: {f:?}", i + 1);
        assert_eq!(
            f.digest,
            format!("{x_digest:016x}"),
            "node {}: {f:?}",
            i + 1
        );
        assert_eq!(f.class, "deterministic", "{f:?}");
        assert!(
            f.effect.as_deref().is_some_and(|e| e.contains("append")),
            "{f:?}"
        );
        let id: String = POISON.iter().map(|b| format!("{b:02x}")).collect();
        assert!(
            f.command.as_deref().is_some_and(|c| c.contains(&id)),
            "{f:?}"
        );
        assert!(nodes[*i].as_ref().unwrap().role() == Role::Stopped);
    }
    for n in nodes.iter_mut() {
        close_stopped(n.take().unwrap());
    }

    // Restarted as they are, they stop on it again: nobody gets past it.
    let reopened: Vec<std::io::Result<RaftReplicator<HeedStore>>> =
        tokio::task::block_in_place(|| {
            std::thread::scope(|s| {
                let hs: Vec<_> = (0..3u64)
                    .map(|i| {
                        let (d, c) = (dirs[i as usize].clone(), cluster_config(&ports, i + 1));
                        s.spawn(move || try_open_node_in(&d, i + 1, c, test_opts()))
                    })
                    .collect();
                hs.into_iter().map(|h| h.join().expect("open")).collect()
            })
        });
    tokio::time::sleep(Duration::from_secs(3)).await;
    for r in reopened.into_iter().flatten() {
        assert!(
            r.applied_index() < index,
            "a node applied past the poisoned entry"
        );
        close_stopped(r);
    }

    // A digest that is not the entry's is refused at boot.
    let holder = failures[0].0;
    let wrong = RaftOpts {
        apply_skip: vec![SkipSpec {
            group: 0,
            index,
            digest: Some(x_digest ^ 1),
        }],
        ..test_opts()
    };
    let (d, c) = (
        dirs[holder].clone(),
        cluster_config(&ports, holder as u64 + 1),
    );
    let e = tokio::task::block_in_place(move || try_open_node_in(&d, holder as u64 + 1, c, wrong))
        .err()
        .expect("a wrong digest is refused");
    assert!(
        e.to_string().contains("is not the entry that failed"),
        "{e}"
    );

    // With the skip on every node, they resume.
    let skip = RaftOpts {
        apply_skip: vec![SkipSpec {
            group: 0,
            index,
            digest: Some(x_digest),
        }],
        ..test_opts()
    };
    let nodes = tokio::task::block_in_place(|| open_all(&dirs, &ports, &vec![skip; 3]));
    let l = tokio::task::block_in_place(|| leader_of(&nodes));
    let last = propose_all(nodes[l].as_ref().unwrap(), &entries[K as usize..]).await;
    tokio::task::block_in_place(|| wait_applied(&nodes, last));
    let qid =
        crate::rsm::qlog::set::QLogSet::queue_id_of(super::apply::TENANT, super::apply::QUEUE);
    let x_hash = super::apply::hash_at(0xBAD, 0);
    for (i, n) in nodes.iter().enumerate() {
        let n = n.as_ref().unwrap();
        let st = n.apply_status();
        let skipped = st["skipped"].as_array().expect("skipped");
        assert!(
            skipped.iter().any(|s| s["index"].as_u64() == Some(index)),
            "node {} lists the skip: {st}",
            i + 1
        );
        let qr = n.qlog_reader().expect("qlog reader");
        let mut reread = 0;
        for o in base..base + 3 {
            if let Some(rec) = qr.read_owned(qid, pid, o).expect("read") {
                assert_ne!(
                    rec.seq,
                    index,
                    "node {}: offset {o} reads the skipped record",
                    i + 1
                );
                assert!(
                    rec.payload.iter().all(|b| *b == 0xAB),
                    "node {}: offset {o} holds a payload that is not the workload's",
                    i + 1
                );
                reread += 1;
            }
        }
        assert!(
            reread > 0,
            "node {}: the workload re-used the skipped offsets",
            i + 1
        );
        for f in qr
            .committed_frames(qid, pid, 0, u64::MAX, true)
            .expect("frames")
        {
            assert!(
                !f.hashes.chunks(16).any(|h| h == x_hash),
                "node {}: the skipped entry's dedup hashes are still readable",
                i + 1
            );
        }
        let recs = qr
            .entry_records_range(index, index + 1)
            .expect("entry record");
        assert_eq!(recs.len(), 1, "node {}", i + 1);
        let e = crate::rsm::entry::decode_entry(&recs[0].0.entry).expect("decode");
        assert!(apply::is_skip_marker(&e), "node {}: {e:?}", i + 1);
    }
    crate::rsm::faults::stop_refusing_apply_of(POISON);
    tokio::task::block_in_place(|| converge(nodes, N, "skip"));
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// The operator endpoints end to end, on a FOLLOWER's router (auth off,
/// tenancy on): the membership read (the leader's view, fetched), a tenant
/// refused, malformed calls refused, and — once a member dies — the change
/// that would cost the quorum refused (409 `no_quorum`), an empty voter set
/// refused (409 `last_voter`), and the dead member removed (200, forwarded to
/// the leader).
#[cfg(all(feature = "server", feature = "kafka"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn the_membership_endpoints_answer_through_the_router() {
    use axum::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    let _one = serial();
    log_init();
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("endpoints")).collect();
    let ports = free_ports(3);
    let mut nodes = open_facades(&dirs, &vec![test_opts(); 3], |id| {
        cluster_config(&ports, id)
    })
    .await;
    let l = facade_leader(&nodes).await;
    let (f, dead) = ((l + 1) % 3, (l + 2) % 3);
    let (lid, fid, did) = (l as u64 + 1, f as u64 + 1, dead as u64 + 1);

    let cfg_dir = scratch("endpoints-cfg");
    std::env::set_var("QUEEN_RAFT_DIR", cfg_dir.display().to_string());
    let cfg = crate::config::load();
    let rsm: Arc<dyn Rsm> = nodes[f].clone().unwrap();
    let state = crate::handlers::raft::build_raft_state_with(&cfg, Some(rsm)).expect("state");
    let auth = crate::auth::Authenticator::new(crate::config::AuthConfig {
        enabled: false,
        algorithm: "HS256".into(),
        secret: String::new(),
        public_key: String::new(),
        jwks_url: String::new(),
        jwks_refresh_interval_seconds: 3600,
        jwks_request_timeout_ms: 5000,
        issuer: String::new(),
        audience: String::new(),
        clock_skew_seconds: 30,
        skip_paths: Vec::new(),
        roles_claim: "role".into(),
        roles_array_claim: "roles".into(),
        role_admin: "admin".into(),
        role_read_write: "read-write".into(),
        role_read_only: "read-only".into(),
        role_write_only: "write-only".into(),
    });
    let router = crate::handlers::raft::build_raft_router(state, auth, true);
    let call =
        |method: Method, path: String, body: Option<String>, tenant: Option<&'static str>| {
            let router = router.clone();
            async move {
                let mut req = Request::builder().method(method).uri(path);
                if let Some(t) = tenant {
                    req = req.header(crate::config::TENANT_HEADER, t);
                }
                let req = req
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.unwrap_or_default()))
                    .expect("request");
                let resp = router.oneshot(req).await.expect("answer");
                let status = resp.status();
                let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                    .await
                    .expect("body");
                let v: serde_json::Value =
                    serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
                (status, v)
            }
        };
    const M: &str = "/api/v1/system/raft/membership";

    let (s, v) = call(Method::GET, M.into(), None, None).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(
        v["membership"]["voters"],
        serde_json::json!([1, 2, 3]),
        "{v}"
    );
    assert_eq!(v["membership"]["source"], "leader", "{v}");
    assert_eq!(v["membership"]["leader"], lid, "{v}");
    let (s, v) = call(
        Method::GET,
        M.into(),
        None,
        Some("00000000-0000-0000-0000-000000000042"),
    )
    .await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{v}");
    let (s, v) = call(
        Method::POST,
        format!("{M}/learners"),
        Some("{}".into()),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    let (s, v) = call(Method::DELETE, format!("{M}/members/abc"), None, None).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");

    // A member dies.
    close_facade(nodes[dead].take().unwrap()).await;
    let end = Instant::now() + Duration::from_secs(20);
    loop {
        let (_, v) = call(Method::GET, M.into(), None, None).await;
        let silent = v["membership"]["members"]
            .as_array()
            .is_some_and(|ms| ms.iter().any(|m| m["nodeId"] == did && m["live"] == false));
        if silent {
            break;
        }
        assert!(
            Instant::now() < end,
            "the dead member was never reported silent: {v}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (s, v) = call(Method::DELETE, format!("{M}/members/{lid}"), None, None).await;
    assert_eq!(s, StatusCode::CONFLICT, "{v}");
    assert_eq!(v["code"], "no_quorum", "{v}");
    let (s, v) = call(
        Method::PUT,
        format!("{M}/voters"),
        Some(r#"{"voters":[]}"#.into()),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT, "{v}");
    assert_eq!(v["code"], "last_voter", "{v}");
    let (s, v) = call(Method::DELETE, format!("{M}/members/{did}"), None, None).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let mut want = vec![lid, fid];
    want.sort_unstable();
    assert_eq!(v["membership"]["voters"], serde_json::json!(want), "{v}");
    assert_eq!(v["ok"], true, "{v}");
    // A learner comes and goes (a learner is not counted for the quorum).
    let body = format!(
        r#"{{"id":{did},"raft":"{}","http":"127.0.0.1:1"}}"#,
        node_addr(&ports, did)
    );
    let (s, v) = call(Method::POST, format!("{M}/learners"), Some(body), None).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["membership"]["learners"], serde_json::json!([did]), "{v}");
    let (s, v) = call(Method::DELETE, format!("{M}/members/{did}"), None, None).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["membership"]["learners"], serde_json::json!([]), "{v}");
    assert_eq!(v["membership"]["voters"], serde_json::json!(want), "{v}");

    drop(router);
    for n in nodes.into_iter().flatten() {
        close_facade(n).await;
    }
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
    let _ = std::fs::remove_dir_all(cfg_dir);
}

/// Writes go on while the membership changes: openraft appends a change's
/// config entries itself, between two entries the leader's batcher proposed
/// at PREDICTED indexes. The batcher pauses around the change
/// (`batcher::QuiesceReq`) and plans again from the log's end. Before, the
/// next entry landed two indexes late, the batcher stopped for good, and
/// every write answered 500 ("planner channel closed") until leadership moved
/// (Jepsen membership nemesis, 2026-09-25).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn writes_go_on_across_membership_changes() {
    let _one = serial();
    log_init();
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("member-writes")).collect();
    let ports = free_ports(4);
    let opts = vec![test_opts(); 3];
    let nodes = open_facades(&dirs, &opts, |id| cluster_config(&ports[..3], id)).await;
    facade_leader(&nodes).await;

    const DEADLINE: Duration = Duration::from_secs(10);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seq = Arc::new(AtomicU64::new(0));
    let mut clients = Vec::new();
    for node in nodes.iter().flatten() {
        for _ in 0..4 {
            let (node, stop, seq) = (node.clone(), stop.clone(), seq.clone());
            clients.push(tokio::spawn(async move {
                let mut worst = Duration::ZERO;
                let (mut ok, mut failed) = (0u64, Vec::new());
                while !stop.load(Ordering::Relaxed) {
                    let n = seq.fetch_add(1, Ordering::Relaxed);
                    let ctx = crate::rsm::facade::ReqCtx::new(
                        crate::config::DEFAULT_TENANT,
                        crate::rsm::facade::Deadline::after(DEADLINE),
                    );
                    let t0 = Instant::now();
                    let r = node
                        .push(ctx, crate::rsm::facade::PushReq { raw: push_body(n) })
                        .await;
                    worst = worst.max(t0.elapsed());
                    match r {
                        Ok(_) => ok += 1,
                        Err(e) => failed.push((t0.elapsed(), e.to_string())),
                    }
                }
                (worst, ok, failed)
            }));
        }
    }

    // Twice: a learner joins (a config entry) and leaves (another).
    tokio::time::sleep(Duration::from_millis(500)).await;
    for _ in 0..2 {
        for change in [
            MembershipChange::AddLearner {
                node: 4,
                raft: format!("127.0.0.1:{}", ports[3]),
                http: "127.0.0.1:1".into(),
            },
            MembershipChange::Remove { node: 4 },
        ] {
            let l = facade_leader(&nodes).await;
            let repl = nodes[l].as_ref().unwrap().repl_for_test();
            let crate::rsm::replicator::node::NodeReplicator::Raft(r) = &*repl else {
                panic!("an openraft node");
            };
            r.admin_change(change, Instant::now() + Duration::from_secs(10))
                .await
                .expect("membership change");
            drop(repl);
            tokio::time::sleep(Duration::from_millis(700)).await;
        }
    }
    stop.store(true, Ordering::Relaxed);

    let mut worst = Duration::ZERO;
    let (mut ok, mut failed) = (0u64, Vec::new());
    for c in clients {
        let (w, o, f) = c.await.expect("client");
        worst = worst.max(w);
        ok += o;
        failed.extend(f);
    }
    eprintln!("pushes answered: {ok}; the slowest across four membership changes: {worst:?}");
    assert!(ok > 100, "the clients pushed ({ok} answered)");
    assert!(
        failed.is_empty(),
        "every push is answered across the membership changes: {failed:?}"
    );
    assert!(
        worst < Duration::from_secs(3),
        "a push waited {worst:?} across a membership change"
    );
    for n in nodes.into_iter().flatten() {
        close_facade(n).await;
    }
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}
