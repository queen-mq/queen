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
