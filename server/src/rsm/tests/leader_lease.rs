//! A leader counts as the leader under the leader lease, on a running cluster
//! of three whole nodes (facade, batcher, consumption engine, openraft) over
//! real HTTP on localhost, like `cluster_version.rs`.
//!
//! - [`a_leader_cut_off_from_the_others_stops_leading_until_a_quorum_answers_again`]:
//!   the leader leads while a quorum answers it. With the other two closed,
//!   openraft keeps it Leader (no newer term reaches it), but once no quorum
//!   acknowledged it within openraft's leader lease (`election_timeout_max`,
//!   2 s by default) it does not lead and names no leader: the Kafka facade
//!   then answers Metadata with no leader and Fetch with
//!   NOT_LEADER_OR_FOLLOWER, and its clients ask another node. With one
//!   member back, a node leads again.
//!
//! Without the lease in `RaftFacade::leads`, the cut-off leader went on
//! leading. Jepsen's Kafka workload (a partition that cut the leader off) saw
//! a consumer fetch from it past the end of its stale state, answered
//! OFFSET_OUT_OF_RANGE, and the consumer went back to offset 0.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::Rsm;
use crate::rsm::replicator::raft::{ClusterConfig, RaftOpts};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// One cluster at a time from this file and `txn_reserved.rs`, which opens
/// its cluster with the helpers below (each runs three whole nodes).
pub(super) static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(super) const TENANT: &str = crate::config::DEFAULT_TENANT;

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-leader-lease-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

pub(super) fn free_ports(n: usize) -> Vec<u16> {
    let ls: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind"))
        .collect();
    ls.iter()
        .map(|l| l.local_addr().expect("addr").port())
        .collect()
}

pub(super) fn cluster_config(ports: &[u16], id: u64) -> ClusterConfig {
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

fn opts() -> RaftOpts {
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
        kinds: Some(crate::rsm::effect::SUPPORTED_KINDS_VERSION),
        link_hold: Duration::from_secs(3600),
        link_hold_disk_pct: 100.0,
        member_hold: Duration::ZERO,
        member_hold_disk_pct: 100.0,
    }
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
    RaftFacade::open_cluster_node_for_test(&ctx, batcher, cluster, opts())
        .unwrap_or_else(|e| panic!("open facade {id}: {e}"))
}

pub(super) async fn open(dir: &Path, cluster: ClusterConfig) -> Arc<RaftFacade> {
    let d = dir.to_path_buf();
    Arc::new(
        tokio::task::spawn_blocking(move || open_facade(&d, cluster))
            .await
            .expect("open"),
    )
}

pub(super) async fn close(f: Arc<RaftFacade>) {
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

/// The open nodes openraft calls leader.
fn raft_leaders(nodes: &[Option<Arc<RaftFacade>>]) -> Vec<usize> {
    (0..nodes.len())
        .filter(|i| {
            nodes[*i]
                .as_ref()
                .is_some_and(|n| n.health().role == "leader")
        })
        .collect()
}

/// The open nodes that lead: openraft's leader, under the lease.
pub(super) fn leading(nodes: &[Option<Arc<RaftFacade>>]) -> Vec<usize> {
    (0..nodes.len())
        .filter(|i| nodes[*i].as_ref().is_some_and(|n| n.leads(TENANT)))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_leader_cut_off_from_the_others_stops_leading_until_a_quorum_answers_again() {
    let _one = ONE_AT_A_TIME.lock().await;
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("cut-off")).collect();
    let ports = free_ports(3);
    // All three at once: a fresh node asks its peers whether a cluster exists
    // before it initializes one, and waits for them to answer.
    let mut hs = Vec::new();
    for (i, d) in dirs.iter().enumerate() {
        let (d, c) = (d.clone(), cluster_config(&ports, i as u64 + 1));
        hs.push(tokio::task::spawn_blocking(move || open_facade(&d, c)));
    }
    let mut nodes: Vec<Option<Arc<RaftFacade>>> = Vec::new();
    for h in hs {
        nodes.push(Some(Arc::new(h.await.expect("open"))));
    }

    // A leader a quorum answers leads, and is the only one.
    let end = Instant::now() + Duration::from_secs(30);
    let l = loop {
        let (raft, lead) = (raft_leaders(&nodes), leading(&nodes));
        if raft.len() == 1 && lead == raft {
            break raft[0];
        }
        assert!(
            Instant::now() < end,
            "no single leader that leads: openraft's {raft:?}, leading {lead:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    // The other two close: nobody acknowledges the leader any more.
    for i in 0..3 {
        if i != l {
            close(nodes[i].take().expect("open")).await;
        }
    }
    let cut = nodes[l].clone().expect("the leader stays open");
    let end = Instant::now() + Duration::from_secs(10);
    while cut.leads(TENANT) {
        assert!(
            Instant::now() < end,
            "a leader cut off from its quorum for 10 s still leads"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Still openraft's leader: the lease, not a step-down, is what ended it.
    assert_eq!(
        cut.health().role,
        "leader",
        "the cut-off node is no longer openraft's leader, so this proves nothing"
    );
    assert_eq!(cut.leader_node(TENANT), None, "it names no other leader");
    drop(cut);

    // One member back makes a quorum again: a node leads.
    let back = (l + 1) % 3;
    nodes[back] = Some(open(&dirs[back], cluster_config(&ports, back as u64 + 1)).await);
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        let (raft, lead) = (raft_leaders(&nodes), leading(&nodes));
        if raft.len() == 1 && lead == raft {
            break;
        }
        assert!(
            Instant::now() < end,
            "with a quorum back no node leads: openraft's {raft:?}, leading {lead:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    for n in nodes.into_iter().flatten() {
        close(n).await;
    }
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}
