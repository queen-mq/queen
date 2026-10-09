//! The cluster version (§12.8, D20) on running clusters of whole nodes —
//! facade, batcher, consumption engine, openraft — over real HTTP on
//! localhost. Each member tells the leader the highest effect catalogue
//! version it reads (the test's nodes play builds that read more, or less, or
//! say nothing as a node older than this build), and the leader's batcher
//! raises the replicated version by itself.
//!
//! - [`the_cluster_version_waits_for_the_last_member_and_then_rises_by_itself`]:
//!   two members reading 4 and one that says nothing, then reads 3, hold the
//!   version at the baseline (3); once the last one reads 4 it rises to 4 on
//!   every node.
//! - [`the_cluster_version_never_falls`]: a cluster at 4 keeps it across a
//!   leader change and a restart, with a new leader that has heard nothing yet.
//! - [`a_check_waits_for_cluster_version_5_and_is_then_served_by_every_node`]:
//!   the first FORWARDED SHAPE minted at a catalogue version (5, the KV
//!   `check` op): refused, retryable, by every node while one member reads 4;
//!   served by every node, the followers through the leader, once it reads 5.
//!
//! The refusals of a node that reads less (to join, to boot) are
//! `raft_cluster.rs`'s; the writers' gate is the consumption engine's
//! (`consume/tests_ack.rs`) and the batcher's (`tests/batcher.rs`).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::Rsm;
use crate::rsm::replicator::raft::{ClusterConfig, RaftOpts};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// One cluster at a time from this file (each runs three whole nodes).
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-cluster-version-{tag}-{}-{}",
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

/// A node that says it reads catalogue version `kinds` (`None`: says nothing).
fn opts(kinds: Option<u32>) -> RaftOpts {
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
        kinds,
        link_hold: Duration::from_secs(3600),
        link_hold_disk_pct: 100.0,
    }
}

/// A whole node; its leader compares the cluster version with what every
/// member reads every 100 ms.
fn open_facade(dir: &Path, cluster: ClusterConfig, kinds: Option<u32>) -> RaftFacade {
    let ctx = crate::rsm::facade::RsmBuildCtx {
        data_dir: dir.display().to_string(),
        notifier: crate::notify::Notifier::new(false),
        disk_high_pct: 99.0,
        disk_low_pct: 98.0,
    };
    let batcher = crate::rsm::batcher::BatcherConfig {
        maintenance_every_ms: 0,
        cluster_version_every_ms: 100,
        ..crate::rsm::batcher::BatcherConfig::from_env()
    };
    let id = cluster.node_id;
    RaftFacade::open_cluster_node_for_test(&ctx, batcher, cluster, opts(kinds))
        .unwrap_or_else(|e| panic!("open facade {id}: {e}"))
}

async fn open(dir: &Path, cluster: ClusterConfig, kinds: Option<u32>) -> Arc<RaftFacade> {
    let d = dir.to_path_buf();
    Arc::new(
        tokio::task::spawn_blocking(move || open_facade(&d, cluster, kinds))
            .await
            .expect("open"),
    )
}

async fn close(f: Arc<RaftFacade>) {
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

/// The one node that leads, waiting for it.
async fn leader(nodes: &[Option<Arc<RaftFacade>>]) -> usize {
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        let leaders: Vec<usize> = (0..nodes.len())
            .filter(|i| {
                nodes[*i]
                    .as_ref()
                    .is_some_and(|n| n.health().role == "leader")
            })
            .collect();
        if leaders.len() == 1 {
            return leaders[0];
        }
        assert!(Instant::now() < end, "no single ready leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Every open node's committed cluster version.
fn versions(nodes: &[Option<Arc<RaftFacade>>]) -> Vec<u32> {
    nodes
        .iter()
        .flatten()
        .map(|n| n.cluster_version())
        .collect()
}

/// Wait until every open node is at `want`.
async fn until_version(nodes: &[Option<Arc<RaftFacade>>], want: u32) {
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        let got = versions(nodes);
        if got.iter().all(|v| *v == want) {
            return;
        }
        assert!(
            Instant::now() < end,
            "the cluster version is {got:?}, never {want} everywhere"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until the leader has heard every member say what it reads (as the
/// membership status shows it: `want` per node id), then for ten of its
/// comparisons more.
async fn heard(nodes: &[Option<Arc<RaftFacade>>], want: &[(u64, Option<u32>)]) {
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        let l = leader(nodes).await;
        let st = nodes[l]
            .as_ref()
            .unwrap()
            .raft_membership(crate::rsm::facade::ReqCtx::new(
                crate::config::DEFAULT_TENANT,
                crate::rsm::facade::Deadline::after(Duration::from_secs(5)),
            ))
            .await
            .expect("membership");
        let v: serde_json::Value = serde_json::from_str(&st.body).expect("json");
        let got: Vec<(u64, Option<u32>)> = v["membership"]["members"]
            .as_array()
            .expect("members")
            .iter()
            .map(|m| {
                (
                    m["nodeId"].as_u64().expect("id"),
                    m["kinds"].as_u64().map(|k| k as u32),
                )
            })
            .collect();
        if got == want {
            break;
        }
        assert!(
            Instant::now() < end,
            "the leader heard {got:?}, never {want:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(1_000)).await;
}

/// D20 (a), (b): the version rises to what EVERY member reads, by itself, and
/// only then. A member that says nothing (a node older than this build) counts
/// as the baseline, and so does one that reads 3; once the last member reads
/// 4, the leader raises the version to 4 and every node applies it.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn the_cluster_version_waits_for_the_last_member_and_then_rises_by_itself() {
    let _one = ONE_AT_A_TIME.lock().await;
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("rise")).collect();
    let ports = free_ports(3);
    let reads = [Some(4), Some(4), None];
    let mut opening = Vec::new();
    for (i, d) in dirs.iter().enumerate() {
        let (d, c, k) = (d.clone(), cluster_config(&ports, i as u64 + 1), reads[i]);
        opening.push(tokio::spawn(async move { open(&d, c, k).await }));
    }
    let mut nodes: Vec<Option<Arc<RaftFacade>>> = Vec::new();
    for o in opening {
        nodes.push(Some(o.await.expect("open")));
    }

    // Node 3 says nothing: the baseline holds.
    heard(&nodes, &[(1, Some(4)), (2, Some(4)), (3, None)]).await;
    assert_eq!(versions(&nodes), vec![3, 3, 3]);

    // Node 3 restarts reading 3: still the baseline.
    close(nodes[2].take().unwrap()).await;
    nodes[2] = Some(open(&dirs[2], cluster_config(&ports, 3), Some(3)).await);
    heard(&nodes, &[(1, Some(4)), (2, Some(4)), (3, Some(3))]).await;
    assert_eq!(versions(&nodes), vec![3, 3, 3]);

    // Node 3 restarts reading 4: the leader raises the version to 4, and
    // /health shows it on every node.
    close(nodes[2].take().unwrap()).await;
    nodes[2] = Some(open(&dirs[2], cluster_config(&ports, 3), Some(4)).await);
    until_version(&nodes, 4).await;
    for n in nodes.iter().flatten() {
        let h = n.health();
        assert_eq!((h.cluster_version, h.kinds), (Some(4), Some(4)));
        assert!(
            h.to_json().contains("\"clusterVersion\":4"),
            "{}",
            h.to_json()
        );
    }

    for n in nodes.into_iter().flatten() {
        close(n).await;
    }
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// D20 (c): the version never goes down. A cluster at 4 keeps it when its
/// leader stops — the next leader has heard nobody yet, and a member it does
/// not hear from holds any raise back, so it proposes nothing — and when the
/// old leader comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn the_cluster_version_never_falls() {
    let _one = ONE_AT_A_TIME.lock().await;
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("never-falls")).collect();
    let ports = free_ports(3);
    let mut opening = Vec::new();
    for (i, d) in dirs.iter().enumerate() {
        let (d, c) = (d.clone(), cluster_config(&ports, i as u64 + 1));
        opening.push(tokio::spawn(async move { open(&d, c, Some(4)).await }));
    }
    let mut nodes: Vec<Option<Arc<RaftFacade>>> = Vec::new();
    for o in opening {
        nodes.push(Some(o.await.expect("open")));
    }
    until_version(&nodes, 4).await;

    // The leader stops: the other two elect one, still at 4.
    let l = leader(&nodes).await;
    close(nodes[l].take().unwrap()).await;
    let l2 = leader(&nodes).await;
    assert_ne!(l2, l);
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert_eq!(versions(&nodes), vec![4, 4]);

    // It comes back: every node still at 4, also after many comparisons.
    nodes[l] = Some(open(&dirs[l], cluster_config(&ports, l as u64 + 1), Some(4)).await);
    leader(&nodes).await;
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert_eq!(versions(&nodes), vec![4, 4, 4]);

    for n in nodes.into_iter().flatten() {
        close(n).await;
    }
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// One KV call on one node, as the HTTP layer makes it.
async fn kv(
    n: &RaftFacade,
    ops: serde_json::Value,
) -> Result<Vec<serde_json::Value>, crate::rsm::facade::KvFailure> {
    n.kv(
        crate::rsm::facade::ReqCtx::new(
            crate::config::DEFAULT_TENANT,
            crate::rsm::facade::Deadline::after(Duration::from_secs(10)),
        ),
        crate::rsm::facade::KvReq {
            ops: ops.as_array().expect("an op array").clone(),
        },
    )
    .await
    .map(|o| o.results)
}

/// D20 for a forwarded command shape. The KV `check` op is catalogue version
/// 5: a follower's prepared call carries it to the leader, and a member on
/// the release before cannot decode it. So while one member reads 4 every
/// node refuses a check — retryable, nothing ran, the reason named — and
/// everything older keeps working; once the last member reads 5 the leader
/// raises the version and every node serves the op, a follower by sending the
/// new shape to the leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_check_waits_for_cluster_version_5_and_is_then_served_by_every_node() {
    use crate::rsm::facade::KvFailure;
    use serde_json::json;

    let _one = ONE_AT_A_TIME.lock().await;
    let dirs: Vec<PathBuf> = (0..3).map(|_| scratch("check")).collect();
    let ports = free_ports(3);
    let reads = [Some(5), Some(5), Some(4)];
    let mut opening = Vec::new();
    for (i, d) in dirs.iter().enumerate() {
        let (d, c, k) = (d.clone(), cluster_config(&ports, i as u64 + 1), reads[i]);
        opening.push(tokio::spawn(async move { open(&d, c, k).await }));
    }
    let mut nodes: Vec<Option<Arc<RaftFacade>>> = Vec::new();
    for o in opening {
        nodes.push(Some(o.await.expect("open")));
    }
    until_version(&nodes, 4).await;
    heard(&nodes, &[(1, Some(5)), (2, Some(5)), (3, Some(4))]).await;
    assert_eq!(versions(&nodes), vec![4, 4, 4], "held by the member at 4");

    // Everything a version-4 cluster does still works, from any node.
    let put = kv(
        nodes[0].as_ref().unwrap(),
        json!([{"op":"putIfAbsent","ns":"d20","key":"lock","value":{"owner":"a"},"forever":true}]),
    )
    .await
    .expect("a put at version 4")
    .remove(0);
    assert_eq!(put["applied"], true, "{put}");
    let token = put["version"].as_u64().expect("a version");

    // A check is refused by every node, as "not yet".
    let check = json!([{"op":"check","ns":"d20","key":"lock","expect":token}]);
    for (i, n) in nodes.iter().flatten().enumerate() {
        match kv(n, check.clone()).await {
            Err(KvFailure::NotYet { reason, detail }) => {
                assert_eq!(reason, "kv_check_needs_cluster_version_5", "node {}", i + 1);
                assert!(detail.contains("this cluster is at 4"), "{detail}");
            }
            other => panic!("node {}: a check below version 5 answered {other:?}", i + 1),
        }
    }

    // The last member is upgraded: the version rises, the op is served.
    close(nodes[2].take().unwrap()).await;
    nodes[2] = Some(open(&dirs[2], cluster_config(&ports, 3), Some(5)).await);
    until_version(&nodes, 5).await;
    let l = leader(&nodes).await;
    for (i, n) in nodes.iter().flatten().enumerate() {
        let r = kv(n, check.clone())
            .await
            .unwrap_or_else(|e| panic!("node {} at version 5: {e:?}", i + 1))
            .remove(0);
        assert_eq!(r["applied"], true, "node {}: {r}", i + 1);
        assert_eq!(r["version"].as_u64(), Some(token));
    }
    // A follower's guarded call: the gate is judged by the leader's planner.
    let f = (0..3).find(|i| *i != l).expect("a follower");
    let stale = kv(
        nodes[f].as_ref().unwrap(),
        json!([
            {"op":"check","ns":"d20","key":"lock","expect":token + 1,"required":true},
            {"op":"put","ns":"d20","key":"work","value":1,"forever":true},
        ]),
    )
    .await;
    assert!(
        matches!(stale, Err(KvFailure::Precondition { .. })),
        "{stale:?}"
    );
    let guarded = kv(
        nodes[f].as_ref().unwrap(),
        json!([
            {"op":"check","ns":"d20","key":"lock","expect":token,"required":true},
            {"op":"put","ns":"d20","key":"work","value":2,"forever":true},
            {"op":"get","ns":"d20","key":"work"},
        ]),
    )
    .await
    .expect("a guarded call through a follower");
    assert_eq!(guarded[0]["applied"], true, "{guarded:?}");
    assert_eq!(guarded[2]["value"], 2, "{guarded:?}");

    for n in nodes.into_iter().flatten() {
        close(n).await;
    }
    for d in dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}
