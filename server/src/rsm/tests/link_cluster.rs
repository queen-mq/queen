//! The cluster link between whole nodes: a standby CLUSTER follows a source
//! CLUSTER over the source's Raft RPC port.
//!
//! [`link`](super::link) proves the replay itself, in one process and with
//! the test as the link. Here nothing is the test's: three source nodes and
//! three standby nodes run their facades, batchers and followers, the standby
//! reads the source over HTTP with the link's token, and clients go through
//! the facade's calls on whichever node they reach.
//!
//! - [`a_standby_cluster_follows_its_source_and_is_promoted`]: the standby
//!   holds the source's state, refuses its own clients' writes with the
//!   standby error on every node while it serves their reads, and keeps
//!   following across a leader change of the source, one of its own, and the
//!   stop of the source node it reads. Every node of the source keeps its log
//!   for the standby, under the one name the standby cluster has, and still
//!   knows it after a restart. After a promotion asked of one of its
//!   FOLLOWERS the standby serves the messages the source's consumers had not
//!   acknowledged — not the ones they had, and not the partition one of them
//!   still holds a lease on.
//! - [`a_standby_plans_nothing_its_clients_sent_while_it_had_no_leader`]: the
//!   standby's leader stops while the standby's own clients keep sending and
//!   the source keeps writing. The node that wins the election plans nothing
//!   of what reached it meanwhile: every entry of the standby's log is still
//!   the link's, it goes on following, and once promoted it serves the
//!   source's messages and none of its own clients'.
//! - [`a_standby_is_seeded_from_a_source_with_a_history`]: a source that
//!   purged the start of its log seeds the standby's first node with its
//!   snapshot; the other two join it; a standby it cannot serve holds none of
//!   its log.
//! - [`a_source_keeps_its_log_for_the_seed_it_sent`]: a seed leaves the
//!   standby's first hold on the node that sent it, so a source that writes
//!   and purges on while the standby's node starts still serves its first
//!   read.
//! - [`a_source_refuses_a_standby_without_its_token`]: the link's route
//!   answers a wrong token 401 and nothing else, and a source without a
//!   token serves no link.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::rsm::apply::state_digest;
use crate::rsm::batcher::BatcherConfig;
use crate::rsm::effect::Effect;
use crate::rsm::entry::Entry;
use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{
    ApiReq, Deadline, PopReq, PushReq, ReqCtx, Rsm, RsmBuildCtx, RsmError,
};
use crate::rsm::link::driver::{http_fetch, LinkConfig, LinkSetup};
use crate::rsm::link::wire::{full_entry, Answer, Request};
use crate::rsm::replicator::raft::{ClusterConfig, RaftOpts};
use crate::rsm::replicator::Replicator;
use crate::rsm::store::Store;

use super::link::{comparable, flags_without_the_link, request_rows};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// One cluster test at a time: each runs six full nodes.
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    ONE_AT_A_TIME.lock().await
}

/// `QUEEN_TEST_LOG=<filter>` prints the nodes' logs.
fn log_init() {
    #[cfg(feature = "server")]
    if let Ok(f) = std::env::var("QUEEN_TEST_LOG") {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(f))
            .with_test_writer()
            .try_init();
    }
}

const TOKEN: &str = "the-links-own-token";

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-linkcluster-{tag}-{}-{}",
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

fn raft_addrs(ports: &[u16]) -> Vec<String> {
    ports.iter().map(|p| format!("127.0.0.1:{p}")).collect()
}

/// Node `id` of the cluster on `ports`; `link_token` is what a standby must
/// present to read it.
fn cluster_config(ports: &[u16], id: u64, link_token: Option<&str>) -> ClusterConfig {
    let peers = ports
        .iter()
        .enumerate()
        .map(|(i, p)| format!("{}=127.0.0.1:{p}/127.0.0.1:1", i + 1))
        .collect::<Vec<_>>()
        .join(",");
    let mut c = ClusterConfig::parse(
        id,
        &peers,
        Some(&format!("127.0.0.1:{}", ports[id as usize - 1])),
        None,
    )
    .expect("cluster config");
    c.link_token = link_token.map(str::to_string);
    c
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
        kinds: Some(crate::rsm::effect::SUPPORTED_KINDS_VERSION),
        link_hold: Duration::from_secs(3600),
        // The machine's own disk is not the tests' subject.
        link_hold_disk_pct: 100.0,
    }
}

/// A whole node: its facade, batcher and follower, as the binary runs them
/// (the leader's own steps included), with its cluster and its part in the
/// link given here instead of by the environment.
fn open_node(dir: &Path, cluster: ClusterConfig, link: LinkSetup) -> RaftFacade {
    open_node_with(dir, cluster, test_opts(), link)
}

fn open_node_with(
    dir: &Path,
    cluster: ClusterConfig,
    opts: RaftOpts,
    link: LinkSetup,
) -> RaftFacade {
    let ctx = RsmBuildCtx {
        data_dir: dir.display().to_string(),
        notifier: crate::notify::Notifier::new(false),
        disk_high_pct: 99.0,
        disk_low_pct: 98.0,
    };
    let batcher = BatcherConfig {
        maintenance_every_ms: 0,
        ..BatcherConfig::from_env()
    };
    let id = cluster.node_id;
    RaftFacade::open_link_node_for_test(&ctx, batcher, Some((cluster, opts)), link)
        .unwrap_or_else(|e| panic!("open node {id}: {e}"))
}

type Nodes = Vec<Arc<RaftFacade>>;

/// Open every node of a cluster at once (each waits for a leader).
async fn open_cluster(
    dirs: &[PathBuf],
    ports: &[u16],
    link_token: Option<&str>,
    link: impl Fn() -> LinkSetup,
) -> Nodes {
    let mut opening = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        let (dir, cluster, link) = (
            dir.clone(),
            cluster_config(ports, i as u64 + 1, link_token),
            link(),
        );
        opening.push(tokio::task::spawn_blocking(move || {
            open_node(&dir, cluster, link)
        }));
    }
    let mut nodes = Vec::new();
    for h in opening {
        nodes.push(Arc::new(h.await.expect("open task")));
    }
    nodes
}

/// The index of the one node that leads, waiting for it.
async fn leader_of(nodes: &Nodes) -> usize {
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        let leaders: Vec<usize> = nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.health().role == "leader")
            .map(|(i, _)| i)
            .collect();
        if leaders.len() == 1 {
            return leaders[0];
        }
        assert!(Instant::now() < end, "no single ready leader within 30 s");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Hand the cluster's leadership to another node, and wait until it leads.
async fn move_leadership(nodes: &Nodes) -> usize {
    let from = leader_of(nodes).await;
    let to = (from + 1) % nodes.len();
    let repl = nodes[from].repl_for_test();
    repl.transfer_leadership(Some(to as u64 + 1), Instant::now() + Duration::from_secs(10))
        .await
        .expect("transfer leadership");
    drop(repl);
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        if leader_of(nodes).await == to {
            return to;
        }
        assert!(Instant::now() < end, "leadership did not move to node {to}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn close(nodes: Nodes) {
    for node in nodes {
        let end = Instant::now() + Duration::from_secs(10);
        let mut node = node;
        loop {
            match Arc::try_unwrap(node) {
                Ok(f) => break f.shutdown().await,
                Err(still) => {
                    assert!(Instant::now() < end, "a facade is still shared at close");
                    node = still;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }
}

fn ctx() -> ReqCtx {
    ReqCtx::new(
        crate::config::DEFAULT_TENANT,
        Deadline::after(Duration::from_secs(10)),
    )
}

/// Push message `n` to `orders`, on one of four partitions.
async fn push(node: &RaftFacade, n: u64) -> Result<(), RsmError> {
    let raw = format!(
        r#"{{"items":[{{"queue":"orders","partition":"p{}","payload":{{"n":{n}}},"transactionId":"t{n}"}}]}}"#,
        n % 4
    );
    let out = node
        .push(
            ctx(),
            PushReq {
                raw: raw.into_bytes(),
            },
        )
        .await?;
    let items: Value = serde_json::from_str(&out.body).expect("push body");
    assert_eq!(items[0]["status"], "queued", "{}", out.body);
    Ok(())
}

/// One push to `orders` by a client that gives up after `budget`: message
/// `n`, on one of the same four partitions. The answer's body, if it was
/// taken.
async fn push_within(node: &RaftFacade, n: u64, budget: Duration) -> Result<String, RsmError> {
    let raw = format!(
        r#"{{"items":[{{"queue":"orders","partition":"p{}","payload":{{"n":{n}}},"transactionId":"t{n}"}}]}}"#,
        n % 4
    );
    let ctx = ReqCtx::new(crate::config::DEFAULT_TENANT, Deadline::after(budget));
    let out = node
        .push(
            ctx,
            PushReq {
                raw: raw.into_bytes(),
            },
        )
        .await?;
    Ok(out.body)
}

/// One pop of `orders` in queue mode. The messages' `n`, acknowledged or not.
async fn pop(node: &RaftFacade, auto_ack: bool) -> Result<(Vec<u64>, Value), RsmError> {
    let out = node
        .pop_wildcard(
            ctx(),
            PopReq {
                queue: "orders".into(),
                group: None,
                batch: 100,
                auto_ack,
                wait: false,
                timeout_ms: 2000,
                options: Default::default(),
            },
        )
        .await?;
    if out.empty {
        return Ok((Vec::new(), Value::Null));
    }
    let body: Value = serde_json::from_str(&out.body).expect("pop body");
    let ns = body["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .map(|m| m["data"]["n"].as_u64().expect("n"))
        .collect();
    Ok((ns, body))
}

/// Acknowledge everything `popped` (a leased pop's body) claimed.
async fn ack(node: &RaftFacade, popped: &Value) {
    let (pid, lease) = (
        popped["partitionId"].as_str().expect("partitionId"),
        popped["leaseId"].as_str().expect("leaseId"),
    );
    let acks: Vec<String> = popped["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .map(|m| {
            format!(
                r#"{{"transactionId":"{}","partitionId":"{pid}","status":"completed","leaseId":"{lease}"}}"#,
                m["transactionId"].as_str().expect("transactionId")
            )
        })
        .collect();
    let raw = format!(
        r#"{{"consumerGroup":"__QUEUE_MODE__","acknowledgments":[{}]}}"#,
        acks.join(",")
    );
    let out = node
        .ack(
            ctx(),
            crate::rsm::facade::AckReq {
                queue: Some("orders".into()),
                group: "__QUEUE_MODE__".into(),
                raw: raw.into_bytes(),
            },
        )
        .await
        .expect("ack");
    let results: Value = serde_json::from_str(&out.body).expect("ack body");
    for r in results.as_array().expect("ack array") {
        assert_eq!(r["success"], true, "{r}");
    }
}

async fn api(node: &RaftFacade, method: &str, path: &str) -> (u16, Value) {
    let out = node
        .api(
            ctx(),
            ApiReq {
                method: method.into(),
                path: path.into(),
                query: None,
                body: Vec::new(),
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{method} {path}: {e}"));
    let body = serde_json::from_str(&out.body).unwrap_or(Value::Null);
    (out.status, body)
}

async fn link_status(node: &RaftFacade) -> Value {
    let (status, body) = api(node, "GET", "/api/v1/system/link").await;
    assert_eq!(status, 200, "{body}");
    body
}

/// The standbys `node` keeps its log for, and how far each has got.
fn readers(node: &RaftFacade) -> Vec<(String, u64)> {
    node.repl_for_test()
        .link_source()
        .map(|s| s.readers())
        .unwrap_or_default()
        .into_iter()
        .map(|r| (r.name, r.after))
        .collect()
}

/// What two nodes must agree on (see [`comparable`]).
fn held(node: &RaftFacade) -> impl PartialEq + std::fmt::Debug {
    let store = node.store_for_test();
    let digest = store
        .read(|r| Ok(state_digest(r).expect("digest")))
        .expect("read");
    (comparable(&digest), flags_without_the_link(&store))
}

/// Whether `standby` records every request id `source` does, and at most the
/// two rows of its own standby entry besides (the source's expiry step
/// retires them with the source's own, a window after that entry's stamp).
fn requests_follow(source: &RaftFacade, standby: &RaftFacade) -> bool {
    let (rs, rb) = (
        request_rows(&source.store_for_test(), &[]),
        request_rows(&standby.store_for_test(), &[]),
    );
    rs.iter().all(|row| rb.contains(row)) && rb.len() <= rs.len() + 2
}

/// Every application entry `node` has applied, with its index, as its own
/// log holds them (read the way a standby of this node would read it).
fn applied_entries(node: &RaftFacade) -> Vec<(u64, Entry)> {
    let log = node.repl_for_test().link_source().expect("a cluster node");
    let (mut after, mut after_term, mut out) = (0, 0, Vec::new());
    loop {
        match log.read(after, after_term, 8 << 20).expect("read the log") {
            Answer::Entries { entries, .. } if entries.is_empty() => return out,
            Answer::Entries { entries, .. } => {
                for e in entries {
                    (after, after_term) = (e.index, e.term);
                    out.push((e.index, full_entry(&e.stored).expect("rebuild the entry")));
                }
            }
            other => panic!("a node did not serve its own log: {other:?}"),
        }
    }
}

/// Wait until the standby holds what the source holds: its leader's follower
/// has read everything the source's leader has applied, and the two leaders'
/// states are equal. The source logs entries of its own now and then (an
/// expiry step), so "equal" is looked for, not assumed at one instant.
async fn converge(source: &Nodes, standby: &Nodes, when: &str) {
    let end = Instant::now() + Duration::from_secs(60);
    loop {
        let (s, b) = (leader_of(source).await, leader_of(standby).await);
        let target = source[s].health().applied;
        let status = link_status(&standby[b]).await;
        let f = &status["follower"];
        let read = f["scanned"].as_u64().unwrap_or(0) >= target
            && f["lagEntries"].as_u64() == Some(0)
            && f["state"] == "following";
        if read
            && held(&standby[b]) == held(&source[s])
            && requests_follow(&source[s], &standby[b])
        {
            // And every node of the standby holds what its leader holds.
            let same = |n: &Arc<RaftFacade>| {
                held(n) == held(&standby[b])
                    && request_rows(&n.store_for_test(), &[])
                        == request_rows(&standby[b].store_for_test(), &[])
            };
            if standby.iter().all(same) {
                return;
            }
        }
        if Instant::now() >= end {
            // Which counters differ: the keyspace every effect touches.
            let (cs, cb) = (
                super::link::counter_rows(&source[s].store_for_test()),
                super::link::counter_rows(&standby[b].store_for_test()),
            );
            let only = |x: &[(Vec<u8>, Vec<u8>)], y: &[(Vec<u8>, Vec<u8>)]| -> Vec<String> {
                x.iter()
                    .filter(|row| !y.contains(row))
                    .map(|(k, v)| format!("{k:?}={v:?}"))
                    .collect()
            };
            panic!(
                "{when}: the standby did not converge on its source within 60 s: {status}\n\
                 counters only on the source: {:#?}\ncounters only on the standby: {:#?}\n\
                 source: {:?}\nstandby: {:?}",
                only(&cs, &cb),
                only(&cb, &cs),
                held(&source[s]),
                held(&standby[b]),
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_standby_cluster_follows_its_source_and_is_promoted() {
    let _one = serial().await;
    log_init();

    // The source: three nodes that take a standby's reads with the token.
    let src_dirs: Vec<PathBuf> = (0..3).map(|_| scratch("source")).collect();
    let src_ports = free_ports(3);
    let source = open_cluster(&src_dirs, &src_ports, Some(TOKEN), LinkSetup::default).await;

    // The standby: three empty nodes told to be one, each knowing every node
    // of the source.
    let sb_dirs: Vec<PathBuf> = (0..3).map(|_| scratch("standby")).collect();
    let sb_ports = free_ports(3);
    let sources = raft_addrs(&src_ports);
    let setup = || LinkSetup {
        source: Some(LinkConfig {
            sources: sources.clone(),
            token: Some(TOKEN.to_string()),
            name: None,
        }),
        standby: true,
        seed: None,
        fetch: None,
    };
    let standby = open_cluster(&sb_dirs, &sb_ports, None, setup).await;

    // Clients of the source, on every node: 30 messages, then a consumer
    // that takes one partition's backlog and acknowledges it.
    for n in 0..30 {
        push(&source[n as usize % 3], n).await.expect("push");
    }
    let s = leader_of(&source).await;
    let (acked, popped) = pop(&source[s], false).await.expect("pop");
    assert!(!acked.is_empty(), "the consumer got a partition's backlog");
    ack(&source[s], &popped).await;
    converge(&source, &standby, "after the first messages").await;

    // A standby takes no write, on any node, and says why; its reads work.
    for node in &standby {
        match push(node, 9_000).await {
            Err(RsmError::Standby) => {}
            other => panic!("a standby took a client's write: {other:?}"),
        }
        match pop(node, true).await {
            Err(RsmError::Standby) => {}
            other => panic!("a standby served a pop: {other:?}"),
        }
        let (status, queues) = api(node, "GET", "/api/v1/resources/queues").await;
        assert_eq!(status, 200);
        assert!(
            queues.to_string().contains("orders"),
            "the standby lists the source's queue: {queues}"
        );
    }
    let status = link_status(&standby[leader_of(&standby).await]).await;
    assert_eq!(status["role"], "standby", "{status}");
    assert_eq!(status["leader"], true, "{status}");
    assert!(status["position"]["index"].as_u64() > Some(0), "{status}");
    // The standby drew an id when it became one: the same on each of its
    // nodes, and what the source knows it by.
    let id = status["id"].as_str().expect("the standby's id").to_string();
    assert_eq!(id.len(), 8, "{status}");
    let name = format!("standby-{id}");
    for node in &standby {
        let status = link_status(node).await;
        assert_eq!(status["name"], name.as_str(), "{status}");
    }
    // Every node of the source keeps its log for the standby: the one that
    // is read, and the two that are only told where the standby is.
    fn held_for(node: &RaftFacade) -> Vec<String> {
        readers(node).into_iter().map(|(name, _)| name).collect()
    }
    wait_for("every node of the source holds its log for the standby", || {
        source.iter().all(|node| held_for(node) == [name.clone()])
    })
    .await;
    let status = link_status(&source[0]).await;
    assert_eq!(status["readers"][0]["name"], name.as_str(), "{status}");
    assert_eq!(status["holding"], true, "{status}");

    // The source's leadership moves; its clients go on, and so does the link.
    move_leadership(&source).await;
    for n in 30..50 {
        push(&source[n as usize % 3], n).await.expect("push");
    }
    converge(&source, &standby, "after the source's leader change").await;

    // The standby's leadership moves: the new leader's follower goes on from
    // the position the cluster holds, under the same name — the hold on the
    // source's log is the cluster's, not a node's.
    move_leadership(&standby).await;
    for n in 50..60 {
        push(&source[n as usize % 3], n).await.expect("push");
    }
    converge(&source, &standby, "after the standby's leader change").await;
    for node in &source {
        assert_eq!(held_for(node), [name.clone()], "one standby, one hold");
    }

    // The source node the standby reads stops: the standby reads another,
    // which kept the same entries for it. The node wrote its holds down, so
    // it comes back knowing them.
    let b = leader_of(&standby).await;
    let read_from = link_status(&standby[b]).await["follower"]["source"]
        .as_str()
        .expect("the node the standby reads")
        .to_string();
    let r = sources
        .iter()
        .position(|s| *s == read_from)
        .expect("one of the source's nodes");
    let mut source = source;
    let stopped = source.remove(r);
    close(vec![stopped]).await;
    let written = std::fs::read_to_string(src_dirs[r].join("raft").join("link_holds.json"))
        .expect("the stopped node's holds");
    assert!(written.contains(&name), "{written}");
    leader_of(&source).await;
    for n in 60..65 {
        push(&source[n as usize % 2], n).await.expect("push");
    }
    converge(&source, &standby, "while a node of the source is down").await;
    let (d, c) = (
        src_dirs[r].clone(),
        cluster_config(&src_ports, r as u64 + 1, Some(TOKEN)),
    );
    let back = tokio::task::spawn_blocking(move || open_node(&d, c, LinkSetup::default()))
        .await
        .expect("reopen the source node");
    assert_eq!(held_for(&back), [name.clone()], "known from its file");
    source.insert(r, Arc::new(back));
    for n in 65..70 {
        push(&source[n as usize % 3], n).await.expect("push");
    }
    converge(&source, &standby, "after the source node came back").await;

    // A consumer of the source holds a partition's lease when the source is
    // lost. The lease is state like any other: the promoted cluster keeps
    // that partition for it until the lease runs out, as a new leader of the
    // source would have.
    let s = leader_of(&source).await;
    let (leased, leased_pop) = pop(&source[s], false).await.expect("pop");
    assert!(!leased.is_empty(), "the consumer got a partition's backlog");
    let leased_partition = leased[0] % 4;
    assert!(leased.iter().all(|n| n % 4 == leased_partition), "{leased:?}");
    converge(&source, &standby, "after the lease").await;

    // Promotion, asked of a follower of the standby.
    let b = leader_of(&standby).await;
    let follower = &standby[(b + 1) % 3];
    let (status, promoted) = api(follower, "POST", "/api/v1/system/link/promote").await;
    assert_eq!(status, 200, "{promoted}");
    assert_eq!(promoted["role"], "promoted", "{promoted}");
    // Again: nothing to do, and no error.
    let (status, again) = api(&standby[b], "POST", "/api/v1/system/link/promote").await;
    assert_eq!((status, again["role"].as_str()), (200, Some("promoted")));

    // An ordinary cluster now: it takes writes on every node, and serves the
    // source's messages — all but the ones the source's consumer acknowledged,
    // and the partition its other consumer still holds.
    for (i, node) in standby.iter().enumerate() {
        push(node, 70 + i as u64).await.expect("push after the promotion");
    }
    let want: Vec<u64> = (0..73)
        .filter(|n| !acked.contains(n) && n % 4 != leased_partition)
        .collect();
    let mut served: Vec<u64> = Vec::new();
    let end = Instant::now() + Duration::from_secs(30);
    while served.len() < want.len() {
        let (ns, _) = pop(&standby[b], true).await.expect("pop after the promotion");
        served.extend(ns);
        assert!(
            Instant::now() < end,
            "the promoted cluster served {} of {} messages",
            served.len(),
            want.len()
        );
    }
    served.sort_unstable();
    assert_eq!(
        served, want,
        "what the source's consumers had neither acknowledged nor leased"
    );
    let (more, _) = pop(&standby[b], true).await.expect("pop");
    assert!(
        more.is_empty(),
        "nothing is served twice, and a lease taken on the source holds here: {more:?}"
    );
    // The consumer that took that lease on the source finishes its work
    // here: the lease it was given there is good on the promoted cluster.
    // What it acknowledges is never delivered again, and the partition is
    // free for what was pushed to it since.
    ack(&standby[b], &leased_pop).await;
    let since: Vec<u64> = (70..73).filter(|n| n % 4 == leased_partition).collect();
    let (after_ack, _) = pop(&standby[b], true).await.expect("pop");
    assert_eq!(
        after_ack, since,
        "after an ack with the source's lease: what was pushed since, and nothing it covered"
    );

    // A promoted cluster stays promoted with its configuration unchanged, and
    // the source never heard of any of it.
    let status = link_status(&standby[b]).await;
    assert_eq!(status["role"], "promoted", "{status}");
    assert_eq!(link_status(&source[0]).await["role"], "primary");
    for n in 100..103 {
        push(&source[0], n).await.expect("the source still serves");
    }

    close(standby).await;
    close(source).await;
    for d in src_dirs.into_iter().chain(sb_dirs) {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A standby's leader stops while the standby's own clients keep sending (a
/// standby answers them `standby` and they retry, as the docs tell them to)
/// and the source keeps writing.
///
/// A command that reaches a node while no leader is known waits in its
/// driver for the election's outcome, because the node may win and plan it.
/// A follower forwards to the new leader the moment raft names it, which is
/// before that node has applied the first entry of its term (I13) and so
/// before its driver knows what it leads: the commands of the other node's
/// clients wait there. The node that wins a STANDBY's election plans none of
/// them. Every entry of the standby's log is still the link's, the standby
/// goes on following, and once promoted it serves the source's messages,
/// each once, and not one of its own clients'.
///
/// It used to plan what had waited, in the cycle its first link cycle
/// launched early: an entry of its own, stamped with its own clock, ahead of
/// the source entries it had still to replay. Every node refused the next of
/// those (I5: its stamp was behind) and stopped for good, long before anyone
/// asked for a promotion (Jepsen `fo-crash-lz-kill`, 2026-10-08).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_standby_plans_nothing_its_clients_sent_while_it_had_no_leader() {
    let _one = serial().await;
    log_init();

    let src_dirs: Vec<PathBuf> = (0..3).map(|_| scratch("election-source")).collect();
    let src_ports = free_ports(3);
    let source = open_cluster(&src_dirs, &src_ports, Some(TOKEN), LinkSetup::default).await;
    let sb_dirs: Vec<PathBuf> = (0..3).map(|_| scratch("election-standby")).collect();
    let sb_ports = free_ports(3);
    let sources = raft_addrs(&src_ports);
    let setup = move || LinkSetup {
        source: Some(LinkConfig {
            sources: sources.clone(),
            token: Some(TOKEN.to_string()),
            name: None,
        }),
        standby: true,
        seed: None,
        fetch: None,
    };
    let mut standby = open_cluster(&sb_dirs, &sb_ports, None, &setup).await;
    for n in 0..20 {
        push(&source[n as usize % 3], n).await.expect("push");
    }
    converge(&source, &standby, "before the standby's leader stops").await;

    // The source's clients go on writing: what they write while the standby
    // has no leader is what its next leader replays first.
    let writing = Arc::new(AtomicBool::new(true));
    let writer = tokio::spawn({
        let (source, writing) = (source.clone(), writing.clone());
        async move {
            let mut n = 20;
            while writing.load(Ordering::Relaxed) {
                push(&source[n as usize % 3], n)
                    .await
                    .expect("push to the source");
                n += 1;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            n
        }
    });

    // The standby's own clients, on the two nodes that will be left: each
    // sends a write, gives up on it after 10 ms and sends the next, sooner
    // when it got no answer than when it was told `standby`.
    const CLIENTS: usize = 16;
    let b = leader_of(&standby).await;
    let stopped = standby.remove(b);
    let sending = Arc::new(AtomicBool::new(true));
    let refused = Arc::new(AtomicU64::new(0));
    let taken = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let mut clients = Vec::new();
    for (i, node) in standby.iter().enumerate() {
        for c in 0..CLIENTS {
            let (node, sending, refused, taken) = (
                node.clone(),
                sending.clone(),
                refused.clone(),
                taken.clone(),
            );
            clients.push(tokio::spawn(async move {
                let mut n = 1_000_000 * (1 + (i * CLIENTS + c) as u64);
                while sending.load(Ordering::Relaxed) {
                    let wait = match push_within(&node, n, Duration::from_millis(10)).await {
                        Ok(body) => {
                            taken.lock().expect("taken").push(body);
                            5
                        }
                        Err(RsmError::Standby) => {
                            refused.fetch_add(1, Ordering::Relaxed);
                            5
                        }
                        Err(_) => 1,
                    };
                    n += 1;
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                }
            }));
        }
    }
    let taken = move || taken.lock().expect("taken").clone();
    wait_for("the standby refuses its clients' writes", || {
        refused.load(Ordering::Relaxed) > 0
    })
    .await;

    // The standby's leader stops, and one of the other two wins.
    close(vec![stopped]).await;
    let w = leader_of(&standby).await;
    // It replays what the source wrote meanwhile, and its clients still send.
    let mark = source[leader_of(&source).await].health().applied;
    let end = Instant::now() + Duration::from_secs(60);
    loop {
        assert!(
            taken().is_empty(),
            "a standby took a client's write: {:?}",
            taken()
        );
        for node in &standby {
            assert_ne!(
                node.health().role,
                "stopped",
                "a node of the standby refused an entry of its log and stopped"
            );
        }
        let status = link_status(&standby[w]).await;
        if status["position"]["index"].as_u64() >= Some(mark) {
            break;
        }
        assert!(
            Instant::now() < end,
            "the standby's new leader did not replay up to source entry {mark}: {status}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    sending.store(false, Ordering::Relaxed);
    for client in clients {
        client.await.expect("a client of the standby");
    }
    writing.store(false, Ordering::Relaxed);
    let mut written = writer.await.expect("the source's clients");
    assert!(
        taken().is_empty(),
        "a standby took a client's write: {:?}",
        taken()
    );

    // The standby still follows: it holds what its source holds, and every
    // entry of its log is the link's — the entry that made it a standby, and
    // the source's entries, each with the position it brings.
    converge(&source, &standby, "after the standby's leader stopped").await;
    for node in &standby {
        for (index, entry) in applied_entries(node) {
            assert!(
                entry.effects.iter().any(
                    |e| matches!(e, Effect::FlagSet { key, .. } if crate::rsm::link::is_link_flag(key))
                ),
                "entry {index} of the standby's log is not the link's: {entry:?}"
            );
        }
    }

    // The node that stopped comes back and follows with the others.
    let (d, c, link) = (
        sb_dirs[b].clone(),
        cluster_config(&sb_ports, b as u64 + 1, None),
        setup(),
    );
    let back = tokio::task::spawn_blocking(move || open_node(&d, c, link))
        .await
        .expect("reopen the stopped node");
    standby.insert(b, Arc::new(back));
    for n in written..written + 10 {
        push(&source[n as usize % 3], n).await.expect("push");
    }
    written += 10;
    converge(&source, &standby, "after the stopped node came back").await;

    // Promoted, it serves what the source's clients wrote, each message once.
    // Nothing its own clients sent while it was a standby was kept for now.
    let b = leader_of(&standby).await;
    let (status, promoted) = api(&standby[b], "POST", "/api/v1/system/link/promote").await;
    assert_eq!((status, promoted["role"].as_str()), (200, Some("promoted")));
    let mut served: Vec<u64> = Vec::new();
    let end = Instant::now() + Duration::from_secs(30);
    while served.len() < written as usize {
        let (ns, _) = pop(&standby[b], true).await.expect("pop");
        served.extend(ns);
        assert!(
            Instant::now() < end,
            "the promoted cluster served {} of {written} messages",
            served.len()
        );
    }
    // And what is left, if anything, on each of the four partitions.
    for _ in 0..4 {
        let (more, _) = pop(&standby[b], true).await.expect("pop");
        served.extend(more);
    }
    served.sort_unstable();
    let own: Vec<u64> = served.iter().copied().filter(|n| *n >= written).collect();
    assert!(
        own.is_empty(),
        "the promoted cluster serves what its own clients sent while it was a standby: {own:?}"
    );
    assert_eq!(
        served,
        (0..written).collect::<Vec<u64>>(),
        "the source's messages, each once"
    );

    close(standby).await;
    close(source).await;
    for d in src_dirs.into_iter().chain(sb_dirs) {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// One read of the source node on `port` from the start of its log, with
/// `token`.
async fn read(token: Option<&str>, port: u16) -> Result<Answer, String> {
    let fetch = http_fetch(token.map(str::to_string));
    fetch(
        format!("127.0.0.1:{port}"),
        Request {
            after: 0,
            after_term: 0,
            max_bytes: 0,
            wait_ms: 0,
            reader: "a-test".to_string(),
            hold_only: false,
        },
    )
    .await
}

async fn wait_for(what: &str, mut ready: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(60);
    while !ready() {
        assert!(Instant::now() < end, "{what}: not within 60 s");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A membership change asked of `node`: the answer's status and body.
async fn change(
    node: &RaftFacade,
    change: crate::rsm::replicator::MembershipChange,
) -> (u16, Value) {
    let out = node
        .raft_change_membership(ctx(), change)
        .await
        .expect("membership change");
    (
        out.status,
        serde_json::from_str(&out.body).unwrap_or(Value::Null),
    )
}

/// Why the node stopped to restart, if it did (a received snapshot).
fn restart_requested(node: &RaftFacade) -> Option<String> {
    match &*node.repl_for_test() {
        crate::rsm::replicator::node::NodeReplicator::Raft(r) => r.restart_requested(),
        _ => None,
    }
}

/// A source that has run for a while no longer holds the start of its log, so
/// a standby cannot replay it from an empty state. The standby's first node
/// takes the source's snapshot instead ([`crate::rsm::link::seed`]): it boots
/// as the only voter of a new cluster, on the source's state, a standby at
/// the snapshot's position. The other two nodes are added to it as to any
/// cluster. From there it follows like any standby, across a restart of the
/// seeded node (which does not seed again), and is promoted.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_standby_is_seeded_from_a_source_with_a_history() {
    let _one = serial().await;
    log_init();
    use crate::rsm::replicator::MembershipChange;

    // The source: one node that purges its log as soon as it may.
    let src_dirs = vec![scratch("seed-source")];
    let src_ports = free_ports(1);
    let eager = RaftOpts {
        purge_hold: Duration::from_secs(2),
        log_keep: 0,
        purge_batch: 1,
        ..test_opts()
    };
    let (d, c) = (
        src_dirs[0].clone(),
        cluster_config(&src_ports, 1, Some(TOKEN)),
    );
    let source: Nodes = vec![Arc::new(
        tokio::task::spawn_blocking(move || open_node_with(&d, c, eager, LinkSetup::default()))
            .await
            .expect("open the source"),
    )];
    leader_of(&source).await;
    for n in 0..40 {
        push(&source[0], n).await.expect("push");
    }
    let (acked, popped) = pop(&source[0], false).await.expect("pop");
    assert!(!acked.is_empty());
    ack(&source[0], &popped).await;
    let purged = || match &*source[0].repl_for_test() {
        crate::rsm::replicator::node::NodeReplicator::Raft(r) => r.purged_index(),
        _ => 0,
    };
    wait_for("the source purges the start of its log", || purged() > 0).await;
    // An empty standby could not follow it — and what cannot be served holds
    // nothing: it must not stop the source's purge for as long as it asks.
    match read(Some(TOKEN), src_ports[0]).await.expect("read") {
        Answer::Purged { .. } => {}
        other => panic!("the start of a purged log was served: {other:?}"),
    }
    assert!(
        readers(&source[0]).is_empty(),
        "a hold below the purge point: {:?}",
        readers(&source[0])
    );

    // The standby: node 1 takes the seed, nodes 2 and 3 wait to be added.
    let sb_dirs: Vec<PathBuf> = (0..3).map(|_| scratch("seed-standby")).collect();
    let sb_ports = free_ports(3);
    let sources = raft_addrs(&src_ports);
    let setup = move || LinkSetup {
        source: Some(LinkConfig {
            sources: sources.clone(),
            token: Some(TOKEN.to_string()),
            name: Some("seeded-standby".to_string()),
        }),
        standby: false,
        seed: Some(1),
        fetch: None,
    };
    let mut standby = open_cluster(&sb_dirs, &sb_ports, None, &setup).await;
    wait_for("the seeded node leads its own cluster", || {
        standby[0].health().role == "leader"
    })
    .await;
    assert!(
        crate::rsm::link::seed::read(&sb_dirs[0]).expect("seed record").is_some(),
        "the seeded node keeps the seed's record"
    );
    assert!(
        crate::rsm::link::seed::read(&sb_dirs[1]).expect("seed record").is_none(),
        "only the node the seed names takes it"
    );

    // It is a standby, at the snapshot's position, and follows from there.
    for n in 40..60 {
        push(&source[0], n).await.expect("push");
    }
    converge(&source, &vec![standby[0].clone()], "the seeded node").await;
    let status = link_status(&standby[0]).await;
    assert_eq!(status["role"], "standby", "{status}");
    match push(&standby[0], 9_000).await {
        Err(RsmError::Standby) => {}
        other => panic!("a seeded standby took a write: {other:?}"),
    }

    // Nodes 2 and 3 join the seeded node's cluster: each receives its
    // snapshot, stops to load it, and comes back on it.
    for id in [2u64, 3] {
        let (status, body) = change(
            &standby[0],
            MembershipChange::AddLearner {
                node: id,
                raft: format!("127.0.0.1:{}", sb_ports[id as usize - 1]),
                http: "127.0.0.1:1".into(),
            },
        )
        .await;
        assert_eq!(status, 200, "add learner {id}: {body}");
    }
    for i in [1usize, 2] {
        wait_for("a joining node receives the seeded node's snapshot", || {
            restart_requested(&standby[i]).is_some()
        })
        .await;
        let stopped = standby.remove(i);
        close(vec![stopped]).await;
        let (d, c, link) = (
            sb_dirs[i].clone(),
            cluster_config(&sb_ports, i as u64 + 1, None),
            setup(),
        );
        let back = tokio::task::spawn_blocking(move || open_node(&d, c, link))
            .await
            .expect("reopen");
        standby.insert(i, Arc::new(back));
    }
    let end = Instant::now() + Duration::from_secs(60);
    loop {
        let (status, body) = change(
            &standby[0],
            MembershipChange::Promote {
                nodes: vec![2, 3],
                force: false,
            },
        )
        .await;
        if status == 200 {
            break;
        }
        assert!(Instant::now() < end, "the learners were never promoted: {body}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for n in 60..80 {
        push(&source[0], n).await.expect("push");
    }
    converge(&source, &standby, "the three nodes of the standby").await;

    // The seeded node restarts with its configuration unchanged: it does not
    // seed again, and the cluster is the standby it was.
    let stopped = standby.remove(0);
    close(vec![stopped]).await;
    let (d, c, link) = (
        sb_dirs[0].clone(),
        cluster_config(&sb_ports, 1, None),
        setup(),
    );
    let back = tokio::task::spawn_blocking(move || open_node(&d, c, link))
        .await
        .expect("reopen the seeded node");
    standby.insert(0, Arc::new(back));
    for n in 80..90 {
        push(&source[0], n).await.expect("push");
    }
    converge(&source, &standby, "after the seeded node's restart").await;

    // Promoted, it serves what the source's consumers had not acknowledged.
    let b = leader_of(&standby).await;
    let (status, promoted) = api(&standby[b], "POST", "/api/v1/system/link/promote").await;
    assert_eq!((status, promoted["role"].as_str()), (200, Some("promoted")));
    let mut served: Vec<u64> = Vec::new();
    let end = Instant::now() + Duration::from_secs(30);
    while served.len() < 90 - acked.len() {
        let (ns, _) = pop(&standby[b], true).await.expect("pop");
        served.extend(ns);
        assert!(
            Instant::now() < end,
            "the promoted cluster served {} of {} messages",
            served.len(),
            90 - acked.len()
        );
    }
    served.sort_unstable();
    let want: Vec<u64> = (0..90).filter(|n| !acked.contains(n)).collect();
    assert_eq!(served, want);

    close(standby).await;
    close(source).await;
    for d in src_dirs.into_iter().chain(sb_dirs) {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A seed is a snapshot, and the standby reads its source for the first time
/// long after it was made: once its node has staged the snapshot, started on
/// it, elected itself and logged its standby entry. A source that keeps
/// writing and purging meanwhile must not purge the seed's place away. So the
/// node that sends the snapshot holds its log for the standby, under the name
/// the standby reads with — here the one made of the id it drew before it
/// asked: from before it copies its store, which takes as long as the store is
/// large, and from the copy's checkpoint once it knows it. The standby's first
/// read moves that hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_source_keeps_its_log_for_the_seed_it_sent() {
    let _one = serial().await;
    log_init();
    use futures_util::StreamExt;

    // The source: one node that purges its log as soon as it may.
    let src_dirs = vec![scratch("held-source")];
    let src_ports = free_ports(1);
    let eager = RaftOpts {
        purge_hold: Duration::from_secs(2),
        log_keep: 0,
        purge_batch: 1,
        ..test_opts()
    };
    let (d, c) = (
        src_dirs[0].clone(),
        cluster_config(&src_ports, 1, Some(TOKEN)),
    );
    let source: Nodes = vec![Arc::new(
        tokio::task::spawn_blocking(move || open_node_with(&d, c, eager, LinkSetup::default()))
            .await
            .expect("open the source"),
    )];
    leader_of(&source).await;
    for n in 0..40 {
        push(&source[0], n).await.expect("push");
    }
    let purged = || match &*source[0].repl_for_test() {
        crate::rsm::replicator::node::NodeReplicator::Raft(r) => r.purged_index(),
        _ => 0,
    };
    wait_for("the source purges the start of its log", || purged() > 0).await;

    // A snapshot asked for by nobody is kept for nobody.
    let sources = raft_addrs(&src_ports);
    let mut unnamed = crate::rsm::link::seed::http_snapshot(&sources[0], Some(TOKEN), "")
        .await
        .expect("a snapshot without a reader");
    while let Some(piece) = unnamed.next().await {
        piece.expect("a piece of the snapshot");
    }
    assert!(
        readers(&source[0]).is_empty(),
        "a hold for a snapshot nobody reads after: {:?}",
        readers(&source[0])
    );

    // The standby's one node asks for its seed, and the source's copy of its
    // store is held up here for as long as the source needs to go on.
    let sb_dirs = vec![scratch("held-standby")];
    let sb_ports = free_ports(1);
    let cfg = LinkConfig {
        sources,
        token: Some(TOKEN.to_string()),
        name: None,
    };
    let cluster = cluster_config(&sb_ports, 1, None);
    let me = cluster
        .members
        .get(&1)
        .cloned()
        .expect("node 1 of the standby");
    let (copied, at_the_copy) = std::sync::mpsc::channel::<u64>();
    let (go_on, held_up) = std::sync::mpsc::channel::<()>();
    source[0]
        .repl_for_test()
        .link_source()
        .expect("a cluster node")
        .after_the_next_copy(move |checkpoint| {
            let _ = copied.send(checkpoint);
            let _ = held_up.recv_timeout(Duration::from_secs(120));
        });
    let taking = tokio::task::spawn_blocking({
        let (dir, cfg) = (sb_dirs[0].clone(), cfg.clone());
        move || crate::rsm::link::seed::take_blocking(&dir, 1, me, &cfg)
    });
    let checkpoint = tokio::task::spawn_blocking(move || {
        at_the_copy.recv_timeout(Duration::from_secs(60))
    })
    .await
    .expect("join")
    .expect("the source copied its store for the seed");
    // The source already knows the standby, by the name it will read with,
    // and has held its log since before the copy.
    let record = crate::rsm::link::seed::read(&sb_dirs[0])
        .expect("seed record")
        .expect("the record of a seed being taken");
    assert_eq!(
        record.id.len(),
        8,
        "the standby's id, drawn with the seed: {record:?}"
    );
    let name = format!("standby-{}", record.id);
    let held = readers(&source[0]);
    assert_eq!(held.len(), 1, "{held:?}");
    assert_eq!(held[0].0, name, "{held:?}");
    assert!(
        held[0].1 <= checkpoint,
        "held from before the copy at {checkpoint}: {held:?}"
    );

    // The source goes on, and purges whatever it may: its new entries are
    // durable, and its purge driver has had four steps.
    for n in 40..60 {
        push(&source[0], n).await.expect("push");
    }
    let applied = source[0].health().applied;
    assert!(applied > checkpoint, "{applied} after {checkpoint}");
    wait_for("the source's new entries are durable", || {
        source[0].repl_for_test().metrics().durable_index >= applied
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        purged() <= checkpoint,
        "the source purged up to {}, past the seed it is sending at {checkpoint}",
        purged()
    );

    // The copy is let go: the seed arrives, and the hold is at its position.
    go_on.send(()).expect("the copy waits");
    let seeded = taking
        .await
        .expect("join")
        .expect("the seed")
        .expect("taken");
    assert_eq!(seeded.applied, checkpoint);
    assert_eq!(
        readers(&source[0]),
        vec![(name.clone(), seeded.applied)],
        "the hold a seed leaves"
    );

    // The standby starts on its seed and follows: the source kept what comes
    // after it.
    let setup = LinkSetup {
        source: Some(cfg.clone()),
        standby: false,
        seed: Some(1),
        fetch: None,
    };
    let d = sb_dirs[0].clone();
    let standby: Nodes = vec![Arc::new(
        tokio::task::spawn_blocking(move || open_node(&d, cluster, setup))
            .await
            .expect("open the seeded node"),
    )];
    wait_for("the seeded node leads its own cluster", || {
        standby[0].health().role == "leader"
    })
    .await;
    converge(&source, &standby, "a standby whose source went on under its seed").await;

    // One standby, one name, one hold: what the seed left is what the reads
    // move, and the purge follows it.
    let status = link_status(&standby[0]).await;
    assert_eq!(status["id"], record.id.as_str(), "{status}");
    assert_eq!(status["name"], name.as_str(), "{status}");
    let held = readers(&source[0]);
    assert_eq!(held.len(), 1, "{held:?}");
    assert_eq!(held[0].0, name, "{held:?}");
    assert!(
        held[0].1 > seeded.applied,
        "the hold moved with the standby: {held:?}"
    );
    wait_for("the source purges behind the standby", || {
        purged() > seeded.applied
    })
    .await;

    close(standby).await;
    close(source).await;
    for d in src_dirs.into_iter().chain(sb_dirs) {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A standby is a cluster, of one node or more: a node started with a source
/// to follow and the single-node replicator says what it needs, at boot.
#[test]
fn a_standby_needs_the_cluster_replicator() {
    let dir = scratch("local-standby");
    let ctx = RsmBuildCtx {
        data_dir: dir.display().to_string(),
        notifier: crate::notify::Notifier::new(false),
        disk_high_pct: 99.0,
        disk_low_pct: 98.0,
    };
    let link = LinkSetup {
        source: Some(LinkConfig {
            sources: vec!["127.0.0.1:1".to_string()],
            token: None,
            name: None,
        }),
        standby: true,
        seed: None,
        fetch: None,
    };
    // No cluster given: the replicator is the environment's, the local one.
    let refused = RaftFacade::open_link_node_for_test(&ctx, BatcherConfig::default(), None, link)
        .err()
        .expect("a standby on the single-node replicator was opened");
    assert!(
        refused.contains("QUEEN_LINK_SOURCE needs QUEEN_RAFT_REPLICATOR=openraft"),
        "{refused}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_refuses_a_standby_without_its_token() {
    let _one = serial().await;
    log_init();

    // A node with a link token.
    let dirs = vec![scratch("token")];
    let ports = free_ports(1);
    let node = open_cluster(&dirs, &ports, Some(TOKEN), LinkSetup::default).await;
    leader_of(&node).await;
    push(&node[0], 1).await.expect("push");
    match read(Some(TOKEN), ports[0]).await.expect("the token's holder reads") {
        Answer::Entries { entries, .. } => assert!(!entries.is_empty()),
        other => panic!("{other:?}"),
    }
    let wrong = read(Some("another-token"), ports[0]).await.unwrap_err();
    assert!(wrong.contains("QUEEN_LINK_SOURCE_TOKEN"), "{wrong}");
    let none = read(None, ports[0]).await.unwrap_err();
    assert!(none.contains("QUEEN_LINK_SOURCE_TOKEN"), "{none}");
    close(node).await;

    // A node without one serves no link, whatever is presented.
    let dirs2 = vec![scratch("no-token")];
    let ports2 = free_ports(1);
    let node = open_cluster(&dirs2, &ports2, None, LinkSetup::default).await;
    leader_of(&node).await;
    let off = read(Some(TOKEN), ports2[0]).await.unwrap_err();
    assert!(off.contains("serves no link"), "{off}");
    close(node).await;

    for d in dirs.into_iter().chain(dirs2) {
        let _ = std::fs::remove_dir_all(d);
    }
}
