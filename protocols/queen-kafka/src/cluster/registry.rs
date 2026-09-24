//! The node registry: which facades are live, in Queen's own key/value store.
//!
//! ```text
//!   key    qk:node:<cluster>:<node id>
//!   value  {"nodeId":2,"host":"kafka-2.example.com","port":9092,
//!           "incarnation":"3f2a…","since":1787824800123,"version":"1.3.0"}
//!   ttl    ttlSeconds = QUEEN_KAFKA_CLUSTER_TTL_MS / 1000
//! ```
//!
//! One `tokio` task per process, one call per tick, TWO operations in it: a
//! `put` of this node's own row and a `getPrefix` of the whole cluster's. Three
//! nodes at the default two-second cadence are **3 KV writes and 3 KV reads a
//! second for the entire cluster** — against a broker whose recorded retention
//! path alone runs at 663 deletes a second, that is not a cost worth a knob war.
//!
//! ## Why a registry and not `QUEEN_KAFKA_PEERS=<addr,addr>`
//!
//! A static peer list was the obvious alternative and it is REJECTED, with a
//! regression as the reason rather than a preference: rendezvous over a static
//! set never moves ownership. Kill node 1 and every group that hashes to node 1
//! is answered "your coordinator is node 1, at an address that refuses
//! connections" — for ever, by every surviving node. That is strictly worse
//! than the behaviour this facade already has, where killing one lets clients
//! reconnect to the other and resume from the offsets in Queen. The registry
//! also buys two things a list cannot: an address change (a pod rescheduled
//! onto a new IP) propagates in one heartbeat, and a mis-set node id is
//! detectable at all.
//!
//! ## The escaping that is deliberately absent
//!
//! There is none, and none is needed: `<cluster>` is validated at boot against
//! `[A-Za-z0-9._-]{1,64}` and `<node id>` is an integer, so `:` cannot appear
//! inside either. `qk:node:prod:` can therefore never be a prefix of
//! `qk:node:prod-2:…` and the prefix read is unambiguous by construction.
//! Contrast [`crate::offsets`], which MUST escape, because a group id is an
//! arbitrary string.
//!
//! ## What a lost claim means, and what it does not
//!
//! At BOOT a lost `putIfAbsent` against a row somebody else is REFRESHING is
//! fatal: two facades sharing one node id advertise one address for two
//! processes, so half of every client's group traffic reaches the wrong one.
//! A row that is merely PRESENT is not that (see [`claim`]): the corpse of the
//! process that had this node id a second ago looks identical on the wire, and
//! that is the ordinary shape of a rolling deploy.
//! At RUNTIME the same conflict stops coordination and keeps the data path —
//! exiting would take that node's clients' produce with it for no correctness
//! gain, and the address it advertises is no longer in anybody's view anyway.
//!
//! ## Inside a raft broker: a directory, not a liveness signal
//!
//! When the facade runs INSIDE a raft broker of more than one voter
//! ([`ClusterState::raft_liveness`]) the row's TTL stops being how a node is
//! judged live, because renewing it is a KV write and a KV write waits behind
//! the data path: a leader warming 100k partitions saw its followers' renewals
//! time out for over a minute and advertised itself as the only broker. There
//! the row carries the `raftNode` it runs on and a long TTL
//! ([`super::RAFT_ROW_TTL_FACTOR`]) — it is a DIRECTORY entry — and liveness is
//! what the raft leader last heard from that raft node ([`super::liveness`]).
//! The heartbeat splits in two ([`spawn`]): a renewer that keeps the row
//! current, whose slowness costs nothing, and a viewer that reads the raft
//! members and the directory — reads the local node answers, never queued
//! behind a write — and installs the live set they describe. And a row found
//! under this node's id at boot that was written from the SAME raft node is this
//! broker's previous facade, taken over at once rather than watched for a TTL
//! ([`claim`]).
//!
//! ## Handing the id back
//!
//! A row dies of its TTL, which is the right answer for a node that was killed
//! and the wrong one for a node that was ASKED to stop: for up to `TTL` after a
//! clean shutdown every survivor would keep advertising an address nothing is
//! listening on, and a replacement reusing the same id would meet its
//! predecessor's row. So a stop deletes the row it holds ([`Registration`]),
//! fenced on the version it holds it with, and the TTL stays as the backstop
//! for the stop nobody got to run.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use crate::offsets::NAMESPACE;
use crate::queen::{self, KvOp, QueenApi};

use super::{ClusterState, Node, MAX_NODE_ID};

/// The prefix every node row starts with.
const KEY_PREFIX: &str = "qk:node:";

/// One line per window while the registry is unreachable. A Queen outage is
/// exactly the situation that produces one event per tick per node.
static UNREACHABLE: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// ...and one per window while this node's id is held by somebody else, or
/// while it is alone in a cluster it should not be alone in.
static CONFLICT: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
static ALONE: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// The key of one node's row.
pub fn node_key(cluster: &str, id: i32) -> String {
    format!("{KEY_PREFIX}{cluster}:{id}")
}

/// The prefix every node of one cluster is under.
pub fn cluster_prefix(cluster: &str) -> String {
    format!("{KEY_PREFIX}{cluster}:")
}

/// The row this node writes for itself. `raftNode` only once it is known, so a
/// facade outside raft writes exactly the row it always has.
fn value_of(me: &Node) -> serde_json::Value {
    let mut row = json!({
        "nodeId": me.id,
        "host": me.host,
        "port": me.port,
        "incarnation": me.incarnation,
        "since": std::time::UNIX_EPOCH
            .elapsed()
            .map(|d| d.as_millis() as i64)
            .unwrap_or_default(),
        "version": env!("CARGO_PKG_VERSION"),
    });
    if let (Some(raft), Some(obj)) = (me.raft_node, row.as_object_mut()) {
        obj.insert("raftNode".to_string(), json!(raft));
    }
    row
}

/// A duration as the whole seconds a row's TTL is written in, never 0.
fn ttl_seconds(ttl: Duration) -> u64 {
    ttl.as_secs().max(1)
}

/// One row, read back — or `None` when it is not one of ours.
///
/// The `nodeId` in the VALUE must match the id in the KEY. They are written
/// together and could only disagree if something else wrote under this prefix,
/// and a row whose two halves disagree is not one to route a client to.
pub fn node_of(cluster: &str, key: &str, value: &serde_json::Value) -> Option<Node> {
    let id: i32 = key.strip_prefix(&cluster_prefix(cluster))?.parse().ok()?;
    if !(super::MIN_NODE_ID..=MAX_NODE_ID).contains(&id)
        || value.get("nodeId")?.as_i64()? != i64::from(id)
    {
        return None;
    }
    let port = value.get("port")?.as_i64()?;
    Some(Node {
        id,
        host: value.get("host")?.as_str()?.to_string(),
        port: u16::try_from(port).ok()?,
        incarnation: value
            .get("incarnation")
            .and_then(|i| i.as_str())
            .unwrap_or_default()
            .to_string(),
        raft_node: value
            .get("raftNode")
            .and_then(|r| r.as_u64())
            .filter(|r| *r != 0),
    })
}

/// What one registry call answered.
struct Answered {
    /// Did our own row apply?
    applied: bool,
    /// The version our row now holds, or the winner's when it did not apply.
    version: i64,
    /// `version`, `absent` or `exists` when it did not apply.
    reason: Option<String>,
    /// Who holds our key, when it is not us.
    winner: Option<Node>,
    /// The live set, as the prefix read returned it. The read runs whether or
    /// not the write applied — a lost precondition without `required` is a
    /// verdict and rolls back nothing (024_kv.sql §6.1 point 5) — which is what
    /// lets a node with a conflicting id still SEE the cluster it is refusing
    /// to coordinate in.
    nodes: Vec<Node>,
}

/// Write this node's row and read the whole cluster's, in one call.
///
/// `expect` is the version this node last held, or `Some(0)` for the
/// `putIfAbsent` a boot and a re-acquisition both send.
async fn write_and_read(
    api: &dyn QueenApi,
    state: &ClusterState,
    token: Option<&str>,
    expect: Option<i64>,
) -> queen::Result<Answered> {
    let ops = [
        KvOp::put_ttl(
            NAMESPACE,
            &node_key(&state.cluster, state.me.id),
            value_of(&state.me_row()),
            ttl_seconds(state.row_ttl()),
            expect,
        ),
        // 64 is the ceiling on a node id, so one page always covers the
        // cluster — and it is well under the stored procedure's own clamp of
        // MAX_KV_PREFIX_LIMIT, so no page is ever left behind.
        KvOp::GetPrefix {
            ns: NAMESPACE.to_string(),
            prefix: cluster_prefix(&state.cluster),
            limit: i64::from(MAX_NODE_ID),
            after: None,
        },
    ];
    let answers = api.kv(&ops, token).await?;
    let (write, read) = match (answers.first(), answers.get(1)) {
        (Some(w), Some(r)) => (w, r),
        _ => {
            return Err(queen::Error::Body(
                "the registry call answered fewer than its two operations".to_string(),
            ))
        }
    };
    Ok(Answered {
        applied: write.applied.unwrap_or(false),
        version: write.version,
        reason: write.reason.clone(),
        winner: node_of(
            &state.cluster,
            &node_key(&state.cluster, state.me.id),
            &write.value,
        ),
        nodes: read
            .rows
            .iter()
            .filter_map(|row| node_of(&state.cluster, &row.key, &row.value))
            .collect(),
    })
}

/// Why a boot claim did not succeed.
pub enum Refused {
    /// Another process holds this node id AND is still refreshing its row.
    /// FATAL.
    Taken(String),
    /// Queen could not be reached, or answered something unreadable. NOT
    /// fatal: the heartbeat re-tries the claim, and until it succeeds the
    /// freshness gate keeps this node from coordinating anything.
    Unreachable(queen::Error),
}

/// Is the row this answer describes ours to keep?
fn is_ours(state: &ClusterState, answered: &Answered) -> bool {
    answered.applied
        // Our own row, from a process that had this pid, this millisecond and
        // this hasher seed. Not reachable in practice; adopting the version is
        // the right answer if it ever is.
        || answered
            .winner
            .as_ref()
            .is_some_and(|w| w.incarnation == state.me.incarnation)
}

/// The fatal, with the evidence that made it fatal in the text: an operator
/// reading it has to be able to tell "you set one id twice" from "your last pod
/// had not finished dying".
fn taken(state: &ClusterState, answered: &Answered, evidence: &str) -> Refused {
    let winner = answered.winner.as_ref();
    Refused::Taken(format!(
        "QUEEN_KAFKA_NODE_ID={id} is already held in cluster `{cluster}` by a LIVE facade at \
         {host}:{port} (incarnation {incarnation}): {evidence}.\n\
         Two facades with one node id advertise one address for two processes: half of every \
         client's group traffic would reach the wrong one, and FindCoordinator would never \
         converge.\n\
         Give this facade its own id, or stop the other.",
        id = state.me.id,
        cluster = state.cluster,
        host = winner.map_or("?", |w| w.host.as_str()),
        port = winner.map_or(0, |w| w.port),
        incarnation = winner.map_or("?", |w| w.incarnation.as_str()),
    ))
}

/// Claim this node's id, once, before the listener binds.
///
/// On success the view is installed from the same call, so the facade is a
/// coordinator from its first accepted connection rather than from its first
/// heartbeat.
pub async fn claim(
    api: &dyn QueenApi,
    state: &ClusterState,
    token: Option<&str>,
) -> Result<i64, Refused> {
    // `putIfAbsent`, which WINS against an expired-but-unpruned row
    // (024_kv.sql:1010-1015) — so a node restarting inside the sweeper's lag
    // reclaims its own id rather than losing to its own corpse.
    let answered = write_and_read(api, state, token, Some(0))
        .await
        .map_err(Refused::Unreachable)?;
    if is_ours(state, &answered) {
        install_from_registry(state, answered.nodes);
        return Ok(answered.version);
    }
    if is_predecessor(state, answered.winner.as_ref()) {
        // Inside a raft broker a row filed under THIS broker's raft node was
        // written by this broker's previous facade: one broker runs one facade,
        // so it is our own corpse, not a twin, and it is taken over at once
        // under a fence rather than watched for a TTL. The row outlives its
        // process on purpose there (a long TTL, [`super::RAFT_ROW_TTL_FACTOR`]),
        // so waiting for it to expire would keep a restarted node's listener
        // closed for minutes while raft already reports it live.
        let adopted = write_and_read(api, state, token, Some(answered.version))
            .await
            .map_err(Refused::Unreachable)?;
        if adopted.applied {
            tracing::info!(
                target: "boot",
                node_id = state.me.id,
                cluster = %state.cluster,
                raft_node = state.raft_node().unwrap_or_default(),
                "took over this node id's registry row from the previous facade of this same \
                 raft broker"
            );
            install_from_registry(state, adopted.nodes);
            return Ok(adopted.version);
        }
        return watch_and_adopt(api, state, token, adopted).await;
    }
    watch_and_adopt(api, state, token, answered).await
}

/// Whether `winner` — the row holding this node's id — was written by an
/// earlier facade of the SAME raft broker, which can only be a process that is
/// gone: a broker runs one facade. Never true outside raft, and never true
/// until this node knows its raft node.
fn is_predecessor(state: &ClusterState, winner: Option<&Node>) -> bool {
    state.raft_liveness()
        && state.raft_node().is_some()
        && winner.is_some_and(|w| {
            w.raft_node == state.raft_node() && w.incarnation != state.me.incarnation
        })
}

/// Install the live set a registry read describes, where the registry IS the
/// liveness signal. Inside a raft broker rows outlive their processes by
/// design and a row is not a live node: that view comes from
/// [`refresh_view`].
fn install_from_registry(state: &ClusterState, nodes: Vec<Node>) {
    if !state.raft_liveness() {
        state.install(nodes);
    }
}

/// The boot claim lost to a row this process did not write. That is EITHER a
/// second facade configured with our node id — the operator error the fatal
/// exists for — OR the corpse of our own predecessor, which is exactly what a
/// rolling deploy looks like from inside the replacement: the pod that had this
/// StatefulSet ordinal was killed without getting to hand the id back, and its
/// row is still inside the TTL. Exiting on the second one is a crash loop, and
/// a StatefulSet ordinal is the natural source of a node id, so it is the
/// COMMON case rather than an exotic one.
///
/// The two are told apart by evidence rather than by a timestamp inside the
/// row: a holder that is alive rewrites its row every heartbeat, and every
/// write mints a new `version` from the store's own sequence
/// (024_kv.sql:133-140). So this watches that version for `TTL + one
/// heartbeat`:
///
///   * the row EXPIRES while we watch — the `putIfAbsent` of the next poll
///     wins and we adopt the id, which is the resurrection rule arriving a few
///     seconds late instead of a process exiting;
///   * the version MOVES — somebody is refreshing it, so it is a live facade
///     with our id: FATAL, and the message says which observation proved it;
///   * neither, for the whole window — the holder has not written for longer
///     than the TTL it claims liveness with, so we take the row from it with a
///     write fenced on the version that never moved. A holder that comes back
///     to life meets the runtime half of §7 (its next tick loses on `expect`,
///     it stops coordinating and keeps serving the data path).
///
/// Nothing here compares two machines' clocks. The only clock is this process's
/// own monotonic one, measuring how long WE watched — the property §7 rests on
/// when it says clock skew cannot cause a liveness split.
async fn watch_and_adopt(
    api: &dyn QueenApi,
    state: &ClusterState,
    token: Option<&str>,
    first: Answered,
) -> Result<i64, Refused> {
    // The row's own `ttlSeconds` is the TTL and is rewritten by every
    // heartbeat, so a holder that has stopped writing loses its row within one
    // TTL of its last write. One heartbeat of margin covers the poll that
    // straddles the expiry.
    let window = state.ttl + state.heartbeat;
    let held_version = first.version;
    tracing::info!(
        target: "boot",
        node_id = state.me.id,
        cluster = %state.cluster,
        holder = first.winner.as_ref().map_or("?", |w| w.host.as_str()),
        incarnation = first.winner.as_ref().map_or("?", |w| w.incarnation.as_str()),
        watch_ms = window.as_millis() as u64,
        "QUEEN_KAFKA_NODE_ID is held by a registry row this process did not write. Watching it \
         for one TTL: a holder that is alive rewrites the row every heartbeat, and this facade \
         starts only if it stops, expires, or turns out never to have been there"
    );
    let started = tokio::time::Instant::now();
    let mut last = first;
    while started.elapsed() < window {
        tokio::time::sleep(state.heartbeat).await;
        let answered = write_and_read(api, state, token, Some(0))
            .await
            .map_err(Refused::Unreachable)?;
        if is_ours(state, &answered) {
            tracing::warn!(
                target: "boot",
                node_id = state.me.id,
                cluster = %state.cluster,
                waited_ms = started.elapsed().as_millis() as u64,
                "the registry row holding this node id expired while this facade waited, so the \
                 id is taken back. Its holder stopped without handing it over, which is what a \
                 SIGKILL, an OOM kill or a lost node looks like from here"
            );
            install_from_registry(state, answered.nodes);
            return Ok(answered.version);
        }
        if answered.version != held_version {
            return Err(taken(
                state,
                &answered,
                "its registry row was rewritten while this facade watched it, which only a \
                 running heartbeat does",
            ));
        }
        last = answered;
    }
    // The row outlived the window without ever being rewritten: its holder is
    // not heartbeating, whatever TTL it wrote the row with. Fenced on the
    // version that never moved, so a holder that writes in this very instant
    // wins and we still fail closed.
    let answered = write_and_read(api, state, token, Some(held_version))
        .await
        .map_err(Refused::Unreachable)?;
    if !answered.applied {
        return Err(taken(
            state,
            &answered,
            "its registry row changed under the write that would have taken it over",
        ));
    }
    tracing::warn!(
        target: "boot",
        node_id = state.me.id,
        cluster = %state.cluster,
        holder = last.winner.as_ref().map_or("?", |w| w.host.as_str()),
        watched_ms = started.elapsed().as_millis() as u64,
        "the registry row holding this node id was never refreshed while this facade watched it \
         for a whole TTL, so the id is taken over. Its holder is not heartbeating"
    );
    install_from_registry(state, answered.nodes);
    Ok(answered.version)
}

/// The version this process's registry row holds, shared between the heartbeat
/// that renews it and the stop path that hands it back.
///
/// `0` is "we hold nothing", which is the store's own nothing for a key that is
/// absent or expired ([`crate::queen::KvRow::version`]) rather than a second
/// convention invented here.
#[derive(Clone, Default)]
struct Held(Arc<HeldState>);

#[derive(Default)]
struct HeldState {
    version: AtomicI64,
    /// Has this PROCESS ever held the row? It is not `version != 0`: a row that
    /// expired under a slow tick leaves the version behind and the fact behind
    /// it standing. See [`tick`] for the one decision that turns on it.
    ever: AtomicBool,
    /// The version of a row this node is about to TAKE OVER from its own
    /// predecessor on the same raft broker ([`is_predecessor`]), or 0. The next
    /// write expects it instead of `putIfAbsent`.
    adopt: AtomicI64,
}

impl Held {
    fn new(version: Option<i64>) -> Held {
        let held = Held::default();
        held.set(version);
        held
    }

    fn get(&self) -> Option<i64> {
        match self.0.version.load(Ordering::Relaxed) {
            0 => None,
            v => Some(v),
        }
    }

    fn ever(&self) -> bool {
        self.0.ever.load(Ordering::Relaxed)
    }

    fn set(&self, version: Option<i64>) {
        self.0
            .version
            .store(version.unwrap_or(0), Ordering::Relaxed);
        if version.is_some() {
            self.0.ever.store(true, Ordering::Relaxed);
        }
    }

    fn adopt(&self) -> Option<i64> {
        match self.0.adopt.load(Ordering::Relaxed) {
            0 => None,
            v => Some(v),
        }
    }

    fn set_adopt(&self, version: Option<i64>) {
        self.0.adopt.store(version.unwrap_or(0), Ordering::Relaxed);
    }
}

/// This node's place in the registry, for as long as the process wants it: the
/// heartbeat that renews it, and the one call that gives it back.
///
/// It exists because a stop that only stops the process is not a stop: the row
/// would sit in every peer's view until its TTL, and every client sent to this
/// node's address by a Metadata or a FindCoordinator answer in that window
/// would meet a closed port.
pub struct Registration {
    api: Arc<dyn QueenApi>,
    state: Arc<ClusterState>,
    token: Option<String>,
    held: Held,
    /// The heartbeat — or, inside a raft broker, the renewer and the viewer.
    /// Aborted when the registration is dropped as well as when it is handed
    /// back: a facade whose serve loop ends WITHOUT a deregister (a panic the
    /// in-process supervisor restarts) must not leave a task behind that keeps
    /// rewriting a row under the old incarnation and fights the new one.
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// What a stop managed to do with this node's row.
#[derive(Debug, PartialEq, Eq)]
pub enum Departure {
    /// The row is gone: every peer drops this node on its next read, which is
    /// one heartbeat and not one TTL.
    Released,
    /// There was no row of ours to delete — a facade that never won its claim,
    /// or one whose row had expired under it.
    NothingHeld,
    /// The key holds somebody else's row now, and it is left alone. The reason
    /// the store gave.
    NotOurs(String),
    /// The delete did not happen. The row expires by its TTL instead, which is
    /// the behaviour a kill gets and is never wrong, only slower.
    Failed(String),
}

impl Registration {
    /// Stop heartbeating and delete this node's row, inside `budget`.
    ///
    /// The heartbeat is aborted FIRST and awaited: a tick that landed after the
    /// delete would take the id straight back with its own `putIfAbsent`, and
    /// this node would sit in every peer's view for a full TTL after it had
    /// stopped serving. The delete is FENCED on the version we hold, so the one
    /// window this cannot close — an in-flight write that reaches Queen after
    /// the abort — costs a [`Departure::NotOurs`] and a wait for the TTL, and
    /// can never delete a row that belongs to somebody else.
    pub async fn deregister(mut self, budget: Duration) -> Departure {
        for task in std::mem::take(&mut self.tasks) {
            task.abort();
            let _ = task.await;
        }
        let Some(version) = self.held.get() else {
            return Departure::NothingHeld;
        };
        let ops = [KvOp::delete(
            NAMESPACE,
            &node_key(&self.state.cluster, self.state.me.id),
            Some(version),
        )];
        match tokio::time::timeout(budget, self.api.kv(&ops, self.token.as_deref())).await {
            Err(_) => Departure::Failed(format!(
                "the delete did not answer within {}ms",
                budget.as_millis()
            )),
            Ok(Err(e)) => Departure::Failed(e.to_string()),
            Ok(Ok(answers)) => match answers.first() {
                Some(a) if a.applied == Some(true) => Departure::Released,
                Some(a) => Departure::NotOurs(a.reason.clone().unwrap_or_default()),
                None => Departure::Failed("the delete answered nothing".to_string()),
            },
        }
    }
}

/// The heartbeat: one call every `HEARTBEAT_MS`, for the life of the process.
///
/// It never exits on its own. A tick that fails leaves the view alone and lets
/// the freshness gate run down, which is the correct shape: a node that cannot
/// read the live set stops COORDINATING and keeps serving the data path.
///
/// Inside a raft broker ([`ClusterState::raft_liveness`]) the one call becomes
/// two tasks, because the write and the read no longer answer the same
/// question: a RENEWER keeps this node's directory row current (a write, which
/// rides the pipeline and may be slow under load — nothing waits on it), and a
/// VIEWER reads the raft members and the directory every heartbeat (reads,
/// served by the local node, never queued behind the data path) and installs
/// the live set they describe ([`refresh_view`]).
pub fn spawn(
    api: Arc<dyn QueenApi>,
    state: Arc<ClusterState>,
    token: Option<String>,
    version: Option<i64>,
) -> Registration {
    let held = Held::new(version);
    let tasks = if state.raft_liveness() {
        let renewer = tokio::spawn({
            let (api, state, token, held) = (
                Arc::clone(&api),
                Arc::clone(&state),
                token.clone(),
                held.clone(),
            );
            async move {
                loop {
                    tokio::time::sleep(state.heartbeat).await;
                    renew(api.as_ref(), &state, token.as_deref(), &held).await;
                }
            }
        });
        let viewer = tokio::spawn({
            let (api, state, token) = (Arc::clone(&api), Arc::clone(&state), token.clone());
            async move {
                let mut ticks: u64 = 0;
                loop {
                    tokio::time::sleep(state.heartbeat).await;
                    refresh_view(api.as_ref(), &state, token.as_deref()).await;
                    ticks += 1;
                    alone(&state, ticks);
                }
            }
        });
        vec![renewer, viewer]
    } else {
        vec![tokio::spawn({
            let (api, state, token, held) = (
                Arc::clone(&api),
                Arc::clone(&state),
                token.clone(),
                held.clone(),
            );
            async move {
                let mut ticks: u64 = 0;
                loop {
                    tokio::time::sleep(state.heartbeat).await;
                    tick(api.as_ref(), &state, token.as_deref(), &held).await;
                    ticks += 1;
                    alone(&state, ticks);
                }
            }
        })]
    };
    Registration {
        api,
        state,
        token,
        held,
        tasks,
    }
}

/// One raft-mode renewal of this node's directory row: a write, and nothing
/// else. The verdict rules are [`tick`]'s, with one addition — a row held by
/// this node's own predecessor on the same raft broker is taken over on the
/// next write, fenced on its version, instead of being a conflict.
async fn renew(api: &dyn QueenApi, state: &ClusterState, token: Option<&str>, held: &Held) {
    let expect = Some(held.get().or_else(|| held.adopt()).unwrap_or(0));
    let op = KvOp::put_ttl(
        NAMESPACE,
        &node_key(&state.cluster, state.me.id),
        value_of(&state.me_row()),
        ttl_seconds(state.row_ttl()),
        expect,
    );
    let write = match api.kv(&[op], token).await {
        Ok(answers) => match answers.into_iter().next() {
            Some(w) => w,
            None => return,
        },
        Err(e) => {
            if let Some(suppressed) = UNREACHABLE.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    node_id = state.me.id,
                    cluster = %state.cluster,
                    error = %e,
                    suppressed,
                    "this node's registry row could not be renewed; liveness is raft's, so this \
                     node stays in every broker list while raft hears from it, and the row keeps \
                     its long TTL"
                );
            }
            return;
        }
    };
    if write.applied == Some(true) {
        held.set(Some(write.version));
        held.set_adopt(None);
        return;
    }
    // Expired (or deleted) under us: the next write is a `putIfAbsent`.
    if write.reason.as_deref() == Some("absent") {
        held.set(None);
        held.set_adopt(None);
        return;
    }
    let winner = node_of(
        &state.cluster,
        &node_key(&state.cluster, state.me.id),
        &write.value,
    );
    if winner
        .as_ref()
        .is_some_and(|w| w.incarnation == state.me.incarnation)
    {
        held.set(Some(write.version));
        held.set_adopt(None);
        return;
    }
    if is_predecessor(state, winner.as_ref()) {
        held.set(None);
        held.set_adopt(Some(write.version));
        return;
    }
    // Somebody else with our node id on ANOTHER broker, and they are writing:
    // the operator error the boot fatal exists for, met at runtime.
    held.set(None);
    held.set_adopt(None);
    if held.ever() {
        state.stop_coordinating();
    }
    if let Some(suppressed) = CONFLICT.tick_now() {
        tracing::error!(
            target: "kafka",
            node_id = state.me.id,
            cluster = %state.cluster,
            reason = write.reason.as_deref().unwrap_or("unknown"),
            holder = winner.as_ref().map_or("?", |w| w.host.as_str()),
            suppressed,
            "another process holds QUEEN_KAFKA_NODE_ID; this facade has stopped coordinating \
             groups and is still serving produce and fetch. Give one of the two its own id."
        );
    }
}

/// Read the directory: every registry row of this cluster, with no write.
async fn read_directory(
    api: &dyn QueenApi,
    state: &ClusterState,
    token: Option<&str>,
) -> queen::Result<Vec<Node>> {
    let ops = [KvOp::GetPrefix {
        ns: NAMESPACE.to_string(),
        prefix: cluster_prefix(&state.cluster),
        limit: i64::from(MAX_NODE_ID),
        after: None,
    }];
    let answers = api.kv(&ops, token).await?;
    let read = answers
        .first()
        .ok_or_else(|| queen::Error::Body("the registry read answered no operation".to_string()))?;
    Ok(read
        .rows
        .iter()
        .filter_map(|row| node_of(&state.cluster, &row.key, &row.value))
        .collect())
}

/// One raft-mode view: the broker's raft members (who the leader has heard
/// from, and when) and the directory, judged into a live set and installed
/// ([`super::liveness`]). Both are reads the local node answers; neither waits
/// on a write. `false` when nothing was installed — the view is too old to
/// judge by, the broker is not a raft broker, or a read failed — in which case
/// the last view stands and the coordination gate runs down on its own.
pub async fn refresh_view(api: &dyn QueenApi, state: &ClusterState, token: Option<&str>) -> bool {
    let members = match api.raft_members(token).await {
        Ok(Some(m)) => m,
        Ok(None) => return false,
        Err(e) => {
            if let Some(suppressed) = UNREACHABLE.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    node_id = state.me.id,
                    cluster = %state.cluster,
                    error = %e,
                    suppressed,
                    "the raft members could not be read; this facade keeps its last view of the \
                     cluster and stops coordinating groups once that view is a TTL old"
                );
            }
            return false;
        }
    };
    state.learn_raft_node(members.node_id);
    let rows = match read_directory(api, state, token).await {
        Ok(rows) => rows,
        Err(e) => {
            if let Some(suppressed) = UNREACHABLE.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    node_id = state.me.id,
                    cluster = %state.cluster,
                    error = %e,
                    suppressed,
                    "the node registry could not be read; this facade keeps its last view of the \
                     cluster and stops coordinating groups once that view is a TTL old"
                );
            }
            return false;
        }
    };
    match state.judge(&rows, &members) {
        Some((live, down)) => {
            state.install_view(live, down);
            true
        }
        None => false,
    }
}

/// One heartbeat.
async fn tick(api: &dyn QueenApi, state: &ClusterState, token: Option<&str>, held: &Held) {
    // With no version in hand — at boot, or after our row expired — the write
    // is the `putIfAbsent` again rather than an unconditional upsert, so a node
    // that has been away cannot silently overwrite whoever took its id.
    let expect = Some(held.get().unwrap_or(0));
    let answered = match write_and_read(api, state, token, expect).await {
        Ok(a) => a,
        Err(e) => {
            if let Some(suppressed) = UNREACHABLE.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    node_id = state.me.id,
                    cluster = %state.cluster,
                    error = %e,
                    suppressed,
                    "the node registry could not be reached; this facade keeps serving the data \
                     path and stops coordinating groups once its view goes stale"
                );
            }
            return;
        }
    };

    if answered.applied {
        held.set(Some(answered.version));
        state.install(answered.nodes);
        return;
    }

    // The row is gone (it expired while this process was slow, and the sweeper
    // took it). The next tick re-acquires it with `putIfAbsent`; the view is
    // still installed, because the read in that same call is good.
    if answered.reason.as_deref() == Some("absent") {
        held.set(None);
        state.install(answered.nodes);
        return;
    }

    // Somebody else holds our key. Unless it is us — which would mean a second
    // process with our own incarnation, an impossibility we would rather adopt
    // than fight.
    if answered
        .winner
        .as_ref()
        .is_some_and(|w| w.incarnation == state.me.incarnation)
    {
        held.set(Some(answered.version));
        state.install(answered.nodes);
        return;
    }
    // The row is not ours any more, so there is nothing of ours to give back at
    // a stop: a delete fenced on a version we no longer hold would be refused
    // anyway, and an unfenced one would delete the winner's row.
    held.set(None);
    // The latch is for a node that HAD the row and lost it: somebody wrote over
    // a row we were renewing, which only a live process does, and this one must
    // stop claiming groups for good.
    //
    // A node that has never held it is a different situation with the same
    // answer on the wire, and it is the boot-time one: the registry was
    // unreachable when this process started, so it never claimed its id, and
    // the row it is now meeting may equally be its own predecessor's corpse
    // inside the TTL. Latching there would make a Queen blip at the wrong
    // second cost this facade its coordination for the life of the process.
    // Not latching costs nothing: a node that never wrote its row never
    // installed a view either (the branches that install are the ones that
    // wrote), so it is refusing group RPCs on the freshness gate anyway, and it
    // starts coordinating only once one of its own writes applies. The ERROR
    // below is emitted eitherway, so the operator error is never quiet.
    if held.ever() {
        state.stop_coordinating();
    }
    if let Some(suppressed) = CONFLICT.tick_now() {
        tracing::error!(
            target: "kafka",
            node_id = state.me.id,
            cluster = %state.cluster,
            reason = answered.reason.as_deref().unwrap_or("unknown"),
            holder = answered.winner.as_ref().map_or("?", |w| w.host.as_str()),
            suppressed,
            "another process holds QUEEN_KAFKA_NODE_ID; this facade has stopped coordinating \
             groups and is still serving produce and fetch. Give one of the two its own id."
        );
    }
}

/// A facade that has been up for two ticks and still sees only itself is almost
/// certainly misconfigured, and the two ways to get there are both invisible
/// from inside one process.
fn alone(state: &ClusterState, ticks: u64) {
    if ticks < 2 || state.view().is_none_or(|v| v.nodes.len() > 1) {
        return;
    }
    if let Some(suppressed) = ALONE.tick_now() {
        tracing::warn!(
            target: "kafka",
            node_id = state.me.id,
            cluster = %state.cluster,
            suppressed,
            "this facade is the only node in its cluster registry. If that is not intended, the \
             other facades are writing somewhere else: every node of one cluster must set the \
             same QUEEN_KAFKA_CLUSTER, and must present credentials of ONE Queen tenant — \
             queen.kv is keyed by tenant, so two tenants are two registries and each facade sees \
             only itself."
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::testing;
    use crate::queen::testing::FakeQueen;
    use std::time::Duration;

    fn state_of(me: i32) -> Arc<ClusterState> {
        testing::state(
            &[
                (1, "kafka-1.example.com", 9092),
                (2, "kafka-2.example.com", 9092),
            ],
            me,
        )
    }

    /// A fresh state, as a boot has it: no view at all.
    fn unseen(me: i32, host: &str) -> Arc<ClusterState> {
        ClusterState::new(
            Node {
                id: me,
                host: host.to_string(),
                port: 9092,
                incarnation: super::super::new_incarnation(),
                raft_node: None,
            },
            "rig".to_string(),
            Duration::from_secs(2),
            Duration::from_secs(10),
        )
    }

    #[test]
    fn a_key_names_its_cluster_and_node() {
        assert_eq!(node_key("prod", 2), "qk:node:prod:2");
        assert_eq!(cluster_prefix("prod"), "qk:node:prod:");
        // The charset is what makes this unambiguous without escaping: `:` is
        // outside it, so one cluster's prefix is never another's.
        assert!(!node_key("prod-2", 1).starts_with(&cluster_prefix("prod")));
    }

    #[test]
    fn a_row_whose_halves_disagree_is_not_a_node() {
        let good = json!({"nodeId": 2, "host": "h", "port": 9092, "incarnation": "i"});
        assert_eq!(
            node_of("rig", "qk:node:rig:2", &good),
            Some(Node {
                id: 2,
                host: "h".into(),
                port: 9092,
                incarnation: "i".into(),
                raft_node: None,
            })
        );
        // The id in the key and the id in the value must agree.
        assert_eq!(node_of("rig", "qk:node:rig:3", &good), None);
        // ...and everything else that is not a node row.
        for (key, value) in [
            (
                "qk:node:rig:0",
                json!({"nodeId": 0, "host": "h", "port": 1}),
            ),
            (
                "qk:node:rig:65",
                json!({"nodeId": 65, "host": "h", "port": 1}),
            ),
            ("qk:node:rig:x", good.clone()),
            ("qk:node:other:2", good.clone()),
            ("qk:group:g:orders:0", json!({"offset": 4})),
            ("qk:node:rig:2", json!({"nodeId": 2, "port": 9092})),
            (
                "qk:node:rig:2",
                json!({"nodeId": 2, "host": "h", "port": 99_999}),
            ),
        ] {
            assert_eq!(node_of("rig", key, &value), None, "{key} {value}");
        }
    }

    /// The boot claim is ONE call that both takes the id and reads the cluster,
    /// so a facade coordinates from its first connection and not from its first
    /// heartbeat.
    #[tokio::test]
    async fn a_boot_claim_takes_the_id_and_installs_the_view() {
        let api = FakeQueen::with(&[]);
        let state = unseen(2, "kafka-2.example.com");
        // A peer that is already live.
        api.kv_seed_ttl(
            NAMESPACE,
            "qk:node:rig:1",
            json!({"nodeId": 1, "host": "kafka-1.example.com", "port": 9092, "incarnation": "a"}),
            Some(10),
        );

        let version = match claim(&*api, &state, None).await {
            Ok(v) => v,
            Err(_) => panic!("the claim was refused"),
        };
        assert!(version > 0);
        let view = state.view().expect("the boot call installed no view");
        assert_eq!(view.nodes.iter().map(|n| n.id).collect::<Vec<_>>(), [1, 2]);
        assert!(state.coordinating());
        assert_eq!(
            api.kv_calls.lock().unwrap().len(),
            1,
            "the boot is one call"
        );
    }

    /// A row somebody keeps rewriting, which is the only thing a LIVE holder
    /// does and a corpse does not. `every` is that holder's heartbeat.
    fn heartbeating_holder(api: Arc<FakeQueen>, every: Duration) -> tokio::task::JoinHandle<()> {
        let write = |api: &FakeQueen| {
            api.kv_seed_ttl(
                NAMESPACE,
                "qk:node:rig:2",
                json!({"nodeId": 2, "host": "kafka-9.example.com", "port": 9092,
                       "incarnation": "3f2a"}),
                Some(10),
            );
        };
        // The first write happens HERE and not in the task: a spawned task does
        // not run until the spawner awaits, and the claim under test must meet
        // a row that is already there.
        write(&api);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                write(&api);
            }
        })
    }

    /// Two facades with one node id: the second one does not start. What makes
    /// it fatal is that the holder is REFRESHING its row, and the message says
    /// so — the same configuration with a dead holder is a rolling restart and
    /// is not fatal (the two tests below).
    #[tokio::test(start_paused = true)]
    async fn a_duplicate_node_id_is_fatal_at_boot() {
        let api = FakeQueen::with(&[]);
        let holder = heartbeating_holder(Arc::clone(&api), Duration::from_secs(1));
        let state = unseen(2, "kafka-2.example.com");
        let started = tokio::time::Instant::now();
        match claim(&*api, &state, None).await {
            Err(Refused::Taken(message)) => {
                assert!(message.contains("QUEEN_KAFKA_NODE_ID=2"), "{message}");
                assert!(message.contains("kafka-9.example.com"), "{message}");
                assert!(message.contains("3f2a"), "{message}");
                assert!(message.contains("rewritten"), "the evidence: {message}");
            }
            _ => panic!("a held node id was claimed anyway"),
        }
        // ...and it is decided within a heartbeat or two of the boot, not after
        // a TTL: an operator who set one id twice must be told at once.
        assert!(
            started.elapsed() < state.ttl,
            "the duplicate-id boot took longer than a TTL to fail"
        );
        holder.abort();
    }

    /// THE rolling-deploy case, and the reason a present row cannot be fatal on
    /// its own: the pod that had this node id was killed without handing it
    /// back, its row is still inside the TTL, and the replacement carries the
    /// same id because a StatefulSet ordinal is where a node id comes from.
    /// It waits the corpse out and starts. It does NOT exit.
    #[tokio::test(start_paused = true)]
    async fn a_boot_inside_a_dead_holders_ttl_adopts_the_id_instead_of_exiting() {
        let api = FakeQueen::with(&[]);
        api.kv_seed_ttl(
            NAMESPACE,
            "qk:node:rig:2",
            json!({"nodeId": 2, "host": "kafka-2.example.com", "port": 9092,
                   "incarnation": "the-pod-that-was-killed"}),
            Some(10),
        );
        // A peer that is live throughout, so the adopted view is a real one.
        api.kv_seed_ttl(
            NAMESPACE,
            "qk:node:rig:1",
            json!({"nodeId": 1, "host": "kafka-1.example.com", "port": 9092, "incarnation": "a"}),
            Some(3_600),
        );
        let state = unseen(2, "kafka-2.example.com");
        let started = tokio::time::Instant::now();
        let version = match claim(&*api, &state, None).await {
            Ok(v) => v,
            Err(_) => panic!("a replacement pod refused to start inside its predecessor's TTL"),
        };
        assert!(version > 0);
        // It waited for the corpse's row rather than deleting it blind, and it
        // is serving as soon as it does.
        let waited = started.elapsed();
        assert!(
            waited >= state.ttl,
            "it did not wait the TTL out: {waited:?}"
        );
        assert!(state.coordinating());
        assert_eq!(
            state
                .view()
                .expect("no view")
                .nodes
                .iter()
                .map(|n| n.id)
                .collect::<Vec<_>>(),
            [1, 2],
            "the adopted view is the live set, not just this node"
        );
        assert_eq!(
            api.kv_get(NAMESPACE, "qk:node:rig:2").and_then(|v| v
                .get("incarnation")
                .and_then(|i| i.as_str().map(str::to_string))),
            Some(state.me.incarnation.clone()),
            "the row under this node id is not this process's"
        );
    }

    /// ...and the belt for a row that outlives the watch: a holder configured
    /// with a much longer TTL than ours leaves a row that will not expire in
    /// time. It is still not heartbeating, so it is taken over — with a write
    /// FENCED on the version that never moved, so a holder that comes back to
    /// life in that instant wins instead.
    #[tokio::test(start_paused = true)]
    async fn a_row_that_is_never_refreshed_is_taken_over_under_a_fence() {
        let api = FakeQueen::with(&[]);
        api.kv_seed_ttl(
            NAMESPACE,
            "qk:node:rig:2",
            json!({"nodeId": 2, "host": "kafka-2.example.com", "port": 9092,
                   "incarnation": "a-holder-with-a-long-ttl"}),
            Some(3_600),
        );
        let state = unseen(2, "kafka-2.example.com");
        assert!(
            claim(&*api, &state, None).await.is_ok(),
            "a row nobody refreshes held the id for ever"
        );
        assert_eq!(
            api.kv_get(NAMESPACE, "qk:node:rig:2").and_then(|v| v
                .get("incarnation")
                .and_then(|i| i.as_str().map(str::to_string))),
            Some(state.me.incarnation.clone())
        );
        // The take-over is a fenced write on the version that stood still, not
        // an unconditional one: exactly one `expect` that is neither 0 nor ours.
        let fenced = api
            .kv_ops()
            .into_iter()
            .filter(|op| matches!(op, KvOp::Put { expect: Some(v), .. } if *v != 0))
            .count();
        assert_eq!(fenced, 1, "the take-over was not fenced");
    }

    /// ...and the resurrection rule: a row that has EXPIRED is not a holder,
    /// so a node restarting inside the sweeper's lag reclaims its own id.
    #[tokio::test(start_paused = true)]
    async fn an_expired_row_is_not_a_holder() {
        let api = FakeQueen::with(&[]);
        api.kv_seed_ttl(
            NAMESPACE,
            "qk:node:rig:2",
            json!({"nodeId": 2, "host": "kafka-2.example.com", "port": 9092,
                   "incarnation": "the-process-that-died"}),
            Some(10),
        );
        tokio::time::advance(Duration::from_secs(11)).await;
        let state = unseen(2, "kafka-2.example.com");
        assert!(
            claim(&*api, &state, None).await.is_ok(),
            "a node lost its own id to its own corpse"
        );
    }

    /// An unreachable Queen at boot is not fatal — but the facade does not
    /// coordinate until it has seen the registry.
    #[tokio::test]
    async fn an_unreachable_registry_at_boot_is_not_fatal_and_not_coordinating() {
        let api = FakeQueen::with(&[]);
        api.fail_kv(queen::Error::Transport("connection refused".into()));
        let state = unseen(2, "kafka-2.example.com");
        assert!(matches!(
            claim(&*api, &state, None).await,
            Err(Refused::Unreachable(_))
        ));
        assert!(
            !state.coordinating(),
            "a facade that has never read the registry claimed a group"
        );
        assert!(state.view().is_none());
    }

    /// The tick is ONE call doing both jobs, and it keeps the version it was
    /// handed so the next one is a conditional write and not an upsert.
    #[tokio::test]
    async fn a_tick_writes_its_row_and_reads_the_set_in_one_call() {
        let api = FakeQueen::with(&[]);
        let state = state_of(2);
        let held = Held::default();
        tick(&*api, &state, None, &held).await;

        let calls = api.kv_calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].len(), 2, "the tick is two operations");
        match &calls[0][0] {
            KvOp::Put {
                key,
                ttl_seconds,
                expect,
                forever,
                required,
                ..
            } => {
                assert_eq!(key, "qk:node:rig:2");
                assert_eq!(*ttl_seconds, Some(10));
                assert!(
                    !*forever,
                    "a registry row that never expires is not liveness"
                );
                assert_eq!(*expect, Some(0), "the first tick re-acquires");
                assert!(
                    !*required,
                    "a lost registry write must not abort the read beside it"
                );
            }
            other => panic!("{other:?}"),
        }
        assert!(held.get().is_some(), "the version was not kept");

        // The next tick expects it.
        api.kv_calls.lock().unwrap().clear();
        tick(&*api, &state, None, &held).await;
        let second = api.kv_calls.lock().unwrap()[0][0].clone();
        match second {
            KvOp::Put { expect, .. } => assert_ne!(expect, Some(0)),
            other => panic!("{other:?}"),
        }
    }

    /// A row that expired while this node was slow is re-acquired rather than
    /// overwritten, and the view is still installed from the same call.
    #[tokio::test(start_paused = true)]
    async fn a_row_that_expired_is_re_acquired() {
        let api = FakeQueen::with(&[]);
        let state = state_of(2);
        let held = Held::default();
        tick(&*api, &state, None, &held).await;
        let version = held.get().expect("no version");

        tokio::time::advance(Duration::from_secs(11)).await;
        tick(&*api, &state, None, &held).await;
        // The stale `expect` lost with reason `absent`, so the version is
        // dropped and the NEXT tick is a putIfAbsent that wins.
        assert_eq!(held.get(), None);
        tick(&*api, &state, None, &held).await;
        assert!(held.get().is_some_and(|v| v != version));
        assert!(state.coordinating());
    }

    /// Losing the id at RUNTIME stops coordination and does not stop the
    /// process — and the view stays readable, because the prefix read in the
    /// same call is unaffected by the write's verdict.
    #[tokio::test]
    async fn losing_the_id_at_runtime_stops_coordination_only() {
        let api = FakeQueen::with(&[]);
        let state = state_of(2);
        let held = Held::default();
        tick(&*api, &state, None, &held).await;
        assert!(state.coordinating());

        // Somebody else takes the key.
        api.kv_seed_ttl(
            NAMESPACE,
            "qk:node:rig:2",
            json!({"nodeId": 2, "host": "elsewhere", "port": 9092, "incarnation": "someone-else"}),
            Some(10),
        );
        tick(&*api, &state, None, &held).await;
        assert!(!state.coordinating());
        assert!(state.view().is_some(), "the view was lost with the id");
        assert_eq!(
            held.get(),
            None,
            "a version we no longer hold must not be carried into a stop's delete"
        );
    }

    /// ...and the boot-time shape of the same conflict, which must NOT latch: a
    /// facade whose registry call failed at boot has never held its row, so the
    /// foreign row its first tick meets is as likely to be its own predecessor's
    /// corpse as a live twin. It refuses group RPCs meanwhile because it has no
    /// view, and it coordinates again once its own write applies.
    #[tokio::test(start_paused = true)]
    async fn a_node_that_never_held_its_row_waits_the_holder_out_instead_of_latching() {
        let api = FakeQueen::with(&[]);
        api.fail_kv(queen::Error::Transport("connection refused".into()));
        let state = unseen(2, "kafka-2.example.com");
        assert!(matches!(
            claim(&*api, &state, None).await,
            Err(Refused::Unreachable(_))
        ));
        // The predecessor's row, still inside its TTL.
        api.kv_seed_ttl(
            NAMESPACE,
            "qk:node:rig:2",
            json!({"nodeId": 2, "host": "kafka-2.example.com", "port": 9092,
                   "incarnation": "the-pod-that-was-killed"}),
            Some(10),
        );

        let held = Held::default();
        tick(&*api, &state, None, &held).await;
        assert!(
            !state.coordinating(),
            "a facade with no view of the cluster claimed a group"
        );

        // The corpse expires; the next tick takes the id and the facade
        // coordinates, which a latched `stop_coordinating` would have made
        // impossible for the life of the process.
        tokio::time::advance(Duration::from_secs(11)).await;
        tick(&*api, &state, None, &held).await;
        assert!(held.get().is_some(), "the id was never re-acquired");
        assert!(
            state.coordinating(),
            "the facade never coordinated again after outliving a corpse's row"
        );
    }

    /// The registry read filters what is not a node row, so a group's offsets
    /// under the same namespace can never become a broker in a Metadata answer.
    #[tokio::test]
    async fn only_node_rows_become_nodes() {
        let api = FakeQueen::with(&[]);
        let state = state_of(1);
        api.kv_seed(NAMESPACE, "qk:group:g:orders:0", json!({"offset": 4}));
        api.kv_seed_ttl(NAMESPACE, "qk:node:rig:9", json!({"nodeId": 9}), Some(10));
        api.kv_seed_ttl(
            NAMESPACE,
            "qk:node:other-cluster:5",
            json!({"nodeId": 5, "host": "h", "port": 1, "incarnation": "x"}),
            Some(10),
        );
        let held = Held::default();
        tick(&*api, &state, None, &held).await;
        assert_eq!(
            state
                .view()
                .unwrap()
                .nodes
                .iter()
                .map(|n| n.id)
                .collect::<Vec<_>>(),
            [1]
        );
    }

    // --------------------------------------------------------- handing it back

    /// A registration this test can stop, over a state whose own row is already
    /// claimed — `spawn` as `main` calls it, without the process.
    async fn registered(api: &Arc<FakeQueen>, state: &Arc<ClusterState>) -> Registration {
        let version = match claim(&**api, state, None).await {
            Ok(v) => v,
            Err(_) => panic!("the fixture could not claim its own id"),
        };
        spawn(
            Arc::clone(api) as Arc<dyn QueenApi>,
            Arc::clone(state),
            None,
            Some(version),
        )
    }

    /// THE stop path. A facade that is asked to stop hands its node id back
    /// instead of leaving a row for its peers to advertise for a whole TTL —
    /// which is what makes the replacement's boot claim win immediately, and
    /// what stops FindCoordinator naming a closed port for ten seconds.
    #[tokio::test(start_paused = true)]
    async fn a_stop_deletes_the_row_it_holds() {
        let api = FakeQueen::with(&[]);
        let state = unseen(2, "kafka-2.example.com");
        let registration = registered(&api, &state).await;
        assert!(api.kv_get(NAMESPACE, "qk:node:rig:2").is_some());

        assert_eq!(
            registration.deregister(Duration::from_secs(2)).await,
            Departure::Released
        );
        assert_eq!(
            api.kv_get(NAMESPACE, "qk:node:rig:2"),
            None,
            "the node's own row outlived its process"
        );
        // The delete is FENCED: an unconditional one would take whatever row is
        // under the key, including a successor's.
        let deletes: Vec<KvOp> = api
            .kv_ops()
            .into_iter()
            .filter(|op| matches!(op, KvOp::Delete { .. }))
            .collect();
        assert_eq!(deletes.len(), 1, "{deletes:?}");
        match &deletes[0] {
            KvOp::Delete { ns, key, expect } => {
                assert_eq!(ns, NAMESPACE);
                assert_eq!(key, "qk:node:rig:2");
                assert!(expect.is_some_and(|v| v != 0), "the delete was not fenced");
            }
            other => panic!("{other:?}"),
        }

        // ...and the heartbeat is gone with it: a tick that landed after the
        // delete would take the id straight back and the row would be there for
        // a TTL after the process stopped serving.
        let calls = api.kv_calls.lock().unwrap().len();
        tokio::time::advance(state.heartbeat * 5).await;
        tokio::task::yield_now().await;
        assert_eq!(
            api.kv_calls.lock().unwrap().len(),
            calls,
            "the heartbeat outlived the deregistration"
        );
        assert_eq!(api.kv_get(NAMESPACE, "qk:node:rig:2"), None);
    }

    /// A facade that never won its id has nothing to give back, and says so
    /// rather than deleting a key it does not own.
    #[tokio::test(start_paused = true)]
    async fn a_stop_without_a_row_deletes_nothing() {
        let api = FakeQueen::with(&[]);
        let state = unseen(2, "kafka-2.example.com");
        let registration = spawn(
            Arc::clone(&api) as Arc<dyn QueenApi>,
            Arc::clone(&state),
            None,
            None,
        );
        assert_eq!(
            registration.deregister(Duration::from_secs(2)).await,
            Departure::NothingHeld
        );
        assert!(
            api.kv_ops()
                .iter()
                .all(|op| !matches!(op, KvOp::Delete { .. })),
            "a facade that holds nothing sent a delete anyway"
        );
    }

    /// The one thing a stop must never do: delete the row of whoever holds this
    /// node id NOW. The fence is on the version, so a successor that took the
    /// key survives its predecessor's shutdown.
    #[tokio::test(start_paused = true)]
    async fn a_stop_never_deletes_a_successors_row() {
        let api = FakeQueen::with(&[]);
        let state = unseen(2, "kafka-2.example.com");
        let registration = registered(&api, &state).await;

        // Somebody else takes the key while this process is on its way out.
        api.kv_seed_ttl(
            NAMESPACE,
            "qk:node:rig:2",
            json!({"nodeId": 2, "host": "the-replacement", "port": 9092, "incarnation": "next"}),
            Some(10),
        );
        assert_eq!(
            registration.deregister(Duration::from_secs(2)).await,
            Departure::NotOurs("version".to_string())
        );
        assert_eq!(
            api.kv_get(NAMESPACE, "qk:node:rig:2")
                .and_then(|v| v.get("host").and_then(|h| h.as_str().map(str::to_string))),
            Some("the-replacement".to_string()),
            "the stop deleted a row that was not its own"
        );
    }

    /// A stop cannot hang on a Queen that is not answering: the row expires by
    /// its TTL instead, which is exactly what a kill gets, and the process gets
    /// on with exiting inside whatever window its supervisor gave it.
    #[tokio::test(start_paused = true)]
    async fn a_stop_that_queen_refuses_gives_up_and_leaves_it_to_the_ttl() {
        let api = FakeQueen::with(&[]);
        let state = unseen(2, "kafka-2.example.com");
        let registration = registered(&api, &state).await;
        api.fail_kv(queen::Error::Transport("connection refused".into()));
        match registration.deregister(Duration::from_secs(2)).await {
            Departure::Failed(why) => assert!(why.contains("connection refused"), "{why}"),
            other => panic!("{other:?}"),
        }
        // Not deleted, and not lost either: the TTL is the backstop.
        assert!(api.kv_get(NAMESPACE, "qk:node:rig:2").is_some());
        tokio::time::advance(state.ttl + Duration::from_secs(1)).await;
        assert_eq!(api.kv_get(NAMESPACE, "qk:node:rig:2"), None);
    }

    // ------------------------------------------------------ inside raft broker

    const TTL: Duration = Duration::from_secs(10);
    const BEAT: Duration = Duration::from_secs(2);

    fn raft_state(me: i32) -> Arc<ClusterState> {
        ClusterState::with_raft_liveness(
            Node {
                id: me,
                host: format!("kafka-{me}.example.com"),
                port: 9092,
                incarnation: super::super::new_incarnation(),
                raft_node: Some(me as u64),
            },
            "rig".to_string(),
            BEAT,
            TTL,
        )
    }

    /// A directory row as a raft-mode facade writes it: its raft node, and the
    /// long TTL of a directory entry.
    fn seed_raft_row(api: &FakeQueen, id: i32, incarnation: &str) {
        api.kv_seed_ttl(
            NAMESPACE,
            &node_key("rig", id),
            json!({"nodeId": id, "host": format!("kafka-{id}.example.com"), "port": 9092,
                   "incarnation": incarnation, "raftNode": id}),
            Some(300),
        );
    }

    fn members(answering: u64, acks: &[(u64, u64)], view_age: u64) -> crate::queen::RaftMembers {
        crate::queen::RaftMembers {
            node_id: answering,
            leader_id: Some(1),
            view_age_ms: Some(view_age),
            members: acks
                .iter()
                .map(|(id, age)| crate::queen::RaftMember {
                    node_id: *id,
                    voter: true,
                    last_ack_ms: Some(*age),
                })
                .collect(),
        }
    }

    fn live_ids(state: &ClusterState) -> Vec<i32> {
        state
            .view()
            .map_or_else(Vec::new, |v| v.nodes.iter().map(|n| n.id).collect())
    }

    fn down_ids(state: &ClusterState) -> Vec<i32> {
        state
            .view()
            .map_or_else(Vec::new, |v| v.down.iter().map(|n| n.id).collect())
    }

    /// Let the spawned heartbeat tasks run for `d` of paused time.
    async fn run_for(d: Duration) {
        let step = Duration::from_millis(100);
        let mut left = d;
        while !left.is_zero() {
            let s = step.min(left);
            tokio::time::advance(s).await;
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            left -= s;
        }
    }

    /// THE regression, as the benchmark met it. Inside a raft broker every KV
    /// write is starved — a leader warming 100k partitions answered
    /// `kv_timeout` to the followers' registry renewals for over a minute —
    /// and yet every node stays in every node's broker list, and keeps
    /// coordinating, for as long as raft hears from them. The registry rows
    /// are never rewritten in the whole window.
    #[tokio::test(start_paused = true)]
    async fn inside_raft_a_node_whose_registry_writes_starve_stays_live() {
        let api = FakeQueen::with(&[]);
        for id in 1..=3 {
            seed_raft_row(&api, id, &format!("inc-{id}"));
        }
        *api.raft_members.lock().unwrap() = Some(members(2, &[(1, 30), (2, 80), (3, 120)], 150));
        let state = raft_state(2);
        assert!(refresh_view(&*api, &state, None).await);
        assert_eq!(live_ids(&state), [1, 2, 3]);

        *api.kv_write_error.lock().unwrap() = Some(queen::Error::Status {
            code: 503,
            body: r#"{"error":"kv_unavailable","reason":"kv_timeout"}"#.into(),
            retry_after_ms: Some(1_000),
        });
        let registration = spawn(
            Arc::clone(&api) as Arc<dyn QueenApi>,
            Arc::clone(&state),
            None,
            None,
        );
        for _ in 0..30 {
            run_for(BEAT).await;
            assert_eq!(live_ids(&state), [1, 2, 3], "a live node was dropped");
            assert!(down_ids(&state).is_empty());
            assert!(state.coordinating(), "the gate closed on a busy pipeline");
        }
        drop(registration);
    }

    /// A node the raft leader stops hearing from leaves the live set once a
    /// TTL of silence has passed — not on one late beat — stays a replica
    /// (down, not forgotten), and comes back once it is heard again.
    #[tokio::test(start_paused = true)]
    async fn inside_raft_a_silent_node_drops_within_the_ttl_and_returns_when_heard() {
        let api = FakeQueen::with(&[]);
        for id in 1..=3 {
            seed_raft_row(&api, id, &format!("inc-{id}"));
        }
        *api.raft_members.lock().unwrap() = Some(members(1, &[(1, 0), (2, 50), (3, 50)], 0));
        let state = raft_state(1);
        assert!(refresh_view(&*api, &state, None).await);
        let registration = spawn(
            Arc::clone(&api) as Arc<dyn QueenApi>,
            Arc::clone(&state),
            None,
            None,
        );

        // Node 3 is killed: the leader's figure for it grows with the clock.
        let killed = tokio::time::Instant::now();
        let mut dropped_after = None;
        for _ in 0..100 {
            run_for(Duration::from_millis(500)).await;
            let silent = killed.elapsed().as_millis() as u64;
            *api.raft_members.lock().unwrap() =
                Some(members(1, &[(1, 0), (2, 50), (3, silent)], 0));
            if dropped_after.is_none() && !live_ids(&state).contains(&3) {
                dropped_after = Some(killed.elapsed());
            }
            if silent < TTL.as_millis() as u64 {
                assert_eq!(live_ids(&state), [1, 2, 3], "dropped after {silent} ms");
            }
        }
        let dropped_after = dropped_after.expect("a dead node was never dropped");
        assert!(
            dropped_after <= TTL + BEAT + Duration::from_secs(1),
            "dropped after {dropped_after:?}"
        );
        assert_eq!(live_ids(&state), [1, 2]);
        assert_eq!(down_ids(&state), [3], "a dead voter is still a replica");

        // Restarted: heard again at once.
        *api.raft_members.lock().unwrap() = Some(members(1, &[(1, 0), (2, 50), (3, 40)], 0));
        run_for(BEAT + Duration::from_millis(200)).await;
        assert_eq!(live_ids(&state), [1, 2, 3]);
        assert!(down_ids(&state).is_empty());
        drop(registration);
    }

    /// A node that cannot trust its view — a follower cut off from the leader
    /// — keeps advertising the last live set rather than every other node
    /// looking dead from where it stands, and stops coordinating once that set
    /// is a TTL old.
    #[tokio::test(start_paused = true)]
    async fn inside_raft_a_stale_view_keeps_the_last_live_set_and_closes_the_gate() {
        let api = FakeQueen::with(&[]);
        for id in 1..=3 {
            seed_raft_row(&api, id, &format!("inc-{id}"));
        }
        *api.raft_members.lock().unwrap() = Some(members(2, &[(1, 0), (2, 0), (3, 0)], 100));
        let state = raft_state(2);
        assert!(refresh_view(&*api, &state, None).await);
        let registration = spawn(
            Arc::clone(&api) as Arc<dyn QueenApi>,
            Arc::clone(&state),
            None,
            None,
        );
        // Cut off: the leader's figures age along with the copy.
        *api.raft_members.lock().unwrap() =
            Some(members(2, &[(1, 60_000), (2, 60_000), (3, 60_000)], 60_000));
        run_for(TTL + BEAT).await;
        assert_eq!(
            live_ids(&state),
            [1, 2, 3],
            "a partitioned node shrank the cluster"
        );
        assert!(!state.coordinating());
        drop(registration);
    }

    /// A raft-mode row names its raft node and carries the long TTL of a
    /// directory entry, so a busy pipeline cannot expire it out of the
    /// directory.
    #[tokio::test(start_paused = true)]
    async fn inside_raft_a_row_names_its_raft_node_and_outlives_a_stall() {
        let api = FakeQueen::with(&[]);
        let state = raft_state(2);
        assert!(claim(&*api, &state, None).await.is_ok());
        let put = api
            .kv_ops()
            .into_iter()
            .find(|op| matches!(op, KvOp::Put { .. }))
            .unwrap();
        match put {
            KvOp::Put {
                value, ttl_seconds, ..
            } => {
                assert_eq!(value["raftNode"], 2);
                assert_eq!(
                    ttl_seconds,
                    Some(TTL.as_secs() * u64::from(super::super::RAFT_ROW_TTL_FACTOR))
                );
            }
            _ => unreachable!(),
        }
        // Outside raft the row is exactly what it always was.
        let api = FakeQueen::with(&[]);
        let state = unseen(2, "kafka-2.example.com");
        assert!(claim(&*api, &state, None).await.is_ok());
        let row = api.kv_get(NAMESPACE, "qk:node:rig:2").unwrap();
        assert!(row.get("raftNode").is_none(), "{row}");
    }

    /// A restarted node meets its own predecessor's row — the long TTL keeps
    /// it — and takes it over AT ONCE: the row was written from this same raft
    /// broker, which runs one facade, so it is a corpse and not a twin. Waiting
    /// out its TTL would keep the listener closed for minutes.
    #[tokio::test(start_paused = true)]
    async fn inside_raft_a_restart_takes_its_predecessors_row_over_at_once() {
        let api = FakeQueen::with(&[]);
        seed_raft_row(&api, 2, "the-process-that-was-killed");
        let state = raft_state(2);
        let started = tokio::time::Instant::now();
        let version = match claim(&*api, &state, None).await {
            Ok(v) => v,
            Err(_) => panic!("a restarted node refused its own predecessor's row"),
        };
        assert!(version > 0);
        assert!(
            started.elapsed() < BEAT,
            "it waited: {:?}",
            started.elapsed()
        );
        assert_eq!(
            api.kv_get(NAMESPACE, "qk:node:rig:2").unwrap()["incarnation"],
            state.me.incarnation.as_str()
        );
    }

    /// ...but a row of this node id written from ANOTHER raft broker is the
    /// operator error the fatal exists for, and it is still watched: a live
    /// twin keeps rewriting it and the boot fails.
    #[tokio::test(start_paused = true)]
    async fn inside_raft_a_twin_on_another_broker_is_still_fatal() {
        let api = FakeQueen::with(&[]);
        let write = |api: &FakeQueen| {
            api.kv_seed_ttl(
                NAMESPACE,
                "qk:node:rig:2",
                json!({"nodeId": 2, "host": "kafka-9.example.com", "port": 9092,
                       "incarnation": "twin", "raftNode": 5}),
                Some(300),
            );
        };
        write(&api);
        let twin = {
            let api = Arc::clone(&api);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    write(&api);
                }
            })
        };
        let state = raft_state(2);
        assert!(matches!(
            claim(&*api, &state, None).await,
            Err(Refused::Taken(_))
        ));
        twin.abort();
    }

    /// A node whose boot claim could not reach Queen finds its predecessor's
    /// row at runtime and takes it over on the next renewal, fenced on the
    /// version it found.
    #[tokio::test(start_paused = true)]
    async fn inside_raft_the_renewer_takes_a_predecessors_row_over() {
        let api = FakeQueen::with(&[]);
        seed_raft_row(&api, 2, "the-process-that-was-killed");
        *api.raft_members.lock().unwrap() = Some(members(2, &[(1, 0), (2, 0)], 0));
        let state = raft_state(2);
        let registration = spawn(
            Arc::clone(&api) as Arc<dyn QueenApi>,
            Arc::clone(&state),
            None,
            None,
        );
        run_for(BEAT * 3).await;
        assert_eq!(
            api.kv_get(NAMESPACE, "qk:node:rig:2").unwrap()["incarnation"],
            state.me.incarnation.as_str()
        );
        assert!(state.coordinating());
        let departure = registration.deregister(Duration::from_secs(2)).await;
        assert_eq!(departure, Departure::Released);
    }

    /// A registration that is dropped without a deregister — a serve loop
    /// that panicked — takes its tasks with it, so no orphan keeps rewriting
    /// a row under the old incarnation.
    #[tokio::test(start_paused = true)]
    async fn dropping_a_registration_stops_its_heartbeat() {
        let api = FakeQueen::with(&[]);
        let state = state_of(2);
        let registration = spawn(
            Arc::clone(&api) as Arc<dyn QueenApi>,
            Arc::clone(&state),
            None,
            None,
        );
        run_for(BEAT * 2).await;
        let calls = api.kv_calls.lock().unwrap().len();
        assert!(calls > 0);
        drop(registration);
        run_for(BEAT * 5).await;
        assert_eq!(api.kv_calls.lock().unwrap().len(), calls);
    }
}
