//! CLUSTER MODE: two or three facades that unmodified Kafka clients address as
//! one cluster.
//!
//! Opt-in, and the switch is a single variable: `QUEEN_KAFKA_NODE_ID` in
//! `1..=64`. Absent, nothing in this module is read, spawned, allocated or
//! written, and the facade behaves exactly as it always has — [`Cluster::Single`]
//! is not a one-node cluster, it is the old code path with a name.
//!
//! ## What was actually broken, and what fixes each half
//!
//! Two facades in front of one Queen deployment already share everything
//! durable: offsets are in Queen's key/value store and the log is in Postgres.
//! What they did NOT share was who arbitrates a group, and that produced two
//! distinct defects that need two distinct fixes:
//!
//!   * **Double delivery** — a ROUTING defect. Both facades answered
//!     FindCoordinator with themselves, so one group formed twice, each
//!     generation assigning every partition. Fixed by making group ownership a
//!     deterministic function of a shared live-node set ([`registry`],
//!     [`rendezvous`]) and answering `NOT_COORDINATOR` at a non-owner. That is
//!     a steady-state fix.
//!   * **Offset rewind** — a WRITE defect. `offsets::store` was an
//!     unconditional upsert, so the loser of a race overwrote the winner
//!     silently. Routing shrinks the window but cannot close it: a node that is
//!     stale about the live set still believes it is the owner. Fixed by a
//!     compare-and-set fence on the commit path ([`fence`]). That is a
//!     race-window fix.
//!
//! Neither alone is enough, which is why both are here.
//!
//! ## The tenant is NOT an input to the ownership hash
//!
//! This is the one that would have cost days. [`crate::identity::TenantKey`] is
//! `Tenant(Arc<str>)` when Queen names the cluster a credential acts on, and
//! `Credential(CredentialKey)` when it does not — and `CredentialKey` is seeded
//! from two `RandomState`s drawn once PER PROCESS (secret.rs:81-85). The same
//! credential therefore hashes differently on every facade, and
//! PLAN_QUEEN_KAFKA.md records that the unresolved fallback is the common case.
//! Hashing the tenant would make two facades compute different owners for the
//! same group in almost every deployment: FindCoordinator would never converge
//! and the redirect would loop for ever.
//!
//! So [`Cluster::owner_of_group`] takes the GROUP ID and nothing else. The cost
//! is stated rather than hidden: two Queen tenants running a group of the same
//! name are coordinated by the same facade. That is harmless — they stay two
//! entries in the coordinator's registry (`coordinator/mod.rs`, `GroupKey`) and
//! their offsets and fences are two different `queen.kv` rows, because the
//! tenant is a COLUMN there and an argument to the stored procedure
//! (024_kv.sql:111-114). Ownership simply does not spread by tenant; with two
//! or three nodes and a hash over the group id it spreads by group, which is
//! the thing that needed spreading.
//!
//! ## Leadership is an ADVERTISEMENT, not an access control
//!
//! Every node serves Produce, Fetch, ListOffsets and OffsetFetch for every
//! partition, whatever Metadata said the leader was. Apache Kafka answers
//! `NOT_LEADER_OR_FOLLOWER` at a non-leader because a non-leader DOES NOT HAVE
//! THE DATA; that reason does not exist here, and copying the refusal without
//! the reason would cost real availability for nothing:
//!
//!   * `server/sql/procedures/032_log_fetch.sql:11-19` — a fetch *"is not a
//!     pop. No lease is taken, no `queen.log_consumers` row is read, created or
//!     advanced, no watermark is written, nothing is claimed"*.
//!   * `server/sql/procedures/003_log_push.sql:131-213` — the offset is
//!     allocated by the DATABASE under a `queen.log_partitions` row lock, so
//!     two brokers appending to one partition cannot issue the same offset.
//!
//! and refusing would make each membership change a synchronised metadata storm
//! during the very window in which the nodes disagree about the live set.
//!
//! The one consequence that must be documented rather than discovered:
//! a producer with `max.in.flight.requests.per.connection > 1` that has a batch
//! in flight to the old leader when its metadata moves can have two batches
//! land out of order. Apache Kafka has the identical hazard on a leader change
//! without idempotence, idempotence is not implemented here (`InitProducerId`
//! is not advertised), and the client-side fix is the same one:
//! `max.in.flight.requests.per.connection=1`.

pub mod fence;
pub mod liveness;
pub mod registry;
pub mod rendezvous;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kafka_protocol::error::ResponseError;
use tokio::time::Instant;

use crate::handlers::metadata::SINGLE_NODE_ID;

/// The lowest node id a clustered facade may take.
///
/// **0 is reserved** for today's single-node identity
/// ([`crate::handlers::metadata::SINGLE_NODE_ID`]). Keeping it out of the
/// cluster range makes "am I clustered" unambiguous everywhere, makes a
/// registry row for node 0 an impossible state, and means a client that cached
/// node 0 from single mode is handed a WHOLLY new broker list on its next
/// refresh rather than a half-overlapping one. Apache Kafka is 0-based; the
/// deviation is deliberate and the boot error says so.
pub const MIN_NODE_ID: i32 = 1;

/// The highest. Every live node is written into every Metadata response and the
/// registry is read with ONE `getPrefix` page, so the ceiling keeps that read
/// to a single page under `MAX_KV_PREFIX_LIMIT` (1000). 64 is two orders of
/// magnitude past the two or three this is for.
pub const MAX_NODE_ID: i32 = 64;

/// One facade of a cluster, as the registry describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub id: i32,
    /// What clients are told to connect to — `QUEEN_KAFKA_ADVERTISED_ADDR`,
    /// which must already differ per node.
    pub host: String,
    pub port: u16,
    /// An opaque per-PROCESS token, compared for equality only and never
    /// ordered: it exists to tell "this process" from "a process that had my
    /// id before". Same discipline the stored procedure demands of a `version`
    /// (024_kv.sql:133-139), for the same reason.
    pub incarnation: String,
    /// The raft node of the broker this facade runs INSIDE, when it does
    /// ([`liveness`]): what ties a registry row to the raft member whose
    /// heartbeats say whether it is live. `None` over HTTP, and on a row
    /// written before its writer knew.
    pub raft_node: Option<u64>,
}

/// The live set as of the last successful registry read.
#[derive(Debug)]
pub struct View {
    /// Sorted by node id and deduplicated, so `nodes.first()` is the lowest
    /// live id — which is what `controller_id` is.
    pub nodes: Vec<Node>,
    /// Registered nodes that are storage replicas of every partition but are
    /// NOT live right now: a raft voter the leader has not heard from within
    /// the TTL ([`liveness`]). Never this node, never a node of `nodes`, and
    /// always empty outside a raft broker. They stay in every partition's
    /// replica list and leave its ISR, which is how Kafka reports a broker
    /// that is down — so a readiness check that wants the full ISR waits for
    /// it instead of passing on the survivors.
    pub down: Vec<Node>,
    /// [`tokio::time::Instant`], i.e. MONOTONIC. A wall clock would let an NTP
    /// step forge freshness, and freshness is what stops a partitioned node
    /// from claiming groups it can no longer see the owner of.
    pub read_at: Instant,
}

impl View {
    fn ids(&self) -> Vec<i32> {
        self.nodes.iter().map(|n| n.id).collect()
    }
}

/// Everything cluster mode knows. One per process, behind the [`crate::Facade`]
/// and shared by every per-connection view of it.
pub struct ClusterState {
    /// This facade.
    pub me: Node,
    /// `QUEEN_KAFKA_CLUSTER` — the registry prefix and the advertised
    /// `cluster_id`.
    pub cluster: String,
    /// `QUEEN_KAFKA_CLUSTER_TTL_MS`, as a duration: the `ttlSeconds` on the
    /// registry row AND the staleness bound of the view. One number, because
    /// two would be two ways to disagree about who is live.
    pub ttl: Duration,
    /// `QUEEN_KAFKA_CLUSTER_HEARTBEAT_MS`.
    pub heartbeat: Duration,
    /// Cloned out of the lock and never held across an await — the same idiom,
    /// and the same reason, as `coordinator::Coordinator::groups`. `None` until
    /// the first successful read.
    view: Mutex<Option<Arc<View>>>,
    /// Cleared when this node loses its id to another process (§7). It keeps
    /// serving the data path and stops coordinating; it does NOT exit, because
    /// exiting takes its clients' produce with it for no correctness gain.
    coordinating: AtomicBool,
    /// The per-group fence versions. See [`fence`].
    pub fences: fence::Fences,
    /// Liveness comes from RAFT, not from the registry rows' TTL: the facade
    /// runs inside a raft broker of more than one voter ([`liveness`]). The
    /// registry is then a directory — who is which node, at which address, on
    /// which raft member — and who is live is what the raft leader last heard.
    raft: bool,
    /// The raft node id of the broker this facade runs inside, once the broker
    /// has said (0 until then). It never changes for the life of the process.
    raft_node: AtomicU64,
    /// The raft judge's memory: who was live last time, for its hysteresis.
    judge: Mutex<liveness::Judge>,
}

/// How much longer than `QUEEN_KAFKA_CLUSTER_TTL_MS` a registry row lives when
/// liveness comes from raft ([`ClusterState::row_ttl`]).
///
/// The row is then only a directory entry, and the one thing it must not do is
/// expire because the pipeline its renewal rides is busy: that is exactly how
/// a leader warming 100k partitions came to see itself as the only node (the
/// followers' renewals timed out for over a minute). Thirty TTLs — five minutes
/// at the default — outlasts any stall a live cluster survives, and still lets
/// the row of a node that is gone for good (killed and never restarted, or
/// restarted without the facade) leave the directory on its own. A node that is
/// merely dead is out of the live set long before that: raft says so within
/// one TTL.
pub const RAFT_ROW_TTL_FACTOR: u32 = 30;

impl ClusterState {
    pub fn new(me: Node, cluster: String, heartbeat: Duration, ttl: Duration) -> Arc<ClusterState> {
        ClusterState::build(me, cluster, heartbeat, ttl, false)
    }

    /// A cluster state whose liveness comes from the raft broker the facade
    /// runs inside ([`liveness`]).
    pub fn with_raft_liveness(
        me: Node,
        cluster: String,
        heartbeat: Duration,
        ttl: Duration,
    ) -> Arc<ClusterState> {
        ClusterState::build(me, cluster, heartbeat, ttl, true)
    }

    fn build(
        me: Node,
        cluster: String,
        heartbeat: Duration,
        ttl: Duration,
        raft: bool,
    ) -> Arc<ClusterState> {
        let raft_node = me.raft_node.unwrap_or(0);
        Arc::new(ClusterState {
            me,
            cluster,
            ttl,
            heartbeat,
            view: Mutex::new(None),
            coordinating: AtomicBool::new(true),
            fences: fence::Fences::new(),
            raft,
            raft_node: AtomicU64::new(raft_node),
            judge: Mutex::new(liveness::Judge::new(ttl)),
        })
    }

    /// Whether liveness comes from raft ([`ClusterState::with_raft_liveness`]).
    pub fn raft_liveness(&self) -> bool {
        self.raft
    }

    /// The raft node of this facade's broker, once known.
    pub fn raft_node(&self) -> Option<u64> {
        match self.raft_node.load(Ordering::Relaxed) {
            0 => None,
            id => Some(id),
        }
    }

    /// Record the raft node of this facade's broker. The first answer wins: a
    /// process runs inside ONE broker, and a later different answer can only
    /// be a misrouted call, which must not re-file this node under another.
    pub fn learn_raft_node(&self, id: u64) {
        if id != 0 {
            let _ = self
                .raft_node
                .compare_exchange(0, id, Ordering::Relaxed, Ordering::Relaxed);
        }
    }

    /// This node as its registry row describes it: [`ClusterState::me`] plus
    /// the raft node once known.
    pub fn me_row(&self) -> Node {
        Node {
            raft_node: self.raft_node(),
            ..self.me.clone()
        }
    }

    /// The TTL this node's registry row is written with. The liveness TTL,
    /// unless liveness comes from raft AND the row carries the raft node it is
    /// judged by — then [`RAFT_ROW_TTL_FACTOR`] times it, because the row is a
    /// directory entry and raft is what says the node is live. A row without
    /// its raft node is judged by presence and keeps the short TTL.
    pub fn row_ttl(&self) -> Duration {
        if self.raft && self.raft_node().is_some() {
            self.ttl * RAFT_ROW_TTL_FACTOR
        } else {
            self.ttl
        }
    }

    /// The live set, from the directory rows and the raft members' view, or
    /// `None` when that view is too old to judge by ([`liveness::Judge`]).
    pub fn judge(
        &self,
        rows: &[Node],
        members: &crate::queen::RaftMembers,
    ) -> Option<(Vec<Node>, Vec<Node>)> {
        self.judge
            .lock()
            .expect("the liveness judge lock is never held across a panic")
            .judge(&self.me, rows, members)
    }

    /// The last known live set, or `None` if there has never been one.
    pub fn view(&self) -> Option<Arc<View>> {
        self.view
            .lock()
            .expect("the cluster view lock is never held across a panic")
            .clone()
    }

    /// Replace the view with what the registry just answered.
    ///
    /// `nodes` is sorted, deduplicated by id, and guaranteed to contain THIS
    /// node: a facade that is serving must be reachable in the broker list it
    /// hands out, even in the moment its own registry row is missing or has
    /// been taken by someone else.
    pub fn install(&self, nodes: Vec<Node>) {
        self.install_view(nodes, Vec::new());
    }

    /// [`ClusterState::install`], with the registered nodes that are down
    /// ([`View::down`]).
    ///
    /// THIS node is always live and always as it describes itself: an entry
    /// for its id that came off the registry is replaced by
    /// [`ClusterState::me`], because the row can be a predecessor's until this
    /// process's own write lands, and the address a client is handed for this
    /// node must be the one this node is listening on.
    pub fn install_view(&self, mut nodes: Vec<Node>, mut down: Vec<Node>) {
        nodes.retain(|n| n.id != self.me.id);
        nodes.push(self.me.clone());
        nodes.sort_by_key(|n| n.id);
        nodes.dedup_by_key(|n| n.id);
        down.retain(|d| d.id != self.me.id && !nodes.iter().any(|n| n.id == d.id));
        down.sort_by_key(|n| n.id);
        down.dedup_by_key(|n| n.id);
        *self
            .view
            .lock()
            .expect("the cluster view lock is never held across a panic") = Some(Arc::new(View {
            nodes,
            down,
            read_at: Instant::now(),
        }));
    }

    /// Stop claiming ownership of anything, for good — the runtime half of a
    /// duplicate node id (§7). The data path is untouched.
    pub fn stop_coordinating(&self) {
        self.coordinating.store(false, Ordering::Relaxed);
    }

    /// Is this node allowed to answer a group RPC?
    ///
    /// THE gate. A node whose last successful registry read is older than the
    /// TTL cannot tell who owns a group, so it claims nothing: it answers
    /// COORDINATOR_NOT_AVAILABLE, keeps serving the data path, and keeps
    /// answering Metadata from the last known view. This is what makes a
    /// registry partition unable to become a split brain.
    pub fn coordinating(&self) -> bool {
        self.coordinating.load(Ordering::Relaxed)
            && self.view().is_some_and(|v| v.read_at.elapsed() <= self.ttl)
    }
}

/// Which facade of the cluster this process is, or that there is only one.
///
/// `Clone` because a [`crate::Facade`] is cloned per connection; the clone is
/// an `Arc` bump and nothing else.
#[derive(Clone)]
pub enum Cluster {
    /// `QUEEN_KAFKA_NODE_ID` is not set. Byte for byte the behaviour this
    /// facade has always had.
    Single,
    Enabled(Arc<ClusterState>),
}

impl Cluster {
    /// The state, when there is one.
    pub fn state(&self) -> Option<&Arc<ClusterState>> {
        match self {
            Cluster::Single => None,
            Cluster::Enabled(s) => Some(s),
        }
    }

    /// What Metadata advertises as `cluster_id`.
    ///
    /// In cluster mode it is the configured name, which defaults to `queen` —
    /// so an operator who sets only `QUEEN_KAFKA_NODE_ID` changes nothing a
    /// client can see here. Clients use it to refuse to talk to two different
    /// clusters through one connection pool, so it must be stable across
    /// restarts, and both forms are.
    pub fn cluster_id(&self) -> &str {
        match self {
            Cluster::Single => crate::handlers::metadata::CLUSTER_ID,
            Cluster::Enabled(s) => &s.cluster,
        }
    }

    /// The cluster as ONE request sees it. Taken once per Metadata response, so
    /// that every broker entry and every partition of it is answered from the
    /// same set — a response built from two reads could name a leader that is
    /// not in its own broker list.
    pub fn placement(&self, advertised_host: &str, advertised_port: u16) -> Placement {
        match self {
            Cluster::Single => Placement::new(
                vec![Node {
                    id: SINGLE_NODE_ID,
                    host: advertised_host.to_string(),
                    port: advertised_port,
                    incarnation: String::new(),
                    raft_node: None,
                }],
                Vec::new(),
                false,
            ),
            Cluster::Enabled(s) => {
                // The last known view even when it is STALE: a Metadata that
                // suddenly reported one broker would tell every client the
                // cluster had shrunk, which is a far worse answer than one that
                // is a few seconds old. With no view at all, self alone.
                let (nodes, down) = s.view().map_or_else(
                    || (vec![s.me.clone()], Vec::new()),
                    |v| (v.nodes.clone(), v.down.iter().map(|n| n.id).collect()),
                );
                Placement::new(nodes, down, true)
            }
        }
    }

    /// Who coordinates `group`.
    pub fn owner_of_group(&self, group: &str) -> Owner {
        let Cluster::Enabled(state) = self else {
            return Owner::Us;
        };
        if !state.coordinating() {
            return Owner::Unknown;
        }
        let Some(view) = state.view() else {
            return Owner::Unknown;
        };
        match rendezvous::winner(rendezvous::DOMAIN_GROUP, group.as_bytes(), &view.ids()) {
            Some(id) if id == state.me.id => Owner::Us,
            Some(id) => match view.nodes.iter().find(|n| n.id == id) {
                Some(node) => Owner::Peer(node.clone()),
                // Unreachable: the winner came out of this very list.
                None => Owner::Unknown,
            },
            None => Owner::Unknown,
        }
    }

    /// The refusal a group-addressed WRITE gets at a node that must not serve
    /// it, or `None` when this node may.
    ///
    /// Both codes are retriable and both are already on the closed retriable
    /// sets in `compat/ERRORS.md`: a client answers `NOT_COORDINATOR` by
    /// re-running FindCoordinator (which is the redirect), and
    /// `COORDINATOR_NOT_AVAILABLE` by backing off and asking again.
    ///
    /// It is applied BEFORE the coordinator is touched, so a non-owner never
    /// spawns a group actor — preserving the memory bound
    /// `Coordinator::ask_existing` was written for.
    pub fn group_guard(&self, group: &str) -> Option<ResponseError> {
        match self.owner_of_group(group) {
            Owner::Us => None,
            Owner::Peer(_) => Some(ResponseError::NotCoordinator),
            Owner::Unknown => Some(ResponseError::CoordinatorNotAvailable),
        }
    }
}

/// Who owns a group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owner {
    /// This node — and, in single mode, every group.
    Us,
    /// A peer, at the address it advertises.
    Peer(Node),
    /// The view is too old (or this node lost its id) to say. Nothing may be
    /// claimed from here.
    Unknown,
}

/// The live set of one request, and the answers derived from it.
pub struct Placement {
    nodes: Vec<Node>,
    /// The live ids, sorted: what every partition's leader is hashed over.
    /// Computed once per request rather than once per partition.
    ids: Vec<i32>,
    /// Registered replicas that are down ([`View::down`]), sorted.
    down: Vec<i32>,
    clustered: bool,
    /// Every live node holds every partition — the facade runs inside a raft
    /// broker of more than one voter ([`Placement::replicated`]).
    replicated: bool,
}

/// The replicas of one partition, as a Metadata answer lists them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replicas {
    pub leader: i32,
    /// Leader first, then every other replica — live or down — by id.
    pub replicas: Vec<i32>,
    /// Leader first, then every other LIVE replica by id.
    pub isr: Vec<i32>,
    /// The replicas that are down, by id.
    pub offline: Vec<i32>,
}

impl Placement {
    fn new(nodes: Vec<Node>, down: Vec<i32>, clustered: bool) -> Placement {
        let ids = nodes.iter().map(|n| n.id).collect();
        Placement {
            nodes,
            ids,
            down,
            clustered,
            replicated: false,
        }
    }

    /// Every broker to advertise.
    pub fn brokers(&self) -> &[Node] {
        &self.nodes
    }

    /// The registered replicas that are down, by id.
    pub fn down(&self) -> &[i32] {
        if self.replicated {
            &self.down
        } else {
            &[]
        }
    }

    /// The controller: the LOWEST live id, which is deterministic and needs no
    /// election. Nothing in this facade acts on it — no client-visible admin
    /// API is advertised — but a client that logs it must see one answer from
    /// every node rather than three.
    pub fn controller(&self) -> i32 {
        self.nodes.first().map_or(SINGLE_NODE_ID, |n| n.id)
    }

    /// The same placement, with every registered node a replica of every
    /// partition when `on` and the facade is clustered. Inside a raft broker
    /// that is the truth: each node's log holds every partition, and an
    /// acknowledged write is on a majority of them.
    pub fn replicated(mut self, on: bool) -> Placement {
        self.replicated = on && self.clustered;
        self
    }

    /// The placement of ONE topic's partitions ([`TopicPlacement`]).
    pub fn topic(&self, topic: &str) -> TopicPlacement<'_> {
        TopicPlacement {
            placement: self,
            scorer: rendezvous::PartitionScorer::new(topic),
        }
    }

    /// The replica list of one partition, leader first: the leader alone
    /// unless [`Placement::replicated`], and then every registered node.
    pub fn replicas_of(&self, topic: &str, partition: i32) -> Vec<i32> {
        self.topic(topic).replicas(partition).replicas
    }

    /// The node that leads one partition. In single mode it is node 0 without
    /// so much as a hash, which is what keeps the response byte-identical.
    pub fn leader_of(&self, topic: &str, partition: i32) -> i32 {
        self.topic(topic).leader(partition)
    }
}

/// [`Placement`] for one topic: the topic's hashing prefix computed once, so a
/// Metadata answer for a million partitions costs no allocation per partition
/// beyond the three node lists the wire format needs.
pub struct TopicPlacement<'a> {
    placement: &'a Placement,
    scorer: rendezvous::PartitionScorer,
}

impl TopicPlacement<'_> {
    /// The live node that leads `partition` (node 0 in single mode). Every
    /// node computes the same answer from the same live set.
    pub fn leader(&self, partition: i32) -> i32 {
        if !self.placement.clustered {
            return SINGLE_NODE_ID;
        }
        self.scorer
            .winner(partition, &self.placement.ids)
            .unwrap_or(SINGLE_NODE_ID)
    }

    /// The replicas of `partition`. Outside a raft broker the leader alone is
    /// its replica and its ISR; inside one every registered node is a replica,
    /// the live ones are the ISR, and the down ones are offline.
    pub fn replicas(&self, partition: i32) -> Replicas {
        let leader = self.leader(partition);
        if !self.placement.replicated {
            return Replicas {
                leader,
                replicas: vec![leader],
                isr: vec![leader],
                offline: Vec::new(),
            };
        }
        let live = &self.placement.ids;
        let down = &self.placement.down;
        let mut isr = Vec::with_capacity(live.len());
        isr.push(leader);
        isr.extend(live.iter().copied().filter(|id| *id != leader));
        let mut replicas = Vec::with_capacity(live.len() + down.len());
        replicas.push(leader);
        // Both lists are sorted, so a merge keeps the non-leader replicas in
        // id order without sorting per partition.
        let (mut a, mut b) = (live.iter().peekable(), down.iter().peekable());
        loop {
            let next = match (a.peek(), b.peek()) {
                (Some(x), Some(y)) if x <= y => a.next(),
                (Some(_), Some(_)) => b.next(),
                (Some(_), None) => a.next(),
                (None, Some(_)) => b.next(),
                (None, None) => break,
            };
            if let Some(id) = next.copied().filter(|id| *id != leader) {
                replicas.push(id);
            }
        }
        Replicas {
            leader,
            replicas,
            isr,
            offline: down.clone(),
        }
    }
}

/// A token for this PROCESS, drawn once.
///
/// `RandomState` over (the wall clock, the pid) — the same primitive
/// `secret.rs` already uses for exactly this kind of thing, so no dependency is
/// added. It is never ordered and never parsed; two of them are only ever
/// compared for equality.
pub fn new_incarnation() -> String {
    use std::hash::{BuildHasher, RandomState};
    let millis = std::time::UNIX_EPOCH
        .elapsed()
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default();
    format!(
        "{:016x}",
        RandomState::new().hash_one((millis, std::process::id()))
    )
}

#[cfg(test)]
pub mod testing {
    use super::*;

    /// A cluster state whose view is `nodes` and whose own id is `me`, fresh as
    /// of now. What a facade looks like one heartbeat after boot.
    pub fn state(nodes: &[(i32, &str, u16)], me: i32) -> Arc<ClusterState> {
        let nodes: Vec<Node> = nodes
            .iter()
            .map(|(id, host, port)| Node {
                id: *id,
                host: (*host).to_string(),
                port: *port,
                incarnation: format!("incarnation-{id}"),
                raft_node: None,
            })
            .collect();
        let mine = nodes
            .iter()
            .find(|n| n.id == me)
            .expect("the fixture's own node id is not in its view")
            .clone();
        let state = ClusterState::new(
            mine,
            "rig".to_string(),
            Duration::from_secs(2),
            Duration::from_secs(10),
        );
        state.install(nodes);
        state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn three(me: i32) -> Cluster {
        Cluster::Enabled(testing::state(
            &[
                (1, "kafka-1.example.com", 9092),
                (2, "kafka-2.example.com", 9092),
                (3, "kafka-3.example.com", 9092),
            ],
            me,
        ))
    }

    /// Single mode is not a one-node cluster: it does not hash, it does not
    /// gate, and it advertises node 0.
    #[test]
    fn single_mode_owns_everything_and_leads_everything() {
        let single = Cluster::Single;
        assert_eq!(single.owner_of_group("orders-consumer"), Owner::Us);
        assert_eq!(single.group_guard("orders-consumer"), None);
        assert_eq!(single.cluster_id(), "queen");

        let placement = single.placement("kafka.example.com", 9092);
        assert_eq!(placement.brokers().len(), 1);
        assert_eq!(placement.brokers()[0].id, SINGLE_NODE_ID);
        assert_eq!(placement.controller(), SINGLE_NODE_ID);
        for partition in [0, 1, 7, 1023] {
            assert_eq!(placement.leader_of("orders", partition), SINGLE_NODE_ID);
        }
    }

    /// THE property the whole design rests on: every node computes the same
    /// owner for the same group, and exactly one of them says "me".
    #[tokio::test]
    async fn every_node_names_the_same_owner() {
        let nodes: Vec<Cluster> = (1..=3).map(three).collect();
        for group in ["orders-consumer", "svc.billing", "g", "a group"] {
            let owners: Vec<i32> = nodes
                .iter()
                .map(|c| match c.owner_of_group(group) {
                    Owner::Us => match c {
                        Cluster::Enabled(s) => s.me.id,
                        Cluster::Single => unreachable!(),
                    },
                    Owner::Peer(n) => n.id,
                    Owner::Unknown => -1,
                })
                .collect();
            assert!(
                owners.windows(2).all(|w| w[0] == w[1]),
                "{group} has owners {owners:?}"
            );
            let mine = nodes
                .iter()
                .filter(|c| c.owner_of_group(group) == Owner::Us)
                .count();
            assert_eq!(mine, 1, "{group} is owned by {mine} nodes");
        }
    }

    /// A non-owner refuses the group RPCs with the code that IS the redirect,
    /// and the owner does not.
    #[tokio::test]
    async fn a_non_owner_answers_not_coordinator() {
        let group = "orders-consumer";
        // The pinned table says node 2 owns this one.
        assert_eq!(three(2).group_guard(group), None);
        for me in [1, 3] {
            assert_eq!(
                three(me).group_guard(group),
                Some(ResponseError::NotCoordinator),
                "node {me}"
            );
            match three(me).owner_of_group(group) {
                Owner::Peer(n) => {
                    assert_eq!(n.id, 2);
                    assert_eq!(n.host, "kafka-2.example.com");
                    assert_eq!(n.port, 9092);
                }
                other => panic!("node {me} answered {other:?}"),
            }
        }
    }

    /// The staleness gate, at exactly the TTL. A node that cannot see the live
    /// set claims nothing — and still advertises the cluster it last saw.
    #[tokio::test(start_paused = true)]
    async fn a_stale_view_claims_nothing_and_still_advertises_the_cluster() {
        let cluster = three(1);
        let state = cluster.state().unwrap().clone();
        assert!(state.coordinating());

        // One tick short of the TTL, the gate is still open.
        tokio::time::advance(state.ttl - Duration::from_millis(1)).await;
        assert!(state.coordinating());

        tokio::time::advance(Duration::from_millis(2)).await;
        assert!(!state.coordinating());
        assert_eq!(cluster.owner_of_group("orders-consumer"), Owner::Unknown);
        assert_eq!(
            cluster.group_guard("orders-consumer"),
            Some(ResponseError::CoordinatorNotAvailable)
        );
        // ...and Metadata still answers the whole cluster, because telling
        // every client the cluster shrank is worse than a view a few seconds
        // old.
        assert_eq!(
            cluster
                .placement("kafka-1.example.com", 9092)
                .brokers()
                .len(),
            3
        );

        // A successful read re-opens it.
        state.install(state.view().unwrap().nodes.clone());
        assert!(state.coordinating());
    }

    /// Losing our node id at runtime stops coordination and nothing else.
    #[tokio::test]
    async fn a_lost_node_id_stops_coordination_and_keeps_the_data_path() {
        let cluster = three(2);
        let state = cluster.state().unwrap().clone();
        assert_eq!(cluster.group_guard("orders-consumer"), None);
        state.stop_coordinating();
        assert_eq!(
            cluster.group_guard("orders-consumer"),
            Some(ResponseError::CoordinatorNotAvailable)
        );
        assert_eq!(
            cluster
                .placement("kafka-2.example.com", 9092)
                .brokers()
                .len(),
            3
        );
    }

    /// A view that never mentioned us still has us in it: we are serving, and a
    /// broker list a client cannot reach us at is the classic Kafka footgun.
    #[tokio::test]
    async fn the_view_always_contains_this_node() {
        let state = testing::state(&[(1, "kafka-1", 9092), (2, "kafka-2", 9092)], 2);
        state.install(vec![Node {
            id: 1,
            host: "kafka-1".into(),
            port: 9092,
            incarnation: "other".into(),
            raft_node: None,
        }]);
        let view = state.view().unwrap();
        assert_eq!(view.ids(), [1, 2]);
    }

    /// Two processes never draw the same incarnation, and one process draws one
    /// that is stable for its lifetime by construction (it is drawn once).
    #[test]
    fn an_incarnation_is_a_token_and_not_a_clock() {
        let a = new_incarnation();
        assert_eq!(a.len(), 16);
        assert!(a.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_ne!(a, new_incarnation());
    }
}
