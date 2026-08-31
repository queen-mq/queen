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

/// The row this node writes for itself.
fn value_of(me: &Node) -> serde_json::Value {
    json!({
        "nodeId": me.id,
        "host": me.host,
        "port": me.port,
        "incarnation": me.incarnation,
        "since": std::time::UNIX_EPOCH
            .elapsed()
            .map(|d| d.as_millis() as i64)
            .unwrap_or_default(),
        "version": env!("CARGO_PKG_VERSION"),
    })
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
    let ttl = state.ttl.as_secs().max(1);
    let ops = [
        KvOp::put_ttl(
            NAMESPACE,
            &node_key(&state.cluster, state.me.id),
            value_of(&state.me),
            ttl,
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
        state.install(answered.nodes);
        return Ok(answered.version);
    }
    watch_and_adopt(api, state, token, answered).await
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
            state.install(answered.nodes);
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
    state.install(answered.nodes);
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
    heartbeat: tokio::task::JoinHandle<()>,
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
    pub async fn deregister(self, budget: Duration) -> Departure {
        self.heartbeat.abort();
        let _ = self.heartbeat.await;
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
pub fn spawn(
    api: Arc<dyn QueenApi>,
    state: Arc<ClusterState>,
    token: Option<String>,
    version: Option<i64>,
) -> Registration {
    let held = Held::new(version);
    let heartbeat = tokio::spawn({
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
    });
    Registration {
        api,
        state,
        token,
        held,
        heartbeat,
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
                incarnation: "i".into()
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
}
