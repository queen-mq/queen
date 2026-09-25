//! Cluster membership for an operator (`/api/v1/system/raft/membership`): what the
//! membership is, and the changes that keep a quorum.
//!
//! # Where a change runs
//!
//! On the leader, and only there: the leader holds each member's replication
//! progress and last acknowledgement, which every check below reads. A
//! follower sends the change over the Raft RPC port (`POST
//! /raft/v1/membership`, [`super::network`]) and relays the answer; with no
//! leader known it answers `NotLeader`, which the client retries.
//!
//! # The checks, all before anything is written
//!
//! - **One change at a time.** A change still running on this leader, a joint
//!   configuration (a change interrupted between its two steps), or a
//!   membership entry not yet committed refuses with `in_flight`. The change
//!   names the membership it was checked against
//!   (`Precondition::LastMembershipLogId`), so a change another leader made in
//!   between is refused (`membership_changed`) instead of overwritten. The one
//!   change accepted while a joint configuration stands is the one that
//!   finishes it: setting the voters to its last voter set.
//! - **Quorum.** The voters after the change must have a majority that
//!   answered an RPC within `live_within` (the leader counts as live), and so
//!   must the voters before it: a joint change commits only with both. A change
//!   that would leave voters unable to commit is refused (`no_quorum`): every
//!   write would stop, and undoing it would need that same quorum.
//! - **No empty cluster.** Removing the last voter is refused (`last_voter`).
//!   Rebuilding from one node is `QUEEN_RAFT_FORCE_RECOVER`'s job, not this.
//! - **Catch-up.** A learner becomes a voter only once it holds the log to
//!   within `promote_max_lag` entries of the leader's last one
//!   (`learner_behind`), unless `force`: a voter far behind holds back every
//!   commit whose quorum needs it.
//!
//! Every change is idempotent, so a caller that lost an answer can repeat it:
//! adding a learner already there with the same addresses, promoting a voter,
//! removing a node that is not a member, or setting the voters they already
//! are, succeeds and writes nothing.
//!
//! A voter that is removed is dropped from the membership (not kept as a
//! learner): the leader stops replicating to it and it no longer counts for
//! anything. Its process should be stopped; it must not come back with its
//! data under the same id without being added again.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use openraft::errors::{ClientWriteError, RaftError};
use openraft::raft::Precondition;
use openraft::{ChangeMembers, ServerState};
use serde::{Deserialize, Serialize};

use super::members::MembersState;
use super::types::{rsm_index, NodeId, QueenNode, TypeConfig};
use super::RaftHandle;
use crate::rsm::replicator::{MembershipChange, ReplError};
use crate::rsm::store::Store;

type Metrics = openraft::RaftMetrics<TypeConfig>;

/// `QUEEN_RAFT_PROMOTE_MAX_LAG` (default 1000): how many entries behind the
/// leader's last one a learner may be and still be promoted without `force`.
pub(crate) fn promote_max_lag_from_env() -> u64 {
    std::env::var("QUEEN_RAFT_PROMOTE_MAX_LAG")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(1000)
}

/// What the membership checks share on one node: the lock that keeps changes
/// one at a time, and the two thresholds.
/// Pauses this node's planner around a membership change: resolves once
/// nothing it proposed is still in flight, to a guard that resumes it when
/// dropped (`crate::rsm::batcher::QuiesceReq`); `None` when there is no
/// planner to pause. Installed by the facade.
pub type QuiesceHook = std::sync::Arc<
    dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<Box<dyn Send>>> + Send>>
        + Send
        + Sync,
>;

pub(crate) struct AdminCtx {
    lock: tokio::sync::Mutex<()>,
    /// See [`QuiesceHook`].
    pub(crate) quiesce: std::sync::OnceLock<QuiesceHook>,
    /// See [`promote_max_lag_from_env`].
    pub(crate) promote_max_lag: u64,
    /// A member is live when it acknowledged an RPC from the leader this
    /// recently. Three election timeouts by default: a heartbeat goes out every
    /// 100 ms, so a member silent this long is not merely slow.
    pub(crate) live_within: Duration,
}

impl AdminCtx {
    pub(crate) fn new(promote_max_lag: u64, live_within: Duration) -> AdminCtx {
        AdminCtx {
            lock: tokio::sync::Mutex::new(()),
            quiesce: std::sync::OnceLock::new(),
            promote_max_lag,
            live_within,
        }
    }
}

/// One member, as `GET /api/v1/system/raft/membership` lists it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemberStatus {
    pub node_id: NodeId,
    pub voter: bool,
    /// Raft RPC address.
    pub raft: String,
    /// Client (HTTP) address.
    pub http: String,
    /// The last entry (RSM index) the leader knows this member holds; the
    /// leader's own last entry for the leader. `None`: nothing replicated yet
    /// (a learner still receiving a snapshot, or a member never heard from).
    pub matched: Option<u64>,
    /// Entries between `matched` and the leader's last one.
    pub lag: Option<u64>,
    /// Milliseconds since the member last acknowledged an RPC of the leader
    /// (0 for the leader). On a follower's view, aged by the view's age.
    pub last_ack_ms: Option<u64>,
    /// Acknowledged within the liveness window (the leader always is).
    pub live: bool,
}

/// The membership as one node reports it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MembershipStatus {
    /// The node that took this view.
    pub node_id: NodeId,
    /// `leader`: taken by the leader from its own metrics. `follower`: this
    /// node's membership with the replication figures of the last members
    /// view the leader sent it (`viewAgeMs` old; none before the first one).
    pub source: String,
    pub view_age_ms: Option<u64>,
    pub leader: Option<NodeId>,
    pub term: u64,
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
    /// While a change is between its two steps, every voter set that must
    /// agree (the change in progress goes from the first to the last).
    pub joint: Option<Vec<Vec<NodeId>>>,
    /// The RSM index of the membership entry in effect.
    pub membership_index: Option<u64>,
    /// Whether that entry is committed (as this node knows it).
    pub membership_committed: bool,
    /// A change is unfinished (joint, or not committed): another is refused.
    pub change_in_flight: bool,
    /// This node's last log entry and commit point (RSM numbering).
    pub last_log_index: u64,
    pub committed_index: u64,
    /// The liveness window and the promotion limit the checks use.
    pub live_within_ms: u64,
    pub promote_max_lag: u64,
    pub members: Vec<MemberStatus>,
}

/// A call a follower sends the leader over `POST /raft/v1/membership`: the
/// leader's status when `change` is absent, else the change.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct AdminCall {
    #[serde(default)]
    pub change: Option<MembershipChange>,
    /// What is left of the caller's deadline.
    #[serde(default)]
    pub timeout_ms: u64,
}

/// The leader's answer to an [`AdminCall`] (always a 200: an error here is the
/// caller's to relay, not a transport failure).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct AdminAnswer {
    pub ok: bool,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    /// `code == "not_leader"`: whom the answering node believes leads.
    #[serde(default)]
    pub leader_hint: Option<NodeId>,
    #[serde(default)]
    pub status: Option<MembershipStatus>,
}

impl AdminAnswer {
    pub(crate) fn of(res: Result<MembershipStatus, ReplError>) -> AdminAnswer {
        match res {
            Ok(status) => AdminAnswer {
                ok: true,
                status: Some(status),
                ..AdminAnswer::default()
            },
            Err(e) => {
                let (code, message, hint) = match e {
                    ReplError::NotLeader { hint } => (
                        "not_leader".to_string(),
                        "this node does not lead".to_string(),
                        hint,
                    ),
                    ReplError::Refused { code, message } => (code, message, None),
                    ReplError::Timeout => (
                        "timeout".to_string(),
                        "the deadline passed before the change committed: it may still \
                         complete; read the membership before repeating it"
                            .to_string(),
                        None,
                    ),
                    ReplError::Unsupported(m) => ("unsupported".to_string(), m, None),
                    ReplError::Fatal(m) => ("fatal".to_string(), m, None),
                };
                AdminAnswer {
                    ok: false,
                    code: Some(code),
                    message: Some(message),
                    leader_hint: hint,
                    status: None,
                }
            }
        }
    }

    /// Back to the result the leader had.
    pub(crate) fn into_result(self) -> Result<MembershipStatus, ReplError> {
        if self.ok {
            return self
                .status
                .ok_or_else(|| ReplError::Fatal("the leader answered no membership".into()));
        }
        let code = self.code.unwrap_or_else(|| "fatal".into());
        let message = self.message.unwrap_or_default();
        Err(match code.as_str() {
            "not_leader" => ReplError::NotLeader {
                hint: self.leader_hint,
            },
            "timeout" => ReplError::Timeout,
            "unsupported" => ReplError::Unsupported(message),
            "fatal" => ReplError::Fatal(message),
            _ => ReplError::Refused { code, message },
        })
    }
}

/// Whether the leader's metrics show `id` acknowledging an RPC within `within`.
fn acked_ms(m: &Metrics, id: NodeId) -> Option<u64> {
    match m.heartbeat.as_ref().and_then(|h| h.get(&id)) {
        Some(Some(t)) => Some(openraft::Instant::elapsed(&**t).as_millis() as u64),
        _ => None,
    }
}

/// The membership as this node sees it now: from the leader's own metrics
/// when it leads, else this node's membership with the figures of the last
/// members view the leader sent it.
pub(crate) fn status(m: &Metrics, members: &MembersState, ctx: &AdminCtx) -> MembershipStatus {
    let mem = m.membership_config.membership();
    let configs: Vec<Vec<NodeId>> = mem
        .get_joint_config()
        .iter()
        .map(|c| c.iter().copied().collect())
        .collect();
    let voters: BTreeSet<NodeId> = mem.voter_ids().collect();
    let learners: Vec<NodeId> = mem.learner_ids().collect();
    let mlog = *m.membership_config.log_id();
    let committed = m.local_committed;
    let membership_committed = mlog <= committed;
    let last = m.last_log_index.map(rsm_index).unwrap_or(0);
    let live_ms = ctx.live_within.as_millis() as u64;
    let leading = m.state == ServerState::Leader;
    let received = if leading {
        None
    } else {
        members.received_view()
    };
    // A follower's best guess of the leader's last entry: the furthest any
    // member had replicated when the view was taken.
    let view_last = received
        .as_ref()
        .and_then(|(v, _)| v.members.iter().filter_map(|x| x.matched).max());
    let mut out = Vec::new();
    for (id, node) in mem.nodes() {
        let (matched, last_ack_ms) = if leading {
            if *id == m.id {
                (Some(last), Some(0))
            } else {
                (
                    m.replication
                        .as_ref()
                        .and_then(|r| r.get(id))
                        .and_then(|l| l.as_ref())
                        .map(|l| rsm_index(l.index)),
                    acked_ms(m, *id),
                )
            }
        } else {
            match &received {
                Some((view, age)) => match view.members.iter().find(|x| x.id == *id) {
                    Some(seen) => (
                        if *id == view.leader {
                            view_last
                        } else {
                            seen.matched
                        },
                        seen.last_ack_ms.map(|ms| ms + age.as_millis() as u64),
                    ),
                    None => (None, None),
                },
                None => (None, None),
            }
        };
        let top = if leading { Some(last) } else { view_last };
        out.push(MemberStatus {
            node_id: *id,
            voter: voters.contains(id),
            raft: node.raft.clone(),
            http: node.http.clone(),
            matched,
            lag: match (top, matched) {
                (Some(t), Some(mi)) => Some(t.saturating_sub(mi)),
                _ => None,
            },
            last_ack_ms,
            live: (leading && *id == m.id) || last_ack_ms.is_some_and(|a| a < live_ms),
        });
    }
    MembershipStatus {
        node_id: m.id,
        source: if leading { "leader" } else { "follower" }.to_string(),
        view_age_ms: if leading {
            Some(0)
        } else {
            received.as_ref().map(|(_, a)| a.as_millis() as u64)
        },
        leader: m.current_leader,
        term: m.current_term,
        voters: voters.iter().copied().collect(),
        learners,
        joint: (configs.len() > 1).then_some(configs),
        membership_index: mlog.map(|l| rsm_index(l.index)),
        membership_committed,
        change_in_flight: mem.get_joint_config().len() > 1 || !membership_committed,
        last_log_index: last,
        committed_index: committed.map(|c| rsm_index(c.index)).unwrap_or(0),
        live_within_ms: live_ms,
        promote_max_lag: ctx.promote_max_lag,
        members: out,
    }
}

/// Is `id` live, as the leader sees it: the leader itself, or acknowledged
/// within the window.
fn live(m: &Metrics, id: NodeId, within: Duration) -> bool {
    id == m.id || acked_ms(m, id).is_some_and(|ms| ms < within.as_millis() as u64)
}

/// The quorum rule: `set` has a live majority.
fn check_quorum(
    m: &Metrics,
    set: &BTreeSet<NodeId>,
    within: Duration,
    which: &str,
) -> Result<(), ReplError> {
    let live_ids: Vec<NodeId> = set
        .iter()
        .copied()
        .filter(|id| live(m, *id, within))
        .collect();
    let need = set.len() / 2 + 1;
    if live_ids.len() < need {
        return Err(ReplError::refused(
            "no_quorum",
            format!(
                "{which} voters {set:?} would have {} live member(s) ({live_ids:?} acknowledged an \
                 RPC within {} ms) and need {need} to commit anything: the cluster would stop. \
                 Bring members back, or remove dead voters one at a time while a majority is live",
                live_ids.len(),
                within.as_millis()
            ),
        ));
    }
    Ok(())
}

/// Every learner of `to_promote` holds the log to within the limit.
fn check_caught_up(
    m: &Metrics,
    to_promote: &BTreeSet<NodeId>,
    max_lag: u64,
    within: Duration,
) -> Result<(), ReplError> {
    let last = m.last_log_index.map(rsm_index).unwrap_or(0);
    for id in to_promote {
        let matched = m
            .replication
            .as_ref()
            .and_then(|r| r.get(id))
            .and_then(|l| l.as_ref())
            .map(|l| rsm_index(l.index));
        let behind = match matched {
            Some(mi) => last.saturating_sub(mi),
            None => u64::MAX,
        };
        if behind > max_lag || !live(m, *id, within) {
            return Err(ReplError::refused(
                "learner_behind",
                format!(
                    "learner {id} holds the log up to {} of the leader's {last}{} (the limit is \
                     {max_lag} entries behind, QUEEN_RAFT_PROMOTE_MAX_LAG): wait until it catches \
                     up (GET /api/v1/system/raft/membership shows its lag; a learner that needed a \
                     snapshot restarts to load it), or pass force=true",
                    matched.map_or_else(|| "nothing".to_string(), |x| x.to_string()),
                    if live(m, *id, within) {
                        ""
                    } else {
                        ", and it has not answered recently"
                    },
                ),
            ));
        }
    }
    Ok(())
}

fn check_addr(what: &str, addr: &str) -> Result<(), ReplError> {
    let ok = !addr.trim().is_empty()
        && addr
            .rsplit_once(':')
            .is_some_and(|(h, p)| !h.is_empty() && p.parse::<u16>().is_ok());
    if ok {
        Ok(())
    } else {
        Err(ReplError::refused(
            "bad_request",
            format!("{what} address `{addr}` is not host:port"),
        ))
    }
}

/// What the checks decided: nothing to do, or this change.
enum Plan {
    Nothing,
    Change {
        changes: ChangeMembers<NodeId, QueenNode>,
        retain: bool,
        what: String,
    },
}

/// Decide what `change` needs, against the leader's metrics `m`.
fn plan(m: &Metrics, ctx: &AdminCtx, change: &MembershipChange) -> Result<Plan, ReplError> {
    let mem = m.membership_config.membership();
    let joint = mem.get_joint_config();
    let voters: BTreeSet<NodeId> = mem.voter_ids().collect();
    let learners: BTreeSet<NodeId> = mem.learner_ids().collect();
    let mlog = *m.membership_config.log_id();
    if mlog > m.local_committed {
        return Err(ReplError::refused(
            "in_flight",
            format!(
                "the membership entry {} is not committed yet: a change is in flight; retry once \
                 it commits",
                mlog.map(|l| rsm_index(l.index)).unwrap_or(0)
            ),
        ));
    }
    // A joint configuration left by an interrupted change: only finishing it
    // (the voters set to its last voter set) is accepted.
    if joint.len() > 1 {
        let last: BTreeSet<NodeId> = joint.last().cloned().unwrap_or_default();
        let finishes = matches!(change, MembershipChange::SetVoters { voters: v, .. }
            if v.iter().copied().collect::<BTreeSet<_>>() == last);
        if !finishes {
            return Err(ReplError::refused(
                "in_flight",
                format!(
                    "a membership change is between its two steps (voter sets {joint:?}): finish \
                     it first with PUT /api/v1/system/raft/membership/voters {{\"voters\": {:?}}}",
                    last.iter().collect::<Vec<_>>()
                ),
            ));
        }
    }
    let within = ctx.live_within;
    match change {
        MembershipChange::AddLearner { node, raft, http } => {
            if *node == 0 {
                return Err(ReplError::refused("bad_request", "node ids start at 1"));
            }
            check_addr("raft", raft)?;
            check_addr("http", http)?;
            if voters.contains(node) {
                return Err(ReplError::refused(
                    "already_voter",
                    format!("node {node} is already a voter"),
                ));
            }
            let want = QueenNode::new(raft.trim(), http.trim());
            if let Some(have) = mem.get_node(node) {
                if *have == want {
                    return Ok(Plan::Nothing);
                }
                return Err(ReplError::refused(
                    "address_mismatch",
                    format!(
                        "node {node} is already a learner at {have}: remove it first to give it \
                         other addresses"
                    ),
                ));
            }
            Ok(Plan::Change {
                changes: ChangeMembers::AddNodes([(*node, want)].into_iter().collect()),
                retain: true,
                what: format!("add learner {node}"),
            })
        }
        MembershipChange::Promote { nodes, force } => {
            if nodes.is_empty() {
                return Err(ReplError::refused("bad_request", "no node to promote"));
            }
            let mut add = BTreeSet::new();
            for n in nodes {
                if voters.contains(n) {
                    continue;
                }
                if !learners.contains(n) {
                    return Err(ReplError::refused(
                        "not_a_learner",
                        format!(
                            "node {n} is not a member: add it as a learner first (POST \
                             /api/v1/system/raft/membership/learners)"
                        ),
                    ));
                }
                add.insert(*n);
            }
            if add.is_empty() {
                return Ok(Plan::Nothing);
            }
            if !force {
                check_caught_up(m, &add, ctx.promote_max_lag, within)?;
            }
            let target: BTreeSet<NodeId> = voters.union(&add).copied().collect();
            check_quorum(m, &voters, within, "the current")?;
            check_quorum(m, &target, within, "the new")?;
            Ok(Plan::Change {
                changes: ChangeMembers::AddVoterIds(add.clone()),
                retain: false,
                what: format!("promote {add:?}"),
            })
        }
        MembershipChange::SetVoters {
            voters: want,
            force,
        } => {
            let target: BTreeSet<NodeId> = want.iter().copied().collect();
            if target.is_empty() {
                return Err(ReplError::refused(
                    "last_voter",
                    "a cluster needs at least one voter",
                ));
            }
            for n in &target {
                if !voters.contains(n) && !learners.contains(n) {
                    return Err(ReplError::refused(
                        "not_a_member",
                        format!(
                            "node {n} is not a member: add it as a learner first (POST \
                             /api/v1/system/raft/membership/learners)"
                        ),
                    ));
                }
            }
            if target == voters && joint.len() == 1 {
                return Ok(Plan::Nothing);
            }
            let promoted: BTreeSet<NodeId> = target.difference(&voters).copied().collect();
            if !force && !promoted.is_empty() {
                check_caught_up(m, &promoted, ctx.promote_max_lag, within)?;
            }
            check_quorum(m, &voters, within, "the current")?;
            check_quorum(m, &target, within, "the new")?;
            Ok(Plan::Change {
                changes: ChangeMembers::ReplaceAllVoters(target.clone()),
                retain: false,
                what: format!("set voters {target:?}"),
            })
        }
        MembershipChange::Remove { node } => {
            if voters.contains(node) {
                let target: BTreeSet<NodeId> =
                    voters.iter().copied().filter(|v| v != node).collect();
                if target.is_empty() {
                    return Err(ReplError::refused(
                        "last_voter",
                        format!(
                            "node {node} is the last voter: removing it leaves no cluster (to \
                             rebuild a cluster from one node, see QUEEN_RAFT_FORCE_RECOVER)"
                        ),
                    ));
                }
                check_quorum(m, &voters, within, "the current")?;
                check_quorum(m, &target, within, "the remaining")?;
                Ok(Plan::Change {
                    changes: ChangeMembers::RemoveVoters([*node].into_iter().collect()),
                    retain: false,
                    what: format!("remove voter {node}"),
                })
            } else if learners.contains(node) {
                Ok(Plan::Change {
                    changes: ChangeMembers::RemoveNodes([*node].into_iter().collect()),
                    retain: false,
                    what: format!("remove learner {node}"),
                })
            } else {
                Ok(Plan::Nothing)
            }
        }
    }
}

/// Run `change` on this node, which must lead: the checks, then openraft's
/// (joint) membership change, conditional on the membership the checks saw.
/// Returns once the change committed (or there was nothing to do), with the
/// membership as it then is.
pub(crate) async fn change_on_leader<S: Store + 'static>(
    raft: &RaftHandle<S>,
    members: &MembersState,
    ctx: &AdminCtx,
    change: MembershipChange,
    deadline: Instant,
) -> Result<MembershipStatus, ReplError> {
    use openraft::async_runtime::WatchReceiver;
    let _one = ctx.lock.try_lock().map_err(|_| {
        ReplError::refused(
            "in_flight",
            "another membership change is running on this leader; retry once it answers",
        )
    })?;
    let m = raft.metrics().borrow_watched().clone();
    if m.state != ServerState::Leader {
        return Err(ReplError::NotLeader {
            hint: m.current_leader.filter(|l| *l != m.id),
        });
    }
    let (changes, retain, what) = match plan(&m, ctx, &change)? {
        Plan::Nothing => return Ok(status(&m, members, ctx)),
        Plan::Change {
            changes,
            retain,
            what,
        } => (changes, retain, what),
    };
    let observed = *m.membership_config.log_id();
    tracing::warn!(
        target: "rsm",
        change = %what,
        membership_index = observed.map(|l| rsm_index(l.index)).unwrap_or(0),
        "raft: membership change requested by an operator",
    );
    // This leader's planner proposes every entry at a PREDICTED index, and
    // openraft appends the change's config entries itself: pause the planner
    // (nothing in flight) until the change is done (batcher::QuiesceReq). Left
    // running, the first entry after the change landed two indexes late and
    // the planner stopped: every write answered 500 until leadership moved
    // (Jepsen membership nemesis, 2026-09-25).
    let paused: Option<Box<dyn Send>> = match ctx.quiesce.get() {
        Some(hook) => {
            match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), hook()).await {
                Ok(g) => g,
                Err(_elapsed) => {
                    return Err(ReplError::refused(
                        "busy",
                        "the writes in flight did not drain before the deadline; retry",
                    ))
                }
            }
        }
        None => None,
    };
    let res = tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        raft.change_membership_if(
            changes,
            retain,
            [Precondition::LastMembershipLogId {
                last_membership_log_id: observed,
            }],
        ),
    )
    .await;
    let resp = match res {
        Err(_elapsed) => {
            tracing::warn!(target: "rsm", change = %what, "raft: membership change: the deadline passed; it may still complete");
            // It may still append its final config: keep the planner paused
            // until the membership is uniform and committed again (or this
            // node stops leading; two minutes at most).
            if let Some(g) = paused {
                let raft = raft.clone();
                tokio::spawn(async move {
                    let end = Instant::now() + Duration::from_secs(120);
                    loop {
                        let m = raft.metrics().borrow_watched().clone();
                        let joint = m.membership_config.membership().get_joint_config().len() > 1;
                        let committed = *m.membership_config.log_id() <= m.local_committed;
                        if m.state != ServerState::Leader
                            || (!joint && committed)
                            || Instant::now() >= end
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    drop(g);
                });
            }
            return Err(ReplError::Timeout);
        }
        Ok(Ok(resp)) => resp,
        Ok(Err(e)) => {
            let err = map_write_error(e);
            tracing::warn!(target: "rsm", change = %what, error = %err, "raft: membership change refused");
            return Err(err);
        }
    };
    tracing::warn!(
        target: "rsm",
        change = %what,
        index = rsm_index(resp.log_id.index),
        "raft: membership change committed",
    );
    let m = raft.metrics().borrow_watched().clone();
    Ok(status(&m, members, ctx))
}

fn map_write_error(e: RaftError<TypeConfig, ClientWriteError<TypeConfig>>) -> ReplError {
    match e {
        RaftError::APIError(ClientWriteError::ForwardToLeader(f)) => {
            ReplError::NotLeader { hint: f.leader_id }
        }
        RaftError::APIError(ClientWriteError::LogEntryDiscarded(f)) => ReplError::refused(
            "leader_changed",
            format!(
                "leadership moved while the change was being written (new leader {:?}): it may \
                 still commit; read the membership before repeating it",
                f.leader_id
            ),
        ),
        RaftError::APIError(ClientWriteError::PreconditionFailed(p)) => ReplError::refused(
            "membership_changed",
            format!("the membership changed while this change was checked ({p}); retry"),
        ),
        RaftError::APIError(ClientWriteError::ChangeMembershipError(c)) => {
            let code = match &c {
                openraft::errors::ChangeMembershipError::InProgress(_) => "in_flight",
                openraft::errors::ChangeMembershipError::EmptyMembership(_) => "last_voter",
                openraft::errors::ChangeMembershipError::LearnerNotFound(_) => "not_a_learner",
                _ => "membership_invalid",
            };
            ReplError::refused(code, c.to_string())
        }
        RaftError::Fatal(f) => ReplError::Fatal(f.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_answer_round_trips_every_error_kind() {
        for e in [
            ReplError::NotLeader { hint: Some(3) },
            ReplError::Timeout,
            ReplError::Unsupported("no network".into()),
            ReplError::Fatal("gone".into()),
            ReplError::refused("no_quorum", "x"),
        ] {
            let a = AdminAnswer::of(Err(e.clone()));
            let wire = serde_json::to_vec(&a).unwrap();
            let back: AdminAnswer = serde_json::from_slice(&wire).unwrap();
            assert_eq!(back.into_result().unwrap_err(), e);
        }
        let ok = AdminAnswer::of(Ok(MembershipStatus {
            node_id: 2,
            voters: vec![1, 2],
            ..MembershipStatus::default()
        }));
        let back: AdminAnswer = serde_json::from_slice(&serde_json::to_vec(&ok).unwrap()).unwrap();
        assert_eq!(back.into_result().unwrap().voters, vec![1, 2]);
    }

    #[test]
    fn a_change_forwards_as_tagged_json() {
        let c = MembershipChange::AddLearner {
            node: 4,
            raft: "10.0.0.4:7400".into(),
            http: "10.0.0.4:6632".into(),
        };
        let call = AdminCall {
            change: Some(c.clone()),
            timeout_ms: 5000,
        };
        let json = serde_json::to_string(&call).unwrap();
        assert!(json.contains("\"op\":\"add_learner\""), "{json}");
        let back: AdminCall = serde_json::from_str(&json).unwrap();
        assert_eq!(back.change, Some(c));
        let p: MembershipChange = serde_json::from_str(r#"{"op":"promote","nodes":[4]}"#).unwrap();
        assert_eq!(
            p,
            MembershipChange::Promote {
                nodes: vec![4],
                force: false
            }
        );
    }

    #[test]
    fn addresses_must_be_host_and_port() {
        assert!(check_addr("raft", "10.0.0.4:7400").is_ok());
        assert!(check_addr("raft", "queen-3.queen:7400").is_ok());
        for bad in ["", "10.0.0.4", ":7400", "h:port", "h:70000"] {
            assert!(check_addr("raft", bad).is_err(), "{bad}");
        }
    }

    // -- the checks, on a leader's metrics built by hand --------------------

    use std::collections::BTreeMap;
    use std::sync::Arc;

    use openraft::type_config::TypeConfigExt;

    use super::super::types::{log_id, Membership, StoredMembership};

    /// Node 1 leads `configs` (joint when more than one) plus `learners`;
    /// `live` acknowledged just now, the rest never; `matched` per member
    /// (RSM); the leader's last entry is `last` (RSM); the membership entry
    /// is at RSM 10, committed or not.
    struct Leader<'a> {
        configs: &'a [&'a [u64]],
        learners: &'a [u64],
        live: &'a [u64],
        matched: &'a [(u64, u64)],
        last: u64,
        committed: bool,
    }

    impl Leader<'_> {
        fn metrics(&self) -> Metrics {
            let mut nodes = BTreeMap::new();
            for id in self
                .configs
                .iter()
                .flat_map(|c| c.iter())
                .chain(self.learners)
            {
                nodes.insert(
                    *id,
                    QueenNode::new(format!("n{id}:7400"), format!("n{id}:6632")),
                );
            }
            let configs: Vec<BTreeSet<u64>> = self
                .configs
                .iter()
                .map(|c| c.iter().copied().collect())
                .collect();
            let mut m = Metrics::new_initial(1);
            m.state = ServerState::Leader;
            m.current_leader = Some(1);
            m.current_term = 3;
            m.membership_config = Arc::new(StoredMembership::new(
                Some(log_id(3, 9)),
                Membership::new(configs, nodes.clone()).expect("membership"),
            ));
            m.last_log_index = Some(self.last - 1);
            m.local_committed = Some(log_id(3, if self.committed { self.last - 1 } else { 8 }));
            m.replication = Some(
                nodes
                    .keys()
                    .filter(|id| **id != 1)
                    .map(|id| {
                        let at = self
                            .matched
                            .iter()
                            .find(|(n, _)| n == id)
                            .map(|(_, i)| log_id(3, i - 1));
                        (*id, at)
                    })
                    .collect(),
            );
            m.heartbeat = Some(
                nodes
                    .keys()
                    .filter(|id| **id != 1)
                    .map(|id| {
                        let t = self
                            .live
                            .contains(id)
                            .then(|| openraft::metrics::SerdeInstant::new(TypeConfig::now()));
                        (*id, t)
                    })
                    .collect(),
            );
            m
        }
    }

    fn ctx() -> AdminCtx {
        AdminCtx::new(100, Duration::from_secs(2))
    }

    fn code(r: Result<Plan, ReplError>) -> String {
        match r {
            Err(ReplError::Refused { code, .. }) => code,
            Err(e) => panic!("expected a refusal, got {e:?}"),
            Ok(Plan::Nothing) => "nothing".into(),
            Ok(Plan::Change { what, .. }) => format!("change: {what}"),
        }
    }

    const THREE: &[&[u64]] = &[&[1, 2, 3]];

    #[test]
    fn a_change_is_refused_while_another_is_in_flight() {
        // The membership entry is not committed yet.
        let m = Leader {
            configs: THREE,
            learners: &[],
            live: &[2, 3],
            matched: &[(2, 50), (3, 50)],
            last: 50,
            committed: false,
        }
        .metrics();
        let rm = MembershipChange::Remove { node: 3 };
        assert_eq!(code(plan(&m, &ctx(), &rm)), "in_flight");
        // A joint configuration left by an interrupted change: only finishing
        // it is accepted.
        let m = Leader {
            configs: &[&[1, 2, 3], &[1, 2, 4]],
            learners: &[],
            live: &[2, 3, 4],
            matched: &[(2, 50), (3, 50), (4, 50)],
            last: 50,
            committed: true,
        }
        .metrics();
        assert_eq!(code(plan(&m, &ctx(), &rm)), "in_flight");
        let finish = MembershipChange::SetVoters {
            voters: vec![1, 2, 4],
            force: false,
        };
        assert!(code(plan(&m, &ctx(), &finish)).starts_with("change: set voters"));
        // And the status says so.
        let st = status(&m, &MembersState::default(), &ctx());
        assert!(st.change_in_flight, "{st:?}");
        assert_eq!(st.joint, Some(vec![vec![1, 2, 3], vec![1, 2, 4]]));
    }

    #[test]
    fn no_change_may_leave_the_voters_without_a_live_majority() {
        // Node 3 is dead: removing live node 2 leaves {1, 3} with one live.
        let m = Leader {
            configs: THREE,
            learners: &[],
            live: &[2],
            matched: &[(2, 50), (3, 20)],
            last: 50,
            committed: true,
        }
        .metrics();
        assert_eq!(
            code(plan(&m, &ctx(), &MembershipChange::Remove { node: 2 })),
            "no_quorum"
        );
        assert_eq!(
            code(plan(&m, &ctx(), &MembershipChange::Remove { node: 3 })),
            "change: remove voter 3"
        );
        // Setting the voters to two dead-or-new members and the leader.
        let m = Leader {
            configs: THREE,
            learners: &[4],
            live: &[2, 3],
            matched: &[(2, 50), (3, 50), (4, 50)],
            last: 50,
            committed: true,
        }
        .metrics();
        let set = MembershipChange::SetVoters {
            voters: vec![1, 4, 5],
            force: true,
        };
        assert_eq!(code(plan(&m, &ctx(), &set)), "not_a_member");
        let set = MembershipChange::SetVoters {
            voters: vec![1, 3, 4],
            force: true,
        };
        // 4 is a learner that never answered: {1, 3, 4} has 1 and 3 live, 2 of 3.
        assert_eq!(code(plan(&m, &ctx(), &set)), "change: set voters {1, 3, 4}");
    }

    #[test]
    fn the_last_voter_stays() {
        let m = Leader {
            configs: &[&[1]],
            learners: &[2],
            live: &[2],
            matched: &[(2, 50)],
            last: 50,
            committed: true,
        }
        .metrics();
        assert_eq!(
            code(plan(&m, &ctx(), &MembershipChange::Remove { node: 1 })),
            "last_voter"
        );
        let none = MembershipChange::SetVoters {
            voters: vec![],
            force: false,
        };
        assert_eq!(code(plan(&m, &ctx(), &none)), "last_voter");
        // A learner goes without a quorum question.
        assert_eq!(
            code(plan(&m, &ctx(), &MembershipChange::Remove { node: 2 })),
            "change: remove learner 2"
        );
        // Not a member: nothing to do.
        assert_eq!(
            code(plan(&m, &ctx(), &MembershipChange::Remove { node: 9 })),
            "nothing"
        );
    }

    #[test]
    fn a_learner_far_behind_is_promoted_only_with_force() {
        let behind = Leader {
            configs: THREE,
            learners: &[4],
            live: &[2, 3, 4],
            matched: &[(2, 5000), (3, 5000), (4, 4000)],
            last: 5000,
            committed: true,
        }
        .metrics();
        let promote = |force| MembershipChange::Promote {
            nodes: vec![4],
            force,
        };
        assert_eq!(
            code(plan(&behind, &ctx(), &promote(false))),
            "learner_behind"
        );
        assert!(code(plan(&behind, &ctx(), &promote(true))).starts_with("change: promote"));
        // Caught up (within 100 entries): no force needed.
        let close = Leader {
            matched: &[(2, 5000), (3, 5000), (4, 4950)],
            ..behind_spec()
        }
        .metrics();
        assert!(code(plan(&close, &ctx(), &promote(false))).starts_with("change: promote"));
        // Promoting a voter changes nothing; a stranger is refused.
        let voter = MembershipChange::Promote {
            nodes: vec![2],
            force: false,
        };
        assert_eq!(code(plan(&close, &ctx(), &voter)), "nothing");
        let stranger = MembershipChange::Promote {
            nodes: vec![7],
            force: false,
        };
        assert_eq!(code(plan(&close, &ctx(), &stranger)), "not_a_learner");
    }

    fn behind_spec() -> Leader<'static> {
        Leader {
            configs: THREE,
            learners: &[4],
            live: &[2, 3, 4],
            matched: &[(2, 5000), (3, 5000), (4, 4000)],
            last: 5000,
            committed: true,
        }
    }

    #[test]
    fn adding_a_learner_is_idempotent_and_keeps_its_addresses() {
        let m = behind_spec().metrics();
        let add = |raft: &str| MembershipChange::AddLearner {
            node: 4,
            raft: raft.into(),
            http: "n4:6632".into(),
        };
        assert_eq!(code(plan(&m, &ctx(), &add("n4:7400"))), "nothing");
        assert_eq!(code(plan(&m, &ctx(), &add("n9:7400"))), "address_mismatch");
        let voter = MembershipChange::AddLearner {
            node: 2,
            raft: "n2:7400".into(),
            http: "n2:6632".into(),
        };
        assert_eq!(code(plan(&m, &ctx(), &voter)), "already_voter");
        let new = MembershipChange::AddLearner {
            node: 5,
            raft: "n5:7400".into(),
            http: "n5:6632".into(),
        };
        assert_eq!(code(plan(&m, &ctx(), &new)), "change: add learner 5");
    }
}
