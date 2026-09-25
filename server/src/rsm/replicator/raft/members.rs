//! Who is in the cluster, and when the raft LEADER last heard from each
//! member (`GET /api/v1/raft/liveness`).
//!
//! openraft's leader records, per member, the sending time of the last RPC the
//! member acknowledged — a heartbeat or an append (`RaftMetrics::heartbeat`) —
//! and it replicates several times a second whatever the client load. That is
//! the cluster's one trustworthy liveness signal. A liveness signal that rode
//! the client pipeline — the Kafka facade's node registry was a KV write —
//! stalls exactly when the data path is busiest: a leader warming 100k
//! partitions saw every follower's renewal time out and took itself for the
//! only node.
//!
//! ## How the view reaches the followers: on the appends themselves
//!
//! The leader PUBLISHES its view ([`MembersState::publish`], from the watch
//! task, at most every [`PUBLISH_EVERY`]), and every AppendEntries it sends —
//! heartbeats included — carries the latest one in the [`MEMBERS_HEADER`]
//! header, with its age ([`MembersState::header`]). The follower's append
//! handler keeps the last one it received ([`MembersState::receive`]). So the
//! view travels on the one RPC stream that IS the liveness evidence: a follower
//! that receives appends has a fresh view, and one that does not is not healthy
//! anyway. No connection of its own, no pull, nothing a data-path queue can
//! hold up. (A pull over the follower's pooled RPC client was tried first and
//! starved: warming 500k partitions, the followers' forwarded commands held
//! every ephemeral port to the leader's RPC port, and the pull could not
//! connect.)
//!
//! The leader answers from its own metrics ([`MembersState::leader_view`]); a
//! follower serves the copy it received, with the copy's age, so every node
//! reports ONE authority's observations — which is what lets the facades on
//! different nodes agree about who is live. A follower cut off from the leader
//! serves a copy that ages, and a consumer that sees the age knows not to judge
//! by it.

use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use openraft::ServerState;

use super::types::{rsm_index, NodeId, TypeConfig};
use crate::rsm::replicator::{ClusterMembers, MemberSeen, MembersView};

/// The header an AppendEntries carries the leader's view in:
/// `<term>;<age of the view in ms>;<MembersView JSON>`. The term comes first so
/// a follower can drop a deposed leader's view without parsing it.
pub(crate) const MEMBERS_HEADER: &str = "x-queen-raft-members";

/// How often the leader re-takes its view. Appends go out far more often; they
/// carry the last one taken, with its age.
pub(crate) const PUBLISH_EVERY: Duration = Duration::from_millis(100);

/// The members' view this node can serve: when it took office and the view it
/// publishes while it leads, and the last view received from a leader while it
/// follows.
#[derive(Default)]
pub(crate) struct MembersState {
    /// `(term, when this node became leader of it)` while it leads.
    leading_since: Mutex<Option<(u64, Instant)>>,
    /// The view this node last took as leader: its term, the serialized view,
    /// and when it was taken.
    published: RwLock<Option<(u64, String, Instant)>>,
    /// The last header value received on an append, its term, and when it
    /// arrived. Parsed only when read ([`MembersState::members`]): the append
    /// path reads the term and stores bytes, nothing else.
    received: Mutex<Option<(u64, Vec<u8>, Instant)>>,
}

/// How long ago the leader last heard from one member, in milliseconds.
///
/// `acked` is openraft's own figure — the last RPC the member acknowledged in
/// this leader's term — and it wins whenever there is one. A member with none
/// has not answered since this node took office: its silence is at LEAST the
/// time since the election (`since_election`), and for the PREVIOUS leader it
/// is the time since its last append reached this node (`heard_as_leader`) —
/// which began before the election, so a leader that died is dropped one TTL
/// after it stopped sending, not one TTL after its successor was elected.
fn silence_ms(
    acked: Option<u64>,
    since_election: Option<u64>,
    heard_as_leader: Option<u64>,
) -> Option<u64> {
    acked.or(match (since_election, heard_as_leader) {
        (Some(e), Some(h)) => Some(e.max(h)),
        (e, h) => e.or(h),
    })
}

impl MembersState {
    /// Keep the leadership stamp and the published view in step with the
    /// metrics. Called by the watch task on every metrics change.
    pub(crate) fn note(&self, m: &openraft::RaftMetrics<TypeConfig>) {
        let leading = m.state == ServerState::Leader;
        {
            let mut g = self.leading_since.lock().expect("leading_since");
            match (*g, leading) {
                (Some((term, _)), true) if term == m.current_term => {}
                (_, true) => *g = Some((m.current_term, Instant::now())),
                (Some(_), false) => *g = None,
                (None, false) => {}
            }
        }
        self.publish(m);
    }

    /// Re-take the view this node publishes on its appends — while it leads,
    /// at most every [`PUBLISH_EVERY`] (the metrics change on every commit, and
    /// the view is not rebuilt for each); a node that does not lead publishes
    /// nothing.
    fn publish(&self, m: &openraft::RaftMetrics<TypeConfig>) {
        if m.state != ServerState::Leader {
            let mut g = self.published.write().expect("published");
            if g.is_some() {
                *g = None;
            }
            return;
        }
        let fresh = self
            .published
            .read()
            .expect("published")
            .as_ref()
            .is_some_and(|(term, _, at)| *term == m.current_term && at.elapsed() < PUBLISH_EVERY);
        if fresh {
            return;
        }
        if let Some(view) = self.leader_view(m) {
            if let Ok(json) = serde_json::to_string(&view) {
                *self.published.write().expect("published") =
                    Some((view.term, json, Instant::now()));
            }
        }
    }

    /// The header value an append carries: the published view's term, its age
    /// as of now, and the view. `None` when this node publishes nothing.
    pub(crate) fn header(&self) -> Option<axum::http::HeaderValue> {
        let g = self.published.read().expect("published");
        let (term, json, at) = g.as_ref()?;
        axum::http::HeaderValue::from_str(&format!("{term};{};{json}", at.elapsed().as_millis()))
            .ok()
    }

    /// Keep a view received on an append — unless it is from an older term
    /// than one already received. The header is read BEFORE openraft judges
    /// the append, so a deposed leader still reaching this node must not
    /// replace the new leader's view with its own stale one. Bytes and the
    /// term only: the view is parsed when read.
    pub(crate) fn receive(&self, value: &[u8]) {
        let Some(term) = std::str::from_utf8(value)
            .ok()
            .and_then(|t| t.split_once(';'))
            .and_then(|(term, _)| term.parse::<u64>().ok())
        else {
            return;
        };
        let mut g = self.received.lock().expect("members received");
        if g.as_ref().is_some_and(|(held, _, _)| *held > term) {
            return;
        }
        *g = Some((term, value.to_vec(), Instant::now()));
    }

    /// The last received view and how old it is now: its age when it was sent
    /// plus the time since it arrived. `None` before the first one, or when it
    /// does not parse.
    fn received(&self) -> Option<(MembersView, Duration)> {
        let (_, raw, at) = self.received.lock().expect("members received").clone()?;
        let text = std::str::from_utf8(&raw).ok()?;
        let (_term, rest) = text.split_once(';')?;
        let (age, json) = rest.split_once(';')?;
        let age = Duration::from_millis(age.parse().ok()?);
        let view = serde_json::from_str::<MembersView>(json).ok()?;
        Some((view, age + at.elapsed()))
    }

    /// On a follower: the openraft index every member the LEADER still counts
    /// as live has replicated, from the last view received — the floor the
    /// leader purges by ([`super::replicated_floor`]). A follower that purges
    /// below it and is then elected must send a snapshot to a member the old
    /// leader was still serving from its log (measured: a follower away for
    /// 43 s, under a 600 s hold, got a 5.9 MB snapshot and a restart once
    /// leadership moved). A member is live while the leader heard from it
    /// within `hold`, counted to now: the view's figure plus the view's age.
    /// This node and the leader are left out: neither needs the log from here.
    /// `None` before the first view (or when it does not parse): the caller
    /// then purges nothing.
    pub(crate) fn follower_floor(&self, me: NodeId, hold: Duration) -> Option<u64> {
        let (view, age) = self.received()?;
        let mut floor = u64::MAX;
        for m in &view.members {
            if m.id == me || m.id == view.leader {
                continue;
            }
            let live = m
                .last_ack_ms
                .is_some_and(|ms| Duration::from_millis(ms) + age < hold);
            if live {
                // The view counts in RSM numbering; the purge in openraft
                // indexes (RSM = openraft + 1).
                floor = floor.min(m.matched.map_or(0, |i| i.saturating_sub(1)));
            }
        }
        Some(floor)
    }

    /// The PREVIOUS leader, as this node last heard from it while following:
    /// its id and how long ago its last append arrived here.
    fn previous_leader(&self) -> Option<(NodeId, u64)> {
        let (_, raw, at) = self.received.lock().expect("members received").clone()?;
        let text = std::str::from_utf8(&raw).ok()?;
        let (_, rest) = text.split_once(';')?;
        let (_, json) = rest.split_once(';')?;
        let view = serde_json::from_str::<MembersView>(json).ok()?;
        Some((view.leader, at.elapsed().as_millis() as u64))
    }

    /// The view as this node sees it NOW, when it leads: every member of the
    /// membership, with how long ago it last acknowledged an RPC
    /// ([`silence_ms`] for a member not heard from since this leader took
    /// office). `None` when this node does not lead.
    pub(crate) fn leader_view(&self, m: &openraft::RaftMetrics<TypeConfig>) -> Option<MembersView> {
        if m.state != ServerState::Leader {
            return None;
        }
        let since = self
            .leading_since
            .lock()
            .expect("leading_since")
            .filter(|(term, _)| *term == m.current_term)
            .map(|(_, at)| at.elapsed().as_millis() as u64);
        let previous = self.previous_leader();
        let membership = m.membership_config.membership();
        let voters: std::collections::BTreeSet<NodeId> = membership.voter_ids().collect();
        let members = membership
            .nodes()
            .map(|(id, node)| {
                let last_ack_ms = if *id == m.id {
                    Some(0)
                } else {
                    let acked = match m.heartbeat.as_ref().and_then(|h| h.get(id)) {
                        Some(Some(t)) => Some(openraft::Instant::elapsed(&**t).as_millis() as u64),
                        _ => None,
                    };
                    let heard_as_leader = previous.filter(|(p, _)| p == id).map(|(_, ms)| ms);
                    silence_ms(acked, since, heard_as_leader)
                };
                MemberSeen {
                    id: *id,
                    voter: voters.contains(id),
                    http: node.http.clone(),
                    raft: node.raft.clone(),
                    last_ack_ms,
                    matched: m
                        .replication
                        .as_ref()
                        .and_then(|r| r.get(id))
                        .and_then(|l| l.as_ref())
                        .map(|l| rsm_index(l.index)),
                }
            })
            .collect();
        Some(MembersView {
            leader: m.id,
            term: m.current_term,
            members,
        })
    }

    /// What this node knows: the leader's view taken now when it leads, the
    /// received copy and its age when it follows, no view before the first
    /// append.
    pub(crate) fn members(
        &self,
        m: Option<&openraft::RaftMetrics<TypeConfig>>,
        node_id: NodeId,
    ) -> ClusterMembers {
        let Some(m) = m else {
            return ClusterMembers {
                node_id,
                leader: None,
                term: 0,
                view: None,
                view_age: None,
            };
        };
        if let Some(view) = self.leader_view(m) {
            return ClusterMembers {
                node_id,
                leader: Some(m.id),
                term: m.current_term,
                view: Some(view),
                view_age: Some(Duration::ZERO),
            };
        }
        let (view, view_age) = match self.received() {
            Some((view, age)) => (Some(view), Some(age)),
            None => (None, None),
        };
        ClusterMembers {
            node_id,
            leader: m.current_leader,
            term: m.current_term,
            view,
            view_age,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(leader: NodeId) -> MembersView {
        MembersView {
            leader,
            term: 7,
            members: (1..=3)
                .map(|id| MemberSeen {
                    id,
                    voter: true,
                    http: format!("10.0.0.{id}:6632"),
                    raft: format!("10.0.0.{id}:7400"),
                    last_ack_ms: Some(id * 10),
                    matched: Some(99),
                })
                .collect(),
        }
    }

    /// A leader of `term` that published `view(leader)` `ago` ago.
    fn published(leader: NodeId, term: u64, ago: Duration) -> MembersState {
        let state = MembersState::default();
        let mut v = view(leader);
        v.term = term;
        let json = serde_json::to_string(&v).unwrap();
        *state.published.write().unwrap() = Some((term, json, Instant::now() - ago));
        state
    }

    /// What a leader publishes is what a follower reads back, aged by the
    /// time it spent in flight AND the time since it arrived.
    #[test]
    fn a_published_view_round_trips_through_the_append_header_with_its_age() {
        let leader = published(1, 7, Duration::from_millis(250));
        let header = leader.header().expect("a leader publishes a header");

        let follower = MembersState::default();
        follower.receive(header.as_bytes());
        let (got, age) = follower.received().expect("the follower keeps it");
        assert_eq!(got, view(1));
        assert!(age >= Duration::from_millis(250), "{age:?}");
        assert!(age < Duration::from_secs(5), "{age:?}");

        // A node that does not lead publishes nothing, and an unreadable
        // header is no view rather than a wrong one — nor does it replace the
        // good one.
        assert!(MembersState::default().header().is_none());
        follower.receive(b"not a view");
        assert_eq!(follower.received().unwrap().0, view(1));
    }

    /// openraft's figure wins; without one a member is silent at least since
    /// the election, and the PREVIOUS leader since its last append arrived —
    /// which is what drops a dead leader one TTL after it died rather than one
    /// TTL after its successor was elected.
    #[test]
    fn a_dead_leaders_silence_counts_from_its_last_append_not_the_election() {
        assert_eq!(silence_ms(Some(80), Some(5_000), Some(9_000)), Some(80));
        assert_eq!(silence_ms(None, Some(1_500), Some(3_000)), Some(3_000));
        assert_eq!(silence_ms(None, Some(1_500), None), Some(1_500));
        assert_eq!(silence_ms(None, None, Some(3_000)), Some(3_000));
        assert_eq!(silence_ms(None, None, None), None);
        // The previous leader is looked up in the last view received.
        let follower = MembersState::default();
        follower.receive(published(2, 9, Duration::ZERO).header().unwrap().as_bytes());
        let (id, ms) = follower.previous_leader().unwrap();
        assert_eq!(id, 2);
        assert!(ms < 1_000, "{ms}");
    }

    /// A follower purges no deeper than its leader: the lowest index held by a
    /// member the leader heard from within `hold` — itself and the leader left
    /// out, a silent member dropped once its silence (plus the view's age)
    /// reaches `hold`, openraft numbering (RSM - 1). No view: no floor to go
    /// by, so the caller keeps the whole log.
    #[test]
    fn a_follower_purges_no_deeper_than_what_live_members_hold() {
        let hold = Duration::from_secs(600);
        assert_eq!(MembersState::default().follower_floor(2, hold), None);

        let leader = published(1, 7, Duration::ZERO);
        {
            let mut v = view(1);
            v.members[1].matched = Some(5_000); // node 2, the follower asking
            v.members[2].matched = Some(1_227); // node 3, behind
            v.members[0].matched = Some(9_000); // node 1, the leader
            *leader.published.write().unwrap() =
                Some((7, serde_json::to_string(&v).unwrap(), Instant::now()));
        }
        let follower = MembersState::default();
        follower.receive(leader.header().unwrap().as_bytes());
        // Node 3 holds 1227 (RSM) = 1226 (openraft): node 2 keeps that much.
        assert_eq!(follower.follower_floor(2, hold), Some(1_226));
        // Node 3 asking: node 2 is the only other non-leader.
        assert_eq!(follower.follower_floor(3, hold), Some(4_999));

        // Node 3 silent past the hold (30 ms of silence + the view's age):
        // the leader has stopped holding the log for it, and so does node 2.
        assert_eq!(
            follower.follower_floor(2, Duration::from_millis(20)),
            Some(u64::MAX)
        );
    }

    /// The header is read before openraft judges the append, so a deposed
    /// leader still reaching this node must not replace the new leader's view
    /// with its own: a view of an older term is dropped, the same or a newer
    /// one is kept.
    #[test]
    fn a_deposed_leaders_view_does_not_replace_the_new_leaders() {
        let follower = MembersState::default();
        let new = published(2, 9, Duration::ZERO).header().unwrap();
        let old = published(1, 8, Duration::ZERO).header().unwrap();
        follower.receive(new.as_bytes());
        follower.receive(old.as_bytes());
        assert_eq!(follower.received().unwrap().0.leader, 2);
        let newer = published(3, 10, Duration::ZERO).header().unwrap();
        follower.receive(newer.as_bytes());
        assert_eq!(follower.received().unwrap().0.leader, 3);
    }
}
