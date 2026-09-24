//! Which facades are LIVE when the facade runs inside a raft broker.
//!
//! ## Why the registry's TTL cannot be the liveness signal there
//!
//! Outside raft a node is live while its registry row is: every heartbeat
//! rewrites the row with a TTL, and a row that is not rewritten expires. Inside
//! a raft broker that rewrite is a KV WRITE, and a KV write rides the same
//! pipeline as every push — planned, proposed, committed and applied behind the
//! data path. On a 3-node cluster warming 100k partitions the followers' writes
//! waited past their 10 s budget (`kv_unavailable`/`kv_timeout`) for over a
//! minute, their rows expired, and the raft leader advertised itself as the only
//! broker. Clients that refreshed Metadata in that window pinned every partition
//! to one node for their whole `metadata.max.age.ms`, and a lone node reports
//! replicas = ISR = itself, so readiness checks passed.
//!
//! ## What says a node is live instead
//!
//! Raft itself. The raft leader records, per member, when it last had an RPC
//! (heartbeat or replication) acknowledged, and replicates to every follower
//! several times a second whatever the load — an acknowledged append IS proof of
//! life, and a follower that stops answering stops producing them. That record
//! travels over the raft RPC port, on openraft's own runtime, and never through
//! the pipeline or an admission gate. The broker serves it as
//! `GET /api/v1/raft/liveness` ([`crate::queen::RaftMembers`]): the leader
//! answers from its own record, a follower from the copy it fetched from the
//! leader, with the copy's age. Every node therefore judges by ONE authority's
//! observations, which is what keeps their broker lists — and the rendezvous
//! ownership computed over them — in agreement.
//!
//! The registry stays, as a DIRECTORY: which node id lives at which address on
//! which raft member. Its rows are written with a long TTL
//! ([`super::RAFT_ROW_TTL_FACTOR`]), because an expired row would drop a live
//! node exactly as before; a row that outlives its process is harmless, since
//! raft says that node is dead.
//!
//! ## The rule, with its hysteresis
//!
//!   * A node whose last acknowledgement is older than the TTL is DOWN. The TTL
//!     is `QUEEN_KAFKA_CLUSTER_TTL_MS` (10 s): a hundred raft heartbeats, so one
//!     late beat — or fifty — drops nobody.
//!   * A node that was down comes back only once it has been heard within HALF
//!     the TTL, so a node answering at the edge of the threshold does not flap
//!     in and out of every client's broker list.
//!   * A node seen for the first time is live if it was heard within the TTL.
//!   * When the view itself is older than half the TTL — a follower that cannot
//!     reach the leader, or an election in progress — nothing is judged: the
//!     last live set stands, and the coordination gate
//!     ([`super::ClusterState::coordinating`]) closes once it is a TTL old, the
//!     same way it does when the registry cannot be read.
//!
//! A down node that is a raft VOTER stays in every partition's replica list and
//! leaves the ISR ([`super::View::down`]); a node that is not a raft member at
//! all (a row left behind by a node that was removed) is in neither.

use std::collections::HashMap;
use std::time::Duration;

use super::Node;
use crate::queen::RaftMembers;

/// The judge's memory between two judgements: who was live, for the
/// hysteresis.
#[derive(Debug)]
pub struct Judge {
    ttl: Duration,
    live: HashMap<i32, bool>,
}

impl Judge {
    pub fn new(ttl: Duration) -> Judge {
        Judge {
            ttl,
            live: HashMap::new(),
        }
    }

    /// The live set and the down set, from the directory `rows` and the raft
    /// `members` view — or `None` when the view is too old to judge by.
    ///
    /// `me` is always live and is not judged: a facade that is serving is
    /// reachable in the broker list it hands out (the same rule
    /// [`super::ClusterState::install_view`] applies).
    pub fn judge(
        &mut self,
        me: &Node,
        rows: &[Node],
        members: &RaftMembers,
    ) -> Option<(Vec<Node>, Vec<Node>)> {
        let ttl = self.ttl.as_millis() as u64;
        let fresh = members.view_age_ms.is_some_and(|age| age <= ttl / 2);
        if !fresh {
            return None;
        }
        let mut live = vec![me.clone()];
        let mut down = Vec::new();
        let mut seen: HashMap<i32, bool> = HashMap::with_capacity(rows.len());
        for row in rows {
            if row.id == me.id || seen.contains_key(&row.id) {
                continue;
            }
            let Some(raft_node) = row.raft_node else {
                // A row written before its writer knew its raft node is judged
                // the registry's way — present is live — and it carries the
                // short TTL that makes that true ([`super::ClusterState::row_ttl`]).
                seen.insert(row.id, true);
                live.push(row.clone());
                continue;
            };
            let Some(member) = members.members.iter().find(|m| m.node_id == raft_node) else {
                // Not a raft member: a row left behind by a node that was
                // removed. Neither live nor a replica.
                continue;
            };
            let was = self.live.get(&row.id).copied();
            let is = match member.last_ack_ms {
                // Never heard from, as far as this view knows: keep what was
                // decided, and give a node never judged the benefit of the
                // doubt the registry would have given its row.
                None => was.unwrap_or(true),
                Some(age) => match was {
                    Some(false) => age <= ttl / 2,
                    _ => age <= ttl,
                },
            };
            seen.insert(row.id, is);
            if is {
                live.push(row.clone());
            } else if member.voter {
                down.push(row.clone());
            }
        }
        self.live = seen;
        live.sort_by_key(|n| n.id);
        down.sort_by_key(|n| n.id);
        Some((live, down))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queen::RaftMember;

    const TTL: Duration = Duration::from_secs(10);

    fn node(id: i32, raft: Option<u64>) -> Node {
        Node {
            id,
            host: format!("kafka-{id}.example.com"),
            port: 9092,
            incarnation: format!("inc-{id}"),
            raft_node: raft,
        }
    }

    /// Three facades on raft nodes 1..=3, facade id = raft id.
    fn rows() -> Vec<Node> {
        (1..=3).map(|id| node(id, Some(id as u64))).collect()
    }

    fn members(ages: &[(u64, Option<u64>)], view_age: Option<u64>) -> RaftMembers {
        RaftMembers {
            node_id: 1,
            leader_id: Some(1),
            view_age_ms: view_age,
            members: ages
                .iter()
                .map(|(id, age)| RaftMember {
                    node_id: *id,
                    voter: true,
                    last_ack_ms: *age,
                })
                .collect(),
        }
    }

    fn ids(nodes: &[Node]) -> Vec<i32> {
        nodes.iter().map(|n| n.id).collect()
    }

    #[test]
    fn a_healthy_cluster_is_all_live() {
        let mut j = Judge::new(TTL);
        let (live, down) = j
            .judge(
                &node(1, Some(1)),
                &rows(),
                &members(&[(1, Some(0)), (2, Some(80)), (3, Some(120))], Some(0)),
            )
            .unwrap();
        assert_eq!(ids(&live), [1, 2, 3]);
        assert!(down.is_empty());
    }

    /// THE regression: a node whose registry writes are starved is still live
    /// while raft hears from it. Liveness never looks at the row's age — only at
    /// what the raft leader last heard — so a follower that cannot get a KV
    /// write through for minutes stays in the broker list.
    #[test]
    fn liveness_does_not_depend_on_the_rows_being_rewritten() {
        let mut j = Judge::new(TTL);
        let view = members(&[(1, Some(0)), (2, Some(90)), (3, Some(40))], Some(300));
        for _ in 0..1_000 {
            let (live, _) = j.judge(&node(1, Some(1)), &rows(), &view).unwrap();
            assert_eq!(ids(&live), [1, 2, 3]);
        }
    }

    /// Hysteresis, both ways: a late node is kept until a whole TTL of silence,
    /// and once down it must be heard well inside the TTL to come back.
    #[test]
    fn a_node_is_dropped_after_a_ttl_of_silence_and_rejoins_inside_half_of_one() {
        let mut j = Judge::new(TTL);
        let me = node(1, Some(1));
        let judge = |j: &mut Judge, age3: u64| {
            j.judge(
                &me,
                &rows(),
                &members(&[(1, Some(0)), (2, Some(50)), (3, Some(age3))], Some(0)),
            )
            .unwrap()
        };
        // Late, but not a TTL late: still live — one late beat drops nobody.
        let (live, _) = judge(&mut j, 9_999);
        assert_eq!(ids(&live), [1, 2, 3]);
        let (live, _) = judge(&mut j, 10_000);
        assert_eq!(ids(&live), [1, 2, 3], "exactly the TTL is not past it");
        // Past the TTL: down, and still a replica (a voter), just not live.
        let (live, down) = judge(&mut j, 10_001);
        assert_eq!(ids(&live), [1, 2]);
        assert_eq!(ids(&down), [3]);
        // Heard again but at the edge: stays down (no flapping)...
        let (live, _) = judge(&mut j, 9_000);
        assert_eq!(ids(&live), [1, 2]);
        let (live, _) = judge(&mut j, 5_001);
        assert_eq!(ids(&live), [1, 2]);
        // ...until it is heard well inside the TTL, as a restarted node is.
        let (live, down) = judge(&mut j, 5_000);
        assert_eq!(ids(&live), [1, 2, 3]);
        assert!(down.is_empty());
    }

    /// A view the node cannot trust — a follower cut off from the leader, an
    /// election — judges nobody: the last live set stands rather than every
    /// other node looking dead from the one node that cannot see.
    #[test]
    fn a_stale_or_missing_view_judges_nothing() {
        let mut j = Judge::new(TTL);
        let me = node(2, Some(2));
        let dead_looking = [(1, Some(60_000)), (2, Some(60_000)), (3, Some(60_000))];
        assert!(j
            .judge(&me, &rows(), &members(&dead_looking, None))
            .is_none());
        assert!(j
            .judge(&me, &rows(), &members(&dead_looking, Some(5_001)))
            .is_none());
        // Half the TTL old is still a view.
        assert!(j
            .judge(&me, &rows(), &members(&dead_looking, Some(5_000)))
            .is_some());
    }

    /// This node is always live, and always as it describes itself — its row can
    /// be a predecessor's until its own write lands.
    #[test]
    fn this_node_is_always_live_and_never_judged() {
        let mut j = Judge::new(TTL);
        let mut me = node(3, Some(3));
        me.host = "the-address-this-process-listens-on".into();
        let (live, down) = j
            .judge(
                &me,
                &rows(),
                &members(&[(1, Some(0)), (2, Some(0)), (3, Some(99_999))], Some(0)),
            )
            .unwrap();
        assert_eq!(ids(&live), [1, 2, 3]);
        assert!(down.is_empty());
        assert_eq!(
            live.iter().find(|n| n.id == 3).unwrap().host,
            "the-address-this-process-listens-on"
        );
    }

    /// A row whose raft node is not a member of the raft cluster any more is
    /// neither live nor a replica; a row with no raft node at all is judged the
    /// registry's way (present = live); a learner that is down is not an
    /// offline REPLICA, because it is not one of the voters every write lands on.
    #[test]
    fn membership_decides_who_is_a_replica() {
        let mut j = Judge::new(TTL);
        let rows = vec![
            node(1, Some(1)),
            node(2, Some(2)),
            node(3, Some(3)),
            node(4, Some(9)),
            node(5, None),
        ];
        let mut view = members(
            &[(1, Some(0)), (2, Some(20_000)), (3, Some(20_000))],
            Some(0),
        );
        view.members[2].voter = false;
        let (live, down) = j.judge(&node(1, Some(1)), &rows, &view).unwrap();
        assert_eq!(ids(&live), [1, 5]);
        assert_eq!(ids(&down), [2]);
    }

    /// A member the leader has no figure for yet keeps whatever it was judged,
    /// and a node never judged gets the benefit of the doubt.
    #[test]
    fn an_unknown_age_keeps_the_last_verdict() {
        let mut j = Judge::new(TTL);
        let me = node(1, Some(1));
        let (live, _) = j
            .judge(
                &me,
                &rows(),
                &members(&[(1, Some(0)), (2, None), (3, Some(20_000))], Some(0)),
            )
            .unwrap();
        assert_eq!(ids(&live), [1, 2]);
        let (live, down) = j
            .judge(
                &me,
                &rows(),
                &members(&[(1, Some(0)), (2, None), (3, None)], Some(0)),
            )
            .unwrap();
        assert_eq!(ids(&live), [1, 2], "node 3 came back on no evidence");
        assert_eq!(ids(&down), [3]);
    }
}
