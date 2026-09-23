//! The cluster a node belongs to: its id, where it listens for Raft RPCs,
//! and every voter's two addresses.
//!
//! ```text
//! QUEEN_RAFT_NODE_ID=2
//! QUEEN_RAFT_PEERS=1=10.0.0.1:7400/10.0.0.1:6632,2=10.0.0.2:7400/10.0.0.2:6632,3=10.0.0.3:7400/10.0.0.3:6632
//! QUEEN_RAFT_LISTEN=0.0.0.0:7400        # default: 0.0.0.0 on this node's raft port
//! QUEEN_RAFT_TOKEN=<shared secret>      # optional: every Raft RPC must carry it
//! ```
//!
//! `QUEEN_RAFT_PEERS` lists every voter as `id=raft_addr/http_addr`: the Raft
//! RPCs go to `raft_addr`, and a follower forwards client requests to the
//! leader's `http_addr`. Unset, the node is a single voter with no network.
//!
//! Every node of a new cluster starts with the SAME list on an empty data
//! directory; each one initializes the cluster with it (openraft allows that
//! with identical members) and they elect a leader. After the first start the
//! membership lives in the log and the list is only used for this node's own
//! listen address.

use std::collections::BTreeMap;

use super::types::{NodeId, QueenNode};

/// A multi-node cluster's configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterConfig {
    /// This node.
    pub node_id: NodeId,
    /// The address the Raft RPC server binds.
    pub listen: String,
    /// Every voter of the initial membership, this node included.
    pub members: BTreeMap<NodeId, QueenNode>,
    /// The shared secret every Raft RPC carries (`x-queen-raft-token`).
    pub token: Option<String>,
    /// The member this group would rather have lead (several groups per
    /// process spread their leaders over the nodes, [`ClusterConfig::for_group`]).
    /// `None`: whoever wins the election leads.
    pub preferred_leader: Option<NodeId>,
}

pub const TOKEN_HEADER: &str = "x-queen-raft-token";

impl ClusterConfig {
    /// `QUEEN_RAFT_NODE_ID` (default 1), `QUEEN_RAFT_PEERS`, `QUEEN_RAFT_LISTEN`,
    /// `QUEEN_RAFT_TOKEN`. `Ok(None)` when `QUEEN_RAFT_PEERS` is unset or
    /// empty: a single voter.
    pub fn from_env() -> Result<Option<ClusterConfig>, String> {
        let peers = std::env::var("QUEEN_RAFT_PEERS").unwrap_or_default();
        if peers.trim().is_empty() {
            return Ok(None);
        }
        let node_id = node_id_from_env()?;
        let listen = std::env::var("QUEEN_RAFT_LISTEN").ok();
        let token = std::env::var("QUEEN_RAFT_TOKEN")
            .ok()
            .filter(|t| !t.trim().is_empty());
        ClusterConfig::parse(node_id, &peers, listen.as_deref(), token).map(Some)
    }

    /// Parse `id=raft/http,...` for node `node_id`.
    pub fn parse(
        node_id: NodeId,
        peers: &str,
        listen: Option<&str>,
        token: Option<String>,
    ) -> Result<ClusterConfig, String> {
        let mut members = BTreeMap::new();
        for part in peers.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (id, addrs) = part.split_once('=').ok_or_else(|| {
                format!("QUEEN_RAFT_PEERS: `{part}` is not `id=raft_addr/http_addr`")
            })?;
            let id: NodeId = id
                .trim()
                .parse()
                .map_err(|_| format!("QUEEN_RAFT_PEERS: `{id}` is not a node id"))?;
            if id == 0 {
                return Err("QUEEN_RAFT_PEERS: node ids start at 1".into());
            }
            let (raft, http) = addrs.split_once('/').ok_or_else(|| {
                format!("QUEEN_RAFT_PEERS: node {id} needs `raft_addr/http_addr`, got `{addrs}`")
            })?;
            let (raft, http) = (raft.trim(), http.trim());
            if raft.is_empty() || http.is_empty() {
                return Err(format!("QUEEN_RAFT_PEERS: node {id} has an empty address"));
            }
            if members.insert(id, QueenNode::new(raft, http)).is_some() {
                return Err(format!("QUEEN_RAFT_PEERS: node {id} is listed twice"));
            }
        }
        let own = members
            .get(&node_id)
            .ok_or_else(|| format!("QUEEN_RAFT_NODE_ID={node_id} is not in QUEEN_RAFT_PEERS"))?;
        let listen = match listen.map(str::trim).filter(|l| !l.is_empty()) {
            Some(l) => l.to_string(),
            None => {
                let port = own.raft.rsplit_once(':').map(|(_, p)| p).ok_or_else(|| {
                    format!(
                        "QUEEN_RAFT_PEERS: node {node_id}'s raft address `{}` has no port",
                        own.raft
                    )
                })?;
                format!("0.0.0.0:{port}")
            }
        };
        Ok(ClusterConfig {
            node_id,
            listen,
            members,
            token,
            preferred_leader: None,
        })
    }

    /// The configuration of Raft group `group` (of several in one process,
    /// `QUEEN_RAFT_GROUPS`): every Raft address — the listen address and each
    /// member's — moves to its port + `group`, the client addresses stay (one
    /// HTTP server serves every group), and the group prefers the member at
    /// position `group mod n` (by id) as its leader, so the groups' leaders
    /// spread over the nodes. Group 0 keeps the addresses as configured.
    pub fn for_group(&self, group: usize) -> Result<ClusterConfig, String> {
        let shift = |addr: &str| -> Result<String, String> {
            if group == 0 {
                return Ok(addr.to_string());
            }
            let (host, port) = addr
                .rsplit_once(':')
                .ok_or_else(|| format!("raft address `{addr}` has no port"))?;
            let port: u16 = port
                .parse()
                .map_err(|_| format!("raft address `{addr}`: bad port"))?;
            let port = port
                .checked_add(group as u16)
                .ok_or_else(|| format!("raft address `{addr}`: port + {group} overflows"))?;
            Ok(format!("{host}:{port}"))
        };
        let mut members = BTreeMap::new();
        for (id, n) in &self.members {
            members.insert(*id, QueenNode::new(shift(&n.raft)?, n.http.clone()));
        }
        let ids: Vec<NodeId> = self.members.keys().copied().collect();
        Ok(ClusterConfig {
            node_id: self.node_id,
            listen: shift(&self.listen)?,
            members,
            token: self.token.clone(),
            preferred_leader: (ids.len() > 1).then(|| ids[group % ids.len()]),
        })
    }
}

/// `QUEEN_RAFT_NODE_ID`: a number, or `ordinal` for a StatefulSet pod — the
/// trailing number of `HOSTNAME` plus one (`queen-0` is node 1).
pub fn node_id_from_env() -> Result<NodeId, String> {
    let raw = std::env::var("QUEEN_RAFT_NODE_ID").unwrap_or_else(|_| "1".into());
    let raw = raw.trim();
    if raw.eq_ignore_ascii_case("ordinal") {
        let host = std::env::var("HOSTNAME").unwrap_or_default();
        let digits: String = host
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_digit())
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        let n: u64 = digits.parse().map_err(|_| {
            format!("QUEEN_RAFT_NODE_ID=ordinal: HOSTNAME `{host}` has no trailing number")
        })?;
        return Ok(n + 1);
    }
    match raw.parse::<NodeId>() {
        Ok(0) | Err(_) => Err(format!(
            "QUEEN_RAFT_NODE_ID={raw}: expected a node id from 1, or `ordinal`"
        )),
        Ok(n) => Ok(n),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_three_peers_and_defaults_the_listen_port() {
        let c = ClusterConfig::parse(
            2,
            "1=a:7401/a:6631, 2=b:7402/b:6632,3=c:7403/c:6633",
            None,
            None,
        )
        .expect("parse");
        assert_eq!(c.members.len(), 3);
        assert_eq!(c.members[&2], QueenNode::new("b:7402", "b:6632"));
        assert_eq!(c.listen, "0.0.0.0:7402");
    }

    #[test]
    fn refuses_what_it_cannot_route() {
        for (id, peers) in [
            (4, "1=a:1/a:2"),
            (1, "1=a:1"),
            (1, "1=a:1/a:2,1=b:1/b:2"),
            (1, "x=a:1/a:2"),
            (1, "1=/a:2"),
            (0, "0=a:1/a:2"),
        ] {
            assert!(
                ClusterConfig::parse(id, peers, None, None).is_err(),
                "{id} {peers} must be refused"
            );
        }
    }
}
