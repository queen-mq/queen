//! The dashboard's data in raft mode (PLAN_RAFT.md D17, §14.6, WP-2.8/4.8).
//!
//! Every node keeps its OWN rows ([`model`]) in its `local.db`, a dashboard
//! read gathers the rows of every node, and pure views build the answers from
//! them.

pub mod collector;
pub mod model;
pub mod node_views;
pub mod queue_views;
pub mod store;

/// This node as the dashboard names it: `QUEEN_SERVER_ID`, else `HOSTNAME`,
/// else `node-<raft node id>` — stable across restarts.
pub fn node_label(node_id: u64) -> String {
    ["QUEEN_SERVER_ID", "HOSTNAME"]
        .iter()
        .find_map(|k| std::env::var(k).ok().filter(|v| !v.trim().is_empty()))
        .unwrap_or_else(|| format!("node-{node_id}"))
}

/// The HTTP port this node serves (`PORT`, default 6632).
pub fn http_port() -> i32 {
    std::env::var("PORT")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(6632)
}

/// When this process started serving, for `uptimeSeconds`.
pub fn started() -> std::time::Instant {
    static STARTED: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    *STARTED.get_or_init(std::time::Instant::now)
}

/// The cluster's name on the dashboard: `QUEEN_CELL_ID` when set, else
/// `raft-` and eight hex digits of a hash of the membership (the voters' ids
/// and Raft addresses), so every node of one cluster answers the same id and
/// a different cluster a different one. `members` is `(node id, raft addr)`.
pub fn cluster_id(members: &[(u64, String)]) -> String {
    if let Some(id) = std::env::var("QUEEN_CELL_ID")
        .ok()
        .filter(|v| !v.trim().is_empty())
    {
        return id;
    }
    let mut m: Vec<&(u64, String)> = members.iter().collect();
    m.sort();
    let key: String = m.iter().map(|(id, a)| format!("{id}={a};")).collect();
    format!(
        "raft-{:08x}",
        (xxhash_rust::xxh3::xxh3_64(key.as_bytes()) >> 32) as u32
    )
}
