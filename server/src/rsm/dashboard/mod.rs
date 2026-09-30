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

// This process's CPU over the collector's last interval: percent of one core
// (4 busy cores = 400) as f64 bits, u64::MAX until the first interval closes,
// and that interval in seconds.
static CPU_PCT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(u64::MAX);
static CPU_WINDOW_S: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Record the CPU the collector measured over its last `window_s` seconds.
pub fn set_cpu_pct(pct: f64, window_s: u64) {
    use std::sync::atomic::Ordering;
    CPU_WINDOW_S.store(window_s, Ordering::Relaxed);
    CPU_PCT.store(pct.to_bits(), Ordering::Relaxed);
}

/// The CPU of the collector's last interval and its length in seconds; `None`
/// before the first one closes.
pub fn cpu_pct() -> Option<(f64, u64)> {
    use std::sync::atomic::Ordering;
    let bits = CPU_PCT.load(Ordering::Relaxed);
    (bits != u64::MAX).then(|| (f64::from_bits(bits), CPU_WINDOW_S.load(Ordering::Relaxed)))
}

/// The disk gate of this node's data directory: writes are refused once the
/// filesystem is `high_pct` used and accepted again below `low_pct`.
/// `enabled` is false where the gate is off (tests).
pub struct DiskGate {
    pub dir: std::path::PathBuf,
    pub high_pct: f64,
    pub low_pct: f64,
    pub enabled: bool,
}

static DISK_GATE: std::sync::OnceLock<DiskGate> = std::sync::OnceLock::new();
// Whether the gate is closed now. With several Raft groups in one process
// (QUEEN_RAFT_GROUPS) the group that looked last speaks for all of them.
static STORAGE_FULL: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Name the gate; the first facade of the process wins.
pub fn set_disk_gate(gate: DiskGate) {
    let _ = DISK_GATE.set(gate);
}

pub fn disk_gate() -> Option<&'static DiskGate> {
    DISK_GATE.get()
}

/// Mirror the gate's verdict whenever a facade re-judges it.
pub fn set_storage_full(full: bool) {
    STORAGE_FULL.store(full, std::sync::atomic::Ordering::Relaxed);
}

pub fn storage_full() -> bool {
    STORAGE_FULL.load(std::sync::atomic::Ordering::Relaxed)
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
