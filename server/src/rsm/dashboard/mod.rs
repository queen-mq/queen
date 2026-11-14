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

/// What this node moved over the collector's last interval, in bytes per
/// second.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Traffic {
    /// The interval, in seconds.
    pub window_s: u64,
    /// Storage, `(read, written)`; `None` where the platform does not say.
    pub disk: Option<(f64, f64)>,
    /// The network interfaces, `(received, sent)`; `None` where the platform
    /// does not say.
    pub net: Option<(f64, f64)>,
    /// The Raft RPC port, per channel in
    /// [`Channel::ALL`](crate::rsm::replicator::raft::traffic::Channel::ALL)
    /// order: `[sent, received]`.
    pub raft: [[f64; 2]; 5],
}

impl Traffic {
    /// The Raft view's per-node `traffic` block.
    pub fn to_json(self) -> serde_json::Value {
        use crate::rsm::replicator::raft::traffic::Channel;
        use serde_json::json;
        let mut raft = serde_json::Map::new();
        let (mut sent, mut received) = (0.0, 0.0);
        for (ch, [s, r]) in Channel::ALL.into_iter().zip(self.raft) {
            sent += s;
            received += r;
            raft.insert(
                ch.as_str().into(),
                json!({"sentBytesPerSec": s, "receivedBytesPerSec": r}),
            );
        }
        raft.insert("sentBytesPerSec".into(), json!(sent));
        raft.insert("receivedBytesPerSec".into(), json!(received));
        json!({
            "windowSeconds": self.window_s,
            "raft": raft,
            "network": self.net.map(|(r, s)| json!({"receivedBytesPerSec": r, "sentBytesPerSec": s})),
            "disk": self.disk.map(|(r, w)| json!({"readBytesPerSec": r, "writtenBytesPerSec": w})),
        })
    }
}

static TRAFFIC: std::sync::Mutex<Option<Traffic>> = std::sync::Mutex::new(None);

/// Record what the collector measured over its last interval.
pub fn set_traffic(t: Traffic) {
    *TRAFFIC.lock().unwrap_or_else(|p| p.into_inner()) = Some(t);
}

/// The collector's last interval; `None` before the first one closes.
pub fn traffic() -> Option<Traffic> {
    *TRAFFIC.lock().unwrap_or_else(|p| p.into_inner())
}

/// What this node's log and clients did over the collector's last interval,
/// a second: the entries committed (every node of a group sees the same),
/// the commands the groups it leads planned (`None` with
/// `QUEEN_RAFT_METRICS=0`), the transactions it answered as committed.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct WorkRates {
    pub window_s: u64,
    pub commits_per_sec: f64,
    pub commands_per_sec: Option<f64>,
    pub transactions_per_sec: f64,
}

static WORK: std::sync::Mutex<Option<WorkRates>> = std::sync::Mutex::new(None);

/// Record the work the collector measured over its last interval.
pub fn set_work(w: WorkRates) {
    *WORK.lock().unwrap_or_else(|p| p.into_inner()) = Some(w);
}

/// The node status's `work` block (admin.rs, `GET /api/v1/raft/members`):
/// the last interval's rates, null until it closes, and the connections open
/// now on each listener this node serves (`connections` is empty in a build
/// with no listener).
pub fn work_json() -> serde_json::Value {
    work_json_of(
        *WORK.lock().unwrap_or_else(|p| p.into_inner()),
        &crate::metrics::listeners(false),
    )
}

fn work_json_of(
    rates: Option<WorkRates>,
    listeners: &[crate::metrics::ListenerReading],
) -> serde_json::Value {
    use serde_json::json;
    let connections: serde_json::Map<String, serde_json::Value> = listeners
        .iter()
        .map(|l| (l.name.to_string(), json!(l.open)))
        .collect();
    json!({
        "windowSeconds": rates.map(|r| r.window_s),
        "commitsPerSec": rates.map(|r| r.commits_per_sec),
        "commandsPerSec": rates.and_then(|r| r.commands_per_sec),
        "transactionsPerSec": rates.map(|r| r.transactions_per_sec),
        "connections": connections,
    })
}

#[cfg(test)]
mod work_json_tests {
    use super::*;
    use crate::metrics::ListenerReading;

    #[test]
    fn the_status_says_the_last_minutes_rates_and_the_connections_open_now() {
        let open = |name, open| ListenerReading {
            name,
            open,
            peak: open,
            cap: 20_000,
            ..Default::default()
        };
        let v = work_json_of(
            Some(WorkRates {
                window_s: 60,
                commits_per_sec: 18.5,
                commands_per_sec: Some(20.0),
                transactions_per_sec: 3.0,
            }),
            &[open("broker", 12), open("proxy", 4)],
        );
        assert_eq!(
            v,
            serde_json::json!({
                "windowSeconds": 60, "commitsPerSec": 18.5, "commandsPerSec": 20.0,
                "transactionsPerSec": 3.0, "connections": {"broker": 12, "proxy": 4},
            })
        );
        // Before the collector's first interval closes: no rates, never a 0,
        // and the connections still read live.
        let v = work_json_of(None, &[open("broker", 2)]);
        assert_eq!(v["commitsPerSec"], serde_json::Value::Null);
        assert_eq!(v["transactionsPerSec"], serde_json::Value::Null);
        assert_eq!(v["connections"]["broker"], 2);
    }
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
