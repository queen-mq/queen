//! Spawning, killing and wiping node processes for the scenarios.
//!
//! Every node is a real child process (`s3-consensus node ...`) listening on a
//! real TCP port, so `kill -9` is a real `kill -9`.

use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;

use crate::client::connect_retry;
use crate::client::Conn;
use crate::types::NodeId;
use crate::wire::Req;

#[derive(Clone, Debug)]
pub struct NodeCfg {
    pub id: NodeId,
    pub port: u16,
    pub fsync: String,
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
    pub pre_vote: bool,
    pub leader_restore: bool,
    pub max_in_snapshot_log_to_keep: u64,
    pub file_bytes: u64,
    pub lin_batch_ms: u64,
    pub durable_ms: u64,
    /// `save_committed` durability: none | buffered | fsync (§12.3).
    pub committed: String,
    /// Start the node with `Raft::wait_for_recovery`.
    pub wait_recovery: bool,
}

impl NodeCfg {
    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

pub struct NodeProc {
    pub cfg: NodeCfg,
    pub dir: PathBuf,
    pub child: Option<Child>,
}

pub struct Cluster {
    pub root: PathBuf,
    pub exe: PathBuf,
    pub nodes: Vec<NodeProc>,
    /// Node ids whose data directory is somewhere else than `root/n<id>` —
    /// used to put one node's data on a dm-flakey filesystem.
    pub dir_override: std::collections::BTreeMap<NodeId, PathBuf>,
}

impl Cluster {
    pub fn new(root: PathBuf) -> anyhow::Result<Cluster> {
        let exe = std::env::current_exe().context("current_exe")?;
        std::fs::create_dir_all(&root)?;
        Ok(Cluster {
            root,
            exe,
            nodes: Vec::new(),
            dir_override: std::collections::BTreeMap::new(),
        })
    }

    pub fn addrs(&self) -> Vec<(NodeId, String)> {
        self.nodes
            .iter()
            .map(|n| (n.cfg.id, n.cfg.addr()))
            .collect()
    }

    pub fn find(&self, id: NodeId) -> Option<&NodeProc> {
        self.nodes.iter().find(|n| n.cfg.id == id)
    }

    pub fn addr_of(&self, id: NodeId) -> Option<String> {
        self.find(id).map(|n| n.cfg.addr())
    }

    /// Start a node and wait until it answers on its port.
    pub async fn start(&mut self, cfg: NodeCfg) -> anyhow::Result<()> {
        let dir = match self.dir_override.get(&cfg.id) {
            Some(d) => d.clone(),
            None => self.root.join(format!("n{}", cfg.id)),
        };
        std::fs::create_dir_all(&dir)?;
        let child = self.spawn_proc(&cfg, &dir)?;
        self.nodes.push(NodeProc {
            cfg: cfg.clone(),
            dir,
            child: Some(child),
        });
        connect_retry(&cfg.addr(), Duration::from_secs(20))
            .await
            .with_context(|| format!("node {} never came up on {}", cfg.id, cfg.addr()))?;
        Ok(())
    }

    /// Start a node again after it was stopped or killed (same id, same dir
    /// unless it was wiped).
    pub async fn restart(&mut self, id: NodeId) -> anyhow::Result<()> {
        let (cfg, dir) = {
            let n = self
                .nodes
                .iter()
                .find(|n| n.cfg.id == id)
                .context("unknown node")?;
            (n.cfg.clone(), n.dir.clone())
        };
        let child = self.spawn_proc(&cfg, &dir)?;
        if let Some(n) = self.nodes.iter_mut().find(|n| n.cfg.id == id) {
            n.child = Some(child);
        }
        connect_retry(&cfg.addr(), Duration::from_secs(20)).await?;
        Ok(())
    }

    fn spawn_proc(&self, cfg: &NodeCfg, dir: &PathBuf) -> anyhow::Result<Child> {
        let log = std::fs::File::options()
            .create(true)
            .append(true)
            .open(self.root.join(format!("n{}.log", cfg.id)))?;
        let child = Command::new(&self.exe)
            .arg("node")
            .args(["--id", &cfg.id.to_string()])
            .args(["--dir", &dir.display().to_string()])
            .args(["--listen", &cfg.addr()])
            .args(["--fsync", &cfg.fsync])
            .args(["--heartbeat-ms", &cfg.heartbeat_ms.to_string()])
            .args(["--election-min-ms", &cfg.election_min_ms.to_string()])
            .args(["--election-max-ms", &cfg.election_max_ms.to_string()])
            .args(["--pre-vote", &cfg.pre_vote.to_string()])
            .args(["--leader-restore", &cfg.leader_restore.to_string()])
            .args([
                "--max-in-snapshot-log-to-keep",
                &cfg.max_in_snapshot_log_to_keep.to_string(),
            ])
            .args(["--file-bytes", &cfg.file_bytes.to_string()])
            .args(["--lin-batch-ms", &cfg.lin_batch_ms.to_string()])
            .args(["--durable-ms", &cfg.durable_ms.to_string()])
            .args(["--committed", &cfg.committed])
            .args(["--wait-recovery", &cfg.wait_recovery.to_string()])
            .env(
                "RUST_LOG",
                std::env::var("NODE_LOG").unwrap_or_else(|_| "info".into()),
            )
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .stdin(Stdio::null())
            .spawn()
            .context("spawn node")?;
        Ok(child)
    }

    /// SIGKILL, the way a pod dies.
    pub fn kill9(&mut self, id: NodeId) -> anyhow::Result<()> {
        let n = self
            .nodes
            .iter_mut()
            .find(|n| n.cfg.id == id)
            .context("unknown node")?;
        if let Some(child) = n.child.as_mut() {
            child.kill().ok();
            child.wait().ok();
        }
        n.child = None;
        Ok(())
    }

    /// SIGSTOP / SIGCONT a node process: a frozen pod (GC pause, a disk that
    /// stops answering, a paused container), not a dead one. The socket stays
    /// open and nothing is refused; the peer simply never answers.
    pub fn signal(&self, id: NodeId, sig: &str) -> anyhow::Result<()> {
        let n = self.find(id).context("unknown node")?;
        let pid = n.child.as_ref().context("node is not running")?.id();
        let st = Command::new("kill")
            .arg(format!("-{sig}"))
            .arg(pid.to_string())
            .status()
            .context("kill")?;
        anyhow::ensure!(st.success(), "kill -{sig} {pid} failed");
        Ok(())
    }

    /// Graceful stop through the admin call.
    pub async fn stop(&mut self, id: NodeId) -> anyhow::Result<()> {
        let addr = self.addr_of(id).context("unknown node")?;
        if let Ok(mut c) = Conn::connect(&addr).await {
            let _ = c.unit(Req::Shutdown).await;
        }
        let n = self.nodes.iter_mut().find(|n| n.cfg.id == id).unwrap();
        if let Some(child) = n.child.as_mut() {
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                match child.try_wait()? {
                    Some(_) => break,
                    None if Instant::now() > deadline => {
                        child.kill().ok();
                        child.wait().ok();
                        break;
                    }
                    None => std::thread::sleep(Duration::from_millis(20)),
                }
            }
        }
        n.child = None;
        Ok(())
    }

    /// Delete a node's whole data directory (a replaced disk).
    pub fn wipe(&mut self, id: NodeId) -> anyhow::Result<()> {
        let n = self
            .nodes
            .iter()
            .find(|n| n.cfg.id == id)
            .context("unknown node")?;
        std::fs::remove_dir_all(&n.dir).ok();
        std::fs::create_dir_all(&n.dir)?;
        Ok(())
    }

    pub async fn shutdown_all(&mut self) {
        let ids: Vec<NodeId> = self.nodes.iter().map(|n| n.cfg.id).collect();
        for id in ids {
            let _ = self.stop(id).await;
        }
        for n in self.nodes.iter_mut() {
            if let Some(child) = n.child.as_mut() {
                child.kill().ok();
                child.wait().ok();
            }
            n.child = None;
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for n in self.nodes.iter_mut() {
            if let Some(child) = n.child.as_mut() {
                child.kill().ok();
                child.wait().ok();
            }
        }
    }
}
