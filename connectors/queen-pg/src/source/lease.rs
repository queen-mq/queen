//! The source lease: which node of the cluster runs a source (PLAN §4.2).
//!
//! Every node runs every source document, and a replication slot takes ONE
//! consumer, so one node streams while the others wait. The node that holds
//! `src:<name>:lease` (a TTL'd KV row, `QUEEN_PG_LEASE_TTL_MS`) streams; the
//! others read who holds it and claim again after half a TTL. Adapted from the
//! S3 sink's lease (connectors/queen-s3/src/lease.rs): claimed with
//! `putIfAbsent`, refreshed every third of the TTL with a fenced TTL put
//! (`expect` = the version held, `required`), released with a delete at the
//! version the row HAS when it still names this incarnation.
//!
//! Unlike the S3 sink, the lease is NOT the fence of the data: the pointer is
//! (its CAS rides in every bundle, [`super::pointer`]). The lease only keeps
//! two nodes from fighting over the slot. A node that loses its lease stops
//! streaming; a node that kept streaming anyway (a stall longer than the TTL)
//! is fenced by the pointer on its next bundle.
//!
//! **One writer of the row at a time.** The refresh task and a release both
//! write the row expecting the version this handle last saw; two in flight
//! together expect the SAME version and the second loses, fencing this node
//! out of its own lease. So every write holds `writing` from reading the
//! version it expects until its answer is absorbed.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use crate::error::{Error, Result};
use crate::queen::{KvOp, QueenApi};

use super::pointer::result_at;

/// How long a release keeps trying through a leaderless moment (the broker
/// starts its own drain at SIGTERM, so a release often meets an election).
pub const RELEASE_BUDGET: Duration = Duration::from_secs(5);
const RELEASE_RETRY_MIN: Duration = Duration::from_millis(100);
const RELEASE_RETRY_MAX: Duration = Duration::from_secs(1);

/// The result of one claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Acquired {
    Taken,
    /// Somebody else holds it: their `node`.
    HeldBy(String),
}

/// One source's lease, held by one incarnation of one node.
pub struct Lease {
    api: Arc<dyn QueenApi>,
    key: String,
    node: String,
    /// A restart of the same node is a NEW incarnation, so a row that outlives
    /// the unit that wrote it is never mistaken for this one's.
    incarnation: String,
    ttl_seconds: u64,
    since: Mutex<String>,
    /// The version this handle last wrote; 0 = not held.
    version: AtomicI64,
    lost: AtomicBool,
    writing: tokio::sync::Mutex<()>,
}

impl Lease {
    pub fn new(api: Arc<dyn QueenApi>, key: String, node: &str, ttl_ms: u64) -> Lease {
        Lease {
            api,
            key,
            node: node.to_string(),
            incarnation: mint_incarnation(),
            // Seconds, rounded DOWN, at least one: a liveness claim that
            // outlives its configured window would hold after its owner died.
            ttl_seconds: (ttl_ms / 1_000).max(1),
            since: Mutex::new(String::new()),
            version: AtomicI64::new(0),
            lost: AtomicBool::new(false),
            writing: tokio::sync::Mutex::new(()),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }

    pub fn version(&self) -> i64 {
        self.version.load(Ordering::SeqCst)
    }

    /// A refresh lost its precondition: somebody else holds the row. Final
    /// for this handle.
    pub fn lost(&self) -> bool {
        self.lost.load(Ordering::SeqCst)
    }

    pub fn held(&self) -> bool {
        self.version() != 0 && !self.lost()
    }

    /// A third of the TTL, floored at 100 ms: two refreshes may fail before
    /// the row expires.
    pub fn refresh_interval(&self) -> Duration {
        Duration::from_millis(((self.ttl_seconds * 1_000) / 3).max(100))
    }

    /// Half the TTL: how often a standby node claims again.
    pub fn claim_interval(&self) -> Duration {
        Duration::from_millis(((self.ttl_seconds * 1_000) / 2).max(100))
    }

    /// Claim the lease. `putIfAbsent` wins against an expired row and loses to
    /// a live one, whose value and version come back in the same answer. A
    /// live row naming THIS node under another incarnation is an earlier life
    /// of this unit (restarted by the manager, which waits for the previous
    /// unit to stop): it is taken over at once, at the version it has.
    pub async fn acquire(&self) -> Result<Acquired> {
        let _writing = self.writing.lock().await;
        let since = crate::values::iso_utc_micros(crate::status::now_us());
        let doc = self.doc_with(&since);
        let a = self
            .api
            .kv(vec![KvOp::put_if_absent_ttl(
                self.key.clone(),
                doc.clone(),
                self.ttl_seconds,
            )])
            .await?;
        let r = result_at(&a.results, 0).ok_or_else(|| {
            Error::io(format!("the lease claim of {} answered nothing", self.key))
        })?;
        if a.ok && r.did_apply() && r.version != 0 {
            self.take(since, r.version);
            return Ok(Acquired::Taken);
        }
        let held_by = holder_of(&r.value);
        let earlier_life = r.value.get("node").and_then(Value::as_str) == Some(self.node.as_str())
            && r.value.get("incarnation").and_then(Value::as_str)
                != Some(self.incarnation.as_str());
        if !earlier_life || r.version == 0 {
            return Ok(Acquired::HeldBy(held_by));
        }
        let take_over = KvOp::fence_ttl(self.key.clone(), doc, r.version, self.ttl_seconds);
        let a = self.api.kv(vec![take_over]).await?;
        match result_at(&a.results, 0) {
            Some(t) if a.ok && t.did_apply() && t.version != 0 => {
                self.take(since, t.version);
                Ok(Acquired::Taken)
            }
            _ => Ok(Acquired::HeldBy(holder_of(&a.value))),
        }
    }

    fn take(&self, since: String, version: i64) {
        *self.since.lock().unwrap_or_else(|p| p.into_inner()) = since;
        self.version.store(version, Ordering::SeqCst);
        self.lost.store(false, Ordering::SeqCst);
    }

    /// Renew the lease: a fenced TTL put at the version held. A lost
    /// precondition marks the handle lost and answers [`Error::Fenced`].
    pub async fn refresh(&self) -> Result<()> {
        let _writing = self.writing.lock().await;
        if self.lost() || self.version() == 0 {
            return Err(Error::Fenced(format!("the lease {} is not held", self.key)));
        }
        let op = KvOp::fence_ttl(
            self.key.clone(),
            self.doc(),
            self.version(),
            self.ttl_seconds,
        );
        let a = self.api.kv(vec![op]).await?;
        match result_at(&a.results, 0) {
            Some(r) if a.ok && r.did_apply() && r.version != 0 => {
                self.version.store(r.version, Ordering::SeqCst);
                Ok(())
            }
            _ => {
                self.lost.store(true, Ordering::SeqCst);
                Err(Error::Fenced(format!(
                    "the lease {} was taken by {}",
                    self.key,
                    holder_of(&a.value)
                )))
            }
        }
    }

    /// Give the lease back so another node takes over without waiting out the
    /// TTL. The row is read and deleted at the version it HAS when it still
    /// names this incarnation (the version remembered can be behind: a refresh
    /// whose answer was lost). Transient failures are retried for
    /// [`RELEASE_BUDGET`]; anything else leaves the row to its TTL.
    pub async fn release(&self) {
        let _writing = self.writing.lock().await;
        if self.version() == 0 {
            return;
        }
        self.version.store(0, Ordering::SeqCst);
        let deadline = tokio::time::Instant::now() + RELEASE_BUDGET;
        let mut wait = RELEASE_RETRY_MIN;
        loop {
            match self.give_back().await {
                Ok(()) => return,
                Err(e) if e.is_retryable() && tokio::time::Instant::now() + wait <= deadline => {
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(RELEASE_RETRY_MAX);
                }
                Err(e) => {
                    tracing::warn!(
                        target: crate::LOG_TARGET,
                        key = %self.key,
                        error = %e,
                        "could not give the source lease back; it expires after its TTL"
                    );
                    return;
                }
            }
        }
    }

    async fn give_back(&self) -> Result<()> {
        let a = self.api.kv(vec![KvOp::get(self.key.clone())]).await?;
        let Some(row) = result_at(&a.results, 0).filter(|r| r.found == Some(true)) else {
            return Ok(());
        };
        if row.value.get("incarnation").and_then(Value::as_str) != Some(self.incarnation.as_str()) {
            return Ok(());
        }
        // Not required: losing it means another writer took the row in
        // between — theirs now, nothing to retry.
        self.api
            .kv(vec![KvOp::delete(self.key.clone(), Some(row.version))])
            .await?;
        Ok(())
    }

    pub fn mark_lost(&self) {
        self.lost.store(true, Ordering::SeqCst);
    }

    fn doc(&self) -> Value {
        let since = self.since.lock().unwrap_or_else(|p| p.into_inner()).clone();
        self.doc_with(&since)
    }

    fn doc_with(&self, since: &str) -> Value {
        json!({ "node": self.node, "incarnation": self.incarnation, "since": since })
    }
}

/// Keeps the lease alive while the source streams; aborted when dropped, so a
/// run that returns takes its refresher with it.
pub struct RefreshTask {
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for RefreshTask {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Refresh every [`Lease::refresh_interval`] until the lease is lost. A
/// transient failure skips a tick; a lost precondition ends the task with
/// [`Lease::lost`] set, which the engine polls.
pub fn spawn_refresh(lease: Arc<Lease>) -> RefreshTask {
    let every = lease.refresh_interval();
    let handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(every).await;
            if lease.lost() || lease.version() == 0 {
                return;
            }
            match lease.refresh().await {
                Ok(()) => {}
                Err(e) if !matches!(e, Error::Fenced(_)) && e.is_retryable() => {
                    tracing::warn!(
                        target: crate::LOG_TARGET,
                        key = %lease.key(),
                        error = %e,
                        "source lease refresh failed; retrying on the next tick"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        target: crate::LOG_TARGET,
                        key = %lease.key(),
                        error = %e,
                        "source lease lost: another node runs this source"
                    );
                    lease.mark_lost();
                    return;
                }
            }
        }
    });
    RefreshTask { handle }
}

/// The `node` of a lease row, for status and logs. A value this crate did not
/// write is reported, never parsed at.
pub fn holder_of(value: &Value) -> String {
    value
        .get("node")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string()
}

/// A random UUID-shaped tag (the crate has no uuid dependency).
fn mint_incarnation() -> String {
    let b: [u8; 16] = rand::random();
    let h = hex::encode(b);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intervals_and_incarnations() {
        let api: Arc<dyn QueenApi> = crate::fake::FakeQueen::new();
        let l = Lease::new(api.clone(), "src:a:lease".into(), "n1", 10_000);
        assert_eq!(l.refresh_interval(), Duration::from_millis(3_333));
        assert_eq!(l.claim_interval(), Duration::from_millis(5_000));
        let short = Lease::new(api, "k".into(), "n1", 500);
        assert_eq!(short.ttl_seconds, 1, "never below one second");
        assert_ne!(l.incarnation(), short.incarnation());
        assert_eq!(l.incarnation().len(), 36);
    }

    #[tokio::test]
    async fn the_first_claim_wins_and_a_loser_learns_the_owner() {
        let q = crate::fake::FakeQueen::new();
        let a = Lease::new(q.clone(), "src:s:lease".into(), "node-a", 10_000);
        let b = Lease::new(q.clone(), "src:s:lease".into(), "node-b", 10_000);
        assert_eq!(a.acquire().await.unwrap(), Acquired::Taken);
        assert!(a.held());
        assert_eq!(
            b.acquire().await.unwrap(),
            Acquired::HeldBy("node-a".into())
        );
        assert!(!b.held());
        let v0 = a.version();
        a.refresh().await.unwrap();
        assert_ne!(a.version(), v0, "every write takes a fresh version");
        a.release().await;
        assert!(
            q.kv_value("src:s:lease").is_none(),
            "released without waiting out the TTL"
        );
        assert_eq!(b.acquire().await.unwrap(), Acquired::Taken);
    }

    #[tokio::test]
    async fn an_earlier_life_is_taken_over_and_fenced_out() {
        let q = crate::fake::FakeQueen::new();
        let earlier = Lease::new(q.clone(), "src:s:lease".into(), "node-a", 10_000);
        earlier.acquire().await.unwrap();
        let now = Lease::new(q.clone(), "src:s:lease".into(), "node-a", 10_000);
        assert_eq!(now.acquire().await.unwrap(), Acquired::Taken);
        assert!(matches!(earlier.refresh().await, Err(Error::Fenced(_))));
        assert!(earlier.lost());
        // The earlier life's release leaves the new life's row alone.
        earlier.release().await;
        assert!(q.kv_value("src:s:lease").is_some());
        assert!(now.refresh().await.is_ok());
    }
}
