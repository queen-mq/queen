//! lease — queue ownership across instances, and the commit fence (plan §6.6).
//!
//! One instance runs a queue only while it holds that queue's **lease**, a
//! TTL'd key/value row taken with `putIfAbsent` and refreshed every third of its
//! TTL. That much is the `cluster/registry.rs` shape and it is only half of what
//! this module is for.
//!
//! The other half is the **fence**, and it is the part that makes two instances
//! safe rather than merely unlikely. Every intent batch and every commit batch
//! carries, at index 0, a conditional write of the lease row with
//! `expect: <the version this instance last wrote>` and `"required": true`
//! (024_kv.sql:1498-1502). A `required` precondition that loses rolls the WHOLE
//! batch back, so an instance whose lease was taken away — because it stalled
//! long enough for the TTL to pass, because its clock or its network went away,
//! because an operator started a second copy — cannot move the commit pointer
//! even if it is otherwise perfectly healthy and mid-upload. Its batch fails
//! with [`SinkError::Precondition`] and its queue task stops. Two instances can
//! never commit different window `k`s for one queue, and the object one of them
//! wrote is either identical (same intent, plan §4.2) or never committed.
//!
//! The lease is **always on**, single instance or not. It costs one KV row and
//! one small call every ten seconds, and it is the only thing standing between a
//! double-started container and a lake with two versions of one window.
//!
//! # The keys
//!
//! Three documents live under one family per (sink, queue), spelled
//! `s3:<sink>:<esc queue>:<what>` in the `queen-s3` namespace
//! ([`crate::queen::KV_NAMESPACE`]):
//!
//! | key | written by | expiry |
//! |---|---|---|
//! | `…:intent` | plan §4.3 step 4 | forever |
//! | `…:committed` | plan §4.3 step 6 | forever |
//! | `…:lease` | this module | `QUEEN_S3_LEASE_TTL_MS` |
//!
//! The queue name is escaped with [`crate::layout::escape`] (the offsets.rs
//! rule): a queue named `a:b` must not be able to address another queue's
//! documents. The sink name is not escaped and does not need to be — `config.rs`
//! already restricts it to `[A-Za-z0-9._-]`, for this reason and for the bucket
//! path.
//!
//! The two pointer keys are written "forever": an expired commit pointer is a
//! silent full replay (plan §12).

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::layout::escape;
use crate::obs::now_epoch_ms;
use crate::queen::{KvOp, KvResult, QueenApi, Result};
use crate::types::{Lease as LeaseDoc, SinkError};

/// The KV key of one document of one (sink, queue).
pub fn kv_key(sink: &str, queue: &str, what: &str) -> String {
    format!("s3:{sink}:{}:{what}", escape(queue))
}

/// `s3:<sink>:<esc queue>:intent` — plan §4.3 step 4.
pub fn intent_key(sink: &str, queue: &str) -> String {
    kv_key(sink, queue, "intent")
}

/// `s3:<sink>:<esc queue>:committed` — the commit pointer, plan §4.3 step 6.
pub fn committed_key(sink: &str, queue: &str) -> String {
    kv_key(sink, queue, "committed")
}

/// `s3:<sink>:<esc queue>:lease` — queue ownership, plan §6.6.
pub fn lease_key(sink: &str, queue: &str) -> String {
    kv_key(sink, queue, "lease")
}

/// The result of one claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Acquired {
    /// This instance now owns the queue.
    Taken,
    /// Somebody else does, and here is the `instance` field of their row. The
    /// caller waits out the TTL and tries again — a lease is not a queue.
    HeldBy(String),
}

/// One queue's lease, held by one instance.
///
/// The held version lives in an [`AtomicI64`] rather than behind a lock because
/// it is read by [`Lease::fence_op`] on the queue task and written by the
/// refresh task, and both operations are a single word. `0` is "not held",
/// which is also exactly how the store reports a key that is absent or expired.
pub struct Lease {
    queen: Arc<dyn QueenApi>,
    key: String,
    queue: String,
    instance: String,
    /// One process life. A restart of the same instance is a NEW incarnation, so
    /// a lease row that outlives the process that took it is never mistaken for
    /// this process's own.
    incarnation: String,
    ttl_seconds: u64,
    since_ms: AtomicI64,
    version: AtomicI64,
    lost: AtomicBool,
}

impl Lease {
    /// A lease handle for `queue`. Nothing is claimed until [`Lease::acquire`].
    pub fn new(
        queen: Arc<dyn QueenApi>,
        sink: &str,
        queue: &str,
        instance: &str,
        ttl_ms: u64,
    ) -> Lease {
        Lease {
            queen,
            key: lease_key(sink, queue),
            queue: queue.to_string(),
            instance: instance.to_string(),
            incarnation: mint_incarnation(),
            // The store's TTL is in seconds and it is a liveness claim, so it
            // rounds DOWN to at least one second: a lease that outlived its
            // configured window would be a fence that holds after the owner is
            // gone.
            ttl_seconds: (ttl_ms / 1_000).max(1),
            since_ms: AtomicI64::new(0),
            version: AtomicI64::new(0),
            lost: AtomicBool::new(false),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn instance(&self) -> &str {
        &self.instance
    }

    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }

    /// The version this instance last wrote. `0` = not held.
    pub fn version(&self) -> i64 {
        self.version.load(Ordering::SeqCst)
    }

    /// Whether a refresh or a fence has come back with a lost precondition.
    /// Once true it stays true: this handle is finished, and the queue task
    /// that owns it must stop.
    pub fn lost(&self) -> bool {
        self.lost.load(Ordering::SeqCst)
    }

    pub fn held(&self) -> bool {
        self.version() != 0 && !self.lost()
    }

    /// A third of the TTL, floored at 100 ms: two refreshes may be lost before
    /// the row expires.
    pub fn refresh_interval(&self) -> Duration {
        Duration::from_millis(((self.ttl_seconds * 1_000) / 3).max(100))
    }

    pub fn ttl_seconds(&self) -> u64 {
        self.ttl_seconds
    }

    /// Claim the queue.
    ///
    /// `putIfAbsent` and not a `put`: an expired-but-unpruned row loses to it
    /// (024:1010-1015) while a LIVE row wins and hands back its own value and
    /// version, so a loser learns who owns the queue without a second call.
    pub async fn acquire(&self) -> Result<Acquired> {
        let since_ms = now_epoch_ms();
        let doc = self.doc_at(since_ms);
        let results = self
            .queen
            .kv(vec![KvOp::put_if_absent_ttl(
                self.key.clone(),
                doc,
                self.ttl_seconds,
            )])
            .await?;
        let r = result_at(&results, 0).ok_or_else(|| {
            SinkError::Body(format!(
                "kv answered nothing for the lease claim {}",
                self.key
            ))
        })?;
        if r.did_apply() {
            self.since_ms.store(since_ms, Ordering::SeqCst);
            self.version.store(r.version, Ordering::SeqCst);
            self.lost.store(false, Ordering::SeqCst);
            return Ok(Acquired::Taken);
        }
        Ok(Acquired::HeldBy(holder_of(&r.value)))
    }

    /// The operation that goes at **index 0** of every intent and commit batch.
    ///
    /// It is a write and not a read on purpose: it renews the TTL — a queue that
    /// is committing is a queue whose owner is alive, so a commit IS a heartbeat
    /// — and it takes a new version, which the caller must absorb with
    /// [`Lease::note`] or the next batch will fence itself out.
    pub fn fence_op(&self) -> KvOp {
        KvOp::fence_ttl(
            self.key.clone(),
            self.doc(),
            self.version(),
            self.ttl_seconds,
        )
    }

    /// Absorb the answer to a batch whose index 0 was [`Lease::fence_op`].
    ///
    /// A `required` write that lost never arrives here — it comes back as
    /// [`SinkError::Precondition`] for the whole batch — so anything other than
    /// "applied, new version" is a store that did not do what was asked, and the
    /// safe reading of that is that this instance no longer holds the queue.
    pub fn note(&self, results: &[KvResult]) {
        match result_at(results, 0) {
            Some(r) if r.did_apply() && r.version != 0 => {
                self.version.store(r.version, Ordering::SeqCst);
            }
            _ => self.mark_lost(),
        }
    }

    /// Renew the lease. `Err(`[`SinkError::Precondition`]`)` means the queue is
    /// somebody else's now and this instance must stop reading it.
    pub async fn refresh(&self) -> Result<()> {
        let results = match self.queen.kv(vec![self.fence_op()]).await {
            Ok(results) => results,
            Err(e) => {
                if matches!(e, SinkError::Precondition { .. }) {
                    self.mark_lost();
                }
                return Err(e);
            }
        };
        let applied = result_at(&results, 0)
            .map(KvResult::did_apply)
            .unwrap_or(false);
        self.note(&results);
        if !applied {
            let r = result_at(&results, 0);
            return Err(SinkError::Precondition {
                failed_index: 0,
                reason: r
                    .and_then(|r| r.reason.clone())
                    .unwrap_or_else(|| "no result".to_string()),
                version: r.map(|r| r.version).unwrap_or(0),
                value: r.map(|r| r.value.clone()).unwrap_or(Value::Null),
            });
        }
        Ok(())
    }

    /// Give the queue back, so another instance takes it without waiting out the
    /// TTL. Best effort by construction: the fenced delete either applies or the
    /// lease was already somebody else's, and neither outcome is worth a retry
    /// on a process that is stopping.
    pub async fn release(&self) {
        let version = self.version();
        if version == 0 || self.lost() {
            return;
        }
        let _ = self
            .queen
            .kv(vec![KvOp::delete(self.key.clone(), Some(version))])
            .await;
        self.version.store(0, Ordering::SeqCst);
    }

    /// Declare this handle finished. Idempotent.
    pub fn mark_lost(&self) {
        self.lost.store(true, Ordering::SeqCst);
    }

    fn doc(&self) -> Value {
        self.doc_at(self.since_ms.load(Ordering::SeqCst))
    }

    fn doc_at(&self, since_ms: i64) -> Value {
        serde_json::to_value(LeaseDoc {
            instance: self.instance.clone(),
            incarnation: self.incarnation.clone(),
            since_ms,
        })
        .unwrap_or(Value::Null)
    }
}

/// Keep the lease alive while the queue task works.
///
/// The refresh is a task and not a step of the driver's loop because the loop
/// can legitimately be inside one long thing — a 128 MiB upload to a gateway
/// having a bad minute — for longer than the TTL, and a lease that expires under
/// a healthy owner would hand the queue to a second instance for no reason. The
/// handle aborts the task when it drops, so a queue task that returns takes its
/// refresher with it.
pub struct RefreshTask {
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for RefreshTask {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Refresh `lease` every [`Lease::refresh_interval`] until it is lost.
///
/// A transient failure is not a loss: the tick is skipped and the next one tries
/// again. What ends the task is a lost precondition — somebody else holds the
/// row, or it expired and was taken — and that also sets [`Lease::lost`], which
/// is what the queue task polls.
pub fn spawn_refresh(lease: Arc<Lease>) -> RefreshTask {
    let every = lease.refresh_interval();
    let handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(every).await;
            if lease.lost() {
                return;
            }
            match lease.refresh().await {
                Ok(()) => {}
                Err(e) if e.is_retriable() => {
                    tracing::warn!(
                        target: "queen-s3",
                        queue = %lease.queue(),
                        error = %e,
                        "lease refresh failed; retrying on the next tick"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        target: "queen-s3",
                        queue = %lease.queue(),
                        error = %e,
                        "lease lost: another instance owns this queue"
                    );
                    lease.mark_lost();
                    return;
                }
            }
        }
    });
    RefreshTask { handle }
}

/// The result of operation `index`, by the `index` each answer carries rather
/// than by position — the broker stamps them (kv.rs) and the two agree, but a
/// commit that read the wrong slot would fence the wrong thing.
pub fn result_at(results: &[KvResult], index: usize) -> Option<&KvResult> {
    results
        .iter()
        .find(|r| r.index == index)
        .or_else(|| results.get(index))
}

/// The `instance` of whoever holds the lease, for one log line. A value this
/// process did not write is data, so a shape it does not recognise is reported
/// as unknown rather than parsed at.
fn holder_of(value: &Value) -> String {
    serde_json::from_value::<LeaseDoc>(value.clone())
        .map(|l| l.instance)
        .unwrap_or_else(|_| "unknown".to_string())
}

/// A short random tag, distinct for every process. `rand` is already a
/// dependency for the S3 client's backoff jitter.
fn mint_incarnation() -> String {
    let n: u64 = rand::random();
    format!("{n:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queen::FakeQueen;

    fn lease(queen: &Arc<FakeQueen>, instance: &str) -> Lease {
        Lease::new(queen.clone(), "default", "orders", instance, 30_000)
    }

    #[test]
    fn keys_escape_the_queue_and_name_the_document() {
        assert_eq!(
            lease_key("default", "orders"),
            "s3:default:orders:lease",
            "an ordinary name is never rewritten"
        );
        assert_eq!(intent_key("sink-2", "a/b"), "s3:sink-2:a%2Fb:intent");
        assert_eq!(
            committed_key("default", "a b"),
            "s3:default:a%20b:committed"
        );
        assert_eq!(
            kv_key("default", "a:b", "lease"),
            "s3:default:a%3Ab:lease",
            "a colon in a queue name must not address another queue's documents"
        );
    }

    #[tokio::test]
    async fn the_first_claim_wins_and_the_second_is_told_who_owns_it() {
        let queen: Arc<FakeQueen> = Arc::new(FakeQueen::new());
        let a = lease(&queen, "a");
        let b = lease(&queen, "b");
        assert_eq!(a.acquire().await.unwrap(), Acquired::Taken);
        assert!(a.held());
        assert_eq!(
            b.acquire().await.unwrap(),
            Acquired::HeldBy("a".to_string()),
            "the loser learns the owner from the answer, not from a second call"
        );
        assert!(!b.held());
    }

    #[tokio::test]
    async fn a_refresh_takes_a_new_version_and_the_fence_follows_it() {
        let queen: Arc<FakeQueen> = Arc::new(FakeQueen::new());
        let a = lease(&queen, "a");
        a.acquire().await.unwrap();
        let v0 = a.version();
        a.refresh().await.unwrap();
        let v1 = a.version();
        assert_ne!(v0, v1, "every write takes a fresh version (no ABA)");
        match a.fence_op() {
            KvOp::Put {
                expect, required, ..
            } => {
                assert_eq!(expect, Some(v1));
                assert!(required, "the fence rolls the whole batch back");
            }
            other => panic!("the fence is a conditional put, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_stale_fence_loses_the_whole_batch_and_the_lease_with_it() {
        let queen: Arc<FakeQueen> = Arc::new(FakeQueen::new());
        let a = lease(&queen, "a");
        a.acquire().await.unwrap();
        // Somebody else takes the row over: the version moves under `a`.
        queen.kv_seed(a.key(), serde_json::json!({"instance":"b"}));

        let err = a
            .queen
            .kv(vec![
                a.fence_op(),
                KvOp::put("s3:default:orders:committed", serde_json::json!({"k":1})),
            ])
            .await
            .expect_err("a stale fence must fail the batch");
        match err {
            SinkError::Precondition { failed_index, .. } => assert_eq!(failed_index, 0),
            other => panic!("expected a lost precondition, got {other}"),
        }
        assert_eq!(
            queen.kv_get("s3:default:orders:committed"),
            None,
            "`required` rolls the WHOLE batch back: the pointer must not have moved"
        );
        assert!(a.refresh().await.is_err());
        assert!(a.lost(), "a lost precondition ends this handle");
    }

    #[tokio::test]
    async fn an_expired_lease_is_reclaimable_by_the_next_instance() {
        let queen: Arc<FakeQueen> = Arc::new(FakeQueen::new());
        queen.set_now_ms(1_000_000);
        let a = lease(&queen, "a");
        a.acquire().await.unwrap();
        let b = lease(&queen, "b");
        assert!(matches!(b.acquire().await.unwrap(), Acquired::HeldBy(_)));
        queen.advance_ms(31_000);
        assert_eq!(
            b.acquire().await.unwrap(),
            Acquired::Taken,
            "putIfAbsent wins against an expired row"
        );
        assert!(
            a.refresh().await.is_err(),
            "and the previous owner is fenced out on its next write"
        );
    }

    #[tokio::test]
    async fn release_hands_the_queue_back_without_waiting_out_the_ttl() {
        let queen: Arc<FakeQueen> = Arc::new(FakeQueen::new());
        let a = lease(&queen, "a");
        a.acquire().await.unwrap();
        a.release().await;
        assert_eq!(queen.kv_get(a.key()), None);
        let b = lease(&queen, "b");
        assert_eq!(b.acquire().await.unwrap(), Acquired::Taken);
    }

    #[test]
    fn the_refresh_interval_is_a_third_of_the_ttl() {
        let queen: Arc<FakeQueen> = Arc::new(FakeQueen::new());
        let l = Lease::new(queen, "default", "q", "i", 30_000);
        assert_eq!(l.ttl_seconds(), 30);
        assert_eq!(l.refresh_interval(), Duration::from_millis(10_000));
    }
}
