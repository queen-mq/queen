//! lease — queue ownership across nodes, and the commit fence (plan §6.6).
//!
//! Every node of a cluster runs the sink with the same configuration, so every
//! node has a task for every queue. One node runs a queue only while it holds
//! that queue's **lease**, a TTL'd key/value row taken with `putIfAbsent` and
//! refreshed every third of its TTL; the others are told who holds it and claim
//! again after the TTL. That much is the `cluster/registry.rs` shape and it is
//! only half of what this module is for.
//!
//! The other half is the **fence**, and it is the part that makes two writers
//! safe rather than merely unlikely. Every intent batch and every commit batch
//! carries, at index 0, a conditional write of the lease row with
//! `expect: <the version this node last wrote>` and `"required": true`. The
//! broker's KV planner judges every `expect` against the version the previous
//! writer left, serially, and a `required` precondition that loses rolls the
//! WHOLE batch back (server/src/rsm/planner/kv.rs). So a node whose lease was
//! taken away — because it stalled long enough for the TTL to pass, because it
//! was cut off from the leader, because a second sink with the same name runs
//! elsewhere — cannot move the commit pointer even if it is otherwise perfectly
//! healthy and mid-upload. Its batch fails with [`SinkError::Precondition`] and
//! its queue task stops. Two nodes can never commit different window `k`s for
//! one queue, and the object one of them wrote is either identical (same
//! intent, plan §4.2) or never committed.
//!
//! The pointers a new owner restores are the latest ones: through the broker's
//! adapter a read-only KV call waits for the cluster's read index before it
//! reads, and a call with a write reads at its own entry, so KV reads are
//! linearizable. The pointer writes still expect the versions that were read,
//! so a write made in between by anyone else — another node, an earlier life
//! of this one — is never overwritten: this node's write is the one that fails.
//!
//! The lease is **always on**. It costs one KV row per queue and one small write
//! every third of the TTL, and the write is itself useful: it is a log entry,
//! so it keeps every node's `safeTime` moving on a broker with no other traffic
//! (see [`crate::window::EngineConfig::max_window`]).
//!
//! **One writer of the row at a time, per handle.** The refresh task and the
//! queue task's fenced batches all write the same row, each expecting the
//! version this handle last saw. Two of them in flight together expect the
//! SAME version, and whichever applies second loses its precondition: the node
//! fences itself out of its own queue. So every write of the row — the claim,
//! the refresh, a fenced batch ([`Lease::fenced`]), the release — holds the
//! handle's write lock from the moment its expected version is read until its
//! answer has been absorbed. The fence of a batch is a write of the row, and
//! renews it like a refresh does.
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
//! documents. The broker builds the `…:committed` key itself, with the same
//! escaping, to read the commit pointer for a queue's retention hold
//! (server/src/rsm/maintenance.rs `sink_floor`, `percent_escape`), so the two
//! must agree byte for byte — a test pins it. The sink name is not escaped and
//! does not need to be — `config.rs` already restricts it to `[A-Za-z0-9._-]`,
//! so `s3:<sink>:` is never a prefix of another sink's keys. The sink name is
//! in no object key; two sinks writing one queue are told apart in the bucket
//! by `QUEEN_S3_PREFIX` alone.
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

/// Who holds a queue's lease, as one read of the row says — before a claim,
/// so that a claim is made only for a queue that can be had.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Holder {
    /// No live row: whoever claims first owns the queue.
    Free,
    /// A live row naming this node's instance: an earlier life of this node (a
    /// sink restarted inside the broker, a broker restarted inside the TTL).
    /// [`Lease::acquire`] takes it over at once.
    Mine,
    /// A live row naming another instance — that instance.
    Other(String),
}

/// How long [`Lease::release`] keeps trying while the broker cannot answer —
/// a SIGTERMed leader releases its leases DURING its own leadership hand-off,
/// so its first calls can meet no leader. Ten seconds is comfortably inside the
/// broker's shutdown grace (30 s by default), and far shorter than the lease
/// TTL a lease left behind would cost every queue it held.
pub const RELEASE_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

/// The first wait between two attempts of a release, doubled after each, up
/// to [`RELEASE_RETRY_MAX`].
const RELEASE_RETRY_MIN: std::time::Duration = std::time::Duration::from_millis(100);
const RELEASE_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(1);

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
    /// One handle's life. A restart of the same node is a NEW incarnation, so a
    /// lease row that outlives the process that took it is never mistaken for
    /// this one's own.
    incarnation: String,
    ttl_seconds: u64,
    since_ms: AtomicI64,
    version: AtomicI64,
    lost: AtomicBool,
    /// Held across every write of the row, from reading the version it expects
    /// to absorbing the answer (module docs).
    writing: tokio::sync::Mutex<()>,
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
            writing: tokio::sync::Mutex::new(()),
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

    /// Who holds the queue now: one read of the row, no write. A row this node
    /// cannot parse is somebody else's.
    pub async fn holder(&self) -> Result<Holder> {
        let results = self.queen.kv(vec![KvOp::get(self.key.clone())]).await?;
        let Some(r) = result_at(&results, 0).filter(|r| r.found == Some(true)) else {
            return Ok(Holder::Free);
        };
        Ok(match serde_json::from_value::<LeaseDoc>(r.value.clone()) {
            Ok(doc) if doc.instance == self.instance => Holder::Mine,
            Ok(doc) => Holder::Other(doc.instance),
            Err(_) => Holder::Other("unknown".to_string()),
        })
    }

    /// Claim the queue.
    ///
    /// `putIfAbsent` and not a `put`: an expired-but-unswept row loses to it
    /// (liveness is `expires > now` for every reader and writer) while a LIVE
    /// row wins and hands back its own value and version, so a loser learns who
    /// owns the queue without a second call.
    ///
    /// A live row naming THIS instance under another incarnation is an earlier
    /// life of this node, which no longer runs the queue: it is taken over at
    /// once, at the version it has, rather than waited out for a TTL. The fence
    /// keeps that safe whatever the earlier life is doing — its next fenced
    /// write expects a version that is gone.
    pub async fn acquire(&self) -> Result<Acquired> {
        let _writing = self.writing.lock().await;
        let since_ms = now_epoch_ms();
        let doc = self.doc_at(since_ms);
        let results = self
            .queen
            .kv(vec![KvOp::put_if_absent_ttl(
                self.key.clone(),
                doc.clone(),
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
        let earlier_life = serde_json::from_value::<LeaseDoc>(r.value.clone()).is_ok_and(|held| {
            held.instance == self.instance && held.incarnation != self.incarnation
        });
        if !earlier_life {
            return Ok(Acquired::HeldBy(holder_of(&r.value)));
        }
        let take_over = KvOp::fence_ttl(self.key.clone(), doc, r.version, self.ttl_seconds);
        match self.queen.kv(vec![take_over]).await {
            Ok(results) => match result_at(&results, 0) {
                Some(t) if t.did_apply() && t.version != 0 => {
                    self.since_ms.store(since_ms, Ordering::SeqCst);
                    self.version.store(t.version, Ordering::SeqCst);
                    self.lost.store(false, Ordering::SeqCst);
                    Ok(Acquired::Taken)
                }
                Some(t) => Ok(Acquired::HeldBy(holder_of(&t.value))),
                None => Ok(Acquired::HeldBy(self.instance.clone())),
            },
            // Somebody wrote the row in between: it is theirs.
            Err(SinkError::Precondition { value, .. }) => Ok(Acquired::HeldBy(holder_of(&value))),
            Err(e) => Err(e),
        }
    }

    /// One KV batch behind the fence: [`Lease::fence_op`] at index 0, `ops`
    /// after it, sent and absorbed under the write lock, so the refresh task
    /// cannot write the row between the version this fence expects and the
    /// version it leaves. Indices in the answer are the batch's: `ops[i]` is
    /// answered at `i + 1`.
    ///
    /// A lost precondition at index 0 marks the handle lost. A transport error
    /// changes nothing here: whether the batch applied is unknown, and the
    /// caller decides what to do about it.
    pub async fn fenced(&self, ops: Vec<KvOp>) -> Result<Vec<KvResult>> {
        let _writing = self.writing.lock().await;
        let mut batch = Vec::with_capacity(ops.len() + 1);
        batch.push(self.fence_op());
        batch.extend(ops);
        match self.queen.kv(batch).await {
            Ok(results) => {
                self.note(&results);
                Ok(results)
            }
            Err(e) => {
                if matches!(
                    e,
                    SinkError::Precondition {
                        failed_index: 0,
                        ..
                    }
                ) {
                    self.mark_lost();
                }
                Err(e)
            }
        }
    }

    /// The operation that goes at **index 0** of every intent and commit batch.
    ///
    /// It is a write and not a read on purpose: it renews the TTL — a queue that
    /// is committing is a queue whose owner is alive, so a commit IS a heartbeat
    /// — and it takes a new version, which must be absorbed with [`Lease::note`]
    /// before anything else writes the row. [`Lease::fenced`] does both under
    /// the write lock; a batch built by hand around this op races the refresh.
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
        let _writing = self.writing.lock().await;
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

    /// Give the queue back, so another node takes it without waiting out the
    /// TTL.
    ///
    /// The row is read first and deleted at the version it HAS, when it still
    /// names this handle's incarnation: the version this handle remembers can be
    /// behind the row's — a refresh cancelled with its write already applied, a
    /// batch whose answer was lost — and a delete expecting it would leave the
    /// queue held until the TTL ran out. A row naming anyone else is left
    /// alone.
    ///
    /// A failure that passes — a transport error, a 408/429/5xx, no leader —
    /// is retried, 100 ms doubling to 1 s, for at most [`RELEASE_BUDGET`]: the
    /// broker begins its drain at the signal, so on a leader every release runs
    /// during the node's own leadership hand-off, and one failed call used to
    /// leave the lease to expire. Anything else — a precondition, a refusal —
    /// is final: the row is somebody else's, or nothing will change by asking
    /// again.
    pub async fn release(&self) {
        let _writing = self.writing.lock().await;
        if self.version() == 0 {
            return;
        }
        self.version.store(0, Ordering::SeqCst);
        delete_row_if_ours(&self.queen, &self.key, &self.incarnation, "lease").await;
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
    use tracing::Instrument;
    let every = lease.refresh_interval();
    // In the queue task's span, so its lines name the tenant and the queue.
    let span = tracing::Span::current();
    let handle = tokio::spawn(
        async move {
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
        }
        .instrument(span),
    );
    RefreshTask { handle }
}

/// The result of operation `index`, by the `index` each answer carries rather
/// than by position — the broker stamps them and the two agree, but a commit
/// that read the wrong slot would fence the wrong thing.
pub fn result_at(results: &[KvResult], index: usize) -> Option<&KvResult> {
    results
        .iter()
        .find(|r| r.index == index)
        .or_else(|| results.get(index))
}

/// The `instance` of whoever holds the lease, for one log line. A value this
/// node did not write is data, so a shape it does not recognise is reported as
/// unknown rather than parsed at.
fn holder_of(value: &Value) -> String {
    serde_json::from_value::<LeaseDoc>(value.clone())
        .map(|l| l.instance)
        .unwrap_or_else(|_| "unknown".to_string())
}

/// Delete `key` when its row still names `incarnation`, retrying a failure
/// that passes — a transport error, a 408/429/5xx, no leader — 100 ms doubling
/// to 1 s, for at most [`RELEASE_BUDGET`]. A row naming anyone else is left
/// alone, and a lost precondition is never retried: the row is somebody
/// else's. `what` names the row in the one line that reports giving up.
pub(crate) async fn delete_row_if_ours(
    queen: &Arc<dyn QueenApi>,
    key: &str,
    incarnation: &str,
    what: &str,
) {
    let deadline = tokio::time::Instant::now() + RELEASE_BUDGET;
    let mut wait = RELEASE_RETRY_MIN;
    loop {
        match delete_once(queen, key, incarnation).await {
            Ok(()) => return,
            Err(e) if e.is_retriable() && tokio::time::Instant::now() + wait <= deadline => {
                tracing::debug!(
                    target: "queen-s3",
                    key,
                    error = %e,
                    retry_in_ms = wait.as_millis() as u64,
                    "giving a {what} back failed; retrying"
                );
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(RELEASE_RETRY_MAX);
            }
            Err(e) => {
                tracing::warn!(
                    target: "queen-s3",
                    key,
                    error = %e,
                    "could not give the {what} back; it expires after its TTL"
                );
                return;
            }
        }
    }
}

/// One attempt of [`delete_row_if_ours`]: `Ok` once the row is not ours any
/// more — deleted now, already gone, or somebody else's.
async fn delete_once(queen: &Arc<dyn QueenApi>, key: &str, incarnation: &str) -> Result<()> {
    let results = queen.kv(vec![KvOp::get(key.to_string())]).await?;
    let Some(row) = result_at(&results, 0).filter(|r| r.found == Some(true)) else {
        return Ok(());
    };
    let ours = serde_json::from_value::<LeaseDoc>(row.value.clone())
        .is_ok_and(|doc| doc.incarnation == incarnation);
    if !ours {
        return Ok(());
    }
    // Not `required`: a delete that loses its precondition answers "not
    // applied", which means another writer took the row in between — theirs
    // now, and nothing to retry.
    queen
        .kv(vec![KvOp::delete(key.to_string(), Some(row.version))])
        .await
        .map(|_| ())
}

/// A short random tag, distinct for every lease handle. `rand` is already a
/// dependency for the S3 client's backoff jitter.
pub(crate) fn mint_incarnation() -> String {
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

    /// The broker reads the commit pointer of a held queue by building this key
    /// itself (server/src/rsm/maintenance.rs `sink_floor`: `s3:<sink>:` +
    /// `percent_escape(queue)` + `:committed`, keeping `[A-Za-z0-9._-]` and
    /// `%XX`-encoding every other BYTE with uppercase hex). A queue name with a
    /// space, a slash, a non-ASCII letter and a colon exercises every class.
    #[test]
    fn the_committed_key_is_the_one_the_retention_hold_builds() {
        assert_eq!(
            committed_key("default", "a b/ü:c"),
            "s3:default:a%20b%2F%C3%BC%3Ac:committed"
        );
        assert_eq!(
            committed_key("lake-eu.1", "Orders_2026.v-1"),
            "s3:lake-eu.1:Orders_2026.v-1:committed",
            "the kept set is kept verbatim, case included"
        );
        assert_eq!(
            crate::queen::KV_NAMESPACE,
            "queen-s3",
            "the namespace the hold reads"
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

    /// A fenced batch and a refresh in flight at the same time: without the
    /// write lock both would expect the same version and the second to apply
    /// would fence this handle out of its own queue.
    #[tokio::test(start_paused = true)]
    async fn a_refresh_and_a_fenced_batch_in_flight_together_do_not_fence_each_other() {
        let queen: Arc<FakeQueen> = Arc::new(FakeQueen::new());
        let a = lease(&queen, "a");
        a.acquire().await.unwrap();
        queen.set_kv_latency(Duration::from_millis(50));
        for _ in 0..3 {
            let (batch, refresh) = tokio::join!(
                a.fenced(vec![KvOp::put(
                    "s3:default:orders:committed",
                    serde_json::json!({"k": 1})
                )]),
                a.refresh(),
            );
            let results = batch.expect("the fenced batch applies");
            assert!(results[0].did_apply() && results[1].did_apply());
            refresh.expect("and so does the refresh");
        }
        assert!(!a.lost());
        assert_eq!(
            queen.kv_version(a.key()),
            a.version(),
            "the handle knows the version the row has"
        );
    }

    /// The handle's version can be behind the row's — a write of its own that
    /// applied but whose answer never came back. Release still gives the queue
    /// back: it deletes the row at the version the row has, because the row
    /// names this handle's incarnation. A row naming anyone else stays.
    #[tokio::test]
    async fn release_gives_back_a_row_whose_version_it_did_not_see_and_only_its_own() {
        let queen: Arc<FakeQueen> = Arc::new(FakeQueen::new());
        let a = lease(&queen, "a");
        a.acquire().await.unwrap();
        let own = serde_json::to_value(LeaseDoc {
            instance: "a".into(),
            incarnation: a.incarnation().into(),
            since_ms: 0,
        })
        .unwrap();
        queen.kv_seed(a.key(), own);
        assert_ne!(queen.kv_version(a.key()), a.version());
        a.release().await;
        assert_eq!(
            queen.kv_get(a.key()),
            None,
            "released without waiting out the TTL"
        );

        let c = lease(&queen, "c");
        c.acquire().await.unwrap();
        queen.kv_seed(
            c.key(),
            serde_json::json!({"instance": "d", "incarnation": "x", "sinceMs": 0}),
        );
        c.release().await;
        assert_eq!(
            queen.kv_get(c.key()).unwrap()["instance"],
            "d",
            "another instance's row is not this handle's to delete"
        );
    }

    /// One read says who holds the row, and a claim needs no write to learn
    /// that the queue is somebody else's.
    #[tokio::test]
    async fn holder_reads_the_row_without_writing_it() {
        let queen: Arc<FakeQueen> = Arc::new(FakeQueen::new());
        let a = lease(&queen, "a");
        assert_eq!(a.holder().await.unwrap(), Holder::Free);
        a.acquire().await.unwrap();
        let version = queen.kv_version(a.key());
        let b = lease(&queen, "b");
        assert_eq!(b.holder().await.unwrap(), Holder::Other("a".into()));
        let again = lease(&queen, "a");
        assert_eq!(again.holder().await.unwrap(), Holder::Mine);
        assert_eq!(queen.kv_version(a.key()), version, "nothing was written");
    }

    /// A live row this instance wrote in an earlier life is taken over at once;
    /// the earlier life is fenced out, and another instance's row is never
    /// taken over.
    #[tokio::test]
    async fn an_earlier_life_of_this_instance_is_taken_over_without_waiting() {
        let queen: Arc<FakeQueen> = Arc::new(FakeQueen::new());
        let earlier = lease(&queen, "a");
        earlier.acquire().await.unwrap();
        let now = lease(&queen, "a");
        assert_eq!(now.acquire().await.unwrap(), Acquired::Taken);
        assert!(now.held());
        assert_eq!(
            queen.kv_get(now.key()).unwrap()["incarnation"],
            now.incarnation(),
            "the row names the new life"
        );
        assert!(
            earlier.refresh().await.is_err(),
            "the earlier life is fenced out"
        );

        let b = lease(&queen, "b");
        assert_eq!(b.acquire().await.unwrap(), Acquired::HeldBy("a".into()));
        assert!(!b.held());
    }

    /// A release that meets no leader for its first calls — the SIGTERMed
    /// leader's case — keeps trying and gives the queue back.
    #[tokio::test(start_paused = true)]
    async fn release_rides_out_a_leaderless_moment() {
        let queen: Arc<FakeQueen> = Arc::new(FakeQueen::new());
        let a = lease(&queen, "a");
        a.acquire().await.unwrap();
        let before = queen.kv_calls();
        queen.fail_kv_next(4);
        let started = tokio::time::Instant::now();
        a.release().await;
        assert_eq!(
            queen.kv_get(a.key()),
            None,
            "given back after four failed calls"
        );
        assert_eq!(
            queen.kv_calls() - before,
            6,
            "four failures, then the get and the delete"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "100 ms doubling: {:?}",
            started.elapsed()
        );
    }

    /// The retries are bounded: a broker that never answers costs at most
    /// RELEASE_BUDGET, and the lease is then left to its TTL.
    #[tokio::test(start_paused = true)]
    async fn release_gives_up_after_its_budget() {
        let queen: Arc<FakeQueen> = Arc::new(FakeQueen::new());
        let a = lease(&queen, "a");
        a.acquire().await.unwrap();
        queen.fail_kv_next(1_000);
        let started = tokio::time::Instant::now();
        a.release().await;
        let took = started.elapsed();
        assert!(
            took <= RELEASE_BUDGET && took >= RELEASE_BUDGET - Duration::from_secs(2),
            "{took:?}"
        );
        queen.fail_kv_next(0);
        assert!(queen.kv_get(a.key()).is_some(), "left to expire");
    }

    #[test]
    fn the_refresh_interval_is_a_third_of_the_ttl() {
        let queen: Arc<FakeQueen> = Arc::new(FakeQueen::new());
        let l = Lease::new(queen, "default", "q", "i", 30_000);
        assert_eq!(l.ttl_seconds(), 30);
        assert_eq!(l.refresh_interval(), Duration::from_millis(10_000));
    }
}
