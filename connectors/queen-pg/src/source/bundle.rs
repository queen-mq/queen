//! The bundler (PLAN §4.4): source transactions and snapshot chunks become
//! `/transaction` calls that push their events AND move the pointer, all or
//! nothing; and the decision table for every answer that is not a plain
//! success (I5).
//!
//! The queue holds **units** in stream order. A unit is a complete source
//! transaction, one chunk of a split transaction, one part of a snapshot
//! chunk, or a position change with no events (an idle advance, a transaction
//! of a table this source does not map). Each unit carries the pointer
//! position that holds once it is in Queen (its **cover**), so a bundle — a
//! prefix of the queue — writes the cover of its last unit. Units are never
//! split across bundles; a bundle takes whole units until the next one would
//! pass `maxBundleMessages` / `maxBundleBytes`.
//!
//! One bundle in flight at a time: the next one is planned against the
//! version the previous one returned, so they could not commit out of order
//! even if two were sent.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use crate::error::{Error, Result};
use crate::queen::{KvOp, QueenApi, QueenError};

use super::events::Item;
use super::pointer::{self, Held, Pointer, Position};

/// The flush limits (`maxBundleMessages`, `maxBundleBytes`, `lingerMs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_messages: usize,
    pub max_bytes: usize,
    pub linger: Duration,
}

/// A run of events that commits whole, with the position it reaches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unit {
    pub items: Vec<Item>,
    pub bytes: usize,
    pub cover: Position,
    /// Source transactions this unit completes (counters).
    pub txns: u64,
    pub snapshot_rows: u64,
    /// The commit time of the last source transaction in it.
    pub last_commit_us: Option<i64>,
    /// Flush even with no events (idle advancement, a snapshot step).
    pub force: bool,
}

impl Unit {
    pub fn new(items: Vec<Item>, cover: Position) -> Unit {
        let bytes = items.iter().map(Item::wire_len).sum();
        Unit {
            items,
            bytes,
            cover,
            txns: 0,
            snapshot_rows: 0,
            last_commit_us: None,
            force: false,
        }
    }
}

/// What one `/transaction` carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bundle {
    pub items: Vec<Item>,
    pub cover: Position,
    pub txns: u64,
    pub bytes: usize,
    pub snapshot_rows: u64,
    pub last_commit_us: Option<i64>,
}

/// The queue of units waiting for a bundle.
#[derive(Debug)]
pub struct Bundler {
    units: VecDeque<(Unit, Instant)>,
    items: usize,
    bytes: usize,
    limits: Limits,
}

impl Bundler {
    pub fn new(limits: Limits) -> Bundler {
        Bundler {
            units: VecDeque::new(),
            items: 0,
            bytes: 0,
            limits,
        }
    }

    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// Queue `u`. A position change with no events directly after another one
    /// replaces it (the later position covers the earlier), so a stream of
    /// unmapped transactions costs one unit, not one each.
    pub fn push(&mut self, u: Unit, now: Instant) {
        if u.items.is_empty() && !u.force {
            if let Some((last, _)) = self.units.back_mut() {
                if last.items.is_empty() && !last.force {
                    last.cover = u.cover;
                    last.txns += u.txns;
                    last.last_commit_us = u.last_commit_us.or(last.last_commit_us);
                    return;
                }
            }
        }
        self.items += u.items.len();
        self.bytes += u.bytes;
        self.units.push_back((u, now));
    }

    pub fn is_empty(&self) -> bool {
        self.units.is_empty()
    }

    pub fn items(&self) -> usize {
        self.items
    }

    pub fn has_items(&self) -> bool {
        self.items > 0
    }

    /// Two bundles' worth queued: stop reading the stream until one is sent.
    pub fn backlogged(&self) -> bool {
        self.items >= 2 * self.limits.max_messages || self.bytes >= 2 * self.limits.max_bytes
    }

    /// When the oldest queued event has waited `lingerMs`.
    pub fn deadline(&self) -> Option<Instant> {
        self.units
            .iter()
            .find(|(u, _)| !u.items.is_empty())
            .map(|(_, at)| *at + self.limits.linger)
    }

    /// Whether a bundle should go now, the stream's state aside: a forced
    /// unit, a full bundle's worth, or the linger passed.
    pub fn due(&self, now: Instant) -> bool {
        if self.units.iter().any(|(u, _)| u.force) {
            return true;
        }
        if self.items == 0 {
            return false;
        }
        self.items >= self.limits.max_messages
            || self.bytes >= self.limits.max_bytes
            || self.deadline().is_some_and(|d| d <= now)
    }

    /// The next bundle: whole units from the front until the next one would
    /// pass a limit (always at least one), plus any position-only units that
    /// follow (free progress).
    pub fn take(&mut self) -> Option<Bundle> {
        let mut n = 0usize;
        let (mut items, mut bytes) = (0usize, 0usize);
        for (u, _) in &self.units {
            let heavy = !u.items.is_empty();
            if heavy
                && items > 0
                && (items + u.items.len() > self.limits.max_messages
                    || bytes + u.bytes > self.limits.max_bytes)
            {
                break;
            }
            n += 1;
            items += u.items.len();
            bytes += u.bytes;
        }
        if n == 0 {
            return None;
        }
        let mut b = Bundle {
            items: Vec::with_capacity(items),
            cover: Position::default(),
            txns: 0,
            bytes,
            snapshot_rows: 0,
            last_commit_us: None,
        };
        for _ in 0..n {
            let (u, _) = self.units.pop_front()?;
            b.items.extend(u.items);
            b.cover = u.cover;
            b.txns += u.txns;
            b.snapshot_rows += u.snapshot_rows;
            b.last_commit_us = u.last_commit_us.or(b.last_commit_us);
        }
        self.items -= items;
        self.bytes -= bytes;
        Some(b)
    }
}

/// The body of `POST /api/v1/transaction`: the pushes (when any) and the KV
/// rider, as JSON text — payloads are spliced in untouched.
pub fn body(items: &[Item], kv: &KvOp) -> String {
    let cap = items.iter().map(Item::wire_len).sum::<usize>() + 512;
    let mut out = String::with_capacity(cap);
    if items.is_empty() {
        out.push_str("{\"kv\":[");
    } else {
        out.push_str("{\"operations\":[{\"type\":\"push\",\"items\":[");
        for (i, it) in items.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            it.append_json(&mut out);
        }
        out.push_str("]}],\"kv\":[");
    }
    out.push_str(&kv.to_json().to_string());
    out.push_str("]}");
    out
}

/// 1 s doubling to 30 s, or what the broker asked for.
#[derive(Debug, Clone)]
pub struct Backoff {
    next: Duration,
    min: Duration,
    max: Duration,
}

impl Default for Backoff {
    fn default() -> Backoff {
        Backoff::new(Duration::from_secs(1), Duration::from_secs(30))
    }
}

impl Backoff {
    pub fn new(min: Duration, max: Duration) -> Backoff {
        Backoff {
            next: min,
            min,
            max,
        }
    }

    pub fn step(&mut self) -> Duration {
        let d = self.next;
        self.next = (self.next * 2).min(self.max);
        d
    }

    /// `Retry-After` when the broker sent one, else the next step.
    pub fn after(&mut self, e: &QueenError) -> Duration {
        match e.retry_after_ms() {
            Some(ms) => Duration::from_millis(ms),
            None => self.step(),
        }
    }

    pub fn reset(&mut self) {
        self.next = self.min;
    }
}

/// What a read of the pointer says about a bundle whose answer was not a
/// plain success (I5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// The pointer is exactly this bundle's cover: it committed. Continue
    /// with the version read.
    Committed(i64),
    /// The pointer is still the version this owner holds: the bundle did not
    /// apply.
    NotApplied,
    /// Somebody else moved it: give everything up and start from the top.
    Moved,
}

pub fn judge(read: Option<&Held>, cover: &Pointer, held: i64) -> Verdict {
    match read {
        Some(h) if h.doc.same_place(cover) => Verdict::Committed(h.version),
        Some(h) if h.version == held => Verdict::NotApplied,
        _ => Verdict::Moved,
    }
}

/// One bundle's commit: send, and settle every answer.
pub struct Commit {
    pub api: Arc<dyn QueenApi>,
    pub key: String,
    pub body: String,
    /// The pointer this bundle writes.
    pub cover: Pointer,
    /// The version it expects.
    pub held: i64,
}

impl Commit {
    /// Build the commit of `items` moving the pointer to `cover`.
    pub fn new(
        api: Arc<dyn QueenApi>,
        key: String,
        items: &[Item],
        cover: Pointer,
        held: i64,
    ) -> Commit {
        let op = KvOp::fence(key.clone(), cover.to_value(), held);
        Commit {
            api,
            body: body(items, &op),
            key,
            cover,
            held,
        }
    }

    /// The new version once the bundle is known to be in Queen.
    ///
    /// * success → the version the KV rider returned;
    /// * `kv_precondition` / `duplicate` / an in-doubt error → READ the
    ///   pointer: our cover → committed (a lost answer, or our own retry
    ///   losing to the first attempt); unchanged → not applied (resend after
    ///   a backoff; for `duplicate` that is an invariant broken elsewhere:
    ///   stop); anything else → [`Error::Fenced`];
    /// * 429 → resend after `Retry-After`;
    /// * any other refusal → stop with the broker's reason. A transaction is
    ///   never skipped (I4).
    pub async fn run(self) -> Result<i64> {
        let mut backoff = Backoff::default();
        let mut not_applied = 0u32;
        loop {
            match self.api.transaction(self.body.clone()).await {
                Ok(a) if a.success => {
                    if let Some(r) = a.kv_result(0) {
                        if r.did_apply() && r.version != 0 {
                            return Ok(r.version);
                        }
                    }
                    // Committed, but the answer lacks the version: read it.
                    let read = self.read(&mut backoff).await?;
                    return match judge(read.as_ref(), &self.cover, self.held) {
                        Verdict::Committed(v) => Ok(v),
                        _ => Err(Error::Fenced(
                            "the bundle committed but the pointer says otherwise".into(),
                        )),
                    };
                }
                Ok(a) if a.is_precondition() || a.is_duplicate() => {
                    let read = self.read(&mut backoff).await?;
                    match judge(read.as_ref(), &self.cover, self.held) {
                        Verdict::Committed(v) => return Ok(v),
                        Verdict::Moved => {
                            return Err(Error::Fenced(format!(
                                "another owner moved the pointer {} (the bundle answered {})",
                                self.key,
                                a.reason.as_deref().unwrap_or("?")
                            )))
                        }
                        Verdict::NotApplied if a.is_duplicate() => {
                            return Err(Error::fatal(
                                "duplicate",
                                format!(
                                    "the broker holds messages with this bundle's transaction ids \
                                     but the pointer {} does not cover them ({}); something else \
                                     pushes with this source's ids — POST …/resync starts a new \
                                     epoch",
                                    self.key,
                                    a.error.as_deref().unwrap_or("")
                                ),
                            ))
                        }
                        Verdict::NotApplied => {
                            not_applied += 1;
                            if not_applied > 3 {
                                return Err(Error::Fenced(format!(
                                    "the pointer {} precondition keeps failing at the version \
                                     this owner holds",
                                    self.key
                                )));
                            }
                            tokio::time::sleep(backoff.step()).await;
                        }
                    }
                }
                Ok(a) => {
                    return Err(Error::fatal(
                        "queen_refused",
                        format!(
                            "the broker refused a bundle ({}): {}",
                            a.reason.as_deref().unwrap_or("no reason"),
                            a.error.as_deref().unwrap_or("")
                        ),
                    ))
                }
                Err(e) if e.is_in_doubt() => {
                    let wait = backoff.after(&e);
                    let read = self.read(&mut backoff).await?;
                    match judge(read.as_ref(), &self.cover, self.held) {
                        Verdict::Committed(v) => return Ok(v),
                        Verdict::Moved => {
                            return Err(Error::Fenced(format!(
                                "another owner moved the pointer {} while a bundle was in doubt \
                                 ({e})",
                                self.key
                            )))
                        }
                        Verdict::NotApplied => {
                            tracing::debug!(
                                target: crate::LOG_TARGET,
                                key = %self.key,
                                error = %e,
                                "bundle not applied; resending"
                            );
                            tokio::time::sleep(wait).await;
                        }
                    }
                }
                Err(e) if e.is_retryable() => {
                    tokio::time::sleep(backoff.after(&e)).await;
                }
                Err(e) => return Err(Error::Queen(e)),
            }
        }
    }

    /// The pointer, read until the broker answers (retryable failures back
    /// off; anything else ends the commit).
    async fn read(&self, backoff: &mut Backoff) -> Result<Option<Held>> {
        loop {
            match pointer::read(self.api.as_ref(), &self.key).await {
                Ok(r) => return Ok(r),
                Err(Error::Queen(e)) if e.is_retryable() => {
                    tokio::time::sleep(backoff.after(&e)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::{FakeCall, FakeQueen, Fault};
    use crate::source::pointer::{InTxn, SnapshotProgress};

    fn item(q: &str, p: &str, id: &str) -> Item {
        Item {
            queue: Arc::from(q),
            partition: p.into(),
            txn_id: id.into(),
            payload: format!("{{\"id\":\"{id}\"}}"),
        }
    }

    fn pos(lsn: u64) -> Position {
        Position {
            lsn: crate::repl::Lsn(lsn),
            in_txn: None,
            snapshot: None,
        }
    }

    fn unit(n: usize, lsn: u64) -> Unit {
        let items = (0..n)
            .map(|i| item("q", "p", &format!("{lsn}:{i}")))
            .collect();
        let mut u = Unit::new(items, pos(lsn));
        u.txns = 1;
        u
    }

    fn limits(m: usize) -> Limits {
        Limits {
            max_messages: m,
            max_bytes: 1 << 20,
            linger: Duration::from_millis(20),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn whole_units_until_the_next_would_pass_the_limit() {
        let now = Instant::now();
        let mut b = Bundler::new(limits(10));
        b.push(unit(4, 1), now);
        b.push(unit(4, 2), now);
        assert!(!b.due(now), "8 < 10 and the linger has not passed");
        b.push(unit(4, 3), now);
        assert!(b.due(now), "12 queued: a full bundle can go");
        let first = b.take().unwrap();
        assert_eq!(first.items.len(), 8, "the third unit would pass 10");
        assert_eq!(first.cover, pos(2));
        assert_eq!(first.txns, 2);
        assert!(!b.due(now));
        assert!(
            b.due(now + Duration::from_millis(20)),
            "lingerMs since the first one"
        );
        let second = b.take().unwrap();
        assert_eq!((second.items.len(), second.cover.clone()), (4, pos(3)));
        assert!(b.take().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn an_oversized_unit_goes_alone_and_position_units_merge_and_ride_along() {
        let now = Instant::now();
        let mut b = Bundler::new(limits(10));
        b.push(unit(15, 1), now);
        b.push(Unit::new(Vec::new(), pos(2)), now);
        b.push(Unit::new(Vec::new(), pos(3)), now);
        assert_eq!(b.units.len(), 2, "two position-only units merged into one");
        b.push(unit(1, 4), now);
        let first = b.take().unwrap();
        assert_eq!(first.items.len(), 15, "never split");
        assert_eq!(
            first.cover,
            pos(3),
            "the position-only unit after it rides along"
        );
        let second = b.take().unwrap();
        assert_eq!(second.cover, pos(4));
        // A position-only queue is not due on its own; a forced one is.
        b.push(Unit::new(Vec::new(), pos(5)), now);
        assert!(!b.due(now + Duration::from_secs(60)));
        let mut idle = Unit::new(Vec::new(), pos(6));
        idle.force = true;
        b.push(idle, now);
        assert!(b.due(now));
        let k = b.take().unwrap();
        assert!(k.items.is_empty());
        assert_eq!(k.cover, pos(6));
    }

    #[tokio::test(start_paused = true)]
    async fn the_backlog_threshold_is_two_bundles() {
        let now = Instant::now();
        let mut b = Bundler::new(limits(10));
        b.push(unit(10, 1), now);
        assert!(!b.backlogged());
        b.push(unit(10, 2), now);
        assert!(b.backlogged());
    }

    #[test]
    fn the_body_is_the_plan_wire() {
        let op = KvOp::fence("src:s:pointer", serde_json::json!({"lsn": "0/1"}), 7);
        let v: serde_json::Value =
            serde_json::from_str(&body(&[item("orders", "42", "pg:e:1:0")], &op)).unwrap();
        assert_eq!(v["operations"][0]["type"], "push");
        assert_eq!(v["operations"][0]["items"][0]["queue"], "orders");
        assert_eq!(v["operations"][0]["items"][0]["partition"], "42");
        assert_eq!(v["operations"][0]["items"][0]["transactionId"], "pg:e:1:0");
        assert_eq!(v["kv"][0]["op"], "put");
        assert_eq!(v["kv"][0]["expect"], 7);
        assert_eq!(v["kv"][0]["required"], true);
        assert_eq!(v["kv"][0]["forever"], true);
        let v: serde_json::Value = serde_json::from_str(&body(&[], &op)).unwrap();
        assert!(
            v.get("operations").is_none(),
            "an idle advance is the KV rider alone"
        );
        assert_eq!(v["kv"][0]["key"], "src:s:pointer");
    }

    fn base() -> Pointer {
        Pointer {
            v: 1,
            epoch: "0badf00d".into(),
            system_id: "1".into(),
            slot: "s".into(),
            lsn: crate::repl::Lsn(100),
            in_txn: None,
            snapshot: None,
            resynced_at: None,
            updated_at: String::new(),
        }
    }

    #[test]
    fn the_decision_table() {
        let mut cover = base();
        cover.lsn = crate::repl::Lsn(200);
        let ours = Held {
            doc: cover.clone(),
            version: 9,
        };
        assert_eq!(judge(Some(&ours), &cover, 5), Verdict::Committed(9));
        let unchanged = Held {
            doc: base(),
            version: 5,
        };
        assert_eq!(judge(Some(&unchanged), &cover, 5), Verdict::NotApplied);
        let mut beyond = cover.clone();
        beyond.lsn = crate::repl::Lsn(300);
        let other = Held {
            doc: beyond,
            version: 11,
        };
        assert_eq!(
            judge(Some(&other), &cover, 5),
            Verdict::Moved,
            "beyond is not ours"
        );
        assert_eq!(judge(None, &cover, 5), Verdict::Moved);
        let mut split = cover.clone();
        split.in_txn = Some(InTxn {
            commit_lsn: crate::repl::Lsn(250),
            done: 10,
        });
        let other_chunk = Held {
            doc: Pointer {
                in_txn: Some(InTxn {
                    commit_lsn: crate::repl::Lsn(250),
                    done: 20,
                }),
                ..split.clone()
            },
            version: 12,
        };
        assert_eq!(judge(Some(&other_chunk), &split, 5), Verdict::Moved);
        let mut snap = cover.clone();
        snap.snapshot = SnapshotProgress::start(vec!["public.a".into()]);
        assert_eq!(
            judge(
                Some(&Held {
                    doc: snap.clone(),
                    version: 3
                }),
                &snap,
                1
            ),
            Verdict::Committed(3)
        );
    }

    async fn seeded(q: &Arc<FakeQueen>) -> i64 {
        pointer::create(q.as_ref(), "src:s:pointer", &base())
            .await
            .unwrap()
    }

    fn commit_of(q: &Arc<FakeQueen>, ids: &[&str], lsn: u64, held: i64) -> Commit {
        let items: Vec<Item> = ids.iter().map(|id| item("q", "p", id)).collect();
        let mut cover = base();
        cover.lsn = crate::repl::Lsn(lsn);
        Commit::new(q.clone(), "src:s:pointer".into(), &items, cover, held)
    }

    /// I5, lost answer: the bundle committed, the answer said 503. The read
    /// finds our cover: continue with the version read, nothing pushed twice.
    #[tokio::test(start_paused = true)]
    async fn a_lost_answer_of_a_committed_bundle_continues_without_duplicates() {
        let q = FakeQueen::new();
        let v0 = seeded(&q).await;
        q.inject(FakeCall::Transaction, Fault::LoseAnswer);
        let v1 = commit_of(&q, &["a", "b"], 200, v0).run().await.unwrap();
        assert_eq!(q.messages("q").len(), 2);
        assert_eq!(q.kv_value("src:s:pointer").map(|r| r.1), Some(v1));
        // The next bundle builds on the version read and commits.
        let v2 = commit_of(&q, &["c"], 300, v1).run().await.unwrap();
        assert_ne!(v2, v1);
        let ids: Vec<String> = q
            .messages("q")
            .into_iter()
            .map(|m| m.transaction_id)
            .collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    /// I5, our own resend losing to the first attempt: `duplicate`, then the
    /// read shows our cover.
    #[tokio::test(start_paused = true)]
    async fn a_duplicate_of_our_own_commit_is_committed() {
        let q = FakeQueen::new();
        let v0 = seeded(&q).await;
        let v1 = commit_of(&q, &["a"], 200, v0).run().await.unwrap();
        // Same bundle again, as a retry would send it: the broker says
        // duplicate (and the precondition is stale too).
        let again = commit_of(&q, &["a"], 200, v0).run().await.unwrap();
        assert_eq!(again, v1);
        assert_eq!(q.messages("q").len(), 1);
    }

    /// I5, another owner: a foreign pointer write in between → Fenced, and
    /// nothing of ours lands.
    #[tokio::test(start_paused = true)]
    async fn a_foreign_pointer_write_fences_the_bundle() {
        let q = FakeQueen::new();
        let v0 = seeded(&q).await;
        let mut foreign = base();
        foreign.lsn = crate::repl::Lsn(150);
        pointer::replace(q.as_ref(), "src:s:pointer", &foreign, v0)
            .await
            .unwrap();
        let e = commit_of(&q, &["a"], 200, v0).run().await.unwrap_err();
        assert!(matches!(e, Error::Fenced(_)), "{e}");
        assert!(q.messages("q").is_empty(), "rolled back whole");
    }

    /// In doubt but never applied (refused before the log): read, unchanged,
    /// resend — exactly once in the end.
    #[tokio::test(start_paused = true)]
    async fn an_in_doubt_refusal_is_resent_and_lands_once() {
        let q = FakeQueen::new();
        let v0 = seeded(&q).await;
        q.inject(
            FakeCall::Transaction,
            Fault::Fail(QueenError::Status {
                code: 503,
                body: "no leader".into(),
                retry_after_ms: Some(10),
            }),
        );
        q.inject(
            FakeCall::Transaction,
            Fault::Fail(QueenError::Status {
                code: 429,
                body: "slow down".into(),
                retry_after_ms: Some(10),
            }),
        );
        let v1 = commit_of(&q, &["a"], 200, v0).run().await.unwrap();
        assert_eq!(q.messages("q").len(), 1);
        assert_eq!(q.kv_value("src:s:pointer").map(|r| r.1), Some(v1));
        assert_eq!(q.calls(FakeCall::Transaction), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_refusal_stops_and_a_bad_request_is_not_retried() {
        let q = FakeQueen::new();
        let v0 = seeded(&q).await;
        q.inject(
            FakeCall::Transaction,
            Fault::Fail(QueenError::Status {
                code: 400,
                body: "bad".into(),
                retry_after_ms: None,
            }),
        );
        let e = commit_of(&q, &["a"], 200, v0).run().await.unwrap_err();
        assert!(!e.is_retryable(), "{e}");
        // A 200 refusal (an item the broker will not take): stop, never skip.
        let mut cover = base();
        cover.lsn = crate::repl::Lsn(200);
        let e = Commit::new(
            q.clone(),
            "src:s:pointer".into(),
            &[item("", "p", "a")],
            cover,
            v0,
        )
        .run()
        .await
        .unwrap_err();
        assert_eq!(e.code(), "queen_refused");
        assert!(q.messages("q").is_empty());
    }

    /// Ids in the queue that the pointer does not cover: an invariant broken
    /// elsewhere — stop, never skip and never loop.
    #[tokio::test(start_paused = true)]
    async fn a_duplicate_the_pointer_does_not_cover_stops_with_an_error() {
        let q = FakeQueen::new();
        let v0 = seeded(&q).await;
        q.push_raw("q", "p", "null", Some("a"));
        let e = commit_of(&q, &["a"], 200, v0).run().await.unwrap_err();
        assert_eq!(e.code(), "duplicate");
    }
}
