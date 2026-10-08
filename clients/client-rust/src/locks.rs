//! Locks: a lock and a semaphore, as leases with a fencing token.
//!
//! ```no_run
//! use std::time::Duration;
//! use queen_mq::Queen;
//!
//! # async fn example() -> queen_mq::Result<()> {
//! let queen = Queen::connect_to("http://localhost:6632")?;
//!
//! let lock = queen.lock("daily-report", Duration::from_secs(30));
//! if !lock.acquire().await? {
//!     return Ok(()); // somebody else has it
//! }
//! // Commits only while the lock is still ours, in the same log entry.
//! let resp = queen
//!     .transaction()
//!     .guard(&lock)
//!     .push("reports", serde_json::json!({ "day": "2026-10-08" }))?
//!     .commit()
//!     .await?;
//! if resp.lost_precondition().is_some() {
//!     // The lock is somebody else's now; nothing was pushed.
//! }
//! lock.release().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # What it is
//!
//! A permit is one KV row in the namespace `queen-locks`, written with a
//! lifetime: `acquire` is a `putIfAbsent`, `renew` a `put` with `expect`,
//! `release` a `delete` with `expect`. The broker's `POST /api/v1/locks` does
//! that turning, so every client shares one implementation. A lock is the
//! semaphore of one permit; [`crate::Queen::semaphore`] is the same thing
//! with more.
//!
//! # What it is not: a mutex
//!
//! A permit **expires**, and nobody tells its holder. A process that is
//! paused, partitioned or slow keeps running past its lifetime while somebody
//! else acquires. The lock alone therefore never makes two holders
//! impossible. What makes their *work* exclusive is the token:
//!
//! * inside Queen, [`crate::TransactionBuilder::guard`]: the acks, pushes, KV
//!   writes and timers of a step commit only if the permit is still this
//!   holder's. A holder that was replaced commits nothing;
//! * outside Queen, [`Lock::token`]: a number that only rises on a lock. A
//!   resource that remembers the highest token it accepted and refuses a
//!   lower one refuses the holder that was replaced. Accept an equal one: a
//!   holder writes many times with one token.
//!
//! # The token changes at every renew
//!
//! A renew rewrites the row, so the broker answers a new token and the one
//! before stops working. A [`Lock`] keeps the current one: read
//! [`Lock::token`] and [`Lock::guard`] when you use them, do not hold a copy
//! across an `.await`.
//!
//! # The owner
//!
//! The holder's identity, minted per handle. With it a call is safe to send
//! again when its answer was lost: the broker answers the permit the first
//! attempt took. Two handles with one owner are one holder; pass your own
//! only if that is what you mean.

use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use queen_protocol::kv::KvOperation;
use queen_protocol::locks::{LockOperation, LockRequest, LockResponse, LockResult};
use tokio::sync::watch;

use crate::error::{Error, Result};
use crate::http::Opts;
use crate::inner::Inner;

/// The four lock operations as the broker speaks them, with no state kept:
/// the caller carries the token. [`crate::Queen::lock`] is what most code
/// wants; this is for [`Locks::get`] and for a caller with its own loop.
///
/// As on the KV routes, an `Err` means the call did not happen. A lock held
/// by somebody else is `Ok` with `acquired() == false`.
#[derive(Clone)]
pub struct Locks {
    inner: Arc<Inner>,
}

impl Locks {
    pub(crate) fn new(inner: Arc<Inner>) -> Self {
        Self { inner }
    }

    /// Several operations in one call, each on a different lock: one result
    /// per operation, in order. They are independent; nothing here is
    /// all-or-nothing.
    pub async fn batch(&self, ops: Vec<LockOperation>) -> Result<Vec<LockResult>> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }
        let expected = ops.len();
        let resp: Option<LockResponse> = self
            .inner
            .http
            .post_json("/api/v1/locks", &LockRequest::new(ops), &Opts::default())
            .await?;
        let resp = resp.ok_or_else(|| Error::Decode("locks returned an empty body".into()))?;
        if resp.results.len() != expected {
            return Err(Error::Decode(format!(
                "locks returned {} results for {expected} operations",
                resp.results.len()
            )));
        }
        Ok(resp.results)
    }

    /// One operation, built with [`LockOperation`].
    pub async fn send(&self, op: LockOperation) -> Result<LockResult> {
        Ok(self.batch(vec![op]).await?.remove(0))
    }

    /// Who holds it: `held()`, and the holders by slot.
    pub async fn get(&self, name: &str) -> Result<LockResult> {
        self.send(LockOperation::get(name)).await
    }
}

/// How a [`Lock`] behaves. Built from a lifetime, which is mandatory: there
/// is no default and no "forever", because a lock that never expires is one
/// nobody can take back from a holder that died.
#[derive(Debug, Clone)]
pub struct LockOptions {
    ttl: Duration,
    owner: Option<String>,
    limit: u32,
    auto_renew: bool,
    renew_every: Option<Duration>,
    retry_min: Duration,
    retry_max: Duration,
}

impl LockOptions {
    /// A lock that lives `ttl` between renewals. Rounded UP to whole seconds,
    /// the broker's unit.
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            owner: None,
            limit: 1,
            auto_renew: true,
            renew_every: None,
            retry_min: Duration::from_millis(100),
            retry_max: Duration::from_secs(1),
        }
    }

    /// The holder's identity, instead of the one minted per handle. Two
    /// handles with one owner are one holder.
    pub fn owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self
    }

    /// Make it a semaphore of `limit` permits. Every holder of one name uses
    /// the same limit; it is the caller's and is stored nowhere.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Do not renew in the background: the caller calls [`Lock::renew`].
    pub fn manual_renew(mut self) -> Self {
        self.auto_renew = false;
        self
    }

    /// Renew this often instead of every third of the lifetime.
    pub fn renew_every(mut self, every: Duration) -> Self {
        self.renew_every = Some(every);
        self
    }

    /// How a waiting acquire comes back: first after `min`, then half as long
    /// again each time up to `max`, with jitter.
    pub fn retry(mut self, min: Duration, max: Duration) -> Self {
        self.retry_min = min;
        self.retry_max = max;
        self
    }

    fn ttl_seconds(&self) -> Result<i64> {
        let secs = self.ttl.as_secs() + u64::from(self.ttl.subsec_nanos() > 0);
        if secs == 0 {
            return Err(Error::Invalid(
                "a lock needs a lifetime above zero; a holder that needs longer renews".into(),
            ));
        }
        i64::try_from(secs).map_err(|_| Error::Invalid("that lifetime is too long".into()))
    }
}

fn mint_owner() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "host".into());
    let host: String = host.chars().take(128).collect();
    format!(
        "{host}:{}:{:012x}",
        std::process::id(),
        rand::random::<u64>() & 0xffff_ffff_ffff
    )
}

/// The permit a handle holds.
#[derive(Clone)]
struct Held {
    token: u64,
    slot: u32,
    /// The broker's own guard for this lease period, kept as answered: where
    /// a permit's row lives is the broker's rule, written once, there.
    guard: KvOperation,
    valid_until: Instant,
}

struct State {
    held: Option<Held>,
    /// Bumped at every acquire and release, so a background task or a renew
    /// in flight knows when its permit is no longer the handle's.
    epoch: u64,
}

struct Shared {
    locks: Locks,
    name: String,
    owner: String,
    options: LockOptions,
    state: Mutex<State>,
    /// One renew at a time, and what a guard waits on to read a settled token.
    renewing: tokio::sync::Mutex<()>,
    lost: watch::Sender<bool>,
}

/// One holder's hold on one lock, or on one permit of a semaphore.
///
/// It keeps the token current, renews in the background (every third of the
/// lifetime, unless [`LockOptions::manual_renew`]) and says when the permit is
/// gone ([`Lock::lost`]). "Gone" is the broker saying so, or the lifetime
/// passing on THIS machine's clock with no renew having succeeded: a client
/// that cannot reach the broker has to assume the worst.
///
/// Cheap to clone; every clone is the same holder. Dropping the last one
/// without [`Lock::release`] stops the renewal, and the permit then expires
/// by itself.
#[derive(Clone)]
pub struct Lock {
    shared: Arc<Shared>,
}

impl Lock {
    pub(crate) fn new(inner: Arc<Inner>, name: impl Into<String>, options: LockOptions) -> Self {
        let owner = options.owner.clone().unwrap_or_else(mint_owner);
        let (lost, _) = watch::channel(false);
        Self {
            shared: Arc::new(Shared {
                locks: Locks::new(inner),
                name: name.into(),
                owner,
                options,
                state: Mutex::new(State {
                    held: None,
                    epoch: 0,
                }),
                renewing: tokio::sync::Mutex::new(()),
                lost,
            }),
        }
    }

    pub fn name(&self) -> &str {
        &self.shared.name
    }

    pub fn owner(&self) -> &str {
        &self.shared.owner
    }

    fn held_now(&self) -> Option<Held> {
        let st = self.shared.state.lock().expect("lock state");
        st.held.clone().filter(|h| Instant::now() < h.valid_until)
    }

    /// Whether this handle holds a permit, as far as it can know: the broker
    /// granted or renewed it, and its lifetime has not run out on this
    /// machine's clock. A belief with a deadline, not a proof — the proof is
    /// the guard on the transaction.
    pub fn held(&self) -> bool {
        self.held_now().is_some()
    }

    /// The fencing token of the current lease period.
    pub fn token(&self) -> Option<u64> {
        self.held_now().map(|h| h.token)
    }

    /// The semaphore slot this handle holds (0 for a lock).
    pub fn slot(&self) -> Option<u32> {
        self.held_now().map(|h| h.slot)
    }

    /// The KV operation that holds while the permit is this handle's: a
    /// `check` of the permit's row at the current token, `required`.
    /// [`crate::TransactionBuilder::guard`] adds it and follows a renew; take
    /// this one at the moment you send it, for a KV batch of your own.
    pub fn guard(&self) -> Option<KvOperation> {
        self.held_now().map(|h| h.guard)
    }

    /// Take the permit, once. `Ok(false)` when it is held by somebody else.
    pub async fn acquire(&self) -> Result<bool> {
        self.acquire_within(Duration::ZERO).await
    }

    /// Take the permit, coming back until it is free or `wait` is over.
    pub async fn acquire_within(&self, wait: Duration) -> Result<bool> {
        if self.held() {
            return Ok(true);
        }
        let ttl = self.shared.options.ttl_seconds()?;
        let deadline = Instant::now() + wait;
        let mut pause = self.shared.options.retry_min;
        loop {
            let sent = Instant::now();
            let mut op = LockOperation::acquire(&self.shared.name, ttl).owner(&self.shared.owner);
            if self.shared.options.limit > 1 {
                op = op.limit(self.shared.options.limit);
            }
            let r = self.shared.locks.send(op).await?;
            if r.acquired() {
                self.take(&r, sent, ttl)?;
                return Ok(true);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(false);
            }
            // A random quarter off, so a crowd of waiters spreads.
            let jittered = pause.mul_f64(0.75 + rand::random::<f64>() * 0.25);
            tokio::time::sleep(jittered.min(left)).await;
            pause = pause.mul_f64(1.5).min(self.shared.options.retry_max);
        }
    }

    /// Extend the lease now. `Ok(true)` with a new token in place, or
    /// `Ok(false)`: the permit is gone and the handle says so. An `Err` means
    /// the broker could not be asked — the permit is then neither renewed nor
    /// known lost, and its deadline stands.
    pub async fn renew(&self) -> Result<bool> {
        let _one = self.shared.renewing.lock().await;
        let (held, epoch) = {
            let st = self.shared.state.lock().expect("lock state");
            match &st.held {
                Some(h) => (h.clone(), st.epoch),
                None => return Ok(false),
            }
        };
        let ttl = self.shared.options.ttl_seconds()?;
        let sent = Instant::now();
        let r = self
            .shared
            .locks
            .send(
                LockOperation::renew(&self.shared.name, held.token, ttl)
                    .slot(held.slot)
                    .owner(&self.shared.owner),
            )
            .await?;
        let mut st = self.shared.state.lock().expect("lock state");
        // Released, or lost, while the renew was in flight: its answer is
        // about a permit this handle no longer has.
        if st.epoch != epoch {
            return Ok(false);
        }
        if !r.renewed() {
            st.held = None;
            st.epoch += 1;
            drop(st);
            self.shared.lost.send_replace(true);
            return Ok(false);
        }
        st.held = Some(held_of(&r, sent, ttl)?);
        Ok(true)
    }

    /// Give the permit back. `Ok(true)` when the broker removed it;
    /// `Ok(false)` when it was not this handle's any more, or never held.
    /// Either way the handle holds nothing afterwards and can acquire again.
    pub async fn release(&self) -> Result<bool> {
        // A renew in flight owns the token until it answers.
        let _one = self.shared.renewing.lock().await;
        let held = {
            let mut st = self.shared.state.lock().expect("lock state");
            st.epoch += 1;
            st.held.take()
        };
        let Some(held) = held else {
            return Ok(false);
        };
        let r = self
            .shared
            .locks
            .send(LockOperation::release(&self.shared.name, held.token).slot(held.slot))
            .await?;
        Ok(r.released())
    }

    /// Resolves when the permit is lost: the broker refused a renew or a
    /// guard, or the lifetime ran out here with no renew having succeeded. A
    /// release is not a loss and does not resolve it. Race it against the
    /// work, with `tokio::select!`.
    pub async fn lost(&self) {
        let mut rx = self.shared.lost.subscribe();
        // An error is the sender gone, which a live handle rules out.
        let _ = rx.wait_for(|lost| *lost).await;
    }

    /// Whether the permit was lost since the last acquire.
    pub fn is_lost(&self) -> bool {
        *self.shared.lost.borrow()
    }

    // ---- used by TransactionBuilder::guard ----------------------------------

    /// Waits out a renew in flight: the token is then the current one.
    pub(crate) async fn settled(&self) {
        let _ = self.shared.renewing.lock().await;
    }

    /// The broker said the permit is not this handle's.
    pub(crate) fn mark_lost(&self) {
        let mut st = self.shared.state.lock().expect("lock state");
        if st.held.take().is_some() {
            st.epoch += 1;
            drop(st);
            self.shared.lost.send_replace(true);
        }
    }

    // ---- state ----------------------------------------------------------------

    fn take(&self, r: &LockResult, sent: Instant, ttl: i64) -> Result<()> {
        let held = held_of(r, sent, ttl)?;
        let epoch = {
            let mut st = self.shared.state.lock().expect("lock state");
            st.held = Some(held);
            st.epoch += 1;
            st.epoch
        };
        self.shared.lost.send_replace(false);
        tokio::spawn(keep(Arc::downgrade(&self.shared), epoch));
        Ok(())
    }
}

fn held_of(r: &LockResult, sent: Instant, ttl: i64) -> Result<Held> {
    match (r.token, r.slot, r.guard.clone()) {
        (Some(token), Some(slot), Some(guard)) => Ok(Held {
            token,
            slot,
            guard,
            // Counted from when the request was SENT, so it is never later
            // than the broker's own deadline.
            valid_until: sent + Duration::from_secs(ttl as u64),
        }),
        _ => Err(Error::Decode(
            "a granted permit must carry its token, slot and guard".into(),
        )),
    }
}

/// The background of one hold: renew when due, and call the permit lost when
/// its lifetime runs out here. Holds the handle weakly, so a dropped lock
/// stops being renewed.
async fn keep(shared: Weak<Shared>, epoch: u64) {
    loop {
        let (wait, auto) = {
            let Some(s) = shared.upgrade() else { return };
            let st = s.state.lock().expect("lock state");
            let Some(h) = st.held.as_ref().filter(|_| st.epoch == epoch) else {
                return;
            };
            let left = h.valid_until.saturating_duration_since(Instant::now());
            let every = s
                .options
                .renew_every
                .unwrap_or_else(|| s.options.ttl.max(Duration::from_secs(1)) / 3);
            let auto = s.options.auto_renew;
            (if auto { every.min(left) } else { left }, auto)
        };
        tokio::time::sleep(wait).await;
        let Some(s) = shared.upgrade() else { return };
        let lock = Lock { shared: s };
        let expired = {
            let st = lock.shared.state.lock().expect("lock state");
            match st.held.as_ref().filter(|_| st.epoch == epoch) {
                None => return,
                Some(h) => Instant::now() >= h.valid_until,
            }
        };
        if expired {
            let lost = {
                let mut st = lock.shared.state.lock().expect("lock state");
                let mine = st.epoch == epoch && st.held.is_some();
                if mine {
                    st.held = None;
                    st.epoch += 1;
                }
                mine
            };
            if lost {
                tracing::warn!(lock = %lock.shared.name, "lock lost: its lifetime ran out with no renew");
                lock.shared.lost.send_replace(true);
            }
            return;
        }
        if auto {
            match lock.renew().await {
                Ok(true) => {}
                Ok(false) => return,
                // Could not ask. Not a loss yet: come back sooner, until the
                // deadline above calls it.
                Err(e) => {
                    tracing::warn!(lock = %lock.shared.name, error = %e, "lock renew failed; retrying");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lifetime_is_rounded_up_and_never_zero() {
        let ttl = |d: Duration| LockOptions::new(d).ttl_seconds();
        assert_eq!(ttl(Duration::from_secs(30)).unwrap(), 30);
        assert_eq!(ttl(Duration::from_millis(1500)).unwrap(), 2);
        assert_eq!(ttl(Duration::from_millis(1)).unwrap(), 1);
        assert!(ttl(Duration::ZERO).is_err());
    }

    #[test]
    fn every_handle_mints_its_own_owner() {
        let (a, b) = (mint_owner(), mint_owner());
        assert_ne!(a, b);
        assert!(a.len() <= 256 && a.split(':').count() >= 3, "{a}");
    }
}
