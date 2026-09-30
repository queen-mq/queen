//! Callers waiting for this node to reach an index — applied
//! ([`super::RaftReplicator::wait_applied`]) or committed
//! ([`super::RaftReplicator::wait_committed`]).
//!
//! One registry per kind, ordered by index: a waiter registers a oneshot at
//! the index it needs, and whoever advances the index drains every waiter at
//! or below it — nobody else is touched. The one tokio watch this replaces
//! woke EVERY waiter for EVERY applied entry: on a follower whose apply lagged
//! (~2,000 forwarded requests waiting, ~300 entries a second) that was ~0.6M
//! wake-ups a second, each re-creating its deadline timer, and the lag grew
//! with the wake-ups it caused. Here a waiter wakes once, when its index is
//! reached or its deadline passes, and creates its timer once.
//!
//! The apply thread only publishes the index: [`IndexWaiters::advance`] is an
//! atomic max and an atomic load unless someone waits at or below the new
//! index, and then it unparks ONE drainer thread (`queen-raft-wake`, started
//! by the first waiter) that sends the wake-ups. The apply thread never pays
//! for the waiters themselves. The committed index is published by a task,
//! which drains inline ([`IndexWaiters::advance_and_drain`]).
//!
//! No wake-up is lost between a waiter that registers and a publisher that
//! advances: the waiter stores its registration (`lowest`) before it reads
//! the index, the publisher stores the index before it reads `lowest`, both
//! sequentially consistent — at least one of them sees the other.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Instant;

use tokio::sync::oneshot;

/// Timed-out waiters leave their oneshot behind until the index is reached;
/// past this many, and half of what is registered, a timeout sweeps them out.
const SWEEP_AT: usize = 1024;

struct Inner {
    /// The highest index published.
    reached: AtomicU64,
    /// The lowest index anyone waits for; `u64::MAX` when nobody does.
    lowest: AtomicU64,
    waiting: Mutex<BTreeMap<u64, Vec<oneshot::Sender<()>>>>,
    /// Senders in `waiting`, live or left behind by a timeout.
    registered: AtomicUsize,
    /// Waiters that timed out since the last sweep.
    abandoned: AtomicUsize,
    closed: AtomicBool,
    /// The drainer thread, once a waiter started it.
    drainer: OnceLock<std::thread::Thread>,
    /// Whether [`IndexWaiters::advance`] publishes (a drainer thread answers
    /// the waiters), or only [`IndexWaiters::advance_and_drain`] does.
    threaded: bool,
    name: &'static str,
}

/// Callers waiting for an index. See the module header.
pub(crate) struct IndexWaiters {
    inner: Arc<Inner>,
}

impl IndexWaiters {
    /// A registry whose index starts at `reached`, published by a thread
    /// that must not answer the waiters itself ([`IndexWaiters::advance`]: the
    /// first waiter starts a drainer thread). `name` names its log lines.
    pub(crate) fn new(reached: u64, name: &'static str) -> IndexWaiters {
        IndexWaiters::with(reached, name, true)
    }

    /// A registry published by a task that answers the waiters itself
    /// ([`IndexWaiters::advance_and_drain`]): no drainer thread.
    pub(crate) fn drained_inline(reached: u64, name: &'static str) -> IndexWaiters {
        IndexWaiters::with(reached, name, false)
    }

    fn with(reached: u64, name: &'static str, threaded: bool) -> IndexWaiters {
        IndexWaiters {
            inner: Arc::new(Inner {
                reached: AtomicU64::new(reached),
                lowest: AtomicU64::new(u64::MAX),
                waiting: Mutex::new(BTreeMap::new()),
                registered: AtomicUsize::new(0),
                abandoned: AtomicUsize::new(0),
                closed: AtomicBool::new(false),
                drainer: OnceLock::new(),
                threaded,
                name,
            }),
        }
    }

    /// The highest index published.
    pub(crate) fn reached(&self) -> u64 {
        self.inner.reached.load(SeqCst)
    }

    /// Publish `index` (the apply thread): wakes the drainer only when someone
    /// waits at or below it.
    pub(crate) fn advance(&self, index: u64) {
        if !self.inner.publish(index) {
            return;
        }
        if let Some(t) = self.inner.drainer.get() {
            t.unpark();
        }
    }

    /// Publish `index` and answer the waiters it reaches on the calling task.
    pub(crate) fn advance_and_drain(&self, index: u64) {
        if self.inner.publish(index) {
            self.inner.drain(self.inner.reached.load(SeqCst));
        }
    }

    /// No index will be published any more (the apply thread stopped): every
    /// waiter is answered now, with whether its index had been reached.
    pub(crate) fn close(&self) {
        self.inner.closed.store(true, SeqCst);
        let gone =
            std::mem::take(&mut *self.inner.waiting.lock().unwrap_or_else(|p| p.into_inner()));
        self.inner.lowest.store(u64::MAX, SeqCst);
        self.inner.registered.store(0, SeqCst);
        drop(gone);
        if let Some(t) = self.inner.drainer.get() {
            t.unpark();
        }
    }

    /// Wait until `index` is published; `false` at `deadline`, or once the
    /// registry closed short of it.
    pub(crate) async fn wait(&self, index: u64, deadline: Instant) -> bool {
        let inner = &self.inner;
        if inner.reached.load(SeqCst) >= index {
            return true;
        }
        if inner.closed.load(SeqCst) {
            return false;
        }
        if inner.threaded {
            self.start_drainer();
        }
        let (tx, rx) = oneshot::channel();
        {
            let mut w = inner.waiting.lock().unwrap_or_else(|p| p.into_inner());
            w.entry(index).or_default().push(tx);
            inner.registered.fetch_add(1, SeqCst);
            inner.lowest.fetch_min(index, SeqCst);
        }
        // Registered first, read second (see the module header). A publisher
        // that raced past us may not have seen the registration: answer now;
        // the oneshot left behind goes with the next drain.
        if inner.reached.load(SeqCst) >= index {
            return true;
        }
        if inner.closed.load(SeqCst) {
            return inner.reached.load(SeqCst) >= index;
        }
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), rx).await {
            Ok(Ok(())) => true,
            // Closed (the sender dropped), or the deadline passed.
            Ok(Err(_)) => inner.reached.load(SeqCst) >= index,
            Err(_elapsed) => {
                let reached = inner.reached.load(SeqCst) >= index;
                inner.abandon();
                reached
            }
        }
    }

    /// Waiters registered and not yet drained, timed out ones included (a
    /// test's view).
    #[cfg(test)]
    pub(crate) fn registered(&self) -> usize {
        self.inner.registered.load(SeqCst)
    }

    fn start_drainer(&self) {
        let inner = &self.inner;
        if inner.drainer.get().is_some() {
            return;
        }
        let mut spawned = None;
        inner.drainer.get_or_init(|| {
            let weak: Weak<Inner> = Arc::downgrade(inner);
            let name = inner.name;
            match std::thread::Builder::new()
                .name("queen-raft-wake".into())
                .spawn(move || drain_loop(weak))
            {
                Ok(h) => {
                    let t = h.thread().clone();
                    spawned = Some(h);
                    t
                }
                Err(e) => {
                    // No thread: every waiter still ends at its deadline, and
                    // the next one tries again (get_or_init stores this
                    // thread's handle, which nobody unparks usefully).
                    tracing::error!(target: "rsm", error = %e, name, "raft: the wake thread did not start");
                    std::thread::current()
                }
            }
        });
        // Detached: it ends when the registry closes or goes away.
        drop(spawned);
    }
}

impl Drop for IndexWaiters {
    fn drop(&mut self) {
        self.close();
    }
}

impl Inner {
    /// Store `index`; whether a waiter may be due.
    fn publish(&self, index: u64) -> bool {
        let before = self.reached.fetch_max(index, SeqCst);
        index > before && self.lowest.load(SeqCst) <= index
    }

    /// Answer every waiter at or below `upto`.
    fn drain(&self, upto: u64) {
        let due = {
            let mut w = self.waiting.lock().unwrap_or_else(|p| p.into_inner());
            if w.first_key_value().is_none_or(|(k, _)| *k > upto) {
                return;
            }
            let rest = w.split_off(&upto.saturating_add(1));
            let due = std::mem::replace(&mut *w, rest);
            self.lowest
                .store(w.first_key_value().map_or(u64::MAX, |(k, _)| *k), SeqCst);
            let n: usize = due.values().map(Vec::len).sum();
            self.registered.fetch_sub(n, SeqCst);
            due
        };
        for tx in due.into_values().flatten() {
            let _ = tx.send(());
        }
    }

    /// A waiter gave up. Its oneshot stays registered until its index is
    /// reached; once enough are left behind, they are swept out.
    fn abandon(&self) {
        let n = self.abandoned.fetch_add(1, SeqCst) + 1;
        if n < SWEEP_AT || n < self.registered.load(SeqCst) / 2 {
            return;
        }
        let mut w = self.waiting.lock().unwrap_or_else(|p| p.into_inner());
        let mut removed = 0usize;
        w.retain(|_, v| {
            let before = v.len();
            v.retain(|tx| !tx.is_closed());
            removed += before - v.len();
            !v.is_empty()
        });
        self.registered.fetch_sub(removed, SeqCst);
        self.abandoned.store(0, SeqCst);
        self.lowest
            .store(w.first_key_value().map_or(u64::MAX, |(k, _)| *k), SeqCst);
    }
}

fn drain_loop(weak: Weak<Inner>) {
    loop {
        std::thread::park();
        let Some(inner) = weak.upgrade() else {
            return;
        };
        if inner.closed.load(SeqCst) {
            return;
        }
        inner.drain(inner.reached.load(SeqCst));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn soon() -> Instant {
        Instant::now() + Duration::from_secs(10)
    }

    #[tokio::test]
    async fn a_waiter_wakes_once_its_index_is_reached_and_not_before() {
        let w = Arc::new(IndexWaiters::new(3, "test"));
        assert!(w.wait(3, soon()).await, "already reached");
        let w2 = w.clone();
        let t = tokio::spawn(async move { w2.wait(7, soon()).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        w.advance(6);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!t.is_finished(), "6 does not reach 7");
        w.advance(7);
        assert!(t.await.unwrap());
        assert_eq!(w.registered(), 0, "drained");
    }

    #[tokio::test]
    async fn one_advance_answers_every_waiter_at_or_below_it_and_no_other() {
        let w = Arc::new(IndexWaiters::new(0, "test"));
        let mut low = Vec::new();
        for i in 1..=50u64 {
            let w2 = w.clone();
            low.push(tokio::spawn(async move { w2.wait(i, soon()).await }));
        }
        let mut high = Vec::new();
        for i in 51..=60u64 {
            let w2 = w.clone();
            high.push(tokio::spawn(async move { w2.wait(i, soon()).await }));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(w.registered(), 60);
        w.advance(50);
        for t in low {
            assert!(t.await.unwrap());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            high.iter().all(|t| !t.is_finished()),
            "above 50: still waiting"
        );
        assert_eq!(w.registered(), 10, "only the reached ones were drained");
        w.advance_and_drain(60);
        for t in high {
            assert!(t.await.unwrap());
        }
    }

    #[tokio::test]
    async fn a_deadline_answers_false_and_the_abandoned_are_swept() {
        let w = Arc::new(IndexWaiters::new(0, "test"));
        let t0 = Instant::now();
        assert!(!w.wait(5, Instant::now() + Duration::from_millis(30)).await);
        assert!(t0.elapsed() >= Duration::from_millis(25));
        // Many give up on an index nobody reaches: they do not pile up.
        let mut ts = Vec::new();
        for _ in 0..(3 * SWEEP_AT) {
            let w2 = w.clone();
            ts.push(tokio::spawn(async move {
                w2.wait(1_000, Instant::now() + Duration::from_millis(10))
                    .await
            }));
        }
        for t in ts {
            assert!(!t.await.unwrap());
        }
        assert!(
            w.registered() < SWEEP_AT + 2,
            "timed-out waiters were swept: {} left",
            w.registered()
        );
    }

    #[tokio::test]
    async fn closing_answers_every_waiter_at_once() {
        let w = Arc::new(IndexWaiters::new(0, "test"));
        let w2 = w.clone();
        let t = tokio::spawn(async move { w2.wait(9, soon()).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        let t0 = Instant::now();
        w.close();
        assert!(!t.await.unwrap(), "never reached");
        assert!(t0.elapsed() < Duration::from_secs(1), "not at the deadline");
        assert!(!w.wait(10, soon()).await, "closed: no more waiting");
    }

    /// Publishers racing registrations: no wake-up is lost (every waiter is
    /// answered long before its deadline).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn no_wake_up_is_lost_between_a_registration_and_an_advance() {
        let w = Arc::new(IndexWaiters::new(0, "test"));
        const N: u64 = 20_000;
        let pubw = w.clone();
        let publisher = std::thread::spawn(move || {
            for i in 1..=N {
                pubw.advance(i);
                if i % 64 == 0 {
                    std::thread::yield_now();
                }
            }
        });
        let mut ts = Vec::new();
        for k in 0..2_000u64 {
            let w2 = w.clone();
            ts.push(tokio::spawn(async move {
                let target = 1 + (k * 7919) % N;
                let t0 = Instant::now();
                let ok = w2
                    .wait(target, Instant::now() + Duration::from_secs(20))
                    .await;
                (ok, t0.elapsed())
            }));
        }
        publisher.join().unwrap();
        for t in ts {
            let (ok, took) = t.await.unwrap();
            assert!(ok);
            assert!(
                took < Duration::from_secs(5),
                "a wake-up was lost ({took:?})"
            );
        }
    }
}
