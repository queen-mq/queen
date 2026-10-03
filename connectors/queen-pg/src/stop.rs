//! The stop signal every long-running loop of the crate watches.
//!
//! A [`StopHandle`] is held by whoever may stop a connector (the broker's
//! manager, a test); each loop holds a [`Stop`] and either checks
//! [`Stop::is_stopped`] between steps or awaits [`Stop::wait`] in a `select!`.
//! Stopping is level-triggered and permanent: once stopped, every clone reads
//! stopped and every `wait` returns at once.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

#[derive(Debug, Default)]
struct Inner {
    stopped: AtomicBool,
    notify: Notify,
}

/// The receiving side: cheap to clone, one per task.
#[derive(Debug, Clone, Default)]
pub struct Stop {
    inner: Arc<Inner>,
}

/// The sending side.
#[derive(Debug, Clone, Default)]
pub struct StopHandle {
    inner: Arc<Inner>,
}

/// A new pair, not stopped.
pub fn stop_pair() -> (StopHandle, Stop) {
    let inner = Arc::new(Inner::default());
    (
        StopHandle {
            inner: Arc::clone(&inner),
        },
        Stop { inner },
    )
}

impl StopHandle {
    /// Stop every [`Stop`] of this pair, now and for good.
    pub fn stop(&self) {
        self.inner.stopped.store(true, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }

    pub fn is_stopped(&self) -> bool {
        self.inner.stopped.load(Ordering::SeqCst)
    }

    /// A receiver of this pair.
    pub fn subscribe(&self) -> Stop {
        Stop {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Stop {
    /// A receiver that never stops (tests, one-shot helpers).
    pub fn never() -> Stop {
        Stop::default()
    }

    pub fn is_stopped(&self) -> bool {
        self.inner.stopped.load(Ordering::SeqCst)
    }

    /// Resolves once stopped (at once if already stopped). Cancellation-safe.
    pub async fn wait(&self) {
        loop {
            let notified = self.inner.notify.notified();
            if self.is_stopped() {
                return;
            }
            notified.await;
            if self.is_stopped() {
                return;
            }
        }
    }

    /// Sleep `d`, or less if the stop arrives first. `true` when stopped.
    pub async fn sleep(&self, d: std::time::Duration) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(d) => self.is_stopped(),
            _ = self.wait() => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_stop_wakes_every_waiter_and_stays_stopped() {
        let (h, s) = stop_pair();
        let s2 = h.subscribe();
        let w = tokio::spawn(async move { s2.wait().await });
        assert!(!s.is_stopped());
        h.stop();
        w.await.unwrap();
        assert!(s.is_stopped());
        s.wait().await; // returns at once
        assert!(s.sleep(std::time::Duration::from_secs(3600)).await);
    }
}
