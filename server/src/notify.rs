//! Long-poll waker + inter-instance notification facade.
//!
//! Two jobs, one struct:
//!
//! 1. **Local long-poll waker.** A parked POP (`handlers::handle_pop*`) is a
//!    deadline poll: it re-checks `seg_has_pending` every `POP_WAIT_POLL_MS`.
//!    Instead of a blind `sleep`, it now parks on a per-queue [`tokio::sync::Notify`]
//!    gate. A local PUSH — or a MESSAGE_AVAILABLE from a peer, or an HTTP
//!    `/internal/api/notify` — wakes the gate so the pop re-polls at once. The
//!    per-poll timeout is still honoured, so this is a strict latency improvement:
//!    a missed wake merely falls back to the old poll interval.
//!
//! 2. **Peer fan-out.** When a UDP transport is attached (multi-replica mode), the
//!    same PUSH/maintenance/config-change also broadcasts to peers so their parked
//!    pops / cached flags stay current. With no transport attached (a single stock
//!    broker) every peer call is a no-op and only the local waker runs.
//!
//! The `Notifier` is decoupled from the transport's construction: it starts with
//! an empty transport slot and main.rs calls [`attach_transport`](Notifier::attach_transport)
//! once wiring is complete, breaking the AppState ↔ transport cycle.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::sync::Notify;

use crate::udp::UdpTransport;

pub struct Notifier {
    /// Per-queue wake gates. Created lazily by a waiting pop; a push only wakes an
    /// existing gate, so queues nobody polls never allocate one.
    gates: Mutex<HashMap<String, Arc<Notify>>>,
    /// Woken by every push, for discovery (namespace/task) pops that span queues
    /// and so have no single gate to park on.
    any: Arc<Notify>,
    /// Optional peer transport (multi-replica mode). Set once at startup.
    transport: OnceLock<Arc<UdpTransport>>,
}

impl Notifier {
    pub fn new() -> Arc<Notifier> {
        Arc::new(Notifier {
            gates: Mutex::new(HashMap::new()),
            any: Arc::new(Notify::new()),
            transport: OnceLock::new(),
        })
    }

    /// Wire up the peer transport. Idempotent-ish: only the first attach wins.
    pub fn attach_transport(&self, t: Arc<UdpTransport>) {
        let _ = self.transport.set(t);
    }

    pub fn transport(&self) -> Option<&Arc<UdpTransport>> {
        self.transport.get()
    }

    /// The gate a queue-scoped pop parks on. get-or-create.
    fn gate(&self, queue: &str) -> Arc<Notify> {
        let mut g = self.gates.lock().unwrap();
        if let Some(n) = g.get(queue) {
            return n.clone();
        }
        let n = Arc::new(Notify::new());
        g.insert(queue.to_string(), n.clone());
        n
    }

    /// Park a queue-scoped long-poll for up to `dur`, returning the instant a push
    /// to `queue` wakes the gate. Replaces the old blind `sleep(poll_ms)`. The
    /// waiter is armed (`enable`) before the wait, so a wake that arrives during the
    /// window is delivered; a wake missed in the tiny arm gap simply times out and
    /// the caller re-polls — identical to the pre-existing poll-interval ceiling.
    /// Returns true if a push woke the gate, false if `dur` elapsed first — the
    /// caller uses this to reset (on wake) vs advance (on timeout) its long-poll
    /// backoff (RUSTFIX item 19).
    pub async fn wait_queue(&self, queue: &str, dur: Duration) -> bool {
        let gate = self.gate(queue);
        let notified = gate.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        tokio::time::timeout(dur, notified).await.is_ok()
    }

    /// Park a discovery (namespace/task) long-poll on the shared gate — woken by any
    /// push, since these pops span queues and have no single gate. Returns true on a
    /// push-wake, false on timeout (RUSTFIX item 19).
    pub async fn wait_any(&self, dur: Duration) -> bool {
        let notified = self.any.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        tokio::time::timeout(dur, notified).await.is_ok()
    }

    /// Wake locally-parked pops for `queue` (and all discovery pops). Purely local —
    /// used by inbound peer/HTTP notifications so a received signal never re-broadcasts.
    pub fn wake_local(&self, queue: &str) {
        if !queue.is_empty() {
            if let Some(n) = self.gates.lock().unwrap().get(queue) {
                n.notify_waiters();
            }
        }
        self.any.notify_waiters();
    }

    /// A local PUSH landed: wake local pops AND fan out MESSAGE_AVAILABLE to peers.
    pub fn notify_pushed(&self, queue: &str, partition: &str) {
        self.wake_local(queue);
        if let Some(t) = self.transport.get() {
            t.send_message_available(queue, partition);
        }
    }

    // ---- config/maintenance change broadcasts (local state is mutated by the
    // ---- caller; these only fan the change out to peers) --------------------

    pub fn broadcast_maintenance(&self, enabled: bool) {
        if let Some(t) = self.transport.get() {
            t.send_maintenance(enabled);
        }
    }

    pub fn broadcast_pop_maintenance(&self, enabled: bool) {
        if let Some(t) = self.transport.get() {
            t.send_pop_maintenance(enabled);
        }
    }

    pub fn broadcast_queue_config_set(&self, queue: &str) {
        if let Some(t) = self.transport.get() {
            t.send_queue_config_set(queue);
        }
    }

    pub fn broadcast_queue_config_delete(&self, queue: &str) {
        if let Some(t) = self.transport.get() {
            t.send_queue_config_delete(queue);
        }
    }
}
