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
//! 2. **Peer fan-out.** When a mesh transport is attached (multi-replica mode), the
//!    same PUSH/maintenance/config-change also broadcasts to peers so their parked
//!    pops / cached flags stay current. With no transport attached (a single stock
//!    broker) every peer call is a no-op and only the local waker runs.
//!
//! The `Notifier` is decoupled from the transport's construction: it starts with
//! an empty transport slot and main.rs calls [`attach_transport`](Notifier::attach_transport)
//! once wiring is complete, breaking the AppState ↔ transport cycle.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::sync::Notify;

use crate::mesh::MeshTransport;

/// Bound on a queue's hint mailbox. A hint is only useful until a woken pop drains
/// it, so a small ring (drop-oldest) is plenty; it caps memory when hints arrive
/// faster than any parked pop consumes them.
const HINT_CAP: usize = 64;

/// A queue's wake gate plus its partition-hint mailbox. Created lazily by a
/// parking pop; a push only touches an EXISTING gate, so queues nobody polls
/// allocate neither a gate nor a mailbox.
struct QueueGate {
    notify: Notify,
    /// Partition names that just received data on this queue, drop-oldest at
    /// HINT_CAP. A woken pop drains these to issue a targeted single-partition pop
    /// instead of a wildcard scan (Phase 2). A hint says "partition P got data" for
    /// ANY consumer group — a targeted pop that finds nothing for this group's
    /// cursor simply falls through, which is cheap.
    hints: Mutex<VecDeque<String>>,
}

pub struct Notifier {
    /// Per-queue wake gates + hint mailboxes. Created lazily by a waiting pop; a
    /// push only wakes an existing gate, so queues nobody polls never allocate one.
    gates: Mutex<HashMap<String, Arc<QueueGate>>>,
    /// Woken by every push, for discovery (namespace/task) pops that span queues
    /// and so have no single gate to park on.
    any: Arc<Notify>,
    /// Optional peer transport (multi-replica mode). Set once at startup.
    transport: OnceLock<Arc<MeshTransport>>,
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
    pub fn attach_transport(&self, t: Arc<MeshTransport>) {
        let _ = self.transport.set(t);
    }

    pub fn transport(&self) -> Option<&Arc<MeshTransport>> {
        self.transport.get()
    }

    /// The gate a queue-scoped pop parks on. get-or-create.
    fn gate(&self, queue: &str) -> Arc<QueueGate> {
        let mut g = self.gates.lock().unwrap();
        if let Some(x) = g.get(queue) {
            return x.clone();
        }
        let x = Arc::new(QueueGate {
            notify: Notify::new(),
            hints: Mutex::new(VecDeque::new()),
        });
        g.insert(queue.to_string(), x.clone());
        x
    }

    /// The queue's gate iff one already exists (a pop has parked on it). A push to a
    /// queue nobody polls returns None here — no gate is created and no hint stored,
    /// preserving the lazy-allocation invariant.
    fn existing_gate(&self, queue: &str) -> Option<Arc<QueueGate>> {
        self.gates.lock().unwrap().get(queue).cloned()
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
        let notified = gate.notify.notified();
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

    /// Wake locally-parked pops for `queue` (and all discovery pops), recording a
    /// partition HINT so a woken pop can target the partition that just got data
    /// instead of re-scanning the whole queue. Purely local — used by inbound
    /// peer/HTTP notifications so a received signal never re-broadcasts. Lazy: only
    /// an already-parked queue (existing gate) stores a hint, so an unpolled queue
    /// allocates nothing.
    pub fn wake_local_hint(&self, queue: &str, partition: &str) {
        if !queue.is_empty() {
            if let Some(gate) = self.existing_gate(queue) {
                if !partition.is_empty() {
                    let mut h = gate.hints.lock().unwrap();
                    if h.len() >= HINT_CAP {
                        h.pop_front(); // drop-oldest
                    }
                    h.push_back(partition.to_string());
                }
                gate.notify.notify_waiters();
            }
        }
        self.any.notify_waiters();
    }

    /// Drain up to `max` distinct partition hints for `queue` (FIFO, deduped),
    /// removing them from the mailbox. Empty when no gate exists or nothing is
    /// queued. A woken pop calls this to pick targeted partitions.
    pub fn drain_hints(&self, queue: &str, max: usize) -> Vec<String> {
        let Some(gate) = self.existing_gate(queue) else {
            return Vec::new();
        };
        let mut h = gate.hints.lock().unwrap();
        let mut out: Vec<String> = Vec::new();
        while out.len() < max {
            match h.pop_front() {
                Some(p) => {
                    if !out.contains(&p) {
                        out.push(p);
                    }
                }
                None => break,
            }
        }
        out
    }

    /// A local PUSH landed: wake local pops (with the partition hint) AND fan out
    /// MESSAGE_AVAILABLE to peers.
    pub fn notify_pushed(&self, queue: &str, partition: &str) {
        self.wake_local_hint(queue, partition);
        if let Some(t) = self.transport.get() {
            t.send_message_available(queue, partition);
        }
    }

    /// A committed push bundle landed, touching every (queue, partition) in `keys`:
    /// wake local pops for each queue (with its partition hint), then fan the whole
    /// set out to peers as ONE batched MESSAGE_AVAILABLE frame (instead of one send
    /// per partition). The single-item [`notify_pushed`] stays for the HTTP
    /// `/internal/api/notify` path.
    pub fn notify_pushed_batch(&self, keys: &[(String, String)]) {
        for (queue, partition) in keys {
            self.wake_local_hint(queue, partition);
        }
        if let Some(t) = self.transport.get() {
            t.send_messages_available_batch(keys);
        }
    }

    /// C1 (hot-list wake coalescing): fan a committed push bundle out to peers ONLY
    /// (the batched MESSAGE_AVAILABLE), WITHOUT the per-push local `wake_local_hint`.
    /// With the hot-list on, the pushing broker coalesces its LOCAL wake into a ~5ms
    /// tick (removing the O(parked consumers) notify_waiters storm), while peers still
    /// get an immediate wake — so a mixed hot-list-on/off cluster keeps prompt
    /// cross-broker discovery. No-op with no transport (single broker).
    pub fn fan_out_pushed_batch(&self, keys: &[(String, String)]) {
        if let Some(t) = self.transport.get() {
            t.send_messages_available_batch(keys);
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

    /// Number of allocated gates (test-only: asserts lazy allocation).
    #[cfg(test)]
    fn gate_count(&self) -> usize {
        self.gates.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unpolled_queue_allocates_no_gate_and_stores_no_hint() {
        // A push to a queue nobody has parked on must not allocate a gate or a
        // hint — this is what keeps the maps bounded by #polled-queues, not
        // #partitions.
        let n = Notifier::new();
        n.wake_local_hint("never-polled", "p1");
        assert_eq!(n.gate_count(), 0);
        assert!(n.drain_hints("never-polled", 10).is_empty());
    }

    #[test]
    fn hints_are_bounded_drop_oldest() {
        let n = Notifier::new();
        n.gate("q"); // simulate a parked pop having created the gate
        for i in 0..100 {
            n.wake_local_hint("q", &format!("p{i}"));
        }
        // Cap is HINT_CAP=64, drop-oldest ⇒ the newest 64 survive (p36..p99).
        let drained = n.drain_hints("q", 1000);
        assert_eq!(drained.len(), HINT_CAP);
        assert_eq!(drained.first().unwrap(), "p36");
        assert_eq!(drained.last().unwrap(), "p99");
        // Draining emptied the mailbox.
        assert!(n.drain_hints("q", 10).is_empty());
    }

    #[test]
    fn drain_dedups_within_a_pass() {
        let n = Notifier::new();
        n.gate("q");
        for p in ["a", "b", "a", "c"] {
            n.wake_local_hint("q", p);
        }
        // One drain returns each partition once, in FIFO order.
        assert_eq!(
            n.drain_hints("q", 10),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
        assert!(n.drain_hints("q", 10).is_empty());
    }

    #[test]
    fn drain_respects_max_and_leaves_the_rest() {
        let n = Notifier::new();
        n.gate("q");
        for p in ["x", "y", "z"] {
            n.wake_local_hint("q", p);
        }
        assert_eq!(n.drain_hints("q", 2), vec!["x".to_string(), "y".to_string()]);
        assert_eq!(n.drain_hints("q", 10), vec!["z".to_string()]);
    }

    #[test]
    fn empty_partition_hint_is_not_stored_but_still_wakes() {
        let n = Notifier::new();
        n.gate("q");
        n.wake_local_hint("q", ""); // no partition ⇒ wake only, no hint
        assert!(n.drain_hints("q", 10).is_empty());
    }

    #[tokio::test]
    async fn wake_delivers_hint_to_a_parked_pop() {
        let n = Notifier::new();
        let n2 = n.clone();
        // Park a waiter; it creates the gate.
        let waiter =
            tokio::spawn(async move { n2.wait_queue("q", Duration::from_secs(5)).await });
        // Give the waiter a moment to arm + register.
        tokio::time::sleep(Duration::from_millis(20)).await;
        n.wake_local_hint("q", "p7");
        assert!(waiter.await.unwrap(), "the parked pop should have been woken");
        // The hint the wake carried is now drainable by that pop.
        assert_eq!(n.drain_hints("q", 10), vec!["p7".to_string()]);
    }
}
