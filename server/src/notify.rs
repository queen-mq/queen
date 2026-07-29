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
//!
//! Track B (PLAN_QUEEN_PROXY_CLOUD.md §5): every `qkey` below is the COMPOSITE
//! `handlers::tenant_queue_key(tenant, queue)`, byte-identical to the hot-list ring
//! key. A bare-name gate on a shared cell both over-wakes (every tenant parked on
//! `orders`) and LOSES wakes: [`drain_hints`] pops from the shared deque, so one
//! tenant's pop consumes the hint another tenant's push produced. The peer frames
//! carry the tenant explicitly, so the key never travels the wire as one blob.

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
    /// Idle-eviction second chance (CLOCK), same contract as `hotlist::QueueState`:
    /// set by any wake/park/drain, cleared by each sweep, so a gate survives at least
    /// one full sweep after its last use. One relaxed store, never a clock read.
    used: std::sync::atomic::AtomicBool,
}

pub struct Notifier {
    /// Per-(tenant, queue) wake gates + hint mailboxes, keyed by the composite
    /// `qkey`. Created lazily by a waiting pop; a push only wakes an existing gate,
    /// so queues nobody polls never allocate one.
    gates: Mutex<HashMap<String, Arc<QueueGate>>>,
    /// Secondary index, bare queue name → the composite gate keys holding it. Read only
    /// by the tenant-less (pre-Track-B peer) wake, which would otherwise scan `gates`
    /// whole on the mesh inbound task. Maintained ONLY with tenancy on, and mutated in
    /// exactly the two places `gates` is (`gate` create branch, `evict_idle`), so it is
    /// bounded by `gates` and is not a second leak.
    by_queue: Mutex<HashMap<String, std::collections::HashSet<String>>>,
    /// Woken by every push, for discovery (namespace/task) pops that span queues
    /// and so have no single gate to park on.
    any: Arc<Notify>,
    /// QUEEN_TENANCY_HEADER. OFF ⇒ only the default tenant exists, so a tenant-less
    /// peer frame resolves to it with one hash lookup instead of the fan-out.
    tenancy: bool,
    /// Optional peer transport (multi-replica mode). Set once at startup.
    transport: OnceLock<Arc<MeshTransport>>,
}

impl Notifier {
    pub fn new(tenancy: bool) -> Arc<Notifier> {
        Arc::new(Notifier {
            gates: Mutex::new(HashMap::new()),
            by_queue: Mutex::new(HashMap::new()),
            any: Arc::new(Notify::new()),
            tenancy,
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

    /// The gate a queue-scoped pop parks on. get-or-create. The returned `Arc` is the
    /// eviction interlock: [`evict_idle`] only drops a gate whose strong count is 1, and
    /// no clone can appear while it holds `gates`, so a pop that has taken a gate here
    /// cannot have it evicted out from under it before it parks.
    fn gate(&self, qkey: &str) -> Arc<QueueGate> {
        let mut g = self.gates.lock().unwrap();
        if let Some(x) = g.get(qkey) {
            x.used.store(true, std::sync::atomic::Ordering::Relaxed);
            return x.clone();
        }
        let x = Arc::new(QueueGate {
            notify: Notify::new(),
            hints: Mutex::new(VecDeque::new()),
            used: std::sync::atomic::AtomicBool::new(true),
        });
        g.insert(qkey.to_string(), x.clone());
        drop(g);
        if self.tenancy {
            let (_, queue) = crate::handlers::split_tenant_queue(qkey);
            self.by_queue
                .lock()
                .unwrap()
                .entry(queue.to_string())
                .or_default()
                .insert(qkey.to_string());
        }
        x
    }

    /// The queue's gate iff one already exists (a pop has parked on it). A push to a
    /// queue nobody polls returns None here — no gate is created and no hint stored,
    /// preserving the lazy-allocation invariant.
    fn existing_gate(&self, qkey: &str) -> Option<Arc<QueueGate>> {
        let g = self.gates.lock().unwrap().get(qkey).cloned();
        if let Some(x) = &g {
            x.used.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        g
    }

    /// Idle eviction (shared-cell memory bound), the [`crate::hotlist::HotList::evict_idle`]
    /// counterpart: drop every gate untouched for a full sweep whose `Arc` the map alone
    /// holds. The strong-count test IS the "no outstanding waiter" test — [`wait_queue`]
    /// keeps its `Arc` alive across the whole park — and it is evaluated under `gates`,
    /// so a parking pop can never race an eviction. Dropping an unused gate is
    /// behaviourally inert: a gate is a wake fast path, the pop's own backoff re-poll is
    /// the correctness floor, and the next parking pop recreates it. Returns the count.
    pub fn evict_idle(&self) -> usize {
        // `gates` is held across the index cleanup for the same reason as
        // `HotList::evict_idle`: `gate`'s create branch reaches `by_queue` only after
        // inserting into `gates`, so holding `gates` makes the pair atomic against a
        // re-create. Lock order is always gates → by_queue.
        let mut g = self.gates.lock().unwrap();
        let mut evicted: Vec<String> = Vec::new();
        g.retain(|qkey, gate| {
            if gate.used.swap(false, std::sync::atomic::Ordering::Relaxed)
                || Arc::strong_count(gate) != 1
            {
                return true;
            }
            evicted.push(qkey.clone());
            false
        });
        if self.tenancy && !evicted.is_empty() {
            let mut idx = self.by_queue.lock().unwrap();
            for qkey in &evicted {
                let (_, queue) = crate::handlers::split_tenant_queue(qkey);
                if let Some(set) = idx.get_mut(queue) {
                    set.remove(qkey);
                    if set.is_empty() {
                        idx.remove(queue);
                    }
                }
            }
        }
        evicted.len()
    }

    /// Number of live gates (the map the memory bound is stated over).
    pub fn gate_count(&self) -> usize {
        self.gates.lock().unwrap().len()
    }

    /// Park a queue-scoped long-poll for up to `dur`, returning the instant a push
    /// to `queue` wakes the gate. Replaces the old blind `sleep(poll_ms)`. The
    /// waiter is armed (`enable`) before the wait, so a wake that arrives during the
    /// window is delivered; a wake missed in the tiny arm gap simply times out and
    /// the caller re-polls — identical to the pre-existing poll-interval ceiling.
    /// Returns true if a push woke the gate, false if `dur` elapsed first — the
    /// caller uses this to reset (on wake) vs advance (on timeout) its long-poll
    /// backoff (RUSTFIX item 19).
    pub async fn wait_queue(&self, qkey: &str, dur: Duration) -> bool {
        let gate = self.gate(qkey);
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
    pub fn wake_local_hint(&self, qkey: &str, partition: &str) {
        if !qkey.is_empty() {
            if let Some(gate) = self.existing_gate(qkey) {
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

    /// The receive path for a peer frame that carries NO tenant (a pre-Track-B build,
    /// rolling upgrade). Runs on the mesh inbound task, once per frame item.
    ///
    /// * Tenancy OFF (the OSS default): only the default tenant can exist, so the frame
    ///   resolves to exactly one gate — one hash lookup, the pre-Track-B cost. This is
    ///   the branch every OSS HA rolling upgrade takes.
    /// * Tenancy ON: wake every tenant holding the name (mapping it to the default
    ///   tenant would drop the wake for all the others), found through the `by_queue`
    ///   index so the cost tracks the holders of that name, not the whole gate map.
    pub fn wake_local_hint_all_tenants(&self, queue: &str, partition: &str) {
        if queue.is_empty() {
            return;
        }
        if !self.tenancy {
            let qkey =
                crate::handlers::tenant_queue_key(crate::config::DEFAULT_TENANT, queue);
            self.wake_local_hint(&qkey, partition);
            return;
        }
        let keys: Vec<String> = match self.by_queue.lock().unwrap().get(queue) {
            Some(set) => set.iter().cloned().collect(),
            None => {
                self.any.notify_waiters();
                return;
            }
        };
        for k in keys {
            self.wake_local_hint(&k, partition);
        }
        self.any.notify_waiters();
    }

    /// Drain up to `max` distinct partition hints for `qkey` (FIFO, deduped),
    /// removing them from the mailbox. Empty when no gate exists or nothing is
    /// queued. A woken pop calls this to pick targeted partitions. DESTRUCTIVE, which
    /// is why the gate must be per-tenant: on a shared gate another tenant's pop would
    /// consume the hint this tenant's push produced (a LOST wake, not a duplicated one).
    pub fn drain_hints(&self, qkey: &str, max: usize) -> Vec<String> {
        let Some(gate) = self.existing_gate(qkey) else {
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
    /// MESSAGE_AVAILABLE to peers. `qkey` is the composite key; the peer frame
    /// carries the tenant and the queue as separate fields.
    pub fn notify_pushed(&self, qkey: &str, partition: &str) {
        self.wake_local_hint(qkey, partition);
        if let Some(t) = self.transport.get() {
            t.send_message_available(qkey, partition);
        }
    }

    /// A committed push bundle landed, touching every (qkey, partition) in `keys`:
    /// wake local pops for each queue (with its partition hint), then fan the whole
    /// set out to peers as ONE batched MESSAGE_AVAILABLE frame (instead of one send
    /// per partition). The single-item [`notify_pushed`] stays for the HTTP
    /// `/internal/api/notify` path.
    pub fn notify_pushed_batch(&self, keys: &[(String, String)]) {
        for (qkey, partition) in keys {
            self.wake_local_hint(qkey, partition);
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

    pub fn broadcast_queue_config_set(&self, qkey: &str) {
        if let Some(t) = self.transport.get() {
            t.send_queue_config_set(qkey);
        }
    }

    pub fn broadcast_queue_config_delete(&self, qkey: &str) {
        if let Some(t) = self.transport.get() {
            t.send_queue_config_delete(qkey);
        }
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
        let n = Notifier::new(false);
        n.wake_local_hint("never-polled", "p1");
        assert_eq!(n.gate_count(), 0);
        assert!(n.drain_hints("never-polled", 10).is_empty());
    }

    #[test]
    fn hints_are_bounded_drop_oldest() {
        let n = Notifier::new(false);
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
        let n = Notifier::new(false);
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
        let n = Notifier::new(false);
        n.gate("q");
        for p in ["x", "y", "z"] {
            n.wake_local_hint("q", p);
        }
        assert_eq!(n.drain_hints("q", 2), vec!["x".to_string(), "y".to_string()]);
        assert_eq!(n.drain_hints("q", 10), vec!["z".to_string()]);
    }

    #[test]
    fn empty_partition_hint_is_not_stored_but_still_wakes() {
        let n = Notifier::new(false);
        n.gate("q");
        n.wake_local_hint("q", ""); // no partition ⇒ wake only, no hint
        assert!(n.drain_hints("q", 10).is_empty());
    }

    #[tokio::test]
    async fn wake_delivers_hint_to_a_parked_pop() {
        let n = Notifier::new(false);
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

    // ---------------------------------------------------------------- tenancy
    const TA: &str = "11111111-1111-1111-1111-111111111111";
    const TB: &str = "22222222-2222-2222-2222-222222222222";
    fn k(t: &str, q: &str) -> String {
        crate::handlers::tenant_queue_key(t, q)
    }

    // drain_hints POPS from the deque, so a gate shared by two tenants means tenant
    // B's pop CONSUMES the hint tenant A's push produced — a lost wake for the
    // rightful owner, not merely a duplicated one.
    #[test]
    fn a_hint_is_not_consumed_by_another_tenants_drain() {
        let n = Notifier::new(false);
        let (ka, kb) = (k(TA, "orders"), k(TB, "orders"));
        n.gate(&ka);
        n.gate(&kb);
        n.wake_local_hint(&ka, "p1");
        assert!(n.drain_hints(&kb, 10).is_empty(), "B must not steal A's hint");
        assert_eq!(n.drain_hints(&ka, 10), vec!["p1".to_string()]);
    }

    #[tokio::test]
    async fn wake_wakes_only_the_owning_tenants_gate() {
        let n = Notifier::new(false);
        let (ka, kb) = (k(TA, "orders"), k(TB, "orders"));
        let n2 = n.clone();
        let kb2 = kb.clone();
        let waiter =
            tokio::spawn(async move { n2.wait_queue(&kb2, Duration::from_millis(300)).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        n.wake_local_hint(&ka, "p1");
        assert!(!waiter.await.unwrap(), "tenant B's gate must time out");
    }

    // D2b: with tenancy ON a peer frame with no tenant fans out to every tenant holding
    // that queue name (a safe over-wake), never to the default tenant alone.
    #[test]
    fn tenantless_wake_fans_out_to_every_tenant_gate() {
        let n = Notifier::new(true);
        let (ka, kb, other) = (k(TA, "orders"), k(TB, "orders"), k(TA, "other"));
        n.gate(&ka);
        n.gate(&kb);
        n.gate(&other);
        n.wake_local_hint_all_tenants("orders", "p1");
        assert_eq!(n.drain_hints(&ka, 10), vec!["p1".to_string()]);
        assert_eq!(n.drain_hints(&kb, 10), vec!["p1".to_string()]);
        assert!(n.drain_hints(&other, 10).is_empty(), "other queues untouched");
    }

    // R1 — the flag-OFF invariant, gate side. QUEEN_TENANCY_HEADER unset ⇒ only the
    // default tenant exists ⇒ a tenant-less peer frame is one hash lookup, as it was
    // pre-Track-B. A gate that only a full scan of the map could reach must stay cold.
    #[test]
    fn tenantless_wake_is_o1_default_tenant_when_tenancy_off() {
        let n = Notifier::new(false);
        let def = k(crate::config::DEFAULT_TENANT, "orders");
        let kb = k(TB, "orders");
        n.gate(&def);
        n.gate(&kb);
        n.wake_local_hint_all_tenants("orders", "p1");
        assert_eq!(n.drain_hints(&def, 10), vec!["p1".to_string()]);
        assert!(
            n.drain_hints(&kb, 10).is_empty(),
            "flag-off must not scan the gate map — only the default tenant is reachable"
        );
    }

    // R3 — gates were never removed either, and a gate is created from a client-supplied
    // queue name by any parking long-poll. Same second-chance contract as the rings.
    #[test]
    fn idle_gates_are_evicted_after_a_full_sweep() {
        let n = Notifier::new(false);
        for i in 0..1_000 {
            n.gate(&k(TA, &format!("junk-{i}")));
        }
        assert_eq!(n.gate_count(), 1_000);
        assert_eq!(n.evict_idle(), 0, "second chance keeps a just-used gate");
        assert_eq!(n.evict_idle(), 1_000);
        assert_eq!(n.gate_count(), 0);
    }

    // A wake counts as use, so a gate a producer is actively hinting is never reclaimed
    // out from under the consumer that is about to drain it.
    #[test]
    fn a_woken_gate_survives_the_sweep() {
        let n = Notifier::new(false);
        n.gate("q");
        n.evict_idle();
        n.wake_local_hint("q", "p1"); // marks it used again
        assert_eq!(n.evict_idle(), 0, "a gate used since the last sweep is kept");
        assert_eq!(n.drain_hints("q", 10), vec!["p1".to_string()]);
    }

    // The eviction interlock: a parked pop holds the gate's Arc for the whole park, so
    // the strong-count test under `gates` makes "has an outstanding waiter" and "is
    // evictable" mutually exclusive — no sweep can strand a parked consumer.
    #[tokio::test]
    async fn a_parked_waiter_is_never_evicted_and_still_wakes() {
        let n = Notifier::new(false);
        let n2 = n.clone();
        let waiter =
            tokio::spawn(async move { n2.wait_queue("q", Duration::from_secs(2)).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Two sweeps: the second chance is spent, yet the waiter keeps the gate alive.
        assert_eq!(n.evict_idle(), 0);
        assert_eq!(n.evict_idle(), 0, "a parked waiter must block eviction");
        assert_eq!(n.gate_count(), 1);
        n.wake_local_hint("q", "p1");
        assert!(waiter.await.unwrap(), "the parked pop must still be woken");
    }

    // …and eviction reclaims the fan-out index with it (no second unbounded map).
    #[test]
    fn gate_eviction_reclaims_the_fanout_index() {
        let n = Notifier::new(true);
        n.gate(&k(TA, "orders"));
        n.gate(&k(TB, "orders"));
        assert_eq!(n.by_queue.lock().unwrap().len(), 1);
        n.evict_idle();
        assert_eq!(n.evict_idle(), 2);
        assert!(n.by_queue.lock().unwrap().is_empty(), "the emptied name-set is removed");
    }
}
