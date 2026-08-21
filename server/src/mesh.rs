//! Framed TCP inter-instance mesh transport (segments Rust broker).
//!
//! Replaces the homemade UDP transport (`udp.rs`). Queen runs 1–3 identical
//! broker replicas over one Postgres; PG is the sole source of truth, so this
//! mesh only carries best-effort hints between peers:
//!
//!   1. MESSAGE_AVAILABLE — a push committed on replica A wakes a long-poll pop
//!      parked on replica B at once, instead of waiting out its backoff,
//!   2. maintenance-mode flips propagate to every replica,
//!   3. queue-config changes invalidate peers' caches.
//!
//! None of this needs guaranteed delivery: `reconcile.rs` re-reads the DB every
//! ~60s and the pop long-poll re-queries on its own backoff, so a dropped frame
//! only ever costs a little latency. That invariant is why the send path DROPS
//! rather than buffers when a peer is slow or down — it never blocks a caller and
//! never grows without bound.
//!
//! ## Topology
//! A full mesh of static peers from config. Every node LISTENS on the mesh port
//! and DIALS every configured peer with a reconnect loop. To sidestep connection
//! tie-breaking the two directions are asymmetric: a node SENDS only on its
//! outbound (dialled) connections and RECEIVES only on inbound (accepted) ones.
//! Between any two nodes there are thus two connections, each used one-way.
//!
//! ## Framing
//! Length-prefixed frames: `u32 BE length | u8 type | JSON payload`, where
//! `length` counts the type byte plus the payload. No fixed header, no per-frame
//! signature, no sequence numbers — TCP already provides ordering and integrity,
//! and the mesh tolerates loss. The type tags and JSON payload shapes are
//! inherited from the UDP transport so the inbound effects layer (the `main.rs`
//! `SyncHandlers`) is unchanged. Framing removes the old 1308-byte datagram cap,
//! which is what makes the batched MESSAGE_AVAILABLE form (below) possible.
//!
//! ## Auth
//! A per-connection handshake replaces per-packet HMAC. On connect the dialer
//! sends a HELLO frame `{server_id, nonce, mac}` with `mac = HMAC-SHA256(secret,
//! nonce)`; the listener verifies it (constant-time) before accepting any further
//! frame. An empty secret means open mode (parity with the old insecure default):
//! the HELLO is still exchanged to learn the peer's server_id, just not verified.
//! There is no cross-version / C++ wire compatibility requirement (Rust-only
//! clusters), so the wire is free to be this simple.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

type HmacSha256 = Hmac<Sha256>;

// Message type tags — inherited from `udp.rs` so the inbound effects layer is
// unchanged. Two are new: a connection-level HELLO handshake, and a batched
// MESSAGE_AVAILABLE the datagram cap used to forbid.
pub const T_MESSAGE_AVAILABLE: u8 = 1;
pub const T_MESSAGE_AVAILABLE_BATCH: u8 = 2;
pub const T_HEARTBEAT: u8 = 3;
pub const T_HELLO: u8 = 4;
// 19-wildcard-hotlist §5: batched, coalesced dirty(queue, partition) hints for a
// peer's candidate hot-list, sent only on a vuoto→pending transition (not per
// message). Receipt = an idempotent, commutative mark on the peer's hot-list
// (set + epoch bump). Separate from MESSAGE_AVAILABLE (which is a parked-pop wake
// on EVERY push): a dropped hint is healed by the reseed floor (§8), so — like
// every mesh frame — it is best-effort and never blocks a caller.
pub const T_HOTLIST_DIRTY_BATCH: u8 = 5;
pub const T_QUEUE_CONFIG_SET: u8 = 10;
pub const T_QUEUE_CONFIG_DELETE: u8 = 11;
// EPHEMERAL_QUEUES.md §3.5 — the ephemeral control frame: `{op, tenant, queue}`
// with `op ∈ reset | delete | config_set`, broadcast fire-and-forget by the three
// admin verbs. Its own DECADE and not `12`, so the deferred `T_EPH_DATA_BATCH`
// (§3.5/§9, the replication payload lane) has a neighbouring tag when it lands
// and the two are legible as one family in a packet dump.
pub const T_EPH_ADMIN: u8 = 20;
pub const T_MAINTENANCE_MODE_SET: u8 = 40;
pub const T_POP_MAINTENANCE_MODE_SET: u8 = 41;

// Reject a length prefix larger than this before allocating, so a corrupt or
// hostile peer cannot drive an OOM with a bogus 4 GiB length. Batched wakes are
// the only non-tiny frame and even 100k items fit comfortably under this.
const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

// Bounded per-peer send queue. Full ⇒ drop + bump `dropped` (see the module
// header: loss is healed by reconcile + pop backoff, so a caller is never blocked
// and the queue never grows without bound).
const PEER_QUEUE: usize = 1024;

// Reconnect backoff bounds for the dial loop.
const RECONNECT_MIN: Duration = Duration::from_millis(250);
const RECONNECT_MAX: Duration = Duration::from_secs(5);

/// Effects a received frame applies to local state. `main.rs` supplies these
/// closures (capturing the shared `AppState`/`Notifier`) so this module stays
/// decoupled from the handler layer. All run on an inbound connection task, so
/// they must be cheap and non-blocking (they only touch atomics / in-memory maps).
///
/// Track B (PLAN_QUEEN_PROXY_CLOUD.md §5): the queue-carrying frames also carry the
/// TENANT, passed here as `Option<&str>` — never `""` as a sentinel, because an
/// ABSENT tenant (a pre-Track-B peer during a rolling upgrade) and the default tenant
/// are different things: the former must fan out to every tenant holding that queue
/// name, the latter targets exactly one ring.
pub struct SyncHandlers {
    pub on_message_available: Box<dyn Fn(Option<&str>, &str, &str) + Send + Sync>,
    // 19-wildcard-hotlist §5: a peer's coalesced hot-list dirty hint — mark the
    // local hot-list (idempotent, no re-broadcast). The trailing argument is the
    // hint's SCOPE: `Some(group)` marks only that group's ring (a seek / group-delete
    // repair), `None` marks every ring of the queue — which is what a push means and
    // what a peer too old to send the field always means.
    pub on_hotlist_dirty: Box<dyn Fn(Option<&str>, &str, &str, Option<&str>) + Send + Sync>,
    pub on_maintenance: Box<dyn Fn(bool) + Send + Sync>,
    pub on_pop_maintenance: Box<dyn Fn(bool) + Send + Sync>,
    pub on_queue_config_set: Box<dyn Fn(Option<&str>, &str) + Send + Sync>,
    pub on_queue_config_delete: Box<dyn Fn(Option<&str>, &str) + Send + Sync>,
    /// EPHEMERAL_QUEUES.md §3.5 — `(op, tenant, queue)` from a peer's admin verb.
    ///
    /// The tenant is a `&str` and NOT the `Option<&str>` every frame above takes,
    /// and the asymmetry is deliberate: those are WAKES, where a frame with no
    /// tenant degrades safely to an over-wake across every tenant holding the
    /// name. These are DESTRUCTIVE (`reset` drops messages, `delete` drops the
    /// queue), so the same degradation would let one un-attributable frame wipe
    /// every tenant's queue of that name. A frame whose tenant is absent or
    /// malformed is dropped in `dispatch` and never reaches this closure.
    pub on_eph_admin: Box<dyn Fn(&str, &str, &str) + Send + Sync>,
}

/// One live, ephemeral-capable peer (EPHEMERAL_QUEUES.md §3.5, M3).
///
/// "Capable" means the peer's HELLO carried the v2 fields; "live" means a frame
/// arrived from it within `dead_threshold`. A peer that is one but not the other
/// is simply absent from [`MeshTransport::eph_members`], which is what keeps the
/// rendezvous ring of §3.7 identical on every broker that sees the same
/// membership — and what excludes an old peer mid-rolling-deploy without any
/// version negotiation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EphMember {
    pub server_id: String,
    /// Dialable base URL, e.g. `http://queen-b:6632`. No trailing slash.
    pub http_addr: String,
    /// The peer's `eph_epoch` (M4) as of its last handshake. Not used for
    /// routing — it is how a log line or a support ticket can tell a peer that
    /// RESTARTED from one that merely reconnected.
    pub epoch: u64,
}

/// What a peer's HELLO said about itself.
struct PeerHello {
    server_id: String,
    /// `None` ⇒ pre-v2 peer ⇒ not ephemeral-capable (§8 compat matrix).
    eph: Option<(String, u64)>,
}

// One configured outbound peer: the bounded sender its writer task drains, plus
// the display/liveness state the stats surface reads.
struct Peer {
    host: String,
    port: u16,
    tx: mpsc::Sender<Arc<[u8]>>,
    connected: Arc<AtomicBool>,
    remote_ip: Arc<Mutex<Option<String>>>,
}

/// The listener + per-peer receivers produced by [`MeshTransport::bind`]. Kept
/// separate from the `Arc<MeshTransport>` so the transport itself stays `Sync`
/// (a `TcpListener`/`Receiver` are single-owner) and are handed to
/// [`MeshTransport::start`] once wiring is complete.
pub struct MeshBindings {
    listener: TcpListener,
    // Index-aligned with `MeshTransport::peers`: peer i's writer task owns rx i.
    peer_rxs: Vec<mpsc::Receiver<Arc<[u8]>>>,
}

impl MeshBindings {
    /// The actual bound listen address (resolves the real port when bound to the
    /// ephemeral port 0 — used by tests to cross-wire peers).
    #[allow(dead_code)]
    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }
}

pub struct MeshTransport {
    server_id: String,
    mesh_port: u16,
    secret: String,
    heartbeat: Duration,
    dead_threshold: Duration,
    /// HELLO v2 (§3.5): what WE advertise. See `config::SyncConfig::http_addr`.
    http_addr: String,
    /// HELLO v2 (§3.5, M4): this incarnation's `eph_epoch`, taken from the
    /// engine at bind. Threaded through the transport rather than read from a
    /// global, because there is exactly one engine per process and the mesh
    /// should not be the thing that knows how to find it.
    eph_epoch: u64,

    peers: Vec<Peer>,
    // Inbound liveness: last time we received ANY frame from a peer, keyed by the
    // server_id it announced in its HELLO. A peer whose last inbound frame is
    // older than `dead_threshold` is reported dead in stats.
    liveness: Mutex<HashMap<String, Instant>>,
    /// HELLO v2 (§3.5, M3): `server_id -> (http_addr, eph_epoch)` for the peers
    /// that advertised them. A peer ABSENT here has handshaked with a pre-v2
    /// build and is not ephemeral-capable; it still wakes and still flips
    /// maintenance, it is simply not in the rendezvous ring.
    ///
    /// Overwritten on every HELLO, so a peer that restarted publishes its new
    /// epoch (and, after a redeploy onto a new pod, its new address) the moment
    /// it re-dials — the liveness map alone could not tell those apart (M4).
    eph_peers: Mutex<HashMap<String, (String, u64)>>,
    handlers: SyncHandlers,

    sent: AtomicU64,
    received: AtomicU64,
    dropped: AtomicU64,
    handshake_failures: AtomicU64,
}

impl MeshTransport {
    /// Bind the listener and build the per-peer send channels. Does not spawn
    /// tasks — call [`start`](Self::start) with the returned bindings after wiring
    /// (e.g. `Notifier::attach_transport`) is complete. A bind failure (port in
    /// use) surfaces here so `main.rs` can fall back to the local waker.
    /// `eph_epoch` is the engine's boot incarnation id (M4), advertised in
    /// HELLO v2. It is a PARAMETER and not a `SyncConfig` field because it is a
    /// runtime value drawn by `Ephemeral::new`, not something an operator sets;
    /// a config field would invite someone to make it configurable, which would
    /// defeat the fencing it exists for.
    pub async fn bind(
        cfg: &crate::config::SyncConfig,
        handlers: SyncHandlers,
        eph_epoch: u64,
    ) -> std::io::Result<(Arc<MeshTransport>, MeshBindings)> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", cfg.mesh_port)).await?;
        let mut peers = Vec::with_capacity(cfg.peers.len());
        let mut peer_rxs = Vec::with_capacity(cfg.peers.len());
        for (h, p) in &cfg.peers {
            let (tx, rx) = mpsc::channel::<Arc<[u8]>>(PEER_QUEUE);
            peers.push(Peer {
                host: h.clone(),
                port: *p,
                tx,
                connected: Arc::new(AtomicBool::new(false)),
                remote_ip: Arc::new(Mutex::new(None)),
            });
            peer_rxs.push(rx);
        }
        let t = Arc::new(MeshTransport {
            server_id: cfg.server_id.clone(),
            mesh_port: cfg.mesh_port,
            secret: cfg.secret.clone(),
            heartbeat: Duration::from_millis(cfg.heartbeat_ms),
            dead_threshold: Duration::from_millis(cfg.dead_threshold_ms),
            http_addr: cfg.http_addr.clone(),
            eph_epoch,
            peers,
            liveness: Mutex::new(HashMap::new()),
            eph_peers: Mutex::new(HashMap::new()),
            handlers,
            sent: AtomicU64::new(0),
            received: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            handshake_failures: AtomicU64::new(0),
        });
        Ok((t, MeshBindings { listener, peer_rxs }))
    }

    /// Spawn the accept loop, one dial+writer task per peer, and the liveness
    /// monitor.
    pub fn start(self: &Arc<Self>, bindings: MeshBindings) {
        let acc = self.clone();
        tokio::spawn(async move { acc.accept_loop(bindings.listener).await });
        for (i, rx) in bindings.peer_rxs.into_iter().enumerate() {
            let me = self.clone();
            tokio::spawn(async move { me.peer_task(i, rx).await });
        }
        let mon = self.clone();
        tokio::spawn(async move { mon.liveness_monitor().await });
        tracing::info!(
            target: "mesh",
            server_id = %self.server_id,
            port = self.mesh_port,
            peers = self.peers.len(),
            auth = if self.secret.is_empty() { "off" } else { "hmac" },
            "started"
        );
    }

    // ----------------------------------------------------------------- send side

    /// Fan one pre-encoded frame out to every peer without ever blocking: a
    /// `try_send` into each peer's bounded queue. A full queue (peer slow) or a
    /// closed one (peer down) drops the frame and bumps `dropped` — the contract
    /// that keeps the `Notifier` call-sites off the network path.
    fn broadcast(&self, frame: Arc<[u8]>) {
        for peer in &self.peers {
            if peer.tx.try_send(frame.clone()).is_err() {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// `qkey` is the composite `tenant_queue_key`; it is SPLIT here so the wire
    /// carries `tenant` and `queue` as separate fields (a peer keys its own maps).
    pub fn send_message_available(&self, qkey: &str, partition: &str) {
        let (tenant, queue) = crate::handlers::split_tenant_queue(qkey);
        let payload = serde_json::to_vec(&serde_json::json!({
            "queue": queue,
            "partition": partition,
            "tenant": tenant,
        }))
        .unwrap_or_default();
        self.broadcast(encode_frame(T_MESSAGE_AVAILABLE, &payload).into());
    }

    /// Batched MESSAGE_AVAILABLE: one frame carrying every (qkey, partition) a
    /// committed push touched, so a multi-partition push wakes peers with a single
    /// send instead of one per partition. Empty input is a no-op.
    pub fn send_messages_available_batch(&self, items: &[(String, String)]) {
        if items.is_empty() {
            return;
        }
        let arr: Vec<serde_json::Value> = items
            .iter()
            .map(|(qk, p)| {
                let (t, q) = crate::handlers::split_tenant_queue(qk);
                serde_json::json!({ "queue": q, "partition": p, "tenant": t })
            })
            .collect();
        let payload = serde_json::to_vec(&serde_json::json!({ "items": arr })).unwrap_or_default();
        self.broadcast(encode_frame(T_MESSAGE_AVAILABLE_BATCH, &payload).into());
    }

    /// 19-wildcard-hotlist §5: batched hot-list dirty hints. Same wire shape as
    /// the batched MESSAGE_AVAILABLE (`{"items":[{queue,partition,tenant}]}`), a
    /// distinct tag so a peer marks its hot-list without conflating it with a pop
    /// wake. Empty input is a no-op.
    ///
    /// A group-scoped hint adds `"group"`, which is OMITTED entirely when the hint is
    /// queue-wide — the push path is 99.99% of items and its frames stay byte-identical
    /// to 1.0.1-beta.1. Adding an optional field is how `tenant` was added in Track B
    /// and is safe in both rolling-upgrade directions: an older peer ignores the key and
    /// does the over-mark it always did; a newer peer reading a frame without it takes
    /// the same `None` branch. A new frame TAG would instead be dropped outright.
    pub fn send_hotlist_dirty_batch(&self, items: &[crate::hotlist::DirtyHint]) {
        if items.is_empty() {
            return;
        }
        let arr: Vec<serde_json::Value> = items
            .iter()
            .map(|h| {
                let (t, q) = crate::handlers::split_tenant_queue(&h.qkey);
                let mut it = serde_json::json!({
                    "queue": q, "partition": &*h.partition, "tenant": t
                });
                if let (Some(g), Some(obj)) = (h.group.as_deref(), it.as_object_mut()) {
                    obj.insert("group".into(), serde_json::Value::from(g));
                }
                it
            })
            .collect();
        let payload = serde_json::to_vec(&serde_json::json!({ "items": arr })).unwrap_or_default();
        self.broadcast(encode_frame(T_HOTLIST_DIRTY_BATCH, &payload).into());
    }

    pub fn send_maintenance(&self, enabled: bool) {
        let payload =
            serde_json::to_vec(&serde_json::json!({ "enabled": enabled })).unwrap_or_default();
        self.broadcast(encode_frame(T_MAINTENANCE_MODE_SET, &payload).into());
    }

    pub fn send_pop_maintenance(&self, enabled: bool) {
        let payload =
            serde_json::to_vec(&serde_json::json!({ "enabled": enabled })).unwrap_or_default();
        self.broadcast(encode_frame(T_POP_MAINTENANCE_MODE_SET, &payload).into());
    }

    pub fn send_queue_config_set(&self, qkey: &str) {
        let (tenant, queue) = crate::handlers::split_tenant_queue(qkey);
        let payload = serde_json::to_vec(&serde_json::json!({ "queue": queue, "tenant": tenant }))
            .unwrap_or_default();
        self.broadcast(encode_frame(T_QUEUE_CONFIG_SET, &payload).into());
    }

    /// EPHEMERAL_QUEUES.md §3.5 — broadcast one ephemeral admin op.
    ///
    /// FIRE AND FORGET, on the same terms as every other frame in this module: a
    /// dropped one costs a ghost ring until the config reconcile heals it (§10
    /// Q4), never correctness — the contents of an ephemeral queue are allowed to
    /// disappear (§1.2), so the worst case of a lost `reset` is that a peer's
    /// copy survives a little longer than the operator expected.
    ///
    /// `tenant` and `queue` are sent SEPARATELY and the queue is the BARE name,
    /// without the `eph:` prefix: the prefix is an internal key convention (M5)
    /// and putting it on the wire would make every receiver strip it back off.
    pub fn send_eph_admin(&self, op: &str, tenant: &str, queue: &str) {
        let payload = serde_json::to_vec(&serde_json::json!({
            "op": op,
            "tenant": tenant,
            "queue": queue,
        }))
        .unwrap_or_default();
        self.broadcast(encode_frame(T_EPH_ADMIN, &payload).into());
    }

    pub fn send_queue_config_delete(&self, qkey: &str) {
        let (tenant, queue) = crate::handlers::split_tenant_queue(qkey);
        let payload = serde_json::to_vec(&serde_json::json!({ "queue": queue, "tenant": tenant }))
            .unwrap_or_default();
        self.broadcast(encode_frame(T_QUEUE_CONFIG_DELETE, &payload).into());
    }

    // ---- outbound: dial + writer -------------------------------------------

    /// Owns peer `i`'s outbound connection for the process lifetime: dial, HELLO,
    /// stream frames, and reconnect with exponential backoff on any drop. Re-dials
    /// by hostname on every attempt, so a peer whose IP moved is picked up without
    /// a separate DNS refresh loop.
    async fn peer_task(self: Arc<Self>, i: usize, mut rx: mpsc::Receiver<Arc<[u8]>>) {
        let host = self.peers[i].host.clone();
        let port = self.peers[i].port;
        let mut backoff = RECONNECT_MIN;
        loop {
            match self.dial_and_handshake(&host, port).await {
                Ok(stream) => {
                    backoff = RECONNECT_MIN;
                    // Discard anything that queued while we were down BEFORE marking
                    // the peer connected: replaying stale wakes/flips on reconnect is
                    // pointless (reconcile + pop backoff already covered that window)
                    // and could burst an out-of-date maintenance/config state onto
                    // the peer. Draining first also means an observer that sees
                    // `connected` is guaranteed its next send won't be drained.
                    while rx.try_recv().is_ok() {}
                    let ip = stream.peer_addr().ok().map(|a| a.ip().to_string());
                    *self.peers[i].remote_ip.lock().unwrap() = ip;
                    self.peers[i].connected.store(true, Ordering::Relaxed);
                    self.writer_loop(stream, &mut rx).await;
                    self.peers[i].connected.store(false, Ordering::Relaxed);
                    *self.peers[i].remote_ip.lock().unwrap() = None;
                }
                Err(_) => { /* connect/handshake failed — back off and retry */ }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(RECONNECT_MAX);
        }
    }

    /// Connect and send the HELLO handshake frame. The outbound connection is
    /// send-only, so we never read a reply — the listener silently closes on a bad
    /// handshake, which surfaces here as a later write error.
    async fn dial_and_handshake(&self, host: &str, port: u16) -> std::io::Result<TcpStream> {
        // `connect((host, port))` re-resolves the hostname on each call and tries
        // every resolved address in turn, so an IPv4 listener is reached even when
        // `localhost` resolves to `::1` first.
        let mut stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true).ok();
        let hello = encode_frame(
            T_HELLO,
            &build_hello_payload(
                &self.server_id,
                &self.secret,
                &self.http_addr,
                self.eph_epoch,
            ),
        );
        stream.write_all(&hello).await?;
        Ok(stream)
    }

    /// Drain the peer's send queue onto the socket and emit a heartbeat every
    /// `heartbeat` interval (the periodic write that detects a dead peer even when
    /// no wakes are flowing). Returns on any write error or channel close, handing
    /// control back to the reconnect loop.
    async fn writer_loop(&self, mut stream: TcpStream, rx: &mut mpsc::Receiver<Arc<[u8]>>) {
        let mut hb = tokio::time::interval(self.heartbeat);
        hb.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        hb.tick().await; // consume the immediate first tick (we just sent HELLO)
        loop {
            tokio::select! {
                maybe = rx.recv() => {
                    let Some(frame) = maybe else { return };
                    if stream.write_all(&frame).await.is_err() {
                        return;
                    }
                    self.sent.fetch_add(1, Ordering::Relaxed);
                }
                _ = hb.tick() => {
                    let payload = serde_json::to_vec(&serde_json::json!({
                        "server_id": self.server_id,
                    }))
                    .unwrap_or_default();
                    let frame = encode_frame(T_HEARTBEAT, &payload);
                    if stream.write_all(&frame).await.is_err() {
                        return;
                    }
                    self.sent.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    // ---- inbound: accept + dispatch ----------------------------------------

    async fn accept_loop(self: Arc<Self>, listener: TcpListener) {
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    stream.set_nodelay(true).ok();
                    let me = self.clone();
                    tokio::spawn(async move { me.inbound_conn(stream).await });
                }
                Err(_) => {
                    // A transient accept error should not kill the loop.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    /// One inbound (accepted) connection: verify the HELLO, then dispatch every
    /// subsequent frame. Inbound connections are receive-only — we never write
    /// back on them (the peer's replies flow on our own outbound connection to it).
    async fn inbound_conn(self: Arc<Self>, mut stream: TcpStream) {
        let (ty, payload) = match read_frame(&mut stream).await {
            Ok(f) => f,
            Err(_) => return,
        };
        if ty != T_HELLO {
            self.handshake_failures.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let hello = match verify_hello(&payload, &self.secret) {
            Some(h) => h,
            None => {
                self.handshake_failures.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        let sender_id = hello.server_id;
        // HELLO v2 (§3.5). A peer that advertises the pair joins the rendezvous
        // ring; a peer that does not is simply never inserted, which is the whole
        // of the rolling-deploy story — no version field, no negotiation, and an
        // old peer is excluded by the absence of the thing forwarding needs.
        match hello.eph {
            Some((addr, epoch)) => {
                self.eph_peers
                    .lock()
                    .unwrap()
                    .insert(sender_id.clone(), (addr, epoch));
            }
            // A DOWNGRADE must also be honoured: if this peer was capable and
            // has been rolled back, leaving the stale address in the map would
            // forward ephemeral traffic into a broker that answers 404.
            None => {
                self.eph_peers.lock().unwrap().remove(&sender_id);
            }
        }
        self.mark_seen(&sender_id);
        loop {
            match read_frame(&mut stream).await {
                Ok((ty, payload)) => {
                    self.received.fetch_add(1, Ordering::Relaxed);
                    self.mark_seen(&sender_id);
                    self.dispatch(ty, &payload);
                }
                Err(_) => return, // EOF or malformed frame closes the connection
            }
        }
    }

    fn mark_seen(&self, sender_id: &str) {
        self.liveness
            .lock()
            .unwrap()
            .insert(sender_id.to_string(), Instant::now());
    }

    /// This broker's own mesh identity — the rendezvous ring's node key (§3.7).
    pub fn server_id(&self) -> &str {
        &self.server_id
    }

    /// EPHEMERAL_QUEUES.md §3.5/M3 — the live, ephemeral-capable peers.
    ///
    /// SELF IS NOT IN THE LIST. The caller adds it (§3.7: "over `eph_members()` ∪
    /// self"), because self's liveness is not a question this map can answer and
    /// a transport that inserted itself would make an empty return value
    /// ambiguous between "no peers" and "not wired".
    ///
    /// Two filters, and they are different questions:
    ///   * ALIVE — a frame within `dead_threshold`, from the same map `stats()`
    ///     counts. A peer that stops heartbeating leaves the ring, its partitions
    ///     re-hash to whoever is left, and their contents are gone (§1.2/§1.4).
    ///   * CAPABLE — its HELLO carried `http_addr` and `eph_epoch`. Without an
    ///     address there is nowhere to forward to (§3.6), so an old peer is
    ///     excluded rather than guessed at.
    ///
    /// Sorted by `server_id` so two brokers building the same ring from the same
    /// membership feed the hash the same list — the tie-break in `hrw_pick` is
    /// then the only thing that has to agree, not the iteration order of a map.
    pub fn eph_members(&self) -> Vec<EphMember> {
        let now = Instant::now();
        // Fixed order (liveness, then eph_peers) wherever both are held. They are
        // only ever taken together here, but writing the rule down is cheaper
        // than rediscovering it from a deadlock.
        let live = self.liveness.lock().unwrap();
        let caps = self.eph_peers.lock().unwrap();
        let mut out: Vec<EphMember> = caps
            .iter()
            .filter_map(|(id, (addr, epoch))| {
                let last = live.get(id)?;
                if now.duration_since(*last) > self.dead_threshold {
                    return None;
                }
                Some(EphMember {
                    server_id: id.clone(),
                    http_addr: addr.clone(),
                    epoch: *epoch,
                })
            })
            .collect();
        drop(caps);
        drop(live);
        out.sort_by(|a, b| a.server_id.cmp(&b.server_id));
        out
    }

    /// Log alive→dead and dead→alive transitions for operability (the stats
    /// surface carries the live counts; this makes a peer going away visible in the
    /// broker log without polling). Keyed by the server_id a peer announced in its
    /// HELLO, so a peer that was never seen is simply absent, not "dead".
    async fn liveness_monitor(self: Arc<Self>) {
        let tick = (self.dead_threshold / 2).max(Duration::from_millis(250));
        let mut interval = tokio::time::interval(tick);
        let mut dead: std::collections::HashSet<String> = std::collections::HashSet::new();
        loop {
            interval.tick().await;
            let now = Instant::now();
            let snapshot: Vec<(String, bool)> = {
                let l = self.liveness.lock().unwrap();
                l.iter()
                    .map(|(id, last)| (id.clone(), now.duration_since(*last) > self.dead_threshold))
                    .collect()
            };
            for (id, is_dead) in snapshot {
                if is_dead {
                    if dead.insert(id.clone()) {
                        tracing::warn!(
                            target: "mesh",
                            peer = %id,
                            threshold_ms = self.dead_threshold.as_millis() as u64,
                            "peer down"
                        );
                    }
                } else if dead.remove(&id) {
                    tracing::info!(target: "mesh", peer = %id, "peer recovered");
                }
            }
        }
    }

    fn dispatch(&self, ty: u8, payload: &[u8]) {
        match ty {
            T_HEARTBEAT => { /* liveness already recorded in inbound_conn */ }
            T_MESSAGE_AVAILABLE => {
                let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) else { return };
                let q = v.get("queue").and_then(|x| x.as_str()).unwrap_or("");
                if q.is_empty() {
                    return;
                }
                let p = v.get("partition").and_then(|x| x.as_str()).unwrap_or("");
                let t = frame_tenant(&v);
                (self.handlers.on_message_available)(t.as_deref(), q, p);
            }
            T_MESSAGE_AVAILABLE_BATCH => {
                let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) else { return };
                let Some(items) = v.get("items").and_then(|x| x.as_array()) else { return };
                for it in items {
                    let q = it.get("queue").and_then(|x| x.as_str()).unwrap_or("");
                    if q.is_empty() {
                        continue;
                    }
                    let p = it.get("partition").and_then(|x| x.as_str()).unwrap_or("");
                    let t = frame_tenant(it);
                    (self.handlers.on_message_available)(t.as_deref(), q, p);
                }
            }
            T_HOTLIST_DIRTY_BATCH => {
                let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) else { return };
                let Some(items) = v.get("items").and_then(|x| x.as_array()) else { return };
                for it in items {
                    let q = it.get("queue").and_then(|x| x.as_str()).unwrap_or("");
                    if q.is_empty() {
                        continue;
                    }
                    let p = it.get("partition").and_then(|x| x.as_str()).unwrap_or("");
                    let t = frame_tenant(it);
                    // Absent, non-string or empty ⇒ None ⇒ today's queue-wide mark.
                    // Mirrors `frame_tenant` deliberately: an unreadable optional field
                    // degrades to the safe over-mark, never to a bogus key.
                    let g = it
                        .get("group")
                        .and_then(|x| x.as_str())
                        .filter(|s| !s.is_empty());
                    (self.handlers.on_hotlist_dirty)(t.as_deref(), q, p, g);
                }
            }
            T_MAINTENANCE_MODE_SET => {
                let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) else { return };
                let e = v.get("enabled").and_then(|x| x.as_bool()).unwrap_or(false);
                (self.handlers.on_maintenance)(e);
            }
            T_POP_MAINTENANCE_MODE_SET => {
                let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) else { return };
                let e = v.get("enabled").and_then(|x| x.as_bool()).unwrap_or(false);
                (self.handlers.on_pop_maintenance)(e);
            }
            T_QUEUE_CONFIG_SET => {
                let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) else { return };
                if let Some(q) = v.get("queue").and_then(|x| x.as_str()) {
                    let t = frame_tenant(&v);
                    (self.handlers.on_queue_config_set)(t.as_deref(), q);
                }
            }
            T_QUEUE_CONFIG_DELETE => {
                let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) else { return };
                if let Some(q) = v.get("queue").and_then(|x| x.as_str()) {
                    let t = frame_tenant(&v);
                    (self.handlers.on_queue_config_delete)(t.as_deref(), q);
                }
            }
            T_EPH_ADMIN => {
                let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) else { return };
                let Some(op) = v.get("op").and_then(|x| x.as_str()).filter(|s| !s.is_empty())
                else {
                    return;
                };
                let Some(q) = v.get("queue").and_then(|x| x.as_str()).filter(|s| !s.is_empty())
                else {
                    return;
                };
                // The one frame family that REFUSES the tenant-less fan-out the
                // wakes take (see `on_eph_admin`): these ops destroy, and an
                // un-attributable destructive frame is dropped, not broadened.
                let Some(t) = frame_tenant(&v) else { return };
                (self.handlers.on_eph_admin)(op, &t, q);
            }
            // AN UNKNOWN TAG IS SKIPPED AND THE CONNECTION SURVIVES (§11.2).
            //
            // This is the whole of the rolling-deploy contract in one arm: a peer
            // one version ahead sends a frame this build has no name for, and the
            // only correct reaction is to ignore that frame — closing the socket
            // would take every WAKE on it down with the tag nobody knew, turning
            // a forward-compatible addition into a cluster-wide latency event.
            // Framing is what makes it safe: the length prefix is read before the
            // type, so the reader can step over a frame it cannot interpret. It
            // is also why a NEW capability rides new FIELDS on an existing frame
            // (HELLO v2 above) whenever it can, and a new tag only when it must.
            _ => {}
        }
    }

    // ------------------------------------------------------------------- stats

    /// Status-endpoint stats. Field shape is kept compatible with the old UDP
    /// transport (peer list, sent/received/dropped counters, alive/dead counts);
    /// `signature_failures` now counts handshake rejections and `sequence_rejections`
    /// is retained as a constant 0 (TCP carries no per-frame sequence to reject).
    pub fn stats(&self) -> serde_json::Value {
        let now = Instant::now();
        let peers_json: Vec<serde_json::Value> = self
            .peers
            .iter()
            .map(|p| {
                let connected = p.connected.load(Ordering::Relaxed);
                serde_json::json!({
                    "host": p.host,
                    "port": p.port,
                    "connected": connected,
                    // Back-compat with the UDP shape: "we have a live path to this
                    // peer" (an established outbound connection), plus its remote IP.
                    "resolved": connected,
                    "resolved_ip": p.remote_ip.lock().unwrap().clone(),
                })
            })
            .collect();

        let (alive, dead) = {
            let l = self.liveness.lock().unwrap();
            let mut alive = 0usize;
            let mut dead = 0usize;
            for last in l.values() {
                if now.duration_since(*last) <= self.dead_threshold {
                    alive += 1;
                } else {
                    dead += 1;
                }
            }
            (alive, dead)
        };

        let hs = self.handshake_failures.load(Ordering::Relaxed);
        serde_json::json!({
            "server_id": self.server_id,
            "transport": "tcp-mesh",
            "port": self.mesh_port,
            "peer_count": peers_json.len(),
            "peers": peers_json,
            "servers_alive": alive,
            "servers_dead": dead,
            // EPHEMERAL_QUEUES.md §3.5. How many of the live peers are in the
            // rendezvous ring — i.e. advertised HELLO v2. During a rolling
            // deploy this is BELOW `servers_alive` and that is the expected
            // reading, not an error: the old peers still wake and still flip
            // maintenance, they just do not own ephemeral partitions.
            "eph_members": self.eph_members().len(),
            "messages_sent": self.sent.load(Ordering::Relaxed),
            "messages_received": self.received.load(Ordering::Relaxed),
            "messages_dropped": self.dropped.load(Ordering::Relaxed),
            "signature_failures": hs,
            "handshake_failures": hs,
            "sequence_rejections": 0,
        })
    }
}

// ------------------------------------------------------------------- helpers

/// Read a frame's `tenant` field. `None` means "this peer sent no tenant" — either a
/// pre-Track-B build (rolling upgrade) or a garbled/hostile value: both must degrade
/// to the caller's safe every-tenant fan-out, never to a bogus or default-tenant key.
/// Validated with the same parser the HTTP boundary uses, so a malformed value can
/// never reach a ring key.
fn frame_tenant(v: &serde_json::Value) -> Option<String> {
    v.get("tenant")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
        .and_then(crate::tenant::parse_tenant_uuid)
}

/// Encode a frame: `u32 BE (1 + payload.len()) | u8 type | payload`.
fn encode_frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
    let body_len = 1 + payload.len(); // type byte + payload
    let mut buf = Vec::with_capacity(4 + body_len);
    buf.extend_from_slice(&(body_len as u32).to_be_bytes());
    buf.push(msg_type);
    buf.extend_from_slice(payload);
    buf
}

/// Read one length-prefixed frame, returning `(type, payload)`. Rejects a zero or
/// oversized length before allocating (a malformed/hostile prefix errors instead
/// of driving an OOM); a short read (truncated frame) surfaces as an I/O error
/// from `read_exact`.
async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R) -> std::io::Result<(u8, Vec<u8>)> {
    let mut lenb = [0u8; 4];
    r.read_exact(&mut lenb).await?;
    let len = u32::from_be_bytes(lenb) as usize;
    if len == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "zero-length frame",
        ));
    }
    if len > MAX_FRAME_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame exceeds MAX_FRAME_LEN",
        ));
    }
    let mut tb = [0u8; 1];
    r.read_exact(&mut tb).await?;
    let mut payload = vec![0u8; len - 1];
    r.read_exact(&mut payload).await?;
    Ok((tb[0], payload))
}

/// Build the HELLO payload — v2: `{server_id, nonce, mac, http_addr, eph_epoch}`.
/// `mac = HMAC-SHA256(secret, nonce)` over the nonce's hex bytes; an empty secret
/// leaves `mac` empty (open mode).
///
/// THE TWO NEW FIELDS ARE ADDITIVE JSON, which is what makes the rolling deploy
/// of §8 work in both directions: an older peer reads `server_id`/`nonce`/`mac`
/// exactly where it always did and never looks at the rest, while a newer peer
/// reading an older HELLO finds them absent and marks that peer not
/// ephemeral-capable. This is the same mechanism `tenant` arrived by in Track B,
/// and it is why the capability needed no version number.
///
/// `eph_epoch` goes on the wire as HEX and not as a JSON number: it is a full
/// `u64`, JSON numbers are conventionally doubles, and the value is printed in
/// hex in the boot line and inside every message id (§3.1) — one spelling
/// everywhere is what lets an operator match an `stale` ack to a log line.
fn build_hello_payload(server_id: &str, secret: &str, http_addr: &str, eph_epoch: u64) -> Vec<u8> {
    let nonce = gen_nonce_hex();
    let mac = if secret.is_empty() {
        String::new()
    } else {
        let mut m = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key");
        m.update(nonce.as_bytes());
        hex::encode(m.finalize().into_bytes())
    };
    serde_json::to_vec(&serde_json::json!({
        "server_id": server_id,
        "nonce": nonce,
        "mac": mac,
        "http_addr": http_addr,
        "eph_epoch": format!("{eph_epoch:x}"),
    }))
    .unwrap_or_default()
}

/// Verify a HELLO payload, returning what the peer said about itself. An empty
/// secret accepts any well-formed HELLO (open mode). A configured secret requires
/// a matching HMAC over the nonce, compared in constant time.
///
/// The v2 pair is read only AFTER the MAC has been checked, and it is read as a
/// PAIR: an address without an epoch (or vice versa) is a half-written HELLO and
/// yields `None` — being in the rendezvous ring without a resolvable identity is
/// worse than being out of it.
fn verify_hello(payload: &[u8], secret: &str) -> Option<PeerHello> {
    let v: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let server_id = v.get("server_id").and_then(|x| x.as_str())?;
    if server_id.is_empty() {
        return None;
    }
    if !secret.is_empty() {
        let nonce = v.get("nonce").and_then(|x| x.as_str())?;
        let mac_hex = v.get("mac").and_then(|x| x.as_str())?;
        let mac_bytes = hex::decode(mac_hex).ok()?;
        let mut m = HmacSha256::new_from_slice(secret.as_bytes()).ok()?;
        m.update(nonce.as_bytes());
        m.verify_slice(&mac_bytes).ok()?;
    }
    let addr = v
        .get("http_addr")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty());
    let epoch = v
        .get("eph_epoch")
        .and_then(|x| x.as_str())
        .and_then(|s| u64::from_str_radix(s, 16).ok());
    Some(PeerHello {
        server_id: server_id.to_string(),
        eph: match (addr, epoch) {
            (Some(a), Some(e)) => Some((a.to_string(), e)),
            _ => None,
        },
    })
}

fn gen_nonce_hex() -> String {
    format!("{:016x}{:016x}", rand::random::<u64>(), rand::random::<u64>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_encode_layout() {
        // u32 BE length (= 1 + payload) | type | payload.
        let f = encode_frame(T_MESSAGE_AVAILABLE, b"hi");
        assert_eq!(&f[0..4], &[0, 0, 0, 3]); // len = 1 (type) + 2 (payload)
        assert_eq!(f[4], T_MESSAGE_AVAILABLE);
        assert_eq!(&f[5..], b"hi");
    }

    #[tokio::test]
    async fn frame_roundtrip() {
        let f = encode_frame(T_QUEUE_CONFIG_SET, br#"{"queue":"q"}"#);
        let mut buf: &[u8] = &f;
        let (ty, payload) = read_frame(&mut buf).await.unwrap();
        assert_eq!(ty, T_QUEUE_CONFIG_SET);
        assert_eq!(payload, br#"{"queue":"q"}"#);
        // The reader consumed exactly the frame — nothing left over.
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn frame_roundtrip_empty_payload() {
        let f = encode_frame(T_HEARTBEAT, b"");
        let mut buf: &[u8] = &f;
        let (ty, payload) = read_frame(&mut buf).await.unwrap();
        assert_eq!(ty, T_HEARTBEAT);
        assert!(payload.is_empty());
    }

    #[tokio::test]
    async fn read_frame_truncated_length_prefix() {
        // Fewer than the 4 length bytes → read_exact fails (UnexpectedEof).
        let mut buf: &[u8] = &[0u8, 0u8, 3u8];
        assert!(read_frame(&mut buf).await.is_err());
    }

    #[tokio::test]
    async fn read_frame_truncated_payload() {
        // Claims 10 body bytes but only supplies 3 → the payload read short-reads.
        let mut bytes = 10u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[T_MESSAGE_AVAILABLE, b'a', b'b']);
        let mut buf: &[u8] = &bytes;
        assert!(read_frame(&mut buf).await.is_err());
    }

    #[tokio::test]
    async fn read_frame_zero_length_rejected() {
        let mut buf: &[u8] = &0u32.to_be_bytes();
        let err = read_frame(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_frame_oversized_rejected_without_allocating() {
        // A bogus 4 GiB length must be rejected on sight, not allocated.
        let mut buf: &[u8] = &u32::MAX.to_be_bytes();
        let err = read_frame(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn handshake_good_secret_roundtrips() {
        let payload = build_hello_payload("node-A", "s3cr3t", "http://a:6632", 0xabc);
        let h = verify_hello(&payload, "s3cr3t").expect("good secret verifies");
        assert_eq!(h.server_id, "node-A");
        assert_eq!(h.eph, Some(("http://a:6632".to_string(), 0xabc)));
    }

    #[test]
    fn handshake_bad_secret_rejected() {
        let payload = build_hello_payload("node-A", "s3cr3t", "http://a:6632", 1);
        assert!(verify_hello(&payload, "wrong").is_none());
    }

    #[test]
    fn handshake_open_mode_accepts_any() {
        // Empty secret on both sides = open mode: HELLO is exchanged (to learn the
        // server_id) but not verified.
        let payload = build_hello_payload("node-B", "", "http://b:6632", 2);
        assert_eq!(verify_hello(&payload, "").unwrap().server_id, "node-B");
        // A secured node still learns the id of an authenticated peer even if it
        // itself runs open.
        let signed = build_hello_payload("node-C", "k", "http://c:6632", 3);
        assert_eq!(verify_hello(&signed, "").unwrap().server_id, "node-C");
    }

    #[test]
    fn handshake_secretless_peer_rejected_by_secured_listener() {
        // A peer that sends no MAC cannot talk to a listener that requires one.
        let payload = build_hello_payload("node-D", "", "http://d:6632", 4);
        assert!(verify_hello(&payload, "required-secret").is_none());
    }

    #[test]
    fn handshake_missing_server_id_rejected() {
        let payload = serde_json::to_vec(&serde_json::json!({ "nonce": "aa", "mac": "" })).unwrap();
        assert!(verify_hello(&payload, "").is_none());
    }

    // ---- end-to-end over loopback TCP --------------------------------------
    // Two live MeshTransport instances on ephemeral localhost ports exercise the
    // real dial → handshake → framed send → dispatch path (no Postgres involved).

    use tokio::sync::mpsc as tmpsc;

    // Forwarded as (tenant, queue, partition); `tenant` is None when the frame
    // carried none (an older peer), which the receiver must not conflate with the
    // default tenant.
    type Fwd = (Option<String>, String, String);

    fn forwarding_handlers(tx: tmpsc::UnboundedSender<Fwd>) -> SyncHandlers {
        let dtx = tx.clone();
        let etx = tx.clone();
        SyncHandlers {
            on_message_available: Box::new(move |t, q, p| {
                let _ = tx.send((t.map(|s| s.to_string()), q.to_string(), p.to_string()));
            }),
            // route dirty hints to the same channel, tagged, so the e2e test can
            // assert a HOTLIST_DIRTY frame round-trips.
            on_hotlist_dirty: Box::new(move |t, q, p, g| {
                // The scope rides along in the tag, so a test can tell a group-scoped
                // hint from the queue-wide one the push path sends.
                let tag = match g {
                    Some(g) => format!("dirty:{q}/{g}"),
                    None => format!("dirty:{q}"),
                };
                let _ = dtx.send((t.map(|s| s.to_string()), tag, p.to_string()));
            }),
            on_maintenance: Box::new(|_| {}),
            on_pop_maintenance: Box::new(|_| {}),
            on_queue_config_set: Box::new(|_, _| {}),
            on_queue_config_delete: Box::new(|_, _| {}),
            // EPHEMERAL_QUEUES.md §3.5 — routed to the same channel, tagged
            // "eph:<op>", so one assertion shape covers every frame family.
            on_eph_admin: Box::new(move |op, t, q| {
                let _ = etx.send((Some(t.to_string()), format!("eph:{op}"), q.to_string()));
            }),
        }
    }

    fn mk_cfg(
        peers: Vec<(String, u16)>,
        secret: &str,
        server_id: &str,
    ) -> crate::config::SyncConfig {
        crate::config::SyncConfig {
            enabled: true,
            mesh_port: 0, // ephemeral — the real port is read back from the binding
            peers,
            secret: secret.to_string(),
            heartbeat_ms: 100,
            dead_threshold_ms: 300,
            cache_refresh_ms: 60_000,
            server_id: server_id.to_string(),
            http_addr: format!("http://{server_id}.test:6632"),
        }
    }

    async fn wait_peer0_connected(t: &Arc<MeshTransport>, connected: bool) -> bool {
        for _ in 0..300 {
            let s = t.stats();
            if s["peers"][0]["connected"].as_bool() == Some(connected) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    #[tokio::test]
    async fn mesh_delivers_single_and_batched_wakes() {
        // Node B listens (no peers of its own); node A dials B and sends.
        let (btx, mut brx) = tmpsc::unbounded_channel();
        let (tb, bind_b) = MeshTransport::bind(&mk_cfg(vec![], "", "B"), forwarding_handlers(btx), 0xb0)
            .await
            .unwrap();
        let pb = bind_b.local_addr().unwrap().port();
        tb.start(bind_b);

        let (atx, _arx) = tmpsc::unbounded_channel();
        let (ta, bind_a) = MeshTransport::bind(
            &mk_cfg(vec![("127.0.0.1".into(), pb)], "", "A"),
            forwarding_handlers(atx),
            0xa0,
        )
        .await
        .unwrap();
        ta.start(bind_a);

        assert!(wait_peer0_connected(&ta, true).await, "A never connected to B");

        // Single-item MESSAGE_AVAILABLE. The send takes a COMPOSITE qkey and splits
        // it onto the wire, so the peer receives the tenant alongside the queue.
        let ta_uuid = "11111111-1111-1111-1111-111111111111";
        ta.send_message_available(&crate::handlers::tenant_queue_key(ta_uuid, "q1"), "p1");
        let got = tokio::time::timeout(Duration::from_secs(2), brx.recv())
            .await
            .expect("no wake within 2s")
            .unwrap();
        assert_eq!(got, (Some(ta_uuid.to_string()), "q1".to_string(), "p1".to_string()));

        // Batched form — one frame carrying two partitions.
        let k2 = crate::handlers::tenant_queue_key(ta_uuid, "q2");
        ta.send_messages_available_batch(&[
            (k2.clone(), "pa".into()),
            (k2.clone(), "pb".into()),
        ]);
        let g1 = tokio::time::timeout(Duration::from_secs(2), brx.recv())
            .await
            .unwrap()
            .unwrap();
        let g2 = tokio::time::timeout(Duration::from_secs(2), brx.recv())
            .await
            .unwrap()
            .unwrap();
        let mut both = [g1, g2];
        both.sort();
        assert_eq!(
            both,
            [
                (Some(ta_uuid.to_string()), "q2".to_string(), "pa".to_string()),
                (Some(ta_uuid.to_string()), "q2".to_string(), "pb".to_string())
            ]
        );

        // 19-wildcard-hotlist §5: batched HOTLIST_DIRTY round-trips to a distinct
        // handler (tagged "dirty:" by forwarding_handlers), tenant included.
        let k3 = crate::handlers::tenant_queue_key(ta_uuid, "q3");
        ta.send_hotlist_dirty_batch(&[crate::hotlist::DirtyHint::new(&k3, "p3", None)]);
        let gd = tokio::time::timeout(Duration::from_secs(2), brx.recv())
            .await
            .expect("no dirty within 2s")
            .unwrap();
        assert_eq!(
            gd,
            (Some(ta_uuid.to_string()), "dirty:q3".to_string(), "p3".to_string())
        );

        // …and a GROUP-scoped hint carries its scope over the same frame, so the peer
        // marks one ring instead of every ring of the queue.
        ta.send_hotlist_dirty_batch(&[crate::hotlist::DirtyHint::new(&k3, "p4", Some("workers"))]);
        let gg = tokio::time::timeout(Duration::from_secs(2), brx.recv())
            .await
            .expect("no grouped dirty within 2s")
            .unwrap();
        assert_eq!(
            gg,
            (Some(ta_uuid.to_string()), "dirty:q3/workers".to_string(), "p4".to_string())
        );

        // Received-frame counter advanced on B (single + batch + 2 dirty = 4 frames).
        assert!(tb.stats()["messages_received"].as_u64().unwrap() >= 4);
    }

    // Rolling upgrade, old → new: a frame with no `tenant` field must surface as
    // None (never the default tenant), so the receiver takes the every-tenant
    // fan-out. Dispatched directly — `dispatch` is private to this module.
    #[tokio::test]
    async fn a_tenantless_frame_is_never_attributed_to_a_tenant() {
        let (tx, mut rx) = tmpsc::unbounded_channel();
        let (t, _bind) = MeshTransport::bind(&mk_cfg(vec![], "", "solo"), forwarding_handlers(tx), 0x50)
            .await
            .unwrap();
        t.dispatch(
            T_HOTLIST_DIRTY_BATCH,
            br#"{"items":[{"queue":"orders","partition":"p0"}]}"#,
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            (None, "dirty:orders".to_string(), "p0".to_string())
        );
        // A garbled tenant degrades the same way — never to a bogus ring key.
        t.dispatch(
            T_MESSAGE_AVAILABLE,
            br#"{"queue":"orders","partition":"p1","tenant":"not-a-uuid"}"#,
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            (None, "orders".to_string(), "p1".to_string())
        );
    }

    // Rolling upgrade, new → old: the added `tenant` field is additive JSON, so an
    // old reader still finds `queue`/`partition` exactly where it looked before.
    #[test]
    fn a_new_frame_still_parses_as_the_old_shape() {
        let payload = serde_json::json!({
            "queue": "orders",
            "partition": "p0",
            "tenant": "11111111-1111-1111-1111-111111111111",
        });
        let v: serde_json::Value = serde_json::from_slice(
            &serde_json::to_vec(&payload).unwrap(),
        )
        .unwrap();
        assert_eq!(v.get("queue").and_then(|x| x.as_str()), Some("orders"));
        assert_eq!(v.get("partition").and_then(|x| x.as_str()), Some("p0"));
    }

    #[tokio::test]
    async fn mesh_rejects_wrong_secret() {
        // B requires a secret; A dials with the wrong one.
        let (btx, mut brx) = tmpsc::unbounded_channel();
        let (tb, bind_b) =
            MeshTransport::bind(&mk_cfg(vec![], "right", "B"), forwarding_handlers(btx), 0xb1)
                .await
                .unwrap();
        let pb = bind_b.local_addr().unwrap().port();
        tb.start(bind_b);

        let (atx, _arx) = tmpsc::unbounded_channel();
        let (ta, bind_a) = MeshTransport::bind(
            &mk_cfg(vec![("127.0.0.1".into(), pb)], "wrong", "A"),
            forwarding_handlers(atx),
            0xa1,
        )
        .await
        .unwrap();
        ta.start(bind_a);

        // A keeps re-dialing and getting rejected; B counts handshake failures and
        // never dispatches a wake.
        let mut saw_failure = false;
        for _ in 0..100 {
            ta.send_message_available("q", "p");
            if tb.stats()["handshake_failures"].as_u64().unwrap() >= 1 {
                saw_failure = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(saw_failure, "B never recorded a handshake failure");
        assert!(
            tokio::time::timeout(Duration::from_millis(200), brx.recv())
                .await
                .is_err(),
            "a wake leaked through a failed handshake"
        );
    }

    #[tokio::test]
    async fn mesh_reconnects_when_peer_appears() {
        // A is configured to dial a port that nothing listens on yet: its reconnect
        // loop must keep retrying and establish the link once the peer comes up,
        // with no operator action. (This is the same loop that recovers after a peer
        // restart — an in-process transport can't be torn down mid-test because its
        // spawned tasks each hold an Arc, so we drive the loop from the dead-port
        // side instead.)
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pb = probe.local_addr().unwrap().port();
        drop(probe); // free the port so nothing is listening initially

        let (atx, _arx) = tmpsc::unbounded_channel();
        let (ta, bind_a) = MeshTransport::bind(
            &mk_cfg(vec![("127.0.0.1".into(), pb)], "", "A"),
            forwarding_handlers(atx),
            0xa0,
        )
        .await
        .unwrap();
        ta.start(bind_a);

        // Nothing to connect to yet.
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(
            ta.stats()["peers"][0]["connected"].as_bool(),
            Some(false),
            "A reported connected with no listener present"
        );

        // Bring the peer up on that exact port (retry briefly in case the OS is
        // still releasing it).
        let (btx, mut brx) = tmpsc::unbounded_channel();
        let cfg_b = crate::config::SyncConfig {
            mesh_port: pb,
            ..mk_cfg(vec![], "", "B")
        };
        let (tb, bind_b) = {
            let mut attempt = None;
            for _ in 0..100 {
                match MeshTransport::bind(&cfg_b, forwarding_handlers(btx.clone()), 0xb2).await {
                    Ok(v) => {
                        attempt = Some(v);
                        break;
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
            attempt.expect("B never bound")
        };
        tb.start(bind_b);

        assert!(
            wait_peer0_connected(&ta, true).await,
            "A never reconnected once the peer appeared"
        );
        ta.send_message_available("q3", "p3");
        let got = tokio::time::timeout(Duration::from_secs(2), brx.recv())
            .await
            .expect("no wake after reconnect")
            .unwrap();
        // A bare name splits to the default tenant, so it round-trips as such.
        assert_eq!(
            got,
            (
                Some(crate::config::DEFAULT_TENANT.to_string()),
                "q3".to_string(),
                "p3".to_string()
            )
        );
    }

    #[tokio::test]
    async fn mesh_reports_dead_peer_and_keeps_serving() {
        // A raw client (fully controllable, unlike an in-process transport) sends a
        // HELLO + one frame, then goes silent and disconnects. After dead_threshold
        // the listener must report it dead — and still serve a fresh connection.
        let (btx, mut brx) = tmpsc::unbounded_channel();
        let (tb, bind_b) = MeshTransport::bind(&mk_cfg(vec![], "", "B"), forwarding_handlers(btx), 0xb0)
            .await
            .unwrap();
        let pb = bind_b.local_addr().unwrap().port();
        tb.start(bind_b);

        {
            let mut s = TcpStream::connect(("127.0.0.1", pb)).await.unwrap();
            s.write_all(&encode_frame(T_HELLO, &build_hello_payload("ghost", "", "http://ghost:6632", 0x9)))
                .await
                .unwrap();
            s.write_all(&encode_frame(
                T_MESSAGE_AVAILABLE,
                br#"{"queue":"q","partition":"p"}"#,
            ))
            .await
            .unwrap();
            // s dropped here → connection closed, no further frames or heartbeats.
        }
        let got = tokio::time::timeout(Duration::from_secs(2), brx.recv())
            .await
            .unwrap()
            .unwrap();
        // A hand-rolled frame with no `tenant` field is the pre-Track-B wire shape.
        assert_eq!(got, (None, "q".to_string(), "p".to_string()));

        // Past dead_threshold (300ms in the test cfg) the ghost is reported dead.
        let mut dead = false;
        for _ in 0..100 {
            if tb.stats()["servers_dead"].as_u64().unwrap() >= 1 {
                dead = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(dead, "silent peer was never reported dead");

        // The listener keeps serving: a fresh connection is dispatched normally.
        let mut s2 = TcpStream::connect(("127.0.0.1", pb)).await.unwrap();
        s2.write_all(&encode_frame(T_HELLO, &build_hello_payload("live", "", "http://live:6632", 0xa)))
            .await
            .unwrap();
        s2.write_all(&encode_frame(
            T_MESSAGE_AVAILABLE,
            br#"{"queue":"q2","partition":"p2"}"#,
        ))
        .await
        .unwrap();
        let got2 = tokio::time::timeout(Duration::from_secs(2), brx.recv())
            .await
            .expect("listener stopped serving after a peer died")
            .unwrap();
        assert_eq!(got2, (None, "q2".to_string(), "p2".to_string()));
    }

    // =======================================================================
    // EPHEMERAL_QUEUES.md §3.5 — HELLO v2, T_EPH_ADMIN, unknown-tag tolerance
    // =======================================================================

    /// "Old peers lacking the fields are tolerated and marked not
    /// ephemeral-capable" (§3.5/§8). The v1 HELLO shape is written by hand here
    /// rather than by an older `build_hello_payload`, because the point of the
    /// test is that THIS parser accepts the bytes a previous release emitted.
    #[test]
    fn hello_v1_from_an_old_peer_parses_but_is_not_capable() {
        let v1 = serde_json::to_vec(&serde_json::json!({
            "server_id": "old-node",
            "nonce": "aa",
            "mac": "",
        }))
        .unwrap();
        let h = verify_hello(&v1, "").expect("a v1 HELLO must still handshake");
        assert_eq!(h.server_id, "old-node");
        assert!(h.eph.is_none(), "a peer with no v2 fields is not capable");
    }

    /// Half a v2 HELLO is not a v2 HELLO: an address with no epoch (or an epoch
    /// with no address) leaves the peer OUT of the ring rather than in it with a
    /// hole, because a member the ring cannot forward to or fence against is
    /// worse than one member fewer.
    #[test]
    fn a_half_written_hello_v2_is_not_capable() {
        for payload in [
            serde_json::json!({"server_id":"n","nonce":"aa","mac":"","http_addr":"http://n:1"}),
            serde_json::json!({"server_id":"n","nonce":"aa","mac":"","eph_epoch":"ff"}),
            // A non-hex epoch is a garbled field, not a zero one.
            serde_json::json!({
                "server_id":"n","nonce":"aa","mac":"",
                "http_addr":"http://n:1","eph_epoch":"zz"
            }),
        ] {
            let bytes = serde_json::to_vec(&payload).unwrap();
            assert!(verify_hello(&bytes, "").unwrap().eph.is_none());
        }
    }

    /// The v2 fields are ADDITIVE: an old reader still finds every field it
    /// knows, in the same place (the §8 rolling-deploy direction new → old).
    #[test]
    fn hello_v2_still_parses_as_the_v1_shape() {
        let payload = build_hello_payload("node-A", "", "http://a:6632", 0x1234);
        let v: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(v.get("server_id").and_then(|x| x.as_str()), Some("node-A"));
        assert!(v.get("nonce").and_then(|x| x.as_str()).is_some());
        assert!(v.get("mac").and_then(|x| x.as_str()).is_some());
        // The epoch is hex TEXT, not a JSON number — a full u64 must survive a
        // round trip that a double would round off.
        assert_eq!(v.get("eph_epoch").and_then(|x| x.as_str()), Some("1234"));
        let big = build_hello_payload("n", "", "http://n:1", u64::MAX);
        assert_eq!(verify_hello(&big, "").unwrap().eph.unwrap().1, u64::MAX);
    }

    /// §11.2 — "unknown frame types on old peers must be ignored, not fatal".
    /// The connection has to SURVIVE one: a peer one version ahead sends a tag
    /// this build has no name for, and closing on it would take every wake
    /// sharing that socket down with it.
    #[tokio::test]
    async fn an_unknown_frame_type_is_skipped_and_the_connection_survives() {
        let (btx, mut brx) = tmpsc::unbounded_channel();
        let (tb, bind_b) =
            MeshTransport::bind(&mk_cfg(vec![], "", "B"), forwarding_handlers(btx), 0xb3)
                .await
                .unwrap();
        let pb = bind_b.local_addr().unwrap().port();
        tb.start(bind_b);

        let mut s = TcpStream::connect(("127.0.0.1", pb)).await.unwrap();
        s.write_all(&encode_frame(
            T_HELLO,
            &build_hello_payload("future", "", "http://future:6632", 0xf),
        ))
        .await
        .unwrap();
        // A tag from the future, with a payload this build cannot even parse.
        s.write_all(&encode_frame(213, b"\x00\x01\x02 not json at all"))
            .await
            .unwrap();
        // …followed by a frame it does know. Receiving THIS is the assertion:
        // the reader stepped over the unknown frame using its length prefix and
        // stayed on the connection.
        s.write_all(&encode_frame(
            T_MESSAGE_AVAILABLE,
            br#"{"queue":"after","partition":"p"}"#,
        ))
        .await
        .unwrap();

        let got = tokio::time::timeout(Duration::from_secs(2), brx.recv())
            .await
            .expect("the connection died on an unknown frame type")
            .unwrap();
        assert_eq!(got, (None, "after".to_string(), "p".to_string()));
    }

    /// §3.5 end to end: HELLO v2 puts a peer in `eph_members()` with the address
    /// and epoch it advertised, and a `T_EPH_ADMIN` frame round-trips its three
    /// fields.
    #[tokio::test]
    async fn hello_v2_populates_members_and_eph_admin_round_trips() {
        let (btx, mut brx) = tmpsc::unbounded_channel();
        let (tb, bind_b) =
            MeshTransport::bind(&mk_cfg(vec![], "", "B"), forwarding_handlers(btx), 0xb4)
                .await
                .unwrap();
        let pb = bind_b.local_addr().unwrap().port();
        tb.start(bind_b);

        let (atx, _arx) = tmpsc::unbounded_channel();
        let (ta, bind_a) = MeshTransport::bind(
            &mk_cfg(vec![("127.0.0.1".into(), pb)], "", "A"),
            forwarding_handlers(atx),
            0xdead_beef_cafe,
        )
        .await
        .unwrap();
        ta.start(bind_a);
        assert!(wait_peer0_connected(&ta, true).await, "A never connected to B");

        // B learned A from the HELLO — address and epoch, not just the id.
        let mut members = Vec::new();
        for _ in 0..200 {
            members = tb.eph_members();
            if !members.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            members,
            vec![EphMember {
                server_id: "A".to_string(),
                http_addr: "http://A.test:6632".to_string(),
                epoch: 0xdead_beef_cafe,
            }]
        );
        // A itself is NEVER in its own list — §3.7's caller adds self.
        assert!(ta.eph_members().is_empty());
        assert_eq!(tb.stats()["eph_members"].as_u64(), Some(1));

        let uuid = "11111111-1111-1111-1111-111111111111";
        ta.send_eph_admin("reset", uuid, "inbox");
        let got = tokio::time::timeout(Duration::from_secs(2), brx.recv())
            .await
            .expect("no eph admin frame within 2s")
            .unwrap();
        assert_eq!(
            got,
            (Some(uuid.to_string()), "eph:reset".to_string(), "inbox".to_string())
        );
    }

    /// A destructive frame that cannot name its tenant is DROPPED, where a wake
    /// in the same shape fans out (§3.5). Dispatched directly, since the point is
    /// the parse and not the transport.
    #[tokio::test]
    async fn an_eph_admin_without_a_valid_tenant_is_dropped() {
        let (tx, mut rx) = tmpsc::unbounded_channel();
        let (t, _bind) =
            MeshTransport::bind(&mk_cfg(vec![], "", "solo"), forwarding_handlers(tx), 0x51)
                .await
                .unwrap();
        // No tenant at all (a pre-Track-B shape), a garbled one, and a missing
        // op or queue: all four are silently skipped.
        t.dispatch(T_EPH_ADMIN, br#"{"op":"delete","queue":"inbox"}"#);
        t.dispatch(T_EPH_ADMIN, br#"{"op":"delete","queue":"inbox","tenant":"nope"}"#);
        t.dispatch(
            T_EPH_ADMIN,
            br#"{"queue":"inbox","tenant":"11111111-1111-1111-1111-111111111111"}"#,
        );
        t.dispatch(
            T_EPH_ADMIN,
            br#"{"op":"delete","tenant":"11111111-1111-1111-1111-111111111111"}"#,
        );
        assert!(rx.try_recv().is_err(), "a destructive frame leaked through");
        // …and a well-formed one on the same transport still lands, so the test
        // is not passing because dispatch is broken outright.
        t.dispatch(
            T_EPH_ADMIN,
            br#"{"op":"config_set","queue":"inbox","tenant":"11111111-1111-1111-1111-111111111111"}"#,
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            (
                Some("11111111-1111-1111-1111-111111111111".to_string()),
                "eph:config_set".to_string(),
                "inbox".to_string()
            )
        );
    }

    /// A peer that goes silent leaves the rendezvous ring within
    /// `dead_threshold` — the membership change §3.7 turns into an ownership
    /// move. Capability alone is not membership.
    #[tokio::test]
    async fn a_silent_peer_leaves_eph_members() {
        let (btx, _brx) = tmpsc::unbounded_channel();
        let (tb, bind_b) =
            MeshTransport::bind(&mk_cfg(vec![], "", "B"), forwarding_handlers(btx), 0xb5)
                .await
                .unwrap();
        let pb = bind_b.local_addr().unwrap().port();
        tb.start(bind_b);

        {
            let mut s = TcpStream::connect(("127.0.0.1", pb)).await.unwrap();
            s.write_all(&encode_frame(
                T_HELLO,
                &build_hello_payload("ghost", "", "http://ghost:6632", 7),
            ))
            .await
            .unwrap();
            // dropped → silent from here on
        }
        let mut saw = false;
        for _ in 0..200 {
            if tb.eph_members().len() == 1 {
                saw = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(saw, "a capable peer never entered the ring");

        // dead_threshold is 300ms in the test config.
        let mut gone = false;
        for _ in 0..200 {
            if tb.eph_members().is_empty() {
                gone = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(gone, "a dead peer stayed in the rendezvous ring");
    }
}
