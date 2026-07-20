//! UDP inter-instance sync transport (segments Rust broker).
//!
//! A direct port of the C++ `UDPSyncTransport` + the transport half of
//! `SharedStateManager` (server/src/services/udp_sync_transport.cpp,
//! shared_state_manager.cpp). Queen is a multi-replica broker: peers coordinate
//! over a raw `SOCK_DGRAM` transport so that
//!
//!   1. a PUSH on replica A wakes a parked long-poll POP on replica B immediately
//!      (MESSAGE_AVAILABLE) instead of waiting out its poll interval,
//!   2. maintenance-mode flips propagate to every replica,
//!   3. queue-config changes invalidate peers' caches.
//!
//! ## Wire format
//! Byte-identical framing to the C++ impl — a fixed 92-byte header followed by the
//! payload:
//! ```text
//!   [0]      version (1)
//!   [1]      type    (1)
//!   [2..4]   payload length      (u16, big-endian)
//!   [4..36]  HMAC-SHA256 signature (32)
//!   [36..68] sender_id  (32, NUL-padded)
//!   [68..84] session_id (16, NUL-padded — random per process start)
//!   [84..92] sequence   (u64, big-endian, monotonic per session)
//!   [92..]   payload
//! ```
//! The one intentional divergence from C++ is the payload codec: this Rust-only
//! cluster encodes the payload as **JSON** (the C++ build used MessagePack). The
//! broker is a full replacement for the C++ server, so mixed C++/Rust clusters are
//! out of scope; keeping JSON avoids pulling in a msgpack dependency and the
//! payloads are tiny either way. The header, HMAC scheme, sequence/session replay
//! protection, heartbeats and DNS re-resolution are all faithful ports.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::net::UdpSocket;

type HmacSha256 = Hmac<Sha256>;

const HEADER_SIZE: usize = 92;
const MAX_PAYLOAD: usize = 1308;
const MAX_MESSAGE: usize = HEADER_SIZE + MAX_PAYLOAD;
const VERSION: u8 = 1;

// Message type tags — same numeric values as the C++ `UDPSyncMessageType` so the
// framing stays wire-compatible.
pub const T_MESSAGE_AVAILABLE: u8 = 1;
pub const T_HEARTBEAT: u8 = 3;
pub const T_QUEUE_CONFIG_SET: u8 = 10;
pub const T_QUEUE_CONFIG_DELETE: u8 = 11;
pub const T_MAINTENANCE_MODE_SET: u8 = 40;
pub const T_POP_MAINTENANCE_MODE_SET: u8 = 41;

/// Effects a received packet applies to local state. main.rs supplies these
/// closures (capturing the shared `AppState`/`Notifier`) so this module stays
/// decoupled from the handler layer. All are invoked from the receive task, so
/// they must be cheap and non-blocking (they only touch atomics / in-memory maps).
pub struct SyncHandlers {
    pub on_message_available: Box<dyn Fn(&str, &str) + Send + Sync>,
    pub on_maintenance: Box<dyn Fn(bool) + Send + Sync>,
    pub on_pop_maintenance: Box<dyn Fn(bool) + Send + Sync>,
    pub on_queue_config_set: Box<dyn Fn(&str) + Send + Sync>,
    pub on_queue_config_delete: Box<dyn Fn(&str) + Send + Sync>,
}

struct Peer {
    host: String,
    port: u16,
    addr: Option<SocketAddr>,
}

struct SenderState {
    session_id: String,
    max_seq: u64,
    last_seen: Instant,
}

pub struct UdpTransport {
    server_id: String,
    session_id: String,
    udp_port: u16,
    secret: String,
    dead_threshold: Duration,

    socket: Arc<UdpSocket>,
    peers: Mutex<Vec<Peer>>,
    senders: Mutex<HashMap<String, SenderState>>,
    handlers: SyncHandlers,

    seq: AtomicU64,
    sent: AtomicU64,
    received: AtomicU64,
    dropped: AtomicU64,
    sig_failures: AtomicU64,
    seq_rejections: AtomicU64,
}

impl UdpTransport {
    /// Bind the UDP socket and resolve the initial peer set. Does not spawn tasks —
    /// call [`start`](Self::start) after wiring is complete.
    pub async fn bind(
        cfg: &crate::config::SyncConfig,
        handlers: SyncHandlers,
    ) -> std::io::Result<Arc<UdpTransport>> {
        let bind_addr = format!("0.0.0.0:{}", cfg.udp_port);
        let socket = UdpSocket::bind(&bind_addr).await?;
        let peers: Vec<Peer> = cfg
            .peers
            .iter()
            .map(|(h, p)| Peer {
                host: h.clone(),
                port: *p,
                addr: None,
            })
            .collect();

        let t = Arc::new(UdpTransport {
            server_id: cfg.server_id.clone(),
            session_id: gen_session_id(),
            udp_port: cfg.udp_port,
            secret: cfg.secret.clone(),
            dead_threshold: Duration::from_millis(cfg.dead_threshold_ms),
            socket: Arc::new(socket),
            peers: Mutex::new(peers),
            senders: Mutex::new(HashMap::new()),
            handlers,
            seq: AtomicU64::new(0),
            sent: AtomicU64::new(0),
            received: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            sig_failures: AtomicU64::new(0),
            seq_rejections: AtomicU64::new(0),
        });
        t.resolve_peers().await;
        Ok(t)
    }

    /// Spawn the receive, heartbeat, and DNS-refresh tasks.
    pub fn start(self: &Arc<Self>, cfg: &crate::config::SyncConfig) {
        let recv = self.clone();
        tokio::spawn(async move { recv.recv_loop().await });

        let hb = self.clone();
        let hb_ms = cfg.heartbeat_ms;
        tokio::spawn(async move { hb.heartbeat_loop(hb_ms).await });

        let dns = self.clone();
        tokio::spawn(async move { dns.dns_refresh_loop().await });

        println!(
            "queen-seg-rust: UDP sync started on :{} server_id={} peers={} secret={}",
            self.udp_port,
            self.server_id,
            self.peers.lock().unwrap().len(),
            if self.secret.is_empty() { "off" } else { "hmac" }
        );
    }

    // ----------------------------------------------------------------- send side

    fn build(&self, msg_type: u8, payload: &[u8]) -> Vec<u8> {
        debug_assert!(payload.len() <= MAX_PAYLOAD);
        let mut buf = vec![0u8; HEADER_SIZE + payload.len()];
        buf[0] = VERSION;
        buf[1] = msg_type;
        let len = payload.len() as u16;
        buf[2] = (len >> 8) as u8;
        buf[3] = (len & 0xff) as u8;
        // buf[4..36] signature — filled below.
        write_fixed(&mut buf[36..68], self.server_id.as_bytes());
        write_fixed(&mut buf[68..84], self.session_id.as_bytes());
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        buf[84..92].copy_from_slice(&seq.to_be_bytes());
        buf[HEADER_SIZE..].copy_from_slice(payload);
        if !self.secret.is_empty() {
            let sig = sign(&buf, self.secret.as_bytes());
            buf[4..36].copy_from_slice(&sig);
        }
        buf
    }

    async fn broadcast(&self, msg_type: u8, payload: Vec<u8>) {
        if payload.len() > MAX_PAYLOAD {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let data = self.build(msg_type, &payload);
        // Snapshot resolved peer addresses under the lock, then send outside it.
        let targets: Vec<SocketAddr> = {
            let peers = self.peers.lock().unwrap();
            peers.iter().filter_map(|p| p.addr).collect()
        };
        for addr in targets {
            match self.socket.send_to(&data, addr).await {
                Ok(_) => {
                    self.sent.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => { /* transient — UDP is best-effort, the poll interval covers loss */ }
            }
        }
    }

    /// Fire-and-forget helper: build the payload, spawn the send. Used by the
    /// synchronous handler call-sites so they never block on the network.
    fn spawn_broadcast(self: &Arc<Self>, msg_type: u8, payload: serde_json::Value) {
        let body = serde_json::to_vec(&payload).unwrap_or_default();
        let me = self.clone();
        tokio::spawn(async move { me.broadcast(msg_type, body).await });
    }

    pub fn send_message_available(self: &Arc<Self>, queue: &str, partition: &str) {
        self.spawn_broadcast(
            T_MESSAGE_AVAILABLE,
            serde_json::json!({ "queue": queue, "partition": partition }),
        );
    }

    pub fn send_maintenance(self: &Arc<Self>, enabled: bool) {
        self.spawn_broadcast(T_MAINTENANCE_MODE_SET, serde_json::json!({ "enabled": enabled }));
    }

    pub fn send_pop_maintenance(self: &Arc<Self>, enabled: bool) {
        self.spawn_broadcast(
            T_POP_MAINTENANCE_MODE_SET,
            serde_json::json!({ "enabled": enabled }),
        );
    }

    pub fn send_queue_config_set(self: &Arc<Self>, queue: &str) {
        self.spawn_broadcast(T_QUEUE_CONFIG_SET, serde_json::json!({ "queue": queue }));
    }

    pub fn send_queue_config_delete(self: &Arc<Self>, queue: &str) {
        self.spawn_broadcast(T_QUEUE_CONFIG_DELETE, serde_json::json!({ "queue": queue }));
    }

    // -------------------------------------------------------------- receive side

    async fn recv_loop(self: Arc<Self>) {
        let mut buf = vec![0u8; MAX_MESSAGE];
        loop {
            let (n, _from) = match self.socket.recv_from(&mut buf).await {
                Ok(v) => v,
                Err(_) => {
                    // A transient recv error should not kill the loop.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            if n < HEADER_SIZE {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let pkt = &buf[..n];
            if pkt[0] != VERSION {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let payload_len = ((pkt[2] as usize) << 8) | pkt[3] as usize;
            if HEADER_SIZE + payload_len > n {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            // HMAC (constant-time) when a secret is configured; empty secret = open.
            if !self.secret.is_empty() && !verify(pkt, self.secret.as_bytes()) {
                self.sig_failures.fetch_add(1, Ordering::Relaxed);
                self.dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let sender = read_fixed(&pkt[36..68]);
            let session = read_fixed(&pkt[68..84]);
            let seq = u64::from_be_bytes(pkt[84..92].try_into().unwrap());
            if !self.accept_sequence(&sender, &session, seq) {
                self.seq_rejections.fetch_add(1, Ordering::Relaxed);
                self.dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            self.received.fetch_add(1, Ordering::Relaxed);
            let msg_type = pkt[1];
            let payload = &pkt[HEADER_SIZE..HEADER_SIZE + payload_len];
            self.dispatch(msg_type, payload);
        }
    }

    /// Replay/restart protection: reject packets whose sequence is not strictly
    /// increasing within a session; a new session id resets tracking. Also records
    /// liveness for dead-peer detection.
    fn accept_sequence(&self, sender: &str, session: &str, seq: u64) -> bool {
        let mut map = self.senders.lock().unwrap();
        match map.get_mut(sender) {
            None => {
                map.insert(
                    sender.to_string(),
                    SenderState {
                        session_id: session.to_string(),
                        max_seq: seq,
                        last_seen: Instant::now(),
                    },
                );
                true
            }
            Some(st) => {
                st.last_seen = Instant::now();
                if st.session_id != session {
                    // Peer restarted — reset sequence tracking.
                    st.session_id = session.to_string();
                    st.max_seq = seq;
                    return true;
                }
                if seq <= st.max_seq {
                    return false;
                }
                st.max_seq = seq;
                true
            }
        }
    }

    fn dispatch(&self, msg_type: u8, payload: &[u8]) {
        let v: serde_json::Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return,
        };
        match msg_type {
            T_MESSAGE_AVAILABLE => {
                let q = v.get("queue").and_then(|x| x.as_str()).unwrap_or("");
                if q.is_empty() {
                    return;
                }
                let p = v.get("partition").and_then(|x| x.as_str()).unwrap_or("");
                (self.handlers.on_message_available)(q, p);
            }
            T_HEARTBEAT => { /* liveness already recorded in accept_sequence */ }
            T_MAINTENANCE_MODE_SET => {
                let e = v.get("enabled").and_then(|x| x.as_bool()).unwrap_or(false);
                (self.handlers.on_maintenance)(e);
            }
            T_POP_MAINTENANCE_MODE_SET => {
                let e = v.get("enabled").and_then(|x| x.as_bool()).unwrap_or(false);
                (self.handlers.on_pop_maintenance)(e);
            }
            T_QUEUE_CONFIG_SET => {
                if let Some(q) = v.get("queue").and_then(|x| x.as_str()) {
                    (self.handlers.on_queue_config_set)(q);
                }
            }
            T_QUEUE_CONFIG_DELETE => {
                if let Some(q) = v.get("queue").and_then(|x| x.as_str()) {
                    (self.handlers.on_queue_config_delete)(q);
                }
            }
            _ => {}
        }
    }

    // -------------------------------------------------------------- background

    async fn heartbeat_loop(self: Arc<Self>, interval_ms: u64) {
        let mut tick = tokio::time::interval(Duration::from_millis(interval_ms));
        loop {
            tick.tick().await;
            let payload = serde_json::json!({
                "session_id": self.session_id,
                "sequence": self.sent.load(Ordering::Relaxed),
            });
            let body = serde_json::to_vec(&payload).unwrap_or_default();
            self.broadcast(T_HEARTBEAT, body).await;
        }
    }

    async fn dns_refresh_loop(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        tick.tick().await; // consume the immediate first tick (initial resolve done in bind)
        loop {
            tick.tick().await;
            let changed = self.resolve_peers().await;
            if changed > 0 {
                println!("queen-seg-rust: UDP sync DNS refresh — {changed} peer IP change(s)");
            }
        }
    }

    /// (Re)resolve every peer hostname. Returns the number of peers whose resolved
    /// address changed. Resolution happens off-lock; only the write-back is locked.
    async fn resolve_peers(&self) -> usize {
        let targets: Vec<(usize, String, u16)> = {
            let peers = self.peers.lock().unwrap();
            peers
                .iter()
                .enumerate()
                .map(|(i, p)| (i, p.host.clone(), p.port))
                .collect()
        };
        let mut resolved: Vec<(usize, Option<SocketAddr>)> = Vec::with_capacity(targets.len());
        for (i, host, port) in targets {
            // The transport socket is bound IPv4 (`0.0.0.0`, matching the C++
            // `AF_INET`/`INADDR_ANY`), so we must pick an IPv4 peer address —
            // `localhost` commonly resolves to `::1` first, which an IPv4 socket
            // cannot reach (sends fail silently). Prefer the first IPv4 result,
            // falling back to whatever resolved.
            let addr = match tokio::net::lookup_host((host.as_str(), port)).await {
                Ok(addrs) => {
                    let all: Vec<SocketAddr> = addrs.collect();
                    all.iter().find(|a| a.is_ipv4()).copied().or_else(|| all.first().copied())
                }
                Err(_) => None,
            };
            resolved.push((i, addr));
        }
        let mut changed = 0;
        let mut peers = self.peers.lock().unwrap();
        for (i, addr) in resolved {
            if let Some(p) = peers.get_mut(i) {
                if addr.is_some() && p.addr != addr {
                    p.addr = addr;
                    changed += 1;
                } else if addr.is_some() && p.addr.is_none() {
                    p.addr = addr;
                }
            }
        }
        changed
    }

    // ------------------------------------------------------------------- stats

    pub fn stats(&self) -> serde_json::Value {
        let peers = self.peers.lock().unwrap();
        let peers_json: Vec<serde_json::Value> = peers
            .iter()
            .map(|p| {
                serde_json::json!({
                    "host": p.host,
                    "port": p.port,
                    "resolved": p.addr.is_some(),
                    "resolved_ip": p.addr.map(|a| a.ip().to_string()),
                })
            })
            .collect();
        drop(peers);

        let now = Instant::now();
        let (alive, dead) = {
            let senders = self.senders.lock().unwrap();
            let mut alive = 0usize;
            let mut dead = 0usize;
            for st in senders.values() {
                if now.duration_since(st.last_seen) <= self.dead_threshold {
                    alive += 1;
                } else {
                    dead += 1;
                }
            }
            (alive, dead)
        };

        serde_json::json!({
            "server_id": self.server_id,
            "session_id": self.session_id,
            "port": self.udp_port,
            "peer_count": peers_json.len(),
            "peers": peers_json,
            "servers_alive": alive,
            "servers_dead": dead,
            "messages_sent": self.sent.load(Ordering::Relaxed),
            "messages_received": self.received.load(Ordering::Relaxed),
            "messages_dropped": self.dropped.load(Ordering::Relaxed),
            "signature_failures": self.sig_failures.load(Ordering::Relaxed),
            "sequence_rejections": self.seq_rejections.load(Ordering::Relaxed),
        })
    }
}

// ------------------------------------------------------------------- helpers

/// Copy `src` into a fixed-width field, NUL-padding/truncating to fit.
fn write_fixed(dst: &mut [u8], src: &[u8]) {
    let n = src.len().min(dst.len());
    dst[..n].copy_from_slice(&src[..n]);
    for b in &mut dst[n..] {
        *b = 0;
    }
}

/// Read a NUL-padded fixed-width field back into a String (lossless for ASCII ids).
fn read_fixed(field: &[u8]) -> String {
    let end = field.iter().position(|&b| b == 0).unwrap_or(field.len());
    String::from_utf8_lossy(&field[..end]).into_owned()
}

/// HMAC-SHA256 over version+type+len (0..4) ++ sender+session+seq+payload (36..),
/// i.e. everything except the signature field. Matches the C++ `compute_signature`.
fn sign(buf: &[u8], secret: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(&buf[0..4]);
    mac.update(&buf[36..]);
    let out = mac.finalize().into_bytes();
    let mut sig = [0u8; 32];
    sig.copy_from_slice(&out);
    sig
}

fn verify(buf: &[u8], secret: &[u8]) -> bool {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(&buf[0..4]);
    mac.update(&buf[36..]);
    mac.verify_slice(&buf[4..36]).is_ok()
}

fn gen_session_id() -> String {
    format!("{:016x}", rand::random::<u64>())
}
