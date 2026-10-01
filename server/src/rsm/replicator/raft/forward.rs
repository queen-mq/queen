//! Batched forwarding: a follower's prepared commands reach the leader over a
//! few long-lived streams, many commands to a write, each answered on its own
//! as it resolves (`QUEEN_RAFT_FWD_BATCH`, default on).
//!
//! The per-command path (`/raft/v1/submit`) holds one HTTP/1 connection per
//! command in flight, under `QUEEN_RAFT_FWD_INFLIGHT` (4096) slots: a follower
//! forwards at most 4096 / the leader's command latency commands a second —
//! ~15k/s at 0.27 s — while pushes of one to twenty messages, each its own
//! partition, at 1M msg/s need ~660k forwarded commands a second. It also
//! opened ~26,500 connections once, and the ephemeral ports ran out.
//!
//! # The streams
//!
//! A follower keeps `QUEEN_RAFT_FWD_STREAMS` (4) streams to the leader's Raft
//! RPC listener. Each is one HTTP/1.1 `POST /raft/v1/forward`, authenticated
//! like every Raft RPC (`QUEEN_RAFT_TOKEN`), whose chunked request body carries
//! the commands and whose chunked response body carries the answers, both for
//! as long as the stream lives:
//!
//! - request frame: `u32 len` · `u64 seq` · the command, exactly the body a
//!   `/raft/v1/submit` carries (its request id, its budget);
//! - answer frame: `u32 len` · `u64 seq` · `u8 kind` · the encoded reply
//!   (`kind` 0), or why the leader could not take it (1: the follower retries,
//!   as it retries a 503 of the per-command path);
//! - the leader opens every answer body with a hello frame (`seq` 0, `kind`
//!   2, the protocol version), so a follower knows the stream works before it
//!   sends anything on it.
//!
//! Integers are little-endian; `len` counts the bytes after itself. The
//! writer sends whatever is queued as one chunk: under load the commands that
//! arrive while a write is in flight go out together in the next one, idle it
//! adds no delay (`QUEEN_RAFT_FWD_LINGER_US`, default 0, waits for more).
//! Answers come back in whatever order they resolve, tagged by `seq`. What a
//! follower has in flight is bounded in bytes (`QUEEN_RAFT_FWD_INFLIGHT_MB`,
//! default 64), not in connections.
//!
//! # The leader's intake
//!
//! The streams arrive on the Raft RPC listener, on the `queen-raft` runtime.
//! The intake hands each stream to the runtime the facade serves clients on:
//! there the commands are decoded, admitted — each follower is a source of its
//! own in the admission round robin ([`crate::rsm::admit`]) — handed to the
//! planner, and answered as each resolves. The `queen-raft` threads only move
//! the bytes.
//!
//! # What does not change
//!
//! Every command keeps its request id, and a retry after a lost stream, a
//! deadline or a leader change carries the same id (the facade's loop), so no
//! retry plans a command twice (I6). Deadlines and the pop margin travel in
//! the command as before. A new follower meeting an older leader (the route
//! answers 404) uses the per-command path, and asks again a minute later; an
//! older follower's per-command calls are served as they always were.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot};

// The intake (the `server` feature's Raft RPC listener) is their user.
#[cfg_attr(not(feature = "server"), allow(unused_imports))]
use super::RemoteHandler;
#[cfg_attr(not(feature = "server"), allow(unused_imports))]
use crate::rsm::admit::Source;

/// The stream's route on the Raft RPC listener.
pub(crate) const ROUTE: &str = "/raft/v1/forward";
const CONTENT_TYPE: &str = "application/x-queen-fwd";
/// The follower's node id: its source in the leader's admission round robin.
const FROM_HEADER: &str = "x-queen-fwd-from";
const VERSION: u8 = 1;

const KIND_ANSWER: u8 = 0;
const KIND_FAILED: u8 = 1;
const KIND_HELLO: u8 = 2;

/// A stream with nothing in flight for this long is closed (a middlebox may
/// drop an idle connection without a word); the next command opens another.
const IDLE: Duration = Duration::from_secs(30);

/// How long a stream's connection may take (as the per-command client's). Its
/// answer's head and hello frame may take whatever the command that opens it
/// has left: a leader too busy to answer at once must not look unreachable.
const CONNECT_WITHIN: Duration = Duration::from_secs(2);

/// After a 404 on the route (an older leader), how long a follower uses the
/// per-command path before it asks again.
const LEGACY_RECHECK: Duration = Duration::from_secs(60);

/// The most answer bytes the leader puts in one chunk.
const ANSWER_CHUNK_MAX: usize = 256 * 1024;

/// Commands this node sent over its streams, commands it served on its
/// intake, streams it opened, and leaders it found without the route.
static SENT: AtomicU64 = AtomicU64::new(0);
static SERVED: AtomicU64 = AtomicU64::new(0);
static OPENED: AtomicU64 = AtomicU64::new(0);
static LEGACY: AtomicU64 = AtomicU64::new(0);

/// Prometheus lines for batched forwarding.
pub(crate) fn render(out: &mut String) {
    use std::fmt::Write;
    let _ = writeln!(
        out,
        "# HELP queen_raft_forward_total Batched forwarding: commands this node sent to the leader, commands it served as the leader, streams it opened, and leaders it found without batched forwarding\n# TYPE queen_raft_forward_total counter"
    );
    for (kind, v) in [
        ("sent", &SENT),
        ("served", &SERVED),
        ("streams_opened", &OPENED),
        ("legacy_leader", &LEGACY),
    ] {
        let _ = writeln!(
            out,
            "queen_raft_forward_total{{kind=\"{kind}\"}} {}",
            v.load(Ordering::Relaxed)
        );
    }
}

/// Commands sent over streams so far (a test's check that the batched path
/// carried them).
#[cfg(test)]
pub(crate) fn sent_for_test() -> u64 {
    SENT.load(Ordering::Relaxed)
}

/// The largest frame either side accepts: a command can be as large as a
/// client request (`QUEEN_MAX_BODY_BYTES`, default 64 MiB), like the body cap
/// of `/raft/v1/submit`.
fn max_frame() -> usize {
    static MAX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *MAX.get_or_init(|| {
        let client = std::env::var("QUEEN_MAX_BODY_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(64 * 1024 * 1024);
        2 * client + (1 << 20)
    })
}

/// The follower side's knobs, resolved once.
#[derive(Clone, Debug)]
pub(crate) struct FwdConfig {
    /// `QUEEN_RAFT_FWD_BATCH` (default on): forward over the streams; off,
    /// one `/raft/v1/submit` call per command, as before.
    pub on: bool,
    /// `QUEEN_RAFT_FWD_STREAMS` (default 4): streams to the leader.
    pub streams: usize,
    /// `QUEEN_RAFT_FWD_INFLIGHT_MB` (default 64): command bytes in flight.
    pub inflight_bytes: u64,
    /// `QUEEN_RAFT_FWD_BATCH_KB` (default 1024): the most command bytes one
    /// write carries.
    pub batch_bytes: usize,
    /// `QUEEN_RAFT_FWD_LINGER_US` (default 0): how long a write waits for
    /// more commands before it goes (tokio's timer rounds it up to a
    /// millisecond). 0: whatever is queued goes at once.
    pub linger: Duration,
}

impl FwdConfig {
    pub(crate) fn from_env() -> FwdConfig {
        static CFG: std::sync::OnceLock<FwdConfig> = std::sync::OnceLock::new();
        CFG.get_or_init(|| {
            let num = |k: &str, d: u64| -> u64 {
                std::env::var(k)
                    .ok()
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .unwrap_or(d)
            };
            let on = match std::env::var("QUEEN_RAFT_FWD_BATCH") {
                Ok(v) => !matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "0" | "false" | "off" | "no"
                ),
                Err(_) => true,
            };
            FwdConfig {
                on,
                streams: num("QUEEN_RAFT_FWD_STREAMS", 4).clamp(1, 64) as usize,
                inflight_bytes: num("QUEEN_RAFT_FWD_INFLIGHT_MB", 64).max(1) << 20,
                batch_bytes: (num("QUEEN_RAFT_FWD_BATCH_KB", 1024).max(16) << 10) as usize,
                linger: Duration::from_micros(num("QUEEN_RAFT_FWD_LINGER_US", 0)),
            }
        })
        .clone()
    }
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/// A command frame: `u32 len` · `u64 seq` · `body`.
fn command_frame(out: &mut Vec<u8>, seq: u64, body: &[u8]) {
    out.put_u32_le((8 + body.len()) as u32);
    out.put_u64_le(seq);
    out.extend_from_slice(body);
}

/// An answer frame: `u32 len` · `u64 seq` · `u8 kind` · `body`.
fn answer_frame(seq: u64, kind: u8, body: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(13 + body.len());
    out.put_u32_le((9 + body.len()) as u32);
    out.put_u64_le(seq);
    out.put_u8(kind);
    out.extend_from_slice(body);
    Bytes::from(out)
}

/// Splits a byte stream into frames, `(seq, what follows seq)`. Whole frames
/// are cut out of the incoming chunks without a copy; only a frame split
/// across chunks is assembled.
#[derive(Default)]
struct FrameBuf {
    partial: BytesMut,
}

impl FrameBuf {
    fn feed(&mut self, chunk: Bytes, out: &mut Vec<(u64, Bytes)>) -> Result<(), String> {
        let max = max_frame();
        if self.partial.is_empty() {
            let mut chunk = chunk;
            while let Some(f) = cut(&mut chunk, max)? {
                out.push(f);
            }
            if !chunk.is_empty() {
                self.partial.extend_from_slice(&chunk);
            }
            return Ok(());
        }
        self.partial.extend_from_slice(&chunk);
        loop {
            if self.partial.len() < 4 {
                return Ok(());
            }
            let len = frame_len(&self.partial, max)?;
            if self.partial.len() < 4 + len {
                self.partial.reserve(4 + len - self.partial.len());
                return Ok(());
            }
            self.partial.advance(4);
            let mut frame = self.partial.split_to(len).freeze();
            let seq = frame.get_u64_le();
            out.push((seq, frame));
        }
    }
}

fn frame_len(b: &[u8], max: usize) -> Result<usize, String> {
    let len = u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize;
    if !(8..=max).contains(&len) {
        return Err(format!("a forward frame of {len} bytes"));
    }
    Ok(len)
}

/// One whole frame off the front of `b`, if there is one.
fn cut(b: &mut Bytes, max: usize) -> Result<Option<(u64, Bytes)>, String> {
    if b.len() < 4 {
        return Ok(None);
    }
    let len = frame_len(b, max)?;
    if b.len() < 4 + len {
        return Ok(None);
    }
    b.advance(4);
    let mut frame = b.split_to(len);
    let seq = frame.get_u64_le();
    Ok(Some((seq, frame)))
}

// ---------------------------------------------------------------------------
// The leader's intake
// ---------------------------------------------------------------------------

/// `POST /raft/v1/forward` on the Raft RPC listener: one follower's stream.
/// 503 while no facade has installed its handler yet (the follower retries).
#[cfg(feature = "server")]
pub(crate) async fn intake<S: crate::rsm::store::Store + 'static>(
    axum::extract::State(st): axum::extract::State<Arc<super::network::RpcState<S>>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Body,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(h) = st.remote.get().cloned() else {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "no facade on this node yet",
        )
            .into_response();
    };
    let from = headers
        .get(FROM_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map_or(Source::Forwarded, Source::Node);
    serve_stream(h, st.admin.clients.get().cloned(), from, body)
}

/// A stream's answer: the commands of `body` handled on `clients` (the
/// runtime the facade serves clients on; the calling one when unknown), their
/// answers streamed back as each resolves.
#[cfg(feature = "server")]
pub(crate) fn serve_stream(
    handler: RemoteHandler,
    clients: Option<tokio::runtime::Handle>,
    from: Source,
    body: axum::body::Body,
) -> axum::response::Response {
    let (tx, rx) = mpsc::unbounded_channel::<Bytes>();
    let _ = tx.send(answer_frame(0, KIND_HELLO, &[VERSION]));
    let rt = clients.unwrap_or_else(tokio::runtime::Handle::current);
    rt.spawn(read_commands(body, handler, from, tx, rt.clone()));
    // Every answer ready at once goes out as one chunk.
    let answers = futures_util::stream::unfold(rx, |mut rx| async move {
        let first = rx.recv().await?;
        let Ok(second) = rx.try_recv() else {
            return Some((Ok::<Bytes, io::Error>(first), rx));
        };
        let mut out = Vec::with_capacity(first.len() + second.len() + 64);
        out.extend_from_slice(&first);
        out.extend_from_slice(&second);
        while out.len() < ANSWER_CHUNK_MAX {
            match rx.try_recv() {
                Ok(more) => out.extend_from_slice(&more),
                Err(_) => break,
            }
        }
        Some((Ok(Bytes::from(out)), rx))
    });
    axum::http::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, CONTENT_TYPE)
        .body(axum::body::Body::from_stream(answers))
        .unwrap_or_else(|_| {
            axum::response::IntoResponse::into_response(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            )
        })
}

/// Read the stream's commands; each is handled on its own task, and its
/// answer goes to `tx` when it resolves. The answer body ends once the
/// follower ended its commands and every one of them is answered.
#[cfg(feature = "server")]
async fn read_commands(
    body: axum::body::Body,
    handler: RemoteHandler,
    from: Source,
    tx: mpsc::UnboundedSender<Bytes>,
    rt: tokio::runtime::Handle,
) {
    use futures_util::StreamExt;
    let mut data = body.into_data_stream();
    let mut frames = FrameBuf::default();
    let mut ready = Vec::new();
    while let Some(chunk) = data.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(target: "rsm", error = %e, "raft forward stream: the follower's body ended");
                return;
            }
        };
        if let Err(why) = frames.feed(chunk, &mut ready) {
            tracing::warn!(target: "rsm", ?from, why, "raft forward stream: a bad frame; closing it");
            return;
        }
        for (seq, command) in ready.drain(..) {
            let (h, tx) = (handler.clone(), tx.clone());
            rt.spawn(async move {
                let answer =
                    crate::rsm::admit::forwarded_scope(from, async move { h(command).await }).await;
                let _ = tx.send(match answer {
                    Ok(b) => answer_frame(seq, KIND_ANSWER, &b),
                    Err(e) => answer_frame(seq, KIND_FAILED, e.as_bytes()),
                });
                SERVED.fetch_add(1, Ordering::Relaxed);
            });
        }
    }
}

// ---------------------------------------------------------------------------
// The follower's forwarder
// ---------------------------------------------------------------------------

/// Why a batched call did not get its answer.
#[derive(Debug)]
pub(crate) enum FwdError {
    /// The leader does not serve the route (an older version): use the
    /// per-command path.
    Unsupported,
    /// Anything else in transit, before the command went out: retry (same
    /// request id).
    Transport(String),
    /// The command went out and no answer came back: the leader may have run
    /// it ([`super::RemoteError::Lost`]).
    Lost(String),
}

/// A follower's streams to the leader. See the module header.
pub(crate) struct Forwarder {
    from: u64,
    token: Option<Arc<str>>,
    cfg: FwdConfig,
    /// Command bytes in flight.
    room: Arc<tokio::sync::Semaphore>,
    room_cap: u64,
    /// The streams to the current leader.
    link: std::sync::RwLock<Option<Arc<Link>>>,
    /// A leader that answered the route 404, and until when it is not asked
    /// again.
    legacy: std::sync::RwLock<Option<(Arc<str>, Instant)>>,
}

struct Link {
    addr: Arc<str>,
    slots: Vec<Slot>,
    next: AtomicUsize,
}

/// One of a link's streams: the open one, and the lock only its (re)opening
/// takes — a command finds a live stream without waiting on anyone.
#[derive(Default)]
struct Slot {
    conn: Mutex<Option<Arc<Conn>>>,
    opening: tokio::sync::Mutex<()>,
}

impl Slot {
    fn live(&self) -> Option<Arc<Conn>> {
        self.conn
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .filter(|c| !c.pending.dead())
            .cloned()
    }
}

/// One open stream.
struct Conn {
    tx: mpsc::UnboundedSender<Out>,
    pending: Arc<Pending>,
    seq: AtomicU64,
}

/// A command on its way to the writer.
struct Out {
    seq: u64,
    body: Bytes,
}

type Answer = Result<Bytes, String>;

/// The commands of one stream waiting for their answers.
struct Pending {
    map: Mutex<HashMap<u64, oneshot::Sender<Answer>>>,
    dead: AtomicBool,
}

impl Pending {
    fn new() -> Pending {
        Pending {
            map: Mutex::new(HashMap::new()),
            dead: AtomicBool::new(false),
        }
    }

    fn dead(&self) -> bool {
        self.dead.load(Ordering::Acquire)
    }

    /// Wait for `seq`'s answer; `false` once the stream is dead.
    fn register(&self, seq: u64, tx: oneshot::Sender<Answer>) -> bool {
        let mut m = self.map.lock().unwrap_or_else(|p| p.into_inner());
        if self.dead() {
            return false;
        }
        m.insert(seq, tx);
        true
    }

    fn answer(&self, seq: u64, a: Answer) {
        let tx = self
            .map
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&seq);
        if let Some(tx) = tx {
            let _ = tx.send(a);
        }
    }

    fn forget(&self, seq: u64) {
        self.map
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&seq);
    }

    fn is_empty(&self) -> bool {
        self.map
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_empty()
    }

    /// The stream is gone: every command still waiting fails with `why`.
    fn kill(&self, why: &str) {
        tracing::debug!(target: "rsm", why, "raft forward stream: closed");
        let gone: Vec<_> = {
            let mut m = self.map.lock().unwrap_or_else(|p| p.into_inner());
            self.dead.store(true, Ordering::Release);
            m.drain().map(|(_, tx)| tx).collect()
        };
        for tx in gone {
            let _ = tx.send(Err(why.to_string()));
        }
    }
}

impl Forwarder {
    pub(crate) fn new(from: u64, token: Option<Arc<str>>, cfg: FwdConfig) -> Forwarder {
        let room_cap = cfg
            .inflight_bytes
            .min(tokio::sync::Semaphore::MAX_PERMITS as u64)
            .min(u32::MAX as u64);
        Forwarder {
            from,
            token,
            room: Arc::new(tokio::sync::Semaphore::new(room_cap as usize)),
            room_cap,
            cfg,
            link: std::sync::RwLock::new(None),
            legacy: std::sync::RwLock::new(None),
        }
    }

    /// Whether `addr` answered the route 404 a moment ago (use the
    /// per-command path).
    pub(crate) fn is_legacy(&self, addr: &str) -> bool {
        let l = self.legacy.read().unwrap_or_else(|p| p.into_inner());
        matches!(&*l, Some((a, until)) if &**a == addr && Instant::now() < *until)
    }

    fn note_legacy(&self, addr: &Arc<str>) {
        LEGACY.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            target: "rsm",
            leader = %addr,
            "raft: the leader does not serve batched forwarding (an older version): \
             forwarding one call per command to it"
        );
        *self.legacy.write().unwrap_or_else(|p| p.into_inner()) =
            Some((addr.clone(), Instant::now() + LEGACY_RECHECK));
    }

    /// Send one prepared command (a `/raft/v1/submit` body) to the leader at
    /// `addr` and return its answer, within `ttl`. A `drain` command (it
    /// stores nothing: a pop, an ack) takes no room in the window: the window
    /// is FIFO, and an ack queued behind megabytes of pushes held a follower's
    /// consumers ~0.55 s per ack (2026-09-30).
    pub(crate) async fn call(
        &self,
        addr: &Arc<str>,
        body: Bytes,
        ttl: Duration,
        drain: bool,
    ) -> Result<Bytes, FwdError> {
        let deadline = tokio::time::Instant::now() + ttl;
        let charge = ((body.len() + 12) as u64).clamp(1, self.room_cap) as u32;
        let _room = if drain {
            None
        } else {
            match tokio::time::timeout_at(deadline, self.room.clone().acquire_many_owned(charge))
                .await
            {
                Ok(Ok(p)) => Some(p),
                _ => {
                    return Err(FwdError::Transport(
                        "no room to forward within the deadline".into(),
                    ))
                }
            }
        };
        let link = self.link_to(addr);
        // A stream that died between our pick and our registration is
        // replaced once; after that the caller's loop retries.
        for _ in 0..2 {
            let conn = self.conn(&link, deadline).await?;
            let seq = conn.seq.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = oneshot::channel();
            if !conn.pending.register(seq, tx) {
                continue;
            }
            if conn
                .tx
                .send(Out {
                    seq,
                    body: body.clone(),
                })
                .is_err()
            {
                conn.pending.forget(seq);
                continue;
            }
            let pending = conn.pending.clone();
            drop(conn);
            SENT.fetch_add(1, Ordering::Relaxed);
            return match tokio::time::timeout_at(deadline, rx).await {
                Ok(Ok(Ok(answer))) => Ok(answer),
                // The stream died with it on board (`Pending::kill`), or the
                // leader's intake failed it (a command that did not decode,
                // a task that ended): whether it ran is not known here.
                Ok(Ok(Err(why))) => Err(FwdError::Lost(why)),
                Ok(Err(_)) => Err(FwdError::Lost("the forward stream closed".into())),
                Err(_) => {
                    pending.forget(seq);
                    Err(FwdError::Lost(format!("{addr}: no answer within {ttl:?}")))
                }
            };
        }
        Err(FwdError::Transport(format!(
            "{addr}: the forward stream closed"
        )))
    }

    /// The streams to `addr`, replacing those to a previous leader (their
    /// commands in flight are still answered: the old streams end once their
    /// last answer is in).
    fn link_to(&self, addr: &Arc<str>) -> Arc<Link> {
        if let Some(link) = self
            .link
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .filter(|k| k.addr == *addr)
        {
            return link.clone();
        }
        let mut l = self.link.write().unwrap_or_else(|p| p.into_inner());
        if let Some(link) = l.as_ref().filter(|k| k.addr == *addr) {
            return link.clone();
        }
        let link = Arc::new(Link {
            addr: addr.clone(),
            slots: (0..self.cfg.streams).map(|_| Slot::default()).collect(),
            next: AtomicUsize::new(0),
        });
        *l = Some(link.clone());
        link
    }

    /// A live stream of `link`, opening one where the slot's died.
    async fn conn(
        &self,
        link: &Link,
        deadline: tokio::time::Instant,
    ) -> Result<Arc<Conn>, FwdError> {
        let slot = &link.slots[link.next.fetch_add(1, Ordering::Relaxed) % link.slots.len()];
        if let Some(c) = slot.live() {
            return Ok(c);
        }
        // One opening at a time; whoever waited finds it open.
        let _opening = match tokio::time::timeout_at(deadline, slot.opening.lock()).await {
            Ok(g) => g,
            Err(_) => {
                return Err(FwdError::Transport(
                    "no forward stream within the deadline".into(),
                ))
            }
        };
        if let Some(c) = slot.live() {
            return Ok(c);
        }
        match open(
            &link.addr,
            self.token.as_deref(),
            self.from,
            deadline,
            &self.cfg,
        )
        .await
        {
            Ok(c) => {
                let c = Arc::new(c);
                *slot.conn.lock().unwrap_or_else(|p| p.into_inner()) = Some(c.clone());
                Ok(c)
            }
            Err(FwdError::Unsupported) => {
                self.note_legacy(&link.addr);
                Err(FwdError::Unsupported)
            }
            Err(e) => Err(e),
        }
    }
}

/// Open a stream to `addr`: the request head, then the answer's head and its
/// hello frame, then the writer and reader tasks.
async fn open(
    addr: &str,
    token: Option<&str>,
    from: u64,
    deadline: tokio::time::Instant,
    cfg: &FwdConfig,
) -> Result<Conn, FwdError> {
    let t = |e: String| {
        tracing::debug!(target: "rsm", leader = addr, error = %e, "raft forward stream: not opened");
        FwdError::Transport(format!("{addr}{ROUTE}: {e}"))
    };
    let connect_by = deadline.min(tokio::time::Instant::now() + CONNECT_WITHIN);
    let sock = match tokio::time::timeout_at(connect_by, tokio::net::TcpStream::connect(addr)).await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(t(e.to_string())),
        Err(_) => return Err(t("no connection within the deadline".into())),
    };
    let _ = sock.set_nodelay(true);
    let (r, w) = sock.into_split();
    let mut w = BufWriter::with_capacity(256 * 1024, w);
    let mut head = format!(
        "POST {ROUTE} HTTP/1.1\r\nhost: {addr}\r\ncontent-type: {CONTENT_TYPE}\r\n\
         transfer-encoding: chunked\r\n{FROM_HEADER}: {from}\r\n"
    );
    if let Some(tok) = token {
        head.push_str(super::cluster::TOKEN_HEADER);
        head.push_str(": ");
        head.push_str(tok);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    let sent = async {
        w.write_all(head.as_bytes()).await?;
        w.flush().await
    };
    match tokio::time::timeout_at(deadline, sent).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(t(e.to_string())),
        Err(_) => return Err(t("the request head did not go out in time".into())),
    }
    let mut r = BufReader::with_capacity(64 * 1024, r);
    let (status, chunked) = match tokio::time::timeout_at(deadline, read_head(&mut r)).await {
        Ok(Ok(h)) => h,
        Ok(Err(e)) => return Err(t(e.to_string())),
        Err(_) => return Err(t("no answer head within the deadline".into())),
    };
    match status {
        200 if chunked => {}
        // An older leader: no such route.
        404 | 405 => return Err(FwdError::Unsupported),
        401 => return Err(t("refused this node's QUEEN_RAFT_TOKEN".into())),
        s => return Err(t(format!("status {s}"))),
    }
    let mut body = ChunkedReader::new(r);
    let mut frames = FrameBuf::default();
    let mut ready = Vec::new();
    let hello = async {
        while ready.is_empty() {
            match body.next_chunk().await? {
                Some(c) => frames.feed(c, &mut ready).map_err(io::Error::other)?,
                None => return Err(io::Error::other("the stream ended before its hello")),
            }
        }
        Ok::<_, io::Error>(())
    };
    match tokio::time::timeout_at(deadline, hello).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(t(e.to_string())),
        Err(_) => return Err(t("no hello within the deadline".into())),
    }
    let (seq, first) = ready.remove(0);
    if seq != 0 || first.first() != Some(&KIND_HELLO) || first.get(1) != Some(&VERSION) {
        return Err(t("the stream did not open with a version 1 hello".into()));
    }
    let pending = Arc::new(Pending::new());
    // Answers that arrived with the hello.
    for (seq, a) in ready {
        deliver(&pending, seq, a);
    }
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(write_commands(w, rx, pending.clone(), cfg.clone()));
    tokio::spawn(read_answers(body, frames, pending.clone()));
    OPENED.fetch_add(1, Ordering::Relaxed);
    Ok(Conn {
        tx,
        pending,
        seq: AtomicU64::new(1),
    })
}

fn deliver(pending: &Pending, seq: u64, frame: Bytes) {
    let Some(&kind) = frame.first() else {
        return;
    };
    let body = frame.slice(1..);
    match kind {
        KIND_ANSWER => pending.answer(seq, Ok(body)),
        KIND_FAILED => pending.answer(seq, Err(String::from_utf8_lossy(&body).into_owned())),
        // A hello, or a kind this version does not know: nothing waits for it.
        _ => {}
    }
}

/// The writer: whatever is queued goes out as one chunk; the chunk that ends
/// the body goes once every sender is gone (the link was replaced) or the
/// stream sat idle.
async fn write_commands(
    mut w: BufWriter<OwnedWriteHalf>,
    mut rx: mpsc::UnboundedReceiver<Out>,
    pending: Arc<Pending>,
    cfg: FwdConfig,
) {
    let mut frames: Vec<u8> = Vec::with_capacity(64 * 1024);
    loop {
        let first = match tokio::time::timeout(IDLE, rx.recv()).await {
            Ok(Some(o)) => o,
            Ok(None) => break,
            Err(_) if pending.is_empty() && !pending.dead() => {
                // Idle: closed, so a dead middlebox connection is never used.
                pending.kill("the forward stream was idle");
                break;
            }
            Err(_) => continue,
        };
        frames.clear();
        command_frame(&mut frames, first.seq, &first.body);
        loop {
            while frames.len() < cfg.batch_bytes {
                match rx.try_recv() {
                    Ok(o) => command_frame(&mut frames, o.seq, &o.body),
                    Err(_) => break,
                }
            }
            if cfg.linger.is_zero() || frames.len() >= cfg.batch_bytes {
                break;
            }
            match tokio::time::timeout(cfg.linger, rx.recv()).await {
                Ok(Some(o)) => command_frame(&mut frames, o.seq, &o.body),
                _ => break,
            }
        }
        let written = async {
            w.write_all(format!("{:x}\r\n", frames.len()).as_bytes())
                .await?;
            w.write_all(&frames).await?;
            w.write_all(b"\r\n").await?;
            w.flush().await
        };
        if let Err(e) = written.await {
            pending.kill(&format!("the forward stream broke: {e}"));
            return;
        }
        if frames.capacity() > 4 * cfg.batch_bytes {
            frames = Vec::with_capacity(64 * 1024);
        }
    }
    // The end of the commands; the answers still come. Not a shutdown of the
    // socket: the leader closes a connection whose client half-closed.
    let _ = async {
        w.write_all(b"0\r\n\r\n").await?;
        w.flush().await
    }
    .await;
    w.into_inner().forget();
}

/// The reader: answers to their callers, until the leader ends the stream.
async fn read_answers(mut body: ChunkedReader, mut frames: FrameBuf, pending: Arc<Pending>) {
    let mut ready = Vec::new();
    let why = loop {
        match body.next_chunk().await {
            Ok(Some(chunk)) => {
                if let Err(e) = frames.feed(chunk, &mut ready) {
                    break e;
                }
                for (seq, a) in ready.drain(..) {
                    deliver(&pending, seq, a);
                }
            }
            Ok(None) => break "the leader ended the forward stream".to_string(),
            Err(e) => break format!("the forward stream broke: {e}"),
        }
    };
    pending.kill(&why);
}

/// An HTTP/1.1 response head: the status, and whether the body is chunked.
async fn read_head(r: &mut BufReader<OwnedReadHalf>) -> io::Result<(u16, bool)> {
    let status_line = read_line(r, 1024).await?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse::<u16>().ok())
        .ok_or_else(|| io::Error::other(format!("not an HTTP answer: {status_line:?}")))?;
    let mut chunked = false;
    loop {
        let line = read_line(r, 8192).await?;
        if line == "\r\n" || line == "\n" {
            return Ok((status, chunked));
        }
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("transfer-encoding:") {
            chunked |= v.contains("chunked");
        }
    }
}

/// One line, its end of line included; at most `cap` bytes.
async fn read_line(r: &mut BufReader<OwnedReadHalf>, cap: u64) -> io::Result<String> {
    let mut line = Vec::new();
    (&mut *r).take(cap).read_until(b'\n', &mut line).await?;
    if !line.ends_with(b"\n") {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "the line ended early",
        ));
    }
    String::from_utf8(line).map_err(io::Error::other)
}

/// A chunked body, chunk by chunk (as the bytes arrive, not whole chunks).
struct ChunkedReader {
    r: BufReader<OwnedReadHalf>,
    /// Bytes left in the current chunk.
    left: usize,
    done: bool,
}

impl ChunkedReader {
    fn new(r: BufReader<OwnedReadHalf>) -> ChunkedReader {
        ChunkedReader {
            r,
            left: 0,
            done: false,
        }
    }

    async fn next_chunk(&mut self) -> io::Result<Option<Bytes>> {
        if self.done {
            return Ok(None);
        }
        if self.left == 0 {
            let line = read_line(&mut self.r, 1024).await?;
            let size = line
                .trim_end()
                .split(';')
                .next()
                .map(str::trim)
                .and_then(|s| usize::from_str_radix(s, 16).ok())
                .ok_or_else(|| io::Error::other(format!("a bad chunk size: {line:?}")))?;
            if size == 0 {
                loop {
                    let trailer = read_line(&mut self.r, 8192).await?;
                    if trailer == "\r\n" || trailer == "\n" {
                        break;
                    }
                }
                self.done = true;
                return Ok(None);
            }
            self.left = size;
        }
        let buf = self.r.fill_buf().await?;
        if buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the stream ended inside a chunk",
            ));
        }
        let n = buf.len().min(self.left);
        let chunk = Bytes::copy_from_slice(&buf[..n]);
        self.r.consume(n);
        self.left -= n;
        if self.left == 0 {
            let mut crlf = [0u8; 2];
            self.r.read_exact(&mut crlf).await?;
            if &crlf != b"\r\n" {
                return Err(io::Error::other("a chunk without its end of line"));
            }
        }
        Ok(Some(chunk))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frames_of(chunks: &[Bytes]) -> Vec<(u64, Bytes)> {
        let mut fb = FrameBuf::default();
        let mut out = Vec::new();
        for c in chunks {
            fb.feed(c.clone(), &mut out).expect("frames");
        }
        assert!(fb.partial.is_empty(), "nothing left over");
        out
    }

    #[test]
    fn frames_survive_any_split_of_the_stream() {
        let mut all = Vec::new();
        let bodies: Vec<Vec<u8>> = (0..50u8).map(|i| vec![i; i as usize * 7]).collect();
        for (i, b) in bodies.iter().enumerate() {
            command_frame(&mut all, i as u64 + 1, b);
        }
        for step in [1usize, 3, 13, 64, 1000, all.len()] {
            let chunks: Vec<Bytes> = all.chunks(step).map(Bytes::copy_from_slice).collect();
            let got = frames_of(&chunks);
            assert_eq!(got.len(), bodies.len(), "split by {step}");
            for (i, (seq, body)) in got.iter().enumerate() {
                assert_eq!(*seq, i as u64 + 1);
                assert_eq!(&body[..], &bodies[i][..]);
            }
        }
    }

    #[test]
    fn a_frame_too_short_or_too_long_is_refused() {
        let mut fb = FrameBuf::default();
        let mut out = Vec::new();
        let mut bad = Vec::new();
        bad.put_u32_le(4);
        bad.put_u32_le(0);
        assert!(fb.feed(Bytes::from(bad), &mut out).is_err());
        let mut fb = FrameBuf::default();
        let mut bad = Vec::new();
        bad.put_u32_le(u32::MAX);
        assert!(fb.feed(Bytes::from(bad), &mut out).is_err());
    }

    /// A leader's intake over `handler`, served by a real HTTP server on
    /// localhost (what the Raft RPC listener does with the route).
    #[cfg(feature = "server")]
    async fn serve(handler: RemoteHandler, token: Option<&'static str>) -> String {
        use axum::routing::post;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        let router = axum::Router::new().route(
            ROUTE,
            post(
                move |headers: axum::http::HeaderMap, body: axum::body::Body| {
                    let handler = handler.clone();
                    async move {
                        if let Some(want) = token {
                            let got = headers
                                .get(super::super::cluster::TOKEN_HEADER)
                                .and_then(|v| v.to_str().ok());
                            if got != Some(want) {
                                return axum::response::IntoResponse::into_response(
                                    axum::http::StatusCode::UNAUTHORIZED,
                                );
                            }
                        }
                        let from = headers
                            .get(FROM_HEADER)
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok())
                            .map_or(Source::Forwarded, Source::Node);
                        serve_stream(handler, None, from, body)
                    }
                },
            ),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        addr
    }

    type Fut = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Bytes, String>> + Send>>;

    /// Echoes each command back after a delay the command names (its first
    /// byte, in milliseconds), prefixed with the admission source it ran as.
    fn echo() -> RemoteHandler {
        Arc::new(|body: Bytes| -> Fut {
            Box::pin(async move {
                let ms = body.first().copied().unwrap_or(0) as u64;
                tokio::time::sleep(Duration::from_millis(ms)).await;
                let from = match crate::rsm::admit::forwarded_from() {
                    Source::Node(n) => n,
                    _ => 0,
                };
                let mut out = vec![from as u8];
                out.extend_from_slice(&body);
                Ok(Bytes::from(out))
            })
        })
    }

    fn cfg() -> FwdConfig {
        FwdConfig {
            on: true,
            streams: 2,
            inflight_bytes: 1 << 20,
            batch_bytes: 64 << 10,
            linger: Duration::ZERO,
        }
    }

    /// Many commands at once over two streams: each gets its own answer,
    /// whatever order they resolve in, and ran as the forwarding node's.
    #[cfg(feature = "server")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_command_gets_its_own_answer_in_whatever_order_they_resolve() {
        let addr: Arc<str> = serve(echo(), None).await.into();
        let fwd = Arc::new(Forwarder::new(7, None, cfg()));
        let mut ts = Vec::new();
        for i in 0..2_000u32 {
            let (fwd, addr) = (fwd.clone(), addr.clone());
            ts.push(tokio::spawn(async move {
                let delay = (i % 5 * 3) as u8;
                let mut body = vec![delay];
                body.extend_from_slice(&i.to_le_bytes());
                let got = fwd
                    .call(
                        &addr,
                        Bytes::from(body.clone()),
                        Duration::from_secs(10),
                        false,
                    )
                    .await
                    .expect("answer");
                assert_eq!(got[0], 7, "admitted as node 7's");
                assert_eq!(&got[1..], &body[..], "command {i}");
            }));
        }
        for t in ts {
            t.await.expect("call");
        }
        assert_eq!(
            fwd.room.available_permits() as u64,
            fwd.room_cap,
            "no room leaked"
        );
    }

    /// A stream the leader drops fails its commands at once (the caller's
    /// loop retries), and the next command opens a new one.
    #[cfg(feature = "server")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_broken_stream_fails_its_commands_and_the_next_opens_another() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let g = gate.clone();
        let handler: RemoteHandler = Arc::new(move |body: Bytes| -> Fut {
            let g = g.clone();
            Box::pin(async move {
                if body.first() == Some(&1) {
                    g.notified().await;
                    return Err("stop".to_string());
                }
                Ok(body)
            })
        });
        let addr: Arc<str> = serve(handler, None).await.into();
        let fwd = Arc::new(Forwarder::new(
            2,
            None,
            FwdConfig {
                streams: 1,
                ..cfg()
            },
        ));
        let r = fwd
            .call(
                &addr,
                Bytes::from_static(&[1, 2]),
                Duration::from_millis(200),
                false,
            )
            .await;
        assert!(
            matches!(r, Err(FwdError::Lost(_))),
            "no answer within the deadline (sent: the leader may have run it): {r:?}"
        );
        // Kill the stream under a waiting command.
        let (f2, a2) = (fwd.clone(), addr.clone());
        let waiting = tokio::spawn(async move {
            f2.call(
                &a2,
                Bytes::from_static(&[1, 3]),
                Duration::from_secs(10),
                false,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        {
            let link = fwd.link.read().unwrap().clone().expect("link");
            let conn = link.slots[0].live().expect("conn");
            conn.pending.kill("test kills it");
        }
        let r = tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .expect("answered at once")
            .expect("task");
        assert!(
            matches!(r, Err(FwdError::Lost(ref m)) if m.contains("test kills")),
            "{r:?}"
        );
        let ok = fwd
            .call(
                &addr,
                Bytes::from_static(&[0, 9]),
                Duration::from_secs(5),
                false,
            )
            .await
            .expect("a new stream");
        assert_eq!(&ok[..], &[0, 9]);
        gate.notify_waiters();
    }

    /// An older leader (no route: 404) makes the forwarder fall back to the
    /// per-command path, and remember it for a while.
    #[cfg(feature = "server")]
    #[tokio::test]
    async fn an_older_leader_without_the_route_means_the_per_command_path() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr: Arc<str> = listener.local_addr().expect("addr").to_string().into();
        let router =
            axum::Router::new().route("/raft/v1/submit", axum::routing::post(|| async { "old" }));
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        let fwd = Forwarder::new(2, None, cfg());
        assert!(!fwd.is_legacy(&addr));
        let r = fwd
            .call(
                &addr,
                Bytes::from_static(b"x"),
                Duration::from_secs(2),
                false,
            )
            .await;
        assert!(matches!(r, Err(FwdError::Unsupported)), "{r:?}");
        assert!(fwd.is_legacy(&addr), "remembered");
        assert!(!fwd.is_legacy("127.0.0.1:1"), "for that leader only");
    }

    /// The cluster token travels on the stream's head; a wrong one is
    /// refused, not retried as a transport hiccup forever.
    #[cfg(feature = "server")]
    #[tokio::test]
    async fn the_stream_carries_the_cluster_token() {
        let addr: Arc<str> = serve(echo(), Some("s3cret")).await.into();
        let good = Forwarder::new(1, Some(Arc::from("s3cret")), cfg());
        let got = good
            .call(
                &addr,
                Bytes::from_static(&[0, 1]),
                Duration::from_secs(5),
                false,
            )
            .await
            .expect("answer");
        assert_eq!(&got[..], &[1, 0, 1]);
        let bad = Forwarder::new(1, Some(Arc::from("nope")), cfg());
        let r = bad
            .call(
                &addr,
                Bytes::from_static(&[0, 1]),
                Duration::from_secs(5),
                false,
            )
            .await;
        assert!(
            matches!(r, Err(FwdError::Transport(ref m)) if m.contains("TOKEN")),
            "{r:?}"
        );
    }

    /// In-flight bytes are bounded: a command that finds no room waits, and
    /// fails at its deadline without having been sent.
    #[cfg(feature = "server")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commands_in_flight_are_bounded_by_bytes() {
        let addr: Arc<str> = serve(echo(), None).await.into();
        let fwd = Arc::new(Forwarder::new(
            1,
            None,
            FwdConfig {
                inflight_bytes: 4096,
                ..cfg()
            },
        ));
        // 200 ms each, 2,000 bytes each: two fit at once.
        let slow = |n: u8| {
            let mut b = vec![200u8];
            b.resize(2000, n);
            Bytes::from(b)
        };
        let (f1, a1) = (fwd.clone(), addr.clone());
        let t1 =
            tokio::spawn(async move { f1.call(&a1, slow(1), Duration::from_secs(5), false).await });
        let (f2, a2) = (fwd.clone(), addr.clone());
        let t2 =
            tokio::spawn(async move { f2.call(&a2, slow(2), Duration::from_secs(5), false).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let r = fwd
            .call(&addr, slow(3), Duration::from_millis(50), false)
            .await;
        assert!(
            matches!(r, Err(FwdError::Transport(ref m)) if m.contains("no room")),
            "{r:?}"
        );
        assert!(t1.await.unwrap().is_ok());
        assert!(t2.await.unwrap().is_ok());
        assert!(fwd
            .call(&addr, slow(4), Duration::from_secs(5), false)
            .await
            .is_ok());
    }
}
