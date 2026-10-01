//! Pipelined replication: openraft's `stream_append` over ONE long-lived
//! request per follower, several AppendEntries in flight at once.
//!
//! openraft's replication task hands the network a stream of AppendEntries and
//! reads a stream of answers. Before this, [`super::network::HttpPeer`] had
//! only the unary `append_entries`, so openraft's sequential adapter sent one
//! request, waited for its answer, then sent the next: ONE append in flight per
//! follower, the round trip (~2.0 ms) plus the transfer and the follower's
//! write (~3.2 ms per MB) with no overlap — about 300 MB/s per follower at best
//! (3x16 vCPU, 1M msg/s).
//!
//! # The transport
//!
//! `POST /raft/v1/stream`: an HTTP/1.1 request whose body the leader keeps
//! writing and a response whose body the follower keeps writing, both chunked,
//! full duplex, for as long as the connection lives — the Raft RPC port, its
//! token check and its pooled client, no second listener. Both bodies carry
//! frames:
//!
//! ```text
//! len:u32 | tag:u8 | body          (len counts tag and body; little-endian)
//! tag 1, leader → follower:  session:u64 | members_len:u32 | members | append
//! tag 2, follower → leader:  session:u64 | answer (JSON)
//! ```
//!
//! `append` is exactly the unary route's body ([`super::wire`]), `members` the
//! leader's members view the unary route carries in a header
//! ([`super::members`]), and the answer is the follower's
//! `Result<StreamAppendResult, fatal message>`.
//!
//! # Sessions
//!
//! Each `stream_append` call is a SESSION with its own number on the one
//! connection. The follower feeds a session's requests, in order, to
//! `Raft::stream_append` (itself pipelined into its RaftCore) and writes the
//! answers back in order, tagged; a frame of a new session ends the previous
//! one there. The leader matches answers to its requests in order and skips an
//! answer of an earlier session (one it gave up on). TCP keeps a session's
//! requests in order; Raft's own checks (the vote, `prev_log_id`) make any
//! request that arrives stale harmless, as on the unary route.
//!
//! # Flow control and deadlines
//!
//! The leader reads the next request only while fewer than
//! `QUEEN_RAFT_STREAM_INFLIGHT_MB` (default 64) of frames are unanswered, so at
//! most that plus one request is ever in flight. A request over
//! [`super::wire::MAX_APPEND_BYTES`] is split into several frames (the answer
//! of the last one answers openraft). Each frame keeps the unary route's
//! deadline ([`super::network::append_ttl`]: the soft TTL, one second more,
//! and a microsecond per 50 bytes); the oldest one missing it closes the
//! connection and ends the session with a network error, and openraft opens a
//! new one.
//!
//! # Older nodes
//!
//! A follower that does not serve the route (404) is reached over the unary
//! route for a minute before the stream is tried again; so is every follower
//! with `QUEEN_RAFT_STREAM_APPEND=0`. The handshake's answer headers say what
//! the follower reads, as the unary route's answers do: compressed entries
//! ([`wire::WIRE_HEADER`]) and the effect catalogue
//! ([`super::members::KINDS_HEADER`], D20); an older follower sends neither.

use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::http::{header, Method, Request, StatusCode};
use bytes::{Buf, BufMut, BytesMut};
use futures_util::{Stream, StreamExt};
use http_body_util::BodyExt;
use openraft::errors::{NetworkError, RPCError, Unreachable};
use openraft::raft::{AppendEntriesRequest, StreamAppendResult};
#[cfg(test)]
use openraft::EntryPayload;
use tokio::sync::mpsc;

use super::cluster::TOKEN_HEADER;
use super::network::{append_ttl, HttpClient};
#[cfg(test)]
use super::types::REntry;
use super::types::{NodeId, TypeConfig};
use super::wire;

/// The route.
pub(crate) const STREAM_PATH: &str = "/raft/v1/stream";

const TAG_APPEND: u8 = 1;
const TAG_ANSWER: u8 = 2;

/// A follower's answer to one frame: the stream result, or why its Raft
/// stopped.
pub(crate) type Answer = Result<StreamAppendResult<TypeConfig>, String>;

/// `QUEEN_RAFT_STREAM_APPEND` (default on): replicate over the stream. Off:
/// the unary route, one append in flight per follower (the path before it,
/// kept for an A/B).
pub(crate) fn enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| match std::env::var("QUEEN_RAFT_STREAM_APPEND") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        Err(_) => true,
    })
}

/// `QUEEN_RAFT_STREAM_INFLIGHT_MB` (default 64): unanswered frame bytes per
/// follower before the leader stops reading more requests.
pub(crate) fn inflight_cap() -> usize {
    static CAP: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("QUEEN_RAFT_STREAM_INFLIGHT_MB")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(64)
            .saturating_mul(1 << 20)
    })
}

/// How long a follower that does not serve the stream is reached over the
/// unary route before the stream is tried again.
const UNSUPPORTED_RETRY: Duration = Duration::from_secs(60);

/// How long opening the stream may take.
const CONNECT_TTL: Duration = Duration::from_secs(5);

/// The largest frame either side accepts: one append body (never over twice
/// the larger of a client request and [`wire::MAX_APPEND_BYTES`], plus
/// slack — the unary route's cap) with its members view.
pub(crate) fn max_frame() -> usize {
    static MAX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *MAX.get_or_init(|| {
        let client = std::env::var("QUEEN_MAX_BODY_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(64 * 1024 * 1024);
        2 * client.max(wire::MAX_APPEND_BYTES) + (2 << 20)
    })
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

fn bad(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// One frame: `len | tag | parts...`.
fn frame(tag: u8, parts: &[&[u8]]) -> Bytes {
    let body: usize = parts.iter().map(|p| p.len()).sum();
    let mut out = BytesMut::with_capacity(5 + body);
    out.put_u32_le((1 + body) as u32);
    out.put_u8(tag);
    for p in parts {
        out.put_slice(p);
    }
    out.freeze()
}

/// A leader's request frame.
pub(crate) fn append_frame(session: u64, members: &[u8], append: &[u8]) -> Bytes {
    frame(
        TAG_APPEND,
        &[
            &session.to_le_bytes(),
            &(members.len() as u32).to_le_bytes(),
            members,
            append,
        ],
    )
}

/// A request frame's parts: the session, the members view, the append body.
pub(crate) fn parse_append(mut body: Bytes) -> io::Result<(u64, Bytes, Bytes)> {
    if body.len() < 12 {
        return Err(bad("stream append frame truncated"));
    }
    let session = body.get_u64_le();
    let mlen = body.get_u32_le() as usize;
    if body.len() < mlen {
        return Err(bad("stream append frame: members view truncated"));
    }
    let members = body.split_to(mlen);
    Ok((session, members, body))
}

/// A follower's answer frame.
pub(crate) fn answer_frame(session: u64, answer: &Answer) -> Bytes {
    let json = serde_json::to_vec(answer).unwrap_or_else(|e| {
        serde_json::to_vec(&Answer::Err(format!("answer does not encode: {e}"))).unwrap_or_default()
    });
    frame(TAG_ANSWER, &[&session.to_le_bytes(), &json])
}

/// An answer frame's session and answer.
pub(crate) fn parse_answer(mut body: Bytes) -> io::Result<(u64, Answer)> {
    if body.len() < 8 {
        return Err(bad("stream answer frame truncated"));
    }
    let session = body.get_u64_le();
    let answer: Answer =
        serde_json::from_slice(&body).map_err(|e| bad(format!("stream answer: {e}")))?;
    Ok((session, answer))
}

/// Cuts a byte stream back into frames.
#[derive(Default)]
pub(crate) struct Deframer {
    buf: BytesMut,
}

impl Deframer {
    pub(crate) fn push(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    /// The next whole frame `(tag, body)`, if one is buffered. A length over
    /// [`max_frame`] (or under one byte) is an error: the stream is not
    /// ours, or broken.
    pub(crate) fn next_frame(&mut self) -> io::Result<Option<(u8, Bytes)>> {
        if self.buf.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_le_bytes(self.buf[..4].try_into().expect("4 bytes")) as usize;
        if len == 0 || len > max_frame() {
            return Err(bad(format!("stream frame of {len} bytes")));
        }
        if self.buf.len() < 4 + len {
            // Room for the rest, once (a large append arrives in many chunks).
            self.buf.reserve(4 + len - self.buf.len());
            return Ok(None);
        }
        self.buf.advance(4);
        let mut f = self.buf.split_to(len).freeze();
        let tag = f.get_u8();
        Ok(Some((tag, f)))
    }
}

// ---------------------------------------------------------------------------
// The leader: one connection per follower, a session per stream_append call
// ---------------------------------------------------------------------------

/// Why the stream could not be opened.
#[derive(Debug)]
pub(crate) enum ConnectFail {
    /// The follower does not serve the stream (an older node).
    NotServed,
    Unreachable(String),
    Network(String),
}

/// An open stream to one follower.
pub(crate) struct Conn {
    /// Frames to the follower: the request body.
    tx: mpsc::Sender<Bytes>,
    /// The follower's answers, cut out of the response body by `reader`.
    /// Unbounded, so the reader always drains the connection: every answer
    /// is to a frame counted in flight, and a reader that could block on a
    /// session busy sending would close a loop of full buffers through the
    /// follower back to that session.
    answers: mpsc::UnboundedReceiver<io::Result<(u64, Answer)>>,
    /// The follower reads compressed entries ([`wire::WIRE_HEADER`]).
    pub(crate) reads_zstd: bool,
    /// The catalogue version the follower said it reads on the handshake
    /// ([`super::members::KINDS_HEADER`]; `None`: it said none).
    pub(crate) kinds: Option<u32>,
    /// A session gave up on it (a missed deadline, a protocol error): the
    /// next session opens a new one.
    broken: bool,
    reader: tokio::task::JoinHandle<()>,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

impl Conn {
    fn usable(&self) -> bool {
        !self.broken && !self.tx.is_closed()
    }
}

/// Open the stream: the request goes out with an empty body that the
/// sessions then write into. Runs as a task of its own, so a caller that
/// The sender a follower-side session feeds `Raft::stream_append` from: each
/// request with the instant its frame arrived.
type SessionTx = mpsc::Sender<(AppendEntriesRequest<TypeConfig>, Instant)>;

/// gives up waiting (a heartbeat's deadline) does not throw the attempt away.
async fn connect(
    client: HttpClient,
    url: String,
    token: Option<Arc<str>>,
) -> Result<Conn, ConnectFail> {
    let (tx, rx) = mpsc::channel::<Bytes>(16);
    let body = Body::from_stream(futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|b| (Ok::<Bytes, std::convert::Infallible>(b), rx))
    }));
    let mut req = Request::builder()
        .method(Method::POST)
        .uri(&url)
        .header(header::CONTENT_TYPE, "application/octet-stream");
    if let Some(t) = token.as_deref() {
        req = req.header(TOKEN_HEADER, t);
    }
    let req = req
        .body(body)
        .map_err(|e| ConnectFail::Network(e.to_string()))?;
    let resp = match tokio::time::timeout(CONNECT_TTL, client.request(req)).await {
        Err(_) => {
            return Err(ConnectFail::Network(format!(
                "{url}: the stream did not open within {CONNECT_TTL:?}"
            )))
        }
        Ok(Err(e)) if e.is_connect() => {
            return Err(ConnectFail::Unreachable(format!("{url}: {e}")))
        }
        Ok(Err(e)) => return Err(ConnectFail::Network(format!("{url}: {e}"))),
        Ok(Ok(r)) => r,
    };
    match resp.status() {
        StatusCode::OK => {}
        StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED => {
            return Err(ConnectFail::NotServed)
        }
        StatusCode::UNAUTHORIZED => {
            return Err(ConnectFail::Unreachable(format!(
                "{url}: refused this node's QUEEN_RAFT_TOKEN"
            )))
        }
        s => return Err(ConnectFail::Network(format!("{url}: {s}"))),
    }
    let reads_zstd = wire::reads_zstd(resp.headers().get(wire::WIRE_HEADER).map(|v| v.as_bytes()));
    let kinds = super::members::kinds_of_header(
        resp.headers()
            .get(super::members::KINDS_HEADER)
            .map(|v| v.as_bytes()),
    );
    let (atx, arx) = mpsc::unbounded_channel::<io::Result<(u64, Answer)>>();
    let mut body = resp.into_body();
    let reader = tokio::spawn(crate::obs::panic_policy::non_core(async move {
        let mut d = Deframer::default();
        loop {
            let chunk = match body.frame().await {
                Some(Ok(f)) => match f.into_data() {
                    Ok(data) => data,
                    Err(_) => continue,
                },
                Some(Err(e)) => {
                    let _ = atx.send(Err(io::Error::other(format!("the stream broke: {e}"))));
                    return;
                }
                None => {
                    let _ = atx.send(Err(io::Error::other("the follower closed the stream")));
                    return;
                }
            };
            d.push(&chunk);
            loop {
                let answer = match d.next_frame() {
                    Ok(Some((TAG_ANSWER, f))) => parse_answer(f),
                    Ok(Some((tag, _))) => Err(bad(format!("unexpected stream frame tag {tag}"))),
                    Ok(None) => break,
                    Err(e) => Err(e),
                };
                let stop = answer.is_err();
                if atx.send(answer).is_err() || stop {
                    return;
                }
            }
        }
    }));
    Ok(Conn {
        tx,
        answers: arx,
        reads_zstd,
        kinds,
        broken: false,
        reader,
    })
}

/// The stream state of one [`super::network::HttpPeer`].
#[derive(Default)]
pub(crate) struct StreamPeer {
    conn: Option<Conn>,
    connecting: Option<tokio::task::JoinHandle<Result<Conn, ConnectFail>>>,
    /// The follower does not serve the stream: unary until then.
    unary_until: Option<Instant>,
    session: u64,
}

impl StreamPeer {
    /// Whether to take the unary route now.
    pub(crate) fn unary(&self) -> bool {
        !enabled() || self.unary_until.is_some_and(|t| Instant::now() < t)
    }

    /// The open stream, opening one if needed. `Ok(None)`: the follower does
    /// not serve the stream (take the unary route).
    pub(crate) async fn conn(
        &mut self,
        client: &HttpClient,
        base: &str,
        token: &Option<Arc<str>>,
    ) -> Result<Option<&mut Conn>, RPCError<TypeConfig>> {
        if self.conn.as_ref().is_some_and(|c| !c.usable()) {
            self.conn = None;
        }
        if self.conn.is_none() {
            let task = self.connecting.get_or_insert_with(|| {
                tokio::spawn(crate::obs::panic_policy::non_core(connect(
                    client.clone(),
                    format!("{base}{STREAM_PATH}"),
                    token.clone(),
                )))
            });
            let res = task.await;
            self.connecting = None;
            match res {
                Ok(Ok(c)) => self.conn = Some(c),
                Ok(Err(ConnectFail::NotServed)) => {
                    tracing::info!(target: "rsm", base, "raft: the follower does not serve the append stream; unary appends for now");
                    self.unary_until = Some(Instant::now() + UNSUPPORTED_RETRY);
                    return Ok(None);
                }
                Ok(Err(ConnectFail::Unreachable(m))) => {
                    return Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
                        m,
                    ))))
                }
                Ok(Err(ConnectFail::Network(m))) => {
                    return Err(RPCError::Network(NetworkError::new(&io::Error::other(m))))
                }
                Err(e) => {
                    return Err(RPCError::Network(NetworkError::new(&io::Error::other(
                        format!("opening the append stream: {e}"),
                    ))))
                }
            }
        }
        self.session += 1;
        Ok(self.conn.as_mut())
    }

    /// The current session's number (after [`StreamPeer::conn`]).
    pub(crate) fn session(&self) -> u64 {
        self.session
    }

    /// The stream [`StreamPeer::conn`] opened.
    pub(crate) fn open_conn(&mut self) -> Option<&mut Conn> {
        self.conn.as_mut()
    }
}

/// One frame sent and not answered yet.
struct InFlight {
    bytes: usize,
    sent_at: Instant,
    /// Its own deadline's length ([`append_ttl`] of its size).
    ttl: Duration,
    /// When it must be answered: its TTL from when it was sent, or from when
    /// the frame before it was answered, whichever is later — answers come
    /// in order, so a frame queued behind a large one only starts then.
    deadline: Instant,
    /// Entries it carries (for the stage timings).
    entries: usize,
    /// The last frame of an openraft request: its answer is openraft's.
    last: bool,
}

/// What a session needs besides its connection.
pub(crate) struct SessionCtx {
    pub(crate) id: u64,
    pub(crate) target: NodeId,
    pub(crate) soft: Duration,
    pub(crate) members: Arc<super::members::MembersState>,
}

/// One `stream_append` call: requests from openraft out, answers back, in
/// order.
pub(crate) struct Session<'c, S> {
    conn: &'c mut Conn,
    ctx: SessionCtx,
    input: Option<S>,
    inflight: VecDeque<InFlight>,
    inflight_bytes: usize,
    done: bool,
}

type Item = Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>;

fn net_err(m: impl Into<String>) -> RPCError<TypeConfig> {
    RPCError::Network(NetworkError::new(&io::Error::other(m.into())))
}

impl<'c, S> Session<'c, S>
where
    S: Stream<Item = AppendEntriesRequest<TypeConfig>> + Unpin + Send + 'static,
{
    pub(crate) fn new(conn: &'c mut Conn, ctx: SessionCtx, input: S) -> Session<'c, S> {
        Session {
            conn,
            ctx,
            input: Some(input),
            inflight: VecDeque::new(),
            inflight_bytes: 0,
            done: false,
        }
    }

    /// The session as openraft's answer stream.
    pub(crate) fn into_stream(self) -> impl Stream<Item = Item> + Send + 'c
    where
        S: 'c,
    {
        futures_util::stream::unfold(self, |mut s| async move {
            let item = s.next_answer().await?;
            Some((item, s))
        })
    }

    /// The session gives up: its connection is not used again.
    fn fail(&mut self, e: RPCError<TypeConfig>) -> Option<Item> {
        self.conn.broken = true;
        self.done = true;
        Some(Err(e))
    }

    /// The next answer for openraft; `None` once the input is exhausted and
    /// every request answered, or after an error.
    async fn next_answer(&mut self) -> Option<Item> {
        loop {
            if self.done {
                return None;
            }
            if self.input.is_none() && self.inflight.is_empty() {
                self.done = true;
                return None;
            }
            let can_send = self.input.is_some() && self.inflight_bytes < inflight_cap();
            let deadline = self.inflight.front().map(|f| f.deadline);
            let answers = &mut self.conn.answers;
            let input = &mut self.input;
            enum Ev {
                Answer(Option<io::Result<(u64, Answer)>>),
                Late,
                Request(Option<AppendEntriesRequest<TypeConfig>>),
            }
            // A disabled branch's future is still built (never polled): both
            // are lazy.
            let next_request = async {
                match input.as_mut() {
                    Some(i) => i.next().await,
                    None => std::future::pending().await,
                }
            };
            let late = tokio::time::sleep_until(tokio::time::Instant::from_std(
                deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600)),
            ));
            let ev = tokio::select! {
                biased;
                a = answers.recv() => Ev::Answer(a),
                _ = late, if deadline.is_some() => Ev::Late,
                r = next_request, if can_send => Ev::Request(r),
            };
            match ev {
                Ev::Answer(None) => return self.fail(net_err("the append stream closed")),
                Ev::Answer(Some(Err(e))) => return self.fail(net_err(e.to_string())),
                Ev::Answer(Some(Ok((session, answer)))) => {
                    if session != self.ctx.id {
                        // An answer to a session given up on.
                        continue;
                    }
                    let Some(f) = self.inflight.pop_front() else {
                        return self.fail(net_err("the follower answered a request never sent"));
                    };
                    self.inflight_bytes -= f.bytes;
                    if let Some(next) = self.inflight.front_mut() {
                        next.deadline = next.deadline.max(Instant::now() + next.ttl);
                    }
                    if f.entries > 0 {
                        super::state_machine::stage_add(6, f.sent_at.elapsed());
                    }
                    match answer {
                        Ok(Ok(matching)) if !f.last => {
                            let _ = matching;
                            continue;
                        }
                        Ok(Ok(matching)) => return Some(Ok(Ok(matching))),
                        // A conflict or a higher vote ends the session on
                        // both sides; the connection stays good.
                        Ok(Err(e)) => {
                            self.done = true;
                            return Some(Ok(Err(e)));
                        }
                        Err(fatal) => {
                            return self.fail(RPCError::Unreachable(Unreachable::new(
                                &io::Error::other(format!(
                                    "node {} failed: {fatal}",
                                    self.ctx.target
                                )),
                            )))
                        }
                    }
                }
                Ev::Late => {
                    let f = self.inflight.front().expect("a deadline has a frame");
                    return self.fail(net_err(format!(
                        "node {}: no answer to an append of {} entries ({} bytes) within {:?}",
                        self.ctx.target,
                        f.entries,
                        f.bytes,
                        f.deadline.saturating_duration_since(f.sent_at)
                    )));
                }
                Ev::Request(None) => self.input = None,
                Ev::Request(Some(req)) => {
                    if let Err(e) = self.send(req).await {
                        return self.fail(e);
                    }
                }
            }
        }
    }

    /// Send one openraft request: compressed entries where the follower reads
    /// them, cut into frames of at most [`wire::MAX_APPEND_BYTES`].
    async fn send(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<(), RPCError<TypeConfig>> {
        let zstd = self.conn.reads_zstd && wire::zstd_from_env() && !req.entries.is_empty();
        if zstd {
            wire::prepare_zstd(&req).await.map_err(|e| {
                tracing::error!(target: "rsm", target_node = self.ctx.target, error = %e, "raft append does not compress");
                net_err(e.to_string())
            })?;
        }
        let parts = split(&req, zstd).map_err(|e| {
            tracing::error!(target: "rsm", target_node = self.ctx.target, error = %e, "raft append does not encode");
            net_err(e.to_string())
        })?;
        let count = parts.len();
        let members = self.ctx.members.header();
        let members: &[u8] = members.as_ref().map(|v| v.as_bytes()).unwrap_or(&[]);
        for (i, (entries, body)) in parts.into_iter().enumerate() {
            let f = append_frame(self.ctx.id, members, &body);
            let bytes = f.len();
            let now = Instant::now();
            let ttl = append_ttl(self.ctx.soft, entries, bytes);
            let own = now + ttl;
            // A follower that stopped reading (its window full) is late for
            // the oldest frame it has not answered, or for this one.
            let limit = self.inflight.front().map_or(own, |f| f.deadline.min(own));
            match tokio::time::timeout_at(
                tokio::time::Instant::from_std(limit),
                self.conn.tx.send(f),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(net_err("the append stream closed")),
                Err(_) => {
                    return Err(net_err(format!(
                        "node {}: the append stream took no bytes within {:?}",
                        self.ctx.target,
                        limit.saturating_duration_since(now)
                    )))
                }
            }
            self.inflight.push_back(InFlight {
                bytes,
                sent_at: now,
                ttl,
                deadline: own,
                entries,
                last: i + 1 == count,
            });
            self.inflight_bytes += bytes;
        }
        Ok(())
    }
}

/// `req` as the wire bodies of one or more requests of at most
/// [`wire::MAX_APPEND_BYTES`] each (at least one entry each), with their
/// entry counts: each continues the previous one (`prev_log_id` = the last
/// entry before it) under the same vote and commit point.
pub(crate) fn split(
    req: &AppendEntriesRequest<TypeConfig>,
    zstd: bool,
) -> io::Result<Vec<(usize, Vec<u8>)>> {
    let mut out = Vec::new();
    let mut at = 0usize;
    loop {
        let prev = match at {
            0 => req.prev_log_id,
            i => Some(req.entries[i - 1].log_id),
        };
        let part = AppendEntriesRequest::<TypeConfig> {
            vote: req.vote,
            prev_log_id: prev,
            entries: req.entries[at..].to_vec(),
            leader_commit: req.leader_commit,
        };
        let (body, n) = wire::encode_append(&part, zstd)?;
        if n == 0 && !part.entries.is_empty() {
            return Err(io::Error::other("an append encoded no entry"));
        }
        at += n;
        out.push((n, body));
        if at >= req.entries.len() {
            return Ok(out);
        }
    }
}

// ---------------------------------------------------------------------------
// The follower
// ---------------------------------------------------------------------------

/// Serve one stream connection: cut the request body into frames, feed each
/// session's requests to `Raft::stream_append` in order, and write its
/// answers to `out` (the response body), tagged with the session.
pub(crate) async fn serve<S: crate::rsm::store::Store + 'static>(
    raft: super::RaftHandle<S>,
    members: Arc<super::members::MembersState>,
    body: Body,
    out: mpsc::Sender<Bytes>,
) {
    use openraft::async_runtime::WatchReceiver;
    let mut body = body.into_data_stream();
    let mut metrics = raft.metrics();
    let mut d = Deframer::default();
    // The current session: its number and the sender of its requests
    // (`None` once its answers ended: its later requests are dropped).
    let mut session: Option<(u64, Option<SessionTx>)> = None;
    loop {
        loop {
            let (tag, f) = match d.next_frame() {
                Ok(Some(x)) => x,
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(target: "rsm", error = %e, "raft append stream: a bad frame; closing");
                    return;
                }
            };
            if tag != TAG_APPEND {
                tracing::warn!(target: "rsm", tag, "raft append stream: an unexpected frame; closing");
                return;
            }
            let t0 = Instant::now();
            let (sid, view, append) = match parse_append(f) {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!(target: "rsm", error = %e, "raft append stream: a bad frame; closing");
                    return;
                }
            };
            if !view.is_empty() {
                members.receive(&view);
            }
            let req = match wire::decode_append(&append) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(target: "rsm", error = %e, "raft append stream: an append does not decode; closing");
                    return;
                }
            };
            if !req.entries.is_empty() {
                super::state_machine::stage_add(4, t0.elapsed());
            }
            if session.as_ref().map(|s| s.0) != Some(sid) {
                // A new session: the previous one's requests end here.
                let (itx, irx) = mpsc::channel::<(AppendEntriesRequest<TypeConfig>, Instant)>(64);
                let (stx, srx) = mpsc::unbounded_channel::<(Instant, bool)>();
                let input = futures_util::stream::unfold((irx, stx), |(mut irx, stx)| async move {
                    let (req, at) = irx.recv().await?;
                    let _ = stx.send((at, !req.entries.is_empty()));
                    Some((req, (irx, stx)))
                });
                let answers = raft.stream_append(input);
                tokio::spawn(crate::obs::panic_policy::non_core(answer(
                    sid,
                    answers,
                    srx,
                    out.clone(),
                )));
                session = Some((sid, Some(itx)));
            }
            if let Some((_, Some(tx))) = session.as_mut() {
                if tx.send((req, t0)).await.is_err() {
                    // Its answers ended (a conflict, a higher vote): what the
                    // leader sent after that is dropped with it.
                    session.as_mut().expect("session").1 = None;
                }
            }
        }
        tokio::select! {
            chunk = body.next() => match chunk {
                Some(Ok(chunk)) => d.push(&chunk),
                // The leader closed the stream (or it broke): end every session.
                _ => return,
            },
            // This node's Raft stopped: close the stream, so the server's
            // graceful shutdown does not wait on the leader to.
            r = metrics.changed() => {
                if r.is_err() || metrics.borrow_watched().running_state.is_err() {
                    return;
                }
            }
        }
    }
}

/// Write one session's answers, in order, until they end.
async fn answer(
    session: u64,
    answers: impl Stream<Item = Result<StreamAppendResult<TypeConfig>, openraft::errors::Fatal<TypeConfig>>>
        + Send
        + 'static,
    mut sent: mpsc::UnboundedReceiver<(Instant, bool)>,
    out: mpsc::Sender<Bytes>,
) {
    futures_util::pin_mut!(answers);
    while let Some(r) = answers.next().await {
        if let Ok((at, entries)) = sent.try_recv() {
            if entries {
                super::state_machine::stage_add(5, at.elapsed());
            }
        }
        let a: Answer = r.map_err(|f| f.to_string());
        let end = !matches!(a, Ok(Ok(_)));
        if out.send(answer_frame(session, &a)).await.is_err() || end {
            return;
        }
    }
}

/// The request stream's entries (tests).
#[cfg(test)]
fn entries_of(req: &AppendEntriesRequest<TypeConfig>) -> Vec<u64> {
    req.entries
        .iter()
        .map(|e: &REntry| e.log_id.index)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rsm::replicator::raft::types::{log_id, AppEntry, Vote};

    #[test]
    fn frames_survive_any_chunking() {
        let a = append_frame(7, b"view", b"the append body");
        let b = answer_frame(7, &Ok(Ok(Some(log_id(3, 41)))));
        let c = answer_frame(8, &Err("stopped".into()));
        let all: Vec<u8> = [a.as_ref(), b.as_ref(), c.as_ref()].concat();
        for step in [1usize, 2, 3, 5, 7, 64, all.len()] {
            let mut d = Deframer::default();
            let mut got = Vec::new();
            for chunk in all.chunks(step) {
                d.push(chunk);
                while let Some(f) = d.next_frame().unwrap() {
                    got.push(f);
                }
            }
            assert_eq!(got.len(), 3, "chunks of {step}");
            assert_eq!(got[0].0, TAG_APPEND);
            let (sid, view, body) = parse_append(got[0].1.clone()).unwrap();
            assert_eq!(
                (sid, &view[..], &body[..]),
                (7, &b"view"[..], &b"the append body"[..])
            );
            let (sid, ans) = parse_answer(got[1].1.clone()).unwrap();
            assert_eq!(sid, 7);
            assert_eq!(ans, Ok(Ok(Some(log_id(3, 41)))));
            let (sid, ans) = parse_answer(got[2].1.clone()).unwrap();
            assert_eq!((sid, ans), (8, Err("stopped".into())));
        }
    }

    #[test]
    fn an_absurd_frame_length_is_refused() {
        let mut d = Deframer::default();
        d.push(&u32::MAX.to_le_bytes());
        assert!(d.next_frame().is_err());
        let mut d = Deframer::default();
        d.push(&0u32.to_le_bytes());
        assert!(d.next_frame().is_err());
    }

    fn blank(i: u64) -> REntry {
        REntry {
            log_id: log_id(2, i),
            payload: EntryPayload::Blank,
        }
    }

    #[test]
    fn a_request_is_split_into_requests_that_continue_each_other() {
        let req = AppendEntriesRequest::<TypeConfig> {
            vote: Vote::new_committed(2, 1),
            prev_log_id: Some(log_id(2, 9)),
            entries: (10..14).map(blank).collect(),
            leader_commit: Some(log_id(2, 8)),
        };
        let parts = split(&req, false).unwrap();
        assert_eq!(parts.len(), 1, "small: one frame");
        let back = wire::decode_append(&Bytes::from(parts[0].1.clone())).unwrap();
        assert_eq!(entries_of(&back), vec![10, 11, 12, 13]);
        assert_eq!(back.prev_log_id, Some(log_id(2, 9)));

        // An empty request (a heartbeat, a commit update) is one frame.
        let hb = AppendEntriesRequest::<TypeConfig> {
            entries: Vec::new(),
            ..req.clone()
        };
        let parts = split(&hb, false).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].0, 0);

        // Entries over the cap: each frame continues the one before.
        let big = |i: u64| {
            let mut e = crate::rsm::entry::Entry::new(1, 0, 0);
            e.add_command(
                [i as u8; 16],
                crate::rsm::entry::Outcome::Empty,
                vec![crate::rsm::effect::Effect::Append {
                    pid: i,
                    bucket: 0,
                    base_offset: 0,
                    count: 1,
                    created_at_us: 1,
                    hashes: vec![0; 16],
                    blob: vec![7u8; wire::MAX_APPEND_BYTES / 3],
                }],
            )
            .expect("a command");
            let app = AppEntry::proposed(Arc::new(e), Vec::new());
            REntry {
                log_id: log_id(2, i),
                payload: EntryPayload::Normal(app),
            }
        };
        let req = AppendEntriesRequest::<TypeConfig> {
            vote: Vote::new_committed(2, 1),
            prev_log_id: Some(log_id(2, 9)),
            entries: (10..16).map(big).collect(),
            leader_commit: Some(log_id(2, 8)),
        };
        let parts = split(&req, false).unwrap();
        assert!(parts.len() >= 3, "{} frames", parts.len());
        let mut prev = Some(log_id(2, 9));
        let mut seen = Vec::new();
        for (n, body) in parts {
            assert!(body.len() <= wire::MAX_APPEND_BYTES + (1 << 16));
            let r = wire::decode_append(&Bytes::from(body)).unwrap();
            assert_eq!(r.entries.len(), n);
            assert_eq!(r.prev_log_id, prev, "each frame continues the one before");
            assert_eq!(r.leader_commit, Some(log_id(2, 8)));
            prev = r.entries.last().map(|e| e.log_id);
            seen.extend(entries_of(&r));
        }
        assert_eq!(seen, (10..16).collect::<Vec<_>>());
    }
}
