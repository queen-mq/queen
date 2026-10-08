//! The standby's follower: the task that reads the source and hands what it
//! reads to the batcher.
//!
//! One runs on every node that has a source configured, and does nothing
//! unless its node LEADS a cluster whose role row says standby: only a leader
//! builds entries. While it does, it asks the source for the application
//! entries after the standby's position ([`super::wire`]), rebuilds each one
//! ([`super::wire::full_entry`]) and submits it to the batcher's link channel
//! in order, a whole answer in the pipeline at a time. The batcher owns the
//! rest: whether an entry follows the standby's state, the entry it becomes,
//! and the position, which is written by the entry itself.
//!
//! So this task holds nothing that matters. Whatever happens — the source is
//! unreachable, this node stops leading, the process dies — it starts again
//! from the position committed state holds, on whichever node leads.
//!
//! # What stops it
//!
//! - **Not this node's turn**: another node leads, or the cluster is not a
//!   standby (never one, or promoted). It waits.
//! - **Something that passes**: the source does not answer, a member of this
//!   cluster cannot read what the source's entry raises the cluster version
//!   to, the leadership moved under a batch. It tries again, backing off.
//! - **Something that does not pass** ([`State::Halted`]): the source purged
//!   the entries after the standby's position, or the two logs are not one
//!   (the source's entry at the standby's position has another term, or an
//!   entry does not continue the standby's state). The standby needs a new
//!   seed. The task keeps asking, slowly: the answer is the same until an
//!   operator acts, and it must not take the cluster's log with it.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::watch;

use super::wire::{
    entry_weight, full_entry, Answer, Request, SourceEntry, MAX_BYTES_DEFAULT, READ_PATH,
    TOKEN_HEADER,
};
use super::{Position, Role as LinkRole};
use crate::rsm::batcher::{
    LinkOp, LinkSubmission, LinkTx, Reply, DIVERGED_CODE, MEMBER_BEHIND_CODE, NOT_STANDBY_CODE,
    OUT_OF_SEQUENCE_CODE,
};
use crate::rsm::entry::Entry;
use crate::rsm::replicator::Role;
use crate::rsm::store::Store;

/// How long the source holds a read that has nothing to send (the standby's
/// latency floor when the source is idle is one round trip, not this).
const WAIT_MS: u64 = 2_000;
/// How often an idle task looks at its node's role and its cluster's.
const IDLE_POLL: Duration = Duration::from_millis(200);
/// The most rebuilt source entries between this task and their apply, by
/// their weight in memory ([`entry_weight`]) and by their number: the
/// follower's memory, and how deep it feeds the batcher's pipeline
/// (`QUEEN_LINK_PIPELINE`).
const WINDOW_BYTES: usize = 64 << 20;
const WINDOW_ENTRIES: usize = 256;
/// How much of an answer is rebuilt in one blocking task, in stored bytes and
/// in entries (one entry at least).
const REBUILD_BYTES: usize = 1 << 20;
const REBUILD_ENTRIES: usize = 32;
/// How often the source nodes that are not being read are told the standby's
/// position ([`Request::hold_only`]). Far below the source's hold on its log
/// (`QUEEN_LINK_HOLD_S`), so a node is told many times before it would let go.
const HOLD_EVERY: Duration = Duration::from_secs(5);
/// How long a halted task waits before it asks the source again (the tests
/// do not wait that long).
const HALT_POLL: Duration = if cfg!(test) {
    Duration::from_millis(200)
} else {
    Duration::from_secs(10)
};

/// What a node needs to follow a source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkConfig {
    /// The source's nodes, each the address of its Raft RPC server
    /// (`host:port`, as `QUEEN_RAFT_PEERS` writes them). One is read at a
    /// time; the others are tried when it fails or falls behind.
    pub sources: Vec<String>,
    /// The source's `QUEEN_LINK_TOKEN`.
    pub token: Option<String>,
    /// This standby's name on the source — its hold on the source's log, and
    /// what the source's status calls it — instead of the one made of the
    /// standby's id ([`super::reader_name`]). The same on every node of the
    /// standby, and no other standby of the same source may carry it.
    pub name: Option<String>,
}

impl LinkConfig {
    /// `QUEEN_LINK_SOURCE` (comma-separated `host:port`), `QUEEN_LINK_SOURCE_TOKEN`
    /// and `QUEEN_LINK_NAME` (optional). `Ok(None)`: no source is configured.
    pub fn from_env() -> Result<Option<LinkConfig>, String> {
        let raw = std::env::var("QUEEN_LINK_SOURCE").unwrap_or_default();
        let sources = parse_sources(&raw)?;
        if sources.is_empty() {
            return Ok(None);
        }
        let token = std::env::var("QUEEN_LINK_SOURCE_TOKEN")
            .ok()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty());
        let name = std::env::var("QUEEN_LINK_NAME")
            .ok()
            .map(|n| n.trim().to_string())
            .filter(|n| !n.is_empty());
        Ok(Some(LinkConfig {
            sources,
            token,
            name,
        }))
    }

    /// The source as the role row names it: the configured addresses.
    pub fn label(&self) -> String {
        self.sources.join(",")
    }
}

/// `host:port[,host:port...]`, each with or without `http://`.
pub fn parse_sources(raw: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for part in raw.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let addr = part.strip_prefix("http://").unwrap_or(part);
        let addr = addr.trim_end_matches('/');
        if addr.starts_with("https://") {
            return Err(format!(
                "QUEEN_LINK_SOURCE: `{part}`: the Raft RPC port speaks plain HTTP"
            ));
        }
        match addr.rsplit_once(':') {
            Some((host, port)) if !host.is_empty() && port.parse::<u16>().is_ok() => {
                out.push(addr.to_string())
            }
            _ => {
                return Err(format!(
                    "QUEEN_LINK_SOURCE: `{part}` is not `host:port` (a source node's Raft RPC address)"
                ))
            }
        }
    }
    Ok(out)
}

/// How a node takes part in a cluster link: what `QUEEN_LINK_*` says, or what
/// a test gives instead.
#[derive(Clone, Default)]
pub struct LinkSetup {
    /// The source this node's cluster follows while it is a standby.
    pub source: Option<LinkConfig>,
    /// `QUEEN_LINK_STANDBY`: an EMPTY cluster started with it becomes a
    /// standby of `source`. A cluster that holds entries of its own, or was
    /// promoted, ignores it ([`crate::rsm::batcher::LinkBoot`]), so it can
    /// stay set for the standby's whole life.
    pub standby: bool,
    /// `QUEEN_LINK_SEED=<node id>`: the node of this cluster that takes its
    /// first state from the source's snapshot ([`super::seed`]) — once, while
    /// its directory holds no data and no node of its cluster holds any. The
    /// cluster's other nodes, while fresh, wait to be added to it.
    pub seed: Option<crate::rsm::replicator::NodeId>,
    /// How a source node is read. `None`: over HTTP ([`http_fetch`]).
    pub fetch: Option<Fetch>,
}

impl LinkSetup {
    pub fn from_env() -> Result<LinkSetup, String> {
        let source = LinkConfig::from_env()?;
        let standby = std::env::var("QUEEN_LINK_STANDBY").is_ok_and(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "on" | "yes"
            )
        });
        if standby && source.is_none() {
            return Err(
                "QUEEN_LINK_STANDBY needs QUEEN_LINK_SOURCE: a standby replays a source".into(),
            );
        }
        let seed = match std::env::var("QUEEN_LINK_SEED") {
            Ok(v) if !v.trim().is_empty() => Some(v.trim().parse().map_err(|_| {
                format!(
                    "QUEEN_LINK_SEED={v}: expected the id of the one node that takes the seed"
                )
            })?),
            _ => None,
        };
        if seed.is_some() && source.is_none() {
            return Err(
                "QUEEN_LINK_SEED needs QUEEN_LINK_SOURCE: a seed is the source's snapshot".into(),
            );
        }
        Ok(LinkSetup {
            source,
            standby,
            seed,
            fetch: None,
        })
    }

    /// What makes an empty cluster a standby at its first leadership.
    pub fn boot(&self) -> Option<crate::rsm::batcher::LinkBoot> {
        match (&self.source, self.standby) {
            (Some(source), true) => Some(crate::rsm::batcher::LinkBoot::empty(&source.label())),
            _ => None,
        }
    }
}

/// One read of a source node: its address and the request, to the answer. An
/// `Err` is a transport failure in words (the node is tried again, or another
/// is).
pub type Fetch = Arc<
    dyn Fn(String, Request) -> Pin<Box<dyn Future<Output = Result<Answer, String>> + Send>>
        + Send
        + Sync,
>;

/// An error and what caused it, in one line: the HTTP client's own message
/// stops at "client error (Connect)", and what an operator needs is under it
/// (refused, unreachable, a name that does not resolve).
fn chain(e: &(dyn std::error::Error + 'static)) -> String {
    let mut out = e.to_string();
    let mut cause = e.source();
    while let Some(c) = cause {
        out.push_str(": ");
        out.push_str(&c.to_string());
        cause = c.source();
    }
    out
}

/// Read a source node over HTTP: `POST http://<addr>/link/v1/read` with the
/// link token.
pub fn http_fetch(token: Option<String>) -> Fetch {
    use axum::body::Body;
    use axum::http::{header, Method, StatusCode};
    use http_body_util::BodyExt;

    let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
    connector.set_nodelay(true);
    connector.set_connect_timeout(Some(Duration::from_secs(2)));
    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .pool_idle_timeout(Duration::from_secs(90))
        .build::<_, Body>(connector);
    let token: Option<Arc<str>> = token.map(Arc::from);

    Arc::new(move |addr: String, req: Request| {
        let client = client.clone();
        let token = token.clone();
        Box::pin(async move {
            let url = format!("http://{addr}{READ_PATH}");
            // The source holds the read up to `wait_ms`; the answer may then
            // be megabytes.
            let ttl = Duration::from_millis(req.wait_ms) + Duration::from_secs(20);
            let body = serde_json::to_vec(&req).map_err(|e| e.to_string())?;
            let mut http = axum::http::Request::builder()
                .method(Method::POST)
                .uri(&url)
                .header(header::CONTENT_TYPE, "application/json");
            if let Some(t) = &token {
                http = http.header(TOKEN_HEADER, &**t);
            }
            let http = http
                .body(Body::from(body))
                .map_err(|e| format!("{url}: {e}"))?;
            let resp = match tokio::time::timeout(ttl, client.request(http)).await {
                Err(_) => return Err(format!("{url}: no answer within {ttl:?}")),
                Ok(Err(e)) => return Err(format!("{url}: {}", chain(&e))),
                Ok(Ok(r)) => r,
            };
            let status = resp.status();
            let bytes: Bytes = match tokio::time::timeout(ttl, resp.into_body().collect()).await {
                Err(_) => return Err(format!("{url}: the answer stalled")),
                Ok(Err(e)) => return Err(format!("{url}: {e}")),
                Ok(Ok(b)) => b.to_bytes(),
            };
            match status {
                StatusCode::OK => {
                    Answer::decode(&bytes).map_err(|e| format!("{url}: {e}"))
                }
                StatusCode::UNAUTHORIZED => Err(format!(
                    "{url}: the source refused this standby's QUEEN_LINK_SOURCE_TOKEN"
                )),
                StatusCode::NOT_FOUND => Err(format!(
                    "{url}: the source serves no link (QUEEN_LINK_TOKEN is not set on it, or it \
                     runs a release without one)"
                )),
                other => Err(format!(
                    "{url}: {other}: {}",
                    String::from_utf8_lossy(&bytes[..bytes.len().min(256)])
                )),
            }
        })
    })
}

/// What the follower is doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// Nothing: this node does not lead, or its cluster is not a standby.
    Idle,
    /// Reading the source and replaying it.
    Following,
    /// Held up by something that passes; `error` says what.
    Waiting,
    /// Stopped by something that does not pass; `error` says what. The
    /// standby needs a new seed.
    Halted,
}

impl State {
    pub fn name(&self) -> &'static str {
        match self {
            State::Idle => "idle",
            State::Following => "following",
            State::Waiting => "waiting",
            State::Halted => "halted",
        }
    }
}

/// What the follower last knew: a status page's and a metric's source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    pub state: State,
    /// The source node being read.
    pub source: String,
    /// The last source entry this follower saw committed here: in the
    /// standby's log for good, and applied a moment later.
    pub position: Position,
    /// How far into the source's log this follower has read and replayed:
    /// the position, or past it when consensus-internal entries (which are
    /// not replayed) follow it in the source's log.
    pub scanned: u64,
    /// How far the source node last read had applied.
    pub source_applied: u64,
    /// Why the follower waits or stopped.
    pub error: Option<String>,
    /// Source entries replayed since this process started.
    pub entries: u64,
    /// µs since the epoch of the last answer from the source.
    pub last_answer_us: i64,
}

impl Status {
    /// Entries of the source's log this standby had still to read when the
    /// source last answered.
    pub fn lag_entries(&self) -> u64 {
        self.source_applied
            .saturating_sub(self.scanned.max(self.position.index))
    }

    /// How old the standby's state is against `now_us`: the age of the last
    /// entry it applied, while the source is known to hold more; 0 when it
    /// has everything the source had.
    pub fn lag_us(&self, now_us: i64) -> i64 {
        if self.lag_entries() == 0 {
            return 0;
        }
        now_us.saturating_sub(self.position.now_us).max(0)
    }
}

fn wall_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as i64)
}

/// The running follower: its status, and its stop.
pub struct Follower {
    status: Mutex<Status>,
    stop: tokio::sync::Notify,
    stopped: std::sync::atomic::AtomicBool,
}

impl Follower {
    pub fn status(&self) -> Status {
        self.status.lock().expect("link status").clone()
    }

    /// Stop the task (the node is shutting down).
    pub fn stop(&self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Release);
        self.stop.notify_waiters();
    }

    fn is_stopped(&self) -> bool {
        self.stopped.load(std::sync::atomic::Ordering::Acquire)
    }

    fn set(&self, f: impl FnOnce(&mut Status)) {
        f(&mut self.status.lock().expect("link status"));
    }

    fn state(&self, state: State, error: Option<String>) {
        self.set(|s| {
            if s.state != state || s.error != error {
                match (&error, state) {
                    (Some(why), State::Halted) => {
                        tracing::error!(target: "rsm", why, "rsm link: the standby stopped following its source")
                    }
                    (Some(why), _) => {
                        tracing::warn!(target: "rsm", why, "rsm link: the standby waits")
                    }
                    (None, State::Following) => {
                        tracing::info!(target: "rsm", source = %s.source, index = s.position.index, "rsm link: following the source")
                    }
                    _ => {}
                }
            }
            s.state = state;
            s.error = error;
        });
    }
}

/// Why a stretch of following ended.
enum Ended {
    /// Not this node's turn any more (leadership, a promotion, a shutdown):
    /// look again.
    Turn,
    /// The position must be read again before anything else is sent.
    Resync,
    /// Something that passes.
    Retry(String),
    /// Something that does not.
    Halt(String),
}

/// Start the follower of `cfg`'s source on the current runtime. It ends when
/// the batcher's link channel closes, the store is gone or
/// [`Follower::stop`] is called. The store is held weakly: a node that shuts
/// down takes its store back by value.
pub fn spawn<S: Store + 'static>(
    cfg: LinkConfig,
    store: Weak<S>,
    role: watch::Receiver<Role>,
    link: LinkTx,
    fetch: Fetch,
) -> Arc<Follower> {
    let follower = Arc::new(Follower {
        status: Mutex::new(Status {
            state: State::Idle,
            source: cfg.sources.first().cloned().unwrap_or_default(),
            position: Position::START,
            scanned: 0,
            source_applied: 0,
            error: None,
            entries: 0,
            last_answer_us: 0,
        }),
        stop: tokio::sync::Notify::new(),
        stopped: std::sync::atomic::AtomicBool::new(false),
    });
    let me = follower.clone();
    // Non-core: a panic in the follower stops the link, never the node.
    tokio::spawn(crate::obs::panic_policy::non_core(async move {
        run(cfg, store, role, link, fetch, me).await;
    }));
    follower
}

/// The role row and the position, read off the runtime. `None`: the store is
/// gone (the node is shutting down).
async fn read_state<S: Store + 'static>(
    store: &Weak<S>,
) -> Option<Result<(LinkRole, Position), String>> {
    let store = store.upgrade()?;
    Some(
        tokio::task::spawn_blocking(move || {
            store.read(|r| Ok((super::read_role(r)?, super::read_position(r)?)))
        })
        .await
        .map_err(|e| format!("link state read: {e}"))
        .and_then(|read| read.map_err(|e| e.to_string())),
    )
}

async fn run<S: Store + 'static>(
    cfg: LinkConfig,
    store: Weak<S>,
    mut role: watch::Receiver<Role>,
    link: LinkTx,
    fetch: Fetch,
    me: Arc<Follower>,
) {
    let mut source = 0usize;
    let mut backoff = Duration::from_millis(100);
    loop {
        if me.is_stopped() || link.is_closed() {
            return;
        }
        // Only the leader of a standby follows.
        let turn = if !role.borrow_and_update().is_leader() {
            Ok(None)
        } else {
            match read_state(&store).await {
                None => return,
                Some(Ok((LinkRole::Standby(doc), position))) => Ok(Some((
                    super::reader_name(cfg.name.as_deref(), &doc.id),
                    position,
                ))),
                Some(Ok(_)) => Ok(None),
                Some(Err(e)) => Err(e),
            }
        };
        let (reader, position) = match turn {
            Ok(Some(turn)) => turn,
            not_now => {
                match not_now {
                    Err(e) => me.state(State::Waiting, Some(e)),
                    _ => me.state(State::Idle, None),
                }
                tokio::select! {
                    changed = role.changed() => {
                        // The node's replicator is gone (it stopped, to load
                        // a received snapshot or for good): nothing to follow
                        // for, and a closed watch answers at once, every time.
                        if changed.is_err() {
                            return;
                        }
                    }
                    _ = tokio::time::sleep(IDLE_POLL) => {}
                    _ = me.stop.notified() => {}
                }
                continue;
            }
        };
        me.set(|s| {
            s.position = position;
            s.scanned = position.index;
        });

        let ended = follow(
            &cfg,
            &reader,
            &role,
            &link,
            &fetch,
            &me,
            position,
            &mut source,
        )
        .await;
        let nap = match ended {
            Ended::Turn => {
                backoff = Duration::from_millis(100);
                Duration::ZERO
            }
            Ended::Resync => Duration::from_millis(20),
            Ended::Retry(why) => {
                me.state(State::Waiting, Some(why));
                let nap = backoff;
                backoff = (backoff * 2).min(Duration::from_secs(5));
                nap
            }
            Ended::Halt(why) => {
                me.state(State::Halted, Some(why));
                HALT_POLL
            }
        };
        if !nap.is_zero() {
            tokio::select! {
                _ = tokio::time::sleep(nap) => {}
                _ = me.stop.notified() => {}
            }
        }
    }
}

/// Follow the source from `position`, as `reader`, until something ends it.
#[allow(clippy::too_many_arguments)]
async fn follow(
    cfg: &LinkConfig,
    reader: &str,
    role: &watch::Receiver<Role>,
    link: &LinkTx,
    fetch: &Fetch,
    me: &Arc<Follower>,
    mut position: Position,
    source: &mut usize,
) -> Ended {
    // Source nodes that answered behind this standby, in a row: once every
    // node has, the source itself is behind.
    let mut behind = 0usize;
    // When the source's other nodes were last told how far this standby got.
    let mut held: Option<tokio::time::Instant> = None;
    loop {
        if me.is_stopped() || link.is_closed() || !role.borrow().is_leader() {
            return Ended::Turn;
        }
        let addr = cfg.sources[*source % cfg.sources.len()].clone();
        me.set(|s| s.source = addr.clone());
        // One node is read; the others are told the position now and then,
        // so each keeps the entries after it and can take the reads over.
        if held.is_none_or(|at| at.elapsed() >= HOLD_EVERY) {
            held = Some(tokio::time::Instant::now());
            for other in cfg.sources.iter().filter(|s| **s != addr) {
                let (fetch, other) = (fetch.clone(), other.clone());
                let hold = Request {
                    after: position.index,
                    after_term: position.term,
                    max_bytes: 0,
                    wait_ms: 0,
                    reader: reader.to_string(),
                    hold_only: true,
                };
                // Unanswered, it changes nothing here: the node is told
                // again at the next round.
                tokio::spawn(async move {
                    let _ = fetch(other, hold).await;
                });
            }
        }
        let req = Request {
            after: position.index,
            after_term: position.term,
            max_bytes: MAX_BYTES_DEFAULT as u64,
            wait_ms: WAIT_MS,
            reader: reader.to_string(),
            hold_only: false,
        };
        // A stop must not wait out a held read: the waiter is registered
        // before the flag is looked at, so a stop between the two is seen.
        let stopping = me.stop.notified();
        tokio::pin!(stopping);
        if me.is_stopped() {
            return Ended::Turn;
        }
        let answer = tokio::select! {
            answer = fetch(addr.clone(), req) => answer,
            _ = &mut stopping => return Ended::Turn,
        };
        let answer = match answer {
            Ok(a) => a,
            Err(e) => {
                *source += 1;
                return Ended::Retry(e);
            }
        };
        match answer {
            Answer::Entries {
                entries,
                upto,
                applied,
                ..
            } => {
                me.set(|s| {
                    s.source_applied = applied;
                    s.last_answer_us = wall_us();
                });
                if applied < position.index {
                    // This node of the source has not applied what the
                    // standby holds: a follower behind — or a source that
                    // lost entries, if every node says so.
                    *source += 1;
                    behind += 1;
                    if behind >= cfg.sources.len() {
                        return Ended::Retry(format!(
                            "the source has applied {applied} and this standby is at {}: no \
                             node of the source is as far as the standby",
                            position.index
                        ));
                    }
                    continue;
                }
                behind = 0;
                me.state(State::Following, None);
                if !entries.is_empty() {
                    if let Some(ended) = replay(link, me, &mut position, entries).await {
                        return ended;
                    }
                }
                // Everything the answer covered is here now.
                me.set(|s| s.scanned = s.scanned.max(upto));
            }
            Answer::Purged { purged, applied } => {
                me.set(|s| {
                    s.source_applied = applied;
                    s.last_answer_us = wall_us();
                });
                *source += 1;
                return Ended::Halt(format!(
                    "the source node {addr} purged its log up to {purged} and this standby is at \
                     {}: it needs a new seed",
                    position.index
                ));
            }
            Answer::Mismatch {
                index, source_term, ..
            } => {
                me.set(|s| s.last_answer_us = wall_us());
                return Ended::Halt(format!(
                    "the source's entry {index} has term {} and this standby replayed term {}: \
                     it followed another log and needs a new seed",
                    source_term.map_or("unknown".to_string(), |t| t.to_string()),
                    position.term
                ));
            }
        }
    }
}

/// A source entry handed to the batcher and not yet answered.
struct Sent {
    index: u64,
    term: u64,
    now_us: i64,
    weight: usize,
    reply: tokio::sync::oneshot::Receiver<Reply>,
}

/// Replay one answer's entries, in order. `None`: every one applied, and
/// `position` is the last.
///
/// An answer is megabytes of stored entries, and many times that once its
/// payloads are decompressed. So the entries are rebuilt a few at a time and
/// handed to the batcher through a WINDOW: at most [`WINDOW_BYTES`] of rebuilt
/// entries (and [`WINDOW_ENTRIES`] of them) are between this task and their
/// apply, which is what the follower holds in memory and how far ahead of its
/// commits it feeds the batcher's pipeline.
async fn replay(
    link: &LinkTx,
    me: &Arc<Follower>,
    position: &mut Position,
    entries: Vec<SourceEntry>,
) -> Option<Ended> {
    let mut rest = entries.into_iter().peekable();
    let mut sent: std::collections::VecDeque<Sent> = std::collections::VecDeque::new();
    let mut held = 0usize;
    let mut prev = position.index;
    // Why no more is sent. What was sent before is still answered first: the
    // position this function leaves is then the one the standby holds.
    let mut ended: Option<Ended> = None;
    loop {
        let room = held < WINDOW_BYTES && sent.len() < WINDOW_ENTRIES;
        if ended.is_none() && room && rest.peek().is_some() {
            let mut chunk = Vec::new();
            let mut stored = 0usize;
            while let Some(e) = rest.peek() {
                if !chunk.is_empty()
                    && (stored + e.stored.len() > REBUILD_BYTES || chunk.len() >= REBUILD_ENTRIES)
                {
                    break;
                }
                stored += e.stored.len();
                chunk.extend(rest.next());
            }
            // Rebuilding decompresses every payload: off the runtime.
            let rebuilt = tokio::task::spawn_blocking(move || {
                chunk
                    .into_iter()
                    .map(|e| full_entry(&e.stored).map(|entry| (e.index, e.term, entry)))
                    .collect::<std::io::Result<Vec<(u64, u64, Entry)>>>()
            })
            .await;
            match rebuilt {
                Ok(Ok(rebuilt)) => {
                    for (index, term, entry) in rebuilt {
                        let (now_us, weight) = (entry.now_us, entry_weight(&entry));
                        let (sub, reply) = LinkSubmission::new(LinkOp::Mirror {
                            prev,
                            index,
                            term,
                            entry: Arc::new(entry),
                        });
                        if link.send(sub).await.is_err() {
                            return Some(Ended::Turn);
                        }
                        sent.push_back(Sent {
                            index,
                            term,
                            now_us,
                            weight,
                            reply,
                        });
                        held += weight;
                        prev = index;
                    }
                }
                // Damaged in transit, or a shape this build does not read:
                // the same read may come back whole from another node, or
                // after an upgrade.
                Ok(Err(e)) => {
                    ended = Some(Ended::Retry(format!("a source entry was not read: {e}")))
                }
                Err(e) => ended = Some(Ended::Retry(format!("link rebuild task: {e}"))),
            }
            continue;
        }
        let Some(s) = sent.pop_front() else {
            return ended;
        };
        held -= s.weight;
        match s.reply.await {
            Ok(Reply::Done { .. }) => {
                *position = Position {
                    index: s.index,
                    term: s.term,
                    now_us: s.now_us,
                };
                me.set(|st| {
                    st.position = *position;
                    st.entries += 1;
                });
            }
            // This node stopped leading: the next leader's follower goes on.
            Ok(Reply::Retry { .. }) | Err(_) => return Some(Ended::Turn),
            // What was sent after it does not follow the standby's position
            // any more: the batcher refuses each, and nothing is applied out
            // of its turn.
            Ok(Reply::Refused(r)) => {
                return Some(match r.code.as_str() {
                    NOT_STANDBY_CODE => Ended::Turn,
                    OUT_OF_SEQUENCE_CODE => Ended::Resync,
                    DIVERGED_CODE => Ended::Halt(r.message),
                    MEMBER_BEHIND_CODE => Ended::Retry(r.message),
                    _ => Ended::Retry(format!("{}: {}", r.code, r.message)),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_addresses_are_host_and_port() {
        assert_eq!(
            parse_sources(" a:7400, http://b:7400/ ,,c.d.svc:1 ").unwrap(),
            vec!["a:7400", "b:7400", "c.d.svc:1"]
        );
        assert!(parse_sources("").unwrap().is_empty());
        assert!(parse_sources("a").is_err());
        assert!(parse_sources("a:port").is_err());
        assert!(parse_sources(":7400").is_err());
        assert!(parse_sources("https://a:7400").is_err());
    }

    #[test]
    fn the_lag_is_zero_once_the_standby_has_what_the_source_had() {
        let mut s = Status {
            state: State::Following,
            source: "a:1".into(),
            position: Position {
                index: 10,
                term: 1,
                now_us: 1_000,
            },
            scanned: 10,
            source_applied: 10,
            error: None,
            entries: 0,
            last_answer_us: 0,
        };
        assert_eq!((s.lag_entries(), s.lag_us(9_000)), (0, 0));
        s.source_applied = 14;
        assert_eq!((s.lag_entries(), s.lag_us(9_000)), (4, 8_000));
        // The source's log ends with entries that are not replayed (a new
        // leader's blank entry): read, so nothing is owed.
        s.scanned = 14;
        assert_eq!((s.lag_entries(), s.lag_us(9_000)), (0, 0));
    }
}
