//! The Raft network: HTTP between the cluster's nodes.
//!
//! Each node runs a small Raft RPC server (`QUEEN_RAFT_LISTEN`) on openraft's
//! own runtime, so client load never delays a vote or an append, and reaches
//! its peers with one pooled HTTP/1.1 client (the repository's one client
//! pattern, [`crate::peerclient`]: hyper-util, `nodelay`, no reqwest).
//!
//! | route | body | answer |
//! |---|---|---|
//! | `POST /raft/v1/append` | [`super::wire`] binary | `Result<AppendEntriesResponse, RaftError>` JSON |
//! | `POST /raft/v1/vote` | `VoteRequest` JSON | `Result<VoteResponse, RaftError>` JSON |
//! | `POST /raft/v1/prevote` | `VoteRequest` JSON | `Result<VoteResponse, RaftError>` JSON |
//! | `POST /raft/v1/transfer` | `TransferLeaderRequest` JSON | `Result<TransferLeaderResponse, RaftError>` JSON |
//! | `POST /raft/v1/snapshot` | [`super::snapshot`] stream | `Result<SnapshotResponse, RaftError>` JSON |
//! | `POST /raft/v1/members` | empty | the leader's `MembersView` JSON, 503 on a non-leader ([`super::members`]) |
//!
//! Every `append` from the leader also carries its members view in the
//! `x-queen-raft-members` header ([`super::members`]).
//!
//! A transport failure is `Unreachable` when the peer could not be connected
//! (openraft backs off) and a network error otherwise (openraft retries).
//! When `QUEEN_RAFT_TOKEN` is set every request carries it and every server
//! refuses a request without it.
//!
//! A single voter never sends an RPC (it is its own quorum): it runs
//! [`NoNetwork`].

use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::http::{header, Method, Request, StatusCode};
use http_body_util::BodyExt;
use openraft::errors::{
    NetworkError, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable,
};
use openraft::network::v2::RaftNetworkV2;
use openraft::network::{RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::OptionalSend;
use serde::de::DeserializeOwned;

use super::cluster::TOKEN_HEADER;
use super::state_machine::{Checkpoint, Snapshot};
use super::types::{Node, NodeId, TypeConfig, Vote};
use super::wire;

// ---------------------------------------------------------------------------
// A single voter: no network
// ---------------------------------------------------------------------------

fn no_peer() -> RPCError<TypeConfig> {
    RPCError::Unreachable(Unreachable::new(&io::Error::other(
        "no raft network: this node runs as a single voter",
    )))
}

#[derive(Clone, Default)]
pub(crate) struct NoNetwork;

impl RaftNetworkFactory<TypeConfig> for NoNetwork {
    type Network = NoPeer;

    async fn new_client(&mut self, _target: NodeId, _node: &Node) -> NoPeer {
        NoPeer
    }
}

pub(crate) struct NoPeer;

impl RaftNetworkV2<TypeConfig> for NoPeer {
    type SnapshotData = Checkpoint;

    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        Err(no_peer())
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        Err(no_peer())
    }

    async fn full_snapshot(
        &mut self,
        _vote: Vote,
        _snapshot: Snapshot,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        Err(StreamingError::from(no_peer()))
    }
}

// ---------------------------------------------------------------------------
// HTTP: the client side
// ---------------------------------------------------------------------------

pub(crate) type HttpClient =
    hyper_util::client::legacy::Client<hyper_util::client::legacy::connect::HttpConnector, Body>;

/// `QUEEN_RAFT_RPC_POOL`: idle connections kept per peer (default: no cap).
fn rpc_pool_from_env() -> usize {
    std::env::var("QUEEN_RAFT_RPC_POOL")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(usize::MAX)
        .max(1)
}

/// One pooled client for every peer.
pub(crate) fn http_client() -> HttpClient {
    let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
    connector.set_nodelay(true);
    connector.set_connect_timeout(Some(Duration::from_secs(2)));
    hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        // Idle connections kept per peer: `QUEEN_RAFT_RPC_POOL` (default: no cap,
        // hyper's own default). A connection the pool will not keep is closed after
        // its request, and a closed one sits in TIME_WAIT: whenever the forwards in
        // flight exceed the cap, the churn exhausts the ephemeral ports. Measured
        // twice: with a cap of 1024, ~2,500 concurrent forwards (the ordered pipeline,
        // 2,000 single-message producer shards) froze every request for ~35 s out of
        // each minute. Idle connections cost a socket each, nothing else. Earlier,
        // with 8 kept idle, every other request opened (and then
        // closed) a connection, and the TIME_WAIT sockets exhausted the ephemeral
        // ports in ~20 s at 100k msg/s — connect() then spun in the kernel on
        // every core of the follower.
        .pool_max_idle_per_host(rpc_pool_from_env())
        .pool_idle_timeout(Duration::from_secs(90))
        .build::<_, Body>(connector)
}

/// What a peer RPC can fail with, before it becomes openraft's error.
#[derive(Debug)]
pub(crate) enum Fail {
    /// The peer could not be connected (or refused this node): back off.
    Unreachable(String),
    /// Anything else in transit: retry.
    Network(String),
}

impl Fail {
    fn rpc(self) -> RPCError<TypeConfig> {
        match self {
            Fail::Unreachable(m) => RPCError::Unreachable(Unreachable::new(&io::Error::other(m))),
            Fail::Network(m) => RPCError::Network(NetworkError::new(&io::Error::other(m))),
        }
    }

    pub(crate) fn streaming(self) -> StreamingError<TypeConfig> {
        match self {
            Fail::Unreachable(m) => {
                StreamingError::Unreachable(Unreachable::new(&io::Error::other(m)))
            }
            Fail::Network(m) => StreamingError::Network(NetworkError::new(&io::Error::other(m))),
        }
    }
}

/// The network factory of a cluster node.
#[derive(Clone)]
pub(crate) struct HttpNetwork {
    client: HttpClient,
    token: Option<Arc<str>>,
    snap: Arc<super::snapshot::SendCtx>,
    /// The leader's members view, carried on every append it sends
    /// ([`super::members`]).
    members: Arc<super::members::MembersState>,
}

impl HttpNetwork {
    pub(crate) fn new(
        token: Option<String>,
        snap: Arc<super::snapshot::SendCtx>,
        members: Arc<super::members::MembersState>,
    ) -> HttpNetwork {
        HttpNetwork {
            client: http_client(),
            token: token.map(Arc::from),
            snap,
            members,
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for HttpNetwork {
    type Network = HttpPeer;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> HttpPeer {
        HttpPeer {
            target,
            node: node.clone(),
            base: format!("http://{}", node.raft),
            client: self.client.clone(),
            token: self.token.clone(),
            snap: self.snap.clone(),
            members: self.members.clone(),
        }
    }
}

/// The connection to one peer.
pub(crate) struct HttpPeer {
    target: NodeId,
    node: Node,
    base: String,
    client: HttpClient,
    token: Option<Arc<str>>,
    snap: Arc<super::snapshot::SendCtx>,
    members: Arc<super::members::MembersState>,
}

/// POST `body` to `url` and return the answer's bytes; anything but a 2xx is a
/// failure.
pub(crate) async fn post(
    client: &HttpClient,
    url: &str,
    token: Option<&str>,
    content_type: &str,
    body: Body,
    ttl: Duration,
) -> Result<Bytes, Fail> {
    post_with(client, url, token, content_type, body, ttl, None).await
}

/// [`post`], with one more header: the leader's members view on an append
/// ([`super::members::MEMBERS_HEADER`]).
async fn post_with(
    client: &HttpClient,
    url: &str,
    token: Option<&str>,
    content_type: &str,
    body: Body,
    ttl: Duration,
    members: Option<axum::http::HeaderValue>,
) -> Result<Bytes, Fail> {
    let mut req = Request::builder()
        .method(Method::POST)
        .uri(url)
        .header(header::CONTENT_TYPE, content_type);
    if let Some(t) = token {
        req = req.header(TOKEN_HEADER, t);
    }
    if let Some(v) = members {
        req = req.header(super::members::MEMBERS_HEADER, v);
    }
    let req = req.body(body).map_err(|e| Fail::Network(e.to_string()))?;
    let resp = match tokio::time::timeout(ttl, client.request(req)).await {
        Err(_) => return Err(Fail::Network(format!("{url}: no answer within {ttl:?}"))),
        Ok(Err(e)) if e.is_connect() => return Err(Fail::Unreachable(format!("{url}: {e}"))),
        Ok(Err(e)) => return Err(Fail::Network(format!("{url}: {e}"))),
        Ok(Ok(r)) => r,
    };
    let status = resp.status();
    let bytes = match tokio::time::timeout(ttl, resp.into_body().collect()).await {
        Err(_) => return Err(Fail::Network(format!("{url}: the answer stalled"))),
        Ok(Err(e)) => return Err(Fail::Network(format!("{url}: {e}"))),
        Ok(Ok(b)) => b.to_bytes(),
    };
    if status == StatusCode::UNAUTHORIZED {
        return Err(Fail::Unreachable(format!(
            "{url}: refused this node's QUEEN_RAFT_TOKEN"
        )));
    }
    if !status.is_success() {
        let text = String::from_utf8_lossy(&bytes[..bytes.len().min(512)]).into_owned();
        return Err(Fail::Network(format!("{url}: {status}: {text}")));
    }
    Ok(bytes)
}

fn decode<T: DeserializeOwned>(bytes: &[u8], what: &str) -> Result<T, Fail> {
    serde_json::from_slice(bytes).map_err(|e| Fail::Network(format!("{what} answer: {e}")))
}

impl HttpPeer {
    async fn call<T: DeserializeOwned>(
        &self,
        path: &str,
        content_type: &str,
        body: Vec<u8>,
        ttl: Duration,
        members: Option<axum::http::HeaderValue>,
    ) -> Result<Result<T, RaftError<TypeConfig>>, RPCError<TypeConfig>> {
        let url = format!("{}{}", self.base, path);
        let bytes = post_with(
            &self.client,
            &url,
            self.token.as_deref(),
            content_type,
            Body::from(body),
            ttl,
            members,
        )
        .await
        .map_err(Fail::rpc)?;
        decode(&bytes, path).map_err(Fail::rpc)
    }

    /// The peer answered with its own error: for these RPCs only `Fatal` (the
    /// peer's Raft stopped), so to this node the peer is unreachable.
    fn remote(&self, e: RaftError<TypeConfig>) -> RPCError<TypeConfig> {
        RPCError::Unreachable(Unreachable::new(&io::Error::other(format!(
            "node {} ({}) failed: {e}",
            self.target, self.node
        ))))
    }

    async fn json_rpc<Q: serde::Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        req: &Q,
        ttl: Duration,
    ) -> Result<T, RPCError<TypeConfig>> {
        let body = serde_json::to_vec(req)
            .map_err(|e| RPCError::Network(NetworkError::new(&io::Error::other(e))))?;
        self.call::<T>(path, "application/json", body, ttl, None)
            .await?
            .map_err(|e| self.remote(e))
    }
}

impl RaftNetworkV2<TypeConfig> for HttpPeer {
    type SnapshotData = Checkpoint;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let (body, n) = wire::encode_append(&rpc).map_err(|e| {
            tracing::error!(target: "rsm", target_node = self.target, error = %e, "raft append does not encode");
            RPCError::Network(NetworkError::new(&e))
        })?;
        let cut = n < rpc.entries.len();
        let last_sent = n.checked_sub(1).map(|i| rpc.entries[i].log_id);
        // Every append — heartbeats included — carries the leader's members
        // view: the liveness evidence and the view of it travel together
        // (super::members).
        let res = self
            .call::<AppendEntriesResponse<TypeConfig>>(
                "/raft/v1/append",
                "application/octet-stream",
                body,
                option.soft_ttl(),
                self.members.header(),
            )
            .await?;
        match res {
            // The request was cut at MAX_APPEND_BYTES: report how far it got,
            // openraft sends the rest next.
            Ok(AppendEntriesResponse::Success) if cut => {
                Ok(AppendEntriesResponse::PartialSuccess(last_sent))
            }
            Ok(r) => Ok(r),
            Err(e) => Err(self.remote(e)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.json_rpc("/raft/v1/vote", &rpc, option.soft_ttl())
            .await
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.json_rpc("/raft/v1/prevote", &rpc, option.soft_ttl())
            .await
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.json_rpc("/raft/v1/transfer", &req, option.soft_ttl())
            .await
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let url = format!("{}/raft/v1/snapshot", self.base);
        let target = self.target;
        let node = self.node.clone();
        tokio::select! {
            closed = cancel => Err(StreamingError::Closed(closed)),
            res = super::snapshot::send(
                &self.snap,
                &self.client,
                &url,
                self.token.as_deref(),
                target,
                vote,
                snapshot,
            ) => match res {
                Ok(Ok(resp)) => Ok(resp),
                Ok(Err(remote)) => Err(StreamingError::Network(NetworkError::new(&io::Error::other(
                    format!("node {target} ({node}) refused the snapshot: {remote}"),
                )))),
                Err(fail) => Err(fail.streaming()),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP: the server side
// ---------------------------------------------------------------------------

#[cfg(feature = "server")]
pub(crate) use server::{bind, serve, RpcState};

#[cfg(feature = "server")]
mod server {
    use std::sync::Arc;

    use axum::body::{Body, Bytes};
    use axum::extract::State;
    use axum::http::{header, HeaderMap, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::post;
    use openraft::errors::RaftError;
    use openraft::raft::{TransferLeaderRequest, VoteRequest};

    use super::super::cluster::TOKEN_HEADER;
    use super::super::snapshot::RecvCtx;
    use super::super::types::TypeConfig;
    use super::super::{wire, RaftHandle};
    use crate::rsm::store::Store;

    pub(crate) struct RpcState<S: Store + 'static> {
        pub(crate) raft: RaftHandle<S>,
        pub(crate) token: Option<Arc<str>>,
        pub(crate) snap: Arc<RecvCtx>,
        /// Plans a follower's prepared command here, on the leader: set by the
        /// facade once it exists ([`super::super::RaftReplicator::set_remote_handler`]).
        pub(crate) remote: Arc<std::sync::OnceLock<super::super::RemoteHandler>>,
        /// Answers a peer's gather of this node's dashboard data: set by the
        /// facade ([`super::super::RaftReplicator::set_local_handler`]).
        pub(crate) local: Arc<std::sync::OnceLock<super::super::RemoteHandler>>,
        /// This node's members view, answered to a follower's relay.
        pub(crate) members: Arc<super::super::members::MembersState>,
    }

    impl<S: Store + 'static> RpcState<S> {
        #[allow(clippy::result_large_err)]
        fn check(&self, headers: &HeaderMap) -> Result<(), Response> {
            match &self.token {
                None => Ok(()),
                Some(want) => {
                    let got = headers.get(TOKEN_HEADER).map(|v| v.as_bytes());
                    if got.is_some_and(|g| token_eq(g, want.as_bytes())) {
                        Ok(())
                    } else {
                        Err((StatusCode::UNAUTHORIZED, "bad raft token").into_response())
                    }
                }
            }
        }
    }

    /// Compare a presented token with the cluster's without an early exit on
    /// the first differing byte (the length is not secret).
    fn token_eq(got: &[u8], want: &[u8]) -> bool {
        if got.len() != want.len() {
            return false;
        }
        let diff = got.iter().zip(want).fold(0u8, |d, (a, b)| d | (a ^ b));
        std::hint::black_box(diff) == 0
    }

    /// Every RPC: the token is checked from the headers before the body is
    /// read, and the handler runs as non-core work — the listener shares the
    /// `queen-raft` runtime, whose threads are core, and a panic in one RPC
    /// (a decode, a dashboard gather) must kill that request, not the node.
    async fn guard<S: Store + 'static>(
        State(st): State<Arc<RpcState<S>>>,
        req: axum::extract::Request,
        next: axum::middleware::Next,
    ) -> Response {
        if let Err(r) = st.check(req.headers()) {
            return r;
        }
        crate::obs::panic_policy::non_core(next.run(req)).await
    }

    /// Body cap of the RPCs that carry log entries or a client command (an
    /// append, a follower's prepared command): one entry can be as large as a
    /// client request (`QUEEN_MAX_BODY_BYTES`) and an append carries up to
    /// [`wire::MAX_APPEND_BYTES`], never fewer than one entry.
    fn entry_body_cap() -> usize {
        let client = std::env::var("QUEEN_MAX_BODY_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(64 * 1024 * 1024);
        2 * client.max(wire::MAX_APPEND_BYTES) + (1 << 20)
    }

    /// Body cap of the control RPCs (vote, pre-vote, transfer, read index,
    /// dashboard gather).
    const CONTROL_BODY_CAP: usize = 1 << 20;

    fn json_answer<T: serde::Serialize>(v: &T) -> Response {
        match serde_json::to_vec(v) {
            Ok(b) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                b,
            )
                .into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        }
    }

    fn bad_request(e: impl std::fmt::Display) -> Response {
        (StatusCode::BAD_REQUEST, e.to_string()).into_response()
    }

    async fn append<S: Store + 'static>(
        State(st): State<Arc<RpcState<S>>>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        // The leader's members view rides every append (super::members); the
        // token was already checked by `guard`, before the body was read.
        if let Some(v) = headers.get(super::super::members::MEMBERS_HEADER) {
            st.members.receive(v.as_bytes());
        }
        let req = match wire::decode_append(&body) {
            Ok(r) => r,
            Err(e) => return bad_request(e),
        };
        json_answer(&st.raft.append_entries(req).await)
    }

    async fn vote<S: Store + 'static>(
        State(st): State<Arc<RpcState<S>>>,
        body: Bytes,
    ) -> Response {
        match serde_json::from_slice::<VoteRequest<TypeConfig>>(&body) {
            Ok(req) => json_answer(&st.raft.vote(req).await),
            Err(e) => bad_request(e),
        }
    }

    async fn prevote<S: Store + 'static>(
        State(st): State<Arc<RpcState<S>>>,
        body: Bytes,
    ) -> Response {
        match serde_json::from_slice::<VoteRequest<TypeConfig>>(&body) {
            Ok(req) => json_answer(&st.raft.pre_vote(req).await),
            Err(e) => bad_request(e),
        }
    }

    async fn transfer<S: Store + 'static>(
        State(st): State<Arc<RpcState<S>>>,
        body: Bytes,
    ) -> Response {
        match serde_json::from_slice::<TransferLeaderRequest<TypeConfig>>(&body) {
            Ok(req) => {
                let res: Result<_, RaftError<TypeConfig>> = st
                    .raft
                    .handle_transfer_leader(req)
                    .await
                    .map_err(RaftError::Fatal);
                json_answer(&res)
            }
            Err(e) => bad_request(e),
        }
    }

    /// A follower's prepared command (`QUEEN_RAFT_CLIENT_OFFLOAD`): planned by
    /// this node's batcher exactly as a local one, answered with the encoded
    /// reply. 503 when this node cannot plan (no facade yet, or not leader —
    /// the reply then says so and the follower retries elsewhere).
    async fn submit<S: Store + 'static>(
        State(st): State<Arc<RpcState<S>>>,
        body: Bytes,
    ) -> Response {
        let Some(h) = st.remote.get().cloned() else {
            return (StatusCode::SERVICE_UNAVAILABLE, "no facade on this node yet").into_response();
        };
        match h(body).await {
            Ok(b) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/octet-stream")],
                b,
            )
                .into_response(),
            Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e).into_response(),
        }
    }

    /// A peer's gather of this node's own dashboard data (PLAN_RAFT.md D17:
    /// node-local metrics, cluster views gather from every node). JSON in and
    /// out; never on the message path.
    async fn local<S: Store + 'static>(
        State(st): State<Arc<RpcState<S>>>,
        body: Bytes,
    ) -> Response {
        let Some(h) = st.local.get().cloned() else {
            return (StatusCode::SERVICE_UNAVAILABLE, "no facade on this node yet").into_response();
        };
        match h(body).await {
            Ok(b) => (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], b)
                .into_response(),
            Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e).into_response(),
        }
    }

    /// A follower's linearizable read point: this node confirms it still leads
    /// (a heartbeat round) and answers the RSM index the follower must have
    /// applied before it reads.
    async fn read_index<S: Store + 'static>(
        State(st): State<Arc<RpcState<S>>>,
    ) -> Response {
        match st
            .raft
            .ensure_linearizable(openraft::raft::ReadPolicy::ReadIndex)
            .await
        {
            Ok(read) => {
                let index = read.index() + 1;
                (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/json")],
                    format!("{{\"index\":{index}}}"),
                )
                    .into_response()
            }
            Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
        }
    }

    /// The leader's members view as it stands now, for an operator (a
    /// follower gets it on every append instead: [`super::super::members`]).
    /// 503 on a node that does not lead.
    async fn members<S: Store + 'static>(State(st): State<Arc<RpcState<S>>>) -> Response {
        let m = {
            use openraft::async_runtime::WatchReceiver;
            st.raft.metrics().borrow_watched().clone()
        };
        match st.members.leader_view(&m) {
            Some(view) => json_answer(&view),
            None => (StatusCode::SERVICE_UNAVAILABLE, "not the leader").into_response(),
        }
    }

    async fn snapshot<S: Store + 'static>(
        State(st): State<Arc<RpcState<S>>>,
        body: Body,
    ) -> Response {
        match super::super::snapshot::receive(&st.snap, &st.raft, body).await {
            Ok(res) => json_answer(&res),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        }
    }

    /// Bind the Raft RPC listener. Blocking, at boot: a taken port fails the
    /// open instead of a background task.
    pub(crate) fn bind(addr: &str) -> std::io::Result<std::net::TcpListener> {
        let l = std::net::TcpListener::bind(addr)
            .map_err(|e| std::io::Error::other(format!("bind the raft listener on {addr}: {e}")))?;
        l.set_nonblocking(true)?;
        Ok(l)
    }

    /// Serve the Raft RPCs until `shutdown` resolves. Runs on openraft's
    /// runtime.
    pub(crate) async fn serve<S: Store + 'static>(
        listener: std::net::TcpListener,
        state: RpcState<S>,
        shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        let listener = match tokio::net::TcpListener::from_std(listener) {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(target: "rsm", error = %e, "raft listener");
                return;
            }
        };
        if state.token.is_none() {
            if let Ok(a) = listener.local_addr() {
                if !a.ip().is_loopback() {
                    tracing::warn!(
                        target: "rsm",
                        addr = %a,
                        "the raft RPC listener is reachable beyond loopback without \
                         QUEEN_RAFT_TOKEN: anyone who can reach it can vote, append and \
                         install snapshots"
                    );
                }
            }
        }
        let state = Arc::new(state);
        use axum::extract::DefaultBodyLimit;
        let router = axum::Router::new()
            .route("/raft/v1/append", post(append::<S>))
            .route("/raft/v1/submit", post(submit::<S>))
            .layer(DefaultBodyLimit::max(entry_body_cap()))
            .merge(
                axum::Router::new()
                    .route("/raft/v1/vote", post(vote::<S>))
                    .route("/raft/v1/prevote", post(prevote::<S>))
                    .route("/raft/v1/transfer", post(transfer::<S>))
                    .route("/raft/v1/read_index", post(read_index::<S>))
                    .route("/raft/v1/local", post(local::<S>))
                    .route("/raft/v1/members", post(members::<S>))
                    .layer(DefaultBodyLimit::max(CONTROL_BODY_CAP)),
            )
            // A snapshot is streamed to disk (the `Body` extractor has no cap).
            .merge(axum::Router::new().route("/raft/v1/snapshot", post(snapshot::<S>)))
            .layer(axum::middleware::from_fn_with_state(state.clone(), guard::<S>))
            .with_state(state);
        if let Err(e) = axum::serve(listener, router)
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::error!(target: "rsm", error = %e, "raft RPC server stopped");
        }
    }
}
