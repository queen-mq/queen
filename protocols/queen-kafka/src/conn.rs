//! One client connection: read a request, answer it, in order.
//!
//! Kafka's own rule is one request at a time per connection, answered in
//! order. Produce is the one exception here, for throughput: once a push is in
//! the raft log the next request is read while it commits, because anything
//! sent after it lands after it. Answers still leave in request order.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, Encodable};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::core::BoxFuture;
use crate::handlers;
use crate::server::Ctx;
use crate::versions;

/// What a connection knows about its client.
pub struct Conn {
    /// The Queen token SASL/PLAIN carried, sent as the bearer of every call.
    pub bearer: Option<String>,
    pub authenticated: bool,
    pub handshaken: bool,
    pub client_id: String,
    pub client_host: String,
}

/// One answer, in the order its request came.
pub type Reply = BoxFuture<'static, Option<Bytes>>;

pub enum Step {
    Reply(Reply),
    /// Answer, then close: a failed authentication.
    ReplyThenClose(Reply),
    /// `acks=0`: Kafka sends nothing back.
    Silent,
    Close,
}

pub fn ready(bytes: Option<Bytes>) -> Step {
    match bytes {
        Some(b) => Step::Reply(Box::pin(async move { Some(b) })),
        None => Step::Close,
    }
}

pub async fn serve(ctx: Arc<Ctx>, stream: TcpStream, peer: SocketAddr) {
    let (mut rd, wr) = stream.into_split();
    let (tx, rx) = mpsc::channel::<Reply>(16);
    let writer = tokio::spawn(write_loop(wr, rx));
    let mut conn = Conn {
        bearer: None,
        authenticated: false,
        handshaken: false,
        client_id: String::new(),
        client_host: format!("/{}", peer.ip()),
    };
    loop {
        let frame = match read_frame(&mut rd, ctx.cfg.max_request_bytes).await {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => {
                tracing::debug!(target: "kafka", %peer, error = %e, "connection closed");
                break;
            }
        };
        match handle(&ctx, &mut conn, frame).await {
            Step::Reply(r) => {
                if tx.send(r).await.is_err() {
                    break;
                }
            }
            Step::ReplyThenClose(r) => {
                let _ = tx.send(r).await;
                break;
            }
            Step::Silent => {}
            Step::Close => break,
        }
    }
    drop(tx);
    let _ = writer.await;
}

async fn write_loop(mut wr: OwnedWriteHalf, mut rx: mpsc::Receiver<Reply>) {
    while let Some(reply) = rx.recv().await {
        match reply.await {
            Some(bytes) => {
                if wr.write_all(&bytes).await.is_err() {
                    break;
                }
            }
            None => break,
        }
    }
    let _ = wr.shutdown().await;
}

async fn read_frame(
    rd: &mut tokio::net::tcp::OwnedReadHalf,
    max: usize,
) -> std::io::Result<Option<Bytes>> {
    let mut len = [0u8; 4];
    match rd.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let n = i32::from_be_bytes(len);
    if n < 8 || n as usize > max {
        return Err(std::io::Error::other(format!("a request of {n} bytes")));
    }
    let mut buf = vec![0u8; n as usize];
    rd.read_exact(&mut buf).await?;
    Ok(Some(Bytes::from(buf)))
}

/// Header and body of one answer, length-prefixed.
pub fn encode<R: Encodable>(
    key: ApiKey,
    version: i16,
    correlation_id: i32,
    body: &R,
) -> Option<Bytes> {
    let mut buf = BytesMut::with_capacity(256);
    buf.put_i32(0);
    let header = ResponseHeader::default().with_correlation_id(correlation_id);
    if let Err(e) = header.encode(&mut buf, key.response_header_version(version)) {
        tracing::error!(target: "kafka", ?key, version, error = %e, "cannot encode a response header");
        return None;
    }
    if let Err(e) = body.encode(&mut buf, version) {
        tracing::error!(target: "kafka", ?key, version, error = %e, "cannot encode a response");
        return None;
    }
    let len = (buf.len() - 4) as i32;
    buf[0..4].copy_from_slice(&len.to_be_bytes());
    Some(buf.freeze())
}

macro_rules! decode {
    ($t:ty, $buf:expr, $version:expr, $key:expr) => {
        match <$t>::decode(&mut $buf, $version) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(target: "kafka", key = ?$key, version = $version, error = %e, "undecodable request");
                return Step::Close;
            }
        }
    };
}

async fn handle(ctx: &Arc<Ctx>, conn: &mut Conn, frame: Bytes) -> Step {
    let key_raw = i16::from_be_bytes([frame[0], frame[1]]);
    let version = i16::from_be_bytes([frame[2], frame[3]]);
    let correlation = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
    let Ok(key) = ApiKey::try_from(key_raw) else {
        tracing::debug!(target: "kafka", key_raw, "unknown API key; closing");
        return Step::Close;
    };
    if key == ApiKey::ApiVersions {
        return ready(handlers::meta::api_versions(version, correlation));
    }
    let Some(api) = versions::lookup(key_raw) else {
        tracing::debug!(target: "kafka", ?key, "API not served; closing");
        return Step::Close;
    };
    if version < api.min || version > api.max {
        tracing::debug!(target: "kafka", ?key, version, "version not advertised; closing");
        return Step::Close;
    }
    let mut buf = frame;
    let header = match RequestHeader::decode(&mut buf, key.request_header_version(version)) {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(target: "kafka", ?key, error = %e, "undecodable request header");
            return Step::Close;
        }
    };
    if let Some(id) = &header.client_id {
        if conn.client_id.as_str() != id.as_str() {
            conn.client_id = id.to_string();
        }
    }
    let gated = !matches!(key, ApiKey::SaslHandshake | ApiKey::SaslAuthenticate);
    if gated && ctx.cfg.sasl_required && !conn.authenticated {
        tracing::info!(target: "kafka", ?key, "request before SASL authentication; closing");
        return Step::Close;
    }
    let reply = |body: Option<Bytes>| ready(body);
    use handlers::*;
    match key {
        ApiKey::Produce => {
            let req = decode!(ProduceRequest, buf, version, key);
            produce::produce(ctx, conn, req, version, correlation).await
        }
        ApiKey::Fetch => {
            let req = decode!(FetchRequest, buf, version, key);
            let out = fetch::fetch(ctx, conn, req, version).await;
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::ListOffsets => {
            let req = decode!(ListOffsetsRequest, buf, version, key);
            let out = fetch::list_offsets(ctx, conn, req, version).await;
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::Metadata => {
            let req = decode!(MetadataRequest, buf, version, key);
            let out = meta::metadata(ctx, conn, req, version).await;
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::FindCoordinator => {
            let req = decode!(FindCoordinatorRequest, buf, version, key);
            let out = meta::find_coordinator(ctx, req, version);
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::InitProducerId => {
            let req = decode!(InitProducerIdRequest, buf, version, key);
            let out = meta::init_producer_id(ctx, req);
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::JoinGroup => {
            let req = decode!(JoinGroupRequest, buf, version, key);
            let out = group::join(ctx, conn, req, version).await;
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::SyncGroup => {
            let req = decode!(SyncGroupRequest, buf, version, key);
            let out = group::sync(ctx, req).await;
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::Heartbeat => {
            let req = decode!(HeartbeatRequest, buf, version, key);
            let out = group::heartbeat(ctx, req);
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::LeaveGroup => {
            let req = decode!(LeaveGroupRequest, buf, version, key);
            let out = group::leave(ctx, req);
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::OffsetCommit => {
            let req = decode!(OffsetCommitRequest, buf, version, key);
            let out = offsets::commit(ctx, conn, req).await;
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::OffsetFetch => {
            let req = decode!(OffsetFetchRequest, buf, version, key);
            let out = offsets::fetch(ctx, conn, req, version).await;
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::ListGroups => {
            let req = decode!(ListGroupsRequest, buf, version, key);
            let out = group::list(ctx, req);
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::DescribeGroups => {
            let req = decode!(DescribeGroupsRequest, buf, version, key);
            let out = group::describe(ctx, req);
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::DeleteGroups => {
            let req = decode!(DeleteGroupsRequest, buf, version, key);
            let out = group::delete(ctx, conn, req).await;
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::CreateTopics => {
            let req = decode!(CreateTopicsRequest, buf, version, key);
            let out = admin::create_topics(ctx, conn, req, version).await;
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::DeleteTopics => {
            let req = decode!(DeleteTopicsRequest, buf, version, key);
            let out = admin::delete_topics(ctx, conn, req, version).await;
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::CreatePartitions => {
            let req = decode!(CreatePartitionsRequest, buf, version, key);
            let out = admin::create_partitions(ctx, conn, req).await;
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::DescribeConfigs => {
            let req = decode!(DescribeConfigsRequest, buf, version, key);
            let out = admin::describe_configs(ctx, conn, req).await;
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::SaslHandshake => {
            let req = decode!(SaslHandshakeRequest, buf, version, key);
            let out = sasl::handshake(conn, req);
            reply(encode(key, version, correlation, &out))
        }
        ApiKey::SaslAuthenticate => {
            let req = decode!(SaslAuthenticateRequest, buf, version, key);
            let (out, close) = sasl::authenticate(ctx, conn, req).await;
            match encode(key, version, correlation, &out) {
                // A failed authentication is answered, then the connection
                // closes, as Kafka does.
                Some(bytes) if close => Step::ReplyThenClose(Box::pin(async move { Some(bytes) })),
                b => reply(b),
            }
        }
        _ => Step::Close,
    }
}
