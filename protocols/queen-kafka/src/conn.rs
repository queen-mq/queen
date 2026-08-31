//! The Kafka listener: framing, request-header decode, the optional TLS wrapper
//! and the per-connection serial dispatch.
//!
//! Wire shape (kafka.apache.org/protocol): every request and every response is a
//! 4-byte big-endian length followed by that many bytes — a request header then
//! the request body, a response header then the response body. `tokio_util`'s
//! `LengthDelimitedCodec` is exactly that codec, driven here through the
//! `Decoder`/`Encoder` traits rather than through `Framed`: the loop below has
//! to finish one request, write its response, and only then look at the next
//! byte (Apache Kafka mutes a channel while a request is in flight, and clients
//! rely on responses coming back in request order), which is the plain shape of
//! a hand-driven codec and an awkward one through a `Stream`/`Sink` pair.
//!
//! ## A connection is a state machine now (M5)
//!
//! Until M5 a connection was a loop with no memory: every frame was answered
//! from the process-wide facade and nothing carried over. Two things now do —
//! the TLS server name the client asked for, and the credential SASL/PLAIN
//! presented — so the loop carries a [`Conn`], and [`dispatch`] takes it by
//! `&mut`. Both live exactly as long as the socket: a facade restart is a
//! broker restart, and every client already knows how to reconnect and
//! re-authenticate.
//!
//! The same shape is what makes the SASL gate a *place* rather than a check
//! scattered through the handlers: before a connection is admitted, three APIs
//! are answerable and everything else closes the connection, which is what
//! Apache Kafka's `SaslServerAuthenticator` does — see [`crate::sasl`].

use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::{
    AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, AlterConfigsRequest, ApiKey,
    ApiVersionsRequest, CreateAclsRequest, CreatePartitionsRequest, CreateTopicsRequest,
    DeleteAclsRequest, DeleteGroupsRequest, DeleteTopicsRequest, DescribeAclsRequest,
    DescribeConfigsRequest, DescribeGroupsRequest, EndTxnRequest, FetchRequest,
    FindCoordinatorRequest, HeartbeatRequest, IncrementalAlterConfigsRequest,
    InitProducerIdRequest, JoinGroupRequest, LeaveGroupRequest, ListGroupsRequest,
    ListOffsetsRequest, MetadataRequest, OffsetCommitRequest, OffsetDeleteRequest,
    OffsetFetchRequest, ProduceRequest, RequestHeader, ResponseHeader, SaslAuthenticateRequest,
    SaslHandshakeRequest, SyncGroupRequest, TxnOffsetCommitRequest,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::codec::{Encoder, LengthDelimitedCodec};

use crate::handlers::{
    acls, add_offsets_to_txn, add_partitions_to_txn, alter_configs, api_versions,
    create_partitions, create_topics, delete_groups, delete_topics, describe_configs,
    describe_groups, end_txn, fetch, find_coordinator, heartbeat, incremental_alter_configs,
    init_producer_id, join_group, leave_group, list_groups, list_offsets, metadata, offset_commit,
    offset_delete, offset_fetch, produce, sasl_authenticate, sasl_handshake, sync_group,
    txn_offset_commit,
};
use crate::obs::Sampler;
use crate::sasl::SaslState;
use crate::txn;
use crate::versions::{self, Support};
use crate::Facade;

/// Ceiling on a single request frame. Kafka's own `socket.request.max.bytes`
/// defaults to 100 MiB and clients size their batches under it, so this is the
/// number they already respect.
///
/// It is a ceiling on what may be READ and not on what may be reserved for: the
/// declared length is a number an unauthenticated peer picks, and the buffer
/// this connection reads into never runs more than [`READ_BUF_BYTES`] ahead of
/// the bytes that have actually arrived ([`next_frame`]). Handing the length
/// prefix straight to `LengthDelimitedCodec` does the opposite — its
/// `decode_head` ends in `src.reserve(n - src.len())`, so four bytes of `0x0640
/// 0000` would buy 100 MiB of heap per connection, before a single body byte
/// and before any credential.
pub const MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

/// Ceiling on a frame from a connection that has NOT authenticated, when this
/// listener authenticates at all.
///
/// Everything a connection may legitimately send before it is admitted is
/// small: an ApiVersions, a SaslHandshake, and a SaslAuthenticate carrying one
/// bearer token. Apache Kafka bounds the same window for the same reason
/// (`SaslServerAuthenticator.MAX_RECEIVE_SIZE`, 512 KiB); 64 KiB is far past
/// any token and keeps what an anonymous peer can make this process hold to a
/// number that multiplies safely by [`Policy::max_connections`].
pub const PRE_AUTH_MAX_FRAME_BYTES: usize = 64 * 1024;

/// Read buffer a fresh connection starts with, and the most the buffer is
/// allowed to run ahead of the bytes that have arrived. One ApiVersions
/// exchange is a few dozen bytes; a 100 MiB Produce grows in steps of this,
/// which is what makes the memory a function of what was SENT rather than of
/// what was DECLARED.
const READ_BUF_BYTES: usize = 16 * 1024;

/// The 4-byte big-endian length in front of every request and every response.
const LENGTH_PREFIX: usize = 4;

/// The frame codec, in one place so the listener and the tests cannot disagree
/// about the framing.
///
/// The listener uses it to WRITE responses. Requests are read by
/// [`next_frame`], which is the same framing decided by hand — see
/// [`MAX_FRAME_BYTES`] for the one thing the codec does that a listener facing
/// the open internet cannot afford.
pub fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .big_endian()
        .length_field_length(LENGTH_PREFIX)
        .max_frame_length(MAX_FRAME_BYTES)
        .new_codec()
}

/// How long a TLS handshake may take before the connection is dropped.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a connection may say NOTHING between requests before it is closed.
///
/// Kafka's `connections.max.idle.ms`, same default and same purpose: every
/// client already reconnects transparently when a broker drops an idle
/// connection, and a facade that never does is a facade whose sockets, tasks
/// and buffers are held by whoever opened them and walked away.
///
/// It bounds the wait for the FIRST byte of a request and nothing else. A
/// long-poll Fetch parked for thirty seconds is not idle by this measure: its
/// request has arrived, and the silence is ours.
/// `pub(crate)` for [`crate::handlers::describe_configs`], which reports it as
/// `connections.max.idle.ms`: the number a broker resource answers there has to
/// be the one this loop actually enforces, not a second copy of it.
pub(crate) const IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// How long a connection may go quiet in the MIDDLE of a frame.
///
/// A peer that has declared a length owes us that many bytes, and until they
/// arrive this process is holding what has: a partial frame is the one place
/// where saying nothing costs the facade rather than the client. The timer is
/// reset by every read that returns anything, so a slow link finishes a large
/// batch — what it refuses is the connection that sends a length prefix and
/// then stops, which is the cheapest thing an attacker can do.
const PARTIAL_FRAME_TIMEOUT: Duration = Duration::from_secs(30);

/// The outcome of one request frame.
#[derive(Debug)]
pub enum Reply {
    /// Response header + body, ready to be length-prefixed and written back.
    Send(Bytes),
    /// The request was handled and NOTHING goes back on the wire. The one API
    /// that does this is Produce with `acks=0`, where the absence of a response
    /// is the protocol (`handlers::produce`): the client is not waiting for one
    /// and would read any bytes written here as the answer to its next request.
    Silent,
    /// Write this response, then close. The SASL refusal: Apache Kafka answers
    /// SASL_AUTHENTICATION_FAILED and drops the connection, because the error
    /// code is what tells a client the credential is wrong rather than the
    /// broker unwell (KIP-152) and there is nothing further to say on a
    /// connection that will never be admitted.
    SendThenClose(Bytes, String),
    /// Nothing the client could parse can be written — an unknown API key
    /// leaves us without a response schema, a header we cannot decode leaves us
    /// without even a correlation id, and an unauthenticated request leaves us
    /// with nothing we are willing to answer. Apache Kafka drops the connection
    /// in all three; the string is the log line.
    Close(String),
}

/// One line per window for connections that ended badly, not one per
/// connection: a client pinned to a version this build refuses, or a probe that
/// opens and closes, is exactly the shape that floods a log.
static CLOSED: Sampler = Sampler::new(10_000);
/// The same, for credentials Queen refused — the line a fleet with a stale
/// password produces on every reconnect of every consumer.
static AUTH_REFUSED: Sampler = Sampler::new(10_000);
/// ...and for the connections that were admitted, so a deployment can see that
/// SASL is working without a line per connection.
static AUTH_OK: Sampler = Sampler::new(10_000);
/// A TLS handshake that never completed: a plaintext client against a TLS
/// listener produces one per reconnect, for ever.
static TLS_FAILED: Sampler = Sampler::new(10_000);
/// ...and connections refused because the listener is already full, which is by
/// definition a situation that repeats.
static AT_CAPACITY: Sampler = Sampler::new(10_000);

/// Accept loop. One tokio task per connection, and never returns.
///
/// `tls` is the optional listener config (`QUEEN_KAFKA_TLS_CERT`/`_KEY`,
/// [`crate::tls`]). With it, the client's SNI is read off the completed
/// handshake and becomes this connection's routing name.
pub async fn serve(
    listener: TcpListener,
    facade: Arc<Facade>,
    tls: Option<Arc<rustls::ServerConfig>>,
) {
    let acceptor = tls.map(tokio_rustls::TlsAcceptor::from);
    // How many connections this listener serves at once. Every accepted socket
    // costs a task, a read buffer and — until it authenticates — whatever it
    // has sent, so the number of them is a number and not "as many as the file
    // descriptor limit allows". A refused connection is closed immediately
    // rather than left in the backlog, so a client sees a reset and reconnects
    // instead of hanging on a socket nobody will read.
    let slots = Arc::new(tokio::sync::Semaphore::new(facade.policy.max_connections));
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let Ok(slot) = Arc::clone(&slots).try_acquire_owned() else {
                    if let Some(suppressed) = AT_CAPACITY.tick_now() {
                        tracing::warn!(
                            target: "kafka",
                            %peer,
                            suppressed,
                            connections = facade.policy.max_connections,
                            "the Kafka listener is at QUEEN_KAFKA_MAX_CONNECTIONS; \
                             this connection was closed without being served"
                        );
                    }
                    drop(stream);
                    continue;
                };
                // Small request/response bodies on a keep-alive connection with
                // one request in flight: Nagle would sit on every response for
                // up to ~40ms waiting to coalesce, for no benefit.
                stream.set_nodelay(true).ok();
                let facade = Arc::clone(&facade);
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    // Held for the life of the connection, released by the drop
                    // at the end of this task however it ends.
                    let _slot = slot;
                    let outcome = match acceptor {
                        None => {
                            connection(stream, Conn::new(&facade, None, kafka_host(&peer))).await
                        }
                        Some(acceptor) => {
                            let handshake = tokio::time::timeout(
                                TLS_HANDSHAKE_TIMEOUT,
                                acceptor.accept(stream),
                            )
                            .await;
                            match handshake {
                                Ok(Ok(stream)) => {
                                    let sni = crate::tls::server_name(&stream);
                                    connection(stream, Conn::new(&facade, sni, kafka_host(&peer)))
                                        .await
                                }
                                // Neither is the facade's fault and both are
                                // client-driven, so both are sampled: a
                                // plaintext client against a TLS port, a
                                // scanner, a proxy health check that opens a
                                // socket and leaves.
                                Ok(Err(e)) => {
                                    if let Some(suppressed) = TLS_FAILED.tick_now() {
                                        tracing::warn!(
                                            target: "kafka",
                                            %peer,
                                            suppressed,
                                            error = %e,
                                            "TLS handshake failed"
                                        );
                                    }
                                    return;
                                }
                                Err(_) => {
                                    if let Some(suppressed) = TLS_FAILED.tick_now() {
                                        tracing::warn!(
                                            target: "kafka",
                                            %peer,
                                            suppressed,
                                            timeout_ms = TLS_HANDSHAKE_TIMEOUT.as_millis() as u64,
                                            "TLS handshake did not complete in time"
                                        );
                                    }
                                    return;
                                }
                            }
                        }
                    };
                    if let Err(e) = outcome {
                        // At warn, sampled, and not at debug: a connection that
                        // ends in a refusal is the one event an operator has to
                        // be able to see without turning debug on, and it is
                        // also the one an unhappy client repeats.
                        if let Some(suppressed) = CLOSED.tick_now() {
                            tracing::warn!(
                                target: "kafka",
                                %peer,
                                suppressed,
                                error = %e,
                                "connection closed"
                            );
                        }
                    }
                });
            }
            Err(e) => {
                // A transient accept error (fd exhaustion, a peer that reset
                // between SYN and accept) must not kill the listener.
                tracing::warn!(target: "kafka", error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// One connection's state: the facade as it sees it, and where it is in the
/// SASL conversation.
pub struct Conn {
    /// This connection's view — its credential, and the Queen lane its calls go
    /// through. Replaced exactly once, when SASL admits it.
    pub facade: Facade,
    pub sasl: SaslState,
    /// The TLS server name the client asked for, kept for the log even when
    /// `QUEEN_KAFKA_FORWARD_SNI_HOST` is off: "which name did this consumer
    /// dial" is the first question of every routing problem.
    pub sni: Option<String>,
    /// Where this connection came from, already in Apache Kafka's own spelling
    /// (`/127.0.0.1`) so nothing downstream has to know the format.
    ///
    /// It exists because the accept loop is the ONLY place that knows it, and
    /// because DescribeGroups answers it as a member's HOST column (M7 F2). It
    /// is carried and never interpreted: no routing, no authorization and no
    /// rate limiting reads it, so a proxy that rewrites the peer address costs
    /// an operator a column and nothing else.
    pub peer: String,
    /// This connection's identity inside [`crate::txn::Txns`].
    ///
    /// Minted per accepted connection and never reused, so dropping everything
    /// this connection staged is one comparison. It exists because a
    /// transaction's STAGE is process-wide state that belongs to a connection:
    /// keyed on `Conn` it could not be dropped by the InitProducerId fence,
    /// could not be charged against a process-wide byte budget, and would have
    /// no single place for the timeout sweep to walk.
    pub id: txn::ConnId,
}

impl Conn {
    /// A fresh connection against the process-wide facade.
    pub fn new(root: &Facade, sni: Option<String>, peer: String) -> Conn {
        let facade = root.for_connection(sni.as_deref());
        let sasl = if facade.policy.sasl_plain {
            SaslState::Unauthenticated
        } else {
            SaslState::Disabled
        };
        Conn {
            facade,
            sasl,
            sni,
            peer,
            id: txn::next_conn_id(),
        }
    }
}

impl Drop for Conn {
    /// A closed connection has no open transaction, so its stage is dropped
    /// here.
    ///
    /// This is the ORDINARY path for a producer that closes and the CRASH path
    /// for one that does not, and both are safe for the same reason: a lost
    /// stage IS an aborted transaction, because nothing of it was ever written
    /// to the log. What it buys is the memory, immediately, instead of at the
    /// timeout sweep — which matters most for the producer that opened a large
    /// transaction and then died.
    fn drop(&mut self) {
        self.facade.txns.drop_connection(self.id);
    }
}

/// A peer address the way Apache Kafka writes one into a group description:
/// `InetAddress.toString()` with an empty hostname, i.e. `/127.0.0.1`, and
/// without the ephemeral port — which is what `kafka-consumer-groups.sh` prints
/// under HOST and what every UI renders.
fn kafka_host(peer: &std::net::SocketAddr) -> String {
    format!("/{}", peer.ip())
}

/// One connection, serial: decode a frame, handle it, write the response, and
/// only then look for the next frame. Returns `Ok` on a clean close.
///
/// Generic over the stream so the plaintext and TLS listeners are the same
/// loop: everything above the socket — framing, muting, dispatch — is
/// identical, and a second copy of it would be a second place for the two to
/// drift.
async fn connection<S>(mut stream: S, mut conn: Conn) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut codec = codec();
    let mut inbuf = BytesMut::with_capacity(READ_BUF_BYTES);
    let mut outbuf = BytesMut::new();
    loop {
        // The ceiling this connection is held to right now. A connection that
        // has not authenticated gets the small one — see
        // [`PRE_AUTH_MAX_FRAME_BYTES`] — and earns the full one by presenting a
        // credential, which is the same order Apache Kafka does it in.
        let limit = if conn.sasl.admitted() {
            MAX_FRAME_BYTES
        } else {
            PRE_AUTH_MAX_FRAME_BYTES
        };
        let Some(frame) = next_frame(&mut stream, &mut inbuf, limit).await? else {
            return Ok(());
        };
        match dispatch(&mut conn, frame).await {
            Reply::Send(body) => {
                outbuf.clear();
                codec.encode(body, &mut outbuf)?;
                stream.write_all(&outbuf).await?;
                stream.flush().await?;
            }
            // Straight on to the next frame: the client is already writing it.
            Reply::Silent => {}
            // The answer is written FIRST and the connection dies after it: a
            // client that never receives the error code cannot tell a refused
            // credential from an unreachable broker.
            Reply::SendThenClose(body, why) => {
                outbuf.clear();
                codec.encode(body, &mut outbuf)?;
                stream.write_all(&outbuf).await?;
                stream.flush().await?;
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, why));
            }
            Reply::Close(why) => {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, why));
            }
        }
    }
}

/// The next whole request frame — the bytes AFTER the length prefix — or `None`
/// when the peer closed cleanly between requests.
///
/// This is the framing the codec would do, done by hand, and the difference is
/// the whole point: nothing is allocated for a length a peer merely CLAIMED.
/// The buffer is grown by [`READ_BUF_BYTES`] at a time as bytes arrive, so what
/// this connection costs is what it has actually sent — and both silences that
/// could hold that cost open are bounded ([`IDLE_TIMEOUT`],
/// [`PARTIAL_FRAME_TIMEOUT`]).
///
/// Whatever is already buffered is drained before the socket is touched again:
/// a client is free to pipeline several requests into one write, and they must
/// be answered one at a time, in order.
async fn next_frame<S>(
    stream: &mut S,
    inbuf: &mut BytesMut,
    limit: usize,
) -> std::io::Result<Option<Bytes>>
where
    S: AsyncRead + Unpin,
{
    loop {
        if inbuf.len() >= LENGTH_PREFIX {
            let declared = u32::from_be_bytes([inbuf[0], inbuf[1], inbuf[2], inbuf[3]]) as usize;
            if declared > limit {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "a frame of {declared} bytes was declared and the ceiling for this \
                         connection is {limit}"
                    ),
                ));
            }
            if inbuf.len() >= LENGTH_PREFIX + declared {
                inbuf.advance(LENGTH_PREFIX);
                return Ok(Some(inbuf.split_to(declared).freeze()));
            }
        }
        // Room for the next read, and never more than one buffer's worth of it.
        if inbuf.capacity() - inbuf.len() < READ_BUF_BYTES {
            inbuf.reserve(READ_BUF_BYTES);
        }
        let quiet = if inbuf.is_empty() {
            IDLE_TIMEOUT
        } else {
            PARTIAL_FRAME_TIMEOUT
        };
        let read = match tokio::time::timeout(quiet, stream.read_buf(inbuf)).await {
            Ok(read) => read?,
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "nothing was read for {}s with {} bytes of a partial request buffered",
                        quiet.as_secs(),
                        inbuf.len()
                    ),
                ))
            }
        };
        if read == 0 {
            return if inbuf.is_empty() {
                Ok(None)
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("EOF with {} bytes of a partial frame", inbuf.len()),
                ))
            };
        }
    }
}

/// Handle one request frame (the bytes after the length prefix). No panics:
/// every malformed input leaves through `Reply::Close`. The only I/O it can do
/// is the handler's own, through the connection's facade.
pub async fn dispatch(conn: &mut Conn, frame: Bytes) -> Reply {
    // THE ONE FRAME THAT IS NOT A KAFKA REQUEST. After a SaslHandshake **v0**
    // the next frame is a bare RFC 4616 token — same 4-byte length prefix, no
    // request header, no api key — so it has to be recognised before anything
    // tries to read one out of it. See `handlers::sasl_handshake`.
    if conn.sasl == SaslState::AwaitingRawToken {
        return raw_sasl_token(conn, &frame).await;
    }

    // `api_key` and `api_version` sit at offsets 0 and 2 of EVERY request header
    // version. That fixed prefix is what makes the correct header version
    // reachable before anything else is decoded — the header itself is v1 or v2
    // (flexible) depending on the api_version, and guessing wrong desynchronises
    // the whole rest of the frame.
    if frame.len() < 4 {
        return Reply::Close(format!(
            "request frame of {} bytes is too short to carry an api key and version",
            frame.len()
        ));
    }
    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);

    let (key, advertised) = match versions::classify(api_key, api_version) {
        Support::Advertised(k) => (k, true),
        // ApiVersions is the one API that can answer a version it does not
        // speak, and it MUST (see `handlers::api_versions::unsupported_version`).
        Support::UnsupportedVersion(ApiKey::ApiVersions) => (ApiKey::ApiVersions, false),
        // For everything else the body is a layout we do not have, so there is
        // no request to answer and no version to answer it at. Apache Kafka
        // closes the connection here too (an unsupported version fails
        // `parseRequest` as an invalid request); the client reconnects and
        // renegotiates.
        Support::UnsupportedVersion(k) => {
            let a = versions::lookup(api_key).expect("classified against the table");
            return Reply::Close(format!(
                "{k:?} v{api_version} is outside the advertised window {}..={}; \
                 a client that read our ApiVersions answer never sends it",
                a.min, a.max
            ));
        }
        Support::UnknownApi => {
            return Reply::Close(format!(
                "api key {api_key} is not in the advertised table; \
                 a client that read our ApiVersions answer never sends it"
            ))
        }
    };

    // THE SASL GATE. Before a connection has presented a credential it may
    // negotiate versions and it may authenticate, and that is all. Apache Kafka
    // refuses the rest by throwing `IllegalSaslStateException` out of
    // `SaslServerAuthenticator.handleKafkaRequest`, which is an
    // `AuthenticationException`, which closes the channel with no response —
    // so this closes too. Answering an error code instead would be answering an
    // unauthenticated request, which is the one thing a gate exists to stop.
    if !conn.sasl.admitted() && !matches!(key, ApiKey::ApiVersions | ApiKey::SaslHandshake) {
        // SaslAuthenticate is the third pre-auth API and the one with an order
        // to it: it is answerable only after a v1 handshake has agreed the
        // mechanism, and `SaslState::AwaitingAuthenticate` is that agreement.
        // Without it there is nothing to interpret its bytes as, and Kafka
        // closes there too.
        let awaited =
            key == ApiKey::SaslAuthenticate && matches!(conn.sasl, SaslState::AwaitingAuthenticate);
        if !awaited {
            return Reply::Close(format!(
                "{key:?} v{api_version} arrived before this connection authenticated; \
                 QUEEN_KAFKA_SASL is on, so a connection sends SaslHandshake(PLAIN) and \
                 SaslAuthenticate first"
            ));
        }
    }

    let mut buf = frame;
    let header_version = key.request_header_version(api_version);
    let header = match RequestHeader::decode(&mut buf, header_version) {
        Ok(h) => h,
        Err(e) => {
            return Reply::Close(format!(
                "request header v{header_version} for {key:?} v{api_version}: {e}"
            ))
        }
    };

    match key {
        ApiKey::ApiVersions => {
            let rendered = if advertised {
                // Decoding the body is what rejects a truncated or padded frame;
                // the fields themselves are informational until the M6 client
                // matrix, where they are the one place a client names itself.
                match ApiVersionsRequest::decode(&mut buf, api_version) {
                    Ok(req) => {
                        tracing::debug!(
                            target: "kafka",
                            software = %req.client_software_name.as_str(),
                            software_version = %req.client_software_version.as_str(),
                            client_id = header.client_id.as_ref().map(|s| s.as_str()).unwrap_or(""),
                            api_version,
                            "api versions"
                        );
                        api_versions::handle(api_version)
                    }
                    Err(e) => return Reply::Close(format!("ApiVersions v{api_version} body: {e}")),
                }
            } else {
                // The version is above (or below) our window, so the body is a
                // layout we do not have. Do not touch it — answer the quirk.
                api_versions::unsupported_version()
            };
            respond(
                key,
                header.correlation_id,
                &rendered.body,
                rendered.encode_version,
            )
        }
        ApiKey::SaslHandshake => {
            let req = match SaslHandshakeRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("SaslHandshake v{api_version} body: {e}")),
            };
            let body = sasl_handshake::handle(&req, api_version, &mut conn.sasl);
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::SaslAuthenticate => {
            let req = match SaslAuthenticateRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => {
                    return Reply::Close(format!("SaslAuthenticate v{api_version} body: {e}"))
                }
            };
            let (body, outcome) =
                sasl_authenticate::handle(&conn.facade, &req, &mut conn.sasl).await;
            let reply = respond(key, header.correlation_id, &body, api_version);
            settle(conn, outcome, reply)
        }
        ApiKey::Metadata => {
            let req = match MetadataRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("Metadata v{api_version} body: {e}")),
            };
            // Since M5 this is the CONNECTION's credential: `QUEEN_TOKEN` on a
            // listener with no SASL, and the token this connection presented on
            // one with it.
            let body = metadata::handle(&conn.facade, &req, api_version, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::Produce => {
            let req = match ProduceRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("Produce v{api_version} body: {e}")),
            };
            match produce::handle(&conn.facade, &req, conn.facade.token()).await {
                Some(body) => respond(key, header.correlation_id, &body, api_version),
                // acks=0. See `Reply::Silent`.
                None => Reply::Silent,
            }
        }
        ApiKey::Fetch => {
            let req = match FetchRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("Fetch v{api_version} body: {e}")),
            };
            // This is the one handler that can hold the connection for a long
            // time, and that is the design: a long-poll Fetch parks with its
            // connection muted, exactly as PLAN_QUEEN_KAFKA.md says it should.
            let body = fetch::handle(&conn.facade, &req, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::ListOffsets => {
            let req = match ListOffsetsRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("ListOffsets v{api_version} body: {e}")),
            };
            let body = list_offsets::handle(&conn.facade, &req, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::FindCoordinator => {
            let req = match FindCoordinatorRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("FindCoordinator v{api_version} body: {e}")),
            };
            let body = find_coordinator::handle(&conn.facade, &req);
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::JoinGroup => {
            let req = match JoinGroupRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("JoinGroup v{api_version} body: {e}")),
            };
            // The SECOND handler that can hold a connection for a long time,
            // and the one whose parking is the protocol rather than a poll: a
            // JoinGroup is answered when the group's join window closes, which
            // is what makes every member of a rebalance learn the same
            // generation. See `handlers::join_group`.
            //
            // `client_id` comes from the request HEADER — the only place it
            // exists — and is used to name the member id the coordinator mints.
            let client_id = header.client_id.as_ref().map(|s| s.as_str()).unwrap_or("");
            let body =
                join_group::handle(&conn.facade, &req, api_version, client_id, &conn.peer).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::SyncGroup => {
            let req = match SyncGroupRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("SyncGroup v{api_version} body: {e}")),
            };
            // Parks too, for a follower: until the leader posts the assignment.
            let body = sync_group::handle(&conn.facade, &req).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::Heartbeat => {
            let req = match HeartbeatRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("Heartbeat v{api_version} body: {e}")),
            };
            let body = heartbeat::handle(&conn.facade, &req).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::LeaveGroup => {
            let req = match LeaveGroupRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("LeaveGroup v{api_version} body: {e}")),
            };
            let body = leave_group::handle(&conn.facade, &req).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::OffsetCommit => {
            let req = match OffsetCommitRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("OffsetCommit v{api_version} body: {e}")),
            };
            let body = offset_commit::handle(&conn.facade, &req, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::OffsetFetch => {
            let req = match OffsetFetchRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("OffsetFetch v{api_version} body: {e}")),
            };
            let body = offset_fetch::handle(&conn.facade, &req, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        // ------------------------------------------------------ M7 F1: topics admin
        ApiKey::CreateTopics => {
            let req = match CreateTopicsRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("CreateTopics v{api_version} body: {e}")),
            };
            // The VERSION is passed through, and it is load-bearing rather than
            // decorative: KIP-599's THROTTLING_QUOTA_EXCEEDED is a code only a
            // v6 client knows, so a rate cap has to be answered differently
            // either side of that line (`handlers::create_topics`).
            let body =
                create_topics::handle(&conn.facade, &req, api_version, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::DeleteTopics => {
            let req = match DeleteTopicsRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("DeleteTopics v{api_version} body: {e}")),
            };
            let body = delete_topics::handle(&conn.facade, &req, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::DescribeConfigs => {
            let req = match DescribeConfigsRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("DescribeConfigs v{api_version} body: {e}")),
            };
            let body = describe_configs::handle(&conn.facade, &req, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        // ------------------------------------------------------ M7 F2: groups admin
        ApiKey::ListGroups => {
            let req = match ListGroupsRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("ListGroups v{api_version} body: {e}")),
            };
            // The VERSION is passed through because KIP-518's `states_filter`
            // is a v4 field and is HONOURED: below v4 a client cannot send one
            // and cannot read the state it would have filtered on
            // (`handlers::list_groups`).
            let body =
                list_groups::handle(&conn.facade, &req, api_version, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::DescribeGroups => {
            let req = match DescribeGroupsRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("DescribeGroups v{api_version} body: {e}")),
            };
            let body = describe_groups::handle(&conn.facade, &req, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::DeleteGroups => {
            let req = match DeleteGroupsRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("DeleteGroups v{api_version} body: {e}")),
            };
            let body = delete_groups::handle(&conn.facade, &req, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        // ------------------------------------------------ M7 F3: idempotent producer
        ApiKey::InitProducerId => {
            let req = match InitProducerIdRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("InitProducerId v{api_version} body: {e}")),
            };
            // The ONE handler on this listener that awaits nothing: a producer
            // id is minted from process state, so the grant cannot fail for
            // infrastructure reasons and cannot be slow. That is the property
            // the papercut fix wants (`handlers::init_producer_id`).
            // No longer synchronous: the IDEMPOTENT half still awaits nothing,
            // and the TRANSACTIONAL half claims the id with a compare-and-set
            // against Queen (`handlers::init_producer_id`). The connection id
            // goes with it, because the stage that claim opens belongs to this
            // connection and is dropped when it closes.
            let body =
                init_producer_id::handle(&conn.facade, &req, conn.id, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        // ---------------------------------- M7 F4: the remaining admin surface
        //
        // The ACL family is answered without a facade, without a Queen call and
        // without a version: nothing in `1..=3` changes a field, a code or a
        // shape, so the request version is only the encoding `respond` uses
        // (`handlers::acls`).
        ApiKey::DescribeAcls => {
            let req = match DescribeAclsRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("DescribeAcls v{api_version} body: {e}")),
            };
            let body = acls::describe(&req);
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::CreateAcls => {
            let req = match CreateAclsRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("CreateAcls v{api_version} body: {e}")),
            };
            let body = acls::create(&req);
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::DeleteAcls => {
            let req = match DeleteAclsRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("DeleteAcls v{api_version} body: {e}")),
            };
            let body = acls::delete(&req);
            respond(key, header.correlation_id, &body, api_version)
        }
        // The write half of the config surface. Neither takes `api_version`:
        // nothing in `0..=2` or `0..=1` changes a field, a code or a shape, so
        // the request version is only the encoding `respond` uses. Both are
        // Queen writes and both are awaited.
        ApiKey::AlterConfigs => {
            let req = match AlterConfigsRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("AlterConfigs v{api_version} body: {e}")),
            };
            let body = alter_configs::handle(&conn.facade, &req, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::IncrementalAlterConfigs => {
            let req = match IncrementalAlterConfigsRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => {
                    return Reply::Close(format!(
                        "IncrementalAlterConfigs v{api_version} body: {e}"
                    ))
                }
            };
            let body =
                incremental_alter_configs::handle(&conn.facade, &req, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        // The two remaining admin writes. Neither takes `api_version` either:
        // nothing in `0..=3` or at OffsetDelete's one version changes a field, a
        // code or a shape, so the request version is only the encoding
        // `respond` uses.
        ApiKey::CreatePartitions => {
            let req = match CreatePartitionsRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => {
                    return Reply::Close(format!("CreatePartitions v{api_version} body: {e}"))
                }
            };
            let body = create_partitions::handle(&conn.facade, &req, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::OffsetDelete => {
            let req = match OffsetDeleteRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("OffsetDelete v{api_version} body: {e}")),
            };
            let body = offset_delete::handle(&conn.facade, &req, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        // ------------------------------------------------------ M9: transactions
        //
        // Three of these four await nothing at all: a transaction's partitions,
        // its group and its offsets are STAGED in this process
        // (`crate::txn`), and the only request that talks to Queen is EndTxn —
        // which is the whole design, because that one call is one Postgres
        // transaction carrying every record and every offset together.
        //
        // None of them takes `api_version`: nothing inside `0..=3` changes a
        // field, a code or a shape for any of the four, so the request version
        // is only the encoding `respond` uses.
        ApiKey::AddPartitionsToTxn => {
            let req = match AddPartitionsToTxnRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => {
                    return Reply::Close(format!("AddPartitionsToTxn v{api_version} body: {e}"))
                }
            };
            let body = add_partitions_to_txn::handle(&conn.facade, &req, conn.facade.token());
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::AddOffsetsToTxn => {
            let req = match AddOffsetsToTxnRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("AddOffsetsToTxn v{api_version} body: {e}")),
            };
            let body = add_offsets_to_txn::handle(&conn.facade, &req, conn.facade.token());
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::EndTxn => {
            let req = match EndTxnRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("EndTxn v{api_version} body: {e}")),
            };
            // THE call. Every staged record and every staged offset of this
            // transaction go to Queen in one bundle, or none of them does.
            let body = end_txn::handle(&conn.facade, &req, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        ApiKey::TxnOffsetCommit => {
            let req = match TxnOffsetCommitRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("TxnOffsetCommit v{api_version} body: {e}")),
            };
            let body = txn_offset_commit::handle(&conn.facade, &req, conn.facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        // Unreachable while the table and this match agree, which is the point:
        // a row added to `versions::ADVERTISED` without an arm here is a clean
        // close and a log line, not a wrong answer on the wire.
        _ => Reply::Close(format!("{key:?} is advertised but has no handler")),
    }
}

/// The **v0** SASL flow: this frame is a bare RFC 4616 token.
///
/// There is no response schema on this path — that is the whole difference
/// between v0 and v1 — so success is an empty frame (Kafka writes the SASL
/// server's response token, which for PLAIN is zero bytes) and every failure is
/// a disconnect. A client that chose v0 chose the flow where a refusal has no
/// error code, and it is why v1 exists.
async fn raw_sasl_token(conn: &mut Conn, frame: &Bytes) -> Reply {
    match sasl_authenticate::authenticate(&conn.facade, frame, &mut conn.sasl).await {
        sasl_authenticate::Outcome::Admitted => {
            admit(conn);
            Reply::Send(Bytes::new())
        }
        sasl_authenticate::Outcome::Rejected(why) => {
            refused(conn, &why);
            Reply::Close(format!("SASL v0: {why}"))
        }
        sasl_authenticate::Outcome::Unavailable(why) => {
            unavailable(&why);
            Reply::Close(format!("SASL v0: {why}"))
        }
        // Unreachable: this path is only taken in `AwaitingRawToken`, which is
        // exactly the state `authenticate` accepts. Closing rather than
        // asserting, because a listener does not panic on its own bookkeeping.
        sasl_authenticate::Outcome::OutOfPlace(why) => Reply::Close(format!("SASL v0: {why}")),
    }
}

/// Turn one SaslAuthenticate outcome into what happens to the connection.
fn settle(conn: &mut Conn, outcome: sasl_authenticate::Outcome, reply: Reply) -> Reply {
    match outcome {
        sasl_authenticate::Outcome::Admitted => {
            admit(conn);
            reply
        }
        // The answer goes out and the connection dies behind it: the error code
        // is what stops the client retrying a password that will never work.
        sasl_authenticate::Outcome::Rejected(why) => {
            refused(conn, &why);
            match reply {
                Reply::Send(body) => Reply::SendThenClose(body, why),
                other => other,
            }
        }
        // No answer at all, so the client reads a disconnect and retries. See
        // `handlers::sasl_authenticate`.
        sasl_authenticate::Outcome::Unavailable(why) => {
            unavailable(&why);
            Reply::Close(why)
        }
        // ILLEGAL_SASL_STATE. The answer goes out and the connection lives:
        // nothing has gone wrong with it, and Apache Kafka does not close here
        // either — this is its ordinary request path, not its authenticator.
        sasl_authenticate::Outcome::OutOfPlace(_) => reply,
    }
}

/// Bind the credential this connection just proved to everything it will do
/// with it: the token every call to Queen carries, and the scope its consumer
/// groups live in ([`Facade::authenticated_as`]).
fn admit(conn: &mut Conn) {
    let Some(token) = conn.sasl.token().map(str::to_string) else {
        debug_assert!(false, "admitted a connection that presented no credential");
        return;
    };
    conn.facade = conn.facade.authenticated_as(&token);
    // Two lines for one event, and the split is the anti-flood rule: the INFO
    // is sampled, so a fleet reconnecting produces one line per window with a
    // count; the DEBUG is per connection, for the operator who has turned debug
    // on precisely because they are chasing one. Neither carries the token —
    // the label is what identifies a connection here (`crate::sasl`).
    tracing::debug!(
        target: "kafka",
        user = conn.sasl.user().unwrap_or(""),
        sni = conn.sni.as_deref().unwrap_or(""),
        "sasl authenticated this connection"
    );
    if let Some(suppressed) = AUTH_OK.tick_now() {
        tracing::info!(
            target: "kafka",
            user = conn.sasl.user().unwrap_or(""),
            sni = conn.sni.as_deref().unwrap_or(""),
            suppressed,
            "sasl authenticated"
        );
    }
}

/// A credential Queen refused. At warn and sampled: one stale password in a
/// fleet is one line per reconnect of every consumer in it.
fn refused(conn: &Conn, why: &str) {
    tracing::debug!(
        target: "kafka",
        sni = conn.sni.as_deref().unwrap_or(""),
        "sasl refused this connection: {why}"
    );
    if let Some(suppressed) = AUTH_REFUSED.tick_now() {
        tracing::warn!(
            target: "kafka",
            sni = conn.sni.as_deref().unwrap_or(""),
            suppressed,
            "sasl authentication failed: {why}"
        );
    }
}

/// A credential that could not be CHECKED. A different line from the one above
/// on purpose: this one is about Queen, not about the client, and an operator
/// reading "authentication failed" for an outage would go looking at the wrong
/// thing.
fn unavailable(why: &str) {
    tracing::debug!(target: "kafka", "sasl could not verify a credential: {why}");
    if let Some(suppressed) = AUTH_REFUSED.tick_now() {
        tracing::warn!(
            target: "kafka",
            suppressed,
            "sasl could not verify a credential; the connection was dropped so the client \
             retries: {why}"
        );
    }
}

/// Encode a response header + body at `version`. The header version is derived
/// from the API and the response version, never assumed: ApiVersions alone uses
/// a v0 (non-flexible) header at every version.
fn respond<T: Encodable>(key: ApiKey, correlation_id: i32, body: &T, version: i16) -> Reply {
    let mut out = BytesMut::new();
    let header = ResponseHeader::default().with_correlation_id(correlation_id);
    if let Err(e) = header.encode(&mut out, key.response_header_version(version)) {
        return Reply::Close(format!("{key:?} response header: {e}"));
    }
    if let Err(e) = body.encode(&mut out, version) {
        return Reply::Close(format!("{key:?} v{version} response body: {e}"));
    }
    Reply::Send(out.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::error::ResponseError;
    use kafka_protocol::messages::ApiVersionsResponse;
    use kafka_protocol::protocol::{Message, StrBytes};
    use tokio::net::TcpStream;
    use tokio_util::codec::Decoder;

    // ---------------------------------------------------------------- framing

    fn framed(body: &[u8]) -> BytesMut {
        let mut out = BytesMut::new();
        codec()
            .encode(Bytes::copy_from_slice(body), &mut out)
            .unwrap();
        out
    }

    #[test]
    fn frames_round_trip() {
        let wire = framed(b"queen");
        assert_eq!(&wire[..4], &5u32.to_be_bytes());
        let mut buf = wire;
        let got = codec().decode(&mut buf).unwrap().unwrap();
        assert_eq!(&got[..], b"queen");
        assert!(buf.is_empty());
    }

    /// A frame that arrives in pieces — length prefix, then part of the body,
    /// then the rest — yields nothing until it is whole, and then yields it once.
    #[test]
    fn split_frames_wait_for_the_whole_body() {
        let wire = framed(b"0123456789");
        let mut codec = codec();
        let mut buf = BytesMut::new();

        buf.extend_from_slice(&wire[..3]); // half a length prefix
        assert!(codec.decode(&mut buf).unwrap().is_none());
        buf.extend_from_slice(&wire[3..4]); // prefix complete, body absent
        assert!(codec.decode(&mut buf).unwrap().is_none());
        buf.extend_from_slice(&wire[4..9]); // half the body
        assert!(codec.decode(&mut buf).unwrap().is_none());
        buf.extend_from_slice(&wire[9..]); // the rest
        assert_eq!(&codec.decode(&mut buf).unwrap().unwrap()[..], b"0123456789");
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    /// Pipelining: three frames in a single read come back one at a time, in
    /// order, without another read.
    #[test]
    fn pipelined_frames_in_one_read() {
        let mut buf = BytesMut::new();
        for body in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
            buf.extend_from_slice(&framed(body));
        }
        let mut codec = codec();
        assert_eq!(&codec.decode(&mut buf).unwrap().unwrap()[..], b"one");
        assert_eq!(&codec.decode(&mut buf).unwrap().unwrap()[..], b"two");
        assert_eq!(&codec.decode(&mut buf).unwrap().unwrap()[..], b"three");
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    // ------------------------------------------------- what a length prefix buys
    //
    // The read path is `next_frame` and not the codec, and these are the reason.
    // A `LengthDelimitedCodec` reserves the whole DECLARED length the moment it
    // has the four bytes that declare it, so on a listener facing the internet
    // the cheapest possible message — four bytes and a hang-up — is 100 MiB of
    // heap, per connection, before any credential.

    /// Four bytes claiming the largest legal frame, and then silence: nothing is
    /// allocated for it, and the connection does not get to hold the buffer
    /// open for ever either.
    #[tokio::test(start_paused = true)]
    async fn a_declared_length_allocates_nothing_and_does_not_wait_for_ever() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut server = server;
        let mut inbuf = BytesMut::with_capacity(READ_BUF_BYTES);

        client
            .write_all(&(MAX_FRAME_BYTES as u32).to_be_bytes())
            .await
            .unwrap();
        // The peer says nothing further. The paused clock runs out the
        // partial-frame timeout on its own, because nothing else is runnable.
        let err = next_frame(&mut server, &mut inbuf, MAX_FRAME_BYTES)
            .await
            .expect_err("a frame that never arrived was accepted");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut, "{err}");
        assert!(
            inbuf.capacity() <= 4 * READ_BUF_BYTES,
            "a declared length reserved {} bytes of heap",
            inbuf.capacity()
        );
    }

    /// A connection that says nothing at all between requests is closed, which
    /// is Kafka's own `connections.max.idle.ms`.
    #[tokio::test(start_paused = true)]
    async fn a_connection_that_says_nothing_is_closed() {
        let (_client, mut server) = tokio::io::duplex(64);
        let mut inbuf = BytesMut::new();
        let err = next_frame(&mut server, &mut inbuf, MAX_FRAME_BYTES)
            .await
            .expect_err("an idle connection was held open");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut, "{err}");
    }

    /// ...and a clean close between requests is not an error: that is a client
    /// going away, which every client does.
    #[tokio::test]
    async fn a_clean_close_between_requests_is_not_an_error() {
        let (client, mut server) = tokio::io::duplex(64);
        drop(client);
        let mut inbuf = BytesMut::new();
        assert!(next_frame(&mut server, &mut inbuf, MAX_FRAME_BYTES)
            .await
            .unwrap()
            .is_none());
    }

    /// The limit is compared against the DECLARED length, before the body is
    /// waited for: the pre-auth ceiling refuses on the prefix alone, and the
    /// full one accepts the same prefix and waits.
    #[tokio::test(start_paused = true)]
    async fn the_frame_ceiling_is_checked_on_the_prefix() {
        let declared = (PRE_AUTH_MAX_FRAME_BYTES + 1) as u32;

        let (mut client, mut server) = tokio::io::duplex(64);
        client.write_all(&declared.to_be_bytes()).await.unwrap();
        let mut inbuf = BytesMut::new();
        let err = next_frame(&mut server, &mut inbuf, PRE_AUTH_MAX_FRAME_BYTES)
            .await
            .expect_err("a frame past the pre-auth ceiling was accepted");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{err}");
        assert!(err.to_string().contains("ceiling"), "{err}");

        // The same prefix on an admitted connection is legal and simply waits —
        // proved here by what ends it, which is the timeout and not the
        // ceiling.
        let (mut client, mut server) = tokio::io::duplex(64);
        client.write_all(&declared.to_be_bytes()).await.unwrap();
        let mut inbuf = BytesMut::new();
        let err = next_frame(&mut server, &mut inbuf, MAX_FRAME_BYTES)
            .await
            .expect_err("the body never arrived");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut, "{err}");
    }

    /// A frame that arrives in pieces is still one frame, and the pieces are
    /// not held against the peer: every read that brings something resets the
    /// timer.
    #[tokio::test(start_paused = true)]
    async fn a_frame_that_dribbles_in_is_still_read() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let wire = framed(b"0123456789");
        let feeder = tokio::spawn(async move {
            for byte in wire.clone() {
                // Most of a partial-frame timeout between bytes: slow is not
                // hostile, and a link that is making progress finishes.
                tokio::time::sleep(PARTIAL_FRAME_TIMEOUT / 2).await;
                client.write_all(&[byte]).await.unwrap();
            }
            client
        });
        let mut inbuf = BytesMut::new();
        let frame = next_frame(&mut server, &mut inbuf, MAX_FRAME_BYTES)
            .await
            .unwrap()
            .expect("the frame arrived");
        assert_eq!(&frame[..], b"0123456789");
        feeder.await.unwrap();
    }

    /// An oversized length prefix is refused on the prefix alone — the body is
    /// never waited for and nothing is reserved for it.
    #[test]
    fn oversized_frames_are_rejected_on_the_prefix() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&((MAX_FRAME_BYTES + 1) as u32).to_be_bytes());
        let err = codec().decode(&mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

        // The largest legal frame is still accepted at the prefix (it simply
        // waits for a body that will not arrive in this test).
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&(MAX_FRAME_BYTES as u32).to_be_bytes());
        assert!(codec().decode(&mut buf).unwrap().is_none());
    }

    // ----------------------------------------------------------- request head

    /// Build a request frame body the way a client does: header at the version
    /// the API mandates for `api_version`, then the request payload.
    fn api_versions_request(api_version: i16, correlation_id: i32) -> Bytes {
        let mut out = BytesMut::new();
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::ApiVersions as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")));
        header
            .encode(
                &mut out,
                ApiKey::ApiVersions.request_header_version(api_version),
            )
            .unwrap();
        // Above our window there is no body layout to write: the facade must
        // not read it either.
        if api_version <= ApiVersionsRequest::VERSIONS.max {
            ApiVersionsRequest::default()
                .with_client_software_name(StrBytes::from_static_str("queen-kafka-test"))
                .with_client_software_version(StrBytes::from_static_str("1.3.0"))
                .encode(&mut out, api_version)
                .unwrap();
        }
        out.freeze()
    }

    /// The header version is a function of (api_key, api_version): v1 below
    /// ApiVersions v3, flexible v2 from v3 up. Decoding with the wrong one
    /// desynchronises the frame, so pin that we ask the schema, not a constant.
    #[test]
    fn header_decodes_at_the_version_the_api_mandates() {
        for (api_version, want_header) in [(0i16, 1i16), (2, 1), (3, 2), (9, 2)] {
            assert_eq!(
                ApiKey::ApiVersions.request_header_version(api_version),
                want_header
            );
            let mut frame = api_versions_request(api_version, 4242);
            let header = RequestHeader::decode(&mut frame, want_header).unwrap();
            assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
            assert_eq!(header.request_api_version, api_version);
            assert_eq!(header.correlation_id, 4242);
            assert_eq!(header.client_id.unwrap().as_str(), "queen-kafka-test");
        }
    }

    // ------------------------------------------------------------- dispatched

    /// The peer address the accept loop would have handed a connection, in the
    /// shape [`kafka_host`] makes.
    const TEST_PEER: &str = "/127.0.0.1";

    /// A facade wired to a fake Queen with one queue. The ApiVersions tests
    /// never reach it; the Metadata, Produce and group ones do.
    fn facade() -> Arc<Facade> {
        Arc::new(crate::handlers::testing::facade(&[("orders", 2)]))
    }

    /// A connection on it, with no TLS and no SASL — every test below that is
    /// not ABOUT the listener's policy sees the shape a plain OSS deployment
    /// has.
    fn conn() -> Conn {
        Conn::new(&facade(), None, TEST_PEER.to_string())
    }

    fn sent(reply: Reply) -> Bytes {
        match reply {
            Reply::Send(b) => b,
            other => panic!("expected a response, got {other:?}"),
        }
    }

    fn closed(reply: Reply) -> String {
        match reply {
            Reply::Close(why) => why,
            other => panic!("expected a close, got {other:?}"),
        }
    }

    /// The client side of the exchange, decoded with `kafka-protocol`'s own
    /// types: header at the version the API mandates, then the body.
    fn decode_response(mut wire: Bytes, version: i16) -> (i32, ApiVersionsResponse) {
        let header = ResponseHeader::decode(
            &mut wire,
            ApiKey::ApiVersions.response_header_version(version),
        )
        .unwrap();
        let body = ApiVersionsResponse::decode(&mut wire, version).unwrap();
        assert!(wire.is_empty(), "{} trailing bytes", wire.len());
        (header.correlation_id, body)
    }

    #[tokio::test]
    async fn api_versions_round_trips() {
        let mut f = conn();
        for version in [0i16, 1, 2, 3] {
            let reply = dispatch(&mut f, api_versions_request(version, 7)).await;
            let (correlation_id, body) = decode_response(sent(reply), version);
            assert_eq!(correlation_id, 7);
            assert_eq!(body.error_code, 0);
            assert_eq!(body.api_keys.len(), versions::ADVERTISED.len());
            // The table, in the table's order.
            for (got, want) in body.api_keys.iter().zip(versions::ADVERTISED) {
                assert_eq!(got.api_key, want.key as i16);
                assert_eq!(got.min_version, want.min);
                assert_eq!(got.max_version, want.max);
            }
        }
    }

    /// The bootstrap quirk: a version above our window is answered
    /// UNSUPPORTED_VERSION with a body encoded at v0, so a client that does not
    /// know what we speak can parse the refusal and downgrade — and the body
    /// names the version to downgrade TO, which is the whole reason a client
    /// reads this response at all.
    #[tokio::test]
    async fn above_our_window_falls_back_to_a_v0_body() {
        let wire = sent(dispatch(&mut conn(), api_versions_request(9, 11)).await);

        // Exact size is the strongest pin on "encoded at v0": a v0 header is 4
        // bytes of correlation id, and a v0 body is error_code (2) + an array of
        // one entry (4 + 6). At v1+ the body would also carry
        // throttle_time_ms. Apache Kafka answers the same 12-byte body.
        assert_eq!(wire.len(), 16, "not a v0-encoded response: {wire:?}");

        let (correlation_id, body) = decode_response(wire, 0);
        assert_eq!(correlation_id, 11);
        assert_eq!(body.error_code, 35); // UNSUPPORTED_VERSION
        let advertised = versions::lookup(ApiKey::ApiVersions as i16).unwrap();
        assert_eq!(body.api_keys.len(), 1);
        assert_eq!(body.api_keys[0].api_key, ApiKey::ApiVersions as i16);
        assert_eq!(body.api_keys[0].min_version, advertised.min);
        assert_eq!(body.api_keys[0].max_version, advertised.max);
    }

    #[tokio::test]
    async fn unadvertised_api_keys_close_cleanly() {
        let mut f = conn();
        // A real Kafka API this build does not offer: the KIP-848 group
        // protocol, which PLAN_QUEEN_KAFKA.md excludes by name.
        let mut frame = BytesMut::new();
        frame.extend_from_slice(&(ApiKey::ConsumerGroupHeartbeat as i16).to_be_bytes());
        frame.extend_from_slice(&0i16.to_be_bytes());
        frame.extend_from_slice(&1i32.to_be_bytes());
        assert!(
            closed(dispatch(&mut f, frame.freeze()).await).contains(&format!(
                "api key {}",
                ApiKey::ConsumerGroupHeartbeat as i16
            ))
        );

        // Not a Kafka API key at all.
        let mut frame = BytesMut::new();
        frame.extend_from_slice(&30_000i16.to_be_bytes());
        frame.extend_from_slice(&0i16.to_be_bytes());
        assert!(closed(dispatch(&mut f, frame.freeze()).await).contains("api key 30000"));
    }

    /// An advertised API at a version above its window has no body layout we
    /// could read and no version we could answer at, so the connection closes —
    /// which is what Apache Kafka does with an unsupported version too. Only
    /// ApiVersions has the downgrade quirk, and it is tested above.
    #[tokio::test]
    async fn an_advertised_api_above_its_window_closes() {
        let mut frame = BytesMut::new();
        frame.extend_from_slice(&(ApiKey::Metadata as i16).to_be_bytes());
        frame.extend_from_slice(&12i16.to_be_bytes());
        frame.extend_from_slice(&1i32.to_be_bytes());
        let why = closed(dispatch(&mut conn(), frame.freeze()).await);
        assert!(why.contains("Metadata v12"), "{why}");
        assert!(why.contains("0..=9"), "{why}");
    }

    /// Garbage never panics: it closes, whatever shape it has.
    #[tokio::test]
    async fn garbage_closes_instead_of_panicking() {
        let mut f = conn();
        for junk in [
            Bytes::new(),
            Bytes::from_static(&[0x00]),
            Bytes::from_static(&[0x00, 0x12, 0x00]),
            // The right api key and version, then nothing where the header is.
            Bytes::from_static(&[0x00, 0x12, 0x00, 0x03]),
            // The right api key and version, then a header that lies about the
            // length of its client id.
            Bytes::from_static(&[0x00, 0x12, 0x00, 0x00, 0, 0, 0, 1, 0x7f, 0xff]),
            Bytes::from_static(&[0xff; 64]),
            // Metadata (api key 3) v9, then nothing where its body is.
            Bytes::from_static(&[0x00, 0x03, 0x00, 0x09]),
        ] {
            closed(dispatch(&mut f, junk).await);
        }
    }

    /// The contract `versions.rs` exists to keep: every `(key, version)` the
    /// table advertises reaches a handler. A row added without an arm in
    /// `dispatch` fails here rather than answering a client with a close.
    #[tokio::test]
    async fn every_advertised_version_is_dispatched() {
        let mut f = conn();
        for api in versions::ADVERTISED {
            for version in api.min..=api.max {
                let frame = match api.key {
                    ApiKey::ApiVersions => api_versions_request(version, 1),
                    ApiKey::Metadata => metadata_request(version, 1, Some(&["orders"])),
                    ApiKey::Produce => produce_request(version, 1, 1),
                    ApiKey::Fetch => fetch_request(version, 1),
                    ApiKey::ListOffsets => list_offsets_request(version, 1),
                    ApiKey::FindCoordinator => find_coordinator_request(version, 1),
                    // A group per version, because a JoinGroup that is not the
                    // FIRST of its group reopens a join window that waits for
                    // the members already in it — a minute of rebalance
                    // timeout, in a test that is about the dispatch table.
                    ApiKey::JoinGroup => join_group_request(version, 1, &format!("walk-{version}")),
                    ApiKey::SyncGroup => sync_group_request(version, 1),
                    ApiKey::Heartbeat => heartbeat_request(version, 1),
                    ApiKey::LeaveGroup => leave_group_request(version, 1),
                    ApiKey::OffsetCommit => offset_commit_request(version, 1),
                    ApiKey::OffsetFetch => offset_fetch_request(version, 1),
                    // On this connection SASL is OFF, so both of these are
                    // answered ILLEGAL_SASL_STATE — which is still a RESPONSE,
                    // which is what this test is about. The conversation itself
                    // is exercised below.
                    ApiKey::SaslHandshake => sasl_handshake_request(version, 1, "PLAIN"),
                    ApiKey::SaslAuthenticate => sasl_authenticate_request(version, 1, b""),
                    // A topic per version, because the second create of one
                    // name is answered TOPIC_ALREADY_EXISTS — still a response,
                    // but this test is about the dispatch table and a name that
                    // means the same thing at every version keeps it that way.
                    ApiKey::CreateTopics => {
                        create_topics_request(version, 1, &format!("walk-{version}"))
                    }
                    ApiKey::DeleteTopics => delete_topics_request(version, 1),
                    ApiKey::DescribeConfigs => describe_configs_request(version, 1),
                    ApiKey::ListGroups => list_groups_request(version, 1),
                    ApiKey::DescribeGroups => describe_groups_request(version, 1),
                    // A group per version, and one nobody has ever heard of:
                    // the answer is GROUP_ID_NOT_FOUND, which is a RESPONSE,
                    // which is what this test is about. Deleting the group the
                    // JoinGroup rows above just made would empty the fixture
                    // under every later version of the walk.
                    ApiKey::DeleteGroups => {
                        delete_groups_request(version, 1, &format!("walk-{version}"))
                    }
                    // A fresh grant at every version: below v3 the request has
                    // no producer id field at all, and at v3+ this sends the
                    // "mint me one" sentinel, which is the arm every client
                    // opens with.
                    ApiKey::InitProducerId => init_producer_id_request(version, 1),
                    // The ACL family: one filter, one creation, one filter. No
                    // per-version name trick is needed and that is the point of
                    // the family — nothing is created, so no version of the walk
                    // can leave state behind for the next one to trip over.
                    ApiKey::DescribeAcls => describe_acls_request(version, 1),
                    ApiKey::CreateAcls => create_acls_request(version, 1),
                    ApiKey::DeleteAcls => delete_acls_request(version, 1),
                    // The config write half: a TOPIC resource on the fixture's
                    // one queue, which has no config record, so every version
                    // answers the same untracked refusal and none of them
                    // writes anything for the next version to trip over.
                    ApiKey::AlterConfigs => alter_configs_request(version, 1),
                    ApiKey::IncrementalAlterConfigs => {
                        incremental_alter_configs_request(version, 1)
                    }
                    // The two remaining admin writes. Neither leaves anything
                    // behind for the next version of the walk: CreatePartitions
                    // never writes at all, and the OffsetDelete names a group
                    // per version so that the first pass's delete cannot turn
                    // the second pass into GROUP_ID_NOT_FOUND — still a
                    // response, but a different one, and this test is about the
                    // dispatch table.
                    ApiKey::CreatePartitions => create_partitions_request(version, 1),
                    ApiKey::OffsetDelete => {
                        offset_delete_request(version, 1, &format!("walk-{version}"))
                    }
                    // The transaction family. Its OWN transactional id per
                    // version, and the per-version trick is REQUIRED here
                    // rather than convenient: the four requests share one piece
                    // of state, so a fixture reusing one id would meet the
                    // stage the previous version left and be answered a
                    // different error — a fenced epoch, a second group, a
                    // transaction already committing.
                    //
                    // Each id is bound first, because these four APIs answer a
                    // request for an id no InitProducerId claimed with
                    // INVALID_TXN_STATE, which is a RESPONSE and would pass
                    // this test while exercising nothing.
                    ApiKey::AddPartitionsToTxn => {
                        bind_txn(&f, version);
                        add_partitions_to_txn_request(version, 1, &txn_id(version))
                    }
                    ApiKey::AddOffsetsToTxn => {
                        bind_txn(&f, version);
                        add_offsets_to_txn_request(version, 1, &txn_id(version))
                    }
                    ApiKey::EndTxn => {
                        bind_txn(&f, version);
                        end_txn_request(version, 1, &txn_id(version))
                    }
                    ApiKey::TxnOffsetCommit => {
                        bind_txn(&f, version);
                        txn_offset_commit_request(version, 1, &txn_id(version))
                    }
                    other => panic!("{other:?} is advertised but this test cannot build one"),
                };
                match dispatch(&mut f, frame).await {
                    Reply::Send(_) => {}
                    other => {
                        panic!(
                            "{:?} v{version} is advertised but answered {other:?}",
                            api.key
                        )
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------- metadata

    /// A Metadata request the way a client writes it.
    fn metadata_request(api_version: i16, correlation_id: i32, topics: Option<&[&str]>) -> Bytes {
        use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
        use kafka_protocol::messages::TopicName;

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::Metadata as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::Metadata.request_header_version(api_version),
            )
            .unwrap();
        MetadataRequest::default()
            .with_topics(topics.map(|ts| {
                ts.iter()
                    .map(|t| {
                        MetadataRequestTopic::default()
                            .with_name(Some(TopicName(StrBytes::from_string(t.to_string()))))
                    })
                    .collect()
            }))
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// End to end through the dispatcher: header, body, and a topic a client can
    /// act on.
    #[tokio::test]
    async fn metadata_round_trips_through_dispatch() {
        use kafka_protocol::messages::MetadataResponse;

        for version in [0i16, 1, 3, 4, 8, 9] {
            let wire = sent(
                dispatch(
                    &mut conn(),
                    metadata_request(version, 77, Some(&["orders"])),
                )
                .await,
            );
            let mut buf = wire;
            let header =
                ResponseHeader::decode(&mut buf, ApiKey::Metadata.response_header_version(version))
                    .unwrap();
            let body = MetadataResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());

            assert_eq!(header.correlation_id, 77);
            assert_eq!(body.brokers.len(), 1, "v{version}");
            assert_eq!(body.brokers[0].host.as_str(), "kafka.example.com");
            assert_eq!(body.topics.len(), 1, "v{version}");
            assert_eq!(body.topics[0].error_code, 0, "v{version}");
            // 2 live lanes, 4 configured.
            assert_eq!(body.topics[0].partitions.len(), 4, "v{version}");
        }
    }

    // ---------------------------------------------------------------- produce

    /// A Produce request the way a client writes it: one record for `orders`
    /// partition 0, at `acks`.
    fn produce_request(api_version: i16, correlation_id: i32, acks: i16) -> Bytes {
        use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
        use kafka_protocol::messages::{ProduceRequest, TopicName};
        use kafka_protocol::records::{
            Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
            NO_PRODUCER_ID,
        };

        let record = Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp: 1_756_000_000_000,
            key: None,
            value: Some(Bytes::from_static(b"queen")),
            headers: Default::default(),
        };
        let mut records = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut records,
            [&record],
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .unwrap();

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::Produce as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::Produce.request_header_version(api_version),
            )
            .unwrap();
        ProduceRequest::default()
            .with_acks(acks)
            .with_timeout_ms(30_000)
            .with_topic_data(vec![TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partition_data(vec![PartitionProduceData::default()
                    .with_index(0)
                    .with_records(Some(records.freeze()))])])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    #[tokio::test]
    async fn produce_round_trips_through_dispatch() {
        use kafka_protocol::messages::ProduceResponse;

        for version in [3i16, 5, 8, 9] {
            let wire = sent(dispatch(&mut conn(), produce_request(version, 55, -1)).await);
            let mut buf = wire;
            let header =
                ResponseHeader::decode(&mut buf, ApiKey::Produce.response_header_version(version))
                    .unwrap();
            let body = ProduceResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());

            assert_eq!(header.correlation_id, 55);
            assert_eq!(body.responses.len(), 1, "v{version}");
            let p = &body.responses[0].partition_responses[0];
            assert_eq!(p.error_code, 0, "v{version}");
            assert_eq!(p.base_offset, 0, "v{version}");
        }
    }

    /// The acks=0 contract at the dispatch boundary: no frame is produced at
    /// all, so nothing is written back onto the connection.
    #[tokio::test]
    async fn an_acks_zero_produce_writes_no_frame() {
        let reply = dispatch(&mut conn(), produce_request(9, 56, 0)).await;
        assert!(
            matches!(reply, Reply::Silent),
            "acks=0 answered {reply:?} instead of nothing"
        );
    }

    /// ...and the connection stays usable afterwards: the request that follows
    /// a silent one gets ITS answer, and not a stale frame.
    #[tokio::test]
    async fn a_connection_survives_an_acks_zero_produce() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, facade(), None));

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut wire = BytesMut::new();
        let mut codec = codec();
        codec.encode(produce_request(9, 200, 0), &mut wire).unwrap();
        codec
            .encode(api_versions_request(3, 201), &mut wire)
            .unwrap();
        client.write_all(&wire).await.unwrap();

        let mut buf = BytesMut::new();
        let frame = loop {
            if let Some(f) = codec.decode(&mut buf).unwrap() {
                break f;
            }
            assert!(
                client.read_buf(&mut buf).await.unwrap() > 0,
                "server hung up"
            );
        };
        // The FIRST frame back belongs to the ApiVersions request: the produce
        // produced none.
        let (correlation_id, body) = decode_response(frame.freeze(), 3);
        assert_eq!(correlation_id, 201);
        assert_eq!(body.error_code, 0);
    }

    // ----------------------------------------------------------------- fetch

    /// A Fetch request the way a client writes it: one partition of `orders`
    /// from offset 0, with no long poll (the dispatch tests must not park).
    fn fetch_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
        use kafka_protocol::messages::{FetchRequest, TopicName};

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::Fetch as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(&mut out, ApiKey::Fetch.request_header_version(api_version))
            .unwrap();
        FetchRequest::default()
            .with_replica_id((-1).into())
            .with_max_wait_ms(0)
            .with_min_bytes(1)
            .with_max_bytes(1024 * 1024)
            .with_topics(vec![FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(0)
                    .with_fetch_offset(0)
                    .with_partition_max_bytes(1024 * 1024)])])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    #[tokio::test]
    async fn fetch_round_trips_through_dispatch() {
        use kafka_protocol::messages::FetchResponse;

        for version in [4i16, 5, 6] {
            let wire = sent(dispatch(&mut conn(), fetch_request(version, 88)).await);
            let mut buf = wire;
            let header =
                ResponseHeader::decode(&mut buf, ApiKey::Fetch.response_header_version(version))
                    .unwrap();
            let body = FetchResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());

            assert_eq!(header.correlation_id, 88);
            assert_eq!(body.responses.len(), 1, "v{version}");
            let p = &body.responses[0].partitions[0];
            assert_eq!(p.error_code, 0, "v{version}");
            assert_eq!(p.high_watermark, 0, "v{version}");
        }
    }

    // ---------------------------------------------------------- list offsets

    /// A ListOffsets request the way a client writes it: the latest offset of
    /// one partition of `orders`.
    fn list_offsets_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::list_offsets_request::{
            ListOffsetsPartition, ListOffsetsTopic,
        };
        use kafka_protocol::messages::{ListOffsetsRequest, TopicName};

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::ListOffsets as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::ListOffsets.request_header_version(api_version),
            )
            .unwrap();
        ListOffsetsRequest::default()
            .with_replica_id((-1).into())
            .with_topics(vec![ListOffsetsTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![ListOffsetsPartition::default()
                    .with_partition_index(0)
                    .with_timestamp(-1)])])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    #[tokio::test]
    async fn list_offsets_round_trips_through_dispatch() {
        use kafka_protocol::messages::ListOffsetsResponse;

        for version in [1i16, 2, 4, 5] {
            let wire = sent(dispatch(&mut conn(), list_offsets_request(version, 99)).await);
            let mut buf = wire;
            let header = ResponseHeader::decode(
                &mut buf,
                ApiKey::ListOffsets.response_header_version(version),
            )
            .unwrap();
            let body = ListOffsetsResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());

            assert_eq!(header.correlation_id, 99);
            let p = &body.topics[0].partitions[0];
            assert_eq!(p.error_code, 0, "v{version}");
            assert_eq!(p.offset, 0, "v{version}: an empty lane starts at 0");
        }
    }

    // --------------------------------------------------------------- groups

    /// The header for a group request, at the version the API mandates.
    fn group_header(key: ApiKey, api_version: i16, correlation_id: i32) -> BytesMut {
        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(key as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(&mut out, key.request_header_version(api_version))
            .unwrap();
        out
    }

    fn find_coordinator_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::FindCoordinatorRequest;
        let mut out = group_header(ApiKey::FindCoordinator, api_version, correlation_id);
        FindCoordinatorRequest::default()
            .with_key(StrBytes::from_static_str("orders-consumer"))
            .with_key_type(0)
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    fn sasl_handshake_request(api_version: i16, correlation_id: i32, mechanism: &str) -> Bytes {
        use kafka_protocol::messages::SaslHandshakeRequest;
        let mut out = group_header(ApiKey::SaslHandshake, api_version, correlation_id);
        SaslHandshakeRequest::default()
            .with_mechanism(StrBytes::from_string(mechanism.to_string()))
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    fn sasl_authenticate_request(api_version: i16, correlation_id: i32, token: &[u8]) -> Bytes {
        use kafka_protocol::messages::SaslAuthenticateRequest;
        let mut out = group_header(ApiKey::SaslAuthenticate, api_version, correlation_id);
        SaslAuthenticateRequest::default()
            .with_auth_bytes(Bytes::copy_from_slice(token))
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// A first JoinGroup: no member id, so v4 answers MEMBER_ID_REQUIRED and
    /// v0..v3 join with a minted one.
    fn join_group_request(api_version: i16, correlation_id: i32, group: &str) -> Bytes {
        use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
        use kafka_protocol::messages::{GroupId, JoinGroupRequest};
        let mut out = group_header(ApiKey::JoinGroup, api_version, correlation_id);
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(60_000)
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::from_static(b"subscription"))])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    fn sync_group_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::{GroupId, SyncGroupRequest};
        let mut out = group_header(ApiKey::SyncGroup, api_version, correlation_id);
        SyncGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("orders-consumer")))
            .with_member_id(StrBytes::from_static_str("member-1"))
            .with_generation_id(1)
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    fn heartbeat_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::{GroupId, HeartbeatRequest};
        let mut out = group_header(ApiKey::Heartbeat, api_version, correlation_id);
        HeartbeatRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("orders-consumer")))
            .with_member_id(StrBytes::from_static_str("member-1"))
            .with_generation_id(1)
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    fn leave_group_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::{GroupId, LeaveGroupRequest};
        let mut out = group_header(ApiKey::LeaveGroup, api_version, correlation_id);
        LeaveGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("orders-consumer")))
            .with_member_id(StrBytes::from_static_str("member-1"))
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// A simple consumer's commit: generation -1 and no member id, which is the
    /// one commit shape that needs no membership.
    fn offset_commit_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::offset_commit_request::{
            OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        };
        use kafka_protocol::messages::{GroupId, OffsetCommitRequest, TopicName};
        let mut out = group_header(ApiKey::OffsetCommit, api_version, correlation_id);
        OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("orders-consumer")))
            .with_member_id(StrBytes::from_static_str(""))
            .with_generation_id_or_member_epoch(-1)
            .with_topics(vec![OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![OffsetCommitRequestPartition::default()
                    .with_partition_index(0)
                    .with_committed_offset(41)
                    .with_committed_metadata(Some(StrBytes::from_static_str("")))])])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    fn offset_fetch_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
        use kafka_protocol::messages::{GroupId, OffsetFetchRequest, TopicName};
        let mut out = group_header(ApiKey::OffsetFetch, api_version, correlation_id);
        OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("orders-consumer")))
            .with_topics(Some(vec![OffsetFetchRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partition_indexes(vec![0])]))
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// The group conversation, end to end through the dispatcher and over one
    /// connection's worth of frames: find the coordinator, join (twice, for the
    /// member id), sync, heartbeat, commit, fetch back, leave.
    ///
    /// It is here rather than in the handlers because it is the DISPATCH that
    /// is being proved: seven APIs, seven response schemas, one correlation id
    /// each, all decodable by a client.
    #[tokio::test]
    async fn a_whole_group_conversation_round_trips_through_dispatch() {
        use kafka_protocol::messages::{
            FindCoordinatorResponse, HeartbeatResponse, JoinGroupResponse, LeaveGroupResponse,
            OffsetCommitResponse, OffsetFetchResponse, SyncGroupResponse,
        };

        let mut f = conn();
        let decode = |key: ApiKey, version: i16, wire: Bytes| -> Bytes {
            let mut buf = wire;
            let header =
                ResponseHeader::decode(&mut buf, key.response_header_version(version)).unwrap();
            assert_eq!(header.correlation_id, 1);
            buf
        };

        // 1. Where is the coordinator? Here.
        let mut buf = decode(
            ApiKey::FindCoordinator,
            3,
            sent(dispatch(&mut f, find_coordinator_request(3, 1)).await),
        );
        let coordinator = FindCoordinatorResponse::decode(&mut buf, 3).unwrap();
        assert_eq!(coordinator.error_code, 0);
        assert_eq!(coordinator.host.as_str(), "kafka.example.com");

        // 2. The member id round trip, then the join itself.
        let mut buf = decode(
            ApiKey::JoinGroup,
            4,
            sent(dispatch(&mut f, join_group_request(4, 1, "orders-consumer")).await),
        );
        let minted = JoinGroupResponse::decode(&mut buf, 4).unwrap();
        assert_eq!(minted.error_code, 79, "MEMBER_ID_REQUIRED");
        let member = minted.member_id.clone();

        // The same request with the id in it: one field's difference.
        let wire = {
            use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
            use kafka_protocol::messages::{GroupId, JoinGroupRequest};
            let mut out = group_header(ApiKey::JoinGroup, 4, 1);
            JoinGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from_static_str("orders-consumer")))
                .with_member_id(member.clone())
                .with_protocol_type(StrBytes::from_static_str("consumer"))
                .with_session_timeout_ms(10_000)
                .with_rebalance_timeout_ms(60_000)
                .with_protocols(vec![JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_static_str("range"))
                    .with_metadata(Bytes::from_static(b"subscription"))])
                .encode(&mut out, 4)
                .unwrap();
            out.freeze()
        };
        let mut buf = decode(ApiKey::JoinGroup, 4, sent(dispatch(&mut f, wire).await));
        let joined = JoinGroupResponse::decode(&mut buf, 4).unwrap();
        assert_eq!(joined.error_code, 0);
        assert_eq!(joined.leader, member, "the only member leads");
        let generation = joined.generation_id;

        // 3. The leader syncs, assigning itself everything.
        let sync = {
            use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
            use kafka_protocol::messages::{GroupId, SyncGroupRequest};
            let mut out = group_header(ApiKey::SyncGroup, 2, 1);
            SyncGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from_static_str("orders-consumer")))
                .with_member_id(member.clone())
                .with_generation_id(generation)
                .with_assignments(vec![SyncGroupRequestAssignment::default()
                    .with_member_id(member.clone())
                    .with_assignment(Bytes::from_static(b"every-partition"))])
                .encode(&mut out, 2)
                .unwrap();
            out.freeze()
        };
        let mut buf = decode(ApiKey::SyncGroup, 2, sent(dispatch(&mut f, sync).await));
        let synced = SyncGroupResponse::decode(&mut buf, 2).unwrap();
        assert_eq!(synced.error_code, 0);
        assert_eq!(&synced.assignment[..], b"every-partition");

        // 4. A heartbeat from the member of the current generation.
        let beat = {
            use kafka_protocol::messages::{GroupId, HeartbeatRequest};
            let mut out = group_header(ApiKey::Heartbeat, 2, 1);
            HeartbeatRequest::default()
                .with_group_id(GroupId(StrBytes::from_static_str("orders-consumer")))
                .with_member_id(member.clone())
                .with_generation_id(generation)
                .encode(&mut out, 2)
                .unwrap();
            out.freeze()
        };
        let mut buf = decode(ApiKey::Heartbeat, 2, sent(dispatch(&mut f, beat).await));
        assert_eq!(
            HeartbeatResponse::decode(&mut buf, 2).unwrap().error_code,
            0
        );

        // 5. Commit an offset, and read it back.
        let commit = {
            use kafka_protocol::messages::offset_commit_request::{
                OffsetCommitRequestPartition, OffsetCommitRequestTopic,
            };
            use kafka_protocol::messages::{GroupId, OffsetCommitRequest, TopicName};
            let mut out = group_header(ApiKey::OffsetCommit, 6, 1);
            OffsetCommitRequest::default()
                .with_group_id(GroupId(StrBytes::from_static_str("orders-consumer")))
                .with_member_id(member.clone())
                .with_generation_id_or_member_epoch(generation)
                .with_topics(vec![OffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from_static_str("orders")))
                    .with_partitions(vec![OffsetCommitRequestPartition::default()
                        .with_partition_index(0)
                        .with_committed_offset(41)
                        .with_committed_metadata(Some(StrBytes::from_static_str(
                            "batch-7",
                        )))])])
                .encode(&mut out, 6)
                .unwrap();
            out.freeze()
        };
        let mut buf = decode(
            ApiKey::OffsetCommit,
            6,
            sent(dispatch(&mut f, commit).await),
        );
        let committed = OffsetCommitResponse::decode(&mut buf, 6).unwrap();
        assert_eq!(committed.topics[0].partitions[0].error_code, 0);

        let mut buf = decode(
            ApiKey::OffsetFetch,
            7,
            sent(dispatch(&mut f, offset_fetch_request(7, 1)).await),
        );
        let fetched = OffsetFetchResponse::decode(&mut buf, 7).unwrap();
        assert_eq!(fetched.error_code, 0);
        assert_eq!(fetched.topics[0].partitions[0].committed_offset, 41);
        assert_eq!(
            fetched.topics[0].partitions[0]
                .metadata
                .as_ref()
                .unwrap()
                .as_str(),
            "batch-7"
        );

        // 6. And goodbye.
        let leave = {
            use kafka_protocol::messages::{GroupId, LeaveGroupRequest};
            let mut out = group_header(ApiKey::LeaveGroup, 2, 1);
            LeaveGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from_static_str("orders-consumer")))
                .with_member_id(member)
                .encode(&mut out, 2)
                .unwrap();
            out.freeze()
        };
        let mut buf = decode(ApiKey::LeaveGroup, 2, sent(dispatch(&mut f, leave).await));
        assert_eq!(
            LeaveGroupResponse::decode(&mut buf, 2).unwrap().error_code,
            0
        );
    }

    /// The whole loop, over a real socket: two pipelined requests in one write
    /// come back as two responses, in order, on one connection.
    #[tokio::test]
    async fn serves_pipelined_requests_over_a_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, facade(), None));

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut wire = BytesMut::new();
        let mut codec = codec();
        codec
            .encode(api_versions_request(3, 100), &mut wire)
            .unwrap();
        codec
            .encode(api_versions_request(3, 101), &mut wire)
            .unwrap();
        client.write_all(&wire).await.unwrap();

        let mut buf = BytesMut::new();
        let mut got = Vec::new();
        while got.len() < 2 {
            match codec.decode(&mut buf).unwrap() {
                Some(f) => got.push(decode_response(f.freeze(), 3)),
                None => {
                    assert!(
                        client.read_buf(&mut buf).await.unwrap() > 0,
                        "server hung up"
                    );
                }
            }
        }
        assert_eq!(got[0].0, 100);
        assert_eq!(got[1].0, 101);
        assert_eq!(got[0].1.error_code, 0);
        assert_eq!(got[1].1.error_code, 0);
    }

    // ------------------------------------------------- M5: TLS, SNI and SASL

    /// One end of a live connection, framed the way a client frames it.
    ///
    /// Generic over the socket so the plaintext and TLS tests drive the same
    /// exchange: what M5 changed is the transport and the handshake in front of
    /// the frames, not the frames.
    struct Peer<S> {
        stream: S,
        codec: LengthDelimitedCodec,
        buf: BytesMut,
    }

    impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> Peer<S> {
        fn new(stream: S) -> Peer<S> {
            Peer {
                stream,
                codec: codec(),
                buf: BytesMut::new(),
            }
        }

        async fn send(&mut self, frame: Bytes) {
            let mut wire = BytesMut::new();
            self.codec.encode(frame, &mut wire).unwrap();
            self.stream.write_all(&wire).await.unwrap();
            self.stream.flush().await.unwrap();
        }

        /// The next frame, or `None` when the server closed instead of
        /// answering — which is itself a protocol answer here (`Reply::Close`).
        async fn recv(&mut self) -> Option<Bytes> {
            loop {
                if let Some(f) = self.codec.decode(&mut self.buf).unwrap() {
                    return Some(f.freeze());
                }
                match self.stream.read_buf(&mut self.buf).await {
                    Ok(0) | Err(_) => return None,
                    Ok(_) => {}
                }
            }
        }
    }

    /// A facade over a fake Queen that only accepts `TENANT_TOKEN`, with the
    /// lane wrapper that makes a forwarded `Host` observable.
    const TENANT_TOKEN: &str = "eyJhbGciOi.tenant.token";

    fn listener_facade(
        policy: crate::Policy,
    ) -> (Arc<Facade>, Arc<crate::queen::testing::FakeQueen>) {
        let api = crate::queen::testing::FakeQueen::with(&[("orders", 2)]);
        // A listener that authenticates is one whose Queen has a credential to
        // check against; a listener that does not is one whose Queen takes
        // whatever `QUEEN_TOKEN` is, including nothing.
        if policy.sasl_plain {
            api.accept_only(TENANT_TOKEN);
        }
        let routed = crate::queen::testing::Routed::over(Arc::clone(&api));
        let facade = crate::handlers::testing::over(routed, policy);
        (Arc::new(facade), api)
    }

    /// `[authzid]\0username\0password`, as a client's SASL library builds it.
    fn plain_bytes(user: &str, password: &str) -> Vec<u8> {
        format!("\0{user}\0{password}").into_bytes()
    }

    /// The SASL/PLAIN v1 conversation, up to and including the answer.
    async fn authenticate<S>(peer: &mut Peer<S>, password: &str) -> i16
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        use kafka_protocol::messages::{SaslAuthenticateResponse, SaslHandshakeResponse};

        peer.send(sasl_handshake_request(1, 10, "PLAIN")).await;
        let mut wire = peer.recv().await.expect("the handshake was answered");
        ResponseHeader::decode(&mut wire, ApiKey::SaslHandshake.response_header_version(1))
            .unwrap();
        assert_eq!(
            SaslHandshakeResponse::decode(&mut wire, 1)
                .unwrap()
                .error_code,
            0
        );

        peer.send(sasl_authenticate_request(
            1,
            11,
            &plain_bytes("acme", password),
        ))
        .await;
        let Some(mut wire) = peer.recv().await else {
            panic!("the credential was not answered at all");
        };
        ResponseHeader::decode(
            &mut wire,
            ApiKey::SaslAuthenticate.response_header_version(1),
        )
        .unwrap();
        SaslAuthenticateResponse::decode(&mut wire, 1)
            .unwrap()
            .error_code
    }

    /// One Metadata round trip, or `None` if the connection is gone.
    async fn metadata_works<S>(peer: &mut Peer<S>) -> bool
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        use kafka_protocol::messages::MetadataResponse;
        peer.send(metadata_request(9, 21, Some(&["orders"]))).await;
        let Some(mut wire) = peer.recv().await else {
            return false;
        };
        ResponseHeader::decode(&mut wire, ApiKey::Metadata.response_header_version(9)).unwrap();
        let body = MetadataResponse::decode(&mut wire, 9).unwrap();
        body.topics.len() == 1 && body.topics[0].error_code == 0
    }

    /// A plaintext listener with `policy`, and a client on it.
    async fn plaintext(
        policy: crate::Policy,
    ) -> (Peer<TcpStream>, Arc<crate::queen::testing::FakeQueen>) {
        let (facade, api) = listener_facade(policy);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, facade, None));
        (Peer::new(TcpStream::connect(addr).await.unwrap()), api)
    }

    /// The TLS listener, and a client that dials it naming `sni`.
    async fn over_tls(
        policy: crate::Policy,
        sni: &'static str,
    ) -> (
        Peer<tokio_rustls::client::TlsStream<TcpStream>>,
        Arc<crate::queen::testing::FakeQueen>,
    ) {
        let (facade, api) = listener_facade(policy);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(
            listener,
            facade,
            Some(Arc::new(crate::tls::testing::server())),
        ));

        let connector = tokio_rustls::TlsConnector::from(Arc::new(crate::tls::testing::client()));
        let tcp = TcpStream::connect(addr).await.unwrap();
        let stream = connector
            .connect(rustls::pki_types::ServerName::try_from(sni).unwrap(), tcp)
            .await
            .expect("the TLS handshake completed");
        (Peer::new(stream), api)
    }

    /// The listener speaks TLS, and everything above it is unchanged: the same
    /// frames, the same responses, over a real handshake with a real (if
    /// self-signed) certificate.
    #[tokio::test]
    async fn a_tls_listener_serves_the_same_protocol() {
        let (mut peer, _) = over_tls(crate::Policy::default(), "kafka.example.com").await;
        assert!(metadata_works(&mut peer).await, "TLS changed the protocol");
    }

    /// SNI capture, and what it is FOR: with `QUEEN_KAFKA_FORWARD_SNI_HOST` the
    /// name the client dialled becomes the `Host` of that connection's calls to
    /// Queen, which is what the proxy routes on (proxy/src/acting.rs).
    #[tokio::test]
    async fn the_server_name_a_client_dialled_reaches_queen_as_the_host() {
        let policy = crate::Policy {
            forward_sni_host: true,
            ..Default::default()
        };
        let (mut peer, api) = over_tls(policy, "shared.queenmq.cloud").await;
        assert!(metadata_works(&mut peer).await);
        assert_eq!(
            api.hosts(),
            ["shared.queenmq.cloud"],
            "the call to Queen did not carry the name the client dialled"
        );
    }

    /// ...and with the knob off, which is the default and every OSS
    /// deployment: the name is still captured, and nothing is rewritten.
    #[tokio::test]
    async fn without_the_knob_the_server_name_changes_nothing() {
        let (mut peer, api) = over_tls(crate::Policy::default(), "shared.queenmq.cloud").await;
        assert!(metadata_works(&mut peer).await);
        assert!(
            api.hosts().is_empty(),
            "a Host was forwarded without QUEEN_KAFKA_FORWARD_SNI_HOST"
        );
    }

    fn sasl_policy() -> crate::Policy {
        crate::Policy {
            sasl_plain: true,
            ..Default::default()
        }
    }

    /// The happy path, end to end: handshake, authenticate, and the connection
    /// then serves ordinary requests — with the credential the CLIENT
    /// presented, not the process's.
    #[tokio::test]
    async fn sasl_plain_admits_a_good_credential_and_the_connection_then_works() {
        let (mut peer, api) = plaintext(sasl_policy()).await;
        assert_eq!(authenticate(&mut peer, TENANT_TOKEN).await, 0);
        assert!(metadata_works(&mut peer).await);

        // Every call Queen saw carried this connection's token and never the
        // process credential. (There is ONE call, not two: the authentication
        // probe warmed this credential's catalog entry, so the Metadata behind
        // it was free — see `handlers::sasl_authenticate`.)
        let tokens = api.tokens.lock().unwrap().clone();
        assert!(!tokens.is_empty(), "nothing reached Queen at all");
        assert!(
            tokens.iter().all(|t| t.as_deref() == Some(TENANT_TOKEN)),
            "a call reached Queen with something other than the connection's token: {tokens:?}"
        );
    }

    /// A wrong password is answered SASL_AUTHENTICATION_FAILED and the
    /// connection then dies — the code is what stops a client retrying for ever.
    #[tokio::test]
    async fn a_refused_credential_is_answered_and_then_the_connection_closes() {
        let (mut peer, _) = plaintext(sasl_policy()).await;
        assert_eq!(
            authenticate(&mut peer, "not-the-token").await,
            ResponseError::SaslAuthenticationFailed.code()
        );
        assert!(
            peer.recv().await.is_none(),
            "the connection survived a refused credential"
        );
    }

    /// A mechanism this facade does not offer is refused WITHOUT killing the
    /// connection: a client with a mechanism list picks again.
    #[tokio::test]
    async fn a_mechanism_we_do_not_offer_leaves_the_connection_usable() {
        use kafka_protocol::messages::SaslHandshakeResponse;
        let (mut peer, _) = plaintext(sasl_policy()).await;

        peer.send(sasl_handshake_request(1, 30, "SCRAM-SHA-512"))
            .await;
        let mut wire = peer.recv().await.expect("the handshake was answered");
        ResponseHeader::decode(&mut wire, ApiKey::SaslHandshake.response_header_version(1))
            .unwrap();
        let body = SaslHandshakeResponse::decode(&mut wire, 1).unwrap();
        assert_eq!(
            body.error_code,
            ResponseError::UnsupportedSaslMechanism.code()
        );
        assert_eq!(
            body.mechanisms
                .iter()
                .map(|m| m.as_str())
                .collect::<Vec<_>>(),
            ["PLAIN"],
            "the client was not told what it could pick instead"
        );

        // ...and it does pick again.
        assert_eq!(authenticate(&mut peer, TENANT_TOKEN).await, 0);
        assert!(metadata_works(&mut peer).await);
    }

    /// The gate: before a credential, ApiVersions is answered and everything
    /// else drops the connection with no response — which is what Apache
    /// Kafka's `SaslServerAuthenticator` does.
    #[tokio::test]
    async fn nothing_but_the_negotiation_is_answered_before_authentication() {
        // ApiVersions, which every client sends first and must be able to.
        let (mut peer, _) = plaintext(sasl_policy()).await;
        peer.send(api_versions_request(3, 40)).await;
        let wire = peer.recv().await.expect("ApiVersions was refused pre-auth");
        assert_eq!(decode_response(wire, 3).1.error_code, 0);

        // Everything else, one fresh connection each: the refusal IS the close,
        // so a connection cannot be reused to try the next one.
        for frame in [
            metadata_request(9, 41, Some(&["orders"])),
            produce_request(9, 42, -1),
            fetch_request(6, 43),
            list_offsets_request(5, 44),
            find_coordinator_request(3, 45),
            offset_fetch_request(7, 46),
            // A SaslAuthenticate with no handshake in front of it: there is no
            // agreed mechanism, so there is nothing to read its bytes as.
            sasl_authenticate_request(1, 47, &plain_bytes("acme", TENANT_TOKEN)),
        ] {
            let (mut peer, api) = plaintext(sasl_policy()).await;
            peer.send(frame).await;
            assert!(
                peer.recv().await.is_none(),
                "an unauthenticated request was answered"
            );
            assert!(
                api.tokens.lock().unwrap().is_empty(),
                "an unauthenticated request reached Queen"
            );
        }
    }

    /// The legacy flow: after a SaslHandshake **v0** the token is a bare frame
    /// with no Kafka request around it, and success is an empty frame back.
    #[tokio::test]
    async fn the_v0_flow_carries_the_token_as_a_raw_frame() {
        use kafka_protocol::messages::SaslHandshakeResponse;
        let (mut peer, _) = plaintext(sasl_policy()).await;

        peer.send(sasl_handshake_request(0, 50, "PLAIN")).await;
        let mut wire = peer.recv().await.unwrap();
        ResponseHeader::decode(&mut wire, ApiKey::SaslHandshake.response_header_version(0))
            .unwrap();
        assert_eq!(
            SaslHandshakeResponse::decode(&mut wire, 0)
                .unwrap()
                .error_code,
            0
        );

        peer.send(Bytes::from(plain_bytes("acme", TENANT_TOKEN)))
            .await;
        assert_eq!(
            peer.recv().await.expect("the raw token was answered").len(),
            0,
            "PLAIN's server response is empty"
        );
        assert!(metadata_works(&mut peer).await);
    }

    /// ...and its refusal, which has no error code to carry because the flow
    /// has no response schema: the connection simply goes.
    #[tokio::test]
    async fn the_v0_flow_refuses_by_closing() {
        let (mut peer, _) = plaintext(sasl_policy()).await;
        peer.send(sasl_handshake_request(0, 51, "PLAIN")).await;
        peer.recv().await.unwrap();
        peer.send(Bytes::from(plain_bytes("acme", "not-the-token")))
            .await;
        assert!(peer.recv().await.is_none());
    }

    /// The pre-auth ceiling, over a real socket and against the real gate: a
    /// connection that has not authenticated cannot make this process hold
    /// more than [`PRE_AUTH_MAX_FRAME_BYTES`] for it, and saying so costs it
    /// the connection.
    #[tokio::test]
    async fn an_unauthenticated_connection_cannot_declare_a_large_frame() {
        let (mut peer, api) = plaintext(sasl_policy()).await;
        peer.stream
            .write_all(&((PRE_AUTH_MAX_FRAME_BYTES + 1) as u32).to_be_bytes())
            .await
            .unwrap();
        peer.stream.flush().await.unwrap();
        assert!(
            peer.recv().await.is_none(),
            "an oversized pre-auth frame was answered"
        );
        assert!(api.tokens.lock().unwrap().is_empty());
    }

    /// ...and the same connection, once it has presented a credential, is held
    /// to the full Kafka ceiling instead — a producer's batches are the reason
    /// that ceiling is 100 MiB.
    #[tokio::test]
    async fn an_authenticated_connection_may_send_a_large_frame() {
        let (mut peer, _) = plaintext(sasl_policy()).await;
        assert_eq!(authenticate(&mut peer, TENANT_TOKEN).await, 0);

        // Declared, then abandoned: what is being proved is that the size was
        // not refused on the prefix, so the connection is still waiting for a
        // body when this test drops it.
        peer.stream
            .write_all(&((PRE_AUTH_MAX_FRAME_BYTES + 1) as u32).to_be_bytes())
            .await
            .unwrap();
        peer.stream.flush().await.unwrap();
        // A metadata request would be answered if the connection were still
        // being read as a fresh frame; it is not, because the facade is
        // waiting out the body this test never sends. What matters is that the
        // connection is still open.
        assert!(
            tokio::time::timeout(Duration::from_millis(200), peer.recv())
                .await
                .is_err(),
            "the connection was closed for a legal frame size"
        );
    }

    /// The listener serves as many connections as it says it will, and closes
    /// the rest instead of accepting work it cannot do.
    #[tokio::test]
    async fn the_listener_refuses_connections_past_its_cap() {
        let policy = crate::Policy {
            max_connections: 2,
            ..Default::default()
        };
        let (facade, _) = listener_facade(policy);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, facade, None));

        // Two connections, each proved to be established and holding its slot
        // by a request it answers.
        let mut held = Vec::new();
        for correlation in [1, 2] {
            let mut peer = Peer::new(TcpStream::connect(addr).await.unwrap());
            peer.send(api_versions_request(3, correlation)).await;
            let wire = peer.recv().await.expect("a connection inside the cap");
            assert_eq!(decode_response(wire, 3).0, correlation);
            held.push(peer);
        }

        // The third is accepted by the OS and closed by the listener: nothing
        // is answered on it.
        let mut third = Peer::new(TcpStream::connect(addr).await.unwrap());
        third.send(api_versions_request(3, 3)).await;
        assert!(
            third.recv().await.is_none(),
            "a connection past the cap was served"
        );

        // ...and the slot comes back when a connection ends, so a full listener
        // recovers rather than staying full.
        held.pop();
        for _ in 0..100 {
            let mut again = Peer::new(TcpStream::connect(addr).await.unwrap());
            again.send(api_versions_request(3, 4)).await;
            if let Some(wire) = again.recv().await {
                assert_eq!(decode_response(wire, 3).0, 4);
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("the slot of a closed connection was never released");
    }

    /// With SASL off — the default and every OSS deployment — nothing is gated
    /// and the process credential is what reaches Queen.
    #[tokio::test]
    async fn a_listener_without_sasl_gates_nothing() {
        let (mut peer, _) = plaintext(crate::Policy::default()).await;
        assert!(metadata_works(&mut peer).await);
    }

    /// Two tenants that pick the same group name get two groups.
    ///
    /// Before M5 there was one process credential and the group registry was
    /// keyed by the group id alone. With SASL there are many credentials, and
    /// `orders-consumer` is a name two strangers pick: one registry entry for
    /// both would put two tenants' consumers in one generation, rebalancing
    /// each other for ever. See `coordinator::GroupKey`.
    #[tokio::test]
    async fn two_credentials_naming_one_group_are_two_groups() {
        use kafka_protocol::messages::JoinGroupResponse;

        let facade = crate::handlers::testing::facade(&[("orders", 2)]);
        let minted = |wire: Bytes| {
            let mut buf = wire;
            ResponseHeader::decode(&mut buf, ApiKey::JoinGroup.response_header_version(4)).unwrap();
            let body = JoinGroupResponse::decode(&mut buf, 4).unwrap();
            assert_eq!(body.error_code, 79, "MEMBER_ID_REQUIRED");
            body.member_id.as_str().to_string()
        };
        let connection = |token: &str| {
            let mut c = Conn::new(&facade, None, TEST_PEER.to_string());
            c.facade = c.facade.authenticated_as(token);
            c
        };

        let mut a = connection("tenant-a");
        let mut b = connection("tenant-b");
        let first = minted(sent(
            dispatch(&mut a, join_group_request(4, 1, "orders-consumer")).await,
        ));
        let second = minted(sent(
            dispatch(&mut b, join_group_request(4, 1, "orders-consumer")).await,
        ));
        assert_ne!(first, second, "two tenants were minted one member id");
        assert_eq!(
            facade.coordinator.live_groups(),
            2,
            "one group actor is serving two tenants"
        );

        // ...and a second connection on the SAME credential joins the group
        // that is already there rather than a third.
        let mut again = connection("tenant-a");
        minted(sent(
            dispatch(&mut again, join_group_request(4, 1, "orders-consumer")).await,
        ));
        assert_eq!(facade.coordinator.live_groups(), 2);
    }

    // --------------------------------------------------- the secrets rule

    /// Everything this process wrote to `tracing`, however it was formatted.
    #[derive(Clone, Default)]
    struct Capture(Arc<std::sync::Mutex<Vec<u8>>>);

    impl Capture {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).to_string()
        }
    }

    impl std::io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
        type Writer = Capture;

        fn make_writer(&'a self) -> Capture {
            self.clone()
        }
    }

    /// A subscriber that is interested in everything and records nothing.
    ///
    /// Its whole purpose is what installing it GLOBALLY does to the callsite
    /// interest cache — see [`keep_every_callsite_reachable`].
    struct AlwaysInterested;

    impl tracing::Subscriber for AlwaysInterested {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
            Some(tracing::level_filters::LevelFilter::TRACE)
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// Make every `tracing` callsite in this binary permanently reachable, so a
    /// thread-local subscriber decides what is captured rather than a cache
    /// another test warmed.
    ///
    /// The INTEREST of a callsite is global and is computed the first time
    /// anything reaches it; `tracing::subscriber::set_default` is thread-local,
    /// and the tests in this binary run in parallel. A callsite another test
    /// reaches first — on a thread with no subscriber — is cached as "never",
    /// and the test below would then capture nothing: it would either FAIL, or,
    /// worse, pass while proving nothing. `rebuild_interest_cache` alone does
    /// not settle it, because it only recomputes the callsites that have
    /// already been registered and the race is with the ones that have not.
    ///
    /// So the process gets ONE permanent global subscriber that throws every
    /// event away and says it wants all of them. With it registered, no
    /// callsite's interest can be "never" and the process-wide max level cannot
    /// fall below TRACE, whatever any thread does with a scoped subscriber. The
    /// events still go to the thread-local default when there is one, which is
    /// the capture.
    fn keep_every_callsite_reachable() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            // `Err` only if something else in this binary already set one, which
            // nothing does; either way what follows is what matters.
            let _ = tracing::subscriber::set_global_default(AlwaysInterested);
            tracing::callsite::rebuild_interest_cache();
        });
    }

    /// THE secrets rule, checked rather than asserted in a comment: no line
    /// this facade writes, at any level, can contain a credential.
    ///
    /// Driven through `dispatch` rather than a socket on purpose. A
    /// `#[tokio::test]` is a current-thread runtime, so every `.await` below
    /// runs on THIS thread — which is what makes the thread-local subscriber
    /// see the logs the handlers write. A spawned connection task would write
    /// to the global one and be invisible here.
    ///
    /// The whole conversation is exercised, at TRACE, because "not in the
    /// logs" has to mean not at any level: the happy path (which logs an
    /// admission), the refusal (which logs a failure), the malformed responses
    /// (which log a parse error), and an unauthenticated request (which logs a
    /// close).
    #[tokio::test]
    async fn no_credential_reaches_the_log_at_any_level() {
        const SECRET: &str = "s3cr3t-tenant-token-do-not-log";
        // Before the capture is installed, and not after: what it does is
        // global and permanent, and what it protects against is the OTHER tests
        // in this binary reaching a callsite first. See the function.
        keep_every_callsite_reachable();
        let capture = Capture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(capture.clone())
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let api = crate::queen::testing::FakeQueen::with(&[("orders", 2)]);
        api.accept_only(SECRET);
        let facade = crate::handlers::testing::over(api, sasl_policy());

        // 1. A credential that is refused.
        let mut wrong = Conn::new(&facade, None, TEST_PEER.to_string());
        dispatch(&mut wrong, sasl_handshake_request(1, 1, "PLAIN")).await;
        dispatch(
            &mut wrong,
            sasl_authenticate_request(1, 2, &plain_bytes("acme", "wrong-but-secret")),
        )
        .await;

        // 2. Malformed responses, which is where a naive parser would echo the
        //    input it could not read.
        let mut malformed = Conn::new(&facade, None, TEST_PEER.to_string());
        dispatch(&mut malformed, sasl_handshake_request(1, 3, "PLAIN")).await;
        for bytes in [
            format!("no-nuls-{SECRET}").into_bytes(),
            format!("boss\0acme\0{SECRET}").into_bytes(),
            format!("\0\0{SECRET}").into_bytes(),
        ] {
            dispatch(&mut malformed, sasl_authenticate_request(1, 4, &bytes)).await;
        }

        // 3. The happy path, which logs an admission — and then real work with
        //    the credential on it, which logs whatever a handler logs.
        let mut good = Conn::new(&facade, None, TEST_PEER.to_string());
        dispatch(&mut good, sasl_handshake_request(1, 5, "PLAIN")).await;
        dispatch(
            &mut good,
            sasl_authenticate_request(1, 6, &plain_bytes("acme", SECRET)),
        )
        .await;
        dispatch(&mut good, metadata_request(9, 7, Some(&["orders"]))).await;
        dispatch(&mut good, produce_request(9, 8, -1)).await;

        // 4. The v0 flow, whose token is a bare frame.
        let mut legacy = Conn::new(&facade, None, TEST_PEER.to_string());
        dispatch(&mut legacy, sasl_handshake_request(0, 9, "PLAIN")).await;
        dispatch(&mut legacy, Bytes::from(plain_bytes("acme", SECRET))).await;

        // 5. An unauthenticated request, which logs a close.
        let mut ungated = Conn::new(&facade, None, TEST_PEER.to_string());
        dispatch(&mut ungated, metadata_request(9, 10, Some(&["orders"]))).await;

        let logged = capture.text();
        assert!(
            !logged.is_empty(),
            "nothing was captured, so this test proves nothing"
        );
        assert!(
            logged.contains("sasl"),
            "the SASL paths logged nothing at all: {logged}"
        );
        assert!(
            logged.contains("sasl refused this connection"),
            "the refusal path logged nothing, so its absence proves nothing: {logged}"
        );
        assert!(
            !logged.contains(SECRET),
            "a credential reached the log:\n{logged}"
        );
        assert!(
            !logged.contains("wrong-but-secret"),
            "a REFUSED credential reached the log:\n{logged}"
        );
        // The label is the point of having one: it is what makes a connection
        // identifiable in the log without the token being in it.
        assert!(
            logged.contains("acme"),
            "the username label never reached the log, so a line names nobody:\n{logged}"
        );
    }
    // ------------------------------------------------ M7 F1: topics admin

    /// A CreateTopics request the way an AdminClient writes it: one topic,
    /// KIP-464's "I do not care" for the width and the replication factor.
    fn create_topics_request(api_version: i16, correlation_id: i32, topic: &str) -> Bytes {
        use kafka_protocol::messages::create_topics_request::CreatableTopic;
        use kafka_protocol::messages::TopicName;

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::CreateTopics as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::CreateTopics.request_header_version(api_version),
            )
            .unwrap();
        CreateTopicsRequest::default()
            .with_timeout_ms(30_000)
            .with_topics(vec![CreatableTopic::default()
                .with_name(TopicName(StrBytes::from_string(topic.to_string())))
                .with_num_partitions(-1)
                .with_replication_factor(-1)])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// A DeleteTopics request naming one topic that is not there — the answer
    /// is UNKNOWN_TOPIC_OR_PARTITION, which is a RESPONSE, which is what the
    /// walk test is about. Deleting `orders` would empty the fixture under
    /// every later version of the walk.
    fn delete_topics_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::TopicName;

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::DeleteTopics as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::DeleteTopics.request_header_version(api_version),
            )
            .unwrap();
        DeleteTopicsRequest::default()
            .with_timeout_ms(30_000)
            .with_topic_names(vec![TopicName(StrBytes::from_static_str("never-existed"))])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// A DescribeConfigs request for both resource types at once, which is what
    /// `kafka-configs.sh` and every UI reach for.
    fn describe_configs_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::DescribeConfigs as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::DescribeConfigs.request_header_version(api_version),
            )
            .unwrap();
        DescribeConfigsRequest::default()
            .with_resources(vec![
                DescribeConfigsResource::default()
                    .with_resource_type(2)
                    .with_resource_name(StrBytes::from_static_str("orders")),
                DescribeConfigsResource::default()
                    .with_resource_type(4)
                    .with_resource_name(StrBytes::from_static_str("0")),
            ])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// End to end through the dispatcher for all three, at every advertised
    /// version: a real client decodes what comes back, and the fields it acts
    /// on are the ones asserted.
    #[tokio::test]
    async fn the_topics_admin_apis_round_trip_through_dispatch() {
        use kafka_protocol::messages::{
            CreateTopicsResponse, DeleteTopicsResponse, DescribeConfigsResponse,
        };

        for version in 2i16..=6 {
            let wire = sent(
                dispatch(
                    &mut conn(),
                    create_topics_request(version, 41, &format!("made-{version}")),
                )
                .await,
            );
            let mut buf = wire;
            let header = ResponseHeader::decode(
                &mut buf,
                ApiKey::CreateTopics.response_header_version(version),
            )
            .unwrap();
            let body = CreateTopicsResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());
            assert_eq!(header.correlation_id, 41);
            assert_eq!(body.topics[0].error_code, 0, "v{version}");
            // v5 is where the created topic's real width comes back; below it
            // the field is not on the wire at all.
            if version >= 5 {
                assert_eq!(body.topics[0].num_partitions, 4, "v{version}");
                assert_eq!(body.topics[0].replication_factor, 1, "v{version}");
                assert!(!body.topics[0].configs.as_ref().unwrap().is_empty());
            }
        }

        for version in 1i16..=5 {
            let wire = sent(dispatch(&mut conn(), delete_topics_request(version, 42)).await);
            let mut buf = wire;
            let header = ResponseHeader::decode(
                &mut buf,
                ApiKey::DeleteTopics.response_header_version(version),
            )
            .unwrap();
            let body = DeleteTopicsResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());
            assert_eq!(header.correlation_id, 42);
            assert_eq!(body.responses[0].error_code, 3, "v{version}");
            // v5 is where "there is no such queue" gets to say so in words.
            if version >= 5 {
                assert!(body.responses[0].error_message.is_some(), "v{version}");
            }
        }

        for version in 1i16..=4 {
            let wire = sent(dispatch(&mut conn(), describe_configs_request(version, 43)).await);
            let mut buf = wire;
            let header = ResponseHeader::decode(
                &mut buf,
                ApiKey::DescribeConfigs.response_header_version(version),
            )
            .unwrap();
            let body = DescribeConfigsResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());
            assert_eq!(header.correlation_id, 43);
            assert_eq!(body.results[0].error_code, 0, "v{version} topic");
            assert_eq!(body.results[1].error_code, 0, "v{version} broker");
            assert!(!body.results[1].configs.is_empty(), "v{version}");
            // v3 is where the type of a config becomes a field; below it a
            // client renders every value as a string.
            if version >= 3 {
                assert!(
                    body.results[1].configs.iter().all(|c| c.config_type != 0),
                    "v{version}: a config was answered with an UNKNOWN type"
                );
            }
        }
    }
    // ------------------------------------------------ M7 F2: groups admin

    /// A ListGroups request the way `kafka-consumer-groups.sh --list` writes
    /// one: no state filter, which means "every group".
    fn list_groups_request(api_version: i16, correlation_id: i32) -> Bytes {
        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::ListGroups as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::ListGroups.request_header_version(api_version),
            )
            .unwrap();
        ListGroupsRequest::default()
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// A DescribeGroups request for a group nobody has heard of — error 0 and
    /// state `Dead`, which is Apache Kafka's own answer and still a response.
    fn describe_groups_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::GroupId;

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::DescribeGroups as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::DescribeGroups.request_header_version(api_version),
            )
            .unwrap();
        DescribeGroupsRequest::default()
            .with_groups(vec![GroupId(StrBytes::from_static_str("walk-describe"))])
            // v3's field, and the encoder refuses a field set on a version that
            // does not carry it — which is exactly the check that keeps this
            // walk honest about what each version is.
            .with_include_authorized_operations(api_version >= 3)
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    fn delete_groups_request(api_version: i16, correlation_id: i32, group: &str) -> Bytes {
        use kafka_protocol::messages::GroupId;

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::DeleteGroups as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::DeleteGroups.request_header_version(api_version),
            )
            .unwrap();
        DeleteGroupsRequest::default()
            .with_groups_names(vec![GroupId(StrBytes::from_string(group.to_string()))])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// End to end through the dispatcher for all three, at every advertised
    /// version: a real client decodes what comes back, and the fields it acts
    /// on are the ones asserted.
    #[tokio::test]
    async fn the_groups_admin_apis_round_trip_through_dispatch() {
        use kafka_protocol::messages::{
            DeleteGroupsResponse, DescribeGroupsResponse, ListGroupsResponse,
        };

        // One connection for all of it, so the JoinGroup below and the reads
        // after it see the same coordinator.
        let mut f = conn();
        dispatch(&mut f, join_group_request(4, 7, "dispatch-group")).await;

        for version in 0i16..=4 {
            let wire = sent(dispatch(&mut f, list_groups_request(version, 51)).await);
            let mut buf = wire;
            let header = ResponseHeader::decode(
                &mut buf,
                ApiKey::ListGroups.response_header_version(version),
            )
            .unwrap();
            let body = ListGroupsResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());
            assert_eq!(header.correlation_id, 51);
            assert_eq!(body.error_code, 0, "v{version}");
            assert_eq!(body.groups.len(), 1, "v{version}");
            assert_eq!(
                body.groups[0].group_id.0.as_str(),
                "dispatch-group",
                "v{version}"
            );
            // The state is a v4 field, and below it the wire carries none.
            if version >= 4 {
                assert!(!body.groups[0].group_state.is_empty(), "v{version}");
            }
        }

        for version in 0i16..=3 {
            let wire = sent(dispatch(&mut f, describe_groups_request(version, 52)).await);
            let mut buf = wire;
            let header = ResponseHeader::decode(
                &mut buf,
                ApiKey::DescribeGroups.response_header_version(version),
            )
            .unwrap();
            let body = DescribeGroupsResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());
            assert_eq!(header.correlation_id, 52);
            // Kafka's own answer for a group it has never heard of, measured
            // against apache/kafka:3.9.1: no error, state Dead.
            assert_eq!(body.groups[0].error_code, 0, "v{version}");
            assert_eq!(body.groups[0].group_state.as_str(), "Dead", "v{version}");
            assert_eq!(
                body.groups[0].authorized_operations,
                i32::MIN,
                "v{version}: a permission set was invented"
            );
        }

        for version in 0i16..=2 {
            let wire = sent(
                dispatch(
                    &mut f,
                    delete_groups_request(version, 53, &format!("walk-{version}")),
                )
                .await,
            );
            let mut buf = wire;
            let header = ResponseHeader::decode(
                &mut buf,
                ApiKey::DeleteGroups.response_header_version(version),
            )
            .unwrap();
            let body = DeleteGroupsResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());
            assert_eq!(header.correlation_id, 53);
            assert_eq!(body.results[0].error_code, 69, "v{version}");
        }
    }

    // ------------------------------------------- M7 F3: the idempotent producer

    /// An InitProducerId request the way a stock producer opens with one:
    /// non-transactional, and asking to be given an id.
    fn init_producer_id_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::ProducerId;

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::InitProducerId as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::InitProducerId.request_header_version(api_version),
            )
            .unwrap();
        InitProducerIdRequest::default()
            // null, not "": the field is nullable and this producer is not
            // transactional. brod writes "" here and both are read the same way
            // (`idempotent::transactional_id`).
            .with_transactional_id(None)
            .with_transaction_timeout_ms(60_000)
            .with_producer_id(ProducerId(-1))
            .with_producer_epoch(-1)
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    // ------------------------------------------------------------ transactions

    /// The `transactional.id` the dispatch walk uses at one version.
    fn txn_id(version: i16) -> String {
        format!("walk-txn-{version}")
    }

    /// Bind that id, the way InitProducerId would have. The four transaction
    /// APIs all refuse an id no producer claimed, so the walk claims it first.
    fn bind_txn(f: &Conn, version: i16) {
        let tenant = f.facade.catalog.tenant_key(f.facade.token());
        f.facade
            .txns
            .bind(
                &tenant,
                &txn_id(version),
                7,
                0,
                100,
                f.id,
                Duration::from_secs(60),
            )
            .expect("the fixture is under the open-transaction cap");
    }

    fn txn_header(key: ApiKey, api_version: i16, correlation_id: i32) -> BytesMut {
        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(key as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(&mut out, key.request_header_version(api_version))
            .unwrap();
        out
    }

    fn add_partitions_to_txn_request(api_version: i16, correlation_id: i32, id: &str) -> Bytes {
        use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
        use kafka_protocol::messages::{ProducerId, TopicName, TransactionalId};

        let mut out = txn_header(ApiKey::AddPartitionsToTxn, api_version, correlation_id);
        AddPartitionsToTxnRequest::default()
            .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_string(
                id.to_string(),
            )))
            .with_v3_and_below_producer_id(ProducerId(7))
            .with_v3_and_below_producer_epoch(0)
            .with_v3_and_below_topics(vec![AddPartitionsToTxnTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![0])])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    fn add_offsets_to_txn_request(api_version: i16, correlation_id: i32, id: &str) -> Bytes {
        use kafka_protocol::messages::{GroupId, ProducerId, TransactionalId};

        let mut out = txn_header(ApiKey::AddOffsetsToTxn, api_version, correlation_id);
        AddOffsetsToTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_string(id.to_string())))
            .with_producer_id(ProducerId(7))
            .with_producer_epoch(0)
            .with_group_id(GroupId(StrBytes::from_static_str("walk-txn-group")))
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    fn end_txn_request(api_version: i16, correlation_id: i32, id: &str) -> Bytes {
        use kafka_protocol::messages::{ProducerId, TransactionalId};

        let mut out = txn_header(ApiKey::EndTxn, api_version, correlation_id);
        EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_string(id.to_string())))
            .with_producer_id(ProducerId(7))
            .with_producer_epoch(0)
            // ABORT, so the walk writes nothing: a commit would send a bundle
            // and this test is about the dispatch table.
            .with_committed(false)
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    fn txn_offset_commit_request(api_version: i16, correlation_id: i32, id: &str) -> Bytes {
        use kafka_protocol::messages::txn_offset_commit_request::{
            TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
        };
        use kafka_protocol::messages::{GroupId, ProducerId, TopicName, TransactionalId};

        let mut out = txn_header(ApiKey::TxnOffsetCommit, api_version, correlation_id);
        TxnOffsetCommitRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_string(id.to_string())))
            .with_group_id(GroupId(StrBytes::from_static_str("walk-txn-group")))
            .with_producer_id(ProducerId(7))
            .with_producer_epoch(0)
            .with_generation_id(-1)
            .with_member_id(StrBytes::from_static_str(""))
            .with_topics(vec![TxnOffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![TxnOffsetCommitRequestPartition::default()
                    .with_partition_index(0)
                    .with_committed_offset(1)])])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// End to end through the dispatcher at every advertised version: a real
    /// client decodes what comes back, and the two fields it acts on — the id
    /// and the epoch — are the ones asserted.
    #[tokio::test]
    async fn init_producer_id_round_trips_through_dispatch() {
        use kafka_protocol::messages::InitProducerIdResponse;

        let mut f = conn();
        let mut granted = Vec::new();
        for version in 0i16..=4 {
            let wire = sent(dispatch(&mut f, init_producer_id_request(version, 91)).await);
            let mut buf = wire;
            let header = ResponseHeader::decode(
                &mut buf,
                ApiKey::InitProducerId.response_header_version(version),
            )
            .unwrap();
            let body = InitProducerIdResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());
            assert_eq!(header.correlation_id, 91);
            assert_eq!(body.error_code, 0, "v{version}");
            assert_eq!(body.producer_epoch, 0, "v{version}");
            assert!(body.producer_id.0 > 0, "v{version}: {:?}", body.producer_id);
            assert_eq!(body.throttle_time_ms, 0, "v{version}");
            granted.push(body.producer_id.0);
        }
        granted.sort_unstable();
        let minted = granted.len();
        granted.dedup();
        assert_eq!(granted.len(), minted, "two producers were given one id");
    }

    // -------------------------------------- M7 F4: the ACL family (29, 30, 31)

    /// A DescribeAcls request the way `kafka-acls.sh --list` composes one: the
    /// ANY filter, which matches everything a broker holds.
    fn describe_acls_request(api_version: i16, correlation_id: i32) -> Bytes {
        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::DescribeAcls as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::DescribeAcls.request_header_version(api_version),
            )
            .unwrap();
        DescribeAclsRequest::default()
            .with_resource_type_filter(1) // ANY
            .with_pattern_type_filter(1) // ANY
            .with_operation(1) // ANY
            .with_permission_type(1) // ANY
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// A CreateAcls request the way `kafka-acls.sh --add --allow-principal
    /// User:alice --operation Read --topic orders` composes one.
    fn create_acls_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::create_acls_request::AclCreation;

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::CreateAcls as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::CreateAcls.request_header_version(api_version),
            )
            .unwrap();
        CreateAclsRequest::default()
            .with_creations(vec![AclCreation::default()
                .with_resource_type(2) // TOPIC
                .with_resource_name(StrBytes::from_static_str("orders"))
                .with_resource_pattern_type(3) // LITERAL
                .with_principal(StrBytes::from_static_str("User:alice"))
                .with_host(StrBytes::from_static_str("*"))
                .with_operation(3) // READ
                .with_permission_type(3)]) // ALLOW
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// A DeleteAcls request the way `kafka-acls.sh --remove` composes one.
    fn delete_acls_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::delete_acls_request::DeleteAclsFilter;

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::DeleteAcls as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::DeleteAcls.request_header_version(api_version),
            )
            .unwrap();
        DeleteAclsRequest::default()
            .with_filters(vec![DeleteAclsFilter::default()
                .with_resource_type_filter(2)
                .with_resource_name_filter(Some(StrBytes::from_static_str("orders")))
                .with_pattern_type_filter(3)
                .with_operation(3)
                .with_permission_type(3)])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// End to end through the dispatcher at every advertised version, decoded
    /// with the client half of `kafka-protocol` — which is the half that matters
    /// here, because the whole value of answering 54 instead of closing the
    /// connection is that a client can READ the refusal and print it.
    #[tokio::test]
    async fn the_acl_family_round_trips_security_disabled_through_dispatch() {
        use crate::handlers::acls::{NO_AUTHORIZER, NO_AUTHORIZER_ON_THE_BROKER};
        use kafka_protocol::messages::{
            CreateAclsResponse, DeleteAclsResponse, DescribeAclsResponse,
        };

        let mut f = conn();
        for version in 1i16..=3 {
            let mut buf = sent(dispatch(&mut f, describe_acls_request(version, 54)).await);
            let header = ResponseHeader::decode(
                &mut buf,
                ApiKey::DescribeAcls.response_header_version(version),
            )
            .unwrap();
            let body = DescribeAclsResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());
            assert_eq!(header.correlation_id, 54);
            assert_eq!(body.error_code, 54, "v{version}");
            // Describe carries the oracle's LONGER sentence; the two writes
            // below carry the short one. Kafka really does use two literals.
            assert_eq!(
                body.error_message.as_ref().map(|s| s.as_str()),
                Some(NO_AUTHORIZER_ON_THE_BROKER),
                "v{version}"
            );
            assert!(body.resources.is_empty(), "v{version}");

            let mut buf = sent(dispatch(&mut f, create_acls_request(version, 55)).await);
            ResponseHeader::decode(
                &mut buf,
                ApiKey::CreateAcls.response_header_version(version),
            )
            .unwrap();
            let body = CreateAclsResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());
            // ONE result for ONE creation. A top-level-only error would decode
            // as "the call succeeded and created nothing".
            assert_eq!(body.results.len(), 1, "v{version}");
            assert_eq!(body.results[0].error_code, 54, "v{version}");
            assert_eq!(
                body.results[0].error_message.as_ref().map(|s| s.as_str()),
                Some(NO_AUTHORIZER),
                "v{version}"
            );

            let mut buf = sent(dispatch(&mut f, delete_acls_request(version, 56)).await);
            ResponseHeader::decode(
                &mut buf,
                ApiKey::DeleteAcls.response_header_version(version),
            )
            .unwrap();
            let body = DeleteAclsResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());
            assert_eq!(body.filter_results.len(), 1, "v{version}");
            assert_eq!(body.filter_results[0].error_code, 54, "v{version}");
            assert_eq!(
                body.filter_results[0]
                    .error_message
                    .as_ref()
                    .map(|s| s.as_str()),
                Some(NO_AUTHORIZER),
                "v{version}"
            );
            assert!(
                body.filter_results[0].matching_acls.is_empty(),
                "v{version}"
            );
        }
    }

    // ------------------------------ M7 F4: the config write half (33, 44)

    /// An AlterConfigs the way the deprecated `admin.alterConfigs` composes one:
    /// a TOPIC resource and one config. `orders` is in the fixture's catalog and
    /// has no config record, so the answer is the untracked refusal — which is a
    /// RESPONSE, identical at every version, and leaves nothing behind for the
    /// next version of the walk.
    fn alter_configs_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::alter_configs_request::{
            AlterConfigsResource, AlterableConfig,
        };

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::AlterConfigs as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::AlterConfigs.request_header_version(api_version),
            )
            .unwrap();
        AlterConfigsRequest::default()
            .with_resources(vec![AlterConfigsResource::default()
                .with_resource_type(2)
                .with_resource_name(StrBytes::from_static_str("orders"))
                .with_configs(vec![AlterableConfig::default()
                    .with_name(StrBytes::from_static_str("retention.ms"))
                    .with_value(Some(StrBytes::from_static_str("604800000")))])])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// An IncrementalAlterConfigs the way `kafka-configs.sh --alter --add-config
    /// retention.ms=604800000` composes one: operation SET on a TOPIC resource.
    fn incremental_alter_configs_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::incremental_alter_configs_request::{
            AlterConfigsResource, AlterableConfig,
        };

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::IncrementalAlterConfigs as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::IncrementalAlterConfigs.request_header_version(api_version),
            )
            .unwrap();
        IncrementalAlterConfigsRequest::default()
            .with_resources(vec![AlterConfigsResource::default()
                .with_resource_type(2)
                .with_resource_name(StrBytes::from_static_str("orders"))
                .with_configs(vec![AlterableConfig::default()
                    .with_name(StrBytes::from_static_str("retention.ms"))
                    .with_config_operation(0)
                    .with_value(Some(StrBytes::from_static_str("604800000")))])])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// End to end through the dispatcher at every advertised version, decoded
    /// with the client half of `kafka-protocol`.
    ///
    /// The refusal is the untracked one, and that it DECODES is the whole point:
    /// `kafka-configs.sh` prints the message out of an
    /// `InvalidConfigurationException`, so a sentence nobody can decode is a
    /// sentence nobody reads.
    #[tokio::test]
    async fn the_config_write_half_round_trips_through_dispatch() {
        use kafka_protocol::error::ResponseError;
        use kafka_protocol::messages::{AlterConfigsResponse, IncrementalAlterConfigsResponse};

        let mut f = conn();
        for version in 0i16..=2 {
            let mut buf = sent(dispatch(&mut f, alter_configs_request(version, 33)).await);
            let header = ResponseHeader::decode(
                &mut buf,
                ApiKey::AlterConfigs.response_header_version(version),
            )
            .unwrap();
            let body = AlterConfigsResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());
            assert_eq!(header.correlation_id, 33);
            assert_eq!(body.responses.len(), 1, "v{version}");
            assert_eq!(
                body.responses[0].error_code,
                ResponseError::InvalidConfig.code(),
                "v{version}"
            );
            assert!(
                body.responses[0]
                    .error_message
                    .as_ref()
                    .is_some_and(|m| m.as_str().contains("rewrites every config column")),
                "v{version}: {:?}",
                body.responses[0].error_message
            );
        }

        for version in 0i16..=1 {
            let mut buf =
                sent(dispatch(&mut f, incremental_alter_configs_request(version, 44)).await);
            let header = ResponseHeader::decode(
                &mut buf,
                ApiKey::IncrementalAlterConfigs.response_header_version(version),
            )
            .unwrap();
            let body = IncrementalAlterConfigsResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());
            assert_eq!(header.correlation_id, 44);
            assert_eq!(body.responses.len(), 1, "v{version}");
            assert_eq!(
                body.responses[0].error_code,
                ResponseError::InvalidConfig.code(),
                "v{version}"
            );
        }
    }

    // ------------------------- M7 F4: the remaining admin writes (37, 47)

    /// A CreatePartitions the way `kafka-topics.sh --alter --topic orders
    /// --partitions 2` composes one. `orders` is four lanes wide in the
    /// fixture, so this is a DECREASE — the case whose answer is Apache
    /// Kafka's own sentence, identical at every version, and one that writes
    /// nothing for the next version of the walk to trip over.
    fn create_partitions_request(api_version: i16, correlation_id: i32) -> Bytes {
        use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;
        use kafka_protocol::messages::TopicName;

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::CreatePartitions as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::CreatePartitions.request_header_version(api_version),
            )
            .unwrap();
        CreatePartitionsRequest::default()
            .with_topics(vec![CreatePartitionsTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_count(2)])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// An OffsetDelete the way `kafka-consumer-groups.sh --group g --topic
    /// orders --delete-offsets` composes one. The group is one nobody has
    /// heard of, so the answer is GROUP_ID_NOT_FOUND — a RESPONSE, which is
    /// what the dispatch walk is about.
    fn offset_delete_request(api_version: i16, correlation_id: i32, group: &str) -> Bytes {
        use kafka_protocol::messages::offset_delete_request::{
            OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
        };
        use kafka_protocol::messages::{GroupId, TopicName};

        let mut out = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::OffsetDelete as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("queen-kafka-test")))
            .encode(
                &mut out,
                ApiKey::OffsetDelete.request_header_version(api_version),
            )
            .unwrap();
        OffsetDeleteRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_topics(vec![OffsetDeleteRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![
                    OffsetDeleteRequestPartition::default().with_partition_index(0)
                ])])
            .encode(&mut out, api_version)
            .unwrap();
        out.freeze()
    }

    /// End to end through the dispatcher, decoded with the client half of
    /// `kafka-protocol`. The two things worth asserting on the wire are the
    /// two the tools print: CreatePartitions' `error_message`, which is where
    /// `kafka-topics.sh` gets the sentence an operator reads, and
    /// OffsetDelete's TOP-LEVEL code, which is the field the Java AdminClient
    /// checks before it ever looks at a partition.
    #[tokio::test]
    async fn the_remaining_admin_writes_round_trip_through_dispatch() {
        use kafka_protocol::error::ResponseError;
        use kafka_protocol::messages::{CreatePartitionsResponse, OffsetDeleteResponse};

        let mut f = conn();
        for version in 0i16..=3 {
            let mut buf = sent(dispatch(&mut f, create_partitions_request(version, 37)).await);
            let header = ResponseHeader::decode(
                &mut buf,
                ApiKey::CreatePartitions.response_header_version(version),
            )
            .unwrap();
            let body = CreatePartitionsResponse::decode(&mut buf, version).unwrap();
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());
            assert_eq!(header.correlation_id, 37);
            assert_eq!(body.results.len(), 1, "v{version}");
            assert_eq!(
                body.results[0].error_code,
                ResponseError::InvalidPartitions.code(),
                "v{version}"
            );
            assert_eq!(
                body.results[0]
                    .error_message
                    .as_ref()
                    .map(|m| m.as_str().to_string()),
                Some(
                    "The topic orders currently has 4 partition(s); 2 would not be an increase."
                        .to_string()
                ),
                "v{version}"
            );
        }

        let mut buf = sent(dispatch(&mut f, offset_delete_request(0, 47, "dispatch-g")).await);
        let header =
            ResponseHeader::decode(&mut buf, ApiKey::OffsetDelete.response_header_version(0))
                .unwrap();
        let body = OffsetDeleteResponse::decode(&mut buf, 0).unwrap();
        assert!(buf.is_empty(), "{} trailing bytes", buf.len());
        assert_eq!(header.correlation_id, 47);
        assert_eq!(body.error_code, ResponseError::GroupIdNotFound.code());
        assert!(body.topics.is_empty());
    }
}
