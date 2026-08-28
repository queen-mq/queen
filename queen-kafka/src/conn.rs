//! The Kafka listener: framing, request-header decode and the per-connection
//! serial dispatch.
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

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, MetadataRequest, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use crate::handlers::{api_versions, metadata};
use crate::versions::{self, Support};
use crate::Facade;

/// Ceiling on a single request frame. Kafka's own `socket.request.max.bytes`
/// defaults to 100 MiB and clients size their batches under it, so this is the
/// number they already respect. It exists to keep a bogus or hostile 4-byte
/// length prefix from turning into an allocation: the codec compares the
/// declared length against it *before* reserving anything.
pub const MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

/// Read buffer a fresh connection starts with. One ApiVersions exchange is a
/// few dozen bytes; the buffer grows on demand for the batches of M2 onward.
const READ_BUF_BYTES: usize = 16 * 1024;

/// The frame codec, in one place so the listener and the tests cannot disagree
/// about the framing.
pub fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .big_endian()
        .length_field_length(4)
        .max_frame_length(MAX_FRAME_BYTES)
        .new_codec()
}

/// The outcome of one request frame.
#[derive(Debug)]
pub enum Reply {
    /// Response header + body, ready to be length-prefixed and written back.
    Send(Bytes),
    /// Nothing the client could parse can be written — an unknown API key
    /// leaves us without a response schema, and a header we cannot decode
    /// leaves us without even a correlation id. Apache Kafka drops the
    /// connection in the same situations; the string is the log line.
    Close(String),
}

/// Accept loop. One tokio task per connection, and never returns.
pub async fn serve(listener: TcpListener, facade: Arc<Facade>) {
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                // Small request/response bodies on a keep-alive connection with
                // one request in flight: Nagle would sit on every response for
                // up to ~40ms waiting to coalesce, for no benefit.
                stream.set_nodelay(true).ok();
                let facade = Arc::clone(&facade);
                tokio::spawn(async move {
                    if let Err(e) = connection(stream, facade).await {
                        tracing::debug!(target: "kafka", %peer, error = %e, "connection closed");
                    }
                });
            }
            Err(e) => {
                // A transient accept error (fd exhaustion, a peer that reset
                // between SYN and accept) must not kill the listener.
                tracing::warn!(target: "kafka", error = %e, "accept failed");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
}

/// One connection, serial: decode a frame, handle it, write the response, and
/// only then look for the next frame. Returns `Ok` on a clean close.
async fn connection(mut stream: TcpStream, facade: Arc<Facade>) -> std::io::Result<()> {
    let mut codec = codec();
    let mut inbuf = BytesMut::with_capacity(READ_BUF_BYTES);
    let mut outbuf = BytesMut::new();
    loop {
        // Drain what is already buffered before touching the socket: a client
        // is free to pipeline several requests into one write, and they must be
        // answered one at a time, in order.
        let frame = match codec.decode(&mut inbuf)? {
            Some(f) => f,
            None => {
                if stream.read_buf(&mut inbuf).await? == 0 {
                    return if inbuf.is_empty() {
                        Ok(())
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("EOF with {} bytes of a partial frame", inbuf.len()),
                        ))
                    };
                }
                continue;
            }
        };
        match dispatch(&facade, frame.freeze()).await {
            Reply::Send(body) => {
                outbuf.clear();
                codec.encode(body, &mut outbuf)?;
                stream.write_all(&outbuf).await?;
                stream.flush().await?;
            }
            Reply::Close(why) => {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, why));
            }
        }
    }
}

/// Handle one request frame (the bytes after the length prefix). No panics:
/// every malformed input leaves through `Reply::Close`. The only I/O it can do
/// is the handler's own, through `facade`.
pub async fn dispatch(facade: &Facade, frame: Bytes) -> Reply {
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
        ApiKey::Metadata => {
            let req = match MetadataRequest::decode(&mut buf, api_version) {
                Ok(r) => r,
                Err(e) => return Reply::Close(format!("Metadata v{api_version} body: {e}")),
            };
            // M1 reaches Queen with the process credential. M5 replaces this
            // with the connection's own, which is why it is an argument.
            let body = metadata::handle(facade, &req, api_version, facade.token()).await;
            respond(key, header.correlation_id, &body, api_version)
        }
        // Unreachable while the table and this match agree, which is the point:
        // a row added to `versions::ADVERTISED` without an arm here is a clean
        // close and a log line, not a wrong answer on the wire.
        _ => Reply::Close(format!("{key:?} is advertised but has no handler")),
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
    use kafka_protocol::messages::ApiVersionsResponse;
    use kafka_protocol::protocol::{Message, StrBytes};

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

    /// A facade wired to a fake Queen with one queue. The ApiVersions tests
    /// never reach it; the Metadata ones do.
    fn facade() -> Arc<Facade> {
        Arc::new(Facade {
            advertised_host: "kafka.example.com".into(),
            advertised_port: 9092,
            default_partitions: 4,
            queen_token: None,
            catalog: crate::queen::Catalog::new(crate::queen::testing::FakeQueen::with(&[(
                "orders", 2,
            )])),
        })
    }

    fn sent(reply: Reply) -> Bytes {
        match reply {
            Reply::Send(b) => b,
            Reply::Close(why) => panic!("expected a response, got close: {why}"),
        }
    }

    fn closed(reply: Reply) -> String {
        match reply {
            Reply::Close(why) => why,
            Reply::Send(_) => panic!("expected a close, got a response"),
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
        let f = facade();
        for version in [0i16, 1, 2, 3] {
            let reply = dispatch(&f, api_versions_request(version, 7)).await;
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
    /// know what we speak can parse the refusal and downgrade.
    #[tokio::test]
    async fn above_our_window_falls_back_to_a_v0_body() {
        let wire = sent(dispatch(&facade(), api_versions_request(9, 11)).await);

        // Exact size is the strongest pin on "encoded at v0": a v0 header is 4
        // bytes of correlation id, and a v0 body is error_code (2) + an empty
        // array (4). At v1+ the body would also carry throttle_time_ms.
        assert_eq!(wire.len(), 10, "not a v0-encoded response: {wire:?}");

        let (correlation_id, body) = decode_response(wire, 0);
        assert_eq!(correlation_id, 11);
        assert_eq!(body.error_code, 35); // UNSUPPORTED_VERSION
        assert!(body.api_keys.is_empty());
    }

    #[tokio::test]
    async fn unadvertised_api_keys_close_cleanly() {
        let f = facade();
        // A real Kafka API this build does not offer yet.
        let mut frame = BytesMut::new();
        frame.extend_from_slice(&(ApiKey::Produce as i16).to_be_bytes());
        frame.extend_from_slice(&9i16.to_be_bytes());
        frame.extend_from_slice(&1i32.to_be_bytes());
        assert!(closed(dispatch(&f, frame.freeze()).await).contains("api key 0"));

        // Not a Kafka API key at all.
        let mut frame = BytesMut::new();
        frame.extend_from_slice(&30_000i16.to_be_bytes());
        frame.extend_from_slice(&0i16.to_be_bytes());
        assert!(closed(dispatch(&f, frame.freeze()).await).contains("api key 30000"));
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
        let why = closed(dispatch(&facade(), frame.freeze()).await);
        assert!(why.contains("Metadata v12"), "{why}");
        assert!(why.contains("0..=9"), "{why}");
    }

    /// Garbage never panics: it closes, whatever shape it has.
    #[tokio::test]
    async fn garbage_closes_instead_of_panicking() {
        let f = facade();
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
            closed(dispatch(&f, junk).await);
        }
    }

    /// The contract `versions.rs` exists to keep: every `(key, version)` the
    /// table advertises reaches a handler. A row added without an arm in
    /// `dispatch` fails here rather than answering a client with a close.
    #[tokio::test]
    async fn every_advertised_version_is_dispatched() {
        let f = facade();
        for api in versions::ADVERTISED {
            for version in api.min..=api.max {
                let frame = match api.key {
                    ApiKey::ApiVersions => api_versions_request(version, 1),
                    ApiKey::Metadata => metadata_request(version, 1, Some(&["orders"])),
                    other => panic!("{other:?} is advertised but this test cannot build one"),
                };
                match dispatch(&f, frame).await {
                    Reply::Send(_) => {}
                    Reply::Close(why) => {
                        panic!("{:?} v{version} is advertised but closed: {why}", api.key)
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
            let wire =
                sent(dispatch(&facade(), metadata_request(version, 77, Some(&["orders"]))).await);
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

    /// The whole loop, over a real socket: two pipelined requests in one write
    /// come back as two responses, in order, on one connection.
    #[tokio::test]
    async fn serves_pipelined_requests_over_a_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, facade()));

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
}
