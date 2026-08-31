//! queen-sqs — an SQS/SNS wire front for QueenMQ.
//!
//! Spec: PLAN_QUEEN_SQS.md (repo root). The facade is a separate binary that
//! speaks the two SQS wire protocols to clients and plain HTTP to a Queen broker
//! or proxy as a normal client, so an unmodified AWS SDK — and every driver
//! built on one: Celery, Laravel's `sqs` queue, Symfony Messenger, Spring Cloud
//! AWS, MassTransit, sqs-consumer, the aws CLI, Terraform, KEDA — reaches Queen
//! by changing `endpoint_url` and nothing else.
//!
//! ## The sentence the whole design protects
//!
//! **It is stateless: any instance answers any request, and a plain Service or
//! load balancer in front is supported.** That is the opposite of queen-kafka,
//! deliberately, and it is why the receipt handle is self-contained and
//! tamper-evident ([`handle`]), why the SQS/SNS registry lives in Queen's
//! key/value store rather than in this process ([`registry`]), and why every
//! in-memory structure here is a cache with a TTL. A restart is free.
//!
//! ## The layers, and what may know what
//!
//! ```text
//!   axum listener        one route, both protocols            (this file)
//!     ↓  proto::sniff    X-Amz-Target | Action=               (proto)
//!     ↓  sigv4::verify   who signed it                        (sigv4, credentials)
//!     ↓  actions::dispatch  one implementation per verb       (actions)
//!     ↓  queen::QueenApi    push / pop / ack / kv / timers    (queen)
//!   ← proto::render_ok | render_error, in the protocol it came in
//! ```
//!
//! Each arrow is one-way: an action never learns which protocol served it, the
//! codecs never learn what an action means, and nothing below the action layer
//! ever sees a header or a credential.

pub mod actions;
pub mod config;
pub mod credentials;
pub mod envelope;
pub mod error;
pub mod handle;
pub mod md5;
pub mod obs;
pub mod proto;
pub mod queen;
pub mod registry;
pub mod sigv4;
pub mod sns;

/// The whole facade driven over a real socket by a real HTTP client. It lives
/// inside the library rather than in `tests/` for one reason: the broker double
/// ([`queen::testing`]) is `#[cfg(test)]`, and an integration target compiles
/// against the library WITHOUT that flag — so a test binary outside this crate
/// could only reach a real broker.
#[cfg(test)]
mod http_tests;

use std::sync::Arc;

use crate::actions::{Ctx, Principal};
use crate::config::{AuthMode, Config};
use crate::credentials::Directory;
use crate::error::{ErrorKind, SqsError, SqsResult};
use crate::handle::Handles;
use crate::obs::Sampler;
use crate::proto::{ProtoResponse, Protocol};
use crate::queen::{Catalog, QueenApi};
use crate::registry::Registry;

/// The largest request this listener will read.
///
/// SQS's own ceiling is 1 MiB of message (`MaximumMessageSize`, raised by AWS in
/// 2025), and a `SendMessageBatch` may carry that much in total; the doubling
/// covers the form encoding, the base64 of binary attributes and the headers
/// around them, and stops there. It is a cap on BYTES READ, applied before the
/// body is in memory: without it any unauthenticated client can ask this process
/// to buffer whatever it likes, and the signature that would have refused it is
/// computed over the body it is still reading.
pub const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;

/// How the listener hands [`handle_request`] the request line, which is not in
/// any header and which SigV4 signs.
///
/// The same device, and the same guarantee, as [`proto::HEADER_QUERY_STRING`]:
/// `:` is not a legal character in an HTTP/1.1 header name, so hyper refuses a
/// request that tries to send one, and HTTP/2's real pseudo-headers are consumed
/// by the protocol layer and never reach a `HeaderMap`. The only writer is this
/// crate.
pub const HEADER_METHOD: &str = ":method";
/// The request's PATH, still percent-encoded, exactly as it arrived — see
/// [`HEADER_METHOD`]. Normalizing it is how a verifier disagrees with a signer.
pub const HEADER_PATH: &str = ":path";

/// The health endpoint. Unauthenticated by design: it is read by a Kubernetes
/// probe and an LB, neither of which holds a credential, and it answers whether
/// this PROCESS is serving — never whether Queen is reachable, because a facade
/// that fails its liveness probe on a broker blip is a fleet that restarts
/// itself instead of retrying.
pub const HEALTH_PATH: &str = "/healthz";

/// One line per ten seconds for each of the three, per instance. The rule is
/// obs.rs's: a signature refused by every worker in a fleet on the same
/// misconfigured clock is the cheapest request on this listener and would
/// otherwise be the most expensive line in the log pipeline.
static AUTH_REFUSED: Sampler = Sampler::new(10_000);
static SENDER_REFUSED: Sampler = Sampler::new(10_000);
static RECEIVER_FAILED: Sampler = Sampler::new(10_000);

/// Everything a request handler is allowed to know.
///
/// One per PROCESS, built at boot, shared by every request: unlike queen-kafka's
/// per-connection facade there is nothing per-connection here, because there are
/// no connections — an SQS client opens an HTTP request, and the credential it
/// signs with is a property of that request, not of the socket. What varies per
/// request lives in [`actions::Ctx`].
pub struct Facade {
    pub config: Config,
    /// Queen itself, for the data path.
    pub queen: Arc<dyn QueenApi>,
    /// The queue list, cached briefly and single-flighted. The same object as
    /// `queen` underneath: the split is between the admin calls, which every
    /// request would otherwise repeat, and the data calls, which are neither
    /// cached nor collapsible.
    pub catalog: Arc<Catalog>,
    /// The SQS/SNS state Queen has no column for, in Queen's key/value store.
    pub registry: Arc<Registry>,
    /// The receipt-handle minter and verifier.
    pub handles: Handles,
    /// The principals this deployment knows.
    pub credentials: Directory,
}

impl Facade {
    pub fn new(config: Config, queen: Arc<dyn QueenApi>) -> Facade {
        let catalog = Arc::new(Catalog::new(Arc::clone(&queen)));
        let registry = Arc::new(Registry::new(Arc::clone(&queen)));
        let handles = Handles::new(&config.handle_secret);
        let credentials = config.credentials.clone();
        Facade {
            config,
            queen,
            catalog,
            registry,
            handles,
            credentials,
        }
    }

    /// The bearer this facade presents to Queen on behalf of `access_key_id`:
    /// the principal's own when the directory names one, and the process default
    /// otherwise. The one place a credential becomes a token, so no action ever
    /// holds either.
    ///
    /// `None` for the key id is not "no token": it is "no principal", which is
    /// the `auth=off` listener and the principal the directory does not name,
    /// and both get `QUEEN_TOKEN`. A key id is only ever passed in here AFTER
    /// its signature verified — an unverified one would be an impersonation
    /// with a bearer attached.
    pub fn token_for(&self, access_key_id: Option<&str>) -> Option<String> {
        access_key_id
            .and_then(|id| self.credentials.get(id))
            .and_then(|credential| credential.queen_token.clone())
            .or_else(|| self.config.queen_token.clone())
    }
}

/// The listener. ONE route besides health, and that is not a simplification:
/// both protocols POST to `/`, the action is in a header or in the body, and a
/// queue's URL path (`/<account>/<name>`) is an ADDRESS a client keeps rather
/// than a route this facade dispatches on — SQS sends every action to the
/// queue's own URL or to the endpoint root, interchangeably, so the path is read
/// as data and never as a route.
///
/// `/healthz` is the one exception and cannot collide with a queue: every URL
/// this facade mints has TWO path segments, and a queue named `healthz` is
/// addressed at `/<account>/healthz`.
pub fn router(facade: Arc<Facade>) -> axum::Router {
    axum::Router::new()
        .route(HEALTH_PATH, axum::routing::get(health))
        .fallback(serve)
        .with_state(facade)
}

/// Liveness. Deliberately says nothing about Queen — see [`HEALTH_PATH`].
async fn health() -> axum::response::Response {
    into_response(ProtoResponse {
        status: 200,
        content_type: "application/json",
        body: "{\"status\":\"ok\",\"service\":\"queen-sqs\"}".to_string(),
        headers: Vec::new(),
    })
}

/// Everything else: read the body under the cap, flatten the request line into
/// the header list, answer.
async fn serve(
    axum::extract::State(facade): axum::extract::State<Arc<Facade>>,
    method: axum::http::Method,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    body: axum::body::Body,
) -> axum::response::Response {
    let request_id = new_request_id();
    let protocol = {
        // Cheap and infallible, and needed before the body exists: a request
        // refused for its SIZE still has to answer in a shape its client parses.
        let target = headers
            .get(proto::HEADER_TARGET)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        match target.trim().is_empty() {
            true => Protocol::Query,
            false => Protocol::Json,
        }
    };

    let body = match read_body(body).await {
        Ok(bytes) => bytes,
        Err(error) => {
            note(&error, &request_id, "");
            return into_response(proto::render_error(protocol, &error, &request_id));
        }
    };

    let mut list: Vec<(String, String)> = Vec::with_capacity(headers.len() + 3);
    for (name, value) in headers.iter() {
        // A header whose bytes are not UTF-8 is dropped rather than lossily
        // decoded: if the client signed it, the signature refuses the request,
        // which is the correct outcome and a better one than verifying against
        // a value nobody sent.
        if let Ok(value) = value.to_str() {
            list.push((name.as_str().to_string(), value.to_string()));
        }
    }
    // HTTP/2 carries the authority as a pseudo-header the `HeaderMap` never
    // shows, while every SDK signs `host`. Without this line an h2 client's
    // signature is refused for a header it did send.
    if !list.iter().any(|(name, _)| name == "host") {
        if let Some(authority) = uri.authority() {
            list.push(("host".to_string(), authority.to_string()));
        }
    }
    list.push((HEADER_METHOD.to_string(), method.as_str().to_string()));
    list.push((HEADER_PATH.to_string(), uri.path().to_string()));
    if let Some(query) = uri.query() {
        list.push((proto::HEADER_QUERY_STRING.to_string(), query.to_string()));
    }

    into_response(answer(&facade, &list, &body, &request_id).await)
}

/// The whole request path, from bytes to bytes. Public because the conformance
/// tests drive it directly rather than through a socket: everything it needs is
/// a header list and a body, which is exactly what a test can build.
pub async fn handle_request(
    facade: &Arc<Facade>,
    headers: &[(String, String)],
    body: &[u8],
) -> ProtoResponse {
    let request_id = new_request_id();
    answer(facade, headers, body, &request_id).await
}

/// [`handle_request`] with the id already minted, which the socket path needs
/// because it may have to refuse before it has a body to answer about.
async fn answer(
    facade: &Arc<Facade>,
    headers: &[(String, String)],
    body: &[u8],
    request_id: &str,
) -> ProtoResponse {
    let protocol = proto::protocol_of(headers);
    // Which API was addressed, for the ERROR path only: a refusal raised before
    // the body is decoded still has to answer in the namespace of the service
    // the client thinks it is talking to, and after `sniff` has failed nothing
    // else in the request says which one that is.
    let version = proto::version_of(headers, body);
    let version = version.as_deref();

    // The order below is the whole security posture of this listener and does
    // not commute. Size, then signature, then decode, then act: a body is
    // bounded before it is held, a request is attributed before it is parsed,
    // and no parser sees bytes nobody vouched for.
    if body.len() > MAX_BODY_BYTES {
        let error = too_large();
        note(&error, request_id, "");
        return proto::render_error_versioned(protocol, version, &error, request_id);
    }

    let principal = match authenticate(facade, headers, body) {
        Ok(principal) => principal,
        Err(error) => {
            if let Some(suppressed) = AUTH_REFUSED.tick_now() {
                tracing::warn!(
                    target: "sqs",
                    suppressed,
                    code = error.kind.json_type(),
                    // The presented key id is public — it travels in the clear
                    // in every Authorization header — and it is the one field
                    // that tells an operator WHICH client is misconfigured.
                    access_key_id = presented(headers, body).unwrap_or_default(),
                    request_id,
                    "{}", error.message
                );
            }
            return proto::render_error_versioned(protocol, version, &error, request_id);
        }
    };

    let request = match proto::sniff(headers, body) {
        Ok(request) => request,
        Err(error) => {
            note(&error, request_id, "");
            return proto::render_error_versioned(protocol, version, &error, request_id);
        }
    };

    let ctx = Ctx {
        facade: Arc::clone(facade),
        principal,
        host: proto::header(headers, "host")
            .unwrap_or_default()
            .to_string(),
        request_id: request_id.to_string(),
    };
    // The namespace an answer is rendered in is the request's `Version`, and a
    // Query client that omits it would be answered in SQS's — which for an SNS
    // action is a document from a service the client did not address. The ACTION
    // is the only remaining evidence, and resolving it HERE rather than in the
    // codec is what keeps `proto` from having to recognise the SNS action set
    // (its own contract forbids it). A request that names a version keeps it.
    let version = request.version.clone().or_else(|| {
        actions::Action::from_name(&request.action)
            .filter(|action| action.is_sns())
            .map(|_| proto::query::VERSION_SNS.to_string())
    });
    let version = version.as_deref();
    match actions::dispatch(&ctx, &request).await {
        Ok(payload) => proto::render_ok_versioned(
            request.protocol,
            &request.action,
            version,
            request_id,
            payload,
        ),
        Err(error) => {
            note(&error, request_id, &request.action);
            proto::render_error_versioned(request.protocol, version, &error, request_id)
        }
    }
}

/// Who is asking.
///
/// Under [`AuthMode::Off`] nothing is verified, so nothing may be TRUSTED: the
/// request is attributed to whatever key id it presented — a label, for the log
/// line — and its bearer is the process default, never the one the directory
/// holds for that key id. Reading an unverified key id as a principal would make
/// `auth=off` a way to borrow another tenant's token by typing its name.
fn authenticate(
    facade: &Arc<Facade>,
    headers: &[(String, String)],
    body: &[u8],
) -> SqsResult<Principal> {
    let signed = signed_request(headers, body);
    match facade.config.auth {
        AuthMode::Off => Ok(Principal {
            access_key_id: Some(
                sigv4::presented_access_key_id(&signed)
                    .unwrap_or_else(|| credentials::ANONYMOUS_ACCESS_KEY_ID.to_string()),
            ),
            queen_token: facade.token_for(None),
        }),
        AuthMode::SigV4 => {
            let verified = sigv4::verify(
                &signed,
                &facade.credentials,
                &facade.config.region,
                obs::now_epoch_ms(),
            )?;
            Ok(Principal {
                queen_token: facade.token_for(Some(&verified.access_key_id)),
                access_key_id: Some(verified.access_key_id),
            })
        }
    }
}

/// The request as the signature algorithm reads it: the request line out of the
/// pseudo-headers, and the REAL headers only.
///
/// The filter is what keeps the two lists honest. `sniff` reads `:query`, and
/// the signature must not: a canonical request built over a header the client
/// could not have sent is one no client can reproduce.
fn signed_request<'a>(headers: &'a [(String, String)], body: &'a [u8]) -> sigv4::SignedRequest<'a> {
    sigv4::SignedRequest {
        method: proto::header(headers, HEADER_METHOD).unwrap_or("POST"),
        path: proto::header(headers, HEADER_PATH).unwrap_or("/"),
        query: proto::header(headers, proto::HEADER_QUERY_STRING).unwrap_or(""),
        headers,
        body,
    }
}

/// The access key id a refused request claimed, for the log line only.
fn presented(headers: &[(String, String)], body: &[u8]) -> Option<String> {
    sigv4::presented_access_key_id(&signed_request(headers, body))
}

/// Read the body under [`MAX_BODY_BYTES`], without ever holding more than that.
async fn read_body(body: axum::body::Body) -> SqsResult<bytes::Bytes> {
    use http_body_util::BodyExt as _;
    match http_body_util::Limited::new(body, MAX_BODY_BYTES)
        .collect()
        .await
    {
        Ok(collected) => Ok(collected.to_bytes()),
        Err(e) if e.is::<http_body_util::LengthLimitError>() => Err(too_large()),
        // A body that stopped arriving. The client is gone or the connection
        // broke; there is nobody to explain it to, and the answer exists only
        // so that this path has one.
        Err(_) => Err(SqsError::with(
            ErrorKind::InvalidParameterValue,
            "The request body could not be read to the end.",
        )),
    }
}

/// The refusal for a body over the cap.
///
/// `InvalidParameterValue` because the closed catalog has no
/// `RequestEntityTooLarge`: AWS answers one, we may not invent a code, and of
/// the twenty-five this is the one that says "something you sent is too big"
/// with a `Sender` fault and a 400 rather than a retry-forever 5xx.
fn too_large() -> SqsError {
    SqsError::with(
        ErrorKind::InvalidParameterValue,
        format!(
            "The request body is larger than the {MAX_BODY_BYTES} byte limit of this endpoint."
        ),
    )
}

/// One sampled line per refusal class. Client mistakes are `debug` because a
/// fleet makes millions of them and each one already went back to its author;
/// a `Receiver` fault is this deployment's problem and is `warn`.
fn note(error: &SqsError, request_id: &str, action: &str) {
    match error.kind.fault() {
        error::Fault::Receiver => {
            if let Some(suppressed) = RECEIVER_FAILED.tick_now() {
                tracing::warn!(
                    target: "sqs",
                    suppressed,
                    code = error.kind.json_type(),
                    action,
                    request_id,
                    "{}", error.message
                );
            }
        }
        error::Fault::Sender => {
            if let Some(suppressed) = SENDER_REFUSED.tick_now() {
                tracing::debug!(
                    target: "sqs",
                    suppressed,
                    code = error.kind.json_type(),
                    action,
                    request_id,
                    "{}", error.message
                );
            }
        }
    }
}

/// The id on every answer and in every log line. A v4 UUID: it is what an SDK
/// prints in its own error, and it is the only string a client's report and this
/// facade's log have in common.
fn new_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// A rendered answer as axum serves it.
fn into_response(answer: ProtoResponse) -> axum::response::Response {
    let mut builder = axum::response::Response::builder()
        .status(answer.status)
        .header(axum::http::header::CONTENT_TYPE, answer.content_type);
    for (name, value) in &answer.headers {
        builder = builder.header(name, value);
    }
    // Every name and value above is minted by this crate — a uuid, a digit
    // string, a static content type — so the fallible build cannot fail today.
    // It is answered rather than unwrapped because the day it can, the failure
    // must be one request's 500 and not the process.
    builder
        .body(axum::body::Body::from(answer.body))
        .unwrap_or_else(|_| {
            axum::response::Response::builder()
                .status(500)
                .body(axum::body::Body::empty())
                .expect("a bodiless 500 is always constructible")
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthMode, ReceiveMode};
    use crate::queen::testing::FakeQueen;

    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

    fn facade(spec: &str, default_token: Option<&str>) -> Arc<Facade> {
        let config = Config {
            listen: config::DEFAULT_LISTEN.to_string(),
            auth: AuthMode::SigV4,
            credentials: Directory::from_spec(spec).expect("a directory"),
            region: config::DEFAULT_REGION.to_string(),
            account: config::DEFAULT_ACCOUNT.to_string(),
            receive_mode: ReceiveMode::Exact,
            default_partitions: 4,
            handle_secret: b"a unit test handle secret".to_vec(),
            handle_secret_generated: false,
            queen_url: "http://queen.invalid:6632".to_string(),
            queen_token: default_token.map(str::to_string),
            embedded: false,
            shutdown_grace_ms: 0,
            tls: None,
        };
        let queen: Arc<dyn QueenApi> = FakeQueen::empty() as Arc<dyn QueenApi>;
        Arc::new(Facade::new(config, queen))
    }

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    /// The one place a credential becomes a bearer, in all three of its cases.
    #[test]
    fn a_principals_own_token_wins_and_everything_else_is_the_default() {
        let facade = facade(
            &format!("HASTOKEN:{SECRET}:tok-its-own,NOTOKEN:{SECRET}"),
            Some("tok-default"),
        );
        assert_eq!(
            facade.token_for(Some("HASTOKEN")).as_deref(),
            Some("tok-its-own")
        );
        // A principal the directory names but gives no token of its own: the
        // single-tenant OSS shape.
        assert_eq!(
            facade.token_for(Some("NOTOKEN")).as_deref(),
            Some("tok-default")
        );
        // An unknown key id, and no key id at all — which is the `auth=off`
        // listener. Neither can borrow another principal's bearer.
        assert_eq!(
            facade.token_for(Some("STRANGER")).as_deref(),
            Some("tok-default")
        );
        assert_eq!(facade.token_for(None).as_deref(), Some("tok-default"));
    }

    /// With no process default there is no bearer, and a principal without one
    /// of its own reaches Queen unauthenticated rather than as somebody else.
    #[test]
    fn without_a_process_default_there_is_nothing_to_fall_back_to() {
        let facade = facade(&format!("NOTOKEN:{SECRET}"), None);
        assert_eq!(facade.token_for(Some("NOTOKEN")), None);
        assert_eq!(facade.token_for(None), None);
    }

    /// The request line is carried by pseudo-headers and the signature reads it
    /// from there. A caller that omits them gets the shape every SDK sends.
    #[test]
    fn the_request_line_travels_as_pseudo_headers() {
        let list = headers(&[
            ("host", "sqs.example.test"),
            (HEADER_METHOD, "GET"),
            (HEADER_PATH, "/000000000000/orders"),
            (proto::HEADER_QUERY_STRING, "Action=ListQueues"),
        ]);
        let signed = signed_request(&list, b"");
        assert_eq!(signed.method, "GET");
        assert_eq!(signed.path, "/000000000000/orders");
        assert_eq!(signed.query, "Action=ListQueues");

        let bare = headers(&[("host", "sqs.example.test")]);
        let signed = signed_request(&bare, b"body");
        assert_eq!(signed.method, "POST");
        assert_eq!(signed.path, "/");
        assert_eq!(signed.query, "");
        assert_eq!(signed.body, b"body");
    }

    /// The size refusal is a client's, not a server's: a `Sender` fault and a
    /// 400, so an SDK stops instead of retrying two megabytes for ever.
    #[test]
    fn the_size_refusal_names_the_cap_and_blames_the_sender() {
        let error = too_large();
        assert_eq!(error.kind, ErrorKind::InvalidParameterValue);
        assert_eq!(error.kind.http_status(), 400);
        assert_eq!(error.kind.fault(), error::Fault::Sender);
        assert!(
            error.message.contains(&MAX_BODY_BYTES.to_string()),
            "{}",
            error.message
        );
    }

    /// The rendered answer keeps its status, its content type and every header
    /// the codec put on it — which is where `x-amzn-requestid` lives.
    #[test]
    fn a_rendered_answer_survives_the_conversion_whole() {
        let response = into_response(ProtoResponse {
            status: 403,
            content_type: "text/xml",
            body: "<ErrorResponse/>".to_string(),
            headers: vec![
                ("x-amzn-requestid".to_string(), "rid-1".to_string()),
                ("retry-after".to_string(), "3".to_string()),
            ],
        });
        assert_eq!(response.status(), 403);
        assert_eq!(response.headers().get("content-type").unwrap(), "text/xml");
        assert_eq!(response.headers().get("x-amzn-requestid").unwrap(), "rid-1");
        assert_eq!(response.headers().get("retry-after").unwrap(), "3");
    }
}
