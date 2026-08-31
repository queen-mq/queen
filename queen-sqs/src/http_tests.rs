//! The facade end to end: a real socket, a real HTTP client, both protocols.
//!
//! WHAT THESE COVER THAT THE MODULE SUITES CANNOT. Every layer below has its own
//! tests and they are the thorough ones; these exist for the SEAMS between them,
//! which is where an integration is wrong:
//!
//!   * that the request line reaches SigV4 at all — the method, the raw path and
//!     the raw query string are not headers, and the pseudo-headers that carry
//!     them ([`crate::HEADER_METHOD`], [`crate::HEADER_PATH`],
//!     [`crate::proto::HEADER_QUERY_STRING`]) are this crate's only writing;
//!   * that the answer comes back in the protocol the request arrived in, on the
//!     ERROR path too, including errors raised before the body was decoded;
//!   * that the `Host` a client actually reached is the host in the URLs it is
//!     handed back, which is what makes the next request go anywhere;
//!   * that a principal's bearer, and only that principal's, reaches Queen;
//!   * that the body cap is applied before the body is held.
//!
//! WHY THEY LIVE IN THE LIBRARY. The broker double ([`crate::queen::testing`])
//! is `#[cfg(test)]`, and an integration target in `tests/` compiles against the
//! library without that flag — so a test binary outside this crate could only
//! reach a real broker, which is the compat rig's job and not a unit suite's.
//!
//! WHY THE SIGNER BELOW IS HAND-ROLLED. Signing with [`crate::sigv4`]'s own
//! helpers would prove that the module agrees with itself. [`sign`] rebuilds the
//! canonical request, the string to sign and the key schedule from the
//! specification, using only `hmac`/`sha2`/`hex`, so a passing signed request is
//! two independent implementations agreeing.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use hmac::{Mac, SimpleHmac};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::config::{AuthMode, Config, ReceiveMode};
use crate::credentials::Directory;
use crate::handle::Handles;
use crate::queen::testing::{iso_from_epoch_ms, FakeQueen};
use crate::queen::{Catalog, QueenApi};
use crate::registry::Registry;
use crate::{obs, router, Facade};

const AKID: &str = "AKIDEXAMPLE";
const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
/// The bearer the directory holds for [`AKID`] — what must reach Queen for a
/// request that principal signed.
const PRINCIPAL_TOKEN: &str = "tok-principal";
/// The bearer everything else gets.
const DEFAULT_TOKEN: &str = "tok-process-default";
const JSON_CONTENT_TYPE: &str = "application/x-amz-json-1.0";
const FORM_CONTENT_TYPE: &str = "application/x-www-form-urlencoded; charset=utf-8";
const SQS_VERSION: &str = "2012-11-05";

// ---------------------------------------------------------------------- the rig

/// One facade, on a loopback port nobody chose, over a broker double.
struct Rig {
    /// `http://127.0.0.1:<port>` — what a client configures as `endpoint_url`.
    base: String,
    host: String,
    client: reqwest::Client,
    fake: Arc<FakeQueen>,
    /// Set when this rig signs its requests.
    credential: Option<(String, String)>,
    serving: tokio::task::JoinHandle<()>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.serving.abort();
    }
}

/// One answer, reduced to what an assertion reads.
struct Answer {
    status: u16,
    headers: BTreeMap<String, String>,
    body: String,
}

impl Answer {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body)
            .unwrap_or_else(|e| panic!("the answer is not JSON ({e}): {}", self.body))
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }

    /// The text between `<tag>` and `</tag>`, which is all an XML assertion here
    /// needs — the renderer's own suite proves the document's shape.
    fn tag(&self, name: &str) -> Option<&str> {
        let open = format!("<{name}>");
        let close = format!("</{name}>");
        let start = self.body.find(&open)? + open.len();
        let end = self.body[start..].find(&close)? + start;
        Some(&self.body[start..end])
    }

    fn expect_tag(&self, name: &str) -> &str {
        self.tag(name)
            .unwrap_or_else(|| panic!("no <{name}> in {}", self.body))
    }
}

impl Rig {
    async fn start(auth: AuthMode) -> Rig {
        let fake = FakeQueen::empty();
        let queen: Arc<dyn QueenApi> = Arc::clone(&fake) as Arc<dyn QueenApi>;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let host = listener.local_addr().expect("bound").to_string();

        let credentials = Directory::from_spec(&format!("{AKID}:{SECRET}:{PRINCIPAL_TOKEN}"))
            .expect("the directory parses");
        let config = Config {
            listen: host.clone(),
            auth,
            credentials: credentials.clone(),
            region: crate::config::DEFAULT_REGION.to_string(),
            account: crate::config::DEFAULT_ACCOUNT.to_string(),
            receive_mode: ReceiveMode::Exact,
            // Four lanes rather than sixty-four: a round trip that sends one
            // message and receives it should not depend on which of sixty-four
            // synthesized partitions the hash chose.
            default_partitions: 4,
            handle_secret: b"an integration handle secret".to_vec(),
            handle_secret_generated: false,
            queen_url: "http://queen.invalid:6632".to_string(),
            queen_token: Some(DEFAULT_TOKEN.to_string()),
            embedded: false,
            shutdown_grace_ms: 0,
            tls: None,
        };
        // Every cache OFF: a test that counts calls to Queen, or that reads back
        // what the call before it wrote, must not depend on a TTL.
        let facade = Arc::new(Facade {
            config,
            catalog: Arc::new(Catalog::with_ttl(Arc::clone(&queen), Duration::ZERO)),
            registry: Arc::new(Registry::with_ttl(Arc::clone(&queen), Duration::ZERO)),
            handles: Handles::new(b"an integration handle secret"),
            credentials,
            queen,
        });

        let app = router(facade);
        let serving = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Rig {
            base: format!("http://{host}"),
            host,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("a client"),
            fake,
            credential: None,
            serving,
        }
    }

    /// The development listener: nothing is verified.
    async fn open() -> Rig {
        Rig::start(AuthMode::Off).await
    }

    /// The production listener, with this rig signing as [`AKID`].
    async fn signed() -> Rig {
        let mut rig = Rig::start(AuthMode::SigV4).await;
        rig.credential = Some((AKID.to_string(), SECRET.to_string()));
        rig
    }

    /// The production listener, with this rig NOT signing.
    async fn verifying() -> Rig {
        Rig::start(AuthMode::SigV4).await
    }

    async fn send(
        &self,
        method: &str,
        path: &str,
        query: &str,
        mut headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Answer {
        headers.push(("host".to_string(), self.host.clone()));
        if let Some((akid, secret)) = &self.credential {
            sign(
                akid,
                secret,
                "queen-1",
                "sqs",
                method,
                path,
                query,
                &mut headers,
                &body,
                &compact_now(),
            );
        }
        let url = match query.is_empty() {
            true => format!("{}{path}", self.base),
            false => format!("{}{path}?{query}", self.base),
        };
        let mut request = self
            .client
            .request(method.parse().expect("a method"), url)
            .body(body);
        for (name, value) in headers {
            // `host` is hyper's to write, from the URL's authority — which is
            // the same string, since this rig dials the address it bound.
            if name != "host" {
                request = request.header(name, value);
            }
        }
        let response = request.send().await.expect("the facade answered");
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    value.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect();
        Answer {
            status,
            headers,
            body: response.text().await.expect("a body"),
        }
    }

    /// One JSON 1.0 request: the protocol every SDK major since late 2023 speaks.
    async fn json(&self, action: &str, body: Value) -> Answer {
        self.send(
            "POST",
            "/",
            "",
            vec![
                ("x-amz-target".to_string(), format!("AmazonSQS.{action}")),
                ("content-type".to_string(), JSON_CONTENT_TYPE.to_string()),
            ],
            body.to_string().into_bytes(),
        )
        .await
    }

    /// One Query/XML request: async-aws, the older SDK majors, and all of SNS.
    async fn query(&self, action: &str, params: &[(&str, &str)]) -> Answer {
        self.form(action, SQS_VERSION, params).await
    }

    /// The same, in SNS's API version — which is the whole of what selects SNS's
    /// XML namespace and its `<member>`-wrapped lists.
    async fn sns(&self, action: &str, params: &[(&str, &str)]) -> Answer {
        self.form(action, crate::proto::query::VERSION_SNS, params)
            .await
    }

    async fn form(&self, action: &str, version: &str, params: &[(&str, &str)]) -> Answer {
        let mut form = form_urlencoded::Serializer::new(String::new());
        form.append_pair("Action", action);
        form.append_pair("Version", version);
        for (name, value) in params {
            form.append_pair(name, value);
        }
        self.send(
            "POST",
            "/",
            "",
            vec![("content-type".to_string(), FORM_CONTENT_TYPE.to_string())],
            form.finish().into_bytes(),
        )
        .await
    }

    /// One JSON 1.0 request under SNS's target prefix. No SDK sends this — SNS
    /// never moved off Query — but the codec accepts it, so the action layer has
    /// to answer it.
    async fn json_sns(&self, action: &str, body: Value) -> Answer {
        self.send(
            "POST",
            "/",
            "",
            vec![
                (
                    "x-amz-target".to_string(),
                    format!("AmazonSimpleNotificationService.{action}"),
                ),
                ("content-type".to_string(), JSON_CONTENT_TYPE.to_string()),
            ],
            body.to_string().into_bytes(),
        )
        .await
    }

    /// The queue URL a client would have been handed, without asking for it.
    fn url(&self, queue: &str) -> String {
        format!("{}/{}/{queue}", self.base, crate::config::DEFAULT_ACCOUNT)
    }

    async fn create(&self, queue: &str) -> Answer {
        self.json("CreateQueue", json!({ "QueueName": queue }))
            .await
    }

    /// Every bearer this rig's facade has presented to Queen, deduplicated.
    fn bearers(&self) -> Vec<Option<String>> {
        let mut seen: Vec<Option<String>> = Vec::new();
        for token in self.fake.tokens.lock().unwrap().iter() {
            if !seen.contains(token) {
                seen.push(token.clone());
            }
        }
        seen
    }
}

// -------------------------------------------------------------- the test signer

/// SigV4, rebuilt from the specification rather than from [`crate::sigv4`].
///
/// Appends `x-amz-date` and `authorization` to `headers`. Every header already
/// in the list is signed, in the sorted order the algorithm requires, which is
/// what makes a tampered body or a swapped header refuse.
#[allow(clippy::too_many_arguments)]
fn sign(
    akid: &str,
    secret: &str,
    region: &str,
    service: &str,
    method: &str,
    path: &str,
    query: &str,
    headers: &mut Vec<(String, String)>,
    body: &[u8],
    amz_date: &str,
) {
    headers.push(("x-amz-date".to_string(), amz_date.to_string()));

    let mut signed: Vec<(String, String)> = headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    signed.sort();
    let names: Vec<String> = signed.iter().map(|(name, _)| name.clone()).collect();
    let signed_headers = names.join(";");
    let canonical_headers: String = signed
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect();

    let payload = hex::encode(Sha256::digest(body));
    let canonical = format!(
        "{}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload}",
        method.to_ascii_uppercase()
    );
    let date = &amz_date[..8];
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let signature = hex::encode(hmac(
        &derive(secret, date, region, service),
        to_sign.as_bytes(),
    ));
    headers.push((
        "authorization".to_string(),
        format!(
            "AWS4-HMAC-SHA256 Credential={akid}/{scope}, SignedHeaders={signed_headers}, \
             Signature={signature}"
        ),
    ));
}

fn derive(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let key = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let key = hmac(&key, region.as_bytes());
    let key = hmac(&key, service.as_bytes());
    hmac(&key, b"aws4_request")
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = <SimpleHmac<Sha256>>::new_from_slice(key).expect("any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// `YYYYMMDDTHHMMSSZ` for now. The signature's clock is the wall clock, so a
/// signed test signs against it.
fn compact_now() -> String {
    compact(obs::now_epoch_ms())
}

fn compact(ms: i64) -> String {
    let iso = iso_from_epoch_ms(ms);
    let mut out: String = iso
        .chars()
        .take(19)
        .filter(|c| !matches!(c, '-' | ':'))
        .collect();
    out.push('Z');
    out
}

/// Percent-encoding for a presigned URL's parameters: everything but the
/// unreserved set, uppercase hex, `/` included.
fn encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

// ------------------------------------------------------------------- the health

/// The probe an orchestrator makes, which holds no credential and must never be
/// asked for one — including on the listener that verifies everything else.
#[tokio::test]
async fn health_answers_on_a_verifying_listener_without_a_credential() {
    let rig = Rig::verifying().await;
    let answer = rig
        .send("GET", "/healthz", "", Vec::new(), Vec::new())
        .await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert_eq!(answer.json()["status"], "ok");
    assert_eq!(answer.json()["service"], "queen-sqs");
}

// -------------------------------------------------------------- the JSON protocol

/// THE round trip, over a socket, in the protocol every current SDK speaks:
/// create, send, receive, delete, and the queue is empty again.
#[tokio::test]
async fn a_json_send_receive_delete_round_trip() {
    let rig = Rig::open().await;
    let created = rig.create("orders").await;
    assert_eq!(created.status, 200, "{}", created.body);
    let url = created.json()["QueueUrl"]
        .as_str()
        .expect("a URL")
        .to_string();
    assert_eq!(url, rig.url("orders"));

    let sent = rig
        .json(
            "SendMessage",
            json!({"QueueUrl": url, "MessageBody": "This is a test message"}),
        )
        .await;
    assert_eq!(sent.status, 200, "{}", sent.body);
    let sent = sent.json();
    // AWS's own published vector for this body. boto3 checks it client-side.
    assert_eq!(sent["MD5OfMessageBody"], "fafb00f5732ab283681e124bf8747ed1");
    let message_id = sent["MessageId"].as_str().expect("an id").to_string();

    let received = rig
        .json("ReceiveMessage", json!({"QueueUrl": url}))
        .await
        .json();
    let messages = received["Messages"].as_array().expect("a list").clone();
    assert_eq!(messages.len(), 1, "{received}");
    assert_eq!(messages[0]["Body"], "This is a test message");
    assert_eq!(messages[0]["MessageId"], message_id.as_str());
    assert_eq!(
        messages[0]["MD5OfBody"], "fafb00f5732ab283681e124bf8747ed1",
        "the receive spells the same digest MD5OfBody"
    );
    let handle = messages[0]["ReceiptHandle"]
        .as_str()
        .expect("a handle")
        .to_string();

    let deleted = rig
        .json(
            "DeleteMessage",
            json!({"QueueUrl": url, "ReceiptHandle": handle}),
        )
        .await;
    assert_eq!(deleted.status, 200, "{}", deleted.body);
    assert_eq!(deleted.json(), json!({}), "an action with no output shape");

    let after = rig
        .json("ReceiveMessage", json!({"QueueUrl": url}))
        .await
        .json();
    // An empty receive omits the member rather than sending an empty list,
    // which is AWS's own shape and is visible to an SDK's paginator.
    assert_eq!(after, json!({}), "the message is gone");
}

/// A message attribute survives the whole path — the envelope, the broker
/// double, the envelope again — and its digest is the one boto3 recomputes.
#[tokio::test]
async fn message_attributes_survive_the_round_trip_with_their_digest() {
    let rig = Rig::open().await;
    let url = rig.create("orders").await.json()["QueueUrl"]
        .as_str()
        .expect("a URL")
        .to_string();
    let sent = rig
        .json(
            "SendMessage",
            json!({
                "QueueUrl": url,
                "MessageBody": "body",
                "MessageAttributes": {
                    "tenant": {"DataType": "String", "StringValue": "acme"}
                }
            }),
        )
        .await
        .json();
    let digest = sent["MD5OfMessageAttributes"]
        .as_str()
        .expect("an attribute digest")
        .to_string();

    let received = rig
        .json(
            "ReceiveMessage",
            json!({"QueueUrl": url, "MessageAttributeNames": ["All"]}),
        )
        .await
        .json();
    let message = &received["Messages"][0];
    assert_eq!(
        message["MessageAttributes"]["tenant"]["StringValue"],
        "acme"
    );
    assert_eq!(message["MessageAttributes"]["tenant"]["DataType"], "String");
    assert_eq!(
        message["MD5OfMessageAttributes"],
        digest.as_str(),
        "the digest a client validates is the same on both legs"
    );
}

/// Every answer is traceable, and no two share an id — which is the whole use
/// of one.
#[tokio::test]
async fn every_answer_carries_a_fresh_request_id() {
    let rig = Rig::open().await;
    let first = rig.json("ListQueues", json!({})).await;
    let second = rig.json("ListQueues", json!({})).await;
    // The error path too: an id is most needed on the answer somebody reports.
    let failed = rig
        .json(
            "SendMessage",
            json!({"QueueUrl": rig.url("nope"), "MessageBody": "x"}),
        )
        .await;

    let ids: Vec<&str> = [&first, &second, &failed]
        .iter()
        .map(|answer| answer.header("x-amzn-requestid").expect("an id"))
        .collect();
    assert_eq!(ids[0].len(), 36, "a v4 uuid: {}", ids[0]);
    assert_ne!(ids[0], ids[1]);
    assert_ne!(ids[1], ids[2]);
}

/// The JSON rendering of the catalog, over the wire: the `__type` an SDK
/// switches on, the compatibility header, and the status.
#[tokio::test]
async fn a_json_error_renders_as_json_1_0() {
    let rig = Rig::open().await;
    let answer = rig
        .json(
            "SendMessage",
            json!({"QueueUrl": rig.url("absent"), "MessageBody": "x"}),
        )
        .await;
    assert_eq!(answer.status, 400);
    assert_eq!(answer.header("content-type"), Some(JSON_CONTENT_TYPE));
    assert_eq!(
        answer.json()["__type"],
        "com.amazonaws.sqs#QueueDoesNotExist"
    );
    assert_eq!(
        answer.header("x-amzn-query-error"),
        Some("AWS.SimpleQueueService.NonExistentQueue;Sender"),
        "the header the SDKs read when they speak JSON to a Query-era model"
    );
}

/// The closed action set, over the wire. An action nobody implements is
/// `InvalidAction` and never a plausible success.
#[tokio::test]
async fn an_unknown_action_is_invalid_action() {
    let rig = Rig::open().await;
    let answer = rig.json("Frobnicate", json!({})).await;
    assert_eq!(answer.status, 400);
    assert_eq!(answer.json()["__type"], "com.amazonaws.sqs#InvalidAction");
    // The name the client sent is not echoed back into this facade's answer.
    assert!(!answer.body.contains("Frobnicate"), "{}", answer.body);
}

/// Every action in the set is reachable: none answers `InvalidAction`.
///
/// The negative half of [`an_unknown_action_is_invalid_action`], and the reason
/// it is a walk rather than a list: an action added to
/// [`crate::actions::Action::ALL`] and forgotten in `dispatch` would be one no
/// client can use, and nothing else in this suite would notice. The requests are
/// deliberately EMPTY — what is under test is that the name resolves to an
/// implementation, so any answer but `InvalidAction` passes.
#[tokio::test]
async fn every_action_in_the_set_reaches_an_implementation() {
    let rig = Rig::open().await;
    for action in crate::actions::Action::ALL {
        let answer = rig.json(action.name(), json!({})).await;
        let code = answer.json()["__type"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        assert!(
            !code.ends_with("#InvalidAction"),
            "{} is in the action set and dispatch does not answer it: {}",
            action.name(),
            answer.body
        );
    }
}

/// `PurgeQueue` end to end: the messages go, the queue stays, and the second
/// purge inside the window is the 403 an SDK branches on.
#[tokio::test]
async fn a_purge_empties_the_queue_and_the_next_one_is_refused() {
    let rig = Rig::open().await;
    let url = rig.create("orders").await.json()["QueueUrl"]
        .as_str()
        .expect("a URL")
        .to_string();
    rig.json(
        "SendMessage",
        json!({"QueueUrl": url, "MessageBody": "before the purge"}),
    )
    .await;

    let purged = rig.json("PurgeQueue", json!({"QueueUrl": url})).await;
    assert_eq!(purged.status, 200, "{}", purged.body);
    assert_eq!(purged.json().get("Messages"), None);

    let received = rig.json("ReceiveMessage", json!({"QueueUrl": url})).await;
    assert_eq!(received.status, 200, "{}", received.body);
    assert_eq!(
        received.json().get("Messages"),
        None,
        "the purged message came back: {}",
        received.body
    );

    let again = rig.json("PurgeQueue", json!({"QueueUrl": url})).await;
    assert_eq!(again.status, 403, "{}", again.body);
    assert_eq!(
        again.json()["__type"],
        "com.amazonaws.sqs#PurgeQueueInProgress"
    );

    // …and the queue is still there, with its attributes.
    let attributes = rig
        .json(
            "GetQueueAttributes",
            json!({"QueueUrl": url, "AttributeNames": ["QueueArn"]}),
        )
        .await;
    assert_eq!(
        attributes.json()["Attributes"]["QueueArn"],
        json!("arn:aws:sqs:queen-1:000000000000:orders")
    );
}

/// The same over the Query protocol, where the refusal is an `<ErrorResponse>`
/// carrying the LEGACY code — which is not the JSON spelling.
#[tokio::test]
async fn a_query_purge_answers_the_envelope_and_the_legacy_code() {
    let rig = Rig::open().await;
    rig.query("CreateQueue", &[("QueueName", "orders")]).await;
    let url = rig.url("orders");

    let purged = rig.query("PurgeQueue", &[("QueueUrl", &url)]).await;
    assert_eq!(purged.status, 200, "{}", purged.body);
    assert!(
        purged.body.contains("<PurgeQueueResponse"),
        "{}",
        purged.body
    );

    let again = rig.query("PurgeQueue", &[("QueueUrl", &url)]).await;
    assert_eq!(again.status, 403, "{}", again.body);
    assert_eq!(
        again.expect_tag("Code"),
        "AWS.SimpleQueueService.PurgeQueueInProgress"
    );
}

/// The batch cap, reached through `dispatch`: AWS gives the over-long batch its
/// own code, and an SDK's batching helper branches on it.
#[tokio::test]
async fn a_batch_over_the_cap_is_refused_with_its_own_code() {
    let rig = Rig::open().await;
    let url = rig.create("orders").await.json()["QueueUrl"]
        .as_str()
        .expect("a URL")
        .to_string();
    let entries: Vec<Value> = (0..11)
        .map(|i| json!({"Id": format!("e{i}"), "MessageBody": "x"}))
        .collect();
    let answer = rig
        .json(
            "SendMessageBatch",
            json!({"QueueUrl": url, "Entries": entries}),
        )
        .await;
    assert_eq!(answer.status, 400);
    assert_eq!(
        answer.json()["__type"],
        "com.amazonaws.sqs#TooManyEntriesInBatchRequest"
    );
}

// ------------------------------------------------------------- the Query protocol

/// The other half of the field, over the wire: a form body in, an XML document
/// out, wrapped in the envelope the Query-era SDKs parse.
#[tokio::test]
async fn a_query_create_and_send_round_trip_answers_xml() {
    let rig = Rig::open().await;
    let created = rig.query("CreateQueue", &[("QueueName", "orders")]).await;
    assert_eq!(created.status, 200, "{}", created.body);
    assert_eq!(
        created.header("content-type"),
        Some("text/xml"),
        "{}",
        created.body
    );
    assert!(
        created
            .body
            .starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"),
        "{}",
        created.body
    );
    assert!(
        created
            .body
            .contains("<CreateQueueResponse xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">"),
        "{}",
        created.body
    );
    let url = created.expect_tag("QueueUrl").to_string();
    assert_eq!(url, rig.url("orders"));
    assert_eq!(
        created.expect_tag("RequestId"),
        created.header("x-amzn-requestid").expect("an id"),
        "the id in the envelope is the id in the header"
    );

    let sent = rig
        .query(
            "SendMessage",
            &[
                ("QueueUrl", &url),
                ("MessageBody", "This is a test message"),
            ],
        )
        .await;
    assert_eq!(sent.status, 200, "{}", sent.body);
    assert!(sent.body.contains("<SendMessageResult>"), "{}", sent.body);
    assert_eq!(
        sent.expect_tag("MD5OfMessageBody"),
        "fafb00f5732ab283681e124bf8747ed1"
    );
    // The MessageId is the BROKER's own message id, whatever shape it mints —
    // this facade never invents one, so the assertion is that it is there and
    // was not swallowed by the codec.
    assert!(!sent.expect_tag("MessageId").is_empty(), "{}", sent.body);

    let received = rig.query("ReceiveMessage", &[("QueueUrl", &url)]).await;
    assert_eq!(received.expect_tag("Body"), "This is a test message");
    let handle = received.expect_tag("ReceiptHandle").to_string();

    let deleted = rig
        .query(
            "DeleteMessage",
            &[("QueueUrl", &url), ("ReceiptHandle", &handle)],
        )
        .await;
    assert_eq!(deleted.status, 200, "{}", deleted.body);
    assert!(
        deleted.body.contains("<DeleteMessageResponse"),
        "an action with no output shape still gets its envelope: {}",
        deleted.body
    );
    assert!(
        !deleted.body.contains("<DeleteMessageResult"),
        "...and no result element: {}",
        deleted.body
    );
}

/// `ChangeMessageVisibilityBatch` over the Query protocol, which is where its
/// entry list has a spelling of its OWN:
/// `ChangeMessageVisibilityBatchRequestEntry.N.*`, lifted onto the canonical
/// `Entries` by the codec. An action missing from that lift table would answer
/// `EmptyBatchRequest` to a request that named two entries — a failure no unit
/// test above the codec can see.
#[tokio::test]
async fn a_query_visibility_batch_lifts_its_own_entry_spelling() {
    let rig = Rig::open().await;
    rig.query("CreateQueue", &[("QueueName", "orders")]).await;
    let url = rig.url("orders");
    rig.query(
        "SendMessage",
        &[("QueueUrl", &url), ("MessageBody", "kept")],
    )
    .await;
    let handle = rig
        .query("ReceiveMessage", &[("QueueUrl", &url)])
        .await
        .expect_tag("ReceiptHandle")
        .to_string();

    let answer = rig
        .query(
            "ChangeMessageVisibilityBatch",
            &[
                ("QueueUrl", &url),
                ("ChangeMessageVisibilityBatchRequestEntry.1.Id", "kept"),
                (
                    "ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle",
                    &handle,
                ),
                (
                    "ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout",
                    "120",
                ),
                ("ChangeMessageVisibilityBatchRequestEntry.2.Id", "forged"),
                (
                    "ChangeMessageVisibilityBatchRequestEntry.2.ReceiptHandle",
                    "not-a-handle",
                ),
            ],
        )
        .await;

    assert_eq!(answer.status, 200, "{}", answer.body);
    // The success carries the action's OWN result element, and the failure the
    // one every batch action shares.
    assert!(
        answer
            .body
            .contains("<ChangeMessageVisibilityBatchResultEntry>"),
        "{}",
        answer.body
    );
    assert!(answer.body.contains("<Id>kept</Id>"), "{}", answer.body);
    assert!(
        answer.body.contains("<BatchResultErrorEntry>"),
        "{}",
        answer.body
    );
    assert!(answer.body.contains("<Id>forged</Id>"), "{}", answer.body);
    assert!(
        answer.body.contains("<Code>ReceiptHandleIsInvalid</Code>"),
        "{}",
        answer.body
    );
    assert!(
        answer.body.contains("<SenderFault>true</SenderFault>"),
        "{}",
        answer.body
    );

    // …and the extension is the one the entry named, so the message is still in
    // flight past its original window.
    assert_eq!(rig.fake.extends.lock().unwrap()[0].1, 120);
}

/// A batch, both legs, with a per-entry failure that does not take the batch
/// down with it: a `BatchResultErrorEntry` beside the successes is the shape
/// every SDK's batching helper reads.
#[tokio::test]
async fn a_batch_reports_per_entry_success_and_failure_side_by_side() {
    let rig = Rig::open().await;
    let url = rig.create("orders").await.json()["QueueUrl"]
        .as_str()
        .expect("a URL")
        .to_string();

    let sent = rig
        .json(
            "SendMessageBatch",
            json!({
                "QueueUrl": url,
                "Entries": [
                    {"Id": "a", "MessageBody": "first"},
                    {"Id": "b", "MessageBody": "second"},
                ]
            }),
        )
        .await
        .json();
    let successful = sent["Successful"].as_array().expect("a list");
    assert_eq!(successful.len(), 2, "{sent}");
    assert_eq!(successful[0]["Id"], "a");
    assert_eq!(successful[1]["Id"], "b");
    assert!(successful[0]["MessageId"].is_string(), "{sent}");

    // One handle this facade minted, and one it did not.
    let handle = rig
        .json("ReceiveMessage", json!({"QueueUrl": url}))
        .await
        .json()["Messages"][0]["ReceiptHandle"]
        .as_str()
        .expect("a handle")
        .to_string();
    let deleted = rig
        .json(
            "DeleteMessageBatch",
            json!({
                "QueueUrl": url,
                "Entries": [
                    {"Id": "real", "ReceiptHandle": handle},
                    {"Id": "forged", "ReceiptHandle": "not-a-handle"},
                ]
            }),
        )
        .await
        .json();
    assert_eq!(deleted["Successful"][0]["Id"], "real", "{deleted}");
    assert_eq!(deleted["Failed"][0]["Id"], "forged", "{deleted}");
    assert_eq!(deleted["Failed"][0]["Code"], "ReceiptHandleIsInvalid");
    assert_eq!(deleted["Failed"][0]["SenderFault"], true);
}

/// The lifecycle verb claim width 1 makes exact: a delivery is kept in flight
/// for longer, and the handle that named it still deletes it afterwards.
#[tokio::test]
async fn a_visibility_change_keeps_the_handle_good() {
    let rig = Rig::open().await;
    let url = rig.create("orders").await.json()["QueueUrl"]
        .as_str()
        .expect("a URL")
        .to_string();
    rig.json(
        "SendMessage",
        json!({"QueueUrl": url, "MessageBody": "kept"}),
    )
    .await;
    let handle = rig
        .json("ReceiveMessage", json!({"QueueUrl": url}))
        .await
        .json()["Messages"][0]["ReceiptHandle"]
        .as_str()
        .expect("a handle")
        .to_string();

    let extended = rig
        .json(
            "ChangeMessageVisibility",
            json!({"QueueUrl": url, "ReceiptHandle": handle, "VisibilityTimeout": 60}),
        )
        .await;
    assert_eq!(extended.status, 200, "{}", extended.body);
    assert_eq!(extended.json(), json!({}));

    let deleted = rig
        .json(
            "DeleteMessage",
            json!({"QueueUrl": url, "ReceiptHandle": handle}),
        )
        .await;
    assert_eq!(
        deleted.status, 200,
        "the handle is not tied to the first window: {}",
        deleted.body
    );
}

/// The plan's loudest non-goal, over the wire: `AddPermission` is ACCEPTED and
/// never enforced. It is validated as far as SQS validates it — so a client
/// still learns that its queue does not exist — and then does nothing, which is
/// the honest half of "accepted and not enforced".
#[tokio::test]
async fn add_permission_is_accepted_validated_and_enforced_by_nothing() {
    let rig = Rig::open().await;
    let url = rig.create("orders").await.json()["QueueUrl"]
        .as_str()
        .expect("a URL")
        .to_string();

    let accepted = rig
        .json(
            "AddPermission",
            json!({
                "QueueUrl": url,
                "Label": "everyone",
                "AWSAccountIds": ["000000000000"],
                "Actions": ["SendMessage"],
            }),
        )
        .await;
    assert_eq!(accepted.status, 200, "{}", accepted.body);
    assert_eq!(accepted.json(), json!({}));

    // Validated: the label AWS requires, and the queue AWS checks.
    let unlabelled = rig.json("AddPermission", json!({"QueueUrl": url})).await;
    assert_eq!(unlabelled.status, 400, "{}", unlabelled.body);
    assert_eq!(
        unlabelled.json()["__type"],
        "com.amazonaws.sqs#MissingParameter"
    );
    let absent = rig
        .json(
            "RemovePermission",
            json!({"QueueUrl": rig.url("absent"), "Label": "everyone"}),
        )
        .await;
    assert_eq!(absent.status, 400, "{}", absent.body);
    assert_eq!(
        absent.json()["__type"],
        "com.amazonaws.sqs#QueueDoesNotExist"
    );

    // Not enforced: nothing about the queue changed, and no policy is reported.
    let attributes = rig
        .json(
            "GetQueueAttributes",
            json!({"QueueUrl": url, "AttributeNames": ["All"]}),
        )
        .await
        .json();
    assert!(
        attributes["Attributes"].get("Policy").is_none(),
        "a policy nothing reads is a policy nothing reports: {attributes}"
    );
}

/// The catalog's OTHER rendering, over the wire. Same failure, different code
/// string, different document — which is why the catalog carries both.
#[tokio::test]
async fn a_query_error_renders_as_an_xml_error_response() {
    let rig = Rig::open().await;
    let answer = rig
        .query(
            "SendMessage",
            &[("QueueUrl", &rig.url("absent")), ("MessageBody", "x")],
        )
        .await;
    assert_eq!(answer.status, 400);
    assert_eq!(answer.header("content-type"), Some("text/xml"));
    assert!(answer.body.contains("<ErrorResponse"), "{}", answer.body);
    assert_eq!(answer.expect_tag("Type"), "Sender");
    assert_eq!(
        answer.expect_tag("Code"),
        "AWS.SimpleQueueService.NonExistentQueue"
    );
    assert!(
        !answer.body.contains("com.amazonaws.sqs#"),
        "the JSON spelling has no business here: {}",
        answer.body
    );
}

/// ONE action layer under two codecs: a queue created through the form is a
/// queue the JSON protocol lists, with the same URL.
#[tokio::test]
async fn the_two_protocols_are_one_implementation() {
    let rig = Rig::open().await;
    rig.query("CreateQueue", &[("QueueName", "from-the-form")])
        .await;
    let listed = rig.json("ListQueues", json!({})).await.json();
    assert_eq!(
        listed["QueueUrls"],
        json!([rig.url("from-the-form")]),
        "{listed}"
    );

    // ...and the reverse, including the empty listing's shape.
    let listed = rig
        .query("ListQueues", &[("QueueNamePrefix", "nothing-matches")])
        .await;
    assert!(listed.body.contains("<ListQueuesResult"), "{}", listed.body);
    assert!(!listed.body.contains("<QueueUrl>"), "{}", listed.body);
}

/// The Query protocol carries a number as the STRING it is; the JSON one carries
/// a number. One action implementation must read both — the single place the two
/// protocols do not converge on their own, and the one every ceiling depends on.
///
/// The proof is the CEILING, not the value: a parameter that reached the action
/// as an unparsed string would silently fall back to a default, and a test that
/// only sent a legal value could not tell the difference. Eleven is over SQS's
/// cap of ten, so only a reader that turned both spellings into a number refuses
/// both.
#[tokio::test]
async fn a_number_is_the_same_parameter_from_either_codec() {
    let rig = Rig::open().await;
    let url = rig.create("orders").await.json()["QueueUrl"]
        .as_str()
        .expect("a URL")
        .to_string();

    let over_by_form = rig
        .query(
            "ReceiveMessage",
            &[("QueueUrl", &url), ("MaxNumberOfMessages", "11")],
        )
        .await;
    assert_eq!(over_by_form.status, 400, "{}", over_by_form.body);
    assert_eq!(over_by_form.expect_tag("Code"), "InvalidParameterValue");

    let over_by_json = rig
        .json(
            "ReceiveMessage",
            json!({"QueueUrl": url, "MaxNumberOfMessages": 11}),
        )
        .await;
    assert_eq!(over_by_json.status, 400, "{}", over_by_json.body);
    assert_eq!(
        over_by_json.json()["__type"],
        "com.amazonaws.sqs#InvalidParameterValue"
    );

    // ...and the value at the cap is served by both.
    rig.json(
        "SendMessage",
        json!({"QueueUrl": url, "MessageBody": "one"}),
    )
    .await;
    let at_cap = rig
        .query(
            "ReceiveMessage",
            &[("QueueUrl", &url), ("MaxNumberOfMessages", "10")],
        )
        .await;
    assert_eq!(at_cap.status, 200, "{}", at_cap.body);
    assert_eq!(at_cap.expect_tag("Body"), "one");
    assert_eq!(
        rig.json(
            "ReceiveMessage",
            json!({"QueueUrl": url, "MaxNumberOfMessages": 10})
        )
        .await
        .status,
        200
    );
}

// -------------------------------------------------------------------- addressing

/// The URL a client is handed names the host that client REACHED, not the one
/// this process bound — which is the difference between a working SDK and one
/// that posts its next request into the void behind a load balancer.
#[tokio::test]
async fn a_queue_url_names_the_host_the_client_reached() {
    let rig = Rig::open().await;
    let created = rig.create("orders").await.json();
    let url = created["QueueUrl"].as_str().expect("a URL");
    assert_eq!(url, format!("http://{}/000000000000/orders", rig.host));

    // And the URL round-trips: the next action, posted to the root as every SDK
    // does, addresses the queue by that string.
    let attributes = rig
        .json(
            "GetQueueAttributes",
            json!({"QueueUrl": url, "AttributeNames": ["QueueArn"]}),
        )
        .await
        .json();
    assert_eq!(
        attributes["Attributes"]["QueueArn"],
        "arn:aws:sqs:queen-1:000000000000:orders"
    );
}

// -------------------------------------------------------------------------- auth

/// The development listener serves a request that carries no credential at all.
#[tokio::test]
async fn with_auth_off_an_unsigned_request_is_served() {
    let rig = Rig::open().await;
    let answer = rig.json("ListQueues", json!({})).await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    // ...and reaches Queen as the process default, never as the principal whose
    // key id the request could have claimed without proving.
    assert_eq!(rig.bearers(), vec![Some(DEFAULT_TOKEN.to_string())]);
}

/// The production listener, end to end: a signature this test built from the
/// specification verifies, and the whole round trip works under it.
#[tokio::test]
async fn with_sigv4_a_signed_request_is_served() {
    let rig = Rig::signed().await;
    let created = rig.create("orders").await;
    assert_eq!(created.status, 200, "{}", created.body);
    let url = created.json()["QueueUrl"]
        .as_str()
        .expect("a URL")
        .to_string();

    let sent = rig
        .json(
            "SendMessage",
            json!({"QueueUrl": url, "MessageBody": "signed"}),
        )
        .await;
    assert_eq!(sent.status, 200, "{}", sent.body);
    let received = rig
        .json("ReceiveMessage", json!({"QueueUrl": url}))
        .await
        .json();
    assert_eq!(received["Messages"][0]["Body"], "signed");
}

/// A verified principal's OWN bearer is what reaches Queen — the one place a
/// credential becomes a token.
#[tokio::test]
async fn a_verified_principal_reaches_queen_as_itself() {
    let rig = Rig::signed().await;
    assert_eq!(rig.create("orders").await.status, 200);
    assert_eq!(rig.bearers(), vec![Some(PRINCIPAL_TOKEN.to_string())]);
}

/// No credential on a verifying listener: the code AWS answers, and the one an
/// SDK reports as "no credentials configured" rather than as a server fault.
#[tokio::test]
async fn with_sigv4_an_unsigned_request_is_refused() {
    let rig = Rig::verifying().await;
    let answer = rig.json("ListQueues", json!({})).await;
    // 403, which is the status AWS gives every authentication refusal and the
    // one an SDK's credential provider branches on.
    assert_eq!(answer.status, 403, "{}", answer.body);
    assert_eq!(
        answer.json()["__type"],
        "com.amazonaws.sqs#MissingAuthenticationToken"
    );
    assert!(
        rig.fake.tokens.lock().unwrap().is_empty(),
        "a refused request reaches Queen not at all"
    );
}

/// The property the whole verifier exists for: the signature covers the BODY.
/// Sign one request, send another.
#[tokio::test]
async fn with_sigv4_a_tampered_body_is_refused() {
    let rig = Rig::verifying().await;
    let mut headers = vec![
        (
            "x-amz-target".to_string(),
            "AmazonSQS.ListQueues".to_string(),
        ),
        ("content-type".to_string(), JSON_CONTENT_TYPE.to_string()),
        ("host".to_string(), rig.host.clone()),
    ];
    let signed_body = br#"{"QueueNamePrefix":"a"}"#.to_vec();
    sign(
        AKID,
        SECRET,
        "queen-1",
        "sqs",
        "POST",
        "/",
        "",
        &mut headers,
        &signed_body,
        &compact_now(),
    );
    let answer = rig
        .send(
            "POST",
            "/",
            "",
            headers,
            br#"{"QueueNamePrefix":"b"}"#.to_vec(),
        )
        .await;
    assert_eq!(answer.status, 403, "{}", answer.body);
    assert_eq!(
        answer.json()["__type"],
        "com.amazonaws.sqs#SignatureDoesNotMatch"
    );
}

/// An access key id this deployment does not know is told so, rather than being
/// told its signature is wrong: AWS distinguishes the two, and a client sent to
/// debug the wrong field loses an afternoon.
#[tokio::test]
async fn with_sigv4_an_unknown_key_id_is_refused_as_such() {
    let mut rig = Rig::verifying().await;
    rig.credential = Some(("AKIDNOTOURS".to_string(), SECRET.to_string()));
    let answer = rig.json("ListQueues", json!({})).await;
    assert_eq!(answer.status, 403, "{}", answer.body);
    assert_eq!(
        answer.json()["__type"],
        "com.amazonaws.sqs#InvalidClientTokenId"
    );
}

/// A signature over the request LINE, not just the body: the same bytes signed
/// for one path, replayed against another, do not verify.
#[tokio::test]
async fn with_sigv4_a_replayed_signature_does_not_travel_to_another_path() {
    let rig = Rig::verifying().await;
    let body = json!({}).to_string().into_bytes();
    let mut headers = vec![
        (
            "x-amz-target".to_string(),
            "AmazonSQS.ListQueues".to_string(),
        ),
        ("content-type".to_string(), JSON_CONTENT_TYPE.to_string()),
        ("host".to_string(), rig.host.clone()),
    ];
    sign(
        AKID,
        SECRET,
        "queen-1",
        "sqs",
        "POST",
        "/",
        "",
        &mut headers,
        &body,
        &compact_now(),
    );
    // The signature is good for `/`. The same headers on the queue's own URL —
    // which SQS accepts as an address — are a canonical request nobody signed.
    let answer = rig
        .send("POST", "/000000000000/orders", "", headers, body)
        .await;
    assert_eq!(answer.status, 403, "{}", answer.body);
    assert_eq!(
        answer.json()["__type"],
        "com.amazonaws.sqs#SignatureDoesNotMatch"
    );
}

/// A refusal that happens BEFORE the body is decoded still answers in the
/// client's own protocol — which the facade knows from the headers alone.
#[tokio::test]
async fn a_signature_refusal_answers_in_the_protocol_it_arrived_in() {
    let rig = Rig::verifying().await;
    let answer = rig.query("ListQueues", &[]).await;
    assert_eq!(answer.status, 403, "{}", answer.body);
    assert_eq!(answer.header("content-type"), Some("text/xml"));
    assert!(answer.body.contains("<ErrorResponse"), "{}", answer.body);
    assert_eq!(answer.expect_tag("Code"), "MissingAuthenticationToken");
    assert!(
        answer.header("x-amzn-requestid").is_some(),
        "a refusal is traceable too"
    );
}

/// An error raised BEFORE the body is decoded still has to name the API the
/// client addressed. SNS is the whole reason: it never moved off the Query
/// protocol, its namespace is not SQS's, and a refusal is the commonest answer a
/// misconfigured client ever sees.
#[tokio::test]
async fn a_refusal_before_the_body_is_decoded_keeps_the_apis_own_namespace() {
    let rig = Rig::verifying().await;
    let form = form_urlencoded::Serializer::new(String::new())
        .append_pair("Action", "ListTopics")
        .append_pair("Version", crate::proto::query::VERSION_SNS)
        .finish();
    let answer = rig
        .send(
            "POST",
            "/",
            "",
            vec![("content-type".to_string(), FORM_CONTENT_TYPE.to_string())],
            form.into_bytes(),
        )
        .await;
    assert_eq!(answer.status, 403, "{}", answer.body);
    assert!(
        answer
            .body
            .contains("xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\""),
        "{}",
        answer.body
    );
    // ...and an SQS client is still answered in SQS's, which is the case that
    // must not have moved.
    let answer = rig.query("ListQueues", &[]).await;
    assert!(
        answer
            .body
            .contains("xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\""),
        "{}",
        answer.body
    );
}

/// The presigned variant, which is the only path on which the QUERY STRING is
/// both the signature's input and the request's parameters. It exercises the two
/// pseudo-headers that carry a request line no header holds.
#[tokio::test]
async fn a_presigned_get_is_verified_and_served() {
    let rig = Rig::verifying().await;
    let amz_date = compact_now();
    let scope = format!("{}/queen-1/sqs/aws4_request", &amz_date[..8]);
    // Sorted by byte, which is what the canonical query is — so the string sent
    // and the string signed are the same one.
    let query = [
        ("Action", "ListQueues"),
        ("Version", SQS_VERSION),
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
        ("X-Amz-Credential", &format!("{AKID}/{scope}")),
        ("X-Amz-Date", &amz_date),
        ("X-Amz-Expires", "300"),
        ("X-Amz-SignedHeaders", "host"),
    ]
    .iter()
    .map(|(name, value)| format!("{}={}", encode(name), encode(value)))
    .collect::<Vec<String>>()
    .join("&");

    let canonical = format!(
        "GET\n/\n{query}\nhost:{}\n\nhost\nUNSIGNED-PAYLOAD",
        rig.host
    );
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let signature = hex::encode(hmac(
        &derive(SECRET, &amz_date[..8], "queen-1", "sqs"),
        to_sign.as_bytes(),
    ));

    let answer = rig
        .send(
            "GET",
            "/",
            &format!("{query}&X-Amz-Signature={signature}"),
            Vec::new(),
            Vec::new(),
        )
        .await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    // The parameters were read from the query string too, not only the
    // signature: this is a real ListQueues.
    assert!(
        answer.body.contains("<ListQueuesResponse"),
        "{}",
        answer.body
    );

    // One character of the signature, and it is nothing. The replacement is
    // chosen against the character it replaces: a fixed `0` is the same
    // signature one time in sixteen, which is a test that passes fifteen times
    // and then reports a facade that accepts forged URLs.
    let last = signature.chars().last().expect("a signature");
    let broken = format!(
        "{}{}",
        &signature[..signature.len() - 1],
        if last == '0' { '1' } else { '0' }
    );
    let refused = rig
        .send(
            "GET",
            "/",
            &format!("{query}&X-Amz-Signature={broken}"),
            Vec::new(),
            Vec::new(),
        )
        .await;
    assert_eq!(refused.status, 403, "{}", refused.body);
}

// -------------------------------------------------------------------- the limits

/// The cap is on BYTES READ. Without it any unauthenticated client can ask this
/// process to buffer whatever it likes, and the signature that would have
/// refused it is computed over the body it is still reading.
#[tokio::test]
async fn a_body_over_the_cap_is_refused_and_reaches_nothing() {
    let rig = Rig::verifying().await;
    let oversized = vec![b'x'; crate::MAX_BODY_BYTES + 1];
    let answer = rig
        .send(
            "POST",
            "/",
            "",
            vec![
                (
                    "x-amz-target".to_string(),
                    "AmazonSQS.SendMessage".to_string(),
                ),
                ("content-type".to_string(), JSON_CONTENT_TYPE.to_string()),
            ],
            oversized,
        )
        .await;
    assert_eq!(answer.status, 400, "{}", answer.body);
    assert_eq!(
        answer.json()["__type"],
        "com.amazonaws.sqs#InvalidParameterValue"
    );
    assert!(
        answer.json()["message"]
            .as_str()
            .unwrap_or_default()
            .contains(&crate::MAX_BODY_BYTES.to_string()),
        "the refusal names the cap: {}",
        answer.body
    );
    assert!(
        rig.fake.tokens.lock().unwrap().is_empty(),
        "nothing that large was ever attributed, let alone forwarded"
    );
}

/// A body just under the cap is served — the boundary is a cap and not a
/// smaller number nobody documented.
#[tokio::test]
async fn a_body_under_the_cap_is_served() {
    let rig = Rig::open().await;
    let url = rig.create("orders").await.json()["QueueUrl"]
        .as_str()
        .expect("a URL")
        .to_string();
    // Under 2 MiB on the wire, and under the queue's own 256 KiB
    // MaximumMessageSize, which is the ceiling the ACTION applies.
    let body = "y".repeat(200_000);
    let sent = rig
        .json("SendMessage", json!({"QueueUrl": url, "MessageBody": body}))
        .await;
    assert_eq!(sent.status, 200, "{}", sent.body);

    // ...and the action's own ceiling still refuses what the transport allowed.
    let too_big = "z".repeat(300_000);
    let refused = rig
        .json(
            "SendMessage",
            json!({"QueueUrl": url, "MessageBody": too_big}),
        )
        .await;
    assert_eq!(refused.status, 400, "{}", refused.body);
    assert_eq!(
        refused.json()["__type"],
        "com.amazonaws.sqs#InvalidParameterValue"
    );
}

/// A request that names no action at all — neither header nor form field — is
/// `InvalidAction` rather than a parse failure with no code.
#[tokio::test]
async fn a_request_that_names_no_action_is_refused_by_code() {
    let rig = Rig::open().await;
    let answer = rig
        .send(
            "POST",
            "/",
            "",
            vec![("content-type".to_string(), FORM_CONTENT_TYPE.to_string())],
            b"QueueUrl=http://h/q".to_vec(),
        )
        .await;
    assert_eq!(answer.status, 400);
    assert_eq!(answer.expect_tag("Code"), "InvalidAction");
}

/// `handle_request` is the same path without a socket, which is what the
/// conformance corpus drives. It must answer identically — including the
/// pseudo-headers, since a caller that omits them gets the defaults.
#[tokio::test]
async fn the_socketless_entry_point_answers_the_same_thing() {
    let fake = FakeQueen::empty();
    let queen: Arc<dyn QueenApi> = Arc::clone(&fake) as Arc<dyn QueenApi>;
    let facade = Arc::new(Facade::new(
        Config {
            listen: "127.0.0.1:9324".to_string(),
            auth: AuthMode::Off,
            credentials: Directory::empty(),
            region: crate::config::DEFAULT_REGION.to_string(),
            account: crate::config::DEFAULT_ACCOUNT.to_string(),
            receive_mode: ReceiveMode::Exact,
            default_partitions: 4,
            handle_secret: b"an integration handle secret".to_vec(),
            handle_secret_generated: false,
            queen_url: "http://queen.invalid:6632".to_string(),
            queen_token: None,
            embedded: false,
            shutdown_grace_ms: 0,
            tls: None,
        },
        queen,
    ));
    let headers = vec![
        (
            "x-amz-target".to_string(),
            "AmazonSQS.CreateQueue".to_string(),
        ),
        ("host".to_string(), "sqs.example.test:9324".to_string()),
    ];
    let answer = crate::handle_request(&facade, &headers, br#"{"QueueName":"orders"}"#).await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    let payload: Value = serde_json::from_str(&answer.body).expect("JSON");
    assert_eq!(
        payload["QueueUrl"],
        "http://sqs.example.test:9324/000000000000/orders"
    );
    assert!(crate::proto::header(&answer.headers, "x-amzn-requestid").is_some());
}

/// A FIFO queue over the wire, out-of-order deletes and all (M2).
///
/// The unit tests below the codec can see the delete-set and the cursor; only
/// this one sees that the pieces those tests drive are reachable through a
/// socket: that a `.fifo` queue is created over the JSON protocol, that a
/// receive answers a whole group in order with its `MessageGroupId`, that a
/// mid-batch `DeleteMessage` is answered like any other, and that the messages
/// it did NOT name come back — which is the property a naive facade loses and
/// no client would ever discover from a status code.
#[tokio::test]
async fn a_fifo_round_trip_deletes_out_of_order_and_keeps_the_rest() {
    let rig = Rig::open().await;
    let created = rig
        .json(
            "CreateQueue",
            json!({"QueueName": "orders.fifo", "Attributes": {"FifoQueue": "true"}}),
        )
        .await;
    assert_eq!(created.status, 200, "{}", created.body);
    let url = rig.url("orders.fifo");

    for (index, dedup) in ["d1", "d2", "d3"].iter().enumerate() {
        let sent = rig
            .json(
                "SendMessage",
                json!({
                    "QueueUrl": url,
                    "MessageBody": format!("m{index}"),
                    "MessageGroupId": "customer-7",
                    "MessageDeduplicationId": dedup
                }),
            )
            .await;
        assert_eq!(sent.status, 200, "{}", sent.body);
        // The SequenceNumber is the offset the push allocated, counting within
        // the group.
        assert_eq!(sent.json()["SequenceNumber"], index.to_string());
    }

    let received = rig
        .json(
            "ReceiveMessage",
            json!({"QueueUrl": url, "MaxNumberOfMessages": 10, "AttributeNames": ["All"]}),
        )
        .await
        .json();
    let messages = received["Messages"].as_array().expect("a list").clone();
    let bodies: Vec<&str> = messages
        .iter()
        .map(|m| m["Body"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(bodies, ["m0", "m1", "m2"], "one group, in order");
    assert_eq!(messages[0]["Attributes"]["MessageGroupId"], "customer-7");
    assert_eq!(messages[1]["Attributes"]["MessageDeduplicationId"], "d2");

    // The middle one, and only the middle one.
    let deleted = rig
        .json(
            "DeleteMessage",
            json!({"QueueUrl": url, "ReceiptHandle": messages[1]["ReceiptHandle"]}),
        )
        .await;
    assert_eq!(deleted.status, 200, "{}", deleted.body);
    assert_eq!(deleted.json(), json!({}));

    // The claim goes back, and what the client did not delete comes back with
    // it — the two messages around the gap, never the one it deleted.
    rig.fake.advance(std::time::Duration::from_secs(31));
    let again = rig
        .json(
            "ReceiveMessage",
            json!({"QueueUrl": url, "MaxNumberOfMessages": 10}),
        )
        .await
        .json();
    let bodies: Vec<&str> = again["Messages"]
        .as_array()
        .expect("a list")
        .iter()
        .map(|m| m["Body"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(bodies, ["m0", "m2"]);
}

/// The same queue over the QUERY protocol, which is what async-aws and the
/// older SDK majors speak: `ReceiveRequestAttemptId` and the FIFO send
/// parameters are flat form fields there, and a retry of one receive must
/// answer the same messages rather than claim a second group.
#[tokio::test]
async fn a_query_fifo_receive_replays_its_attempt_id() {
    let rig = Rig::open().await;
    rig.query(
        "CreateQueue",
        &[
            ("QueueName", "orders.fifo"),
            ("Attribute.1.Name", "FifoQueue"),
            ("Attribute.1.Value", "true"),
        ],
    )
    .await;
    let url = rig.url("orders.fifo");
    let sent = rig
        .query(
            "SendMessage",
            &[
                ("QueueUrl", &url),
                ("MessageBody", "hello"),
                ("MessageGroupId", "g"),
                ("MessageDeduplicationId", "d1"),
            ],
        )
        .await;
    assert_eq!(sent.status, 200, "{}", sent.body);
    assert!(
        sent.body.contains("<SequenceNumber>0</SequenceNumber>"),
        "{}",
        sent.body
    );

    let first = rig
        .query(
            "ReceiveMessage",
            &[("QueueUrl", &url), ("ReceiveRequestAttemptId", "attempt-1")],
        )
        .await;
    assert_eq!(first.status, 200, "{}", first.body);
    assert!(first.body.contains("<Body>hello</Body>"), "{}", first.body);

    let again = rig
        .query(
            "ReceiveMessage",
            &[("QueueUrl", &url), ("ReceiveRequestAttemptId", "attempt-1")],
        )
        .await;
    // Everything but the `RequestId`, which is this request's own and is meant
    // to differ: the same messages under the same receipt handles.
    let strip = |body: &str| {
        body.lines()
            .filter(|line| !line.contains("<RequestId>"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    assert_eq!(
        strip(&again.body),
        strip(&first.body),
        "the same attempt id answers the same messages"
    );
    assert!(
        again.body.contains("<ReceiptHandle>"),
        "handles and all: {}",
        again.body
    );
}

// ------------------------------------------------------------------ M3: DLQ

/// `ListDeadLetterSourceQueues` over the QUERY protocol, which is where its
/// answer's shape is decided: AWS's model spells the member `queueUrls`, and the
/// SQS flattening renders it as a repeated `<QueueUrl>` with the plural never
/// written at all. No unit test above the codec can see that.
#[tokio::test]
async fn the_dead_letter_sources_flatten_into_the_query_protocols_own_shape() {
    let rig = Rig::open().await;
    rig.create("orders-dlq").await;
    let policy = format!(
        r#"{{"deadLetterTargetArn":"arn:aws:sqs:{}:{}:orders-dlq","maxReceiveCount":3}}"#,
        crate::config::DEFAULT_REGION,
        crate::config::DEFAULT_ACCOUNT
    );
    let created = rig
        .json(
            "CreateQueue",
            json!({"QueueName": "orders", "Attributes": {"RedrivePolicy": policy}}),
        )
        .await;
    assert_eq!(created.status, 200, "{}", created.body);

    let answer = rig
        .query(
            "ListDeadLetterSourceQueues",
            &[("QueueUrl", &rig.url("orders-dlq"))],
        )
        .await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert!(
        answer.body.contains("<ListDeadLetterSourceQueuesResult>"),
        "{}",
        answer.body
    );
    assert_eq!(answer.expect_tag("QueueUrl"), rig.url("orders"));
    assert!(!answer.body.contains("queueUrls"), "{}", answer.body);

    // ...and the JSON protocol keeps the member name AWS's model gives it.
    let json = rig
        .json(
            "ListDeadLetterSourceQueues",
            json!({"QueueUrl": rig.url("orders-dlq")}),
        )
        .await;
    assert_eq!(json.json()["queueUrls"], json!([rig.url("orders")]));
}

/// A `RedrivePolicy` that names nothing is refused ON THE WIRE, in the protocol
/// it arrived in — and the refusal is `InvalidParameterValue`, which is the code
/// an SDK's validation branch reads rather than a 500.
#[tokio::test]
async fn a_policy_naming_no_queue_is_refused_in_both_protocols() {
    let rig = Rig::open().await;
    let policy = format!(
        r#"{{"deadLetterTargetArn":"arn:aws:sqs:{}:{}:nowhere","maxReceiveCount":3}}"#,
        crate::config::DEFAULT_REGION,
        crate::config::DEFAULT_ACCOUNT
    );
    let answer = rig
        .json(
            "CreateQueue",
            json!({"QueueName": "orders", "Attributes": {"RedrivePolicy": policy.clone()}}),
        )
        .await;
    assert_eq!(answer.status, 400);
    assert_eq!(
        answer.json()["__type"],
        "com.amazonaws.sqs#InvalidParameterValue"
    );

    let answer = rig
        .query(
            "CreateQueue",
            &[
                ("QueueName", "orders-2"),
                ("Attribute.1.Name", "RedrivePolicy"),
                ("Attribute.1.Value", &policy),
            ],
        )
        .await;
    assert_eq!(answer.status, 400);
    assert_eq!(answer.expect_tag("Code"), "InvalidParameterValue");
    assert_eq!(answer.expect_tag("Type"), "Sender");
}

/// The move-task trio end to end: start, list, cancel. The two codes AWS added
/// with this API in 2023 are rendered here for the first time, and their
/// spellings differ between the protocols.
#[tokio::test]
async fn the_message_move_task_trio_answers_over_the_wire() {
    let rig = Rig::open().await;
    rig.create("orders-dlq").await;
    let policy = format!(
        r#"{{"deadLetterTargetArn":"arn:aws:sqs:{}:{}:orders-dlq","maxReceiveCount":1}}"#,
        crate::config::DEFAULT_REGION,
        crate::config::DEFAULT_ACCOUNT
    );
    rig.json(
        "CreateQueue",
        json!({"QueueName": "orders", "Attributes": {"RedrivePolicy": policy}}),
    )
    .await;
    let source_arn = format!(
        "arn:aws:sqs:{}:{}:orders-dlq",
        crate::config::DEFAULT_REGION,
        crate::config::DEFAULT_ACCOUNT
    );

    // A queue that is not a dead-letter target cannot be a source, and the
    // refusal is `UnsupportedOperation` — whose Query spelling carries the
    // `AWS.SimpleQueueService.` prefix its JSON one does not.
    let refused = rig
        .query(
            "StartMessageMoveTask",
            &[(
                "SourceArn",
                &format!(
                    "arn:aws:sqs:{}:{}:orders",
                    crate::config::DEFAULT_REGION,
                    crate::config::DEFAULT_ACCOUNT
                ),
            )],
        )
        .await;
    assert_eq!(refused.status, 400, "{}", refused.body);
    assert_eq!(
        refused.expect_tag("Code"),
        "AWS.SimpleQueueService.UnsupportedOperation"
    );

    // The dead-letter queue itself is a legal source.
    let started = rig
        .json("StartMessageMoveTask", json!({"SourceArn": source_arn}))
        .await;
    assert_eq!(started.status, 200, "{}", started.body);
    let handle = started.json()["TaskHandle"]
        .as_str()
        .expect("a handle")
        .to_string();

    // A handle naming no task is `ResourceNotFoundException`, spelled the same
    // way in both protocols and answered 404.
    let unknown = rig
        .json("CancelMessageMoveTask", json!({"TaskHandle": "Z m9v"}))
        .await;
    assert_eq!(unknown.status, 404, "{}", unknown.body);
    assert_eq!(
        unknown.json()["__type"],
        "com.amazonaws.sqs#ResourceNotFoundException"
    );

    // The listing answers the task, and the cancel takes its handle.
    let listed = rig
        .json("ListMessageMoveTasks", json!({"SourceArn": source_arn}))
        .await;
    assert_eq!(listed.status, 200, "{}", listed.body);
    let results = listed.json()["Results"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(results.len(), 1, "{}", listed.body);
    assert_eq!(results[0]["SourceArn"], json!(source_arn));

    // An empty dead-letter queue means the mover may already have completed the
    // task, in which case a cancel is legally refused — either answer is a
    // rendering of this trio and both are asserted rather than raced on.
    let cancelled = rig
        .json("CancelMessageMoveTask", json!({"TaskHandle": handle}))
        .await;
    match cancelled.status {
        200 => assert!(
            cancelled.json()["ApproximateNumberOfMessagesMoved"].is_number(),
            "{}",
            cancelled.body
        ),
        400 => assert_eq!(
            cancelled.json()["__type"],
            "com.amazonaws.sqs#UnsupportedOperation"
        ),
        other => panic!("unexpected status {other}: {}", cancelled.body),
    }
}

// ----------------------------------------------------------------------- sns

/// SNS end to end over the protocol every SNS client actually speaks. What this
/// covers that the action suite cannot: the NAMESPACE, the `<member>` wrapping,
/// and the Query flattening of an attribute map — SNS writes
/// `Attributes.entry.N.key`, which is not SQS's spelling and is decoded by the
/// same codec.
#[tokio::test]
async fn a_topic_and_a_subscription_over_the_query_protocol() {
    let rig = Rig::open().await;
    rig.create("orders").await;

    let created = rig.sns("CreateTopic", &[("Name", "events")]).await;
    assert_eq!(created.status, 200, "{}", created.body);
    assert!(
        created
            .body
            .contains("xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\""),
        "{}",
        created.body
    );
    let topic = created.expect_tag("TopicArn").to_string();
    assert_eq!(topic, "arn:aws:sns:queen-1:000000000000:events");

    // The subscribe carries its attributes in SNS's own flattening.
    let subscribed = rig
        .sns(
            "Subscribe",
            &[
                ("TopicArn", &topic),
                ("Protocol", "sqs"),
                ("Endpoint", "arn:aws:sqs:queen-1:000000000000:orders"),
                ("Attributes.entry.1.key", "RawMessageDelivery"),
                ("Attributes.entry.1.value", "true"),
                ("ReturnSubscriptionArn", "true"),
            ],
        )
        .await;
    assert_eq!(subscribed.status, 200, "{}", subscribed.body);
    let subscription = subscribed.expect_tag("SubscriptionArn").to_string();
    assert!(
        subscription.starts_with(&format!("{topic}:")),
        "{subscription}"
    );

    // ...and it reads back through the attribute map SNS renders as entries.
    let read = rig
        .sns(
            "GetSubscriptionAttributes",
            &[("SubscriptionArn", &subscription)],
        )
        .await;
    assert_eq!(read.status, 200, "{}", read.body);
    assert!(read.body.contains("<entry>"), "{}", read.body);
    assert!(
        read.body.contains("<key>RawMessageDelivery</key>"),
        "{}",
        read.body
    );
    assert!(read.body.contains("<value>true</value>"), "{}", read.body);

    // The listing is `<member>`-wrapped, which is SNS's shape and not SQS's
    // flattened one.
    let listed = rig
        .sns("ListSubscriptionsByTopic", &[("TopicArn", &topic)])
        .await;
    assert_eq!(listed.status, 200, "{}", listed.body);
    assert!(listed.body.contains("<Subscriptions>"), "{}", listed.body);
    assert!(listed.body.contains("<member>"), "{}", listed.body);
    assert_eq!(listed.expect_tag("Protocol"), "sqs");
    assert_eq!(
        listed.expect_tag("Endpoint"),
        "arn:aws:sqs:queen-1:000000000000:orders"
    );

    // The topic listing, the same shape one level up.
    let topics = rig.sns("ListTopics", &[]).await;
    assert!(topics.body.contains("<Topics>"), "{}", topics.body);
    assert_eq!(topics.expect_tag("TopicArn"), topic);
}

/// The one status that separates the two services: SNS answers **404** where
/// every SQS "does not exist" answers 400, and it does so in both codecs.
#[tokio::test]
async fn a_missing_topic_is_a_404_in_both_codecs() {
    let rig = Rig::open().await;
    let absent = "arn:aws:sns:queen-1:000000000000:absent";

    let query = rig.sns("GetTopicAttributes", &[("TopicArn", absent)]).await;
    assert_eq!(query.status, 404, "{}", query.body);
    assert_eq!(query.expect_tag("Code"), "NotFound");
    assert_eq!(query.expect_tag("Type"), "Sender");
    assert!(
        query
            .body
            .contains("xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\""),
        "an SNS error is an SNS document: {}",
        query.body
    );

    let json = rig
        .json_sns("GetTopicAttributes", json!({ "TopicArn": absent }))
        .await;
    assert_eq!(json.status, 404, "{}", json.body);
    assert_eq!(json.json()["__type"], "com.amazonaws.sns#NotFound");

    // ...against SQS's own answer for the same shape of mistake, which is a 400
    // in a different namespace.
    let sqs = rig
        .json("GetQueueAttributes", json!({"QueueUrl": rig.url("absent")}))
        .await;
    assert_eq!(sqs.status, 400, "{}", sqs.body);
    assert_eq!(sqs.json()["__type"], "com.amazonaws.sqs#QueueDoesNotExist");
}

/// SNS's tags are a LIST of pairs and SQS's are a map — the same word, two
/// shapes, and the Query rendering is where the difference is visible.
#[tokio::test]
async fn the_sns_tag_actions_render_a_member_list() {
    let rig = Rig::open().await;
    let topic = rig
        .sns("CreateTopic", &[("Name", "events")])
        .await
        .expect_tag("TopicArn")
        .to_string();

    let tagged = rig
        .sns(
            "TagResource",
            &[
                ("ResourceArn", &topic),
                ("Tags.member.1.Key", "team"),
                ("Tags.member.1.Value", "billing"),
            ],
        )
        .await;
    assert_eq!(tagged.status, 200, "{}", tagged.body);
    // An action with an EMPTY output shape writes its result element; one with
    // no output shape at all writes none (the next test). AWS self-closes the
    // empty one and this writer opens and closes it, which is the same document
    // to every parser — the distinction that carries meaning is present-versus-
    // absent, and that is what is asserted.
    assert!(
        tagged.body.contains("<TagResourceResult"),
        "{}",
        tagged.body
    );

    let listed = rig
        .sns("ListTagsForResource", &[("ResourceArn", &topic)])
        .await;
    assert_eq!(listed.status, 200, "{}", listed.body);
    assert!(listed.body.contains("<Tags>"), "{}", listed.body);
    assert!(listed.body.contains("<member>"), "{}", listed.body);
    assert_eq!(listed.expect_tag("Key"), "team");
    assert_eq!(listed.expect_tag("Value"), "billing");
    // Never SQS's map spelling, which the same member name renders as on the
    // other service.
    assert!(!listed.body.contains("<entry>"), "{}", listed.body);

    let untagged = rig
        .sns(
            "UntagResource",
            &[("ResourceArn", &topic), ("TagKeys.member.1", "team")],
        )
        .await;
    assert_eq!(untagged.status, 200, "{}", untagged.body);
    let listed = rig
        .sns("ListTagsForResource", &[("ResourceArn", &topic)])
        .await;
    assert!(!listed.body.contains("<member>"), "{}", listed.body);

    // ...and the tag actions of a topic that is not there answer their own
    // 404 code, which is not the one every other SNS action answers.
    let gone = rig
        .sns(
            "ListTagsForResource",
            &[("ResourceArn", "arn:aws:sns:queen-1:000000000000:absent")],
        )
        .await;
    assert_eq!(gone.status, 404, "{}", gone.body);
    assert_eq!(gone.expect_tag("Code"), "ResourceNotFound");
}

/// The lifecycle actions with no output shape write no result element, and
/// `SetTopicAttributes` reaches the action through the two-scalar shape only SNS
/// uses.
#[tokio::test]
async fn the_sns_actions_without_a_result_write_no_result_element() {
    let rig = Rig::open().await;
    let topic = rig
        .sns("CreateTopic", &[("Name", "events")])
        .await
        .expect_tag("TopicArn")
        .to_string();

    let set = rig
        .sns(
            "SetTopicAttributes",
            &[
                ("TopicArn", &topic),
                ("AttributeName", "DisplayName"),
                ("AttributeValue", "Events"),
            ],
        )
        .await;
    assert_eq!(set.status, 200, "{}", set.body);
    assert!(
        !set.body.contains("SetTopicAttributesResult"),
        "{}",
        set.body
    );
    assert!(set.body.contains("<RequestId>"), "{}", set.body);
    let read = rig.sns("GetTopicAttributes", &[("TopicArn", &topic)]).await;
    assert!(
        read.body.contains("<key>DisplayName</key>"),
        "{}",
        read.body
    );
    assert!(read.body.contains("<value>Events</value>"), "{}", read.body);

    let deleted = rig.sns("DeleteTopic", &[("TopicArn", &topic)]).await;
    assert_eq!(deleted.status, 200, "{}", deleted.body);
    assert!(
        !deleted.body.contains("DeleteTopicResult"),
        "{}",
        deleted.body
    );
    // Idempotent, over the wire: the second delete is a success too.
    assert_eq!(
        rig.sns("DeleteTopic", &[("TopicArn", &topic)]).await.status,
        200
    );
}

/// The JSON codec reaches the same action layer under SNS's target prefix, and
/// its errors carry SNS's `__type` namespace rather than SQS's.
#[tokio::test]
async fn the_sns_action_set_is_reachable_under_its_own_json_target() {
    let rig = Rig::open().await;
    let created = rig.json_sns("CreateTopic", json!({"Name": "events"})).await;
    assert_eq!(created.status, 200, "{}", created.body);
    assert_eq!(
        created.json()["TopicArn"],
        "arn:aws:sns:queen-1:000000000000:events"
    );
    // A bare JSON object with no envelope: the Query wrapper is the other
    // protocol's shape.
    assert!(
        !created.body.contains("CreateTopicResponse"),
        "{}",
        created.body
    );

    let refused = rig
        .json_sns(
            "CreateTopic",
            json!({"Name": "events", "Attributes": {"DisplayName": "x"}}),
        )
        .await;
    assert_eq!(refused.status, 400, "{}", refused.body);
    assert_eq!(
        refused.json()["__type"],
        "com.amazonaws.sns#InvalidParameter"
    );
    assert_eq!(
        refused.header("x-amzn-query-error"),
        Some("InvalidParameter;Sender")
    );

    // The subscription family over the same codec, which is where the shapes
    // differ most: a list is a JSON array here and a `<member>` sequence there.
    rig.create("orders").await;
    let topic = "arn:aws:sns:queen-1:000000000000:events";
    let subscribed = rig
        .json_sns(
            "Subscribe",
            json!({"TopicArn": topic, "Protocol": "sqs",
                   "Endpoint": "arn:aws:sqs:queen-1:000000000000:orders",
                   "Attributes": {"RawMessageDelivery": "true"}}),
        )
        .await;
    assert_eq!(subscribed.status, 200, "{}", subscribed.body);
    let subscription = subscribed.json()["SubscriptionArn"]
        .as_str()
        .expect("an ARN")
        .to_string();

    let listed = rig
        .json_sns("ListSubscriptionsByTopic", json!({ "TopicArn": topic }))
        .await;
    let subscriptions = listed.json()["Subscriptions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(subscriptions.len(), 1, "{}", listed.body);
    assert_eq!(subscriptions[0]["Protocol"], "sqs");

    let read = rig
        .json_sns(
            "GetSubscriptionAttributes",
            json!({ "SubscriptionArn": subscription.clone() }),
        )
        .await;
    assert_eq!(read.json()["Attributes"]["RawMessageDelivery"], "true");

    // An action with no output shape answers `{}` here and no result element
    // there — JSON 1.0 has no way to write "nothing" that an SDK will parse.
    let removed = rig
        .json_sns("Unsubscribe", json!({ "SubscriptionArn": subscription }))
        .await;
    assert_eq!(removed.status, 200, "{}", removed.body);
    assert_eq!(removed.body, "{}");
}

/// THE end-to-end shape a framework builds: a topic, a queue subscribed to it,
/// a publish, and the notification coming back out of `ReceiveMessage`.
///
/// What this covers that the action suite cannot: the two services meeting on
/// one listener in one request sequence, the Query flattening of
/// `MessageAttributes.entry.N.Value.DataType` (SNS's spelling, not SQS's), and
/// the notification surviving as an SQS BODY — the document every SNS-to-SQS
/// consumer parses, read here through the receive path rather than out of the
/// push this facade wrote.
#[tokio::test]
async fn a_publish_reaches_the_subscribed_queue_as_a_notification() {
    let rig = Rig::open().await;
    let url = rig.create("orders").await.json()["QueueUrl"]
        .as_str()
        .expect("a URL")
        .to_string();
    let topic = rig
        .sns("CreateTopic", &[("Name", "events")])
        .await
        .expect_tag("TopicArn")
        .to_string();
    rig.sns(
        "Subscribe",
        &[
            ("TopicArn", &topic),
            ("Protocol", "sqs"),
            ("Endpoint", "arn:aws:sqs:queen-1:000000000000:orders"),
        ],
    )
    .await;

    let published = rig
        .sns(
            "Publish",
            &[
                ("TopicArn", &topic),
                ("Message", "hello from a topic"),
                ("Subject", "a subject"),
                ("MessageAttributes.entry.1.Name", "kind"),
                ("MessageAttributes.entry.1.Value.DataType", "String"),
                ("MessageAttributes.entry.1.Value.StringValue", "order"),
            ],
        )
        .await;
    assert_eq!(published.status, 200, "{}", published.body);
    let message_id = published.expect_tag("MessageId").to_string();
    assert!(!message_id.is_empty());
    // The Query envelope is SNS's, result element and all.
    assert!(
        published.body.contains("<PublishResult>"),
        "{}",
        published.body
    );

    let received = rig
        .json(
            "ReceiveMessage",
            json!({"QueueUrl": url, "MaxNumberOfMessages": 10}),
        )
        .await;
    let messages = received.json()["Messages"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(messages.len(), 1, "{}", received.body);
    let notification: Value = serde_json::from_str(messages[0]["Body"].as_str().expect("a body"))
        .expect("the body is the notification document");
    assert_eq!(notification["Type"], "Notification");
    assert_eq!(notification["Message"], "hello from a topic");
    assert_eq!(notification["Subject"], "a subject");
    assert_eq!(notification["TopicArn"], topic);
    assert_eq!(
        notification["MessageAttributes"]["kind"],
        json!({"Type": "String", "Value": "order"})
    );
    // The publish's own id is inside the envelope; the SQS MessageId is the
    // broker's, one per delivery, and the two are different by design.
    assert_eq!(notification["MessageId"], Value::String(message_id.clone()));
    assert_ne!(messages[0]["MessageId"], Value::String(message_id));
}

/// `PublishBatch` over the Query protocol: SNS's own entry spelling in, SNS's
/// own `<member>` result lists out, and a per-entry failure beside a success.
#[tokio::test]
async fn a_publish_batch_reports_per_entry_over_the_query_protocol() {
    let rig = Rig::open().await;
    rig.create("orders").await;
    let topic = rig
        .sns("CreateTopic", &[("Name", "events")])
        .await
        .expect_tag("TopicArn")
        .to_string();
    rig.sns(
        "Subscribe",
        &[
            ("TopicArn", &topic),
            ("Protocol", "sqs"),
            ("Endpoint", "arn:aws:sqs:queen-1:000000000000:orders"),
        ],
    )
    .await;

    let answer = rig
        .sns(
            "PublishBatch",
            &[
                ("TopicArn", &topic),
                ("PublishBatchRequestEntries.member.1.Id", "good"),
                ("PublishBatchRequestEntries.member.1.Message", "first"),
                ("PublishBatchRequestEntries.member.2.Id", "bad"),
                ("PublishBatchRequestEntries.member.2.Message", ""),
            ],
        )
        .await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert!(answer.body.contains("<Successful>"), "{}", answer.body);
    assert!(answer.body.contains("<Failed>"), "{}", answer.body);
    assert!(answer.body.contains("<member>"), "{}", answer.body);
    // Both entries are reported, each under its client-supplied `Id` — which is
    // the whole contract of a batch answer.
    assert!(answer.body.contains("<Id>good</Id>"), "{}", answer.body);
    assert!(answer.body.contains("<Id>bad</Id>"), "{}", answer.body);
    assert!(
        !answer.body.contains("<MessageId></MessageId>"),
        "{}",
        answer.body
    );
    assert_eq!(answer.expect_tag("Code"), "InvalidParameter");
    assert_eq!(answer.expect_tag("SenderFault"), "true");

    // The whole-batch refusal carries SNS's spelling of the code — no
    // `AWS.SimpleQueueService.` prefix, which is what an SNS client's error
    // mapping reads.
    let empty = rig.sns("PublishBatch", &[("TopicArn", &topic)]).await;
    assert_eq!(empty.status, 400, "{}", empty.body);
    assert_eq!(empty.expect_tag("Code"), "EmptyBatchRequest");
    assert!(
        empty
            .body
            .contains("xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\""),
        "{}",
        empty.body
    );
}

/// A publish to a topic nobody created is SNS's 404, in both codecs — the
/// status is what an SDK's retry classifier reads before it reads anything else.
#[tokio::test]
async fn a_publish_to_a_missing_topic_is_a_404_in_both_codecs() {
    let rig = Rig::open().await;
    let absent = "arn:aws:sns:queen-1:000000000000:absent";

    let query = rig
        .sns("Publish", &[("TopicArn", absent), ("Message", "m")])
        .await;
    assert_eq!(query.status, 404, "{}", query.body);
    assert_eq!(query.expect_tag("Code"), "NotFound");

    let json = rig
        .json_sns("Publish", json!({"TopicArn": absent, "Message": "m"}))
        .await;
    assert_eq!(json.status, 404, "{}", json.body);
    assert_eq!(json.json()["__type"], "com.amazonaws.sns#NotFound");
}

/// A filter policy set over the wire decides delivery over the wire: the policy
/// is a JSON document inside a form field, and a grammar this endpoint cannot
/// evaluate is refused at `Subscribe` rather than stored.
#[tokio::test]
async fn a_filter_policy_travels_through_the_query_protocol_and_decides_delivery() {
    let rig = Rig::open().await;
    let url = rig.create("orders").await.json()["QueueUrl"]
        .as_str()
        .expect("a URL")
        .to_string();
    let topic = rig
        .sns("CreateTopic", &[("Name", "events")])
        .await
        .expect_tag("TopicArn")
        .to_string();
    let endpoint = "arn:aws:sqs:queen-1:000000000000:orders";

    // A policy this engine cannot evaluate never reaches the store.
    let refused = rig
        .sns(
            "Subscribe",
            &[
                ("TopicArn", &topic),
                ("Protocol", "sqs"),
                ("Endpoint", endpoint),
                ("Attributes.entry.1.key", "FilterPolicy"),
                (
                    "Attributes.entry.1.value",
                    r#"{"k":[{"cidr":"10.0.0.0/8"}]}"#,
                ),
            ],
        )
        .await;
    assert_eq!(refused.status, 400, "{}", refused.body);
    assert_eq!(refused.expect_tag("Code"), "InvalidParameter");
    assert!(refused.body.contains("cidr"), "{}", refused.body);

    rig.sns(
        "Subscribe",
        &[
            ("TopicArn", &topic),
            ("Protocol", "sqs"),
            ("Endpoint", endpoint),
            ("Attributes.entry.1.key", "FilterPolicy"),
            ("Attributes.entry.1.value", r#"{"kind":["order"]}"#),
            ("Attributes.entry.2.key", "RawMessageDelivery"),
            ("Attributes.entry.2.value", "true"),
        ],
    )
    .await;

    for kind in ["refund", "order"] {
        let published = rig
            .sns(
                "Publish",
                &[
                    ("TopicArn", &topic),
                    ("Message", "hello"),
                    ("MessageAttributes.entry.1.Name", "kind"),
                    ("MessageAttributes.entry.1.Value.DataType", "String"),
                    ("MessageAttributes.entry.1.Value.StringValue", kind),
                ],
            )
            .await;
        assert_eq!(published.status, 200, "{}", published.body);
    }

    let received = rig
        .json(
            "ReceiveMessage",
            json!({"QueueUrl": url, "MaxNumberOfMessages": 10,
                   "MessageAttributeNames": ["All"]}),
        )
        .await;
    let messages = received.json()["Messages"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        messages.len(),
        1,
        "only the matching publish: {}",
        received.body
    );
    // ...and `RawMessageDelivery` means the body is the message itself, with the
    // publish's attributes forwarded as the queue's own.
    assert_eq!(messages[0]["Body"], "hello");
    assert_eq!(
        messages[0]["MessageAttributes"]["kind"]["StringValue"],
        "order"
    );
}

/// A Query request that names no `Version` at all is still answered in the
/// namespace of the API it addressed. The action set is the only evidence left,
/// and it is read where the action set lives — never in the codec.
#[tokio::test]
async fn an_sns_action_without_a_version_still_answers_in_sns() {
    let rig = Rig::open().await;
    let answer = rig
        .send(
            "POST",
            "/",
            "",
            vec![("content-type".to_string(), FORM_CONTENT_TYPE.to_string())],
            b"Action=ListTopics".to_vec(),
        )
        .await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert!(
        answer
            .body
            .contains("xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\""),
        "{}",
        answer.body
    );
    // ...and an SQS action with no version is still SQS's.
    let answer = rig
        .send(
            "POST",
            "/",
            "",
            vec![("content-type".to_string(), FORM_CONTENT_TYPE.to_string())],
            b"Action=ListQueues".to_vec(),
        )
        .await;
    assert!(
        answer
            .body
            .contains("xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\""),
        "{}",
        answer.body
    );
}
