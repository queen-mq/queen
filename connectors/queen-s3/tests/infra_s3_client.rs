//! The S3 client, over a socket.
//!
//! A local axum server plays the bucket and RECORDS every request, so the
//! assertions are on the bytes that left the process: the addressing style, the
//! `Host` and `Authorization` headers, the `Content-MD5`, the SSE headers, the
//! multipart sequence, and what is and is not retried.
//!
//! The signing arithmetic is pinned by the AWS vectors in `src/s3/sigv4.rs`;
//! nothing here re-checks it. What these tests cover is everything BETWEEN the
//! signature and the wire, which is where a client is usually wrong.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{OriginalUri, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;

use queen_s3::config::Sse;
use queen_s3::s3::{ObjectStore, S3Client, S3Config};
use queen_s3::types::SinkError;

// ---------------------------------------------------------------------------
// A recording bucket
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    path: String,
    query: String,
    headers: Vec<(String, String)>,
    body: Bytes,
}

impl Recorded {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

#[derive(Debug, Clone)]
struct Canned {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: String,
}

impl Canned {
    fn ok() -> Canned {
        Canned {
            status: 200,
            headers: Vec::new(),
            body: String::new(),
        }
    }

    fn etag(etag: &str) -> Canned {
        Canned {
            status: 200,
            headers: vec![("etag", format!("\"{etag}\""))],
            body: String::new(),
        }
    }

    fn xml(body: &str) -> Canned {
        Canned {
            status: 200,
            headers: Vec::new(),
            body: body.to_string(),
        }
    }

    fn status(status: u16) -> Canned {
        Canned {
            status,
            headers: Vec::new(),
            body: format!("<Error><Code>{status}</Code></Error>"),
        }
    }
}

#[derive(Default)]
struct MockState {
    seen: Mutex<Vec<Recorded>>,
    answers: Mutex<VecDeque<Canned>>,
}

struct Bucket {
    port: u16,
    state: Arc<MockState>,
}

impl Bucket {
    async fn start(answers: Vec<Canned>) -> Bucket {
        let state = Arc::new(MockState {
            seen: Mutex::new(Vec::new()),
            answers: Mutex::new(answers.into_iter().collect()),
        });
        let app = Router::new().fallback(handle).with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Bucket { port, state }
    }

    fn seen(&self) -> Vec<Recorded> {
        self.state.seen.lock().unwrap().clone()
    }

    fn only(&self) -> Recorded {
        let seen = self.seen();
        assert_eq!(seen.len(), 1, "expected one request, saw {}", seen.len());
        seen.into_iter().next().unwrap()
    }

    /// A path-style client — the versitygw/MinIO shape, which needs no DNS
    /// trickery because the bucket is a path segment.
    fn path_style(&self, sse: Sse) -> S3Client {
        S3Client::new(S3Config {
            endpoint: format!("http://127.0.0.1:{}", self.port),
            region: "eu-central-1".to_string(),
            bucket: "lake".to_string(),
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            path_style: true,
            sse,
            retry_base_ms: 1,
            request_timeout: Duration::from_secs(10),
            ..S3Config::default()
        })
        .unwrap()
    }

    /// A virtual-host client, reached by pointing `lake.s3.test` at the
    /// listener: the whole point is that the URL and the `Host` header carry the
    /// bucket as a DNS label, which is what AWS itself takes.
    fn virtual_host(&self, sse: Sse) -> S3Client {
        let addr: std::net::SocketAddr = format!("127.0.0.1:{}", self.port).parse().unwrap();
        let http = reqwest::Client::builder()
            .resolve("lake.s3.test", addr)
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        S3Client::with_http(
            S3Config {
                endpoint: format!("http://s3.test:{}", self.port),
                region: "eu-central-1".to_string(),
                bucket: "lake".to_string(),
                access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                path_style: false,
                sse,
                retry_base_ms: 1,
                ..S3Config::default()
            },
            http,
        )
        .unwrap()
    }
}

async fn handle(
    State(state): State<Arc<MockState>>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    state.seen.lock().unwrap().push(Recorded {
        method: method.to_string(),
        path: uri.path().to_string(),
        query: uri.query().unwrap_or("").to_string(),
        headers: headers
            .iter()
            .map(|(n, v)| {
                (
                    n.as_str().to_string(),
                    v.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect(),
        body,
    });
    let canned = state.answers.lock().unwrap().pop_front().unwrap_or(Canned {
        status: 500,
        headers: Vec::new(),
        body: "the mock ran out of answers".to_string(),
    });
    let mut resp = (StatusCode::from_u16(canned.status).unwrap(), canned.body).into_response();
    for (name, value) in canned.headers {
        resp.headers_mut().insert(name, value.parse().unwrap());
    }
    resp
}

fn md5_hex(body: &[u8]) -> String {
    use md5::Digest;
    hex::encode(md5::Md5::digest(body))
}

fn md5_b64(body: &[u8]) -> String {
    use base64::Engine;
    use md5::Digest;
    base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(body))
}

fn sha256_hex(body: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(body))
}

// ---------------------------------------------------------------------------
// The single PUT
// ---------------------------------------------------------------------------

const KEY: &str = "queen/queue=orders/dt=2026-09-04/hour=10/w-0000000001-1-2.jsonl.zst";

#[tokio::test]
async fn a_path_style_put_addresses_the_bucket_as_a_path_segment() {
    let body = Bytes::from_static(b"{\"a\":1}\n");
    let bucket = Bucket::start(vec![Canned::etag(&md5_hex(&body))]).await;
    let out = bucket
        .path_style(Sse::Off)
        .put(KEY, body.clone(), "application/x-ndjson")
        .await
        .unwrap();
    assert_eq!(out.etag, md5_hex(&body));

    let req = bucket.only();
    assert_eq!(req.method, "PUT");
    // The `=` of a Hive component is percent-encoded ON THE WIRE — which is
    // what boto3 and every Spark writer send for exactly these keys, and what
    // the canonical request signs. The service decodes it back to the key the
    // layout named.
    assert_eq!(
        req.path,
        "/lake/queen/queue%3Dorders/dt%3D2026-09-04/hour%3D10/w-0000000001-1-2.jsonl.zst"
    );
    assert_eq!(req.query, "");
    assert_eq!(req.body, body);
    assert_eq!(
        req.header("host"),
        Some(&format!("127.0.0.1:{}", bucket.port)[..])
    );
    assert_eq!(req.header("content-type"), Some("application/x-ndjson"));
    assert_eq!(req.header("content-md5"), Some(&md5_b64(&body)[..]));
    assert_eq!(
        req.header("x-amz-content-sha256"),
        Some(&sha256_hex(&body)[..])
    );
    let date = req.header("x-amz-date").unwrap();
    assert_eq!(date.len(), 16, "{date}");
    let authz = req.header("authorization").unwrap();
    assert!(
        authz.starts_with(&format!(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/{}/eu-central-1/s3/aws4_request, ",
            &date[..8]
        )),
        "{authz}"
    );
    // The signed set is exactly the headers that were sent, in order.
    assert!(
        authz.contains(
            "SignedHeaders=content-md5;content-type;host;x-amz-content-sha256;x-amz-date, "
        ),
        "{authz}"
    );
    assert!(!req.header("host").unwrap().starts_with("lake."));
}

#[tokio::test]
async fn a_virtual_host_put_addresses_the_bucket_as_a_dns_label() {
    let body = Bytes::from_static(b"x");
    let bucket = Bucket::start(vec![Canned::etag(&md5_hex(&body))]).await;
    bucket
        .virtual_host(Sse::Off)
        .put("queen/a.jsonl", body, "application/x-ndjson")
        .await
        .unwrap();

    let req = bucket.only();
    assert_eq!(req.path, "/queen/a.jsonl", "no bucket in the path");
    assert_eq!(
        req.header("host"),
        Some(&format!("lake.s3.test:{}", bucket.port)[..]),
        "the bucket is the first DNS label, and it is a SIGNED header"
    );
    let authz = req.header("authorization").unwrap();
    assert!(authz.contains("host;"), "{authz}");
}

#[tokio::test]
async fn sse_headers_are_sent_exactly_as_configured() {
    let body = Bytes::from_static(b"x");

    let bucket = Bucket::start(vec![Canned::etag(&md5_hex(&body))]).await;
    bucket
        .path_style(Sse::Aes256)
        .put("k", body.clone(), "text/plain")
        .await
        .unwrap();
    let req = bucket.only();
    assert_eq!(req.header("x-amz-server-side-encryption"), Some("AES256"));
    assert_eq!(
        req.header("x-amz-server-side-encryption-aws-kms-key-id"),
        None
    );
    assert!(
        req.header("authorization")
            .unwrap()
            .contains("x-amz-server-side-encryption,"),
        "an SSE header that is not SIGNED is one a proxy can strip"
    );

    let bucket = Bucket::start(vec![Canned::etag(&md5_hex(&body))]).await;
    bucket
        .path_style(Sse::Kms {
            key_id: Some("arn:aws:kms:eu-central-1:1234:key/beefcafe".to_string()),
        })
        .put("k", body.clone(), "text/plain")
        .await
        .unwrap();
    let req = bucket.only();
    assert_eq!(req.header("x-amz-server-side-encryption"), Some("aws:kms"));
    assert_eq!(
        req.header("x-amz-server-side-encryption-aws-kms-key-id"),
        Some("arn:aws:kms:eu-central-1:1234:key/beefcafe")
    );

    let bucket = Bucket::start(vec![Canned::etag(&md5_hex(&body))]).await;
    bucket
        .path_style(Sse::Off)
        .put("k", body, "text/plain")
        .await
        .unwrap();
    assert_eq!(bucket.only().header("x-amz-server-side-encryption"), None);
}

#[tokio::test]
async fn a_wrong_etag_is_an_integrity_failure_and_an_opaque_one_is_not() {
    let body = Bytes::from_static(b"hello");

    let bucket = Bucket::start(vec![Canned::etag("00000000000000000000000000000000")]).await;
    let err = bucket
        .path_style(Sse::Off)
        .put("k", body.clone(), "text/plain")
        .await
        .unwrap_err();
    assert!(matches!(err, SinkError::Integrity(_)), "{err:?}");

    // SSE-KMS: the ETag is deliberately NOT the MD5, so there is nothing to
    // compare and the upload is not failed for it.
    let bucket = Bucket::start(vec![Canned::etag("some-opaque-kms-etag")]).await;
    let out = bucket
        .path_style(Sse::Kms { key_id: None })
        .put("k", body, "text/plain")
        .await
        .unwrap();
    assert_eq!(out.etag, "some-opaque-kms-etag");
}

// ---------------------------------------------------------------------------
// Multipart
// ---------------------------------------------------------------------------

/// A client whose threshold and part size make a small body a multipart upload.
fn multipart_client(bucket: &Bucket) -> S3Client {
    S3Client::new(S3Config {
        endpoint: format!("http://127.0.0.1:{}", bucket.port),
        region: "eu-central-1".to_string(),
        bucket: "lake".to_string(),
        access_key: "AKIA".to_string(),
        secret_key: "secret".to_string(),
        path_style: true,
        multipart_threshold: 8,
        part_size: 4,
        retry_base_ms: 1,
        ..S3Config::default()
    })
    .unwrap()
}

#[tokio::test]
async fn a_body_over_the_threshold_goes_up_in_parts_and_completes() {
    let bucket = Bucket::start(vec![
        Canned::xml("<InitiateMultipartUploadResult><UploadId>up-42</UploadId></InitiateMultipartUploadResult>"),
        Canned::etag("aaa"),
        Canned::etag("bbb"),
        Canned::etag("ccc"),
        Canned::xml("<CompleteMultipartUploadResult><ETag>\"deadbeef-3\"</ETag></CompleteMultipartUploadResult>"),
    ])
    .await;

    // Ten bytes, four to a part: three parts, the last one short.
    let body = Bytes::from_static(b"0123456789");
    let parts_seen = Arc::new(Mutex::new(Vec::new()));
    let sink = parts_seen.clone();
    let mut client = multipart_client(&bucket);
    client.on_mid_upload(Arc::new(move |done| sink.lock().unwrap().push(done)));

    let out = client.put("k", body, "application/x-ndjson").await.unwrap();
    assert_eq!(out.etag, "deadbeef-3");

    let seen = bucket.seen();
    assert_eq!(seen.len(), 5);

    assert_eq!(seen[0].method, "POST");
    assert_eq!(seen[0].path, "/lake/k");
    assert_eq!(seen[0].query, "uploads=");
    assert_eq!(seen[0].header("content-type"), Some("application/x-ndjson"));

    for (i, expected) in ["0123", "4567", "89"].iter().enumerate() {
        let req = &seen[1 + i];
        assert_eq!(req.method, "PUT");
        assert_eq!(
            req.query,
            format!("partNumber={}&uploadId=up-42", i + 1),
            "the query is canonical (sorted) and signed"
        );
        assert_eq!(req.body, Bytes::from_static(expected.as_bytes()));
        assert!(
            req.header("authorization").unwrap().contains("Signature="),
            "every part is signed on its own"
        );
    }

    let complete = &seen[4];
    assert_eq!(complete.method, "POST");
    assert_eq!(complete.query, "uploadId=up-42");
    let xml = String::from_utf8_lossy(&complete.body);
    assert!(xml.starts_with("<CompleteMultipartUpload>"), "{xml}");
    assert!(
        xml.contains("<PartNumber>1</PartNumber><ETag>\"aaa\"</ETag>"),
        "{xml}"
    );
    assert!(
        xml.contains("<PartNumber>3</PartNumber><ETag>\"ccc\"</ETag>"),
        "{xml}"
    );

    // The crash hook fires BETWEEN parts, never before the first.
    assert_eq!(*parts_seen.lock().unwrap(), vec![1, 2]);
}

#[tokio::test]
async fn a_completed_upload_whose_part_count_disagrees_is_an_integrity_failure() {
    let bucket = Bucket::start(vec![
        Canned::xml("<InitiateMultipartUploadResult><UploadId>up-1</UploadId></InitiateMultipartUploadResult>"),
        Canned::etag("aaa"),
        Canned::etag("bbb"),
        Canned::etag("ccc"),
        Canned::xml("<CompleteMultipartUploadResult><ETag>\"deadbeef-2\"</ETag></CompleteMultipartUploadResult>"),
    ])
    .await;
    let err = multipart_client(&bucket)
        .put("k", Bytes::from_static(b"0123456789"), "text/plain")
        .await
        .unwrap_err();
    assert!(matches!(err, SinkError::Integrity(_)), "{err:?}");
}

#[tokio::test]
async fn a_two_hundred_carrying_an_error_document_is_not_a_success() {
    let bucket = Bucket::start(vec![
        Canned::xml("<InitiateMultipartUploadResult><UploadId>up-1</UploadId></InitiateMultipartUploadResult>"),
        Canned::etag("aaa"),
        Canned::etag("bbb"),
        Canned::etag("ccc"),
        // S3 really does answer 200 with an error body here.
        Canned::xml("<Error><Code>InternalError</Code></Error>"),
        Canned::ok(), // the abort
    ])
    .await;
    let err = multipart_client(&bucket)
        .put("k", Bytes::from_static(b"0123456789"), "text/plain")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("InternalError"), "{err:?}");
    let seen = bucket.seen();
    assert_eq!(
        seen.last().unwrap().method,
        "DELETE",
        "the upload is aborted"
    );
    assert_eq!(seen.last().unwrap().query, "uploadId=up-1");
}

#[tokio::test]
async fn a_failed_part_aborts_the_upload_rather_than_stranding_it() {
    let mut answers = vec![Canned::xml(
        "<InitiateMultipartUploadResult><UploadId>up-9</UploadId></InitiateMultipartUploadResult>",
    )];
    // Every attempt at part 1 fails, non-retriably.
    answers.push(Canned::status(403));
    answers.push(Canned::ok()); // the abort
    let bucket = Bucket::start(answers).await;

    let err = multipart_client(&bucket)
        .put("k", Bytes::from_static(b"0123456789"), "text/plain")
        .await
        .unwrap_err();
    assert!(
        matches!(err, SinkError::Status { code: 403, .. }),
        "{err:?}"
    );
    let seen = bucket.seen();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[2].method, "DELETE");
    assert_eq!(seen[2].query, "uploadId=up-9");
}

// ---------------------------------------------------------------------------
// Retries — plan §6.7
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_slow_down_and_a_five_oh_three_are_retried_until_they_are_not() {
    let body = Bytes::from_static(b"x");
    let bucket = Bucket::start(vec![
        Canned::status(503),
        Canned::status(500),
        Canned::etag(&md5_hex(&body)),
    ])
    .await;
    bucket
        .path_style(Sse::Off)
        .put("k", body, "text/plain")
        .await
        .unwrap();
    assert_eq!(bucket.seen().len(), 3, "two failures, then the success");
}

#[tokio::test]
async fn a_four_oh_three_is_not_retried() {
    let bucket = Bucket::start(vec![Canned::status(403), Canned::etag("x")]).await;
    let err = bucket
        .path_style(Sse::Off)
        .put("k", Bytes::from_static(b"x"), "text/plain")
        .await
        .unwrap_err();
    assert!(
        matches!(err, SinkError::Status { code: 403, .. }),
        "{err:?}"
    );
    assert_eq!(bucket.seen().len(), 1, "a refusal is not a hiccup");
}

#[tokio::test]
async fn attempts_are_bounded_and_the_last_failure_is_the_one_returned() {
    let bucket = Bucket::start((0..10).map(|_| Canned::status(503)).collect()).await;
    let client = S3Client::new(S3Config {
        endpoint: format!("http://127.0.0.1:{}", bucket.port),
        bucket: "lake".to_string(),
        path_style: true,
        max_attempts: 3,
        retry_base_ms: 1,
        ..S3Config::default()
    })
    .unwrap();
    let err = client
        .put("k", Bytes::from_static(b"x"), "text/plain")
        .await
        .unwrap_err();
    assert!(
        matches!(err, SinkError::Status { code: 503, .. }),
        "{err:?}"
    );
    assert_eq!(bucket.seen().len(), 3);
}

#[tokio::test]
async fn a_retry_after_is_honoured_as_a_floor() {
    let body = Bytes::from_static(b"x");
    let bucket = Bucket::start(vec![
        Canned {
            status: 429,
            headers: vec![("retry-after", "1".to_string())],
            body: "<Error><Code>SlowDown</Code></Error>".to_string(),
        },
        Canned::etag(&md5_hex(&body)),
    ])
    .await;
    let started = std::time::Instant::now();
    bucket
        .path_style(Sse::Off)
        .put("k", body, "text/plain")
        .await
        .unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "the service said one second and it was asked with retry_base_ms=1"
    );
    assert_eq!(bucket.seen().len(), 2);
}

// ---------------------------------------------------------------------------
// The read side
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_missing_object_is_an_answer_and_not_a_failure() {
    let bucket = Bucket::start(vec![
        Canned::status(404),
        Canned::status(404),
        Canned::status(404),
    ])
    .await;
    let client = bucket.path_style(Sse::Off);
    assert_eq!(client.head("nope").await.unwrap(), None);
    assert_eq!(client.get("nope").await.unwrap(), None);
    client.delete("nope").await.unwrap();
    assert_eq!(bucket.seen().len(), 3, "and none of the three is retried");
    assert_eq!(bucket.seen()[0].method, "HEAD");
    assert_eq!(bucket.seen()[1].method, "GET");
    assert_eq!(bucket.seen()[2].method, "DELETE");
}

#[tokio::test]
async fn head_and_get_report_what_the_object_is() {
    let bucket = Bucket::start(vec![
        Canned {
            status: 200,
            headers: vec![
                ("etag", "\"abc\"".to_string()),
                ("content-length", "1234".to_string()),
            ],
            body: String::new(),
        },
        Canned::xml("the bytes"),
    ])
    .await;
    let client = bucket.path_style(Sse::Off);
    let meta = client.head("k").await.unwrap().unwrap();
    assert_eq!(meta.etag, "abc", "the quotes are stripped");
    assert_eq!(meta.size, 1234);
    assert_eq!(
        client.get("k").await.unwrap().unwrap(),
        Bytes::from_static(b"the bytes")
    );
}

#[tokio::test]
async fn list_asks_v2_pages_by_start_after_and_decodes_its_keys() {
    let bucket = Bucket::start(vec![
        Canned::xml(
            "<ListBucketResult><Contents><Key>queen/_queen/orders/checkpoint/0000000020.json.zst</Key></Contents>\
             <Contents><Key>queen/_queen/orders/checkpoint/0000000040.json.zst</Key></Contents>\
             <IsTruncated>true</IsTruncated></ListBucketResult>",
        ),
        Canned::xml(
            "<ListBucketResult><Contents><Key>a&amp;b</Key></Contents>\
             <IsTruncated>false</IsTruncated></ListBucketResult>",
        ),
    ])
    .await;
    let client = bucket.path_style(Sse::Off);

    let page = client
        .list("queen/_queen/orders/checkpoint/", None, 2)
        .await
        .unwrap();
    assert_eq!(page.keys.len(), 2);
    assert_eq!(
        page.next.as_deref(),
        Some("queen/_queen/orders/checkpoint/0000000040.json.zst"),
        "the cursor is the last key, which is what start-after takes"
    );

    let page2 = client
        .list("queen/_queen/orders/checkpoint/", page.next.clone(), 2)
        .await
        .unwrap();
    assert_eq!(page2.keys, vec!["a&b"], "XML entities are decoded");
    assert_eq!(page2.next, None);

    let seen = bucket.seen();
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].path, "/lake", "a LIST addresses the BUCKET");
    assert_eq!(
        seen[0].query,
        "list-type=2&max-keys=2&prefix=queen%2F_queen%2Forders%2Fcheckpoint%2F"
    );
    assert!(
        seen[1]
            .query
            .contains("start-after=queen%2F_queen%2Forders%2Fcheckpoint%2F0000000040.json.zst"),
        "{}",
        seen[1].query
    );
    // The canonical query is sorted, and the signature covers it.
    assert!(
        seen[1].query.starts_with("list-type=2&max-keys=2&prefix="),
        "{}",
        seen[1].query
    );
}

#[tokio::test]
async fn a_virtual_host_list_addresses_the_root() {
    let bucket = Bucket::start(vec![Canned::xml(
        "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>",
    )])
    .await;
    let page = bucket
        .virtual_host(Sse::Off)
        .list("queen/", None, 10)
        .await
        .unwrap();
    assert!(page.keys.is_empty());
    let req = bucket.only();
    assert_eq!(req.path, "/");
    assert_eq!(
        req.header("host"),
        Some(&format!("lake.s3.test:{}", bucket.port)[..])
    );
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_request_is_counted_by_op_and_code() {
    let body = Bytes::from_static(b"x");
    let bucket = Bucket::start(vec![Canned::status(503), Canned::etag(&md5_hex(&body))]).await;
    let metrics = Arc::new(queen_s3::obs::Metrics::new());
    let client = bucket.path_style(Sse::Off).with_metrics(metrics.clone());
    client.put("k", body, "text/plain").await.unwrap();
    assert_eq!(
        metrics.counter(
            "queen_s3_s3_requests_total",
            &[("op", "put"), ("code", "503")]
        ),
        1
    );
    assert_eq!(
        metrics.counter(
            "queen_s3_s3_requests_total",
            &[("op", "put"), ("code", "200")]
        ),
        1
    );
    assert!(metrics
        .render()
        .contains("queen_s3_s3_requests_total{op=\"put\",code=\"200\"} 1"));
}
