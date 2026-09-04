//! The object store: the trait the sink writes through, the S3 client that
//! implements it over reqwest + SigV4, and an in-memory double.
//!
//! WHAT THIS DELIBERATELY DOES NOT DO. No conditional PUT (`If-None-Match`), no
//! LIST on the correctness path, no read-after-write assumption. The window
//! protocol of plan §4 makes a retried upload BYTE-IDENTICAL, so overwriting a
//! key with the same bytes is the whole idempotency story — which is what makes
//! this work on DigitalOcean Spaces, where conditional PUT is unverifiable, and
//! on any gateway whose LIST is eventually consistent (plan §12).
//!
//! WHAT IT DOES CHECK. Integrity, both ways S3 offers it:
//!
//! * a single PUT sends `Content-MD5` and the answer's `ETag` is compared with
//!   the MD5 that was computed — a truncated body is caught by the service AND
//!   by the client;
//! * a completed multipart upload's `ETag` carries a `-<parts>` suffix, and the
//!   part count is compared with the number of parts that were sent.
//!
//! Both degrade honestly rather than falsely: SSE-KMS makes the `ETag` an opaque
//! value that is NOT the MD5 (an AWS rule, not a bug), and a gateway may answer
//! a multipart `ETag` with no suffix at all. In both cases the shape is not the
//! one integrity is defined over, so the check is skipped and said so at debug —
//! never failed, which would make SSE-KMS unusable, and never silently passed
//! under a name that suggests it was verified.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;

use super::sigv4::{self, Credentials, SigningRequest};
use crate::config::Sse;
use crate::queen::BoxFuture;
use crate::types::SinkError;

pub type Result<T> = std::result::Result<T, SinkError>;

/// What a PUT produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOutcome {
    /// The service's `ETag`, quotes stripped. For a single PUT of an
    /// unencrypted object it is the MD5 in hex; for a multipart upload it is
    /// `<hex>-<parts>`; under SSE-KMS it is opaque.
    pub etag: String,
}

/// What a HEAD found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub size: u64,
    pub etag: String,
}

/// One page of a LIST.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Listing {
    pub keys: Vec<String>,
    /// The cursor to pass back as `after`. `None` when the listing is complete.
    pub next: Option<String>,
}

/// The bucket, as the sink uses it.
///
/// Five verbs and no more, which is also the bucket policy the deploy page asks
/// for (plan §6.9): `PutObject`, `GetObject`, `ListBucket` (prefix-scoped),
/// `AbortMultipartUpload`, and `DeleteObject` **on `_queen/*/checkpoint/`
/// only** — the sink never deletes a data object.
pub trait ObjectStore: Send + Sync {
    /// Write `body` at `key`, as a single PUT below the multipart threshold and
    /// as a multipart upload above it. Overwriting is expected and safe: the
    /// same window always produces the same bytes.
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: Bytes,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<PutOutcome>>;

    /// `None` for an object that is not there — a 404 is an answer here, not a
    /// failure.
    fn head<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<ObjectMeta>>>;

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Bytes>>>;

    /// One page of keys under `prefix`, after `after` (exclusive), at most
    /// `max`.
    fn list<'a>(
        &'a self,
        prefix: &'a str,
        after: Option<String>,
        max: u32,
    ) -> BoxFuture<'a, Result<Listing>>;

    /// Idempotent: deleting what is not there is a success.
    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<()>>;
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Bytes per multipart part. 16 MiB is comfortably above S3's 5 MiB minimum and
/// keeps a 128 MiB object to eight parts — few enough that the completion is
/// small, large enough that the per-part round trip is amortised.
pub const PART_SIZE: usize = 16 * 1024 * 1024;

/// The backoff ceiling of plan §6.7.
pub const MAX_BACKOFF_MS: u64 = 60_000;

#[derive(Debug, Clone)]
pub struct S3Config {
    /// `scheme://host[:port]`, no path.
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    /// `true` for versitygw/MinIO-shaped hosts, which address the bucket as the
    /// first path segment rather than as a DNS label.
    pub path_style: bool,
    pub sse: Sse,
    /// Objects at or below this go up as a single PUT.
    pub multipart_threshold: usize,
    /// Bytes per multipart part. S3 requires every part but the LAST to be at
    /// least 5 MiB, and [`S3Config::default`] is [`PART_SIZE`] — a smaller
    /// value is only ever useful against a gateway that does not enforce the
    /// minimum, i.e. a test.
    pub part_size: usize,
    /// Attempts per request, the first included. The driver's own loop is the
    /// "retry forever" of plan §6.7; this bound is what stops one call sitting
    /// on a queue's whole window budget.
    pub max_attempts: u32,
    /// First backoff step. Doubles, jittered, up to [`MAX_BACKOFF_MS`].
    pub retry_base_ms: u64,
    pub request_timeout: Duration,
}

impl Default for S3Config {
    fn default() -> S3Config {
        S3Config {
            endpoint: String::new(),
            region: "us-east-1".to_string(),
            bucket: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            path_style: false,
            sse: Sse::Off,
            multipart_threshold: 64 * 1024 * 1024,
            part_size: PART_SIZE,
            max_attempts: 6,
            retry_base_ms: 1_000,
            request_timeout: Duration::from_secs(120),
        }
    }
}

impl S3Config {
    /// The S3 half of the process configuration.
    pub fn from_config(cfg: &crate::config::Config) -> S3Config {
        S3Config {
            endpoint: cfg.endpoint.clone(),
            region: cfg.region.clone(),
            bucket: cfg.bucket.clone(),
            access_key: cfg.access_key.clone(),
            secret_key: cfg.secret_key.clone(),
            path_style: cfg.path_style,
            sse: cfg.sse.clone(),
            multipart_threshold: cfg.multipart_threshold_bytes(),
            ..S3Config::default()
        }
    }
}

/// The endpoint, split once at construction.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Endpoint {
    scheme: String,
    host: String,
    port: Option<u16>,
}

impl Endpoint {
    fn parse(raw: &str) -> std::result::Result<Endpoint, String> {
        let (scheme, rest) = match raw.split_once("://") {
            Some((s, r)) if s == "http" || s == "https" => (s.to_string(), r),
            _ => {
                return Err(format!(
                    "QUEEN_S3_ENDPOINT={raw} must start with http:// or https://"
                ))
            }
        };
        let authority = rest.trim_end_matches('/');
        if authority.is_empty() || authority.contains('/') {
            return Err(format!(
                "QUEEN_S3_ENDPOINT={raw} is scheme://host[:port] and nothing else"
            ));
        }
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) => {
                let port: u16 = p
                    .parse()
                    .map_err(|_| format!("QUEEN_S3_ENDPOINT={raw} has a port that is not one"))?;
                (h.to_string(), Some(port))
            }
            None => (authority.to_string(), None),
        };
        if host.is_empty() {
            return Err(format!("QUEEN_S3_ENDPOINT={raw} has no host"));
        }
        Ok(Endpoint { scheme, host, port })
    }

    /// The `Host` header, which is also the URL's authority — and, being a
    /// signed header, the one field a proxy in front must not rewrite.
    fn authority(&self, bucket: Option<&str>) -> String {
        let host = match bucket {
            Some(b) => format!("{b}.{}", self.host),
            None => self.host.clone(),
        };
        match self.port {
            Some(p) => format!("{host}:{p}"),
            None => host,
        }
    }
}

/// A hook the driver installs to implement `QUEEN_S3_CRASH_AT=mid_upload`: it is
/// called BETWEEN multipart parts, with the number of parts already uploaded.
/// The point of the crash matrix is that a process killed here leaves a real
/// stranded multipart upload, and the restart must still produce the identical
/// object.
pub type MidUploadHook = Arc<dyn Fn(usize) + Send + Sync>;

/// The S3 client.
pub struct S3Client {
    cfg: S3Config,
    endpoint: Endpoint,
    creds: Credentials,
    http: reqwest::Client,
    metrics: Option<Arc<crate::obs::Metrics>>,
    mid_upload: Option<MidUploadHook>,
}

impl S3Client {
    pub fn new(cfg: S3Config) -> std::result::Result<S3Client, String> {
        let http = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| format!("cannot build the S3 HTTP client: {e}"))?;
        S3Client::with_http(cfg, http)
    }

    /// [`S3Client::new`] on a caller's own `reqwest::Client`.
    ///
    /// Two reasons it is public: a process that opens several clients should
    /// share one connection pool, and a test that exercises VIRTUAL-HOST
    /// addressing needs `ClientBuilder::resolve` to point `bucket.host` at a
    /// loopback listener — there being no other way to reach a bucket-prefixed
    /// hostname that DNS has never heard of.
    pub fn with_http(
        cfg: S3Config,
        http: reqwest::Client,
    ) -> std::result::Result<S3Client, String> {
        let endpoint = Endpoint::parse(&cfg.endpoint)?;
        if cfg.bucket.is_empty() {
            return Err("QUEEN_S3_BUCKET is empty".to_string());
        }
        let creds = Credentials::new(cfg.access_key.clone(), cfg.secret_key.clone());
        Ok(S3Client {
            cfg,
            endpoint,
            creds,
            http,
            metrics: None,
            mid_upload: None,
        })
    }

    pub fn from_config(cfg: &crate::config::Config) -> std::result::Result<S3Client, String> {
        S3Client::new(S3Config::from_config(cfg))
    }

    /// Count every request in `queen_s3_s3_requests_total{op,code}`.
    pub fn with_metrics(mut self, metrics: Arc<crate::obs::Metrics>) -> S3Client {
        self.metrics = Some(metrics);
        self
    }

    /// Install the `mid_upload` fault hook. See [`MidUploadHook`].
    pub fn on_mid_upload(&mut self, hook: MidUploadHook) {
        self.mid_upload = Some(hook);
    }

    pub fn config(&self) -> &S3Config {
        &self.cfg
    }

    /// The path a request signs and addresses: the key under the bucket, with
    /// the bucket in front of it in path-style addressing.
    fn resource_path(&self, key: &str) -> String {
        let key = key.trim_start_matches('/');
        match self.cfg.path_style {
            true => format!("/{}/{key}", self.cfg.bucket),
            false => format!("/{key}"),
        }
    }

    /// The path of a bucket-level request (LIST).
    fn bucket_path(&self) -> String {
        match self.cfg.path_style {
            true => format!("/{}", self.cfg.bucket),
            false => "/".to_string(),
        }
    }

    fn host_header(&self) -> String {
        match self.cfg.path_style {
            true => self.endpoint.authority(None),
            false => self.endpoint.authority(Some(&self.cfg.bucket)),
        }
    }

    /// The `x-amz-server-side-encryption*` headers this configuration adds to
    /// an object-creating request (PUT, and CreateMultipartUpload — never the
    /// individual parts, which inherit the upload's).
    fn sse_headers(&self) -> Vec<(String, String)> {
        match &self.cfg.sse {
            Sse::Off => Vec::new(),
            Sse::Aes256 => vec![(
                "x-amz-server-side-encryption".to_string(),
                "AES256".to_string(),
            )],
            Sse::Kms { key_id } => {
                let mut out = vec![(
                    "x-amz-server-side-encryption".to_string(),
                    "aws:kms".to_string(),
                )];
                if let Some(id) = key_id {
                    out.push((
                        "x-amz-server-side-encryption-aws-kms-key-id".to_string(),
                        id.clone(),
                    ));
                }
                out
            }
        }
    }

    /// Build, sign and send one request, retrying what plan §6.7 says to retry.
    async fn call(&self, call: Call<'_>) -> Result<S3Response> {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let outcome = self.send_once(&call).await;
            let retry_after_ms = match &outcome {
                Err(SinkError::Status { retry_after_ms, .. }) => *retry_after_ms,
                _ => None,
            };
            let retriable = match &outcome {
                Ok(_) => false,
                Err(e) => e.is_retriable(),
            };
            if !retriable || attempt >= self.cfg.max_attempts {
                return outcome;
            }
            let wait = self.backoff_ms(attempt, retry_after_ms);
            tracing::debug!(
                target: "queen-s3",
                op = call.op,
                attempt,
                wait_ms = wait,
                "retrying an S3 request"
            );
            tokio::time::sleep(Duration::from_millis(wait)).await;
        }
    }

    /// Exponential with FULL jitter, floored at a tenth of the step so a retry
    /// storm cannot collapse into a tight loop, and never below a `Retry-After`
    /// the service named.
    fn backoff_ms(&self, attempt: u32, retry_after_ms: Option<i64>) -> u64 {
        let step = self
            .cfg
            .retry_base_ms
            .saturating_mul(1u64 << (attempt - 1).min(20))
            .min(MAX_BACKOFF_MS);
        use rand::Rng;
        let jittered = rand::thread_rng().gen_range(step / 10..=step.max(1));
        let asked = retry_after_ms.unwrap_or(0).max(0) as u64;
        jittered.max(asked).min(MAX_BACKOFF_MS)
    }

    async fn send_once(&self, call: &Call<'_>) -> Result<S3Response> {
        let body = call.body.clone().unwrap_or_default();
        let payload_hash = sigv4::sha256_hex(&body);
        let amz_date = sigv4::amz_date_now();
        let host = self.host_header();

        let mut headers: Vec<(String, String)> = vec![
            ("host".to_string(), host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        headers.extend(call.headers.iter().cloned());

        let signed = sigv4::sign(
            &self.creds,
            &self.cfg.region,
            &amz_date,
            &SigningRequest {
                method: call.method,
                path: call.path,
                query: call.query,
                headers: &headers,
                payload_hash: &payload_hash,
            },
        );

        // The URL carries the SIGNED forms of both the path and the query, so
        // the bytes on the wire and the bytes in the canonical request cannot
        // drift: one encoding, applied once, used twice.
        let mut url = format!(
            "{}://{host}{}",
            self.endpoint.scheme,
            sigv4::canonical_uri(call.path)
        );
        let query = sigv4::canonical_query(call.query);
        if !query.is_empty() {
            url.push('?');
            url.push_str(&query);
        }

        let method = reqwest::Method::from_bytes(call.method.as_bytes()).map_err(|e| {
            SinkError::Config(format!("{} is not an HTTP method: {e}", call.method))
        })?;
        let mut req = self.http.request(method, &url);
        for (name, value) in &headers {
            req = req.header(name.as_str(), value.as_str());
        }
        req = req.header(reqwest::header::AUTHORIZATION, &signed.authorization);
        if call.body.is_some() {
            req = req.body(body);
        }

        let started = std::time::Instant::now();
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                self.note(call.op, 0);
                return Err(SinkError::Transport(e.to_string()));
            }
        };
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        let retry_after_ms = headers
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<i64>().ok())
            .and_then(|s| s.checked_mul(1_000))
            .filter(|ms| *ms >= 0);
        let body = resp
            .bytes()
            .await
            .map_err(|e| SinkError::Transport(e.to_string()))?;
        self.note(call.op, status);
        tracing::debug!(
            target: "queen-s3",
            op = call.op,
            status,
            bytes = body.len(),
            ms = started.elapsed().as_millis() as u64,
            "s3 call"
        );
        if !(200..300).contains(&status) {
            return Err(SinkError::Status {
                code: status,
                body: String::from_utf8_lossy(&body).chars().take(2_000).collect(),
                retry_after_ms,
            });
        }
        Ok(S3Response {
            etag: header_str(&headers, "etag").as_deref().map(strip_quotes),
            content_length: header_str(&headers, "content-length")
                .and_then(|v| v.parse::<u64>().ok()),
            body,
        })
    }

    fn note(&self, op: &str, code: u16) {
        if let Some(m) = &self.metrics {
            m.s3_request(op, code);
        }
    }

    // ---- the write path --------------------------------------------------

    async fn put_single(&self, key: &str, body: Bytes, content_type: &str) -> Result<PutOutcome> {
        let md5 = md5_digest(&body);
        let mut headers = vec![
            ("content-type".to_string(), content_type.to_string()),
            ("content-md5".to_string(), base64_std(&md5)),
        ];
        headers.extend(self.sse_headers());
        let resp = self
            .call(Call {
                op: "put",
                method: "PUT",
                path: &self.resource_path(key),
                query: &[],
                headers: &headers,
                body: Some(body),
            })
            .await?;
        let etag = resp.etag.unwrap_or_default();
        verify_single_etag(key, &etag, &hex::encode(md5))?;
        Ok(PutOutcome { etag })
    }

    async fn put_multipart(
        &self,
        key: &str,
        body: Bytes,
        content_type: &str,
    ) -> Result<PutOutcome> {
        let path = self.resource_path(key);
        let mut headers = vec![("content-type".to_string(), content_type.to_string())];
        headers.extend(self.sse_headers());
        let created = self
            .call(Call {
                op: "multipart_create",
                method: "POST",
                path: &path,
                query: &[("uploads".to_string(), String::new())],
                headers: &headers,
                body: Some(Bytes::new()),
            })
            .await?;
        let xml = String::from_utf8_lossy(&created.body).into_owned();
        let Some(upload_id) = xml_tag(&xml, "UploadId") else {
            return Err(SinkError::Body(format!(
                "CreateMultipartUpload for {key} answered no UploadId"
            )));
        };

        match self.upload_parts(&path, &upload_id, &body, key).await {
            Ok(outcome) => Ok(outcome),
            Err(e) => {
                // Best effort, and deliberately not retried into the failure
                // path: a stranded upload costs storage until the bucket's
                // AbortIncompleteMultipartUpload lifecycle rule sweeps it, and
                // the lifecycle rule is what the deploy page asks for anyway.
                let _ = self
                    .call(Call {
                        op: "multipart_abort",
                        method: "DELETE",
                        path: &path,
                        query: &[("uploadId".to_string(), upload_id.clone())],
                        headers: &[],
                        body: None,
                    })
                    .await;
                Err(e)
            }
        }
    }

    async fn upload_parts(
        &self,
        path: &str,
        upload_id: &str,
        body: &Bytes,
        key: &str,
    ) -> Result<PutOutcome> {
        let part_size = self.cfg.part_size.max(1);
        let mut etags: Vec<(usize, String)> = Vec::new();
        let mut offset = 0usize;
        while offset < body.len() {
            if offset > 0 {
                if let Some(hook) = &self.mid_upload {
                    hook(etags.len());
                }
            }
            let end = (offset + part_size).min(body.len());
            let part_number = etags.len() + 1;
            let resp = self
                .call(Call {
                    op: "multipart_part",
                    method: "PUT",
                    path,
                    query: &[
                        ("partNumber".to_string(), part_number.to_string()),
                        ("uploadId".to_string(), upload_id.to_string()),
                    ],
                    headers: &[],
                    body: Some(body.slice(offset..end)),
                })
                .await?;
            let etag = resp.etag.ok_or_else(|| {
                SinkError::Body(format!("part {part_number} of {key} answered no ETag"))
            })?;
            etags.push((part_number, etag));
            offset = end;
        }

        let mut xml = String::from("<CompleteMultipartUpload>");
        for (n, etag) in &etags {
            xml.push_str(&format!(
                "<Part><PartNumber>{n}</PartNumber><ETag>\"{}\"</ETag></Part>",
                xml_escape(etag)
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");

        let resp = self
            .call(Call {
                op: "multipart_complete",
                method: "POST",
                path,
                query: &[("uploadId".to_string(), upload_id.to_string())],
                headers: &[("content-type".to_string(), "application/xml".to_string())],
                body: Some(Bytes::from(xml)),
            })
            .await?;
        // S3 can answer 200 with an ERROR document on completion — the one
        // status code in this API that does not mean what it says, and the
        // reason the body is read rather than the status trusted.
        let text = String::from_utf8_lossy(&resp.body).into_owned();
        if let Some(code) = xml_tag(&text, "Code") {
            return Err(SinkError::Status {
                code: 200,
                body: format!("CompleteMultipartUpload for {key} answered {code}: {text}"),
                retry_after_ms: None,
            });
        }
        let etag = xml_tag(&text, "ETag")
            .map(|e| strip_quotes(&e))
            .or(resp.etag)
            .unwrap_or_default();
        verify_multipart_etag(key, &etag, etags.len())?;
        Ok(PutOutcome { etag })
    }
}

/// One request, before signing.
struct Call<'a> {
    /// The metric label, and the word in the log line.
    op: &'static str,
    method: &'static str,
    /// Raw (unencoded) path, bucket included when path-style.
    path: &'a str,
    query: &'a [(String, String)],
    headers: &'a [(String, String)],
    /// `None` sends no body at all; `Some(empty)` sends an empty one, which is
    /// what a POST needs.
    body: Option<Bytes>,
}

struct S3Response {
    etag: Option<String>,
    content_length: Option<u64>,
    body: Bytes,
}

impl ObjectStore for S3Client {
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: Bytes,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<PutOutcome>> {
        Box::pin(async move {
            match body.len() > self.cfg.multipart_threshold {
                true => self.put_multipart(key, body, content_type).await,
                false => self.put_single(key, body, content_type).await,
            }
        })
    }

    fn head<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<ObjectMeta>>> {
        Box::pin(async move {
            let path = self.resource_path(key);
            match self
                .call(Call {
                    op: "head",
                    method: "HEAD",
                    path: &path,
                    query: &[],
                    headers: &[],
                    body: None,
                })
                .await
            {
                Ok(resp) => Ok(Some(ObjectMeta {
                    size: resp.content_length.unwrap_or(0),
                    etag: resp.etag.unwrap_or_default(),
                })),
                Err(SinkError::Status { code: 404, .. }) => Ok(None),
                Err(e) => Err(e),
            }
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Bytes>>> {
        Box::pin(async move {
            let path = self.resource_path(key);
            match self
                .call(Call {
                    op: "get",
                    method: "GET",
                    path: &path,
                    query: &[],
                    headers: &[],
                    body: None,
                })
                .await
            {
                Ok(resp) => Ok(Some(resp.body)),
                Err(SinkError::Status { code: 404, .. }) => Ok(None),
                Err(e) => Err(e),
            }
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        after: Option<String>,
        max: u32,
    ) -> BoxFuture<'a, Result<Listing>> {
        Box::pin(async move {
            // ListObjectsV2 paged by `start-after` rather than by
            // `continuation-token`: the cursor is then a plain key, which is
            // what this trait's `after` is, and every S3-compatible gateway
            // implements it. The listing is not on any correctness path
            // (plan §4.3), so the weaker cursor costs nothing.
            let mut query = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), prefix.to_string()),
                ("max-keys".to_string(), max.clamp(1, 1000).to_string()),
            ];
            if let Some(a) = &after {
                query.push(("start-after".to_string(), a.clone()));
            }
            let resp = self
                .call(Call {
                    op: "list",
                    method: "GET",
                    path: &self.bucket_path(),
                    query: &query,
                    headers: &[],
                    body: None,
                })
                .await?;
            let text = String::from_utf8_lossy(&resp.body).into_owned();
            let keys: Vec<String> = xml_tags(&text, "Key")
                .into_iter()
                .map(xml_unescape)
                .collect();
            let truncated = xml_tag(&text, "IsTruncated")
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            let next = match truncated {
                true => keys.last().cloned(),
                false => None,
            };
            Ok(Listing { keys, next })
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let path = self.resource_path(key);
            match self
                .call(Call {
                    op: "delete",
                    method: "DELETE",
                    path: &path,
                    query: &[],
                    headers: &[],
                    body: None,
                })
                .await
            {
                Ok(_) => Ok(()),
                Err(SinkError::Status { code: 404, .. }) => Ok(()),
                Err(e) => Err(e),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Integrity
// ---------------------------------------------------------------------------

/// Compare a single PUT's `ETag` with the MD5 that was sent.
///
/// Only when the shape is the one integrity is defined over: 32 hex characters.
/// Under SSE-KMS the `ETag` is deliberately NOT the MD5 (an AWS rule), and a
/// gateway may answer something else again — in both cases the check is skipped
/// and logged, never failed and never silently claimed.
pub fn verify_single_etag(key: &str, etag: &str, md5_hex: &str) -> Result<()> {
    let looks_like_md5 = etag.len() == 32 && etag.bytes().all(|b| b.is_ascii_hexdigit());
    if !looks_like_md5 {
        tracing::debug!(
            target: "queen-s3",
            key,
            etag,
            "ETag is not an MD5 (SSE-KMS or a gateway of its own): integrity not verified here"
        );
        return Ok(());
    }
    if !etag.eq_ignore_ascii_case(md5_hex) {
        return Err(SinkError::Integrity(format!(
            "{key}: the service returned ETag {etag} for a body whose MD5 is {md5_hex}"
        )));
    }
    Ok(())
}

/// Compare a completed multipart `ETag`'s `-<parts>` suffix with the number of
/// parts that were sent.
pub fn verify_multipart_etag(key: &str, etag: &str, parts: usize) -> Result<()> {
    let Some((_, suffix)) = etag.rsplit_once('-') else {
        tracing::debug!(
            target: "queen-s3",
            key,
            etag,
            "multipart ETag carries no part count: integrity not verified here"
        );
        return Ok(());
    };
    let Ok(reported) = suffix.parse::<usize>() else {
        tracing::debug!(target: "queen-s3", key, etag, "multipart ETag suffix is not a count");
        return Ok(());
    };
    if reported != parts {
        return Err(SinkError::Integrity(format!(
            "{key}: the service completed {reported} parts, {parts} were uploaded (ETag {etag})"
        )));
    }
    Ok(())
}

fn md5_digest(body: &[u8]) -> [u8; 16] {
    use md5::Digest;
    md5::Md5::digest(body).into()
}

fn base64_std(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn header_str(headers: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
    headers.get(name)?.to_str().ok().map(|v| v.to_string())
}

fn strip_quotes(v: &str) -> String {
    v.trim().trim_matches('"').to_string()
}

// ---------------------------------------------------------------------------
// The three lines of XML this client speaks
// ---------------------------------------------------------------------------
//
// No XML crate, deliberately: the whole surface is `<UploadId>`, `<ETag>`,
// `<Key>`, `<IsTruncated>` and `<Code>` — five element names read out of
// documents S3 wrote. A parser would be a dependency, an attack surface and a
// tree, for five `find`s.

/// The text of the FIRST `<tag>…</tag>`, or `None`.
pub fn xml_tag(body: &str, tag: &str) -> Option<String> {
    xml_tags(body, tag).into_iter().next()
}

/// The text of every `<tag>…</tag>`, in document order.
pub fn xml_tags(body: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else { break };
        out.push(after[..end].to_string());
        rest = &after[end + close.len()..];
    }
    out
}

/// The five predefined entities, which is all S3 emits in a key.
pub fn xml_unescape(s: String) -> String {
    if !s.contains('&') {
        return s;
    }
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

// ---------------------------------------------------------------------------
// The in-memory double
// ---------------------------------------------------------------------------

/// One PUT, as it happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutRecord {
    pub key: String,
    pub bytes: usize,
    pub content_type: String,
    /// MD5 hex — the same `ETag` a real single PUT answers.
    pub etag: String,
    /// SHA-256 hex of the body, which is what a manifest records: two PUTs of
    /// one window must agree here, and that is the crash matrix's assertion.
    pub sha256: String,
}

#[derive(Default)]
struct MemState {
    objects: std::collections::BTreeMap<String, (Bytes, String, String)>,
    puts: Vec<PutRecord>,
    fail_next: usize,
}

/// An in-memory [`ObjectStore`] with S3's own ETag semantics.
///
/// It is not a mock: a test asserts against the bytes it holds and the sequence
/// of PUTs it saw, which is exactly what the determinism and crash-matrix
/// arguments are about.
#[derive(Default)]
pub struct MemoryStore {
    state: Mutex<MemState>,
}

impl MemoryStore {
    pub fn new() -> MemoryStore {
        MemoryStore::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MemState> {
        self.state.lock().expect("MemoryStore lock")
    }

    /// Fail the next `n` operations with a retriable transport error.
    pub fn fail_next(&self, n: usize) {
        self.lock().fail_next = n;
    }

    /// Every PUT this store saw, in order, including overwrites of one key.
    pub fn puts(&self) -> Vec<PutRecord> {
        self.lock().puts.clone()
    }

    /// The keys it holds, sorted.
    pub fn keys(&self) -> Vec<String> {
        self.lock().objects.keys().cloned().collect()
    }

    /// One object's bytes, without going through the trait.
    pub fn bytes_of(&self, key: &str) -> Option<Bytes> {
        self.lock().objects.get(key).map(|(b, _, _)| b.clone())
    }

    pub fn len(&self) -> usize {
        self.lock().objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn take_failure(&self) -> Option<SinkError> {
        let mut st = self.lock();
        if st.fail_next == 0 {
            return None;
        }
        st.fail_next -= 1;
        Some(SinkError::Transport("injected object-store failure".into()))
    }
}

impl ObjectStore for MemoryStore {
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: Bytes,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<PutOutcome>> {
        Box::pin(async move {
            if let Some(e) = self.take_failure() {
                return Err(e);
            }
            let etag = hex::encode(md5_digest(&body));
            let record = PutRecord {
                key: key.to_string(),
                bytes: body.len(),
                content_type: content_type.to_string(),
                etag: etag.clone(),
                sha256: sigv4::sha256_hex(&body),
            };
            let mut st = self.lock();
            st.objects.insert(
                key.to_string(),
                (body, etag.clone(), content_type.to_string()),
            );
            st.puts.push(record);
            Ok(PutOutcome { etag })
        })
    }

    fn head<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<ObjectMeta>>> {
        Box::pin(async move {
            if let Some(e) = self.take_failure() {
                return Err(e);
            }
            Ok(self.lock().objects.get(key).map(|(b, etag, _)| ObjectMeta {
                size: b.len() as u64,
                etag: etag.clone(),
            }))
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Bytes>>> {
        Box::pin(async move {
            if let Some(e) = self.take_failure() {
                return Err(e);
            }
            Ok(self.lock().objects.get(key).map(|(b, _, _)| b.clone()))
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        after: Option<String>,
        max: u32,
    ) -> BoxFuture<'a, Result<Listing>> {
        Box::pin(async move {
            if let Some(e) = self.take_failure() {
                return Err(e);
            }
            let st = self.lock();
            let all: Vec<String> = st
                .objects
                .keys()
                .filter(|k| k.starts_with(prefix))
                .filter(|k| after.as_ref().is_none_or(|a| *k > a))
                .cloned()
                .collect();
            let max = max.clamp(1, 1000) as usize;
            let truncated = all.len() > max;
            let keys: Vec<String> = all.into_iter().take(max).collect();
            Ok(Listing {
                next: match truncated {
                    true => keys.last().cloned(),
                    false => None,
                },
                keys,
            })
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if let Some(e) = self.take_failure() {
                return Err(e);
            }
            self.lock().objects.remove(key);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(path_style: bool, sse: Sse) -> S3Client {
        S3Client::new(S3Config {
            endpoint: "https://s3.example.com".to_string(),
            region: "eu-central-1".to_string(),
            bucket: "lake".to_string(),
            access_key: "AKIA".to_string(),
            secret_key: "secret".to_string(),
            path_style,
            sse,
            ..S3Config::default()
        })
        .unwrap()
    }

    #[test]
    fn endpoints_split_into_scheme_host_and_port() {
        assert_eq!(
            Endpoint::parse("http://gw:7070").unwrap(),
            Endpoint {
                scheme: "http".into(),
                host: "gw".into(),
                port: Some(7070)
            }
        );
        assert_eq!(
            Endpoint::parse("https://s3.example.com/").unwrap(),
            Endpoint {
                scheme: "https".into(),
                host: "s3.example.com".into(),
                port: None
            }
        );
        for bad in ["gw:7070", "ftp://gw", "https://", "https://gw:notaport"] {
            assert!(Endpoint::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn addressing_decides_the_path_and_the_host() {
        let virtual_host = client(false, Sse::Off);
        assert_eq!(virtual_host.host_header(), "lake.s3.example.com");
        assert_eq!(
            virtual_host.resource_path("queen/a.jsonl"),
            "/queen/a.jsonl"
        );
        assert_eq!(virtual_host.bucket_path(), "/");

        let path_style = client(true, Sse::Off);
        assert_eq!(path_style.host_header(), "s3.example.com");
        assert_eq!(
            path_style.resource_path("queen/a.jsonl"),
            "/lake/queen/a.jsonl"
        );
        assert_eq!(path_style.bucket_path(), "/lake");
    }

    #[test]
    fn a_port_survives_into_the_host_header() {
        let c = S3Client::new(S3Config {
            endpoint: "http://gw:7070".to_string(),
            bucket: "lake".to_string(),
            path_style: true,
            ..S3Config::default()
        })
        .unwrap();
        assert_eq!(c.host_header(), "gw:7070");
        let v = S3Client::new(S3Config {
            endpoint: "http://gw:7070".to_string(),
            bucket: "lake".to_string(),
            path_style: false,
            ..S3Config::default()
        })
        .unwrap();
        assert_eq!(v.host_header(), "lake.gw:7070");
    }

    #[test]
    fn sse_headers_are_exactly_what_the_mode_asks_for() {
        assert!(client(false, Sse::Off).sse_headers().is_empty());
        assert_eq!(
            client(false, Sse::Aes256).sse_headers(),
            vec![(
                "x-amz-server-side-encryption".to_string(),
                "AES256".to_string()
            )]
        );
        let kms = client(
            false,
            Sse::Kms {
                key_id: Some("arn:key".to_string()),
            },
        )
        .sse_headers();
        assert_eq!(kms[0].1, "aws:kms");
        assert_eq!(kms[1].0, "x-amz-server-side-encryption-aws-kms-key-id");
        assert_eq!(kms[1].1, "arn:key");
        let kms_no_id = client(false, Sse::Kms { key_id: None }).sse_headers();
        assert_eq!(kms_no_id.len(), 1);
    }

    #[test]
    fn single_put_integrity_is_checked_only_when_the_etag_is_an_md5() {
        let md5 = "9a0364b9e99bb480dd25e1f0284c8555";
        assert!(verify_single_etag("k", md5, md5).is_ok());
        assert!(verify_single_etag("k", &md5.to_uppercase(), md5).is_ok());
        let err = verify_single_etag("k", "0123456789abcdef0123456789abcdef", md5).unwrap_err();
        assert!(matches!(err, SinkError::Integrity(_)), "{err:?}");
        // SSE-KMS and gateways of their own: not the shape, not verified here.
        assert!(verify_single_etag("k", "opaque-kms-value", md5).is_ok());
        assert!(verify_single_etag("k", "", md5).is_ok());
    }

    #[test]
    fn multipart_integrity_compares_the_part_count() {
        assert!(verify_multipart_etag("k", "abc-3", 3).is_ok());
        let err = verify_multipart_etag("k", "abc-2", 3).unwrap_err();
        assert!(matches!(err, SinkError::Integrity(_)), "{err:?}");
        // No suffix at all: not the shape, not verified.
        assert!(verify_multipart_etag("k", "abc", 3).is_ok());
        assert!(verify_multipart_etag("k", "abc-many", 3).is_ok());
    }

    #[test]
    fn the_xml_reader_finds_what_s3_writes_and_nothing_else() {
        let body = "<InitiateMultipartUploadResult><Bucket>lake</Bucket>\
                    <UploadId>abc123</UploadId></InitiateMultipartUploadResult>";
        assert_eq!(xml_tag(body, "UploadId").as_deref(), Some("abc123"));
        assert_eq!(xml_tag(body, "Nope"), None);
        let listing = "<ListBucketResult><Contents><Key>a/b</Key></Contents>\
                       <Contents><Key>a&amp;b</Key></Contents>\
                       <IsTruncated>true</IsTruncated></ListBucketResult>";
        assert_eq!(xml_tags(listing, "Key"), vec!["a/b", "a&amp;b"]);
        assert_eq!(xml_unescape("a&amp;b".to_string()), "a&b");
        assert_eq!(xml_unescape("plain".to_string()), "plain");
        assert_eq!(xml_tag(listing, "IsTruncated").as_deref(), Some("true"));
        // An unterminated element stops the scan rather than looping.
        assert_eq!(xml_tags("<Key>a", "Key"), Vec::<String>::new());
    }

    #[test]
    fn backoff_grows_jitters_and_stops_at_a_minute() {
        let c = client(false, Sse::Off);
        for attempt in 1..=12u32 {
            let ms = c.backoff_ms(attempt, None);
            assert!(ms <= MAX_BACKOFF_MS, "attempt {attempt} waited {ms}");
            assert!(ms > 0, "attempt {attempt} waited nothing");
        }
        // A Retry-After the service named is a FLOOR, never a ceiling.
        assert!(c.backoff_ms(1, Some(5_000)) >= 5_000);
        assert_eq!(c.backoff_ms(1, Some(600_000)), MAX_BACKOFF_MS);
    }

    #[tokio::test]
    async fn the_memory_store_answers_md5_etags_and_records_every_put() {
        let s = MemoryStore::new();
        let body = Bytes::from_static(b"hello");
        let out = s.put("a", body.clone(), "text/plain").await.unwrap();
        assert_eq!(out.etag, hex::encode(md5_digest(b"hello")));
        assert_eq!(s.get("a").await.unwrap().unwrap(), body);
        assert_eq!(
            s.head("a").await.unwrap(),
            Some(ObjectMeta {
                size: 5,
                etag: out.etag.clone()
            })
        );
        assert_eq!(s.head("nope").await.unwrap(), None);
        assert_eq!(s.get("nope").await.unwrap(), None);

        // A retry of the same window writes the same key with the same bytes:
        // two PUTs, one object, one sha256.
        s.put("a", body.clone(), "text/plain").await.unwrap();
        let puts = s.puts();
        assert_eq!(puts.len(), 2);
        assert_eq!(puts[0], puts[1]);
        assert_eq!(s.len(), 1);

        s.delete("a").await.unwrap();
        assert!(s.is_empty());
        s.delete("a").await.unwrap();
    }

    #[tokio::test]
    async fn the_memory_store_lists_by_prefix_and_pages() {
        let s = MemoryStore::new();
        for k in ["p/a", "p/b", "p/c", "q/d"] {
            s.put(k, Bytes::from_static(b"x"), "text/plain")
                .await
                .unwrap();
        }
        let page = s.list("p/", None, 2).await.unwrap();
        assert_eq!(page.keys, vec!["p/a", "p/b"]);
        assert_eq!(page.next.as_deref(), Some("p/b"));
        let page = s.list("p/", page.next, 2).await.unwrap();
        assert_eq!(page.keys, vec!["p/c"]);
        assert_eq!(page.next, None);
        assert_eq!(s.list("q/", None, 10).await.unwrap().keys, vec!["q/d"]);
    }

    #[tokio::test]
    async fn the_memory_store_injects_exactly_n_failures() {
        let s = MemoryStore::new();
        s.fail_next(2);
        assert!(s.put("a", Bytes::new(), "x").await.is_err());
        assert!(s.get("a").await.is_err());
        assert!(s.put("a", Bytes::new(), "x").await.is_ok());
    }
}
