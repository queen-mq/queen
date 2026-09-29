//! Copies the supervisor status document to the broker's key/value store so a
//! Laravel dashboard served by other hosts can show this supervisor.
//!
//! The wire format is `queen.supervisor.remote-status/v1`, defined by the
//! Laravel package's `RemoteStatusDocument` and read back by its
//! `RemoteStatusReader`. A key/value value is capped at 64 KiB while a status
//! document may reach 1 MiB, so the document is split across keys that share
//! one prefix:
//!
//! ```text
//! <key>/head        {format, write, chunks, bytes}
//! <key>/chunk/0000  {write, index, data}   data: base64 of a document slice
//! ```
//!
//! The head and every chunk travel in ONE batch call, which the broker applies
//! in one transaction. Every chunk carries the head's write id, so a chunk
//! left behind by an earlier, larger document is ignored by the reader.
//!
//! The local status.json stays the source of truth for this host. Publishing
//! is best effort by construction: a failure never stops or otherwise changes
//! supervision, is bounded by the request timeout the resolver budgets into
//! the heartbeat, and is reported once per failure streak.

use crate::{
    connection_endpoints, read_response_limited, validate_connection, validate_identifier,
    QueenConfig, MAX_CONFIG_BYTES, MAX_CONTROL_TTL_SECONDS, MAX_STATUS_BYTES,
};
use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use std::time::{Duration, Instant};

pub(crate) const FORMAT: &str = "queen.supervisor.remote-status/v1";
/// Raw bytes per chunk: 60 000 base64 characters, well under the 64 KiB value cap.
pub(crate) const CHUNK_BYTES: usize = 45_000;
/// The reader rejects a head that declares more chunks than this.
pub(crate) const MAX_CHUNKS: usize = 24;

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemoteStatusConfig {
    pub(crate) connection: QueenConfig,
    pub(crate) namespace: String,
    pub(crate) key: String,
    pub(crate) interval: u64,
    pub(crate) ttl: u64,
}

impl RemoteStatusConfig {
    pub(crate) fn validate(
        &self,
        heartbeat_timeout: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        validate_connection("remote_status", &self.connection)?;
        validate_identifier(&self.namespace, "remote_status namespace")?;
        validate_identifier(&self.key, "remote_status key")?;
        if self.interval == 0 || self.interval >= heartbeat_timeout {
            return Err(format!(
                "remote_status interval must be positive and shorter than heartbeat_timeout [{heartbeat_timeout}]"
            )
            .into());
        }
        if self.ttl < heartbeat_timeout || self.ttl > MAX_CONTROL_TTL_SECONDS {
            return Err(format!(
                "remote_status ttl must be at least heartbeat_timeout [{heartbeat_timeout}] and at most {MAX_CONTROL_TTL_SECONDS} seconds"
            )
            .into());
        }
        Ok(())
    }

    /// One synchronous publish per endpoint may run inside a control-loop
    /// iteration, so it belongs to the heartbeat budget.
    pub(crate) fn publish_budget(
        &self,
        http_timeout: u64,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        u64::try_from(connection_endpoints(&self.connection).len())?
            .checked_mul(http_timeout)
            .ok_or_else(|| "remote_status publish budget overflowed".into())
    }
}

pub(crate) struct RemoteStatusPublisher<'a> {
    config: &'a RemoteStatusConfig,
    last_published_at: Option<Instant>,
    last_published_state: Option<String>,
    failing: bool,
}

impl<'a> RemoteStatusPublisher<'a> {
    pub(crate) fn new(config: &'a RemoteStatusConfig) -> Self {
        Self {
            config,
            last_published_at: None,
            last_published_state: None,
            failing: false,
        }
    }

    /// Publish the document, at most once per interval unless the supervisor
    /// state changed (pausing, terminating, stopping) since the last publish.
    pub(crate) fn publish(
        &mut self,
        client: &reqwest::blocking::Client,
        document: &serde_json::Value,
    ) {
        let config = self.config;
        self.publish_at(Instant::now(), document, |body, operations| {
            send(client, &config.connection, body, operations)
        });
    }

    fn publish_at<F>(&mut self, now: Instant, document: &serde_json::Value, send: F)
    where
        F: FnOnce(&[u8], usize) -> Result<(), Box<dyn std::error::Error>>,
    {
        let state = document
            .get("state")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        if let Some(published_at) = self.last_published_at {
            if state == self.last_published_state
                && now.saturating_duration_since(published_at)
                    < Duration::from_secs(self.config.interval)
            {
                return;
            }
        }

        let result = request_body(
            document,
            &self.config.namespace,
            &self.config.key,
            self.config.ttl,
        )
        .and_then(|(body, operations)| send(&body, operations));
        // A failure is retried at the next interval, not on every loop iteration.
        self.last_published_at = Some(now);
        self.last_published_state = state;
        match result {
            Ok(()) if self.failing => {
                self.failing = false;
                eprintln!("Queen supervisor remote status publishing recovered.");
            }
            Ok(()) => {}
            Err(error) if !self.failing => {
                self.failing = true;
                eprintln!(
                    "Queen supervisor remote status publish failed: {error}. Local supervision continues; \
the remote dashboard will show this supervisor as stale."
                );
            }
            Err(_) => {}
        }
    }
}

/// Publish through the optional publisher; a no-op when remote status is disabled.
pub(crate) fn publish(
    publisher: &mut Option<RemoteStatusPublisher<'_>>,
    client: &reqwest::blocking::Client,
    document: &serde_json::Value,
) {
    if let Some(publisher) = publisher {
        publisher.publish(client, document);
    }
}

/// The `/api/v1/kv` request body and the number of operations it carries.
fn request_body(
    document: &serde_json::Value,
    namespace: &str,
    key: &str,
    ttl: u64,
) -> Result<(Vec<u8>, usize), Box<dyn std::error::Error>> {
    let mut document = document.clone();
    // The legacy nested pool map duplicates pool_status, which every current
    // reader prefers. Dropping it roughly halves what travels.
    if let Some(object) = document.as_object_mut() {
        object.remove("pools");
    }
    let operations = operations(&document, namespace, key, ttl, &new_write_id()?)?;
    let count = operations.len();
    let body = serde_json::to_vec(&serde_json::json!({ "operations": operations }))?;
    Ok((body, count))
}

/// KV put operations for one batch call: the head, then every chunk in order.
pub(crate) fn operations(
    document: &serde_json::Value,
    namespace: &str,
    key: &str,
    ttl: u64,
    write_id: &str,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let encoded = serde_json::to_vec(document)?;
    if encoded.len() as u64 > MAX_STATUS_BYTES {
        return Err(format!(
            "the status document is {} bytes, above the {MAX_STATUS_BYTES}-byte ceiling",
            encoded.len()
        )
        .into());
    }
    let chunks: Vec<&[u8]> = encoded.chunks(CHUNK_BYTES).collect();
    if chunks.is_empty() || chunks.len() > MAX_CHUNKS {
        return Err(format!("the status document needs {} chunks", chunks.len()).into());
    }

    let mut operations = Vec::with_capacity(chunks.len() + 1);
    operations.push(serde_json::json!({
        "op": "put",
        "ns": namespace,
        "key": format!("{key}/head"),
        "value": {
            "format": FORMAT,
            "write": write_id,
            "chunks": chunks.len(),
            "bytes": encoded.len(),
        },
        "ttlSeconds": ttl,
    }));
    for (index, chunk) in chunks.iter().enumerate() {
        operations.push(serde_json::json!({
            "op": "put",
            "ns": namespace,
            "key": format!("{key}/chunk/{index:04}"),
            "value": {
                "write": write_id,
                "index": index,
                "data": base64_encode(chunk),
            },
            "ttlSeconds": ttl,
        }));
    }
    Ok(operations)
}

/// Try each endpoint in order. Every put carries a fresh write id, so a write
/// replayed on the next endpoint after a lost response is harmless.
fn send(
    client: &reqwest::blocking::Client,
    connection: &QueenConfig,
    body: &[u8],
    operations: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut last_error = None;
    for endpoint in connection_endpoints(connection) {
        match send_to(client, connection, endpoint, body, operations) {
            Ok(()) => return Ok(()),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| "remote_status connection has no Queen URL".into()))
}

fn send_to(
    client: &reqwest::blocking::Client,
    connection: &QueenConfig,
    endpoint: &str,
    body: &[u8],
    operations: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut url = reqwest::Url::parse(endpoint)?;
    url.path_segments_mut()
        .map_err(|_| "Queen URL cannot be a base URL")?
        .extend(["api", "v1", "kv"]);
    let mut request = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body.to_vec());
    for (name, value) in &connection.headers {
        request = request.header(name, value);
    }
    if let Some(token) = &connection.bearer_token {
        request = request.bearer_auth(token);
    }
    let response = request.send()?.error_for_status()?;
    let response: serde_json::Value =
        serde_json::from_slice(&read_response_limited(response, MAX_CONFIG_BYTES)?)?;
    verify_applied(&response, operations)
}

/// A batch answers `{results: [...]}` index-aligned to its operations; the
/// write counts only when every put reports `applied: true`.
fn verify_applied(
    response: &serde_json::Value,
    operations: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let reason = |value: &serde_json::Value, fallback: &str| {
        value
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(fallback)
            .to_owned()
    };
    let Some(results) = response
        .get("results")
        .and_then(serde_json::Value::as_array)
        .filter(|results| results.len() == operations)
    else {
        return Err(format!(
            "the broker did not apply the write ({})",
            reason(response, "unexpected response")
        )
        .into());
    };
    for result in results {
        if result.get("applied") != Some(&serde_json::Value::Bool(true)) {
            return Err(format!(
                "the broker did not apply the write ({})",
                reason(result, "not applied")
            )
            .into());
        }
    }
    Ok(())
}

fn new_write_id() -> Result<String, Box<dyn std::error::Error>> {
    let mut bytes = [0_u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// Standard, padded base64: what PHP's strict `base64_decode` accepts.
fn base64_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let group = (u32::from(chunk[0]) << 16)
            | (u32::from(chunk.get(1).copied().unwrap_or(0)) << 8)
            | u32::from(chunk.get(2).copied().unwrap_or(0));
        let symbol = |shift: u32| char::from(BASE64_ALPHABET[(group >> shift) as usize & 63]);
        encoded.push(symbol(18));
        encoded.push(symbol(12));
        encoded.push(if chunk.len() > 1 { symbol(6) } else { '=' });
        encoded.push(if chunk.len() > 2 { symbol(0) } else { '=' });
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};
    use std::collections::HashMap;

    fn remote_config(interval: u64, ttl: u64) -> RemoteStatusConfig {
        RemoteStatusConfig {
            connection: QueenConfig {
                url: "http://127.0.0.1:6632".into(),
                urls: vec![
                    "http://127.0.0.1:6632".into(),
                    "http://127.0.0.1:6633".into(),
                ],
                bearer_token: None,
                headers: HashMap::new(),
            },
            namespace: "queen-supervisor".into(),
            key: "orders".into(),
            interval,
            ttl,
        }
    }

    fn base64_decode(encoded: &str) -> Vec<u8> {
        let value = |symbol: u8| BASE64_ALPHABET.iter().position(|&c| c == symbol).unwrap() as u32;
        let mut decoded = Vec::new();
        for group in encoded.as_bytes().chunks(4) {
            let padding = group.iter().filter(|&&symbol| symbol == b'=').count();
            let bits = group
                .iter()
                .map(|&symbol| if symbol == b'=' { 0 } else { value(symbol) })
                .fold(0_u32, |bits, sextet| (bits << 6) | sextet);
            decoded.extend_from_slice(&bits.to_be_bytes()[1..4 - padding]);
        }
        decoded
    }

    /// Answer one request with `response` and hand back the raw request,
    /// including a body announced by Content-Length.
    fn serve_http_once(response: &str) -> (String, std::thread::JoinHandle<String>) {
        use std::io::Write;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let response = response.to_owned();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                let text = String::from_utf8_lossy(&request).into_owned();
                if let Some(end) = text.find("\r\n\r\n") {
                    let length = text[..end]
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|value| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            stream.write_all(response.as_bytes()).unwrap();
            String::from_utf8(request).unwrap()
        });
        (format!("http://{address}"), handle)
    }

    fn http_response(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    #[test]
    fn a_publish_is_one_authenticated_kv_batch_the_reader_can_reassemble() {
        let (endpoint, server) = serve_http_once(&http_response(
            "200 OK",
            r#"{"results":[{"applied":true},{"applied":true}]}"#,
        ));
        let mut config = remote_config(3, 600);
        config.connection.urls = vec![endpoint];
        config.connection.bearer_token = Some("write-secret".into());
        config.connection.headers = HashMap::from([("X-Queen-Key".into(), "header-secret".into())]);
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let mut publisher = RemoteStatusPublisher::new(&config);
        let document = serde_json::json!({"state": "running", "pools": {}, "pool_status": []});

        publisher.publish(&client, &document);

        assert!(!publisher.failing);
        let request = server.join().unwrap();
        let (head, body) = request.split_once("\r\n\r\n").unwrap();
        let head = head.to_ascii_lowercase();
        assert!(head.starts_with("post /api/v1/kv http/1.1"), "{head}");
        assert!(
            head.contains("authorization: bearer write-secret"),
            "{head}"
        );
        assert!(head.contains("x-queen-key: header-secret"), "{head}");
        assert!(head.contains("content-type: application/json"), "{head}");
        let body: serde_json::Value = serde_json::from_str(body).unwrap();
        let operations = body["operations"].as_array().unwrap();
        assert_eq!(operations.len(), 2);
        assert_eq!(operations[0]["key"], "orders/head");
        assert_eq!(operations[0]["value"]["format"], FORMAT);
        assert_eq!(
            operations[1]["value"]["write"],
            operations[0]["value"]["write"]
        );
        let published: serde_json::Value = serde_json::from_slice(&base64_decode(
            operations[1]["value"]["data"].as_str().unwrap(),
        ))
        .unwrap();
        assert_eq!(
            published,
            serde_json::json!({"state": "running", "pool_status": []})
        );
    }

    #[test]
    fn a_rejected_publish_is_reported_without_failing_supervision() {
        let (endpoint, server) = serve_http_once(&http_response(
            "503 Service Unavailable",
            r#"{"error":"unavailable"}"#,
        ));
        let mut config = remote_config(3, 600);
        config.connection.urls = vec![endpoint];
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let mut publisher = RemoteStatusPublisher::new(&config);

        publisher.publish(&client, &serde_json::json!({"state": "running"}));

        server.join().unwrap();
        assert!(publisher.failing);
    }

    #[test]
    fn base64_matches_the_standard_padded_alphabet() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64_encode(&[0xfb, 0xff, 0xbf]), "+/+/");
    }

    #[test]
    fn write_ids_are_distinct_lowercase_hex_of_the_readers_width() {
        let first = new_write_id().unwrap();
        let second = new_write_id().unwrap();
        assert_eq!(first.len(), 32);
        assert!(first
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')));
        assert_ne!(first, second);
    }

    #[test]
    fn a_small_document_is_one_head_and_one_chunk() {
        let document = serde_json::json!({"state": "running", "path": "a/b", "name": "città"});
        let operations = operations(
            &document,
            "queen-supervisor",
            "orders",
            600,
            "0123456789abcdef0123456789abcdef",
        )
        .unwrap();

        assert_eq!(operations.len(), 2);
        let encoded = serde_json::to_vec(&document).unwrap();
        assert_eq!(
            operations[0],
            serde_json::json!({
                "op": "put",
                "ns": "queen-supervisor",
                "key": "orders/head",
                "value": {
                    "format": FORMAT,
                    "write": "0123456789abcdef0123456789abcdef",
                    "chunks": 1,
                    "bytes": encoded.len(),
                },
                "ttlSeconds": 600,
            })
        );
        assert_eq!(operations[1]["key"], "orders/chunk/0000");
        assert_eq!(operations[1]["ns"], "queen-supervisor");
        assert_eq!(operations[1]["ttlSeconds"], 600);
        assert_eq!(
            operations[1]["value"]["write"],
            "0123456789abcdef0123456789abcdef"
        );
        assert_eq!(operations[1]["value"]["index"], 0);
        let data = operations[1]["value"]["data"].as_str().unwrap();
        assert_eq!(base64_decode(data), encoded);
    }

    #[test]
    fn a_large_document_splits_into_ordered_chunks_that_reassemble() {
        let document = serde_json::json!({"state": "running", "padding": "x".repeat(100_000)});
        let operations = operations(
            &document,
            "ns",
            "orders",
            600,
            "0123456789abcdef0123456789abcdef",
        )
        .unwrap();
        let encoded = serde_json::to_vec(&document).unwrap();

        assert_eq!(operations[0]["value"]["chunks"], 3);
        assert_eq!(operations[0]["value"]["bytes"], encoded.len());
        assert_eq!(operations.len(), 4);
        let mut reassembled = Vec::new();
        for (index, operation) in operations[1..].iter().enumerate() {
            assert_eq!(operation["key"], format!("orders/chunk/{index:04}"));
            assert_eq!(operation["value"]["index"], index);
            let chunk = base64_decode(operation["value"]["data"].as_str().unwrap());
            assert!(chunk.len() <= CHUNK_BYTES);
            reassembled.extend(chunk);
        }
        assert_eq!(reassembled, encoded);
    }

    #[test]
    fn a_document_above_the_status_ceiling_is_refused() {
        let document = serde_json::json!({"padding": "x".repeat(MAX_STATUS_BYTES as usize)});
        let error = operations(
            &document,
            "ns",
            "orders",
            600,
            "0123456789abcdef0123456789abcdef",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("ceiling"), "{error}");
    }

    #[test]
    fn the_legacy_pool_map_does_not_travel() {
        let document =
            serde_json::json!({"state": "running", "pools": {"default": {}}, "pool_status": []});
        let (body, count) = request_body(&document, "ns", "orders", 600).unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(count, 2);
        let data = body["operations"][1]["value"]["data"].as_str().unwrap();
        let published: serde_json::Value = serde_json::from_slice(&base64_decode(data)).unwrap();
        assert_eq!(
            published,
            serde_json::json!({"state": "running", "pool_status": []})
        );
    }

    #[test]
    fn every_put_must_be_applied() {
        let applied = serde_json::json!({"results": [{"applied": true}, {"applied": true}]});
        assert!(verify_applied(&applied, 2).is_ok());

        let short = verify_applied(&applied, 3).unwrap_err().to_string();
        assert!(short.contains("unexpected response"), "{short}");

        let verdict = serde_json::json!({"ok": false, "reason": "kv_precondition"});
        let refused = verify_applied(&verdict, 2).unwrap_err().to_string();
        assert!(refused.contains("kv_precondition"), "{refused}");

        let partial = serde_json::json!({"results": [{"applied": true}, {"applied": false, "reason": "kv_value_too_large"}]});
        let rejected = verify_applied(&partial, 2).unwrap_err().to_string();
        assert!(rejected.contains("kv_value_too_large"), "{rejected}");
    }

    #[test]
    fn validation_mirrors_the_laravel_resolver() {
        assert!(remote_config(3, 600).validate(24).is_ok());
        assert!(remote_config(0, 600).validate(24).is_err());
        assert!(remote_config(24, 600).validate(24).is_err());
        assert!(remote_config(3, 23).validate(24).is_err());
        assert!(remote_config(3, MAX_CONTROL_TTL_SECONDS + 1)
            .validate(24)
            .is_err());

        let mut blank_key = remote_config(3, 600);
        blank_key.key = String::new();
        assert!(blank_key.validate(24).is_err());

        let mut credentials_in_url = remote_config(3, 600);
        credentials_in_url.connection.urls = vec!["http://user:secret@127.0.0.1:6632".into()];
        assert!(credentials_in_url.validate(24).is_err());
    }

    #[test]
    fn the_publish_budget_is_one_timeout_per_endpoint() {
        assert_eq!(remote_config(3, 600).publish_budget(5).unwrap(), 10);
        assert!(remote_config(3, 600).publish_budget(u64::MAX).is_err());
    }

    #[test]
    fn publishing_is_throttled_to_the_interval_unless_the_state_changes() {
        let config = remote_config(3, 600);
        let mut publisher = RemoteStatusPublisher::new(&config);
        let sent = Cell::new(0);
        let send = |_: &[u8], _: usize| {
            sent.set(sent.get() + 1);
            Ok(())
        };
        let start = Instant::now();
        let running = serde_json::json!({"state": "running"});

        publisher.publish_at(start, &running, send);
        publisher.publish_at(start + Duration::from_secs(1), &running, send);
        assert_eq!(sent.get(), 1);

        publisher.publish_at(
            start + Duration::from_secs(2),
            &serde_json::json!({"state": "paused"}),
            send,
        );
        assert_eq!(sent.get(), 2);

        publisher.publish_at(
            start + Duration::from_secs(4),
            &serde_json::json!({"state": "paused"}),
            send,
        );
        assert_eq!(sent.get(), 2);
        publisher.publish_at(
            start + Duration::from_secs(5),
            &serde_json::json!({"state": "paused"}),
            send,
        );
        assert_eq!(sent.get(), 3);
    }

    #[test]
    fn a_failed_publish_waits_for_the_next_interval_and_then_recovers() {
        let config = remote_config(3, 600);
        let mut publisher = RemoteStatusPublisher::new(&config);
        let outcomes = RefCell::new(vec![
            Ok(()),
            Err::<(), Box<dyn std::error::Error>>("broker down".into()),
        ]);
        let attempts = Cell::new(0);
        let send = |_: &[u8], _: usize| {
            attempts.set(attempts.get() + 1);
            outcomes.borrow_mut().pop().unwrap()
        };
        let start = Instant::now();
        let running = serde_json::json!({"state": "running"});

        publisher.publish_at(start, &running, send);
        assert!(publisher.failing);
        publisher.publish_at(start + Duration::from_secs(1), &running, send);
        assert_eq!(attempts.get(), 1);

        publisher.publish_at(start + Duration::from_secs(3), &running, send);
        assert_eq!(attempts.get(), 2);
        assert!(!publisher.failing);
    }
}
