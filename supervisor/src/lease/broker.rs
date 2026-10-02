//! The broker side of lease renewal: the worker's client settings, checked
//! once when it connects, and one renewal request across its endpoints.

use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Read;
use std::time::Duration;

const MAX_RESPONSE_BYTES: u64 = 64 * 1024;

/// The client settings a worker sends; no Debug, since it holds its token.
#[derive(Deserialize)]
pub(super) struct ClientConfig {
    pub(super) urls: Vec<String>,
    #[serde(rename = "bearerToken", default)]
    bearer_token: Option<String>,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(rename = "timeoutMillis")]
    pub(super) timeout_millis: u64,
}

pub(super) struct Broker {
    client: reqwest::blocking::Client,
    urls: Vec<reqwest::Url>,
    headers: HeaderMap,
    timeout: Duration,
}

impl Broker {
    /// Refuse settings that could only fail later, while the worker can
    /// still fall back to its own helper.
    pub(super) fn new(
        client: reqwest::blocking::Client,
        config: &ClientConfig,
    ) -> Result<Self, String> {
        let urls = config
            .urls
            .iter()
            .map(|url| {
                reqwest::Url::parse(url)
                    .ok()
                    .filter(|url| {
                        matches!(url.scheme(), "http" | "https") && !url.cannot_be_a_base()
                    })
                    .ok_or_else(|| "invalid broker URL".to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut headers = HeaderMap::new();
        for (name, value) in &config.headers {
            let name =
                HeaderName::from_bytes(name.as_bytes()).map_err(|_| "invalid header name")?;
            if name == CONTENT_TYPE || (name == AUTHORIZATION && config.bearer_token.is_some()) {
                continue;
            }
            let value = HeaderValue::from_str(value).map_err(|_| "invalid header value")?;
            headers.insert(name, value);
        }
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(token) = &config.bearer_token {
            let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| "invalid bearer token")?;
            value.set_sensitive(true);
            headers.insert(AUTHORIZATION, value);
        }
        Ok(Self {
            client,
            urls,
            headers,
            timeout: Duration::from_millis(config.timeout_millis),
        })
    }

    /// One renewal, trying each endpoint in turn; a definitive answer from
    /// one endpoint is final.
    pub(super) fn renew(&self, lease_id: &str, seconds: u64) -> Result<(), String> {
        let (status, body) = self.post(
            &["api", "v1", "lease", lease_id, "extend"],
            serde_json::json!({ "seconds": seconds }).to_string(),
            "lease renewal",
            true,
        )?;
        renewed(status, &body)
    }

    /// One transaction. It moves to the next endpoint only when this one
    /// could not be reached: once a request may have reached a broker, its
    /// outcome is unknown, and sending it again could push its copies twice.
    pub(super) fn transaction(&self, body: &serde_json::Value) -> Result<(), String> {
        let (status, answer) = self.post(
            &["api", "v1", "transaction"],
            body.to_string(),
            "the transaction",
            false,
        )?;
        committed(status, &answer)
    }

    /// The first definitive answer. An endpoint that cannot be reached passes
    /// the request to the next; so does one that times out, fails to answer
    /// or answers with a server error, when `resend` allows sending the
    /// request again.
    fn post(
        &self,
        path: &[&str],
        body: String,
        what: &str,
        resend: bool,
    ) -> Result<(reqwest::StatusCode, Vec<u8>), String> {
        let mut last = String::from("no broker URL");
        for base in &self.urls {
            let mut url = base.clone();
            // Checked in new(): every URL can be a base.
            if let Ok(mut segments) = url.path_segments_mut() {
                segments.pop_if_empty().extend(path);
            }
            let response = self
                .client
                .post(url)
                .timeout(self.timeout)
                .headers(self.headers.clone())
                .body(body.clone())
                .send();
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    let unreached = error.is_connect();
                    last = describe(error, what);
                    if unreached || resend {
                        continue;
                    }
                    return Err(last);
                }
            };
            let status = response.status();
            let mut answer = Vec::new();
            if let Err(error) = response.take(MAX_RESPONSE_BYTES).read_to_end(&mut answer) {
                last = format!("reading the answer failed: {}", error.kind());
                if resend {
                    continue;
                }
                return Err(last);
            }
            if status.is_server_error() && resend {
                last = format!("{what} answered {status}");
                continue;
            }
            return Ok((status, answer));
        }
        Err(last)
    }
}

/// The cause without the URL, which would crowd out the bounded diagnostic.
fn describe(error: reqwest::Error, what: &str) -> String {
    if error.is_timeout() {
        format!("{what} timed out")
    } else if error.is_connect() {
        "the broker connection failed".into()
    } else {
        error.without_url().to_string()
    }
}

/// Only `success: true` proves the broker applied every operation; the
/// transaction is all or nothing.
pub(super) fn committed(status: reqwest::StatusCode, body: &[u8]) -> Result<(), String> {
    let answer: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| format!("the transaction answered {status}"))?;
    if status.is_success()
        && answer.get("success").and_then(serde_json::Value::as_bool) == Some(true)
    {
        return Ok(());
    }
    Err(answer
        .get("error")
        .and_then(serde_json::Value::as_str)
        .map_or_else(
            || format!("the broker refused the transaction ({status})"),
            str::to_owned,
        ))
}

/// Only an affected-row count proves the broker still owned and extended the
/// lease, as in the PHP client's `renew`.
pub(super) fn renewed(status: reqwest::StatusCode, body: &[u8]) -> Result<(), String> {
    let answer: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| format!("lease renewal answered {status}"))?;
    let expiry = answer
        .get("newExpiresAt")
        .or_else(|| answer.get("expiresAt"))
        .or_else(|| answer.get("lease_expires_at"));
    let valid_expiry = expiry.is_none_or(|value| value.as_str().is_some_and(is_rfc3339));
    let success = status.is_success()
        && answer.get("success").and_then(serde_json::Value::as_bool) == Some(true)
        && answer
            .get("renewed")
            .and_then(serde_json::Value::as_i64)
            .is_some_and(|rows| rows > 0)
        && valid_expiry;
    if success {
        return Ok(());
    }
    Err(answer
        .get("error")
        .and_then(serde_json::Value::as_str)
        .map_or_else(
            || format!("the broker refused the lease renewal ({status})"),
            str::to_owned,
        ))
}

fn is_rfc3339(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 20
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[10] == b'T'
        && (value.ends_with('Z')
            || value
                .get(19..)
                .is_some_and(|zone| zone.contains(['+', '-'])))
}

/// A local HTTP server answering each request with the next scripted answer,
/// the last one forever, and logging `"<request line> | <authorization> | <body>"`.
#[cfg(test)]
pub(super) mod test_server {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::sync::{Arc, Mutex};

    pub(crate) type Log = Arc<Mutex<Vec<String>>>;

    pub(crate) fn start(answers: Vec<(u16, &'static str)>) -> (String, Log) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&log);
        std::thread::spawn(move || {
            for (index, stream) in listener.incoming().enumerate() {
                let Ok(mut stream) = stream else { continue };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request = String::new();
                if reader.read_line(&mut request).is_err() {
                    continue;
                }
                let (mut length, mut authorization) = (0, String::new());
                loop {
                    let mut header = String::new();
                    if reader.read_line(&mut header).is_err() || header.trim().is_empty() {
                        break;
                    }
                    let lower = header.to_ascii_lowercase();
                    if let Some(value) = lower.strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap_or(0);
                    }
                    if lower.starts_with("authorization:") {
                        authorization = header.trim().to_owned();
                    }
                }
                let mut body = vec![0; length];
                let _ = reader.read_exact(&mut body);
                seen.lock().unwrap().push(format!(
                    "{} | {authorization} | {}",
                    request.trim(),
                    String::from_utf8_lossy(&body)
                ));
                let (status, answer) = answers[index.min(answers.len() - 1)];
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                    answer.len()
                );
            }
        });
        (format!("http://{address}"), log)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) const RENEWED: &str =
        r#"{"success":true,"renewed":1,"newExpiresAt":"2026-09-30T10:00:00Z"}"#;

    fn config(urls: Vec<String>) -> ClientConfig {
        ClientConfig {
            urls,
            bearer_token: Some("secret".into()),
            headers: HashMap::from([
                ("Authorization".into(), "Basic stale".into()),
                ("X-Tenant".into(), "a".into()),
            ]),
            timeout_millis: 1000,
        }
    }

    fn broker(urls: Vec<String>) -> Broker {
        Broker::new(reqwest::blocking::Client::new(), &config(urls)).unwrap()
    }

    #[test]
    fn only_an_affected_row_count_and_a_valid_expiry_prove_a_renewal() {
        let ok = reqwest::StatusCode::OK;
        assert!(renewed(ok, RENEWED.as_bytes()).is_ok());
        assert!(renewed(ok, br#"{"success":true,"renewed":1}"#).is_ok());
        assert!(renewed(
            ok,
            br#"{"success":true,"renewed":1,"expiresAt":"2026-09-30T10:00:00+02:00"}"#
        )
        .is_ok());
        assert!(renewed(ok, br#"{"success":true,"renewed":0}"#).is_err());
        assert!(renewed(ok, br#"{"success":true}"#).is_err());
        assert!(renewed(ok, br#"{"success":true,"renewed":1,"newExpiresAt":"soon"}"#).is_err());
        assert_eq!(
            renewed(
                ok,
                br#"{"success":false,"renewed":1,"error":"lease not found"}"#
            ),
            Err("lease not found".to_owned())
        );
        assert!(renewed(reqwest::StatusCode::NOT_FOUND, b"<html>").is_err());
    }

    #[test]
    fn a_multibyte_expiry_is_refused_without_a_panic() {
        assert!(!is_rfc3339("2026-09-30T10:00:0\u{e9}000"));
        assert!(renewed(
            reqwest::StatusCode::OK,
            "{\"success\":true,\"renewed\":1,\"newExpiresAt\":\"2026-09-30T10:00:0\u{e9}000\"}"
                .as_bytes()
        )
        .is_err());
    }

    #[test]
    fn a_renewal_carries_the_workers_token_and_escapes_the_lease_id() {
        let (url, log) = test_server::start(vec![(200, RENEWED)]);
        assert_eq!(broker(vec![url]).renew("lease/1", 30), Ok(()));
        assert_eq!(
            log.lock().unwrap()[0],
            r#"POST /api/v1/lease/lease%2F1/extend HTTP/1.1 | authorization: Bearer secret | {"seconds":30}"#
        );
    }

    #[test]
    fn a_transaction_carries_the_workers_token_and_only_success_commits_it() {
        let (url, log) = test_server::start(vec![
            (200, r#"{"success":true,"results":[]}"#),
            (
                200,
                r#"{"success":false,"error":"ack_rejected","results":[]}"#,
            ),
        ]);
        let body = serde_json::json!({"operations": [], "requiredLeases": ["l"]});
        assert_eq!(broker(vec![url.clone()]).transaction(&body), Ok(()));
        assert_eq!(
            log.lock().unwrap()[0],
            format!("POST /api/v1/transaction HTTP/1.1 | authorization: Bearer secret | {body}")
        );
        assert_eq!(
            broker(vec![url]).transaction(&body),
            Err("ack_rejected".to_owned())
        );

        // Unreachable: the next endpoint. Reached, outcome unknown: never again.
        let (unavailable, unavailable_log) = test_server::start(vec![(503, "{}")]);
        let (healthy, healthy_log) = test_server::start(vec![(200, r#"{"success":true}"#)]);
        let refused = "http://127.0.0.1:9".to_owned();
        assert_eq!(
            broker(vec![refused, healthy.clone()]).transaction(&body),
            Ok(())
        );
        assert!(broker(vec![unavailable, healthy])
            .transaction(&body)
            .is_err());
        assert_eq!(unavailable_log.lock().unwrap().len(), 1);
        assert_eq!(
            healthy_log.lock().unwrap().len(),
            1,
            "a transaction was sent twice"
        );

        let ok = reqwest::StatusCode::OK;
        assert!(committed(ok, br#"{"success":true}"#).is_ok());
        assert!(committed(ok, br#"{"results":[]}"#).is_err());
        assert!(committed(reqwest::StatusCode::BAD_REQUEST, br#"{"success":true}"#).is_err());
    }

    #[test]
    fn a_failed_endpoint_moves_on_and_a_definitive_answer_is_final() {
        let (unavailable, _) = test_server::start(vec![(503, "{}")]);
        let (healthy, log) = test_server::start(vec![(200, RENEWED)]);
        let refused = "http://127.0.0.1:9".to_owned();
        assert_eq!(
            broker(vec![refused, unavailable, healthy.clone()]).renew("l", 30),
            Ok(())
        );
        assert_eq!(log.lock().unwrap().len(), 1);

        let (refusing, _) = test_server::start(vec![(
            200,
            r#"{"success":false,"error":"lease not found"}"#,
        )]);
        assert_eq!(
            broker(vec![refusing, healthy]).renew("l", 30),
            Err("lease not found".to_owned())
        );
        assert_eq!(
            log.lock().unwrap().len(),
            1,
            "a definitive refusal was retried elsewhere"
        );
    }

    #[test]
    fn settings_that_can_only_fail_later_are_refused_at_once() {
        let client = reqwest::blocking::Client::new();
        let mut invalid = config(vec!["mailto:queen@example.com".into()]);
        assert!(Broker::new(client.clone(), &invalid).is_err());
        invalid = config(vec!["http://queen:6632".into()]);
        invalid.headers.insert("X-Bad".into(), "line\nbreak".into());
        assert!(Broker::new(client, &invalid).is_err());
    }
}
