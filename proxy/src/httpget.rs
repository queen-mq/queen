//! Minimal async HTTP/HTTPS GET for the JWKS fetch (RUSTFIX item 7).
//!
//! Self-contained on tokio + tokio-rustls (the `ring` provider) so the broker
//! needs no extra HTTP-client / TLS backend: this avoids reqwest's aws-lc-rs pull
//! and keeps the Docker build cmake-free. It handles Content-Length and chunked
//! response bodies; it follows no redirects (a JWKS URL is a direct endpoint) and
//! HTTPS verifies the chain against the bundled webpki root set.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Hard cap on a fetched response body. A real JWKS document is a few KB; 1 MiB is
/// generous headroom while bounding memory against a malicious/MITM'd endpoint that
/// streams an unbounded body (the per-request timeout only bounds *slow* responses).
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// GET `url` and parse the response body as JSON, bounded by `timeout`.
pub async fn get_json(url: &str, timeout: Duration) -> Result<serde_json::Value, String> {
    let bytes = tokio::time::timeout(timeout, fetch(url))
        .await
        .map_err(|_| format!("timeout after {}ms", timeout.as_millis()))??;
    serde_json::from_slice(&bytes).map_err(|e| format!("json parse: {e}"))
}

/// POST `url` with an `application/x-www-form-urlencoded` body built from
/// `params`, parsing the response as JSON. Same self-contained tokio-rustls
/// stack as `get_json`; used for the OAuth token exchange (oauth.rs). Additive
/// to this file — shares parse_url / tls_config / parse_response with `fetch`.
pub async fn post_form(
    url: &str,
    params: &[(&str, &str)],
    timeout: Duration,
) -> Result<serde_json::Value, String> {
    let body = encode_form(params);
    let headers = [
        ("Content-Type", "application/x-www-form-urlencoded"),
        ("Accept", "application/json"),
    ];
    let bytes = tokio::time::timeout(timeout, send("POST", url, &headers, Some(body.into_bytes())))
        .await
        .map_err(|_| format!("timeout after {}ms", timeout.as_millis()))??;
    serde_json::from_slice(&bytes).map_err(|e| format!("json parse: {e}"))
}

/// GET `url` with caller-supplied request headers (e.g. Authorization +
/// User-Agent for the GitHub API, which rejects UA-less requests), parsing the
/// response as JSON.
pub async fn get_json_with_headers(
    url: &str,
    headers: &[(&str, &str)],
    timeout: Duration,
) -> Result<serde_json::Value, String> {
    let bytes = tokio::time::timeout(timeout, send("GET", url, headers, None))
        .await
        .map_err(|_| format!("timeout after {}ms", timeout.as_millis()))??;
    serde_json::from_slice(&bytes).map_err(|e| format!("json parse: {e}"))
}

/// Percent-encode form params (application/x-www-form-urlencoded). Unreserved
/// chars pass through; everything else becomes %XX (space -> %20, so a literal
/// '+' is correctly encoded %2B). Accepted by the Google/GitHub token endpoints.
fn encode_form(params: &[(&str, &str)]) -> String {
    fn enc(s: &str, out: &mut String) {
        for &b in s.as_bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
    }
    let mut out = String::new();
    for (i, (k, v)) in params.iter().enumerate() {
        if i > 0 {
            out.push('&');
        }
        enc(k, &mut out);
        out.push('=');
        enc(v, &mut out);
    }
    out
}

/// Method-generic single request/response over a fresh (optionally TLS)
/// connection with `Connection: close`. `extra` headers are written verbatim
/// after Host; a default User-Agent is added only when the caller supplied none.
async fn send(
    method: &str,
    url: &str,
    extra: &[(&str, &str)],
    body: Option<Vec<u8>>,
) -> Result<Vec<u8>, String> {
    let (https, host, port, path) = parse_url(url)?;
    let addr = format!("{host}:{port}");
    let tcp = TcpStream::connect(&addr)
        .await
        .map_err(|e| format!("connect {addr}: {e}"))?;
    if https {
        let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config()));
        let dnsname = rustls::pki_types::ServerName::try_from(host.clone())
            .map_err(|_| format!("invalid host {host}"))?;
        let mut stream = connector
            .connect(dnsname, tcp)
            .await
            .map_err(|e| format!("tls handshake: {e}"))?;
        exchange_req(&mut stream, method, &host, &path, extra, body.as_deref()).await
    } else {
        let mut stream = tcp;
        exchange_req(&mut stream, method, &host, &path, extra, body.as_deref()).await
    }
}

async fn exchange_req<S>(
    stream: &mut S,
    method: &str,
    host: &str,
    path: &str,
    extra: &[(&str, &str)],
    body: Option<&[u8]>,
) -> Result<Vec<u8>, String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {host}\r\n");
    if !extra.iter().any(|(k, _)| k.eq_ignore_ascii_case("user-agent")) {
        req.push_str("User-Agent: queen-proxy\r\n");
    }
    for (k, v) in extra {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    if let Some(b) = body {
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
    }
    req.push_str("Connection: close\r\n\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("write: {e}"))?;
    if let Some(b) = body {
        stream.write_all(b).await.map_err(|e| format!("write body: {e}"))?;
    }
    stream.flush().await.ok();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = match stream.read(&mut chunk).await {
            Ok(n) => n,
            // A peer that closes the TCP connection without sending TLS
            // close_notify surfaces through rustls as UnexpectedEof. With
            // `Connection: close` the close IS the end of the message, and
            // Google's token endpoint closes exactly this way — treating it as
            // an error made every token exchange fail with
            // "google token exchange failed" while the body had already
            // arrived in full. Safe to accept because parse_response verifies
            // the body against Content-Length (and dechunk requires the
            // terminating zero chunk), so a genuinely truncated response is
            // still rejected, just further down and with a clearer message.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(format!("read: {e}")),
        };
        if n == 0 {
            break;
        }
        if buf.len() + n > MAX_RESPONSE_BYTES {
            return Err(format!("response exceeds {MAX_RESPONSE_BYTES}-byte cap"));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    parse_response(&buf)
}

async fn fetch(url: &str) -> Result<Vec<u8>, String> {
    let (https, host, port, path) = parse_url(url)?;
    let addr = format!("{host}:{port}");
    let tcp = TcpStream::connect(&addr)
        .await
        .map_err(|e| format!("connect {addr}: {e}"))?;
    let raw = if https {
        let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config()));
        let dnsname = rustls::pki_types::ServerName::try_from(host.clone())
            .map_err(|_| format!("invalid host {host}"))?;
        let mut stream = connector
            .connect(dnsname, tcp)
            .await
            .map_err(|e| format!("tls handshake: {e}"))?;
        exchange(&mut stream, &host, &path).await?
    } else {
        let mut stream = tcp;
        exchange(&mut stream, &host, &path).await?
    };
    parse_response(&raw)
}

fn parse_url(url: &str) -> Result<(bool, String, u16, String), String> {
    let (https, rest) = if let Some(r) = url.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("http://") {
        (false, r)
    } else {
        return Err("url must start with http:// or https://".into());
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse::<u16>().map_err(|_| "invalid port".to_string())?,
        ),
        None => (authority.to_string(), if https { 443 } else { 80 }),
    };
    if host.is_empty() {
        return Err("empty host".into());
    }
    Ok((https, host, port, path.to_string()))
}

async fn exchange<S>(stream: &mut S, host: &str, path: &str) -> Result<Vec<u8>, String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: queen\r\n\
         Accept: application/json\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("write: {e}"))?;
    stream.flush().await.ok();
    // Connection: close — read to EOF, but cap the total so a malicious/MITM'd
    // endpoint cannot stream an unbounded body and OOM the broker.
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = match stream.read(&mut chunk).await {
            Ok(n) => n,
            // Same close_notify tolerance as exchange_req — see the comment
            // there. This is the JWKS path: without it, a Google id_token
            // could never be verified even once the token exchange succeeded.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(format!("read: {e}")),
        };
        if n == 0 {
            break; // EOF (Connection: close)
        }
        if buf.len() + n > MAX_RESPONSE_BYTES {
            return Err(format!("response exceeds {MAX_RESPONSE_BYTES}-byte cap"));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Ok(buf)
}

fn parse_response(raw: &[u8]) -> Result<Vec<u8>, String> {
    let sep = find_subslice(raw, b"\r\n\r\n").ok_or("no header terminator")?;
    let head = String::from_utf8_lossy(&raw[..sep]);
    let body = &raw[sep + 4..];

    let mut lines = head.split("\r\n");
    let status = lines.next().unwrap_or("");
    let code = status
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse::<u16>().ok())
        .unwrap_or(0);
    if !(200..300).contains(&code) {
        return Err(format!("http status {code}"));
    }

    let mut chunked = false;
    let mut content_length: Option<usize> = None;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim();
            if k.eq_ignore_ascii_case("transfer-encoding")
                && v.to_ascii_lowercase().contains("chunked")
            {
                chunked = true;
            } else if k.eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse::<usize>().ok();
            }
        }
    }
    if chunked {
        // dechunk already rejects a truncated body: it needs the terminating
        // zero-size chunk and checks each declared chunk length.
        return dechunk(body);
    }
    // This is what makes tolerating an abrupt (close_notify-less) EOF safe in
    // the read loops above: a short body is a truncated response, not a valid
    // one, and it must not reach the JSON parser as if it were complete.
    // Transfer-Encoding wins over Content-Length per RFC 9112, hence the early
    // return above.
    match content_length {
        Some(len) if body.len() < len => Err(format!(
            "truncated body: Content-Length {len}, received {}",
            body.len()
        )),
        // A server may append nothing after the body; trim any surplus so the
        // caller sees exactly the declared message.
        Some(len) => Ok(body[..len].to_vec()),
        None => Ok(body.to_vec()),
    }
}

fn dechunk(mut body: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    loop {
        let nl = find_subslice(body, b"\r\n").ok_or("chunk: missing size line")?;
        let size_line = String::from_utf8_lossy(&body[..nl]);
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16).map_err(|_| "chunk: bad size".to_string())?;
        body = &body[nl + 2..];
        if size == 0 {
            break;
        }
        if body.len() < size {
            return Err("chunk: truncated body".into());
        }
        out.extend_from_slice(&body[..size]);
        body = &body[size..];
        if body.len() >= 2 {
            body = &body[2..]; // trailing CRLF after each chunk
        }
    }
    Ok(out)
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn tls_config() -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("rustls: safe default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn res(head: &str, body: &str) -> Vec<u8> {
        format!("{head}\r\n\r\n{body}").into_bytes()
    }

    #[test]
    fn content_length_exact_is_accepted() {
        let r = res("HTTP/1.1 200 OK\r\nContent-Length: 5", "hello");
        assert_eq!(parse_response(&r).unwrap(), b"hello");
    }

    #[test]
    fn content_length_short_is_rejected_as_truncated() {
        // The case tolerating a close_notify-less EOF could otherwise let
        // through: connection died mid-body.
        let r = res("HTTP/1.1 200 OK\r\nContent-Length: 11", "hello");
        let e = parse_response(&r).unwrap_err();
        assert!(e.contains("truncated body"), "unexpected error: {e}");
    }

    #[test]
    fn surplus_after_content_length_is_trimmed() {
        let r = res("HTTP/1.1 200 OK\r\nContent-Length: 5", "hellojunk");
        assert_eq!(parse_response(&r).unwrap(), b"hello");
    }

    #[test]
    fn no_content_length_returns_whole_body() {
        let r = res("HTTP/1.1 200 OK\r\nServer: x", "hello");
        assert_eq!(parse_response(&r).unwrap(), b"hello");
    }

    #[test]
    fn chunked_wins_over_content_length_and_validates_terminator() {
        // Transfer-Encoding takes precedence (RFC 9112); a bogus Content-Length
        // alongside it must not be applied.
        let r = res(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 999",
            "5\r\nhello\r\n0\r\n\r\n",
        );
        assert_eq!(parse_response(&r).unwrap(), b"hello");

        let truncated = res(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked",
            "5\r\nhel",
        );
        assert!(parse_response(&truncated).is_err());
    }

    #[test]
    fn non_2xx_still_fails_before_body_handling() {
        let r = res("HTTP/1.1 400 Bad Request\r\nContent-Length: 2", "no");
        assert!(parse_response(&r).unwrap_err().contains("400"));
    }
}
