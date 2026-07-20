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

/// GET `url` and parse the response body as JSON, bounded by `timeout`.
pub async fn get_json(url: &str, timeout: Duration) -> Result<serde_json::Value, String> {
    let bytes = tokio::time::timeout(timeout, fetch(url))
        .await
        .map_err(|_| format!("timeout after {}ms", timeout.as_millis()))??;
    serde_json::from_slice(&bytes).map_err(|e| format!("json parse: {e}"))
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
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: queen-seg\r\n\
         Accept: application/json\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("write: {e}"))?;
    stream.flush().await.ok();
    // Connection: close — read to EOF gets the whole response.
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .map_err(|e| format!("read: {e}"))?;
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
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("transfer-encoding")
                && v.to_ascii_lowercase().contains("chunked")
            {
                chunked = true;
            }
        }
    }
    if chunked {
        dechunk(body)
    } else {
        Ok(body.to_vec())
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
