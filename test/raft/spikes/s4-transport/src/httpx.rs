//! Transport (b): HTTP/1.1 with binary bodies over a hyper keep-alive pool.
//!
//! The comparison point for §12.5. Two structural differences from the framed
//! transport, both of which the numbers must show:
//!   - HTTP/1.1 has no multiplexing: a connection carries one request at a
//!     time, so N in-flight commands need N connections. The pool opens them.
//!   - Every command carries request and response headers on the wire.
//!
//! Authentication: a pooled connection cannot carry the per-connection
//! handshake of §9.2 (the pool hands out whichever connection is idle), so the
//! HTTP variant MACs every request body with the shared secret instead
//! (`--mac 1`), which is also the only thing that makes it comparable to the
//! framed transport's per-frame MAC.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;

use crate::frame::{T_CMD, T_STATS_REQ};
use crate::net::{self, ConnIo, Counters};
use crate::{auth, tcpx, wire};

const MAC_HEADER: &str = "x-queen-raft-mac";

// ------------------------------------------------------------------ client

#[derive(Clone)]
pub struct CountingConnector {
    addr: SocketAddr,
    tls: Option<Arc<rustls::ClientConfig>>,
    c: Arc<Counters>,
}

impl tower_service::Service<hyper::Uri> for CountingConnector {
    type Response = TokioIo<ConnIo>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: hyper::Uri) -> Self::Future {
        let (addr, tls, c) = (self.addr, self.tls.clone(), self.c.clone());
        Box::pin(async move { Ok(TokioIo::new(net::connect(addr, tls, c).await?)) })
    }
}

pub struct HttpClient {
    client: hyper_util::client::legacy::Client<CountingConnector, Full<Bytes>>,
    url: String,
    stats_url: String,
    secret: Option<Vec<u8>>,
    pub counters: Arc<Counters>,
    pub handshake_us: u64,
}

impl HttpClient {
    pub async fn connect(
        addr: SocketAddr,
        pool: usize,
        secret: &[u8],
        per_request_mac: bool,
        tls: Option<Arc<rustls::ClientConfig>>,
        counters: Arc<Counters>,
    ) -> io::Result<Self> {
        let scheme = if tls.is_some() { "https" } else { "http" };
        let connector = CountingConnector {
            addr,
            tls: tls.clone(),
            c: counters.clone(),
        };
        let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
            .pool_max_idle_per_host(pool)
            .pool_idle_timeout(Duration::from_secs(600))
            .build(connector);
        let me = Self {
            client,
            url: format!("{scheme}://localhost:{}/f", addr.port()),
            stats_url: format!("{scheme}://localhost:{}/stats", addr.port()),
            secret: if per_request_mac {
                Some(secret.to_vec())
            } else {
                None
            },
            counters,
            handshake_us: 0,
        };
        // Warm a few connections only (the pool sizes itself to the offered
        // concurrency during the warmup window; pre-opening `pool` of them
        // would hide how many HTTP/1.1 actually needs).
        let warm_n = pool.min(4).max(1);
        let t0 = Instant::now();
        let warm = wire::encode_command(&[0u8; 16], 0, "t", "q", "0", 1, &[0u8; 8]);
        let mut hs = Vec::new();
        for _ in 0..warm_n {
            hs.push(me.forward([0u8; 16], &warm));
        }
        for h in hs {
            h.await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }
        let handshake_us = t0.elapsed().as_micros() as u64 / warm_n as u64;
        Ok(Self { handshake_us, ..me })
    }

    pub async fn forward(&self, _req_id: [u8; 16], body: &[u8]) -> io::Result<Vec<u8>> {
        let mut b = Request::builder()
            .method(Method::POST)
            .uri(&self.url)
            .header(hyper::header::CONTENT_TYPE, "application/octet-stream");
        if let Some(s) = &self.secret {
            b = b.header(MAC_HEADER, auth::request_mac(s, body));
        }
        let req = b
            .body(Full::new(Bytes::copy_from_slice(body)))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        let resp = self
            .client
            .request(req)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        if resp.status() != StatusCode::OK {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("http {}", resp.status()),
            ));
        }
        let b = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
            .to_bytes();
        Ok(b.to_vec())
    }

    pub async fn stats(&self, _req_id: [u8; 16]) -> io::Result<wire::Stats> {
        let req = Request::builder()
            .method(Method::GET)
            .uri(&self.stats_url)
            .body(Full::new(Bytes::new()))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        let resp = self
            .client
            .request(req)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let b = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
            .to_bytes();
        wire::Stats::decode(&b).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "stats"))
    }
}

// ------------------------------------------------------------------ leader

pub async fn serve(
    addr: SocketAddr,
    secret: Vec<u8>,
    per_request_mac: bool,
    tls: Option<Arc<rustls::ServerConfig>>,
    counters: Arc<Counters>,
) -> io::Result<()> {
    let l = TcpListener::bind(addr).await?;
    eprintln!(
        "[leader] http listening on {addr} mac={per_request_mac} tls={}",
        tls.is_some()
    );
    loop {
        let (s, _peer) = l.accept().await?;
        let tls = tls.clone();
        let c = counters.clone();
        let secret = secret.clone();
        tokio::spawn(async move {
            let _g = net::OpenGuard(c.clone());
            let io = match net::accept_wrap(s, tls, c.clone()).await {
                Ok(io) => io,
                Err(e) => {
                    eprintln!("[leader] accept: {e}");
                    return;
                }
            };
            let svc = service_fn(move |req: Request<Incoming>| {
                let c = c.clone();
                let secret = secret.clone();
                async move {
                    Ok::<_, std::convert::Infallible>(
                        route(req, &secret, per_request_mac, &c).await,
                    )
                }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .keep_alive(true)
                .serve_connection(TokioIo::new(io), svc)
                .await
            {
                eprintln!("[leader] http conn: {e}");
            }
        });
    }
}

async fn route(
    req: Request<Incoming>,
    secret: &[u8],
    per_request_mac: bool,
    c: &Counters,
) -> Response<Full<Bytes>> {
    if req.uri().path() == "/stats" {
        let body = tcpx::stats_of(c).encode(&[0u8; 16]);
        return Response::new(Full::new(Bytes::from(body)));
    }
    let mac = req
        .headers()
        .get(MAC_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let body = match req.into_body().collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => return bad(StatusCode::BAD_REQUEST),
    };
    if per_request_mac {
        match mac {
            Some(m) if m == auth::request_mac(secret, &body) => {}
            _ => {
                c.cmds.fetch_add(0, Ordering::Relaxed);
                return bad(StatusCode::FORBIDDEN);
            }
        }
    }
    match tcpx::handle(T_CMD, &body, c) {
        Some((_, out)) => Response::new(Full::new(Bytes::from(out))),
        None => bad(StatusCode::BAD_REQUEST),
    }
}

fn bad(s: StatusCode) -> Response<Full<Bytes>> {
    let mut r = Response::new(Full::new(Bytes::new()));
    *r.status_mut() = s;
    r
}

/// Unused marker so the stats frame type stays in one place.
pub const _STATS: u8 = T_STATS_REQ;
