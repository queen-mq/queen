//! Sockets, byte counting, TLS and process CPU.
//!
//! Bytes are counted on the raw `TcpStream`, i.e. BELOW rustls and below
//! HTTP/1.1 framing, so "bytes on the wire per message" includes TLS record
//! overhead and HTTP headers.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

#[derive(Default, Debug)]
pub struct Counters {
    pub rx: AtomicU64,
    pub tx: AtomicU64,
    /// connections ever opened (cumulative)
    pub conns: AtomicU64,
    /// connections currently open (gauge): for HTTP this is the pool's real
    /// size, which the cumulative counter overstates when the pool churns.
    pub open: AtomicU64,
    pub cmds: AtomicU64,
    pub msgs: AtomicU64,
}

impl Counters {
    pub fn snapshot(&self) -> (u64, u64, u64, u64, u64, u64) {
        (
            self.rx.load(Ordering::Relaxed),
            self.tx.load(Ordering::Relaxed),
            self.conns.load(Ordering::Relaxed),
            self.open.load(Ordering::Relaxed),
            self.cmds.load(Ordering::Relaxed),
            self.msgs.load(Ordering::Relaxed),
        )
    }
}

pub struct CountingStream<S> {
    inner: S,
    c: Arc<Counters>,
}

impl<S> CountingStream<S> {
    pub fn new(inner: S, c: Arc<Counters>) -> Self {
        Self { inner, c }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for CountingStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let r = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &r {
            let n = buf.filled().len() - before;
            self.c.rx.fetch_add(n as u64, Ordering::Relaxed);
        }
        r
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for CountingStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let r = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &r {
            self.c.tx.fetch_add(*n as u64, Ordering::Relaxed);
        }
        r
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let r = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        if let Poll::Ready(Ok(n)) = &r {
            self.c.tx.fetch_add(*n as u64, Ordering::Relaxed);
        }
        r
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

pub trait Duplex: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Duplex for T {}

/// Boxed stream that also carries hyper-util's `Connection` marker, so the
/// same plain/TLS stream works for the framed transport and for the hyper
/// client pool.
pub struct ConnIo(pub Box<dyn Duplex>);

impl AsyncRead for ConnIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for ConnIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0).poll_shutdown(cx)
    }
}

impl hyper_util::client::legacy::connect::Connection for ConnIo {
    fn connected(&self) -> hyper_util::client::legacy::connect::Connected {
        hyper_util::client::legacy::connect::Connected::new()
    }
}

// ---------------------------------------------------------------- TLS setup

/// Spike-only: a throwaway self-signed `localhost` certificate minted at start.
/// The private key never leaves the leader process; the DER certificate is
/// written to `cert_out` for the receiver to trust.
pub fn server_tls(cert_out: &str) -> Arc<rustls::ServerConfig> {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("rcgen");
    let cert_der = ck.cert.der().to_vec();
    std::fs::write(cert_out, &cert_der).expect("write cert");
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(ck.key_pair.serialize_der().into());
    let cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![rustls::pki_types::CertificateDer::from(cert_der)], key)
        .expect("server config");
    Arc::new(cfg)
}

pub fn client_tls(cert_path: &str) -> Arc<rustls::ClientConfig> {
    let der = std::fs::read(cert_path).expect("read cert");
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(der))
        .expect("add root");
    Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

// ------------------------------------------------------------ connect/accept

pub async fn connect(
    addr: SocketAddr,
    tls: Option<Arc<rustls::ClientConfig>>,
    c: Arc<Counters>,
) -> io::Result<ConnIo> {
    let s = TcpStream::connect(addr).await?;
    s.set_nodelay(true)?;
    c.conns.fetch_add(1, Ordering::Relaxed);
    c.open.fetch_add(1, Ordering::Relaxed);
    let s = CountingStream::new(s, c);
    match tls {
        None => Ok(ConnIo(Box::new(s))),
        Some(cfg) => {
            let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
            let s = tokio_rustls::TlsConnector::from(cfg)
                .connect(name, s)
                .await?;
            Ok(ConnIo(Box::new(s)))
        }
    }
}

pub async fn accept_wrap(
    s: TcpStream,
    tls: Option<Arc<rustls::ServerConfig>>,
    c: Arc<Counters>,
) -> io::Result<ConnIo> {
    s.set_nodelay(true)?;
    c.conns.fetch_add(1, Ordering::Relaxed);
    c.open.fetch_add(1, Ordering::Relaxed);
    let s = CountingStream::new(s, c);
    match tls {
        None => Ok(ConnIo(Box::new(s))),
        Some(cfg) => {
            let s = tokio_rustls::TlsAcceptor::from(cfg).accept(s).await?;
            Ok(ConnIo(Box::new(s)))
        }
    }
}

// ------------------------------------------------------------------- rusage

/// Decrements the live-connection gauge when a connection task ends.
pub struct OpenGuard(pub Arc<Counters>);

impl Drop for OpenGuard {
    fn drop(&mut self) {
        self.0.open.fetch_sub(1, Ordering::Relaxed);
    }
}

/// (user_us, sys_us, max_rss_bytes) for this process.
pub fn rusage() -> (u64, u64, u64) {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    if rc != 0 {
        return (0, 0, 0);
    }
    let us = |t: libc::timeval| t.tv_sec as u64 * 1_000_000 + t.tv_usec as u64;
    // macOS reports ru_maxrss in bytes, Linux in kilobytes.
    let rss = if cfg!(target_os = "macos") {
        u.ru_maxrss as u64
    } else {
        u.ru_maxrss as u64 * 1024
    };
    (us(u.ru_utime), us(u.ru_stime), rss)
}
