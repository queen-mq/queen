//! Framing, startup, authentication and TLS for the replication connection.
//! OWNER: agent R. Private to `repl`; the public surface is `repl::client`.
//!
//! Upstream tokio-postgres cannot run the replication protocol (its backend
//! parser has no CopyBothResponse), so this is a small client of our own:
//! postgres-protocol builds the frontend messages and runs SCRAM and MD5;
//! the backend side is framed and parsed here (tag byte + Int32 length).
//!
//! Errors follow the regular connection's classification
//! ([`crate::pg::connect::classify`]) wherever the two can meet the same
//! failure, so the status block reads the same whichever connection hit it:
//! a server's ErrorResponse is [`Error::Pg`] with its SQLSTATE; failing to
//! reach the server (refused, unreachable, timed out) is a transient
//! `Error::Pg` without one; a TLS verdict, a missing password or a SCRAM
//! signature that does not verify is a non-transient one. A connection that
//! breaks AFTER it was established, or a frame that makes no sense, is
//! [`Error::Io`] (retryable).

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use postgres_protocol::authentication::md5_hash;
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256, SCRAM_SHA_256};
use postgres_protocol::message::frontend;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

use crate::config::{ConnectionSpec, SslMode};
use crate::error::{Error, Result};
use crate::pg::connect::{transient_sqlstate, EgressPolicy, SESSION_OPTIONS};

/// The largest frame accepted. A server builds each message in one
/// StringInfo, capped at 1 GiB (MaxAllocSize), so anything larger is a
/// corrupt length, not data — and refusing it keeps a garbage length from
/// becoming a giant allocation.
const MAX_FRAME: usize = (1 << 30) + (1 << 20);

/// How much the read buffer grows per read while a frame is incomplete.
const READ_CHUNK: usize = 8 << 20;
const READ_MIN: usize = 16 << 10;

/// TCP keepalive and `TCP_USER_TIMEOUT` (Linux), the values the regular
/// connection runs with (pg/connect.rs). A replication flow that dies
/// without a RST (a failover, a dropped NAT entry) would otherwise surface
/// only when the kernel gives up retransmitting a status update, about
/// fifteen minutes on Linux, or never while nothing is written; with these
/// a dead peer is an error in about a minute.
const KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_RETRIES: u32 = 3;
#[cfg(target_os = "linux")]
const TCP_USER_TIMEOUT: Duration = Duration::from_secs(60);

/// The socket, plain or TLS: one type so everything above it is written once.
pub(crate) enum Io {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for Io {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Io::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Io::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Io {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Io::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Io::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Io::Plain(s) => Pin::new(s).poll_flush(cx),
            Io::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Io::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Io::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// One backend message: its tag and its body (without the length).
#[derive(Debug, Clone)]
pub(crate) struct Frame {
    pub tag: u8,
    pub body: Bytes,
}

/// The framed connection. Both buffers live here, not in any future, which
/// is what makes reads cancellation-safe: a cancelled [`Wire::fill`] leaves
/// every byte it got in `rbuf`, and a frame leaves `rbuf` only whole, in
/// [`Wire::take_frame`], which never awaits. Writes queue in `wbuf` and
/// [`Wire::flush`] removes only what the socket took, so a cancelled flush
/// sends the rest on the next one instead of tearing a message.
pub(crate) struct Wire {
    io: Io,
    rbuf: BytesMut,
    wbuf: BytesMut,
}

impl Wire {
    pub fn new(io: Io) -> Wire {
        Wire {
            io,
            rbuf: BytesMut::with_capacity(READ_MIN),
            wbuf: BytesMut::with_capacity(1024),
        }
    }

    /// The write queue: frontend messages are appended here, then flushed.
    pub fn wbuf(&mut self) -> &mut BytesMut {
        &mut self.wbuf
    }

    pub fn is_tls(&self) -> bool {
        matches!(self.io, Io::Tls(_))
    }

    /// The next whole frame in the read buffer, if there is one. Never awaits.
    pub fn take_frame(&mut self) -> Result<Option<Frame>> {
        let Some(total) = self.pending_len()? else {
            return Ok(None);
        };
        if self.rbuf.len() < total {
            return Ok(None);
        }
        let mut f = self.rbuf.split_to(total);
        let tag = f[0];
        f.advance(5);
        Ok(Some(Frame {
            tag,
            body: f.freeze(),
        }))
    }

    /// The whole size (tag included) of the frame at the head of the buffer,
    /// once its header is in.
    fn pending_len(&self) -> Result<Option<usize>> {
        if self.rbuf.len() < 5 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([self.rbuf[1], self.rbuf[2], self.rbuf[3], self.rbuf[4]]);
        let len = len as usize;
        if len < 4 {
            return Err(Error::io(format!(
                "the server sent a frame ('{}') with length {len}",
                self.rbuf[0].escape_ascii()
            )));
        }
        if len > MAX_FRAME {
            return Err(Error::io(format!(
                "the server sent a frame ('{}') of {len} bytes; the limit is {MAX_FRAME}",
                self.rbuf[0].escape_ascii()
            )));
        }
        Ok(Some(len + 1))
    }

    /// Read more bytes into the buffer. Cancellation-safe: `read_buf` either
    /// appended bytes or did nothing. End of stream is an error: every way
    /// the server ends a conversation on purpose is a message first.
    pub async fn fill(&mut self) -> Result<()> {
        let want = match self.pending_len()? {
            Some(total) if total > self.rbuf.len() => {
                (total - self.rbuf.len()).clamp(READ_MIN, READ_CHUNK)
            }
            _ => READ_MIN,
        };
        self.rbuf.reserve(want);
        let n = self
            .io
            .read_buf(&mut self.rbuf)
            .await
            .map_err(|e| Error::io(format!("reading from the server: {e}")))?;
        if n == 0 {
            return Err(Error::io("the server closed the connection"));
        }
        Ok(())
    }

    /// The next frame, reading as needed. Cancellation-safe.
    pub async fn read_frame(&mut self) -> Result<Frame> {
        loop {
            if let Some(f) = self.take_frame()? {
                return Ok(f);
            }
            self.fill().await?;
        }
    }

    /// Send everything queued. Cancellation leaves the unsent bytes queued.
    pub async fn flush(&mut self) -> Result<()> {
        while !self.wbuf.is_empty() {
            let n = self
                .io
                .write(&self.wbuf)
                .await
                .map_err(|e| Error::io(format!("writing to the server: {e}")))?;
            if n == 0 {
                return Err(Error::io(
                    "writing to the server: the socket accepts no more",
                ));
            }
            self.wbuf.advance(n);
        }
        self.io
            .flush()
            .await
            .map_err(|e| Error::io(format!("writing to the server: {e}")))
    }

    /// Close the socket (TLS: close_notify first). Errors are of no interest.
    pub async fn shutdown(&mut self) {
        let _ = self.io.shutdown().await;
    }
}

/// What the server told us at startup.
#[derive(Debug, Clone, Default)]
pub(crate) struct Session {
    /// ParameterStatus values (`server_version`, `integer_datetimes`, ...).
    pub params: Vec<(String, String)>,
    pub backend_pid: i32,
}

/// Resolve (egress policy included), connect, TLS, startup, authenticate,
/// wait for ReadyForQuery. `connect_timeout_ms` bounds the name lookup and
/// then each address in turn, as libpq's `connect_timeout` (and the regular
/// connection) do. The next address is tried only when the previous one
/// could not be reached; a server that ANSWERED (an ErrorResponse, a TLS
/// verdict, an authentication failure) ends the attempt, as in libpq.
pub(crate) async fn connect(
    conn: &ConnectionSpec,
    password: Option<&str>,
    policy: &EgressPolicy,
    application_name: &str,
) -> Result<(Wire, Session)> {
    let budget = Duration::from_millis(conn.connect_timeout_ms.max(1));
    let addrs = tokio::time::timeout(
        budget,
        crate::pg::connect::resolve(&conn.host, conn.port, policy),
    )
    .await
    .map_err(|_| {
        unreachable_error(format!(
            "resolving {} took longer than connectTimeoutMs ({} ms)",
            conn.host, conn.connect_timeout_ms
        ))
    })??;
    let tls = crate::pg::connect::tls_client_config(conn)?;
    let host = bare_host(&conn.host);
    let mut last = None;
    for addr in addrs {
        let attempt = async {
            let tcp = connect_tcp(addr).await?;
            let io = negotiate_tls(tcp, conn.ssl_mode, tls.clone(), host).await?;
            let mut wire = Wire::new(io);
            let session = startup(
                &mut wire,
                &conn.user,
                &conn.database,
                password,
                application_name,
            )
            .await?;
            Ok::<_, Error>((wire, session))
        };
        match tokio::time::timeout(budget, attempt).await {
            Ok(Ok(done)) => return Ok(done),
            Ok(Err(e)) if !is_unreachable(&e) => return Err(e),
            Ok(Err(e)) => last = Some(e),
            Err(_) => {
                last = Some(unreachable_error(format!(
                    "connecting to {addr} ({}) took longer than connectTimeoutMs ({} ms)",
                    conn.host, conn.connect_timeout_ms
                )))
            }
        }
    }
    Err(last.unwrap_or_else(|| unreachable_error(format!("{} resolves to no address", conn.host))))
}

/// `[::1]` (the URL form of an IPv6 literal) → `::1`.
fn bare_host(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

/// The server could not be reached: transient, no SQLSTATE (the regular
/// connection reports the same failure the same way).
fn unreachable_error(message: String) -> Error {
    Error::Pg {
        sqlstate: None,
        message,
        transient: true,
    }
}

fn is_unreachable(e: &Error) -> bool {
    matches!(
        e,
        Error::Pg {
            sqlstate: None,
            transient: true,
            ..
        } | Error::Io(_)
    )
}

async fn connect_tcp(addr: SocketAddr) -> Result<TcpStream> {
    let tcp = TcpStream::connect(addr)
        .await
        .map_err(|e| unreachable_error(format!("could not connect to {addr}: {e}")))?;
    // Standby status updates are tiny and latency-bound.
    let _ = tcp.set_nodelay(true);
    set_liveness(&tcp);
    Ok(tcp)
}

/// Best effort, like the options themselves: a socket that refuses one
/// still carries the protocol.
fn set_liveness(tcp: &TcpStream) {
    let sock = socket2::SockRef::from(tcp);
    let keepalive = socket2::TcpKeepalive::new().with_time(KEEPALIVE_IDLE);
    #[cfg(not(any(
        target_os = "aix",
        target_os = "redox",
        target_os = "solaris",
        target_os = "openbsd"
    )))]
    let keepalive = keepalive.with_interval(KEEPALIVE_INTERVAL);
    #[cfg(not(any(
        target_os = "aix",
        target_os = "redox",
        target_os = "solaris",
        target_os = "windows",
        target_os = "openbsd"
    )))]
    let keepalive = keepalive.with_retries(KEEPALIVE_RETRIES);
    let _ = sock.set_tcp_keepalive(&keepalive);
    #[cfg(target_os = "linux")]
    let _ = sock.set_tcp_user_timeout(Some(TCP_USER_TIMEOUT));
}

/// SSLRequest per `mode`, then TLS on `S`. `prefer` falls back to plain text
/// on `N`; `require` and `verify-full` refuse it.
pub(crate) async fn negotiate_tls(
    mut tcp: TcpStream,
    mode: SslMode,
    tls: Option<Arc<rustls::ClientConfig>>,
    host: &str,
) -> Result<Io> {
    let Some(config) = tls.filter(|_| mode != SslMode::Disable) else {
        return Ok(Io::Plain(tcp));
    };
    let mut req = BytesMut::with_capacity(8);
    frontend::ssl_request(&mut req);
    tcp.write_all(&req)
        .await
        .map_err(|e| unreachable_error(format!("sending SSLRequest: {e}")))?;
    let mut answer = [0u8; 1];
    tcp.read_exact(&mut answer)
        .await
        .map_err(|e| unreachable_error(format!("reading the answer to SSLRequest: {e}")))?;
    match answer[0] {
        b'S' => {
            let name = server_name(host, &tcp, mode)?;
            let stream = tokio_rustls::TlsConnector::from(config)
                .connect(name, tcp)
                .await
                .map_err(tls_error)?;
            Ok(Io::Tls(Box::new(stream)))
        }
        b'N' if mode == SslMode::Prefer => Ok(Io::Plain(tcp)),
        b'N' => Err(Error::Pg {
            sqlstate: None,
            message: format!(
                "the server does not support TLS, and sslMode {} requires it",
                ssl_mode_name(mode)
            ),
            transient: false,
        }),
        // A server that refuses before it reads the startup packet: an
        // ErrorResponse (a postmaster that cannot fork writes an old-protocol
        // one, whose garbage length is an Io error here, not a hang).
        b'E' => {
            let mut wire = Wire::new(Io::Plain(tcp));
            wire.rbuf.extend_from_slice(b"E");
            let f = wire.read_frame().await?;
            Err(error_response(&f.body))
        }
        other => Err(Error::io(format!(
            "the server answered SSLRequest with '{}'",
            other.escape_ascii()
        ))),
    }
}

fn ssl_mode_name(mode: SslMode) -> &'static str {
    match mode {
        SslMode::Disable => "disable",
        SslMode::Prefer => "prefer",
        SslMode::Require => "require",
        SslMode::VerifyFull => "verify-full",
    }
}

/// The name TLS sends (SNI) and `verify-full` checks. Without verification
/// a host that is not a valid DNS name (an underscore, say) still connects:
/// the peer's address stands in.
fn server_name(host: &str, tcp: &TcpStream, mode: SslMode) -> Result<ServerName<'static>> {
    match ServerName::try_from(host.to_string()) {
        Ok(n) => Ok(n),
        Err(e) if mode == SslMode::VerifyFull => Err(Error::config(format!(
            "connection.host {host:?} is not a name TLS can verify ({e})"
        ))),
        Err(_) => {
            let peer = tcp
                .peer_addr()
                .map_err(|e| Error::io(format!("the socket has no peer address: {e}")))?;
            Ok(ServerName::IpAddress(peer.ip().into()))
        }
    }
}

/// A TLS failure: a certificate or handshake verdict is not transient (the
/// configuration must change); the connection dropping during the handshake
/// is.
fn tls_error(e: std::io::Error) -> Error {
    let verdict = e
        .get_ref()
        .is_some_and(|inner| inner.downcast_ref::<rustls::Error>().is_some());
    Error::Pg {
        sqlstate: None,
        message: format!("TLS handshake: {e}"),
        transient: !verdict,
    }
}

/// StartupMessage, authentication, then ParameterStatus/BackendKeyData up to
/// ReadyForQuery. The startup parameters make this a logical replication
/// connection (`replication=database`) with the session settings every
/// connection of the crate shares, and UTF-8 text whatever the database's
/// encoding.
pub(crate) async fn startup(
    wire: &mut Wire,
    user: &str,
    database: &str,
    password: Option<&str>,
    application_name: &str,
) -> Result<Session> {
    let mut params = vec![
        ("user", user),
        ("replication", "database"),
        ("application_name", application_name),
        ("options", SESSION_OPTIONS),
        ("client_encoding", "UTF8"),
    ];
    // Absent, the server uses the user name, as libpq does.
    if !database.is_empty() {
        params.push(("database", database));
    }
    frontend::startup_message(params, wire.wbuf())
        .map_err(|e| Error::config(format!("connection parameters: {e}")))?;
    wire.flush().await?;
    authenticate(wire, user, password).await?;
    let mut session = Session::default();
    loop {
        let f = wire.read_frame().await?;
        match f.tag {
            b'S' => {
                let mut r = Body::new(&f);
                session.params.push((r.cstr()?, r.cstr()?));
            }
            // A 4-byte secret on protocol 3.0; only the pid is kept.
            b'K' => session.backend_pid = Body::new(&f).i32()?,
            b'Z' => return Ok(session),
            b'E' => return Err(error_response(&f.body)),
            b'N' | b'v' => {}
            t => return Err(unexpected(t, "startup")),
        }
    }
}

async fn authenticate(wire: &mut Wire, user: &str, password: Option<&str>) -> Result<()> {
    let need = || {
        password.ok_or_else(|| Error::Pg {
            sqlstate: None,
            message:
                "the server asks for a password and the connector has none (connection.password)"
                    .into(),
            transient: false,
        })
    };
    let mut scram: Option<ScramSha256> = None;
    loop {
        let f = wire.read_frame().await?;
        match f.tag {
            b'R' => {}
            b'E' => return Err(error_response(&f.body)),
            // A notice, or NegotiateProtocolVersion (we ask for 3.0 and no
            // protocol options, so there is nothing to adapt to).
            b'N' | b'v' => continue,
            t => return Err(unexpected(t, "authentication")),
        }
        let mut r = Body::new(&f);
        match r.i32()? {
            0 => {
                if scram.is_some() {
                    return Err(auth_failed("the server ended SCRAM without its signature"));
                }
                return Ok(());
            }
            3 => {
                frontend::password_message(need()?.as_bytes(), wire.wbuf())
                    .map_err(|e| Error::config(format!("connection.password: {e}")))?;
                wire.flush().await?;
            }
            5 => {
                let salt = r.take::<4>()?;
                let hash = md5_hash(user.as_bytes(), need()?.as_bytes(), salt);
                frontend::password_message(hash.as_bytes(), wire.wbuf())
                    .map_err(|e| Error::config(format!("connection.password: {e}")))?;
                wire.flush().await?;
            }
            10 => {
                let mut offered = Vec::new();
                loop {
                    let m = r.cstr()?;
                    if m.is_empty() {
                        break;
                    }
                    offered.push(m);
                }
                if !offered.iter().any(|m| m == SCRAM_SHA_256) {
                    return Err(Error::fatal(
                        "auth_method",
                        format!(
                            "the server offers SASL {} only; queen-pg speaks SCRAM-SHA-256",
                            offered.join(", ")
                        ),
                    ));
                }
                // No channel binding ("n,,"): saying "y" while the server
                // offers SCRAM-SHA-256-PLUS over TLS would read as a
                // downgrade attack and fail.
                let s = ScramSha256::new(need()?.as_bytes(), ChannelBinding::unsupported());
                frontend::sasl_initial_response(SCRAM_SHA_256, s.message(), wire.wbuf())
                    .map_err(|e| Error::io(format!("SCRAM: {e}")))?;
                wire.flush().await?;
                scram = Some(s);
            }
            11 => {
                let s = scram
                    .as_mut()
                    .ok_or_else(|| Error::io("the server continued a SASL exchange that never began"))?;
                s.update(r.rest())
                    .map_err(|e| auth_failed(&format!("SCRAM: {e}")))?;
                frontend::sasl_response(s.message(), wire.wbuf())
                    .map_err(|e| Error::io(format!("SCRAM: {e}")))?;
                wire.flush().await?;
            }
            12 => {
                let mut s = scram
                    .take()
                    .ok_or_else(|| Error::io("the server ended a SASL exchange that never began"))?;
                s.finish(r.rest())
                    .map_err(|e| auth_failed(&format!("SCRAM server signature: {e}")))?;
                // AuthenticationOk follows.
                return expect_auth_ok(wire).await;
            }
            code @ (2 | 6 | 7 | 8 | 9) => {
                return Err(Error::fatal(
                    "auth_method",
                    format!(
                        "the server asks for {} authentication; queen-pg supports password, md5 and scram-sha-256 (pg_hba.conf)",
                        match code {
                            2 => "Kerberos V5",
                            6 => "SCM credentials",
                            9 => "SSPI",
                            _ => "GSSAPI",
                        }
                    ),
                ))
            }
            code => {
                return Err(Error::fatal(
                    "auth_method",
                    format!("the server asks for authentication method {code}, which queen-pg does not know"),
                ))
            }
        }
    }
}

async fn expect_auth_ok(wire: &mut Wire) -> Result<()> {
    loop {
        let f = wire.read_frame().await?;
        match f.tag {
            b'R' if Body::new(&f).i32()? == 0 => return Ok(()),
            b'E' => return Err(error_response(&f.body)),
            b'N' => {}
            t => return Err(unexpected(t, "authentication")),
        }
    }
}

fn auth_failed(message: &str) -> Error {
    Error::Pg {
        sqlstate: None,
        message: message.to_string(),
        transient: false,
    }
}

/// An ErrorResponse (or NoticeResponse) body → [`Error::Pg`]: SQLSTATE,
/// message, then detail and hint when the server sent them; transient per
/// [`transient_sqlstate`]. Text is decoded leniently: an error must survive
/// being reported.
pub(crate) fn error_response(body: &[u8]) -> Error {
    let (mut code, mut message, mut detail, mut hint) = (None, None, None, None);
    let mut rest = body;
    while let Some((&field, tail)) = rest.split_first() {
        if field == 0 {
            break;
        }
        let Some(end) = tail.iter().position(|&b| b == 0) else {
            break;
        };
        let v = String::from_utf8_lossy(&tail[..end]).into_owned();
        match field {
            b'C' => code = Some(v),
            b'M' => message = Some(v),
            b'D' => detail = Some(v),
            b'H' => hint = Some(v),
            _ => {}
        }
        rest = &tail[end + 1..];
    }
    let mut message =
        message.unwrap_or_else(|| "the server reported an error without a message".into());
    if let Some(d) = detail {
        message.push_str(" (detail: ");
        message.push_str(&d);
        message.push(')');
    }
    if let Some(h) = hint {
        message.push_str(" (hint: ");
        message.push_str(&h);
        message.push(')');
    }
    Error::Pg {
        transient: code.as_deref().is_some_and(transient_sqlstate),
        sqlstate: code,
        message,
    }
}

pub(crate) fn unexpected(tag: u8, during: &str) -> Error {
    Error::io(format!(
        "the server sent message '{}' during {during}",
        tag.escape_ascii()
    ))
}

/// What a simple query returned.
#[derive(Debug, Clone, Default)]
pub(crate) struct QueryOutcome {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

/// One simple Query: RowDescription / DataRow* / CommandComplete per
/// statement, then ReadyForQuery. The first ErrorResponse is the result,
/// returned once the server is ready again (so the connection stays usable).
pub(crate) async fn simple_query(wire: &mut Wire, sql: &str) -> Result<QueryOutcome> {
    frontend::query(sql, wire.wbuf()).map_err(|e| Error::config(format!("query: {e}")))?;
    wire.flush().await?;
    let mut out = QueryOutcome::default();
    let mut failed: Option<Error> = None;
    loop {
        let f = wire.read_frame().await?;
        match f.tag {
            b'T' => {
                let mut r = Body::new(&f);
                let n = r.u16()?;
                out.columns.clear();
                for _ in 0..n {
                    out.columns.push(r.cstr()?);
                    r.take::<18>()?;
                }
                r.end()?;
            }
            b'D' => {
                let mut r = Body::new(&f);
                let n = usize::from(r.u16()?);
                r.need(n.saturating_mul(4))?;
                let mut row = Vec::with_capacity(n);
                for _ in 0..n {
                    let len = r.i32()?;
                    row.push(if len < 0 {
                        None
                    } else {
                        let b = r.bytes(len as usize)?;
                        Some(
                            String::from_utf8(b.to_vec())
                                .map_err(|_| Error::io("a query result is not UTF-8"))?,
                        )
                    });
                }
                r.end()?;
                out.rows.push(row);
            }
            b'C' | b'I' | b'N' | b'S' => {}
            b'E' => {
                failed.get_or_insert_with(|| error_response(&f.body));
            }
            b'Z' => return failed.map_or(Ok(out), Err),
            // COPY FROM STDIN: refuse it so the server can get back to us.
            b'G' => {
                frontend::copy_fail("queen-pg sends no COPY data", wire.wbuf())
                    .map_err(|e| Error::io(format!("CopyFail: {e}")))?;
                wire.flush().await?;
            }
            // COPY TO STDOUT: its data is skipped.
            b'H' | b'd' | b'c' => {}
            t => return Err(unexpected(t, "a simple query")),
        }
    }
}

/// A cursor over a frame body; every read is bounds-checked.
pub(crate) struct Body<'a> {
    b: &'a [u8],
    tag: u8,
}

impl<'a> Body<'a> {
    pub fn new(f: &'a Frame) -> Body<'a> {
        Body {
            b: &f.body,
            tag: f.tag,
        }
    }

    fn short(&self) -> Error {
        Error::io(format!(
            "the server sent a truncated '{}' message",
            self.tag.escape_ascii()
        ))
    }

    pub fn need(&self, n: usize) -> Result<()> {
        if self.b.len() < n {
            Err(self.short())
        } else {
            Ok(())
        }
    }

    pub fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.need(N)?;
        let mut a = [0u8; N];
        a.copy_from_slice(&self.b[..N]);
        self.b = &self.b[N..];
        Ok(a)
    }

    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        self.need(n)?;
        let (head, tail) = self.b.split_at(n);
        self.b = tail;
        Ok(head)
    }

    pub fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take()?))
    }

    pub fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take()?))
    }

    pub fn cstr(&mut self) -> Result<String> {
        let end = self
            .b
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| self.short())?;
        let s = String::from_utf8_lossy(&self.b[..end]).into_owned();
        self.b = &self.b[end + 1..];
        Ok(s)
    }

    pub fn rest(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.b)
    }

    pub fn end(&self) -> Result<()> {
        if self.b.is_empty() {
            Ok(())
        } else {
            Err(Error::io(format!(
                "the server sent a '{}' message with {} trailing bytes",
                self.tag.escape_ascii(),
                self.b.len()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![tag];
        v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        v.extend_from_slice(body);
        v
    }

    async fn wire_pair() -> (Wire, TcpStream) {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        let (c, s) = tokio::join!(TcpStream::connect(a), l.accept());
        (Wire::new(Io::Plain(c.unwrap())), s.unwrap().0)
    }

    #[tokio::test]
    async fn frames_come_out_whole_however_the_bytes_arrive() {
        let (mut w, _peer) = wire_pair().await;
        let mut all = frame(b'Z', b"I");
        all.extend(frame(b'd', &[7u8; 100]));
        all.extend(frame(b'c', b""));
        for cut in 0..=all.len() {
            w.rbuf.clear();
            w.rbuf.extend_from_slice(&all[..cut]);
            let mut got = Vec::new();
            while let Some(f) = w.take_frame().unwrap() {
                got.push((f.tag, f.body.len()));
            }
            let want: Vec<(u8, usize)> = [(b'Z', 1), (b'd', 100), (b'c', 0)]
                .into_iter()
                .scan(0usize, |end, (t, n)| {
                    *end += 5 + n;
                    Some((t, n, *end))
                })
                .filter(|&(_, _, end)| end <= cut)
                .map(|(t, n, _)| (t, n))
                .collect();
            assert_eq!(got, want, "cut {cut}");
            w.rbuf.extend_from_slice(&all[cut..]);
            while let Some(f) = w.take_frame().unwrap() {
                got.push((f.tag, f.body.len()));
            }
            assert_eq!(got, vec![(b'Z', 1), (b'd', 100), (b'c', 0)]);
        }
    }

    #[tokio::test]
    async fn absurd_lengths_are_io_errors() {
        let (mut w, _peer) = wire_pair().await;
        for header in [
            [b'd', 0, 0, 0, 3],
            [b'd', 0x7f, 0xff, 0xff, 0xff],
            [b'd', 0xff, 0xff, 0xff, 0xff],
        ] {
            w.rbuf.clear();
            w.rbuf.extend_from_slice(&header);
            assert!(matches!(w.take_frame(), Err(Error::Io(_))), "{header:?}");
        }
    }

    #[tokio::test]
    async fn the_socket_keeps_alive() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp = connect_tcp(l.local_addr().unwrap()).await.unwrap();
        let sock = socket2::SockRef::from(&tcp);
        assert!(sock.keepalive().unwrap());
        assert!(tcp.nodelay().unwrap());
    }

    #[tokio::test]
    async fn end_of_stream_is_an_io_error() {
        let (mut w, peer) = wire_pair().await;
        drop(peer);
        assert!(matches!(w.read_frame().await, Err(Error::Io(_))));
    }

    fn err_body(fields: &[(u8, &str)]) -> Vec<u8> {
        let mut b = Vec::new();
        for (t, v) in fields {
            b.push(*t);
            b.extend_from_slice(v.as_bytes());
            b.push(0);
        }
        b.push(0);
        b
    }

    #[test]
    fn error_responses_carry_sqlstate_and_classification() {
        let e = error_response(&err_body(&[
            (b'S', "FATAL"),
            (b'V', "FATAL"),
            (b'C', "28P01"),
            (b'M', "password authentication failed for user \"x\""),
        ]));
        assert_eq!(
            e,
            Error::Pg {
                sqlstate: Some("28P01".into()),
                message: "password authentication failed for user \"x\"".into(),
                transient: false,
            }
        );
        for (code, transient) in [
            ("08006", true),
            ("53300", true),
            ("57P01", true),
            ("57P03", true),
            ("55006", true),
            ("40001", true),
            ("42704", false),
            ("55000", false),
            ("XX000", false),
        ] {
            let e = error_response(&err_body(&[(b'C', code), (b'M', "m")]));
            assert_eq!(e.is_retryable(), transient, "{code}");
        }
        let e = error_response(&err_body(&[
            (b'C', "55006"),
            (b'M', "replication slot \"s\" is active for PID 7"),
            (b'D', "d"),
            (b'H', "h"),
        ]));
        assert_eq!(
            e.to_string(),
            "postgres 55006: replication slot \"s\" is active for PID 7 (detail: d) (hint: h)"
        );
        // Garbage: still an error with a message, never a panic.
        for body in [&b""[..], b"\0", b"C", b"C080", b"M\xff\xfe\0\0", b"\x01x\0"] {
            assert!(matches!(error_response(body), Error::Pg { .. }));
        }
    }
}
