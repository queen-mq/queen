//! The replication client: a `replication=database` connection, the three
//! replication commands this crate uses, and the CopyBoth stream. OWNER: agent R.
//!
//! Contract the source engine relies on:
//! * [`ReplicationStream::next`] is CANCELLATION-SAFE (it may sit in a
//!   `tokio::select!` beside timers): partial frames stay in an internal
//!   buffer, no frame is ever lost or split by a cancelled call.
//! * [`ReplicationStream::send_status`] writes one Standby Status Update
//!   (write, flush, apply, client time, reply-requested) and flushes it.
//! * The server ending the copy (CopyDone, or the connection closing after an
//!   ErrorResponse) is `Ok(None)` / `Err(Error::Pg{..})` with the server's
//!   message and SQLSTATE. The connection dropping without either is
//!   `Err(Error::Io(..))`.
//! * Nothing here answers keepalives on its own: only the caller knows the
//!   flush position it may confirm (PLAN §4.4, persist first, confirm
//!   second).
//! * Times in [`StreamEvent`] are Unix microseconds, like pgoutput's.

use std::time::Duration;

use bytes::{BufMut, Bytes};
use postgres_protocol::message::frontend;

use super::pgoutput::{self, Message};
use super::wire::{self, Session, Wire};
use super::{Lsn, PG_EPOCH_OFFSET_US};
use crate::config::ConnectionSpec;
use crate::error::{Error, Result};
use crate::pg::catalog::{quote_ident, quote_literal};
use crate::pg::connect::EgressPolicy;

/// `IDENTIFY_SYSTEM`'s row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemIdentity {
    pub system_id: String,
    pub timeline: u32,
    pub xlogpos: Lsn,
    pub dbname: Option<String>,
}

/// One frame of the CopyBoth stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEvent {
    /// `'w'`.
    XLogData {
        /// The LSN the output plugin attached to this message: a change's
        /// own, a BEGIN's first, a COMMIT's end LSN; 0/0 for a message
        /// written ahead of another in the same callback (Relation, Type,
        /// Origin). Not an end-of-WAL position: a logical walsender sends
        /// this same value as `wal_end`.
        wal_start: Lsn,
        /// Equal to `wal_start` on a logical stream (walsender.c
        /// `WalSndPrepareWrite`).
        wal_end: Lsn,
        /// When the server sent it, Unix microseconds.
        server_time_us: i64,
        message: Message,
    },
    /// `'k'`.
    Keepalive {
        /// How far the walsender has read the WAL (`sentPtr`): every
        /// transaction that committed before it has been sent.
        wal_end: Lsn,
        /// Unix microseconds.
        server_time_us: i64,
        /// The server wants a Standby Status Update now: without one it
        /// ends the connection when `wal_sender_timeout` runs out.
        reply_requested: bool,
    },
}

/// An open replication connection, before streaming.
pub struct ReplicationClient {
    wire: Wire,
    session: Session,
}

/// The CopyBoth stream after `START_REPLICATION`.
pub struct ReplicationStream {
    wire: Wire,
    session: Session,
    /// The server sent CopyDone: the stream is over.
    server_done: bool,
    /// We sent CopyDone.
    client_done: bool,
}

impl ReplicationClient {
    /// Connect with `replication=database`, TLS per `conn.ssl_mode`
    /// (SSLRequest, then rustls), password auth (cleartext, MD5,
    /// SCRAM-SHA-256), the egress policy checked on the resolved addresses,
    /// and the session GUCs `TimeZone=UTC`, `DateStyle=ISO`,
    /// `IntervalStyle=postgres`, `extra_float_digits=3`, `bytea_output=hex`
    /// in the startup packet's `options` (plus `client_encoding=UTF8`).
    /// `conn.connect_timeout_ms` bounds the lookup and each address's
    /// connect + TLS + authentication, like the regular connection.
    pub async fn connect(
        conn: &ConnectionSpec,
        password: Option<&str>,
        policy: &EgressPolicy,
        application_name: &str,
    ) -> Result<ReplicationClient> {
        let (wire, session) = wire::connect(conn, password, policy, application_name).await?;
        Ok(ReplicationClient { wire, session })
    }

    /// `IDENTIFY_SYSTEM`.
    pub async fn identify_system(&mut self) -> Result<SystemIdentity> {
        let out = wire::simple_query(&mut self.wire, "IDENTIFY_SYSTEM").await?;
        let row = match out.rows.as_slice() {
            [row] if row.len() >= 3 => row,
            _ => {
                return Err(Error::io(
                    "IDENTIFY_SYSTEM did not answer one row of four columns",
                ))
            }
        };
        let field = |i: usize| {
            row[i]
                .clone()
                .ok_or_else(|| Error::io(format!("IDENTIFY_SYSTEM column {} is NULL", i + 1)))
        };
        Ok(SystemIdentity {
            system_id: field(0)?,
            timeline: field(1)?
                .parse()
                .map_err(|_| Error::io("IDENTIFY_SYSTEM timeline is not a number"))?,
            xlogpos: field(2)?.parse().map_err(Error::io)?,
            dbname: row.get(3).cloned().flatten(),
        })
    }

    /// One statement (or several, `;`-separated) over the simple query
    /// protocol: the rows, each value as the type's text output. A logical
    /// replication connection runs plain SQL too (`SHOW`, `SET`, a
    /// `SELECT`), which is what this is for; never `START_REPLICATION`.
    pub async fn simple_query(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        Ok(wire::simple_query(&mut self.wire, sql).await?.rows)
    }

    /// A ParameterStatus value the server reported at startup
    /// (`server_version`, `server_encoding`, `TimeZone`, ...).
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.session
            .params
            .iter()
            .rev()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The server process (walsender) of this connection:
    /// `pg_replication_slots.active_pid` while it streams.
    pub fn backend_pid(&self) -> i32 {
        self.session.backend_pid
    }

    /// Whether the connection runs over TLS.
    pub fn is_tls(&self) -> bool {
        self.wire.is_tls()
    }

    /// Terminate, bounded by `timeout`; never fails loudly. Dropping the
    /// client closes the socket too, but the server then logs an unexpected
    /// EOF.
    pub async fn close(mut self, timeout: Duration) {
        let _ = tokio::time::timeout(timeout, async {
            frontend::terminate(self.wire.wbuf());
            if self.wire.flush().await.is_ok() {
                self.wire.shutdown().await;
            }
        })
        .await;
    }

    /// `START_REPLICATION SLOT <slot> LOGICAL <start> (proto_version '1',
    /// publication_names '<publication>', messages 'true')`. Identifiers are
    /// quoted; the publication name is a string literal (quotes doubled)
    /// holding the quoted identifier, because pgoutput splits the list as
    /// identifiers (an unquoted name would be lower-cased).
    pub async fn start_logical(
        mut self,
        slot: &str,
        start: Lsn,
        publication: &str,
    ) -> Result<ReplicationStream> {
        let sql = start_replication_sql(slot, start, publication);
        frontend::query(&sql, self.wire.wbuf())
            .map_err(|e| Error::config(format!("START_REPLICATION: {e}")))?;
        self.wire.flush().await?;
        loop {
            let f = self.wire.read_frame().await?;
            match f.tag {
                b'W' => {
                    return Ok(ReplicationStream {
                        wire: self.wire,
                        session: self.session,
                        server_done: false,
                        client_done: false,
                    })
                }
                b'E' => return Err(wire::error_response(&f.body)),
                b'N' | b'S' => {}
                t => return Err(wire::unexpected(t, "START_REPLICATION")),
            }
        }
    }
}

fn start_replication_sql(slot: &str, start: Lsn, publication: &str) -> String {
    format!(
        "START_REPLICATION SLOT {} LOGICAL {start} (proto_version '1', publication_names {}, messages 'true')",
        quote_ident(slot),
        quote_literal(&quote_ident(publication)),
    )
}

impl ReplicationStream {
    /// The next frame. `Ok(None)` when the server ended the copy.
    /// CANCELLATION-SAFE: the only await is the socket read, which keeps
    /// what it got in the connection's buffer; a frame is taken out of it
    /// whole and returned in the same poll.
    pub async fn next(&mut self) -> Result<Option<StreamEvent>> {
        if self.server_done {
            return Ok(None);
        }
        loop {
            while let Some(f) = self.wire.take_frame()? {
                match f.tag {
                    b'd' => return copy_data(f.body).map(Some),
                    b'c' => {
                        self.server_done = true;
                        return Ok(None);
                    }
                    b'E' => return Err(wire::error_response(&f.body)),
                    b'N' | b'S' => {}
                    t => return Err(wire::unexpected(t, "replication")),
                }
            }
            self.wire.fill().await?;
        }
    }

    /// One Standby Status Update. `flush` (and `apply`) is what the slot may
    /// advance `confirmed_flush_lsn` to: only what Queen already holds.
    /// A cancelled call leaves the rest of the message queued; the next
    /// `send_status` or `close` sends it first.
    pub async fn send_status(
        &mut self,
        write: Lsn,
        flush: Lsn,
        apply: Lsn,
        reply_requested: bool,
    ) -> Result<()> {
        let now = crate::status::now_us().saturating_sub(PG_EPOCH_OFFSET_US);
        let b = self.wire.wbuf();
        b.put_u8(b'd');
        b.put_i32(4 + 34);
        b.put_u8(b'r');
        b.put_u64(write.0);
        b.put_u64(flush.0);
        b.put_u64(apply.0);
        b.put_i64(now);
        b.put_u8(u8::from(reply_requested));
        self.wire.flush().await
    }

    /// The walsender of this connection (`pg_replication_slots.active_pid`).
    pub fn backend_pid(&self) -> i32 {
        self.session.backend_pid
    }

    /// CopyDone + Terminate, bounded by `timeout`; never fails loudly.
    ///
    /// Between the two it waits for the server to finish the command
    /// (CommandComplete, ReadyForQuery), skipping whatever data was still in
    /// flight: the walsender releases the slot before it answers, so when
    /// `close` returns within the timeout the slot is free for the next
    /// `START_REPLICATION` (otherwise: 55006, "slot is active").
    pub async fn close(mut self, timeout: Duration) {
        let graceful = async {
            if !self.client_done {
                self.client_done = true;
                frontend::copy_done(self.wire.wbuf());
                self.wire.flush().await?;
            }
            loop {
                if self.wire.read_frame().await?.tag == b'Z' {
                    break;
                }
            }
            frontend::terminate(self.wire.wbuf());
            self.wire.flush().await?;
            self.wire.shutdown().await;
            Ok::<_, Error>(())
        };
        let _ = tokio::time::timeout(timeout, graceful).await;
    }
}

/// A CopyData body: `'w'` XLogData or `'k'` primary keepalive.
fn copy_data(body: Bytes) -> Result<StreamEvent> {
    let int = |at: usize| {
        let mut a = [0u8; 8];
        a.copy_from_slice(&body[at..at + 8]);
        u64::from_be_bytes(a)
    };
    let time = |at: usize| (int(at) as i64).saturating_add(PG_EPOCH_OFFSET_US);
    match body.first() {
        Some(b'w') if body.len() >= 25 => Ok(StreamEvent::XLogData {
            wal_start: Lsn(int(1)),
            wal_end: Lsn(int(9)),
            server_time_us: time(17),
            message: pgoutput::decode_bytes(body.slice(25..))?,
        }),
        Some(b'k') if body.len() >= 18 => Ok(StreamEvent::Keepalive {
            wal_end: Lsn(int(1)),
            server_time_us: time(9),
            reply_requested: body[17] != 0,
        }),
        Some(t @ (b'w' | b'k')) => Err(Error::io(format!(
            "the server sent a truncated '{}' CopyData ({} bytes)",
            t.escape_ascii(),
            body.len()
        ))),
        Some(t) => Err(Error::io(format!(
            "the server sent CopyData of unknown kind '{}'",
            t.escape_ascii()
        ))),
        None => Err(Error::io("the server sent an empty CopyData")),
    }
}

#[cfg(test)]
mod tests {
    //! The client against a scripted server on a local socket: every
    //! authentication path, TLS both ways, the replication commands, and the
    //! stream cut at every byte while `next` is being cancelled.

    use std::sync::Arc;

    use bytes::Buf;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::super::pgoutput::decode;
    use super::*;
    use crate::config::SslMode;
    use crate::pg::connect::SESSION_OPTIONS;

    /// Self-signed P-256 certificate for `localhost` and `127.0.0.1`
    /// (CA:FALSE, serverAuth; valid 2026-2126) and its key: the scripted
    /// server's identity, and its own trust anchor for `verify-full`.
    const TEST_CERT: &str = "-----BEGIN CERTIFICATE-----
MIIBcjCCARigAwIBAgIJAJo7mPRGM92sMAoGCCqGSM49BAMCMBQxEjAQBgNVBAMM
CWxvY2FsaG9zdDAgFw0yNjEwMDIxNDQwMzZaGA8yMTI2MDkwODE0NDAzNlowFDES
MBAGA1UEAwwJbG9jYWxob3N0MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE4oHm
f/jZIu/++xurEkb4v+hkQjQIBdli8o7wxc5F1T/CV3AZ/yL2R2cYDzmEs93kOgiW
muAppXq7cxU/A22Fp6NRME8wGgYDVR0RBBMwEYIJbG9jYWxob3N0hwR/AAABMAwG
A1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgeAMBMGA1UdJQQMMAoGCCsGAQUFBwMB
MAoGCCqGSM49BAMCA0gAMEUCIEj+XtMgYqJeNKHXxHzD6MgnyjeXkgLE5SO9Vge/
jpPWAiEA8cgq9F9xs1RCQqpBcgBqkNmoP+JAbp7eh/1J8dzaWWE=
-----END CERTIFICATE-----
";
    const TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgmwIGMKWde66Eyd+9
7oikt5pB6/+pDM05VnCXrj/urCihRANCAATigeZ/+Nki7/77G6sSRvi/6GRCNAgF
2WLyjvDFzkXVP8JXcBn/IvZHZxgPOYSz3eQ6CJaa4CmlertzFT8DbYWn
-----END PRIVATE KEY-----
";

    fn msg(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![tag];
        v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        v.extend_from_slice(body);
        v
    }

    fn auth(code: i32, extra: &[u8]) -> Vec<u8> {
        let mut b = code.to_be_bytes().to_vec();
        b.extend_from_slice(extra);
        msg(b'R', &b)
    }

    fn cstrs(parts: &[&str]) -> Vec<u8> {
        let mut b = Vec::new();
        for p in parts {
            b.extend_from_slice(p.as_bytes());
            b.push(0);
        }
        b
    }

    fn error(code: &str, message: &str) -> Vec<u8> {
        let mut b = cstrs(&["SERROR", &format!("C{code}"), &format!("M{message}")]);
        b.push(0);
        msg(b'E', &b)
    }

    /// AuthenticationOk, two parameters, the backend key, ReadyForQuery.
    fn ready_after_auth() -> Vec<u8> {
        let mut v = auth(0, b"");
        v.extend(msg(b'S', &cstrs(&["server_version", "18.2"])));
        v.extend(msg(b'S', &cstrs(&["integer_datetimes", "on"])));
        let mut k = 4242i32.to_be_bytes().to_vec();
        k.extend_from_slice(&7i32.to_be_bytes());
        v.extend(msg(b'K', &k));
        v.extend(msg(b'Z', b"I"));
        v
    }

    async fn read_untagged<S: AsyncRead + Unpin>(s: &mut S) -> Vec<u8> {
        let len = s.read_i32().await.unwrap() as usize;
        let mut b = vec![0u8; len - 4];
        s.read_exact(&mut b).await.unwrap();
        b
    }

    async fn read_msg<S: AsyncRead + Unpin>(s: &mut S) -> (u8, Vec<u8>) {
        let tag = s.read_u8().await.unwrap();
        (tag, read_untagged(s).await)
    }

    /// The startup packet's parameters, checking the protocol version.
    async fn read_startup<S: AsyncRead + Unpin>(s: &mut S) -> Vec<(String, String)> {
        let b = read_untagged(s).await;
        assert_eq!(&b[..4], &0x0003_0000i32.to_be_bytes(), "protocol 3.0");
        let parts: Vec<String> = b[4..]
            .split(|&c| c == 0)
            .map(|p| String::from_utf8(p.to_vec()).unwrap())
            .collect();
        parts
            .chunks(2)
            .filter(|kv| kv.len() == 2 && !kv[0].is_empty())
            .map(|kv| (kv[0].clone(), kv[1].clone()))
            .collect()
    }

    fn spec(port: u16, ssl_mode: SslMode) -> ConnectionSpec {
        ConnectionSpec {
            url: None,
            host: "127.0.0.1".into(),
            port,
            database: "app".into(),
            user: "cdc".into(),
            password: None,
            password_sealed: None,
            ssl_mode,
            ssl_root_cert: None,
            connect_timeout_ms: 5_000,
        }
    }

    /// A one-connection scripted server; the script gets the accepted socket.
    async fn serve<F, Fut>(script: F) -> (u16, tokio::task::JoinHandle<()>)
    where
        F: FnOnce(TcpStream) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let h = tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            script(s).await
        });
        (port, h)
    }

    async fn connect(
        port: u16,
        mode: SslMode,
        password: Option<&str>,
    ) -> Result<ReplicationClient> {
        ReplicationClient::connect(
            &spec(port, mode),
            password,
            &EgressPolicy::allow_all(),
            "queen-pg/test",
        )
        .await
    }

    /// Answer IDENTIFY_SYSTEM like PostgreSQL 18.
    async fn answer_identify<S: AsyncRead + AsyncWrite + Unpin>(s: &mut S) {
        let (tag, body) = read_msg(s).await;
        assert_eq!((tag, body.as_slice()), (b'Q', &b"IDENTIFY_SYSTEM\0"[..]));
        let mut t = 4u16.to_be_bytes().to_vec();
        for (name, oid) in [
            ("systemid", 25u32),
            ("timeline", 23),
            ("xlogpos", 25),
            ("dbname", 25),
        ] {
            t.extend(cstrs(&[name]));
            t.extend_from_slice(&0u32.to_be_bytes());
            t.extend_from_slice(&0i16.to_be_bytes());
            t.extend_from_slice(&oid.to_be_bytes());
            t.extend_from_slice(&(-1i16).to_be_bytes());
            t.extend_from_slice(&(-1i32).to_be_bytes());
            t.extend_from_slice(&0i16.to_be_bytes());
        }
        let mut d = 4u16.to_be_bytes().to_vec();
        for v in ["7431125789012345678", "1", "0/16B3748", "app"] {
            d.extend_from_slice(&(v.len() as i32).to_be_bytes());
            d.extend_from_slice(v.as_bytes());
        }
        let mut out = msg(b'T', &t);
        out.extend(msg(b'D', &d));
        out.extend(msg(b'C', b"IDENTIFY_SYSTEM\0"));
        out.extend(msg(b'Z', b"I"));
        s.write_all(&out).await.unwrap();
    }

    fn identity() -> SystemIdentity {
        SystemIdentity {
            system_id: "7431125789012345678".into(),
            timeline: 1,
            xlogpos: Lsn(0x16B_3748),
            dbname: Some("app".into()),
        }
    }

    #[tokio::test]
    async fn startup_packet_and_cleartext_password() {
        let (port, server) = serve(|mut s| async move {
            let params = read_startup(&mut s).await;
            let want: Vec<(String, String)> = [
                ("user", "cdc"),
                ("replication", "database"),
                ("application_name", "queen-pg/test"),
                ("options", SESSION_OPTIONS),
                ("client_encoding", "UTF8"),
                ("database", "app"),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
            assert_eq!(params, want);
            s.write_all(&auth(3, b"")).await.unwrap();
            assert_eq!(read_msg(&mut s).await, (b'p', b"s3cret\0".to_vec()));
            s.write_all(&ready_after_auth()).await.unwrap();
            answer_identify(&mut s).await;
        })
        .await;
        let mut c = connect(port, SslMode::Disable, Some("s3cret"))
            .await
            .unwrap();
        assert_eq!(c.parameter("server_version"), Some("18.2"));
        assert_eq!(c.backend_pid(), 4242);
        assert!(!c.is_tls());
        assert_eq!(c.identify_system().await.unwrap(), identity());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn md5_password() {
        let (port, server) = serve(|mut s| async move {
            read_startup(&mut s).await;
            let salt = [1u8, 2, 3, 4];
            s.write_all(&auth(5, &salt)).await.unwrap();
            let want = postgres_protocol::authentication::md5_hash(b"cdc", b"pw", salt);
            let (tag, body) = read_msg(&mut s).await;
            assert_eq!(tag, b'p');
            assert_eq!(body, cstrs(&[&want]));
            s.write_all(&ready_after_auth()).await.unwrap();
        })
        .await;
        connect(port, SslMode::Disable, Some("pw")).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn refusals_are_classified() {
        // A wrong password: the server's 28P01, not retryable.
        let (port, _s) = serve(|mut s| async move {
            read_startup(&mut s).await;
            s.write_all(&auth(3, b"")).await.unwrap();
            read_msg(&mut s).await;
            s.write_all(&error("28P01", "password authentication failed"))
                .await
                .unwrap();
        })
        .await;
        let e = connect(port, SslMode::Disable, Some("nope"))
            .await
            .err()
            .unwrap();
        assert!(
            matches!(&e, Error::Pg { sqlstate: Some(c), transient: false, .. } if c == "28P01"),
            "{e:?}"
        );

        // Out of connection slots: 53300, retryable.
        let (port, _s) = serve(|mut s| async move {
            read_startup(&mut s).await;
            s.write_all(&error("53300", "too many clients already"))
                .await
                .unwrap();
        })
        .await;
        let e = connect(port, SslMode::Disable, None).await.err().unwrap();
        assert!(e.is_retryable(), "{e:?}");

        // A password is asked for and there is none.
        let (port, _s) = serve(|mut s| async move {
            read_startup(&mut s).await;
            s.write_all(&auth(10, &cstrs(&["SCRAM-SHA-256", ""])))
                .await
                .unwrap();
            let _ = s.read_u8().await;
        })
        .await;
        let e = connect(port, SslMode::Disable, None).await.err().unwrap();
        assert!(
            matches!(
                e,
                Error::Pg {
                    sqlstate: None,
                    transient: false,
                    ..
                }
            ),
            "{e:?}"
        );

        // Methods this crate does not speak.
        for (code, extra) in [
            (7, Vec::new()),
            (2, Vec::new()),
            (10, cstrs(&["SCRAM-SHA-256-PLUS", ""])),
            (99, Vec::new()),
        ] {
            let (port, _s) = serve(move |mut s| async move {
                read_startup(&mut s).await;
                s.write_all(&auth(code, &extra)).await.unwrap();
                let _ = s.read_u8().await;
            })
            .await;
            let e = connect(port, SslMode::Disable, Some("pw"))
                .await
                .err()
                .unwrap();
            assert_eq!(e.code(), "auth_method", "{code}: {e:?}");
            assert!(!e.is_retryable());
        }

        // A server that skips SCRAM's final message cannot authenticate
        // itself: refused.
        let (port, _s) = serve(|mut s| async move {
            read_startup(&mut s).await;
            s.write_all(&auth(10, &cstrs(&["SCRAM-SHA-256", ""])))
                .await
                .unwrap();
            read_msg(&mut s).await;
            s.write_all(&auth(0, b"")).await.unwrap();
            let _ = s.read_u8().await;
        })
        .await;
        let e = connect(port, SslMode::Disable, Some("pw"))
            .await
            .err()
            .unwrap();
        assert!(
            matches!(
                e,
                Error::Pg {
                    sqlstate: None,
                    transient: false,
                    ..
                }
            ),
            "{e:?}"
        );
    }

    #[tokio::test]
    async fn unreachable_and_silent_servers_are_transient() {
        // Nothing listens.
        let port = {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap().port()
        };
        let e = connect(port, SslMode::Disable, None).await.err().unwrap();
        assert!(
            matches!(
                e,
                Error::Pg {
                    sqlstate: None,
                    transient: true,
                    ..
                }
            ),
            "{e:?}"
        );

        // Accepts, never answers: bounded by connectTimeoutMs.
        let (port, _s) = serve(|s| async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(s);
        })
        .await;
        let mut sp = spec(port, SslMode::Disable);
        sp.connect_timeout_ms = 200;
        let t0 = std::time::Instant::now();
        let e = ReplicationClient::connect(&sp, None, &EgressPolicy::allow_all(), "t")
            .await
            .err()
            .unwrap();
        assert!(t0.elapsed() < Duration::from_secs(5));
        assert!(
            matches!(
                e,
                Error::Pg {
                    sqlstate: None,
                    transient: true,
                    ..
                }
            ),
            "{e:?}"
        );

        // The egress policy refuses before any packet.
        let strict = EgressPolicy {
            allow_private: false,
        };
        let e = ReplicationClient::connect(&spec(port, SslMode::Disable), None, &strict, "t")
            .await
            .err()
            .unwrap();
        assert_eq!(e.code(), "egress");
    }

    async fn expect_ssl_request(s: &mut TcpStream) {
        let b = read_untagged(s).await;
        assert_eq!(b, 80_877_103i32.to_be_bytes());
    }

    #[tokio::test]
    async fn ssl_prefer_falls_back_and_require_refuses() {
        let (port, server) = serve(|mut s| async move {
            expect_ssl_request(&mut s).await;
            s.write_all(b"N").await.unwrap();
            read_startup(&mut s).await;
            s.write_all(&ready_after_auth()).await.unwrap();
        })
        .await;
        let c = connect(port, SslMode::Prefer, None).await.unwrap();
        assert!(!c.is_tls());
        server.await.unwrap();

        for mode in [SslMode::Require, SslMode::VerifyFull] {
            let (port, _s) = serve(|mut s| async move {
                expect_ssl_request(&mut s).await;
                s.write_all(b"N").await.unwrap();
                let _ = s.read_u8().await;
            })
            .await;
            let e = connect(port, mode, None).await.err().unwrap();
            assert!(
                matches!(
                    e,
                    Error::Pg {
                        sqlstate: None,
                        transient: false,
                        ..
                    }
                ),
                "{e:?}"
            );
        }
    }

    fn tls_acceptor() -> tokio_rustls::TlsAcceptor {
        use rustls::pki_types::pem::PemObject;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        let cert = CertificateDer::from_pem_slice(TEST_CERT.as_bytes()).unwrap();
        let key = PrivateKeyDer::from_pem_slice(TEST_KEY.as_bytes()).unwrap();
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .unwrap();
        tokio_rustls::TlsAcceptor::from(Arc::new(config))
    }

    /// SSLRequest, `S`, the TLS handshake, then a whole session over TLS.
    async fn tls_server(mut s: TcpStream, accept: bool) {
        expect_ssl_request(&mut s).await;
        s.write_all(b"S").await.unwrap();
        let Ok(mut t) = tls_acceptor().accept(s).await else {
            assert!(!accept, "the handshake should have worked");
            return;
        };
        assert!(accept, "the client should have refused the certificate");
        read_startup(&mut t).await;
        t.write_all(&auth(3, b"")).await.unwrap();
        assert_eq!(read_msg(&mut t).await, (b'p', b"pw\0".to_vec()));
        t.write_all(&ready_after_auth()).await.unwrap();
        answer_identify(&mut t).await;
        // Terminate, then close_notify.
        assert_eq!(read_msg(&mut t).await.0, b'X');
    }

    #[tokio::test]
    async fn tls_require_and_verify_full() {
        // require: any certificate, encrypted all the same.
        let (port, server) = serve(|s| tls_server(s, true)).await;
        let mut c = connect(port, SslMode::Require, Some("pw")).await.unwrap();
        assert!(c.is_tls());
        assert_eq!(c.identify_system().await.unwrap(), identity());
        c.close(Duration::from_secs(5)).await;
        server.await.unwrap();

        // verify-full against the certificate itself, by name and by IP.
        for host in ["localhost", "127.0.0.1"] {
            let (port, server) = serve(|s| tls_server(s, true)).await;
            let mut sp = spec(port, SslMode::VerifyFull);
            sp.host = host.into();
            sp.ssl_root_cert = Some(TEST_CERT.into());
            let mut c =
                ReplicationClient::connect(&sp, Some("pw"), &EgressPolicy::allow_all(), "t")
                    .await
                    .unwrap();
            assert_eq!(c.identify_system().await.unwrap(), identity());
            c.close(Duration::from_secs(5)).await;
            server.await.unwrap();
        }

        // verify-full with the public roots: the self-signed certificate is
        // a verdict, not a network failure.
        let (port, server) = serve(|s| tls_server(s, false)).await;
        let mut sp = spec(port, SslMode::VerifyFull);
        sp.host = "localhost".into();
        let e = ReplicationClient::connect(&sp, Some("pw"), &EgressPolicy::allow_all(), "t")
            .await
            .err()
            .unwrap();
        assert!(
            matches!(&e, Error::Pg { sqlstate: None, transient: false, message } if message.contains("TLS")),
            "{e:?}"
        );
        server.await.unwrap();
    }

    fn xlog(wal: u64, payload: &[u8]) -> Vec<u8> {
        let mut b = vec![b'w'];
        b.extend_from_slice(&wal.to_be_bytes());
        b.extend_from_slice(&(wal + 0x100).to_be_bytes());
        b.extend_from_slice(&5_000_000i64.to_be_bytes());
        b.extend_from_slice(payload);
        msg(b'd', &b)
    }

    fn keepalive(wal_end: u64, reply: bool) -> Vec<u8> {
        let mut b = vec![b'k'];
        b.extend_from_slice(&wal_end.to_be_bytes());
        b.extend_from_slice(&6_000_000i64.to_be_bytes());
        b.push(u8::from(reply));
        msg(b'd', &b)
    }

    fn payloads() -> Vec<Vec<u8>> {
        let mut begin = vec![b'B'];
        begin.extend_from_slice(&0x200u64.to_be_bytes());
        begin.extend_from_slice(&1_000i64.to_be_bytes());
        begin.extend_from_slice(&77u32.to_be_bytes());
        let mut rel = vec![b'R'];
        rel.extend_from_slice(&16_390u32.to_be_bytes());
        rel.extend(cstrs(&["public", "t"]));
        rel.push(b'd');
        rel.extend_from_slice(&1i16.to_be_bytes());
        rel.push(1);
        rel.extend(cstrs(&["id"]));
        rel.extend_from_slice(&23u32.to_be_bytes());
        rel.extend_from_slice(&(-1i32).to_be_bytes());
        let mut ins = vec![b'I'];
        ins.extend_from_slice(&16_390u32.to_be_bytes());
        ins.push(b'N');
        ins.extend_from_slice(&1i16.to_be_bytes());
        ins.push(b't');
        let big = "x".repeat(70_000);
        ins.extend_from_slice(&(big.len() as i32).to_be_bytes());
        ins.extend_from_slice(big.as_bytes());
        let mut commit = vec![b'C', 0];
        commit.extend_from_slice(&0x200u64.to_be_bytes());
        commit.extend_from_slice(&0x230u64.to_be_bytes());
        commit.extend_from_slice(&1_000i64.to_be_bytes());
        vec![begin, rel, ins, commit]
    }

    /// START_REPLICATION, a stream fed one byte per write while the client
    /// cancels `next` whenever it would wait, a status update, the server's
    /// CopyDone, and `close`'s orderly end.
    #[tokio::test]
    async fn streaming_survives_cancellation_at_every_byte() {
        let (port, server) = serve(|mut s| async move {
            read_startup(&mut s).await;
            s.write_all(&ready_after_auth()).await.unwrap();
            let (tag, body) = read_msg(&mut s).await;
            assert_eq!(tag, b'Q');
            assert_eq!(
                std::str::from_utf8(&body).unwrap(),
                "START_REPLICATION SLOT \"queen_x\" LOGICAL 0/1A0 (proto_version '1', publication_names '\"Pub''s\"', messages 'true')\0"
            );
            let mut w = 0u16.to_be_bytes().to_vec();
            w.insert(0, 0);
            s.write_all(&msg(b'N', &[b'M', b'x', 0, 0])).await.unwrap();
            s.write_all(&msg(b'W', &w)).await.unwrap();
            let mut stream = Vec::new();
            for (i, p) in payloads().iter().enumerate() {
                stream.extend(xlog(0x100 + i as u64, p));
            }
            stream.extend(keepalive(0x300, true));
            stream.extend(msg(b'S', &cstrs(&["TimeZone", "UTC"])));
            for b in stream {
                s.write_all(&[b]).await.unwrap();
                s.flush().await.unwrap();
                tokio::task::yield_now().await;
            }
            // The status update.
            let (tag, body) = read_msg(&mut s).await;
            assert_eq!(tag, b'd');
            let mut b = &body[..];
            assert_eq!(b.get_u8(), b'r');
            assert_eq!((b.get_u64(), b.get_u64(), b.get_u64()), (0x300, 0x230, 0x230));
            let pg_now = crate::status::now_us() - PG_EPOCH_OFFSET_US;
            assert!((b.get_i64() - pg_now).abs() < 60_000_000);
            assert_eq!(b.get_u8(), 0);
            assert!(b.is_empty());
            // The server ends the copy; the client's close answers it.
            s.write_all(&msg(b'c', b"")).await.unwrap();
            assert_eq!(read_msg(&mut s).await, (b'c', vec![]));
            let mut done = msg(b'C', b"START_STREAMING\0");
            done.extend(msg(b'Z', b"I"));
            s.write_all(&done).await.unwrap();
            assert_eq!(read_msg(&mut s).await, (b'X', vec![]));
            assert_eq!(s.read(&mut [0u8; 1]).await.unwrap(), 0);
        })
        .await;

        let c = connect(port, SslMode::Disable, None).await.unwrap();
        let mut st = c
            .start_logical("queen_x", Lsn(0x1A0), "Pub's")
            .await
            .unwrap();
        assert_eq!(st.backend_pid(), 4242);
        let mut got = Vec::new();
        let mut cancelled = 0u32;
        while got.len() < 5 {
            tokio::select! {
                biased;
                ev = st.next() => got.push(ev.unwrap().unwrap()),
                _ = tokio::task::yield_now() => cancelled += 1,
            }
        }
        assert!(cancelled > 100, "only {cancelled} cancellations");
        let want: Vec<StreamEvent> = payloads()
            .iter()
            .enumerate()
            .map(|(i, p)| StreamEvent::XLogData {
                wal_start: Lsn(0x100 + i as u64),
                wal_end: Lsn(0x200 + i as u64),
                server_time_us: 5_000_000 + PG_EPOCH_OFFSET_US,
                message: decode(p).unwrap(),
            })
            .chain([StreamEvent::Keepalive {
                wal_end: Lsn(0x300),
                server_time_us: 6_000_000 + PG_EPOCH_OFFSET_US,
                reply_requested: true,
            }])
            .collect();
        assert_eq!(got, want);
        st.send_status(Lsn(0x300), Lsn(0x230), Lsn(0x230), false)
            .await
            .unwrap();
        assert_eq!(st.next().await.unwrap(), None);
        assert_eq!(st.next().await.unwrap(), None);
        st.close(Duration::from_secs(5)).await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn stream_errors() {
        // START_REPLICATION refused: the server's error, classified.
        let (port, _s) = serve(|mut s| async move {
            read_startup(&mut s).await;
            s.write_all(&ready_after_auth()).await.unwrap();
            read_msg(&mut s).await;
            let mut out = error("55006", "replication slot \"queen_x\" is active for PID 9");
            out.extend(msg(b'Z', b"I"));
            s.write_all(&out).await.unwrap();
            let _ = s.read_u8().await;
        })
        .await;
        let c = connect(port, SslMode::Disable, None).await.unwrap();
        let e = c.start_logical("queen_x", Lsn(1), "p").await.err().unwrap();
        assert!(
            matches!(&e, Error::Pg { sqlstate: Some(c), transient: true, .. } if c == "55006"),
            "{e:?}"
        );

        // An error mid-stream, a garbage frame, and a dropped connection.
        for (tail, check) in [
            (error("XX000", "boom"), "Pg"),
            (msg(b'd', b"x"), "Io"),
            (msg(b'd', &[b'w', 0, 0]), "Io"),
            (msg(b'Q', b""), "Io"),
            (Vec::new(), "Io"),
        ] {
            let (port, _s) = serve(move |mut s| async move {
                read_startup(&mut s).await;
                s.write_all(&ready_after_auth()).await.unwrap();
                read_msg(&mut s).await;
                s.write_all(&msg(b'W', &[0, 0, 0])).await.unwrap();
                s.write_all(&tail).await.unwrap();
            })
            .await;
            let c = connect(port, SslMode::Disable, None).await.unwrap();
            let mut st = c.start_logical("s", Lsn(1), "p").await.unwrap();
            let e = st.next().await.err().unwrap();
            match check {
                "Pg" => assert!(matches!(e, Error::Pg { .. }), "{e:?}"),
                _ => assert!(matches!(e, Error::Io(_)), "{e:?}"),
            }
            // close() on a broken stream returns, quietly.
            st.close(Duration::from_millis(200)).await;
        }
    }

    #[test]
    fn start_replication_quotes_both_names() {
        assert_eq!(
            start_replication_sql("queen_orders_src", Lsn(0x1_0000_00A0), "queen_orders_src"),
            "START_REPLICATION SLOT \"queen_orders_src\" LOGICAL 1/A0 (proto_version '1', publication_names '\"queen_orders_src\"', messages 'true')"
        );
        assert!(start_replication_sql("s", Lsn(0), "a\"b'c")
            .contains("publication_names '\"a\"\"b''c\"'"));
    }

    #[test]
    fn copy_data_kinds() {
        let ka = |b: &[u8]| copy_data(Bytes::copy_from_slice(b));
        let mut k = vec![b'k'];
        k.extend_from_slice(&5u64.to_be_bytes());
        k.extend_from_slice(&0i64.to_be_bytes());
        k.push(1);
        assert_eq!(
            ka(&k).unwrap(),
            StreamEvent::Keepalive {
                wal_end: Lsn(5),
                server_time_us: PG_EPOCH_OFFSET_US,
                reply_requested: true
            }
        );
        for bad in [
            &b""[..],
            b"k",
            &k[..17],
            b"w\0\0\0",
            b"z",
            b"wxxxxxxxxxxxxxxxxxxxxxxxx",
        ] {
            assert!(matches!(ka(bad), Err(Error::Io(_))), "{bad:?}");
        }
    }
}
