//! Connecting: DNS + the egress policy, TLS (rustls, ring), timeouts, session
//! settings, and the classification of PostgreSQL errors. OWNER: agent C.
//!
//! Shared with the replication client (`repl::wire`): [`resolve`] and
//! [`tls_client_config`], so both connections meet the same policy and the
//! same certificates.
//!
//! Why the connection goes to the CHECKED address: the egress policy is a
//! decision about an IP address, and a host name can resolve differently a
//! millisecond later (DNS rebinding: a public answer for the check, a private
//! one for the connect). [`connect`] therefore resolves once, checks every
//! address, and hands tokio-postgres the addresses as `hostaddr`; the name
//! stays the `host`, which is what TLS uses for SNI and verification.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::WebPkiSupportedAlgorithms;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};
use rustls_pki_types::pem::PemObject;

use crate::config::{ConnectionSpec, SslMode};
use crate::error::{Error, Result};
use crate::LOG_TARGET;

/// The session settings of EVERY connection the crate opens (the startup
/// packet's `options`), so text output is identical on the replication and
/// the regular connection (src/values.rs relies on it).
pub const SESSION_OPTIONS: &str = "-c TimeZone=UTC -c DateStyle=ISO -c IntervalStyle=postgres -c extra_float_digits=3 -c bytea_output=hex";

/// TCP keepalive on every regular connection: idle time before the first
/// probe. The tokio-postgres default is two hours, which is how long a sink
/// would sit on a statement whose server vanished without a RST (a failover,
/// a dropped NAT entry) before it noticed. With 30 s + 3 probes 10 s apart a
/// dead peer is detected in about a minute.
const KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_RETRIES: u32 = 3;

/// `TCP_USER_TIMEOUT` (Linux only; ignored elsewhere): how long written data
/// may stay unacknowledged. Keepalive probes do not run while the send queue
/// is non-empty, so this is what bounds a write into a dead connection. It
/// never fires on a slow statement: the server's kernel acks our bytes at
/// once, however long the statement then runs.
const TCP_USER_TIMEOUT: Duration = Duration::from_secs(60);

/// `QUEEN_PG_ALLOW_PRIVATE_NETWORKS`: whether a host may resolve to a
/// loopback, private (RFC 1918, ULA), link-local, unspecified or CGNAT
/// address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EgressPolicy {
    pub allow_private: bool,
}

impl EgressPolicy {
    pub fn allow_all() -> EgressPolicy {
        EgressPolicy {
            allow_private: true,
        }
    }

    /// Refuse when any address is in a forbidden range.
    ///
    /// ANY, not "drop the bad ones and use the rest": a name that answers
    /// both a public and a private address is exactly the rebinding shape
    /// this policy exists for.
    pub fn check(&self, addrs: &[SocketAddr]) -> Result<()> {
        if self.allow_private {
            return Ok(());
        }
        for a in addrs {
            if let Some(range) = forbidden_range(a.ip()) {
                return Err(egress_refused(&a.ip().to_string(), a.ip(), range));
            }
        }
        Ok(())
    }
}

fn egress_refused(what: &str, ip: IpAddr, range: &str) -> Error {
    let subject = if what == ip.to_string() {
        format!("{ip}")
    } else {
        format!("{what} resolves to {ip}, which")
    };
    Error::fatal(
        "egress",
        format!(
            "{subject} is a {range} address, and this broker refuses private networks \
             (QUEEN_PG_ALLOW_PRIVATE_NETWORKS=false); use a public address, or set \
             QUEEN_PG_ALLOW_PRIVATE_NETWORKS=true on the broker"
        ),
    )
}

/// The forbidden range `ip` falls in, `None` when it may be dialled.
///
/// Beyond the ranges the policy names, `0.0.0.0/8` is refused whole (Linux
/// dials `0.0.0.0` as the local host), and the IPv6 forms that carry an IPv4
/// address are judged by that address: IPv4-mapped `::ffff:a.b.c.d`, the
/// deprecated IPv4-compatible `::a.b.c.d`, and the NAT64 well-known prefix
/// `64:ff9b::/96` (a translator would carry the connection to the embedded
/// IPv4 address, private ones included).
fn forbidden_range(ip: IpAddr) -> Option<&'static str> {
    match ip {
        IpAddr::V4(v4) => forbidden_v4(v4),
        IpAddr::V6(v6) => forbidden_v6(v6),
    }
}

fn forbidden_v4(ip: Ipv4Addr) -> Option<&'static str> {
    let [a, b, _, _] = ip.octets();
    match (a, b) {
        (0, _) => Some("unspecified (0.0.0.0/8)"),
        (127, _) => Some("loopback (127.0.0.0/8)"),
        (10, _) => Some("private (RFC 1918 10.0.0.0/8)"),
        (172, 16..=31) => Some("private (RFC 1918 172.16.0.0/12)"),
        (192, 168) => Some("private (RFC 1918 192.168.0.0/16)"),
        (100, 64..=127) => Some("shared CGNAT (100.64.0.0/10)"),
        (169, 254) => Some("link-local (169.254.0.0/16, cloud metadata)"),
        _ => None,
    }
}

fn forbidden_v6(ip: Ipv6Addr) -> Option<&'static str> {
    if ip.is_unspecified() {
        return Some("unspecified (::)");
    }
    if ip.is_loopback() {
        return Some("loopback (::1)");
    }
    if let Some(v4) = ip.to_ipv4_mapped() {
        return forbidden_v4(v4);
    }
    let s = ip.segments();
    // Deprecated IPv4-compatible `::a.b.c.d` (`::` and `::1` handled above).
    if s[..6] == [0, 0, 0, 0, 0, 0] {
        return forbidden_v4(embedded_v4(&s));
    }
    // NAT64 well-known prefix 64:ff9b::/96.
    if s[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
        return forbidden_v4(embedded_v4(&s));
    }
    match s[0] {
        x if x & 0xffc0 == 0xfe80 => Some("link-local (fe80::/10)"),
        x if x & 0xffc0 == 0xfec0 => Some("site-local (fec0::/10, deprecated)"),
        x if x & 0xfe00 == 0xfc00 => Some("unique local (fc00::/7)"),
        _ => None,
    }
}

fn embedded_v4(s: &[u16; 8]) -> Ipv4Addr {
    Ipv4Addr::new(
        (s[6] >> 8) as u8,
        (s[6] & 0xff) as u8,
        (s[7] >> 8) as u8,
        (s[7] & 0xff) as u8,
    )
}

/// Resolve `host:port` and check every address against `policy`.
///
/// Unbounded in time (the system resolver's own timeouts apply): a caller
/// with a connect budget wraps it, as [`connect`] does. An IP literal
/// resolves to itself without DNS; a bracketed IPv6 literal (`[::1]`, the
/// URL form) is accepted too.
pub async fn resolve(host: &str, port: u16, policy: &EgressPolicy) -> Result<Vec<SocketAddr>> {
    let bare = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    let found = tokio::net::lookup_host((bare, port))
        .await
        .map_err(|e| Error::Pg {
            sqlstate: None,
            message: format!("could not resolve {host}: {e}"),
            transient: true,
        })?;
    let mut addrs: Vec<SocketAddr> = Vec::new();
    for a in found {
        if !addrs.contains(&a) {
            addrs.push(a);
        }
    }
    if addrs.is_empty() {
        return Err(Error::Pg {
            sqlstate: None,
            message: format!("{host} resolves to no address"),
            transient: true,
        });
    }
    if !policy.allow_private {
        for a in &addrs {
            if let Some(range) = forbidden_range(a.ip()) {
                return Err(egress_refused(host, a.ip(), range));
            }
        }
    }
    Ok(addrs)
}

/// The rustls client config for `conn.ssl_mode` (`None` for `disable`).
/// `prefer`/`require`: encrypt, no certificate verification (libpq's
/// meaning); `verify-full`: webpki roots or `ssl_root_cert`, hostname checked.
///
/// libpq semantics, spelled out: `require` (and `prefer` when the server
/// speaks TLS) protects against passive eavesdropping only — any certificate
/// is accepted, so an active man-in-the-middle is NOT stopped. That is what
/// the mode means in libpq too (without a `root.crt`, which a broker has no
/// home directory to hold), and what operators expect when they copy a
/// `sslmode=require` URL. `verify-full` is the mode that authenticates the
/// server: chain to a trusted root AND the certificate names the host.
/// `ssl_root_cert` is used by `verify-full` only (the validator refuses it
/// elsewhere); it REPLACES the webpki roots, as `sslrootcert` does in libpq.
///
/// Every config is built on an explicit `ring` provider: no process-wide
/// default is installed or read (the broker may link other TLS users).
pub fn tls_client_config(conn: &ConnectionSpec) -> Result<Option<Arc<rustls::ClientConfig>>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = match conn.ssl_mode {
        SslMode::Disable => return Ok(None),
        SslMode::Prefer | SslMode::Require | SslMode::VerifyFull => {
            rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
                .with_safe_default_protocol_versions()
                .map_err(|e| Error::config(format!("connection.sslMode: TLS setup failed: {e}")))?
        }
    };
    let config = match conn.ssl_mode {
        SslMode::VerifyFull => {
            let roots = match conn.ssl_root_cert.as_deref() {
                Some(pem) => root_store_from_pem(pem)
                    .map_err(|e| Error::config(format!("connection.sslRootCert: {e}")))?,
                None => RootCertStore {
                    roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
                },
            };
            builder.with_root_certificates(roots).with_no_client_auth()
        }
        _ => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AnyCertificate {
                algorithms: provider.signature_verification_algorithms,
            }))
            .with_no_client_auth(),
    };
    Ok(Some(Arc::new(config)))
}

/// Parse `pem` (one or more `CERTIFICATE` sections) into a root store. The
/// error names what is wrong, for `connection.sslRootCert: …`.
pub fn root_store_from_pem(pem: &str) -> std::result::Result<RootCertStore, String> {
    let mut roots = RootCertStore::empty();
    let mut n = 0usize;
    for cert in CertificateDer::pem_slice_iter(pem.as_bytes()) {
        let cert = cert.map_err(|e| format!("not a valid PEM certificate list ({e})"))?;
        roots
            .add(cert)
            .map_err(|e| format!("certificate {} is not usable as a root ({e})", n + 1))?;
        n += 1;
    }
    if n == 0 {
        return Err("no PEM CERTIFICATE section found".into());
    }
    Ok(roots)
}

/// `sslmode=prefer|require`: accept whatever certificate the server shows
/// (libpq's meaning, see [`tls_client_config`]). The handshake signatures ARE
/// still verified with the presented certificate's key, so the session is a
/// real TLS session with whoever holds that key — it is the identity of that
/// party that is not checked.
#[derive(Debug)]
struct AnyCertificate {
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for AnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// A regular connection with [`SESSION_OPTIONS`]; its connection task is
/// spawned on the current runtime and ends when the client is dropped.
///
/// `connect_timeout_ms` bounds each phase the way libpq's `connect_timeout`
/// does: the name lookup, then every address in turn (TCP connect, TLS,
/// authentication, startup). tokio-postgres alone bounds only the TCP
/// connect, and a server that accepts and then never answers the startup
/// would hang the caller forever.
pub async fn connect(
    conn: &ConnectionSpec,
    password: Option<&str>,
    policy: &EgressPolicy,
    application_name: &str,
) -> Result<tokio_postgres::Client> {
    let budget = Duration::from_millis(conn.connect_timeout_ms.max(1));
    let addrs = tokio::time::timeout(budget, resolve(&conn.host, conn.port, policy))
        .await
        .map_err(|_| Error::Pg {
            sqlstate: None,
            message: format!(
                "resolving {} took longer than connectTimeoutMs ({} ms)",
                conn.host, conn.connect_timeout_ms
            ),
            transient: true,
        })??;

    let host = conn
        .host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(&conn.host);
    let mut cfg = tokio_postgres::Config::new();
    // One (host, hostaddr) pair per checked address: tokio-postgres dials the
    // hostaddr and uses the host for TLS, trying the pairs in order.
    for a in &addrs {
        cfg.host(host);
        cfg.hostaddr(a.ip());
    }
    cfg.port(conn.port)
        .user(&conn.user)
        .dbname(&conn.database)
        .options(SESSION_OPTIONS)
        .application_name(application_name)
        .connect_timeout(budget)
        .keepalives(true)
        .keepalives_idle(KEEPALIVE_IDLE)
        .keepalives_interval(KEEPALIVE_INTERVAL)
        .keepalives_retries(KEEPALIVE_RETRIES)
        .tcp_user_timeout(TCP_USER_TIMEOUT);
    if let Some(p) = password {
        cfg.password(p);
    }

    let total = budget.saturating_mul(addrs.len().clamp(1, 16) as u32);
    let timed_out = || Error::Pg {
        sqlstate: None,
        message: format!(
            "connecting to {}:{} took longer than connectTimeoutMs ({} ms per address)",
            conn.host, conn.port, conn.connect_timeout_ms
        ),
        transient: true,
    };
    let client = match tls_client_config(conn)? {
        None => {
            cfg.ssl_mode(tokio_postgres::config::SslMode::Disable);
            let (client, connection) =
                tokio::time::timeout(total, cfg.connect(tokio_postgres::NoTls))
                    .await
                    .map_err(|_| timed_out())?
                    .map_err(|e| classify(&e))?;
            spawn_connection(connection);
            client
        }
        Some(tls) => {
            cfg.ssl_mode(match conn.ssl_mode {
                SslMode::Prefer => tokio_postgres::config::SslMode::Prefer,
                _ => tokio_postgres::config::SslMode::Require,
            });
            let make = tokio_postgres_rustls::MakeRustlsConnect::new((*tls).clone());
            let (client, connection) = tokio::time::timeout(total, cfg.connect(make))
                .await
                .map_err(|_| timed_out())?
                .map_err(|e| classify(&e))?;
            spawn_connection(connection);
            client
        }
    };
    Ok(client)
}

/// Drive the connection on the current runtime. Its end is logged at debug
/// only: the CLIENT sees the same failure on its next call (as a closed
/// connection), and that is where it is reported and classified.
fn spawn_connection<S, T>(connection: tokio_postgres::Connection<S, T>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::debug!(target: LOG_TARGET, error = %error_text(&e), "postgres connection ended");
        }
    });
}

/// Classify a tokio-postgres error: SQLSTATE classes 08 (connection), 57P01-3
/// (admin shutdown, crash), 53 (resources), 40001/40P01 (serialization,
/// deadlock), 55006 (object in use: a slot held by another walsender) and I/O
/// errors are transient; everything else is not.
///
/// "I/O" is decided on the error's source chain, never on message text: a
/// closed connection, or an `io::Error` of a network kind (refused, reset,
/// aborted, broken pipe, timed out, unexpected EOF, unreachable). An
/// `io::Error` that carries a `rustls::Error` is a certificate or handshake
/// verdict — not transient, the configuration must change — and so are the
/// other kinds (`InvalidInput` from encoding a parameter, say), which would
/// fail the same way again.
pub fn classify(e: &tokio_postgres::Error) -> Error {
    if let Some(db) = e.as_db_error() {
        let code = db.code().code();
        let mut message = db.message().to_string();
        if let Some(h) = db.hint() {
            message.push_str(" (hint: ");
            message.push_str(h);
            message.push(')');
        }
        return Error::Pg {
            sqlstate: Some(code.to_string()),
            message,
            transient: transient_sqlstate(code),
        };
    }
    Error::Pg {
        sqlstate: None,
        message: error_text(e),
        transient: e.is_closed() || transient_io(e),
    }
}

/// The SQLSTATEs [`classify`] calls transient.
pub fn transient_sqlstate(code: &str) -> bool {
    code.starts_with("08")
        || code.starts_with("53")
        || matches!(
            code,
            "57P01" | "57P02" | "57P03" | "40001" | "40P01" | "55006"
        )
}

fn transient_io(e: &tokio_postgres::Error) -> bool {
    use std::error::Error as _;
    use std::io::ErrorKind as K;
    let mut src = e.source();
    while let Some(s) = src {
        if let Some(io) = s.downcast_ref::<std::io::Error>() {
            if io
                .get_ref()
                .is_some_and(|inner| inner.downcast_ref::<rustls::Error>().is_some())
            {
                return false;
            }
            return matches!(
                io.kind(),
                K::ConnectionRefused
                    | K::ConnectionReset
                    | K::ConnectionAborted
                    | K::NotConnected
                    | K::BrokenPipe
                    | K::TimedOut
                    | K::UnexpectedEof
                    | K::Interrupted
                    | K::WouldBlock
                    | K::AddrNotAvailable
                    | K::HostUnreachable
                    | K::NetworkUnreachable
                    | K::NetworkDown
            );
        }
        src = s.source();
    }
    false
}

/// The error with its whole source chain (tokio-postgres's own Display is
/// only the kind: "error connecting to server").
fn error_text(e: &tokio_postgres::Error) -> String {
    use std::error::Error as _;
    let mut out = e.to_string();
    let mut src = e.source();
    while let Some(s) = src {
        let t = s.to_string();
        if !t.is_empty() && !out.ends_with(&t) {
            out.push_str(": ");
            out.push_str(&t);
        }
        src = s.source();
    }
    out
}

/// A self-signed CA certificate (P-256, CN "queen-pg test root", valid
/// 2026-2036), for the PEM tests here and in `config_validate`.
#[cfg(test)]
pub(crate) const TEST_ROOT_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIBUzCB+qADAgECAgkA7kaF8kns7SMwCgYIKoZIzj0EAwIwHTEbMBkGA1UEAwwS
cXVlZW4tcGcgdGVzdCByb290MB4XDTI2MTAwMjE0MTcwMFoXDTM2MDkyOTE0MTcw
MFowHTEbMBkGA1UEAwwScXVlZW4tcGcgdGVzdCByb290MFkwEwYHKoZIzj0CAQYI
KoZIzj0DAQcDQgAEPeDOFnn7virFW4nZHO1jJRFJYCPZvtcV5T4IyiO5cR3T8bP8
Q2E1s9LWVaNQhO3U5jPUXo8Xd6SqI080of3ADaMjMCEwDwYDVR0TAQH/BAUwAwEB
/zAOBgNVHQ8BAf8EBAMCAQYwCgYIKoZIzj0EAwIDSAAwRQIgbOTRbVTU+kqZX7mT
yvsN8fexnRB+nftqYfu2XkRCi+cCIQCj9FfEnWvgpmouBuEumLqk5GcP7iEyEH9v
Fg16jr1tmA==
-----END CERTIFICATE-----
";

#[cfg(test)]
mod tests {
    use super::*;

    fn sa(s: &str) -> SocketAddr {
        let ip: IpAddr = s.parse().unwrap();
        SocketAddr::new(ip, 5432)
    }

    fn strict() -> EgressPolicy {
        EgressPolicy {
            allow_private: false,
        }
    }

    #[test]
    fn the_strict_policy_refuses_every_private_range() {
        for bad in [
            "127.0.0.1",
            "127.255.255.254",
            "0.0.0.0",
            "0.1.2.3",
            "10.0.0.1",
            "10.255.255.255",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "100.64.0.1",
            "100.127.255.255",
            "169.254.169.254",
            "169.254.0.1",
            "::1",
            "::",
            "fe80::1",
            "febf::1",
            "fec0::1",
            "fc00::1",
            "fd12:3456::1",
            "::ffff:127.0.0.1",
            "::ffff:10.1.2.3",
            "::ffff:169.254.169.254",
            "::ffff:192.168.0.1",
            "::127.0.0.1",
            "64:ff9b::a00:1",
            "64:ff9b::7f00:1",
        ] {
            let e = strict().check(&[sa(bad)]).unwrap_err();
            assert_eq!(e.code(), "egress", "{bad}");
            assert!(!e.is_retryable(), "{bad}");
            assert!(
                e.to_string().contains("QUEEN_PG_ALLOW_PRIVATE_NETWORKS"),
                "{bad}: {e}"
            );
        }
    }

    #[test]
    fn the_strict_policy_lets_public_addresses_through() {
        for good in [
            "8.8.8.8",
            "1.1.1.1",
            "172.15.255.255",
            "172.32.0.1",
            "192.169.0.1",
            "100.63.255.255",
            "100.128.0.1",
            "169.253.1.1",
            "11.0.0.1",
            "2001:4860:4860::8888",
            "2606:4700::1111",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "fe00::1",
        ] {
            strict()
                .check(&[sa(good)])
                .unwrap_or_else(|e| panic!("{good}: {e}"));
        }
    }

    #[test]
    fn one_private_address_among_public_ones_refuses_them_all() {
        let e = strict()
            .check(&[sa("8.8.8.8"), sa("10.0.0.5"), sa("1.1.1.1")])
            .unwrap_err();
        assert!(e.to_string().contains("10.0.0.5"), "{e}");
        EgressPolicy::allow_all()
            .check(&[sa("127.0.0.1"), sa("10.0.0.5")])
            .unwrap();
    }

    #[tokio::test]
    async fn resolve_checks_what_it_resolved() {
        let e = resolve("127.0.0.1", 5432, &strict()).await.unwrap_err();
        assert_eq!(e.code(), "egress");
        assert!(e.to_string().contains("127.0.0.1"), "{e}");
        let e = resolve("localhost", 5432, &strict()).await.unwrap_err();
        assert_eq!(e.code(), "egress");
        assert!(e.to_string().contains("localhost resolves to"), "{e}");
        let ok = resolve("127.0.0.1", 5433, &EgressPolicy::allow_all())
            .await
            .unwrap();
        assert_eq!(ok, vec![sa("127.0.0.1").with_port(5433)]);
        let ok = resolve("[::1]", 5432, &EgressPolicy::allow_all())
            .await
            .unwrap();
        assert_eq!(ok, vec![sa("::1")]);
    }

    #[tokio::test]
    async fn an_unresolvable_name_is_a_transient_postgres_error() {
        let e = resolve("no-such-host.invalid", 5432, &EgressPolicy::allow_all())
            .await
            .unwrap_err();
        assert_eq!(e.code(), "postgres");
        assert!(e.is_retryable());
    }

    trait WithPort {
        fn with_port(self, p: u16) -> SocketAddr;
    }
    impl WithPort for SocketAddr {
        fn with_port(mut self, p: u16) -> SocketAddr {
            self.set_port(p);
            self
        }
    }

    fn spec(mode: SslMode) -> ConnectionSpec {
        ConnectionSpec {
            url: None,
            host: "db.example.com".into(),
            port: 5432,
            database: "app".into(),
            user: "u".into(),
            password: None,
            password_sealed: None,
            ssl_mode: mode,
            ssl_root_cert: None,
            connect_timeout_ms: 10_000,
        }
    }

    #[test]
    fn tls_configs_per_mode() {
        assert!(tls_client_config(&spec(SslMode::Disable))
            .unwrap()
            .is_none());
        assert!(tls_client_config(&spec(SslMode::Prefer)).unwrap().is_some());
        assert!(tls_client_config(&spec(SslMode::Require))
            .unwrap()
            .is_some());
        assert!(tls_client_config(&spec(SslMode::VerifyFull))
            .unwrap()
            .is_some());
        let mut bad = spec(SslMode::VerifyFull);
        bad.ssl_root_cert = Some("not a certificate".into());
        let e = tls_client_config(&bad).unwrap_err();
        assert_eq!(e.code(), "config");
        assert!(e.to_string().contains("connection.sslRootCert"), "{e}");
        let mut good = spec(SslMode::VerifyFull);
        good.ssl_root_cert = Some(TEST_ROOT_PEM.into());
        assert!(tls_client_config(&good).unwrap().is_some());
    }

    #[test]
    fn the_pem_parser_wants_at_least_one_certificate() {
        assert!(root_store_from_pem("").is_err());
        assert!(root_store_from_pem(
            "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"
        )
        .is_err());
        let garbled = TEST_ROOT_PEM.replace("MIIBUzCB", "MIIBUzXX");
        assert!(root_store_from_pem(TEST_ROOT_PEM).is_ok());
        let two = format!("{TEST_ROOT_PEM}{TEST_ROOT_PEM}");
        assert_eq!(root_store_from_pem(&two).unwrap().len(), 2);
        assert!(root_store_from_pem(&garbled).is_err());
    }

    #[test]
    fn transient_sqlstates() {
        for t in [
            "08000", "08006", "08P01", "53300", "53100", "57P01", "57P02", "57P03", "40001",
            "40P01", "55006",
        ] {
            assert!(transient_sqlstate(t), "{t}");
        }
        for n in [
            "23505", "42P01", "42501", "28P01", "3D000", "57P04", "57014", "22P02", "55000",
        ] {
            assert!(!transient_sqlstate(n), "{n}");
        }
    }

    #[tokio::test]
    async fn a_refused_connection_is_transient() {
        // Bind and drop a listener: its port now refuses connections.
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        drop(l);
        let mut c = spec(SslMode::Disable);
        c.host = "127.0.0.1".into();
        c.port = port;
        c.connect_timeout_ms = 2_000;
        let e = connect(&c, Some("x"), &EgressPolicy::allow_all(), "t")
            .await
            .unwrap_err();
        assert_eq!(e.code(), "postgres", "{e}");
        assert!(e.is_retryable(), "{e}");
    }

    #[tokio::test]
    async fn a_server_that_never_answers_times_out() {
        // Accepts, then says nothing: tokio-postgres alone would wait forever.
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let hold = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((s, _)) = l.accept().await {
                held.push(s);
            }
        });
        let mut c = spec(SslMode::Disable);
        c.host = "127.0.0.1".into();
        c.port = port;
        c.connect_timeout_ms = 300;
        let t0 = std::time::Instant::now();
        let e = connect(&c, Some("x"), &EgressPolicy::allow_all(), "t")
            .await
            .unwrap_err();
        assert!(t0.elapsed() < Duration::from_secs(5));
        assert!(e.is_retryable(), "{e}");
        assert!(e.to_string().contains("connectTimeoutMs"), "{e}");
        hold.abort();
    }

    #[tokio::test]
    async fn the_egress_policy_runs_before_any_packet() {
        let mut c = spec(SslMode::Disable);
        c.host = "127.0.0.1".into();
        let e = connect(&c, None, &strict(), "t").await.unwrap_err();
        assert_eq!(e.code(), "egress");
    }
}
