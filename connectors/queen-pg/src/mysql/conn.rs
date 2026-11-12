//! MySQL connections: the egress check, TLS, timeouts, and the one error
//! classifier.
//!
//! The host is resolved here and checked against the node's egress policy
//! (the same as Postgres: `QUEEN_PG_ALLOW_PRIVATE_NETWORKS`), and the driver
//! is given the address, never the name, so the check cannot be raced by a
//! second lookup; `verify-full` still checks the certificate against the
//! name. `sslMode` means what it means for Postgres: `disable` plain,
//! `prefer` TLS when the server offers it, `require` TLS without checking
//! the certificate, `verify-full` TLS with the chain and the host name
//! checked (the webpki roots, or `sslRootCert`).

use std::net::SocketAddr;
use std::time::Duration;

use mysql_async::{Conn, OptsBuilder, SslOpts};

use crate::config::{ConnectionSpec, SslMode};
use crate::error::{Error, Result};
use crate::pg::connect::EgressPolicy;

/// Server error numbers worth a retry: too many connections (1040), server
/// shutdown (1053), lock wait timeout (1205), deadlock (1213), query
/// interrupted or killed (1317, 1927), read-only for a moment during a
/// failover (1290, 1836), too many user connections (1203, 1226),
/// statement timeout (3024).
const TRANSIENT: &[u16] = &[
    1040, 1053, 1203, 1205, 1213, 1226, 1290, 1317, 1836, 1927, 3024,
];

/// Classify a driver error.
pub fn classify(e: &mysql_async::Error) -> Error {
    use mysql_async::Error as E;
    match e {
        E::Server(s) => Error::Mysql {
            code: Some(s.code),
            message: s.message.clone(),
            transient: TRANSIENT.contains(&s.code),
        },
        E::Io(io) => Error::Mysql {
            code: None,
            message: format!("connection: {io}"),
            transient: true,
        },
        E::Driver(d) => Error::Mysql {
            code: None,
            message: d.to_string(),
            transient: matches!(
                d,
                mysql_async::DriverError::ConnectionClosed
                    | mysql_async::DriverError::PoolDisconnected
            ),
        },
        other => Error::Mysql {
            code: None,
            message: other.to_string(),
            transient: false,
        },
    }
}

/// The addresses of `c.host`, refused when the policy says so. A lookup
/// failure is retryable (DNS comes back).
pub async fn resolve(c: &ConnectionSpec, policy: &EgressPolicy) -> Result<Vec<SocketAddr>> {
    crate::pg::connect::resolve(&c.host, c.port, policy)
        .await
        .map_err(|e| match e {
            Error::Pg {
                message, transient, ..
            } => Error::Mysql {
                code: None,
                message,
                transient,
            },
            other => other,
        })
}

fn is_ip(host: &str) -> bool {
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<std::net::IpAddr>()
        .is_ok()
}

fn ssl_opts(c: &ConnectionSpec, verify: bool) -> SslOpts {
    if !verify {
        return SslOpts::default()
            .with_danger_accept_invalid_certs(true)
            .with_danger_skip_domain_validation(true);
    }
    let mut o = SslOpts::default();
    if let Some(pem) = &c.ssl_root_cert {
        o = o
            .with_root_certs(vec![pem.as_bytes().to_vec().into()])
            .with_disable_built_in_roots(true);
    }
    if !is_ip(&c.host) {
        // The driver dials the checked address; the certificate must still
        // name the host the document gave.
        o = o.with_danger_tls_hostname_override(Some(c.host.clone()));
    }
    o
}

/// One connection. `setup` runs once it is open (session variables).
pub async fn connect(
    c: &ConnectionSpec,
    password: Option<&str>,
    policy: &EgressPolicy,
    setup: &[String],
) -> Result<Conn> {
    let budget = Duration::from_millis(c.connect_timeout_ms.max(1));
    let addrs = tokio::time::timeout(budget, resolve(c, policy))
        .await
        .map_err(|_| Error::Mysql {
            code: None,
            message: format!("resolving {} took longer than {budget:?}", c.host),
            transient: true,
        })??;
    let mut last = None;
    for addr in addrs {
        let tls = match c.ssl_mode {
            SslMode::Disable => None,
            SslMode::Prefer | SslMode::Require => Some(ssl_opts(c, false)),
            SslMode::VerifyFull => Some(ssl_opts(c, true)),
        };
        match dial(c, password, addr, tls, setup, budget).await {
            Ok(conn) => return Ok(conn),
            // `prefer`: a server without TLS gets a plain connection.
            Err(e)
                if c.ssl_mode == SslMode::Prefer
                    && matches!(&e, Error::Mysql { message, .. } if message.contains("SSL")) =>
            {
                match dial(c, password, addr, None, setup, budget).await {
                    Ok(conn) => return Ok(conn),
                    Err(e) => last = Some(e),
                }
            }
            Err(e) => last = Some(e),
        }
    }
    Err(last.unwrap_or_else(|| Error::Mysql {
        code: None,
        message: format!("{} resolved to no address", c.host),
        transient: true,
    }))
}

async fn dial(
    c: &ConnectionSpec,
    password: Option<&str>,
    addr: SocketAddr,
    tls: Option<SslOpts>,
    setup: &[String],
    budget: Duration,
) -> Result<Conn> {
    let opts = OptsBuilder::default()
        .ip_or_hostname(addr.ip().to_string())
        .tcp_port(addr.port())
        .user(Some(c.user.clone()))
        .pass(password.map(str::to_string))
        .db_name(Some(c.database.clone()))
        // Never a Unix socket, whatever the server says its socket is.
        .prefer_socket(false)
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .tcp_nodelay(true)
        .stmt_cache_size(64)
        .ssl_opts(tls)
        .setup(setup.to_vec());
    match tokio::time::timeout(budget, Conn::new(opts)).await {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(e)) => Err(match classify(&e) {
            // A refused login is the document's to fix; MySQL may still be
            // starting, so it stays retryable.
            Error::Mysql {
                code: Some(1045),
                message,
                ..
            } => Error::Mysql {
                code: Some(1045),
                message: format!("{message} (check connection.user and the password)"),
                transient: true,
            },
            other => other,
        }),
        Err(_) => Err(Error::Mysql {
            code: None,
            message: format!(
                "connecting to {}:{} ({addr}) took longer than {budget:?}",
                c.host, c.port
            ),
            transient: true,
        }),
    }
}
