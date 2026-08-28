//! END TO END for `PG_SSL_ROOT_CERT` (QUEEN-CLOUD spec §0), against a real
//! PostgreSQL serving a certificate signed by a PRIVATE CA — the shape every
//! managed provider presents (Scaleway RDB, Cloud SQL, Aiven) and the reason
//! this variable exists. Unit tests in `src/pgtls.rs` prove the PEM parses;
//! only this proves the parsed anchors are what rustls validates the chain
//! against, through the real `Broker::start` path.
//!
//! ONE test function on purpose: `PG_SSL_ROOT_CERT` is process-global env, and
//! two tests mutating it on cargo's parallel threads would race.
//!
//! ```bash
//! # a CA, and a server certificate for `localhost` signed by it
//! openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
//!     -pkeyopt ec_param_enc:named_curve -sha256 -nodes -days 3650 \
//!     -keyout ca.key -out ca.pem -subj "/CN=Test CA" \
//!     -addext "basicConstraints=critical,CA:TRUE"
//! printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > s.ext
//! openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
//!     -pkeyopt ec_param_enc:named_curve -nodes \
//!     -keyout server.key -out server.csr -subj "/CN=localhost"
//! openssl x509 -req -sha256 -in server.csr -CA ca.pem -CAkey ca.key \
//!     -CAcreateserial -out server.crt -days 3650 -extfile s.ext
//!
//! # the key must be 0600 and owned by the postgres user INSIDE the image,
//! # which a bind mount cannot do without root — so bake it in
//! printf 'FROM postgres:16-alpine\nCOPY server.crt /certs/server.crt\nCOPY server.key /certs/server.key\nRUN chown postgres:postgres /certs/* && chmod 600 /certs/server.key\n' > Dockerfile
//! docker build -t queen-tls-pg:test .
//! docker run --rm -d --name queen-tls-pg -e POSTGRES_PASSWORD=postgres -p 5488:5432 \
//!     queen-tls-pg:test -c ssl=on -c ssl_cert_file=/certs/server.crt -c ssl_key_file=/certs/server.key
//!
//! QUEEN_TLS_TEST_PG=localhost:5488 QUEEN_TLS_TEST_CA=$PWD/ca.pem \
//!     cargo test --test pg_tls_root_cert -- --ignored --nocapture
//! ```
//!
//! Two things that will waste an hour if they are got wrong, both paid for
//! while writing this:
//!
//!   * **`-sha256` on the signing step is not optional** — LibreSSL defaults to
//!     SHA-1 for `x509 -req`, and rustls rejects a SHA-1 signature outright,
//!     with the same opaque "error performing TLS handshake" as an unknown
//!     issuer.
//!   * **`-pkeyopt ec_param_enc:named_curve` is not optional either** — LibreSSL
//!     otherwise writes the EC public key with EXPLICIT curve parameters, which
//!     webpki refuses to parse. `RootCertStore::add` accepts such a certificate
//!     happily (it only extracts the SPKI bytes), so the failure appears only at
//!     handshake time, looking exactly like a wrong CA. RSA sidesteps both.
//!
//! The host must be `localhost`, not `127.0.0.1`: the certificate above carries
//! both, but a hostname is what a real cell uses and is what exercises rustls's
//! DNS-name matching.
//!
//! `QUEEN_TLS_TEST_CA` is a PATH here on purpose — it is test rigging, not the
//! product contract. The product variable takes the PEM CONTENT, which is
//! exactly what this test then puts in `PG_SSL_ROOT_CERT`.

use queen::{Broker, BrokerConfig, StartError};

const OTHER_CA: &str = "-----BEGIN CERTIFICATE-----\n\
MIIBWTCCAQCgAwIBAgIJAKmzX6A030osMAoGCCqGSM49BAMCMB8xHTAbBgNVBAMM\n\
FFF1ZWVuIFRlc3QgUm9vdCBDQSAyMCAXDTI2MDgyNzExMTYxN1oYDzIxMjYwODAz\n\
MTExNjE3WjAfMR0wGwYDVQQDDBRRdWVlbiBUZXN0IFJvb3QgQ0EgMjBZMBMGByqG\n\
SM49AgEGCCqGSM49AwEHA0IABF6twyLh3QAgSqyQuFXN6YQpXqV9UW5xVjHk9QhH\n\
TLKoCVy19qOVzNumm5N3EQVVKDaAnssJkg1dL7nEtgBHV0qjIzAhMA8GA1UdEwEB\n\
/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMCA0cAMEQCIGCfF1DG\n\
B/pG4xgUMEbP3/6vEIKVVA+xzh8lHN6SN1jRAiB/wSDR3RDFzV3Mw4/S8WIJFlvW\n\
cFiWxl/YjudWoYm2+g==\n\
-----END CERTIFICATE-----\n";

fn target() -> (String, u16) {
    let t = std::env::var("QUEEN_TLS_TEST_PG").unwrap_or_else(|_| "localhost:5488".to_string());
    match t.split_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().expect("port")),
        None => (t, 5432),
    }
}

/// A broker configured for TLS against the test database, with every background
/// loop off and no schema applied: the only thing under test is whether the
/// pool's TLS handshake succeeds, and nothing here should write to the target.
fn cfg(host: &str, port: u16, reject: bool) -> BrokerConfig {
    BrokerConfig::new()
        .pg(host, port, "postgres", "postgres", "postgres")
        .pg_use_ssl(true)
        .pg_ssl_reject_unauthorized(reject)
        .apply_schema(false)
        .retention(false)
        .stats_refresh(false)
        .system_metrics(false)
        .log_reports(false)
        .pool_size(2)
}

#[tokio::test]
#[ignore = "requires a TLS-serving postgres and its CA — see the module doc comment"]
async fn pg_ssl_root_cert_verifies_a_private_ca() {
    let (host, port) = target();
    let ca_path = std::env::var("QUEEN_TLS_TEST_CA")
        .expect("QUEEN_TLS_TEST_CA=<path to the CA pem> — see the module doc comment");
    let ca = std::fs::read_to_string(&ca_path).expect("read the CA pem");

    // 1. Malformed CA material: refused BEFORE anything is dialled, and as an
    //    error rather than a process exit, because this is the library path.
    //    (The binary turns the identical check into `obs::fatal`.)
    std::env::set_var("PG_SSL_ROOT_CERT", "/etc/ssl/certs/whatever.pem");
    let e = Broker::start(cfg(&host, port, true)).await.err().expect("must refuse to start");
    match &e {
        StartError::Config(m) => {
            assert!(m.contains("PG_SSL_ROOT_CERT"), "{m}");
            assert!(m.contains("FILE PATH"), "a path must be named as such: {m}");
        }
        other => panic!("malformed CA must be StartError::Config, got {other}"),
    }
    println!("malformed CA     -> {e}");

    // 2. The status quo ante: a private chain against the compiled-in Mozilla
    //    set. This is why every cell so far shipped with the escape hatch.
    std::env::remove_var("PG_SSL_ROOT_CERT");
    let e = Broker::start(cfg(&host, port, true)).await.err().expect("webpki-roots must refuse");
    assert!(matches!(e, StartError::Connect(_)), "expected a connect failure, got {e}");
    println!("no CA            -> {e}");

    // 3. The negative control, FIRST: an unrelated CA must not open this link.
    //    Without it, everything below would also pass against an implementation
    //    that ignored the variable and quietly turned verification off.
    std::env::set_var("PG_SSL_ROOT_CERT", OTHER_CA);
    let e = Broker::start(cfg(&host, port, true)).await.err().expect("wrong CA must refuse");
    assert!(matches!(e, StartError::Connect(_)), "expected a connect failure, got {e}");
    println!("wrong CA         -> {e}");

    // 4. The decision this feature rests on: a supplied CA OUTRANKS the escape
    //    hatch, so a wrong CA still fails with reject_unauthorized=false. If the
    //    hatch won instead, this would connect — and the fix would be silently
    //    inert on exactly the cells that carry the flag from before the upgrade.
    let e = Broker::start(cfg(&host, port, false))
        .await
        .err()
        .expect("PG_SSL_REJECT_UNAUTHORIZED=false must not rescue a wrong CA");
    assert!(matches!(e, StartError::Connect(_)), "expected a connect failure, got {e}");
    println!("wrong CA + hatch -> {e}");

    // 5. The fix.
    std::env::set_var("PG_SSL_ROOT_CERT", &ca);
    let broker = Broker::start(cfg(&host, port, true))
        .await
        .expect("the supplied CA must validate the chain");
    println!("supplied CA      -> connected");
    broker.shutdown().await;

    // 6. And the escape hatch alone is unchanged, for anyone who has not
    //    migrated yet — this must stay a pure addition.
    std::env::remove_var("PG_SSL_ROOT_CERT");
    let broker = Broker::start(cfg(&host, port, false))
        .await
        .expect("reject_unauthorized=false alone is unchanged");
    println!("hatch only       -> connected");
    broker.shutdown().await;

    println!("\nOK — the supplied CA is the CA that is trusted, and the only one.");
}
