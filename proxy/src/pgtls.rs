//! Postgres TLS connector (RUSTFIX item 5).
//!
//! C++ read `PG_USE_SSL` (default false) and `PG_SSL_REJECT_UNAUTHORIZED`
//! (default true) and appended `sslmode=require` / `sslmode=prefer` to the libpq
//! connection string (config.hpp:90-91, 107-120). The Rust pool was hard-wired
//! `NoTls`, so `PG_USE_SSL=true` did nothing.
//!
//! We build a rustls `ClientConfig` on the `ring` provider. When
//! `reject_unauthorized` is true we trust the bundled Mozilla root set
//! (`webpki-roots`); when false we install a verifier that accepts any
//! certificate — the equivalent of libpq `sslmode=require` (encryption without
//! chain validation), which many managed Postgres deployments need because they
//! present a self-signed chain.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio_postgres_rustls::MakeRustlsConnect;

/// Build the TLS connector for the deadpool pool / migration connects. Only
/// called when `PG_USE_SSL=true`. `reject_unauthorized` mirrors
/// `PG_SSL_REJECT_UNAUTHORIZED`: true ⇒ validate the chain against webpki roots,
/// false ⇒ accept any certificate (encrypt-only).
pub fn make_connector(reject_unauthorized: bool) -> MakeRustlsConnect {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = if reject_unauthorized {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("rustls: safe default protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth()
    } else {
        rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .expect("rustls: safe default protocol versions")
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCert { provider }))
            .with_no_client_auth()
    };
    MakeRustlsConnect::new(config)
}

/// A `ServerCertVerifier` that accepts any presented certificate. Gated strictly
/// behind `PG_SSL_REJECT_UNAUTHORIZED=false` — this is the deliberate
/// "encryption without authentication" mode, never the default.
#[derive(Debug)]
struct AcceptAnyCert {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
