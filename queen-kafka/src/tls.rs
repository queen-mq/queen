//! The optional TLS listener, and the one thing it is here to carry besides
//! encryption: the client's SNI.
//!
//! `QUEEN_KAFKA_TLS_CERT` + `QUEEN_KAFKA_TLS_KEY` switch the Kafka port from
//! plaintext to TLS. Both or neither, checked at boot ([`crate::Facade`]'s
//! binary does it in `main.rs`): a half-configured listener is a
//! misconfiguration worth failing on, never a silent fallback to the plaintext
//! port — the same rule and the same pair of variables the proxy's own origin
//! listener uses (proxy/src/config.rs `tls_material`).
//!
//! ## SNI is routing, not decoration
//!
//! A Kafka client has no `Host` header. Its whole addressing is
//! `bootstrap.servers`, and the only place the name it dialled survives into
//! the connection is the TLS SNI extension. The Cloud proxy routes by host —
//! the first DNS label of `Host` names the cluster, unless the name is one of
//! `QUEEN_PROXY_SHARED_HOSTS`, where the credential names it instead
//! (proxy/src/acting.rs, decision z). So a facade fronting many tenants behind
//! one address has exactly one piece of evidence about which name a connection
//! asked for, and this module is where it is captured
//! ([`server_name`]). `QUEEN_KAFKA_FORWARD_SNI_HOST` is what turns it from a
//! log field into the `Host` header of that connection's calls to Queen
//! ([`crate::queen::QueenApi::with_host`]).
//!
//! No ALPN protocol is advertised. Kafka's wire protocol has no registered ALPN
//! id and no client offers one, so advertising anything would be inventing a
//! negotiation that clients would fail rather than skip.
//!
//! The PEM handling is a copy of the proxy's listener path (proxy/src/main.rs
//! `pem_blocks`), deliberately: base64 and `rustls::pki_types` are already in
//! the tree, the four labels are the four openssl emits, and one more crate for
//! forty lines is a dependency to keep in step forever.

use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as B64_STD;
use base64::Engine as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpStream;

/// The rustls `ServerConfig` for a PEM certificate chain and its key, read from
/// the two configured paths.
///
/// Every failure names the variable that named the file, because every one of
/// them is an operator's boot and none of them is recoverable at runtime.
pub fn server_config(cert_path: &str, key_path: &str) -> Result<rustls::ServerConfig, String> {
    let cert_pem = std::fs::read_to_string(cert_path)
        .map_err(|e| format!("QUEEN_KAFKA_TLS_CERT={cert_path} cannot be read: {e}"))?;
    let key_pem = std::fs::read_to_string(key_path)
        .map_err(|e| format!("QUEEN_KAFKA_TLS_KEY={key_path} cannot be read: {e}"))?;
    let chain =
        certificates(&cert_pem).map_err(|e| format!("QUEEN_KAFKA_TLS_CERT={cert_path}: {e}"))?;
    let key = private_key(&key_pem).map_err(|e| format!("QUEEN_KAFKA_TLS_KEY={key_path}: {e}"))?;
    config_from(chain, key)
}

/// The same, from material already in memory. The seam the tests use, and the
/// one place the rustls builder is spelled.
pub fn config_from(
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<rustls::ServerConfig, String> {
    // The ring provider explicitly rather than the process default: `reqwest`
    // brings its own rustls into this binary for the Queen side, and a config
    // built from whatever happened to be installed first is a config nobody
    // chose. Same provider and same spelling as server/src/pgtls.rs and the
    // proxy's listener, so the binary has one crypto backend.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("rustls could not take its safe default protocol versions: {e}"))?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|e| {
            format!(
                "the certificate and the key do not make a usable listener: {e} \
                 (the usual cause is a key that does not belong to the certificate, or a chain \
                 whose LEAF is not the first block in the file)"
            )
        })
}

/// The SNI this connection asked for, lowercased by rustls, or `None` when the
/// client sent none.
///
/// A client that dials an IP address sends no SNI at all — TLS forbids it — so
/// `None` is an ordinary answer and not an error: it means "this connection
/// named no host", and the facade then reaches Queen the way a non-TLS one
/// does.
pub fn server_name(stream: &tokio_rustls::server::TlsStream<TcpStream>) -> Option<String> {
    stream.get_ref().1.server_name().map(str::to_string)
}

/// The full chain: leaf first, then any intermediates. A chain file truncated
/// to its first block is a listener that fails only for clients that do not
/// already hold the intermediate, which is the worst kind of half-working.
fn certificates(pem: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let chain: Vec<CertificateDer<'static>> = blocks(pem)
        .into_iter()
        .filter(|(label, _)| label == "CERTIFICATE")
        .map(|(_, der)| CertificateDer::from(der))
        .collect();
    if chain.is_empty() {
        return Err("no CERTIFICATE block found in the file".to_string());
    }
    Ok(chain)
}

/// The first private key in the file, in any of the three encodings openssl
/// emits.
fn private_key(pem: &str) -> Result<PrivateKeyDer<'static>, String> {
    for (label, der) in blocks(pem) {
        match label.as_str() {
            "PRIVATE KEY" => return Ok(PrivateKeyDer::Pkcs8(der.into())),
            "RSA PRIVATE KEY" => return Ok(PrivateKeyDer::Pkcs1(der.into())),
            "EC PRIVATE KEY" => return Ok(PrivateKeyDer::Sec1(der.into())),
            _ => {}
        }
    }
    Err("no PRIVATE KEY, RSA PRIVATE KEY or EC PRIVATE KEY block found in the file".to_string())
}

/// `(label, DER)` for every well-formed PEM block, skipping anything that is
/// not one — a `subject=`/`issuer=` preamble, a comment, a trailing note.
fn blocks(pem: &str) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let mut label: Option<String> = None;
    let mut body = String::new();
    for line in pem.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("-----BEGIN ") {
            label = rest.strip_suffix("-----").map(str::to_string);
            body.clear();
        } else if line.starts_with("-----END ") {
            if let Some(l) = label.take() {
                if let Ok(der) = B64_STD.decode(body.as_bytes()) {
                    out.push((l, der));
                }
            }
            body.clear();
        } else if label.is_some() {
            body.push_str(line);
        }
    }
    out
}

/// A self-signed certificate and its key, for the tests of this crate.
///
/// Generated once with `openssl req -x509` over a P-256 key and valid until
/// 2126, so no test can start failing on a calendar. Nothing outside `cfg(test)`
/// can reach it, and the key is a throwaway that has never protected anything —
/// it exists so the listener tests exercise a REAL handshake rather than a
/// mocked one.
#[cfg(test)]
pub mod testing {
    use super::*;

    pub const CERT_PEM: &str = "\
-----BEGIN CERTIFICATE-----
MIIBdjCCARygAwIBAgIJANV41LV1o/gQMAoGCCqGSM49BAMCMBsxGTAXBgNVBAMM
EHF1ZWVuLWthZmthIHRlc3QwIBcNMjYwODI4MTIyNTUwWhgPMjEyNjA4MDQxMjI1
NTBaMBsxGTAXBgNVBAMMEHF1ZWVuLWthZmthIHRlc3QwWTATBgcqhkjOPQIBBggq
hkjOPQMBBwNCAARdlget+89VGHXfC7zA6HlCeeMRV8T2UH1h3o5riGO/933SZktL
1Fl0o838Kzs3ZUnJWgwUkWbCUNrpEKDwcuddo0cwRTBDBgNVHREEPDA6ghFrYWZr
YS5leGFtcGxlLmNvbYIUc2hhcmVkLnF1ZWVubXEuY2xvdWSCCWxvY2FsaG9zdIcE
fwAAATAKBggqhkjOPQQDAgNIADBFAiAzXdkkGP0Am063H8DWaT+sL7sWM1xF0HU6
aTUORp7DYAIhAKif63VkTwMYq6m9kSchU9hZcBU0TsuRZvShR/xhYBsb
-----END CERTIFICATE-----
";

    pub const KEY_PEM: &str = "\
-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgI098cQnkRLhtJQ7E
VlRmM9Z/lkDwtVnX9JXBqCkBUi6hRANCAARdlget+89VGHXfC7zA6HlCeeMRV8T2
UH1h3o5riGO/933SZktL1Fl0o838Kzs3ZUnJWgwUkWbCUNrpEKDwcudd
-----END PRIVATE KEY-----
";

    /// The listener config the tests serve with.
    pub fn server() -> rustls::ServerConfig {
        config_from(
            certificates(CERT_PEM).expect("the test certificate parses"),
            private_key(KEY_PEM).expect("the test key parses"),
        )
        .expect("the test certificate and key make a listener")
    }

    /// A client config that verifies nothing.
    ///
    /// The certificate above is self-signed and these tests are about the
    /// listener — the handshake completing, the SNI arriving, the framing
    /// surviving — not about a chain of trust that would only be exercising
    /// `webpki`. Confined to `cfg(test)`, like the proxy's own `AcceptAnyCert`.
    pub fn client() -> rustls::ClientConfig {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .expect("rustls: safe default protocol versions")
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCert { provider }))
            .with_no_client_auth()
    }

    #[derive(Debug)]
    struct AcceptAnyCert {
        provider: Arc<rustls::crypto::CryptoProvider>,
    }

    impl rustls::client::danger::ServerCertVerifier for AcceptAnyCert {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &rustls::pki_types::ServerName<'_>,
            _ocsp_response: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls12_signature(
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
            dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls13_signature(
                message,
                cert,
                dss,
                &self.provider.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            self.provider
                .signature_verification_algorithms
                .supported_schemes()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_material_parses_into_a_listener() {
        assert_eq!(certificates(testing::CERT_PEM).unwrap().len(), 1);
        assert!(private_key(testing::KEY_PEM).is_ok());
        // The whole point: the pair makes a config, which is where a key that
        // does not match its certificate would be caught.
        testing::server();
    }

    /// A file with the two blocks the other way round, with a preamble, and
    /// with CRLF line endings — all three of which reach an operator's disk.
    #[test]
    fn the_reader_survives_the_shapes_a_real_file_has() {
        let bundle = format!(
            "subject=CN = queen-kafka test\nissuer=self\n{}\n# a note\n{}",
            testing::CERT_PEM,
            testing::KEY_PEM
        );
        assert_eq!(certificates(&bundle).unwrap().len(), 1);
        assert!(private_key(&bundle).is_ok());

        let crlf = testing::CERT_PEM.replace('\n', "\r\n");
        assert_eq!(certificates(&crlf).unwrap().len(), 1);
    }

    /// Every refusal says which of the two files is wrong and what was in it,
    /// because that is the whole diagnosis an operator gets at boot.
    #[test]
    fn the_wrong_file_in_the_wrong_variable_says_so() {
        let e = certificates(testing::KEY_PEM).unwrap_err();
        assert!(e.contains("no CERTIFICATE block"), "{e}");
        let e = private_key(testing::CERT_PEM).unwrap_err();
        assert!(e.contains("no PRIVATE KEY"), "{e}");
        // Truncated by an env file or a chat window: no block closes, so
        // nothing is found, and the message is the same honest one.
        assert!(certificates("-----BEGIN CERTIFICATE-----").is_err());
    }

    #[test]
    fn a_missing_file_names_the_variable_that_named_it() {
        let e = server_config("/nonexistent/cert.pem", "/nonexistent/key.pem").unwrap_err();
        assert!(e.contains("QUEEN_KAFKA_TLS_CERT"), "{e}");
        assert!(e.contains("/nonexistent/cert.pem"), "{e}");
    }
}
