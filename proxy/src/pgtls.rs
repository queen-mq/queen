//! Postgres TLS connector (RUSTFIX item 5).
//!
//! C++ read `PG_USE_SSL` (default false) and `PG_SSL_REJECT_UNAUTHORIZED`
//! (default true) and appended `sslmode=require` / `sslmode=prefer` to the libpq
//! connection string (config.hpp:90-91, 107-120). The Rust pool was hard-wired
//! `NoTls`, so `PG_USE_SSL=true` did nothing.
//!
//! We build a rustls `ClientConfig` on the `ring` provider, with exactly three
//! trust policies — see `trust_policy`, which is the whole decision:
//!
//! | root-CA PEM supplied | reject_unauthorized | trust                        |
//! |----------------------|---------------------|------------------------------|
//! | no                   | true (default)      | compiled-in Mozilla set      |
//! | no                   | false               | ANY certificate (encrypt only)|
//! | **yes**              | either              | **ONLY the supplied CA**     |
//!
//! ## Why the supplied CA REPLACES the Mozilla set rather than joining it
//!
//! A `RootCertStore` is a flat set of trust anchors and a chain validates if it
//! reaches ANY of them, so "webpki + mine" would mean every public CA on earth
//! keeps its power to issue a certificate this process accepts for the database
//! host. The whole reason the operator supplied a CA is that the database is
//! signed by a private one; a publicly-trusted certificate for that host is
//! therefore not a fallback, it is an impersonation. Replacing also matches
//! libpq's `sslrootcert`, which every Postgres operator already knows, and it
//! keeps the rule statable in one line: **the CA you name is the CA you trust.**
//! A managed provider whose chain IS publicly trusted needs no variable at all.
//!
//! ## Why the CA outranks `*_SSL_REJECT_UNAUTHORIZED=false`
//!
//! The escape hatch exists only because there was no way to supply a CA. Once
//! there is one, honouring `false` over it would mean the security fix silently
//! does nothing on precisely the cells that carry the leftover escape hatch from
//! before the upgrade. So a supplied CA turns verification ON, and each service's
//! config warns at boot that the escape hatch has become dead weight. The failure
//! mode of getting the CA wrong is a loud connect error at boot, not a downgrade.
//!
//! ## The variable holds PEM CONTENT, not a path
//!
//! `PG_SSL_ROOT_CERT` (broker) / `PXDB_SSL_ROOT_CERT` (proxy) — same family, same
//! shape as every other credential in the contract (`PG_PASSWORD`,
//! `QUEEN_PROXY_JWT_ED25519_PEM`): the value IS the material. A CA certificate is
//! public, so it travels in a secret store, a Helm value, or a compose file
//! without ceremony.
//!
//! **Trap: a multi-line value does not survive `docker --env-file`** (nor
//! systemd's `EnvironmentFile`) — both are line-oriented and stop the value at
//! the first newline, leaving `-----BEGIN CERTIFICATE-----` alone in the
//! variable. Use `docker run -e PG_SSL_ROOT_CERT="$(cat ca.pem)"`, a compose
//! `environment:` block scalar, or a Kubernetes secret — or pass the PEM on one
//! line with `\n` for the newlines, which `parse_pem_certificates` un-escapes
//! (a real PEM never contains a backslash, so the un-escaping cannot corrupt
//! one).
//!
//! ## This file is DUPLICATED, verbatim
//!
//! `server/src/pgtls.rs` and `proxy/src/pgtls.rs` are byte-identical, on purpose:
//! the broker and the proxy must not be able to disagree about what the database
//! path trusts. It references nothing from either crate — only `rustls`,
//! `tokio-postgres-rustls`, `webpki-roots` and `base64`, which both already
//! depend on — so a change is copied across unmodified. Verify with
//! `diff server/src/pgtls.rs proxy/src/pgtls.rs`. Only the env var NAMES differ,
//! and those live in each service's config module, never here.

use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as B64_STD;
use base64::Engine;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio_postgres_rustls::MakeRustlsConnect;

/// What the connector will trust, decided from the two knobs alone.
///
/// Pure and exhaustive so both services can print the policy at boot and know it
/// is the one `make_connector` will actually install — a boot line that is
/// derived separately from the behaviour it describes is a boot line that can
/// lie.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrustPolicy {
    /// The compiled-in Mozilla set (`webpki-roots`). The default, and what a
    /// publicly-trusted certificate needs.
    WebpkiRoots,
    /// ONLY the operator-supplied CA (see the module header for why it replaces
    /// rather than joins). `escape_hatch_redundant` is true when
    /// `*_SSL_REJECT_UNAUTHORIZED=false` was ALSO set: the CA wins, the flag now
    /// does nothing, and the operator should be told so.
    SuppliedCa { escape_hatch_redundant: bool },
    /// Accept any certificate — encryption WITHOUT authentication, the libpq
    /// `sslmode=require` equivalent. Reachable only by explicitly setting
    /// `*_SSL_REJECT_UNAUTHORIZED=false` and supplying no CA.
    AcceptAny,
}

impl TrustPolicy {
    /// Stable token for the boot log / metrics.
    pub fn as_str(&self) -> &'static str {
        match self {
            TrustPolicy::WebpkiRoots => "webpki-roots",
            TrustPolicy::SuppliedCa { .. } => "supplied-ca",
            TrustPolicy::AcceptAny => "accept-any",
        }
    }

    /// True when the connection is authenticated as well as encrypted.
    pub fn verifies(&self) -> bool {
        !matches!(self, TrustPolicy::AcceptAny)
    }
}

/// The trust decision. See the table in the module header.
pub fn trust_policy(has_root_ca: bool, reject_unauthorized: bool) -> TrustPolicy {
    match (has_root_ca, reject_unauthorized) {
        (true, reject) => TrustPolicy::SuppliedCa { escape_hatch_redundant: !reject },
        (false, true) => TrustPolicy::WebpkiRoots,
        (false, false) => TrustPolicy::AcceptAny,
    }
}

/// The one-word answer to "what does this process trust on its PostgreSQL
/// link?", for the boot log. `plaintext` when TLS is off entirely — reporting a
/// trust policy for a connection that negotiates no TLS at all would be the
/// most misleading line in the file.
pub fn trust_label(use_ssl: bool, has_root_ca: bool, reject_unauthorized: bool) -> &'static str {
    if !use_ssl {
        return "plaintext";
    }
    trust_policy(has_root_ca, reject_unauthorized).as_str()
}

/// Is that link AUTHENTICATED, not merely encrypted? The single boolean a
/// security review wants out of the boot log; false for plaintext and for the
/// deliberate accept-any mode.
pub fn link_authenticated(use_ssl: bool, has_root_ca: bool, reject_unauthorized: bool) -> bool {
    use_ssl && trust_policy(has_root_ca, reject_unauthorized).verifies()
}

/// One service's names for the three Postgres TLS knobs. The only thing that
/// differs between the broker and the proxy, so it is the only thing passed in —
/// everything else about the policy is decided by the shared code below, which
/// is how the two services are kept from drifting into different rules for the
/// same question.
#[derive(Clone, Copy, Debug)]
pub struct TlsVars {
    pub use_ssl: &'static str,
    pub reject_unauthorized: &'static str,
    pub root_cert: &'static str,
}

/// The broker's names (`server/src/config.rs`).
///
/// Both constants live in both copies of this file on purpose — the pairing is
/// documented in one place, and the file stays byte-identical. Exactly one of
/// them is unused in each crate, hence the `allow`.
#[allow(dead_code)]
pub const BROKER_VARS: TlsVars = TlsVars {
    use_ssl: "PG_USE_SSL",
    reject_unauthorized: "PG_SSL_REJECT_UNAUTHORIZED",
    root_cert: "PG_SSL_ROOT_CERT",
};

/// The proxy's names for the same three knobs against the pxdb
/// (`proxy/src/config.rs`). See `BROKER_VARS` for why both are here.
#[allow(dead_code)]
pub const PROXY_VARS: TlsVars = TlsVars {
    use_ssl: "PXDB_USE_SSL",
    reject_unauthorized: "PXDB_SSL_REJECT_UNAUTHORIZED",
    root_cert: "PXDB_SSL_ROOT_CERT",
};

/// The one boot-time warning about a configuration that is LEGAL but almost
/// certainly not what the operator meant. `None` = nothing to say.
///
/// Not fatal, in all three cases, because each one is a running deployment's
/// status quo: the job here is to make the gap between what the operator
/// believes and what the process does impossible to miss in the boot log, not
/// to refuse to start a cell that was working yesterday.
pub fn boot_advisory(
    use_ssl: bool,
    has_root_ca: bool,
    reject_unauthorized: bool,
    v: TlsVars,
) -> Option<String> {
    // Loudest first: the operator supplied a CA, so they believe the link is
    // verified — and it is not even encrypted.
    if has_root_ca && !use_ssl {
        return Some(format!(
            "{} is set but {} is false — the connection to PostgreSQL is PLAINTEXT and the CA is \
             not used by anything. Set {}=true.",
            v.root_cert, v.use_ssl, v.use_ssl
        ));
    }
    if !use_ssl {
        return None;
    }
    match trust_policy(has_root_ca, reject_unauthorized) {
        TrustPolicy::SuppliedCa { escape_hatch_redundant: true } => Some(format!(
            "{}=false is now unnecessary and is being IGNORED: {} supplies a CA, so the chain is \
             verified against it. The escape hatch existed only because there was no way to \
             supply a CA — unset it, and this line goes away.",
            v.reject_unauthorized, v.root_cert
        )),
        TrustPolicy::SuppliedCa { .. } => None,
        TrustPolicy::WebpkiRoots => None,
        TrustPolicy::AcceptAny => Some(format!(
            "{}=false: the PostgreSQL connection is ENCRYPTED BUT NOT AUTHENTICATED — any \
             certificate is accepted, so anything that can intercept the connection can read and \
             rewrite it. This was the only way to reach a managed database on a private CA; it no \
             longer is. Put that CA's PEM in {} and unset this flag.",
            v.reject_unauthorized, v.root_cert
        )),
    }
}

/// Build the TLS connector for the deadpool pool / migration connects. Only
/// called when `PG_USE_SSL` / `PXDB_USE_SSL` is true.
///
/// `Err` iff a `root_ca_pem` was supplied and is unusable — the caller turns
/// that into a boot failure. It is never a silent fall-back to the webpki set:
/// that would be a downgrade with no signal, which is the exact class of bug the
/// `reject_unauthorized` default of `true` exists to prevent.
pub fn make_connector(
    reject_unauthorized: bool,
    root_ca_pem: Option<&str>,
) -> Result<MakeRustlsConnect, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = match trust_policy(root_ca_pem.is_some(), reject_unauthorized) {
        TrustPolicy::SuppliedCa { .. } => {
            // `trust_policy` returns this arm only for `Some`; the empty string
            // is not a silent "trust nothing", `root_store_from_pem` rejects it.
            let roots = root_store_from_pem(root_ca_pem.unwrap_or(""))?;
            rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .expect("rustls: safe default protocol versions")
                .with_root_certificates(roots)
                .with_no_client_auth()
        }
        TrustPolicy::WebpkiRoots => {
            let mut roots = RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .expect("rustls: safe default protocol versions")
                .with_root_certificates(roots)
                .with_no_client_auth()
        }
        TrustPolicy::AcceptAny => rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .expect("rustls: safe default protocol versions")
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCert { provider }))
            .with_no_client_auth(),
    };
    Ok(MakeRustlsConnect::new(config))
}

/// A `RootCertStore` holding exactly the CAs in `pem`, or a message naming what
/// is wrong with it. Every failure here is fatal at boot in both services, so
/// the messages are written for an operator reading one line of a crashed
/// container's log, not for a developer with the file in front of them.
pub fn root_store_from_pem(pem: &str) -> Result<RootCertStore, String> {
    let ders = parse_pem_certificates(pem)?;
    let total = ders.len();
    let mut roots = RootCertStore::empty();
    for (i, der) in ders.into_iter().enumerate() {
        roots.add(CertificateDer::from(der)).map_err(|e| {
            format!(
                "CERTIFICATE block {} of {total} decoded from base64 but is not a usable X.509 \
                 trust anchor: {e}",
                i + 1
            )
        })?;
    }
    Ok(roots)
}

/// DER bodies of every `CERTIFICATE` block, in file order.
///
/// STRICT on purpose. The listener's `pem_blocks` (proxy main.rs) silently drops
/// a block whose base64 will not decode, which is right for "find the key in
/// this file" and wrong for a boot validator: silence is how a truncated CA
/// turns into an unexplainable TLS error three layers down.
fn parse_pem_certificates(pem: &str) -> Result<Vec<Vec<u8>>, String> {
    // One-line form: `-----BEGIN CERTIFICATE-----\nMIIC...` as LITERAL
    // backslash-n, which is how a PEM survives a line-oriented env file. A real
    // PEM contains no backslash, so this cannot corrupt a well-formed value.
    let normalized = pem.replace("\\n", "\n");
    let mut out: Vec<Vec<u8>> = Vec::new();
    let mut other_labels: Vec<String> = Vec::new();
    let mut open: Option<(String, usize)> = None;
    let mut body = String::new();

    for (idx, line) in normalized.lines().enumerate() {
        let lineno = idx + 1;
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("-----BEGIN ") {
            let label = rest.trim_end_matches('-').trim().to_string();
            if let Some((open_label, open_line)) = &open {
                return Err(format!(
                    "line {lineno}: -----BEGIN {label}----- while the {open_label} block opened on \
                     line {open_line} is still unterminated"
                ));
            }
            open = Some((label, lineno));
            body.clear();
        } else if let Some(rest) = line.strip_prefix("-----END ") {
            let end_label = rest.trim_end_matches('-').trim().to_string();
            let (label, open_line) = open.take().ok_or_else(|| {
                format!("line {lineno}: -----END {end_label}----- with no matching -----BEGIN")
            })?;
            if end_label != label {
                return Err(format!(
                    "line {lineno}: -----END {end_label}----- closes a block that opened as \
                     -----BEGIN {label}----- on line {open_line}"
                ));
            }
            if label == "CERTIFICATE" || label == "X509 CERTIFICATE" || label == "TRUSTED CERTIFICATE"
            {
                let der = B64_STD.decode(body.as_bytes()).map_err(|e| {
                    format!(
                        "the body of the {label} block opened on line {open_line} is not valid \
                         base64: {e} (a PEM copied through a chat window or a JSON string is the \
                         usual cause)"
                    )
                })?;
                if der.is_empty() {
                    return Err(format!(
                        "the {label} block opened on line {open_line} is empty"
                    ));
                }
                out.push(der);
            } else {
                other_labels.push(label);
            }
            body.clear();
        } else if open.is_some() {
            body.push_str(line);
        }
    }

    if let Some((label, open_line)) = open {
        return Err(format!(
            "-----BEGIN {label}----- on line {open_line} is never closed by an -----END {label}-----"
        ));
    }
    if out.is_empty() {
        return Err(if !other_labels.is_empty() {
            format!(
                "no CERTIFICATE block found; this PEM holds only: {}",
                other_labels.join(", ")
            )
        } else if looks_like_a_path(&normalized) {
            format!(
                "no -----BEGIN CERTIFICATE----- block found — the value looks like a FILE PATH \
                 ({}), and this variable takes the certificate CONTENT, not a path to it. Pass it \
                 as \"$(cat <file>)\"",
                normalized.trim()
            )
        } else {
            "no -----BEGIN CERTIFICATE----- block found — this variable takes the PEM CONTENT of \
             the CA certificate. If it was set from a line-oriented env file, the value was \
             truncated at the first newline: pass it as \"$(cat <file>)\" or on one line with \\n \
             for the newlines"
                .to_string()
        });
    }
    Ok(out)
}

/// Best-effort "the operator gave us a path" detector, used only to pick a
/// better error message.
fn looks_like_a_path(v: &str) -> bool {
    let v = v.trim();
    !v.is_empty()
        && !v.contains('\n')
        && v.len() < 512
        && (v.starts_with('/') || v.starts_with("./") || v.starts_with("~/") || v.contains(".pem")
            || v.contains(".crt")
            || v.contains(".cert"))
}

/// A `ServerCertVerifier` that accepts any presented certificate. Gated strictly
/// behind `PG_SSL_REJECT_UNAUTHORIZED=false` with no CA supplied — this is the
/// deliberate "encryption without authentication" mode, never the default.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Self-signed P-256 CA, `CN = Queen Test Root CA 1`, valid to 2126 so the
    /// suite cannot rot. Generated with `openssl req -x509 -newkey ec`.
    const CA1: &str = "-----BEGIN CERTIFICATE-----\n\
MIIBWTCCAQCgAwIBAgIJAL8p4O4CsUCPMAoGCCqGSM49BAMCMB8xHTAbBgNVBAMM\n\
FFF1ZWVuIFRlc3QgUm9vdCBDQSAxMCAXDTI2MDgyNzExMTYxN1oYDzIxMjYwODAz\n\
MTExNjE3WjAfMR0wGwYDVQQDDBRRdWVlbiBUZXN0IFJvb3QgQ0EgMTBZMBMGByqG\n\
SM49AgEGCCqGSM49AwEHA0IABK2aWQlKA30FdUuAoqedY++p9RRv1mJdSYJnZYjk\n\
z4W0yiSMBR6Pn+I6eJ//uhHMo8ljUnb6vHVbQmRMpmwJxHejIzAhMA8GA1UdEwEB\n\
/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMCA0cAMEQCIB78dHbp\n\
t7iRqxbzNe/Egm2MMPQV001UAOZDbz66P1jzAiBk0xCGVWPF5H31ZQIH2eNLxhrX\n\
eZqodYLb9Doe7S+Ykg==\n\
-----END CERTIFICATE-----\n";

    /// A second, unrelated CA — for the "a bundle is a bundle" case.
    const CA2: &str = "-----BEGIN CERTIFICATE-----\n\
MIIBWTCCAQCgAwIBAgIJAKmzX6A030osMAoGCCqGSM49BAMCMB8xHTAbBgNVBAMM\n\
FFF1ZWVuIFRlc3QgUm9vdCBDQSAyMCAXDTI2MDgyNzExMTYxN1oYDzIxMjYwODAz\n\
MTExNjE3WjAfMR0wGwYDVQQDDBRRdWVlbiBUZXN0IFJvb3QgQ0EgMjBZMBMGByqG\n\
SM49AgEGCCqGSM49AwEHA0IABF6twyLh3QAgSqyQuFXN6YQpXqV9UW5xVjHk9QhH\n\
TLKoCVy19qOVzNumm5N3EQVVKDaAnssJkg1dL7nEtgBHV0qjIzAhMA8GA1UdEwEB\n\
/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMCA0cAMEQCIGCfF1DG\n\
B/pG4xgUMEbP3/6vEIKVVA+xzh8lHN6SN1jRAiB/wSDR3RDFzV3Mw4/S8WIJFlvW\n\
cFiWxl/YjudWoYm2+g==\n\
-----END CERTIFICATE-----\n";

    // ---------------------------------------------------------------
    // the decision table
    // ---------------------------------------------------------------

    #[test]
    fn trust_policy_is_the_documented_table() {
        assert_eq!(trust_policy(false, true), TrustPolicy::WebpkiRoots);
        assert_eq!(trust_policy(false, false), TrustPolicy::AcceptAny);
        assert_eq!(
            trust_policy(true, true),
            TrustPolicy::SuppliedCa { escape_hatch_redundant: false }
        );
        // The point of the whole exercise: a supplied CA beats the escape hatch,
        // and says the hatch is now dead weight.
        assert_eq!(
            trust_policy(true, false),
            TrustPolicy::SuppliedCa { escape_hatch_redundant: true }
        );
    }

    #[test]
    fn only_accept_any_skips_verification() {
        assert!(trust_policy(false, true).verifies());
        assert!(trust_policy(true, true).verifies());
        assert!(trust_policy(true, false).verifies());
        assert!(!trust_policy(false, false).verifies());
    }

    #[test]
    fn the_boot_label_never_claims_a_trust_policy_for_a_plaintext_link() {
        assert_eq!(trust_label(false, true, true), "plaintext");
        assert_eq!(trust_label(false, false, false), "plaintext");
        assert_eq!(trust_label(true, false, true), "webpki-roots");
        assert_eq!(trust_label(true, true, true), "supplied-ca");
        assert_eq!(trust_label(true, true, false), "supplied-ca");
        assert_eq!(trust_label(true, false, false), "accept-any");
    }

    #[test]
    fn authenticated_is_true_only_for_a_verified_tls_link() {
        assert!(!link_authenticated(false, true, true), "plaintext is not authenticated");
        assert!(!link_authenticated(true, false, false), "accept-any is not authenticated");
        assert!(link_authenticated(true, false, true));
        assert!(link_authenticated(true, true, true));
        // The combination the whole feature exists for: a CA supplied on a cell
        // that still carries the old escape hatch is AUTHENTICATED.
        assert!(link_authenticated(true, true, false));
    }

    // ---------------------------------------------------------------
    // the boot advisory
    // ---------------------------------------------------------------

    #[test]
    fn a_ca_with_tls_off_warns_that_the_link_is_plaintext() {
        let m = boot_advisory(false, true, true, BROKER_VARS).expect("warns");
        assert!(m.contains("PLAINTEXT"), "{m}");
        assert!(m.contains("PG_SSL_ROOT_CERT"), "{m}");
        assert!(m.contains("PG_USE_SSL"), "{m}");
        // Same rule, proxy names — the whole point of TlsVars.
        let p = boot_advisory(false, true, true, PROXY_VARS).expect("warns");
        assert!(p.contains("PXDB_SSL_ROOT_CERT") && p.contains("PXDB_USE_SSL"), "{p}");
        assert!(!p.contains("PG_USE_SSL="), "{p}");
    }

    #[test]
    fn a_ca_beside_the_escape_hatch_says_the_hatch_is_ignored() {
        let m = boot_advisory(true, true, false, BROKER_VARS).expect("warns");
        assert!(m.contains("PG_SSL_REJECT_UNAUTHORIZED=false"), "{m}");
        assert!(m.contains("IGNORED"), "{m}");
        let p = boot_advisory(true, true, false, PROXY_VARS).expect("warns");
        assert!(p.contains("PXDB_SSL_REJECT_UNAUTHORIZED=false"), "{p}");
    }

    #[test]
    fn the_escape_hatch_alone_says_the_link_is_unauthenticated() {
        let m = boot_advisory(true, false, false, BROKER_VARS).expect("warns");
        assert!(m.contains("NOT AUTHENTICATED"), "{m}");
        assert!(m.contains("PG_SSL_ROOT_CERT"), "{m}");
    }

    #[test]
    fn a_correct_configuration_says_nothing() {
        // plaintext, nothing configured: the OSS default, silent.
        assert!(boot_advisory(false, false, true, BROKER_VARS).is_none());
        assert!(boot_advisory(false, false, false, BROKER_VARS).is_none());
        // TLS against a publicly-trusted certificate.
        assert!(boot_advisory(true, false, true, BROKER_VARS).is_none());
        // TLS against a private CA, done right — the target state.
        assert!(boot_advisory(true, true, true, BROKER_VARS).is_none());
    }

    // ---------------------------------------------------------------
    // the root store
    // ---------------------------------------------------------------

    #[test]
    fn one_ca_pem_yields_one_anchor() {
        let roots = root_store_from_pem(CA1).expect("CA1 parses");
        assert_eq!(roots.len(), 1);
    }

    #[test]
    fn a_bundle_yields_every_anchor_in_it() {
        let roots = root_store_from_pem(&format!("{CA1}{CA2}")).expect("bundle parses");
        assert_eq!(roots.len(), 2);
    }

    #[test]
    fn text_around_the_blocks_is_ignored_like_openssl_does() {
        let pem = format!("subject=CN = Queen Test Root CA 1\nissuer=self\n{CA1}\n# trailing note\n");
        assert_eq!(root_store_from_pem(&pem).expect("parses").len(), 1);
    }

    /// The env-file workaround: one line, `\n` as two literal characters. It has
    /// to yield exactly the same anchors as the multi-line form, or the
    /// documented escape from the docker `--env-file` trap is a lie.
    #[test]
    fn a_backslash_n_escaped_one_liner_is_the_same_certificate() {
        let one_line = CA1.replace('\n', "\\n");
        assert!(!one_line.contains('\n'));
        let a = parse_pem_certificates(CA1).expect("multi-line");
        let b = parse_pem_certificates(&one_line).expect("one-line");
        assert_eq!(a, b);
        assert_eq!(root_store_from_pem(&one_line).expect("parses").len(), 1);
    }

    #[test]
    fn crlf_line_endings_parse() {
        let crlf = CA1.replace('\n', "\r\n");
        assert_eq!(root_store_from_pem(&crlf).expect("parses").len(), 1);
    }

    // ---------------------------------------------------------------
    // every malformed shape is an error, and the message says which
    // ---------------------------------------------------------------

    #[test]
    fn an_empty_value_is_rejected_not_silently_trust_nothing() {
        let e = root_store_from_pem("").unwrap_err();
        assert!(e.contains("no -----BEGIN CERTIFICATE----- block found"), "{e}");
    }

    #[test]
    fn a_file_path_says_so() {
        let e = root_store_from_pem("/etc/ssl/certs/scaleway-ca.pem").unwrap_err();
        assert!(e.contains("FILE PATH"), "{e}");
        assert!(e.contains("CONTENT"), "{e}");
    }

    #[test]
    fn a_value_truncated_by_an_env_file_says_so() {
        let e = root_store_from_pem("-----BEGIN CERTIFICATE-----").unwrap_err();
        assert!(e.contains("never closed"), "{e}");
    }

    #[test]
    fn a_key_instead_of_a_certificate_names_what_was_found() {
        let e = root_store_from_pem(
            "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIA==\n-----END PRIVATE KEY-----\n",
        )
        .unwrap_err();
        assert!(e.contains("no CERTIFICATE block found"), "{e}");
        assert!(e.contains("PRIVATE KEY"), "{e}");
    }

    #[test]
    fn a_corrupted_body_is_an_error_not_a_dropped_block() {
        // The trap this strictness exists for: `pem_blocks` in the listener path
        // would silently drop this and report "no certificate", or worse, parse
        // the rest of a bundle and quietly trust fewer anchors than the operator
        // supplied.
        let pem = CA1.replace("MIIBWTCCAQCg", "not base64 !!!");
        assert_ne!(pem, CA1, "the fixture changed; pick a prefix that is still in it");
        let e = root_store_from_pem(&pem).unwrap_err();
        assert!(e.contains("not valid base64"), "{e}");
    }

    #[test]
    fn valid_base64_that_is_not_a_certificate_is_an_error() {
        let pem = "-----BEGIN CERTIFICATE-----\naGVsbG8gd29ybGQ=\n-----END CERTIFICATE-----\n";
        let e = root_store_from_pem(pem).unwrap_err();
        assert!(e.contains("not a usable X.509 trust anchor"), "{e}");
        assert!(e.contains("block 1 of 1"), "{e}");
    }

    #[test]
    fn one_bad_certificate_in_a_bundle_fails_the_whole_bundle() {
        let bad = "-----BEGIN CERTIFICATE-----\naGVsbG8gd29ybGQ=\n-----END CERTIFICATE-----\n";
        let e = root_store_from_pem(&format!("{CA1}{bad}")).unwrap_err();
        assert!(e.contains("block 2 of 2"), "{e}");
    }

    #[test]
    fn mismatched_end_label_is_an_error() {
        let pem = CA1.replace("-----END CERTIFICATE-----", "-----END TRUSTED CERTIFICATE-----");
        let e = root_store_from_pem(&pem).unwrap_err();
        assert!(e.contains("closes a block that opened as"), "{e}");
    }

    #[test]
    fn a_second_begin_before_the_first_end_is_an_error() {
        let pem = "-----BEGIN CERTIFICATE-----\nAAAA\n-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
        let e = root_store_from_pem(pem).unwrap_err();
        assert!(e.contains("still unterminated"), "{e}");
    }

    // ---------------------------------------------------------------
    // the connector itself
    // ---------------------------------------------------------------

    #[test]
    fn every_policy_builds_a_connector() {
        assert!(make_connector(true, None).is_ok());
        assert!(make_connector(false, None).is_ok());
        assert!(make_connector(true, Some(CA1)).is_ok());
        // The redundant-escape-hatch combination still builds — the CA wins and
        // the service warns; it is not a boot failure.
        assert!(make_connector(false, Some(CA1)).is_ok());
    }

    #[test]
    fn a_malformed_ca_fails_the_connector_in_both_reject_modes() {
        let bad = "-----BEGIN CERTIFICATE-----\nnot base64 !!!\n-----END CERTIFICATE-----\n";
        assert!(make_connector(true, Some(bad)).is_err());
        // Especially here: `reject_unauthorized=false` must NOT be a way to make
        // a broken CA silently acceptable.
        assert!(make_connector(false, Some(bad)).is_err());
    }
}
