//! AWS Signature Version 4, the SIGNING side, for service `s3`.
//!
//! The verification side already exists in this repository
//! (protocols/queen-sqs/src/sigv4.rs, 1915 lines: `canonical_request`:424,
//! `string_to_sign`:411, `signing_key`:451, `uri_encode`:660, `sha256_hex`:680).
//! This is the same canonical form with the roles swapped, and the arithmetic is
//! ported rather than re-derived — where the two files differ, this one is
//! wrong.
//!
//! TWO THINGS S3 DOES DIFFERENTLY FROM EVERY OTHER AWS SERVICE, and both are
//! in [`canonical_uri`]:
//!
//! * **The path is encoded ONCE.** For other services the canonical URI is the
//!   already-encoded path encoded a second time; for S3 it is encoded once.
//!   A key called `test$file.text` therefore signs as `/test%24file.text` —
//!   which is exactly the AWS documentation's own PUT Object vector, and the
//!   reason that vector is pinned below.
//! * **The path is NOT normalised.** No `.`/`..` collapsing, no duplicate-slash
//!   removal: an object key is bytes, and `a//b` is a different object from
//!   `a/b`. queen-sqs normalises (:640) because SQS is an RPC endpoint whose
//!   path is `/`; doing it here would sign a different resource from the one the
//!   request addresses.
//!
//! Everything else — the header canonicalisation, the four-step key derivation,
//! the string to sign, the `Authorization` line — is common, and the two AWS
//! documentation vectors at the bottom of this file are what say so. If a vector
//! stops matching, the port is wrong; the vector is not.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::types::Micros;

pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const TERMINATOR: &str = "aws4_request";
pub const SERVICE: &str = "s3";
/// The payload hash of an empty body, and the one every request without a body
/// carries.
pub const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// The static credential pair. STS/IRSA is v2 (plan §6.2), so there is no
/// session token here — and a `x-amz-security-token` header would have to be
/// SIGNED, not merely sent, which is why its absence is a type rather than an
/// omission.
#[derive(Clone)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
}

impl Credentials {
    pub fn new(access_key: impl Into<String>, secret_key: impl Into<String>) -> Credentials {
        Credentials {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
        }
    }
}

/// Never print the secret. The struct is held by the S3 client, which is held
/// by the driver, which is `Debug`-derived in places nobody is watching.
impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .finish()
    }
}

/// One request, in the terms the canonical form is built from.
///
/// `path` and `query` are RAW — unencoded — because the encoding rule is part
/// of the signature and must be applied in exactly one place. Handing this an
/// already-encoded path would double-encode it and produce a signature for a
/// resource nobody asked for.
pub struct SigningRequest<'a> {
    pub method: &'a str,
    /// The absolute path, starting with `/`, with the OBJECT KEY unencoded:
    /// `/queue=orders/dt=2026-09-04/w-1.jsonl.zst`.
    pub path: &'a str,
    /// Query parameters, unencoded, in any order — [`canonical_query`] sorts.
    pub query: &'a [(String, String)],
    /// Every header that is to be signed, names in any case. `host`,
    /// `x-amz-content-sha256` and `x-amz-date` must be among them.
    pub headers: &'a [(String, String)],
    /// Lowercase hex SHA-256 of the body, or `UNSIGNED-PAYLOAD`. It is also the
    /// value of the `x-amz-content-sha256` header, and the two must agree.
    pub payload_hash: &'a str,
}

/// What signing produced. The canonical request and the string to sign are
/// carried out with the signature deliberately: when a gateway answers
/// `SignatureDoesNotMatch` it echoes ITS canonical request, and having ours in
/// hand is the difference between a diff and a guess.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signed {
    pub authorization: String,
    pub signature: String,
    pub signed_headers: String,
    pub canonical_request: String,
    pub string_to_sign: String,
    pub scope: String,
}

/// Percent-encode for a canonical request. `encode_slash: false` is the PATH's
/// rule (a `/` stays a `/`); `true` is the query's, where every reserved byte is
/// escaped. Uppercase hex, unreserved set only.
///
/// Ported verbatim from protocols/queen-sqs/src/sigv4.rs:660.
pub fn uri_encode(s: &str, encode_slash: bool) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => {
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
    out
}

/// Lowercase hex SHA-256 — the payload-hash form.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// HMAC-SHA256.
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut mac = <Hmac<Sha256>>::new_from_slice(key).expect("HMAC takes a key of any length");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

/// `HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), service), "aws4_request")`.
pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, TERMINATOR.as_bytes())
}

/// The canonical URI: S3's single encoding, and no normalisation. See the
/// module header for why both halves of that sentence matter.
pub fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    uri_encode(path, false)
}

/// The canonical query string: every name and value encoded, then sorted by the
/// ENCODED bytes — which is not the same order as sorting the raw pairs, and is
/// the order AWS specifies.
pub fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(n, v)| (uri_encode(n, true), uri_encode(v, true)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(n, v)| format!("{n}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// The canonical headers block and the `SignedHeaders` list.
///
/// Names lowercased, values trimmed and their internal whitespace runs
/// collapsed, sorted by name. Two headers of one name are joined with a comma,
/// in the order they were given.
pub fn canonical_headers(headers: &[(String, String)]) -> (String, String) {
    let mut folded: Vec<(String, String)> = Vec::with_capacity(headers.len());
    for (name, value) in headers {
        let name = name.trim().to_ascii_lowercase();
        let value = collapse_whitespace(value);
        match folded.iter_mut().find(|(n, _)| *n == name) {
            Some((_, existing)) => {
                existing.push(',');
                existing.push_str(&value);
            }
            None => folded.push((name, value)),
        }
    }
    folded.sort_by(|a, b| a.0.cmp(&b.0));
    let mut block = String::new();
    for (name, value) in &folded {
        block.push_str(name);
        block.push(':');
        block.push_str(value);
        block.push('\n');
    }
    let signed = folded
        .iter()
        .map(|(n, _)| n.as_str())
        .collect::<Vec<_>>()
        .join(";");
    (block, signed)
}

/// Trim, and collapse every run of spaces or tabs to one space.
fn collapse_whitespace(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut in_space = false;
    for c in value.trim().chars() {
        if c == ' ' || c == '\t' {
            in_space = true;
            continue;
        }
        if in_space && !out.is_empty() {
            out.push(' ');
        }
        in_space = false;
        out.push(c);
    }
    out
}

/// The canonical request, and the `SignedHeaders` list that goes with it.
pub fn canonical_request(req: &SigningRequest<'_>) -> (String, String) {
    let (headers_block, signed_headers) = canonical_headers(req.headers);
    let canonical = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        req.method.to_ascii_uppercase(),
        canonical_uri(req.path),
        canonical_query(req.query),
        headers_block,
        signed_headers,
        req.payload_hash
    );
    (canonical, signed_headers)
}

/// `<yyyymmdd>/<region>/s3/aws4_request`.
pub fn credential_scope(date: &str, region: &str) -> String {
    format!("{date}/{region}/{SERVICE}/{TERMINATOR}")
}

/// `AWS4-HMAC-SHA256\n<amz-date>\n<scope>\n<sha256 of the canonical request>`.
pub fn string_to_sign(amz_date: &str, scope: &str, canonical_request: &str) -> String {
    format!(
        "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    )
}

/// Sign one request. `amz_date` is the `x-amz-date` value,
/// `YYYYMMDD'T'HHMMSS'Z'`, and it MUST be the same string the request's
/// `x-amz-date` header carries — the signature covers it.
pub fn sign(creds: &Credentials, region: &str, amz_date: &str, req: &SigningRequest<'_>) -> Signed {
    let date = &amz_date[..8.min(amz_date.len())];
    let scope = credential_scope(date, region);
    let (canonical, signed_headers) = canonical_request(req);
    let sts = string_to_sign(amz_date, &scope, &canonical);
    let key = signing_key(&creds.secret_key, date, region, SERVICE);
    let signature = hex::encode(hmac_sha256(&key, sts.as_bytes()));
    let authorization = format!(
        "{ALGORITHM} Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        creds.access_key
    );
    Signed {
        authorization,
        signature,
        signed_headers,
        canonical_request: canonical,
        string_to_sign: sts,
        scope,
    }
}

/// `YYYYMMDD'T'HHMMSS'Z'` for a point in time.
///
/// Derived from [`Micros::to_iso`] so there is ONE civil-date implementation in
/// this crate rather than two that can disagree about a leap year.
pub fn amz_date(t: Micros) -> String {
    let iso = t.to_iso();
    // `1970-01-01T00:00:00.000000Z` → `19700101T000000Z`.
    if iso.len() < 20 {
        return "19700101T000000Z".to_string();
    }
    let b = iso.as_bytes();
    let take = |from: usize, to: usize| std::str::from_utf8(&b[from..to]).unwrap_or("0");
    format!(
        "{}{}{}T{}{}{}Z",
        take(0, 4),
        take(5, 7),
        take(8, 10),
        take(11, 13),
        take(14, 16),
        take(17, 19)
    )
}

/// The signing clock: the real wall clock, in the `x-amz-date` form.
///
/// This is the ONE place in the connector where the process's own clock decides
/// anything, and it is not a window boundary: S3 refuses a request whose date is
/// more than fifteen minutes from its own, so the signature has to carry real
/// time. Every boundary in [`crate::window`] comes from PostgreSQL (plan §12).
pub fn amz_date_now() -> String {
    amz_date(Micros(crate::obs::now_epoch_ms().saturating_mul(1_000)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The credentials of every example in the AWS SigV4 documentation.
    fn example_creds() -> Credentials {
        Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
    }

    fn h(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(n, v)| ((*n).to_string(), (*v).to_string()))
            .collect()
    }

    /// AWS documentation, "Examples of the complete Signature Version 4 signing
    /// process (Python)" — **GET Object**.
    ///
    /// If this stops matching, the port is wrong. Do not fix the vector.
    #[test]
    fn aws_vector_get_object() {
        let headers = h(&[
            ("Host", "examplebucket.s3.amazonaws.com"),
            ("Range", "bytes=0-9"),
            ("x-amz-content-sha256", EMPTY_PAYLOAD_SHA256),
            ("x-amz-date", "20130524T000000Z"),
        ]);
        let req = SigningRequest {
            method: "GET",
            path: "/test.txt",
            query: &[],
            headers: &headers,
            payload_hash: EMPTY_PAYLOAD_SHA256,
        };
        let signed = sign(&example_creds(), "us-east-1", "20130524T000000Z", &req);

        assert_eq!(
            signed.canonical_request,
            "GET\n/test.txt\n\nhost:examplebucket.s3.amazonaws.com\nrange:bytes=0-9\n\
             x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n\
             x-amz-date:20130524T000000Z\n\n\
             host;range;x-amz-content-sha256;x-amz-date\n\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            signed.string_to_sign,
            "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n\
             7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972"
        );
        assert_eq!(
            signed.signature,
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
        assert_eq!(
            signed.authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    /// The same documentation page — **PUT Object**, body `Welcome to Amazon S3.`
    ///
    /// This one is the single-encoding pin: the key is `test$file.text` and the
    /// canonical URI is `/test%24file.text`.
    #[test]
    fn aws_vector_put_object() {
        let body = b"Welcome to Amazon S3.";
        let payload_hash = sha256_hex(body);
        assert_eq!(
            payload_hash, "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
            "the vector's own payload hash"
        );
        let headers = h(&[
            ("Date", "Fri, 24 May 2013 00:00:00 GMT"),
            ("Host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", &payload_hash),
            ("x-amz-date", "20130524T000000Z"),
            ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
        ]);
        let req = SigningRequest {
            method: "PUT",
            path: "/test$file.text",
            query: &[],
            headers: &headers,
            payload_hash: &payload_hash,
        };
        let signed = sign(&example_creds(), "us-east-1", "20130524T000000Z", &req);

        assert!(
            signed
                .canonical_request
                .starts_with("PUT\n/test%24file.text\n\n"),
            "S3 encodes the path ONCE and does not normalise it:\n{}",
            signed.canonical_request
        );
        assert_eq!(
            signed.signed_headers,
            "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class"
        );
        assert_eq!(
            signed.signature,
            "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        );
    }

    #[test]
    fn the_derived_key_is_the_documented_one() {
        // AWS's own worked example of the four-step derivation.
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
            "s3",
        );
        assert_eq!(
            hex::encode(key),
            "dbb893acc010964918f1fd433add87c70e8b0db6be30c1fbeafefa5ec6ba8378"
        );
    }

    #[test]
    fn the_query_is_sorted_by_its_encoded_bytes() {
        let q = vec![
            ("uploadId".to_string(), "abc/def+ghi".to_string()),
            ("partNumber".to_string(), "10".to_string()),
        ];
        assert_eq!(
            canonical_query(&q),
            "partNumber=10&uploadId=abc%2Fdef%2Bghi"
        );
        assert_eq!(canonical_query(&[]), "");
        // A valueless parameter still carries its `=`, which is what `?uploads`
        // and `?delete` are on the wire.
        let q = vec![("uploads".to_string(), String::new())];
        assert_eq!(canonical_query(&q), "uploads=");
    }

    #[test]
    fn headers_are_lowercased_trimmed_collapsed_and_sorted() {
        let (block, signed) = canonical_headers(&h(&[
            ("X-Amz-Date", "20130524T000000Z"),
            ("Content-Type", "  application/json   ; charset=utf-8 "),
            ("HOST", "example.com"),
        ]));
        assert_eq!(signed, "content-type;host;x-amz-date");
        assert_eq!(
            block,
            "content-type:application/json ; charset=utf-8\nhost:example.com\n\
             x-amz-date:20130524T000000Z\n"
        );
    }

    #[test]
    fn a_repeated_header_folds_into_one_comma_separated_value() {
        let (block, signed) = canonical_headers(&h(&[
            ("x-amz-meta-a", "1"),
            ("host", "example.com"),
            ("x-amz-meta-a", "2"),
        ]));
        assert_eq!(signed, "host;x-amz-meta-a");
        assert!(block.contains("x-amz-meta-a:1,2\n"), "{block}");
    }

    #[test]
    fn the_path_is_encoded_once_and_never_normalised() {
        // Hive components: `=` is encoded, `/` is not — which is what boto3
        // sends for exactly these keys.
        assert_eq!(
            canonical_uri("/queen/queue=orders/dt=2026-09-04/w-1.jsonl.zst"),
            "/queen/queue%3Dorders/dt%3D2026-09-04/w-1.jsonl.zst"
        );
        // An escaped name is encoded a second time on the wire, which is
        // correct: the OBJECT KEY contains a literal `%`.
        assert_eq!(canonical_uri("/queue=a%2Fb"), "/queue%3Da%252Fb");
        // No normalisation: `a//b` and `a/./b` are different objects.
        assert_eq!(canonical_uri("/a//b"), "/a//b");
        assert_eq!(canonical_uri("/a/./b"), "/a/./b");
        assert_eq!(canonical_uri("/a/../b"), "/a/../b");
        assert_eq!(canonical_uri(""), "/");
    }

    #[test]
    fn amz_dates_come_out_of_the_one_calendar() {
        assert_eq!(amz_date(Micros(0)), "19700101T000000Z");
        assert_eq!(
            amz_date(Micros::parse_iso("2013-05-24T00:00:00Z").unwrap()),
            "20130524T000000Z"
        );
        assert_eq!(
            amz_date(Micros::parse_iso("2026-09-04T10:03:41.918204Z").unwrap()),
            "20260904T100341Z"
        );
        assert_eq!(amz_date(Micros::MIN), "19700101T000000Z");
        let now = amz_date_now();
        assert_eq!(now.len(), 16, "{now}");
        assert!(now.ends_with('Z') && now.as_bytes()[8] == b'T', "{now}");
    }

    #[test]
    fn credentials_never_print_their_secret() {
        let text = format!("{:?}", example_creds());
        assert!(!text.contains("wJalrXUtnFEMI"), "{text}");
        assert!(text.contains("AKIAIOSFODNN7EXAMPLE"), "{text}");
    }
}
