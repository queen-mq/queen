//! SigV4 verification, in-house.
//!
//! CONTRACT. Given the bytes of a request and a [`crate::credentials::Directory`],
//! `verify` answers WHICH principal signed it or one of the four signature
//! errors. It computes; it does not decide policy, and it never looks at what
//! the request is asking for.
//!
//! This is the harder direction of the algorithm. A signer controls its own
//! canonical form; a VERIFIER has to reconstruct, byte for byte, the canonical
//! request the CLIENT built out of choices the client made — which header set it
//! signed, in which order it wrote the query, how it encoded each byte. Every
//! disagreement is an unsigned-request error a user cannot debug from the
//! outside, so each rule below is written from the AWS specification and pinned
//! by vectors produced by botocore itself, never inferred from prose.
//!
//! The rules that are easy to get wrong, stated once:
//!
//!   * **Double URI-encoding** of the path in the canonical request. S3's
//!     single-encoding quirk does NOT apply to SQS — assuming it does breaks
//!     every queue name with a space or a `%` in it. The path arrives encoded
//!     once (that is what a URL is) and [`uri_encode`] encodes it again.
//!   * `UNSIGNED-PAYLOAD` is accepted as the payload hash: some SDKs send it
//!     over HTTPS, and every presigner uses it. The body is then unauthenticated,
//!     which is why the plan accepts it rather than pretending otherwise. A
//!     `x-amz-content-sha256` that names a HEX digest is checked against the
//!     body — a signed-but-unchecked digest would be a body-tampering hole
//!     wearing a signature.
//!   * The canonical query string is the query AS IT ARRIVED, split on `&` and
//!     sorted by byte, name then value, with each pair's encoding left alone.
//!     Signers sign what they send (botocore sorts the raw pairs, the Go and
//!     Java v2 signers rewrite the URL into their canonical form before
//!     signing), so re-encoding here would disagree with all three the moment a
//!     client spells `~` as `%7E`.
//!   * The header set is the CLIENT's `SignedHeaders` list, lowercased, values
//!     trimmed and inner runs of spaces collapsed, repeats joined with `,` in
//!     arrival order, and `host` must be in it.
//!   * The credential scope must name `sqs` or `sns` — the two services this
//!     listener answers — and any REGION the client chose (see [`verify`]). The
//!     request date must be inside [`MAX_CLOCK_SKEW_MS`], a check that exists
//!     because a replayed request is otherwise valid for ever.
//!
//! No AWS crates. `hmac`, `sha2`, `hex` are in-tree and the ruling is inherited.

use crate::credentials::Directory;
use crate::error::{ErrorKind, SqsError, SqsResult};

/// How far a request's `X-Amz-Date` may be from this clock. Fifteen minutes is
/// AWS's own window — the one its SDKs were written against, and the one whose
/// expiry message they parrot back to users — so a client whose clock drifts
/// fails here exactly where it would fail against the real service. A wider
/// window is a longer replay opportunity; a narrower one refuses honest clients
/// that AWS would have served.
pub const MAX_CLOCK_SKEW_MS: i64 = 15 * 60 * 1000;

/// The service name in every credential scope this facade accepts. SNS requests
/// arrive on the same listener and are signed for `sns`, which is why the check
/// admits two names rather than one constant.
pub const SERVICE_SQS: &str = "sqs";
pub const SERVICE_SNS: &str = "sns";

/// The only algorithm SigV4 has, spelled the way both variants spell it.
pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";
/// The payload hash a client sends when it declines to hash its body.
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
/// AWS's ceiling on a presigned URL's lifetime: seven days.
pub const MAX_PRESIGN_EXPIRES_SECONDS: i64 = 7 * 24 * 60 * 60;

const TERMINATOR: &str = "aws4_request";
const HEADER_AUTHORIZATION: &str = "authorization";
const HEADER_X_AMZ_DATE: &str = "x-amz-date";
const HEADER_DATE: &str = "date";
const HEADER_HOST: &str = "host";
const HEADER_CONTENT_SHA256: &str = "x-amz-content-sha256";
const PARAM_ALGORITHM: &str = "X-Amz-Algorithm";
const PARAM_CREDENTIAL: &str = "X-Amz-Credential";
const PARAM_DATE: &str = "X-Amz-Date";
const PARAM_EXPIRES: &str = "X-Amz-Expires";
const PARAM_SIGNED_HEADERS: &str = "X-Amz-SignedHeaders";
const PARAM_SIGNATURE: &str = "X-Amz-Signature";

/// What a client is told when its signature did not reproduce ours. AWS's own
/// sentence, verbatim: SDK users search for it, and the answer they find applies
/// here for the same reasons.
const MISMATCH: &str = "The request signature we calculated does not match the signature you \
                        provided. Check your AWS Secret Access Key and signing method.";

/// A request, reduced to exactly what the algorithm reads.
///
/// Borrowed, because verification happens before anything else and must not cost
/// a copy of the body: the payload hash is computed over these bytes as they
/// arrived, and any normalization at all — a JSON reserialization, a lossy UTF-8
/// pass — changes the hash and refuses a valid request.
pub struct SignedRequest<'a> {
    pub method: &'a str,
    /// The path as it arrived, NOT normalized: `//a/../b` is what the client
    /// signed, and canonicalizing it here is how a verifier disagrees with a
    /// signer.
    pub path: &'a str,
    /// The raw query string, without the leading `?`.
    pub query: &'a str,
    /// Every header, lowercased names, in arrival order.
    pub headers: &'a [(String, String)],
    pub body: &'a [u8],
}

/// The `Authorization: AWS4-HMAC-SHA256 Credential=…/…/…/…/aws4_request,
/// SignedHeaders=…, Signature=…` header, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzHeader {
    pub access_key_id: String,
    /// `YYYYMMDD`, the scope's date — which is NOT necessarily the request's.
    pub date: String,
    pub region: String,
    pub service: String,
    /// Lowercased, in the order the client wrote them, which IS the order the
    /// canonical request must use.
    pub signed_headers: Vec<String>,
    /// Lowercase hex, 64 characters.
    pub signature: String,
}

/// The presigned variant's parameters, read out of the query string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresignedParams {
    pub credential: AuthzHeader,
    pub amz_date: String,
    pub expires_seconds: i64,
}

/// Who signed a request, once it verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified {
    pub access_key_id: String,
    /// Epoch milliseconds of the request's `X-Amz-Date`, for the log line that
    /// explains a skew refusal.
    pub signed_at_ms: i64,
    pub service: String,
}

/// Verify a request against the directory.
///
/// `now_ms` is a parameter and not a clock read, so the skew tests are
/// deterministic.
///
/// `region` is this deployment's (`QUEEN_SQS_REGION`) and is deliberately NOT
/// enforced: the signing key is derived from the region the CLIENT put in its
/// own scope, and an SDK signs with the region its user configured — pointing
/// boto3 at an `endpoint_url` does not change its `region_name`. Refusing a
/// scope that says `us-east-1` would make the one promise this facade exists to
/// keep ("change endpoint_url and nothing else") false for every client on
/// earth. The parameter stays in the signature because a deployment that wants
/// the pin should get it without a call-site change; the SERVICE is pinned
/// instead, to the two this listener actually answers, because a scope naming
/// `s3` is a request that arrived at the wrong door.
pub fn verify(
    req: &SignedRequest<'_>,
    directory: &Directory,
    region: &str,
    now_ms: i64,
) -> SqsResult<Verified> {
    // Taken and not enforced, for the reason above — not a leftover.
    let _ = region;
    match parse_presigned(req.query) {
        Some(params) => verify_presigned(req, directory, &params?, now_ms),
        None => verify_header(req, directory, now_ms),
    }
}

/// The `Authorization` header variant: what every SDK sends by default.
fn verify_header(
    req: &SignedRequest<'_>,
    directory: &Directory,
    now_ms: i64,
) -> SqsResult<Verified> {
    let Some(header) = header_value(req, HEADER_AUTHORIZATION) else {
        return Err(refuse(
            ErrorKind::MissingAuthenticationToken,
            "Request is missing Authentication Token",
        ));
    };
    let authz = parse_authorization(&header)?;
    let amz_date = request_timestamp(req)?;
    let signed_at_ms = epoch_ms_of(&amz_date).ok_or_else(|| {
        refuse(
            ErrorKind::IncompleteSignature,
            format!("Date is not an ISO-8601 basic timestamp: '{amz_date}'"),
        )
    })?;
    check_scope(&authz, &amz_date)?;
    check_skew(signed_at_ms, now_ms, &amz_date)?;
    check_signature(req, directory, &authz, &amz_date, false)?;
    Ok(Verified {
        access_key_id: authz.access_key_id,
        signed_at_ms,
        service: authz.service,
    })
}

/// The presigned-query variant: a URL somebody was handed, with a lifetime of
/// its own instead of the skew window's.
fn verify_presigned(
    req: &SignedRequest<'_>,
    directory: &Directory,
    params: &PresignedParams,
    now_ms: i64,
) -> SqsResult<Verified> {
    let authz = &params.credential;
    let signed_at_ms = epoch_ms_of(&params.amz_date).ok_or_else(|| {
        refuse(
            ErrorKind::IncompleteSignature,
            format!(
                "X-Amz-Date is not an ISO-8601 basic timestamp: '{}'",
                params.amz_date
            ),
        )
    })?;
    check_scope(authz, &params.amz_date)?;
    // A presigned URL carries its OWN lifetime, so the skew window only guards
    // the future side: one signed in the future is a clock problem, one signed
    // in the past is exactly what a presigned URL is until it expires.
    if signed_at_ms - now_ms > MAX_CLOCK_SKEW_MS {
        return Err(refuse(
            ErrorKind::SignatureDoesNotMatch,
            format!(
                "Signature not yet current: {} is still later than {} (this server's clock + 15 \
                 min.)",
                params.amz_date,
                compact_from_epoch_ms(now_ms + MAX_CLOCK_SKEW_MS)
            ),
        ));
    }
    let expires_at_ms = signed_at_ms + params.expires_seconds * 1000;
    if now_ms > expires_at_ms {
        return Err(refuse(
            ErrorKind::SignatureDoesNotMatch,
            format!(
                "Signature expired: the URL was signed at {} for {} seconds and expired at {}; \
                 this server's clock reads {}",
                params.amz_date,
                params.expires_seconds,
                compact_from_epoch_ms(expires_at_ms),
                compact_from_epoch_ms(now_ms)
            ),
        ));
    }
    check_signature(req, directory, authz, &params.amz_date, true)?;
    Ok(Verified {
        access_key_id: authz.access_key_id.clone(),
        signed_at_ms,
        service: authz.service.clone(),
    })
}

/// The scope's service and date, which are checked before anything is hashed:
/// both are cheap, and both produce a message that says what to change.
fn check_scope(authz: &AuthzHeader, amz_date: &str) -> SqsResult<()> {
    if authz.service != SERVICE_SQS && authz.service != SERVICE_SNS {
        return Err(refuse(
            ErrorKind::SignatureDoesNotMatch,
            format!(
                "Credential should be scoped to correct service: '{SERVICE_SQS}' or \
                 '{SERVICE_SNS}', not '{}'",
                authz.service
            ),
        ));
    }
    if amz_date.get(..8).is_some_and(|date| authz.date != date) {
        return Err(refuse(
            ErrorKind::SignatureDoesNotMatch,
            format!(
                "Date in Credential scope does not match YYYYMMDD from ISO-8601 version of date \
                 from HTTP: '{amz_date}'"
            ),
        ));
    }
    Ok(())
}

fn check_skew(signed_at_ms: i64, now_ms: i64, amz_date: &str) -> SqsResult<()> {
    if now_ms - signed_at_ms > MAX_CLOCK_SKEW_MS {
        return Err(refuse(
            ErrorKind::SignatureDoesNotMatch,
            format!(
                "Signature expired: {amz_date} is now earlier than {} ({} - 15 min.)",
                compact_from_epoch_ms(now_ms - MAX_CLOCK_SKEW_MS),
                compact_from_epoch_ms(now_ms)
            ),
        ));
    }
    if signed_at_ms - now_ms > MAX_CLOCK_SKEW_MS {
        return Err(refuse(
            ErrorKind::SignatureDoesNotMatch,
            format!(
                "Signature not yet current: {amz_date} is still later than {} ({} + 15 min.)",
                compact_from_epoch_ms(now_ms + MAX_CLOCK_SKEW_MS),
                compact_from_epoch_ms(now_ms)
            ),
        ));
    }
    Ok(())
}

/// Look the principal up, rebuild the canonical request, and compare.
///
/// The lookup happens HERE, after the cheap refusals, so that a request with an
/// expired clock is told about its clock whichever key id it names.
fn check_signature(
    req: &SignedRequest<'_>,
    directory: &Directory,
    authz: &AuthzHeader,
    amz_date: &str,
    presigned: bool,
) -> SqsResult<()> {
    let Some(credential) = directory.get(&authz.access_key_id) else {
        // AWS distinguishes an unknown key id from a bad signature and so do
        // we: this directory is a handful of operator-configured triples, not a
        // user table, so there is no enumeration to protect — and a client that
        // is told "signature" when it configured the wrong key id debugs the
        // wrong thing for an afternoon.
        return Err(refuse(
            ErrorKind::InvalidClientTokenId,
            "The security token included in the request is invalid.",
        ));
    };
    require_signed_headers(req, &authz.signed_headers)?;
    let scope = format!(
        "{}/{}/{}/{TERMINATOR}",
        authz.date, authz.region, authz.service
    );
    let key = signing_key(
        &credential.secret,
        &authz.date,
        &authz.region,
        &authz.service,
    );
    let presented = authz.signature.to_ascii_lowercase();
    for payload_hash in payload_hashes(req, &authz.signed_headers, presigned)? {
        let canonical = canonical_request(req, &authz.signed_headers, &payload_hash);
        let computed = hex::encode(hmac_sha256(
            &key,
            string_to_sign(amz_date, &scope, &canonical).as_bytes(),
        ));
        if Directory::verify(computed.as_bytes(), presented.as_bytes()) {
            return Ok(());
        }
    }
    Err(refuse(ErrorKind::SignatureDoesNotMatch, MISMATCH))
}

/// Every header the client says it signed must be on the request, and `host`
/// must be among them.
///
/// Naming the missing header is the whole point: the canonical request would
/// otherwise be built with an empty value, the signature would not match, and
/// the client would be told to check a secret that is perfectly fine.
fn require_signed_headers(req: &SignedRequest<'_>, signed_headers: &[String]) -> SqsResult<()> {
    for name in signed_headers {
        if header_value(req, name).is_none() {
            return Err(refuse(
                ErrorKind::SignatureDoesNotMatch,
                format!("Request is missing the signed header '{name}'"),
            ));
        }
    }
    Ok(())
}

/// The payload hashes to try, best first.
///
/// One candidate in every ordinary case. Two only for a presigned URL with no
/// declared hash, because the presigners disagree: botocore signs the (empty)
/// body's digest while the JavaScript v3 presigner signs `UNSIGNED-PAYLOAD`, and
/// a verifier that picks one locks the other out of every presigned URL.
fn payload_hashes(
    req: &SignedRequest<'_>,
    signed_headers: &[String],
    presigned: bool,
) -> SqsResult<Vec<String>> {
    let declared = signed_headers
        .iter()
        .any(|h| h == HEADER_CONTENT_SHA256)
        .then(|| header_value(req, HEADER_CONTENT_SHA256))
        .flatten();
    let computed = sha256_hex(req.body);
    match declared {
        // The client declined to hash its body and signed that decision. The
        // body is unauthenticated from here on, which is the plan's accepted
        // trade and not an accident.
        Some(value) if value == UNSIGNED_PAYLOAD || value.starts_with("STREAMING-") => {
            Ok(vec![value])
        }
        // A declared hex digest is signed, so an attacker cannot change it — but
        // it is the canonical request's payload line, so a request whose BODY
        // was replaced would otherwise verify against the old digest. Check it.
        Some(value) => {
            if !value.eq_ignore_ascii_case(&computed) {
                return Err(refuse(
                    ErrorKind::SignatureDoesNotMatch,
                    "The x-amz-content-sha256 header this request signed does not match the SHA-256 \
                     of the body it carries.",
                ));
            }
            Ok(vec![value])
        }
        None if presigned => Ok(vec![computed, UNSIGNED_PAYLOAD.to_string()]),
        None => Ok(vec![computed]),
    }
}

/// `AWS4-HMAC-SHA256\n<amz-date>\n<scope>\n<sha256 of the canonical request>`.
pub fn string_to_sign(amz_date: &str, scope: &str, canonical_request: &str) -> String {
    format!(
        "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    )
}

/// The canonical request: method, canonical URI, canonical query, canonical
/// headers, signed headers, payload hash — each on its own line.
///
/// `signed_headers` is the client's list in the client's order; a name the
/// request does not carry contributes an empty value here, which is why
/// [`require_signed_headers`] runs first in the verification path.
pub fn canonical_request(
    req: &SignedRequest<'_>,
    signed_headers: &[String],
    payload_hash: &str,
) -> String {
    let path = if req.path.is_empty() { "/" } else { req.path };
    let mut out = String::with_capacity(256 + req.query.len());
    out.push_str(&req.method.to_ascii_uppercase());
    out.push('\n');
    out.push_str(&uri_encode(&normalize_path(path), false));
    out.push('\n');
    out.push_str(&canonical_query(req.query));
    out.push('\n');
    for name in signed_headers {
        out.push_str(name);
        out.push(':');
        out.push_str(&header_value(req, name).unwrap_or_default());
        out.push('\n');
    }
    out.push('\n');
    out.push_str(&signed_headers.join(";"));
    out.push('\n');
    out.push_str(payload_hash);
    out
}

/// `HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), service), "aws4_request")`.
pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, TERMINATOR.as_bytes())
}

pub fn parse_authorization(header: &str) -> SqsResult<AuthzHeader> {
    let Some(rest) = header.trim().strip_prefix(ALGORITHM) else {
        let algorithm = header.split_whitespace().next().unwrap_or("");
        return Err(refuse(
            ErrorKind::IncompleteSignature,
            format!("Unsupported AWS 'algorithm': '{algorithm}'"),
        ));
    };
    let (mut credential, mut signed_headers, mut signature) = (None, None, None);
    for part in rest.split(',') {
        let part = part.trim();
        let Some((name, value)) = part.split_once('=') else {
            continue;
        };
        match name.trim() {
            "Credential" => credential = Some(value.trim()),
            "SignedHeaders" => signed_headers = Some(value.trim()),
            "Signature" => signature = Some(value.trim()),
            _ => {}
        }
    }
    let credential = required(credential, "Credential")?;
    let signed_headers = required(signed_headers, "SignedHeaders")?;
    let signature = required(signature, "Signature")?;
    build_authz(credential, signed_headers, signature)
}

fn required<'a>(value: Option<&'a str>, name: &str) -> SqsResult<&'a str> {
    match value.filter(|v| !v.is_empty()) {
        Some(value) => Ok(value),
        None => Err(refuse(
            ErrorKind::IncompleteSignature,
            format!(
                "Authorization header requires '{name}' parameter. Authorization=AWS4-HMAC-SHA256 \
                 Credential=…, SignedHeaders=…, Signature=…"
            ),
        )),
    }
}

/// The two variants differ in where the three fields come from and in nothing
/// else, so they are validated in one place.
fn build_authz(credential: &str, signed_headers: &str, signature: &str) -> SqsResult<AuthzHeader> {
    let scope: Vec<&str> = credential.split('/').collect();
    if scope.len() != 5 || scope[4] != TERMINATOR || scope.iter().take(4).any(|p| p.is_empty()) {
        return Err(refuse(
            ErrorKind::IncompleteSignature,
            format!(
                "Credential must be in the format \
                 <access-key-id>/<YYYYMMDD>/<region>/<service>/{TERMINATOR}"
            ),
        ));
    }
    if scope[1].len() != 8 || !scope[1].bytes().all(|b| b.is_ascii_digit()) {
        return Err(refuse(
            ErrorKind::IncompleteSignature,
            format!(
                "Credential scope's date must be YYYYMMDD, not '{}'",
                scope[1]
            ),
        ));
    }
    let signed_headers: Vec<String> = signed_headers
        .split(';')
        .map(|h| h.trim().to_ascii_lowercase())
        .filter(|h| !h.is_empty())
        .collect();
    if !signed_headers.iter().any(|h| h == HEADER_HOST) {
        return Err(refuse(
            ErrorKind::IncompleteSignature,
            "SignedHeaders must include 'host'",
        ));
    }
    Ok(AuthzHeader {
        access_key_id: scope[0].to_string(),
        date: scope[1].to_string(),
        region: scope[2].to_string(),
        service: scope[3].to_string(),
        signed_headers,
        signature: signature.to_string(),
    })
}

/// `X-Amz-Algorithm`, `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`,
/// `X-Amz-SignedHeaders`, `X-Amz-Signature` — the presigned form. `None` when
/// the query carries no `X-Amz-Algorithm`, which is how a plain request is told
/// from a presigned one.
///
/// Parameter NAMES are matched case-insensitively (a signer picks the case, and
/// the canonical query keeps whatever it picked); VALUES are percent-decoded,
/// because a scope arrives as `AKID%2F20260830%2F…`.
pub fn parse_presigned(query: &str) -> Option<SqsResult<PresignedParams>> {
    let algorithm = query_param(query, PARAM_ALGORITHM)?;
    if algorithm != ALGORITHM {
        return Some(Err(refuse(
            ErrorKind::IncompleteSignature,
            format!("Unsupported AWS 'algorithm': '{algorithm}'"),
        )));
    }
    Some(parse_presigned_params(query))
}

fn parse_presigned_params(query: &str) -> SqsResult<PresignedParams> {
    let credential = query_param(query, PARAM_CREDENTIAL);
    let signed_headers = query_param(query, PARAM_SIGNED_HEADERS);
    let signature = query_param(query, PARAM_SIGNATURE);
    let amz_date = query_param(query, PARAM_DATE);
    let expires = query_param(query, PARAM_EXPIRES);
    let credential = required_param(credential.as_deref(), PARAM_CREDENTIAL)?;
    let signed_headers = required_param(signed_headers.as_deref(), PARAM_SIGNED_HEADERS)?;
    let signature = required_param(signature.as_deref(), PARAM_SIGNATURE)?;
    let amz_date = required_param(amz_date.as_deref(), PARAM_DATE)?.to_string();
    let expires = required_param(expires.as_deref(), PARAM_EXPIRES)?;
    let expires_seconds = expires.parse::<i64>().ok().filter(|s| *s > 0);
    let Some(expires_seconds) = expires_seconds.filter(|s| *s <= MAX_PRESIGN_EXPIRES_SECONDS)
    else {
        return Err(refuse(
            ErrorKind::IncompleteSignature,
            format!(
                "{PARAM_EXPIRES} must be a whole number of seconds between 1 and \
                 {MAX_PRESIGN_EXPIRES_SECONDS}, not '{expires}'"
            ),
        ));
    };
    Ok(PresignedParams {
        credential: build_authz(credential, signed_headers, signature)?,
        amz_date,
        expires_seconds,
    })
}

fn required_param<'a>(value: Option<&'a str>, name: &str) -> SqsResult<&'a str> {
    match value.filter(|v| !v.is_empty()) {
        Some(value) => Ok(value),
        None => Err(refuse(
            ErrorKind::IncompleteSignature,
            format!("Query-string authentication requires the '{name}' parameter."),
        )),
    }
}

/// The access key id a request PRESENTS, without verifying anything.
///
/// For the `auth=off` listener's log line and for the Cloud introspection lookup
/// that will replace the static directory: both need to know who a request
/// claims to be before anything has decided whether it is. Nothing that grants
/// access may call this.
pub fn presented_access_key_id(req: &SignedRequest<'_>) -> Option<String> {
    if let Some(Ok(params)) = parse_presigned(req.query) {
        return Some(params.credential.access_key_id);
    }
    let header = header_value(req, HEADER_AUTHORIZATION)?;
    parse_authorization(&header)
        .ok()
        .map(|authz| authz.access_key_id)
}

/// RFC 3986 `remove_dot_segments`, which is what a canonical URI is built over.
///
/// The signers do this and so must the verifier: botocore's `SigV4Auth`
/// normalizes the path (`quote(normalize_url_path(path), safe='/~')`) BEFORE the
/// second encoding, and the Java and Go v2 SDKs do the same for every service
/// but S3. A request whose target carries `//`, `/./` or `/../` — which the aws
/// CLI will mint from an `--endpoint-url` a user typed with a trailing `/.` — is
/// therefore signed over the collapsed path, and a verifier working from the raw
/// one cannot reproduce the signature of a client that did nothing wrong.
///
/// Empty in, `/` out: a canonical URI is never the empty string.
fn normalize_path(path: &str) -> String {
    if !path.contains("//") && !path.contains("/.") {
        // The overwhelmingly common shape — `/` or `/<account>/<name>` — needs
        // no work and must be handed through unchanged.
        return path.to_string();
    }
    let mut segments: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            // An empty segment is a `//`, which collapses; `.` is this
            // directory.
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            other => segments.push(other),
        }
    }
    let trailing = path.ends_with('/') || path.ends_with("/.") || path.ends_with("/..");
    let mut out = String::with_capacity(path.len());
    for segment in &segments {
        out.push('/');
        out.push_str(segment);
    }
    if out.is_empty() || (trailing && !out.ends_with('/')) {
        out.push('/');
    }
    out
}

/// Percent-encode for a canonical request. `encode_slash: false` is the PATH's
/// rule (a `/` stays a `/`); `true` is the query's, where every reserved byte is
/// escaped. Uppercase hex, unreserved set only — the one function every rule
/// above is expressed through.
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

/// Lowercase hex SHA-256, the payload-hash form.
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(bytes))
}

/// The canonical query string: the pairs as they arrived, sorted by byte, with
/// `X-Amz-Signature` dropped.
///
/// The signature parameter is dropped unconditionally rather than only for the
/// presigned variant: it is the one parameter no signer can have signed, and no
/// header-signed request carries one.
fn canonical_query(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(&str, &str)> = query
        .split('&')
        .map(|pair| pair.split_once('=').unwrap_or((pair, "")))
        .filter(|(name, _)| !name.eq_ignore_ascii_case(PARAM_SIGNATURE))
        .collect();
    pairs.sort_unstable();
    pairs
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// One header's canonical value: repeats joined with `,` in ARRIVAL order,
/// each trimmed with its inner runs of whitespace collapsed to one space.
fn header_value(req: &SignedRequest<'_>, name: &str) -> Option<String> {
    let mut joined: Option<String> = None;
    for (key, value) in req.headers {
        if !key.eq_ignore_ascii_case(name) {
            continue;
        }
        let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
        match joined.as_mut() {
            Some(acc) => {
                acc.push(',');
                acc.push_str(&value);
            }
            None => joined = Some(value),
        }
    }
    joined
}

/// A query parameter's value, percent-decoded, by case-insensitive name.
fn query_param(query: &str, name: &str) -> Option<String> {
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| percent_decode(value))
}

/// Percent-decode, over BYTES and never over `str` indices.
///
/// The distinction is the whole function. A query string is client bytes reached
/// on the PRE-AUTH path — before any credential is looked up — and `%` followed
/// by the first byte of a multi-byte character (`%a€`) is a slice that lands
/// inside a character. Slicing the `&str` there panics, the connection is
/// dropped with no answer, and an unauthenticated client chose the input.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                match (hex_nibble(bytes[i + 1]), hex_nibble(bytes[i + 2])) {
                    (Some(high), Some(low)) => {
                        out.push(high << 4 | low);
                        i += 3;
                    }
                    // Not an escape after all: a literal `%` is what it is.
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// One hexadecimal digit's value, or `None` for a byte that is not one.
fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// The timestamp the client signed, in the compact form the string-to-sign uses.
///
/// `X-Amz-Date` first, then `Date`: AWS accepts either ("Authorization header
/// requires existence of either a 'X-Amz-Date' or a 'Date' header") and botocore
/// SENDS either — given a `Date` header it deletes `X-Amz-Date` and signs with
/// the RFC-1123 value, so a verifier that only reads `X-Amz-Date` refuses a
/// perfectly ordinary boto3 request.
fn request_timestamp(req: &SignedRequest<'_>) -> SqsResult<String> {
    for name in [HEADER_X_AMZ_DATE, HEADER_DATE] {
        let Some(value) = header_value(req, name) else {
            continue;
        };
        if epoch_ms_of(&value).is_some() {
            return Ok(value);
        }
        if let Some(epoch_ms) = epoch_ms_of_http_date(&value) {
            return Ok(compact_from_epoch_ms(epoch_ms));
        }
        return Err(refuse(
            ErrorKind::IncompleteSignature,
            format!("The '{name}' header is not a date this facade can read: '{value}'"),
        ));
    }
    Err(refuse(
        ErrorKind::IncompleteSignature,
        "Authorization header requires existence of either a 'X-Amz-Date' or a 'Date' header.",
    ))
}

/// `YYYYMMDDTHHMMSSZ` to epoch milliseconds. `None` when it is not that shape,
/// which is how the `Date` fallback is selected.
fn epoch_ms_of(compact: &str) -> Option<i64> {
    let bytes = compact.as_bytes();
    if bytes.len() != 16 || bytes[8] != b'T' || bytes[15] != b'Z' {
        return None;
    }
    if !bytes[..8]
        .iter()
        .chain(&bytes[9..15])
        .all(u8::is_ascii_digit)
    {
        return None;
    }
    let number = |from: usize, to: usize| compact[from..to].parse::<i64>().ok();
    epoch_ms_of_parts(
        number(0, 4)?,
        number(4, 6)?,
        number(6, 8)?,
        number(9, 11)?,
        number(11, 13)?,
        number(13, 15)?,
    )
}

/// `Sun, 30 Aug 2026 12:00:00 GMT` (RFC 7231) and the `-0000` spelling botocore
/// writes. Numeric offsets other than zero are refused rather than applied: HTTP
/// mandates GMT, and a verifier that silently shifts a timestamp it did not
/// expect gets the skew window wrong in the one direction nobody tests.
fn epoch_ms_of_http_date(value: &str) -> Option<i64> {
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let rest = value.split_once(',').map_or(value, |(_, rest)| rest);
    let fields: Vec<&str> = rest.split_whitespace().collect();
    if fields.len() != 5 {
        return None;
    }
    let day = fields[0].parse::<i64>().ok()?;
    let month = MONTHS.iter().position(|m| *m == fields[1])? as i64 + 1;
    let year = fields[2].parse::<i64>().ok()?;
    let time: Vec<&str> = fields[3].split(':').collect();
    if time.len() != 3 {
        return None;
    }
    let zone = fields[4];
    if !matches!(zone, "GMT" | "UTC" | "UT" | "Z" | "+0000" | "-0000") {
        return None;
    }
    epoch_ms_of_parts(
        year,
        month,
        day,
        time[0].parse().ok()?,
        time[1].parse().ok()?,
        time[2].parse().ok()?,
    )
}

fn epoch_ms_of_parts(
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
) -> Option<i64> {
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some((days * 86_400 + hour * 3_600 + minute * 60 + second) * 1000)
}

/// Epoch milliseconds back to `YYYYMMDDTHHMMSSZ`, for the sentence a skew
/// refusal carries: an operator debugging one needs to see THIS server's clock,
/// which is the number the client could not know.
fn compact_from_epoch_ms(epoch_ms: i64) -> String {
    let seconds = epoch_ms.div_euclid(1000);
    let (days, rest) = (seconds.div_euclid(86_400), seconds.rem_euclid(86_400));
    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (rest / 3_600, (rest % 3_600) / 60, rest % 60);
    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

/// Howard Hinnant's `days_from_civil`, the proleptic Gregorian calendar in
/// integer arithmetic. It is here rather than behind a date crate because the
/// only two things this file needs from a calendar are these two functions, and
/// a dependency that parses timezones would be a larger surface than the
/// algorithm it replaced.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_index = (month + 9) % 12;
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    (if month <= 2 { year + 1 } else { year }, month, day)
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    use hmac::Mac;
    let mut mac = <hmac::Hmac<sha2::Sha256> as hmac::Mac>::new_from_slice(key)
        .expect("HMAC accepts a key of any length");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

/// The one place this module builds an error.
///
/// Fields rather than [`SqsError::with`], so that the signature path depends on
/// the error CATALOG and not on the catalog's constructors: it is the first code
/// a request touches and it should not acquire a second reason to change. Every
/// refusal here is a 4xx that will fail identically on retry, so none of them
/// carries a backoff.
fn refuse(kind: ErrorKind, message: impl Into<String>) -> SqsError {
    SqsError {
        kind,
        message: message.into(),
        retry_after_ms: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The AWS documentation's own example principal, which is also the
    /// aws-sig-v4-test-suite's. Using it means the vectors below can be checked
    /// against AWS's published files by hand.
    const AKID: &str = "AKIDEXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

    /// 2026-08-30T12:00:00Z and 2015-08-30T12:36:00Z — when the vectors below
    /// were signed. The second is the test suite's own timestamp.
    const NOW_MS: i64 = 1_788_091_200_000;
    const GOLDEN_MS: i64 = 1_440_938_160_000;

    fn directory() -> Directory {
        Directory::from_spec(&format!("{AKID}:{SECRET}:tok-1")).unwrap()
    }

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn authz(signed_headers: &str, signature: &str) -> String {
        authz_scoped("20260830/us-east-1/sqs", signed_headers, signature)
    }

    fn authz_scoped(scope: &str, signed_headers: &str, signature: &str) -> String {
        format!(
            "{ALGORITHM} Credential={AKID}/{scope}/aws4_request, SignedHeaders={signed_headers}, \
             Signature={signature}"
        )
    }

    fn request<'a>(
        method: &'a str,
        path: &'a str,
        query: &'a str,
        headers: &'a [(String, String)],
        body: &'a str,
    ) -> SignedRequest<'a> {
        SignedRequest {
            method,
            path,
            query,
            headers,
            body: body.as_bytes(),
        }
    }

    // ---------------------------------------------------------------- vectors
    //
    // Every signature below was produced by botocore (the reference signer)
    // against the key above, not by this implementation. A vector that this
    // implementation cannot reproduce is this implementation's bug, which is the
    // only way round that is worth anything.

    /// The Query protocol: a form body, header-signed. What aws-sdk-php and
    /// async-aws put on the wire.
    #[test]
    fn a_botocore_signed_query_protocol_post_verifies() {
        let headers = headers(&[
            (
                "authorization",
                &authz(
                    "content-type;host;x-amz-date",
                    "02c85e759cc5e2ce840146c82043bf13598030989142aceb4e9627da8ac45d39",
                ),
            ),
            (
                "content-type",
                "application/x-www-form-urlencoded; charset=utf-8",
            ),
            ("host", "localhost:9324"),
            ("x-amz-date", "20260830T120000Z"),
        ]);
        let req = request(
            "POST",
            "/",
            "",
            &headers,
            "Action=ListQueues&Version=2012-11-05",
        );
        let got = verify(&req, &directory(), "queen-1", NOW_MS).unwrap();
        assert_eq!(got.access_key_id, AKID);
        assert_eq!(got.service, SERVICE_SQS);
        assert_eq!(got.signed_at_ms, NOW_MS);
    }

    /// AWS JSON 1.0: what every SDK major since late 2023 sends.
    #[test]
    fn a_botocore_signed_json_1_0_post_verifies() {
        let headers = headers(&[
            (
                "authorization",
                &authz(
                    "content-type;host;x-amz-date;x-amz-target",
                    "2af03e1d2e6291b3acb256d658d6473c28aaf89266f9157ffb08e3efdbdc4c55",
                ),
            ),
            ("content-type", "application/x-amz-json-1.0"),
            ("host", "localhost:9324"),
            ("x-amz-date", "20260830T120000Z"),
            ("x-amz-target", "AmazonSQS.SendMessage"),
        ]);
        let body =
            r#"{"QueueUrl":"http://localhost:9324/000000000000/orders","MessageBody":"hello"}"#;
        let req = request("POST", "/", "", &headers, body);
        assert!(verify(&req, &directory(), "queen-1", NOW_MS).is_ok());
    }

    /// `UNSIGNED-PAYLOAD` is accepted, and what that costs is exactly this: the
    /// same signature verifies over a different body. The plan accepts it; the
    /// test states it so nobody discovers it by accident.
    #[test]
    fn an_unsigned_payload_request_verifies_and_its_body_is_not_authenticated() {
        let headers = headers(&[
            (
                "authorization",
                &authz(
                    "content-type;host;x-amz-content-sha256;x-amz-date;x-amz-target",
                    "45ade1c57134349bbb0cad70c8ef81591eb87c3f80bdc79eb3e87571f9529214",
                ),
            ),
            ("content-type", "application/x-amz-json-1.0"),
            ("host", "queue.example.com"),
            ("x-amz-content-sha256", UNSIGNED_PAYLOAD),
            ("x-amz-date", "20260830T120000Z"),
            ("x-amz-target", "AmazonSQS.ReceiveMessage"),
        ]);
        let body = r#"{"QueueUrl":"http://q/000000000000/orders"}"#;
        assert!(verify(
            &request("POST", "/", "", &headers, body),
            &directory(),
            "queen-1",
            NOW_MS
        )
        .is_ok());
        let tampered = r#"{"QueueUrl":"http://q/000000000000/other"}"#;
        assert!(verify(
            &request("POST", "/", "", &headers, tampered),
            &directory(),
            "queen-1",
            NOW_MS
        )
        .is_ok());
    }

    /// The presigned-query variant, botocore's flavour: the body's digest is
    /// what it signs, and the signature parameter is not part of what it signed.
    #[test]
    fn a_botocore_presigned_get_verifies() {
        let headers = headers(&[("host", "localhost:9324")]);
        let query = "Action=ReceiveMessage&Version=2012-11-05&X-Amz-Algorithm=AWS4-HMAC-SHA256\
                     &X-Amz-Credential=AKIDEXAMPLE%2F20260830%2Fus-east-1%2Fsqs%2Faws4_request\
                     &X-Amz-Date=20260830T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host\
                     &X-Amz-Signature=\
                     c25d475cacef18e88e7aa5143bf5556a368c22cdaf8510b97b87d90c4804c9f3";
        let req = request("GET", "/000000000000/orders", query, &headers, "");
        let got = verify(&req, &directory(), "queen-1", NOW_MS).unwrap();
        assert_eq!(got.access_key_id, AKID);
        // Still inside its 900 seconds, and no longer after them.
        assert!(verify(&req, &directory(), "queen-1", NOW_MS + 899_000).is_ok());
    }

    /// The other presigner. The JavaScript v3 flavour signs `UNSIGNED-PAYLOAD`
    /// for a service that is not S3, so a verifier that only tries the body
    /// digest locks it out of every presigned URL.
    #[test]
    fn a_presigner_that_signs_unsigned_payload_verifies_too() {
        let headers = headers(&[("host", "localhost:9324")]);
        let query = "Action=ReceiveMessage&X-Amz-Algorithm=AWS4-HMAC-SHA256\
                     &X-Amz-Credential=AKIDEXAMPLE%2F20260830%2Fus-east-1%2Fsqs%2Faws4_request\
                     &X-Amz-Date=20260830T120000Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host\
                     &X-Amz-Signature=\
                     29fcb61f1f5cf87bab200f728a3305db8032c5300ebb749b61578e0644724cf4";
        let req = request("GET", "/000000000000/orders", query, &headers, "");
        assert!(verify(&req, &directory(), "queen-1", NOW_MS).is_ok());
    }

    /// SNS shares the listener and signs for its own service, in whatever region
    /// its client was configured with. Both are accepted: the service because it
    /// is one of the two this facade answers, the region because the client
    /// chose it and this deployment's own label ("queen-1", passed here) is not
    /// a thing any SDK will ever be told.
    #[test]
    fn an_sns_request_in_a_foreign_region_verifies() {
        let headers = headers(&[
            (
                "authorization",
                &authz_scoped(
                    "20260830/eu-central-1/sns",
                    "content-type;host;x-amz-date",
                    "37d7f11bd4813b00689e6e9e68f97d6c65e24d5f6b37dfdf3352322e8142a16f",
                ),
            ),
            (
                "content-type",
                "application/x-www-form-urlencoded; charset=utf-8",
            ),
            ("host", "localhost:9324"),
            ("x-amz-date", "20260830T120000Z"),
        ]);
        let body = "Action=Publish&Version=2010-03-31\
                    &TopicArn=arn%3Aaws%3Asns%3Aqueen-1%3A000000000000%3Aevents&Message=hi";
        let got = verify(
            &request("POST", "/", "", &headers, body),
            &directory(),
            "queen-1",
            NOW_MS,
        )
        .unwrap();
        assert_eq!(got.service, SERVICE_SNS);
    }

    /// The encoding rules, all four in one vector: a `.fifo` name (a dot in a
    /// segment is not a dot SEGMENT), a space that must survive as `%2520`, a
    /// query sorted by byte with its own encoding untouched, and an empty value.
    #[test]
    fn the_path_and_query_encoding_rules_hold_on_a_fifo_queue_url() {
        let headers = headers(&[
            (
                "authorization",
                &authz(
                    "host;x-amz-date",
                    "d7f4c96250c8ed660ce2ecdc4f226309b995ea850d2ffe88e944c8d6a5f07569",
                ),
            ),
            ("host", "localhost:9324"),
            ("x-amz-date", "20260830T120000Z"),
        ]);
        let path = "/000000000000/my%20orders.fifo";
        let query = "b=%2Fslash&a=tilde~&c=plus%2Bsign&Empty=";
        let req = request("GET", path, query, &headers, "");
        assert!(verify(&req, &directory(), "queen-1", NOW_MS).is_ok());
        // And the canonical form botocore built, verbatim.
        let signed = ["host".to_string(), "x-amz-date".to_string()];
        assert_eq!(
            canonical_request(&req, &signed, &sha256_hex(b"")),
            "GET\n/000000000000/my%2520orders.fifo\n\
             Empty=&a=tilde~&b=%2Fslash&c=plus%2Bsign\n\
             host:localhost:9324\nx-amz-date:20260830T120000Z\n\n\
             host;x-amz-date\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    /// Given a `Date` header, botocore deletes `X-Amz-Date` and signs the
    /// RFC-1123 value — with the `-0000` zone spelling, not `GMT`. A verifier
    /// that only reads `X-Amz-Date` refuses this ordinary request.
    #[test]
    fn a_request_dated_with_the_date_header_verifies() {
        let headers = headers(&[
            (
                "authorization",
                &authz(
                    "content-type;date;host;x-amz-target",
                    "a91a48f99e7d5b377b30bb8c073da8255acd5a725d88eb70e58e5c9cc9f40cb6",
                ),
            ),
            ("content-type", "application/x-amz-json-1.0"),
            ("date", "Sun, 30 Aug 2026 12:00:00 -0000"),
            ("host", "localhost:9324"),
            ("x-amz-target", "AmazonSQS.ListQueues"),
        ]);
        let got = verify(
            &request("POST", "/", "", &headers, "{}"),
            &directory(),
            "queen-1",
            NOW_MS,
        )
        .unwrap();
        assert_eq!(got.signed_at_ms, NOW_MS);
    }

    /// Trimall: leading and trailing whitespace gone, inner runs collapsed to
    /// one space. The value on the wire keeps its padding.
    #[test]
    fn header_values_are_trimmed_and_their_inner_runs_collapsed() {
        let headers = headers(&[
            (
                "authorization",
                &authz(
                    "content-type;host;x-amz-date;x-amz-meta-note;x-amz-target",
                    "7fbf5c2eb465c7d6c661e3a77f2ced3e0df44c265c504037966cc9042c581494",
                ),
            ),
            ("content-type", "application/x-amz-json-1.0"),
            ("host", "localhost:9324"),
            ("x-amz-date", "20260830T120000Z"),
            ("x-amz-meta-note", "  a   b  "),
            ("x-amz-target", "AmazonSQS.ListQueues"),
        ]);
        assert!(verify(
            &request("POST", "/", "", &headers, "{}"),
            &directory(),
            "queen-1",
            NOW_MS
        )
        .is_ok());
    }

    /// The golden: the AWS documentation's example key at the AWS SigV4 test
    /// suite's own timestamp, signed for `sqs` by botocore, verified end to end.
    #[test]
    fn the_golden_aws_example_key_verifies_end_to_end() {
        let headers = headers(&[
            (
                "authorization",
                &authz_scoped(
                    "20150830/us-east-1/sqs",
                    "content-type;host;x-amz-date;x-amz-target",
                    "f1f446d481babb69893208e88662ef7545cd03b671a3a598ab3e88e779311a69",
                ),
            ),
            ("content-type", "application/x-amz-json-1.0"),
            ("host", "localhost:9324"),
            ("x-amz-date", "20150830T123600Z"),
            ("x-amz-target", "AmazonSQS.ListQueues"),
        ]);
        let req = request("POST", "/", "", &headers, r#"{"MaxResults":10}"#);
        let got = verify(&req, &directory(), "queen-1", GOLDEN_MS).unwrap();
        assert_eq!(got.access_key_id, AKID);
    }

    /// The published `get-vanilla` case from aws-sig-v4-test-suite, byte for
    /// byte: canonical request, string to sign, signature. It is signed for the
    /// service literally named "service", so it exercises the ALGORITHM rather
    /// than [`verify`] — which is the point, since every other vector here would
    /// still pass if the algorithm were subtly ours instead of AWS's.
    #[test]
    fn the_published_get_vanilla_vector_reproduces_byte_for_byte() {
        let headers = headers(&[
            ("host", "example.amazonaws.com"),
            ("x-amz-date", "20150830T123600Z"),
        ]);
        let req = request("GET", "/", "", &headers, "");
        let signed = ["host".to_string(), "x-amz-date".to_string()];
        let canonical = canonical_request(&req, &signed, &sha256_hex(b""));
        assert_eq!(
            canonical,
            "GET\n/\n\nhost:example.amazonaws.com\nx-amz-date:20150830T123600Z\n\n\
             host;x-amz-date\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        let sts = string_to_sign(
            "20150830T123600Z",
            "20150830/us-east-1/service/aws4_request",
            &canonical,
        );
        assert_eq!(
            sts,
            "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/service/aws4_request\n\
             bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63"
        );
        let key = signing_key(SECRET, "20150830", "us-east-1", "service");
        assert_eq!(
            hex::encode(hmac_sha256(&key, sts.as_bytes())),
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    // ------------------------------------------------------------- refusals

    fn refused(req: &SignedRequest<'_>, now_ms: i64) -> SqsError {
        verify(req, &directory(), "queen-1", now_ms).unwrap_err()
    }

    /// The whole reason the body is hashed rather than trusted.
    #[test]
    fn a_tampered_body_does_not_verify() {
        let headers = headers(&[
            (
                "authorization",
                &authz(
                    "content-type;host;x-amz-date",
                    "02c85e759cc5e2ce840146c82043bf13598030989142aceb4e9627da8ac45d39",
                ),
            ),
            (
                "content-type",
                "application/x-www-form-urlencoded; charset=utf-8",
            ),
            ("host", "localhost:9324"),
            ("x-amz-date", "20260830T120000Z"),
        ]);
        let tampered = request(
            "POST",
            "/",
            "",
            &headers,
            "Action=PurgeQueue&Version=2012-11-05",
        );
        assert_eq!(
            refused(&tampered, NOW_MS).kind,
            ErrorKind::SignatureDoesNotMatch
        );
        // And so does every other byte of the canonical request: the method, the
        // path, the query and a signed header's value.
        let body = "Action=ListQueues&Version=2012-11-05";
        assert_eq!(
            refused(&request("GET", "/", "", &headers, body), NOW_MS).kind,
            ErrorKind::SignatureDoesNotMatch
        );
        assert_eq!(
            refused(&request("POST", "/other", "", &headers, body), NOW_MS).kind,
            ErrorKind::SignatureDoesNotMatch
        );
        assert_eq!(
            refused(&request("POST", "/", "a=1", &headers, body), NOW_MS).kind,
            ErrorKind::SignatureDoesNotMatch
        );
        let moved = headers
            .iter()
            .map(|(k, v)| {
                let v = if k == "host" {
                    "elsewhere:9324"
                } else {
                    v.as_str()
                };
                (k.clone(), v.to_string())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            refused(&request("POST", "/", "", &moved, body), NOW_MS).kind,
            ErrorKind::SignatureDoesNotMatch
        );
    }

    /// A signed `x-amz-content-sha256` that names a digest is the canonical
    /// request's payload line, so a request whose body was swapped would verify
    /// against the OLD digest. It is checked against the body instead.
    #[test]
    fn a_content_sha256_that_disagrees_with_the_body_is_refused() {
        let headers = headers(&[
            (
                "authorization",
                &authz(
                    "host;x-amz-content-sha256;x-amz-date",
                    "00".repeat(32).as_str(),
                ),
            ),
            ("host", "localhost:9324"),
            ("x-amz-content-sha256", &sha256_hex(b"the body it signed")),
            ("x-amz-date", "20260830T120000Z"),
        ]);
        let err = refused(
            &request("POST", "/", "", &headers, "a different body"),
            NOW_MS,
        );
        assert_eq!(err.kind, ErrorKind::SignatureDoesNotMatch);
        assert!(
            err.message.contains("x-amz-content-sha256"),
            "{}",
            err.message
        );
    }

    #[test]
    fn a_clock_outside_the_skew_window_is_refused_in_both_directions() {
        let headers = headers(&[
            (
                "authorization",
                &authz(
                    "content-type;host;x-amz-date",
                    "02c85e759cc5e2ce840146c82043bf13598030989142aceb4e9627da8ac45d39",
                ),
            ),
            (
                "content-type",
                "application/x-www-form-urlencoded; charset=utf-8",
            ),
            ("host", "localhost:9324"),
            ("x-amz-date", "20260830T120000Z"),
        ]);
        let req = request(
            "POST",
            "/",
            "",
            &headers,
            "Action=ListQueues&Version=2012-11-05",
        );
        // The edges themselves are inside the window.
        assert!(verify(&req, &directory(), "queen-1", NOW_MS + MAX_CLOCK_SKEW_MS).is_ok());
        assert!(verify(&req, &directory(), "queen-1", NOW_MS - MAX_CLOCK_SKEW_MS).is_ok());
        let late = refused(&req, NOW_MS + MAX_CLOCK_SKEW_MS + 1);
        assert_eq!(late.kind, ErrorKind::SignatureDoesNotMatch);
        assert!(
            late.message.starts_with("Signature expired:"),
            "{}",
            late.message
        );
        // The server's own clock is in the sentence: it is the number the client
        // could not have known, and the one that explains the refusal.
        assert!(
            late.message.contains("20260830T121500Z"),
            "{}",
            late.message
        );
        let early = refused(&req, NOW_MS - MAX_CLOCK_SKEW_MS - 1);
        assert!(
            early.message.starts_with("Signature not yet current:"),
            "{}",
            early.message
        );
    }

    #[test]
    fn a_presigned_url_expires_when_its_own_lifetime_runs_out() {
        let headers = headers(&[("host", "localhost:9324")]);
        let query = "Action=ReceiveMessage&Version=2012-11-05&X-Amz-Algorithm=AWS4-HMAC-SHA256\
                     &X-Amz-Credential=AKIDEXAMPLE%2F20260830%2Fus-east-1%2Fsqs%2Faws4_request\
                     &X-Amz-Date=20260830T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host\
                     &X-Amz-Signature=\
                     c25d475cacef18e88e7aa5143bf5556a368c22cdaf8510b97b87d90c4804c9f3";
        let req = request("GET", "/000000000000/orders", query, &headers, "");
        let err = refused(&req, NOW_MS + 900_001);
        assert_eq!(err.kind, ErrorKind::SignatureDoesNotMatch);
        assert!(
            err.message.starts_with("Signature expired:"),
            "{}",
            err.message
        );
        // Past the skew window on the OTHER side is a clock problem, not an
        // expiry: a presigned URL is supposed to be old.
        let early = refused(&req, NOW_MS - MAX_CLOCK_SKEW_MS - 1);
        assert!(
            early.message.starts_with("Signature not yet current:"),
            "{}",
            early.message
        );
    }

    #[test]
    fn a_presign_lifetime_outside_the_documented_range_is_refused() {
        for expires in ["0", "-1", "604801", "soon", ""] {
            let query = format!(
                "X-Amz-Algorithm=AWS4-HMAC-SHA256\
                 &X-Amz-Credential=AKIDEXAMPLE%2F20260830%2Fus-east-1%2Fsqs%2Faws4_request\
                 &X-Amz-Date=20260830T120000Z&X-Amz-Expires={expires}&X-Amz-SignedHeaders=host\
                 &X-Amz-Signature=00"
            );
            let err = parse_presigned(&query).unwrap().unwrap_err();
            assert_eq!(err.kind, ErrorKind::IncompleteSignature, "{expires}");
        }
    }

    /// AWS tells an unknown key id apart from a bad signature, and so does this:
    /// the directory is operator config, not a user table, so there is no
    /// enumeration worth trading a debuggable error for.
    #[test]
    fn an_unknown_access_key_id_is_told_apart_from_a_bad_signature() {
        let headers = headers(&[
            (
                "authorization",
                &authz(
                    "content-type;host;x-amz-date",
                    "02c85e759cc5e2ce840146c82043bf13598030989142aceb4e9627da8ac45d39",
                ),
            ),
            (
                "content-type",
                "application/x-www-form-urlencoded; charset=utf-8",
            ),
            ("host", "localhost:9324"),
            ("x-amz-date", "20260830T120000Z"),
        ]);
        let req = request(
            "POST",
            "/",
            "",
            &headers,
            "Action=ListQueues&Version=2012-11-05",
        );
        let other = Directory::from_spec(&format!("SOMEONEELSE:{SECRET}:tok")).unwrap();
        let err = verify(&req, &other, "queen-1", NOW_MS).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidClientTokenId);
        // An empty directory is the `auth=sigv4` misconfiguration, and it
        // answers the same thing rather than a signature error.
        let err = verify(&req, &Directory::empty(), "queen-1", NOW_MS).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidClientTokenId);
        // The right key id with the wrong secret is the OTHER error.
        let wrong = Directory::from_spec(&format!("{AKID}:not-the-secret:tok")).unwrap();
        let err = verify(&req, &wrong, "queen-1", NOW_MS).unwrap_err();
        assert_eq!(err.kind, ErrorKind::SignatureDoesNotMatch);
        assert_eq!(err.message, MISMATCH);
    }

    /// The one scope field that IS pinned. A request signed for `iam` arrived at
    /// the wrong door, and its own SDK will say so.
    #[test]
    fn a_scope_naming_another_service_is_refused() {
        let headers = headers(&[
            (
                "authorization",
                &authz_scoped(
                    "20260830/us-east-1/iam",
                    "content-type;host;x-amz-date;x-amz-target",
                    "7fe855e4d28dde383d180c4a868b61057248af410ca6abe6936326ce6abd9fd7",
                ),
            ),
            ("content-type", "application/x-amz-json-1.0"),
            ("host", "localhost:9324"),
            ("x-amz-date", "20260830T120000Z"),
            ("x-amz-target", "AmazonSQS.ListQueues"),
        ]);
        let err = refused(&request("POST", "/", "", &headers, "{}"), NOW_MS);
        assert_eq!(err.kind, ErrorKind::SignatureDoesNotMatch);
        assert!(
            err.message.contains("scoped to correct service"),
            "{}",
            err.message
        );
    }

    /// A scope date that disagrees with the request's own date is refused before
    /// anything is hashed: it cannot verify, and the reason is worth saying.
    #[test]
    fn a_scope_date_that_is_not_the_requests_date_is_named() {
        let headers = headers(&[
            (
                "authorization",
                &authz_scoped("20260829/us-east-1/sqs", "host;x-amz-date", &"0".repeat(64)),
            ),
            ("host", "localhost:9324"),
            ("x-amz-date", "20260830T120000Z"),
        ]);
        let err = refused(&request("POST", "/", "", &headers, ""), NOW_MS);
        assert!(
            err.message.contains("Date in Credential scope"),
            "{}",
            err.message
        );
    }

    #[test]
    fn a_request_with_no_credential_at_all_is_a_missing_token() {
        let headers = headers(&[("host", "localhost:9324")]);
        let err = refused(&request("POST", "/", "", &headers, "{}"), NOW_MS);
        assert_eq!(err.kind, ErrorKind::MissingAuthenticationToken);
    }

    /// Each malformed `Authorization` says which part is wrong. The client that
    /// sent it is a piece of software: the sentence is for the human reading its
    /// log.
    #[test]
    fn a_malformed_authorization_header_names_what_is_wrong() {
        let cases = [
            ("Basic dXNlcjpwYXNz", "Unsupported AWS 'algorithm'"),
            (
                "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=00",
                "requires 'Credential' parameter",
            ),
            (
                "AWS4-HMAC-SHA256 Credential=A/20260830/us-east-1/sqs/aws4_request, Signature=00",
                "requires 'SignedHeaders' parameter",
            ),
            (
                "AWS4-HMAC-SHA256 Credential=A/20260830/us-east-1/sqs/aws4_request, \
                 SignedHeaders=host",
                "requires 'Signature' parameter",
            ),
            (
                "AWS4-HMAC-SHA256 Credential=A/20260830/us-east-1/sqs, SignedHeaders=host, \
                 Signature=00",
                "Credential must be in the format",
            ),
            (
                "AWS4-HMAC-SHA256 Credential=A/2026-08-30/us-east-1/sqs/aws4_request, \
                 SignedHeaders=host, Signature=00",
                "date must be YYYYMMDD",
            ),
            (
                "AWS4-HMAC-SHA256 Credential=A/20260830/us-east-1/sqs/aws4_request, \
                 SignedHeaders=content-type, Signature=00",
                "must include 'host'",
            ),
        ];
        for (header, expected) in cases {
            let err = parse_authorization(header).unwrap_err();
            assert_eq!(err.kind, ErrorKind::IncompleteSignature, "{header}");
            assert!(
                err.message.contains(expected),
                "{header} answered {}",
                err.message
            );
        }
    }

    #[test]
    fn a_signed_header_the_request_does_not_carry_is_named() {
        let headers = headers(&[
            (
                "authorization",
                &authz("host;x-amz-date;x-amz-target", &"0".repeat(64)),
            ),
            ("host", "localhost:9324"),
            ("x-amz-date", "20260830T120000Z"),
        ]);
        let err = refused(&request("POST", "/", "", &headers, ""), NOW_MS);
        assert_eq!(err.kind, ErrorKind::SignatureDoesNotMatch);
        assert!(err.message.contains("x-amz-target"), "{}", err.message);
    }

    #[test]
    fn a_request_with_no_date_anywhere_says_which_headers_would_do() {
        let headers = headers(&[
            ("authorization", &authz("host", &"0".repeat(64))),
            ("host", "localhost:9324"),
        ]);
        let err = refused(&request("POST", "/", "", &headers, ""), NOW_MS);
        assert_eq!(err.kind, ErrorKind::IncompleteSignature);
        assert!(err.message.contains("'X-Amz-Date'"), "{}", err.message);
        assert!(err.message.contains("'Date'"), "{}", err.message);
    }

    // ------------------------------------------------------------ the pieces

    #[test]
    fn uri_encode_is_the_unreserved_set_and_uppercase_hex() {
        assert_eq!(uri_encode("abcXYZ019-._~", true), "abcXYZ019-._~");
        assert_eq!(uri_encode("/a/b", false), "/a/b");
        assert_eq!(uri_encode("/a/b", true), "%2Fa%2Fb");
        assert_eq!(uri_encode("a b", true), "a%20b");
        assert_eq!(uri_encode("a+b", true), "a%2Bb");
        // Already-encoded input is encoded AGAIN: that is the double encoding,
        // and it is why a queue named "my queue" survives the round trip.
        assert_eq!(uri_encode("/q/my%20queue", false), "/q/my%2520queue");
        // Hex is uppercase, and multi-byte UTF-8 goes byte by byte.
        assert_eq!(uri_encode("ü", true), "%C3%BC");
        assert_eq!(uri_encode("=&?", true), "%3D%26%3F");
    }

    /// The verifier reads a query string nobody has authenticated yet, so every
    /// byte sequence a client can put in a URL has to have an ANSWER rather than
    /// a panic. `%` followed by the first byte of a multi-byte character is the
    /// one that used to abort the request with no reply at all.
    #[test]
    fn a_percent_escape_over_a_character_boundary_decodes_instead_of_panicking() {
        assert_eq!(percent_decode("%a€"), "%a€");
        assert_eq!(percent_decode("%€%"), "%€%");
        assert_eq!(percent_decode("€%"), "€%");
        // The ordinary cases still decode, in both hex cases.
        assert_eq!(percent_decode("a%2Fb"), "a/b");
        assert_eq!(percent_decode("a%2fb"), "a/b");
        assert_eq!(percent_decode("%C3%BC"), "ü");
        // A truncated or malformed escape is a literal `%`.
        assert_eq!(percent_decode("%zz"), "%zz");
        assert_eq!(percent_decode("100%"), "100%");
        // ...and the parameter reader that runs before any credential lookup
        // reaches the same bytes.
        assert_eq!(
            query_param(
                "X-Amz-Algorithm=%a€&X-Amz-Date=20260830T101500Z",
                "x-amz-date"
            ),
            Some("20260830T101500Z".to_string())
        );
    }

    /// The reference signers normalize the path before they encode it. A
    /// verifier that did not would refuse a request nobody got wrong.
    #[test]
    fn the_canonical_uri_removes_dot_segments_the_way_the_signers_do() {
        assert_eq!(normalize_path("/"), "/");
        assert_eq!(
            normalize_path("/000000000000/orders"),
            "/000000000000/orders"
        );
        assert_eq!(normalize_path("//a//b"), "/a/b");
        assert_eq!(normalize_path("/a/./b"), "/a/b");
        assert_eq!(normalize_path("/a/../b"), "/b");
        assert_eq!(normalize_path("/a/b/.."), "/a/");
        assert_eq!(normalize_path("/.."), "/");
        assert_eq!(normalize_path("/a/"), "/a/");
        assert_eq!(normalize_path(""), "");
        // A `.` INSIDE a segment is not a dot segment: `orders.fifo` is a queue.
        assert_eq!(
            normalize_path("/0/orders.fifo/./x"),
            "/0/orders.fifo/x",
            "only whole segments are dot segments"
        );
        // ...and the canonical request carries the normalized form, doubly
        // encoded like every other path.
        let headers = headers(&[("host", "localhost:9324")]);
        let canonical = canonical_request(
            &request("POST", "/0/./orders", "", &headers, ""),
            &["host".to_string()],
            "UNSIGNED-PAYLOAD",
        );
        assert!(canonical.starts_with("POST\n/0/orders\n"), "{canonical}");
    }

    #[test]
    fn the_canonical_query_sorts_by_byte_and_keeps_the_clients_encoding() {
        assert_eq!(canonical_query(""), "");
        assert_eq!(canonical_query("b=2&a=1"), "a=1&b=2");
        // Uppercase sorts before lowercase because the sort is over bytes.
        assert_eq!(canonical_query("a=1&B=2"), "B=2&a=1");
        // Repeated names sort by value, second.
        assert_eq!(canonical_query("a=2&a=1"), "a=1&a=2");
        // A name with no `=` gets one; the client's own encoding is untouched,
        // in either direction.
        assert_eq!(canonical_query("flag&x=%7E"), "flag=&x=%7E");
        assert_eq!(canonical_query("x=~"), "x=~");
        // The signature parameter is never part of what was signed.
        assert_eq!(canonical_query("X-Amz-Signature=deadbeef&a=1"), "a=1");
    }

    #[test]
    fn duplicate_headers_are_joined_with_commas_in_arrival_order() {
        let headers = headers(&[
            ("host", "localhost:9324"),
            ("x-amz-meta-tag", " first "),
            ("x-amz-meta-tag", "second   value"),
        ]);
        let req = request("POST", "/", "", &headers, "");
        assert_eq!(
            header_value(&req, "x-amz-meta-tag").as_deref(),
            Some("first,second value")
        );
        assert_eq!(
            header_value(&req, "X-Amz-Meta-Tag").as_deref(),
            Some("first,second value")
        );
        assert_eq!(header_value(&req, "absent"), None);
    }

    #[test]
    fn presented_access_key_id_reads_both_variants_without_verifying() {
        let headers = headers(&[
            ("authorization", &authz("host", &"0".repeat(64))),
            ("host", "localhost:9324"),
        ]);
        // A signature of zeroes: nothing here verifies, and that is the point.
        assert_eq!(
            presented_access_key_id(&request("POST", "/", "", &headers, "")).as_deref(),
            Some(AKID)
        );
        let bare = vec![("host".to_string(), "localhost:9324".to_string())];
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256\
                     &X-Amz-Credential=AKIDEXAMPLE%2F20260830%2Fus-east-1%2Fsqs%2Faws4_request\
                     &X-Amz-Date=20260830T120000Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host\
                     &X-Amz-Signature=00";
        assert_eq!(
            presented_access_key_id(&request("GET", "/", query, &bare, "")).as_deref(),
            Some(AKID)
        );
        assert_eq!(
            presented_access_key_id(&request("GET", "/", "", &bare, "")),
            None
        );
    }

    /// A query with no `X-Amz-Algorithm` is not a presigned request, and saying
    /// so is what selects the header variant.
    #[test]
    fn a_query_without_the_algorithm_parameter_is_not_presigned() {
        assert!(parse_presigned("").is_none());
        assert!(parse_presigned("Action=ListQueues&Version=2012-11-05").is_none());
        assert!(parse_presigned("X-Amz-Algorithm=AWS4-HMAC-SHA256").is_some());
        // Present but not ours: an error, not a fall-through to the header
        // variant, because a client that asked for AWS4-ECDSA is not going to be
        // helped by "missing Authentication Token".
        let err = parse_presigned("X-Amz-Algorithm=AWS4-ECDSA-P256-SHA256")
            .unwrap()
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::IncompleteSignature);
    }

    #[test]
    fn the_presigned_scope_arrives_percent_encoded_and_is_decoded() {
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256\
                     &X-Amz-Credential=AKIDEXAMPLE%2F20260830%2Feu-central-1%2Fsns%2Faws4_request\
                     &X-Amz-Date=20260830T120000Z&X-Amz-Expires=60\
                     &X-Amz-SignedHeaders=host%3Bx-amz-date&X-Amz-Signature=abc";
        let params = parse_presigned(query).unwrap().unwrap();
        assert_eq!(params.credential.access_key_id, AKID);
        assert_eq!(params.credential.region, "eu-central-1");
        assert_eq!(params.credential.service, SERVICE_SNS);
        assert_eq!(params.credential.signed_headers, ["host", "x-amz-date"]);
        assert_eq!(params.expires_seconds, 60);
        assert_eq!(params.amz_date, "20260830T120000Z");
    }

    #[test]
    fn the_compact_timestamp_round_trips_through_the_civil_calendar() {
        for (compact, epoch_ms) in [
            ("19700101T000000Z", 0),
            ("20150830T123600Z", GOLDEN_MS),
            ("20260830T120000Z", NOW_MS),
            // A leap day, and the last second of a leap year.
            ("20240229T235959Z", 1_709_251_199_000),
            ("20241231T235959Z", 1_735_689_599_000),
        ] {
            assert_eq!(epoch_ms_of(compact), Some(epoch_ms), "{compact}");
            assert_eq!(compact_from_epoch_ms(epoch_ms), compact);
        }
        for malformed in [
            "",
            "20260830",
            "20260830T120000",
            "20260830X120000Z",
            "2026-08-30T12:00:00Z",
            "20261330T120000Z",
        ] {
            assert_eq!(epoch_ms_of(malformed), None, "{malformed}");
        }
    }

    #[test]
    fn the_http_date_forms_that_are_read_are_the_ones_that_mean_utc() {
        for value in [
            "Sun, 30 Aug 2026 12:00:00 GMT",
            "Sun, 30 Aug 2026 12:00:00 -0000",
            "Sun, 30 Aug 2026 12:00:00 +0000",
            "30 Aug 2026 12:00:00 UTC",
        ] {
            assert_eq!(epoch_ms_of_http_date(value), Some(NOW_MS), "{value}");
        }
        // A real offset is refused rather than applied: HTTP mandates GMT, and
        // guessing here moves the skew window by hours.
        assert_eq!(
            epoch_ms_of_http_date("Sun, 30 Aug 2026 14:00:00 +0200"),
            None
        );
        assert_eq!(
            epoch_ms_of_http_date("Sunday, 30-Aug-26 12:00:00 GMT"),
            None
        );
        assert_eq!(epoch_ms_of_http_date("nonsense"), None);
    }

    #[test]
    fn percent_decoding_leaves_what_is_not_an_escape_alone() {
        assert_eq!(percent_decode("a%2Fb"), "a/b");
        assert_eq!(percent_decode("a%2fb"), "a/b");
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("100%2"), "100%2");
        assert_eq!(percent_decode("%zz"), "%zz");
        // A `+` in a QUERY is a literal plus. Only a form body spells a space
        // that way, and a form body is not this function's input.
        assert_eq!(percent_decode("a+b"), "a+b");
        assert_eq!(percent_decode("%C3%BC"), "ü");
    }

    #[test]
    fn the_signing_key_is_the_four_step_ladder() {
        // The AWS documentation's own worked example: the derived key for
        // 20150830/us-east-1/iam is published, so this pins the ladder itself
        // rather than one signature that used it.
        let key = signing_key(SECRET, "20150830", "us-east-1", "iam");
        assert_eq!(
            hex::encode(key),
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
        );
    }
}
