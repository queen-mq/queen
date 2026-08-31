//! The receipt handle: everything a delete needs, carried by the client.
//!
//! CONTRACT. `encode` turns one delivered message into an opaque string;
//! `decode` turns it back, or refuses it. A handle is SELF-CONTAINED and
//! TAMPER-EVIDENT, and those two properties are what let this facade keep the
//! sentence PLAN_QUEEN_SQS.md opens with: *any instance answers any request*. A
//! handle that referred to server-side state would make the delete stick to the
//! instance that served the receive, and a plain Service in front of two
//! replicas would start losing deletes.
//!
//! ```text
//! base64url( {q, p, t, l, d, x} ) . base64url( HMAC-SHA256(secret, payload)[..16] )
//! ```
//!
//! What each field is FOR — none of them is decoration:
//!
//!   * `q` the queue name, so a handle presented against another queue is
//!     refused rather than acted on;
//!   * `p` the PARTITION ID, because the ack wire is partitionId-keyed
//!     (`005_log_ack.sql`) and the deleting instance never saw the receive;
//!   * `t` the transactionId, which is what an ack addresses the message by;
//!   * `l` the leaseId. THE load-bearing field: a handle from a PREVIOUS
//!     delivery of the same message names a lease that is gone, so it fails on
//!     mismatch instead of deleting whatever is in flight now — which is exactly
//!     AWS's own contract (`ReceiptHandleIsInvalid`, then `MessageNotInflight`);
//!   * `m` the broker's MESSAGE UUID. The transaction id above is the client's
//!     `MessageDeduplicationId`, which is unique only inside the queue's dedup
//!     window — so one FIFO claim CAN hold two messages under one `t`, and a
//!     delete-set keyed by `t` would mark both from one delete
//!     ([`crate::actions::fifo`]). This is what names ONE of them;
//!   * `x` the expiry, so a handle cannot be replayed for ever.
//!
//! The tag is HMAC and not a hash: a hash of public fields is not a signature,
//! and a handle a client can MINT is a client that can delete a message it never
//! received. The key is `QUEEN_SQS_HANDLE_SECRET` when the operator sets one —
//! and it must be set for a multi-instance deployment, or each instance mints
//! handles the others reject. Unset, one is generated per process, which is
//! correct for a single instance and fails loudly rather than silently the day a
//! second one appears.
//!
//! ## Three details of the encoding
//!
//! The tag is computed over the base64 TEXT of the payload rather than over the
//! JSON bytes it decodes to. Verification then hashes exactly the characters the
//! client sent, and no question about JSON re-serialization — key order,
//! integer spelling, escaping — can ever put a valid handle and its own tag on
//! different sides of a comparison.
//!
//! The alphabet is base64url WITHOUT padding, so a handle survives being put in
//! a query string, a header or a log line unquoted: SDKs pass receipt handles
//! around as opaque strings and several of them URL-encode nothing.
//!
//! The tag is truncated to 128 bits. That is not a weakened MAC in any sense
//! that matters here — forging one is 2^128 work — and it keeps a handle at
//! roughly 240 characters, well inside [`MAX_HANDLE_BYTES`], which is itself a
//! cap on how much an unauthenticated caller can make this process hash.

use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::error::{ErrorKind, SqsError, SqsResult};

/// The longest handle this facade will look at. A real one is around 240 bytes
/// (a queue name of at most 80 characters, three uuids, two integers); the cap
/// is generous and its job is to bound the work a caller can ask for before the
/// tag has verified anything.
pub const MAX_HANDLE_BYTES: usize = 1024;

/// Bytes of HMAC-SHA256 kept, left-truncated (module header).
const TAG_BYTES: usize = 16;

/// URL-safe, unpadded: a handle is passed around by clients that quote nothing.
const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// The ONE sentence every refusal answers with. Written once, deliberately: a
/// handle that does not decode, one whose tag does not verify and one that has
/// expired must be indistinguishable, and two different messages would tell a
/// forger which half of the handle to work on.
const INVALID: &str = "The input receipt handle is not a valid receipt handle.";

type HmacSha256 = Hmac<Sha256>;

/// What a handle carries. Field names on the wire are one character because the
/// whole thing is base64'd into a header-sized string that clients pass around,
/// echo in logs and sometimes put in a URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    pub queue: String,
    /// The opaque partition id an ack is addressed by — NOT the partition name.
    pub partition_id: String,
    pub transaction_id: String,
    pub lease_id: String,
    /// The broker's message uuid — the only field that names ONE delivered
    /// message when a claim holds two under one dedup key (module header).
    pub message_id: String,
    /// Epoch milliseconds after which this handle is refused. Set from the
    /// visibility timeout plus slack: a handle outliving its own lease can only
    /// produce `MessageNotInflight`, and refusing it here saves the round trip.
    pub expires_at_ms: i64,
}

/// The payload, as it is spelled inside the base64.
///
/// `deny_unknown_fields` and every field mandatory: the tag has already proved
/// this facade wrote the bytes, so a payload that does not match is a version
/// mismatch between two instances rather than an attack — and answering it as a
/// refusal is what makes such a rollout visible instead of silently wrong.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    q: String,
    p: String,
    t: String,
    l: String,
    m: String,
    x: i64,
}

/// The minter and the verifier, holding the process's HMAC key.
pub struct Handles {
    secret: Vec<u8>,
}

/// Deliberately says nothing: the key mints handles, and a `Debug` that printed
/// it would put it in any log line that formats the facade.
impl std::fmt::Debug for Handles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handles")
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl Handles {
    pub fn new(secret: &[u8]) -> Handles {
        Handles {
            secret: secret.to_vec(),
        }
    }

    pub fn encode(&self, receipt: &Receipt) -> String {
        let wire = Wire {
            q: receipt.queue.clone(),
            p: receipt.partition_id.clone(),
            t: receipt.transaction_id.clone(),
            l: receipt.lease_id.clone(),
            m: receipt.message_id.clone(),
            x: receipt.expires_at_ms,
        };
        // Infallible: `Wire` is six owned scalars, so the only documented
        // failure of `to_vec` — a map with non-string keys, a type that refuses
        // to serialize — cannot arise.
        let payload = serde_json::to_vec(&wire).unwrap_or_default();
        let payload = B64.encode(payload);
        let tag = self.tag(payload.as_bytes());
        format!("{payload}.{}", B64.encode(&tag[..TAG_BYTES]))
    }

    /// Decode and verify. Every refusal is
    /// [`crate::error::ErrorKind::ReceiptHandleIsInvalid`] and they are
    /// deliberately indistinguishable from one another: a handle that does not
    /// decode, one whose tag does not verify and one that has expired all answer
    /// the same, because telling them apart is telling a forger which half of
    /// the handle to work on.
    ///
    /// The tag is checked in CONSTANT TIME, for the same reason the signature
    /// comparison in [`crate::credentials`] is.
    pub fn decode(&self, handle: &str, now_ms: i64) -> SqsResult<Receipt> {
        if handle.len() > MAX_HANDLE_BYTES {
            return Err(invalid());
        }
        let (payload, tag) = handle.split_once('.').ok_or_else(invalid)?;
        // A second separator means the client is holding something this facade
        // did not mint; the tag would refuse it anyway, and refusing it here
        // keeps "one dot, two halves" a property of the format.
        if tag.contains('.') {
            return Err(invalid());
        }
        let tag = B64.decode(tag).map_err(|_| invalid())?;
        if tag.len() != TAG_BYTES {
            return Err(invalid());
        }
        // Constant time, and over the CHARACTERS the client sent (module
        // header). `verify_truncated_left` is the hmac crate's own comparison —
        // there is no branch on the tag's contents anywhere in this file.
        self.mac(payload.as_bytes())
            .verify_truncated_left(&tag)
            .map_err(|_| invalid())?;

        // Authenticated from here down: these bytes are ones this facade wrote.
        let payload = B64.decode(payload).map_err(|_| invalid())?;
        let wire: Wire = serde_json::from_slice(&payload).map_err(|_| invalid())?;
        // At the expiry, not after it: a handle whose lease has just ended can
        // do nothing but produce `MessageNotInflight` one round trip later.
        if now_ms >= wire.x {
            return Err(invalid());
        }
        Ok(Receipt {
            queue: wire.q,
            partition_id: wire.p,
            transaction_id: wire.t,
            lease_id: wire.l,
            message_id: wire.m,
            expires_at_ms: wire.x,
        })
    }

    fn mac(&self, bytes: &[u8]) -> HmacSha256 {
        // Infallible for HMAC: the construction accepts a key of any length,
        // hashing one that exceeds the block size.
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.secret)
            .expect("HMAC accepts a key of any length");
        mac.update(bytes);
        mac
    }

    fn tag(&self, bytes: &[u8]) -> [u8; 32] {
        self.mac(bytes).finalize().into_bytes().into()
    }
}

/// Built field-wise rather than through `SqsError::with` so this module — and
/// the tests that pin it — stand on their own while the error catalog's
/// constructors are still being written. The value is the one those constructors
/// build.
fn invalid() -> SqsError {
    SqsError {
        kind: ErrorKind::ReceiptHandleIsInvalid,
        message: INVALID.to_string(),
        retry_after_ms: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_756_000_000_000;

    fn handles() -> Handles {
        Handles::new(b"a process secret")
    }

    fn receipt() -> Receipt {
        Receipt {
            queue: "orders".into(),
            partition_id: "9f1c2b7e-6d3a-4c1b-8f52-0a4d7e9b1c33".into(),
            transaction_id: "3c7a91d4-2e58-4b0f-9d16-77ab5e2c4d81".into(),
            lease_id: "b21e4f60-5c8d-4a37-91e2-6d0f3a8c5b74".into(),
            message_id: "5e8c0a92-1f4b-4d73-a6c8-2b91d47e0f65".into(),
            expires_at_ms: NOW + 30_000,
        }
    }

    /// Flip one character of `handle`, in the half `payload` selects, to a
    /// character that is still in the alphabet — so the refusal is the tag's and
    /// not the base64 decoder's.
    fn flip(handle: &str, payload: bool) -> String {
        let (left, right) = handle.split_once('.').unwrap();
        let half = if payload { left } else { right };
        let mut bytes = half.as_bytes().to_vec();
        let last = bytes.len() - 1;
        bytes[last] = if bytes[last] == b'A' { b'B' } else { b'A' };
        let flipped = String::from_utf8(bytes).unwrap();
        if payload {
            format!("{flipped}.{right}")
        } else {
            format!("{left}.{flipped}")
        }
    }

    #[test]
    fn a_handle_round_trips() {
        let handles = handles();
        let receipt = receipt();
        let handle = handles.encode(&receipt);
        assert_eq!(handles.decode(&handle, NOW).unwrap(), receipt);
    }

    /// Same input, same handle — an SDK that receives a message twice and
    /// compares handles is comparing two strings, and a nondeterministic
    /// encoding would make that comparison mean nothing.
    #[test]
    fn encoding_is_deterministic_and_receipt_specific() {
        let handles = handles();
        let receipt = receipt();
        assert_eq!(handles.encode(&receipt), handles.encode(&receipt));

        let mut other = receipt.clone();
        other.lease_id = "00000000-0000-4000-8000-000000000000".into();
        assert_ne!(handles.encode(&receipt), handles.encode(&other));
    }

    /// Opaque and portable: no field in the clear, nothing outside the
    /// URL-safe alphabet, and comfortably inside the cap.
    #[test]
    fn a_handle_is_opaque_url_safe_and_small() {
        let handle = handles().encode(&receipt());
        assert!(handle.len() < MAX_HANDLE_BYTES, "{} bytes", handle.len());
        assert!(!handle.contains("orders"), "the queue name is in the clear");
        assert!(
            handle
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')),
            "{handle} is not URL-safe"
        );
    }

    /// The forgery test, and the reason the tag is a MAC: the payload is public,
    /// readable and rewritable by anyone holding a handle — a client that edited
    /// `t` to another message's transaction id would be deleting a message it
    /// never received.
    #[test]
    fn a_rewritten_payload_is_refused() {
        let handles = handles();
        let handle = handles.encode(&receipt());
        assert_eq!(
            handles.decode(&flip(&handle, true), NOW).unwrap_err().kind,
            ErrorKind::ReceiptHandleIsInvalid
        );

        // The whole payload replaced, not one character: a well-formed handle
        // for a different message, tagged with nothing.
        let mut forged = receipt();
        forged.transaction_id = "00000000-0000-4000-8000-000000000000".into();
        let forged = serde_json::to_vec(&Wire {
            q: forged.queue,
            p: forged.partition_id,
            t: forged.transaction_id,
            l: forged.lease_id,
            m: forged.message_id,
            x: forged.expires_at_ms,
        })
        .unwrap();
        let (_, tag) = handle.split_once('.').unwrap();
        let forged = format!("{}.{tag}", B64.encode(forged));
        assert!(handles.decode(&forged, NOW).is_err());
    }

    #[test]
    fn a_rewritten_tag_is_refused() {
        let handles = handles();
        let handle = handles.encode(&receipt());
        assert!(handles.decode(&flip(&handle, false), NOW).is_err());
    }

    /// The multi-instance failure this facade must make LOUD: two processes with
    /// different keys mint handles the other refuses, immediately and visibly,
    /// rather than accepting them and acting on unverified fields.
    #[test]
    fn another_processes_key_is_refused() {
        let handle = handles().encode(&receipt());
        let stranger = Handles::new(b"a different process secret");
        assert_eq!(
            stranger.decode(&handle, NOW).unwrap_err().kind,
            ErrorKind::ReceiptHandleIsInvalid
        );
        // …and a key that is a prefix of the real one is a different key.
        assert!(Handles::new(b"a process").decode(&handle, NOW).is_err());
    }

    /// Expiry is a boundary, and the boundary is closed on the refusing side.
    #[test]
    fn a_handle_expires_at_its_own_deadline() {
        let handles = handles();
        let receipt = receipt();
        let handle = handles.encode(&receipt);
        let expiry = receipt.expires_at_ms;
        assert!(handles.decode(&handle, expiry - 1).is_ok());
        assert!(handles.decode(&handle, expiry).is_err());
        assert!(handles.decode(&handle, expiry + 60_000).is_err());
    }

    /// Everything a client can present that is not a handle at all. None of
    /// these may panic, and every one answers the same.
    #[test]
    fn malformed_handles_are_refused_without_panicking() {
        let handles = handles();
        let valid = handles.encode(&receipt());
        let (payload, tag) = valid.split_once('.').unwrap();
        let cases = vec![
            String::new(),
            ".".to_string(),
            "not-a-handle".to_string(),
            // The payload alone, and the tag alone.
            payload.to_string(),
            format!(".{tag}"),
            format!("{payload}."),
            // A third half.
            format!("{valid}.{tag}"),
            // Base64 that decodes to nothing this facade wrote.
            format!("{}.{tag}", B64.encode(b"not json")),
            format!("{}.{tag}", B64.encode(br#"{"q":"orders"}"#)),
            // An extra field: a newer instance's handle, refused rather than
            // half-read.
            format!(
                "{}.{tag}",
                B64.encode(br#"{"q":"q","p":"p","t":"t","l":"l","m":"m","x":9999999999999,"z":1}"#)
            ),
            // Not the alphabet.
            format!("***.{tag}"),
            format!("{payload}.***"),
            // A tag of the wrong length, right alphabet.
            format!("{payload}.{}", B64.encode([0u8; 8])),
            format!("{payload}.{}", B64.encode([0u8; 32])),
            // Over the cap: refused before anything is hashed.
            format!("{}.{tag}", "A".repeat(MAX_HANDLE_BYTES)),
        ];
        for case in cases {
            let err = handles.decode(&case, NOW).unwrap_err();
            assert_eq!(
                err.kind,
                ErrorKind::ReceiptHandleIsInvalid,
                "{case:?} answered the wrong kind"
            );
        }
    }

    /// Indistinguishable, which is the point: a forger learns nothing about
    /// which half of the handle failed.
    #[test]
    fn every_refusal_answers_the_same_sentence() {
        let handles = handles();
        let receipt = receipt();
        let handle = handles.encode(&receipt);
        let refusals = [
            handles.decode("garbage", NOW).unwrap_err(),
            handles.decode(&flip(&handle, true), NOW).unwrap_err(),
            handles.decode(&flip(&handle, false), NOW).unwrap_err(),
            handles.decode(&handle, receipt.expires_at_ms).unwrap_err(),
            Handles::new(b"another").decode(&handle, NOW).unwrap_err(),
        ];
        for refusal in &refusals {
            assert_eq!(refusal.kind, refusals[0].kind);
            assert_eq!(refusal.message, refusals[0].message);
            assert_eq!(refusal.retry_after_ms, None);
        }
    }

    /// The field a redelivery invalidates. Two deliveries of the same message
    /// differ in `l` alone, and the second handle must not be usable for the
    /// first delivery's lease — this is AWS's own contract for a stale handle.
    #[test]
    fn a_stale_delivery_is_a_different_handle() {
        let handles = handles();
        let first = receipt();
        let mut second = first.clone();
        second.lease_id = "7d5e2c19-4b3a-4f68-8e21-9c0b6a3d5f42".into();

        let first_handle = handles.encode(&first);
        let second_handle = handles.encode(&second);
        assert_ne!(first_handle, second_handle);
        // Both verify — the facade cannot know from bytes alone which lease is
        // live, and answering that is the ack's job. What it CAN guarantee is
        // that the lease id it acts on is the one that was minted with it.
        assert_eq!(
            handles.decode(&first_handle, NOW).unwrap().lease_id,
            first.lease_id
        );
        assert_eq!(
            handles.decode(&second_handle, NOW).unwrap().lease_id,
            second.lease_id
        );
    }

    /// The field that tells two messages of ONE claim apart. A dedup key is
    /// unique only inside the queue's dedup window, so a FIFO claim can hold two
    /// messages under one `t` and one lease — and the delete of one of them must
    /// not be a delete of both ([`crate::actions::fifo`]).
    #[test]
    fn two_messages_under_one_dedup_key_are_two_handles() {
        let handles = handles();
        let first = receipt();
        let mut second = first.clone();
        second.message_id = "0c93f7a1-8b25-4e6d-9f30-14a7c8e2b590".into();

        let (a, b) = (handles.encode(&first), handles.encode(&second));
        assert_ne!(a, b, "same key, same lease, different message");
        assert_eq!(
            handles.decode(&b, NOW).unwrap().message_id,
            second.message_id
        );
    }

    /// A long queue name — SQS allows 80 characters — still fits, with room to
    /// spare, so the cap is never reached by an honest handle.
    #[test]
    fn the_longest_honest_handle_fits_the_cap() {
        let mut receipt = receipt();
        receipt.queue = "q".repeat(80);
        receipt.expires_at_ms = i64::MAX;
        let handle = handles().encode(&receipt);
        assert!(
            handle.len() < MAX_HANDLE_BYTES / 2,
            "{} bytes",
            handle.len()
        );
        assert_eq!(handles().decode(&handle, NOW).unwrap(), receipt);
    }

    /// An empty key is a key: the facade never has one (config generates one
    /// when the operator sets none), and the code must not treat it specially.
    #[test]
    fn an_empty_secret_still_mints_and_verifies() {
        let keyless = Handles::new(b"");
        let receipt = receipt();
        let handle = keyless.encode(&receipt);
        assert_eq!(keyless.decode(&handle, NOW).unwrap(), receipt);
        assert!(handles().decode(&handle, NOW).is_err());
    }

    #[test]
    fn the_key_never_reaches_a_log_line() {
        assert_eq!(
            format!("{:?}", Handles::new(b"a process secret")),
            r#"Handles { secret: "<redacted>" }"#
        );
    }
}
