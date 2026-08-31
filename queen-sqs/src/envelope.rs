//! What an SQS message looks like inside a Queen payload.
//!
//! CONTRACT. `encode` turns a body plus its attributes into the JSON Queen
//! stores; `decode` turns a stored payload back into a message. `decode` NEVER
//! fails: a payload that is not this shape is a native Queen producer's own JSON
//! and is served as `body = the payload's own text, no attributes`, so mixed
//! native/SQS consumption works in both directions. That fallback is the reason
//! the keys are one character long — a strict shape is cheap to recognize and
//! the odds of a native payload colliding with it are the one acknowledged risk
//! (a native `{"b": …}` shaped payload, documented, not defended against).
//!
//! ```text
//! {"b": "<body string>",                    // always present
//!  "a": {"name": {"t": "String|Number|Binary[.custom]", "v": "<string or b64>"}},
//!  "s": {...},                              // system attributes (AWSTraceHeader)
//!  "m": "<original MessageId>"}             // only on moved (redriven) copies
//! ```
//!
//! Bodies are STRINGS in SQS, always, which is why there is no mandatory base64
//! here and why `b` is not an envelope of bytes: a JSON body round-trips as the
//! text the sender wrote, byte for byte, including its whitespace.
//!
//! Binary attribute values ARE base64 in the wire protocol and stay base64 here,
//! so a value never changes representation between the client and the store —
//! the MD5 in [`crate::md5`] is computed over the DECODED bytes and would
//! otherwise be computed over two different things on the two sides.
//!
//! ## Recognition, and how strict it is
//!
//! [`decode`](Envelope::decode) accepts an object whose keys are a SUBSET of the
//! four above, carrying `b` as a string, with every optional field of the right
//! type and every base64 value decodable. Anything else was written by someone
//! who is not this facade and is served as itself. Strictness is what keeps the
//! collision surface to the one documented shape: a native payload with an extra
//! key, a numeric `b`, or an attribute entry this facade would never write all
//! fall out to the native path instead of being half-read.
//!
//! ## The charset
//!
//! SQS restricts a body to the XML 1.0 character production, and the restriction
//! is REAL rather than advisory: a body carrying a `\0` reaches the Query
//! protocol's XML rendering and produces a document no SDK can parse, which is a
//! failure the sender cannot see and the receiver cannot explain.
//! [`validate_body`] is therefore on the send path, and it is the only thing in
//! this module that can fail.

use std::collections::BTreeMap;

use base64::Engine;
use serde_json::{Map, Value};

use crate::error::{ErrorKind, SqsError, SqsResult};

/// The envelope's four keys. Named constants because [`Envelope::decode`]'s
/// recognition test is "does it have exactly this shape", and a literal in two
/// places is how that test and the writer drift apart.
pub const KEY_BODY: &str = "b";
pub const KEY_ATTRIBUTES: &str = "a";
pub const KEY_SYSTEM: &str = "s";
pub const KEY_MOVED_FROM: &str = "m";

/// An attribute entry's two keys, under [`KEY_ATTRIBUTES`].
const KEY_TYPE: &str = "t";
const KEY_VALUE: &str = "v";

/// The type label prefix that makes a value bytes rather than text. SQS spells
/// custom binary labels `Binary.png`, so the test is on the prefix and not on
/// equality — and it is written once, here, because the parser, the encoder and
/// the digest all have to agree about which values are bytes.
const BINARY: &str = "Binary";

/// Standard alphabet WITH padding — the alphabet the SQS wire itself uses for
/// `BinaryValue`, so a value crosses the facade without ever changing
/// representation (module header).
const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

/// One message attribute's value. `String` and `Number` carry text; `Binary`
/// carries bytes, which the wire and this envelope both spell base64.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttributeValue {
    String(String),
    Binary(Vec<u8>),
}

/// One message attribute.
///
/// `data_type` is kept VERBATIM and not parsed into an enum: SQS allows custom
/// labels (`Number.float`, `Binary.png`), they are part of the MD5 the client
/// validates, and a facade that normalized them would fail that check for
/// everyone who uses one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageAttribute {
    pub data_type: String,
    pub value: AttributeValue,
}

impl MessageAttribute {
    /// A text-valued attribute: `String`, `Number`, or a custom label on either.
    pub fn string(data_type: impl Into<String>, value: impl Into<String>) -> MessageAttribute {
        MessageAttribute {
            data_type: data_type.into(),
            value: AttributeValue::String(value.into()),
        }
    }

    /// A bytes-valued attribute. The caller passes DECODED bytes; base64 is this
    /// module's spelling of them and never the caller's.
    pub fn binary(data_type: impl Into<String>, value: impl Into<Vec<u8>>) -> MessageAttribute {
        MessageAttribute {
            data_type: data_type.into(),
            value: AttributeValue::Binary(value.into()),
        }
    }
}

/// Whether a type label names bytes: `Binary`, or a custom `Binary.*`. Public
/// because the request parser has to make the same call this module does when it
/// decides whether an incoming value is `StringValue` or `BinaryValue`.
pub fn is_binary_type(data_type: &str) -> bool {
    data_type == BINARY
        || data_type
            .strip_prefix(BINARY)
            .is_some_and(|suffix| suffix.starts_with('.'))
}

/// A message, decoded.
///
/// `BTreeMap`, and it is load-bearing rather than tidy: the attribute MD5 is
/// computed over the attributes sorted by NAME, so the order has to be the
/// map's own rather than an insertion order that depends on how the request was
/// parsed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Envelope {
    pub body: String,
    pub attributes: BTreeMap<String, MessageAttribute>,
    /// System attributes — `AWSTraceHeader` is the only one AWS defines.
    pub system: BTreeMap<String, String>,
    /// The MessageId this copy was moved FROM, present only on a redriven
    /// message. It is what lets a DLQ consumer correlate back to the original
    /// after the move minted a fresh id.
    pub moved_from: Option<String>,
}

impl Envelope {
    /// A body with nothing else.
    pub fn of(body: impl Into<String>) -> Envelope {
        Envelope {
            body: body.into(),
            ..Envelope::default()
        }
    }

    /// The JSON Queen stores. Empty maps are OMITTED, so the common case — a
    /// body and nothing else — is `{"b":"…"}` and costs three bytes of overhead.
    pub fn encode(&self) -> Value {
        let mut out = Map::with_capacity(4);
        out.insert(KEY_BODY.to_string(), Value::String(self.body.clone()));
        if !self.attributes.is_empty() {
            let mut attributes = Map::with_capacity(self.attributes.len());
            for (name, attribute) in &self.attributes {
                let mut entry = Map::with_capacity(2);
                entry.insert(
                    KEY_TYPE.to_string(),
                    Value::String(attribute.data_type.clone()),
                );
                let value = match &attribute.value {
                    AttributeValue::String(text) => text.clone(),
                    AttributeValue::Binary(bytes) => B64.encode(bytes),
                };
                entry.insert(KEY_VALUE.to_string(), Value::String(value));
                attributes.insert(name.clone(), Value::Object(entry));
            }
            out.insert(KEY_ATTRIBUTES.to_string(), Value::Object(attributes));
        }
        if !self.system.is_empty() {
            let system = self
                .system
                .iter()
                .map(|(name, value)| (name.clone(), Value::String(value.clone())))
                .collect();
            out.insert(KEY_SYSTEM.to_string(), Value::Object(system));
        }
        if let Some(moved_from) = &self.moved_from {
            out.insert(
                KEY_MOVED_FROM.to_string(),
                Value::String(moved_from.clone()),
            );
        }
        Value::Object(out)
    }

    /// The inverse, and it cannot fail. A payload that is not this shape is a
    /// native producer's: its own JSON text becomes the body and it has no
    /// attributes.
    pub fn decode(payload: &Value) -> Envelope {
        match as_envelope(payload) {
            Some(envelope) => envelope,
            None => Envelope::of(native_body(payload)),
        }
    }

    /// Whether `payload` is this facade's shape rather than a native producer's.
    /// Exposed because the delete-set path has to answer it without building an
    /// envelope.
    pub fn is_envelope(payload: &Value) -> bool {
        as_envelope(payload).is_some()
    }
}

/// The body a payload this facade did not write is served as.
///
/// A JSON string IS already a body and is served as its own contents: a native
/// producer that pushed `"hello"` means the five characters, and re-quoting them
/// would hand every SQS consumer a body with quotes and escapes in it that no
/// sender ever wrote. Everything else — an object, an array, a number, `null` —
/// has no reading other than its own JSON text, which is complete and
/// byte-exact.
fn native_body(payload: &Value) -> String {
    match payload {
        Value::String(text) => text.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// The recognition test and the decode in one pass: `None` means "not an
/// envelope", and the caller then serves the payload as itself.
fn as_envelope(payload: &Value) -> Option<Envelope> {
    let obj = payload.as_object()?;
    // A subset of the four keys, and the mandatory one. An object carrying
    // anything else was not written by this facade.
    if !obj.keys().all(|k| {
        matches!(
            k.as_str(),
            KEY_BODY | KEY_ATTRIBUTES | KEY_SYSTEM | KEY_MOVED_FROM
        )
    }) {
        return None;
    }
    let body = obj.get(KEY_BODY)?.as_str()?.to_string();

    let mut attributes = BTreeMap::new();
    if let Some(a) = obj.get(KEY_ATTRIBUTES) {
        for (name, entry) in a.as_object()? {
            let entry = entry.as_object()?;
            if !entry
                .keys()
                .all(|k| matches!(k.as_str(), KEY_TYPE | KEY_VALUE))
            {
                return None;
            }
            let data_type = entry.get(KEY_TYPE)?.as_str()?.to_string();
            let raw = entry.get(KEY_VALUE)?.as_str()?;
            let value = if is_binary_type(&data_type) {
                // Undecodable base64 under a Binary label is not something this
                // facade can have written, so the payload is not ours.
                AttributeValue::Binary(B64.decode(raw).ok()?)
            } else {
                AttributeValue::String(raw.to_string())
            };
            attributes.insert(name.clone(), MessageAttribute { data_type, value });
        }
    }

    let mut system = BTreeMap::new();
    if let Some(s) = obj.get(KEY_SYSTEM) {
        for (name, value) in s.as_object()? {
            system.insert(name.clone(), value.as_str()?.to_string());
        }
    }

    let moved_from = match obj.get(KEY_MOVED_FROM) {
        Some(m) => Some(m.as_str()?.to_string()),
        None => None,
    };

    Some(Envelope {
        body,
        attributes,
        system,
        moved_from,
    })
}

/// The SQS body charset, which is the XML 1.0 `Char` production:
///
/// ```text
/// #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
/// ```
///
/// The surrogate range is absent from the list and needs no test: a Rust `char`
/// cannot hold one, so a `&str` has already excluded them.
pub fn is_allowed_char(c: char) -> bool {
    matches!(c, '\u{9}' | '\u{A}' | '\u{D}')
        || matches!(c, '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..='\u{10FFFF}')
}

/// Refuse a body SQS would not carry.
///
/// Applies to attribute String values too — AWS restricts them to the same
/// production — which is why the check is a function over text rather than a
/// step inside `encode`.
///
/// An EMPTY body passes here. "A message must have a body" is a different rule,
/// belonging to the action that has the request in front of it, and answering it
/// from this function would report an empty send as a charset violation.
pub fn validate_body(body: &str) -> SqsResult<()> {
    match body.chars().find(|c| !is_allowed_char(*c)) {
        None => Ok(()),
        // The offending code point and nothing around it: a body is a
        // customer's payload, and an error message that quoted its context
        // would put that payload in this facade's logs.
        Some(c) => Err(invalid_contents(c)),
    }
}

/// [`SqsError`] is built field-wise rather than through `SqsError::with` so this
/// module — and the tests that pin it — stand on their own while the error
/// catalog's constructors are still being written. The value is the one those
/// constructors build.
fn invalid_contents(c: char) -> SqsError {
    SqsError {
        kind: ErrorKind::InvalidMessageContents,
        message: format!(
            "The message body contains U+{:04X}, which is outside the allowed character set.",
            c as u32
        ),
        retry_after_ms: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A round trip through actual JSON TEXT, not just the `Value`: the
    /// envelope is stored and re-parsed, and a shape that only survives in
    /// memory is not one.
    fn round_trip(envelope: &Envelope) -> Envelope {
        let payload = envelope.encode();
        let text = serde_json::to_string(&payload).unwrap();
        let reparsed: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(reparsed, payload, "the envelope is not JSON-stable");
        assert!(Envelope::is_envelope(&reparsed));
        Envelope::decode(&reparsed)
    }

    fn json(text: &str) -> Value {
        serde_json::from_str(text).unwrap()
    }

    #[test]
    fn a_body_alone_is_three_bytes_of_overhead() {
        let envelope = Envelope::of("hello");
        assert_eq!(
            serde_json::to_string(&envelope.encode()).unwrap(),
            r#"{"b":"hello"}"#
        );
        assert_eq!(round_trip(&envelope), envelope);
    }

    #[test]
    fn a_body_round_trips_verbatim() {
        // Whitespace, quotes, newlines and multi-byte text: a body is the text
        // the sender wrote, and JSON escaping is not allowed to be visible.
        let envelope = Envelope::of("  {\"nested\": \"json\"}\n\tlast line — é 🐤  ");
        assert_eq!(round_trip(&envelope), envelope);
    }

    #[test]
    fn attributes_of_every_kind_round_trip() {
        let mut envelope = Envelope::of("hello");
        envelope
            .attributes
            .insert("str".into(), MessageAttribute::string("String", "value"));
        envelope
            .attributes
            .insert("num".into(), MessageAttribute::string("Number", "42"));
        envelope.attributes.insert(
            "custom".into(),
            MessageAttribute::string("String.foo", "bar"),
        );
        envelope.attributes.insert(
            "bin".into(),
            MessageAttribute::binary("Binary", [0, 1, 2, 255]),
        );
        envelope.attributes.insert(
            "png".into(),
            MessageAttribute::binary("Binary.png", b"\x89PNG".to_vec()),
        );
        assert_eq!(round_trip(&envelope), envelope);
    }

    /// The stored spelling of a binary value is the wire's own base64, so the
    /// bytes never change representation between client and store.
    #[test]
    fn a_binary_value_is_stored_as_the_wires_base64() {
        let mut envelope = Envelope::of("x");
        envelope.attributes.insert(
            "bin".into(),
            MessageAttribute::binary("Binary", [0, 1, 2, 255]),
        );
        assert_eq!(
            envelope.encode(),
            json(r#"{"b":"x","a":{"bin":{"t":"Binary","v":"AAEC/w=="}}}"#)
        );
    }

    #[test]
    fn system_attributes_and_the_moved_marker_round_trip() {
        let mut envelope = Envelope::of("hello");
        envelope.system.insert(
            "AWSTraceHeader".into(),
            "Root=1-5759e988-bd862e3fe1be46a994272793".into(),
        );
        envelope.moved_from = Some("6a1b0e1c-0a1f-4a2b-9c3d-4e5f60718293".into());
        assert_eq!(round_trip(&envelope), envelope);
        assert_eq!(
            envelope.encode(),
            json(
                r#"{"b":"hello",
                    "s":{"AWSTraceHeader":"Root=1-5759e988-bd862e3fe1be46a994272793"},
                    "m":"6a1b0e1c-0a1f-4a2b-9c3d-4e5f60718293"}"#
            )
        );
    }

    /// The empty maps are absent, not present-and-empty: `{"a":{}}` is three
    /// bytes on every message that has no attributes, and it is also a shape
    /// this facade never writes.
    #[test]
    fn empty_maps_are_omitted() {
        let encoded = Envelope::of("hello").encode();
        let obj = encoded.as_object().unwrap();
        assert_eq!(obj.len(), 1);
        assert!(!obj.contains_key(KEY_ATTRIBUTES));
        assert!(!obj.contains_key(KEY_SYSTEM));
        assert!(!obj.contains_key(KEY_MOVED_FROM));
    }

    /// The recognition matrix. Left column: is this the facade's shape?
    #[test]
    fn the_recognition_matrix_holds() {
        let cases: &[(bool, &str)] = &[
            // Ours.
            (true, r#"{"b":"hello"}"#),
            (true, r#"{"b":""}"#),
            (true, r#"{"b":"hello","a":{"x":{"t":"String","v":"1"}}}"#),
            (true, r#"{"b":"hello","a":{"x":{"t":"Binary","v":"AAEC"}}}"#),
            (true, r#"{"b":"hello","s":{"AWSTraceHeader":"Root=1-2"}}"#),
            (true, r#"{"b":"hello","m":"an-id"}"#),
            (true, r#"{"b":"hello","a":{},"s":{},"m":"an-id"}"#),
            // Not ours, one reason each.
            (false, r#"{"b":123}"#),  // `b` is not a string
            (false, r#"{"b":null}"#), // nor is null
            (false, r#"{"a":{"x":{"t":"String","v":"1"}}}"#), // no `b` at all
            (false, r#"{"b":"hello","x":1}"#), // a key this facade never writes
            (false, r#"{"b":"hello","a":[]}"#), // `a` is not an object
            (false, r#"{"b":"hello","a":{"x":"1"}}"#), // an entry that is not {t,v}
            (false, r#"{"b":"hello","a":{"x":{"t":"String"}}}"#), // half an entry
            (false, r#"{"b":"hello","a":{"x":{"t":"String","v":1}}}"#), // a numeric value
            (
                false,
                r#"{"b":"hello","a":{"x":{"t":"String","v":"1","z":2}}}"#,
            ), // an extra key
            (
                false,
                r#"{"b":"hello","a":{"x":{"t":"Binary","v":"not base64!!"}}}"#,
            ),
            (false, r#"{"b":"hello","s":{"AWSTraceHeader":7}}"#), // a non-string system value
            (false, r#"{"b":"hello","m":7}"#),                    // a numeric moved-from
            (false, r#"{"order":42,"total":10}"#),                // a native producer's document
            (false, r#"[{"b":"hello"}]"#),                        // not even an object
            (false, r#""just a string""#),
            (false, "42"),
            (false, "null"),
        ];
        for (ours, text) in cases {
            assert_eq!(
                Envelope::is_envelope(&json(text)),
                *ours,
                "recognition disagrees about {text}"
            );
        }
    }

    /// The native path: a document this facade did not write is served as its
    /// own text, with no attributes, so a Queen producer's messages are readable
    /// through an SQS consumer.
    #[test]
    fn a_native_document_is_served_as_its_own_text() {
        let decoded = Envelope::decode(&json(r#"{"order":42,"total":10}"#));
        assert_eq!(decoded.body, r#"{"order":42,"total":10}"#);
        assert!(decoded.attributes.is_empty());
        assert!(decoded.system.is_empty());
        assert_eq!(decoded.moved_from, None);
    }

    /// A JSON string payload is ALREADY a body: it is served unquoted, because
    /// a body with quotes and escapes in it is one no sender wrote.
    #[test]
    fn a_native_string_payload_is_served_unquoted() {
        assert_eq!(Envelope::decode(&json(r#""hello""#)).body, "hello");
        assert_eq!(
            Envelope::decode(&json(r#""with \"quotes\"""#)).body,
            r#"with "quotes""#
        );
    }

    #[test]
    fn other_native_payloads_are_served_as_their_json_text() {
        assert_eq!(Envelope::decode(&json("42")).body, "42");
        assert_eq!(Envelope::decode(&json("null")).body, "null");
        assert_eq!(Envelope::decode(&json("[1,2]")).body, "[1,2]");
    }

    /// A near-miss falls out to the native path WHOLE rather than being
    /// half-read: the body is the document's text, attributes included, so
    /// nothing is silently dropped.
    #[test]
    fn a_near_miss_falls_back_whole() {
        let payload = json(r#"{"b":"hello","x":1}"#);
        let decoded = Envelope::decode(&payload);
        assert_eq!(decoded.body, r#"{"b":"hello","x":1}"#);
        assert!(decoded.attributes.is_empty());
    }

    #[test]
    fn the_charset_accepts_what_sqs_carries() {
        for body in [
            "",
            "plain text",
            "tabs\tand\nnewlines\r\n",
            "unicode: é 中文 🐤",
            "\u{D7FF}\u{E000}\u{FFFD}\u{10FFFF}",
        ] {
            assert!(validate_body(body).is_ok(), "refused {body:?}");
        }
    }

    #[test]
    fn the_charset_refuses_what_xml_cannot_carry() {
        for body in [
            "a\u{0}b",   // NUL
            "\u{1}",     // SOH
            "bell\u{7}", //
            "\u{B}",     // vertical tab — NOT one of the three allowed controls
            "\u{C}",     // form feed, likewise
            "\u{1B}[0m", // an ANSI escape, the plausible accident
            "\u{FFFE}",  // a noncharacter above the allowed range
            "\u{FFFF}",
        ] {
            let err = validate_body(body).unwrap_err();
            assert_eq!(
                err.kind,
                ErrorKind::InvalidMessageContents,
                "accepted {body:?}"
            );
        }
    }

    /// The refusal names the code point and quotes nothing else: the body is a
    /// customer's payload and an error message is a log line.
    #[test]
    fn the_charset_refusal_names_the_code_point_only() {
        let err = validate_body("secret-token\u{0}").unwrap_err();
        assert!(err.message.contains("U+0000"), "{}", err.message);
        assert!(!err.message.contains("secret-token"), "{}", err.message);
    }

    #[test]
    fn binary_labels_are_recognized_by_prefix() {
        assert!(is_binary_type("Binary"));
        assert!(is_binary_type("Binary.png"));
        assert!(!is_binary_type("BinaryX"));
        assert!(!is_binary_type("String"));
        assert!(!is_binary_type("String.Binary"));
    }
}
