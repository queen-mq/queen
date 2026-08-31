//! The three MD5 fields, computed exactly as AWS documents them.
//!
//! CONTRACT. These are CORRECTNESS, not decoration: boto3 (and the JS, Java and
//! .NET SDKs) recompute them client-side after every send and every receive, and
//! raise on a mismatch. A facade that omitted them, or computed them over a
//! normalized form of the message, would fail on the SDK's own side with an
//! error that names the SDK and not this facade — which is the worst possible
//! failure mode to debug.
//!
//! `MD5OfMessageBody` is plain: the MD5 of the body's UTF-8 bytes, lowercase hex.
//!
//! `MD5OfMessageAttributes` has an ENCODING OF ITS OWN, and it is the one thing
//! in this file that cannot be guessed. Attributes are sorted by name, and for
//! each one the digest is fed, in order:
//!
//! ```text
//!   name length    (4 bytes, big-endian)   name bytes
//!   type length    (4 bytes, big-endian)   type bytes (the FULL label, custom suffix included)
//!   transport byte (1 = String or Number, 2 = Binary)
//!   value length   (4 bytes, big-endian)   value bytes (Binary: the DECODED bytes)
//! ```
//!
//! Two details that look like details and are not: `Number` uses the STRING
//! transport byte, and a custom label (`Number.float`) is hashed whole. Both are
//! places where a plausible-looking implementation produces a digest that
//! differs from every SDK's.
//!
//! `MD5OfMessageSystemAttributes` is the same algorithm over the system
//! attribute map, and is emitted only when that map is non-empty — an empty
//! digest field is not the same as an absent one to the SDKs that check it.
//!
//! MD5 is used here because AWS's protocol says MD5. It authenticates nothing;
//! it is a transport checksum, and the crate is RustCrypto's like every other
//! hash in this repository.
//!
//! ## Where the vectors come from
//!
//! The body vector below (`"This is a test message"` →
//! `fafb00f5732ab283681e124bf8747ed1`) is AWS's own published one, so the file is
//! anchored to a value nothing in this repository produced. The attribute
//! vectors are locked goldens, derived from a SECOND implementation of the
//! encoding above written straight from the specification — two implementations
//! agreeing is what a golden is worth here, because a digest that only agrees
//! with itself would pin a misreading of the spec as firmly as the spec.

use std::collections::BTreeMap;

use md5::{Digest, Md5};

use crate::envelope::{AttributeValue, MessageAttribute};

/// The transport byte for a `String`- or `Number`-typed value.
pub const TRANSPORT_STRING: u8 = 1;
/// The transport byte for a `Binary`-typed value.
pub const TRANSPORT_BINARY: u8 = 2;

/// The type label every system attribute carries. AWS defines exactly one
/// system attribute, `AWSTraceHeader`, and it is a `String`; the map is
/// therefore `name → value` with no room for a label, and the digest still needs
/// one.
const SYSTEM_TYPE: &str = "String";

/// `MD5OfMessageBody` — lowercase hex over the body's UTF-8 bytes.
pub fn body_md5(body: &str) -> String {
    hex::encode(Md5::digest(body.as_bytes()))
}

/// `MD5OfMessageAttributes`, or `None` when there are no attributes: the field
/// is ABSENT rather than the digest of nothing, which is what the SDKs expect.
pub fn attributes_md5(attributes: &BTreeMap<String, MessageAttribute>) -> Option<String> {
    if attributes.is_empty() {
        return None;
    }
    let mut digest = Md5::new();
    // The map's own iteration order IS the sort AWS specifies — byte order of
    // the UTF-8 name. Re-sorting here would be a second opinion on the order
    // that decides the digest; the type is the opinion (see
    // [`crate::envelope::Envelope::attributes`]).
    for (name, attribute) in attributes {
        let (transport, value) = match &attribute.value {
            AttributeValue::String(text) => (TRANSPORT_STRING, text.as_bytes()),
            AttributeValue::Binary(bytes) => (TRANSPORT_BINARY, bytes.as_slice()),
        };
        feed(
            &mut digest,
            name.as_bytes(),
            attribute.data_type.as_bytes(),
            transport,
            value,
        );
    }
    Some(hex::encode(digest.finalize()))
}

/// `MD5OfMessageSystemAttributes`. Same algorithm, same absence rule.
pub fn system_attributes_md5(system: &BTreeMap<String, String>) -> Option<String> {
    if system.is_empty() {
        return None;
    }
    let mut digest = Md5::new();
    for (name, value) in system {
        feed(
            &mut digest,
            name.as_bytes(),
            SYSTEM_TYPE.as_bytes(),
            TRANSPORT_STRING,
            value.as_bytes(),
        );
    }
    Some(hex::encode(digest.finalize()))
}

/// One attribute's contribution, in the documented order. The length prefixes
/// are what make the encoding unambiguous — without them a name/value pair could
/// be split two ways and two different attribute sets would digest alike.
fn feed(digest: &mut Md5, name: &[u8], data_type: &[u8], transport: u8, value: &[u8]) {
    length_prefixed(digest, name);
    length_prefixed(digest, data_type);
    digest.update([transport]);
    length_prefixed(digest, value);
}

fn length_prefixed(digest: &mut Md5, bytes: &[u8]) {
    // 4 bytes, big-endian, and a `u32` because that is the width AWS specifies:
    // a `usize` would be eight bytes on this target and produce a digest no
    // client agrees with.
    digest.update((bytes.len() as u32).to_be_bytes());
    digest.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(data_type: &str, value: &str) -> MessageAttribute {
        MessageAttribute::string(data_type, value)
    }

    fn attrs(pairs: &[(&str, MessageAttribute)]) -> BTreeMap<String, MessageAttribute> {
        pairs
            .iter()
            .map(|(name, a)| (name.to_string(), a.clone()))
            .collect()
    }

    /// AWS's own published vector. If this line ever fails, the failure is the
    /// hash crate or the encoding of the string, and nothing else in the file is
    /// worth reading until it passes.
    #[test]
    fn the_body_digest_is_the_documented_vector() {
        assert_eq!(
            body_md5("This is a test message"),
            "fafb00f5732ab283681e124bf8747ed1"
        );
    }

    #[test]
    fn the_body_digest_is_over_bytes_not_characters() {
        // An empty body still has a digest — the field is always present on a
        // send result, unlike the attribute one.
        assert_eq!(body_md5(""), "d41d8cd98f00b204e9800998ecf8427e");
        // Multi-byte UTF-8 digests as its bytes, which is what the SDK hashes.
        assert_eq!(body_md5("héllo"), "be50e8478cf24ff3595bc7307fb91b50");
    }

    /// The absence rule: no attributes means no field, not the digest of an
    /// empty input. An SDK that receives `d41d8c…` for a message with no
    /// attributes computes nothing to compare it against and raises.
    #[test]
    fn no_attributes_means_no_digest() {
        assert_eq!(attributes_md5(&BTreeMap::new()), None);
        assert_eq!(system_attributes_md5(&BTreeMap::new()), None);
    }

    #[test]
    fn one_string_attribute_matches_its_golden() {
        let a = attrs(&[("test-attribute", text("String", "test-value"))]);
        assert_eq!(
            attributes_md5(&a).unwrap(),
            "c38e447bda89281029d55c818cc8b9f9"
        );
    }

    /// `Number` shares the STRING transport byte — and still digests
    /// differently from the same value labelled `String`, because the label
    /// itself is hashed. Both halves of that sentence are load-bearing: an
    /// implementation that gave `Number` transport byte 2 would fail every
    /// client, and one that normalized the label away would fail them for a
    /// different reason.
    #[test]
    fn a_number_attribute_hashes_its_label_over_the_string_transport() {
        let number = attrs(&[("count", text("Number", "42"))]);
        assert_eq!(
            attributes_md5(&number).unwrap(),
            "2ee5fa915753ff72599b2514463a2897"
        );
        let string = attrs(&[("count", text("String", "42"))]);
        assert_ne!(attributes_md5(&number), attributes_md5(&string));
    }

    /// A binary attribute digests its DECODED bytes. The wire and the envelope
    /// both spell the value base64, so the tempting bug is to hash the base64
    /// text — which is a different digest, and one no client computes.
    #[test]
    fn a_binary_attribute_hashes_the_decoded_bytes() {
        let a = attrs(&[("blob", MessageAttribute::binary("Binary", [0, 1, 2, 255]))]);
        assert_eq!(
            attributes_md5(&a).unwrap(),
            "3b1b4028306ffa157a32d5916f8f714b"
        );
        // The same value as its base64 text is a different message entirely.
        let as_text = attrs(&[("blob", text("Binary", "AAEC/w=="))]);
        assert_ne!(attributes_md5(&as_text), attributes_md5(&a));
    }

    /// A custom label is hashed WHOLE, suffix included: `String.foo` is ten
    /// bytes of type, not six.
    #[test]
    fn a_custom_label_is_hashed_in_full() {
        let custom = attrs(&[("label", text("String.foo", "bar"))]);
        assert_eq!(
            attributes_md5(&custom).unwrap(),
            "58d3b219a649974d7b3c4c00ac2920a3"
        );
        let plain = attrs(&[("label", text("String", "bar"))]);
        assert_ne!(attributes_md5(&custom), attributes_md5(&plain));
    }

    /// The golden this file is really pinned by: one of each kind at once.
    #[test]
    fn the_four_kinds_together_match_their_golden() {
        let a = attrs(&[
            ("bin", MessageAttribute::binary("Binary", [0, 1, 2, 255])),
            ("custom", text("String.foo", "bar")),
            ("num", text("Number", "42")),
            ("str", text("String", "hello")),
        ]);
        assert_eq!(
            attributes_md5(&a).unwrap(),
            "59a923d8b436253750446d622c646886"
        );
    }

    /// Insertion order cannot reach the digest: the map sorts, and the sort is
    /// the specification's. A client sends attributes in whatever order its
    /// language's map iterated and validates against the sorted digest.
    #[test]
    fn insertion_order_does_not_reach_the_digest() {
        let forwards = attrs(&[
            ("bin", MessageAttribute::binary("Binary", [0, 1, 2, 255])),
            ("custom", text("String.foo", "bar")),
            ("num", text("Number", "42")),
            ("str", text("String", "hello")),
        ]);
        let backwards = attrs(&[
            ("str", text("String", "hello")),
            ("num", text("Number", "42")),
            ("custom", text("String.foo", "bar")),
            ("bin", MessageAttribute::binary("Binary", [0, 1, 2, 255])),
        ]);
        assert_eq!(attributes_md5(&forwards), attributes_md5(&backwards));
    }

    /// The system map has no labels of its own, so the digest supplies
    /// `String` — the same encoding, one field short.
    #[test]
    fn the_system_digest_supplies_the_string_label() {
        let system: BTreeMap<String, String> = [(
            "AWSTraceHeader".to_string(),
            "Root=1-5759e988-bd862e3fe1be46a994272793".to_string(),
        )]
        .into_iter()
        .collect();
        assert_eq!(
            system_attributes_md5(&system).unwrap(),
            "62a56dd927315f2b2e12832b84617ea5"
        );
        // And it is the attribute encoding, not a second one: the same pair as
        // a labelled String attribute digests identically.
        let same = attrs(&[(
            "AWSTraceHeader",
            text("String", "Root=1-5759e988-bd862e3fe1be46a994272793"),
        )]);
        assert_eq!(attributes_md5(&same), system_attributes_md5(&system));
    }

    /// The length prefixes are what keep two different attribute sets apart:
    /// without them `ab`/`c` and `a`/`bc` would feed the digest the same bytes.
    #[test]
    fn the_length_prefixes_separate_the_fields() {
        let one = attrs(&[("ab", text("String", "c"))]);
        let other = attrs(&[("a", text("String", "bc"))]);
        assert_ne!(attributes_md5(&one), attributes_md5(&other));
    }
}
