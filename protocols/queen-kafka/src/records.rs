//! The payload envelope: one Kafka record as one Queen payload, and back.
//!
//! Kafka records are arbitrary bytes — key, value and every header value are
//! byte strings with no encoding attached — and Queen payloads are JSON
//! (`queen.log_segments` frames carry a JSON document per message). Nothing
//! bridges those two for free, so the facade defines exactly one shape and both
//! directions live here: [`encode`] for Produce (M2) and [`decode`] for Fetch
//! (M3). One module, so the two can never drift.
//!
//! ```text
//! {"k": <base64 key or null>,
//!  "v": <base64 value or null>,
//!  "h": [{"k":"<header name>","v":<base64 or null>}, …]   // omitted when empty
//!  "t": <producer CreateTime, ms>}                         // omitted when -1
//! ```
//!
//! base64 rather than a JSON string because a Kafka value is not text: a
//! protobuf or Avro payload is not valid UTF-8, and JSON has no way to carry the
//! bytes that are not. `k` and `v` are always written, including as `null` — a
//! null key and a null value are both meaningful in Kafka (a null key means
//! "partition me round-robin", a null value is a compaction tombstone) and are
//! not the same thing as an empty one, which is why the null survives the trip
//! rather than becoming `""`.
//!
//! ## A payload that is not an envelope
//!
//! The other producer of a Queen queue is Queen itself, and its payloads are
//! whatever the application pushed. Those have to stay readable through the
//! facade — "Kafka producers in, native consumers out" has a mirror image, and
//! native-producer → Kafka-consumer is the interop half that costs nothing to
//! support. [`decode`] therefore recognises the envelope by shape and falls back
//! for everything else: no key, the payload's own JSON text as the value, and
//! the stored timestamp. A Kafka consumer of a native queue reads the documents
//! exactly as they are stored.
//!
//! Recognition is deliberately strict — an object whose keys are a subset of
//! `k`/`v`/`h`/`t`, carrying both `k` and `v`, with every field of the right
//! type and every base64 field decodable. The facade writes nothing else, so
//! anything that fails a check was written by someone else and is served as
//! itself. The one shape that could in principle be misread is a native payload
//! that happens to be `{"k":…,"v":…}` with base64-looking strings; it is the
//! price of a self-describing envelope with no reserved key, and the strict
//! subset rule is what keeps it to that single coincidence.
//!
//! ## What the envelope does not carry
//!
//! The record's OFFSET and its partition are not in it: those are Queen's, and
//! writing them into the payload would mean storing a number the broker assigns
//! after the payload is built. The producer's `producer_id`/`sequence` are not
//! in it either — the facade refuses idempotent producers rather than storing
//! sequences it does not enforce (see `handlers::produce`).

use base64::Engine;
use bytes::Bytes;
use kafka_protocol::records::{Record, NO_TIMESTAMP};
use serde_json::{Map, Value};

use crate::wire::Header;

/// The envelope's four keys. Named once: [`encode`], [`decode`] and the
/// subset check all read them.
const K_KEY: &str = "k";
const K_VALUE: &str = "v";
const K_HEADERS: &str = "h";
const K_TIMESTAMP: &str = "t";

/// Standard alphabet WITH padding — the same engine the broker and the proxy
/// use for every other base64 field in this repo (server/src/handlers/data.rs),
/// so a payload written here is decodable by every existing tool.
const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

/// A record's contents, recovered from a payload.
///
/// The offset, the partition and the batch flags are not here: this is the part
/// of a record that a payload can carry (see the module header).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Decoded {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    /// In order, and with repeats: Kafka's header list may carry one name
    /// twice, and the envelope's array is what preserves that where
    /// `kafka-protocol`'s `IndexMap` cannot (see [`crate::wire`]).
    pub headers: Vec<Header>,
    /// Milliseconds, or [`NO_TIMESTAMP`].
    pub timestamp: i64,
}

/// Encode one decoded Kafka record as the Queen payload it becomes.
///
/// `record.offset` and the batch flags are dropped on purpose (module header).
///
/// `headers` is the record's header list AS IT ARRIVED — in order, repeats
/// included — recovered from the batch's own bytes by [`crate::wire`]. It is
/// separate from the record because `Record.headers` is an `IndexMap` keyed by
/// name and cannot hold a repeat: a batch carrying `x` twice reaches this
/// function with one `x` in the map, and the array written here would silently
/// be one pair short. `None` falls back to that map, which is the honest answer
/// when the recovery pass could not line itself up with what the crate decoded
/// (see [`crate::wire::header_lists`]) — and for a record with distinct names,
/// which is nearly all of them, the two are the same list.
pub fn encode(record: &Record, headers: Option<&[Header]>) -> Value {
    let mut out = Map::with_capacity(4);
    out.insert(K_KEY.to_string(), b64_or_null(record.key.as_ref()));
    out.insert(K_VALUE.to_string(), b64_or_null(record.value.as_ref()));
    let from_the_map: Vec<Header>;
    let headers = match headers {
        Some(headers) => headers,
        None => {
            from_the_map = record
                .headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.clone()))
                .collect();
            &from_the_map
        }
    };
    if !headers.is_empty() {
        let headers = headers
            .iter()
            .map(|(name, value)| {
                let mut h = Map::with_capacity(2);
                h.insert(K_KEY.to_string(), Value::String(name.clone()));
                h.insert(K_VALUE.to_string(), b64_or_null(value.as_ref()));
                Value::Object(h)
            })
            .collect();
        out.insert(K_HEADERS.to_string(), Value::Array(headers));
    }
    // -1 is Kafka's "this record has no timestamp", and an absent key says the
    // same thing in a byte or two rather than a signed integer a reader has to
    // know to special-case.
    if record.timestamp != NO_TIMESTAMP {
        out.insert(K_TIMESTAMP.to_string(), Value::from(record.timestamp));
    }
    Value::Object(out)
}

/// Decode a stored payload back into a record's contents.
///
/// `stored_timestamp` is the timestamp the log itself has for this record — the
/// `ts` field of a `POST /api/v1/fetch` record. It is the answer when the
/// payload carries no producer timestamp of its own, whether because the
/// envelope omitted `t` or because the payload is not an envelope at all. With
/// neither, the result is [`NO_TIMESTAMP`], which is what Kafka calls "unknown"
/// and every client already renders.
pub fn decode(payload: &Value, stored_timestamp: Option<i64>) -> Decoded {
    match as_envelope(payload) {
        Some(mut decoded) => {
            if decoded.timestamp == NO_TIMESTAMP {
                decoded.timestamp = stored_timestamp.unwrap_or(NO_TIMESTAMP);
            }
            decoded
        }
        // Not ours: a native Queen producer wrote it. The value is the payload's
        // own JSON text, which is the only representation of it that is both
        // complete and byte-exact.
        None => Decoded {
            key: None,
            value: Some(Bytes::from(serde_json::to_vec(payload).unwrap_or_default())),
            headers: Vec::new(),
            timestamp: stored_timestamp.unwrap_or(NO_TIMESTAMP),
        },
    }
}

/// The envelope test and the decode in one pass: `None` means "not an
/// envelope", and every caller then serves the payload as itself.
fn as_envelope(payload: &Value) -> Option<Decoded> {
    let obj = payload.as_object()?;
    // A subset of our four keys, and both of the mandatory ones. An object
    // carrying anything else was not written by this facade.
    if !obj
        .keys()
        .all(|k| matches!(k.as_str(), K_KEY | K_VALUE | K_HEADERS | K_TIMESTAMP))
    {
        return None;
    }
    let key = from_b64_or_null(obj.get(K_KEY)?)?;
    let value = from_b64_or_null(obj.get(K_VALUE)?)?;

    let mut headers = Vec::new();
    if let Some(h) = obj.get(K_HEADERS) {
        for entry in h.as_array()? {
            let entry = entry.as_object()?;
            if !entry.keys().all(|k| matches!(k.as_str(), K_KEY | K_VALUE)) {
                return None;
            }
            let name = entry.get(K_KEY)?.as_str()?.to_string();
            headers.push((name, from_b64_or_null(entry.get(K_VALUE)?)?));
        }
    }

    let timestamp = match obj.get(K_TIMESTAMP) {
        Some(t) => t.as_i64()?,
        None => NO_TIMESTAMP,
    };

    Some(Decoded {
        key,
        value,
        headers,
        timestamp,
    })
}

fn b64_or_null(bytes: Option<&Bytes>) -> Value {
    match bytes {
        Some(b) => Value::String(B64.encode(b)),
        None => Value::Null,
    }
}

/// `null` → `Some(None)`, a decodable base64 string → `Some(Some(bytes))`,
/// anything else → `None`, which fails the whole envelope test.
fn from_b64_or_null(v: &Value) -> Option<Option<Bytes>> {
    match v {
        Value::Null => Some(None),
        Value::String(s) => Some(Some(Bytes::from(B64.decode(s).ok()?))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::protocol::StrBytes;
    use kafka_protocol::records::TimestampType;

    /// A record the way `RecordBatchDecoder` hands one over.
    fn record(
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
        timestamp: i64,
    ) -> Record {
        let mut map: IndexMap<StrBytes, Option<Bytes>> = IndexMap::new();
        for (name, v) in headers {
            map.insert(
                StrBytes::from_string(name.to_string()),
                v.map(Bytes::copy_from_slice),
            );
        }
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp,
            key: key.map(Bytes::copy_from_slice),
            value: value.map(Bytes::copy_from_slice),
            headers: map,
        }
    }

    /// What a lossless round trip means here: everything the envelope claims to
    /// carry comes back identical.
    fn round_trip(r: &Record) -> Decoded {
        let payload = encode(r, None);
        // Through actual JSON text, not just the `Value`: the envelope is stored
        // and re-parsed, and a shape that only survives in memory is not one.
        let text = serde_json::to_string(&payload).unwrap();
        let reparsed: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(reparsed, payload, "the envelope is not JSON-stable");
        decode(&reparsed, None)
    }

    fn expect(r: &Record) -> Decoded {
        Decoded {
            key: r.key.clone(),
            value: r.value.clone(),
            headers: r
                .headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            timestamp: r.timestamp,
        }
    }

    #[test]
    fn an_ordinary_record_round_trips() {
        let r = record(
            Some(b"user-42"),
            Some(br#"{"amount":10}"#),
            &[("trace-id", Some(b"abc".as_slice()))],
            1_756_000_000_000,
        );
        assert_eq!(round_trip(&r), expect(&r));
    }

    /// A null key is not an empty key: it is what tells Kafka to partition the
    /// record round-robin, and the two must not collapse into one another.
    #[test]
    fn a_null_key_and_an_empty_key_stay_different() {
        let null_key = record(None, Some(b"v"), &[], 1);
        let empty_key = record(Some(b""), Some(b"v"), &[], 1);

        assert_eq!(round_trip(&null_key), expect(&null_key));
        assert_eq!(round_trip(&empty_key), expect(&empty_key));
        assert_eq!(encode(&null_key, None)["k"], Value::Null);
        assert_eq!(encode(&empty_key, None)["k"], Value::String(String::new()));
        assert_ne!(round_trip(&null_key), round_trip(&empty_key));
    }

    /// ...and a null VALUE is a compaction tombstone, which is the one record
    /// whose whole meaning is that it has no value.
    #[test]
    fn a_null_value_and_an_empty_value_stay_different() {
        let tombstone = record(Some(b"k"), None, &[], 1);
        let empty = record(Some(b"k"), Some(b""), &[], 1);

        assert_eq!(round_trip(&tombstone), expect(&tombstone));
        assert_eq!(round_trip(&empty), expect(&empty));
        assert_eq!(encode(&tombstone, None)["v"], Value::Null);
        assert_ne!(round_trip(&tombstone), round_trip(&empty));
    }

    #[test]
    fn a_record_with_neither_key_nor_value_round_trips() {
        let r = record(None, None, &[], NO_TIMESTAMP);
        assert_eq!(round_trip(&r), expect(&r));
    }

    /// No headers means the key is absent, and an absent `h` decodes to no
    /// headers — the two ends of the same saving.
    #[test]
    fn empty_headers_are_absent_and_absent_headers_are_empty() {
        let r = record(Some(b"k"), Some(b"v"), &[], 1);
        let payload = encode(&r, None);
        assert!(payload.get("h").is_none(), "an empty array was written");
        assert!(decode(&payload, None).headers.is_empty());
    }

    /// A header with a null value is a real thing on the wire (the value is a
    /// nullable byte string) and is not the same as a header with an empty one.
    #[test]
    fn a_header_with_a_null_value_survives() {
        let r = record(
            Some(b"k"),
            Some(b"v"),
            &[
                ("null-header", None),
                ("empty-header", Some(b"".as_slice())),
            ],
            1,
        );
        let got = round_trip(&r);
        assert_eq!(got, expect(&r));
        assert_eq!(
            got.headers,
            vec![
                ("null-header".to_string(), None),
                ("empty-header".to_string(), Some(Bytes::from_static(b""))),
            ]
        );
    }

    /// The envelope's header array is a LIST, so a name that arrives twice is
    /// stored twice, in the order it arrived. It is the half of the fix that
    /// lives here: [`crate::wire`] recovers the repeat from the batch, and this
    /// is what carries it into Queen — a pair dropped at this step is a value
    /// that never reaches the log and can never be read back.
    #[test]
    fn a_repeated_header_name_is_stored_as_two_pairs() {
        // The record as `kafka-protocol` hands it over: its map has ALREADY
        // collapsed the two `x`es into one. The ordered list beside it is what
        // the producer actually sent.
        let r = record(
            None,
            Some(b"v"),
            &[
                ("x", Some(b"2".as_slice())),
                ("y", Some(b"solo".as_slice())),
            ],
            1,
        );
        let sent: Vec<Header> = vec![
            ("x".to_string(), Some(Bytes::from_static(b"1"))),
            ("y".to_string(), Some(Bytes::from_static(b"solo"))),
            ("x".to_string(), Some(Bytes::from_static(b"2"))),
        ];

        let payload = encode(&r, Some(&sent));
        assert_eq!(
            payload["h"],
            serde_json::json!([
                {"k": "x", "v": B64.encode("1")},
                {"k": "y", "v": B64.encode("solo")},
                {"k": "x", "v": B64.encode("2")},
            ]),
            "the envelope collapsed a repeated name"
        );
        assert_eq!(decode(&payload, None).headers, sent);

        // ...and without the list, the map is all there is: one `x`, the last
        // value. That is the shape this argument exists to replace.
        assert_eq!(decode(&encode(&r, None), None).headers.len(), 2);
    }

    #[test]
    fn header_order_is_preserved() {
        let r = record(
            None,
            Some(b"v"),
            &[
                ("z", Some(b"1".as_slice())),
                ("a", Some(b"2".as_slice())),
                ("m", Some(b"3".as_slice())),
            ],
            1,
        );
        let names: Vec<String> = round_trip(&r).headers.into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, ["z", "a", "m"]);
    }

    /// The reason the envelope is base64 and not JSON strings: none of these
    /// bytes are valid UTF-8, and every one of them is an ordinary payload
    /// (protobuf, Avro, a compressed blob, a raw uuid key).
    #[test]
    fn non_utf8_bytes_survive_everywhere() {
        let nasty: Vec<u8> = vec![0x00, 0xff, 0xfe, 0x80, 0x01, 0x7f, 0xc3, 0x28];
        let r = record(
            Some(&nasty),
            Some(&nasty),
            &[("bin", Some(nasty.as_slice()))],
            1,
        );
        let got = round_trip(&r);
        assert_eq!(got, expect(&r));
        assert_eq!(got.key.unwrap(), Bytes::from(nasty.clone()));
        assert_eq!(got.headers[0].1.as_ref().unwrap(), &Bytes::from(nasty));
    }

    /// A record with no timestamp writes no `t`, and reads back as "unknown".
    #[test]
    fn a_missing_timestamp_is_absent_not_minus_one() {
        let r = record(Some(b"k"), Some(b"v"), &[], NO_TIMESTAMP);
        let payload = encode(&r, None);
        assert!(payload.get("t").is_none(), "-1 was written out");
        assert_eq!(decode(&payload, None).timestamp, NO_TIMESTAMP);
    }

    /// ...and when the log knows when the record landed, that is the answer
    /// instead. Same rule for an envelope without `t` and for a payload that is
    /// not an envelope at all.
    #[test]
    fn the_stored_timestamp_fills_in_for_a_missing_one() {
        let payload = encode(&record(Some(b"k"), Some(b"v"), &[], NO_TIMESTAMP), None);
        assert_eq!(
            decode(&payload, Some(1_756_000_000_001)).timestamp,
            1_756_000_000_001
        );
        // A producer timestamp is never overwritten by the log's.
        let with_t = encode(&record(Some(b"k"), Some(b"v"), &[], 42), None);
        assert_eq!(decode(&with_t, Some(1_756_000_000_001)).timestamp, 42);
    }

    #[test]
    fn a_zero_timestamp_is_a_timestamp() {
        let r = record(Some(b"k"), Some(b"v"), &[], 0);
        assert_eq!(encode(&r, None)["t"], Value::from(0));
        assert_eq!(decode(&encode(&r, None), Some(99)).timestamp, 0);
    }

    // ------------------------------------------------- the native-producer path

    /// The interop half: a payload a Queen application pushed, read by a Kafka
    /// consumer. It is served as its own JSON text, with no key.
    #[test]
    fn a_native_payload_is_served_as_its_own_json() {
        let payload: Value = serde_json::from_str(r#"{"orderId":7,"total":10.5}"#).unwrap();
        let got = decode(&payload, Some(1_756_000_000_002));
        assert_eq!(got.key, None);
        assert!(got.headers.is_empty());
        assert_eq!(got.timestamp, 1_756_000_000_002);
        // Byte-exact JSON, which is what a consumer of a native queue expects to
        // find in the value.
        let text = String::from_utf8(got.value.unwrap().to_vec()).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap(),
            payload,
            "the value is not the payload"
        );
    }

    /// Every JSON shape a native payload can have, not only objects.
    #[test]
    fn every_native_payload_shape_is_servable() {
        for raw in [
            "null",
            "true",
            "42",
            "-1.5",
            r#""a string""#,
            "[]",
            r#"[1,2,3]"#,
            "{}",
            r#"{"nested":{"a":[1,{"b":null}]}}"#,
            // Objects that share SOME of the envelope's keys but not its shape.
            r#"{"k":"orders","v":1,"extra":true}"#,
            r#"{"k":"only-k"}"#,
            r#"{"v":"only-v"}"#,
            r#"{"k":null,"v":null,"h":"not an array"}"#,
            r#"{"k":null,"v":null,"t":"not a number"}"#,
            r#"{"k":null,"v":null,"h":[{"k":1,"v":null}]}"#,
            r#"{"k":null,"v":null,"h":[{"k":"n","v":null,"junk":1}]}"#,
            // `v` is a string but not base64.
            r#"{"k":null,"v":"not base64!!"}"#,
        ] {
            let payload: Value = serde_json::from_str(raw).unwrap();
            let got = decode(&payload, Some(7));
            assert_eq!(got.key, None, "{raw} was read as an envelope");
            assert_eq!(got.timestamp, 7, "{raw}");
            assert_eq!(
                serde_json::from_slice::<Value>(&got.value.unwrap()).unwrap(),
                payload,
                "{raw} did not come back as itself"
            );
        }
    }

    /// The envelope test is by SHAPE, so the smallest legal envelope — both
    /// mandatory keys, both null — is still recognised as one and does not fall
    /// into the native path.
    #[test]
    fn the_smallest_envelope_is_still_an_envelope() {
        let payload: Value = serde_json::from_str(r#"{"k":null,"v":null}"#).unwrap();
        assert_eq!(
            decode(&payload, Some(5)),
            Decoded {
                key: None,
                value: None,
                headers: Vec::new(),
                timestamp: 5,
            }
        );
    }
}
