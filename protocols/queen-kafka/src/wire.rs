//! The RECORDS SECTION of a RecordBatch v2, in both directions — the one part
//! of the Kafka wire format this facade reads and writes byte by byte itself.
//!
//! Everything else is `kafka-protocol`'s: the frames, the request and response
//! schemas, the batch header, its CRC. This module exists for one field the
//! crate cannot represent.
//!
//! ## Repeated header names
//!
//! A Kafka record's headers are an ORDERED LIST of name/value pairs and the
//! format permits the same name twice — that is not a corner of the spec, it is
//! what the header list is FOR (KIP-82). W3C `tracestate`-style accumulation,
//! multi-value routing hints and per-hop provenance all write a name more than
//! once, and every Kafka client hands the list to the application as a list.
//!
//! `kafka-protocol` models `Record.headers` as an `IndexMap<StrBytes,
//! Option<Bytes>>`. It is ordered, and it is KEYED: a batch carrying `x=1` and
//! `x=2` is already one `x=2` by the time the crate's decoder returns, and a
//! record with two `x`es cannot be handed to its encoder at all. Before this
//! module the facade inherited both halves, which was the worst shape a defect
//! can have — the producer is answered error code 0, the consumer is handed a
//! well-formed record, and a value has disappeared between them with nothing on
//! either side to see. The loss was at DECODE, before the payload envelope, so
//! the value never reached Queen and no later read could recover it.
//!
//! So both directions of the header list are done here:
//!
//!   * [`header_lists`] walks the DECOMPRESSED records section a batch was
//!     decoded from and recovers each record's headers in order, duplicates
//!     included ([`crate::handlers::produce`]);
//!   * [`encode`] writes a batch whose records carry the ordered lists the
//!     envelope stored, which the crate's encoder could not have written
//!     ([`crate::handlers::fetch`]).
//!
//! ## Why this is not a second implementation of the format
//!
//! Neither direction re-implements a batch. The decode side READS bytes the
//! crate has already framed, CRC-checked and decompressed, and checks itself
//! against what the crate decoded from the same bytes (see [`header_lists`]):
//! disagreement is answered by using the crate's map, never by guessing. The
//! encode side goes through `encode_with_custom_compression`, the seam the crate
//! provides for replacing the records section, so the batch header, the length
//! and the CRC — the parts where a mistake is silent and breaks every consumer —
//! are still the crate's. What is written here is exactly the per-record body
//! Kafka defines, and [`tests::our_records_are_the_crates_records`] pins it
//! against the crate's own encoder byte for byte on every record the crate CAN
//! express.
//!
//! The same rule as `crate::decompress`, which re-implements the four codecs
//! against their own budget: nothing about the FORMAT is decided here.

use std::cell::Cell;
use std::io;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::records::{Compression, Record, RecordBatchEncoder, RecordEncodeOptions};

/// One record header: a name, and a value that may be null.
///
/// The same pair `crate::records::Decoded` carries, so a header list crosses
/// the envelope without changing shape.
pub type Header = (String, Option<Bytes>);

/// The record attributes byte. Kafka defines no per-record attribute bits (they
/// all live in the batch header), and every writer sends zero.
const NO_RECORD_ATTRIBUTES: u8 = 0;

/// The length a null byte string is written with — key, value or header value.
const NULL_LENGTH: i32 = -1;

// ------------------------------------------------------------------ decoding

/// The ordered header list of every record in one batch, read from the bytes
/// the crate decoded that batch from.
///
/// `section` is the DECOMPRESSED records section (the buffer
/// `crate::decompress` handed the decoder) and `records` is what the decoder
/// made of it, in order. The answer is one list per record, index-aligned with
/// `records`.
///
/// `None` means "this walk does not agree with the crate's, so it is not to be
/// trusted": the caller then keeps the crate's own (collapsed) headers, which
/// is exactly the behaviour that was there before this module. It is returned
/// for a section that does not parse, a section carrying fewer records than the
/// crate found, a header name that is not UTF-8 — and, the check that matters,
/// for any record whose list does not COLLAPSE to the map the crate decoded.
/// That last one is the alignment proof: the crate builds its map by inserting
/// each pair in order, so the same pairs in the same order must produce the same
/// map, and anything else means the two walks are not reading the same records.
pub fn header_lists(section: &[u8], records: &[Record]) -> Option<Vec<Vec<Header>>> {
    let mut cursor = Cursor::new(section);
    let mut out = Vec::with_capacity(records.len());
    for record in records {
        let list = cursor.record_headers()?;
        if !collapses_to(&list, record) {
            return None;
        }
        out.push(list);
    }
    Some(out)
}

/// Does `list`, inserted in order the way `kafka-protocol` inserts it, produce
/// the map the crate decoded for this record? See [`header_lists`].
///
/// Through a map and not a scan: a record's header count is a client's number,
/// bounded only by the frame, so a list-of-lists lookup here is a quadratic one
/// produce request can buy.
fn collapses_to(list: &[Header], record: &Record) -> bool {
    if list.len() < record.headers.len() {
        return false;
    }
    let mut at: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    let mut collapsed: Vec<(&str, &Option<Bytes>)> = Vec::new();
    for (name, value) in list {
        // An `IndexMap` insert keeps the first POSITION and takes the last
        // VALUE, which is precisely the loss this module exists to undo.
        match at.get(name.as_str()) {
            Some(i) => collapsed[*i].1 = value,
            None => {
                at.insert(name.as_str(), collapsed.len());
                collapsed.push((name.as_str(), value));
            }
        }
    }
    collapsed.len() == record.headers.len()
        && collapsed.iter().zip(record.headers.iter()).all(
            |((name, value), (theirs, their_value))| {
                *name == theirs.as_str() && *value == their_value
            },
        )
}

/// A byte-by-byte reader over one records section. Every method answers `None`
/// rather than panicking: the bytes came off a client's connection.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Cursor<'a> {
        Cursor { bytes, at: 0 }
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(n)?;
        let slice = self.bytes.get(self.at..end)?;
        self.at = end;
        Some(slice)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }

    /// LEB128, at most five bytes — `UnsignedVarInt` in the crate, down to the
    /// truncation on the fifth byte.
    fn unsigned_varint(&mut self) -> Option<u32> {
        let mut value = 0u32;
        for i in 0..5 {
            let b = u32::from(self.u8()?);
            value |= (b & 0x7f) << (i * 7);
            if b < 0x80 {
                break;
            }
        }
        Some(value)
    }

    fn unsigned_varlong(&mut self) -> Option<u64> {
        let mut value = 0u64;
        for i in 0..10 {
            let b = u64::from(self.u8()?);
            value |= (b & 0x7f) << (i * 7);
            if b < 0x80 {
                break;
            }
        }
        Some(value)
    }

    fn varint(&mut self) -> Option<i32> {
        self.unsigned_varint().map(unzigzag_32)
    }

    fn varlong(&mut self) -> Option<i64> {
        self.unsigned_varlong().map(unzigzag_64)
    }

    /// A length-prefixed byte string that may be null (`-1`).
    fn nullable_bytes(&mut self) -> Option<Option<&'a [u8]>> {
        let len = self.varint()?;
        match len {
            NULL_LENGTH => Some(None),
            len if len < 0 => None,
            len => self.take(len as usize).map(Some),
        }
    }

    /// One record, read for its headers alone.
    ///
    /// The whole body is walked because a header list can only be reached
    /// through it, and the body is taken as a slice of its own declared length
    /// first — the same `try_get_bytes` the crate's decoder does, so a record
    /// with trailing bytes inside its length is read the same way here.
    fn record_headers(&mut self) -> Option<Vec<Header>> {
        let size = self.varint()?;
        if size < 0 {
            return None;
        }
        let mut body = Cursor::new(self.take(size as usize)?);
        let _attributes = body.u8()?;
        let _timestamp_delta = body.varlong()?;
        let _offset_delta = body.varint()?;
        let _key = body.nullable_bytes()?;
        let _value = body.nullable_bytes()?;

        let count = body.varint()?;
        if count < 0 {
            return None;
        }
        // NOT `with_capacity(count)`: the count is a client's number and the
        // records it describes are already bounded by the section's length, so
        // reserving for it is the same unbounded allocation `decompress::Budget`
        // exists to refuse.
        let mut headers = Vec::new();
        for _ in 0..count {
            let name_len = body.varint()?;
            if name_len < 0 {
                return None;
            }
            let name = std::str::from_utf8(body.take(name_len as usize)?).ok()?;
            let value = body.nullable_bytes()?;
            headers.push((name.to_string(), value.map(Bytes::copy_from_slice)));
        }
        Some(headers)
    }
}

fn unzigzag_32(v: u32) -> i32 {
    ((v >> 1) as i32) ^ -((v & 1) as i32)
}

fn unzigzag_64(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

// ------------------------------------------------------------------ encoding

/// Encode one uncompressed RecordBatch v2 whose records carry `headers[i]`.
///
/// `records[i]` is read for everything a record and its batch are made of — the
/// offsets, the timestamps, the producer fields, the key and the value — EXCEPT
/// its `headers` map, which cannot express what this function is for. The
/// ordered list in `headers[i]` is written instead, duplicate names and all.
///
/// The batch header, its length and its CRC are the crate's: the records are
/// handed to `encode_with_custom_compression`, whose compressor seam is called
/// with the records section the crate built and the buffer to put one in, and
/// this writes its own there. What the crate builds for that call is thrown
/// away, so what it is handed is a SHAPE of each record — the fields the batch
/// header is computed from, with no key, no value and no headers — and the real
/// bodies are written once, here.
///
/// One batch, always: the crate starts a new one when a record's producer
/// fields or its `offset - sequence` differ from the first's, and the caller
/// ([`crate::handlers::fetch`]) builds records that never do. If one ever
/// arrived, the seam would be called a second time and this returns an error
/// rather than writing the same records under a second header — a wrong batch
/// on the wire is a consumer reading records that are not there.
pub fn encode(records: &[Record], headers: &[Vec<Header>]) -> Result<Bytes, String> {
    if records.len() != headers.len() {
        return Err(format!(
            "{} records with {} header lists: they are written together",
            records.len(),
            headers.len()
        ));
    }
    if records.is_empty() {
        return Ok(Bytes::new());
    }
    // The bases every delta in the section is written against. The crate
    // computes the same two numbers for the batch header — the minimum offset
    // and the minimum timestamp of the batch — and the section has to agree
    // with the header it sits under.
    let base_offset = records.iter().map(|r| r.offset).min().unwrap_or(0);
    let base_timestamp = records.iter().map(|r| r.timestamp).min().unwrap_or(0);

    let shapes: Vec<Record> = records.iter().map(shape).collect();
    let seams = Cell::new(0usize);
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode_with_custom_compression(
        &mut buf,
        shapes.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
        Some(
            |_shapes: &mut BytesMut, out: &mut BytesMut, _codec: Compression| {
                if seams.replace(seams.get() + 1) > 0 {
                    return Err(io::Error::other(
                        "these records do not encode as one batch; the records section cannot be \
                         written twice",
                    )
                    .into());
                }
                for (record, list) in records.iter().zip(headers) {
                    write_record(out, record, list, base_offset, base_timestamp)
                        .map_err(io::Error::other)?;
                }
                Ok(())
            },
        ),
    )
    .map_err(|e| format!("{e:#}"))?;
    Ok(buf.freeze())
}

/// One record with everything the BATCH HEADER is computed from and nothing
/// else. See [`encode`] for why the bodies the crate would build from these are
/// thrown away.
fn shape(record: &Record) -> Record {
    Record {
        key: None,
        value: None,
        headers: Default::default(),
        ..record.clone()
    }
}

/// One record body, exactly as Kafka defines it: a length, then the fields the
/// length covers.
fn write_record(
    out: &mut BytesMut,
    record: &Record,
    headers: &[Header],
    base_offset: i64,
    base_timestamp: i64,
) -> Result<(), String> {
    let offset_delta = record.offset - base_offset;
    let offset_delta = i32::try_from(offset_delta).map_err(|_| {
        format!(
            "record {} sits {offset_delta} offsets past the batch base, which no batch can \
             express",
            record.offset
        )
    })?;
    // Wrapping, and only reachable through a stored timestamp: the envelope's
    // `t` is whatever JSON number a payload carries, so two records of one read
    // can be i64::MIN and i64::MAX apart. The crate's encoder subtracts these
    // the same way (it panics on the overflow in a debug build); wrapping is
    // what the wire carries either way, and the delta is what a consumer adds
    // back to the batch's base.
    let timestamp_delta = record.timestamp.wrapping_sub(base_timestamp);

    let mut size = 1 // attributes
        + varlong_len(timestamp_delta)
        + varint_len(offset_delta)
        + nullable_len(record.key.as_deref())?
        + nullable_len(record.value.as_deref())?
        + varint_len(count(headers.len(), "headers")?);
    for (name, value) in headers {
        let name_len = count(name.len(), "a header name")?;
        size += varint_len(name_len) + name.len() + nullable_len(value.as_deref())?;
    }

    put_varint(out, count(size, "a record")?);
    out.put_u8(NO_RECORD_ATTRIBUTES);
    put_varlong(out, timestamp_delta);
    put_varint(out, offset_delta);
    put_nullable(out, record.key.as_deref());
    put_nullable(out, record.value.as_deref());
    put_varint(out, headers.len() as i32);
    for (name, value) in headers {
        put_varint(out, name.len() as i32);
        out.put_slice(name.as_bytes());
        put_nullable(out, value.as_deref());
    }
    Ok(())
}

/// A length the wire writes as an `i32`, or the reason it cannot be written.
/// Every one of these is unreachable through a frame — `conn::MAX_FRAME_BYTES`
/// is 100 MiB — and each is checked because the alternative is a negative
/// length on the wire.
fn count(n: usize, what: &str) -> Result<i32, String> {
    i32::try_from(n).map_err(|_| format!("{what} is {n} bytes, past what a record can express"))
}

/// The bytes a nullable byte string costs: its length, and itself.
fn nullable_len(value: Option<&[u8]>) -> Result<usize, String> {
    Ok(match value {
        None => varint_len(NULL_LENGTH),
        Some(v) => varint_len(count(v.len(), "a record field")?) + v.len(),
    })
}

fn put_nullable(out: &mut BytesMut, value: Option<&[u8]>) {
    match value {
        None => put_varint(out, NULL_LENGTH),
        Some(v) => {
            put_varint(out, v.len() as i32);
            out.put_slice(v);
        }
    }
}

fn zigzag_32(v: i32) -> u32 {
    ((v << 1) ^ (v >> 31)) as u32
}

fn zigzag_64(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn put_varint(out: &mut BytesMut, value: i32) {
    let mut v = zigzag_32(value);
    while v >= 0x80 {
        out.put_u8((v as u8) | 0x80);
        v >>= 7;
    }
    out.put_u8(v as u8);
}

fn put_varlong(out: &mut BytesMut, value: i64) {
    let mut v = zigzag_64(value);
    while v >= 0x80 {
        out.put_u8((v as u8) | 0x80);
        v >>= 7;
    }
    out.put_u8(v as u8);
}

fn varint_len(value: i32) -> usize {
    let mut v = zigzag_32(value);
    let mut len = 1;
    while v >= 0x80 {
        v >>= 7;
        len += 1;
    }
    len
}

fn varlong_len(value: i64) -> usize {
    let mut v = zigzag_64(value);
    let mut len = 1;
    while v >= 0x80 {
        v >>= 7;
        len += 1;
    }
    len
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::protocol::StrBytes;
    use kafka_protocol::records::{
        RecordBatchDecoder, TimestampType, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
    };

    /// A record as the facade builds one for a fetch response: one batch, no
    /// producer, offsets from the log.
    fn record(offset: i64, base: i64, key: Option<&[u8]>, value: &[u8], timestamp: i64) -> Record {
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: (-1i64 + (offset - base)) as i32,
            timestamp,
            key: key.map(Bytes::copy_from_slice),
            value: Some(Bytes::copy_from_slice(value)),
            headers: IndexMap::new(),
        }
    }

    fn with_headers(mut r: Record, headers: &[(&str, Option<&[u8]>)]) -> Record {
        for (name, value) in headers {
            r.headers.insert(
                StrBytes::from_string(name.to_string()),
                value.map(Bytes::copy_from_slice),
            );
        }
        r
    }

    fn list(headers: &[(&str, Option<&[u8]>)]) -> Vec<Header> {
        headers
            .iter()
            .map(|(name, value)| (name.to_string(), value.map(Bytes::copy_from_slice)))
            .collect()
    }

    /// The records section of an encoded batch: everything after the 61-byte
    /// batch header.
    fn section(batch: &Bytes) -> Vec<u8> {
        batch[61..].to_vec()
    }

    // ------------------------------------------------------------- encoding

    /// THE pin on the encode side: for every record the crate CAN express —
    /// which is every record with distinct header names — this module writes
    /// the bytes the crate's own encoder writes. Nothing about the format is
    /// decided here, and this is what says so.
    #[test]
    fn our_records_are_the_crates_records() {
        let cases: Vec<Vec<Record>> = vec![
            vec![with_headers(
                record(0, 0, Some(b"k"), b"v", 1_756_000_000_000),
                &[("trace-id", Some(b"abc".as_slice()))],
            )],
            // No key, no headers, a null value is not expressible here (the
            // fixture always sets one) so the empty one stands in for it.
            vec![record(0, 0, None, b"", 0)],
            // A run with gaps, several headers, a null header value, and
            // timestamps that go backwards inside the batch.
            vec![
                with_headers(
                    record(100, 100, Some(b"a"), b"one", 1_756_000_000_005),
                    &[
                        ("a", Some(b"1".as_slice())),
                        ("b", None),
                        ("empty", Some(b"".as_slice())),
                    ],
                ),
                record(104, 100, None, b"two", 1_756_000_000_001),
                with_headers(
                    record(105, 100, Some(&[0u8, 0xff, 0x80]), b"three", -1),
                    &[("bin", Some(&[0u8, 0xff][..]))],
                ),
            ],
            // Lengths either side of a varint boundary, and a header name long
            // enough to need two bytes for its own length.
            vec![with_headers(
                record(7, 7, Some(&[7u8; 200]), &[9u8; 5000], 1),
                &[(&"n".repeat(300), Some(&[1u8; 1000][..]))],
            )],
        ];

        for records in cases {
            let mut theirs = BytesMut::new();
            RecordBatchEncoder::encode(
                &mut theirs,
                records.iter(),
                &RecordEncodeOptions {
                    version: 2,
                    compression: Compression::None,
                },
            )
            .expect("the crate encodes the fixture");
            let lists: Vec<Vec<Header>> = records
                .iter()
                .map(|r| {
                    r.headers
                        .iter()
                        .map(|(name, value)| (name.to_string(), value.clone()))
                        .collect()
                })
                .collect();
            let ours = encode(&records, &lists).expect("we encode the fixture");
            assert_eq!(
                ours,
                theirs.freeze(),
                "our batch is not the crate's, for {} records",
                records.len()
            );
        }
    }

    /// ...and the thing the crate cannot do: two headers with one name, in
    /// order, both on the wire. Decoded by the crate's own decoder, so the CRC
    /// and the framing are checked by the code every Rust client runs — and
    /// read back by [`header_lists`], which is the only reader that can see
    /// both of them.
    #[test]
    fn a_repeated_header_name_survives_the_encoder() {
        let records = vec![record(0, 0, None, b"v", 1)];
        let lists = vec![list(&[
            ("x", Some(b"1".as_slice())),
            ("y", Some(b"solo".as_slice())),
            ("x", Some(b"2".as_slice())),
        ])];

        let batch = encode(&records, &lists).expect("a duplicate name encodes");
        let mut raw = batch.clone();
        let sets = RecordBatchDecoder::decode_all(&mut raw).expect("the client side decodes it");
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].records.len(), 1);
        // What the CRATE sees is the collapse this module exists to work
        // around: one `x`, the last value.
        assert_eq!(sets[0].records[0].headers.len(), 2);
        assert_eq!(
            sets[0].records[0]
                .headers
                .get(&StrBytes::from_static_str("x")),
            Some(&Some(Bytes::from_static(b"2")))
        );
        // What is actually on the wire is both of them, in order.
        let back = header_lists(&section(&batch), &sets[0].records).expect("the walk agrees");
        assert_eq!(back, lists);
    }

    #[test]
    fn a_header_list_longer_than_the_records_is_refused() {
        let records = vec![record(0, 0, None, b"v", 1)];
        assert!(encode(&records, &[]).is_err());
        assert!(encode(&[], &[Vec::new()]).is_err());
        assert_eq!(encode(&[], &[]), Ok(Bytes::new()));
    }

    /// A record too far past the batch base to encode is refused, not written
    /// with a wrapped delta.
    #[test]
    fn an_unencodable_offset_delta_is_an_error() {
        let far = i64::from(i32::MAX) + 2;
        let records = vec![record(0, 0, None, b"a", 1), record(far, 0, None, b"b", 1)];
        let lists = vec![Vec::new(), Vec::new()];
        assert!(encode(&records, &lists).is_err());
    }

    // ------------------------------------------------------------- decoding

    /// The round trip through both halves of this module, over every shape a
    /// header list can have.
    #[test]
    fn every_header_list_round_trips() {
        let cases: Vec<Vec<Header>> = vec![
            Vec::new(),
            list(&[("solo", Some(b"1".as_slice()))]),
            list(&[("null", None)]),
            list(&[("empty", Some(b"".as_slice()))]),
            // The defect, in every arrangement: adjacent, separated, three of
            // them, and one whose repeats carry a null and an empty value.
            list(&[("x", Some(b"1".as_slice())), ("x", Some(b"2".as_slice()))]),
            list(&[
                ("x", Some(b"1".as_slice())),
                ("y", Some(b"solo".as_slice())),
                ("x", Some(b"2".as_slice())),
            ]),
            list(&[
                ("x", Some(b"1".as_slice())),
                ("x", Some(b"2".as_slice())),
                ("x", Some(b"3".as_slice())),
            ]),
            list(&[("x", None), ("x", Some(b"".as_slice())), ("x", None)]),
            // Names and values that are not text.
            list(&[("üñïçø∂é", Some(&[0u8, 0xff, 0x80][..]))]),
        ];

        for headers in cases {
            let records = vec![record(0, 0, Some(b"k"), b"v", 1)];
            let batch = encode(&records, std::slice::from_ref(&headers)).expect("encoded");
            let mut raw = batch.clone();
            let sets = RecordBatchDecoder::decode_all(&mut raw).expect("decoded");
            let back = header_lists(&section(&batch), &sets[0].records)
                .unwrap_or_else(|| panic!("{headers:?} did not read back"));
            assert_eq!(back, vec![headers]);
        }
    }

    /// The alignment check: a walk that does not collapse to the map the crate
    /// decoded is not trusted, and the caller keeps the crate's answer.
    #[test]
    fn a_section_that_does_not_agree_is_refused() {
        let records = vec![record(0, 0, None, b"v", 1)];
        let lists = vec![list(&[("x", Some(b"1".as_slice()))])];
        let batch = encode(&records, &lists).unwrap();
        let mut raw = batch.clone();
        let decoded = RecordBatchDecoder::decode_all(&mut raw).unwrap()[0]
            .records
            .clone();

        // Truncated, empty, and a section belonging to other records.
        assert_eq!(header_lists(&[], &decoded), None);
        let ours = section(&batch);
        assert_eq!(header_lists(&ours[..ours.len() - 1], &decoded), None);
        let other = encode(
            &[record(0, 0, None, b"v", 1)],
            &[list(&[("y", Some(b"1".as_slice()))])],
        )
        .unwrap();
        assert_eq!(header_lists(&section(&other), &decoded), None);
        // ...and the section it was really decoded from does agree.
        assert_eq!(header_lists(&ours, &decoded), Some(lists));
    }

    /// A section carrying more records than the crate decoded is read for the
    /// records it was asked about and no further: the answer is index-aligned
    /// with them, which is what the caller zips against.
    #[test]
    fn only_the_records_asked_about_are_walked() {
        let records = vec![
            record(0, 0, None, b"a", 1),
            record(1, 0, None, b"b", 1),
            record(2, 0, None, b"c", 1),
        ];
        let lists = vec![
            list(&[("a", Some(b"1".as_slice()))]),
            list(&[("b", Some(b"2".as_slice()))]),
            list(&[("c", Some(b"3".as_slice()))]),
        ];
        let batch = encode(&records, &lists).unwrap();
        let mut raw = batch.clone();
        let decoded = RecordBatchDecoder::decode_all(&mut raw).unwrap()[0]
            .records
            .clone();
        assert_eq!(
            header_lists(&section(&batch), &decoded[..2]),
            Some(lists[..2].to_vec())
        );
    }

    // --------------------------------------------------------------- varints

    /// The two varint codecs against the crate's, which is the reference
    /// implementation for both: same zigzag, same continuation bits, same
    /// truncation on the last byte.
    #[test]
    fn the_varints_are_the_crates_varints() {
        let ints: [i32; 12] = [
            0,
            1,
            -1,
            63,
            64,
            -64,
            8191,
            8192,
            i32::MAX,
            i32::MIN,
            1_000_000,
            -1_000_000,
        ];
        for v in ints {
            let mut buf = BytesMut::new();
            put_varint(&mut buf, v);
            assert_eq!(
                buf.len(),
                varint_len(v),
                "the length of {v} is not its size"
            );
            assert_eq!(
                Cursor::new(&buf).varint(),
                Some(v),
                "{v} did not round trip"
            );
        }
        let longs: [i64; 10] = [
            0,
            1,
            -1,
            i64::from(i32::MAX) + 1,
            i64::MAX,
            i64::MIN,
            -1_756_000_000_000,
            1_756_000_000_000,
            127,
            -128,
        ];
        for v in longs {
            let mut buf = BytesMut::new();
            put_varlong(&mut buf, v);
            assert_eq!(
                buf.len(),
                varlong_len(v),
                "the length of {v} is not its size"
            );
            assert_eq!(
                Cursor::new(&buf).varlong(),
                Some(v),
                "{v} did not round trip"
            );
        }
    }

    /// Nothing a client can send makes the reader run past its buffer or
    /// allocate for a number it made up.
    #[test]
    fn a_hostile_section_is_answered_none() {
        let records = vec![record(0, 0, None, b"v", 1)];
        for hostile in [
            // A record length past the section.
            vec![0xfe, 0xff, 0xff, 0xff, 0x0f],
            // A negative record length.
            vec![0x01],
            // A body that ends inside its own header count.
            vec![0x06, 0x00, 0x00, 0x00, 0xff, 0xff],
            // A header count of two billion on a body that ends with it.
            vec![
                0x14, 0x00, 0x00, 0x00, 0x01, 0x01, 0xfe, 0xff, 0xff, 0xff, 0x0f,
            ],
            // A varint that never terminates.
            vec![0xff; 32],
        ] {
            assert_eq!(header_lists(&hostile, &records), None, "{hostile:?}");
        }
    }
}
