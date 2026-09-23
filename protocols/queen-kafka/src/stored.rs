//! Kafka batches as the broker STORES them, read back for the Queen side.
//!
//! From phase 2 of the Kafka-on-raft plan a raft broker running this facade
//! in-process appends a Produce's RecordBatch v2 bytes VERBATIM — one Queen
//! `Append` per partition, `count` = the records in it — instead of one JSON
//! envelope per record (server/src/rsm/kafka_batch.rs owns that format and the
//! offset stamping). A Kafka Fetch hands those bytes back untouched; this module
//! is for everything that is NOT a Kafka Fetch: a native `pop`, the JSON
//! `POST /api/v1/fetch`, the message browser, a dead letter. They all expect one
//! Queen message per record, and they get exactly the message the envelope path
//! used to store: [`crate::records::encode`]'s `{"k","v","h","t"}` object, so a
//! native consumer of a Kafka topic reads the same JSON whichever way the record
//! was written.
//!
//! Decoding goes through the same budgeted decompressors the produce path uses
//! ([`crate::decompress`]), and the header lists are recovered from the
//! decompressed bytes ([`crate::wire::header_lists`]) so a header name a
//! producer repeated survives the trip here too.

use bytes::Bytes;

use crate::decompress::{self, Budget, Refusal};
use crate::records as envelope;
use crate::wire;

/// The most one stored `Append` may decompress to when read back for the Queen
/// side: the same ceiling a whole Produce request may expand to, because one
/// `Append` is at most one partition of one Produce request.
pub const MAX_DECODED_BYTES: usize = crate::conn::MAX_FRAME_BYTES;

/// The most records one stored `Append` may declare when read back.
pub const MAX_DECODED_RECORDS: usize = 1_000_000;

/// One Kafka record, as a Queen message.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredRecord {
    /// The record's absolute offset: its batch's stamped base plus its delta.
    pub offset: i64,
    /// The producer's timestamp (CreateTime), milliseconds.
    pub timestamp: i64,
    /// The envelope, serialized: the payload a native consumer reads.
    pub envelope: Vec<u8>,
}

/// Decode the RecordBatch v2 bytes of one stored `Append` into one Queen
/// message per record, in offset order.
///
/// `batches` is the payload AFTER the broker's 5-byte prefix. An error is a
/// batch that no longer decodes — corruption on disk, since the facade checked
/// every CRC before the bytes were appended — and names the cause.
pub fn decode(batches: &[u8]) -> Result<Vec<StoredRecord>, String> {
    let budget = Budget::new(MAX_DECODED_BYTES, MAX_DECODED_RECORDS);
    let mut raw = Bytes::copy_from_slice(batches);
    let decoded = decompress::decode_all(&mut raw, &budget).map_err(|r| match r {
        Refusal::Corrupt(why) => format!("stored batch does not decode: {why}"),
        Refusal::TooLarge(why) => format!("stored batch decodes past its budget: {why}"),
    })?;
    let mut out = Vec::new();
    for batch in &decoded {
        let lists = wire::header_lists(&batch.records, &batch.set.records);
        for (i, record) in batch.set.records.iter().enumerate() {
            let value = envelope::encode(
                record,
                lists.as_ref().and_then(|l| l.get(i)).map(Vec::as_slice),
            );
            out.push(StoredRecord {
                offset: record.offset,
                timestamp: record.timestamp,
                envelope: serde_json::to_vec(&value)
                    .map_err(|e| format!("cannot serialize an envelope: {e}"))?,
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::protocol::StrBytes;
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };

    pub(crate) fn batch(base: i64, values: &[&[u8]], compression: Compression) -> Vec<u8> {
        let records: Vec<Record> = values
            .iter()
            .enumerate()
            .map(|(i, v)| Record {
                transactional: false,
                control: false,
                partition_leader_epoch: -1,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: base + i as i64,
                sequence: -1,
                timestamp: 1_700_000_000_000 + i as i64,
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::copy_from_slice(v)),
                headers: {
                    let mut h = IndexMap::new();
                    h.insert(
                        StrBytes::from_static_str("h"),
                        Some(Bytes::from_static(b"x")),
                    );
                    h
                },
                delete_horizon: false,
            })
            .collect();
        let mut out = bytes::BytesMut::new();
        RecordBatchEncoder::encode(
            &mut out,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .unwrap();
        out.to_vec()
    }

    /// Every record comes back as the envelope the JSON path stores, at its
    /// stamped offset, for every codec a producer may choose.
    #[test]
    fn a_stored_batch_reads_back_as_the_envelopes_the_json_path_stores() {
        for codec in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let mut bytes = batch(40, &[b"one", b"two"], codec);
            bytes.extend(batch(42, &[b"three"], codec));
            let got = decode(&bytes).unwrap();
            assert_eq!(got.len(), 3, "{codec:?}");
            assert_eq!(
                got.iter().map(|r| r.offset).collect::<Vec<_>>(),
                vec![40, 41, 42]
            );
            let first: serde_json::Value = serde_json::from_slice(&got[0].envelope).unwrap();
            let decoded = envelope::decode(&first, None);
            assert_eq!(decoded.key.as_deref(), Some(&b"k0"[..]));
            assert_eq!(decoded.value.as_deref(), Some(&b"one"[..]));
            assert_eq!(decoded.headers.len(), 1);
            assert_eq!(decoded.timestamp, 1_700_000_000_000);
        }
    }

    #[test]
    fn a_damaged_batch_is_an_error_not_a_panic() {
        let mut bytes = batch(0, &[b"v"], Compression::None);
        let n = bytes.len();
        bytes[n - 1] ^= 0xFF;
        assert!(decode(&bytes).is_err());
        assert!(decode(&bytes[..10]).is_err());
    }
}
