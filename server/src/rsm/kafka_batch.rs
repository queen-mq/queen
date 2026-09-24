//! Kafka record batches stored VERBATIM (phase 2 of the Kafka-on-raft plan).
//!
//! A Kafka Produce reaching this broker through the in-process facade
//! (src/kafka_inproc.rs) appends each partition's RecordBatch v2 bytes as ONE
//! [`crate::rsm::effect::Effect::Append`] whose `count` is the number of records
//! in them. Nothing in the replicated or on-disk format changes, because the
//! payload describes itself:
//!
//! ```text
//! MAGIC (FF FF FF FF) | VERSION (1) | RecordBatch v2 | RecordBatch v2 | …
//! ```
//!
//! A packed-frames payload can never start with [`MAGIC`]: its first four bytes
//! are the first frame's body length, and a frame body is capped far below
//! `0xFFFF_FFFF` (`MAX_FRAME_BODY_LEN`, 256 MiB). Every reader of an `Append`'s
//! payload tells the two apart by that prefix alone ([`is_kafka`]).
//!
//! ## Offsets
//!
//! The planner stamps each batch's `baseOffset` — the first 8 bytes of the
//! batch, OUTSIDE its CRC, which covers `attributes..end` — so the stored bytes
//! are exactly what a Kafka Fetch returns: no recompression and no CRC
//! recomputation, which is Kafka's own in-place offset assignment. The
//! `partitionLeaderEpoch` is left as the producer wrote it (-1), the same
//! unknown epoch Metadata advertises.
//!
//! ## The Queen identity of a Kafka record
//!
//! The Queen side (a native pop, its ack, a dead letter) still sees one message
//! per record. Each record's transaction id is SYNTHETIC — `kafka:<offset>`,
//! unique within its partition by construction and derivable from the offset
//! alone — and the `Append` carries its xxh3_128 exactly as it carries a pushed
//! message's, so the delivered set, ack-by-hash and every index built on the
//! hash list work unchanged. A Kafka append never PROBES the dedup index (Kafka
//! deduplicates by producer sequence, not by id), and a native producer that
//! chose `kafka:<n>` as its own transaction id would collide with the record
//! at offset n: the prefix is reserved.

/// The first four bytes of a stored Kafka payload.
pub const MAGIC: [u8; 4] = [0xFF; 4];

/// The payload format version after [`MAGIC`].
pub const VERSION: u8 = 1;

/// `MAGIC | VERSION`.
pub const PREFIX_LEN: usize = 5;

/// The fixed RecordBatch v2 header: `baseOffset` through `recordsCount`.
pub const BATCH_HEADER_LEN: usize = 61;

/// Bytes of the header before `batchLength` counts: `baseOffset` and the
/// length field itself.
const LEN_PREFIX: usize = 12;

/// Attribute bits (KIP-98).
const ATTR_TRANSACTIONAL: i16 = 1 << 4;
const ATTR_CONTROL: i16 = 1 << 5;

/// Whether an `Append` payload is stored Kafka batches rather than frames.
pub fn is_kafka(blob: &[u8]) -> bool {
    blob.len() >= PREFIX_LEN && blob[..4] == MAGIC
}

/// A stored payload for `batches`.
pub fn wrap(batches: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(PREFIX_LEN + batches.len());
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.extend_from_slice(batches);
    out
}

/// The batches of a stored payload, or `None` when `blob` is not one.
pub fn batches(blob: &[u8]) -> Option<&[u8]> {
    (is_kafka(blob) && blob[4] == VERSION).then(|| &blob[PREFIX_LEN..])
}

/// One batch header, as far as an append needs it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchHead {
    /// Where the batch starts within the batches slice.
    pub start: usize,
    /// Its whole length, header included.
    pub len: usize,
    pub records: u32,
    pub attributes: i16,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub max_timestamp: i64,
}

fn be_i16(b: &[u8], at: usize) -> i16 {
    i16::from_be_bytes([b[at], b[at + 1]])
}
fn be_i32(b: &[u8], at: usize) -> i32 {
    i32::from_be_bytes(b[at..at + 4].try_into().unwrap())
}
fn be_i64(b: &[u8], at: usize) -> i64 {
    i64::from_be_bytes(b[at..at + 8].try_into().unwrap())
}

/// Walk the batch headers of `batches` without decompressing anything.
///
/// Refuses what the append cannot number honestly: a batch that is not v2, a
/// length that runs past the bytes, a batch with no records, one whose offsets
/// are not dense (`lastOffsetDelta != records - 1`, which a producer never
/// sends), and a transactional or control batch, which never reaches the
/// verbatim path (EndTxn writes transactions). The CRCs were checked by the
/// facade before the bytes left it.
pub fn scan(batches: &[u8]) -> Result<Vec<BatchHead>, String> {
    let mut out = Vec::new();
    let mut at = 0usize;
    while at < batches.len() {
        let rest = &batches[at..];
        if rest.len() < BATCH_HEADER_LEN {
            return Err(format!(
                "batch at byte {at} is {} bytes, shorter than a v2 header",
                rest.len()
            ));
        }
        let magic = rest[16];
        if magic != 2 {
            return Err(format!("batch at byte {at} has magic {magic}, not 2"));
        }
        let batch_len = be_i32(rest, 8);
        if batch_len < (BATCH_HEADER_LEN - LEN_PREFIX) as i32 {
            return Err(format!("batch at byte {at} declares length {batch_len}"));
        }
        let len = LEN_PREFIX + batch_len as usize;
        if len > rest.len() {
            return Err(format!(
                "batch at byte {at} declares {len} bytes, {} remain",
                rest.len()
            ));
        }
        let attributes = be_i16(rest, 21);
        if attributes & (ATTR_TRANSACTIONAL | ATTR_CONTROL) != 0 {
            return Err(format!(
                "batch at byte {at} is transactional or control (attributes {attributes:#x})"
            ));
        }
        let last_offset_delta = be_i32(rest, 23);
        let records = be_i32(rest, 57);
        if records <= 0 || last_offset_delta != records - 1 {
            return Err(format!(
                "batch at byte {at} declares {records} records with last offset delta \
                 {last_offset_delta}; only dense batches are appended"
            ));
        }
        out.push(BatchHead {
            start: at,
            len,
            records: records as u32,
            attributes,
            max_timestamp: be_i64(rest, 35),
            producer_id: be_i64(rest, 43),
            producer_epoch: be_i16(rest, 51),
            base_sequence: be_i32(rest, 53),
        });
        at += len;
    }
    if out.is_empty() {
        return Err("no record batch".to_string());
    }
    Ok(out)
}

/// The records `heads` hold, summed.
pub fn record_count(heads: &[BatchHead]) -> u64 {
    heads.iter().map(|h| h.records as u64).sum()
}

/// Stamp each batch's `baseOffset`, the first starting at `base` and each next
/// one after the records of the ones before it. `batches` is the slice
/// [`scan`] produced `heads` from.
pub fn stamp(batches: &mut [u8], heads: &[BatchHead], base: u64) {
    let mut next = base;
    for h in heads {
        batches[h.start..h.start + 8].copy_from_slice(&(next as i64).to_be_bytes());
        next += h.records as u64;
    }
}

/// The synthetic transaction id of the Kafka record at `offset` (module header).
pub fn txn(offset: u64) -> String {
    format!("kafka:{offset}")
}

/// The xxh3_128 of [`txn`], the hash an `Append` carries for that record.
pub fn hash(offset: u64) -> [u8; 16] {
    crate::util::txn_hash128(&txn(offset))
}

/// The hash list of an append of `count` records from `base`.
pub fn hashes(base: u64, count: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(count as usize * 16);
    for off in base..base + count {
        out.extend_from_slice(&hash(off));
    }
    out
}

/// A stable, cluster-unique message id for the Kafka record at `offset` of
/// partition `pid`: an xxh3_128 of the two, shaped as a version-8 UUID.
pub fn message_id(pid: u64, offset: u64) -> [u8; 16] {
    let mut seed = [0u8; 16];
    seed[..8].copy_from_slice(&pid.to_le_bytes());
    seed[8..].copy_from_slice(&offset.to_le_bytes());
    let mut id = xxhash_rust::xxh3::xxh3_128(&seed).to_be_bytes();
    id[6] = (id[6] & 0x0F) | 0x80; // version 8
    id[8] = (id[8] & 0x3F) | 0x80; // RFC 4122 variant
    id
}

/// One Kafka record of a stored payload, as the Queen side reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueenView {
    pub offset: u64,
    pub message_id: [u8; 16],
    pub txn: String,
    /// The JSON envelope the facade's `records::encode` builds.
    pub payload: Vec<u8>,
}

/// The records of a stored payload as Queen messages, in offset order.
///
/// Decoding needs the Kafka codecs, which come with the `kafka` feature; a
/// binary built without it cannot read a Kafka record for the Queen side and
/// says so.
#[cfg(feature = "kafka")]
pub fn queen_view(blob: &[u8], pid: u64) -> Result<Vec<QueenView>, String> {
    let batches = batches(blob).ok_or("not a stored Kafka payload")?;
    let records = queen_kafka::stored::decode(batches)?;
    Ok(records
        .into_iter()
        .map(|r| {
            let offset = r.offset.max(0) as u64;
            QueenView {
                offset,
                message_id: message_id(pid, offset),
                txn: txn(offset),
                payload: r.envelope,
            }
        })
        .collect())
}

/// Without the `kafka` feature a stored Kafka payload cannot be decoded.
#[cfg(not(feature = "kafka"))]
pub fn queen_view(_blob: &[u8], _pid: u64) -> Result<Vec<QueenView>, String> {
    Err(
        "this binary was built without the `kafka` feature and cannot decode a stored \
         Kafka batch"
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal, uncompressed v2 batch of `n` records as a producer writes it:
    /// base offset 0, dense deltas, producer id -1. The records section is
    /// opaque to everything here, so it is filled with bytes that are not
    /// records; only the header is read.
    pub(crate) fn fake_batch(n: i32) -> Vec<u8> {
        let body = vec![0u8; 8 * n as usize];
        let batch_len = (BATCH_HEADER_LEN - LEN_PREFIX + body.len()) as i32;
        let mut b = Vec::new();
        b.extend_from_slice(&0i64.to_be_bytes()); // baseOffset
        b.extend_from_slice(&batch_len.to_be_bytes());
        b.extend_from_slice(&(-1i32).to_be_bytes()); // partitionLeaderEpoch
        b.push(2); // magic
        b.extend_from_slice(&0u32.to_be_bytes()); // crc
        b.extend_from_slice(&0i16.to_be_bytes()); // attributes
        b.extend_from_slice(&(n - 1).to_be_bytes()); // lastOffsetDelta
        b.extend_from_slice(&7i64.to_be_bytes()); // baseTimestamp
        b.extend_from_slice(&9i64.to_be_bytes()); // maxTimestamp
        b.extend_from_slice(&(-1i64).to_be_bytes()); // producerId
        b.extend_from_slice(&(-1i16).to_be_bytes()); // producerEpoch
        b.extend_from_slice(&(-1i32).to_be_bytes()); // baseSequence
        b.extend_from_slice(&n.to_be_bytes()); // recordsCount
        b.extend_from_slice(&body);
        b
    }

    #[test]
    fn a_frames_payload_is_never_mistaken_for_kafka() {
        let frames = crate::frames::pack_frames(&[crate::frames::FrameIn {
            message_id: [1; 16],
            txn: "t",
            trace_id: None,
            producer_sub: None,
            payload: b"{}",
            encrypted: false,
        }]);
        assert!(!is_kafka(&frames));
        assert!(is_kafka(&wrap(&fake_batch(1))));
        assert!(batches(&wrap(&fake_batch(1))).is_some());
    }

    #[test]
    fn only_an_idempotent_producers_payload_writes_a_window() {
        assert!(!writes_window(&wrap(&fake_batch(2))));
        let mut idempotent = fake_batch(2);
        idempotent[43..51].copy_from_slice(&7i64.to_be_bytes()); // producerId
        assert!(writes_window(&wrap(&idempotent)));
        assert!(!writes_window(b"not a stored Kafka payload"));
    }

    #[test]
    fn two_batches_are_scanned_and_stamped_in_order() {
        let mut two = fake_batch(3);
        two.extend(fake_batch(2));
        let heads = scan(&two).unwrap();
        assert_eq!(heads.len(), 2);
        assert_eq!(record_count(&heads), 5);
        assert_eq!(heads[0].max_timestamp, 9);
        stamp(&mut two, &heads, 100);
        assert_eq!(be_i64(&two, heads[0].start), 100);
        assert_eq!(be_i64(&two, heads[1].start), 103);
        // The CRC-covered region is untouched: only the 8 base-offset bytes of
        // each header moved.
        let mut fresh = fake_batch(3);
        fresh.extend(fake_batch(2));
        let differs: Vec<usize> = (0..two.len()).filter(|&i| two[i] != fresh[i]).collect();
        assert!(differs
            .iter()
            .all(|&i| i < 8 || (i >= heads[1].start && i < heads[1].start + 8)));
    }

    #[test]
    fn what_cannot_be_numbered_is_refused() {
        assert!(scan(&[]).is_err());
        assert!(scan(&fake_batch(2)[..30]).is_err());
        let mut v1 = fake_batch(1);
        v1[16] = 1;
        assert!(scan(&v1).is_err());
        let mut txn = fake_batch(1);
        txn[22] |= ATTR_TRANSACTIONAL as u8;
        assert!(scan(&txn).is_err());
        let mut sparse = fake_batch(3);
        sparse[23..27].copy_from_slice(&5i32.to_be_bytes());
        assert!(scan(&sparse).is_err());
        let mut long = fake_batch(1);
        long[8..12].copy_from_slice(&10_000i32.to_be_bytes());
        assert!(scan(&long).is_err());
    }

    #[test]
    fn the_synthetic_identity_is_stable_and_distinct() {
        assert_eq!(txn(42), "kafka:42");
        assert_eq!(hash(42), crate::util::txn_hash128("kafka:42"));
        assert_ne!(hash(42), hash(43));
        let h = hashes(10, 3);
        assert_eq!(h.len(), 48);
        assert_eq!(&h[16..32], &hash(11));
        assert_eq!(message_id(1, 5), message_id(1, 5));
        assert_ne!(message_id(1, 5), message_id(2, 5));
        assert_eq!(message_id(1, 5)[6] >> 4, 8);
    }
}

// ---------------------------------------------------------------------------
// The idempotent producer, durable (phase 2).
//
// The facade's sequence window (protocols/queen-kafka/src/idempotent.rs) held
// in the facade's memory, so a restart cost at-least-once for the batches in
// flight. On the verbatim path the window is the BROKER's: one KV row per
// (partition, producer) in the facade's namespace, read and rewritten by the
// planner in the SAME entry as the append it admits — durable and replicated
// with it, and exactly the facade's rules, which are Kafka's.
// ---------------------------------------------------------------------------

/// The facade's KV namespace: the windows live beside its committed offsets.
pub const PRODUCER_NS: &str = "queen-kafka";

/// Batches remembered per (partition, producer): Kafka's own five.
pub const WINDOW: usize = 5;

/// How long a window outlives its producer's last append: Kafka's
/// `producer.id.expiration.ms` default, one day.
pub const PRODUCER_TTL_US: i64 = 86_400 * 1_000_000;

/// The KV key of a producer's window on one partition.
pub fn producer_key(producer_id: i64, pid: u64) -> String {
    format!("qk:seq:{pid}:{producer_id}")
}

/// Whether a stored Kafka payload comes from an idempotent producer, so its
/// append rewrites a window: a KV row, whose version only the control step of
/// a lanes cycle may hand out (I18). `false` for anything that is not a
/// well-formed stored payload — the planner refuses those before writing.
pub fn writes_window(blob: &[u8]) -> bool {
    batches(blob)
        .and_then(|b| scan(b).ok())
        .and_then(|heads| heads.first().map(|h| h.producer_id >= 0))
        .unwrap_or(false)
}

/// One producer's window on one partition.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProducerWindow {
    /// The producer epoch the remembered batches belong to.
    pub e: i16,
    /// The newest batches, oldest first: `(base_seq, last_seq, base_offset)`.
    pub r: Vec<(i32, i32, u64)>,
}

/// What the window says about an append.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SeqVerdict {
    /// No producer id: at-least-once, nothing to check.
    NotIdempotent,
    /// The next batches of this producer: append them.
    Accept,
    /// Exactly what was appended before: answer its base offset, write nothing.
    Duplicate(u64),
    /// Refused, with the Kafka error's NAME as the code.
    Refuse { code: &'static str, message: String },
}

/// Kafka's own sequence arithmetic, wrap included.
pub fn increment_sequence(seq: i32, n: i32) -> i32 {
    if seq > i32::MAX - n {
        n - (i32::MAX - seq) - 1
    } else {
        seq + n
    }
}

fn last_sequence(h: &BatchHead) -> i32 {
    increment_sequence(h.base_sequence, h.records as i32 - 1)
}

fn is_before(seq: i32, expected: i32) -> bool {
    seq.wrapping_sub(expected) < 0
}

fn refuse(code: &'static str, message: String) -> SeqVerdict {
    SeqVerdict::Refuse { code, message }
}

/// The facade's `Producers::check`, over a window read from the log.
pub fn check_sequence(heads: &[BatchHead], window: Option<&ProducerWindow>) -> SeqVerdict {
    let idempotent = heads.iter().filter(|h| h.producer_id >= 0).count();
    if idempotent == 0 {
        return SeqVerdict::NotIdempotent;
    }
    if idempotent != heads.len() {
        return refuse(
            "INVALID_RECORD",
            "one produce entry mixes idempotent and non-idempotent record batches".into(),
        );
    }
    let first = &heads[0];
    if heads
        .iter()
        .any(|h| h.producer_id != first.producer_id || h.producer_epoch != first.producer_epoch)
    {
        return refuse(
            "INVALID_RECORD",
            "one produce entry carries record batches from more than one producer session".into(),
        );
    }
    for pair in heads.windows(2) {
        let expected = increment_sequence(last_sequence(&pair[0]), 1);
        if pair[1].base_sequence != expected {
            return refuse(
                "OUT_OF_ORDER_SEQUENCE_NUMBER",
                format!(
                    "the batches of this request are not contiguous: sequence {} follows a batch \
                     ending at {}",
                    pair[1].base_sequence,
                    last_sequence(&pair[0])
                ),
            );
        }
    }
    let (producer, epoch, base_seq) =
        (first.producer_id, first.producer_epoch, first.base_sequence);
    let Some(w) = window else {
        return if base_seq == 0 {
            SeqVerdict::Accept
        } else {
            refuse(
                "OUT_OF_ORDER_SEQUENCE_NUMBER",
                format!(
                    "no sequence state for producer {producer} on this partition, and the batch \
                     starts at sequence {base_seq} rather than 0 — bump the producer epoch and \
                     resend (KIP-360)"
                ),
            )
        };
    };
    if epoch < w.e {
        return refuse(
            "INVALID_PRODUCER_EPOCH",
            format!(
                "producer {producer} sent epoch {epoch}, below the epoch {} already seen",
                w.e
            ),
        );
    }
    if epoch > w.e {
        // A bump is a reset: the producer restarted its own sequences.
        return SeqVerdict::Accept;
    }
    let Some(newest) = w.r.last() else {
        return if base_seq == 0 {
            SeqVerdict::Accept
        } else {
            refuse(
                "OUT_OF_ORDER_SEQUENCE_NUMBER",
                format!(
                    "nothing has been appended for producer {producer} at epoch {epoch}, and the \
                     batch starts at sequence {base_seq} rather than 0"
                ),
            )
        };
    };
    let expected = increment_sequence(newest.1, 1);
    if base_seq == expected {
        return SeqVerdict::Accept;
    }
    // A resend of exactly what was appended: the offsets the original got.
    if let Some(at) =
        w.r.iter()
            .position(|b| b.0 == base_seq && b.1 == last_sequence(first))
    {
        let run = &w.r[at..];
        if run.len() >= heads.len()
            && run
                .iter()
                .zip(heads)
                .all(|(b, h)| b.0 == h.base_sequence && b.1 == last_sequence(h))
        {
            return SeqVerdict::Duplicate(run[0].2);
        }
    }
    if is_before(base_seq, expected) {
        return refuse(
            "DUPLICATE_SEQUENCE_NUMBER",
            format!(
                "sequence {base_seq} for producer {producer} is at or below the last appended \
                 sequence {} and is not a batch this log appended",
                newest.1
            ),
        );
    }
    refuse(
        "OUT_OF_ORDER_SEQUENCE_NUMBER",
        format!(
            "sequence {base_seq} for producer {producer} would leave a gap: the next sequence \
             this log will append is {expected}"
        ),
    )
}

/// The window after appending `heads` at `base` (the facade's `commit`).
pub fn advance(window: Option<ProducerWindow>, heads: &[BatchHead], base: u64) -> ProducerWindow {
    let epoch = heads[0].producer_epoch;
    let mut w = match window {
        Some(w) if w.e >= epoch => w,
        _ => ProducerWindow {
            e: epoch,
            r: Vec::new(),
        },
    };
    let mut offset = base;
    for h in heads {
        w.r.push((h.base_sequence, last_sequence(h), offset));
        offset += h.records as u64;
    }
    if w.r.len() > WINDOW {
        let cut = w.r.len() - WINDOW;
        w.r.drain(..cut);
    }
    w
}

#[cfg(test)]
mod idem_tests {
    use super::*;

    fn head(producer_id: i64, epoch: i16, base_sequence: i32, records: u32) -> BatchHead {
        BatchHead {
            start: 0,
            len: 0,
            records,
            attributes: 0,
            producer_id,
            producer_epoch: epoch,
            base_sequence,
            max_timestamp: 0,
        }
    }

    /// The facade's window rules, over a durable window: first batch, next
    /// batch, exact resend, gap, fenced epoch, bump, and the five kept.
    #[test]
    fn the_window_is_kafkas() {
        assert_eq!(
            check_sequence(&[head(-1, -1, -1, 3)], None),
            SeqVerdict::NotIdempotent
        );
        assert_eq!(
            check_sequence(&[head(7, 0, 0, 3)], None),
            SeqVerdict::Accept
        );
        assert!(matches!(
            check_sequence(&[head(7, 0, 5, 3)], None),
            SeqVerdict::Refuse {
                code: "OUT_OF_ORDER_SEQUENCE_NUMBER",
                ..
            }
        ));
        let w = advance(None, &[head(7, 0, 0, 3)], 100);
        assert_eq!(w.r, vec![(0, 2, 100)]);
        assert_eq!(
            check_sequence(&[head(7, 0, 3, 2)], Some(&w)),
            SeqVerdict::Accept
        );
        assert_eq!(
            check_sequence(&[head(7, 0, 0, 3)], Some(&w)),
            SeqVerdict::Duplicate(100)
        );
        assert!(matches!(
            check_sequence(&[head(7, 0, 9, 1)], Some(&w)),
            SeqVerdict::Refuse {
                code: "OUT_OF_ORDER_SEQUENCE_NUMBER",
                ..
            }
        ));
        assert!(matches!(
            check_sequence(&[head(7, 0, 1, 1)], Some(&w)),
            SeqVerdict::Refuse {
                code: "DUPLICATE_SEQUENCE_NUMBER",
                ..
            }
        ));
        let bumped = advance(Some(w.clone()), &[head(7, 1, 0, 1)], 200);
        assert_eq!(bumped.e, 1);
        assert_eq!(bumped.r, vec![(0, 0, 200)]);
        assert!(matches!(
            check_sequence(&[head(7, 0, 3, 1)], Some(&bumped)),
            SeqVerdict::Refuse {
                code: "INVALID_PRODUCER_EPOCH",
                ..
            }
        ));
        let mut long = w;
        for i in 0..10u32 {
            long = advance(Some(long), &[head(7, 0, 3 + i as i32, 1)], 103 + i as u64);
        }
        assert_eq!(long.r.len(), WINDOW);
        assert_eq!(long.r.last(), Some(&(12, 12, 112)));
        // A two-batch resend matches as a run.
        let w2 = advance(None, &[head(8, 0, 0, 2), head(8, 0, 2, 2)], 0);
        assert_eq!(
            check_sequence(&[head(8, 0, 0, 2), head(8, 0, 2, 2)], Some(&w2)),
            SeqVerdict::Duplicate(0)
        );
    }

    #[test]
    fn sequences_wrap_as_kafkas_do() {
        assert_eq!(increment_sequence(i32::MAX, 1), 0);
        assert_eq!(increment_sequence(5, 3), 8);
    }
}
