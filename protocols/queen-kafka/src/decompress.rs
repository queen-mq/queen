//! Record-batch decoding with a ceiling on the memory a batch is allowed to ask
//! for — what it expands to, and what it says it will expand to.
//!
//! `compression.type` is a PRODUCER setting: which of the four codecs arrives is
//! the client's choice and there is no negotiation to refuse it (`Cargo.toml`,
//! `handlers::produce`). What the client also chooses, and what nothing on the
//! wire declares, is the RATIO — a record batch of a few hundred bytes is a
//! legal zstd frame for hundreds of megabytes of zeros, and
//! `RecordBatchDecoder::decode_all` materialises whatever comes out of the codec
//! into one buffer before a single record is read. `conn::MAX_FRAME_BYTES`
//! bounds the COMPRESSED request and says nothing about that, so the frame limit
//! a Kafka broker relies on is no bound at all here: one 10 KB frame is enough
//! to ask for terabytes of heap, from an unauthenticated peer, on a connection
//! nobody had to open a second of.
//!
//! So the expansion is budgeted. [`decode_all`] is `decode_all` with the four
//! codecs replaced by bounded ones ([`RecordBatchDecoder::decode_with_custom_compression`]
//! is the seam the crate provides for exactly this), and every byte a codec
//! produces is charged to a [`Budget`] the CALLER owns — one per Produce
//! request, not one per batch, because a request carries as many batches as it
//! likes and a per-batch ceiling would multiply by their number.
//!
//! ## The ceiling, and why it is the frame size
//!
//! The budget the caller sets is `conn::MAX_FRAME_BYTES`: exactly the records a
//! producer could have sent UNCOMPRESSED in one request. That is the whole rule
//! — compression buys a client no more room than it already had — and it has the
//! property a smaller, cleverer number would not: it can never be the reason a
//! legitimate produce fails, because a producer that wanted more than this from
//! the facade could not have sent it uncompressed either. What it removes is the
//! amplification, which is the actual defect: a request now costs the sender
//! what it costs the facade.
//!
//! The second ceiling, on DECLARED records, is the same rule applied to the
//! other number a batch header can lie about — see [`Budget`].
//!
//! ## Why the codecs are re-implemented here
//!
//! `kafka-protocol`'s own decompressors write into an unbounded `BytesMut` and
//! take no budget, and there is no hook inside them to add one. Each function
//! below is therefore the crate's own decompressor (kafka-protocol-0.18.0,
//! `src/compression/*.rs`) with its output buffer replaced by [`Bounded`] — same
//! codec crates, same Kafka-flavoured snappy framing, same fallback to raw
//! snappy for the producers that write it. Nothing about the FORMAT is decided
//! here; the only thing added is where the bytes go and when to stop.

use std::io::{self, Write};

use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::records::{Compression, RecordBatchDecoder, RecordSet};

/// Xerial's framing header, the one Kafka producers write around snappy blocks.
/// A stream that does not start with it is raw snappy — see [`snappy`].
const XERIAL_MAGIC: &[u8; 16] = b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01";

/// What one request may still spend on decoding: bytes of decompressed records,
/// and records it has DECLARED it will decode.
///
/// Two ceilings, because a batch has two ways of asking for memory. The bytes
/// are the codec's output (the module header). The count is the `record_count`
/// in the batch header, which the decoder trusts far enough to
/// `Vec::reserve(record_count)` before it reads one byte of records — so a
/// 61-byte batch header declaring two billion records asks for hundreds of
/// gigabytes on a frame that is one packet, and the allocator's answer to that
/// is to abort the process. It is charged BEFORE any decode, off the headers
/// `RecordBatchDecoder::decode_batch_info` reads without decompressing.
///
/// Not `Sync`, deliberately: a budget belongs to the request that made it and
/// crosses no task boundary, so it needs no atomics and cannot be shared into a
/// second decode by accident.
pub struct Budget {
    left: std::cell::Cell<usize>,
    records_left: std::cell::Cell<usize>,
    /// Set by the take that ran out, so the caller can tell "this batch expands
    /// past the ceiling" from "this batch is corrupt" — two different answers to
    /// a producer. Armed (cleared) at the top of every [`decode_all`], so it
    /// always describes the decode that just failed and not an earlier one.
    exhausted: std::cell::Cell<bool>,
}

/// Why a batch did not decode. The two are different Kafka error codes and
/// different producer behaviour, so they are not one string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// The bytes are not a batch this build can read: a failed codec, a
    /// truncated block, a malformed varint.
    Corrupt(String),
    /// The records decode, and they expand past what the request is allowed.
    TooLarge(String),
}

impl Budget {
    pub fn new(bytes: usize, records: usize) -> Budget {
        Budget {
            left: std::cell::Cell::new(bytes),
            records_left: std::cell::Cell::new(records),
            exhausted: std::cell::Cell::new(false),
        }
    }

    /// Charge `n` records a batch header says it carries, or refuse.
    ///
    /// The caller has the headers and has not decoded anything yet, which is the
    /// only moment this can be checked at: one instruction later the decoder has
    /// already reserved for them.
    pub fn declare_records(&self, n: usize) -> Result<(), String> {
        match self.records_left.get().checked_sub(n) {
            Some(left) => {
                self.records_left.set(left);
                Ok(())
            }
            None => Err(format!(
                "the batch headers declare {n} more records with room for {} in this request; \
                 a declared count is memory reserved before a record is read",
                self.records_left.get()
            )),
        }
    }

    /// Charge `n` bytes of output, or refuse.
    fn take(&self, n: usize) -> io::Result<()> {
        match self.left.get().checked_sub(n) {
            Some(left) => {
                self.left.set(left);
                Ok(())
            }
            None => {
                self.exhausted.set(true);
                Err(io::Error::other(format!(
                    "the records expand past what this request may decompress to \
                     ({n} more bytes with {} left)",
                    self.left.get()
                )))
            }
        }
    }
}

/// One decoded batch: what the crate made of it, and the bytes it made it from.
///
/// The second half is here for one field: a Kafka record's headers are an
/// ordered list that may carry a name twice, and `Record.headers` is a map that
/// cannot hold the second one (see [`crate::wire`]). The decompressed records
/// section is the only place the repeat still exists, so it is kept — as
/// `Bytes`, which is a refcount and a range over the buffer the codec already
/// produced, not a copy of it.
#[derive(Debug)]
pub struct Batch {
    pub set: RecordSet,
    pub records: Bytes,
}

/// Every record batch in `raw`, decoded with `budget` bounding the expansion.
///
/// `RecordBatchDecoder::decode_all`'s loop, with the bounded codecs in place of
/// the crate's. The error is a string rather than the crate's `anyhow::Error`
/// because that is all a caller does with it — it becomes the `error_message` of
/// a produce response — and it keeps `anyhow` out of this crate's dependencies.
pub fn decode_all(raw: &mut Bytes, budget: &Budget) -> Result<Vec<Batch>, Refusal> {
    budget.exhausted.set(false);
    let mut batches = Vec::new();
    // Filled by the decompression seam below, which the decoder calls exactly
    // once per batch — the same seam the budget is charged through.
    let section: std::cell::Cell<Option<Bytes>> = std::cell::Cell::new(None);
    while raw.has_remaining() {
        let set = RecordBatchDecoder::decode_with_custom_compression(
            raw,
            Some(|records: &mut Bytes, codec: Compression| {
                decompress(records, codec, budget)
                    .inspect(|out| section.set(Some(out.clone())))
                    .map_err(Into::into)
            }),
        )
        .map_err(|e| {
            let why = format!("{e:#}");
            if budget.exhausted.get() {
                Refusal::TooLarge(why)
            } else {
                Refusal::Corrupt(why)
            }
        })?;
        batches.push(Batch {
            set,
            records: section.take().unwrap_or_default(),
        });
    }
    Ok(batches)
}

/// One batch's records, decompressed into at most what is left of the budget.
fn decompress(raw: &mut Bytes, codec: Compression, budget: &Budget) -> io::Result<Bytes> {
    let compressed = raw.copy_to_bytes(raw.remaining());
    match codec {
        // Nothing to expand: the bytes are already in the frame. Charged all the
        // same, so one request's budget is the total of what it decodes however
        // its batches are compressed.
        Compression::None => {
            budget.take(compressed.len())?;
            Ok(compressed)
        }
        Compression::Gzip => {
            let mut d = flate2::write::GzDecoder::new(Bounded::new(budget));
            d.write_all(&compressed)?;
            Ok(d.finish()?.into_bytes())
        }
        Compression::Snappy => snappy(&compressed, budget),
        Compression::Lz4 => {
            let mut out = Bounded::new(budget);
            let mut d = lz4::Decoder::new(compressed.reader())?;
            io::copy(&mut d, &mut out)?;
            Ok(out.into_bytes())
        }
        Compression::Zstd => {
            let mut out = Bounded::new(budget);
            zstd::stream::copy_decode(compressed.reader(), &mut out)?;
            Ok(out.into_bytes())
        }
    }
}

/// Kafka's snappy: Xerial's length-prefixed blocks, falling back to raw snappy
/// for the producers that write that instead.
///
/// The block structure is walked here rather than streamed through [`Bounded`]
/// because snappy is the one codec that ALLOCATES before it decompresses: every
/// block declares its decompressed length and the decoder writes into a slice of
/// exactly that size, so the bound has to be applied to the declared length
/// before the allocation, not to the bytes after it.
fn snappy(compressed: &Bytes, budget: &Budget) -> io::Result<Bytes> {
    let truncated =
        || io::Error::other("the snappy stream ends in the middle of a block".to_string());

    if !compressed.starts_with(XERIAL_MAGIC) {
        let len = snap::raw::decompress_len(compressed).map_err(io::Error::other)?;
        budget.take(len)?;
        let mut out = vec![0u8; len];
        snap::raw::Decoder::new()
            .decompress(compressed, &mut out)
            .map_err(io::Error::other)?;
        return Ok(out.into());
    }

    let mut rest = &compressed[XERIAL_MAGIC.len()..];
    let mut out: Vec<u8> = Vec::new();
    while !rest.is_empty() {
        let (head, tail) = rest.split_at_checked(4).ok_or_else(truncated)?;
        let size = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as usize;
        let (block, tail) = tail.split_at_checked(size).ok_or_else(truncated)?;
        rest = tail;

        let len = snap::raw::decompress_len(block).map_err(io::Error::other)?;
        budget.take(len)?;
        let start = out.len();
        out.reserve_exact(len);
        out.resize(start + len, 0);
        snap::raw::Decoder::new()
            .decompress(block, &mut out[start..])
            .map_err(io::Error::other)?;
    }
    Ok(out.into())
}

/// An `io::Write` that stops at the budget instead of at the heap.
struct Bounded<'a> {
    out: BytesMut,
    budget: &'a Budget,
}

impl<'a> Bounded<'a> {
    fn new(budget: &'a Budget) -> Bounded<'a> {
        Bounded {
            out: BytesMut::new(),
            budget,
        }
    }

    fn into_bytes(self) -> Bytes {
        self.out.freeze()
    }
}

impl Write for Bounded<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Charged BEFORE the copy: the point is not to notice afterwards.
        self.budget.take(buf.len())?;
        self.out.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::records::{
        Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType, NO_PRODUCER_ID,
    };

    const CODECS: [Compression; 5] = [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ];

    fn record(value: &[u8]) -> Record {
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp: 1_756_000_000_000,
            key: None,
            value: Some(Bytes::copy_from_slice(value)),
            headers: IndexMap::new(),
        }
    }

    /// A batch built the way a CLIENT builds one — `kafka-protocol`'s own
    /// encoder, so what is decoded below is bytes a real producer can send.
    fn batch(records: &[Record], compression: Compression) -> Bytes {
        let mut out = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut out,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .expect("the client side encodes it");
        out.freeze()
    }

    fn values(batches: &[Batch]) -> Vec<Bytes> {
        batches
            .iter()
            .flat_map(|b| b.set.records.iter())
            .map(|r| r.value.clone().unwrap_or_default())
            .collect()
    }

    /// Every codec a producer may choose round-trips, and the budget is charged
    /// the DECOMPRESSED size in each case.
    #[test]
    fn every_codec_round_trips_under_the_budget() {
        for codec in CODECS {
            let records = [record(b"one"), record(b"two"), record(b"three")];
            let mut raw = batch(&records, codec);
            let budget = Budget::new(1024 * 1024, 1024);
            let sets = decode_all(&mut raw, &budget).unwrap_or_else(|e| panic!("{codec:?}: {e:?}"));
            assert_eq!(
                values(&sets),
                vec![
                    Bytes::from_static(b"one"),
                    Bytes::from_static(b"two"),
                    Bytes::from_static(b"three")
                ],
                "{codec:?}"
            );
            assert!(
                budget.left.get() < 1024 * 1024,
                "{codec:?} was decoded without charging the budget"
            );
        }
    }

    /// Every batch keeps the DECOMPRESSED section its records were decoded
    /// from, whatever the producer compressed it with. It is the only place a
    /// repeated header name still exists ([`crate::wire`]), and the walk over it
    /// lining up with the crate's records is what says the two belong together —
    /// including when one entry carries several batches, where a section paired
    /// with the wrong batch would put one record's headers on another.
    #[test]
    fn every_batch_keeps_the_section_its_records_came_from() {
        for codec in CODECS {
            let mut raw = BytesMut::new();
            raw.extend_from_slice(&batch(&[record(b"one"), record(b"two")], codec));
            raw.extend_from_slice(&batch(&[record(b"three")], codec));
            let mut raw = raw.freeze();

            let budget = Budget::new(1024 * 1024, 1024);
            let batches =
                decode_all(&mut raw, &budget).unwrap_or_else(|e| panic!("{codec:?}: {e:?}"));
            assert_eq!(batches.len(), 2, "{codec:?}");
            for (b, records) in batches.iter().zip([2, 1]) {
                assert!(!b.records.is_empty(), "{codec:?}: no section was kept");
                assert_eq!(
                    crate::wire::header_lists(&b.records, &b.set.records),
                    Some(vec![Vec::new(); records]),
                    "{codec:?}: the section does not belong to these records"
                );
            }
        }
    }

    /// Several batches in one partition entry share ONE budget: a request is
    /// not allowed to multiply the ceiling by the number of batches it carries.
    #[test]
    fn batches_in_one_entry_share_the_budget() {
        let mut raw = BytesMut::new();
        for _ in 0..4 {
            raw.extend_from_slice(&batch(&[record(&[b'x'; 64])], Compression::Zstd));
        }
        let mut raw = raw.freeze();

        let budget = Budget::new(1024 * 1024, 1024);
        assert_eq!(decode_all(&mut raw.clone(), &budget).unwrap().len(), 4);
        let spent = 1024 * 1024 - budget.left.get();

        // Two batches' worth of room for four batches: the second half is
        // refused rather than served from a budget that reset.
        let tight = Budget::new(spent / 2, 1024);
        assert!(matches!(
            decode_all(&mut raw, &tight),
            Err(Refusal::TooLarge(_))
        ));
    }

    /// THE defect: a batch whose compressed size says nothing about what it
    /// expands to. 64 MiB of zeros is a couple of kilobytes in every codec that
    /// has one, and a decode with a small budget must refuse it rather than
    /// materialise it.
    #[test]
    fn a_decompression_bomb_is_refused_by_its_output_size() {
        for codec in CODECS {
            if codec == Compression::None {
                continue; // nothing to expand: the frame limit IS the bound
            }
            let bomb = [record(&vec![0u8; 8 * 1024 * 1024])];
            let mut raw = batch(&bomb, codec);
            // The bomb property: the batch as SENT fits in the budget, and what
            // it decodes to does not.
            assert!(
                raw.len() < 1024 * 1024,
                "{codec:?} did not compress the fixture ({} bytes)",
                raw.len()
            );

            let budget = Budget::new(1024 * 1024, 1024);
            match decode_all(&mut raw, &budget) {
                Err(Refusal::TooLarge(_)) => {}
                Err(other) => panic!("{codec:?} was refused as {other:?}, not as too large"),
                Ok(_) => panic!("{codec:?} expanded 8 MiB into a 1 MiB budget"),
            }
        }
    }

    /// Corrupt is not the same answer as too large: a truncated batch is refused
    /// with a budget that has room to spare, and says so.
    #[test]
    fn a_truncated_batch_is_corrupt_and_not_too_large() {
        for codec in CODECS {
            let whole = batch(&[record(b"one"), record(b"two")], codec);
            let mut raw = whole.slice(..whole.len() - 4);
            let budget = Budget::new(1024 * 1024, 1024);
            match decode_all(&mut raw, &budget) {
                Err(Refusal::Corrupt(_)) => {}
                other => panic!("{codec:?} truncated: {other:?}"),
            }
        }
    }

    /// Raw snappy — no Xerial header — is what some producers write, and the
    /// crate's own decoder falls back to it. So does this one, and it is bounded
    /// on the length the block declares rather than after the allocation.
    #[test]
    fn raw_snappy_is_read_and_bounded_on_its_declared_length() {
        let plain = vec![7u8; 512 * 1024];
        let compressed: Bytes = snap::raw::Encoder::new()
            .compress_vec(&plain)
            .expect("raw snappy compresses")
            .into();

        let budget = Budget::new(1024 * 1024, 1024);
        let out = snappy(&compressed, &budget).expect("raw snappy decodes");
        assert_eq!(out.len(), plain.len());
        assert_eq!(budget.left.get(), 1024 * 1024 - plain.len());

        let tight = Budget::new(plain.len() - 1, 1024);
        assert!(snappy(&compressed, &tight).is_err());
        assert!(tight.exhausted.get());
    }

    /// An empty batch is a shape a producer really sends (a flush with nothing
    /// in it) and must not be mistaken for either failure.
    #[test]
    fn an_empty_batch_decodes_to_no_records() {
        for codec in CODECS {
            let mut raw = batch(&[], codec);
            let budget = Budget::new(1024 * 1024, 1024);
            let sets = decode_all(&mut raw, &budget).unwrap_or_else(|e| panic!("{codec:?}: {e:?}"));
            assert!(values(&sets).is_empty(), "{codec:?}");
        }
    }
}
