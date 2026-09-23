//! The per-queue log record of `ALICE_PGLESS_NEWARCH.md` §3.1 — one
//! [`crate::rsm::effect::Effect::Append`]'s bytes on disk, extended from the
//! segment [`crate::rsm::segments::frame`] with the leader's `seq` stamp and
//! the cross-queue transaction envelope Phase B will fill in.
//!
//! ```text
//! len:u32 | xxh3:u64 | seq:u64 | pid:u64 | base_offset:u64 | count:u32 |
//! created_at:i64 | txn_kind:u8 |
//!   [if txn_kind==1: gtid:u128, n_participants:u16, participants:[u64; n]] |
//! hashes[16*count] | payload
//! ```
//!
//! The high bit of `txn_kind` is a flag, not a kind: [`FLAG_PAYLOAD_ZSTD`]
//! marks a message record whose payload is stored zstd-compressed (the log
//! writer's node-local codec, [`super::codec`]). [`Header::kind`] masks it off.
//!
//! Little-endian throughout, hand-rolled in the style of
//! `segments/frame.rs` and the pgless `native/record.rs`: no serde on disk,
//! every field written in one place and read in one place.
//!
//! Two spans decide what the format can do, exactly as in `frame.rs`:
//!
//! - `len` counts every byte AFTER itself, so a scanner that has read the
//!   first four bytes knows exactly where the next record starts without
//!   trusting any other field. That is what makes a `.qlog` file
//!   self-describing and a `.qidx` rebuildable by scanning it (§5).
//! - the checksum covers every byte AFTER itself, so it protects the fields
//!   the reader is about to believe — `seq`, `pid`, `base_offset`, `count`,
//!   `created_at`, the transaction envelope, the hash list and the payload —
//!   not only the payload. A torn tail (the last group that was writing when
//!   the process died) fails this checksum and is truncated (§5).
//!
//! `hashes` is `16 * count` bytes, the per-message transaction-id hashes in
//! frame order, exactly as [`crate::rsm::effect::Effect::Append`] carries them
//! (the dedup authority reads them here — lever 1 re-pointed at the queue log).
//! Its length is derived from `count`, not stored a second time, and `payload`
//! is whatever is left after it. A `count`, an `n_participants` or a `len` that
//! does not fit is refused BEFORE anything is allocated (the anti-OOM rule the
//! frame codec keeps).
//!
//! # What Phase A does and does not do with `txn`
//!
//! The `txn_kind`/`gtid`/`participants` fields are part of the format NOW so
//! Phase B (transactions, present-in-all) does not have to reshape the record
//! and re-encode every file. Phase A only ROUND-TRIPS them: it encodes and
//! decodes a `txn_kind == 1` record faithfully and never interprets the
//! transaction (no commit rule, no participant fan-out). A single-queue op —
//! even multi-partition within one queue — is `txn_kind == 0`.
//!
//! NOTHING HERE IS A POSITION. A record does not know which file or byte offset
//! it landed at; that is node-local (`.qidx`, [`super::index`]) and never part
//! of the bytes two nodes must agree on.

use xxhash_rust::xxh3::xxh3_64;

/// `txn_kind == 0`: a single-queue op (even multi-partition within one queue).
/// The record carries no transaction envelope.
pub const TXN_NONE: u8 = 0;

/// `txn_kind == 1`: a cross-queue transaction. The `gtid` and the participant
/// queue ids follow the fixed header. Reserved for Phase B; Phase A only
/// round-trips it.
pub const TXN_CROSS_QUEUE: u8 = 1;

/// `txn_kind == 2`: a payload-free ENTRY record, not a message — the per-queue
/// log of record that REPLACES the global raft log (per-queue-only, killing the
/// second fsync). It carries the leader's `seq` and the serialized payload-free
/// entry bytes (its effects — a push, a pop, an ack) in the `payload` slot, with
/// `base_offset = count = 0` (it indexes no partition: the message records
/// beside it, `txn_kind ∈ {0,1}`, are what pop/dedup read — the index build
/// SKIPS entry records). Recovery merges every queue's entry records by `seq`
/// and applies them in that order, exactly as the raft-log replay did.
/// `created_at_us` carries the entry's `now_us` so a replay reconstructs the
/// entry's monotone clock without a second field.
///
/// Phase C: the `pid` slot carries `copies` — how many queue logs the writer
/// wrote this SAME entry record to (every queue log its effects touch, or the
/// system log). Recovery treats an entry as durable only when it finds all
/// `copies` of it (a group whose fsyncs did not all land leaves some copies
/// missing, and nothing of that group was acknowledged). `0` (a record written
/// by [`encode_entry_into`] before Phase C) reads as one copy.
pub const REC_ENTRY: u8 = 2;

/// High bit of `txn_kind`: the payload is zstd-compressed. Only message records
/// carry it; the kind is `txn_kind & !FLAG_PAYLOAD_ZSTD` ([`Header::kind`]).
pub const FLAG_PAYLOAD_ZSTD: u8 = 0x80;

/// One message's hash, in bytes: the xxh3_128 of its transaction id, exactly as
/// [`crate::rsm::segments::frame::HASH_LEN`].
pub const HASH_LEN: usize = 16;

/// The fixed, always-present prefix:
/// `len:u32 | xxh3:u64 | seq:u64 | pid:u64 | base_offset:u64 | count:u32 |
/// created_at:i64 | txn_kind:u8`.
pub const FIXED_PREFIX: usize = 4 + 8 + 8 + 8 + 8 + 4 + 8 + 1; // 49

/// Bytes of the fixed prefix that `len` covers: everything after `len` itself.
/// The minimum legal `len` (a `txn_kind == 0`, `count == 0`, empty-payload
/// record is exactly this many body bytes).
const FIXED_AFTER_LEN: usize = FIXED_PREFIX - 4; // 45

/// Bytes the checksum does NOT cover: `len` and the checksum itself. Every
/// byte from here to the end is protected (torn-tail detection).
pub const UNCHECKED_PREFIX: usize = 4 + 8; // 12

/// The transaction envelope's own fixed part, present only when
/// `txn_kind == 1`: `gtid:u128 | n_participants:u16` (the participant array of
/// `8 * n` bytes follows it).
const TXN_ENVELOPE: usize = 16 + 2; // 18

/// The largest `len` (body length) a record header may declare. As in
/// `frame.rs` this is NOT the planner's entry cap; it is the bound that keeps a
/// damaged or hostile header from being believed far enough to drive an
/// allocation. It matches [`crate::rsm::segments::frame::MAX_FRAME_BODY_LEN`].
pub const MAX_RECORD_BODY: u32 = 256 * 1024 * 1024;

/// What a record's bytes can be wrong about. Every variant is a statement about
/// BYTES, never about semantics; the caller decides whether a given failure is
/// a torn tail it may truncate (§5) or corruption of a sealed file it must
/// surface.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordError {
    /// The buffer ends before the record the header describes.
    Truncated { need: usize, have: usize },
    /// `len` is below the fixed body or above [`MAX_RECORD_BODY`].
    BadLength(u32),
    /// A `txn_kind` this build does not know (0, 1 and [`REC_ENTRY`] exist).
    BadTxnKind(u8),
    /// The declared parts (`txn` envelope + `16 * count` hashes) do not fit in
    /// the `len` the header declares.
    Stride {
        count: u32,
        txn_bytes: usize,
        body: u32,
    },
    /// xxh3 of the record body does not match the header.
    Checksum { want: u64, got: u64 },
    /// The caller asked to encode a hash list that is not `16 * count`. Raised
    /// on the WRITE side.
    HashStride { count: u32, hashes: usize },
    /// The caller asked to encode more participants than the `u16` count can
    /// name.
    TooManyParticipants(usize),
}

impl std::fmt::Display for RecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordError::Truncated { need, have } => {
                write!(f, "record needs {need} bytes, buffer holds {have}")
            }
            RecordError::BadLength(l) => write!(f, "record length {l} is not believable"),
            RecordError::BadTxnKind(k) => write!(f, "record txn_kind {k} is not known"),
            RecordError::Stride {
                count,
                txn_bytes,
                body,
            } => write!(
                f,
                "count {count} + {txn_bytes} txn bytes need more than the {body} bytes of body"
            ),
            RecordError::Checksum { want, got } => {
                write!(f, "record checksum {want:#018x} != {got:#018x}")
            }
            RecordError::HashStride { count, hashes } => write!(
                f,
                "count {count} wants {} hash bytes, got {hashes}",
                *count as usize * HASH_LEN
            ),
            RecordError::TooManyParticipants(n) => {
                write!(f, "{n} participants do not fit a u16 count")
            }
        }
    }
}

impl std::error::Error for RecordError {}

impl From<RecordError> for std::io::Error {
    fn from(e: RecordError) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
    }
}

/// The fixed header of a record, parsed but NOT yet verified.
///
/// Everything here comes from bytes whose checksum has not been checked, so a
/// scanner may use [`Header::record_len`] to step to the next record and must
/// NOT believe `seq`, `pid`, `base_offset`, `count`, `created_at_us` or
/// `txn_kind` until [`verify`] has passed. The transaction envelope and the
/// hash/payload split are resolved only by [`decode`], on the whole verified
/// record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    /// Bytes after the `len` field: the whole record minus four.
    pub body_len: u32,
    pub checksum: u64,
    pub seq: u64,
    pub pid: u64,
    pub base_offset: u64,
    pub count: u32,
    pub created_at_us: i64,
    pub txn_kind: u8,
}

impl Header {
    /// The record kind (`txn_kind` without its flag bit).
    pub fn kind(&self) -> u8 {
        self.txn_kind & !FLAG_PAYLOAD_ZSTD
    }

    /// Whether the payload is stored zstd-compressed.
    pub fn payload_zstd(&self) -> bool {
        self.txn_kind & FLAG_PAYLOAD_ZSTD != 0
    }

    /// Total bytes of the record, `len` field included.
    pub fn record_len(&self) -> usize {
        4 + self.body_len as usize
    }

    /// Bytes of hash list: 16 per message, derived from `count`.
    pub fn hashes_len(&self) -> usize {
        self.count as usize * HASH_LEN
    }

    /// The exclusive end offset this record gives its partition:
    /// `base_offset + count`.
    pub fn end_offset(&self) -> u64 {
        self.base_offset.saturating_add(self.count as u64)
    }
}

/// The transaction envelope of a decoded record (`txn_kind == 1`). Reserved for
/// Phase B; `participants` is the raw little-endian `u64` array (queue ids).
#[derive(Clone, Copy, Debug)]
pub struct TxnRef<'a> {
    pub gtid: u128,
    participants: &'a [u8],
}

impl<'a> TxnRef<'a> {
    /// How many participant queues the transaction names.
    pub fn n(&self) -> usize {
        self.participants.len() / 8
    }

    /// The `i`-th participant queue id.
    pub fn participant(&self, i: usize) -> u64 {
        let at = i * 8;
        u64::from_le_bytes(self.participants[at..at + 8].try_into().expect("8 bytes"))
    }

    /// The participant queue ids as a vector.
    pub fn participants(&self) -> Vec<u64> {
        (0..self.n()).map(|i| self.participant(i)).collect()
    }
}

/// A record decoded in place: the hash list and the payload borrow the buffer.
#[derive(Clone, Copy, Debug)]
pub struct RecordRef<'a> {
    pub header: Header,
    pub txn: Option<TxnRef<'a>>,
    pub hashes: &'a [u8],
    pub payload: &'a [u8],
}

/// How many bytes the record for these fields will occupy on disk.
pub fn encoded_len(count: u32, n_participants: usize, payload_len: usize) -> usize {
    let txn = if n_participants == 0 {
        0
    } else {
        TXN_ENVELOPE + n_participants * 8
    };
    FIXED_PREFIX + txn + count as usize * HASH_LEN + payload_len
}

/// Append one record to `out` and return how many bytes it added.
///
/// `txn` is `Some((gtid, participants))` for a cross-queue transaction (Phase
/// B), `None` for an ordinary single-queue op. The checksum is computed over
/// the bytes this call just wrote, after they are in place, so there is exactly
/// one copy of the payload.
// The record's fields, positional like `frame.rs`'s `encode_into` (the same
// discipline: one write site per field). `seq` and `txn` push it past the
// argument-count lint the raft class allows here throughout.
#[allow(clippy::too_many_arguments)]
pub fn encode_into(
    out: &mut Vec<u8>,
    seq: u64,
    pid: u64,
    base_offset: u64,
    count: u32,
    created_at_us: i64,
    txn: Option<(u128, &[u64])>,
    hashes: &[u8],
    payload: &[u8],
) -> Result<usize, RecordError> {
    encode_msg_into(
        out,
        seq,
        pid,
        base_offset,
        count,
        created_at_us,
        txn,
        hashes,
        payload,
        false,
    )
}

/// [`encode_into`] with the [`FLAG_PAYLOAD_ZSTD`] bit: `payload_zstd` says the
/// `payload` bytes are already zstd-compressed.
#[allow(clippy::too_many_arguments)]
pub fn encode_msg_into(
    out: &mut Vec<u8>,
    seq: u64,
    pid: u64,
    base_offset: u64,
    count: u32,
    created_at_us: i64,
    txn: Option<(u128, &[u64])>,
    hashes: &[u8],
    payload: &[u8],
    payload_zstd: bool,
) -> Result<usize, RecordError> {
    if hashes.len() != count as usize * HASH_LEN {
        return Err(RecordError::HashStride {
            count,
            hashes: hashes.len(),
        });
    }
    let (txn_kind, txn_extra) = match txn {
        None => (TXN_NONE, 0usize),
        Some((_, parts)) => {
            if parts.len() > u16::MAX as usize {
                return Err(RecordError::TooManyParticipants(parts.len()));
            }
            (TXN_CROSS_QUEUE, TXN_ENVELOPE + parts.len() * 8)
        }
    };
    let body_len = FIXED_AFTER_LEN + txn_extra + hashes.len() + payload.len();
    if body_len > MAX_RECORD_BODY as usize {
        return Err(RecordError::BadLength(MAX_RECORD_BODY));
    }
    let start = out.len();
    out.reserve(4 + body_len);
    out.extend_from_slice(&(body_len as u32).to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes()); // checksum, filled in below
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&pid.to_le_bytes());
    out.extend_from_slice(&base_offset.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&created_at_us.to_le_bytes());
    out.push(if payload_zstd {
        txn_kind | FLAG_PAYLOAD_ZSTD
    } else {
        txn_kind
    });
    if let Some((gtid, parts)) = txn {
        out.extend_from_slice(&gtid.to_le_bytes());
        out.extend_from_slice(&(parts.len() as u16).to_le_bytes());
        for p in parts {
            out.extend_from_slice(&p.to_le_bytes());
        }
    }
    out.extend_from_slice(hashes);
    out.extend_from_slice(payload);
    let sum = xxh3_64(&out[start + UNCHECKED_PREFIX..]);
    out[start + 4..start + UNCHECKED_PREFIX].copy_from_slice(&sum.to_le_bytes());
    Ok(4 + body_len)
}

/// Append one payload-free ENTRY record ([`REC_ENTRY`]) to `out` and return how
/// many bytes it added. `now_us` is the entry's monotone clock (recovered from
/// `created_at_us`); `entry` is the serialized payload-free entry (its effects).
/// `pid`/`base_offset`/`count` are 0 — an entry record indexes no partition, and
/// carrying `count == 0` means no hash list, so `entry` is the whole tail. One
/// write of the entry bytes; the checksum covers everything after it, exactly as
/// a message record, so a torn entry record fails the same check and is truncated.
pub fn encode_entry_into(out: &mut Vec<u8>, seq: u64, now_us: i64, entry: &[u8]) -> usize {
    encode_entry_copies_into(out, seq, now_us, 1, entry)
}

/// [`encode_entry_into`] with the Phase C `copies` count in the `pid` slot: the
/// number of queue logs the writer wrote this same entry record to (see
/// [`REC_ENTRY`]). What the log writer uses.
pub fn encode_entry_copies_into(
    out: &mut Vec<u8>,
    seq: u64,
    now_us: i64,
    copies: u64,
    entry: &[u8],
) -> usize {
    encode_entry_record_into(out, seq, now_us, copies, 0, entry)
}

/// [`encode_entry_copies_into`] plus the Raft `term` of the entry, carried in
/// the `base_offset` slot (an entry record indexes no partition, so the slot is
/// otherwise always 0). The openraft log storage needs every entry's log id —
/// `(term, index)` — back at recovery; the local replicator writes `0`.
pub fn encode_entry_record_into(
    out: &mut Vec<u8>,
    seq: u64,
    now_us: i64,
    copies: u64,
    term: u64,
    entry: &[u8],
) -> usize {
    let body_len = FIXED_AFTER_LEN + entry.len();
    debug_assert!(
        body_len <= MAX_RECORD_BODY as usize,
        "entry record too large"
    );
    let start = out.len();
    out.reserve(4 + body_len);
    out.extend_from_slice(&(body_len as u32).to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes()); // checksum, filled in below
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&copies.to_le_bytes()); // pid slot = copies (Phase C)
    out.extend_from_slice(&term.to_le_bytes()); // base_offset slot = the Raft term
    out.extend_from_slice(&0u32.to_le_bytes()); // count
    out.extend_from_slice(&now_us.to_le_bytes()); // created_at carries now_us
    out.push(REC_ENTRY);
    out.extend_from_slice(entry);
    let sum = xxh3_64(&out[start + UNCHECKED_PREFIX..]);
    out[start + 4..start + UNCHECKED_PREFIX].copy_from_slice(&sum.to_le_bytes());
    4 + body_len
}

/// Parse the fixed header. The buffer may hold more or less than the whole
/// record; only [`FIXED_PREFIX`] bytes are read.
///
/// The checks here are the ones that MUST happen before any allocation: a
/// length below the fixed body, and a length past the cap. Both bound the only
/// allocation an untrusted header can drive — the read buffer sized by `len`
/// ([`Header::record_len`], at most `4 + MAX_RECORD_BODY`). The transaction
/// envelope and the `count`-fits check are resolved by [`decode`] on the bytes
/// this bound has already made safe to read, so `count` and `n_participants`
/// only ever index into an already-bounded buffer, never size an allocation.
pub fn parse_header(buf: &[u8]) -> Result<Header, RecordError> {
    if buf.len() < FIXED_PREFIX {
        return Err(RecordError::Truncated {
            need: FIXED_PREFIX,
            have: buf.len(),
        });
    }
    let body_len = u32::from_le_bytes(buf[0..4].try_into().expect("4 bytes"));
    if (body_len as usize) < FIXED_AFTER_LEN || body_len > MAX_RECORD_BODY {
        return Err(RecordError::BadLength(body_len));
    }
    Ok(Header {
        body_len,
        checksum: u64::from_le_bytes(buf[4..12].try_into().expect("8 bytes")),
        seq: u64::from_le_bytes(buf[12..20].try_into().expect("8 bytes")),
        pid: u64::from_le_bytes(buf[20..28].try_into().expect("8 bytes")),
        base_offset: u64::from_le_bytes(buf[28..36].try_into().expect("8 bytes")),
        count: u32::from_le_bytes(buf[36..40].try_into().expect("4 bytes")),
        created_at_us: i64::from_le_bytes(buf[40..48].try_into().expect("8 bytes")),
        txn_kind: buf[48],
    })
}

/// Verify a whole record's checksum. `frame` must be exactly the record.
pub fn verify(frame: &[u8], header: &Header) -> Result<(), RecordError> {
    if frame.len() != header.record_len() {
        return Err(RecordError::Truncated {
            need: header.record_len(),
            have: frame.len(),
        });
    }
    let got = xxh3_64(&frame[UNCHECKED_PREFIX..]);
    if got != header.checksum {
        return Err(RecordError::Checksum {
            want: header.checksum,
            got,
        });
    }
    Ok(())
}

/// PLAN_RAFT_DRAIN_FIX P3.2: how many leading bytes hold the header AND the hash
/// block of a record with no txn envelope — the only bytes a dedup read needs.
/// `hashes` sits right after the fixed prefix, before the payload.
pub fn hashes_prefix_len(count: u32) -> usize {
    FIXED_PREFIX + count as usize * HASH_LEN
}

/// PLAN_RAFT_DRAIN_FIX P3.2: split the hash block out of a record's leading
/// [`hashes_prefix_len`] bytes, WITHOUT the payload. `Ok(None)` when the record
/// carries a txn envelope (its hash block sits after a variable-length
/// participant array): the caller reads and [`decode`]s the whole record.
///
/// The checksum covers the payload too, so it CANNOT be verified from a prefix;
/// the caller must cross-check the header against the index record that located
/// it (position, `len`, `pid`, `base_offset`, `count`).
pub fn hashes_from_prefix(buf: &[u8]) -> Result<Option<(Header, &[u8])>, RecordError> {
    let header = parse_header(buf)?;
    match header.kind() {
        // REC_ENTRY carries count 0: an empty hash block.
        TXN_NONE | REC_ENTRY => {
            let end = FIXED_PREFIX + header.hashes_len();
            if end > header.record_len() {
                return Err(RecordError::Stride {
                    count: header.count,
                    txn_bytes: 0,
                    body: header.body_len,
                });
            }
            if buf.len() < end {
                return Err(RecordError::Truncated {
                    need: end,
                    have: buf.len(),
                });
            }
            Ok(Some((header, &buf[FIXED_PREFIX..end])))
        }
        TXN_CROSS_QUEUE => Ok(None),
        other => Err(RecordError::BadTxnKind(other)),
    }
}

/// Parse, verify and split one record. `buf` must start at the record; anything
/// past it is ignored. No caller ever sees bytes whose checksum has not
/// matched, and the transaction envelope, hash list and payload are split only
/// AFTER the checksum has passed — so a corrupt `txn_kind`, `n_participants` or
/// `count` is caught as a checksum failure, never acted on.
pub fn decode(buf: &[u8]) -> Result<RecordRef<'_>, RecordError> {
    let header = parse_header(buf)?;
    let total = header.record_len();
    if buf.len() < total {
        return Err(RecordError::Truncated {
            need: total,
            have: buf.len(),
        });
    }
    let frame = &buf[..total];
    verify(frame, &header)?;

    // The checksum has passed: every byte below is trustworthy. `off` walks the
    // variable tail. Each step is bounded against `total`, so a lie that
    // survived the checksum (it cannot) still could not index out of the frame.
    let mut off = FIXED_PREFIX;
    let txn = match header.kind() {
        // REC_ENTRY: a payload-free entry record (per-queue-only log of record).
        // No txn envelope; count is 0 so the hash list is empty and `payload`
        // below is the whole entry-bytes tail. It is NOT a message — the index
        // build and the pop/dedup read skip it (count 0, txn_kind 2).
        TXN_NONE | REC_ENTRY => None,
        TXN_CROSS_QUEUE => {
            if off + TXN_ENVELOPE > total {
                return Err(RecordError::Stride {
                    count: header.count,
                    txn_bytes: TXN_ENVELOPE,
                    body: header.body_len,
                });
            }
            let gtid = u128::from_le_bytes(frame[off..off + 16].try_into().expect("16 bytes"));
            let n =
                u16::from_le_bytes(frame[off + 16..off + 18].try_into().expect("2 bytes")) as usize;
            off += TXN_ENVELOPE;
            let parts_bytes = n * 8;
            if off + parts_bytes > total {
                return Err(RecordError::Stride {
                    count: header.count,
                    txn_bytes: TXN_ENVELOPE + parts_bytes,
                    body: header.body_len,
                });
            }
            let participants = &frame[off..off + parts_bytes];
            off += parts_bytes;
            Some(TxnRef { gtid, participants })
        }
        other => return Err(RecordError::BadTxnKind(other)),
    };

    let hashes_len = header.hashes_len();
    if off + hashes_len > total {
        return Err(RecordError::Stride {
            count: header.count,
            txn_bytes: off - FIXED_PREFIX,
            body: header.body_len,
        });
    }
    let hashes = &frame[off..off + hashes_len];
    let payload = &frame[off + hashes_len..];
    Ok(RecordRef {
        header,
        txn,
        hashes,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_entry_record_round_trips_and_is_not_a_message() {
        // A payload-free entry record (per-queue-only log of record): the entry
        // bytes go in the payload slot, count/pid/base are 0, txn_kind == REC_ENTRY.
        let entry = b"serialized-payload-free-entry-effects";
        let mut buf = Vec::new();
        let n = encode_entry_into(&mut buf, 4242, 1_700_000_000_000_000, entry);
        assert_eq!(n, buf.len());
        let rr = decode(&buf).expect("entry record decodes");
        assert_eq!(rr.header.txn_kind, REC_ENTRY);
        assert_eq!(rr.header.seq, 4242);
        assert_eq!(rr.header.created_at_us, 1_700_000_000_000_000);
        assert_eq!(rr.header.count, 0, "an entry record indexes no partition");
        assert_eq!(rr.header.base_offset, 0);
        assert!(rr.txn.is_none());
        assert!(rr.hashes.is_empty());
        assert_eq!(rr.payload, entry, "the entry bytes survive the round trip");
    }

    #[test]
    fn an_entry_record_carries_its_copies_in_the_pid_slot() {
        // Phase C: `copies` rides in the pid slot; base/count stay 0 so the
        // record still has no hash list and indexes nothing.
        let mut buf = Vec::new();
        let n = encode_entry_copies_into(&mut buf, 77, 5, 3, b"entry-bytes");
        assert_eq!(n, buf.len());
        let rr = decode(&buf).expect("decodes");
        assert_eq!(rr.header.txn_kind, REC_ENTRY);
        assert_eq!(rr.header.seq, 77);
        assert_eq!(rr.header.pid, 3, "copies");
        assert_eq!((rr.header.base_offset, rr.header.count), (0, 0));
        assert_eq!(rr.payload, b"entry-bytes");
        // The pre-copies encoder writes one copy.
        let mut one = Vec::new();
        encode_entry_into(&mut one, 77, 5, b"entry-bytes");
        assert_eq!(decode(&one).expect("decodes").header.pid, 1);
    }

    #[test]
    fn a_torn_entry_record_fails_the_checksum() {
        let mut buf = Vec::new();
        encode_entry_into(&mut buf, 7, 1, b"abcdefgh");
        *buf.last_mut().expect("nonempty") ^= 0xFF; // corrupt the last entry byte
        assert!(matches!(decode(&buf), Err(RecordError::Checksum { .. })));
    }

    #[test]
    fn a_message_record_still_round_trips_next_to_the_entry_kind() {
        // The new REC_ENTRY branch must not disturb ordinary message decoding.
        let hashes = vec![0xABu8; HASH_LEN]; // count == 1
        let mut buf = Vec::new();
        encode_into(&mut buf, 9, 3, 100, 1, 42, None, &hashes, b"hello").expect("encode msg");
        let rr = decode(&buf).expect("message decodes");
        assert_eq!(rr.header.txn_kind, TXN_NONE);
        assert_eq!(rr.header.count, 1);
        assert_eq!(rr.header.base_offset, 100);
        assert_eq!(rr.payload, b"hello");
    }
}
