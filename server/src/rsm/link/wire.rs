//! What a standby asks its source and what it gets back.
//!
//! One call: "the application entries after this one". The standby names the
//! last source entry it applied by index AND term; the source checks that its
//! own log holds that entry (raft log matching: one index and term, one entry
//! of one log) before it answers with what follows, so a standby that replayed
//! another log is told so instead of being fed entries that do not continue
//! its state.
//!
//! # The request
//!
//! JSON, small and read by people in a capture:
//!
//! ```json
//! {"after":41233,"afterTerm":7,"maxBytes":8388608,"waitMs":2000,"reader":"standby-5e1f09c2"}
//! ```
//!
//! With `"holdOnly":true` the source reads nothing and only notes how far the
//! standby has got ([`Request::hold_only`]).
//!
//! # The answer
//!
//! Binary, little-endian (the entries are megabytes of payload):
//!
//! ```text
//! "QLNK" | version:u8 | status:u8 | applied:u64 | purged:u64 | a:u64 | b:u64
//!        | n:u32 | n × ( index:u64 | term:u64 | len:u32 | entry[len] )
//! ```
//!
//! - `status` 0, [`Answer::Entries`]: `n` application entries above `after`,
//!   ascending. Consensus-internal entries are not among them, so the indexes
//!   have gaps; `a` is `upto`, the last index the answer covers, internal
//!   entries included. `applied` is how far the answering node has applied,
//!   so `applied - upto` is what the standby has still to read — zero for a
//!   standby that holds everything, whatever internal entries end the
//!   source's log.
//! - `status` 1, [`Answer::Purged`]: the source's log starts above `after`
//!   (`purged` is the last index it dropped). The standby needs a new seed.
//! - `status` 2, [`Answer::Mismatch`]: the source's entry at `after` (`a`) has
//!   term `b`, not the standby's (`u64::MAX`: the source's log does not reach
//!   it). The standby followed another log.
//!
//! An entry travels in the STORED form a follower of the source receives
//! ([`crate::rsm::replicator::raft`]'s wire): the payload-free entry and every
//! `Append` payload as the queue logs hold it, compressed or raw. The source
//! sends what it reads, with no encode and no codec; [`full_entry`] rebuilds
//! the planned entry on the standby.

use std::io;

use bytes::Bytes;

use crate::rsm::effect::{Effect, Reader, Writer};
use crate::rsm::entry::{decode_entry, Entry};

/// The route of a source node's Raft RPC server that a standby reads.
pub const READ_PATH: &str = "/link/v1/read";
/// The route that streams a source node's snapshot to a standby's seed
/// ([`super::seed`]).
pub const SNAPSHOT_PATH: &str = "/link/v1/snapshot";
/// The header that carries the link's token: the source's `QUEEN_LINK_TOKEN`,
/// which a standby holds as `QUEEN_LINK_SOURCE_TOKEN`. Not the source's
/// cluster token: it lets a standby read, and nothing else.
pub const TOKEN_HEADER: &str = "x-queen-link-token";

/// The most a source reads into one answer unless the standby asks for less.
/// One entry is always sent whole, however large.
pub const MAX_BYTES_DEFAULT: usize = 8 << 20;
/// The longest a source holds a call that has nothing to send.
pub const WAIT_MS_MAX: u64 = 30_000;

/// The standby's call.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Request {
    /// The source index of the last entry the standby applied (0: none).
    pub after: u64,
    /// Its term in the source's log (0 with `after` 0).
    #[serde(rename = "afterTerm")]
    pub after_term: u64,
    /// Stop adding entries to the answer past this many bytes.
    #[serde(rename = "maxBytes", default)]
    pub max_bytes: u64,
    /// With nothing to send, wait this long for the source to apply more.
    #[serde(rename = "waitMs", default)]
    pub wait_ms: u64,
    /// Who asks: a name for the source's log and its hold on the entries this
    /// standby still needs.
    #[serde(default)]
    pub reader: String,
    /// Read nothing: only tell this node how far the standby has got, so it
    /// keeps the entries after that. A standby reads ONE source node and
    /// tells the others this way, so any of them can take over the reads.
    #[serde(rename = "holdOnly", default, skip_serializing_if = "std::ops::Not::not")]
    pub hold_only: bool,
}

/// One application entry of the source's log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceEntry {
    pub index: u64,
    pub term: u64,
    /// The entry in stored form ([`full_entry`] reads it).
    pub stored: Bytes,
}

/// The source's answer. See the module header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    Entries {
        entries: Vec<SourceEntry>,
        /// Every application entry at or below this index is in `entries` or
        /// was read before.
        upto: u64,
        applied: u64,
        purged: u64,
    },
    Purged {
        purged: u64,
        applied: u64,
    },
    Mismatch {
        index: u64,
        /// `None`: the source's log does not reach `index`.
        source_term: Option<u64>,
        applied: u64,
        purged: u64,
    },
}

const MAGIC: &[u8; 4] = b"QLNK";
const VERSION: u8 = 1;
const STATUS_ENTRIES: u8 = 0;
const STATUS_PURGED: u8 = 1;
const STATUS_MISMATCH: u8 = 2;

fn bad(what: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.into())
}

impl Answer {
    pub fn encode(&self) -> Vec<u8> {
        let (status, applied, purged, a, b, entries): (u8, u64, u64, u64, u64, &[SourceEntry]) =
            match self {
                Answer::Entries {
                    entries,
                    upto,
                    applied,
                    purged,
                } => (STATUS_ENTRIES, *applied, *purged, *upto, 0, entries),
                Answer::Purged { purged, applied } => (STATUS_PURGED, *applied, *purged, 0, 0, &[]),
                Answer::Mismatch {
                    index,
                    source_term,
                    applied,
                    purged,
                } => (
                    STATUS_MISMATCH,
                    *applied,
                    *purged,
                    *index,
                    source_term.unwrap_or(u64::MAX),
                    &[],
                ),
            };
        let size: usize = entries.iter().map(|e| 20 + e.stored.len()).sum();
        let mut w = Writer::with_capacity(4 + 2 + 32 + 4 + size);
        for byte in MAGIC {
            w.u8(*byte);
        }
        w.u8(VERSION);
        w.u8(status);
        w.u64(applied);
        w.u64(purged);
        w.u64(a);
        w.u64(b);
        w.u32(entries.len() as u32);
        for e in entries {
            w.u64(e.index);
            w.u64(e.term);
            w.blob(&e.stored);
        }
        w.into_inner()
    }

    /// Read an answer. The entries are slices of `bytes` (no copy).
    pub fn decode(bytes: &Bytes) -> io::Result<Answer> {
        let field = |e| bad(format!("link answer: {e:?}"));
        let mut r = Reader::new(bytes);
        for byte in MAGIC {
            if r.u8("magic").map_err(field)? != *byte {
                return Err(bad("link answer: not a link answer"));
            }
        }
        let version = r.u8("version").map_err(field)?;
        if version != VERSION {
            return Err(bad(format!(
                "link answer: version {version}, and this build reads {VERSION}"
            )));
        }
        let status = r.u8("status").map_err(field)?;
        let applied = r.u64("applied").map_err(field)?;
        let purged = r.u64("purged").map_err(field)?;
        let a = r.u64("a").map_err(field)?;
        let b = r.u64("b").map_err(field)?;
        let n = r.u32("count").map_err(field)? as usize;
        let mut entries = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            let index = r.u64("entry index").map_err(field)?;
            let term = r.u64("entry term").map_err(field)?;
            let len = r.u32("entry len").map_err(field)? as usize;
            if len > r.remaining() {
                return Err(bad("link answer: an entry runs past the answer"));
            }
            let at = bytes.len() - r.remaining();
            r.skip(len, "entry").map_err(field)?;
            entries.push(SourceEntry {
                index,
                term,
                stored: bytes.slice(at..at + len),
            });
        }
        if !r.done() {
            return Err(bad("link answer: trailing bytes"));
        }
        match status {
            STATUS_ENTRIES => Ok(Answer::Entries {
                entries,
                upto: a,
                applied,
                purged,
            }),
            STATUS_PURGED if entries.is_empty() => Ok(Answer::Purged { purged, applied }),
            STATUS_MISMATCH if entries.is_empty() => Ok(Answer::Mismatch {
                index: a,
                source_term: (b != u64::MAX).then_some(b),
                applied,
                purged,
            }),
            other => Err(bad(format!("link answer: status {other}"))),
        }
    }
}

/// The planned entry of a stored-form entry:
///
/// ```text
/// pf_len:u32 | payload-free entry | n:u32 | n × ( zstd:u8 | len:u32 | bytes )
/// ```
///
/// The payload-free entry is decoded (its checksum and every field checked),
/// and each `Append` takes its payload back, decompressed when it was stored
/// compressed. A payload-free `Append` carries the length of the frame its
/// payload makes; a payload that does not make that length is refused, so a
/// payload attached to the wrong append is an error here and never a message
/// on the standby.
pub fn full_entry(stored: &[u8]) -> io::Result<Entry> {
    /// The bytes still to read.
    struct Rest<'a>(&'a [u8]);
    impl<'a> Rest<'a> {
        fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
            if n > self.0.len() {
                return Err(bad("stored entry truncated"));
            }
            let (head, tail) = self.0.split_at(n);
            self.0 = tail;
            Ok(head)
        }
        fn u32(&mut self) -> io::Result<usize> {
            let b = self.take(4)?;
            Ok(u32::from_le_bytes(b.try_into().expect("4 bytes")) as usize)
        }
    }
    let mut rest = Rest(stored);

    let pf_len = rest.u32()?;
    let mut entry = decode_entry(rest.take(pf_len)?)
        .map_err(|e| bad(format!("stored entry does not decode: {e:?}")))?;
    let n = rest.u32()?;
    let appends = entry
        .effects
        .iter()
        .filter(|e| matches!(e, Effect::Append { .. }))
        .count();
    if n != appends {
        return Err(bad(format!(
            "stored entry: {n} payloads for {appends} appends"
        )));
    }
    for eff in entry.effects.iter_mut() {
        let Effect::Append { count, blob, .. } = eff else {
            continue;
        };
        let zstd = match rest.take(1)?[0] {
            0 => false,
            1 => true,
            other => return Err(bad(format!("stored entry: payload flag {other}"))),
        };
        let len = rest.u32()?;
        let bytes = rest.take(len)?;
        let payload = if zstd {
            crate::rsm::qlog::codec::decompress(bytes)?
        } else {
            bytes.to_vec()
        };
        let want = match <[u8; 4]>::try_from(blob.as_slice()) {
            Ok(b) => u32::from_le_bytes(b) as usize,
            Err(_) => return Err(bad("stored entry: an append is not payload-free")),
        };
        if crate::rsm::segments::frame::encoded_len(*count, payload.len()) != want {
            return Err(bad(
                "stored entry: a payload is not the length its append names",
            ));
        }
        *blob = payload;
    }
    if !rest.0.is_empty() {
        return Err(bad("stored entry has trailing bytes"));
    }
    Ok(entry)
}

/// What a rebuilt entry weighs in memory, near enough: its payloads, and a
/// little for each effect.
pub fn entry_weight(e: &Entry) -> usize {
    e.effects
        .iter()
        .map(|eff| match eff {
            Effect::Append { blob, .. } => 64 + blob.len(),
            _ => 64,
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64, len: usize) -> SourceEntry {
        SourceEntry {
            index,
            term: 3,
            stored: Bytes::from(vec![index as u8; len]),
        }
    }

    #[test]
    fn every_answer_round_trips() {
        for a in [
            Answer::Entries {
                entries: vec![entry(5, 0), entry(9, 300), entry(10, 1)],
                upto: 11,
                applied: 12,
                purged: 2,
            },
            Answer::Entries {
                entries: Vec::new(),
                upto: 4,
                applied: 4,
                purged: 0,
            },
            Answer::Purged {
                purged: 900,
                applied: 1_000,
            },
            Answer::Mismatch {
                index: 41,
                source_term: Some(6),
                applied: 50,
                purged: 3,
            },
            Answer::Mismatch {
                index: 41,
                source_term: None,
                applied: 30,
                purged: 0,
            },
        ] {
            let bytes = Bytes::from(a.encode());
            assert_eq!(Answer::decode(&bytes).expect("decode"), a);
        }
    }

    #[test]
    fn a_damaged_answer_is_an_error() {
        let whole = Answer::Entries {
            entries: vec![entry(5, 64)],
            upto: 5,
            applied: 5,
            purged: 0,
        }
        .encode();
        for cut in [0, 3, 5, 20, whole.len() - 1] {
            assert!(
                Answer::decode(&Bytes::copy_from_slice(&whole[..cut])).is_err(),
                "an answer cut at {cut} was read"
            );
        }
        let mut other = whole.clone();
        other[4] = VERSION + 1;
        assert!(Answer::decode(&Bytes::from(other)).is_err());
        let mut longer = whole;
        longer.push(0);
        assert!(Answer::decode(&Bytes::from(longer)).is_err());
    }
}
