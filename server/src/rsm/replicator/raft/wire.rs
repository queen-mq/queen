//! The Raft RPC wire format.
//!
//! AppendEntries carries its entries as raw bytes — an application entry is
//! the RSM entry codec's encoding, which has its own length and checksum —
//! behind a small JSON header:
//!
//! ```text
//! version:u8 | header_len:u32 | header JSON {vote, prev_log_id, leader_commit, n}
//! n × ( term:u64 | index:u64 | kind:u8 | len:u32 | bytes )
//! ```
//!
//! `kind` is 0 for an application entry, 1 for a blank entry, 2 for a
//! membership entry (its bytes are the membership as JSON). Every integer is
//! little-endian. Every other message is JSON: they are small.

use std::io;

use bytes::Bytes;
use openraft::raft::AppendEntriesRequest;
use openraft::EntryPayload;
use serde::{Deserialize, Serialize};

use super::types::{log_id, term_of, AppEntry, LogId, Membership, REntry, TypeConfig, Vote};

const VERSION: u8 = 1;
const KIND_APP: u8 = 0;
const KIND_BLANK: u8 = 1;
const KIND_MEMBERSHIP: u8 = 2;

/// The largest AppendEntries body the sender builds. A request over it is cut
/// at an entry boundary (at least one entry always goes) and the rest follows
/// in the next request.
pub const MAX_APPEND_BYTES: usize = 32 * 1024 * 1024;

#[derive(Serialize)]
struct HeaderRef<'a> {
    vote: &'a Vote,
    prev_log_id: &'a Option<LogId>,
    leader_commit: &'a Option<LogId>,
    n: usize,
}

#[derive(Deserialize)]
struct Header {
    vote: Vote,
    prev_log_id: Option<LogId>,
    leader_commit: Option<LogId>,
    n: usize,
}

fn bad(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Encode `req`, keeping at most [`MAX_APPEND_BYTES`] of entries (never fewer
/// than one). Returns the bytes and how many entries they carry.
pub fn encode_append(req: &AppendEntriesRequest<TypeConfig>) -> io::Result<(Vec<u8>, usize)> {
    let mut parts: Vec<(u64, u64, u8, Bytes)> = Vec::with_capacity(req.entries.len());
    let mut total = 0usize;
    for e in &req.entries {
        let (kind, bytes) = match &e.payload {
            EntryPayload::Normal(app) => (KIND_APP, app.wire()?),
            EntryPayload::Blank => (KIND_BLANK, Bytes::new()),
            EntryPayload::Membership(m) => (
                KIND_MEMBERSHIP,
                Bytes::from(serde_json::to_vec(m).map_err(io::Error::other)?),
            ),
        };
        let size = 21 + bytes.len();
        if !parts.is_empty() && total + size > MAX_APPEND_BYTES {
            break;
        }
        total += size;
        parts.push((term_of(&e.log_id), e.log_id.index, kind, bytes));
    }
    let n = parts.len();
    let header = serde_json::to_vec(&HeaderRef {
        vote: &req.vote,
        prev_log_id: &req.prev_log_id,
        leader_commit: &req.leader_commit,
        n,
    })
    .map_err(io::Error::other)?;
    let mut out = Vec::with_capacity(5 + header.len() + total);
    out.push(VERSION);
    out.extend_from_slice(&(header.len() as u32).to_le_bytes());
    out.extend_from_slice(&header);
    for (term, index, kind, bytes) in parts {
        out.extend_from_slice(&term.to_le_bytes());
        out.extend_from_slice(&index.to_le_bytes());
        out.push(kind);
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&bytes);
    }
    Ok((out, n))
}

struct Cursor<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(n)
            .filter(|e| *e <= self.b.len())
            .ok_or_else(|| bad("append request truncated"))?;
        let s = &self.b[self.at..end];
        self.at = end;
        Ok(s)
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }
}

/// Decode an AppendEntries body. Every application entry is decoded by the RSM
/// codec, which verifies its checksum and fields.
pub fn decode_append(b: &[u8]) -> io::Result<AppendEntriesRequest<TypeConfig>> {
    let mut c = Cursor { b, at: 0 };
    let v = c.u8()?;
    if v != VERSION {
        return Err(bad(format!(
            "append request version {v}, expected {VERSION}"
        )));
    }
    let hlen = c.u32()? as usize;
    let h: Header = serde_json::from_slice(c.take(hlen)?)
        .map_err(|e| bad(format!("append request header: {e}")))?;
    let mut entries: Vec<REntry> = Vec::with_capacity(h.n.min(1 << 16));
    for _ in 0..h.n {
        let term = c.u64()?;
        let index = c.u64()?;
        let kind = c.u8()?;
        let len = c.u32()? as usize;
        let bytes = c.take(len)?;
        let payload = match kind {
            KIND_APP => EntryPayload::Normal(AppEntry::from_wire(bytes)?),
            KIND_BLANK => EntryPayload::Blank,
            KIND_MEMBERSHIP => {
                let m: Membership = serde_json::from_slice(bytes)
                    .map_err(|e| bad(format!("membership entry {index}: {e}")))?;
                EntryPayload::Membership(m)
            }
            k => return Err(bad(format!("append entry {index}: unknown kind {k}"))),
        };
        entries.push(REntry {
            log_id: log_id(term, index),
            payload,
        });
    }
    if c.at != b.len() {
        return Err(bad("append request has trailing bytes"));
    }
    Ok(AppendEntriesRequest {
        vote: h.vote,
        prev_log_id: h.prev_log_id,
        entries,
        leader_commit: h.leader_commit,
    })
}
