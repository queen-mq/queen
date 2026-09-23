//! openraft's type configuration for Queen, the application entry openraft
//! carries, and the encoding of openraft's own entries in the queue logs.
//!
//! # Index numbering
//!
//! openraft numbers its log from 0; the RSM (apply, the store's
//! `applied_index`, the queue-log record `seq`, the batcher) numbers from 1,
//! with 0 meaning "nothing applied". The adapter keeps the two apart in one
//! place: RSM index = openraft index + 1 ([`rsm_index`]). Nothing outside
//! `replicator/raft` ever sees an openraft index.

use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

use serde::de::{Deserializer, SeqAccess, Visitor};
use serde::{Deserialize, Serialize, Serializer};

use crate::rsm::entry::{decode_entry, encode_entry, encode_entry_payload_free, Entry};
use crate::rsm::qlog::codec::Pre;

pub type NodeId = u64;
pub type Node = openraft::BasicNode;

openraft::declare_raft_types!(
    /// Queen's openraft types. The application data is one planned [`Entry`]
    /// (a batch of commands' effects); the response carries nothing (every
    /// outcome was decided by the planner and travels in the entry). Terms are
    /// `u64` and a term has at most one leader (the standard Raft leader id).
    pub TypeConfig:
        D = AppEntry,
        R = (),
        NodeId = NodeId,
        Node = Node,
        Term = u64,
        LeaderId = openraft::impls::leader_id_std::LeaderId<Self::Term, Self::NodeId>,
);

pub type LogId = openraft::alias::LogIdOf<TypeConfig>;
pub type Vote = openraft::alias::VoteOf<TypeConfig>;
pub type REntry = openraft::alias::EntryOf<TypeConfig>;
pub type StoredMembership = openraft::alias::StoredMembershipOf<TypeConfig>;
pub type Membership = openraft::Membership<NodeId, Node>;
pub type SnapshotMeta = openraft::alias::SnapshotMetaOf<TypeConfig>;

/// The RSM index of an openraft log index (see the module header).
pub fn rsm_index(raft_index: u64) -> u64 {
    raft_index + 1
}

/// An openraft log id from a term and an openraft index.
pub fn log_id(term: u64, raft_index: u64) -> LogId {
    LogId::new_term_index(term, raft_index)
}

/// The term of a log id.
pub fn term_of(id: &LogId) -> u64 {
    id.leader_id.term
}

/// The openraft log id of the entry the store last applied: `None` when the
/// store has applied nothing (`applied_index == 0`).
pub fn applied_log_id(applied_index: u64, applied_term: u64) -> Option<LogId> {
    (applied_index > 0).then(|| log_id(applied_term, applied_index - 1))
}

// ---------------------------------------------------------------------------
// The application entry
// ---------------------------------------------------------------------------

/// What openraft carries as application data: one [`Entry`].
///
/// Cloning is a reference count. The planned entry (with its payloads) is
/// `full`; the PAYLOAD-FREE form apply needs (each `Append` blob replaced by
/// its 4-byte frame length, the form a replay from the queue logs produces)
/// is set by the log writer once the group holding the entry is fsynced, or
/// at recovery. An entry read back from the queue logs has no `full` form:
/// its payloads live only in the message records.
#[derive(Clone)]
pub struct AppEntry(Arc<AppEntryInner>);

struct AppEntryInner {
    full: Option<Arc<Entry>>,
    payload_free: OnceLock<Arc<Entry>>,
    /// The qlog codec work started at propose (it overlaps the previous
    /// fsync); taken once, by the writer.
    pre: Mutex<Vec<Pre>>,
}

impl AppEntry {
    /// A proposal: the planned entry and the codec work already started on
    /// its payloads.
    pub fn proposed(full: Arc<Entry>, pre: Vec<Pre>) -> AppEntry {
        AppEntry(Arc::new(AppEntryInner {
            full: Some(full),
            payload_free: OnceLock::new(),
            pre: Mutex::new(pre),
        }))
    }

    /// An entry recovered from the queue logs: payload-free only.
    pub fn recovered(payload_free: Arc<Entry>) -> AppEntry {
        let pf = OnceLock::new();
        let _ = pf.set(payload_free);
        AppEntry(Arc::new(AppEntryInner {
            full: None,
            payload_free: pf,
            pre: Mutex::new(Vec::new()),
        }))
    }

    /// The entry with its payloads, when this node has it.
    pub fn full(&self) -> Option<&Arc<Entry>> {
        self.0.full.as_ref()
    }

    /// The codec work started at propose (empty the second time).
    pub fn take_pre(&self) -> Vec<Pre> {
        std::mem::take(&mut *self.0.pre.lock().expect("pre lock"))
    }

    /// Record the payload-free form (the writer, after the fsync).
    pub fn set_payload_free(&self, pf: Arc<Entry>) {
        let _ = self.0.payload_free.set(pf);
    }

    /// The payload-free form apply takes: the one the writer recorded, or,
    /// for an entry the writer never saw, derived from the full entry exactly
    /// as the writer derives it.
    pub fn payload_free(&self) -> std::io::Result<Arc<Entry>> {
        if let Some(pf) = self.0.payload_free.get() {
            return Ok(pf.clone());
        }
        let full = self.0.full.as_ref().ok_or_else(|| {
            std::io::Error::other("an application entry has neither a full nor a payload-free form")
        })?;
        let bytes = encode_entry_payload_free(full)
            .map_err(|e| std::io::Error::other(format!("payload-free encode: {e:?}")))?;
        let pf = Arc::new(
            decode_entry(&bytes)
                .map_err(|e| std::io::Error::other(format!("payload-free decode: {e:?}")))?,
        );
        let _ = self.0.payload_free.set(pf.clone());
        Ok(pf)
    }

    fn summary(&self) -> (usize, usize, bool) {
        let e = self.0.full.as_ref().or_else(|| self.0.payload_free.get());
        match e {
            Some(e) => (e.commands.len(), e.effects.len(), self.0.full.is_some()),
            None => (0, 0, false),
        }
    }
}

impl fmt::Debug for AppEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (c, e, full) = self.summary();
        write!(f, "AppEntry{{commands:{c}, effects:{e}, full:{full}}}")
    }
}

impl fmt::Display for AppEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (c, e, _) = self.summary();
        write!(f, "entry({c} commands, {e} effects)")
    }
}

/// On the wire an entry is its full encoded form. A payload-free entry can
/// never be sent: a follower would store 4-byte lengths as payloads. Refusing
/// here makes that a transport error, never silent data loss.
impl Serialize for AppEntry {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let full = self.0.full.as_ref().ok_or_else(|| {
            serde::ser::Error::custom("a payload-free entry cannot be replicated")
        })?;
        let bytes = encode_entry(full).map_err(|e| serde::ser::Error::custom(format!("{e:?}")))?;
        s.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for AppEntry {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<AppEntry, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Vec<u8>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an encoded entry")
            }
            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Vec<u8>, E> {
                Ok(v.to_vec())
            }
            fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<Vec<u8>, E> {
                Ok(v)
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
                let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(b) = seq.next_element::<u8>()? {
                    out.push(b);
                }
                Ok(out)
            }
        }
        let bytes = d.deserialize_bytes(V)?;
        let e = decode_entry(&bytes).map_err(|e| serde::de::Error::custom(format!("{e:?}")))?;
        Ok(AppEntry::proposed(Arc::new(e), Vec::new()))
    }
}

// ---------------------------------------------------------------------------
// openraft's own entries in the queue logs
// ---------------------------------------------------------------------------

/// First four bytes of a consensus-internal entry record. An RSM entry starts
/// with its body length, which the codec caps far below `u32::MAX`, so the
/// two can never be confused.
pub const INTERNAL_MAGIC: u32 = u32::MAX;
/// A leader's blank entry.
pub const INTERNAL_BLANK: u8 = 1;
/// A membership entry; the payload is the membership as JSON.
pub const INTERNAL_MEMBERSHIP: u8 = 2;

/// The record body of a consensus-internal entry.
pub fn encode_internal(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.extend_from_slice(&INTERNAL_MAGIC.to_le_bytes());
    out.push(kind);
    out.extend_from_slice(payload);
    out
}

/// `Some((kind, payload))` when `bytes` is a consensus-internal record body.
pub fn decode_internal(bytes: &[u8]) -> Option<(u8, &[u8])> {
    if bytes.len() >= 5 && bytes[..4] == INTERNAL_MAGIC.to_le_bytes() {
        Some((bytes[4], &bytes[5..]))
    } else {
        None
    }
}
