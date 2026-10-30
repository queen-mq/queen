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

use bytes::Bytes;

use crate::rsm::entry::{decode_entry, encode_entry_payload_free, Entry};
use crate::rsm::qlog::codec::{Pre, StoredPayload};

pub type NodeId = u64;
pub type Node = QueenNode;

/// A cluster member's two addresses. The Raft RPCs go to `raft`; a follower
/// forwards the client requests it receives to the leader's `http`.
///
/// `addr` is accepted for `raft` so a membership written as openraft's
/// `BasicNode` (the single-voter replicator before the cluster step) still
/// reads.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueenNode {
    /// `host:port` of the node's Raft RPC listener.
    #[serde(default, alias = "addr")]
    pub raft: String,
    /// `host:port` of the node's client API.
    #[serde(default)]
    pub http: String,
}

impl QueenNode {
    pub fn new(raft: impl Into<String>, http: impl Into<String>) -> QueenNode {
        QueenNode {
            raft: raft.into(),
            http: http.into(),
        }
    }
}

impl fmt::Display for QueenNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "raft={} http={}", self.raft, self.http)
    }
}

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
/// Cloning is a reference count. An entry has one of two shapes:
///
/// - **full**: the planned entry with its payloads, on the leader that
///   proposed it. Before it reaches openraft the proposer awaits the qlog codec
///   on its `Append` blobs ([`AppEntry::settle_codec`]), so the leader's writer
///   and every follower get the same stored bytes and nobody compresses twice.
/// - **stored**: the PAYLOAD-FREE entry, its encoding (the queue logs' entry
///   record) and every `Append` payload exactly as the leader stored it
///   (compressed or raw) — what a follower receives ([`AppEntry::from_stored_wire`])
///   and what a node reads back from its queue logs to send to a follower that
///   is behind. Written as it is: no encode, no codec.
///
/// The payload-free form apply takes is set by the log writer once the group
/// holding the entry is fsynced, or at construction for a stored entry. An
/// entry recovered from the queue logs at boot has only that form.
#[derive(Clone)]
pub struct AppEntry(Arc<AppEntryInner>);

struct AppEntryInner {
    full: Option<Arc<Entry>>,
    payload_free: OnceLock<Arc<Entry>>,
    /// The payload-free encoding, once known (a stored entry has it from the
    /// start; a full entry's is made once, for the wire).
    pf_bytes: OnceLock<Bytes>,
    /// The qlog codec work started at propose (it overlaps the previous
    /// fsync); taken once, by [`AppEntry::settle_codec`] or the writer.
    pre: Mutex<Vec<Pre>>,
    /// A full entry's `Append` blobs in stored form, in effect order (`None` =
    /// stored raw): set by [`AppEntry::settle_codec`].
    z: OnceLock<Arc<Vec<Option<Bytes>>>>,
    /// A stored entry's `Append` payloads, in effect order.
    stored: Option<Arc<Vec<StoredPayload>>>,
    /// When the leader proposed it (`None` on a follower): the apply path's
    /// stage timings start here.
    proposed_at: Option<std::time::Instant>,
    /// The wire encoding (stored form), made once and shared by every follower
    /// the entry is sent to (and every resend).
    wire: OnceLock<Bytes>,
    /// The wire encoding as one zstd frame, made once for every follower that
    /// reads it ([`AppEntry::wire_zstd`]); `Some(None)` when compressing it
    /// saves too little.
    wire_z: tokio::sync::OnceCell<Option<Bytes>>,
    /// [`AppEntry::mem_bytes`], fixed at its first call: the raft cache adds
    /// it at append and takes it away at evict, so it must not change in
    /// between as the encoded forms appear.
    mem: OnceLock<usize>,
}

/// What one effect holds in memory: its variable-length fields, plus a fixed
/// 64 B for the rest. Every body counts — a KV value, a streams state, a dead
/// letter's payload, a timer's frame, a trace's data — not only `Append`'s.
pub(crate) fn effect_mem_bytes(eff: &crate::rsm::effect::Effect) -> usize {
    use crate::rsm::effect::Effect;
    64 + match eff {
        Effect::Append { blob, hashes, .. } => blob.len() + hashes.len(),
        Effect::KvPut {
            tenant,
            ns,
            key,
            value,
            ..
        } => tenant.len() + ns.len() + key.len() + value.len(),
        Effect::KvDelete { tenant, ns, key } => tenant.len() + ns.len() + key.len(),
        Effect::StreamsStatePut { key, value, .. } => key.len() + value.len(),
        Effect::StreamsStateDelete { key, .. } => key.len(),
        Effect::DlqInsert {
            tenant,
            queue,
            group,
            txn,
            payload,
            error,
            ..
        } => tenant.len() + queue.len() + group.len() + txn.len() + payload.len() + error.len(),
        Effect::DlqDelete { tenant, queue, .. } => tenant.len() + queue.len(),
        Effect::TimerUpsert {
            tenant,
            queue,
            key,
            row,
        } => {
            tenant.len()
                + queue.len()
                + key.len()
                + row.frame.len()
                + row.partition.len()
                + row.txn.len()
                + row.last_error.as_ref().map_or(0, |e| e.len())
                + row.producer_sub.as_ref().map_or(0, |p| p.len())
        }
        Effect::TimerDelete { tenant, queue, key } => tenant.len() + queue.len() + key.len(),
        Effect::TraceAppend { event } | Effect::TraceRecord { event } => {
            event.tenant.len()
                + event.txn.len()
                + event.event_type.len()
                + event.data.len()
                + event.consumer_group.as_ref().map_or(0, |g| g.len())
                + event.worker.as_ref().map_or(0, |w| w.len())
                + event.names.iter().map(|n| n.len() + 24).sum::<usize>()
        }
        Effect::FlagSet { key, value } => key.len() + value.len(),
        Effect::EphemeralConfigSet {
            tenant,
            queue,
            options,
            ..
        } => tenant.len() + queue.len() + options.len(),
        Effect::GarbageAdd { pids, .. } => pids.len() * 8,
        Effect::DeleteChunk { pids, resume, .. } => pids.len() * 8 + resume.len(),
        Effect::CursorSet { group, row, .. } => {
            group.len()
                + row.metadata.len()
                + row.delivered.len() * 16
                + row.worker.as_ref().map_or(0, |w| w.len())
        }
        Effect::QueueUpsert { tenant, queue, .. } | Effect::QueueDelete { tenant, queue } => {
            tenant.len() + queue.len()
        }
        Effect::PartitionCreate {
            tenant,
            queue,
            partition,
            ..
        } => tenant.len() + queue.len() + partition.len(),
        _ => 0,
    }
}

fn appends_of(e: &Entry) -> usize {
    e.effects
        .iter()
        .filter(|eff| matches!(eff, crate::rsm::effect::Effect::Append { .. }))
        .count()
}

impl AppEntry {
    fn with(
        full: Option<Arc<Entry>>,
        pf: Option<Arc<Entry>>,
        pf_bytes: Option<Bytes>,
        pre: Vec<Pre>,
        stored: Option<Arc<Vec<StoredPayload>>>,
        wire: Option<Bytes>,
    ) -> AppEntry {
        let payload_free = OnceLock::new();
        if let Some(pf) = pf {
            let _ = payload_free.set(pf);
        }
        let pfb = OnceLock::new();
        if let Some(b) = pf_bytes {
            let _ = pfb.set(b);
        }
        let w = OnceLock::new();
        if let Some(b) = wire {
            let _ = w.set(b);
        }
        let proposed_at = full.is_some().then(std::time::Instant::now);
        AppEntry(Arc::new(AppEntryInner {
            proposed_at,
            full,
            payload_free,
            pf_bytes: pfb,
            pre: Mutex::new(pre),
            z: OnceLock::new(),
            stored,
            wire: w,
            wire_z: tokio::sync::OnceCell::new(),
            mem: OnceLock::new(),
        }))
    }

    /// A proposal: the planned entry and the codec work already started on
    /// its payloads.
    pub fn proposed(full: Arc<Entry>, pre: Vec<Pre>) -> AppEntry {
        AppEntry::with(Some(full), None, None, pre, None, None)
    }

    /// An entry recovered from the queue logs: payload-free only.
    pub fn recovered(payload_free: Arc<Entry>) -> AppEntry {
        AppEntry::with(None, Some(payload_free), None, Vec::new(), None, None)
    }

    /// An entry in stored form: its payload-free decode and encoding, and every
    /// `Append` payload as stored. `wire`, when given, is its wire encoding.
    pub fn stored(
        payload_free: Arc<Entry>,
        pf_bytes: Bytes,
        payloads: Vec<StoredPayload>,
        wire: Option<Bytes>,
    ) -> AppEntry {
        AppEntry::with(
            None,
            Some(payload_free),
            Some(pf_bytes),
            Vec::new(),
            Some(Arc::new(payloads)),
            wire,
        )
    }

    /// An entry received from the leader in the LEGACY wire form (the full
    /// entry, raw payloads): its bytes decoded (the codec checks the checksum
    /// and every field).
    pub fn from_wire(bytes: &[u8]) -> std::io::Result<AppEntry> {
        let e = decode_entry(bytes).map_err(|e| {
            std::io::Error::other(format!("replicated entry does not decode: {e:?}"))
        })?;
        Ok(AppEntry::proposed(Arc::new(e), Vec::new()))
    }

    /// An entry received from the leader in stored form:
    ///
    /// ```text
    /// pf_len:u32 | payload-free entry | n:u32 | n × ( zstd:u8 | len:u32 | bytes )
    /// ```
    ///
    /// The payloads are slices of `bytes` (no copy). The payload-free entry is
    /// decoded (checksum and fields checked) and must hold exactly `n` appends.
    pub fn from_stored_wire(bytes: Bytes) -> std::io::Result<AppEntry> {
        let bad = |m: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, m.to_string());
        let mut at = 0usize;
        let take = |at: &mut usize, n: usize| -> std::io::Result<std::ops::Range<usize>> {
            let end = at
                .checked_add(n)
                .filter(|e| *e <= bytes.len())
                .ok_or_else(|| bad("stored entry truncated"))?;
            let r = *at..end;
            *at = end;
            Ok(r)
        };
        let u32_at = |at: &mut usize| -> std::io::Result<u32> {
            let r = take(at, 4)?;
            Ok(u32::from_le_bytes(bytes[r].try_into().expect("4 bytes")))
        };
        let pf_len = u32_at(&mut at)? as usize;
        let pf_bytes = bytes.slice(take(&mut at, pf_len)?);
        let pf = decode_entry(&pf_bytes).map_err(|e| {
            std::io::Error::other(format!("replicated entry does not decode: {e:?}"))
        })?;
        let n = u32_at(&mut at)? as usize;
        if n != appends_of(&pf) {
            return Err(bad("stored entry: payload count differs from its appends"));
        }
        let mut payloads = Vec::with_capacity(n);
        for _ in 0..n {
            let r = take(&mut at, 1)?;
            let zstd = match bytes[r.start] {
                0 => false,
                1 => true,
                _ => return Err(bad("stored entry: bad payload flag")),
            };
            let len = u32_at(&mut at)? as usize;
            payloads.push(StoredPayload {
                zstd,
                bytes: bytes.slice(take(&mut at, len)?),
            });
        }
        if at != bytes.len() {
            return Err(bad("stored entry has trailing bytes"));
        }
        Ok(AppEntry::stored(
            Arc::new(pf),
            pf_bytes,
            payloads,
            Some(bytes),
        ))
    }

    /// An entry received from the leader as ONE zstd frame of its stored-form
    /// wire bytes ([`AppEntry::wire_zstd`]): decompressed (the frame's declared
    /// size is bounded by the codec), then read as [`AppEntry::from_stored_wire`].
    /// The frame is kept: if this node leads next, it sends the same bytes.
    pub fn from_stored_wire_zstd(z: Bytes) -> std::io::Result<AppEntry> {
        let raw = crate::rsm::qlog::codec::decompress(&z)?;
        let e = AppEntry::from_stored_wire(Bytes::from(raw))?;
        let _ = e.0.wire_z.set(Some(z));
        Ok(e)
    }

    /// The wire bytes ([`AppEntry::wire`]) as one zstd frame at level 1, or
    /// `None` when that saves under a tenth. Made ONCE per entry — on the
    /// blocking pool, never on a raft runtime thread — and shared by every
    /// follower's append and every resend; a concurrent caller waits for the
    /// first. The leader's payloads under the disk codec's floor go raw into
    /// the queue logs and so, before this, raw onto the wire: at 1-20 messages
    /// of ~300 B per push, ~2.4x the bytes per message.
    pub async fn wire_zstd(&self) -> std::io::Result<Option<Bytes>> {
        let entry = self.clone();
        self.0
            .wire_z
            .get_or_try_init(|| async move {
                let raw = entry.wire()?;
                tokio::task::spawn_blocking(move || {
                    crate::rsm::qlog::codec::compress_wire(&raw).map(Bytes::from)
                })
                .await
                .map_err(|e| std::io::Error::other(format!("wire compression: {e}")))
            })
            .await
            .cloned()
    }

    /// [`AppEntry::wire_zstd`]'s answer once made; `None` before.
    pub fn wire_zstd_ready(&self) -> Option<Option<Bytes>> {
        self.0.wire_z.get().cloned()
    }

    /// Await the codec work on a full entry's payloads (started at propose, on
    /// the codec pool) and keep the stored form. The proposer calls it before
    /// the entry reaches openraft, in proposal order. A no-op for a stored
    /// entry or when already settled.
    pub async fn settle_codec(&self) {
        let Some(full) = self.0.full.as_ref() else {
            return;
        };
        if self.0.z.get().is_some() {
            return;
        }
        let pre = std::mem::take(&mut *self.0.pre.lock().expect("pre lock"));
        let n = appends_of(full);
        let z: Vec<Option<Bytes>> = if pre.len() == n {
            for p in &pre {
                p.wait().await;
            }
            pre.into_iter()
                .map(|p| p.finish().map(Bytes::from))
                .collect()
        } else {
            vec![None; n]
        };
        let _ = self.0.z.set(Arc::new(z));
    }

    /// A full entry's settled stored forms ([`AppEntry::settle_codec`]).
    pub fn z(&self) -> Option<Arc<Vec<Option<Bytes>>>> {
        self.0.z.get().cloned()
    }

    /// A stored entry's parts: the payload-free entry, its encoding and the
    /// payloads.
    pub fn stored_parts(&self) -> Option<(Arc<Entry>, Bytes, Arc<Vec<StoredPayload>>)> {
        let stored = self.0.stored.as_ref()?;
        Some((
            self.0.payload_free.get()?.clone(),
            self.0.pf_bytes.get()?.clone(),
            stored.clone(),
        ))
    }

    /// Whether this node holds the entry's payloads (full or stored).
    pub fn has_payloads(&self) -> bool {
        self.0.full.is_some() || self.0.stored.is_some()
    }

    /// The wire bytes, in stored form, encoded once. A full entry's payloads
    /// go as the codec left them (raw when it was never settled — always a
    /// valid stored form). A payload-free entry has none: sending it would
    /// store 4-byte lengths as payloads on a follower, so it is an error here,
    /// never silent data loss.
    pub fn wire(&self) -> std::io::Result<Bytes> {
        use crate::rsm::effect::Effect;
        if let Some(b) = self.0.wire.get() {
            return Ok(b.clone());
        }
        let pf_bytes = self.pf_bytes()?;
        let mut out: Vec<u8>;
        if let Some(full) = self.0.full.as_ref() {
            let z = self.0.z.get();
            let blobs: Vec<&[u8]> = full
                .effects
                .iter()
                .filter_map(|eff| match eff {
                    Effect::Append { blob, .. } => Some(blob.as_slice()),
                    _ => None,
                })
                .collect();
            let size: usize = blobs.iter().map(|b| b.len() + 5).sum();
            out = Vec::with_capacity(8 + pf_bytes.len() + size);
            out.extend_from_slice(&(pf_bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(&pf_bytes);
            out.extend_from_slice(&(blobs.len() as u32).to_le_bytes());
            for (i, raw) in blobs.iter().enumerate() {
                match z.and_then(|z| z.get(i).cloned().flatten()) {
                    Some(zb) => {
                        out.push(1);
                        out.extend_from_slice(&(zb.len() as u32).to_le_bytes());
                        out.extend_from_slice(&zb);
                    }
                    None => {
                        out.push(0);
                        out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
                        out.extend_from_slice(raw);
                    }
                }
            }
        } else if let Some(stored) = self.0.stored.as_ref() {
            let size: usize = stored.iter().map(|p| p.bytes.len() + 5).sum();
            out = Vec::with_capacity(8 + pf_bytes.len() + size);
            out.extend_from_slice(&(pf_bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(&pf_bytes);
            out.extend_from_slice(&(stored.len() as u32).to_le_bytes());
            for p in stored.iter() {
                out.push(u8::from(p.zstd));
                out.extend_from_slice(&(p.bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(&p.bytes);
            }
        } else {
            return Err(std::io::Error::other(
                "a payload-free entry cannot be replicated",
            ));
        }
        let bytes = Bytes::from(out);
        let _ = self.0.wire.set(bytes.clone());
        Ok(bytes)
    }

    /// The payload-free encoding (made once from the full entry when needed,
    /// shared by the wire and the log writer's entry records).
    pub(crate) fn pf_bytes(&self) -> std::io::Result<Bytes> {
        if let Some(b) = self.0.pf_bytes.get() {
            return Ok(b.clone());
        }
        let full =
            self.0.full.as_ref().ok_or_else(|| {
                std::io::Error::other("a payload-free entry cannot be replicated")
            })?;
        let b = Bytes::from(
            encode_entry_payload_free(full)
                .map_err(|e| std::io::Error::other(format!("payload-free encode: {e:?}")))?,
        );
        let _ = self.0.pf_bytes.set(b.clone());
        Ok(b)
    }

    /// The bytes this entry holds in memory: what the raft log cache counts
    /// against its cap (`log_store.rs` `evict`), and a gauge. Every effect's
    /// body counts ([`effect_mem_bytes`]), in each decoded form the entry
    /// holds, and each encoded form: when only the `Append` payloads counted,
    /// a cache of state, KV and dead-letter entries "at" its 512 MB cap held
    /// several times that.
    pub fn mem_bytes(&self) -> usize {
        *self.0.mem.get_or_init(|| self.mem_estimate())
    }

    /// [`AppEntry::mem_bytes`]'s value: every decoded and encoded form held
    /// now, and for a proposed entry that has none of its encodings yet (the
    /// raft cache counts it at append, before the writer and the wire make
    /// them), one more copy of its bodies for them.
    fn mem_estimate(&self) -> usize {
        let blobs = |e: &Entry| -> usize { e.effects.iter().map(effect_mem_bytes).sum() };
        let decoded = self.0.full.as_ref().map(|e| blobs(e)).unwrap_or(0)
            + self.0.payload_free.get().map(|e| blobs(e)).unwrap_or(0);
        let encoded = self.0.pf_bytes.get().map(|b| b.len()).unwrap_or(0)
            + self.0.wire.get().map(|b| b.len()).unwrap_or(0);
        let ahead = if self.0.full.is_some() && encoded == 0 {
            decoded
        } else {
            0
        };
        let full = decoded + ahead;
        let z: usize = self
            .0
            .z
            .get()
            .map(|z| z.iter().flatten().map(|b| b.len()).sum())
            .unwrap_or(0);
        let stored: usize = self
            .0
            .stored
            .as_ref()
            .map(|s| s.iter().map(|p| p.bytes.len() + 16).sum())
            .unwrap_or(0);
        (full + z + stored).max(64) + encoded
    }

    /// The entry with its payloads, when this node proposed it.
    ///
    /// Kept for the entry's whole life in the cache, beside its encoded forms
    /// (dropping it once `pf_bytes`/`wire` exist would need every reader of
    /// this borrow audited first; [`AppEntry::mem_bytes`] counts it instead).
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
    /// When this node proposed the entry, if it did.
    pub fn proposed_at(&self) -> Option<std::time::Instant> {
        self.0.proposed_at
    }

    pub fn payload_free(&self) -> std::io::Result<Arc<Entry>> {
        if let Some(pf) = self.0.payload_free.get() {
            return Ok(pf.clone());
        }
        let bytes = self.pf_bytes().map_err(|_| {
            std::io::Error::other("an application entry has neither a full nor a payload-free form")
        })?;
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
            Some(e) => (e.commands.len(), e.effects.len(), self.has_payloads()),
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
        let bytes = self.wire().map_err(serde::ser::Error::custom)?;
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
        AppEntry::from_stored_wire(Bytes::from(bytes))
            .map_err(|e| serde::de::Error::custom(e.to_string()))
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
