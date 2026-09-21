//! `rsm/qlog/set.rs` — the applier-owned per-queue [`QLog`] registry
//! (`ALICE_PGLESS_NEWARCH.md` §1–§5, Phase A1).
//!
//! # SHADOW write only — not yet authoritative (Phase A1)
//!
//! Phase A0 built the per-queue store ([`QLog`]); this registry is Phase A1,
//! the FIRST wiring of it into apply, and it is a SHADOW: gated behind
//! `QUEEN_RAFT_QLOG` (default off), it writes every `Append` a SECOND time —
//! into its queue's log — alongside the segment write that is still
//! authoritative. Pop, dedup and recovery still read the segments and LMDB;
//! nothing here is read back except by the A1 match test. A2 switches the
//! reads over; until then the registry exists only to prove the write path is
//! byte-faithful (the match test) and to pay nothing when the knob is off (the
//! no-op test).
//!
//! # What it owns, and when it writes
//!
//! One [`QLog`] per queue, opened lazily under `<data_dir>/qlog/q<queue_id>/`
//! (sibling to `seg/`, `log/`, `store/`) on the first FLUSH that carries a
//! record for that queue. The applier owns the set exactly as it owns
//! [`crate::rsm::segments::Segments`].
//!
//! An `Append` is [`QLogSet::buffer`]ed — copied into an owned record keyed by
//! its queue — during `apply`, because the effect's `hashes`/`blob` borrow the
//! entry and are freed when `apply` returns, while the write happens later, at
//! the transaction boundary. [`QLogSet::flush`] then drains each queue's buffer
//! into ONE [`QLog::append_group`] per queue — one `write` + one fsync — and
//! the applier calls it at every store commit AND every durable point (§11.3/
//! §11.4), so the buffer never holds more than one store-commit window and a
//! durable point always leaves every buffered record fsynced (§5).
//!
//! # Determinism (I2)
//!
//! Nothing here reads a clock, the environment or randomness. `seq` and
//! `created_at_us` are the leader's inputs (from the entry/effect); `queue_id`
//! is a stable [`xxhash_rust::xxh3`] of `(tenant, queue)` (see
//! [`QLogSet::queue_id_of`]) — the same shape `planner::bucket_of` uses. The
//! queue id is node-local anyway (it only names a directory, never replicated
//! state), but keeping it a pure function of the two names keeps the shadow
//! path free of any environment the I2 deny-gate forbids.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;

use crate::rsm::qlog::{QLog, QLogOptions, RecordInput};

/// One `Append` buffered until the next flush, OWNING its bytes.
///
/// The effect's `hashes`/`blob` are borrowed from the entry and go away when
/// `apply` returns; the flush that writes them runs later (at a commit or a
/// durable point), so the buffer keeps its own copies. Bounded by one
/// store-commit window's worth of appends (the flush drains it every commit).
struct Buffered {
    seq: u64,
    pid: u64,
    base_offset: u64,
    count: u32,
    created_at_us: i64,
    hashes: Vec<u8>,
    payload: Vec<u8>,
}

/// The per-queue [`QLog`] registry (Phase A1 shadow). One instance per applier;
/// every method takes `&mut self` (the applier is the single writer).
pub struct QLogSet {
    /// `<data_dir>/qlog`. Each queue's files live under `q<queue_id>/` below it.
    root: PathBuf,
    /// Roll size + fsync mode, mirrored from the segment writer's options so the
    /// shadow rolls and fsyncs on the same terms the authoritative store does.
    opts: QLogOptions,
    /// One open log per queue id. `BTreeMap` so the flush order is deterministic
    /// (I2 hygiene — the order does not reach replicated state, but the shadow
    /// path keeps no incidental nondeterminism either).
    logs: BTreeMap<u64, QLog>,
    /// Records buffered since the last flush, per queue id. Drained whole at
    /// each commit/durable point.
    pending: BTreeMap<u64, Vec<Buffered>>,
}

impl QLogSet {
    /// A fresh registry rooted at `root` (`<data_dir>/qlog`). Opens no file yet;
    /// each queue's log is created on the first flush that carries a record for
    /// it.
    pub fn new(root: PathBuf, opts: QLogOptions) -> QLogSet {
        QLogSet {
            root,
            opts,
            logs: BTreeMap::new(),
            pending: BTreeMap::new(),
        }
    }

    /// The stable per-queue id: `xxh3_64(tenant ␟ queue)`, the two-name twin of
    /// `planner::bucket_of`'s `xxh3(tenant ␟ queue ␟ partition)`. Deterministic
    /// and collision-free on the `0x1F` separator (it cannot occur inside a name
    /// segment). A1 has no catalog-assigned queue id — queues are keyed by their
    /// `(tenant, queue)` names throughout — so this hash IS the id; a later
    /// phase may replace it with a catalog id without changing any record's
    /// bytes.
    pub fn queue_id_of(tenant: &str, queue: &str) -> u64 {
        let mut buf = Vec::with_capacity(tenant.len() + queue.len() + 1);
        buf.extend_from_slice(tenant.as_bytes());
        buf.push(0x1F);
        buf.extend_from_slice(queue.as_bytes());
        xxhash_rust::xxh3::xxh3_64(&buf)
    }

    /// Buffer one `Append` for its queue (a shadow of the `segments.append`
    /// that just ran). Copies the bytes so the record can outlive the entry.
    /// `seq` is the entry index (the leader's global order stamp).
    #[allow(clippy::too_many_arguments)]
    pub fn buffer(
        &mut self,
        tenant: &str,
        queue: &str,
        seq: u64,
        pid: u64,
        base_offset: u64,
        count: u32,
        created_at_us: i64,
        hashes: &[u8],
        payload: &[u8],
    ) {
        let qid = Self::queue_id_of(tenant, queue);
        self.pending.entry(qid).or_default().push(Buffered {
            seq,
            pid,
            base_offset,
            count,
            created_at_us,
            hashes: hashes.to_vec(),
            payload: payload.to_vec(),
        });
    }

    /// Drain every queue's buffer into ONE [`QLog::append_group`] per queue —
    /// one `write` and one fsync each — opening the queue's log lazily. Called
    /// at each store commit and each durable point, so no buffer outlives a
    /// commit window and a durable point leaves every buffered record fsynced.
    ///
    /// A group is taken out of `pending` BEFORE the (fallible) open/append, so a
    /// mid-flush I/O error does not leave a half-written group buffered for a
    /// retry: the applier poisons and is dropped, and the shadow log — which no
    /// reader depends on in A1 — is reopened (torn tail truncated) on restart.
    pub fn flush(&mut self) -> io::Result<()> {
        let qids: Vec<u64> = self
            .pending
            .iter()
            .filter(|(_, v)| !v.is_empty())
            .map(|(k, _)| *k)
            .collect();
        for qid in qids {
            let bufs = std::mem::take(self.pending.get_mut(&qid).expect("pending queue present"));
            let log = match self.logs.entry(qid) {
                Entry::Occupied(o) => o.into_mut(),
                Entry::Vacant(v) => {
                    let (log, _rec) = QLog::open(&self.root, qid, self.opts)?;
                    v.insert(log)
                }
            };
            let inputs: Vec<RecordInput<'_>> = bufs
                .iter()
                .map(|b| RecordInput {
                    seq: b.seq,
                    pid: b.pid,
                    base_offset: b.base_offset,
                    count: b.count,
                    created_at_us: b.created_at_us,
                    txn: None,
                    hashes: &b.hashes,
                    payload: &b.payload,
                })
                .collect();
            log.append_group(&inputs)?;
        }
        Ok(())
    }

    /// Drop a deleted queue's log handle and any records still buffered for it
    /// (§NA-I5: a log for a dropped queue is GC'd). A1 drops only the in-RAM
    /// handle and buffer; unlinking the on-disk `q<id>/` directory is retention
    /// (§3.3), a later phase — and the shadow is never read in A1, so a stale
    /// directory left behind harms nothing here.
    pub fn remove(&mut self, tenant: &str, queue: &str) {
        let qid = Self::queue_id_of(tenant, queue);
        self.logs.remove(&qid);
        self.pending.remove(&qid);
    }

    /// The open log for a queue id, for the A1 match test. Test-only: the
    /// product reads nothing from the shadow set until A2.
    #[cfg(test)]
    pub fn log(&self, queue_id: u64) -> Option<&QLog> {
        self.logs.get(&queue_id)
    }
}
