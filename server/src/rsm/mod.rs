//! `rsm/` — the replicated state machine of PLAN_RAFT.md: Queen without
//! Postgres, one replicated log.
//!
//! One leader turns client commands into log entries of EFFECTS; every node
//! applies committed entries, in order, to its own full copy of the state
//! (payload segment files plus an embedded ordered store). This module is the
//! whole of that machinery; nothing outside it may write committed state.
//!
//! # The module map (§3.4)
//!
//! The map below is the plan's, verbatim, with the work package that owns each
//! module. A module that does not exist yet is declared here as an EMPTY INLINE
//! module with its doc comment, so that:
//!
//! - the map is in the code, not only in the plan, and `cargo doc` shows it;
//! - the crate compiles today, with `rsm::` reachable from both crate roots;
//! - the WP that fills a module changes exactly ONE line here —
//!   `pub mod apply {}` becomes `pub mod apply;` — and creates `apply.rs`
//!   (or `apply/mod.rs`). No other file is touched, so parallel WPs do not
//!   collide on this one.
//!
//! An inline stub must never grow a body. Code goes in the file.
//!
//! | module | role | owner |
//! |---|---|---|
//! | [`effect`] | the effect catalogue and its codec (§5.2) | WP-1.1 (done) |
//! | [`entry`] | entry header, commands, outcomes, request ids (§5.1, §5.4) | WP-1.1 (done) |
//! | `command` | command types built by the receiver (§9.1) | WP-1.7 |
//! | `planner` | leader-side planning, pure over (committed view, overlay) (§7) | WP-1.5, phase 2 |
//! | `apply` | the only mutator of committed state (I1) | WP-1.4 |
//! | `state` | committed view: store access + RAM derived indexes (§6.3) | WP-1.2 |
//! | `store` | ordered store adapter (heed, D9) and keyspaces (§6.1) | WP-1.2 |
//! | `segments` | payload segment files (§11.2) | WP-1.3 |
//! | `dedup` | the dedup index, option (a) lean encoding (D10) | WP-1.2 |
//! | `batcher` | cycle driver: drain, plan, propose, await apply (§7.1) | WP-1.6 |
//! | `replicator` | the consensus seam: trait + `local` + `raft` (§12.1) | WP-1.6, WP-3.1 |
//! | `raftlog` | consensus log storage (§12.3) | WP-3.2 |
//! | `snapshot` | durable point, manifest, transfer, install (§11.6) | WP-4.6 |
//! | `net` | port 6634: framing, handshake, connection pools (§12.5) | WP-4.1 |
//! | `forward` | receiver → leader commands, hold, retries (§9.2) | WP-4.2 |
//! | `reads` | local stale reads and the read barrier (§9.4) | WP-2.6, WP-4.5 |
//! | `loops` | leader-only and node-local loops (§10) | phase 2 |
//! | `identity` | IDENTITY, bootstrap, join, replacement (§12.6) | WP-4.3 |
//! | `digest` | divergence detection (§12.9) | WP-4.7 |
//! | `local_metrics` | node-local metrics store (D17) | WP-2.8 |
//! | `faults` | crash points (§13.5) | WP-1.8 |
//!
//! # The rules this module lives under
//!
//! - **I1** committed state is mutated only by `apply`.
//! - **I2** `apply` is a pure function of (committed state, entry): no clock,
//!   no randomness, no environment, no hash-map iteration order, no floats.
//! - **I15** every I/O, RPC and store call has a deadline; no blocking call on
//!   a tokio worker; no `std::sync::Mutex` across an `.await`.
//! - **I16** an unknown effect kind, outcome version or catalogue version
//!   STOPS this node (no votes, no acks, no apply). It is never skipped. So
//!   does a body whose frame verified and which still did not decode: those
//!   bytes are what a quorum committed, so they are not a torn tail and §11.5
//!   must not truncate or discard them (I11). Both are
//!   [`effect::CodecError::fatal`].
//!
//! Nothing here is wired into the HTTP handlers yet: the storage seam
//! (`QUEEN_STORAGE=postgres|raft`) is WP-1.7's, and until then the postgres
//! class is the only path a request can take.

pub mod effect;
pub mod entry;
pub mod facade;

// ---------------------------------------------------------------------------
// The map as stubs. One line each, replaced by the owning WP (see the header).
// ---------------------------------------------------------------------------

/// Command types built by the receiver (§9.1): request id, deadline, tenant,
/// producer subject, kind, and the receiver's pre-work (hashes, packed blobs,
/// encrypted frames, message ids). Owner: WP-1.7.
pub mod command {}

/// Leader-side planning (§7): pure functions over (committed view, overlay)
/// producing effects and outcomes, never mutations (I1). One submodule per
/// atomic unit of §8. Owners: WP-1.5 (push, pop, ack), phase 2 (the rest).
pub mod planner;

/// The apply thread: the ONLY mutator of the store and the segment files (I1),
/// deterministic (I2), plus counters, wakes, waiters, durable points (§11.4),
/// recovery (§11.5) and local file GC (§11.7). Owner: WP-1.4.
pub mod apply;

/// The committed view: store reads plus the RAM-derived indexes of §6.3
/// (ready rings, deadline heaps, hot caches, timer wheel, notifiers).
/// Owner: WP-1.2.
pub mod state;

/// The ordered store adapter (heed/LMDB, D9) and the replicated keyspaces of
/// §6.1, with the four ratified pins: `MDB_NOSYNC`, a read-transaction handle
/// (never free-standing get/scan), `max_readers` ≥ the blocking pool, and the
/// map-size rule of §11.8. Owner: WP-1.2.
pub mod store;

/// Payload segment files (§11.2): per-bucket, append-only, rolling at
/// `QUEEN_RAFT_SEGMENT_BYTES`, sealed files immutable, the index of a sealed
/// file in its own `.qidx` beside it (§6.1 amendment). Owner: WP-1.3.
pub mod segments;

/// The per-queue append-only log store of `ALICE_PGLESS_NEWARCH.md` §3
/// (Phase A). ISOLATED: dead code, wired into nothing, changing no live
/// behaviour until a later phase swaps the byte store over to it. Record codec
/// (extends `segments/frame.rs`, with the leader `seq` and the Phase-B txn
/// envelope), per-queue writer with group commit, sparse `.qidx` index,
/// read/scan, recovery with torn-tail truncation, and unlink-dead retention.
pub mod qlog;

/// Dedup, option (a) in the lean encoding (D10): a store index per hash
/// `(pid, hash) → (offset, created_at)`, pruned by the txns window, whose hash
/// lists outlive the segments retention deletes. Owner: WP-1.2.
pub mod dedup;

/// The cycle driver (§7.1): drain the command channel under the batch caps,
/// plan, propose, wait for local apply, clear the overlay. Owner: WP-1.6.
pub mod batcher;

/// The consensus seam (§12.1): the `Replicator` trait, `local` (phases 1–2,
/// no network) and `raft` (the openraft adapter, phase 3).
/// Owners: WP-1.6, WP-3.1.
pub mod replicator;

/// Consensus log storage (§12.3): raft-log 0.4.6 behind the library's traits,
/// with a durable `save_committed` (S3 plan change 5). Owner: WP-3.2.
pub mod raftlog {}

/// Durable point, manifest, transfer and the atomic `CURRENT`-flip install
/// (§11.6, I17). Owner: WP-4.6.
pub mod snapshot {}

/// Port 6634 (D12): length-prefixed frames, the mutual HMAC handshake, the
/// sequenced per-frame MAC, one reader task per connection, separate pools for
/// Raft RPCs, forwarding and snapshots. Owner: WP-4.1.
pub mod net {}

/// Receiver → leader: commands, outcomes, `PayloadRead`, request-id retries
/// and the hold of D13 (§9.2). Owner: WP-4.2.
pub mod forward {}

/// Reads (§9.4): local stale reads from store read transactions and immutable
/// file bytes, and the coalesced read barrier for linearizable reads.
/// Owners: WP-2.6, WP-4.5.
pub mod reads {}

/// The loops of §10: leader-only (timer wheel, retention, KV and trace expiry,
/// chunked deletes, cluster version) and node-local (metrics, durable points,
/// snapshot triggers, digest reports). Owner: phase 2.
pub mod loops {}

/// IDENTITY `{cluster_id, node_id, generation, disk_uuid, fence}`, bootstrap,
/// join and replacement (§12.6, D21, I9). Owner: WP-4.3.
pub mod identity {}

/// Divergence detection (§12.9): the digest chain and the state digest.
/// Owner: WP-4.7.
pub mod digest {}

/// Crash points (§13.5), from the pgless `native/faults.rs`. Owner: WP-1.8.
/// The node-local metrics store (D17): `system_metrics`, `worker_metrics`,
/// `worker_metrics_summary`, `queue_lag_metrics`, `queue_parked_replica` and
/// `retention_history`, none of them replicated. Owner: WP-2.8.
pub mod local_metrics {}
pub mod fasthash;
pub mod faults;

/// Node-local timing metrics (O18, D17, PERF-1): the lock-free `queen_raft_*`
/// histograms and counters the pipeline feeds and `/metrics/prometheus` reads.
/// Not replicated; gated by `QUEEN_RAFT_METRICS` (default on).
pub mod timing;

/// TEMPORARY claim-scarcity diagnostics (2026-09-21). Not for merge.
pub mod dbgctr;

#[cfg(test)]
mod tests;
