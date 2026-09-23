//! The tests of `rsm/`. WP-1.1 brings the codec ones; later WPs add their own
//! files here and one line below.
//!
//! - [`samples`] the canonical value of every effect kind and one whole entry.
//!   Everything else is measured against these, so there is one place to look
//!   when a shape changes.
//! - [`roundtrip`] encode → decode → equal, and the layout rules of §5.1.
//! - [`golden`] the fixture bytes under `golden/`: a test that fails on any
//!   byte change (a change there is a FORMAT change and needs a kind version
//!   bump, §5.3).
//! - [`fuzz`] random and mutated bytes into the decoders: they must error,
//!   never panic and never allocate on a length prefix they have not checked.
//! - [`gates`] what the bytes MEAN: the catalogue-version gate over effects
//!   and outcomes alike (I16, D20), the checksum boundary that decides whether
//!   a failure is a torn tail or a committed entry this node cannot apply
//!   (I11), the counters of I18, the distinct request ids of §5.4, and the
//!   values the encoder refuses to write.
//! - [`columns`] the catalogue against the SQL that specifies it: every column
//!   of the one table an effect carries whole has a slot (§8, G-1).
//! - [`store`] the store against a real LMDB environment (WP-1.2): CRUD and
//!   range scans per keyspace, read isolation, the four pins of D9, the dedup
//!   index of D10 and the derived indexes of §6.3.
//! - [`apply`] the apply thread against a real store and real segment files
//!   (WP-1.4): the gates on an entry (I16, I18, I5, the log's own order), the
//!   message path, the counters of §6.4 recomputed from the rows, I2's
//!   determinism property, the idempotence §11.5's repair rests on, the
//!   durable point that did not happen, and the claim pin against file GC.
//! - [`apply_crash`] `kill -9` of a child process mid-apply, then re-apply
//!   from the durable index and compare the digest with an uninterrupted run:
//!   the crash half of the idempotence above.
//! - [`store_crash`] `kill -9` of a child process while it commits, and inside
//!   an uncommitted transaction, then a reopen: the check G0 made WP-1.2 owe
//!   for D9 — minus its durability leg, which only dropped unflushed writes on
//!   the VM can decide (the file says so at its head).

mod apply;
mod apply_crash;
mod batcher;
mod columns;
mod dedup_txns;
mod facade;
#[cfg(feature = "kafka")]
mod facade_kafka;
mod facade_kv;
mod fuzz;
mod gates;
mod golden;
mod keep_overlay;
mod kv;
mod kv_crash;
mod planner_ack;
mod planner_harness;
mod planner_overlay;
mod planner_pop;
mod planner_push;
mod pop_bounded;
mod qlog_read;
mod qlog_shadow;
mod qlog_wal;
mod raft;
mod raft_cluster;
mod replicator;
mod replicator_crash;
mod roundtrip;
pub(super) mod samples;
mod store;
mod store_crash;
mod timers;
mod timers_crash;
