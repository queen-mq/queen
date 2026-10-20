//! Kafka wire protocol for Queen: a translation layer, nothing more.
//!
//! Every Kafka request becomes operations of the Queen broker this library runs
//! inside ([`core::Broker`]), and nothing the facade knows outlives a
//! connection or a leader's term:
//!
//! | Kafka                         | Queen                                                  |
//! |-------------------------------|--------------------------------------------------------|
//! | Produce                       | push, in order per connection (`in_log`)               |
//! | Fetch, ListOffsets            | read at an offset (`fetch`), bounds, time lookup       |
//! | OffsetCommit / OffsetFetch    | the consumer group's positions                         |
//! | Metadata, FindCoordinator     | queues, raft members; the raft leader leads everything |
//! | Join/Sync/Heartbeat/Leave     | in memory on the raft leader ([`groups`])              |
//! | Create/Delete topics          | create / delete queue, width row in KV                 |
//!
//! The design and what is left for later: `REDESIGN.md`.

pub mod config;
pub mod core;
pub mod decompress;
pub mod records;
pub mod sasl;
pub mod wire;

mod conn;
mod errors;
mod groups;
mod handlers;
mod server;
mod topics;
mod versions;

pub use config::Config;
pub use server::serve;
