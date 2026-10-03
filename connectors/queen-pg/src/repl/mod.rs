//! The replication connection: our own small client for the PostgreSQL
//! streaming replication protocol (upstream tokio-postgres cannot run
//! CopyBoth), and the `pgoutput` decoder.

pub mod client;
pub mod lsn;
pub mod pgoutput;
pub mod wire;

pub use client::{ReplicationClient, ReplicationStream, StreamEvent, SystemIdentity};
pub use lsn::Lsn;

/// Microseconds between the Unix epoch and PostgreSQL's (2000-01-01 UTC).
pub const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;
