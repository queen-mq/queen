//! QueenMQ broker as an embeddable Rust library.
//!
//! This crate root exists alongside `src/main.rs` (the standalone HTTP broker
//! binary) and compiles the SAME module tree. The binary keeps its own crate
//! root and its own `mod` declarations, so the server is byte-identical with or
//! without this file; the library target adds the [`embedded`] facade on top.
//!
//! The embedded facade does not re-implement any broker logic: every operation
//! invokes the same handler functions the HTTP router dispatches to, minus the
//! socket, and parses the rendered bytes back into [`queen_protocol`] types —
//! the same types the `protocol_conformance` tests pin those bytes to. Running
//! embedded therefore IS running the broker, not a lookalike.
//!
//! Entry point: [`embedded::Broker::start`].
//!
//! The engine modules below are compiled for both targets. In the library they
//! are intentionally private — the supported public surface is `embedded` (plus
//! the re-exported protocol types); everything else remains an implementation
//! detail with no stability promise. `dead_code` is allowed crate-wide because
//! the library target does not reference the HTTP-only paths (router, mesh,
//! auth middleware) that the binary uses.
#![allow(dead_code)]
// The handlers module glob-re-exports every handler family for the binary's
// router; the library target dispatches only to the data-path families, so the
// unused-glob lint would fire on files this target deliberately does not edit.
#![allow(unused_imports)]

mod ack_fusion;
mod pop_autopilot;
mod pop_fusion;
mod ack_registry;
mod auth;
mod config;
mod db;
mod dedup;
mod encryption;
// EPHEMERAL_QUEUES.md §3.2 — twin of the `mod ephemeral;` in main.rs. The
// engine needs no pool, no mesh and no sweeper, so the library target compiles
// and runs it unchanged.
mod ephemeral;
mod file_buffer;
mod frames;
mod fusion;
mod handlers;
mod hotlist;
mod httpget;
mod internal;
// Twin of the `mod kafka_facade;` in main.rs (the twin-list rule of this header).
// Compiled, never started: embedded mode supervises a child process that talks
// HTTP to the broker's listener, and an embedded `queen::Broker` has no listener.
// `handlers::status` reads its process-global, which is `None` here.
mod kafka_facade;
// Twin of the `mod sqs_facade;` in main.rs, on the same terms as `kafka_facade`
// above: compiled, never started, and its process-global reads `None` here.
mod sqs_facade;
// Twin of the `mod s3_sink;` in main.rs (the S3 / data-lake sink connector), on
// the same terms as the two facades above: compiled, never started, and its
// process-global reads `None` here.
mod s3_sink;
mod lease;
mod mesh;
mod metrics;
mod migrate;
mod notify;
mod obs;
mod peerclient;
mod pgtls;
mod quota;
mod reconcile;
mod retention;
mod schema;
mod stats;
mod switches;
mod syscollect;
mod tenant;
mod util;
mod admission;

/// Broker version, embedded from server.json at build time (see build.rs).
/// Same value the binary reports from /health.
pub const VERSION: &str = env!("QUEEN_VERSION");

pub mod embedded;

/// The canonical wire types, re-exported so embedding applications can name
/// request/response types without adding a second dependency.
pub use queen_protocol as protocol;

pub use embedded::{Broker, BrokerConfig, DeleteQueueResult, Error, StartError};
