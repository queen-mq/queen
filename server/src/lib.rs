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
mod lease;
mod mesh;
mod metrics;
mod migrate;
mod notify;
mod obs;
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
