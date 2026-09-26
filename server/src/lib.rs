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

mod auth;
mod config;
mod encryption;
// EPHEMERAL_QUEUES.md §3.2 — twin of the `mod ephemeral;` in main.rs.
mod ephemeral;
mod frames;
mod handlers;
mod httpget;
// Twin of the `mod kafka_inproc;` in main.rs: the Kafka facade run IN-PROCESS
// (feature `kafka`). Compiled, never started here, and its process-global reads
// `None` for `handlers::status`.
#[cfg(feature = "kafka")]
mod kafka_inproc;
mod metrics;
mod notify;
mod obs;
mod peerclient;
mod quota;
// The replicated state machine: the broker's storage. In BOTH crate roots (the
// twin-list rule of this header): the embedded `queen::Broker` is a
// single-node deployment, which is exactly the topology the LocalReplicator
// serves (O15).
mod rsm;
mod switches;
mod syscollect;
mod tenant;
mod util;

/// Broker version, embedded from server.json at build time (see build.rs).
/// Same value the binary reports from /health.
pub const VERSION: &str = env!("QUEEN_VERSION");

pub mod embedded;

/// W7 fuzz entry points (PLAN_SINGLE_BINARY.md): the broker's untrusted
/// decoders as `fn(&[u8])`, driven by `server/fuzz` (cargo-fuzz). Not a
/// supported API; compiled for tests and with the `fuzzing` feature only.
#[cfg(any(test, feature = "fuzzing"))]
#[doc(hidden)]
pub mod fuzzing;

/// The canonical wire types, re-exported so embedding applications can name
/// request/response types without adding a second dependency.
pub use queen_protocol as protocol;

pub use embedded::{Broker, BrokerConfig, DeleteQueueResult, Error, StartError};
