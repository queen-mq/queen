//! PostgreSQL source and sink for QueenMQ, linked into the broker
//! (PLAN_PG_CONNECTORS.md).
//!
//! * The **source** streams committed changes of PostgreSQL 17+ tables into
//!   queues through logical replication (`pgoutput`). Each source transaction
//!   is pushed in ONE Queen transaction together with the WAL position it
//!   reached, so a restart resumes exactly where Queen's state says.
//! * The **sink** is a consumer group the broker runs for you: each batch is
//!   applied to a PostgreSQL table in ONE PostgreSQL transaction together with
//!   the offsets it reached, so a redelivery is recognized and dropped.
//!
//! Both: exactly-once effects. Neither speaks HTTP: the broker hands each
//! connector a [`queen::QueenApi`] bound to the connector's tenant.
//!
//! Module ownership during the build (2026-10-02): `repl/*` + `values` (R),
//! `source/*` (S), `sink/*` (K), `config_validate` + `fake` + `metrics` +
//! `status` + `connector` + `pg/*` (C). `error`, `stop`, `queen`,
//! `config`, `repl/lsn` are the shared contract.

pub mod config;
mod config_validate;
pub mod error;
pub mod metrics;
pub mod pg;
pub mod queen;
pub mod repl;
pub mod sink;
pub mod source;
pub mod status;
pub mod stop;
pub mod values;

#[cfg(any(test, feature = "fake"))]
pub mod fake;

mod connector;

pub use connector::{Connector, Context, RunEnd};
pub use error::{Error, Result};
pub use metrics::Metrics;
pub use stop::{stop_pair, Stop, StopHandle};

/// Log target of every line this crate writes.
pub const LOG_TARGET: &str = "queen-pg";

/// `queen-pg/<version>`: the application_name of every connection.
pub fn application_name() -> String {
    concat!("queen-pg/", env!("CARGO_PKG_VERSION")).to_string()
}
