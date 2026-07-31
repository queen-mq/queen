//! `queen migrate ...` subcommand — RETIRED on the log engine.
//!
//! The seg-era big-bang OFFLINE rows→segments migration was built on the
//! retired seg engine's `storage_v2_migrate` SQL family (seg_migration_progress
//! bookkeeping, explicit-seq segment writes, cursor seeding), all of which was
//! deleted with the seg engine (18-log-engine.md §1: the storage_v2_migrate
//! rows→seg migration file "is retired; a rows→log migration can be written
//! later if ever needed"). The log engine is greenfield — dev contract, no data
//! migration —
//! so this module is a stub: it keeps the CLI entry compiling and returns a
//! clear error WITHOUT touching the DB.

use crate::config::Config;

/// Human-readable reason the subcommand is unavailable.
pub const MIGRATION_UNSUPPORTED: &str = "migration is not supported on the log engine";

/// Entry point for `queen migrate ...` (main.rs subcommand dispatch).
/// Reports the retirement and exits non-zero. Never opens a DB connection.
pub async fn run(_cfg: Config, _args: Vec<String>) {
    tracing::error!(target: "migrate", reason = %MIGRATION_UNSUPPORTED, "migrate subcommand unsupported on log engine");
    tracing::error!(
        target: "migrate",
        "seg-era rows->segments migration was retired with the seg engine (developer/18-log-engine.md); the log engine is greenfield and boots its own schema"
    );
    std::process::exit(1);
}
