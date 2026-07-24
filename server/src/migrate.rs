//! `queen-seg migrate ...` subcommand — RETIRED on the log engine.
//!
//! The seg-era big-bang OFFLINE rows→segments migration was built on the
//! `028_storage_v2_migrate.sql` SQL family (seg_migration_progress bookkeeping,
//! explicit-seq segment writes, cursor seeding), all of which was deleted with
//! the seg engine (18-log-engine.md §1: "028_storage_v2_migrate.sql (rows→seg
//! migration) is retired; a rows→log migration can be written later if ever
//! needed"). The log engine is greenfield — dev contract, no data migration —
//! so this module is a stub: it keeps the CLI entry compiling and returns a
//! clear error WITHOUT touching the DB.
//!
//! (The /api/v1/migration/* HTTP handlers in handlers/migration.rs are a
//! separate pg_dump/pg_restore admin tool and do not go through this module.)

use crate::config::Config;

/// Human-readable reason the subcommand is unavailable.
pub const MIGRATION_UNSUPPORTED: &str = "migration is not supported on the log engine";

/// Entry point for `queen-seg migrate ...` (main.rs subcommand dispatch).
/// Reports the retirement and exits non-zero. Never opens a DB connection.
pub async fn run(_cfg: Config, _args: Vec<String>) {
    tracing::error!(target: "migrate", reason = %MIGRATION_UNSUPPORTED, "migrate subcommand unsupported on log engine");
    tracing::error!(
        target: "migrate",
        "seg-era rows->segments migration was retired with the seg engine (developer/18-log-engine.md); the log engine is greenfield and boots its own schema"
    );
    std::process::exit(1);
}
