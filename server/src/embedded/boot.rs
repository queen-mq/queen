//! Embedded composition root.
//!
//! Mirrors the binary's boot (`run_raft` in `src/main.rs`, which stays the
//! single source of truth for the HTTP server) with three deliberate
//! differences:
//!
//! * no HTTP listener, no auth/tenancy middleware, no JWKS refresh — the
//!   embedding application is in-process and already trusted;
//! * boot failures return [`StartError`] instead of exiting the process;
//! * the loops spawned HERE keep their `JoinHandle`s so `shutdown()` can
//!   abort them.
//!
//! KEEP IN SYNC: if `run_raft` gains or reorders a boot step, this file must
//! follow.

use std::sync::Arc;

use super::{BrokerConfig, StartError};
use crate::config;
use crate::handlers::AppState;

/// Every boolean env knob the boot path reads (the `env_bool` call sites in
/// config.rs plus its EXTERNAL_BOOL_KEYS). The binary exits the process on an
/// unparseable value (`obs::fatal`); a library must not, so boot() pre-validates
/// the same keys through `config::env_bool_checked` and returns
/// `StartError::Config` BEFORE `config::load()` can reach its fatal path.
/// KEEP IN SYNC with the `env_bool` call sites in config.rs.
const BOOT_BOOL_KEYS: &[&str] = &[
    "QUEEN_LOG_JSON",
    "JWT_ENABLED",
    "QUEEN_TENANCY_HEADER",
    "QUEEN_KV_TRUSTED_PROXY",
    "QUEEN_KAFKA_EMBEDDED",
    "QUEEN_KV_REQUIRE_GRANT",
    "QUEEN_EPHEMERAL_REQUIRE_GRANT",
];

pub(super) struct Booted {
    pub st: Arc<AppState>,
    pub tasks: Vec<tokio::task::JoinHandle<()>>,
}

pub(super) async fn boot(bc: &BrokerConfig) -> Result<Booted, StartError> {
    // Refuse malformed boolean env BEFORE config::load(), whose own reads
    // would exit the host process (env_bool -> obs::fatal). Numeric knobs
    // silently default in config::load, so booleans are the only exit path.
    for k in BOOT_BOOL_KEYS {
        config::env_bool_checked(k, false).map_err(StartError::Config)?;
    }

    // Same env-driven defaults as the binary (QUEEN_* tuning knobs keep
    // working), with the BrokerConfig fields winning over env where set.
    let mut cfg = config::load();
    // The data directory is required: the BrokerConfig field, else
    // QUEEN_RAFT_DIR. The binary's default (/var/lib/queen/raft) is not used
    // implicitly, because two embedded apps on one host must never share one.
    match &bc.raft_dir {
        Some(dir) => cfg.raft_dir = dir.display().to_string(),
        None if std::env::var("QUEEN_RAFT_DIR").is_ok_and(|v| !v.trim().is_empty()) => {}
        None => {
            return Err(StartError::Config(
                "no data directory: set BrokerConfig::raft(dir) or QUEEN_RAFT_DIR".into(),
            ))
        }
    }
    if let Some(ms) = bc.stmt_timeout_ms {
        cfg.stmt_timeout = std::time::Duration::from_millis(ms);
    }
    // Refused, not defaulted like the env knobs: a fraction passed for a
    // percentage (0.85) would otherwise refuse every write.
    for (field, pct, slot) in [
        (
            "raft_disk_high_pct",
            bc.raft_disk_high_pct,
            &mut cfg.raft_disk_high_pct,
        ),
        (
            "raft_disk_low_pct",
            bc.raft_disk_low_pct,
            &mut cfg.raft_disk_low_pct,
        ),
    ] {
        if let Some(pct) = pct {
            if !config::is_pct(pct) {
                return Err(StartError::Config(format!(
                    "{field} must be a percentage in 1..=100"
                )));
            }
            *slot = pct;
        }
    }

    // Env knobs that only make sense with the HTTP surface are ignored
    // embedded — say so instead of silently dropping them.
    if cfg.tenancy_header {
        tracing::warn!(
            target: "boot",
            "QUEEN_TENANCY_HEADER is ignored embedded — the embedded broker is single-tenant by construction"
        );
    }
    if cfg.auth.enabled {
        tracing::warn!(
            target: "boot",
            "JWT_ENABLED is ignored embedded — there is no HTTP surface; the in-process caller is trusted"
        );
    }

    // Embedded is single-tenant by construction: every call is stamped with the
    // default tenant, exactly like the OSS HTTP path with the flag off.
    cfg.tenancy_header = false;

    if cfg.raft_dir.trim().is_empty() {
        return Err(StartError::Config(
            "QUEEN_RAFT_DIR is empty (the broker's data directory, §11.1)".into(),
        ));
    }
    tracing::info!(target: "boot", dir = %cfg.raft_dir, "embedded broker");
    // W1 panic policy (PLAN_SINGLE_BINARY.md): a panic on a core thread
    // (planner, log writer, apply, checkpoint, openraft) aborts the process
    // instead of leaving a silently wedged broker in the host. The embedded
    // variant never touches the host's own panics or locks. Idempotent; the
    // binary installs the full policy in `main`.
    crate::obs::panic_policy::install_embedded();
    // Crash points (§13.5), off unless QUEEN_TEST_FAULTS is set. KEEP IN SYNC
    // with `run_raft`.
    crate::rsm::faults::init_from_env();
    // Register the state-machine builder before `build_raft_state` builds the
    // facade (first-write-wins; KEEP IN SYNC with `run_raft`).
    crate::rsm::facade::set_builder(crate::rsm::facade::real::real_builder);
    let st = crate::handlers::raft::build_raft_state(&cfg).map_err(StartError::Config)?;
    let tasks = vec![crate::ephemeral::spawn_backstop(st.ephemeral.clone())];
    Ok(Booted { st, tasks })
}
