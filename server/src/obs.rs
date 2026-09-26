//! Observability foundation (LOGGING_PLAN.md, Phase 0 + Phase 1).
//!
//! The broker had no logging framework: 86 raw `println!/eprintln!`, no
//! timestamps, no levels, and the Helm-injected `LOG_LEVEL` was never read. This
//! module installs a `tracing` subscriber whose `EnvFilter` finally honours
//! `RUST_LOG`/`LOG_LEVEL`, a panic hook that turns a silent task death into a
//! structured ERROR (see the panic note below), a single load-safe
//! sampling primitive (`Sampler`, generalising `ack_registry::maybe_report`), and
//! the periodic `rates` / `sizes` aggregate reporters.
//!
//! ## Line shape
//! `<RFC3339>  <LEVEL>  <target>  <message>  key=val …` — every line carries a
//! broker-generated UTC timestamp and a subsystem `target`, and (by the WHERE
//! rule) at least one of queue/partition/group/worker/peer/offset.
//!
//! ## Panics: unwind, except on the core (PLAN_SINGLE_BINARY.md W1)
//! `Cargo.toml` builds with `panic = "unwind"`. The hook installed by
//! [`install_panic_hook`] ([`panic_policy`]) emits a structured ERROR for every
//! panic and then ABORTS when the panicking thread is a core thread (planner,
//! log writer/syncer, apply, checkpoint writer, raft runtime, segment writers)
//! or the panic is a poisoned-lock unwrap; every other thread or task unwinds
//! and only that task/connection dies. After an abort k8s restarts the pod,
//! the raft class replays from its durable state and
//! `file_buffer::startup_recovery` drains any spooled data.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Install the process-wide `tracing` subscriber. Call ONCE, first thing in
/// `main`, before any `info!/warn!/error!`. Verbosity is resolved from
/// `RUST_LOG`, else the Helm-injected `LOG_LEVEL`, else `info`; both accept the
/// full `EnvFilter` syntax (`info,queen::pop=debug`). `QUEEN_LOG_JSON=1`
/// switches to a one-object-per-line JSON formatter for structured shippers.
///
/// Binary-only (`server` feature): the library must never install a global
/// subscriber — the embedding application owns tracing.
#[cfg(feature = "server")]
pub fn init() {
    use tracing_subscriber::fmt::time::UtcTime;
    use tracing_subscriber::EnvFilter;

    let directive = std::env::var("RUST_LOG")
        .ok()
        .or_else(|| std::env::var("LOG_LEVEL").ok())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "info".to_string());
    let env_filter = EnvFilter::try_new(&directive).unwrap_or_else(|_| EnvFilter::new("info"));

    // Same boolean spellings as every other knob. This runs BEFORE the subscriber
    // exists, so an unparseable value cannot be reported here — it falls back to
    // text and `config::load()` (which validates QUEEN_LOG_JSON eagerly, moments
    // later) fails the boot with the real message.
    let json = std::env::var("QUEEN_LOG_JSON")
        .ok()
        .and_then(|v| crate::config::parse_bool(&v))
        .unwrap_or(false);
    let builder = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_timer(UtcTime::rfc_3339())
        .with_target(true);
    if json {
        builder.json().flatten_event(true).init();
    } else {
        // ANSI off: broker logs are almost always captured by a container/journal
        // driver where escape codes are noise.
        builder.with_ansi(false).init();
    }
}

pub mod panic_policy;

/// Install the process panic policy ([`panic_policy::install`]): a structured
/// ERROR for every panic, then abort on a core thread or a poisoned lock and
/// unwind everywhere else. Chains the previous hook so backtraces still print.
/// Call first thing in `main`.
pub fn install_panic_hook() {
    panic_policy::install();
}

/// Structured fatal: log the reason at ERROR, then exit(1). Replaces the scattered
/// `eprintln!("FATAL: …"); process::exit(1)` boot aborts so a fatal is greppable
/// by level and carries a timestamp.
pub fn fatal(reason: impl std::fmt::Display) -> ! {
    tracing::error!(target: "boot", "FATAL: {reason}");
    std::process::exit(1);
}

/// Load-safe sampling primitive — the ONE sanctioned way to log from a
/// per-request / per-message path. A wall-clock time-window gate (generalising
/// `ack_registry::maybe_report`): at most one emit per `interval_ms` per instance,
/// process-wide, chosen by a CAS so exactly one thread wins. Returns the number of
/// events suppressed since the last emit so the caller can print `suppressed=N`.
///
/// ```ignore
/// static ENC_WARN: Sampler = Sampler::new(10_000);
/// if let Some(suppressed) = ENC_WARN.tick_now() {
///     warn!(target: "push", queue = %q, suppressed, "encryption failed; stored plaintext");
/// }
/// ```
pub struct Sampler {
    last_ms: AtomicI64,
    interval_ms: i64,
    suppressed: AtomicU64,
}

impl Sampler {
    pub const fn new(interval_ms: i64) -> Self {
        Sampler {
            last_ms: AtomicI64::new(0),
            interval_ms,
            suppressed: AtomicU64::new(0),
        }
    }

    /// `Some(suppressed_since_last)` when it is time to emit (and this thread won
    /// the slot); `None` otherwise, having counted this call as suppressed.
    pub fn tick(&self, now_ms: i64) -> Option<u64> {
        let prev = self.last_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(prev) < self.interval_ms {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        if self
            .last_ms
            .compare_exchange(prev, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        Some(self.suppressed.swap(0, Ordering::Relaxed))
    }

    /// `tick` using the process clock.
    pub fn tick_now(&self) -> Option<u64> {
        self.tick(crate::util::now_epoch_ms())
    }
}

/// Resolve when either SIGTERM (k8s pod termination) or Ctrl-C arrives, logging
/// the signal. Fed to axum's `with_graceful_shutdown` so in-flight requests
/// finish before exit.
pub async fn shutdown_signal() {
    use tokio::signal;
    let ctrl_c = async {
        let _ = signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    tracing::info!(target: "shutdown", "signal received, draining in-flight requests");
}
