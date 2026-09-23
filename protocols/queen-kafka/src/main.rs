//! queen-kafka binary: environment configuration, then the listener.
//! Spec: PLAN_QUEEN_KAFKA.md (repo root); the protocol lives in the library, and
//! so does the boot ([`queen_kafka::boot`]) that a broker running this facade
//! in-process shares.

use std::sync::Arc;

use queen_kafka::{boot, queen};

/// Resolves when this process is asked to stop, with the name of what asked.
///
/// SIGTERM is the one that matters and the one nothing here used to handle:
/// it is what `kubectl delete pod` and every rolling deploy send, what
/// `systemctl stop` sends, and what the broker's own supervisor sends to this
/// process in embedded mode (server/src/kafka_facade.rs). Without a handler the
/// default action ends the process where it stands, which in cluster mode means
/// a registry row left behind for a whole TTL — and in a container, where this
/// is pid 1, it means the signal is not even delivered.
///
/// Ctrl-C is here for the same reason in the shape a person uses: a facade run
/// from a terminal and stopped from that terminal must hand its id back too.
#[cfg(unix)]
async fn stop_signal() -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            // Refusing to start over this would be worse than the loss: the
            // facade still serves, it just cannot be stopped politely.
            tracing::warn!(
                target: "boot",
                error = %e,
                "SIGTERM cannot be handled in this process; a stop will not hand this node's \
                 registry row back and its peers will advertise it until the row's TTL expires"
            );
            let _ = tokio::signal::ctrl_c().await;
            return "ctrl-c";
        }
    };
    tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = tokio::signal::ctrl_c() => "ctrl-c",
    }
}

#[cfg(not(unix))]
async fn stop_signal() -> &'static str {
    let _ = tokio::signal::ctrl_c().await;
    "ctrl-c"
}

/// Slim tracing init, aligned with the broker's and the proxy's obs.rs:
/// LOG_LEVEL / RUST_LOG feed the EnvFilter, QUEEN_LOG_JSON switches format.
fn init_tracing() {
    let filter = std::env::var("RUST_LOG")
        .or_else(|_| std::env::var("LOG_LEVEL"))
        .unwrap_or_else(|_| "info".to_string());
    let filter = tracing_subscriber::EnvFilter::try_new(filter)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let json = std::env::var("QUEEN_LOG_JSON")
        .map(|v| v == "true")
        .unwrap_or(false);
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
    std::panic::set_hook(Box::new(|info| {
        tracing::error!(target: "panic", "{info}");
    }));
}

#[tokio::main]
async fn main() {
    init_tracing();
    let cfg = match boot::Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(target: "boot", "FATAL: {e}");
            std::process::exit(1);
        }
    };
    let api = match queen::HttpQueen::new(cfg.queen_url()) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            tracing::error!(target: "boot", "FATAL: {e}");
            std::process::exit(1);
        }
    };
    if let Err(e) = boot::serve(cfg, api, "http", None, stop_signal()).await {
        tracing::error!(target: "boot", "FATAL: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SIGTERM is what stops this process, and it is handled rather than
    /// defaulted: `kubectl delete pod`, `systemctl stop` and the broker's own
    /// supervisor in embedded mode all send exactly this, and the default
    /// action would end the process before it could hand its node id back.
    ///
    /// The signal is raised at THIS process, so the assertion is the real
    /// thing and not a stand-in. The test installs its own listener first: from
    /// that moment tokio owns the disposition of SIGTERM process-wide, so a
    /// missed delivery can only make this test slow, never kill the run.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_sigterm_is_what_stops_this_process() {
        use tokio::signal::unix::{signal, SignalKind};
        let ours = signal(SignalKind::terminate()).expect("cannot handle SIGTERM here");
        let mut stopping = tokio::spawn(stop_signal());
        let me = std::process::id().to_string();
        // Raised until it is seen: the spawned task registers its own stream
        // asynchronously, and a signal delivered before it does is one nothing
        // is listening for. Re-raising is free — the disposition is already
        // tokio's, and `ours` above guarantees that from the first line.
        for _ in 0..50 {
            let raised = std::process::Command::new("kill")
                .args(["-TERM", &me])
                .status()
                .expect("cannot raise SIGTERM");
            assert!(raised.success(), "kill -TERM refused");
            if let Ok(stopped) =
                tokio::time::timeout(std::time::Duration::from_millis(100), &mut stopping).await
            {
                assert_eq!(stopped.expect("the stop task panicked"), "SIGTERM");
                // `ours` is held to here on purpose: it is what guarantees the
                // disposition was tokio's before the first `kill`.
                drop(ours);
                return;
            }
        }
        panic!("SIGTERM did not stop the process's serve loop");
    }
}
