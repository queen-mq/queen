//! `queen-s3` — the process.
//!
//! Boot is configuration, two clients, a health listener and a task per queue;
//! everything that reads a log or writes an object lives behind
//! [`queen_s3::driver`]. The two facades' entry points have the same shape for
//! the same reason — a `main` that grows policy is a policy nothing can test
//! without a process.
//!
//! What this file owns, and nothing else does:
//!
//! * **the queue set** — `QUEEN_S3_QUEUES` as a list, or `*` re-listed every ten
//!   discovery intervals so a queue created after boot gets a task;
//! * **ownership** — one lease per queue (plan §6.6), re-tried after its TTL
//!   when another instance holds it, refreshed while this one works;
//! * **the signals** — SIGTERM and Ctrl-C set the shutdown flag every queue task
//!   observes, and the process exits when they have drained (plan §6.7). The
//!   embedded supervisor's grace is 30 s and this process is expected to be well
//!   inside it (server/src/s3_sink.rs);
//! * **the exit codes**, so an orchestrator can tell a bad variable from a bad
//!   afternoon.
//!
//! | code | meaning | does a restart help? |
//! |---|---|---|
//! | 0 | drained on a signal | — |
//! | 1 | the health listener could not bind `QUEEN_S3_LISTEN` | maybe |
//! | 2 | the configuration is wrong; the line names the variable | no |
//! | 3 | Queen or the bucket could not be reached AT BOOT | maybe |
//!
//! Code 3 is deliberately separate from the sink's normal failure policy, which
//! is to lag rather than to die (plan §6.7): once running, an unreachable broker
//! or bucket is a retry behind a backoff and a red `/healthz`. At boot it is
//! almost always a wrong URL, a wrong key or a missing network, and a container
//! that exits says so louder than one that sits there logging.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use queen_s3::config::{Config, CrashAt, Queues};
use queen_s3::driver::{
    mid_upload_abort_hook, reject_static_partitions, DriverConfig, MemoryBudget, QueueDriver,
    Runtime, Shutdown, Stop,
};
use queen_s3::health::{self, HealthState};
use queen_s3::lease::{spawn_refresh, Acquired, Lease};
use queen_s3::obs::Metrics;
use queen_s3::queen::{HttpQueen, QueenApi};
use queen_s3::s3::{ObjectStore, S3Client};
use queen_s3::writer::{factory, WriterFactory};

/// How long a queue whose lease somebody else holds waits before trying again:
/// the TTL, so the earliest useful moment is the first one after the holder
/// could have expired.
fn retry_after_ttl(cfg: &Config) -> Duration {
    Duration::from_millis(cfg.lease_ttl_ms)
}

/// A queue that stopped for a reason a restart of the TASK cannot fix — the
/// queue does not exist, the bucket refused a write — is retried, slowly. The
/// operator fixes it; the sink notices without being restarted.
const TERMINAL_RETRY: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() {
    let cfg = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(e) => {
            // Before tracing is up, and on stderr: an operator reading a
            // container that would not start gets one line, and it is this one.
            eprintln!("queen-s3: {e}");
            std::process::exit(2);
        }
    };

    init_tracing();
    tracing::info!(target: "queen-s3", "{}", cfg.boot_line());

    if let Err(e) = reject_static_partitions(&cfg) {
        tracing::error!(target: "queen-s3", "{e}");
        eprintln!("queen-s3: {e}");
        std::process::exit(2);
    }

    let metrics = Arc::new(Metrics::new());
    let health = Arc::new(HealthState::new(metrics.clone(), cfg.max_window_ms));
    let listen = cfg.listen;
    let health_for_server = health.clone();
    let server = tokio::spawn(async move { health::serve(listen, health_for_server).await });

    // --- the two clients ---------------------------------------------------
    let queen: Arc<dyn QueenApi> = match HttpQueen::from_config(&cfg) {
        Ok(q) => Arc::new(q),
        Err(e) => {
            tracing::error!(target: "queen-s3", "{e}");
            eprintln!("queen-s3: {e}");
            std::process::exit(2);
        }
    };
    let store: Arc<dyn ObjectStore> = match S3Client::from_config(&cfg) {
        Ok(mut client) => {
            if cfg.crash_at == CrashAt::MidUpload {
                // The multipart half of the fault; the single-PUT half is in the
                // driver, between the first object and the manifest.
                client.on_mid_upload(mid_upload_abort_hook());
            }
            Arc::new(client.with_metrics(metrics.clone()))
        }
        Err(e) => {
            tracing::error!(target: "queen-s3", "{e}");
            eprintln!("queen-s3: {e}");
            std::process::exit(2);
        }
    };

    // --- boot probes: both sides, before anything claims a queue -----------
    // The Queen probe is an EMPTY discovery batch: it answers safeTime alone,
    // writes nothing, and is classed `Consume` at the proxy exactly like the
    // fetch and kv calls the sink lives on — so a consume-scoped key passes.
    // `list_queues` (`GET /api/v1/resources/queues`, `read` scope) is only
    // reached when QUEEN_S3_QUEUES=* asks for it, below.
    if let Err(e) = queen.partitions_changed(Vec::new()).await {
        let msg = format!(
            "cannot reach Queen at {} ({e}). The sink reads through POST /api/v1/fetch and \
             POST /api/v1/partitions/changed and commits through POST /api/v1/kv, so QUEEN_URL \
             must be a broker or a proxy and QUEEN_TOKEN must carry the `consume` scope \
             (plus `read` only for QUEEN_S3_QUEUES=*)",
            cfg.queen_url
        );
        tracing::error!(target: "queen-s3", "{msg}");
        eprintln!("queen-s3: {msg}");
        std::process::exit(3);
    }
    // A HEAD of the prefix: 404 is a fine answer (nothing written yet), a 403 or
    // a connection failure is the credential or the endpoint being wrong.
    if let Err(e) = store.head(&format!("{}/", cfg.prefix)).await {
        let msg = format!(
            "cannot reach the bucket {} at {} ({e}). QUEEN_S3_ACCESS_KEY/_SECRET_KEY must be \
             allowed s3:PutObject, s3:GetObject and s3:ListBucket on {}/*, and \
             QUEEN_S3_PATH_STYLE=true is what most gateways need",
            cfg.bucket, cfg.endpoint, cfg.prefix
        );
        tracing::error!(target: "queen-s3", "{msg}");
        eprintln!("queen-s3: {msg}");
        std::process::exit(3);
    }

    // --- the shared runtime -------------------------------------------------
    let writers: Arc<dyn WriterFactory> = Arc::from(factory(&cfg.writer));
    let shutdown = Shutdown::new();
    let rt = Runtime {
        cfg: Arc::new(DriverConfig::from_config(&cfg)),
        queen: queen.clone(),
        store,
        writers,
        metrics,
        health,
        budget: Arc::new(MemoryBudget::new(cfg.memory_bytes())),
        shutdown: shutdown.clone(),
    };

    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let signal = shutdown_signal().await;
        tracing::info!(
            target: "queen-s3",
            signal,
            "shutting down: no new reads, the window in flight is finished"
        );
        signal_shutdown.trigger();
    });

    tokio::select! {
        joined = server => match joined {
            Ok(Ok(())) => tracing::info!(target: "queen-s3", "health listener stopped"),
            Ok(Err(e)) => {
                tracing::error!(target: "queen-s3", "{e}");
                eprintln!("queen-s3: {e}");
                std::process::exit(1);
            }
            Err(e) => {
                tracing::error!(target: "queen-s3", "health listener panicked: {e}");
                std::process::exit(1);
            }
        },
        () = supervise(rt, Arc::new(cfg)) => {
            tracing::info!(target: "queen-s3", "every queue drained; exiting");
        }
    }
    std::process::exit(0);
}

/// Keep one task per queue alive for as long as the process runs.
///
/// With `QUEEN_S3_QUEUES=*` the set is re-listed every ten discovery intervals:
/// often enough that a queue created at ten in the morning is being sinked a
/// minute later, rarely enough that the listing is not a load. Queues are never
/// removed — a task whose queue disappeared discovers
/// `UNKNOWN_TOPIC_OR_PARTITION`, stops, and retries slowly, which is also what
/// should happen if the queue comes back.
async fn supervise(rt: Runtime, cfg: Arc<Config>) {
    let mut tasks: BTreeMap<String, tokio::task::JoinHandle<()>> = BTreeMap::new();
    let relist = Duration::from_millis(cfg.discovery_interval_ms.saturating_mul(10).max(1_000));

    while !rt.shutdown.is_set() {
        let queues = match &cfg.queues {
            Queues::Named(names) => names.clone(),
            Queues::All => match rt.queen.list_queues().await {
                Ok(names) => names,
                Err(e) => {
                    tracing::warn!(target: "queen-s3", error = %e, "cannot list the queues; keeping the set this instance already has");
                    Vec::new()
                }
            },
        };
        for queue in queues {
            if tasks.contains_key(&queue) {
                continue;
            }
            tracing::info!(target: "queen-s3", queue = %queue, "starting a queue task");
            let rt = rt.clone();
            let cfg = cfg.clone();
            let name = queue.clone();
            tasks.insert(
                queue,
                tokio::spawn(async move { own_and_run(rt, cfg, name).await }),
            );
        }
        if matches!(cfg.queues, Queues::Named(_)) {
            // A fixed list needs no re-listing; wait for the signal instead of
            // waking up every few seconds to do nothing.
            rt.shutdown.wait().await;
            break;
        }
        tokio::select! {
            _ = tokio::time::sleep(relist) => {}
            _ = rt.shutdown.wait() => break,
        }
    }

    for (queue, handle) in tasks {
        if let Err(e) = handle.await {
            tracing::error!(target: "queen-s3", queue = %queue, error = %e, "queue task panicked");
        }
    }
}

/// One queue, for the life of the process: take the lease, run the driver, and
/// decide what to do with the way it stopped.
async fn own_and_run(rt: Runtime, cfg: Arc<Config>, queue: String) {
    while !rt.shutdown.is_set() {
        let lease = Arc::new(Lease::new(
            rt.queen.clone(),
            &cfg.sink,
            &queue,
            &cfg.instance,
            cfg.lease_ttl_ms,
        ));
        match lease.acquire().await {
            Ok(Acquired::Taken) => {}
            Ok(Acquired::HeldBy(owner)) => {
                tracing::info!(
                    target: "queen-s3",
                    queue = %queue,
                    owner = %owner,
                    "another instance owns this queue; retrying after the lease TTL"
                );
                sleep_or_shutdown(&rt.shutdown, retry_after_ttl(&cfg)).await;
                continue;
            }
            Err(e) => {
                tracing::warn!(target: "queen-s3", queue = %queue, error = %e, "cannot claim the queue lease; retrying");
                sleep_or_shutdown(&rt.shutdown, retry_after_ttl(&cfg)).await;
                continue;
            }
        }

        // The refresher lives exactly as long as the driver: its handle aborts
        // the task on drop, so a queue this instance stops running is a queue it
        // stops claiming to own.
        let refresher = spawn_refresh(lease.clone());
        let driver = QueueDriver::new(rt.clone(), queue.clone(), lease.clone());
        let stop = driver.run().await;
        drop(refresher);
        lease.release().await;

        match stop {
            Stop::Drained => return,
            Stop::Crashed(_) => return,
            Stop::Fenced(_) => {
                sleep_or_shutdown(&rt.shutdown, retry_after_ttl(&cfg)).await;
            }
            Stop::Failed(why) => {
                tracing::error!(
                    target: "queen-s3",
                    queue = %queue,
                    why,
                    retry_in_s = TERMINAL_RETRY.as_secs(),
                    "queue stopped; it will be retried in case the cause was fixed"
                );
                sleep_or_shutdown(&rt.shutdown, TERMINAL_RETRY).await;
            }
        }
    }
}

async fn sleep_or_shutdown(shutdown: &Shutdown, d: Duration) {
    tokio::select! {
        _ = tokio::time::sleep(d) => {}
        _ = shutdown.wait() => {}
    }
}

/// `QUEEN_S3_LOG_FORMAT=json` gives the structured form a log pipeline wants;
/// anything else gives the human one. `RUST_LOG` selects the level, defaulting
/// to `info` for this crate and `warn` for its dependencies — the same default
/// the broker and both facades use, so one mental model covers four binaries.
fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn,queen_s3=info,queen-s3=info"));
    let json = std::env::var("QUEEN_S3_LOG_FORMAT")
        .map(|v| v.trim().eq_ignore_ascii_case("json"))
        .unwrap_or(false);
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match json {
        true => builder.json().init(),
        false => builder.init(),
    }
}

/// SIGTERM or Ctrl-C, whichever arrives first, named so the log line says which.
///
/// SIGTERM is the one that matters: an orchestrator sends it, and plan §6.7
/// gives the sink a grace period to finish the window it is uploading — which is
/// why the shutdown path is a signal the driver observes rather than an
/// immediate exit.
async fn shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(target: "queen-s3", "cannot listen for SIGTERM: {e}");
                let _ = tokio::signal::ctrl_c().await;
                return "ctrl-c";
            }
        };
        tokio::select! {
            _ = term.recv() => "sigterm",
            _ = tokio::signal::ctrl_c() => "ctrl-c",
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "ctrl-c"
    }
}
