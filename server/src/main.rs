mod auth;
mod config;
mod encryption;
// EPHEMERAL_QUEUES.md §3.2 — the in-RAM queue class. In BOTH crate roots (the
// twin-list rule of lib.rs).
mod ephemeral;
mod frames;
mod handlers;
mod httpget;
#[cfg(feature = "server")]
mod proxy_embed;
// The Kafka wire facade run IN-PROCESS (PLAN_SINGLE_BINARY.md W2): with
// `QUEEN_KAFKA_EMBEDDED=true` the queen-kafka library runs inside this process on
// its own runtime, calling the broker through the router — no child.
#[cfg(feature = "kafka")]
mod kafka_inproc;
mod metrics;
mod notify;
mod obs;
// The broker→broker forwarding client. Twin of the `mod peerclient;` in lib.rs
// (the twin-list rule of lib.rs's header).
mod peerclient;
mod quota;
// The replicated state machine: the broker's storage. Twin of the `mod rsm;`
// in lib.rs (the twin-list rule of lib.rs's header).
#[allow(dead_code)]
mod rsm;
mod switches;
mod syscollect;
mod tenant;
mod util;

/// Broker version, embedded from server.json at build time (see build.rs). Single
/// source of truth shared with the Docker image tag (build.sh) and /health.
pub const VERSION: &str = env!("QUEEN_VERSION");

/// Memory hunts only (`--features jemalloc-prof`): jemalloc with its heap
/// profiler as the process allocator. `MALLOC_CONF=prof:true,...` turns the
/// profiler on at start; the default build keeps the system allocator.
#[cfg(feature = "jemalloc-prof")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// The process allocator: mimalloc. A push's payload is allocated on an HTTP
/// worker and freed on the planner's lane threads; glibc malloc takes the
/// allocating arena's lock for every such free, and at a few hundred
/// thousand messages a second the lanes spent most of their time blocked on
/// it (measured 2026-09-23: 62% of a lane's wall off-CPU in `cfree`).
/// mimalloc hands a cross-thread free back without a lock.
#[cfg(not(feature = "jemalloc-prof"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// The accept backlog asked of the kernel, which caps it at
/// `net.core.somaxconn` (4096 on current kernels).
const LISTEN_BACKLOG: u32 = 4096;

/// Bind an HTTP listener with a real accept backlog. `TcpListener::bind` asks
/// for 128 (mio copies the standard library's value; `ss -ltn` on the VM showed
/// `LISTEN 0 128`), and a burst of reconnects overflows that: the kernel drops
/// the handshakes and the clients stall on retransmits for seconds (measured
/// 2026-09-23, 2.3M `ListenOverflows` on the benchmark broker).
async fn bind_listener(addr: &str) -> std::io::Result<tokio::net::TcpListener> {
    let mut last = None;
    for sa in tokio::net::lookup_host(addr).await? {
        let sock = if sa.is_ipv4() {
            tokio::net::TcpSocket::new_v4()?
        } else {
            tokio::net::TcpSocket::new_v6()?
        };
        sock.set_reuseaddr(true)?;
        match sock.bind(sa) {
            Ok(()) => return sock.listen(LISTEN_BACKLOG),
            Err(e) => last = Some(e),
        }
    }
    Err(last.unwrap_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "no address to bind")
    }))
}

#[tokio::main]
async fn main() {
    // LOGGING_PLAN.md Phase 0: install the tracing subscriber (honours
    // LOG_LEVEL/RUST_LOG) and the panic hook BEFORE anything can log or panic.
    obs::init();
    obs::install_panic_hook();
    run_raft(config::load()).await;
}

/// The broker boot: auth, the replicated state machine, the router (the
/// proxy in front of it when `QUEEN_PROXY_EMBEDDED=true`), the in-process Kafka
/// facade when enabled, and the listeners. Shutdown hands leadership off first.
async fn run_raft(cfg: config::Config) {
    // Crash points (§13.5), off unless QUEEN_TEST_FAULTS is set. Read once here,
    // before any command can reach a fault point (test/raft/crash arms them).
    rsm::faults::init_from_env();

    // Auth (fail fast on bad key material).
    if let Err(e) = cfg.auth.validate() {
        obs::fatal(format!("invalid JWT auth configuration: {e}"));
    }
    let authenticator = auth::Authenticator::new(cfg.auth.clone());
    if cfg.auth.enabled && authenticator.uses_jwks() {
        match authenticator.fetch_jwks().await {
            Ok(n) => tracing::info!(target: "auth", keys = n, "JWKS pre-fetch OK"),
            Err(e) => {
                tracing::warn!(target: "auth", error = %e, "JWKS pre-fetch failed (will retry on demand)")
            }
        }
        let a = authenticator.clone();
        let interval = authenticator.jwks_refresh_interval();
        tokio::spawn(async move {
            static JWKS_FAIL: obs::Sampler = obs::Sampler::new(60_000);
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = a.fetch_jwks().await {
                    if let Some(suppressed) = JWKS_FAIL.tick_now() {
                        tracing::warn!(target: "auth", error = %e, suppressed, "JWKS refresh failed");
                    }
                }
            }
        });
    }

    config::log_effective(&cfg);

    // §11.1 — the data directory is required. Create it now; the state machine
    // refuses to open a directory it cannot use.
    if cfg.raft_dir.trim().is_empty() {
        obs::fatal("QUEEN_RAFT_DIR is required (the broker's data directory, §11.1)");
    }
    if let Err(e) = std::fs::create_dir_all(&cfg.raft_dir) {
        tracing::warn!(
            target: "boot",
            dir = %cfg.raft_dir,
            error = %e,
            "could not create the raft data directory"
        );
    }

    // Install the state-machine builder before `build_raft_state` builds the
    // facade. First-write-wins.
    rsm::facade::set_builder(rsm::facade::real::real_builder);

    // The Kafka facade, IN-PROCESS (kafka_inproc.rs). Resolved HERE, before the
    // state machine opens and the listener binds: the one knob with no default
    // (QUEEN_KAFKA_ADVERTISED_ADDR) is unfixable by retrying, so boot dies on it
    // naming the fix.
    #[cfg(feature = "kafka")]
    let kafka_cfg = if cfg.kafka_facade.enabled {
        match kafka_inproc::preflight() {
            Ok(c) => Some(c),
            Err(e) => obs::fatal(format!("QUEEN_KAFKA_EMBEDDED=true: {e}")),
        }
    } else {
        None
    };
    #[cfg(not(feature = "kafka"))]
    if cfg.kafka_facade.enabled {
        tracing::warn!(
            target: "kafka",
            "QUEEN_KAFKA_EMBEDDED=true, but this binary was built without the `kafka` \
             feature: no Kafka listener is started"
        );
    }

    let state = match handlers::raft::build_raft_state(&cfg) {
        Ok(s) => s,
        Err(e) => obs::fatal(format!("raft state init failed: {e}")),
    };

    // The event-loop lag probe and the 1 Hz parked sampler: the dashboard
    // collector flushes them with everything else.
    metrics::spawn_samplers(state.metrics.clone());
    // The ephemeral (RAM) queues' lease/ttl/idle backstop.
    ephemeral::spawn_backstop(state.ephemeral.clone());

    // On SIGTERM a node that leads first hands its leadership to a caught-up
    // peer, and only then does the listener drain: stopping the leader (a
    // rolling restart) costs the cluster one transfer, not an election timeout
    // without a leader (1-2 s). It drains as a follower.
    let handoff_rsm = state.rsm.clone();
    let shutdown = {
        let rsm = handoff_rsm.clone();
        async move {
            obs::shutdown_signal().await;
            rsm.hand_off_leadership(HAND_OFF_WAIT).await;
        }
    };

    // The Kafka facade's own KV calls reach the state machine directly
    // (kafka_inproc.rs), so it keeps its own handles on it and on auth.
    #[cfg(feature = "kafka")]
    let (kafka_rsm, kafka_auth) = (state.rsm.clone(), authenticator.clone());
    // With the proxy on the public port, the facade still reaches the broker
    // router itself, never the proxy's edge: it authenticates its own clients.
    #[cfg(feature = "kafka")]
    let kafka_router = proxy_embed::enabled().then(|| {
        handlers::raft::build_raft_router(state.clone(), authenticator.clone(), cfg.tenancy_header)
    });

    // The single binary (PLAN_SINGLE_BINARY.md W3/W4): the proxy fronts the
    // public port; the broker router behind it has tenancy on, the broker's
    // own JWT off, and no socket — the proxy authenticated the caller. With
    // QUEEN_PROXY_PORT set to another port (proxy_embed::separate_port), the
    // proxy serves THAT port and PORT serves the broker router itself, with the
    // broker's own auth and tenancy: an internal port for clients inside the
    // network, scrapers and probes.
    let mut embedded_proxy = None;
    let mut proxy_front = None;
    let app = if proxy_embed::enabled() {
        let mut inner_auth = cfg.auth.clone();
        inner_auth.enabled = false;
        let inner = handlers::raft::build_raft_router(
            state.clone(),
            auth::Authenticator::new(inner_auth),
            true,
        );
        let (proxy, public) = match proxy_embed::public_router(state.rsm.clone(), inner) {
            Ok(v) => v,
            Err(e) => obs::fatal(format!("single binary: {e}")),
        };
        embedded_proxy = Some(proxy);
        match proxy_embed::separate_port(&cfg.port) {
            Some(port) => {
                proxy_front = Some((port, public));
                handlers::raft::build_raft_router(state, authenticator, cfg.tenancy_header)
            }
            None => {
                tracing::info!(target: "boot", "single binary: proxy in-process on the public port");
                public
            }
        }
    } else {
        handlers::raft::build_raft_router(state, authenticator, cfg.tenancy_header)
    };

    let addr = config::host_port(&cfg.bind_addr, &cfg.port);
    let listener = match bind_listener(&addr).await {
        Ok(l) => l,
        Err(e) => obs::fatal(format!("cannot bind {addr}: {e}")),
    };
    // Bound before anything serves, so a taken proxy port fails the boot the
    // way a taken PORT does.
    let proxy_front = match proxy_front {
        Some((port, public)) => {
            let proxy_addr = config::host_port(&cfg.bind_addr, &port);
            match bind_listener(&proxy_addr).await {
                Ok(l) => {
                    tracing::info!(
                        target: "boot",
                        proxy = %proxy_addr,
                        broker = %addr,
                        "single binary: proxy in-process on its own port; the broker router serves PORT (internal)"
                    );
                    Some((l, public))
                }
                Err(e) => obs::fatal(format!("cannot bind {proxy_addr} (QUEEN_PROXY_PORT): {e}")),
            }
        }
        None => None,
    };
    tracing::info!(
        target: "boot",
        version = VERSION,
        addr = %addr,
        data_dir = %cfg.raft_dir,
        planner_queue_depth = cfg.raft_planner_queue_depth,
        "listening"
    );

    // The Kafka facade starts once the router exists and the HTTP listener is
    // bound; its transport is the broker router (kafka_inproc.rs): a clone of
    // `app`, or its own instance when the proxy fronts the public port.
    #[cfg(feature = "kafka")]
    let kafka = kafka_cfg.map(|k| {
        kafka_inproc::start(
            &cfg.kafka_facade,
            k,
            kafka_router.unwrap_or_else(|| app.clone()),
            &cfg.port,
            &cfg.raft_dir,
            kafka_rsm,
            kafka_auth,
        )
    });

    if let Some((proxy_listener, public)) = proxy_front {
        // Two listeners, one shutdown: the hand-off runs once, then both drain.
        let shutdown = futures_util::FutureExt::shared(shutdown);
        let broker = async {
            if let Err(e) = axum::serve(listener, app)
                .tcp_nodelay(true)
                .with_graceful_shutdown(shutdown.clone())
                .await
            {
                tracing::error!(target: "boot", error = %e, "serve loop ended with error");
            }
        };
        // W7 on the proxy's port only: TLS, connection limits, the edge.
        let proxy = async {
            if let Err(e) = proxy_embed::serve(proxy_listener, public, shutdown.clone()).await {
                obs::fatal(format!("single binary: {e}"));
            }
        };
        tokio::join!(broker, proxy);
    } else if embedded_proxy.is_some() {
        // W7: TLS, connection limits and the peer address for the edge.
        if let Err(e) = proxy_embed::serve(listener, app, shutdown).await {
            obs::fatal(format!("single binary: {e}"));
        }
    } else if let Err(e) = axum::serve(listener, app)
        .tcp_nodelay(true)
        .with_graceful_shutdown(shutdown)
        .await
    {
        tracing::error!(target: "boot", error = %e, "serve loop ended with error");
    }
    // The facade goes down with the broker, and this AWAITS it (bounded by
    // QUEEN_KAFKA_SHUTDOWN_GRACE_MS): a cluster-mode facade hands its registry
    // row back through the router, which still serves after the listener closed.
    #[cfg(feature = "kafka")]
    if let Some(k) = kafka {
        k.shutdown().await;
    }
    // The embedded proxy's open usage minute and pending queue rows.
    if let Some(proxy) = embedded_proxy {
        queen_proxy::app::shutdown_drain(&proxy).await;
    }
    // Leading again after the drain (no peer was caught up at the signal, or
    // a long drain outlived the peers' hold on handing it back): hand off once
    // more before exiting.
    handoff_rsm.hand_off_leadership(HAND_OFF_WAIT).await;
    tracing::info!(target: "shutdown", "shutdown complete");
}

/// How long a node on its way out waits for another node to take its
/// leadership over (`Rsm::hand_off_leadership`); a transfer takes one round
/// trip and a vote.
const HAND_OFF_WAIT: std::time::Duration = std::time::Duration::from_secs(3);
