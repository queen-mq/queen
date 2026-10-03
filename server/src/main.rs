// The twin-list rule of lib.rs: this binary compiles the library's modules
// itself, so what only the library's embedders reach (`queen::Broker`, e.g. the
// ephemeral configure/delete calls) is unused here. Those modules carry
// `#[allow(dead_code)]` in this root; the library target still checks them.
mod auth;
mod config;
mod encryption;
// EPHEMERAL_QUEUES.md §3.2 — the in-RAM queue class. In BOTH crate roots (the
// twin-list rule of lib.rs).
#[allow(dead_code)]
mod ephemeral;
mod frames;
#[allow(dead_code)]
mod handlers;
mod httpget;
#[cfg(feature = "server")]
mod proxy_embed;
// The Kafka wire facade run IN-PROCESS (PLAN_SINGLE_BINARY.md W2): with
// `QUEEN_KAFKA_EMBEDDED=true` the queen-kafka library runs inside this process on
// its own runtime, calling the broker through the router — no child.
#[cfg(feature = "kafka")]
mod kafka_inproc;
#[allow(dead_code)]
mod metrics;
#[allow(dead_code)]
mod notify;
mod obs;
// The broker→broker forwarding client. Twin of the `mod peerclient;` in lib.rs
// (the twin-list rule of lib.rs's header).
mod peerclient;
// The Postgres connectors run IN-PROCESS (PLAN_PG_CONNECTORS.md §6): every node
// runs every connector document on its own runtime, reaching the state machine
// directly — no child, no second binary.
#[cfg(feature = "pg")]
mod pg_inproc;
#[allow(dead_code)]
mod quota;
// The replicated state machine: the broker's storage. Twin of the `mod rsm;`
// in lib.rs (the twin-list rule of lib.rs's header).
#[allow(dead_code)]
mod rsm;
// The S3 / data-lake sink run IN-PROCESS: with `QUEEN_S3_EMBEDDED=true` the
// queen-s3 library runs inside this process on its own runtime, on every node,
// reading this node's own state — no child, no second binary.
#[cfg(feature = "s3")]
mod s3_inproc;
#[allow(dead_code)]
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
    #[cfg(target_os = "linux")]
    let thp_off = transparent_huge_pages_off();
    // LOGGING_PLAN.md Phase 0: install the tracing subscriber (honours
    // LOG_LEVEL/RUST_LOG) and the panic hook BEFORE anything can log or panic.
    obs::init();
    obs::install_panic_hook();
    #[cfg(target_os = "linux")]
    if thp_off {
        tracing::info!(target: "boot", "transparent huge pages off for this process");
    }
    run_raft(config::load()).await;
}

/// Transparent huge pages off for this process. mimalloc asks for them on its
/// arenas (`madvise(MADV_HUGEPAGE)`), and once the page cache has taken the
/// free memory the kernel compacts on the fault path to make one: on three
/// nodes at 480-600k msg/s every node stalled together for seconds, and a
/// 10-minute soak did not recover; with them off the same soak ran clean
/// (2026-09-28). The flag is the process's (`PR_SET_THP_DISABLE`), so it holds
/// for memory mapped before this call too. `MIMALLOC_ALLOW_THP=1` keeps them.
#[cfg(target_os = "linux")]
fn transparent_huge_pages_off() -> bool {
    let keep = std::env::var("MIMALLOC_ALLOW_THP").is_ok_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "on" | "yes"
        )
    });
    // SAFETY: PR_SET_THP_DISABLE reads no memory; the unused arguments must
    // be zero, passed at the width the kernel reads them.
    !keep
        && unsafe {
            libc::prctl(
                libc::PR_SET_THP_DISABLE,
                1 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
            )
        } == 0
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
    // The S3 sink, IN-PROCESS (s3_inproc.rs). Resolved here for the same
    // reason: the variables with no default (bucket, endpoint, region, queues,
    // keypair) are unfixable by retrying, so boot dies naming the one missing.
    #[cfg(feature = "s3")]
    let s3_cfg = if cfg.s3_sink.enabled {
        match s3_inproc::preflight() {
            Ok(c) => Some(c),
            Err(e) => obs::fatal(format!("QUEEN_S3_EMBEDDED=true: {e}")),
        }
    } else {
        None
    };
    #[cfg(not(feature = "s3"))]
    if cfg.s3_sink.enabled {
        tracing::warn!(
            target: "queen-s3",
            "QUEEN_S3_EMBEDDED=true, but this binary was built without the `s3` \
             feature: no sink is started"
        );
    }

    // The Postgres connectors (pg_inproc.rs): their node knobs (QUEEN_PG_*) are
    // read HERE, strictly, whether or not this node runs the manager — a value
    // out of range is unfixable by retrying, and the connectors API reads the
    // egress policy from them on every node.
    #[cfg(feature = "pg")]
    let pg_knobs = match pg_inproc::preflight(&cfg.server_id, proxy_embed::enabled()) {
        Ok(k) => k,
        Err(e) => obs::fatal(format!("Postgres connectors: {e}")),
    };
    #[cfg(not(feature = "pg"))]
    if cfg.pg_connectors.requested {
        tracing::warn!(
            target: "queen-pg",
            "QUEEN_PG_CONNECTORS=true, but this binary was built without the `pg` feature: \
             no connector runs and /api/v1/connectors is not served"
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
    // Ephemeral partitions across the raft nodes: placement over the members'
    // view, hand-over when ownership moves, control rows converged each second.
    handlers::spawn_ephemeral_placement(state.clone());

    // On SIGTERM a node that leads first hands its leadership to a caught-up
    // peer, and only then does the listener drain: stopping the leader (a
    // rolling restart) costs the cluster one transfer, not an election timeout
    // without a leader (1-2 s). It drains as a follower.
    let handoff_rsm = state.rsm.clone();
    // The S3 sink (s3_inproc.rs) starts later, once the listener is bound; it
    // is put here so the signal can start its drain at once.
    #[cfg(feature = "s3")]
    let s3_at_signal: std::sync::Arc<
        std::sync::OnceLock<std::sync::Arc<s3_inproc::InProcess>>,
    > = std::sync::Arc::new(std::sync::OnceLock::new());
    // The Postgres connectors start later, once the listener is bound; they
    // are put here so the signal can start their drain at once.
    #[cfg(feature = "pg")]
    let pg_at_signal: std::sync::Arc<
        std::sync::OnceLock<std::sync::Arc<pg_inproc::InProcess>>,
    > = std::sync::Arc::new(std::sync::OnceLock::new());
    let shutdown = {
        let rsm = handoff_rsm.clone();
        let st = state.clone();
        #[cfg(feature = "s3")]
        let s3_at_signal = std::sync::Arc::clone(&s3_at_signal);
        #[cfg(feature = "pg")]
        let pg_at_signal = std::sync::Arc::clone(&pg_at_signal);
        async move {
            obs::shutdown_signal().await;
            // The sink drains beside everything below — no new reads, the
            // window in flight committed, its leases given back — rather than
            // after the ephemeral drain and the hand-off, which can take tens
            // of seconds between them.
            #[cfg(feature = "s3")]
            if let Some(s3) = s3_at_signal.get() {
                s3.begin_shutdown();
            }
            // The connectors drain beside everything below — a source flushes
            // the bundle in flight and gives its lease back, a sink finishes
            // its batch — rather than after the ephemeral drain and the
            // hand-off, which can take seconds between them.
            #[cfg(feature = "pg")]
            if let Some(pg) = pg_at_signal.get() {
                pg.begin_shutdown();
            }
            // The ephemeral rings go to their next owners first, while the
            // peers still answer and this node still serves.
            handlers::ephemeral_drain(&st).await;
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
    // The S3 sink reads and writes through the state machine itself
    // (s3_inproc.rs); the broker router is its route for a KV batch on a
    // follower that does not take writes. Never the proxy's edge, like the
    // facade's.
    #[cfg(feature = "s3")]
    let s3_rsm = state.rsm.clone();
    #[cfg(feature = "s3")]
    let s3_router = proxy_embed::enabled().then(|| {
        handlers::raft::build_raft_router(state.clone(), authenticator.clone(), cfg.tenancy_header)
    });
    // The connectors reach the state machine directly (pg_inproc.rs): no
    // router, no auth, no tenancy header in between — each acts as its own
    // tenant.
    #[cfg(feature = "pg")]
    let pg_rsm = state.rsm.clone();

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
        let (proxy, public) = match proxy_embed::public_router(state.rsm.clone(), inner.clone()) {
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
                // The other brokers relay ephemeral requests to PORT: theirs
                // go to the broker router, everyone else's to the proxy.
                proxy_embed::with_peer_relays(public, inner)
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

    // The S3 sink starts once the router exists and the listener is bound, on
    // every node; its queues' leases decide which node writes which queue. The
    // signal starts its drain through `s3_at_signal` (above).
    #[cfg(feature = "s3")]
    let s3 = s3_cfg.map(|c| {
        let s3 = s3_inproc::start(
            &cfg.s3_sink,
            c,
            s3_router.unwrap_or_else(|| app.clone()),
            s3_rsm,
            None,
        );
        let _ = s3_at_signal.set(std::sync::Arc::clone(&s3));
        s3
    });

    // The Postgres connectors start once the listener is bound, on every node;
    // a source's lease decides which node streams it, a sink's consumer group
    // spreads its work. The signal starts their drain through `pg_at_signal`.
    #[cfg(feature = "pg")]
    let pg = if cfg.pg_connectors.enabled {
        let pg = pg_inproc::start(pg_knobs, pg_rsm);
        let _ = pg_at_signal.set(std::sync::Arc::clone(&pg));
        Some(pg)
    } else {
        tracing::info!(
            target: "queen-pg",
            "QUEEN_PG_CONNECTORS=false: this node runs no connector (the API still stores \
             documents for the nodes that do)"
        );
        None
    };

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
    // The sink has been draining since the signal; this waits for what is
    // left of QUEEN_S3_SHUTDOWN_GRACE_MS, counted from the signal. Its KV
    // commits reach the state machine directly, so the closed listener does
    // not stop them.
    #[cfg(feature = "s3")]
    if let Some(s3) = s3 {
        s3.shutdown().await;
    }
    // The connectors have been draining since the signal; this waits for what
    // is left of QUEEN_PG_SHUTDOWN_GRACE_MS, counted from the signal. Their
    // calls reach the state machine directly, so the closed listener does not
    // stop them.
    #[cfg(feature = "pg")]
    if let Some(pg) = pg {
        pg.shutdown().await;
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
