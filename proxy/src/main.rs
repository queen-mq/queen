//! queen-proxy — multi-tenant data-plane gateway for QueenMQ cells.
//! Spec: PLAN_QUEEN_PROXY_CLOUD.md (repo root). Module ownership: CONTRACTS.md.

mod acting;
mod auth;
mod cache;
mod config;
mod console;
mod db;
mod errors;
mod gateway;
mod httpget;
mod limits;
mod meter;
mod oauth;
mod obs;
mod pgtls;
mod registry;
mod routes;
mod spool;
mod state;
mod webapp;

use std::sync::Arc;

use axum::routing::get;
use axum::Router;
use base64::engine::general_purpose::STANDARD as B64_STD;
use base64::Engine;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::state::{AppState, St};

/// How long the TLS listener lets live connections finish their in-flight
/// request after the shutdown signal. The plaintext listener gets the
/// equivalent from `axum::serve`'s own graceful shutdown.
const TLS_SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

/// Tokio worker-thread count. `#[tokio::main]` defaults to the HOST core count,
/// but a shared free cell runs the proxy inside a CPU-capped cgroup (e.g. 2
/// cores of an 8-core box): spawning 8 workers onto 2 cores oversubscribes them
/// 4×, and the scheduler contention showed up as a very low IPC (0.41) and a
/// scheduler-heavy profile on the bench VM. Size the pool to the cell's actual
/// CPU budget: the cgroup-v2 `cpu.max` quota if one is set, an explicit
/// `QUEEN_PROXY_WORKER_THREADS` override if given, else the host core count (the
/// old behaviour, correct for an uncapped/dedicated cell).
fn chosen_worker_threads() -> usize {
    let env = std::env::var("QUEEN_PROXY_WORKER_THREADS").ok();
    let cpu_max = std::fs::read_to_string("/sys/fs/cgroup/cpu.max").ok();
    let hostn = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    resolve_worker_threads(env.as_deref(), cpu_max.as_deref(), hostn)
}

/// Pure worker-count policy (testable): explicit env override wins; else the
/// cgroup-v2 `cpu.max` quota ("<quota> <period>" µs, or "max <period>") rounded
/// up and clamped to [1, host]; else the host core count.
fn resolve_worker_threads(env: Option<&str>, cpu_max: Option<&str>, host: usize) -> usize {
    if let Some(v) = env {
        if let Ok(n) = v.trim().parse::<usize>() {
            if n >= 1 {
                return n;
            }
        }
    }
    if let Some(s) = cpu_max {
        let mut it = s.split_whitespace();
        if let (Some(q), Some(p)) = (it.next(), it.next()) {
            if q != "max" {
                if let (Ok(q), Ok(p)) = (q.parse::<u64>(), p.parse::<u64>()) {
                    if p > 0 {
                        return (((q + p - 1) / p) as usize).clamp(1, host.max(1));
                    }
                }
            }
        }
    }
    host.max(1)
}

#[cfg(test)]
mod worker_thread_tests {
    use super::resolve_worker_threads;
    #[test]
    fn env_override_wins() {
        assert_eq!(resolve_worker_threads(Some("3"), Some("200000 100000"), 8), 3);
        assert_eq!(resolve_worker_threads(Some(" 1 "), None, 8), 1);
        // junk / zero env falls through to the next source
        assert_eq!(resolve_worker_threads(Some("0"), Some("200000 100000"), 8), 2);
        assert_eq!(resolve_worker_threads(Some("x"), None, 8), 8);
    }
    #[test]
    fn cgroup_quota_rounds_up_and_clamps() {
        assert_eq!(resolve_worker_threads(None, Some("200000 100000"), 8), 2); // 2 cores
        assert_eq!(resolve_worker_threads(None, Some("250000 100000"), 8), 3); // 2.5 -> 3
        assert_eq!(resolve_worker_threads(None, Some("50000 100000"), 8), 1); // 0.5 -> 1
        assert_eq!(resolve_worker_threads(None, Some("max 100000"), 8), 8); // uncapped -> host
        assert_eq!(resolve_worker_threads(None, Some("1600000 100000"), 8), 8); // clamp to host
    }
    #[test]
    fn no_signal_uses_host() {
        assert_eq!(resolve_worker_threads(None, None, 4), 4);
        assert_eq!(resolve_worker_threads(None, None, 0), 1);
    }
}

fn main() {
    let workers = chosen_worker_threads();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("build tokio runtime");
    rt.block_on(async_main(workers));
}

async fn async_main(worker_threads: usize) {
    obs::init();
    tracing::info!(worker_threads, "queen-proxy starting");
    let cfg = config::Config::load();

    let db = match &cfg.pxdb {
        Some(pxcfg) => {
            // Before the connect, so a cell that fails to reach its pxdb still
            // says on what terms it was trying to. `ssl_trust` is one of
            // plaintext / webpki-roots / supplied-ca / accept-any, and
            // `ssl_verified` is the single boolean a security review wants:
            // the broker prints the identical pair for its own PostgreSQL.
            tracing::info!(
                target: "config",
                host = %pxcfg.host,
                port = pxcfg.port,
                database = %pxcfg.dbname,
                use_ssl = pxcfg.use_ssl,
                ssl_trust = pxcfg.trust_label(),
                ssl_verified = pxcfg.link_authenticated(),
                "config: pxdb"
            );
            match db::create_pool(pxcfg).await {
                Ok(pool) => {
                    if let Err(e) = db::apply_migrations(&pool).await {
                        tracing::error!("migrations failed: {e}");
                        std::process::exit(1);
                    }
                    Some(pool)
                }
                Err(e) => {
                    tracing::error!("pxdb unavailable: {e}");
                    std::process::exit(1);
                }
            }
        }
        None => {
            if cfg.dev_static.is_none() {
                tracing::error!(
                    "no PXDB_HOST and no QUEEN_PROXY_DEV_CELL_URL — nothing to serve"
                );
                std::process::exit(1);
            }
            tracing::warn!("running in dev-static mode (no pxdb)");
            None
        }
    };

    // Upstream connector to the cell broker. TCP_NODELAY on: the proxy relays
    // small request/response bodies (a push item, a pop batch) and Nagle would
    // otherwise sit on them for up to ~40ms waiting to coalesce, adding tail
    // latency for zero benefit on a keep-alive, one-request-in-flight pool.
    let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
    connector.set_nodelay(true);
    let upstream = hyper_util::client::legacy::Client::builder(
        hyper_util::rt::TokioExecutor::new(),
    )
    // Cap idle connections retained per host: the pool is reused across requests
    // (verified ~0 new-connections/s under load), so a modest ceiling keeps the
    // idle set bounded without churning live traffic.
    .pool_max_idle_per_host(64)
    .build::<_, axum::body::Body>(connector);

    let cache = cache::ClusterCache::new(&cfg, db.clone());
    let limits = limits::Limits::new(&cfg);
    let meter = Arc::new(meter::Meter::new(&cfg));
    meter.spawn_flush(db.clone());
    let registry = registry::Registry::new(db.clone());
    let keys = auth::Keys::from_config(&cfg);

    // Identity material that WAS supplied must be able to serve the mode it is
    // in, or the process stops here — alongside the pxdb and TLS gates above and
    // below, and for the same reason. A proxy with a pxdb and no signer boots,
    // passes every health check, serves the whole API-key data plane, and
    // answers 500 to every console login; the first cloud cell shipped exactly
    // that. Material that was never supplied is the API-key-only proxy and is
    // WARNED about instead of refused, so `deploy/proxy.mdx`'s "PXDB_HOST is the
    // only variable it refuses to start without" stays true. `jwt_boot`
    // classifies; the policy and the messages live there, pinned by tests.
    {
        let boot = config::jwt_boot(config::JwtMaterial {
            has_pxdb: cfg.pxdb.is_some(),
            // Same emptiness rule the signer applies, so the mode the gate names
            // is the mode auth::Keys actually built.
            ed_private: cfg.jwt_ed25519_pem.as_deref().is_some_and(|s| !s.trim().is_empty()),
            ed_public: config::jwt_ed25519_pub_pem().is_some(),
            hs_secret: cfg.jwt_hs_secret.as_deref().is_some_and(|s| !s.trim().is_empty()),
            can_mint: keys.can_mint(),
            can_verify: keys.can_verify(),
        });
        if let Some(w) = &boot.warn {
            tracing::warn!(target: "auth", "{w}");
        }
        match &boot.fatal {
            Some(e) => {
                tracing::error!(target: "auth", mode = boot.mode.as_str(), "{e}");
                std::process::exit(1);
            }
            // The audience rides on this line because it is otherwise
            // invisible: a cell whose `aud` does not match what the control
            // plane stamps rejects every fleet token, and a cell with none
            // configured accepts every sibling's. Both are worth being able to
            // read off a boot log.
            None => tracing::info!(
                target: "auth",
                mode = boot.mode.as_str(),
                mint = keys.can_mint(),
                verify = keys.can_verify(),
                aud = cfg.jwt_audience.as_deref().unwrap_or("<any>"),
                "jwt identity"
            ),
        }
    }

    let st: St = Arc::new(AppState {
        cfg,
        db,
        upstream,
        cache,
        limits,
        meter,
        registry,
        keys,
    });

    st.cache.spawn_listener();
    // Batched api_keys.last_used_at: one statement per interval, off the
    // request path.
    st.cache.spawn_touch_flush();
    st.registry.spawn_reconciler();
    // Queue rows admitted on the data path, coalesced per (cluster, queue) and
    // written once per tick -- never awaited by a push.
    st.registry.spawn_persister();
    // Deny-list GC: drops revoked_tokens rows past their own exp.
    auth::spawn_revocation_sweep(st.clone());
    // Daily usage rollup (usage_minutes -> usage_days) + monthly quota checks.
    meter::spawn_rollup(st.clone());

    // Storage-quota pump: over_storage (registry reconciler, from the broker's
    // retainedBytes) -> limits.set_push_blocked, with release when back under.
    {
        let st2 = st.clone();
        tokio::spawn(async move {
            let mut blocked: std::collections::HashSet<uuid::Uuid> = Default::default();
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                let now: std::collections::HashSet<uuid::Uuid> =
                    st2.registry.over_storage().into_iter().collect();
                for id in now.difference(&blocked) {
                    tracing::warn!(target: "limits", cluster = %id, "storage quota exceeded; pushes blocked");
                    st2.limits.set_push_blocked(*id, true);
                }
                for id in blocked.difference(&now) {
                    tracing::info!(target: "limits", cluster = %id, "storage back under quota; pushes unblocked");
                    st2.limits.set_push_blocked(*id, false);
                }
                blocked = now;
            }
        });
    }

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/.well-known/jwks.json", get(jwks))
        .nest("/auth", oauth::router())
        .nest("/api/console", console::router())
        // three routes, not a bare wildcard: /console/*path alone does not
        // match "/console/" (empty remainder) under axum 0.7's matchit
        .route("/console", get(console::spa))
        .route("/console/", get(console::spa))
        .route("/console/*path", get(console::spa))
        // Everything else: broker-bound paths to the data-plane pipeline, the
        // rest to the auth-gated dashboard at `/`. The fallback used to be
        // `gateway::handle` alone, which sent unknown paths upstream and let
        // the BROKER's own embedded webapp answer — unauthenticated and
        // tenant-unaware. See webapp.rs.
        .fallback(webapp::route_fallback)
        .with_state(st.clone());

    // OPTIONAL HTTPS (PLAN §11 Phase 1 "TLS (CF origin)"): only when both
    // QUEEN_PROXY_TLS_CERT and QUEEN_PROXY_TLS_KEY are set. Resolved and parsed
    // before the bind so unusable material fails fast instead of after the port
    // is taken; unset leaves the plaintext path below untouched.
    let tls = match config::tls_material() {
        Ok(Some(m)) => match tls_server_config(&m) {
            Ok(cfg) => {
                tracing::info!(cert = %m.cert_path, "TLS listener enabled (rustls/ring)");
                Some(cfg)
            }
            Err(e) => {
                tracing::error!(cert = %m.cert_path, key = %m.key_path, "TLS material unusable: {e}");
                std::process::exit(1);
            }
        },
        Ok(None) => None,
        Err(e) => {
            tracing::error!("{e}");
            std::process::exit(1);
        }
    };

    let addr = config::host_port(&st.cfg.bind_addr, st.cfg.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
    tracing::info!(
        addr,
        enforce = st.cfg.enforce,
        dev_static = st.cfg.dev_static.is_some(),
        // Named, not counted: an operator debugging "why does my host 401"
        // needs to see whether the name they configured is the name a client
        // actually sends. These are public DNS labels, not secrets.
        shared_hosts = ?st.cfg.shared_hosts,
        "queen-proxy up"
    );
    match tls {
        Some(tls_cfg) => serve_tls(listener, app, tls_cfg).await,
        None => axum::serve(listener, app)
            .with_graceful_shutdown(obs::shutdown_signal())
            .await
            .expect("serve"),
    }

    // Past this point the listener is closed. Metering and the registry's
    // pending queue rows are still in memory: flush them before the process
    // goes away, or the open minute is silently lost on every restart.
    drain_usage(&st).await;
}

/// Flush the metering accumulators and the registry's pending queue rows on
/// the way out. `Meter::drain` spools to disk when pxdb will not take the
/// rows, so the bound here is purely about not hanging on a dead pxdb — the
/// spool, not this timeout, is what keeps the usage (recovered by
/// `spawn_flush` on the next start). A queue row that misses the bound is a
/// restart-safety floor the reconciler rewrites on its next pass.
async fn drain_usage(st: &St) {
    let budget = config::shutdown_drain_budget();
    let started = std::time::Instant::now();
    let drains = async {
        tokio::join!(st.meter.drain(), st.registry.drain());
    };
    match tokio::time::timeout(budget, drains).await {
        Ok(()) => tracing::info!(
            target: "meter",
            ms = started.elapsed().as_millis() as u64,
            "usage and queue rows drained at shutdown"
        ),
        Err(_) => tracing::warn!(
            target: "meter",
            budget_ms = budget.as_millis() as u64,
            "usage drain exceeded its shutdown budget; exiting anyway"
        ),
    }
}

// ---------------------------------------------------------------------------
// Optional TLS listener
// ---------------------------------------------------------------------------

/// Serve HTTPS on an already-bound TCP listener. axum 0.7's `axum::serve` takes
/// a plain `TcpListener`, so the TLS path drives hyper directly: accept ->
/// rustls handshake -> one hyper http1 connection per stream, with the Router
/// as the service.
///
/// Shutdown mirrors the plaintext path: stop accepting, ask live connections to
/// finish their in-flight request (`graceful_shutdown`), then wait at most
/// TLS_SHUTDOWN_GRACE before moving on to the usage drain.
async fn serve_tls(listener: tokio::net::TcpListener, app: Router, cfg: rustls::ServerConfig) {
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(cfg));
    let (shut_tx, shut_rx) = tokio::sync::watch::channel(false);
    // Liveness handle: every connection task holds a clone, so `recv` returns
    // None exactly when the last connection is done.
    let (live, mut live_done) = tokio::sync::mpsc::channel::<()>(1);
    let mut signal = std::pin::pin!(obs::shutdown_signal());

    loop {
        let accepted = tokio::select! {
            _ = &mut signal => break,
            a = listener.accept() => a,
        };
        let (stream, peer) = match accepted {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(target: "tls", err = %e, "accept failed");
                continue;
            }
        };
        let acceptor = acceptor.clone();
        let svc = hyper_util::service::TowerToHyperService::new(app.clone());
        let mut shut_rx = shut_rx.clone();
        let live = live.clone();
        tokio::spawn(async move {
            let _live = live;
            let tls_stream = match acceptor.accept(stream).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::debug!(target: "tls", peer = %peer, err = %e, "handshake failed");
                    return;
                }
            };
            let conn = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(tls_stream), svc)
                .with_upgrades();
            tokio::pin!(conn);
            // `if !asked` disables the branch once signalled: watch::changed()
            // returns immediately (Err) after the sender drops, and re-selecting
            // on it would spin.
            let mut asked = false;
            loop {
                tokio::select! {
                    res = conn.as_mut() => {
                        if let Err(e) = res {
                            tracing::debug!(target: "tls", peer = %peer, err = %e, "connection ended");
                        }
                        break;
                    }
                    _ = shut_rx.changed(), if !asked => {
                        asked = true;
                        conn.as_mut().graceful_shutdown();
                    }
                }
            }
        });
    }

    let _ = shut_tx.send(true);
    drop(live);
    if tokio::time::timeout(TLS_SHUTDOWN_GRACE, live_done.recv()).await.is_err() {
        tracing::warn!(target: "tls", "connections still live after the shutdown grace; closing anyway");
    }
}

/// rustls `ServerConfig` from PEM files, on the ring provider — the same stack
/// pgtls.rs uses outbound, so the origin leg adds no new crypto backend.
fn tls_server_config(m: &config::TlsMaterial) -> Result<rustls::ServerConfig, String> {
    let cert_pem = std::fs::read_to_string(&m.cert_path).map_err(|e| format!("read cert: {e}"))?;
    let key_pem = std::fs::read_to_string(&m.key_path).map_err(|e| format!("read key: {e}"))?;
    let chain = pem_certificates(&cert_pem)?;
    let key = pem_private_key(&key_pem)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut cfg = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|e| e.to_string())?;
    // The listener speaks HTTP/1.1 only (hyper's http1 server is what this
    // crate builds), so advertise exactly that rather than letting a client
    // negotiate h2 and fail.
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(cfg)
}

/// `(label, DER)` for every well-formed PEM block. Hand-rolled rather than
/// adding rustls-pemfile: base64 and rustls::pki_types are already in the tree
/// and this has to understand exactly four labels.
fn pem_blocks(pem: &str) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let mut label: Option<String> = None;
    let mut body = String::new();
    for line in pem.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("-----BEGIN ") {
            label = rest.strip_suffix("-----").map(str::to_string);
            body.clear();
        } else if line.starts_with("-----END ") {
            if let Some(l) = label.take() {
                if let Ok(der) = B64_STD.decode(body.as_bytes()) {
                    out.push((l, der));
                }
            }
            body.clear();
        } else if label.is_some() {
            body.push_str(line);
        }
    }
    out
}

/// The full chain: leaf first, then any intermediates (a CF origin cert ships
/// as a single block, but a chain file must not be truncated to its first).
fn pem_certificates(pem: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let chain: Vec<CertificateDer<'static>> = pem_blocks(pem)
        .into_iter()
        .filter(|(label, _)| label == "CERTIFICATE")
        .map(|(_, der)| CertificateDer::from(der))
        .collect();
    if chain.is_empty() {
        return Err("no CERTIFICATE block found".to_string());
    }
    Ok(chain)
}

/// First private key in the file, in any of the three encodings openssl emits.
fn pem_private_key(pem: &str) -> Result<PrivateKeyDer<'static>, String> {
    for (label, der) in pem_blocks(pem) {
        match label.as_str() {
            "PRIVATE KEY" => return Ok(PrivateKeyDer::Pkcs8(der.into())),
            "RSA PRIVATE KEY" => return Ok(PrivateKeyDer::Pkcs1(der.into())),
            "EC PRIVATE KEY" => return Ok(PrivateKeyDer::Sec1(der.into())),
            _ => {}
        }
    }
    Err("no PRIVATE KEY, RSA PRIVATE KEY or EC PRIVATE KEY block found".to_string())
}

/// Liveness plus the two switches that silently change what this proxy DOES.
///
/// Enforcement and tenant-header injection were previously observable only from
/// the boot log, which survives a restart: a harness grepping the log file for
/// `enforce=true` happily passes against a proxy that has since been replaced
/// by a shadow-mode one, and a limiter test then asserts nothing at all. State
/// a cell can be asked about beats state that has to be inferred.
async fn healthz(axum::extract::State(st): axum::extract::State<St>) -> axum::response::Response {
    let body = serde_json::json!({
        "status": "ok",
        "enforce": st.limits.enforcing(),
        "tenant_header": st.cfg.send_tenant_header,
        // Third switch that silently changes what this proxy DOES: on a shared
        // host the cluster comes from the credential, not from Host. A COUNT,
        // not the list — this endpoint is unauthenticated, and a harness only
        // needs to know whether the feature is on.
        "shared_hosts": st.cfg.shared_hosts.len(),
    });
    let mut resp = axum::response::IntoResponse::into_response(body.to_string());
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    resp
}

async fn jwks(axum::extract::State(st): axum::extract::State<St>) -> axum::response::Response {
    let mut resp = axum::response::IntoResponse::into_response(st.keys.jwks_json());
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    resp
}
