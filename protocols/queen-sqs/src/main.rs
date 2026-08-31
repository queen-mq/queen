//! The binary: read the config, build the facade, serve until told to stop.
//!
//! CONTRACT. Everything in this file is process lifecycle and nothing else — no
//! protocol, no policy. The order below is fixed and each step exists because
//! skipping it is a failure an operator cannot diagnose from a client:
//!
//!   1. `Config::from_env`, which is the ONLY environment read in the process.
//!      It also owns the one refusal that cannot be a warning: a `sigv4`
//!      listener with an empty credential directory, which could answer nothing
//!      but `InvalidClientTokenId` and would read to every client as its own
//!      misconfiguration.
//!   2. Say out loud, once, each thing that is legal and dangerous: an
//!      `auth=off` listener (and louder when it is not bound to loopback), a
//!      GENERATED receipt-handle secret, a receive mode the broker cannot serve.
//!      None of these is fatal — a laptop, a rig and a single-replica deployment
//!      are all real — and none of them may be quiet.
//!   3. Say, in ONE boot line, what the deployment actually is: the listener, the
//!      auth mode, the receive mode, the region/account pair that appears in
//!      every queue URL, and whether the receipt-handle secret was CONFIGURED or
//!      GENERATED. The last one is load-bearing: a generated secret is correct
//!      for a single instance and silently breaks the day a second replica is
//!      added, so it is said out loud at every boot rather than discovered as
//!      `ReceiptHandleIsInvalid` under a load balancer.
//!   4. Serve, plain or TLS.
//!   5. On SIGTERM, stop accepting and let in-flight requests finish inside
//!      `QUEEN_SQS_SHUTDOWN_GRACE_MS`. A long poll is a request that is doing its
//!      job while looking idle, so the grace has to outlive one.
//!
//! EMBEDDED MODE (`QUEEN_SQS_EMBEDDED=true`) is the broker's side of the same
//! contract, not this file's: the broker spawns this binary as a supervised
//! child on loopback, and the secrets are stripped from the child's environment
//! after `Config::from_env` has read them — which is why nothing below may read
//! the environment again.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use base64::engine::general_purpose::STANDARD as B64_STD;
use base64::Engine as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;

use queen_sqs::config::{AuthMode, Config};
use queen_sqs::{queen, router, Facade};

#[tokio::main]
async fn main() {
    init_tracing();

    let config = match Config::from_env() {
        Ok(config) => config,
        Err(e) => {
            tracing::error!(target: "boot", "FATAL: {e}");
            std::process::exit(1);
        }
    };
    for warning in warnings(&config) {
        tracing::warn!(target: "boot", "{warning}");
    }

    // Parsed HERE and not on the first connection: a certificate that does not
    // match its key must fail the boot rather than every client.
    let tls = match &config.tls {
        None => None,
        Some(material) => match tls_config(&material.cert_path, &material.key_path) {
            Ok(server_config) => Some(server_config),
            Err(e) => {
                tracing::error!(target: "boot", "FATAL: {e}");
                std::process::exit(1);
            }
        },
    };

    // The one line an operator compares against what their clients are
    // configured with. The token and the handle secret are not in it — only
    // whether they exist, and where the handle secret came from.
    tracing::info!(
        target: "boot",
        listen = %config.listen,
        queen_url = %config.queen_url,
        authenticated = config.queen_token.is_some(),
        auth = %config.auth.as_str(),
        principals = config.credentials.len(),
        receive_mode = %config.receive_mode.as_str(),
        default_partitions = config.default_partitions,
        endpoint = %format_args!("{}://<host>/{}/<queue>", config.scheme(), config.account),
        region = %config.region,
        account = %config.account,
        handle_secret = %if config.handle_secret_generated { "generated" } else { "configured" },
        tls = tls.is_some(),
        embedded = config.embedded,
        shutdown_grace_ms = config.shutdown_grace_ms,
        "queen-sqs starting"
    );

    let api = match queen::HttpQueen::new(&config.queen_url) {
        Ok(api) => Arc::new(api),
        Err(e) => {
            tracing::error!(target: "boot", "FATAL: {e}");
            std::process::exit(1);
        }
    };

    let listen = config.listen.clone();
    let grace = Duration::from_millis(config.shutdown_grace_ms);
    let app = router(Arc::new(Facade::new(config, api)));

    let listener = match TcpListener::bind(&listen).await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::error!(target: "boot", "FATAL: cannot bind QUEEN_SQS_LISTEN={listen}: {e}");
            std::process::exit(1);
        }
    };
    tracing::info!(target: "boot", listen = %listen, "listening");

    match tls {
        Some(server_config) => serve_tls(listener, app, server_config, grace).await,
        None => serve_plain(listener, app, grace).await,
    }
    tracing::info!(target: "shutdown", "queen-sqs has stopped");
}

// ---------------------------------------------------------------------- guards

/// Everything legal, dangerous and worth saying at boot.
///
/// A function rather than four inline `warn!`s so that the SET is testable: the
/// value here is that nobody removes one by accident, and a test that walks the
/// configurations is the only thing that notices when someone does.
fn warnings(config: &Config) -> Vec<String> {
    let mut out = Vec::new();
    if config.auth == AuthMode::Off {
        let mut line = format!(
            "QUEEN_SQS_AUTH=off: no request is authenticated, and every one of them reaches Queen \
             with {}.",
            match config.queen_token.is_some() {
                true => "QUEEN_TOKEN",
                false => "no bearer at all",
            }
        );
        if !binds_loopback(&config.listen) {
            line.push_str(&format!(
                " This listener is bound to {}, which is NOT loopback: anything that can reach it \
                 can read, write and delete every queue on that broker. Development only.",
                config.listen
            ));
        }
        out.push(line);
        if !config.credentials.is_empty() {
            out.push(format!(
                "QUEEN_SQS_CREDENTIALS names {} principal(s) and QUEEN_SQS_AUTH=off ignores all of \
                 them: nothing is verified and no principal's own Queen token is ever used.",
                config.credentials.len()
            ));
        }
    }
    if config.handle_secret_generated {
        out.push(
            "QUEEN_SQS_HANDLE_SECRET is unset, so a key was generated for THIS process. Receipt \
             handles are tagged with it: they do not survive a restart, and a second replica \
             cannot serve a DeleteMessage for a message this one delivered \
             (ReceiptHandleIsInvalid). Set it to the same value everywhere before running more \
             than one instance."
                .to_string(),
        );
    }
    // `QUEEN_SQS_RECEIVE_MODE=amortized` is NOT a warning here, and used to be:
    // it needs `maxPerPartition` on the broker (C-SQS-1, unimplemented), so
    // `Config::from_source` now REFUSES it and this process never starts with
    // it. A warning for a state the config layer makes unreachable is a line
    // that can only ever be dead, and a second answer to a question that already
    // has one — see `config::tests::the_unserved_receive_mode_is_refused_at_boot`.
    out
}

/// Whether this listen address can only be reached from this host.
///
/// Conservative in the direction that matters: anything not recognizably
/// loopback is reported as reachable, so a hostname nobody can resolve produces
/// the loud warning rather than the quiet one.
fn binds_loopback(listen: &str) -> bool {
    let host = match listen.rsplit_once(':') {
        Some((host, _)) => host,
        None => listen,
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    matches!(host, "127.0.0.1" | "::1" | "localhost") || host.starts_with("127.")
}

// ------------------------------------------------------------------- listeners

/// The plaintext listener, and the whole of the OSS deployment.
///
/// Two stages, because `axum::serve`'s graceful shutdown waits for every live
/// connection with no bound of its own: the signal closes the listener, and the
/// grace bounds what happens after. A `ReceiveMessage` parked on a 20 second
/// long poll is the request this is sized for.
async fn serve_plain(listener: TcpListener, app: Router, grace: Duration) {
    let (fired, stopped) = tokio::sync::oneshot::channel::<&'static str>();
    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        let signal = stop_signal().await;
        let _ = fired.send(signal);
    });
    let mut serving = tokio::spawn(async move {
        if let Err(e) = server.await {
            tracing::error!(target: "serve", error = %e, "the listener stopped");
        }
    });
    tokio::select! {
        _ = &mut serving => {}
        signal = stopped => {
            announce(signal.unwrap_or("shutdown"), grace);
            drain(serving, grace).await;
        }
    }
}

/// The optional TLS listener. `axum::serve` takes a plain `TcpListener`, so this
/// leg drives hyper directly: accept, rustls handshake, one hyper http1
/// connection per stream with the Router as the service. Same construction as
/// the proxy's, and for the same reason.
async fn serve_tls(
    listener: TcpListener,
    app: Router,
    server_config: rustls::ServerConfig,
    grace: Duration,
) {
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
    let (shut_tx, shut_rx) = tokio::sync::watch::channel(false);
    // Liveness: every connection task holds a clone of the sender, so the
    // receiver completes exactly when the last connection is done.
    let (live, mut last_gone) = tokio::sync::mpsc::channel::<()>(1);
    let mut signal = std::pin::pin!(stop_signal());

    let stopped_by = loop {
        let accepted = tokio::select! {
            name = &mut signal => break name,
            accepted = listener.accept() => accepted,
        };
        let (stream, peer) = match accepted {
            Ok(accepted) => accepted,
            Err(e) => {
                tracing::warn!(target: "tls", error = %e, "accept failed");
                continue;
            }
        };
        let acceptor = acceptor.clone();
        let service = hyper_util::service::TowerToHyperService::new(app.clone());
        let mut shut_rx = shut_rx.clone();
        let live = live.clone();
        tokio::spawn(async move {
            let _live = live;
            let stream = match acceptor.accept(stream).await {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::debug!(target: "tls", peer = %peer, error = %e, "handshake failed");
                    return;
                }
            };
            let connection = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service);
            tokio::pin!(connection);
            // `if !asked` disables the branch once signalled: after the sender
            // is dropped `changed()` returns immediately, and re-selecting on it
            // would spin.
            let mut asked = false;
            loop {
                tokio::select! {
                    served = connection.as_mut() => {
                        if let Err(e) = served {
                            tracing::debug!(target: "tls", peer = %peer, error = %e, "connection ended");
                        }
                        break;
                    }
                    _ = shut_rx.changed(), if !asked => {
                        asked = true;
                        connection.as_mut().graceful_shutdown();
                    }
                }
            }
        });
    };

    announce(stopped_by, grace);
    let _ = shut_tx.send(true);
    // The loop's own clone, so that "every sender is gone" means "every
    // connection is gone".
    drop(live);
    if tokio::time::timeout(grace, last_gone.recv()).await.is_err() {
        cut_short(grace);
    }
}

fn announce(signal: &str, grace: Duration) {
    tracing::info!(
        target: "shutdown",
        signal,
        grace_ms = grace.as_millis() as u64,
        "queen-sqs is stopping: the listener is closed and in-flight requests are finishing"
    );
}

fn cut_short(grace: Duration) {
    tracing::warn!(
        target: "shutdown",
        grace_ms = grace.as_millis() as u64,
        "QUEEN_SQS_SHUTDOWN_GRACE_MS expired with requests still in flight; they were cut off. A \
         long poll runs for up to 20 seconds, so a grace shorter than that ends one on every \
         deploy"
    );
}

/// Wait for the accepted work to finish, bounded by the grace.
async fn drain(serving: tokio::task::JoinHandle<()>, grace: Duration) {
    if tokio::time::timeout(grace, serving).await.is_err() {
        cut_short(grace);
    }
}

/// Resolves when this process is asked to stop, with the name of what asked.
///
/// SIGTERM is the one that matters: it is what `kubectl delete pod` and every
/// rolling deploy send, what `systemctl stop` sends, and what the broker's
/// supervisor sends this process in embedded mode. Without a handler the default
/// action ends the process where it stands — which for this facade means every
/// in-flight `ReceiveMessage` becomes a claim held on messages nobody will ack
/// until their visibility lapses.
///
/// Ctrl-C is here in the shape a person uses, for the same reason.
#[cfg(unix)]
async fn stop_signal() -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(term) => term,
        Err(e) => {
            // Refusing to start over this would be worse than the loss: the
            // facade still serves, it just cannot be stopped politely.
            tracing::warn!(
                target: "boot",
                error = %e,
                "SIGTERM cannot be handled in this process; a stop will cut off in-flight \
                 requests instead of draining them"
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

// ------------------------------------------------------------------------- tls

/// The rustls `ServerConfig` for a PEM chain and its key.
///
/// Hand-rolled PEM, exactly as queen-kafka's `tls.rs` does it and for the same
/// reason: base64 and `rustls::pki_types` are already here, the labels are the
/// four openssl emits, and one more crate for forty lines is a dependency to
/// keep in step forever. Every failure names the variable that named the file.
fn tls_config(cert_path: &str, key_path: &str) -> Result<rustls::ServerConfig, String> {
    let cert_pem = std::fs::read_to_string(cert_path)
        .map_err(|e| format!("QUEEN_SQS_TLS_CERT={cert_path} cannot be read: {e}"))?;
    let key_pem = std::fs::read_to_string(key_path)
        .map_err(|e| format!("QUEEN_SQS_TLS_KEY={key_path} cannot be read: {e}"))?;
    let chain =
        certificates(&cert_pem).map_err(|e| format!("QUEEN_SQS_TLS_CERT={cert_path}: {e}"))?;
    let key = private_key(&key_pem).map_err(|e| format!("QUEEN_SQS_TLS_KEY={key_path}: {e}"))?;
    // The ring provider explicitly rather than the process default: `reqwest`
    // brings its own rustls into this binary for the Queen side, and a config
    // built from whatever happened to be installed first is one nobody chose.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("rustls could not take its safe default protocol versions: {e}"))?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|e| {
            format!(
                "the certificate and the key do not make a usable listener: {e} (the usual cause \
                 is a key that does not belong to the certificate, or a chain whose LEAF is not \
                 the first block in the file)"
            )
        })?;
    // Advertised deliberately: this leg serves HTTP/1.1 only, and a client that
    // negotiated h2 over ALPN and then met an http1 connection would fail in a
    // way that looks like a network fault.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

/// The full chain: leaf first, then any intermediates. A chain truncated to its
/// first block is a listener that fails only for clients that do not already
/// hold the intermediate, which is the worst kind of half-working.
fn certificates(pem: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let chain: Vec<CertificateDer<'static>> = pem_blocks(pem)
        .into_iter()
        .filter(|(label, _)| label == "CERTIFICATE")
        .map(|(_, der)| CertificateDer::from(der))
        .collect();
    if chain.is_empty() {
        return Err("no CERTIFICATE block found in the file".to_string());
    }
    Ok(chain)
}

/// The first private key in the file, in any of the three encodings openssl
/// emits.
fn private_key(pem: &str) -> Result<PrivateKeyDer<'static>, String> {
    for (label, der) in pem_blocks(pem) {
        match label.as_str() {
            "PRIVATE KEY" => return Ok(PrivateKeyDer::Pkcs8(der.into())),
            "RSA PRIVATE KEY" => return Ok(PrivateKeyDer::Pkcs1(der.into())),
            "EC PRIVATE KEY" => return Ok(PrivateKeyDer::Sec1(der.into())),
            _ => {}
        }
    }
    Err("no PRIVATE KEY, RSA PRIVATE KEY or EC PRIVATE KEY block found in the file".to_string())
}

/// `(label, DER)` for every well-formed PEM block, skipping anything that is not
/// one — a `subject=`/`issuer=` preamble, a comment, a trailing note.
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
            if let Some(label) = label.take() {
                if let Ok(der) = B64_STD.decode(body.as_bytes()) {
                    out.push((label, der));
                }
            }
            body.clear();
        } else if label.is_some() {
            body.push_str(line);
        }
    }
    out
}

/// Slim tracing init, aligned with the broker's, the proxy's and queen-kafka's:
/// LOG_LEVEL / RUST_LOG feed the EnvFilter, QUEEN_LOG_JSON switches format.
///
/// The one environment read outside `Config` — deliberately, because it has to
/// happen BEFORE the config is read for the config's own failure to be logged
/// at all, and because none of the three names carries a secret.
fn init_tracing() {
    let filter = std::env::var("RUST_LOG")
        .or_else(|_| std::env::var("LOG_LEVEL"))
        .unwrap_or_else(|_| "info".to_string());
    let filter = tracing_subscriber::EnvFilter::try_new(filter)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let json = std::env::var("QUEEN_LOG_JSON")
        .map(|value| value == "true")
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn config(pairs: &[(&str, &str)]) -> Config {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Config::from_source(&|name| map.get(name).cloned()).expect("boots")
    }

    /// The default development shape says three things: it is open, its handle
    /// secret is per-process, and — because 0.0.0.0 is the default bind — that
    /// open listener is reachable.
    #[test]
    fn an_open_listener_on_every_interface_is_said_out_loud() {
        let said = warnings(&config(&[("QUEEN_SQS_AUTH", "off")])).join("\n");
        assert!(said.contains("QUEEN_SQS_AUTH=off"), "{said}");
        assert!(said.contains("NOT loopback"), "{said}");
        assert!(said.contains("QUEEN_SQS_HANDLE_SECRET"), "{said}");
    }

    /// The same listener on loopback is a laptop, and gets the warning without
    /// the sentence about the fleet.
    #[test]
    fn loopback_gets_the_quiet_half_of_the_warning() {
        let said = warnings(&config(&[
            ("QUEEN_SQS_AUTH", "off"),
            ("QUEEN_SQS_LISTEN", "127.0.0.1:9324"),
        ]))
        .join("\n");
        assert!(said.contains("QUEEN_SQS_AUTH=off"), "{said}");
        assert!(!said.contains("NOT loopback"), "{said}");
    }

    /// A configured secret and a verified listener have nothing to warn about —
    /// which is the property that makes the warnings above worth reading.
    #[test]
    fn the_production_shape_is_silent() {
        let said = warnings(&config(&[
            (
                "QUEEN_SQS_CREDENTIALS",
                "AKID:wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY:tok",
            ),
            ("QUEEN_SQS_HANDLE_SECRET", "a shared sixteen+ byte key"),
        ]));
        assert!(said.is_empty(), "{said:?}");
    }

    /// Dead configuration is worth a line of its own: an operator who set
    /// credentials believes they are in force.
    #[test]
    fn credentials_that_auth_off_ignores_are_reported_as_ignored() {
        let said = warnings(&config(&[
            ("QUEEN_SQS_AUTH", "off"),
            (
                "QUEEN_SQS_CREDENTIALS",
                "AKID:wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY:tok",
            ),
            ("QUEEN_SQS_HANDLE_SECRET", "a shared sixteen+ byte key"),
        ]))
        .join("\n");
        assert!(said.contains("ignores all of them"), "{said}");
    }

    /// A mode the broker cannot serve does not warn, it does not START: the
    /// boot fails and the operator reads why. This is the binary's half of
    /// `config::tests::the_unserved_receive_mode_is_refused_at_boot` — that a
    /// refusal reaches the exit path rather than being softened into a line
    /// somewhere in `warnings`.
    #[test]
    fn an_unserved_receive_mode_never_boots() {
        let map: HashMap<String, String> = [
            ("QUEEN_SQS_AUTH", "off"),
            ("QUEEN_SQS_RECEIVE_MODE", "amortized"),
            ("QUEEN_SQS_HANDLE_SECRET", "a shared sixteen+ byte key"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let err = Config::from_source(&|name| map.get(name).cloned())
            .expect_err("amortized cannot be served, so it cannot be started");
        assert!(err.contains("C-SQS-1"), "{err}");
        assert!(err.contains("PLAN_QUEEN_SQS.md"), "{err}");
        // …and nothing in the warning set mentions it, because nothing can
        // reach `warnings` in that state.
        let said = warnings(&config(&[
            ("QUEEN_SQS_AUTH", "off"),
            ("QUEEN_SQS_HANDLE_SECRET", "a shared sixteen+ byte key"),
        ]))
        .join("\n");
        assert!(!said.contains("amortized"), "{said}");
    }

    #[test]
    fn loopback_is_recognized_in_every_spelling_and_nothing_else_is() {
        for listen in [
            "127.0.0.1:9324",
            "[::1]:9324",
            "localhost:9324",
            "127.9.9.9:1",
        ] {
            assert!(binds_loopback(listen), "{listen}");
        }
        for listen in [
            "0.0.0.0:9324",
            "[::]:9324",
            "10.0.0.5:9324",
            "sqs.internal:9324",
        ] {
            assert!(!binds_loopback(listen), "{listen}");
        }
    }

    /// The PEM reader takes what openssl writes, including the preamble
    /// `openssl x509` leaves in front of a certificate.
    #[test]
    fn pem_blocks_are_read_past_whatever_surrounds_them() {
        let pem = "subject=CN = queen\n-----BEGIN CERTIFICATE-----\naGVsbG8=\n-----END CERTIFICATE-----\n\
                   \n# a note\n-----BEGIN PRIVATE KEY-----\nd29ybGQ=\n-----END PRIVATE KEY-----\n";
        let blocks = pem_blocks(pem);
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].0, "CERTIFICATE");
        assert_eq!(blocks[0].1, b"hello");
        assert_eq!(blocks[1].0, "PRIVATE KEY");
        assert!(certificates(pem).is_ok());
        assert!(private_key(pem).is_ok());
    }

    /// A file with no material in it fails the BOOT, naming which of the two
    /// variables pointed at it.
    #[test]
    fn tls_material_that_is_not_there_names_its_own_variable() {
        let err = tls_config("/nonexistent/cert.pem", "/nonexistent/key.pem").expect_err("refused");
        assert!(err.contains("QUEEN_SQS_TLS_CERT"), "{err}");
        assert!(certificates("").is_err());
        assert!(
            private_key("-----BEGIN CERTIFICATE-----\naGk=\n-----END CERTIFICATE-----").is_err()
        );
    }

    /// The signal handler is installed at THIS process, so the assertion is the
    /// real one: raise SIGTERM and watch the future resolve.
    #[cfg(unix)]
    #[tokio::test]
    async fn sigterm_is_what_stops_it() {
        use tokio::signal::unix::{signal, SignalKind};
        // Registered here too, so that the default action (terminate the test
        // runner) is not what happens if the handler below is not up yet.
        let mut ours = signal(SignalKind::terminate()).expect("cannot handle SIGTERM here");
        let mut stopping = tokio::spawn(stop_signal());
        // The handler is installed asynchronously; a signal raised before it is
        // up is one nothing observes.
        tokio::time::sleep(Duration::from_millis(50)).await;
        unsafe { libc_raise_sigterm() };
        let name = tokio::time::timeout(Duration::from_secs(5), &mut stopping)
            .await
            .expect("the stop signal resolved")
            .expect("the task did not panic");
        assert_eq!(name, "SIGTERM");
        // Drain our own copy so the runner does not carry it into another test.
        let _ = tokio::time::timeout(Duration::from_millis(50), ours.recv()).await;
    }

    /// `raise(SIGTERM)` without a libc dependency: the signal is sent to this
    /// process by the kernel's own kill(2), reached through the one extern the
    /// test needs.
    #[cfg(unix)]
    unsafe fn libc_raise_sigterm() {
        extern "C" {
            fn raise(sig: i32) -> i32;
        }
        // 15 on every platform this builds for.
        raise(15);
    }
}
