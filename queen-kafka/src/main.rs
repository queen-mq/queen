//! queen-kafka binary: environment configuration, then the listener.
//! Spec: PLAN_QUEEN_KAFKA.md (repo root); the protocol lives in the library.

use std::sync::Arc;
use std::time::Duration;

use queen_kafka::cluster::{self, registry, Cluster, ClusterState};
use queen_kafka::coordinator::{Coordinator, GroupConfig};
use queen_kafka::handlers::metadata;
use queen_kafka::{conn, queen, tls, Facade, Policy};

/// Resolved configuration. Every field is validated at boot — a Kafka facade
/// that starts on bad configuration does not fail, it lies: clients bootstrap
/// successfully and then fail on the connection they were told to make.
#[derive(Clone, PartialEq, Eq)]
struct Config {
    /// Broker or proxy this facade speaks plain HTTP to, as a normal Queen
    /// client. `QUEEN_URL`, defaulting to the broker's own default listen
    /// address (`QUEEN_BIND_ADDR` 0.0.0.0 + `PORT` 6632, server/src/config.rs).
    queen_url: String,
    /// `QUEEN_TOKEN` — bearer token for that broker, when it has auth enabled.
    /// Optional, and never logged. From M5 a connection may present its own.
    queen_token: Option<String>,
    /// Address the Kafka listener binds. `QUEEN_KAFKA_ADDR`, default
    /// `0.0.0.0:9092` — 9092 because that is the port every client's default
    /// `bootstrap.servers` already names.
    listen_addr: String,
    /// Host:port this facade tells clients to reach it on.
    /// `QUEEN_KAFKA_ADVERTISED_ADDR`; no default, see `resolve`.
    advertised_host: String,
    advertised_port: u16,
    /// Partition count for a topic auto-created on first use (M1).
    /// `QUEEN_KAFKA_DEFAULT_PARTITIONS`, default 1024: partitions are cheap on
    /// Queen and a fine lane count is what makes the mapping "Kafka partition n
    /// = Queen partition n" worth having.
    default_partitions: u32,
    /// `QUEEN_KAFKA_NODE_ID` — the ONE switch of cluster mode
    /// ([`queen_kafka::cluster`]). `None` is the default and is this facade's
    /// behaviour bit for bit: nothing below is read, spawned or written.
    node_id: Option<i32>,
    /// `QUEEN_KAFKA_CLUSTER` — the registry prefix and the advertised
    /// `cluster_id`. Only read when `node_id` is set, and refused without it.
    cluster: String,
    /// `QUEEN_KAFKA_CLUSTER_HEARTBEAT_MS` / `QUEEN_KAFKA_CLUSTER_TTL_MS`.
    cluster_heartbeat: Duration,
    cluster_ttl: Duration,
    /// The consumer-group timings (M4): the join window and the session-timeout
    /// bounds. Resolved by [`GroupConfig::resolve`], which owns their names,
    /// defaults and validation — they belong next to the state machine that
    /// reads them, not in this file's `resolve`.
    groups: GroupConfig,
    /// `QUEEN_KAFKA_TLS_CERT` + `QUEEN_KAFKA_TLS_KEY` (M5). `None` is a
    /// plaintext listener, which is the default and the whole of the OSS
    /// deployment. Both or neither — see `resolve`.
    tls: Option<(String, String)>,
    /// What the listener does to a connection before a handler sees it: SASL,
    /// and SNI-derived Host forwarding.
    policy: Policy,
}

/// Hand-written rather than derived, for ONE field: `queen_token` is a bearer
/// token, and a derived `Debug` prints it.
///
/// Nothing prints a `Config` today. That is exactly why this is here — the whole
/// value of a redacting `Debug` is that it is already in place on the day
/// someone adds `?cfg` to a boot line or a `panic!` renders the struct. The rest
/// of the fields are configuration a person set and would want to read back, so
/// they are printed as they are. Same rule, and the same reason, as
/// [`queen_kafka::secret::CredentialKey`].
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("queen_url", &self.queen_url)
            // Whether there is one is worth printing; which one it is never is.
            .field(
                "queen_token",
                &self.queen_token.as_ref().map(|_| "<redacted>"),
            )
            .field("listen_addr", &self.listen_addr)
            .field("advertised_host", &self.advertised_host)
            .field("advertised_port", &self.advertised_port)
            .field("default_partitions", &self.default_partitions)
            .field("node_id", &self.node_id)
            .field("cluster", &self.cluster)
            .field("cluster_heartbeat", &self.cluster_heartbeat)
            .field("cluster_ttl", &self.cluster_ttl)
            .field("groups", &self.groups)
            .field("tls", &self.tls)
            .field("policy", &self.policy)
            .finish()
    }
}

/// Hosts that are legal to BIND and never legal to ADVERTISE. A client told to
/// connect to a wildcard dials the wildcard.
const WILDCARD_HOSTS: [&str; 3] = ["0.0.0.0", "::", "0000:0000:0000:0000:0000:0000:0000:0000"];

/// Upper bound on `QUEEN_KAFKA_DEFAULT_PARTITIONS`, and the SAME bound the
/// metadata handler clamps a queue's live lane count to — one number, so a
/// configured width and a discovered one can never disagree about what is
/// answerable. See [`metadata::MAX_ADVERTISED_PARTITIONS`].
const MAX_DEFAULT_PARTITIONS: u32 = metadata::MAX_ADVERTISED_PARTITIONS;

impl Config {
    fn from_env() -> Result<Config, String> {
        Config::resolve(|k| std::env::var(k).ok())
    }

    /// Pure resolution against a getter, so the boot policy is testable without
    /// mutating the process environment. A present-but-empty variable is
    /// treated as unset: an empty value in a compose file or a Helm template is
    /// a variable someone meant to set, and silently taking `""` as a host is
    /// how the footgun below fires.
    fn resolve(get: impl Fn(&str) -> Option<String>) -> Result<Config, String> {
        let env = |k: &str| {
            get(k)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };

        // Checked here rather than on the first metadata refresh: a facade that
        // accepts a URL it can never call is the same lie as one that advertises
        // an address nobody can reach.
        let queen_url = queen::normalize_base_url(
            &env("QUEEN_URL").unwrap_or_else(|| "http://localhost:6632".to_string()),
        )?;
        let queen_token = env("QUEEN_TOKEN");

        let listen_addr = env("QUEEN_KAFKA_ADDR").unwrap_or_else(|| "0.0.0.0:9092".to_string());
        split_host_port("QUEEN_KAFKA_ADDR", &listen_addr)?;

        // THE footgun (PLAN_QUEEN_KAFKA.md M1). A Kafka client connects once to
        // a bootstrap address, asks for Metadata, and from then on talks only to
        // the addresses the broker advertised. Get this wrong and every symptom
        // appears one step away from the cause: bootstrap succeeds, `kcat -L`
        // prints a broker list, and every produce and fetch then hangs or
        // refuses against an address the operator never typed. There is no
        // default that is right more often than it is wrong, so there is none.
        let advertised = env("QUEEN_KAFKA_ADVERTISED_ADDR").ok_or_else(|| {
            format!(
                "QUEEN_KAFKA_ADVERTISED_ADDR is not set and has no default.\n\
                 It is the host:port OTHER MACHINES use to reach this process — Kafka clients \
                 bootstrap once, then reconnect to whatever address this facade advertises, so \
                 a wrong or missing value fails AFTER a successful connection and looks like a \
                 broker fault.\n\
                 It is not the bind address: that is QUEEN_KAFKA_ADDR (currently {listen_addr}).\n\
                 Set it to something your clients resolve and route to, e.g. \
                 QUEEN_KAFKA_ADVERTISED_ADDR=kafka.example.com:9092 or =10.0.0.7:9092."
            )
        })?;
        let (advertised_host, advertised_port) =
            split_host_port("QUEEN_KAFKA_ADVERTISED_ADDR", &advertised)?;
        if WILDCARD_HOSTS.contains(&advertised_host.to_ascii_lowercase().as_str()) {
            return Err(format!(
                "QUEEN_KAFKA_ADVERTISED_ADDR={advertised} advertises a wildcard host.\n\
                 Clients would be handed {advertised_host} as the address to connect to, and \
                 every one of them would fail immediately after a successful bootstrap.\n\
                 Advertise the routable host clients reach this process on; the wildcard belongs \
                 in QUEEN_KAFKA_ADDR (currently {listen_addr}), which is what gets bound."
            ));
        }

        // Loud on a bad value rather than silently substituting the default:
        // the default is not there to paper over a typo (same rule as the
        // broker's boolean knobs, server/src/config.rs).
        let default_partitions = match env("QUEEN_KAFKA_DEFAULT_PARTITIONS") {
            None => 1024,
            Some(v) => match v.parse::<u32>() {
                // The ceiling is not arbitrary: every partition of every
                // requested topic is written out in full in every Metadata
                // response (~34 bytes each), and clients refresh on their own
                // timer. 100k lanes is a ~3.4 MB answer per topic — already
                // absurd, and the last order of magnitude before a typo becomes
                // an out-of-memory instead of an error message.
                Ok(n) if (1..=MAX_DEFAULT_PARTITIONS).contains(&n) => n,
                _ => {
                    return Err(format!(
                        "QUEEN_KAFKA_DEFAULT_PARTITIONS={v} is not a partition count — \
                         give it an integer in 1..={MAX_DEFAULT_PARTITIONS}, or unset it for 1024"
                    ))
                }
            },
        };

        // TLS: both paths or neither. A half-configured listener silently
        // serving plaintext is the failure this refuses — the same rule and the
        // same shape as the proxy's own origin listener
        // (proxy/src/config.rs `tls_material`).
        let tls =
            match (env("QUEEN_KAFKA_TLS_CERT"), env("QUEEN_KAFKA_TLS_KEY")) {
                (Some(cert), Some(key)) => Some((cert, key)),
                (None, None) => None,
                (Some(_), None) => return Err(
                    "QUEEN_KAFKA_TLS_CERT is set without QUEEN_KAFKA_TLS_KEY. TLS needs both, and \
                     starting without it would serve the Kafka port in cleartext — which is not \
                     what setting a certificate meant."
                        .to_string(),
                ),
                (None, Some(_)) => return Err(
                    "QUEEN_KAFKA_TLS_KEY is set without QUEEN_KAFKA_TLS_CERT. TLS needs both, and \
                     starting without it would serve the Kafka port in cleartext — which is not \
                     what setting a key meant."
                        .to_string(),
                ),
            };

        // SASL. One value today, and it is spelled out rather than taken as a
        // boolean so that adding SCRAM later is a new value here rather than a
        // second variable that can disagree with this one.
        let sasl_plain = match env("QUEEN_KAFKA_SASL").as_deref() {
            None => false,
            Some(v) if v.eq_ignore_ascii_case("plain") => true,
            Some(v) => {
                return Err(format!(
                    "QUEEN_KAFKA_SASL={v} is not a mechanism this facade offers. The only value is \
                     `plain`; unset it for a listener that authenticates nobody and reaches Queen \
                     with QUEEN_TOKEN."
                ))
            }
        };
        let forward_sni_host = match env("QUEEN_KAFKA_FORWARD_SNI_HOST").as_deref() {
            None => false,
            Some(v) if v.eq_ignore_ascii_case("true") => true,
            Some(v) if v.eq_ignore_ascii_case("false") => false,
            Some(v) => {
                return Err(format!(
                    "QUEEN_KAFKA_FORWARD_SNI_HOST={v} is not a boolean — give it `true` or \
                     `false`, or unset it for `false`"
                ))
            }
        };
        // A knob that reads a value the listener cannot have is a knob that
        // does nothing, and one that does nothing is worse than one that is
        // missing: an operator sets it, sees no routing, and looks everywhere
        // but here. Without TLS there is no SNI at all.
        if forward_sni_host && tls.is_none() {
            return Err(
                "QUEEN_KAFKA_FORWARD_SNI_HOST=true needs a TLS listener: the server name it \
                 forwards is the TLS SNI, and a plaintext connection carries none. Set \
                 QUEEN_KAFKA_TLS_CERT and QUEEN_KAFKA_TLS_KEY, or unset this."
                    .to_string(),
            );
        }

        // The one knob that is about the LISTENER rather than about a client:
        // how many connections it serves at once. Loud on a bad value, and
        // bounded at both ends — a listener with one connection and a listener
        // with a million are both typos.
        let max_connections = match env("QUEEN_KAFKA_MAX_CONNECTIONS") {
            None => queen_kafka::DEFAULT_MAX_CONNECTIONS,
            Some(v) => match v.parse::<usize>() {
                Ok(n) if (16..=1_000_000).contains(&n) => n,
                _ => {
                    return Err(format!(
                        "QUEEN_KAFKA_MAX_CONNECTIONS={v} is not a connection count — give it an \
                         integer in 16..=1000000, or unset it for {}",
                        queen_kafka::DEFAULT_MAX_CONNECTIONS
                    ))
                }
            },
        };

        // ------------------------------------------------------------ cluster
        //
        // One switch, and it is `QUEEN_KAFKA_NODE_ID`. Everything else here is
        // refused when it is absent rather than quietly ignored: a knob that
        // does nothing is worse than one that is missing (the same rule
        // QUEEN_KAFKA_FORWARD_SNI_HOST follows above).
        let node_id = match env("QUEEN_KAFKA_NODE_ID") {
            None => None,
            Some(v) => match v.parse::<i32>() {
                Ok(n) if (cluster::MIN_NODE_ID..=cluster::MAX_NODE_ID).contains(&n) => Some(n),
                _ => {
                    return Err(format!(
                        "QUEEN_KAFKA_NODE_ID={v} is not a node id — give it an integer in \
                         {min}..={max}, or unset it for a facade that is on its own.\n\
                         0 is RESERVED for the single-node identity this facade advertises when \
                         it is not clustered, so that `am I clustered` is never ambiguous and a \
                         client's cached broker list is replaced wholesale rather than \
                         half-overlapping. Apache Kafka is 0-based; this deviates deliberately.",
                        min = cluster::MIN_NODE_ID,
                        max = cluster::MAX_NODE_ID,
                    ))
                }
            },
        };

        let cluster =
            match env("QUEEN_KAFKA_CLUSTER") {
                None => queen_kafka::handlers::metadata::CLUSTER_ID.to_string(),
                Some(_) if node_id.is_none() => return Err(
                    "QUEEN_KAFKA_CLUSTER is set without QUEEN_KAFKA_NODE_ID. A cluster name alone \
                     would change the cluster_id this facade advertises without clustering \
                     anything — a second, silent axis. Set QUEEN_KAFKA_NODE_ID (1..=64) as well, \
                     or unset this."
                        .to_string(),
                ),
                Some(name) => {
                    // The charset is what makes the registry prefix unambiguous
                    // WITHOUT escaping: `:` is outside it, so one cluster's key
                    // prefix can never be a prefix of another's
                    // (queen_kafka::cluster::registry).
                    if name.len() > 64
                        || !name
                            .chars()
                            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
                    {
                        return Err(format!(
                        "QUEEN_KAFKA_CLUSTER={name} is not a cluster name — 1 to 64 characters of \
                         [A-Za-z0-9._-]. The name goes in the key prefix of the node registry, \
                         and a `:` in it would make one cluster's rows readable as another's."
                    ));
                    }
                    name
                }
            };

        // The registry is written with THIS PROCESS's credential, so there has
        // to be one. Named at boot rather than discovered as an empty view.
        if node_id.is_some() && queen_token.is_none() {
            return Err(format!(
                "QUEEN_KAFKA_NODE_ID={} needs QUEEN_TOKEN. The node registry lives in Queen KV \
                 and is written with THIS PROCESS's credential, not a client's, because the \
                 broker list every client is handed has to be the same list. Give this facade a \
                 Queen token, or unset QUEEN_KAFKA_NODE_ID for single-node mode.\n\
                 All facades of one cluster must present credentials of ONE Queen tenant: \
                 queen.kv is keyed by tenant, so two tenants are two registries and each facade \
                 would see only itself.",
                node_id.unwrap_or_default()
            ));
        }

        let cluster_heartbeat =
            millis("QUEEN_KAFKA_CLUSTER_HEARTBEAT_MS", &env, 2_000, 500, 30_000)?;
        let cluster_ttl = millis("QUEEN_KAFKA_CLUSTER_TTL_MS", &env, 10_000, 3_000, 120_000)?;
        // 5x by default, and never below 3x. Under 3x a single slow KV write
        // evicts a live node and moves group ownership for nothing — a
        // rebalance storm caused by the liveness system itself.
        if cluster_ttl < cluster_heartbeat * 3 {
            return Err(format!(
                "QUEEN_KAFKA_CLUSTER_TTL_MS={ttl} is less than three times \
                 QUEEN_KAFKA_CLUSTER_HEARTBEAT_MS={hb}. The TTL is how long a node stays live \
                 after its last successful write, so a margin under 3x makes one slow KV call \
                 evict a healthy node and move every group it coordinates. Raise the TTL or \
                 lower the heartbeat.",
                ttl = cluster_ttl.as_millis(),
                hb = cluster_heartbeat.as_millis(),
            ));
        }

        Ok(Config {
            queen_url,
            queen_token,
            listen_addr,
            advertised_host,
            advertised_port,
            default_partitions,
            node_id,
            cluster,
            cluster_heartbeat,
            cluster_ttl,
            groups: GroupConfig::resolve(&|k| get(k))?,
            tls,
            policy: Policy {
                sasl_plain,
                forward_sni_host,
                max_connections,
            },
        })
    }
}

/// A duration knob in milliseconds, bounded at both ends and LOUD on a bad
/// value — the rule every other knob in this file follows: the default is not
/// there to paper over a typo.
fn millis(
    key: &str,
    env: &impl Fn(&str) -> Option<String>,
    default_ms: u64,
    min_ms: u64,
    max_ms: u64,
) -> Result<Duration, String> {
    match env(key) {
        None => Ok(Duration::from_millis(default_ms)),
        Some(v) => match v.parse::<u64>() {
            Ok(ms) if (min_ms..=max_ms).contains(&ms) => Ok(Duration::from_millis(ms)),
            _ => Err(format!(
                "{key}={v} is not a duration this facade will run on — give it a whole number of \
                 milliseconds in {min_ms}..={max_ms}, or unset it for {default_ms}"
            )),
        },
    }
}

/// Split `host:port`, or `[v6]:port`. Returns the reason a value is unusable
/// rather than a bare `None`, because every one of these is an operator typo
/// that has to name itself in the boot log.
fn split_host_port(key: &str, raw: &str) -> Result<(String, u16), String> {
    let (host, port) = match raw.strip_prefix('[') {
        Some(rest) => rest.split_once("]:").ok_or_else(|| {
            format!("{key}={raw} opens a bracketed IPv6 host but has no `]:port` after it")
        })?,
        None => {
            let split = raw.rsplit_once(':').ok_or_else(|| {
                format!(
                    "{key}={raw} carries no port. Kafka addresses are host AND port — write it \
                     as host:port, e.g. {raw}:9092"
                )
            })?;
            if split.0.contains(':') {
                return Err(format!(
                    "{key}={raw} looks like a bare IPv6 address. Bracket the host so the port is \
                     unambiguous, e.g. [{}]:9092",
                    raw
                ));
            }
            split
        }
    };
    if host.is_empty() {
        return Err(format!("{key}={raw} has an empty host"));
    }
    match port.parse::<u16>() {
        Ok(0) | Err(_) => Err(format!(
            "{key}={raw} has no usable port: `{port}` is not a TCP port number"
        )),
        Ok(p) => Ok((host.to_string(), p)),
    }
}

/// How long the stop path may spend handing this node's registry row back
/// before it gives up and lets the TTL do it.
///
/// It is a budget and not a hope: in EMBEDDED mode the broker's supervisor
/// SIGTERMs this process and SIGKILLs it after `QUEEN_KAFKA_SHUTDOWN_GRACE_MS`
/// (default 5000, server/src/config.rs), so one KV round trip has to finish
/// well inside that window or not at all. A stop that misses it is not a
/// failure, only a slower one: the row expires on its own TTL, which is the
/// behaviour every kill has always had.
const DEREGISTER_BUDGET: Duration = Duration::from_secs(2);

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
    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(target: "boot", "FATAL: {e}");
            std::process::exit(1);
        }
    };
    // The TLS material is read and PARSED here rather than on the first
    // connection: a certificate that does not match its key must fail the boot,
    // not every client.
    let tls = match &cfg.tls {
        None => None,
        Some((cert, key)) => match tls::server_config(cert, key) {
            Ok(c) => Some(Arc::new(c)),
            Err(e) => {
                tracing::error!(target: "boot", "FATAL: {e}");
                std::process::exit(1);
            }
        },
    };
    // The one combination that is legal and dangerous: PLAIN puts a bearer
    // token on the wire in cleartext. Not fatal — a loopback rig and a
    // sidecar-encrypted mesh are both real — but it must never be quiet.
    if cfg.policy.sasl_plain && tls.is_none() {
        tracing::warn!(
            target: "boot",
            "QUEEN_KAFKA_SASL=plain on a PLAINTEXT listener: every client will send its Queen \
             token in the clear. Set QUEEN_KAFKA_TLS_CERT and QUEEN_KAFKA_TLS_KEY unless the \
             link is already encrypted underneath this process."
        );
    }

    // The advertised pair is printed at every boot on purpose: it is the one
    // value an operator has to compare against what their clients actually use.
    // The token is not printed, here or anywhere — only whether there is one.
    tracing::info!(
        target: "boot",
        queen_url = %cfg.queen_url,
        authenticated = cfg.queen_token.is_some(),
        listen = %cfg.listen_addr,
        advertised = %format_args!("{}:{}", cfg.advertised_host, cfg.advertised_port),
        default_partitions = cfg.default_partitions,
        group_join_delay_ms = cfg.groups.join_delay.as_millis() as u64,
        tls = tls.is_some(),
        sasl = if cfg.policy.sasl_plain { "plain" } else { "none" },
        forward_sni_host = cfg.policy.forward_sni_host,
        // 0 is the single-node identity and can be nothing else, so one field
        // says both whether this facade is clustered and which node it is.
        node_id = cfg.node_id.unwrap_or(metadata::SINGLE_NODE_ID),
        cluster = %cfg.cluster,
        "queen-kafka starting"
    );

    let api = match queen::HttpQueen::new(&cfg.queen_url) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            tracing::error!(target: "boot", "FATAL: {e}");
            std::process::exit(1);
        }
    };

    // Cluster mode, before the listener binds: claim this node's id, read the
    // live set in the same call, and start the heartbeat that keeps both true.
    // The registration is kept for the stop path, which is the other half of a
    // rolling deploy: a node that goes away without giving its id back is a
    // TTL of peers advertising a closed port, and a replacement pod meeting its
    // own predecessor's row.
    let mut registration: Option<registry::Registration> = None;
    let cluster = match cfg.node_id {
        None => Cluster::Single,
        Some(id) => {
            let state = ClusterState::new(
                cluster::Node {
                    id,
                    host: cfg.advertised_host.clone(),
                    port: cfg.advertised_port,
                    incarnation: cluster::new_incarnation(),
                },
                cfg.cluster.clone(),
                cfg.cluster_heartbeat,
                cfg.cluster_ttl,
            );
            let version =
                match registry::claim(api.as_ref(), &state, cfg.queen_token.as_deref()).await {
                    Ok(version) => Some(version),
                    // The single most likely operator error, caught for one KV
                    // operation. Fatal, because two facades on one node id
                    // advertise one address for two processes.
                    Err(registry::Refused::Taken(message)) => {
                        tracing::error!(target: "boot", "FATAL: {message}");
                        std::process::exit(1);
                    }
                    // NOT fatal: a facade that crash-loops through a Queen
                    // restart takes its clients' produce with it, and the
                    // freshness gate already refuses to coordinate anything
                    // until the registry has been read. The heartbeat re-tries
                    // the claim, and a duplicate id discovered there stops
                    // coordination rather than the process.
                    Err(registry::Refused::Unreachable(e)) => {
                        tracing::error!(
                            target: "boot",
                            error = %e,
                            "the node registry could not be read at boot; this facade will serve \
                             produce and fetch and will NOT coordinate any consumer group until \
                             a heartbeat succeeds"
                        );
                        None
                    }
                };
            registration = Some(registry::spawn(
                Arc::clone(&api) as Arc<dyn queen::QueenApi>,
                Arc::clone(&state),
                cfg.queen_token.clone(),
                version,
            ));
            tracing::info!(
                target: "boot",
                node_id = id,
                cluster = %cfg.cluster,
                heartbeat_ms = cfg.cluster_heartbeat.as_millis() as u64,
                ttl_ms = cfg.cluster_ttl.as_millis() as u64,
                peers = state.view().map_or(0, |v| v.nodes.len()),
                "cluster mode is on"
            );
            Cluster::Enabled(state)
        }
    };

    let facade = Arc::new(Facade::new(
        cfg.advertised_host.clone(),
        cfg.advertised_port,
        cluster,
        cfg.default_partitions,
        cfg.queen_token.clone(),
        // One client object for both paths: it owns the connection pool, and
        // the metadata calls and the data calls are the same HTTP to the same
        // broker.
        api,
        // Empty at boot, and that is the whole of the restart story: a facade
        // that comes back knows no members, tells the survivors so, and they
        // rejoin — the same sequence a Kafka broker failover produces. What
        // they resume FROM is in Queen and was never here.
        Coordinator::new(cfg.groups),
        cfg.policy,
    ));

    let listener = match tokio::net::TcpListener::bind(&cfg.listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(target: "boot", "FATAL: cannot bind QUEEN_KAFKA_ADDR={}: {e}", cfg.listen_addr);
            std::process::exit(1);
        }
    };
    // The accept loop runs until this process is asked to stop. Dropping it
    // closes the listening socket, so no NEW connection is accepted from that
    // moment; the connections already being served are tasks of their own and
    // keep going for as long as the stop path below takes, which is the small
    // drain a rolling deploy wants and costs nothing to have.
    let stopped_by = tokio::select! {
        _ = conn::serve(listener, facade, tls) => "the accept loop ended",
        signal = stop_signal() => signal,
    };
    tracing::info!(target: "shutdown", signal = stopped_by, "queen-kafka is stopping");

    // Hand the node id back. Everything durable this facade had is in Queen and
    // was never here (the group membership it forgets is the one thing a
    // restart is allowed to lose), so the ONLY thing a stop owes the cluster is
    // its registry row: deleted, its peers drop this node within one heartbeat
    // instead of one TTL, and a replacement with the same id claims it at once
    // rather than waiting the corpse out.
    if let Some(registration) = registration {
        match registration.deregister(DEREGISTER_BUDGET).await {
            registry::Departure::Released => tracing::info!(
                target: "shutdown",
                node_id = cfg.node_id.unwrap_or(metadata::SINGLE_NODE_ID),
                cluster = %cfg.cluster,
                "this node's registry row is deleted; its peers stop advertising it on their next \
                 registry read"
            ),
            registry::Departure::NothingHeld => tracing::info!(
                target: "shutdown",
                "this facade held no registry row to hand back"
            ),
            // Neither of these is worth failing the exit over, and neither is
            // silent: the row is somebody else's, or it expires on its TTL the
            // way a killed node's does.
            registry::Departure::NotOurs(reason) => tracing::warn!(
                target: "shutdown",
                reason = %reason,
                "the registry row under this node id is not the one this process wrote, so it was \
                 left alone; whoever holds it now keeps it"
            ),
            registry::Departure::Failed(why) => tracing::warn!(
                target: "shutdown",
                error = %why,
                ttl_ms = cfg.cluster_ttl.as_millis() as u64,
                "this node's registry row could not be deleted; peers will keep advertising this \
                 address until the row's TTL expires"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(pairs: &[(&str, &str)]) -> Result<Config, String> {
        let owned: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Config::resolve(move |k| {
            owned
                .iter()
                .find(|(key, _)| key == k)
                .map(|(_, v)| v.clone())
        })
    }

    #[test]
    fn defaults_are_the_brokers_own() {
        let cfg = resolve(&[("QUEEN_KAFKA_ADVERTISED_ADDR", "kafka.example.com:9092")]).unwrap();
        // server/src/config.rs: QUEEN_BIND_ADDR defaults to 0.0.0.0 and PORT to 6632.
        assert_eq!(cfg.queen_url, "http://localhost:6632");
        assert_eq!(cfg.listen_addr, "0.0.0.0:9092");
        assert_eq!(cfg.advertised_host, "kafka.example.com");
        assert_eq!(cfg.advertised_port, 9092);
        assert_eq!(cfg.default_partitions, 1024);
        assert_eq!(cfg.queen_token, None, "no token unless one is given");
    }

    #[test]
    fn every_knob_is_overridable() {
        let cfg = resolve(&[
            ("QUEEN_URL", "http://queen-mq-v1:6632/"),
            ("QUEEN_TOKEN", "eyJhbGciOi.stub.token"),
            ("QUEEN_KAFKA_ADDR", "127.0.0.1:19092"),
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "[2001:db8::7]:19092"),
            ("QUEEN_KAFKA_DEFAULT_PARTITIONS", "16"),
        ])
        .unwrap();
        // The trailing slash is normalized away, so paths concatenate to one.
        assert_eq!(cfg.queen_url, "http://queen-mq-v1:6632");
        assert_eq!(cfg.queen_token.as_deref(), Some("eyJhbGciOi.stub.token"));
        assert_eq!(cfg.listen_addr, "127.0.0.1:19092");
        assert_eq!(cfg.advertised_host, "2001:db8::7");
        assert_eq!(cfg.advertised_port, 19092);
        assert_eq!(cfg.default_partitions, 16);
    }

    /// A `Config` is the one struct in this binary that holds a credential, and
    /// printing it must not print it — however the printing comes about, on the
    /// day someone adds `?cfg` to a log line. Everything else it holds is
    /// configuration and stays readable.
    #[test]
    fn printing_the_config_never_prints_the_token() {
        const SECRET: &str = "eyJhbGciOi.a-real-looking.tenant-token";
        let cfg = resolve(&[
            ("QUEEN_TOKEN", SECRET),
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "kafka.example.com:9092"),
        ])
        .unwrap();
        let printed = format!("{cfg:?}");
        assert!(!printed.contains(SECRET), "{printed}");
        assert!(!printed.contains("a-real-looking"), "{printed}");
        // The FACT of a credential is diagnostic and stays.
        assert!(printed.contains("redacted"), "{printed}");
        // ...and the rest of the configuration is still readable.
        assert!(printed.contains("kafka.example.com"), "{printed}");

        let none = resolve(&[("QUEEN_KAFKA_ADVERTISED_ADDR", "kafka.example.com:9092")]).unwrap();
        assert!(format!("{none:?}").contains("queen_token: None"));
    }

    #[test]
    fn the_advertised_address_is_required_and_says_why() {
        let err = resolve(&[]).unwrap_err();
        assert!(err.contains("QUEEN_KAFKA_ADVERTISED_ADDR"));
        assert!(
            err.contains("QUEEN_KAFKA_ADDR"),
            "must point at the bind knob"
        );
        assert!(err.contains("host:port") || err.contains(":9092"));
        // Present-but-empty is the same as unset, not an empty host.
        assert!(resolve(&[("QUEEN_KAFKA_ADVERTISED_ADDR", "")]).is_err());
        assert!(resolve(&[("QUEEN_KAFKA_ADVERTISED_ADDR", "   ")]).is_err());
    }

    #[test]
    fn a_wildcard_is_refused_only_where_it_is_wrong() {
        for wildcard in ["0.0.0.0:9092", "[::]:9092"] {
            let err = resolve(&[("QUEEN_KAFKA_ADVERTISED_ADDR", wildcard)]).unwrap_err();
            assert!(err.contains("wildcard"), "{wildcard}: {err}");
        }
        // ...and accepted where it belongs.
        let cfg = resolve(&[
            ("QUEEN_KAFKA_ADDR", "0.0.0.0:9092"),
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "10.0.0.7:9092"),
        ])
        .unwrap();
        assert_eq!(cfg.advertised_host, "10.0.0.7");
    }

    #[test]
    fn addresses_without_a_usable_port_are_refused() {
        for bad in [
            "kafka.example.com",
            "kafka.example.com:",
            "host:0",
            "host:no",
        ] {
            assert!(
                resolve(&[("QUEEN_KAFKA_ADVERTISED_ADDR", bad)]).is_err(),
                "{bad} was accepted"
            );
        }
        // A bare IPv6 address is ambiguous, and the error says how to fix it.
        let err = resolve(&[("QUEEN_KAFKA_ADVERTISED_ADDR", "2001:db8::7")]).unwrap_err();
        assert!(err.contains("Bracket"), "{err}");
        // The bind address goes through the same check.
        assert!(resolve(&[
            ("QUEEN_KAFKA_ADDR", "0.0.0.0"),
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
        ])
        .is_err());
    }

    /// A QUEEN_URL the facade could never call fails at boot, not on the first
    /// metadata refresh three hours later.
    #[test]
    fn a_queen_url_that_is_not_an_http_url_is_refused_at_boot() {
        for bad in ["queen-mq-v1:6632", "ftp://queen-mq-v1", "not a url"] {
            let err = resolve(&[
                ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
                ("QUEEN_URL", bad),
            ])
            .unwrap_err();
            assert!(err.contains("QUEEN_URL"), "{bad}: {err}");
        }
    }

    /// The group knobs are resolved as part of the boot, so a typo in one of
    /// them fails the process rather than being discovered by the first
    /// consumer that joins.
    #[test]
    fn the_group_knobs_are_part_of_the_boot() {
        let cfg = resolve(&[
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
            ("QUEEN_KAFKA_GROUP_JOIN_DELAY_MS", "250"),
        ])
        .unwrap();
        assert_eq!(cfg.groups.join_delay, std::time::Duration::from_millis(250));

        let err = resolve(&[
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
            ("QUEEN_KAFKA_GROUP_JOIN_DELAY_MS", "three seconds"),
        ])
        .unwrap_err();
        assert!(err.contains("QUEEN_KAFKA_GROUP_JOIN_DELAY_MS"), "{err}");
    }

    /// The listener's own knob: how many connections it serves at once. It has
    /// a default because every deployment has one, and it is loud on a bad
    /// value because a listener that serves nobody is not what a typo meant.
    #[test]
    fn the_connection_cap_defaults_and_is_loud_when_wrong() {
        let cfg = resolve(&[("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092")]).unwrap();
        assert_eq!(
            cfg.policy.max_connections,
            queen_kafka::DEFAULT_MAX_CONNECTIONS
        );

        let cfg = resolve(&[
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
            ("QUEEN_KAFKA_MAX_CONNECTIONS", "20000"),
        ])
        .unwrap();
        assert_eq!(cfg.policy.max_connections, 20_000);

        // An empty value is unset and not zero, the same rule every other knob
        // in this file follows.
        assert_eq!(
            resolve(&[
                ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
                ("QUEEN_KAFKA_MAX_CONNECTIONS", "  "),
            ])
            .unwrap()
            .policy
            .max_connections,
            queen_kafka::DEFAULT_MAX_CONNECTIONS
        );
        for bad in ["0", "-1", "8", "many", "2000000"] {
            let err = resolve(&[
                ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
                ("QUEEN_KAFKA_MAX_CONNECTIONS", bad),
            ])
            .unwrap_err();
            assert!(err.contains("QUEEN_KAFKA_MAX_CONNECTIONS"), "{bad}: {err}");
        }
    }

    #[test]
    fn a_bad_partition_count_is_loud_not_defaulted() {
        for bad in ["0", "-1", "1024 partitions", "1e3", "4294967296", "1000000"] {
            let err = resolve(&[
                ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
                ("QUEEN_KAFKA_DEFAULT_PARTITIONS", bad),
            ])
            .unwrap_err();
            assert!(
                err.contains("QUEEN_KAFKA_DEFAULT_PARTITIONS"),
                "{bad}: {err}"
            );
        }
    }

    /// TLS is both files or neither: a half-configured listener that silently
    /// served plaintext would be a certificate an operator believes in.
    #[test]
    fn the_tls_pair_is_both_or_neither() {
        let cfg = resolve(&[("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092")]).unwrap();
        assert_eq!(cfg.tls, None, "TLS is opt-in");

        let cfg = resolve(&[
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
            ("QUEEN_KAFKA_TLS_CERT", "/etc/queen/tls.crt"),
            ("QUEEN_KAFKA_TLS_KEY", "/etc/queen/tls.key"),
        ])
        .unwrap();
        assert_eq!(
            cfg.tls,
            Some(("/etc/queen/tls.crt".into(), "/etc/queen/tls.key".into()))
        );

        for half in ["QUEEN_KAFKA_TLS_CERT", "QUEEN_KAFKA_TLS_KEY"] {
            let err = resolve(&[
                ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
                (half, "/etc/queen/half.pem"),
            ])
            .unwrap_err();
            assert!(err.contains(half), "{half}: {err}");
            assert!(err.contains("cleartext"), "{half}: {err}");
        }
    }

    /// SASL names its mechanism rather than being a boolean, so the day SCRAM
    /// exists it is a value here and not a second variable that can disagree
    /// with this one.
    #[test]
    fn the_sasl_knob_names_a_mechanism() {
        let off = resolve(&[("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092")]).unwrap();
        assert!(!off.policy.sasl_plain, "SASL is opt-in");

        for spelling in ["plain", "PLAIN", "Plain"] {
            let cfg = resolve(&[
                ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
                ("QUEEN_KAFKA_SASL", spelling),
            ])
            .unwrap();
            assert!(cfg.policy.sasl_plain, "{spelling}");
        }

        for bad in ["true", "scram-sha-256", "oauthbearer", "1"] {
            let err = resolve(&[
                ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
                ("QUEEN_KAFKA_SASL", bad),
            ])
            .unwrap_err();
            assert!(err.contains("QUEEN_KAFKA_SASL"), "{bad}: {err}");
        }
    }

    /// A knob that cannot do anything is worse than one that is missing: the
    /// name it forwards is the TLS SNI, and a plaintext listener has none.
    #[test]
    fn forwarding_the_server_name_needs_a_listener_that_has_one() {
        let err = resolve(&[
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
            ("QUEEN_KAFKA_FORWARD_SNI_HOST", "true"),
        ])
        .unwrap_err();
        assert!(err.contains("QUEEN_KAFKA_FORWARD_SNI_HOST"), "{err}");
        assert!(err.contains("QUEEN_KAFKA_TLS_CERT"), "{err}");

        let cfg = resolve(&[
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
            ("QUEEN_KAFKA_TLS_CERT", "/etc/queen/tls.crt"),
            ("QUEEN_KAFKA_TLS_KEY", "/etc/queen/tls.key"),
            ("QUEEN_KAFKA_FORWARD_SNI_HOST", "true"),
        ])
        .unwrap();
        assert!(cfg.policy.forward_sni_host);

        // Loud on a value that is not a boolean, rather than reading it as
        // false — the same rule as every other knob here.
        let err = resolve(&[
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
            ("QUEEN_KAFKA_FORWARD_SNI_HOST", "yes"),
        ])
        .unwrap_err();
        assert!(err.contains("QUEEN_KAFKA_FORWARD_SNI_HOST"), "{err}");
    }

    // ------------------------------------------------------------- cluster

    /// The default is not "a one-node cluster", it is the configuration this
    /// facade has always had — asserted as a STRUCT so a new field cannot slip
    /// into the default without this failing.
    #[test]
    fn absent_cluster_configuration_is_exactly_todays_configuration() {
        let cfg = resolve(&[("QUEEN_KAFKA_ADVERTISED_ADDR", "kafka.example.com:9092")]).unwrap();
        assert_eq!(cfg.node_id, None);
        assert_eq!(
            cfg.cluster, "queen",
            "the cluster_id every client already sees"
        );
        assert_eq!(cfg.cluster_heartbeat, Duration::from_millis(2_000));
        assert_eq!(cfg.cluster_ttl, Duration::from_millis(10_000));

        // ...and the whole struct is the one the pre-cluster resolve produced.
        let expected = Config {
            queen_url: "http://localhost:6632".to_string(),
            queen_token: None,
            listen_addr: "0.0.0.0:9092".to_string(),
            advertised_host: "kafka.example.com".to_string(),
            advertised_port: 9092,
            default_partitions: 1024,
            node_id: None,
            cluster: "queen".to_string(),
            cluster_heartbeat: Duration::from_millis(2_000),
            cluster_ttl: Duration::from_millis(10_000),
            groups: cfg.groups,
            tls: None,
            policy: Policy::default(),
        };
        assert_eq!(cfg, expected);
    }

    /// Node 0 is reserved for the single-node identity, and the error says so
    /// rather than leaving an operator to discover that Kafka is 0-based and
    /// this is not.
    #[test]
    fn a_node_id_outside_the_cluster_range_is_refused_and_names_the_reservation() {
        for bad in ["0", "-1", "65", "two", "1.0", "1000"] {
            let err = resolve(&[
                ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
                ("QUEEN_TOKEN", "t"),
                ("QUEEN_KAFKA_NODE_ID", bad),
            ])
            .unwrap_err();
            assert!(err.contains("QUEEN_KAFKA_NODE_ID"), "{bad}: {err}");
            assert!(err.contains("1..=64"), "{bad}: {err}");
        }
        assert!(resolve(&[
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
            ("QUEEN_TOKEN", "t"),
            ("QUEEN_KAFKA_NODE_ID", "0"),
        ])
        .unwrap_err()
        .contains("RESERVED"));

        let cfg = resolve(&[
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
            ("QUEEN_TOKEN", "t"),
            ("QUEEN_KAFKA_NODE_ID", "64"),
        ])
        .unwrap();
        assert_eq!(cfg.node_id, Some(64));
    }

    /// One axis, one switch: a cluster NAME without a node id would change the
    /// advertised cluster_id of a facade that is not clustered.
    #[test]
    fn a_cluster_name_without_a_node_id_is_refused() {
        let err = resolve(&[
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
            ("QUEEN_KAFKA_CLUSTER", "prod"),
        ])
        .unwrap_err();
        assert!(err.contains("QUEEN_KAFKA_CLUSTER"), "{err}");
        assert!(err.contains("QUEEN_KAFKA_NODE_ID"), "{err}");
    }

    /// The charset is what makes the registry prefix unambiguous without
    /// escaping, so it is enforced where it is set and not where it is read.
    #[test]
    fn a_cluster_name_is_checked_against_the_prefix_charset() {
        let cfg = resolve(&[
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
            ("QUEEN_TOKEN", "t"),
            ("QUEEN_KAFKA_NODE_ID", "2"),
            ("QUEEN_KAFKA_CLUSTER", "prod.eu-west_1"),
        ])
        .unwrap();
        assert_eq!(cfg.cluster, "prod.eu-west_1");

        for bad in ["prod:eu", "prod eu", "così", &"c".repeat(65), "prod/eu"] {
            let err = resolve(&[
                ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
                ("QUEEN_TOKEN", "t"),
                ("QUEEN_KAFKA_NODE_ID", "2"),
                ("QUEEN_KAFKA_CLUSTER", bad),
            ])
            .unwrap_err();
            assert!(err.contains("QUEEN_KAFKA_CLUSTER"), "{bad}: {err}");
        }
    }

    /// The registry is written with THIS process's credential, so cluster mode
    /// needs one — and the error says why rather than just naming a variable.
    #[test]
    fn cluster_mode_requires_a_token_of_its_own() {
        let err = resolve(&[
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
            ("QUEEN_KAFKA_NODE_ID", "2"),
        ])
        .unwrap_err();
        assert!(err.contains("QUEEN_TOKEN"), "{err}");
        assert!(err.contains("registry"), "{err}");
        // ...and a single-node facade still needs none.
        assert!(resolve(&[("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092")]).is_ok());
    }

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

    /// The liveness margin. Below 3x, one slow KV call evicts a healthy node
    /// and moves every group it coordinates — a rebalance storm the liveness
    /// system caused by itself.
    #[test]
    fn the_ttl_must_be_at_least_three_heartbeats() {
        let err = resolve(&[
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
            ("QUEEN_TOKEN", "t"),
            ("QUEEN_KAFKA_NODE_ID", "2"),
            ("QUEEN_KAFKA_CLUSTER_HEARTBEAT_MS", "2000"),
            ("QUEEN_KAFKA_CLUSTER_TTL_MS", "5000"),
        ])
        .unwrap_err();
        assert!(err.contains("QUEEN_KAFKA_CLUSTER_TTL_MS=5000"), "{err}");
        assert!(
            err.contains("QUEEN_KAFKA_CLUSTER_HEARTBEAT_MS=2000"),
            "{err}"
        );

        let cfg = resolve(&[
            ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
            ("QUEEN_TOKEN", "t"),
            ("QUEEN_KAFKA_NODE_ID", "2"),
            ("QUEEN_KAFKA_CLUSTER_HEARTBEAT_MS", "1000"),
            ("QUEEN_KAFKA_CLUSTER_TTL_MS", "3000"),
        ])
        .unwrap();
        assert_eq!(cfg.cluster_heartbeat, Duration::from_millis(1_000));
        assert_eq!(cfg.cluster_ttl, Duration::from_millis(3_000));

        // Loud on a value that is not a duration, and on one outside the
        // bounds — never a silent fall back to the default.
        for (key, bad) in [
            ("QUEEN_KAFKA_CLUSTER_HEARTBEAT_MS", "0"),
            ("QUEEN_KAFKA_CLUSTER_HEARTBEAT_MS", "100"),
            ("QUEEN_KAFKA_CLUSTER_HEARTBEAT_MS", "two seconds"),
            ("QUEEN_KAFKA_CLUSTER_TTL_MS", "1000000"),
            ("QUEEN_KAFKA_CLUSTER_TTL_MS", "-1"),
        ] {
            let err = resolve(&[
                ("QUEEN_KAFKA_ADVERTISED_ADDR", "host:9092"),
                ("QUEEN_TOKEN", "t"),
                ("QUEEN_KAFKA_NODE_ID", "2"),
                (key, bad),
            ])
            .unwrap_err();
            assert!(err.contains(key), "{key}={bad}: {err}");
        }
    }
}
