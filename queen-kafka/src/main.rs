//! queen-kafka binary: environment configuration, then the listener.
//! Spec: PLAN_QUEEN_KAFKA.md (repo root); the protocol lives in the library.

use std::sync::Arc;

use queen_kafka::{conn, queen, Facade};

/// Resolved configuration. Every field is validated at boot — a Kafka facade
/// that starts on bad configuration does not fail, it lies: clients bootstrap
/// successfully and then fail on the connection they were told to make.
#[derive(Debug, Clone, PartialEq, Eq)]
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
}

/// Hosts that are legal to BIND and never legal to ADVERTISE. A client told to
/// connect to a wildcard dials the wildcard.
const WILDCARD_HOSTS: [&str; 3] = ["0.0.0.0", "::", "0000:0000:0000:0000:0000:0000:0000:0000"];

/// Upper bound on `QUEEN_KAFKA_DEFAULT_PARTITIONS`. See where it is used.
const MAX_DEFAULT_PARTITIONS: u32 = 100_000;

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

        Ok(Config {
            queen_url,
            queen_token,
            listen_addr,
            advertised_host,
            advertised_port,
            default_partitions,
        })
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
        "queen-kafka starting"
    );

    let api = match queen::HttpQueen::new(&cfg.queen_url) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            tracing::error!(target: "boot", "FATAL: {e}");
            std::process::exit(1);
        }
    };
    let facade = Arc::new(Facade {
        advertised_host: cfg.advertised_host.clone(),
        advertised_port: cfg.advertised_port,
        default_partitions: cfg.default_partitions,
        queen_token: cfg.queen_token.clone(),
        catalog: queen::Catalog::new(api),
    });

    let listener = match tokio::net::TcpListener::bind(&cfg.listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(target: "boot", "FATAL: cannot bind QUEEN_KAFKA_ADDR={}: {e}", cfg.listen_addr);
            std::process::exit(1);
        }
    };
    conn::serve(listener, facade).await;
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
}
