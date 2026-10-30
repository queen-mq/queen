//! The facade's knobs, read once from the environment at boot.

use std::time::Duration;

/// Everything the facade reads from the environment.
#[derive(Debug, Clone)]
pub struct Config {
    /// `QUEEN_KAFKA_ADDR`, default `0.0.0.0:9092`: where the listener binds.
    pub listen: String,
    /// `QUEEN_KAFKA_ADVERTISED_ADDR` (required): the `host:port` clients are
    /// told to dial for THIS node. Other nodes are advertised at their own
    /// raft host with this port (the broker computes them, [`crate::core::Broker::cluster`]).
    pub advertised_host: String,
    pub advertised_port: i32,
    /// `QUEEN_KAFKA_DEFAULT_PARTITIONS`, default 1024: the width of a topic
    /// nobody declared one for. Unchanged from the first facade, so a topic it
    /// created keeps hashing keys to the same partitions.
    pub default_partitions: i32,
    /// `QUEEN_KAFKA_GROUP_JOIN_DELAY_MS`, default 3000: how long a new group
    /// waits for more members before its first generation (Kafka's
    /// `group.initial.rebalance.delay.ms`).
    pub join_delay: Duration,
    /// `QUEEN_KAFKA_DEDUP_WINDOW_SECONDS`, default 900: the dedup window of a
    /// topic the facade creates, and of a topic with none when an idempotent
    /// producer first writes to it. 0 leaves every topic as it is (a created
    /// one keeps Queen's default, 3600). A Kafka producer retries a batch for
    /// at most `delivery.timeout.ms` (2 minutes by default), and 900 s is the
    /// floor the broker keeps a push's index row for anyway: a longer window
    /// catches no retry and costs memory and boot time per push.
    pub dedup_window_seconds: i64,
    /// `QUEEN_KAFKA_AUTO_CREATE`, default true: Metadata creates an unknown
    /// topic when the client allows it.
    pub auto_create: bool,
    /// `QUEEN_KAFKA_SASL=required`: refuse every request but ApiVersions and
    /// the SASL pair until the connection authenticated.
    pub sasl_required: bool,
    /// `QUEEN_KAFKA_MAX_CONNECTIONS`, default 10000.
    pub max_connections: usize,
    /// `QUEEN_KAFKA_MAX_REQUEST_BYTES`, default 100 MiB (Kafka's
    /// `socket.request.max.bytes`).
    pub max_request_bytes: usize,
}

impl Config {
    pub fn from_env() -> Result<Config, String> {
        Self::from_lookup(|k| std::env::var(k).ok().filter(|v| !v.trim().is_empty()))
    }

    pub fn from_lookup(env: impl Fn(&str) -> Option<String>) -> Result<Config, String> {
        let listen = env("QUEEN_KAFKA_ADDR").unwrap_or_else(|| "0.0.0.0:9092".to_string());
        let advertised = env("QUEEN_KAFKA_ADVERTISED_ADDR").ok_or_else(|| {
            "QUEEN_KAFKA_ADVERTISED_ADDR is required: the host:port clients dial for this node, \
             e.g. QUEEN_KAFKA_ADVERTISED_ADDR=kafka-0.example.com:9092"
                .to_string()
        })?;
        let (advertised_host, advertised_port) = split_host_port(advertised.trim())
            .ok_or_else(|| format!("QUEEN_KAFKA_ADVERTISED_ADDR={advertised} is not host:port"))?;
        let int = |k: &str, default: i64, min: i64, max: i64| -> Result<i64, String> {
            match env(k) {
                None => Ok(default),
                Some(v) => match v.trim().parse::<i64>() {
                    Ok(n) if (min..=max).contains(&n) => Ok(n),
                    _ => Err(format!("{k}={v} is not an integer in {min}..={max}")),
                },
            }
        };
        let flag = |k: &str, default: bool| match env(k).as_deref().map(str::trim) {
            None => default,
            Some(v) => matches!(v, "1" | "true" | "TRUE" | "yes" | "on" | "required"),
        };
        // `plain` is what the first facade read and what its deployments set;
        // `required` was this one's word. Anything else stops the boot: read
        // as "off", a misspelt value lets a connection skip the login, and its
        // calls then carry no token (refused where the broker checks tokens).
        let sasl_required = match env("QUEEN_KAFKA_SASL").map(|v| v.trim().to_ascii_lowercase()) {
            None => false,
            Some(v) if v == "plain" || v == "required" => true,
            Some(v) if matches!(v.as_str(), "off" | "none" | "false" | "0") => false,
            Some(v) => {
                return Err(format!(
                    "QUEEN_KAFKA_SASL={v}: expected plain (SASL/PLAIN on every connection) or off"
                ))
            }
        };
        Ok(Config {
            listen,
            advertised_host,
            advertised_port,
            default_partitions: int("QUEEN_KAFKA_DEFAULT_PARTITIONS", 1024, 1, 100_000)? as i32,
            join_delay: Duration::from_millis(int(
                "QUEEN_KAFKA_GROUP_JOIN_DELAY_MS",
                3000,
                0,
                300_000,
            )? as u64),
            dedup_window_seconds: int("QUEEN_KAFKA_DEDUP_WINDOW_SECONDS", 900, 0, 604_800)?,
            auto_create: flag("QUEEN_KAFKA_AUTO_CREATE", true),
            sasl_required,
            max_connections: int("QUEEN_KAFKA_MAX_CONNECTIONS", 10_000, 1, 1_000_000)? as usize,
            max_request_bytes: int("QUEEN_KAFKA_MAX_REQUEST_BYTES", 100 << 20, 1 << 20, 1 << 30)?
                as usize,
        })
    }

    pub fn advertised(&self) -> String {
        format!("{}:{}", self.advertised_host, self.advertised_port)
    }
}

// ------------------------------------------------------------------ limits
//
// The ceilings that keep one client from spending the broker's memory. Read
// once from the environment, on first use: the facade runs inside the broker,
// so an allocation it cannot make aborts the whole process, and Produce,
// Fetch and the coordinators run on the raft leader only, so a request that
// kills one leader kills each new leader in turn.

use std::sync::OnceLock;

/// `name` from the environment, or `default` when it is unset or not an
/// integer in `min..=max` (said once in the log: a limit must not stop a boot).
fn limit(cell: &'static OnceLock<i64>, name: &str, default: i64, min: i64, max: i64) -> i64 {
    *cell.get_or_init(|| {
        match std::env::var(name).ok().filter(|v| !v.trim().is_empty()) {
            None => default,
            Some(v) => match v.trim().parse::<i64>() {
                Ok(n) if (min..=max).contains(&n) => n,
                _ => {
                    tracing::warn!(target: "kafka", %name, value = %v, default, "not an integer in {min}..={max}; the default applies");
                    default
                }
            },
        }
    })
}

/// The largest frame a connection may send before SASL completes: Kafka's
/// `sasl.server.max.receive.size`. A login is a few hundred bytes, and a
/// client nobody knows yet must not make the broker allocate 100 MiB.
pub const PRE_AUTH_MAX_FRAME_BYTES: usize = 512 << 10;

/// `QUEEN_KAFKA_FRAME_READ_TIMEOUT_MS`, default 30000: how long the rest of a
/// frame may take once its first byte arrived.
pub fn frame_read_timeout() -> Duration {
    static V: OnceLock<i64> = OnceLock::new();
    Duration::from_millis(limit(
        &V,
        "QUEEN_KAFKA_FRAME_READ_TIMEOUT_MS",
        30_000,
        100,
        3_600_000,
    ) as u64)
}

/// `QUEEN_KAFKA_IDLE_TIMEOUT_MS`, default 600000: a connection that sends no
/// request for this long is closed (Kafka's `connections.max.idle.ms`).
pub fn idle_timeout() -> Duration {
    static V: OnceLock<i64> = OnceLock::new();
    Duration::from_millis(limit(
        &V,
        "QUEEN_KAFKA_IDLE_TIMEOUT_MS",
        600_000,
        1_000,
        86_400_000,
    ) as u64)
}

/// `QUEEN_KAFKA_INFLIGHT_BYTES`, default 256 MiB: the request frames every
/// connection together may hold at once. A frame waits for room before its
/// buffer is allocated; its bytes stay in the socket meanwhile.
pub fn inflight_bytes() -> usize {
    static V: OnceLock<i64> = OnceLock::new();
    limit(
        &V,
        "QUEEN_KAFKA_INFLIGHT_BYTES",
        256 << 20,
        1 << 20,
        64 << 30,
    ) as usize
}

/// `QUEEN_KAFKA_FETCH_MAX_BYTES`, default 50 MiB (Kafka's `fetch.max.bytes`):
/// the most one Fetch answer carries, whatever the request asks. A request
/// asking for 0 or less gets this, not an unlimited answer.
pub fn fetch_max_bytes() -> i64 {
    static V: OnceLock<i64> = OnceLock::new();
    limit(
        &V,
        "QUEEN_KAFKA_FETCH_MAX_BYTES",
        50 << 20,
        1 << 10,
        1 << 30,
    )
}

/// `QUEEN_KAFKA_METADATA_MAX_PARTITIONS`, default 100000: the partitions one
/// Metadata answer lists; the topics past it are answered with an error.
pub fn metadata_max_partitions() -> usize {
    static V: OnceLock<i64> = OnceLock::new();
    limit(
        &V,
        "QUEEN_KAFKA_METADATA_MAX_PARTITIONS",
        100_000,
        1,
        10_000_000,
    ) as usize
}

/// `QUEEN_KAFKA_GROUP_MAX_MEMBERS`, default 1000 (Kafka's
/// `group.max.size`): members one consumer group may have.
pub fn group_max_members() -> usize {
    static V: OnceLock<i64> = OnceLock::new();
    limit(&V, "QUEEN_KAFKA_GROUP_MAX_MEMBERS", 1000, 1, 1_000_000) as usize
}

/// `host:port`, `[v6]:port`. The host is kept as written.
pub fn split_host_port(s: &str) -> Option<(String, i32)> {
    let (host, port) = s.rsplit_once(':')?;
    let port: i32 = port.parse().ok().filter(|p| (1..=65535).contains(p))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.is_empty() {
        return None;
    }
    Some((host.to_string(), port))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(pairs: &[(&str, &str)]) -> Result<Config, String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Config::from_lookup(|k| pairs.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone()))
    }

    #[test]
    fn the_advertised_address_is_required_and_parsed() {
        assert!(cfg(&[]).is_err());
        let c = cfg(&[("QUEEN_KAFKA_ADVERTISED_ADDR", "kafka-0.svc:9093")]).unwrap();
        assert_eq!(
            (c.advertised_host.as_str(), c.advertised_port),
            ("kafka-0.svc", 9093)
        );
        assert_eq!(c.listen, "0.0.0.0:9092");
        assert_eq!(c.default_partitions, 1024);
        assert!(cfg(&[("QUEEN_KAFKA_ADVERTISED_ADDR", "nohost")]).is_err());
        let v6 = cfg(&[("QUEEN_KAFKA_ADVERTISED_ADDR", "[::1]:9092")]).unwrap();
        assert_eq!(v6.advertised_host, "::1");
    }

    /// `plain`, the value the first facade's deployments set, requires SASL
    /// as `required` does; a value it does not know stops the boot instead of
    /// leaving every connection unchecked.
    #[test]
    fn sasl_plain_and_required_require_a_login_and_anything_else_is_refused() {
        let with = |v: &str| {
            cfg(&[
                ("QUEEN_KAFKA_ADVERTISED_ADDR", "kafka-0.svc:9092"),
                ("QUEEN_KAFKA_SASL", v),
            ])
        };
        let addr = [("QUEEN_KAFKA_ADVERTISED_ADDR", "kafka-0.svc:9092")];
        assert!(!cfg(&addr).unwrap().sasl_required);
        for v in ["plain", "PLAIN", " plain ", "required"] {
            assert!(with(v).unwrap().sasl_required, "{v}");
        }
        for v in ["off", "none", "false", "0"] {
            assert!(!with(v).unwrap().sasl_required, "{v}");
        }
        let e = with("scram").unwrap_err();
        assert!(e.contains("QUEEN_KAFKA_SASL=scram"), "{e}");
    }
}
