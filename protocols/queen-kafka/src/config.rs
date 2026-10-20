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
            sasl_required: env("QUEEN_KAFKA_SASL").as_deref().map(str::trim) == Some("required"),
            max_connections: int("QUEEN_KAFKA_MAX_CONNECTIONS", 10_000, 1, 1_000_000)? as usize,
            max_request_bytes: int("QUEEN_KAFKA_MAX_REQUEST_BYTES", 100 << 20, 1 << 20, 1 << 30)?
                as usize,
        })
    }

    pub fn advertised(&self) -> String {
        format!("{}:{}", self.advertised_host, self.advertised_port)
    }
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
}
