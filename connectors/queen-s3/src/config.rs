//! The operator's whole surface, read once at boot (plan §6.2).
//!
//! CONTRACT, copied from protocols/queen-sqs/src/config.rs because the embedded
//! mode makes it load-bearing rather than tidy: [`Config::from_env`] is the ONLY
//! reader of the process environment in this binary. `QUEEN_S3_EMBEDDED` has the
//! broker spawn this process and strip its own secrets out of the child's
//! environment (server/src/kafka_facade.rs `STRIPPED_ENV`), so a late
//! `std::env::var` would read a variable that is deliberately no longer there.
//!
//! **Blank is unset.** A Kubernetes manifest, a Compose file and a `.env` all
//! spell "leave this alone" as `""`, and a sink that read that as a zero-length
//! bucket name would fail somewhere far from the line the operator wrote.
//!
//! **Every refusal names the variable and the accepted values**, because the
//! only thing an operator sees is one line on a container that would not start.

use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;

use crate::types::{Align, Compression, Format, Layout, ParquetCodec, Start};
use crate::writer::WriterConfig;

/// The broker this sink talks to when nothing says otherwise — the broker's own
/// default bind, spelled exactly as the two facades spell it.
pub const DEFAULT_QUEEN_URL: &str = "http://localhost:6632";
/// The sink name, and therefore part of every KV key and every bucket path.
pub const DEFAULT_SINK: &str = "default";
/// The bucket prefix everything is written under. `_queen/` sidecars live
/// beneath it too, so one bucket can hold several sinks side by side.
pub const DEFAULT_PREFIX: &str = "queen";
pub const DEFAULT_TARGET_MB: u64 = 128;
pub const DEFAULT_MAX_WINDOW_MS: u64 = 300_000;
pub const DEFAULT_CHECKPOINT_EVERY: u32 = 20;
pub const DEFAULT_MEMORY_MB: u64 = 1024;
pub const DEFAULT_FETCH_CONCURRENCY: u32 = 4;
pub const DEFAULT_DISCOVERY_INTERVAL_MS: u64 = 2_000;
pub const DEFAULT_SAFE_GUARD_MS: u64 = 5_000;
pub const DEFAULT_LISTEN: &str = "127.0.0.1:9333";
pub const DEFAULT_LEASE_TTL_MS: u64 = 30_000;
pub const DEFAULT_MULTIPART_THRESHOLD_MB: u64 = 64;

/// The sink name's alphabet. It is a path segment in the bucket AND a segment
/// of a KV key, so it is restricted to what needs no escaping in either — a
/// name with a `/` in it would be a sink writing into another sink's prefix.
const SINK_MAX: usize = 64;

/// Server-side encryption, as `x-amz-server-side-encryption` spells it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Sse {
    /// No SSE header at all. The bucket's own default still applies — which is
    /// why the deploy page says to set one (plan §6.9).
    Off,
    /// `x-amz-server-side-encryption: AES256`.
    Aes256,
    /// `x-amz-server-side-encryption: aws:kms`, with the key id when one was
    /// given. On a plaintext lake the KMS key policy IS the access control
    /// (plan §4.7, §6.9).
    Kms { key_id: Option<String> },
}

impl Sse {
    pub fn as_str(&self) -> &'static str {
        match self {
            Sse::Off => "off",
            Sse::Aes256 => "AES256",
            Sse::Kms { .. } => "aws:kms",
        }
    }
}

/// Which queues this instance sinks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Queues {
    /// `*` — every queue of the tenant, re-listed every discovery interval.
    /// Several such instances contend for each queue's lease (plan §6.6).
    All,
    /// An explicit list, in the order the operator wrote it.
    Named(Vec<String>),
}

impl Queues {
    pub fn is_all(&self) -> bool {
        matches!(self, Queues::All)
    }
}

/// Where the process is told to die, for the crash matrix of plan §9 (2).
///
/// It is configuration and not a test hook because the matrix kills a REAL
/// process in a REAL container: the whole point is that the restart re-derives
/// window `k` from the intent and rewrites the same bytes, and a fault injected
/// from inside a test harness would not exercise the restart at all.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CrashAt {
    /// Never — the only value a production deployment has.
    Never,
    /// After the intent is committed to KV, before any object exists.
    AfterIntent,
    /// Between two parts of a multipart upload.
    MidUpload,
    /// After the data object lands, before the manifest.
    AfterUpload,
    /// After the manifest lands, before the commit pointer moves.
    BeforeCommit,
    /// After the commit pointer moves, before the checkpoint.
    AfterCommit,
}

impl CrashAt {
    pub fn as_str(self) -> &'static str {
        match self {
            CrashAt::Never => "never",
            CrashAt::AfterIntent => "after_intent",
            CrashAt::MidUpload => "mid_upload",
            CrashAt::AfterUpload => "after_upload",
            CrashAt::BeforeCommit => "before_commit",
            CrashAt::AfterCommit => "after_commit",
        }
    }

    pub fn is_armed(self) -> bool {
        self != CrashAt::Never
    }
}

/// Everything the process was configured with.
#[derive(Clone, PartialEq)]
pub struct Config {
    pub queen_url: String,
    /// The bearer. `None` is a broker with authentication off, which is the
    /// local development stack and nothing else.
    pub queen_token: Option<String>,
    pub sink: String,
    pub queues: Queues,
    /// `QUEEN_S3_PARTITIONS` — the zero-broker-change mode of plan §5.1: the
    /// operator names the lanes, so no discovery call is made. `None` = use
    /// `POST /api/v1/partitions/changed`.
    pub partitions: Option<BTreeMap<String, Vec<String>>>,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    /// No leading and no trailing slash, so `format!("{prefix}/…")` is the one
    /// spelling every key builder uses (see [`crate::layout`]).
    pub prefix: String,
    pub access_key: String,
    pub secret_key: String,
    pub path_style: bool,
    pub sse: Sse,
    pub layout: Layout,
    pub align: Align,
    /// The writer's whole configuration — format, compression, codec and the
    /// determinism pins that go with them (plan §6.4).
    pub writer: WriterConfig,
    pub target_mb: u64,
    pub max_window_ms: u64,
    pub start: Start,
    pub checkpoint_every: u32,
    pub memory_mb: u64,
    pub fetch_concurrency: u32,
    pub discovery_interval_ms: u64,
    pub safe_guard_ms: u64,
    pub listen: SocketAddr,
    pub instance: String,
    /// Whether [`Config::instance`] was generated rather than configured, so
    /// the boot line can say which — a lease held by a name that changes on
    /// every restart never gets handed back, it only expires.
    pub instance_generated: bool,
    pub lease_ttl_ms: u64,
    pub multipart_threshold_mb: u64,
    pub crash_at: CrashAt,
}

/// Hand-written, for three fields: `queen_token`, `secret_key` and the KMS key
/// id. A derived `Debug` prints all three, and the whole value of a redacting
/// one is that it is already there on the day somebody adds `?config` to a
/// route or a panic renders the struct. Same rule, same reason, as both facades.
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("queen_url", &self.queen_url)
            .field("queen_token", &self.queen_token.as_ref().map(|_| "<set>"))
            .field("sink", &self.sink)
            .field("queues", &self.queues)
            .field("partitions", &self.partitions)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("access_key", &self.access_key)
            .field("secret_key", &"<set>")
            .field("path_style", &self.path_style)
            .field("sse", &mask_sse(&self.sse))
            .field("layout", &self.layout)
            .field("align", &self.align)
            .field("writer", &self.writer)
            .field("target_mb", &self.target_mb)
            .field("max_window_ms", &self.max_window_ms)
            .field("start", &self.start)
            .field("checkpoint_every", &self.checkpoint_every)
            .field("memory_mb", &self.memory_mb)
            .field("fetch_concurrency", &self.fetch_concurrency)
            .field("discovery_interval_ms", &self.discovery_interval_ms)
            .field("safe_guard_ms", &self.safe_guard_ms)
            .field("listen", &self.listen)
            .field("instance", &self.instance)
            .field("lease_ttl_ms", &self.lease_ttl_ms)
            .field("multipart_threshold_mb", &self.multipart_threshold_mb)
            .field("crash_at", &self.crash_at)
            .finish()
    }
}

/// The boot line, and the ONE rendering of a `Config` anything is allowed to
/// print. See [`Config::boot_line`].
impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.boot_line())
    }
}

/// `aws:kms` plus the last four characters of the key id, and never more.
///
/// A KMS key id is not a secret the way a signing key is — but it names the
/// account and the key an auditor would go and look at, it ends up in log
/// aggregation by way of the boot line, and four characters is enough for the
/// operator to confirm they set the one they meant.
fn mask_sse(sse: &Sse) -> String {
    match sse {
        Sse::Off => "off".to_string(),
        Sse::Aes256 => "AES256".to_string(),
        Sse::Kms { key_id: None } => "aws:kms".to_string(),
        Sse::Kms { key_id: Some(id) } => {
            let tail: String = id
                .chars()
                .rev()
                .take(4)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            format!("aws:kms(…{tail})")
        }
    }
}

/// An enum as the ENVIRONMENT spells it, not as `Debug` does.
///
/// The boot line is read by whoever set the variables, so it has to say
/// `per-partition` and not `PerPartition`: a line an operator cannot paste back
/// into a manifest is a line that invites a second guess. Every enum here
/// derives `Serialize` with the wire spelling already, so this is that spelling
/// and cannot drift from the parser above.
fn wire<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "?".to_string())
}

impl Config {
    /// Read and validate the whole surface. `Err` names the variable and what
    /// was wrong with it, in the one message an operator sees at boot.
    pub fn from_env() -> Result<Config, String> {
        Config::from_source(&|name| std::env::var(name).ok())
    }

    /// [`Config::from_env`] against a fixed list of pairs — the seam the tests
    /// use.
    ///
    /// It is a real seam and not a convenience: a suite that set process
    /// environment variables could not run its cases in parallel, and since
    /// Rust 1.80 `std::env::set_var` is documented as unsound beside threads.
    /// Every rule below is therefore exercised through a list.
    pub fn from_pairs(pairs: &[(&str, &str)]) -> Result<Config, String> {
        Config::from_source(&|name| {
            pairs
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| (*v).to_string())
        })
    }

    /// [`Config::from_env`] against an arbitrary source.
    pub fn from_source(get: &dyn Fn(&str) -> Option<String>) -> Result<Config, String> {
        let read = |name: &str| -> Option<String> {
            get(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };

        let queen_url = read("QUEEN_URL").unwrap_or_else(|| DEFAULT_QUEEN_URL.to_string());
        let queen_url = crate::queen::normalize_base_url(&queen_url)?;
        let queen_token = read("QUEEN_TOKEN");

        let sink = read("QUEEN_S3_SINK").unwrap_or_else(|| DEFAULT_SINK.to_string());
        validate_sink(&sink)?;

        let queues = match read("QUEEN_S3_QUEUES") {
            None => {
                return Err(
                    "QUEEN_S3_QUEUES is not set: the sink has nothing to read. It is a comma \
                     separated list of queue names, or `*` for every queue of the tenant"
                        .to_string(),
                )
            }
            Some(spec) if spec == "*" => Queues::All,
            Some(spec) => {
                let names: Vec<String> = spec
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if names.is_empty() {
                    return Err(format!(
                        "QUEEN_S3_QUEUES={spec} names no queue. It is a comma separated list of \
                         queue names, or `*` for every queue of the tenant"
                    ));
                }
                Queues::Named(names)
            }
        };

        let partitions = match read("QUEEN_S3_PARTITIONS") {
            None => None,
            Some(spec) => Some(parse_partitions(&spec)?),
        };

        let endpoint = required(
            read("QUEEN_S3_ENDPOINT"),
            "QUEEN_S3_ENDPOINT",
            "the S3 API base URL, e.g. https://s3.eu-central-1.amazonaws.com or http://gw:7070",
        )?;
        validate_endpoint(&endpoint)?;
        let region = required(
            read("QUEEN_S3_REGION"),
            "QUEEN_S3_REGION",
            "the region label the SigV4 credential scope is signed with, e.g. eu-central-1 (any \
             label an S3-compatible gateway accepts, commonly us-east-1)",
        )?;
        let bucket = required(
            read("QUEEN_S3_BUCKET"),
            "QUEEN_S3_BUCKET",
            "the destination bucket",
        )?;
        validate_bucket(&bucket)?;
        let prefix = normalize_prefix(
            &read("QUEEN_S3_PREFIX").unwrap_or_else(|| DEFAULT_PREFIX.to_string()),
        )?;
        let access_key = required(
            read("QUEEN_S3_ACCESS_KEY"),
            "QUEEN_S3_ACCESS_KEY",
            "the S3 access key id",
        )?;
        let secret_key = required(
            read("QUEEN_S3_SECRET_KEY"),
            "QUEEN_S3_SECRET_KEY",
            "the S3 secret access key",
        )?;
        let path_style =
            boolean("QUEEN_S3_PATH_STYLE", read("QUEEN_S3_PATH_STYLE"))?.unwrap_or(false);

        let sse = match read("QUEEN_S3_SSE") {
            None => Sse::Off,
            Some(v) => match v.to_ascii_lowercase().as_str() {
                "aes256" => Sse::Aes256,
                "aws:kms" | "kms" => Sse::Kms {
                    key_id: read("QUEEN_S3_SSE_KMS_KEY_ID"),
                },
                other => {
                    return Err(format!(
                        "QUEEN_S3_SSE={other} is not a mode. It is `AES256` (bucket-managed keys) \
                         or `aws:kms` (+ QUEEN_S3_SSE_KMS_KEY_ID). Unset it to send no \
                         x-amz-server-side-encryption header at all"
                    ))
                }
            },
        };
        // A key id beside AES256 is a policy the operator wrote and this
        // process would silently not apply — the bucket would encrypt with its
        // own key and the deploy would look correct.
        if !matches!(sse, Sse::Kms { .. }) && read("QUEEN_S3_SSE_KMS_KEY_ID").is_some() {
            return Err(format!(
                "QUEEN_S3_SSE_KMS_KEY_ID is set but QUEEN_S3_SSE={} — the key id would be \
                 ignored and every object encrypted with the bucket's own key. Set \
                 QUEEN_S3_SSE=aws:kms, or unset the key id",
                sse.as_str()
            ));
        }

        let format = match read("QUEEN_S3_FORMAT") {
            None => Format::Jsonl,
            Some(v) => match v.to_ascii_lowercase().as_str() {
                "jsonl" | "json" | "ndjson" => Format::Jsonl,
                "parquet" => Format::Parquet,
                other => {
                    return Err(format!(
                        "QUEEN_S3_FORMAT={other} is not a format. It is `jsonl` (one JSON object \
                         per line, every reader takes it) or `parquet`"
                    ))
                }
            },
        };
        let compression = match read("QUEEN_S3_COMPRESSION") {
            None => Compression::Zstd,
            Some(v) => match v.to_ascii_lowercase().as_str() {
                "zstd" | "zst" => Compression::Zstd,
                "gzip" | "gz" => Compression::Gzip,
                "none" | "off" => Compression::None,
                other => {
                    return Err(format!(
                        "QUEEN_S3_COMPRESSION={other} is not a codec. It is `zstd`, `gzip` or \
                         `none`, and it applies to `jsonl` objects — a Parquet file's codec is \
                         QUEEN_S3_PARQUET_CODEC"
                    ))
                }
            },
        };
        let parquet_codec = match read("QUEEN_S3_PARQUET_CODEC") {
            None => ParquetCodec::Zstd,
            Some(v) => match v.to_ascii_lowercase().as_str() {
                "zstd" | "zst" => ParquetCodec::Zstd,
                "snappy" | "snap" => ParquetCodec::Snappy,
                other => {
                    return Err(format!(
                        "QUEEN_S3_PARQUET_CODEC={other} is not a codec. It is `zstd` or `snappy`"
                    ))
                }
            },
        };

        let layout = match read("QUEEN_S3_LAYOUT") {
            None => Layout::Merged,
            Some(v) => match v.to_ascii_lowercase().as_str() {
                "merged" => Layout::Merged,
                "per-partition" | "per_partition" | "perpartition" => Layout::PerPartition,
                other => {
                    return Err(format!(
                        "QUEEN_S3_LAYOUT={other} is not a layout. It is `merged` (one object per \
                         window, partitions as a column — the only shape that survives a million \
                         lanes) or `per-partition` (one object per window per partition)"
                    ))
                }
            },
        };
        let align = match read("QUEEN_S3_ALIGN") {
            None => Align::Hour,
            Some(v) => match v.to_ascii_lowercase().as_str() {
                "hour" | "hourly" => Align::Hour,
                "day" | "daily" => Align::Day,
                "none" | "off" => Align::None,
                other => {
                    return Err(format!(
                        "QUEEN_S3_ALIGN={other} is not an alignment. It is `hour`, `day` or \
                         `none`; it is the Hive bucket a window may not straddle, so dt=/hour= \
                         are exact"
                    ))
                }
            },
        };
        let start = match read("QUEEN_S3_START") {
            None => Start::Latest,
            Some(v) => match v.to_ascii_lowercase().as_str() {
                "latest" | "now" | "end" => Start::Latest,
                "earliest" | "beginning" => Start::Earliest,
                other => {
                    return Err(format!(
                        "QUEEN_S3_START={other} is not a start. It is `latest` (a queue with no \
                         committed pointer starts at the current safeTime) or `earliest` (it \
                         backfills everything retention still holds)"
                    ))
                }
            },
        };

        let target_mb = bounded(
            "QUEEN_S3_TARGET_MB",
            read("QUEEN_S3_TARGET_MB"),
            DEFAULT_TARGET_MB,
            1,
            5_120,
            "the uncompressed buffered bytes a window closes at",
        )?;
        let max_window_ms = bounded(
            "QUEEN_S3_MAX_WINDOW_MS",
            read("QUEEN_S3_MAX_WINDOW_MS"),
            DEFAULT_MAX_WINDOW_MS,
            100,
            86_400_000,
            "how long a window may stay open; the lag SLO is about this plus the safe lag",
        )?;
        let checkpoint_every = bounded(
            "QUEEN_S3_CHECKPOINT_EVERY",
            read("QUEEN_S3_CHECKPOINT_EVERY"),
            DEFAULT_CHECKPOINT_EVERY as u64,
            1,
            100_000,
            "windows between position checkpoints; it bounds the re-read after a restart",
        )? as u32;
        let memory_mb = bounded(
            "QUEEN_S3_MEMORY_MB",
            read("QUEEN_S3_MEMORY_MB"),
            DEFAULT_MEMORY_MB,
            1,
            1_048_576,
            "the global buffer budget across every queue",
        )?;
        let fetch_concurrency = bounded(
            "QUEEN_S3_FETCH_CONCURRENCY",
            read("QUEEN_S3_FETCH_CONCURRENCY"),
            DEFAULT_FETCH_CONCURRENCY as u64,
            1,
            256,
            "in-flight fetch calls per queue; every one of them spends the broker's POP lane \
             admission budget, so it is the throttle a backfill is held back with",
        )? as u32;
        let discovery_interval_ms = bounded(
            "QUEEN_S3_DISCOVERY_INTERVAL_MS",
            read("QUEEN_S3_DISCOVERY_INTERVAL_MS"),
            DEFAULT_DISCOVERY_INTERVAL_MS,
            10,
            3_600_000,
            "how often an idle queue asks which partitions moved",
        )?;
        let safe_guard_ms = bounded(
            "QUEEN_S3_SAFE_GUARD_MS",
            read("QUEEN_S3_SAFE_GUARD_MS"),
            DEFAULT_SAFE_GUARD_MS,
            0,
            3_600_000,
            "subtracted from the broker's safeTime before a window may close; it is ADDED to the \
             broker's own guard and never subtracted from it",
        )?;
        let lease_ttl_ms = bounded(
            "QUEEN_S3_LEASE_TTL_MS",
            read("QUEEN_S3_LEASE_TTL_MS"),
            DEFAULT_LEASE_TTL_MS,
            1_000,
            3_600_000,
            "how long a queue lease survives without a refresh",
        )?;
        let multipart_threshold_mb = bounded(
            "QUEEN_S3_MULTIPART_THRESHOLD_MB",
            read("QUEEN_S3_MULTIPART_THRESHOLD_MB"),
            DEFAULT_MULTIPART_THRESHOLD_MB,
            5,
            5_120,
            "objects at or below this go up as a single PUT with a Content-MD5; above it they go \
             up as a multipart upload",
        )?;

        let listen_raw = read("QUEEN_S3_LISTEN").unwrap_or_else(|| DEFAULT_LISTEN.to_string());
        let listen: SocketAddr = listen_raw.parse().map_err(|e| {
            format!(
                "QUEEN_S3_LISTEN={listen_raw} is not a host:port ({e}). It is where /healthz and \
                 /metrics are served, e.g. {DEFAULT_LISTEN} or 0.0.0.0:9333"
            )
        })?;

        let (instance, instance_generated) = match read("QUEEN_S3_INSTANCE") {
            Some(v) => (v, false),
            None => match hostname() {
                Some(h) => (h, false),
                None => (random_instance(), true),
            },
        };

        let crash_at = match read("QUEEN_S3_CRASH_AT") {
            None => CrashAt::Never,
            Some(v) => match v.to_ascii_lowercase().as_str() {
                "never" | "off" | "none" => CrashAt::Never,
                "after_intent" => CrashAt::AfterIntent,
                "mid_upload" => CrashAt::MidUpload,
                "after_upload" => CrashAt::AfterUpload,
                "before_commit" => CrashAt::BeforeCommit,
                "after_commit" => CrashAt::AfterCommit,
                other => {
                    return Err(format!(
                        "QUEEN_S3_CRASH_AT={other} is not a fault point. It is `after_intent`, \
                         `mid_upload`, `after_upload`, `before_commit` or `after_commit` — the \
                         crash matrix of the test plan. Unset it anywhere that is not a test"
                    ))
                }
            },
        };

        Ok(Config {
            queen_url,
            queen_token,
            sink,
            queues,
            partitions,
            endpoint,
            region,
            bucket,
            prefix,
            access_key,
            secret_key,
            path_style,
            sse,
            layout,
            align,
            writer: WriterConfig {
                format,
                compression,
                parquet_codec,
                ..WriterConfig::default()
            },
            target_mb,
            max_window_ms,
            start,
            checkpoint_every,
            memory_mb,
            fetch_concurrency,
            discovery_interval_ms,
            safe_guard_ms,
            listen,
            instance,
            instance_generated,
            lease_ttl_ms,
            multipart_threshold_mb,
            crash_at,
        })
    }

    /// The one line printed at boot, and the only rendering of a `Config`
    /// anything may log. Never the token, never the secret key, and never a KMS
    /// key id beyond its last four characters.
    pub fn boot_line(&self) -> String {
        let queues = match &self.queues {
            Queues::All => "*".to_string(),
            Queues::Named(names) => names.join(","),
        };
        let partitions = match &self.partitions {
            None => "discovery".to_string(),
            Some(map) => format!("static({} queues)", map.len()),
        };
        let compression = match self.writer.format {
            Format::Jsonl => wire(&self.writer.compression),
            Format::Parquet => wire(&self.writer.parquet_codec),
        };
        format!(
            "queen-s3 {version} sink={sink} instance={instance}{gen} queues={queues} \
             partitions={partitions} queen={queen} token={token} \
             s3={endpoint} bucket={bucket} prefix={prefix} region={region} \
             addressing={addressing} sse={sse} \
             format={format} compression={compression} layout={layout} align={align} \
             target_mb={target} max_window_ms={window} start={start} \
             checkpoint_every={ckpt} memory_mb={memory} fetch_concurrency={fetch} \
             discovery_interval_ms={disc} safe_guard_ms={guard} lease_ttl_ms={lease} \
             multipart_threshold_mb={mp} listen={listen} crash_at={crash}",
            version = crate::writer::CRATE_VERSION,
            sink = self.sink,
            instance = self.instance,
            gen = if self.instance_generated {
                "(generated)"
            } else {
                ""
            },
            queen = self.queen_url,
            token = if self.queen_token.is_some() {
                "<set>"
            } else {
                "<unset>"
            },
            endpoint = self.endpoint,
            bucket = self.bucket,
            prefix = self.prefix,
            region = self.region,
            addressing = if self.path_style {
                "path-style"
            } else {
                "virtual-host"
            },
            sse = mask_sse(&self.sse),
            format = wire(&self.writer.format),
            layout = wire(&self.layout),
            align = wire(&self.align),
            target = self.target_mb,
            window = self.max_window_ms,
            start = wire(&self.start),
            ckpt = self.checkpoint_every,
            memory = self.memory_mb,
            fetch = self.fetch_concurrency,
            disc = self.discovery_interval_ms,
            guard = self.safe_guard_ms,
            lease = self.lease_ttl_ms,
            mp = self.multipart_threshold_mb,
            listen = self.listen,
            crash = self.crash_at.as_str(),
        )
    }

    /// `QUEEN_S3_TARGET_MB` in bytes.
    pub fn target_bytes(&self) -> usize {
        (self.target_mb as usize).saturating_mul(1024 * 1024)
    }

    /// `QUEEN_S3_MEMORY_MB` in bytes.
    pub fn memory_bytes(&self) -> usize {
        (self.memory_mb as usize).saturating_mul(1024 * 1024)
    }

    /// `QUEEN_S3_MULTIPART_THRESHOLD_MB` in bytes.
    pub fn multipart_threshold_bytes(&self) -> usize {
        (self.multipart_threshold_mb as usize).saturating_mul(1024 * 1024)
    }
}

// ---------------------------------------------------------------------------
// Parsing helpers — every one of them names its variable in the failure.
// ---------------------------------------------------------------------------

fn required(v: Option<String>, name: &str, what: &str) -> Result<String, String> {
    v.ok_or_else(|| format!("{name} is not set: it is {what}"))
}

fn boolean(name: &str, v: Option<String>) -> Result<Option<bool>, String> {
    match v {
        None => Ok(None),
        Some(raw) => match raw.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(Some(true)),
            "false" | "0" | "no" | "off" => Ok(Some(false)),
            other => Err(format!(
                "{name}={other} is not a boolean. It is `true` or `false`"
            )),
        },
    }
}

fn bounded(
    name: &str,
    v: Option<String>,
    default: u64,
    min: u64,
    max: u64,
    what: &str,
) -> Result<u64, String> {
    let Some(raw) = v else { return Ok(default) };
    let n: u64 = raw.parse().map_err(|_| {
        format!("{name}={raw} is not a whole number. It is {what}, in {min}..={max}")
    })?;
    if n < min || n > max {
        return Err(format!("{name}={n} is outside {min}..={max}. It is {what}"));
    }
    Ok(n)
}

fn validate_sink(sink: &str) -> Result<(), String> {
    let ok = !sink.is_empty()
        && sink.len() <= SINK_MAX
        && sink
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-');
    if ok {
        return Ok(());
    }
    Err(format!(
        "QUEEN_S3_SINK={sink} is not a sink name. It is 1..={SINK_MAX} characters of \
         [A-Za-z0-9._-]: the name is a path segment in the bucket AND a segment of every KV key, \
         so anything else would be a sink writing into another sink's prefix"
    ))
}

fn validate_bucket(bucket: &str) -> Result<(), String> {
    if bucket.contains('/') || bucket.contains(' ') || bucket.is_empty() {
        return Err(format!(
            "QUEEN_S3_BUCKET={bucket} is not a bucket name: it is one name, with no slash and no \
             space. A prefix inside the bucket is QUEEN_S3_PREFIX"
        ));
    }
    Ok(())
}

fn validate_endpoint(endpoint: &str) -> Result<(), String> {
    let rest = endpoint
        .strip_prefix("https://")
        .or_else(|| endpoint.strip_prefix("http://"))
        .ok_or_else(|| {
            format!(
                "QUEEN_S3_ENDPOINT={endpoint} has no scheme: it starts with http:// or https://"
            )
        })?;
    let authority = rest.split('/').next().unwrap_or("");
    if authority.is_empty() {
        return Err(format!("QUEEN_S3_ENDPOINT={endpoint} has no host"));
    }
    // A path on the endpoint would silently become part of every object key,
    // and the bucket would be addressed under it. QUEEN_S3_PREFIX is the field
    // for that, and it is escaped and checked.
    if rest.trim_end_matches('/').contains('/') {
        return Err(format!(
            "QUEEN_S3_ENDPOINT={endpoint} carries a path. The endpoint is scheme://host[:port] \
             only; a prefix inside the bucket is QUEEN_S3_PREFIX"
        ));
    }
    Ok(())
}

/// No leading and no trailing slash, no `..`, no empty segment.
fn normalize_prefix(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim_matches('/');
    if trimmed.is_empty() {
        return Err(format!(
            "QUEEN_S3_PREFIX={raw} is empty. It is the root every object is written under, e.g. \
             `{DEFAULT_PREFIX}` or `lake/queen`"
        ));
    }
    for segment in trimmed.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(format!(
                "QUEEN_S3_PREFIX={raw} has an empty or relative segment ({segment:?}). It is a \
                 plain path, e.g. `{DEFAULT_PREFIX}` or `lake/queen`"
            ));
        }
    }
    Ok(trimmed.to_string())
}

/// `queue:0..1023,queue2:a,b,c` — the static list mode of plan §5.1.
///
/// The comma separates BOTH the queues and the names inside one queue, which is
/// what the plan's example spells, so the parse rule is: a token carrying a `:`
/// opens a new queue, and every token after it without one extends that queue's
/// list. A `A..B` token expands to the decimal names in that inclusive range —
/// the Kafka-shaped case this mode exists for.
fn parse_partitions(spec: &str) -> Result<BTreeMap<String, Vec<String>>, String> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut current: Option<String> = None;
    for token in spec.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let (queue, item) = match token.split_once(':') {
            Some((q, rest)) => {
                let q = q.trim();
                if q.is_empty() {
                    return Err(format!(
                        "QUEEN_S3_PARTITIONS={spec} has a `:` with no queue in front of it. The \
                         syntax is queue:0..1023,queue2:a,b,c"
                    ));
                }
                current = Some(q.to_string());
                out.entry(q.to_string()).or_default();
                (q.to_string(), rest.trim().to_string())
            }
            None => match &current {
                Some(q) => (q.clone(), token.to_string()),
                None => {
                    return Err(format!(
                        "QUEEN_S3_PARTITIONS={spec} starts with a partition name and no queue. \
                         The syntax is queue:0..1023,queue2:a,b,c"
                    ))
                }
            },
        };
        if item.is_empty() {
            continue;
        }
        let names = expand_range(&item, spec)?;
        out.entry(queue).or_default().extend(names);
    }
    if out.is_empty() || out.values().all(|v| v.is_empty()) {
        return Err(format!(
            "QUEEN_S3_PARTITIONS={spec} names no partition. The syntax is \
             queue:0..1023,queue2:a,b,c; unset the variable to discover partitions through \
             POST /api/v1/partitions/changed instead"
        ));
    }
    Ok(out)
}

/// The widest static range one token may expand to. A typo (`0..1000000000`)
/// would otherwise allocate for a lifetime before the sink says anything.
const MAX_RANGE: i64 = 1_000_000;

fn expand_range(item: &str, spec: &str) -> Result<Vec<String>, String> {
    let Some((lo, hi)) = item.split_once("..") else {
        return Ok(vec![item.to_string()]);
    };
    let parse = |s: &str| -> Result<i64, String> {
        s.trim().parse::<i64>().map_err(|_| {
            format!(
                "QUEEN_S3_PARTITIONS={spec}: `{item}` is not a range. A range is two whole \
                 numbers, `0..1023`, and it is inclusive at both ends"
            )
        })
    };
    let (lo, hi) = (parse(lo)?, parse(hi)?);
    if hi < lo {
        return Err(format!(
            "QUEEN_S3_PARTITIONS={spec}: `{item}` runs backwards ({lo} > {hi})"
        ));
    }
    if hi - lo + 1 > MAX_RANGE {
        return Err(format!(
            "QUEEN_S3_PARTITIONS={spec}: `{item}` is {} names, more than the {MAX_RANGE} a static \
             list may hold. Unset QUEEN_S3_PARTITIONS and let discovery find them",
            hi - lo + 1
        ));
    }
    Ok((lo..=hi).map(|n| n.to_string()).collect())
}

/// The host's name, for the lease identity. `None` when the platform will not
/// say — a container with no `HOSTNAME` — and the caller mints a random id.
fn hostname() -> Option<String> {
    std::env::var("HOSTNAME")
        .ok()
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
}

/// A random instance id, for a host that has no name to give.
///
/// It changes on every restart, which is exactly why the boot line says the id
/// was generated: a lease held under a name nobody keeps is never handed back,
/// it only expires, and a queue is then idle for a lease TTL after every
/// restart.
fn random_instance() -> String {
    use rand::Rng;
    let n: u64 = rand::thread_rng().gen();
    format!("s3-{n:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The smallest configuration that starts: everything without a default.
    fn base() -> Vec<(&'static str, &'static str)> {
        vec![
            ("QUEEN_S3_QUEUES", "orders"),
            ("QUEEN_S3_ENDPOINT", "https://s3.example.com"),
            ("QUEEN_S3_REGION", "eu-central-1"),
            ("QUEEN_S3_BUCKET", "lake"),
            ("QUEEN_S3_ACCESS_KEY", "AKIA"),
            ("QUEEN_S3_SECRET_KEY", "shhh"),
        ]
    }

    fn with(extra: &[(&'static str, &'static str)]) -> Result<Config, String> {
        let mut pairs = base();
        for (k, v) in extra {
            pairs.retain(|(existing, _)| existing != k);
            pairs.push((k, v));
        }
        Config::from_pairs(&pairs)
    }

    #[test]
    fn defaults_are_the_plan_table() {
        let c = with(&[]).unwrap();
        assert_eq!(c.queen_url, DEFAULT_QUEEN_URL);
        assert_eq!(c.queen_token, None);
        assert_eq!(c.sink, "default");
        assert_eq!(c.queues, Queues::Named(vec!["orders".to_string()]));
        assert_eq!(c.partitions, None);
        assert_eq!(c.prefix, "queen");
        assert!(!c.path_style);
        assert_eq!(c.sse, Sse::Off);
        assert_eq!(c.writer.format, Format::Jsonl);
        assert_eq!(c.writer.compression, Compression::Zstd);
        assert_eq!(c.writer.parquet_codec, ParquetCodec::Zstd);
        assert_eq!(c.layout, Layout::Merged);
        assert_eq!(c.align, Align::Hour);
        assert_eq!(c.target_mb, 128);
        assert_eq!(c.max_window_ms, 300_000);
        assert_eq!(c.start, Start::Latest);
        assert_eq!(c.checkpoint_every, 20);
        assert_eq!(c.memory_mb, 1024);
        assert_eq!(c.fetch_concurrency, 4);
        assert_eq!(c.discovery_interval_ms, 2_000);
        assert_eq!(c.safe_guard_ms, 5_000);
        assert_eq!(c.listen.to_string(), "127.0.0.1:9333");
        assert_eq!(c.lease_ttl_ms, 30_000);
        assert_eq!(c.multipart_threshold_mb, 64);
        assert_eq!(c.crash_at, CrashAt::Never);
        assert!(!c.instance.is_empty());
    }

    #[test]
    fn every_variable_without_a_default_is_named_when_it_is_missing() {
        for missing in [
            "QUEEN_S3_QUEUES",
            "QUEEN_S3_ENDPOINT",
            "QUEEN_S3_REGION",
            "QUEEN_S3_BUCKET",
            "QUEEN_S3_ACCESS_KEY",
            "QUEEN_S3_SECRET_KEY",
        ] {
            let pairs: Vec<(&str, &str)> =
                base().into_iter().filter(|(k, _)| *k != missing).collect();
            let err = Config::from_pairs(&pairs).unwrap_err();
            assert!(err.contains(missing), "{missing} missing said: {err}");
        }
    }

    #[test]
    fn blank_is_unset() {
        let c = with(&[("QUEEN_S3_SINK", "   "), ("QUEEN_TOKEN", "")]).unwrap();
        assert_eq!(c.sink, "default");
        assert_eq!(c.queen_token, None);
    }

    #[test]
    fn every_enum_parses_and_every_bad_value_names_the_alternatives() {
        assert_eq!(
            with(&[("QUEEN_S3_FORMAT", "parquet")])
                .unwrap()
                .writer
                .format,
            Format::Parquet
        );
        assert_eq!(
            with(&[("QUEEN_S3_COMPRESSION", "gzip")])
                .unwrap()
                .writer
                .compression,
            Compression::Gzip
        );
        assert_eq!(
            with(&[("QUEEN_S3_COMPRESSION", "none")])
                .unwrap()
                .writer
                .compression,
            Compression::None
        );
        assert_eq!(
            with(&[("QUEEN_S3_PARQUET_CODEC", "snappy")])
                .unwrap()
                .writer
                .parquet_codec,
            ParquetCodec::Snappy
        );
        assert_eq!(
            with(&[("QUEEN_S3_LAYOUT", "per-partition")])
                .unwrap()
                .layout,
            Layout::PerPartition
        );
        assert_eq!(
            with(&[("QUEEN_S3_ALIGN", "day")]).unwrap().align,
            Align::Day
        );
        assert_eq!(
            with(&[("QUEEN_S3_ALIGN", "none")]).unwrap().align,
            Align::None
        );
        assert_eq!(
            with(&[("QUEEN_S3_START", "earliest")]).unwrap().start,
            Start::Earliest
        );
        assert_eq!(
            with(&[("QUEEN_S3_SSE", "AES256")]).unwrap().sse,
            Sse::Aes256
        );
        assert_eq!(
            with(&[
                ("QUEEN_S3_SSE", "aws:kms"),
                ("QUEEN_S3_SSE_KMS_KEY_ID", "abcd1234")
            ])
            .unwrap()
            .sse,
            Sse::Kms {
                key_id: Some("abcd1234".to_string())
            }
        );
        assert!(with(&[("QUEEN_S3_PATH_STYLE", "true")]).unwrap().path_style);

        for (var, bad, expect) in [
            ("QUEEN_S3_FORMAT", "avro", "jsonl"),
            ("QUEEN_S3_COMPRESSION", "lz4", "zstd"),
            ("QUEEN_S3_PARQUET_CODEC", "brotli", "snappy"),
            ("QUEEN_S3_LAYOUT", "flat", "merged"),
            ("QUEEN_S3_ALIGN", "minute", "hour"),
            ("QUEEN_S3_START", "middle", "latest"),
            ("QUEEN_S3_SSE", "rot13", "AES256"),
            ("QUEEN_S3_PATH_STYLE", "maybe", "boolean"),
            ("QUEEN_S3_CRASH_AT", "whenever", "after_intent"),
        ] {
            let err = with(&[(var, bad)]).unwrap_err();
            assert!(err.contains(var), "{var}={bad} said: {err}");
            assert!(err.contains(bad), "{var}={bad} said: {err}");
            assert!(
                err.contains(expect),
                "{var}={bad} did not offer {expect}: {err}"
            );
        }
    }

    #[test]
    fn every_crash_point_parses() {
        for (raw, want) in [
            ("after_intent", CrashAt::AfterIntent),
            ("mid_upload", CrashAt::MidUpload),
            ("after_upload", CrashAt::AfterUpload),
            ("before_commit", CrashAt::BeforeCommit),
            ("after_commit", CrashAt::AfterCommit),
            ("never", CrashAt::Never),
        ] {
            assert_eq!(with(&[("QUEEN_S3_CRASH_AT", raw)]).unwrap().crash_at, want);
        }
        assert!(CrashAt::AfterIntent.is_armed());
        assert!(!CrashAt::Never.is_armed());
    }

    #[test]
    fn queues_takes_a_list_or_a_star() {
        assert_eq!(
            with(&[("QUEEN_S3_QUEUES", "*")]).unwrap().queues,
            Queues::All
        );
        assert!(with(&[("QUEEN_S3_QUEUES", "*")]).unwrap().queues.is_all());
        assert_eq!(
            with(&[("QUEEN_S3_QUEUES", "a, b ,c")]).unwrap().queues,
            Queues::Named(vec!["a".into(), "b".into(), "c".into()])
        );
        assert!(with(&[("QUEEN_S3_QUEUES", ",,")])
            .unwrap_err()
            .contains("QUEEN_S3_QUEUES"));
    }

    #[test]
    fn static_partitions_take_ranges_and_lists() {
        let c = with(&[("QUEEN_S3_PARTITIONS", "orders:0..3,clicks:a,b,c")]).unwrap();
        let map = c.partitions.unwrap();
        assert_eq!(map["orders"], vec!["0", "1", "2", "3"]);
        assert_eq!(map["clicks"], vec!["a", "b", "c"]);
    }

    #[test]
    fn static_partitions_refuse_what_cannot_be_meant() {
        for bad in ["a,b,c", "orders:9..0", "orders:0..2000000", ":x", "orders:"] {
            let err = with(&[("QUEEN_S3_PARTITIONS", bad)]).unwrap_err();
            assert!(err.contains("QUEEN_S3_PARTITIONS"), "{bad} said: {err}");
        }
    }

    #[test]
    fn sink_name_alphabet_is_enforced() {
        assert_eq!(
            with(&[("QUEEN_S3_SINK", "lake.eu-1_2")]).unwrap().sink,
            "lake.eu-1_2"
        );
        for bad in ["a/b", "a b", "a:b", &"x".repeat(65)] {
            let err = with(&[("QUEEN_S3_SINK", Box::leak(bad.to_string().into_boxed_str()))])
                .unwrap_err();
            assert!(err.contains("QUEEN_S3_SINK"), "{bad} said: {err}");
        }
    }

    #[test]
    fn prefix_loses_its_slashes_and_refuses_relative_segments() {
        assert_eq!(
            with(&[("QUEEN_S3_PREFIX", "/lake/queen/")]).unwrap().prefix,
            "lake/queen"
        );
        for bad in ["/", "lake//queen", "lake/../etc", "lake/./queen"] {
            let err = with(&[("QUEEN_S3_PREFIX", bad)]).unwrap_err();
            assert!(err.contains("QUEEN_S3_PREFIX"), "{bad} said: {err}");
        }
    }

    #[test]
    fn endpoint_must_be_a_bare_origin() {
        assert!(with(&[("QUEEN_S3_ENDPOINT", "http://gw:7070")]).is_ok());
        assert!(with(&[("QUEEN_S3_ENDPOINT", "http://gw:7070/")]).is_ok());
        for bad in ["gw:7070", "http://", "https://s3.example.com/lake"] {
            let err = with(&[("QUEEN_S3_ENDPOINT", bad)]).unwrap_err();
            assert!(err.contains("QUEEN_S3_ENDPOINT"), "{bad} said: {err}");
        }
    }

    #[test]
    fn numbers_are_bounded_and_the_refusal_says_the_range() {
        assert_eq!(
            with(&[("QUEEN_S3_TARGET_MB", "512")]).unwrap().target_mb,
            512
        );
        for (var, bad) in [
            ("QUEEN_S3_TARGET_MB", "0"),
            ("QUEEN_S3_TARGET_MB", "banana"),
            ("QUEEN_S3_CHECKPOINT_EVERY", "0"),
            ("QUEEN_S3_FETCH_CONCURRENCY", "0"),
            ("QUEEN_S3_MULTIPART_THRESHOLD_MB", "1"),
            ("QUEEN_S3_LEASE_TTL_MS", "10"),
            ("QUEEN_S3_MAX_WINDOW_MS", "1"),
        ] {
            let err = with(&[(var, bad)]).unwrap_err();
            assert!(err.contains(var), "{var}={bad} said: {err}");
        }
    }

    #[test]
    fn a_kms_key_beside_aes256_is_refused_rather_than_ignored() {
        let err =
            with(&[("QUEEN_S3_SSE", "AES256"), ("QUEEN_S3_SSE_KMS_KEY_ID", "k")]).unwrap_err();
        assert!(err.contains("QUEEN_S3_SSE_KMS_KEY_ID"), "{err}");
    }

    #[test]
    fn listen_must_be_a_socket_address() {
        assert_eq!(
            with(&[("QUEEN_S3_LISTEN", "0.0.0.0:1234")])
                .unwrap()
                .listen
                .to_string(),
            "0.0.0.0:1234"
        );
        assert!(with(&[("QUEEN_S3_LISTEN", "nowhere")])
            .unwrap_err()
            .contains("QUEEN_S3_LISTEN"));
    }

    #[test]
    fn queen_url_is_normalized_and_checked() {
        assert_eq!(
            with(&[("QUEEN_URL", "http://broker:6632/")])
                .unwrap()
                .queen_url,
            "http://broker:6632"
        );
        assert!(with(&[("QUEEN_URL", "ftp://broker")])
            .unwrap_err()
            .contains("QUEEN_URL"));
    }

    #[test]
    fn the_boot_line_never_carries_a_secret() {
        let c = with(&[
            ("QUEEN_TOKEN", "sk_live_do_not_print_me"),
            ("QUEEN_S3_SECRET_KEY", "wJalrXUtnFEMI"),
            ("QUEEN_S3_SSE", "aws:kms"),
            (
                "QUEEN_S3_SSE_KMS_KEY_ID",
                "arn:aws:kms:eu-central-1:1234:key/beefcafe",
            ),
        ])
        .unwrap();
        let line = c.boot_line();
        assert!(!line.contains("sk_live_do_not_print_me"), "{line}");
        assert!(!line.contains("wJalrXUtnFEMI"), "{line}");
        assert!(!line.contains("beefcafe"), "{line}");
        assert!(line.contains("aws:kms(…cafe)"), "{line}");
        assert!(line.contains("token=<set>"), "{line}");
        assert!(line.contains("bucket=lake"), "{line}");
        // Display is boot_line, and Debug redacts the same three fields.
        assert_eq!(format!("{c}"), line);
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("sk_live_do_not_print_me"), "{dbg}");
        assert!(!dbg.contains("wJalrXUtnFEMI"), "{dbg}");
        assert!(!dbg.contains("beefcafe"), "{dbg}");
    }

    #[test]
    fn the_boot_line_spells_the_enums_the_way_the_environment_does() {
        let c = with(&[
            ("QUEEN_S3_FORMAT", "parquet"),
            ("QUEEN_S3_PARQUET_CODEC", "snappy"),
            ("QUEEN_S3_LAYOUT", "per-partition"),
            ("QUEEN_S3_ALIGN", "day"),
            ("QUEEN_S3_START", "earliest"),
        ])
        .unwrap();
        let line = c.boot_line();
        assert!(line.contains("format=parquet"), "{line}");
        assert!(line.contains("compression=snappy"), "{line}");
        assert!(line.contains("layout=per-partition"), "{line}");
        assert!(line.contains("align=day"), "{line}");
        assert!(line.contains("start=earliest"), "{line}");
        // A jsonl sink reports the JSONL codec, not the Parquet one.
        let c = with(&[("QUEEN_S3_COMPRESSION", "gzip")]).unwrap();
        assert!(
            c.boot_line().contains("format=jsonl compression=gzip"),
            "{}",
            c.boot_line()
        );
    }

    #[test]
    fn byte_helpers_agree_with_the_megabytes() {
        let c = with(&[("QUEEN_S3_TARGET_MB", "2"), ("QUEEN_S3_MEMORY_MB", "3")]).unwrap();
        assert_eq!(c.target_bytes(), 2 * 1024 * 1024);
        assert_eq!(c.memory_bytes(), 3 * 1024 * 1024);
        assert_eq!(c.multipart_threshold_bytes(), 64 * 1024 * 1024);
    }
}
