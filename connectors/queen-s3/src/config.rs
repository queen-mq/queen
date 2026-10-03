//! The operator's whole surface (plan §6.2), in two halves.
//!
//! **Node-wide** ([`NodeKnobs`]): what one node has one of, whichever tenants'
//! sinks it runs — the memory budget they all share, the fetch concurrency, the
//! discovery interval, the safe guard, the lease TTL, the multipart threshold,
//! the checkpoint cadence, the lease instance, the crash point. Environment
//! only, read once at the broker's boot ([`NodeKnobs::from_env_with`]).
//!
//! **Per tenant** ([`Config`]): one sink of one broker tenant — its queues, its
//! bucket and credentials, its format and layout, its window. The default
//! tenant's comes from the `QUEEN_S3_*` environment ([`Config::from_env`]),
//! turned on by `QUEEN_S3_QUEUES`; every other tenant's from its control-plane
//! document ([`Config::from_tenant_doc`]), the same settings spelled in
//! camelCase. One rule reads each setting whichever source states it; only the
//! name a message uses differs.
//!
//! The broker reads `QUEEN_S3_EMBEDDED` (its switch, never read here). The
//! environment readers here are the ONLY readers of the process environment in
//! this crate, and they run at boot: nothing reads a variable later, so what
//! the boot line prints is what the sink runs with. Every node of a cluster is
//! configured identically; the per-queue lease decides which node writes which
//! queue.
//!
//! **Blank is unset.** A Kubernetes manifest, a Compose file and a `.env` all
//! spell "leave this alone" as `""`, and a sink that read that as a zero-length
//! bucket name would fail somewhere far from the line the operator wrote.
//!
//! **Every refusal names the variable — or the document field — and the
//! accepted values**, because the broker fails its boot with that one line, and
//! a control plane answers a refused document with it.

use std::collections::BTreeMap;
use std::fmt;

use serde_json::Value;

use crate::types::{Align, Compression, Format, Layout, ParquetCodec, Start};
use crate::writer::WriterConfig;

/// The broker's default tenant (server/src/config.rs `DEFAULT_TENANT`): the
/// tenant of every request on a broker without tenancy, and the one the
/// environment's sink runs for. Every object key names its tenant, so a
/// single-tenant broker's keys carry this one.
pub const DEFAULT_TENANT: &str = "00000000-0000-0000-0000-000000000001";

/// The sink name. It scopes the sink's KV documents (lease, intent, commit
/// pointer: `s3:<sink>:<queue>:…`), is recorded in every intent and manifest,
/// and is what a queue's `retentionSinkHold` names. It is in no object key.
pub const DEFAULT_SINK: &str = "default";
/// The bucket prefix everything is written under, `_queen/` sidecars included.
/// It is the only thing that tells two sinks apart in a bucket: an object key
/// is the prefix, the queue and the window, with no sink name in it, so two
/// sinks writing the same queue into one bucket use different prefixes.
pub const DEFAULT_PREFIX: &str = "queen";
pub const DEFAULT_TARGET_MB: u64 = 128;
pub const DEFAULT_MAX_WINDOW_MS: u64 = 300_000;
pub const DEFAULT_CHECKPOINT_EVERY: u32 = 20;
/// The buffer budget across every queue this node owns. The buffers live in the
/// broker's own process, beside its caches and its log, so the default is half
/// of what the standalone 1.5.0 sink took for itself; a node that owns many
/// queues with large windows raises it.
pub const DEFAULT_MEMORY_MB: u64 = 512;
pub const DEFAULT_FETCH_CONCURRENCY: u32 = 4;
pub const DEFAULT_DISCOVERY_INTERVAL_MS: u64 = 2_000;
pub const DEFAULT_SAFE_GUARD_MS: u64 = 5_000;
pub const DEFAULT_LEASE_TTL_MS: u64 = 30_000;
pub const DEFAULT_MULTIPART_THRESHOLD_MB: u64 = 64;

/// Variables of the standalone 1.5.0 sink that mean nothing in-process: the
/// health listener (the broker serves [`crate::Sink::status`] and
/// [`crate::Sink::prometheus`] itself) and the log format (the broker owns
/// tracing). Set, they are reported once at boot and otherwise ignored —
/// refusing them would fail a broker's boot over a line left in a manifest.
pub const IGNORED_VARIABLES: &[&str] = &["QUEEN_S3_LISTEN", "QUEEN_S3_LOG_FORMAT"];

/// The sink name's alphabet. The name is a segment of every KV key the sink
/// writes (`s3:<sink>:<queue>:…`, lease.rs), so it is restricted to what needs
/// no escaping there: with `:` excluded, one sink's `s3:<sink>:` is never a
/// prefix of another sink's keys. It is in no object key (layout.rs), so it
/// does not separate two sinks in the bucket — `QUEEN_S3_PREFIX` does.
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

/// Which queues the sink runs. Every node runs the same set; each queue's lease
/// decides which node writes it (plan §6.6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Queues {
    /// `*` — every queue of the tenant, re-listed every ten discovery
    /// intervals, so a queue created later is picked up.
    All,
    /// An explicit list, in the order the operator wrote it.
    Named(Vec<String>),
}

impl Queues {
    pub fn is_all(&self) -> bool {
        matches!(self, Queues::All)
    }
}

/// Where the sink is told to die, for the crash matrix of plan §9 (2). TEST
/// ONLY: never set it on a node that serves anything.
///
/// It is configuration and not a test hook because the matrix kills a REAL
/// process: the whole point is that the restart re-derives window `k` from the
/// intent and rewrites the same bytes, and a fault injected from inside a test
/// harness would not exercise the restart at all. In-process, the crash is
/// `std::process::abort()` of the BROKER (`crate::driver::CrashMode::Abort`) —
/// a node crash, which is exactly what an end-to-end crash test wants: the
/// queue's lease expires and another node, or this one restarted, redoes the
/// window.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CrashAt {
    /// Never — the only value a production deployment has.
    Never,
    /// After the intent is committed to KV, before any object exists.
    AfterIntent,
    /// Between two parts of a multipart upload — or, for a window small enough
    /// to go up in one PUT, after its first data object and before its
    /// manifest.
    MidUpload,
    /// After the window's objects and its manifest land, before the commit
    /// step starts.
    AfterUpload,
    /// In the commit step, before the commit pointer's KV write.
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

/// Everything the sink was configured with.
#[derive(Clone, PartialEq)]
pub struct Config {
    /// The broker tenant this sink runs for: the `tenant=` key of every object
    /// it writes, and the scope of its manifests and checkpoints.
    pub tenant: String,
    /// The `tenant` label its series carry ([`Config::with_tenant_label`]);
    /// `None` writes none.
    pub tenant_label: Option<String>,
    pub sink: String,
    pub queues: Queues,
    /// `QUEEN_S3_PARTITIONS` — the static-list mode plan §5.1 sketched. Parsed
    /// so that its refusal can name the variable
    /// ([`crate::driver::reject_static_partitions`]); `None` = discovery.
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
    // The node-wide knobs, copied from the [`NodeKnobs`] this configuration
    // was built with ([`Config::node_knobs`] gives them back).
    pub checkpoint_every: u32,
    pub memory_mb: u64,
    pub fetch_concurrency: u32,
    pub discovery_interval_ms: u64,
    pub safe_guard_ms: u64,
    /// Who this node is in a queue lease (`QUEEN_S3_INSTANCE`, or the default
    /// the broker passes — its node identity).
    pub instance: String,
    /// Whether [`Config::instance`] was generated rather than configured, so
    /// the boot line can say which — a lease held by a name that changes on
    /// every restart never gets handed back, it only expires.
    pub instance_generated: bool,
    pub lease_ttl_ms: u64,
    pub multipart_threshold_mb: u64,
    pub crash_at: CrashAt,
    /// The [`IGNORED_VARIABLES`] that were set, for the one warning at boot.
    pub ignored: Vec<&'static str>,
}

/// Hand-written, for two fields: `secret_key` and the KMS key id. A derived
/// `Debug` prints both, and the whole value of a redacting one is that it is
/// already there on the day somebody adds `?config` to a route or a panic
/// renders the struct. Same rule, same reason, as both facades.
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("tenant", &self.tenant)
            .field("tenant_label", &self.tenant_label)
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
            .field("instance", &self.instance)
            .field("lease_ttl_ms", &self.lease_ttl_ms)
            .field("multipart_threshold_mb", &self.multipart_threshold_mb)
            .field("crash_at", &self.crash_at)
            .field("ignored", &self.ignored)
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
pub(crate) fn wire<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "?".to_string())
}

// ---------------------------------------------------------------------------
// Node-wide knobs
// ---------------------------------------------------------------------------

/// What is node-wide: one value per node, read from the environment only,
/// whichever tenants' sinks the node runs. The memory budget is the ONE budget
/// every sink of the node shares ([`crate::SinkShared`]); the instance is who
/// this node is in every tenant's leases; the rest are the pace and the
/// margins of the node's reads, which no tenant's document may change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeKnobs {
    /// `QUEEN_S3_MEMORY_MB`: the buffer budget across every queue of every
    /// sink this node runs.
    pub memory_mb: u64,
    /// `QUEEN_S3_FETCH_CONCURRENCY`: in-flight fetch calls per queue.
    pub fetch_concurrency: u32,
    /// `QUEEN_S3_DISCOVERY_INTERVAL_MS`.
    pub discovery_interval_ms: u64,
    /// `QUEEN_S3_SAFE_GUARD_MS`.
    pub safe_guard_ms: u64,
    /// `QUEEN_S3_LEASE_TTL_MS`.
    pub lease_ttl_ms: u64,
    /// `QUEEN_S3_MULTIPART_THRESHOLD_MB`.
    pub multipart_threshold_mb: u64,
    /// `QUEEN_S3_CHECKPOINT_EVERY`.
    pub checkpoint_every: u32,
    /// Who this node is in a queue lease (`QUEEN_S3_INSTANCE`, or the default
    /// the broker passes — its node identity).
    pub instance: String,
    /// Whether [`NodeKnobs::instance`] was generated rather than configured, so
    /// the boot line can say which — a lease held by a name that changes on
    /// every restart never gets handed back, it only expires.
    pub instance_generated: bool,
    /// `QUEEN_S3_CRASH_AT` — test only.
    pub crash_at: CrashAt,
    /// The [`IGNORED_VARIABLES`] that were set, for the one warning at boot.
    pub ignored: Vec<&'static str>,
}

/// The node-wide variables, by name: what a tenant document may not set.
const NODE_VARIABLES: &[(&str, &str)] = &[
    ("memoryMb", "QUEEN_S3_MEMORY_MB"),
    ("fetchConcurrency", "QUEEN_S3_FETCH_CONCURRENCY"),
    ("discoveryIntervalMs", "QUEEN_S3_DISCOVERY_INTERVAL_MS"),
    ("safeGuardMs", "QUEEN_S3_SAFE_GUARD_MS"),
    ("leaseTtlMs", "QUEEN_S3_LEASE_TTL_MS"),
    ("multipartThresholdMb", "QUEEN_S3_MULTIPART_THRESHOLD_MB"),
    ("checkpointEvery", "QUEEN_S3_CHECKPOINT_EVERY"),
    ("instance", "QUEEN_S3_INSTANCE"),
    ("crashAt", "QUEEN_S3_CRASH_AT"),
];

impl NodeKnobs {
    /// Read and validate the node-wide variables from the process environment.
    /// `Err` names the variable and what was wrong with it, in the one line
    /// the broker fails its boot with.
    ///
    /// `instance_default` is who this node is in a queue lease when
    /// `QUEEN_S3_INSTANCE` is not set: the broker passes its node identity. A
    /// blank one gets a random id, which the boot line marks as generated.
    pub fn from_env_with(instance_default: &str) -> Result<NodeKnobs, String> {
        NodeKnobs::from_source(&|name| std::env::var(name).ok(), instance_default)
    }

    /// [`NodeKnobs::from_env_with`] against a fixed list of pairs — the seam
    /// the tests use (a suite that set process environment variables could
    /// not run its cases in parallel, and since Rust 1.80 `std::env::set_var`
    /// is documented as unsound beside threads).
    pub fn from_pairs_with(
        pairs: &[(&str, &str)],
        instance_default: &str,
    ) -> Result<NodeKnobs, String> {
        NodeKnobs::from_source(&pairs_source(pairs), instance_default)
    }

    /// [`NodeKnobs::from_env_with`] against an arbitrary source.
    pub fn from_source(
        get: &dyn Fn(&str) -> Option<String>,
        instance_default: &str,
    ) -> Result<NodeKnobs, String> {
        let read = |name: &str| trimmed(get(name));
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
            "the buffer budget across every queue of every sink this node runs, inside the \
             broker's process",
        )?;
        let fetch_concurrency = bounded(
            "QUEEN_S3_FETCH_CONCURRENCY",
            read("QUEEN_S3_FETCH_CONCURRENCY"),
            DEFAULT_FETCH_CONCURRENCY as u64,
            1,
            256,
            "in-flight fetch calls per queue; each one is a read of this node's log on the \
             broker's blocking pool, so it is the throttle a backfill is held back with",
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
            "subtracted from the node's safeTime before a window may close: a margin on the \
             broker's clock, paid in latency",
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

        let (instance, instance_generated) = match read("QUEEN_S3_INSTANCE") {
            Some(v) => (v, false),
            None => match instance_default.trim() {
                "" => (random_instance(), true),
                node => (node.to_string(), false),
            },
        };

        let ignored: Vec<&'static str> = IGNORED_VARIABLES
            .iter()
            .copied()
            .filter(|name| read(name).is_some())
            .collect();

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

        Ok(NodeKnobs {
            memory_mb,
            fetch_concurrency,
            discovery_interval_ms,
            safe_guard_ms,
            lease_ttl_ms,
            multipart_threshold_mb,
            checkpoint_every,
            instance,
            instance_generated,
            crash_at,
            ignored,
        })
    }

    /// `QUEEN_S3_MEMORY_MB` in bytes.
    pub fn memory_bytes(&self) -> usize {
        (self.memory_mb as usize).saturating_mul(1024 * 1024)
    }
}

// ---------------------------------------------------------------------------
// Per-tenant settings, from the environment or from a tenant document
// ---------------------------------------------------------------------------

/// One per-tenant setting. The rule that reads it is the same whichever source
/// states it; only the spelling of its name differs — `QUEEN_S3_FORMAT` in the
/// environment, `format` in a tenant document — and every message names the
/// setting the way its source spells it.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Field {
    Queues,
    Partitions,
    Endpoint,
    Region,
    Bucket,
    Prefix,
    AccessKey,
    SecretKey,
    PathStyle,
    Sse,
    SseKmsKeyId,
    Format,
    Compression,
    ParquetCodec,
    Layout,
    Align,
    Start,
    TargetMb,
    MaxWindowMs,
    Sink,
}

impl Field {
    const ALL: [Field; 20] = [
        Field::Queues,
        Field::Partitions,
        Field::Endpoint,
        Field::Region,
        Field::Bucket,
        Field::Prefix,
        Field::AccessKey,
        Field::SecretKey,
        Field::PathStyle,
        Field::Sse,
        Field::SseKmsKeyId,
        Field::Format,
        Field::Compression,
        Field::ParquetCodec,
        Field::Layout,
        Field::Align,
        Field::Start,
        Field::TargetMb,
        Field::MaxWindowMs,
        Field::Sink,
    ];

    fn env(self) -> &'static str {
        match self {
            Field::Queues => "QUEEN_S3_QUEUES",
            Field::Partitions => "QUEEN_S3_PARTITIONS",
            Field::Endpoint => "QUEEN_S3_ENDPOINT",
            Field::Region => "QUEEN_S3_REGION",
            Field::Bucket => "QUEEN_S3_BUCKET",
            Field::Prefix => "QUEEN_S3_PREFIX",
            Field::AccessKey => "QUEEN_S3_ACCESS_KEY",
            Field::SecretKey => "QUEEN_S3_SECRET_KEY",
            Field::PathStyle => "QUEEN_S3_PATH_STYLE",
            Field::Sse => "QUEEN_S3_SSE",
            Field::SseKmsKeyId => "QUEEN_S3_SSE_KMS_KEY_ID",
            Field::Format => "QUEEN_S3_FORMAT",
            Field::Compression => "QUEEN_S3_COMPRESSION",
            Field::ParquetCodec => "QUEEN_S3_PARQUET_CODEC",
            Field::Layout => "QUEEN_S3_LAYOUT",
            Field::Align => "QUEEN_S3_ALIGN",
            Field::Start => "QUEEN_S3_START",
            Field::TargetMb => "QUEEN_S3_TARGET_MB",
            Field::MaxWindowMs => "QUEEN_S3_MAX_WINDOW_MS",
            Field::Sink => "QUEEN_S3_SINK",
        }
    }

    /// The field's name in a tenant document. `None` for the two that are not
    /// one: the secret key, which is passed beside the document so the
    /// document can be stored and shown, and the static partition list, which
    /// is refused anyway.
    fn json(self) -> Option<&'static str> {
        Some(match self {
            Field::Queues => "queues",
            Field::Endpoint => "endpoint",
            Field::Region => "region",
            Field::Bucket => "bucket",
            Field::Prefix => "prefix",
            Field::AccessKey => "accessKey",
            Field::PathStyle => "pathStyle",
            Field::Sse => "sse",
            Field::SseKmsKeyId => "sseKmsKeyId",
            Field::Format => "format",
            Field::Compression => "compression",
            Field::ParquetCodec => "parquetCodec",
            Field::Layout => "layout",
            Field::Align => "align",
            Field::Start => "start",
            Field::TargetMb => "targetMb",
            Field::MaxWindowMs => "maxWindowMs",
            Field::Sink => "sink",
            Field::Partitions | Field::SecretKey => return None,
        })
    }
}

/// Where per-tenant settings are read from, and how a message names each.
trait Settings {
    /// The text of a setting, trimmed; `None` when it is absent or blank.
    fn text(&self, f: Field) -> Result<Option<String>, String>;
    /// The setting's name in this source's spelling.
    fn name(&self, f: Field) -> &'static str;
    /// A list setting stated as a list (a JSON array of strings); `None` when
    /// it is not stated that way.
    fn list(&self, _f: Field) -> Result<Option<Vec<String>>, String> {
        Ok(None)
    }
}

/// The process environment, or the tests' pairs.
struct EnvSettings<'a> {
    get: &'a dyn Fn(&str) -> Option<String>,
}

impl Settings for EnvSettings<'_> {
    fn text(&self, f: Field) -> Result<Option<String>, String> {
        Ok(trimmed((self.get)(f.env())))
    }

    fn name(&self, f: Field) -> &'static str {
        f.env()
    }
}

/// A tenant document's object.
struct DocSettings<'a> {
    doc: &'a serde_json::Map<String, Value>,
}

impl Settings for DocSettings<'_> {
    fn text(&self, f: Field) -> Result<Option<String>, String> {
        let Some(key) = f.json() else {
            return Ok(None);
        };
        let numeric = matches!(f, Field::TargetMb | Field::MaxWindowMs);
        match self.doc.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) => Ok(trimmed(Some(s.clone()))),
            Some(Value::Bool(b)) if f == Field::PathStyle => Ok(Some(b.to_string())),
            Some(Value::Number(n)) if numeric => Ok(Some(n.to_string())),
            Some(Value::Array(_)) if f == Field::Queues => Ok(None),
            Some(other) => Err(format!(
                "{key}={other} is not {}",
                match f {
                    Field::PathStyle => "a boolean: it is true or false",
                    _ if numeric => "a whole number",
                    Field::Queues => "a list of queues: it is \"*\", \"a,b\" or [\"a\",\"b\"]",
                    _ => "a string",
                }
            )),
        }
    }

    fn name(&self, f: Field) -> &'static str {
        f.json().unwrap_or(f.env())
    }

    fn list(&self, f: Field) -> Result<Option<Vec<String>>, String> {
        let Some(key) = f.json() else {
            return Ok(None);
        };
        let Some(Value::Array(items)) = self.doc.get(key) else {
            return Ok(None);
        };
        items
            .iter()
            .map(|item| match item {
                Value::String(s) => Ok(s.trim().to_string()),
                other => Err(format!(
                    "{key} holds {other}, which is not a queue name: the list is of strings"
                )),
            })
            .collect::<Result<Vec<String>, String>>()
            .map(Some)
    }
}

/// Every per-tenant setting but the secret key, parsed and checked.
struct TenantSettings {
    sink: String,
    queues: Queues,
    partitions: Option<BTreeMap<String, Vec<String>>>,
    endpoint: String,
    region: String,
    bucket: String,
    prefix: String,
    access_key: String,
    path_style: bool,
    sse: Sse,
    format: Format,
    compression: Compression,
    parquet_codec: ParquetCodec,
    layout: Layout,
    align: Align,
    start: Start,
    target_mb: u64,
    max_window_ms: u64,
}

/// The queue set, `None` when the source states none.
fn queues_of(s: &dyn Settings) -> Result<Option<Queues>, String> {
    let name = s.name(Field::Queues);
    if let Some(list) = s.list(Field::Queues)? {
        if list.iter().any(|q| q == "*") {
            return Err(format!(
                "{name} lists `*` among queue names: every queue of the tenant is the string \
                 \"*\" on its own"
            ));
        }
        let names: Vec<String> = list.into_iter().filter(|q| !q.is_empty()).collect();
        if names.is_empty() {
            return Err(format!(
                "{name} names no queue. It is a list of queue names, or `*` for every queue of \
                 the tenant"
            ));
        }
        return Ok(Some(Queues::Named(names)));
    }
    let Some(spec) = s.text(Field::Queues)? else {
        return Ok(None);
    };
    if spec == "*" {
        return Ok(Some(Queues::All));
    }
    let names: Vec<String> = spec
        .split(',')
        .map(|q| q.trim().to_string())
        .filter(|q| !q.is_empty())
        .collect();
    if names.is_empty() {
        return Err(format!(
            "{name}={spec} names no queue. It is a comma separated list of queue names, or `*` \
             for every queue of the tenant"
        ));
    }
    Ok(Some(Queues::Named(names)))
}

/// Everything per tenant but the queue set and the secret key, which the two
/// sources state differently.
fn tenant_settings(s: &dyn Settings, queues: Queues) -> Result<TenantSettings, String> {
    let n = |f: Field| s.name(f);

    let sink = s
        .text(Field::Sink)?
        .unwrap_or_else(|| DEFAULT_SINK.to_string());
    validate_sink(n(Field::Sink), n(Field::Prefix), &sink)?;

    let partitions = match s.text(Field::Partitions)? {
        None => None,
        Some(spec) => Some(parse_partitions(&spec)?),
    };

    let endpoint = required(
        s.text(Field::Endpoint)?,
        n(Field::Endpoint),
        "the S3 API base URL, e.g. https://s3.eu-central-1.amazonaws.com or http://gw:7070",
    )?;
    validate_endpoint(n(Field::Endpoint), n(Field::Prefix), &endpoint)?;
    let region = required(
        s.text(Field::Region)?,
        n(Field::Region),
        "the region label the SigV4 credential scope is signed with, e.g. eu-central-1 (any \
         label an S3-compatible gateway accepts, commonly us-east-1)",
    )?;
    let bucket = required(
        s.text(Field::Bucket)?,
        n(Field::Bucket),
        "the destination bucket",
    )?;
    validate_bucket(n(Field::Bucket), n(Field::Prefix), &bucket)?;
    let prefix = normalize_prefix(
        n(Field::Prefix),
        &s.text(Field::Prefix)?
            .unwrap_or_else(|| DEFAULT_PREFIX.to_string()),
    )?;
    let access_key = required(
        s.text(Field::AccessKey)?,
        n(Field::AccessKey),
        "the S3 access key id",
    )?;
    let path_style = boolean(n(Field::PathStyle), s.text(Field::PathStyle)?)?.unwrap_or(false);

    let kms_key = s.text(Field::SseKmsKeyId)?;
    let sse = match s.text(Field::Sse)? {
        None => Sse::Off,
        Some(v) => match v.to_ascii_lowercase().as_str() {
            "aes256" => Sse::Aes256,
            "aws:kms" | "kms" => Sse::Kms {
                key_id: kms_key.clone(),
            },
            other => {
                return Err(format!(
                    "{}={other} is not a mode. It is `AES256` (bucket-managed keys) or `aws:kms` \
                     (+ {}). Unset it to send no x-amz-server-side-encryption header at all",
                    n(Field::Sse),
                    n(Field::SseKmsKeyId)
                ))
            }
        },
    };
    // A key id beside AES256 is a policy the operator wrote and this process
    // would silently not apply — the bucket would encrypt with its own key and
    // the deploy would look correct.
    if !matches!(sse, Sse::Kms { .. }) && kms_key.is_some() {
        return Err(format!(
            "{} is set but {}={} — the key id would be ignored and every object encrypted with \
             the bucket's own key. Set {}=aws:kms, or unset the key id",
            n(Field::SseKmsKeyId),
            n(Field::Sse),
            sse.as_str(),
            n(Field::Sse)
        ));
    }

    let format = match s.text(Field::Format)? {
        None => Format::Jsonl,
        Some(v) => match v.to_ascii_lowercase().as_str() {
            "jsonl" | "json" | "ndjson" => Format::Jsonl,
            "parquet" => Format::Parquet,
            other => {
                return Err(format!(
                    "{}={other} is not a format. It is `jsonl` (one JSON object per line, every \
                     reader takes it) or `parquet`",
                    n(Field::Format)
                ))
            }
        },
    };
    let compression = match s.text(Field::Compression)? {
        None => Compression::Zstd,
        Some(v) => match v.to_ascii_lowercase().as_str() {
            "zstd" | "zst" => Compression::Zstd,
            "gzip" | "gz" => Compression::Gzip,
            "none" | "off" => Compression::None,
            other => {
                return Err(format!(
                    "{}={other} is not a codec. It is `zstd`, `gzip` or `none`, and it applies \
                     to `jsonl` objects — a Parquet file's codec is {}",
                    n(Field::Compression),
                    n(Field::ParquetCodec)
                ))
            }
        },
    };
    let parquet_codec = match s.text(Field::ParquetCodec)? {
        None => ParquetCodec::Zstd,
        Some(v) => match v.to_ascii_lowercase().as_str() {
            "zstd" | "zst" => ParquetCodec::Zstd,
            "snappy" | "snap" => ParquetCodec::Snappy,
            other => {
                return Err(format!(
                    "{}={other} is not a codec. It is `zstd` or `snappy`",
                    n(Field::ParquetCodec)
                ))
            }
        },
    };

    let layout = match s.text(Field::Layout)? {
        None => Layout::Merged,
        Some(v) => match v.to_ascii_lowercase().as_str() {
            "merged" => Layout::Merged,
            "per-partition" | "per_partition" | "perpartition" => Layout::PerPartition,
            other => {
                return Err(format!(
                    "{}={other} is not a layout. It is `merged` (one object per window, \
                     partitions as a column — the only shape that survives a million lanes) or \
                     `per-partition` (one object per window per partition)",
                    n(Field::Layout)
                ))
            }
        },
    };
    let align = match s.text(Field::Align)? {
        None => Align::Hour,
        Some(v) => match v.to_ascii_lowercase().as_str() {
            "hour" | "hourly" => Align::Hour,
            "day" | "daily" => Align::Day,
            "none" | "off" => Align::None,
            other => {
                return Err(format!(
                    "{}={other} is not an alignment. It is `hour`, `day` or `none`; it is the \
                     Hive bucket a window may not straddle, so dt=/hour= are exact",
                    n(Field::Align)
                ))
            }
        },
    };
    let start = match s.text(Field::Start)? {
        None => Start::Latest,
        Some(v) => match v.to_ascii_lowercase().as_str() {
            "latest" | "now" | "end" => Start::Latest,
            "earliest" | "beginning" => Start::Earliest,
            other => {
                return Err(format!(
                    "{}={other} is not a start. It is `latest` (a queue with no committed \
                     pointer starts at the current safeTime) or `earliest` (it backfills \
                     everything retention still holds)",
                    n(Field::Start)
                ))
            }
        },
    };

    let target_mb = bounded(
        n(Field::TargetMb),
        s.text(Field::TargetMb)?,
        DEFAULT_TARGET_MB,
        1,
        5_120,
        "the uncompressed buffered bytes a window closes at",
    )?;
    let max_window_ms = bounded(
        n(Field::MaxWindowMs),
        s.text(Field::MaxWindowMs)?,
        DEFAULT_MAX_WINDOW_MS,
        100,
        86_400_000,
        "how long a window may stay open; the lag SLO is about this plus the safe lag",
    )?;

    Ok(TenantSettings {
        sink,
        queues,
        partitions,
        endpoint,
        region,
        bucket,
        prefix,
        access_key,
        path_style,
        sse,
        format,
        compression,
        parquet_codec,
        layout,
        align,
        start,
        target_mb,
        max_window_ms,
    })
}

/// The fields a tenant document may hold, for the refusal of one it may not.
fn document_fields() -> String {
    Field::ALL
        .iter()
        .filter_map(|f| f.json())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The document's object, every key a known per-tenant field.
fn document_object(doc: &Value) -> Result<&serde_json::Map<String, Value>, String> {
    let Value::Object(map) = doc else {
        return Err(format!(
            "a tenant's sink is a JSON object of its settings, not {}",
            match doc {
                Value::Null => "null",
                Value::Bool(_) => "a boolean",
                Value::Number(_) => "a number",
                Value::String(_) => "a string",
                Value::Array(_) => "a list",
                Value::Object(_) => "an object",
            }
        ));
    };
    for key in map.keys() {
        if Field::ALL.iter().any(|f| f.json() == Some(key.as_str())) {
            continue;
        }
        if key == "secretKey" {
            return Err(
                "secretKey is not a field of the document: the secret key is passed beside it, \
                 so the document can be stored and shown without it"
                    .to_string(),
            );
        }
        if let Some((_, var)) = NODE_VARIABLES.iter().find(|(field, _)| field == key) {
            return Err(format!(
                "{key} is node-wide, not a setting of one tenant's sink: it is {var} in the \
                 environment of every node"
            ));
        }
        return Err(format!(
            "{key} is not a field of a tenant's sink. The fields are: {}",
            document_fields()
        ));
    }
    Ok(map)
}

/// Check a tenant document exactly as [`Config::from_tenant_doc`] does, minus
/// the secret key, which a control plane holds apart from the document: what
/// it accepts, [`Config::from_tenant_doc`] accepts. `Err` names the field and
/// what was wrong with it.
pub fn validate_tenant_doc(doc: &Value) -> Result<(), String> {
    tenant_doc_settings(doc).map(|_| ())
}

fn tenant_doc_settings(doc: &Value) -> Result<TenantSettings, String> {
    let map = document_object(doc)?;
    let s = DocSettings { doc: map };
    let queues = queues_of(&s)?.ok_or_else(|| {
        "queues is not set: it is the queues the sink writes — a list of queue names (\"a,b\" or \
         [\"a\",\"b\"]), or \"*\" for every queue of the tenant"
            .to_string()
    })?;
    tenant_settings(&s, queues)
}

/// A function over a fixed list of pairs, as a [`NodeKnobs::from_source`] or
/// [`Config::from_env_source`] source.
fn pairs_source<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
    move |name: &str| {
        pairs
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| (*v).to_string())
    }
}

fn trimmed(v: Option<String>) -> Option<String> {
    v.map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

impl Config {
    /// The environment's sink — the one `QUEEN_S3_*` configures, for
    /// `tenant` (the broker passes its default tenant).
    ///
    /// `Ok(None)` when `QUEEN_S3_QUEUES` is unset: this node runs no
    /// environment sink, only the tenants' sinks a control plane configures.
    /// With it set, every variable without a default is required as ever.
    /// With it unset and another per-tenant `QUEEN_S3_*` variable set (a
    /// bucket, an endpoint, a key…), the configuration is refused: that sink
    /// would never start, and the line says that `QUEEN_S3_QUEUES` is what
    /// turns it on. `Err` names the variable and what was wrong with it.
    pub fn from_env(node: &NodeKnobs, tenant: &str) -> Result<Option<Config>, String> {
        Config::from_env_source(node, tenant, &|name| std::env::var(name).ok())
    }

    /// [`Config::from_env`] against a fixed list of pairs: the tests' seam.
    pub fn from_env_pairs(
        node: &NodeKnobs,
        tenant: &str,
        pairs: &[(&str, &str)],
    ) -> Result<Option<Config>, String> {
        Config::from_env_source(node, tenant, &pairs_source(pairs))
    }

    /// [`Config::from_env`] against an arbitrary source.
    pub fn from_env_source(
        node: &NodeKnobs,
        tenant: &str,
        get: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Option<Config>, String> {
        let s = EnvSettings { get };
        let Some(queues) = queues_of(&s)? else {
            if let Some(set) = Field::ALL
                .iter()
                .find(|f| **f != Field::Queues && s.text(**f).ok().flatten().is_some())
            {
                return Err(format!(
                    "{} is set but QUEEN_S3_QUEUES is not: QUEEN_S3_QUEUES is what turns the \
                     environment's sink on — the queues it writes, or `*` for every queue of \
                     the tenant. Set it, or unset the sink's QUEEN_S3_* variables to run only \
                     the sinks a control plane configures",
                    set.env()
                ));
            }
            return Ok(None);
        };
        let settings = tenant_settings(&s, queues)?;
        let secret_key = required(
            s.text(Field::SecretKey)?,
            "QUEEN_S3_SECRET_KEY",
            "the S3 secret access key",
        )?;
        Config::assemble(node, tenant, settings, secret_key).map(Some)
    }

    /// A tenant's sink from its control-plane document: `doc` is a JSON object
    /// of the per-tenant settings with camelCase names, and `secret_key` the S3
    /// secret, which the document never holds.
    ///
    /// Required: `endpoint`, `region`, `bucket`, `accessKey`, `queues` (`"*"`,
    /// `"a,b"` or `["a","b"]`). Optional, with the defaults, ranges and rules
    /// of their `QUEEN_S3_*` variables: `prefix`, `pathStyle`, `sse`,
    /// `sseKmsKeyId`, `format`, `compression`, `parquetCodec`, `layout`,
    /// `align`, `start`, `targetMb`, `maxWindowMs`, `sink`. Any other field is
    /// refused — a typo is not silently ignored — and a node-wide one
    /// (`memoryMb`, `leaseTtlMs`, …) is refused naming its variable. `Err`
    /// names the field and what was wrong with it.
    pub fn from_tenant_doc(
        node: &NodeKnobs,
        tenant: &str,
        doc: &Value,
        secret_key: &str,
    ) -> Result<Config, String> {
        let settings = tenant_doc_settings(doc)?;
        let secret_key = required(
            trimmed(Some(secret_key.to_string())),
            "the secret key",
            "the S3 secret access key that goes with accessKey",
        )?;
        let mut cfg = Config::assemble(node, tenant, settings, secret_key)?;
        // The environment's leftovers are the node's to report, once — not
        // every tenant sink's.
        cfg.ignored.clear();
        Ok(cfg)
    }

    /// The tenant label this sink's per-queue and per-sink series carry:
    /// `None` writes no tenant label (the broker's default tenant, whose
    /// families stay as a single-tenant broker exports them), `Some(id)`
    /// writes `tenant="<id>"`. `None` until set.
    pub fn with_tenant_label(mut self, label: Option<String>) -> Config {
        self.tenant_label = label;
        self
    }

    /// The node-wide knobs this configuration carries a copy of.
    pub fn node_knobs(&self) -> NodeKnobs {
        NodeKnobs {
            memory_mb: self.memory_mb,
            fetch_concurrency: self.fetch_concurrency,
            discovery_interval_ms: self.discovery_interval_ms,
            safe_guard_ms: self.safe_guard_ms,
            lease_ttl_ms: self.lease_ttl_ms,
            multipart_threshold_mb: self.multipart_threshold_mb,
            checkpoint_every: self.checkpoint_every,
            instance: self.instance.clone(),
            instance_generated: self.instance_generated,
            crash_at: self.crash_at,
            ignored: self.ignored.clone(),
        }
    }

    fn assemble(
        node: &NodeKnobs,
        tenant: &str,
        t: TenantSettings,
        secret_key: String,
    ) -> Result<Config, String> {
        let tenant = tenant.trim();
        if tenant.is_empty() {
            return Err(
                "the tenant is empty: every object key names the broker tenant its sink runs for"
                    .to_string(),
            );
        }
        Ok(Config {
            tenant: tenant.to_string(),
            tenant_label: None,
            sink: t.sink,
            queues: t.queues,
            partitions: t.partitions,
            endpoint: t.endpoint,
            region: t.region,
            bucket: t.bucket,
            prefix: t.prefix,
            access_key: t.access_key,
            secret_key,
            path_style: t.path_style,
            sse: t.sse,
            layout: t.layout,
            align: t.align,
            writer: WriterConfig {
                format: t.format,
                compression: t.compression,
                parquet_codec: t.parquet_codec,
                tenant: Some(tenant.to_string()),
                ..WriterConfig::default()
            },
            target_mb: t.target_mb,
            max_window_ms: t.max_window_ms,
            start: t.start,
            checkpoint_every: node.checkpoint_every,
            memory_mb: node.memory_mb,
            fetch_concurrency: node.fetch_concurrency,
            discovery_interval_ms: node.discovery_interval_ms,
            safe_guard_ms: node.safe_guard_ms,
            instance: node.instance.clone(),
            instance_generated: node.instance_generated,
            lease_ttl_ms: node.lease_ttl_ms,
            multipart_threshold_mb: node.multipart_threshold_mb,
            crash_at: node.crash_at,
            ignored: node.ignored.clone(),
        })
    }

    // -- one call, one sink: the single-tenant shape ------------------------

    /// The environment's node knobs and its sink for the broker's default
    /// tenant, in one call — for a caller that runs exactly one sink. The same
    /// as [`NodeKnobs::from_env_with`] then [`Config::from_env`] with
    /// [`DEFAULT_TENANT`], except that an unset `QUEEN_S3_QUEUES` is an error:
    /// this caller has no other sink to run.
    pub fn from_env_with(instance_default: &str) -> Result<Config, String> {
        Config::from_source(&|name| std::env::var(name).ok(), instance_default)
    }

    /// [`Config::from_env_with`] against a fixed list of pairs, with no instance
    /// default — the seam the tests use.
    pub fn from_pairs(pairs: &[(&str, &str)]) -> Result<Config, String> {
        Config::from_pairs_with(pairs, "")
    }

    /// [`Config::from_pairs`] with an instance default.
    pub fn from_pairs_with(
        pairs: &[(&str, &str)],
        instance_default: &str,
    ) -> Result<Config, String> {
        Config::from_source(&pairs_source(pairs), instance_default)
    }

    /// [`Config::from_env_with`] against an arbitrary source.
    pub fn from_source(
        get: &dyn Fn(&str) -> Option<String>,
        instance_default: &str,
    ) -> Result<Config, String> {
        let node = NodeKnobs::from_source(get, instance_default)?;
        Config::from_env_source(&node, DEFAULT_TENANT, get)?.ok_or_else(|| {
            "QUEEN_S3_QUEUES is not set: the sink has nothing to read. It is a comma separated \
             list of queue names, or `*` for every queue of the tenant"
                .to_string()
        })
    }

    /// The one line logged at boot, and the only rendering of a `Config`
    /// anything may log. Never the secret key, and never a KMS key id beyond
    /// its last four characters.
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
            "queen-s3 {version} tenant={tenant} sink={sink} instance={instance}{gen} queues={queues} \
             partitions={partitions} \
             s3={endpoint} bucket={bucket} prefix={prefix} region={region} \
             addressing={addressing} sse={sse} \
             format={format} compression={compression} layout={layout} align={align} \
             target_mb={target} max_window_ms={window} start={start} \
             checkpoint_every={ckpt} memory_mb={memory} fetch_concurrency={fetch} \
             discovery_interval_ms={disc} safe_guard_ms={guard} lease_ttl_ms={lease} \
             multipart_threshold_mb={mp} crash_at={crash}",
            version = crate::writer::CRATE_VERSION,
            tenant = self.tenant,
            sink = self.sink,
            instance = self.instance,
            gen = if self.instance_generated {
                "(generated)"
            } else {
                ""
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

fn validate_sink(name: &str, prefix_name: &str, sink: &str) -> Result<(), String> {
    let ok = !sink.is_empty()
        && sink.len() <= SINK_MAX
        && sink
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-');
    if ok {
        return Ok(());
    }
    Err(format!(
        "{name}={sink} is not a sink name. It is 1..={SINK_MAX} characters of [A-Za-z0-9._-]: \
         the name scopes the sink's KV documents (lease, intent, commit pointer) and anything \
         else could run into another sink's keys. It is in no object key: two sinks writing the \
         same queue use different {prefix_name} values"
    ))
}

fn validate_bucket(name: &str, prefix_name: &str, bucket: &str) -> Result<(), String> {
    if bucket.contains('/') || bucket.contains(' ') || bucket.is_empty() {
        return Err(format!(
            "{name}={bucket} is not a bucket name: it is one name, with no slash and no space. A \
             prefix inside the bucket is {prefix_name}"
        ));
    }
    Ok(())
}

fn validate_endpoint(name: &str, prefix_name: &str, endpoint: &str) -> Result<(), String> {
    let rest = endpoint
        .strip_prefix("https://")
        .or_else(|| endpoint.strip_prefix("http://"))
        .ok_or_else(|| {
            format!("{name}={endpoint} has no scheme: it starts with http:// or https://")
        })?;
    let authority = rest.split('/').next().unwrap_or("");
    if authority.is_empty() {
        return Err(format!("{name}={endpoint} has no host"));
    }
    // A path on the endpoint would silently become part of every object key,
    // and the bucket would be addressed under it. The prefix is the field for
    // that, and it is escaped and checked.
    if rest.trim_end_matches('/').contains('/') {
        return Err(format!(
            "{name}={endpoint} carries a path. The endpoint is scheme://host[:port] only; a \
             prefix inside the bucket is {prefix_name}"
        ));
    }
    Ok(())
}

/// No leading and no trailing slash, no `..`, no empty segment.
fn normalize_prefix(name: &str, raw: &str) -> Result<String, String> {
    let trimmed = raw.trim_matches('/');
    if trimmed.is_empty() {
        return Err(format!(
            "{name}={raw} is empty. It is the root every object is written under, e.g. \
             `{DEFAULT_PREFIX}` or `lake/queen`"
        ));
    }
    for segment in trimmed.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(format!(
                "{name}={raw} has an empty or relative segment ({segment:?}). It is a plain path, \
                 e.g. `{DEFAULT_PREFIX}` or `lake/queen`"
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

/// A random instance id, for a broker that passed no node identity.
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
        assert_eq!(
            c.memory_mb, 512,
            "in-process: half of the standalone 1.5.0 default"
        );
        assert_eq!(c.fetch_concurrency, 4);
        assert_eq!(c.discovery_interval_ms, 2_000);
        assert_eq!(c.safe_guard_ms, 5_000);
        assert_eq!(c.lease_ttl_ms, 30_000);
        assert_eq!(c.multipart_threshold_mb, 64);
        assert_eq!(c.crash_at, CrashAt::Never);
        assert!(c.ignored.is_empty());
        assert!(!c.instance.is_empty());
        assert!(
            c.instance_generated,
            "no QUEEN_S3_INSTANCE and no default: a random id, marked as such"
        );
    }

    #[test]
    fn the_instance_is_the_variable_then_the_broker_s_node_identity() {
        let pairs = base();
        let c = Config::from_pairs_with(&pairs, "node-2").unwrap();
        assert_eq!(c.instance, "node-2");
        assert!(!c.instance_generated);
        assert!(!c.boot_line().contains("(generated)"));

        let mut pairs = base();
        pairs.push(("QUEEN_S3_INSTANCE", "lake-writer-a"));
        let c = Config::from_pairs_with(&pairs, "node-2").unwrap();
        assert_eq!(c.instance, "lake-writer-a", "the operator's name wins");

        let c = Config::from_pairs_with(&base(), "   ").unwrap();
        assert!(c.instance_generated, "a blank default is no default");
        assert!(c.boot_line().contains("(generated)"));
    }

    #[test]
    fn the_standalone_variables_are_ignored_and_named_never_refused() {
        // QUEEN_S3_EMBEDDED is the broker's switch: set or not, nothing here
        // reads it, and nothing refuses it.
        let c = with(&[
            ("QUEEN_S3_EMBEDDED", "true"),
            ("QUEEN_S3_LISTEN", "0.0.0.0:9333"),
            ("QUEEN_S3_LOG_FORMAT", "json"),
        ])
        .unwrap();
        assert_eq!(c.ignored, vec!["QUEEN_S3_LISTEN", "QUEEN_S3_LOG_FORMAT"]);
        // The broker's own client variables are not the sink's business at
        // all: other components of the broker read them.
        let c = with(&[("QUEEN_URL", "http://x:1"), ("QUEEN_TOKEN", "t")]).unwrap();
        assert!(c.ignored.is_empty());
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
        let c = with(&[("QUEEN_S3_SINK", "   "), ("QUEEN_S3_PREFIX", "")]).unwrap();
        assert_eq!(c.sink, "default");
        assert_eq!(c.prefix, "queen");
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
            // The refusal says what the name is for: the KV documents. It is in
            // no object key (layout.rs), so the message points at the prefix as
            // what separates two sinks in a bucket, and claims no path segment.
            assert!(err.contains("KV documents"), "{bad} said: {err}");
            assert!(err.contains("QUEEN_S3_PREFIX"), "{bad} said: {err}");
            assert!(!err.contains("path segment"), "{bad} said: {err}");
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
    fn the_boot_line_never_carries_a_secret() {
        let c = with(&[
            ("QUEEN_S3_SECRET_KEY", "wJalrXUtnFEMI"),
            ("QUEEN_S3_SSE", "aws:kms"),
            (
                "QUEEN_S3_SSE_KMS_KEY_ID",
                "arn:aws:kms:eu-central-1:1234:key/beefcafe",
            ),
        ])
        .unwrap();
        let line = c.boot_line();
        assert!(!line.contains("wJalrXUtnFEMI"), "{line}");
        assert!(!line.contains("beefcafe"), "{line}");
        assert!(line.contains("aws:kms(…cafe)"), "{line}");
        assert!(line.contains("bucket=lake"), "{line}");
        // Display is boot_line, and Debug redacts the same two fields.
        assert_eq!(format!("{c}"), line);
        let dbg = format!("{c:?}");
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

    // -- the split: node knobs, the environment's sink, a tenant document ----

    fn node() -> NodeKnobs {
        NodeKnobs::from_pairs_with(&[], "node-1@host").unwrap()
    }

    /// The smallest document that starts a sink.
    fn doc() -> Value {
        serde_json::json!({
            "endpoint": "https://s3.example.com",
            "region": "eu-central-1",
            "bucket": "lake",
            "accessKey": "AKIA",
            "queues": "orders",
        })
    }

    fn doc_with(extra: Value) -> Value {
        let mut d = doc();
        for (k, v) in extra.as_object().unwrap() {
            d[k] = v.clone();
        }
        d
    }

    #[test]
    fn node_knobs_are_the_node_wide_variables_with_their_defaults_and_bounds() {
        let n = node();
        assert_eq!(n.memory_mb, 512);
        assert_eq!(n.fetch_concurrency, 4);
        assert_eq!(n.discovery_interval_ms, 2_000);
        assert_eq!(n.safe_guard_ms, 5_000);
        assert_eq!(n.lease_ttl_ms, 30_000);
        assert_eq!(n.multipart_threshold_mb, 64);
        assert_eq!(n.checkpoint_every, 20);
        assert_eq!(n.instance, "node-1@host");
        assert!(!n.instance_generated);
        assert_eq!(n.crash_at, CrashAt::Never);
        assert_eq!(n.memory_bytes(), 512 * 1024 * 1024);
        let err = NodeKnobs::from_pairs_with(&[("QUEEN_S3_LEASE_TTL_MS", "10")], "n").unwrap_err();
        assert!(
            err.starts_with("QUEEN_S3_LEASE_TTL_MS=10 is outside"),
            "{err}"
        );
        // Per-tenant variables are not the node's business.
        assert!(NodeKnobs::from_pairs_with(&[("QUEEN_S3_FORMAT", "nonsense")], "n").is_ok());
    }

    #[test]
    fn the_environment_s_sink_is_off_without_queues_and_refused_half_configured() {
        let n = node();
        assert_eq!(
            Config::from_env_pairs(&n, DEFAULT_TENANT, &[]).unwrap(),
            None
        );
        // Node-wide variables alone are a node that runs control-plane sinks.
        assert_eq!(
            Config::from_env_pairs(&n, DEFAULT_TENANT, &[("QUEEN_S3_MEMORY_MB", "64")]).unwrap(),
            None
        );
        for var in [
            "QUEEN_S3_BUCKET",
            "QUEEN_S3_ENDPOINT",
            "QUEEN_S3_SECRET_KEY",
            "QUEEN_S3_FORMAT",
        ] {
            let err = Config::from_env_pairs(&n, DEFAULT_TENANT, &[(var, "x")]).unwrap_err();
            assert!(
                err.starts_with(&format!("{var} is set but QUEEN_S3_QUEUES is not")),
                "{err}"
            );
            assert!(
                err.contains("QUEEN_S3_QUEUES is what turns the environment's sink on"),
                "{err}"
            );
        }
        let c = Config::from_env_pairs(&n, "t-1", &base()).unwrap().unwrap();
        assert_eq!(c.tenant, "t-1");
        assert_eq!(c.writer.tenant.as_deref(), Some("t-1"));
        assert_eq!(c.tenant_label, None);
        assert_eq!(c.node_knobs(), n, "the node's knobs, copied");
        // With QUEEN_S3_QUEUES set, a variable without a default is required.
        let mut partial = base();
        partial.retain(|(k, _)| *k != "QUEEN_S3_REGION");
        let err = Config::from_env_pairs(&n, "t-1", &partial).unwrap_err();
        assert!(err.starts_with("QUEEN_S3_REGION is not set"), "{err}");
        let err = Config::from_env_pairs(&n, "  ", &base()).unwrap_err();
        assert!(err.contains("the tenant is empty"), "{err}");
    }

    #[test]
    fn a_tenant_document_takes_the_environment_s_defaults() {
        let n = node();
        let from_doc = Config::from_tenant_doc(&n, "t-1", &doc(), "shhh").unwrap();
        let from_env = Config::from_env_pairs(&n, "t-1", &base()).unwrap().unwrap();
        assert_eq!(from_doc, from_env, "one rule, two spellings");
        assert_eq!(from_doc.secret_key, "shhh");
        assert_eq!(from_doc.tenant, "t-1");
    }

    /// Every optional field, set in both spellings, lands on the same value.
    #[test]
    fn every_document_field_is_its_variable_spelled_in_camel_case() {
        let n = node();
        let pairs: &[(&str, &str, Value)] = &[
            ("QUEEN_S3_PREFIX", "lake/eu", serde_json::json!("lake/eu")),
            ("QUEEN_S3_PATH_STYLE", "true", serde_json::json!(true)),
            ("QUEEN_S3_SSE", "aws:kms", serde_json::json!("aws:kms")),
            (
                "QUEEN_S3_SSE_KMS_KEY_ID",
                "arn:k",
                serde_json::json!("arn:k"),
            ),
            ("QUEEN_S3_FORMAT", "parquet", serde_json::json!("parquet")),
            ("QUEEN_S3_COMPRESSION", "gzip", serde_json::json!("gzip")),
            (
                "QUEEN_S3_PARQUET_CODEC",
                "snappy",
                serde_json::json!("snappy"),
            ),
            (
                "QUEEN_S3_LAYOUT",
                "per-partition",
                serde_json::json!("per-partition"),
            ),
            ("QUEEN_S3_ALIGN", "day", serde_json::json!("day")),
            ("QUEEN_S3_START", "earliest", serde_json::json!("earliest")),
            ("QUEEN_S3_TARGET_MB", "64", serde_json::json!(64)),
            (
                "QUEEN_S3_MAX_WINDOW_MS",
                "60000",
                serde_json::json!("60000"),
            ),
            ("QUEEN_S3_SINK", "lake-eu", serde_json::json!("lake-eu")),
        ];
        let mut env = base();
        let mut d = doc();
        for (var, text, json) in pairs {
            env.push((var, text));
            let key = Field::ALL
                .iter()
                .find(|f| f.env() == *var)
                .unwrap()
                .json()
                .unwrap();
            d[key] = json.clone();
        }
        let from_env = Config::from_env_pairs(&n, "t", &env).unwrap().unwrap();
        let from_doc = Config::from_tenant_doc(&n, "t", &d, "shhh").unwrap();
        assert_eq!(from_doc, from_env);
        assert!(from_doc.path_style);
        assert_eq!(from_doc.target_mb, 64);
        assert_eq!(from_doc.max_window_ms, 60_000);
        assert_eq!(from_doc.sink, "lake-eu");
    }

    #[test]
    fn a_document_s_queues_are_a_star_a_comma_list_or_a_list() {
        let n = node();
        let q = |v: Value| {
            Config::from_tenant_doc(&n, "t", &doc_with(serde_json::json!({ "queues": v })), "s")
                .map(|c| c.queues)
        };
        assert_eq!(q(serde_json::json!("*")).unwrap(), Queues::All);
        let named = Queues::Named(vec!["a".into(), "b".into()]);
        assert_eq!(q(serde_json::json!("a, b")).unwrap(), named);
        assert_eq!(q(serde_json::json!(["a", " b "])).unwrap(), named);
        for (bad, says) in [
            (serde_json::json!([]), "names no queue"),
            (serde_json::json!(",,"), "names no queue"),
            (serde_json::json!(["a", "*"]), "lists `*` among queue names"),
            (serde_json::json!(["a", 1]), "not a queue name"),
            (serde_json::json!(7), "is not a list of queues"),
        ] {
            let err = q(bad.clone()).unwrap_err();
            assert!(err.starts_with("queues"), "{bad}: {err}");
            assert!(err.contains(says), "{bad}: {err}");
        }
        let mut d = doc();
        d.as_object_mut().unwrap().remove("queues");
        let err = Config::from_tenant_doc(&n, "t", &d, "s").unwrap_err();
        assert!(err.starts_with("queues is not set"), "{err}");
    }

    /// A refusal names the JSON field, as its variable's refusal names the
    /// variable — and advises in the document's own spelling.
    #[test]
    fn a_document_s_refusals_name_its_fields() {
        let n = node();
        for (field, value, says) in [
            (
                "endpoint",
                serde_json::json!("s3.example.com"),
                "endpoint=s3.example.com has no scheme",
            ),
            (
                "endpoint",
                serde_json::json!("https://h/p"),
                "a prefix inside the bucket is prefix",
            ),
            (
                "bucket",
                serde_json::json!("a/b"),
                "bucket=a/b is not a bucket name",
            ),
            ("bucket", serde_json::json!(5), "bucket=5 is not a string"),
            (
                "prefix",
                serde_json::json!("a//b"),
                "prefix=a//b has an empty or relative segment",
            ),
            (
                "pathStyle",
                serde_json::json!("maybe"),
                "pathStyle=maybe is not a boolean",
            ),
            (
                "pathStyle",
                serde_json::json!(1),
                "pathStyle=1 is not a boolean",
            ),
            ("sse", serde_json::json!("des"), "(+ sseKmsKeyId)"),
            (
                "sseKmsKeyId",
                serde_json::json!("k"),
                "sseKmsKeyId is set but sse=off",
            ),
            (
                "format",
                serde_json::json!("xml"),
                "format=xml is not a format",
            ),
            (
                "compression",
                serde_json::json!("lz4"),
                "a Parquet file's codec is parquetCodec",
            ),
            (
                "parquetCodec",
                serde_json::json!("lz4"),
                "parquetCodec=lz4 is not a codec",
            ),
            (
                "layout",
                serde_json::json!("flat"),
                "layout=flat is not a layout",
            ),
            (
                "align",
                serde_json::json!("week"),
                "align=week is not an alignment",
            ),
            (
                "start",
                serde_json::json!("middle"),
                "start=middle is not a start",
            ),
            (
                "targetMb",
                serde_json::json!(0),
                "targetMb=0 is outside 1..=5120",
            ),
            (
                "targetMb",
                serde_json::json!(1.5),
                "targetMb=1.5 is not a whole number",
            ),
            (
                "targetMb",
                serde_json::json!(true),
                "targetMb=true is not a whole number",
            ),
            (
                "maxWindowMs",
                serde_json::json!(-1),
                "maxWindowMs=-1 is not a whole number",
            ),
            (
                "sink",
                serde_json::json!("a:b"),
                "same queue use different prefix values",
            ),
        ] {
            let d = doc_with(serde_json::json!({ field: value }));
            let err = Config::from_tenant_doc(&n, "t", &d, "s").unwrap_err();
            assert!(err.contains(says), "{field}={value}: {err}");
            assert!(!err.contains("QUEEN_S3_"), "{field}={value}: {err}");
        }
        for missing in ["endpoint", "region", "bucket", "accessKey"] {
            let mut d = doc();
            d.as_object_mut().unwrap().remove(missing);
            let err = Config::from_tenant_doc(&n, "t", &d, "s").unwrap_err();
            assert!(err.starts_with(&format!("{missing} is not set")), "{err}");
        }
        let err = Config::from_tenant_doc(&n, "t", &doc(), " ").unwrap_err();
        assert!(err.starts_with("the secret key is not set"), "{err}");
    }

    /// Nothing in a document is ignored: a typo, the secret, a node-wide knob,
    /// a document that is not an object — each refused with its reason.
    #[test]
    fn a_document_field_nobody_reads_is_refused() {
        let n = node();
        for (extra, says) in [
            (
                serde_json::json!({"bukcet": "x"}),
                "bukcet is not a field of a tenant's sink",
            ),
            (
                serde_json::json!({"partitions": "q:0..3"}),
                "partitions is not a field",
            ),
            (
                serde_json::json!({"secretKey": "x"}),
                "secretKey is not a field of the document",
            ),
            (serde_json::json!({"memoryMb": 64}), "memoryMb is node-wide"),
            (
                serde_json::json!({"leaseTtlMs": 1000}),
                "QUEEN_S3_LEASE_TTL_MS",
            ),
            (
                serde_json::json!({"instance": "n"}),
                "QUEEN_S3_INSTANCE in the environment",
            ),
            (
                serde_json::json!({"crashAt": "mid_upload"}),
                "crashAt is node-wide",
            ),
        ] {
            let d = doc_with(extra.clone());
            let err = Config::from_tenant_doc(&n, "t", &d, "s").unwrap_err();
            assert!(err.contains(says), "{extra}: {err}");
        }
        let err = Config::from_tenant_doc(&n, "t", &serde_json::json!([1]), "s").unwrap_err();
        assert!(err.contains("not a list"), "{err}");
        // The refusal of a typo lists what the fields are.
        let err = validate_tenant_doc(&doc_with(serde_json::json!({"regoin": "x"}))).unwrap_err();
        assert!(
            err.contains("accessKey") && err.contains("maxWindowMs"),
            "{err}"
        );
    }

    /// The control plane's check is the sink's own, minus the secret: the same
    /// verdict, word for word, on every document.
    #[test]
    fn validate_tenant_doc_is_from_tenant_doc_without_the_secret() {
        let n = node();
        for d in [
            doc(),
            doc_with(serde_json::json!({"format": "parquet", "parquetCodec": "snappy"})),
            doc_with(serde_json::json!({"format": "xml"})),
            doc_with(serde_json::json!({"targetMb": 99999})),
            doc_with(serde_json::json!({"memoryMb": 64})),
            doc_with(serde_json::json!({"queues": ["a", "*"]})),
            serde_json::json!({}),
            serde_json::json!("doc"),
        ] {
            let full = Config::from_tenant_doc(&n, "t", &d, "secret").map(|_| ());
            assert_eq!(validate_tenant_doc(&d), full, "{d}");
        }
    }

    #[test]
    fn the_tenant_label_is_off_until_set() {
        let n = node();
        let c = Config::from_tenant_doc(&n, "t-9", &doc(), "s").unwrap();
        assert_eq!(c.tenant_label, None);
        let c = c.with_tenant_label(Some("t-9".into()));
        assert_eq!(c.tenant_label.as_deref(), Some("t-9"));
        assert_eq!(c.with_tenant_label(None).tenant_label, None);
    }

    /// The one-sink shape the broker has used so far keeps its contract: the
    /// default tenant, and QUEEN_S3_QUEUES required.
    #[test]
    fn one_call_one_sink_is_the_default_tenant_s_environment_sink() {
        let c = with(&[]).unwrap();
        assert_eq!(c.tenant, DEFAULT_TENANT);
        assert_eq!(c.writer.tenant.as_deref(), Some(DEFAULT_TENANT));
        let err = Config::from_pairs(&[]).unwrap_err();
        assert!(err.starts_with("QUEEN_S3_QUEUES is not set"), "{err}");
    }

    #[test]
    fn byte_helpers_agree_with_the_megabytes() {
        let c = with(&[("QUEEN_S3_TARGET_MB", "2"), ("QUEEN_S3_MEMORY_MB", "3")]).unwrap();
        assert_eq!(c.target_bytes(), 2 * 1024 * 1024);
        assert_eq!(c.memory_bytes(), 3 * 1024 * 1024);
        assert_eq!(c.multipart_threshold_bytes(), 64 * 1024 * 1024);
    }
}
