//! The connector document (PLAN_PG_CONNECTORS.md §3) and the node knobs.
//!
//! The document is what `PUT /api/v1/connectors/:name` stores: JSON, camelCase,
//! unknown fields refused. [`ConnectorDoc::validate`] is the one place its
//! rules live; the broker's API calls it before storing and the manager calls
//! it again before running (a document written by an older broker must not
//! run with rules it never met).

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// `^[a-z0-9][a-z0-9_-]{0,47}$`.
pub fn valid_connector_name(name: &str) -> bool {
    let b = name.as_bytes();
    !b.is_empty()
        && b.len() <= 48
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_' || *c == b'-')
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Source,
    Sink,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Source => "source",
            Kind::Sink => "sink",
        }
    }
}

/// The whole document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorDoc {
    pub kind: Kind,
    #[serde(default = "yes")]
    pub enabled: bool,
    pub connection: ConnectionSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink: Option<SinkSpec>,
    /// Broker-written: when the document was last stored (ISO-8601 UTC). The
    /// manager's fingerprint: a new value restarts the connector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    /// Broker-written by `POST …/resync`: the source drops its pointer and
    /// slot and snapshots again when this is newer than the pointer's
    /// `resyncedAt`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resync_requested_at: Option<String>,
    /// Broker-written by `DELETE …?dropSlot=true` on a source: the owner drops
    /// the slot (and a managed publication), then removes the document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleting: Option<DeleteRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeleteRequest {
    pub drop_slot: bool,
    pub requested_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum SslMode {
    Disable,
    #[default]
    Prefer,
    Require,
    VerifyFull,
}

/// Where the database is and how to log in.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectionSpec {
    /// `postgres://user:pass@host:port/db?sslmode=…`: accepted on PUT only and
    /// normalized into the fields below by [`ConnectorDoc::normalize`] (which
    /// moves its password into `password`). Never stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default)]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub user: String,
    /// Write-only: the broker seals it into `password_sealed` and never
    /// returns it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// The sealed password (the broker's encryption envelope, JSON text).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_sealed: Option<String>,
    #[serde(default)]
    pub ssl_mode: SslMode,
    /// PEM, `verify-full` only (default: the webpki roots).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssl_root_cert: Option<String>,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
}

fn default_port() -> u16 {
    5432
}
fn default_connect_timeout_ms() -> u64 {
    10_000
}
fn yes() -> bool {
    true
}

/// How a table's rows map to partitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(untagged)]
pub enum PartitionBy {
    /// `"key"` (the replica identity key, the default) or `"single"` (one
    /// partition named `all`: total commit order in the queue).
    Mode(PartitionMode),
    /// Explicit columns.
    Columns(Vec<String>),
    #[default]
    #[serde(skip)]
    Default,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PartitionMode {
    Key,
    Single,
}

impl PartitionBy {
    /// `Default` reads as `Key`.
    pub fn is_single(&self) -> bool {
        matches!(self, PartitionBy::Mode(PartitionMode::Single))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TableSpec {
    /// `schema.name` or `name` (= `public.name`).
    pub table: String,
    pub queue: String,
    #[serde(default, skip_serializing_if = "is_default_partition")]
    pub partition_by: PartitionBy,
}

fn is_default_partition(p: &PartitionBy) -> bool {
    matches!(p, PartitionBy::Default)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotMode {
    #[default]
    Initial,
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TruncatePolicy {
    #[default]
    Skip,
    Emit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceSpec {
    pub tables: Vec<TableSpec>,
    /// Default `queen_<name>` with `-` → `_`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<String>,
    /// Default `queen_<name>` with `-` → `_`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication: Option<String>,
    #[serde(default = "yes")]
    pub manage_publication: bool,
    #[serde(default)]
    pub snapshot: SnapshotMode,
    #[serde(default = "d_chunk")]
    pub snapshot_chunk_rows: u32,
    #[serde(default = "d_bundle_msgs")]
    pub max_bundle_messages: u32,
    #[serde(default = "d_bundle_bytes")]
    pub max_bundle_bytes: u64,
    #[serde(default = "d_linger")]
    pub linger_ms: u64,
    #[serde(default = "d_heartbeat")]
    pub heartbeat_seconds: u64,
    #[serde(default)]
    pub on_truncate: TruncatePolicy,
}

fn d_chunk() -> u32 {
    2000
}
fn d_bundle_msgs() -> u32 {
    1000
}
fn d_bundle_bytes() -> u64 {
    4 * 1024 * 1024
}
fn d_linger() -> u64 {
    20
}
fn d_heartbeat() -> u64 {
    10
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SinkMode {
    Append,
    Upsert,
    Cdc,
    Sql,
}

/// Target columns for the message's metadata (append / upsert).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MetadataColumns {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SinkSpec {
    pub queue: String,
    /// Default `pg-<name>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<String>,
    #[serde(default = "d_sub_mode")]
    pub subscription_mode: String,
    /// `schema.name` or `name`.
    pub table: String,
    pub mode: SinkMode,
    /// upsert/cdc: default the table's primary key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<Vec<String>>,
    /// sql mode only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statement: Option<String>,
    /// sql mode only: `$`, `$.a.b[2]`, `@partition`, `@partitionId`,
    /// `@offset`, `@transactionId`, `@createdAt`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataColumns>,
    #[serde(default = "d_batch")]
    pub batch: u32,
    #[serde(default = "d_workers")]
    pub workers: u32,
    #[serde(default = "d_lease")]
    pub lease_seconds: u32,
    #[serde(default = "d_attempts")]
    pub max_attempts: u32,
    #[serde(default = "d_progress")]
    pub progress_table: String,
    #[serde(default = "yes")]
    pub create_progress_table: bool,
}

fn d_sub_mode() -> String {
    "all".into()
}
fn d_batch() -> u32 {
    500
}
fn d_workers() -> u32 {
    1
}
fn d_lease() -> u32 {
    60
}
fn d_attempts() -> u32 {
    3
}
fn d_progress() -> String {
    "queen.sink_progress".into()
}

impl ConnectorDoc {
    /// Parse a stored or submitted document.
    ///
    /// `connection.passwordSet` is what a READ says about the password
    /// ([`ConnectorDoc::redacted`]); a document read and written back as it
    /// came carries it, so it is accepted here and dropped. (A marker in
    /// `password` itself would be stored as the password.)
    pub fn from_json(v: &serde_json::Value) -> Result<ConnectorDoc> {
        let mut v = v.clone();
        if let Some(c) = v
            .get_mut("connection")
            .and_then(serde_json::Value::as_object_mut)
        {
            c.remove("passwordSet");
        }
        serde_json::from_value(v).map_err(|e| Error::config(e.to_string()))
    }

    /// Fold `connection.url` into the fields (its password into `password`),
    /// fill nothing else. Called by the API before [`ConnectorDoc::validate`].
    pub fn normalize(&mut self) -> Result<()> {
        crate::config_validate::normalize(self)
    }

    /// Every rule of PLAN §3.2 (limits refused, never clamped). `name` is the
    /// connector name the document is stored under.
    pub fn validate(&self, name: &str) -> Result<()> {
        crate::config_validate::validate(self, name)
    }

    /// The document as the API returns it: no `password`, no
    /// `passwordSealed`; `"passwordSet": true|false` instead.
    pub fn redacted(&self) -> serde_json::Value {
        crate::config_validate::redacted(self)
    }

    /// The source's slot name (default `queen_<name>`, `-` → `_`).
    pub fn slot_name(&self, name: &str) -> String {
        self.source
            .as_ref()
            .and_then(|s| s.slot.clone())
            .unwrap_or_else(|| format!("queen_{}", name.replace('-', "_")))
    }

    /// The source's publication name (default `queen_<name>`, `-` → `_`).
    pub fn publication_name(&self, name: &str) -> String {
        self.source
            .as_ref()
            .and_then(|s| s.publication.clone())
            .unwrap_or_else(|| format!("queen_{}", name.replace('-', "_")))
    }

    /// The sink's consumer group (default `pg-<name>`).
    pub fn consumer_group(&self, name: &str) -> String {
        self.sink
            .as_ref()
            .and_then(|s| s.consumer_group.clone())
            .unwrap_or_else(|| format!("pg-{name}"))
    }
}

/// The node-wide knobs (environment only, PLAN §3.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeKnobs {
    /// This node's id (the broker's server id): written into leases.
    pub node: String,
    pub threads: usize,
    pub reload_ms: u64,
    pub lease_ttl_ms: u64,
    pub shutdown_grace_ms: u64,
    pub allow_private_networks: bool,
}

impl NodeKnobs {
    /// Defaults for tests and for a node with no environment.
    pub fn defaults(node: impl Into<String>) -> NodeKnobs {
        NodeKnobs {
            node: node.into(),
            threads: 2,
            reload_ms: 5_000,
            lease_ttl_ms: 10_000,
            shutdown_grace_ms: 10_000,
            allow_private_networks: true,
        }
    }

    /// Read `QUEEN_PG_*` (strict: a value out of range is an error naming the
    /// variable). `proxy_embedded` decides the egress default.
    pub fn from_env(node: &str, proxy_embedded: bool) -> std::result::Result<NodeKnobs, String> {
        crate::config_validate::knobs_from_env(node, proxy_embedded)
    }
}
