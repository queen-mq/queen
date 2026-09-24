//! The proxy's state in the broker's KV: the one layout every repository and
//! the Postgres import agree on (PLAN_SINGLE_BINARY.md W3/W5).
//!
//! - **Tenant:** everything lives under [`PROXY_TENANT`], a reserved broker
//!   tenant no customer can reach (the gateway only ever injects a cluster's
//!   random `broker_tenant_uuid`).
//! - **Tables:** one namespace per Postgres table ([`ns`]), one JSON document
//!   per row, keyed `#<primary key>`. Every key starts with [`K`] because the
//!   broker's `getPrefix` needs a non-empty prefix: a whole table is
//!   `scan(ns, "#")`.
//! - **Indexes:** a Postgres UNIQUE or lookup index is a second namespace
//!   (`<table>.<column>`) whose key is `#<value>` and whose value is the row's
//!   id — written in the SAME atomic batch as the row, `putIfAbsent` +
//!   `required: true` for a unique one, so two nodes can never both win. A
//!   "rows of X" index is `#<x id>/<row id>` with an empty value, listed by
//!   prefix.
//! - **Time:** every timestamp is epoch microseconds (UTC), `*_us`.
//! - **Cascades:** Postgres' `ON DELETE CASCADE` becomes an explicit delete of
//!   the child rows and their index keys, in the parent's batch where it fits
//!   (a batch is atomic; very large cascades go in several).

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// The broker tenant the proxy's state lives under.
pub const PROXY_TENANT: &str = "00000000-0000-0000-0000-00000000fffe";

/// Every key of the layout starts with this (see the module header).
pub const K: &str = "#";

/// `#<id>`.
pub fn key(id: impl std::fmt::Display) -> String {
    format!("{K}{id}")
}

/// `#<a>/<b>` — a "rows of a" index entry, or a composite key.
pub fn key2(a: impl std::fmt::Display, b: impl std::fmt::Display) -> String {
    format!("{K}{a}/{b}")
}

/// `#<a>/` — the prefix listing every `key2(a, _)`.
pub fn prefix(a: impl std::fmt::Display) -> String {
    format!("{K}{a}/")
}

/// The id at the end of a `key2` index entry.
pub fn tail(k: &str) -> &str {
    k.rsplit('/').next().unwrap_or(k)
}

/// Namespaces: tables, then their indexes (`<table>.<column>`).
pub mod ns {
    pub const TENANTS: &str = "px.tenants"; // #<id> -> TenantDoc
    pub const TENANT_SLUG: &str = "px.tenants.slug"; // #<slug> -> id (UNIQUE)

    pub const USERS: &str = "px.users"; // #<id> -> UserDoc
    pub const USER_EMAIL: &str = "px.users.email"; // #<email> -> id (UNIQUE, exact)
    pub const USER_TENANT: &str = "px.users.tenant"; // #<tenant>/<user> -> ""

    pub const IDENTITIES: &str = "px.identities"; // #<id> -> IdentityDoc
    pub const IDENTITY_PROVIDER: &str = "px.identities.provider"; // #<provider>:<provider_id> -> id (UNIQUE)
    pub const IDENTITY_USER: &str = "px.identities.user"; // #<user>/<identity> -> ""

    pub const PLANS: &str = "px.plans"; // #<id> -> PlanDoc
    pub const PLAN_CODE: &str = "px.plans.code"; // #<code> -> id (UNIQUE)

    pub const CELLS: &str = "px.cells"; // #<id> -> CellDoc
    pub const CELL_SLUG: &str = "px.cells.slug"; // #<slug> -> id (UNIQUE)

    pub const CLUSTERS: &str = "px.clusters"; // #<id> -> ClusterDoc
    pub const CLUSTER_SLUG: &str = "px.clusters.slug"; // #<slug> -> id (UNIQUE)
    pub const CLUSTER_TENANT: &str = "px.clusters.tenant"; // #<tenant>/<cluster> -> ""
    pub const CLUSTER_CELL: &str = "px.clusters.cell"; // #<cell>/<cluster> -> ""

    pub const ROLES: &str = "px.roles"; // #<user>/<cluster> -> RoleDoc (PK)
    pub const ROLE_CLUSTER: &str = "px.roles.cluster"; // #<cluster>/<user> -> ""

    pub const KEYS: &str = "px.keys"; // #<id> -> ApiKeyDoc
    pub const KEY_HASH: &str = "px.keys.hash"; // #<key_hash> -> id (UNIQUE)
    pub const KEY_CLUSTER: &str = "px.keys.cluster"; // #<cluster>/<key> -> ""

    pub const QUEUES: &str = "px.queues"; // #<id> -> QueueDoc
    pub const QUEUE_NAME: &str = "px.queues.name"; // #<cluster>/<name> -> id (UNIQUE among live rows)

    pub const USAGE_MIN: &str = "px.usage.min"; // #<cluster>/<minute_us>/<op_class>/<node> -> UsageDoc (TTL)
    pub const USAGE_DAY: &str = "px.usage.day"; // #<cluster>/<day YYYY-MM-DD>/<op_class> -> UsageDoc

    pub const OPS: &str = "px.ops"; // #<tenant>/<inv_at_us>/<id> -> OperationDoc (newest first)
    pub const OPS_CLUSTER: &str = "px.ops.cluster"; // #<cluster>/<inv_at_us>/<id> -> "" (tenant + id in OPS)

    pub const REVOKED: &str = "px.revoked"; // #<jti> -> RevokedDoc (TTL until expiry)

    pub const OUTBOX: &str = "px.outbox"; // #<created_us 20 digits>/<id> -> OutboxDoc

    pub const META: &str = "px.meta"; // #schema -> MetaDoc
}

/// `i64::MAX - at_us`, zero-padded: a key that sorts NEWEST first.
pub fn inverted(at_us: i64) -> String {
    format!("{:019}", i64::MAX - at_us.max(0))
}

/// `at_us` zero-padded: a key that sorts oldest first.
pub fn ordered(at_us: i64) -> String {
    format!("{:020}", at_us.max(0))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TenantDoc {
    pub id: Uuid,
    pub slug: String,
    pub name: String,
    /// active | grace | suspended | deleting
    pub status: String,
    pub created_at_us: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UserDoc {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub email: String,
    pub password_hash: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub is_operator: bool,
    #[serde(default)]
    pub last_login_at_us: Option<i64>,
    pub created_at_us: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IdentityDoc {
    pub id: Uuid,
    pub user_id: Uuid,
    /// local | google | github
    pub provider: String,
    pub provider_id: String,
    pub email: String,
    pub verified: bool,
    pub created_at_us: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PlanDoc {
    pub id: Uuid,
    pub code: String,
    /// shared | dedicated
    pub cell_class: String,
    pub max_req_per_sec: Option<i64>,
    pub req_burst: Option<i64>,
    pub max_msgs_per_sec: Option<i64>,
    pub msgs_burst: Option<i64>,
    pub max_queues: Option<i64>,
    pub max_partitions_per_queue: Option<i64>,
    pub max_parked_pops: Option<i64>,
    pub max_payload_bytes: Option<i64>,
    pub max_batch_items: Option<i64>,
    pub max_retained_bytes: Option<i64>,
    pub max_retention_seconds: Option<i64>,
    pub monthly_msgs_quota: Option<i64>,
    /// Open JSON (Postgres default `{"kv":true,"timers":true,"ephemeral":true}`).
    pub features: Value,
    pub created_at_us: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CellDoc {
    pub id: Uuid,
    pub slug: String,
    pub region: String,
    pub base_url: String,
    /// shared | dedicated
    pub class: String,
    pub capacity_slots: i64,
    pub used_slots: i64,
    pub broker_version: Option<String>,
    /// active | draining | dead
    pub status: String,
    pub cell_secret: Option<String>,
    pub created_at_us: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterDoc {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub cell_id: Uuid,
    pub plan_id: Uuid,
    pub slug: String,
    pub broker_tenant_uuid: Uuid,
    /// active | push_blocked | suspended | deleting
    pub status: String,
    /// Open JSON of limit overrides (`{}` when none).
    pub limit_overrides: Value,
    pub created_at_us: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RoleDoc {
    pub user_id: Uuid,
    pub cluster_id: Uuid,
    /// admin | producer | consumer | viewer
    pub role: String,
    pub created_at_us: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ApiKeyDoc {
    pub id: Uuid,
    pub cluster_id: Uuid,
    pub name: String,
    /// sha256 hex of the secret (64 chars).
    pub key_hash: String,
    /// subset of produce | consume | admin | read
    pub scopes: Vec<String>,
    pub created_by: Option<Uuid>,
    pub created_at_us: i64,
    pub last_used_at_us: Option<i64>,
    pub revoked_at_us: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct QueueDoc {
    pub id: Uuid,
    pub cluster_id: Uuid,
    pub name: String,
    pub partitions_count: i64,
    pub created_at_us: i64,
    pub deleted_at_us: Option<i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct UsageDoc {
    pub msgs: i64,
    pub reqs: i64,
    pub bytes_in: i64,
    pub bytes_out: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct OperationDoc {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub cluster_id: Option<Uuid>,
    /// user | api_key | control_plane | system
    pub actor: String,
    pub actor_id: Option<Uuid>,
    pub action: String,
    pub target: Option<String>,
    pub meta: Value,
    pub at_us: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RevokedDoc {
    pub jti: String,
    pub expires_at_us: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct OutboxDoc {
    pub id: Uuid,
    pub kind: String,
    pub payload: Value,
    pub created_at_us: i64,
    pub consumed_at_us: Option<i64>,
}

/// `px.meta #schema`: the layout version, and when/what the Postgres import
/// brought in (W5).
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct MetaDoc {
    pub version: u32,
    pub imported_at_us: Option<i64>,
    #[serde(default)]
    pub imported_rows: Value,
}

/// The layout version [`MetaDoc::version`] carries.
pub const SCHEMA_VERSION: u32 = 1;
