use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use deadpool_postgres::Pool;

use crate::db;
use crate::fusion::{json_escape_into, Fusion};
use crate::metrics::Metrics;
use crate::admission::Admission;

/// PLAN_CONFLATION §3.2/§3.3 — the durable per-(queue, group) delivery policy the
/// broker caches off `queen.consumer_groups_metadata`. SQL is the authority; a
/// pop's request flag is only used to REGISTER a brand-new group and to detect a
/// conflict with what is already stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupPolicy {
    pub conflation: bool,
}

pub struct AppState {
    pub pool: Pool,
    pub fusion: Arc<Fusion>,
    // ACK REGISTRY fast path (server/src/ack_registry.rs): leasing pops record
    // (worker, batch_end, delivered-batch hash set) here; a later full-batch
    // completed ack resolves to one positional cursor advance instead of the
    // per-ack log_ack_by_hash_v1 hash resolution. Any miss falls through to the
    // unchanged SQL ack path — the registry is an optimization, never authority.
    pub ack_registry: Arc<crate::ack_registry::AckRegistry>,
    // ACK FUSION (server/src/ack_fusion.rs): coalesces registry-fast-path
    // full-batch acks into ONE queen.log_ack_multi_v1 transaction per flush (one
    // commit / one fsync for N cursor advances), fire-on-idle. Disabled
    // (QUEEN_ACK_FUSION unset) ⇒ enabled() is false and the ack handler never
    // enqueues — the synchronous log_ack_at_v1 fast path is byte-identical.
    pub ack_fusion: Arc<crate::ack_fusion::AckFusion>,
    // POP FUSION (server/src/pop_fusion.rs): claim-leg coalescing; the serve
    // path routes through it only when enabled() and the steady-path
    // preconditions hold (fresh cfg, no reseed due).
    pub pop_fusion: Arc<crate::pop_fusion::PopFusion>,
    pub admission: Arc<Admission>,
    pub metrics: Arc<Metrics>,
    pub stmt_timeout: Duration,
    pub pop_default_timeout_ms: u64,
    /// Effective subscription mode for a grouped pop that sends none (`new` | `all`,
    /// from DEFAULT_SUBSCRIPTION_MODE). Group-less "queue mode" pops ignore this —
    /// the SQL hard-pins them to `all`.
    pub default_subscription_mode: String,
    // Long-poll pending gate on the pinned/discovery pop paths (see
    // config.rs): park on a cheap probe instead of running the full pop SP on
    // every backoff re-poll. QUEEN_POP_PENDING_GATE=false disables.
    pub pop_pending_gate: bool,
    // RUSTFIX item 19: exponential-backoff knobs for the long-poll re-query interval.
    pub pop_wait_initial_interval_ms: u64,
    pub pop_wait_backoff_threshold: u32,
    pub pop_wait_backoff_multiplier: f64,
    pub pop_wait_max_interval_ms: u64,
    // zstd level for broker-packed segments on the transaction push path (the
    // fusion path carries its own copy).
    pub zstd_level: i32,
    // Per-queue configured lease time (seconds), read from queen.queues on
    // first use. No invalidation for now (queue-config invalidation is a later
    // slice); a reconfigure of leaseTime is not reflected until restart.
    pub lease_cache: Mutex<HashMap<String, i32>>,
    // RUSTFIX item 8: at-rest payload encryption. `encryption` holds the key (or is
    // disabled); `enc_cache` memoizes each queue's encryption_enabled flag (same
    // lazy-fetch + UDP-invalidation lifecycle as lease_cache).
    pub encryption: Arc<crate::encryption::Encryption>,
    pub enc_cache: Mutex<HashMap<String, bool>>,
    // System maintenance flags, mirrored to queen.system_state (the SAME
    // {"enabled":..} rows the C++ SharedStateManager uses). When `maintenance` is
    // on, pushes are diverted to the file buffer (RUSTFIX item 17) and reported
    // status:"buffered", exactly like C++ — nothing reaches queen.seg_segments
    // until maintenance is disabled and the buffer drains. `pop_maintenance` pauses
    // pops (handle_pop / handle_pop_partition early-return {messages:[],paused:true}).
    pub maintenance: AtomicBool,
    pub pop_maintenance: AtomicBool,
    // Long-poll waker + inter-instance notifier. A local push wakes locally-parked
    // pops through this, and (when a UDP transport is attached) fans MESSAGE_AVAILABLE
    // / maintenance / queue-config changes out to peer replicas. With no peers it is
    // a pure in-process waker — no packets, behaviour otherwise unchanged.
    pub notifier: Arc<crate::notify::Notifier>,
    // Disk spool for DB-outage durability (RUSTFIX item 1) and maintenance-mode
    // buffering (item 17). Failed pushes and maintenance-diverted pushes are
    // appended here and replayed to the DB by the background drain loop.
    pub file_buffer: Arc<crate::file_buffer::FileBufferManager>,
    // Partition-id -> queue-name memo, used to attribute the partitionId-keyed
    // ack wire to a queue for the per-queue ack metrics. Filled by keyed pops
    // (queue known at zero cost) and lazily by a DB lookup on an ack-first miss.
    // The mapping is immutable (a partition never changes queue), so entries
    // never go stale; the map is only size-capped.
    pub partition_queue: Mutex<HashMap<String, String>>,
    // Phase 2 first-contact safety (targeted pop). Monotonic positive cache of
    // (queue -> {group}) pairs whose group-first-contact BULK SEED (004_log_pop's
    // log_pop_wildcard_*_v1 / log_pop_discover_wire_v1) is known committed — i.e.
    // the single-row consumer_groups_metadata marker exists. Until a (queue,
    // group) is seeded, a woken pop's hint-driven targeted single-partition pop
    // (db::pop_specific -> queen.log_pop_v1) is SUPPRESSED and the pop falls
    // through to the wildcard backstop, which CARRIES the seed. This restores the
    // invariant the anti-convoy fix (51e50c4) relies on: no per-partition lazy
    // first-contact INSERT storm before the set-based seed has run — the merge
    // regression that wedged the broker on Lock:transactionid at t=0. Nested map
    // so the steady-state hit borrows queue+group with no allocation; cleared
    // alongside the other per-queue caches by reconcile so a delete+recreate
    // self-heals within one interval.
    //
    // PLAN_CONFLATION §3.2: the set became a map because the SAME row that
    // proves the seed also carries the group's DELIVERY POLICY. SQL is the
    // authority on conflation (§3.3) and this cache is how the pop path reads it
    // for free: a hit is zero DB, so the steady-state hot path pays nothing for
    // an authoritative policy read. Presence semantics are unchanged —
    // `group_seeded` is now `group_policy(..).is_some()`.
    pub seeded_groups: Mutex<HashMap<String, HashMap<String, GroupPolicy>>>,
    // Wildcard candidate hot-list (19-wildcard-hotlist.md, server/src/hotlist.rs).
    // Disabled (QUEEN_HOTLIST unset) ⇒ every hook is a no-op / one branch and the
    // wildcard pop takes the unchanged SQL candidate-scan path (byte-identical).
    // EPHEMERAL_QUEUES.md §3.2 — the in-RAM queue class. Constructed on every
    // broker and on the embedded facade: it needs no database, no mesh and no
    // flag, so there is no configuration in which it is absent (M9). Its whole
    // interaction with the durable engine is the wake gate it shares
    // (`notifier`), and even that is namespaced (`eph:` in the queue half), so
    // the two can only ever cross-WAKE — which is a hint, never state.
    pub ephemeral: Arc<crate::ephemeral::Ephemeral>,
    // EPHEMERAL_QUEUES.md §3.6 — the pooled broker→broker client, used ONLY to
    // relay an ephemeral push/pop/ack to the partition's rendezvous owner. It is
    // built on every broker, including single ones and the embedded facade, and
    // costs a connection pool that never opens a connection there: the
    // forwarding path is unreachable without a live mesh (§3.7), so a
    // conditional field would be a second way to express the same "no peers".
    pub peers: Arc<crate::peerclient::PeerClient>,
    pub hotlist: Arc<crate::hotlist::HotList>,
    // POP AUTOPILOT (server/src/pop_autopilot.rs): the per-(tenant, queue, group)
    // width controller for grouped wildcard pops that opted in with
    // `?autopilot=true`. In-memory only, zero database traffic anywhere on the pop
    // path, and inert (one relaxed comparison) with QUEEN_POP_AUTOPILOT=off. A
    // request that does not carry the parameter is treated byte-identically in
    // every switch position.
    pub autopilot: Arc<crate::pop_autopilot::PopAutopilot>,
    // §8 reseed/cold-start interval (ms). QUEEN_HOTLIST_RESEED_MS (default 30s).
    pub hotlist_reseed_ms: i64,
    // §8 how often that reseed is a FULL walk instead of the windowed one, and how
    // far back the windowed one looks. QUEEN_HOTLIST_RESEED_FULL_MS (default 5 min,
    // 0 = always full) and QUEEN_HOTLIST_RESEED_WINDOW_MS (resolved at load).
    pub hotlist_reseed_full_ms: i64,
    pub hotlist_reseed_window_ms: i64,
    // Track B (PLAN_QUEEN_PROXY_CLOUD.md §5): native tenant scoping flag
    // (QUEEN_TENANCY_HEADER). Off ⇒ every request is the default tenant and the
    // pid-ownership gate is skipped (vacuously true — no non-default queues exist),
    // so the OSS path is byte-identical. On ⇒ pid-addressed ops verify ownership.
    pub tenancy_enabled: bool,
    // Track B (§5): CONFIRMED-ownership cache for the pid→tenant gate. Keyed
    // "pid\x1ftenant", it holds ONLY positives written by `tenant_owns_partition`
    // after the authoritative DB check returned true — never by pop traffic, so
    // (unlike `partition_queue`) it cannot be poisoned into granting ownership. A
    // partition's tenant is immutable (UUIDs are never reused; a deleted
    // partition's ack simply no-ops in SQL), so a positive is valid forever. This
    // removes the per-ack read-only ownership round trip the bench measured at
    // ~0.784 commits/delivered-msg (~23% of transactions) on the cloud path,
    // after the first ack of each partition. NEGATIVES are never cached: a forged
    // or foreign pid re-checks every time, so an attacker cannot grow the map,
    // and legitimate acks (pids from a real pop) are always positive.
    pub ownership_ok: Mutex<HashSet<String>>,
    // PLAN_KV_TIMERS §9.3 — the occupancy gate. The measurement this broker last
    // read plus ITS OWN delta since, which is what makes the block immediate for
    // the writer that overruns instead of one rollup period late. The write path
    // pays one hashmap lookup and no SQL; anything that would make it pay a query
    // is the anti-pattern §2.4 D7 forbids.
    pub quota: Arc<crate::quota::Quotas>,
    // PLAN_KV_TIMERS §12.1 — the boot flags plus the operator's runtime kill
    // switches, in one place so no caller can check one level and forget the
    // other. Shared with the sweeper (which reads `fire_allowed`) and with the
    // reconcile loop (which mirrors queen.system_state into it).
    pub switches: Arc<crate::switches::Switches>,
    // Rung 5 of the degradation ladder (§12.1): consecutive KV pool refusals.
    // Above `kv_standalone_shed_after`, STANDALONE KV writes are shed while
    // in-wire KV writes continue — the transaction is the value of the product
    // and POST /api/v1/kv is the convenience.
    pub kv_pressure: std::sync::atomic::AtomicU32,
    pub kv_standalone_shed_after: u32,
    // Broker-direct dashboard identity surface (handlers/standalone.rs):
    // whether JWT auth is on decides what /auth/me and /auth/login answer;
    // `server_id` (QUEEN_SERVER_ID → HOSTNAME → random) becomes the synthetic
    // cluster's cell_slug so the SPA's cell-level pages can name this broker.
    pub auth_enabled: bool,
    pub server_id: String,
}

const PARTITION_QUEUE_CACHE_CAP: usize = 100_000;
// Cap on the confirmed-ownership positive cache (see `ownership_ok`).
const OWNERSHIP_CACHE_CAP: usize = 500_000;

// RUSTFIX item 18: fallback lease when a queue has no queen.queues row / DB is
// unreachable — the "60" floor of COALESCE(request, queue.lease_time, 60).
const DEFAULT_LEASE_SECONDS: i32 = 60;

// Track B (PLAN_QUEEN_PROXY_CLOUD.md §5): per-queue scalar caches (lease time,
// encryption flag, group-seed markers) are keyed by queue NAME, which collides
// when two tenants hold the same queue name. Key them by (tenant, name) instead
// so a same-named queue of another tenant can never poison the cache (encryption
// especially — a wrong flag would down/upgrade a tenant's at-rest handling). The
// default tenant when the feature is off ⇒ the key is a stable "<default>\x1f<q>"
// so behaviour is byte-identical to a bare-name key.
#[inline]
pub(crate) fn tenant_queue_key(tenant: &str, queue: &str) -> String {
    let mut k = String::with_capacity(tenant.len() + 1 + queue.len());
    k.push_str(tenant);
    k.push('\u{1f}');
    k.push_str(queue);
    k
}

/// Inverse of `tenant_queue_key`: split a composite key back into (tenant, queue).
/// Used by the metrics collector (syscollect.rs) to attribute per-queue counters
/// to the right tenant at flush time, and by the Prometheus parked gauge. A key
/// without the separator (never produced by `tenant_queue_key`) yields the
/// default tenant + the whole string as the queue, so callers are always safe.
#[inline]
pub(crate) fn split_tenant_queue(key: &str) -> (&str, &str) {
    match key.split_once('\u{1f}') {
        Some((t, q)) => (t, q),
        None => (crate::config::DEFAULT_TENANT, key),
    }
}

impl AppState {
    // Resolve the queue's lease time, caching the lookup. Falls back to
    // DEFAULT_LEASE_SECONDS when the queue has no queen.queues row yet or the DB
    // is unreachable. The std Mutex guard is always dropped before the .await.
    async fn lease_time_for(&self, queue: &str, tenant: &str) -> i32 {
        let key = tenant_queue_key(tenant, queue);
        if let Some(v) = self.lease_cache.lock().unwrap().get(&key).copied() {
            return v;
        }
        let v = match self.pool.get().await {
            Ok(c) => db::queue_lease_time(&c, queue, tenant)
                .await
                .ok()
                .flatten()
                .unwrap_or(DEFAULT_LEASE_SECONDS),
            Err(_) => DEFAULT_LEASE_SECONDS,
        };
        self.lease_cache.lock().unwrap().insert(key, v);
        v
    }

    // RUSTFIX item 8: resolve the queue's encryption_enabled flag, caching the
    // lookup (same shape as lease_time_for). False when the queue has no
    // queen.queues row yet or the DB is unreachable. Guard dropped before .await.
    pub(crate) async fn encryption_enabled_for(&self, queue: &str, tenant: &str) -> bool {
        let key = tenant_queue_key(tenant, queue);
        if let Some(v) = self.enc_cache.lock().unwrap().get(&key).copied() {
            return v;
        }
        let v = match self.pool.get().await {
            Ok(c) => db::queue_encryption_enabled(&c, queue, tenant)
                .await
                .ok()
                .flatten()
                .unwrap_or(false),
            Err(_) => false,
        };
        self.enc_cache.lock().unwrap().insert(key, v);
        v
    }

    // Phase 2 first-contact safety: has this (queue, group)'s group-first-contact
    // bulk seed committed (004_log_pop)? Fast path = the monotonic positive cache (zero
    // DB, no allocation on a hit). On a miss, ONE indexed marker lookup
    // (db::group_policy_lookup); a positive result is cached so the
    // steady-state targeted pop path never reads again. A pool/DB error, or an
    // absent marker, returns false — the caller then uses the wildcard backstop,
    // which is always first-contact-safe. This NEVER returns true before the seed
    // exists, which is the whole point: it gates hint-driven targeted pops until
    // the convoy-preventing seed is in place. Guard is dropped before every await.
    pub(crate) async fn group_seeded(&self, queue: &str, group: &str, tenant: &str) -> bool {
        self.group_policy(queue, group, tenant).await.is_some()
    }

    // PLAN_CONFLATION §3.3: the durable delivery policy of (queue, group), or
    // None when the group has no registration row on this queue yet. Same cache,
    // same single indexed lookup on a miss, same monotonic-positive discipline as
    // the seed probe it replaced — a registered group is immutable policy-wise
    // (group-setting-wins, §1.1), so a positive is valid until reconcile clears
    // the per-queue caches (delete+recreate self-heals within one interval).
    pub(crate) async fn group_policy(
        &self,
        queue: &str,
        group: &str,
        tenant: &str,
    ) -> Option<GroupPolicy> {
        // Track B (§5): the seed marker lives in the (tenant_id, …)-keyed
        // consumer_groups_metadata, so the in-memory positive cache is keyed by
        // (tenant, queue) too — tenant A's seed must not gate tenant B's fast path
        // on a same-named (queue, group).
        let key = tenant_queue_key(tenant, queue);
        {
            let g = self.seeded_groups.lock().unwrap();
            if let Some(gs) = g.get(&key) {
                if let Some(p) = gs.get(group) {
                    return Some(*p);
                }
            }
        }
        let found = match self.pool.get().await {
            Ok(c) => db::group_policy_lookup(&c, queue, group, tenant)
                .await
                .unwrap_or(None),
            Err(_) => None,
        };
        if let Some(conflation) = found {
            let policy = GroupPolicy { conflation };
            self.seeded_groups
                .lock()
                .unwrap()
                .entry(key)
                .or_default()
                .insert(group.to_string(), policy);
            return Some(policy);
        }
        None
    }

    // Record a partition -> queue mapping learned from a pop response.
    pub(crate) fn remember_partition_queue(&self, partition_id: &str, queue: &str) {
        if partition_id.is_empty() || queue.is_empty() {
            return;
        }
        let mut m = self.partition_queue.lock().unwrap();
        if m.len() >= PARTITION_QUEUE_CACHE_CAP {
            m.clear(); // rare, cheap reset; repopulates from live traffic
        }
        m.entry(partition_id.to_string()).or_insert_with(|| queue.to_string());
    }

    // Memo-only partition→queue resolution (no DB). The ack-fusion fast path uses
    // this so it never holds a pooled client across the async flush-wait: on the
    // steady-state hot path the memo is always populated by the pop that created
    // the lease, so this hits; the rare miss (ack-first) falls back to the
    // client-carrying queue_for_partition, whose short-lived client is dropped
    // before the fusion enqueue.
    pub(crate) fn partition_queue_memo(&self, partition_id: &str) -> Option<String> {
        self.partition_queue.lock().unwrap().get(partition_id).cloned()
    }

    // Resolve a partition id to its queue name for ack attribution: memo first,
    // then one DB lookup on miss. None when the partition is unknown (deleted /
    // rows-engine id) — the ack still succeeds, it just goes unattributed.
    pub(crate) async fn queue_for_partition(
        &self,
        client: &deadpool_postgres::Client,
        partition_id: &str,
    ) -> Option<String> {
        if let Some(q) = self.partition_queue.lock().unwrap().get(partition_id).cloned() {
            return Some(q);
        }
        match db::partition_queue_name(client, partition_id).await {
            Ok(Some(q)) => {
                self.remember_partition_queue(partition_id, &q);
                Some(q)
            }
            _ => None,
        }
    }

    // Track B (PLAN_QUEEN_PROXY_CLOUD.md §5): pid→queue→tenant OWNERSHIP gate for
    // the pid-addressed operations (ack, messages/:pid, delete, traces). Returns
    // true when the partition belongs to `tenant`. Tenancy OFF ⇒ always true with
    // NO DB round-trip (an OSS broker has only default-tenant queues, so the OSS
    // ack/messages paths are byte-identical). Tenancy ON ⇒ one indexed EXISTS; a
    // pool/DB error is treated as NOT owned (deny-by-default — a transient DB
    // failure must never open a cross-tenant hole). Never consults the pid→queue
    // memo: ownership must not trust a cache that pop traffic can populate.
    // Cache-only ownership probe (no DB, no pooled connection): lets the hot ack
    // path skip acquiring a connection entirely on a confirmed-ownership hit.
    // A miss returns false and the caller falls back to the authoritative
    // `tenant_owns_partition` (which acquires a connection and populates the
    // cache). Vacuously true when tenancy is off.
    #[inline]
    pub(crate) fn tenant_owns_partition_cached(&self, partition_id: &str, tenant: &str) -> bool {
        if !self.tenancy_enabled {
            return true;
        }
        if partition_id.is_empty() {
            return false;
        }
        self.ownership_ok
            .lock()
            .unwrap()
            .contains(&tenant_queue_key(tenant, partition_id))
    }

    pub(crate) async fn tenant_owns_partition(
        &self,
        client: &deadpool_postgres::Client,
        partition_id: &str,
        tenant: &str,
    ) -> bool {
        if !self.tenancy_enabled {
            return true;
        }
        if partition_id.is_empty() {
            return false;
        }
        // Confirmed-ownership cache (positives only): a hit skips the DB round
        // trip entirely. The key is the same "\x1f" composite used elsewhere.
        let key = tenant_queue_key(tenant, partition_id);
        if self.ownership_ok.lock().unwrap().contains(&key) {
            return true;
        }
        let owned = db::partition_belongs_to_tenant(client, partition_id, tenant)
            .await
            .unwrap_or(false);
        if owned {
            let mut c = self.ownership_ok.lock().unwrap();
            // Bounded (positives are capped by real partitions × tenants, itself
            // plan-capped, but guard against pathological growth). On overflow
            // clear and let it re-warm — correctness is unaffected, ownership is
            // re-derived from the DB.
            if c.len() >= OWNERSHIP_CACHE_CAP {
                c.clear();
            }
            c.insert(key);
        }
        owned
    }

    // RUSTFIX item 19: the next long-poll re-query interval for a given consecutive
    // empty-wait count (C++ _set_next_backoff_time, lib/queen.hpp:2281-2296). Below
    // the threshold it is the initial interval; above it grows
    // initial*count*multiplier, clamped to max. A push-wake resets `count` to 0.
    pub(crate) fn pop_backoff_interval(&self, backoff_count: u32) -> Duration {
        let ms = if backoff_count > self.pop_wait_backoff_threshold {
            let v = (self.pop_wait_initial_interval_ms as f64)
                * (backoff_count as f64)
                * self.pop_wait_backoff_multiplier;
            (v as u64).min(self.pop_wait_max_interval_ms)
        } else {
            self.pop_wait_initial_interval_ms
        };
        Duration::from_millis(ms.max(1))
    }
}

pub(crate) fn json(status: StatusCode, body: String) -> Response {
    // RFC 9110 §15.3.5: 204 responses MUST NOT carry content. Announcing a
    // content-length on a body hyper then elides makes strict HTTP/1.1 clients
    // (Node's undici/llhttp) treat the connection as poisoned and drop it —
    // under empty-poll load that snowballed into ECONNRESET storms. The empty
    // pop/maintenance bodies carried no information the clients read (they all
    // early-return on status 204), so drop the body, keep the status.
    if status == StatusCode::NO_CONTENT {
        return StatusCode::NO_CONTENT.into_response();
    }
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

// Build a VALID JSON error body: {"error":"<prefix><escaped message>"}. The
// message is JSON-escaped (via json_escape_into) while the STRUCTURAL quotes are
// left intact, so clients receive parseable JSON carrying the real error text.
// Replaces the old format-then-blanket-quote-replace pattern, whose replacement
// of every quote mangled the structural quotes into invalid `{'error':'..'}`
// (which is what hid the real DB error on the streams path).
pub(crate) fn json_err(prefix: &str, e: impl std::fmt::Display) -> String {
    let mut out = String::from("{\"error\":\"");
    out.push_str(prefix);
    json_escape_into(&mut out, &e.to_string());
    out.push_str("\"}");
    out
}

// RUSTFIX item 25: map an SP result carrying an embedded {"error":...} to the
// right HTTP status (parity with every C++ submit_sp_call): 404 when the error
// text contains "not found" (case-insensitive), else 500; otherwise 200. The
// original body bytes are preserved so the client-visible error text is unchanged.
pub(crate) fn sp_result_to_response(txt: String) -> Response {
    match serde_json::from_str::<serde_json::Value>(&txt) {
        Ok(v) => {
            if let Some(err) = v.get("error").filter(|e| !e.is_null()) {
                let msg = err.as_str().unwrap_or("").to_ascii_lowercase();
                let code = if msg.contains("not found") {
                    StatusCode::NOT_FOUND
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                };
                return json(code, txt);
            }
            json(StatusCode::OK, txt)
        }
        // Not JSON (SPs always return JSON; a parse failure means a raw body already).
        Err(_) => json(StatusCode::OK, txt),
    }
}


mod data;
// PLAN_KV_TIMERS.md §8.1 — the KV and timer HTTP surfaces. Compiled and
// REGISTERED unconditionally, like every other module here: the boot flags that
// once decided whether their routes existed are gone, so there is no cell where
// /api/v1/kv or /api/v1/timers is absent (see the header of `switches.rs`).
mod kv;
// EPHEMERAL_QUEUES.md §3.3 — the three hot verbs of the RAM-class queues.
// Registered unconditionally like everything else here (M9): there is no cell
// where `/api/v1/ephemeral/*` is absent, so a 404 never means "feature off".
mod ephemeral;
mod timers;
mod queues;
mod messages;
mod traces;
mod status;
mod consumer_groups;
mod maintenance;
mod streams;
mod standalone;
// Dashboard SPA assets (rust-embed): HTTP-broker only. The embedded library
// serves no static files, so the module and its rust-embed dependency are
// gated out of default-features = false builds.
#[cfg(feature = "server")]
mod static_files;
mod analytics;

pub use data::*;
pub use kv::*;
pub use ephemeral::*;
pub use timers::*;
pub use queues::*;
pub use messages::*;
pub use traces::*;
pub use status::*;
pub use consumer_groups::*;
pub use maintenance::*;
pub use streams::*;
pub use standalone::*;
#[cfg(feature = "server")]
pub use static_files::*;
pub use analytics::*;

pub(crate) fn status_is_ok(s: Option<&str>) -> bool {
    // The JS/Go/etc clients send "completed" for success and "failed" for a nack.
    // Absent status defaults to success (a bare completion).
    match s {
        Some(v) => matches!(v, "completed" | "success" | "acked" | "ok"),
        None => true,
    }
}

// RUSTFIX item 10: normalize a client ack status to the four v0.16.0 outcomes the
// segment ack SP branches on. `retry` and `dlq` must survive to SQL (the old code
// collapsed everything to a bool). Absent / unrecognized => completed / failed
// respectively (matching status_is_ok's completed set; anything else is a nack).
pub(crate) fn normalize_ack_status(s: Option<&str>) -> &'static str {
    match s {
        None => "completed",
        Some("completed") | Some("success") | Some("acked") | Some("ok") => "completed",
        Some("retry") => "retry",
        Some("dlq") => "dlq",
        Some(_) => "failed",
    }
}

// Collect the given query keys into a JSON filter object, keeping only
// non-empty string values.
pub(crate) fn filters_from_query(
    params: &HashMap<String, String>,
    keys: &[&str],
) -> serde_json::Map<String, serde_json::Value> {
    let mut m = serde_json::Map::new();
    for &k in keys {
        if let Some(v) = params.get(k) {
            if !v.is_empty() {
                m.insert(k.to_string(), serde_json::Value::String(v.clone()));
            }
        }
    }
    m
}

pub(crate) fn qint(params: &HashMap<String, String>, key: &str, def: i32) -> i32 {
    params.get(key).and_then(|v| v.parse::<i32>().ok()).unwrap_or(def)
}

pub(crate) fn qbool(params: &HashMap<String, String>, key: &str, def: bool) -> bool {
    match params.get(key).map(|s| s.as_str()) {
        Some("false" | "0" | "no") => false,
        Some("true" | "1" | "yes") => true,
        _ => def,
    }
}

