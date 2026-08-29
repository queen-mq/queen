use std::time::Duration;

/// Track B (native tenant scoping, PLAN_QUEEN_PROXY_CLOUD.md §5). The broker gains
/// ONE opaque concept: a `tenant_id` scoping key on queue identity, taken from a
/// trusted header the colocated proxy sets. These constants are the fixed contract.
///
/// The default tenant — used for EVERY request when the feature is off, and when
/// the header is absent while it is on — so OSS/self-host behaviour is byte-identical
/// (the DDL column defaults to this same value, so no backfill is ever needed).
pub const DEFAULT_TENANT: &str = "00000000-0000-0000-0000-000000000001";
/// The trusted header the proxy injects. The broker never validates the tenant
/// against anything (it is opaque; the trust is network — the cell boundary).
pub const TENANT_HEADER: &str = "x-queen-tenant";

/// The `JWT_ALGORITHM` values the broker accepts, in the order the boot error
/// lists them. This is the ONE spelling of the set: `AuthConfig::validate` is
/// matched against it and its "not supported" message is BUILT from it, so the
/// message can no longer name a set the validation does not enforce.
///
/// `auth.rs::check_alg_allowed` is the other half of the contract — the value
/// accepted at boot must be a value the verifier accepts at request time. The
/// two used to disagree about HS384/HS512 (refused here, implemented there,
/// which left pinning HS512 impossible and pushed operators to the strictly
/// wider `auto`); `auth::tests::boot_and_the_verifier_accept_the_same_algorithms`
/// now fails if they ever drift apart again.
pub const SUPPORTED_JWT_ALGORITHMS: &[&str] = &[
    "HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "EdDSA", "auto",
];

/// JWT auth configuration, mirroring the C++ `AuthConfig` (server/include/queen/config.hpp).
/// When `enabled` is false (the default) the auth middleware passes every request
/// through untouched — this is how the whole existing test-suite runs.
#[derive(Clone)]
pub struct AuthConfig {
    pub enabled: bool,
    pub algorithm: String,
    pub secret: String,
    /// PEM public key for RS256/EdDSA (static-key deployments). Consulted before
    /// the JWKS cache (RUSTFIX item 7).
    pub public_key: String,
    /// JWKS endpoint for RS256/EdDSA key discovery + rotation (RUSTFIX item 7).
    pub jwks_url: String,
    pub jwks_refresh_interval_seconds: i64,
    pub jwks_request_timeout_ms: i64,
    pub issuer: String,
    pub audience: String,
    pub clock_skew_seconds: i64,
    pub skip_paths: Vec<String>,
    pub roles_claim: String,
    pub roles_array_claim: String,
    pub role_admin: String,
    pub role_read_write: String,
    pub role_read_only: String,
    pub role_write_only: String,
}

impl AuthConfig {
    fn from_env() -> AuthConfig {
        let skip_raw = env_str(
            "JWT_SKIP_PATHS",
            "/health,/metrics/prometheus,/metrics,/",
        );
        let skip_paths: Vec<String> = skip_raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        AuthConfig {
            enabled: env_bool("JWT_ENABLED", false),
            algorithm: env_str("JWT_ALGORITHM", "HS256"),
            secret: env_str("JWT_SECRET", ""),
            public_key: env_str("JWT_PUBLIC_KEY", ""),
            jwks_url: env_str("JWT_JWKS_URL", ""),
            jwks_refresh_interval_seconds: env_int("JWT_JWKS_REFRESH_INTERVAL", 3600),
            jwks_request_timeout_ms: env_int("JWT_JWKS_TIMEOUT_MS", 5000),
            issuer: env_str("JWT_ISSUER", ""),
            audience: env_str("JWT_AUDIENCE", ""),
            clock_skew_seconds: env_int("JWT_CLOCK_SKEW", 30),
            skip_paths,
            roles_claim: env_str("JWT_ROLES_CLAIM", "role"),
            roles_array_claim: env_str("JWT_ROLES_ARRAY_CLAIM", "roles"),
            role_admin: env_str("JWT_ROLE_ADMIN", "admin"),
            role_read_write: env_str("JWT_ROLE_READ_WRITE", "read-write"),
            role_read_only: env_str("JWT_ROLE_READ_ONLY", "read-only"),
            role_write_only: env_str("JWT_ROLE_WRITE_ONLY", "write-only"),
        }
    }

    /// Exact-match, plus trailing-slash prefix match for entries longer than "/"
    /// (identical to the C++ `should_skip_path`).
    pub fn should_skip(&self, path: &str) -> bool {
        for skip in &self.skip_paths {
            if path == skip {
                return true;
            }
            if skip.len() > 1 && skip.ends_with('/') && path.starts_with(skip.as_str()) {
                return true;
            }
        }
        false
    }

    /// Startup config validation (RUSTFIX item 7), mirroring the C++
    /// `AuthConfig::validate` (config.hpp:539-561): when auth is enabled, the
    /// configured algorithm must have usable key material. Returns Err with a
    /// message naming the missing credentials so boot can fail loudly.
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        match self.algorithm.as_str() {
            // The whole HMAC family, not just HS256: `auth.rs` verifies HS384 and
            // HS512 off the same `JWT_SECRET` bytes, so refusing them here only
            // denied operators the ability to PIN one — `auto`, the workaround,
            // accepts all seven algorithms and is the weaker posture.
            "HS256" | "HS384" | "HS512" | "auto" => {
                if self.secret.is_empty() && self.jwks_url.is_empty() && self.public_key.is_empty() {
                    return Err(format!(
                        "JWT_ENABLED=true with JWT_ALGORITHM={} but no key material: set JWT_SECRET (HS256/HS384/HS512), or JWT_PUBLIC_KEY / JWT_JWKS_URL (RS256/EdDSA)",
                        self.algorithm
                    ));
                }
            }
            "RS256" | "RS384" | "RS512" | "EdDSA" => {
                if self.jwks_url.is_empty() && self.public_key.is_empty() {
                    return Err(format!(
                        "JWT_ENABLED=true with JWT_ALGORITHM={} but neither JWT_PUBLIC_KEY nor JWT_JWKS_URL is set",
                        self.algorithm
                    ));
                }
            }
            other => {
                let mut list = SUPPORTED_JWT_ALGORITHMS.join(", ");
                if let Some(i) = list.rfind(", ") {
                    list.replace_range(i..i + 2, ", or ");
                }
                return Err(format!("JWT_ALGORITHM={other} is not supported (use {list})"));
            }
        }
        Ok(())
    }
}

/// Inter-instance sync configuration, mirroring the C++ `InterInstanceConfig`
/// + `SharedStateConfig` (server/include/queen/config.hpp). Drives `mesh.rs` /
/// `notify.rs`. When `enabled` is true but `peers` is empty (a single stock
/// broker) the mesh transport is never bound — the broker behaves exactly as
/// before, except the in-process long-poll waker (which needs no cluster) is live.
#[derive(Clone)]
pub struct SyncConfig {
    pub enabled: bool,
    /// TCP mesh listen/dial port. `QUEEN_MESH_PORT`, falling back to the legacy
    /// `QUEEN_UDP_NOTIFY_PORT` (default 6633).
    pub mesh_port: u16,
    /// Host the mesh listener binds. `QUEEN_MESH_BIND_ADDR`, defaulting to
    /// whatever `QUEEN_BIND_ADDR` gives the HTTP listener — so one variable
    /// moves both listeners, and this second knob exists only for the split
    /// case (API on loopback, mesh on the pod IP).
    ///
    /// Not to be confused with `http_addr` below: bind is who can REACH this
    /// process, advertise is where peers are TOLD to find it. Narrowing the
    /// bind while advertising a routable host is the one combination that
    /// looks configured and connects to nothing, which is why both are on the
    /// boot line.
    pub bind_addr: String,
    /// Parsed peer list — (host, port) pairs from `QUEEN_MESH_PEERS` (falling back
    /// to the legacy `QUEEN_UDP_PEERS`). Empty ⇒ no peers ⇒ no mesh.
    pub peers: Vec<(String, u16)>,
    /// HMAC-SHA256 secret for the connection handshake. Empty ⇒ open mode (no auth).
    pub secret: String,
    pub heartbeat_ms: u64,
    pub dead_threshold_ms: u64,
    /// `QUEEN_CACHE_REFRESH_INTERVAL_MS` (default 60000). RUSTFIX item 16: drives
    /// the periodic reconcile loop (reconcile.rs) that re-reads the maintenance
    /// flags from queen.system_state and drops the lease cache, so a replica heals
    /// within one interval after a lost UDP packet.
    pub cache_refresh_ms: u64,
    /// This node's identity in heartbeats/stats (env `QUEEN_SERVER_ID`, else a
    /// short random id). Cosmetic for the mesh itself — but load-bearing for
    /// EPHEMERAL_QUEUES.md §3.7, where it is the rendezvous hash's node key: two
    /// brokers sharing one `QUEEN_SERVER_ID` would compute the same score and
    /// disagree about which of them owns a partition.
    pub server_id: String,
    /// This broker's dialable base URL FOR PEERS — the `http_addr` of HELLO v2
    /// (EPHEMERAL_QUEUES.md §3.5) and the address a non-owner forwards an
    /// ephemeral push/pop/ack to (§3.6). Scheme is always `http` (mesh traffic is
    /// intra-cell) and the port is this broker's own `PORT`, so the only knob is
    /// the HOST: `QUEEN_MESH_ADVERTISE_HOST`, defaulting to the OS hostname.
    ///
    /// The host is a knob because it is the one part a process cannot work out
    /// for itself: the OS hostname is exactly right in Kubernetes (the pod's, and
    /// resolvable through the headless service) and exactly wrong behind NAT or
    /// on a laptop running two brokers under one hostname. It is advertised, not
    /// probed: a peer never guesses our address from the socket it accepted,
    /// because that is the DIAL side's ephemeral source address, not a port
    /// anything listens on.
    pub http_addr: String,
}

impl SyncConfig {
    fn from_env() -> SyncConfig {
        // Prefer the new QUEEN_MESH_* names; fall back to the legacy QUEEN_UDP_*
        // ones so existing deployments keep working unchanged.
        let mesh_port = env_int("QUEEN_MESH_PORT", env_int("QUEEN_UDP_NOTIFY_PORT", 6633)) as u16;
        // Unset ⇒ wherever the HTTP listener binds. Written as a nested default
        // rather than the empty-sentinel fallback the peer list uses below,
        // because this is a DEFAULT and that is an ALIAS, and the two read the
        // same in the docs unless the source distinguishes them. An explicitly
        // empty value is rejected, not inherited: nobody means "" by it.
        //
        // A bad QUEEN_BIND_ADDR is reported under its own name, not this one,
        // because `load` builds `bind_addr` before `sync` and struct fields are
        // evaluated in source order.
        let bind_addr = checked_bind_addr(
            "QUEEN_MESH_BIND_ADDR",
            "QUEEN_MESH_PORT",
            env_str("QUEEN_MESH_BIND_ADDR", &env_str("QUEEN_BIND_ADDR", "0.0.0.0")),
        );
        let peers_raw = {
            let mesh = env_str("QUEEN_MESH_PEERS", "");
            if !mesh.is_empty() {
                mesh
            } else {
                env_str("QUEEN_UDP_PEERS", "")
            }
        };
        let peers = parse_mesh_peers(&peers_raw, mesh_port);
        let server_id = std::env::var("QUEEN_SERVER_ID")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| std::env::var("HOSTNAME").ok().filter(|v| !v.is_empty()))
            .unwrap_or_else(|| format!("queen-{:08x}", rand::random::<u32>()));
        // EPHEMERAL_QUEUES.md §3.5. Read from the SAME `PORT` variable the HTTP
        // listener binds (below), and not from a second knob: an advertised port
        // that could differ from the one this process listens on is a
        // misconfiguration nobody would ever detect from the outside — peers
        // would simply forward into a closed socket.
        let advertise_host = {
            let h = env_str("QUEEN_MESH_ADVERTISE_HOST", "");
            if h.is_empty() {
                os_hostname()
            } else {
                h
            }
        };
        let http_addr = format!("http://{}:{}", advertise_host, env_str("PORT", "6632"));
        SyncConfig {
            enabled: env_bool("QUEEN_SYNC_ENABLED", true),
            mesh_port,
            bind_addr,
            peers,
            secret: env_str("QUEEN_SYNC_SECRET", ""),
            heartbeat_ms: env_int("QUEEN_SYNC_HEARTBEAT_MS", 1000).max(1) as u64,
            dead_threshold_ms: env_int("QUEEN_SYNC_DEAD_THRESHOLD_MS", 5000).max(1) as u64,
            cache_refresh_ms: env_int("QUEEN_CACHE_REFRESH_INTERVAL_MS", 60000).max(1) as u64,
            server_id,
            http_addr,
        }
    }

    /// The mesh transport only starts when sync is enabled AND at least one peer is
    /// configured (matches the C++ `enabled_ = shared_state.enabled && has_udp_peers()`).
    pub fn mesh_active(&self) -> bool {
        self.enabled && !self.peers.is_empty()
    }
}

/// The OS hostname, for the default advertised address (§3.5).
///
/// `gethostname(2)` and not `$HOSTNAME`: the environment variable is set by
/// interactive shells and by Docker, but NOT by every init that starts a broker,
/// and an unset one would silently advertise the fallback to the whole cell. The
/// env var is kept as the second source (a container that overrode it meant it),
/// and loopback is the last resort — an address that resolves to this process is
/// strictly better than one that resolves to nothing, because with a single
/// broker the forwarding path is never taken anyway.
fn os_hostname() -> String {
    let mut buf = [0 as libc::c_char; 256];
    // SAFETY: the pointer and length describe a live, correctly sized stack
    // buffer; gethostname NUL-terminates within it or fails.
    if unsafe { libc::gethostname(buf.as_mut_ptr(), buf.len()) } == 0 {
        let bytes: Vec<u8> = buf.iter().take_while(|&&c| c != 0).map(|&c| c as u8).collect();
        if let Ok(s) = String::from_utf8(bytes) {
            if !s.is_empty() {
                return s;
            }
        }
    }
    std::env::var("HOSTNAME")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

/// Parse the peer list: comma-separated `host` or `host:port` entries (a bare host
/// uses `default_port`). Tolerates an `http://` prefix and surrounding whitespace,
/// matching the C++ `parse_udp_peers`.
fn parse_mesh_peers(raw: &str, default_port: u16) -> Vec<(String, u16)> {
    raw.split(',')
        .filter_map(|entry| {
            let mut s = entry.trim();
            if let Some(rest) = s.strip_prefix("http://") {
                s = rest;
            }
            if let Some(rest) = s.strip_prefix("https://") {
                s = rest;
            }
            if s.is_empty() {
                return None;
            }
            // Split host:port on the LAST ':' so IPv6 literals (unlikely here) or
            // stray colons degrade gracefully; a non-numeric tail keeps default port.
            match s.rsplit_once(':') {
                Some((host, port)) if !host.is_empty() => match port.trim().parse::<u16>() {
                    Ok(p) => Some((host.trim().to_string(), p)),
                    Err(_) => Some((s.to_string(), default_port)),
                },
                _ => Some((s.to_string(), default_port)),
            }
        })
        .collect()
}

pub struct Config {
    pub port: String,
    /// Host the HTTP listener binds — `QUEEN_BIND_ADDR`, default `0.0.0.0`:
    /// every interface, which is what the address was hardcoded to before the
    /// knob existed, so an upgrade changes nothing until someone sets it.
    ///
    /// HOST only. The port is `PORT` and nowhere else, because a variable that
    /// could carry its own port would be a second place to write one, and the
    /// two disagreeing is not a conflict anything downstream could resolve —
    /// so `load` rejects a value with a port instead of picking a winner.
    pub bind_addr: String,
    pub pg: deadpool_postgres::Config,
    pub pool_size: usize,
    // Postgres TLS (RUSTFIX item 5). C++ `PG_USE_SSL` (default false) /
    // `PG_SSL_REJECT_UNAUTHORIZED` (default true), config.hpp:90-91. When
    // `pg_use_ssl` is false the pool is created with `NoTls` (byte-for-byte
    // unchanged); when true a rustls connector is wired in `db.rs`, with cert
    // verification disabled iff `pg_ssl_reject_unauthorized` is false.
    pub pg_use_ssl: bool,
    pub pg_ssl_reject_unauthorized: bool,
    /// `PG_SSL_ROOT_CERT` — the PEM **CONTENT** of the CA that signed the
    /// database's certificate, not a path to it (same family shape as
    /// `PG_PASSWORD`: the value IS the material). Set it and the chain is
    /// verified against exactly that CA instead of the compiled-in Mozilla set,
    /// which is what a managed PostgreSQL on a private CA needs — Scaleway RDB,
    /// Cloud SQL and Aiven all present private chains, and before this knob
    /// existed the only way to connect at all was
    /// `PG_SSL_REJECT_UNAUTHORIZED=false`, i.e. encryption with no
    /// authentication.
    ///
    /// A supplied CA REPLACES the Mozilla set and outranks
    /// `PG_SSL_REJECT_UNAUTHORIZED=false`; both rules, and why, are in
    /// `pgtls.rs`'s header. Malformed PEM is fatal at boot, never a silent
    /// fall-back.
    ///
    /// **Trap:** a multi-line value does not survive `docker --env-file` or
    /// systemd's `EnvironmentFile` — they stop at the first newline. Use
    /// `-e PG_SSL_ROOT_CERT="$(cat ca.pem)"`, a compose block scalar, or a
    /// Kubernetes secret; or put the PEM on one line with `\n` for the
    /// newlines, which the parser un-escapes.
    pub pg_ssl_root_cert: Option<String>,
    pub stmt_timeout: Duration,
    pub zstd_level: i32,
    // Admission arbiter (admission.rs): one budget of concurrent write
    // transactions for the whole commit path, sensed via passive commit-train
    // detection. init/min/max bound the budget; pool_reserve keeps connections
    // out of it for unmetered writes and reads.
    pub admission_init: u64,
    pub admission_min: u64,
    pub admission_max: u64,
    pub admission_pool_reserve: u64,
    pub admission_train_gap_us: u64,
    pub admission_tick_ms: u64,
    // Guaranteed minimum lane shares of the budget (wake order under
    // exhaustion is fixed: ack > pop > push > maint).
    pub admission_share_push: f64,
    pub admission_share_pop: f64,
    pub admission_share_ack: f64,
    pub admission_share_maint: f64,
    pub admission_nosync_budget: u64,
    pub admission_trace: bool,
    // pop long-poll (RUSTFIX item 19). Default idle wait 30s (DEFAULT_TIMEOUT); the
    // re-query interval backs off exponentially instead of a fixed poll.
    pub pop_default_timeout_ms: u64,
    /// Canonical `new` | `all`, already normalized by `normalize_subscription_mode`.
    pub default_subscription_mode: String,
    pub pop_wait_initial_interval_ms: u64,
    pub pop_wait_backoff_threshold: u32,
    pub pop_wait_backoff_multiplier: f64,
    pub pop_wait_max_interval_ms: u64,
    // Long-poll pending gate on the pinned and discovery pop paths: each
    // backoff re-poll probes a cheap indexed has_pending sibling (no admission
    // permit) and parks on `false` instead of running the full pop SP — the
    // same structure the wildcard path has carried since 2026-07-24. Kill
    // switch only; the wildcard gate is not governed by it.
    pub pop_pending_gate: bool,
    // cross-request fusion
    pub fusion_shards: usize,
    pub fusion_frames: usize,
    pub fusion_hold_ms: u64,
    // background retention/eviction sweep cadence (ms)
    pub retention_interval_ms: u64,
    // Broker-side dedup cache (doc 18 §5, server/src/dedup.rs). `dedup_cache_mb`
    // caps the global cache footprint (whole-partition LRU eviction);
    // `dedup_cache_enabled` is the kill switch — disabled, the broker always
    // sends p_verified = -1 and SQL probes the full window (always sound).
    //
    // SIZING: the cache holds ~16 bytes per in-window message hash across all
    // active partitions, so the working set is roughly
    //     needed_mb ≈ 16 × msg_rate(msg/s) × dedup_window_seconds / 1e6
    // (e.g. 700k msg/s × 300s ≈ 3.4 GB → set QUEEN_DEDUP_CACHE_MB≈6144). Under
    // the cap the cache degrades GRACEFULLY, not catastrophically: partitions
    // that don't fit are SUPPRESSED (they push with p_verified=-1 and pay a PG
    // full-window probe instead of a broker re-hydration) rather than triggering
    // a re-hydration thrash loop. A one-line rate-limited warning with this
    // formula is logged while suppression is active.
    #[allow(dead_code)]
    pub dedup_cache_mb: usize,
    #[allow(dead_code)]
    pub dedup_cache_enabled: bool,
    // Broker-side ACK REGISTRY (server/src/ack_registry.rs). Caps the in-memory
    // lease→(worker, batch_end, batch-hash-set) map that turns a full-batch
    // completed ack into one positional cursor advance (no PG hash resolution).
    // Whole-entry LRU eviction under the cap; an evicted/expired lease just takes
    // the unchanged queen.log_ack_by_hash_v1 path — always sound. Entries are
    // small and short-lived (consumed on ack), so 64 MB holds a very large number
    // of concurrently in-flight leases.
    pub ack_registry_mb: usize,
    // Kill switch (QUEEN_ACK_REGISTRY=0 disables). Disabled ⇒ every ack takes the
    // unchanged log_ack_by_hash_v1 path — exactly the pre-registry behaviour.
    pub ack_registry_enabled: bool,
    // Broker-side ACK FUSION (server/src/ack_fusion.rs). Coalesces registry-fast-
    // path full-batch acks into ONE queen.log_ack_multi_v1 transaction per flush
    // (one commit / one fsync for N cursor advances), fire-on-idle like the push
    // fusion. `ack_fusion_enabled` is the flag (QUEEN_ACK_FUSION, default ON;
    // "0"/"false" reverts to the synchronous log_ack_at_v1 fast path, byte-
    // identical to the pre-fusion behavior);
    // `ack_fusion_shards` buffers acks by hash(partition) (defaults to
    // fusion_shards); `ack_fusion_hold_ms` is the liveness backstop tick, NOT a
    // latency floor (fire-on-idle commits a lone ack in one RTT).
    pub ack_fusion_enabled: bool,
    pub ack_fusion_shards: usize,
    pub ack_fusion_hold_ms: u64,
    pub pop_fusion_enabled: bool,
    pub pop_fusion_shards: usize,
    pub pop_fusion_hold_ms: u64,
    pub pop_fusion_max_jobs: usize,
    pub pop_fusion_max_inflight: u32,
    // Retention/metrics background-job knobs (RUSTFIX item 20 — C++ JobsConfig,
    // config.hpp:286-329). `retention_batch_size` bounds each metrics-purge DELETE;
    // `metrics_retention_days` is the worker/system-metrics purge window (default
    // 90, matching the C++ RetentionService, not the old hardcoded 7).
    // `retention_parallelism` is LIVE again (2026-08-10): it is the number of
    // concurrent per-partition step workers in retention phases 1-3. It was
    // parsed-and-ignored from the log-engine port until then, which is why the
    // deletion rate was capped at one partition at a time — the measured ceiling
    // (~13.8k step rows/s) sat just under what 1M msg/s needs (~14.6k), so the
    // DB grew without bound at 1M and held fine at 600k. Batch size cannot
    // substitute: the step cost is per-row, and a bigger batch only lengthens
    // the push-allocator lock hold (see retention.rs module docs).
    //
    // `partition_cleanup_days` IS live again (2026-07-30): retention phase 4 deletes
    // empty, inactive-for-that-long partitions via
    // queen.log_partition_cleanup_step_v1, restoring the C++
    // RetentionService::cleanup_inactive_partitions() the log engine had dropped.
    // Floor of 1 day is the C++ clamp; the off switch is the enabled flag, so 0 does
    // not silently mean "delete everything quiet since a second ago".
    pub retention_batch_size: usize,
    /// Concurrent per-partition step workers in retention phases 1-3. Default 1
    /// (the historical serial cycle — existing deployments do not change
    /// behaviour by upgrading); clamped to retention::MAX_PARALLELISM. Each
    /// worker holds one maintenance-lane admission slot and one pooled
    /// connection while it runs, so raise QUEEN_ADMISSION_SHARE_MAINT with it.
    pub retention_parallelism: usize,
    /// Ceiling on the DUE PARTITIONS one cycle pulls per queue per rule
    /// (QUEEN_RETENTION_DUE_CAP; 0 = derive, see `load`). The work list is
    /// ordered oldest-watermark-first, so anything past the cap is simply the
    /// next cycle's head — the bound costs latency on a backlog, never
    /// coverage. Sized against the measured cell: ~500 newly-eligible
    /// segments/s over a 5 s cycle is ~2500 partition visits, at 2-5 ms a step
    /// call, so the default leaves ~2x headroom at `retention_parallelism = 1`
    /// and scales with the workers that have to execute those calls.
    pub retention_due_cap: usize,
    /// Cadence of the retention WATERMARK SAFETY WALK
    /// (QUEEN_RETENTION_SAFETY_WALK_MS, default 86_400_000 = daily). The walk
    /// re-derives `log_partitions.oldest_live_at` / `oldest_txn_at` from
    /// reality and repairs drift, so the per-cycle work list is allowed to
    /// trust a cached fact (001_log_schema). It is ALSO the one-time BACKFILL
    /// that makes those columns non-NULL in the first place.
    ///
    /// 0 disables the RECURRING walk only — the first walk still runs, because
    /// a broker that skipped it would have an empty work list and retention
    /// would stop with nothing in the log to say so. See retention.rs.
    pub retention_safety_walk_ms: u64,
    pub metrics_retention_days: i32,
    pub partition_cleanup_days: i32,
    pub partition_cleanup_enabled: bool,
    // stats reconciler cadence (ms) — segments-native queen.stats refresh
    // (server/src/stats.rs). Mirrors the C++ StatsService STATS_INTERVAL_MS.
    pub stats_interval_ms: u64,
    // retained-bytes slow-lane cadence (ms) — queen.stats.retained_bytes
    // refresh (server/src/stats.rs::spawn_retained_bytes, 028_retained_bytes).
    // The one O(segments) heap scan in the stats family, deliberately on its
    // own much slower loop; feeds only the proxy storage-quota gauge, which is
    // hysteretic by contract. Per-replica like every loop knob (the advisory
    // lock serializes runs, it does not deduplicate them — x3-replica
    // arithmetic, see helm_v1/broker/prod.yaml).
    pub retained_bytes_interval_ms: u64,
    // worker/system metrics flush cadence (ms) — server/src/syscollect.rs writes
    // one queen.worker_metrics + queen.system_metrics row per replica per window.
    pub metrics_flush_ms: u64,
    // JWT authentication (disabled by default).
    pub auth: AuthConfig,
    // Inter-instance mesh notifications (enabled by default, but inert with no peers).
    pub sync: SyncConfig,
    // Disk spool for DB-outage / maintenance push durability (RUSTFIX items 1, 17).
    pub file_buffer: FileBufferConfig,
    // EMBEDDED MODE for the Kafka wire facade (server/src/kafka_facade.rs).
    // Off by default: nothing is spawned, nothing is logged, no behaviour changes.
    pub kafka_facade: KafkaFacadeConfig,
    // Wildcard candidate hot-list (19-wildcard-hotlist.md, server/src/hotlist.rs).
    // Broker-side candidate selection for wildcard pops. Default ON;
    // QUEEN_HOTLIST=0 (or =false) reverts to the legacy per-pop SQL candidate
    // scan, byte-identical (every hook is a no-op / one branch). `hotlist_shards` sub-
    // shards each (queue,group) ring (defaults to fusion_shards); `hotlist_window_batch`
    // is the windowBuffer early-promotion threshold (§6).
    pub hotlist_enabled: bool,
    pub hotlist_shards: usize,
    pub hotlist_window_batch: u32,
    // Reseed / cold-start interval (ms) — §8 correctness floor for missed marks /
    // dropped mesh hints. Default 30s; lower for tests / tighter cross-broker floor.
    pub hotlist_reseed_ms: i64,
    // How often a reseed is a FULL walk over every partition of the queue instead of
    // the windowed walk over recently-written ones (ms). Default 5 min.
    //
    // The window is sound for everything a push creates, because a push is the only
    // way a partition BECOMES pending and every push bumps last_write_at. What only
    // the full walk finds: a ring entry wrongly cleared, an INFLIGHT stranded by a
    // dropped pop future, a WHEEL park left behind by a lost promote-on-ack, a mesh
    // hint dropped past the coalescing bound, and a backward seek on another broker.
    // Every one of those is a repair path, so this knob is really "how long may a
    // repair hide" — and it trades directly against database CPU, because the full
    // walk is Theta(partitions in the queue) per ring while the windowed one is
    // Theta(partitions written in the window). Both run log_hotlist_reseed_window_v1
    // — the full one with its lower bound pinned to '-infinity' — since 2026-08-23,
    // when the dedicated full-walk statement turned out to be Theta(partitions in the
    // CELL) under the generic plan prepare_cached converges to (its header in
    // 004_log_pop.sql has the numbers).
    //
    // 0 is the config-only kill switch, and it restores the pre-windowing broker
    // (C2, PLAN_HOTLIST_FOLLOWUP.md) — which is more than the periodic cadence:
    //   * every PERIODIC reseed is a full walk again, at QUEEN_HOTLIST_RESEED_MS;
    //   * the repair walks that this patch added around a cursor move stop fanning
    //     their rows out to the peers, and the one it added to the consumer-group
    //     delete does not run at all. Both existed only because a peer's windowed pass
    //     cannot see a cursor that moved backwards; with every peer walking everything
    //     every 30s again, each one re-discovers it within a floor on its own. (A
    //     PER-PARTITION seek still sends its one hint: that is a single item of the
    //     same shape and cost as a push's, not the whole-pending-set fan-out this
    //     bullet is about.);
    //   * the durable repair markers (queen.hotlist_repairs) are still WRITTEN, so a
    //     mixed cluster keeps working, but this broker stops polling them.
    // What still runs is the seek's own LOCAL full walk, which predates the windowing
    // by a year and is not part of the trade.
    pub hotlist_reseed_full_ms: i64,
    // Lookback of the windowed reseed (ms). 0 = derive it as max(4x reseed interval,
    // 120s). Wider than the interval on purpose: the window must survive a few
    // skipped or failed passes, last_write_at is quantized to 1s by the push path,
    // and the broker's clock is not the database's. Cost is linear in the partitions
    // actually written in the window, so generosity here is nearly free. Derived or
    // explicit, the value in force is the clamped one — see resolve_reseed_window_ms
    // for the band and why a value below it is unsound rather than merely aggressive.
    pub hotlist_reseed_window_ms: i64,
    // Idle-eviction sweep cadence (ms) for the per-(tenant,queue) hot-list rings and
    // long-poll wake gates. Both use a second-chance flag, so a queue is dropped after
    // [sweep, 2×sweep) with no mark / pop / parked waiter and no live ring entry. This
    // is what bounds the two maps on a SHARED cell, where an untrusted tenant can name
    // an unbounded number of distinct queues. 0 disables the sweep (unbounded growth —
    // only for a single-tenant deployment that wants the pre-eviction behaviour).
    pub hotlist_idle_sweep_ms: u64,
    // `QUEEN_HOTLIST_UNSERVED_TRIM_MS` (default 30000; 0 disables). How long a hot-list
    // ring may go without a single SERVED POP on this broker before the trim drops it —
    // see `HotList::trim_unserved` for the full argument. This is the standby-broker
    // bound: rings are filled by paths that do not depend on this broker's own traffic
    // (the reseed timer, mesh marks from a peer's pushes) and drained only by pops, so a
    // broker behind an active/passive LB otherwise accumulates the whole cell's pending
    // set forever — measured 827 000 ready entries / 2.12 GB RSS on the passive broker of
    // the 2026-08-24 soak cell, against 727 MB on the active one, and a proximate cause
    // of two global OOMs. `hotlist_idle_sweep_ms` cannot bound it: that sweep requires an
    // EMPTY ring, which is exactly what a ring nobody drains never becomes.
    //
    // The trim keys on served pops and never on entry age: with ~1060 pops/s a legitimate
    // 827 000-entry backlog puts the FIFO head at ~780 s, so age cannot tell a starved
    // standby from a healthy, backlogged, actively-served queue. 0 restores the
    // pre-2026-08-24 behaviour exactly (no clock read, no scan, no eviction).
    pub hotlist_unserved_trim_ms: i64,
    // POP AUTOPILOT (server/src/pop_autopilot.rs) — the broker choosing `partitions`
    // (and echoing `batch`) for a grouped wildcard pop that opted in with
    // `?autopilot=true`. A request that does not carry the parameter is treated
    // byte-identically in every one of the three switch positions, so this knob is
    // about what the controller may DO, never about whether the surface exists.
    //
    // `QUEEN_POP_AUTOPILOT` (on | shadow | off, default on; the boolean spellings
    // are accepted and mean on/off): `shadow` computes the decision and reports it
    // without applying it — the rollout position — and `off` removes the controller
    // entirely, including its in-memory lane state and its logs.
    //
    // The rest are the law's constants. `TARGET_AGE_MS` is the ready-age the width
    // loop steers to (a quarter of it is the shrink threshold); the two DWELL knobs
    // bound how often one (tenant, queue, group) may change its width, in BOTH
    // directions, and exist to stop the multiplicative loop from limit-cycling;
    // `MAX_LANES` bounds the in-memory state on a cell with a very large number of
    // queues (past it the controller stops creating lanes, which degrades to
    // today's defaults rather than evicting a live one).
    //
    // `BATCH` is the batch handed to a consumer that DELEGATED that dimension, and
    // only to such a consumer — a client that sends its own `batch` is never
    // touched by it. It is 100 rather than the 200 an absent field resolves to in
    // the handler because 100 is what the fleet has actually run (every SDK
    // defaults to it client-side) and because the matched arms of 2026-08-24
    // measured 200 at +35% p99. The full argument, and the drain-aware successor
    // this constant is standing in for, are at
    // `pop_autopilot::AUTO_BATCH_DEFAULT`.
    //
    // `BURST_CAP` is the FEED-FORWARD BURST BYPASS ceiling (0..=64, default 0 =
    // disabled). With it set, a delegated width becomes `max(W_steady, min(ready,
    // cap))` for that ONE request: the feedback loop still owns the steady width and
    // its dwell, but a lane that has 393 partitions ready right now no longer drains
    // them at width 1 for four dwell periods. It can never exceed what exists at that
    // instant, so it cannot produce the speculative wide scan the controller exists
    // to prevent. The full argument is at `pop_autopilot::burst_width`.
    pub pop_autopilot_mode: String,
    pub pop_autopilot_batch: i32,
    pub pop_autopilot_target_age_ms: f64,
    pub pop_autopilot_dwell_ms: i64,
    pub pop_autopilot_dwell_pops: u32,
    pub pop_autopilot_burst_cap: i32,
    pub pop_autopilot_max_lanes: usize,
    // LOGGING_PLAN.md Phase 1: cadence of the periodic `rates`/`sizes` aggregate
    // log blocks (ms), and how many hot queues the per-queue lines rank & show.
    pub log_rates_ms: u64,
    pub log_top_n_queues: usize,
    // Track B (PLAN_QUEEN_PROXY_CLOUD.md §5): native tenant scoping on queue
    // identity. Default OFF ⇒ every request uses config::DEFAULT_TENANT and the
    // broker behaves byte-identically to today. ON ⇒ the tenant is read from the
    // `x-queen-tenant` header (absent ⇒ default tenant; malformed ⇒ 400). The
    // broker never validates the value — it is opaque; the trust is the cell
    // network (the proxy is the only thing that can set the header).
    pub tenancy_header: bool,

    // ----------------------------------------------------- kv + timers (PLAN_KV_TIMERS.md)
    //
    // THERE IS NO `kv_enabled` / `timers_enabled` HERE, and that absence is the
    // decision (Alice, 2026-08-18, supersedes §16 and §20.4). KV and timers are not
    // features a deployment opts into, they are part of the engine, like push and pop
    // — and `Config` carries no `push_enabled` either. A boot flag is not a neutral
    // convenience: its existence is the statement that the surface is optional, that a
    // client may find a cell without it, and that every SDK, doc page and dashboard
    // must carry the "if enabled" caveat. So the flags are gone rather than defaulted
    // to `true`.
    //
    // What remains, in `switches.rs`, is the RUNTIME kill switch (`kv_enabled`,
    // `timers_schedule_enabled`, `timers_fire_enabled` in `queen.system_state`), which
    // is a different instrument: same class as maintenance mode, flipped by an
    // operator during an incident on a live cell, expected to be flipped back, and
    // read on every call rather than once at boot. Read the header of `switches.rs`
    // before reintroducing anything here — a gate and a kill switch look alike and are
    // opposites.
    //
    // What follows are the LIMITS and CADENCES of those surfaces. Every one is a knob
    // on something that is always running; none of them can turn it off.
    //
    // --- KV shape limits (§9.2). "Shape" limits live in SQL, inside the SP, plus a
    // body guard at the HTTP boundary: seven clients and the EMBEDDED broker (which
    // never passes through the handlers) inherit one rule instead of seven copies.
    // These values are what the broker hands the SP, so the two enforcement points
    // can never drift.
    //
    // NOTE, and it must be documented rather than discovered: the two points MEASURE
    // DIFFERENT THINGS. The HTTP boundary measures raw body bytes; the SP measures
    // `octet_length(value::text)`, the canonical JSONB text, which is normally
    // shorter. A value near the ceiling can pass one and fail the other.
    pub kv_max_value_bytes: usize,
    /// 512. The PK index tuple is `uuid(16) + namespace + key` and the btree ceiling
    /// is ~2704 bytes, so 64+512+16 sits at a fifth of it. Without the cap the failure
    /// mode is a `54000` on the first insert — runtime instead of validation.
    pub kv_max_key_bytes: usize,
    /// Ops per call. The IN-WIRE cap is a fixed 64 inside `kv_apply_v1` (§6.1) — it is
    /// a property of `p_in_wire`, not a knob, so it is deliberately absent here.
    pub kv_max_ops_per_call: usize,
    /// Budget per CALL, not per operation (§6.1 point 4). The op count alone bounds
    /// nothing: 63 `getMany` of 256 keys read 16 128 rows, and with values near the
    /// ceiling that is ~1 GiB of detoast BEFORE the first partition lock is taken and
    /// WHILE the `queen.kv` row locks are held. Three budgets apply together: op
    /// count, the SUM of keys across all ops, and the read-bytes aggregate below.
    pub kv_max_keys_per_call: usize,
    /// 4 MiB, applied to the aggregate INSIDE the SP — including when `p_in_wire`,
    /// not only on the HTTP routes. A cap on the number of keys is not a cap on
    /// bytes: 1000 keys of 64 KiB are 64 MB. The real resource is the byte.
    pub kv_max_read_bytes: usize,
    /// `getPrefix` limit ceiling. A request's `limit` is CLAMPED to it, never
    /// rejected, and `truncated` tells the truth — a 400 on too high a limit is an
    /// error the user cannot fix without reading the server's config.
    pub kv_prefix_limit: usize,

    // --- KV serving (§8.4). This is the FIRST endpoint of the product whose call
    // rate is decided by the customer's END USERS rather than by message volume.
    // Every other route has a natural limiter upstream; here the limiter is somebody
    // else's web traffic.
    /// DEDICATED connection pool, DERIVED from the pool rather than hardcoded (the
    /// same precedent as `admission_floor` below). The pool IS the semaphore, and the
    /// property no other defence gives is this: at ~1 ms reads its capacity is far
    /// above any rate limit, so it does not bind in normal conditions; it becomes the
    /// limiter exactly when the DATABASE slows down, and the overflow takes a 503
    /// instead of stealing connections from the message path.
    pub kv_pool_size: usize,
    /// With tenancy ON, the ABSENCE of a `queen.kv_quota` row is a DENIAL (403
    /// `feature_gated`), not a permission (§9.4 correction 1): the tenant header is
    /// opaque and unvalidated, so a client that rotates it on every request would
    /// otherwise mint a fresh unlimited tenant each time. DERIVED from the tenancy
    /// flag so a self-hosted operator never has to understand why they should
    /// configure anything.
    pub kv_require_grant: bool,
    /// Per-tenant token bucket, evaluated BEFORE `pool.get()`, answering 429 +
    /// `Retry-After`. In the BROKER and not only in the proxy, because self-hosted and
    /// dedicated-without-proxy are real deployments and a defence that exists only in
    /// the proxy is not a defence of the product. 200 reads/s is ~20% of one core at
    /// 0,3-1 ms per PK read, and the rule being defended is that KV reads must not be
    /// able to consume more than ~10% of the backend CPU serving the log.
    pub kv_read_rate: u32,
    pub kv_read_burst: u32,
    /// 100 writes/s: every write is a durable commit (~4 ms fsync).
    pub kv_write_rate: u32,
    pub kv_write_burst: u32,
    /// Aggregate CELL ceiling above the per-tenant limits: N conforming tenants can
    /// sum to a non-conforming cell. 0 disables it.
    pub kv_cell_rate: u32,
    /// Size cap on the per-tenant in-RAM maps (token bucket, quota cache). The cap
    /// DENIES, it does not evict (§9.4 correction 2) — eviction under an unvalidated,
    /// attacker-chosen tenant id is just a slower unbounded map. The house precedent
    /// is `handlers/mod.rs`: negatives are never cached, for exactly this reason.
    pub kv_max_tenants: usize,
    /// Usage-rollup sub-cadence (§7.5). Same cost class as retention's
    /// `PARTITION_SWEEP_EVERY`: the phase is O(space), not O(work), so it gets its own
    /// slow clock. It must NEVER move onto the due-driven path.
    pub kv_usage_every_ms: u64,
    /// How often each broker RE-READS the measurement and the limits (§9.3). The
    /// enforcer is the local delta, so this cadence does not bound the overrun — it
    /// bounds how long a RELEASE takes, which is the half that may be slow.
    pub kv_quota_refresh_ms: u64,
    /// Release band, as a percentage of the cap. Block above the cap, release only
    /// under this — the same number and the same hysteresis as the proxy's
    /// `STORAGE_RELEASE_PERCENT` (`proxy/src/registry.rs:44`). Without a band a tenant
    /// oscillates in and out of the block on every refresh.
    pub kv_quota_release_percent: i64,
    /// The watermark, in percent, above which a tenant makes the cell "hot" and the
    /// usage rollup drops to the refresh cadence (§9.3). 80, because §14.3 point 5
    /// says that with a soft quota 80% is already late.
    pub kv_quota_hot_percent: i64,
    /// Consecutive KV pool refusals before rung 5 of §12.1 engages and STANDALONE KV
    /// writes are shed. In-wire KV writes continue: the transaction is the value of
    /// the product and `POST /api/v1/kv` is the convenience.
    pub kv_standalone_shed_after: u32,

    // --- Timers shape limits (§9.2).
    /// DERIVED, not independent: `min(1 MiB, plan.max_payload_bytes)`. A timer BECOMES
    /// a message, so if its ceiling is not the message ceiling the timer is a service
    /// door around the plan's `max_payload_bytes`. Written as an independent constant,
    /// that hole opens by itself.
    pub timers_max_payload_bytes: usize,
    /// FINITE by default (90 days), never 0-means-unlimited. With an infinite horizon
    /// the row quota becomes permanent instead of cyclic: the tenant fills `max_timers`
    /// and never frees it. Finite, the worst case is computable:
    /// `rows <= schedule_rate * horizon`.
    pub timers_max_horizon_s: i64,
    /// Ops per `POST /api/v1/timers` batch. Also the metering unit: ONE message is
    /// billed per `schedule` op, never per call — at a cap of 256 ops, billing per
    /// call would under-count by up to 256x (§9.7, ratified §20.3).
    pub timers_max_ops_per_call: usize,

    // --- Ephemeral queues (EPHEMERAL_QUEUES.md §3.8). RAM-class queues whose
    // contents survive nothing.
    //
    // THERE IS NO `QUEEN_EPHEMERAL_ENABLED`, and its absence is the same decision
    // the KV and timer flags were removed for (see the header of `switches.rs`):
    // the routes are registered unconditionally, so no client can ever have to ask
    // whether a cell "has ephemeral queues". Pausing the surface at runtime is a
    // kill switch, and cloud gating is a grant — neither is a boot flag.
    //
    // Every knob below is a CEILING on a resource in this process's heap, which is
    // why they read differently from the durable ones: there is no database to
    // absorb an overrun, so the bound is the only thing between a mispublishing
    // producer and the cell's memory.
    /// `QUEEN_EPHEMERAL_MAX_BYTES` (256 MiB). The cell's total ephemeral footprint;
    /// crossing it answers 503 `ephemeral_unavailable` (§1.6 rung 3). ~3% of a
    /// 2c/8 GB free cell at full burn (§10 Q2); dedicated cells raise it.
    pub ephemeral_max_bytes: i64,
    /// `QUEEN_EPHEMERAL_QUEUE_MAX_BYTES` (16 MiB) — the per-queue DEFAULT, and the
    /// ceiling a `configure` may not exceed. Both roles, one number, deliberately:
    /// a separate "max the tenant may ask for" is a knob nobody would ever set
    /// differently and a second value to keep consistent.
    pub ephemeral_queue_max_bytes: i64,
    /// `QUEEN_EPHEMERAL_QUEUE_MAX_LENGTH` (10 000) — same dual role. A byte cap
    /// alone does not bound the per-message bookkeeping (cursors, leases, attempt
    /// counts), which is what a flood of tiny messages actually consumes.
    pub ephemeral_queue_max_length: i64,
    /// `QUEEN_EPHEMERAL_LEASE_S` (30). At-least-once holds only while the owning
    /// incarnation lives (§1.3), so this is a redelivery latency, never a
    /// durability window.
    pub ephemeral_lease_s: i64,
    /// `QUEEN_EPHEMERAL_RETRY_LIMIT` (5). Exhausted attempts DROP the message and
    /// count `eph_dropped_retry` — there is no DLQ in this class (§9), and a
    /// silent infinite retry would be a poison-message loop with no exit.
    pub ephemeral_retry_limit: u32,
    /// `QUEEN_EPHEMERAL_IMPLICIT_IDLE_S` (300). How long an IMPLICIT queue may sit
    /// empty and unpolled before it is collected. Declared queues are never
    /// collected: their configuration is durable and their emptiness is normal.
    pub ephemeral_implicit_idle_s: i64,
    /// `QUEEN_EPHEMERAL_REQUIRE_GRANT`. With tenancy ON, the ABSENCE of a grant row
    /// is a DENIAL (403 `feature_gated`) and not a permission — the identical
    /// posture, and the identical derivation, as `kv_require_grant` above (§1.6
    /// rung 2 / M7). The tenant header is opaque and unvalidated, so a client that
    /// rotates it on every request would otherwise mint a fresh unlimited tenant —
    /// and on THIS class an unlimited tenant is unlimited RAM.
    pub ephemeral_require_grant: bool,
    /// `QUEEN_EPHEMERAL_RATE` / `_BURST`: per-tenant token bucket over PUSHED
    /// MESSAGES per second (§1.6 rung 2), evaluated before a single byte is
    /// charged, answering 429 + `Retry-After`. A grant row's `max_msgs_per_sec`
    /// overrides both. The default is deliberately an order of magnitude above the
    /// durable commit-bound ceiling of a free cell: this class costs a `VecDeque`
    /// push, so the rate is a runaway-producer guard and not a throughput policy.
    pub ephemeral_rate: u32,
    pub ephemeral_burst: u32,

    // --- Sweeper (§7). ONE background component, TWO clocks. The asymmetry is a
    // principle, not a convenience: a late fire is product latency and is visible,
    // while a late KV prune is invisible (the `kv_live_v1` predicate already hides the
    // expired row on the first read) and costs only table size.
    //
    // LEADERLESS, unlike retention: retention takes xact advisory lock 737_001 and
    // one replica works per cycle; the sweeper must be the opposite on both axes,
    // due-driven and leaderless, so every replica drains in parallel sharing the work
    // through `SKIP LOCKED`. It takes NO advisory lock, so it consumes no new number
    // and cannot close a cycle with the advisory space (§2.1). 737_003 stays reserved
    // for PLAN_S3_ARCHIVE.md, which already claimed it.
    pub sweeper_enabled: bool,
    /// Sleep floor when there IS work. Not a latency floor for its own sake: a healthy
    /// timer lands within ~10 ms above this.
    pub sweeper_min_sleep_ms: u64,
    /// Sleep ceiling, and therefore the maximum recovery window for a timer scheduled
    /// on ANOTHER broker — this is the net that makes the `T_TIMER_DUE` mesh frame
    /// unnecessary in v1 (§7.4, §18.3). A multi-broker cluster with no mesh works,
    /// with at most one second of delay, and `deliverAt` is "no earlier than".
    pub sweeper_max_sleep_ms: u64,
    /// Empty-table backoff ceiling (§7.1): after K cycles with a NULL `nextInMs` and
    /// zero pruned rows the sleep climbs to here, reset by a local `hint()`. The 1 s
    /// ceiling is a net for DELIVERY latency and has no meaning when there is nothing
    /// to deliver.
    pub sweeper_idle_max_sleep_ms: u64,
    /// Claim lease. Also the exact window in which a `cancel` or a `reschedule` can
    /// answer `too_late` (§4.3), and the maximum delay after a broker dies holding
    /// claims (§12).
    pub sweeper_lease_ms: u64,
    /// Rows per claim call. The per-shard floor derived from it is
    /// `GREATEST(ceil(batch / 64), 8)` inside the SP (§6.2): plain integer division
    /// gives 3 at the default and 1 for an operator who lowers the batch, and at
    /// `v_per = 1` a single late shard can never catch up.
    pub sweeper_claim_batch: i64,
    /// Ceiling on rows drained per CYCLE. Claim and fire happen in the SAME iteration
    /// (§7.2): draining 5000 rows in 25 rounds up front means the first round's claims
    /// expire before the 25th fire commits, another broker re-claims them, our fire
    /// finds different tokens, every segment is `stale`, and two brokers steal work
    /// from each other while `lateMs` climbs — a LIVELOCK observable only as "the
    /// sweeper is slow".
    pub sweeper_cycle_max_rows: i64,
    /// Cap on the due probe's count (§6.2). It runs every cycle, and an exact count
    /// would be O(due) precisely in the failure the probe exists to detect; the probe
    /// reports `dueCapped` instead of lying.
    pub sweeper_due_cap: i64,
    /// Fire-transaction ceiling in BYTES, not segments (§7.2). Retention's lesson is
    /// that ITS step cost is per ROW so a bigger batch only stalls pushes; here the
    /// cost is per byte of WAL, so the cap is in bytes. Over it, the batch splits into
    /// several calls, each its own transaction.
    pub sweeper_max_fire_bytes: usize,
    /// `attempts` grows ONLY on permanent and config failures (§4.5). Past this the
    /// timer goes to the destination queue's DLQ with `consumer_group = '__timer__'`.
    /// Infrastructure never consumes the DLQ budget.
    pub sweeper_max_attempts: i32,
    /// Default 1, clamped by `sweeper::MAX_PARALLELISM` at the call site exactly like
    /// `retention_parallelism`. 1 because the fire is a WRITING transaction holding
    /// partition locks, not a maintenance delete: two concurrent fires on the same
    /// partitions contend for the same push serializer. Raise it only if `lateMs`
    /// grows under load AND the profile says the bottleneck is the round trip rather
    /// than the commit.
    pub sweeper_parallelism: usize,
    /// Poison isolation (§7.6). A fire is ONE transaction for many segments, so one
    /// poisoned segment fails the whole batch; without this that batch fails FOREVER
    /// and the healthy segments never arrive. On a PERMANENT error with more than one
    /// segment the broker replays the batch one segment per call: the healthy ones
    /// commit, the poisoned one takes its `attempts`. It is the most insidious failure
    /// of the feature, because the symptom (`lateMs` rising) does not name the culprit.
    pub sweeper_isolate_on_permanent: bool,
    /// Per-tenant round-robin budget INSIDE the claim (§6.2). Applied after the claim
    /// it would not work at all: discarding already-claimed rows puts them back at the
    /// head of the index (`visible_at` did not change) and they are re-selected next
    /// cycle — a livelock that burns the whole batch every round. Because the shard is
    /// a hash of `timer_key` and carries no tenant, one tenant's timers spread over all
    /// 64 shards, so a tenant scheduling 200 000 timers for 09:00 fills EVERY claim
    /// batch of EVERY broker until it drains.
    pub sweeper_fire_rate_per_tenant: i64,
    /// Exponential backoff for a failed fire: `min(min_ms * 2^attempts, max_ms)`,
    /// computed broker-side. On a TRANSIENT failure the attempt is not counted.
    pub sweeper_backoff_min_ms: i64,
    pub sweeper_backoff_max_ms: i64,
}

/// File-buffer configuration, mirroring the C++ `FileBufferConfig`
/// (config.hpp:356-392). Drives `file_buffer.rs`. Defaults match C++ exactly.
#[derive(Clone)]
pub struct FileBufferConfig {
    pub dir: String,
    pub flush_interval_ms: u64,
    pub max_batch_size: usize,
    pub max_events_per_file: usize,
}

impl FileBufferConfig {
    fn from_env() -> FileBufferConfig {
        FileBufferConfig {
            // Plan pins the Linux default; the C++ Apple `/tmp/queen` default is
            // dev-only. Must be a writable, persistent path for durability.
            dir: env_str("FILE_BUFFER_DIR", "/var/lib/queen/buffers"),
            flush_interval_ms: env_int("FILE_BUFFER_FLUSH_MS", 100).max(1) as u64,
            max_batch_size: env_int("FILE_BUFFER_MAX_BATCH", 100).max(1) as usize,
            max_events_per_file: env_int("FILE_BUFFER_EVENTS_PER_FILE", 10000).max(1) as usize,
        }
    }
}

/// Embedded queen-kafka (server/src/kafka_facade.rs): the Kafka wire facade run as
/// a supervised CHILD PROCESS of this broker, talking to it over loopback HTTP.
///
/// Three knobs, and only the first one decides anything: with `enabled` false the
/// supervisor is never constructed, no child is spawned, and `/status` renders the
/// two fields it always did. The CHILD's own configuration is not repeated here —
/// it inherits this process's environment, so every `QUEEN_KAFKA_*` variable the
/// facade documents (`QUEEN_KAFKA_ADVERTISED_ADDR`, `_ADDR`, `_DEFAULT_PARTITIONS`,
/// `_SASL`, `_TLS_*`, the cluster and group timings) forwards verbatim and has
/// exactly the meaning it has when the facade runs on its own.
#[derive(Clone)]
pub struct KafkaFacadeConfig {
    pub enabled: bool,
    /// Empty means "the `queen-kafka` file next to the broker executable", which
    /// is resolved from `current_exe` at boot rather than written down here: a
    /// path derived from argv is not a configuration default.
    pub bin: String,
    /// How long a stopping child has between SIGTERM and SIGKILL.
    pub shutdown_grace_ms: u64,
}

impl KafkaFacadeConfig {
    fn from_env() -> KafkaFacadeConfig {
        KafkaFacadeConfig {
            enabled: env_bool("QUEEN_KAFKA_EMBEDDED", false),
            bin: env_str("QUEEN_KAFKA_BIN", ""),
            // 5s: the facade's shutdown is closing sockets, not draining work —
            // every offset it holds is already in Queen — so the window only has
            // to cover a process that is mid-syscall, not one with state to
            // flush. Floored at 100ms because a grace of zero is a SIGKILL with
            // extra steps.
            shutdown_grace_ms: env_int("QUEEN_KAFKA_SHUTDOWN_GRACE_MS", 5000).max(100) as u64,
        }
    }
}

// C++ `get_env_string` parity (config.hpp:29-33): a present-but-empty env var
// returns "" verbatim; only a genuinely-unset var falls back to the default.
// (RUSTFIX item 6 — the old `.filter(|v| !v.is_empty())` treated ""` as unset,
// which silently restored default lists e.g. for JWT_SKIP_PATHS="".)
/// Canonicalize a subscription mode to one of the two spellings the SQL actually
/// understands.
///
/// `004_log_pop.sql` matches with `COALESCE(p_sub_mode,'all') = 'new'` — an EXACT
/// string compare — so every other spelling falls through to full-backlog replay.
/// That silently broke a documented alias: the Go SDK exports
/// `SubscriptionModeNewOnly = "new-only"` ("alias for SubscriptionModeNew"), the
/// CLI advertises `all|new|new-only` in `--from-mode`, and the JS README shows
/// `.subscriptionMode('new-only')` as real usage — yet `new-only` reached the SQL
/// verbatim, missed the `= 'new'` arm, and replayed everything: the exact opposite
/// of what it advertises, on every client, with no test catching it.
///
/// Anything not recognized as "start at the tail" still resolves to `all`, which is
/// both the historical fallthrough and the safe direction (a consumer that replays
/// too much is recoverable; one that skips messages is not). That deliberately keeps
/// working the informal spellings already in the test suites, e.g. `from_beginning`.
pub(crate) fn normalize_subscription_mode(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "new" | "new-only" | "new_only" | "newonly" => "new".to_string(),
        _ => "all".to_string(),
    }
}

/// Join a bind host and a port into an authority, bracketing an IPv6 literal.
/// Unbracketed, `::1` + `6632` is `::1:6632`, which is NOT a `SocketAddr` —
/// that grammar requires the brackets — so it falls through to the name
/// resolver, where it survives only because the platform's getaddrinfo happens
/// to take a bare numeric v6 host. Bracketing keeps the bind on the parse path
/// rather than resting on that, and puts the canonical form in the boot line,
/// which is the string an operator pastes into curl. A hostname passes through
/// untouched and is resolved at bind time.
pub fn host_port(host: &str, port: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// The HTTP listener's bind host, validated. Also the mesh listener's default,
/// which is why it is a function and not an inline `env_str`.
fn http_bind_addr() -> String {
    checked_bind_addr("QUEEN_BIND_ADDR", "PORT", env_str("QUEEN_BIND_ADDR", "0.0.0.0"))
}

/// FATAL on a bind host that carries a port, or that is explicitly empty.
/// Both would otherwise be joined with the port into an address nothing
/// listens on, and the process would die at bind quoting a string no operator
/// ever wrote (`0.0.0.0:7000:6632`). An IP literal — v4 or v6 — or a hostname
/// is accepted here; whether the hostname RESOLVES is still the bind's problem,
/// because resolution can fail for reasons that have nothing to do with config.
/// `port_key` is the variable that owns the port for THIS listener, so the
/// message points at the right one rather than at `PORT` for the mesh.
fn checked_bind_addr(k: &str, port_key: &str, v: String) -> String {
    if v.is_empty() {
        crate::obs::fatal(format!(
            "{k} is set to the empty string — give it a host or IP, or unset it for 0.0.0.0"
        ));
    }
    if v.parse::<std::net::IpAddr>().is_err() && v.contains(':') {
        crate::obs::fatal(format!(
            "{k}={v} carries a port — set the host or IP only, the port comes from {port_key}"
        ));
    }
    v
}

fn env_str(k: &str, def: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| def.to_string())
}
fn env_int(k: &str, def: i64) -> i64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(def)
}
// RUSTFIX item 4: PG_DATABASE (Rust name) → PG_DB (C++ name, config.hpp:86) →
// "postgres". Uses a non-empty filter independent of `env_str`'s item-6 semantics
// so an explicitly-empty PG_DATABASE still falls through instead of yielding "".
pub fn resolve_db_name() -> String {
    std::env::var("PG_DATABASE")
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| std::env::var("PG_DB").ok().filter(|v| !v.is_empty()))
        .unwrap_or_else(|| "postgres".to_string())
}
fn env_f64(k: &str, def: f64) -> f64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(def)
}
// ---------------------------------------------------------------------------
// Boolean env parsing — ONE parser for every boolean knob in the broker.
//
// History: the C++ `get_env_bool` (config.hpp:11-15) accepted only the exact
// literal "true", and RUSTFIX item 6 kept that parity to stop a permissive parser
// from turning `JWT_ENABLED=1` into an accidental auth-ENABLE. But strictness with
// a silent fallback fails in the OTHER direction just as badly: `JWT_ENABLED=1`
// resolved to the `false` default, i.e. a broker running with authentication OFF
// and not a word in the log.
//
// The fix keeps the intent (no value is ever guessed into a security-relevant
// direction) while removing the silence:
//   * the common spellings are accepted, case-insensitively — true/false, 1/0,
//     yes/no, on/off (surrounding whitespace trimmed);
//   * anything else non-empty is a FATAL config error naming the variable and the
//     bad value — the default is never used to paper over a typo;
//   * unset (or present-but-empty, e.g. `JWT_ENABLED=` in a compose file) uses the
//     documented default.
// Every boolean env read in the broker routes through here, so `=on` and `=0` mean
// the same thing everywhere.
// ---------------------------------------------------------------------------

/// The accepted spellings, listed in the error message so an operator who gets it
/// wrong is told exactly what is legal.
const BOOL_SPELLINGS: &str = "true/false, 1/0, yes/no, on/off";

/// Pure parser: `Some(bool)` for a recognised spelling, `None` otherwise.
/// Case-insensitive, whitespace-trimmed. An empty/whitespace-only string is NOT
/// recognised here — callers treat that as "unset" (see `resolve_bool`).
pub(crate) fn parse_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Resolution rules, factored out of the env lookup so they are unit-testable
/// without mutating process-global state.
fn resolve_bool(key: &str, raw: Option<&str>, def: bool) -> Result<bool, String> {
    match raw {
        // Unset, or set to the empty string (a very common way to "leave it
        // alone" in compose/Helm) ⇒ the documented default.
        None => Ok(def),
        Some(v) if v.trim().is_empty() => Ok(def),
        Some(v) => parse_bool(v).ok_or_else(|| {
            format!("{key}=\"{v}\" is not a boolean (expected {BOOL_SPELLINGS})")
        }),
    }
}

/// Env-reading form that reports a bad value instead of aborting — used by
/// `validate_bools` to collect every mistake in one pass.
pub(crate) fn env_bool_checked(k: &str, def: bool) -> Result<bool, String> {
    resolve_bool(k, std::env::var(k).ok().as_deref(), def)
}

/// The call-site form: an unparseable value kills the process with a clear message
/// rather than silently resolving to `def`.
pub(crate) fn env_bool(k: &str, def: bool) -> bool {
    match env_bool_checked(k, def) {
        Ok(v) => v,
        Err(e) => crate::obs::fatal(e),
    }
}

/// Render a secret for the startup config block: never the value, only whether it
/// is set and how long it is (enough to tell "I forgot to mount the secret" from
/// "I mounted the wrong one" without putting key material in a log shipper).
fn mask(secret: &str) -> String {
    if secret.is_empty() {
        "<unset>".to_string()
    } else {
        format!("<set:{} chars>", secret.chars().count())
    }
}

/// One startup block printing the configuration the broker ACTUALLY resolved —
/// the counterpart to the strict boolean parser above. A knob that was misspelled,
/// defaulted, or read from a legacy alias is visible here instead of having to be
/// inferred from behaviour. Secrets (JWT_SECRET, PG_PASSWORD,
/// QUEEN_ENCRYPTION_KEY, QUEEN_SYNC_SECRET) are masked by `mask`, never printed.
pub fn log_effective(cfg: &Config) {
    tracing::info!(
        target: "boot",
        version = crate::VERSION,
        port = %cfg.port,
        bind = %cfg.bind_addr,
        pool = cfg.pool_size,
        stmt_timeout_ms = cfg.stmt_timeout.as_millis() as u64,
        max_body_bytes = %env_str("QUEEN_MAX_BODY_BYTES", "<default>"),
        "config: server"
    );
    tracing::info!(
        target: "boot",
        host = %cfg.pg.host.clone().unwrap_or_default(),
        port = cfg.pg.port.unwrap_or_default(),
        user = %cfg.pg.user.clone().unwrap_or_default(),
        password = %mask(cfg.pg.password.as_deref().unwrap_or("")),
        database = %cfg.pg.dbname.clone().unwrap_or_default(),
        use_ssl = cfg.pg_use_ssl,
        ssl_reject_unauthorized = cfg.pg_ssl_reject_unauthorized,
        // What the pool ACTUALLY trusts, from the same pure functions db.rs
        // feeds the connector — `plaintext` / `webpki-roots` / `supplied-ca` /
        // `accept-any`. `ssl_verified` is the one question a security review
        // asks of this line: is the link to the database AUTHENTICATED, or
        // merely encrypted? The proxy prints the identical pair for the pxdb.
        ssl_trust = crate::pgtls::trust_label(
            cfg.pg_use_ssl,
            cfg.pg_ssl_root_cert.is_some(),
            cfg.pg_ssl_reject_unauthorized,
        ),
        ssl_verified = crate::pgtls::link_authenticated(
            cfg.pg_use_ssl,
            cfg.pg_ssl_root_cert.is_some(),
            cfg.pg_ssl_reject_unauthorized,
        ),
        "config: postgres"
    );
    tracing::info!(
        target: "boot",
        enabled = cfg.auth.enabled,
        algorithm = %cfg.auth.algorithm,
        secret = %mask(&cfg.auth.secret),
        public_key = %mask(&cfg.auth.public_key),
        jwks_url = %cfg.auth.jwks_url,
        issuer = %cfg.auth.issuer,
        audience = %cfg.auth.audience,
        clock_skew_s = cfg.auth.clock_skew_seconds,
        skip_paths = ?cfg.auth.skip_paths,
        "config: auth"
    );
    tracing::info!(
        target: "boot",
        enabled = cfg.sync.enabled,
        mesh_active = cfg.sync.mesh_active(),
        server_id = %cfg.sync.server_id,
        mesh_port = cfg.sync.mesh_port,
        mesh_bind = %cfg.sync.bind_addr,
        // EPHEMERAL_QUEUES.md §3.5/§3.6: what peers are told to forward to. On
        // the boot line because a wrong advertised address fails ONLY in the
        // other direction — this broker works perfectly and its peers cannot
        // reach it — which is the failure mode a boot line exists to pre-empt.
        http_addr = %cfg.sync.http_addr,
        peers = ?cfg.sync.peers,
        secret = %mask(&cfg.sync.secret),
        heartbeat_ms = cfg.sync.heartbeat_ms,
        dead_threshold_ms = cfg.sync.dead_threshold_ms,
        cache_refresh_ms = cfg.sync.cache_refresh_ms,
        "config: sync"
    );
    tracing::info!(
        target: "boot",
        fusion_shards = cfg.fusion_shards,
        fusion_frames = cfg.fusion_frames,
        fusion_hold_ms = cfg.fusion_hold_ms,
        dedup_cache = cfg.dedup_cache_enabled,
        dedup_cache_mb = cfg.dedup_cache_mb,
        ack_registry = cfg.ack_registry_enabled,
        ack_registry_mb = cfg.ack_registry_mb,
        ack_fusion = cfg.ack_fusion_enabled,
        ack_fusion_shards = cfg.ack_fusion_shards,
        ack_fusion_hold_ms = cfg.ack_fusion_hold_ms,
        hotlist = cfg.hotlist_enabled,
        hotlist_shards = cfg.hotlist_shards,
        hotlist_window_batch = cfg.hotlist_window_batch,
        hotlist_reseed_ms = cfg.hotlist_reseed_ms,
        hotlist_reseed_full_ms = cfg.hotlist_reseed_full_ms,
        hotlist_reseed_window_ms = cfg.hotlist_reseed_window_ms,
        hotlist_idle_sweep_ms = cfg.hotlist_idle_sweep_ms,
        hotlist_unserved_trim_ms = cfg.hotlist_unserved_trim_ms,
        zstd_level = cfg.zstd_level,
        "config: engine"
    );
    tracing::info!(
        target: "boot",
        pop_default_timeout_ms = cfg.pop_default_timeout_ms,
        pop_pending_gate = cfg.pop_pending_gate,
        pop_wait_initial_ms = cfg.pop_wait_initial_interval_ms,
        pop_wait_backoff_threshold = cfg.pop_wait_backoff_threshold,
        pop_wait_backoff_multiplier = cfg.pop_wait_backoff_multiplier,
        pop_wait_max_ms = cfg.pop_wait_max_interval_ms,
        // POP AUTOPILOT: on the pop line rather than a block of its own — it is a
        // pop knob, and §14.4's rule is that a third periodic block is a line
        // nobody reads.
        pop_autopilot = %cfg.pop_autopilot_mode,
        pop_autopilot_batch = cfg.pop_autopilot_batch,
        pop_autopilot_target_age_ms = cfg.pop_autopilot_target_age_ms,
        pop_autopilot_dwell_ms = cfg.pop_autopilot_dwell_ms,
        pop_autopilot_dwell_pops = cfg.pop_autopilot_dwell_pops,
        pop_autopilot_burst_cap = cfg.pop_autopilot_burst_cap,
        pop_autopilot_max_lanes = cfg.pop_autopilot_max_lanes,
        admission = %format!(
            "{}/{}/{}", cfg.admission_min, cfg.admission_init, cfg.admission_max),
        admission_pool_reserve = cfg.admission_pool_reserve,
        admission_train_gap_us = cfg.admission_train_gap_us,
        admission_tick_ms = cfg.admission_tick_ms,
        admission_shares = %format!(
            "push={} pop={} ack={} maint={}",
            cfg.admission_share_push, cfg.admission_share_pop,
            cfg.admission_share_ack, cfg.admission_share_maint),
        admission_nosync_budget = cfg.admission_nosync_budget,
        "config: flow"
    );
    // The Vegas-era knobs are gone with the limiter itself; anyone still
    // setting them (deploy files, helm) must hear about it at boot rather
    // than tune a ghost.
    for dead in [
        "QUEEN_SEG_PUSH_INIT", "QUEEN_SEG_PUSH_MIN", "QUEEN_SEG_PUSH_MAX",
        "QUEEN_SEG_POP_INIT", "QUEEN_SEG_POP_MIN", "QUEEN_SEG_POP_MAX",
        "QUEEN_VEGAS_ALPHA", "QUEEN_VEGAS_BETA",
    ] {
        if std::env::var(dead).is_ok() {
            tracing::warn!(target: "boot",
                var = dead,
                "deprecated knob is set but no longer read: admission is \
                 governed by QUEEN_ADMISSION_* (see admission.rs)");
        }
    }
    tracing::info!(
        target: "boot",
        retention_interval_ms = cfg.retention_interval_ms,
        retention_batch_size = cfg.retention_batch_size,
        retention_parallelism = cfg.retention_parallelism,
        // The number ACTUALLY in force, never the formula (the derived-value
        // rule the kv/timers block below states): an operator reading this line
        // is trying to explain a delete rate, and "0 = derive" explains nothing.
        retention_due_cap = cfg.retention_due_cap,
        // 0 here means "recurring walk off, backfill still runs" — the WARN
        // above says so at the same boot, so the number can stay bare.
        retention_safety_walk_ms = cfg.retention_safety_walk_ms,
        metrics_retention_days = cfg.metrics_retention_days,
        partition_cleanup = cfg.partition_cleanup_enabled,
        partition_cleanup_days = cfg.partition_cleanup_days,
        stats_interval_ms = cfg.stats_interval_ms,
        retained_bytes_interval_ms = cfg.retained_bytes_interval_ms,
        metrics_flush_ms = cfg.metrics_flush_ms,
        apply_schema = env_bool("QUEEN_APPLY_SCHEMA", true),
        "config: jobs"
    );
    tracing::info!(
        target: "boot",
        dir = %cfg.file_buffer.dir,
        flush_interval_ms = cfg.file_buffer.flush_interval_ms,
        max_batch_size = cfg.file_buffer.max_batch_size,
        max_events_per_file = cfg.file_buffer.max_events_per_file,
        "config: file_buffer"
    );
    // Embedded queen-kafka. Printed ONLY when it is on: default-off must add no
    // line to a boot log, so that a broker running without the feature reads today
    // exactly as it read before the feature existed. The child's own knobs are not
    // echoed here — it prints its whole resolved configuration itself, and that
    // line arrives in this same log stream tagged `kafka`.
    if cfg.kafka_facade.enabled {
        tracing::info!(
            target: "boot",
            bin = %if cfg.kafka_facade.bin.trim().is_empty() {
                "<beside the broker binary>".to_string()
            } else {
                cfg.kafka_facade.bin.clone()
            },
            shutdown_grace_ms = cfg.kafka_facade.shutdown_grace_ms,
            "config: kafka_facade"
        );
    }
    tracing::info!(
        target: "boot",
        encryption_key = %mask(&env_str("QUEEN_ENCRYPTION_KEY", "")),
        tenancy_header = cfg.tenancy_header,
        "config: security"
    );
    // PLAN_KV_TIMERS.md. No `kv=` / `timers=` line: they were the boot flags, and a
    // config block that still printed "kv: true" on every boot would keep teaching
    // operators that there is a state where it prints false. The derived values
    // (`kv_pool`, `kv_require_grant`, `timers_max_payload_bytes`) are printed with the
    // number ACTUALLY in force, not the formula — the formula is in the struct docs,
    // the log carries the value.
    tracing::info!(
        target: "boot",
        kv_pool = cfg.kv_pool_size,
        kv_require_grant = cfg.kv_require_grant,
        kv_max_value_bytes = cfg.kv_max_value_bytes,
        kv_max_key_bytes = cfg.kv_max_key_bytes,
        kv_budgets = %format!(
            "{}ops/{}keys/{}B",
            cfg.kv_max_ops_per_call, cfg.kv_max_keys_per_call, cfg.kv_max_read_bytes),
        kv_prefix_limit = cfg.kv_prefix_limit,
        kv_rate = %format!(
            "{}r/{}rb/{}w/{}wb/{}cell",
            cfg.kv_read_rate, cfg.kv_read_burst,
            cfg.kv_write_rate, cfg.kv_write_burst, cfg.kv_cell_rate),
        kv_max_tenants = cfg.kv_max_tenants,
        kv_usage_every_ms = cfg.kv_usage_every_ms,
        // The two cadences on one line, because the pair is what an operator needs
        // in order to read the published overrun formula: the rollup WRITES the
        // measurement and the refresh READS it, and no reader can be fresher than
        // its writer (§9.3). `hot` is the cadence in force above the watermark.
        kv_quota = %format!(
            "refresh {}ms / release {}% / hot above {}%",
            cfg.kv_quota_refresh_ms, cfg.kv_quota_release_percent, cfg.kv_quota_hot_percent),
        kv_standalone_shed_after = cfg.kv_standalone_shed_after,
        timers_max_payload_bytes = cfg.timers_max_payload_bytes,
        timers_max_horizon_s = cfg.timers_max_horizon_s,
        timers_max_ops_per_call = cfg.timers_max_ops_per_call,
        "config: kv_timers"
    );
    // EPHEMERAL_QUEUES.md §3.8. No `enabled=` field, for the same reason the block
    // above has none: printing "ephemeral: true" on every boot would keep teaching
    // operators that there is a state where it prints false (M9). What an operator
    // needs from this line is the three CEILINGS — this is the only feature in the
    // broker whose overrun is paid in RAM rather than in disk or latency.
    tracing::info!(
        target: "boot",
        eph_max_bytes = cfg.ephemeral_max_bytes,
        eph_queue = %format!(
            "{}B/{}msgs",
            cfg.ephemeral_queue_max_bytes, cfg.ephemeral_queue_max_length),
        eph_lease_s = cfg.ephemeral_lease_s,
        eph_retry_limit = cfg.ephemeral_retry_limit,
        eph_implicit_idle_s = cfg.ephemeral_implicit_idle_s,
        // The grant posture, printed with the value ACTUALLY in force rather than
        // the formula (the `kv_require_grant` rule one block up): "on" here means
        // a tenant with no row in the grant table is refused, which is the single
        // most confusing line to reconstruct from the environment during an
        // incident, because nothing in the environment spells it.
        eph_require_grant = cfg.ephemeral_require_grant,
        eph_rate = %format!("{}/{}burst msgs/s", cfg.ephemeral_rate, cfg.ephemeral_burst),
        "config: ephemeral"
    );
    tracing::info!(
        target: "boot",
        // `QUEEN_SWEEPER=false` is the one knob that still stops the task, and it is
        // not a feature gate: it turns off the REAPER of surfaces that keep running.
        // A cell with it off accumulates expired KV rows that are never pruned and
        // timers that never fire (main.rs warns at boot, once).
        enabled = cfg.sweeper_enabled,
        sleep_ms = %format!(
            "{}..{} (idle {})",
            cfg.sweeper_min_sleep_ms, cfg.sweeper_max_sleep_ms, cfg.sweeper_idle_max_sleep_ms),
        lease_ms = cfg.sweeper_lease_ms,
        claim_batch = cfg.sweeper_claim_batch,
        cycle_max_rows = cfg.sweeper_cycle_max_rows,
        due_cap = cfg.sweeper_due_cap,
        max_fire_bytes = cfg.sweeper_max_fire_bytes,
        max_attempts = cfg.sweeper_max_attempts,
        parallelism = cfg.sweeper_parallelism,
        isolate_on_permanent = cfg.sweeper_isolate_on_permanent,
        fire_rate_per_tenant = cfg.sweeper_fire_rate_per_tenant,
        backoff_ms = %format!(
            "{}..{}", cfg.sweeper_backoff_min_ms, cfg.sweeper_backoff_max_ms),
        "config: sweeper"
    );
    tracing::info!(
        target: "boot",
        level = %env_str("RUST_LOG", &env_str("LOG_LEVEL", "info")),
        json = env_bool("QUEEN_LOG_JSON", false),
        rates_ms = cfg.log_rates_ms,
        top_n_queues = cfg.log_top_n_queues,
        "config: logging"
    );
}

/// Boolean knobs read OUTSIDE this module (obs.rs, schema.rs, fusion.rs) — they
/// use `parse_bool`/`env_bool` at their own call sites, but several of them are
/// consulted lazily (or, for `QUEEN_LOG_JSON`, before the subscriber exists), so a
/// typo would surface late or not at all. `load()` validates them eagerly here so
/// EVERY boolean in the broker fails at the same moment, with the same message.
const EXTERNAL_BOOL_KEYS: &[(&str, bool)] = &[
    ("QUEEN_LOG_JSON", false),
    ("QUEEN_APPLY_SCHEMA", true),
    ("QUEEN_V2_BUNDLE_LOG", false),
];

/// `PG_SSL_ROOT_CERT` as raw PEM, or `None`. Whitespace-only counts as unset —
/// an env file that leaves the variable blank must mean "not configured", not
/// "configured with nothing", which would otherwise boot-fail every deployment
/// that templates the variable in unconditionally.
///
/// Read through this ONE function everywhere (`load`, `db::cancel_tls`, the
/// embedded pre-pass) so the pool, the cancel connector and the boot gate cannot
/// disagree about whether a CA is configured — the same discipline the comment
/// on `cancel_tls` already applies to the two booleans.
pub(crate) fn pg_ssl_root_cert() -> Option<String> {
    // Deliberately `env_str` and not a bare `std::env::var`: the webdoc's
    // config reference is SCRAPED from the env_bool/env_int/env_f64/env_str
    // call sites in this file (webdoc/scripts/gen-config.mjs), so a knob read
    // any other way ships undocumented.
    Some(env_str("PG_SSL_ROOT_CERT", "")).filter(|v| !v.trim().is_empty())
}

/// Validate the CA material and report the anchor count, WITHOUT exiting: the
/// binary turns an `Err` into `obs::fatal` in `load()`, the embedded library
/// path turns it into `StartError::Config` before `load()` can be reached
/// (`embedded/boot.rs`, exactly as it pre-validates the booleans).
///
/// Validated whenever the variable is SET, even with `PG_USE_SSL=false`: a
/// variable that is set is a statement of intent, and finding out at boot that
/// the PEM is truncated beats finding out at the next TLS handshake.
pub(crate) fn check_pg_ssl_root_cert() -> Result<Option<usize>, String> {
    match pg_ssl_root_cert() {
        None => Ok(None),
        Some(pem) => match crate::pgtls::root_store_from_pem(&pem) {
            Ok(roots) => Ok(Some(roots.len())),
            Err(e) => Err(format!(
                "PG_SSL_ROOT_CERT is set but unusable: {e}. It takes the PEM CONTENT of the CA \
                 certificate that signed the database's certificate — not a path, and not a \
                 fingerprint. Fix it or unset it; it is not ignored, because a database link \
                 believed to be verified and silently is not is the failure this variable exists \
                 to remove."
            )),
        },
    }
}

pub fn load() -> Config {
    for (k, def) in EXTERNAL_BOOL_KEYS {
        let _ = env_bool(k, *def);
    }

    let mut pg = deadpool_postgres::Config::new();
    pg.host = Some(env_str("PG_HOST", "localhost"));
    pg.port = Some(env_int("PG_PORT", 5432) as u16);
    pg.user = Some(env_str("PG_USER", "postgres"));
    pg.password = Some(env_str("PG_PASSWORD", "postgres"));
    // RUSTFIX item 4: prefer PG_DATABASE, fall back to the C++ name PG_DB, then
    // "postgres". Explicit non-empty chain (not env_str) so an empty PG_DATABASE
    // still falls through to PG_DB rather than resolving to "" under item 6.
    pg.dbname = Some(resolve_db_name());

    // --- admission floor (campaign 2026-08-03/04) --------------------------
    // The AIMD budget hunts in a band (measured 20-27) that sits BELOW what the
    // workload needs, because it shrinks on `oldest_age > 20ms` while healthy
    // pop work routinely takes 37-64 ms. Left at the old floor of 8 the anchor
    // shape ran at 1143 ms p50 with 12 510 messages permanently in flight and
    // 76 of 160 pool connections idle; raising the floor alone took it to
    // 240 ms. The floor is therefore a real default, not a tuning knob.
    //
    // DERIVED from the pool rather than hardcoded: admission.rs `ceiling()` is
    // `max.min(pool_size - reserve).max(min)`, so a fixed floor larger than the
    // pool would admit more concurrent transactions than there are connections
    // — exactly what the module's own contract forbids. Two thirds of the
    // usable pool yields 96 on the default (160 - 16), which is the value the
    // sweep settled on, and stays safe on a small deployment.
    let pool_size = env_int("DB_POOL_SIZE", 160) as usize;
    let admission_pool_reserve = env_int("QUEEN_ADMISSION_POOL_RESERVE", 16).max(0) as u64;
    let admission_floor =
        ((pool_size as u64).saturating_sub(admission_pool_reserve) * 2 / 3).max(8) as i64;

    // --- kv + timers derivations (PLAN_KV_TIMERS.md §8.4, §9.2, §9.4) ------
    // Resolved BEFORE the struct because three of them are DERIVED from another
    // knob, and a derived default that is written as a constant is a default that
    // silently stops tracking what it was derived from.
    let tenancy_header = env_bool("QUEEN_TENANCY_HEADER", false);
    // §8.4: derived from the pool rather than hardcoded, same precedent as
    // `admission_floor`. With DB_POOL_SIZE=160 the KV pool is 16 — exactly the
    // admission reserve the product already considers acceptable to hold idle.
    let kv_pool_size = env_int("QUEEN_KV_POOL_SIZE", kv_pool_default(pool_size)).max(1) as usize;
    // §9.4 correction 4. `tenant.rs` is explicit that with the header on, the tenant
    // is OPAQUE and validated against nothing — the trust is the cell network. With
    // KV on top of that, a client rotating the header per request gets a fresh key
    // space, a fresh (unlimited) quota lookup and a permanent entry in the broker's
    // maps, on a deployment the design itself calls real (self-hosted / dedicated
    // with no proxy). So the combination requires the operator to STATE that a proxy
    // sets the header, and the boot dies otherwise: a safety interlock must fail
    // closed, and `env_bool` already treats a typo as fatal for the same reason.
    // The unsafe thing here is NOT the KV surface, it is an opaque tenant identity
    // that nothing validates. KV only made it visible, because it is the first
    // surface addressable purely BY NAME.
    //
    // So the interlock is KEYED ON THE TENANCY MODE ALONE. It used to also test the
    // KV flag; with that flag gone the requirement is simply unconditional for anyone
    // running with the header, which is the honest shape it always had: choosing
    // QUEEN_TENANCY_HEADER=1 IS the assertion that a proxy in front sets the header
    // and strips the client's, and a deployment that cannot assert it is not a
    // deployment missing a feature, it is a deployment with an open door.
    //
    // FATAL, and there is no third option to offer. There is no longer a flag that
    // could switch KV off to make this safe — and there should not be: "the engine is
    // missing on some cells" is worse than a boot that tells you which env to set. If
    // an operator genuinely has to take the KV surface down on a live cell, that is
    // the RUNTIME kill switch (`switches.rs`, `kv_enabled` in `queen.system_state`,
    // POST /api/v1/system/kv-timers), which is a different instrument for a different
    // situation and does not make an unvalidated tenant header safe anyway.
    if tenancy_header && !env_bool("QUEEN_KV_TRUSTED_PROXY", false) {
        crate::obs::fatal(
            "QUEEN_TENANCY_HEADER=1 without QUEEN_KV_TRUSTED_PROXY=1: the tenant header \
             is opaque and validated against nothing, so any caller could read and write \
             another tenant's KV state BY NAME. Set QUEEN_KV_TRUSTED_PROXY=1 to affirm \
             that a proxy in front sets x-queen-tenant and strips the client's. If you \
             cannot affirm that, do not run with QUEEN_TENANCY_HEADER=1 — the KV surface \
             is part of the engine and cannot be switched off at boot to make an \
             unvalidated tenant identity safe (an operator pausing KV on a running cell \
             wants the runtime kill switch: POST /api/v1/system/kv-timers)",
        );
    }

    // --- Postgres TLS trust (spec §0: PG_SSL_ROOT_CERT) --------------------
    // Malformed CA material is FATAL here, next to the other boot gates. It is
    // never a fall-back to the compiled-in Mozilla set: falling back would be a
    // downgrade with no signal, which is precisely what the default of
    // PG_SSL_REJECT_UNAUTHORIZED=true exists to prevent.
    let pg_use_ssl = env_bool("PG_USE_SSL", false);
    let pg_ssl_reject_unauthorized = env_bool("PG_SSL_REJECT_UNAUTHORIZED", true);
    let pg_ssl_root_cert = pg_ssl_root_cert();
    let ca_anchors = match check_pg_ssl_root_cert() {
        Ok(n) => n,
        Err(e) => crate::obs::fatal(e),
    };
    // The legal-but-probably-wrong combinations, worded once in pgtls.rs and
    // used identically by the proxy against PXDB_*.
    if let Some(msg) = crate::pgtls::boot_advisory(
        pg_use_ssl,
        pg_ssl_root_cert.is_some(),
        pg_ssl_reject_unauthorized,
        crate::pgtls::BROKER_VARS,
    ) {
        tracing::warn!(target: "boot", anchors = ca_anchors, "{msg}");
    }

    let mut cfg = Config {
        port: env_str("PORT", "6632"),
        bind_addr: http_bind_addr(),
        pg,
        pool_size,
        pg_use_ssl,
        pg_ssl_reject_unauthorized,
        pg_ssl_root_cert,
        stmt_timeout: Duration::from_millis(env_int("QUEEN_STMT_TIMEOUT_MS", 30000) as u64),
        zstd_level: env_int("QUEEN_V2_ZSTD_LEVEL", 3) as i32,
        admission_init: env_int("QUEEN_ADMISSION_INIT", admission_floor).max(1) as u64,
        admission_min: env_int("QUEEN_ADMISSION_MIN", admission_floor).max(1) as u64,
        admission_max: env_int("QUEEN_ADMISSION_MAX", 128).max(1) as u64,
        admission_pool_reserve,
        admission_train_gap_us: env_int("QUEEN_ADMISSION_TRAIN_GAP_US", 300).max(50) as u64,
        admission_tick_ms: env_int("QUEEN_ADMISSION_TICK_MS", 500).max(100) as u64,
        admission_share_push: env_f64("QUEEN_ADMISSION_SHARE_PUSH", 0.25),
        admission_share_pop: env_f64("QUEEN_ADMISSION_SHARE_POP", 0.40),
        admission_share_ack: env_f64("QUEEN_ADMISSION_SHARE_ACK", 0.30),
        admission_share_maint: env_f64("QUEEN_ADMISSION_SHARE_MAINT", 0.05),
        admission_nosync_budget: env_int("QUEEN_ADMISSION_NOSYNC_BUDGET", 64).max(1) as u64,
        admission_trace: env_bool("QUEEN_ADMISSION_TRACE", false),
        // RUSTFIX item 19: honor DEFAULT_TIMEOUT (C++ name) first, then
        // POP_DEFAULT_TIMEOUT_MS, then 30000 — not the old 2000.
        pop_default_timeout_ms: env_int("DEFAULT_TIMEOUT", env_int("POP_DEFAULT_TIMEOUT_MS", 30000))
            .max(1) as u64,
        // Subscription mode applied when a pop names a consumer group but does NOT
        // send subscriptionMode. `new` = the group starts at the tail; `all` = it
        // replays the whole retained backlog.
        //
        // Two things happened here. The C++ broker honored DEFAULT_SUBSCRIPTION_MODE
        // and the shipped charts set it to `new`; the Rust port dropped the env var
        // (this file had no "subscription" in it at all) AND hardcoded the
        // per-request default to `all`. So a chart that still says `new` was
        // silently serving `all`, and the first group created after traffic resumed
        // replayed from the start. This restores the env var — same treatment as
        // DEFAULT_TIMEOUT above — and makes `new` the default it always claimed to
        // be. Set DEFAULT_SUBSCRIPTION_MODE=all to get the old Rust behavior back
        // without a rebuild.
        default_subscription_mode: normalize_subscription_mode(&env_str(
            "DEFAULT_SUBSCRIPTION_MODE",
            "new",
        )),
        pop_pending_gate: env_bool("QUEEN_POP_PENDING_GATE", true),
        pop_wait_initial_interval_ms: env_int("POP_WAIT_INITIAL_INTERVAL_MS", 100).max(1) as u64,
        pop_wait_backoff_threshold: env_int("POP_WAIT_BACKOFF_THRESHOLD", 3).max(0) as u32,
        pop_wait_backoff_multiplier: env_f64("POP_WAIT_BACKOFF_MULTIPLIER", 2.0),
        pop_wait_max_interval_ms: env_int("POP_WAIT_MAX_INTERVAL_MS", 1000).max(1) as u64,
        fusion_shards: env_int("QUEEN_V2_FUSION_SHARDS", 8).max(1) as usize,
        fusion_frames: env_int("QUEEN_V2_FUSION_FRAMES", 500).max(1) as usize,
        // 15 -> 3 (campaign 2026-08-03/04). The hold is paid TWICE per flow —
        // once on ingress, once on the derived republish — so at 15 ms it was
        // ~30 of the ~95 ms of broker-side latency on the anchor shape. Cutting
        // it took 170 -> 142.9 ms p50 there. NOTE the sign flips with regime:
        // measured at 96 workers with the ring backed up, hold=2 was WORSE
        // (340 ms), because thinner batches cost more WAL contention than the
        // hold costs latency. It only pays once the ring drains. 3 is the
        // measured plateau (2 and 3 tie on p50, 3 has the better p99).
        fusion_hold_ms: env_int("QUEEN_V2_FUSION_HOLD_MS", 3).max(1) as u64,
        // Segments/log-engine sweep cadence (docs 17-18): retention runs as
        // frequent, incremental, bounded steps (advisory-locked, p_max_rows per
        // call) instead of the old C++ 5-minute monolith — a 5s default keeps
        // each step tiny so sweeps never stall pushes with long row locks.
        retention_interval_ms: env_int("RETENTION_INTERVAL", 5000).max(1) as u64,
        dedup_cache_mb: env_int("QUEEN_DEDUP_CACHE_MB", 512).max(1) as usize,
        // Kill switch semantics (doc 18 §5): QUEEN_DEDUP_CACHE=0/false/no/off
        // disables, unset enables. Correctness never depends on the cache.
        dedup_cache_enabled: env_bool("QUEEN_DEDUP_CACHE", true),
        ack_registry_mb: env_int("QUEEN_ACK_REGISTRY_MB", 64).max(1) as usize,
        ack_registry_enabled: env_bool("QUEEN_ACK_REGISTRY", true),
        // ACK FUSION default ON (operator decision 2026-07-24 after the VM A/B:
        // rows_per_commit 104-107 under fsync backpressure, push_multi 38→21ms).
        // "0"/"false" disables — the kill switch mirrors the QUEEN_HOTLIST parse.
        ack_fusion_enabled: env_bool("QUEEN_ACK_FUSION", true),
        ack_fusion_shards: env_int("QUEEN_ACK_FUSION_SHARDS", env_int("QUEEN_V2_FUSION_SHARDS", 8))
            .max(1) as usize,
        ack_fusion_hold_ms: env_int("QUEEN_ACK_FUSION_HOLD_MS", 3).max(1) as u64,
        // POP FUSION (server/src/pop_fusion.rs): N pop claim legs share one
        // transaction/commit. Default OFF for the A/B; the win is the sparse-
        // partition regime where pop commits dominate the WAL (2026-08-02).
        pop_fusion_enabled: env_bool("QUEEN_POP_FUSION", false),
        pop_fusion_shards: env_int("QUEEN_POP_FUSION_SHARDS", 4).max(1) as usize,
        pop_fusion_hold_ms: env_int("QUEEN_POP_FUSION_HOLD_MS", 3).max(1) as u64,
        pop_fusion_max_jobs: env_int("QUEEN_POP_FUSION_MAX_JOBS", 16).max(1) as usize,
        pop_fusion_max_inflight: env_int("QUEEN_POP_FUSION_CONCURRENCY", 1).max(1) as u32,
        retention_batch_size: env_int("RETENTION_BATCH_SIZE", 1000).max(1) as usize,
        retention_parallelism: env_int("RETENTION_PARALLELISM", 1).max(1) as usize,
        // 0 = derive from the two knobs above; resolved right after the struct
        // is built so the boot line and every reader see the number in force.
        // Range-CHECKED below, not clamped, for the reason the autopilot batch
        // is: a due cap nobody chose is how a fleet ends up quietly falling
        // behind on retention again.
        retention_due_cap: env_int("QUEEN_RETENTION_DUE_CAP", 0).max(0) as usize,
        // Range-checked below too. 0 is a legal value here and means "no
        // recurring walk" — NOT "no walk at all" (retention.rs).
        retention_safety_walk_ms: env_int("QUEEN_RETENTION_SAFETY_WALK_MS", 86_400_000).max(0)
            as u64,
        metrics_retention_days: env_int("METRICS_RETENTION_DAYS", 90).max(1) as i32,
        partition_cleanup_days: env_int("PARTITION_CLEANUP_DAYS", 30).max(1) as i32,
        partition_cleanup_enabled: env_bool("QUEEN_PARTITION_CLEANUP_ENABLED", true),
        stats_interval_ms: env_int("STATS_INTERVAL_MS", 10000).max(1000) as u64,
        retained_bytes_interval_ms: env_int("RETAINED_BYTES_INTERVAL_MS", 600000).max(1000)
            as u64,
        metrics_flush_ms: env_int("METRICS_FLUSH_MS", 60000).max(1000) as u64,
        auth: AuthConfig::from_env(),
        sync: SyncConfig::from_env(),
        file_buffer: FileBufferConfig::from_env(),
        kafka_facade: KafkaFacadeConfig::from_env(),
        // HOT-LIST default ON (operator decision 2026-07-24 after the VM A/B:
        // candidate scans gone from the profile, ingress lag flat, combo with
        // ack fusion beats the scan path on total delivered even on a slow
        // disk). "0"/"false" disables — the legacy SQL candidate-scan path
        // stays intact behind the kill switch.
        hotlist_enabled: env_bool("QUEEN_HOTLIST", true),
        hotlist_shards: env_int("QUEEN_HOTLIST_SHARDS", env_int("QUEEN_V2_FUSION_SHARDS", 8))
            .max(1) as usize,
        hotlist_window_batch: env_int("QUEEN_HOTLIST_WINDOW_BATCH", 100).max(1) as u32,
        hotlist_reseed_ms: env_int("QUEEN_HOTLIST_RESEED_MS", 30000).max(1),
        hotlist_reseed_full_ms: env_int("QUEEN_HOTLIST_RESEED_FULL_MS", 300_000).max(0),
        // 0 = derive. Resolved to a concrete value right after the struct is built,
        // so the boot line and every reader see the number actually in force.
        hotlist_reseed_window_ms: env_int("QUEEN_HOTLIST_RESEED_WINDOW_MS", 0).max(0),
        hotlist_idle_sweep_ms: env_int("QUEEN_HOTLIST_IDLE_SWEEP_MS", 300_000).max(0) as u64,
        hotlist_unserved_trim_ms: env_int("QUEEN_HOTLIST_UNSERVED_TRIM_MS", 30_000).max(0),
        // POP AUTOPILOT (server/src/pop_autopilot.rs). The mode string is
        // validated below, next to the other enumerated knobs — an unrecognised
        // value is fatal rather than silently resolving to the default, the same
        // rule `env_bool` applies to every boolean in the broker.
        pop_autopilot_mode: env_str("QUEEN_POP_AUTOPILOT", "on"),
        // Keep the literal in step with `pop_autopilot::AUTO_BATCH_DEFAULT` — it
        // is spelled out here because the generated environment reference reads
        // the default off this call site. Range-checked below, not clamped: a
        // batch knob that silently corrects itself is how a fat-fingered value
        // reaches production unnoticed.
        pop_autopilot_batch: env_int("QUEEN_POP_AUTOPILOT_BATCH", 100) as i32,
        pop_autopilot_target_age_ms: env_f64("QUEEN_POP_AUTOPILOT_TARGET_AGE_MS", 25.0).max(1.0),
        pop_autopilot_dwell_ms: env_int("QUEEN_POP_AUTOPILOT_DWELL_MS", 500).max(1),
        pop_autopilot_dwell_pops: env_int("QUEEN_POP_AUTOPILOT_DWELL_POPS", 16).max(1) as u32,
        // 0 = the bypass is off and the plan is byte-identical to the feedback
        // loop's own answer. Range-checked below like `_BATCH`, never clamped: a
        // ceiling that silently corrects itself is how a value nobody chose reaches
        // production.
        pop_autopilot_burst_cap: env_int("QUEEN_POP_AUTOPILOT_BURST_CAP", 0) as i32,
        pop_autopilot_max_lanes: env_int("QUEEN_POP_AUTOPILOT_MAX_LANES", 50_000).max(0) as usize,
        log_rates_ms: env_int("QUEEN_LOG_RATES_MS", 10000).max(1000) as u64,
        log_top_n_queues: env_int("QUEEN_LOG_TOPN_QUEUES", 10).max(1) as usize,
        tenancy_header,
        // ------------------------------------------- kv + timers (PLAN_KV_TIMERS.md)
        kv_max_value_bytes: env_int("QUEEN_KV_MAX_VALUE_BYTES", 65536).max(1) as usize,
        kv_max_key_bytes: env_int("QUEEN_KV_MAX_KEY_BYTES", 512).max(1) as usize,
        kv_max_ops_per_call: env_int("QUEEN_KV_MAX_OPS_PER_CALL", 256).max(1) as usize,
        kv_max_keys_per_call: env_int("QUEEN_KV_MAX_KEYS_PER_CALL", 1024).max(1) as usize,
        kv_max_read_bytes: env_int("QUEEN_KV_MAX_READ_BYTES", 4 * 1024 * 1024).max(1) as usize,
        kv_prefix_limit: env_int("QUEEN_KV_PREFIX_LIMIT", 1000).max(1) as usize,
        kv_pool_size,
        kv_require_grant: env_bool("QUEEN_KV_REQUIRE_GRANT", tenancy_header),
        kv_read_rate: env_int("QUEEN_KV_READ_RATE", 200).max(1) as u32,
        kv_read_burst: env_int("QUEEN_KV_READ_BURST", 400).max(1) as u32,
        kv_write_rate: env_int("QUEEN_KV_WRITE_RATE", 100).max(1) as u32,
        kv_write_burst: env_int("QUEEN_KV_WRITE_BURST", 200).max(1) as u32,
        // 0 = no aggregate cell ceiling. The default is ten times the per-tenant read
        // rate: it is a ceiling on the SUM of conforming tenants, not a second
        // per-tenant limit, so it must sit well above one tenant's share.
        kv_cell_rate: env_int("QUEEN_KV_CELL_RATE", 2000).max(0) as u32,
        kv_max_tenants: env_int("QUEEN_KV_MAX_TENANTS", 10_000).max(1) as usize,
        kv_usage_every_ms: env_int("QUEEN_KV_USAGE_EVERY_MS", 300_000).max(1000) as u64,
        kv_quota_refresh_ms: env_int("QUEEN_KV_QUOTA_REFRESH_MS", 30_000).max(1000) as u64,
        // Clamped to 1..=99: 100 would mean "release exactly at the cap", i.e. no
        // band and the oscillation the band exists to remove; 0 would mean a tenant
        // that ever blocked can only be released by emptying itself completely.
        kv_quota_release_percent: env_int("QUEEN_KV_QUOTA_RELEASE_PERCENT", 90).clamp(1, 99),
        kv_quota_hot_percent: env_int("QUEEN_KV_QUOTA_HOT_PERCENT", 80).clamp(1, 100),
        kv_standalone_shed_after: env_int("QUEEN_KV_STANDALONE_SHED_AFTER", 5).max(1) as u32,
        timers_max_payload_bytes: env_int("QUEEN_TIMERS_MAX_PAYLOAD_BYTES", 1024 * 1024)
            .max(1) as usize,
        timers_max_horizon_s: env_int("QUEEN_TIMERS_MAX_HORIZON_S", 7_776_000).max(1),
        timers_max_ops_per_call: env_int("QUEEN_TIMERS_MAX_OPS_PER_CALL", 256).max(1) as usize,
        // ------------------------------------- ephemeral (EPHEMERAL_QUEUES.md §3.8)
        // `.max(1)` and not `.max(0)` on the three budgets: 0 in this engine means
        // UNLIMITED, and an operator who types `QUEEN_EPHEMERAL_MAX_BYTES=0`
        // meaning "off" would get a cell with no memory ceiling at all. There is
        // no way to spell "off" here on purpose (no boot gate — M9), so the floor
        // makes the typo harmless instead of catastrophic.
        ephemeral_max_bytes: env_int("QUEEN_EPHEMERAL_MAX_BYTES", 256 * 1024 * 1024).max(1),
        ephemeral_queue_max_bytes: env_int("QUEEN_EPHEMERAL_QUEUE_MAX_BYTES", 16 * 1024 * 1024)
            .max(1),
        ephemeral_queue_max_length: env_int("QUEEN_EPHEMERAL_QUEUE_MAX_LENGTH", 10_000).max(1),
        // Clamped to a day: a lease longer than that is indistinguishable from a
        // lost message on a class whose contents do not survive a deploy.
        ephemeral_lease_s: env_int("QUEEN_EPHEMERAL_LEASE_S", 30).clamp(1, 86_400),
        ephemeral_retry_limit: env_int("QUEEN_EPHEMERAL_RETRY_LIMIT", 5).clamp(1, 1000) as u32,
        // 0 IS legal here and means "never collect implicit queues" — unlike the
        // budgets, an operator who wants that is asking for something coherent.
        ephemeral_implicit_idle_s: env_int("QUEEN_EPHEMERAL_IMPLICIT_IDLE_S", 300).max(0),
        // DERIVED from the tenancy flag, exactly like `kv_require_grant` above: a
        // self-hosted operator never has to discover why they should configure
        // something, and a cloud cell is off until the plan grants it.
        ephemeral_require_grant: env_bool("QUEEN_EPHEMERAL_REQUIRE_GRANT", tenancy_header),
        ephemeral_rate: env_int("QUEEN_EPHEMERAL_RATE", 5000).max(1) as u32,
        ephemeral_burst: env_int("QUEEN_EPHEMERAL_BURST", 10_000).max(1) as u32,
        sweeper_enabled: env_bool("QUEEN_SWEEPER", true),
        sweeper_min_sleep_ms: env_int("QUEEN_SWEEPER_MIN_SLEEP_MS", 5).max(1) as u64,
        sweeper_max_sleep_ms: env_int("QUEEN_SWEEPER_MAX_SLEEP_MS", 1000).max(1) as u64,
        sweeper_idle_max_sleep_ms: env_int("QUEEN_SWEEPER_IDLE_MAX_SLEEP_MS", 30_000).max(1)
            as u64,
        sweeper_lease_ms: env_int("QUEEN_SWEEPER_LEASE_MS", 30_000).max(1) as u64,
        sweeper_claim_batch: env_int("QUEEN_SWEEPER_CLAIM_BATCH", 200).max(1),
        sweeper_cycle_max_rows: env_int("QUEEN_SWEEPER_CYCLE_MAX_ROWS", 5000).max(1),
        sweeper_due_cap: env_int("QUEEN_SWEEPER_DUE_CAP", 2000).max(1),
        sweeper_max_fire_bytes: env_int("QUEEN_SWEEPER_MAX_FIRE_BYTES", 8 * 1024 * 1024)
            .max(1) as usize,
        sweeper_max_attempts: env_int("QUEEN_SWEEPER_MAX_ATTEMPTS", 5).max(1) as i32,
        // .max(1) only — the ceiling is sweeper::MAX_PARALLELISM, applied at the call
        // site exactly as retention.rs applies its own. One clamp, one owner.
        sweeper_parallelism: env_int("QUEEN_SWEEPER_PARALLELISM", 1).max(1) as usize,
        sweeper_isolate_on_permanent: env_bool("QUEEN_SWEEPER_ISOLATE_ON_PERMANENT", true),
        sweeper_fire_rate_per_tenant: env_int("QUEEN_SWEEPER_FIRE_RATE_PER_TENANT", 50).max(1),
        sweeper_backoff_min_ms: env_int("QUEEN_SWEEPER_BACKOFF_MIN_MS", 1000).max(1),
        sweeper_backoff_max_ms: env_int("QUEEN_SWEEPER_BACKOFF_MAX_MS", 60_000).max(1),
    };
    // §8 windowed reseed: derive it when unset, then clamp it — see
    // resolve_reseed_window_ms. Resolved here, before anything reads the field, so the
    // boot line and every consumer see the number actually in force.
    // POP AUTOPILOT: reject a mis-spelled kill switch at boot instead of resolving
    // it to the default. `QUEEN_POP_AUTOPILOT=shaddow` silently meaning `on` is the
    // exact failure `env_bool` was rewritten to stop (a knob that reads as set and
    // behaves as unset, with not a word in the log).
    if crate::pop_autopilot::Mode::parse(&cfg.pop_autopilot_mode).is_none() {
        crate::obs::fatal(format!(
            "QUEEN_POP_AUTOPILOT=\"{}\" is not a mode (expected on/shadow/off, or the \
             boolean spellings {BOOL_SPELLINGS} for on/off)",
            cfg.pop_autopilot_mode
        ));
    }
    // Same rule for the delegated batch, and for the same reason the boolean
    // parser stopped being permissive: `env_int` resolves an unparseable value to
    // the default silently, so a typo would run the fleet on a number nobody
    // chose, with not a word in the log. Reject rather than clamp — an operator
    // who asked for a batch of a million has a misunderstanding a clamp would
    // hide.
    {
        use crate::pop_autopilot::{AUTO_BATCH_MAX, AUTO_BATCH_MIN};
        let raw = std::env::var("QUEEN_POP_AUTOPILOT_BATCH").ok();
        let set = raw.as_deref().map(str::trim).filter(|v| !v.is_empty());
        let parsed = set.map(|v| v.parse::<i32>());
        match parsed {
            Some(Err(_)) => crate::obs::fatal(format!(
                "QUEEN_POP_AUTOPILOT_BATCH=\"{}\" is not an integer (expected \
                 {AUTO_BATCH_MIN}..={AUTO_BATCH_MAX})",
                set.unwrap_or_default()
            )),
            Some(Ok(v)) if !(AUTO_BATCH_MIN..=AUTO_BATCH_MAX).contains(&v) => {
                crate::obs::fatal(format!(
                    "QUEEN_POP_AUTOPILOT_BATCH={v} is outside {AUTO_BATCH_MIN}..={AUTO_BATCH_MAX}"
                ))
            }
            _ => {}
        }
    }
    // Same rule, same reason, for the burst-bypass ceiling.
    if let Err(e) = burst_cap_verdict(
        std::env::var("QUEEN_POP_AUTOPILOT_BURST_CAP")
            .ok()
            .as_deref(),
    ) {
        crate::obs::fatal(e);
    }
    // RETENTION WORK LIST (retention.rs / 001_log_schema): same rule again for
    // the two knobs that govern how much of the due list a cycle takes and how
    // often the watermarks are re-derived from reality. Both are integers whose
    // wrong value is INVISIBLE in operation — a due cap of "1O" (letter O)
    // silently means the default, and a safety walk of "1d" silently means
    // daily-by-accident — so an unparseable value is fatal rather than
    // defaulted.
    for (key, max) in [
        ("QUEEN_RETENTION_DUE_CAP", crate::retention::DUE_CAP_MAX as i64),
        ("QUEEN_RETENTION_SAFETY_WALK_MS", crate::retention::SAFETY_WALK_MAX_MS as i64),
    ] {
        let raw = std::env::var(key).ok();
        if let Err(e) = nonneg_env_verdict(key, raw.as_deref(), max) {
            crate::obs::fatal(e);
        }
    }
    // 0 = derive. batch_size x parallelism x DUE_CAP_FACTOR: the batch size is
    // the operator's declared appetite for maintenance work per step call, and
    // the parallelism is how many of those calls a cycle can actually execute
    // in its period, so the product is the honest budget. At the defaults
    // (1000 x 1 x 5) that is 5000 due partitions per queue per rule per cycle,
    // ~2x the ~2500 visits the measured cell needs to keep pace with ~500
    // newly-eligible segments/s at a 5 s cadence.
    if cfg.retention_due_cap == 0 {
        cfg.retention_due_cap = cfg
            .retention_batch_size
            .saturating_mul(cfg.retention_parallelism)
            .saturating_mul(crate::retention::DUE_CAP_FACTOR)
            .clamp(1, crate::retention::DUE_CAP_MAX);
    }
    if cfg.retention_safety_walk_ms == 0 {
        // Not a lie about "0 disables": the RECURRING walk is what stops. The
        // first one still has to run or the work list is empty and retention
        // stops dead with nothing in the log to say so, so the schedule is
        // pushed a year out rather than switched off (retention.rs).
        tracing::warn!(target: "boot",
            "QUEEN_RETENTION_SAFETY_WALK_MS=0 disables the RECURRING retention watermark \
             walk; the one-time backfill still runs (without it the retention work list \
             is empty and nothing is ever deleted), and drift introduced later will not \
             self-heal until the knob is set again");
    }
    let requested_window_ms = cfg.hotlist_reseed_window_ms;
    cfg.hotlist_reseed_window_ms =
        resolve_reseed_window_ms(requested_window_ms, cfg.hotlist_reseed_ms);
    // Only an OPERATOR's value is worth a warning; the derived one is ours and is
    // inside the band by construction.
    if requested_window_ms != 0 && cfg.hotlist_reseed_window_ms != requested_window_ms {
        if cfg.hotlist_reseed_window_ms > requested_window_ms {
            tracing::warn!(target: "boot",
                requested_ms = requested_window_ms,
                in_force_ms = cfg.hotlist_reseed_window_ms,
                reseed_ms = cfg.hotlist_reseed_ms,
                "QUEEN_HOTLIST_RESEED_WINDOW_MS is not wider than the gap two \
                 consecutive reseeds can leave (QUEEN_HOTLIST_RESEED_MS + its \
                 de-phasing jitter), which would leave a band of writes no pass ever \
                 covers; RAISED to {} ms",
                cfg.hotlist_reseed_window_ms);
        } else {
            tracing::warn!(target: "boot",
                requested_ms = requested_window_ms,
                in_force_ms = cfg.hotlist_reseed_window_ms,
                "QUEEN_HOTLIST_RESEED_WINDOW_MS exceeds the ceiling this lookback is \
                 arithmetically safe at; LOWERED to {} ms — to reseed over everything, \
                 set QUEEN_HOTLIST_RESEED_FULL_MS=0 instead",
                cfg.hotlist_reseed_window_ms);
        }
    }
    cfg
}

impl Config {
    /// POP AUTOPILOT knobs, resolved once. ONE owner for the mode parse, so the
    /// two boot paths (`main.rs` and `embedded/boot.rs`) cannot disagree about
    /// what `QUEEN_POP_AUTOPILOT` said. `load()` has already refused an
    /// unrecognised value, so the fallback here is unreachable in a booted broker
    /// and exists only for a `Config` assembled by hand in a test.
    pub fn pop_autopilot_knobs(&self) -> crate::pop_autopilot::Knobs {
        crate::pop_autopilot::Knobs {
            mode: crate::pop_autopilot::Mode::parse(&self.pop_autopilot_mode)
                .unwrap_or(crate::pop_autopilot::Knobs::defaults().mode),
            // Clamped here and range-CHECKED in `load()`: the check is what an
            // operator sees, this is what keeps a hand-assembled `Config` in a
            // test from handing the claim path a zero.
            auto_batch: self
                .pop_autopilot_batch
                .clamp(crate::pop_autopilot::AUTO_BATCH_MIN, crate::pop_autopilot::AUTO_BATCH_MAX),
            target_age_ms: self.pop_autopilot_target_age_ms,
            dwell_ms: self.pop_autopilot_dwell_ms,
            dwell_pops: self.pop_autopilot_dwell_pops,
            // Clamped for the same reason `auto_batch` is: `load()` has already
            // refused an out-of-range value, so this only keeps a hand-assembled
            // test `Config` from handing the claim path a ceiling above the checkout
            // limit. 0 stays 0, which is the bypass disabled.
            burst_cap: self.pop_autopilot_burst_cap.clamp(
                crate::pop_autopilot::BURST_CAP_MIN,
                crate::pop_autopilot::BURST_CAP_MAX,
            ),
            max_lanes: self.pop_autopilot_max_lanes,
        }
    }
}

/// The widest windowed-reseed lookback that is still a lookback (C1,
/// PLAN_HOTLIST_FOLLOWUP.md). One week is already ~5 orders of magnitude short of where
/// `now() - make_interval(secs => …)` in log_hotlist_reseed_window_v1 walks off the
/// bottom of `timestamptz`, and it is a guard rail rather than a tuning recommendation:
/// the way to reseed over EVERYTHING is QUEEN_HOTLIST_RESEED_FULL_MS=0, which runs the
/// full walk — the query written for that question — instead of a window so wide it
/// scans the whole partition set through the index built for a narrow one.
const RESEED_WINDOW_CEILING_MS: i64 = 7 * 24 * 3_600_000;

/// The windowed reseed's lookback that is actually in force, from the raw knob
/// (`0` = derive) and the reseed cadence.
///
/// Derivation: four intervals of slack so the window survives a few skipped or failed
/// passes, floored at 120s because the pop candidate scan already treats 2 minutes of
/// `last_write_at` staleness as absorbed (001_log_schema.sql) and the push path
/// quantizes that column to 1s.
///
/// The clamp then applies to BOTH the derived and the operator's value (C1). The floor
/// is the gap two consecutive passes of one ring can leave — the cadence plus the widest
/// de-phasing offset that cadence draws — because a window narrower than that is a
/// permanent blind band: a partition written after one pass read past it and more than
/// `window` before the next pass starts falls in NEITHER pass's window, and only the full
/// walk (5 minutes by default) ever comes back for it. That is the exact invariant the
/// window exists to preserve, and the derivation upheld it while an explicit value was
/// free to break it. The floor is the MINIMUM that is sound, not a recommendation: the 2s
/// tick the floor is polled on, the walk's own duration and any broker/database clock
/// skew all sit outside it, and are what the derivation's 4x margin is really for.
///
/// The ceiling wins over the floor when a very long cadence puts the two in conflict:
/// staying inside the arithmetic is not negotiable, and a cadence that slow has already
/// chosen its own repair latency.
/// The dedicated KV pool's default size, DERIVED from the main pool
/// (PLAN_KV_TIMERS.md §8.4) rather than hardcoded — the same precedent as
/// `admission_floor`.
///
/// A tenth of the pool, floored at 4 and capped at 32. With the default
/// `DB_POOL_SIZE = 160` that is 16, exactly the admission reserve the product
/// already accepts holding idle. The floor keeps a small deployment usable (a pool
/// of 8 would otherwise derive 0 and wedge every KV call); the cap is what makes the
/// pool a real bulkhead — an unbounded share would let a slow database turn KV
/// traffic into starvation of the message path instead of into 503s.
fn kv_pool_default(pool_size: usize) -> i64 {
    ((pool_size / 10) as i64).clamp(4, 32)
}

fn resolve_reseed_window_ms(requested: i64, reseed_ms: i64) -> i64 {
    let want = if requested == 0 {
        reseed_ms.saturating_mul(4).max(120_000)
    } else {
        requested
    };
    let floor = reseed_ms.saturating_add(crate::hotlist::max_reseed_jitter_ms(reseed_ms));
    want.max(floor).min(RESEED_WINDOW_CEILING_MS)
}

/// The boot verdict on `QUEEN_POP_AUTOPILOT_BURST_CAP` — the same rule the `_BATCH`
/// block above applies inline (reject a typo rather than resolving it to the
/// default; reject rather than clamp), returned as a VALUE so it is unit-testable.
/// `load()` turns an `Err` into the identical `obs::fatal`, which exits the process
/// and can therefore never be exercised from a test.
///
/// A typo here is worse than elsewhere: the default is 0, so an unparseable value
/// silently resolving to it would leave the bypass OFF on the exact cell an operator
/// just turned it on for, with not a word in the log.
/// Shared verdict for the plain "non-negative integer, 0 has a meaning, there is
/// a sane ceiling" knobs (the two retention work-list knobs). Same posture as
/// `burst_cap_verdict`: unset / present-but-empty keeps the documented default,
/// anything else must parse and be in range or the boot dies naming the value.
fn nonneg_env_verdict(key: &str, raw: Option<&str>, max: i64) -> Result<(), String> {
    let set = raw.map(str::trim).filter(|v| !v.is_empty());
    match set.map(|v| v.parse::<i64>()) {
        Some(Err(_)) => Err(format!(
            "{key}=\"{}\" is not an integer (expected 0..={max})",
            set.unwrap_or_default()
        )),
        Some(Ok(v)) if !(0..=max).contains(&v) => Err(format!("{key}={v} is outside 0..={max}")),
        _ => Ok(()),
    }
}

fn burst_cap_verdict(raw: Option<&str>) -> Result<(), String> {
    use crate::pop_autopilot::{BURST_CAP_MAX, BURST_CAP_MIN};
    let set = raw.map(str::trim).filter(|v| !v.is_empty());
    match set.map(|v| v.parse::<i32>()) {
        Some(Err(_)) => Err(format!(
            "QUEEN_POP_AUTOPILOT_BURST_CAP=\"{}\" is not an integer (expected \
             {BURST_CAP_MIN}..={BURST_CAP_MAX}, 0 = disabled)",
            set.unwrap_or_default()
        )),
        Some(Ok(v)) if !(BURST_CAP_MIN..=BURST_CAP_MAX).contains(&v) => Err(format!(
            "QUEEN_POP_AUTOPILOT_BURST_CAP={v} is outside {BURST_CAP_MIN}..={BURST_CAP_MAX} \
             (0 = disabled)"
        )),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two retention work-list knobs are range-CHECKED at boot for the same
    /// reason every other enumerated knob is: `env_int` resolves an
    /// unparseable value to the DEFAULT silently, so a typo would run the fleet
    /// on a number nobody chose with not a word in the log — and both of these
    /// are invisible in operation (`QUEEN_RETENTION_DUE_CAP=1O` would just look
    /// like a slower-than-expected delete rate).
    #[test]
    fn the_retention_work_list_knobs_are_range_checked() {
        let key = "QUEEN_RETENTION_DUE_CAP";
        let max = crate::retention::DUE_CAP_MAX as i64;
        // Unset and present-but-empty keep the documented default, like every
        // other knob.
        assert_eq!(nonneg_env_verdict(key, None, max), Ok(()));
        assert_eq!(nonneg_env_verdict(key, Some("   "), max), Ok(()));
        // 0 is legal and MEANINGFUL on both knobs (derive / no recurring walk).
        assert_eq!(nonneg_env_verdict(key, Some("0"), max), Ok(()));
        assert_eq!(nonneg_env_verdict(key, Some(" 5000 "), max), Ok(()));
        assert!(nonneg_env_verdict(key, Some("1O"), max).is_err(), "letter O must not pass");
        assert!(nonneg_env_verdict(key, Some("-1"), max).is_err());
        assert!(nonneg_env_verdict(key, Some(&(max + 1).to_string()), max).is_err());
        // The safety-walk ceiling is a year: past it the walk is
        // indistinguishable from off, and off has its own spelling (0).
        let walk_max = crate::retention::SAFETY_WALK_MAX_MS as i64;
        assert_eq!(walk_max, 365 * 24 * 3_600_000);
        assert_eq!(
            nonneg_env_verdict("QUEEN_RETENTION_SAFETY_WALK_MS", Some("86400000"), walk_max),
            Ok(())
        );
    }

    /// The burst-bypass ceiling is range-CHECKED at boot, not clamped: 0 (disabled)
    /// and 64 (the checkout ceiling) are the ends of the accepted interval, and both
    /// a value past it and a value that is not a number at all are fatal.
    #[test]
    fn the_burst_cap_accepts_its_range_and_refuses_everything_else() {
        assert_eq!(crate::pop_autopilot::BURST_CAP_MIN, 0);
        assert_eq!(
            crate::pop_autopilot::BURST_CAP_MAX,
            crate::pop_autopilot::W_CEILING,
            "a cap above the checkout ceiling could not raise any width"
        );
        // Unset and present-but-empty both mean "leave it at the default", the same
        // rule every other knob follows.
        assert_eq!(burst_cap_verdict(None), Ok(()));
        assert_eq!(burst_cap_verdict(Some("  ")), Ok(()));
        for ok in ["0", "1", "64", " 64 "] {
            assert_eq!(burst_cap_verdict(Some(ok)), Ok(()), "{ok:?} is in range");
        }
        for bad in ["65", "-1", "1000"] {
            let err = burst_cap_verdict(Some(bad)).unwrap_err();
            assert!(err.contains("is outside 0..=64"), "{bad:?} -> {err}");
        }
        for bad in ["abc", "64.0", "sixty-four"] {
            let err = burst_cap_verdict(Some(bad)).unwrap_err();
            assert!(err.contains("is not an integer"), "{bad:?} -> {err}");
            assert!(err.contains(bad), "the message quotes the value: {err}");
        }
    }

    #[test]
    fn parses_every_accepted_spelling_case_insensitively() {
        for t in ["true", "TRUE", "True", "1", "yes", "YES", "on", "ON", " true ", "\tOn\n"] {
            assert_eq!(parse_bool(t), Some(true), "{t:?} should parse as true");
        }
        for f in ["false", "FALSE", "False", "0", "no", "NO", "off", "OFF", " false ", "\tOff\n"] {
            assert_eq!(parse_bool(f), Some(false), "{f:?} should parse as false");
        }
    }

    #[test]
    fn rejects_unrecognised_values() {
        // Including the near-misses that a permissive parser would guess at.
        for bad in ["maybe", "y", "n", "t", "f", "2", "-1", "enabled", "disabled", "truthy", "0.0"] {
            assert_eq!(parse_bool(bad), None, "{bad:?} should not parse");
        }
    }

    #[test]
    fn unrecognised_value_is_an_error_not_the_default() {
        // The regression this whole change exists for: a bad value must NOT
        // silently resolve to the default in either direction.
        let err = resolve_bool("JWT_ENABLED", Some("maybe"), false).unwrap_err();
        assert_eq!(
            err,
            "JWT_ENABLED=\"maybe\" is not a boolean (expected true/false, 1/0, yes/no, on/off)"
        );
        // A default of `true` is just as unusable a fallback.
        assert!(resolve_bool("PG_SSL_REJECT_UNAUTHORIZED", Some("sure"), true).is_err());
        // The message names the variable and quotes the offending value verbatim.
        let err = resolve_bool("QUEEN_HOTLIST", Some(" Yep "), true).unwrap_err();
        assert!(err.starts_with("QUEEN_HOTLIST=\" Yep \""), "got: {err}");
    }

    #[test]
    fn unset_and_empty_use_the_default() {
        assert_eq!(resolve_bool("JWT_ENABLED", None, false), Ok(false));
        assert_eq!(resolve_bool("QUEEN_HOTLIST", None, true), Ok(true));
        // Present-but-empty (`JWT_ENABLED=` in a compose/Helm file) means "leave
        // it alone", NOT false — the old parser resolved "" to false, which for a
        // default-true knob like PG_SSL_REJECT_UNAUTHORIZED was a silent downgrade.
        assert_eq!(resolve_bool("JWT_ENABLED", Some(""), false), Ok(false));
        assert_eq!(resolve_bool("PG_SSL_REJECT_UNAUTHORIZED", Some("   "), true), Ok(true));
    }

    #[test]
    fn set_values_win_over_the_default() {
        assert_eq!(resolve_bool("JWT_ENABLED", Some("1"), false), Ok(true));
        assert_eq!(resolve_bool("QUEEN_HOTLIST", Some("off"), true), Ok(false));
    }

    #[test]
    fn env_lookup_round_trips_through_the_same_rules() {
        // Uniquely named so parallel tests can't observe each other's env.
        const K: &str = "QUEEN_TEST_ENV_BOOL_ROUNDTRIP";
        assert_eq!(env_bool_checked(K, true), Ok(true)); // unset -> default
        std::env::set_var(K, "YES");
        assert_eq!(env_bool_checked(K, false), Ok(true));
        std::env::set_var(K, "banana");
        assert!(env_bool_checked(K, false).is_err());
        std::env::remove_var(K);
    }

    /// PLAN_KV_TIMERS.md §8.4. The property that matters is not the arithmetic, it is
    /// that the KV pool is a BULKHEAD: bounded above so KV can never eat the message
    /// path's connections, and bounded below so a small deployment still works.
    #[test]
    fn kv_pool_is_derived_from_the_pool_and_bounded_both_ways() {
        // The documented default deployment.
        assert_eq!(kv_pool_default(160), 16);
        // Floor: a tenth of a tiny pool rounds to zero, which would wedge every KV
        // call on an empty pool instead of serving it.
        assert_eq!(kv_pool_default(8), 4);
        assert_eq!(kv_pool_default(0), 4);
        // Ceiling: a very large pool must not hand KV an unbounded share, or a slow
        // database turns KV traffic into starvation of the message path rather than
        // into 503s on the KV routes.
        assert_eq!(kv_pool_default(4000), 32);
        // Monotone in between, so raising DB_POOL_SIZE never shrinks the KV pool.
        let mut last = 0;
        for p in (0..=4000).step_by(37) {
            let v = kv_pool_default(p);
            assert!(v >= last, "kv pool shrank at pool_size={p}");
            last = v;
        }
    }

    #[test]
    fn secrets_are_masked_never_printed() {
        assert_eq!(mask(""), "<unset>");
        let m = mask("hunter2-super-secret");
        assert!(!m.contains("hunter2"), "mask leaked the secret: {m}");
        assert_eq!(m, "<set:20 chars>");
    }
}

#[cfg(test)]
mod subscription_mode_tests {
    use super::normalize_subscription_mode;

    /// `new-only` is documented as an alias of `new` by the Go SDK constant, the
    /// CLI --from-mode help and the JS README, but the SQL compares the literal
    /// exactly, so it used to reach `= 'new'`, miss, and replay the whole backlog:
    /// the exact opposite of what it advertises.
    #[test]
    fn documented_aliases_of_new_all_resolve_to_new() {
        for raw in ["new", "new-only", "new_only", "newonly", "NEW", " New-Only "] {
            assert_eq!(normalize_subscription_mode(raw), "new", "{raw}");
        }
    }

    /// Anything unrecognized keeps resolving to `all`: that is both the historical
    /// fallthrough and the safe direction. Replaying too much is recoverable;
    /// skipping messages is not.
    #[test]
    fn everything_else_resolves_to_all() {
        for raw in ["all", "ALL", "from_beginning", "earliest", "", "garbage"] {
            assert_eq!(normalize_subscription_mode(raw), "all", "{raw}");
        }
    }

    /// The output must be one of the two spellings the SQL understands, never a
    /// pass-through of the caller's text.
    #[test]
    fn output_is_always_one_of_the_two_sql_literals() {
        for raw in ["new", "all", "whatever", "new-only"] {
            let m = normalize_subscription_mode(raw);
            assert!(m == "new" || m == "all", "{raw} -> {m}");
        }
    }
}

/// C1 (PLAN_HOTLIST_FOLLOWUP.md): the windowed reseed's lookback is only sound above
/// the gap two consecutive passes of one ring can leave, and only arithmetically safe
/// below a ceiling. The derivation upheld both; an explicit value used to bypass them.
#[cfg(test)]
mod reseed_window_tests {
    use super::{resolve_reseed_window_ms, RESEED_WINDOW_CEILING_MS};
    use crate::hotlist::max_reseed_jitter_ms;

    /// The gap the window has to span: one cadence plus the widest de-phasing offset
    /// that cadence can draw.
    fn gap(reseed_ms: i64) -> i64 {
        reseed_ms + max_reseed_jitter_ms(reseed_ms)
    }

    #[test]
    fn a_derived_window_is_four_cadences_with_a_two_minute_floor() {
        // The shipped default: 30s cadence -> 120s, from the 120s floor and from 4x
        // alike. Both halves of the max() still visible.
        assert_eq!(resolve_reseed_window_ms(0, 30_000), 120_000);
        assert_eq!(resolve_reseed_window_ms(0, 1_000), 120_000); // 4x is below the floor
        assert_eq!(resolve_reseed_window_ms(0, 60_000), 240_000); // 4x is above it
    }

    #[test]
    fn a_derived_window_always_clears_the_gap_it_has_to_span() {
        // The property the clamp exists to guarantee, checked across the cadences an
        // operator might plausibly set: the derivation must never be the thing that
        // needs raising.
        for reseed_ms in [1, 100, 1_000, 5_000, 30_000, 75_000, 120_000, 600_000, 3_600_000] {
            let w = resolve_reseed_window_ms(0, reseed_ms);
            assert!(
                w >= gap(reseed_ms),
                "derived window {w} does not span the {} ms gap at cadence {reseed_ms}",
                gap(reseed_ms)
            );
        }
    }

    #[test]
    fn an_explicit_window_below_the_gap_between_two_passes_is_raised() {
        // The foot-gun: 10s of lookback on a 30s cadence leaves ~35s of writes that
        // NEITHER pass covers, and only the full walk (5 min) ever comes back for them.
        assert_eq!(resolve_reseed_window_ms(10_000, 30_000), 45_000);
        assert_eq!(gap(30_000), 45_000);
        // Below the cadence by one millisecond is the same mistake, more quietly.
        assert_eq!(resolve_reseed_window_ms(44_999, 30_000), 45_000);
    }

    #[test]
    fn the_floor_follows_the_jitter_and_not_a_constant() {
        // D1 made the de-phasing span a fifth of a long interval, so on a 5-minute
        // cadence the gap is 360s and a 310s window is short — which a floor written
        // against the old 15s constant would have accepted.
        assert_eq!(gap(300_000), 360_000);
        assert_eq!(resolve_reseed_window_ms(310_000, 300_000), 360_000);
    }

    #[test]
    fn an_explicit_window_wide_enough_is_left_exactly_as_set() {
        // Clamping is a guard rail, not an opinion: anything inside the band survives
        // verbatim, including a very generous one.
        assert_eq!(resolve_reseed_window_ms(45_000, 30_000), 45_000);
        assert_eq!(resolve_reseed_window_ms(600_000, 30_000), 600_000);
        assert_eq!(resolve_reseed_window_ms(RESEED_WINDOW_CEILING_MS, 30_000),
                   RESEED_WINDOW_CEILING_MS);
    }

    #[test]
    fn an_absurd_window_is_lowered_to_the_ceiling_without_overflowing() {
        // i64::MAX ms is ~292 million years: make_interval would take it, `now() -`
        // would not. Nothing here may panic or wrap on the way to saying so.
        assert_eq!(resolve_reseed_window_ms(i64::MAX, 30_000), RESEED_WINDOW_CEILING_MS);
        assert_eq!(resolve_reseed_window_ms(RESEED_WINDOW_CEILING_MS + 1, 30_000),
                   RESEED_WINDOW_CEILING_MS);
    }

    #[test]
    fn the_ceiling_wins_when_a_slow_cadence_puts_the_floor_above_it() {
        // A month-long cadence wants a month-long window; the two clamps disagree and
        // the arithmetic one has to win. The value is still returned, never a panic
        // from a reversed clamp range.
        let month = 30 * 24 * 3_600_000i64;
        assert!(gap(month) > RESEED_WINDOW_CEILING_MS);
        assert_eq!(resolve_reseed_window_ms(0, month), RESEED_WINDOW_CEILING_MS);
        assert_eq!(resolve_reseed_window_ms(1_000, month), RESEED_WINDOW_CEILING_MS);
        // Same for a cadence so long it would overflow the multiplication outright.
        assert_eq!(resolve_reseed_window_ms(0, i64::MAX), RESEED_WINDOW_CEILING_MS);
    }
}
