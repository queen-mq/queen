#pragma once

#include <string>
#include <vector>
#include <cstdlib>
#include <cstring>

namespace queen {

// Helper function to get boolean from environment
inline bool get_env_bool(const char* name, bool default_value) {
    const char* value = std::getenv(name);
    if (!value) return default_value;
    return std::strcmp(value, "true") == 0;
}

// Helper function to get int from environment
inline int get_env_int(const char* name, int default_value) {
    const char* value = std::getenv(name);
    return value ? std::atoi(value) : default_value;
}

// Helper function to get double from environment
inline double get_env_double(const char* name, double default_value) {
    const char* value = std::getenv(name);
    return value ? std::atof(value) : default_value;
}

// Helper function to get string from environment
inline std::string get_env_string(const char* name, const std::string& default_value) {
    const char* value = std::getenv(name);
    return value ? std::string(value) : default_value;
}

struct ServerConfig {
    int port = 6632;
    std::string host = "0.0.0.0";
    std::string worker_id = "cpp-worker-1";
    int num_workers = 10;  // Number of worker threads
    
    static ServerConfig from_env() {
        ServerConfig config;
        config.port = get_env_int("PORT", 6632);
        config.host = get_env_string("HOST", "0.0.0.0");
        config.worker_id = get_env_string("WORKER_ID", "cpp-worker-1");
        config.num_workers = get_env_int("NUM_WORKERS", 10);
        return config;
    }
};

struct DatabaseConfig {
    // Connection settings
    std::string user = "postgres";
    std::string host = "localhost";
    std::string database = "postgres";
    std::string password = "postgres";
    std::string port = "5432";
    std::string schema = "queen";
    
    // SSL configuration
    bool use_ssl = false;
    bool ssl_reject_unauthorized = true;
    
    // Pool configuration
    int pool_size = 150;
    int idle_timeout = 30000;           // 30 seconds
    int connection_timeout = 2000;       // 2 seconds
    int statement_timeout = 30000;       // 30 seconds
    int lock_timeout = 10000;            // 10 seconds

    // Socket-level health checks. These bound how long a TCP connection
    // can sit unresponsive before the kernel raises an error on the FD,
    // so the broker can detect silent network drops (cloud-managed PG
    // maintenance, load-balancer reroutes, hypervisor pause, etc.) without
    // waiting for the OS defaults (~14 min TCP retransmit / ~2 h keepalive).
    // Defaults: kernel notices a black-holed peer within ~30 s.
    int tcp_user_timeout_ms = 30000;     // TCP_USER_TIMEOUT (ms, Linux only)
    int keepalives_idle = 60;            // seconds idle before first probe
    int keepalives_interval = 10;        // seconds between probes
    int keepalives_count = 3;            // probes before declaring dead

    static DatabaseConfig from_env() {
        DatabaseConfig config;
        config.user = get_env_string("PG_USER", "postgres");
        config.host = get_env_string("PG_HOST", "localhost");
        config.database = get_env_string("PG_DB", "postgres");
        config.password = get_env_string("PG_PASSWORD", "postgres");
        config.port = get_env_string("PG_PORT", "5432");
        
        config.use_ssl = get_env_bool("PG_USE_SSL", false);
        config.ssl_reject_unauthorized = get_env_bool("PG_SSL_REJECT_UNAUTHORIZED", true);
        
        config.pool_size = get_env_int("DB_POOL_SIZE", 150);
        config.idle_timeout = get_env_int("DB_IDLE_TIMEOUT", 30000);
        config.connection_timeout = get_env_int("DB_CONNECTION_TIMEOUT", 2000);
        config.statement_timeout = get_env_int("DB_STATEMENT_TIMEOUT", 30000);
        config.lock_timeout = get_env_int("DB_LOCK_TIMEOUT", 10000);

        config.tcp_user_timeout_ms = get_env_int("DB_TCP_USER_TIMEOUT_MS", 30000);
        config.keepalives_idle     = get_env_int("DB_KEEPALIVES_IDLE", 60);
        config.keepalives_interval = get_env_int("DB_KEEPALIVES_INTERVAL", 10);
        config.keepalives_count    = get_env_int("DB_KEEPALIVES_COUNT", 3);

        return config;
    }
    
    std::string connection_string() const {
        std::string conn_str = "host=" + host + " port=" + port + " dbname=" + database + 
                               " user=" + user + " password=" + password;
        
        // Add SSL configuration
        if (use_ssl) {
            conn_str += " sslmode=require";
            if (!ssl_reject_unauthorized) {
                // Allow self-signed certificates
                conn_str += " sslmode=prefer";
            }
        } else {
            conn_str += " sslmode=disable";
        }
        
        // CRITICAL: Add connect_timeout (in seconds) for initial connection
        // This is the only timeout that can safely be in connection string
        // (works with both direct PostgreSQL and PgBouncer)
        conn_str += " connect_timeout=" + std::to_string(connection_timeout / 1000);

        // Socket-level keepalive + TCP_USER_TIMEOUT. Without these, libpq
        // inherits OS defaults (Linux: ~2 h before the first keepalive probe,
        // ~14 min before tcp_retries2 gives up on retransmits). That window
        // is longer than any cloud-managed maintenance pause, so the broker
        // would never observe the connection going bad — exactly the failure
        // mode hit during Cloud SQL maintenance/migration. With the defaults
        // here, the kernel raises an error on the socket within ~30 s of a
        // silent black-hole, which surfaces to libqueen as a poll error and
        // to AsyncDbPool as a failed health check. tcp_user_timeout is a
        // Linux-only setting; libpq silently ignores it on other platforms.
        conn_str += " keepalives=1"
                    " keepalives_idle=" + std::to_string(keepalives_idle) +
                    " keepalives_interval=" + std::to_string(keepalives_interval) +
                    " keepalives_count=" + std::to_string(keepalives_count) +
                    " tcp_user_timeout=" + std::to_string(tcp_user_timeout_ms);

        // NOTE: statement_timeout, lock_timeout, and idle_in_transaction_session_timeout
        // CANNOT be set via connection string options when using PgBouncer
        // These are now set in DatabaseConnection constructor via SET commands
        
        return conn_str;
    }
};

struct QueueConfig {
    // Pop operation defaults
    int default_timeout = 30000;         // 30 seconds
    int default_batch_size = 1;
    
    // ThreadPool sizing
    int db_thread_pool_service_threads = 5; // Threads for background service DB operations
    
    // Backoff cleanup (used by stream poll workers and POP_WAIT)
    int backoff_cleanup_inactive_threshold = 3600; // 3600s (1 hour) - Remove backoff state entries inactive for N seconds
    
    // POP_WAIT backoff (used by sidecar long-polling via SharedStateManager)
    int pop_wait_initial_interval_ms = 100;    // Initial poll interval for POP_WAIT
    int pop_wait_backoff_threshold = 3;        // Consecutive empty checks before backoff
    double pop_wait_backoff_multiplier = 2.0;  // Exponential backoff multiplier
    int pop_wait_max_interval_ms = 1000;       // Max poll interval after backoff (ms)
    
    // Response queue timer settings
    int response_timer_interval_ms = 25; // Response timer polling interval in ms
    int response_batch_size = 100;       // Base number of responses to process per timer tick
    int response_batch_max = 500;        // Maximum responses per tick even under backlog
    
    // Sidecar pool
    int sidecar_pool_size = 50;             // Number of connections in sidecar pool
    
    // Sidecar micro-batching tuning
    int sidecar_micro_batch_wait_ms = 5;    // LEGACY. Global fallback for any type-specific
                                            // QUEEN_<TYPE>_MAX_HOLD_MS that isn't set (libqueen §9).
    int sidecar_max_items_per_tx = 1000;    // Max items per database transaction
    int sidecar_max_batch_size = 1000;      // Max requests per micro-batch
    int sidecar_max_pending_count = 50;     // Max pending requests before forcing immediate send

    // ========================================================================
    // libqueen tuning (§9 of LIBQUEEN_IMPROVEMENTS.md).
    // These are read directly from the environment by libqueen itself; the
    // fields here exist for documentation and optional pre-parse / validation
    // by the acceptor. Absent values fall back to the per-type defaults.
    // ========================================================================

    // Concurrency-controller mode: "vegas" (adaptive, default) or "static".
    std::string queen_concurrency_mode = "vegas";

    // Vegas adaptive-controller tuning (global).
    int    queen_vegas_min_limit             = 1;
    int    queen_vegas_max_limit             = 32;   // 2026-04-22: was 16
    int    queen_vegas_alpha                 = 3;
    int    queen_vegas_beta                  = 12;   // 2026-04-22: was 6
    int    queen_vegas_rtt_window_samples    = 50;
    int    queen_vegas_rtt_min_window_sec    = 30;
    int    queen_vegas_update_interval_ms    = 1000;

    // Per-type batch policy defaults. Each type may be overridden individually
    // by `QUEEN_<TYPE>_{PREFERRED_BATCH_SIZE,MAX_HOLD_MS,MAX_BATCH_SIZE,MAX_CONCURRENT}`.
    // Values here are for documentation only — libqueen reads env directly.
    //
    // `max_concurrent` raised 2026-04-22 from 4 → 24 (PUSH) and 4 → 16 (ACK/POP)
    // based on Vegas-uncapped sweep (test-perf/results/sweep_2026-04-22_07-41-19):
    // the old cap of 4 throttled throughput by ~74% vs letting Vegas explore.
    struct QueenTypeDefaults {
        int preferred_batch_size;
        int max_hold_ms;
        int max_batch_size;
        int max_concurrent;
    };
    QueenTypeDefaults queen_push         = { 50,  20, 500, 24};
    QueenTypeDefaults queen_pop          = { 20,   5, 500, 16};
    QueenTypeDefaults queen_ack          = { 50,  20, 500, 16};
    QueenTypeDefaults queen_transaction  = {  1,   0,   1,  1};
    QueenTypeDefaults queen_renew_lease  = { 10, 100, 100,  2};
    QueenTypeDefaults queen_custom       = {  1,   0,   1,  1};
    
    // Consumer group subscription
    std::string default_subscription_mode = "";  // "" = all (default), "new" = skip history, "new-only" = same as new
    
    static QueueConfig from_env() {
        QueueConfig config;
        
        config.default_timeout = get_env_int("DEFAULT_TIMEOUT", 30000);
        config.default_batch_size = get_env_int("DEFAULT_BATCH_SIZE", 1);
        
        // ThreadPool sizing
        config.db_thread_pool_service_threads = get_env_int("DB_THREAD_POOL_SERVICE_THREADS", 5);
        
        // Backoff cleanup
        config.backoff_cleanup_inactive_threshold = get_env_int("QUEUE_BACKOFF_CLEANUP_THRESHOLD", 3600);
        
        // POP_WAIT backoff
        config.pop_wait_initial_interval_ms = get_env_int("POP_WAIT_INITIAL_INTERVAL_MS", 100);
        config.pop_wait_backoff_threshold = get_env_int("POP_WAIT_BACKOFF_THRESHOLD", 3);
        config.pop_wait_backoff_multiplier = get_env_double("POP_WAIT_BACKOFF_MULTIPLIER", 2.0);
        config.pop_wait_max_interval_ms = get_env_int("POP_WAIT_MAX_INTERVAL_MS", 1000);
        
        config.response_timer_interval_ms = get_env_int("RESPONSE_TIMER_INTERVAL_MS", 25);
        config.response_batch_size = get_env_int("RESPONSE_BATCH_SIZE", 100);
        config.response_batch_max = get_env_int("RESPONSE_BATCH_MAX", 500);
        
        config.sidecar_pool_size = get_env_int("SIDECAR_POOL_SIZE", 50);
        
        // Sidecar micro-batching tuning
        config.sidecar_micro_batch_wait_ms = get_env_int("SIDECAR_MICRO_BATCH_WAIT_MS", 5);
        config.sidecar_max_items_per_tx = get_env_int("SIDECAR_MAX_ITEMS_PER_TX", 1000);
        config.sidecar_max_batch_size = get_env_int("SIDECAR_MAX_BATCH_SIZE", 1000);
        config.sidecar_max_pending_count = get_env_int("SIDECAR_MAX_PENDING_COUNT", 50);

        // libqueen tuning (see LIBQUEEN_IMPROVEMENTS.md §9). libqueen reads env
        // directly at construction; these are mirrored for visibility / logs.
        config.queen_concurrency_mode       = get_env_string("QUEEN_CONCURRENCY_MODE", "vegas");
        config.queen_vegas_min_limit        = get_env_int("QUEEN_VEGAS_MIN_LIMIT", 1);
        config.queen_vegas_max_limit        = get_env_int("QUEEN_VEGAS_MAX_LIMIT", 32);
        config.queen_vegas_alpha            = get_env_int("QUEEN_VEGAS_ALPHA", 3);
        config.queen_vegas_beta             = get_env_int("QUEEN_VEGAS_BETA", 12);
        config.queen_vegas_rtt_window_samples = get_env_int("QUEEN_VEGAS_RTT_WINDOW_SAMPLES", 50);
        config.queen_vegas_rtt_min_window_sec = get_env_int("QUEEN_VEGAS_RTT_MIN_WINDOW_SEC", 30);
        config.queen_vegas_update_interval_ms = get_env_int("QUEEN_VEGAS_UPDATE_INTERVAL_MS", 1000);

        auto read_type = [](const char* seg, QueueConfig::QueenTypeDefaults& d) {
            std::string pfx = std::string("QUEEN_") + seg + "_";
            d.preferred_batch_size = get_env_int((pfx + "PREFERRED_BATCH_SIZE").c_str(), d.preferred_batch_size);
            d.max_hold_ms          = get_env_int((pfx + "MAX_HOLD_MS").c_str(),          d.max_hold_ms);
            d.max_batch_size       = get_env_int((pfx + "MAX_BATCH_SIZE").c_str(),       d.max_batch_size);
            d.max_concurrent       = get_env_int((pfx + "MAX_CONCURRENT").c_str(),       d.max_concurrent);
        };
        read_type("PUSH",        config.queen_push);
        read_type("POP",         config.queen_pop);
        read_type("ACK",         config.queen_ack);
        read_type("TRANSACTION", config.queen_transaction);
        read_type("RENEW_LEASE", config.queen_renew_lease);
        read_type("CUSTOM",      config.queen_custom);
        
        config.default_subscription_mode = get_env_string("DEFAULT_SUBSCRIPTION_MODE", "");
        
        return config;
    }
};

struct JobsConfig {
    // Retention service
    int retention_interval = 300000;     // 5 minutes
    int retention_batch_size = 1000;
    int partition_cleanup_days = 30;
    
    // Metrics retention (messages_consumed table)
    int metrics_retention_days = 90;     // Keep 90 days of metrics
    
    // Metrics collector intervals
    int metrics_sample_interval_ms = 1000;    // 1 second - How often to sample metrics
    int metrics_aggregate_interval_s = 60;    // 60 seconds - How often to aggregate and save to DB
    
    // Eviction service
    int eviction_interval = 60000;       // 1 minute
    int eviction_batch_size = 1000;
    
    // Stats service - pre-computed analytics
    int stats_interval_ms = 10000;            // 10 seconds - Fast aggregation interval
    int stats_reconcile_interval_ms = 120000; // 2 minutes - Full reconciliation interval (scans messages table)
    int stats_history_retention_days = 7;     // 7 days - How long to keep stats history

    // PUSHPOPLOOKUPSOL: partition_lookup reconciliation (safety-net for
    // queen.update_partition_lookup_v1 calls missed during crashes or
    // transient failures of the per-push follow-up call).
    int partition_lookup_reconcile_interval_ms = 5000;  // 5 seconds
    int partition_lookup_reconcile_lookback_seconds = 60;

    static JobsConfig from_env() {
        JobsConfig config;
        
        config.retention_interval = get_env_int("RETENTION_INTERVAL", 300000);
        config.retention_batch_size = get_env_int("RETENTION_BATCH_SIZE", 1000);
        config.partition_cleanup_days = get_env_int("PARTITION_CLEANUP_DAYS", 30);
        
        config.metrics_retention_days = get_env_int("METRICS_RETENTION_DAYS", 90);
        
        config.metrics_sample_interval_ms = get_env_int("METRICS_SAMPLE_INTERVAL_MS", 1000);
        config.metrics_aggregate_interval_s = get_env_int("METRICS_AGGREGATE_INTERVAL_S", 60);
        
        config.eviction_interval = get_env_int("EVICTION_INTERVAL", 60000);
        config.eviction_batch_size = get_env_int("EVICTION_BATCH_SIZE", 1000);
        
        // Stats service
        config.stats_interval_ms = get_env_int("STATS_INTERVAL_MS", 10000);
        config.stats_reconcile_interval_ms = get_env_int("STATS_RECONCILE_INTERVAL_MS", 120000);
        config.stats_history_retention_days = get_env_int("STATS_HISTORY_RETENTION_DAYS", 7);

        // PUSHPOPLOOKUPSOL: partition_lookup reconcile service
        config.partition_lookup_reconcile_interval_ms =
            get_env_int("PARTITION_LOOKUP_RECONCILE_INTERVAL_MS", 5000);
        config.partition_lookup_reconcile_lookback_seconds =
            get_env_int("PARTITION_LOOKUP_RECONCILE_LOOKBACK_SECONDS", 60);

        return config;
    }
};

struct LoggingConfig {
    std::string log_level = "info";
    
    static LoggingConfig from_env() {
        LoggingConfig config;
        config.log_level = get_env_string("LOG_LEVEL", "info");
        return config;
    }
};

struct FileBufferConfig {
    // Platform-specific default
    #ifdef __APPLE__
        std::string buffer_dir = "/tmp/queen";
    #else
        std::string buffer_dir = "/var/lib/queen/buffers";
    #endif
    
    int flush_interval_ms = 100;
    size_t max_batch_size = 100;
    size_t max_events_per_file = 10000;  // Create new buffer file after N events
    
    static FileBufferConfig from_env() {
        FileBufferConfig config;
        
        // Get from environment or use platform default
        const char* env_dir = std::getenv("FILE_BUFFER_DIR");
        if (env_dir && env_dir[0] != '\0') {
            config.buffer_dir = env_dir;
        }
        // else: use default already set above
        
        // Ensure buffer_dir is never empty - fallback to platform default
        if (config.buffer_dir.empty()) {
            #ifdef __APPLE__
                config.buffer_dir = "/tmp/queen";
            #else
                config.buffer_dir = "/var/lib/queen/buffers";
            #endif
        }
        
        config.flush_interval_ms = get_env_int("FILE_BUFFER_FLUSH_MS", 100);
        config.max_batch_size = get_env_int("FILE_BUFFER_MAX_BATCH", 100);
        config.max_events_per_file = get_env_int("FILE_BUFFER_EVENTS_PER_FILE", 10000);
        
        return config;
    }
};

// UDP peer entry with host and port
struct UdpPeerEntry {
    std::string host;
    int port;
};

struct SharedStateConfig {
    bool enabled = true;
    
    // Security - HMAC secret for packet signing
    std::string sync_secret;
    
    // Cache limits
    int partition_cache_max = 10000;
    
    // TTL settings (milliseconds)
    int partition_cache_ttl_ms = 300000;  // 5 minutes
    
    // Periodic refresh
    int queue_config_refresh_ms = 60000;  // 60 seconds
    
    // Heartbeat
    int heartbeat_interval_ms = 1000;   // 1 second
    int dead_threshold_ms = 5000;       // 5 seconds (5 missed heartbeats)
    
    // Performance
    int recv_buffer_mb = 8;             // UDP receive buffer size
    
    static SharedStateConfig from_env() {
        SharedStateConfig config;
        config.enabled = get_env_bool("QUEEN_SYNC_ENABLED", true);
        config.sync_secret = get_env_string("QUEEN_SYNC_SECRET", "");
        config.partition_cache_max = get_env_int("QUEEN_CACHE_PARTITION_MAX", 10000);
        config.partition_cache_ttl_ms = get_env_int("QUEEN_CACHE_PARTITION_TTL_MS", 300000);
        config.queue_config_refresh_ms = get_env_int("QUEEN_CACHE_REFRESH_INTERVAL_MS", 60000);
        config.heartbeat_interval_ms = get_env_int("QUEEN_SYNC_HEARTBEAT_MS", 1000);
        config.dead_threshold_ms = get_env_int("QUEEN_SYNC_DEAD_THRESHOLD_MS", 5000);
        config.recv_buffer_mb = get_env_int("QUEEN_SYNC_RECV_BUFFER_MB", 8);
        return config;
    }
    
    bool validate() const {
        // Secret is only required if sync is enabled AND there are UDP peers
        // For now, allow empty secret (insecure mode for development)
        if (enabled && !sync_secret.empty() && sync_secret.length() != 64) {
            return false;
        }
        return true;
    }
};

// ============================================================================
// JWT Authentication Configuration
// ============================================================================
struct AuthConfig {
    // Master enable/disable for JWT authentication
    bool enabled = false;
    
    // Algorithm: "HS256", "RS256", "EdDSA", or "auto" (try based on token header)
    std::string algorithm = "HS256";
    
    // HS256: Shared secret for HMAC signing (base64 or raw string)
    std::string secret = "";
    
    // RS256: JWKS URL for fetching public keys from identity provider
    std::string jwks_url = "";
    
    // RS256: Static public key in PEM format (alternative to JWKS URL)
    std::string public_key = "";
    
    // Optional token validation settings
    std::string issuer = "";              // Expected 'iss' claim (empty = any issuer)
    std::string audience = "";            // Expected 'aud' claim (empty = any audience)
    int clock_skew_seconds = 30;          // Tolerance for exp/nbf/iat checks
    
    // Paths that skip authentication (comma-separated in env var).
    // should_skip_path() does an exact match plus a trailing-slash prefix
    // match — both /metrics and /metrics/prometheus need explicit entries.
    std::vector<std::string> skip_paths = {"/health", "/metrics/prometheus", "/metrics", "/"};
    
    // JWKS refresh settings (for RS256)
    int jwks_refresh_interval_seconds = 3600;  // Refresh JWKS every hour
    int jwks_request_timeout_ms = 5000;        // Timeout for JWKS HTTP requests
    
    // Role-based access control settings
    std::string roles_claim = "role";          // Claim name containing role (proxy uses "role")
    std::string roles_array_claim = "roles";   // Alternative: claim with array of roles
    std::string role_admin = "admin";          // Role value for admin access
    std::string role_read_write = "read-write";// Role value for read-write access
    std::string role_read_only = "read-only";  // Role value for read-only access
    std::string role_write_only = "write-only";// Role value for write-only (produce-only) access
    
    static AuthConfig from_env() {
        AuthConfig config;
        
        config.enabled = get_env_bool("JWT_ENABLED", false);
        config.algorithm = get_env_string("JWT_ALGORITHM", "HS256");
        config.secret = get_env_string("JWT_SECRET", "");
        config.jwks_url = get_env_string("JWT_JWKS_URL", "");
        config.public_key = get_env_string("JWT_PUBLIC_KEY", "");
        config.issuer = get_env_string("JWT_ISSUER", "");
        config.audience = get_env_string("JWT_AUDIENCE", "");
        config.clock_skew_seconds = get_env_int("JWT_CLOCK_SKEW", 30);
        
        // Parse skip paths from comma-separated list
        std::string skip_str = get_env_string("JWT_SKIP_PATHS", "/health,/metrics/prometheus,/metrics,/");
        config.skip_paths.clear();
        if (!skip_str.empty()) {
            size_t pos = 0;
            std::string remaining = skip_str;
            while ((pos = remaining.find(',')) != std::string::npos) {
                std::string path = remaining.substr(0, pos);
                // Trim whitespace
                size_t start = path.find_first_not_of(" \t");
                size_t end = path.find_last_not_of(" \t");
                if (start != std::string::npos) {
                    config.skip_paths.push_back(path.substr(start, end - start + 1));
                }
                remaining = remaining.substr(pos + 1);
            }
            // Handle last element
            if (!remaining.empty()) {
                size_t start = remaining.find_first_not_of(" \t");
                size_t end = remaining.find_last_not_of(" \t");
                if (start != std::string::npos) {
                    config.skip_paths.push_back(remaining.substr(start, end - start + 1));
                }
            }
        }
        
        config.jwks_refresh_interval_seconds = get_env_int("JWT_JWKS_REFRESH_INTERVAL", 3600);
        config.jwks_request_timeout_ms = get_env_int("JWT_JWKS_TIMEOUT_MS", 5000);
        
        config.roles_claim = get_env_string("JWT_ROLES_CLAIM", "role");
        config.roles_array_claim = get_env_string("JWT_ROLES_ARRAY_CLAIM", "roles");
        config.role_admin = get_env_string("JWT_ROLE_ADMIN", "admin");
        config.role_read_write = get_env_string("JWT_ROLE_READ_WRITE", "read-write");
        config.role_read_only = get_env_string("JWT_ROLE_READ_ONLY", "read-only");
        config.role_write_only = get_env_string("JWT_ROLE_WRITE_ONLY", "write-only");
        
        return config;
    }
    
    // Validate configuration
    bool validate() const {
        if (!enabled) return true;
        
        // Must have either secret (HS256) or JWKS URL/public key (RS256/EdDSA)
        if (algorithm == "HS256" || algorithm == "auto") {
            if (secret.empty() && jwks_url.empty() && public_key.empty()) {
                return false;  // Need at least one credential source
            }
        }
        
        if (algorithm == "RS256" || algorithm == "EdDSA") {
            if (jwks_url.empty() && public_key.empty()) {
                return false;  // RS256/EdDSA needs JWKS or public key
            }
        }
        
        // Validate algorithm value
        if (algorithm != "HS256" && algorithm != "RS256" && algorithm != "EdDSA" && algorithm != "auto") {
            return false;
        }
        
        return true;
    }
    
    // Check if a path should skip authentication
    bool should_skip_path(const std::string& path) const {
        for (const auto& skip : skip_paths) {
            if (path == skip) return true;
            // Prefix match for paths like /assets/ (must be longer than just "/")
            // Skip the root "/" for prefix matching as it would match everything
            if (skip.length() > 1 && skip.back() == '/' && path.find(skip) == 0) {
                return true;
            }
        }
        return false;
    }
};

struct InterInstanceConfig {
    // UDP peers - host:port or just host (uses default port)
    std::string udp_peers = "";
    int udp_port = 6633;  // Default UDP notification port
    
    // Shared state configuration
    SharedStateConfig shared_state;
    
    static InterInstanceConfig from_env() {
        InterInstanceConfig config;
        config.udp_peers = get_env_string("QUEEN_UDP_PEERS", "");
        config.udp_port = get_env_int("QUEEN_UDP_NOTIFY_PORT", 6633);
        config.shared_state = SharedStateConfig::from_env();
        return config;
    }
    
    // Returns true if UDP peer notification is enabled
    bool has_udp_peers() const {
        return !udp_peers.empty();
    }
    
    // Parse comma-separated UDP peers into vector of {host, port}
    // Accepts: "host:port" or "host" (uses default udp_port)
    std::vector<UdpPeerEntry> parse_udp_peers() const {
        std::vector<UdpPeerEntry> entries;
        if (udp_peers.empty()) return entries;
        
        std::string remaining = udp_peers;
        size_t pos = 0;
        while ((pos = remaining.find(',')) != std::string::npos) {
            std::string entry = remaining.substr(0, pos);
            auto parsed = parse_udp_entry(entry, udp_port);
            if (!parsed.host.empty()) {
                entries.push_back(parsed);
            }
            remaining = remaining.substr(pos + 1);
        }
        // Handle last element
        if (!remaining.empty()) {
            auto parsed = parse_udp_entry(remaining, udp_port);
            if (!parsed.host.empty()) {
                entries.push_back(parsed);
            }
        }
        return entries;
    }
    
private:
    // Parse a single UDP peer entry: "host:port" or "host"
    static UdpPeerEntry parse_udp_entry(const std::string& entry, int default_port) {
        UdpPeerEntry result;
        result.port = default_port;
        
        std::string s = entry;
        
        // Trim whitespace
        size_t start = s.find_first_not_of(" \t");
        size_t end = s.find_last_not_of(" \t");
        if (start == std::string::npos) return result;
        s = s.substr(start, end - start + 1);
        
        // Strip protocol if present (for flexibility)
        if (s.size() > 7 && s.substr(0, 7) == "http://") {
            s = s.substr(7);
        } else if (s.size() > 8 && s.substr(0, 8) == "https://") {
            s = s.substr(8);
        }
        
        // Strip trailing slash
        if (!s.empty() && s.back() == '/') {
            s.pop_back();
        }
        
        // Split host:port
        size_t colon = s.rfind(':');  // Use rfind for IPv6 compatibility
        if (colon != std::string::npos) {
            result.host = s.substr(0, colon);
            try {
                result.port = std::stoi(s.substr(colon + 1));
            } catch (...) {
                // Keep default port
            }
        } else {
            result.host = s;
        }
        
        return result;
    }
};

struct Config {
    ServerConfig server;
    DatabaseConfig database;
    QueueConfig queue;
    JobsConfig jobs;
    LoggingConfig logging;
    FileBufferConfig file_buffer;
    InterInstanceConfig inter_instance;
    AuthConfig auth;
    
    static Config load() {
        Config config;
        config.server = ServerConfig::from_env();
        config.database = DatabaseConfig::from_env();
        config.queue = QueueConfig::from_env();
        config.jobs = JobsConfig::from_env();
        config.logging = LoggingConfig::from_env();
        config.file_buffer = FileBufferConfig::from_env();
        config.inter_instance = InterInstanceConfig::from_env();
        config.auth = AuthConfig::from_env();
        return config;
    }
};

} // namespace queen
