#include "queen/shared_state_manager.hpp"
#include "queen/async_database.hpp"
#include <spdlog/spdlog.h>
#include <chrono>

namespace queen {

// Global instance
std::shared_ptr<SharedStateManager> global_shared_state;

SharedStateManager::SharedStateManager(const InterInstanceConfig& config,
                                       const std::string& server_id,
                                       std::shared_ptr<AsyncDbPool> db_pool)
    : config_(config)
    , server_id_(server_id)
    , db_pool_(db_pool)
    , server_health_(config.shared_state.dead_threshold_ms)
{
    // Enable if UDP peers are configured
    enabled_ = config.shared_state.enabled && config.has_udp_peers();
    
    if (enabled_) {
        // Create transport
        transport_ = std::make_shared<UDPSyncTransport>(
            config.shared_state,
            server_id,
            config.udp_port
        );
        
        // Add peers
        for (const auto& peer : config.parse_udp_peers()) {
            transport_->add_peer(peer.host, peer.port);
        }
        
        spdlog::info("SharedStateManager: Enabled with {} peers, server_id={}",
                    config.parse_udp_peers().size(), server_id);
    } else {
        spdlog::info("SharedStateManager: Disabled (no UDP peers or sync disabled)");
    }
}

SharedStateManager::~SharedStateManager() {
    stop();
}

void SharedStateManager::start() {
    // Always load queue configs and maintenance modes on startup
    if (db_pool_) {
        spdlog::info("SharedStateManager: Loading queue configs and maintenance modes from database...");
        refresh_queue_configs_from_db();
        refresh_maintenance_mode_from_db();
        refresh_pop_maintenance_mode_from_db();
    }
    
    if (!enabled_ || running_) {
        if (!enabled_) {
            spdlog::info("SharedStateManager: Sync disabled, but queue config cache is available");
        }
        return;
    }
    
    // Resolve peer hostnames
    transport_->resolve_peers();
    
    // Set up message handlers
    setup_message_handlers();
    
    // Start transport
    if (!transport_->start()) {
        spdlog::error("SharedStateManager: Failed to start transport");
        return;
    }
    
    running_ = true;
    
    // Start background threads
    heartbeat_thread_ = std::thread(&SharedStateManager::heartbeat_loop, this);
    
    if (db_pool_) {
        refresh_thread_ = std::thread(&SharedStateManager::refresh_loop, this);
    }
    
    dns_refresh_thread_ = std::thread(&SharedStateManager::dns_refresh_loop, this);
    
    spdlog::info("SharedStateManager: Started");
}

void SharedStateManager::stop() {
    if (!running_) return;
    
    spdlog::info("SharedStateManager: Stopping...");
    running_ = false;
    
    if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
    if (refresh_thread_.joinable()) refresh_thread_.join();
    if (dns_refresh_thread_.joinable()) dns_refresh_thread_.join();
    
    if (transport_) {
        transport_->stop();
    }
    
    spdlog::info("SharedStateManager: Stopped");
}

// ============================================================
// Queue Config Cache
// ============================================================

std::optional<caches::CachedQueueConfig> SharedStateManager::get_queue_config(const std::string& queue) {
    return queue_configs_.get(queue);
}

std::optional<caches::CachedQueueConfig> SharedStateManager::get_or_fetch_queue_config(const std::string& queue) {
    // First, check the cache
    auto cached = queue_configs_.get(queue);
    if (cached) {
        return cached;
    }
    
    // Cache miss - try to fetch from DB
    if (!db_pool_) {
        spdlog::debug("SharedStateManager: Cache miss for queue '{}', no DB pool available", queue);
        return std::nullopt;
    }
    
    try {
        auto conn = db_pool_->acquire();
        if (!conn) {
            spdlog::warn("SharedStateManager: Cache miss for queue '{}', failed to acquire DB connection", queue);
            return std::nullopt;
        }
        
        // Query for this specific queue
        std::string sql = R"(
            SELECT 
                id, name, namespace, task, priority,
                lease_time, retry_limit, retry_delay, 
                max_queue_size, ttl, dead_letter_queue, 
                dlq_after_max_retries, delayed_processing,
                window_buffer, retention_seconds, completed_retention_seconds,
                encryption_enabled, storage
            FROM queen.queues
            WHERE name = $1
        )";
        
        const char* params[] = { queue.c_str() };
        PGresult* raw_result = PQexecParams(conn.get(), sql.c_str(), 1, nullptr, params, nullptr, nullptr, 0);
        auto result = std::unique_ptr<PGresult, decltype(&PQclear)>(raw_result, PQclear);
        
        if (PQresultStatus(result.get()) != PGRES_TUPLES_OK) {
            spdlog::warn("SharedStateManager: DB query failed for queue '{}': {}", 
                        queue, PQerrorMessage(conn.get()));
            return std::nullopt;
        }
        
        int num_rows = PQntuples(result.get());
        if (num_rows == 0) {
            // Queue doesn't exist in DB (might be auto-created later)
            spdlog::debug("SharedStateManager: Queue '{}' not found in DB", queue);
            return std::nullopt;
        }
        
        // Parse the result and populate cache
        caches::CachedQueueConfig cfg;
        cfg.id = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "id"));
        cfg.name = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "name"));
        
        const char* ns = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "namespace"));
        cfg.namespace_name = (ns && strlen(ns) > 0) ? ns : "";
        
        const char* task = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "task"));
        cfg.task = (task && strlen(task) > 0) ? task : "";
        
        const char* priority = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "priority"));
        cfg.priority = (priority && strlen(priority) > 0) ? std::stoi(priority) : 0;
        
        const char* lease_time = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "lease_time"));
        cfg.lease_time = (lease_time && strlen(lease_time) > 0) ? std::stoi(lease_time) : 300;
        
        const char* retry_limit = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "retry_limit"));
        cfg.retry_limit = (retry_limit && strlen(retry_limit) > 0) ? std::stoi(retry_limit) : 3;
        
        const char* retry_delay = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "retry_delay"));
        cfg.retry_delay = (retry_delay && strlen(retry_delay) > 0) ? std::stoi(retry_delay) : 1000;
        
        const char* max_size = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "max_queue_size"));
        cfg.max_size = (max_size && strlen(max_size) > 0) ? std::stoi(max_size) : 10000;
        
        const char* ttl = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "ttl"));
        cfg.ttl = (ttl && strlen(ttl) > 0) ? std::stoi(ttl) : 3600;
        
        const char* dlq = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "dead_letter_queue"));
        cfg.dlq_enabled = (dlq && (strcmp(dlq, "t") == 0 || strcmp(dlq, "true") == 0));
        
        const char* dlq_max = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "dlq_after_max_retries"));
        cfg.dlq_after_max_retries = (dlq_max && (strcmp(dlq_max, "t") == 0 || strcmp(dlq_max, "true") == 0));
        
        const char* delayed = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "delayed_processing"));
        cfg.delayed_processing = (delayed && strlen(delayed) > 0) ? std::stoi(delayed) : 0;
        
        const char* window = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "window_buffer"));
        cfg.window_buffer = (window && strlen(window) > 0) ? std::stoi(window) : 0;
        
        const char* retention = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "retention_seconds"));
        cfg.retention_seconds = (retention && strlen(retention) > 0) ? std::stoi(retention) : 0;
        
        const char* completed_retention = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "completed_retention_seconds"));
        cfg.completed_retention_seconds = (completed_retention && strlen(completed_retention) > 0) ? std::stoi(completed_retention) : 0;
        
        const char* encrypted = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "encryption_enabled"));
        cfg.encryption_enabled = (encrypted && (strcmp(encrypted, "t") == 0 || strcmp(encrypted, "true") == 0));

        const char* storage = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "storage"));
        cfg.storage = (storage && strlen(storage) > 0) ? storage : "rows";

        // Populate cache for future calls
        queue_configs_.set(queue, cfg);
        spdlog::debug("SharedStateManager: Fetched and cached config for queue '{}'", queue);
        
        return cfg;
        
    } catch (const std::exception& e) {
        spdlog::warn("SharedStateManager: Failed to fetch queue config for '{}': {}", queue, e.what());
        return std::nullopt;
    }
}

void SharedStateManager::set_queue_config(const std::string& queue, const caches::CachedQueueConfig& config) {
    queue_configs_.set(queue, config);
    
    if (running_ && transport_) {
        nlohmann::json payload = {
            {"queue", queue},
            {"config", {
                {"id", config.id},
                {"lease_time", config.lease_time},
                {"retry_limit", config.retry_limit},
                {"retry_delay", config.retry_delay},
                {"delayed_processing", config.delayed_processing},
                {"window_buffer", config.window_buffer},
                {"encryption_enabled", config.encryption_enabled},
                {"dlq_enabled", config.dlq_enabled},
                {"dlq_after_max_retries", config.dlq_after_max_retries},
                {"max_size", config.max_size},
                {"ttl", config.ttl},
                {"priority", config.priority},
                {"namespace_name", config.namespace_name},
                {"task", config.task},
                {"retention_seconds", config.retention_seconds},
                {"completed_retention_seconds", config.completed_retention_seconds},
                {"retention_enabled", config.retention_enabled},
                {"max_wait_time_seconds", config.max_wait_time_seconds}
            }},
            {"version", config.version}
        };
        
        transport_->broadcast(UDPSyncMessageType::QUEUE_CONFIG_SET, payload);
    }
}

void SharedStateManager::delete_queue_config(const std::string& queue) {
    queue_configs_.remove(queue);
    
    if (running_ && transport_) {
        nlohmann::json payload = {{"queue", queue}};
        transport_->broadcast(UDPSyncMessageType::QUEUE_CONFIG_DELETE, payload);
    }
}

// ============================================================
// Server Health
// ============================================================

std::vector<std::string> SharedStateManager::get_alive_servers() {
    return server_health_.get_alive_servers();
}

std::vector<std::string> SharedStateManager::get_dead_servers() {
    return server_health_.get_dead_servers();
}

// ============================================================
// Message Available Notification
// ============================================================

void SharedStateManager::on_message_available(MessageAvailableCallback callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    message_available_callback_ = std::move(callback);
}

void SharedStateManager::notify_message_available(const std::string& queue, const std::string& partition) {
    // Notify local Queen instances (thread-safe via uv_async)
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        if (message_available_callback_) {
            message_available_callback_(queue, partition);
        }
    }
    
    // Broadcast to other servers via UDP
    if (running_ && transport_) {
        nlohmann::json payload = {
            {"queue", queue},
            {"partition", partition},
            {"ts", now_ms()}
        };
        
        transport_->broadcast(UDPSyncMessageType::MESSAGE_AVAILABLE, payload);
    }
}

// ============================================================
// Message Handlers
// ============================================================

void SharedStateManager::setup_message_handlers() {
    using Type = UDPSyncMessageType;
    
    transport_->on_message(Type::MESSAGE_AVAILABLE, [this](Type, const std::string& sender, const nlohmann::json& p) {
        handle_message_available(sender, p);
    });
    
    transport_->on_message(Type::HEARTBEAT, [this](Type, const std::string& sender, const nlohmann::json& p) {
        handle_heartbeat(sender, p);
    });
    
    transport_->on_message(Type::QUEUE_CONFIG_SET, [this](Type, const std::string& sender, const nlohmann::json& p) {
        handle_queue_config_set(sender, p);
    });
    
    transport_->on_message(Type::QUEUE_CONFIG_DELETE, [this](Type, const std::string& sender, const nlohmann::json& p) {
        handle_queue_config_delete(sender, p);
    });
    
    transport_->on_message(Type::MAINTENANCE_MODE_SET, [this](Type, const std::string& sender, const nlohmann::json& p) {
        handle_maintenance_mode_set(sender, p);
    });
    
    transport_->on_message(Type::POP_MAINTENANCE_MODE_SET, [this](Type, const std::string& sender, const nlohmann::json& p) {
        handle_pop_maintenance_mode_set(sender, p);
    });
}

void SharedStateManager::handle_message_available(const std::string& sender, const nlohmann::json& payload) {
    std::string queue = payload.value("queue", "");
    std::string partition = payload.value("partition", "");
    
    spdlog::debug("SharedState: MESSAGE_AVAILABLE from {} for {}:{}", sender, queue, partition);
    
    // Notify local Queen instances (thread-safe via uv_async)
    if (!queue.empty()) {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        if (message_available_callback_) {
            spdlog::debug("SharedState: Invoking callback for {}:{}", queue, partition);
            message_available_callback_(queue, partition);
        } else {
            spdlog::warn("SharedState: No callback registered!");
        }
    }
}

void SharedStateManager::handle_heartbeat(const std::string& sender, const nlohmann::json& payload) {
    std::string session_id = payload.value("session_id", "");
    uint64_t sequence = payload.value("sequence", 0ULL);
    
    server_health_.record_heartbeat(sender, session_id, sequence);
}

void SharedStateManager::handle_queue_config_set(const std::string& sender, const nlohmann::json& payload) {
    std::string queue = payload.value("queue", "");
    if (queue.empty()) return;
    
    auto config_json = payload.value("config", nlohmann::json{});
    
    caches::CachedQueueConfig config;
    config.name = queue;
    config.id = config_json.value("id", "");
    config.lease_time = config_json.value("lease_time", 300);
    config.retry_limit = config_json.value("retry_limit", 3);
    config.retry_delay = config_json.value("retry_delay", 1000);
    config.delayed_processing = config_json.value("delayed_processing", 0);
    config.window_buffer = config_json.value("window_buffer", 0);
    config.encryption_enabled = config_json.value("encryption_enabled", false);
    config.dlq_enabled = config_json.value("dlq_enabled", false);
    config.dlq_after_max_retries = config_json.value("dlq_after_max_retries", false);
    config.max_size = config_json.value("max_size", 10000);
    config.ttl = config_json.value("ttl", 3600);
    config.priority = config_json.value("priority", 0);
    config.namespace_name = config_json.value("namespace_name", "");
    config.task = config_json.value("task", "");
    config.version = payload.value("version", 0ULL);
    config.retention_seconds = config_json.value("retention_seconds", 0);
    config.completed_retention_seconds = config_json.value("completed_retention_seconds", 0);
    config.retention_enabled = config_json.value("retention_enabled", false);
    config.max_wait_time_seconds = config_json.value("max_wait_time_seconds", 0);
    
    queue_configs_.set(queue, config);
    
    spdlog::debug("SharedState: QUEUE_CONFIG_SET from {} for {}", sender, queue);
}

void SharedStateManager::handle_queue_config_delete(const std::string& sender, const nlohmann::json& payload) {
    std::string queue = payload.value("queue", "");
    if (queue.empty()) return;
    
    queue_configs_.remove(queue);
    
    spdlog::debug("SharedState: QUEUE_CONFIG_DELETE from {} for {}", sender, queue);
}

void SharedStateManager::handle_maintenance_mode_set(const std::string& sender, const nlohmann::json& payload) {
    bool enabled = payload.value("enabled", false);
    
    bool prev = maintenance_mode_.exchange(enabled);
    if (prev != enabled) {
        spdlog::info("SharedState: MAINTENANCE_MODE_SET from {} -> {} (from peer {})", 
                    prev ? "ON" : "OFF", enabled ? "ON" : "OFF", sender);
    }
}

void SharedStateManager::handle_pop_maintenance_mode_set(const std::string& sender, const nlohmann::json& payload) {
    bool enabled = payload.value("enabled", false);
    
    bool prev = pop_maintenance_mode_.exchange(enabled);
    if (prev != enabled) {
        spdlog::info("SharedState: POP_MAINTENANCE_MODE_SET from {} -> {} (from peer {})", 
                    prev ? "ON" : "OFF", enabled ? "ON" : "OFF", sender);
    }
}

// ============================================================
// Background Tasks
// ============================================================

void SharedStateManager::heartbeat_loop() {
    spdlog::info("SharedStateManager: Heartbeat thread started (interval={}ms)",
                 config_.shared_state.heartbeat_interval_ms);
    
    while (running_) {
        // Send heartbeat
        nlohmann::json payload = {
            {"session_id", transport_->session_id()},
            {"sequence", transport_->messages_sent()},
            {"ts", now_ms()}
        };
        
        transport_->broadcast(UDPSyncMessageType::HEARTBEAT, payload);
        
        // Check for dead servers
        server_health_.check_dead_servers();
        
        // Sleep
        std::this_thread::sleep_for(
            std::chrono::milliseconds(config_.shared_state.heartbeat_interval_ms)
        );
    }
    
    spdlog::info("SharedStateManager: Heartbeat thread stopped");
}

void SharedStateManager::refresh_loop() {
    if (!db_pool_) return;
    
    spdlog::info("SharedStateManager: Refresh thread started (interval={}ms)",
                 config_.shared_state.queue_config_refresh_ms);
    
    // Initial delay to let server start up
    std::this_thread::sleep_for(std::chrono::seconds(5));
    
    while (running_) {
        try {
            refresh_queue_configs_from_db();
        } catch (const std::exception& e) {
            spdlog::warn("SharedStateManager: Failed to refresh queue configs: {}", e.what());
        }
        
        try {
            refresh_maintenance_mode_from_db();
        } catch (const std::exception& e) {
            spdlog::warn("SharedStateManager: Failed to refresh maintenance mode: {}", e.what());
        }
        
        try {
            refresh_pop_maintenance_mode_from_db();
        } catch (const std::exception& e) {
            spdlog::warn("SharedStateManager: Failed to refresh pop maintenance mode: {}", e.what());
        }
        
        std::this_thread::sleep_for(
            std::chrono::milliseconds(config_.shared_state.queue_config_refresh_ms)
        );
    }
    
    spdlog::info("SharedStateManager: Refresh thread stopped");
}

void SharedStateManager::dns_refresh_loop() {
    constexpr int DNS_REFRESH_INTERVAL_SECONDS = 30;
    
    spdlog::info("SharedStateManager: DNS refresh thread started (interval={}s)",
                 DNS_REFRESH_INTERVAL_SECONDS);
    
    while (running_) {
        std::this_thread::sleep_for(std::chrono::seconds(DNS_REFRESH_INTERVAL_SECONDS));
        
        if (!running_) break;
        
        try {
            int changes = transport_->resolve_peers();
            
            if (changes > 0) {
                spdlog::warn("SharedStateManager: DNS refresh detected {} IP change(s)", changes);
            }
        } catch (const std::exception& e) {
            spdlog::error("SharedStateManager: DNS refresh failed: {}", e.what());
        }
    }
    
    spdlog::info("SharedStateManager: DNS refresh thread stopped");
}

void SharedStateManager::refresh_queue_configs_from_db() {
    if (!db_pool_) return;
    
    try {
        auto conn = db_pool_->acquire();
        if (!conn) {
            spdlog::warn("SharedStateManager: Failed to acquire DB connection for queue config refresh");
            return;
        }
        
        std::string sql = R"(
            SELECT 
                id, name, namespace, task, priority,
                lease_time, retry_limit, retry_delay, 
                max_queue_size, ttl, dead_letter_queue, 
                dlq_after_max_retries, delayed_processing,
                window_buffer, retention_seconds, completed_retention_seconds,
                encryption_enabled
            FROM queen.queues
        )";
        
        sendAndWait(conn.get(), sql.c_str());
        auto result = getTuplesResult(conn.get());
        
        int num_rows = PQntuples(result.get());
        
        std::vector<caches::CachedQueueConfig> configs;
        
        for (int i = 0; i < num_rows; i++) {
            caches::CachedQueueConfig cfg;
            
            cfg.id = PQgetvalue(result.get(), i, PQfnumber(result.get(), "id"));
            cfg.name = PQgetvalue(result.get(), i, PQfnumber(result.get(), "name"));
            
            const char* ns = PQgetvalue(result.get(), i, PQfnumber(result.get(), "namespace"));
            cfg.namespace_name = (ns && strlen(ns) > 0) ? ns : "";
            
            const char* task = PQgetvalue(result.get(), i, PQfnumber(result.get(), "task"));
            cfg.task = (task && strlen(task) > 0) ? task : "";
            
            const char* priority = PQgetvalue(result.get(), i, PQfnumber(result.get(), "priority"));
            cfg.priority = (priority && strlen(priority) > 0) ? std::stoi(priority) : 0;
            
            const char* lease_time = PQgetvalue(result.get(), i, PQfnumber(result.get(), "lease_time"));
            cfg.lease_time = (lease_time && strlen(lease_time) > 0) ? std::stoi(lease_time) : 300;
            
            const char* retry_limit = PQgetvalue(result.get(), i, PQfnumber(result.get(), "retry_limit"));
            cfg.retry_limit = (retry_limit && strlen(retry_limit) > 0) ? std::stoi(retry_limit) : 3;
            
            const char* retry_delay = PQgetvalue(result.get(), i, PQfnumber(result.get(), "retry_delay"));
            cfg.retry_delay = (retry_delay && strlen(retry_delay) > 0) ? std::stoi(retry_delay) : 1000;
            
            const char* max_size = PQgetvalue(result.get(), i, PQfnumber(result.get(), "max_queue_size"));
            cfg.max_size = (max_size && strlen(max_size) > 0) ? std::stoi(max_size) : 10000;
            
            const char* ttl = PQgetvalue(result.get(), i, PQfnumber(result.get(), "ttl"));
            cfg.ttl = (ttl && strlen(ttl) > 0) ? std::stoi(ttl) : 3600;
            
            const char* dlq = PQgetvalue(result.get(), i, PQfnumber(result.get(), "dead_letter_queue"));
            cfg.dlq_enabled = (dlq && (strcmp(dlq, "t") == 0 || strcmp(dlq, "true") == 0));
            
            const char* dlq_max = PQgetvalue(result.get(), i, PQfnumber(result.get(), "dlq_after_max_retries"));
            cfg.dlq_after_max_retries = (dlq_max && (strcmp(dlq_max, "t") == 0 || strcmp(dlq_max, "true") == 0));
            
            const char* delayed = PQgetvalue(result.get(), i, PQfnumber(result.get(), "delayed_processing"));
            cfg.delayed_processing = (delayed && strlen(delayed) > 0) ? std::stoi(delayed) : 0;
            
            const char* window = PQgetvalue(result.get(), i, PQfnumber(result.get(), "window_buffer"));
            cfg.window_buffer = (window && strlen(window) > 0) ? std::stoi(window) : 0;
            
            const char* retention = PQgetvalue(result.get(), i, PQfnumber(result.get(), "retention_seconds"));
            cfg.retention_seconds = (retention && strlen(retention) > 0) ? std::stoi(retention) : 0;
            
            const char* completed_retention = PQgetvalue(result.get(), i, PQfnumber(result.get(), "completed_retention_seconds"));
            cfg.completed_retention_seconds = (completed_retention && strlen(completed_retention) > 0) ? std::stoi(completed_retention) : 0;
            
            const char* encrypted = PQgetvalue(result.get(), i, PQfnumber(result.get(), "encryption_enabled"));
            cfg.encryption_enabled = (encrypted && (strcmp(encrypted, "t") == 0 || strcmp(encrypted, "true") == 0));
            
            configs.push_back(std::move(cfg));
        }
        
        queue_configs_.bulk_update(configs, true);
        
        spdlog::debug("SharedStateManager: Queue config cache refreshed with {} entries", configs.size());
        
    } catch (const std::exception& e) {
        spdlog::warn("SharedStateManager: Failed to refresh queue configs from DB: {}", e.what());
    }
}

void SharedStateManager::refresh_maintenance_mode_from_db() {
    if (!db_pool_) return;
    
    try {
        auto conn = db_pool_->acquire();
        if (!conn) {
            spdlog::warn("SharedStateManager: Failed to acquire DB connection for maintenance mode refresh");
            return;
        }
        
        std::string sql = R"(
            SELECT value->>'enabled' as enabled
            FROM queen.system_state
            WHERE key = 'maintenance_mode'
        )";
        
        sendAndWait(conn.get(), sql.c_str());
        auto result = getTuplesResult(conn.get());
        
        bool enabled = false;
        if (PQntuples(result.get()) > 0) {
            const char* val = PQgetvalue(result.get(), 0, 0);
            enabled = (val && std::string(val) == "true");
        }
        
        bool prev = maintenance_mode_.exchange(enabled);
        if (prev != enabled) {
            spdlog::info("SharedStateManager: Maintenance mode changed {} -> {} (from DB refresh)", 
                        prev ? "ON" : "OFF", enabled ? "ON" : "OFF");
        }
        
    } catch (const std::exception& e) {
        spdlog::warn("SharedStateManager: Failed to refresh maintenance mode from DB: {}", e.what());
    }
}

void SharedStateManager::refresh_pop_maintenance_mode_from_db() {
    if (!db_pool_) return;
    
    try {
        auto conn = db_pool_->acquire();
        if (!conn) {
            spdlog::warn("SharedStateManager: Failed to acquire DB connection for pop maintenance mode refresh");
            return;
        }
        
        std::string sql = R"(
            SELECT value->>'enabled' as enabled
            FROM queen.system_state
            WHERE key = 'pop_maintenance_mode'
        )";
        
        sendAndWait(conn.get(), sql.c_str());
        auto result = getTuplesResult(conn.get());
        
        bool enabled = false;
        if (PQntuples(result.get()) > 0) {
            const char* val = PQgetvalue(result.get(), 0, 0);
            enabled = (val && std::string(val) == "true");
        }
        
        bool prev = pop_maintenance_mode_.exchange(enabled);
        if (prev != enabled) {
            spdlog::info("SharedStateManager: Pop maintenance mode changed {} -> {} (from DB refresh)", 
                        prev ? "ON" : "OFF", enabled ? "ON" : "OFF");
        }
        
    } catch (const std::exception& e) {
        spdlog::warn("SharedStateManager: Failed to refresh pop maintenance mode from DB: {}", e.what());
    }
}

// ============================================================
// Maintenance Mode (PUSH)
// ============================================================

void SharedStateManager::set_maintenance_mode(bool enabled) {
    bool prev = maintenance_mode_.exchange(enabled);
    
    if (prev != enabled) {
        spdlog::info("SharedStateManager: Maintenance mode set {} -> {}", 
                    prev ? "ON" : "OFF", enabled ? "ON" : "OFF");
    }
    
    // Persist to database
    if (db_pool_) {
        try {
            auto conn = db_pool_->acquire();
            if (conn) {
                std::string sql = R"(
                    INSERT INTO queen.system_state (key, value, updated_at)
                    VALUES ('maintenance_mode', $1::jsonb, NOW())
                    ON CONFLICT (key) DO UPDATE SET 
                        value = EXCLUDED.value,
                        updated_at = NOW()
                )";
                
                std::string value_json = enabled ? R"({"enabled": true})" : R"({"enabled": false})";
                
                sendQueryParamsAsync(conn.get(), sql, {value_json});
                getCommandResult(conn.get());
            }
        } catch (const std::exception& e) {
            spdlog::warn("SharedStateManager: Failed to persist maintenance mode to DB: {}", e.what());
        }
    }
    
    // Broadcast to other instances
    if (running_ && transport_) {
        nlohmann::json payload = {
            {"enabled", enabled},
            {"ts", now_ms()}
        };
        transport_->broadcast(UDPSyncMessageType::MAINTENANCE_MODE_SET, payload);
    }
}

void SharedStateManager::set_pop_maintenance_mode(bool enabled) {
    bool prev = pop_maintenance_mode_.exchange(enabled);
    
    if (prev != enabled) {
        spdlog::info("SharedStateManager: Pop maintenance mode set {} -> {}", 
                    prev ? "ON" : "OFF", enabled ? "ON" : "OFF");
    }
    
    // Persist to database
    if (db_pool_) {
        try {
            auto conn = db_pool_->acquire();
            if (conn) {
                std::string sql = R"(
                    INSERT INTO queen.system_state (key, value, updated_at)
                    VALUES ('pop_maintenance_mode', $1::jsonb, NOW())
                    ON CONFLICT (key) DO UPDATE SET 
                        value = EXCLUDED.value,
                        updated_at = NOW()
                )";
                
                std::string value_json = enabled ? R"({"enabled": true})" : R"({"enabled": false})";
                
                sendQueryParamsAsync(conn.get(), sql, {value_json});
                getCommandResult(conn.get());
            }
        } catch (const std::exception& e) {
            spdlog::warn("SharedStateManager: Failed to persist pop maintenance mode to DB: {}", e.what());
        }
    }
    
    // Broadcast to other instances
    if (running_ && transport_) {
        nlohmann::json payload = {
            {"enabled", enabled},
            {"ts", now_ms()}
        };
        transport_->broadcast(UDPSyncMessageType::POP_MAINTENANCE_MODE_SET, payload);
    }
}

// ============================================================
// Stats
// ============================================================

nlohmann::json SharedStateManager::get_stats() const {
    nlohmann::json stats;
    
    stats["enabled"] = enabled_;
    stats["running"] = running_.load();
    stats["server_id"] = server_id_;
    
    if (transport_) {
        stats["transport"] = transport_->get_stats();
    }
    
    stats["queue_config_cache"] = {
        {"size", queue_configs_.size()},
        {"hits", queue_configs_.hits()},
        {"misses", queue_configs_.misses()},
        {"hit_rate", queue_configs_.hit_rate()}
    };
    
    stats["server_health"] = {
        {"servers_tracked", server_health_.server_count()},
        {"servers_alive", server_health_.get_alive_servers().size()},
        {"servers_dead", server_health_.get_dead_servers().size()},
        {"heartbeats_received", server_health_.heartbeats_received()},
        {"restarts_detected", server_health_.restarts_detected()}
    };
    
    stats["maintenance_mode"] = maintenance_mode_.load();
    stats["pop_maintenance_mode"] = pop_maintenance_mode_.load();
    
    return stats;
}

} // namespace queen
