#include "queen/async_queue_manager.hpp"
#include "queen/file_buffer.hpp"
#include "queen/encryption.hpp"
#include "queen/shared_state_manager.hpp"
#include "queen/storage_v2.hpp"
#include <spdlog/spdlog.h>

// External global for shared state
namespace queen {
extern std::shared_ptr<SharedStateManager> global_shared_state;
}
#include <random>
#include <iomanip>
#include <sstream>
#include <thread>
#include <chrono>
#include <numeric>
#include <map>
#include <fstream>
#include <filesystem>
#include <algorithm>

namespace queen {

// --- Constructor ---

AsyncQueueManager::AsyncQueueManager(std::shared_ptr<AsyncDbPool> async_db_pool, 
                                   const QueueConfig& config, 
                                   const std::string& schema_name)
    : async_db_pool_(async_db_pool), config_(config), schema_name_(schema_name) {
    if (!async_db_pool_) {
        throw std::invalid_argument("Async database pool cannot be null");
    }
}

// --- UUID Generation (same as QueueManager) ---

std::string AsyncQueueManager::generate_uuid() {
    // Per-thread state: no global mutex (handlers run one-per-worker-thread).
    // Monotonicity is per-thread; uniqueness across threads is guaranteed by the
    // 62 random bits. Hand-rolled hex avoids std::stringstream.
    static thread_local uint64_t last_ms = 0;
    static thread_local uint16_t sequence = 0;
    static thread_local std::mt19937_64 gen(
        (static_cast<uint64_t>(std::random_device{}()) << 32) ^ std::random_device{}());

    auto now = std::chrono::system_clock::now();
    uint64_t current_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

    if (current_ms <= last_ms) {
        sequence++;
    } else {
        last_ms = current_ms;
        sequence = 0;
    }

    std::array<uint8_t, 16> bytes;
    bytes[0] = (last_ms >> 40) & 0xFF;
    bytes[1] = (last_ms >> 32) & 0xFF;
    bytes[2] = (last_ms >> 24) & 0xFF;
    bytes[3] = (last_ms >> 16) & 0xFF;
    bytes[4] = (last_ms >> 8) & 0xFF;
    bytes[5] = last_ms & 0xFF;

    uint16_t sequence_and_version = sequence & 0x0FFF;
    bytes[6] = 0x70 | (sequence_and_version >> 8);
    bytes[7] = sequence_and_version & 0xFF;

    uint64_t rand_data = gen();
    bytes[8]  = 0x80 | ((rand_data >> 56) & 0x3F);
    bytes[9]  = (rand_data >> 48) & 0xFF;
    bytes[10] = (rand_data >> 40) & 0xFF;
    bytes[11] = (rand_data >> 32) & 0xFF;
    bytes[12] = (rand_data >> 24) & 0xFF;
    bytes[13] = (rand_data >> 16) & 0xFF;
    bytes[14] = (rand_data >> 8) & 0xFF;
    bytes[15] = rand_data & 0xFF;

    static const char hexd[] = "0123456789abcdef";
    char buf[36];
    int p = 0;
    for (int i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) buf[p++] = '-';
        buf[p++] = hexd[bytes[i] >> 4];
        buf[p++] = hexd[bytes[i] & 0x0F];
    }
    return std::string(buf, p);
}

std::string AsyncQueueManager::generate_transaction_id() {
    return generate_uuid();
}

// --- Schema Initialization ---

namespace {

// Helper: Read file contents
std::string read_sql_file(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open SQL file: " + path);
    }
    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

// Helper: Execute SQL (blocking, handles multi-statement)
bool exec_sql(PGconn* conn, const std::string& sql, const std::string& context = "") {
    if (!PQsendQuery(conn, sql.c_str())) {
        spdlog::error("[{}] PQsendQuery failed: {}", context, PQerrorMessage(conn));
        return false;
    }
    
    while (PQisBusy(conn)) {
        if (!PQconsumeInput(conn)) {
            spdlog::error("[{}] PQconsumeInput failed: {}", context, PQerrorMessage(conn));
            return false;
        }
    }
    
    bool success = true;
    PGresult* res;
    while ((res = PQgetResult(conn)) != nullptr) {
        auto status = PQresultStatus(res);
        if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
            spdlog::error("[{}] SQL failed: {}", context, PQresultErrorMessage(res));
            success = false;
        }
        PQclear(res);
    }
    return success;
}

// Helper: Query single integer value
int query_int(PGconn* conn, const std::string& sql, int default_val = -1) {
    PGresult* res = PQexec(conn, sql.c_str());
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) PQclear(res);
        return default_val;
    }
    int val = default_val;
    if (PQntuples(res) > 0 && PQnfields(res) > 0) {
        val = std::atoi(PQgetvalue(res, 0, 0));
    }
    PQclear(res);
    return val;
}

// Helper: Get sorted SQL files from directory
std::vector<std::string> get_sql_files(const std::string& dir) {
    std::vector<std::string> files;
    if (!std::filesystem::exists(dir)) return files;
    
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.path().extension() == ".sql") {
            files.push_back(entry.path().string());
        }
    }
    std::sort(files.begin(), files.end());
    return files;
}

} // anonymous namespace

bool AsyncQueueManager::initialize_schema() {
    try {
        auto conn = async_db_pool_->acquire();
        if (!conn) {
            spdlog::error("Failed to acquire connection for schema initialization");
            return false;
        }

        spdlog::info("Initializing schema: {}", schema_name_);

        // Concurrent broker boots apply schema.sql + every procedures file at
        // the same time; parallel CREATE OR REPLACE FUNCTION on the same
        // function trips PostgreSQL's low-level catalog race ("tuple
        // concurrently updated") and fails the boot. Serialize the whole
        // apply behind a session advisory lock (blocks until the peer's apply
        // finishes — the apply is idempotent, so last writer wins is fine).
        // Declared AFTER conn so the guard releases the lock before the
        // connection returns to the pool, on every exit path including
        // exceptions.
        if (!exec_sql(conn.get(),
                      "SELECT pg_advisory_lock(hashtext('queen_schema_apply'))",
                      "schema apply advisory lock")) {
            spdlog::error("Failed to acquire schema apply advisory lock");
            return false;
        }
        struct SchemaLockGuard {
            PGconn* conn;
            ~SchemaLockGuard() {
                exec_sql(conn,
                         "SELECT pg_advisory_unlock(hashtext('queen_schema_apply'))",
                         "schema apply advisory unlock");
            }
        } schema_lock_guard{conn.get()};
        
        // Get base paths (relative to executable or absolute)
        std::string schema_dir = "schema";
        std::string migrations_dir = "migrations";
        
        // Check common locations (lib/schema is where libqueen stores schema files)
        for (const auto& base : {".", "..", "lib", "../lib", "server", "../server"}) {
            if (std::filesystem::exists(std::string(base) + "/schema/schema.sql")) {
                schema_dir = std::string(base) + "/schema";
                migrations_dir = std::string(base) + "/migrations";
                break;
            }
        }
        
        // Create queen schema first
        if (!exec_sql(conn.get(), "CREATE SCHEMA IF NOT EXISTS queen", "create schema")) {
            spdlog::error("Failed to create queen schema");
            return false;
        }

        // If no schema version, this is a fresh install - run base schema
        spdlog::info("Running base schema initialization...");
            
        std::string schema_file = schema_dir + "/schema.sql";
        if (!std::filesystem::exists(schema_file)) {
            spdlog::error("Base schema file not found: {}", schema_file);
            // Fall back to legacy embedded schema
            throw std::runtime_error("Base schema file not found: " + schema_file);
        }
            
        // Load and execute base schema
        std::string schema_sql = read_sql_file(schema_file);
        if (!exec_sql(conn.get(), schema_sql, "base schema")) {
            spdlog::error("Failed to apply base schema");
            return false;
        }
        spdlog::info("Base schema applied successfully");

        
        // Always load/update stored procedures (they use CREATE OR REPLACE, so idempotent)
        {
            std::string procedures_dir = schema_dir + "/procedures";
            auto procedure_files = get_sql_files(procedures_dir);
            if (!procedure_files.empty()) {
                spdlog::info("Loading {} stored procedure(s)...", procedure_files.size());
                for (const auto& proc_file : procedure_files) {
                    spdlog::debug("Loading stored procedure: {}", proc_file);
                    std::string proc_sql = read_sql_file(proc_file);
                    if (!exec_sql(conn.get(), proc_sql, proc_file)) {
                        spdlog::error("Failed to load procedure: {}", proc_file);
                        return false;
                    }
                }
            }
        }

        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to initialize schema: {}", e.what());
        return false;
    }
}

// --- Maintenance Mode ---

bool AsyncQueueManager::check_maintenance_mode_with_cache() {
    // Check if cache is still valid (reduces DB churn from 10 queries/sec to 1 query/sec)
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()
    ).count();
    
    uint64_t last_check = last_maintenance_check_ms_.load();
    if (last_check > 0 && (now_ms - last_check) < MAINTENANCE_CACHE_TTL_MS) {
        // Cache is still valid, return cached value
        return maintenance_mode_cached_.load();
    }
    
    // Cache expired or first check - query database
    try {
        auto conn = async_db_pool_->acquire();
        
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
        
        // Update cache
        maintenance_mode_cached_.store(enabled);
        last_maintenance_check_ms_.store(now_ms);
        
        return enabled;
    } catch (const std::exception& e) {
        spdlog::error("Failed to check maintenance mode from database: {}", e.what());
        // On DB error, return cached value as fallback
        return maintenance_mode_cached_.load();
    }
}

// --- Queue/Partition Management ---

bool AsyncQueueManager::ensure_queue_exists(PGconn* conn,
                                           const std::string& queue_name, 
                                           const std::string& namespace_name,
                                           const std::string& task_name) {
    try {
        std::string sql = R"(
            INSERT INTO queen.queues (name, namespace, task, priority, lease_time, retry_limit, retry_delay, max_queue_size, ttl)
            VALUES ($1, $2, $3, 0, 300, 3, 1000, 0, 3600)
            ON CONFLICT (name) DO NOTHING
        )";
        
        std::vector<std::string> params = {
            queue_name,
            namespace_name.empty() ? "" : namespace_name,
            task_name.empty() ? "" : task_name
        };
        
        sendQueryParamsAsync(conn, sql, params);
        getCommandResult(conn);
        
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to ensure queue exists: {}", e.what());
        return false;
    }
}

bool AsyncQueueManager::ensure_partition_exists(PGconn* conn,
                                               const std::string& queue_name, 
                                               const std::string& partition_name) {
    try {
        spdlog::debug("Ensuring partition exists: queue='{}', partition='{}'", queue_name, partition_name);
        
        std::string sql = R"(
            INSERT INTO queen.partitions (queue_id, name)
            SELECT id, $2 FROM queen.queues WHERE name = $1
            ON CONFLICT (queue_id, name) DO NOTHING
            RETURNING id, name
        )";
        
        std::vector<std::string> params = {queue_name, partition_name};
        sendQueryParamsAsync(conn, sql, params);
        
        auto result = getTuplesResult(conn);
        
        if (PQntuples(result.get()) > 0) {
            spdlog::debug("Partition created: id='{}', name='{}'", 
                         PQgetvalue(result.get(), 0, 0), 
                         PQgetvalue(result.get(), 0, 1));
        } else {
            spdlog::debug("Partition already exists (ON CONFLICT triggered)");
        }
        
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to ensure partition exists: {}", e.what());
        return false;
    }
}

// --- Helper for FileBufferManager ---

void AsyncQueueManager::push_single_message(
    const std::string& queue_name,
    const std::string& partition_name,
    const nlohmann::json& payload,
    const std::string& namespace_name,
    const std::string& task,
    const std::string& transaction_id,
    const std::string& trace_id
) {
    auto conn = async_db_pool_->acquire();
    
    // Generate transaction ID if not provided
    std::string tx_id = transaction_id.empty() ? generate_uuid() : transaction_id;
    
    // Ensure queue exists
    ensure_queue_exists(conn.get(), queue_name, namespace_name, task);
    
    // Ensure partition exists
    ensure_partition_exists(conn.get(), queue_name, partition_name);
    
    // Get next sequence number
    std::string seq_sql = R"(
        SELECT COALESCE(MAX(sequence), 0) + 1 as next_seq
        FROM queen.messages
        WHERE queue_name = $1 AND partition_name = $2
    )";
    
    sendQueryParamsAsync(conn.get(), seq_sql, {queue_name, partition_name});
    auto seq_result = getTuplesResult(conn.get());
    
    int64_t sequence = 1;
    if (PQntuples(seq_result.get()) > 0) {
        const char* seq_str = PQgetvalue(seq_result.get(), 0, 0);
        sequence = seq_str ? std::stoll(seq_str) : 1;
    }
    
    // Check if queue has encryption enabled
    bool encryption_enabled = false;
    if (global_shared_state) {
        auto cached_config = global_shared_state->get_queue_config(queue_name);
        if (cached_config) {
            encryption_enabled = cached_config->encryption_enabled;
        }
    }
    
    // Encrypt payload if needed
    nlohmann::json payload_to_store = payload;
    bool is_encrypted = false;
    
    if (encryption_enabled) {
        EncryptionService* enc_service = get_encryption_service();
        if (enc_service && enc_service->is_enabled()) {
    std::string payload_str = payload.dump();
            auto encrypted = enc_service->encrypt_payload(payload_str);
            if (encrypted.has_value()) {
                payload_to_store = {
                    {"encrypted", encrypted->encrypted},
                    {"iv", encrypted->iv},
                    {"authTag", encrypted->auth_tag}
                };
                is_encrypted = true;
            }
        }
    }
    
    // Serialize payload (now potentially encrypted)
    std::string payload_str = payload_to_store.dump();
    
    // Insert message
    std::string insert_sql = R"(
        INSERT INTO queen.messages 
        (queue_name, partition_name, payload, transaction_id, trace_id, 
         namespace, task, status, sequence, is_encrypted, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9, NOW())
    )";
    
    sendQueryParamsAsync(conn.get(), insert_sql, {
        queue_name,
        partition_name,
        payload_str,
        tx_id,
        trace_id,
        namespace_name,
        task,
        std::to_string(sequence),
        is_encrypted ? "true" : "false"
    });
    
    getCommandResult(conn.get());
}

// --- Push Single Message ---

PushResult AsyncQueueManager::push_single_message(const PushItem& item) {
    PushResult result;
    result.transaction_id = item.transaction_id.value_or(generate_transaction_id());
    
    // MAINTENANCE MODE: Route to file buffer
    if (check_maintenance_mode_with_cache() && file_buffer_manager_) {
        try {
            nlohmann::json event = {
                {"queue", item.queue},
                {"partition", item.partition},
                {"payload", item.payload},
                {"transactionId", result.transaction_id},
                {"failover", true}
            };
            
            if (item.trace_id.has_value() && !item.trace_id->empty()) {
                event["traceId"] = *item.trace_id;
            }

            if (item.producer_sub.has_value() && !item.producer_sub->empty()) {
                event["producerSub"] = *item.producer_sub;
            }

            if (file_buffer_manager_->write_event(event)) {
                result.status = "buffered";
                result.message_id = std::nullopt;
                result.trace_id = item.trace_id;
                return result;
            } else {
                result.status = "failed";
                result.error = "File buffer write failed";
                return result;
            }
        } catch (const std::exception& e) {
            spdlog::error("Failed to write to file buffer during maintenance mode: {}", e.what());
            result.status = "failed";
            result.error = std::string("Buffer write error: ") + e.what();
            return result;
        }
    }
    
    try {
        auto conn = async_db_pool_->acquire();
        
        // Check if queue exists
        sendQueryParamsAsync(conn.get(), "SELECT id FROM queen.queues WHERE name = $1", {item.queue});
        auto queue_check = getTuplesResult(conn.get());
        
        if (PQntuples(queue_check.get()) == 0) {
            result.status = "failed";
            result.error = "Queue '" + item.queue + "' does not exist. Please create it using the configure endpoint first.";
            return result;
        }
        
        // Ensure partition exists
        if (!ensure_partition_exists(conn.get(), item.queue, item.partition)) {
            result.status = "failed";
            result.error = "Failed to create partition";
            return result;
        }
        
        // Generate UUIDv7 for message ID
        std::string message_id = generate_uuid();
        
        std::string sql;
        std::vector<std::string> params;
        
        // Use NULL placeholder (empty string passed as NULL via $N::<type>) to uniformly handle
        // optional trace_id and producer_sub without exploding the number of SQL branches.
        std::string trace_id_param = item.trace_id.value_or("");
        std::string producer_sub_param = item.producer_sub.value_or("");

        if (!trace_id_param.empty()) {
            sql = R"(
                INSERT INTO queen.messages (
                    id, transaction_id, partition_id, payload, trace_id, producer_sub, created_at
                )
                SELECT $1, $2, p.id, $3, $4, NULLIF($5, ''), NOW()
                FROM queen.partitions p
                JOIN queen.queues q ON q.id = p.queue_id
                WHERE q.name = $6 AND p.name = $7
                RETURNING id, trace_id
            )";

            params = {
                message_id,
                result.transaction_id,
                item.payload.dump(),
                trace_id_param,
                producer_sub_param,
                item.queue,
                item.partition
            };
        } else {
            sql = R"(
                INSERT INTO queen.messages (
                    id, transaction_id, partition_id, payload, producer_sub, created_at
                )
                SELECT $1, $2, p.id, $3, NULLIF($4, ''), NOW()
                FROM queen.partitions p
                JOIN queen.queues q ON q.id = p.queue_id
                WHERE q.name = $5 AND p.name = $6
                RETURNING id, trace_id
            )";

            params = {
                message_id,
                result.transaction_id,
                item.payload.dump(),
                producer_sub_param,
                item.queue,
                item.partition
            };
        }
        
        sendQueryParamsAsync(conn.get(), sql, params);
        auto query_result = getTuplesResult(conn.get());
        
        if (PQntuples(query_result.get()) > 0) {
            result.status = "queued";
            result.message_id = PQgetvalue(query_result.get(), 0, 0);
            const char* trace_val = PQgetvalue(query_result.get(), 0, 1);
            if (trace_val && std::string(trace_val).length() > 0) {
                result.trace_id = trace_val;
            }
        } else {
            result.status = "failed";
            result.error = "Insert returned no result";
        }
        
    } catch (const std::exception& e) {
        // Database push failed - try file buffer failover
        spdlog::warn("Database push failed: {}. Attempting file buffer failover...", e.what());
        
        if (file_buffer_manager_) {
            // Mark database as unhealthy for faster failover on subsequent requests
            file_buffer_manager_->mark_db_unhealthy();
            
            try {
                nlohmann::json event = {
                    {"queue", item.queue},
                    {"partition", item.partition},
                    {"payload", item.payload},
                    {"transactionId", result.transaction_id},
                    {"failover", true}  // Failover buffer (FIFO, will be replayed when DB recovers)
                };
                
                if (item.trace_id.has_value() && !item.trace_id->empty()) {
                    event["traceId"] = *item.trace_id;
                }

                if (item.producer_sub.has_value() && !item.producer_sub->empty()) {
                    event["producerSub"] = *item.producer_sub;
                }

                if (file_buffer_manager_->write_event(event)) {
                    spdlog::info("Message successfully failed over to file buffer");
                    result.status = "buffered";
                    result.message_id = std::nullopt;
                    result.trace_id = item.trace_id;
                    result.error = std::nullopt;  // Clear error since failover succeeded
                    return result;
                } else {
                    spdlog::error("File buffer write failed during failover");
                    result.status = "failed";
                    result.error = std::string("Database error: ") + e.what() + "; File buffer also failed";
                    return result;
                }
            } catch (const std::exception& fb_error) {
                spdlog::error("File buffer failover exception: {}", fb_error.what());
                result.status = "failed";
                result.error = std::string("Database error: ") + e.what() + "; Failover error: " + fb_error.what();
                return result;
            }
        } else {
            // No file buffer configured - return database error
            spdlog::error("Database push failed and no file buffer configured: {}", e.what());
            result.status = "failed";
            result.error = e.what();
            return result;
        }
    }
    
    return result;
}
// --- Push Messages Internal (Bypasses Maintenance Mode) ---

std::vector<PushResult> AsyncQueueManager::push_messages_internal(const std::vector<PushItem>& items) {
    if (items.empty()) {
        return {};
    }
    
    // NOTE: This method NEVER checks maintenance mode
    // It's used ONLY for internal operations like file buffer drain
    // DO NOT call from user-facing code paths!
    
    spdlog::debug("push_messages_internal: Processing {} items (bypassing maintenance mode)", items.size());
    
    // Group items by queue and partition while PRESERVING ORDER
    std::vector<std::pair<std::string, std::vector<size_t>>> partition_groups_ordered;
    std::map<std::string, size_t> key_to_index;
    
    for (size_t i = 0; i < items.size(); ++i) {
        const auto& item = items[i];
        std::string key = item.queue + ":" + item.partition;
        
        if (key_to_index.find(key) == key_to_index.end()) {
            key_to_index[key] = partition_groups_ordered.size();
            partition_groups_ordered.push_back({key, {i}});
        } else {
            size_t group_idx = key_to_index[key];
            partition_groups_ordered[group_idx].second.push_back(i);
        }
    }
    
    std::vector<PushResult> all_results(items.size());
    
    // Batch capacity check - check ALL queues in ONE query
    try {
        // Build map of queue -> total batch size across all partitions
        std::map<std::string, int> queue_batch_sizes;
        for (const auto& [partition_key, item_indices] : partition_groups_ordered) {
            size_t colon_pos = partition_key.find(':');
            std::string queue_name = partition_key.substr(0, colon_pos);
            queue_batch_sizes[queue_name] += item_indices.size();
        }
        
        // Check capacity for all queues with max_queue_size set
        if (!queue_batch_sizes.empty()) {
            std::vector<std::string> queues_to_check;
            for (const auto& [queue_name, _] : queue_batch_sizes) {
                queues_to_check.push_back(queue_name);
            }
            
            // Build PostgreSQL array
            auto build_pg_array = [](const std::vector<std::string>& vec) -> std::string {
                std::string result = "{";
                for (size_t i = 0; i < vec.size(); ++i) {
                    if (i > 0) result += ",";
                    result += "\"";
                    for (char c : vec[i]) {
                        if (c == '\\' || c == '"') result += '\\';
                        result += c;
                    }
                    result += "\"";
                }
                result += "}";
                return result;
            };
            
            std::string queues_array = build_pg_array(queues_to_check);
            
            auto capacity_conn = async_db_pool_->acquire();
            std::string capacity_check_sql = R"(
                SELECT 
                  q.name,
                  q.max_queue_size,
                  (
                    SELECT COUNT(m.id)::integer
                    FROM queen.messages m
                    JOIN queen.partitions p ON p.id = m.partition_id
                    LEFT JOIN queen.partition_consumers pc ON pc.partition_id = p.id
                      AND pc.consumer_group = '__QUEUE_MODE__'
                    WHERE p.queue_id = q.id
                           AND (pc.last_consumed_created_at IS NULL 
                                OR m.created_at > pc.last_consumed_created_at
                           OR (m.created_at = pc.last_consumed_created_at AND m.id > pc.last_consumed_id))
                  ) as current_depth
                FROM queen.queues q
                WHERE q.name = ANY($1::varchar[])
                  AND q.max_queue_size > 0
            )";
            
            sendQueryParamsAsync(capacity_conn.get(), capacity_check_sql, {queues_array});
            auto capacity_result = getTuplesResult(capacity_conn.get());
            
            // Check each queue's capacity
            for (int i = 0; i < PQntuples(capacity_result.get()); ++i) {
                std::string queue_name = PQgetvalue(capacity_result.get(), i, 0);
                const char* max_size_str = PQgetvalue(capacity_result.get(), i, 1);
                const char* current_depth_str = PQgetvalue(capacity_result.get(), i, 2);
                
                int max_size = (max_size_str && strlen(max_size_str) > 0) ? std::stoi(max_size_str) : 0;
                int current_depth = (current_depth_str && strlen(current_depth_str) > 0) ? std::stoi(current_depth_str) : 0;
                int batch_size = queue_batch_sizes[queue_name];
                
                if (current_depth + batch_size > max_size) {
                    // Queue would exceed capacity - fail all messages for this queue
                    std::string error_msg = "Queue '" + queue_name + "' would exceed max capacity (" +
                                          std::to_string(max_size) + "). Current: " + 
                                          std::to_string(current_depth) + ", Batch: " + 
                                          std::to_string(batch_size);
                    
                    spdlog::warn(error_msg);
                    
                    // Find all items for this queue and mark them as failed
                    for (size_t item_idx = 0; item_idx < items.size(); ++item_idx) {
                        if (items[item_idx].queue == queue_name) {
                            PushResult failed_result;
                            failed_result.transaction_id = items[item_idx].transaction_id.value_or(generate_transaction_id());
                            failed_result.status = "failed";
                            failed_result.error = error_msg;
                            all_results[item_idx] = failed_result;
                        }
                    }
                    
                    // Remove this queue from partition_groups_ordered
                    partition_groups_ordered.erase(
                        std::remove_if(partition_groups_ordered.begin(), partition_groups_ordered.end(),
                            [&queue_name](const auto& pair) {
                                return pair.first.substr(0, pair.first.find(':')) == queue_name;
                            }),
                        partition_groups_ordered.end()
                    );
                }
            }
        }
    } catch (const std::exception& e) {
        spdlog::error("Batch capacity check failed: {}", e.what());
        // Continue with push - individual checks will catch issues
    }
    
    // Process each partition group using stored procedure
    for (const auto& [partition_key, item_indices] : partition_groups_ordered) {
    try {
            // STORAGE V2: route each (queue, partition) group by the queue's
            // CURRENT storage flag (fetched like push.cpp does). Buffered
            // events are storage-agnostic; the engine decision belongs here,
            // at replay time. v1 'rows' groups fall through untouched.
            size_t sep_pos = partition_key.find(':');
            std::string group_queue = partition_key.substr(0, sep_pos);
            std::string group_partition = partition_key.substr(sep_pos + 1);
            std::string group_storage = "rows";
            if (global_shared_state) {
                auto storage_cfg = global_shared_state->get_or_fetch_queue_config(group_queue);
                if (storage_cfg) group_storage = storage_cfg->storage;
            }

        auto conn = async_db_pool_->acquire();

            if (group_storage == "segments") {
                // ---- segments replay path -----------------------------------
                // Mirrors routes_v2::handle_push_v2 semantics: intra-batch
                // duplicate txns resolve first-wins BEFORE packing (the blob
                // must not contain them; v1's dup_rank rule); dedup-window
                // duplicates come back as {status:'duplicate', dups:[{i,mid}]}
                // with nothing written, so the group is repacked without the
                // dup frames and retried (rare path by design). Payloads pass
                // through opaque — no encryption on segment queues (slice 1;
                // configure rejects the combination).
                struct LiveFrame { size_t item_idx; storage_v2::FrameIn frame; };
                std::vector<LiveFrame> live;
                live.reserve(item_indices.size());
                std::map<std::string, std::string> seen_txn;  // txn -> first message id

                for (size_t idx : item_indices) {
                    const auto& item = items[idx];
                    storage_v2::FrameIn f;
                    f.message_id = generate_uuid();
                    f.transaction_id = item.transaction_id.value_or(generate_transaction_id());

                    PushResult result;
                    result.transaction_id = f.transaction_id;

                    auto seen = seen_txn.find(f.transaction_id);
                    if (seen != seen_txn.end()) {
                        result.status = "duplicate";       // first occurrence wins
                        result.message_id = seen->second;
                        all_results[idx] = result;
                        continue;
                    }
                    seen_txn[f.transaction_id] = f.message_id;

                    if (item.trace_id.has_value() && !item.trace_id->empty()) {
                        f.trace_id = *item.trace_id;
                    }
                    if (item.producer_sub.has_value() && !item.producer_sub->empty()) {
                        f.producer_sub = *item.producer_sub;
                    }
                    f.payload = item.payload.dump();

                    result.status = "queued";              // provisional; dedup below
                    result.message_id = f.message_id;
                    all_results[idx] = result;
                    live.push_back({idx, std::move(f)});
                }

                static const std::string seg_sql =
                    "SELECT q2.push_segment_wire_v1($1, $2, $3::jsonb, $4, $5::int)";

                while (!live.empty()) {
                    nlohmann::json metas = nlohmann::json::array();
                    std::vector<storage_v2::FrameIn> frames;
                    frames.reserve(live.size());
                    for (size_t k = 0; k < live.size(); ++k) {
                        metas.push_back({{"i", static_cast<int>(k)},
                                         {"mid", live[k].frame.message_id},
                                         {"txn", live[k].frame.transaction_id}});
                        frames.push_back(live[k].frame);   // copy: a dup retry repacks
                    }
                    auto blob = storage_v2::zstd_compress(storage_v2::pack_frames(frames));
                    if (blob.empty()) {
                        throw std::runtime_error("segment compression failed");
                    }
                    std::string blob_b64 = storage_v2::b64_encode(blob.data(), blob.size());

                    sendQueryParamsAsync(conn.get(), seg_sql,
                                         {group_queue, group_partition, metas.dump(),
                                          blob_b64, std::to_string(frames.size())});
                    auto seg_result = getTuplesResult(conn.get());
                    if (PQntuples(seg_result.get()) == 0) {
                        throw std::runtime_error("push_segment_wire_v1 returned no rows");
                    }

                    auto sp_result = nlohmann::json::parse(PQgetvalue(seg_result.get(), 0, 0));
                    if (sp_result.contains("error") && !sp_result["error"].is_null()) {
                        throw std::runtime_error(sp_result["error"].is_string()
                            ? sp_result["error"].get<std::string>()
                            : sp_result["error"].dump());
                    }

                    if (sp_result.value("status", "") == "duplicate") {
                        // Frame index -> original message id (from the dedup
                        // window); mark dups, drop them, repack, retry.
                        std::vector<char> is_dup(live.size(), 0);
                        size_t dup_count = 0;
                        for (const auto& d : sp_result["dups"]) {
                            int i = d.value("i", -1);
                            if (i < 0 || i >= static_cast<int>(live.size()) || is_dup[i]) continue;
                            is_dup[i] = 1;
                            dup_count++;
                            all_results[live[i].item_idx].status = "duplicate";
                            all_results[live[i].item_idx].message_id = d.value("mid", "");
                        }
                        if (dup_count == 0) {
                            // Defensive: duplicate status without a dup list
                            // would retry the same blob forever.
                            throw std::runtime_error(
                                "push_segment_wire_v1 reported duplicates without a dup list");
                        }
                        std::vector<LiveFrame> next;
                        next.reserve(live.size() - dup_count);
                        for (size_t k = 0; k < live.size(); ++k) {
                            if (!is_dup[k]) next.push_back(std::move(live[k]));
                        }
                        live = std::move(next);
                        continue;
                    }

                    break;  // queued: provisional per-item results already correct
                }

                continue;   // next partition group (v1 path below untouched)
            }

            // Build JSON array for stored procedure
            nlohmann::json messages_array = nlohmann::json::array();
            
            for (size_t idx : item_indices) {
                const auto& item = items[idx];
                std::string msg_id = generate_uuid();
                std::string txn_id = item.transaction_id.value_or(generate_transaction_id());
                
                // Check if queue has encryption enabled
                // Note: Check SharedStateManager cache even when UDP sync is disabled
                // (queue configs are loaded from DB on startup regardless of sync status)
        bool encryption_enabled = false;
                if (global_shared_state) {
                    auto cached_config = global_shared_state->get_queue_config(item.queue);
            if (cached_config) {
                encryption_enabled = cached_config->encryption_enabled;
                    }
            }
                
                // Encrypt payload if needed - store as JSON object, not string!
                nlohmann::json payload_to_store = item.payload;
                bool is_encrypted = false;
                
                if (encryption_enabled) {
                    EncryptionService* enc_service = get_encryption_service();
                if (enc_service && enc_service->is_enabled()) {
                        std::string payload_str = item.payload.dump();
                        auto encrypted = enc_service->encrypt_payload(payload_str);
                    if (encrypted.has_value()) {
                            // Store as JSON object (matching normal push.cpp behavior)
                            payload_to_store = {
                            {"encrypted", encrypted->encrypted},
                            {"iv", encrypted->iv},
                            {"authTag", encrypted->auth_tag}
                        };
                        is_encrypted = true;
                        }
                    }
                }
                
                nlohmann::json msg_obj;
                msg_obj["messageId"] = msg_id;
                msg_obj["transactionId"] = txn_id;
                msg_obj["queue"] = item.queue;
                msg_obj["partition"] = item.partition;
                msg_obj["payload"] = payload_to_store;  // JSON object, not string!
                msg_obj["is_encrypted"] = is_encrypted;
                if (item.trace_id.has_value() && !item.trace_id->empty()) {
                    msg_obj["traceId"] = *item.trace_id;
                }
                if (item.producer_sub.has_value() && !item.producer_sub->empty()) {
                    msg_obj["producerSub"] = *item.producer_sub;
                }

                messages_array.push_back(msg_obj);
                
                // Pre-populate result
                PushResult result;
                result.transaction_id = txn_id;
                result.message_id = msg_id;
                result.status = "queued";
                all_results[idx] = result;
                }
                
            // Call stored procedure (v3: CTE pipeline, no catalog DDL).
            static const std::string sql = "SELECT queen.push_messages_v3($1::jsonb)";
            sendQueryParamsAsync(conn.get(), sql, {messages_array.dump()});
            auto result = getTuplesResult(conn.get());
            
            if (PQntuples(result.get()) > 0) {
                const char* result_str = PQgetvalue(result.get(), 0, 0);
                if (result_str && strlen(result_str) > 0) {
                    try {
                        nlohmann::json sp_result = nlohmann::json::parse(result_str);
                        
                        if (sp_result.contains("error")) {
                            std::string error_msg = sp_result["error"].get<std::string>();
                            for (size_t idx : item_indices) {
                                all_results[idx].status = "failed";
                                all_results[idx].error = error_msg;
                    }
                }
                    } catch (...) {
                        // SP returned success without detailed JSON
                    }
                }
            }
            
        } catch (const std::exception& e) {
            spdlog::error("push_messages_internal failed for partition group: {}", e.what());
            for (size_t idx : item_indices) {
                all_results[idx].status = "failed";
                all_results[idx].error = e.what();
            }
        }
    }
    
    return all_results;
        }
        
// --- Helper: Check if error is connection-related (not retryable via cache invalidation) ---
static bool is_connection_error(const std::string& error_msg) {
    // Connection errors should NOT trigger cache invalidation retry
    // These indicate the DB is down, not that cache is stale
    return error_msg.find("connection") != std::string::npos ||
           error_msg.find("Connection") != std::string::npos ||
           error_msg.find("server closed") != std::string::npos ||
           error_msg.find("could not connect") != std::string::npos ||
           error_msg.find("timeout") != std::string::npos ||
           error_msg.find("SSL") != std::string::npos;
}

// --- Pool Statistics ---

AsyncQueueManager::PoolStats AsyncQueueManager::get_pool_stats() const {
    return {
        async_db_pool_->size(),
        async_db_pool_->available(),
        async_db_pool_->size() - async_db_pool_->available()
    };
}

// --- Health Check ---

bool AsyncQueueManager::health_check() {
    try {
        auto conn = async_db_pool_->acquire();
        sendAndWait(conn.get(), "SELECT 1");
        auto result = getTuplesResult(conn.get());
        return PQntuples(result.get()) > 0;
    } catch (const std::exception& e) {
        spdlog::error("Health check failed: {}", e.what());
        return false;
    }
}

// --- Pop using Stored Procedure (Single Round Trip) ---
// HIGH-PERFORMANCE version used by poll workers for wait=true long-polling

PopResult AsyncQueueManager::pop_messages_sp(
    const std::string& queue_name,
    const std::string& partition_name,  // Empty for any partition
    const std::string& consumer_group,
    const PopOptions& options
) {
    PopResult result;
    
    try {
        auto conn = async_db_pool_->acquire();
        
        // Build batch JSON format for pop_messages_batch_v2
        // This avoids the expensive global lease cleanup in the older pop_messages_v2
        nlohmann::json batch_item;
        batch_item["idx"] = 0;
        batch_item["queue"] = queue_name;
        batch_item["partition"] = partition_name;
        batch_item["consumerGroup"] = consumer_group;
        batch_item["batch"] = options.batch;
        batch_item["leaseTime"] = 0;  // Use queue's configured lease_time
        batch_item["subMode"] = options.subscription_mode.value_or("all");
        batch_item["subFrom"] = options.subscription_from.value_or("");
        
        nlohmann::json batch_array = nlohmann::json::array();
        batch_array.push_back(batch_item);
        
        std::string sql = "SELECT queen.pop_messages_batch_v2($1::jsonb)";
        std::vector<std::string> params = { batch_array.dump() };
        
        sendQueryParamsAsync(conn.get(), sql, params);
        auto query_result = getTuplesResult(conn.get());
        
        if (PQntuples(query_result.get()) == 0) {
            return result;
        }
        
        // Parse JSON result
        const char* json_str = PQgetvalue(query_result.get(), 0, 0);
        if (!json_str || strlen(json_str) == 0) {
            return result;
        }
        
        auto json_result = nlohmann::json::parse(json_str);
        
        // Unwrap batch format: [{idx, result: {messages, leaseId}}] -> {messages, leaseId}
        if (json_result.is_array() && !json_result.empty() && json_result[0].contains("result")) {
            json_result = json_result[0]["result"];
        }
        
        // Extract lease ID
        if (json_result.contains("leaseId") && !json_result["leaseId"].is_null()) {
            result.lease_id = json_result["leaseId"].get<std::string>();
        }
        
        // Extract messages
        if (!json_result.contains("messages") || !json_result["messages"].is_array()) {
            return result;
        }
        
        auto& messages_json = json_result["messages"];
        if (messages_json.empty()) {
            return result;
        }
        
        // Get encryption service if needed
        EncryptionService* enc_service = get_encryption_service();
        
        // Parse each message
        for (const auto& msg_json : messages_json) {
            Message msg;
            
            msg.id = msg_json.value("id", "");
            msg.transaction_id = msg_json.value("transactionId", "");
            msg.partition_id = msg_json.value("partitionId", "");
            msg.queue_name = msg_json.value("queue", "");
            msg.partition_name = msg_json.value("partition", "");
            msg.retry_count = msg_json.value("retryCount", 0);
            msg.priority = msg_json.value("priority", 0);
            
            // Parse trace_id
            if (msg_json.contains("traceId") && !msg_json["traceId"].is_null()) {
                msg.trace_id = msg_json["traceId"].get<std::string>();
            }
            
            // Parse created_at timestamp
            if (msg_json.contains("createdAt") && !msg_json["createdAt"].is_null()) {
                std::string ts_str = msg_json["createdAt"].get<std::string>();
                // Parse ISO8601 timestamp
                std::tm tm = {};
                int ms = 0;
                if (sscanf(ts_str.c_str(), "%d-%d-%dT%d:%d:%d.%dZ",
                          &tm.tm_year, &tm.tm_mon, &tm.tm_mday,
                          &tm.tm_hour, &tm.tm_min, &tm.tm_sec, &ms) >= 6) {
                    tm.tm_year -= 1900;
                    tm.tm_mon -= 1;
                    time_t time = timegm(&tm);
                    msg.created_at = std::chrono::system_clock::from_time_t(time) + 
                                    std::chrono::milliseconds(ms);
            }
            }
            
            // Handle payload/data - check if encrypted
            nlohmann::json payload_data;
            if (msg_json.contains("data")) {
                payload_data = msg_json["data"];
            }
            
            // Check if encrypted and decrypt
            if (payload_data.is_object() && 
                payload_data.contains("encrypted") && 
                payload_data.contains("iv") && 
                payload_data.contains("authTag") &&
                enc_service && enc_service->is_enabled()) {
                try {
                    EncryptionService::EncryptedData encrypted_data{
                        payload_data["encrypted"].get<std::string>(),
                        payload_data["iv"].get<std::string>(),
                        payload_data["authTag"].get<std::string>()
                    };
                    auto decrypted = enc_service->decrypt_payload(encrypted_data);
                    if (decrypted.has_value()) {
                        msg.payload = nlohmann::json::parse(decrypted.value());
                    } else {
                        msg.payload = payload_data;
                    }
                } catch (...) {
                    msg.payload = payload_data;
                }
            } else {
                msg.payload = payload_data;
            }
            
            result.messages.push_back(msg);
        }
        
        // Auto-ack if enabled - use stored procedure directly
        if (options.auto_ack && !result.messages.empty() && result.lease_id.has_value()) {
            spdlog::debug("Auto-ack enabled - acknowledging {} messages via stored procedure", result.messages.size());
            
            try {
                // Build JSON array for ack stored procedure
                nlohmann::json ack_array = nlohmann::json::array();
                for (const auto& msg : result.messages) {
                    nlohmann::json ack_obj;
                    ack_obj["transaction_id"] = msg.transaction_id;
                    ack_obj["partition_id"] = msg.partition_id;
                    ack_obj["consumer_group"] = consumer_group;
                    ack_obj["status"] = "completed";
                    ack_array.push_back(ack_obj);
                }
                
                std::string ack_sql = "SELECT queen.ack_messages_v2($1::jsonb)";
                sendQueryParamsAsync(conn.get(), ack_sql, {ack_array.dump()});
                getTuplesResult(conn.get());
        
                spdlog::debug("Auto-ack completed for {} messages", result.messages.size());
    } catch (const std::exception& e) {
                spdlog::error("Auto-ack failed: {}", e.what());
            }
        }
        
    } catch (const std::exception& e) {
        spdlog::error("pop_messages_sp failed: {}", e.what());
    }
    
    return result;
}


// ===================================
// MAINTENANCE MODE
// ===================================

void AsyncQueueManager::set_maintenance_mode(bool enabled) {
    try {
        auto conn = async_db_pool_->acquire();
        
        // Persist to database for multi-instance support
        std::string sql = R"(
            INSERT INTO queen.system_state (key, value, updated_at)
            VALUES ('maintenance_mode', $1::jsonb, NOW())
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value,
                updated_at = NOW()
        )";
        
        nlohmann::json value = {{"enabled", enabled}};
        sendQueryParamsAsync(conn.get(), sql, {value.dump()});
        getCommandResult(conn.get());
        
        // Update cache immediately on this instance
        maintenance_mode_cached_.store(enabled);
        last_maintenance_check_ms_.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count()
        );
        
        // Handle file buffer state changes
        if (file_buffer_manager_) {
            if (enabled) {
                // ENABLE: Pause background drain to prevent infinite loop
                file_buffer_manager_->pause_background_drain();
                spdlog::info("Maintenance mode ENABLED - PUSHes routing to file buffer, drain paused");
            } else {
                // DISABLE: Force finalize .tmp files and resume drain
                spdlog::info("Maintenance mode DISABLED - finalizing buffered messages and resuming drain");
                
                // Step 1: Force finalize all .tmp files to make messages visible
                file_buffer_manager_->force_finalize_all();
                
                // Step 2: Resume background drain (will wake processor)
                file_buffer_manager_->resume_background_drain();
                
                size_t pending = file_buffer_manager_->get_pending_count();
                spdlog::info("File buffer will drain {} pending messages to database", pending);
            }
        } else {
            spdlog::info("Maintenance mode {} (persisted to database for all instances)", 
                        enabled ? "ENABLED - routing PUSHes to file buffer" : "DISABLED - resuming normal operations");
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to set maintenance mode: {}", e.what());
        throw;
    }
}

bool AsyncQueueManager::get_maintenance_mode_fresh() {
    // Force cache invalidation by setting last_check to 0
    last_maintenance_check_ms_.store(0);
    return check_maintenance_mode_with_cache();
}

size_t AsyncQueueManager::get_buffer_pending_count() const {
    if (!file_buffer_manager_) {
        return 0;
    }
    return file_buffer_manager_->get_pending_count();
}

bool AsyncQueueManager::is_buffer_healthy() const {
    if (!file_buffer_manager_) {
        return true;  // No buffer = healthy by default
    }
    return file_buffer_manager_->is_db_healthy();
}

nlohmann::json AsyncQueueManager::get_buffer_stats() const {
    if (!file_buffer_manager_) {
        return {
            {"pendingCount", 0},
            {"failedCount", 0},
            {"dbHealthy", true},
            {"failedFiles", {
                {"count", 0},
                {"totalBytes", 0},
                {"totalMB", 0.0},
                {"failoverCount", 0},
                {"qos0Count", 0}
            }}
        };
    }
    
    auto failed_stats = file_buffer_manager_->get_failed_files_stats();
    double total_mb = failed_stats.total_bytes / (1024.0 * 1024.0);
    
    return {
        {"pendingCount", file_buffer_manager_->get_pending_count()},
        {"failedCount", file_buffer_manager_->get_failed_count()},
        {"dbHealthy", file_buffer_manager_->is_db_healthy()},
        {"failedFiles", {
            {"count", failed_stats.file_count},
            {"totalBytes", failed_stats.total_bytes},
            {"totalMB", total_mb},
            {"failoverCount", failed_stats.failover_count},
            {"qos0Count", failed_stats.qos0_count}
        }}
    };
}

} // namespace queen

