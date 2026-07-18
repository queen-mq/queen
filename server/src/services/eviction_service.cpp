#include "queen/eviction_service.hpp"

#include <json.hpp>

namespace queen {

EvictionService::EvictionService(
    std::shared_ptr<AsyncDbPool> db_pool,
    std::shared_ptr<astp::ThreadPool> db_thread_pool,
    std::shared_ptr<astp::ThreadPool> system_thread_pool,
    int eviction_interval_ms,
    int eviction_batch_size
) : db_pool_(db_pool),
    db_thread_pool_(db_thread_pool),
    system_thread_pool_(system_thread_pool),
    eviction_interval_ms_(eviction_interval_ms),
    eviction_batch_size_(eviction_batch_size) {
}

EvictionService::~EvictionService() {
    stop();
}

void EvictionService::start() {
    if (running_) {
        spdlog::warn("EvictionService already running");
        return;
    }
    
    running_ = true;
    schedule_next_run();
    
    spdlog::info("EvictionService started: interval={}ms, batch_size={}",
                 eviction_interval_ms_, eviction_batch_size_);
}

void EvictionService::stop() {
    if (!running_) return;
    running_ = false;
    spdlog::info("EvictionService stopped");
}

void EvictionService::schedule_next_run() {
    if (!running_) return;
    
    // Schedule in system threadpool (same pattern as MetricsCollector)
    system_thread_pool_->push([this]() {
        this->eviction_cycle();
    });
}

void EvictionService::eviction_cycle() {
    auto cycle_start = std::chrono::steady_clock::now();
    
    // Advisory lock ID shared between retention and eviction services
    // Prevents multiple instances from running cleanup concurrently (avoids deadlocks)
    // MUST match the lock ID in retention_service.cpp
    constexpr int64_t CLEANUP_LOCK_ID = 737001;
    
    // Connection used to hold the advisory lock for the duration of eviction
    // Uses transaction-level lock (pg_advisory_xact_lock) for PgBouncer compatibility
    AsyncDbPool::PooledConnection lock_conn;
    bool has_lock = false;
    bool in_transaction = false;
    
    try {
        lock_conn = db_pool_->acquire();
        
        // Start transaction to hold the advisory lock
        // Transaction-level locks auto-release on COMMIT/ROLLBACK, avoiding
        // "you don't own this lock" errors with PgBouncer transaction pooling
        sendQueryParamsAsync(lock_conn.get(), "BEGIN", {});
        getCommandResult(lock_conn.get());
        in_transaction = true;
        
        // Try to acquire transaction-level advisory lock (non-blocking)
        sendQueryParamsAsync(lock_conn.get(), 
            "SELECT pg_try_advisory_xact_lock($1::bigint)", 
            {std::to_string(CLEANUP_LOCK_ID)});
        auto lock_result = getTuplesResult(lock_conn.get());
        
        if (PQntuples(lock_result.get()) > 0) {
            std::string result_str = PQgetvalue(lock_result.get(), 0, 0);
            has_lock = (result_str == "t");
        }
        
        if (!has_lock) {
            // Another instance is running cleanup - skip this cycle
            spdlog::debug("EvictionService: Skipping cycle, another instance holds the lock");
        } else {
            // We have the lock - run eviction
            int evicted = evict_expired_waiting_messages();

            // Only log if something was evicted
            if (evicted > 0) {
                spdlog::info("EvictionService: Evicted {} messages exceeding max_wait_time", evicted);
            }

            // Storage v2 (segments): enforce max_queue_size on v2 queues in
            // one single-transaction sweep (queen.seg_evict_v1,
            // 026_storage_v2_maintenance.sql). Still under the advisory lock
            // held on lock_conn, so concurrent broker instances never
            // double-sweep.
            try {
                auto conn = db_pool_->acquire();
                sendQueryParamsAsync(conn.get(), "SELECT queen.seg_evict_v1()", {});
                auto sweep = getTuplesResult(conn.get());
                if (PQntuples(sweep.get()) > 0 && !PQgetisnull(sweep.get(), 0, 0)) {
                    // {"queues":N,"segments_deleted":N,"messages_evicted":N}
                    const std::string counts = PQgetvalue(sweep.get(), 0, 0);
                    auto j = nlohmann::json::parse(counts, nullptr, false);
                    bool swept = j.is_object() &&
                                 (j.value("segments_deleted", 0) > 0 ||
                                  j.value("messages_evicted", 0) > 0);
                    if (swept) {
                        spdlog::info("EvictionService: v2 segments sweep {}", counts);
                    } else {
                        spdlog::debug("EvictionService: v2 segments sweep {}", counts);
                    }
                }
            } catch (const std::exception& e) {
                spdlog::error("EvictionService v2 sweep error: {}", e.what());
            }
        }
        
    } catch (const std::exception& e) {
        spdlog::error("EvictionService cycle error: {}", e.what());
    }
    
    // End transaction to release the lock (COMMIT releases xact-level advisory lock)
    if (in_transaction && lock_conn) {
        try {
            sendQueryParamsAsync(lock_conn.get(), "COMMIT", {});
            getCommandResult(lock_conn.get());
        } catch (const std::exception& e) {
            // Transaction will be rolled back when connection returns to pool anyway
            spdlog::debug("EvictionService: Failed to commit lock transaction: {}", e.what());
        }
    }
    
    // Calculate sleep time and reschedule
    if (running_) {
        auto cycle_end = std::chrono::steady_clock::now();
        auto cycle_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            cycle_end - cycle_start
        );
        
        auto sleep_time = eviction_interval_ms_ - cycle_duration.count();
        if (sleep_time < 0) sleep_time = 0;
        
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
        schedule_next_run();  // Recursive scheduling
    }
}

int EvictionService::evict_expired_waiting_messages() {
    try {
        auto conn = db_pool_->acquire();
        
        // Delete messages that have been waiting longer than max_wait_time_seconds
        // This applies to messages that are pending (not yet consumed)
        std::string sql = R"(
            WITH messages_to_evict AS (
                SELECT DISTINCT m.id, p.id as partition_id
                FROM queen.messages m
                JOIN queen.partitions p ON m.partition_id = p.id
                JOIN queen.queues q ON p.queue_id = q.id
                LEFT JOIN queen.partition_consumers pc ON p.id = pc.partition_id
                WHERE q.max_wait_time_seconds > 0
                  AND m.created_at < NOW() - (q.max_wait_time_seconds || ' seconds')::INTERVAL
                  AND (pc.last_consumed_id IS NULL OR m.id > pc.last_consumed_id)
                LIMIT $1
            )
DELETE FROM queen.messages 
            WHERE id IN (SELECT id FROM messages_to_evict)
            RETURNING (SELECT partition_id FROM messages_to_evict LIMIT 1) as partition_id
        )";
        
        sendQueryParamsAsync(conn.get(), sql, {std::to_string(eviction_batch_size_)});
        auto result = getTuplesResult(conn.get());  // Use getTuplesResult because of RETURNING clause
        
        char* affected_str = PQcmdTuples(result.get());
        int evicted = (affected_str && *affected_str) ? std::stoi(affected_str) : 0;
            
            // Record in retention_history if messages were evicted
        if (evicted > 0 && PQntuples(result.get()) > 0) {
                try {
                std::string partition_id = PQgetvalue(result.get(), 0, PQfnumber(result.get(), "partition_id"));
                    std::string insert_history = R"(
                        INSERT INTO queen.retention_history (partition_id, messages_deleted, retention_type)
                        VALUES ($1::uuid, $2, 'max_wait_time_eviction')
                    )";
                sendQueryParamsAsync(conn.get(), insert_history, {partition_id, std::to_string(evicted)});
                getCommandResult(conn.get());
                } catch (const std::exception& e) {
                    spdlog::warn("Failed to record retention_history for eviction: {}", e.what());
                }
            }
            
            return evicted;
        
    } catch (const std::exception& e) {
        spdlog::error("evict_expired_waiting_messages error: {}", e.what());
    }
    
    return 0;
}

} // namespace queen

