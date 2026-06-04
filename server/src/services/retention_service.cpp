#include "queen/retention_service.hpp"
#include "queen/shared_state_manager.hpp"

#include <algorithm>
#include <array>
#include <thread>
#include <vector>

namespace queen {

// External global for shared state
extern std::shared_ptr<SharedStateManager> global_shared_state;

RetentionService::RetentionService(
    std::shared_ptr<AsyncDbPool> db_pool,
    std::shared_ptr<astp::ThreadPool> db_thread_pool,
    std::shared_ptr<astp::ThreadPool> system_thread_pool,
    int retention_interval_ms,
    int retention_batch_size,
    int retention_parallelism,
    int partition_cleanup_days,
    int metrics_retention_days
) : db_pool_(db_pool),
    db_thread_pool_(db_thread_pool),
    system_thread_pool_(system_thread_pool),
    retention_interval_ms_(retention_interval_ms),
    retention_batch_size_(retention_batch_size),
    retention_parallelism_(retention_parallelism < 1 ? 1 : retention_parallelism),
    partition_cleanup_days_(partition_cleanup_days),
    metrics_retention_days_(metrics_retention_days) {
}

RetentionService::~RetentionService() {
    stop();
}

void RetentionService::start() {
    if (running_) {
        spdlog::warn("RetentionService already running");
        return;
    }
    
    running_ = true;
    schedule_next_run();
    
    spdlog::info("RetentionService started: interval={}ms, batch_size={}, parallelism={}, "
                 "partition_cleanup_days={}, metrics_retention_days={}",
                 retention_interval_ms_, retention_batch_size_, retention_parallelism_,
                 partition_cleanup_days_, metrics_retention_days_);
}

void RetentionService::stop() {
    if (!running_) return;
    running_ = false;
    spdlog::info("RetentionService stopped");
}

void RetentionService::schedule_next_run() {
    if (!running_) return;
    
    // Schedule in system threadpool (same pattern as MetricsCollector)
    system_thread_pool_->push([this]() {
        this->cleanup_cycle();
    });
}

void RetentionService::cleanup_cycle() {
    spdlog::info("RetentionService: Running cleanup cycle");
    auto cycle_start = std::chrono::steady_clock::now();
    
    // Advisory lock ID shared between retention and eviction services
    // Prevents multiple instances from running cleanup concurrently (avoids deadlocks)
    constexpr int64_t CLEANUP_LOCK_ID = 737001;
    
    // Connection used to hold the advisory lock for the duration of cleanup
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
            spdlog::debug("RetentionService: Skipping cycle, another instance holds the lock");
        } else {
            // We have the lock - run all cleanup operations
            int expired = cleanup_expired_messages();
            int completed = cleanup_completed_messages();
            int partitions = cleanup_inactive_partitions();
            int metrics = cleanup_old_metrics();
            
            // Only log if something was cleaned up
            if (expired > 0 || completed > 0 || partitions > 0 || metrics > 0) {
                spdlog::info("RetentionService: Cleaned up expired_messages={}, completed_messages={}, "
                            "inactive_partitions={}, old_metrics={}",
                            expired, completed, partitions, metrics);
            }
        }
        
    } catch (const std::exception& e) {
        spdlog::error("RetentionService cycle error: {}", e.what());
    }
    
    // End transaction to release the lock (COMMIT releases xact-level advisory lock)
    if (in_transaction && lock_conn) {
        try {
            sendQueryParamsAsync(lock_conn.get(), "COMMIT", {});
            getCommandResult(lock_conn.get());
        } catch (const std::exception& e) {
            // Transaction will be rolled back when connection returns to pool anyway
            spdlog::debug("RetentionService: Failed to commit lock transaction: {}", e.what());
        }
    }
    
    // Calculate sleep time and reschedule
    if (running_) {
        auto cycle_end = std::chrono::steady_clock::now();
        auto cycle_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            cycle_end - cycle_start
        );
        spdlog::info("RetentionService: cycle completed in {}ms", cycle_duration.count());
        auto sleep_time = retention_interval_ms_ - cycle_duration.count();
        if (sleep_time < 0) sleep_time = 0;
        
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
        schedule_next_run();  // Recursive scheduling
    }
}

int RetentionService::run_partitions_parallel(
        size_t n, int workers,
        const std::function<int(PGconn*, size_t)>& fn) {
    if (n == 0) return 0;

    std::atomic<int> total{0};
    std::atomic<size_t> cursor{0};

    // Each worker owns its own pooled connection and pulls partitions off a
    // shared work-stealing cursor (balances uneven partition sizes). Disjoint
    // partitions per worker => no row overlap, no extra locking needed; the
    // cleanup_cycle advisory lock already excludes other replicas.
    auto worker = [&]() {
        try {
            auto conn = db_pool_->acquire();
            int local = 0;
            size_t i;
            while (running_ && (i = cursor.fetch_add(1)) < n) {
                local += fn(conn.get(), i);
            }
            total.fetch_add(local);
        } catch (const std::exception& e) {
            spdlog::error("RetentionService parallel worker error: {}", e.what());
        }
    };

    int w = std::max(1, std::min(workers, static_cast<int>(n)));
    if (w <= 1) {
        worker();  // legacy path: run inline on the calling thread
    } else {
        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(w - 1));
        for (int t = 0; t < w - 1; t++) threads.emplace_back(worker);
        worker();  // also use the calling thread
        for (auto& th : threads) th.join();
    }
    return total.load();
}

int RetentionService::delete_expired_partition(
        PGconn* conn, const std::string& partition_id, const std::string& cutoff) {
    static const std::string delete_sql = R"(
        DELETE FROM queen.messages
        WHERE id IN (
            SELECT id FROM queen.messages
            WHERE partition_id = $1::uuid
              AND created_at < $2::timestamptz
            LIMIT $3
        )
    )";
    int deleted = 0;
    int batch_deleted;
    do {
        sendQueryParamsAsync(conn, delete_sql,
            {partition_id, cutoff, std::to_string(retention_batch_size_)});
        auto result = getCommandResultPtr(conn);
        char* affected_str = PQcmdTuples(result.get());
        batch_deleted = (affected_str && *affected_str) ? std::stoi(affected_str) : 0;
        deleted += batch_deleted;
    } while (batch_deleted == retention_batch_size_ && running_);
    return deleted;
}

int RetentionService::cleanup_expired_messages() {
    try {
        // Step 1: list partitions that actually have expired messages (cheap
        // pre-filters keep this to a few hundred rows). Materialize so the
        // listing connection is released before the parallel delete phase.
        std::vector<std::array<std::string, 2>> parts;  // [partition_id, cutoff]
        {
            auto conn = db_pool_->acquire();
            std::string partitions_sql = R"(
                SELECT p.id as partition_id, 
                       NOW() - (q.retention_seconds || ' seconds')::INTERVAL as cutoff
                FROM queen.partitions p
                JOIN queen.queues q ON p.queue_id = q.id
                WHERE q.retention_enabled = true
                  AND q.retention_seconds > 0
                  AND p.created_at < NOW() - (q.retention_seconds || ' seconds')::INTERVAL
                  AND EXISTS (
                      SELECT 1 FROM queen.messages m
                      WHERE m.partition_id = p.id
                        AND m.created_at < NOW() - (q.retention_seconds || ' seconds')::INTERVAL
                  )
            )";
            sendQueryParamsAsync(conn.get(), partitions_sql, {});
            auto pr = getTuplesResult(conn.get());
            int n = PQntuples(pr.get());
            if (n == 0) return 0;
            int pid_col = PQfnumber(pr.get(), "partition_id");
            int cutoff_col = PQfnumber(pr.get(), "cutoff");
            parts.reserve(static_cast<size_t>(n));
            for (int i = 0; i < n; i++) {
                parts.push_back({PQgetvalue(pr.get(), i, pid_col),
                                 PQgetvalue(pr.get(), i, cutoff_col)});
            }
        }

        // Step 2: delete per partition across retention_parallelism_ connections.
        return run_partitions_parallel(parts.size(), retention_parallelism_,
            [this, &parts](PGconn* c, size_t i) {
                return delete_expired_partition(c, parts[i][0], parts[i][1]);
            });
    } catch (const std::exception& e) {
        spdlog::error("cleanup_expired_messages error: {}", e.what());
        return 0;
    }
}

int RetentionService::delete_completed_partition(
        PGconn* conn, const std::string& partition_id,
        const std::string& safe_consumed_id, const std::string& cutoff) {
    // Only delete messages where id <= MIN(last_consumed_id) across all consumer
    // groups (i.e. fully consumed) AND older than the completed-retention cutoff.
    static const std::string delete_sql = R"(
        DELETE FROM queen.messages
        WHERE id IN (
            SELECT id FROM queen.messages
            WHERE partition_id = $1::uuid
              AND id <= $2::uuid
              AND created_at < $3::timestamptz
            LIMIT $4
        )
    )";
    static const std::string insert_history_sql = R"(
        INSERT INTO queen.retention_history (partition_id, messages_deleted, retention_type)
        VALUES ($1::uuid, $2, 'completed_retention')
    )";

    int partition_deleted = 0;
    int batch_deleted;
    do {
        sendQueryParamsAsync(conn, delete_sql,
            {partition_id, safe_consumed_id, cutoff, std::to_string(retention_batch_size_)});
        auto result = getCommandResultPtr(conn);
        char* affected_str = PQcmdTuples(result.get());
        batch_deleted = (affected_str && *affected_str) ? std::stoi(affected_str) : 0;
        partition_deleted += batch_deleted;
    } while (batch_deleted == retention_batch_size_ && running_);

    if (partition_deleted > 0) {
        try {
            sendQueryParamsAsync(conn, insert_history_sql,
                {partition_id, std::to_string(partition_deleted)});
            getCommandResult(conn);
        } catch (const std::exception& e) {
            spdlog::warn("Failed to record retention_history: {}", e.what());
        }
    }
    return partition_deleted;
}

int RetentionService::cleanup_completed_messages() {
    try {
        // Step 1: list partitions with completed_retention due, plus the MINIMUM
        // last_consumed_id across ALL consumer groups (so we only delete what
        // every group has processed). Cheap pre-filters keep this small. Note:
        // PostgreSQL has no MIN() for UUID, hence the ::text round-trip.
        // Materialize so the listing connection is released before deletes.
        std::vector<std::array<std::string, 3>> parts;  // [partition_id, cutoff, safe_consumed_id]
        {
            auto conn = db_pool_->acquire();
            std::string partitions_sql = R"(
                SELECT p.id as partition_id,
                       NOW() - (q.completed_retention_seconds || ' seconds')::INTERVAL as cutoff,
                       MIN(pc.last_consumed_id::text)::uuid as safe_consumed_id
                FROM queen.partitions p
                JOIN queen.queues q ON p.queue_id = q.id
                JOIN queen.partition_consumers pc ON pc.partition_id = p.id
                WHERE q.retention_enabled = true
                  AND q.completed_retention_seconds > 0
                  AND p.created_at < NOW() - (q.completed_retention_seconds || ' seconds')::INTERVAL
                  AND EXISTS (
                      SELECT 1 FROM queen.messages m
                      WHERE m.partition_id = p.id
                        AND m.created_at < NOW() - (q.completed_retention_seconds || ' seconds')::INTERVAL
                  )
                GROUP BY p.id, q.completed_retention_seconds
                HAVING MIN(pc.last_consumed_id::text)::uuid != '00000000-0000-0000-0000-000000000000'
            )";
            sendQueryParamsAsync(conn.get(), partitions_sql, {});
            auto pr = getTuplesResult(conn.get());
            int n = PQntuples(pr.get());
            if (n == 0) return 0;
            int pid_col = PQfnumber(pr.get(), "partition_id");
            int cutoff_col = PQfnumber(pr.get(), "cutoff");
            int safe_col = PQfnumber(pr.get(), "safe_consumed_id");
            parts.reserve(static_cast<size_t>(n));
            for (int i = 0; i < n; i++) {
                parts.push_back({PQgetvalue(pr.get(), i, pid_col),
                                 PQgetvalue(pr.get(), i, cutoff_col),
                                 PQgetvalue(pr.get(), i, safe_col)});
            }
        }

        // Step 2: delete per partition across retention_parallelism_ connections.
        return run_partitions_parallel(parts.size(), retention_parallelism_,
            [this, &parts](PGconn* c, size_t i) {
                return delete_completed_partition(c, parts[i][0], parts[i][2], parts[i][1]);
            });
    } catch (const std::exception& e) {
        spdlog::error("cleanup_completed_messages error: {}", e.what());
        return 0;
    }
}

int RetentionService::cleanup_inactive_partitions() {
    try {
        auto conn = db_pool_->acquire();
        
        // Find inactive partitions by checking:
        // 1. No messages exist in the partition
        // 2. No recent activity from ANY consumer group (using partition_consumers)
        // 3. Falls back to partition created_at if no consumers exist
        //
        // Uses MAX across all consumer groups to find most recent activity.
        // Considers both last_consumed_at (successful ack) and pc.created_at 
        // (consumer started but hasn't acked yet) to avoid deleting partitions
        // with active consumers.
        std::string select_sql = R"(
            SELECT p.id, q.name as queue_name, p.name as partition_name
            FROM queen.partitions p
            JOIN queen.queues q ON p.queue_id = q.id
            LEFT JOIN queen.partition_consumers pc ON pc.partition_id = p.id
            WHERE NOT EXISTS (
                SELECT 1 FROM queen.messages m 
                WHERE m.partition_id = p.id 
                LIMIT 1
            )
            GROUP BY p.id, q.name, p.name, p.created_at
            HAVING GREATEST(
                p.created_at,
                COALESCE(MAX(pc.last_consumed_at), p.created_at),
                COALESCE(MAX(pc.created_at), p.created_at)
            ) < NOW() - ($1 || ' days')::INTERVAL
            LIMIT 1000
        )";
        
        sendQueryParamsAsync(conn.get(), select_sql, {std::to_string(partition_cleanup_days_)});
        auto select_result = getTuplesResult(conn.get());
        
        int num_to_delete = PQntuples(select_result.get());
        if (num_to_delete == 0) {
            return 0;
        }
        
        // Collect partition IDs for deletion
        std::vector<std::string> partition_ids;
        partition_ids.reserve(num_to_delete);
        int id_col = PQfnumber(select_result.get(), "id");
        for (int i = 0; i < num_to_delete; i++) {
            partition_ids.push_back(PQgetvalue(select_result.get(), i, id_col));
        }
        
        // Build parameterized IN clause for safe deletion
        std::string placeholders;
        std::vector<std::string> params;
        params.reserve(partition_ids.size());
        for (size_t i = 0; i < partition_ids.size(); i++) {
            if (i > 0) placeholders += ",";
            placeholders += "$" + std::to_string(i + 1) + "::uuid";
            params.push_back(partition_ids[i]);
        }
        
        // Delete only the partitions we identified (two-phase to ensure consistency)
        std::string delete_sql = "DELETE FROM queen.partitions WHERE id IN (" + placeholders + ")";
        
        sendQueryParamsAsync(conn.get(), delete_sql, params);
        auto result = getCommandResultPtr(conn.get());
        
        char* affected_str = PQcmdTuples(result.get());
        int deleted = (affected_str && *affected_str) ? std::stoi(affected_str) : 0;
        
        return deleted;
        
    } catch (const std::exception& e) {
        spdlog::error("cleanup_inactive_partitions error: {}", e.what());
    }
    
    return 0;
}

int RetentionService::cleanup_old_metrics() {
    try {
        auto conn = db_pool_->acquire();
        
        int total_deleted = 0;
        
        // Clean messages_consumed metrics
        std::string sql1 = R"(
            DELETE FROM queen.messages_consumed
            WHERE acked_at < NOW() - ($1 || ' days')::INTERVAL
        )";
        
        sendQueryParamsAsync(conn.get(), sql1, {std::to_string(metrics_retention_days_)});
        auto result1 = getCommandResultPtr(conn.get());
        char* affected1_str = PQcmdTuples(result1.get());
        total_deleted += (affected1_str && *affected1_str) ? std::stoi(affected1_str) : 0;
        
        // Clean system_metrics
        std::string sql2 = R"(
DELETE FROM queen.system_metrics 
            WHERE timestamp < NOW() - ($1 || ' days')::INTERVAL
        )";
        
        sendQueryParamsAsync(conn.get(), sql2, {std::to_string(metrics_retention_days_)});
        auto result2 = getCommandResultPtr(conn.get());
        char* affected2_str = PQcmdTuples(result2.get());
        total_deleted += (affected2_str && *affected2_str) ? std::stoi(affected2_str) : 0;
        
        // Clean retention_history itself (older than metrics_retention_days)
        std::string sql3 = R"(
            DELETE FROM queen.retention_history
            WHERE executed_at < NOW() - ($1 || ' days')::INTERVAL
        )";
        
        sendQueryParamsAsync(conn.get(), sql3, {std::to_string(metrics_retention_days_)});
        auto result3 = getCommandResultPtr(conn.get());
        char* affected3_str = PQcmdTuples(result3.get());
        total_deleted += (affected3_str && *affected3_str) ? std::stoi(affected3_str) : 0;
        
        return total_deleted;
        
    } catch (const std::exception& e) {
        spdlog::error("cleanup_old_metrics error: {}", e.what());
    }
    
    return 0;
}

} // namespace queen

