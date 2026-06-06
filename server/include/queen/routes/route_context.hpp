#pragma once

#include <memory>
#include <string>
#include <App.h>

// QueenCluster (and Queen) are used by value through ctx.queen->submit(...) in
// route TUs, so the complete type must be visible here.
#include "queen_cluster.hpp"

namespace queen {

// Forward declarations
class AsyncQueueManager;
class FileBufferManager;
class PushFailoverStorage;
class MetricsCollector;
struct Config;

} // namespace queen

namespace astp {
class ThreadPool;
}

namespace queen {
namespace routes {

/**
 * Context object containing all dependencies needed by route handlers.
 * This is passed to each route setup function and captured by lambdas.
 */
struct RouteContext {
    // Core queue operations manager
    std::shared_ptr<AsyncQueueManager> async_queue_manager;
    
    // File buffer for maintenance mode and failover
    std::shared_ptr<FileBufferManager> file_buffer;
    
    // Function-split engine cluster (push/ack | pop | rest), shared across all
    // HTTP workers. Raw pointer - lifetime is the process (owned by a global in
    // acceptor_server.cpp). ctx.queen->submit()/invalidate_request() dispatch to
    // the right engine by JobType.
    QueenCluster* queen;
    
    // uWS worker event loop for deferred response delivery
    uWS::Loop* worker_loop;
    
    // Configuration reference
    const Config& config;
    
    // Worker identifier for logging and routing
    int worker_id;
    
    // Database thread pool
    std::shared_ptr<astp::ThreadPool> db_thread_pool;
    
    // Storage for pending push items (for file buffer failover on failure)
    std::shared_ptr<PushFailoverStorage> push_failover_storage;
    
    // Background metrics collector (singleton, started by Worker 0; may be
    // null on non-Worker-0 routes if scrape arrives before initialization
    // completes — handle defensively in callers).
    std::shared_ptr<MetricsCollector> metrics_collector;
    
    // System threadpool used by background services. Exposed here so the
    // Prometheus exporter can publish queue/depth gauges without having to
    // reach into file-static globals.
    std::shared_ptr<astp::ThreadPool> system_thread_pool;
    
    // Replica identity used as Prometheus labels.
    std::string hostname;
    int port = 0;
    
    RouteContext(
        std::shared_ptr<AsyncQueueManager> qm,
        std::shared_ptr<FileBufferManager> fb,
        QueenCluster* q,
        uWS::Loop* loop,
        const Config& cfg,
        int wid,
        std::shared_ptr<astp::ThreadPool> dbtp,
        std::shared_ptr<PushFailoverStorage> pfs = nullptr,
        std::shared_ptr<MetricsCollector> mc = nullptr,
        std::shared_ptr<astp::ThreadPool> systp = nullptr,
        std::string host = "",
        int p = 0
    ) : async_queue_manager(std::move(qm)),
        file_buffer(std::move(fb)),
        queen(q),
        worker_loop(loop),
        config(cfg),
        worker_id(wid),
        db_thread_pool(std::move(dbtp)),
        push_failover_storage(std::move(pfs)),
        metrics_collector(std::move(mc)),
        system_thread_pool(std::move(systp)),
        hostname(std::move(host)),
        port(p)
    {}
};

} // namespace routes
} // namespace queen
