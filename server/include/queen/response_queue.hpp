#pragma once

#include <queue>
#include <mutex>
#include <memory>
#include <string>
#include <unordered_map>
#include <atomic>
#include <chrono>
#include <optional>
#include <json.hpp>
// Forward declaration to avoid including full uWebSockets headers
namespace uWS {
    template <bool SSL>
    struct HttpResponse;
}
#include <spdlog/spdlog.h>

namespace queen {

/**
 * Thread-safe response queue for passing results from DB threadpool to uWebSockets event loop
 */
class ResponseQueue {
public:
    struct ResponseItem {
        std::string request_id;
        nlohmann::json data;
        bool is_error;
        int status_code;
        std::chrono::steady_clock::time_point timestamp;
        
        ResponseItem() = default;
        ResponseItem(const std::string& id, const nlohmann::json& d, bool err = false, int code = 200)
            : request_id(id), data(d), is_error(err), status_code(code), 
              timestamp(std::chrono::steady_clock::now()) {}
    };

private:
    std::queue<ResponseItem> queue_;
    mutable std::mutex mutex_;
    
public:
    void push(const std::string& request_id, const nlohmann::json& data, 
              bool is_error = false, int status_code = 200) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // Check response size before queuing (optional size check)
        try {
            // Quick size estimation without full serialization
            std::string sample = data.dump(0, ' ', false, nlohmann::json::error_handler_t::ignore);
            if (sample.length() > 100 * 1024 * 1024) { // 100MB threshold for queue rejection
                spdlog::error("Response too large to queue: ~{} bytes for request {}", sample.length(), request_id);
                // Don't queue extremely large responses
                return;
            } else if (sample.length() > 5 * 1024 * 1024) { // 5MB warning threshold
                spdlog::warn("Large response queued: ~{} bytes for request {}", sample.length(), request_id);
            }
        } catch (const std::exception& e) {
            spdlog::debug("Could not estimate response size for {}: {}", request_id, e.what());
            // Continue anyway - the actual serialization will handle errors
        }
        
        queue_.emplace(request_id, data, is_error, status_code);
    }
    
    bool pop(ResponseItem& item) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) return false;
        
        item = std::move(queue_.front());
        queue_.pop();
        return true;
    }
    
    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }
    
    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }
};

/**
 * Registry for tracking HTTP response objects safely
 */
class ResponseRegistry {
public:
    using AbortCallback = std::function<void(const std::string& request_id)>;
    
    struct ResponseEntry {
        uWS::HttpResponse<false>* response;
        std::chrono::steady_clock::time_point created_at;
        std::atomic<bool> valid{true};
        std::mutex mutex;
        int worker_id;  // Track which worker owns this response
        AbortCallback on_abort;  // Optional callback when connection aborts
        
        ResponseEntry(uWS::HttpResponse<false>* res, int wid) 
            : response(res), created_at(std::chrono::steady_clock::now()), worker_id(wid) {}
    };

private:
    std::unordered_map<std::string, std::shared_ptr<ResponseEntry>> responses_;
    mutable std::mutex registry_mutex_;
    
    std::string generate_uuid() const;

    // Shared delivery: writes `json_str` (already serialized; empty => 204-style
    // end()) to the registered response, with small/large (chunked) handling.
    bool deliver_string(const std::string& request_id, std::string json_str, int status_code);

public:
    std::string register_response(uWS::HttpResponse<false>* res, int worker_id, 
                                  AbortCallback on_abort = nullptr);
    
    bool send_response(const std::string& request_id, const nlohmann::json& data, 
                      bool is_error = false, int status_code = 200);

    /**
     * Deliver an already-serialized JSON string as the response body, skipping
     * the nlohmann parse+dump round-trip. Used by hot paths that pass the
     * stored-procedure result straight through (Stage 2 raw result pass-through).
     */
    bool send_response_raw(const std::string& request_id, std::string json_body,
                           int status_code = 200);
    
    /**
     * Check if a request is still valid (not aborted)
     * @param request_id The request ID to check
     * @return true if the request exists and is valid, false if aborted or not found
     */
    bool is_valid(const std::string& request_id) const;
    
    void cleanup_expired(std::chrono::milliseconds max_age = std::chrono::seconds(60));
    
    size_t size() const {
        std::lock_guard<std::mutex> lock(registry_mutex_);
        return responses_.size();
    }
};

/**
 * Storage for pending push items for file buffer failover.
 * When a push is submitted to sidecar, items are stored here.
 * If sidecar fails (DB down), items are retrieved and written to file buffer.
 * Per-worker instance to avoid cross-thread synchronization.
 */
class PushFailoverStorage {
public:
    /**
     * Store items for a request (before sidecar submit)
     * @param request_id The unique request identifier
     * @param items_json JSON array of push items (as string for efficiency)
     */
    void store(const std::string& request_id, const std::string& items_json) {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_items_[request_id] = items_json;
        spdlog::debug("PushFailoverStorage: stored {} bytes for request {}", 
                     items_json.size(), request_id);
    }
    
    /**
     * Retrieve and remove items for a request (on sidecar response)
     * @param request_id The unique request identifier
     * @return Optional items_json string, empty if not found
     */
    std::optional<std::string> retrieve_and_remove(const std::string& request_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = pending_items_.find(request_id);
        if (it == pending_items_.end()) {
            return std::nullopt;
        }
        std::string items = std::move(it->second);
        pending_items_.erase(it);
        spdlog::debug("PushFailoverStorage: retrieved {} bytes for request {}", 
                     items.size(), request_id);
        return items;
    }
    
    /**
     * Remove items without retrieving (on successful push)
     */
    void remove(const std::string& request_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_items_.erase(request_id);
    }
    
    /**
     * Get count of pending items (for stats/debugging)
     */
    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return pending_items_.size();
    }

private:
    std::unordered_map<std::string, std::string> pending_items_;
    mutable std::mutex mutex_;
};

} // namespace queen
