#pragma once

#include <string>
#include <unordered_map>
#include <optional>
#include <memory>
#include <atomic>
#include <shared_mutex>
#include <vector>

namespace queen {
namespace caches {

/**
 * Cached queue configuration
 * Matches the structure in queen.queues table
 */
struct CachedQueueConfig {
    std::string id;
    std::string name;
    int lease_time = 300;
    int retry_limit = 3;
    int retry_delay = 1000;
    int delayed_processing = 0;
    int window_buffer = 0;
    bool encryption_enabled = false;
    bool dlq_enabled = false;
    bool dlq_after_max_retries = false;
    int max_size = 10000;
    int ttl = 3600;
    int priority = 0;
    std::string namespace_name;
    std::string task;
    uint64_t version = 0;  // For conflict resolution (last write wins)
    
    // Retention settings
    int retention_seconds = 0;
    int completed_retention_seconds = 0;
    bool retention_enabled = false;
    
    // Eviction settings
    int max_wait_time_seconds = 0;

    // Storage engine: "rows" (v1, queen.messages) or "segments" (v2, queen.seg_segments)
    std::string storage = "rows";
};

/**
 * Queue Config Cache
 * 
 * Optimized for read-heavy workload (queue config rarely changes).
 * Uses shared_mutex for reader-writer locking.
 * 
 * Properties:
 * - Reads: Shared lock (multiple concurrent readers)
 * - Writes: Exclusive lock
 * - Thread-safety: Fully thread-safe
 */
class QueueConfigCache {
public:
    using ConfigMap = std::unordered_map<std::string, CachedQueueConfig>;
    
    QueueConfigCache() = default;
    
    /**
     * Get queue config by name
     * @param queue_name The queue name
     * @return The config if found, nullopt otherwise
     */
    std::optional<CachedQueueConfig> get(const std::string& queue_name) const {
        std::shared_lock lock(mutex_);
        auto it = data_.find(queue_name);
        if (it != data_.end()) {
            hits_++;
            return it->second;
        }
        misses_++;
        return std::nullopt;
    }
    
    /**
     * Set queue config
     * @param queue_name The queue name
     * @param config The configuration
     */
    void set(const std::string& queue_name, const CachedQueueConfig& config) {
        std::unique_lock lock(mutex_);
        
        // Check version for conflict resolution (last write wins based on version)
        auto it = data_.find(queue_name);
        if (it != data_.end() && it->second.version > config.version) {
            // Existing config has higher version, skip update
            return;
        }
        
        data_[queue_name] = config;
    }
    
    /**
     * Remove queue config
     * @param queue_name The queue name
     * @return true if removed, false if not found
     */
    bool remove(const std::string& queue_name) {
        std::unique_lock lock(mutex_);
        return data_.erase(queue_name) > 0;
    }
    
    /**
     * Bulk update all configs (for periodic refresh)
     * @param configs Vector of configs to set
     * @param remove_missing If true, remove queues not in the input
     */
    void bulk_update(const std::vector<CachedQueueConfig>& configs, bool remove_missing = false) {
        std::unique_lock lock(mutex_);
        
        if (remove_missing) {
            data_.clear();
        }
        
        for (const auto& config : configs) {
            data_[config.name] = config;
        }
    }
    
    /**
     * Get all queue names (for cleanup detection)
     */
    std::vector<std::string> keys() const {
        std::shared_lock lock(mutex_);
        std::vector<std::string> result;
        result.reserve(data_.size());
        for (const auto& [name, _] : data_) {
            result.push_back(name);
        }
        return result;
    }
    
    /**
     * Check if queue exists in cache
     */
    bool contains(const std::string& queue_name) const {
        std::shared_lock lock(mutex_);
        return data_.find(queue_name) != data_.end();
    }
    
    /**
     * Get cache size
     */
    size_t size() const {
        std::shared_lock lock(mutex_);
        return data_.size();
    }
    
    /**
     * Clear all entries
     */
    void clear() {
        std::unique_lock lock(mutex_);
        data_.clear();
    }
    
    // Stats
    uint64_t hits() const { return hits_.load(); }
    uint64_t misses() const { return misses_.load(); }
    double hit_rate() const {
        uint64_t h = hits_.load();
        uint64_t m = misses_.load();
        if (h + m == 0) return 0.0;
        return static_cast<double>(h) / static_cast<double>(h + m);
    }
    
private:
    ConfigMap data_;
    mutable std::shared_mutex mutex_;
    
    mutable std::atomic<uint64_t> hits_{0};
    mutable std::atomic<uint64_t> misses_{0};
};

} // namespace caches
} // namespace queen

