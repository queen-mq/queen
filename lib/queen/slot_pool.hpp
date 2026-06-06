#ifndef _QUEEN_SLOT_POOL_HPP_
#define _QUEEN_SLOT_POOL_HPP_

#include <uv.h>
#include <libpq-fe.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "metrics.hpp"
#include "pending_job.hpp"

namespace queen {

class Queen; // forward decl

// A single PostgreSQL connection slot owned by a Queen worker. Lifecycle
// (connect / reconnect / disconnect) is managed by Queen; this struct holds
// the per-slot runtime state.
//
// In-flight batch attribution:
//   - `current_type` identifies which `PerTypeState.concurrency` issued the
//     acquire that this in-flight batch corresponds to. Set in `_fire_batch`,
//     cleared in `_on_slot_freed`.
//   - `current_fire` is stamped at fire time and composed into a
//     `CompletionRecord` on completion.
struct DBConnection {
    PGconn*     conn                  = nullptr;
    int         socket_fd             = -1;
    uint16_t    idx                   = 0;
    uv_poll_t   poll_handle;
    bool        poll_initialized      = false;
    Queen*      pool                  = nullptr;
    std::vector<std::shared_ptr<PendingJob>> jobs;
    std::vector<std::pair<int, int>>         job_idx_ranges;  // (start_idx, count) per job

    // Per in-flight batch attribution. `current_type == _SENTINEL` means
    // "slot is idle" (no concurrency slot held). Centralizing both fields
    // under a single "release on free" path eliminates the counter-drift
    // risk of multiple acquire/release callers.
    JobType     current_type          = JobType::_SENTINEL;
    FireRecord  current_fire{};

    // PUSHSER: distinct partition keys held in-flight by this slot's PUSH batch.
    // Added to Queen::_push_inflight_partitions at fire, removed on slot free.
    // Event-loop-thread-only (no synchronization needed). Empty for non-PUSH.
    std::vector<std::string> push_partition_keys;

    // Lock-free reconnection signaling: set by the reconnect thread,
    // consumed by the event-loop thread (`_finalize_reconnected_slots`).
    std::atomic<bool> needs_poll_init{false};

    DBConnection() = default;

    DBConnection(DBConnection&& other) noexcept
        : conn(other.conn)
        , socket_fd(other.socket_fd)
        , idx(other.idx)
        , poll_handle(other.poll_handle)
        , poll_initialized(other.poll_initialized)
        , pool(other.pool)
        , jobs(std::move(other.jobs))
        , job_idx_ranges(std::move(other.job_idx_ranges))
        , current_type(other.current_type)
        , current_fire(other.current_fire)
        , push_partition_keys(std::move(other.push_partition_keys))
        , needs_poll_init(other.needs_poll_init.load(std::memory_order_relaxed))
    {
        other.conn             = nullptr;
        other.socket_fd        = -1;
        other.poll_initialized = false;
        other.current_type     = JobType::_SENTINEL;
    }

    DBConnection& operator=(DBConnection&& other) noexcept {
        if (this != &other) {
            conn             = other.conn;
            socket_fd        = other.socket_fd;
            idx              = other.idx;
            poll_handle      = other.poll_handle;
            poll_initialized = other.poll_initialized;
            pool             = other.pool;
            jobs             = std::move(other.jobs);
            job_idx_ranges   = std::move(other.job_idx_ranges);
            current_type     = other.current_type;
            current_fire     = other.current_fire;
            push_partition_keys = std::move(other.push_partition_keys);
            needs_poll_init.store(
                other.needs_poll_init.load(std::memory_order_relaxed),
                std::memory_order_relaxed);

            other.conn             = nullptr;
            other.socket_fd        = -1;
            other.poll_initialized = false;
            other.current_type     = JobType::_SENTINEL;
        }
        return *this;
    }

    DBConnection(const DBConnection&)            = delete;
    DBConnection& operator=(const DBConnection&) = delete;
};

} // namespace queen

#endif // _QUEEN_SLOT_POOL_HPP_
