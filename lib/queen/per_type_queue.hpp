#ifndef _QUEEN_PER_TYPE_QUEUE_HPP_
#define _QUEEN_PER_TYPE_QUEUE_HPP_

#include <uv.h>

#include <chrono>
#include <cstddef>
#include <deque>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "pending_job.hpp"

namespace queen {

// Layer 1 — thread-safe per-type job queue.
//
// Holds jobs and reports state; does NOT decide when to fire. Access from
// HTTP worker threads (push_back) and from the event-loop thread
// (take_front, oldest_age, erase_by_request_id) is serialized via a
// private `uv_mutex_t`.
//
// `front_enqueue_time` is maintained so the drain orchestrator can compute
// "age of the oldest queued job" in O(1) without scanning the deque.
class PerTypeQueue {
  public:
    using Clock      = std::chrono::steady_clock;
    using Pending    = std::shared_ptr<PendingJob>;

    PerTypeQueue() noexcept { uv_mutex_init(&_mutex); }
    ~PerTypeQueue() noexcept { uv_mutex_destroy(&_mutex); }

    PerTypeQueue(const PerTypeQueue&)            = delete;
    PerTypeQueue& operator=(const PerTypeQueue&) = delete;
    PerTypeQueue(PerTypeQueue&&)                 = delete;
    PerTypeQueue& operator=(PerTypeQueue&&)      = delete;

    // Thread-safe. Returns true if this pushed the transition-from-empty
    // (useful for submit-kick heuristics).
    bool
    push_back(Pending job) {
        uv_mutex_lock(&_mutex);
        bool was_empty = _deque.empty();
        if (was_empty) _front_enqueue_time = Clock::now();
        _deque.push_back(std::move(job));
        uv_mutex_unlock(&_mutex);
        return was_empty;
    }

    // Thread-safe. Push to front (used for requeueing on transient error /
    // POP backoff re-entry). Resets `front_enqueue_time` if the front changed.
    void
    push_front(Pending job) {
        uv_mutex_lock(&_mutex);
        _deque.push_front(std::move(job));
        _front_enqueue_time = Clock::now();
        uv_mutex_unlock(&_mutex);
    }

    // Thread-safe. Take up to `n` items from the front, FIFO order preserved.
    std::vector<Pending>
    take_front(size_t n) {
        std::vector<Pending> out;
        out.reserve(n);
        uv_mutex_lock(&_mutex);
        size_t take = std::min(n, _deque.size());
        for (size_t i = 0; i < take; ++i) {
            out.push_back(std::move(_deque.front()));
            _deque.pop_front();
        }
        if (_deque.empty()) {
            _front_enqueue_time = {};
        } else {
            // The new front is a pre-existing job; its notional enqueue time
            // is unchanged from the caller's perspective. Keep the recorded
            // `front_enqueue_time` as a safe upper bound on queued age.
            // (A more precise per-job enqueue stamp would require a second
            // deque; not worth the hot-path cost.)
        }
        uv_mutex_unlock(&_mutex);
        return out;
    }

    // PUSHSER: take a disjoint-partition batch for the push engine. Scans FIFO
    // and selects up to `n` jobs such that:
    //   * no selected job touches a partition in `excluded` (the partitions with
    //     an in-flight push transaction) -> at most one in-flight transaction
    //     per partition; and
    //   * this batch claims at most `max_parts` DISTINCT partitions, so a single
    //     batch cannot monopolize every partition and starve the other (up to C)
    //     concurrent push transactions. Jobs whose NEW partitions would exceed
    //     `max_parts` are left in place (picked up by a sibling/next batch);
    //     jobs for partitions already in this batch are still coalesced in.
    // The very first selected job is always admitted even if it alone exceeds
    // max_parts (guarantees progress for a single wide multi-partition push).
    // Skipped/over-limit jobs are kept in place with FIFO order preserved. A job
    // with no partition_keys claims nothing (SQL advisory lock is its backstop).
    std::vector<Pending>
    take_front_disjoint(size_t n,
                        const std::unordered_set<std::string>& excluded,
                        size_t max_parts) {
        std::vector<Pending> out;
        out.reserve(std::min(n, size_t{64}));
        uv_mutex_lock(&_mutex);
        std::deque<Pending> kept;
        std::unordered_set<std::string> batch_parts;
        for (auto& job : _deque) {
            bool eligible = out.size() < n;
            if (eligible) {
                size_t new_parts = 0;
                for (const auto& k : job->job.partition_keys) {
                    if (excluded.count(k)) { eligible = false; break; }
                    if (!batch_parts.count(k)) ++new_parts;
                }
                // Cap distinct partitions per batch (except the first job, which
                // always goes in so a lone wide push still makes progress).
                if (eligible && !batch_parts.empty()
                    && batch_parts.size() + new_parts > max_parts) {
                    eligible = false;
                }
            }
            if (eligible) {
                for (const auto& k : job->job.partition_keys) batch_parts.insert(k);
                out.push_back(std::move(job));
            } else {
                kept.push_back(std::move(job));
            }
        }
        _deque.swap(kept);
        // Conservative: any kept job is blocked/deferred and will be re-evaluated
        // on the next slot-free/submit/timer kick, so resetting the front stamp
        // here does not strand it (its partition frees -> drain -> taken).
        _front_enqueue_time = _deque.empty() ? Clock::time_point{} : Clock::now();
        uv_mutex_unlock(&_mutex);
        return out;
    }

    // Snapshot of current state. Returns oldest_age=0 when empty.
    struct Snapshot {
        size_t                    size;
        std::chrono::milliseconds oldest_age;
    };

    Snapshot
    snapshot(Clock::time_point now) const {
        Snapshot s{0, std::chrono::milliseconds(0)};
        uv_mutex_lock(&_mutex);
        s.size = _deque.size();
        if (s.size > 0 && _front_enqueue_time.time_since_epoch().count() != 0) {
            s.oldest_age = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - _front_enqueue_time);
            if (s.oldest_age.count() < 0) s.oldest_age = std::chrono::milliseconds(0);
        }
        uv_mutex_unlock(&_mutex);
        return s;
    }

    size_t
    size() const noexcept {
        uv_mutex_lock(&_mutex);
        size_t s = _deque.size();
        uv_mutex_unlock(&_mutex);
        return s;
    }

    // Thread-safe. Remove the first job matching `request_id`. Returns true
    // if found. Maintains `front_enqueue_time` correctly.
    bool
    erase_by_request_id(const std::string& request_id) {
        uv_mutex_lock(&_mutex);
        bool erased = false;
        for (auto it = _deque.begin(); it != _deque.end(); ++it) {
            if ((*it)->job.request_id == request_id) {
                bool was_front = (it == _deque.begin());
                _deque.erase(it);
                if (_deque.empty()) {
                    _front_enqueue_time = {};
                } else if (was_front) {
                    _front_enqueue_time = Clock::now();
                }
                erased = true;
                break;
            }
        }
        uv_mutex_unlock(&_mutex);
        return erased;
    }

    // Test-only / diagnostic accessor.
    Clock::time_point
    front_enqueue_time() const {
        uv_mutex_lock(&_mutex);
        auto t = _front_enqueue_time;
        uv_mutex_unlock(&_mutex);
        return t;
    }

  private:
    mutable uv_mutex_t _mutex;
    std::deque<Pending> _deque;
    Clock::time_point   _front_enqueue_time{};
};

} // namespace queen

#endif // _QUEEN_PER_TYPE_QUEUE_HPP_
