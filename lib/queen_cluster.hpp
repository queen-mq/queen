#ifndef _QUEEN_CLUSTER_HPP_
#define _QUEEN_CLUSTER_HPP_

#include <functional>
#include <memory>
#include <string>

#include "queen.hpp"

namespace queen {

// ============================================================================
// QueenCluster - function-split engine topology (push-serialization arch)
// ============================================================================
// Owns exactly three libqueen engines, process-global and decoupled from
// NUM_WORKERS, and routes each job to the right one by JobType:
//
//   PUSH / ACK / TRANSACTION  -> push engine  (owns the per-partition in-flight
//                                              gate; all message writes funnel
//                                              here so the gate is single-thread
//                                              and lock-free)
//   POP                       -> pop engine   (isolated; pop path unchanged)
//   everything else           -> rest engine  (custom / partition_lookup /
//                                              renew_lease / streams / stats)
//
// `submit()` and `invalidate_request()` are drop-in compatible with the old
// per-worker `Queen*` the routes used, so route handlers need no change beyond
// the context pointer type. Engines are shared across all HTTP worker threads;
// Queen::submit is already thread-safe (per-type queue mutex + uv_async_send),
// so concurrent submits from many workers are safe by construction.
class QueenCluster {
  public:
    QueenCluster(std::unique_ptr<Queen> push_engine,
                 std::unique_ptr<Queen> pop_engine,
                 std::unique_ptr<Queen> rest_engine) noexcept
        : _push(std::move(push_engine)),
          _pop(std::move(pop_engine)),
          _rest(std::move(rest_engine)) {}

    QueenCluster(const QueenCluster&)            = delete;
    QueenCluster& operator=(const QueenCluster&) = delete;

    // Route a job type to its owning engine.
    Queen*
    engine_for(JobType t) noexcept {
        switch (t) {
            case JobType::PUSH:
            case JobType::ACK:
            case JobType::TRANSACTION:
                return _push.get();
            case JobType::POP:
                return _pop.get();
            default:
                return _rest.get();
        }
    }

    // Thread-safe (delegates to Queen::submit).
    void
    submit(JobRequest&& job, std::function<void(std::string result)> cb) {
        engine_for(job.op_type)->submit(std::move(job), std::move(cb));
    }

    // Parked POPs (the pop-backoff tracker) live only on the pop engine, and
    // invalidate_request is only ever called for POP requests (pop.cpp).
    bool
    invalidate_request(const std::string& request_id) noexcept(true) {
        return _pop->invalidate_request(request_id);
    }

    // Wake parked POPs on a message-available notification (UDP / local push).
    void
    update_pop_backoff_tracker(const std::string& queue_name,
                               const std::string& partition_name) {
        _pop->update_pop_backoff_tracker(queue_name, partition_name);
    }

    Queen* push_engine() noexcept { return _push.get(); }
    Queen* pop_engine()  noexcept { return _pop.get();  }
    Queen* rest_engine() noexcept { return _rest.get(); }

  private:
    std::unique_ptr<Queen> _push;
    std::unique_ptr<Queen> _pop;
    std::unique_ptr<Queen> _rest;
};

} // namespace queen

#endif // _QUEEN_CLUSTER_HPP_
