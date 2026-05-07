#ifndef _QUEEN_BATCH_POLICY_HPP_
#define _QUEEN_BATCH_POLICY_HPP_

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <string>

#include "pending_job.hpp"

namespace queen {

// Layer 2 — per-type batch policy (Triton-style).
//
// Pure function of per-type queue state. Answers exactly one question:
// should we fire a batch now? And if so, how many items should we take?
//
// The policy knows NOTHING about slots or concurrency. It is pure.
enum class FireDecision { FIRE, HOLD };

struct BatchPolicy {
    size_t                   preferred_batch_size = 50;
    std::chrono::milliseconds max_hold_ms{20};
    size_t                   max_batch_size       = 500;
    size_t                   max_concurrent       = 4;  // owned by policy struct for
                                                        // convenience; the actual gate
                                                        // lives in ConcurrencyController.

    // Fire iff queue_size >= preferred OR oldest queued job has waited max_hold_ms.
    // Empty queue always HOLDs.
    FireDecision
    should_fire(size_t queue_size,
                std::chrono::milliseconds oldest_age) const noexcept {
        if (queue_size == 0) return FireDecision::HOLD;
        if (queue_size >= preferred_batch_size) return FireDecision::FIRE;
        if (oldest_age >= max_hold_ms) return FireDecision::FIRE;
        return FireDecision::HOLD;
    }

    // How many items to take in the current fire. Bounded by max_batch_size.
    size_t
    batch_size_to_take(size_t queue_size) const noexcept {
        return std::min(queue_size, max_batch_size);
    }
};

namespace detail {

inline int
env_int(const char* name, int fallback) noexcept {
    const char* v = std::getenv(name);
    if (!v || !*v) return fallback;
    try { return std::atoi(v); } catch (...) { return fallback; }
}

inline const char*
job_type_env_segment(JobType t) noexcept {
    switch (t) {
        case JobType::PUSH:                   return "PUSH";
        case JobType::POP:                    return "POP";
        case JobType::ACK:                    return "ACK";
        case JobType::TRANSACTION:            return "TRANSACTION";
        case JobType::RENEW_LEASE:            return "RENEW_LEASE";
        case JobType::CUSTOM:                 return "CUSTOM";
        case JobType::STREAMS_REGISTER_QUERY: return "STREAMS_REGISTER";
        case JobType::STREAMS_CYCLE:          return "STREAMS_CYCLE";
        case JobType::STREAMS_STATE_GET:      return "STREAMS_STATE_GET";
        default:                              return "UNKNOWN";
    }
}

struct BatchPolicyDefaults {
    size_t preferred_batch_size;
    int    max_hold_ms;
    size_t max_batch_size;
    size_t max_concurrent;
};

inline BatchPolicyDefaults
default_batch_policy_for(JobType t) noexcept {
    // Defaults per LIBQUEEN_IMPROVEMENTS.md §9.2, with `max_concurrent` raised
    // from the original plan values (4/4/4 for PUSH/POP/ACK) to the empirically
    // optimal ceiling discovered by the 2026-04-22 Vegas-uncapped sweep
    // (see test-perf/results/sweep_2026-04-22_07-41-19):
    //
    //   - Vegas naturally converges well below these ceilings on small PG
    //     (to ~17 for 1 KB payloads, ~6 for 10 KB) driven by observed RTT.
    //   - The old cap of 4 artificially throttled throughput by ~74% on a
    //     2-core PG / 2-worker S1-equivalent run.
    //   - Raising PUSH to 24 lets Vegas explore the real knee; ACK/POP to 16
    //     because their advisory-lock contention caps usable parallelism.
    //
    // Layout: {preferred, max_hold_ms, max_batch, max_concurrent}.
    //
    // queen-streams entries:
    //   STREAMS_REGISTER_QUERY: one-shot at SDK startup; {1, 0, 1, 2} mirrors
    //     TRANSACTION shape — single, no hold, low concurrency.
    //   STREAMS_CYCLE: per-partition atomic cycle. v0.1 keeps max_batch_size=1
    //     (plan requirement) but max_concurrent=16 because Queen's
    //     per-partition lease exclusivity means many partitions' cycles can
    //     commit in parallel without contending. Vegas adapts down if PG
    //     pushes back.
    //   STREAMS_STATE_GET: pure read, batchable like POP; mirrors POP shape.
    switch (t) {
        case JobType::PUSH:                   return {50,  20,  500, 24};
        case JobType::POP:                    return {20,   5,  500, 16};
        case JobType::ACK:                    return {50,  20,  500, 16};
        case JobType::TRANSACTION:            return { 1,   0,    1,  1};
        case JobType::RENEW_LEASE:            return {10, 100,  100,  2};
        case JobType::CUSTOM:                 return { 1,   0,    1,  1};
        case JobType::STREAMS_REGISTER_QUERY: return { 1,   0,    1,  2};
        case JobType::STREAMS_CYCLE:          return { 1,   0,    1, 16};
        case JobType::STREAMS_STATE_GET:      return {20,   5,  500, 16};
        default:                              return { 1,   5,    1,  1};
    }
}

} // namespace detail

// Build the policy for a given type from environment variables, falling back
// to the plan-specified defaults.
//
// `SIDECAR_MICRO_BATCH_WAIT_MS` (env) or `legacy_wait_ms_override` (constructor
// argument) acts as the global fallback for any type-specific MAX_HOLD_MS that
// isn't explicitly set (plan §9.1).
inline BatchPolicy
make_batch_policy_from_env(JobType t, int legacy_wait_ms_override = -1) noexcept {
    auto d  = detail::default_batch_policy_for(t);
    auto seg = detail::job_type_env_segment(t);

    auto name = [&](const char* knob) {
        return std::string("QUEEN_") + seg + "_" + knob;
    };

    int legacy_hold =
        (legacy_wait_ms_override >= 0)
            ? legacy_wait_ms_override
            : detail::env_int("SIDECAR_MICRO_BATCH_WAIT_MS", d.max_hold_ms);

    BatchPolicy p;
    p.preferred_batch_size = static_cast<size_t>(detail::env_int(
        name("PREFERRED_BATCH_SIZE").c_str(), static_cast<int>(d.preferred_batch_size)));
    p.max_hold_ms = std::chrono::milliseconds(detail::env_int(
        name("MAX_HOLD_MS").c_str(), legacy_hold));
    p.max_batch_size = static_cast<size_t>(detail::env_int(
        name("MAX_BATCH_SIZE").c_str(), static_cast<int>(d.max_batch_size)));
    p.max_concurrent = static_cast<size_t>(detail::env_int(
        name("MAX_CONCURRENT").c_str(), static_cast<int>(d.max_concurrent)));

    // Sanitize.
    if (p.preferred_batch_size == 0) p.preferred_batch_size = 1;
    if (p.max_batch_size       == 0) p.max_batch_size       = 1;
    if (p.max_concurrent       == 0) p.max_concurrent       = 1;
    if (p.max_hold_ms.count() < 0)  p.max_hold_ms = std::chrono::milliseconds(0);

    return p;
}

} // namespace queen

#endif // _QUEEN_BATCH_POLICY_HPP_
