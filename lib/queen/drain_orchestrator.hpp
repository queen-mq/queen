#ifndef _QUEEN_DRAIN_ORCHESTRATOR_HPP_
#define _QUEEN_DRAIN_ORCHESTRATOR_HPP_

#include <array>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>

#include "batch_policy.hpp"
#include "concurrency/concurrency_controller.hpp"
#include "concurrency/static_limit.hpp"
#include "concurrency/vegas_limit.hpp"
#include "metrics.hpp"
#include "pending_job.hpp"
#include "per_type_queue.hpp"

namespace queen {

// Aggregate of per-type state owned by Queen.
//
// Layer responsibilities (plan §5):
//   Layer 1: `queue`        — thread-safe job queue, reports state.
//   Layer 2: `policy`       — decides FIRE / HOLD given state.
//   Layer 3: `concurrency`  — gates in-flight batch count.
//   (Layer 4 — drain orchestration — lives in the Queen class itself, since
//    it must touch slots, libpq, and libuv handles which the Queen owns.)
struct PerTypeState {
    PerTypeQueue                          queue;
    BatchPolicy                           policy;
    std::unique_ptr<ConcurrencyController> concurrency;
    PerTypeMetrics                        metrics;
};

// Concurrency-mode selection (`QUEEN_CONCURRENCY_MODE`, default `vegas`).
enum class ConcurrencyMode { Static, Vegas };

inline ConcurrencyMode
concurrency_mode_from_env() noexcept {
    const char* v = std::getenv("QUEEN_CONCURRENCY_MODE");
    if (!v || !*v) return ConcurrencyMode::Vegas;
    std::string s(v);
    if (s == "static") return ConcurrencyMode::Static;
    return ConcurrencyMode::Vegas;
}

// Per-type concurrency mode. PUSH defaults to STATIC at its policy limit (~16,
// the measured flat optimum): the per-partition gate already bounds per-
// partition concurrency, and Vegas (RTT-adaptive) structurally under-shoots the
// push optimum to ~10 because each disjoint coalesced commit has a higher RTT,
// leaving ~40% throughput on the table. Override per type with
// QUEEN_PUSH_CONCURRENCY_MODE=vegas|static. All other types follow the global
// QUEEN_CONCURRENCY_MODE (Vegas by default) for adaptive backpressure.
inline ConcurrencyMode
concurrency_mode_for(JobType t, ConcurrencyMode global_default) noexcept {
    if (t == JobType::PUSH) {
        const char* v = std::getenv("QUEEN_PUSH_CONCURRENCY_MODE");
        if (v && std::string(v) == "vegas")  return ConcurrencyMode::Vegas;
        if (v && std::string(v) == "static") return ConcurrencyMode::Static;
        return ConcurrencyMode::Static;
    }
    return global_default;
}

inline VegasLimit::Config
vegas_config_from_env(uint16_t per_type_max_concurrent) noexcept {
    VegasLimit::Config c;
    c.min_limit =
        static_cast<uint16_t>(detail::env_int("QUEEN_VEGAS_MIN_LIMIT", 1));
    // Global Vegas ceiling. Default raised from 16 → 32 (2026-04-22) so the
    // per-type `MAX_CONCURRENT` defaults (24 for PUSH, 16 for ACK/POP) are
    // not clipped. Operators who tune per-type MAX_CONCURRENT above 32 must
    // also bump this knob.
    uint16_t env_max =
        static_cast<uint16_t>(detail::env_int("QUEEN_VEGAS_MAX_LIMIT", 32));
    // Effective max is the tighter of per-type `MAX_CONCURRENT` and global
    // `QUEEN_VEGAS_MAX_LIMIT` (plan §9.3).
    c.max_limit =
        (per_type_max_concurrent < env_max) ? per_type_max_concurrent : env_max;
    c.alpha =
        static_cast<uint16_t>(detail::env_int("QUEEN_VEGAS_ALPHA", 3));
    // Shrink threshold raised 6 → 12 (2026-04-22) to scale with the new
    // default `MAX_CONCURRENT=24` for PUSH. Must be less than MAX_CONCURRENT
    // for Vegas to ever shrink (because queue_load ≤ in_flight ≤ cap).
    c.beta =
        static_cast<uint16_t>(detail::env_int("QUEEN_VEGAS_BETA", 12));
    c.rtt_window_samples = static_cast<uint16_t>(
        detail::env_int("QUEEN_VEGAS_RTT_WINDOW_SAMPLES", 50));
    c.rtt_min_window = std::chrono::seconds(
        detail::env_int("QUEEN_VEGAS_RTT_MIN_WINDOW_SEC", 30));
    c.update_interval = std::chrono::milliseconds(
        detail::env_int("QUEEN_VEGAS_UPDATE_INTERVAL_MS", 1000));
    return c;
}

// Build the concurrency controller for a given type given the configured
// policy (for its `max_concurrent`) and the global mode.
inline std::unique_ptr<ConcurrencyController>
make_concurrency_controller(const BatchPolicy& policy, ConcurrencyMode mode) {
    uint16_t per_type_max = static_cast<uint16_t>(policy.max_concurrent);
    if (mode == ConcurrencyMode::Static) {
        return std::make_unique<StaticLimit>(per_type_max);
    }
    return std::make_unique<VegasLimit>(vegas_config_from_env(per_type_max));
}

} // namespace queen

#endif // _QUEEN_DRAIN_ORCHESTRATOR_HPP_
