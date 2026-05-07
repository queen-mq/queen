#ifndef _QUEEN_HPP_
#define _QUEEN_HPP_
#ifdef __cplusplus
#include <uv.h>
#include <libpq-fe.h>
#include <json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "queen/batch_policy.hpp"
#include "queen/concurrency/concurrency_controller.hpp"
#include "queen/concurrency/static_limit.hpp"
#include "queen/concurrency/vegas_limit.hpp"
#include "queen/drain_orchestrator.hpp"
#include "queen/metrics.hpp"
#include "queen/pending_job.hpp"
#include "queen/per_type_queue.hpp"
#include "queen/slot_pool.hpp"
#include "worker_metrics.hpp"

namespace queen {

// UUIDv7 generator - time-ordered UUIDs for proper message ordering
inline std::string
generate_uuidv7() {
    static std::mutex uuid_mutex;
    static uint64_t last_ms = 0;
    static uint16_t sequence = 0;
    static std::random_device rd;
    static std::mt19937_64 gen(rd());

    std::lock_guard<std::mutex> lock(uuid_mutex);

    auto now = std::chrono::system_clock::now();
    uint64_t current_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

    if (current_ms <= last_ms) {
        sequence++;
    } else {
        last_ms = current_ms;
        sequence = 0;
    }

    std::array<uint8_t, 16> bytes;

    bytes[0] = (last_ms >> 40) & 0xFF;
    bytes[1] = (last_ms >> 32) & 0xFF;
    bytes[2] = (last_ms >> 24) & 0xFF;
    bytes[3] = (last_ms >> 16) & 0xFF;
    bytes[4] = (last_ms >> 8) & 0xFF;
    bytes[5] = last_ms & 0xFF;

    uint16_t sequence_and_version = sequence & 0x0FFF;
    bytes[6] = 0x70 | (sequence_and_version >> 8);
    bytes[7] = sequence_and_version & 0xFF;

    uint64_t rand_data = gen();
    bytes[8] = 0x80 | ((rand_data >> 56) & 0x3F);
    bytes[9] = (rand_data >> 48) & 0xFF;
    bytes[10] = (rand_data >> 40) & 0xFF;
    bytes[11] = (rand_data >> 32) & 0xFF;
    bytes[12] = (rand_data >> 24) & 0xFF;
    bytes[13] = (rand_data >> 16) & 0xFF;
    bytes[14] = (rand_data >> 8) & 0xFF;
    bytes[15] = rand_data & 0xFF;

    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (int i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) ss << '-';
        ss << std::setw(2) << static_cast<int>(bytes[i]);
    }

    return ss.str();
}

// ============================================================================
// Schema Initialization
// ============================================================================

namespace detail {

inline std::string
read_sql_file(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open SQL file: " + path);
    }
    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

inline std::vector<std::string>
get_sql_files(const std::string& dir) {
    std::vector<std::string> files;
    if (!std::filesystem::exists(dir)) return files;

    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.path().extension() == ".sql") {
            files.push_back(entry.path().string());
        }
    }
    std::sort(files.begin(), files.end());
    return files;
}

inline bool
exec_sql(PGconn* conn, const std::string& sql, const std::string& context = "") {
    if (!PQsendQuery(conn, sql.c_str())) {
        spdlog::error("[libqueen] [{}] PQsendQuery failed: {}", context, PQerrorMessage(conn));
        return false;
    }

    while (PQisBusy(conn)) {
        if (!PQconsumeInput(conn)) {
            spdlog::error("[libqueen] [{}] PQconsumeInput failed: {}", context, PQerrorMessage(conn));
            return false;
        }
    }

    bool success = true;
    PGresult* res;
    while ((res = PQgetResult(conn)) != nullptr) {
        ExecStatusType status = PQresultStatus(res);
        if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
            spdlog::error("[libqueen] [{}] SQL error: {}", context, PQerrorMessage(conn));
            success = false;
        }
        PQclear(res);
    }
    return success;
}

} // namespace detail

inline bool
initialize_schema(const std::string& connection_string,
                  const std::string& schema_dir = "schema") {
    PGconn* conn = PQconnectdb(connection_string.c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        spdlog::error("[libqueen] Failed to connect for schema initialization: {}",
                      PQerrorMessage(conn));
        PQfinish(conn);
        return false;
    }

    spdlog::info("[libqueen] Initializing schema from: {}", schema_dir);

    try {
        if (!detail::exec_sql(conn, "CREATE SCHEMA IF NOT EXISTS queen", "create schema")) {
            spdlog::error("[libqueen] Failed to create queen schema");
            PQfinish(conn);
            return false;
        }

        std::string schema_file = schema_dir + "/schema.sql";
        if (!std::filesystem::exists(schema_file)) {
            spdlog::error("[libqueen] Base schema file not found: {}", schema_file);
            PQfinish(conn);
            return false;
        }

        std::string schema_sql = detail::read_sql_file(schema_file);
        if (!detail::exec_sql(conn, schema_sql, "base schema")) {
            spdlog::error("[libqueen] Failed to apply base schema");
            PQfinish(conn);
            return false;
        }
        spdlog::info("[libqueen] Base schema applied successfully");

        std::string procedures_dir = schema_dir + "/procedures";
        auto procedure_files = detail::get_sql_files(procedures_dir);

        if (!procedure_files.empty()) {
            spdlog::info("[libqueen] Loading {} stored procedure(s)...", procedure_files.size());
            for (const auto& proc_file : procedure_files) {
                spdlog::debug("[libqueen] Loading: {}", proc_file);
                std::string proc_sql = detail::read_sql_file(proc_file);
                if (!detail::exec_sql(conn, proc_sql, proc_file)) {
                    spdlog::error("[libqueen] Failed to load procedure: {}", proc_file);
                    PQfinish(conn);
                    return false;
                }
            }
            spdlog::info("[libqueen] All stored procedures loaded successfully");
        }

        PQfinish(conn);
        spdlog::info("[libqueen] Schema initialization complete");
        return true;

    } catch (const std::exception& e) {
        spdlog::error("[libqueen] Schema initialization failed: {}", e.what());
        PQfinish(conn);
        return false;
    }
}

// ============================================================================
// Queen — libqueen core.
// ============================================================================
//
// Architecture (see LIBQUEEN_IMPROVEMENTS.md):
//
//   Layer 1: PerTypeQueue        — one thread-safe queue per JobType.
//   Layer 2: BatchPolicy         — Triton-style preferred_batch_size +
//                                  max_hold_ms + max_batch_size.
//   Layer 3: ConcurrencyController — static or Vegas, per type.
//   Layer 4: _drain_orchestrator() — event-driven: submit-kick, slot-free
//                                  kick, timer-kick. Only place that touches
//                                  slots, libpq, libuv handles.
//
// Event-loop-only state: `_db_connections[*]`, `_free_slot_indexes`,
// `_pop_backoff_tracker`, `_drain_pass_counter`, per-type `metrics`.
//
// Cross-thread state: `PerTypeQueue` internals, `_backoff_signal_queue`,
// `DBConnection::needs_poll_init` (atomic).
//
// Correctness invariant (plan §12 NEW): per-type in_flight counter cannot
// drift. Exactly one release per acquire, centralized in `_on_slot_freed`.
class Queen {
  public:
    Queen(const std::string& conn_str,
          uint16_t statement_timeout_ms = 30000,
          uint16_t db_connection_count = 10,
          uint16_t queue_interval_ms = 10,
          uint16_t pop_wait_initial_interval_ms = 100,
          uint16_t pop_wait_backoff_threshold = 3,
          double   pop_wait_backoff_multiplier = 3.0,
          uint16_t pop_wait_max_interval_ms = 5000,
          uint16_t worker_id = 0,
          const std::string& hostname = "localhost") noexcept(false)
        : _hostname(hostname),
          _loop(nullptr),
          _loop_initialized(false),
          _legacy_wait_ms(queue_interval_ms),
          _conn_str(conn_str),
          _statement_timeout_ms(statement_timeout_ms),
          _db_connection_count(db_connection_count),
          _pop_wait_initial_interval_ms(pop_wait_initial_interval_ms),
          _pop_wait_backoff_threshold(pop_wait_backoff_threshold),
          _pop_wait_backoff_multiplier(pop_wait_backoff_multiplier),
          _pop_wait_max_interval_ms(pop_wait_max_interval_ms),
          _worker_id(worker_id) {
        uv_mutex_init(&_mutex_backoff_signal);

        // In-flight deadline: a slot whose query hasn't completed within
        // this many ms is treated as dead (jobs requeued, slot disconnected,
        // reconnect thread takes over). This is the safety net for silent
        // network drops where neither libpq nor the kernel raises an error
        // — exactly the case observed during Cloud SQL maintenance windows.
        //
        // Default: statement_timeout + 5 s grace. The grace covers normal
        // round-trip + JSON marshalling + per-job dispatch; queries that
        // legitimately take longer would already have been killed on the PG
        // side by `SET statement_timeout`. Floor at 2 s so very aggressive
        // statement_timeout values (e.g. 100 ms) don't cause spurious kills.
        {
            const char* env = std::getenv("LIBQUEEN_INFLIGHT_DEADLINE_MS");
            uint64_t v = env ? static_cast<uint64_t>(std::atoll(env))
                             : static_cast<uint64_t>(statement_timeout_ms) + 5000ull;
            if (v < 2000ull) v = 2000ull;
            _inflight_deadline_ms = std::chrono::milliseconds(v);
        }

        // Build per-type state (policy + concurrency controller). The
        // `queue_interval_ms` constructor arg is honored as the legacy
        // fallback for any type-specific MAX_HOLD_MS that isn't set via
        // `QUEEN_<TYPE>_MAX_HOLD_MS` or `SIDECAR_MICRO_BATCH_WAIT_MS` (env).
        auto mode = concurrency_mode_from_env();
        for (size_t i = 0; i < JobTypeCount; ++i) {
            JobType t = job_type_from_index(i);
            _types[i].policy      = make_batch_policy_from_env(
                t, static_cast<int>(_legacy_wait_ms));
            _types[i].concurrency = make_concurrency_controller(_types[i].policy, mode);
        }

        _metrics = std::make_unique<WorkerMetrics>(
            _hostname,
            _worker_id,
            [this](const std::string& sql, const std::vector<std::string>& params) {
                _submit_metrics_write(sql, params);
            }
        );

        _init();
    }

    ~Queen() {
        _reconnect_running.store(false);
        if (_reconnect_thread.joinable()) {
            _reconnect_thread.join();
        }

        if (_loop_initialized) {
            uv_loop_close(_loop);
            delete _loop;
        }
        uv_mutex_destroy(&_mutex_backoff_signal);
    }

    // Thread-safe. Enqueue a job into its type-specific queue and fire a
    // submit-kick so the event loop evaluates whether to drain.
    void
    submit(JobRequest&& job, std::function<void(std::string result)> cb) {
        auto pending = std::make_shared<PendingJob>(PendingJob{std::move(job), std::move(cb)});
        size_t idx = job_type_index(pending->job.op_type);
        if (idx >= JobTypeCount) {
            spdlog::error("[Worker {}] [libqueen] submit: invalid job type {}", _worker_id, idx);
            return;
        }
        _types[idx].queue.push_back(pending);
        uv_async_send(&_queue_signal);
    }

    // Thread-safe. Remove the first job matching `request_id` across all
    // per-type queues and the POP backoff tracker (via a dedicated async
    // signal, since the backoff tracker is event-loop-only).
    bool
    invalidate_request(const std::string& request_id) noexcept(true) {
        try {
            bool erased = false;
            for (auto& ts : _types) {
                if (ts.queue.erase_by_request_id(request_id)) {
                    erased = true;
                    // Don't break — a given request_id is unique; but this is
                    // also the correctness cost of walking all types if not
                    // found.
                    break;
                }
            }

            // Tracker removal happens on the event-loop thread to avoid
            // racing with `_process_slot_result`. Record the ID and poke.
            {
                uv_mutex_lock(&_mutex_backoff_signal);
                _invalidate_ids.push_back(request_id);
                uv_mutex_unlock(&_mutex_backoff_signal);
                uv_async_send(&_backoff_signal);
            }
            return erased;
        } catch (const std::exception& e) {
            spdlog::error("[Worker {}] [libqueen] Failed to invalidate request: {}", _worker_id, e.what());
            return false;
        }
    }

    // Thread-safe. Asynchronously notify the event loop to wake pop-backoff
    // consumers waiting for this queue/partition.
    void
    update_pop_backoff_tracker(const std::string& queue_name, const std::string& partition_name) {
        uv_mutex_lock(&_mutex_backoff_signal);
        _backoff_signal_queue.push_back({queue_name, partition_name});
        uv_mutex_unlock(&_mutex_backoff_signal);
        uv_async_send(&_backoff_signal);
    }

  private:
    // ------------------------------------------------------------------
    // Worker identity / stats
    // ------------------------------------------------------------------
    std::string _hostname;

    std::chrono::steady_clock::time_point _last_timer_expected;
    uint64_t _event_loop_lag = 0;
    uint64_t _jobs_done = 0;
    uint16_t _stats_interval_ms = 1000;
    uint64_t _drain_passes_submit = 0;
    uint64_t _drain_passes_slot_free = 0;
    uint64_t _drain_passes_timer = 0;
    uint64_t _safety_timer_rearms_total = 0;

    std::unique_ptr<WorkerMetrics> _metrics;

    // ------------------------------------------------------------------
    // libuv loop / handles
    // ------------------------------------------------------------------
    uv_loop_t*        _loop;
    uv_timer_t        _queue_timer;   // safety-net / POP-deadline timer (re-armed).
    uv_timer_t        _stats_timer;   // fixed 1 Hz.
    uv_async_t        _queue_signal;  // submit-kick.
    std::atomic<bool> _loop_initialized;
    uint16_t          _legacy_wait_ms;  // SIDECAR_MICRO_BATCH_WAIT_MS; used only
                                        // as the legacy fallback for per-type
                                        // MAX_HOLD_MS in BatchPolicy::from_env.
    bool              _safety_timer_running = false;

    // Backoff signal (UDP notifications + invalidation requests).
    struct BackoffSignal {
        std::string queue_name;
        std::string partition_name;
    };
    uv_async_t              _backoff_signal;
    uv_mutex_t              _mutex_backoff_signal;
    std::deque<BackoffSignal> _backoff_signal_queue;
    std::deque<std::string>   _invalidate_ids;

    // ------------------------------------------------------------------
    // Per-type state (Layers 1–3)
    // ------------------------------------------------------------------
    std::array<PerTypeState, JobTypeCount> _types;
    size_t _drain_pass_counter = 0;

    // ------------------------------------------------------------------
    // DB connections / slots
    // ------------------------------------------------------------------
    std::string _conn_str;
    uint16_t    _statement_timeout_ms;
    uint16_t    _db_connection_count;
    std::vector<uint16_t>     _free_slot_indexes;
    std::vector<DBConnection> _db_connections;

    // Slots whose in-flight query has been outstanding longer than this
    // are treated as dead in `_check_stuck_slots` (event-loop thread).
    // Initialized in the constructor from LIBQUEEN_INFLIGHT_DEADLINE_MS.
    std::chrono::milliseconds _inflight_deadline_ms{std::chrono::milliseconds(35000)};

    // ------------------------------------------------------------------
    // POP long-poll state
    // ------------------------------------------------------------------
    uint16_t _pop_wait_initial_interval_ms;
    uint16_t _pop_wait_backoff_threshold;
    double   _pop_wait_backoff_multiplier;
    uint16_t _pop_wait_max_interval_ms;
    uint16_t _max_backoff_age_seconds = 65;
    uint16_t _worker_id;

    // Backoff tracker: "queue/partition" or "queue/*" -> (request_id ->
    // pending). Holds parked POPs that are waiting on next_check or a push
    // notification. Event-loop-only.
    std::unordered_map<std::string,
                       std::map<std::string, std::shared_ptr<PendingJob>>>
        _pop_backoff_tracker;

    static std::string
    _get_backoff_key(const std::string& queue_name,
                     const std::string& partition_name) noexcept(true) {
        if (partition_name.empty()) return queue_name + "/*";
        return queue_name + "/" + partition_name;
    }

    // ------------------------------------------------------------------
    // Reconnection thread
    // ------------------------------------------------------------------
    std::thread       _reconnect_thread;
    std::atomic<bool> _reconnect_running{false};
    uv_async_t        _reconnect_signal;

    //  _       _ _
    // (_) _ _ <_> |_
    // | || ' || | _|
    // |_||_|_||_|\__|
    void
    _init() noexcept(false) {
        std::thread([this] {
            if (_loop_initialized) return;

            _loop = new uv_loop_t;
            if (uv_loop_init(_loop) != 0) {
                delete _loop;
                _loop = nullptr;
                throw std::runtime_error("Failed to initialize libuv loop");
            }
            _loop_initialized.store(true);

            uv_handle_set_data((uv_handle_t*)&_queue_timer,      this);
            uv_handle_set_data((uv_handle_t*)&_stats_timer,      this);
            uv_handle_set_data((uv_handle_t*)&_queue_signal,     this);
            uv_handle_set_data((uv_handle_t*)&_reconnect_signal, this);
            uv_handle_set_data((uv_handle_t*)&_backoff_signal,   this);

            uv_timer_init(_loop, &_queue_timer);
            uv_timer_init(_loop, &_stats_timer);
            uv_timer_start(&_stats_timer, _uv_stats_timer_cb, 0, _stats_interval_ms);

            uv_async_init(_loop, &_queue_signal,     _uv_async_cb);
            uv_async_init(_loop, &_reconnect_signal, _uv_reconnect_signal_cb);
            uv_async_init(_loop, &_backoff_signal,   _uv_backoff_signal_cb);

            if (!_connect_all_slots()) {
                throw std::runtime_error("Failed to connect all slots to the database");
            }

            _start_reconnect_thread();

            _last_timer_expected = std::chrono::steady_clock::now();
            uv_run(_loop, UV_RUN_DEFAULT);
        }).detach();
    }

    // -------- Async/timer callbacks: all three funnel into _drain_orchestrator.

    // Submit-kick (plan §5.5): called via uv_async_send from submit().
    // uv_async_send coalesces, so one drain pass may service many submits.
    static void
    _uv_async_cb(uv_async_t* handle) noexcept(false) {
        auto* self = static_cast<Queen*>(uv_handle_get_data((uv_handle_t*)handle));
        ++self->_drain_passes_submit;
        self->_drain_orchestrator();
    }

    // Timer-kick (plan §5.5): max-hold safety net + earliest POP next_check.
    // Fires as a one-shot; `_drain_orchestrator` re-arms at the end.
    static void
    _uv_timer_cb(uv_timer_t* handle) noexcept(false) {
        auto* self = static_cast<Queen*>(uv_handle_get_data((uv_handle_t*)handle));

        auto now = std::chrono::steady_clock::now();
        uint64_t lag = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - self->_last_timer_expected).count();
        self->_event_loop_lag = lag;
        self->_last_timer_expected = now;

        ++self->_drain_passes_timer;
        self->_drain_orchestrator();
    }

    static void
    _uv_reconnect_signal_cb(uv_async_t* handle) noexcept(false) {
        auto* self = static_cast<Queen*>(uv_handle_get_data((uv_handle_t*)handle));
        self->_finalize_reconnected_slots();
        // Newly available slots may accept work immediately.
        self->_drain_orchestrator();
    }

    static void
    _uv_backoff_signal_cb(uv_async_t* handle) noexcept(true) {
        auto* self = static_cast<Queen*>(uv_handle_get_data((uv_handle_t*)handle));
        self->_process_backoff_signals();
        // Wake-ups may have made POPs ready; run a drain pass.
        self->_drain_orchestrator();
    }

    void
    _process_backoff_signals() noexcept(true) {
        std::deque<BackoffSignal> signals;
        std::deque<std::string>   invalidations;

        uv_mutex_lock(&_mutex_backoff_signal);
        signals.swap(_backoff_signal_queue);
        invalidations.swap(_invalidate_ids);
        uv_mutex_unlock(&_mutex_backoff_signal);

        for (const auto& sig : signals) {
            _update_pop_backoff_tracker(sig.queue_name, sig.partition_name);
        }

        for (const auto& id : invalidations) {
            _invalidate_from_backoff_tracker(id);
        }

        if (!signals.empty()) {
            spdlog::debug("[Worker {}] [libqueen] Processed {} backoff signals from UDP",
                          _worker_id, signals.size());
        }
    }

    void
    _invalidate_from_backoff_tracker(const std::string& request_id) noexcept(true) {
        for (auto it = _pop_backoff_tracker.begin(); it != _pop_backoff_tracker.end(); ) {
            it->second.erase(request_id);
            if (it->second.empty()) {
                it = _pop_backoff_tracker.erase(it);
            } else {
                ++it;
            }
        }
    }

    // Event-loop-thread: initialize poll handles for slots that were
    // reconnected by the background thread.
    void
    _finalize_reconnected_slots() noexcept(true) {
        for (uint16_t i = 0; i < _db_connection_count; i++) {
            DBConnection& slot = _db_connections[i];

            if (!slot.needs_poll_init.load(std::memory_order_acquire)) {
                continue;
            }

            if (slot.socket_fd >= 0) {
                if (uv_poll_init(_loop, &slot.poll_handle, slot.socket_fd) == 0) {
                    slot.poll_handle.data = &slot;
                    slot.poll_initialized = true;

                    _free_slot_indexes.push_back(i);
                    spdlog::info("[Worker {}] [libqueen] Slot {} poll initialized and added to pool",
                                 _worker_id, i);
                } else {
                    spdlog::error("[Worker {}] [libqueen] Failed to init poll for reconnected slot {}",
                                  _worker_id, i);
                    PQfinish(slot.conn);
                    slot.conn = nullptr;
                    slot.socket_fd = -1;
                }
            }

            slot.needs_poll_init.store(false, std::memory_order_release);
        }
    }

    // -------- Stats timer.

    static void
    _uv_stats_timer_cb(uv_timer_t* handle) noexcept(false) {
        auto* self = static_cast<Queen*>(uv_handle_get_data((uv_handle_t*)handle));

        self->_cleanup_stale_backoff_entries();
        self->_check_stuck_slots();

        size_t backoff_size            = self->_pop_backoff_tracker.size();
        size_t free_slot_indexes_size  = self->_free_slot_indexes.size();
        size_t db_connections_size     = self->_db_connections.size();

        // Aggregate queue depth across all types.
        size_t job_queue_size = 0;
        for (auto& ts : self->_types) job_queue_size += ts.queue.size();

        int64_t event_loop_lag = static_cast<int64_t>(self->_event_loop_lag);
        if (event_loop_lag < 0) event_loop_lag = 0;

        if (self->_metrics) {
            self->_metrics->record_stats_sample(
                static_cast<uint64_t>(event_loop_lag),
                static_cast<uint16_t>(free_slot_indexes_size),
                static_cast<uint16_t>(db_connections_size),
                job_queue_size,
                backoff_size,
                self->_jobs_done
            );

            // Per-queue sample of currently-parked POP requests. Cheap
            // O(N) walk of the backoff tracker; the sample is *added* to
            // a running per-queue sum inside WorkerMetrics, then divided
            // by the number of samples at minute-boundary flush so the
            // value persisted is the minute-averaged gauge instead of a
            // single point-in-time reading.
            //
            // backoff_key shape is "queue/partition" or "queue/*" (see
            // _get_backoff_key). We split on the *last* '/' since queue
            // names cannot contain '/'. Multiple partitions of the same
            // queue accumulate into the same map entry.
            std::unordered_map<std::string, uint64_t> parked_by_queue;
            parked_by_queue.reserve(self->_pop_backoff_tracker.size());
            for (const auto& [key, request_map] : self->_pop_backoff_tracker) {
                auto slash = key.rfind('/');
                if (slash == std::string::npos) continue;
                parked_by_queue[key.substr(0, slash)] += request_map.size();
            }
            self->_metrics->add_parked_sample(parked_by_queue);

            self->_metrics->check_and_flush();
        }

        if (self->_jobs_done > 0 || job_queue_size > 0) {
            auto jobs_per_second = self->_jobs_done / (self->_stats_interval_ms / 1000.0);

            // Replace the old EVL: line with the per-type summary proposed
            // in plan §10 (`push(q=.. f=../.. p99rtt=..ms) …`).
            std::ostringstream line;
            line << "[Worker " << self->_worker_id << "] [libqueen] ";
            bool first = true;
            for (size_t i = 0; i < JobTypeCount; ++i) {
                auto& ts = self->_types[i];
                size_t q = ts.queue.size();
                if (q == 0 && ts.concurrency->in_flight() == 0
                    && ts.metrics.batches_fired_total == 0) continue;
                if (!first) line << " ";
                first = false;
                line << job_type_name(job_type_from_index(i))
                     << "(q=" << q
                     << " f=" << ts.concurrency->in_flight()
                     << "/"   << ts.concurrency->current_limit()
                     << " p99rtt=" << ts.metrics.rtt_percentile_ms(99.0) << "ms)";
            }
            line << " slots=" << free_slot_indexes_size << "/" << db_connections_size
                 << " jobs/s=" << static_cast<uint64_t>(jobs_per_second)
                 << " drains=" << (self->_drain_passes_submit + self->_drain_passes_slot_free
                                   + self->_drain_passes_timer)
                 << " evl=" << event_loop_lag << "ms";
            spdlog::info("{}", line.str());
        }

        self->_jobs_done = 0;
    }

    // Scan every slot for queries that have been in-flight longer than
    // `_inflight_deadline_ms`. Such slots are recycled the same way as a
    // poll-error or PQconsumeInput failure: jobs requeued, slot
    // disconnected, concurrency released, drain re-kicked. The reconnect
    // thread then picks up the disconnected slot and rebuilds it.
    //
    // This is the safety net for silent network drops (cloud-managed PG
    // maintenance, load-balancer reroutes, hypervisor pause, etc.) where
    // neither libpq nor the kernel surfaces an error on the FD within
    // a useful timeframe. Without this check, a stuck slot would keep
    // its concurrency counter held indefinitely and block new fires of
    // the same JobType.
    //
    // Runs on the event-loop thread (called from `_uv_stats_timer_cb`),
    // so it shares all the same single-thread invariants as the rest of
    // the orchestrator.
    void
    _check_stuck_slots() noexcept(false) {
        auto now = std::chrono::steady_clock::now();

        for (uint16_t i = 0; i < _db_connection_count; i++) {
            DBConnection& slot = _db_connections[i];

            // current_type == _SENTINEL means the slot is idle. There is
            // no in-flight query, so the deadline does not apply.
            if (slot.current_type == JobType::_SENTINEL) continue;

            // If the connection has already been torn down (slot.conn ==
            // nullptr), the reconnect thread owns it. Don't double-handle.
            if (slot.conn == nullptr) continue;

            auto age = now - slot.current_fire.fire_time;
            if (age <= _inflight_deadline_ms) continue;

            auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(age).count();
            spdlog::warn(
                "[Worker {}] [libqueen] Slot {} in-flight deadline exceeded "
                "({} ms > {} ms, type={}, batch={}) - treating as dead connection",
                _worker_id, slot.idx, age_ms, _inflight_deadline_ms.count(),
                job_type_name(slot.current_type),
                slot.current_fire.batch_size);

            _handle_slot_error(
                slot,
                "In-flight deadline exceeded after " +
                std::to_string(age_ms) + "ms (suspected silent network drop)");
        }
    }

    void
    _cleanup_stale_backoff_entries() noexcept(true) {
        auto now = std::chrono::steady_clock::now();
        auto STALE_THRESHOLD = std::chrono::seconds(_max_backoff_age_seconds);

        size_t removed = 0;

        for (auto queue_it = _pop_backoff_tracker.begin(); queue_it != _pop_backoff_tracker.end(); ) {
            auto& request_map = queue_it->second;

            for (auto req_it = request_map.begin(); req_it != request_map.end(); ) {
                auto& job_ptr = req_it->second;

                if (job_ptr && (now - job_ptr->job.next_check) > STALE_THRESHOLD) {
                    spdlog::debug("[Worker {}] [libqueen] Removing stale backoff entry: {}/{}",
                                  _worker_id, queue_it->first, req_it->first);
                    req_it = request_map.erase(req_it);
                    removed++;
                } else {
                    ++req_it;
                }
            }

            if (request_map.empty()) {
                queue_it = _pop_backoff_tracker.erase(queue_it);
            } else {
                ++queue_it;
            }
        }

        if (removed > 0) {
            spdlog::info("[Worker {}] [libqueen] Cleaned up {} stale backoff entries", _worker_id, removed);
        }
    }

    void
    _submit_metrics_write(const std::string& sql, const std::vector<std::string>& params) {
        JobRequest job_req;
        job_req.op_type   = JobType::CUSTOM;
        job_req.request_id = "metrics_" + generate_uuidv7();
        job_req.sql       = sql;
        job_req.params    = params;
        submit(std::move(job_req), [](std::string) {});
    }

    //    _               _                   _                    _
    //   | | _  ___  ._ _|_| ___  _ _  ___ | | ___  ___ ___ ___ _| |
    //  _| || )/ . \| '_|| |/ ._>| '_>/ . \| |/ . \/ ._>/ ._>/ . |
    //  \__/|/ \___/|_|  |_|\___.|_|  \___/|_|\___/\___.\___.\___.|
    //
    // ------------------------------------------------------------------
    // Socket poll callback: dispatches to _process_slot_result or error path.
    // ------------------------------------------------------------------
    static void
    _uv_socket_event_cb(uv_poll_t* handle, int status, int events) noexcept(false) {
        auto* slot = static_cast<DBConnection*>(handle->data);
        if (!slot || !slot->pool) return;
        auto* pool = slot->pool;

        if (status < 0) {
            // libuv reports a socket-level failure (POLLERR / POLLHUP / EBADF
            // / etc). Previously this branch only logged and returned, which
            // left the slot permanently in-flight: jobs were never requeued,
            // the concurrency counter was never released, and the reconnect
            // thread skipped the slot because `slot->conn != nullptr`. That
            // is the "broker doesn't recover after PG comes back" failure
            // mode observed during Cloud SQL maintenance windows.
            //
            // Treat it the same as a PQconsumeInput failure: requeue jobs,
            // disconnect, release concurrency, kick the drain orchestrator.
            // The reconnect thread will pick the slot up on its next pass.
            spdlog::error(
                "[Worker {}] [libqueen] Poll error on slot {}: {} - "
                "treating as dead connection",
                pool->_worker_id, slot->idx, uv_strerror(status));
            pool->_handle_slot_error(
                *slot,
                std::string("uv_poll error: ") + uv_strerror(status));
            return;
        }

        if (events & UV_WRITABLE) {
            int flush_result = PQflush(slot->conn);
            if (flush_result == 0) {
                uv_poll_start(handle, UV_READABLE, _uv_socket_event_cb);
            } else if (flush_result == -1) {
                spdlog::error("[Worker {}] [libqueen] PQflush failed", pool->_worker_id);
                return;
            }
        }

        if (events & UV_READABLE) {
            if (!PQconsumeInput(slot->conn)) {
                pool->_handle_slot_error(*slot, std::string("PQconsumeInput failed: ") + PQerrorMessage(slot->conn));
                return;
            }

            if (PQisBusy(slot->conn) == 0) {
                pool->_process_slot_result(*slot);
            }
        }
    }

    void
    _start_watching_slot(DBConnection& slot, int events) noexcept(true) {
        if (slot.poll_initialized && slot.socket_fd >= 0) {
            uv_poll_start(&slot.poll_handle, events, _uv_socket_event_cb);
        }
    }

    //   _
    //  | |_  _   _ _  ___
    //  | _ || | | ' |/ ._>
    //  |___|\___|_|_|\___.
    //
    // ------------------------------------------------------------------
    // DRAIN ORCHESTRATOR (Layer 4)
    // ------------------------------------------------------------------
    //
    // Single entry point for all three event sources (submit-kick, slot-
    // free-kick, timer-kick). Iterates types in round-robin for fairness,
    // packs batches into free slots as long as BatchPolicy says FIRE and
    // ConcurrencyController grants `try_acquire`. Re-arms the safety timer
    // at the end.
    void
    _drain_orchestrator() noexcept {
        // 1. POP pre-pass: promote ready parked POPs back into the POP queue;
        //    send empty responses to POPs whose wait_deadline has expired.
        _evaluate_pop_backoff_ready();

        // 2. Main drain loop.
        auto now = std::chrono::steady_clock::now();
        auto next_timer = now + std::chrono::hours(24);
        bool have_deadline = false;

        size_t start = (_drain_pass_counter++) % JobTypeCount;
        for (size_t i = 0; i < JobTypeCount; ++i) {
            size_t  idx = (start + i) % JobTypeCount;
            JobType t   = job_type_from_index(idx);
            auto&   ts  = _types[idx];

            while (true) {
                auto snap = ts.queue.snapshot(now);
                if (snap.size == 0) break;

                if (ts.policy.should_fire(snap.size, snap.oldest_age) == FireDecision::HOLD) {
                    // Schedule safety wakeup for when max_hold elapses.
                    auto front_t = ts.queue.front_enqueue_time();
                    if (front_t.time_since_epoch().count() != 0) {
                        auto fire_at = front_t + ts.policy.max_hold_ms;
                        if (!have_deadline || fire_at < next_timer) {
                            next_timer     = fire_at;
                            have_deadline  = true;
                        }
                    }
                    break;
                }

                if (!ts.concurrency->try_acquire()) break;

                DBConnection* slot = _try_get_free_slot();
                if (!slot) {
                    ts.concurrency->release();
                    goto drain_arm_timer;
                }

                size_t take = ts.policy.batch_size_to_take(snap.size);
                auto batch = ts.queue.take_front(take);
                if (batch.empty()) {
                    // Race: queue drained between snapshot and take.
                    ts.concurrency->release();
                    _free_slot(*slot);
                    break;
                }

                FireRecord fire{};
                fire.fire_time                  = now;
                fire.type                       = t;
                fire.batch_size                 = static_cast<uint32_t>(batch.size());
                fire.queue_size_at_fire         = static_cast<uint32_t>(snap.size);
                fire.oldest_age_at_fire         = snap.oldest_age;
                fire.in_flight_at_fire          = ts.concurrency->in_flight();
                fire.concurrency_limit_at_fire  = ts.concurrency->current_limit();
                fire.slot_idx                   = static_cast<uint8_t>(slot->idx);

                if (!_fire_batch(*slot, t, std::move(batch), fire)) {
                    // Fire failed. `_fire_batch` already released the slot
                    // and the concurrency counter, and requeued the jobs.
                    break;
                }
                ts.metrics.record_fire(fire);
            }
        }

    drain_arm_timer:
        // 3. Fold in earliest POP next_check deadline.
        auto pop_deadline = _earliest_pop_next_check();
        if (pop_deadline.time_since_epoch().count() != 0) {
            if (!have_deadline || pop_deadline < next_timer) {
                next_timer    = pop_deadline;
                have_deadline = true;
            }
        }

        // 4. Re-arm safety timer.
        if (have_deadline) {
            _rearm_safety_timer(next_timer, now);
        } else {
            _stop_safety_timer();
        }
    }

    // Examine `_pop_backoff_tracker`:
    //   - wait_deadline expired → send empty response, remove from tracker.
    //   - next_check ≤ now       → promote job back into the POP queue.
    //   - otherwise               → stay parked.
    void
    _evaluate_pop_backoff_ready() noexcept {
        auto now = std::chrono::steady_clock::now();

        std::vector<std::shared_ptr<PendingJob>> to_expire;
        std::vector<std::shared_ptr<PendingJob>> to_promote;

        for (auto qit = _pop_backoff_tracker.begin(); qit != _pop_backoff_tracker.end(); ) {
            auto& request_map = qit->second;
            for (auto rit = request_map.begin(); rit != request_map.end(); ) {
                auto job = rit->second;
                if (!job) { rit = request_map.erase(rit); continue; }

                bool has_deadline = (job->job.wait_deadline != std::chrono::steady_clock::time_point{});
                if (has_deadline && job->job.wait_deadline <= now) {
                    to_expire.push_back(job);
                    rit = request_map.erase(rit);
                } else if (job->job.next_check <= now) {
                    to_promote.push_back(job);
                    rit = request_map.erase(rit);
                } else {
                    ++rit;
                }
            }
            if (request_map.empty()) qit = _pop_backoff_tracker.erase(qit);
            else                     ++qit;
        }

        for (auto& job : to_expire) _send_empty_response(job);

        if (!to_promote.empty()) {
            auto& pop_queue = _types[job_type_index(JobType::POP)].queue;
            // Promote to the FRONT (these are older than anything newly-submitted).
            // Use push_front to preserve "oldest first" ordering.
            for (auto it = to_promote.rbegin(); it != to_promote.rend(); ++it) {
                pop_queue.push_front(*it);
            }
        }
    }

    // Earliest next_check across the backoff tracker. Returns epoch if none.
    std::chrono::steady_clock::time_point
    _earliest_pop_next_check() const noexcept {
        std::chrono::steady_clock::time_point best{};
        bool have = false;
        for (const auto& [k, m] : _pop_backoff_tracker) {
            (void)k;
            for (const auto& [id, job] : m) {
                (void)id;
                if (!job) continue;
                auto nc = job->job.next_check;
                if (nc.time_since_epoch().count() == 0) continue;
                if (!have || nc < best) { best = nc; have = true; }
            }
        }
        return have ? best : std::chrono::steady_clock::time_point{};
    }

    // Re-arm the safety timer. Floor of 5 ms on the delta to bound timer
    // wakeup cost (plan §5.5). If the deadline is already past, fire
    // immediately (5 ms — still honors the floor).
    void
    _rearm_safety_timer(std::chrono::steady_clock::time_point deadline,
                        std::chrono::steady_clock::time_point now) noexcept {
        auto delta_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
        if (delta_ms < 5) delta_ms = 5;

        _last_timer_expected = now + std::chrono::milliseconds(delta_ms);
        uv_timer_start(&_queue_timer, _uv_timer_cb,
                       static_cast<uint64_t>(delta_ms), 0);
        _safety_timer_running = true;
        ++_safety_timer_rearms_total;
    }

    void
    _stop_safety_timer() noexcept {
        if (_safety_timer_running) {
            uv_timer_stop(&_queue_timer);
            _safety_timer_running = false;
        }
    }

    // ------------------------------------------------------------------
    // Fire a single batch into a DB slot.
    // ------------------------------------------------------------------
    //
    // For CUSTOM jobs, `batch.size() == 1` (enforced by BatchPolicy max_batch_size=1
    // for CUSTOM) and we dispatch via the single-job CUSTOM path. All other
    // types use the batched stored-procedure path.
    //
    // Returns false if PQsendQueryParams fails; in that case:
    //   - jobs have been pushed back to the front of their type queue
    //   - concurrency counter has been released
    //   - slot has been either disconnected (bad conn) or re-added to free list.
    bool
    _fire_batch(DBConnection& slot, JobType type,
                std::vector<std::shared_ptr<PendingJob>> batch,
                const FireRecord& fire) noexcept(false) {
        // Attribution — only these two lines establish slot-to-type linkage.
        slot.current_type = type;
        slot.current_fire = fire;

        if (type == JobType::CUSTOM) {
            return _fire_custom(slot, std::move(batch));
        }
        return _fire_batched(slot, type, std::move(batch));
    }

    bool
    _fire_batched(DBConnection& slot, JobType type,
                  std::vector<std::shared_ptr<PendingJob>> batch) noexcept(false) {
        nlohmann::json combined = nlohmann::json::array();
        std::vector<std::pair<int,int>> idx_ranges;
        int sequential_idx = 0;

        for (const auto& pending : batch) {
            int start_idx = sequential_idx;
            int count = 0;
            if (!pending->job.params.empty()) {
                try {
                    auto arr = nlohmann::json::parse(pending->job.params[0]);
                    for (auto& item : arr) {
                        // Renumber idx/index so result dispatch works across
                        // merged batches. POP uses "idx", PUSH/ACK use
                        // "index" — set both to be safe.
                        item["idx"]   = sequential_idx;
                        item["index"] = sequential_idx;
                        sequential_idx++;
                        combined.push_back(std::move(item));
                        count++;
                    }
                } catch (const std::exception& e) {
                    spdlog::error("[Worker {}] [libqueen] Failed to parse job params JSON: {}",
                                  _worker_id, e.what());
                }
            }
            idx_ranges.push_back({start_idx, count});
        }

        std::string merged_jsonb = combined.dump();
        const char* param_ptrs[] = { merged_jsonb.c_str() };

        const std::string& sql = JobTypeToSqlTable().at(type);
        slot.jobs.insert(slot.jobs.end(),
                         std::make_move_iterator(batch.begin()),
                         std::make_move_iterator(batch.end()));
        slot.job_idx_ranges = std::move(idx_ranges);

        int sent = PQsendQueryParams(
            slot.conn, sql.c_str(),
            1, nullptr, param_ptrs,
            nullptr, nullptr, 0);

        if (!sent) {
            spdlog::error("[Worker {}] [libqueen] Failed to send jobs: {}",
                          _worker_id, PQerrorMessage(slot.conn));
            _fire_failure_cleanup(slot);
            return false;
        }

        _start_watching_slot(slot, UV_WRITABLE | UV_READABLE);
        return true;
    }

    bool
    _fire_custom(DBConnection& slot,
                 std::vector<std::shared_ptr<PendingJob>> batch) noexcept(false) {
        // Plan §9.2: CUSTOM batch size is always 1.
        auto& job = batch.front();
        slot.jobs.push_back(job);
        slot.job_idx_ranges.push_back({0, 1});

        std::vector<const char*> param_ptrs;
        param_ptrs.reserve(job->job.params.size());
        for (const auto& p : job->job.params) param_ptrs.push_back(p.c_str());

        int sent = PQsendQueryParams(
            slot.conn,
            job->job.sql.c_str(),
            static_cast<int>(param_ptrs.size()),
            nullptr,
            param_ptrs.empty() ? nullptr : param_ptrs.data(),
            nullptr, nullptr, 0);

        if (!sent) {
            spdlog::error("[Worker {}] [libqueen] Failed to send custom query: {}",
                          _worker_id, PQerrorMessage(slot.conn));
            _fire_failure_cleanup(slot);
            return false;
        }

        _start_watching_slot(slot, UV_WRITABLE | UV_READABLE);
        return true;
    }

    // Shared failure path for _fire_batched/_fire_custom when PQsendQueryParams
    // fails. Re-queues the slot's pending jobs at the front (preserving FIFO
    // for the specific type), releases the concurrency counter, and either
    // disconnects (bad conn) or frees the slot.
    void
    _fire_failure_cleanup(DBConnection& slot) noexcept(true) {
        // Route jobs back into their type-queues (front).
        std::vector<std::shared_ptr<PendingJob>> to_requeue;
        to_requeue.swap(slot.jobs);
        slot.job_idx_ranges.clear();
        _requeue_jobs(to_requeue);

        // Release concurrency for the slot's pending batch, mirroring
        // _on_slot_freed. Use ok=false so adaptive controllers don't count
        // the failed RTT against themselves.
        if (slot.current_type != JobType::_SENTINEL) {
            auto idx = job_type_index(slot.current_type);
            _types[idx].concurrency->release();
            CompletionRecord rec{slot.current_fire,
                                 std::chrono::steady_clock::now(),
                                 false, -1};
            _types[idx].concurrency->on_completion(rec);
            _types[idx].metrics.record_completion(rec);
            slot.current_type = JobType::_SENTINEL;
        }

        if (PQstatus(slot.conn) != CONNECTION_OK) {
            spdlog::warn("[Worker {}] [libqueen] Connection dead, marking for reconnection", _worker_id);
            _disconnect_slot(slot);
        } else {
            _free_slot(slot);
        }
    }

    // ------------------------------------------------------------------
    // Completion dispatch: called from _uv_socket_event_cb once PG has a result.
    // ------------------------------------------------------------------
    void
    _process_slot_result(DBConnection& slot) noexcept(false) {
        bool any_error = false;
        int  pg_err    = 0;

        PGresult* res;
        while ((res = PQgetResult(slot.conn)) != NULL) {
            ExecStatusType status = PQresultStatus(res);
            if (status == PGRES_TUPLES_OK || status == PGRES_COMMAND_OK) {
                if (!slot.jobs.empty() && slot.jobs[0]->job.op_type == JobType::CUSTOM) {
                    _process_custom_result(slot, res);
                    PQclear(res);
                    continue;
                }

                auto data = PQgetvalue(res, 0, 0);
                auto json_results = nlohmann::json::parse(data);

                // PUSHPOPLOOKUPSOL: push_messages_v3 now returns
                //   { "items": [...], "partition_updates": [...] }
                // Extract partition_updates here for the post-commit follow-up
                // call, then re-point json_results at the items array so the
                // existing per-job dispatch below continues to work unchanged.
                nlohmann::json partition_updates = nlohmann::json::array();
                if (!slot.jobs.empty()
                    && slot.jobs[0]->job.op_type == JobType::PUSH
                    && json_results.is_object()
                    && json_results.contains("items")) {
                    if (json_results.contains("partition_updates")) {
                        partition_updates = std::move(json_results["partition_updates"]);
                    }
                    json_results = std::move(json_results["items"]);
                }

                // execute_transaction_v2 returns the wrapper object
                //   { "transactionId": ..., "success": ..., "results": [...] }
                // We can't unwrap it like PUSH because the route forwards the
                // wrapper to clients verbatim (TransactionBuilder reads
                // result.success / result.error). Instead we credit metrics
                // here from the inner results array, then deliver the original
                // wrapper to each job's callback and short-circuit the per-job
                // dispatch — the JobType::TRANSACTION case in the switch below
                // would never see this object-shaped response otherwise.
                //
                // execute_transaction_v2 is documented as NOT batchable so
                // slot.jobs.size() == 1 in practice; the loop is for safety.
                if (!slot.jobs.empty()
                    && slot.jobs[0]->job.op_type == JobType::TRANSACTION
                    && json_results.is_object()
                    && json_results.contains("results")
                    && json_results["results"].is_array()) {
                    if (_metrics) {
                        _metrics->record_transaction();
                        std::unordered_set<std::string> queues_touched;
                        for (const auto& op_result : json_results["results"]) {
                            if (op_result.contains("dlq")
                                && op_result["dlq"].is_boolean()
                                && op_result["dlq"].get<bool>()) {
                                _metrics->record_dlq();
                            }
                            if (op_result.contains("queueName")
                                && op_result["queueName"].is_string()) {
                                queues_touched.insert(
                                    op_result["queueName"].get<std::string>());
                            }
                        }
                        for (const auto& qn : queues_touched) {
                            _metrics->record_transaction_with_queue(qn);
                        }
                    }
                    for (auto& job : slot.jobs) {
                        job->callback(json_results.dump());
                        _jobs_done++;
                    }
                    slot.jobs.clear();
                    slot.job_idx_ranges.clear();
                    PQclear(res);
                    continue;
                }

                if (!json_results.is_array()) {
                    for (auto& job : slot.jobs) {
                        job->callback(json_results.dump());
                        _jobs_done++;
                    }
                    slot.jobs.clear();
                    slot.job_idx_ranges.clear();
                    PQclear(res);
                    continue;
                }

                std::map<int, nlohmann::json> results_by_idx;
                for (auto& result_item : json_results) {
                    int idx = -1;
                    if (result_item.contains("idx"))        idx = result_item["idx"].get<int>();
                    else if (result_item.contains("index")) idx = result_item["index"].get<int>();
                    if (idx >= 0) results_by_idx[idx] = std::move(result_item);
                }

                for (size_t job_idx = 0; job_idx < slot.jobs.size(); ++job_idx) {
                    auto& job = slot.jobs[job_idx];

                    int start_idx = 0, count = 1;
                    if (job_idx < slot.job_idx_ranges.size()) {
                        start_idx = slot.job_idx_ranges[job_idx].first;
                        count     = slot.job_idx_ranges[job_idx].second;
                    }

                    nlohmann::json job_results = nlohmann::json::array();
                    for (int i = start_idx; i < start_idx + count; ++i) {
                        auto it = results_by_idx.find(i);
                        if (it != results_by_idx.end()) job_results.push_back(it->second);
                    }

                    if (job_results.empty()) {
                        spdlog::warn("[Worker {}] [libqueen] No results found for job {} (idx range [{}, {}))",
                                     _worker_id, job_idx, start_idx, start_idx + count);
                        continue;
                    }

                    switch (job->job.op_type) {
                        case JobType::POP: {
                            nlohmann::json& result_item = job_results[0];
                            bool has_messages = result_item.contains("result") &&
                                                result_item["result"].contains("messages") &&
                                                !result_item["result"]["messages"].empty();
                            bool has_wait_deadline =
                                (job->job.wait_deadline != std::chrono::steady_clock::time_point{});

                            if (!has_messages && has_wait_deadline) {
                                // Park in backoff tracker; _drain_orchestrator
                                // will promote it when next_check arrives.
                                _set_next_backoff_time(job);
                            } else {
                                // Remove from tracker (if present from a prior
                                // park/promote cycle) and dispatch the result.
                                std::string backoff_key = _get_backoff_key(job->job.queue_name, job->job.partition_name);
                                auto& queue_map = _pop_backoff_tracker[backoff_key];
                                queue_map.erase(job->job.request_id);
                                if (queue_map.empty()) _pop_backoff_tracker.erase(backoff_key);

                                // pop_unified_batch_v4 bakes per-message
                                // partitionId / leaseId into each message
                                // (so multi-partition pops are
                                // self-describing). For the single-partition
                                // case the procedure still emits the same
                                // values, so this loop is a no-op there.
                                // We only stamp absent fields, never
                                // overwrite — that's how a v4 multi-partition
                                // batch (different partitionId per msg)
                                // survives this loop without being collapsed
                                // to the top-level "first claimed partition".
                                if (result_item.contains("result") &&
                                    result_item["result"].contains("partitionId") &&
                                    result_item["result"].contains("messages")) {
                                    auto partition_id = result_item["result"]["partitionId"];
                                    auto lease_id = result_item["result"].value("leaseId", nlohmann::json(nullptr));
                                    for (auto& msg : result_item["result"]["messages"]) {
                                        if (!msg.contains("partitionId") && !partition_id.is_null()) {
                                            msg["partitionId"] = partition_id;
                                        }
                                        if (!msg.contains("leaseId") && !lease_id.is_null()) {
                                            msg["leaseId"] = lease_id;
                                        }
                                    }
                                }

                                if (_metrics) {
                                    _metrics->record_pop_request();
                                    if (has_messages) {
                                        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                            std::chrono::system_clock::now().time_since_epoch()).count();
                                        size_t message_count = result_item["result"]["messages"].size();
                                        for (const auto& msg : result_item["result"]["messages"]) {
                                            uint64_t lag_ms = 0;
                                            if (msg.contains("createdAt") && msg["createdAt"].is_string()) {
                                                try {
                                                    std::string created_str = msg["createdAt"].get<std::string>();
                                                    std::tm tm = {};
                                                    int ms = 0;
                                                    if (sscanf(created_str.c_str(), "%d-%d-%dT%d:%d:%d.%d",
                                                              &tm.tm_year, &tm.tm_mon, &tm.tm_mday,
                                                              &tm.tm_hour, &tm.tm_min, &tm.tm_sec, &ms) >= 6) {
                                                        tm.tm_year -= 1900;
                                                        tm.tm_mon  -= 1;
                                                        auto created_time = timegm(&tm);
                                                        auto created_ms = static_cast<int64_t>(created_time) * 1000 + ms;
                                                        lag_ms = static_cast<uint64_t>(
                                                            std::max(0LL, static_cast<long long>(now_ms - created_ms)));
                                                    }
                                                } catch (...) {}
                                            }
                                            _metrics->record_pop_with_lag(job->job.queue_name, lag_ms);
                                        }

                                        if (job->job.auto_ack) {
                                            _metrics->record_ack_request();
                                            _metrics->record_ack_messages(message_count, message_count, 0);
                                            // Credit this auto-ack back to the same queue for
                                            // per-queue ack counters (PR 3c).
                                            _metrics->record_ack_with_queue(
                                                job->job.queue_name, message_count, 0);
                                        }
                                    } else {
                                        // Empty pop response (no messages + no wait deadline,
                                        // or wait deadline already expired). Track per-queue so
                                        // operators can see polling pressure on idle queues.
                                        _metrics->record_pop_empty(job->job.queue_name);
                                    }
                                }

                                nlohmann::json wrapped = nlohmann::json::array();
                                wrapped.push_back(result_item);
                                _jobs_done++;
                                job->callback(wrapped.dump());
                            }
                            break;
                        }
                        case JobType::PUSH: {
                            _update_pop_backoff_tracker(job->job.queue_name, job->job.partition_name);
                            if (_metrics) {
                                // Global counters: one HTTP push = one request,
                                // total messages across all targeted queues.
                                size_t total_count = job->job.item_count > 0
                                    ? job->job.item_count
                                    : 0;
                                _metrics->record_push_request();
                                _metrics->record_push_messages(total_count);

                                // Per-queue attribution. push_messages_v3 echoes
                                // `queueName` on every item (PR 3c followup); we
                                // group by it so a multi-queue push credits each
                                // queue with the right count. This is what feeds
                                // queue_lag_metrics.push_message_count and the
                                // System view's per-queue Push/s chart.
                                //
                                // Fallback: if the SP is older and items lack
                                // `queueName`, we fall through to job_req's
                                // queue_name (test_contention.cpp sets that).
                                std::unordered_map<std::string, size_t> by_queue;
                                bool any_queue_name_in_results = false;
                                for (const auto& result : job_results) {
                                    if (result.contains("queueName") && result["queueName"].is_string()) {
                                        any_queue_name_in_results = true;
                                        by_queue[result["queueName"].get<std::string>()]++;
                                    }
                                }
                                if (any_queue_name_in_results) {
                                    for (const auto& [qn, count] : by_queue) {
                                        _metrics->record_push_per_queue(qn, count);
                                    }
                                } else if (!job->job.queue_name.empty()) {
                                    _metrics->record_push_per_queue(
                                        job->job.queue_name, total_count);
                                }
                            }
                            _jobs_done++;
                            job->callback(job_results.dump());
                            break;
                        }
                        case JobType::ACK: {
                            if (_metrics) {
                                _metrics->record_ack_request();
                                size_t total = job->job.item_count > 0 ? job->job.item_count : job_results.size();
                                size_t success = 0, failed = 0, dlq = 0;
                                // Per-queue tallies (PR 3c). ack_messages_v2
                                // already returns `queueName` for every
                                // result item, so per-queue attribution is
                                // free — no extra JOIN needed.
                                struct QS { size_t s = 0; size_t f = 0; };
                                std::unordered_map<std::string, QS> by_queue;
                                for (const auto& result : job_results) {
                                    bool is_success = result.contains("success") && result["success"].get<bool>();
                                    if (is_success) success++; else failed++;
                                    if (result.contains("dlq") && result["dlq"].get<bool>()) dlq++;
                                    // Attribute per-queue when the proc returned it; otherwise skip silently.
                                    if (result.contains("queueName") && result["queueName"].is_string()) {
                                        auto& q = by_queue[result["queueName"].get<std::string>()];
                                        if (is_success) q.s++; else q.f++;
                                    }
                                }
                                _metrics->record_ack_messages(total, success, failed);
                                for (size_t i = 0; i < dlq; ++i) _metrics->record_dlq();
                                for (const auto& [qn, qs] : by_queue) {
                                    _metrics->record_ack_with_queue(qn, qs.s, qs.f);
                                }
                            }
                            _jobs_done++;
                            job->callback(job_results.dump());
                            break;
                        }
                        case JobType::TRANSACTION: {
                            if (_metrics) {
                                _metrics->record_transaction();
                                // Per-queue transaction attribution (PR 3c).
                                // 004_transaction.sql now returns `queueName` for
                                // push-ops; we credit each distinct queue once per
                                // transaction so a transaction that spans multiple
                                // queues shows up under each of them.
                                std::unordered_set<std::string> queues_touched;
                                for (const auto& result : job_results) {
                                    if (result.contains("dlq") && result["dlq"].get<bool>()) _metrics->record_dlq();
                                    if (result.contains("queueName") && result["queueName"].is_string()) {
                                        queues_touched.insert(result["queueName"].get<std::string>());
                                    }
                                }
                                for (const auto& qn : queues_touched) {
                                    _metrics->record_transaction_with_queue(qn);
                                }
                            }
                            _jobs_done++;
                            job->callback(job_results.dump());
                            break;
                        }
                        case JobType::STREAMS_CYCLE: {
                            // queen-streams cycles ack the source partition's
                            // cursor AND push to the sink queue inside one PG
                            // transaction (queen.streams_cycle_v1). The raw
                            // ack/push primitives are NOT exposed here, so we
                            // re-credit them to the dashboard's metrics manually
                            // — otherwise streaming workloads show up as
                            // "push > ack" indefinitely (cf PR-streams-metrics).
                            //
                            // Each result item shape:
                            //   { idx,
                            //     result: {
                            //       success, query_id, partition_id, queueName,
                            //       push_results: [{queueName,...}, ...],
                            //       ack_result: {success, count, dlq, ...} | null
                            //     } }
                            if (_metrics) {
                                for (const auto& outer : job_results) {
                                    if (!outer.contains("result") || !outer["result"].is_object()) continue;
                                    const auto& r = outer["result"];

                                    // Source ack — counts streamed messages as
                                    // "consumed" on the per-queue Ack/s chart.
                                    if (r.contains("ack_result")
                                        && r["ack_result"].is_object()
                                        && r["ack_result"].value("success", false)) {
                                        size_t ack_count = r["ack_result"].value("count", 1);
                                        _metrics->record_ack_request();
                                        _metrics->record_ack_messages(ack_count, ack_count, 0);
                                        if (r.contains("queueName") && r["queueName"].is_string()) {
                                            _metrics->record_ack_with_queue(
                                                r["queueName"].get<std::string>(),
                                                ack_count, 0);
                                        }
                                        if (r["ack_result"].value("dlq", false)) {
                                            _metrics->record_dlq();
                                        }
                                    }

                                    // Sink push — queen.streams_cycle_v1 invokes
                                    // queen.push_messages_v3 internally, and its
                                    // items array (in push_results) carries the
                                    // sink queueName per row. Mirror what
                                    // JobType::PUSH does.
                                    if (r.contains("push_results") && r["push_results"].is_array()) {
                                        const auto& pushes = r["push_results"];
                                        if (!pushes.empty()) {
                                            _metrics->record_push_request();
                                            _metrics->record_push_messages(pushes.size());
                                            std::unordered_map<std::string, size_t> by_queue;
                                            for (const auto& p : pushes) {
                                                if (p.contains("queueName") && p["queueName"].is_string()) {
                                                    by_queue[p["queueName"].get<std::string>()]++;
                                                }
                                            }
                                            for (const auto& [qn, cnt] : by_queue) {
                                                _metrics->record_push_per_queue(qn, cnt);
                                            }
                                        }
                                    }
                                }
                            }
                            _jobs_done++;
                            job->callback(job_results.dump());
                            break;
                        }
                        default: {
                            _jobs_done++;
                            job->callback(job_results.dump());
                            break;
                        }
                    }
                }

                // PUSHPOPLOOKUPSOL: after all per-job callbacks for a PUSH
                // drain have fired, enqueue a single CUSTOM job to refresh
                // partition_lookup for the partitions this drain touched.
                // Fire-and-forget: failures are logged and healed by the
                // periodic PartitionLookupReconcileService.
                if (partition_updates.is_array() && !partition_updates.empty()) {
                    JobRequest pl_job;
                    pl_job.op_type    = JobType::CUSTOM;
                    pl_job.request_id = "pl-refresh";
                    pl_job.sql        = "SELECT queen.update_partition_lookup_v1($1::jsonb)";
                    pl_job.params     = { partition_updates.dump() };
                    pl_job.item_count = partition_updates.size();
                    uint16_t wid = _worker_id;
                    submit(std::move(pl_job), [wid](std::string result) {
                        try {
                            auto r = nlohmann::json::parse(result);
                            if (r.is_object()
                                && r.contains("success")
                                && r["success"].is_boolean()
                                && !r["success"].get<bool>()) {
                                spdlog::warn("[Worker {}] [libqueen] update_partition_lookup_v1 failed: {}",
                                             wid, r.value("error", "unknown"));
                            }
                        } catch (...) {
                            // Swallowed: reconciler heals eventually.
                        }
                    });
                }

                slot.jobs.clear();
                slot.job_idx_ranges.clear();
            } else {
                std::string error_msg = PQresultErrorMessage(res);
                spdlog::error("[Worker {}] [libqueen] Query failed: {}", _worker_id, error_msg);

                if (_metrics) _metrics->record_db_error();
                any_error = true;
                const char* sev = PQresultErrorField(res, PG_DIAG_SQLSTATE);
                if (sev && *sev) pg_err = std::atoi(sev);

                nlohmann::json error_result = {{"success", false}, {"error", error_msg}};

                for (auto& job : slot.jobs) {
                    job->callback(error_result.dump());
                    _jobs_done++;
                }
                slot.jobs.clear();
                slot.job_idx_ranges.clear();
            }
            PQclear(res);
        }

        uv_poll_stop(&slot.poll_handle);
        _on_slot_freed(slot, !any_error, pg_err);
    }

    //  ___       _ _    ____                       _
    // / __| ___ | | |_ | ___| _ _  ___ ___ ___  __| |
    // \__ \/ _ \|  _/  |  _| | '_>/ ._>/ ._>/ ._>/ . |
    // |___/\___/ _|    |_|   |_|  \___.\___.\___.\___.|
    //
    // Central "slot returned to pool" path. See plan §8.3.
    //
    // Exactly one release per acquire: this is the only place
    // concurrency->release() is called for normal and error completions.
    // (The PQsendQueryParams-failure path releases its own counter via
    // _fire_failure_cleanup because in that path we never reach here.)
    void
    _on_slot_freed(DBConnection& slot, bool ok, int pg_error_code) noexcept {
        if (slot.current_type != JobType::_SENTINEL) {
            auto idx = job_type_index(slot.current_type);
            CompletionRecord rec{slot.current_fire,
                                 std::chrono::steady_clock::now(),
                                 ok, pg_error_code};
            _types[idx].concurrency->release();
            _types[idx].concurrency->on_completion(rec);
            _types[idx].metrics.record_completion(rec);
            slot.current_type = JobType::_SENTINEL;
        }

        if (slot.conn != nullptr) {
            _free_slot(slot);
        }

        ++_drain_passes_slot_free;
        _drain_orchestrator();
    }

    //    _       _
    //   | | ___ | |_  ___
    //  _| |/ . \| . \<_-<
    //  \__/\___/|___//__/
    //
    void
    _process_custom_result(DBConnection& slot, PGresult* res) noexcept(true) {
        nlohmann::json result;

        int num_rows = PQntuples(res);
        int num_cols = PQnfields(res);

        if (num_rows == 0) {
            result = {
                {"success", true},
                {"rows", nlohmann::json::array()},
                {"rowCount", 0},
                {"command", PQcmdStatus(res) ? PQcmdStatus(res) : ""}
            };
        } else if (num_rows == 1 && num_cols == 1) {
            const char* value = PQgetvalue(res, 0, 0);
            if (value && strlen(value) > 0) {
                try { result = nlohmann::json::parse(value); }
                catch (...) { result = {{"success", true}, {"value", value}}; }
            } else {
                result = {{"success", true}, {"value", nullptr}};
            }
        } else {
            nlohmann::json rows = nlohmann::json::array();
            for (int row = 0; row < num_rows; ++row) {
                nlohmann::json row_obj;
                for (int col = 0; col < num_cols; ++col) {
                    const char* col_name = PQfname(res, col);
                    if (PQgetisnull(res, row, col)) {
                        row_obj[col_name] = nullptr;
                    } else {
                        const char* value = PQgetvalue(res, row, col);
                        if (value && (value[0] == '{' || value[0] == '[')) {
                            try { row_obj[col_name] = nlohmann::json::parse(value); }
                            catch (...) { row_obj[col_name] = value; }
                        } else {
                            row_obj[col_name] = value ? value : "";
                        }
                    }
                }
                rows.push_back(std::move(row_obj));
            }
            result = {{"success", true}, {"rows", std::move(rows)}, {"rowCount", num_rows}};
        }

        if (!slot.jobs.empty()) {
            slot.jobs[0]->callback(result.dump());
            _jobs_done++;
        }
        slot.jobs.clear();
        slot.job_idx_ranges.clear();
    }

    // Route each job back to the front of its type-queue. Preserves the
    // per-type FIFO property for requeues from a single caller: jobs are
    // pushed to the front in reverse order, so the original head remains
    // the head after requeue.
    void
    _requeue_jobs(const std::vector<std::shared_ptr<PendingJob>>& jobs) noexcept(true) {
        // Group by type first so the same type's jobs preserve relative
        // order when routed to push_front.
        std::array<std::vector<std::shared_ptr<PendingJob>>, JobTypeCount> by_type;
        for (const auto& j : jobs) {
            size_t idx = job_type_index(j->job.op_type);
            if (idx < JobTypeCount) by_type[idx].push_back(j);
        }
        for (size_t idx = 0; idx < JobTypeCount; ++idx) {
            auto& v = by_type[idx];
            if (v.empty()) continue;
            auto& q = _types[idx].queue;
            for (auto it = v.rbegin(); it != v.rend(); ++it) {
                q.push_front(*it);
            }
        }
    }

    void
    _set_next_backoff_time(std::shared_ptr<PendingJob>& job) noexcept(true) {
        job->job.backoff_count++;
        auto now = std::chrono::steady_clock::now();
        if (job->job.backoff_count > _pop_wait_backoff_threshold) {
            job->job.next_check = now + std::chrono::milliseconds(static_cast<long long>(
                _pop_wait_initial_interval_ms * job->job.backoff_count * _pop_wait_backoff_multiplier));
            if (job->job.next_check > now + std::chrono::milliseconds(_pop_wait_max_interval_ms)) {
                job->job.next_check = now + std::chrono::milliseconds(_pop_wait_max_interval_ms);
            }
        } else {
            job->job.next_check = now + std::chrono::milliseconds(_pop_wait_initial_interval_ms);
        }
        std::string backoff_key = _get_backoff_key(job->job.queue_name, job->job.partition_name);
        _pop_backoff_tracker[backoff_key][job->job.request_id] = job;
    }

    void
    _update_pop_backoff_tracker(const std::string& queue_name,
                                const std::string& partition_name) noexcept(true) {
        auto now = std::chrono::steady_clock::now();

        std::string specific_key = _get_backoff_key(queue_name, partition_name);
        auto it = _pop_backoff_tracker.find(specific_key);
        if (it != _pop_backoff_tracker.end()) {
            for (auto& [request_id, job_ptr] : it->second) {
                (void)request_id;
                job_ptr->job.next_check    = now;
                job_ptr->job.backoff_count = 0;
            }
        }

        std::string wildcard_key = queue_name + "/*";
        it = _pop_backoff_tracker.find(wildcard_key);
        if (it != _pop_backoff_tracker.end()) {
            for (auto& [request_id, job_ptr] : it->second) {
                (void)request_id;
                job_ptr->job.next_check    = now;
                job_ptr->job.backoff_count = 0;
            }
        }
    }

    void
    _send_empty_response(std::shared_ptr<PendingJob>& job) noexcept(true) {
        // Long-poll expired with nothing to return — count as an empty pop
        // for this queue so polling pressure is visible on idle queues.
        if (_metrics) {
            _metrics->record_pop_request();
            _metrics->record_pop_empty(job->job.queue_name);
        }
        auto response = nlohmann::json::array();
        response.push_back({
            {"idx", 0},
            {"result", {
                {"success", true},
                {"queue", job->job.queue_name},
                {"partition", job->job.partition_name},
                {"partitionId", nullptr},
                {"leaseId", nullptr},
                {"consumerGroup", job->job.consumer_group},
                {"messages", nlohmann::json::array()}
            }}
        });
        job->callback(response.dump());
    }

    //  ___                            _    _
    // |  _> ___ ._ _ ._ _  ___  ___ _| |_ <_> ___ ._ _  ___
    // | <__/ . \| ' || ' |/ ._>/ | ' | |  | |/ . \| ' |<_-<
    // `___/\___/|_|_||_|_|\___.\_|_. |_|  |_|\___/|_|_|/__/
    //
    DBConnection*
    _try_get_free_slot() noexcept(true) {
        if (_free_slot_indexes.empty()) return nullptr;
        uint16_t idx = _free_slot_indexes.back();
        _free_slot_indexes.pop_back();
        DBConnection& slot = _db_connections[idx];
        slot.idx = idx;
        return &slot;
    }

    void
    _free_slot(DBConnection& slot) noexcept(true) {
        _free_slot_indexes.push_back(slot.idx);
    }

    bool _connect_all_slots() noexcept(true) {
        _db_connections.resize(_db_connection_count);
        for (uint16_t i = 0; i < _db_connection_count; i++) {
            DBConnection& slot = _db_connections[i];
            slot.pool = this;
            if (!_connect_slot(slot)) {
                return false;
            }
            _free_slot_indexes.push_back(i);
        }
        return true;
    }

    bool
    _connect_slot(DBConnection& slot) noexcept(true) {
        slot.conn = PQconnectdb(_conn_str.c_str());

        if (PQstatus(slot.conn) != CONNECTION_OK) {
            spdlog::error("[Worker {}] [libqueen] Connection failed: {}", _worker_id, PQerrorMessage(slot.conn));
            PQfinish(slot.conn);
            slot.conn = nullptr;
            return false;
        }

        if (PQsetnonblocking(slot.conn, 1) != 0) {
            spdlog::error("[Worker {}] [libqueen] Failed to set non-blocking: {}", _worker_id, PQerrorMessage(slot.conn));
            PQfinish(slot.conn);
            slot.conn = nullptr;
            return false;
        }

        std::string timeout_sql = "SET statement_timeout = " + std::to_string(_statement_timeout_ms);
        PGresult* res = PQexec(slot.conn, timeout_sql.c_str());
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            spdlog::warn("[Worker {}] [libqueen] Failed to set statement timeout: {}",
                         _worker_id, PQerrorMessage(slot.conn));
        }
        PQclear(res);

        slot.socket_fd = PQsocket(slot.conn);

        if (_loop_initialized && slot.socket_fd >= 0) {
            if (uv_poll_init(_loop, &slot.poll_handle, slot.socket_fd) == 0) {
                slot.poll_handle.data = &slot;
                slot.poll_initialized = true;
            } else {
                spdlog::warn("[Worker {}] [libqueen] Failed to initialize uv_poll for slot", _worker_id);
                slot.poll_initialized = false;
            }
        }

        return true;
    }

    void
    _handle_slot_error(DBConnection& slot, const std::string& error_msg) noexcept(false) {
        spdlog::error("[Worker {}] [libqueen] Slot error: {}", _worker_id, error_msg);

        std::vector<std::shared_ptr<PendingJob>> to_requeue;
        to_requeue.swap(slot.jobs);
        slot.job_idx_ranges.clear();
        _requeue_jobs(to_requeue);

        _disconnect_slot(slot);   // closes conn, stops poll.
        _on_slot_freed(slot, false, -1);
    }

    void
    _disconnect_slot(DBConnection& slot) noexcept(true) {
        if (slot.poll_initialized) {
            uv_poll_stop(&slot.poll_handle);
            slot.poll_initialized = false;
        }

        if (slot.conn) {
            PQfinish(slot.conn);
            slot.conn = nullptr;
            slot.socket_fd = -1;
        }

        slot.needs_poll_init.store(false, std::memory_order_release);
    }

    void
    _start_reconnect_thread() noexcept(true) {
        _reconnect_running.store(true);
        _reconnect_thread = std::thread([this] {
            _reconnect_loop();
        });
    }

    void
    _reconnect_loop() noexcept(true) {
        spdlog::info("[Worker {}] [libqueen] Reconnection thread started", _worker_id);

        while (_reconnect_running.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            if (!_reconnect_running.load()) break;

            for (uint16_t i = 0; i < _db_connection_count; i++) {
                DBConnection& slot = _db_connections[i];

                if (slot.conn != nullptr || slot.needs_poll_init.load(std::memory_order_acquire)) {
                    continue;
                }

                spdlog::info("[Worker {}] [libqueen] Reconnecting dead slot {}", _worker_id, i);

                if (_reconnect_slot_db(slot)) {
                    slot.needs_poll_init.store(true, std::memory_order_release);
                    uv_async_send(&_reconnect_signal);
                    spdlog::info("[Worker {}] [libqueen] Slot {} reconnected, signaling event loop", _worker_id, i);
                } else {
                    spdlog::warn("[Worker {}] [libqueen] Failed to reconnect slot {}", _worker_id, i);
                }

                break;
            }
        }

        spdlog::info("[Worker {}] [libqueen] Reconnection thread stopped", _worker_id);
    }

    bool
    _reconnect_slot_db(DBConnection& slot) noexcept(true) {
        slot.conn = PQconnectdb(_conn_str.c_str());

        if (PQstatus(slot.conn) != CONNECTION_OK) {
            spdlog::error("[Worker {}] [libqueen] Reconnection failed: {}", _worker_id, PQerrorMessage(slot.conn));
            PQfinish(slot.conn);
            slot.conn = nullptr;
            return false;
        }

        if (PQsetnonblocking(slot.conn, 1) != 0) {
            spdlog::error("[Worker {}] [libqueen] Failed to set non-blocking on reconnect: {}",
                          _worker_id, PQerrorMessage(slot.conn));
            PQfinish(slot.conn);
            slot.conn = nullptr;
            return false;
        }

        std::string timeout_sql = "SET statement_timeout = " + std::to_string(_statement_timeout_ms);
        PGresult* res = PQexec(slot.conn, timeout_sql.c_str());
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            spdlog::warn("[Worker {}] [libqueen] Failed to set statement timeout on reconnect: {}",
                         _worker_id, PQerrorMessage(slot.conn));
        }
        PQclear(res);

        slot.socket_fd = PQsocket(slot.conn);

        return true;
    }
};

} // namespace queen

#endif /* __cplusplus */
#endif /* _QUEEN_HPP_ */
