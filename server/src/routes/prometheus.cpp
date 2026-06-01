// ============================================================================
// /metrics/prometheus  —  Prometheus text exposition for queen
// ============================================================================
//
// Each replica exposes its OWN process state (db pool, threadpools, response
// registries, file buffer, latest MetricsCollector sample, shared_state
// snapshot) plus a single read of queen.worker_metrics_summary for the
// cluster-wide lifetime totals.
//
// Cluster-wide series are emitted with the synthetic label  scope="cluster"
// so PromQL queries that span replicas can use  max(queen_cluster_*)  rather
// than  sum(...)  (every replica returns the same singleton totals).
//
// Per-replica series carry no extra label — Prometheus already labels them
// with  instance  /  job  via its scrape config.
//
// Exposition format reference:
//   https://prometheus.io/docs/instrumenting/exposition_formats/

#include "queen/routes/route_registry.hpp"
#include "queen/routes/route_context.hpp"
#include "queen/routes/route_helpers.hpp"
#include "queen/async_queue_manager.hpp"
#include "queen/file_buffer.hpp"
#include "queen/response_queue.hpp"
#include "queen/metrics_collector.hpp"
#include "queen/shared_state_manager.hpp"
#include "queen.hpp"
#include "threadpool.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <sstream>
#include <string>

#include <json.hpp>
#include <spdlog/spdlog.h>

// External globals defined in acceptor_server.cpp / status.cpp.
namespace queen {
extern std::vector<std::shared_ptr<ResponseRegistry>> worker_response_registries;
}  // namespace queen

namespace queen {
namespace routes {

namespace {

// ---------------------------------------------------------------------------
// Exposition-format helpers.
// ---------------------------------------------------------------------------

// Escape label values per the Prometheus text format: backslash, double-quote
// and newline must be escaped. Everything else is passed through.
std::string escape_label(const std::string& v) {
    std::string out;
    out.reserve(v.size());
    for (char c : v) {
        switch (c) {
            case '\\': out += "\\\\"; break;
            case '"':  out += "\\\""; break;
            case '\n': out += "\\n";  break;
            default:   out += c;
        }
    }
    return out;
}

// Tiny accumulator that builds the response body incrementally. Each metric
// family is emitted as: HELP line, TYPE line, then one or more sample lines.
class Body {
public:
    void help(const std::string& name, const std::string& text) {
        buf_ << "# HELP " << name << ' ' << text << '\n';
    }
    void type(const std::string& name, const std::string& kind) {
        buf_ << "# TYPE " << name << ' ' << kind << '\n';
    }
    // Emit  name value  with no labels.
    template <typename T>
    void sample(const std::string& name, T value) {
        buf_ << name << ' ' << value << '\n';
    }
    // Emit  name{labels} value  — labels is an already-formatted
    // {k="v",k2="v2"} string (or empty).
    template <typename T>
    void sample(const std::string& name,
                const std::string& labels,
                T value) {
        buf_ << name << labels << ' ' << value << '\n';
    }
    std::string str() const { return buf_.str(); }

private:
    std::ostringstream buf_;
};

// Build a  {k="v",k2="v2"}  label block. Pairs with empty values are skipped.
std::string labels(std::initializer_list<std::pair<const char*, std::string>> pairs) {
    std::string out;
    bool first = true;
    for (const auto& [k, v] : pairs) {
        if (v.empty()) continue;
        if (first) { out += '{'; first = false; }
        else       { out += ','; }
        out += k;
        out += "=\"";
        out += escape_label(v);
        out += '"';
    }
    if (!first) out += '}';
    return out;
}

// ---------------------------------------------------------------------------
// Metric emitters — each one writes a complete metric family.
// ---------------------------------------------------------------------------

// Process / runtime gauges sourced from the latest in-memory MetricsSample.
void write_process_metrics(Body& b, const RouteContext& ctx) {
    MetricsSample s{};
    bool have_sample = false;
    if (ctx.metrics_collector) {
        s = ctx.metrics_collector->get_latest_sample();
        have_sample = s.timestamp.time_since_epoch().count() != 0;
    }

    b.help("queen_uptime_seconds", "Seconds since the queen process started.");
    b.type("queen_uptime_seconds", "gauge");
    b.sample("queen_uptime_seconds", have_sample ? s.uptime_seconds : 0);

    if (!have_sample) return;

    b.help("queen_cpu_user_microseconds_total",
           "Cumulative user CPU time consumed by the process, in microseconds.");
    b.type("queen_cpu_user_microseconds_total", "counter");
    b.sample("queen_cpu_user_microseconds_total", s.cpu_user_us);

    b.help("queen_cpu_system_microseconds_total",
           "Cumulative system CPU time consumed by the process, in microseconds.");
    b.type("queen_cpu_system_microseconds_total", "counter");
    b.sample("queen_cpu_system_microseconds_total", s.cpu_system_us);

    b.help("queen_process_resident_memory_bytes",
           "Resident set size of the queen process, in bytes.");
    b.type("queen_process_resident_memory_bytes", "gauge");
    b.sample("queen_process_resident_memory_bytes", s.memory_rss_bytes);

    b.help("queen_process_virtual_memory_bytes",
           "Virtual memory size of the queen process, in bytes.");
    b.type("queen_process_virtual_memory_bytes", "gauge");
    b.sample("queen_process_virtual_memory_bytes", s.memory_virtual_bytes);
}

// Database connection-pool gauges (read live from AsyncQueueManager).
void write_db_pool_metrics(Body& b, const RouteContext& ctx) {
    auto pool = ctx.async_queue_manager->get_pool_stats();

    b.help("queen_db_pool_size",
           "Total connections in the asynchronous Postgres pool.");
    b.type("queen_db_pool_size", "gauge");
    b.sample("queen_db_pool_size", pool.total);

    b.help("queen_db_pool_idle",
           "Idle connections currently available in the asynchronous Postgres pool.");
    b.type("queen_db_pool_idle", "gauge");
    b.sample("queen_db_pool_idle", pool.available);

    b.help("queen_db_pool_active",
           "Connections currently checked out from the asynchronous Postgres pool.");
    b.type("queen_db_pool_active", "gauge");
    b.sample("queen_db_pool_active", pool.total - pool.available);
}

// Threadpool gauges. Both pools are emitted with a  pool="db|system"  label.
void write_threadpool_metrics(Body& b, const RouteContext& ctx) {
    b.help("queen_threadpool_size",
           "Worker count in each background threadpool.");
    b.type("queen_threadpool_size", "gauge");
    if (ctx.db_thread_pool) {
        b.sample("queen_threadpool_size",
                 labels({{"pool", "db"}}),
                 ctx.db_thread_pool->pool_size());
    }
    if (ctx.system_thread_pool) {
        b.sample("queen_threadpool_size",
                 labels({{"pool", "system"}}),
                 ctx.system_thread_pool->pool_size());
    }

    b.help("queen_threadpool_queue_size",
           "Tasks pending in each background threadpool.");
    b.type("queen_threadpool_queue_size", "gauge");
    if (ctx.db_thread_pool) {
        b.sample("queen_threadpool_queue_size",
                 labels({{"pool", "db"}}),
                 ctx.db_thread_pool->queue_size());
    }
    if (ctx.system_thread_pool) {
        b.sample("queen_threadpool_queue_size",
                 labels({{"pool", "system"}}),
                 ctx.system_thread_pool->queue_size());
    }
}

// Per-worker response-registry gauges.
void write_registry_metrics(Body& b) {
    b.help("queen_response_registry_size",
           "In-flight HTTP responses awaiting completion in each worker.");
    b.type("queen_response_registry_size", "gauge");
    for (size_t i = 0; i < worker_response_registries.size(); ++i) {
        const auto& reg = worker_response_registries[i];
        if (!reg) continue;
        b.sample("queen_response_registry_size",
                 labels({{"worker_id", std::to_string(i)}}),
                 reg->size());
    }
}

// File buffer (Worker 0 only — defensive: ctx.file_buffer may be null on
// other workers, in which case we publish zero so the series is always
// present and Grafana can scrape any replica).
void write_file_buffer_metrics(Body& b, const RouteContext& ctx) {
    uint64_t pending = 0;
    uint64_t failed = 0;
    int      healthy = 1;
    if (ctx.file_buffer) {
        pending = ctx.file_buffer->get_pending_count();
        failed  = ctx.file_buffer->get_failed_count();
        healthy = ctx.file_buffer->is_db_healthy() ? 1 : 0;
    }

    b.help("queen_file_buffer_pending",
           "Push events currently buffered to disk awaiting database flush.");
    b.type("queen_file_buffer_pending", "gauge");
    b.sample("queen_file_buffer_pending", pending);

    b.help("queen_file_buffer_failed",
           "Push events that failed to flush from the file buffer.");
    b.type("queen_file_buffer_failed", "gauge");
    b.sample("queen_file_buffer_failed", failed);

    b.help("queen_file_buffer_db_healthy",
           "1 if the file buffer believes the database is healthy, 0 otherwise.");
    b.type("queen_file_buffer_db_healthy", "gauge");
    b.sample("queen_file_buffer_db_healthy", healthy);
}

// Push-maintenance flag. Reads through the AsyncQueueManager's TTL-cached
// accessor — same path the push handler uses, so we never hit the DB on a
// scrape.
void write_maintenance_metrics(Body& b, const RouteContext& ctx) {
    int enabled = 0;
    if (ctx.async_queue_manager) {
        try {
            enabled = ctx.async_queue_manager->get_maintenance_mode() ? 1 : 0;
        } catch (const std::exception& e) {
            spdlog::debug("[/metrics/prometheus] maintenance check failed: {}", e.what());
        }
    }
    b.help("queen_maintenance_mode_enabled",
           "1 when push maintenance mode is active (push requests are buffered to disk), 0 otherwise.");
    b.type("queen_maintenance_mode_enabled", "gauge");
    b.sample("queen_maintenance_mode_enabled", enabled);
}

// Sidecar / cluster-sync metrics from the latest MetricsSample. These are
// running counters reported as gauges because MetricsCollector resets them
// each aggregation window — Prometheus will compute rates via PromQL on the
// raw "count over the last sample window" value.
void write_sidecar_metrics(Body& b, const RouteContext& ctx) {
    if (!ctx.metrics_collector) return;
    MetricsSample s = ctx.metrics_collector->get_latest_sample();
    if (s.timestamp.time_since_epoch().count() == 0) return;

    b.help("queen_sidecar_op_count",
           "Sidecar operations observed in the latest sample window, by op.");
    b.type("queen_sidecar_op_count", "gauge");
    b.sample("queen_sidecar_op_count", labels({{"op", "push"}}), s.sidecar_push_count);
    b.sample("queen_sidecar_op_count", labels({{"op", "pop"}}),  s.sidecar_pop_count);
    b.sample("queen_sidecar_op_count", labels({{"op", "ack"}}),  s.sidecar_ack_count);

    b.help("queen_sidecar_op_latency_microseconds",
           "Average sidecar operation latency in the latest sample window, by op.");
    b.type("queen_sidecar_op_latency_microseconds", "gauge");
    b.sample("queen_sidecar_op_latency_microseconds", labels({{"op", "push"}}), s.sidecar_push_latency_us);
    b.sample("queen_sidecar_op_latency_microseconds", labels({{"op", "pop"}}),  s.sidecar_pop_latency_us);
    b.sample("queen_sidecar_op_latency_microseconds", labels({{"op", "ack"}}),  s.sidecar_ack_latency_us);

    b.help("queen_sidecar_op_items",
           "Items processed by sidecar operations in the latest sample window, by op.");
    b.type("queen_sidecar_op_items", "gauge");
    b.sample("queen_sidecar_op_items", labels({{"op", "push"}}), s.sidecar_push_items);
    b.sample("queen_sidecar_op_items", labels({{"op", "pop"}}),  s.sidecar_pop_items);
    b.sample("queen_sidecar_op_items", labels({{"op", "ack"}}),  s.sidecar_ack_items);

    b.help("queen_queue_backoff_groups",
           "Total consumer groups currently in pop-backoff across all queues.");
    b.type("queen_queue_backoff_groups", "gauge");
    b.sample("queen_queue_backoff_groups", s.total_backed_off_groups);

    b.help("queen_queue_backoff_queues",
           "Number of queues that have at least one consumer group in backoff.");
    b.type("queen_queue_backoff_queues", "gauge");
    b.sample("queen_queue_backoff_queues", s.queues_with_backoff);

    b.help("queen_queue_backoff_avg_interval_milliseconds",
           "Average backoff interval across all backed-off groups, in milliseconds.");
    b.type("queen_queue_backoff_avg_interval_milliseconds", "gauge");
    b.sample("queen_queue_backoff_avg_interval_milliseconds", s.avg_backoff_interval_ms);

    // Cluster-sync block — only emitted when SharedState is enabled.
    b.help("queen_shared_state_enabled",
           "1 if cross-replica UDP shared-state sync is enabled, 0 otherwise.");
    b.type("queen_shared_state_enabled", "gauge");
    b.sample("queen_shared_state_enabled", s.shared_state_enabled ? 1 : 0);

    if (!s.shared_state_enabled) return;

    b.help("queen_qc_cache_size", "Entries in the queue-config cache.");
    b.type("queen_qc_cache_size", "gauge");
    b.sample("queen_qc_cache_size", s.qc_cache_size);

    b.help("queen_qc_cache_hits_total", "Cumulative queue-config cache hits.");
    b.type("queen_qc_cache_hits_total", "counter");
    b.sample("queen_qc_cache_hits_total", s.qc_cache_hits);

    b.help("queen_qc_cache_misses_total", "Cumulative queue-config cache misses.");
    b.type("queen_qc_cache_misses_total", "counter");
    b.sample("queen_qc_cache_misses_total", s.qc_cache_misses);

    b.help("queen_servers", "Replicas tracked by shared-state, by liveness state.");
    b.type("queen_servers", "gauge");
    b.sample("queen_servers", labels({{"state", "alive"}}), s.servers_alive);
    b.sample("queen_servers", labels({{"state", "dead"}}),  s.servers_dead);

    b.help("queen_transport_messages_total",
           "Cumulative UDP-sync messages by direction.");
    b.type("queen_transport_messages_total", "counter");
    b.sample("queen_transport_messages_total", labels({{"dir", "sent"}}),     s.transport_sent);
    b.sample("queen_transport_messages_total", labels({{"dir", "received"}}), s.transport_received);
    b.sample("queen_transport_messages_total", labels({{"dir", "dropped"}}),  s.transport_dropped);
}

// ---------------------------------------------------------------------------
// JSON parsing helpers — libqueen's CUSTOM op wraps SP results in slightly
// different shapes depending on the row count, so all unwrap helpers probe
// both common shapes (raw object and array-of-single-row).
// ---------------------------------------------------------------------------

nlohmann::json unwrap_sp(const nlohmann::json& j, const char* sp_name) {
    if (j.is_object() && j.contains(sp_name)) return j[sp_name];
    if (j.is_array() && !j.empty()) {
        const auto& first = j[0];
        if (first.is_object() && first.contains(sp_name)) return first[sp_name];
        return first;
    }
    return j;
}

uint64_t json_u64(const nlohmann::json& obj, const char* key) {
    if (!obj.is_object() || !obj.contains(key) || obj[key].is_null()) return 0;
    const auto& v = obj[key];
    if (v.is_number_unsigned()) return v.get<uint64_t>();
    if (v.is_number_integer())  return static_cast<uint64_t>(std::max<int64_t>(0, v.get<int64_t>()));
    if (v.is_number_float())    return static_cast<uint64_t>(std::max(0.0, v.get<double>()));
    if (v.is_string()) {
        try { return std::stoull(v.get<std::string>()); } catch (...) { return 0; }
    }
    return 0;
}

// ---------------------------------------------------------------------------
// Cluster lifetime totals (queen.worker_metrics_summary).
// Emitted with scope="cluster" so PromQL across replicas can use max(...)
// instead of sum(...) on these singleton series.
// ---------------------------------------------------------------------------
void format_cluster_totals(Body& b, const nlohmann::json& totals) {
    if (!totals.is_object()) return;

    const std::string scope = labels({{"scope", "cluster"}});
    auto counter = [&](const char* name, const char* help, uint64_t v) {
        b.help(name, help); b.type(name, "counter"); b.sample(name, scope, v);
    };

    counter("queen_cluster_push_requests_total",
            "Cumulative push HTTP requests handled across the cluster.",
            json_u64(totals, "pushRequests"));
    counter("queen_cluster_pop_requests_total",
            "Cumulative pop HTTP requests handled across the cluster.",
            json_u64(totals, "popRequests"));
    counter("queen_cluster_ack_requests_total",
            "Cumulative ack HTTP requests handled across the cluster.",
            json_u64(totals, "ackRequests"));
    counter("queen_cluster_transactions_total",
            "Cumulative transaction calls handled across the cluster.",
            json_u64(totals, "transactions"));

    counter("queen_cluster_push_messages_total",
            "Cumulative messages pushed across the cluster.",
            json_u64(totals, "pushMessages"));
    counter("queen_cluster_pop_messages_total",
            "Cumulative messages popped across the cluster (may exceed push_messages due to consumer-group fan-out).",
            json_u64(totals, "popMessages"));
    counter("queen_cluster_ack_messages_total",
            "Cumulative ack attempts across the cluster.",
            json_u64(totals, "ackMessages"));

    b.help("queen_cluster_ack_total",
           "Cumulative acks across the cluster, by result.");
    b.type("queen_cluster_ack_total", "counter");
    b.sample("queen_cluster_ack_total",
             labels({{"scope", "cluster"}, {"result", "success"}}),
             json_u64(totals, "ackSuccess"));
    b.sample("queen_cluster_ack_total",
             labels({{"scope", "cluster"}, {"result", "failed"}}),
             json_u64(totals, "ackFailed"));

    counter("queen_cluster_db_errors_total",
            "Cumulative database errors observed across the cluster.",
            json_u64(totals, "dbErrors"));
    counter("queen_cluster_dlq_total",
            "Cumulative messages routed to the dead-letter queue across the cluster.",
            json_u64(totals, "dlqCount"));
}

// ---------------------------------------------------------------------------
// Per-queue last-minute series (queen.queue_lag_metrics).
// Each metric is emitted as a gauge — values are deltas for the most recent
// minute bucket, not lifetime cumulative — so PromQL should NOT use rate().
// Use the metric value directly as "events per minute" or divide by 60 for
// per-second.
// ---------------------------------------------------------------------------
void format_per_queue(Body& b, const nlohmann::json& per_queue) {
    if (!per_queue.is_array() || per_queue.empty()) return;

    auto family = [&](const char* name, const char* help, const char* type) {
        b.help(name, help); b.type(name, type);
    };

    family("queen_queue_pop_messages_per_minute",
           "Messages popped from this queue in the most recent minute bucket "
           "(includes consumer-group fan-out).", "gauge");
    family("queen_queue_pop_lag_milliseconds",
           "Pop latency over the most recent minute bucket, by stat.", "gauge");
    family("queen_queue_push_requests_per_minute",
           "Push HTTP requests targeting this queue in the most recent minute.", "gauge");
    family("queen_queue_push_messages_per_minute",
           "Messages enqueued to this queue in the most recent minute.", "gauge");
    family("queen_queue_pop_empty_per_minute",
           "Empty pop responses for this queue in the most recent minute.", "gauge");
    family("queen_queue_ack_per_minute",
           "Acks for this queue in the most recent minute, by result.", "gauge");
    family("queen_queue_transactions_per_minute",
           "Transactions touching this queue in the most recent minute.", "gauge");
    family("queen_queue_parked_consumers",
           "Consumers currently parked on this queue (cluster-wide minute-average).", "gauge");
    family("queen_queue_metrics_age_seconds",
           "Seconds since the latest queue_lag_metrics bucket was flushed.", "gauge");

    for (const auto& q : per_queue) {
        if (!q.is_object()) continue;
        std::string queue;
        if (q.contains("queue") && q["queue"].is_string()) queue = q["queue"].get<std::string>();
        if (queue.empty()) continue;

        const std::string l = labels({{"queue", queue}});
        b.sample("queen_queue_pop_messages_per_minute",   l, json_u64(q, "pop_count"));
        b.sample("queen_queue_pop_lag_milliseconds",
                 labels({{"queue", queue}, {"stat", "avg"}}),
                 json_u64(q, "avg_lag_ms"));
        b.sample("queen_queue_pop_lag_milliseconds",
                 labels({{"queue", queue}, {"stat", "max"}}),
                 json_u64(q, "max_lag_ms"));
        b.sample("queen_queue_push_requests_per_minute",  l, json_u64(q, "push_request_count"));
        b.sample("queen_queue_push_messages_per_minute",  l, json_u64(q, "push_message_count"));
        b.sample("queen_queue_pop_empty_per_minute",      l, json_u64(q, "pop_empty_count"));
        b.sample("queen_queue_ack_per_minute",
                 labels({{"queue", queue}, {"result", "success"}}),
                 json_u64(q, "ack_success_count"));
        b.sample("queen_queue_ack_per_minute",
                 labels({{"queue", queue}, {"result", "failed"}}),
                 json_u64(q, "ack_failed_count"));
        b.sample("queen_queue_transactions_per_minute",   l, json_u64(q, "transaction_count"));
        b.sample("queen_queue_parked_consumers",          l, json_u64(q, "parked_count"));
        b.sample("queen_queue_metrics_age_seconds",       l, json_u64(q, "bucket_age_seconds"));
    }
}

// ---------------------------------------------------------------------------
// Per-worker last-minute series (queen.worker_metrics).
// Labels: hostname, worker_id, pid. Surfaces uneven workload across workers
// (event-loop lag spikes, queue backlog) that's invisible in cluster totals.
// ---------------------------------------------------------------------------
void format_per_worker(Body& b, const nlohmann::json& per_worker) {
    if (!per_worker.is_array() || per_worker.empty()) return;

    auto family = [&](const char* name, const char* help, const char* type) {
        b.help(name, help); b.type(name, type);
    };

    family("queen_worker_event_loop_lag_milliseconds",
           "Event-loop lag in the most recent minute bucket, by stat.", "gauge");
    family("queen_worker_free_slots",
           "Free job slots in the libqueen scheduler, by stat.", "gauge");
    family("queen_worker_db_connections", "DB connections held by this worker.", "gauge");
    family("queen_worker_job_queue_size",
           "libqueen job queue depth in the most recent minute, by stat.", "gauge");
    family("queen_worker_backoff_size",
           "Number of pop requests in backoff for this worker.", "gauge");
    family("queen_worker_jobs_done_per_minute",
           "Jobs completed by this worker in the most recent minute.", "gauge");
    family("queen_worker_requests_per_minute",
           "Requests handled by this worker in the most recent minute, by op.", "gauge");
    family("queen_worker_messages_per_minute",
           "Messages handled by this worker in the most recent minute, by op.", "gauge");
    family("queen_worker_ack_per_minute",
           "Acks handled by this worker in the most recent minute, by result.", "gauge");
    family("queen_worker_lag_milliseconds",
           "Pop lag observed by this worker in the most recent minute, by stat.", "gauge");
    family("queen_worker_db_errors_per_minute",
           "DB errors observed by this worker in the most recent minute.", "gauge");
    family("queen_worker_dlq_per_minute",
           "Messages routed to DLQ by this worker in the most recent minute.", "gauge");
    family("queen_worker_metrics_age_seconds",
           "Seconds since this worker's last metrics bucket was flushed.", "gauge");

    for (const auto& w : per_worker) {
        if (!w.is_object()) continue;
        std::string host = w.value("hostname", std::string{});
        std::string wid  = std::to_string(json_u64(w, "worker_id"));
        std::string pid  = std::to_string(json_u64(w, "pid"));

        const std::string base = labels({
            {"hostname",  host},
            {"worker_id", wid},
            {"pid",       pid},
        });

        b.sample("queen_worker_event_loop_lag_milliseconds",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"stat", "avg"}}),
                 json_u64(w, "avg_event_loop_lag_ms"));
        b.sample("queen_worker_event_loop_lag_milliseconds",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"stat", "max"}}),
                 json_u64(w, "max_event_loop_lag_ms"));
        b.sample("queen_worker_free_slots",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"stat", "avg"}}),
                 json_u64(w, "avg_free_slots"));
        b.sample("queen_worker_free_slots",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"stat", "min"}}),
                 json_u64(w, "min_free_slots"));
        b.sample("queen_worker_db_connections",   base, json_u64(w, "db_connections"));
        b.sample("queen_worker_job_queue_size",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"stat", "avg"}}),
                 json_u64(w, "avg_job_queue_size"));
        b.sample("queen_worker_job_queue_size",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"stat", "max"}}),
                 json_u64(w, "max_job_queue_size"));
        b.sample("queen_worker_backoff_size",        base, json_u64(w, "backoff_size"));
        b.sample("queen_worker_jobs_done_per_minute", base, json_u64(w, "jobs_done"));

        b.sample("queen_worker_requests_per_minute",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"op", "push"}}),
                 json_u64(w, "push_request_count"));
        b.sample("queen_worker_requests_per_minute",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"op", "pop"}}),
                 json_u64(w, "pop_request_count"));
        b.sample("queen_worker_requests_per_minute",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"op", "ack"}}),
                 json_u64(w, "ack_request_count"));
        b.sample("queen_worker_requests_per_minute",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"op", "transaction"}}),
                 json_u64(w, "transaction_count"));

        b.sample("queen_worker_messages_per_minute",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"op", "push"}}),
                 json_u64(w, "push_message_count"));
        b.sample("queen_worker_messages_per_minute",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"op", "pop"}}),
                 json_u64(w, "pop_message_count"));
        b.sample("queen_worker_messages_per_minute",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"op", "ack"}}),
                 json_u64(w, "ack_message_count"));

        b.sample("queen_worker_ack_per_minute",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"result", "success"}}),
                 json_u64(w, "ack_success_count"));
        b.sample("queen_worker_ack_per_minute",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"result", "failed"}}),
                 json_u64(w, "ack_failed_count"));

        b.sample("queen_worker_lag_milliseconds",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"stat", "avg"}}),
                 json_u64(w, "avg_lag_ms"));
        b.sample("queen_worker_lag_milliseconds",
                 labels({{"hostname", host}, {"worker_id", wid}, {"pid", pid}, {"stat", "max"}}),
                 json_u64(w, "max_lag_ms"));

        b.sample("queen_worker_db_errors_per_minute",  base, json_u64(w, "db_error_count"));
        b.sample("queen_worker_dlq_per_minute",        base, json_u64(w, "dlq_count"));
        b.sample("queen_worker_metrics_age_seconds",   base, json_u64(w, "bucket_age_seconds"));
    }
}

// ---------------------------------------------------------------------------
// DLQ depth — current count, total + per-queue.
// ---------------------------------------------------------------------------
void format_dlq(Body& b, const nlohmann::json& dlq) {
    if (!dlq.is_object()) return;

    b.help("queen_dlq_depth",
           "Messages currently sitting in the dead-letter queue.");
    b.type("queen_dlq_depth", "gauge");
    b.sample("queen_dlq_depth",
             labels({{"scope", "cluster"}}),
             json_u64(dlq, "total"));

    if (!dlq.contains("per_queue") || !dlq["per_queue"].is_array()) return;

    b.help("queen_dlq_depth_by_queue",
           "Messages in DLQ per queue.");
    b.type("queen_dlq_depth_by_queue", "gauge");
    for (const auto& row : dlq["per_queue"]) {
        if (!row.is_object()) continue;
        std::string queue = row.value("queue", std::string{});
        if (queue.empty()) continue;
        b.sample("queen_dlq_depth_by_queue",
                 labels({{"queue", queue}}),
                 json_u64(row, "count"));
    }
}

// ---------------------------------------------------------------------------
// Top-level: parse SP result and append all DB-sourced metric families.
// Returns the empty string if the SP result is unusable so the route falls
// back to live-only metrics gracefully.
// ---------------------------------------------------------------------------
std::string format_db_metrics(const std::string& sp_result) {
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(sp_result);
    } catch (const std::exception& e) {
        spdlog::debug("[/metrics/prometheus] DB metrics parse failed: {}", e.what());
        return {};
    }

    nlohmann::json root = unwrap_sp(j, "get_prometheus_metrics_v1");
    if (!root.is_object()) return {};

    Body b;
    if (root.contains("system_totals")) format_cluster_totals(b, root["system_totals"]);
    if (root.contains("per_queue_lag")) format_per_queue(b, root["per_queue_lag"]);
    if (root.contains("per_worker"))    format_per_worker(b, root["per_worker"]);
    if (root.contains("dlq"))           format_dlq(b, root["dlq"]);
    return b.str();
}

// Send the assembled body as Prometheus text. Must run on the worker loop
// thread (uWS responses are not thread-safe).
void send_prometheus(uWS::HttpResponse<false>* res, const std::string& body) {
    res->writeStatus("200 OK");
    res->writeHeader("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
    res->writeHeader("Cache-Control", "no-cache");
    res->end(body);
}

}  // namespace

// ---------------------------------------------------------------------------
// Route registration.
// ---------------------------------------------------------------------------
void setup_prometheus_routes(uWS::App* app, const RouteContext& ctx) {
    app->get("/metrics/prometheus", [ctx](auto* res, auto* req) {
        (void)req;

        // Build the in-process portion synchronously — none of these reads
        // touch the database.
        Body body;
        try {
            write_process_metrics(body, ctx);
            write_db_pool_metrics(body, ctx);
            write_threadpool_metrics(body, ctx);
            write_registry_metrics(body);
            write_file_buffer_metrics(body, ctx);
            write_maintenance_metrics(body, ctx);
            write_sidecar_metrics(body, ctx);
        } catch (const std::exception& e) {
            spdlog::warn("[/metrics/prometheus] live block failed: {}", e.what());
        }

        const std::string live = body.str();

        // If the per-worker libqueen instance is not ready (early startup
        // window, or DB unavailable in failover mode) just return live
        // metrics — Prometheus will still scrape successfully.
        if (!ctx.queen) {
            send_prometheus(res, live);
            return;
        }

        // Track abort so the deferred callback knows whether it can write.
        // Both onAborted and worker_loop->defer run on the same uWS loop
        // thread, so a plain bool would suffice; using atomic for clarity.
        auto aborted = std::make_shared<std::atomic<bool>>(false);
        res->onAborted([aborted]() { aborted->store(true, std::memory_order_relaxed); });

        auto* worker_loop = ctx.worker_loop;

        queen::JobRequest job_req;
        job_req.op_type    = queen::JobType::CUSTOM;
        job_req.request_id = "prom_" + queen::generate_uuidv7();
        job_req.sql        = "SELECT queen.get_prometheus_metrics_v1()";

        try {
            ctx.queen->submit(std::move(job_req),
                [res, worker_loop, aborted, live = std::move(live)](std::string result) {
                    worker_loop->defer(
                        [res, aborted, live = std::move(live), result = std::move(result)]() mutable {
                            if (aborted->load(std::memory_order_relaxed)) return;
                            // Deferred callbacks run on the uWS loop: a throw here would
                            // escape to the acceptor catch-all and kill the worker.
                            try {
                                std::string db_block = format_db_metrics(result);
                                if (!db_block.empty()) live.append(db_block);
                                send_prometheus(res, live);
                            } catch (const std::exception& e) {
                                spdlog::warn("[/metrics/prometheus] defer failed: {}", e.what());
                                if (!aborted->load(std::memory_order_relaxed)) send_prometheus(res, live);
                            }
                        });
                });
        } catch (const std::exception& e) {
            spdlog::warn("[/metrics/prometheus] submit failed: {}", e.what());
            if (!aborted->load(std::memory_order_relaxed)) send_prometheus(res, std::string());
        }
    });
}

}  // namespace routes
}  // namespace queen
