#include "queen/routes/route_registry.hpp"
#include "queen/routes/route_context.hpp"
#include "queen/routes/route_helpers.hpp"
#include "queen/async_queue_manager.hpp"
#include "queen/response_queue.hpp"
#include "queen/queue_types.hpp"
#include "queen/shared_state_manager.hpp"
#include "queen/routes/storage_v2_routes.hpp"
#include "queen/encryption.hpp"
#include "queen.hpp"  // libqueen
#include "simdjson.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <string>

// External globals (declared in acceptor_server.cpp)
namespace queen {
extern std::vector<std::shared_ptr<ResponseRegistry>> worker_response_registries;
extern std::shared_ptr<SharedStateManager> global_shared_state;
}

namespace queen {
namespace routes {

namespace {

// Decrypt encrypted message payloads in place (only when encryption is enabled).
void pop_decrypt_messages(nlohmann::json& response) {
    if (!response.contains("messages") || !response["messages"].is_array()) return;
    EncryptionService* enc_service = get_encryption_service();
    if (!enc_service || !enc_service->is_enabled()) return;
    for (auto& msg : response["messages"]) {
        if (msg.contains("data") && msg["data"].is_object()) {
            auto& data = msg["data"];
            if (data.contains("encrypted") && data.contains("iv") && data.contains("authTag")) {
                try {
                    EncryptionService::EncryptedData enc_data{
                        data["encrypted"].get<std::string>(),
                        data["iv"].get<std::string>(),
                        data["authTag"].get<std::string>()
                    };
                    auto decrypted = enc_service->decrypt_payload(enc_data);
                    if (decrypted.has_value()) msg["data"] = nlohmann::json::parse(decrypted.value());
                } catch (...) {}
            }
        }
    }
}

// True if the "messages" array inside the raw result object is empty (tolerant
// of JSONB whitespace).
bool pop_messages_empty(std::string_view r) {
    size_t m = r.find("\"messages\"");
    if (m == std::string_view::npos) return true;
    size_t b = r.find('[', m);
    if (b == std::string_view::npos) return true;
    size_t i = r.find_first_not_of(" \t\r\n", b + 1);
    return (i == std::string_view::npos || r[i] == ']');
}

// Deliver a POP result. libqueen returns [{idx, result:{...}}]; the client gets
// the inner `result` object. With encryption OFF (the common case) we slice that
// object out RAW with simdjson and stream it — no nlohmann parse/dump. With
// encryption ON (rare) we fall back to nlohmann so payloads can be decrypted.
void pop_deliver(int worker_id, const std::string& request_id, std::string result) {
    EncryptionService* enc = get_encryption_service();
    bool enc_on = enc && enc->is_enabled();

    if (!enc_on) {
        thread_local simdjson::ondemand::parser parser;
        simdjson::padded_string padded(result);
        simdjson::ondemand::document doc;
        simdjson::ondemand::array arr;
        if (!parser.iterate(padded).get(doc) && !doc.get_array().get(arr)) {
            for (auto el : arr) {
                simdjson::ondemand::object o;
                if (el.get_object().get(o)) break;
                simdjson::ondemand::value rv;
                if (o["result"].get(rv) == simdjson::SUCCESS) {
                    std::string_view rraw;
                    if (rv.raw_json().get(rraw) == simdjson::SUCCESS) {
                        int status = pop_messages_empty(rraw) ? 204 : 200;
                        worker_response_registries[worker_id]->send_response_raw(
                            request_id, std::string(rraw), status);
                        return;
                    }
                }
                break;  // only the first (and only) element
            }
        }
        // any parse hiccup falls through to the nlohmann path
    }

    nlohmann::json json_response;
    int status_code = 200;
    bool is_error = false;
    try {
        json_response = nlohmann::json::parse(result);
        if (json_response.is_array() && !json_response.empty() && json_response[0].contains("result")) {
            json_response = json_response[0]["result"];
        }
        pop_decrypt_messages(json_response);
        if (json_response.contains("messages") && json_response["messages"].empty()) status_code = 204;
    } catch (const std::exception& e) {
        json_response = {{"error", e.what()}};
        status_code = 500;
        is_error = true;
    }
    worker_response_registries[worker_id]->send_response(request_id, json_response, is_error, status_code);
}

} // namespace

void setup_pop_routes(uWS::App* app, const RouteContext& ctx) {
    // SPECIFIC POP from queue/partition
    app->get("/api/v1/pop/queue/:queue/partition/:partition", [ctx](auto* res, auto* req) {
        // Check authentication - READ_WRITE required for pop (consumes messages)
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_WRITE);
        
        try {
            // POP MAINTENANCE MODE: Return empty immediately if enabled
            // Zero-cost check: just reads an atomic bool, no DB hit
            if (global_shared_state && global_shared_state->get_pop_maintenance_mode()) {
                nlohmann::json response = {
                    {"messages", nlohmann::json::array()},
                    {"paused", true}
                };
                send_json_response(res, response, 200);
                return;
            }
            
            std::string queue_name = std::string(req->getParameter(0));
            std::string partition_name = std::string(req->getParameter(1));
            std::string consumer_group = get_query_param(req, "consumerGroup", "__QUEUE_MODE__");
            
            bool wait = get_query_param_bool(req, "wait", false);
            int timeout_ms = get_query_param_int(req, "timeout", ctx.config.queue.default_timeout);
            int batch = get_query_param_int(req, "batch", ctx.config.queue.default_batch_size);
            // v4: ?partitions=N requests up to N partitions in a single pop.
            // On the specific-partition route this is silently capped at 1
            // by the SQL procedure (the wildcard branch is the only place
            // that honours max_partitions > 1).
            int max_partitions = std::max(1, get_query_param_int(req, "partitions", 1));

            // Pool stats available via ctx.async_queue_manager->get_pool_stats() if needed for debugging
            
            PopOptions options;
            options.wait = false;  // Always false - registry handles waiting
            options.timeout = timeout_ms;
            options.batch = batch;
            options.max_partitions = max_partitions;
            options.auto_ack = get_query_param_bool(req, "autoAck", false);
            
            // Parse subscription mode
            std::string sub_mode = get_query_param(req, "subscriptionMode", "");
            if (!sub_mode.empty()) {
                options.subscription_mode = sub_mode;
            }
            std::string sub_from = get_query_param(req, "subscriptionFrom", "");
            if (!sub_from.empty()) {
                options.subscription_from = sub_from;
            }

            // STORAGE V2: segment queues take the segments pop path (same
            // wire format). Long-poll wait is deadline-polled inside the v2
            // handler (no libqueen parking for CUSTOM jobs). The effective
            // subscription window is computed exactly like the v1 path below
            // and forwarded to the q2 SQL args.
            if (queen::routes_v2::is_segment_queue(queue_name)) {
                int lease_secs = 300;
                if (global_shared_state) {
                    auto cfg = global_shared_state->get_or_fetch_queue_config(queue_name);
                    if (cfg && cfg->lease_time > 0) lease_secs = cfg->lease_time;
                }
                std::string v2_sub_mode = options.subscription_mode.value_or(
                    ctx.config.queue.default_subscription_mode);
                std::string v2_sub_from = options.subscription_from.value_or("");
                queen::routes_v2::handle_pop_v2(ctx, res, queue_name, partition_name,
                                                consumer_group, options.batch,
                                                lease_secs, options.auto_ack,
                                                wait, timeout_ms,
                                                v2_sub_mode, v2_sub_from);
                return;
            }

            // Register response for async delivery
            std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
                res, ctx.worker_id, 
                wait ? [ctx](const std::string& req_id) {
                    spdlog::info("SPOP: Connection aborted for {}", req_id);
                    ctx.queen->invalidate_request(req_id);
                } : std::function<void(const std::string&)>(nullptr)
            );
            
            // Build pop params JSON for stored procedure
            std::string worker_id_str = ctx.async_queue_manager->generate_uuid();
            // Use server's default subscription mode if not specified (empty string = "all")
            std::string effective_sub_mode = options.subscription_mode.value_or(ctx.config.queue.default_subscription_mode);
            std::string effective_sub_from = options.subscription_from.value_or("");
            
            nlohmann::json pop_params = nlohmann::json::array();
            pop_params.push_back({
                {"queue_name", queue_name},
                {"partition_name", partition_name},
                {"consumer_group", consumer_group},
                {"batch_size", options.batch},
                {"lease_seconds", 0},  // 0 = use queue's configured leaseTime
                {"worker_id", worker_id_str},
                {"sub_mode", effective_sub_mode},
                {"sub_from", effective_sub_from},
                {"auto_ack", options.auto_ack},
                {"max_partitions", options.max_partitions}
            });
            
            // Build Queen job request
            queen::JobRequest job_req;
            job_req.op_type = queen::JobType::POP;
            job_req.request_id = request_id;
            job_req.queue_name = queue_name;
            job_req.partition_name = partition_name;
            job_req.consumer_group = consumer_group;
            job_req.batch_size = options.batch;
            job_req.max_partitions = options.max_partitions;
            job_req.auto_ack = options.auto_ack;
            job_req.params = {pop_params.dump()};
            
            // Set wait deadline if long-polling
            if (wait) {
                job_req.wait_deadline = std::chrono::steady_clock::now() + 
                                        std::chrono::milliseconds(timeout_ms);
                job_req.next_check = std::chrono::steady_clock::now();  // Check immediately
            }
            
            // Capture context for callback
            auto worker_loop = ctx.worker_loop;
            auto worker_id = ctx.worker_id;

            ctx.queen->submit(std::move(job_req), [worker_loop, worker_id, request_id](std::string result) {
                worker_loop->defer([result = std::move(result), worker_id, request_id]() {
                    pop_deliver(worker_id, request_id, std::move(result));
                });
            });
            
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });
    
    // POP from queue (any partition)
    app->get("/api/v1/pop/queue/:queue", [ctx](auto* res, auto* req) {
        // Check authentication - READ_WRITE required for pop (consumes messages)
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_WRITE);
        
        try {
            // POP MAINTENANCE MODE: Return empty immediately if enabled
            // Zero-cost check: just reads an atomic bool, no DB hit
            if (global_shared_state && global_shared_state->get_pop_maintenance_mode()) {
                nlohmann::json response = {
                    {"messages", nlohmann::json::array()},
                    {"paused", true}
                };
                send_json_response(res, response, 200);
                return;
            }
            
            std::string queue_name = std::string(req->getParameter(0));
            std::string consumer_group = get_query_param(req, "consumerGroup", "__QUEUE_MODE__");
            
            bool wait = get_query_param_bool(req, "wait", false);
            int timeout_ms = get_query_param_int(req, "timeout", ctx.config.queue.default_timeout);
            int batch = get_query_param_int(req, "batch", ctx.config.queue.default_batch_size);
            // v4: ?partitions=N drains up to N sparsely-loaded partitions in
            // a single round-trip (wildcard pop only).
            int max_partitions = std::max(1, get_query_param_int(req, "partitions", 1));

            PopOptions options;
            options.wait = false;  // Always false - registry handles waiting
            options.timeout = timeout_ms;
            options.batch = batch;
            options.max_partitions = max_partitions;
            options.auto_ack = get_query_param_bool(req, "autoAck", false);
            
            // Parse subscription mode
            std::string sub_mode = get_query_param(req, "subscriptionMode", "");
            if (!sub_mode.empty()) {
                options.subscription_mode = sub_mode;
            }
            std::string sub_from = get_query_param(req, "subscriptionFrom", "");
            if (!sub_from.empty()) {
                options.subscription_from = sub_from;
            }

            // STORAGE V2: wildcard pop over a segment queue drains up to
            // ?partitions=N partitions per call, mirroring the v4 shape. The
            // effective subscription window is computed exactly like the v1
            // path below and forwarded to the q2 SQL args.
            if (queen::routes_v2::is_segment_queue(queue_name)) {
                int lease_secs = 300;
                if (global_shared_state) {
                    auto cfg = global_shared_state->get_or_fetch_queue_config(queue_name);
                    if (cfg && cfg->lease_time > 0) lease_secs = cfg->lease_time;
                }
                std::string v2_sub_mode = options.subscription_mode.value_or(
                    ctx.config.queue.default_subscription_mode);
                std::string v2_sub_from = options.subscription_from.value_or("");
                queen::routes_v2::handle_pop_wildcard_v2(ctx, res, queue_name,
                                                         consumer_group, options.batch,
                                                         lease_secs, options.auto_ack,
                                                         options.max_partitions,
                                                         wait, timeout_ms,
                                                         v2_sub_mode, v2_sub_from);
                return;
            }

            // Register response for async delivery
            std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
                res, ctx.worker_id,
                wait ? [ctx](const std::string& req_id) {
                    spdlog::info("QPOP: Connection aborted for {}", req_id);
                    ctx.queen->invalidate_request(req_id);
                } : std::function<void(const std::string&)>(nullptr)
            );
            
            // Build pop params JSON for stored procedure
            std::string worker_id_str = ctx.async_queue_manager->generate_uuid();
            // Use server's default subscription mode if not specified (empty string = "all")
            std::string effective_sub_mode = options.subscription_mode.value_or(ctx.config.queue.default_subscription_mode);
            std::string effective_sub_from = options.subscription_from.value_or("");
            
            nlohmann::json pop_params = nlohmann::json::array();
            pop_params.push_back({
                {"queue_name", queue_name},
                {"partition_name", ""},  // Any partition
                {"consumer_group", consumer_group},
                {"batch_size", options.batch},
                {"lease_seconds", 0},  // 0 = use queue's configured leaseTime
                {"worker_id", worker_id_str},
                {"sub_mode", effective_sub_mode},
                {"sub_from", effective_sub_from},
                {"auto_ack", options.auto_ack},
                {"max_partitions", options.max_partitions}
            });
            
            // Build Queen job request
            queen::JobRequest job_req;
            job_req.op_type = queen::JobType::POP;
            job_req.request_id = request_id;
            job_req.queue_name = queue_name;
            job_req.partition_name = "";  // Any partition
            job_req.consumer_group = consumer_group;
            job_req.batch_size = options.batch;
            job_req.max_partitions = options.max_partitions;
            job_req.auto_ack = options.auto_ack;
            job_req.params = {pop_params.dump()};
            
            // Set wait deadline if long-polling
            if (wait) {
                job_req.wait_deadline = std::chrono::steady_clock::now() + 
                                        std::chrono::milliseconds(timeout_ms);
                job_req.next_check = std::chrono::steady_clock::now();
            }
            
            // Capture context for callback
            auto worker_loop = ctx.worker_loop;
            auto worker_id = ctx.worker_id;

            ctx.queen->submit(std::move(job_req), [worker_loop, worker_id, request_id](std::string result) {
                worker_loop->defer([result = std::move(result), worker_id, request_id]() {
                    pop_deliver(worker_id, request_id, std::move(result));
                });
            });
            
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });
}

} // namespace routes
} // namespace queen

