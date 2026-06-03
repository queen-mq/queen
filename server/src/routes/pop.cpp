#include "queen/routes/route_registry.hpp"
#include "queen/routes/route_context.hpp"
#include "queen/routes/route_helpers.hpp"
#include "queen/async_queue_manager.hpp"
#include "queen/response_queue.hpp"
#include "queen/queue_types.hpp"
#include "queen/shared_state_manager.hpp"
#include "queen/encryption.hpp"
#include "queen.hpp"  // libqueen
#include <spdlog/spdlog.h>
#include <chrono>

// External globals (declared in acceptor_server.cpp)
namespace queen {
extern std::vector<std::shared_ptr<ResponseRegistry>> worker_response_registries;
extern std::shared_ptr<SharedStateManager> global_shared_state;
}

namespace queen {
namespace routes {

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
                send_json_response(res, response, 204);
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
            
            // Helper to decrypt messages
            auto decrypt_messages = [](nlohmann::json& response) {
                if (!response.contains("messages") || !response["messages"].is_array()) return;
                queen::EncryptionService* enc_service = queen::get_encryption_service();
                if (!enc_service || !enc_service->is_enabled()) return;
                
                for (auto& msg : response["messages"]) {
                    if (msg.contains("data") && msg["data"].is_object()) {
                        auto& data = msg["data"];
                        if (data.contains("encrypted") && data.contains("iv") && data.contains("authTag")) {
                            try {
                                queen::EncryptionService::EncryptedData enc_data{
                                    data["encrypted"].get<std::string>(),
                                    data["iv"].get<std::string>(),
                                    data["authTag"].get<std::string>()
                                };
                                auto decrypted = enc_service->decrypt_payload(enc_data);
                                if (decrypted.has_value()) {
                                    msg["data"] = nlohmann::json::parse(decrypted.value());
                                }
                            } catch (...) {}
                        }
                    }
                }
            };
            
            // Submit to Queen
            ctx.queen->submit(std::move(job_req), [worker_loop, worker_id, request_id, decrypt_messages](std::string result) {
                worker_loop->defer([result = std::move(result), worker_id, request_id, decrypt_messages]() {
                    nlohmann::json json_response;
                    int status_code = 200;
                    bool is_error = false;
                    
                    try {
                        json_response = nlohmann::json::parse(result);
                        
                        // Extract result from array format
                        if (json_response.is_array() && !json_response.empty() && 
                            json_response[0].contains("result")) {
                            json_response = json_response[0]["result"];
                        }
                        
                        // Decrypt any encrypted payloads
                        decrypt_messages(json_response);
                        
                        // Set status code based on whether messages were returned
                        if (json_response.contains("messages") && json_response["messages"].empty()) {
                            status_code = 204;  // No Content
                        }
                    } catch (const std::exception& e) {
                        json_response = {{"error", e.what()}};
                        status_code = 500;
                        is_error = true;
                    }
                    
                    worker_response_registries[worker_id]->send_response(
                        request_id, json_response, is_error, status_code);
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
                send_json_response(res, response, 204);
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
            
            // Helper to decrypt messages
            auto decrypt_messages = [](nlohmann::json& response) {
                if (!response.contains("messages") || !response["messages"].is_array()) return;
                queen::EncryptionService* enc_service = queen::get_encryption_service();
                if (!enc_service || !enc_service->is_enabled()) return;
                
                for (auto& msg : response["messages"]) {
                    if (msg.contains("data") && msg["data"].is_object()) {
                        auto& data = msg["data"];
                        if (data.contains("encrypted") && data.contains("iv") && data.contains("authTag")) {
                            try {
                                queen::EncryptionService::EncryptedData enc_data{
                                    data["encrypted"].get<std::string>(),
                                    data["iv"].get<std::string>(),
                                    data["authTag"].get<std::string>()
                                };
                                auto decrypted = enc_service->decrypt_payload(enc_data);
                                if (decrypted.has_value()) {
                                    msg["data"] = nlohmann::json::parse(decrypted.value());
                                }
                            } catch (...) {}
                        }
                    }
                }
            };
            
            // Submit to Queen
            ctx.queen->submit(std::move(job_req), [worker_loop, worker_id, request_id, decrypt_messages](std::string result) {
                worker_loop->defer([result = std::move(result), worker_id, request_id, decrypt_messages]() {
                    nlohmann::json json_response;
                    int status_code = 200;
                    bool is_error = false;
                    
                    try {
                        json_response = nlohmann::json::parse(result);
                        
                        // Extract result from array format
                        if (json_response.is_array() && !json_response.empty() && 
                            json_response[0].contains("result")) {
                            json_response = json_response[0]["result"];
                        }
                        
                        // Decrypt any encrypted payloads
                        decrypt_messages(json_response);
                        
                        // Set status code based on whether messages were returned
                        if (json_response.contains("messages") && json_response["messages"].empty()) {
                            status_code = 204;
                        }
                    } catch (const std::exception& e) {
                        json_response = {{"error", e.what()}};
                        status_code = 500;
                        is_error = true;
                    }
                    
                    worker_response_registries[worker_id]->send_response(
                        request_id, json_response, is_error, status_code);
                });
            });
            
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });
}

} // namespace routes
} // namespace queen

