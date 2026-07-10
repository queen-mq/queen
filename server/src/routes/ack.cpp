#include "queen/routes/route_registry.hpp"
#include "queen/routes/route_context.hpp"
#include "queen/routes/route_helpers.hpp"
#include "queen/async_queue_manager.hpp"
#include "queen/shared_state_manager.hpp"
#include "queen/routes/storage_v2_routes.hpp"
#include "queen.hpp"  // libqueen
#include "queen/response_queue.hpp"
#include <spdlog/spdlog.h>
#include <chrono>

// External globals
namespace queen {
extern std::shared_ptr<SharedStateManager> global_shared_state;
extern std::vector<std::shared_ptr<ResponseRegistry>> worker_response_registries;
}

namespace queen {
namespace routes {

void setup_ack_routes(uWS::App* app, const RouteContext& ctx) {
    // ASYNC ACK batch
    app->post("/api/v1/ack/batch", [ctx](auto* res, auto* req) {
        // Check authentication - READ_WRITE required for ack
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_WRITE);
        
        read_json_body(res,
            [res, ctx](const nlohmann::json& body) {
                try {
                    if (!body.contains("acknowledgments") || !body["acknowledgments"].is_array()) {
                        send_error_response(res, "acknowledgments array is required", 400);
                        return;
                    }
                    
                    auto acknowledgments = body["acknowledgments"];
                    std::string consumer_group = "__QUEUE_MODE__";
                    
                    if (body.contains("consumerGroup") && !body["consumerGroup"].is_null() && body["consumerGroup"].is_string()) {
                        consumer_group = body["consumerGroup"];
                    }
                    
                    // Validate each acknowledgment has required fields
                    std::vector<nlohmann::json> ack_items;
                    for (const auto& ack_json : acknowledgments) {
                        if (!ack_json.contains("transactionId") || ack_json["transactionId"].is_null() || !ack_json["transactionId"].is_string()) {
                            send_error_response(res, "Each acknowledgment must have a valid transactionId string", 400);
                            return;
                        }
                        
                        // CRITICAL: partition_id is now MANDATORY
                        if (!ack_json.contains("partitionId") || ack_json["partitionId"].is_null() || !ack_json["partitionId"].is_string()) {
                            send_error_response(res, "Each acknowledgment must have a valid partitionId string to ensure message uniqueness", 400);
                            return;
                        }
                        
                        // Build ack item with consumer group
                        nlohmann::json ack_item = ack_json;
                        ack_item["consumerGroup"] = consumer_group;
                        ack_items.push_back(ack_item);
                    }

                    // STORAGE V2: acks whose leaseId belongs to a segments
                    // lease are resolved via the in-process lease registry.
                    {
                        nlohmann::json probe = nlohmann::json::array();
                        for (const auto& a : ack_items) probe.push_back(a);
                        if (queen::routes_v2::try_handle_ack_v2(ctx, res, probe)) {
                            return;
                        }
                    }

                    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
                        res, ctx.worker_id, nullptr
                    );
                        
                    // libqueen injects idx/index during the batched merge.
                    nlohmann::json ack_json = nlohmann::json::array();
                    for (const auto& ack : ack_items) {
                        ack_json.push_back(ack);
                    }
                    
                    // Build Queen job request
                    queen::JobRequest job_req;
                    job_req.op_type = queen::JobType::ACK;
                    job_req.request_id = request_id;
                    job_req.params = {ack_json.dump()};
                    job_req.item_count = ack_items.size();
                    
                    // Capture context for callback
                    auto worker_loop = ctx.worker_loop;
                    auto worker_id = ctx.worker_id;
                    
                    ctx.queen->submit(std::move(job_req), [worker_loop, worker_id, request_id](std::string result) {
                        worker_loop->defer([result = std::move(result), worker_id, request_id]() {
                            nlohmann::json json_response;
                            int status_code = 200;
                            bool is_error = false;
                            
                            try {
                                json_response = nlohmann::json::parse(result);
                            } catch (const std::exception& e) {
                                json_response = {{"error", e.what()}};
                                status_code = 500;
                                is_error = true;
                            }
                            
                            worker_response_registries[worker_id]->send_response(
                                request_id, json_response, is_error, status_code);
                        });
                    });
                    
                    spdlog::debug("[Worker {}] ACK BATCH: Submitted {} items (request_id={})", 
                                 ctx.worker_id, ack_items.size(), request_id);
                    
                } catch (const std::exception& e) {
                    send_error_response(res, e.what(), 500);
                }
            },
            [res](const std::string& error) {
                send_error_response(res, error, 400);
            }
        );
    });
    
    // ASYNC Single ACK
    app->post("/api/v1/ack", [ctx](auto* res, auto* req) {
        // Check authentication - READ_WRITE required for ack
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_WRITE);
        
        read_json_body(res,
            [res, ctx](const nlohmann::json& body) {
                try {
                    std::string transaction_id = "";
                    if (body.contains("transactionId") && !body["transactionId"].is_null() && body["transactionId"].is_string()) {
                        transaction_id = body["transactionId"];
                    } else {
                        send_error_response(res, "transactionId is required", 400);
                        return;
                    }
                    
                    std::string status = "completed";
                    if (body.contains("status") && !body["status"].is_null() && body["status"].is_string()) {
                        status = body["status"];
                    }
                    
                    std::string consumer_group = "__QUEUE_MODE__";
                    if (body.contains("consumerGroup") && !body["consumerGroup"].is_null() && body["consumerGroup"].is_string()) {
                        consumer_group = body["consumerGroup"];
                    }
                    
                    std::optional<std::string> error;
                    if (body.contains("error") && !body["error"].is_null() && body["error"].is_string()) {
                        error = body["error"];
                    }
                    
                    std::optional<std::string> lease_id;
                    if (body.contains("leaseId") && !body["leaseId"].is_null() && body["leaseId"].is_string()) {
                        lease_id = body["leaseId"];
                    }
                    
                    // CRITICAL: partition_id is now MANDATORY to prevent acking wrong message
                    // when transactionId is not unique across partitions
                    std::optional<std::string> partition_id;
                    if (body.contains("partitionId") && !body["partitionId"].is_null() && body["partitionId"].is_string()) {
                        partition_id = body["partitionId"];
                    } else {
                        send_error_response(res, "partitionId is required to ensure message uniqueness", 400);
                        return;
                    }

                    // STORAGE V2: single ack against a segments lease.
                    {
                        nlohmann::json probe = nlohmann::json::array();
                        probe.push_back({{"transactionId", transaction_id},
                                         {"partitionId", partition_id.value()},
                                         {"leaseId", lease_id.value_or("")},
                                         {"status", status},
                                         {"consumerGroup", consumer_group}});
                        if (queen::routes_v2::try_handle_ack_v2(ctx, res, probe)) {
                            return;
                        }
                    }

                    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
                        res, ctx.worker_id, nullptr
                    );

                    // Build JSON array with single ACK
                    nlohmann::json ack_json = nlohmann::json::array();
                    ack_json.push_back({
                        {"transactionId", transaction_id},
                        {"partitionId", partition_id.value()},
                        {"leaseId", lease_id.value_or("")},
                        {"status", status},
                        {"consumerGroup", consumer_group},
                        {"error", error.value_or("")}
                    });
                    
                    // Build Queen job request
                    queen::JobRequest job_req;
                    job_req.op_type = queen::JobType::ACK;
                    job_req.request_id = request_id;
                    job_req.params = {ack_json.dump()};
                    job_req.item_count = 1;
                    
                    // Capture context for callback
                    auto worker_loop = ctx.worker_loop;
                    auto worker_id = ctx.worker_id;
                    
                    ctx.queen->submit(std::move(job_req), [worker_loop, worker_id, request_id](std::string result) {
                        worker_loop->defer([result = std::move(result), worker_id, request_id]() {
                            nlohmann::json json_response;
                            int status_code = 200;
                            bool is_error = false;
                            
                            try {
                                json_response = nlohmann::json::parse(result);
                            } catch (const std::exception& e) {
                                json_response = {{"error", e.what()}};
                                status_code = 500;
                                is_error = true;
                            }
                            
                            worker_response_registries[worker_id]->send_response(
                                request_id, json_response, is_error, status_code);
                        });
                    });
                    
                    spdlog::debug("[Worker {}] ACK: Submitted (request_id={})", 
                                 ctx.worker_id, request_id);
                    
                } catch (const std::exception& e) {
                    send_error_response(res, e.what(), 500);
                }
            },
            [res](const std::string& error) {
                send_error_response(res, error, 400);
            }
        );
    });
}

} // namespace routes
} // namespace queen
