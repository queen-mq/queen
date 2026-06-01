#include "queen/routes/route_registry.hpp"
#include "queen/routes/route_context.hpp"
#include "queen/routes/route_helpers.hpp"
#include "queen/async_queue_manager.hpp"
#include "queen/queue_types.hpp"
#include "queen/file_buffer.hpp"
#include "queen/response_queue.hpp"
#include "queen.hpp"  // libqueen
#include "queen/encryption.hpp"
#include "queen/shared_state_manager.hpp"
#include <spdlog/spdlog.h>
#include <chrono>
#include <set>

namespace queen {

// External globals (declared in acceptor_server.cpp)
extern std::vector<std::shared_ptr<ResponseRegistry>> worker_response_registries;
extern std::shared_ptr<SharedStateManager> global_shared_state;

namespace routes {

void setup_push_routes(uWS::App* app, const RouteContext& ctx) {
    // PUSH endpoint - uses per-worker sidecar for true async non-blocking operation
    app->post("/api/v1/push", [ctx](auto* res, auto* req) {
        // Check authentication - WRITE_ONLY required for push: produce-only
        // clients (write-only role) are allowed, read-write/admin still pass,
        // but read-only is rejected (issue #31).
        // Capture claims so we can stamp the authenticated producer 'sub' onto each message.
        // This closes the impersonation hole described in issue #23: the client's claimed
        // identity cannot be forged because the server derives it from the validated JWT.
        std::optional<auth::JwtClaims> auth_claims;
        REQUIRE_AUTH_WITH_CLAIMS(res, req, ctx, auth::AccessLevel::WRITE_ONLY, auth_claims);

        read_json_body(res,
            [res, ctx, auth_claims = std::move(auth_claims)](const nlohmann::json& body) {
                try {
                    if (!body.contains("items") || !body["items"].is_array()) {
                        send_error_response(res, "items array is required", 400);
                        return;
                    }
                    
                    size_t item_count = body["items"].size();
                    spdlog::debug("[Worker {}] PUSH: Processing {} items", ctx.worker_id, item_count);
                    
                    // Parse and validate items
                    std::vector<PushItem> items;
                    for (const auto& item_json : body["items"]) {
                        // Validate partition type if present
                        if (item_json.contains("partition") && !item_json["partition"].is_string()) {
                            send_error_response(res, "Partition must be a string", 400);
                            return;
                        }
                        
                        // Validate queue type
                        if (!item_json.contains("queue") || !item_json["queue"].is_string()) {
                            send_error_response(res, "Queue must be a string", 400);
                            return;
                        }
                        
                        // Validate transactionId type if present
                        if (item_json.contains("transactionId") && !item_json["transactionId"].is_string()) {
                            send_error_response(res, "TransactionId must be a string", 400);
                            return;
                        }
                        
                        // Validate traceId type if present
                        if (item_json.contains("traceId") && !item_json["traceId"].is_string()) {
                            send_error_response(res, "TraceId must be a string", 400);
                            return;
                        }
                        
                        PushItem item;
                        item.queue = item_json["queue"];
                        item.partition = item_json.value("partition", "Default");
                        item.payload = item_json.value("payload", nlohmann::json{});
                        if (item_json.contains("transactionId")) {
                            item.transaction_id = item_json["transactionId"];
                        }
                        if (item_json.contains("traceId")) {
                            item.trace_id = item_json["traceId"];
                        }
                        // SECURITY: producer_sub is *always* server-stamped from the validated
                        // JWT subject. Any "producerSub" field in the client body is intentionally
                        // ignored so a client cannot impersonate another producer. When auth is
                        // disabled, producer_sub stays empty (NULL in DB).
                        if (auth_claims.has_value() && !auth_claims->subject.empty()) {
                            item.producer_sub = auth_claims->subject;
                        }
                        items.push_back(std::move(item));
                    }
                    
                    // Handle empty push immediately - no need for DB call
                    if (items.empty()) {
                        send_json_response(res, nlohmann::json::array(), 201);
                        return;
                    }
                    
                    // MAINTENANCE MODE: Route to file buffer instead of sidecar
                    // Uses global_shared_state for zero-cost cached check (atomic bool read)
                    // Cache is refreshed periodically by SharedStateManager background thread
                    if (global_shared_state && global_shared_state->get_maintenance_mode() && ctx.file_buffer) {
                        spdlog::debug("[Worker {}] PUSH: Maintenance mode active, buffering {} items", 
                                     ctx.worker_id, items.size());
                        
                        nlohmann::json results = nlohmann::json::array();
                        bool all_buffered = true;
                        
                        for (const auto& item : items) {
                            nlohmann::json event = {
                                {"queue", item.queue},
                                {"partition", item.partition},
                                {"payload", item.payload},
                                {"failover", true}
                            };
                            
                            if (item.transaction_id.has_value() && !item.transaction_id->empty()) {
                                event["transactionId"] = *item.transaction_id;
                            } else {
                                event["transactionId"] = ctx.async_queue_manager->generate_uuid();
                            }
                            
                            if (item.trace_id.has_value() && !item.trace_id->empty()) {
                                event["traceId"] = *item.trace_id;
                            }

                            if (item.producer_sub.has_value() && !item.producer_sub->empty()) {
                                event["producerSub"] = *item.producer_sub;
                            }

                            if (ctx.file_buffer->write_event(event)) {
                                nlohmann::json result = {
                                    {"status", "buffered"},
                                    {"queue", item.queue},
                                    {"partition", item.partition}
                                };
                                if (item.transaction_id.has_value()) {
                                    result["transactionId"] = *item.transaction_id;
                                }
                                results.push_back(result);
                            } else {
                                all_buffered = false;
                                results.push_back({
                                    {"status", "failed"},
                                    {"queue", item.queue},
                                    {"partition", item.partition},
                                    {"error", "File buffer write failed"}
                                });
                            }
                        }
                        
                        spdlog::info("[Worker {}] PUSH: Buffered {} items during maintenance mode", 
                                    ctx.worker_id, items.size());
                        
                        send_json_response(res, results, all_buffered ? 201 : 500);
                        return;
                    }
                    
                    // Register response for async delivery (per-worker registry - no contention!)
                    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
                        res, ctx.worker_id, nullptr
                    );
                        
                    // Build JSON array for stored procedure
                    // Generate UUIDv7 message IDs on C++ side
                    // Get encryption service once for all items
                    EncryptionService* enc_service = get_encryption_service();
                    
                    nlohmann::json items_json = nlohmann::json::array();
                    for (const auto& item : items) {
                        // Check if queue has encryption enabled
                        // Uses lazy-loading: checks cache first, falls back to DB on miss
                        bool queue_encryption_enabled = false;
                        if (global_shared_state) {
                            auto config = global_shared_state->get_or_fetch_queue_config(item.queue);
                            if (config) {
                                queue_encryption_enabled = config->encryption_enabled;
                            }
                            // If config is nullopt, queue doesn't exist yet (will be auto-created)
                            // Auto-created queues have encryption_enabled=false by default
                        } else {
                            spdlog::warn("[Worker {}] PUSH: global_shared_state is NULL!", ctx.worker_id);
                        }
                        
                        // Encrypt payload if queue requires it
                        nlohmann::json payload_to_store = item.payload;
                        bool is_encrypted = false;
                        
                        if (queue_encryption_enabled && enc_service && enc_service->is_enabled()) {
                            std::string payload_str = item.payload.dump();
                            auto encrypted = enc_service->encrypt_payload(payload_str);
                            if (encrypted.has_value()) {
                                payload_to_store = {
                                    {"encrypted", encrypted->encrypted},
                                    {"iv", encrypted->iv},
                                    {"authTag", encrypted->auth_tag}
                                };
                                is_encrypted = true;
                            } else {
                                spdlog::warn("[Worker {}] PUSH: Encryption failed for queue '{}', storing plaintext", 
                                            ctx.worker_id, item.queue);
                            }
                        } else if (queue_encryption_enabled) {
                            spdlog::warn("[Worker {}] PUSH: Queue '{}' requires encryption but service not available", 
                                        ctx.worker_id, item.queue);
                        }
                        
                            nlohmann::json item_json = {
                                {"queue", item.queue},
                                {"partition", item.partition},
                                {"payload", payload_to_store},
                            {"is_encrypted", is_encrypted},
                                {"messageId", ctx.async_queue_manager->generate_uuid()}  // UUIDv7
                            };
                            if (item.transaction_id.has_value() && !item.transaction_id->empty()) {
                                item_json["transactionId"] = *item.transaction_id;
                            }
                            if (item.trace_id.has_value() && !item.trace_id->empty()) {
                                item_json["traceId"] = *item.trace_id;
                            }
                            if (item.producer_sub.has_value() && !item.producer_sub->empty()) {
                                item_json["producerSub"] = *item.producer_sub;
                            }
                            items_json.push_back(item_json);
                        }
                        
                    // Store items for failover before submitting
                    // If DB fails, callback will retrieve and write to file buffer
                    if (ctx.push_failover_storage) {
                        ctx.push_failover_storage->store(request_id, items_json.dump());
                    }
                    
                    // Build Queen job request
                    queen::JobRequest job_req;
                    job_req.op_type = queen::JobType::PUSH;
                    job_req.request_id = request_id;
                    job_req.params = {items_json.dump()};
                    job_req.item_count = items.size();
                    
                    // Capture context for callback
                    auto worker_loop = ctx.worker_loop;
                    auto worker_id = ctx.worker_id;
                    auto push_failover_storage = ctx.push_failover_storage;
                    auto file_buffer = ctx.file_buffer;
                    
                    // Submit to per-worker Queen instance - RETURNS IMMEDIATELY!
                    ctx.queen->submit(std::move(job_req), [worker_loop, worker_id, request_id, push_failover_storage, file_buffer, items_json](std::string result) {
                        // Callback runs on Queen's event loop thread
                        // Defer to uWS event loop for safe response delivery
                        worker_loop->defer([result = std::move(result), worker_id, request_id, push_failover_storage, file_buffer, items_json]() {
                            nlohmann::json json_response;
                            int status_code = 201;
                            bool is_error = false;
                            bool db_error_detected = false;
                            std::string db_error_msg;
                            
                            try {
                                json_response = nlohmann::json::parse(result);
                                
                                // Check if the response indicates a database error
                                // Queen returns {"success": false, "error": "..."} on DB failure
                                if (json_response.is_object() && 
                                    json_response.contains("success") && 
                                    json_response["success"].is_boolean() &&
                                    !json_response["success"].get<bool>()) {
                                    db_error_detected = true;
                                    db_error_msg = json_response.value("error", "Database error");
                                    spdlog::warn("[Worker {}] PUSH: Database error detected: {}", worker_id, db_error_msg);
                                }
                                
                            } catch (const std::exception& e) {
                                // Parse failed - also treat as DB error for failover
                                db_error_detected = true;
                                db_error_msg = e.what();
                                spdlog::error("[Worker {}] PUSH result parse failed: {}", worker_id, e.what());
                            }
                            
                            // If database error detected, try file buffer failover
                            if (db_error_detected) {
                                if (push_failover_storage && file_buffer) {
                                    auto items_json_opt = push_failover_storage->retrieve_and_remove(request_id);
                                    if (items_json_opt.has_value()) {
                                        try {
                                            auto items = nlohmann::json::parse(items_json_opt.value());
                                            nlohmann::json results = nlohmann::json::array();
                                            bool all_buffered = true;
                                            
                                            for (const auto& item : items) {
                                                nlohmann::json event = {
                                                    {"queue", item.value("queue", "")},
                                                    {"partition", item.value("partition", "Default")},
                                                    {"payload", item.value("payload", nlohmann::json{})},
                                                    {"failover", true}
                                                };
                                                if (item.contains("transactionId")) {
                                                    event["transactionId"] = item["transactionId"];
                                                } else if (item.contains("messageId")) {
                                                    event["transactionId"] = item["messageId"];
                                                }
                                                if (item.contains("traceId")) {
                                                    event["traceId"] = item["traceId"];
                                                }
                                                if (item.contains("producerSub")) {
                                                    event["producerSub"] = item["producerSub"];
                                                }
                                                
                                                if (file_buffer->write_event(event)) {
                                                    results.push_back({{"status", "buffered"}, {"queue", item.value("queue", "")}});
                                                } else {
                                                    all_buffered = false;
                                                    results.push_back({{"status", "failed"}, {"error", "File buffer write failed"}});
                                                }
                                            }
                                            
                                            spdlog::info("[Worker {}] PUSH: Failed over {} items to file buffer after DB error", 
                                                        worker_id, items.size());
                                            json_response = results;
                                            status_code = all_buffered ? 201 : 500;
                                            is_error = !all_buffered;
                                        } catch (const std::exception& parse_err) {
                                            spdlog::error("[Worker {}] PUSH: Failed to parse failover items: {}", worker_id, parse_err.what());
                                            json_response = {{"error", db_error_msg}};
                                            status_code = 500;
                                            is_error = true;
                                        }
                                    } else {
                                        spdlog::error("[Worker {}] PUSH: No failover data found for request {}", worker_id, request_id);
                                        json_response = {{"error", db_error_msg}};
                                        status_code = 500;
                                        is_error = true;
                                    }
                                } else {
                                    spdlog::error("[Worker {}] PUSH: DB error but no file buffer configured: {}", worker_id, db_error_msg);
                                    json_response = {{"error", db_error_msg}};
                                    status_code = 500;
                                    is_error = true;
                                }
                            } else {
                                // Success path - cleanup failover storage and notify
                                if (push_failover_storage) {
                                    push_failover_storage->remove(request_id);
                                }
                                
                                // Notify other instances via UDP that messages are available
                                // This triggers pop backoff reset on remote workers
                                if (global_shared_state && global_shared_state->is_enabled()) {
                                    // Extract unique queue/partition pairs from the pushed items
                                    // items_json is already a nlohmann::json array
                                    std::set<std::pair<std::string, std::string>> notified;
                                    for (const auto& item : items_json) {
                                        std::string queue = item.value("queue", "");
                                        std::string partition = item.value("partition", "Default");
                                        if (!queue.empty() && notified.insert({queue, partition}).second) {
                                            global_shared_state->notify_message_available(queue, partition);
                                        }
                                    }
                                }
                            }
                            
                            // Deliver response via ResponseRegistry
                            worker_response_registries[worker_id]->send_response(
                                request_id, json_response, is_error, status_code);
                        });
                    });
                    
                } catch (const std::exception& e) {
                    spdlog::error("[Worker {}] PUSH: Error: {}", ctx.worker_id, e.what());
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
