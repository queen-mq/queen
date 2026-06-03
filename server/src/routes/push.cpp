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
#include "simdjson.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <set>
#include <vector>
#include <utility>
#include <cstdlib>
#include <cstdio>
#include <cstring>

namespace queen {

// External globals (declared in acceptor_server.cpp)
extern std::vector<std::shared_ptr<ResponseRegistry>> worker_response_registries;
extern std::shared_ptr<SharedStateManager> global_shared_state;

namespace routes {

namespace {

// Whether the simdjson fast path is enabled (QUEEN_PUSH_SIMD=1). Read once.
bool push_simd_enabled() {
    static const bool enabled = []() {
        const char* e = std::getenv("QUEEN_PUSH_SIMD");
        return e && (std::strcmp(e, "1") == 0 || std::strcmp(e, "true") == 0);
    }();
    return enabled;
}

// Whether raw result pass-through is enabled (QUEEN_PUSH_RAW_RESULT=1). On the
// success path this skips re-parsing + re-serializing the stored-procedure
// result and streams it straight to the client. Read once.
bool push_raw_result_enabled() {
    static const bool enabled = []() {
        const char* e = std::getenv("QUEEN_PUSH_RAW_RESULT");
        return e && (std::strcmp(e, "1") == 0 || std::strcmp(e, "true") == 0);
    }();
    return enabled;
}

// Append `s` as a JSON string literal (with surrounding quotes) to `out`,
// escaping the characters JSON requires. Multi-byte UTF-8 passes through.
void append_json_escaped(std::string& out, std::string_view s) {
    out.push_back('"');
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned int>(static_cast<unsigned char>(c)));
                    out += buf;
                } else {
                    out.push_back(c);
                }
        }
    }
    out.push_back('"');
}

// ----------------------------------------------------------------------------
// Shared submit path (used by both the nlohmann and simdjson builders).
//
// Registers the async response, stores the (already-serialized) items array for
// failover, submits the PUSH job to libqueen, and on completion handles the
// DB-error -> file-buffer failover and the success-path "message available"
// notification. Building the items array exactly once kills the old double
// dump().
// ----------------------------------------------------------------------------
void finish_push_submit(uWS::HttpResponse<false>* res,
                        const RouteContext& ctx,
                        std::string items_json_str,
                        size_t item_count,
                        std::vector<std::pair<std::string, std::string>> notify_pairs) {
    // Register response for async delivery (per-worker registry - no contention!)
    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
        res, ctx.worker_id, nullptr);

    // Store items for failover before submitting. If DB fails, the callback
    // retrieves and writes them to the file buffer.
    if (ctx.push_failover_storage) {
        ctx.push_failover_storage->store(request_id, items_json_str);
    }

    queen::JobRequest job_req;
    job_req.op_type    = queen::JobType::PUSH;
    job_req.request_id = request_id;
    job_req.item_count = item_count;
    job_req.params     = {std::move(items_json_str)};

    auto worker_loop           = ctx.worker_loop;
    auto worker_id             = ctx.worker_id;
    auto push_failover_storage = ctx.push_failover_storage;
    auto file_buffer           = ctx.file_buffer;

    ctx.queen->submit(std::move(job_req),
        [worker_loop, worker_id, request_id, push_failover_storage, file_buffer,
         notify_pairs = std::move(notify_pairs)](std::string result) {
            // Callback runs on Queen's event loop thread; defer to the uWS loop
            // for safe response delivery.
            worker_loop->defer([result = std::move(result), worker_id, request_id,
                                push_failover_storage, file_buffer, notify_pairs]() {
                // Stage 2 fast path: a successful push procedure returns a JSON
                // array (the per-message status). Detect that cheaply (first
                // non-ws char is '[') and stream the result string straight to
                // the client, skipping the nlohmann parse + re-serialize. The
                // DB-error sentinel is an object ('{') and falls through.
                if (push_raw_result_enabled()) {
                    size_t p = result.find_first_not_of(" \t\r\n");
                    if (p != std::string::npos && result[p] == '[') {
                        if (push_failover_storage) {
                            push_failover_storage->remove(request_id);
                        }
                        if (global_shared_state && global_shared_state->is_enabled()) {
                            std::set<std::pair<std::string, std::string>> notified;
                            for (const auto& qp : notify_pairs) {
                                if (!qp.first.empty() && notified.insert(qp).second) {
                                    global_shared_state->notify_message_available(qp.first, qp.second);
                                }
                            }
                        }
                        worker_response_registries[worker_id]->send_response_raw(
                            request_id, std::move(result), 201);
                        return;
                    }
                    // Not a success array (error wrapper / non-array): fall
                    // through to the full nlohmann handling below.
                }

                nlohmann::json json_response;
                int status_code = 201;
                bool is_error = false;
                bool db_error_detected = false;
                std::string db_error_msg;

                try {
                    json_response = nlohmann::json::parse(result);
                    // Queen returns {"success": false, "error": "..."} on DB failure.
                    if (json_response.is_object() &&
                        json_response.contains("success") &&
                        json_response["success"].is_boolean() &&
                        !json_response["success"].get<bool>()) {
                        db_error_detected = true;
                        db_error_msg = json_response.value("error", "Database error");
                        spdlog::warn("[Worker {}] PUSH: Database error detected: {}", worker_id, db_error_msg);
                    }
                } catch (const std::exception& e) {
                    db_error_detected = true;
                    db_error_msg = e.what();
                    spdlog::error("[Worker {}] PUSH result parse failed: {}", worker_id, e.what());
                }

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
                    // Success path - cleanup failover storage and notify.
                    if (push_failover_storage) {
                        push_failover_storage->remove(request_id);
                    }
                    if (global_shared_state && global_shared_state->is_enabled()) {
                        std::set<std::pair<std::string, std::string>> notified;
                        for (const auto& qp : notify_pairs) {
                            if (!qp.first.empty() && notified.insert(qp).second) {
                                global_shared_state->notify_message_available(qp.first, qp.second);
                            }
                        }
                    }
                }

                worker_response_registries[worker_id]->send_response(
                    request_id, json_response, is_error, status_code);
            });
        });
}

// ----------------------------------------------------------------------------
// Legacy nlohmann path: parse -> validate -> build items array -> submit.
// Also owns the maintenance-mode (file-buffer) branch, which the simdjson path
// falls back to.
// ----------------------------------------------------------------------------
void handle_push_json(uWS::HttpResponse<false>* res,
                      const RouteContext& ctx,
                      const std::optional<auth::JwtClaims>& auth_claims,
                      const nlohmann::json& body) {
    try {
        if (!body.contains("items") || !body["items"].is_array()) {
            send_error_response(res, "items array is required", 400);
            return;
        }

        spdlog::debug("[Worker {}] PUSH: Processing {} items", ctx.worker_id, body["items"].size());

        std::vector<PushItem> items;
        for (const auto& item_json : body["items"]) {
            if (item_json.contains("partition") && !item_json["partition"].is_string()) {
                send_error_response(res, "Partition must be a string", 400);
                return;
            }
            if (!item_json.contains("queue") || !item_json["queue"].is_string()) {
                send_error_response(res, "Queue must be a string", 400);
                return;
            }
            if (item_json.contains("transactionId") && !item_json["transactionId"].is_string()) {
                send_error_response(res, "TransactionId must be a string", 400);
                return;
            }
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
            // JWT subject; any "producerSub" in the client body is ignored.
            if (auth_claims.has_value() && !auth_claims->subject.empty()) {
                item.producer_sub = auth_claims->subject;
            }
            items.push_back(std::move(item));
        }

        if (items.empty()) {
            send_json_response(res, nlohmann::json::array(), 201);
            return;
        }

        // MAINTENANCE MODE: route to file buffer instead of the sidecar.
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

        // Build the items array for the stored procedure (single dump in
        // finish_push_submit) and collect queue/partition notify pairs.
        EncryptionService* enc_service = get_encryption_service();
        nlohmann::json items_json = nlohmann::json::array();
        std::vector<std::pair<std::string, std::string>> notify_pairs;
        notify_pairs.reserve(items.size());

        for (const auto& item : items) {
            bool queue_encryption_enabled = false;
            if (global_shared_state) {
                auto config = global_shared_state->get_or_fetch_queue_config(item.queue);
                if (config) {
                    queue_encryption_enabled = config->encryption_enabled;
                }
            } else {
                spdlog::warn("[Worker {}] PUSH: global_shared_state is NULL!", ctx.worker_id);
            }

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
            items_json.push_back(std::move(item_json));
            notify_pairs.emplace_back(item.queue, item.partition);
        }

        finish_push_submit(res, ctx, items_json.dump(), items.size(), std::move(notify_pairs));

    } catch (const std::exception& e) {
        spdlog::error("[Worker {}] PUSH: Error: {}", ctx.worker_id, e.what());
        send_error_response(res, e.what(), 500);
    }
}

// ----------------------------------------------------------------------------
// simdjson fast path: parse the envelope, pass each payload through RAW (zero
// re-serialization), build the stored-procedure argument string in a single
// pass, and submit. Returns the item count, or -1 on a validation error (with
// `error` set for a 400 response).
// ----------------------------------------------------------------------------
int build_push_items_simd(std::string& body,
                          const std::string& auth_sub,
                          const RouteContext& ctx,
                          std::string& items_str,
                          std::vector<std::pair<std::string, std::string>>& notify_pairs,
                          std::string& error) {
    // One On-Demand parser per worker thread (stateful, not thread-safe).
    thread_local simdjson::ondemand::parser parser;

    EncryptionService* enc_service = get_encryption_service();

    items_str.clear();
    items_str.reserve(body.size() + 1024);
    items_str.push_back('[');

    int count = 0;
    try {
        simdjson::padded_string_view padded(body.data(), body.size(), body.capacity());
        simdjson::ondemand::document doc;
        if (parser.iterate(padded).get(doc)) {
            error = "Invalid JSON";
            return -1;
        }
        simdjson::ondemand::object root;
        if (doc.get_object().get(root)) {
            error = "items array is required";
            return -1;
        }
        simdjson::ondemand::array items;
        if (root["items"].get_array().get(items)) {
            error = "items array is required";
            return -1;
        }

        for (auto item_result : items) {
            simdjson::ondemand::object item;
            if (item_result.get_object().get(item)) {
                error = "items must be objects";
                return -1;
            }

            std::string queue, partition = "Default", transaction_id, trace_id;
            std::string_view payload_raw;
            bool has_queue = false, has_payload = false, has_txid = false, has_traceid = false;

            for (auto field : item) {
                std::string_view key;
                if (field.unescaped_key().get(key)) continue;

                if (key == "queue") {
                    std::string_view v;
                    if (field.value().get_string().get(v)) { error = "Queue must be a string"; return -1; }
                    queue.assign(v); has_queue = true;
                } else if (key == "partition") {
                    std::string_view v;
                    if (field.value().get_string().get(v)) { error = "Partition must be a string"; return -1; }
                    partition.assign(v);
                } else if (key == "transactionId") {
                    std::string_view v;
                    if (field.value().get_string().get(v)) { error = "TransactionId must be a string"; return -1; }
                    transaction_id.assign(v); has_txid = true;
                } else if (key == "traceId") {
                    std::string_view v;
                    if (field.value().get_string().get(v)) { error = "TraceId must be a string"; return -1; }
                    trace_id.assign(v); has_traceid = true;
                } else if (key == "payload") {
                    simdjson::ondemand::value pv;
                    if (field.value().get(pv)) { error = "Invalid payload JSON"; return -1; }
                    if (pv.raw_json().get(payload_raw)) { error = "Invalid payload JSON"; return -1; }
                    has_payload = true;
                }
                // Any other field (incl. client-supplied "producerSub") is ignored
                // and auto-skipped by the On-Demand iterator.
            }

            if (!has_queue) {
                error = "Queue must be a string";
                return -1;
            }

            // Encryption: encrypt the RAW payload slice directly (no dump needed).
            bool is_encrypted = false;
            EncryptionService::EncryptedData enc;
            bool queue_encryption_enabled = false;
            if (global_shared_state) {
                auto config = global_shared_state->get_or_fetch_queue_config(queue);
                if (config) queue_encryption_enabled = config->encryption_enabled;
            }
            if (queue_encryption_enabled && enc_service && enc_service->is_enabled()) {
                std::string payload_str(has_payload ? payload_raw : std::string_view("{}"));
                auto encrypted = enc_service->encrypt_payload(payload_str);
                if (encrypted.has_value()) {
                    enc = *encrypted;
                    is_encrypted = true;
                } else {
                    spdlog::warn("[Worker {}] PUSH(simd): Encryption failed for queue '{}', storing plaintext",
                                 ctx.worker_id, queue);
                }
            }

            if (count > 0) items_str.push_back(',');
            ++count;

            items_str += "{\"queue\":";
            append_json_escaped(items_str, queue);
            items_str += ",\"partition\":";
            append_json_escaped(items_str, partition);
            items_str += ",\"payload\":";
            if (is_encrypted) {
                items_str += "{\"encrypted\":";
                append_json_escaped(items_str, enc.encrypted);
                items_str += ",\"iv\":";
                append_json_escaped(items_str, enc.iv);
                items_str += ",\"authTag\":";
                append_json_escaped(items_str, enc.auth_tag);
                items_str += "}";
            } else if (has_payload) {
                items_str.append(payload_raw.data(), payload_raw.size());  // RAW pass-through
            } else {
                items_str += "{}";
            }
            items_str += ",\"is_encrypted\":";
            items_str += is_encrypted ? "true" : "false";
            items_str += ",\"messageId\":";
            append_json_escaped(items_str, ctx.async_queue_manager->generate_uuid());
            if (has_txid && !transaction_id.empty()) {
                items_str += ",\"transactionId\":";
                append_json_escaped(items_str, transaction_id);
            }
            if (has_traceid && !trace_id.empty()) {
                items_str += ",\"traceId\":";
                append_json_escaped(items_str, trace_id);
            }
            if (!auth_sub.empty()) {
                items_str += ",\"producerSub\":";
                append_json_escaped(items_str, auth_sub);
            }
            items_str += "}";

            notify_pairs.emplace_back(std::move(queue), std::move(partition));
        }
    } catch (const std::exception& e) {
        error = std::string("Invalid JSON: ") + e.what();
        return -1;
    }

    items_str.push_back(']');
    return count;
}

void handle_push_simd(uWS::HttpResponse<false>* res,
                      const RouteContext& ctx,
                      const std::optional<auth::JwtClaims>& auth_claims,
                      std::string& body) {
    // Maintenance mode needs the full per-item handling (file-buffer events);
    // fall back to the nlohmann path for that rare case.
    if (global_shared_state && global_shared_state->get_maintenance_mode() && ctx.file_buffer) {
        try {
            nlohmann::json parsed = nlohmann::json::parse(body);
            handle_push_json(res, ctx, auth_claims, parsed);
        } catch (const std::exception& e) {
            send_error_response(res, std::string("Invalid JSON: ") + e.what(), 400);
        }
        return;
    }

    std::string auth_sub;
    if (auth_claims.has_value() && !auth_claims->subject.empty()) {
        auth_sub = auth_claims->subject;
    }

    std::string items_str;
    std::vector<std::pair<std::string, std::string>> notify_pairs;
    std::string error;
    int count = build_push_items_simd(body, auth_sub, ctx, items_str, notify_pairs, error);

    if (count < 0) {
        send_error_response(res, error, 400);
        return;
    }
    if (count == 0) {
        send_json_response(res, nlohmann::json::array(), 201);
        return;
    }

    finish_push_submit(res, ctx, std::move(items_str), static_cast<size_t>(count), std::move(notify_pairs));
}

} // namespace

void setup_push_routes(uWS::App* app, const RouteContext& ctx) {
    app->post("/api/v1/push", [ctx](auto* res, auto* req) {
        // WRITE_ONLY required for push: produce-only clients are allowed,
        // read-only is rejected. Claims are captured so the server can stamp the
        // authenticated producer 'sub' onto each message (anti-impersonation).
        std::optional<auth::JwtClaims> auth_claims;
        REQUIRE_AUTH_WITH_CLAIMS(res, req, ctx, auth::AccessLevel::WRITE_ONLY, auth_claims);

        if (push_simd_enabled()) {
            read_body_raw(res,
                [res, ctx, auth_claims = std::move(auth_claims)](std::string& body) {
                    handle_push_simd(res, ctx, auth_claims, body);
                },
                [res](const std::string& error) {
                    send_error_response(res, error, 400);
                });
        } else {
            read_json_body(res,
                [res, ctx, auth_claims = std::move(auth_claims)](const nlohmann::json& body) {
                    handle_push_json(res, ctx, auth_claims, body);
                },
                [res](const std::string& error) {
                    send_error_response(res, error, 400);
                });
        }
    });
}

} // namespace routes
} // namespace queen
