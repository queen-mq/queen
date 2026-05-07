// ============================================================================
// POST /streams/v1/queries
// ============================================================================
//
// Idempotently register a streaming query. Called by the SDK once at the
// start of `Stream.run({ queryId })` to:
//   * obtain the internal query UUID for subsequent /streams/v1/cycle calls
//   * validate config_hash against any prior registration of the same name
//
// Request body (JSON):
//   {
//     "name":         "orders.per_customer_per_min",   // user-facing queryId
//     "source_queue": "orders",
//     "sink_queue":   "orders.totals_per_customer_per_min",   // optional
//     "config_hash":  "<sha-of-operator-chain>",
//     "reset":        false                            // optional
//   }
//
// Response body (JSON):
//   {
//     "success":     true,
//     "query_id":    "<uuid>",
//     "name":        "orders.per_customer_per_min",
//     "config_hash": "...",
//     "fresh":       false,
//     "reset":       false
//   }
//
// On config_hash mismatch (without reset:true), 409 Conflict with details so
// the SDK can surface a clear error to the user.
//
// Implementation: builds a single-element JobRequest array (idx=0) and
// submits to libqueen as JobType::STREAMS_REGISTER_QUERY. libqueen's drain
// orchestrator dispatches queen.streams_register_query_v1 over its async-PG
// pipeline; the response array is unwrapped to a single object before being
// sent back to the client.
// ============================================================================

#include "queen/routes/route_registry.hpp"
#include "queen/routes/route_context.hpp"
#include "queen/routes/route_helpers.hpp"
#include "queen.hpp"  // libqueen
#include "queen/response_queue.hpp"
#include <spdlog/spdlog.h>

namespace queen {
extern std::vector<std::shared_ptr<ResponseRegistry>> worker_response_registries;
}

namespace queen {
namespace routes {

void setup_streams_register_query_routes(uWS::App* app, const RouteContext& ctx) {
    app->post("/streams/v1/queries", [ctx](auto* res, auto* req) {
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_WRITE);

        read_json_body(res,
            [res, ctx](const nlohmann::json& body) {
                try {
                    if (!body.is_object()) {
                        send_error_response(res, "request body must be a JSON object", 400);
                        return;
                    }
                    if (!body.contains("name") || !body["name"].is_string() || body["name"].get<std::string>().empty()) {
                        send_error_response(res, "name is required", 400);
                        return;
                    }
                    if (!body.contains("source_queue") || !body["source_queue"].is_string() || body["source_queue"].get<std::string>().empty()) {
                        send_error_response(res, "source_queue is required", 400);
                        return;
                    }
                    if (!body.contains("config_hash") || !body["config_hash"].is_string() || body["config_hash"].get<std::string>().empty()) {
                        send_error_response(res, "config_hash is required", 400);
                        return;
                    }

                    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
                        res, ctx.worker_id, nullptr
                    );

                    // Wrap the single registration in a one-element array so
                    // libqueen's _fire_batched parses it as the SP expects.
                    nlohmann::json request_item = body;
                    request_item["idx"] = 0;
                    nlohmann::json requests_array = nlohmann::json::array();
                    requests_array.push_back(std::move(request_item));

                    queen::JobRequest job_req;
                    job_req.op_type    = queen::JobType::STREAMS_REGISTER_QUERY;
                    job_req.request_id = request_id;
                    job_req.params     = {requests_array.dump()};
                    job_req.item_count = 1;

                    auto worker_loop = ctx.worker_loop;
                    auto worker_id   = ctx.worker_id;

                    ctx.queen->submit(std::move(job_req),
                        [worker_loop, worker_id, request_id](std::string result) {
                            worker_loop->defer([result = std::move(result), worker_id, request_id]() {
                                nlohmann::json json_response;
                                int status_code = 200;
                                bool is_error = false;

                                try {
                                    auto parsed = nlohmann::json::parse(result);
                                    // Default callback dispatch returns the array
                                    // of result entries for this job's idx range.
                                    // For a single-item register call, expect one
                                    // element; unwrap it to {success, query_id, ...}.
                                    if (parsed.is_array() && !parsed.empty() && parsed[0].contains("result")) {
                                        json_response = parsed[0]["result"];
                                        if (json_response.is_object()
                                            && json_response.value("success", true) == false) {
                                            // 409 Conflict for hash mismatch is the
                                            // canonical signal to the SDK; SDK surfaces
                                            // it as a thrown exception with the inner
                                            // error string.
                                            status_code = 409;
                                            is_error = true;
                                        }
                                    } else if (parsed.is_object() && parsed.contains("error")) {
                                        json_response = parsed;
                                        status_code = 500;
                                        is_error = true;
                                    } else {
                                        json_response = parsed;
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
            },
            [res](const std::string& error) {
                send_error_response(res, error, 400);
            }
        );
    });
}

} // namespace routes
} // namespace queen
