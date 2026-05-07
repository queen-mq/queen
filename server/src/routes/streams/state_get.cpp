// ============================================================================
// POST /streams/v1/state/get
// ============================================================================
//
// Read state rows for a (query_id, partition_id, keys[]) shard. Used by
// stateful operators at cycle start to load the state they need to apply
// the next batch.
//
// Request body (JSON):
//   {
//     "query_id":     "<uuid>",
//     "partition_id": "<uuid>",
//     "keys":         ["window:2026-05-06T10:01", ...]    // optional; empty/missing = all
//   }
//
// Response body (JSON):
//   {
//     "success": true,
//     "rows":    [
//        {"key": "window:...", "value": {...}, "updated_at": "..."}
//     ]
//   }
//
// Implementation: builds a single-element JobRequest array (idx=0) and
// submits as JobType::STREAMS_STATE_GET. This JobType is BATCHABLE in
// libqueen — multiple concurrent state_get calls (e.g., from many workers
// each starting a cycle) are merged into one drain pass and one
// streams_state_get_v1 SP call. The route unwraps the per-idx result back
// into the single-call response shape.
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

void setup_streams_state_get_routes(uWS::App* app, const RouteContext& ctx) {
    app->post("/streams/v1/state/get", [ctx](auto* res, auto* req) {
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_ONLY);

        read_json_body(res,
            [res, ctx](const nlohmann::json& body) {
                try {
                    if (!body.is_object()) {
                        send_error_response(res, "request body must be a JSON object", 400);
                        return;
                    }
                    if (!body.contains("query_id") || !body["query_id"].is_string()) {
                        send_error_response(res, "query_id is required", 400);
                        return;
                    }
                    if (!body.contains("partition_id") || !body["partition_id"].is_string()) {
                        send_error_response(res, "partition_id is required", 400);
                        return;
                    }

                    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
                        res, ctx.worker_id, nullptr
                    );

                    nlohmann::json request_item = body;
                    request_item["idx"] = 0;
                    if (!request_item.contains("keys") || !request_item["keys"].is_array()) {
                        request_item["keys"] = nlohmann::json::array();
                    }
                    nlohmann::json requests_array = nlohmann::json::array();
                    requests_array.push_back(std::move(request_item));

                    queen::JobRequest job_req;
                    job_req.op_type    = queen::JobType::STREAMS_STATE_GET;
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
                                    if (parsed.is_array() && !parsed.empty() && parsed[0].contains("result")) {
                                        json_response = parsed[0]["result"];
                                        if (json_response.is_object()
                                            && json_response.value("success", true) == false) {
                                            status_code = 400;
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
