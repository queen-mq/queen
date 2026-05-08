// ============================================================================
// POST /streams/v1/cycle
// ============================================================================
//
// Atomically commit one streaming cycle for a partition: state mutations,
// sink-queue push, and source ack — all in one PG transaction via
// queen.streams_cycle_v1.
//
// Request body (JSON):
//   {
//     "query_id":     "<uuid>",
//     "partition_id": "<source-partition-uuid>",
//     "consumer_group": "streams.<queryId>",         // optional, defaults to __QUEUE_MODE__
//     "state_ops":  [
//        {"type":"upsert","key":"window:..","value":{...}},
//        {"type":"delete","key":"window:.."}
//     ],
//     "push_items": [                                // optional, sink emissions
//        {"queue":"...","partition":"...","payload":{...},"transactionId":"..."}
//     ],
//     "ack": {                                       // optional, source ack
//        "transactionId":"...","leaseId":"...","status":"completed"
//     }
//   }
//
// Response body (JSON):
//   {
//     "success":           true,
//     "query_id":          "...",
//     "partition_id":      "...",
//     "state_ops_applied": 1,
//     "push_results":      [...],   // from push_messages_v3
//     "ack_result":        {success: true, lease_released: true, dlq: false}
//   }
//
// Implementation: builds a single-element JobRequest array (idx=0) and
// submits as JobType::STREAMS_CYCLE. The SP processes the cycle atomically
// and returns an array result keyed by idx; the route unwraps to a single
// response object.
//
// Atomicity guarantee
// -------------------
// streams_cycle_v1 wraps state_ops + push_items + ack in a single PG
// transaction. push_messages_v3 is composed inline so partition_lookup is
// updated for sink-queue partitions (libqueen's auto-refresh path is gated
// on JobType::PUSH; STREAMS_CYCLE does it itself).
// ============================================================================

#include "queen/routes/route_registry.hpp"
#include "queen/routes/route_context.hpp"
#include "queen/routes/route_helpers.hpp"
#include "queen/async_queue_manager.hpp"   // for ctx.async_queue_manager->generate_uuid()
#include "queen.hpp"  // libqueen
#include "queen/response_queue.hpp"
#include <spdlog/spdlog.h>

namespace queen {
extern std::vector<std::shared_ptr<ResponseRegistry>> worker_response_registries;
}

namespace queen {
namespace routes {

void setup_streams_cycle_routes(uWS::App* app, const RouteContext& ctx) {
    app->post("/streams/v1/cycle", [ctx](auto* res, auto* req) {
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_WRITE);

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

                    // Stamp a UUIDv7 messageId on every sink push item that
                    // doesn't already have one. Mirrors what /api/v1/push
                    // (push.cpp ~line 211) does for normal HTTP pushes.
                    //
                    // Why here and not in the SP: the cycle's sink push goes
                    // through queen.push_messages_v3 which falls back to PG's
                    // gen_random_uuid() (UUIDv4 — random) when messageId is
                    // absent. UUIDv4 ids tie-break (created_at, id) ordering
                    // randomly when many messages in the same cycle share the
                    // same created_at (PG's transaction_timestamp). UUIDv7
                    // generation here uses async_queue_manager->generate_uuid(),
                    // which has a per-ms sequence counter, so messageIds are
                    // strictly monotonic across calls inside one cycle AND
                    // across concurrent cycles on the same worker. The pop's
                    // ORDER BY (created_at, id) then preserves the SinkOperator's
                    // emit order, which is the source partition FIFO order.
                    //
                    // Without this, .gate()-style streams (rate limiters) would
                    // see correctly-ordered allows on the source side but
                    // randomly-ordered arrivals on the sink side.
                    if (request_item.contains("push_items")
                        && request_item["push_items"].is_array()) {
                        for (auto& pi : request_item["push_items"]) {
                            if (pi.is_object()
                                && (!pi.contains("messageId")
                                    || pi["messageId"].is_null()
                                    || (pi["messageId"].is_string()
                                        && pi["messageId"].get<std::string>().empty()))) {
                                pi["messageId"] = ctx.async_queue_manager->generate_uuid();
                            }
                        }
                    }

                    nlohmann::json requests_array = nlohmann::json::array();
                    requests_array.push_back(std::move(request_item));

                    // item_count counts the ops the SP will perform: state_ops +
                    // push_items + (1 if ack else 0). This feeds libqueen's
                    // metrics/observability without affecting dispatch.
                    size_t op_count = 0;
                    if (body.contains("state_ops") && body["state_ops"].is_array()) {
                        op_count += body["state_ops"].size();
                    }
                    if (body.contains("push_items") && body["push_items"].is_array()) {
                        op_count += body["push_items"].size();
                    }
                    if (body.contains("ack") && !body["ack"].is_null()) {
                        op_count += 1;
                    }
                    if (op_count == 0) op_count = 1;

                    queen::JobRequest job_req;
                    job_req.op_type    = queen::JobType::STREAMS_CYCLE;
                    job_req.request_id = request_id;
                    job_req.params     = {requests_array.dump()};
                    job_req.item_count = op_count;

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
                                            // Cycle failures are 200 (the SDK
                                            // inspects success/error and decides
                                            // whether to retry). Use 500 only for
                                            // protocol-level breakage below.
                                            status_code = 200;
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
