#include "queen/routes/route_registry.hpp"
#include "queen/routes/route_context.hpp"
#include "queen/routes/route_helpers.hpp"
#include "queen/async_queue_manager.hpp"
#include "queen.hpp"  // libqueen
#include "queen/response_queue.hpp"
#include <spdlog/spdlog.h>

// External globals
namespace queen {
extern std::vector<std::shared_ptr<ResponseRegistry>> worker_response_registries;
}

namespace queen {
namespace routes {

namespace {
// Merge state for the dual-engine lease renewal (v1 queen.renew_lease_v2 +
// v2 queen.seg_renew_lease_v1). Namespace scope: local structs inside the generic
// route lambda would make every touching expression template-dependent.
struct RenewMergeState {
    int pending = 2;
    std::string v1_raw;
    std::string v2_raw;
};
}  // namespace

void setup_lease_routes(uWS::App* app, const RouteContext& ctx) {
    // Lease extension
    app->post("/api/v1/lease/:leaseId/extend", [ctx](auto* res, auto* req) {
        // Check authentication - READ_WRITE required for lease operations
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_WRITE);
        
        // CRITICAL: Extract URL parameters BEFORE read_json_body.
        // In uWebSockets, HttpRequest* req is only valid during the synchronous
        // scope of the route handler. Once read_json_body sets up the async onData
        // callback and returns, req is recycled. Capturing req in the body callback
        // causes use-after-free (SIGSEGV).
        std::string lease_id = std::string(req->getParameter(0));
        
        read_json_body(res,
            [res, ctx, lease_id](const nlohmann::json& body) {
                try {
                    int seconds = body.value("seconds", 60);
                    
                    spdlog::debug("[Worker {}] Extending lease: {}, seconds: {}", ctx.worker_id, lease_id, seconds);
                    
                    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
                        res, ctx.worker_id, nullptr
                    );
                    
                    // Build JSON array with single renewal
                    nlohmann::json items_json = nlohmann::json::array();
                    items_json.push_back({
                        {"leaseId", lease_id},
                        {"extendSeconds", seconds}
                    });
                    
                    // Build Queen job request
                    queen::JobRequest job_req;
                    job_req.op_type = queen::JobType::RENEW_LEASE;
                    job_req.request_id = request_id;
                    job_req.params = {items_json.dump()};
                    job_req.item_count = 1;

                    // Capture context for callback
                    auto worker_loop = ctx.worker_loop;
                    auto worker_id = ctx.worker_id;

                    // STORAGE V2: a leaseId is either a v1 lease
                    // (queen.partition_consumers.worker_id) or a v2 one
                    // (queen.seg_consumers.worker_id) — the route can't tell which, so
                    // it renews BOTH engines and merges: success if either
                    // renewed. The non-owning engine's UPDATE matches 0 rows
                    // (queen.seg_consumers stays O(partitions x groups), so the
                    // unindexed worker_id probe is cheap). Response shape is
                    // unchanged: v1's array of {index, leaseId, success, error,
                    // expiresAt}; expiresAt is the later of the two if both
                    // renewed. Both callbacks defer onto this worker's loop, so
                    // the merge state needs no locking.
                    auto merge_st = std::make_shared<RenewMergeState>();

                    auto finish_merge = [worker_id, request_id, lease_id, merge_st]() {
                        nlohmann::json json_response;
                        int status_code = 200;
                        bool is_error = false;
                        // Guard the whole merge: an nlohmann type error escaping
                        // a deferred callback would unwind the worker thread.
                        try {

                        // v2 leg: queen.seg_renew_lease_v1 -> {renewed, expiresAt}.
                        bool v2_ok = false;
                        std::string v2_expires;
                        try {
                            auto v2 = nlohmann::json::parse(merge_st->v2_raw);
                            if (v2.is_object() && v2.value("renewed", 0) > 0) {
                                v2_ok = true;
                                v2_expires = v2.value("expiresAt", "");
                            }
                        } catch (...) { /* treated as not renewed */ }

                        // v1 leg: queen.renew_lease_v2 -> array of results (or
                        // {"success":false,"error":...} on DB failure).
                        try {
                            auto v1 = nlohmann::json::parse(merge_st->v1_raw);
                            if (v1.is_array() && !v1.empty() && v1[0].is_object()) {
                                json_response = std::move(v1);
                            } else {
                                // Keep the wire shape with a v1-style entry.
                                std::string err = "Lease not found";
                                if (v1.is_object() && v1.contains("error") && v1["error"].is_string()) {
                                    err = v1["error"].get<std::string>();
                                }
                                json_response = nlohmann::json::array();
                                json_response.push_back({
                                    {"index", 0}, {"leaseId", lease_id},
                                    {"success", false}, {"error", err},
                                    {"expiresAt", nullptr}});
                            }
                        } catch (const std::exception& e) {
                            if (!v2_ok) {
                                // Both legs unusable: preserve the old 500 path.
                                json_response = {{"error", e.what()}};
                                status_code = 500;
                                is_error = true;
                            } else {
                                json_response = nlohmann::json::array();
                                json_response.push_back({
                                    {"index", 0}, {"leaseId", lease_id},
                                    {"success", false}, {"error", nullptr},
                                    {"expiresAt", nullptr}});
                            }
                        }

                        if (v2_ok && json_response.is_array() && !json_response.empty()) {
                            auto& entry = json_response[0];
                            bool v1_ok = entry.contains("success")
                                && entry["success"].is_boolean() && entry["success"].get<bool>();
                            std::string v1_expires =
                                (entry.contains("expiresAt") && entry["expiresAt"].is_string())
                                    ? entry["expiresAt"].get<std::string>() : "";
                            entry["success"] = true;
                            entry["error"] = nullptr;
                            // ISO-8601 UTC strings compare chronologically.
                            entry["expiresAt"] = (v1_ok && v1_expires > v2_expires)
                                ? v1_expires : v2_expires;
                        }
                        } catch (const std::exception& e) {
                            json_response = {{"error", e.what()}};
                            status_code = 500;
                            is_error = true;
                        }

                        worker_response_registries[worker_id]->send_response(
                            request_id, json_response, is_error, status_code);
                    };

                    ctx.queen->submit(std::move(job_req),
                        [worker_loop, merge_st, finish_merge](std::string result) {
                            worker_loop->defer([result = std::move(result), merge_st, finish_merge]() {
                                merge_st->v1_raw = std::move(result);
                                if (--merge_st->pending == 0) finish_merge();
                            });
                        });

                    queen::JobRequest v2_job;
                    v2_job.op_type = queen::JobType::CUSTOM;
                    v2_job.request_id = request_id;
                    v2_job.sql = "SELECT queen.seg_renew_lease_v1($1, $2::int)";
                    v2_job.params = {lease_id, std::to_string(seconds)};

                    ctx.queen->submit(std::move(v2_job),
                        [worker_loop, merge_st, finish_merge](std::string result) {
                            worker_loop->defer([result = std::move(result), merge_st, finish_merge]() {
                                merge_st->v2_raw = std::move(result);
                                if (--merge_st->pending == 0) finish_merge();
                            });
                        });
                    
                    spdlog::debug("[Worker {}] RENEW_LEASE: Submitted (request_id={})", 
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
