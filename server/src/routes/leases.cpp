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
