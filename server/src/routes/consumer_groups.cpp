#include "queen/routes/route_registry.hpp"
#include "queen/routes/route_context.hpp"
#include "queen/routes/route_helpers.hpp"
#include "queen/async_queue_manager.hpp"
#include "queen/response_queue.hpp"
#include "queen.hpp"  // libqueen
#include <spdlog/spdlog.h>

// External globals (declared in acceptor_server.cpp)
namespace queen {
extern std::vector<std::shared_ptr<ResponseRegistry>> worker_response_registries;
}

namespace queen {
namespace routes {

// Helper: Submit a stored procedure call via libqueen and handle the response
static void submit_sp_call(
    const RouteContext& ctx,
    uWS::HttpResponse<false>* res,
    const std::string& sql,
    const std::vector<std::string>& params = {}
) {
    std::string request_id = worker_response_registries[ctx.worker_id]->register_response(
        res, ctx.worker_id, nullptr
    );
    
    queen::JobRequest job_req;
    job_req.op_type = queen::JobType::CUSTOM;
    job_req.request_id = request_id;
    job_req.sql = sql;
    job_req.params = params;
    
    auto worker_loop = ctx.worker_loop;
    auto worker_id = ctx.worker_id;
    
    ctx.queen->submit(std::move(job_req), [worker_loop, worker_id, request_id](std::string result) {
        worker_loop->defer([result = std::move(result), worker_id, request_id]() {
            nlohmann::json json_response;
            int status_code = 200;
            bool is_error = false;
            
            try {
                json_response = nlohmann::json::parse(result);
                
                // Check for error in response
                if (json_response.contains("error") && !json_response["error"].is_null()) {
                    is_error = true;
                    status_code = 500;
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
}

void setup_consumer_group_routes(uWS::App* app, const RouteContext& ctx) {
    // POST /api/v1/stats/refresh - Trigger immediate stats refresh (for stale data)
    // Uses force=true to bypass the 5-second debounce check
    app->post("/api/v1/stats/refresh", [ctx](auto* res, auto* req) {
        // Check authentication - ADMIN required for stats refresh
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::ADMIN);
        
        try {
            spdlog::info("[Worker {}] Manual stats refresh triggered (force=true)", ctx.worker_id);
            submit_sp_call(ctx, res, "SELECT queen.refresh_all_stats_v1(true)");
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });
    
    // GET /api/v1/consumer-groups - Consumer groups summary (async via stored procedure)
    // Uses v4 which pre-resolves metadata per (consumer_group, queue) before joining
    // ~10x faster than v3 at scale by eliminating cartesian explosion in metadata join
    // Note: returns approximate time lag and partitionsWithLag instead of exact totalLag count
    app->get("/api/v1/consumer-groups", [ctx](auto* res, auto* req) {
        // Check authentication - READ_ONLY required for viewing consumer groups
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_ONLY);
        
        try {
            submit_sp_call(ctx, res, "SELECT queen.get_consumer_groups_v4()");
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });
    
    // GET /api/v1/consumer-groups/lagging - Get lagging partitions (async via stored procedure)
    app->get("/api/v1/consumer-groups/lagging", [ctx](auto* res, auto* req) {
        // Check authentication - READ_ONLY required for viewing consumer groups
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_ONLY);
        
        try {
            int min_lag_seconds = 3600; // Default: 1 hour
            std::string lag_param = get_query_param(req, "minLagSeconds");
            if (!lag_param.empty()) {
                min_lag_seconds = std::stoi(lag_param);
            }
            submit_sp_call(ctx, res, 
                "SELECT queen.get_lagging_partitions_v1($1::integer)",
                {std::to_string(min_lag_seconds)});
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });
    
    // GET /api/v1/consumer-groups/:group - Consumer group details (async via stored procedure)
    app->get("/api/v1/consumer-groups/:group", [ctx](auto* res, auto* req) {
        // Check authentication - READ_ONLY required for viewing consumer groups
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_ONLY);
        
        try {
            std::string consumer_group = std::string(req->getParameter(0));
            submit_sp_call(ctx, res, 
                "SELECT queen.get_consumer_group_details_v1($1)",
                {consumer_group});
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });
    
    // DELETE /api/v1/consumer-groups/:group - Delete consumer group (async via stored procedure)
    app->del("/api/v1/consumer-groups/:group", [ctx](auto* res, auto* req) {
        // Check authentication - ADMIN required for deleting consumer groups
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::ADMIN);
        
        try {
            std::string consumer_group = std::string(req->getParameter(0));
            bool delete_metadata = get_query_param_bool(req, "deleteMetadata", true);
            
            spdlog::info("[Worker {}] DELETE consumer group: {} (deleteMetadata={})", 
                ctx.worker_id, consumer_group, delete_metadata);
            
            submit_sp_call(ctx, res, 
                "SELECT queen.delete_consumer_group_v1($1, $2::boolean)",
                {consumer_group, delete_metadata ? "true" : "false"});
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });
    
    // POST /api/v1/consumer-groups/:group/subscription - Update subscription timestamp (async)
    app->post("/api/v1/consumer-groups/:group/subscription", [ctx](auto* res, auto* req) {
        // Check authentication - READ_WRITE required for subscription changes
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_WRITE);
        
        std::string consumer_group = std::string(req->getParameter(0));
        
        read_json_body(res,
            [res, ctx, consumer_group](const nlohmann::json& body) {
                try {
                    if (!body.contains("subscriptionTimestamp")) {
                        send_error_response(res, "Missing subscriptionTimestamp in request body", 400);
                        return;
                    }
                    
                    std::string new_timestamp = body["subscriptionTimestamp"].get<std::string>();
                    
                    spdlog::info("[Worker {}] UPDATE consumer group subscription: {} -> {}", 
                        ctx.worker_id, consumer_group, new_timestamp);
                    
                    submit_sp_call(ctx, res, 
                        "SELECT queen.update_consumer_group_subscription_v1($1, $2)",
                        {consumer_group, new_timestamp});
                } catch (const std::exception& e) {
                    send_error_response(res, e.what(), 500);
                }
            },
            [res](const std::string& error) {
                send_error_response(res, error, 400);
            }
        );
    });
    
    // DELETE /api/v1/consumer-groups/:group/queues/:queue - Delete CG for specific queue only
    app->del("/api/v1/consumer-groups/:group/queues/:queue", [ctx](auto* res, auto* req) {
        // Check authentication - ADMIN required for consumer group deletion
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::ADMIN);
        
        try {
            std::string consumer_group = std::string(req->getParameter(0));
            std::string queue_name = std::string(req->getParameter(1));
            bool delete_metadata = get_query_param_bool(req, "deleteMetadata", true);
            
            spdlog::info("[Worker {}] DELETE consumer group for queue: {} / {} (deleteMetadata={})", 
                ctx.worker_id, consumer_group, queue_name, delete_metadata);
            
            submit_sp_call(ctx, res, 
                "SELECT queen.delete_consumer_group_for_queue_v1($1, $2, $3::boolean)",
                {consumer_group, queue_name, delete_metadata ? "true" : "false"});
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });
    
    // POST /api/v1/consumer-groups/:group/queues/:queue/seek - Seek cursor to timestamp or end
    app->post("/api/v1/consumer-groups/:group/queues/:queue/seek", [ctx](auto* res, auto* req) {
        // Check authentication - READ_WRITE required for seek operations
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_WRITE);
        
        std::string consumer_group = std::string(req->getParameter(0));
        std::string queue_name = std::string(req->getParameter(1));
        
        read_json_body(res,
            [res, ctx, consumer_group, queue_name](const nlohmann::json& body) {
                try {
                    bool seek_to_end = body.value("toEnd", false);
                    std::string target_timestamp = body.value("timestamp", "");
                    
                    if (!seek_to_end && target_timestamp.empty()) {
                        send_error_response(res, "Must specify either 'toEnd': true or 'timestamp'", 400);
                        return;
                    }
                    
                    spdlog::info("[Worker {}] SEEK consumer group: {} / {} (toEnd={}, timestamp={})", 
                        ctx.worker_id, consumer_group, queue_name, seek_to_end, target_timestamp);
                    
                    if (seek_to_end) {
                        submit_sp_call(ctx, res, 
                            "SELECT queen.seek_consumer_group_v1($1, $2, NULL, true)",
                            {consumer_group, queue_name});
                    } else {
                        submit_sp_call(ctx, res, 
                            "SELECT queen.seek_consumer_group_v1($1, $2, $3::timestamptz, false)",
                            {consumer_group, queue_name, target_timestamp});
                    }
                } catch (const std::exception& e) {
                    send_error_response(res, e.what(), 500);
                }
            },
            [res](const std::string& error) {
                send_error_response(res, error, 400);
            }
        );
    });
    
    // POST /api/v1/consumer-groups/:group/queues/:queue/partitions/:partition/seek - Seek single partition to end
    app->post("/api/v1/consumer-groups/:group/queues/:queue/partitions/:partition/seek", [ctx](auto* res, auto* req) {
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_WRITE);
        
        try {
            std::string consumer_group = std::string(req->getParameter(0));
            std::string queue_name = std::string(req->getParameter(1));
            std::string partition_name = std::string(req->getParameter(2));
            
            spdlog::info("[Worker {}] SEEK PARTITION: {} / {} / {}", 
                ctx.worker_id, consumer_group, queue_name, partition_name);
            
            submit_sp_call(ctx, res, 
                "SELECT queen.seek_partition_v1($1, $2, $3)",
                {consumer_group, queue_name, partition_name});
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });
}

} // namespace routes
} // namespace queen
