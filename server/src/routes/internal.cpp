#include "queen/routes/route_registry.hpp"
#include "queen/routes/route_context.hpp"
#include "queen/routes/route_helpers.hpp"
#include "queen/shared_state_manager.hpp"
#include <spdlog/spdlog.h>

namespace queen {
namespace routes {

void setup_internal_routes(uWS::App* app, const RouteContext& ctx) {
    // ============================================================
    // HTTP endpoint for peer-to-peer notifications (backward compat)
    // Notifications are now primarily sent via UDP through SharedStateManager,
    // but this HTTP endpoint allows external systems to trigger notifications
    // ============================================================
    app->post("/internal/api/notify", [ctx](auto* res, auto* req) {
        // Check authentication - ADMIN required for internal operations
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::ADMIN);
        
        read_json_body(res,
            [res](const nlohmann::json& body) {
                try {
                    std::string queue = body.value("queue", "");
                    std::string partition = body.value("partition", "");
                    
                    if (queue.empty()) {
                        send_error_response(res, "queue is required", 400);
                        return;
                    }
                    
                    if (global_shared_state) {
                        // All notification types trigger message_available
                        global_shared_state->notify_message_available(queue, partition);
                    }
                    
                    send_json_response(res, {{"status", "ok"}}, 200);
                } catch (const std::exception& e) {
                    send_error_response(res, e.what(), 400);
                }
            },
            [res](const std::string& error) {
                send_error_response(res, error, 400);
            }
        );
    });
    
    // ============================================================
    // Stats endpoint for SharedStateManager (UDPSYNC distributed cache)
    // ============================================================
    app->get("/internal/api/shared-state/stats", [ctx](auto* res, auto* req) {
        // Check authentication - ADMIN required for internal operations
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::ADMIN);
        
        try {
            nlohmann::json stats;
            if (global_shared_state) {
                stats = global_shared_state->get_stats();
            } else {
                stats = {{"enabled", false}, {"reason", "not_initialized"}};
            }
            
            send_json_response(res, stats, 200);
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });
    
    // Legacy alias for backward compatibility
    app->get("/internal/api/inter-instance/stats", [ctx](auto* res, auto* req) {
        // Check authentication - ADMIN required for internal operations
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::ADMIN);
        
        try {
            nlohmann::json stats;
            if (global_shared_state) {
                stats = global_shared_state->get_stats();
            } else {
                stats = {{"enabled", false}, {"reason", "not_initialized"}};
            }
            
            send_json_response(res, stats, 200);
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });
    
    spdlog::debug("Internal routes configured (shared state endpoints)");
}

} // namespace routes
} // namespace queen
