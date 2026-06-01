#include "queen/routes/route_registry.hpp"
#include "queen/routes/route_context.hpp"
#include "queen/routes/route_helpers.hpp"
#include "queen/async_queue_manager.hpp"
#include "queen/shared_state_manager.hpp"
#include <spdlog/spdlog.h>

// External reference to global shared state manager
extern std::shared_ptr<queen::SharedStateManager> global_shared_state;

namespace queen {
namespace routes {

void setup_maintenance_routes(uWS::App* app, const RouteContext& ctx) {
    // GET maintenance mode status (always fetch fresh from DB)
    app->get("/api/v1/system/maintenance", [ctx](auto* res, auto* req) {
        // Check authentication - ADMIN required for system operations
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::ADMIN);
        
        try {
            // Force fresh check from database (bypass cache for status endpoint)
            bool current_mode = ctx.async_queue_manager->get_maintenance_mode_fresh();
            auto buffer_stats = ctx.async_queue_manager->get_buffer_stats();
            
            // Also get pop maintenance mode status
            bool pop_mode = false;
            if (global_shared_state) {
                pop_mode = global_shared_state->get_pop_maintenance_mode();
            }
            
            nlohmann::json response = {
                {"maintenanceMode", current_mode},
                {"popMaintenanceMode", pop_mode},
                {"bufferedMessages", ctx.async_queue_manager->get_buffer_pending_count()},
                {"bufferHealthy", ctx.async_queue_manager->is_buffer_healthy()},
                {"bufferStats", buffer_stats}
            };
            send_json_response(res, response);
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });
    
    // POST toggle maintenance mode
    // Uses SharedStateManager for cluster-wide propagation via UDP broadcast
    app->post("/api/v1/system/maintenance", [ctx](auto* res, auto* req) {
        // Check authentication - ADMIN required for system operations
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::ADMIN);
        
        read_json_body(res,
            [res, ctx](const nlohmann::json& body) {
                try {
                    if (!body.contains("enabled") || !body["enabled"].is_boolean()) {
                        send_error_response(res, "enabled (boolean) is required", 400);
                        return;
                    }
                    
                    bool enable = body["enabled"];
                    
                    // Use SharedStateManager for cluster-wide propagation
                    // This will: 1) update local atomic cache, 2) persist to DB, 3) broadcast to peers
                    if (global_shared_state) {
                        global_shared_state->set_maintenance_mode(enable);
                    } else {
                        // Fallback to AsyncQueueManager if SharedStateManager not available
                        ctx.async_queue_manager->set_maintenance_mode(enable);
                    }
                    
                    nlohmann::json response = {
                        {"maintenanceMode", enable},
                        {"bufferedMessages", ctx.async_queue_manager->get_buffer_pending_count()},
                        {"bufferHealthy", ctx.async_queue_manager->is_buffer_healthy()},
                        {"message", enable ? 
                            "Maintenance mode ENABLED. All PUSHes routing to file buffer." :
                            "Maintenance mode DISABLED. Background processor will drain buffer to DB."
                        }
                    };
                    
                    send_json_response(res, response);
                    
                } catch (const std::exception& e) {
                    send_error_response(res, e.what(), 500);
                }
            },
            [res](const std::string& error) {
                send_error_response(res, error, 400);
            }
        );
    });
    
    // ============================================================
    // Pop Maintenance Mode (consumers receive empty arrays)
    // ============================================================
    
    // GET pop maintenance mode status (always fetch fresh from DB)
    app->get("/api/v1/system/maintenance/pop", [ctx](auto* res, auto* req) {
        // Check authentication - ADMIN required for system operations
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::ADMIN);
        
        try {
            bool current_mode = false;
            if (global_shared_state) {
                current_mode = global_shared_state->get_pop_maintenance_mode();
            }
            
            nlohmann::json response = {
                {"popMaintenanceMode", current_mode},
                {"message", current_mode ? 
                    "Pop maintenance mode is ON. All POP operations return empty arrays." :
                    "Pop maintenance mode is OFF. Normal operation."
                }
            };
            send_json_response(res, response);
        } catch (const std::exception& e) {
            send_error_response(res, e.what(), 500);
        }
    });
    
    // POST toggle pop maintenance mode
    // Uses SharedStateManager for cluster-wide propagation via UDP broadcast
    app->post("/api/v1/system/maintenance/pop", [ctx](auto* res, auto* req) {
        // Check authentication - ADMIN required for system operations
        REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::ADMIN);
        
        read_json_body(res,
            [res, ctx](const nlohmann::json& body) {
                try {
                    if (!body.contains("enabled") || !body["enabled"].is_boolean()) {
                        send_error_response(res, "enabled (boolean) is required", 400);
                        return;
                    }
                    
                    bool enable = body["enabled"];
                    
                    // Use SharedStateManager for cluster-wide propagation
                    // This will: 1) update local atomic cache, 2) persist to DB, 3) broadcast to peers
                    if (global_shared_state) {
                        global_shared_state->set_pop_maintenance_mode(enable);
                    } else {
                        send_error_response(res, "SharedStateManager not available", 500);
                        return;
                    }
                    
                    nlohmann::json response = {
                        {"popMaintenanceMode", enable},
                        {"message", enable ? 
                            "Pop maintenance mode ENABLED. All POP operations will return empty arrays." :
                            "Pop maintenance mode DISABLED. Normal operation resumed."
                        }
                    };
                    
                    send_json_response(res, response);
                    
                } catch (const std::exception& e) {
                    send_error_response(res, e.what(), 500);
                }
            },
            [res](const std::string& error) {
                send_error_response(res, error, 400);
            }
        );
    });
    
    // GET shared state / UDPSYNC cache stats
    app->get("/api/v1/system/shared-state", [ctx](auto* res, auto* req) {
        // Check authentication - ADMIN required for system operations
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
}

} // namespace routes
} // namespace queen

