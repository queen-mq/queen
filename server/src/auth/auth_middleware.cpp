#include "queen/auth/auth_middleware.hpp"
#include <spdlog/spdlog.h>
#include <algorithm>

namespace queen {
namespace auth {

// Global auth middleware instance
std::shared_ptr<AuthMiddleware> global_auth_middleware;

// ============================================================================
// AuthMiddleware Implementation
// ============================================================================

AuthMiddleware::AuthMiddleware(const AuthConfig& config) 
    : config_(config) {
    
    if (config_.enabled) {
        validator_ = std::make_unique<JwtValidator>(config_);
        spdlog::info("[AuthMiddleware] JWT authentication enabled");
        spdlog::info("[AuthMiddleware] Algorithm: {}", config_.algorithm);
        spdlog::info("[AuthMiddleware] Skip paths: {}", [&]() {
            std::string result;
            for (size_t i = 0; i < config_.skip_paths.size(); i++) {
                if (i > 0) result += ", ";
                result += config_.skip_paths[i];
            }
            return result;
        }());
        
        if (!config_.issuer.empty()) {
            spdlog::info("[AuthMiddleware] Expected issuer: {}", config_.issuer);
        }
        if (!config_.audience.empty()) {
            spdlog::info("[AuthMiddleware] Expected audience: {}", config_.audience);
        }
    } else {
        spdlog::info("[AuthMiddleware] JWT authentication disabled");
    }
}

bool AuthMiddleware::should_skip(std::string_view path) const {
    return config_.should_skip_path(std::string(path));
}

AuthCheck AuthMiddleware::check(uWS::HttpRequest* req, AccessLevel required_level) {
    // If auth is disabled, allow everything
    if (!config_.enabled) {
        return {true, std::nullopt, "", 200};
    }
    
    // Get path for skip check
    std::string path(req->getUrl());
    
    // Check if path should skip auth
    if (should_skip(path)) {
        spdlog::debug("[AuthMiddleware] Skipping auth for path: {}", path);
        return {true, std::nullopt, "", 200};
    }
    
    // Public endpoints don't need authentication
    if (required_level == AccessLevel::PUBLIC) {
        return {true, std::nullopt, "", 200};
    }
    
    // Extract token from Authorization header
    auto token = extract_token(req);
    if (!token) {
        spdlog::debug("[AuthMiddleware] No token provided for path: {}", path);
        return {false, std::nullopt, "Authentication required", 401};
    }
    
    // Validate token
    auto result = validator_->validate(*token);
    if (!result.valid) {
        spdlog::debug("[AuthMiddleware] Token validation failed: {}", result.error);
        return {false, std::nullopt, result.error, result.status_code};
    }
    
    // Check access level
    if (!check_access_level(*result.claims, required_level)) {
        std::string user = result.claims->username.empty() ? 
                          result.claims->subject : result.claims->username;
        std::string role = result.claims->role.empty() ? 
                          (result.claims->roles.empty() ? "(none)" : result.claims->roles[0]) : 
                          result.claims->role;
        
        spdlog::debug("[AuthMiddleware] Access denied for user '{}' with role '{}', required: {}", 
                     user, role, access_level_to_string(required_level));
        
        return {false, result.claims, "Insufficient permissions", 403};
    }
    
    // Authentication and authorization successful
    return {true, result.claims, "", 200};
}

std::optional<std::string> AuthMiddleware::extract_token(uWS::HttpRequest* req) const {
    // Get Authorization header
    std::string_view auth_header = req->getHeader("authorization");
    
    if (auth_header.empty()) {
        return std::nullopt;
    }
    
    // Check for "Bearer " prefix (case-insensitive)
    const std::string bearer_prefix = "Bearer ";
    const std::string bearer_prefix_lower = "bearer ";
    
    if (auth_header.size() > bearer_prefix.size()) {
        std::string prefix(auth_header.substr(0, bearer_prefix.size()));
        
        // Convert prefix to lowercase for comparison
        std::transform(prefix.begin(), prefix.end(), prefix.begin(), ::tolower);
        
        if (prefix == bearer_prefix_lower) {
            return std::string(auth_header.substr(bearer_prefix.size()));
        }
    }
    
    // If no "Bearer " prefix, return the whole header as token
    // (some clients might send just the token)
    return std::string(auth_header);
}

bool AuthMiddleware::refresh_jwks() {
    if (validator_) {
        return validator_->refresh_jwks();
    }
    return false;
}

bool AuthMiddleware::check_access_level(const JwtClaims& claims, AccessLevel required) const {
    switch (required) {
        case AccessLevel::PUBLIC:
            return true;
            
        case AccessLevel::READ_ONLY:
            // Any reader (read-only, read-write, admin). Excludes write-only.
            return claims.is_read_only(config_);
            
        case AccessLevel::WRITE_ONLY:
            // Any writer (write-only, read-write, admin). Excludes read-only.
            return claims.is_write_only(config_);
            
        case AccessLevel::READ_WRITE:
            return claims.is_read_write(config_);
            
        case AccessLevel::ADMIN:
            return claims.is_admin(config_);
            
        default:
            return false;
    }
}

// ============================================================================
// Route Access Level Mapping
// ============================================================================

AccessLevel get_route_access_level(std::string_view method, std::string_view path) {
    // Convert path to string for easier comparison
    std::string p(path);
    std::string m(method);
    
    // =========================================================================
    // PUBLIC endpoints (no auth required)
    // =========================================================================
    
    // Health and metrics
    if (p == "/health" || p == "/metrics" || p == "/metrics/prometheus") {
        return AccessLevel::PUBLIC;
    }
    
    // Static files (dashboard)
    if (p == "/" || p.find("/assets/") == 0 || p.find("/favicon") == 0) {
        return AccessLevel::PUBLIC;
    }
    
    // =========================================================================
    // ADMIN only endpoints
    // =========================================================================
    
    // System maintenance
    if (p.find("/api/v1/system/") == 0) {
        return AccessLevel::ADMIN;
    }
    
    // Internal peer communication
    if (p.find("/internal/") == 0) {
        return AccessLevel::ADMIN;
    }
    
    // Consumer group deletion
    if (m == "DELETE" && p.find("/api/v1/consumer-groups/") == 0) {
        return AccessLevel::ADMIN;
    }
    
    // Queue deletion
    if (m == "DELETE" && p.find("/api/v1/resources/queues/") == 0) {
        return AccessLevel::ADMIN;
    }
    
    // Stats refresh (admin operation)
    if (p == "/api/v1/stats/refresh") {
        return AccessLevel::ADMIN;
    }
    
    // =========================================================================
    // READ_ONLY endpoints (GET requests to status/info endpoints)
    // =========================================================================
    
    if (m == "GET") {
        // Status and analytics
        if (p.find("/api/v1/status") == 0 ||
            p.find("/api/v1/analytics") == 0) {
            return AccessLevel::READ_ONLY;
        }
        
        // Resource listing
        if (p.find("/api/v1/resources/") == 0) {
            return AccessLevel::READ_ONLY;
        }
        
        // Message listing (not consuming)
        if (p.find("/api/v1/messages") == 0) {
            return AccessLevel::READ_ONLY;
        }
        
        // Consumer groups info
        if (p.find("/api/v1/consumer-groups") == 0) {
            return AccessLevel::READ_ONLY;
        }
        
        // DLQ viewing
        if (p.find("/api/v1/dlq") == 0) {
            return AccessLevel::READ_ONLY;
        }
        
        // Traces viewing
        if (p.find("/api/v1/traces") == 0) {
            return AccessLevel::READ_ONLY;
        }
    }
    
    // =========================================================================
    // WRITE_ONLY endpoints (pure produce — no read/consume)
    // =========================================================================
    
    // Push messages. WRITE_ONLY so a `write-only` producer can publish, while
    // still allowing read-write/admin. Pop/ack/transaction below stay
    // READ_WRITE because they consume (issue #31).
    if (p == "/api/v1/push") {
        return AccessLevel::WRITE_ONLY;
    }
    
    // =========================================================================
    // READ_WRITE endpoints (everything else - pop, ack, transaction, etc.)
    // =========================================================================
    
    // Pop messages
    if (p.find("/api/v1/pop") == 0) {
        return AccessLevel::READ_WRITE;
    }
    
    // Acknowledge messages
    if (p.find("/api/v1/ack") == 0) {
        return AccessLevel::READ_WRITE;
    }
    
    // Transactions
    if (p == "/api/v1/transaction") {
        return AccessLevel::READ_WRITE;
    }
    
    // Lease management
    if (p.find("/api/v1/lease/") == 0) {
        return AccessLevel::READ_WRITE;
    }
    
    // Configure queue
    if (p == "/api/v1/configure") {
        return AccessLevel::READ_WRITE;
    }
    
    // Consumer group subscription changes
    if (m == "POST" && p.find("/api/v1/consumer-groups/") == 0 && 
        p.find("/subscription") != std::string::npos) {
        return AccessLevel::READ_WRITE;
    }
    
    // Traces creation
    if (m == "POST" && p == "/api/v1/traces") {
        return AccessLevel::READ_WRITE;
    }
    
    // Message deletion (individual messages)
    if (m == "DELETE" && p.find("/api/v1/messages/") == 0) {
        return AccessLevel::READ_WRITE;
    }
    
    // Default: require READ_WRITE for unknown endpoints
    spdlog::debug("[AccessLevel] Unknown endpoint {} {}, defaulting to READ_WRITE", m, p);
    return AccessLevel::READ_WRITE;
}

} // namespace auth
} // namespace queen

