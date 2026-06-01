#pragma once

#include "queen/auth/jwt_validator.hpp"
#include "queen/config.hpp"
#include <App.h>
#include <string>
#include <string_view>
#include <memory>
#include <optional>

namespace queen {
namespace auth {

// ============================================================================
// Access Levels
// ============================================================================

/**
 * Access levels for route protection.
 *
 * Mostly hierarchical (ADMIN > READ_WRITE > READ_ONLY > PUBLIC), with one
 * deliberately non-hierarchical capability:
 *
 *   WRITE_ONLY — grants producing (e.g. POST /api/v1/push) WITHOUT granting
 *   the ability to read or consume. A `write-only` token therefore passes
 *   WRITE_ONLY endpoints but is rejected by READ_ONLY (can't read) and
 *   READ_WRITE (can't pop/ack) endpoints. `read-write` and `admin` still
 *   satisfy WRITE_ONLY since they can write too. This lets untrusted external
 *   producers push events without being able to read other clients' data
 *   (issue #31).
 */
enum class AccessLevel {
    PUBLIC,       // No authentication required
    READ_ONLY,    // Any reader token (read-only, read-write, or admin)
    WRITE_ONLY,   // write-only, read-write, or admin role (produce, no consume)
    READ_WRITE,   // read-write or admin role required
    ADMIN         // admin role required
};

// Convert AccessLevel to string for logging
inline std::string access_level_to_string(AccessLevel level) {
    switch (level) {
        case AccessLevel::PUBLIC: return "PUBLIC";
        case AccessLevel::READ_ONLY: return "READ_ONLY";
        case AccessLevel::WRITE_ONLY: return "WRITE_ONLY";
        case AccessLevel::READ_WRITE: return "READ_WRITE";
        case AccessLevel::ADMIN: return "ADMIN";
        default: return "UNKNOWN";
    }
}

// ============================================================================
// Auth Check Result
// ============================================================================

/**
 * Result of authentication check
 */
struct AuthCheck {
    bool authorized = false;                  // Whether the request is authorized
    std::optional<JwtClaims> claims;          // Claims from the token (if authenticated)
    std::string error;                        // Error message (if not authorized)
    int status_code = 401;                    // HTTP status code to return
};

// ============================================================================
// Auth Middleware
// ============================================================================

/**
 * Authentication middleware for Queen routes
 * 
 * Handles JWT token extraction, validation, and role-based access control.
 * Can be used with the REQUIRE_AUTH macro for easy integration into routes.
 */
class AuthMiddleware {
public:
    /**
     * Create auth middleware with the given configuration
     */
    explicit AuthMiddleware(const AuthConfig& config);
    
    /**
     * Check if a path should skip authentication
     * 
     * @param path The request path
     * @return true if authentication should be skipped
     */
    bool should_skip(std::string_view path) const;
    
    /**
     * Validate request authentication and check access level
     * 
     * @param req The uWebSockets HTTP request
     * @param required_level The minimum access level required
     * @return AuthCheck result with authorization status and claims
     */
    AuthCheck check(uWS::HttpRequest* req, AccessLevel required_level);
    
    /**
     * Extract bearer token from Authorization header
     * 
     * @param req The uWebSockets HTTP request
     * @return The token string (without "Bearer " prefix) or empty if not found
     */
    std::optional<std::string> extract_token(uWS::HttpRequest* req) const;
    
    /**
     * Trigger a JWKS refresh (for RS256)
     */
    bool refresh_jwks();
    
    /**
     * Check if auth is enabled
     */
    bool is_enabled() const { return config_.enabled; }
    
    /**
     * Get the underlying validator (for testing)
     */
    JwtValidator* get_validator() { return validator_.get(); }
    
private:
    const AuthConfig& config_;
    std::unique_ptr<JwtValidator> validator_;
    
    /**
     * Check if claims meet the required access level
     */
    bool check_access_level(const JwtClaims& claims, AccessLevel required) const;
};

// ============================================================================
// Global Auth Middleware Instance
// ============================================================================

/**
 * Global auth middleware instance
 * Initialized in acceptor_server.cpp during startup
 */
extern std::shared_ptr<AuthMiddleware> global_auth_middleware;

// ============================================================================
// Route Access Level Mapping
// ============================================================================

/**
 * Determine the required access level for a route
 * 
 * @param method HTTP method (GET, POST, etc.)
 * @param path Request path
 * @return Required AccessLevel
 */
AccessLevel get_route_access_level(std::string_view method, std::string_view path);

} // namespace auth
} // namespace queen

