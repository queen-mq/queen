#pragma once

#include <App.h>
#include <json.hpp>
#include <string>
#include <functional>
#include <map>
#include <chrono>
#include "queen/auth/auth_middleware.hpp"

namespace queen {
namespace routes {

// ============================================================================
// Authentication Macros
// ============================================================================

/**
 * REQUIRE_AUTH - Check authentication and authorization for a route
 * 
 * Usage in route handlers:
 *   REQUIRE_AUTH(res, req, ctx, auth::AccessLevel::READ_WRITE);
 * 
 * If auth fails, sends error response and returns from the handler.
 * If auth is disabled, passes through without checking.
 * 
 * @param res  The uWS HttpResponse pointer
 * @param req  The uWS HttpRequest pointer
 * @param ctx  The RouteContext reference
 * @param level The required AccessLevel (PUBLIC, READ_ONLY, READ_WRITE, ADMIN)
 */
#define REQUIRE_AUTH(res, req, ctx, level) \
    do { \
        if (ctx.config.auth.enabled && queen::auth::global_auth_middleware) { \
            auto _auth_result = queen::auth::global_auth_middleware->check(req, level); \
            if (!_auth_result.authorized) { \
                send_error_response(res, _auth_result.error, _auth_result.status_code); \
                return; \
            } \
        } \
    } while(0)

/**
 * REQUIRE_AUTH_WITH_CLAIMS - Check auth and capture claims for use in handler
 * 
 * Usage:
 *   std::optional<auth::JwtClaims> claims;
 *   REQUIRE_AUTH_WITH_CLAIMS(res, req, ctx, auth::AccessLevel::READ_WRITE, claims);
 *   // Now 'claims' contains the JWT claims if auth was successful
 * 
 * @param res    The uWS HttpResponse pointer
 * @param req    The uWS HttpRequest pointer
 * @param ctx    The RouteContext reference
 * @param level  The required AccessLevel
 * @param claims Variable to store the claims (std::optional<auth::JwtClaims>)
 */
// Note: the out-parameter name must NOT collide with any member of AuthCheck
// (e.g. "claims"), otherwise token-pasting in the macro body rewrites the
// wrong identifier. Use a sigil-style name and reassign at the bottom.
#define REQUIRE_AUTH_WITH_CLAIMS(res, req, ctx, level, _claims_out) \
    do { \
        if (ctx.config.auth.enabled && queen::auth::global_auth_middleware) { \
            auto _auth_result = queen::auth::global_auth_middleware->check(req, level); \
            if (!_auth_result.authorized) { \
                send_error_response(res, _auth_result.error, _auth_result.status_code); \
                return; \
            } \
            _claims_out = _auth_result.claims; \
        } \
    } while(0)

// ============================================================================
// CORS and Response Helpers
// ============================================================================

/**
 * Setup CORS headers for cross-origin requests
 */
void setup_cors_headers(uWS::HttpResponse<false>* res);

/**
 * Send JSON response with proper headers and status code
 */
void send_json_response(uWS::HttpResponse<false>* res, const nlohmann::json& json, int status_code = 200);

/**
 * Send error response as JSON
 */
void send_error_response(uWS::HttpResponse<false>* res, const std::string& error, int status_code = 500);

// ============================================================================
// Request Body Helpers
// ============================================================================

/**
 * Read and parse JSON body from HTTP request
 * Handles chunked data and parsing errors
 */
void read_json_body(
    uWS::HttpResponse<false>* res,
    std::function<void(const nlohmann::json&)> callback,
    std::function<void(const std::string&)> error_callback
);

/**
 * Read the raw request body WITHOUT parsing it.
 *
 * Accumulates chunks into a std::string and, on the final chunk, guarantees at
 * least 64 bytes of readable slack past the data (so SIMD parsers such as
 * simdjson can over-read safely without an extra copy). The buffer is handed to
 * `on_complete` by reference and stays alive for the duration of that call.
 * Used by hot routes that parse with simdjson and pass payloads through raw.
 */
void read_body_raw(
    uWS::HttpResponse<false>* res,
    std::function<void(std::string&)> on_complete,
    std::function<void(const std::string&)> error_callback
);

// ============================================================================
// Query Parameter Helpers
// ============================================================================

/**
 * URL decode a string (handles %XX encoding and + for spaces)
 */
std::string url_decode(const std::string& str);

/**
 * Get query parameter as string with default value
 */
std::string get_query_param(uWS::HttpRequest* req, const std::string& key, const std::string& default_value = "");

/**
 * Get query parameter as integer with default value
 */
int get_query_param_int(uWS::HttpRequest* req, const std::string& key, int default_value);

/**
 * Get query parameter as boolean with default value
 */
bool get_query_param_bool(uWS::HttpRequest* req, const std::string& key, bool default_value);

// ============================================================================
// Static File Serving Helpers
// ============================================================================

/**
 * Get MIME type based on file extension
 */
std::string get_mime_type(const std::string& file_path);

/**
 * Serve static file from disk with security checks and caching headers
 * Returns true if file was served (successfully or with error), false if file not found
 */
bool serve_static_file(
    uWS::HttpResponse<false>* res,
    const std::string& file_path,
    const std::string& webapp_root
);

// ============================================================================
// Common Business Logic Helpers
// ============================================================================

/**
 * Parse and determine subscription mode and timestamp for consumer groups
 * Returns {mode_value, timestamp_sql}
 */
std::pair<std::string, std::string> parse_subscription_mode(
    const std::string& sub_mode,
    const std::string& sub_from,
    const std::string& default_sub_mode
);

/**
 * Format timestamp for JSON response (ISO 8601 with milliseconds)
 */
std::string format_timestamp_iso8601(const std::chrono::system_clock::time_point& tp);

} // namespace routes
} // namespace queen

