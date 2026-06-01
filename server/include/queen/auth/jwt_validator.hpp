#pragma once

#include "queen/config.hpp"
#include <string>
#include <vector>
#include <optional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <chrono>

namespace queen {
namespace auth {

// ============================================================================
// JWT Claims Structure
// ============================================================================

/**
 * Parsed JWT claims relevant to Queen authentication
 */
struct JwtClaims {
    std::string subject;                    // 'sub' claim - user identifier
    std::string issuer;                     // 'iss' claim - token issuer
    std::string audience;                   // 'aud' claim - intended audience
    std::vector<std::string> roles;         // Extracted roles from token
    int64_t expires_at = 0;                 // 'exp' claim - expiration time (unix timestamp)
    int64_t issued_at = 0;                  // 'iat' claim - issued time (unix timestamp)
    int64_t not_before = 0;                 // 'nbf' claim - not valid before (unix timestamp)
    
    // Additional claims from proxy tokens
    std::string user_id;                    // 'id' claim from proxy
    std::string username;                   // 'username' claim from proxy
    std::string role;                       // 'role' claim (single role) from proxy
    
    /**
     * Check if the claims contain a specific role
     */
    bool has_role(const std::string& role_name) const;
    
    /**
     * Check if user has admin role
     */
    bool is_admin(const AuthConfig& config) const;
    
    /**
     * Check if user has read-write role (or higher)
     */
    bool is_read_write(const AuthConfig& config) const;
    
    /**
     * Check if user has read-only role (or higher)
     */
    bool is_read_only(const AuthConfig& config) const;
    
    /**
     * Check if the user may WRITE (produce). True for write-only, read-write,
     * and admin. Note this is intentionally NOT implied by is_read_only: a
     * write-only token can write but cannot read/consume (issue #31).
     */
    bool is_write_only(const AuthConfig& config) const;
};

// ============================================================================
// Validation Result
// ============================================================================

/**
 * Result of JWT validation
 */
struct ValidationResult {
    bool valid = false;                     // Whether the token is valid
    std::optional<JwtClaims> claims;        // Parsed claims (if valid)
    std::string error;                      // Error message (if invalid)
    int status_code = 401;                  // HTTP status code to return
};

// ============================================================================
// JWKS Key Cache Entry
// ============================================================================

/**
 * Cached public key from JWKS endpoint
 */
struct JwkEntry {
    std::string kid;                        // Key ID
    std::string kty;                        // Key type (RSA, OKP)
    std::string algorithm;                  // Algorithm (RS256, RS384, RS512, EdDSA, etc.)
    std::string pem;                        // Public key in PEM format
    int64_t fetched_at;                     // When this key was fetched (unix timestamp)
};

// ============================================================================
// JWT Validator
// ============================================================================

/**
 * JWT token validator supporting HS256, RS256, and EdDSA algorithms
 * 
 * Features:
 * - HS256 validation with shared secret
 * - RS256 validation with JWKS URL or static public key
 * - EdDSA (Ed25519) validation with JWKS URL or static public key
 * - JWKS key caching with automatic refresh
 * - Claim validation (exp, iat, nbf, iss, aud)
 * - Role extraction from multiple claim formats
 */
class JwtValidator {
public:
    /**
     * Create a JWT validator with the given configuration
     */
    explicit JwtValidator(const AuthConfig& config);
    
    /**
     * Validate a JWT token
     * 
     * @param token The JWT token string (without "Bearer " prefix)
     * @return ValidationResult with valid flag, claims, and error info
     */
    ValidationResult validate(const std::string& token);
    
    /**
     * Refresh JWKS keys from the configured URL
     * Called periodically or when an unknown key ID is encountered
     * 
     * @return true if refresh was successful
     */
    bool refresh_jwks();
    
    /**
     * Get the last JWKS refresh timestamp
     */
    int64_t get_last_jwks_refresh() const { return last_jwks_refresh_; }
    
    /**
     * Check if JWKS refresh is needed based on configured interval
     */
    bool needs_jwks_refresh() const;
    
private:
    const AuthConfig& config_;
    
    // JWKS key cache: kid -> JwkEntry
    std::unordered_map<std::string, JwkEntry> jwks_cache_;
    mutable std::mutex jwks_mutex_;
    int64_t last_jwks_refresh_ = 0;
    
    // Internal validation methods
    ValidationResult validate_hs256(const std::string& token);
    ValidationResult validate_rs256(const std::string& token);
    ValidationResult validate_eddsa(const std::string& token);
    
    // JWKS helpers
    std::optional<std::string> get_public_key_for_kid(const std::string& kid);
    bool fetch_jwks();
    std::string jwk_to_pem(const std::string& n, const std::string& e);
    std::string jwk_to_pem_ed25519(const std::string& x);
    
    // Claim extraction and validation
    JwtClaims extract_claims(const std::string& payload_json);
    bool validate_time_claims(const JwtClaims& claims, std::string& error);
    bool validate_issuer_audience(const JwtClaims& claims, std::string& error);
    
    // Helper to decode base64url
    static std::string base64url_decode(const std::string& input);
    
    // Helper to get current unix timestamp
    static int64_t current_timestamp();
};

} // namespace auth
} // namespace queen

