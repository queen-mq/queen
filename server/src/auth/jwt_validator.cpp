#include "queen/auth/jwt_validator.hpp"
#include <jwt-cpp/jwt.h>
#include <httplib.h>
#include <json.hpp>
#include <spdlog/spdlog.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <openssl/pem.h>
#include <openssl/bn.h>
#include <ctime>
#include <sstream>
#include <algorithm>

namespace queen {
namespace auth {

// ============================================================================
// JwtClaims Implementation
// ============================================================================

bool JwtClaims::has_role(const std::string& role_name) const {
    // Check single role field first (proxy format)
    if (!role.empty() && role == role_name) {
        return true;
    }
    // Check roles array
    for (const auto& r : roles) {
        if (r == role_name) return true;
    }
    return false;
}

bool JwtClaims::is_admin(const AuthConfig& config) const {
    return has_role(config.role_admin);
}

bool JwtClaims::is_read_write(const AuthConfig& config) const {
    return has_role(config.role_admin) || has_role(config.role_read_write);
}

bool JwtClaims::is_read_only(const AuthConfig& config) const {
    return has_role(config.role_admin) || 
           has_role(config.role_read_write) || 
           has_role(config.role_read_only);
}

bool JwtClaims::is_write_only(const AuthConfig& config) const {
    // Anyone who can write: explicit write-only, plus read-write and admin.
    // Deliberately excludes read-only — a write-only client must not be able
    // to read, and a read-only client must not be able to write.
    return has_role(config.role_admin) ||
           has_role(config.role_read_write) ||
           has_role(config.role_write_only);
}

// ============================================================================
// JwtValidator Implementation
// ============================================================================

JwtValidator::JwtValidator(const AuthConfig& config) : config_(config) {
    spdlog::info("[JwtValidator] Initialized with algorithm: {}", config_.algorithm);
    
    // If RS256/EdDSA/auto with JWKS URL, do initial fetch
    if ((config_.algorithm == "RS256" || config_.algorithm == "EdDSA" || config_.algorithm == "auto") && 
        !config_.jwks_url.empty()) {
        spdlog::info("[JwtValidator] Pre-fetching JWKS from: {}", config_.jwks_url);
        if (fetch_jwks()) {
            spdlog::info("[JwtValidator] JWKS pre-fetch successful, cached {} keys", jwks_cache_.size());
        } else {
            spdlog::warn("[JwtValidator] JWKS pre-fetch failed, will retry on first request");
        }
    }
}

ValidationResult JwtValidator::validate(const std::string& token) {
    if (token.empty()) {
        return {false, std::nullopt, "Empty token", 401};
    }
    
    try {
        // Decode without verification first to check algorithm
        auto decoded = jwt::decode(token);
        std::string alg = decoded.get_algorithm();
        
        spdlog::debug("[JwtValidator] Token algorithm: {}, configured: {}", alg, config_.algorithm);
        
        if (config_.algorithm == "auto") {
            // Auto-detect based on token's algorithm
            if (alg == "HS256") {
                return validate_hs256(token);
            } else if (alg == "RS256" || alg == "RS384" || alg == "RS512") {
                return validate_rs256(token);
            } else if (alg == "EdDSA") {
                return validate_eddsa(token);
            } else {
                return {false, std::nullopt, "Unsupported algorithm: " + alg, 401};
            }
        } else if (config_.algorithm == "EdDSA") {
            if (alg != "EdDSA") {
                return {false, std::nullopt, "Token uses " + alg + ", expected EdDSA", 401};
            }
            return validate_eddsa(token);
        } else if (config_.algorithm == "HS256") {
            if (alg != "HS256") {
                return {false, std::nullopt, "Token uses " + alg + ", expected HS256", 401};
            }
            return validate_hs256(token);
        } else if (config_.algorithm == "RS256") {
            if (alg != "RS256" && alg != "RS384" && alg != "RS512") {
                return {false, std::nullopt, "Token uses " + alg + ", expected RS256", 401};
            }
            return validate_rs256(token);
        } else {
            return {false, std::nullopt, "Invalid algorithm configuration", 500};
        }
        
    } catch (const jwt::error::token_verification_exception& e) {
        spdlog::debug("[JwtValidator] Token verification failed: {}", e.what());
        return {false, std::nullopt, std::string("Token verification failed: ") + e.what(), 401};
    } catch (const std::exception& e) {
        spdlog::debug("[JwtValidator] Token decode error: {}", e.what());
        return {false, std::nullopt, std::string("Token decode error: ") + e.what(), 401};
    }
}

ValidationResult JwtValidator::validate_hs256(const std::string& token) {
    if (config_.secret.empty()) {
        return {false, std::nullopt, "HS256 secret not configured", 500};
    }
    
    try {
        auto decoded = jwt::decode(token);
        
        // Build verifier
        auto verifier = jwt::verify()
            .allow_algorithm(jwt::algorithm::hs256{config_.secret})
            .leeway(config_.clock_skew_seconds);
        
        // Add issuer check if configured
        if (!config_.issuer.empty()) {
            verifier.with_issuer(config_.issuer);
        }
        
        // Add audience check if configured
        if (!config_.audience.empty()) {
            verifier.with_audience(config_.audience);
        }
        
        // Verify signature and standard claims
        verifier.verify(decoded);
        
        // Extract claims
        JwtClaims claims;
        
        // Standard claims
        if (decoded.has_subject()) {
            claims.subject = decoded.get_subject();
        }
        if (decoded.has_issuer()) {
            claims.issuer = decoded.get_issuer();
        }
        if (decoded.has_audience()) {
            auto aud_set = decoded.get_audience();
            if (!aud_set.empty()) {
                claims.audience = *aud_set.begin();
            }
        }
        if (decoded.has_expires_at()) {
            claims.expires_at = std::chrono::duration_cast<std::chrono::seconds>(
                decoded.get_expires_at().time_since_epoch()).count();
        }
        if (decoded.has_issued_at()) {
            claims.issued_at = std::chrono::duration_cast<std::chrono::seconds>(
                decoded.get_issued_at().time_since_epoch()).count();
        }
        if (decoded.has_not_before()) {
            claims.not_before = std::chrono::duration_cast<std::chrono::seconds>(
                decoded.get_not_before().time_since_epoch()).count();
        }
        
        // Proxy-specific claims
        if (decoded.has_payload_claim("id")) {
            claims.user_id = decoded.get_payload_claim("id").as_string();
        }
        if (decoded.has_payload_claim("username")) {
            claims.username = decoded.get_payload_claim("username").as_string();
        }
        
        // Role claim (single value - proxy format)
        if (decoded.has_payload_claim(config_.roles_claim)) {
            try {
                claims.role = decoded.get_payload_claim(config_.roles_claim).as_string();
            } catch (...) {
                // Not a string, ignore
            }
        }
        
        // Roles array claim
        if (decoded.has_payload_claim(config_.roles_array_claim)) {
            try {
                auto roles_json = decoded.get_payload_claim(config_.roles_array_claim);
                // jwt-cpp returns the claim as a json value, need to iterate
                auto roles_set = roles_json.as_set();
                for (const auto& r : roles_set) {
                    claims.roles.push_back(r);
                }
            } catch (...) {
                // Try as array of strings via to_json
                try {
                    auto claim_json = decoded.get_payload_claim(config_.roles_array_claim).to_json();
                    if (claim_json.is<picojson::array>()) {
                        const auto& arr = claim_json.get<picojson::array>();
                        for (const auto& item : arr) {
                            if (item.is<std::string>()) {
                                claims.roles.push_back(item.get<std::string>());
                            }
                        }
                    }
                } catch (...) {
                    // Ignore parsing errors
                }
            }
        }
        
        spdlog::debug("[JwtValidator] HS256 validation successful for user: {}", 
                     claims.username.empty() ? claims.subject : claims.username);
        
        return {true, claims, "", 200};
        
    } catch (const jwt::error::token_verification_exception& e) {
        std::string err_msg = e.what();
        spdlog::debug("[JwtValidator] HS256 verification failed: {}", err_msg);
        
        // Provide more specific error messages
        if (err_msg.find("signature") != std::string::npos) {
            return {false, std::nullopt, "Invalid token signature", 401};
        } else if (err_msg.find("expired") != std::string::npos || 
                   err_msg.find("exp") != std::string::npos) {
            return {false, std::nullopt, "Token has expired", 401};
        } else if (err_msg.find("issuer") != std::string::npos) {
            return {false, std::nullopt, "Invalid token issuer", 401};
        } else if (err_msg.find("audience") != std::string::npos) {
            return {false, std::nullopt, "Invalid token audience", 401};
        }
        
        return {false, std::nullopt, err_msg, 401};
    } catch (const std::exception& e) {
        return {false, std::nullopt, std::string("HS256 validation error: ") + e.what(), 401};
    }
}

ValidationResult JwtValidator::validate_rs256(const std::string& token) {
    try {
        auto decoded = jwt::decode(token);
        
        // Get the key ID from token header
        std::string kid;
        if (decoded.has_key_id()) {
            kid = decoded.get_key_id();
        }
        
        spdlog::debug("[JwtValidator] RS256 token with kid: {}", kid.empty() ? "(none)" : kid);
        
        // Get public key
        std::optional<std::string> pem;
        
        // First try static public key if configured
        if (!config_.public_key.empty()) {
            pem = config_.public_key;
            spdlog::debug("[JwtValidator] Using static public key");
        } else {
            // Try to get from JWKS cache
            pem = get_public_key_for_kid(kid);
            
            if (!pem) {
                // Key not found, try refreshing JWKS
                spdlog::debug("[JwtValidator] Key '{}' not in cache, refreshing JWKS", kid);
                if (!refresh_jwks()) {
                    return {false, std::nullopt, "Failed to fetch JWKS", 500};
                }
                
                pem = get_public_key_for_kid(kid);
                if (!pem) {
                    return {false, std::nullopt, "Unknown key ID: " + kid, 401};
                }
            }
        }
        
        // Build verifier with RSA public key
        auto verifier = jwt::verify()
            .allow_algorithm(jwt::algorithm::rs256{*pem, "", "", ""})
            .leeway(config_.clock_skew_seconds);
        
        // Add issuer check if configured
        if (!config_.issuer.empty()) {
            verifier.with_issuer(config_.issuer);
        }
        
        // Add audience check if configured
        if (!config_.audience.empty()) {
            verifier.with_audience(config_.audience);
        }
        
        // Verify signature and standard claims
        verifier.verify(decoded);
        
        // Extract claims (same as HS256)
        JwtClaims claims;
        
        if (decoded.has_subject()) {
            claims.subject = decoded.get_subject();
        }
        if (decoded.has_issuer()) {
            claims.issuer = decoded.get_issuer();
        }
        if (decoded.has_audience()) {
            auto aud_set = decoded.get_audience();
            if (!aud_set.empty()) {
                claims.audience = *aud_set.begin();
            }
        }
        if (decoded.has_expires_at()) {
            claims.expires_at = std::chrono::duration_cast<std::chrono::seconds>(
                decoded.get_expires_at().time_since_epoch()).count();
        }
        if (decoded.has_issued_at()) {
            claims.issued_at = std::chrono::duration_cast<std::chrono::seconds>(
                decoded.get_issued_at().time_since_epoch()).count();
        }
        if (decoded.has_not_before()) {
            claims.not_before = std::chrono::duration_cast<std::chrono::seconds>(
                decoded.get_not_before().time_since_epoch()).count();
        }
        
        // Custom claims
        if (decoded.has_payload_claim("id")) {
            try {
                claims.user_id = decoded.get_payload_claim("id").as_string();
            } catch (...) {}
        }
        if (decoded.has_payload_claim("username")) {
            try {
                claims.username = decoded.get_payload_claim("username").as_string();
            } catch (...) {}
        }
        
        // Role claims
        if (decoded.has_payload_claim(config_.roles_claim)) {
            try {
                claims.role = decoded.get_payload_claim(config_.roles_claim).as_string();
            } catch (...) {
                // Not a string, ignore
            }
        }
        
        if (decoded.has_payload_claim(config_.roles_array_claim)) {
            try {
                auto roles_json = decoded.get_payload_claim(config_.roles_array_claim);
                auto roles_set = roles_json.as_set();
                for (const auto& r : roles_set) {
                    claims.roles.push_back(r);
                }
            } catch (...) {
                try {
                    auto claim_json = decoded.get_payload_claim(config_.roles_array_claim).to_json();
                    if (claim_json.is<picojson::array>()) {
                        const auto& arr = claim_json.get<picojson::array>();
                        for (const auto& item : arr) {
                            if (item.is<std::string>()) {
                                claims.roles.push_back(item.get<std::string>());
                            }
                        }
                    }
                } catch (...) {}
            }
        }
        
        spdlog::debug("[JwtValidator] RS256 validation successful for subject: {}", claims.subject);
        
        return {true, claims, "", 200};
        
    } catch (const jwt::error::token_verification_exception& e) {
        std::string err_msg = e.what();
        spdlog::debug("[JwtValidator] RS256 verification failed: {}", err_msg);
        return {false, std::nullopt, err_msg, 401};
    } catch (const std::exception& e) {
        return {false, std::nullopt, std::string("RS256 validation error: ") + e.what(), 401};
    }
}

ValidationResult JwtValidator::validate_eddsa(const std::string& token) {
    try {
        auto decoded = jwt::decode(token);
        
        // Get the key ID from token header
        std::string kid;
        if (decoded.has_key_id()) {
            kid = decoded.get_key_id();
        }
        
        spdlog::debug("[JwtValidator] EdDSA token with kid: {}", kid.empty() ? "(none)" : kid);
        
        // Get public key
        std::optional<std::string> pem;
        
        // First try static public key if configured
        if (!config_.public_key.empty()) {
            pem = config_.public_key;
            spdlog::debug("[JwtValidator] Using static public key for EdDSA");
        } else {
            // Try to get from JWKS cache
            pem = get_public_key_for_kid(kid);
            
            if (!pem) {
                // Key not found, try refreshing JWKS
                spdlog::debug("[JwtValidator] Key '{}' not in cache, refreshing JWKS", kid);
                if (!refresh_jwks()) {
                    return {false, std::nullopt, "Failed to fetch JWKS", 500};
                }
                
                pem = get_public_key_for_kid(kid);
                if (!pem) {
                    return {false, std::nullopt, "Unknown key ID: " + kid, 401};
                }
            }
        }
        
        // Build verifier with Ed25519 public key
        auto verifier = jwt::verify()
            .allow_algorithm(jwt::algorithm::ed25519{*pem, "", "", ""})
            .leeway(config_.clock_skew_seconds);
        
        // Add issuer check if configured
        if (!config_.issuer.empty()) {
            verifier.with_issuer(config_.issuer);
        }
        
        // Add audience check if configured
        if (!config_.audience.empty()) {
            verifier.with_audience(config_.audience);
        }
        
        // Verify signature and standard claims
        verifier.verify(decoded);
        
        // Extract claims (same as RS256)
        JwtClaims claims;
        
        if (decoded.has_subject()) {
            claims.subject = decoded.get_subject();
        }
        if (decoded.has_issuer()) {
            claims.issuer = decoded.get_issuer();
        }
        if (decoded.has_audience()) {
            auto aud_set = decoded.get_audience();
            if (!aud_set.empty()) {
                claims.audience = *aud_set.begin();
            }
        }
        if (decoded.has_expires_at()) {
            claims.expires_at = std::chrono::duration_cast<std::chrono::seconds>(
                decoded.get_expires_at().time_since_epoch()).count();
        }
        if (decoded.has_issued_at()) {
            claims.issued_at = std::chrono::duration_cast<std::chrono::seconds>(
                decoded.get_issued_at().time_since_epoch()).count();
        }
        if (decoded.has_not_before()) {
            claims.not_before = std::chrono::duration_cast<std::chrono::seconds>(
                decoded.get_not_before().time_since_epoch()).count();
        }
        
        // Custom claims
        if (decoded.has_payload_claim("id")) {
            try {
                claims.user_id = decoded.get_payload_claim("id").as_string();
            } catch (...) {}
        }
        if (decoded.has_payload_claim("username")) {
            try {
                claims.username = decoded.get_payload_claim("username").as_string();
            } catch (...) {}
        }
        
        // Role claims
        if (decoded.has_payload_claim(config_.roles_claim)) {
            try {
                claims.role = decoded.get_payload_claim(config_.roles_claim).as_string();
            } catch (...) {
                // Not a string, ignore
            }
        }
        
        if (decoded.has_payload_claim(config_.roles_array_claim)) {
            try {
                auto roles_json = decoded.get_payload_claim(config_.roles_array_claim);
                auto roles_set = roles_json.as_set();
                for (const auto& r : roles_set) {
                    claims.roles.push_back(r);
                }
            } catch (...) {
                try {
                    auto claim_json = decoded.get_payload_claim(config_.roles_array_claim).to_json();
                    if (claim_json.is<picojson::array>()) {
                        const auto& arr = claim_json.get<picojson::array>();
                        for (const auto& item : arr) {
                            if (item.is<std::string>()) {
                                claims.roles.push_back(item.get<std::string>());
                            }
                        }
                    }
                } catch (...) {}
            }
        }
        
        spdlog::debug("[JwtValidator] EdDSA validation successful for subject: {}", claims.subject);
        
        return {true, claims, "", 200};
        
    } catch (const jwt::error::token_verification_exception& e) {
        std::string err_msg = e.what();
        spdlog::debug("[JwtValidator] EdDSA verification failed: {}", err_msg);
        return {false, std::nullopt, err_msg, 401};
    } catch (const std::exception& e) {
        return {false, std::nullopt, std::string("EdDSA validation error: ") + e.what(), 401};
    }
}

bool JwtValidator::refresh_jwks() {
    if (config_.jwks_url.empty()) {
        spdlog::warn("[JwtValidator] No JWKS URL configured");
        return false;
    }
    
    return fetch_jwks();
}

bool JwtValidator::needs_jwks_refresh() const {
    if (config_.jwks_url.empty()) return false;
    
    auto now = current_timestamp();
    return (now - last_jwks_refresh_) > config_.jwks_refresh_interval_seconds;
}

std::optional<std::string> JwtValidator::get_public_key_for_kid(const std::string& kid) {
    std::lock_guard<std::mutex> lock(jwks_mutex_);
    
    // If no kid specified, return first key (for single-key JWKS)
    if (kid.empty() && !jwks_cache_.empty()) {
        return jwks_cache_.begin()->second.pem;
    }
    
    auto it = jwks_cache_.find(kid);
    if (it != jwks_cache_.end()) {
        return it->second.pem;
    }
    
    return std::nullopt;
}

bool JwtValidator::fetch_jwks() {
    try {
        // Parse JWKS URL
        std::string url = config_.jwks_url;
        std::string host, path;
        int port = 443;
        bool use_ssl = true;
        
        // Parse URL
        if (url.find("https://") == 0) {
            url = url.substr(8);
            use_ssl = true;
            port = 443;
        } else if (url.find("http://") == 0) {
            url = url.substr(7);
            use_ssl = false;
            port = 80;
        }
        
        size_t path_start = url.find('/');
        if (path_start != std::string::npos) {
            host = url.substr(0, path_start);
            path = url.substr(path_start);
        } else {
            host = url;
            path = "/";
        }
        
        // Check for port in host
        size_t port_start = host.find(':');
        if (port_start != std::string::npos) {
            port = std::stoi(host.substr(port_start + 1));
            host = host.substr(0, port_start);
        }
        
        spdlog::debug("[JwtValidator] Fetching JWKS from {}:{}{}", host, port, path);
        
        // Create HTTP client
        // httplib::Client with scheme handles SSL automatically
        std::string scheme = use_ssl ? "https://" : "http://";
        httplib::Client cli(scheme + host + ":" + std::to_string(port));
        
        cli.set_connection_timeout(config_.jwks_request_timeout_ms / 1000, 
                                   (config_.jwks_request_timeout_ms % 1000) * 1000);
        cli.set_read_timeout(config_.jwks_request_timeout_ms / 1000,
                             (config_.jwks_request_timeout_ms % 1000) * 1000);
        
        auto res = cli.Get(path);
        
        if (!res) {
            spdlog::error("[JwtValidator] JWKS fetch failed: connection error");
            return false;
        }
        
        if (res->status != 200) {
            spdlog::error("[JwtValidator] JWKS fetch failed: HTTP {}", res->status);
            return false;
        }
        
        // Parse JWKS JSON
        auto jwks = nlohmann::json::parse(res->body);
        
        if (!jwks.contains("keys") || !jwks["keys"].is_array()) {
            spdlog::error("[JwtValidator] Invalid JWKS response: missing 'keys' array");
            return false;
        }
        
        // Process keys
        std::lock_guard<std::mutex> lock(jwks_mutex_);
        jwks_cache_.clear();
        
        for (const auto& key : jwks["keys"]) {
            if (!key.contains("kty")) {
                continue;
            }
            
            std::string kty = key["kty"];
            JwkEntry entry;
            entry.kid = key.value("kid", "");
            entry.kty = kty;
            entry.fetched_at = current_timestamp();
            
            if (kty == "RSA") {
                // RSA key - need modulus (n) and exponent (e)
                if (!key.contains("n") || !key.contains("e")) {
                    spdlog::debug("[JwtValidator] Skipping RSA key without n/e: kid={}", entry.kid);
                    continue;
                }
                
                entry.algorithm = key.value("alg", "RS256");
                entry.pem = jwk_to_pem(key["n"], key["e"]);
                
            } else if (kty == "OKP") {
                // Octet Key Pair - Ed25519/Ed448
                std::string crv = key.value("crv", "");
                if (crv != "Ed25519") {
                    spdlog::debug("[JwtValidator] Skipping OKP key with unsupported curve: crv={}", crv);
                    continue;
                }
                
                if (!key.contains("x")) {
                    spdlog::debug("[JwtValidator] Skipping Ed25519 key without x parameter: kid={}", entry.kid);
                    continue;
                }
                
                entry.algorithm = key.value("alg", "EdDSA");
                entry.pem = jwk_to_pem_ed25519(key["x"]);
                
            } else {
                // Unsupported key type
                spdlog::debug("[JwtValidator] Skipping unsupported key type: kty={}", kty);
                continue;
            }
            
            if (!entry.pem.empty()) {
                jwks_cache_[entry.kid] = entry;
                spdlog::debug("[JwtValidator] Cached key: kid={}, kty={}, alg={}", entry.kid, entry.kty, entry.algorithm);
            }
        }
        
        last_jwks_refresh_ = current_timestamp();
        spdlog::info("[JwtValidator] JWKS refreshed, cached {} keys", jwks_cache_.size());
        
        return !jwks_cache_.empty();
        
    } catch (const std::exception& e) {
        spdlog::error("[JwtValidator] JWKS fetch error: {}", e.what());
        return false;
    }
}

// Suppress OpenSSL 3.0 deprecation warnings for RSA_* functions
// These functions still work and are needed for JWK to PEM conversion
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif

std::string JwtValidator::jwk_to_pem(const std::string& n_b64url, const std::string& e_b64url) {
    try {
        // Decode base64url to binary
        std::string n_bytes = base64url_decode(n_b64url);
        std::string e_bytes = base64url_decode(e_b64url);
        
        if (n_bytes.empty() || e_bytes.empty()) {
            return "";
        }
        
        // Create BIGNUM from binary data
        BIGNUM* n_bn = BN_bin2bn(reinterpret_cast<const unsigned char*>(n_bytes.data()), 
                                 static_cast<int>(n_bytes.size()), nullptr);
        BIGNUM* e_bn = BN_bin2bn(reinterpret_cast<const unsigned char*>(e_bytes.data()), 
                                 static_cast<int>(e_bytes.size()), nullptr);
        
        if (!n_bn || !e_bn) {
            if (n_bn) BN_free(n_bn);
            if (e_bn) BN_free(e_bn);
            return "";
        }
        
        // Create RSA key (deprecated in OpenSSL 3.0 but still functional)
        RSA* rsa = RSA_new();
        if (!rsa) {
            BN_free(n_bn);
            BN_free(e_bn);
            return "";
        }
        
        // Set RSA key components (takes ownership of BIGNUMs)
        if (RSA_set0_key(rsa, n_bn, e_bn, nullptr) != 1) {
            RSA_free(rsa);
            // Note: RSA_set0_key takes ownership on success, so don't free BIGNUMs here
            return "";
        }
        
        // Create EVP_PKEY
        EVP_PKEY* pkey = EVP_PKEY_new();
        if (!pkey) {
            RSA_free(rsa);
            return "";
        }
        
        if (EVP_PKEY_assign_RSA(pkey, rsa) != 1) {
            EVP_PKEY_free(pkey);
            RSA_free(rsa);
            return "";
        }
        
        // Write to PEM
        BIO* bio = BIO_new(BIO_s_mem());
        if (!bio) {
            EVP_PKEY_free(pkey);
            return "";
        }
        
        if (PEM_write_bio_PUBKEY(bio, pkey) != 1) {
            BIO_free(bio);
            EVP_PKEY_free(pkey);
            return "";
        }
        
        // Read PEM string
        char* pem_data = nullptr;
        long pem_len = BIO_get_mem_data(bio, &pem_data);
        
        std::string pem(pem_data, pem_len);
        
        BIO_free(bio);
        EVP_PKEY_free(pkey);
        
        return pem;
        
    } catch (const std::exception& e) {
        spdlog::error("[JwtValidator] JWK to PEM conversion error: {}", e.what());
        return "";
    }
}

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif

std::string JwtValidator::jwk_to_pem_ed25519(const std::string& x_b64url) {
    try {
        // Decode base64url to binary (32 bytes for Ed25519 public key)
        std::string x_bytes = base64url_decode(x_b64url);
        
        if (x_bytes.size() != 32) {
            spdlog::error("[JwtValidator] Ed25519 public key must be 32 bytes, got {}", x_bytes.size());
            return "";
        }
        
        // Ed25519 public key ASN.1 DER structure:
        // SEQUENCE {
        //   SEQUENCE {
        //     OBJECT IDENTIFIER 1.3.101.112 (Ed25519)
        //   }
        //   BIT STRING (public key)
        // }
        // 
        // The fixed prefix for Ed25519 public keys is:
        // 30 2a 30 05 06 03 2b 65 70 03 21 00
        // (12 bytes) followed by the 32-byte public key
        
        const unsigned char prefix[] = {
            0x30, 0x2a,             // SEQUENCE, 42 bytes total
            0x30, 0x05,             // SEQUENCE, 5 bytes (algorithm identifier)
            0x06, 0x03, 0x2b, 0x65, 0x70,  // OID 1.3.101.112 (Ed25519)
            0x03, 0x21, 0x00        // BIT STRING, 33 bytes (00 prefix + 32 byte key)
        };
        
        // Combine prefix + public key bytes
        std::vector<unsigned char> der_bytes;
        der_bytes.insert(der_bytes.end(), prefix, prefix + sizeof(prefix));
        der_bytes.insert(der_bytes.end(), 
                         reinterpret_cast<const unsigned char*>(x_bytes.data()),
                         reinterpret_cast<const unsigned char*>(x_bytes.data()) + x_bytes.size());
        
        // Convert DER to PEM using OpenSSL
        BIO* bio = BIO_new(BIO_s_mem());
        if (!bio) {
            return "";
        }
        
        // Create EVP_PKEY from DER
        const unsigned char* der_ptr = der_bytes.data();
        EVP_PKEY* pkey = d2i_PUBKEY(nullptr, &der_ptr, static_cast<long>(der_bytes.size()));
        
        if (!pkey) {
            BIO_free(bio);
            spdlog::error("[JwtValidator] Failed to parse Ed25519 public key DER");
            return "";
        }
        
        // Write to PEM
        if (PEM_write_bio_PUBKEY(bio, pkey) != 1) {
            EVP_PKEY_free(pkey);
            BIO_free(bio);
            spdlog::error("[JwtValidator] Failed to write Ed25519 public key to PEM");
            return "";
        }
        
        // Read PEM string
        char* pem_data = nullptr;
        long pem_len = BIO_get_mem_data(bio, &pem_data);
        
        std::string pem(pem_data, pem_len);
        
        EVP_PKEY_free(pkey);
        BIO_free(bio);
        
        return pem;
        
    } catch (const std::exception& e) {
        spdlog::error("[JwtValidator] Ed25519 JWK to PEM conversion error: {}", e.what());
        return "";
    }
}

std::string JwtValidator::base64url_decode(const std::string& input) {
    // Convert base64url to standard base64
    std::string b64 = input;
    
    // Replace URL-safe characters
    std::replace(b64.begin(), b64.end(), '-', '+');
    std::replace(b64.begin(), b64.end(), '_', '/');
    
    // Add padding if needed
    while (b64.size() % 4 != 0) {
        b64 += '=';
    }
    
    // Decode base64
    BIO* bio = BIO_new_mem_buf(b64.data(), static_cast<int>(b64.size()));
    BIO* b64_bio = BIO_new(BIO_f_base64());
    BIO_set_flags(b64_bio, BIO_FLAGS_BASE64_NO_NL);
    bio = BIO_push(b64_bio, bio);
    
    std::vector<char> buffer(b64.size());
    int decoded_len = BIO_read(bio, buffer.data(), static_cast<int>(buffer.size()));
    
    BIO_free_all(bio);
    
    if (decoded_len < 0) {
        return "";
    }
    
    return std::string(buffer.data(), decoded_len);
}

int64_t JwtValidator::current_timestamp() {
    return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

} // namespace auth
} // namespace queen

