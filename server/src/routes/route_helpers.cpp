#include "queen/routes/route_helpers.hpp"
#include <spdlog/spdlog.h>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <filesystem>
#include <regex>
#include <algorithm>

namespace queen {
namespace routes {

void setup_cors_headers(uWS::HttpResponse<false>* res) {
    res->writeHeader("Access-Control-Allow-Origin", "*");
    res->writeHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
    res->writeHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    res->writeHeader("Access-Control-Max-Age", "86400");
}

void send_json_response(uWS::HttpResponse<false>* res, const nlohmann::json& json, int status_code) {
    // Status must be written BEFORE headers in uWebSockets
    std::string status_text;
    switch (status_code) {
        case 200: status_text = "200 OK"; break;
        case 201: status_text = "201 Created"; break;
        case 204: status_text = "204 No Content"; break;
        case 400: status_text = "400 Bad Request"; break;
        case 401: status_text = "401 Unauthorized"; break;
        case 403: status_text = "403 Forbidden"; break;
        case 404: status_text = "404 Not Found"; break;
        case 409: status_text = "409 Conflict"; break;
        case 500: status_text = "500 Internal Server Error"; break;
        case 503: status_text = "503 Service Unavailable"; break;
        default: status_text = std::to_string(status_code); break;
    }
    res->writeStatus(status_text);
    setup_cors_headers(res);
    res->writeHeader("Content-Type", "application/json");
    res->end(json.dump());
}

void send_error_response(uWS::HttpResponse<false>* res, const std::string& error, int status_code) {
    nlohmann::json error_json = {{"error", error}};
    send_json_response(res, error_json, status_code);
}

void read_json_body(uWS::HttpResponse<false>* res,
                    std::function<void(const nlohmann::json&)> callback,
                    std::function<void(const std::string&)> error_callback) {
    auto buffer = std::make_shared<std::string>();
    auto completed = std::make_shared<bool>(false);
    
    res->onData([callback, error_callback, buffer, completed](std::string_view chunk, bool is_last) {
        buffer->append(chunk);
        
        if (is_last && !*completed) {
            *completed = true;
            try {
                if (buffer->empty()) {
                    error_callback("Empty request body");
                } else {
                    nlohmann::json json = nlohmann::json::parse(*buffer);
                    callback(json);
                }
            } catch (const std::exception& e) {
                error_callback("Invalid JSON: " + std::string(e.what()));
            }
        }
    });
    
    res->onAborted([completed]() {
        *completed = true;
    });
}

std::string url_decode(const std::string& str) {
    std::string result;
    result.reserve(str.length());
    for (size_t i = 0; i < str.length(); ++i) {
        if (str[i] == '%' && i + 2 < str.length()) {
            int value;
            std::istringstream is(str.substr(i + 1, 2));
            if (is >> std::hex >> value) {
                result += static_cast<char>(value);
                i += 2;
            } else {
                result += str[i];
            }
        } else if (str[i] == '+') {
            result += ' ';
        } else {
            result += str[i];
        }
    }
    return result;
}

std::string get_query_param(uWS::HttpRequest* req, const std::string& key, const std::string& default_value) {
    std::string query = std::string(req->getQuery());
    size_t pos = query.find(key + "=");
    if (pos == std::string::npos) return default_value;
    pos += key.length() + 1;
    size_t end = query.find('&', pos);
    if (end == std::string::npos) end = query.length();
    return url_decode(query.substr(pos, end - pos));
}

int get_query_param_int(uWS::HttpRequest* req, const std::string& key, int default_value) {
    std::string value = get_query_param(req, key);
    if (value.empty()) return default_value;
    try {
        return std::stoi(value);
    } catch (...) {
        return default_value;
    }
}

bool get_query_param_bool(uWS::HttpRequest* req, const std::string& key, bool default_value) {
    std::string value = get_query_param(req, key);
    if (value.empty()) return default_value;
    return value == "true" || value == "1";
}

std::string get_mime_type(const std::string& file_path) {
    std::filesystem::path p(file_path);
    std::string ext = p.extension().string();
    std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
    
    static const std::map<std::string, std::string> mime_types = {
        {".html", "text/html; charset=utf-8"},
        {".htm", "text/html; charset=utf-8"},
        {".js", "application/javascript; charset=utf-8"},
        {".mjs", "application/javascript; charset=utf-8"},
        {".css", "text/css; charset=utf-8"},
        {".json", "application/json; charset=utf-8"},
        {".png", "image/png"},
        {".jpg", "image/jpeg"},
        {".jpeg", "image/jpeg"},
        {".gif", "image/gif"},
        {".svg", "image/svg+xml"},
        {".ico", "image/x-icon"},
        {".woff", "font/woff"},
        {".woff2", "font/woff2"},
        {".ttf", "font/ttf"},
        {".eot", "application/vnd.ms-fontobject"},
        {".webp", "image/webp"},
        {".wasm", "application/wasm"}
    };
    
    auto it = mime_types.find(ext);
    return it != mime_types.end() ? it->second : "application/octet-stream";
}

bool serve_static_file(uWS::HttpResponse<false>* res, 
                       const std::string& file_path,
                       const std::string& webapp_root) {
    try {
        // Convert to absolute paths first
        std::filesystem::path abs_file_path = std::filesystem::absolute(file_path);
        std::filesystem::path abs_webapp_root = std::filesystem::absolute(webapp_root);
        
        // Check if file exists first (canonical throws if file doesn't exist)
        if (!std::filesystem::exists(abs_file_path)) {
            return false; // File not found
        }
        
        if (!std::filesystem::is_regular_file(abs_file_path)) {
            return false; // Not a file
        }
        
        // Now safe to get canonical path for security check
        std::filesystem::path normalized_path = std::filesystem::canonical(abs_file_path);
        std::filesystem::path root_path = std::filesystem::canonical(abs_webapp_root);
        
        // Security check - ensure normalized path is within webapp root
        auto normalized_str = normalized_path.string();
        auto root_str = root_path.string();
        if (normalized_str.substr(0, root_str.length()) != root_str) {
            spdlog::warn("Directory traversal attempt blocked: {}", file_path);
            res->writeStatus("403");
            res->end("Forbidden");
            return true;
        }
        
        // Read file content
        std::ifstream file(normalized_path, std::ios::binary);
        if (!file) {
            return false;
        }
        
        // Get file size
        file.seekg(0, std::ios::end);
        size_t file_size = file.tellg();
        file.seekg(0, std::ios::beg);
        
        // Read into string
        std::string content(file_size, '\0');
        file.read(&content[0], file_size);
        file.close();
        
        // Determine MIME type
        std::string mime_type = get_mime_type(file_path);
        
        // Determine cache strategy
        std::string cache_control;
        std::string file_name = normalized_path.filename().string();
        
        // Files served from /assets/ are always Vite-hashed and immutable by
        // construction. Use the canonical path to detect this so the SPA
        // fallback (which probes for a file before falling back to index.html)
        // still works correctly.
        bool under_assets = normalized_str.find(root_str + "/assets/") == 0
                         || normalized_str.find(root_str + "\\assets\\") == 0;

        // Legacy fingerprint pattern (name.<hex>.{js,css}) used by some
        // pipelines that hash outside /assets/.
        std::regex fingerprint_regex(R"(.*\.[a-f0-9]{8,}\.(js|css)$)");

        if (under_assets || std::regex_match(file_name, fingerprint_regex)) {
            cache_control = "public, max-age=31536000, immutable";
        }
        // index.html - never cache
        else if (file_name == "index.html") {
            cache_control = "no-cache, no-store, must-revalidate";
        }
        // Other files - cache for 1 hour
        else {
            cache_control = "public, max-age=3600";
        }
        
        // Send response
        setup_cors_headers(res);
        res->writeHeader("Content-Type", mime_type);
        res->writeHeader("Cache-Control", cache_control);
        res->writeStatus("200");
        res->end(content);  // uWebSockets automatically sets Content-Length
        
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Error serving static file {}: {}", file_path, e.what());
        res->writeStatus("500");
        res->end("Internal Server Error");
        return true;
    }
}

std::pair<std::string, std::string> parse_subscription_mode(
    const std::string& sub_mode,
    const std::string& sub_from,
    const std::string& default_sub_mode) {
    
    std::string effective_sub_mode = sub_mode.empty() ? default_sub_mode : sub_mode;
    std::string subscription_mode_value;
    std::string subscription_timestamp_sql;
    
    // Check subscriptionFrom parameter first (explicit user preference takes precedence)
    if (!sub_from.empty() && sub_from != "now") {
        subscription_mode_value = "timestamp";
        subscription_timestamp_sql = "'" + sub_from + "'::timestamptz";
    } else if (effective_sub_mode == "new" || effective_sub_mode == "new-only" || sub_from == "now") {
        subscription_mode_value = "new";
        subscription_timestamp_sql = "NOW()";
    } else {
        subscription_mode_value = "all";
        subscription_timestamp_sql = "'1970-01-01 00:00:00+00'";
    }
    
    return {subscription_mode_value, subscription_timestamp_sql};
}

std::string format_timestamp_iso8601(const std::chrono::system_clock::time_point& tp) {
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        tp.time_since_epoch()) % 1000;
    
    std::stringstream ss;
    ss << std::put_time(std::gmtime(&time_t), "%Y-%m-%dT%H:%M:%S");
    ss << '.' << std::setfill('0') << std::setw(3) << ms.count() << 'Z';
    
    return ss.str();
}

} // namespace routes
} // namespace queen

