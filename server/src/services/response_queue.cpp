#include "queen/response_queue.hpp"
#include <App.h>
#include <random>
#include <sstream>
#include <iomanip>

namespace queen {

std::string ResponseRegistry::generate_uuid() const {
    // Simple UUID generation for request IDs
    static thread_local std::random_device rd;
    static thread_local std::mt19937 gen(rd());
    static thread_local std::uniform_int_distribution<> dis(0, 15);
    static thread_local std::uniform_int_distribution<> dis2(8, 11);
    
    std::stringstream ss;
    ss << std::hex;
    for (int i = 0; i < 8; i++) {
        ss << dis(gen);
    }
    ss << "-";
    for (int i = 0; i < 4; i++) {
        ss << dis(gen);
    }
    ss << "-4";
    for (int i = 0; i < 3; i++) {
        ss << dis(gen);
    }
    ss << "-";
    ss << dis2(gen);
    for (int i = 0; i < 3; i++) {
        ss << dis(gen);
    }
    ss << "-";
    for (int i = 0; i < 12; i++) {
        ss << dis(gen);
    }
    return ss.str();
}

std::string ResponseRegistry::register_response(uWS::HttpResponse<false>* res, int worker_id, 
                                               AbortCallback on_abort) {
    std::string request_id = generate_uuid();
    auto entry = std::make_shared<ResponseEntry>(res, worker_id);
    entry->on_abort = on_abort;
    
    // Set up abort handler to mark response as invalid AND remove from registry immediately
    // Capture this, request_id by value, and entry by shared_ptr
    res->onAborted([this, request_id, entry]() {
        // Mark entry as invalid first
        {
            std::lock_guard<std::mutex> lock(entry->mutex);
            entry->valid = false;
            entry->response = nullptr;
        }
        
        // Call user-provided abort callback (e.g., to remove intention from registry)
        if (entry->on_abort) {
            try {
                entry->on_abort(request_id);
            } catch (const std::exception& e) {
                spdlog::error("Exception in abort callback for {}: {}", request_id, e.what());
            }
        }
        
        // Remove from registry immediately to prevent memory leak
        {
            std::lock_guard<std::mutex> lock(registry_mutex_);
            auto it = responses_.find(request_id);
            if (it != responses_.end()) {
                responses_.erase(it);
                spdlog::debug("Response {} aborted and removed from registry", request_id);
            }
        }
    });
    
    {
        std::lock_guard<std::mutex> lock(registry_mutex_);
        responses_[request_id] = entry;
    }
    
    spdlog::debug("Registered response with ID: {} (worker {})", request_id, worker_id);
    return request_id;
}


bool ResponseRegistry::send_response(const std::string& request_id, const nlohmann::json& data,
                                   [[maybe_unused]] bool is_error, int status_code) {
    // Null/empty -> 204-style empty body; otherwise serialize once and deliver.
    std::string body;
    if (!(data.is_null() || data.empty())) {
        body = data.dump(-1, ' ', false, nlohmann::json::error_handler_t::ignore);
    }
    return deliver_string(request_id, std::move(body), status_code);
}

bool ResponseRegistry::send_response_raw(const std::string& request_id, std::string json_body,
                                         int status_code) {
    return deliver_string(request_id, std::move(json_body), status_code);
}

bool ResponseRegistry::deliver_string(const std::string& request_id, std::string json_str,
                                      int status_code) {
    std::shared_ptr<ResponseEntry> entry;

    // Find and remove from registry
    {
        std::lock_guard<std::mutex> lock(registry_mutex_);
        auto it = responses_.find(request_id);
        if (it == responses_.end()) {
            spdlog::debug("Response ID {} not found in registry", request_id);
            return false;
        }
        entry = it->second;
        responses_.erase(it);
    }

    // Lock the entry to ensure it's still valid before we start sending.
    // The lock is released when 'lock' goes out of scope.
    std::lock_guard<std::mutex> lock(entry->mutex);
    if (!entry->valid || !entry->response) {
        spdlog::debug("Response {} was already aborted or invalid before sending", request_id);
        return false;
    }

    try {
        // Set headers and status code
        entry->response->writeHeader("Access-Control-Allow-Origin", "*");
        entry->response->writeHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
        entry->response->writeHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
        entry->response->writeHeader("Content-Type", "application/json");
        entry->response->writeStatus(std::to_string(status_code));

        // Handle empty responses (like 204 No Content)
        if (json_str.empty()) {
            entry->response->end();
            entry->response = nullptr;
            entry->valid = false;
            return true;
        }

        // Log warning for very large responses
        if (json_str.length() > 10 * 1024 * 1024) { // 10MB threshold
            spdlog::warn("Very large response being sent: {} bytes for request {}", json_str.length(), request_id);
        }

        // For small payloads, send directly and we are done.
        if (json_str.length() <= 64 * 1024) {
            entry->response->end(json_str);
            entry->response = nullptr;
            entry->valid = false;
            spdlog::debug("Successfully sent small response for ID: {}", request_id);
            return true;
        }

        // --- RE-CORRECTED LARGE PAYLOAD STREAMING LOGIC ---

        // 1. Create shared state for the asynchronous operation.
        // We move the payload into this state to keep it alive.
        struct ResponseData {
            std::string payload;
            size_t offset = 0;
            // Keep a reference to the entry to manage its state
            std::shared_ptr<ResponseEntry> entry_ref;
        };
        auto responseData = std::make_shared<ResponseData>();
        responseData->payload = std::move(json_str);
        responseData->entry_ref = entry;

        const size_t totalSize = responseData->payload.length();

        // 2. Define the onWritable (backpressure) handler.
        // This lambda will ONLY be called by the event loop when the socket
        // transitions from non-writable to writable.
        auto writer = [responseData, totalSize](uWS::HttpResponse<false>* res, int last_offset) mutable -> bool {
            // Check if the response was aborted while we were waiting
            if (!responseData->entry_ref->valid) {
                return false; // Unregister
            }

            responseData->offset = last_offset; // Sync offset from where we left off
            bool finished = false;

            // We are in onWritable, so cork() is SAFE.
            res->cork([&]() {
                const size_t chunkSize = 64 * 1024;
                
                // Write in a loop until we hit backpressure *again*
                while (responseData->offset < totalSize) {
                    size_t remaining = totalSize - responseData->offset;
                    size_t currentChunkSize = std::min(chunkSize, remaining);
                    std::string_view chunk(responseData->payload.data() + responseData->offset, currentChunkSize);

                    auto [ok, done] = res->tryEnd(chunk, totalSize);

                    if (done) {
                        finished = true;
                        responseData->entry_ref->response = nullptr;
                        responseData->entry_ref->valid = false;
                        return; // Exit cork lambda
                    }

                    if (!ok) {
                        // Hit backpressure again. Stop and wait for the *next* onWritable.
                        return; // Exit cork lambda
                    }

                    // Chunk sent, update offset and continue the loop
                    responseData->offset += currentChunkSize; 
                }
            });

            return !finished; // Keep handler registered if not finished
        };

        // 3. Register the onWritable handler *before* the first write attempt.
        entry->response->onWritable([writer, res = entry->response](int offset) mutable {
            return writer(res, offset);
        });

        // 4. Perform the *initial* write loop.
        // We do NOT cork here. We write chunks until we either finish
        // or hit backpressure.
        const size_t chunkSize = 64 * 1024;

        while (responseData->offset < totalSize) {
            // Check for abort *before* writing
            if (!entry->valid) {
                return false; // Aborted during initial write loop
            }

            size_t remaining = totalSize - responseData->offset;
            size_t currentChunkSize = std::min(chunkSize, remaining);
            std::string_view chunk(responseData->payload.data() + responseData->offset, currentChunkSize);

            auto [ok, done] = entry->response->tryEnd(chunk, totalSize);

            if (done) {
                // Success! The entire response was sent in the initial loop.
                spdlog::debug("Successfully sent large response for ID {} in initial write loop", request_id);
                entry->response = nullptr;
                entry->valid = false;
                // The onWritable handler will just be discarded, never called.
                return true; // We are finished.
            }

            if (!ok) {
                // Hit backpressure. Stop the loop.
                // The onWritable handler we registered in step 3 will now
                // automatically take over when the socket is ready.
                spdlog::debug("Started streaming large response for ID {} (hit backpressure)", request_id);
                return true; // The stream is now managed by the event loop.
            }

            // Chunk was sent successfully (ok=true), update offset and continue the loop
            responseData->offset += currentChunkSize;
        }

        // We should only exit the loop via 'done' or 'ok=false' (backpressure)
        // This code path should ideally not be reachable.
        spdlog::debug("Large response for ID {} finished initial loop (unexpected exit)", request_id);
        entry->response = nullptr;
        entry->valid = false;
        return true;

    } catch (const std::exception& e) {
        spdlog::error("Error sending response for ID {}: {}", request_id, e.what());
        // Ensure entry is invalidated on error
        entry->response = nullptr;
        entry->valid = false;
        return false;
    }
}

bool ResponseRegistry::is_valid(const std::string& request_id) const {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    auto it = responses_.find(request_id);
    if (it == responses_.end()) {
        return false;  // Not found = aborted or expired
    }
    return it->second->valid.load();
}

void ResponseRegistry::cleanup_expired(std::chrono::milliseconds max_age) {
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(registry_mutex_);
    
    for (auto it = responses_.begin(); it != responses_.end();) {
        if (now - it->second->created_at > max_age) {
            spdlog::debug("Cleaning up expired response: {}", it->first);
            it = responses_.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace queen
