/**
 *
 *           Message Queue Client - C++ Implementation            
 *                 Single Header Library                          
 *                                                                
 * 
 * Features:
 * - Fluent API matching Node.js client
 * - HTTP client with retry and failover
 * - Bearer-token auth, 429 backoff and terminal 403 codes (proxy contract)
 * - Load balancing (round-robin & session)
 * - Client-side buffering with time/count triggers, a blocking max_size
 *   bound, and lossless flush retry (1.0.6 buffer contract)
 * - Consumer groups and partitions
 * - Atomic transactions
 * - Lease renewal
 * - Dead Letter Queue (DLQ) queries
 * - Concurrent consumers using astp::ThreadPool
 * - Graceful shutdown
 * 
 * Dependencies:
 * - C++17 or later
 * - nlohmann/json
 * - cpp-httplib (header-only HTTP client)
 * - astp::ThreadPool
 * 
 * Usage:
 *   #include "queen_client.hpp"
 *   
 *   queen::QueenClient client("http://localhost:6632");
 *   client.queue("tasks").create();
 *   client.queue("tasks").push({{"data", {{"job", "test"}}}}});
 *   client.queue("tasks").consume([](const json& msg) {
 *       std::cout << "Processing: " << msg << std::endl;
 *   });
 *   client.close();
 */

#pragma once

#include <string>
#include <vector>
#include <deque>
#include <cstdint>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <functional>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <sstream>
#include <iomanip>
#include <random>
#include <stdexcept>
#include <iostream>
#include <optional>
#include <cctype>
#include <cstdlib>
#include <limits>
#include <regex>
#include <future>
#include <csignal>

// JSON library (nlohmann)
#include <json.hpp>

// HTTP library
#ifndef CPPHTTPLIB_OPENSSL_SUPPORT
#define CPPHTTPLIB_OPENSSL_SUPPORT
#endif
#include <httplib.h>

// ThreadPool
#include "../server/include/threadpool.hpp"

namespace queen {

using json = nlohmann::json;

// ============================================================================
// Forward Declarations
// ============================================================================

class QueenClient;
class QueueBuilder;
class TransactionBuilder;
class HttpClient;
class LoadBalancer;
class BufferManager;
class MessageBuffer;
class ConsumerManager;

// ============================================================================
// Configuration Structures
// ============================================================================

/**
 * Which 429 backoff policy applies to a request.
 * Pop marks a long-poll pop (wait=true): the outer poll loop is already an
 * indefinite wait, so a 429 there is waited out rather than surfaced as a
 * failure after a handful of tries.
 */
enum class RetryKind {
    Default,
    Pop
};

/**
 * Backoff policy applied when the server returns HTTP 429 (rate limited),
 * separate from the 5xx/network retry_attempts above. Zero-value fields fall
 * back to kind-based defaults:
 * - max_attempts: bounded (10 total attempts) for ordinary requests (push,
 *   admin calls, non-waiting pop); unbounded for a long-poll pop. Setting it
 *   explicitly applies the same bound to both kinds.
 * - base_millis: initial backoff absent a Retry-After header (default 500).
 * - cap_millis: ceiling for the exponential backoff (default 30000).
 * A Retry-After header (seconds) always wins over the computed delay.
 * See PLAN_QUEEN_PROXY_CLOUD.md §4/§9 (client 429 backoff, blocker B4).
 */
struct Retry429Options {
    int max_attempts = 0;                    // 0 = kind-based default
    int base_millis = 0;                     // 0 = 500
    int cap_millis = 0;                      // 0 = 30000
};

struct ClientConfig {
    std::vector<std::string> urls;
    int timeout_millis = 30000;
    int retry_attempts = 3;
    int retry_delay_millis = 1000;
    std::string load_balancing_strategy = "round-robin"; // "round-robin" or "session"
    bool enable_failover = true;
    std::string bearer_token;                // sent as "Authorization: Bearer <token>"
    Retry429Options retry_429;
};

struct QueueConfig {
    int lease_time = 300;                    // seconds
    int retry_limit = 3;
    int priority = 0;
    int delayed_processing = 0;              // seconds
    int window_buffer = 0;                   // seconds
    int max_size = 0;
    int retention_seconds = 0;
    int completed_retention_seconds = 0;
    bool encryption_enabled = false;
    
    json to_json() const {
        return {
            {"leaseTime", lease_time},
            {"retryLimit", retry_limit},
            {"priority", priority},
            {"delayedProcessing", delayed_processing},
            {"windowBuffer", window_buffer},
            {"maxSize", max_size},
            {"retentionSeconds", retention_seconds},
            {"completedRetentionSeconds", completed_retention_seconds},
            {"encryptionEnabled", encryption_enabled}
        };
    }
};

struct ConsumeOptions {
    std::string queue;
    std::string partition;
    std::string namespace_name;
    std::string task;
    std::string group;
    int concurrency = 1;
    /**
     * The pop's message budget. 0 means UNSET -- the dimension pop autopilot
     * lets the broker choose, from state no client can see. It used to mean the
     * client-side default of 1, which is now applied only when autopilot is off,
     * so that "never set a batch" and "set a batch of 1" stay distinguishable
     * all the way to the wire. See util::pop_sizing.
     */
    int batch = 0;
    int limit = 0;                           // 0 = unlimited
    int idle_millis = 0;                     // 0 = no idle timeout
    bool auto_ack = true;
    bool wait = true;
    int timeout_millis = 30000;
    bool renew_lease = false;
    int renew_lease_interval_millis = 60000;
    std::string subscription_mode;
    std::string subscription_from;
    bool each = false;
    /// v4 multi-partition pop cap. 0 = unset, as for `batch` above.
    int max_partitions = 0;
    /**
     * Broker-side pop sizing. Unset takes the client-wide default (on, unless
     * QUEEN_SDK_POP_AUTOPILOT turned it off at construction); false restores the
     * pre-1.2 client-side defaults byte for byte.
     */
    std::optional<bool> autopilot;
    /**
     * Last-value delivery for this consumer group on this queue
     * (PLAN_CONFLATION.md §1.1): under backlog the group is served ONE message
     * per partition, the newest, instead of the whole backlog.
     *
     * It is a property of the GROUP, not of the call: the first pop that
     * registers the group persists it, and from then on the stored value wins
     * for every consumer of that group. Declaring the opposite of what is stored
     * does not flip anything -- it warns (§3.3).
     *
     * Requires a broker >= 1.1.0. An older one ignores the flag, which this
     * client detects on the first response and refuses to continue past (§4);
     * see ConflationUnsupportedError.
     */
    bool conflation = false;
    std::atomic<bool>* stop_signal = nullptr;
};

struct BufferOptions {
    int message_count = 100;
    int time_millis = 1000;
    /**
     * Backpressure bound: once this many messages are waiting, a buffered
     * push() BLOCKS until the flusher drains below the bound, instead of
     * growing the heap. 0 (or negative) means the DEFAULT bound of
     * 4 * message_count -- "unbounded" is deliberately not expressible,
     * because unbounded was the defect (measured on the Go client: 20.9M
     * messages / 11.7 GB accumulated in 45s and all lost at exit, with zero
     * client-side errors). Floored at message_count: a buffer that must block
     * before it can assemble one batch would deadlock against its own flush
     * threshold. The bound is approximate: a batch that fails to send is put
     * back at the front, so occupancy can overshoot by up to one batch.
     */
    int max_size = 0;
    /**
     * How long the flusher waits before retrying a batch whose POST failed.
     * The batch is re-queued at the FRONT of the buffer, in order, and retried
     * until it lands, the buffer is stopped, or an explicit flush deadline
     * expires -- never dropped. 0 (or negative) means the default of 250ms.
     */
    int retry_delay_millis = 0;
};

/**
 * Fill in defaults and enforce the bound's invariants, mirroring the 1.0.6
 * buffer contract of the JS/Python/Go/PHP SDKs. The resolved options are what
 * a MessageBuffer actually runs with; the raw struct keeps 0 = "not set" so
 * the default bound can follow the caller's message_count (a caller asking for
 * message_count 1000 gets a 4000 bound, not the 400 that suits the default
 * batch of 100).
 */
inline BufferOptions resolve_buffer_options(const BufferOptions& options) {
    BufferOptions resolved = options;
    if (resolved.message_count <= 0) resolved.message_count = 100;
    if (resolved.time_millis <= 0) resolved.time_millis = 1000;
    if (resolved.max_size <= 0) resolved.max_size = 4 * resolved.message_count;
    if (resolved.max_size < resolved.message_count) resolved.max_size = resolved.message_count;
    if (resolved.retry_delay_millis <= 0) resolved.retry_delay_millis = 250;
    return resolved;
}

// ============================================================================
// Errors
// ============================================================================

/**
 * Thrown for any HTTP response with a >= 400 status. Derives from
 * std::runtime_error and keeps what() equal to the server's "error" message,
 * so existing catch (const std::exception&) sites are unaffected; the extra
 * accessors let callers branch on the proxy contract without string matching:
 *
 *   429  Retry-After: <seconds>  {"error", "code": "rate_limited" | "quota_exceeded"}
 *   403                          {"error", "code": "cluster_suspended" | "storage_quota_exceeded"
 *                                                | "feature_gated" | "forbidden"}
 *
 * code() is empty when the body carries no such field (e.g. a broker that
 * predates the proxy contract). Transport failures (no response at all) still
 * surface as a plain std::runtime_error.
 */
class HttpError : public std::runtime_error {
private:
    int status_code_;
    std::string body_;
    std::string code_;
    std::optional<double> retry_after_seconds_;

public:
    HttpError(int status_code, const std::string& message, const std::string& body,
              const std::string& code, std::optional<double> retry_after_seconds)
        : std::runtime_error(message), status_code_(status_code), body_(body),
          code_(code), retry_after_seconds_(retry_after_seconds) {
    }

    int status_code() const { return status_code_; }
    const std::string& body() const { return body_; }
    const std::string& code() const { return code_; }

    /** Retry-After (seconds) from a 429; empty when absent or non-numeric. */
    std::optional<double> retry_after_seconds() const { return retry_after_seconds_; }

    /**
     * Terminal cluster_suspended 403: nothing short of operator intervention
     * resolves it, so consumer loops must stop rather than back off and retry.
     * The other 403 codes are equally non-retryable but are surfaced through
     * code() instead of being named here.
     */
    bool is_cluster_suspended() const {
        return status_code_ == 403 && code_ == "cluster_suspended";
    }
};

/**
 * An explicit buffer flush ran out of deadline with messages still unsent.
 *
 * The flusher retries a failed batch forever by default -- the right behavior
 * for a background flush, and the wrong one for a shutdown path with a SIGTERM
 * grace period to respect -- so flush_buffer()/flush_all_buffers() accept a
 * deadline, and this is what expiring it throws. The messages are STILL IN THE
 * BUFFER when this is raised (nothing was dropped; the failed batch went back
 * to the front): the caller learns exactly how many were not accepted and can
 * retry, persist them, or fail loudly. what() carries the transport error plus
 * the count, unflushed_count() carries the number alone.
 *
 * Derives from std::runtime_error so existing catch (const std::exception&)
 * sites are unaffected.
 */
class BufferFlushError : public std::runtime_error {
private:
    std::string queue_address_;
    int unflushed_count_;

public:
    BufferFlushError(const std::string& message, const std::string& queue_address,
                     int unflushed_count)
        : std::runtime_error(message), queue_address_(queue_address),
          unflushed_count_(unflushed_count) {
    }

    const std::string& queue_address() const { return queue_address_; }
    int unflushed_count() const { return unflushed_count_; }
};

/**
 * Conflation was requested and the broker did not apply it
 * (PLAN_CONFLATION.md §4, the degrade-loudly rule).
 *
 * There is no version or capability negotiation anywhere in this SDK, so a
 * client that sends `conflation=true` to a broker predating 1.1.0 gets the
 * unknown query param ignored and the WHOLE BACKLOG delivered, message by
 * message, with nothing said. That is the one failure mode conflation cannot
 * have: the feature exists to process the newest state and nothing else.
 *
 * The evidence is the response itself. A 1.1.0 broker echoes `"conflation":true`
 * on every conflating response -- empty ones included, which is why a conflating
 * pop answers 200-with-a-body where a plain one answers 204 -- so the absence of
 * that echo is proof, and it is available on the FIRST round trip, before a
 * single message has been handled. This is an error and not a warning: `pop()`
 * throws it instead of returning its messages, and a consume loop stops.
 *
 * A `"conflationConflict":true` response is NOT this error. That key can only
 * come from a broker that speaks conflation; it means the stored group policy
 * disagreed with the request and won (§3.3), which is a warning, once per
 * (queue, group) -- rejecting there is what would break a rolling deploy.
 *
 * Derives from std::runtime_error, so a `catch (const std::exception&)` site
 * still sees it; catch this type to tell "your broker is too old" apart from a
 * transport failure.
 */
class ConflationUnsupportedError : public std::runtime_error {
private:
    std::string queue_;
    std::string group_;

public:
    ConflationUnsupportedError(const std::string& queue, const std::string& group)
        // The message opens with the canonical wording from PLAN_CONFLATION §4,
        // byte-identical across all seven SDKs so one grep finds every language;
        // the queue and group are appended because in C++ the accessors below
        // are rarely reached and what() is what ends up in the log.
        : std::runtime_error(
              "conflation was requested but this broker did not apply it \xE2\x80\x94 "
              "requires broker >= 1.1.0 (queue '" + queue + "', consumer group '" +
              group + "')"),
          queue_(queue), group_(group) {
    }

    const std::string& queue() const { return queue_; }
    const std::string& group() const { return group_; }
};

/**
 * The broker, or the proxy in front of it, is older than 1.1 and has no
 * ephemeral routes at all (EPHEMERAL_QUEUES.md §4, §8).
 *
 * No SDK in this product negotiates capabilities, so there is nothing to probe
 * and nothing to fall back to: the WHOLE /api/v1/ephemeral/* family answers 404
 * -- the broker because the routes were never registered, the proxy because an
 * unknown API path is `route_blocked` and it fails closed. Both are one verdict,
 * "upgrade".
 *
 * Exactly one 404 on this family means something else, which is why the mapping
 * reads the body's CODE and not the status the two share: see
 * EphemeralQueueNotFoundError.
 *
 * It derives from HttpError rather than std::runtime_error on purpose. A 404 IS
 * an HTTP refusal, so every existing `catch (const HttpError&)` around a push or
 * a pop keeps catching this one; what the distinct type buys is telling "your
 * broker is too old" apart from any other refusal without matching on prose.
 * `code()` is `ephemeral_unsupported` and `original_code()` is whatever the peer
 * sent -- empty from a broker, `route_blocked` from a proxy -- because that is
 * the evidence for the claim this exception makes.
 */
class EphemeralUnsupportedError : public HttpError {
private:
    std::string original_code_;

public:
    /** Verbatim across every SDK: operators grep this string. */
    static constexpr const char* MESSAGE =
        "broker/proxy does not support ephemeral queues (requires >= 1.1)";

    explicit EphemeralUnsupportedError(const HttpError& cause)
        : HttpError(cause.status_code(), MESSAGE, cause.body(), "ephemeral_unsupported",
                    cause.retry_after_seconds()),
          original_code_(cause.code()) {
    }

    /** The peer's own code, kept so the two 404s stay distinguishable in a log. */
    const std::string& original_code() const { return original_code_; }
};

/**
 * `depth` named an ephemeral queue that is not there
 * (EPHEMERAL_QUEUES.md §3.1).
 *
 * The ONLY verb of the family that can say this, and that is worth knowing
 * rather than discovering: push and pop create a queue by naming it, `reset`
 * answers dropped:0 and `delete` answers deleted:false. So this is a real DATA
 * fact -- a queue name typo, or a ring that was empty and idle long enough to be
 * collected -- and not the DEPLOYMENT fact EphemeralUnsupportedError states.
 * Collapsing the two would send somebody chasing a broker version over a queue
 * name.
 *
 * Same shape as its sibling and deliberately BESIDE it rather than under it, so
 * that `catch (const EphemeralUnsupportedError&)` never catches a missing queue:
 * both derive from HttpError, so a 404 stays an HTTP refusal and every existing
 * `catch (const HttpError&)` around a depth call keeps catching it. `code()` is
 * CODE, the broker's own code string unchanged, and `queue()` names the queue
 * that was not found -- the body of the original 404 stays reachable through
 * body(), because that answer is the evidence for the claim this exception
 * makes.
 */
class EphemeralQueueNotFoundError : public HttpError {
private:
    std::string queue_;

    static std::string message_for(const std::string& queue) {
        return queue.empty() ? std::string("ephemeral: that queue does not exist")
                             : "ephemeral: queue \"" + queue + "\" does not exist";
    }

public:
    /**
     * The broker's own code, kept identical across every SDK (Go
     * ErrEphemeralQueueNotFound, queen_protocol::EPHEMERAL_QUEUE_NOT_FOUND_CODE)
     * so a code seen in one language's logs means the same thing in the next.
     */
    static constexpr const char* CODE = "ephemeral_queue_not_found";

    EphemeralQueueNotFoundError(const HttpError& cause, const std::string& queue)
        : HttpError(cause.status_code(), message_for(queue), cause.body(), CODE,
                    cause.retry_after_seconds()),
          queue_(queue) {
    }

    /** The queue the caller asked about, so what() is not the only clue. */
    const std::string& queue() const { return queue_; }
};

// ============================================================================
// Utility Functions
// ============================================================================

namespace util {

/**
 * Generate UUIDv7 (time-ordered UUID with millisecond precision)
 * Based on queen::QueueManager::generate_uuid()
 */
inline std::string generate_uuid_v7() {
    static std::mutex uuid_mutex;
    static uint64_t last_ms = 0;
    static uint16_t sequence = 0;
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    
    std::lock_guard<std::mutex> lock(uuid_mutex);
    
    // Get current time in milliseconds since epoch
    auto now = std::chrono::system_clock::now();
    uint64_t current_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    
    // If the clock moved backwards or stayed the same, increment the sequence
    if (current_ms <= last_ms) {
        sequence++;
    } else {
        last_ms = current_ms;
        sequence = 0;
    }
    
    std::array<uint8_t, 16> bytes;
    
    // 48-bit unix_ts_ms (big-endian)
    bytes[0] = (last_ms >> 40) & 0xFF;
    bytes[1] = (last_ms >> 32) & 0xFF;
    bytes[2] = (last_ms >> 24) & 0xFF;
    bytes[3] = (last_ms >> 16) & 0xFF;
    bytes[4] = (last_ms >> 8) & 0xFF;
    bytes[5] = last_ms & 0xFF;
    
    // 4-bit version (0111) and 12-bit sequence
    uint16_t sequence_and_version = sequence & 0x0FFF;
    bytes[6] = 0x70 | (sequence_and_version >> 8);
    bytes[7] = sequence_and_version & 0xFF;
    
    // 2-bit variant (10) and 62-bits of random data
    uint64_t rand_data = gen();
    bytes[8] = 0x80 | ((rand_data >> 56) & 0x3F);
    bytes[9] = (rand_data >> 48) & 0xFF;
    bytes[10] = (rand_data >> 40) & 0xFF;
    bytes[11] = (rand_data >> 32) & 0xFF;
    bytes[12] = (rand_data >> 24) & 0xFF;
    bytes[13] = (rand_data >> 16) & 0xFF;
    bytes[14] = (rand_data >> 8) & 0xFF;
    bytes[15] = rand_data & 0xFF;
    
    // Format to string: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (int i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) ss << '-';
        ss << std::setw(2) << static_cast<int>(bytes[i]);
    }
    
    return ss.str();
}

/**
 * Validate UUID format
 */
inline bool is_valid_uuid(const std::string& str) {
    static std::regex uuid_regex(
        "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        std::regex::icase
    );
    return std::regex_match(str, uuid_regex);
}

/**
 * URL encode string
 */
inline std::string url_encode(const std::string& value) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;
    
    for (char c : value) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
        } else {
            escaped << '%' << std::setw(2) << int((unsigned char) c);
        }
    }
    
    return escaped.str();
}

/**
 * Parse URL into components
 */
inline std::tuple<std::string, std::string, int> parse_url(const std::string& url) {
    std::regex url_regex(R"(^(https?)://([^:/]+)(?::(\d+))?$)");
    std::smatch match;
    
    if (!std::regex_match(url, match, url_regex)) {
        throw std::invalid_argument("Invalid URL format: " + url);
    }
    
    std::string scheme = match[1].str();
    std::string host = match[2].str();
    int port = match[3].str().empty() ? (scheme == "https" ? 443 : 80) : std::stoi(match[3].str());
    
    return {scheme, host, port};
}

/**
 * Get current timestamp in ISO 8601 format
 */
inline std::string get_iso_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    std::stringstream ss;
    ss << std::put_time(std::gmtime(&time_t_now), "%Y-%m-%dT%H:%M:%S");
    ss << '.' << std::setfill('0') << std::setw(3) << ms.count() << 'Z';
    
    return ss.str();
}

/**
 * Delay before the next 429 retry attempt (milliseconds).
 * Honors Retry-After (seconds) when the server sent one, with +-20% jitter to
 * avoid a synchronized thundering herd; otherwise falls back to exponential
 * backoff (base_millis * 2^attempt_index, capped at cap_millis), also jittered.
 */
inline int compute_retry429_delay_millis(int attempt_index,
                                         std::optional<double> retry_after_seconds,
                                         int base_millis, int cap_millis) {
    double delay_millis;

    if (retry_after_seconds.has_value() && *retry_after_seconds >= 0) {
        delay_millis = *retry_after_seconds * 1000.0;
    } else {
        delay_millis = base_millis;
        for (int i = 0; i < attempt_index && delay_millis < cap_millis; ++i) {
            delay_millis *= 2;
        }
        if (delay_millis > cap_millis) {
            delay_millis = cap_millis;
        }
    }

    // Each consumer thread jitters independently.
    static thread_local std::mt19937 jitter_gen(std::random_device{}());
    std::uniform_real_distribution<double> jitter(0.8, 1.2);

    double final_millis = delay_millis * jitter(jitter_gen);
    return final_millis > 0 ? static_cast<int>(final_millis) : 0;
}

/**
 * Logging utility (controlled by QUEEN_CLIENT_LOG env var)
 */
inline bool is_log_enabled() {
    static bool enabled = []() {
        const char* env = std::getenv("QUEEN_CLIENT_LOG");
        return env && std::string(env) == "true";
    }();
    return enabled;
}

inline void log(const std::string& operation, const std::string& details) {
    if (is_log_enabled()) {
        std::cout << "[" << get_iso_timestamp() << "] [INFO] [" << operation << "] " 
                  << details << std::endl;
    }
}

inline void log_warn(const std::string& operation, const std::string& details) {
    if (is_log_enabled()) {
        std::cerr << "[" << get_iso_timestamp() << "] [WARN] [" << operation << "] "
                  << details << std::endl;
    }
}

inline void log_error(const std::string& operation, const std::string& details) {
    if (is_log_enabled()) {
        std::cerr << "[" << get_iso_timestamp() << "] [ERROR] [" << operation << "] "
                  << details << std::endl;
    }
}

/**
 * A warning that is NOT gated behind QUEEN_CLIENT_LOG.
 *
 * Reserved for facts an operator cannot act on if they never see them, and used
 * by exactly one thing today: the conflation declaration conflict
 * (PLAN_CONFLATION.md §3.3). That rule is the plan's deliberate improvement over
 * the precedent it found -- a second consumer declaring a different
 * subscriptionMode is dropped on the floor today with no log line, no counter
 * and no response field (M3) -- so routing its one warning through a channel
 * that is off by default would reproduce exactly the silence it replaces.
 *
 * Rate limiting is the caller's job. The one caller warns once per
 * (queue, group) per process, so this can never flood.
 */
inline void log_warn_always(const std::string& operation, const std::string& details) {
    std::cerr << "[" << get_iso_timestamp() << "] [WARN] [" << operation << "] "
              << details << std::endl;
}

// ---------------------------------------------------------------------------
// Conflation response contract (PLAN_CONFLATION.md §3.1, §4)
// ---------------------------------------------------------------------------

/**
 * True when the response says conflation was actually APPLIED. The broker emits
 * the key only when true and on every conflating response, empty ones included.
 * A JSON null (a 204, or an empty body) is a legitimate input here and answers
 * false: that is precisely the old-broker-with-an-idle-queue shape.
 */
inline bool conflation_applied(const json& response) {
    return response.is_object() && response.contains("conflation") &&
           response["conflation"].is_boolean() && response["conflation"].get<bool>();
}

/**
 * True when the broker reports that the request disagreed with the stored group
 * policy and the STORED one won (§3.3). Only a broker that understands
 * conflation can emit this, which is what separates a conflict from an old
 * broker.
 */
inline bool conflation_conflicted(const json& response) {
    return response.is_object() && response.contains("conflationConflict") &&
           response["conflationConflict"].is_boolean() &&
           response["conflationConflict"].get<bool>();
}

/**
 * Warn once per (queue, group) per process; returns true when this call is the
 * one that warned. Per (queue, GROUP) and not per process: two groups on one
 * queue are two independent policies, and collapsing them would hide the second
 * misconfiguration behind the first.
 */
inline bool warn_conflation_conflict_once(const std::string& queue, const std::string& group) {
    static std::mutex mutex;
    static std::unordered_set<std::string> warned;

    {
        std::lock_guard<std::mutex> lock(mutex);
        // \x1f (unit separator) cannot appear in a queue name that survived URL
        // encoding, so the two halves of the key cannot be confused for one.
        if (!warned.insert(queue + "\x1f" + group).second) {
            return false;
        }
    }

    log_warn_always("conflation",
        "consumer declared conflation but the group does not have it: queue '" + queue +
        "', consumer group '" + group + "'. The STORED group setting wins, so this "
        "consumer is receiving FULL batches. Declare conflation on every consumer of "
        "the group, or recreate the group. This warns once per queue+group.");
    return true;
}

/**
 * What to call this pop's target in a conflation warning. A pop addresses either
 * a queue or a (namespace, task) pair; the label only ever names a log line and
 * keys the once-per-target warning, so any stable spelling of the pair will do.
 */
inline std::string pop_target_label(const std::string& queue, const std::string& namespace_name,
                                    const std::string& task) {
    if (!queue.empty()) return queue;
    if (!namespace_name.empty() && !task.empty()) return namespace_name + "/" + task;
    if (!namespace_name.empty()) return namespace_name;
    return task;
}

/**
 * The whole client-side conflation contract in one place, so `pop()` and
 * `consume()` -- two separate code paths in this SDK, which is the standing
 * hazard §4 opens with -- cannot drift apart on it.
 *
 * No-op unless conflation was requested. Then:
 *   applied  -> nothing to say.
 *   conflict -> the broker speaks conflation and the stored policy won: warn,
 *               once per (queue, group). Not an error -- §3.3/Q3, a reject here
 *               takes down the already-correct half of a rolling deploy.
 *   paused   -> pop maintenance: the request never reached the claim path, so
 *               there is no policy to echo and nothing to conclude from the
 *               absence of one. Not a verdict, not an error.
 *   neither  -> the broker never applied it and never heard of it. Raise (§4).
 */
inline void enforce_conflation_contract(bool requested, const json& response,
                                        const std::string& queue, const std::string& group) {
    if (!requested || conflation_applied(response)) {
        return;
    }
    if (conflation_conflicted(response)) {
        warn_conflation_conflict_once(queue, group);
        return;
    }
    if (response.is_object() && response.value("paused", false)) {
        return;
    }
    throw ConflationUnsupportedError(queue, group);
}

/**
 * Standard base64 (RFC 4648), WITH padding.
 *
 * A timer payload travels base64 on the wire (PLAN_KV_TIMERS.md §4.1) and the
 * broker decodes it with the STANDARD engine. Padding is not cosmetic here: an
 * unpadded encoder produces a 22023 from the stored procedure and nothing in
 * the error names the encoder as the cause.
 *
 * Written out rather than borrowed from httplib::detail, which is a private
 * namespace of a vendored header and has no encoder at all in some releases.
 */
inline std::string base64_encode(const std::string& input) {
    static const char* alphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    std::string out;
    out.reserve(((input.size() + 2) / 3) * 4);

    size_t i = 0;
    while (i + 2 < input.size()) {
        uint32_t chunk = (static_cast<unsigned char>(input[i]) << 16) |
                         (static_cast<unsigned char>(input[i + 1]) << 8) |
                         static_cast<unsigned char>(input[i + 2]);
        out += alphabet[(chunk >> 18) & 0x3F];
        out += alphabet[(chunk >> 12) & 0x3F];
        out += alphabet[(chunk >> 6) & 0x3F];
        out += alphabet[chunk & 0x3F];
        i += 3;
    }

    size_t remaining = input.size() - i;
    if (remaining == 1) {
        uint32_t chunk = static_cast<unsigned char>(input[i]) << 16;
        out += alphabet[(chunk >> 18) & 0x3F];
        out += alphabet[(chunk >> 12) & 0x3F];
        out += "==";
    } else if (remaining == 2) {
        uint32_t chunk = (static_cast<unsigned char>(input[i]) << 16) |
                         (static_cast<unsigned char>(input[i + 1]) << 8);
        out += alphabet[(chunk >> 18) & 0x3F];
        out += alphabet[(chunk >> 12) & 0x3F];
        out += alphabet[(chunk >> 6) & 0x3F];
        out += '=';
    }

    return out;
}

/** Inverse of base64_encode. Throws on anything that is not valid base64. */
inline std::string base64_decode(const std::string& input) {
    auto value_of = [](char c) -> int {
        if (c >= 'A' && c <= 'Z') return c - 'A';
        if (c >= 'a' && c <= 'z') return c - 'a' + 26;
        if (c >= '0' && c <= '9') return c - '0' + 52;
        if (c == '+') return 62;
        if (c == '/') return 63;
        return -1;
    };

    if (input.size() % 4 != 0) {
        throw std::invalid_argument("base64_decode: length is not a multiple of 4");
    }

    std::string out;
    out.reserve((input.size() / 4) * 3);

    for (size_t i = 0; i < input.size(); i += 4) {
        int quad[4];
        int pad = 0;
        for (int k = 0; k < 4; ++k) {
            char c = input[i + k];
            if (c == '=') {
                // Padding is only legal in the last two positions of the last quad.
                if (i + 4 != input.size() || k < 2) {
                    throw std::invalid_argument("base64_decode: misplaced padding");
                }
                quad[k] = 0;
                ++pad;
            } else {
                quad[k] = value_of(c);
                if (quad[k] < 0) {
                    throw std::invalid_argument("base64_decode: invalid character");
                }
                if (pad > 0) {
                    throw std::invalid_argument("base64_decode: data after padding");
                }
            }
        }
        uint32_t chunk = (static_cast<uint32_t>(quad[0]) << 18) |
                         (static_cast<uint32_t>(quad[1]) << 12) |
                         (static_cast<uint32_t>(quad[2]) << 6) |
                         static_cast<uint32_t>(quad[3]);
        out += static_cast<char>((chunk >> 16) & 0xFF);
        if (pad < 2) out += static_cast<char>((chunk >> 8) & 0xFF);
        if (pad < 1) out += static_cast<char>(chunk & 0xFF);
    }

    return out;
}

/**
 * Milliseconds -> ttlSeconds, ROUNDED UP.
 *
 * PLAN_KV_TIMERS.md §20.1, ratified: the KV wire speaks `ttlSeconds` and
 * nothing else, and the comfortable forms (`until: <date>`) are converted to a
 * delta of seconds at send time, rounded UP. Rounded DOWN, a marker expires
 * before the window it was supposed to cover -- which is the one direction that
 * turns an idempotency marker into a duplicate external effect.
 *
 * A non-positive TTL throws here rather than travelling as `ttlSeconds: 0`: the
 * stored procedure would refuse it anyway, and paying a round trip to learn
 * that a deadline is already in the past is not a service to anybody.
 */
inline long long ttl_seconds_from_millis(long long millis) {
    if (millis <= 0) {
        throw std::invalid_argument(
            "ttlSeconds must be greater than zero; a deadline in the past is a caller bug, "
            "and exactly one of ttlSeconds and forever is required on every KV write");
    }
    return (millis + 999) / 1000;
}

// ============================================================================
// Pop autopilot (server/src/pop_autopilot.rs)
// ============================================================================
//
// The broker owns a controller that sizes a pop from state this client cannot
// see: how many partitions of the (queue, group) are ready, how old their
// oldest ready message is, at what rate messages are arriving. Two knobs are
// under its control -- `partitions` (the sweep width) and `batch` (the message
// budget for the sweep).
//
// THE RULE, and it is the only one: an explicit user value is sacred. Autopilot
// applies ONLY to the knobs the user left unset, and it applies to them one by
// one. A consumer that pins partitions(1) and says nothing about batch keeps its
// single-partition claim forever and lets the broker size the batch; the pinned
// dimension is never "adjusted", not even towards a value the controller would
// consider better.
//
// The wire shape follows the conflation precedent above: a client that is not
// engaging autopilot sends the byte-identical request it sent before this
// feature existed. `autopilot=true` is emitted only when at least one knob is
// being left to the broker, never as `autopilot=false`, and the delegated knobs
// are simply omitted.
//
// WHAT AN OLD BROKER DOES, and why there is no capability check. A broker older
// than 1.2 ignores unknown query params: the request succeeds, and the omitted
// knobs fall back to the SERVER-side defaults (batch 200, partitions 1) instead
// of the old client-side ones. That is a sizing difference, not a correctness
// one -- nothing is lost, misordered or delivered twice -- so unlike conflation
// (which silently hands a last-value consumer a whole backlog) this degrades
// quietly and on purpose.

/**
 * The environment variable that disables pop autopilot for a whole process:
 * QUEEN_SDK_POP_AUTOPILOT=off restores the client-side defaults this SDK applied
 * before autopilot existed, byte for byte. It is read once, in the QueenClient
 * constructor, so a single deployment can be rolled back without touching code.
 */
inline constexpr const char* ENV_POP_AUTOPILOT = "QUEEN_SDK_POP_AUTOPILOT";

/**
 * The vocabulary, split from the lookup so it can be tested without mutating a
 * process-wide variable. "off", "false", "0", "no" and "disabled" all disable
 * autopilot (case-insensitive, surrounding space ignored); every other value,
 * including the empty one, leaves it on.
 */
inline bool autopilot_disabled_by_value(const std::string& raw) {
    const auto first = raw.find_first_not_of(" \t\n\r\f\v");
    if (first == std::string::npos) return false;
    const auto last = raw.find_last_not_of(" \t\n\r\f\v");
    std::string v = raw.substr(first, last - first + 1);
    for (char& c : v) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return v == "off" || v == "false" || v == "0" || v == "no" || v == "disabled";
}

inline bool autopilot_disabled_by_env() {
    const char* raw = std::getenv(ENV_POP_AUTOPILOT);
    return raw != nullptr && autopilot_disabled_by_value(raw);
}

/**
 * Which of the three sizing keys travel on one pop, and with what values.
 * `has_batch` / `has_partitions` false means "this key does not travel" -- the
 * dimension the broker is choosing.
 */
struct PopSizing {
    bool autopilot = false;
    bool has_batch = false;
    int batch = 0;
    bool has_partitions = false;
    int partitions = 0;
};

/**
 * The batch/partitions/autopilot decision for one pop.
 *
 * IT EXISTS SO THERE IS EXACTLY ONE COPY OF THE EMISSION RULE. pop() and
 * consume() build their query strings separately in this SDK -- the standing
 * hazard PLAN_CONFLATION §4 names -- and a rule with three branches and a
 * per-dimension carve-out is precisely the kind that gets copied wrong. Both
 * builders call this and then only PLACE what it returns; where each key sits in
 * the query string stays with the builder, because the pre-autopilot key order
 * is part of what "byte-identical" means here.
 *
 * `batch` and `max_partitions` are the USER's own values: 0 (or less) means the
 * setter was never called, which is what autopilot acts on. Note the case that
 * looks like an omission and is not -- when both are set there is nothing left
 * for the controller to decide, so `autopilot=true` is NOT emitted and the
 * request is byte-identical to the one this SDK sent before autopilot existed.
 */
inline PopSizing pop_sizing(int batch, int max_partitions, int fallback_batch, bool autopilot) {
    const bool batch_set = batch > 0;
    const bool partitions_set = max_partitions > 0;

    PopSizing out;
    if (autopilot && !(batch_set && partitions_set)) {
        out.autopilot = true;
        out.has_batch = batch_set;
        out.batch = batch;
        out.has_partitions = partitions_set;
        out.partitions = max_partitions;
        return out;
    }

    out.autopilot = false;
    out.has_batch = true;
    out.batch = batch_set ? batch : fallback_batch;
    // The legacy gate: partitions travels only above 1, because 1 IS the
    // server-side default and a v4-era client never sent it.
    out.has_partitions = partitions_set && max_partitions > 1;
    out.partitions = max_partitions;
    return out;
}

/**
 * What the broker chose for one pop, echoed back under "autopilot" when the
 * request engaged autopilot. `present` is false when the broker said nothing --
 * a pop that never asked, a broker older than 1.2, or a bodiless 204, which has
 * no body to carry the echo.
 */
struct AutopilotDecision {
    bool present = false;
    int partitions = 0;
    int batch = 0;
    /// The broker's advice on how long to wait before polling again (wire name
    /// waitMs). Advice, not a lease: the consume loop honours it for the sleep it
    /// was already taking between empty non-waiting pops, nothing more. 0 = none.
    int wait_millis = 0;
};

/**
 * Read the additive echo out of a decoded pop response.
 *
 * Unknown keys inside it are ignored, and an unknown-shaped value is treated as
 * absent rather than as an error: this field is the broker telling the client
 * what it did, and a client that refused to run because a newer broker grew a
 * fourth number would be a self-inflicted outage.
 */
inline AutopilotDecision parse_autopilot_decision(const json& response) {
    AutopilotDecision out;
    if (!response.is_object() || !response.contains("autopilot")) return out;
    const json& raw = response["autopilot"];
    if (!raw.is_object()) return out;

    auto num = [](const json& v) -> int {
        return v.is_number() && !v.is_boolean() ? v.get<int>() : 0;
    };

    out.present = true;
    if (raw.contains("partitions")) out.partitions = num(raw["partitions"]);
    if (raw.contains("batch")) out.batch = num(raw["batch"]);
    if (raw.contains("waitMs")) out.wait_millis = num(raw["waitMs"]);
    return out;
}

/// The sleep the consume loop has always taken between two empty pops that are
/// NOT long-polling. A waiting pop already blocks on the broker.
inline constexpr int EMPTY_POLL_BACKOFF_MILLIS = 100;

/**
 * How long to wait after an empty pop: the broker's advice when it gave one, the
 * historical constant otherwise. The advice is honoured as given, without a
 * ceiling of this client's invention -- the broker knows the arrival rate on
 * this queue and this client does not.
 */
inline int empty_poll_delay_millis(const AutopilotDecision& decision) {
    return decision.present && decision.wait_millis > 0 ? decision.wait_millis
                                                        : EMPTY_POLL_BACKOFF_MILLIS;
}

} // namespace util

/**
 * What one pop returned, plus the broker's account of how it sized it.
 *
 * `autopilot.present` is false when the pop did not engage autopilot, when the
 * broker is older than 1.2, or when the answer was a bodiless 204 (an empty
 * non-conflating pop) with nowhere to carry the echo.
 */
struct PopResult {
    nlohmann::json messages = nlohmann::json::array();
    util::AutopilotDecision autopilot;
};

// ============================================================================
// KV and timers - value types (PLAN_KV_TIMERS.md §5, §4)
// ============================================================================

/**
 * The expiry of a KV write, which is MANDATORY and is exactly one thing.
 *
 * §5.1: every put, putIfAbsent and incr carries exactly one of `ttlSeconds`
 * (an integer greater than zero) and `forever: true`. Zero or two declarations
 * are the same error, because both mean the caller did not decide, and a
 * default is how a marker becomes immortal. The rule lives in the stored
 * procedure so all seven clients inherit it; this type makes the wrong shape
 * INEXPRESSIBLE in C++ instead of merely refused one round trip later.
 *
 * A put NEVER inherits the previous key's expiry -- that is not expressible on
 * this wire, on purpose.
 */
class KvTtl {
private:
    bool forever_ = false;
    long long seconds_ = 0;

    KvTtl(bool forever, long long seconds) : forever_(forever), seconds_(seconds) {}

public:
    /** A relative time-to-live in whole seconds. Must be greater than zero. */
    static KvTtl seconds(long long value) {
        if (value <= 0) {
            throw std::invalid_argument("KvTtl::seconds requires a value greater than zero");
        }
        return KvTtl(false, value);
    }

    /**
     * No expiry.
     *
     * FORBIDDEN in anything CI executes (§10.4): a test that goes wrong leaves
     * immortal state in a shared database, and the next run inherits it.
     */
    static KvTtl forever() { return KvTtl(true, 0); }

    /**
     * The admitted sugar: an absolute instant, converted to a delta of seconds
     * at send time and rounded UP (§20.1). There is no `expiresAt` field on the
     * wire and there will not be -- one clock, and it is the database's.
     */
    static KvTtl until(std::chrono::system_clock::time_point when) {
        auto delta = std::chrono::duration_cast<std::chrono::milliseconds>(
            when - std::chrono::system_clock::now()).count();
        return KvTtl(false, util::ttl_seconds_from_millis(delta));
    }

    /** Write this expiry onto an operation, as exactly one field. */
    void apply(json& op) const {
        if (forever_) {
            op["forever"] = true;
        } else {
            op["ttlSeconds"] = seconds_;
        }
    }
};

/**
 * Options for put, putIfAbsent and delete.
 *
 * `expect` is the optimistic lock, and §5.3 spells the three cases: absent is an
 * unconditional upsert, 0 is "must not exist" (and wins even against an expired
 * row not yet pruned), N > 0 is a pure UPDATE that never creates the row.
 *
 * `required` escalates a lost precondition from a value into a rolled-back
 * transaction, and is only meaningful on a rider inside a bundle: it is the
 * gate. On the standalone surface it turns a 200 verdict into a 200 verdict
 * with a different shape, which is rarely what anyone wants.
 *
 * NOTE, and it is a real gap against client-js: there is no "explicitly
 * undefined" spelling here. In JavaScript, writing `expect: undefined` means
 * the caller intended to fence and computed nothing, which that client treats
 * as a bug rather than a silent downgrade to upsert. In C++ an unset
 * std::optional is indistinguishable from never having asked, so the guard does
 * not exist -- compute the version before you build the options.
 */
struct KvWriteOptions {
    std::optional<long long> expect;
    bool required = false;
};

/**
 * Options for incr.
 *
 * There is NO `expect` here, by construction: incr is the way OUT of CAS, and a
 * precondition would reintroduce the very loop incr exists to remove (§5.4).
 *
 * `max` and `min` do not saturate and do not truncate: the call that would
 * break the ceiling does not apply and comes back with the CURRENT value. That
 * is what makes `applied` THE ADMISSION DECISION for a rate limiter -- without
 * it the caller compares client-side after incrementing, so the request that
 * broke the ceiling has already spent budget and cannot be undone.
 */
struct KvIncrOptions {
    std::optional<long long> min;
    std::optional<long long> max;
    bool required = false;
};

/**
 * One timer to schedule.
 *
 * `delay_millis` is `delayMs` on the wire, a RELATIVE duration in
 * MILLISECONDS (§20.6): the declared rule of the product is "durations that can
 * be sub-second are in milliseconds, the ones that cannot are in seconds", and
 * a 250 ms retry backoff is a central use of timers while a sub-second TTL is
 * not a real use for anybody. An absolute instant is not expressible: one
 * clock, Postgres's, so no skew between brokers can enter anywhere. A delay in
 * the past is LEGAL and fires on the first cycle.
 *
 * `deliverAt` is "NOT BEFORE", never "exactly at". A healthy timer lands within
 * about ten milliseconds above the sweeper's minimum sleep; above one second
 * there is a wake-up problem, not a load problem.
 *
 * `txn` is minted when left empty, the same contract push already has for
 * transactionId. Each schedule mints its OWN -- §20.2, ratified: a rescheduled
 * timer is a new message, so "this timer, rescheduled, delivered this message"
 * stays answerable. Pass one explicitly only if you intend to correlate.
 */
struct TimerSchedule {
    std::string queue;
    std::string timer_key;
    json payload = json::object();
    long long delay_millis = 0;
    std::string partition;                   // empty = the broker's "Default"
    std::string txn;                         // empty = minted here
};

/**
 * Read a KV counter as int64, or fail loudly.
 *
 * §5.4: the counter is `numeric` server-side, so there is no overflow THERE; a
 * typed SDK exposes int64 and fails EXPLICITLY rather than handing back a
 * number that is quietly wrong. Past 2^53 a JSON parser loses precision in
 * silence, which is the failure this exists to prevent.
 */
inline long long kv_int64(const json& value) {
    if (value.is_number_integer()) {
        return value.get<long long>();
    }
    if (value.is_number_unsigned()) {
        auto raw = value.get<unsigned long long>();
        if (raw > static_cast<unsigned long long>(std::numeric_limits<long long>::max())) {
            throw std::runtime_error("kv_int64: counter is past int64 and cannot be represented");
        }
        return static_cast<long long>(raw);
    }
    if (value.is_number_float()) {
        throw std::runtime_error(
            "kv_int64: counter " + value.dump() +
            " is not an integer this client can represent exactly; read it as a string instead");
    }
    throw std::runtime_error("kv_int64: expected a JSON number, got " + value.dump());
}

// ============================================================================
// Ephemeral queues - the options structs (EPHEMERAL_QUEUES.md §1, §3.1)
// ============================================================================

/**
 * The seven knobs of `configure` (§3.1), and a CLOSED set by construction.
 *
 * The other SDKs refuse an unknown option key at runtime, because every one of
 * these bounds something (bytes, length, age, redelivery) and a silently
 * ignored `ttlSecond` is a ring that grows until a global budget answers 503.
 * A struct makes that refusal a COMPILE error instead, which is the same rule
 * enforced earlier.
 *
 * Unset fields are not sent, so the broker's own defaults own everything the
 * caller did not name -- there is no client-side default here at all.
 */
struct EphemeralOptions {
    /// Per-queue byte budget. Breaching it applies `policy`.
    std::optional<long long> max_bytes;
    /// Per-queue message budget. Breaching it applies `policy`.
    std::optional<long long> max_length;
    /// "reject" (429 queue_full) or "dropOldest" (feed semantics). Empty = unset.
    std::string policy;
    /// Drop messages older than this. NOT the durable `retention`, which cleans
    /// consumed history and never touches pending.
    std::optional<int> ttl_seconds;
    /// Redelivery lease. An unacked message comes back when it expires.
    std::optional<int> lease_seconds;
    /// Attempts before a message is DROPPED and counted. There is no DLQ (§9).
    std::optional<int> retry_limit;
    /// {"ms": …, "count": …} -- let a waiting pop fatten its batch. Null = unset.
    json window_buffer = nullptr;
};

/** `partition` picks the ring; empty leaves the choice to the broker. */
struct EphemeralPushOptions {
    std::string partition;
};

struct EphemeralPopOptions {
    /// Empty = every partition of the queue, the broker's choice of order.
    std::string partition;
    /// 0 = unset, so the broker's own batch default applies.
    int batch = 0;
    /// A real long poll, parked on a RAM gate with no database behind it (§3.4).
    bool wait = false;
    /// Milliseconds. 0 with `wait` means the 30s default; ignored without it.
    int timeout_millis = 0;
    /// The WHOLE of the consumption semantics (§1.5): same group = competing
    /// consumers, own group = fan-out, empty = the groupless queue mode.
    std::string group;
    /// Commit at delivery. At-most-once, and the only mode that is.
    bool auto_ack = false;
};

struct EphemeralAckOptions {
    /// Pass the same group the pop used -- cursors are per group.
    std::string group;
    /// "completed" (the broker's default), "failed" or "retry". Empty = unset.
    std::string status;
    /// Free-form, carried to the metrics and the log; empty = unset.
    std::string error;
};

// ============================================================================
// KV and timers - the ONE place operations are minted
//
// Every KV or timer operation this client sends is built here, whether it goes
// out standalone on POST /api/v1/kv (or POST /api/v1/timers) or rides a bundle
// as a top-level `kv` / `timers` array on /api/v1/transaction. Two builders is
// how the standalone path and the rider path drift apart, and the drift is
// invisible until the gate that works over HTTP silently stops working inside a
// transaction. test_kv_timers.cpp asserts the two are byte-identical.
// ============================================================================

namespace wire {

inline json kv_get_op(const std::string& ns, const std::string& key) {
    return json{{"op", "get"}, {"ns", ns}, {"key", key}};
}

inline json kv_write_op(const std::string& op_name, const std::string& ns, const std::string& key,
                        const json& value, const KvTtl& ttl, const KvWriteOptions& options) {
    json op = {{"op", op_name}, {"ns", ns}, {"key", key}, {"value", value}};
    ttl.apply(op);
    if (options.expect.has_value()) {
        op["expect"] = *options.expect;
    }
    if (options.required) {
        op["required"] = true;
    }
    return op;
}

inline json kv_delete_op(const std::string& ns, const std::string& key,
                         const KvWriteOptions& options) {
    json op = {{"op", "delete"}, {"ns", ns}, {"key", key}};
    if (options.expect.has_value()) {
        op["expect"] = *options.expect;
    }
    if (options.required) {
        op["required"] = true;
    }
    return op;
}

inline json kv_incr_op(const std::string& ns, const std::string& key, long long delta,
                       const KvTtl& ttl, const KvIncrOptions& options) {
    json op = {{"op", "incr"}, {"ns", ns}, {"key", key}, {"delta", delta}};
    // The TTL of incr is CREATE-ONLY server-side: a live row keeps its expiry.
    // If incr extended it, a fixed-window limiter on an always-active client
    // would never close its window, i.e. would stop limiting exactly under load.
    ttl.apply(op);
    if (options.min.has_value()) {
        op["min"] = *options.min;
    }
    if (options.max.has_value()) {
        op["max"] = *options.max;
    }
    if (options.required) {
        op["required"] = true;
    }
    return op;
}

/**
 * putIfAbsent desugars to `put` with `expect: 0` INSIDE the stored procedure,
 * one code path there. It keeps its own name on the wire because that is the
 * name of the thing, and because `applied` answering "did I win?" is the most
 * frequent question asked of this API.
 *
 * A caller-supplied expect other than 0 is a contradiction, and it dies here
 * rather than costing a round trip to be told so.
 */
inline json kv_put_if_absent_op(const std::string& ns, const std::string& key, const json& value,
                                const KvTtl& ttl, const KvWriteOptions& options) {
    if (options.expect.has_value() && *options.expect != 0) {
        throw std::invalid_argument(
            "putIfAbsent desugars to put with expect:0; a different expect is a contradiction");
    }
    KvWriteOptions cleaned;
    cleaned.required = options.required;
    return kv_write_op("putIfAbsent", ns, key, value, ttl, cleaned);
}

inline json timer_schedule_op(const TimerSchedule& timer) {
    json op = {
        {"op", "schedule"},
        {"queue", timer.queue},
        {"timerKey", timer.timer_key},
        {"delayMs", timer.delay_millis},
        // The payload is the serialized JSON, base64'd. The broker stores the
        // bytes as they arrive and the fire pushes them into the destination
        // queue unchanged, so what a consumer eventually reads is exactly this.
        {"payload", util::base64_encode(timer.payload.dump())},
        {"txn", timer.txn.empty() ? util::generate_uuid_v7() : timer.txn}
    };
    if (!timer.partition.empty()) {
        op["partition"] = timer.partition;
    }
    return op;
}

inline json timer_cancel_op(const std::string& queue, const std::string& timer_key,
                            const std::string& txn) {
    json op = {{"op", "cancel"}, {"queue", queue}, {"timerKey", timer_key}};
    if (!txn.empty()) {
        op["txn"] = txn;
    }
    return op;
}

/**
 * Pull one element out of a `{"results":[...]}` envelope.
 *
 * The client-side half of the alignment guard (§6.4, §8.2): N operations in, N
 * results out. A missing element read as "not found" would turn a broker that
 * never ran the operation -- an old broker whose wire procedure predates this
 * feature, for instance -- into a business answer, and the caller would act on
 * a gate that never fired.
 */
// ---------------------------------------------------------------------------
// Ephemeral bodies (EPHEMERAL_QUEUES.md §3.1). Minted here, next to the KV and
// timer ops, for the same reason: one place per wire shape.
// ---------------------------------------------------------------------------

/**
 * One message on the ephemeral wire is `{payload}` and nothing else -- no
 * transactionId, because there is no dedup index to hold one, and no queue or
 * partition, because the envelope already carries them.
 *
 * The `{"data":…}` / `{"payload":…}` sugar is the durable push's, deliberately
 * reproduced so one mental model covers both families, INCLUDING its trap: an
 * object that happens to have a `data` key is read as the sugar, and its other
 * keys do not travel. Wrap it -- `{"payload": obj}` -- when the object IS the
 * payload.
 */
inline json eph_message(const json& item) {
    if (item.is_null()) {
        throw std::invalid_argument(
            "ephemeral: a message may not be null; send {\"payload\": null} to push a null payload");
    }
    if (item.is_object()) {
        if (item.contains("payload")) {
            return json{{"payload", item["payload"]}};
        }
        if (item.contains("data")) {
            return json{{"payload", item["data"]}};
        }
    }
    return json{{"payload", item}};
}

inline json eph_messages(const json& messages) {
    json out = json::array();
    if (messages.is_array()) {
        for (const auto& item : messages) {
            out.push_back(eph_message(item));
        }
    } else {
        out.push_back(eph_message(messages));
    }
    return out;
}

/** `{queue, partition?, messages:[{payload}…]}` -- identity on the ENVELOPE. */
inline json eph_push_body(const std::string& queue, const std::string& partition,
                          const json& messages) {
    json body = {{"queue", queue}};
    // Omitted, never defaulted client-side: which partition an ephemeral push
    // without one lands on is the broker's rule, and inventing a "Default" here
    // would take that decision away from it in a way the caller never asked for.
    if (!partition.empty()) {
        body["partition"] = partition;
    }
    body["messages"] = eph_messages(messages);
    return body;
}

/** Only the options the caller set, in the plan's order. */
inline json eph_configure_body(const std::string& queue, const EphemeralOptions& options) {
    json opts = json::object();
    if (options.max_bytes.has_value()) {
        opts["maxBytes"] = *options.max_bytes;
    }
    if (options.max_length.has_value()) {
        opts["maxLength"] = *options.max_length;
    }
    if (!options.policy.empty()) {
        opts["policy"] = options.policy;
    }
    if (options.ttl_seconds.has_value()) {
        opts["ttlSeconds"] = *options.ttl_seconds;
    }
    if (options.lease_seconds.has_value()) {
        opts["leaseSeconds"] = *options.lease_seconds;
    }
    if (options.retry_limit.has_value()) {
        opts["retryLimit"] = *options.retry_limit;
    }
    if (!options.window_buffer.is_null()) {
        opts["windowBuffer"] = options.window_buffer;
    }
    return json{{"queue", queue}, {"options", opts}};
}

/**
 * An ack entry is `{id, status?, error?}`. Accepts a popped message, a bare id
 * string, or the wire object itself; a per-entry status wins over the call-wide
 * default, which is how a mixed batch (some completed, one retry) is expressed
 * in a single request.
 */
inline json eph_ack_entry(const json& entry, size_t index, const EphemeralAckOptions& options) {
    std::string id;
    if (entry.is_string()) {
        id = entry.get<std::string>();
    } else if (entry.is_object() && entry.contains("id") && entry["id"].is_string()) {
        id = entry["id"].get<std::string>();
    }
    if (id.empty()) {
        throw std::invalid_argument(
            "ephemeral: ack at index " + std::to_string(index) +
            " carries no message id; pass the popped message, or its `id`");
    }

    json ack = {{"id", id}};

    if (entry.is_object() && entry.contains("status") && entry["status"].is_string()) {
        ack["status"] = entry["status"];
    } else if (!options.status.empty()) {
        ack["status"] = options.status;
    }

    if (entry.is_object() && entry.contains("error") && entry["error"].is_string()) {
        ack["error"] = entry["error"];
    } else if (!options.error.empty()) {
        ack["error"] = options.error;
    }

    return ack;
}

inline json eph_ack_body(const std::string& queue, const json& acks,
                         const EphemeralAckOptions& options) {
    json entries = json::array();
    if (acks.is_array()) {
        for (size_t i = 0; i < acks.size(); ++i) {
            entries.push_back(eph_ack_entry(acks[i], i, options));
        }
    } else {
        entries.push_back(eph_ack_entry(acks, 0, options));
    }

    json body = {{"queue", queue}};
    if (!options.group.empty()) {
        body["group"] = options.group;
    }
    body["acks"] = entries;
    return body;
}

inline json single_result(const json& response, const char* what) {
    if (response.is_object() && response.contains("results") && response["results"].is_array() &&
        !response["results"].empty()) {
        return response["results"][0];
    }
    throw std::runtime_error(
        std::string(what) + ": the broker returned no result for the operation; this is an "
        "alignment failure, not an absent key");
}

} // namespace wire

// ============================================================================
// LoadBalancer - Distributes requests across multiple servers
// ============================================================================

class LoadBalancer {
private:
    std::vector<std::string> urls_;
    std::string strategy_;
    std::atomic<size_t> current_index_{0};
    std::mutex session_mutex_;
    std::unordered_map<std::string, size_t> session_map_;
    std::string session_id_;
    
public:
    LoadBalancer(const std::vector<std::string>& urls, const std::string& strategy = "round-robin")
        : urls_(urls), strategy_(strategy) {
        if (urls_.empty()) {
            throw std::invalid_argument("URLs vector cannot be empty");
        }
        
        // Remove trailing slashes
        for (auto& url : urls_) {
            if (!url.empty() && url.back() == '/') {
                url.pop_back();
            }
        }
        
        // Generate session ID
        session_id_ = "session_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
    }
    
    std::string get_next_url(const std::string& session_key = "") {
        const std::string& key = session_key.empty() ? session_id_ : session_key;
        
        if (strategy_ == "session") {
            // Session affinity: stick to the same server per session
            std::lock_guard<std::mutex> lock(session_mutex_);
            
            if (session_map_.find(key) == session_map_.end()) {
                size_t assigned_index = current_index_.fetch_add(1) % urls_.size();
                session_map_[key] = assigned_index;
            }
            
            return urls_[session_map_[key]];
        }
        
        // Round robin: cycle through URLs
        size_t index = current_index_.fetch_add(1) % urls_.size();
        return urls_[index];
    }
    
    std::vector<std::string> get_all_urls() const {
        return urls_;
    }
    
    std::string get_strategy() const {
        return strategy_;
    }
    
    void reset() {
        current_index_ = 0;
        std::lock_guard<std::mutex> lock(session_mutex_);
        session_map_.clear();
    }
};

// ============================================================================
// HttpClient - HTTP client with retry, failover support
// ============================================================================

class HttpClient {
private:
    std::string base_url_;
    std::shared_ptr<LoadBalancer> load_balancer_;
    int timeout_millis_;
    int retry_attempts_;
    int retry_delay_millis_;
    bool enable_failover_;
    std::string bearer_token_;
    Retry429Options retry_429_;
    
    struct HttpResponse {
        int status_code = 0;
        std::string body;
        bool success = false;
        std::string error_message;
        std::string error_code;                     // proxy contract "code" field
        std::optional<double> retry_after_seconds;  // 429 only
    };

    struct Retry429Policy {
        int max_attempts;                           // 0 = unbounded
        int base_millis;
        int cap_millis;
    };
    
    HttpResponse execute_request(const std::string& url, const std::string& method,
                                 const std::string& path, const json& body = nullptr,
                                 int request_timeout_millis = 0) {
        try {
            auto [scheme, host, port] = util::parse_url(url);
            
            int timeout_sec = (request_timeout_millis > 0 ? request_timeout_millis : timeout_millis_) / 1000;
            
            httplib::Client client(host, port);
            client.set_connection_timeout(timeout_sec);
            client.set_read_timeout(timeout_sec);
            client.set_write_timeout(timeout_sec);
            
            httplib::Headers headers = {
                {"Content-Type", "application/json"}
            };
            if (!bearer_token_.empty()) {
                headers.emplace("Authorization", "Bearer " + bearer_token_);
            }
            
            httplib::Result res;
            
            if (method == "GET") {
                res = client.Get(path.c_str(), headers);
            } else if (method == "POST") {
                std::string body_str = body.is_null() ? "" : body.dump();
                res = client.Post(path.c_str(), headers, body_str, "application/json");
            } else if (method == "PUT") {
                std::string body_str = body.is_null() ? "" : body.dump();
                res = client.Put(path.c_str(), headers, body_str, "application/json");
            } else if (method == "DELETE") {
                res = client.Delete(path.c_str(), headers);
            } else {
                return {0, "", false, "Unsupported HTTP method: " + method, "", std::nullopt};
            }
            
            if (!res) {
                return {0, "", false, "HTTP request failed: " + httplib::to_string(res.error()), "", std::nullopt};
            }
            
            // Handle 204 No Content
            if (res->status == 204) {
                return {204, "", true, "", "", std::nullopt};
            }
            
            // Handle errors
            if (res->status >= 400) {
                std::string error_msg = "HTTP " + std::to_string(res->status);
                std::string error_code;
                try {
                    if (!res->body.empty()) {
                        json error_body = json::parse(res->body);
                        if (error_body.contains("error")) {
                            error_msg = error_body["error"].get<std::string>();
                        }
                        if (error_body.contains("code") && error_body["code"].is_string()) {
                            error_code = error_body["code"].get<std::string>();
                        }
                    }
                } catch (...) {
                    // Ignore JSON parse errors
                }

                std::optional<double> retry_after;
                if (res->status == 429) {
                    retry_after = parse_retry_after(res->get_header_value("Retry-After"));
                }
                return {res->status, res->body, false, error_msg, error_code, retry_after};
            }
            
            return {res->status, res->body, true, "", "", std::nullopt};
            
        } catch (const std::exception& e) {
            return {0, "", false, std::string("Exception: ") + e.what(), "", std::nullopt};
        }
    }
    
    /** Retry-After header value (seconds) -> number; empty when non-numeric. */
    static std::optional<double> parse_retry_after(const std::string& value) {
        if (value.empty()) {
            return std::nullopt;
        }
        try {
            size_t consumed = 0;
            double seconds = std::stod(value, &consumed);
            if (consumed != value.size() || seconds < 0) {
                return std::nullopt;
            }
            return seconds;
        } catch (...) {
            return std::nullopt;
        }
    }

    Retry429Policy retry429_policy_for(RetryKind retry_kind) const {
        Retry429Policy policy;
        policy.base_millis = retry_429_.base_millis > 0 ? retry_429_.base_millis : 500;
        policy.cap_millis = retry_429_.cap_millis > 0 ? retry_429_.cap_millis : 30000;

        if (retry_429_.max_attempts > 0) {
            policy.max_attempts = retry_429_.max_attempts; // explicit override applies to both kinds
        } else {
            policy.max_attempts = (retry_kind == RetryKind::Pop) ? 0 : 10;
        }

        return policy;
    }

    /**
     * One logical request against a single URL, transparently retrying HTTP 429
     * with backoff until the policy for retry_kind is exhausted (or never, for
     * an unbounded pop policy). Every other outcome is returned immediately:
     * 429 is the only status this layer treats as retryable, 5xx/network retry
     * and cross-backend failover are the callers' job.
     */
    HttpResponse execute_with_retry429(const std::string& url, const std::string& method,
                                       const std::string& path, const json& body,
                                       int request_timeout_millis, RetryKind retry_kind) {
        Retry429Policy policy = retry429_policy_for(retry_kind);
        int tries = 0;

        while (true) {
            ++tries;
            HttpResponse response = execute_request(url, method, path, body, request_timeout_millis);

            if (response.status_code != 429) {
                return response;
            }

            if (policy.max_attempts > 0 && tries >= policy.max_attempts) {
                util::log_error("HttpClient.retry429", method + " " + path +
                    " max 429 attempts exhausted after " + std::to_string(tries) +
                    " (code=" + response.error_code + ")");
                return response;
            }

            int delay = util::compute_retry429_delay_millis(tries - 1, response.retry_after_seconds,
                                                            policy.base_millis, policy.cap_millis);
            util::log_warn("HttpClient.retry429", method + " " + path + " attempt " +
                std::to_string(tries) + ", retrying in " + std::to_string(delay) +
                "ms (code=" + response.error_code + ")");
            std::this_thread::sleep_for(std::chrono::milliseconds(delay));
        }
    }
    
    HttpResponse request_with_retry(const std::string& method, const std::string& path,
                                    const json& body = nullptr, int request_timeout_millis = 0,
                                    RetryKind retry_kind = RetryKind::Default) {
        HttpResponse last_response;
        
        for (int attempt = 0; attempt < retry_attempts_; ++attempt) {
            std::string url = get_url();
            last_response = execute_with_retry429(url, method, path, body,
                                                  request_timeout_millis, retry_kind);
            
            if (last_response.success) {
                return last_response;
            }
            
            // Don't retry on client errors (4xx)
            if (last_response.status_code >= 400 && last_response.status_code < 500) {
                return last_response;
            }
            
            // Wait before retry (except on last attempt)
            if (attempt < retry_attempts_ - 1) {
                int delay = retry_delay_millis_ * (1 << attempt); // Exponential backoff
                std::this_thread::sleep_for(std::chrono::milliseconds(delay));
            }
        }
        
        return last_response;
    }
    
    HttpResponse request_with_failover(const std::string& method, const std::string& path,
                                       const json& body = nullptr, int request_timeout_millis = 0,
                                       RetryKind retry_kind = RetryKind::Default) {
        if (!load_balancer_ || !enable_failover_) {
            return request_with_retry(method, path, body, request_timeout_millis, retry_kind);
        }
        
        auto urls = load_balancer_->get_all_urls();
        std::unordered_set<std::string> attempted_urls;
        HttpResponse last_response;
        
        for (size_t i = 0; i < urls.size(); ++i) {
            std::string url = load_balancer_->get_next_url();
            
            if (attempted_urls.count(url)) {
                continue;
            }
            
            attempted_urls.insert(url);
            // 429s are retried in place (same backend, backoff-paced) inside
            // execute_with_retry429: rate limiting is a tenant-quota signal, not
            // a backend-health one, so an exhausted 429 falls into the 4xx
            // short-circuit below instead of failing over to another backend.
            last_response = execute_with_retry429(url, method, path, body,
                                                  request_timeout_millis, retry_kind);
            
            if (last_response.success) {
                return last_response;
            }
            
            // Don't retry on client errors (4xx)
            if (last_response.status_code >= 400 && last_response.status_code < 500) {
                return last_response;
            }
        }
        
        return last_response;
    }
    
    std::string get_url() const {
        if (load_balancer_) {
            return load_balancer_->get_next_url();
        }
        return base_url_;
    }

    /**
     * Turn a failed response into the exception callers see. A response that
     * carries an HTTP status becomes an HttpError (status/body/code, plus
     * Retry-After on a 429); a transport failure has no status and stays a
     * plain runtime_error.
     */
    [[noreturn]] static void throw_response_error(const HttpResponse& response) {
        if (response.status_code > 0) {
            throw HttpError(response.status_code, response.error_message, response.body,
                            response.error_code, response.retry_after_seconds);
        }
        throw std::runtime_error(response.error_message);
    }
    
public:
    HttpClient(const std::string& base_url, int timeout_millis = 30000,
               int retry_attempts = 3, int retry_delay_millis = 1000,
               const std::string& bearer_token = "",
               const Retry429Options& retry_429 = Retry429Options())
        : base_url_(base_url), timeout_millis_(timeout_millis),
          retry_attempts_(retry_attempts), retry_delay_millis_(retry_delay_millis),
          enable_failover_(false), bearer_token_(bearer_token), retry_429_(retry_429) {
        // Remove trailing slash
        if (!base_url_.empty() && base_url_.back() == '/') {
            base_url_.pop_back();
        }
    }
    
    HttpClient(std::shared_ptr<LoadBalancer> load_balancer, int timeout_millis = 30000,
               int retry_attempts = 3, int retry_delay_millis = 1000, bool enable_failover = true,
               const std::string& bearer_token = "",
               const Retry429Options& retry_429 = Retry429Options())
        : load_balancer_(load_balancer), timeout_millis_(timeout_millis),
          retry_attempts_(retry_attempts), retry_delay_millis_(retry_delay_millis),
          enable_failover_(enable_failover), bearer_token_(bearer_token), retry_429_(retry_429) {
    }
    
    /**
     * retry_kind: pass RetryKind::Pop for long-poll (wait=true) pop requests to
     * get the unbounded-with-backoff 429 policy; leave it defaulted for
     * everything else (push, admin calls, non-waiting pop), which gets the
     * bounded 10-attempt budget.
     */
    json get(const std::string& path, int request_timeout_millis = 0,
             RetryKind retry_kind = RetryKind::Default) {
        auto response = request_with_failover("GET", path, nullptr, request_timeout_millis, retry_kind);
        
        if (!response.success) {
            throw_response_error(response);
        }
        
        if (response.body.empty() || response.status_code == 204) {
            return nullptr;
        }
        
        return json::parse(response.body);
    }
    
    json post(const std::string& path, const json& body = nullptr, int request_timeout_millis = 0,
              RetryKind retry_kind = RetryKind::Default) {
        auto response = request_with_failover("POST", path, body, request_timeout_millis, retry_kind);
        
        if (!response.success) {
            throw_response_error(response);
        }
        
        if (response.body.empty() || response.status_code == 204) {
            return nullptr;
        }
        
        return json::parse(response.body);
    }
    
    json put(const std::string& path, const json& body = nullptr, int request_timeout_millis = 0,
             RetryKind retry_kind = RetryKind::Default) {
        auto response = request_with_failover("PUT", path, body, request_timeout_millis, retry_kind);
        
        if (!response.success) {
            throw_response_error(response);
        }
        
        if (response.body.empty() || response.status_code == 204) {
            return nullptr;
        }
        
        return json::parse(response.body);
    }
    
    json del(const std::string& path, int request_timeout_millis = 0,
             RetryKind retry_kind = RetryKind::Default) {
        auto response = request_with_failover("DELETE", path, nullptr, request_timeout_millis, retry_kind);
        
        if (!response.success) {
            throw_response_error(response);
        }
        
        if (response.body.empty() || response.status_code == 204) {
            return nullptr;
        }
        
        return json::parse(response.body);
    }
    
    std::shared_ptr<LoadBalancer> get_load_balancer() const {
        return load_balancer_;
    }
};

// ============================================================================
// MessageBuffer - Buffer for a single queue/partition address
// ============================================================================

/**
 * The client-side linger for one `queue/partition` address: single pushes
 * accumulate here and leave as one POST once `message_count` messages are
 * waiting, or `time_millis` after the first one arrived. Port of the 1.0.6
 * buffer rewrite that shipped in the JS/Python/Go/PHP SDKs; two properties
 * beyond the batching are load-bearing:
 *
 *  - `max_size` is a BLOCKING bound, not a hint. add() parks on a condition
 *    variable while the buffer is full, so a producer that outruns the flush
 *    pipeline is paced down to the drain rate instead of growing the heap.
 *    (Measured on the Go client, which had exactly this shape: 1.46M adds/s
 *    against a 1.0M/s flush pipeline accumulated 20.9M messages / 11.7 GB in
 *    45s and lost every one at exit with zero errors; the bounded version
 *    sustained 881k msg/s with exact send/receive parity in 71 MB.)
 *
 *  - A batch that fails to send goes BACK to the front of the buffer, in
 *    order, and is retried after `retry_delay_millis` -- indefinitely, until
 *    it lands, the buffer is stopped, or an explicit flush deadline expires.
 *    It is never dropped. Before this port, the C++ flusher extracted the
 *    batch before the POST and only logged the failure: up to message_count
 *    messages vanished per failed request, with the caller long since told
 *    the push succeeded.
 *
 * THREADING SHAPE: one flusher thread per buffer, owning ALL sends for its
 * address. That is the C++ spelling of the JS invariant "exactly one drain
 * loop per buffer" -- a single sender by construction, so batches can never
 * interleave out of order -- and it doubles as the time-based trigger (the
 * detached timer threads of the old implementation, which could outlive the
 * buffer they pointed into, are gone). Producers and the flusher share one
 * mutex; producers park on `not_full_`, the flusher parks on `flusher_wake_`,
 * and explicit flush callers park on `drain_progress_`.
 *
 * Buffered messages live only in this process's memory. A crash, or an exit
 * that skips close(), loses them -- buffering belongs on telemetry-shaped
 * traffic, not on anything that must not be lost.
 */
class MessageBuffer {
public:
    using Clock = std::chrono::steady_clock;
    /** POSTs one batch; throws when the broker did not accept it. */
    using Sender = std::function<void(const std::vector<json>&)>;

private:
    std::string queue_address_;
    BufferOptions options_;                  // resolved: every field positive
    Sender sender_;

    mutable std::mutex mutex_;
    std::condition_variable not_full_;       // producers parked on the max_size bound
    std::condition_variable flusher_wake_;   // the flusher, while idle / lingering / pacing a retry
    std::condition_variable drain_progress_; // flush_until() callers, between drain events

    std::deque<json> messages_;
    Clock::time_point first_message_time_{};
    bool has_first_message_ = false;
    bool flush_requested_ = false;           // an add or a flush caller wants a drain now
    bool draining_ = false;                  // the flusher is inside its drain loop
    bool stopped_ = false;
    // Tightest deadline any explicit flush caller is currently willing to wait
    // out. The drain consults it only on a FAILED batch: a deadline bounds the
    // retrying of failures, never the sending of batches that land.
    Clock::time_point flush_deadline_ = Clock::time_point::max();
    std::string last_flush_error_;
    // Times the drain consumed a deadline and gave up with messages still
    // buffered. Monotonic, so a flush caller can tell "a failure happened at
    // or after my deadline" without having to catch the flusher between two
    // drains -- the flusher holds the mutex from one drain to the next, so a
    // waiter keyed on observing draining_ == false could starve forever.
    uint64_t give_up_count_ = 0;
    // Earliest instant the flusher may start a drain again after giving up on
    // a deadline: the fresh time_millis linger the JS SDK's endFlush schedules,
    // so a consumed deadline does not turn into a hot retry loop.
    Clock::time_point next_drain_at_ = Clock::time_point::min();

    std::thread flusher_;

    /**
     * Send until the buffer is empty, the buffer stops, or the deadline is
     * consumed by a failure. Called by the flusher thread with `lock` held;
     * the lock is released around every POST.
     */
    void drain(std::unique_lock<std::mutex>& lock) {
        flush_requested_ = false;
        draining_ = true;

        while (!messages_.empty() && !stopped_) {
            const size_t batch_size =
                std::min(messages_.size(), static_cast<size_t>(options_.message_count));
            std::vector<json> batch;
            batch.reserve(batch_size);
            for (size_t i = 0; i < batch_size; ++i) {
                batch.push_back(std::move(messages_.front()));
                messages_.pop_front();
            }
            if (messages_.empty()) {
                has_first_message_ = false;
            }

            lock.unlock();
            bool sent = false;
            std::string error_text;
            try {
                sender_(batch);
                sent = true;
            } catch (const std::exception& e) {
                error_text = e.what();
            }
            lock.lock();

            if (sent) {
                // Capacity is freed only when the batch is definitively gone.
                // Waking at extraction instead would let producers refill
                // against room that never freed if the POST then failed.
                not_full_.notify_all();
                drain_progress_.notify_all();
                continue;
            }

            // NOT dropped: back at the FRONT, in order. These messages were
            // queued before everything still in the buffer, and a retry must
            // not reorder a partition's lane. This is the one place occupancy
            // can exceed max_size, by at most one batch.
            messages_.insert(messages_.begin(),
                             std::make_move_iterator(batch.begin()),
                             std::make_move_iterator(batch.end()));
            if (!has_first_message_) {
                first_message_time_ = Clock::now();
                has_first_message_ = true;
            }
            last_flush_error_ = error_text;
            util::log_error("MessageBuffer.flush", "push failed for " + queue_address_ +
                ": " + error_text + " (" + std::to_string(batch_size) +
                " message(s) requeued)");

            const auto now = Clock::now();
            if (now >= flush_deadline_) {
                // The deadline is consumed, not left armed: the caller that
                // set it is about to throw, and background operation resumes
                // with the default "retry until it lands" -- after a fresh
                // linger window, not immediately, or a dead broker would turn
                // the flusher into a hot loop the moment a deadline expired.
                flush_deadline_ = Clock::time_point::max();
                ++give_up_count_;
                next_drain_at_ = now + std::chrono::milliseconds(options_.time_millis);
                break;
            }
            auto wake_at = now + std::chrono::milliseconds(options_.retry_delay_millis);
            if (flush_deadline_ < wake_at) {
                wake_at = flush_deadline_;
            }
            // The retry delay, but a shutdown must not wait out a pause that
            // only exists to pace a broker that is not answering.
            flusher_wake_.wait_until(lock, wake_at, [&] { return stopped_; });
        }

        draining_ = false;
        flush_deadline_ = Clock::time_point::max();
        if (!messages_.empty()) {
            // Whatever remains (a requeued batch, or adds that landed while
            // the drain was giving up) gets a fresh linger window, so it
            // cannot sit unnoticed until the next add.
            first_message_time_ = Clock::now();
            has_first_message_ = true;
        }
        drain_progress_.notify_all();
    }

    void flusher_loop() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!stopped_) {
            if (messages_.empty()) {
                flush_requested_ = false;
                flusher_wake_.wait(lock, [&] { return stopped_ || !messages_.empty(); });
                continue;
            }
            // An explicit flush request drains NOW -- it overrides the linger
            // timer and the post-give-up pause alike. Otherwise: at the count
            // threshold the drain is due immediately, except while the pause a
            // consumed deadline leaves behind is running; below it, the timer
            // that started with the first message rules.
            if (!flush_requested_) {
                const bool at_threshold =
                    messages_.size() >= static_cast<size_t>(options_.message_count);
                const auto fire_at = at_threshold
                    ? next_drain_at_
                    : first_message_time_ + std::chrono::milliseconds(options_.time_millis);
                if (Clock::now() < fire_at) {
                    const bool state_changed = flusher_wake_.wait_until(lock, fire_at, [&] {
                        return stopped_ || flush_requested_ ||
                               (!at_threshold &&
                                messages_.size() >=
                                    static_cast<size_t>(options_.message_count));
                    });
                    if (state_changed) {
                        continue; // re-evaluate under the new state (it may be `stopped`)
                    }
                }
            }
            drain(lock);
        }
    }

public:
    MessageBuffer(const std::string& queue_address, const BufferOptions& options,
                  Sender sender)
        : queue_address_(queue_address), options_(resolve_buffer_options(options)),
          sender_(std::move(sender)) {
        flusher_ = std::thread([this] { flusher_loop(); });
    }

    ~MessageBuffer() {
        stop();
        // The flusher may be inside a POST; joining waits it out (bounded by
        // the HTTP client's own timeout/retry budget). Anything cheaper leaves
        // a thread pointing into a dead object.
        if (flusher_.joinable()) {
            flusher_.join();
        }
    }

    /**
     * Append one message, BLOCKING while the buffer is at max_size.
     *
     * Returns once the message is in the buffer. Throws if the buffer is
     * stopped -- before or while parked -- because an add that could not be
     * buffered must never look like a successful push.
     */
    void add(const json& formatted_message) {
        std::unique_lock<std::mutex> lock(mutex_);

        // BACKPRESSURE. Re-checked in a loop, not once: the wake is a
        // broadcast, and the first producers to resume can fill the room that
        // was freed, so the rest have to park again. Being at the bound means
        // producers outran the flusher -- make sure a drain is actually coming
        // before parking (the time-based trigger may be most of a second away).
        while (messages_.size() >= static_cast<size_t>(options_.max_size) && !stopped_) {
            flush_requested_ = true;
            flusher_wake_.notify_all();
            not_full_.wait(lock);
        }

        if (stopped_) {
            throw std::runtime_error("Queen buffer " + queue_address_ +
                " is stopped (client closed): message not buffered");
        }

        if (messages_.empty()) {
            first_message_time_ = Clock::now();
            has_first_message_ = true;
        }
        messages_.push_back(formatted_message);

        // The first message starts the flusher's linger clock; crossing the
        // threshold starts a drain. In between, the flusher is already parked
        // on a timed wait and needs no signal.
        if (messages_.size() == 1 ||
            messages_.size() >= static_cast<size_t>(options_.message_count)) {
            flusher_wake_.notify_all();
        }
    }

    /**
     * Wait until everything buffered has LANDED (not merely been extracted),
     * or until `deadline` -- at which point a BufferFlushError reports how
     * many messages are still buffered, none of them dropped.
     *
     * The deadline bounds the retrying of failures and is checked between
     * attempts, mirroring the JS/PHP SDKs: the first attempt always happens
     * (a caller with a zero deadline still gets one real try), a batch that
     * lands never counts against it, and a single in-flight POST is bounded
     * by the HTTP client's own timeout rather than cut short. Pass
     * Clock::time_point::max() to retry until it lands or the buffer stops.
     * Returns silently on a stopped buffer: cleanup() reports the discard.
     */
    void flush_until(Clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex_);
        const uint64_t give_ups_before = give_up_count_;

        while (true) {
            if (stopped_) {
                return;
            }
            if (messages_.empty() && !draining_) {
                return;
            }
            // Give up only once the DRAIN has: a bumped give_up_count_ means a
            // real attempt failed at or after the (tightened) deadline. Keying
            // on the counter instead of on catching the flusher idle matters
            // -- the flusher can hold the mutex from one drain straight into
            // the next, so "idle" may never be observable from here.
            if (give_up_count_ > give_ups_before && !messages_.empty() &&
                Clock::now() >= deadline) {
                const int count = static_cast<int>(messages_.size());
                const std::string reason =
                    last_flush_error_.empty() ? "buffer flush deadline expired"
                                              : last_flush_error_;
                throw BufferFlushError(reason + " (" + std::to_string(count) +
                    " message(s) still buffered for " + queue_address_ + ", not sent)",
                    queue_address_, count);
            }
            // Tighten, not replace: the shortest patience of any concurrent
            // flush caller wins, otherwise a shutdown could be held open by a
            // background retry loop that was started with none.
            if (deadline < flush_deadline_) {
                flush_deadline_ = deadline;
            }
            flush_requested_ = true;
            flusher_wake_.notify_all();
            drain_progress_.wait(lock);
        }
    }

    /**
     * Stop the buffer: adds are refused, parked adds wake AND THROW (their
     * message was never buffered -- reporting success for a message dropped on
     * the floor is the failure mode this class exists to remove), the flusher
     * unwinds at its next check, and flush_until() callers return.
     */
    void stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopped_) {
                return;
            }
            stopped_ = true;
        }
        not_full_.notify_all();
        flusher_wake_.notify_all();
        drain_progress_.notify_all();
    }

    size_t message_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return messages_.size();
    }

    const BufferOptions& get_options() const {
        return options_;
    }

    int first_message_age_millis() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!has_first_message_) return 0;
        return static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(
            Clock::now() - first_message_time_).count());
    }
};

// ============================================================================
// BufferManager - Manages buffers for all queues
// ============================================================================

/**
 * The registry of per-address MessageBuffers. One buffer per `queue/partition`
 * (the granularity the broker fuses writes on), created on the first buffered
 * push to that address -- whose options configure the buffer for its lifetime.
 * All batching, backpressure, retry and ordering live in MessageBuffer; this
 * class only routes, aggregates stats, and owns the shutdown sequencing.
 *
 * Buffers are retired only by cleanup(), never when they drain empty: a
 * concurrent producer may already hold the buffer it looked up, and retiring
 * it under that producer would strand its message in an object nothing ever
 * flushes again. An idle buffer costs one parked thread and an empty deque.
 */
class BufferManager {
private:
    std::shared_ptr<HttpClient> http_client_;
    std::unordered_map<std::string, std::shared_ptr<MessageBuffer>> buffers_;
    // Guards the registry only, and is never held across an add or a flush --
    // holding it across a blocking add() would queue every producer on every
    // queue behind one parked one.
    mutable std::mutex mutex_;
    std::atomic<int> flush_count_{0};
    std::atomic<bool> stopped_{false};

    std::shared_ptr<MessageBuffer> get_buffer(const std::string& queue_address) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = buffers_.find(queue_address);
        return it == buffers_.end() ? nullptr : it->second;
    }

    static MessageBuffer::Clock::time_point absolute_deadline(int deadline_millis) {
        return deadline_millis < 0
            ? MessageBuffer::Clock::time_point::max()
            : MessageBuffer::Clock::now() + std::chrono::milliseconds(deadline_millis);
    }

public:
    BufferManager(std::shared_ptr<HttpClient> http_client)
        : http_client_(http_client) {
    }

    ~BufferManager() {
        cleanup();
    }

    /**
     * Buffer one message, BLOCKING while that address's buffer is at its
     * max_size bound. Throws if the client has been closed -- a push after
     * cleanup() would otherwise create a fresh buffer that nothing will ever
     * flush, which is the same false success the bound exists to remove.
     */
    void add_message(const std::string& queue_address, const json& formatted_message,
                    const BufferOptions& options) {
        std::shared_ptr<MessageBuffer> buffer;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopped_) {
                throw std::runtime_error(
                    "Queen client is closed: message not buffered for " + queue_address);
            }
            auto it = buffers_.find(queue_address);
            if (it == buffers_.end()) {
                auto sender = [this](const std::vector<json>& items) {
                    json request = {{"items", items}};
                    http_client_->post("/api/v1/push", request);
                    flush_count_++;
                };
                it = buffers_.emplace(queue_address,
                    std::make_shared<MessageBuffer>(queue_address, options, sender)).first;
            }
            buffer = it->second;
        }
        // Outside the registry lock, deliberately: this call can park.
        buffer->add(formatted_message);
    }

    /**
     * Send everything buffered for one address. deadline_millis < 0 (the
     * default) retries a failing batch until it lands or the buffer stops;
     * otherwise a BufferFlushError after the deadline reports how many
     * messages are still buffered (none dropped).
     */
    void flush_buffer(const std::string& queue_address, int deadline_millis = -1) {
        auto buffer = get_buffer(queue_address);
        if (!buffer) {
            return;
        }
        buffer->flush_until(absolute_deadline(deadline_millis));
    }

    /**
     * Send everything buffered, for every address. The deadline is one
     * absolute instant shared by all of them, every buffer is attempted even
     * after an earlier one fails -- an unreachable queue must not strand the
     * others' messages -- and the first failure is rethrown at the end.
     */
    void flush_all_buffers(int deadline_millis = -1) {
        const auto deadline = absolute_deadline(deadline_millis);

        std::vector<std::shared_ptr<MessageBuffer>> buffers;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            buffers.reserve(buffers_.size());
            for (const auto& pair : buffers_) {
                buffers.push_back(pair.second);
            }
        }

        std::exception_ptr first_error;
        for (const auto& buffer : buffers) {
            try {
                buffer->flush_until(deadline);
            } catch (...) {
                if (!first_error) {
                    first_error = std::current_exception();
                }
            }
        }
        if (first_error) {
            std::rethrow_exception(first_error);
        }
    }

    json get_stats() const {
        std::lock_guard<std::mutex> lock(mutex_);

        int total_buffered = 0;
        int oldest_age = 0;

        for (const auto& pair : buffers_) {
            total_buffered += static_cast<int>(pair.second->message_count());
            oldest_age = std::max(oldest_age, pair.second->first_message_age_millis());
        }

        return {
            {"activeBuffers", buffers_.size()},
            {"totalBufferedMessages", total_buffered},
            {"oldestBufferAge", oldest_age},
            {"flushesPerformed", flush_count_.load()}
        };
    }

    /**
     * Stop every buffer and discard what is left, loudly. Stopping wakes
     * parked adds (they throw: their message was never buffered) and ends any
     * retry loop, then joining the flusher threads makes destruction safe.
     * Anything still buffered here is lost -- which is why close() flushes
     * with a deadline first and this logs what remains.
     */
    void cleanup() {
        std::unordered_map<std::string, std::shared_ptr<MessageBuffer>> doomed;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopped_ = true;
            doomed.swap(buffers_);
        }

        int unflushed = 0;
        for (const auto& pair : doomed) {
            pair.second->stop();
            unflushed += static_cast<int>(pair.second->message_count());
        }
        if (unflushed > 0) {
            util::log_error("BufferManager.cleanup", std::to_string(unflushed) +
                " unflushed message(s) discarded");
        }
        doomed.clear(); // destructors join the flusher threads
    }
};

// ============================================================================
// TransactionBuilder - Atomic transactions
// ============================================================================

class TransactionBuilder {
private:
    std::shared_ptr<HttpClient> http_client_;
    json operations_ = json::array();
    // The two rider arrays. They are TOP-LEVEL fields of the request body and
    // never elements of `operations` -- see the comment on commit().
    json kv_ops_ = json::array();
    json timer_ops_ = json::array();
    std::vector<std::string> required_leases_;

    class QueuePushBuilder {
    private:
        TransactionBuilder* parent_;
        std::string queue_name_;
        
    public:
        QueuePushBuilder(TransactionBuilder* parent, const std::string& queue_name)
            : parent_(parent), queue_name_(queue_name) {
        }
        
        TransactionBuilder& push(const std::vector<json>& items) {
            json push_items = json::array();
            
            for (const auto& item : items) {
                json payload;
                if (item.contains("data")) {
                    payload = item["data"];
                } else if (item.contains("payload")) {
                    payload = item["payload"];
                } else {
                    payload = item;
                }
                
                // Same contract as QueueBuilder::push: the caller's transactionId
                // is what makes a retried transaction idempotent inside the dedup
                // window, so it has to reach the wire. Absent, mint one here
                // rather than leaving the broker to do it.
                json formatted = {
                    {"queue", queue_name_},
                    {"payload", payload},
                    {"transactionId", item.contains("transactionId")
                        ? item["transactionId"].get<std::string>()
                        : util::generate_uuid_v7()}
                };

                if (item.contains("traceId") && item["traceId"].is_string()) {
                    std::string trace_id = item["traceId"].get<std::string>();
                    if (util::is_valid_uuid(trace_id)) {
                        formatted["traceId"] = trace_id;
                    }
                }

                push_items.push_back(formatted);
            }
            
            parent_->operations_.push_back({
                {"type", "push"},
                {"items", push_items}
            });
            
            return *parent_;
        }
    };

    /**
     * KV writes and reads that ride this bundle, bound to one namespace.
     *
     * Every method returns the TransactionBuilder, exactly like
     * QueuePushBuilder::push, so a bundle reads as one chain.
     *
     * THE POINT OF PUTTING KV HERE INSTEAD OF CALLING client.kv() SEPARATELY
     * (§5.2): the ack transaction is the primary fence and `expect` is the
     * secondary assertion. A state write that shares the transaction with the
     * ack is undone when an expired lease makes the ack fail -- something a
     * compare-and-set cannot do, because an `expect` on a version that still
     * matches succeeds even from a zombie consumer.
     */
    class TxKvBuilder {
    private:
        TransactionBuilder* parent_;
        std::string namespace_;

    public:
        TxKvBuilder(TransactionBuilder* parent, const std::string& ns)
            : parent_(parent), namespace_(ns) {
        }

        TransactionBuilder& get(const std::string& key) {
            parent_->kv_ops_.push_back(wire::kv_get_op(namespace_, key));
            return *parent_;
        }

        TransactionBuilder& put(const std::string& key, const json& value, const KvTtl& ttl,
                                const KvWriteOptions& options = KvWriteOptions()) {
            parent_->kv_ops_.push_back(
                wire::kv_write_op("put", namespace_, key, value, ttl, options));
            return *parent_;
        }

        TransactionBuilder& put_if_absent(const std::string& key, const json& value,
                                          const KvTtl& ttl,
                                          const KvWriteOptions& options = KvWriteOptions()) {
            parent_->kv_ops_.push_back(
                wire::kv_put_if_absent_op(namespace_, key, value, ttl, options));
            return *parent_;
        }

        // `del`, not `delete`: the keyword is taken, and it is the same spelling
        // QueueBuilder and HttpClient already use.
        TransactionBuilder& del(const std::string& key,
                                const KvWriteOptions& options = KvWriteOptions()) {
            parent_->kv_ops_.push_back(wire::kv_delete_op(namespace_, key, options));
            return *parent_;
        }

        TransactionBuilder& incr(const std::string& key, long long delta, const KvTtl& ttl,
                                 const KvIncrOptions& options = KvIncrOptions()) {
            parent_->kv_ops_.push_back(
                wire::kv_incr_op(namespace_, key, delta, ttl, options));
            return *parent_;
        }
    };

    /**
     * Timers that ride this bundle.
     *
     * A cancel sent here rides the bundle's own authorization, unlike the
     * standalone client.timers().cancel(), which has a route that is never
     * blockable (§9.6). Use the standalone one when the cancel must land
     * regardless; use this one when the cancel must be atomic with the ack.
     */
    class TxTimersBuilder {
    private:
        TransactionBuilder* parent_;

    public:
        explicit TxTimersBuilder(TransactionBuilder* parent) : parent_(parent) {
        }

        TransactionBuilder& schedule(const TimerSchedule& timer) {
            parent_->timer_ops_.push_back(wire::timer_schedule_op(timer));
            return *parent_;
        }

        TransactionBuilder& cancel(const std::string& queue, const std::string& timer_key,
                                   const std::string& txn = "") {
            parent_->timer_ops_.push_back(wire::timer_cancel_op(queue, timer_key, txn));
            return *parent_;
        }
    };

public:
    TransactionBuilder(std::shared_ptr<HttpClient> http_client)
        : http_client_(http_client) {
    }

    TransactionBuilder& ack(const json& message, const std::string& status = "completed", const json& context = json::object()) {
        std::vector<json> messages;
        if (message.is_array()) {
            messages = message.get<std::vector<json>>();
        } else {
            messages.push_back(message);
        }
        
        for (const auto& msg : messages) {
            std::string transaction_id;
            if (msg.is_string()) {
                transaction_id = msg.get<std::string>();
            } else if (msg.contains("transactionId")) {
                transaction_id = msg["transactionId"].get<std::string>();
            } else if (msg.contains("id")) {
                transaction_id = msg["id"].get<std::string>();
            } else {
                throw std::runtime_error("Message must have transactionId or id property");
            }
            
            if (!msg.is_object() || !msg.contains("partitionId")) {
                throw std::runtime_error("Message must have partitionId property to ensure message uniqueness");
            }
            
            std::string partition_id = msg["partitionId"].get<std::string>();
            
            json ack_op = {
                {"type", "ack"},
                {"transactionId", transaction_id},
                {"partitionId", partition_id},
                {"status", status}
            };
            
            // Add consumerGroup if provided in context
            if (context.contains("consumerGroup") && !context["consumerGroup"].is_null()) {
                ack_op["consumerGroup"] = context["consumerGroup"].get<std::string>();
            }
            
            if (msg.contains("leaseId") && !msg["leaseId"].is_null()) {
                std::string lease_id = msg["leaseId"].get<std::string>();
                required_leases_.push_back(lease_id);
            }
            
            operations_.push_back(ack_op);
        }
        
        return *this;
    }
    
    QueuePushBuilder queue(const std::string& queue_name) {
        return QueuePushBuilder(this, queue_name);
    }

    /** KV operations riding this bundle, bound to one namespace. */
    TxKvBuilder kv(const std::string& ns) {
        return TxKvBuilder(this, ns);
    }

    /** Timer operations riding this bundle. */
    TxTimersBuilder timers() {
        return TxTimersBuilder(this);
    }

    /**
     * Commit the bundle.
     *
     * WHERE THE TWO RIDER ARRAYS GO, AND WHY IT IS NOT NEGOTIABLE
     * (PLAN_KV_TIMERS.md §6.3, §8.2, §10.4). `kv` and `timers` are TOP-LEVEL
     * fields of the request body, beside `operations` and never inside it. The
     * reason is a silent failure in another client that this wire is shared
     * with: two Go struct fields carrying the same JSON key at the same level
     * are BOTH dropped by encoding/json, with no error and no warning. Growing
     * `operations` a `kv` leg would therefore let a body go out with ZERO KV
     * operations while the broker committed the transaction WITHOUT THE GATE --
     * the putIfAbsent the bundle existed for would simply never have happened,
     * and nothing anywhere would say so. C++ has no such failure mode, but the
     * shape is not this client's to choose.
     *
     * The arrays are omitted entirely when empty, never sent as `[]`: a bundle
     * that uses neither feature must be byte-identical to what this client sent
     * before the feature existed (§6.3), and the broker's own skip is written on
     * `jsonb_typeof` for the same reason.
     *
     * `results[]` is a FLAT index space with an append-only layout: the
     * operations first, exactly as today, then the KV array, then the timers.
     * A push or an ack never changes index because a rider is present.
     *
     * WHAT THIS RETURNS INSTEAD OF THROWING (§8.3). A lost `required` KV
     * precondition comes back as HTTP 200 with `success:false` and
     * `reason:"kv_precondition"`, and this method RETURNS it. That outcome is
     * the expected result of every legitimate redelivery -- it is the
     * idempotency marker doing its job -- and turning it into an exception
     * would put the single most frequent outcome of this product inside every
     * caller's error path, its retry policy and its error metrics. The body
     * carries `failedIndex` (in the flat space), `kvReason`, `version` and
     * `value`, so branch on those; never string-match the message.
     *
     * Every other failure still throws, and a refusal by the ladder (403/429)
     * still arrives as an HttpError from the transport layer.
     */
    json commit() {
        if (operations_.empty() && kv_ops_.empty() && timer_ops_.empty()) {
            throw std::runtime_error("Transaction has no operations to commit");
        }

        // Remove duplicate leases
        std::sort(required_leases_.begin(), required_leases_.end());
        required_leases_.erase(std::unique(required_leases_.begin(), required_leases_.end()),
                              required_leases_.end());

        json request = {
            {"operations", operations_},
            {"requiredLeases", required_leases_}
        };
        if (!kv_ops_.empty()) {
            request["kv"] = kv_ops_;
        }
        if (!timer_ops_.empty()) {
            request["timers"] = timer_ops_;
        }

        json result = http_client_->post("/api/v1/transaction", request);

        if (!result.contains("success") || !result["success"].get<bool>()) {
            if (result.is_object() && result.value("reason", std::string()) == "kv_precondition") {
                return result;
            }
            std::string error = result.contains("error") ?
                result["error"].get<std::string>() : "Transaction failed";
            throw std::runtime_error(error);
        }

        return result;
    }
};

// ============================================================================
// KvBuilder - the standalone KV surface (PLAN_KV_TIMERS.md §5, §8.1)
//
// Every operation goes through POST /api/v1/kv, which §8.1 calls the complete
// surface: it is the only route carrying `incr`, and using one route means one
// request shape to learn and one code path to keep correct. The path routes
// (GET/PUT/DELETE /api/v1/kv/:ns/{key}) exist as sugar for the handful of cases
// people write by hand with curl; this client does not use them, so it also
// does not read the `ETag` header they return -- which saves bandwidth, never
// the round trip to the database, since there is no cache in front of the KV
// and there will not be one (§8.5).
//
// WHAT THIS CLIENT DOES NOT HAVE, stated rather than implied (§10.2 gives the
// C++ client get / put / putIfAbsent / delete / incr and nothing else):
//   * `getMany` and `getPrefix`. Both exist on the broker's POST /api/v1/kv.
//     getPrefix is refused inside a transaction by design, since its cost is
//     not bounded by the caller.
//   * the `once` helper client-js has.
// Use the HTTP surface directly if you need them.
//
// STATUS CODES ARE NOT VERDICTS (§8.1). An absent key, a lost putIfAbsent race
// and a delete that hit nothing are all HTTP 200 with an explicit field, so
// they arrive here as VALUES and never as exceptions. Read `found` on a get and
// `applied` on a write. Do not test the returned json for truthiness.
// ============================================================================

class KvBuilder {
private:
    std::shared_ptr<HttpClient> http_client_;
    std::string namespace_;

    json apply(const json& op, const char* what) {
        json request = {{"operations", json::array({op})}};
        return wire::single_result(http_client_->post("/api/v1/kv", request), what);
    }

public:
    KvBuilder(std::shared_ptr<HttpClient> http_client, const std::string& ns)
        : http_client_(http_client), namespace_(ns) {
    }

    /**
     * Read one key. Returns {found, key, value, version, expiresAt, updatedAt}.
     *
     * `found` is separate from `value` because a JSON null is a legal value:
     * {found:true, value:null} and {found:false} are different things and this
     * client does not collapse them. An expired key is NEVER returned and never
     * counts as existing, whether or not the sweeper has pruned it yet (§5.7).
     */
    json get(const std::string& key) {
        return apply(wire::kv_get_op(namespace_, key), "kv get");
    }

    /**
     * Write a key. Returns {applied, key, value, version} and, when it did not
     * apply, `reason` from the closed taxonomy exists | absent | version |
     * limit | type -- with the CURRENT value and version, so the loser needs no
     * second round trip.
     *
     * The version handed to a loser is ADVISORY. It is not a fencing token to
     * reuse blindly: a row that failed the predicate was never locked, so the
     * number describes an instant that has already passed.
     */
    json put(const std::string& key, const json& value, const KvTtl& ttl,
             const KvWriteOptions& options = KvWriteOptions()) {
        return apply(wire::kv_write_op("put", namespace_, key, value, ttl, options), "kv put");
    }

    /**
     * Claim a key only if it does not exist -- the idempotency marker.
     *
     * Exactly one concurrent caller wins: the conflict arm takes the row lock
     * BEFORE evaluating its condition, so the second caller re-evaluates
     * against the new row and does not apply. It also wins against an expired
     * row that has not been pruned yet, which resurrects the key as a NEW
     * lineage.
     *
     * `putIfAbsent` plus a TTL is NOT a distributed lock. An expiry does not
     * revoke anything: the old holder keeps working, it simply no longer has
     * the row. The defence is fencing -- carry the `version` you got as
     * `expect` on every later write, so a lapsed holder fails with
     * reason "version" instead of overwriting the new one.
     */
    json put_if_absent(const std::string& key, const json& value, const KvTtl& ttl,
                       const KvWriteOptions& options = KvWriteOptions()) {
        return apply(wire::kv_put_if_absent_op(namespace_, key, value, ttl, options),
                     "kv putIfAbsent");
    }

    /** Delete a key. `applied:false, reason:"absent"` when there was nothing. */
    json del(const std::string& key, const KvWriteOptions& options = KvWriteOptions()) {
        return apply(wire::kv_delete_op(namespace_, key, options), "kv delete");
    }

    /**
     * Add to a numeric key, atomically, with an optional ceiling.
     *
     * With `max`, `applied` IS the admission decision: the call that would
     * break the ceiling does not apply, and the counter is untouched. Read the
     * returned `value` with kv_int64() if you need it as an integer -- the
     * server side is `numeric` and has no overflow, so the client is where the
     * representable range has to be checked.
     *
     * The TTL is CREATE-ONLY: an existing live key keeps its own expiry. An
     * expired key counts as zero and starts a NEW window, which is what makes a
     * fixed-window limiter a single call.
     */
    json incr(const std::string& key, long long delta, const KvTtl& ttl,
              const KvIncrOptions& options = KvIncrOptions()) {
        return apply(wire::kv_incr_op(namespace_, key, delta, ttl, options), "kv incr");
    }
};

// ============================================================================
// TimersBuilder - the standalone timer surface (PLAN_KV_TIMERS.md §4, §8.1)
//
// WHAT THIS CLIENT DOES NOT HAVE (§10.2 gives C++ schedule and cancel):
//   * `peek` (GET /api/v1/timers/:queue/{timerKey}) and `list`
//     (GET /api/v1/timers/:queue). Both exist on the broker.
//   * there is no separate `reschedule()`: schedule() IS the upsert, and the
//     returned `status` tells you which happened, "scheduled" or "rescheduled".
//     A rescheduled timer is a NEW timer under an old name -- its retry budget
//     resets, and it gets a new txn.
// ============================================================================

class TimersBuilder {
private:
    std::shared_ptr<HttpClient> http_client_;

public:
    explicit TimersBuilder(std::shared_ptr<HttpClient> http_client)
        : http_client_(http_client) {
    }

    /**
     * Schedule (or reschedule) one timer.
     *
     * Returns {ok, status, queue, timerKey, txn, messageId, deliverAt}, where
     * `status` is one of the closed taxonomy scheduled | rescheduled |
     * too_late. `too_late` means the row was already claimed by a broker about
     * to fire it: granting the reschedule would deliver the OLD payload after
     * the caller believes it replaced it. The window is bounded by the sweeper
     * lease. The remedy is a new timer key, or waiting for the delivery and
     * acting on the message.
     *
     * `messageId` is promised HERE, at schedule time, so the eventual frame can
     * be correlated before it exists.
     */
    json schedule(const TimerSchedule& timer) {
        json request = {{"operations", json::array({wire::timer_schedule_op(timer)})}};
        return wire::single_result(http_client_->post("/api/v1/timers", request),
                                   "timer schedule");
    }

    /**
     * Cancel a pending timer.
     *
     * THIS GOES THROUGH ITS OWN ROUTE ON PURPOSE (§9.6). A cancel sent inside
     * POST /api/v1/timers inherits the SCHEDULE's authorization class, so on a
     * cluster whose quota is full it is refused -- and a tenant that cannot
     * cancel keeps producing messages it cannot stop, because the fire never
     * switches itself off. DELETE /api/v1/timers/:queue/{timerKey} is the route
     * that is guaranteed to work, and it is the one an SDK must use.
     *
     * Returns {ok, status, queue, timerKey, txn}. Two answers deserve care:
     *
     *   `cancelled`  ok:true. It will not fire.
     *   `absent`     ok:FALSE. There is no tombstone: a delivered timer has no
     *                row left, so `absent` means "NO LONGER PENDING", which
     *                INCLUDES already delivered. The authority is the log --
     *                look for this txn in the destination queue, which is why
     *                the answer hands the txn back. A cancel against another
     *                tenant's timer also answers `absent`.
     *   `too_late`   ok:false. A broker holds the claim and is about to fire.
     *
     * Which is why a compensation consumer must check the saga's KV state
     * before compensating: the timer may have fired five milliseconds before
     * your cancel arrived, and `absent` will not tell you that it did.
     */
    json cancel(const std::string& queue, const std::string& timer_key,
                const std::string& txn = "") {
        std::string path = "/api/v1/timers/" + util::url_encode(queue) + "/" +
                           util::url_encode(timer_key);
        if (!txn.empty()) {
            path += "?txn=" + util::url_encode(txn);
        }
        return http_client_->del(path);
    }
};

// ============================================================================
// EphemeralBuilder - RAM-class queues (EPHEMERAL_QUEUES.md §1, §3.1, §4)
//
// Eight verbs over one route family, /api/v1/ephemeral/*: configure, reset,
// del, push, pop, ack, queues, depth. Flat methods, not a builder chain -- the
// durable queue("x").partition("p").push(...) fluency exists because a durable
// queue has a dozen configured properties that read well as a sentence; an
// ephemeral queue has a ring in a broker's RAM and a handful of bounds, and a
// chain would only hide how few moving parts there are.
//
// WHAT THIS CLASS IS ABOUT, BEFORE ANY SIGNATURE: contents survive NOTHING
// (§1.2). Not a restart, not a crash, not a deploy, not the ownership move a
// membership change causes. Treat a failover like a Redis restart. Declared
// CONFIGURATION is durable -- it lives in PG and comes back after a restart, as
// configured and EMPTY. There is no replay, no history, no subscriptionMode and
// no DLQ, because none of those concepts has a referent when there is no
// history to have.
//
// DELIVERY IS NOT "AT MOST ONCE" (§1.3), and the docs must not say it is. The
// class picks what can be LOST; the ack mode picks the guarantee. `auto_ack`
// advances the cursor at delivery and is at-most-once. The default -- explicit
// ack -- is at-least-once for as long as the owning broker incarnation lives:
// an unacked message redelivers when its lease expires, with `attempts`
// incremented, until retryLimit, after which it is DROPPED and counted.
// Consumers still need idempotency, exactly as on durable queues.
//
// CONSUMPTION SEMANTICS COME FROM THE GROUP, EXACTLY AS ON THE DURABLE ENGINE
// (§1.5): same group = competing consumers, own group = fan-out, no group = the
// queue mode. There is no queue-level mode to choose.
//
// WHAT THIS CLIENT DOES NOT HAVE, and why. The JS, Python and PHP SDKs also
// offer a BUFFERED ephemeral push, which reuses their 1.0.6 buffer machinery
// through a parametrized drain (§4.1). This client's BufferManager now honours
// that same contract -- the maxSize bound, a failed batch back at the FRONT and
// retried, nothing dropped (see test_buffer.cpp) -- but it is not yet wired to
// this surface: there is no sink parameter on the drain, so an ephemeral push
// has no way to say WHERE its batch goes. Ephemeral pushes here are unbuffered
// until a follow-up adds that parameter; nothing in the buffer's behaviour
// stands in the way any more.
// ============================================================================

class EphemeralBuilder {
private:
    std::shared_ptr<HttpClient> http_client_;

    /** Long-poll default, matching the durable pop's, when wait is asked for. */
    static constexpr int DEFAULT_WAIT_TIMEOUT_MILLIS = 30000;

    /**
     * The HTTP deadline must outlive the server's own long-poll timeout, or the
     * client aborts a request the broker was about to answer. Same 5s slack the
     * durable pop uses.
     */
    static constexpr int WAIT_TIMEOUT_SLACK_MILLIS = 5000;

    static void require_queue(const std::string& queue) {
        if (queue.empty()) {
            throw std::invalid_argument("ephemeral: queue must be a non-empty string");
        }
    }

    /**
     * Every request in this class goes through here, so the two 404 rules have
     * ONE home. THE BODY'S CODE decides which one, not the status:
     *
     *   * `ephemeral_queue_not_found` -- the routes are there and answered; the
     *     QUEUE is not. Only `depth` can say this (§3.1): push and pop create
     *     implicitly, `reset` answers dropped:0, `del` answers deleted:false. It
     *     is checked for on every verb anyway, because which verbs can say it is
     *     the broker's business and this client should not re-encode that list.
     *   * anything else -- an old broker that never registered the routes, or an
     *     old proxy answering `route_blocked` because it fails closed on unknown
     *     API paths. Both mean "upgrade".
     *
     * `queue` is passed only so a missing-queue error can name it.
     */
    template <typename Fn>
    json call(Fn&& fn, const std::string& queue = "") {
        try {
            return fn();
        } catch (const EphemeralUnsupportedError&) {
            throw;
        } catch (const EphemeralQueueNotFoundError&) {
            throw;
        } catch (const HttpError& error) {
            if (error.status_code() == 404) {
                if (error.code() == EphemeralQueueNotFoundError::CODE) {
                    throw EphemeralQueueNotFoundError(error, queue);
                }
                throw EphemeralUnsupportedError(error);
            }
            throw;
        }
    }

public:
    explicit EphemeralBuilder(std::shared_ptr<HttpClient> http_client)
        : http_client_(http_client) {
    }

    // ---------------------------------------------------------------- declare

    /**
     * Declare a queue and its bounds. Persists the OPTIONS in PG (§1.1): the
     * configuration survives a restart, the contents never do, and the queue
     * comes back declared and empty.
     *
     * Optional in every sense -- a push or a pop that names an unknown queue
     * creates it implicitly with the tenant defaults. Declare when you want
     * non-default bounds, or when you want the queue to exist in the dashboard
     * before its first message.
     */
    json configure(const std::string& queue,
                   const EphemeralOptions& options = EphemeralOptions()) {
        require_queue(queue);
        return call([&] {
            return http_client_->post("/api/v1/ephemeral/configure",
                                      wire::eph_configure_body(queue, options));
        }, queue);
    }

    /**
     * Drop every message, void every lease, rewind every group cursor. Answers
     * {dropped}.
     *
     * A verb that would be indefensible on a durable queue and is merely honest
     * here: it destroys nothing the class ever promised to keep (§1.2). The
     * declared configuration stays.
     */
    json reset(const std::string& queue) {
        require_queue(queue);
        return call([&] {
            return http_client_->post("/api/v1/ephemeral/reset", json{{"queue", queue}});
        }, queue);
    }

    /**
     * Delete the queue: contents, cursors, and the declared configuration in
     * PG. Named `del` because `delete` is a keyword, exactly as on KvBuilder.
     */
    json del(const std::string& queue) {
        require_queue(queue);
        return call([&] {
            return http_client_->del("/api/v1/ephemeral/queue/" + util::url_encode(queue));
        }, queue);
    }

    // ------------------------------------------------------------------- push

    /**
     * Push one message or many. All-or-nothing per request; answers {pushed}.
     *
     *     client.ephemeral().push("presence", json::array({{{"typing", true}}}));
     *     client.ephemeral().push("presence", msgs, {"room-7"});
     *
     * Each message may be a bare value, {"payload": …} or {"data": …} -- the
     * durable push's sugar, trap included (see wire::eph_message).
     */
    json push(const std::string& queue, const json& messages,
              const EphemeralPushOptions& options = EphemeralPushOptions()) {
        require_queue(queue);
        if (messages.is_array() && messages.empty()) {
            return json{{"pushed", 0}};
        }
        json body = wire::eph_push_body(queue, options.partition, messages);
        return call([&] {
            return http_client_->post("/api/v1/ephemeral/push", body);
        }, queue);
    }

    // -------------------------------------------------------------------- pop

    /**
     * Take up to `batch` messages. Answers {queue, messages}, with `messages`
     * an EMPTY ARRAY when there was nothing -- never null, so iterating the
     * result is always safe even on an idle queue or a bodiless 204.
     *
     * Each message is {id, partition, payload, attempts}. The `id` is opaque:
     * it encodes the owning broker incarnation, which is what lets an ack that
     * arrives after a restart or an ownership move answer `stale` instead of
     * acking somebody else's message.
     *
     * `wait` is a real long poll (§3.4) -- no database behind it and no polling
     * interval anywhere, which is the structural reason an ephemeral inbox
     * answers in transport time. The HTTP deadline is set past the broker's own
     * timeout so the broker's timeout always fires first.
     */
    json pop(const std::string& queue,
             const EphemeralPopOptions& options = EphemeralPopOptions()) {
        require_queue(queue);

        int timeout_millis = options.timeout_millis > 0 ? options.timeout_millis
                                                        : DEFAULT_WAIT_TIMEOUT_MILLIS;

        std::stringstream path;
        path << "/api/v1/ephemeral/pop?queue=" << util::url_encode(queue);
        if (!options.partition.empty()) {
            path << "&partition=" << util::url_encode(options.partition);
        }
        if (options.batch > 0) {
            path << "&batch=" << options.batch;
        }
        // Sent only when true, so a plain pop is the shortest query this route
        // can receive and the broker's own defaults own everything else.
        if (options.wait) {
            path << "&wait=true";
            path << "&timeout=" << timeout_millis;
        }
        if (!options.group.empty()) {
            path << "&group=" << util::url_encode(options.group);
        }
        if (options.auto_ack) {
            path << "&autoAck=true";
        }

        json result = call([&] {
            return http_client_->get(
                path.str(),
                options.wait ? timeout_millis + WAIT_TIMEOUT_SLACK_MILLIS : 0,
                // A long poll that meets a 429 should back off and keep waiting
                // rather than give up after a handful of tries.
                options.wait ? RetryKind::Pop : RetryKind::Default);
        }, queue);

        json messages = json::array();
        if (result.is_object() && result.contains("messages") && result["messages"].is_array()) {
            for (const auto& message : result["messages"]) {
                if (!message.is_null()) {
                    messages.push_back(message);
                }
            }
        }

        std::string answered_queue = queue;
        if (result.is_object() && result.contains("queue") && result["queue"].is_string()) {
            answered_queue = result["queue"].get<std::string>();
        }

        return json{{"queue", answered_queue}, {"messages", messages}};
    }

    // -------------------------------------------------------------------- ack

    /**
     * Acknowledge popped messages. Answers {results:[{id, outcome}]} with
     * `outcome` in {acked, redelivered, stale, unknown}.
     *
     * `stale` is NOT an error and never arrives as one: it is the answer to an
     * ack whose message belonged to a previous incarnation of the ring, which
     * is how this class fences a restart or an ownership move without a lease
     * protocol.
     *
     * A failed or retried message comes back with attempts+1 until retryLimit,
     * then it is dropped and counted. There is no DLQ.
     */
    json ack(const std::string& queue, const json& acks,
             const EphemeralAckOptions& options = EphemeralAckOptions()) {
        require_queue(queue);
        if (acks.is_array() && acks.empty()) {
            return json{{"results", json::array()}};
        }
        json body = wire::eph_ack_body(queue, acks, options);
        return call([&] {
            return http_client_->post("/api/v1/ephemeral/ack", body);
        }, queue);
    }

    /** The boolean sugar: true is `completed`, false is `failed`. */
    json ack(const std::string& queue, const json& acks, bool status,
             const std::string& group = "") {
        EphemeralAckOptions options;
        options.group = group;
        options.status = status ? "completed" : "failed";
        return ack(queue, acks, options);
    }

    // ----------------------------------------------------------------- status

    /**
     * Every ephemeral queue this tenant currently has, declared and implicit.
     *
     * Free to poll: the gauges are read out of the broker's own memory, with no
     * database behind them -- unlike the durable meter, whose 1s poll is
     * load-bearing on PG.
     */
    json queues() {
        return call([&] {
            return http_client_->get("/api/v1/ephemeral/queues");
        });
    }

    /**
     * Depth gauges for one queue: ring length, bytes, and the group cursors.
     *
     * THE ONLY VERB THAT CAN TELL YOU A QUEUE IS MISSING. Everything else either
     * creates the queue (push, pop) or answers a normal body about having done
     * nothing (`reset` -> dropped:0, `del` -> deleted:false). Here an unknown
     * queue throws EphemeralQueueNotFoundError -- a different fact from
     * EphemeralUnsupportedError, which is about the broker's version, and worth
     * distinguishing precisely because both are 404s.
     */
    json depth(const std::string& queue) {
        require_queue(queue);
        return call([&] {
            return http_client_->get("/api/v1/ephemeral/queues/" + util::url_encode(queue) +
                                     "/depth");
        }, queue);
    }
};

// ============================================================================
// DLQBuilder - Dead Letter Queue query builder
// ============================================================================

class DLQBuilder {
private:
    std::shared_ptr<HttpClient> http_client_;
    std::string queue_name_;
    std::string consumer_group_;
    std::string partition_;
    int limit_ = 100;
    int offset_ = 0;
    std::string from_;
    std::string to_;
    
public:
    DLQBuilder(std::shared_ptr<HttpClient> http_client, const std::string& queue_name,
              const std::string& consumer_group = "", const std::string& partition = "")
        : http_client_(http_client), queue_name_(queue_name),
          consumer_group_(consumer_group), partition_(partition) {
    }
    
    DLQBuilder& limit(int count) {
        limit_ = std::max(1, count);
        return *this;
    }
    
    DLQBuilder& offset(int count) {
        offset_ = std::max(0, count);
        return *this;
    }
    
    DLQBuilder& from(const std::string& timestamp) {
        from_ = timestamp;
        return *this;
    }
    
    DLQBuilder& to(const std::string& timestamp) {
        to_ = timestamp;
        return *this;
    }
    
    json get() {
        std::stringstream params;
        params << "?queue=" << util::url_encode(queue_name_);
        params << "&limit=" << limit_;
        params << "&offset=" << offset_;
        
        if (!consumer_group_.empty()) {
            params << "&consumerGroup=" << util::url_encode(consumer_group_);
        }
        
        if (!partition_.empty()) {
            params << "&partition=" << util::url_encode(partition_);
        }
        
        if (!from_.empty()) {
            params << "&from=" << util::url_encode(from_);
        }
        
        if (!to_.empty()) {
            params << "&to=" << util::url_encode(to_);
        }
        
        try {
            json result = http_client_->get("/api/v1/dlq" + params.str());
            if (result.is_null()) {
                return {{"messages", json::array()}, {"total", 0}};
            }
            return result;
        } catch (const std::exception& e) {
            util::log_error("DLQBuilder.get", std::string("Error: ") + e.what());
            return {{"messages", json::array()}, {"total", 0}};
        }
    }
};

// ============================================================================
// ConsumerManager - Manages concurrent workers using ThreadPool
// ============================================================================

class ConsumerManager {
private:
    std::shared_ptr<HttpClient> http_client_;
    QueenClient* queen_;
    astp::ThreadPool thread_pool_;
    
    void enhance_message_with_trace(json& message, const std::string& consumer_group) {
        // Note: In C++, we can't add methods to json objects like in JavaScript
        // Instead, we provide a separate trace function that takes the message
        // The user would call: queen.trace(message, trace_config)
    }
    
public:
    ConsumerManager(std::shared_ptr<HttpClient> http_client, QueenClient* queen,
                   int pool_size = std::thread::hardware_concurrency())
        : http_client_(http_client), queen_(queen), thread_pool_(pool_size) {
    }
    
    ~ConsumerManager() {
        thread_pool_.wait();
    }
    
    void start(std::function<void(const json&)> handler, const ConsumeOptions& options);
    // Implementation will be defined after QueenClient declaration
};

// ============================================================================
// QueueBuilder - Fluent API for queue operations
// ============================================================================

class QueueBuilder {
private:
    QueenClient* queen_;
    std::shared_ptr<HttpClient> http_client_;
    std::shared_ptr<BufferManager> buffer_manager_;
    std::string queue_name_;
    std::string partition_ = "Default";
    std::string namespace_;
    std::string task_;
    std::string group_;
    QueueConfig config_;
    
    // Consume options.
    //
    // batch_ / max_partitions_ hold the USER's value, and 0 means the setter was
    // never called -- which is the dimension pop autopilot gets to choose. The
    // client-side defaults are applied at emission time (util::pop_sizing), not
    // here, because filling them in here would erase the difference between
    // "never called batch()" and "called batch(1)".
    int concurrency_ = 1;
    int batch_ = 0;
    int limit_ = 0;
    int idle_millis_ = 0;
    bool auto_ack_ = true;
    bool wait_ = true;
    int timeout_millis_ = 30000;
    bool renew_lease_ = false;
    int renew_lease_interval_millis_ = 60000;
    std::string subscription_mode_;
    std::string subscription_from_;
    bool each_ = false;
    int max_partitions_ = 0;
    bool conflation_ = false;
    /// Per-builder override: unset = the client default.
    std::optional<bool> autopilot_;

    // Buffer options
    std::optional<BufferOptions> buffer_options_;
    
public:
    QueueBuilder(QueenClient* queen, std::shared_ptr<HttpClient> http_client,
                std::shared_ptr<BufferManager> buffer_manager, const std::string& queue_name = "")
        : queen_(queen), http_client_(http_client), buffer_manager_(buffer_manager),
          queue_name_(queue_name) {
    }
    
    // Queue configuration methods
    QueueBuilder& namespace_name(const std::string& name) {
        namespace_ = name;
        return *this;
    }
    
    QueueBuilder& task(const std::string& name) {
        task_ = name;
        return *this;
    }
    
    QueueBuilder& config(const QueueConfig& cfg) {
        config_ = cfg;
        return *this;
    }
    
    json create() {
        json payload = {
            {"queue", queue_name_},
            {"namespace", namespace_},
            {"task", task_},
            {"options", config_.to_json()}
        };
        
        return http_client_->post("/api/v1/configure", payload);
    }
    
    json del() {
        if (queue_name_.empty()) {
            throw std::runtime_error("Queue name is required for delete operation");
        }
        
        std::string path = "/api/v1/resources/queues/" + util::url_encode(queue_name_);
        return http_client_->del(path);
    }
    
    // Push methods
    QueueBuilder& partition(const std::string& name) {
        partition_ = name;
        return *this;
    }
    
    QueueBuilder& buffer(const BufferOptions& options) {
        buffer_options_ = options;
        return *this;
    }
    
    json push(const std::vector<json>& payload) {
        if (queue_name_.empty()) {
            throw std::runtime_error("Queue name is required for push operation");
        }
        
        json formatted_items = json::array();
        
        for (const auto& item : payload) {
            json payload_value;
            if (item.contains("data")) {
                payload_value = item["data"];
            } else if (item.contains("payload")) {
                payload_value = item["payload"];
            } else {
                payload_value = item;
            }
            
            json formatted = {
                {"queue", queue_name_},
                {"partition", partition_},
                {"payload", payload_value},
                {"transactionId", item.contains("transactionId") ? 
                    item["transactionId"].get<std::string>() : util::generate_uuid_v7()}
            };
            
            if (item.contains("traceId") && item["traceId"].is_string()) {
                std::string trace_id = item["traceId"].get<std::string>();
                if (util::is_valid_uuid(trace_id)) {
                    formatted["traceId"] = trace_id;
                }
            }
            
            formatted_items.push_back(formatted);
        }
        
        // Client-side buffering
        if (buffer_options_.has_value()) {
            for (const auto& item : formatted_items) {
                std::string queue_address = queue_name_ + "/" + partition_;
                buffer_manager_->add_message(queue_address, item, buffer_options_.value());
            }
            
            return {
                {"buffered", true},
                {"count", formatted_items.size()}
            };
        }
        
        // Immediate push
        json request = {{"items", formatted_items}};
        return http_client_->post("/api/v1/push", request);
    }
    
    // Consume configuration methods
    QueueBuilder& group(const std::string& name) {
        group_ = name;
        return *this;
    }
    
    QueueBuilder& concurrency(int count) {
        concurrency_ = std::max(1, count);
        return *this;
    }
    
    // Pin the message budget for one pop. Leave it unset and the broker sizes it
    // (see autopilot below), where it used to mean the client-side default of 1.
    // batch(0) is not "a batch of zero" and never was: it is the absence of an
    // opinion, so it reads as unset.
    QueueBuilder& batch(int size) {
        batch_ = size > 0 ? size : 0;
        return *this;
    }

    // v4 multi-partition pop: claim up to N partitions per call. With
    // partitions(N), batch(B) becomes a global cap on total messages
    // returned across all claimed partitions. All N share one leaseId,
    // so a single renew() call extends them all atomically.
    // Leave it unset and the broker chooses the sweep width (see autopilot
    // below); partitions(1) pins the legacy single-partition behaviour, which is
    // a decision the broker is told about and never overrides.
    QueueBuilder& partitions(int n) {
        max_partitions_ = n > 0 ? n : 0;
        return *this;
    }

    /**
     * Turn broker-side pop sizing on or off for this builder.
     *
     * On (the default) the broker chooses batch and partitions for the pops of
     * this builder, per pop, from state this client cannot see. Even then, a
     * knob set explicitly travels on the wire as it always did and is never
     * second-guessed: autopilot only ever fills the knobs left unset.
     *
     * autopilot(false) restores this SDK's pre-1.2 behaviour byte for byte: the
     * client-side defaults come back (batch 1, partitions 1) and no autopilot
     * parameter is sent. QUEEN_SDK_POP_AUTOPILOT=off does the same for a whole
     * process; an explicit call here outranks the environment in both
     * directions.
     *
     * Setting BOTH batch and partitions leaves autopilot nothing to decide, so
     * no autopilot parameter is sent in that case either, whatever this says.
     */
    QueueBuilder& autopilot(bool enabled = true) {
        autopilot_ = enabled;
        return *this;
    }
    
    QueueBuilder& limit(int count) {
        limit_ = count;
        return *this;
    }
    
    QueueBuilder& idle_millis(int millis) {
        idle_millis_ = millis;
        return *this;
    }
    
    QueueBuilder& auto_ack(bool enabled) {
        auto_ack_ = enabled;
        return *this;
    }
    
    QueueBuilder& renew_lease(bool enabled, int interval_millis = 60000) {
        renew_lease_ = enabled;
        if (interval_millis > 0) {
            renew_lease_interval_millis_ = interval_millis;
        }
        return *this;
    }
    
    QueueBuilder& subscription_mode(const std::string& mode) {
        subscription_mode_ = mode;
        return *this;
    }
    
    QueueBuilder& subscription_from(const std::string& from) {
        subscription_from_ = from;
        return *this;
    }

    /**
     * Last-value delivery for this consumer group (PLAN_CONFLATION.md §1.1):
     * under backlog the group is served ONE message per partition -- the newest
     * -- instead of the whole backlog. Sits beside subscription_mode() because
     * it is the same kind of fact: a delivery policy of the GROUP, fixed when
     * the group is first registered and never re-negotiated by a later pop.
     *
     *   client.queue("recompute").group("workers").conflation().consume(handler);
     *
     * Requires a broker >= 1.1.0: an older one ignores the flag and delivers the
     * whole backlog, which pop() and consume() detect on the first response and
     * refuse to continue past (ConflationUnsupportedError).
     *
     * NEEDS group(). Conflation is a policy of a consumer group, and a group-less
     * pop is queue mode, which has no group identity to hang one on -- the broker
     * refuses that combination with a 400 (§3.3).
     *
     * Composes with this client's auto_ack(), which defaults to true. The
     * combination §3.3 refuses is the broker-side `autoAck` query param, which
     * commits at delivery with no lease -- this SDK never sends it. auto_ack()
     * here means "ack after the handler returns", i.e. the leased at-least-once
     * shape conflation is built on, so the §1.3 guarantee holds.
     */
    QueueBuilder& conflation(bool enabled = true) {
        conflation_ = enabled;
        return *this;
    }

    QueueBuilder& each() {
        each_ = true;
        return *this;
    }
    
    // Consume method
    void consume(std::function<void(const json&)> handler, std::atomic<bool>* stop_signal = nullptr);

    /**
     * This builder's resolved autopilot decision: its own flag when set,
     * otherwise the client-wide default settled in the QueenClient constructor.
     * Defined out of line, below QueenClient, because it reads the client.
     */
    bool autopilot_enabled() const;
    // Implementation will be defined after QueenClient declaration
    
    // Pop methods
    QueueBuilder& wait(bool enabled) {
        wait_ = enabled;
        return *this;
    }
    
    json pop() {
        return pop_with_decision().messages;
    }

    /**
     * Claim messages and report what the broker chose for this pop.
     *
     * The same call as pop() -- this is the shape that also carries the additive
     * `autopilot` echo:
     *
     *   auto res = client.queue("events").group("workers").pop_result();
     *   if (res.autopilot.present) {
     *       std::cout << res.autopilot.partitions << " partitions, batch "
     *                 << res.autopilot.batch << "\n";
     *   }
     */
    PopResult pop_result() {
        return pop_with_decision();
    }

private:
    PopResult pop_with_decision() {
        std::stringstream path;
        
        if (!queue_name_.empty()) {
            if (partition_ != "Default") {
                path << "/api/v1/pop/queue/" << util::url_encode(queue_name_)
                     << "/partition/" << util::url_encode(partition_);
            } else {
                path << "/api/v1/pop/queue/" << util::url_encode(queue_name_);
            }
        } else if (!namespace_.empty() || !task_.empty()) {
            path << "/api/v1/pop";
        } else {
            throw std::runtime_error("Must specify queue, namespace, or task for pop operation");
        }
        
        // Batch, partitions and with them the autopilot flag. The RULE for which
        // of the three travel lives in one place (util::pop_sizing) because
        // consume() builds its query string separately; only the PLACEMENT is
        // here, and it is the pre-autopilot placement so an autopilot-off request
        // is byte-identical to the one this SDK used to send.
        const util::PopSizing sizing =
            util::pop_sizing(batch_, max_partitions_, 1, autopilot_enabled());

        std::stringstream params;
        params << "?";
        if (sizing.autopilot) {
            params << "autopilot=true&";
        }
        if (sizing.has_batch) {
            params << "batch=" << sizing.batch << "&";
        }
        params << "wait=" << (wait_ ? "true" : "false");
        params << "&timeout=" << timeout_millis_;
        
        if (!group_.empty()) {
            params << "&consumerGroup=" << util::url_encode(group_);
        }
        if (!namespace_.empty()) {
            params << "&namespace=" << util::url_encode(namespace_);
        }
        if (!task_.empty()) {
            params << "&task=" << util::url_encode(task_);
        }
        if (!subscription_mode_.empty()) {
            params << "&subscriptionMode=" << util::url_encode(subscription_mode_);
        }
        if (!subscription_from_.empty()) {
            params << "&subscriptionFrom=" << util::url_encode(subscription_from_);
        }
        // v4 multi-partition pop: drain up to N sparse partitions per call. Under
        // autopilot a pinned width travels even when it is 1, because 1 is then a
        // decision and not the absence of one.
        if (sizing.has_partitions) {
            params << "&partitions=" << sizing.partitions;
        }
        // PLAN_CONFLATION §3.1: emitted ONLY when true. `conflation=false` is
        // never sent -- it would read as "turn this group's policy off", which a
        // pop is not allowed to do (the stored setting wins), and against a
        // conflating group it would merely book a conflict.
        if (conflation_) {
            params << "&conflation=true";
        }

        try {
            int client_timeout = wait_ ? timeout_millis_ + 5000 : timeout_millis_;
            // A waiting pop backs off indefinitely through a 429 rather than
            // giving up after a handful of tries.
            json result = http_client_->get(path.str() + params.str(), client_timeout,
                                            wait_ ? RetryKind::Pop : RetryKind::Default);

            // PLAN_CONFLATION §4, degrade loudly. BEFORE the empty-response
            // return: an old broker answers an idle queue with a bodiless 204,
            // and that is the likeliest first contact -- returning [] there
            // would hide the version mismatch until the queue happened to have
            // a backlog, i.e. until it did damage.
            util::enforce_conflation_contract(
                conflation_, result,
                util::pop_target_label(queue_name_, namespace_, task_), group_);

            // The broker's own account of how it sized this pop, when the
            // request engaged autopilot and the answer had a body to carry it (a
            // bodiless 204 cannot, so an empty short pop reports nothing).
            PopResult out;
            out.autopilot = util::parse_autopilot_decision(result);

            if (result.is_null() || !result.contains("messages")) {
                return out;
            }

            out.messages = result["messages"];
            return out;
        } catch (const ConflationUnsupportedError&) {
            // Deliberately NOT swallowed into the [] contract below. This method
            // returns [] for anything the caller can retry through; a broker that
            // cannot conflate is not that -- every [] returned here would be a
            // backlog silently drained one message at a time (§4).
            throw;
        } catch (const HttpError& e) {
            // Also covers a 429 whose retry429 budget was exhausted and a
            // terminal 403 (e.g. cluster_suspended): both are logged with their
            // code rather than thrown, matching this method's swallow-to-[]
            // contract.
            util::log_error("QueueBuilder.pop", std::string("Error: ") + e.what() +
                " (status=" + std::to_string(e.status_code()) + ", code=" + e.code() + ")");
            return PopResult{};
        } catch (const std::exception& e) {
            util::log_error("QueueBuilder.pop", std::string("Error: ") + e.what());
            return PopResult{};
        }
    }

public:
    
    // Buffer management
    /**
     * Flush this builder's queue/partition buffer. With no deadline (the
     * default) a failing batch is retried until it lands or the client
     * closes; with one, a BufferFlushError after deadline_millis reports how
     * many messages are still buffered (none dropped).
     */
    void flush_buffer(int deadline_millis = -1) {
        if (queue_name_.empty()) {
            throw std::runtime_error("Queue name is required for buffer flush");
        }
        std::string queue_address = queue_name_ + "/" + partition_;
        buffer_manager_->flush_buffer(queue_address, deadline_millis);
    }
    
    // DLQ methods
    DLQBuilder dlq(const std::string& consumer_group = "") {
        if (queue_name_.empty()) {
            throw std::runtime_error("Queue name is required for DLQ operations");
        }
        return DLQBuilder(http_client_, queue_name_, consumer_group, 
                         partition_ != "Default" ? partition_ : "");
    }
};

// ============================================================================
// QueenClient - Main client class
// ============================================================================

class QueenClient {
private:
    ClientConfig config_;
    std::shared_ptr<HttpClient> http_client_;
    std::shared_ptr<BufferManager> buffer_manager_;
    std::atomic<bool> shutdown_requested_{false};
    // Process-wide kill switch for pop autopilot, settled at construction from
    // QUEEN_SDK_POP_AUTOPILOT. Read ONCE rather than on every pop: it is a
    // deployment-level rollback, and re-reading it per request would let a
    // running process change wire shape halfway through. A per-builder
    // .autopilot(..) still outranks it.
    bool autopilot_off_ = util::autopilot_disabled_by_env();
    
    void setup_graceful_shutdown() {
        // Register signal handlers
        std::signal(SIGINT, [](int) {
            std::cout << "\nReceived SIGINT, shutting down gracefully..." << std::endl;
            // Note: In production, you'd need a global instance pointer or better signal handling
            exit(0);
        });
        
        std::signal(SIGTERM, [](int) {
            std::cout << "\nReceived SIGTERM, shutting down gracefully..." << std::endl;
        });
    }
    
public:
    QueenClient(const std::string& url) {
        config_.urls = {url};
        config_.timeout_millis = 30000;
        config_.retry_attempts = 3;
        config_.retry_delay_millis = 1000;
        config_.load_balancing_strategy = "round-robin";
        config_.enable_failover = true;
        
        http_client_ = std::make_shared<HttpClient>(url, config_.timeout_millis,
            config_.retry_attempts, config_.retry_delay_millis);
        
        buffer_manager_ = std::make_shared<BufferManager>(http_client_);
        
        setup_graceful_shutdown();
    }
    
    // Authenticated (proxy/cloud) use goes through this constructor:
    //   ClientConfig config; config.bearer_token = "...";
    //   QueenClient client({"https://cell.example"}, config);
    QueenClient(const std::vector<std::string>& urls, const ClientConfig& config = ClientConfig())
        : config_(config) {
        config_.urls = urls;
        
        if (urls.size() == 1) {
            http_client_ = std::make_shared<HttpClient>(urls[0], config_.timeout_millis,
                config_.retry_attempts, config_.retry_delay_millis,
                config_.bearer_token, config_.retry_429);
        } else {
            auto load_balancer = std::make_shared<LoadBalancer>(urls, config_.load_balancing_strategy);
            http_client_ = std::make_shared<HttpClient>(load_balancer, config_.timeout_millis,
                config_.retry_attempts, config_.retry_delay_millis, config_.enable_failover,
                config_.bearer_token, config_.retry_429);
        }
        
        buffer_manager_ = std::make_shared<BufferManager>(http_client_);
        
        setup_graceful_shutdown();
    }
    
    QueueBuilder queue(const std::string& name = "") {
        return QueueBuilder(this, http_client_, buffer_manager_, name);
    }

    /**
     * Whether pop autopilot is off for this client because the environment
     * asked (util::ENV_POP_AUTOPILOT). Read by the builders; a per-call
     * .autopilot(..) still outranks it.
     */
    bool is_autopilot_off() const {
        return autopilot_off_;
    }
    
    TransactionBuilder transaction() {
        return TransactionBuilder(http_client_);
    }

    /**
     * The transactional key/value surface, bound to one namespace
     * (PLAN_KV_TIMERS.md §5).
     *
     * A namespace is `^[a-z0-9][a-z0-9._-]{0,63}$` and is NOT a table to
     * enumerate: there is no listing without a prefix, and this client has no
     * prefix read at all.
     *
     * THE RULE THAT DECIDES EVERYTHING ELSE (§5.2): a read-modify-write across
     * two calls is safe ONLY when the KV key derives from the partition key.
     * Then the lanes serialize and the key has no other writer inside that
     * consumer group -- two different groups on the same partition still race.
     * When it does not derive, use the atomics (`incr`) or the precondition
     * (`expect`). And put `expect` in even when you believe the lane serializes
     * you: if it never fails it cost nothing, and the day it fails you have
     * just discovered that two consumers are serving one partition, with a
     * verdict instead of a wrong total.
     *
     * This surface is on every cell. There is nothing to switch on before
     * using it and nothing to probe for: KV is part of the engine, like push
     * and pop, and the broker has no boot flag that could leave it out.
     *
     * An operator can still PAUSE it during an incident (the runtime kill
     * switch), and that is a different thing from an absent feature: the
     * routes answer **503** with `Retry-After` -- temporary, come back -- and
     * inside `transaction()` the kv rider is refused permanently with a 403.
     * Both arrive as an HttpError. Nothing answers 404 for being switched off.
     */
    KvBuilder kv(const std::string& ns) {
        return KvBuilder(http_client_, ns);
    }

    /**
     * The scheduled-message surface (PLAN_KV_TIMERS.md §4).
     *
     * A timer is a promise to push a message into a queue later. `deliverAt` is
     * "NOT BEFORE", never "exactly at". Two timers on the same queue and
     * partition that mature in the same batch enter the log in order of
     * EXPIRY, not in order of scheduling.
     *
     * Billing follows the promise, not the delivery: a schedule counts one
     * message, a reschedule counts another, a cancel counts zero and is not
     * refunded, and the fire counts zero.
     *
     * Like `kv()`, this surface is on every cell and needs nothing turned on
     * first. An operator's runtime kill switch can pause the SCHEDULE (503 on
     * the routes, 403 on a transaction rider, both HttpError); cancels are
     * never blocked, and timers already scheduled still fire.
     */
    TimersBuilder timers() {
        return TimersBuilder(http_client_);
    }

    /**
     * RAM-class queues (EPHEMERAL_QUEUES.md §1, §3.1).
     *
     * Read the note on EphemeralBuilder once: the contents survive NOTHING --
     * treat a failover like a Redis restart -- while a declared configuration is
     * durable and comes back EMPTY. Consumption semantics come from the pop's
     * `group`, exactly as on the durable engine; there is no queue-level mode.
     *
     * Unlike kv() and timers(), this surface has a VERSION FLOOR. A broker or
     * proxy older than 1.1 has no such routes and answers 404 on every one of
     * them, which this surface maps to EphemeralUnsupportedError -- there is no
     * capability negotiation anywhere in this SDK, so the refusal is the only
     * signal there is.
     *
     * One 404 is NOT that: `depth` answers `ephemeral_queue_not_found` for a
     * queue that is not there, and that arrives as EphemeralQueueNotFoundError.
     * The two are told apart by the body's code, never by the status they share.
     */
    EphemeralBuilder ephemeral() {
        return EphemeralBuilder(http_client_);
    }

    json ack(const json& message, bool status = true, const json& context = json::object()) {
        bool is_batch = message.is_array();
        
        if (is_batch) {
            auto messages = message.get<std::vector<json>>();
            if (messages.empty()) {
                return {{"processed", 0}, {"results", json::array()}};
            }
            
            json acknowledgments = json::array();
            std::string status_str = status ? "completed" : "failed";
            
            for (const auto& msg : messages) {
                std::string transaction_id;
                if (msg.is_string()) {
                    transaction_id = msg.get<std::string>();
                } else if (msg.contains("transactionId")) {
                    transaction_id = msg["transactionId"].get<std::string>();
                } else if (msg.contains("id")) {
                    transaction_id = msg["id"].get<std::string>();
                } else {
                    throw std::runtime_error("Message must have transactionId or id property");
                }
                
                if (!msg.is_object() || !msg.contains("partitionId")) {
                    throw std::runtime_error("Message must have partitionId property");
                }
                
                std::string partition_id = msg["partitionId"].get<std::string>();
                
                json ack = {
                    {"transactionId", transaction_id},
                    {"partitionId", partition_id},
                    {"status", status_str},
                    {"error", context.contains("error") ? context["error"] : nullptr}
                };
                
                if (msg.contains("leaseId") && !msg["leaseId"].is_null()) {
                    ack["leaseId"] = msg["leaseId"];
                }
                
                acknowledgments.push_back(ack);
            }
            
            json request = {
                {"acknowledgments", acknowledgments},
                {"consumerGroup", context.contains("group") ? context["group"] : nullptr}
            };
            
            try {
                json result = http_client_->post("/api/v1/ack/batch", request);
                return {{"success", true}, {"result", result}};
            } catch (const std::exception& e) {
                return {{"success", false}, {"error", e.what()}};
            }
        }
        
        // Single message
        std::string transaction_id;
        std::string partition_id;
        
        if (message.is_string()) {
            transaction_id = message.get<std::string>();
        } else if (message.contains("transactionId")) {
            transaction_id = message["transactionId"].get<std::string>();
        } else if (message.contains("id")) {
            transaction_id = message["id"].get<std::string>();
        } else {
            return {{"success", false}, {"error", "Message must have transactionId or id property"}};
        }
        
        if (!message.is_object() || !message.contains("partitionId")) {
            return {{"success", false}, {"error", "Message must have partitionId property"}};
        }
        
        partition_id = message["partitionId"].get<std::string>();
        
        std::string status_str = status ? "completed" : "failed";
        
        json body = {
            {"transactionId", transaction_id},
            {"partitionId", partition_id},
            {"status", status_str},
            {"error", context.contains("error") ? context["error"] : nullptr},
            {"consumerGroup", context.contains("group") ? context["group"] : nullptr}
        };
        
        if (message.contains("leaseId") && !message["leaseId"].is_null()) {
            body["leaseId"] = message["leaseId"];
        }
        
        try {
            json result = http_client_->post("/api/v1/ack", body);
            return {{"success", true}, {"result", result}};
        } catch (const std::exception& e) {
            return {{"success", false}, {"error", e.what()}};
        }
    }
    
    json renew(const json& message_or_lease_id) {
        std::vector<std::string> lease_ids;
        
        if (message_or_lease_id.is_string()) {
            lease_ids.push_back(message_or_lease_id.get<std::string>());
        } else if (message_or_lease_id.is_array()) {
            for (const auto& item : message_or_lease_id) {
                if (item.is_string()) {
                    lease_ids.push_back(item.get<std::string>());
                } else if (item.contains("leaseId")) {
                    lease_ids.push_back(item["leaseId"].get<std::string>());
                }
            }
        } else if (message_or_lease_id.is_object() && message_or_lease_id.contains("leaseId")) {
            lease_ids.push_back(message_or_lease_id["leaseId"].get<std::string>());
        }
        
        if (lease_ids.empty()) {
            return {{"success", false}, {"error", "No valid lease IDs found for renewal"}};
        }

        // Dedupe: with v4 multi-partition pop, all messages in one batch
        // share the same leaseId (one renew_lease_v2 call extends every
        // claimed partition_consumers row). Without this dedupe, callers
        // passing the full message vector would issue N redundant identical
        // HTTP calls. Preserve insertion order for deterministic output.
        {
            std::unordered_set<std::string> seen;
            std::vector<std::string> deduped;
            deduped.reserve(lease_ids.size());
            for (const auto& id : lease_ids) {
                if (seen.insert(id).second) {
                    deduped.push_back(id);
                }
            }
            lease_ids.swap(deduped);
        }

        json results = json::array();
        for (const auto& lease_id : lease_ids) {
            try {
                json result = http_client_->post("/api/v1/lease/" + lease_id + "/extend", json::object());
                results.push_back({
                    {"leaseId", lease_id},
                    {"success", true},
                    {"newExpiresAt", result.contains("newExpiresAt") ? result["newExpiresAt"] : 
                                     result.contains("lease_expires_at") ? result["lease_expires_at"] : nullptr}
                });
            } catch (const std::exception& e) {
                results.push_back({
                    {"leaseId", lease_id},
                    {"success", false},
                    {"error", e.what()}
                });
            }
        }
        
        return message_or_lease_id.is_array() ? results : results[0];
    }
    
    /**
     * Flush every buffer. deadline_millis < 0 (the default) retries failing
     * batches until they land; see BufferManager::flush_all_buffers.
     */
    void flush_all_buffers(int deadline_millis = -1) {
        buffer_manager_->flush_all_buffers(deadline_millis);
    }

    json get_buffer_stats() const {
        return buffer_manager_->get_stats();
    }

    /**
     * How long close() keeps retrying failing flush batches before giving up.
     * The flusher retries forever while the process is running, which is
     * wrong on the way out: a SIGTERM grace period is finite, so shutdown
     * stops after this deadline and reports what is left instead of hanging
     * until the runtime is killed. Same value as the JS SDK's close deadline.
     */
    static constexpr int CLOSE_FLUSH_DEADLINE_MILLIS = 30000;

    void close() {
        std::cout << "Closing Queen client..." << std::endl;
        shutdown_requested_ = true;

        try {
            buffer_manager_->flush_all_buffers(CLOSE_FLUSH_DEADLINE_MILLIS);
            std::cout << "All buffers flushed" << std::endl;
        } catch (const std::exception& e) {
            // BufferFlushError::what() carries the unflushed count; cleanup()
            // below logs the discard as well. Loud, not silent.
            std::cerr << "Error flushing buffers: " << e.what() << std::endl;
        }

        buffer_manager_->cleanup();
        std::cout << "Queen client closed" << std::endl;
    }
    
    bool is_shutdown_requested() const {
        return shutdown_requested_;
    }
    
    std::shared_ptr<HttpClient> get_http_client() const {
        return http_client_;
    }
};

// ============================================================================
// ConsumerManager Implementation (after QueenClient is defined)
// ============================================================================

inline void ConsumerManager::start(std::function<void(const json&)> handler,
                                   const ConsumeOptions& options) {
    // Build the path
    std::string path;
    if (!options.queue.empty()) {
        if (!options.partition.empty()) {
            path = "/api/v1/pop/queue/" + util::url_encode(options.queue) +
                   "/partition/" + util::url_encode(options.partition);
        } else {
            path = "/api/v1/pop/queue/" + util::url_encode(options.queue);
        }
    } else if (!options.namespace_name.empty() || !options.task.empty()) {
        path = "/api/v1/pop";
    } else {
        throw std::runtime_error("Must specify queue, namespace, or task");
    }
    
    // Build params. Batch, partitions and with them the autopilot flag: 0 means
    // the user set nothing (QueueBuilder leaves it that way on purpose), which is
    // the dimension the broker gets to choose. THE RULE lives in one place
    // (util::pop_sizing) precisely because this is the SECOND parameter builder;
    // only the placement of the keys is here, and it is the pre-autopilot
    // placement so an autopilot-off request is byte-identical.
    const util::PopSizing sizing = util::pop_sizing(
        options.batch, options.max_partitions, 1,
        options.autopilot.value_or(!queen_->is_autopilot_off()));

    std::stringstream params;
    params << "?";
    if (sizing.autopilot) {
        params << "autopilot=true&";
    }
    if (sizing.has_batch) {
        params << "batch=" << sizing.batch << "&";
    }
    params << "wait=" << (options.wait ? "true" : "false");
    params << "&timeout=" << options.timeout_millis;
    
    if (!options.group.empty()) {
        params << "&consumerGroup=" << util::url_encode(options.group);
    }
    if (!options.namespace_name.empty()) {
        params << "&namespace=" << util::url_encode(options.namespace_name);
    }
    if (!options.task.empty()) {
        params << "&task=" << util::url_encode(options.task);
    }
    if (!options.subscription_mode.empty()) {
        params << "&subscriptionMode=" << util::url_encode(options.subscription_mode);
    }
    if (!options.subscription_from.empty()) {
        params << "&subscriptionFrom=" << util::url_encode(options.subscription_from);
    }
    // v4 multi-partition pop: drain up to N sparse partitions per call. Under
    // autopilot a pinned width travels even when it is 1, because 1 is then a
    // decision and not the absence of one.
    if (sizing.has_partitions) {
        params << "&partitions=" << sizing.partitions;
    }
    // PLAN_CONFLATION §3.1, emitted only when true. This is the SECOND place the
    // flag has to be spelled onto the wire in this SDK: consume() and pop() build
    // their query strings independently, and an option wired into one and not the
    // other is the standing failure this feature was warned about (§4).
    if (options.conflation) {
        params << "&conflation=true";
    }

    std::string full_url = path + params.str();
    
    // Workers run inside packaged_tasks whose futures are only wait()ed on, so
    // a thrown exception would be discarded. A terminal (403) response is
    // recorded here instead and rethrown to the caller once every worker has
    // stopped.
    struct TerminalError {
        std::mutex mutex;
        std::exception_ptr error;
    };
    auto terminal_error = std::make_shared<TerminalError>();

    // Names this consumer's target in a conflation warning, and keys the
    // once-per-(target, group) rule (§3.3). Computed once: it cannot change
    // across a run.
    const std::string conflation_target =
        util::pop_target_label(options.queue, options.namespace_name, options.task);

    // Worker function
    auto worker = [this, handler, full_url, options, terminal_error,
                   conflation_target](int worker_id) {
        int processed_count = 0;
        auto last_message_time = std::chrono::steady_clock::now();
        
        while (true) {
            // Check stop signal
            if (options.stop_signal && options.stop_signal->load()) {
                break;
            }
            if (queen_->is_shutdown_requested()) {
                break;
            }
            
            // Check limit
            if (options.limit > 0 && processed_count >= options.limit) {
                break;
            }
            
            // Check idle timeout
            if (options.idle_millis > 0) {
                auto now = std::chrono::steady_clock::now();
                auto idle_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - last_message_time).count();
                if (idle_time >= options.idle_millis) {
                    break;
                }
            }
            
            try {
                int client_timeout = options.wait ? options.timeout_millis + 5000 : options.timeout_millis;
                json result = http_client_->get(full_url, client_timeout,
                                                options.wait ? RetryKind::Pop : RetryKind::Default);

                // PLAN_CONFLATION §4, degrade loudly. Placed BEFORE the
                // empty-response continue on purpose: the check has to fire on
                // the first round trip, and against an old broker the first
                // round trip on an idle queue is a bodiless 204 that the
                // continue below would swallow into an eternal poll.
                util::enforce_conflation_contract(options.conflation, result,
                                                  conflation_target, options.group);

                if (result.is_null() || !result.contains("messages") ||
                    result["messages"].empty()) {
                    if (options.wait) {
                        continue; // Long polling timeout, retry
                    } else {
                        // The broker's advised pacing when this pop engaged
                        // autopilot and the broker had an opinion (it knows the
                        // arrival rate on this queue and this client does not),
                        // otherwise the historical 100ms.
                        std::this_thread::sleep_for(std::chrono::milliseconds(
                            util::empty_poll_delay_millis(
                                util::parse_autopilot_decision(result))));
                        continue;
                    }
                }
                
                json messages = result["messages"];
                if (messages.empty()) {
                    continue;
                }
                
                last_message_time = std::chrono::steady_clock::now();
                
                // TODO: Set up lease renewal if enabled
                
                // Process messages
                if (options.each) {
                    for (const auto& msg : messages) {
                        if (options.stop_signal && options.stop_signal->load()) break;
                        
                        try {
                            handler(msg);
                            if (options.auto_ack) {
                                json context = options.group.empty() ? 
                                    json::object() : json{{"group", options.group}};
                                queen_->ack(msg, true, context);
                            }
                        } catch (const std::exception& e) {
                            if (options.auto_ack) {
                                json context = options.group.empty() ? 
                                    json::object() : json{{"group", options.group}};
                                queen_->ack(msg, false, context);
                            }
                        }
                        
                        processed_count++;
                        if (options.limit > 0 && processed_count >= options.limit) break;
                    }
                } else {
                    // Process as batch
                    try {
                        handler(messages);
                        if (options.auto_ack) {
                            json context = options.group.empty() ? 
                                json::object() : json{{"group", options.group}};
                            queen_->ack(messages, true, context);
                        }
                    } catch (const std::exception& e) {
                        if (options.auto_ack) {
                            json context = options.group.empty() ? 
                                json::object() : json{{"group", options.group}};
                            queen_->ack(messages, false, context);
                        }
                    }
                    processed_count += messages.size();
                }
                
            } catch (const ConflationUnsupportedError& e) {
                // PLAN_CONFLATION §4. Terminal, and terminal in the same way a
                // 403 is: retrying cannot make an old broker conflate, and every
                // further poll would hand the handler another message off a
                // backlog the caller believes is being conflated away. Recorded
                // rather than rethrown here because a worker runs inside a
                // packaged_task whose future is only wait()ed on -- a throw
                // would be discarded, and the loop would look like it simply
                // ended.
                util::log_error("ConsumerManager.worker", std::string("Worker ") +
                              std::to_string(worker_id) + " stopping: " + e.what());
                {
                    std::lock_guard<std::mutex> lock(terminal_error->mutex);
                    if (!terminal_error->error) {
                        terminal_error->error = std::current_exception();
                    }
                }
                break;
            } catch (const HttpError& e) {
                // 429 (rate limited): HttpClient already retries this internally
                // with backoff (unbounded for a wait=true pop). This branch is a
                // defensive fallback for the case where an explicit
                // retry_429.max_attempts override got exhausted -- back off and
                // keep polling instead of hot-looping.
                if (e.status_code() == 429) {
                    int delay = e.retry_after_seconds().has_value()
                        ? static_cast<int>(*e.retry_after_seconds() * 1000) : 1000;
                    util::log_warn("ConsumerManager.worker", std::string("Worker ") +
                                  std::to_string(worker_id) + " rate-limited (code=" + e.code() +
                                  "), retrying in " + std::to_string(delay) + "ms");
                    std::this_thread::sleep_for(std::chrono::milliseconds(delay));
                    continue;
                }

                // 403 (forbidden): terminal. cluster_suspended in particular can
                // never resolve itself, and none of the other proxy codes
                // (storage_quota_exceeded / feature_gated / forbidden) are worth
                // hot-looping either -- stop this worker and surface the error
                // to the caller instead of retrying.
                if (e.status_code() == 403) {
                    util::log_error("ConsumerManager.worker", std::string("Worker ") +
                                  std::to_string(worker_id) + " forbidden (code=" + e.code() +
                                  "): " + e.what());
                    std::lock_guard<std::mutex> lock(terminal_error->mutex);
                    if (!terminal_error->error) {
                        terminal_error->error = std::current_exception();
                    }
                    break;
                }

                util::log_error("ConsumerManager.worker", std::string("Worker ") +
                              std::to_string(worker_id) + " error: " + e.what() +
                              " (status=" + std::to_string(e.status_code()) + ")");
                throw;
            } catch (const std::exception& e) {
                // Check if timeout error (expected for long polling)
                std::string error_msg = e.what();
                if (error_msg.find("timeout") != std::string::npos && options.wait) {
                    continue; // Retry on timeout
                }
                
                // Network error - wait before retry
                if (error_msg.find("Connection refused") != std::string::npos ||
                    error_msg.find("connect") != std::string::npos) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    continue;
                }
                
                // Other errors - rethrow
                util::log_error("ConsumerManager.worker", std::string("Worker ") + 
                              std::to_string(worker_id) + " error: " + e.what());
                throw;
            }
        }
    };
    
    // Start workers in thread pool
    std::vector<std::future<void>> futures;
    for (int i = 0; i < options.concurrency; ++i) {
        futures.push_back(thread_pool_.future_from_push([worker, i]() {
            worker(i);
        }));
    }
    
    // Wait for all workers to complete
    for (auto& future : futures) {
        future.wait();
    }

    if (terminal_error->error) {
        std::rethrow_exception(terminal_error->error);
    }
}

// ============================================================================
// QueueBuilder::consume Implementation (after ConsumerManager is defined)
// ============================================================================

inline void QueueBuilder::consume(std::function<void(const json&)> handler,
                                  std::atomic<bool>* stop_signal) {
    ConsumeOptions options;
    options.queue = queue_name_;
    options.partition = partition_ != "Default" ? partition_ : "";
    options.namespace_name = namespace_;
    options.task = task_;
    options.group = group_;
    options.concurrency = concurrency_;
    options.batch = batch_;
    options.limit = limit_;
    options.idle_millis = idle_millis_;
    options.auto_ack = auto_ack_;
    options.wait = wait_;
    options.timeout_millis = timeout_millis_;
    options.renew_lease = renew_lease_;
    options.renew_lease_interval_millis = renew_lease_interval_millis_;
    options.subscription_mode = subscription_mode_;
    options.subscription_from = subscription_from_;
    options.each = each_;
    options.max_partitions = max_partitions_;
    options.conflation = conflation_;
    // Resolved here so ConsumerManager sees a decision and not an empty
    // optional. batch and max_partitions keep their 0 when autopilot is on, and
    // that 0 has to survive all the way to the param builder: it is the ONLY
    // record that the user said nothing about that dimension.
    options.autopilot = autopilot_enabled();
    options.stop_signal = stop_signal;
    
    ConsumerManager consumer_manager(http_client_, queen_);
    consumer_manager.start(handler, options);
}

inline bool QueueBuilder::autopilot_enabled() const {
    if (autopilot_.has_value()) return *autopilot_;
    return queen_ == nullptr || !queen_->is_autopilot_off();
}

} // namespace queen

