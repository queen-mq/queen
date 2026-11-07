/**
 * Queen MQ client for C++ -- one header.
 *
 * The same client as the JavaScript, Python, Go, Rust and PHP ones, in C++17:
 *
 * - Typed messages and results (Message, PushResult, AckResult,
 *   TransactionResult); payloads stay nlohmann::json
 * - Push, pop, consume (per message or per batch, any concurrency), ack with
 *   every status, the ack reading its consumer group and lease from the message
 * - Transactions: acks, pushes (to any partition), KV writes and timers in one
 *   log entry, guarded by a lock
 * - KV (the eight operations), timers (schedule, cancel, peek, list), locks
 *   and semaphores with background renewal, ephemeral queues, reads by offset
 * - admin(): the management and observability routes
 * - Transport: keep-alive connection pool, HTTPS with certificate
 *   verification, retry, 429 backoff, failover, affinity load balancing
 * - Client-side push buffering with a blocking bound and lossless retry
 * - Binary push body and framed pop (with zstd)
 *
 * Dependencies: C++17, cpp-httplib 0.27+, OpenSSL (for https://; define
 * QUEEN_CLIENT_NO_TLS to build without it), nlohmann/json 3.11 (vendored in
 * third_party/ when the program has none), zstd only with -DQUEEN_CLIENT_ZSTD
 * (the push_compression and framed_pop options need it).
 *
 * With CMake: add_subdirectory(clients/client-cpp) or FetchContent, then
 * target_link_libraries(app PRIVATE queen::client).
 *
 * Usage:
 *   #include "queen_client.hpp"
 *
 *   queen::Queen queen("http://localhost:6632");
 *   queen.queue("orders").partition("customer-42").push({{"total", 19.99}});
 *   queen.queue("orders").group("billing").batch(10).limit(100)
 *        .consume([&](const queen::Message& m) { std::cout << m.data << "\n"; });
 *   queen.close();
 */

#pragma once

#define QUEEN_CLIENT_VERSION "2.8.0"
#define QUEEN_CLIENT_VERSION_MAJOR 2
#define QUEEN_CLIENT_VERSION_MINOR 8
#define QUEEN_CLIENT_VERSION_PATCH 0

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
#include <csignal>
#include <algorithm>
#include <array>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <tuple>
#include <utility>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

// JSON: the program's own nlohmann/json when it has one (one copy per program),
// else the one vendored beside this header.
#if __has_include(<nlohmann/json.hpp>)
#include <nlohmann/json.hpp>
#else
#include "third_party/nlohmann/json.hpp"
#endif

// HTTP. TLS is on unless the build says QUEEN_CLIENT_NO_TLS: an https:// URL
// needs it, and OpenSSL at link time.
#if !defined(QUEEN_CLIENT_NO_TLS) && !defined(CPPHTTPLIB_OPENSSL_SUPPORT)
#define CPPHTTPLIB_OPENSSL_SUPPORT
#endif
#include <httplib.h>

// zstd, for the push_compression and framed_pop options. Opt-in: a build that
// does not define QUEEN_CLIENT_ZSTD needs neither the header nor the library.
#ifdef QUEEN_CLIENT_ZSTD
#include <zstd.h>
#endif

namespace queen {

using json = nlohmann::json;

/** This client's version: 2.8.0. */
inline const char* version() { return QUEEN_CLIENT_VERSION; }

// ============================================================================
// Forward Declarations
// ============================================================================

class Queen;
class QueueBuilder;
class TransactionBuilder;
class HttpClient;
class LoadBalancer;
class BufferManager;
class MessageBuffer;
class ConsumerManager;
class Lock;

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
 * separate from the 5xx/network retry_attempts. Zero-value fields fall
 * back to kind-based defaults:
 * - max_attempts: bounded (10 total attempts) for ordinary requests (push,
 *   admin calls, non-waiting pop); unbounded for a long-poll pop. Setting it
 *   explicitly applies the same bound to both kinds.
 * - base_millis: initial backoff absent a Retry-After header (default 500).
 * - cap_millis: ceiling for the exponential backoff (default 30000).
 * A Retry-After header (seconds) always wins over the computed delay.
 */
struct Retry429Options {
    int max_attempts = 0;                    // 0 = kind-based default
    int base_millis = 0;                     // 0 = 500
    int cap_millis = 0;                      // 0 = 30000
};

/**
 * How a request picks its backend when the client has more than one URL.
 *
 * Affinity, the default as in the JS, Python, Go and Rust clients, hashes the
 * request's grouping key -- queue, partition and consumer group -- onto a ring,
 * so the pops of one consumer group stay on one backend while it is healthy.
 * RoundRobin spreads every request. Session picks one backend per client and
 * keeps it until it fails.
 */
enum class LoadBalancing {
    Affinity,
    RoundRobin,
    Session
};

struct ClientConfig {
    std::vector<std::string> urls;
    /// Per request; a long-poll pop adds its own poll window to it.
    int timeout_millis = 30000;
    /// Tries per request on one backend for a 5xx or a transport fault.
    int retry_attempts = 3;
    /// The first pause between those tries; it doubles each time.
    int retry_delay_millis = 1000;
    LoadBalancing load_balancing = LoadBalancing::Affinity;
    /// Virtual nodes per backend on the affinity ring (128 in every SDK).
    int affinity_hash_ring = 128;
    /// How long a backend that failed stays out before it is tried again.
    int health_retry_after_millis = 5000;
    /// With several URLs, a 5xx or a transport fault moves to the next backend.
    bool enable_failover = true;
    /// Sent as "Authorization: Bearer <token>" (the proxy, Queen Cloud).
    std::string bearer_token;
    /// Extra headers on every request, e.g. {"x-queen-tenant", "acme"}.
    std::vector<std::pair<std::string, std::string>> headers;
    Retry429Options retry_429;
    /// Idle keep-alive connections kept per backend (as Go's MaxIdleConnsPerHost).
    int max_idle_connections_per_host = 256;
    /// An idle connection older than this is closed instead of reused (0 = never).
    int idle_connection_timeout_millis = 60000;
    /// Verify an https:// backend's certificate and host name. Turn off only
    /// for a self-signed test broker.
    bool tls_verify = true;
    /// CA bundle file and/or directory for https://; empty = the system store.
    std::string ca_cert_path;
    std::string ca_cert_dir;
    /**
     * Install SIGINT/SIGTERM handlers. Off by default: a library has no
     * business taking over process-wide signals unless the program asks, and
     * the Rust client has the same rule. On, the first signal stops every
     * consume() loop after its current message (StopReason::Cancelled) and
     * makes Queen::wait_for_signal() return, so the program can close() --
     * which flushes buffers -- and exit; a second signal ends the process the
     * way the signal would have without these handlers.
     */
    bool handle_signals = false;
    // The three below need a broker that has the binary push body and the
    // framed pop answer. Off, the client sends and reads the JSON every broker
    // knows.
    //
    // binary_push: a push goes out as the binary push body (wire::
    // encode_push_binary) instead of JSON. Items with a trace id stay JSON.
    bool binary_push = false;
    // push_compression (with binary_push): the client frames the messages as
    // the broker stores them, mints their ids and zstd-compresses each
    // partition's batch once; the broker keeps those bytes as they are. A
    // transaction that pushes sends its pushes the same way (the binary
    // transaction body), and the rest of the bundle as JSON in front of them.
    // Needs a build with QUEEN_CLIENT_ZSTD.
    bool push_compression = false;
    // framed_pop: pops ask for the messages as the stored records (frames=true)
    // and unpack them here (wire::decode_pop_frames). A broker that does not
    // know the parameter answers JSON, which is read as always. Needs a build
    // with QUEEN_CLIENT_ZSTD.
    bool framed_pop = false;
};

/**
 * A queue's options for configure() / create(). Every field is optional and
 * only the ones set are sent; the broker's default owns the rest (the table on
 * the docs' Queues page). Names and units are the broker's, in this client's
 * spelling: `retry_delay` is milliseconds, the others seconds unless named.
 *
 * configure() is a FULL upsert on the broker: an option left unset goes back
 * to its default, it does not keep the value an earlier configure gave it.
 */
struct QueueConfig {
    std::optional<int> lease_time;                  // seconds, default 300 (60 for a queue made by push)
    std::optional<int> retry_limit;                 // default 3
    std::optional<int> retry_delay;                 // MILLISECONDS, default 1000
    std::optional<bool> dead_letter_queue;          // default true
    std::optional<bool> dlq_after_max_retries;      // default true
    std::optional<int> priority;
    std::optional<int> delayed_processing;          // seconds
    std::optional<int> window_buffer;               // seconds
    std::optional<int> max_size;
    std::optional<int> ttl;                         // seconds
    std::optional<int> dedup_window_seconds;        // default 3600; 0 turns dedup off
    std::optional<bool> retention_enabled;          // must be true for the two below
    std::optional<int> retention_seconds;
    std::optional<int> completed_retention_seconds;
    std::optional<int> max_wait_time_seconds;
    std::optional<int> min_pop_wait_time;
    std::optional<bool> encryption_enabled;
    std::optional<std::string> retention_sink_hold;
    std::optional<int> retention_sink_hold_max_seconds;

    json to_json() const {
        json out = json::object();
        auto put = [&out](const char* key, const auto& value) {
            if (value.has_value()) out[key] = *value;
        };
        put("leaseTime", lease_time);
        put("retryLimit", retry_limit);
        put("retryDelay", retry_delay);
        put("deadLetterQueue", dead_letter_queue);
        put("dlqAfterMaxRetries", dlq_after_max_retries);
        put("priority", priority);
        put("delayedProcessing", delayed_processing);
        put("windowBuffer", window_buffer);
        put("maxSize", max_size);
        put("ttl", ttl);
        put("dedupWindowSeconds", dedup_window_seconds);
        put("retentionEnabled", retention_enabled);
        put("retentionSeconds", retention_seconds);
        put("completedRetentionSeconds", completed_retention_seconds);
        put("maxWaitTimeSeconds", max_wait_time_seconds);
        put("minPopWaitTime", min_pop_wait_time);
        put("encryptionEnabled", encryption_enabled);
        put("retentionSinkHold", retention_sink_hold);
        put("retentionSinkHoldMaxSeconds", retention_sink_hold_max_seconds);
        return out;
    }
};

// Missing configuration is off: no reporter thread or KV traffic.
struct SupervisionConfig { std::string group; };

/**
 * Stops a consume() loop from another thread: after the message it is on, or
 * at once while it waits between polls. Copies share one flag, so hand a copy
 * to the builder (QueueBuilder::cancel) and keep one to call cancel() on.
 */
class Cancel {
private:
    struct State {
        std::mutex mutex;
        std::condition_variable wake;
        bool cancelled = false;
    };
    std::shared_ptr<State> state_ = std::make_shared<State>();

public:
    void cancel() {
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            state_->cancelled = true;
        }
        state_->wake.notify_all();
    }

    bool is_cancelled() const {
        std::lock_guard<std::mutex> lock(state_->mutex);
        return state_->cancelled;
    }

    /** Sleep up to `duration`; true when cancelled before or during it. */
    bool wait_for(std::chrono::milliseconds duration) const {
        std::unique_lock<std::mutex> lock(state_->mutex);
        return state_->wake.wait_for(lock, duration, [this] { return state_->cancelled; });
    }
};

/**
 * What a consume() loop runs with. QueueBuilder fills it; it is public for a
 * caller that drives ConsumerManager itself.
 */
struct ConsumeOptions {
    std::optional<SupervisionConfig> supervision;
    std::string queue;
    std::string partition;
    std::string namespace_name;
    std::string task;
    std::string group;
    int concurrency = 1;
    /**
     * The pop's message budget. 0 means UNSET -- the dimension pop autopilot
     * lets the broker choose, from state no client can see. See
     * util::pop_sizing.
     */
    int batch = 0;
    /// Messages to handle before stopping, counted across ALL workers; 0 = unlimited.
    long long limit = 0;
    int idle_millis = 0;                     // 0 = no idle timeout
    bool auto_ack = true;
    bool wait = true;
    int timeout_millis = 30000;
    bool renew_lease = false;
    int renew_lease_interval_millis = 60000;
    std::string subscription_mode;
    std::string subscription_from;
    /// v4 multi-partition pop cap. 0 = unset, as for `batch` above.
    int max_partitions = 0;
    /**
     * Broker-side pop sizing. Unset takes the client-wide default (on, unless
     * QUEEN_SDK_POP_AUTOPILOT turned it off at construction); false restores the
     * pre-1.2 client-side defaults byte for byte.
     */
    std::optional<bool> autopilot;
    /**
     * Last-value delivery for this consumer group on this queue: under backlog
     * the group is served ONE message per partition, the newest. A property of
     * the GROUP, fixed by the first pop that registers it. Requires a broker
     * >= 1.1.0; an older one is detected on the first response and refused
     * (ConflationUnsupportedError).
     */
    bool conflation = false;
    /// Set to stop the loop from another thread.
    std::optional<Cancel> cancel;
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
 * The base of everything this client throws for a request that did not
 * happen or did not succeed. A misuse -- a missing queue name, an ack of a
 * message with no partition -- is std::invalid_argument instead, thrown before
 * anything is sent.
 */
class Error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/**
 * No answer: the connection could not be made, broke, or timed out. Always
 * retryable -- and for a write its outcome is UNKNOWN: the broker may have
 * committed it. Make every write safe to repeat (a transactionId derived from
 * the work, a KV putIfAbsent gate).
 */
class NetworkError : public Error {
private:
    bool timed_out_ = false;

public:
    explicit NetworkError(const std::string& message, bool timed_out = false)
        : Error(message), timed_out_(timed_out) {
    }
    /** The connection timed out (a read that timed out reads as a failed read). */
    bool timed_out() const { return timed_out_; }
    bool is_retryable() const { return true; }
};

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
 * predates the proxy contract). A transport failure (no response at all) is a
 * NetworkError.
 */
class HttpError : public Error {
private:
    int status_code_;
    std::string body_;
    std::string code_;
    std::optional<double> retry_after_seconds_;

public:
    HttpError(int status_code, const std::string& message, const std::string& body,
              const std::string& code, std::optional<double> retry_after_seconds)
        : Error(message), status_code_(status_code), body_(body),
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

    /** 429: the backend is alive and shedding load. */
    bool is_rate_limited() const { return status_code_ == 429; }

    /** 403: a refusal no retry resolves (suspended, quota, gated, forbidden). */
    bool is_terminal_refusal() const { return status_code_ == 403; }

    /** Worth sending again: a 429 or a 5xx. Any other 4xx is the caller's. */
    bool is_retryable() const { return status_code_ == 429 || status_code_ >= 500; }
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
class BufferFlushError : public Error {
private:
    std::string queue_address_;
    int unflushed_count_;

public:
    BufferFlushError(const std::string& message, const std::string& queue_address,
                     int unflushed_count)
        : Error(message), queue_address_(queue_address),
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
class ConflationUnsupportedError : public Error {
private:
    std::string queue_;
    std::string group_;

public:
    ConflationUnsupportedError(const std::string& queue, const std::string& group)
        // The message opens with the canonical wording from PLAN_CONFLATION §4,
        // byte-identical across all seven SDKs so one grep finds every language;
        // the queue and group are appended because in C++ the accessors below
        // are rarely reached and what() is what ends up in the log.
        : Error(
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
 * and nothing to fall back to: the WHOLE /api/v1/ephemeral family answers 404
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

/** The lower-case hyphenated text of 16 id bytes. */
inline std::string uuid_text(const uint8_t* bytes) {
    static const char* const HEX = "0123456789abcdef";
    std::string text;
    text.reserve(36);
    for (int i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) text.push_back('-');
        text.push_back(HEX[bytes[i] >> 4]);
        text.push_back(HEX[bytes[i] & 0x0F]);
    }
    return text;
}

/**
 * Generate UUIDv7 (time-ordered UUID with millisecond precision) as its 16
 * bytes. Based on queen::QueueManager::generate_uuid()
 */
inline std::array<uint8_t, 16> generate_uuid_v7_bytes() {
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

    return bytes;
}

/** A UUIDv7 as text: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx */
inline std::string generate_uuid_v7() {
    return uuid_text(generate_uuid_v7_bytes().data());
}

/** A canonical UUID: 8-4-4-4-12 hex digits, either case. */
inline bool is_valid_uuid(const std::string& str) {
    if (str.size() != 36) return false;
    for (size_t i = 0; i < str.size(); ++i) {
        const unsigned char c = static_cast<unsigned char>(str[i]);
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (c != '-') return false;
        } else if (!std::isxdigit(c)) {
            return false;
        }
    }
    return true;
}

/**
 * Percent-encode everything but the unreserved characters (RFC 3986), byte by
 * byte, in lower-case hex: the spelling this client has always sent.
 */
inline std::string url_encode(const std::string& value) {
    static const char* const HEX = "0123456789abcdef";
    std::string out;
    out.reserve(value.size() * 3);
    for (char ch : value) {
        const unsigned char c = static_cast<unsigned char>(ch);
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('%');
            out.push_back(HEX[c >> 4]);
            out.push_back(HEX[c & 0x0F]);
        }
    }
    return out;
}

/**
 * Split a broker URL into scheme, host and port: http(s)://host[:port], an
 * IPv6 host in brackets. Throws std::invalid_argument for anything else -- a
 * path included, since the client adds the API's own.
 */
inline std::tuple<std::string, std::string, int> parse_url(const std::string& url) {
    auto invalid = [&url]() {
        return std::invalid_argument(
            "invalid broker URL '" + url + "': expected http(s)://host[:port], with no path");
    };
    const auto scheme_end = url.find("://");
    if (scheme_end == std::string::npos) throw invalid();
    const std::string scheme = url.substr(0, scheme_end);
    if (scheme != "http" && scheme != "https") throw invalid();
    const std::string rest = url.substr(scheme_end + 3);
    std::string host;
    std::string port_text;
    if (!rest.empty() && rest[0] == '[') {
        const auto close = rest.find(']');
        if (close == std::string::npos) throw invalid();
        host = rest.substr(0, close + 1);
        const std::string after = rest.substr(close + 1);
        if (!after.empty()) {
            if (after[0] != ':') throw invalid();
            port_text = after.substr(1);
        }
    } else {
        const auto colon = rest.find(':');
        host = rest.substr(0, colon);
        if (colon != std::string::npos) port_text = rest.substr(colon + 1);
    }
    if (host.empty() || host.find('/') != std::string::npos || host.find('?') != std::string::npos) {
        throw invalid();
    }
    int port = scheme == "https" ? 443 : 80;
    if (!port_text.empty() || rest.find(':') != std::string::npos) {
        if (port_text.empty() || port_text.size() > 5) throw invalid();
        for (char c : port_text) {
            if (!std::isdigit(static_cast<unsigned char>(c))) throw invalid();
        }
        port = std::stoi(port_text);
        if (port < 1 || port > 65535) throw invalid();
    }
    return {scheme, host, port};
}

/**
 * FNV-1a, 32-bit: the affinity ring's hash. The same function over the same
 * strings in every SDK, so every client maps a key to the same backend: part
 * of the contract even though it never travels.
 */
inline uint32_t fnv1a(const std::string& s) {
    uint32_t hash = 2166136261u;
    for (char ch : s) {
        hash ^= static_cast<unsigned char>(ch);
        hash *= 16777619u;
    }
    return hash;
}

/** The partition a push names when it names none. */
inline constexpr const char* DEFAULT_PARTITION = "Default";

/** The consumer group of a pop that names none: queue mode. */
inline constexpr const char* QUEUE_MODE_GROUP = "__QUEUE_MODE__";

/**
 * The broker's grouping of a pop -- `queue:partition:group`, `*` for any
 * partition, or `namespace:task:group` for a discovery pop -- which is the
 * affinity key. Empty when the request names neither (an admin call).
 */
inline std::string grouping_key(const std::string& queue, const std::string& partition,
                                const std::string& namespace_name, const std::string& task,
                                const std::string& group) {
    const std::string g = group.empty() ? std::string(QUEUE_MODE_GROUP) : group;
    if (!queue.empty()) {
        return queue + ":" + (partition.empty() ? std::string("*") : partition) + ":" + g;
    }
    if (!namespace_name.empty() || !task.empty()) {
        return (namespace_name.empty() ? std::string("*") : namespace_name) + ":" +
               (task.empty() ? std::string("*") : task) + ":" + g;
    }
    return std::string();
}

/**
 * A message's consumer group as an ack sends it: null for queue mode (no group,
 * or the broker's "__QUEUE_MODE__"), which is what every 2.x broker has always
 * been sent for it.
 */
inline json group_on_wire(const std::string& group) {
    return group.empty() || group == QUEUE_MODE_GROUP ? json(nullptr) : json(group);
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
 * The whole client-side conflation contract in one place. pop() and every
 * consume() worker reach it through the one pop path (detail::pop_once).
 *
 * No-op unless conflation was requested. Then:
 *   applied  -> nothing to say.
 *   conflict -> the broker speaks conflation and the stored policy won: warn,
 *               once per (queue, group). Not an error -- §3.3/Q3, a reject here
 *               takes down the already-correct half of a rolling deploy.
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
    throw ConflationUnsupportedError(queue, group);
}

/**
 * Standard base64 (RFC 4648), WITH padding.
 *
 * A timer payload travels base64 on the wire (PLAN_KV_TIMERS.md §4.1) and the
 * broker decodes it with the STANDARD engine. Padding is not cosmetic here: an
 * unpadded encoder gets the payload refused as invalid base64.
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
 * broker would refuse it anyway, and paying a round trip to learn
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
 * before autopilot existed, byte for byte. It is read once, in the Queen
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
 * THE EMISSION RULE, in one copy: a rule with three branches and a
 * per-dimension carve-out is precisely the kind that gets copied wrong.
 * detail::pop_query calls this and only PLACES what it returns.
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

/// The longest handler error a nack carries: a message and the head of a
/// trace. The broker keeps it with the dead letter, once per message.
inline constexpr size_t MAX_NACK_ERROR_BYTES = 4096;

/**
 * A handler's what() made safe to send as a nack's `error`: capped at
 * MAX_NACK_ERROR_BYTES, and every byte that is not valid UTF-8 replaced by
 * U+FFFD. nlohmann::json refuses to serialize invalid UTF-8 (type_error 316),
 * so a raw what() carrying, say, a Latin-1 file name would make the nack's body
 * unsendable and leave the message leased, which is what the nack is there to
 * prevent. A cut through a multi-byte character is replaced the same way.
 */
inline std::string nack_error_text(const std::string& what) {
    const std::string capped =
        what.size() > MAX_NACK_ERROR_BYTES ? what.substr(0, MAX_NACK_ERROR_BYTES) : what;
    return json::parse(json(capped).dump(-1, ' ', false, json::error_handler_t::replace))
        .get<std::string>();
}

} // namespace util

// ============================================================================
// Typed values: messages, push, ack, renew and transaction results
//
// The same shapes as the Rust client's (crates/queen-protocol) and Go's
// Message / PushResponse / AckResponse: the envelope is typed, the payload
// (`data`) stays JSON.
// ============================================================================

/** How an ack settles a message. */
enum class AckStatus {
    /// Done: the group's cursor moves past it.
    Completed,
    /// Spends a retry: redelivered after retryDelay, dead-lettered past retryLimit.
    Failed,
    /// Redelivered without spending a retry.
    Retry,
    /// Dead-lettered now.
    Dlq
};

inline const char* to_string(AckStatus status) {
    switch (status) {
    case AckStatus::Completed: return "completed";
    case AckStatus::Failed: return "failed";
    case AckStatus::Retry: return "retry";
    case AckStatus::Dlq: return "dlq";
    }
    return "completed";
}

namespace util {

/** A string field of a JSON object, "" when absent, null or not a string. A number is its text. */
inline std::string text_of(const json& object, const char* key) {
    if (!object.is_object()) return std::string();
    auto it = object.find(key);
    if (it == object.end() || it->is_null()) return std::string();
    if (it->is_string()) return it->get<std::string>();
    if (it->is_number()) return it->dump();
    return std::string();
}

} // namespace util

/**
 * One delivered message, as a pop or a consume loop hands it over.
 *
 * Ack it with Queen::ack(message): the call reads the consumer group and the
 * lease from the message itself, so a forgotten group cannot ack the wrong
 * cursor (the queue-mode one), which is how a message used to come back.
 */
struct Message {
    /// The broker's message id (a UUIDv7).
    std::string id;
    /// The producer's dedup key.
    std::string transaction_id;
    /// Empty when the message has none.
    std::string trace_id;
    /// The payload, as pushed.
    json data;
    /// Who pushed it, when the proxy stamped it; empty otherwise.
    std::string producer_sub;
    std::string created_at;
    std::string queue;
    std::string partition;
    std::string partition_id;
    /// Empty for a commit-on-delivery pop: there is nothing to ack.
    std::string lease_id;
    /// "__QUEUE_MODE__" when the pop named no group.
    std::string consumer_group;
    /// Which delivery this is: 1 the first time, more after a nack or an
    /// expired lease. 0 when the broker did not say.
    int delivery_attempt = 0;
    /// The message's offset in its partition, when the broker sent it.
    std::optional<long long> offset;

    /**
     * Read one message of a pop answer. `envelope` is the answer around it:
     * a field the message lacks (queue, partition, lease, group) is taken from
     * there, which is where a single-partition pop puts them.
     */
    static Message from_json(const json& item, const json& envelope = json()) {
        auto field = [&](const char* key) {
            std::string value = util::text_of(item, key);
            return value.empty() ? util::text_of(envelope, key) : value;
        };
        Message m;
        m.id = util::text_of(item, "id");
        m.transaction_id = util::text_of(item, "transactionId");
        m.trace_id = util::text_of(item, "traceId");
        if (item.is_object() && item.contains("data")) {
            m.data = item["data"];
        } else if (item.is_object() && item.contains("payload")) {
            m.data = item["payload"];
        }
        m.producer_sub = util::text_of(item, "producerSub");
        m.created_at = util::text_of(item, "createdAt");
        m.queue = field("queue");
        m.partition = field("partition");
        m.partition_id = field("partitionId");
        m.lease_id = field("leaseId");
        m.consumer_group = field("consumerGroup");
        if (item.is_object() && item.contains("deliveryAttempt") &&
            item["deliveryAttempt"].is_number_integer()) {
            m.delivery_attempt = item["deliveryAttempt"].get<int>();
        }
        if (item.is_object() && item.contains("offset") && item["offset"].is_number_integer()) {
            m.offset = item["offset"].get<long long>();
        }
        return m;
    }

    /** The broker's own shape, for logging or forwarding. */
    json to_json() const {
        auto opt = [](const std::string& value) { return value.empty() ? json(nullptr) : json(value); };
        return {{"id", id},
                {"transactionId", transaction_id},
                {"traceId", opt(trace_id)},
                {"data", data},
                {"producerSub", opt(producer_sub)},
                {"createdAt", created_at},
                {"queue", queue},
                {"partition", partition},
                {"partitionId", partition_id},
                {"leaseId", lease_id},
                {"consumerGroup", consumer_group},
                {"deliveryAttempt", delivery_attempt},
                {"offset", offset ? json(*offset) : json(nullptr)}};
    }
};

/**
 * One message for QueueBuilder::push_items: a payload with its own dedup key.
 * Give every message a transaction id derived from the work
 * ("receipt-" + order_id): a repeat inside the queue's dedup window is then
 * answered `duplicate` and writes nothing. Empty = minted here (a UUIDv7).
 */
struct PushItem {
    json payload;
    std::string transaction_id;
};

/** What the broker did with one pushed message. */
enum class PushStatus {
    Queued,
    /// Its transaction id is already in the partition, inside the dedup window:
    /// nothing written, and the step already happened.
    Duplicate,
    /// Refused; nothing written. `error` says why when the broker does.
    Error,
    /// Accepted by this client's push buffer; the verdict comes with the flush.
    Buffered,
    /// A 1.x status the 2.x broker never sends.
    Failed,
    /// A status this client does not know.
    Unknown
};

struct PushResult {
    size_t index = 0;
    std::string message_id;
    std::string transaction_id;
    std::string queue_name;
    PushStatus status = PushStatus::Unknown;
    /// The status as the broker spelled it.
    std::string status_text;
    /// The message's offset in its partition (the pre-existing one for a duplicate).
    std::optional<long long> offset;
    std::string error;

    /** Written, now or before: queued or duplicate. */
    bool ok() const { return status == PushStatus::Queued || status == PushStatus::Duplicate; }

    static PushResult from_json(const json& item, size_t fallback_index) {
        PushResult r;
        r.index = item.is_object() && item.contains("index") && item["index"].is_number_unsigned()
            ? item["index"].get<size_t>() : fallback_index;
        r.message_id = util::text_of(item, "message_id");
        if (r.message_id.empty()) r.message_id = util::text_of(item, "messageId");
        r.transaction_id = util::text_of(item, "transaction_id");
        if (r.transaction_id.empty()) r.transaction_id = util::text_of(item, "transactionId");
        r.queue_name = util::text_of(item, "queueName");
        r.status_text = util::text_of(item, "status");
        r.status = r.status_text == "queued"      ? PushStatus::Queued
                 : r.status_text == "duplicate"   ? PushStatus::Duplicate
                 : r.status_text == "error"       ? PushStatus::Error
                 : r.status_text == "buffered"    ? PushStatus::Buffered
                 : r.status_text == "failed"      ? PushStatus::Failed
                                                  : PushStatus::Unknown;
        if (item.is_object() && item.contains("offset") && item["offset"].is_number_integer()) {
            r.offset = item["offset"].get<long long>();
        }
        r.error = util::text_of(item, "error");
        return r;
    }
};

/** The broker's verdict on one acknowledgment. A refusal is a value, not an exception. */
struct AckResult {
    size_t index = 0;
    std::string transaction_id;
    bool success = false;
    /// Why it was refused: the lease expired or was released, the message is settled.
    std::string error;
    bool lease_released = false;
    /// The ack filed the message in the dead-letter queue.
    bool dlq = false;
    /// Nothing to do: the message was already settled.
    bool noop = false;
    /// With conflation: how many older messages this ack settled with it.
    std::optional<long long> conflated;

    static AckResult from_json(const json& item, size_t fallback_index) {
        AckResult r;
        r.index = item.is_object() && item.contains("index") && item["index"].is_number_unsigned()
            ? item["index"].get<size_t>() : fallback_index;
        r.transaction_id = util::text_of(item, "transactionId");
        r.error = util::text_of(item, "error");
        // HTTP 200 also carries refusals: an item is a success only when it
        // says so (or says nothing) and carries no error.
        const bool flag = !(item.is_object() && item.contains("success") &&
                            item["success"].is_boolean() && !item["success"].get<bool>());
        r.success = flag && r.error.empty();
        auto boolean = [&item](const char* key) {
            return item.is_object() && item.contains(key) && item[key].is_boolean() &&
                   item[key].get<bool>();
        };
        r.lease_released = boolean("leaseReleased");
        r.dlq = boolean("dlq");
        r.noop = boolean("noop");
        if (item.is_object() && item.contains("conflated") && item["conflated"].is_number_integer()) {
            r.conflated = item["conflated"].get<long long>();
        }
        return r;
    }
};

/** The broker's answer to a lease renewal. */
struct RenewResult {
    std::string lease_id;
    /// false: the lease is gone (expired, released by an ack or nack, or never there).
    bool success = false;
    std::string new_expires_at;
    std::string error;
};

/**
 * What one pop returned, plus the broker's account of how it sized it.
 *
 * `autopilot.present` is false when the pop did not engage autopilot, when the
 * broker is older than 1.2, or when the answer was a bodiless 204 (an empty
 * non-conflating pop) with nowhere to carry the echo.
 */
struct PopResult {
    std::vector<Message> messages;
    util::AutopilotDecision autopilot;
    bool empty() const { return messages.empty(); }
};

/** Why a consume() loop stopped. */
enum class StopReason {
    /// The loop ended on its own (a terminal error is thrown instead).
    Ended,
    /// It handled `limit` messages.
    Limit,
    /// No message for `idle_millis`.
    Idle,
    /// A Cancel was triggered, or a signal arrived (ClientConfig::handle_signals).
    Cancelled
};

/** What a consumer did before it stopped. The counters are the loop's decisions. */
struct ConsumeSummary {
    uint64_t processed = 0;
    uint64_t acked = 0;
    uint64_t nacked = 0;
    StopReason stopped_by = StopReason::Ended;
};

/**
 * The answer to a commit.
 *
 * `success` false with reason "kv_precondition" is a VALUE, returned and not
 * thrown: a required KV precondition lost (the idempotency marker of a
 * redelivered step doing its job, or a lock's guard), and nothing committed.
 * Branch on failed_index, kv_reason, version and value. Every other rollback
 * is thrown as TransactionError.
 */
struct TransactionResult {
    bool success = false;
    std::string transaction_id;
    /// One result per operation, in a flat index space: every pushed item and
    /// every ack in the order added, then the kv rider, then the timers.
    json results = json::array();
    std::string error;
    std::string reason;
    /// The flat index of the precondition that lost.
    std::optional<size_t> failed_index;
    std::string kv_reason;
    std::optional<long long> version;
    json value;
    /// The answer as it came, for the fields above do not cover.
    json raw;

    bool lost_precondition() const { return !success && reason == "kv_precondition"; }

    static TransactionResult from_json(const json& answer) {
        TransactionResult r;
        r.raw = answer;
        if (!answer.is_object()) return r;
        r.success = answer.contains("success") && answer["success"].is_boolean() &&
                    answer["success"].get<bool>();
        r.transaction_id = util::text_of(answer, "transactionId");
        if (answer.contains("results") && answer["results"].is_array()) r.results = answer["results"];
        r.error = util::text_of(answer, "error");
        r.reason = util::text_of(answer, "reason");
        if (answer.contains("failedIndex") && answer["failedIndex"].is_number_unsigned()) {
            r.failed_index = answer["failedIndex"].get<size_t>();
        }
        r.kv_reason = util::text_of(answer, "kvReason");
        if (answer.contains("version") && answer["version"].is_number_integer()) {
            r.version = answer["version"].get<long long>();
        }
        if (answer.contains("value")) r.value = answer["value"];
        return r;
    }
};

/** A transaction the broker rolled back for a reason other than a lost precondition. */
class TransactionError : public Error {
private:
    TransactionResult result_;

public:
    explicit TransactionError(TransactionResult result)
        : Error("transaction rolled back: " +
                (result.error.empty() ? std::string("no reason given") : result.error)),
          result_(std::move(result)) {
    }
    const TransactionResult& result() const { return result_; }
};

namespace util {

/** The messages of a pop answer (null for a bodiless 204). */
inline std::vector<Message> messages_of(const json& answer) {
    std::vector<Message> out;
    if (!answer.is_object() || !answer.contains("messages") || !answer["messages"].is_array()) {
        return out;
    }
    out.reserve(answer["messages"].size());
    for (const auto& item : answer["messages"]) {
        out.push_back(Message::from_json(item, answer));
    }
    return out;
}

/**
 * The per-item verdicts of POST /api/v1/ack or /api/v1/ack/batch: HTTP 200 with
 * one item per acknowledgment, in request order. A short or misaligned answer
 * throws instead of being read against the wrong message.
 */
inline std::vector<AckResult> parse_ack_results(const json& answer, size_t expected) {
    if (!answer.is_array()) {
        if (answer.is_object() && !text_of(answer, "error").empty()) {
            throw Error("ack refused: " + text_of(answer, "error"));
        }
        throw Error("ack: unexpected answer, no per-item result array");
    }
    if (answer.size() != expected) {
        throw Error("ack returned " + std::to_string(answer.size()) + " results for " +
                    std::to_string(expected) + " acknowledgments");
    }
    std::vector<AckResult> out;
    out.reserve(answer.size());
    for (size_t i = 0; i < answer.size(); ++i) {
        out.push_back(AckResult::from_json(answer[i], i));
    }
    return out;
}

/** The verdicts of a push, in request order. */
inline std::vector<PushResult> parse_push_results(const json& answer) {
    std::vector<PushResult> out;
    const json* items = &answer;
    if (answer.is_object() && answer.contains("results") && answer["results"].is_array()) {
        items = &answer["results"];
    }
    if (!items->is_array()) return out;
    out.reserve(items->size());
    for (size_t i = 0; i < items->size(); ++i) {
        out.push_back(PushResult::from_json((*items)[i], i));
    }
    return out;
}

/**
 * The answer to POST /api/v1/lease/:leaseId/extend, which is HTTP 200 whether
 * or not anything was renewed: success false means the lease is gone, and its
 * messages may already be on their way to another consumer.
 */
inline RenewResult parse_renew(const json& answer, const std::string& lease_id) {
    const json& item = (answer.is_array() && !answer.empty()) ? answer[0] : answer;
    RenewResult r;
    r.lease_id = lease_id;
    if (!item.is_object()) {
        r.error = "unexpected lease renewal answer";
        return r;
    }
    for (const char* key : {"newExpiresAt", "expiresAt", "lease_expires_at"}) {
        r.new_expires_at = text_of(item, key);
        if (!r.new_expires_at.empty()) break;
    }
    r.success = item.contains("success") && item["success"].is_boolean()
        ? item["success"].get<bool>() : !r.new_expires_at.empty();
    if (!r.success) {
        r.error = text_of(item, "error");
        if (r.error.empty()) {
            r.error = "lease not renewed: it expired, was released by an ack or nack, or does not exist";
        }
    }
    return r;
}

} // namespace util

// ============================================================================
// KV and timers - value types (PLAN_KV_TIMERS.md §5, §4)
// ============================================================================

/**
 * The expiry of a KV write, which is MANDATORY and is exactly one thing.
 *
 * §5.1: every put, putIfAbsent and incr carries exactly one of `ttlSeconds`
 * (an integer greater than zero) and `forever: true`. Zero or two declarations
 * are the same error, because both mean the caller did not decide, and a
 * default is how a marker becomes immortal. The rule lives in the broker so
 * all seven clients inherit it; this type makes the wrong shape
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
     * immortal state in a shared broker, and the next run inherits it.
     */
    static KvTtl forever() { return KvTtl(true, 0); }

    /**
     * The admitted sugar: an absolute instant, converted to a delta of seconds
     * at send time and rounded UP (§20.1). There is no `expiresAt` field on the
     * wire and there will not be -- one clock, and it is the broker's.
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
 * clock, the broker's, so no client's skew can enter anywhere. A delay in the
 * past is LEGAL and fires on the first cycle.
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
            throw Error("kv_int64: counter is past int64 and cannot be represented");
        }
        return static_cast<long long>(raw);
    }
    if (value.is_number_float()) {
        throw Error(
            "kv_int64: counter " + value.dump() +
            " is not an integer this client can represent exactly; read it as a string instead");
    }
    throw Error("kv_int64: expected a JSON number, got " + value.dump());
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
    /// Commit at delivery: the broker moves the cursor past the messages as it
    /// hands them out. No lease, nothing to ack, at-most-once, and the only
    /// mode that is.
    bool commit_on_delivery = false;
    /// DEPRECATED: the old name of commit_on_delivery, kept so existing code
    /// still compiles. Either one set to true commits at delivery.
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

/** A prefix scan's options: an exclusive keyset cursor, a page size, keys without values. */
struct KvPrefixOptions {
    /// Resume after this key (exclusive); empty = from the start.
    std::string after;
    /// Keys per page; 0 = the broker's default. Clamped server-side, with
    /// `truncated` saying what the clamp cost and `nextAfter` where to resume.
    int limit = 0;
    /// Keys and versions without values.
    bool keys_only = false;
};

namespace wire {

inline json kv_get_op(const std::string& ns, const std::string& key) {
    return json{{"op", "get"}, {"ns", ns}, {"key", key}};
}

inline json kv_get_many_op(const std::string& ns, const std::vector<std::string>& keys) {
    if (keys.empty()) {
        throw std::invalid_argument("getMany needs at least one key");
    }
    return json{{"op", "getMany"}, {"ns", ns}, {"keys", keys}};
}

inline json kv_get_prefix_op(const std::string& ns, const std::string& prefix,
                             const KvPrefixOptions& options) {
    json op = {{"op", "getPrefix"}, {"ns", ns}, {"prefix", prefix}};
    if (!options.after.empty()) op["after"] = options.after;
    if (options.limit > 0) op["limit"] = options.limit;
    if (options.keys_only) op["keysOnly"] = true;
    return op;
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
 * putIfAbsent desugars to `put` with `expect: 0` INSIDE the broker, one code
 * path there. It keeps its own name on the wire because that is the
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

/**
 * A precondition that writes nothing: the key is at `version`, or -- version 0
 * -- is not there. It takes no expiry.
 *
 * The version is an argument and not KvWriteOptions::expect because here it is
 * MANDATORY: a check without one says nothing, and the broker refuses it. With
 * `required` it is the gate of a whole batch or transaction on a key that call
 * does not write, which is how a step is tied to a lock (Lock::guard() is
 * exactly this operation).
 */
inline json kv_check_op(const std::string& ns, const std::string& key, long long version,
                        bool required) {
    if (version < 0) {
        throw std::invalid_argument(
            "a check's version is 0 (the key must not exist) or above");
    }
    json op = {{"op", "check"}, {"ns", ns}, {"key", key}, {"expect", version}};
    if (required) {
        op["required"] = true;
    }
    return op;
}

// ---------------------------------------------------------------------------
// Lock operations (POST /api/v1/locks). One place per wire shape, as above.
// ---------------------------------------------------------------------------

inline void lock_require_name(const std::string& name) {
    // The broker's rule, checked here so the mistake surfaces at the call: no
    // '#', which sits between a name and its slot in the row's key.
    bool bad = name.empty() || name.size() > 256;
    for (size_t i = 0; !bad && i < name.size(); ++i) {
        unsigned char c = static_cast<unsigned char>(name[i]);
        bad = c < 0x20 || c == 0x7f || c == '#' ||
              (c == 0xc2 && i + 1 < name.size() &&
               static_cast<unsigned char>(name[i + 1]) >= 0x80 &&
               static_cast<unsigned char>(name[i + 1]) <= 0x9f);
    }
    if (bad) {
        throw std::invalid_argument(
            "a lock's name is a non-empty string of at most 256 bytes, without control "
            "characters and without '#'");
    }
}

inline void lock_require_ttl(long long ttl_seconds) {
    // Mandatory, finite, in whole seconds. There is no `forever`: a lock that
    // never expires is one nobody can take back from a holder that died.
    if (ttl_seconds <= 0) {
        throw std::invalid_argument(
            "a lock needs a lifetime: ttl_seconds above zero. There is no forever; a holder "
            "that needs longer renews");
    }
}

inline void lock_require_token(long long token, const char* what) {
    if (token <= 0) {
        throw std::invalid_argument(
            std::string(what) +
            " needs the token of the permit (the one the last acquire or renew answered)");
    }
}

inline json lock_acquire_op(const std::string& name, long long ttl_seconds,
                            const std::string& owner, int limit) {
    lock_require_name(name);
    lock_require_ttl(ttl_seconds);
    if (limit < 1 || limit > 1024) {
        throw std::invalid_argument("limit is a whole number from 1 (a lock) to 1024");
    }
    json op = {{"op", "acquire"}, {"name", name}, {"ttlSeconds", ttl_seconds}};
    if (!owner.empty()) {
        op["owner"] = owner;
    }
    // A lock is the semaphore of one, and sends no limit at all.
    if (limit > 1) {
        op["limit"] = limit;
    }
    return op;
}

inline json lock_renew_op(const std::string& name, long long token, long long ttl_seconds,
                          int slot, const std::string& owner) {
    lock_require_name(name);
    lock_require_token(token, "renew");
    lock_require_ttl(ttl_seconds);
    json op = {{"op", "renew"}, {"name", name}, {"token", token}, {"ttlSeconds", ttl_seconds}};
    // Slot 0, the only slot a lock has, is the default and is not sent.
    if (slot != 0) {
        op["slot"] = slot;
    }
    if (!owner.empty()) {
        op["owner"] = owner;
    }
    return op;
}

inline json lock_release_op(const std::string& name, long long token, int slot) {
    lock_require_name(name);
    lock_require_token(token, "release");
    json op = {{"op", "release"}, {"name", name}, {"token", token}};
    if (slot != 0) {
        op["slot"] = slot;
    }
    return op;
}

inline json lock_get_op(const std::string& name) {
    lock_require_name(name);
    return json{{"op", "get"}, {"name", name}};
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
 * never ran the operation -- an old broker that predates this feature, for
 * instance -- into a business answer, and the caller would act on
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
    throw Error(
        std::string(what) + ": the broker returned no result for the operation; this is an "
        "alignment failure, not an absent key");
}

} // namespace wire

// ============================================================================
// The binary push body and the framed pop answer
// ============================================================================
//
// The binary push body is an alternative to the JSON body of POST /api/v1/push:
//
//   "QPB1" | u8 flags | [u32 raw_len, when zstd] | body
//   body:  u32 groups | per group: u16 len, queue | u16 len, partition | u32 n
//          | n x ([u16 len, transactionId, when flag 4] | u32 len, payload)
//
// All integers little-endian. flags: 1 = the body is one zstd frame of raw_len
// bytes, 2 = answer with the counts only, 4 = every item carries its
// transaction id (empty = the broker mints it). No trace id. Each payload is
// one JSON document.
//
// With flag 8 (encode_push_frames) a group is the messages as the broker stores
// them, compressed here once:
//
//   body:  u32 groups | per group: u16 len, queue | u16 len, partition | u32 n | u32 z_len | z
//   z:     one zstd frame of n frames: u32 body_len | u8 0 | u8[16] message id
//          | u16 len, transactionId | payload
//
// The message id is a version-7 UUID minted by the client; a message without
// its own transaction id uses the id's text. The broker checks every frame and
// keeps the compressed bytes as they are, so it does not compress them again.
//
// A broker that does not know this body answers 400; a client sends it only
// when ClientConfig::binary_push is set, so by default a push is the JSON
// request every broker knows.
//
// The binary transaction body (encode_transaction_frames), an alternative to
// the JSON body of POST /api/v1/transaction, carries the pushes the same way:
//
//   "QTB1" | u8 flags | u32 len, head | u32 groups | the groups, as with flag 8 above
//
// head is the JSON transaction body without its push operations: the acks in
// `operations`, `requiredLeases`, `kv`, `timers`. flags: 2 = the answer leaves
// out the pushes' and the acks' results and counts them (`pushed`,
// `duplicate`, `acked`); the riders' results stay. In the answer the head's
// operations come first and the groups' messages after them, in the order
// written; the riders follow, as in the JSON body.
//
// The framed pop answer is what a broker sends for a pop with `frames=true`:
// the messages as the stored records, each still the zstd frame the producer
// (or the broker) made, unpacked here.
//
//   "QPF1" | u8 1 | u32 len, the answer's own keys as one JSON object
//   u16 len, queue | u16 len, leaseId | u16 len, consumerGroup | u32 claims
//   per claim:  u64 partitionId | u16 len, partition | u64 start | u64 end | u32 deliveryAttempt | u32 records
//   per record: u64 base_offset | i64 created_at_us | u32 count | u8 codec (0 raw, 1 zstd) | u32 len | bytes
//
// A record's bytes are `count` frames:
//
//   u32 body_len | u8 flags | u8[16] message id | [u8[16] trace id, flag 1]
//   | u16 len, transactionId | [u16 len, producer, flag 2] | payload
//
// Message i of a record has offset base_offset+i and belongs to the delivery
// when that lies in the claim's start..=end. The JSON object in front holds
// what the JSON answer has beside its messages: the conflation echo and the
// autopilot's choice.

namespace wire {

inline constexpr const char* PUSH_BINARY_CONTENT_TYPE = "application/x-queen-push";
inline constexpr const char* POP_FRAMES_CONTENT_TYPE = "application/x-queen-pop";

/** Whether this build has zstd (QUEEN_CLIENT_ZSTD). */
inline bool zstd_available() {
#ifdef QUEEN_CLIENT_ZSTD
    return true;
#else
    return false;
#endif
}

/**
 * One zstd frame of `raw`, at the fastest level. The frame carries its content
 * size, which the broker asks of a frame it stores as it came.
 */
inline std::string zstd_compress(const std::string& raw) {
#ifdef QUEEN_CLIENT_ZSTD
    std::string frame(ZSTD_compressBound(raw.size()), '\0');
    const size_t written = ZSTD_compress(frame.data(), frame.size(), raw.data(), raw.size(), 1);
    if (ZSTD_isError(written)) {
        throw Error(std::string("zstd: ") + ZSTD_getErrorName(written));
    }
    frame.resize(written);
    return frame;
#else
    (void)raw;
    throw Error("zstd is not in this build: compile with -DQUEEN_CLIENT_ZSTD and link -lzstd");
#endif
}

inline std::string zstd_decompress(const std::string& frame) {
#ifdef QUEEN_CLIENT_ZSTD
    // Largest record inflated here: the broker's own bound on a stored one.
    constexpr unsigned long long MAX_RECORD_BYTES = 1ULL << 30;
    const unsigned long long size = ZSTD_getFrameContentSize(frame.data(), frame.size());
    if (size == ZSTD_CONTENTSIZE_ERROR || size == ZSTD_CONTENTSIZE_UNKNOWN || size > MAX_RECORD_BYTES) {
        throw Error("zstd: a frame without a plausible content size");
    }
    std::string raw(static_cast<size_t>(size), '\0');
    const size_t written = ZSTD_decompress(raw.data(), raw.size(), frame.data(), frame.size());
    if (ZSTD_isError(written) || written != raw.size()) {
        throw Error(std::string("zstd: ") +
            (ZSTD_isError(written) ? ZSTD_getErrorName(written) : "a frame shorter than its content size"));
    }
    return raw;
#else
    (void)frame;
    throw Error("zstd is not in this build: compile with -DQUEEN_CLIENT_ZSTD and link -lzstd");
#endif
}

/** One partition's messages for encode_push_binary and encode_push_frames. */
struct PushBatch {
    std::string queue;
    std::string partition;
    std::vector<std::string> payloads;                          // each one JSON document
    // Empty optional, or one transaction id per payload ("" = the broker mints it).
    std::optional<std::vector<std::string>> transaction_ids;
};

/** A name or a transaction id does not fit its 16-bit length. */
class PushBinaryTooLong : public std::length_error {
public:
    using std::length_error::length_error;
};

inline void put_u16(std::string& out, size_t value) {
    out.push_back(static_cast<char>(value & 0xFF));
    out.push_back(static_cast<char>((value >> 8) & 0xFF));
}

inline void put_u32(std::string& out, size_t value) {
    for (int shift = 0; shift < 32; shift += 8) {
        out.push_back(static_cast<char>((value >> shift) & 0xFF));
    }
}

inline void put_text16(std::string& out, const std::string& text, const char* what) {
    if (text.size() > 0xFFFF) {
        throw PushBinaryTooLong(std::string(what) + " longer than 65535 bytes");
    }
    put_u16(out, text.size());
    out += text;
}

inline void check_push_batch(const PushBatch& batch) {
    if (batch.transaction_ids && batch.transaction_ids->size() != batch.payloads.size()) {
        throw std::invalid_argument(std::to_string(batch.transaction_ids->size()) +
            " transaction ids for " + std::to_string(batch.payloads.size()) + " payloads");
    }
}

/**
 * Encode batches as a binary push body. compress: the body is zstd-compressed.
 * counts: the broker answers {"count","queued","duplicate","error"} instead of
 * one object per message.
 */
inline std::string encode_push_binary(const std::vector<PushBatch>& batches,
                                      bool compress = false, bool counts = false) {
    bool with_txn = false;
    for (const auto& batch : batches) {
        with_txn = with_txn || batch.transaction_ids.has_value();
    }
    const int flags = (counts ? 2 : 0) | (with_txn ? 4 : 0);

    std::string body;
    put_u32(body, batches.size());
    for (const auto& batch : batches) {
        check_push_batch(batch);
        put_text16(body, batch.queue, "queue name");
        put_text16(body, batch.partition, "partition name");
        put_u32(body, batch.payloads.size());
        for (size_t i = 0; i < batch.payloads.size(); ++i) {
            if (with_txn) {
                put_text16(body, batch.transaction_ids ? (*batch.transaction_ids)[i] : std::string(),
                           "transaction id");
            }
            put_u32(body, batch.payloads[i].size());
            body += batch.payloads[i];
        }
    }

    std::string out = "QPB1";
    if (!compress) {
        out.push_back(static_cast<char>(flags));
        out += body;
        return out;
    }
    out.push_back(static_cast<char>(flags | 1));
    put_u32(out, body.size());
    out += zstd_compress(body);
    return out;
}

/**
 * Encode batches as a binary push body whose groups are the messages framed as
 * the broker stores them and compressed once, here. counts: the broker answers
 * with the counts instead of one object per message.
 */
inline std::string append_frame_groups(std::string out, const std::vector<PushBatch>& batches);

inline std::string encode_push_frames(const std::vector<PushBatch>& batches, bool counts = false) {
    std::string out = "QPB1";
    out.push_back(static_cast<char>(8 | (counts ? 2 : 0)));
    return append_frame_groups(std::move(out), batches);
}

/**
 * Encode a binary transaction body: head, the JSON transaction body without
 * its push operations (empty for none), then batches as groups of messages
 * framed as the broker stores them and compressed once, here. counts: the
 * broker counts the pushes and the acks in its answer instead of rendering one
 * result for each.
 */
inline std::string encode_transaction_frames(const std::string& head,
                                             const std::vector<PushBatch>& batches,
                                             bool counts = false) {
    std::string out = "QTB1";
    out.push_back(static_cast<char>(counts ? 2 : 0));
    put_u32(out, head.size());
    out += head;
    return append_frame_groups(std::move(out), batches);
}

/**
 * Appends `u32 groups | per group: u16 len, queue | u16 len, partition | u32 n
 * | u32 z_len | z` to out: each batch's messages framed as the broker stores
 * them, with a version-7 id minted here, as one zstd frame.
 */
inline std::string append_frame_groups(std::string out, const std::vector<PushBatch>& batches) {
    put_u32(out, batches.size());
    std::string frames;
    for (const auto& batch : batches) {
        check_push_batch(batch);
        frames.clear();
        for (size_t i = 0; i < batch.payloads.size(); ++i) {
            const auto id = util::generate_uuid_v7_bytes();
            std::string txn = batch.transaction_ids ? (*batch.transaction_ids)[i] : std::string();
            if (txn.empty()) {
                txn = util::uuid_text(id.data());
            }
            if (txn.size() > 0xFFFF) {
                throw PushBinaryTooLong("transaction id longer than 65535 bytes");
            }
            put_u32(frames, 1 + 16 + 2 + txn.size() + batch.payloads[i].size());
            frames.push_back('\0');
            frames.append(reinterpret_cast<const char*>(id.data()), id.size());
            put_text16(frames, txn, "transaction id");
            frames += batch.payloads[i];
        }
        put_text16(out, batch.queue, "queue name");
        put_text16(out, batch.partition, "partition name");
        put_u32(out, batch.payloads.size());
        const std::string compressed = zstd_compress(frames);
        put_u32(out, compressed.size());
        out += compressed;
    }
    return out;
}

/**
 * Adds one pushed item to batches, in the last one when it is the same
 * partition's. False when the item cannot travel in a binary body; throws
 * json::exception for a payload JSON does not carry.
 */
inline bool add_push_item(std::vector<PushBatch>& batches, const json& item) {
    if (!item.is_object() || !item.contains("queue") || !item["queue"].is_string() ||
        !item.contains("payload")) {
        return false;
    }
    if (item.contains("traceId") && !item["traceId"].is_null()) {
        return false;
    }
    std::string txn;
    if (item.contains("transactionId") && !item["transactionId"].is_null()) {
        if (!item["transactionId"].is_string()) return false;
        txn = item["transactionId"].get<std::string>();
    }
    std::string partition = "Default";
    if (item.contains("partition") && !item["partition"].is_null()) {
        if (!item["partition"].is_string()) return false;
        partition = item["partition"].get<std::string>();
    }
    const std::string queue = item["queue"].get<std::string>();
    if (batches.empty() || batches.back().queue != queue || batches.back().partition != partition) {
        batches.push_back({queue, partition, {}, std::vector<std::string>{}});
    }
    batches.back().payloads.push_back(item["payload"].dump());
    batches.back().transaction_ids->push_back(std::move(txn));
    return true;
}

/**
 * The items of a JSON push ({queue, partition, payload, transactionId}) as a
 * binary push body, or nothing when one of them cannot travel in it: a trace
 * id, a transaction id that is not a string, a payload JSON does not carry, a
 * name over 65535 bytes. The caller then sends the JSON body, and the broker
 * answers that as it always did. Consecutive items of one partition share a
 * group, so the order of the items is the order on the wire.
 *
 * compress: the messages framed and compressed here, kept by the broker as
 * they are.
 */
inline std::optional<std::string> binary_push_body(const json& items, bool compress) {
    if (!items.is_array() || items.empty()) {
        return std::nullopt;
    }
    std::vector<PushBatch> batches;
    try {
        for (const auto& item : items) {
            if (!add_push_item(batches, item)) {
                return std::nullopt;
            }
        }
        return compress ? encode_push_frames(batches) : encode_push_binary(batches);
    } catch (const PushBinaryTooLong&) {
        // The JSON push says so in the broker's words.
        return std::nullopt;
    } catch (const json::exception&) {
        // A payload JSON does not carry (text that is not UTF-8).
        return std::nullopt;
    }
}

/** A JSON transaction body as the binary one (transaction_frames). */
struct TransactionFrames {
    std::string body;
    // Empty when the broker's answer lists the operations as the caller added
    // them (every other operation before every push). Otherwise order[k] is
    // the place, among the caller's operations, of the k-th result the broker
    // sends: it lists the head's operations first, then the pushes.
    std::optional<std::vector<size_t>> order;
};

/**
 * A JSON transaction body ({operations, requiredLeases, kv, timers}) as the
 * binary one, when its pushes can travel in it: there is at least one, and
 * none carries a trace id. Everything but the pushes stays in the JSON head;
 * consecutive pushed items of one partition share a group, so the pushes keep
 * their order. Nothing otherwise: the caller then sends the JSON body.
 */
inline std::optional<TransactionFrames> transaction_frames(const json& request) {
    if (!request.is_object() || !request.contains("operations") || !request["operations"].is_array()) {
        return std::nullopt;
    }
    json head_operations = json::array();
    std::vector<PushBatch> batches;
    std::vector<size_t> head_at;
    std::vector<size_t> push_at;
    size_t flat = 0;
    try {
        for (const auto& operation : request["operations"]) {
            if (!operation.is_object() || operation.value("type", json()) != "push") {
                head_operations.push_back(operation);
                head_at.push_back(flat++);
                continue;
            }
            // A push names its items, or is one item itself.
            if (operation.contains("items") && operation["items"].is_array()) {
                for (const auto& item : operation["items"]) {
                    if (!add_push_item(batches, item)) return std::nullopt;
                    push_at.push_back(flat++);
                }
            } else {
                if (!add_push_item(batches, operation)) return std::nullopt;
                push_at.push_back(flat++);
            }
        }
        if (batches.empty()) {
            return std::nullopt;
        }
        json head = request;
        head["operations"] = std::move(head_operations);
        TransactionFrames framed{encode_transaction_frames(head.dump(), batches), std::nullopt};
        std::vector<size_t> order = std::move(head_at);
        order.insert(order.end(), push_at.begin(), push_at.end());
        for (size_t k = 0; k < order.size(); ++k) {
            if (order[k] != k) {
                framed.order = std::move(order);
                break;
            }
        }
        return framed;
    } catch (const PushBinaryTooLong&) {
        // The JSON request says so in the broker's words.
        return std::nullopt;
    } catch (const json::exception&) {
        // A payload or a head JSON does not carry (text that is not UTF-8).
        return std::nullopt;
    }
}

/**
 * Puts the results of a binary transaction back in the order the caller added
 * its operations in, and gives each its own index again. The riders' results,
 * after the operations', are already in place.
 */
inline void restore_result_order(json& answer, const std::optional<std::vector<size_t>>& order) {
    if (!order || !answer.is_object() || !answer.contains("results") ||
        !answer["results"].is_array() || answer["results"].size() < order->size()) {
        return;
    }
    json restored = answer["results"];
    for (size_t k = 0; k < order->size(); ++k) {
        const size_t at = (*order)[k];
        restored[at] = answer["results"][k];
        if (restored[at].is_object()) {
            restored[at]["index"] = at;
        }
    }
    answer["results"] = std::move(restored);
}

/** Whether a pop answer's bytes are the framed one. */
inline bool is_pop_frames(const std::string& bytes) {
    return bytes.size() > 4 && bytes.compare(0, 4, "QPF1") == 0;
}

/** Reads little-endian fields off a byte range; a read past the end throws. */
class FrameReader {
private:
    const unsigned char* at_;
    size_t left_;
    std::string short_;

    uint64_t number(size_t width) {
        const unsigned char* p = take(width);
        uint64_t value = 0;
        for (size_t i = 0; i < width; ++i) {
            value |= static_cast<uint64_t>(p[i]) << (8 * i);
        }
        return value;
    }

public:
    FrameReader(const char* data, size_t size, std::string short_message = "truncated")
        : at_(reinterpret_cast<const unsigned char*>(data)), left_(size), short_(std::move(short_message)) {}

    const unsigned char* take(size_t length) {
        if (left_ < length) {
            throw Error("framed pop answer: " + short_);
        }
        const unsigned char* piece = at_;
        at_ += length;
        left_ -= length;
        return piece;
    }

    uint8_t u8() { return *take(1); }
    size_t u16() { return static_cast<size_t>(number(2)); }
    uint32_t u32() { return static_cast<uint32_t>(number(4)); }
    uint64_t u64() { return number(8); }

    std::string bytes(size_t length) {
        return std::string(reinterpret_cast<const char*>(take(length)), length);
    }

    std::string text() { return bytes(u16()); }
    std::string rest() { return bytes(left_); }
};

/** Microseconds since the epoch as the broker writes `createdAt`. */
inline std::string iso_from_micros(int64_t micros) {
    std::time_t seconds = static_cast<std::time_t>(micros / 1000000);
    std::tm parts{};
#ifdef _WIN32
    gmtime_s(&parts, &seconds);
#else
    gmtime_r(&seconds, &parts);
#endif
    // Room for the widest ints, so no compiler can call the format truncated.
    char text[96];
    std::snprintf(text, sizeof(text), "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
                  parts.tm_year + 1900, parts.tm_mon + 1, parts.tm_mday,
                  parts.tm_hour, parts.tm_min, parts.tm_sec,
                  static_cast<int>((micros % 1000000) / 1000));
    return text;
}

/**
 * Unpack a framed pop answer into the json the JSON answer parses to:
 *
 *   {success, queue, partition, partitionId, leaseId, consumerGroup,
 *    messages: [{id, transactionId, traceId, data, producerSub, createdAt,
 *                partitionId, partition, leaseId, consumerGroup,
 *                deliveryAttempt, offset}],
 *    partitionsClaimed, ...the answer's own keys}
 *
 * The messages are in delivery order. Everything that reads a pop answer (the
 * conflation contract, the autopilot's choice, the consume loop) reads this as
 * it reads the JSON one. Throws std::runtime_error on an answer that is not
 * whole.
 */
inline json decode_pop_frames(const std::string& bytes) {
    if (!is_pop_frames(bytes)) {
        throw Error("framed pop answer: not a framed pop answer");
    }
    FrameReader answer(bytes.data() + 4, bytes.size() - 4);
    const int version = answer.u8();
    json own = nullptr;
    if (version == 1) {
        const std::string text = answer.bytes(answer.u32());
        if (!text.empty()) {
            try {
                own = json::parse(text);
            } catch (const json::exception& error) {
                throw Error(std::string("framed pop answer: its own keys: ") + error.what());
            }
        }
    } else if (version != 0) {
        throw Error("framed pop answer: unknown version " + std::to_string(version));
    }

    const std::string queue = answer.text();
    const std::string lease_id = answer.text();
    const std::string consumer_group = answer.text();
    const uint32_t claims = answer.u32();
    json messages = json::array();
    std::string first_partition;
    std::string first_partition_id;

    for (uint32_t claim = 0; claim < claims; ++claim) {
        const std::string partition_id = std::to_string(answer.u64());
        const std::string partition = answer.text();
        const uint64_t start = answer.u64();
        const uint64_t end = answer.u64();
        const uint32_t delivery_attempt = answer.u32();
        const uint32_t records = answer.u32();
        if (claim == 0) {
            first_partition = partition;
            first_partition_id = partition_id;
        }
        for (uint32_t record = 0; record < records; ++record) {
            const uint64_t base = answer.u64();
            const std::string created_at = iso_from_micros(static_cast<int64_t>(answer.u64()));
            const uint32_t count = answer.u32();
            const uint8_t codec = answer.u8();
            std::string raw = answer.bytes(answer.u32());
            if (codec == 1) {
                try {
                    raw = zstd_decompress(raw);
                } catch (const std::runtime_error& error) {
                    throw Error(std::string("framed pop answer: ") + error.what());
                }
            } else if (codec != 0) {
                throw Error("framed pop answer: unknown codec " + std::to_string(codec));
            }
            FrameReader frames(raw.data(), raw.size(),
                               "a record is shorter than its " + std::to_string(count) + " frames");
            for (uint32_t i = 0; i < count; ++i) {
                const uint32_t length = frames.u32();
                FrameReader frame(reinterpret_cast<const char*>(frames.take(length)), length,
                                  "a frame is malformed");
                const uint8_t flags = frame.u8();
                const unsigned char* id = frame.take(16);
                const unsigned char* trace = (flags & 1) ? frame.take(16) : nullptr;
                std::string transaction_id = frame.text();
                json producer_sub = nullptr;
                if (flags & 2) {
                    producer_sub = frame.text();
                }
                if (flags & 4) {
                    throw Error("framed pop answer: a frame is encrypted");
                }
                const uint64_t offset = base + i;
                if (offset < start || offset > end) {
                    continue;
                }
                const std::string payload = frame.rest();
                json data = nullptr;
                if (!payload.empty()) {
                    try {
                        data = json::parse(payload);
                    } catch (const json::exception& error) {
                        throw Error(std::string("framed pop answer: a payload: ") + error.what());
                    }
                }
                messages.push_back({
                    {"id", util::uuid_text(id)},
                    {"transactionId", std::move(transaction_id)},
                    {"traceId", trace ? json(util::uuid_text(trace)) : json(nullptr)},
                    {"data", std::move(data)},
                    {"producerSub", std::move(producer_sub)},
                    {"createdAt", created_at},
                    {"partitionId", partition_id},
                    {"partition", partition},
                    {"leaseId", lease_id},
                    {"consumerGroup", consumer_group},
                    {"deliveryAttempt", delivery_attempt},
                    {"offset", offset}
                });
            }
        }
    }

    json result = {
        {"success", true},
        {"queue", queue},
        {"partition", first_partition},
        {"partitionId", first_partition_id},
        {"leaseId", lease_id},
        {"consumerGroup", consumer_group},
        {"messages", std::move(messages)},
        {"partitionsClaimed", claims}
    };
    if (own.is_object()) {
        for (auto it = own.begin(); it != own.end(); ++it) {
            // "frames" only says the answer is this one; the JSON answer has no such key.
            if (it.key() != "frames") {
                result[it.key()] = it.value();
            }
        }
    }
    return result;
}

} // namespace wire

// ============================================================================
// LoadBalancer - backend selection across broker replicas
// ============================================================================

/**
 * Picks a backend per request and tracks which ones are answering. A port of
 * the Rust client's balancer (clients/client-rust/src/lb.rs), which is itself
 * the JS/Go/Python one: same strategies, same ring, same hash.
 *
 * Brokers are interchangeable, so any of them can serve any request -- but
 * consumers should not bounce between them. Two replicas polling the same
 * partition for the same group contend for one claim. LoadBalancing::Affinity
 * hashes the request's grouping key (util::grouping_key, the broker's own
 * grouping) onto a ring of `replicas` virtual nodes per backend, so each
 * (queue, partition, group) stays on one replica for as long as it is healthy.
 * The hash is FNV-1a over the same strings as the other SDKs, so every client
 * maps a key to the same backend.
 *
 * Health: a 5xx or a transport fault marks a backend down (never a 4xx, the
 * caller's mistake, and never a 429, a backend that answered). It comes back
 * after `retry_after_millis`, or as soon as it answers. When every backend is
 * down one is picked anyway: refusing to send is worse than sending to a
 * backend that may have recovered.
 *
 * Thread-safe.
 */
class LoadBalancer {
private:
    using Clock = std::chrono::steady_clock;

    struct Health {
        bool healthy = true;
        uint32_t failures = 0;
        std::optional<Clock::time_point> last_failure;
    };
    struct VNode {
        uint32_t hash;
        size_t server;
    };

    std::vector<std::string> urls_;
    LoadBalancing strategy_;
    size_t replicas_;
    std::chrono::milliseconds retry_after_;

    mutable std::mutex mutex_;
    std::vector<Health> health_;
    std::vector<VNode> ring_;
    size_t next_ = 0;
    std::optional<size_t> session_;

    std::vector<VNode> build_ring(const std::vector<size_t>& servers) const {
        std::vector<VNode> ring;
        ring.reserve(servers.size() * replicas_);
        for (size_t s : servers) {
            for (size_t i = 0; i < replicas_; ++i) {
                ring.push_back({util::fnv1a(urls_[s] + "#vnode" + std::to_string(i)), s});
            }
        }
        std::sort(ring.begin(), ring.end(),
                  [](const VNode& a, const VNode& b) { return a.hash < b.hash; });
        return ring;
    }

    std::vector<size_t> live_locked() const {
        std::vector<size_t> live;
        for (size_t i = 0; i < urls_.size(); ++i) {
            if (health_[i].healthy) live.push_back(i);
        }
        return live;
    }

public:
    LoadBalancer(std::vector<std::string> urls, LoadBalancing strategy = LoadBalancing::Affinity,
                 int replicas = 128, int retry_after_millis = 5000)
        : strategy_(strategy), replicas_(replicas > 0 ? static_cast<size_t>(replicas) : 1),
          retry_after_(std::max(0, retry_after_millis)) {
        if (urls.empty()) {
            throw std::invalid_argument("at least one broker URL is required");
        }
        for (auto& url : urls) {
            while (!url.empty() && url.back() == '/') url.pop_back();
            urls_.push_back(url);
        }
        health_.resize(urls_.size());
        std::vector<size_t> all(urls_.size());
        for (size_t i = 0; i < all.size(); ++i) all[i] = i;
        ring_ = build_ring(all);
    }

    const std::vector<std::string>& urls() const { return urls_; }
    LoadBalancing strategy() const { return strategy_; }

    /** The backend index for a request with this affinity key ("" = none). */
    size_t pick(const std::string& affinity_key = "") {
        std::lock_guard<std::mutex> lock(mutex_);
        // A backend that failed long enough ago goes back in the pool; without
        // this a transient blip would take a replica out for good.
        const auto now = Clock::now();
        bool readmitted = false;
        for (auto& h : health_) {
            if (!h.healthy && h.last_failure && now - *h.last_failure >= retry_after_) {
                h.healthy = true;
                readmitted = true;
            }
        }
        const auto live = live_locked();
        if (live.empty()) {
            const size_t i = next_ % urls_.size();
            ++next_;
            return i;
        }
        if (readmitted) {
            ring_ = build_ring(live);
        }
        switch (strategy_) {
        case LoadBalancing::Affinity:
            if (!affinity_key.empty() && !ring_.empty()) {
                const uint32_t target = util::fnv1a(affinity_key);
                // The first virtual node clockwise from the key's position.
                size_t start = static_cast<size_t>(
                    std::lower_bound(ring_.begin(), ring_.end(), target,
                                     [](const VNode& v, uint32_t t) { return v.hash < t; }) -
                    ring_.begin());
                if (start >= ring_.size()) start = ring_.size() - 1;
                for (size_t off = 0; off < ring_.size(); ++off) {
                    const VNode& v = ring_[(start + off) % ring_.size()];
                    if (health_[v.server].healthy) return v.server;
                }
                return live[0];
            }
            // No key (an admin call, say): nothing to be sticky about.
            break;
        case LoadBalancing::Session:
            if (session_ && health_[*session_].healthy) return *session_;
            session_ = live[next_ % live.size()];
            ++next_;
            return *session_;
        case LoadBalancing::RoundRobin:
            break;
        }
        const size_t i = live[next_ % live.size()];
        ++next_;
        return i;
    }

    /** A backend answered: clear its failures, and put it back on the ring if it was out. */
    void mark_healthy(size_t index) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (index >= health_.size()) return;
        const bool was_down = !health_[index].healthy;
        health_[index] = Health{};
        if (was_down) ring_ = build_ring(live_locked());
    }

    /** A backend failed in a way that says it is down: a 5xx or a transport fault. */
    void mark_unhealthy(size_t index) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (index >= health_.size()) return;
        auto& h = health_[index];
        h.healthy = false;
        ++h.failures;
        h.last_failure = Clock::now();
        ring_ = build_ring(live_locked());
    }

    size_t healthy_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return live_locked().size();
    }

    /** The URL pick() chooses. */
    std::string get_next_url(const std::string& affinity_key = "") {
        return urls_[pick(affinity_key)];
    }

    std::vector<std::string> get_all_urls() const { return urls_; }
};

// ============================================================================
// HttpClient - transport: a keep-alive connection pool, TLS, retry, failover
// ============================================================================

/**
 * Per-request knobs. Zero values mean "the client's".
 */
struct RequestOptions {
    /// Overrides the client timeout. A long-poll pop needs its own, longer than
    /// the poll window.
    int timeout_millis = 0;
    /// Which 429 policy applies (RetryKind::Pop for a long-poll pop).
    RetryKind retry_kind = RetryKind::Default;
    /// Pins this request to a backend on the affinity ring; empty = none.
    std::string affinity_key;
    /// One attempt on one backend (429s are still waited out): for a write
    /// whose verdict the caller has to read, which a blind resend would hide.
    bool no_retry = false;
};

/**
 * The transport every other class uses. Three retry mechanisms sit on top of
 * each other and are kept apart, as in the Rust client:
 *
 *   1. 429 in place: the backend is alive and shedding load. Retried against
 *      the same backend with backoff (Retry-After when sent, jittered).
 *   2. Failover: a 5xx or a transport fault marks the backend down and the
 *      next one is tried, once each, when there is more than one URL.
 *   3. Retry: with one backend (or failover off) the same URL is retried
 *      retry_attempts times with exponential backoff.
 *
 * A 4xx other than 429 triggers none of them: it is the caller's mistake.
 *
 * CONNECTIONS ARE REUSED. Every backend has a pool of idle keep-alive
 * connections (up to ClientConfig::max_idle_connections_per_host); a request
 * takes one, or opens one, and gives it back when the answer has been read.
 * A connection that failed is closed instead. Before 2.7.1 this client opened a
 * TCP connection per request: one extra round trip on every call, and on the
 * broker an accept and a close per request.
 *
 * HTTPS IS HTTPS. A URL's scheme decides: https:// is TLS, with the server's
 * certificate and host name verified against the system's CA store (or
 * ClientConfig::ca_cert_path). Before 2.7.1 an https:// URL was sent as plain
 * HTTP, bearer token included.
 *
 * Thread-safe: share one per process (Queen does).
 */
class HttpClient {
public:
    /** An answer with its status, before any status is turned into an exception. */
    struct Response {
        int status = 0;
        std::string body;
        std::string content_type;
        std::optional<double> retry_after_seconds;  // 429 only
    };

private:
    using Clock = std::chrono::steady_clock;

    ClientConfig config_;
    std::shared_ptr<LoadBalancer> load_balancer_;
    httplib::Headers base_headers_;

    struct Idle {
        std::unique_ptr<httplib::Client> client;
        Clock::time_point since;
    };
    mutable std::mutex pool_mutex_;
    std::unordered_map<std::string, std::vector<Idle>> idle_;

    // The binary push body and the framed pop answer (namespace wire). Off by
    // default: the client then sends and reads the JSON every broker knows.
    bool binary_push_ = false;
    bool push_compression_ = false;
    bool framed_pop_ = false;

    static bool is_https(const std::string& base_url) {
        return base_url.rfind("https://", 0) == 0;
    }

    std::unique_ptr<httplib::Client> make_connection(const std::string& base_url) const {
        // Built from the full URL, so its scheme picks plain or TLS.
        auto client = std::make_unique<httplib::Client>(base_url);
        client->set_keep_alive(true);
        client->set_tcp_nodelay(true);
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
        if (is_https(base_url)) {
            client->enable_server_certificate_verification(config_.tls_verify);
            client->enable_server_hostname_verification(config_.tls_verify);
            if (!config_.ca_cert_path.empty() || !config_.ca_cert_dir.empty()) {
                client->set_ca_cert_path(config_.ca_cert_path, config_.ca_cert_dir);
            }
        }
#endif
        return client;
    }

    std::unique_ptr<httplib::Client> acquire(const std::string& base_url) {
        const auto now = Clock::now();
        const auto max_idle = std::chrono::milliseconds(config_.idle_connection_timeout_millis);
        std::vector<Idle> stale;
        {
            std::lock_guard<std::mutex> lock(pool_mutex_);
            auto& idle = idle_[base_url];
            while (!idle.empty()) {
                Idle entry = std::move(idle.back());
                idle.pop_back();
                if (config_.idle_connection_timeout_millis <= 0 || now - entry.since < max_idle) {
                    return std::move(entry.client);
                }
                // Idle too long: a proxy or the broker may have dropped it.
                stale.push_back(std::move(entry));
            }
        }
        return make_connection(base_url);  // `stale` closes outside the lock
    }

    void release(const std::string& base_url, std::unique_ptr<httplib::Client> client) {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        auto& idle = idle_[base_url];
        if (static_cast<int>(idle.size()) < config_.max_idle_connections_per_host) {
            idle.push_back({std::move(client), Clock::now()});
        }
    }

    static std::optional<double> parse_retry_after(const std::string& value) {
        if (value.empty()) return std::nullopt;
        try {
            size_t consumed = 0;
            double seconds = std::stod(value, &consumed);
            if (consumed != value.size() || seconds < 0) return std::nullopt;
            return seconds;
        } catch (...) {
            return std::nullopt;
        }
    }

    /**
     * One HTTP exchange on one backend. Any status comes back as a Response;
     * only a transport fault throws (NetworkError), and only then is the
     * connection closed instead of going back to the pool.
     */
    Response execute_once(const std::string& base_url, const std::string& method,
                          const std::string& path, const std::string& body,
                          const char* content_type, int timeout_millis) {
        auto connection = acquire(base_url);
        const auto timeout = std::chrono::milliseconds(
            timeout_millis > 0 ? timeout_millis : config_.timeout_millis);
        connection->set_connection_timeout(timeout);
        connection->set_read_timeout(timeout);
        connection->set_write_timeout(timeout);

        httplib::Headers headers = base_headers_;

        // A body's Content-Type travels as httplib's content-type argument,
        // which adds the header itself: putting it in `headers` as well would
        // send it twice. A GET or DELETE carries it as a plain header.
        httplib::Result result;
        if (method == "GET") {
            headers.emplace("Content-Type", content_type);
            result = connection->Get(path, headers);
        } else if (method == "POST") {
            result = connection->Post(path, headers, body, content_type);
        } else if (method == "PUT") {
            result = connection->Put(path, headers, body, content_type);
        } else if (method == "DELETE") {
            headers.emplace("Content-Type", content_type);
            result = connection->Delete(path, headers);
        } else {
            throw std::invalid_argument("unsupported HTTP method: " + method);
        }

        if (!result) {
            const auto error = result.error();
            const bool timed_out = error == httplib::Error::ConnectionTimeout;
            throw NetworkError(method + " " + base_url + path + ": " + httplib::to_string(error),
                               timed_out);
        }

        Response response;
        response.status = result->status;
        response.body = std::move(result->body);
        response.content_type = result->get_header_value("Content-Type");
        if (response.status == 429) {
            response.retry_after_seconds = parse_retry_after(result->get_header_value("Retry-After"));
        }
        // The answer was read whole, so the connection is clean: keep it,
        // unless the server said it is closing it.
        if (result->get_header_value("Connection") != "close") {
            release(base_url, std::move(connection));
        }
        return response;
    }

    /** An HTTP status of 400 or more as the exception callers see. */
    [[noreturn]] static void throw_status(const Response& response) {
        std::string message = "HTTP " + std::to_string(response.status);
        std::string code;
        try {
            if (!response.body.empty()) {
                json error_body = json::parse(response.body);
                if (error_body.is_object()) {
                    if (error_body.contains("error") && error_body["error"].is_string()) {
                        message = error_body["error"].get<std::string>();
                    }
                    if (error_body.contains("code") && error_body["code"].is_string()) {
                        code = error_body["code"].get<std::string>();
                    }
                }
            }
        } catch (...) {
            // Not JSON: the status line is the message.
        }
        throw HttpError(response.status, message, response.body, code,
                        response.retry_after_seconds);
    }

    struct Retry429Policy {
        int max_attempts;  // 0 = unbounded
        int base_millis;
        int cap_millis;
    };

    Retry429Policy retry429_policy_for(RetryKind retry_kind) const {
        Retry429Policy policy;
        policy.base_millis = config_.retry_429.base_millis > 0 ? config_.retry_429.base_millis : 500;
        policy.cap_millis = config_.retry_429.cap_millis > 0 ? config_.retry_429.cap_millis : 30000;
        if (config_.retry_429.max_attempts > 0) {
            policy.max_attempts = config_.retry_429.max_attempts;  // applies to both kinds
        } else {
            policy.max_attempts = (retry_kind == RetryKind::Pop) ? 0 : 10;
        }
        return policy;
    }

    /**
     * One logical request against one backend, waiting out HTTP 429 with
     * backoff until the policy for the request's kind is spent (never, for a
     * long-poll pop). Throws HttpError for any status of 400 or more, and
     * NetworkError for a transport fault.
     */
    Response attempt_with_429(const std::string& base_url, const std::string& method,
                              const std::string& path, const std::string& body,
                              const char* content_type, const RequestOptions& options) {
        const Retry429Policy policy = retry429_policy_for(options.retry_kind);
        int tries = 0;
        for (;;) {
            ++tries;
            Response response = execute_once(base_url, method, path, body, content_type,
                                             options.timeout_millis);
            if (response.status != 429) {
                if (response.status >= 400) throw_status(response);
                return response;
            }
            if (policy.max_attempts > 0 && tries >= policy.max_attempts) {
                util::log_error("HttpClient.retry429", method + " " + path +
                    " max 429 attempts exhausted after " + std::to_string(tries));
                throw_status(response);
            }
            const int delay = util::compute_retry429_delay_millis(
                tries - 1, response.retry_after_seconds, policy.base_millis, policy.cap_millis);
            util::log_warn("HttpClient.retry429", method + " " + path + " attempt " +
                std::to_string(tries) + ", retrying in " + std::to_string(delay) + "ms");
            std::this_thread::sleep_for(std::chrono::milliseconds(delay));
        }
    }

    /** One backend, retry_attempts tries with exponential backoff on 5xx and transport faults. */
    Response attempt_with_retry(const std::string& base_url, const std::string& method,
                                const std::string& path, const std::string& body,
                                const char* content_type, const RequestOptions& options) {
        const int attempts = std::max(1, config_.retry_attempts);
        for (int attempt = 0;; ++attempt) {
            try {
                return attempt_with_429(base_url, method, path, body, content_type, options);
            } catch (const HttpError& e) {
                if (!e.is_retryable() || e.status_code() == 429 || attempt + 1 >= attempts) throw;
            } catch (const NetworkError&) {
                if (attempt + 1 >= attempts) throw;
            }
            const long long delay = static_cast<long long>(config_.retry_delay_millis) << attempt;
            util::log_warn("HttpClient.retry", method + " " + path + " attempt " +
                std::to_string(attempt + 1) + " failed, retrying in " + std::to_string(delay) + "ms");
            std::this_thread::sleep_for(std::chrono::milliseconds(delay));
        }
    }

    /** Failover across backends, retry within one, 429 in place. */
    Response send(const std::string& method, const std::string& path, const std::string& body,
                  const char* content_type, const RequestOptions& options) {
        const size_t n = load_balancer_->urls().size();
        const bool use_failover = config_.enable_failover && n > 1 && !options.no_retry;
        const size_t rounds = use_failover ? n : 1;

        std::vector<size_t> attempted;
        std::exception_ptr last;
        for (size_t round = 0; round < rounds; ++round) {
            const size_t backend = load_balancer_->pick(options.affinity_key);
            if (use_failover &&
                std::find(attempted.begin(), attempted.end(), backend) != attempted.end()) {
                continue;
            }
            attempted.push_back(backend);
            const std::string& base_url = load_balancer_->urls()[backend];
            try {
                // Failover already moves elsewhere, so one attempt per backend.
                Response response = (use_failover || options.no_retry)
                    ? attempt_with_429(base_url, method, path, body, content_type, options)
                    : attempt_with_retry(base_url, method, path, body, content_type, options);
                load_balancer_->mark_healthy(backend);
                return response;
            } catch (const HttpError& e) {
                // A 4xx (an exhausted 429 included) is the caller's, or the
                // backend answering: not a health signal, and not retried.
                if (e.status_code() < 500) throw;
                load_balancer_->mark_unhealthy(backend);
                last = std::current_exception();
            } catch (const NetworkError&) {
                load_balancer_->mark_unhealthy(backend);
                last = std::current_exception();
            }
        }
        if (last) std::rethrow_exception(last);
        throw NetworkError("no backend available");
    }

    static json parse_body(const Response& response) {
        if (response.status == 204 || response.body.empty()) return nullptr;
        try {
            return json::parse(response.body);
        } catch (const json::exception& e) {
            throw Error(std::string("the broker's answer is not JSON: ") + e.what());
        }
    }

    static std::string body_text(const json& body) {
        if (body.is_binary()) {
            const auto& bytes = body.get_binary();
            return std::string(bytes.begin(), bytes.end());
        }
        return body.is_null() ? std::string() : body.dump();
    }

    static const char* content_type_of(const json& body) {
        // A binary value is a body already encoded (the binary push and the
        // binary transaction body), sent as it is.
        return body.is_binary() ? wire::PUSH_BINARY_CONTENT_TYPE : "application/json";
    }

    void init() {
        if (config_.urls.empty()) {
            throw std::invalid_argument("at least one broker URL is required");
        }
        for (auto& url : config_.urls) {
            while (!url.empty() && url.back() == '/') url.pop_back();
            util::parse_url(url);  // throws std::invalid_argument for a malformed URL
#ifndef CPPHTTPLIB_OPENSSL_SUPPORT
            if (is_https(url)) {
                throw std::invalid_argument(
                    "an https:// URL needs TLS, and this build has none: include "
                    "queen_client.hpp without QUEEN_CLIENT_NO_TLS and link OpenSSL");
            }
#endif
        }
        load_balancer_ = std::make_shared<LoadBalancer>(
            config_.urls, config_.load_balancing, config_.affinity_hash_ring,
            config_.health_retry_after_millis);

        base_headers_.emplace("User-Agent", std::string("queen-client-cpp/") + QUEEN_CLIENT_VERSION);
        if (!config_.bearer_token.empty()) {
            base_headers_.emplace("Authorization", "Bearer " + config_.bearer_token);
        }
        for (const auto& header : config_.headers) {
            base_headers_.emplace(header.first, header.second);
        }
        set_wire_forms(config_.binary_push, config_.push_compression, config_.framed_pop);
    }

public:
    explicit HttpClient(const ClientConfig& config) : config_(config) {
        init();
    }

    /** One broker, the common case; the knobs a test or a script reaches for. */
    HttpClient(const std::string& base_url, int timeout_millis = 30000, int retry_attempts = 3,
               int retry_delay_millis = 1000, const std::string& bearer_token = "",
               const Retry429Options& retry_429 = Retry429Options()) {
        config_.urls = {base_url};
        config_.timeout_millis = timeout_millis;
        config_.retry_attempts = retry_attempts;
        config_.retry_delay_millis = retry_delay_millis;
        config_.bearer_token = bearer_token;
        config_.retry_429 = retry_429;
        init();
    }

    HttpClient(const HttpClient&) = delete;
    HttpClient& operator=(const HttpClient&) = delete;

    const ClientConfig& config() const { return config_; }

    /**
     * Turn on the binary push body and the framed pop answer. Throws when an
     * option needs zstd and this build has none: better at construction than
     * at the first compressed record.
     */
    void set_wire_forms(bool binary_push, bool push_compression, bool framed_pop) {
        if (((binary_push && push_compression) || framed_pop) && !wire::zstd_available()) {
            throw std::invalid_argument(
                "push_compression and framed_pop need zstd: compile with -DQUEEN_CLIENT_ZSTD and link -lzstd");
        }
        binary_push_ = binary_push;
        push_compression_ = push_compression;
        framed_pop_ = framed_pop;
    }

    /** The answer of any method, any status, with the retries above. Throws for a status >= 400. */
    Response request(const std::string& method, const std::string& path,
                     const std::string& body = std::string(),
                     const char* content_type = "application/json",
                     const RequestOptions& options = RequestOptions()) {
        return send(method, path, body, content_type, options);
    }

    json get(const std::string& path, const RequestOptions& options) {
        // A pop route asks for the stored records when framed_pop is on.
        const bool framed = framed_pop_ && path.rfind("/api/v1/pop", 0) == 0;
        const std::string target = framed
            ? path + (path.find('?') == std::string::npos ? "?" : "&") + "frames=true"
            : path;
        Response response = send("GET", target, std::string(), "application/json", options);
        if (response.status == 204 || response.body.empty()) return nullptr;
        // The framed answer, unpacked into the json the JSON answer parses to,
        // so its readers do not tell the two apart. A broker that does not know
        // the parameter answered JSON.
        if (framed && wire::is_pop_frames(response.body)) {
            return wire::decode_pop_frames(response.body);
        }
        return parse_body(response);
    }

    /**
     * retry_kind: RetryKind::Pop for a long-poll pop (unbounded 429 backoff);
     * the default for everything else (10 attempts).
     */
    json get(const std::string& path, int request_timeout_millis = 0,
             RetryKind retry_kind = RetryKind::Default) {
        RequestOptions options;
        options.timeout_millis = request_timeout_millis;
        options.retry_kind = retry_kind;
        return get(path, options);
    }

    /** A GET whose answer is text (GET /metrics). */
    std::string get_text(const std::string& path, const RequestOptions& options = RequestOptions()) {
        return send("GET", path, std::string(), "application/json", options).body;
    }

    json post(const std::string& path, const json& body, const RequestOptions& options) {
        if (binary_push_ && path == "/api/v1/push" && body.is_object() && body.contains("items")) {
            // Encoded once, before any retry: every attempt sends the same
            // bytes (with push_compression, the same message ids). Nothing =
            // an item the binary body does not carry, and the push stays JSON.
            if (auto encoded = wire::binary_push_body(body["items"], push_compression_)) {
                return parse_body(send("POST", path, *encoded, wire::PUSH_BINARY_CONTENT_TYPE, options));
            }
        }
        if (binary_push_ && push_compression_ && path == "/api/v1/transaction") {
            // The pushes of a bundle as frames built and compressed here, the
            // rest of it as the JSON head in front of them. Nothing = no push,
            // or one the binary body does not carry: the bundle stays JSON.
            if (auto framed = wire::transaction_frames(body)) {
                json answer = parse_body(
                    send("POST", path, framed->body, wire::PUSH_BINARY_CONTENT_TYPE, options));
                // The broker answers the head's operations before the pushes:
                // the caller gets the results in the order of its own operations.
                wire::restore_result_order(answer, framed->order);
                return answer;
            }
        }
        return parse_body(send("POST", path, body_text(body), content_type_of(body), options));
    }

    json post(const std::string& path, const json& body = nullptr, int request_timeout_millis = 0,
              RetryKind retry_kind = RetryKind::Default) {
        RequestOptions options;
        options.timeout_millis = request_timeout_millis;
        options.retry_kind = retry_kind;
        return post(path, body, options);
    }

    json put(const std::string& path, const json& body, const RequestOptions& options) {
        return parse_body(send("PUT", path, body_text(body), content_type_of(body), options));
    }

    json put(const std::string& path, const json& body = nullptr, int request_timeout_millis = 0,
             RetryKind retry_kind = RetryKind::Default) {
        RequestOptions options;
        options.timeout_millis = request_timeout_millis;
        options.retry_kind = retry_kind;
        return put(path, body, options);
    }

    json del(const std::string& path, const RequestOptions& options) {
        return parse_body(send("DELETE", path, std::string(), "application/json", options));
    }

    json del(const std::string& path, int request_timeout_millis = 0,
             RetryKind retry_kind = RetryKind::Default) {
        RequestOptions options;
        options.timeout_millis = request_timeout_millis;
        options.retry_kind = retry_kind;
        return del(path, options);
    }

    /**
     * One best-effort publication (the consumer status reports): one attempt,
     * two seconds at most, no 429 or failover retries, on a connection of its
     * own so its time limit never sticks to a pooled one.
     */
    json post_observation(const std::string& path, const json& body) {
        const std::string& base_url = load_balancer_->urls()[load_balancer_->pick()];
        auto connection = make_connection(base_url);
        connection->set_connection_timeout(std::chrono::milliseconds(2000));
        connection->set_read_timeout(std::chrono::milliseconds(2000));
        connection->set_write_timeout(std::chrono::milliseconds(2000));
        connection->set_max_timeout(std::chrono::milliseconds(2000));
        connection->set_keep_alive(false);
        httplib::Headers headers = base_headers_;
        auto result = connection->Post(path, headers, body.dump(), "application/json");
        if (!result) {
            throw NetworkError("POST " + base_url + path + ": " + httplib::to_string(result.error()));
        }
        Response response;
        response.status = result->status;
        response.body = result->body;
        if (response.status >= 400) throw_status(response);
        return parse_body(response);
    }

    std::shared_ptr<LoadBalancer> get_load_balancer() const { return load_balancer_; }

    /** Idle pooled connections, over every backend. */
    size_t idle_connections() const {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        size_t total = 0;
        for (const auto& entry : idle_) total += entry.second.size();
        return total;
    }

    /** Close every idle connection. */
    void close_idle_connections() {
        std::unordered_map<std::string, std::vector<Idle>> doomed;
        {
            std::lock_guard<std::mutex> lock(pool_mutex_);
            doomed.swap(idle_);
        }
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
    // The batch the flusher is sending, out of messages_ while its request runs.
    size_t in_flight_ = 0;
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

            in_flight_ = batch_size;
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
            in_flight_ = 0;

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
                // A flush request armed while this batch was in flight is spent
                // too: honouring it would start the next drain at once, with no
                // pause, against a broker that is failing.
                flush_requested_ = false;
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
            throw Error("Queen buffer " + queue_address_ +
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
            // on the counter instead of on catching the flusher idle matters --
            // the flusher can hold the mutex from one drain straight into the
            // next, so "idle" may never be observable from here. What is left
            // counts the batch in flight: a failed one goes back to the front.
            const bool gave_up = give_up_count_ > give_ups_before;
            if (gave_up && Clock::now() >= deadline) {
                const size_t left = messages_.size() + in_flight_;
                if (left > 0) {
                    const int count = static_cast<int>(left);
                    const std::string reason =
                        last_flush_error_.empty() ? "buffer flush deadline expired"
                                                  : last_flush_error_;
                    throw BufferFlushError(reason + " (" + std::to_string(count) +
                        " message(s) still buffered for " + queue_address_ + ", not sent)",
                        queue_address_, count);
                }
            }
            // Arm a drain -- but not again once a give-up has happened past
            // this caller's deadline: an expired deadline re-armed makes the
            // flusher give up at its first failure with no pause, and around
            // again, a storm of requests against a broker that is failing.
            // Tighten, not replace: the shortest patience of any concurrent
            // flush caller wins.
            if (!gave_up || Clock::now() < deadline) {
                if (deadline < flush_deadline_) {
                    flush_deadline_ = deadline;
                }
                flush_requested_ = true;
                flusher_wake_.notify_all();
            }
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

// One optional observer per consume invocation. A reporter owns no worker process.
class ConsumerSupervision {
    using Clock = std::chrono::steady_clock;
    std::shared_ptr<HttpClient> http_;
    ConsumeOptions options_;
    std::string id_;
    Clock::time_point started_ = Clock::now();
    int64_t started_epoch_;
    mutable std::mutex mutex_;
    std::condition_variable wake_;
    std::thread publisher_;
    bool stopped_ = false;
    int running_ = 0;
    uint64_t sequence_ = 0, completed_ = 0, failed_ = 0;
    json last_ = nullptr;
    std::map<uint64_t, Clock::time_point> active_;
    static int64_t epoch() {
        return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    }
    static std::string identifier() {
        auto value = util::generate_uuid_v7();
        value.erase(std::remove(value.begin(), value.end(), '-'), value.end());
        return value;
    }
    static json scope(const std::string& value) { return value.empty() ? json(nullptr) : json(value); }
    void publish(const char* state) {
        const auto raw = document(state).dump();
        if (raw.size() > 45000) throw Error("consumer status exceeds one chunk");
        const auto write = identifier(), slot = options_.supervision->group + "/" + id_;
        json operations = json::array({
            {{"op","put"},{"ns","queen-supervisor"},{"key",slot+"/head"},{"ttlSeconds",60},
                {"value",{{"format","queen.supervisor.remote-status/v1"},{"write",write},{"chunks",1},{"bytes",raw.size()}}}},
            {{"op","put"},{"ns","queen-supervisor"},{"key",slot+"/chunk/0000"},{"ttlSeconds",60},
                {"value",{{"write",write},{"index",0},{"data",util::base64_encode(raw)}}}}
        });
        auto response = http_->post_observation("/api/v1/kv", {{"operations",operations}});
        if (!response.contains("results") || !response["results"].is_array() || response["results"].size() != 2)
            throw Error("consumer status publication was not applied");
        for (const auto& result : response["results"]) if (result.value("applied", false) != true)
            throw Error("consumer status publication was not applied");
    }
    void run() noexcept {
        bool failing = false;
        auto send = [&](const char* state) {
            try { publish(state); failing = false; }
            catch (...) {
                if (!failing) { try { util::log_error("Consumer.supervision", "Status publication failed; consumption continues"); } catch (...) {} }
                failing = true;
            }
        };
        send("running");
        std::unique_lock<std::mutex> lock(mutex_);
        while (!wake_.wait_for(lock, std::chrono::seconds(10), [&] { return stopped_; })) {
            lock.unlock(); send("running"); lock.lock();
        }
        lock.unlock(); send("stopped");
    }
public:
    ConsumerSupervision(std::shared_ptr<HttpClient> http, const ConsumeOptions& options)
        : http_(std::move(http)), options_(options), id_(identifier()), started_epoch_(epoch()) {
        const auto& group = options_.supervision->group;
        if (!std::regex_match(group, std::regex("[A-Za-z0-9][A-Za-z0-9._-]{0,254}")) || group == "coordination")
            throw std::invalid_argument("supervision group must be a valid application/deployment name");
        if (options_.concurrency < 1 || options_.concurrency > 4096)
            throw std::invalid_argument("supervision requires concurrency between 1 and 4096");
    }
    ~ConsumerSupervision() { stop(); }
    void start() { publisher_ = std::thread([this] { run(); }); }
    void stop() {
        { std::lock_guard<std::mutex> lock(mutex_); stopped_ = true; }
        wake_.notify_one();
        if (publisher_.joinable()) publisher_.join();
    }
    void worker_started() { std::lock_guard<std::mutex> lock(mutex_); ++running_; }
    void worker_exited() { std::lock_guard<std::mutex> lock(mutex_); --running_; }
    uint64_t begin_handler() {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto id = sequence_++; active_[id] = Clock::now(); return id;
    }
    void end_handler(uint64_t id, bool success) {
        std::lock_guard<std::mutex> lock(mutex_);
        active_.erase(id); last_ = epoch(); if (success) ++completed_; else ++failed_;
    }
    json document(const char* state) const {
        std::lock_guard<std::mutex> lock(mutex_);
        json oldest = nullptr;
        for (const auto& entry : active_) {
            auto age = std::chrono::duration_cast<std::chrono::seconds>(Clock::now()-entry.second).count();
            if (oldest.is_null() || age > oldest.get<int64_t>()) oldest = age;
        }
        const char* host = std::getenv("HOSTNAME");
        if (!host) host = std::getenv("COMPUTERNAME");
#ifdef _WIN32
        const auto pid = _getpid();
#else
        const auto pid = getpid();
#endif
        return {{"schema","queen.consumer.status/v1"},{"instance_id",id_},{"engine","cpp"},{"execution_model","threads"},
            {"hostname",host ? json(host) : json(nullptr)},{"pid",pid},{"state",state},{"updated_at_epoch",epoch()},
            {"started_at_epoch",started_epoch_},{"uptime_seconds",std::chrono::duration_cast<std::chrono::seconds>(Clock::now()-started_).count()},
            {"configuration",{{"heartbeat_timeout",30}}},
            {"pool_status",json::array({{{"name","consumer"},{"queue",scope(options_.queue)},{"namespace",scope(options_.namespace_name)},
                {"task",scope(options_.task)},{"consumer_group",options_.group.empty() ? "__QUEUE_MODE__" : options_.group},
                {"desired",options_.concurrency},{"running",running_},{"busy",active_.size()},{"completed",completed_},{"failed",failed_},
                {"last_completed_at_epoch",last_},{"oldest_inflight_seconds",oldest}}})}};
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
                throw Error(
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
// Locks - a lock and a semaphore, as leases with a fencing token
//
// WHAT IT IS. A permit is one KV row in the namespace `queen-locks`, written
// with a lifetime: acquire is a putIfAbsent, renew a put with expect, release a
// delete with expect. The broker's POST /api/v1/locks does that turning, so
// every client shares one implementation. A lock is the semaphore of one
// permit.
//
// WHAT IT IS NOT: A MUTEX. A permit EXPIRES, and nobody tells its holder. A
// process that is paused, partitioned or slow keeps running past its lifetime
// while somebody else acquires. So the lock alone never makes two holders
// impossible; what makes their WORK exclusive is the token:
//
//   * inside Queen, TransactionBuilder::guard(lock): the acks, pushes, KV
//     writes and timers of a step commit only if the permit is still this
//     holder's, in the same log entry. A holder that was replaced commits
//     nothing.
//   * outside Queen, Lock::token(): a number that only rises on a lock. A
//     resource that remembers the highest token it accepted and refuses a lower
//     one refuses the holder that was replaced. Accept an EQUAL one: a holder
//     writes many times with one token.
//
// STATUS CODES ARE NOT VERDICTS, as on the KV surface: a lock somebody else
// holds, a token that is no longer the row's, a release of what was not held
// are HTTP 200 with `acquired`, `renewed` or `released` false. An HttpError
// from here means the CALL did not happen.
//
// A RENEW ANSWERS A NEW TOKEN. It rewrites the row, so the token before it
// stops working -- for the guard, for the next renew, for the release.
// ============================================================================

/** The KV namespace the permits live in. */
inline const char* const LOCK_NAMESPACE = "queen-locks";

/**
 * How a Lock behaves. The lifetime is not here: it is mandatory and is an
 * argument of Queen::lock.
 */
struct LockOptions {
    /// The holder's identity, instead of the one minted per handle. Two handles
    /// with one owner are one holder.
    std::string owner;
    /// How a waiting acquire comes back: first after retry_min_millis, then
    /// half as long again each time up to retry_max_millis, with jitter.
    int retry_min_millis = 100;
    int retry_max_millis = 1000;
    /// Renew in the background while the permit is held, as the JS, Python,
    /// Go and Rust handles do. false = renew yourself: keep_alive() at the
    /// checkpoints of the work, or renew().
    bool auto_renew = true;
    /// The background renew period; 0 = a third of the lifetime.
    int renew_every_millis = 0;
};

/**
 * The four lock operations as the broker speaks them, with no state kept: the
 * caller carries the token. Queen::lock is what most code wants; this is
 * for `get` (who holds it?) and for a caller with its own loop.
 */
class LocksBuilder {
private:
    std::shared_ptr<HttpClient> http_client_;

public:
    explicit LocksBuilder(std::shared_ptr<HttpClient> http_client)
        : http_client_(std::move(http_client)) {
    }

    /**
     * Several operations in one call, each on a different lock (build them with
     * the wire::lock_*_op functions). One result per operation, in order; they
     * are independent, nothing here is all-or-nothing.
     */
    json batch(const json& operations, const RequestOptions& options = RequestOptions()) {
        if (!operations.is_array() || operations.empty()) {
            throw std::invalid_argument("a locks call needs at least one operation");
        }
        json response =
            http_client_->post("/api/v1/locks", json{{"operations", operations}}, options);
        if (!response.is_object() || !response.contains("results") ||
            !response["results"].is_array() ||
            response["results"].size() != operations.size()) {
            throw Error(
                "locks: the broker did not answer one result per operation; this is an "
                "alignment failure, not a verdict");
        }
        return response["results"];
    }

    /**
     * Take a permit: {acquired, slot, token, owner, guard, already?}, or
     * {acquired:false, reason:"held"|"contended", holders}. `limit` above 1
     * makes it a semaphore; every caller of one name passes the same limit,
     * which is stored nowhere.
     */
    json acquire(const std::string& name, long long ttl_seconds,
                 const std::string& owner = "", int limit = 1) {
        return batch(json::array({wire::lock_acquire_op(name, ttl_seconds, owner, limit)}))[0];
    }

    /** Extend a permit: {renewed, slot, token, guard} with a NEW token, or
     *  {renewed:false, reason:"lost", holders}. */
    json renew(const std::string& name, long long token, long long ttl_seconds, int slot = 0,
               const std::string& owner = "", const RequestOptions& options = RequestOptions()) {
        return batch(json::array({wire::lock_renew_op(name, token, ttl_seconds, slot, owner)}),
                     options)[0];
    }

    /** The client's per-request timeout, in milliseconds. */
    int timeout_millis() const { return http_client_->config().timeout_millis; }

    /** Give a permit back: {released}; false with reason "lost" when the token
     *  is no longer the row's. */
    json release(const std::string& name, long long token, int slot = 0) {
        return batch(json::array({wire::lock_release_op(name, token, slot)}))[0];
    }

    /** Who holds it: {held, holders:[{slot, owner, token, since, expiresAt, renewedAt}]}. */
    json get(const std::string& name) {
        return batch(json::array({wire::lock_get_op(name)}))[0];
    }
};

/** Thrown where something needed a held lock and the handle holds none. */
class LockNotHeldError : public Error {
public:
    explicit LockNotHeldError(const std::string& message) : Error(message) {
    }
};

/**
 * One holder's hold on one lock, or on one permit of a semaphore.
 *
 *     auto lock = queen.lock("daily-report", 30);
 *     if (!lock.acquire()) return;                  // somebody else has it
 *     queen.transaction()
 *         .guard(lock)                              // commits only while it is ours
 *         .queue("reports").push(report)
 *         .commit();
 *     lock.release();
 *
 * IT RENEWS ITSELF. While the permit is held a background thread renews it
 * every third of the lifetime (LockOptions::auto_renew, on by default), and
 * is_lost() / wait_lost() say when it is gone: the broker said so, or the
 * lifetime passed on this machine's clock with no renew having succeeded -- a
 * client that cannot reach the broker has to assume the worst. With
 * auto_renew off, call keep_alive() at the checkpoints of the work instead.
 *
 * THE TOKEN CHANGES AT EVERY RENEW. The handle keeps the current one: read
 * token() and guard() when you use them. TransactionBuilder::guard reads them
 * when the bundle is sent, and sends the bundle again when it lost only to
 * this handle's own renewal.
 *
 * THE OWNER is the holder's identity, minted per handle. With it a call is safe
 * to send again when its answer was lost: the broker answers the permit the
 * first attempt took.
 *
 * Movable, not copyable; the background renewal ends with the handle (the
 * permit then expires by itself). Its methods may be called from any thread.
 */
class Lock {
private:
    using Clock = std::chrono::steady_clock;

    struct State {
        LocksBuilder locks;
        std::string name;
        long long ttl_seconds;
        int limit;
        LockOptions options;
        std::string owner;

        mutable std::mutex mutex;
        std::condition_variable wake;      // the renewer's sleep; wait_lost()
        std::condition_variable settled;   // a renew in flight finished
        long long token = 0;
        int slot = -1;
        // The broker's own guard of the current lease period, kept as answered:
        // where a permit's row lives is the broker's rule, written once, there.
        json guard;
        Clock::time_point valid_until{};
        Clock::time_point renewed_at{};
        bool lost = false;
        bool renewing = false;
        bool stopping = false;
        // Bumped at every acquire, release and loss, so a renewer or a renew
        // in flight knows when its permit is no longer the handle's.
        uint64_t epoch = 0;
        std::thread renewer;

        State(std::shared_ptr<HttpClient> http, std::string n, long long ttl, int lim,
              LockOptions opts, std::string own)
            : locks(std::move(http)), name(std::move(n)), ttl_seconds(ttl), limit(lim),
              options(std::move(opts)), owner(std::move(own)) {
        }
    };

    std::unique_ptr<State> s_;

    static std::string mint_owner() {
        // Same sources as the consumer's status report: no socket header for a
        // host name, and the two spellings of the process id.
        const char* env = std::getenv("HOSTNAME");
        if (!env) env = std::getenv("COMPUTERNAME");
        std::string host = env ? std::string(env).substr(0, 128) : std::string("host");
#ifdef _WIN32
        const auto pid = _getpid();
#else
        const auto pid = getpid();
#endif
        std::random_device rd;
        std::ostringstream out;
        out << host << ":" << pid << ":" << std::hex << std::setfill('0')
            << std::setw(6) << (rd() & 0xffffff) << std::setw(6) << (rd() & 0xffffff);
        return out.str();
    }

    // Called with the mutex held.
    static void take(State& s, const json& result, Clock::time_point sent_at) {
        s.token = result.at("token").get<long long>();
        s.slot = result.at("slot").get<int>();
        s.guard = result.at("guard");
        // Counted from when the request was SENT, so it is never later than the
        // broker's own deadline.
        s.valid_until = sent_at + std::chrono::seconds(s.ttl_seconds);
        s.renewed_at = sent_at;
        s.lost = false;
    }

    // Called with the mutex held. `lost`: the permit went away under the
    // holder (as opposed to a release).
    static void drop(State& s, bool lost) {
        const bool had = s.token != 0;
        s.token = 0;
        s.slot = -1;
        s.guard = nullptr;
        s.valid_until = Clock::time_point{};
        ++s.epoch;
        if (lost && had) s.lost = true;
        s.wake.notify_all();
    }

    static Clock::duration renew_period(const State& s) {
        if (s.options.renew_every_millis > 0) {
            return std::chrono::milliseconds(s.options.renew_every_millis);
        }
        return std::chrono::milliseconds(std::max<long long>(s.ttl_seconds, 1) * 1000 / 3);
    }

    /**
     * The background of one hold. It calls the permit lost when its lifetime
     * runs out on this machine's clock, and with auto_renew it renews every
     * period before that. A renew is one request, cut off at the permit's
     * deadline: a broker that does not answer cannot hide the loss. A manual
     * renew (renew(), keep_alive()) moves the schedule.
     */
    static void renew_loop(State* s, uint64_t epoch) {
        try {
            std::unique_lock<std::mutex> lock(s->mutex);
            std::optional<Clock::time_point> retry_at;  // after a renew that could not be made
            while (!s->stopping && s->epoch == epoch && s->token != 0) {
                const bool automatic = s->options.auto_renew;
                const Clock::time_point renewed_before = s->renewed_at;
                const Clock::time_point due = retry_at ? *retry_at : s->renewed_at + renew_period(*s);
                const Clock::time_point wake_at = automatic ? std::min(due, s->valid_until)
                                                            : s->valid_until;
                s->wake.wait_until(lock, wake_at, [&] {
                    return s->stopping || s->epoch != epoch || s->renewed_at != renewed_before;
                });
                if (s->stopping || s->epoch != epoch) return;
                if (s->renewed_at != renewed_before) {
                    retry_at.reset();  // renewed elsewhere: a new schedule
                    continue;
                }
                const auto now = Clock::now();
                if (now >= s->valid_until) {
                    util::log_warn("Lock.renew", "lock '" + s->name + "': lifetime ran out with no renew");
                    drop(*s, true);
                    return;
                }
                if (!automatic || now < due) continue;

                const long long token = s->token;
                const int slot = s->slot;
                RequestOptions once;
                once.no_retry = true;
                once.timeout_millis = static_cast<int>(std::max<long long>(1, std::min<long long>(
                    s->locks.timeout_millis(),
                    std::chrono::duration_cast<std::chrono::milliseconds>(s->valid_until - now).count())));
                s->renewing = true;
                lock.unlock();
                const auto sent_at = Clock::now();
                json result;
                std::string failure;
                try {
                    result = s->locks.renew(s->name, token, s->ttl_seconds, slot, s->owner, once);
                } catch (const std::exception& e) {
                    failure = e.what();
                }
                lock.lock();
                s->renewing = false;
                s->settled.notify_all();
                if (s->stopping || s->epoch != epoch) return;
                if (failure.empty()) {
                    if (!result.value("renewed", false)) {
                        util::log_warn("Lock.renew", "lock '" + s->name + "' is lost: " +
                                       result.value("reason", std::string("not renewed")));
                        drop(*s, true);
                        return;
                    }
                    take(*s, result, sent_at);
                    retry_at.reset();
                } else {
                    // The broker could not be asked: neither renewed nor known
                    // lost. Try again soon, until the lifetime decides.
                    util::log_warn("Lock.renew", "lock '" + s->name + "': renew failed: " + failure);
                    retry_at = Clock::now() + std::min<Clock::duration>(std::chrono::seconds(1),
                                                                         renew_period(*s));
                }
            }
        } catch (...) {
            // Nothing may leave a std::thread; the lifetime still decides.
        }
    }

    // Called with the mutex released.
    void join_renewer() {
        if (s_ && s_->renewer.joinable() && s_->renewer.get_id() != std::this_thread::get_id()) {
            s_->renewer.join();
        }
    }

    void shutdown() {
        if (!s_) return;
        {
            std::lock_guard<std::mutex> lock(s_->mutex);
            s_->stopping = true;
        }
        s_->wake.notify_all();
        join_renewer();
    }

    State& state() const {
        if (!s_) throw std::logic_error("this Lock was moved from");
        return *s_;
    }

public:
    Lock(std::shared_ptr<HttpClient> http_client, const std::string& name, long long ttl_seconds,
         int limit, const LockOptions& options) {
        const std::string owner = options.owner.empty() ? mint_owner() : options.owner;
        // Validated here, so a bad name or lifetime surfaces where the handle
        // is made and not at the first acquire.
        (void)wire::lock_acquire_op(name, ttl_seconds, owner, limit);
        s_ = std::make_unique<State>(std::move(http_client), name, ttl_seconds, limit, options, owner);
    }

    Lock(Lock&& other) noexcept = default;
    Lock& operator=(Lock&& other) noexcept {
        if (this != &other) {
            shutdown();
            s_ = std::move(other.s_);
        }
        return *this;
    }
    Lock(const Lock&) = delete;
    Lock& operator=(const Lock&) = delete;
    ~Lock() { shutdown(); }

    const std::string& name() const { return state().name; }
    const std::string& owner() const { return state().owner; }
    int limit() const { return state().limit; }

    /**
     * Whether this handle holds a permit, as far as it can know: the broker
     * granted or renewed it, and its lifetime has not run out on this
     * machine's clock. A belief with a deadline, not a proof -- the proof is
     * the guard on the transaction.
     */
    bool held() const {
        State& s = state();
        std::lock_guard<std::mutex> lock(s.mutex);
        return s.token != 0 && Clock::now() < s.valid_until;
    }

    /** The fencing token of the current lease period; 0 when not held. */
    long long token() const {
        State& s = state();
        std::lock_guard<std::mutex> lock(s.mutex);
        return s.token != 0 && Clock::now() < s.valid_until ? s.token : 0;
    }

    /** The semaphore slot this handle holds (0 for a lock); -1 when not held. */
    int slot() const {
        State& s = state();
        std::lock_guard<std::mutex> lock(s.mutex);
        return s.token != 0 && Clock::now() < s.valid_until ? s.slot : -1;
    }

    /**
     * The KV operation that holds while the permit is this handle's: a `check`
     * of the permit's row at the current token, required.
     * TransactionBuilder::guard adds it for you; use this one to put it in a KV
     * batch yourself, at the moment you send.
     */
    json guard() const {
        State& s = state();
        std::lock_guard<std::mutex> lock(s.mutex);
        if (s.token == 0 || Clock::now() >= s.valid_until) {
            throw LockNotHeldError("lock '" + s.name + "' is not held; there is nothing to guard with");
        }
        return s.guard;
    }

    /**
     * Take the permit. With wait_millis it keeps trying until the permit is
     * free or the wait is over, coming back every 100 ms to 1 s with jitter,
     * and BLOCKS for that long. On success the background renewal starts.
     */
    bool acquire(int wait_millis = 0) {
        State& s = state();
        if (held()) return true;
        const auto deadline = Clock::now() + std::chrono::milliseconds(wait_millis);
        double pause = s.options.retry_min_millis;
        std::mt19937 rng{std::random_device{}()};
        std::uniform_real_distribution<double> jitter(0.75, 1.0);
        for (;;) {
            const auto sent_at = Clock::now();
            json result = s.locks.acquire(s.name, s.ttl_seconds, s.owner, s.limit);
            if (result.value("acquired", false)) {
                uint64_t epoch;
                {
                    std::lock_guard<std::mutex> lock(s.mutex);
                    take(s, result, sent_at);
                    epoch = ++s.epoch;
                }
                // The watcher runs on every hold: it renews with auto_renew,
                // and in either mode calls the permit lost at its deadline.
                join_renewer();  // a previous hold's watcher, already ending
                s.renewer = std::thread(&Lock::renew_loop, &s, epoch);
                return true;
            }
            const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - Clock::now()).count();
            if (left <= 0) return false;
            // A random quarter off, so a crowd of waiters spreads.
            const auto nap = static_cast<long long>(pause * jitter(rng));
            std::this_thread::sleep_for(std::chrono::milliseconds(std::min<long long>(nap, left)));
            pause = std::min<double>(pause * 1.5, s.options.retry_max_millis);
        }
    }

    /**
     * Extend the lease now. true with a new token in place, or false: the
     * permit is gone. Throws when the broker could not be asked -- the permit
     * is then neither renewed nor known lost, and its deadline stands.
     */
    bool renew() {
        State& s = state();
        long long token;
        int slot;
        uint64_t epoch;
        {
            std::unique_lock<std::mutex> lock(s.mutex);
            s.settled.wait(lock, [&] { return !s.renewing; });
            if (s.token == 0) return false;
            token = s.token;
            slot = s.slot;
            epoch = s.epoch;
            s.renewing = true;
        }
        const auto sent_at = Clock::now();
        json result;
        try {
            result = s.locks.renew(s.name, token, s.ttl_seconds, slot, s.owner);
        } catch (...) {
            std::lock_guard<std::mutex> lock(s.mutex);
            s.renewing = false;
            s.settled.notify_all();
            throw;
        }
        std::lock_guard<std::mutex> lock(s.mutex);
        s.renewing = false;
        s.settled.notify_all();
        if (s.epoch != epoch) return s.token != 0;
        if (!result.value("renewed", false)) {
            drop(s, true);
            return false;
        }
        take(s, result, sent_at);
        s.wake.notify_all();  // the watcher re-reads its schedule from the new renewal
        return true;
    }

    /**
     * The checkpoint of long work when auto_renew is off: renews once a third
     * of the lifetime has passed since the permit was taken or last renewed,
     * and sends nothing before then. With auto_renew on it only reads held().
     * false means stop: the permit expired here, or the broker says it is
     * somebody else's.
     */
    bool keep_alive() {
        State& s = state();
        {
            std::lock_guard<std::mutex> lock(s.mutex);
            if (s.token == 0 || Clock::now() >= s.valid_until) {
                drop(s, true);
                return false;
            }
            if (s.options.auto_renew ||
                Clock::now() - s.renewed_at < std::chrono::milliseconds(s.ttl_seconds * 1000 / 3)) {
                return true;
            }
        }
        return renew();
    }

    /**
     * Give the permit back. true when the broker removed it; false when it was
     * not this handle's any more or was never held. Either way the handle
     * holds nothing afterwards and can acquire again.
     */
    bool release() {
        State& s = state();
        long long token;
        int slot;
        {
            std::unique_lock<std::mutex> lock(s.mutex);
            s.settled.wait(lock, [&] { return !s.renewing; });
            if (s.token == 0) return false;
            token = s.token;
            slot = s.slot;
            drop(s, false);
        }
        join_renewer();
        return s.locks.release(s.name, token, slot).value("released", false);
    }

    /**
     * Whether the permit this handle held went away under it: the broker said
     * so, or its lifetime ran out with no renew. Reset by the next acquire().
     */
    bool is_lost() const {
        State& s = state();
        std::lock_guard<std::mutex> lock(s.mutex);
        return s.lost;
    }

    /** Block until the permit is lost or `timeout` passes; true when it was lost. */
    bool wait_lost(std::chrono::milliseconds timeout) const {
        State& s = state();
        std::unique_lock<std::mutex> lock(s.mutex);
        return s.wake.wait_for(lock, timeout, [&] { return s.lost; });
    }

    /** Wait for a renew in flight to finish, so token() and guard() are settled. */
    void settle() const {
        State& s = state();
        std::unique_lock<std::mutex> lock(s.mutex);
        s.settled.wait(lock, [&] { return !s.renewing; });
    }

    /** The broker said the permit is not this handle's (TransactionBuilder::guard). */
    void mark_lost() {
        State& s = state();
        std::lock_guard<std::mutex> lock(s.mutex);
        drop(s, true);
    }
};

// ============================================================================
// TransactionBuilder - one step, all or nothing
// ============================================================================

/**
 * One message pushed inside a transaction. Unlike a plain push, a `trace_id`
 * set here is stored and comes back at delivery; it must be a UUID. Empty
 * fields are not sent (an empty transaction_id is minted here).
 */
struct TxPushItem {
    json payload;
    std::string transaction_id;
    std::string trace_id;
};

class TransactionBuilder {
private:
    std::shared_ptr<HttpClient> http_client_;
    json operations_ = json::array();
    // The two rider arrays. They are TOP-LEVEL fields of the request body and
    // never elements of `operations` -- see the comment on commit().
    json kv_ops_ = json::array();
    json timer_ops_ = json::array();
    std::vector<std::string> required_leases_;
    // Lock handles whose permit this bundle commits under. Kept as handles, not
    // as operations: the token is read when commit() sends, because a renew
    // between here and there changes it. Each must outlive the commit.
    std::vector<Lock*> guards_;

    void add_ack(const Message& message, AckStatus status, const std::string& reason) {
        if (message.partition_id.empty()) {
            throw std::invalid_argument(
                "cannot ack a message with no partition id: a transactionId alone is not unique "
                "across partitions, so this could commit a cursor for a message you never saw");
        }
        if (message.transaction_id.empty()) {
            throw std::invalid_argument("cannot ack a message with no transaction id");
        }
        json op = {
            {"type", "ack"},
            {"transactionId", message.transaction_id},
            {"partitionId", message.partition_id},
            {"status", to_string(status)}
        };
        // The group comes from the message: the pop that delivered it named it.
        // Queue mode sends none, as it always has.
        json group = util::group_on_wire(message.consumer_group);
        if (!group.is_null()) {
            op["consumerGroup"] = std::move(group);
        }
        if (!message.lease_id.empty()) {
            // Each ack names its own lease. A Queen 2 broker fences an ack with
            // the lease the operation carries, and lends it one from
            // requiredLeases only when the bundle names a single lease: in a
            // bundle spanning two leases, an ack without its own would be
            // applied whoever holds its partition now.
            op["leaseId"] = message.lease_id;
            required_leases_.push_back(message.lease_id);
        }
        if (!reason.empty()) {
            op["error"] = util::nack_error_text(reason);
        }
        operations_.push_back(std::move(op));
    }

    void add_push(const std::string& queue, const std::string& partition,
                  const std::vector<TxPushItem>& items) {
        if (queue.empty()) {
            throw std::invalid_argument("a transaction push needs a queue name");
        }
        json push_items = json::array();
        for (const auto& item : items) {
            // The transactionId is what makes a retried transaction idempotent
            // inside the dedup window, so it has to reach the wire: minted here
            // when absent rather than left to the broker.
            json formatted = {
                {"queue", queue},
                {"payload", item.payload},
                {"transactionId", item.transaction_id.empty() ? util::generate_uuid_v7()
                                                              : item.transaction_id}
            };
            if (!partition.empty()) {
                formatted["partition"] = partition;
            }
            if (!item.trace_id.empty()) {
                if (!util::is_valid_uuid(item.trace_id)) {
                    throw std::invalid_argument(
                        "traceId must be a UUID (the broker drops anything else silently), got '" +
                        item.trace_id + "'");
                }
                formatted["traceId"] = item.trace_id;
            }
            push_items.push_back(std::move(formatted));
        }
        if (push_items.empty()) return;
        operations_.push_back({{"type", "push"}, {"items", std::move(push_items)}});
    }

public:
    /** The pushes of a bundle to one queue: `.queue(q).partition(p).push(payload)`. */
    class QueuePushBuilder {
    private:
        TransactionBuilder* parent_;
        std::string queue_;
        std::string partition_;

    public:
        QueuePushBuilder(TransactionBuilder* parent, const std::string& queue)
            : parent_(parent), queue_(queue) {
        }

        /** The partition the pushes go to (the entity's lane). Unset = "Default". */
        QueuePushBuilder& partition(const std::string& name) {
            partition_ = name;
            return *this;
        }

        /** One message; the payload travels as it is. */
        TransactionBuilder& push(const json& payload) {
            if (payload.is_array()) {
                throw std::invalid_argument(
                    "push() takes ONE payload; for several messages call push_many(), and to send "
                    "an array as one payload wrap it: push_many({payload})");
            }
            parent_->add_push(queue_, partition_, {TxPushItem{payload, "", ""}});
            return *parent_;
        }

        /** One message with its own transaction id (the dedup key). */
        TransactionBuilder& push(const json& payload, const std::string& transaction_id) {
            if (payload.is_array()) {
                throw std::invalid_argument(
                    "push() takes ONE payload; for several messages call push_items()");
            }
            parent_->add_push(queue_, partition_, {TxPushItem{payload, transaction_id, ""}});
            return *parent_;
        }

        /** Several messages, one per payload. */
        TransactionBuilder& push_many(const std::vector<json>& payloads) {
            std::vector<TxPushItem> items;
            items.reserve(payloads.size());
            for (const auto& payload : payloads) items.push_back(TxPushItem{payload, "", ""});
            parent_->add_push(queue_, partition_, items);
            return *parent_;
        }

        /** Messages with their own transaction id (the dedup key) and trace id. */
        TransactionBuilder& push_items(const std::vector<TxPushItem>& items) {
            parent_->add_push(queue_, partition_, items);
            return *parent_;
        }
    };

    /**
     * KV writes and reads that ride this bundle, bound to one namespace.
     *
     * THE POINT OF PUTTING KV HERE INSTEAD OF CALLING queen.kv() SEPARATELY:
     * the ack transaction is the primary fence and `expect` is the secondary
     * assertion. A state write that shares the transaction with the ack is
     * undone when an expired lease makes the ack fail -- something a
     * compare-and-set cannot do, because an `expect` on a version that still
     * matches succeeds even from a zombie consumer.
     *
     * getPrefix is not here: the broker refuses it inside a transaction, its
     * cost not being bounded by the caller.
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

        TransactionBuilder& get_many(const std::vector<std::string>& keys) {
            parent_->kv_ops_.push_back(wire::kv_get_many_op(namespace_, keys));
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

        // `del`, not `delete`: the keyword is taken.
        TransactionBuilder& del(const std::string& key,
                                const KvWriteOptions& options = KvWriteOptions()) {
            parent_->kv_ops_.push_back(wire::kv_delete_op(namespace_, key, options));
            return *parent_;
        }

        /**
         * A precondition on a key the bundle does not write: `version` is what
         * it must be at, or 0 for a key that must not exist. Required by
         * default here, because inside a bundle that is what it is for: the
         * gate. Pass false for a look that holds nothing back.
         */
        TransactionBuilder& check(const std::string& key, long long version,
                                  bool required = true) {
            parent_->kv_ops_.push_back(wire::kv_check_op(namespace_, key, version, required));
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
     * standalone queen.timers().cancel(), whose route is never blockable. Use
     * the standalone one when the cancel must land regardless; this one when
     * the cancel must be atomic with the ack.
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

    explicit TransactionBuilder(std::shared_ptr<HttpClient> http_client)
        : http_client_(std::move(http_client)) {
    }

    /**
     * Settle a delivered message in this bundle. The consumer group and the
     * lease come from the message; `reason` travels as the ack's error (kept
     * on the dead-letter row when a failure files it there).
     */
    TransactionBuilder& ack(const Message& message, AckStatus status = AckStatus::Completed,
                            const std::string& reason = "") {
        add_ack(message, status, reason);
        return *this;
    }

    /** ack(message, AckStatus::Failed, reason): spends a retry. */
    TransactionBuilder& nack(const Message& message, const std::string& reason) {
        add_ack(message, AckStatus::Failed, reason);
        return *this;
    }

    TransactionBuilder& ack_all(const std::vector<Message>& messages,
                                AckStatus status = AckStatus::Completed,
                                const std::string& reason = "") {
        for (const auto& message : messages) add_ack(message, status, reason);
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
     * Commit this bundle only while `lock` is held.
     *
     * The guard is a `check` of the permit's row at the lock's token, required:
     * the broker judges it in the same log entry as the acks, pushes, keys and
     * timers beside it. A holder that was paused past its lifetime and replaced
     * commits NOTHING -- which the lock by itself cannot promise, since nobody
     * stops an expired holder from running.
     *
     * The token is read when commit() sends. A guard that loses is the verdict,
     * returned like any lost required precondition (`reason:"kv_precondition"`),
     * and the handle then holds nothing -- unless it lost only to the handle's
     * own background renewal, which moved the token while the bundle was in
     * flight: the bundle is then sent again with the new token (nothing
     * committed, so it is the same step, not a second one).
     *
     * commit() throws LockNotHeldError when the handle holds nothing: a step
     * that asked for a guard must not go out without one. `lock` must outlive
     * the commit.
     */
    TransactionBuilder& guard(Lock& lock) {
        guards_.push_back(&lock);
        return *this;
    }

    /**
     * Commit the bundle.
     *
     * WHERE THE TWO RIDER ARRAYS GO, AND WHY IT IS NOT NEGOTIABLE. `kv` and
     * `timers` are TOP-LEVEL fields of the request body, beside `operations`
     * and never inside it: two Go struct fields carrying the same JSON key at
     * the same level are BOTH dropped by encoding/json, with no error, so a
     * `kv` leg inside `operations` could go out with zero KV operations while
     * the broker committed without the gate. The arrays are omitted entirely
     * when empty, never sent as `[]`.
     *
     * `results` is a FLAT index space with an append-only layout: the
     * operations first (one slot per pushed item and per ack), then the KV
     * array, then the timers.
     *
     * A lost `required` KV precondition is RETURNED (success false, reason
     * "kv_precondition"), not thrown: it is the expected result of every
     * legitimate redelivery -- the idempotency marker doing its job. Branch on
     * failed_index, kv_reason, version and value; never string-match the
     * message. Every other rollback throws TransactionError, and a refusal by
     * the proxy (403/429) an HttpError.
     */
    TransactionResult commit() {
        if (operations_.empty() && kv_ops_.empty() && timer_ops_.empty() && guards_.empty()) {
            throw std::invalid_argument("a transaction needs at least one operation");
        }

        std::vector<std::string> leases = required_leases_;
        std::sort(leases.begin(), leases.end());
        leases.erase(std::unique(leases.begin(), leases.end()), leases.end());

        // Where the kv rider starts in the flat results space.
        size_t flat_operations = 0;
        for (const auto& op : operations_) {
            flat_operations += (op.value("type", std::string()) == "push" && op.contains("items"))
                                   ? op["items"].size() : 1;
        }

        for (int attempt = 0;; ++attempt) {
            json request = {
                {"operations", operations_},
                {"requiredLeases", leases}
            };
            // The guards go first in `kv`: guard i is op i of the rider. A
            // handle that holds nothing throws before anything is sent.
            json kv = json::array();
            for (Lock* lock : guards_) {
                lock->settle();
                kv.push_back(lock->guard());
            }
            for (const auto& op : kv_ops_) kv.push_back(op);
            if (!kv.empty()) request["kv"] = std::move(kv);
            if (!timer_ops_.empty()) request["timers"] = timer_ops_;

            TransactionResult result =
                TransactionResult::from_json(http_client_->post("/api/v1/transaction", request));

            if (result.success) return result;
            if (!result.lost_precondition()) throw TransactionError(std::move(result));

            // A guard that lost: which lock, and was it only our own renewal?
            Lock* lost_lock = nullptr;
            if (result.failed_index && *result.failed_index >= flat_operations &&
                *result.failed_index - flat_operations < guards_.size()) {
                lost_lock = guards_[*result.failed_index - flat_operations];
            }
            if (!lost_lock) return result;  // the caller's own precondition
            const bool own_renewal = result.kv_reason == "version" && result.value.is_object() &&
                                     util::text_of(result.value, "owner") == lost_lock->owner();
            if (!own_renewal) {
                // Expired, released, or another holder's: the lock is gone.
                lost_lock->mark_lost();
                return result;
            }
            lost_lock->settle();
            if (attempt >= 3 || !lost_lock->held()) return result;
        }
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
// the round trip to the broker, since there is no cache in front of the KV
// and there will not be one (§8.5).
//
// The eight operations: get, getMany, getPrefix, put, putIfAbsent, delete,
// incr and check. getPrefix is refused inside a transaction by design, since
// its cost is not bounded by the caller. There is no `once` helper (client-js
// and client-py have one): write the putIfAbsent with `required` yourself,
// which is the same wire and one line longer.
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
     * Read several keys in one round trip. Returns {rows, missing, truncated}:
     * absence comes back as a list of missing keys, not as a gap the caller
     * works out by subtraction.
     */
    json get_many(const std::vector<std::string>& keys) {
        return apply(wire::kv_get_many_op(namespace_, keys), "kv getMany");
    }

    /**
     * The keys that start with `prefix`, a page at a time, in byte order:
     * {rows, truncated, nextAfter}. Pass `nextAfter` back as options.after for
     * the following page. Each page is its own snapshot. Only here, never
     * inside a transaction (the broker refuses it there).
     */
    json get_prefix(const std::string& prefix, const KvPrefixOptions& options = KvPrefixOptions()) {
        return apply(wire::kv_get_prefix_op(namespace_, prefix, options), "kv getPrefix");
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
     * Exactly one concurrent caller wins: the broker plans every write at one
     * serial point, so the second caller is judged against the winner's row
     * and does not apply. It also wins against an expired
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
     * Is the key still at `version`? Writes nothing. Version 0 asks the
     * opposite: the key must not exist.
     *
     * Returns {applied, key, version}; `applied` is "the precondition held".
     * When it did not: `reason` (version | absent | exists) with the CURRENT
     * value and version, what a reader would see.
     *
     * On its own it is a linearizable look. Where it earns its place is beside
     * writes, required, in a transaction (`tx.kv(ns).check(key, version)`):
     * everything else then commits only if a key the bundle does not write is
     * unchanged -- which is how a step is tied to a lock.
     *
     * Needs a broker at cluster version 5, the first with this operation.
     */
    json check(const std::string& key, long long version, bool required = false) {
        return apply(wire::kv_check_op(namespace_, key, version, required), "kv check");
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
// schedule, cancel, peek and list. There is no separate `reschedule()`:
//   schedule() IS the upsert, and the
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

    /**
     * Read one pending timer: {found, queue, timerKey, txn, deliverAt,
     * partition, payload, encrypted, attempts, claimed, ...}. The payload comes
     * back AS STORED, base64: if the queue encrypts at rest it is the envelope,
     * and `encrypted` says so.
     */
    json peek(const std::string& queue, const std::string& timer_key) {
        return http_client_->get("/api/v1/timers/" + util::url_encode(queue) + "/" +
                                 util::url_encode(timer_key));
    }

    /**
     * A queue's pending timers, a page at a time: {rows, truncated, nextAfter}.
     * `after` resumes after that timer key (exclusive, byte order); `limit` is
     * clamped server-side. There is no tenant-wide list: the queue is
     * mandatory.
     */
    json list(const std::string& queue, const std::string& after = "", int limit = 0) {
        std::string path = "/api/v1/timers/" + util::url_encode(queue);
        std::string query;
        if (!after.empty()) query += "after=" + util::url_encode(after);
        if (limit > 0) query += (query.empty() ? "" : "&") + std::string("limit=") + std::to_string(limit);
        if (!query.empty()) path += "?" + query;
        json out = http_client_->get(path);
        return out.is_null()
            ? json{{"rows", json::array()}, {"truncated", false}, {"nextAfter", nullptr}}
            : out;
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
// CONFIGURATION is durable -- it lives in the broker's replicated log and comes
// back after a restart, as configured and EMPTY. There is no replay, no history, no subscriptionMode and
// no DLQ, because none of those concepts has a referent when there is no
// history to have.
//
// DELIVERY IS NOT "AT MOST ONCE" (§1.3), and the docs must not say it is. The
// class picks what can be LOST; the ack mode picks the guarantee.
// `commit_on_delivery` (formerly `auto_ack`) advances the cursor at delivery
// and is at-most-once. The default -- explicit ack -- is at-least-once for as
// long as the owning broker incarnation lives: an unacked message redelivers
// when its lease expires, with `attempts` incremented, until retryLimit, after
// which it is DROPPED and counted.
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
     * Declare a queue and its bounds. Persists the OPTIONS in the broker's
     * replicated log (§1.1): the configuration survives a restart, the contents
     * never do, and the queue comes back declared and empty.
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
     * Delete the queue: contents, cursors, and the declared configuration.
     * Named `del` because `delete` is a keyword, exactly as on KvBuilder.
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
        if (options.commit_on_delivery || options.auto_ack) {
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
     * Free to poll: the gauges are read out of the broker's own memory.
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
// Reads by offset: QueueBuilder::fetch and QueueBuilder::changed_since
//
//   auto read = client.queue("orders").fetch({"customer-42", "customer-7"});
//   read["customer-42"].records   // offset, transaction_id, payload, ts
//
//   auto changed = client.queue("orders").changed_since("2026-10-20T14:30:00Z");
//   changed.partitions, changed.safe_time
//
// fetch is POST /api/v1/fetch and changed_since is POST /api/v1/partitions/changed.
// Neither takes a lease, moves a cursor or needs an ack: two readers asking the
// same question get the same answer, and no consumer group notices them. Each
// is served from the answering node's own applied state.
//
// Both do the loops a caller of the raw routes has to write:
// - fetch asks for at most 1000 partitions per call, and asks again from one
//   past the last record until each partition reaches the high watermark its
//   first answer reported. A partition whose start was deleted by retention is
//   read from its oldest kept record (an offset below logStartOffset answers
//   OFFSET_OUT_OF_RANGE and no records, so a loop that does not restart there
//   silently reads nothing).
// - changed_since follows the page cursor (next) until the broker says the pass
//   is over, and answers the smallest safeTime of its pages.
// ============================================================================

/** One record read by offset. */
struct FetchedRecord {
    int64_t offset = 0;
    std::string transaction_id;  // the id the message was pushed with
    json payload;                // as pushed (null when it was empty)
    std::string ts;              // when the push that wrote it was stored
};

/** What fetch read of one partition. */
struct FetchedPartition {
    std::string partition;
    std::vector<FetchedRecord> records;  // in offset order
    int64_t high_watermark = 0;          // the next offset the partition will assign
    int64_t log_start_offset = 0;        // the oldest offset retention kept
    // Where to start next time to read only what is new. Above high_watermark
    // it says the partition was deleted and written again.
    int64_t next_offset = 0;
};

struct FetchOptions {
    // Starts a partition at an offset, e.g. the next_offset of an earlier read.
    // A partition not named starts at 0, the oldest record still kept.
    std::map<std::string, int64_t> offsets;
    // Caps the payload bytes of one call: each entry's budget and the ceiling
    // of the whole call. 8 MiB is the most the broker reads for one entry.
    int64_t max_bytes = 8 * 1024 * 1024;
};

/** One partition of a discovery answer. */
struct ChangedPartition {
    std::string name;
    std::string id;            // new when a partition is deleted and created again
    int64_t last_offset = -1;  // the offset of its last record (-1 when none)
    int64_t log_start = 0;
    std::string last_write_at;
};

struct ChangedPartitions {
    std::vector<ChangedPartition> partitions;  // in the order they were created
    // The broker's time up to which every write is counted in the answer: pass
    // it as the next since to see only what changed after this call. It comes
    // from the broker's clock; never compare it with the local one.
    std::string safe_time;
};

namespace reads {

// The broker refuses more than 1024 entries in one call; 1000 leaves room.
constexpr size_t ENTRIES_PER_CALL = 1000;
// Partitions per discovery page: the broker's default and its ceiling.
constexpr int CHANGED_PAGE = 1000;

inline int64_t int_of(const json& v, int64_t fallback) {
    return v.is_number_integer() ? v.get<int64_t>() : fallback;
}

inline std::string string_of(const json& v) {
    return v.is_string() ? v.get<std::string>() : std::string();
}

/** Microseconds since the epoch in the broker's own spelling (2026-10-20T14:30:00.000000Z). */
inline std::string iso_from_micros(int64_t micros) {
    const int64_t per_day = 86400000000LL;
    int64_t days = micros / per_day;
    int64_t rem = micros % per_day;
    if (rem < 0) {
        rem += per_day;
        --days;
    }
    // Howard Hinnant's civil_from_days: days since 1970-01-01 to a date in the
    // proleptic Gregorian calendar.
    const int64_t z = days + 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const int64_t doe = z - era * 146097;
    const int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const int64_t mp = (5 * doy + 2) / 153;
    const int64_t d = doy - (153 * mp + 2) / 5 + 1;
    const int64_t m = mp < 10 ? mp + 3 : mp - 9;
    const int64_t y = yoe + era * 400 + (m <= 2 ? 1 : 0);
    const int64_t secs = rem / 1000000;
    char text[96];  // room for the widest ints, so no compiler calls the format truncated
    std::snprintf(text, sizeof(text), "%04lld-%02lld-%02lldT%02lld:%02lld:%02lld.%06lldZ",
                  static_cast<long long>(y), static_cast<long long>(m), static_cast<long long>(d),
                  static_cast<long long>(secs / 3600), static_cast<long long>((secs / 60) % 60),
                  static_cast<long long>(secs % 60), static_cast<long long>(rem % 1000000));
    return text;
}

inline std::map<std::string, FetchedPartition> fetch(HttpClient& http, const std::string& queue,
                                                     const std::vector<std::string>& partitions,
                                                     const FetchOptions& options) {
    if (options.max_bytes < 1) {
        throw std::invalid_argument("fetch: max_bytes must be positive");
    }
    struct State {
        FetchedPartition read;
        bool has_end = false;
        int64_t end = 0;  // the high watermark of the first answer: the read ends there
    };
    std::vector<std::string> names;
    std::map<std::string, State> state;
    for (const auto& name : partitions) {
        if (name.empty()) {
            throw std::invalid_argument("fetch: a partition name must be a non-empty string");
        }
        if (state.count(name)) {
            continue;
        }
        auto offset = options.offsets.find(name);
        const int64_t start = offset == options.offsets.end() ? 0 : offset->second;
        if (start < 0) {
            throw std::invalid_argument("fetch: the offset of '" + name + "' must be non-negative");
        }
        State s;
        s.read.partition = name;
        s.read.next_offset = start;
        state.emplace(name, std::move(s));
        names.push_back(name);
    }

    std::vector<std::string> pending = names;
    while (!pending.empty()) {
        std::vector<std::string> again;
        for (size_t i = 0; i < pending.size(); i += ENTRIES_PER_CALL) {
            const size_t end_of_chunk = std::min(pending.size(), i + ENTRIES_PER_CALL);
            json entries = json::array();
            for (size_t j = i; j < end_of_chunk; ++j) {
                entries.push_back({{"queue", queue},
                                   {"partition", pending[j]},
                                   {"offset", state[pending[j]].read.next_offset},
                                   {"maxBytes", options.max_bytes}});
            }
            json answer = http.post("/api/v1/fetch", {{"entries", entries}, {"maxBytes", options.max_bytes}});
            const size_t asked = end_of_chunk - i;
            if (!answer.is_object() || !answer.contains("entries") || !answer["entries"].is_array() ||
                answer["entries"].size() != asked) {
                throw Error("fetch " + queue + ": the broker answered " +
                                         std::to_string(answer.is_object() && answer.contains("entries") &&
                                                                answer["entries"].is_array()
                                                            ? answer["entries"].size()
                                                            : 0) +
                                         " entries for " + std::to_string(asked) + " asked");
            }

            size_t delivered = 0;
            // Entries that came back empty while short of their end: the call's
            // byte ceiling was spent by the entries before them.
            std::vector<std::string> starved;
            for (size_t j = i; j < end_of_chunk; ++j) {
                const std::string& name = pending[j];
                const json& entry = answer["entries"][j - i];
                State& s = state[name];
                s.read.high_watermark = int_of(entry.value("highWatermark", json()), s.read.high_watermark);
                s.read.log_start_offset = int_of(entry.value("logStartOffset", json()), s.read.log_start_offset);
                const std::string error = string_of(entry.value("error", json()));
                if (!error.empty()) {
                    if (error == "OFFSET_OUT_OF_RANGE" && s.read.next_offset < s.read.log_start_offset) {
                        // Retention deleted what was asked for: go on from the
                        // oldest kept.
                        s.read.next_offset = s.read.log_start_offset;
                        if (!s.has_end) {
                            s.has_end = true;
                            s.end = s.read.high_watermark;
                        }
                        if (s.read.next_offset < s.end) {
                            again.push_back(name);
                        }
                        continue;
                    }
                    if (error == "OFFSET_OUT_OF_RANGE" || error == "UNKNOWN_TOPIC_OR_PARTITION") {
                        continue;
                    }
                    throw Error("fetch " + queue + "/" + name + ": the broker answered " + error);
                }
                if (!s.has_end) {
                    s.has_end = true;
                    s.end = s.read.high_watermark;
                }
                const json records = entry.value("records", json::array());
                if (records.is_array() && !records.empty()) {
                    delivered += records.size();
                    for (const auto& r : records) {
                        FetchedRecord record;
                        record.offset = int_of(r.value("offset", json()), 0);
                        record.transaction_id = string_of(r.value("transactionId", json()));
                        record.payload = r.value("payload", json());
                        record.ts = string_of(r.value("ts", json()));
                        s.read.records.push_back(std::move(record));
                    }
                    s.read.next_offset = s.read.records.back().offset + 1;
                    if (s.read.next_offset < s.end) {
                        again.push_back(name);
                    }
                } else if (s.read.next_offset < s.end) {
                    starved.push_back(name);
                }
            }
            // An empty entry is asked again only when its call read something:
            // a call that read nothing at all would answer the same next time.
            if (delivered > 0) {
                again.insert(again.end(), starved.begin(), starved.end());
            }
        }
        pending = std::move(again);
    }

    std::map<std::string, FetchedPartition> out;
    for (auto& [name, s] : state) {
        out.emplace(name, std::move(s.read));
    }
    return out;
}

/** `since` empty = every partition of the queue. */
inline ChangedPartitions changed_since(HttpClient& http, const std::string& queue, const std::string& since) {
    ChangedPartitions out;
    std::string after;
    while (true) {
        json entry = {{"queue", queue}, {"limit", CHANGED_PAGE}};
        if (!since.empty()) {
            entry["since"] = since;
        }
        if (!after.empty()) {
            entry["after"] = after;
        }
        json answer = http.post("/api/v1/partitions/changed", {{"entries", json::array({entry})}});
        if (!answer.is_object()) {
            throw Error("changed_since " + queue + ": the broker answered no entry");
        }
        // Pages served by different nodes: the smallest is the one that holds
        // for all of them. The broker spells every one the same way, so the
        // strings compare in time order.
        const std::string page_safe = string_of(answer.value("safeTime", json()));
        if (!page_safe.empty() && (out.safe_time.empty() || page_safe < out.safe_time)) {
            out.safe_time = page_safe;
        }
        const json results = answer.value("entries", json::array());
        if (!results.is_array() || results.empty()) {
            throw Error("changed_since " + queue + ": the broker answered no entry");
        }
        const json& result = results[0];
        const std::string error = string_of(result.value("error", json()));
        if (error == "UNKNOWN_TOPIC_OR_PARTITION") {
            return out;
        }
        if (!error.empty()) {
            throw Error("changed_since " + queue + ": the broker answered " + error);
        }
        for (const auto& p : result.value("partitions", json::array())) {
            ChangedPartition partition;
            partition.name = string_of(p.value("name", json()));
            partition.id = string_of(p.value("id", json()));
            partition.last_offset = int_of(p.value("lastOffset", json()), -1);
            partition.log_start = int_of(p.value("logStart", json()), 0);
            partition.last_write_at = string_of(p.value("lastWriteAt", json()));
            out.partitions.push_back(std::move(partition));
        }
        after = string_of(result.value("next", json()));
        if (after.empty()) {
            return out;
        }
    }
}

}  // namespace reads

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
        
        // A failure throws: an empty dead-letter queue and an unreachable
        // broker are not the same answer.
        json result = http_client_->get("/api/v1/dlq" + params.str());
        if (result.is_null()) {
            return {{"messages", json::array()}, {"total", 0}};
        }
        return result;
    }
};

// ============================================================================
// Admin - the management and observability routes
// ============================================================================

/** Query parameters, in order: {{"limit", "10"}, {"search", "orders"}}. */
using Query = std::vector<std::pair<std::string, std::string>>;

/** Where seek_consumer_group moves a group's cursor: past the end, or to a time. */
struct SeekOptions {
    /// Past every message there now: the group skips the backlog.
    bool to_end = false;
    /// An ISO-8601 instant; used when to_end is false.
    std::string timestamp;
};

/**
 * The broker's management and observability routes, as the JS, Python, Go,
 * Rust and PHP clients have them. Answers are the broker's JSON as it comes;
 * a failure throws (HttpError, NetworkError).
 *
 * Writes that change what consumers see (retry_message, delete_message,
 * seek_*) are sent once, without the transport's 5xx/network resend: their
 * verdict is the caller's to read, and a blind resend would hide it.
 */
class Admin {
private:
    std::shared_ptr<HttpClient> http_;

    static std::string with_query(const std::string& path, const Query& params) {
        if (params.empty()) return path;
        std::string out = path + "?";
        for (size_t i = 0; i < params.size(); ++i) {
            if (i > 0) out += "&";
            out += util::url_encode(params[i].first) + "=" + util::url_encode(params[i].second);
        }
        return out;
    }

    static RequestOptions once() {
        RequestOptions options;
        options.no_retry = true;
        return options;
    }

    static std::string enc(const std::string& value) { return util::url_encode(value); }

public:
    explicit Admin(std::shared_ptr<HttpClient> http) : http_(std::move(http)) {
    }

    // ------------------------------------------------------------ resources

    json overview() { return http_->get("/api/v1/resources/overview"); }
    json namespaces() { return http_->get("/api/v1/resources/namespaces"); }
    json tasks() { return http_->get("/api/v1/resources/tasks"); }

    /** Filters: namespace, task, search, limit, offset. */
    json list_queues(const Query& params = {}) {
        return http_->get(with_query("/api/v1/resources/queues", params));
    }

    /** One queue: its options, namespace, task and counts. */
    json queue(const std::string& name) {
        return http_->get("/api/v1/resources/queues/" + enc(name));
    }

    /** Pending messages, for one group or every group. */
    json queue_depth(const std::string& name, const std::string& group = "") {
        Query params;
        if (!group.empty()) params.push_back({"group", group});
        return http_->get(with_query("/api/v1/resources/queues/" + enc(name) + "/depth", params));
    }

    json partitions(const Query& params = {}) {
        return http_->get(with_query("/api/v1/resources/partitions", params));
    }

    // ------------------------------------------------------------- messages

    json list_messages(const Query& params = {}) {
        return http_->get(with_query("/api/v1/messages", params));
    }

    json message(const std::string& partition_id, const std::string& transaction_id) {
        return http_->get("/api/v1/messages/" + enc(partition_id) + "/" + enc(transaction_id));
    }

    json delete_message(const std::string& partition_id, const std::string& transaction_id) {
        return http_->del("/api/v1/messages/" + enc(partition_id) + "/" + enc(transaction_id), once());
    }

    /**
     * Put a dead-lettered message back into its partition (and out of the
     * dead-letter queue). The copy lands at the partition's TAIL, so it is out
     * of order with respect to its own key.
     */
    json retry_message(const std::string& partition_id, const std::string& transaction_id) {
        return http_->post("/api/v1/messages/" + enc(partition_id) + "/" + enc(transaction_id) +
                           "/retry", json::object(), once());
    }

    /** The dead-letter queue: filters queue, consumerGroup, limit, offset. */
    json dlq(const Query& params = {}) {
        json out = http_->get(with_query("/api/v1/dlq", params));
        return out.is_null() ? json{{"messages", json::array()}, {"total", 0}} : out;
    }

    // --------------------------------------------------------------- traces

    /** Record a trace event: {transactionId, partitionId, consumerGroup, traceName?, eventType?, data}. */
    json record_trace(const json& trace) { return http_->post("/api/v1/traces", trace); }

    json trace_names(const Query& params = {}) {
        return http_->get(with_query("/api/v1/traces/names", params));
    }

    json traces_by_name(const std::string& name, const Query& params = {}) {
        return http_->get(with_query("/api/v1/traces/by-name/" + enc(name), params));
    }

    json traces_for_message(const std::string& partition_id, const std::string& transaction_id) {
        return http_->get("/api/v1/traces/" + enc(partition_id) + "/" + enc(transaction_id));
    }

    // ------------------------------------------------------ consumer groups

    json list_consumer_groups() { return http_->get("/api/v1/consumer-groups"); }

    json consumer_group(const std::string& name) {
        return http_->get("/api/v1/consumer-groups/" + enc(name));
    }

    json lagging_consumers(long long min_lag_seconds) {
        return http_->get("/api/v1/consumer-groups/lagging?minLagSeconds=" +
                          std::to_string(min_lag_seconds));
    }

    json delete_consumer_group(const std::string& group, bool delete_metadata = false) {
        return http_->del("/api/v1/consumer-groups/" + enc(group) +
                          "?deleteMetadata=" + (delete_metadata ? "true" : "false"), once());
    }

    json delete_consumer_group_for_queue(const std::string& group, const std::string& queue,
                                         bool delete_metadata = false) {
        return http_->del("/api/v1/consumer-groups/" + enc(group) + "/queues/" + enc(queue) +
                          "?deleteMetadata=" + (delete_metadata ? "true" : "false"), once());
    }

    /**
     * Move a group's cursor on a queue: past the end (to_end), or to a time.
     * This is how a group that already exists is replayed -- subscription_mode
     * only applies to a group's first pop.
     */
    json seek_consumer_group(const std::string& group, const std::string& queue,
                             const SeekOptions& seek) {
        json body = json::object();
        if (seek.to_end) {
            body["toEnd"] = true;
        } else if (!seek.timestamp.empty()) {
            body["timestamp"] = seek.timestamp;
        } else {
            throw std::invalid_argument("seek needs to_end or a timestamp");
        }
        return http_->post("/api/v1/consumer-groups/" + enc(group) + "/queues/" + enc(queue) +
                           "/seek", body, once());
    }

    /** Move one partition of a group's cursor past its end. */
    json seek_consumer_group_partition(const std::string& group, const std::string& queue,
                                       const std::string& partition) {
        return http_->post("/api/v1/consumer-groups/" + enc(group) + "/queues/" + enc(queue) +
                           "/partitions/" + enc(partition) + "/seek", json::object(), once());
    }

    /** Where a group that has not popped yet starts (an ISO-8601 instant). */
    json set_subscription_timestamp(const std::string& group, const std::string& timestamp) {
        return http_->post("/api/v1/consumer-groups/" + enc(group) + "/subscription",
                           json{{"subscriptionTimestamp", timestamp}});
    }

    json refresh_consumer_stats() { return http_->post("/api/v1/stats/refresh", json::object()); }

    /** Extend a lease by `seconds` (0 = the broker's default). */
    json renew_lease(const std::string& lease_id, int seconds = 0) {
        json body = json::object();
        if (seconds > 0) body["seconds"] = seconds;
        return http_->post("/api/v1/lease/" + enc(lease_id) + "/extend", body);
    }

    // --------------------------------------------------------------- status

    json status(const Query& params = {}) { return http_->get(with_query("/api/v1/status", params)); }

    json queue_stats(const Query& params = {}) {
        return http_->get(with_query("/api/v1/status/queues", params));
    }

    json queue_detail(const std::string& name, const Query& params = {}) {
        return http_->get(with_query("/api/v1/status/queues/" + enc(name), params));
    }

    json analytics(const Query& params = {}) {
        return http_->get(with_query("/api/v1/status/analytics", params));
    }

    json system_metrics(const Query& params = {}) {
        return http_->get(with_query("/api/v1/analytics/system-metrics", params));
    }

    json worker_metrics(const Query& params = {}) {
        return http_->get(with_query("/api/v1/analytics/worker-metrics", params));
    }

    json health() { return http_->get("/health"); }

    /** The Prometheus text of GET /metrics. */
    std::string metrics() { return http_->get_text("/metrics"); }
};

// ============================================================================
// The pop - one code path for pop(), pop_result() and every consume() worker
// ============================================================================

namespace detail {

/** Everything one pop request is made of. */
struct PopTarget {
    std::string queue;
    std::string partition;          // empty = any partition of the queue
    std::string namespace_name;
    std::string task;
    std::string group;
    int batch = 0;                  // 0 = unset (autopilot's to choose)
    int max_partitions = 0;         // 0 = unset
    bool autopilot = true;
    bool wait = true;
    int timeout_millis = 30000;
    std::string subscription_mode;
    std::string subscription_from;
    bool conflation = false;
    bool commit_on_delivery = false;
};

inline std::string pop_path(const PopTarget& t) {
    if (!t.queue.empty()) {
        if (!t.partition.empty()) {
            return "/api/v1/pop/queue/" + util::url_encode(t.queue) + "/partition/" +
                   util::url_encode(t.partition);
        }
        return "/api/v1/pop/queue/" + util::url_encode(t.queue);
    }
    if (!t.namespace_name.empty() || !t.task.empty()) {
        return "/api/v1/pop";
    }
    throw std::invalid_argument("a pop needs a queue, or a namespace/task to discover one");
}

/**
 * The query string: the keys this SDK has always sent, and with autopilot off
 * the same keys and values as before autopilot. (cpp-httplib re-sorts the keys
 * when it sends, so it is the set that is pinned, not the order.)
 */
inline std::string pop_query(const PopTarget& t) {
    // Batch, partitions and the autopilot flag: THE RULE lives in one place.
    const util::PopSizing sizing = util::pop_sizing(t.batch, t.max_partitions, 1, t.autopilot);
    std::string q = "?";
    if (sizing.autopilot) q += "autopilot=true&";
    if (sizing.has_batch) q += "batch=" + std::to_string(sizing.batch) + "&";
    q += std::string("wait=") + (t.wait ? "true" : "false");
    q += "&timeout=" + std::to_string(t.timeout_millis);
    if (!t.group.empty()) q += "&consumerGroup=" + util::url_encode(t.group);
    if (!t.namespace_name.empty()) q += "&namespace=" + util::url_encode(t.namespace_name);
    if (!t.task.empty()) q += "&task=" + util::url_encode(t.task);
    if (!t.subscription_mode.empty()) q += "&subscriptionMode=" + util::url_encode(t.subscription_mode);
    if (!t.subscription_from.empty()) q += "&subscriptionFrom=" + util::url_encode(t.subscription_from);
    // v4 multi-partition pop. Under autopilot a pinned width travels even when
    // it is 1, because 1 is then a decision and not the absence of one.
    if (sizing.has_partitions) q += "&partitions=" + std::to_string(sizing.partitions);
    // Emitted only when true: `conflation=false` would read as "turn the
    // group's policy off", which a pop may not do.
    if (t.conflation) q += "&conflation=true";
    // Commit at delivery: no lease, nothing to ack. `autoAck` is the name every
    // 2.x broker reads.
    if (t.commit_on_delivery) q += "&autoAck=true";
    return q;
}

inline std::string affinity_key(const PopTarget& t) {
    return util::grouping_key(t.queue, t.partition, t.namespace_name, t.task, t.group);
}

/**
 * One pop round trip. Throws on failure -- a 403, an exhausted retry budget,
 * a network fault -- because an empty result has to mean an empty queue and
 * nothing else (before 2.7.1 this SDK logged the failure and returned [], so
 * revoked credentials looked like an idle queue).
 */
inline PopResult pop_once(HttpClient& http, const PopTarget& t) {
    RequestOptions options;
    // Five seconds of slack, so the broker's long-poll timer fires before the
    // client's and a quiet queue reads as an empty poll, not a timeout.
    options.timeout_millis = t.wait ? t.timeout_millis + 5000 : t.timeout_millis;
    options.retry_kind = t.wait ? RetryKind::Pop : RetryKind::Default;
    options.affinity_key = affinity_key(t);

    json answer = http.get(pop_path(t) + pop_query(t), options);

    // Degrade loudly (conflation): BEFORE the empty-answer return, because an
    // old broker answers an idle queue with a bodiless 204, the likeliest first
    // contact, and the check has to fire on it.
    util::enforce_conflation_contract(t.conflation, answer,
                                      util::pop_target_label(t.queue, t.namespace_name, t.task),
                                      t.group);
    if (answer.is_object() && answer.contains("success") && answer["success"].is_boolean() &&
        !answer["success"].get<bool>()) {
        const std::string error = util::text_of(answer, "error");
        throw Error("pop failed: " + (error.empty() ? std::string("no reason given") : error));
    }
    PopResult out;
    out.autopilot = util::parse_autopilot_decision(answer);
    out.messages = util::messages_of(answer);
    // Every 2.x broker names the group on each message; when an answer does
    // not (a proxy, a stub), the message takes the group this pop asked for,
    // so its ack never falls back to the queue-mode cursor.
    for (auto& message : out.messages) {
        if (message.consumer_group.empty() && !t.group.empty()) message.consumer_group = t.group;
        if (message.queue.empty()) message.queue = t.queue;
    }
    return out;
}

/** Signals, when ClientConfig::handle_signals asked for them. */
inline std::atomic<int>& signal_count() {
    static std::atomic<int> count{0};
    return count;
}

inline void on_signal(int sig) {
    // The first signal asks the program to stop; a second one does what the
    // signal would have done without these handlers.
    if (signal_count().fetch_add(1) >= 1) {
        std::signal(sig, SIG_DFL);
        std::raise(sig);
    }
}

inline void install_signal_handlers() {
    static std::once_flag once;
    std::call_once(once, [] {
        signal_count();  // initialized before a handler can run
        std::signal(SIGINT, on_signal);
        std::signal(SIGTERM, on_signal);
    });
}

} // namespace detail

class Queen;

// ============================================================================
// ConsumerManager - the consume() loop
// ============================================================================

/**
 * Runs `concurrency` workers, one thread each, that pop, hand messages to the
 * handler and settle them. QueueBuilder::consume and consume_batch are what
 * most code wants; this is public for a caller that builds ConsumeOptions
 * itself.
 *
 * A handler signals failure by throwing (anything). With auto_ack (the
 * default) a message whose handler returned is acked, one whose handler threw
 * is nacked with the exception's text -- which spends a retry and, past
 * retryLimit, files the message in the dead-letter queue with that text. With
 * auto_ack(false) nothing is sent: the handler settles messages itself, and a
 * failed one comes back when its lease expires.
 *
 * ORDERING AFTER A FAILURE (per-message mode). A nack releases the failed
 * message's PARTITION and leaves its cursor at that message, so the later
 * messages of the same partition in this batch are redelivered: the loop skips
 * them instead of handling messages whose acks the broker would refuse. The
 * other partitions of a multi-partition pop are handled. With auto_ack(false)
 * the loop abandons the rest of the batch after a failure, since the handler's
 * ack of a later message could commit the failed one.
 */
class ConsumerManager {
public:
    using Handler = std::function<void(const Message&)>;
    using BatchHandler = std::function<void(const std::vector<Message>&)>;

private:
    std::shared_ptr<HttpClient> http_client_;
    Queen* queen_;

    struct Shared {
        ConsumeOptions options;
        detail::PopTarget target;
        std::shared_ptr<ConsumerSupervision> supervision;
        std::atomic<uint64_t> processed{0};
        std::atomic<uint64_t> acked{0};
        std::atomic<uint64_t> nacked{0};
        std::atomic<bool> stop{false};
        std::mutex mutex;
        StopReason reason = StopReason::Ended;
        std::exception_ptr error;
    };

    using Process = std::function<void(Shared&, std::vector<Message>&)>;

    void stop_with(Shared& shared, StopReason reason) {
        if (!shared.stop.exchange(true)) {
            std::lock_guard<std::mutex> lock(shared.mutex);
            shared.reason = reason;
        }
    }

    void record_error(Shared& shared) {
        {
            std::lock_guard<std::mutex> lock(shared.mutex);
            if (!shared.error) shared.error = std::current_exception();
        }
        stop_with(shared, StopReason::Ended);
    }

    bool cancelled(const Shared& shared) const;   // defined after Queen

    bool should_stop(const Shared& shared) const {
        return shared.stop.load() || cancelled(shared);
    }

    bool limit_reached(const Shared& shared) const {
        return shared.options.limit > 0 &&
               shared.processed.load() >= static_cast<uint64_t>(shared.options.limit);
    }

    /** Sleep, waking early when the loop is cancelled; true = stop now. */
    bool pause(const Shared& shared, int millis) const {
        if (shared.options.cancel) {
            if (shared.options.cancel->wait_for(std::chrono::milliseconds(millis))) return true;
            return should_stop(shared);
        }
        const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(millis);
        while (std::chrono::steady_clock::now() < until) {
            if (should_stop(shared)) return true;
            const auto left = until - std::chrono::steady_clock::now();
            std::this_thread::sleep_for(std::min<std::chrono::steady_clock::duration>(
                left, std::chrono::milliseconds(100)));
        }
        return should_stop(shared);
    }

    /**
     * Run the handler and say how it failed, if it did. A handler can throw
     * anything, not only a std::exception, and every throw is a failed message:
     * none may escape into the worker.
     */
    template <typename F, typename Arg>
    static std::optional<std::string> run_handler(const F& handler, const Arg& input) {
        try {
            handler(input);
            return std::nullopt;
        } catch (const std::exception& e) {
            return std::string(e.what());
        } catch (...) {
            return std::string("handler threw a non-standard exception");
        }
    }

    bool settle(Shared& shared, const Message& message, const std::optional<std::string>& failure);
    void settle_batch(Shared& shared, const std::vector<Message>& messages,
                      const std::optional<std::string>& failure);
    void worker(int id, Shared& shared, const Process& process);
    ConsumeSummary run_loop(ConsumeOptions options, Process process);

public:
    ConsumerManager(std::shared_ptr<HttpClient> http_client, Queen* queen)
        : http_client_(std::move(http_client)), queen_(queen) {
    }

    /** One message per handler call. */
    ConsumeSummary run(Handler handler, const ConsumeOptions& options);

    /** A whole claimed batch per handler call, settled with one request. */
    ConsumeSummary run_batch(BatchHandler handler, const ConsumeOptions& options);
};

// ============================================================================
// QueueBuilder - Fluent API for queue operations
// ============================================================================

class QueueBuilder {
private:
    Queen* queen_;
    std::shared_ptr<HttpClient> http_client_;
    std::shared_ptr<BufferManager> buffer_manager_;
    std::string queue_name_;
    std::string partition_ = "Default";
    std::string namespace_;
    std::string task_;
    std::string group_;
    QueueConfig config_;

    // Consume options. batch_ / max_partitions_ hold the USER's value, and 0
    // means the setter was never called -- which is the dimension pop
    // autopilot gets to choose (util::pop_sizing).
    std::optional<SupervisionConfig> supervision_;
    int concurrency_ = 1;
    int batch_ = 0;
    long long limit_ = 0;
    int idle_millis_ = 0;
    bool auto_ack_ = true;
    bool commit_on_delivery_ = false;
    bool wait_ = true;
    int timeout_millis_ = 30000;
    bool renew_lease_ = false;
    int renew_lease_interval_millis_ = 60000;
    std::string subscription_mode_;
    std::string subscription_from_;
    int max_partitions_ = 0;
    bool conflation_ = false;
    /// Per-builder override: unset = the client default.
    std::optional<bool> autopilot_;
    std::optional<Cancel> cancel_;

    std::optional<BufferOptions> buffer_options_;

    void require_queue(const char* what) const {
        if (queue_name_.empty()) {
            throw std::invalid_argument(std::string("a queue name is required for ") + what);
        }
    }

    std::string partition_or_empty() const {
        return partition_ == util::DEFAULT_PARTITION ? std::string() : partition_;
    }

    detail::PopTarget pop_target(bool commit_on_delivery) const {
        detail::PopTarget t;
        t.queue = queue_name_;
        t.partition = partition_or_empty();
        t.namespace_name = namespace_;
        t.task = task_;
        t.group = group_;
        t.batch = batch_;
        t.max_partitions = max_partitions_;
        t.autopilot = autopilot_enabled();
        t.wait = wait_;
        t.timeout_millis = timeout_millis_;
        t.subscription_mode = subscription_mode_;
        t.subscription_from = subscription_from_;
        t.conflation = conflation_;
        t.commit_on_delivery = commit_on_delivery;
        return t;
    }

    ConsumeOptions consume_options() const {
        if (commit_on_delivery_) {
            throw std::invalid_argument(
                "commit_on_delivery() is a pop() option; consume() always leases its messages");
        }
        ConsumeOptions options;
        options.supervision = supervision_;
        options.queue = queue_name_;
        options.partition = partition_or_empty();
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
        options.max_partitions = max_partitions_;
        options.conflation = conflation_;
        // Resolved here, so the loop sees a decision and not an empty optional.
        options.autopilot = autopilot_enabled();
        options.cancel = cancel_;
        return options;
    }

public:
    QueueBuilder(Queen* queen, std::shared_ptr<HttpClient> http_client,
                 std::shared_ptr<BufferManager> buffer_manager, const std::string& queue_name = "")
        : queen_(queen), http_client_(std::move(http_client)),
          buffer_manager_(std::move(buffer_manager)), queue_name_(queue_name) {
    }

    const std::string& name() const { return queue_name_; }

    /** The affinity key of this builder's requests: the broker's grouping. */
    std::string affinity_key() const {
        return util::grouping_key(queue_name_, partition_or_empty(), namespace_, task_, group_);
    }

    // ------------------------------------------------------------- naming

    QueueBuilder& namespace_name(const std::string& name) {
        namespace_ = name;
        return *this;
    }

    QueueBuilder& task(const std::string& name) {
        task_ = name;
        return *this;
    }

    /** The partition: one per entity (an order, a customer), created by the first push. */
    QueueBuilder& partition(const std::string& name) {
        partition_ = name.empty() ? std::string(util::DEFAULT_PARTITION) : name;
        return *this;
    }

    /** The consumer group. None = queue mode: each message goes to one worker. */
    QueueBuilder& group(const std::string& name) {
        group_ = name;
        return *this;
    }

    // ------------------------------------------------------------ lifecycle

    /** The options create() sends. */
    QueueBuilder& config(const QueueConfig& cfg) {
        config_ = cfg;
        return *this;
    }

    /**
     * Create or reconfigure the queue with these options. A full upsert: an
     * option left unset goes back to the broker's default. The namespace and
     * task set on this builder go with it.
     */
    json configure(const QueueConfig& cfg) {
        require_queue("configure");
        json body = {{"queue", queue_name_}, {"options", cfg.to_json()}};
        if (!namespace_.empty()) body["namespace"] = namespace_;
        if (!task_.empty()) body["task"] = task_;
        json out = http_client_->post("/api/v1/configure", body);
        // A configure failure can come back inside a 200 body.
        if (out.is_object() && !util::text_of(out, "error").empty()) {
            throw Error("configure failed: " + util::text_of(out, "error"));
        }
        return out;
    }

    /** configure() with the options set by config() (none = every default). */
    json create() { return configure(config_); }

    /** Drop the queue and everything in it. */
    json del() {
        require_queue("delete");
        return http_client_->del("/api/v1/resources/queues/" + util::url_encode(queue_name_));
    }

    // ----------------------------------------------------------------- push

    /** Buffer this builder's pushes on the client (see BufferOptions). */
    QueueBuilder& buffer(const BufferOptions& options) {
        buffer_options_ = options;
        return *this;
    }

    /**
     * Push one message to this queue and partition; the payload travels as it
     * is. Its transaction id is minted here (a UUIDv7): use push_items() to
     * give it one derived from the work, which is what makes a retried push a
     * `duplicate` instead of a second message.
     *
     * Returns one PushResult per message: read `status` (queued, duplicate,
     * error) -- the broker answers HTTP 201 even when it refuses items. A
     * buffered push returns nothing yet: its verdict comes with the flush.
     */
    std::vector<PushResult> push(const json& payload) {
        if (payload.is_array()) {
            throw std::invalid_argument(
                "push() takes ONE payload; for several messages call push_many(), and to send an "
                "array as one payload wrap it: push_many({payload})");
        }
        return push_items({PushItem{payload, ""}});
    }

    /**
     * One message with its own transaction id: the dedup key, derived from the
     * work ("order-" + id), so pushing it again inside the queue's dedup window
     * is answered `duplicate` and writes nothing.
     */
    std::vector<PushResult> push(const json& payload, const std::string& transaction_id) {
        if (payload.is_array()) {
            throw std::invalid_argument(
                "push() takes ONE payload; for several messages call push_items()");
        }
        return push_items({PushItem{payload, transaction_id}});
    }

    /** Several messages, one per payload, in order. */
    std::vector<PushResult> push_many(const std::vector<json>& payloads) {
        std::vector<PushItem> items;
        items.reserve(payloads.size());
        for (const auto& payload : payloads) items.push_back(PushItem{payload, ""});
        return push_items(items);
    }

    /** Messages with their own transaction ids (the dedup key). */
    std::vector<PushResult> push_items(const std::vector<PushItem>& items) {
        require_queue("push");
        if (items.empty()) return {};
        json formatted = json::array();
        for (const auto& item : items) {
            formatted.push_back({
                {"queue", queue_name_},
                {"partition", partition_},
                {"payload", item.payload},
                {"transactionId", item.transaction_id.empty() ? util::generate_uuid_v7()
                                                              : item.transaction_id}
            });
        }
        if (buffer_options_.has_value()) {
            const std::string address = queue_name_ + "/" + partition_;
            for (const auto& item : formatted) {
                buffer_manager_->add_message(address, item, *buffer_options_);
            }
            return {};
        }
        RequestOptions options;
        options.affinity_key = affinity_key();
        return util::parse_push_results(
            http_client_->post("/api/v1/push", json{{"items", std::move(formatted)}}, options));
    }

    /**
     * Flush this builder's queue/partition buffer. With no deadline (the
     * default) a failing batch is retried until it lands or the client
     * closes; with one, a BufferFlushError after deadline_millis reports how
     * many messages are still buffered (none dropped).
     */
    void flush_buffer(int deadline_millis = -1) {
        require_queue("flush_buffer");
        buffer_manager_->flush_buffer(queue_name_ + "/" + partition_, deadline_millis);
    }

    // -------------------------------------------------------- pop / consume

    QueueBuilder& supervision(std::optional<SupervisionConfig> config = std::nullopt) {
        supervision_ = std::move(config);
        return *this;
    }

    /** consume(): workers polling in parallel, one thread each. */
    QueueBuilder& concurrency(int count) {
        concurrency_ = std::max(1, count);
        return *this;
    }

    /**
     * Pin the message budget of one pop. Unset, the broker sizes it
     * (autopilot). batch(0) is the absence of an opinion and reads as unset.
     */
    QueueBuilder& batch(int size) {
        batch_ = size > 0 ? size : 0;
        return *this;
    }

    /**
     * Multi-partition pop: claim up to N partitions per call; batch() is then a
     * cap on the messages across all of them, which share one lease. Unset, the
     * broker chooses the width; partitions(1) pins one partition per pop.
     */
    QueueBuilder& partitions(int n) {
        max_partitions_ = n > 0 ? n : 0;
        return *this;
    }

    /**
     * Broker-side pop sizing for this builder. On (the default) the broker
     * chooses the batch and partitions left unset, per pop, from state this
     * client cannot see; a knob set explicitly is never second-guessed.
     * autopilot(false) restores the pre-1.2 client-side defaults (batch 1,
     * partitions 1) byte for byte; QUEEN_SDK_POP_AUTOPILOT=off does the same
     * for a whole process, and this call outranks it in both directions.
     */
    QueueBuilder& autopilot(bool enabled = true) {
        autopilot_ = enabled;
        return *this;
    }

    /**
     * consume(): stop after this many messages, counted across all workers.
     * A soft cap, as in the Rust client: a worker finishes the message it is
     * handling (with consume_batch, the batch), so a loop can end up to
     * concurrency - 1 messages past it, or a batch past it.
     */
    QueueBuilder& limit(long long count) {
        limit_ = count;
        return *this;
    }

    /** consume(): stop after this long without a message. */
    QueueBuilder& idle_millis(int millis) {
        idle_millis_ = millis;
        return *this;
    }

    /**
     * consume(): ack after the handler returns, nack when it throws (the
     * default). Off, the handler settles its messages itself and a failure is
     * only logged. No effect on pop().
     */
    QueueBuilder& auto_ack(bool enabled) {
        auto_ack_ = enabled;
        return *this;
    }

    /**
     * pop(): the broker moves the group's cursor past the messages as it hands
     * them out. No lease, nothing to ack, at-most-once. consume() refuses it:
     * it always leases. The broker refuses it with conflation() (a 400).
     */
    QueueBuilder& commit_on_delivery(bool enabled = true) {
        commit_on_delivery_ = enabled;
        return *this;
    }

    /** Long-poll (the default): an empty pop waits on the broker up to timeout_millis. */
    QueueBuilder& wait(bool enabled) {
        wait_ = enabled;
        return *this;
    }

    /** The long-poll window of a waiting pop, in milliseconds (default 30000). */
    QueueBuilder& timeout_millis(int millis) {
        timeout_millis_ = millis > 0 ? millis : 30000;
        return *this;
    }

    /** consume(): renew the batch's lease every interval while the handler runs. */
    QueueBuilder& renew_lease(bool enabled, int interval_millis = 60000) {
        renew_lease_ = enabled;
        if (interval_millis > 0) renew_lease_interval_millis_ = interval_millis;
        return *this;
    }

    /**
     * Where a NEW group starts: "new" (the default: messages pushed after its
     * first pop), "all" (from the oldest kept). Fixed at the group's first pop;
     * move an existing group with admin().seek_consumer_group().
     */
    QueueBuilder& subscription_mode(const std::string& mode) {
        subscription_mode_ = mode;
        return *this;
    }

    /** Where a NEW group starts, as an ISO-8601 instant. */
    QueueBuilder& subscription_from(const std::string& from) {
        subscription_from_ = from;
        return *this;
    }

    /**
     * Last-value delivery for this consumer group: under backlog the group is
     * served ONE message per partition, the newest. A policy of the GROUP,
     * fixed when it is first registered. Needs group(); needs a broker >= 1.1.0,
     * and an older one is refused on the first answer
     * (ConflationUnsupportedError).
     */
    QueueBuilder& conflation(bool enabled = true) {
        conflation_ = enabled;
        return *this;
    }

    /** consume(): stop the loop from another thread with cancel.cancel(). */
    QueueBuilder& cancel(const Cancel& token) {
        cancel_ = token;
        return *this;
    }

    /**
     * This builder's resolved autopilot decision: its own flag when set,
     * otherwise the client-wide default. Defined below Queen, which it reads.
     */
    bool autopilot_enabled() const;

    /**
     * Claim messages once. They are leased: ack them (Queen::ack), or they come
     * back when the lease expires. With commit_on_delivery() there is nothing
     * to ack.
     *
     * Throws on failure (HttpError, NetworkError, ConflationUnsupportedError):
     * an empty vector means an empty queue.
     */
    std::vector<Message> pop() { return pop_result().messages; }

    /** pop(), plus the broker's account of how it sized the pop (autopilot). */
    PopResult pop_result() {
        return detail::pop_once(*http_client_, pop_target(commit_on_delivery_));
    }

    /**
     * Consume one message per handler call until a stop condition: limit(),
     * idle_millis(), cancel(), a handled signal. Returns what the loop did;
     * throws the first terminal error (a 403, a 4xx on the pop, a conflation
     * fault) once every worker has stopped.
     */
    ConsumeSummary consume(std::function<void(const Message&)> handler);

    /** Consume a whole claimed batch per handler call; one ack (or nack) covers it. */
    ConsumeSummary consume_batch(std::function<void(const std::vector<Message>&)> handler);

    // ---------------------------------------------------------------- reads

    /**
     * Every record of these partitions, read by offset: no lease, no cursor,
     * nothing to ack. From each partition's offset in options.offsets (default
     * 0, the oldest record still kept) up to where the partition ended when it
     * was first read. One FetchedPartition per partition named.
     */
    std::map<std::string, FetchedPartition> fetch(const std::vector<std::string>& partitions,
                                                  const FetchOptions& options = FetchOptions()) {
        require_queue("fetch");
        return reads::fetch(*http_client_, queue_name_, partitions, options);
    }

    /**
     * The partitions written at or after `since` (an ISO-8601 string; empty
     * lists them all), in the order they were created, and the answer's
     * safe_time: pass it as the next `since`.
     */
    ChangedPartitions changed_since(const std::string& since = "") {
        require_queue("changed_since");
        return reads::changed_since(*http_client_, queue_name_, since);
    }

    ChangedPartitions changed_since(std::chrono::system_clock::time_point since) {
        return changed_since(reads::iso_from_micros(
            std::chrono::duration_cast<std::chrono::microseconds>(since.time_since_epoch()).count()));
    }

    /** The dead-letter queue of this queue (and this builder's partition, if any). */
    DLQBuilder dlq(const std::string& consumer_group = "") {
        require_queue("dlq");
        return DLQBuilder(http_client_, queue_name_, consumer_group, partition_or_empty());
    }
};

// ============================================================================
// Queen - the client
// ============================================================================

/**
 * A connected client: one connection pool, one load balancer and one set of
 * push buffers. Create it once per process and share it; it is thread-safe.
 *
 *     queen::Queen queen("http://localhost:6632");
 *     queen.queue("orders").partition("customer-42").push({{"total", 19.99}});
 *     queen.queue("orders").group("billing").batch(10)
 *          .consume([](const queen::Message& m) { std::cout << m.data << "\n"; });
 *     queen.close();
 *
 * Opens no connection until the first request.
 */
class Queen {
private:
    ClientConfig config_;
    std::shared_ptr<HttpClient> http_client_;
    std::shared_ptr<BufferManager> buffer_manager_;
    std::atomic<bool> shutdown_requested_{false};
    // Process-wide kill switch for pop autopilot, read ONCE: re-reading it per
    // request would let a running process change wire shape halfway through.
    bool autopilot_off_ = util::autopilot_disabled_by_env();

    void init() {
        http_client_ = std::make_shared<HttpClient>(config_);
        buffer_manager_ = std::make_shared<BufferManager>(http_client_);
        if (config_.handle_signals) detail::install_signal_handlers();
    }

    static void require_ackable(const Message& message) {
        if (message.partition_id.empty()) {
            throw std::invalid_argument(
                "cannot ack a message with no partition id: a transactionId alone is not unique "
                "across partitions, so this could commit a cursor for a message you never saw");
        }
        if (message.transaction_id.empty()) {
            throw std::invalid_argument("cannot ack a message with no transaction id");
        }
    }

public:
    /** One broker. */
    explicit Queen(const std::string& url) {
        config_.urls = {url};
        init();
    }

    /** One broker, with options (bearer token, timeouts, TLS, wire forms). */
    Queen(const std::string& url, const ClientConfig& config) : config_(config) {
        config_.urls = {url};
        init();
    }

    /** Several brokers (one cluster): load-balanced, with failover. */
    Queen(const std::vector<std::string>& urls, const ClientConfig& config = ClientConfig())
        : config_(config) {
        config_.urls = urls;
        init();
    }

    Queen(const Queen&) = delete;
    Queen& operator=(const Queen&) = delete;

    const ClientConfig& config() const { return config_; }

    /** Start a call chain on a queue (empty: a discovery pop by namespace/task). */
    QueueBuilder queue(const std::string& name = "") {
        return QueueBuilder(this, http_client_, buffer_manager_, name);
    }

    /** One step, all or nothing: acks, pushes, KV writes and timers in one log entry. */
    TransactionBuilder transaction() {
        return TransactionBuilder(http_client_);
    }

    /**
     * Key/value state in the same broker as the queue, bound to one namespace
     * (`^[a-z0-9][a-z0-9._-]{0,63}$`). To write state atomically with an ack,
     * which is what it is for, use the riders on transaction().
     */
    KvBuilder kv(const std::string& ns) {
        return KvBuilder(http_client_, ns);
    }

    /**
     * A lock: one holder at a time, held as a lease of `ttl_seconds`, with a
     * fencing token. Creating the handle sends nothing; acquire() takes it and
     * starts its background renewal. It is a lease, not a mutex:
     * TransactionBuilder::guard is what makes the WORK exclusive.
     */
    Lock lock(const std::string& name, long long ttl_seconds,
              const LockOptions& options = LockOptions()) {
        return Lock(http_client_, name, ttl_seconds, 1, options);
    }

    /**
     * A semaphore: at most `limit` holders at a time. One handle is ONE
     * permit; make a handle per holder. Every holder of one name passes the
     * same limit -- it is stored nowhere.
     */
    Lock semaphore(const std::string& name, int limit, long long ttl_seconds,
                   const LockOptions& options = LockOptions()) {
        return Lock(http_client_, name, ttl_seconds, limit, options);
    }

    /** The four lock operations with no state kept: `get` (who holds it?) and hand-rolled loops. */
    LocksBuilder locks() {
        return LocksBuilder(http_client_);
    }

    /** Scheduled deliveries: a message that becomes real later. */
    TimersBuilder timers() {
        return TimersBuilder(http_client_);
    }

    /** RAM-class queues whose contents survive nothing (read EphemeralBuilder first). */
    EphemeralBuilder ephemeral() {
        return EphemeralBuilder(http_client_);
    }

    /** Management and observability routes. */
    Admin admin() {
        return Admin(http_client_);
    }

    // --------------------------------------------------------------- acking

    /**
     * Settle a message. The consumer group and the lease come from the message
     * itself. A refusal (the lease expired or was released, the message is
     * already settled) is a value -- AckResult::success false -- not an
     * exception; a call that could not be made throws.
     */
    AckResult ack_with(const Message& message, AckStatus status, const std::string& reason = "") {
        require_ackable(message);
        json body = {
            {"transactionId", message.transaction_id},
            {"partitionId", message.partition_id},
            {"status", to_string(status)},
            {"error", reason.empty() ? json(nullptr) : json(util::nack_error_text(reason))},
            {"consumerGroup", util::group_on_wire(message.consumer_group)}
        };
        if (!message.lease_id.empty()) body["leaseId"] = message.lease_id;
        return util::parse_ack_results(http_client_->post("/api/v1/ack", body), 1)[0];
    }

    /** Mark a message completed: its group's cursor moves past it. */
    AckResult ack(const Message& message) {
        return ack_with(message, AckStatus::Completed);
    }

    /**
     * Mark a message failed: it is redelivered until the queue's retryLimit is
     * spent, then dead-lettered; `reason` is kept on the dead-letter row.
     */
    AckResult nack(const Message& message, const std::string& reason) {
        return ack_with(message, AckStatus::Failed, reason);
    }

    /**
     * Settle several messages in one request. The batch route carries one
     * consumer group, so every message must belong to the same one.
     */
    std::vector<AckResult> ack_all_with(const std::vector<Message>& messages, AckStatus status,
                                        const std::string& reason = "") {
        if (messages.empty()) return {};
        const std::string& group = messages[0].consumer_group;
        json acknowledgments = json::array();
        for (const auto& message : messages) {
            require_ackable(message);
            if (message.consumer_group != group) {
                throw std::invalid_argument(
                    "cannot batch-ack across consumer groups: the batch carries one group, but got "
                    "both '" + group + "' and '" + message.consumer_group + "'");
            }
            json item = {
                {"transactionId", message.transaction_id},
                {"partitionId", message.partition_id},
                {"status", to_string(status)},
                {"error", reason.empty() ? json(nullptr) : json(util::nack_error_text(reason))}
            };
            if (!message.lease_id.empty()) item["leaseId"] = message.lease_id;
            acknowledgments.push_back(std::move(item));
        }
        json body = {
            {"acknowledgments", std::move(acknowledgments)},
            {"consumerGroup", util::group_on_wire(group)}
        };
        return util::parse_ack_results(http_client_->post("/api/v1/ack/batch", body), messages.size());
    }

    std::vector<AckResult> ack_all(const std::vector<Message>& messages) {
        return ack_all_with(messages, AckStatus::Completed);
    }

    std::vector<AckResult> nack_all(const std::vector<Message>& messages, const std::string& reason) {
        return ack_all_with(messages, AckStatus::Failed, reason);
    }

    /**
     * Extend a lease by the broker's default (60 s). Every message of one pop
     * shares its lease, a multi-partition claim included, so one call covers
     * the batch. success false: the lease is gone.
     */
    RenewResult renew(const std::string& lease_id) {
        if (lease_id.empty()) {
            throw std::invalid_argument("renew needs a lease id (a commit-on-delivery pop has none)");
        }
        json answer = http_client_->post("/api/v1/lease/" + util::url_encode(lease_id) + "/extend",
                                         json::object());
        return util::parse_renew(answer, lease_id);
    }

    RenewResult renew(const Message& message) { return renew(message.lease_id); }

    /** One renewal per distinct lease among `messages`, in first-seen order. */
    std::vector<RenewResult> renew(const std::vector<Message>& messages) {
        std::vector<RenewResult> out;
        std::unordered_set<std::string> seen;
        for (const auto& message : messages) {
            if (!message.lease_id.empty() && seen.insert(message.lease_id).second) {
                out.push_back(renew(message.lease_id));
            }
        }
        return out;
    }

    // -------------------------------------------------------------- buffers

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
     * How long close() keeps retrying failing flush batches before giving up:
     * a SIGTERM grace period is finite, so shutdown stops after this deadline
     * and reports what is left instead of hanging.
     */
    static constexpr int CLOSE_FLUSH_DEADLINE_MILLIS = 30000;

    /** Flush the push buffers (30 s at most), stop them, and close idle connections. */
    void close() {
        shutdown_requested_ = true;
        try {
            buffer_manager_->flush_all_buffers(CLOSE_FLUSH_DEADLINE_MILLIS);
        } catch (const std::exception& e) {
            // BufferFlushError::what() carries the unflushed count. Loud.
            util::log_warn_always("Queen.close", std::string("buffers not flushed: ") + e.what());
        }
        buffer_manager_->cleanup();
        http_client_->close_idle_connections();
    }

    /** close() was called, or a handled signal arrived (ClientConfig::handle_signals). */
    bool is_shutdown_requested() const {
        return shutdown_requested_.load() ||
               (config_.handle_signals && detail::signal_count().load() > 0);
    }

    /**
     * Block until SIGINT or SIGTERM arrives (ClientConfig::handle_signals) or
     * close() is called -- then call close() to flush, and return from main.
     * Throws std::logic_error without handle_signals: it would block forever.
     */
    void wait_for_signal() const {
        if (!config_.handle_signals) {
            throw std::logic_error("wait_for_signal() needs ClientConfig::handle_signals = true");
        }
        while (!is_shutdown_requested()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    /**
     * Whether pop autopilot is off for this client because the environment
     * asked (util::ENV_POP_AUTOPILOT). A per-builder autopilot(..) outranks it.
     */
    bool is_autopilot_off() const {
        return autopilot_off_;
    }

    /** The transport, for a route this client has no call for. */
    std::shared_ptr<HttpClient> get_http_client() const {
        return http_client_;
    }
};

/** The 2.7.0 name of Queen. */
using QueenClient = Queen;

// ============================================================================
// LeaseRenewal - keeps one popped batch's lease alive while it is handled
// ============================================================================

/**
 * consume()'s renew_lease(): while a popped batch is handled, renew its lease
 * every interval. One request per DISTINCT lease per tick: every message of a
 * pop shares its lease, a multi-partition claim included. The broker extends
 * by its default (60 s), so the interval has to be shorter than that and than
 * the queue's leaseTime for renewal to keep anything alive.
 *
 * Scoped: the destructor stops the timer and joins it, so renewal ends with the
 * batch, after its ack or nack, however the processing block is left. A failed
 * renewal is logged and never stops the consumer.
 */
class LeaseRenewal {
private:
    Queen* queen_;
    std::vector<std::string> lease_ids_;
    std::chrono::milliseconds interval_;
    std::mutex mutex_;
    std::condition_variable wake_;
    bool stopped_ = false;
    std::thread thread_;

    // Runs on the renewal thread, so nothing may leave it.
    void renew_once() noexcept {
        for (const auto& lease_id : lease_ids_) {
            try {
                RenewResult result = queen_->renew(lease_id);
                if (!result.success) {
                    util::log_error("ConsumerManager.leaseRenewal", "lease " + lease_id + ": " + result.error);
                }
            } catch (const std::exception& e) {
                util::log_error("ConsumerManager.leaseRenewal", e.what());
            } catch (...) {
                util::log_error("ConsumerManager.leaseRenewal", "renewal failed");
            }
        }
    }

public:
    LeaseRenewal(Queen* queen, const std::vector<Message>& messages, int interval_millis)
        : queen_(queen), interval_(interval_millis) {
        std::unordered_set<std::string> seen;
        for (const auto& message : messages) {
            // A commit-on-delivery message has no lease: nothing to renew.
            if (!message.lease_id.empty() && seen.insert(message.lease_id).second) {
                lease_ids_.push_back(message.lease_id);
            }
        }
        if (lease_ids_.empty() || interval_millis <= 0) return;
        try {
            thread_ = std::thread([this]() {
                try {
                    std::unique_lock<std::mutex> lock(mutex_);
                    while (!wake_.wait_for(lock, interval_, [this]() { return stopped_; })) {
                        lock.unlock();
                        renew_once();
                        lock.lock();
                    }
                } catch (...) {
                    util::log_error("ConsumerManager.leaseRenewal", "renewal stopped");
                }
            });
        } catch (const std::exception& e) {
            // No thread to renew with: handle the batch without renewal rather
            // than stop the consumer over it.
            util::log_error("ConsumerManager.leaseRenewal",
                            std::string("renewal not started for this batch: ") + e.what());
        }
    }

    ~LeaseRenewal() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopped_ = true;
        }
        wake_.notify_all();
        if (thread_.joinable()) thread_.join();
    }

    LeaseRenewal(const LeaseRenewal&) = delete;
    LeaseRenewal& operator=(const LeaseRenewal&) = delete;
};

// ============================================================================
// ConsumerManager implementation (after Queen is defined)
// ============================================================================

inline bool ConsumerManager::cancelled(const Shared& shared) const {
    return (shared.options.cancel && shared.options.cancel->is_cancelled()) ||
           (queen_ && queen_->is_shutdown_requested());
}

/**
 * Ack or nack one message after its handler ran; returns whether the handler
 * succeeded. With auto_ack off nothing is sent. The broker refuses an ack with
 * HTTP 200 and success false on the item, so the verdict is read and a refusal
 * logged; the counters record what the loop decided.
 */
inline bool ConsumerManager::settle(Shared& shared, const Message& message,
                                    const std::optional<std::string>& failure) {
    if (!shared.options.auto_ack) {
        if (failure) {
            util::log_error("ConsumerManager", "handler failed (auto_ack off, not nacked; the "
                            "broker delivers it again when its lease expires): " +
                            util::nack_error_text(*failure));
        }
        return !failure;
    }
    if (!failure) {
        try {
            AckResult result = queen_->ack(message);
            if (!result.success) {
                util::log_error("ConsumerManager", "ack of " + message.transaction_id +
                                " refused by the broker: " + result.error);
            }
        } catch (const std::exception& e) {
            // Not delivered: the lease expires and the broker redelivers it.
            util::log_error("ConsumerManager", "ack of " + message.transaction_id + " failed: " + e.what());
        }
        shared.acked++;
        return true;
    }
    const std::string reason = util::nack_error_text(*failure);
    util::log_error("ConsumerManager", "handler failed: " + reason);
    try {
        AckResult result = queen_->nack(message, reason);
        if (!result.success) {
            util::log_error("ConsumerManager", "nack of " + message.transaction_id +
                            " refused by the broker: " + result.error);
        }
    } catch (const std::exception& e) {
        util::log_error("ConsumerManager", "nack of " + message.transaction_id + " failed: " + e.what());
    }
    shared.nacked++;
    return false;
}

/** Settle a whole batch with one request. The same rules as settle(). */
inline void ConsumerManager::settle_batch(Shared& shared, const std::vector<Message>& messages,
                                          const std::optional<std::string>& failure) {
    if (messages.empty()) return;
    if (!shared.options.auto_ack) {
        if (failure) {
            util::log_error("ConsumerManager", "batch handler failed (auto_ack off, not nacked): " +
                            util::nack_error_text(*failure));
        }
        return;
    }
    const AckStatus status = failure ? AckStatus::Failed : AckStatus::Completed;
    const std::string reason = failure ? util::nack_error_text(*failure) : std::string();
    if (failure) util::log_error("ConsumerManager", "batch handler failed: " + reason);
    try {
        auto results = queen_->ack_all_with(messages, status, reason);
        size_t refused = 0;
        std::string first;
        for (const auto& r : results) {
            if (!r.success && refused++ == 0) first = r.error;
        }
        if (refused > 0) {
            util::log_error("ConsumerManager", std::to_string(refused) + " of " +
                            std::to_string(results.size()) + " settled messages refused by the broker: " +
                            first);
        }
    } catch (const std::exception& e) {
        util::log_error("ConsumerManager", std::string("batch settle failed: ") + e.what());
    }
    if (failure) {
        shared.nacked += messages.size();
    } else {
        shared.acked += messages.size();
    }
}

inline void ConsumerManager::worker(int id, Shared& shared, const Process& process) {
    struct Observation {
        std::shared_ptr<ConsumerSupervision> reporter;
        explicit Observation(std::shared_ptr<ConsumerSupervision> r) : reporter(std::move(r)) {
            if (reporter) reporter->worker_started();
        }
        ~Observation() {
            if (reporter) reporter->worker_exited();
        }
    } observation(shared.supervision);

    const std::string who = "Worker " + std::to_string(id);
    auto last_message = std::chrono::steady_clock::now();

    while (true) {
        if (shared.stop.load()) break;
        if (cancelled(shared)) {
            stop_with(shared, StopReason::Cancelled);
            break;
        }
        if (limit_reached(shared)) {
            stop_with(shared, StopReason::Limit);
            break;
        }
        if (shared.options.idle_millis > 0 &&
            std::chrono::steady_clock::now() - last_message >=
                std::chrono::milliseconds(shared.options.idle_millis)) {
            stop_with(shared, StopReason::Idle);
            break;
        }

        PopResult popped;
        const auto polled_at = std::chrono::steady_clock::now();
        try {
            popped = detail::pop_once(*http_client_, shared.target);
        } catch (const ConflationUnsupportedError& e) {
            // Terminal, like a 403: retrying cannot make an old broker
            // conflate, and every further poll would hand the handler another
            // message off a backlog the caller believes is conflated away.
            util::log_error("ConsumerManager", who + " stopping: " + e.what());
            record_error(shared);
            break;
        } catch (const HttpError& e) {
            if (e.status_code() == 403) {
                // cluster_suspended, storage_quota_exceeded, feature_gated,
                // forbidden: none resolves itself.
                util::log_error("ConsumerManager", who + " forbidden (code=" + e.code() + "): " + e.what());
                record_error(shared);
                break;
            }
            if (e.is_retryable()) {
                // A 429 whose budget was spent, or a 5xx the transport's own
                // retries did not outlast (a broker restarting, an election).
                const int delay = e.status_code() == 429 && e.retry_after_seconds()
                    ? static_cast<int>(*e.retry_after_seconds() * 1000) : 1000;
                util::log_warn("ConsumerManager", who + " pop failed (status=" +
                               std::to_string(e.status_code()) + "): " + e.what() +
                               ", retrying in " + std::to_string(delay) + "ms");
                if (pause(shared, delay)) {
                    if (!shared.stop.load()) stop_with(shared, StopReason::Cancelled);
                    break;
                }
                continue;
            }
            // Any other 4xx: a request the broker will never serve.
            util::log_error("ConsumerManager", who + " pop refused (status=" +
                            std::to_string(e.status_code()) + "): " + e.what());
            record_error(shared);
            break;
        } catch (const NetworkError& e) {
            util::log_warn("ConsumerManager", who + " pop failed: " + std::string(e.what()) +
                           ", retrying in 1000ms");
            if (pause(shared, 1000)) {
                if (!shared.stop.load()) stop_with(shared, StopReason::Cancelled);
                break;
            }
            continue;
        } catch (...) {
            record_error(shared);
            break;
        }

        if (popped.messages.empty()) {
            // A waiting pop parks on the broker, so it needs no pause of its
            // own -- unless it came back empty long before its window (a proxy
            // that does not hold the long poll), which would make the loop spin.
            const auto waited = std::chrono::steady_clock::now() - polled_at;
            const bool answered_early =
                waited < std::chrono::milliseconds(std::min(1000, shared.target.timeout_millis / 2));
            if (!shared.target.wait || answered_early) {
                // The broker's advised pacing when this pop engaged autopilot,
                // otherwise the historical 100 ms.
                if (pause(shared, util::empty_poll_delay_millis(popped.autopilot))) {
                    if (!shared.stop.load()) stop_with(shared, StopReason::Cancelled);
                    break;
                }
            }
            continue;
        }

        last_message = std::chrono::steady_clock::now();

        // Keep the claim alive while the handler runs; scoped to this batch.
        std::optional<LeaseRenewal> renewal;
        if (shared.options.renew_lease && shared.options.renew_lease_interval_millis > 0) {
            renewal.emplace(queen_, popped.messages, shared.options.renew_lease_interval_millis);
        }
        try {
            process(shared, popped.messages);
        } catch (...) {
            record_error(shared);
            break;
        }
    }
}

inline ConsumeSummary ConsumerManager::run_loop(ConsumeOptions options, Process process) {
    if (options.queue.empty() && options.namespace_name.empty() && options.task.empty()) {
        throw std::invalid_argument("consume needs a queue, or a namespace/task to discover one");
    }
    Shared shared;
    shared.options = options;
    shared.target.queue = options.queue;
    shared.target.partition = options.partition == util::DEFAULT_PARTITION ? std::string()
                                                                           : options.partition;
    shared.target.namespace_name = options.namespace_name;
    shared.target.task = options.task;
    shared.target.group = options.group;
    shared.target.batch = options.batch;
    shared.target.max_partitions = options.max_partitions;
    shared.target.autopilot = options.autopilot.value_or(!(queen_ && queen_->is_autopilot_off()));
    shared.target.wait = options.wait;
    shared.target.timeout_millis = options.timeout_millis;
    shared.target.subscription_mode = options.subscription_mode;
    shared.target.subscription_from = options.subscription_from;
    shared.target.conflation = options.conflation;
    shared.target.commit_on_delivery = false;  // consume always leases
    (void)detail::pop_path(shared.target);     // a bad target throws here, not in a worker

    if (options.supervision) {
        shared.supervision = std::make_shared<ConsumerSupervision>(http_client_, options);
        shared.supervision->start();
    }

    const int workers = std::max(1, options.concurrency);
    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(workers));
    for (int i = 0; i < workers; ++i) {
        threads.emplace_back([this, i, &shared, &process]() {
            try {
                worker(i, shared, process);
            } catch (...) {
                record_error(shared);
            }
        });
    }
    for (auto& t : threads) t.join();
    if (shared.supervision) shared.supervision->stop();

    if (shared.error) std::rethrow_exception(shared.error);
    ConsumeSummary summary;
    summary.processed = shared.processed.load();
    summary.acked = shared.acked.load();
    summary.nacked = shared.nacked.load();
    summary.stopped_by = shared.reason;
    return summary;
}

inline ConsumeSummary ConsumerManager::run(Handler handler, const ConsumeOptions& options) {
    return run_loop(options, [this, handler](Shared& shared, std::vector<Message>& messages) {
        std::unordered_set<std::string> failed_partitions;
        size_t skipped = 0;
        for (const auto& message : messages) {
            if (should_stop(shared)) break;
            if (failed_partitions.count(message.partition_id) > 0) {
                ++skipped;
                continue;
            }
            uint64_t observed = 0;
            if (shared.supervision) observed = shared.supervision->begin_handler();
            auto failure = run_handler(handler, message);
            if (shared.supervision) shared.supervision->end_handler(observed, !failure);
            const bool ok = settle(shared, message, failure);
            shared.processed++;
            if (!ok && !shared.options.auto_ack) {
                util::log_warn("ConsumerManager", "handler failed; abandoning the rest of this "
                               "batch (it will be redelivered)");
                break;
            }
            if (!ok) failed_partitions.insert(message.partition_id);
            if (limit_reached(shared)) break;
        }
        if (skipped > 0) {
            util::log_warn("ConsumerManager", std::to_string(skipped) + " message(s) left behind "
                           "a failed message in their partition; the broker delivers them again");
        }
    });
}

inline ConsumeSummary ConsumerManager::run_batch(BatchHandler handler, const ConsumeOptions& options) {
    return run_loop(options, [this, handler](Shared& shared, std::vector<Message>& messages) {
        uint64_t observed = 0;
        if (shared.supervision) observed = shared.supervision->begin_handler();
        auto failure = run_handler(handler, messages);
        if (shared.supervision) shared.supervision->end_handler(observed, !failure);
        settle_batch(shared, messages, failure);
        shared.processed += messages.size();
    });
}

// ============================================================================
// QueueBuilder implementation (after Queen and ConsumerManager)
// ============================================================================

inline bool QueueBuilder::autopilot_enabled() const {
    if (autopilot_.has_value()) return *autopilot_;
    return queen_ == nullptr || !queen_->is_autopilot_off();
}

inline ConsumeSummary QueueBuilder::consume(std::function<void(const Message&)> handler) {
    ConsumeOptions options = consume_options();
    ConsumerManager manager(http_client_, queen_);
    return manager.run(std::move(handler), options);
}

inline ConsumeSummary QueueBuilder::consume_batch(
    std::function<void(const std::vector<Message>&)> handler) {
    ConsumeOptions options = consume_options();
    ConsumerManager manager(http_client_, queen_);
    return manager.run_batch(std::move(handler), options);
}

} // namespace queen
