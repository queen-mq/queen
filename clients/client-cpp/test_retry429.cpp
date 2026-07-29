/**
 * Queen C++ Client - Proxy contract test suite
 *
 * Client-side auth / 429 / 403 handling (PLAN_QUEEN_PROXY_CLOUD.md §4/§9,
 * blocker B4 -- "client 429/backoff work ... mandatory pre-enforcement").
 * Mirrors clients/client-go/retry429_test.go and
 * clients/client-js/test-v2/http-unit/retry429.test.js.
 *
 * The proxy error contract under test:
 *
 *   429  Retry-After: <seconds>  {"error", "code": "rate_limited" | "quota_exceeded"}
 *   403                          {"error", "code": "cluster_suspended" | "storage_quota_exceeded"
 *                                                | "feature_gated" | "forbidden"}
 *
 * These run against an in-process httplib::Server on localhost -- no broker,
 * no Postgres, so the suite is self-contained:
 *
 *   make retry429 && ./bin/test_retry429
 */

#include "queen_client.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <vector>
#include <string>

using namespace queen;
using json = nlohmann::json;

#define GREEN "\033[32m"
#define RED "\033[31m"
#define BLUE "\033[34m"
#define RESET "\033[0m"

// ============================================================================
// Test double: serves a canned response plan in request order
// ============================================================================

struct PlannedResponse {
    int status = 200;
    std::string body = R"({"ok":true})";
    std::string retry_after;                 // Retry-After header, when non-empty
};

struct RecordedHit {
    std::string method;
    std::string path;
    std::string authorization;
    long long at_millis;                     // since server start
};

inline PlannedResponse rate_limited_429(const std::string& retry_after = "") {
    return {429, R"({"error":"slow down","code":"rate_limited"})", retry_after};
}

class PlanServer {
private:
    httplib::Server server_;
    std::thread thread_;
    int port_ = 0;
    std::chrono::steady_clock::time_point start_;

    mutable std::mutex mutex_;
    std::vector<PlannedResponse> plan_;
    PlannedResponse fallback_;
    size_t index_ = 0;
    std::vector<RecordedHit> hits_;

public:
    PlanServer(const std::vector<PlannedResponse>& plan, const PlannedResponse& fallback)
        : plan_(plan), fallback_(fallback) {
        auto handler = [this](const httplib::Request& req, httplib::Response& res) {
            PlannedResponse planned;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                planned = index_ < plan_.size() ? plan_[index_] : fallback_;
                ++index_;
                hits_.push_back({req.method, req.path, req.get_header_value("Authorization"),
                                 std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::steady_clock::now() - start_).count()});
            }

            if (!planned.retry_after.empty()) {
                res.set_header("Retry-After", planned.retry_after);
            }
            res.status = planned.status;
            res.set_content(planned.body, "application/json");
        };

        server_.Get(".*", handler);
        server_.Post(".*", handler);
        server_.Put(".*", handler);
        server_.Delete(".*", handler);

        port_ = server_.bind_to_any_port("127.0.0.1");
        start_ = std::chrono::steady_clock::now();
        thread_ = std::thread([this]() { server_.listen_after_bind(); });
        server_.wait_until_ready();
    }

    ~PlanServer() {
        server_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    std::vector<RecordedHit> hits() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return hits_;
    }

    size_t hit_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return hits_.size();
    }
};

// ============================================================================
// Assertions
// ============================================================================

static int failures = 0;

void check(bool condition, const std::string& what) {
    if (!condition) {
        ++failures;
        std::cout << RED << "    x " << what << RESET << std::endl;
    }
}

void run_test(const std::string& name, std::function<void()> test_fn) {
    std::cout << BLUE << "Running: " << name << RESET << std::endl;
    int before = failures;

    try {
        test_fn();
    } catch (const std::exception& e) {
        ++failures;
        std::cout << RED << "    x unexpected exception: " << e.what() << RESET << std::endl;
    }

    if (failures == before) {
        std::cout << GREEN << "PASS: " << name << RESET << std::endl;
    } else {
        std::cout << RED << "FAIL: " << name << RESET << std::endl;
    }
}

// Fast backoff so the bounded-attempt tests don't sleep for minutes.
ClientConfig fast_config(int max_attempts = 0) {
    ClientConfig config;
    config.retry_429.max_attempts = max_attempts;
    config.retry_429.base_millis = 1;
    config.retry_429.cap_millis = 2;
    return config;
}

// ============================================================================
// Tests
// ============================================================================

void test_bearer_token_on_every_request() {
    PlanServer server({}, {200, R"({"messages":[]})", ""});

    ClientConfig config;
    config.bearer_token = "test-token-123";
    QueenClient client({server.url()}, config);

    client.queue("contract-queue").create();
    client.queue("contract-queue").batch(1).wait(false).pop();
    client.queue("contract-queue").del();

    auto hits = server.hits();
    check(hits.size() == 3, "expected 3 requests, got " + std::to_string(hits.size()));
    for (const auto& hit : hits) {
        check(hit.authorization == "Bearer test-token-123",
              hit.method + " " + hit.path + " sent Authorization '" + hit.authorization + "'");
    }
}

void test_no_authorization_header_without_token() {
    PlanServer server({}, {200, R"({"ok":true})", ""});

    QueenClient client(server.url());
    client.queue("contract-queue").create();

    auto hits = server.hits();
    check(hits.size() == 1, "expected 1 request");
    check(hits[0].authorization.empty(), "unauthenticated client must not send Authorization");
}

void test_429_then_success_is_transparent() {
    PlanServer server({rate_limited_429(), rate_limited_429()}, {200, R"({"ok":true})", ""});

    QueenClient client({server.url()}, fast_config());
    json result = client.queue("contract-queue").create();

    check(!result.is_null(), "request should succeed after the 429s are retried away");
    check(server.hit_count() == 3, "expected 3 attempts, got " + std::to_string(server.hit_count()));
}

void test_retry_after_wins_over_exponential_backoff() {
    PlanServer server({rate_limited_429("0.4")}, {200, R"({"ok":true})", ""});

    // base/cap are deliberately tiny: only an honored Retry-After can produce
    // a gap this long.
    QueenClient client({server.url()}, fast_config());
    client.queue("contract-queue").create();

    auto hits = server.hits();
    check(hits.size() == 2, "expected 2 attempts");
    if (hits.size() == 2) {
        long long gap = hits[1].at_millis - hits[0].at_millis;
        // 400ms +-20% jitter.
        check(gap >= 320 && gap <= 520, "Retry-After gap was " + std::to_string(gap) + "ms");
    }
}

void test_ordinary_requests_are_bounded_at_10() {
    PlanServer server({}, rate_limited_429());

    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    try {
        client.queue("contract-queue").create();
    } catch (const HttpError& e) {
        threw = true;
        check(e.status_code() == 429, "status should be 429, got " + std::to_string(e.status_code()));
        check(e.code() == "rate_limited", "code should be rate_limited, got '" + e.code() + "'");
    }

    check(threw, "an exhausted 429 budget must surface as HttpError");
    check(server.hit_count() == 10, "expected 10 attempts, got " + std::to_string(server.hit_count()));
}

void test_long_poll_pop_retries_past_the_ordinary_bound() {
    std::vector<PlannedResponse> plan(14, rate_limited_429());
    PlanServer server(plan, {200, R"({"messages":[{"id":"m1"}]})", ""});

    QueenClient client({server.url()}, fast_config());
    json messages = client.queue("contract-queue").batch(1).wait(true).pop();

    check(messages.size() == 1, "long-poll pop should eventually receive its message");
    check(server.hit_count() == 15, "expected 15 attempts, got " + std::to_string(server.hit_count()));
}

void test_non_waiting_pop_stays_bounded() {
    PlanServer server({}, rate_limited_429());

    QueenClient client({server.url()}, fast_config());
    // pop() swallows errors and returns [] -- the attempt count is what proves
    // a non-waiting pop uses the ordinary bounded policy.
    json messages = client.queue("contract-queue").batch(1).wait(false).pop();

    check(messages.empty(), "pop should swallow the exhausted 429 and return []");
    check(server.hit_count() == 10, "expected 10 attempts, got " + std::to_string(server.hit_count()));
}

void test_429_does_not_fail_over_to_another_backend() {
    PlanServer server_a({}, rate_limited_429());
    PlanServer server_b({}, rate_limited_429());

    QueenClient client({server_a.url(), server_b.url()}, fast_config(3));

    try {
        client.queue("contract-queue").create();
    } catch (const HttpError&) {
        // expected
    }

    size_t hits_a = server_a.hit_count();
    size_t hits_b = server_b.hit_count();
    // Rate limiting is a tenant-quota signal, not a backend-health one: all
    // three attempts must land on whichever backend was picked first.
    check((hits_a == 3 && hits_b == 0) || (hits_a == 0 && hits_b == 3),
          "429 leaked across backends: a=" + std::to_string(hits_a) + " b=" + std::to_string(hits_b));
}

void test_terminal_403_codes_are_surfaced() {
    const std::vector<std::string> codes = {
        "cluster_suspended", "storage_quota_exceeded", "feature_gated", "forbidden"
    };

    for (const auto& code : codes) {
        PlanServer server({}, {403, R"({"error":"nope","code":")" + code + R"("})", ""});

        QueenClient client({server.url()}, fast_config());

        bool threw = false;
        try {
            client.queue("contract-queue").create();
        } catch (const HttpError& e) {
            threw = true;
            check(e.status_code() == 403, code + ": status should be 403");
            check(e.code() == code, code + ": code should round-trip, got '" + e.code() + "'");
            check(e.is_cluster_suspended() == (code == "cluster_suspended"),
                  code + ": is_cluster_suspended() misreported");
            check(std::string(e.what()) == "nope", code + ": what() should be the server message");
        }

        check(threw, code + ": a 403 must surface as HttpError");
        check(server.hit_count() == 1, code + ": a 403 must not be retried");
    }
}

void test_quota_exceeded_429_code_is_surfaced() {
    PlanServer server({}, {429, R"({"error":"over quota","code":"quota_exceeded"})", "0"});

    QueenClient client({server.url()}, fast_config(1));

    bool threw = false;
    try {
        client.queue("contract-queue").create();
    } catch (const HttpError& e) {
        threw = true;
        check(e.code() == "quota_exceeded", "code should be quota_exceeded, got '" + e.code() + "'");
        check(e.retry_after_seconds().has_value() && *e.retry_after_seconds() == 0.0,
              "Retry-After: 0 should parse to 0, not be dropped");
    }
    check(threw, "a 429 must surface as HttpError once its budget is exhausted");
}

void test_consumer_stops_on_terminal_403() {
    PlanServer server({}, {403, R"({"error":"cluster suspended","code":"cluster_suspended"})", ""});

    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    try {
        client.queue("contract-queue").wait(true).consume([](const json&) {});
    } catch (const HttpError& e) {
        threw = true;
        check(e.is_cluster_suspended(), "consume() should surface the cluster_suspended 403");
    }

    check(threw, "consume() must stop and rethrow on a terminal 403");
    check(server.hit_count() == 1, "a terminal 403 must not be re-polled, got " +
          std::to_string(server.hit_count()) + " hits");
}

void test_consumer_backs_off_on_an_exhausted_429() {
    std::vector<PlannedResponse> plan(3, rate_limited_429("0.05"));
    PlanServer server(plan, {200, R"({"messages":[{"id":"m1"}]})", ""});

    // max_attempts=1 disables the in-client retry, so the consumer loop's own
    // 429 fallback is what has to keep the poll alive.
    QueenClient client({server.url()}, fast_config(1));
    client.queue("contract-queue").wait(true).limit(1).consume([](const json&) {});

    check(server.hit_count() == 4, "consumer should poll through the 429s, got " +
          std::to_string(server.hit_count()) + " hits");
}

void test_backoff_is_exponential_capped_and_jittered() {
    // No Retry-After: base * 2^attempt, capped, +-20%.
    for (int attempt = 0; attempt < 4; ++attempt) {
        int expected = 500 << attempt;
        int delay = util::compute_retry429_delay_millis(attempt, std::nullopt, 500, 30000);
        check(delay >= static_cast<int>(expected * 0.8) && delay <= static_cast<int>(expected * 1.2),
              "attempt " + std::to_string(attempt) + " delay " + std::to_string(delay) +
              "ms outside +-20% of " + std::to_string(expected) + "ms");
    }

    int capped = util::compute_retry429_delay_millis(20, std::nullopt, 500, 30000);
    check(capped >= 24000 && capped <= 36000, "capped delay was " + std::to_string(capped) + "ms");

    // Retry-After (seconds) wins, still jittered.
    int honored = util::compute_retry429_delay_millis(5, 2.0, 500, 30000);
    check(honored >= 1600 && honored <= 2400, "Retry-After delay was " + std::to_string(honored) + "ms");

    // A negative Retry-After is ignored in favour of the exponential fallback.
    int negative = util::compute_retry429_delay_millis(0, -1.0, 500, 30000);
    check(negative >= 400 && negative <= 600, "negative Retry-After delay was " +
          std::to_string(negative) + "ms");
}

// ============================================================================

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "Queen C++ Client - Proxy Contract Tests" << std::endl;
    std::cout << "========================================\n" << std::endl;

    run_test("Bearer token on every request", test_bearer_token_on_every_request);
    run_test("No Authorization header without a token", test_no_authorization_header_without_token);
    run_test("429 then success is transparent", test_429_then_success_is_transparent);
    run_test("Retry-After wins over exponential backoff", test_retry_after_wins_over_exponential_backoff);
    run_test("Ordinary requests are bounded at 10 attempts", test_ordinary_requests_are_bounded_at_10);
    run_test("Long-poll pop retries past the ordinary bound", test_long_poll_pop_retries_past_the_ordinary_bound);
    run_test("Non-waiting pop stays bounded", test_non_waiting_pop_stays_bounded);
    run_test("429 does not fail over to another backend", test_429_does_not_fail_over_to_another_backend);
    run_test("Terminal 403 codes are surfaced", test_terminal_403_codes_are_surfaced);
    run_test("quota_exceeded 429 code is surfaced", test_quota_exceeded_429_code_is_surfaced);
    run_test("Consumer stops on a terminal 403", test_consumer_stops_on_terminal_403);
    run_test("Consumer backs off on an exhausted 429", test_consumer_backs_off_on_an_exhausted_429);
    run_test("Backoff is exponential, capped and jittered", test_backoff_is_exponential_capped_and_jittered);

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << GREEN << "All proxy contract tests passed" << RESET << std::endl;
    } else {
        std::cout << RED << failures << " check(s) failed" << RESET << std::endl;
    }

    return failures > 0 ? 1 : 0;
}
