/**
 * Queen C++ Client - Client tokens test suite
 *
 * ClientConfig::token_source, a function that fetches the bearer token: the
 * token is kept until a client token nears its `exp`, and fetched again after
 * a 401, when the request is sent once more (a second 401 is the caller's).
 * One fetch at a time, shared by every connection of the client. A fixed
 * bearer_token is never retried on a 401. And Queen::tokens().mint() on the
 * wire (POST /api/v1/tokens). Mirrors
 * clients/client-js/test-v2/http-unit/tokens.test.js and
 * clients/client-rust/tests/tokens_wire.rs.
 *
 * These run against an in-process httplib::Server on localhost, like
 * test_retry429.cpp -- no broker, so the suite is self-contained:
 *
 *   make tokens && ./bin/test_tokens
 */

#include "queen_client.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <atomic>
#include <vector>
#include <string>

using namespace queen;
using json = nlohmann::json;

#define GREEN "\033[32m"
#define RED "\033[31m"
#define BLUE "\033[34m"
#define RESET "\033[0m"

// ============================================================================
// Test double: answers each request with what a responder says
// ============================================================================

struct PlannedResponse {
    int status = 200;
    std::string body = R"({"ok":true})";
};

struct RecordedHit {
    std::string method;
    std::string path;
    std::string authorization;
    size_t authorization_count = 0;          // copies of the header on the request
    std::string body;
};

inline PlannedResponse token_expired_401() {
    return {401, R"({"error":"client token expired","code":"token_expired"})"};
}

inline PlannedResponse no_messages() {
    return {200, R"({"messages":[]})"};
}

class PlanServer {
public:
    /// Called for every request, in arrival order, under the server's lock.
    using Responder = std::function<PlannedResponse(const RecordedHit&)>;

private:
    httplib::Server server_;
    std::thread thread_;
    int port_ = 0;

    mutable std::mutex mutex_;
    Responder responder_;
    std::vector<RecordedHit> hits_;

public:
    explicit PlanServer(Responder responder) : responder_(std::move(responder)) {
        auto handler = [this](const httplib::Request& req, httplib::Response& res) {
            RecordedHit hit{req.method, req.path, req.get_header_value("Authorization"),
                            req.get_header_value_count("Authorization"), req.body};
            PlannedResponse planned;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                hits_.push_back(hit);
                planned = responder_(hit);
            }
            res.status = planned.status;
            res.set_content(planned.body, "application/json");
        };

        server_.Get(".*", handler);
        server_.Post(".*", handler);
        server_.Put(".*", handler);
        server_.Delete(".*", handler);

        port_ = server_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this]() { server_.listen_after_bind(); });
        server_.wait_until_ready();
    }

    /** Answers `plan` in request order, then `fallback`. */
    PlanServer(const std::vector<PlannedResponse>& plan, const PlannedResponse& fallback)
        : PlanServer([plan, fallback, index = size_t{0}](const RecordedHit&) mutable {
              return index < plan.size() ? plan[index++] : fallback;
          }) {
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
// Tokens
// ============================================================================

/** Base64url without padding, the JWT segment encoding. */
std::string base64url(const std::string& raw) {
    std::string out = util::base64_encode(raw);
    for (char& c : out) {
        if (c == '+') c = '-';
        if (c == '/') c = '_';
    }
    while (!out.empty() && out.back() == '=') out.pop_back();
    return out;
}

long long now_seconds() {
    return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

/** A client token whose middle segment is `claims` (the signature is fake). */
std::string client_token_with(const json& claims) {
    return std::string(util::CLIENT_TOKEN_PREFIX) + base64url(json{{"alg", "HS256"}}.dump()) + "." +
           base64url(claims.dump()) + ".sig";
}

/** A client token whose `exp` is `seconds_from_now` away. */
std::string client_token(long long seconds_from_now, const std::string& sub = "dev1") {
    return client_token_with({{"sub", sub}, {"exp", now_seconds() + seconds_from_now}, {"typ", "client"}});
}

/** A source answering token-0, token-1, ... and counting its calls in `calls`. */
std::function<std::string()> counting_source(const std::shared_ptr<std::atomic<int>>& calls) {
    return [calls]() { return "token-" + std::to_string(calls->fetch_add(1)); };
}

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

/** The request a device makes most: a pop of its own partition. */
size_t pop(QueenClient& client) {
    return client.queue("commands").partition("dev1").batch(1).wait(false).pop().size();
}

// ============================================================================
// Tests
// ============================================================================

void test_a_401_fetches_a_new_token_and_sends_the_request_again() {
    PlanServer server({token_expired_401(), no_messages()}, no_messages());
    auto calls = std::make_shared<std::atomic<int>>(0);

    ClientConfig config;
    config.token_source = counting_source(calls);
    // The one more try after a 401 spends none of the 5xx/network attempts.
    config.retry_attempts = 1;
    QueenClient client(server.url(), config);

    check(pop(client) == 0, "the pop should succeed once the new token is sent");
    auto hits = server.hits();
    check(hits.size() == 2, "expected 2 requests, got " + std::to_string(hits.size()));
    if (hits.size() == 2) {
        check(hits[0].authorization == "Bearer token-0",
              "the first try sent '" + hits[0].authorization + "'");
        check(hits[1].authorization == "Bearer token-1",
              "the retry should carry the new token, sent '" + hits[1].authorization + "'");
        check(hits[1].path == hits[0].path, "the retry should be the same request");
    }
    check(calls->load() == 2, "expected 2 calls of the function, got " + std::to_string(calls->load()));

    // The new token is kept: the next request fetches nothing.
    pop(client);
    check(calls->load() == 2, "the next request fetched again: " + std::to_string(calls->load()) + " calls");
    hits = server.hits();
    check(hits.size() == 3 && hits[2].authorization == "Bearer token-1",
          "the next request should carry the kept token");
}

void test_a_second_401_is_the_callers() {
    PlanServer server({token_expired_401(), token_expired_401(), token_expired_401()}, no_messages());
    auto calls = std::make_shared<std::atomic<int>>(0);

    ClientConfig config;
    config.token_source = counting_source(calls);
    QueenClient client(server.url(), config);

    bool threw = false;
    try {
        pop(client);
    } catch (const HttpError& e) {
        threw = true;
        check(e.status_code() == 401, "status should be 401, got " + std::to_string(e.status_code()));
        check(e.code() == "token_expired", "code should be token_expired, got '" + e.code() + "'");
    }
    check(threw, "a second 401 must reach the caller as HttpError");
    check(server.hit_count() == 2, "one refresh, one retry: expected 2 requests, got " +
          std::to_string(server.hit_count()));
    check(calls->load() == 2, "expected 2 calls of the function, got " + std::to_string(calls->load()));
}

void test_a_fixed_token_is_not_retried_on_a_401() {
    PlanServer server({token_expired_401()}, no_messages());

    ClientConfig config;
    config.bearer_token = "qk_live_static";
    QueenClient client(server.url(), config);

    bool threw = false;
    try {
        pop(client);
    } catch (const HttpError& e) {
        threw = true;
        check(e.status_code() == 401, "status should be 401, got " + std::to_string(e.status_code()));
    }
    check(threw, "a 401 with a fixed token must reach the caller");
    auto hits = server.hits();
    check(hits.size() == 1, "a fixed token must not be retried, got " + std::to_string(hits.size()) +
          " requests");
    check(!hits.empty() && hits[0].authorization == "Bearer qk_live_static",
          "the fixed token should be sent as it is");
}

void test_the_function_is_called_once_across_requests() {
    const std::string token = client_token(3600);
    PlanServer server([](const RecordedHit& hit) -> PlannedResponse {
        if (hit.path == "/api/v1/tokens") {
            return {201, json{{"token", client_token(600, "dev-2")}, {"sub", "dev-2"}, {"key", "k1"},
                              {"expiresAt", "2026-11-11T12:00:00+00:00"}, {"expiresIn", 600}}.dump()};
        }
        if (hit.path == "/api/v1/kv") return {200, R"({"results":[{"found":false,"key":"k"}]})"};
        if (hit.path.rfind("/api/v1/pop", 0) == 0) return no_messages();
        return {200, R"({"ok":true})"};
    });
    auto calls = std::make_shared<std::atomic<int>>(0);

    ClientConfig config;
    config.token_source = [calls, token]() {
        calls->fetch_add(1);
        return token;
    };
    QueenClient client(server.url(), config);

    client.queue("orders").create();
    pop(client);
    client.kv("devices").get("k");
    client.tokens().mint({"dev-2", 600});
    client.admin().health();
    // The consumer status reports go out on a connection of their own.
    client.get_http_client()->post_observation("/api/v1/kv", {{"operations", json::array()}});

    check(calls->load() == 1, "expected 1 call of the function, got " + std::to_string(calls->load()));
    auto hits = server.hits();
    check(hits.size() == 6, "expected 6 requests, got " + std::to_string(hits.size()));
    for (const auto& hit : hits) {
        const std::string where = hit.method + " " + hit.path;
        check(hit.authorization == "Bearer " + token, where + " sent Authorization '" +
              hit.authorization + "'");
        check(hit.authorization_count == 1,
              where + " sent Authorization " + std::to_string(hit.authorization_count) + " times");
    }
}

void test_a_client_token_near_its_exp_is_fetched_again() {
    PlanServer server({}, no_messages());

    // An hour left: kept.
    auto long_calls = std::make_shared<std::atomic<int>>(0);
    ClientConfig long_lived;
    long_lived.token_source = [long_calls]() {
        long_calls->fetch_add(1);
        return client_token(3600);
    };
    QueenClient keeps(server.url(), long_lived);
    for (int i = 0; i < 3; ++i) pop(keeps);
    check(long_calls->load() == 1, "a token with an hour left should be kept, got " +
          std::to_string(long_calls->load()) + " calls");

    // Ten seconds left, under the 30 s refresh-ahead: fetched again each time.
    auto short_calls = std::make_shared<std::atomic<int>>(0);
    auto newest = std::make_shared<std::string>();
    ClientConfig short_lived;
    short_lived.token_source = [short_calls, newest]() {
        *newest = client_token(10, "dev-" + std::to_string(short_calls->fetch_add(1)));
        return *newest;
    };
    QueenClient refreshes(server.url(), short_lived);
    for (int i = 0; i < 3; ++i) pop(refreshes);
    check(short_calls->load() == 3, "a token 10 s from its exp should be fetched again, got " +
          std::to_string(short_calls->load()) + " calls");
    auto hits = server.hits();
    check(hits.size() == 6 && hits[5].authorization == "Bearer " + *newest,
          "the last request should carry the newest token");
}

void test_the_expiry_is_read_from_a_client_token_and_nothing_else() {
    const auto at = util::token_expires_at(client_token(100));
    check(at.has_value(), "a client token's exp should be read");
    if (at) {
        const auto left = std::chrono::duration_cast<std::chrono::seconds>(
            *at - std::chrono::system_clock::now()).count();
        check(left > 90 && left <= 100, "the expiry should be about 100 s away, got " +
              std::to_string(left) + " s");
    }
    check(util::token_expires_at(client_token_with({{"exp", 1700000000}})) ==
              std::chrono::system_clock::time_point(std::chrono::seconds(1700000000)),
          "exp should be read as unix seconds");

    const std::string header = base64url(json{{"alg", "HS256"}}.dump());
    const std::vector<std::pair<std::string, std::string>> unreadable = {
        {"qk_live_abc", "an API key"},
        {"token-0", "an opaque token"},
        {"qt_not.a", "two segments"},
        {"qt_a.b.c.d", "four segments"},
        {"qt_a.!!!.c", "a middle segment that is not base64url"},
        {"qt_" + header + "." + base64url("not json") + ".sig", "claims that are not JSON"},
        {"qt_" + header + "." + base64url("[1,2]") + ".sig", "claims that are not an object"},
        {client_token_with({{"sub", "dev1"}}), "no exp"},
        {client_token_with({{"exp", "1700000000"}}), "an exp that is a string"},
        {client_token_with({{"exp", -5}}), "a negative exp"},
        {client_token_with({{"exp", 1e300}}), "an exp past what the clock counts to"},
    };
    for (const auto& entry : unreadable) {
        check(!util::token_expires_at(entry.first).has_value(),
              entry.second + " should have no expiry");
    }

    for (const std::string s : {"", "a", "ab", "abc", "abcd", "{\"exp\":1}"}) {
        const auto decoded = util::base64url_decode(base64url(s));
        check(decoded.has_value() && *decoded == s, "base64url should round-trip '" + s + "'");
    }
}

void test_a_401_met_by_many_requests_refreshes_once() {
    // token-0 is refused wherever it is sent; any later token is taken.
    PlanServer server([](const RecordedHit& hit) {
        return hit.authorization == "Bearer token-0" ? token_expired_401() : no_messages();
    });
    auto calls = std::make_shared<std::atomic<int>>(0);

    ClientConfig config;
    config.token_source = [calls]() {
        // Slow enough that the other requests wait for this call, not make their own.
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        return "token-" + std::to_string(calls->fetch_add(1));
    };
    QueenClient client(server.url(), config);

    std::atomic<int> succeeded{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&]() {
            try {
                pop(client);
                succeeded.fetch_add(1);
            } catch (const std::exception& e) {
                std::cout << RED << "    a request failed: " << e.what() << RESET << std::endl;
            }
        });
    }
    for (auto& t : threads) t.join();

    check(succeeded.load() == 4, "every request should succeed, " + std::to_string(succeeded.load()) +
          " of 4 did");
    check(calls->load() == 2, "one fetch, then one refresh for every 401: expected 2 calls, got " +
          std::to_string(calls->load()));
    for (const auto& hit : server.hits()) {
        check(hit.authorization == "Bearer token-0" || hit.authorization == "Bearer token-1",
              "a request sent '" + hit.authorization + "'");
    }
}

void test_a_function_that_gives_no_token_sends_nothing() {
    struct Case {
        std::string what;
        std::function<std::string()> source;
        std::string message;  // what what() must contain
    };
    const std::vector<Case> cases = {
        {"throws", []() -> std::string { throw std::runtime_error("backend down"); }, "backend down"},
        {"throws a non-exception", []() -> std::string { throw 42; }, "threw"},
        {"answers an empty token", []() { return std::string(); }, "empty"},
        {"answers only spaces", []() { return std::string(" \n"); }, "empty"},
        {"answers a token with a line break", []() { return std::string("qk\r\nX-Injected: 1"); }, "header"},
    };
    for (const auto& c : cases) {
        // Two backends: a token that cannot be had is not a backend's fault,
        // so it must not fail over to the other one either.
        PlanServer a({}, no_messages());
        PlanServer b({}, no_messages());
        ClientConfig config;
        config.token_source = c.source;
        QueenClient client({a.url(), b.url()}, config);

        bool threw = false;
        try {
            pop(client);
        } catch (const TokenSourceError& e) {
            threw = true;
            const std::string what = e.what();
            check(what.rfind("token source: ", 0) == 0, c.what + ": what() is '" + what + "'");
            check(what.find(c.message) != std::string::npos,
                  c.what + ": what() should say '" + c.message + "', is '" + what + "'");
        } catch (const std::exception& e) {
            check(false, c.what + ": expected TokenSourceError, got '" + std::string(e.what()) + "'");
        }
        check(threw, c.what + ": the request must fail with TokenSourceError");
        check(a.hit_count() + b.hit_count() == 0,
              c.what + ": nothing must be sent, got " + std::to_string(a.hit_count() + b.hit_count()) +
              " requests");
    }
}

void test_a_fixed_token_and_a_function_together_are_refused() {
    PlanServer server({}, no_messages());

    ClientConfig config;
    config.bearer_token = "qk_live_x";
    config.token_source = []() { return std::string("qt_a.b.c"); };

    bool threw = false;
    try {
        QueenClient client(server.url(), config);
    } catch (const std::invalid_argument& e) {
        threw = true;
        check(std::string(e.what()).find("token_source") != std::string::npos,
              "the error should name the two options, is '" + std::string(e.what()) + "'");
    }
    check(threw, "bearer_token and token_source together must be refused at construction");
    check(server.hit_count() == 0, "a refused configuration must send nothing");
}

void test_mint_posts_the_entity_and_reads_the_answer() {
    PlanServer server({{201, R"({"token":"qt_a.b.c","sub":"dev-8f3a","key":"k1",)"
                             R"("expiresAt":"2026-11-11T12:00:00+00:00","expiresIn":600})"}},
                      {200, R"({"ok":true})"});

    ClientConfig config;
    config.bearer_token = "qk_live_role";
    QueenClient client(server.url(), config);

    MintTokenResponse minted = client.tokens().mint({"dev-8f3a", 600});
    check(minted.token == "qt_a.b.c", "token is '" + minted.token + "'");
    check(minted.sub == "dev-8f3a", "sub is '" + minted.sub + "'");
    check(minted.key == "k1", "key is '" + minted.key + "'");
    check(minted.expires_at == "2026-11-11T12:00:00+00:00", "expires_at is '" + minted.expires_at + "'");
    check(minted.expires_in == 600, "expires_in is " + std::to_string(minted.expires_in));

    auto hits = server.hits();
    check(hits.size() == 1, "expected 1 request, got " + std::to_string(hits.size()));
    if (!hits.empty()) {
        check(hits[0].method == "POST" && hits[0].path == "/api/v1/tokens",
              "sent " + hits[0].method + " " + hits[0].path);
        const json body = json::parse(hits[0].body);
        check(body == json{{"sub", "dev-8f3a"}, {"ttlSeconds", 600}}, "sent the body " + body.dump());
        check(hits[0].authorization == "Bearer qk_live_role",
              "the mint should carry the client's credential, sent '" + hits[0].authorization + "'");
    }

    bool refused = false;
    try {
        client.tokens().mint(MintTokenRequest{});
    } catch (const std::invalid_argument& e) {
        refused = std::string(e.what()).find("sub is required") != std::string::npos;
    }
    check(refused, "an empty sub must be refused before anything is sent");
    check(server.hit_count() == 1, "an empty sub must send nothing");
}

void test_mint_sends_the_key_and_the_grants_by_their_wire_names() {
    PlanServer server({{201, R"({"token":"qt_a.b.c","sub":"dev-1","key":"k2","expiresAt":"x","expiresIn":3600})"},
                       {404, R"({"error":"no such key","code":"key_unknown"})"}},
                      {200, R"({"ok":true})"});

    ClientConfig config;
    config.bearer_token = "qk_live_admin";
    QueenClient client(server.url(), config);

    Grant telemetry;
    telemetry.queue = "telemetry";
    telemetry.partition = "{sub}";
    telemetry.ops = {"produce"};
    Grant state;
    state.kv = "device-state";
    state.key = "{sub}";
    state.ops = {"read", "write"};

    MintTokenRequest request;
    request.sub = "dev-1";
    request.key = "devices";
    request.allow = std::vector<Grant>{telemetry, state};
    client.tokens().mint(request);

    auto hits = server.hits();
    if (!hits.empty()) {
        const json expected = {
            {"sub", "dev-1"},
            {"key", "devices"},
            {"allow", json::array({
                {{"queue", "telemetry"}, {"partition", "{sub}"}, {"ops", {"produce"}}},
                {{"kv", "device-state"}, {"key", "{sub}"}, {"ops", {"read", "write"}}},
            })},
        };
        const json body = json::parse(hits[0].body);
        check(body == expected, "sent the body " + body.dump());
    }

    // A refusal is the broker's, with its code.
    bool threw = false;
    try {
        client.tokens().mint(request);
    } catch (const HttpError& e) {
        threw = true;
        check(e.status_code() == 404 && e.code() == "key_unknown",
              "the refusal should round-trip, got " + std::to_string(e.status_code()) + " '" + e.code() + "'");
    }
    check(threw, "a refused mint must throw HttpError");
}

// ============================================================================

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "Queen C++ Client - Client Token Tests" << std::endl;
    std::cout << "========================================\n" << std::endl;

    run_test("A 401 fetches a new token and sends the request again",
             test_a_401_fetches_a_new_token_and_sends_the_request_again);
    run_test("A second 401 is the caller's", test_a_second_401_is_the_callers);
    run_test("A fixed token is not retried on a 401", test_a_fixed_token_is_not_retried_on_a_401);
    run_test("The function is called once across requests", test_the_function_is_called_once_across_requests);
    run_test("A client token near its exp is fetched again", test_a_client_token_near_its_exp_is_fetched_again);
    run_test("The expiry is read from a client token and nothing else",
             test_the_expiry_is_read_from_a_client_token_and_nothing_else);
    run_test("A 401 met by many requests refreshes once", test_a_401_met_by_many_requests_refreshes_once);
    run_test("A function that gives no token sends nothing", test_a_function_that_gives_no_token_sends_nothing);
    run_test("A fixed token and a function together are refused",
             test_a_fixed_token_and_a_function_together_are_refused);
    run_test("Mint posts the entity and reads the answer", test_mint_posts_the_entity_and_reads_the_answer);
    run_test("Mint sends the key and the grants by their wire names",
             test_mint_sends_the_key_and_the_grants_by_their_wire_names);

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << GREEN << "All client token tests passed" << RESET << std::endl;
    } else {
        std::cout << RED << failures << " check(s) failed" << RESET << std::endl;
    }

    return failures > 0 ? 1 : 0;
}
