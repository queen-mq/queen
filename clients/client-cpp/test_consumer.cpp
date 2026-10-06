/**
 * Queen C++ Client - consume path contract test suite: the consume loop, and
 * the ack() and renew() calls it settles with
 *
 * WHAT THIS FILE IS FOR.
 *
 *   - ack() AND renew() READ THE BODY. Both routes answer HTTP 200 when the
 *     broker settled or extended nothing: the per-item `success` of the ack
 *     array, and the `success`/`renewed` of the renewal, are the only signal.
 *   - A 4xx ON A POP STOPS consume() WITH THAT ERROR; A 5xx DOES NOT. A
 *     worker runs in a pool task whose future is only waited on, so an error
 *     thrown there is gone unless the loop carries it out. A 5xx is a broker
 *     restarting or electing: the loop waits and polls again.
 *
 * Like its siblings this runs against an in-process httplib::Server -- no
 * broker, so every response is the one the test chose:
 *
 *   make consumer && ./bin/test_consumer
 *
 * The end-to-end half, against a real broker, is the settlement section of
 * test_client.cpp.
 *
 * Siblings: test_conflation.cpp, test_autopilot.cpp, test_retry429.cpp.
 */

#include "queen_client.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <vector>
#include <string>
#include <set>

using namespace queen;
using json = nlohmann::json;

#define GREEN "\033[32m"
#define RED "\033[31m"
#define BLUE "\033[34m"
#define RESET "\033[0m"

// ============================================================================
// Test double: records every request and answers from a responder
// ============================================================================

struct RecordedCall {
    std::string method;
    std::string path;                        // path only, no query string
    std::string body;
};

using Responder = std::function<void(const httplib::Request&, httplib::Response&)>;

class StubServer {
private:
    httplib::Server server_;
    std::thread thread_;
    int port_ = 0;
    Responder responder_;

    mutable std::mutex mutex_;
    std::vector<RecordedCall> calls_;

public:
    explicit StubServer(Responder responder)
        : responder_(std::move(responder)) {
        auto handler = [this](const httplib::Request& req, httplib::Response& res) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                calls_.push_back({req.method, req.path, req.body});
            }
            responder_(req, res);
        };

        server_.Get(".*", handler);
        server_.Post(".*", handler);
        server_.Put(".*", handler);
        server_.Delete(".*", handler);

        port_ = server_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this]() { server_.listen_after_bind(); });
        server_.wait_until_ready();
    }

    ~StubServer() {
        server_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    std::vector<RecordedCall> calls() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return calls_;
    }

    size_t count_with_prefix(const std::string& prefix) const {
        std::lock_guard<std::mutex> lock(mutex_);
        size_t n = 0;
        for (const auto& call : calls_) {
            if (call.path.rfind(prefix, 0) == 0) ++n;
        }
        return n;
    }
};
/// Raises a stop signal after a budget, so a loop that fails to stop on its own
/// FAILS its test instead of hanging the suite (the idiom of test_conflation.cpp).
class StopAfter {
private:
    std::atomic<bool> flag_{false};
    std::atomic<bool> done_{false};
    std::thread timer_;

public:
    explicit StopAfter(int budget_millis) {
        timer_ = std::thread([this, budget_millis]() {
            auto deadline = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(budget_millis);
            while (!done_.load() && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            flag_.store(true);
        });
    }

    ~StopAfter() {
        done_.store(true);
        if (timer_.joinable()) {
            timer_.join();
        }
    }

    std::atomic<bool>* signal() { return &flag_; }
};
// ============================================================================
// Broker shapes
// ============================================================================

/// A pop answer: `count` messages t1..tN, all under `lease`, the way a 2.x
/// broker sends a claim. Message i is in partitions[(i - 1) % size]: one
/// partition by default, several for a multi-partition claim.
std::string popped(int count, const std::string& lease, const std::string& group = "workers",
                   const std::vector<std::string>& partitions = {"p1"}) {
    json messages = json::array();
    for (int i = 1; i <= count; ++i) {
        messages.push_back({
            {"id", "t" + std::to_string(i)},
            {"transactionId", "t" + std::to_string(i)},
            {"partitionId", partitions[(i - 1) % partitions.size()]},
            {"partition", "Default"},
            {"leaseId", lease},
            {"consumerGroup", group},
            {"data", {{"n", i}}}
        });
    }
    return json{{"success", true}, {"leaseId", lease}, {"messages", messages},
                {"partitionsClaimed", 1}}.dump();
}

/// The ack wire answer, `[{index, transactionId, success, error, ...}]`, one
/// item per acknowledgment in request order, for the single and the batch
/// route alike. `refused` lists the transaction ids to answer success:false.
std::string ack_answer(const std::string& request_body,
                       const std::set<std::string>& refused = {}) {
    json body = json::parse(request_body);
    json items = body.contains("acknowledgments") ? body["acknowledgments"] : json::array({body});
    json out = json::array();
    int index = 0;
    for (const auto& item : items) {
        std::string txn = item.value("transactionId", "");
        bool ok = refused.count(txn) == 0;
        out.push_back({
            {"index", index++},
            {"transactionId", txn},
            {"success", ok},
            {"error", ok ? json(nullptr) : json("invalid or expired lease")},
            {"leaseReleased", false},
            {"dlq", false},
            {"noop", false}
        });
    }
    return out.dump();
}

/// The lease renewal answer of a 2.x broker (server/src/rsm/facade/real.rs).
std::string renew_answer(const std::string& lease, bool renewed) {
    json expires = renewed ? json("2030-01-01T00:00:00.000Z") : json(nullptr);
    return json{{"leaseId", lease}, {"success", renewed}, {"renewed", renewed ? 1 : 0},
                {"newExpiresAt", expires}, {"expiresAt", expires},
                {"lease_expires_at", expires}}.dump();
}

bool starts_with(const std::string& text, const std::string& prefix) {
    return text.rfind(prefix, 0) == 0;
}

/// A broker with one claim to hand out: the first pop gets `first_pop`, every
/// later one a 204. Acks are accepted, renewals succeed.
Responder one_claim(const std::string& first_pop) {
    auto pops = std::make_shared<std::atomic<int>>(0);
    return [pops, first_pop](const httplib::Request& req, httplib::Response& res) {
        if (starts_with(req.path, "/api/v1/pop")) {
            if ((*pops)++ == 0) {
                res.status = 200;
                res.set_content(first_pop, "application/json");
            } else {
                res.status = 204;
            }
            return;
        }
        if (starts_with(req.path, "/api/v1/ack")) {
            res.status = 200;
            res.set_content(ack_answer(req.body), "application/json");
            return;
        }
        if (starts_with(req.path, "/api/v1/lease/")) {
            std::string lease = req.path.substr(std::string("/api/v1/lease/").size());
            lease = lease.substr(0, lease.find('/'));
            res.status = 200;
            res.set_content(renew_answer(lease, true), "application/json");
            return;
        }
        res.status = 404;
    };
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

// Client with the 429 backoff shortened so no test in this file can sleep.
ClientConfig fast_config() {
    ClientConfig config;
    config.retry_429.base_millis = 1;
    config.retry_429.cap_millis = 2;
    return config;
}

// ============================================================================
// ack() and renew() read the body
// ============================================================================

json message(const std::string& txn, const std::string& lease = "lease-1") {
    return {{"transactionId", txn}, {"partitionId", "p1"}, {"leaseId", lease}};
}

void test_ack_reports_a_refused_acknowledgment() {
    StubServer server([](const httplib::Request& req, httplib::Response& res) {
        res.status = 200;
        res.set_content(ack_answer(req.body, {"t1"}), "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    json single = client.ack(message("t1"), true, {{"group", "workers"}});
    check(single.value("success", true) == false,
          "a refused ack must report success:false, got " + single.dump());
    check(single.value("error", "").find("invalid or expired lease") != std::string::npos,
          "the broker's reason must reach the caller, got " + single.dump());

    json batch = client.ack(json::array({message("t1"), message("t2")}), true,
                            {{"group", "workers"}});
    check(batch.value("success", true) == false,
          "a batch with a refused item must report success:false, got " + batch.dump());
    check(batch.value("error", "").find("invalid or expired lease") != std::string::npos,
          "the batch must name the refusal, got " + batch.dump());
}

void test_ack_reports_an_accepted_acknowledgment() {
    StubServer server([](const httplib::Request& req, httplib::Response& res) {
        res.status = 200;
        res.set_content(ack_answer(req.body), "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    json single = client.ack(message("t1"), true, {{"group", "workers"}});
    check(single.value("success", false), "an accepted ack is a success, got " + single.dump());
    check(single.contains("result") && single["result"].is_array(),
          "the broker's answer stays under \"result\", got " + single.dump());

    json batch = client.ack(json::array({message("t1"), message("t2")}), true,
                            {{"group", "workers"}});
    check(batch.value("success", false), "an accepted batch is a success, got " + batch.dump());
}

void test_renew_reports_a_lease_the_broker_did_not_extend() {
    StubServer server([](const httplib::Request& req, httplib::Response& res) {
        bool known = req.path == "/api/v1/lease/live/extend";
        res.status = 200;
        res.set_content(renew_answer(known ? "live" : "gone", known), "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    json gone = client.renew("gone");
    check(gone.value("success", true) == false,
          "a lease the broker did not extend must report success:false, got " + gone.dump());
    check(!gone.value("error", "").empty(), "a failed renewal must say why, got " + gone.dump());

    json live = client.renew("live");
    check(live.value("success", false), "an extended lease is a success, got " + live.dump());
    check(live["newExpiresAt"] == "2030-01-01T00:00:00.000Z",
          "the new expiry must be reported, got " + live.dump());
}

// ============================================================================
// A 4xx on a pop stops consume() with that error, a 5xx does not
// ============================================================================

void test_a_4xx_on_a_pop_stops_consume_with_that_error() {
    StubServer server([](const httplib::Request&, httplib::Response& res) {
        res.status = 400;
        res.set_content(R"({"success":false,"error":"bad pop","code":"bad_request"})",
                        "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    int status = 0;
    StopAfter watchdog(3000);
    try {
        client.queue("consume-pop-error").group("workers").wait(false)
              .consume([](const json&) {}, watchdog.signal());
    } catch (const HttpError& e) {
        status = e.status_code();
    }

    check(status == 400, "consume() must rethrow the broker's 400, got status " +
          std::to_string(status));
    check(server.count_with_prefix("/api/v1/pop") == 1,
          "a refused pop is not retried, got " +
          std::to_string(server.count_with_prefix("/api/v1/pop")) + " pops");
}

void test_a_5xx_keeps_consume_polling() {
    // A 503 first (a broker electing a leader), then the claim.
    auto pops = std::make_shared<std::atomic<int>>(0);
    Responder claim = one_claim(popped(1, "lease-1"));
    StubServer server([pops, claim](const httplib::Request& req, httplib::Response& res) {
        if (starts_with(req.path, "/api/v1/pop") && (*pops)++ == 0) {
            res.status = 503;
            res.set_content(R"({"error":"no leader yet"})", "application/json");
            return;
        }
        claim(req, res);
    });
    ClientConfig config = fast_config();
    config.retry_attempts = 1;               // the loop's retry, not HttpClient's
    QueenClient client({server.url()}, config);

    std::atomic<int> handled{0};
    bool threw = false;
    StopAfter watchdog(5000);
    try {
        client.queue("consume-5xx").group("workers").wait(false).limit(1)
              .consume([&handled](const json&) { handled++; }, watchdog.signal());
    } catch (const std::exception&) {
        threw = true;
    }

    check(!threw, "a 5xx must not stop consume()");
    check(handled.load() == 1, "the claim after the 5xx must be handled, handled " +
          std::to_string(handled.load()));
}

// ============================================================================

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "Queen C++ Client - Consume Contract Tests" << std::endl;
    std::cout << "========================================\n" << std::endl;

    run_test("ack() reports a refused acknowledgment", test_ack_reports_a_refused_acknowledgment);
    run_test("ack() reports an accepted acknowledgment",
             test_ack_reports_an_accepted_acknowledgment);
    run_test("renew() reports a lease the broker did not extend",
             test_renew_reports_a_lease_the_broker_did_not_extend);

    run_test("a 4xx on a pop stops consume() with that error",
             test_a_4xx_on_a_pop_stops_consume_with_that_error);
    run_test("a 5xx keeps consume() polling", test_a_5xx_keeps_consume_polling);

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << GREEN << "All consume contract tests passed" << RESET << std::endl;
    } else {
        std::cout << RED << failures << " check(s) failed" << RESET << std::endl;
    }

    return failures > 0 ? 1 : 0;
}
