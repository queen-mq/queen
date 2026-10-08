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
 *   - renew_lease() RENEWS. While the handler runs, one
 *     POST /api/v1/lease/:leaseId/extend per lease per interval: every message
 *     of a pop shares its leaseId, so a batch is one request, not one per
 *     message. Nothing after the handler returns.
 *   - A HANDLER THAT THROWS IS NACKED, whatever auto_ack() says. auto_ack(false)
 *     hands the SUCCESS path to the handler, not the failure path: a handler
 *     that threw never settled its messages. Left leased, a poison message
 *     comes back only when its lease expires, which never charges the queue's
 *     retry budget (server/src/rsm/consume/pop.rs: "The retry budget is
 *     charged only by an explicit `failed`, never by an expiry"), so it never
 *     reaches the DLQ.
 *   - A NACK DROPS THE LATER MESSAGES OF ITS PARTITION, AND ONLY THOSE. The
 *     nack releases the lease of its message's partition and leaves that
 *     partition's cursor at the completed prefix (server/src/rsm/consume/
 *     ack.rs, the `Failed` branch), so the later messages of that partition
 *     are redelivered and an ack for one of them is refused: handling them now
 *     only makes duplicates. The other partitions of a multi-partition pop
 *     keep their lease, and their messages are handled.
 *   - commit_on_delivery() IS A pop() OPTION. It puts the broker's
 *     `autoAck=true` on the pop and nothing else: the broker moves the group's
 *     cursor as it hands the messages out, so there is no lease and nothing to
 *     ack. consume() always leases its messages, so it refuses the option
 *     before any request, and auto_ack() never sends it.
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
#include <map>

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
    std::string target;                      // raw request target, query string included
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
                calls_.push_back({req.method, req.path, req.body, req.target});
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

/// Every acknowledgment the client sent, flattened across /api/v1/ack and
/// /api/v1/ack/batch, in the order they arrived. The consumer group of a batch
/// is copied onto each of its items.
std::vector<json> acks_sent(const StubServer& server) {
    std::vector<json> out;
    for (const auto& call : server.calls()) {
        if (call.path == "/api/v1/ack") {
            out.push_back(json::parse(call.body));
        } else if (call.path == "/api/v1/ack/batch") {
            json body = json::parse(call.body);
            for (auto item : body["acknowledgments"]) {
                item["consumerGroup"] = body["consumerGroup"];
                out.push_back(item);
            }
        }
    }
    return out;
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
// renew_lease() renews while the handler runs
// ============================================================================

void test_renew_lease_renews_while_the_handler_runs() {
    StubServer server(one_claim(popped(3, "lease-1")));
    QueenClient client({server.url()}, fast_config());

    StopAfter watchdog(5000);
    client.queue("consume-renew").group("workers").renew_lease(true, 100)
          .wait(false).idle_millis(300)
          .consume([](const json&) {
              std::this_thread::sleep_for(std::chrono::milliseconds(450));
          }, watchdog.signal());

    size_t renewals = server.count_with_prefix("/api/v1/lease/");
    check(server.count_with_prefix("/api/v1/lease/lease-1/extend") == renewals,
          "every renewal must be for the batch's lease");
    // 450 ms at a 100 ms interval is four ticks; one request per message would
    // be twelve. The bounds leave room for a slow scheduler.
    check(renewals >= 2, "expected the lease renewed while the handler ran, got " +
          std::to_string(renewals) + " renewal(s)");
    check(renewals <= 6, "expected one renewal per lease per interval, got " +
          std::to_string(renewals));

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    check(server.count_with_prefix("/api/v1/lease/") == renewals,
          "renewal must stop when the handler returns");
}

void test_no_renewal_unless_asked() {
    StubServer server(one_claim(popped(1, "lease-1")));
    QueenClient client({server.url()}, fast_config());

    StopAfter watchdog(5000);
    client.queue("consume-no-renew").group("workers")
          .wait(false).idle_millis(300)
          .consume([](const json&) {
              std::this_thread::sleep_for(std::chrono::milliseconds(250));
          }, watchdog.signal());

    check(server.count_with_prefix("/api/v1/lease/") == 0,
          "renew_lease() was never called, so nothing may be renewed");
}

// ============================================================================
// A throwing handler is nacked
// ============================================================================

// auto_ack(false) leaves settling to the handler, failures included: the
// consumer sends no nack, by design, and the message comes back when its lease
// expires. A throw must not stop consume() either.
void test_a_throwing_handler_is_not_nacked_without_auto_ack() {
    StubServer server(one_claim(popped(1, "lease-1")));
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    StopAfter watchdog(5000);
    try {
        client.queue("consume-no-nack").group("workers").each().auto_ack(false)
              .wait(false).idle_millis(300)
              .consume([](const json&) { throw std::runtime_error("boom"); }, watchdog.signal());
    } catch (...) {
        threw = true;
    }

    check(!threw, "a failing handler must not stop consume()");
    auto acks = acks_sent(server);
    check(acks.empty(), "auto_ack(false) sends no nack, got " + json(acks).dump());
}

void test_a_throwing_batch_handler_is_not_nacked_without_auto_ack() {
    StubServer server(one_claim(popped(2, "lease-1")));
    QueenClient client({server.url()}, fast_config());

    StopAfter watchdog(5000);
    client.queue("consume-no-nack-batch").group("workers").auto_ack(false)
          .wait(false).idle_millis(300)
          .consume([](const json&) { throw std::runtime_error("boom"); }, watchdog.signal());

    auto acks = acks_sent(server);
    check(acks.empty(), "auto_ack(false) sends no nack, got " + json(acks).dump());
}

// Without auto_ack, the handler acks by itself. After a failure, an ack of a
// later message of the SAME partition would move the cursor past the failed
// one, which would never come back: those messages are not handled. The other
// partition of the claim is.
void test_without_auto_ack_a_failure_skips_the_rest_of_its_partition() {
    // t1 and t3 in p1, t2 and t4 in p2, one lease: a multi-partition pop.
    StubServer server(one_claim(popped(4, "lease-1", "workers", {"p1", "p2"})));
    QueenClient client({server.url()}, fast_config());

    std::vector<std::string> handled;
    StopAfter watchdog(5000);
    client.queue("consume-no-nack-skip").group("workers").each().auto_ack(false)
          .wait(false).idle_millis(300)
          .consume([&](const json& msg) {
              handled.push_back(msg["transactionId"].get<std::string>());
              if (msg["transactionId"] == "t1") throw std::runtime_error("boom");
              client.ack(msg, true, {{"group", "workers"}});
          }, watchdog.signal());

    check(handled == std::vector<std::string>({"t1", "t2", "t4"}),
          "p1 after the failed t1 must not be handled, p2 must; handled " + json(handled).dump());

    std::vector<std::string> settled;
    for (const auto& ack : acks_sent(server)) {
        settled.push_back(ack.value("transactionId", "") + ":" + ack.value("status", ""));
    }
    check(settled == std::vector<std::string>({"t2:completed", "t4:completed"}),
          "only the handler's own acks, and no nack, got " + json(settled).dump());
}

void test_a_nack_drops_the_later_messages_of_its_partition() {
    StubServer server(one_claim(popped(3, "lease-1")));
    QueenClient client({server.url()}, fast_config());

    std::vector<std::string> handled;
    StopAfter watchdog(5000);
    client.queue("consume-abandon").group("workers").each()
          .wait(false).idle_millis(300)
          .consume([&handled](const json& msg) {
              handled.push_back(msg["transactionId"].get<std::string>());
              if (msg["transactionId"] == "t2") throw std::runtime_error("boom");
          }, watchdog.signal());

    check(handled == std::vector<std::string>({"t1", "t2"}),
          "the message after a nack in its partition must not be handled, handled " +
          json(handled).dump());

    auto acks = acks_sent(server);
    check(acks.size() == 2, "expected an ack and a nack, got " + std::to_string(acks.size()) +
          " acknowledgment(s)");
    if (acks.size() != 2) return;
    check(acks[0].value("transactionId", "") == "t1" && acks[0].value("status", "") == "completed",
          "t1 must be acked, got " + acks[0].dump());
    check(acks[1].value("transactionId", "") == "t2" && acks[1].value("status", "") == "failed",
          "t2 must be nacked, got " + acks[1].dump());
}

void test_a_nack_leaves_the_other_partitions_of_the_claim_alone() {
    // t1 and t3 in p1, t2 and t4 in p2, one lease: a multi-partition pop.
    StubServer server(one_claim(popped(4, "lease-1", "workers", {"p1", "p2"})));
    QueenClient client({server.url()}, fast_config());

    std::vector<std::string> handled;
    StopAfter watchdog(5000);
    client.queue("consume-multi-partition").group("workers").each()
          .wait(false).idle_millis(300)
          .consume([&handled](const json& msg) {
              handled.push_back(msg["transactionId"].get<std::string>());
              if (msg["transactionId"] == "t1") throw std::runtime_error("boom");
          }, watchdog.signal());

    check(handled == std::vector<std::string>({"t1", "t2", "t4"}),
          "p2 keeps its lease and must be handled, p1 after t1 must not; handled " +
          json(handled).dump());

    auto acks = acks_sent(server);
    std::vector<std::string> settled;
    for (const auto& ack : acks) {
        settled.push_back(ack.value("transactionId", "") + ":" + ack.value("status", ""));
    }
    check(settled == std::vector<std::string>({"t1:failed", "t2:completed", "t4:completed"}),
          "expected t1 nacked and t2, t4 acked, got " + json(settled).dump());
}

void test_a_nack_survives_an_error_that_is_not_utf8_or_short() {
    StubServer server(one_claim(popped(2, "lease-1", "workers", {"p1", "p2"})));
    QueenClient client({server.url()}, fast_config());

    const std::string latin1 = std::string("cannot open caf") + '\xe9' + ".txt";
    const std::string huge(20000, 'x');
    StopAfter watchdog(5000);
    client.queue("consume-bad-reason").group("workers").each()
          .wait(false).idle_millis(300)
          .consume([&](const json& msg) {
              throw std::runtime_error(msg["transactionId"] == "t1" ? latin1 : huge);
          }, watchdog.signal());

    auto acks = acks_sent(server);
    check(acks.size() == 2, "both messages must be nacked, got " + std::to_string(acks.size()));
    if (acks.size() != 2) return;
    auto error_of = [](const json& ack) {
        return ack.contains("error") && ack["error"].is_string() ? ack["error"].get<std::string>()
                                                                 : std::string();
    };
    std::string first = error_of(acks[0]);
    check(first.rfind("cannot open caf", 0) == 0 && first.find("\xef\xbf\xbd") != std::string::npos,
          "an invalid byte is replaced by U+FFFD, got " + acks[0].dump());
    std::string second = error_of(acks[1]);
    check(second.size() == util::MAX_NACK_ERROR_BYTES,
          "a long error is capped at " + std::to_string(util::MAX_NACK_ERROR_BYTES) +
          " bytes, got " + std::to_string(second.size()));
}

void test_a_handler_that_throws_a_non_standard_value_is_nacked() {
    StubServer server(one_claim(popped(1, "lease-1")));
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    StopAfter watchdog(5000);
    try {
        client.queue("consume-throw-int").group("workers").each()
              .wait(false).idle_millis(300)
              .consume([](const json&) { throw 42; }, watchdog.signal());
    } catch (...) {
        threw = true;
    }

    check(!threw, "a failing handler must not stop consume()");
    auto acks = acks_sent(server);
    check(acks.size() == 1 && acks[0].value("status", "") == "failed",
          "the message must be nacked, got " + json(acks).dump());
}

// ============================================================================
// commit_on_delivery() commits at delivery, on pop() only
// ============================================================================

/// The query of a request target as name -> value. httplib's client sorts the
/// parameters again before it sends them (see test_ephemeral.cpp), so the set
/// of parameters is what a test can pin, not their order.
std::map<std::string, std::string> query_of(const std::string& target) {
    std::map<std::string, std::string> params;
    auto mark = target.find('?');
    if (mark == std::string::npos) return params;
    std::string query = target.substr(mark + 1);
    size_t start = 0;
    while (start < query.size()) {
        size_t end = query.find('&', start);
        std::string pair = query.substr(start, end == std::string::npos ? std::string::npos
                                                                        : end - start);
        auto eq = pair.find('=');
        if (!pair.empty()) {
            params[pair.substr(0, eq)] = eq == std::string::npos ? "" : pair.substr(eq + 1);
        }
        if (end == std::string::npos) break;
        start = end + 1;
    }
    return params;
}

/// The query of the one pop that `configure` sends, to a broker with nothing to
/// give.
std::map<std::string, std::string> pop_query(const std::function<void(QueueBuilder&)>& configure) {
    StubServer server([](const httplib::Request&, httplib::Response& res) { res.status = 204; });
    QueenClient client({server.url()}, fast_config());

    auto builder = client.queue("commit-on-delivery");
    builder.group("workers").wait(false);
    configure(builder);
    builder.pop();

    auto calls = server.calls();
    if (calls.size() != 1) return {{"<requests>", std::to_string(calls.size())}};
    return query_of(calls[0].target);
}

void test_commit_on_delivery_puts_auto_ack_on_the_pop() {
    auto leased = pop_query([](QueueBuilder&) {});
    auto committed = pop_query([](QueueBuilder& b) { b.commit_on_delivery(); });

    check(leased.count("autoAck") == 0,
          "a plain pop must not send autoAck, got " + json(leased).dump());
    check(committed.count("autoAck") == 1 && committed["autoAck"] == "true",
          "commit_on_delivery() must send autoAck=true, got " + json(committed).dump());
    committed.erase("autoAck");
    check(committed == leased,
          "commit_on_delivery() must add autoAck=true and nothing else, got " +
          json(committed).dump() + " against " + json(leased).dump());
}

void test_only_commit_on_delivery_sends_auto_ack() {
    auto off = pop_query([](QueueBuilder& b) { b.commit_on_delivery(false); });
    auto turned_off = pop_query([](QueueBuilder& b) {
        b.commit_on_delivery().commit_on_delivery(false);
    });
    auto acked = pop_query([](QueueBuilder& b) { b.auto_ack(true); });

    check(off.count("autoAck") == 0,
          "commit_on_delivery(false) must not send autoAck, got " + json(off).dump());
    check(turned_off.count("autoAck") == 0,
          "commit_on_delivery(false) must undo commit_on_delivery(), got " +
          json(turned_off).dump());
    check(acked.count("autoAck") == 0,
          "auto_ack() is the consume() handler's ack and must not reach the pop, got " +
          json(acked).dump());
}

void test_consume_refuses_commit_on_delivery_before_any_request() {
    StubServer server(one_claim(popped(1, "lease-1")));
    QueenClient client({server.url()}, fast_config());

    std::atomic<int> handled{0};
    std::string reason;
    StopAfter watchdog(3000);
    try {
        client.queue("consume-commit-on-delivery").group("workers").commit_on_delivery()
              .wait(false).idle_millis(300)
              .consume([&handled](const json&) { handled++; }, watchdog.signal());
    } catch (const std::invalid_argument& e) {
        reason = e.what();
    }

    check(!reason.empty(), "consume() must refuse commit_on_delivery() with std::invalid_argument");
    check(reason.find("commit_on_delivery") != std::string::npos,
          "the refusal must name the option, got: " + reason);
    check(server.calls().empty(), "the refusal must come before any request, got " +
          std::to_string(server.calls().size()) + " request(s)");
    check(handled.load() == 0, "no message may reach the handler");
}

// ============================================================================

void test_optional_consumer_supervision() {
    for (bool enabled : {false, true}) {
        StubServer server([](const httplib::Request& req, httplib::Response& res) {
            if (req.path == "/api/v1/kv") {
                res.set_content(R"({"results":[{"applied":true},{"applied":true}]})", "application/json");
            } else { res.set_content(popped(1, "lease-1"), "application/json"); }
        });
        QueenClient client({server.url()}, fast_config());
        auto builder = client.queue("orders").concurrency(2).each().limit(1).auto_ack(false).wait(false);
        if (enabled) builder.supervision(SupervisionConfig{"billing-production"});
        std::atomic<int> calls{0};
        builder.consume([&](const json&) { if (++calls == 1) throw std::runtime_error("private failure"); });
        std::vector<json> docs;
        for (const auto& call : server.calls()) if (call.path == "/api/v1/kv") {
            auto ops = json::parse(call.body).at("operations");
            check(ops.size() == 2, "status must be an atomic head/chunk batch");
            check(ops[0]["ns"] == "queen-supervisor" && ops[0]["ttlSeconds"] == 60 && ops[1]["ttlSeconds"] == 60, "namespace and TTL");
            check(ops[0]["value"]["write"] == ops[1]["value"]["write"], "generation must match");
            auto raw = util::base64_decode(ops[1]["value"]["data"].get<std::string>());
            check(raw.size() == ops[0]["value"]["bytes"].get<size_t>(), "UTF-8 byte length");
            check(raw.find("private failure") == std::string::npos, "error text must not be published");
            docs.push_back(json::parse(raw));
        }
        check(calls == 2, "publication must not affect consumption");
        check(server.count_with_prefix("/api/v1/ack") == 0, "manual ACK policy must be preserved");
        if (!enabled) { check(docs.empty(), "default off must not publish"); continue; }
        check(docs.size() >= 2, "lifecycle observations");
        auto last = docs.back(); const auto& pool = last["pool_status"][0];
        check(last["state"] == "stopped", "final state");
        check(pool["running"] == 0 && pool["busy"] == 0, "actual exits");
        check(pool["completed"] == 1 && pool["failed"] == 1, "handler outcomes");
    }
}

void test_supervision_progress_and_validation() {
    ConsumeOptions options; options.queue = "orders"; options.concurrency = 2;
    options.supervision = SupervisionConfig{"billing"};
    ConsumerSupervision reporter(nullptr, options), other(nullptr, options);
    check(reporter.document("running")["instance_id"] != other.document("running")["instance_id"], "unique instance identity");
    reporter.worker_started(); auto id = reporter.begin_handler();
    auto pool = reporter.document("running")["pool_status"][0];
    check(pool["running"] == 1 && pool["busy"] == 1 && pool["completed"] == 0, "only started threads count as running");
    reporter.end_handler(id, true); reporter.worker_exited();
    pool = reporter.document("running")["pool_status"][0];
    check(pool["running"] == 0 && pool["completed"] == 1 && pool["oldest_inflight_seconds"].is_null(), "progress and exit cleanup");
    for (const auto& group : {"", "coordination", "a/b", "a\n"}) {
        options.supervision = SupervisionConfig{group}; bool refused = false;
        try { ConsumerSupervision invalid(nullptr, options); } catch (const std::invalid_argument&) { refused = true; }
        check(refused, "invalid publication group must fail");
    }
}

void test_supervision_publication_is_bounded() {
    StubServer server([](const httplib::Request& req, httplib::Response& res) {
        if (req.path == "/api/v1/kv") {
            check(req.get_header_value("Authorization") == "Bearer test-token", "publication uses existing credentials");
            std::this_thread::sleep_for(std::chrono::seconds(3));
            res.status = 429; res.set_header("Retry-After", "1000");
        } else { res.set_content(popped(1, "lease-1"), "application/json"); }
    });
    auto config = fast_config(); config.bearer_token = "test-token";
    QueenClient client({server.url()}, config);
    const auto started = std::chrono::steady_clock::now();
    int handled = 0;
    client.queue("orders").supervision(SupervisionConfig{"billing"}).auto_ack(false).wait(false).limit(1)
        .consume([&](const json&) { ++handled; });
    check(handled == 1, "failed publication cannot stop the handler");
    check(std::chrono::steady_clock::now() - started < std::chrono::seconds(6), "publication cannot inherit unbounded retries");
    check(server.count_with_prefix("/api/v1/kv") == 2, "exactly one attempt per lifecycle publication");
}

int main() {
    run_test("optional supervision reports actual consumer lifecycle", test_optional_consumer_supervision);
    run_test("supervision observes progress and validates groups", test_supervision_progress_and_validation);
    run_test("supervision publication is bounded", test_supervision_publication_is_bounded);

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

    run_test("renew_lease() renews while the handler runs",
             test_renew_lease_renews_while_the_handler_runs);
    run_test("no renewal unless asked", test_no_renewal_unless_asked);

    run_test("a throwing handler is not nacked without auto_ack",
             test_a_throwing_handler_is_not_nacked_without_auto_ack);
    run_test("a throwing batch handler is not nacked without auto_ack",
             test_a_throwing_batch_handler_is_not_nacked_without_auto_ack);
    run_test("without auto_ack a failure skips the rest of its partition",
             test_without_auto_ack_a_failure_skips_the_rest_of_its_partition);
    run_test("a nack drops the later messages of its partition",
             test_a_nack_drops_the_later_messages_of_its_partition);
    run_test("a nack leaves the other partitions of the claim alone",
             test_a_nack_leaves_the_other_partitions_of_the_claim_alone);
    run_test("a nack survives an error that is not UTF-8 or short",
             test_a_nack_survives_an_error_that_is_not_utf8_or_short);
    run_test("a handler that throws a non-standard value is nacked",
             test_a_handler_that_throws_a_non_standard_value_is_nacked);

    run_test("commit_on_delivery() puts autoAck=true on the pop",
             test_commit_on_delivery_puts_auto_ack_on_the_pop);
    run_test("only commit_on_delivery() sends autoAck", test_only_commit_on_delivery_sends_auto_ack);
    run_test("consume() refuses commit_on_delivery() before any request",
             test_consume_refuses_commit_on_delivery_before_any_request);

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << GREEN << "All consume contract tests passed" << RESET << std::endl;
    } else {
        std::cout << RED << failures << " check(s) failed" << RESET << std::endl;
    }

    return failures > 0 ? 1 : 0;
}
