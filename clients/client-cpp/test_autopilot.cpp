/**
 * Queen C++ Client - pop autopilot wire-contract test suite
 *
 * WHAT THIS FILE IS FOR.
 * The four things a client can be wrong about here, and why each is asserted
 * against the WHOLE query string rather than against one parameter:
 *
 *   1. BOTH ENTRY POINTS MUST AGREE. Before 2.7.1, pop() and consume() assembled
 *      their query strings separately in this SDK -- the standing hazard
 *      PLAN_CONFLATION §4 names by name -- so a rule implemented in one and not
 *      the other was the default failure, not an exotic one. Since 2.7.1 both go
 *      through one code path (detail::pop_once); every case still runs through
 *      both, so a change that splits them again fails here. Against a live
 *      broker the half that forgot simply gets a differently sized claim and
 *      passes.
 *   2. NOT ENGAGING AUTOPILOT MUST BE BYTE-IDENTICAL TO THE OLD SDK. The escape
 *      hatch is only worth having if it is exact, and "exact" is not something a
 *      test of one parameter can show: a stray autopilot=true, or a batch that
 *      stopped being emitted, is a different request. Hence full-string equality
 *      of the query rather than a substring search for the key under test.
 *
 *      One caveat, stated because it bounds the claim: cpp-httplib re-serialises
 *      a request target from its parsed parameter map, which is key-SORTED. So
 *      what these strings pin is the exact set of keys and values that reached
 *      the socket, not the order this client wrote them in. Order is not part of
 *      the contract with the broker anyway -- the set is.
 *   3. AN EXPLICIT VALUE IS SACRED, PER DIMENSION. partitions(1) and "never
 *      called partitions" both used to reach the wire as nothing at all; they
 *      are now different requests, and the pinned one must survive autopilot or
 *      a consumer that asked for one lane gets swept across many.
 *   4. THE ADDITIVE RESPONSE FIELD MUST NOT BE LOAD-BEARING. A broker that does
 *      not send it, sends it half-filled, or sends it with fields this SDK has
 *      never heard of, all have to work.
 *
 * Like its siblings this runs against an in-process httplib::Server -- no
 * broker:
 *
 *   make autopilot && ./bin/test_autopilot
 *
 * Siblings: test_conflation.cpp, test_kv_timers.cpp, test_retry429.cpp.
 */

#include "queen_client.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <vector>
#include <string>
#include <sstream>
#include <functional>

using namespace queen;
using json = nlohmann::json;

#define GREEN "\033[32m"
#define RED "\033[31m"
#define BLUE "\033[34m"
#define RESET "\033[0m"

// ============================================================================
// Test double
// ============================================================================

struct RecordedCall {
    std::string method;
    std::string target;   // raw request target, query string included
    std::string body;
};

using Responder = std::function<void(const httplib::Request&, httplib::Response&)>;

/// One delivered frame, shaped like `render_pop_parts`. `extra` carries the
/// additive autopilot echo under test.
inline Responder pop_with(const std::string& extra = "") {
    return [extra](const httplib::Request&, httplib::Response& res) {
        res.status = 200;
        res.set_content(
            std::string(R"({"success":true,"queue":"orders","partition":"Default",)"
                        R"("partitionId":"p1","leaseId":"lease-1","consumerGroup":"workers",)"
                        R"("messages":[{"id":"m1","transactionId":"t1","data":{"n":1},)"
                        R"("queue":"orders","partition":"Default","partitionId":"p1",)"
                        R"("leaseId":"lease-1"}],"partitionsClaimed":1)") +
                extra + "}",
            "application/json");
    };
}

/// An empty pop that still carries a body, so the echo has somewhere to sit.
inline Responder empty_pop_with(const std::string& extra = "") {
    return [extra](const httplib::Request&, httplib::Response& res) {
        res.status = 200;
        res.set_content(
            std::string(R"({"success":true,"queue":"orders","partition":"",)"
                        R"("partitionId":"","leaseId":"","consumerGroup":"workers",)"
                        R"("messages":[],"partitionsClaimed":0)") +
                extra + "}",
            "application/json");
    };
}

class StubServer {
private:
    httplib::Server server_;
    std::thread thread_;
    int port_ = 0;
    Responder responder_;

    mutable std::mutex mutex_;
    std::vector<RecordedCall> calls_;

public:
    explicit StubServer(Responder responder) : responder_(std::move(responder)) {
        auto handler = [this](const httplib::Request& req, httplib::Response& res) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                calls_.push_back({req.method, req.target, req.body});
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
        if (thread_.joinable()) thread_.join();
    }

    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    std::vector<RecordedCall> calls() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return calls_;
    }

    /// The query string of the first POP, exactly as it arrived. Consume also
    /// acks, and the ack is not under test here.
    std::string first_pop_query() const {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& call : calls_) {
            if (call.target.rfind("/api/v1/pop", 0) == 0) {
                const auto at = call.target.find('?');
                return at == std::string::npos ? "" : call.target.substr(at + 1);
            }
        }
        return "<no pop request>";
    }

    size_t pop_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        size_t n = 0;
        for (const auto& call : calls_) {
            if (call.target.rfind("/api/v1/pop", 0) == 0) ++n;
        }
        return n;
    }
};

/// Cancels a consume loop after a budget, so a loop that fails to stop on its
/// own FAILS its test instead of hanging it. Hand token() to the builder's
/// cancel() before consume().
class StopAfter {
private:
    Cancel token_;
    std::atomic<bool> done_{false};
    std::thread timer_;

public:
    explicit StopAfter(int budget_millis) {
        timer_ = std::thread([this, budget_millis]() {
            auto deadline = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(budget_millis);
            while (!done_.load() && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
            token_.cancel();
        });
    }

    ~StopAfter() {
        done_.store(true);
        if (timer_.joinable()) timer_.join();
    }

    const Cancel& token() const { return token_; }
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

ClientConfig fast_config() {
    ClientConfig config;
    config.retry_429.base_millis = 1;
    config.retry_429.cap_millis = 2;
    return config;
}

// The shared spine of every case: a named queue and group, no long poll, default
// timeout. Everything that varies below is sizing. Split in two because the
// harness sees the query key-sorted (see the caveat at the top of this file), so
// `partitions` lands between the group and the tail.
static const std::string GROUP = "consumerGroup=workers";
static const std::string TAIL = "timeout=30000&wait=false";

/// One sizing case, run through BOTH param builders.
struct SizingCase {
    std::string name;
    std::function<void(QueueBuilder&)> build;
    std::string want;
};

static const std::vector<SizingCase> SIZING_CASES = {
    // Nothing set: both knobs go to the broker, neither travels.
    {"nothing set", [](QueueBuilder&) {}, "autopilot=true&" + GROUP + "&" + TAIL},
    // A pinned width travels, the batch is delegated.
    {"partitions only", [](QueueBuilder& b) { b.partitions(4); },
     "autopilot=true&" + GROUP + "&partitions=4&" + TAIL},
    // The pin that used to be indistinguishable from unset.
    {"partitions pinned to one", [](QueueBuilder& b) { b.partitions(1); },
     "autopilot=true&" + GROUP + "&partitions=1&" + TAIL},
    // A pinned batch travels, the sweep width is delegated.
    {"batch only", [](QueueBuilder& b) { b.batch(50); },
     "autopilot=true&batch=50&" + GROUP + "&" + TAIL},
    // Both set: nothing left to decide, so no autopilot parameter at all.
    {"both set", [](QueueBuilder& b) { b.batch(50).partitions(4); },
     "batch=50&" + GROUP + "&partitions=4&" + TAIL},
    // ...including the one where the old SDK never emitted partitions=1.
    {"both set, partitions one", [](QueueBuilder& b) { b.batch(50).partitions(1); },
     "batch=50&" + GROUP + "&" + TAIL},
    // The escape hatch: the client-side defaults are back.
    {"autopilot off, nothing set", [](QueueBuilder& b) { b.autopilot(false); },
     "batch=1&" + GROUP + "&" + TAIL},
    {"autopilot off, partitions one",
     [](QueueBuilder& b) { b.autopilot(false).partitions(1); },
     "batch=1&" + GROUP + "&" + TAIL},
    {"autopilot off, both set",
     [](QueueBuilder& b) { b.autopilot(false).batch(50).partitions(4); },
     "batch=50&" + GROUP + "&partitions=4&" + TAIL},
    // autopilot(true) is the default spelled out: it must change nothing.
    {"autopilot on, both set",
     [](QueueBuilder& b) { b.autopilot(true).batch(50).partitions(4); },
     "batch=50&" + GROUP + "&partitions=4&" + TAIL},
    // batch(0) is not "a batch of zero" and never was.
    {"batch zero is unset", [](QueueBuilder& b) { b.batch(0); },
     "autopilot=true&" + GROUP + "&" + TAIL},
};

// ============================================================================
// (a) Param assembly, from BOTH builders
// ============================================================================

void test_pop_param_assembly() {
    for (const auto& tc : SIZING_CASES) {
        StubServer server(pop_with());
        QueenClient client({server.url()}, fast_config());

        auto builder = client.queue("orders");
        builder.group("workers").wait(false);
        tc.build(builder);
        builder.pop();

        check(server.first_pop_query() == tc.want,
              "pop [" + tc.name + "]\n      got:  " + server.first_pop_query() +
                  "\n      want: " + tc.want);
    }
}

void test_consume_param_assembly() {
    for (const auto& tc : SIZING_CASES) {
        StubServer server(pop_with());
        QueenClient client({server.url()}, fast_config());
        StopAfter budget(3000);

        auto builder = client.queue("orders");
        builder.group("workers").wait(false).limit(1).cancel(budget.token());
        tc.build(builder);
        const ConsumeSummary summary = builder.consume([](const Message&) {});

        check(server.first_pop_query() == tc.want,
              "consume [" + tc.name + "]\n      got:  " + server.first_pop_query() +
                  "\n      want: " + tc.want);
        check(summary.stopped_by == StopReason::Limit,
              "consume [" + tc.name + "] did not stop at its limit");
    }
}

// ============================================================================
// (b) The process-wide rollback
// ============================================================================

void test_env_var_vocabulary() {
    for (const char* v : {"off", "OFF", " off ", "false", "0", "no", "disabled"}) {
        check(util::autopilot_disabled_by_value(v),
              std::string(v) + " should disable autopilot");
    }
    for (const char* v : {"", "on", "true", "1", "yes", "nonsense"}) {
        check(!util::autopilot_disabled_by_value(v),
              std::string(v) + " should leave autopilot on");
    }
}

void test_env_var_is_read_once_at_construction() {
    // A client built while the variable is set sends the pre-autopilot request.
    setenv(util::ENV_POP_AUTOPILOT, "off", 1);
    StubServer rolled_back_server(pop_with());
    QueenClient rolled_back({rolled_back_server.url()}, fast_config());
    unsetenv(util::ENV_POP_AUTOPILOT);

    // ...and a client built after it was cleared is back on autopilot, while the
    // first one stays rolled back: this is a deployment-level switch, not a
    // per-request one.
    StubServer live_server(pop_with());
    QueenClient live({live_server.url()}, fast_config());

    rolled_back.queue("orders").group("workers").wait(false).pop();
    live.queue("orders").group("workers").wait(false).pop();

    check(rolled_back_server.first_pop_query() == "batch=1&" + GROUP + "&" + TAIL,
          "the environment switch did not roll the client back: " +
              rolled_back_server.first_pop_query());
    check(live_server.first_pop_query() == "autopilot=true&" + GROUP + "&" + TAIL,
          "a client built after the variable was cleared must be on autopilot: " +
              live_server.first_pop_query());
}

void test_explicit_autopilot_outranks_the_environment() {
    setenv(util::ENV_POP_AUTOPILOT, "off", 1);
    StubServer server(pop_with());
    QueenClient client({server.url()}, fast_config());
    unsetenv(util::ENV_POP_AUTOPILOT);

    client.queue("orders").group("workers").wait(false).autopilot(true).pop();

    check(server.first_pop_query() == "autopilot=true&" + GROUP + "&" + TAIL,
          "an explicit autopilot(true) must outrank the environment: " +
              server.first_pop_query());
}

// ============================================================================
// (c) The additive response field
// ============================================================================

void test_parse_autopilot_decision() {
    check(!util::parse_autopilot_decision(json::object()).present, "absent");
    check(!util::parse_autopilot_decision(json{{"autopilot", nullptr}}).present, "null");
    check(!util::parse_autopilot_decision(json{{"autopilot", true}}).present,
          "not an object");
    check(!util::parse_autopilot_decision(json{{"autopilot", json::array()}}).present,
          "an array is not an object");

    auto full = util::parse_autopilot_decision(
        json{{"autopilot", {{"partitions", 8}, {"batch", 200}, {"waitMs", 25}}}});
    check(full.present && full.partitions == 8 && full.batch == 200 && full.wait_millis == 25,
          "a complete echo must be read verbatim");

    // waitMs is optional: the broker sends it only when it has an opinion.
    auto no_wait =
        util::parse_autopilot_decision(json{{"autopilot", {{"partitions", 4}, {"batch", 64}}}});
    check(no_wait.present && no_wait.partitions == 4 && no_wait.batch == 64 &&
              no_wait.wait_millis == 0,
          "a missing waitMs is no advice, not a failure");

    // Forward compatibility: a newer broker growing a field must not cost this
    // client the fields it does understand.
    auto unknown = util::parse_autopilot_decision(
        json{{"autopilot",
              {{"partitions", 2}, {"batch", 10}, {"waitMs", 5}, {"reason", "ready_age"}}}});
    check(unknown.present && unknown.partitions == 2 && unknown.batch == 10 &&
              unknown.wait_millis == 5,
          "an unknown neighbour must not cost the known fields");

    // A field of the wrong type is dropped, not fatal.
    auto wrong = util::parse_autopilot_decision(
        json{{"autopilot", {{"partitions", "eight"}, {"batch", 10}}}});
    check(wrong.present && wrong.partitions == 0 && wrong.batch == 10,
          "a wrongly typed field is dropped, not fatal");
}

void test_pop_result_reports_what_the_broker_chose() {
    StubServer server(pop_with(R"(,"autopilot":{"partitions":8,"batch":200,"waitMs":25})"));
    QueenClient client({server.url()}, fast_config());

    auto res = client.queue("orders").group("workers").wait(false).pop_result();

    check(res.messages.size() == 1, "the messages still come back");
    check(res.autopilot.present, "the echo must be reported");
    check(res.autopilot.partitions == 8 && res.autopilot.batch == 200 &&
              res.autopilot.wait_millis == 25,
          "the echo must be reported verbatim");
}

void test_an_absent_echo_is_not_an_error() {
    StubServer server(pop_with());
    QueenClient client({server.url()}, fast_config());

    auto res = client.queue("orders").group("workers").wait(false).pop_result();

    check(res.messages.size() == 1, "a broker older than 1.2 still delivers");
    check(!res.autopilot.present, "and says nothing about autopilot");
}

void test_pop_still_returns_just_the_messages() {
    StubServer server(pop_with(R"(,"autopilot":{"partitions":8,"batch":200})"));
    QueenClient client({server.url()}, fast_config());

    // 2.7.1: the messages are typed (Message); the echo is pop_result()'s.
    std::vector<Message> messages = client.queue("orders").group("workers").wait(false).pop();

    check(messages.size() == 1, "pop() keeps returning just the messages whatever the response grew");
    if (messages.size() != 1) return;
    const Message& m = messages[0];
    check(m.id == "m1" && m.transaction_id == "t1" && m.data == json{{"n", 1}} &&
              m.partition_id == "p1" && m.lease_id == "lease-1" && m.consumer_group == "workers",
          "the message, its group from the answer around it: " + m.to_json().dump());
}

// ============================================================================
// (d) Empty-poll pacing
// ============================================================================

void test_empty_poll_delay() {
    util::AutopilotDecision none;
    check(util::empty_poll_delay_millis(none) == util::EMPTY_POLL_BACKOFF_MILLIS,
          "no echo means the historical delay");

    util::AutopilotDecision no_advice;
    no_advice.present = true;
    check(util::empty_poll_delay_millis(no_advice) == util::EMPTY_POLL_BACKOFF_MILLIS,
          "an echo without waitMs means the historical delay");

    util::AutopilotDecision advised;
    advised.present = true;
    advised.wait_millis = 250;
    check(util::empty_poll_delay_millis(advised) == 250, "the advice is honoured as given");
}

/// The advice replaces the loop's own delay between empty NON-waiting pops. It
/// shows only in the gap between two pops, so the assertion is a comparison
/// against the same loop with no advice: an absolute pop count would be a claim
/// about the machine, this is a claim about the client.
void test_the_advice_slows_the_empty_poll_loop() {
    auto pops_in_a_window = [](const std::string& extra) -> size_t {
        StubServer server(empty_pop_with(extra));
        QueenClient client({server.url()}, fast_config());
        StopAfter budget(600);
        client.queue("orders").group("workers").wait(false).cancel(budget.token())
            .consume([](const Message&) {});
        return server.pop_count();
    };

    const size_t advised = pops_in_a_window(R"(,"autopilot":{"partitions":1,"batch":100,"waitMs":400})");
    const size_t historical = pops_in_a_window("");

    check(advised < historical,
          "a 400ms advice must slow the loop below its own 100ms default: " +
              std::to_string(advised) + " advised vs " + std::to_string(historical) +
              " unadvised");
    check(advised >= 1, "the loop has to poll at least once");
}

// ============================================================================
// (e) The rule itself, in isolation
// ============================================================================

void test_pop_sizing_rule() {
    auto s = util::pop_sizing(0, 0, 1, true);
    check(s.autopilot && !s.has_batch && !s.has_partitions,
          "nothing set: both knobs delegated, neither travels");

    s = util::pop_sizing(50, 0, 1, true);
    check(s.autopilot && s.has_batch && s.batch == 50 && !s.has_partitions,
          "a pinned batch travels, the width is delegated");

    s = util::pop_sizing(0, 1, 1, true);
    check(s.autopilot && !s.has_batch && s.has_partitions && s.partitions == 1,
          "a width pinned to one travels under autopilot");

    s = util::pop_sizing(50, 4, 1, true);
    check(!s.autopilot && s.has_batch && s.batch == 50 && s.has_partitions && s.partitions == 4,
          "both set: nothing to decide, so the flag does not travel");

    s = util::pop_sizing(0, 0, 1, false);
    check(!s.autopilot && s.has_batch && s.batch == 1 && !s.has_partitions,
          "off: the client-side default comes back");

    s = util::pop_sizing(0, 1, 1, false);
    check(!s.has_partitions, "off: partitions=1 is still the absence of the key");
}

// ============================================================================
// (f) Every key at once, autopilot off
// ============================================================================

/// The sizing cases set the sizing keys alone. With every other key a pop can
/// carry set too, autopilot off still sends exactly the pre-autopilot set --
/// and so does consume(), whose loop fills its own copy of the pop target from
/// the consume options: a field forgotten on that road is a consumer that pops
/// a different request than pop() does. (Key-sorted, as everywhere here; the
/// values need no escaping, so what is pinned is the set, not httplib's.)
static const std::string EVERY_KEY =
    "batch=50&conflation=true&consumerGroup=workers&namespace=ns&partitions=4"
    "&subscriptionFrom=2024-01-01T00:00:00Z&subscriptionMode=all&task=t&timeout=1500&wait=true";

QueueBuilder& set_every_key(QueueBuilder& b) {
    return b.group("workers").namespace_name("ns").task("t").autopilot(false).batch(50)
        .partitions(4).wait(true).timeout_millis(1500).subscription_mode("all")
        .subscription_from("2024-01-01T00:00:00Z").conflation(true);
}

void test_every_key_with_autopilot_off() {
    {
        // The conflation echo, or the pop rightly refuses the answer.
        StubServer server(pop_with(R"(,"conflation":true)"));
        QueenClient client({server.url()}, fast_config());

        auto builder = client.queue("orders");
        set_every_key(builder).commit_on_delivery(true).pop();

        const std::string want = "autoAck=true&" + EVERY_KEY;
        check(server.first_pop_query() == want,
              "pop\n      got:  " + server.first_pop_query() + "\n      want: " + want);
    }
    {
        // consume() always leases, so no autoAck.
        StubServer server(pop_with(R"(,"conflation":true)"));
        QueenClient client({server.url()}, fast_config());
        StopAfter budget(3000);

        auto builder = client.queue("orders");
        set_every_key(builder).limit(1).cancel(budget.token());
        builder.consume([](const Message&) {});

        check(server.first_pop_query() == EVERY_KEY,
              "consume\n      got:  " + server.first_pop_query() + "\n      want: " + EVERY_KEY);
    }
}

// ============================================================================
// main
// ============================================================================

int main() {
    std::cout << BLUE << "Queen C++ client - pop autopilot wire tests" << RESET
              << std::endl
              << std::endl;

    run_test("pop() assembles every sizing case", test_pop_param_assembly);
    run_test("consume() assembles them identically", test_consume_param_assembly);

    run_test("the environment vocabulary", test_env_var_vocabulary);
    run_test("the environment switch is read once at construction",
             test_env_var_is_read_once_at_construction);
    run_test("an explicit autopilot() outranks the environment",
             test_explicit_autopilot_outranks_the_environment);

    run_test("the echo is parsed leniently", test_parse_autopilot_decision);
    run_test("pop_result() reports what the broker chose",
             test_pop_result_reports_what_the_broker_chose);
    run_test("an absent echo is not an error", test_an_absent_echo_is_not_an_error);
    run_test("pop() returns just the messages", test_pop_still_returns_just_the_messages);

    run_test("the empty-poll delay honours the advice", test_empty_poll_delay);
    run_test("the advice slows the real consume loop",
             test_the_advice_slows_the_empty_poll_loop);

    run_test("the sizing rule itself", test_pop_sizing_rule);
    run_test("every key at once, autopilot off, from both entry points",
             test_every_key_with_autopilot_off);

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << GREEN << "All pop autopilot wire tests passed" << RESET << std::endl;
    } else {
        std::cout << RED << failures << " check(s) failed" << RESET << std::endl;
    }

    return failures > 0 ? 1 : 0;
}
