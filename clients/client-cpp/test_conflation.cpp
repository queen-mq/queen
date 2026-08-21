/**
 * Queen C++ Client - conflation wire-contract test suite
 *
 * PLAN_CONFLATION.md, the client-cpp half: §4 (the option, both param builders,
 * degrade-loudly, the one-warning-per-group conflict rule), §3.1 (the wire
 * spelling and the two response keys).
 *
 * WHAT THIS FILE IS FOR.
 * Three things, none of which an end-to-end run against a live broker can see:
 *
 *   1. `conflation=true` reaches the wire from BOTH `pop()` and `consume()`.
 *      Those are two separate param builders in this SDK -- the standing hazard
 *      called out at client-js/.../QueueBuilder.js:395-402, left behind by a bug
 *      of exactly this shape -- so an option wired into one and not the other is
 *      the default failure, not an exotic one. Against a live broker the half
 *      that forgot the flag simply delivers whole batches and passes.
 *
 *   2. Degrade-loudly (§4, the blockquote). A new SDK against a broker that
 *      predates 1.1.0 sends `conflation=true`, the broker ignores the unknown
 *      query param, and the consumer silently drains the entire backlog message
 *      by message. The only evidence available to the client is the ABSENCE of
 *      `"conflation":true` on the response -- so the check is the feature, and it
 *      has to fire on the FIRST round trip, before a single message is handled.
 *      A live 1.1.0 broker can never produce that response, so only a stub can.
 *
 *   3. The conflict warning fires exactly ONCE per (queue, group) per process
 *      (§3.3). "Exactly once" is unobservable in an integration test by
 *      construction: one warning and a thousand look identical from the broker
 *      side.
 *
 * These run against an in-process httplib::Server on localhost -- no broker, no
 * Postgres, so the suite is self-contained:
 *
 *   make conflation && ./bin/test_conflation
 *
 * Siblings: test_retry429.cpp, test_kv_timers.cpp -- same shape, same house
 * rules.
 */

#include "queen_client.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <vector>
#include <string>
#include <sstream>

using namespace queen;
using json = nlohmann::json;

#define GREEN "\033[32m"
#define RED "\033[31m"
#define BLUE "\033[34m"
#define RESET "\033[0m"

// ============================================================================
// Test double: records every request verbatim and answers from a responder
// ============================================================================

struct RecordedCall {
    std::string method;
    std::string target;                      // raw request target, query string included
    std::string body;
};

using Responder = std::function<void(const httplib::Request&, httplib::Response&)>;

/// A 1.1.0 broker answering a CONFLATING pop that delivered nothing: 200 with a
/// body, not 204, because the `"conflation":true` echo is what lets the SDK tell
/// a new broker from an old one on the first round trip (§3.1, `pop_status`).
inline void conflating_empty(const httplib::Request&, httplib::Response& res) {
    res.status = 200;
    res.set_content(R"({"messages":[],"partitionsClaimed":0,"conflation":true})",
                    "application/json");
}

/// A 1.1.0 broker answering a conflating pop that delivered its one tail frame.
inline void conflating_message(const httplib::Request&, httplib::Response& res) {
    res.status = 200;
    res.set_content(R"({"messages":[{"id":"m1","transactionId":"t1","data":{"n":1},)"
                    R"("queue":"q","partition":"Default"}],"partitionsClaimed":1,)"
                    R"("conflation":true})",
                    "application/json");
}

/// A pre-1.1.0 broker: the unknown `conflation` query param is ignored and the
/// whole backlog comes back, with no echo anywhere in the response.
inline void old_broker_message(const httplib::Request&, httplib::Response& res) {
    res.status = 200;
    res.set_content(R"({"messages":[{"id":"m1","transactionId":"t1","data":{"n":1},)"
                    R"("queue":"q","partition":"Default"}],"partitionsClaimed":1})",
                    "application/json");
}

/// A pre-1.1.0 broker with nothing to deliver: a bodiless 204, which the HTTP
/// layer turns into a JSON null. This is the shape the degrade check meets FIRST
/// on an idle queue, so it has to be caught by the null branch and not only by
/// the "object without the key" branch.
inline void old_broker_204(const httplib::Request&, httplib::Response& res) {
    res.status = 204;
}

/// §3.3, the conflict this SDK can actually provoke: it only ever sends
/// `conflation=true`, so the disagreement is always requested=true against a
/// group stored as false. The STORED value wins, so there is no `"conflation"`
/// key -- but `"conflationConflict":true` is present, and that key is proof the
/// broker understands conflation. Warn; never the version error.
inline void conflict_message(const httplib::Request&, httplib::Response& res) {
    res.status = 200;
    res.set_content(R"({"messages":[{"id":"m1","transactionId":"t1","data":{"n":1},)"
                    R"("queue":"q","partition":"Default"}],"partitionsClaimed":1,)"
                    R"("conflationConflict":true})",
                    "application/json");
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
    explicit StubServer(Responder responder)
        : responder_(std::move(responder)) {
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
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }

    std::vector<RecordedCall> calls() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return calls_;
    }

    RecordedCall last() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return calls_.empty() ? RecordedCall{} : calls_.back();
    }

    size_t count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return calls_.size();
    }
};

/// Raises a stop signal after a budget, so a consume loop that fails to stop on
/// its own FAILS its test instead of hanging it. Not a nicety: the two
/// degrade-loudly consume tests are exactly the tests whose broken state is an
/// endless poll, and a suite that hangs reports nothing at all -- no failure
/// count, no name, no exit code. Every test here also bounds itself the ordinary
/// way (limit / idle_millis); this is the backstop for the case where the loop
/// ignores both because it never stops polling.
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
    bool fired() const { return flag_.load(); }
};

/// Redirects std::cerr for the duration of the scope. The conflict warning is
/// deliberately NOT gated behind QUEEN_CLIENT_LOG (§3.3: a warning nobody sees
/// is the silence M3 objects to), so this is how "exactly once" is counted.
class CerrCapture {
private:
    std::ostringstream buffer_;
    std::streambuf* saved_;

public:
    CerrCapture() : saved_(std::cerr.rdbuf(buffer_.rdbuf())) {}
    ~CerrCapture() { std::cerr.rdbuf(saved_); }

    std::string text() const { return buffer_.str(); }
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

/// Counts non-overlapping occurrences of `needle` in `haystack`.
size_t occurrences(const std::string& haystack, const std::string& needle) {
    if (needle.empty()) return 0;
    size_t n = 0;
    for (size_t at = haystack.find(needle); at != std::string::npos;
         at = haystack.find(needle, at + needle.size())) {
        ++n;
    }
    return n;
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
// (a) The option reaches the wire -- from BOTH builders
// ============================================================================

void test_pop_sends_conflation_when_declared() {
    StubServer server(conflating_empty);
    QueenClient client({server.url()}, fast_config());

    client.queue("cfl-pop-on").group("workers").conflation(true).wait(false).pop();

    check(server.count() == 1, "pop() should make exactly one request, got " +
          std::to_string(server.count()));
    check(server.last().target.find("conflation=true") != std::string::npos,
          "pop() must carry conflation=true, got: " + server.last().target);
    check(server.last().target.find("consumerGroup=workers") != std::string::npos,
          "pop() must still carry the consumer group, got: " + server.last().target);
}

void test_consume_sends_conflation_when_declared() {
    // limit(1) plus a one-message response ends the loop after a single round
    // trip, the idiom test_retry429.cpp uses for the same purpose.
    StubServer server(conflating_message);
    QueenClient client({server.url()}, fast_config());

    client.queue("cfl-consume-on")
          .group("workers")
          .conflation(true)
          .wait(false)
          .limit(1)
          .consume([](const json&) {});

    check(server.count() >= 1, "consume() should make at least one request");
    check(server.calls().front().target.find("conflation=true") != std::string::npos,
          "consume() must carry conflation=true, got: " + server.calls().front().target);
    check(server.calls().front().target.find("consumerGroup=workers") != std::string::npos,
          "consume() must still carry the consumer group, got: " +
          server.calls().front().target);
}

void test_pop_omits_conflation_when_not_declared() {
    // Byte-compatibility with every deployment that never heard of the flag: a
    // pop that did not ask for conflation must put NOTHING extra on the wire.
    StubServer server(old_broker_message);
    QueenClient client({server.url()}, fast_config());

    client.queue("cfl-pop-off").group("workers").wait(false).pop();

    check(server.last().target.find("conflation") == std::string::npos,
          "an undeclared pop must not mention conflation, got: " + server.last().target);
}

void test_consume_omits_conflation_when_not_declared() {
    StubServer server(old_broker_message);
    QueenClient client({server.url()}, fast_config());

    client.queue("cfl-consume-off").group("workers").wait(false).limit(1)
          .consume([](const json&) {});

    check(server.calls().front().target.find("conflation") == std::string::npos,
          "an undeclared consume must not mention conflation, got: " +
          server.calls().front().target);
}

void test_conflation_false_is_not_sent() {
    // §3.1/queen-protocol: the param is emitted ONLY when true. `conflation=false`
    // would read as "turn this group's policy off", which is not a thing a pop is
    // allowed to do -- the stored setting wins and a false request would merely
    // book a conflict against a conflating group.
    StubServer server(old_broker_message);
    QueenClient client({server.url()}, fast_config());

    client.queue("cfl-pop-false").group("workers").conflation(false).wait(false).pop();

    check(server.last().target.find("conflation") == std::string::npos,
          "conflation(false) must send nothing, got: " + server.last().target);
}

void test_builder_copies_conflation_into_consume_options() {
    // The builder->ConsumeOptions copy is its own line of code and its own way to
    // go wrong: `pop()` reads the builder field directly, `consume()` reads it
    // through the copy. Assert the option's default and that the copy carries a
    // set value.
    ConsumeOptions defaults;
    check(defaults.conflation == false, "ConsumeOptions.conflation must default to false");

    StubServer server(conflating_message);
    QueenClient client({server.url()}, fast_config());

    ConsumeOptions options;
    options.queue = "cfl-options";
    options.group = "workers";
    options.conflation = true;
    options.wait = false;
    options.limit = 1;

    ConsumerManager manager(client.get_http_client(), &client);
    manager.start([](const json&) {}, options);

    check(server.calls().front().target.find("conflation=true") != std::string::npos,
          "ConsumeOptions.conflation must reach the wire, got: " +
          server.calls().front().target);
}

// ============================================================================
// (b) Degrade loudly -- §4, the blockquote
// ============================================================================

void test_pop_raises_when_the_broker_does_not_echo_conflation() {
    StubServer server(old_broker_message);
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    try {
        client.queue("cfl-degrade-pop").group("workers").conflation(true).wait(false).pop();
    } catch (const ConflationUnsupportedError& e) {
        threw = true;
        const std::string message = e.what();
        // The canonical wording of PLAN_CONFLATION §4, byte-identical in all
        // seven SDKs so one grep finds every language. Pinned here because a
        // message this specific is exactly the kind of thing a later edit
        // rephrases without noticing it is a cross-SDK contract.
        const std::string canonical =
            "conflation was requested but this broker did not apply it \xE2\x80\x94 "
            "requires broker >= 1.1.0";
        check(message.rfind(canonical, 0) == 0,
              "the error must open with the canonical message, got: " + message);
        check(e.queue() == "cfl-degrade-pop" && e.group() == "workers",
              "the error must carry the queue and group, got '" + e.queue() + "'/'" +
              e.group() + "'");
    }

    check(threw, "pop() must raise when conflation was requested and not echoed");
}

void test_pop_raises_on_an_old_brokers_204() {
    // The first round trip against an idle queue. A 204 carries no body at all,
    // so the check has to fire on the JSON null and not only on a parsed object.
    StubServer server(old_broker_204);
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    try {
        client.queue("cfl-degrade-204").group("workers").conflation(true).wait(false).pop();
    } catch (const ConflationUnsupportedError&) {
        threw = true;
    }

    check(threw, "a bodiless 204 is an old broker and must raise");
    check(server.count() == 1, "the check must fire on the FIRST round trip, got " +
          std::to_string(server.count()) + " requests");
}

void test_pop_is_quiet_when_the_broker_echoes_conflation() {
    StubServer server(conflating_message);
    QueenClient client({server.url()}, fast_config());

    json messages = client.queue("cfl-echo-ok").group("workers")
                          .conflation(true).wait(false).pop();

    check(messages.is_array() && messages.size() == 1,
          "an echoed conflating pop must return its message");
}

void test_pop_is_quiet_when_conflation_was_never_requested() {
    // The mirror case, and the one that must NOT regress: an SDK that did not ask
    // for conflation has no expectations of the response and every existing
    // deployment keeps working.
    StubServer server(old_broker_204);
    QueenClient client({server.url()}, fast_config());

    json messages = client.queue("cfl-noask").group("workers").wait(false).pop();

    check(messages.is_array() && messages.empty(),
          "an undeclared pop against a 204 must return [] and not raise");
}

void test_consume_stops_on_a_broker_that_does_not_conflate() {
    // The whole point of the error: the loop STOPS instead of quietly draining a
    // backlog one message at a time. Same shape as the terminal-403 test in
    // test_retry429.cpp -- the error has to survive the worker thread and reach
    // the caller of consume().
    StubServer server(old_broker_message);
    QueenClient client({server.url()}, fast_config());

    std::atomic<int> handled{0};
    bool threw = false;
    StopAfter watchdog(3000);
    try {
        // limit(3) bounds a loop that keeps handling messages; the watchdog
        // bounds one that would poll for ever. Neither is reached by a correct
        // client: the error fires on the first response.
        client.queue("cfl-degrade-consume").group("workers").conflation(true)
              .wait(false).limit(3)
              .consume([&handled](const json&) { handled++; }, watchdog.signal());
    } catch (const ConflationUnsupportedError& e) {
        threw = true;
        check(std::string(e.what()).find("1.1.0") != std::string::npos,
              "the error must name the required broker version, got: " +
              std::string(e.what()));
    }

    check(threw, "consume() must stop and rethrow when the broker does not conflate");
    check(handled.load() == 0,
          "not one message may be handled before the loop stops, handled " +
          std::to_string(handled.load()));
    check(server.count() == 1, "the loop must not re-poll, got " +
          std::to_string(server.count()) + " requests");
}

void test_consume_stops_on_an_old_brokers_204() {
    // An idle queue is the likeliest first contact, and the 204 path is where a
    // consume loop would otherwise spin forever without ever noticing.
    StubServer server(old_broker_204);
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    StopAfter watchdog(3000);
    try {
        // An empty response never counts towards limit(), so idle_millis is what
        // bounds this loop; the watchdog covers the case where even that is not
        // reached. Neither is reached by a correct client.
        client.queue("cfl-degrade-consume-204").group("workers").conflation(true)
              .wait(false).idle_millis(600)
              .consume([](const json&) {}, watchdog.signal());
    } catch (const ConflationUnsupportedError&) {
        threw = true;
    }

    check(threw, "an idle old broker must stop the consume loop on the first poll");
    check(server.count() == 1, "the loop must not re-poll, got " +
          std::to_string(server.count()) + " requests");
}

// ============================================================================
// (c) The conflict warning -- once per (queue, group) per process (§3.3)
// ============================================================================

void test_conflict_warns_exactly_once_per_queue_and_group() {
    StubServer server(conflict_message);
    QueenClient client({server.url()}, fast_config());

    std::string captured;
    {
        CerrCapture capture;
        for (int i = 0; i < 3; ++i) {
            client.queue("cfl-conflict-a").group("workers")
                  .conflation(true).wait(false).pop();
        }
        captured = capture.text();
    }

    check(server.count() == 3, "all three pops should have reached the server, got " +
          std::to_string(server.count()));
    check(occurrences(captured, "[conflation]") == 1,
          "exactly one warning per (queue, group) per process, got " +
          std::to_string(occurrences(captured, "[conflation]")) + ":\n" + captured);
    check(captured.find("cfl-conflict-a") != std::string::npos,
          "the warning must name the queue, got:\n" + captured);
    check(captured.find("workers") != std::string::npos,
          "the warning must name the group, got:\n" + captured);
}

void test_conflict_warns_again_for_a_different_group() {
    // Per (queue, GROUP): two groups on one queue are two policies and two
    // warnings. A process-wide once would hide the second misconfiguration.
    StubServer server(conflict_message);
    QueenClient client({server.url()}, fast_config());

    std::string captured;
    {
        CerrCapture capture;
        client.queue("cfl-conflict-b").group("alpha").conflation(true).wait(false).pop();
        client.queue("cfl-conflict-b").group("alpha").conflation(true).wait(false).pop();
        client.queue("cfl-conflict-b").group("beta").conflation(true).wait(false).pop();
        captured = capture.text();
    }

    check(occurrences(captured, "[conflation]") == 2,
          "two groups on one queue must warn twice, got " +
          std::to_string(occurrences(captured, "[conflation]")) + ":\n" + captured);
}

void test_conflict_does_not_raise_the_version_error() {
    // `conflationConflict` can only come from a broker that speaks conflation, so
    // the version error would be a lie. §3.3/Q3: the group setting wins and the
    // SDK warns -- rejecting here is what breaks a rolling deploy.
    StubServer server(conflict_message);
    QueenClient client({server.url()}, fast_config());

    CerrCapture capture;
    json messages = client.queue("cfl-conflict-c").group("workers")
                          .conflation(true).wait(false).pop();

    check(messages.is_array() && messages.size() == 1,
          "a conflicting pop still delivers its messages");
}

void test_no_warning_when_the_broker_applied_conflation() {
    StubServer server(conflating_message);
    QueenClient client({server.url()}, fast_config());

    std::string captured;
    {
        CerrCapture capture;
        client.queue("cfl-noconflict").group("workers").conflation(true).wait(false).pop();
        captured = capture.text();
    }

    check(occurrences(captured, "[conflation]") == 0,
          "an applied conflating pop must warn about nothing, got:\n" + captured);
}

void test_conflict_warning_reaches_the_consume_loop_too() {
    // Same two-builder hazard as the option itself: the warning is wired into
    // pop() and consume() separately.
    StubServer server(conflict_message);
    QueenClient client({server.url()}, fast_config());

    std::string captured;
    {
        CerrCapture capture;
        client.queue("cfl-conflict-consume").group("workers").conflation(true)
              .wait(false).limit(1)
              .consume([](const json&) {});
        captured = capture.text();
    }

    check(occurrences(captured, "[conflation]") == 1,
          "consume() must warn once on a conflict, got " +
          std::to_string(occurrences(captured, "[conflation]")) + ":\n" + captured);
}

// ============================================================================

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "Queen C++ Client - Conflation Wire Tests" << std::endl;
    std::cout << "========================================\n" << std::endl;

    run_test("pop() sends conflation=true", test_pop_sends_conflation_when_declared);
    run_test("consume() sends conflation=true", test_consume_sends_conflation_when_declared);
    run_test("pop() omits it when undeclared", test_pop_omits_conflation_when_not_declared);
    run_test("consume() omits it when undeclared", test_consume_omits_conflation_when_not_declared);
    run_test("conflation(false) is not sent", test_conflation_false_is_not_sent);
    run_test("the builder copies it into ConsumeOptions",
             test_builder_copies_conflation_into_consume_options);

    run_test("pop() raises when the echo is missing",
             test_pop_raises_when_the_broker_does_not_echo_conflation);
    run_test("pop() raises on an old broker's 204", test_pop_raises_on_an_old_brokers_204);
    run_test("pop() is quiet when the echo is there",
             test_pop_is_quiet_when_the_broker_echoes_conflation);
    run_test("pop() is quiet when conflation was never requested",
             test_pop_is_quiet_when_conflation_was_never_requested);
    run_test("consume() stops on a broker that does not conflate",
             test_consume_stops_on_a_broker_that_does_not_conflate);
    run_test("consume() stops on an old broker's 204",
             test_consume_stops_on_an_old_brokers_204);

    run_test("a conflict warns exactly once per (queue, group)",
             test_conflict_warns_exactly_once_per_queue_and_group);
    run_test("a different group warns again",
             test_conflict_warns_again_for_a_different_group);
    run_test("a conflict is not the version error",
             test_conflict_does_not_raise_the_version_error);
    run_test("an applied conflating pop warns about nothing",
             test_no_warning_when_the_broker_applied_conflation);
    run_test("consume() warns on a conflict too",
             test_conflict_warning_reaches_the_consume_loop_too);

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << GREEN << "All conflation wire tests passed" << RESET << std::endl;
    } else {
        std::cout << RED << failures << " check(s) failed" << RESET << std::endl;
    }

    return failures > 0 ? 1 : 0;
}
