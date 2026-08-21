/**
 * Queen C++ Client - ephemeral queues wire-contract test suite
 *
 * EPHEMERAL_QUEUES.md §3.1 (the wire), §1 (the semantics the doc comments
 * carry), §4 (the SDK surface and the one version error), §7.3 (why this file
 * exists at all).
 *
 * WHAT THIS FILE IS FOR. Everything asserted here is the EXACT JSON body the
 * client puts on the wire, plus the exact method, path and query string it puts
 * it on. That is the contract towards the broker, and it is the only thing that
 * catches a wrong wire shape before production: a push whose messages carried
 * the durable per-item {queue, partition, payload} is a 400 nobody sees until a
 * live broker is involved, and a pop that forgot to send `timeout` beside
 * `wait=true` is a long poll returning on the BROKER's default instead of the
 * caller's, which nothing observes at all.
 *
 * And one thing here can never be produced by an end-to-end run against a single
 * broker, which is the strongest argument for the file: the 404 mapping. A
 * broker or proxy older than 1.1 answers 404 on the whole family, and the SDK
 * has to turn that into one clear "upgrade" verdict rather than let it read as
 * "your queue is missing" -- while a 1.1 broker's OWN 404, the one `depth`
 * answers for a queue that is not there, has to stay the second thing and never
 * become the first.
 *
 * Like its siblings it serves its own responses from an in-process
 * httplib::Server, so it needs neither a broker nor Postgres:
 *
 *   make ephemeral && ./bin/test_ephemeral
 *
 * Siblings: test_kv_timers.cpp, test_conflation.cpp, test_retry429.cpp.
 */

#include "queen_client.hpp"
#include <atomic>
#include <iostream>
#include <memory>
#include <thread>
#include <mutex>
#include <map>
#include <vector>
#include <string>

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
    std::string target;   // raw request target, query string included
    std::string body;
};

using Responder = std::function<void(const httplib::Request&, httplib::Response&)>;

/// The default answer: a superset object satisfying every shape this client
/// reads back. Tests that care about the ANSWER pass their own responder; tests
/// that care about the REQUEST use this one.
inline void default_responder(const httplib::Request&, httplib::Response& res) {
    res.status = 200;
    res.set_content(R"({"pushed":1,"dropped":0,"messages":[],"results":[]})",
                    "application/json");
}

/// What a broker older than 1.1 answers on this family: the routes were never
/// registered.
inline void old_broker(const httplib::Request&, httplib::Response& res) {
    res.status = 404;
    res.set_content(R"({"error":"not_found"})", "application/json");
}

/// And what an old PROXY answers -- the same verdict in the proxy's fail-closed
/// vocabulary, since an unknown API path is `route_blocked`.
inline void old_proxy(const httplib::Request&, httplib::Response& res) {
    res.status = 404;
    res.set_content(R"({"error":"route_blocked","code":"route_blocked"})", "application/json");
}

/// The OTHER 404, and the reason the mapping has to read the body: a broker that
/// fully supports the family, answering `depth` about a queue that is not there.
/// Byte-identical to what handlers/ephemeral.rs writes.
inline void queue_not_found(const httplib::Request&, httplib::Response& res) {
    res.status = 404;
    res.set_content(
        R"({"error":"no ephemeral queue by that name exists on this broker","code":"ephemeral_queue_not_found"})",
        "application/json");
}

class CaptureServer {
private:
    httplib::Server server_;
    std::thread thread_;
    int port_ = 0;
    Responder responder_;

    mutable std::mutex mutex_;
    std::vector<RecordedCall> calls_;

public:
    explicit CaptureServer(Responder responder = default_responder)
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

    ~CaptureServer() {
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

    RecordedCall at(size_t index) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return index < calls_.size() ? calls_[index] : RecordedCall{};
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

void check_equal(const std::string& actual, const std::string& expected, const std::string& what) {
    check(actual == expected,
          what + "\n      expected: " + expected + "\n      actual:   " + actual);
}

/**
 * The query of a recorded target, as name -> value.
 *
 * Why this exists instead of a string comparison on the whole target, as the
 * other SDKs' suites do: cpp-httplib's CLIENT does not put our query on the
 * wire verbatim. It parses it into `Params` -- a std::multimap -- and
 * re-serializes it (httplib.h, `append_query_params` at the end of
 * `Client::send_`), so the parameters arrive sorted by name whatever order this
 * SDK emitted them in. Order is therefore NOT observable from this side of the
 * library, and pinning the string would pin httplib's normalization rather than
 * the client's behaviour. What matters is pinned instead, and pinned exactly:
 * which parameters are present, with which values, and -- just as much a part
 * of §3.1 -- which are ABSENT.
 */
std::map<std::string, std::string> query_of(const std::string& target) {
    std::map<std::string, std::string> params;
    auto mark = target.find('?');
    if (mark == std::string::npos) {
        return params;
    }

    std::string query = target.substr(mark + 1);
    size_t start = 0;
    while (start <= query.size()) {
        size_t end = query.find('&', start);
        std::string pair = query.substr(start, end == std::string::npos ? std::string::npos
                                                                        : end - start);
        if (!pair.empty()) {
            auto eq = pair.find('=');
            params[eq == std::string::npos ? pair : pair.substr(0, eq)] =
                eq == std::string::npos ? "" : pair.substr(eq + 1);
        }
        if (end == std::string::npos) {
            break;
        }
        start = end + 1;
    }
    return params;
}

void check_query(const std::string& target,
                 const std::map<std::string, std::string>& expected,
                 const std::string& what) {
    auto actual = query_of(target);
    if (actual == expected) {
        return;
    }
    ++failures;
    std::cout << RED << "    x " << what << RESET << std::endl;
    std::cout << RED << "      actual query: " << target << RESET << std::endl;
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

/// Client with the 429 backoff shortened, so no test in this file can sleep.
ClientConfig fast_config() {
    ClientConfig config;
    config.retry_attempts = 1;
    config.retry_429.base_millis = 1;
    config.retry_429.cap_millis = 2;
    return config;
}

static const std::string QUEUE = "inbox";

// ============================================================================
// Declaration: configure / reset / del
// ============================================================================

void test_configure_sends_the_queue_and_its_options_under_options() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    EphemeralOptions options;
    options.max_bytes = 1048576;
    options.max_length = 500;
    options.policy = "dropOldest";
    options.ttl_seconds = 30;
    options.lease_seconds = 15;
    options.retry_limit = 3;
    options.window_buffer = json{{"ms", 20}, {"count", 50}};

    client.ephemeral().configure(QUEUE, options);

    check_equal(server.last().method, "POST", "configure is a POST");
    check_equal(server.last().target, "/api/v1/ephemeral/configure", "configure's route");
    check_equal(server.last().body,
                R"({"options":{"leaseSeconds":15,"maxBytes":1048576,"maxLength":500,)"
                R"("policy":"dropOldest","retryLimit":3,"ttlSeconds":30,)"
                R"("windowBuffer":{"count":50,"ms":20}},"queue":"inbox"})",
                "configure's body");
}

void test_configure_sends_only_the_options_it_was_given() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    EphemeralOptions only_ttl;
    only_ttl.ttl_seconds = 30;
    client.ephemeral().configure(QUEUE, only_ttl);
    check_equal(server.at(0).body, R"({"options":{"ttlSeconds":30},"queue":"inbox"})",
                "an unset knob must not travel; the broker's default owns it");

    // The empty declaration is legal and means "exist, with the tenant
    // defaults" -- the dashboard-visible half of §1.1.
    client.ephemeral().configure(QUEUE);
    check_equal(server.at(1).body, R"({"options":{},"queue":"inbox"})",
                "configure with no options at all");
}

void test_reset_and_del_name_the_queue_where_each_route_expects_it() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.ephemeral().reset(QUEUE);
    check_equal(server.at(0).method, "POST", "reset is a POST");
    check_equal(server.at(0).target, "/api/v1/ephemeral/reset", "reset's route");
    check_equal(server.at(0).body, R"({"queue":"inbox"})", "reset names the queue in the body");

    client.ephemeral().del(QUEUE);
    check_equal(server.at(1).method, "DELETE", "del is a DELETE");
    check_equal(server.at(1).target, "/api/v1/ephemeral/queue/inbox",
                "del names the queue in the PATH");
    check(server.at(1).body.empty(), "a DELETE on this route carries no body");
}

void test_percent_encodes_a_queue_name_that_would_change_the_path() {
    // A slash left unencoded turns one queue name into two path segments, which
    // is a different route entirely.
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    // Lowercase hex because that is what util::url_encode has always emitted,
    // here as on the KV and timer paths; percent-encoding is case-insensitive
    // (RFC 3986 §6.2.2.1), so this pins THIS client's spelling and not a
    // requirement of the wire.
    client.ephemeral().del("rooms/7");
    check_equal(server.at(0).target, "/api/v1/ephemeral/queue/rooms%2f7", "del percent-encodes");

    client.ephemeral().depth("rooms/7");
    check_equal(server.at(1).target, "/api/v1/ephemeral/queues/rooms%2f7/depth",
                "depth percent-encodes");
}

void test_refuses_a_missing_queue_name_before_spending_a_request() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());
    auto eph = client.ephemeral();

    std::vector<std::function<void()>> calls = {
        [&] { eph.configure(""); },
        [&] { eph.reset(""); },
        [&] { eph.del(""); },
        [&] { eph.push("", json::array({json{{"a", 1}}})); },
        [&] { eph.pop(""); },
        [&] { eph.ack("", json::array({"e:1"})); },
        [&] { eph.depth(""); },
    };

    for (auto& call : calls) {
        bool threw = false;
        try {
            call();
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        check(threw, "an empty queue name must be refused");
    }

    check(server.count() == 0, "nothing may reach the wire");
}

// ============================================================================
// Push
// ============================================================================

void test_push_sends_the_flat_envelope_with_payload_only_messages() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.ephemeral().push(QUEUE, json::array({json{{"a", 1}}, json{{"a", 2}}}));

    check_equal(server.last().method, "POST", "push is a POST");
    check_equal(server.last().target, "/api/v1/ephemeral/push", "push's route");
    check_equal(server.last().body,
                R"({"messages":[{"payload":{"a":1}},{"payload":{"a":2}}],"queue":"inbox"})",
                "the identity is on the ENVELOPE, and each message is payload-only");
}

void test_push_omits_partition_unless_the_caller_named_one() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.ephemeral().push(QUEUE, json::array({json{{"a", 1}}}));
    check(server.at(0).body.find("partition") == std::string::npos,
          "an unnamed partition must not be invented client-side");

    EphemeralPushOptions options;
    options.partition = "room-7";
    client.ephemeral().push(QUEUE, json::array({json{{"a", 1}}}), options);
    check_equal(server.at(1).body,
                R"({"messages":[{"payload":{"a":1}}],"partition":"room-7","queue":"inbox"})",
                "a named partition travels on the envelope");
}

void test_push_accepts_the_durable_push_sugar() {
    // A bare value, {"data": …} or {"payload": …} -- one mental model across
    // both families, including the trap: an object with a `data` key is read as
    // the sugar and its other keys do not travel.
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.ephemeral().push(QUEUE, json::array({
        "plain",
        7,
        json{{"data", json{{"n", 1}}}},
        json{{"payload", json{{"n", 2}}}},
    }));

    check_equal(server.last().body,
                R"({"messages":[{"payload":"plain"},{"payload":7},{"payload":{"n":1}},)"
                R"({"payload":{"n":2}}],"queue":"inbox"})",
                "all four spellings reduce to {payload}");
}

void test_push_carries_no_transaction_id() {
    // There is no dedup index on this engine to hold one (§9).
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.ephemeral().push(QUEUE, json::array({
        json{{"payload", json{{"n", 1}}}, {"transactionId", "t-1"}},
    }));

    check(server.last().body.find("transactionId") == std::string::npos,
          "a message on this wire is {payload} and nothing else");
}

void test_push_of_nothing_answers_pushed_zero_without_spending_a_request() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    json result = client.ephemeral().push(QUEUE, json::array());
    check(result["pushed"] == 0, "an empty push answers pushed:0");
    check(server.count() == 0, "and spends no request");
}

void test_push_refuses_a_null_message_rather_than_inventing_a_null_payload() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    try {
        client.ephemeral().push(QUEUE, json::array({nullptr}));
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    check(threw, "a null message must be refused");
    check(server.count() == 0, "and refused before the request");

    // ... and the explicit form does travel, because null IS a legal payload.
    client.ephemeral().push(QUEUE, json::array({json{{"payload", nullptr}}}));
    check_equal(server.last().body, R"({"messages":[{"payload":null}],"queue":"inbox"})",
                "an explicit null payload is legal and travels");
}

// ============================================================================
// Pop
// ============================================================================

void test_pop_sends_the_queue_and_nothing_else_by_default() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.ephemeral().pop(QUEUE);
    check_equal(server.last().method, "GET", "pop is a GET");
    check_equal(server.last().target, "/api/v1/ephemeral/pop?queue=inbox",
                "a plain pop is the shortest query this route can receive");
    check_query(server.last().target, {{"queue", "inbox"}},
                "and it carries nothing else at all");
}

void test_pop_puts_every_declared_parameter_on_the_query_string() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    EphemeralPopOptions options;
    options.partition = "room-7";
    options.batch = 10;
    options.wait = true;
    options.timeout_millis = 1500;
    options.group = "workers";
    options.auto_ack = true;

    client.ephemeral().pop(QUEUE, options);
    check_query(server.last().target,
                {{"queue", "inbox"},
                 {"partition", "room-7"},
                 {"batch", "10"},
                 {"wait", "true"},
                 {"timeout", "1500"},
                 {"group", "workers"},
                 {"autoAck", "true"}},
                "every declared parameter reaches the query string with its own value");
}

void test_pop_sends_an_explicit_timeout_whenever_it_waits() {
    // And none when it does not: a plain pop leaves every default to the
    // broker, while a long poll states the deadline it is holding the socket
    // open for.
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    EphemeralPopOptions waiting;
    waiting.wait = true;
    client.ephemeral().pop(QUEUE, waiting);
    check_query(server.at(0).target,
                {{"queue", "inbox"}, {"wait", "true"}, {"timeout", "30000"}},
                "a wait with no timeout states the 30s default explicitly");

    EphemeralPopOptions plain;
    plain.batch = 5;
    plain.timeout_millis = 1500;   // ignored: nothing is waiting
    client.ephemeral().pop(QUEUE, plain);
    check_query(server.at(1).target, {{"queue", "inbox"}, {"batch", "5"}},
                "a non-waiting pop sends neither wait nor timeout");
}

void test_pop_returns_an_empty_array_on_a_timeout_and_on_a_bodiless_204() {
    CaptureServer timeout_server([](const httplib::Request&, httplib::Response& res) {
        res.status = 200;
        res.set_content(R"({"queue":"inbox","messages":[]})", "application/json");
    });
    QueenClient timed_out({timeout_server.url()}, fast_config());

    json empty = timed_out.ephemeral().pop(QUEUE);
    check(empty["messages"].is_array() && empty["messages"].empty(),
          "a long poll that timed out answers an empty ARRAY");
    check(empty["queue"] == "inbox", "and still names its queue");

    CaptureServer no_content([](const httplib::Request&, httplib::Response& res) {
        res.status = 204;
    });
    QueenClient bodiless({no_content.url()}, fast_config());

    json parsed = bodiless.ephemeral().pop(QUEUE);
    check(parsed["messages"].is_array() && parsed["messages"].empty(),
          "a 204 must not become null for the caller to trip over");
    check(parsed["queue"] == "inbox", "and the queue is filled in from the request");
}

void test_pop_hands_back_the_frames_it_was_given() {
    CaptureServer server([](const httplib::Request&, httplib::Response& res) {
        res.status = 200;
        res.set_content(
            R"({"queue":"inbox","messages":[{"id":"e:beef:Default:1","partition":"Default",)"
            R"("payload":{"n":1},"attempts":0}]})",
            "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    json result = client.ephemeral().pop(QUEUE);
    check(result["messages"].size() == 1, "one frame in, one frame out");
    check(result["messages"][0]["id"] == "e:beef:Default:1",
          "the id is opaque and is handed back untouched");
    check(result["messages"][0]["attempts"] == 0, "attempts travels with the frame");
}

// ============================================================================
// Ack
// ============================================================================

void test_ack_sends_the_ids_under_acks_with_the_group_beside_them() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    EphemeralAckOptions options;
    options.group = "workers";
    client.ephemeral().ack(QUEUE, json::array({"e:beef:Default:1"}), options);

    check_equal(server.last().method, "POST", "ack is a POST");
    check_equal(server.last().target, "/api/v1/ephemeral/ack", "ack's route");
    check_equal(server.last().body,
                R"({"acks":[{"id":"e:beef:Default:1"}],"group":"workers","queue":"inbox"})",
                "ack's body");
}

void test_ack_takes_popped_messages_bare_ids_or_the_wire_objects() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    // A whole popped frame: only the id travels.
    client.ephemeral().ack(QUEUE, json{{"id", "e:beef:Default:9"},
                                       {"partition", "Default"},
                                       {"payload", json{{"n", 9}}},
                                       {"attempts", 0}});
    check_equal(server.at(0).body, R"({"acks":[{"id":"e:beef:Default:9"}],"queue":"inbox"})",
                "a popped frame reduces to its id");

    client.ephemeral().ack(QUEUE, json::array({
        "e:1",
        json{{"id", "e:2"}, {"status", "retry"}},
    }));
    check_equal(server.at(1).body,
                R"({"acks":[{"id":"e:1"},{"id":"e:2","status":"retry"}],"queue":"inbox"})",
                "bare ids and wire objects mix in one batch");
}

void test_ack_lets_a_per_message_status_win_over_the_call_wide_one() {
    // Which is how a mixed batch -- some completed, one retry -- travels in a
    // single request.
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    EphemeralAckOptions options;
    options.status = "completed";
    client.ephemeral().ack(QUEUE, json::array({
        "e:1",
        json{{"id", "e:2"}, {"status", "retry"}, {"error", "downstream 503"}},
    }), options);

    check_equal(server.last().body,
                R"({"acks":[{"id":"e:1","status":"completed"},)"
                R"({"error":"downstream 503","id":"e:2","status":"retry"}],"queue":"inbox"})",
                "the per-entry status wins, the call-wide one fills the rest");
}

void test_ack_maps_the_boolean_sugar() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.ephemeral().ack(QUEUE, json::array({"e:1"}), false);
    check_equal(server.at(0).body, R"({"acks":[{"id":"e:1","status":"failed"}],"queue":"inbox"})",
                "false is failed");

    client.ephemeral().ack(QUEUE, json::array({"e:1"}), true, "workers");
    check_equal(server.at(1).body,
                R"({"acks":[{"id":"e:1","status":"completed"}],"group":"workers","queue":"inbox"})",
                "true is completed, and the group still travels");
}

void test_ack_omits_group_in_queue_mode_and_refuses_an_ack_with_no_id() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.ephemeral().ack(QUEUE, json::array({"e:1"}));
    check(server.last().body.find("group") == std::string::npos,
          "the groupless queue mode sends no group at all");

    bool threw = false;
    try {
        client.ephemeral().ack(QUEUE, json::array({json{{"payload", json{{"n", 1}}}}}));
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    check(threw, "an ack entry with no id must be refused, not sent empty");
}

void test_ack_of_nothing_answers_empty_results_without_spending_a_request() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    json result = client.ephemeral().ack(QUEUE, json::array());
    check(result["results"].is_array() && result["results"].empty(),
          "an empty ack answers empty results");
    check(server.count() == 0, "and spends no request");
}

// ============================================================================
// Status
// ============================================================================

void test_queues_and_depth_are_plain_gets_on_the_status_routes() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.ephemeral().queues();
    check_equal(server.at(0).method, "GET", "queues is a GET");
    check_equal(server.at(0).target, "/api/v1/ephemeral/queues", "queues' route, with no query");

    client.ephemeral().depth(QUEUE);
    check_equal(server.at(1).method, "GET", "depth is a GET");
    check_equal(server.at(1).target, "/api/v1/ephemeral/queues/inbox/depth", "depth's route");
}

// ============================================================================
// The two kinds of 404 (§4, §8)
//
// The status alone cannot tell them apart, which is exactly why the mapping
// reads the body's CODE:
//
//   * no SDK negotiates a version, so a pre-1.1 broker (routes never
//     registered) and a pre-1.1 proxy (`route_blocked`, fails closed on unknown
//     API paths) both answer 404, and to a caller those are one fact: upgrade;
//   * a broker that DOES support the family answers 404 with
//     `ephemeral_queue_not_found` when `depth` names a queue that is not there.
// ============================================================================

void test_maps_a_missing_broker_route_to_the_one_clear_error() {
    CaptureServer server(old_broker);
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    try {
        client.ephemeral().push(QUEUE, json::array({json{{"a", 1}}}));
    } catch (const EphemeralUnsupportedError& error) {
        threw = true;
        check_equal(error.what(), EphemeralUnsupportedError::MESSAGE,
                    "the canonical message, byte-identical across the SDKs");
        check(error.status_code() == 404, "the status is kept");
        check_equal(error.code(), "ephemeral_unsupported", "and the code is branchable");
    }
    check(threw, "a 404 on this family must raise EphemeralUnsupportedError");
}

void test_maps_the_old_proxy_route_blocked_to_the_same_error() {
    // The proxy's 404 is the same verdict in different words, and the peer's own
    // code is KEPT: "the proxy answered route_blocked" is the evidence for
    // "upgrade", and an SDK that threw it away would leave the operator with a
    // claim and no proof.
    CaptureServer server(old_proxy);
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    try {
        client.ephemeral().pop(QUEUE);
    } catch (const EphemeralUnsupportedError& error) {
        threw = true;
        check_equal(error.original_code(), "route_blocked",
                    "the peer's own code survives the mapping");
    }
    check(threw, "an old proxy's 404 must raise EphemeralUnsupportedError too");
}

void test_the_unsupported_error_is_still_an_http_error() {
    // Every existing `catch (const HttpError&)` around a push keeps catching
    // this one; the distinct type is what a caller uses to tell a version
    // problem apart from any other refusal.
    CaptureServer server(old_broker);
    QueenClient client({server.url()}, fast_config());

    bool caught_as_http_error = false;
    try {
        client.ephemeral().queues();
    } catch (const HttpError& error) {
        caught_as_http_error = true;
        check(dynamic_cast<const EphemeralUnsupportedError*>(&error) != nullptr,
              "and it is still the specific type");
    }
    check(caught_as_http_error, "EphemeralUnsupportedError must be catchable as an HttpError");
}

void test_every_verb_of_the_family_maps_the_404() {
    // Eight verbs, one verdict. A family where six routes say "upgrade" and two
    // say "HTTP 404" is a family somebody will branch on by accident.
    CaptureServer server(old_broker);
    QueenClient client({server.url()}, fast_config());
    auto eph = client.ephemeral();

    std::vector<std::function<void()>> calls = {
        [&] { eph.configure(QUEUE); },
        [&] { eph.reset(QUEUE); },
        [&] { eph.del(QUEUE); },
        [&] { eph.push(QUEUE, json::array({json{{"a", 1}}})); },
        [&] { eph.pop(QUEUE); },
        [&] { eph.ack(QUEUE, json::array({"e:1"})); },
        [&] { eph.queues(); },
        [&] { eph.depth(QUEUE); },
    };

    for (auto& call : calls) {
        bool threw = false;
        try {
            call();
        } catch (const EphemeralUnsupportedError&) {
            threw = true;
        }
        check(threw, "every verb of the family must map its 404");
    }

    check(server.count() == 8, "and every one of them was actually attempted");
}

void test_a_404_for_a_missing_queue_is_its_own_error() {
    // Not "your broker is too old". `depth` is the only verb that can answer a
    // real 404 -- push and pop create implicitly, `reset` answers dropped:0,
    // `del` answers deleted:false -- and collapsing it into the version verdict
    // would send somebody chasing a broker version over a queue name typo.
    CaptureServer server(queue_not_found);
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    try {
        client.ephemeral().depth(QUEUE);
    } catch (const EphemeralUnsupportedError&) {
        check(false, "a missing queue must NOT become the version error");
    } catch (const EphemeralQueueNotFoundError& error) {
        threw = true;
        check(error.status_code() == 404, "the status is kept");
        check_equal(error.code(), "ephemeral_queue_not_found",
                    "the broker's own code, unchanged and branchable");
        check_equal(error.queue(), QUEUE, "the error names the queue that was not found");
        check(std::string(error.what()).find("does not exist") != std::string::npos,
              "and says so in words");
        check(std::string(error.what()).find("1.1") == std::string::npos,
              "a missing queue must not read as a version problem");
        // Nothing the HTTP layer surfaced is lost by the mapping.
        check(error.body().find("ephemeral_queue_not_found") != std::string::npos,
              "the broker's own body survives the mapping");
    }
    check(threw, "a missing queue must raise EphemeralQueueNotFoundError");
}

void test_the_queue_not_found_error_is_still_an_http_error() {
    // Every existing `catch (const HttpError&)` around a depth call keeps
    // catching this one, and the two 404 types stay siblings: catching one must
    // never catch the other.
    CaptureServer server(queue_not_found);
    QueenClient client({server.url()}, fast_config());

    bool caught_as_http_error = false;
    try {
        client.ephemeral().depth(QUEUE);
    } catch (const HttpError& error) {
        caught_as_http_error = true;
        check(dynamic_cast<const EphemeralQueueNotFoundError*>(&error) != nullptr,
              "and it is still the specific type");
        check(dynamic_cast<const EphemeralUnsupportedError*>(&error) == nullptr,
              "and it is NOT the version error");
    }
    check(caught_as_http_error,
          "EphemeralQueueNotFoundError must be catchable as an HttpError");
}

void test_tells_the_two_404s_apart_on_the_same_verb() {
    // The regression this pins: `depth` answering a real 404 while the routes
    // are demonstrably present, because the very next call on the SAME verb --
    // a bare old-broker 404 -- has to read differently. The body decides, and
    // the status, which they share, decides nothing.
    auto call_number = std::make_shared<std::atomic<int>>(0);
    CaptureServer server([call_number](const httplib::Request& req, httplib::Response& res) {
        if ((*call_number)++ == 0) {
            queue_not_found(req, res);
        } else {
            old_broker(req, res);
        }
    });
    QueenClient client({server.url()}, fast_config());

    bool missing = false;
    try {
        client.ephemeral().depth(QUEUE);
    } catch (const EphemeralQueueNotFoundError& error) {
        missing = true;
        check_equal(error.code(), "ephemeral_queue_not_found", "the queue is what is missing");
    }
    check(missing, "the first 404 is the missing queue");

    bool unsupported = false;
    try {
        client.ephemeral().depth(QUEUE);
    } catch (const EphemeralQueueNotFoundError&) {
        check(false, "a bare 404 must NOT become the missing-queue error");
    } catch (const EphemeralUnsupportedError& error) {
        unsupported = true;
        check_equal(error.what(), EphemeralUnsupportedError::MESSAGE,
                    "the second 404 is the version verdict");
    }
    check(unsupported, "the second 404 is the old broker");

    check(server.count() == 2, "and both were actually attempted");
}

void test_leaves_every_other_refusal_alone() {
    // 403 feature_gated is the grant (§1.6 rung 2); it is not a version verdict
    // and must not be dressed up as one.
    CaptureServer server([](const httplib::Request&, httplib::Response& res) {
        res.status = 403;
        res.set_content(R"({"error":"not granted","code":"feature_gated"})", "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    try {
        client.ephemeral().push(QUEUE, json::array({json{{"a", 1}}}));
    } catch (const EphemeralUnsupportedError&) {
        check(false, "a 403 must NOT become the version error");
    } catch (const HttpError& error) {
        threw = true;
        check(error.status_code() == 403, "the status is intact");
        check_equal(error.code(), "feature_gated", "and so is the code");
    }
    check(threw, "a 403 still raises");
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "Queen C++ Client - Ephemeral Wire Tests" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << std::endl;

    run_test("configure sends the queue and its options under `options`",
             test_configure_sends_the_queue_and_its_options_under_options);
    run_test("configure sends only the options it was given",
             test_configure_sends_only_the_options_it_was_given);
    run_test("reset and del name the queue where each route expects it",
             test_reset_and_del_name_the_queue_where_each_route_expects_it);
    run_test("percent-encodes a queue name that would change the path",
             test_percent_encodes_a_queue_name_that_would_change_the_path);
    run_test("refuses a missing queue name before spending a request",
             test_refuses_a_missing_queue_name_before_spending_a_request);

    run_test("push sends the flat envelope with payload-only messages",
             test_push_sends_the_flat_envelope_with_payload_only_messages);
    run_test("push omits `partition` unless the caller named one",
             test_push_omits_partition_unless_the_caller_named_one);
    run_test("push accepts the durable push sugar",
             test_push_accepts_the_durable_push_sugar);
    run_test("push carries no transactionId", test_push_carries_no_transaction_id);
    run_test("push of nothing answers pushed:0 without a request",
             test_push_of_nothing_answers_pushed_zero_without_spending_a_request);
    run_test("push refuses a null message",
             test_push_refuses_a_null_message_rather_than_inventing_a_null_payload);

    run_test("pop sends the queue and nothing else by default",
             test_pop_sends_the_queue_and_nothing_else_by_default);
    run_test("pop puts every declared parameter on the query string",
             test_pop_puts_every_declared_parameter_on_the_query_string);
    run_test("pop sends an explicit timeout whenever it waits",
             test_pop_sends_an_explicit_timeout_whenever_it_waits);
    run_test("pop returns an empty array on a timeout and on a 204",
             test_pop_returns_an_empty_array_on_a_timeout_and_on_a_bodiless_204);
    run_test("pop hands back the frames it was given",
             test_pop_hands_back_the_frames_it_was_given);

    run_test("ack sends the ids under `acks`, with the group beside them",
             test_ack_sends_the_ids_under_acks_with_the_group_beside_them);
    run_test("ack takes popped messages, bare ids, or the wire objects",
             test_ack_takes_popped_messages_bare_ids_or_the_wire_objects);
    run_test("ack lets a per-message status win over the call-wide one",
             test_ack_lets_a_per_message_status_win_over_the_call_wide_one);
    run_test("ack maps the boolean sugar", test_ack_maps_the_boolean_sugar);
    run_test("ack omits `group` in queue mode and refuses an ack with no id",
             test_ack_omits_group_in_queue_mode_and_refuses_an_ack_with_no_id);
    run_test("ack of nothing answers empty results without a request",
             test_ack_of_nothing_answers_empty_results_without_spending_a_request);

    run_test("queues and depth are plain GETs on the status routes",
             test_queues_and_depth_are_plain_gets_on_the_status_routes);

    run_test("maps a missing broker route to the one clear error",
             test_maps_a_missing_broker_route_to_the_one_clear_error);
    run_test("maps the old proxy `route_blocked` to the same error",
             test_maps_the_old_proxy_route_blocked_to_the_same_error);
    run_test("the unsupported error is still an HttpError",
             test_the_unsupported_error_is_still_an_http_error);
    run_test("every verb of the family maps the 404",
             test_every_verb_of_the_family_maps_the_404);
    run_test("a 404 for a missing queue is its own error",
             test_a_404_for_a_missing_queue_is_its_own_error);
    run_test("the queue-not-found error is still an HttpError",
             test_the_queue_not_found_error_is_still_an_http_error);
    run_test("tells the two 404s apart on the same verb",
             test_tells_the_two_404s_apart_on_the_same_verb);
    run_test("leaves every other refusal alone", test_leaves_every_other_refusal_alone);

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << GREEN << "All ephemeral wire tests passed" << RESET << std::endl;
    } else {
        std::cout << RED << failures << " check(s) failed" << RESET << std::endl;
    }

    return failures > 0 ? 1 : 0;
}
