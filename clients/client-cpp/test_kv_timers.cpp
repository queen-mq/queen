/**
 * Queen C++ Client - KV and timers wire-contract test suite
 *
 * PLAN_KV_TIMERS.md, the client half: §5 (KV semantics), §4 (timer semantics),
 * §6.3 and §10.4 (where the two arrays live on the request), §8.1 (200 on
 * applied:false), §8.3 (the failure body), §9.6 (cancel is its own route),
 * §20.1/§20.6 (ttlSeconds for the KV, delayMs for the timers).
 *
 * WHAT THIS FILE IS FOR, AND WHY IT COMES BEFORE THE INTEGRATION SUITE.
 * Everything asserted here is the EXACT JSON BODY the client puts on the wire,
 * plus the exact method and path it puts it on. That is the contract towards the
 * broker, and it is the only thing that catches a wrong wire shape before
 * production: an integration test against a live broker passes just as happily
 * when the client sends its KV operations in the WRONG PLACE, because the broker
 * commits the transaction anyway -- it simply commits it WITHOUT THE GATE that
 * was the entire reason the bundle existed (§8.2, §10.4). No amount of
 * end-to-end testing sees that; a byte comparison of the request body does.
 *
 * These run against an in-process httplib::Server on localhost -- no broker, no
 * Postgres, so the suite is self-contained:
 *
 *   make kvtimers && ./bin/test_kv_timers
 *
 * Sibling: test_retry429.cpp, same shape, same house rules.
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
// Test double: records every request verbatim and answers from a responder
// ============================================================================

struct RecordedCall {
    std::string method;
    std::string target;                      // raw request target, query string included
    std::string body;
};

using Responder = std::function<void(const httplib::Request&, httplib::Response&)>;

/// The default answer: a superset object that satisfies every shape the client
/// reads (a one-element `results` array for the KV/timer surfaces, `success`
/// for the transaction). Tests that care about the ANSWER pass their own
/// responder; tests that care about the REQUEST use this one.
inline void default_responder(const httplib::Request&, httplib::Response& res) {
    res.status = 200;
    res.set_content(R"({"success":true,"results":[{"applied":true,"ok":true}]})",
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

void check_body(const RecordedCall& call, const std::string& expected, const std::string& what) {
    check(call.body == expected,
          what + "\n      expected: " + expected + "\n      actual:   " + call.body);
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
// Pure helpers: base64, the TTL arithmetic, the int64 guard
// ============================================================================

void test_base64_round_trips_including_padding() {
    // The three padding classes plus the empty string. A timer payload is
    // base64 on the wire (§4.1) and the broker decodes it with the STANDARD
    // engine, padding included -- an unpadded encoder produces a 22023 from the
    // stored procedure and nothing else says why.
    const std::vector<std::string> samples = {
        "", "a", "ab", "abc", "abcd", "abcde",
        std::string("\x00\x01\x02\xff", 4),
        R"({"kind":"reminder","n":1})",
    };
    for (const auto& s : samples) {
        std::string encoded = util::base64_encode(s);
        check(util::base64_decode(encoded) == s, "base64 round trip failed for a " +
              std::to_string(s.size()) + "-byte input");
        check(encoded.size() % 4 == 0, "base64 output must be padded to a multiple of 4");
    }
    check(util::base64_encode("abc") == "YWJj", "base64('abc') should be YWJj");
    check(util::base64_encode("ab") == "YWI=", "base64('ab') should be YWI=");
    check(util::base64_encode("a") == "YQ==", "base64('a') should be YQ==");
}

void test_ttl_seconds_rounds_up() {
    // §20.1: `until: <date>` is the admitted sugar, converted to a delta of
    // SECONDS at send time and rounded UP. Rounded DOWN, a marker expires
    // before the window it was meant to cover.
    check(util::ttl_seconds_from_millis(1000) == 1, "1000ms should be 1s");
    check(util::ttl_seconds_from_millis(1001) == 2, "1001ms must round UP to 2s");
    check(util::ttl_seconds_from_millis(1999) == 2, "1999ms must round UP to 2s");
    check(util::ttl_seconds_from_millis(1) == 1, "1ms must round UP to 1s, never to 0");

    bool threw = false;
    try {
        util::ttl_seconds_from_millis(0);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    check(threw, "a non-positive TTL must be refused here, not sent as ttlSeconds:0");
}

void test_kv_int64_refuses_what_it_cannot_represent() {
    // §5.4: the KV counter is `numeric` server-side, so there is no overflow
    // there; a typed SDK exposes int64 and FAILS EXPLICITLY rather than
    // handing back a number that is quietly wrong.
    check(kv_int64(json(42)) == 42, "a small integer should come back unchanged");
    check(kv_int64(json(-42)) == -42, "a negative integer should come back unchanged");

    for (const json& bad : {json(1.5), json("7"), json(nullptr), json::array()}) {
        bool threw = false;
        try {
            kv_int64(bad);
        } catch (const std::runtime_error&) {
            threw = true;
        }
        check(threw, "kv_int64 must refuse " + bad.dump());
    }

    bool threw = false;
    try {
        kv_int64(json::parse("123456789012345678901234567890"));
    } catch (const std::runtime_error&) {
        threw = true;
    }
    check(threw, "kv_int64 must refuse a counter past int64 rather than round it");
}

// ============================================================================
// The standalone KV surface: exact bodies on POST /api/v1/kv
// ============================================================================

void test_kv_get_body() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.kv("orders").get("9f1/items");

    auto call = server.last();
    check(call.method == "POST", "KV goes through POST, got " + call.method);
    // §8.1: the path routes are sugar for what people write by hand; the SDK
    // uses the complete surface, which is also the only one carrying incr.
    check(call.target == "/api/v1/kv", "KV path should be /api/v1/kv, got " + call.target);
    check_body(call, R"({"operations":[{"key":"9f1/items","ns":"orders","op":"get"}]})",
               "kv get body");
}

void test_kv_put_carries_exactly_one_expiry() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.kv("saga").put("order-1", json{{"state", "open"}}, KvTtl::seconds(60));
    check_body(server.last(),
               R"({"operations":[{"key":"order-1","ns":"saga","op":"put","ttlSeconds":60,"value":{"state":"open"}}]})",
               "kv put with ttlSeconds");

    client.kv("saga").put("order-2", json(nullptr), KvTtl::forever());
    // `"value": null` is a legal value and must reach the wire as one (§5.5);
    // and `forever` must appear INSTEAD of ttlSeconds, never beside it (§5.1).
    check_body(server.last(),
               R"({"operations":[{"forever":true,"key":"order-2","ns":"saga","op":"put","value":null}]})",
               "kv put forever with a null value");
}

void test_kv_put_with_expect_and_required() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    KvWriteOptions opts;
    opts.expect = 7;
    opts.required = true;
    client.kv("saga").put("order-1", json{{"state", "done"}}, KvTtl::seconds(60), opts);

    check_body(server.last(),
               R"({"operations":[{"expect":7,"key":"order-1","ns":"saga","op":"put","required":true,"ttlSeconds":60,"value":{"state":"done"}}]})",
               "kv put with expect and required");
}

void test_put_if_absent_never_carries_a_contradictory_expect() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.kv("saga").put_if_absent("marker", json{{"seen", true}}, KvTtl::seconds(3600));
    // putIfAbsent DESUGARS to put with expect:0 inside the stored procedure, so
    // the client sends the name and no expect at all -- one code path there,
    // one shape here.
    check_body(server.last(),
               R"({"operations":[{"key":"marker","ns":"saga","op":"putIfAbsent","ttlSeconds":3600,"value":{"seen":true}}]})",
               "kv putIfAbsent body");

    KvWriteOptions contradiction;
    contradiction.expect = 9;
    bool threw = false;
    try {
        client.kv("saga").put_if_absent("marker", json(1), KvTtl::seconds(60), contradiction);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    check(threw, "putIfAbsent with a non-zero expect is a contradiction and must not reach the wire");
    check(server.count() == 1, "the contradiction must cost no round trip");
}

void test_kv_delete_body() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.kv("saga").del("order-1");
    check_body(server.last(), R"({"operations":[{"key":"order-1","ns":"saga","op":"delete"}]})",
               "kv delete body");

    KvWriteOptions opts;
    opts.expect = 3;
    client.kv("saga").del("order-1", opts);
    check_body(server.last(),
               R"({"operations":[{"expect":3,"key":"order-1","ns":"saga","op":"delete"}]})",
               "kv delete with expect");
}

void test_kv_incr_body() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.kv("quota").incr("acme:2026-08", 1, KvTtl::seconds(3600));
    check_body(server.last(),
               R"({"operations":[{"delta":1,"key":"acme:2026-08","ns":"quota","op":"incr","ttlSeconds":3600}]})",
               "kv incr body");

    KvIncrOptions limited;
    limited.max = 100;
    limited.min = 0;
    client.kv("quota").incr("acme:2026-08", 5, KvTtl::seconds(3600), limited);
    // With `max`, `applied` IS the admission decision (§5.4) -- which is why the
    // ceiling has to travel with the increment instead of being compared
    // client-side after the budget was already spent.
    check_body(server.last(),
               R"({"operations":[{"delta":5,"key":"acme:2026-08","max":100,"min":0,"ns":"quota","op":"incr","ttlSeconds":3600}]})",
               "kv incr with min and max");
}

void test_kv_reads_the_element_out_of_the_results_envelope() {
    CaptureServer server([](const httplib::Request&, httplib::Response& res) {
        res.status = 200;
        res.set_content(
            R"({"results":[{"index":0,"op":"get","found":true,"key":"k","value":{"a":1},"version":9}]})",
            "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    json out = client.kv("n").get("k");
    check(out.value("found", false), "the element, not the envelope, must come back");
    check(out.value("version", 0) == 9, "version should survive the unwrap");
    check(out["value"] == json{{"a", 1}}, "value should survive the unwrap");
}

void test_a_missing_result_is_loud_and_not_a_silent_absent() {
    // The client-side half of the §6.4 / §8.2 alignment guard: N ops in, N
    // results out. An empty `results` read as "not found" would turn a broker
    // that never ran the operation into a business answer.
    CaptureServer server([](const httplib::Request&, httplib::Response& res) {
        res.status = 200;
        res.set_content(R"({"results":[]})", "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    try {
        client.kv("n").get("k");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    check(threw, "an empty results array must raise, never read as found:false");
}

void test_applied_false_is_not_an_error() {
    // §8.1: a lost race is HTTP 200 with an explicit field. It must arrive as a
    // VALUE the caller inspects, never as an exception -- it is the single most
    // frequent outcome of this product.
    CaptureServer server([](const httplib::Request&, httplib::Response& res) {
        res.status = 200;
        res.set_content(
            R"({"results":[{"index":0,"op":"putIfAbsent","applied":false,"reason":"exists","key":"k","value":{"owner":"a"},"version":4}]})",
            "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    json out = client.kv("n").put_if_absent("k", json(1), KvTtl::seconds(60));
    check(out.value("applied", true) == false, "applied:false must come back as a value");
    check(out.value("reason", std::string()) == "exists", "reason should be readable");
    check(out["value"] == json{{"owner", "a"}},
          "the WINNER's value must come back so the loser needs no second round trip");
}

// ============================================================================
// The standalone timer surface
// ============================================================================

void test_timer_schedule_body() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    TimerSchedule timer;
    timer.queue = "reminders";
    timer.timer_key = "order-1";
    timer.payload = json{{"kind", "expire"}};
    timer.delay_millis = 250;
    timer.txn = "11111111-1111-7111-8111-111111111111";
    client.timers().schedule(timer);

    auto call = server.last();
    check(call.method == "POST", "schedule goes through POST, got " + call.method);
    check(call.target == "/api/v1/timers", "schedule path, got " + call.target);
    // §20.6: delayMs, a RELATIVE duration in milliseconds. An absolute instant
    // is not expressible on this wire on purpose -- one clock, Postgres's.
    // Payload is base64 (§4.1).
    const std::string payload_b64 = util::base64_encode(json{{"kind", "expire"}}.dump());
    check_body(call,
               R"({"operations":[{"delayMs":250,"op":"schedule","payload":")" + payload_b64 +
               R"(","queue":"reminders","timerKey":"order-1","txn":"11111111-1111-7111-8111-111111111111"}]})",
               "timer schedule body");
}

void test_timer_schedule_mints_a_fresh_txn_per_call() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    TimerSchedule timer;
    timer.queue = "reminders";
    timer.timer_key = "order-1";
    timer.payload = json{{"n", 1}};
    timer.delay_millis = 1000;

    client.timers().schedule(timer);
    client.timers().schedule(timer);

    auto calls = server.calls();
    check(calls.size() == 2, "expected two schedules");
    if (calls.size() == 2) {
        std::string a = json::parse(calls[0].body)["operations"][0]["txn"];
        std::string b = json::parse(calls[1].body)["operations"][0]["txn"];
        check(util::is_valid_uuid(a), "a minted txn must be a uuid, got '" + a + "'");
        // §20.2, ratified: the txn is OVERWRITTEN by every reschedule -- a
        // rescheduled timer is a new message, so "this timer, rescheduled,
        // delivered this message" stays answerable without ambiguity.
        check(a != b, "each schedule must mint its own txn, got the same one twice");
    }
}

void test_timer_schedule_carries_the_optional_fields() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    TimerSchedule timer;
    timer.queue = "reminders";
    timer.timer_key = "order-1";
    timer.payload = json{{"n", 1}};
    timer.delay_millis = 0;                 // a delay of zero is legal: it fires on the first cycle
    timer.partition = "tenant-a";
    timer.txn = "22222222-2222-7222-8222-222222222222";
    client.timers().schedule(timer);

    const std::string payload_b64 = util::base64_encode(json{{"n", 1}}.dump());
    check_body(server.last(),
               R"({"operations":[{"delayMs":0,"op":"schedule","partition":"tenant-a","payload":")" +
               payload_b64 +
               R"(","queue":"reminders","timerKey":"order-1","txn":"22222222-2222-7222-8222-222222222222"}]})",
               "timer schedule with a partition");
}

void test_cancel_uses_its_own_route_and_never_the_batch() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.timers().cancel("reminders", "order-1");

    auto call = server.last();
    // §9.6, and this is the load-bearing assertion of the timer surface: a
    // cancel sent inside POST /api/v1/timers inherits the SCHEDULE's
    // authorization class, so on a cluster whose quota is full it is refused --
    // and a tenant that cannot cancel keeps producing messages it cannot stop,
    // because the fire never switches itself off. DELETE
    // /api/v1/timers/:queue/*timerKey is the route that is guaranteed to work.
    check(call.method == "DELETE", "cancel must be a DELETE, got " + call.method);
    check(call.target == "/api/v1/timers/reminders/order-1",
          "cancel path, got " + call.target);
    check(call.body.empty(), "cancel carries no body");
}

void test_cancel_echoes_the_expected_txn() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.timers().cancel("reminders", "order-1", "33333333-3333-7333-8333-333333333333");
    // §4.4: `absent` means "no longer pending", which INCLUDES already
    // delivered. The caller echoes the txn it expects so the "was it already
    // delivered?" check against the destination queue needs no second API.
    check(server.last().target ==
              "/api/v1/timers/reminders/order-1?txn=33333333-3333-7333-8333-333333333333",
          "cancel should echo the txn, got " + server.last().target);
}

void test_cancel_percent_encodes_a_slashed_timer_key() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.timers().cancel("re minders", "order/1");
    // Lowercase hex digits: that is what this client's util::url_encode has
    // always emitted, and percent-encoding is case-insensitive, so the broker's
    // catch-all decodes `%2f` back to the `/` the caller wrote. Pinned here so
    // the encoder is not "tidied" into something the route no longer matches.
    check(server.last().target == "/api/v1/timers/re%20minders/order%2f1",
          "cancel must encode both segments, got " + server.last().target);
}

// ============================================================================
// The transaction wire -- §6.3 and §10.4, the shape this whole feature turns on
// ============================================================================

void test_kv_and_timers_are_top_level_and_never_operations() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    TimerSchedule timer;
    timer.queue = "compensation";
    timer.timer_key = "saga-1";
    timer.payload = json{{"undo", true}};
    timer.delay_millis = 30000;
    timer.txn = "44444444-4444-7444-8444-444444444444";

    client.transaction()
        .queue("out").push({json{{"data", json{{"n", 1}}}, {"transactionId", "55555555-5555-7555-8555-555555555555"}}})
        .kv("saga").put_if_absent("saga-1", json{{"state", "open"}}, KvTtl::seconds(600))
        .timers().schedule(timer)
        .commit();

    json body = json::parse(server.last().body);

    // THE assertion. Two Go struct fields carrying the same JSON key at the
    // same level are BOTH dropped by encoding/json, with no error and no
    // warning; growing `operations` a kv leg would therefore let a bundle go
    // out with ZERO kv operations while the broker committed the transaction
    // WITHOUT the gate -- the putIfAbsent the bundle existed for would simply
    // never have happened, and nothing anywhere would say so. C++ does not have
    // that failure mode, but the WIRE is shared by seven clients, so the shape
    // is not the client's to choose (§6.3, §8.2, §10.4).
    check(body.contains("kv") && body["kv"].is_array(), "`kv` must be a top-level array");
    check(body.contains("timers") && body["timers"].is_array(), "`timers` must be a top-level array");
    check(body["kv"].size() == 1, "the kv rider should carry one op");
    check(body["timers"].size() == 1, "the timers rider should carry one op");

    for (const auto& op : body["operations"]) {
        std::string type = op.value("type", std::string());
        check(type != "kv" && type != "timers" && type != "schedule" && type != "cancel",
              "no kv/timer operation may appear inside `operations`, found type '" + type + "'");
    }
    check(body["operations"].size() == 1, "operations should carry the push and nothing else");
}

void test_transaction_riders_are_byte_identical_to_the_standalone_ops() {
    CaptureServer standalone_server;
    CaptureServer wire_server;
    QueenClient standalone({standalone_server.url()}, fast_config());
    QueenClient wire({wire_server.url()}, fast_config());

    KvWriteOptions opts;
    opts.expect = 12;
    opts.required = true;

    standalone.kv("saga").put("k", json{{"v", 1}}, KvTtl::seconds(60), opts);
    wire.transaction().kv("saga").put("k", json{{"v", 1}}, KvTtl::seconds(60), opts).commit();

    json alone = json::parse(standalone_server.last().body)["operations"][0];
    json rider = json::parse(wire_server.last().body)["kv"][0];
    // One mint for both surfaces. Two op builders is how the standalone path
    // and the rider path drift, and the drift is invisible until the gate that
    // works over HTTP silently stops working inside a bundle.
    check(alone == rider, "standalone and in-transaction ops must be identical:\n      " +
          alone.dump() + "\n      " + rider.dump());
}

void test_a_bundle_without_riders_is_byte_identical_to_today() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.transaction()
        .queue("out").push({json{{"data", json{{"n", 1}}}, {"transactionId", "66666666-6666-7666-8666-666666666666"}}})
        .commit();

    // §6.3: zero cost for whoever does not use the feature. An absent array and
    // an empty array are not the same thing on this wire -- the broker skips on
    // `jsonb_typeof`, and a client that always sent `"kv":[]` would make every
    // existing bundle look like a KV bundle in every log and sample on the way.
    std::string body = server.last().body;
    check(body.find("\"kv\"") == std::string::npos,
          "a bundle with no KV must not carry a `kv` key at all: " + body);
    check(body.find("\"timers\"") == std::string::npos,
          "a bundle with no timers must not carry a `timers` key at all: " + body);
    check(body == R"({"operations":[{"items":[{"payload":{"n":1},"queue":"out","transactionId":"66666666-6666-7666-8666-666666666666"}],"type":"push"}],"requiredLeases":[]})",
          "today's bundle must go out unchanged, got: " + body);
}

void test_a_kv_only_transaction_is_legal() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.transaction()
        .kv("saga").put("k", json(1), KvTtl::seconds(60))
        .commit();

    json body = json::parse(server.last().body);
    check(body["operations"].empty(), "a KV-only bundle has no operations");
    check(body["kv"].size() == 1, "and one KV rider");
}

void test_an_entirely_empty_transaction_still_refuses_to_commit() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    try {
        client.transaction().commit();
    } catch (const std::runtime_error&) {
        threw = true;
    }
    check(threw, "an empty transaction must still refuse");
    check(server.count() == 0, "and must not reach the network");
}

void test_transaction_timer_cancel_rides_the_bundle() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());

    client.transaction()
        .timers().cancel("compensation", "saga-1", "77777777-7777-7777-8777-777777777777")
        .commit();

    json rider = json::parse(server.last().body)["timers"][0];
    check(rider == json({{"op", "cancel"}, {"queue", "compensation"},
                         {"timerKey", "saga-1"}, {"txn", "77777777-7777-7777-8777-777777777777"}}),
          "timer cancel rider, got " + rider.dump());
}

// ============================================================================
// commit(): the one wire change §10.2 requires of every client
// ============================================================================

void test_commit_returns_on_a_lost_kv_precondition() {
    CaptureServer server([](const httplib::Request&, httplib::Response& res) {
        // §8.3: HTTP **200**. The transaction really did abort in SQL, but a
        // lost precondition is the EXPECTED outcome of every legitimate
        // redelivery -- it is the idempotency marker doing its job -- and it
        // must pollute neither the error metrics nor the retry policies.
        res.status = 200;
        res.set_content(
            R"({"transactionId":"t1","success":false,"reason":"kv_precondition","error":"kv_precondition_failed",)"
            R"("results":[],"ok":false,"failedIndex":1,"kvReason":"exists","version":90101,"value":{"owner":"a"}})",
            "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    json out;
    bool threw = false;
    try {
        out = client.transaction()
            .queue("out").push({json{{"data", json{{"n", 1}}}}})
            .kv("saga").put_if_absent("saga-1", json(1), KvTtl::seconds(60))
            .commit();
    } catch (const std::exception&) {
        threw = true;
    }

    check(!threw, "a lost KV precondition is a VERDICT and commit() must return it, not throw");
    check(out.value("success", true) == false, "success:false should survive");
    check(out.value("reason", std::string()) == "kv_precondition", "reason should survive");
    // The five fields §8.3 adds, and the reason they are there: without them a
    // client would have to string-match the error message, which is forbidden
    // everywhere in this codebase.
    check(out.value("failedIndex", -1) == 1, "failedIndex should survive, in the FLAT space");
    check(out.value("kvReason", std::string()) == "exists", "kvReason should survive");
    check(out.value("version", 0) == 90101, "version should survive");
    check(out["value"] == json{{"owner", "a"}}, "the winner's value should survive");
}

void test_commit_still_throws_on_every_other_failure() {
    // The narrow exemption is exactly one reason string. Everything else stays
    // an exception, or the change would silently disarm every existing caller's
    // error handling.
    const std::vector<std::string> reasons = {
        "ack_rejected", "duplicate", "bad_request", "db_error", "payload_too_large"
    };
    for (const auto& reason : reasons) {
        CaptureServer server([&reason](const httplib::Request&, httplib::Response& res) {
            res.status = 200;
            res.set_content(
                R"({"transactionId":"t1","success":false,"reason":")" + reason +
                R"(","error":"nope","results":[]})", "application/json");
        });
        QueenClient client({server.url()}, fast_config());

        bool threw = false;
        try {
            client.transaction().kv("saga").put("k", json(1), KvTtl::seconds(60)).commit();
        } catch (const std::runtime_error&) {
            threw = true;
        }
        check(threw, "reason '" + reason + "' must still throw");
    }
}

void test_commit_surfaces_a_gated_403_as_an_http_error() {
    // §9.5/§9.8: the bundle is refused WHOLE with the ladder's own code. That is
    // a transport-level verdict, not a business one, and it must keep arriving
    // as an HttpError so the caller's 403 handling (which already exists for
    // the proxy contract) applies unchanged.
    CaptureServer server([](const httplib::Request&, httplib::Response& res) {
        res.status = 403;
        res.set_content(
            R"({"transactionId":"t1","success":false,"reason":"feature_gated","code":"feature_gated","error":"off","results":[]})",
            "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    try {
        client.transaction().kv("saga").put("k", json(1), KvTtl::seconds(60)).commit();
    } catch (const HttpError& e) {
        threw = true;
        check(e.status_code() == 403, "status should be 403");
        check(e.code() == "feature_gated", "code should be feature_gated, got '" + e.code() + "'");
    }
    check(threw, "a gated bundle must surface as HttpError");
}

void test_kv_paused_by_the_kill_switch_is_a_503_and_not_found_false() {
    // The surface is on every cell, so it is never ABSENT -- but an operator
    // can still pause it mid-incident with the runtime kill switch, and on the
    // routes that reads 503 + Retry-After (temporary, come back). It must not
    // be mistaken for a missing KEY: `found:false` is a business answer and
    // this is the cell declining to answer at all.
    CaptureServer server([](const httplib::Request&, httplib::Response& res) {
        res.status = 503;
        res.set_header("Retry-After", "1");
        res.set_content(R"({"error":"kv_disabled","reason":"kv_disabled"})", "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    bool threw = false;
    try {
        client.kv("n").get("k");
    } catch (const HttpError& e) {
        threw = true;
        check(e.status_code() == 503, "a paused KV surface is a 503");
        check(std::string(e.what()) == "kv_disabled",
              "what() should carry the ladder's code, got '" + std::string(e.what()) + "'");
    }
    check(threw, "a paused surface must not be swallowed into found:false");
}

void test_timers_paused_schedule_is_a_503() {
    // Same for the timer half, with its own code: a paused schedule must never
    // be reported as `ok:false` (which would mean the broker considered the
    // request and declined it on the merits) nor as a delivered promise.
    CaptureServer server([](const httplib::Request&, httplib::Response& res) {
        res.status = 503;
        res.set_header("Retry-After", "1");
        res.set_content(R"({"error":"timers_disabled","reason":"timers_disabled"})",
                        "application/json");
    });
    QueenClient client({server.url()}, fast_config());

    TimerSchedule sched;
    sched.queue = "q";
    sched.timer_key = "t";
    sched.delay_millis = 1000;
    sched.payload = json({{"a", 1}});

    bool threw = false;
    try {
        client.timers().schedule(sched);
    } catch (const HttpError& e) {
        threw = true;
        check(e.status_code() == 503, "a paused timer schedule is a 503");
        check(std::string(e.what()) == "timers_disabled",
              "what() should carry the ladder's code, got '" + std::string(e.what()) + "'");
    }
    check(threw, "a paused schedule must surface as HttpError");
}

// ============================================================================

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "Queen C++ Client - KV / Timers Wire Tests" << std::endl;
    std::cout << "========================================\n" << std::endl;

    run_test("base64 round trips, padding included", test_base64_round_trips_including_padding);
    run_test("until -> ttlSeconds rounds UP", test_ttl_seconds_rounds_up);
    run_test("kv_int64 refuses what it cannot represent", test_kv_int64_refuses_what_it_cannot_represent);

    run_test("kv get body", test_kv_get_body);
    run_test("kv put carries exactly one expiry", test_kv_put_carries_exactly_one_expiry);
    run_test("kv put with expect and required", test_kv_put_with_expect_and_required);
    run_test("putIfAbsent never carries a contradictory expect",
             test_put_if_absent_never_carries_a_contradictory_expect);
    run_test("kv delete body", test_kv_delete_body);
    run_test("kv incr body", test_kv_incr_body);
    run_test("kv reads the element out of the results envelope",
             test_kv_reads_the_element_out_of_the_results_envelope);
    run_test("a missing result is loud, not a silent absent",
             test_a_missing_result_is_loud_and_not_a_silent_absent);
    run_test("applied:false is a value, not an error", test_applied_false_is_not_an_error);

    run_test("timer schedule body", test_timer_schedule_body);
    run_test("timer schedule mints a fresh txn per call", test_timer_schedule_mints_a_fresh_txn_per_call);
    run_test("timer schedule carries the optional fields", test_timer_schedule_carries_the_optional_fields);
    run_test("cancel uses its own route and never the batch",
             test_cancel_uses_its_own_route_and_never_the_batch);
    run_test("cancel echoes the expected txn", test_cancel_echoes_the_expected_txn);
    run_test("cancel percent-encodes a slashed timer key",
             test_cancel_percent_encodes_a_slashed_timer_key);

    run_test("kv and timers are TOP-LEVEL, never operations",
             test_kv_and_timers_are_top_level_and_never_operations);
    run_test("transaction riders are byte-identical to the standalone ops",
             test_transaction_riders_are_byte_identical_to_the_standalone_ops);
    run_test("a bundle without riders is byte-identical to today",
             test_a_bundle_without_riders_is_byte_identical_to_today);
    run_test("a kv-only transaction is legal", test_a_kv_only_transaction_is_legal);
    run_test("an entirely empty transaction still refuses",
             test_an_entirely_empty_transaction_still_refuses_to_commit);
    run_test("transaction timer cancel rides the bundle", test_transaction_timer_cancel_rides_the_bundle);

    run_test("commit() RETURNS on a lost kv precondition", test_commit_returns_on_a_lost_kv_precondition);
    run_test("commit() still throws on every other failure", test_commit_still_throws_on_every_other_failure);
    run_test("commit() surfaces a gated 403 as HttpError", test_commit_surfaces_a_gated_403_as_an_http_error);
    run_test("a paused KV surface is a 503, not found:false",
             test_kv_paused_by_the_kill_switch_is_a_503_and_not_found_false);
    run_test("a paused timer schedule is a 503", test_timers_paused_schedule_is_a_503);

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << GREEN << "All KV/timer wire tests passed" << RESET << std::endl;
    } else {
        std::cout << RED << failures << " check(s) failed" << RESET << std::endl;
    }

    return failures > 0 ? 1 : 0;
}
