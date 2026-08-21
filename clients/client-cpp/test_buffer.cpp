/**
 * Queen C++ Client - Buffer contract test suite (the 1.0.6 discipline)
 *
 * Pins the two properties the 2026-08-20 buffer rewrite added to every SDK
 * (clients/client-js/client-v2/buffer/{BufferManager.js,MessageBuffer.js} and
 * the Python/Go/PHP equivalents), which this client gained last:
 *
 *   1. `max_size` is a BLOCKING bound. An add at the bound parks until the
 *      flusher drains below it -- never an unbounded array, never a message
 *      accepted by a client that will lose it.
 *   2. A batch whose POST fails goes back to the FRONT of the buffer, in
 *      order, and is retried until it lands, the buffer stops, or an explicit
 *      flush deadline expires -- at which point the failure is LOUD
 *      (BufferFlushError says how many are still buffered) and nothing was
 *      dropped.
 *
 * Everything here runs against an in-process httplib::Server on localhost --
 * no broker, no Postgres:
 *
 *   make buffer && ./bin/test_buffer
 *
 * Siblings: test_retry429.cpp, test_kv_timers.cpp, test_conflation.cpp --
 * same shape, same house rules.
 */

#include "queen_client.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <atomic>
#include <vector>
#include <string>
#include <numeric>

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
    std::string target;
    std::string body;
    int status;                              // what the responder answered
};

using Responder = std::function<void(const httplib::Request&, httplib::Response&)>;

/// Broker-shaped success: /api/v1/push answers a per-item array; the buffered
/// drain only needs it to parse.
inline void ok_responder(const httplib::Request&, httplib::Response& res) {
    res.status = 200;
    res.set_content("[]", "application/json");
}

inline void fail_responder(const httplib::Request&, httplib::Response& res) {
    res.status = 500;
    res.set_content(R"({"error":"broker down"})", "application/json");
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
    explicit CaptureServer(Responder responder = ok_responder)
        : responder_(std::move(responder)) {
        // Recorded AFTER the responder so the call carries the status it was
        // answered with -- the suite tells retries apart from deliveries by it.
        auto handler = [this](const httplib::Request& req, httplib::Response& res) {
            responder_(req, res);
            std::lock_guard<std::mutex> lock(mutex_);
            calls_.push_back({req.method, req.target, req.body, res.status});
        };

        server_.Post(".*", handler);

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

// A single failed POST must surface to the buffer's own retry loop
// immediately: no HttpClient-level 5xx retries, no meaningful backoff.
ClientConfig fast_config() {
    ClientConfig config;
    config.timeout_millis = 2000;
    config.retry_attempts = 1;
    config.retry_delay_millis = 1;
    config.retry_429.base_millis = 1;
    config.retry_429.cap_millis = 2;
    return config;
}

/// Poll until pred holds or timeout_millis elapses; returns the final verdict.
bool eventually(std::function<bool()> pred, int timeout_millis = 3000) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_millis);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return pred();
}

/// The payloads of every item in every DELIVERED (status 200) call, in wire
/// order. Every push in this suite uses {"data": <int>}, so payloads are ints.
std::vector<int> delivered_payloads(const std::vector<RecordedCall>& calls) {
    std::vector<int> out;
    for (const auto& call : calls) {
        if (call.status != 200) continue;
        auto body = json::parse(call.body);
        for (const auto& item : body["items"]) {
            out.push_back(item["payload"].get<int>());
        }
    }
    return out;
}

std::vector<int> iota_vector(int count) {
    std::vector<int> out(count);
    std::iota(out.begin(), out.end(), 0);
    return out;
}

std::vector<json> data_items(int from, int count) {
    std::vector<json> items;
    for (int i = 0; i < count; ++i) {
        items.push_back({{"data", from + i}});
    }
    return items;
}

// ============================================================================
// Option resolution: the bound is not optional
// ============================================================================

void test_options_resolve_bounded_defaults() {
    BufferOptions defaults;
    auto resolved = resolve_buffer_options(defaults);
    check(resolved.message_count == 100, "default message_count should be 100");
    check(resolved.time_millis == 1000, "default time_millis should be 1000");
    check(resolved.max_size == 400,
          "an absent max_size must resolve to 4 x message_count, never to unbounded");
    check(resolved.retry_delay_millis == 250, "default retry_delay_millis should be 250");

    BufferOptions small;
    small.message_count = 10;
    check(resolve_buffer_options(small).max_size == 40,
          "the default bound must follow the caller's message_count (10 -> 40)");

    BufferOptions floored;
    floored.message_count = 100;
    floored.max_size = 10;
    check(resolve_buffer_options(floored).max_size == 100,
          "max_size below message_count would deadlock against the flush threshold; it must be floored");

    BufferOptions zero_delay;
    zero_delay.retry_delay_millis = 0;
    check(resolve_buffer_options(zero_delay).retry_delay_millis == 250,
          "a zero retry delay must resolve to the default, not a hot loop");

    BufferOptions explicit_values;
    explicit_values.message_count = 7;
    explicit_values.time_millis = 123;
    explicit_values.max_size = 55;
    explicit_values.retry_delay_millis = 9;
    auto kept = resolve_buffer_options(explicit_values);
    check(kept.message_count == 7 && kept.time_millis == 123 &&
          kept.max_size == 55 && kept.retry_delay_millis == 9,
          "explicit values must survive resolution untouched");
}

// ============================================================================
// Flush triggers
// ============================================================================

void test_count_threshold_triggers_flush() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());
    BufferOptions opts;
    opts.message_count = 3;
    opts.time_millis = 60000; // the timer must not be what flushes here

    json result = client.queue("orders").buffer(opts).push(data_items(0, 3));
    check(result["buffered"] == true && result["count"] == 3,
          "a buffered push must keep its {buffered, count} API shape");

    check(eventually([&] { return server.count() >= 1; }),
          "reaching message_count must flush without waiting for the timer");

    auto calls = server.calls();
    check(calls.size() == 1, "one batch expected, got " + std::to_string(calls.size()));
    check(calls[0].method == "POST" && calls[0].target == "/api/v1/push",
          "the drain must POST /api/v1/push, got " + calls[0].method + " " + calls[0].target);
    auto body = json::parse(calls[0].body);
    check(body["items"].size() == 3, "the batch must carry all 3 messages");
    check(body["items"][0]["queue"] == "orders" && body["items"][0]["partition"] == "Default",
          "items must carry queue and partition");
    check(body["items"][0].contains("transactionId"), "items must carry a transactionId");
    check(delivered_payloads(calls) == iota_vector(3), "payloads must land in push order");
}

void test_timer_triggers_flush_below_threshold() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());
    BufferOptions opts;
    opts.message_count = 100; // never reached
    opts.time_millis = 40;

    client.queue("orders").buffer(opts).push(data_items(0, 2));
    check(eventually([&] { return server.count() >= 1; }),
          "the timer must flush a buffer that never reaches its count");
    check(delivered_payloads(server.calls()) == iota_vector(2),
          "the timer flush must carry both messages, in order");
}

void test_batches_are_message_count_sized_and_ordered() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());
    BufferOptions opts;
    opts.message_count = 4;
    opts.time_millis = 30;

    client.queue("orders").buffer(opts).push(data_items(0, 10));
    client.queue("orders").flush_buffer(); // no deadline: returns once everything LANDED

    auto calls = server.calls();
    check(!calls.empty(), "the flush must have sent something");
    for (const auto& call : calls) {
        auto body = json::parse(call.body);
        check(body["items"].size() <= 4,
              "no batch may exceed message_count, got one of " +
              std::to_string(body["items"].size()));
    }
    check(delivered_payloads(calls) == iota_vector(10),
          "all 10 messages must land exactly once, in producer order");
}

void test_buffers_are_keyed_per_queue_partition_address() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());
    BufferOptions opts;
    opts.message_count = 2;
    opts.time_millis = 60000;

    client.queue("alpha").buffer(opts).push(data_items(0, 1));
    client.queue("beta").buffer(opts).push(data_items(100, 1));
    client.queue("alpha").buffer(opts).push(data_items(1, 1)); // alpha alone reaches its threshold

    check(eventually([&] { return server.count() >= 1; }), "alpha must flush on its own threshold");
    auto calls = server.calls();
    check(calls.size() == 1, "only alpha reached its threshold, got " +
          std::to_string(calls.size()) + " request(s)");
    auto body = json::parse(calls[0].body);
    bool all_alpha = true;
    for (const auto& item : body["items"]) {
        if (item["queue"] != "alpha") all_alpha = false;
    }
    check(body["items"].size() == 2 && all_alpha, "a batch must never mix addresses");
    check(client.get_buffer_stats()["totalBufferedMessages"] == 1,
          "beta's message must still be buffered");

    client.flush_all_buffers();
    auto payloads = delivered_payloads(server.calls());
    check(payloads.size() == 3 && payloads.back() == 100,
          "flush_all_buffers must land beta's message too");
    check(client.get_buffer_stats()["flushesPerformed"] == 2,
          "stats must count one flush per landed batch");
}

// ============================================================================
// Lossless flush retry
// ============================================================================

void test_failed_batch_is_requeued_in_order_and_retried() {
    std::atomic<int> request_index{0};
    CaptureServer server([&](const httplib::Request& req, httplib::Response& res) {
        if (request_index++ < 2) {
            fail_responder(req, res);
        } else {
            ok_responder(req, res);
        }
    });
    QueenClient client({server.url()}, fast_config());
    BufferOptions opts;
    opts.message_count = 3;
    opts.time_millis = 60000;
    opts.retry_delay_millis = 5;

    client.queue("orders").buffer(opts).push(data_items(0, 3));

    check(eventually([&] {
        auto calls = server.calls();
        return !calls.empty() && calls.back().status == 200;
    }), "the failed batch must be retried until it lands");

    auto calls = server.calls();
    check(calls.size() == 3, "expected 2 failures + 1 delivery, got " +
          std::to_string(calls.size()) + " request(s)");
    if (calls.size() == 3) {
        check(calls[0].status == 500 && calls[1].status == 500 && calls[2].status == 200,
              "the first two attempts fail, the third lands");
        check(calls[0].body == calls[1].body && calls[1].body == calls[2].body,
              "every retry must carry the SAME batch, from the front, in order");
    }
    check(delivered_payloads(calls) == iota_vector(3), "nothing lost, nothing duplicated");
}

void test_requeued_batch_stays_ahead_of_later_adds() {
    std::atomic<bool> healthy{false};
    CaptureServer server([&](const httplib::Request& req, httplib::Response& res) {
        if (healthy) {
            ok_responder(req, res);
        } else {
            fail_responder(req, res);
        }
    });
    QueenClient client({server.url()}, fast_config());
    BufferOptions opts;
    opts.message_count = 2;
    opts.time_millis = 60000;
    opts.max_size = 100;
    opts.retry_delay_millis = 5;

    client.queue("orders").buffer(opts).push(data_items(0, 2)); // batch A: fails, requeues
    check(eventually([&] { return server.count() >= 1; }), "batch A's first attempt must happen");
    client.queue("orders").buffer(opts).push(data_items(2, 2)); // appended BEHIND the requeued A

    healthy = true;
    client.queue("orders").flush_buffer();
    check(delivered_payloads(server.calls()) == iota_vector(4),
          "a requeued batch goes back to the FRONT: later adds must not overtake it");
}

void test_nothing_lost_across_intermittent_failures() {
    std::atomic<int> request_index{0};
    CaptureServer server([&](const httplib::Request& req, httplib::Response& res) {
        if (request_index++ % 3 == 0) {
            fail_responder(req, res);
        } else {
            ok_responder(req, res);
        }
    });
    QueenClient client({server.url()}, fast_config());
    BufferOptions opts;
    opts.message_count = 5;
    opts.time_millis = 10;
    opts.max_size = 20;
    opts.retry_delay_millis = 2;

    for (int i = 0; i < 60; ++i) {
        client.queue("orders").buffer(opts).push(data_items(i, 1));
    }
    client.queue("orders").flush_buffer();

    auto payloads = delivered_payloads(server.calls());
    check(payloads.size() == 60, "send/receive parity: expected 60 delivered, got " +
          std::to_string(payloads.size()));
    check(payloads == iota_vector(60), "producer order must survive every retry");
}

// ============================================================================
// Backpressure
// ============================================================================

void test_backpressure_blocks_add_at_bound_and_resumes() {
    std::atomic<bool> release{false};
    // The gate holds the first POST in flight, so the buffer refills to its
    // bound behind it and the parked add below is deterministic. Bounded loop:
    // a failing test must not hang the suite in the server's destructor.
    CaptureServer server([&](const httplib::Request& req, httplib::Response& res) {
        for (int i = 0; i < 5000 && !release; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        ok_responder(req, res);
    });
    QueenClient client({server.url()}, fast_config());
    BufferOptions opts;
    opts.message_count = 2;
    opts.time_millis = 30;
    opts.max_size = 2;
    opts.retry_delay_millis = 5;

    client.queue("orders").buffer(opts).push(data_items(0, 2)); // taken by the drain, POST gated
    check(eventually([&] {
        return client.get_buffer_stats()["totalBufferedMessages"].get<int>() == 0;
    }), "the drain should have the first batch in flight");

    client.queue("orders").buffer(opts).push(data_items(2, 2)); // refills to the bound

    std::atomic<bool> parked_add_done{false};
    std::string parked_error;
    std::thread producer([&] {
        try {
            client.queue("orders").buffer(opts).push(data_items(4, 1));
        } catch (const std::exception& e) {
            parked_error = e.what();
        }
        parked_add_done = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    check(!parked_add_done, "an add at the bound must BLOCK, not grow the buffer");

    release = true;
    bool resumed = eventually([&] { return parked_add_done.load(); });
    check(resumed, "the parked add must resume once the flusher frees capacity");
    if (!resumed) {
        client.close(); // wake the parked add so the suite can finish
    }
    producer.join();
    check(parked_error.empty(), "the resumed add must succeed, got: " + parked_error);

    client.queue("orders").flush_buffer();
    check(delivered_payloads(server.calls()) == iota_vector(5),
          "parity and order must hold across the parked add");
}

void test_parked_add_fails_loudly_on_cleanup() {
    CaptureServer server(fail_responder);
    auto http = std::make_shared<HttpClient>(server.url(), 2000, 1, 1);
    BufferManager manager(http);
    BufferOptions opts;
    opts.message_count = 2;
    opts.time_millis = 60000;
    opts.max_size = 2;
    // Long enough that the drain sits in its retry pause (batch requeued, the
    // buffer visibly full) while the test parks the third add.
    opts.retry_delay_millis = 200;

    manager.add_message("orders/Default", json{{"payload", 0}}, opts);
    manager.add_message("orders/Default", json{{"payload", 1}}, opts);
    check(eventually([&] {
        return manager.get_stats()["totalBufferedMessages"].get<int>() == 2 && server.count() >= 1;
    }), "the failed batch must be back in the buffer, not dropped");

    std::atomic<bool> parked_add_done{false};
    std::string parked_error;
    std::thread producer([&] {
        try {
            manager.add_message("orders/Default", json{{"payload", 2}}, opts);
        } catch (const std::exception& e) {
            parked_error = e.what();
        }
        parked_add_done = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    check(!parked_add_done, "the third add must be parked at the bound");

    manager.cleanup();
    check(eventually([&] { return parked_add_done.load(); }),
          "cleanup must wake the parked add");
    producer.join();
    check(!parked_error.empty() && parked_error.find("not buffered") != std::string::npos,
          "a parked add on a stopped buffer must THROW, never report success; got '" +
          parked_error + "'");
}

// ============================================================================
// Explicit flush deadlines: bounded, loud, lossless
// ============================================================================

void test_flush_deadline_is_loud_and_drops_nothing() {
    std::atomic<bool> healthy{false};
    CaptureServer server([&](const httplib::Request& req, httplib::Response& res) {
        if (healthy) {
            ok_responder(req, res);
        } else {
            fail_responder(req, res);
        }
    });
    QueenClient client({server.url()}, fast_config());
    BufferOptions opts;
    opts.message_count = 2;
    opts.time_millis = 60000;
    opts.retry_delay_millis = 5;

    client.queue("orders").buffer(opts).push(data_items(0, 2));

    bool threw = false;
    try {
        client.queue("orders").flush_buffer(80);
    } catch (const BufferFlushError& e) {
        threw = true;
        check(e.unflushed_count() == 2, "the error must carry the unflushed count, got " +
              std::to_string(e.unflushed_count()));
        check(e.queue_address() == "orders/Default",
              "the error must name the address, got " + e.queue_address());
        check(std::string(e.what()).find("2 message(s) still buffered for orders/Default") !=
              std::string::npos,
              std::string("the failure must be loud; what() = ") + e.what());
    }
    check(threw, "an expired flush deadline must throw BufferFlushError");
    check(client.get_buffer_stats()["totalBufferedMessages"] == 2,
          "after the deadline the messages must STILL be in the buffer");

    healthy = true;
    client.queue("orders").flush_buffer(); // no deadline: retries until landed
    check(delivered_payloads(server.calls()) == iota_vector(2),
          "the retained batch must land intact once the broker recovers");
}

void test_flush_with_zero_deadline_still_attempts_once() {
    CaptureServer server(fail_responder);
    QueenClient client({server.url()}, fast_config());
    BufferOptions opts;
    opts.message_count = 100; // below threshold: no drain in flight before the flush
    opts.time_millis = 60000;
    opts.retry_delay_millis = 5;

    client.queue("orders").buffer(opts).push(data_items(0, 1));

    bool threw = false;
    try {
        client.queue("orders").flush_buffer(0);
    } catch (const BufferFlushError& e) {
        threw = true;
        check(e.unflushed_count() == 1, "one message must be reported unflushed");
    }
    check(threw, "a zero deadline against a dead broker must still throw");
    check(server.count() >= 1,
          "a zero deadline still gets one real attempt before the raise");
}

void test_flush_all_attempts_every_address_and_rethrows() {
    CaptureServer server([&](const httplib::Request& req, httplib::Response& res) {
        // alpha is unreachable, beta is healthy
        if (req.body.find("\"queue\":\"alpha\"") != std::string::npos) {
            fail_responder(req, res);
        } else {
            ok_responder(req, res);
        }
    });
    QueenClient client({server.url()}, fast_config());
    BufferOptions opts;
    opts.message_count = 10;
    opts.time_millis = 60000;
    opts.retry_delay_millis = 5;

    client.queue("alpha").buffer(opts).push(data_items(0, 1));
    client.queue("beta").buffer(opts).push(data_items(100, 1));

    bool threw = false;
    try {
        client.flush_all_buffers(100);
    } catch (const BufferFlushError& e) {
        threw = true;
        check(e.queue_address() == "alpha/Default",
              "the rethrown error must come from the failing address, got " + e.queue_address());
    }
    check(threw, "flush_all_buffers must rethrow the failing address's error");

    auto payloads = delivered_payloads(server.calls());
    check(payloads == std::vector<int>{100},
          "an unreachable queue must not strand the healthy one's messages");
    check(client.get_buffer_stats()["totalBufferedMessages"] == 1,
          "alpha's message must still be buffered, not dropped");
}

// ============================================================================
// Shutdown
// ============================================================================

void test_close_flushes_then_refuses_new_pushes() {
    CaptureServer server;
    QueenClient client({server.url()}, fast_config());
    BufferOptions opts;
    opts.message_count = 100; // below threshold, timer far away: only close() can flush this
    opts.time_millis = 60000;

    client.queue("orders").buffer(opts).push(data_items(0, 3));
    client.close();

    check(delivered_payloads(server.calls()) == iota_vector(3),
          "close() must flush what was still buffered");

    bool threw = false;
    std::string error_text;
    try {
        client.queue("orders").buffer(opts).push(data_items(3, 1));
    } catch (const std::exception& e) {
        threw = true;
        error_text = e.what();
    }
    check(threw && error_text.find("closed") != std::string::npos,
          "a buffered push after close() must be refused, not silently accepted; got '" +
          error_text + "'");
    check(delivered_payloads(server.calls()) == iota_vector(3),
          "nothing may reach the broker after close()");
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "Queen C++ Client - Buffer Contract Tests" << std::endl;
    std::cout << "========================================\n" << std::endl;

    run_test("options resolve to a bound, never to unbounded", test_options_resolve_bounded_defaults);

    run_test("count threshold triggers a flush", test_count_threshold_triggers_flush);
    run_test("timer flushes a buffer that never reaches its count", test_timer_triggers_flush_below_threshold);
    run_test("batches are message_count-sized and ordered", test_batches_are_message_count_sized_and_ordered);
    run_test("buffers are keyed per queue/partition address", test_buffers_are_keyed_per_queue_partition_address);

    run_test("a failed batch is requeued in order and retried", test_failed_batch_is_requeued_in_order_and_retried);
    run_test("a requeued batch stays ahead of later adds", test_requeued_batch_stays_ahead_of_later_adds);
    run_test("nothing is lost across intermittent failures", test_nothing_lost_across_intermittent_failures);

    run_test("an add at the bound blocks and resumes", test_backpressure_blocks_add_at_bound_and_resumes);
    run_test("a parked add fails loudly on cleanup", test_parked_add_fails_loudly_on_cleanup);

    run_test("an expired flush deadline is loud and drops nothing", test_flush_deadline_is_loud_and_drops_nothing);
    run_test("a zero deadline still gets one real attempt", test_flush_with_zero_deadline_still_attempts_once);
    run_test("flush_all attempts every address and rethrows", test_flush_all_attempts_every_address_and_rethrows);

    run_test("close() flushes, then refuses new pushes", test_close_flushes_then_refuses_new_pushes);

    std::cout << std::endl;
    if (failures == 0) {
        std::cout << GREEN << "All buffer contract tests passed" << RESET << std::endl;
    } else {
        std::cout << RED << failures << " check(s) failed" << RESET << std::endl;
    }

    return failures > 0 ? 1 : 0;
}
