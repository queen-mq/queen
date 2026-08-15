// docs:start(app-cpp-webhooks)
//
// A webhook delivery system.
//
// Every SaaS product ends up writing this one, and it is harder than it looks:
// deliveries to one customer's endpoint must arrive in order, a customer whose
// endpoint is down must not slow down anybody else's, failures must be retried
// a bounded number of times, and what never succeeds has to end up somewhere a
// human can look at.
//
// The shape here is one ordered lane per destination, created by the first
// delivery to it. A dead endpoint backs up its own lane and no other; retries
// are the broker's retry budget rather than a loop in your code; and what
// exhausts the budget lands in the dead-letter queue with the error attached.
//
//   webhook-deliveries (one partition per destination)
//     `-- group "sender"  posts each delivery, fails on a dead endpoint
//           `-- retryLimit exhausted -> dead-letter queue
//
// Build it (see examples/tutorials/cpp/01-hello-world.cpp for the headers
// queen_client.hpp expects but this repository does not vendor -- json.hpp
// under clients/server/vendor, threadpool.hpp under clients/server/include --
// and for why -lssl -lcrypto is required even over plain http):
//   mkdir -p build
//   c++ -std=c++17 -O1 -pthread \
//       -I../../../clients/client-cpp -I../../../clients/server/vendor \
//       -I/opt/homebrew/include -I"$(brew --prefix openssl)/include" \
//       webhooks.cpp -o build/webhooks \
//       -L"$(brew --prefix openssl)/lib" -lssl -lcrypto -lpthread
//
// Run it:
//   QUEEN_URL=http://localhost:6632 ./build/webhooks

#include "queen_client.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

using queen::QueenClient;
using json = nlohmann::json;

static std::string run_id() {
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();
    std::string out;
    const char* digits = "0123456789abcdefghijklmnopqrstuvwxyz";
    while (millis > 0) {
        out.insert(out.begin(), digits[millis % 36]);
        millis /= 36;
    }
    return out;
}

struct Endpoint {
    std::string host;
    bool healthy;
};

// Three subscribers. One of them has let its certificate expire, which is the
// most common way a webhook endpoint dies: it answers, but it answers 500.
static const std::vector<Endpoint> ENDPOINTS = {
    {"acme.example", true},
    {"globex.example", true},
    {"initech.example", false},
};
static const int EVENTS_PER_ENDPOINT = 3;
static const int RETRY_LIMIT = 2;
static const std::string GROUP = "sender";

// What a green run looks like: every healthy delivery succeeds once, and each
// dead one is attempted RETRY_LIMIT + 1 times before the budget is gone.
static const int HEALTHY_DELIVERIES = EVENTS_PER_ENDPOINT * 2;
static const int DEAD_DELIVERIES = EVENTS_PER_ENDPOINT;
static const int EXPECTED_ATTEMPTS =
    HEALTHY_DELIVERIES + EVENTS_PER_ENDPOINT * (RETRY_LIMIT + 1);

static int checks = 0;

// A throwing check: the failure travels to main() as an exception, which is
// what turns it into "FAIL: <reason>" and a non-zero exit.
static void check(bool condition, const std::string& description) {
    if (!condition) throw std::runtime_error(description);
    ++checks;
    std::cout << "  ok: " << description << std::endl;
}

static bool is_healthy(const std::string& host) {
    for (const Endpoint& endpoint : ENDPOINTS) {
        if (endpoint.host == host) return endpoint.healthy;
    }
    throw std::runtime_error("unknown endpoint " + host);
}

// Stands in for the HTTP POST to the subscriber. A real sender would use an
// HTTP client and treat any non-2xx as a failure, which is exactly what
// throwing does here.
static void post_to_endpoint(const std::string& host) {
    if (!is_healthy(host)) {
        throw std::runtime_error(host + " answered 500");
    }
}

static std::string join(const std::vector<int>& values) {
    std::ostringstream out;
    for (size_t i = 0; i < values.size(); ++i) {
        if (i) out << ", ";
        out << values[i];
    }
    return out.str();
}

int main() {
    const char* env_url = std::getenv("QUEEN_URL");
    const std::string QUEEN_URL = env_url ? env_url : "http://localhost:6632";
    const std::string DELIVERIES = "app-cpp-webhooks-" + run_id();

    QueenClient client(QUEEN_URL);

    std::string verdict;
    bool failed = false;

    try {
        std::cout << "broker " << QUEEN_URL << std::endl;

        // retryLimit is the delivery budget, and dlqAfterMaxRetries is what
        // happens when it runs out: the broker moves the exhausted message to
        // the dead-letter table with the last error on the row. The broker
        // defaults that flag to true, so it is set here to say what this
        // program depends on rather than to switch anything on; turning it off
        // is what would leave an exhausted delivery merely marked failed and
        // sitting in its lane.
        //
        // leaseTime is the other half of the contract: it is how long the
        // broker waits for a sender that took a delivery and never came back
        // before handing that delivery to someone else.
        //
        // The C++ QueueConfig struct has no dlqAfterMaxRetries field, so the
        // options object is assembled by hand and posted through the client's
        // own HTTP transport, which keeps the base URL, the retry and the 429
        // backoff policy that every other call in this file uses. Writing the
        // flag out is worth the detour because this program's last three checks
        // are about the dead-letter rows.
        json configured = client.get_http_client()->post(
            "/api/v1/configure",
            json{{"queue", DELIVERIES},
                 {"options", {{"leaseTime", 30},
                              {"retryLimit", RETRY_LIMIT},
                              {"dlqAfterMaxRetries", true}}}});
        if (!configured.value("configured", false)) {
            throw std::runtime_error("the broker did not configure the queue");
        }

        // -------------------------------------------------------------- queuing
        //
        // The application emits events. Each one goes into the partition of the
        // endpoint it is destined for, which is what makes "in order per
        // subscriber" a property of the storage rather than of the sender.
        std::cout << "\nqueuing deliveries" << std::endl;
        for (int seq = 1; seq <= EVENTS_PER_ENDPOINT; ++seq) {
            for (const Endpoint& endpoint : ENDPOINTS) {
                // The event id makes the enqueue idempotent: an application
                // that retries its own emit does not create a second delivery.
                client.queue(DELIVERIES).partition(endpoint.host).push({
                    json{{"transactionId",
                          endpoint.host + "-evt-" + std::to_string(seq)},
                         {"data", {{"endpoint", endpoint.host},
                                   {"seq", seq},
                                   {"type", "invoice.paid"},
                                   {"invoiceId", "INV-" + std::to_string(seq)}}}}
                });
            }
        }
        std::cout << "  " << EVENTS_PER_ENDPOINT * ENDPOINTS.size()
                  << " deliveries queued" << std::endl;

        // -------------------------------------------------------------- sending
        //
        // The sender pool. Here this client parts company with the JavaScript:
        // its automatic acknowledgement turns a thrown handler into a nack with
        // a null error, so the dead-letter rows would arrive with no reason on
        // them and a support engineer would have nothing to read. auto_ack
        // (false) hands the acknowledgement back to the handler, which sends
        // the failure with the message that caused it.
        //
        // That is the only difference. The retries are still the broker's: a
        // failed acknowledgement puts the delivery back in its lane until the
        // retry budget is gone, and that survives the sender process dying
        // mid-flight, which a loop inside the handler would not.
        //
        // concurrency(3) runs three poll loops and each pop claims one
        // partition, so the three destinations are drained in parallel and the
        // dead one backs up alone.
        //
        //   wait(false)     keeps the idle clock meaningful: it is only
        //                   consulted between polls, so a long-polling pop
        //                   would stretch the 6 second budget to the length of
        //                   one server-side park.
        //   idle_millis     the deadline. A delivery that never comes back
        //                   fails this run instead of hanging it.
        //   limit()         counts per worker, here and in the JavaScript
        //                   alike: each worker keeps its own tally, so with
        //                   three workers it is a backstop rather than the
        //                   thing that ends the run. The stop flag below is
        //                   what actually ends the loop, and it is raised on
        //                   the outcome -- every healthy delivery sent and
        //                   every dead one dead-lettered -- rather than on an
        //                   attempt count.
        std::cout << "\nsending" << std::endl;
        std::mutex lock;
        std::map<std::string, std::vector<int>> delivered_to;
        std::map<std::string, int> attempts;
        int dead_lettered = 0;
        int delivered_total = 0;
        std::atomic<bool> stop{false};
        std::exception_ptr handler_error;

        client.queue(DELIVERIES)
            .group(GROUP)
            .subscription_mode("all")
            .concurrency(3)
            .each()
            .auto_ack(false)
            .limit(EXPECTED_ATTEMPTS)
            .wait(false)
            .idle_millis(6000)
            .consume([&](const json& msg) {
                try {
                    const std::string host = msg["data"]["endpoint"].get<std::string>();
                    const int seq = msg["data"]["seq"].get<int>();

                    // The popped message carries no attempt counter, so a
                    // sender that wants to back off or give up early on an
                    // error it knows is permanent counts the attempts itself.
                    // That is all this map is.
                    {
                        std::lock_guard<std::mutex> guard(lock);
                        attempts[host] += 1;
                    }

                    std::string error;
                    try {
                        post_to_endpoint(host);
                    } catch (const std::exception& e) {
                        error = e.what();
                    }

                    // The acknowledgement names the consumer group explicitly:
                    // the broker does not read it off the message, and an ack
                    // sent without it commits the wrong cursor.
                    json context = {{"group", GROUP}};
                    if (!error.empty()) context["error"] = error;
                    json ack = client.ack(msg, error.empty(), context);

                    // A rejected acknowledgement still arrives as HTTP 200 with
                    // success: false on the item, so the per-item flag is the
                    // only proof the broker took it. The outer "success" only
                    // says the call did not throw.
                    if (!ack.value("success", false) || !ack["result"].is_array() ||
                        ack["result"].empty() ||
                        !ack["result"][0].value("success", false)) {
                        throw std::runtime_error("the broker rejected an acknowledgement");
                    }

                    std::lock_guard<std::mutex> guard(lock);
                    if (error.empty()) {
                        delivered_to[host].push_back(seq);
                        ++delivered_total;
                        std::cout << "  " << host << " <- event " << seq << std::endl;
                    } else if (ack["result"][0].value("dlq", false)) {
                        // The broker says this failure spent the last of the
                        // retry budget and the delivery is now a dead letter.
                        ++dead_lettered;
                    }

                    if (delivered_total >= HEALTHY_DELIVERIES &&
                        dead_lettered >= DEAD_DELIVERIES) {
                        stop = true;
                    }
                } catch (...) {
                    // auto_ack is off, so nothing acknowledges behind this
                    // handler's back: an escaped exception leaves the delivery
                    // leased until it expires. Carry the error out and stop.
                    std::lock_guard<std::mutex> guard(lock);
                    if (!handler_error) handler_error = std::current_exception();
                    stop = true;
                }
            }, &stop);
        if (handler_error) std::rethrow_exception(handler_error);

        // ------------------------------------------------------------- checking
        std::cout << "\nchecking" << std::endl;

        std::vector<int> in_order;
        for (int seq = 1; seq <= EVENTS_PER_ENDPOINT; ++seq) in_order.push_back(seq);

        for (const Endpoint& endpoint : ENDPOINTS) {
            if (!endpoint.healthy) continue;
            const std::vector<int>& seqs = delivered_to[endpoint.host];
            check(seqs.size() == static_cast<size_t>(EVENTS_PER_ENDPOINT),
                  endpoint.host + " received all " +
                      std::to_string(EVENTS_PER_ENDPOINT) + " events");
            check(seqs == in_order,
                  endpoint.host + " received them in the order they happened");
        }

        check(delivered_to["initech.example"].empty(),
              "the dead endpoint received nothing, as it should");
        check(attempts["initech.example"] > EVENTS_PER_ENDPOINT,
              "the dead endpoint was retried rather than dropped on the first "
              "failure");

        // The dead-letter queue is a table you can read, not a log line. Each
        // row carries the payload, the endpoint it was for, and the last error,
        // which is what a support engineer needs to answer "why did this
        // customer not get it".
        //
        // dlq().get() swallows a transport failure and answers with an empty
        // page rather than throwing, so an unreachable broker shows up as the
        // next three checks failing rather than as an exception.
        json dlq = client.queue(DELIVERIES).dlq().limit(50).get();
        const json& rows = dlq["messages"];

        std::vector<json> dead;
        for (const json& row : rows) {
            if (row["data"]["endpoint"] == "initech.example") dead.push_back(row);
        }

        check(dead.size() == static_cast<size_t>(EVENTS_PER_ENDPOINT),
              "all " + std::to_string(EVENTS_PER_ENDPOINT) +
                  " dead deliveries are in the dead-letter queue");

        bool every_row_explains_itself = true;
        for (const json& row : dead) {
            const std::string message = row.value("errorMessage", std::string());
            if (message.find("answered 500") == std::string::npos) {
                every_row_explains_itself = false;
            }
        }
        check(every_row_explains_itself,
              "each dead-letter row carries the error that killed it");

        bool only_the_dead_endpoint = true;
        for (const json& row : rows) {
            if (row["data"]["endpoint"] != "initech.example") only_the_dead_endpoint = false;
        }
        check(only_the_dead_endpoint,
              "no healthy endpoint put anything in the dead-letter queue");

        std::ostringstream letters;
        for (size_t i = 0; i < dead.size(); ++i) {
            if (i) letters << ", ";
            letters << dead[i]["data"]["endpoint"].get<std::string>() << "/"
                    << dead[i]["data"]["invoiceId"].get<std::string>();
        }
        std::cout << "\n  dead letters: " << letters.str() << std::endl;
        std::cout << "  attempts on the dead endpoint: "
                  << attempts["initech.example"] << ", on "
                  << ENDPOINTS[0].host << ": " << attempts[ENDPOINTS[0].host]
                  << " (" << join(delivered_to[ENDPOINTS[0].host]) << ")"
                  << std::endl;

        // Clean up on success only: a failed run leaves the queue and its
        // dead-letter rows on the broker to be looked at.
        client.queue(DELIVERIES).del();

        verdict = "\nPASS: " + std::to_string(checks) + " checks";
    } catch (const std::exception& err) {
        verdict = std::string("\nFAIL: ") + err.what();
        failed = true;
    }

    client.close();

    (failed ? std::cerr : std::cout) << verdict << std::endl;
    return failed ? 1 : 0;
}
// docs:end
