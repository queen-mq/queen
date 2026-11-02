// docs:start(app-cpp-webhooks)
//
// A webhook sender: ordered per endpoint, retried by the broker, and
// dead-lettered with its error when it never succeeds.
//
// Deliveries to one endpoint have to arrive in the order the events happened,
// an endpoint that is down must not slow anybody else down, a failure is
// retried a bounded number of times, and what never succeeds has to end up
// somewhere a person can read it. Each endpoint gets a partition of its own,
// created by its first delivery, so a dead endpoint backs up its own partition
// and nothing else. Retries are the broker's retry budget, and an exhausted
// delivery lands in the dead-letter queue with the error attached.
//
//   webhook-deliveries (one partition per endpoint)
//     `-- group "sender"  POSTs each delivery; a nack spends one retry
//           `-- retryLimit spent -> dead-letter queue, with the error
//
// Build it. queen_client.hpp carries its nlohmann/json beside it; the one
// header to install is cpp-httplib's (Homebrew puts httplib.h under
// /opt/homebrew/include). The client switches on cpp-httplib's OpenSSL support
// for https://, so -lssl -lcrypto are needed even over plain http.
//   mkdir -p build
//   c++ -std=c++17 -O1 -pthread \
//       -I../../../clients/client-cpp \
//       -I/opt/homebrew/include -I"$(brew --prefix openssl)/include" \
//       webhooks.cpp -o build/webhooks \
//       -L"$(brew --prefix openssl)/lib" -lssl -lcrypto -lpthread
//
// Run it:
//   QUEEN_URL=http://localhost:6632 ./build/webhooks

#include "queen_client.hpp"

#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

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

// Three subscribers. One of them answers 500 to everything. It is listed first,
// so its deliveries are the oldest in the queue and its partition is usually
// handed out first: a sender that let a failing endpoint hold up the others
// would fail the checks below.
static const std::vector<Endpoint> ENDPOINTS = {
    {"initech.example", false},
    {"acme.example", true},
    {"globex.example", true},
};
static const int EVENTS_PER_ENDPOINT = 3;
static const int RETRY_LIMIT = 2;
static const std::string GROUP = "sender";

// What a green run looks like: every healthy delivery succeeds once, and each
// dead one is attempted RETRY_LIMIT + 1 times before the budget is gone.
static const int HEALTHY_DELIVERIES = EVENTS_PER_ENDPOINT * 2;
static const int DEAD_DELIVERIES = EVENTS_PER_ENDPOINT;
static const int EXPECTED_ATTEMPTS =
    HEALTHY_DELIVERIES + DEAD_DELIVERIES * (RETRY_LIMIT + 1);

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

// Stands in for the HTTP POST to the subscriber. A real sender POSTs with an
// HTTP client and throws on any status that is not 2xx, which is what this
// does.
static void post_to_endpoint(const std::string& host) {
    if (!is_healthy(host)) {
        throw std::runtime_error(host + " answered 500");
    }
}

static std::string join(const std::vector<int>& values) {
    std::ostringstream out;
    for (size_t i = 0; i < values.size(); ++i) {
        if (i) out << ",";
        out << values[i];
    }
    return out.str();
}

int main() {
    const char* env_url = std::getenv("QUEEN_URL");
    const std::string QUEEN_URL = env_url ? env_url : "http://localhost:6632";
    const std::string DELIVERIES = "app-cpp-webhooks-" + run_id();

    queen::Queen client(QUEEN_URL);

    std::string verdict;
    bool failed = false;

    try {
        std::cout << "broker " << QUEEN_URL << std::endl;

        // retryLimit is the delivery budget: a delivery that fails
        // RETRY_LIMIT + 1 times is filed in the dead-letter queue with its last
        // error, because dlqAfterMaxRetries is on. The broker turns that flag
        // on by default, and it is written out here because the dead-letter
        // checks at the end depend on it. leaseTime is how long the broker
        // waits for a sender that took a delivery and never came back before
        // it hands the delivery to another sender.
        queen::QueueConfig options;
        options.lease_time = 30;
        options.retry_limit = RETRY_LIMIT;
        options.dlq_after_max_retries = true;
        json configured = client.queue(DELIVERIES).configure(options);
        if (!configured.value("configured", false)) {
            throw std::runtime_error("the broker did not configure the queue");
        }

        // -------------------------------------------------------------- queuing
        //
        // The application emits events. Each delivery goes into the partition
        // of the endpoint it is for, which is what makes "in order per
        // subscriber" a property of the storage instead of something the
        // sender has to arrange.
        std::cout << "\nqueuing deliveries" << std::endl;
        for (int seq = 1; seq <= EVENTS_PER_ENDPOINT; ++seq) {
            for (const Endpoint& endpoint : ENDPOINTS) {
                // The event id. An application that retries its own emit does
                // not create a second delivery.
                client.queue(DELIVERIES).partition(endpoint.host).push(
                    {{"endpoint", endpoint.host},
                     {"seq", seq},
                     {"type", "invoice.paid"},
                     {"invoiceId", "INV-" + std::to_string(seq)}},
                    endpoint.host + "-evt-" + std::to_string(seq));
            }
        }
        std::cout << "  " << EVENTS_PER_ENDPOINT * ENDPOINTS.size()
                  << " deliveries queued" << std::endl;

        // -------------------------------------------------------------- sending
        //
        // The sender pool: three workers, each a thread with a poll loop of its
        // own. A failed delivery goes back to the broker with its error: the
        // broker redelivers it and counts one retry, and deliveryAttempt says
        // which attempt this is. The retries live in the broker, so they
        // survive the sender dying halfway, which a retry loop inside the
        // handler would not.
        //
        // auto_ack(false): the handler acknowledges each delivery itself,
        // because it reads the broker's answer to each one: the dlq flag says
        // when a failure spent the last retry. partitions(1) and batch(1):
        // every pop takes ONE delivery, from one endpoint. A nack releases the
        // endpoint's lease, and the broker then refuses acks for anything else
        // popped under it, so a worker holding more than the failed delivery
        // could not acknowledge the rest.
        //
        //   wait(false)     every pop answers at once; the stop and the idle
        //                   deadline are consulted between pops.
        //   idle_millis     the deadline. A delivery that never comes back
        //                   fails this run instead of hanging it.
        //   limit()         every attempt the run should make, counted across
        //                   the workers. What ends the loop first is the
        //                   outcome: every healthy delivery sent and every dead
        //                   one dead-lettered cancels it.
        std::cout << "\nsending" << std::endl;
        std::mutex lock;
        std::map<std::string, std::vector<int>> delivered_to;
        std::map<std::string, int> attempts;
        int dead_lettered = 0;
        int delivered_total = 0;
        queen::Cancel stop;
        std::exception_ptr handler_error;

        client.queue(DELIVERIES)
            .group(GROUP)
            .subscription_mode("all")
            .concurrency(3)
            .partitions(1)
            .batch(1)
            .auto_ack(false)
            .limit(EXPECTED_ATTEMPTS)
            .wait(false)
            .idle_millis(6000)
            .cancel(stop)
            .consume([&](const queen::Message& msg) {
                try {
                    const std::string host = msg.data["endpoint"].get<std::string>();
                    const int seq = msg.data["seq"].get<int>();

                    std::string error;
                    try {
                        post_to_endpoint(host);
                    } catch (const std::exception& e) {
                        error = e.what();
                    }

                    {
                        // Recorded before the ack: the ack releases the
                        // endpoint's lease, and another worker could take and
                        // record the next delivery before this one otherwise.
                        std::lock_guard<std::mutex> guard(lock);
                        attempts[host] += 1;
                        if (error.empty()) {
                            delivered_to[host].push_back(seq);
                            ++delivered_total;
                            std::cout << "  " << host << " <- event " << seq << std::endl;
                        } else {
                            std::cout << "  " << host << " <- event " << seq
                                      << " failed on attempt "
                                      << msg.delivery_attempt << ": "
                                      << error << std::endl;
                        }
                    }

                    // The ack takes the consumer group and the lease from the
                    // message. A failure carries its error, which is what the
                    // dead letter keeps once the budget is spent.
                    queen::AckResult ack = error.empty() ? client.ack(msg) : client.nack(msg, error);

                    // A refused acknowledgement still arrives as HTTP 200 with
                    // success: false on the item, and ack() returns it as a value.
                    if (!ack.success) {
                        throw std::runtime_error("the broker refused the acknowledgement for " +
                                                 host + "/" + std::to_string(seq) + ": " + ack.error);
                    }

                    std::lock_guard<std::mutex> guard(lock);
                    if (ack.dlq) {
                        // This failure spent the last of the retry budget, and
                        // the delivery is now a dead letter.
                        ++dead_lettered;
                    }
                    if (delivered_total >= HEALTHY_DELIVERIES &&
                        dead_lettered >= DEAD_DELIVERIES) {
                        stop.cancel();
                    }
                } catch (...) {
                    // auto_ack is off, so nothing acknowledges behind this
                    // handler's back: an escaped exception leaves the delivery
                    // leased until it expires. Carry the error out and stop.
                    std::lock_guard<std::mutex> guard(lock);
                    if (!handler_error) handler_error = std::current_exception();
                    stop.cancel();
                }
            });
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
                  endpoint.host + " received them in the order they happened (got " +
                      (seqs.empty() ? std::string("none") : join(seqs)) + ")");
        }

        check(delivered_to["initech.example"].empty(),
              "the dead endpoint received nothing");
        check(attempts["initech.example"] == DEAD_DELIVERIES * (RETRY_LIMIT + 1),
              "each dead delivery was tried " + std::to_string(RETRY_LIMIT + 1) +
                  " times before it was given up (" +
                  std::to_string(attempts["initech.example"]) + " attempts)");

        // The dead letters are records you can query. Each one keeps the
        // payload, so it names the endpoint and the invoice, and the last error,
        // which is what answers "why did this customer not get the webhook".
        // dlq().get() throws when the broker cannot be asked.
        json dlq = client.queue(DELIVERIES).dlq().limit(50).get();
        const json& letters = dlq["messages"];

        std::vector<json> dead;
        for (const json& letter : letters) {
            if (letter["data"]["endpoint"] == "initech.example") dead.push_back(letter);
        }

        check(dead.size() == static_cast<size_t>(EVENTS_PER_ENDPOINT),
              "all " + std::to_string(EVENTS_PER_ENDPOINT) +
                  " dead deliveries are in the dead-letter queue");

        bool every_letter_explains_itself = true;
        for (const json& letter : dead) {
            const std::string error = letter.value("errorMessage", std::string());
            if (error.find("answered 500") == std::string::npos) {
                every_letter_explains_itself = false;
            }
        }
        check(every_letter_explains_itself,
              "each dead letter carries the error that killed it");

        bool only_the_dead_endpoint = true;
        for (const json& letter : letters) {
            if (letter["data"]["endpoint"] != "initech.example") only_the_dead_endpoint = false;
        }
        check(only_the_dead_endpoint,
              "no healthy delivery ended up in the dead-letter queue");

        std::ostringstream summary;
        for (size_t i = 0; i < dead.size(); ++i) {
            if (i) summary << "; ";
            summary << dead[i]["data"]["endpoint"].get<std::string>() << "/"
                    << dead[i]["data"]["invoiceId"].get<std::string>() << ": "
                    << dead[i].value("errorMessage", std::string());
        }
        std::cout << "\n  dead letters: " << summary.str() << std::endl;

        // Clean up on success only: a failed run leaves the queue and its dead
        // letters on the broker to be looked at.
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
