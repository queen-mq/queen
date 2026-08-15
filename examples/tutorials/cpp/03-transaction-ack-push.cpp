// docs:start(tut-cpp-transaction-ack-push)
//
// Tutorial 3 of 4: acknowledge and push in one transaction.
//
// Tutorial 2 handed work from one queue to the next in two steps: push the
// derived message, then let the loop acknowledge the source. Between those two
// steps a crash duplicates work, and in the other order it loses work.
//
// A Queen transaction closes that window: the acknowledgement of the input and
// the push of the output are one PostgreSQL transaction. Both land or neither
// does.
//
// Build it (see 01-hello-world.cpp for the two headers queen_client.hpp
// expects but this repository does not vendor):
//   mkdir -p build
//   g++ -std=c++17 -O2 -pthread \
//       -I../../../clients/client-cpp -I../../../clients/server/vendor \
//       -I/opt/homebrew/include -I"$(brew --prefix openssl)/include" \
//       03-transaction-ack-push.cpp -o build/03-transaction-ack-push \
//       -L"$(brew --prefix openssl)/lib" -lssl -lcrypto -lpthread
//
// Run it:
//   QUEEN_URL=http://localhost:6632 ./build/03-transaction-ack-push

#include "queen_client.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
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

struct Order {
    std::string order_id;
    std::string customer;
    double total;
};

static const std::vector<Order> INPUT = {
    {"A-1", "acme", 120.5},
    {"B-1", "globex", 88.75},
    {"C-1", "initech", 310.0},
};

static int checks = 0;

static void check(bool condition, const std::string& description) {
    if (!condition) throw std::runtime_error(description);
    ++checks;
    std::cout << "  ok: " << description << std::endl;
}

int main() {
    const char* env_url = std::getenv("QUEEN_URL");
    const std::string QUEEN_URL = env_url ? env_url : "http://localhost:6632";
    const std::string RUN = run_id();
    const std::string ORDERS = "tut-cpp-tx-orders-" + RUN;
    const std::string INVOICES = "tut-cpp-tx-invoices-" + RUN;
    const std::string GROUP = "tut-cpp-invoicing";

    QueenClient client(QUEEN_URL);

    std::string verdict;
    bool failed = false;

    try {
        std::cout << "broker " << QUEEN_URL << std::endl;

        for (const Order& order : INPUT) {
            client.queue(ORDERS).partition(order.customer).push({
                json{{"data", {{"orderId", order.order_id},
                               {"customer", order.customer},
                               {"total", order.total}}}}
            });
        }
        std::cout << "pushed " << INPUT.size() << " orders" << std::endl;

        std::cout << "\ninvoicing" << std::endl;
        std::vector<std::string> invoiced;
        std::exception_ptr handler_error;
        std::atomic<bool> stop{false};

        // auto_ack(false) is what makes this tutorial possible: the loop must
        // not acknowledge behind your back, because the acknowledgement is part
        // of the transaction below.
        client.queue(ORDERS)
            .group(GROUP)
            .subscription_mode("all")
            .each()
            .auto_ack(false)
            .limit(static_cast<int>(INPUT.size()))
            .wait(false)
            .idle_millis(5000)
            .consume([&](const json& msg) {
                try {
                    const json& data = msg["data"];

                    // One commit carries both operations. The ack names the
                    // consumer group explicitly: the broker does not read it off
                    // the message, and an ack sent without it commits the wrong
                    // cursor.
                    //
                    // Here the C++ client makes you drop to the wire.
                    // client.transaction() builds exactly this request, but
                    // its push sub-builder emits only queue, payload and
                    // transactionId -- it has no partition() step, so it can
                    // only ever write to a queue's Default lane. The invoices
                    // here are partitioned per customer, so the request is
                    // assembled by hand and sent through the client's own HTTP
                    // transport rather than a second HTTP library.
                    //
                    // requiredLeases is what makes the commit fail if the lease
                    // has expired, which is what stops a slow consumer from
                    // acking work the broker has already handed to someone else.
                    json push_item = {
                        {"queue", INVOICES},
                        {"partition", data["customer"]},
                        {"payload", {{"invoiceId", "INV-" + data["orderId"].get<std::string>()},
                                     {"orderId", data["orderId"]},
                                     {"amount", data["total"]}}},
                        {"transactionId", queen::util::generate_uuid_v7()},
                    };

                    json transaction = {
                        {"operations", json::array({
                            json{{"type", "push"}, {"items", json::array({push_item})}},
                            json{{"type", "ack"},
                                 {"transactionId", msg["transactionId"]},
                                 {"partitionId", msg["partitionId"]},
                                 {"status", "completed"},
                                 {"consumerGroup", GROUP}},
                        })},
                        {"requiredLeases", json::array({msg["leaseId"]})},
                    };

                    json result = client.get_http_client()->post("/api/v1/transaction",
                                                                 transaction);

                    // Check the transaction, not just the absence of an
                    // exception.
                    if (!result.value("success", false)) {
                        throw std::runtime_error("transaction rejected: " +
                                                 result.value("error", std::string("unknown")));
                    }

                    invoiced.push_back(data["orderId"].get<std::string>());
                    std::cout << "  " << data["orderId"].get<std::string>() << " -> INV-"
                              << data["orderId"].get<std::string>() << std::endl;
                } catch (...) {
                    // The consumer swallows handler exceptions, so carry the
                    // error out by hand and stop the loop.
                    if (!handler_error) handler_error = std::current_exception();
                    stop = true;
                }
            }, &stop);
        if (handler_error) std::rethrow_exception(handler_error);

        check(invoiced.size() == INPUT.size(), "every order was invoiced once");

        std::cout << "\nchecking the output queue" << std::endl;

        // The invoices went to one partition per customer, and a pop claims a
        // single partition unless you say otherwise: partitions(10) lets this
        // one call claim up to ten of them, with batch as the total budget
        // across all of them.
        json invoices = client.queue(INVOICES).batch(10).partitions(10).wait(true).pop();

        check(invoices.size() == INPUT.size(),
              std::to_string(INPUT.size()) + " invoices exist");

        std::vector<std::string> ids;
        for (const json& invoice : invoices) {
            ids.push_back(invoice["data"]["orderId"].get<std::string>());
        }
        std::sort(ids.begin(), ids.end());

        std::vector<std::string> expected;
        for (const Order& order : INPUT) expected.push_back(order.order_id);
        std::sort(expected.begin(), expected.end());

        check(ids == expected, "each invoice matches an order, none duplicated");

        // And the input queue is committed for this group: the acks were part
        // of the same transactions that produced those invoices, so the two
        // states cannot disagree.
        json leftovers = client.queue(ORDERS).group(GROUP).batch(10).wait(false).pop();
        check(leftovers.empty(), "the source queue is committed for this group");

        client.queue(ORDERS).del();
        client.queue(INVOICES).del();

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
