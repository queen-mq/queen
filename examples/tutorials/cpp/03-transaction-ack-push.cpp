// docs:start(tut-cpp-transaction-ack-push)
//
// Tutorial 3 of 4: acknowledge and push in one transaction.
//
// Tutorial 2 handed work from one queue to the next in two steps: push the
// derived message, then let the loop acknowledge the source. Between those two
// steps a crash duplicates work, and in the other order it loses work.
//
// A Queen transaction closes that window: the acknowledgement of the input and
// the push of the output are one entry in the broker's replicated log. Both
// land or neither does.
//
// Build it (01-hello-world.cpp says why each flag is there):
//   mkdir -p build
//   g++ -std=c++17 -O2 -pthread \
//       -I../../../clients/client-cpp \
//       -I/opt/homebrew/include -I"$(brew --prefix openssl)/include" \
//       03-transaction-ack-push.cpp -o build/03-transaction-ack-push \
//       -L"$(brew --prefix openssl)/lib" -lssl -lcrypto -lpthread
//
// Run it:
//   QUEEN_URL=http://localhost:6632 ./build/03-transaction-ack-push

#include "queen_client.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
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

    queen::Queen client(QUEEN_URL);

    std::string verdict;
    bool failed = false;

    try {
        std::cout << "broker " << QUEEN_URL << std::endl;

        for (const Order& order : INPUT) {
            client.queue(ORDERS).partition(order.customer).push(
                {{"orderId", order.order_id}, {"customer", order.customer}, {"total", order.total}});
        }
        std::cout << "pushed " << INPUT.size() << " orders" << std::endl;

        std::cout << "\ninvoicing" << std::endl;
        std::vector<std::string> invoiced;
        std::exception_ptr handler_error;
        queen::Cancel stop;

        // auto_ack(false) is what makes this tutorial possible: the loop must
        // not acknowledge behind your back, because the acknowledgement is part
        // of the transaction below.
        client.queue(ORDERS)
            .group(GROUP)
            .subscription_mode("all")
            .auto_ack(false)
            .limit(static_cast<long long>(INPUT.size()))
            .wait(false)
            .idle_millis(5000)
            .cancel(stop)
            .consume([&](const queen::Message& msg) {
                try {
                    const json& data = msg.data;
                    const std::string id = data["orderId"].get<std::string>();

                    // One commit carries both operations: the push of the
                    // invoice into the customer's partition, and the ack of the
                    // order. The ack takes the consumer group and the lease from
                    // the message, and the lease is what makes the commit fail
                    // if it has expired: that stops a slow consumer from acking
                    // work the broker has already handed to someone else.
                    queen::TransactionResult result =
                        client.transaction()
                            .queue(INVOICES)
                            .partition(data["customer"].get<std::string>())
                            .push({{"invoiceId", "INV-" + id},
                                   {"orderId", data["orderId"]},
                                   {"amount", data["total"]}},
                                  "invoice-" + id)  // the dedup key: a retried step is a no-op
                            .ack(msg)
                            .commit();

                    // commit() throws when the broker rolls the step back; a
                    // success false that comes back is a lost precondition,
                    // which this step does not set.
                    if (!result.success) {
                        throw std::runtime_error("transaction rejected: " + result.error);
                    }

                    invoiced.push_back(id);
                    std::cout << "  " << id << " -> INV-" << id << std::endl;
                } catch (...) {
                    // The consumer turns a handler's exception into a nack and
                    // keeps going, so carry the error out by hand and stop the
                    // loop.
                    if (!handler_error) handler_error = std::current_exception();
                    stop.cancel();
                }
            });
        if (handler_error) std::rethrow_exception(handler_error);

        check(invoiced.size() == INPUT.size(), "every order was invoiced once");

        std::cout << "\nchecking the output queue" << std::endl;

        // The invoices went to one partition per customer, and a pop claims a
        // single partition unless you say otherwise: partitions(10) lets this
        // one call claim up to ten of them, with batch as the total budget
        // across all of them.
        auto invoices = client.queue(INVOICES).batch(10).partitions(10).wait(true).pop();

        check(invoices.size() == INPUT.size(),
              std::to_string(INPUT.size()) + " invoices exist");

        std::vector<std::string> ids;
        for (const queen::Message& invoice : invoices) {
            ids.push_back(invoice.data["orderId"].get<std::string>());
        }
        std::sort(ids.begin(), ids.end());

        std::vector<std::string> expected;
        for (const Order& order : INPUT) expected.push_back(order.order_id);
        std::sort(expected.begin(), expected.end());

        check(ids == expected, "each invoice matches an order, none duplicated");

        // And the input queue is committed for this group: the acks were part
        // of the same transactions that produced those invoices, so the two
        // states cannot disagree.
        auto leftovers = client.queue(ORDERS).group(GROUP).batch(10).wait(false).pop();
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
