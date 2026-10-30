// Queen in five minutes: the program behind the homepage guide, in C++.
//
// The homepage shows the regions between `guide:start(<step>)` and `guide:end`,
// verbatim, minus lines ending in `guide:hide`. Everything outside them gives
// the program a clean start and checks that each step did what the page says.
// The C++ client has no streams, so this program skips that step. Two steps go
// to the broker's HTTP API through the client's own HTTP layer, because the
// client has no call for them: `configure` (QueueConfig has no retentionEnabled)
// and `transaction` (a transaction's push cannot name a partition).
//
// Build it from this directory, as one command (01-hello-world.cpp says why
// each flag is there):
//   g++ -std=c++17 -O1 -pthread -I../../../clients/client-cpp
//       -I../../../clients/server/vendor -I/opt/homebrew/include
//       -I"$(brew --prefix openssl)/include" 00-five-minutes.cpp -o build/00-five-minutes
//       -L"$(brew --prefix openssl)/lib" -lssl -lcrypto -lpthread
//
// Run it against the node that step 1 of the guide starts:
//   docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.6.0
//   ./build/00-five-minutes
// run.sh builds a copy with localhost:6632 replaced by $QUEEN_URL.

// guide:start(configure)
#include "queen_client.hpp"

using json = nlohmann::json;
// guide:end

#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

using namespace std::chrono_literals;

// A failed check travels to main() as an exception, which prints it and exits 1.
static void check(bool ok, const std::string& what) {
    if (!ok) throw std::runtime_error(what);
}

int main() try {
    queen::QueenClient admin("http://localhost:6632");
    for (const char* q : {"orders", "shipments", "reminders", "reports"}) {
        try {
            admin.queue(q).del();
        } catch (const std::exception&) {
        }
    }
    admin.ephemeral().reset("price-requests");  // nothing left over from an earlier run

    // guide:start(configure)
    queen::QueenClient client("http://localhost:6632");

    // QueueConfig has no retentionEnabled, so the options go to the broker's
    // configure route through the client's own HTTP layer.
    client.get_http_client()->post("/api/v1/configure", {
        {"queue", "orders"},
        {"namespace", "shop"},              // groups queues in the dashboard
        {"options", {
            {"leaseTime", 30},              // seconds a worker holds a message,
                                            // then it is redelivered
            {"retryLimit", 3},              // failures before the message goes
                                            // to the dead-letter queue
            {"retentionEnabled", true},     // retention runs only when this is on
            {"retentionSeconds", 86400},    // then messages older than a day are removed
        }},
    });
    // guide:end
    {
        json queue = client.get_http_client()->get("/api/v1/resources/queues/orders");
        check(queue["namespace"] == "shop", "the queue is in the shop namespace");
        check(queue["options"]["leaseTime"] == 30, "leaseTime is 30 s");
        check(queue["options"]["retentionEnabled"] == true, "retention is on");
        check(queue["options"]["retentionSeconds"] == 86400, "retention keeps a day");
    }
    std::cout << "ok: configure" << std::endl;

    // guide:start(push)
    client.queue("orders")
        .partition("customer-42") // one ordered partition per customer, created by the push
        .push({{
            {"transactionId", "order-1001"},  // the dedup key: pushing it again is a no-op
            {"data", {{"orderId", 1001}, {"customer", 42}, {"total", 59.9}}},
        }});
    // guide:end
    std::cout << "ok: push" << std::endl;

    // guide:start(consume)
    client.queue("orders")
        .each()                        // the handler gets one message at a time
        .limit(1)                      // stop after one message; a worker leaves this out
        .consume([](const json& msg) { // acked when it returns, retried if it throws
            std::cout << msg["data"] << std::endl;
        });
    // guide:end
    check(client.queue("orders").wait(false).pop().empty(), "queue mode acked the order");
    std::cout << "ok: consume" << std::endl;

    json billed;
    // guide:start(consume-group)
    client.queue("orders")
        .group("billing")          // every group gets every message
        .subscription_mode("all")  // a new group starts at the tail unless told otherwise
        .batch(100)                // up to 100 messages per pop...
        .partitions(10)            // ...from up to 10 partitions
        .renew_lease(true, 2000)   // heartbeat every 2 s while the handler runs
        .auto_ack(false)           // the handler decides the outcome
        .each()
        .limit(1)
        .consume([&](const json& msg) {
            bool ok = msg["data"]["total"].get<double>() > 0;
            client.ack(msg, ok, {{"group", "billing"}});  // true: completed, false: failed
            billed = msg["data"]["orderId"]; // guide:hide
        });
    // guide:end
    check(billed == 1001, "the billing group read the order pushed before it existed");
    std::cout << "ok: consume-group" << std::endl;

    // guide:start(transaction)
    client.queue("orders")
        .group("fulfillment")
        .subscription_mode("all")
        .auto_ack(false)
        .each()
        .limit(1)
        .consume([&](const json& msg) {
            const json& order = msg["data"];
            const std::string id = std::to_string(order["orderId"].get<int>());
            queen::TimerSchedule reminder;                       // a message tomorrow
            reminder.queue = "reminders";
            reminder.timer_key = "review-" + id;
            reminder.delay_millis = 24 * 60 * 60 * 1000;
            reminder.payload = {{"orderId", order["orderId"]},
                                {"customer", order["customer"]}};

            // client.transaction() cannot give its push a partition, so the step goes
            // to the broker's transaction route through the client's own HTTP layer.
            json step = {
                {"operations", json::array({
                    {{"type", "ack"}, {"status", "completed"},   // the input is done
                     {"consumerGroup", "fulfillment"}, {"leaseId", msg["leaseId"]},
                     {"transactionId", msg["transactionId"]},
                     {"partitionId", msg["partitionId"]}},
                    {{"type", "push"}, {"items", json::array({{  // the next event
                        {"queue", "shipments"}, {"partition", msg["partition"]},
                        {"transactionId", "ship-" + id},
                        {"payload", {{"orderId", order["orderId"]}}},
                    }})}},
                })},
                {"requiredLeases", json::array({msg["leaseId"]})},
                {"kv", json::array({queen::wire::kv_write_op("put", "orders", "order-" + id,
                    {{"status", "paid"}}, queen::KvTtl::seconds(7 * 86400), {})})},
                {"timers", json::array({queen::wire::timer_schedule_op(reminder)})},
            };
            client.get_http_client()->post("/api/v1/transaction", step);
        });
    // guide:end
    {
        json state = client.kv("orders").get("order-1001");
        check(state["found"] == true && state["value"]["status"] == "paid",
              "the KV write committed: " + state.dump());
        json shipped = client.queue("shipments").wait(false).pop();
        check(shipped.size() == 1, "the shipment was pushed");
        check(shipped[0]["partition"] == "customer-42", "into the same customer partition");
        // The reminder is pending, a day from now.
        json timer = client.get_http_client()->get("/api/v1/timers/reminders/review-1001");
        check(timer["found"] == true, "the reminder is scheduled: " + timer.dump());
    }
    std::cout << "ok: transaction" << std::endl;

    json replayed;
    // guide:start(replay)
    client.queue("orders")
        .group("audit")                              // a new group, with its own cursor
        .subscription_from("2026-10-22T00:00:00Z")   // everything pushed from this time on
        .each()
        .limit(1)
        .consume([&](const json& msg) {
            std::cout << "again: " << msg["data"] << std::endl;
            replayed = msg["data"]["orderId"]; // guide:hide
        });
    // guide:end
    check(replayed == 1001, "the audit group read the order again");
    std::cout << "ok: replay" << std::endl;

    // guide:start(fetch)
    auto read = client.queue("orders").fetch({"customer-42"});  // no group, lease or ack
    for (const queen::FetchedRecord& record : read["customer-42"].records) {
        std::cout << record.offset << " " << record.payload << std::endl;
    }
    // guide:end
    {
        const auto& records = read["customer-42"].records;
        check(!records.empty() && records[0].payload["orderId"] == 1001,
              "fetch read the partition from its first record");
    }
    std::cout << "ok: fetch" << std::endl;

    // guide:start(lock)
    queen::Lock lock = client.lock("daily-report", 30);  // a lease with a fencing token
    if (lock.acquire()) {                                // false: another worker has it
        try {
            client.transaction()
                .guard(lock)                   // commits only while the lock is ours
                .queue("reports")
                .push({{{"transactionId", "report-2026-10-22"},
                        {"data", {{"day", "2026-10-22"}}}}})
                .commit();
        } catch (...) {
            lock.release();                    // given back when the commit throws, too
            throw;
        }
        lock.release();
    }
    // guide:end
    check(client.queue("reports").wait(false).pop().size() == 1, "the guarded report was pushed");
    std::cout << "ok: lock" << std::endl;

    std::thread worker([&] {
        // guide:start(worker)
        queen::EphemeralPopOptions take;
        take.group = "pricers";
        take.wait = true;
        json batch = client.ephemeral().pop("price-requests", take);
        for (const json& m : batch["messages"]) {
            json answer = {{"sku", m["payload"]["sku"]}, {"price", 59.9}};
            client.ephemeral().push(m["payload"]["replyTo"], json::array({answer}));
        }
        queen::EphemeralAckOptions done;
        done.group = "pricers";
        client.ephemeral().ack("price-requests", batch["messages"], done);
        // guide:end
    });
    std::this_thread::sleep_for(300ms);  // the worker is already waiting when a caller asks

    // guide:start(caller)
    auto inbox = "inbox-" + queen::util::generate_uuid_v7();      // a private reply queue
    json request = {{"sku", "A-1"}, {"replyTo", inbox}};
    client.ephemeral().push("price-requests", json::array({request}));
    queen::EphemeralPopOptions reply_wait;
    reply_wait.wait = true;
    reply_wait.timeout_millis = 5000;
    reply_wait.commit_on_delivery = true;
    json reply = client.ephemeral().pop(inbox, reply_wait);
    std::cout << reply["messages"][0]["payload"] << "\n";      // {"price":59.9,"sku":"A-1"}
    // guide:end
    worker.join();
    check(reply["messages"][0]["payload"]["price"] == 59.9, "the reply came back");
    std::cout << "ok: ephemeral" << std::endl;

    client.close();
    admin.close();
    std::cout << "\nPASS" << std::endl;
    return 0;
} catch (const std::exception& e) {
    std::cerr << "FAIL: " << e.what() << std::endl;
    return 1;
}
